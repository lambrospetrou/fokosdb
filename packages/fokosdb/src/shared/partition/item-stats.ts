import { SQLSchemaMigrations, type SQLSchemaMigration } from "durable-utils/sql-migrations";
import { KeyCodec, type FokosCompositeKey, type KeyBytes } from "../../sharding/key-codec.js";
import { asRoutingHash, type RoutingHash } from "../../sharding/hash-primitives.js";
import { one, tryOne } from "../sql-cursor.js";
import {
	BUCKET_THRESHOLD_MAX,
	bucketThreshold,
	estSql,
	FACT_COLUMNS,
	FACT_NAMES,
	factsOfRow,
	NO_FACTS,
	promotionEstSql,
	type FactsSql,
	type ItemFacts,
	type ItemFactsRow,
	type StorageModel,
} from "./item-size.js";

const sqlMigrations: SQLSchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: "Create the item facts of each bucket",
		// A bucket is one row. It holds the facts of the items of one routingHash in a key range: from its
		// lower bound `(lower_hk, lower_sk)`, up to the lower bound of the next bucket of the same
		// routingHash, in composite-key order. The first bucket of a routingHash has the bound (x'', x''),
		// the smallest composite key, so the buckets of a routingHash cover all its items.
		//
		// - A hash leaf keeps at most one bucket for each routingHash: the first bucket. A routingHash
		//   with fewer than m items (`bucketThreshold`) can have no bucket.
		// - A range leaf keeps buckets of K to 2K items (`RANGE_BUCKET_MIN_ITEMS`). The split planner then reads
		//   the items of one bucket to find a boundary, and not the items of the whole slice.
		//
		// The table starts with `routing_hash`, as the item table does. Thus the buckets of one
		// routingHash are one key range, and a promotion deletes them with one range delete.
		//
		// `promotion_est_bytes` is the rounded estimate of the bucket when the estimate is at or above the
		// promotion floor, and NULL below the floor. The floor is the value when the statement that last
		// wrote the row ran. Each statement that writes a fact calculates the column again from the new
		// facts, never from its old value. The partial index holds only the rows with a value. Thus a write
		// below the floor writes no index entry, and the promotion lookup reads only a few entries, from
		// the largest value.
		sql: `
            CREATE TABLE IF NOT EXISTS item_stats (
                routing_hash        BLOB    NOT NULL,
                lower_hk            BLOB    NOT NULL,
                lower_sk            BLOB    NOT NULL,
                item_count          INTEGER NOT NULL,
                item_bytes          INTEGER NOT NULL,
                key_bytes           INTEGER NOT NULL,
                indexed_item_count  INTEGER NOT NULL,
                indexed_key_bytes   INTEGER NOT NULL,
                overflow_bytes      INTEGER NOT NULL,
                promotion_est_bytes INTEGER,
                PRIMARY KEY (routing_hash, lower_hk, lower_sk)
            ) WITHOUT ROWID, STRICT;

            CREATE INDEX IF NOT EXISTS item_stats_promotion ON item_stats (promotion_est_bytes)
                WHERE promotion_est_bytes IS NOT NULL;`,
	},
];

/**
 * The bucket size `K` of a range leaf: a bucket holds `K` to `2K` items. An insert splits a bucket
 * above `2K` items, and a delete merges two neighbour buckets that hold fewer than `K` items together.
 * The split planner reads the items of at most one bucket for each boundary, so at most 4,096 items.
 * A smaller value makes more `item_stats` rows, and more splits and merges.
 */
export const RANGE_BUCKET_MIN_ITEMS = 2048;

/** The parameters `?4` to `?9` of a bucket statement: the six facts, in the order of `FACT_NAMES`. */
const FACT_PARAMS: FactsSql = ["?4", "?5", "?6", "?7", "?8", "?9"];

/** The parameter of the promotion floor in a bucket statement. */
const FLOOR_PARAM = "?10";

type BucketRow = ItemFactsRow & { lower_hk: ArrayBuffer; lower_sk: ArrayBuffer };

type WrittenRow = BucketRow & { est: number };

const COLUMNS = FACT_COLUMNS.join(", ");

/** The bucket with the greatest bound at or below `(?2, ?3)`. */
const SEEK_BOUND_SQL = `SELECT lower_hk, lower_sk FROM item_stats
	  WHERE routing_hash = ?1 AND (lower_hk, lower_sk) <= (?2, ?3)
	  ORDER BY lower_hk DESC, lower_sk DESC LIMIT 1`;

const NEXT_BUCKET_SQL = `SELECT lower_hk, lower_sk, ${COLUMNS} FROM item_stats
	  WHERE routing_hash = ?1 AND (lower_hk, lower_sk) > (?2, ?3)
	  ORDER BY lower_hk ASC, lower_sk ASC LIMIT 1`;

const PREVIOUS_BUCKET_SQL = `SELECT lower_hk, lower_sk, ${COLUMNS} FROM item_stats
	  WHERE routing_hash = ?1 AND (lower_hk, lower_sk) < (?2, ?3)
	  ORDER BY lower_hk DESC, lower_sk DESC LIMIT 1`;

const LAST_BUCKET_SQL = `SELECT lower_hk, lower_sk, ${COLUMNS} FROM item_stats
	  WHERE routing_hash = ?1 ORDER BY lower_hk DESC, lower_sk DESC LIMIT 1`;

const FACTS_AT_SQL = `SELECT ${COLUMNS} FROM item_stats WHERE routing_hash = ?1 AND (lower_hk, lower_sk) = (?2, ?3)`;

const FACTS_OF_SQL = `SELECT ${FACT_COLUMNS.map((c) => `COALESCE(SUM(${c}), 0) AS ${c}`).join(", ")} FROM item_stats WHERE routing_hash = ?1`;

/**
 * The statements of `ItemStats` that depend on the storage model. The model of an instance does not
 * change, so the constructor builds them one time, and a write does not build SQL text.
 */
type ModelStatements = { addAt: string; seekAdd: string; upsert: string; put: string };

function modelStatements(model: StorageModel): ModelStatements {
	// In RETURNING, a column name reads the new value of the row.
	const returning = `RETURNING lower_hk, lower_sk, ${COLUMNS}, ${estSql(model, FACT_COLUMNS)} AS est`;
	// In UPDATE SET and in DO UPDATE SET, a column name reads the old value of the row.
	const added = FACT_COLUMNS.map((c, i) => `${c} + ${FACT_PARAMS[i]}`) as unknown as FactsSql;
	const update = (target: string) =>
		`UPDATE item_stats SET ${FACT_COLUMNS.map((c, i) => `${c} = ${added[i]}`).join(", ")},
		        promotion_est_bytes = ${promotionEstSql(model, added, FLOOR_PARAM)}
		  WHERE routing_hash = ?1 AND (lower_hk, lower_sk) = ${target}
		 ${returning}`;
	const write = (mode: "add" | "set") => {
		const merged = FACT_COLUMNS.map((c) => (mode === "add" ? `${c} + excluded.${c}` : `excluded.${c}`)) as unknown as FactsSql;
		return `INSERT INTO item_stats (routing_hash, lower_hk, lower_sk, ${COLUMNS}, promotion_est_bytes)
		 VALUES (?1, ?2, ?3, ${FACT_PARAMS.join(", ")}, ${promotionEstSql(model, FACT_PARAMS, FLOOR_PARAM)})
		 ON CONFLICT (routing_hash, lower_hk, lower_sk) DO UPDATE SET
		   ${FACT_COLUMNS.map((c, i) => `${c} = ${merged[i]}`).join(", ")},
		   promotion_est_bytes = ${promotionEstSql(model, merged, FLOOR_PARAM)}
		 ${returning}`;
	};
	return {
		addAt: update("(?2, ?3)"),
		seekAdd: update(`(${SEEK_BOUND_SQL})`),
		upsert: write("add"),
		put: write("set"),
	};
}

/** One bucket: its lower bound and its facts. */
export type Bucket = { bound: FokosCompositeKey; facts: ItemFacts };

/** The bound of the first bucket of a routingHash: the smallest composite key. */
const FIRST_BOUND: FokosCompositeKey = { hk: KeyCodec.asKeyBytes(new Uint8Array()), sk: KeyCodec.asKeyBytes(new Uint8Array()) };

/**
 * The reads of the item table that `ItemStats` needs. Each store implements them over its own table,
 * next to its schema, so `ItemStats` runs SQL only on `item_stats`. Each read covers the items of
 * `routingHash` in the composite interval `[start, end)`, where a null edge is unbounded, and each read
 * must seek the primary key of the item table.
 */
export type ItemSource = {
	/**
	 * The sum of the facts of the items in `[start, end)`. With `limit`, it sums at most the first
	 * `limit` items, in key order. The sum must equal the sum of `itemFacts` over the same items, so
	 * use `factsAggregateSql`.
	 */
	countFacts(routingHash: RoutingHash, start: FokosCompositeKey | null, end: FokosCompositeKey | null, limit?: number): ItemFacts;
	/** The number of items in `[start, end)`, at most `limit`. It reads at most `limit` items. */
	countItems(routingHash: RoutingHash, start: FokosCompositeKey | null, end: FokosCompositeKey | null, limit: number): number;
	/** The key of the item at `offset` in `[start, end)`, in key order, or undefined when the interval has fewer items. */
	itemAt(
		routingHash: RoutingHash,
		start: FokosCompositeKey | null,
		end: FokosCompositeKey | null,
		offset: number,
	): FokosCompositeKey | undefined;
	/** The hash key of the first item of `routingHash`, or undefined when it has no item. */
	firstHashKey(routingHash: RoutingHash): KeyBytes | undefined;
};

export type ItemStatsOptions = {
	/** The reads of the item table of the store. */
	items: ItemSource;
	/** The coefficients of the item table. */
	model: StorageModel;
	/**
	 * The promotion floor of the current policy, or null when the partition has no promotion
	 * (`promotionFloor` in `split-planner.ts`). Each statement that writes facts reads it when the
	 * statement runs, because the policy can change. On a hash leaf, it also sets the bucket threshold
	 * (`bucketThreshold`).
	 */
	promotionFloor: () => number | null;
	/**
	 * True when the partition is a range leaf, and false when it is a hash leaf. The store reads it at
	 * each fact change, because the identity of a partition is not known when the store starts.
	 */
	isRangeLeaf: () => boolean;
	/** The bucket size `K` of a range leaf (`RANGE_BUCKET_MIN_ITEMS`). A test sets a small value. */
	rangeBucketMinItems: number;
	/** The prefix of the log message, for example `fokos/partition`. */
	logName: string;
	logParams: () => Record<string, unknown>;
};

/**
 * Keeps the exact facts of the items of a partition, in buckets. It owns the `item_stats` table and
 * its schema migrations. It records the migrations under its own KV key, so it can share one storage
 * with a host that has its own tables and migrations.
 *
 * The host gives each item change to this class in the same transaction as the item change. Thus,
 * after each commit, the facts of each bucket equal the sums over its items. When a write finds facts
 * that cannot be true, it logs the defect and counts the facts again from the items, in the same
 * transaction. It never throws for a defect, because a throw rolls back a commit apply, and a commit
 * apply must not fail.
 *
 * On a hash leaf:
 * - A routingHash has at most one bucket, its first bucket.
 * - A routingHash with fewer than `m` items (`bucketThreshold`) can have no bucket. Its estimate is
 *   then below the promotion floor, so the promotion lookup does not need it.
 * - A bucket stays while its routingHash has items. A delete of the last item deletes the bucket.
 *
 * On a range leaf:
 * - The bucket of an item is the bucket with the greatest bound at or below the key of the item.
 * - An insert splits a bucket above `2K` items at its middle item.
 * - A delete merges a bucket below `K` items with its smaller neighbour, when the two buckets hold
 *   fewer than `K` items together.
 * - Each bound is the key of an item that existed at the split. Thus the items of a bucket are one key
 *   range of the primary key, and a count of a bucket reads only that key range.
 */
export class ItemStats {
	readonly model: StorageModel;
	#storage: DurableObjectStorage;
	#opts: ItemStatsOptions;
	#migrations: SQLSchemaMigrations;
	#sql: ModelStatements;

	constructor(storage: DurableObjectStorage, opts: ItemStatsOptions) {
		this.#storage = storage;
		this.#opts = opts;
		this.model = opts.model;
		this.#migrations = new SQLSchemaMigrations({
			migrations: sqlMigrations,
			doStorage: storage,
			keyNameTrackingLastMigrationID: "__fokosdb/item_stats/sql_schema_version",
		});
		this.#sql = modelStatements(opts.model);
	}

	runMigrations(): void {
		this.#migrations.runAllSync();
	}

	/**
	 * Applies the change of the item `(hk, sk)` from `oldFacts` to `newFacts` to the bucket of the item.
	 * `oldFacts` is null for a new item, and `newFacts` is null for a deleted item. The item change must
	 * be done before this call, because a repair counts the items.
	 *
	 * On a hash leaf, it returns the rounded estimate of the routingHash after the change, or null when
	 * the routingHash has no bucket or when the facts of the item do not change. On a range leaf, it
	 * returns null, because a range leaf has no promotion.
	 */
	change(routingHash: RoutingHash, hk: KeyBytes, sk: KeyBytes, oldFacts: ItemFacts | null, newFacts: ItemFacts | null): number | null {
		if (oldFacts === null && newFacts === null) {
			return null;
		}
		const delta = subtractFacts(newFacts ?? NO_FACTS, oldFacts ?? NO_FACTS);
		if (FACT_NAMES.every((name) => delta[name] === 0)) {
			// The facts do not change, so this call reads and writes nothing. An UPDATE with the same values
			// writes the row and its index entry again. The routingHash did not grow, so the write signals
			// no promotion. When the leaf is over its cap, the promotion lookup finds the routingHash.
			return null;
		}
		const range = this.#opts.isRangeLeaf();
		if (!range) {
			const row = this.#addAt(routingHash, FIRST_BOUND, delta);
			if (row) {
				return this.#checkedHash(routingHash, row, delta);
			}
			// The routingHash has no bucket, so it has fewer than `m` items. An insert can make it `m`
			// items, so it counts them. Any other change keeps the item count, and no bucket records the item.
			return oldFacts === null ? (this.#createFirst(routingHash, delta)?.est ?? null) : null;
		}

		const key = { hk, sk };
		let row = this.#seekAdd(routingHash, key, delta);
		if (row !== undefined) {
			row = this.#checkedRange(routingHash, row, delta);
		} else if (newFacts === null) {
			return null;
		} else {
			// No bucket is at or below the key, so the first bucket of the routingHash is absent. Normally
			// this is the first insert of the routingHash, which starts its first bucket.
			row = this.#coverBelowFirst(routingHash, oldFacts === null ? delta : null, delta);
		}
		if (oldFacts === null) {
			this.#split(routingHash, key, row);
		} else if (newFacts === null) {
			this.#merge(routingHash, boundOf(row), factsOfRow(row));
		}
		return null;
	}

	/**
	 * Subtracts the facts of deleted items, with one statement for each bucket. The TTL sweep uses it
	 * for each chunk of expired items. An item whose routingHash has no bucket changes nothing. On a
	 * range leaf, each changed bucket then gets the merge check.
	 */
	subtractRows(rows: Iterable<{ routingHash: RoutingHash; hk: KeyBytes; sk: KeyBytes; facts: ItemFacts }>): void {
		const range = this.#opts.isRangeLeaf();
		const groups = new Map<string, { routingHash: RoutingHash; bound: FokosCompositeKey; facts: ItemFacts }>();
		for (const { routingHash, hk, sk, facts } of rows) {
			const bound = range ? this.#boundOf(routingHash, { hk, sk }) : FIRST_BOUND;
			if (bound === undefined) {
				continue;
			}
			const id = `${routingHash.toHex()}/${bound.hk.toHex()}/${bound.sk.toHex()}`;
			const group = groups.get(id);
			groups.set(id, { routingHash, bound, facts: group ? addFacts(group.facts, facts) : facts });
		}
		// The buckets of a range leaf that hold fewer than `K` items after the subtraction. Only these can
		// merge: a merge adds items to the bucket that stays, so a bucket with `K` items or more never merges.
		const small: Array<{ routingHash: RoutingHash; bound: FokosCompositeKey }> = [];
		for (const { routingHash, bound, facts } of groups.values()) {
			const delta = subtractFacts(NO_FACTS, facts);
			const row = this.#addAt(routingHash, bound, delta);
			if (row === undefined) {
				continue;
			}
			if (!range) {
				this.#checkedHash(routingHash, row, delta);
			} else if (this.#checkedRange(routingHash, row, delta).item_count < this.#opts.rangeBucketMinItems) {
				small.push({ routingHash, bound });
			}
		}
		if (small.length > 0) {
			// A merge can delete the row of an earlier group. The items of that group are then in the lower
			// bucket, so the merge check finds the current bucket again.
			for (const { routingHash, bound } of small) {
				const current = this.#boundOf(routingHash, bound);
				const facts = current && this.#factsAt(routingHash, current);
				if (current !== undefined && facts !== undefined) {
					this.#merge(routingHash, current, facts);
				}
			}
		}
	}

	/**
	 * Adds the facts of the items that one import page inserted. The items are in storage order, so the
	 * items of one routingHash are next to each other. A run is the items of one routingHash in the page.
	 *
	 * - A hash leaf adds each run to the first bucket of its routingHash with one statement. When the
	 *   routingHash has no bucket, it creates the bucket only when the routingHash then has `m` items or
	 *   more.
	 * - A range leaf adds the items to the last bucket of the routingHash until that bucket holds `K`
	 *   items. The next item then starts a new bucket at its own key. It reads no item.
	 */
	addRows(rows: Iterable<{ routingHash: RoutingHash; hk: KeyBytes; sk: KeyBytes; facts: ItemFacts }>): void {
		const range = this.#opts.isRangeLeaf();
		const K = this.#opts.rangeBucketMinItems;
		type Pending = {
			routingHash: RoutingHash;
			bound: FokosCompositeKey;
			stored: boolean;
			first: FokosCompositeKey;
			count: number;
			facts: ItemFacts;
		};
		const flush = (p: Pending) => {
			if (p.facts.itemCount === 0) {
				return;
			}
			if (!range) {
				const row = this.#addAt(p.routingHash, FIRST_BOUND, p.facts);
				if (row !== undefined) {
					this.#checkedHash(p.routingHash, row, p.facts);
				} else {
					this.#importFirst(p.routingHash, p.first, p.facts);
				}
				return;
			}
			const row = p.stored ? this.#addAt(p.routingHash, p.bound, p.facts) : this.#upsertAt(p.routingHash, p.bound, p.facts);
			if (row !== undefined) {
				this.#checkedRange(p.routingHash, row, p.facts);
			}
		};
		let run: Pending | null = null;
		for (const { routingHash, hk, sk, facts } of rows) {
			if (run === null || !sameKey(run.routingHash, routingHash)) {
				if (run !== null) {
					flush(run);
				}
				const last = range ? this.#lastBucket(routingHash) : undefined;
				run = last
					? { routingHash, bound: boundOf(last), stored: true, first: { hk, sk }, count: last.item_count, facts: { ...NO_FACTS } }
					: { routingHash, bound: FIRST_BOUND, stored: false, first: { hk, sk }, count: 0, facts: { ...NO_FACTS } };
			}
			// When the last bucket holds `K` items, the item starts a new bucket. This also applies to the
			// first item of a page when the last bucket from an earlier page holds `K` items.
			if (range && run.count >= K) {
				flush(run);
				run = { routingHash, bound: { hk, sk }, stored: false, first: { hk, sk }, count: 0, facts: { ...NO_FACTS } };
			}
			run.count++;
			// Each run has its own facts object, so the sum changes it in place.
			addFactsInto(run.facts, facts);
		}
		if (run !== null) {
			flush(run);
		}
	}

	/** Deletes the buckets of `routingHash`, one key range of the primary key. */
	remove(routingHash: RoutingHash): void {
		this.#storage.sql.exec(`DELETE FROM item_stats WHERE routing_hash = ?1`, routingHash);
	}

	/** The sum of the facts of the buckets of `routingHash`. */
	factsOf(routingHash: RoutingHash): ItemFacts {
		const row = tryOne(this.#storage.sql.exec<ItemFactsRow>(FACTS_OF_SQL, routingHash));
		return row ? factsOfRow(row) : { ...NO_FACTS };
	}

	/** The buckets of `routingHash` with a bound below `end`, in storage order. A null `end` is unbounded. */
	buckets(routingHash: RoutingHash, end: FokosCompositeKey | null): Bucket[] {
		const below = end === null ? "" : "AND (lower_hk, lower_sk) < (?2, ?3)";
		return this.#storage.sql
			.exec<BucketRow>(
				`SELECT lower_hk, lower_sk, ${COLUMNS} FROM item_stats
				  WHERE routing_hash = ?1 ${below} ORDER BY lower_hk, lower_sk`,
				routingHash,
				...(end === null ? [] : [end.hk, end.sk]),
			)
			.toArray()
			.map((row) => ({ bound: boundOf(row), facts: factsOfRow(row) }));
	}

	/**
	 * The promotion lookup of the split decision. It finds up to `limit` routingHashes whose stored
	 * `promotion_est_bytes` is at or above `threshold`, largest first. For each one, it returns one hash
	 * key that has items, because the runtime promotes by hash key. It reads at most `limit` entries of
	 * `item_stats_promotion`, and does at most `limit` primary-key seeks of the item table.
	 */
	largestAtLeast(threshold: number, limit: number): KeyBytes[] {
		const routingHashes = this.#storage.sql
			.exec<{
				routing_hash: ArrayBuffer;
			}>(`SELECT routing_hash FROM item_stats WHERE promotion_est_bytes >= ?1 ORDER BY promotion_est_bytes DESC LIMIT ?2`, threshold, limit)
			.toArray();
		const keys: KeyBytes[] = [];
		for (const { routing_hash } of routingHashes) {
			const hashKey = this.#opts.items.firstHashKey(asRoutingHash(new Uint8Array(routing_hash)));
			if (hashKey === undefined) {
				// The bucket has no items, which is a defect. The lookup skips this entry and reads no other
				// entry in its place, so the read stays bounded. The answer then has fewer keys.
				this.#logDefect(new Uint8Array(routing_hash), FIRST_BOUND, "a bucket of the promotion index has no items", {});
				continue;
			}
			keys.push(hashKey);
		}
		return keys;
	}

	/** The bound of the bucket of `key`: the greatest bound at or below it. One primary-key seek. */
	#boundOf(routingHash: RoutingHash, key: FokosCompositeKey): FokosCompositeKey | undefined {
		const row = tryOne(
			this.#storage.sql.exec<{ lower_hk: ArrayBuffer; lower_sk: ArrayBuffer }>(SEEK_BOUND_SQL, routingHash, key.hk, key.sk),
		);
		return row && boundOf(row);
	}

	/** The bucket with the greatest bound of `routingHash`. One primary-key seek. */
	#lastBucket(routingHash: RoutingHash): BucketRow | undefined {
		return tryOne(this.#storage.sql.exec<BucketRow>(LAST_BUCKET_SQL, routingHash));
	}

	/** The next or the previous bucket of the bucket at `bound`. One primary-key seek. */
	#neighbour(routingHash: RoutingHash, bound: FokosCompositeKey, direction: "next" | "previous"): BucketRow | undefined {
		const sql = direction === "next" ? NEXT_BUCKET_SQL : PREVIOUS_BUCKET_SQL;
		return tryOne(this.#storage.sql.exec<BucketRow>(sql, routingHash, bound.hk, bound.sk));
	}

	/**
	 * Adds `delta` to the bucket of `key` on a range leaf: the bucket with the greatest bound at or below
	 * `key`. One statement. It returns the new row, or undefined when the routingHash has no bucket.
	 */
	#seekAdd(routingHash: RoutingHash, key: FokosCompositeKey, delta: ItemFacts): WrittenRow | undefined {
		return this.#add(this.#sql.seekAdd, routingHash, key, delta);
	}

	/** Adds `delta` to the bucket at `bound`. It returns the new row, or undefined when the bucket is absent. */
	#addAt(routingHash: RoutingHash, bound: FokosCompositeKey, delta: ItemFacts): WrittenRow | undefined {
		return this.#add(this.#sql.addAt, routingHash, bound, delta);
	}

	#add(sql: string, routingHash: RoutingHash, key: FokosCompositeKey, delta: ItemFacts): WrittenRow | undefined {
		return tryOne(this.#storage.sql.exec<WrittenRow>(sql, routingHash, key.hk, key.sk, ...factValues(delta), this.#opts.promotionFloor()));
	}

	/** Adds `facts` to the bucket at `bound`, and creates the bucket with `facts` when it is absent. */
	#upsertAt(routingHash: RoutingHash, bound: FokosCompositeKey, facts: ItemFacts): WrittenRow {
		return this.#write(this.#sql.upsert, routingHash, bound, facts);
	}

	/** Sets the facts of the bucket at `bound` to `facts`, and creates the bucket when it is absent. */
	#putAt(routingHash: RoutingHash, bound: FokosCompositeKey, facts: ItemFacts): WrittenRow {
		return this.#write(this.#sql.put, routingHash, bound, facts);
	}

	#write(sql: string, routingHash: RoutingHash, bound: FokosCompositeKey, facts: ItemFacts): WrittenRow {
		return one(this.#storage.sql.exec<WrittenRow>(sql, routingHash, bound.hk, bound.sk, ...factValues(facts), this.#opts.promotionFloor()));
	}

	#deleteAt(routingHash: RoutingHash, bound: FokosCompositeKey): void {
		this.#storage.sql.exec(
			`DELETE FROM item_stats WHERE routing_hash = ?1 AND (lower_hk, lower_sk) = (?2, ?3)`,
			routingHash,
			bound.hk,
			bound.sk,
		);
	}

	#factsAt(routingHash: RoutingHash, bound: FokosCompositeKey): ItemFacts | undefined {
		const row = tryOne(this.#storage.sql.exec<ItemFactsRow>(FACTS_AT_SQL, routingHash, bound.hk, bound.sk));
		return row && factsOfRow(row);
	}

	/**
	 * Runs after an insert on a hash leaf into a routingHash with no bucket. It creates the first bucket
	 * when the routingHash now has `m` items or more. Such a routingHash has at most
	 * `BUCKET_THRESHOLD_MAX` items, so the count reads at most 17 rows, and the facts read each item.
	 * More items is a defect: the recount of the whole routingHash then creates the bucket.
	 */
	#createFirst(routingHash: RoutingHash, delta: ItemFacts): WrittenRow | undefined {
		const count = this.#opts.items.countItems(routingHash, null, null, BUCKET_THRESHOLD_MAX + 1);
		if (count > BUCKET_THRESHOLD_MAX) {
			return this.#repair(routingHash, null, delta);
		}
		if (count < bucketThreshold(this.model, this.#opts.promotionFloor())) {
			return undefined;
		}
		return this.#putAt(routingHash, FIRST_BOUND, this.#opts.items.countFacts(routingHash, FIRST_BOUND, null, BUCKET_THRESHOLD_MAX + 1));
	}

	/**
	 * Runs after an import page on a hash leaf adds items to a routingHash with no bucket. It creates
	 * the first bucket when the items of the earlier pages and of this page are `m` or more. The items
	 * of the earlier pages are below `first`, the first item of this page. They are fewer than
	 * `BUCKET_THRESHOLD_MAX`, so the count reads at most 17 rows. More items is a defect: the recount of
	 * the whole routingHash then creates the bucket.
	 */
	#importFirst(routingHash: RoutingHash, first: FokosCompositeKey, facts: ItemFacts): void {
		const earlier = this.#opts.items.countFacts(routingHash, FIRST_BOUND, first, BUCKET_THRESHOLD_MAX + 1);
		if (earlier.itemCount > BUCKET_THRESHOLD_MAX) {
			this.#repair(routingHash, null, facts);
		} else if (earlier.itemCount + facts.itemCount >= bucketThreshold(this.model, this.#opts.promotionFloor())) {
			this.#putAt(routingHash, FIRST_BOUND, addFacts(earlier, facts));
		}
	}

	/**
	 * Runs after an insert on a range leaf. While the bucket of `key` holds more than `2K` items, it
	 * splits that bucket at its middle item. It counts the facts of both halves from the items, so a
	 * split also corrects wrong facts.
	 */
	#split(routingHash: RoutingHash, key: FokosCompositeKey, row: WrittenRow): void {
		let bucket = row;
		while (bucket.item_count > 2 * this.#opts.rangeBucketMinItems) {
			const lower = boundOf(bucket);
			const next = this.#neighbour(routingHash, lower, "next");
			const upper = next ? boundOf(next) : null;
			const middle = this.#opts.items.itemAt(routingHash, lower, upper, Math.floor(bucket.item_count / 2));
			if (middle === undefined) {
				// The bucket holds fewer items than its count. The recount sets the true count, and the
				// loop splits again only when that count is still above 2K.
				bucket = this.#repairRange(routingHash, lower, factsOfRow(bucket), NO_FACTS);
				continue;
			}
			const lowerRow = this.#putAt(routingHash, lower, this.#opts.items.countFacts(routingHash, lower, middle));
			const upperRow = this.#putAt(routingHash, middle, this.#opts.items.countFacts(routingHash, middle, upper));
			bucket = KeyCodec.compareComposite(key, middle) >= 0 ? upperRow : lowerRow;
		}
	}

	/**
	 * Runs after a delete on a range leaf. While the bucket at `bound` holds fewer than `K` items, and it
	 * holds fewer than `K` items together with a neighbour, it merges the two buckets. It selects the
	 * neighbour with fewer items. A merge adds the facts of the upper bucket to the lower bucket, and
	 * deletes the upper row.
	 *
	 * After the merges, it deletes a bucket with no items. It keeps the first bucket while other buckets
	 * follow it, because the first bucket must cover the smallest keys.
	 */
	#merge(routingHash: RoutingHash, bound: FokosCompositeKey, bucketFacts: ItemFacts): void {
		const K = this.#opts.rangeBucketMinItems;
		let current = bound;
		let facts = bucketFacts;
		while (facts.itemCount < K) {
			const previous = this.#neighbour(routingHash, current, "previous");
			const next = this.#neighbour(routingHash, current, "next");
			const count = facts.itemCount;
			const withPrevious = previous !== undefined && previous.item_count + count < K ? previous : undefined;
			const withNext = next !== undefined && next.item_count + count < K ? next : undefined;
			if (withPrevious === undefined && withNext === undefined) {
				break;
			}
			const merged =
				withPrevious !== undefined && (withNext === undefined || withPrevious.item_count <= withNext.item_count)
					? this.#mergePair(routingHash, boundOf(withPrevious), current, facts)
					: this.#mergePair(routingHash, current, boundOf(withNext!), factsOfRow(withNext!));
			if (merged === undefined) {
				return;
			}
			current = boundOf(merged);
			facts = factsOfRow(merged);
		}
		if (facts.itemCount === 0) {
			const next = this.#neighbour(routingHash, current, "next");
			if (!isFirstBound(current) || next === undefined) {
				// The facts tell that the bucket has no items. One seek checks it before the delete, because a
				// wrong delete leaves items with no bucket. Then a later change of them must count them again.
				if (this.#opts.items.countItems(routingHash, current, next ? boundOf(next) : null, 1) > 0) {
					this.#repairRange(routingHash, current, facts, NO_FACTS);
				} else {
					this.#deleteAt(routingHash, current);
				}
			}
		}
	}

	/**
	 * Runs on a range leaf when no bucket is at or below a changed item. Thus the first bucket is absent,
	 * and no bucket holds the items below the lowest bucket that is present. This function creates the
	 * first bucket for those items, and returns the last bucket that it writes.
	 *
	 * - Normally the change is the first insert of the routingHash: `insert` holds its facts, and no other
	 *   item is below the lowest bucket. One count of at most 2 items checks it, and the function writes
	 *   `insert` as the first bucket.
	 * - Else it is a defect. The function logs it and counts the items below the lowest bucket again, in
	 *   buckets of `K` items. When a bucket is present, the count reads only the items below it. When no
	 *   bucket is present, the count reads each item of the routingHash one time, in statements of at
	 *   most `K` items, and gives buckets that need no split later.
	 */
	#coverBelowFirst(routingHash: RoutingHash, insert: ItemFacts | null, delta: ItemFacts): WrittenRow {
		const lowest = this.#neighbour(routingHash, FIRST_BOUND, "next");
		const end = lowest ? boundOf(lowest) : null;
		if (insert !== null && this.#opts.items.countItems(routingHash, null, end, 2) <= 1) {
			return this.#upsertAt(routingHash, FIRST_BOUND, insert);
		}
		this.#logDefect(routingHash, FIRST_BOUND, "the facts of a bucket disagree with its items, so they are counted again", {
			stored: null,
			delta,
		});
		const K = this.#opts.rangeBucketMinItems;
		let from = FIRST_BOUND;
		for (;;) {
			// The item at offset K starts the next bucket, so each bucket gets K items.
			const next = this.#opts.items.itemAt(routingHash, from, end, K);
			const row = this.#putAt(routingHash, from, this.#opts.items.countFacts(routingHash, from, next ?? end));
			if (next === undefined) {
				return row;
			}
			from = next;
		}
	}

	/** Adds the facts of the upper bucket to the lower bucket and deletes the upper row. */
	#mergePair(routingHash: RoutingHash, lower: FokosCompositeKey, upper: FokosCompositeKey, upperFacts: ItemFacts): WrittenRow | undefined {
		const merged = this.#addAt(routingHash, lower, upperFacts);
		this.#deleteAt(routingHash, upper);
		return merged;
	}

	/**
	 * Checks the facts that a write on a hash leaf returned. When the facts cannot be true, it starts the
	 * repair. When the bucket has no items, it deletes the bucket, because the routingHash then has no
	 * items. It returns the estimate after the write, or null when it deleted the bucket.
	 */
	#checkedHash(routingHash: RoutingHash, row: WrittenRow, delta: ItemFacts): number | null {
		const facts = factsOfRow(row);
		if (impossible(facts)) {
			return this.#repair(routingHash, subtractFacts(facts, delta), delta)?.est ?? null;
		}
		if (facts.itemCount === 0) {
			this.remove(routingHash);
			return null;
		}
		return row.est;
	}

	/** Checks the facts that a write on a range leaf returned, and repairs the bucket when they cannot be true. */
	#checkedRange(routingHash: RoutingHash, row: WrittenRow, delta: ItemFacts): WrittenRow {
		const facts = factsOfRow(row);
		return impossible(facts) ? this.#repairRange(routingHash, boundOf(row), subtractFacts(facts, delta), delta) : row;
	}

	/** Logs the defect, counts the facts of the bucket at `bound` of a range leaf again from its key range, and writes them. */
	#repairRange(routingHash: RoutingHash, bound: FokosCompositeKey, stored: ItemFacts, delta: ItemFacts): WrittenRow {
		this.#logDefect(routingHash, bound, "the facts of a bucket disagree with its items, so they are counted again", { stored, delta });
		const next = this.#neighbour(routingHash, bound, "next");
		return this.#putAt(routingHash, bound, this.#opts.items.countFacts(routingHash, bound, next ? boundOf(next) : null));
	}

	/**
	 * Logs the defect, counts the facts of all items of `routingHash` again, and writes them to its
	 * first bucket as its only bucket. It returns undefined when the routingHash has no items. The item
	 * change has already run, so the count holds it.
	 */
	#repair(routingHash: RoutingHash, stored: ItemFacts | null, delta: ItemFacts): WrittenRow | undefined {
		this.#logDefect(routingHash, FIRST_BOUND, "the facts of a bucket disagree with its items, so they are counted again", {
			stored,
			delta,
		});
		const counted = this.#opts.items.countFacts(routingHash, FIRST_BOUND, null);
		// The count holds the items of each bucket, so no other bucket can stay.
		this.remove(routingHash);
		if (counted.itemCount === 0) {
			return undefined;
		}
		return this.#putAt(routingHash, FIRST_BOUND, counted);
	}

	#logDefect(routingHash: Uint8Array, bound: FokosCompositeKey, message: string, detail: Record<string, unknown>): void {
		console.error({
			...this.#opts.logParams(),
			message: `${this.#opts.logName}: ${message}`,
			routingHash: routingHash.toHex(),
			bucket: { lowerHk: bound.hk.toHex(), lowerSk: bound.sk.toHex() },
			...detail,
		});
	}
}

/** True when the facts cannot be true: a fact is negative, or the item count is 0 and another fact is not 0. */
function impossible(facts: ItemFacts): boolean {
	return FACT_NAMES.some((name) => facts[name] < 0) || (facts.itemCount === 0 && FACT_NAMES.some((name) => facts[name] !== 0));
}

function boundOf(row: { lower_hk: ArrayBuffer; lower_sk: ArrayBuffer }): FokosCompositeKey {
	return { hk: KeyCodec.asKeyBytes(new Uint8Array(row.lower_hk)), sk: KeyCodec.asKeyBytes(new Uint8Array(row.lower_sk)) };
}

function isFirstBound(bound: FokosCompositeKey): boolean {
	return bound.hk.byteLength === 0 && bound.sk.byteLength === 0;
}

function sameKey(a: RoutingHash, b: RoutingHash): boolean {
	return KeyCodec.compare(KeyCodec.asKeyBytes(a), KeyCodec.asKeyBytes(b)) === 0;
}

function factValues(f: ItemFacts): number[] {
	return FACT_NAMES.map((name) => f[name]);
}

/** Adds the facts `b` to `a`, in place. */
function addFactsInto(a: ItemFacts, b: ItemFacts): void {
	for (const name of FACT_NAMES) {
		a[name] += b[name];
	}
}

export function addFacts(a: ItemFacts, b: ItemFacts): ItemFacts {
	return {
		itemCount: a.itemCount + b.itemCount,
		itemBytes: a.itemBytes + b.itemBytes,
		keyBytes: a.keyBytes + b.keyBytes,
		indexedItemCount: a.indexedItemCount + b.indexedItemCount,
		indexedKeyBytes: a.indexedKeyBytes + b.indexedKeyBytes,
		overflowBytes: a.overflowBytes + b.overflowBytes,
	};
}

function subtractFacts(a: ItemFacts, b: ItemFacts): ItemFacts {
	return {
		itemCount: a.itemCount - b.itemCount,
		itemBytes: a.itemBytes - b.itemBytes,
		keyBytes: a.keyBytes - b.keyBytes,
		indexedItemCount: a.indexedItemCount - b.indexedItemCount,
		indexedKeyBytes: a.indexedKeyBytes - b.indexedKeyBytes,
		overflowBytes: a.overflowBytes - b.overflowBytes,
	};
}
