import { SQLSchemaMigrations, type SQLSchemaMigration } from "durable-utils/sql-migrations";
import { KeyCodec, type FokosCompositeKey, type KeyBytes } from "../../sharding/key-codec.js";
import type { RoutingHash } from "../../sharding/hash-primitives.js";
import { one, tryOne } from "../sql-cursor.js";
import {
	BUCKET_THRESHOLD_MAX,
	bucketThreshold,
	estSql,
	FACT_NAMES,
	NO_FACTS,
	overflowBytesSql,
	promotionEstSql,
	type FactsSql,
	type ItemFacts,
	type StorageModel,
} from "./item-size.js";

const sqlMigrations: SQLSchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: "Create the item facts of each bucket",
		// A bucket is one row: the facts of the items of one routingHash from its lower bound
		// `(lower_hk, lower_sk)` up to the lower bound of the next bucket, in composite-key order. The
		// first bucket of a routingHash has the bound (x'', x''), the smallest composite key. A hash leaf
		// keeps at most the first bucket of each routingHash, and a routingHash with fewer than m items
		// can have none. A range leaf keeps buckets of K to 2K items that cover its slice.
		//
		// The table leads with `routing_hash`, like the item table. Thus the buckets of one routingHash
		// are one key range, and a promotion deletes them with one range delete.
		//
		// `promotion_est_bytes` is the estimate of the bucket, rounded up, when it is at or above the
		// promotion floor of the statement that last wrote the row, and NULL below it. Each statement
		// that writes a fact sets it again from the new facts, never from its old value. The partial
		// index holds only the rows with a value, so a write below the floor writes no index entry, and
		// the promotion lookup reads at most a few entries from the largest value.
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

/** The bucket size `K` of a range leaf: a bucket holds `K` to `2K` items. */
export const BUCKET_ITEMS = 2048;

/** The fact columns of `item_stats`, in the order of `FACT_NAMES`. */
const FACT_COLUMNS = ["item_count", "item_bytes", "key_bytes", "indexed_item_count", "indexed_key_bytes", "overflow_bytes"] as const;

/** The parameters `?4` to `?9` of a bucket statement: the six facts, in the order of `FACT_NAMES`. */
const FACT_PARAMS: FactsSql = ["?4", "?5", "?6", "?7", "?8", "?9"];

/** The parameter of the promotion floor in a bucket statement. */
const FLOOR_PARAM = "?10";

type FactsRow = {
	item_count: number;
	item_bytes: number;
	key_bytes: number;
	indexed_item_count: number;
	indexed_key_bytes: number;
	overflow_bytes: number;
};

type BucketRow = FactsRow & { lower_hk: ArrayBuffer; lower_sk: ArrayBuffer };

type WrittenRow = BucketRow & { est: number };

/** One bucket: its lower bound and its facts. */
export type Bucket = { bound: FokosCompositeKey; facts: ItemFacts };

/** The bound of the first bucket of a routingHash: the smallest composite key. */
const FIRST_BOUND: FokosCompositeKey = { hk: KeyCodec.asKeyBytes(new Uint8Array()), sk: KeyCodec.asKeyBytes(new Uint8Array()) };

export type ItemStatsOptions = {
	/** The item table: `items` or `latest_items`. A fixed name from the code, never user input. */
	table: string;
	/** The hash key and sort key columns of the item table. */
	hkColumn: string;
	skColumn: string;
	/** The SQL condition of an indexed item of the item table, for example `ttl_epoch_utc_seconds IS NOT NULL`. */
	indexedCondition: string;
	/** The coefficients of the item table. */
	model: StorageModel;
	/**
	 * The promotion floor of the current policy, or null when the partition has no promotion. Each
	 * statement that writes facts binds it when it runs. On a hash leaf, it also sets the bucket
	 * threshold (`bucketThreshold`).
	 */
	promotionFloor: () => number | null;
	/** True when the partition is a range leaf. Read at each fact change. */
	rangeLeaf: () => boolean;
	/** The bucket size `K` of a range leaf. */
	bucketItems: number;
	/** The prefix of the log message, for example `fokos/partition`. */
	logName: string;
	logParams: () => Record<string, unknown>;
};

/**
 * The exact facts of the items of a partition, in buckets. It owns the `item_stats` table and its
 * schema migrations, which it tracks under its own KV key. Thus it can share one storage with a host
 * that has its own tables and migrations.
 *
 * The host gives each fact change here in the transaction of the item change, so the facts of each
 * bucket equal the sums over its items after each commit. When a write finds facts that cannot be
 * true, it logs the defect and counts the facts again from the items in the same transaction. It
 * never throws for a defect, because a throw rolls back a commit apply that must not fail.
 *
 * On a hash leaf, a routingHash has at most one bucket, its first bucket. A routingHash with fewer
 * than `m` items (`bucketThreshold`) can have no bucket, and then its estimate is below the promotion
 * floor. A bucket stays while its routingHash has items. On a range leaf, the bucket of
 * an item is the bucket with the greatest bound at or below its key. An insert splits a bucket above
 * `2K` items, and a delete merges a bucket below `K` items with its smaller neighbour when the two
 * hold fewer than `K` items together. A split bound is the key of a real item, so the items of a
 * bucket are one key range of the primary key.
 */
export class ItemStats {
	readonly model: StorageModel;
	#storage: DurableObjectStorage;
	#opts: ItemStatsOptions;
	#migrations: SQLSchemaMigrations;
	#returning: string;

	constructor(storage: DurableObjectStorage, opts: ItemStatsOptions) {
		this.#storage = storage;
		this.#opts = opts;
		this.model = opts.model;
		this.#migrations = new SQLSchemaMigrations({
			migrations: sqlMigrations,
			doStorage: storage,
			keyNameTrackingLastMigrationID: "__fokosdb/item_stats/sql_schema_version",
		});
		// In RETURNING, a column name reads the new value of the row.
		this.#returning = `RETURNING lower_hk, lower_sk, ${FACT_COLUMNS.join(", ")}, ${estSql(opts.model, FACT_COLUMNS)} AS est`;
	}

	runMigrations(): void {
		this.#migrations.runAllSync();
	}

	/**
	 * Applies the change of the item `(hk, sk)` from `oldFacts` to `newFacts` to its bucket. `oldFacts`
	 * is null for a new item, and `newFacts` is null for a deleted item. On a hash leaf it returns the
	 * rounded estimate of the routingHash after the change, or null when the routingHash has no bucket.
	 * On a range leaf it returns null: a range leaf has no promotion.
	 */
	change(routingHash: RoutingHash, hk: KeyBytes, sk: KeyBytes, oldFacts: ItemFacts | null, newFacts: ItemFacts | null): number | null {
		if (oldFacts === null && newFacts === null) {
			return null;
		}
		const delta = subtractFacts(newFacts ?? NO_FACTS, oldFacts ?? NO_FACTS);
		const range = this.#opts.rangeLeaf();
		if (FACT_NAMES.every((name) => delta[name] === 0)) {
			// An update writes the row and its index entry also when the values stay the same.
			return range ? null : this.#estimateOf(routingHash);
		}
		if (!range) {
			const row = this.#addAt(routingHash, FIRST_BOUND, delta);
			if (row) {
				return this.#checkedHash(routingHash, row, delta);
			}
			// A routingHash with no bucket has fewer than `m` items. A change that adds no item changes no
			// facts, because no bucket represents the item.
			return oldFacts === null ? (this.#createFirst(routingHash, delta)?.est ?? null) : null;
		}

		const key = { hk, sk };
		let row = this.#seekAdd(routingHash, key, delta);
		if (row !== undefined) {
			row = this.#checkedRange(routingHash, row, delta);
		} else if (newFacts === null) {
			return null;
		} else if (oldFacts === null) {
			// The routingHash has no bucket, so the insert starts its first bucket.
			row = this.#upsertAt(routingHash, FIRST_BOUND, delta);
		} else {
			// A present item with no bucket is a defect. The recount builds the first bucket from all items.
			row = this.#repair(routingHash, null, delta);
			if (row === undefined) {
				return null;
			}
		}
		if (oldFacts === null) {
			this.#split(routingHash, key, row);
		} else if (newFacts === null) {
			this.#merge(routingHash, boundOf(row), factsOfRow(row));
		}
		return null;
	}

	/**
	 * Subtracts the facts of deleted items, with one statement for each bucket. An item whose
	 * routingHash has no bucket changes nothing. On a range leaf, each changed bucket then gets the
	 * merge check.
	 */
	subtractRows(rows: Iterable<{ routingHash: RoutingHash; hk: KeyBytes; sk: KeyBytes; facts: ItemFacts }>): void {
		const range = this.#opts.rangeLeaf();
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
		for (const { routingHash, bound, facts } of groups.values()) {
			const delta = subtractFacts(NO_FACTS, facts);
			const row = this.#addAt(routingHash, bound, delta);
			if (row === undefined) {
				continue;
			}
			if (range) {
				this.#checkedRange(routingHash, row, delta);
			} else {
				this.#checkedHash(routingHash, row, delta);
			}
		}
		if (range) {
			// A merge can delete the row of an earlier group. Its items are then in the lower bucket.
			for (const { routingHash, bound } of groups.values()) {
				const current = this.#boundOf(routingHash, bound);
				const facts = current && this.#factsAt(routingHash, current);
				if (current !== undefined && facts !== undefined) {
					this.#merge(routingHash, current, facts);
				}
			}
		}
	}

	/**
	 * Adds the facts of imported items. The items are in storage order, so the items of one
	 * routingHash are next to each other. A hash leaf adds each run of one routingHash to its first
	 * bucket with one statement. When the routingHash has no bucket, it creates the bucket only when the
	 * routingHash then has `m` items or more. A range leaf adds the items to its last bucket until it holds `K`
	 * items, and the next item then starts a new bucket at its own key. It reads no item.
	 */
	addRows(rows: Iterable<{ routingHash: RoutingHash; hk: KeyBytes; sk: KeyBytes; facts: ItemFacts }>): void {
		const range = this.#opts.rangeLeaf();
		const K = this.#opts.bucketItems;
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
			// A full last bucket gets no item: the item starts a new bucket. This also applies to the first
			// item of a page when the last bucket that the target holds is full.
			if (range && run.count >= K) {
				flush(run);
				run = { routingHash, bound: { hk, sk }, stored: false, first: { hk, sk }, count: 0, facts: { ...NO_FACTS } };
			}
			run.count++;
			run.facts = addFacts(run.facts, facts);
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
		const row = tryOne(
			this.#storage.sql.exec<FactsRow>(
				`SELECT ${FACT_COLUMNS.map((c) => `COALESCE(SUM(${c}), 0) AS ${c}`).join(", ")} FROM item_stats WHERE routing_hash = ?1`,
				routingHash,
			),
		);
		return row ? factsOfRow(row) : { ...NO_FACTS };
	}

	/** The buckets of `routingHash` with a bound below `end`, in storage order. A null `end` is unbounded. */
	buckets(routingHash: RoutingHash, end: FokosCompositeKey | null): Bucket[] {
		const below = end === null ? "" : "AND (lower_hk, lower_sk) < (?2, ?3)";
		return this.#storage.sql
			.exec<BucketRow>(
				`SELECT lower_hk, lower_sk, ${FACT_COLUMNS.join(", ")} FROM item_stats
				  WHERE routing_hash = ?1 ${below} ORDER BY lower_hk, lower_sk`,
				routingHash,
				...(end === null ? [] : [end.hk, end.sk]),
			)
			.toArray()
			.map((row) => ({ bound: boundOf(row), facts: factsOfRow(row) }));
	}

	/**
	 * One hash key that has items for each of up to `limit` routingHashes whose stored
	 * `promotion_est_bytes` is at or above `threshold`, largest first. It reads at most `limit` entries
	 * of `item_stats_promotion` and makes at most `limit` primary-key seeks of the item table.
	 */
	largestAtLeast(threshold: number, limit: number): KeyBytes[] {
		const { table, hkColumn, skColumn } = this.#opts;
		const routingHashes = this.#storage.sql
			.exec<{
				routing_hash: ArrayBuffer;
			}>(`SELECT routing_hash FROM item_stats WHERE promotion_est_bytes >= ?1 ORDER BY promotion_est_bytes DESC LIMIT ?2`, threshold, limit)
			.toArray();
		const keys: KeyBytes[] = [];
		for (const { routing_hash } of routingHashes) {
			const row = tryOne(
				this.#storage.sql.exec<{ hk: ArrayBuffer }>(
					`SELECT ${hkColumn} AS hk FROM ${table} WHERE routing_hash = ?1 ORDER BY ${hkColumn}, ${skColumn} LIMIT 1`,
					routing_hash,
				),
			);
			if (!row) {
				// The bucket has no items. The lookup skips it and reads no more entries, so its bound stays.
				this.#logDefect(new Uint8Array(routing_hash), FIRST_BOUND, "a bucket of the promotion index has no items", {});
				continue;
			}
			keys.push(KeyCodec.asKeyBytes(new Uint8Array(row.hk)));
		}
		return keys;
	}

	/** The rounded estimate of the first bucket of `routingHash`, or null when it has none. One primary-key seek. */
	#estimateOf(routingHash: RoutingHash): number | null {
		const row = tryOne(
			this.#storage.sql.exec<{ est: number }>(
				`SELECT ${estSql(this.model, FACT_COLUMNS)} AS est FROM item_stats
				  WHERE routing_hash = ?1 AND (lower_hk, lower_sk) = (x'', x'')`,
				routingHash,
			),
		);
		return row?.est ?? null;
	}

	/** The bound of the bucket of `key`: the greatest bound at or below it. One primary-key seek. */
	#boundOf(routingHash: RoutingHash, key: FokosCompositeKey): FokosCompositeKey | undefined {
		const row = tryOne(
			this.#storage.sql.exec<{ lower_hk: ArrayBuffer; lower_sk: ArrayBuffer }>(
				`SELECT lower_hk, lower_sk FROM item_stats
				  WHERE routing_hash = ?1 AND (lower_hk, lower_sk) <= (?2, ?3)
				  ORDER BY lower_hk DESC, lower_sk DESC LIMIT 1`,
				routingHash,
				key.hk,
				key.sk,
			),
		);
		return row && boundOf(row);
	}

	/** The bucket with the greatest bound of `routingHash`. One primary-key seek. */
	#lastBucket(routingHash: RoutingHash): BucketRow | undefined {
		return tryOne(
			this.#storage.sql.exec<BucketRow>(
				`SELECT lower_hk, lower_sk, ${FACT_COLUMNS.join(", ")} FROM item_stats
				  WHERE routing_hash = ?1 ORDER BY lower_hk DESC, lower_sk DESC LIMIT 1`,
				routingHash,
			),
		);
	}

	/** The next or the previous bucket of the bucket at `bound`. One primary-key seek. */
	#neighbour(routingHash: RoutingHash, bound: FokosCompositeKey, direction: "next" | "previous"): BucketRow | undefined {
		const [op, order] = direction === "next" ? [">", "ASC"] : ["<", "DESC"];
		return tryOne(
			this.#storage.sql.exec<BucketRow>(
				`SELECT lower_hk, lower_sk, ${FACT_COLUMNS.join(", ")} FROM item_stats
				  WHERE routing_hash = ?1 AND (lower_hk, lower_sk) ${op} (?2, ?3)
				  ORDER BY lower_hk ${order}, lower_sk ${order} LIMIT 1`,
				routingHash,
				bound.hk,
				bound.sk,
			),
		);
	}

	/**
	 * Adds `delta` to the bucket of `key` on a range leaf: the bucket with the greatest bound at or below
	 * `key`. One statement. It returns the new row, or undefined when the routingHash has no bucket.
	 */
	#seekAdd(routingHash: RoutingHash, key: FokosCompositeKey, delta: ItemFacts): WrittenRow | undefined {
		return this.#add(
			routingHash,
			key,
			delta,
			`(SELECT lower_hk, lower_sk FROM item_stats
			   WHERE routing_hash = ?1 AND (lower_hk, lower_sk) <= (?2, ?3)
			   ORDER BY lower_hk DESC, lower_sk DESC LIMIT 1)`,
		);
	}

	/** Adds `delta` to the bucket at `bound`. It returns the new row, or undefined when the bucket is absent. */
	#addAt(routingHash: RoutingHash, bound: FokosCompositeKey, delta: ItemFacts): WrittenRow | undefined {
		return this.#add(routingHash, bound, delta, "(?2, ?3)");
	}

	#add(routingHash: RoutingHash, key: FokosCompositeKey, delta: ItemFacts, target: string): WrittenRow | undefined {
		const added = FACT_COLUMNS.map((c, i) => `${c} + ${FACT_PARAMS[i]}`) as unknown as FactsSql;
		return tryOne(
			this.#storage.sql.exec<WrittenRow>(
				`UPDATE item_stats SET ${FACT_COLUMNS.map((c, i) => `${c} = ${added[i]}`).join(", ")},
				        promotion_est_bytes = ${promotionEstSql(this.model, added, FLOOR_PARAM)}
				  WHERE routing_hash = ?1 AND (lower_hk, lower_sk) = ${target}
				 ${this.#returning}`,
				routingHash,
				key.hk,
				key.sk,
				...factValues(delta),
				this.#opts.promotionFloor(),
			),
		);
	}

	/** Adds `facts` to the bucket at `bound`, and creates the bucket with `facts` when it is absent. */
	#upsertAt(routingHash: RoutingHash, bound: FokosCompositeKey, facts: ItemFacts): WrittenRow {
		return this.#write(routingHash, bound, facts, "add");
	}

	/** Sets the facts of the bucket at `bound` to `facts`, and creates the bucket when it is absent. */
	#putAt(routingHash: RoutingHash, bound: FokosCompositeKey, facts: ItemFacts): WrittenRow {
		return this.#write(routingHash, bound, facts, "set");
	}

	#write(routingHash: RoutingHash, bound: FokosCompositeKey, facts: ItemFacts, mode: "add" | "set"): WrittenRow {
		const merged = FACT_COLUMNS.map((c) => (mode === "add" ? `${c} + excluded.${c}` : `excluded.${c}`)) as unknown as FactsSql;
		return one(
			this.#storage.sql.exec<WrittenRow>(
				`INSERT INTO item_stats (routing_hash, lower_hk, lower_sk, ${FACT_COLUMNS.join(", ")}, promotion_est_bytes)
				 VALUES (?1, ?2, ?3, ${FACT_PARAMS.join(", ")}, ${promotionEstSql(this.model, FACT_PARAMS, FLOOR_PARAM)})
				 ON CONFLICT (routing_hash, lower_hk, lower_sk) DO UPDATE SET
				   ${FACT_COLUMNS.map((c, i) => `${c} = ${merged[i]}`).join(", ")},
				   promotion_est_bytes = ${promotionEstSql(this.model, merged, FLOOR_PARAM)}
				 ${this.#returning}`,
				routingHash,
				bound.hk,
				bound.sk,
				...factValues(facts),
				this.#opts.promotionFloor(),
			),
		);
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
		const row = tryOne(
			this.#storage.sql.exec<FactsRow>(
				`SELECT ${FACT_COLUMNS.join(", ")} FROM item_stats WHERE routing_hash = ?1 AND (lower_hk, lower_sk) = (?2, ?3)`,
				routingHash,
				bound.hk,
				bound.sk,
			),
		);
		return row && factsOfRow(row);
	}

	/**
	 * The facts of the items of `routingHash` in `[from, to)`, counted from the primary key. A null `to`
	 * is unbounded. With `limit`, it counts at most `limit` items.
	 */
	#count(routingHash: RoutingHash, from: FokosCompositeKey, to: FokosCompositeKey | null, limit?: number): ItemFacts {
		const opts = this.#opts;
		const below = to === null ? "" : `AND (${opts.hkColumn}, ${opts.skColumn}) < (?4, ?5)`;
		const where = `routing_hash = ?1 AND (${opts.hkColumn}, ${opts.skColumn}) >= (?2, ?3) ${below}`;
		// The limited form selects only the key columns, `item_bytes`, and the index flag, so it reads no data.
		const [source, hkColumn, skColumn, indexedCondition] =
			limit === undefined
				? [`${opts.table} WHERE ${where}`, opts.hkColumn, opts.skColumn, opts.indexedCondition]
				: [
						`(SELECT ${opts.hkColumn} AS hk, ${opts.skColumn} AS sk, item_bytes, (${opts.indexedCondition}) AS indexed
						    FROM ${opts.table} WHERE ${where} LIMIT ?${to === null ? 4 : 6})`,
						"hk",
						"sk",
						"indexed",
					];
		const keyBytes = `octet_length(${hkColumn}) + octet_length(${skColumn})`;
		return factsOfRow(
			one(
				this.#storage.sql.exec<FactsRow>(
					`SELECT COUNT(*) AS item_count,
					        COALESCE(SUM(item_bytes), 0) AS item_bytes,
					        COALESCE(SUM(${keyBytes}), 0) AS key_bytes,
					        COALESCE(SUM(CASE WHEN ${indexedCondition} THEN 1 ELSE 0 END), 0) AS indexed_item_count,
					        COALESCE(SUM(CASE WHEN ${indexedCondition} THEN ${keyBytes} ELSE 0 END), 0) AS indexed_key_bytes,
					        COALESCE(SUM(${overflowBytesSql(this.model, "item_bytes")}), 0) AS overflow_bytes
					   FROM ${source}`,
					routingHash,
					from.hk,
					from.sk,
					...(to === null ? [] : [to.hk, to.sk]),
					...(limit === undefined ? [] : [limit]),
				),
			),
		);
	}

	/** The key of the item at `offset` in `[from, to)` of `routingHash`, in key order. One primary-key seek. */
	#itemAt(routingHash: RoutingHash, from: FokosCompositeKey, to: FokosCompositeKey | null, offset: number): FokosCompositeKey | undefined {
		const { table, hkColumn, skColumn } = this.#opts;
		const below = to === null ? "" : `AND (${hkColumn}, ${skColumn}) < (?4, ?5)`;
		const row = tryOne(
			this.#storage.sql.exec<{ hk: ArrayBuffer; sk: ArrayBuffer }>(
				`SELECT ${hkColumn} AS hk, ${skColumn} AS sk FROM ${table}
				  WHERE routing_hash = ?1 AND (${hkColumn}, ${skColumn}) >= (?2, ?3) ${below}
				  ORDER BY ${hkColumn}, ${skColumn} LIMIT 1 OFFSET ?${to === null ? 4 : 6}`,
				routingHash,
				from.hk,
				from.sk,
				...(to === null ? [] : [to.hk, to.sk]),
				offset,
			),
		);
		return row && { hk: KeyCodec.asKeyBytes(new Uint8Array(row.hk)), sk: KeyCodec.asKeyBytes(new Uint8Array(row.sk)) };
	}

	/**
	 * After an insert on a hash leaf into a routingHash with no bucket: creates the first bucket when
	 * the routingHash now has `m` items or more. Such a routingHash has at most `BUCKET_THRESHOLD_MAX`
	 * items, so the count and the facts read each of its items. More items is a defect, and the recount
	 * of the whole routingHash creates the bucket.
	 */
	#createFirst(routingHash: RoutingHash, delta: ItemFacts): WrittenRow | undefined {
		const count = one(
			this.#storage.sql.exec<{ n: number }>(
				`SELECT COUNT(*) AS n FROM (SELECT 1 FROM ${this.#opts.table} WHERE routing_hash = ?1 LIMIT ?2)`,
				routingHash,
				BUCKET_THRESHOLD_MAX + 1,
			),
		).n;
		if (count > BUCKET_THRESHOLD_MAX) {
			return this.#repair(routingHash, null, delta);
		}
		if (count < bucketThreshold(this.model, this.#opts.promotionFloor())) {
			return undefined;
		}
		return this.#putAt(routingHash, FIRST_BOUND, this.#count(routingHash, FIRST_BOUND, null, BUCKET_THRESHOLD_MAX + 1));
	}

	/**
	 * After an import page on a hash leaf into a routingHash with no bucket: creates the first bucket
	 * when the items of the earlier pages and of this page are `m` or more. The earlier items are below
	 * `first`, the first item of the page, and they are fewer than `BUCKET_THRESHOLD_MAX`. More items is
	 * a defect, and the recount of the whole routingHash creates the bucket.
	 */
	#importFirst(routingHash: RoutingHash, first: FokosCompositeKey, facts: ItemFacts): void {
		const earlier = this.#count(routingHash, FIRST_BOUND, first, BUCKET_THRESHOLD_MAX + 1);
		if (earlier.itemCount > BUCKET_THRESHOLD_MAX) {
			this.#repair(routingHash, null, facts);
		} else if (earlier.itemCount + facts.itemCount >= bucketThreshold(this.model, this.#opts.promotionFloor())) {
			this.#putAt(routingHash, FIRST_BOUND, addFacts(earlier, facts));
		}
	}

	/**
	 * After an insert on a range leaf: while the bucket of `key` holds more than `2K` items, splits it at
	 * the item in its middle. Both halves are counted from the items, so a split also repairs drift.
	 */
	#split(routingHash: RoutingHash, key: FokosCompositeKey, row: WrittenRow): void {
		let bucket = row;
		while (bucket.item_count > 2 * this.#opts.bucketItems) {
			const lower = boundOf(bucket);
			const next = this.#neighbour(routingHash, lower, "next");
			const upper = next ? boundOf(next) : null;
			const middle = this.#itemAt(routingHash, lower, upper, Math.floor(bucket.item_count / 2));
			if (middle === undefined) {
				// The bucket holds fewer items than its count. The recount sets the true count, and the
				// loop splits again only when that count is still above 2K.
				bucket = this.#repairRange(routingHash, lower, factsOfRow(bucket), NO_FACTS);
				continue;
			}
			const lowerRow = this.#putAt(routingHash, lower, this.#count(routingHash, lower, middle));
			const upperRow = this.#putAt(routingHash, middle, this.#count(routingHash, middle, upper));
			bucket = KeyCodec.compareComposite(key, middle) >= 0 ? upperRow : lowerRow;
		}
	}

	/**
	 * After a delete on a range leaf: while the bucket at `bound` holds fewer than `K` items and it holds
	 * fewer than `K` items together with a neighbour, adds the upper bucket of the pair to the lower one
	 * and deletes the upper row. It selects the smaller neighbour. A bucket with no items that stays then
	 * goes, except the first bucket while other buckets follow it.
	 */
	#merge(routingHash: RoutingHash, bound: FokosCompositeKey, bucketFacts: ItemFacts): void {
		const K = this.#opts.bucketItems;
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
		if (facts.itemCount === 0 && (!isFirstBound(current) || this.#neighbour(routingHash, current, "next") === undefined)) {
			this.#deleteAt(routingHash, current);
		}
	}

	/** Adds the facts of the upper bucket to the lower bucket and deletes the upper row. */
	#mergePair(routingHash: RoutingHash, lower: FokosCompositeKey, upper: FokosCompositeKey, upperFacts: ItemFacts): WrittenRow | undefined {
		const merged = this.#addAt(routingHash, lower, upperFacts);
		this.#deleteAt(routingHash, upper);
		return merged;
	}

	/**
	 * Checks the facts that a write on a hash leaf returned. Facts that cannot be true start the repair.
	 * A bucket with no item goes, because the routingHash then has no items. It returns the estimate
	 * after the write, or null when the bucket went.
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
		return this.#putAt(routingHash, bound, this.#count(routingHash, bound, next ? boundOf(next) : null));
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
		const counted = this.#count(routingHash, FIRST_BOUND, null);
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

/** True when facts cannot be true: a negative fact, or other facts with no item. */
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

function factsOfRow(row: FactsRow): ItemFacts {
	return {
		itemCount: row.item_count,
		itemBytes: row.item_bytes,
		keyBytes: row.key_bytes,
		indexedItemCount: row.indexed_item_count,
		indexedKeyBytes: row.indexed_key_bytes,
		overflowBytes: row.overflow_bytes,
	};
}

function factValues(f: ItemFacts): number[] {
	return FACT_NAMES.map((name) => f[name]);
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
