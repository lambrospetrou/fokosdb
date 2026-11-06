import { SQLSchemaMigrations, type SQLSchemaMigration } from "durable-utils/sql-migrations";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import type { RoutingHash } from "../../sharding/hash-primitives.js";
import { tryOne } from "../sql-cursor.js";
import {
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
		// first bucket of a routingHash has the bound (x'', x''), the smallest composite key. Each
		// partition keeps only the first bucket of each routingHash that has items.
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

/** The fact columns of `item_stats`, in the order of `FACT_NAMES`. */
const FACT_COLUMNS = ["item_count", "item_bytes", "key_bytes", "indexed_item_count", "indexed_key_bytes", "overflow_bytes"] as const;

type FactsRow = { item_count: number; item_bytes: number; key_bytes: number; indexed_item_count: number; indexed_key_bytes: number; overflow_bytes: number };

/** The bound of the first bucket of a routingHash: the smallest composite key. */
const FIRST_BOUND = "x'', x''";

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
	 * statement that writes facts binds it when it runs.
	 */
	promotionFloor: () => number | null;
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
		this.#returning = `RETURNING ${FACT_COLUMNS.join(", ")}, ${estSql(opts.model, FACT_COLUMNS)} AS est`;
	}

	runMigrations(): void {
		this.#migrations.runAllSync();
	}

	/**
	 * Applies the change of one item from `oldFacts` to `newFacts` to the bucket of its routingHash.
	 * `oldFacts` is null for a new item, and `newFacts` is null for a deleted item. It returns the
	 * rounded estimate of the bucket after the change, or null when the routingHash has no bucket.
	 */
	change(routingHash: RoutingHash, oldFacts: ItemFacts | null, newFacts: ItemFacts | null): number | null {
		if (oldFacts === null && newFacts === null) {
			return null;
		}
		const delta = subtractFacts(newFacts ?? NO_FACTS, oldFacts ?? NO_FACTS);
		if (FACT_NAMES.every((name) => delta[name] === 0)) {
			// An update writes the row and its index entry also when the values stay the same.
			return this.#estimateOf(routingHash);
		}
		if (oldFacts === null) {
			return this.#checked(routingHash, this.#upsert(routingHash, delta), delta);
		}
		const row = this.#update(routingHash, delta);
		if (row) {
			return this.#checked(routingHash, row, delta);
		}
		// A delete of an item that its routingHash has no bucket for changes no facts: the bucket did
		// not represent the item. A present item that stays with no bucket is a defect.
		if (newFacts === null) {
			return null;
		}
		return this.#repair(routingHash, null, delta);
	}

	/**
	 * Subtracts the facts of deleted items of `routingHash`, one statement for the group. A routingHash
	 * with no bucket changes nothing.
	 */
	subtract(routingHash: RoutingHash, facts: ItemFacts): void {
		const delta = subtractFacts(NO_FACTS, facts);
		const row = this.#update(routingHash, delta);
		if (row) {
			this.#checked(routingHash, row, delta);
		}
	}

	/**
	 * Adds the facts of imported items, with one statement for each run of one routingHash. A
	 * migration page is in storage order, so the items of one routingHash are next to each other.
	 */
	addRows(rows: Iterable<{ routingHash: RoutingHash; facts: ItemFacts }>): void {
		let run: { routingHash: RoutingHash; facts: ItemFacts } | null = null;
		for (const { routingHash, facts } of rows) {
			if (run !== null && KeyCodec.compare(KeyCodec.asKeyBytes(run.routingHash), KeyCodec.asKeyBytes(routingHash)) === 0) {
				run.facts = addFacts(run.facts, facts);
				continue;
			}
			if (run !== null) {
				this.#checked(run.routingHash, this.#upsert(run.routingHash, run.facts), run.facts);
			}
			run = { routingHash, facts };
		}
		if (run !== null) {
			this.#checked(run.routingHash, this.#upsert(run.routingHash, run.facts), run.facts);
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

	/**
	 * One hash key that has items for each of up to `limit` routingHashes whose stored
	 * `promotion_est_bytes` is at or above `threshold`, largest first. It reads at most `limit` entries
	 * of `item_stats_promotion` and makes at most `limit` primary-key seeks of the item table.
	 */
	largestAtLeast(threshold: number, limit: number): KeyBytes[] {
		const { table, hkColumn, skColumn } = this.#opts;
		const routingHashes = this.#storage.sql
			.exec<{ routing_hash: ArrayBuffer }>(
				`SELECT routing_hash FROM item_stats WHERE promotion_est_bytes >= ?1 ORDER BY promotion_est_bytes DESC LIMIT ?2`,
				threshold,
				limit,
			)
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
				this.#logDefect(new Uint8Array(routing_hash), "a bucket of the promotion index has no items", {});
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
				  WHERE routing_hash = ?1 AND (lower_hk, lower_sk) = (${FIRST_BOUND})`,
				routingHash,
			),
		);
		return row?.est ?? null;
	}

	/** Adds `facts` to the first bucket of `routingHash`, and creates the bucket with `facts` when it is absent. */
	#upsert(routingHash: RoutingHash, facts: ItemFacts): FactsRow & { est: number } {
		const bound: FactsSql = ["?2", "?3", "?4", "?5", "?6", "?7"];
		const added = FACT_COLUMNS.map((c) => `${c} + excluded.${c}`) as unknown as FactsSql;
		return this.#storage.sql
			.exec<FactsRow & { est: number }>(
				`INSERT INTO item_stats (routing_hash, lower_hk, lower_sk, ${FACT_COLUMNS.join(", ")}, promotion_est_bytes)
				 VALUES (?1, ${FIRST_BOUND}, ${bound.join(", ")}, ${promotionEstSql(this.model, bound, "?8")})
				 ON CONFLICT (routing_hash, lower_hk, lower_sk) DO UPDATE SET
				   ${FACT_COLUMNS.map((c, i) => `${c} = ${added[i]}`).join(", ")},
				   promotion_est_bytes = ${promotionEstSql(this.model, added, "?8")}
				 ${this.#returning}`,
				routingHash,
				...factValues(facts),
				this.#opts.promotionFloor(),
			)
			.one();
	}

	/** Adds `delta` to the first bucket of `routingHash`. It returns the new facts, or undefined when the bucket is absent. */
	#update(routingHash: RoutingHash, delta: ItemFacts): (FactsRow & { est: number }) | undefined {
		const added = FACT_COLUMNS.map((c, i) => `${c} + ?${i + 2}`) as unknown as FactsSql;
		return tryOne(
			this.#storage.sql.exec<FactsRow & { est: number }>(
				`UPDATE item_stats SET ${FACT_COLUMNS.map((c, i) => `${c} = ${added[i]}`).join(", ")},
				        promotion_est_bytes = ${promotionEstSql(this.model, added, "?8")}
				  WHERE routing_hash = ?1 AND (lower_hk, lower_sk) = (${FIRST_BOUND})
				 ${this.#returning}`,
				routingHash,
				...factValues(delta),
				this.#opts.promotionFloor(),
			),
		);
	}

	/**
	 * Checks the facts that a write returned. Facts that cannot be true start the repair. A bucket
	 * with no item goes, because the routingHash then has no items. It returns the estimate after the
	 * write, or null when the bucket went.
	 */
	#checked(routingHash: RoutingHash, row: FactsRow & { est: number }, delta: ItemFacts): number | null {
		const facts = factsOfRow(row);
		const impossible = FACT_NAMES.some((name) => facts[name] < 0) || (facts.itemCount === 0 && FACT_NAMES.some((name) => facts[name] !== 0));
		if (impossible) {
			return this.#repair(routingHash, subtractFacts(facts, delta), delta);
		}
		if (facts.itemCount === 0) {
			this.remove(routingHash);
			return null;
		}
		return row.est;
	}

	/**
	 * Logs the defect one time, counts the facts of the bucket again from its items, and writes them,
	 * or deletes the bucket when it has no items. The item change has already run, so the count holds it.
	 */
	#repair(routingHash: RoutingHash, stored: ItemFacts | null, delta: ItemFacts): number | null {
		this.#logDefect(routingHash, "the facts of a bucket disagree with its items, so they are counted again", { stored, delta });
		const { table, hkColumn, skColumn, indexedCondition } = this.#opts;
		const keyBytes = `octet_length(${hkColumn}) + octet_length(${skColumn})`;
		const counted = this.#storage.sql
			.exec<FactsRow>(
				`SELECT COUNT(*) AS item_count,
				        COALESCE(SUM(item_bytes), 0) AS item_bytes,
				        COALESCE(SUM(${keyBytes}), 0) AS key_bytes,
				        COALESCE(SUM(CASE WHEN ${indexedCondition} THEN 1 ELSE 0 END), 0) AS indexed_item_count,
				        COALESCE(SUM(CASE WHEN ${indexedCondition} THEN ${keyBytes} ELSE 0 END), 0) AS indexed_key_bytes,
				        COALESCE(SUM(${overflowBytesSql(this.model, "item_bytes")}), 0) AS overflow_bytes
				   FROM ${table} WHERE routing_hash = ?1`,
				routingHash,
			)
			.one();
		if (counted.item_count === 0) {
			this.remove(routingHash);
			return null;
		}
		const bound: FactsSql = ["?2", "?3", "?4", "?5", "?6", "?7"];
		return this.#storage.sql
			.exec<FactsRow & { est: number }>(
				`INSERT INTO item_stats (routing_hash, lower_hk, lower_sk, ${FACT_COLUMNS.join(", ")}, promotion_est_bytes)
				 VALUES (?1, ${FIRST_BOUND}, ${bound.join(", ")}, ${promotionEstSql(this.model, bound, "?8")})
				 ON CONFLICT (routing_hash, lower_hk, lower_sk) DO UPDATE SET
				   ${FACT_COLUMNS.map((c) => `${c} = excluded.${c}`).join(", ")},
				   promotion_est_bytes = excluded.promotion_est_bytes
				 ${this.#returning}`,
				routingHash,
				...factValues(factsOfRow(counted)),
				this.#opts.promotionFloor(),
			)
			.one().est;
	}

	#logDefect(routingHash: Uint8Array, message: string, detail: Record<string, unknown>): void {
		console.error({
			...this.#opts.logParams(),
			message: `${this.#opts.logName}: ${message}`,
			routingHash: routingHash.toHex(),
			bucket: { lowerHk: "", lowerSk: "" },
			...detail,
		});
	}
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
