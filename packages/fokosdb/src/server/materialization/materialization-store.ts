/**
 * The SQLite storage of `MaterializationDO`: the latest image of each base item that the partition
 * accepted, the size estimate of each base hash key, and the highest epoch of each producer.
 */
import { SQLSchemaMigrations, type SQLSchemaMigration } from "durable-utils/sql-migrations";
import { DATA_KINDS, type DataKind } from "../../shared/types.js";
import type { KeyBytes } from "../../sharding/key-codec.js";
import { tryOne } from "../../shared/sql-cursor.js";
import { EST_ROW_BYTES_K, estRowBytesExpr } from "../../shared/partition/item-size.js";

/**
 * One image that a producer exports. A live image carries the stored representation of the base item
 * (a json item as its JSONB blob), so the store writes it unchanged. A delete carries no data.
 */
export type LatestImage =
	| {
			opIndex: number;
			hashKey: KeyBytes;
			sortKey: KeyBytes;
			revision: number;
			deleted: false;
			data: string | Uint8Array;
			kind: DataKind;
			ttlAt: number | null;
	  }
	| { opIndex: number; hashKey: KeyBytes; sortKey: KeyBytes; revision: number; deleted: true };

const sqlMigrations: SQLSchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: "Create the latest images, the key size estimates, and the producer fences",
		// latest_items:
		// - One row for each base item: its newest live image, or a tombstone (`deleted_at_ms` is not
		//   NULL, `data` is NULL). `revision` is the base revision of the image.
		// - A rowid table with `data` last, for the reasons of the items table of PartitionStore: a large
		//   payload does not get a private overflow page, and a read of the metadata columns reads no
		//   overflow page.
		// - `item_id` links the rows of other tables to the item, so `(base_hk, base_sk)` is stored one
		//   time. Each statement that deletes a row must delete its linked rows in the same transaction,
		//   because SQLite can give the id to a new row again.
		// - No rule reads the value of `deleted_at_ms`. It orders the garbage collection of tombstones.
		//
		// producer_fences:
		// - The highest epoch of each producer, by its DurableObjectId in hex. A call with a lower epoch
		//   applies nothing. A row is never deleted: an absent row has the epoch 0, so a late call of that
		//   producer would apply again.
		sql: `
            CREATE TABLE IF NOT EXISTS latest_items (
                item_id       INTEGER PRIMARY KEY,
                base_hk       BLOB    NOT NULL,
                base_sk       BLOB    NOT NULL DEFAULT x'',
                revision      INTEGER NOT NULL,
                deleted_at_ms INTEGER,
                newest_seq    INTEGER,
                data_kind     INTEGER,
                ttl_at        INTEGER,
                est_row_bytes INTEGER NOT NULL DEFAULT 0,
                data          ANY,
                UNIQUE (base_hk, base_sk)
            ) STRICT;

            CREATE INDEX IF NOT EXISTS idx_latest_items_deleted_gc ON latest_items (deleted_at_ms) WHERE deleted_at_ms IS NOT NULL;

            CREATE TABLE IF NOT EXISTS key_size_estimates (
                hk        BLOB    PRIMARY KEY,
                est_bytes INTEGER NOT NULL
            ) WITHOUT ROWID, STRICT;

            CREATE TABLE IF NOT EXISTS producer_fences (
                producer_id TEXT    PRIMARY KEY,
                epoch       INTEGER NOT NULL
            ) WITHOUT ROWID, STRICT;`,
	},
];

export class MaterializationStore {
	#storage: DurableObjectStorage;
	#migrations: SQLSchemaMigrations;

	constructor(storage: DurableObjectStorage) {
		this.#storage = storage;
		this.#migrations = new SQLSchemaMigrations({
			migrations: sqlMigrations,
			doStorage: storage,
			keyNameTrackingLastMigrationID: "__fokosdb/mat/sql_schema_version",
		});
	}

	runMigrations(): void {
		this.#migrations.runAllSync();
	}

	get databaseSize(): number {
		return this.#storage.sql.databaseSize;
	}

	transactionSync<T>(fn: () => T): T {
		return this.#storage.transactionSync(fn);
	}

	/** The stored epoch of a producer, or 0 when the producer never called. */
	producerEpoch(producerId: string): number {
		return (
			tryOne(this.#storage.sql.exec<{ epoch: number }>(`SELECT epoch FROM producer_fences WHERE producer_id = ?`, producerId))?.epoch ?? 0
		);
	}

	/**
	 * Raises the stored epoch of a producer to `epoch` when `epoch` is higher. It never lowers it. Keep
	 * the `WHERE` of the update: without it, the update writes the row (one billed row) also when the
	 * value does not change, for example `SET epoch = MAX(epoch, excluded.epoch)`.
	 */
	raiseProducerEpoch(producerId: string, epoch: number): void {
		this.#storage.sql.exec(
			`INSERT INTO producer_fences (producer_id, epoch) VALUES (?, ?)
			 ON CONFLICT (producer_id) DO UPDATE SET epoch = excluded.epoch WHERE excluded.epoch > producer_fences.epoch`,
			producerId,
			epoch,
		);
	}

	/** The revision and the row size of the stored image of a base item, live or tombstone, or undefined when it has none. */
	storedImage(hk: KeyBytes, sk: KeyBytes): { revision: number; estRowBytes: number } | undefined {
		const row = tryOne(
			this.#storage.sql.exec<{ revision: number; est_row_bytes: number }>(
				`SELECT revision, est_row_bytes FROM latest_items WHERE base_hk = ? AND base_sk = ?`,
				hk,
				sk,
			),
		);
		return row && { revision: row.revision, estRowBytes: row.est_row_bytes };
	}

	/**
	 * Stores an image that is newer than the stored one. `oldEstRowBytes` is the row size of the stored
	 * image, or 0 when there is none. A live image replaces the data. A delete keeps the row as a
	 * tombstone with no data, so the space of the payload is free at once, and the tombstone holds the
	 * revision that a late export must not go below. The size estimate of the base hash key changes by
	 * the difference of the two row sizes.
	 */
	putLatestImage(image: LatestImage, nowMs: number, oldEstRowBytes: number): void {
		const sql = this.#storage.sql;
		const row = image.deleted
			? sql.exec<{ est_row_bytes: number }>(
					`INSERT INTO latest_items (base_hk, base_sk, revision, deleted_at_ms, data_kind, ttl_at, est_row_bytes, data)
					 VALUES (?1, ?2, ?3, ?4, NULL, NULL, octet_length(?1) + octet_length(?2) + ${EST_ROW_BYTES_K}, NULL)
					 ON CONFLICT (base_hk, base_sk) DO UPDATE SET
					   revision = excluded.revision, deleted_at_ms = excluded.deleted_at_ms, data_kind = NULL, ttl_at = NULL,
					   est_row_bytes = excluded.est_row_bytes, data = NULL
					 RETURNING est_row_bytes`,
					image.hashKey,
					image.sortKey,
					image.revision,
					nowMs,
				)
			: sql.exec<{ est_row_bytes: number }>(
					`INSERT INTO latest_items (base_hk, base_sk, revision, deleted_at_ms, data_kind, ttl_at, est_row_bytes, data)
					 VALUES (?1, ?2, ?3, NULL, ?4, ?5, ${estRowBytesExpr("?6", "?1", "?2")}, ?6)
					 ON CONFLICT (base_hk, base_sk) DO UPDATE SET
					   revision = excluded.revision, deleted_at_ms = NULL, data_kind = excluded.data_kind, ttl_at = excluded.ttl_at,
					   est_row_bytes = excluded.est_row_bytes, data = excluded.data
					 RETURNING est_row_bytes`,
					image.hashKey,
					image.sortKey,
					image.revision,
					DATA_KINDS.indexOf(image.kind),
					image.ttlAt,
					image.data,
				);
		const newEst = row.one().est_row_bytes;
		sql.exec(
			`INSERT INTO key_size_estimates (hk, est_bytes) VALUES (?, ?)
			 ON CONFLICT (hk) DO UPDATE SET est_bytes = MAX(0, est_bytes + excluded.est_bytes - ?)`,
			image.hashKey,
			newEst,
			oldEstRowBytes,
		);
	}
}
