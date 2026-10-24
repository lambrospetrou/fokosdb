import { SQLSchemaMigrations, type SQLSchemaMigration } from "durable-utils/sql-migrations";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import { tryOne } from "../sql-cursor.js";

const sqlMigrations: SQLSchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: "Create the size estimate of each hash key",
		// One row for each hash key that has items. `est_bytes` is the sum of the row sizes of the key,
		// as the host measures them. The index finds the largest keys for the promotion decision.
		//
		// TODO: Expand the statistics we hold for each key for more accurate decisions.
		//
		sql: `
            CREATE TABLE IF NOT EXISTS key_size_estimates (
                hk        BLOB    NOT NULL PRIMARY KEY,
                est_bytes INTEGER NOT NULL DEFAULT 0
            ) WITHOUT ROWID, STRICT;

            CREATE INDEX IF NOT EXISTS key_size_estimates_by_bytes ON key_size_estimates (est_bytes);`,
	},
];

/**
 * The size estimate of each hash key of a partition. It owns the `key_size_estimates` table and its
 * schema migrations, which it tracks under its own KV key. Thus it can share one storage with a host
 * that has its own tables and migrations.
 *
 * The host measures the size of each row, and gives each change of a row size here in the
 * transaction of the row write. The estimate of a key is then exact: it is the sum of the row sizes
 * of the key. A key with no items has no row.
 */
export class KeySizeStats {
	#storage: DurableObjectStorage;
	#migrations: SQLSchemaMigrations;

	constructor(storage: DurableObjectStorage) {
		this.#storage = storage;
		this.#migrations = new SQLSchemaMigrations({
			migrations: sqlMigrations,
			doStorage: storage,
			keyNameTrackingLastMigrationID: "__fokosdb/key_sizes/sql_schema_version",
		});
	}

	runMigrations(): void {
		this.#migrations.runAllSync();
	}

	/**
	 * Applies the size change of one row of `hk` from `oldBytes` to `newBytes`, and returns the new
	 * estimate of the key. `oldBytes` is 0 for a new row. A row always has a size above 0, so an
	 * equal size means that the estimate row exists. Then it reads the estimate and writes nothing,
	 * because an update writes the row and its index entry also when the value does not change.
	 *
	 * Keep in mind that a row for one `hk` includes many items with different `sk` potentially.
	 */
	change(hk: KeyBytes, oldBytes: number, newBytes: number): number {
		if (oldBytes === newBytes) {
			return this.bytesOf(hk);
		}
		const row = tryOne(
			this.#storage.sql.exec<{ est_bytes: number }>(
				`INSERT INTO key_size_estimates (hk, est_bytes) VALUES (?1, ?2)
				 ON CONFLICT(hk) DO UPDATE SET est_bytes = MAX(0, est_bytes + excluded.est_bytes - ?3)
				 RETURNING est_bytes`,
				hk,
				newBytes,
				oldBytes,
			),
		);
		return row?.est_bytes ?? newBytes;
	}

	/**
	 * Adds the bytes of new rows of `hk`. The migration ingest calls it one time for each key of a
	 * page, with the sizes of the rows that it inserted. Zero bytes writes nothing, so a page of rows
	 * that are already present costs no write.
	 */
	add(hk: KeyBytes, bytes: number): void {
		if (bytes <= 0) {
			return;
		}
		this.#storage.sql.exec(
			`INSERT INTO key_size_estimates (hk, est_bytes) VALUES (?1, ?2)
			 ON CONFLICT(hk) DO UPDATE SET est_bytes = est_bytes + excluded.est_bytes`,
			hk,
			bytes,
		);
	}

	/** Adds the bytes of many new rows, with one `add` for each hash key. */
	addRows(rows: Iterable<{ hk: KeyBytes; bytes: number }>): void {
		// Keyed by the KeyCodec.mapKey hash so no text is built per row. The hash is not an identity:
		// two distinct hash keys can share one value, so a bucket holds every key of one hash and the
		// raw bytes decide which entry a row joins. A bucket has one entry except on a collision.
		const bytesByKey = new Map<bigint, { hk: KeyBytes; bytes: number }[]>();
		for (const { hk, bytes } of rows) {
			const id = KeyCodec.mapKey(hk);
			const bucket = bytesByKey.get(id);
			if (bucket === undefined) {
				bytesByKey.set(id, [{ hk, bytes }]);
				continue;
			}
			const entry = bucket.find((e) => KeyCodec.compare(e.hk, hk) === 0);
			if (entry) {
				entry.bytes += bytes;
			} else {
				bucket.push({ hk, bytes });
			}
		}
		for (const bucket of bytesByKey.values()) {
			for (const { hk, bytes } of bucket) {
				this.add(hk, bytes);
			}
		}
	}

	/**
	 * Removes the bytes of deleted rows of `hk`. When the deleted rows held all the bytes of the key,
	 * no item of the key remains, so the row and its index entry are deleted. Without this, a
	 * partition keeps one row for each key that it ever stored. A key that gets a write again gets a
	 * new row.
	 */
	subtract(hk: KeyBytes, bytes: number): void {
		// `est_bytes <= bytes` is the same test as `est_bytes - bytes <= 0`. We use `<=` and not `=`, so
		// that an estimate that is too low also goes, and does not stay as a row of 0 bytes.
		const removed = this.#storage.sql.exec(`DELETE FROM key_size_estimates WHERE hk = ?1 AND est_bytes <= ?2`, hk, bytes);
		if (removed.rowsWritten === 0) {
			this.#storage.sql.exec(`UPDATE key_size_estimates SET est_bytes = MAX(0, est_bytes - ?1) WHERE hk = ?2`, bytes, hk);
		}
	}

	/** Deletes the estimate of `hk`. A promotion calls it when the key moves out of the partition. */
	remove(hk: KeyBytes): void {
		this.#storage.sql.exec(`DELETE FROM key_size_estimates WHERE hk = ?1`, hk);
	}

	/** The estimate of `hk`, or 0 when the key has no items. */
	bytesOf(hk: KeyBytes): number {
		return (
			tryOne(this.#storage.sql.exec<{ est_bytes: number }>(`SELECT est_bytes FROM key_size_estimates WHERE hk = ?1`, hk))?.est_bytes ?? 0
		);
	}

	/**
	 * Up to `limit` hash keys whose estimate is at least `minBytes`, largest first. One seek of
	 * `key_size_estimates_by_bytes`, which reads at most `limit` rows.
	 */
	largestAtLeast(minBytes: number, limit: number): KeyBytes[] {
		const keys: KeyBytes[] = [];
		for (const row of this.#storage.sql.exec<{ hk: ArrayBuffer }>(
			`SELECT hk FROM key_size_estimates WHERE est_bytes >= ?1 ORDER BY est_bytes DESC LIMIT ?2`,
			minBytes,
			limit,
		)) {
			keys.push(KeyCodec.asKeyBytes(new Uint8Array(row.hk)));
		}
		return keys;
	}
}
