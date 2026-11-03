import { SQLSchemaMigrations, type SQLSchemaMigration } from "durable-utils/sql-migrations";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import type { HashPrefixBucket } from "../../sharding/hash-primitives.js";
import { tryOne } from "../sql-cursor.js";

const sqlMigrations: SQLSchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: "Create the size estimate of each hashPrefixBucket",
		// One row for each storage prefix (hashPrefixBucket) that has items. `est_bytes` is the sum of the
		// row sizes of all hash keys of the prefix, as the host measures them. The host gives the prefix
		// as opaque bytes. The index finds the largest prefixes for the promotion decision.
		//
		// `hk` is one hash key of the prefix, not part of the key: the host promotes by a hash key, and
		// the runtime maps it back to its prefix. A new row stores the hash key of its first item. The
		// hash key can outlive its own items, because its hash still gives the same prefix.
		//
		// TODO: Expand the statistics we hold for each prefix for more accurate decisions.
		//
		sql: `
            CREATE TABLE IF NOT EXISTS key_size_estimates (
                hash_prefix_bucket BLOB    NOT NULL PRIMARY KEY,
                hk                 BLOB    NOT NULL,
                est_bytes          INTEGER NOT NULL DEFAULT 0
            ) WITHOUT ROWID, STRICT;

            CREATE INDEX IF NOT EXISTS key_size_estimates_by_bytes ON key_size_estimates (est_bytes);`,
	},
];

/**
 * The size estimate of each storage prefix of a partition. It owns the `key_size_estimates` table and
 * its schema migrations, which it tracks under its own KV key. Thus it can share one storage with a
 * host that has its own tables and migrations.
 *
 * The host measures the size of each row, and gives each change of a row size here in the
 * transaction of the row write. The estimate of a prefix is then exact: it is the sum of the row sizes
 * of all its hash keys. A prefix with no items has no row.
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
	 * Applies the size change of one row of `hk` from `oldBytes` to `newBytes` to the estimate of
	 * `prefix`, and returns the new estimate. `oldBytes` is 0 for a new row. A row always has a size
	 * above 0, so an equal size means that the estimate row exists. Then it reads the estimate and writes
	 * nothing, because an update writes the row and its index entry also when the value does not change.
	 */
	change(prefix: HashPrefixBucket, hk: KeyBytes, oldBytes: number, newBytes: number): number {
		if (oldBytes === newBytes) {
			return this.bytesOf(prefix);
		}
		const row = tryOne(
			this.#storage.sql.exec<{ est_bytes: number }>(
				`INSERT INTO key_size_estimates (hash_prefix_bucket, hk, est_bytes) VALUES (?4, ?1, ?2)
				 ON CONFLICT(hash_prefix_bucket) DO UPDATE SET est_bytes = MAX(0, est_bytes + excluded.est_bytes - ?3)
				 RETURNING est_bytes`,
				hk,
				newBytes,
				oldBytes,
				prefix,
			),
		);
		return row?.est_bytes ?? newBytes;
	}

	/**
	 * Adds the bytes of new rows of `prefix`. The migration ingest calls it one time for each prefix of
	 * a page, with the sizes of the rows that it inserted. Zero bytes writes nothing, so a page of rows
	 * that are already present costs no write.
	 */
	add(prefix: HashPrefixBucket, hk: KeyBytes, bytes: number): void {
		if (bytes <= 0) {
			return;
		}
		this.#storage.sql.exec(
			`INSERT INTO key_size_estimates (hash_prefix_bucket, hk, est_bytes) VALUES (?3, ?1, ?2)
			 ON CONFLICT(hash_prefix_bucket) DO UPDATE SET est_bytes = est_bytes + excluded.est_bytes`,
			hk,
			bytes,
			prefix,
		);
	}

	/**
	 * Adds the bytes of many new rows, with one `add` for each prefix. A migration page is in storage
	 * order, so the rows of one prefix are next to each other, and a run of rows of one prefix makes one
	 * `add`.
	 */
	addRows(rows: Iterable<{ prefix: HashPrefixBucket; hk: KeyBytes; bytes: number }>): void {
		let run: { prefix: HashPrefixBucket; hk: KeyBytes; bytes: number } | null = null;
		for (const { prefix, hk, bytes } of rows) {
			if (run !== null && KeyCodec.compare(KeyCodec.asKeyBytes(run.prefix), KeyCodec.asKeyBytes(prefix)) === 0) {
				run.bytes += bytes;
				continue;
			}
			if (run !== null) {
				this.add(run.prefix, run.hk, run.bytes);
			}
			run = { prefix, hk, bytes };
		}
		if (run !== null) {
			this.add(run.prefix, run.hk, run.bytes);
		}
	}

	/**
	 * Removes the bytes of deleted rows of `prefix`. When the deleted rows held all the bytes of the
	 * prefix, no item of it remains, so the row and its index entry are deleted. Without this, a
	 * partition keeps one row for each prefix that it ever stored. A prefix that gets a write again gets
	 * a new row.
	 */
	subtract(prefix: HashPrefixBucket, bytes: number): void {
		// `est_bytes <= bytes` is the same test as `est_bytes - bytes <= 0`. We use `<=` and not `=`, so
		// that an estimate that is too low also goes, and does not stay as a row of 0 bytes.
		const removed = this.#storage.sql.exec(
			`DELETE FROM key_size_estimates WHERE hash_prefix_bucket = ?2 AND est_bytes <= ?1`,
			bytes,
			prefix,
		);
		if (removed.rowsWritten === 0) {
			this.#storage.sql.exec(
				`UPDATE key_size_estimates SET est_bytes = MAX(0, est_bytes - ?1) WHERE hash_prefix_bucket = ?2`,
				bytes,
				prefix,
			);
		}
	}

	/** Deletes the estimate of `prefix`. A promotion calls it when the prefix moves out of the partition. */
	remove(prefix: HashPrefixBucket): void {
		this.#storage.sql.exec(`DELETE FROM key_size_estimates WHERE hash_prefix_bucket = ?1`, prefix);
	}

	/** The estimate of `prefix`, or 0 when it has no items. One primary-key lookup. */
	bytesOf(prefix: HashPrefixBucket): number {
		return (
			tryOne(
				this.#storage.sql.exec<{ est_bytes: number }>(`SELECT est_bytes FROM key_size_estimates WHERE hash_prefix_bucket = ?1`, prefix),
			)?.est_bytes ?? 0
		);
	}

	/**
	 * One hash key of each of up to `limit` prefixes whose estimate is at least `minBytes`, largest
	 * first. One seek of `key_size_estimates_by_bytes`, which reads at most `limit` rows.
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
