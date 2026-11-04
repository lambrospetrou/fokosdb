import { SQLSchemaMigrations, type SQLSchemaMigration } from "durable-utils/sql-migrations";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import type { RoutingHash } from "../../sharding/hash-primitives.js";
import { tryOne } from "../sql-cursor.js";

const sqlMigrations: SQLSchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: "Create the size estimate of each routingHash",
		// One row for each routingHash that has items. `est_bytes` is the sum of the
		// row sizes of all hash keys of the routingHash, as the host measures them. The host gives the routingHash
		// as opaque bytes. The index finds the largest routingHashes for the promotion decision.
		//
		// `hk` is one hash key of the routingHash, not part of the key: the host promotes by a hash key, and
		// the runtime maps it back to its routingHash. A new row stores the hash key of its first item. The
		// hash key can outlive its own items, because its hash still gives the same routingHash.
		//
		// TODO: Expand the statistics we hold for each routingHash for more accurate decisions.
		//
		sql: `
            CREATE TABLE IF NOT EXISTS key_size_estimates (
                routing_hash BLOB    NOT NULL PRIMARY KEY,
                hk                 BLOB    NOT NULL,
                est_bytes          INTEGER NOT NULL DEFAULT 0
            ) WITHOUT ROWID, STRICT;

            CREATE INDEX IF NOT EXISTS key_size_estimates_by_bytes ON key_size_estimates (est_bytes);`,
	},
];

/**
 * The size estimate of each routingHash of a partition. It owns the `key_size_estimates` table and
 * its schema migrations, which it tracks under its own KV key. Thus it can share one storage with a
 * host that has its own tables and migrations.
 *
 * The host measures the size of each row, and gives each change of a row size here in the
 * transaction of the row write. The estimate of a routingHash is then exact: it is the sum of the row sizes
 * of all its hash keys. A routingHash with no items has no row.
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
	 * `routingHash`, and returns the new estimate. `oldBytes` is 0 for a new row. A row always has a size
	 * above 0, so an equal size means that the estimate row exists. Then it reads the estimate and writes
	 * nothing, because an update writes the row and its index entry also when the value does not change.
	 */
	change(routingHash: RoutingHash, hk: KeyBytes, oldBytes: number, newBytes: number): number {
		if (oldBytes === newBytes) {
			return this.bytesOf(routingHash);
		}
		const row = tryOne(
			this.#storage.sql.exec<{ est_bytes: number }>(
				`INSERT INTO key_size_estimates (routing_hash, hk, est_bytes) VALUES (?4, ?1, ?2)
				 ON CONFLICT(routing_hash) DO UPDATE SET est_bytes = MAX(0, est_bytes + excluded.est_bytes - ?3)
				 RETURNING est_bytes`,
				hk,
				newBytes,
				oldBytes,
				routingHash,
			),
		);
		return row?.est_bytes ?? newBytes;
	}

	/**
	 * Adds the bytes of new rows of `routingHash`. The migration ingest calls it one time for each routingHash of
	 * a page, with the sizes of the rows that it inserted. Zero bytes writes nothing, so a page of rows
	 * that are already present costs no write.
	 */
	add(routingHash: RoutingHash, hk: KeyBytes, bytes: number): void {
		if (bytes <= 0) {
			return;
		}
		this.#storage.sql.exec(
			`INSERT INTO key_size_estimates (routing_hash, hk, est_bytes) VALUES (?3, ?1, ?2)
			 ON CONFLICT(routing_hash) DO UPDATE SET est_bytes = est_bytes + excluded.est_bytes`,
			hk,
			bytes,
			routingHash,
		);
	}

	/**
	 * Adds the bytes of many new rows, with one `add` for each routingHash. A migration page is in storage
	 * order, so the rows of one routingHash are next to each other, and a run of rows of one routingHash makes one
	 * `add`.
	 */
	addRows(rows: Iterable<{ routingHash: RoutingHash; hk: KeyBytes; bytes: number }>): void {
		let run: { routingHash: RoutingHash; hk: KeyBytes; bytes: number } | null = null;
		for (const { routingHash, hk, bytes } of rows) {
			if (run !== null && KeyCodec.compare(KeyCodec.asKeyBytes(run.routingHash), KeyCodec.asKeyBytes(routingHash)) === 0) {
				run.bytes += bytes;
				continue;
			}
			if (run !== null) {
				this.add(run.routingHash, run.hk, run.bytes);
			}
			run = { routingHash, hk, bytes };
		}
		if (run !== null) {
			this.add(run.routingHash, run.hk, run.bytes);
		}
	}

	/**
	 * Removes the bytes of deleted rows of `routingHash`. When the deleted rows held all the bytes of the
	 * routingHash, no item of it remains, so the row and its index entry are deleted. Without this, a
	 * partition keeps one row for each routingHash that it ever stored. A routingHash that gets a write again gets
	 * a new row.
	 */
	subtract(routingHash: RoutingHash, bytes: number): void {
		// `est_bytes <= bytes` is the same test as `est_bytes - bytes <= 0`. We use `<=` and not `=`, so
		// that an estimate that is too low also goes, and does not stay as a row of 0 bytes.
		const removed = this.#storage.sql.exec(
			`DELETE FROM key_size_estimates WHERE routing_hash = ?2 AND est_bytes <= ?1`,
			bytes,
			routingHash,
		);
		if (removed.rowsWritten === 0) {
			this.#storage.sql.exec(
				`UPDATE key_size_estimates SET est_bytes = MAX(0, est_bytes - ?1) WHERE routing_hash = ?2`,
				bytes,
				routingHash,
			);
		}
	}

	/** Deletes the estimate of `routingHash`. A promotion calls it when the routingHash moves out of the partition. */
	remove(routingHash: RoutingHash): void {
		this.#storage.sql.exec(`DELETE FROM key_size_estimates WHERE routing_hash = ?1`, routingHash);
	}

	/** The estimate of `routingHash`, or 0 when it has no items. One primary-key lookup. */
	bytesOf(routingHash: RoutingHash): number {
		return (
			tryOne(this.#storage.sql.exec<{ est_bytes: number }>(`SELECT est_bytes FROM key_size_estimates WHERE routing_hash = ?1`, routingHash))
				?.est_bytes ?? 0
		);
	}

	/**
	 * One hash key of each of up to `limit` routingHashes whose estimate is at least `minBytes`, largest
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
