import { SQLSchemaMigration, SQLSchemaMigrations } from "durable-utils/sql-migrations";
import type { ItemCondition } from "../types.js";
import invariant from "../invariant.js";

/**
 * PartitionStore owns ALL SQL on the partition's tables: items, pending_transactions,
 * deletion_metadata, key_size_estimates, and promoted_keys — plus the schema migrations and the
 * row-size estimators. No other class touches these tables.
 *
 * Design rules (see docs/agent-plans/adr-lib-layering-refactor.md):
 * - Single-purpose methods named for intent; raw SQL is fine because it lives only here.
 * - Multi-statement atomicity is composed by the CALLER via `transactionSync` — the store does
 *   not decide transaction boundaries (mirrors the DO's existing transactionSync blocks).
 * - Row-reading methods return already-converted data (`string | Uint8Array`, never ArrayBuffer).
 * - Methods used to build RPC `meta` return `{ rowsRead, rowsWritten }` for exactly the
 *   statements the DO counted before the extraction (see each method's doc).
 */

// ---------------------------------------------------------------------------
// Row, cursor, and snapshot types
// ---------------------------------------------------------------------------

export type MigratedItem = {
	hk: string;
	sk: string;
	data: string | Uint8Array;
	ttl_epoch_utc_seconds: number | null;
	v: number;
	last_transaction_ts: number;
};

export type PendingTransactionRow = {
	hk: string;
	sk: string;
	transaction_id: string;
	transaction_ts: number;
	operation: string;
	data: string | Uint8Array | null;
	conditions_json: string | null;
	coordinator_do_id: string;
	created_at: number;
};

export type PendingTransactionCursor = { hk: string; sk: string; transaction_id: string };

export type MigrationCursor = { hk: string; sk: string };

export type PromotedKeyCursor = { hashKey: string };

export type PromotedKeyStatus = "queued" | "promoting" | "promoted";

export type PromotedKeyRow = { hash_key: string; status: PromotedKeyStatus };

export type SqlMetrics = { rowsRead: number; rowsWritten: number };

export type ItemSnapshot = { hk: string; sk: string; found: true; v: number } | { hk: string; sk: string; found: false };

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

// Returns the shortest string p where lo < p ≤ hi, treating characters as UTF-16 code units
// (matching JavaScript string comparison and SQLite TEXT collation). Pre-condition: lo < hi.
function shortestSeparator(lo: string, hi: string): string {
	const minLen = Math.min(lo.length, hi.length);
	for (let i = 0; i < minLen; i++) {
		if (lo.charCodeAt(i) !== hi.charCodeAt(i)) {
			// At index i, hi[i] > lo[i]; hi[0..i+1] is the shortest prefix of hi that exceeds lo.
			return hi.substring(0, i + 1);
		}
	}
	// lo is a proper prefix of hi; one extra character makes the result exceed lo.
	return hi.substring(0, lo.length + 1);
}

/**
 * Pure condition evaluation shared by the non-transactional putItem/deleteItem and the
 * transactional prepare path. Lives with the store (NOT the transaction participant) because
 * both paths evaluate the same conditions against the same item snapshot shape.
 */
export function evaluateConditionsOnItem(item: ItemSnapshot, conditions: ItemCondition[], operationName: string): void {
	for (const condition of conditions) {
		if (condition.type === "item_exists") {
			if (!item.found) {
				throw new Error(`fokos/${operationName}: condition "item_exists" failed — item does not exist (hk=${item.hk}, sk=${item.sk})`);
			}
		} else if (condition.type === "item_not_exists") {
			if (item.found) {
				throw new Error(
					`fokos/${operationName}: condition "item_not_exists" failed — item already exists with v=${item.v} (hk=${item.hk}, sk=${item.sk})`,
				);
			}
		} else if (condition.type === "attribute_equals") {
			const actual = item.found ? item[condition.attribute] : null;
			if (actual !== condition.value) {
				throw new Error(
					`fokos/${operationName}: condition "attribute_equals" failed — attribute "${condition.attribute}" expected ${condition.value}, found ${actual} (hk=${item.hk}, sk=${item.sk})`,
				);
			}
		}
	}
}

export function estimateRowBytes(data: string | Uint8Array, hk: string, sk: string): number {
	const dataBytes = typeof data === "string" ? data.length : data.byteLength;
	// hk and sk are variable-length and physically stored in every row (WITHOUT ROWID PK),
	// so a long hk with many sort keys contributes significant storage that must be counted per-row.
	// 40 = fixed overhead: integer columns (v, ttl_epoch_utc_seconds, last_transaction_ts, est_row_bytes ≈ 4×8 = 32 bytes)
	//      + SQLite B-tree record metadata (header varints, null bitmap ≈ 8 bytes).
	return dataBytes + hk.length + sk.length + 40;
}

export function estimateItemBytes(item: MigratedItem): number {
	const dataSize = typeof item.data === "string" ? item.data.length * 2 : item.data.byteLength;
	return item.hk.length * 2 + item.sk.length * 2 + dataSize + 8 + 64;
}

export function estimatePendingTxBytes(row: PendingTransactionRow): number {
	const dataSize = row.data == null ? 0 : typeof row.data === "string" ? row.data.length * 2 : row.data.byteLength;
	return row.hk.length * 2 + row.sk.length * 2 + 32 + 8 + 8 + dataSize + (row.conditions_json?.length ?? 0) * 2 + 64;
}

/**
 * SQLite returns BLOB columns as ArrayBuffer; the public API speaks `string | Uint8Array`.
 * Every store row-reading method funnels data columns through this.
 */
export function fromSqlData(value: string | ArrayBuffer): string | Uint8Array;
export function fromSqlData(value: string | ArrayBuffer | null): string | Uint8Array | null;
export function fromSqlData(value: string | ArrayBuffer | null): string | Uint8Array | null {
	if (value === null) return null;
	return typeof value === "string" ? value : new Uint8Array(value);
}

// ---------------------------------------------------------------------------
// Schema migrations
// ---------------------------------------------------------------------------

const sqlMigrations: SQLSchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: "Create items table",
		// The type of `data` is ANY to allow SQLite to retain the input type (e.g. BLOB vs TEXT) and avoid unnecessary conversions.
		// The Durable Object API only accepts strings and Uint8Arrays, so we can safely store them as-is and retrieve them with the correct type.
		sql: `
            CREATE TABLE IF NOT EXISTS items (
                hk                    TEXT    NOT NULL,
                sk                    TEXT    NOT NULL DEFAULT '',
                data                  ANY     NOT NULL,
                ttl_epoch_utc_seconds INTEGER,
                v                     INTEGER NOT NULL,
                last_transaction_ts   INTEGER NOT NULL DEFAULT 0,
                est_row_bytes         INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (hk, sk)
            ) WITHOUT ROWID, STRICT;`,
	},
	{
		idMonotonicInc: 2,
		description: "Add last_transaction_ts to items and create transaction support tables",
		sql: `
            CREATE TABLE IF NOT EXISTS pending_transactions (
                hk                    TEXT    NOT NULL,
                sk                    TEXT    NOT NULL DEFAULT '',
                transaction_id        TEXT    NOT NULL,
                transaction_ts        INTEGER NOT NULL,
                operation             TEXT    NOT NULL,
                data                  ANY,
                conditions_json       TEXT,
                coordinator_do_id   TEXT    NOT NULL DEFAULT '',
                created_at            INTEGER NOT NULL,
                PRIMARY KEY (hk, sk, transaction_id)
            ) WITHOUT ROWID, STRICT;

            CREATE INDEX IF NOT EXISTS pending_transactions_created_at
                ON pending_transactions (created_at);

            CREATE TABLE IF NOT EXISTS deletion_metadata (
                id              INTEGER PRIMARY KEY CHECK (id = 1),
                max_deleted_ts  INTEGER NOT NULL DEFAULT 0
            ) STRICT;
            INSERT OR IGNORE INTO deletion_metadata (id, max_deleted_ts) VALUES (1, 0);`,
	},
	{
		idMonotonicInc: 3,
		description: "Add range partition support: promoted_keys table (WITHOUT ROWID; gc_done flag; status index)",
		sql: `
            CREATE TABLE IF NOT EXISTS promoted_keys (
                hash_key   TEXT    NOT NULL PRIMARY KEY,
                status     TEXT    NOT NULL,
                gc_done    INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            ) WITHOUT ROWID, STRICT;
            CREATE INDEX IF NOT EXISTS idx_promoted_keys_status ON promoted_keys (status, gc_done);`,
	},
	{
		idMonotonicInc: 4,
		description: "Add per-row size estimate and key-level size summary for efficient promotion detection",
		sql: `
            CREATE TABLE IF NOT EXISTS key_size_estimates (
                hk        TEXT    NOT NULL PRIMARY KEY,
                est_bytes INTEGER NOT NULL DEFAULT 0
            ) WITHOUT ROWID, STRICT;`,
	},
];

// ---------------------------------------------------------------------------
// The store
// ---------------------------------------------------------------------------

export class PartitionStore {
	#storage: DurableObjectStorage;
	#migrations: SQLSchemaMigrations;

	constructor(storage: DurableObjectStorage) {
		this.#storage = storage;
		this.#migrations = new SQLSchemaMigrations({
			migrations: sqlMigrations,
			doStorage: storage,
		});
	}

	async runMigrations(): Promise<void> {
		await this.#migrations.runAll();
	}

	get databaseSize(): number {
		return this.#storage.sql.databaseSize;
	}

	/**
	 * Atomicity passthrough: multi-statement invariants (e.g. commitLocal's keyset checks,
	 * migration metadata ingestion) are composed by the caller around store calls.
	 */
	transactionSync<T>(fn: () => T): T {
		return this.#storage.transactionSync(fn);
	}

	// ─── items ──────────────────────────────────────────────────────────────

	/** Metrics cover the single SELECT (what the DO surfaced in read meta). */
	getItem(
		hk: string,
		sk: string,
	): {
		row?: { data: string | Uint8Array; ttl_epoch_utc_seconds: number | null; v: number; last_transaction_ts: number };
		rowsRead: number;
		rowsWritten: number;
	} {
		const res = this.#storage.sql.exec<{
			data: string | ArrayBuffer;
			ttl_epoch_utc_seconds: number | null;
			v: number;
			last_transaction_ts: number;
		}>(`SELECT data, ttl_epoch_utc_seconds, v, last_transaction_ts FROM items WHERE hk = ? AND sk = ? LIMIT 1`, hk, sk);
		const row = res.toArray()[0];
		return {
			row: row ? { ...row, data: fromSqlData(row.data) } : undefined,
			rowsRead: res.rowsRead,
			rowsWritten: res.rowsWritten,
		};
	}

	/**
	 * Lightweight existence/version/timestamp read for condition evaluation and prepare checks.
	 * Metrics cover the single SELECT (counted into putItem/deleteItem meta when conditions exist).
	 */
	getItemStamp(hk: string, sk: string): { row?: { v: number; last_transaction_ts: number }; rowsRead: number; rowsWritten: number } {
		const res = this.#storage.sql.exec<{ v: number; last_transaction_ts: number }>(
			`SELECT v, last_transaction_ts FROM items WHERE hk = ? AND sk = ? LIMIT 1`,
			hk,
			sk,
		);
		const row = res.toArray()[0];
		return { row, rowsRead: res.rowsRead, rowsWritten: res.rowsWritten };
	}

	/**
	 * The items upsert with est_row_bytes / key_size_estimates bookkeeping — the single
	 * definition used by BOTH the non-transactional putItem and the transactional commit apply.
	 * Returns the new item version and the key's updated size estimate (feeds promotion checks).
	 * Metrics cover ONLY the items upsert statement (matching the DO's previous meta math —
	 * the old-estimate read and the key_size_estimates upsert were never counted).
	 */
	upsertItem(opts: { hk: string; sk: string; data: string | Uint8Array; ttlEpochUtcSeconds: number | null; lastTransactionTs: number }): {
		version: number;
		keyEstBytes: number;
		rowsRead: number;
		rowsWritten: number;
	} {
		const oldEstRow = this.#storage.sql
			.exec<{ est_row_bytes: number }>(`SELECT est_row_bytes FROM items WHERE hk = ? AND sk = ? LIMIT 1`, opts.hk, opts.sk)
			.toArray()[0];
		const oldEst = oldEstRow?.est_row_bytes ?? 0;
		const newEst = estimateRowBytes(opts.data, opts.hk, opts.sk);

		const writeRes = this.#storage.sql.exec<{ v: number }>(
			`INSERT INTO items (hk, sk, data, ttl_epoch_utc_seconds, v, last_transaction_ts, est_row_bytes)
			 VALUES (?, ?, ?, ?, 1, ?, ?)
			 ON CONFLICT(hk, sk) DO UPDATE SET
			   data = excluded.data,
			   ttl_epoch_utc_seconds = excluded.ttl_epoch_utc_seconds,
			   v = v + 1,
			   last_transaction_ts = excluded.last_transaction_ts,
			   est_row_bytes = excluded.est_row_bytes
			 RETURNING v`,
			opts.hk,
			opts.sk,
			opts.data,
			opts.ttlEpochUtcSeconds,
			opts.lastTransactionTs,
			newEst,
		);
		const rows = writeRes.toArray();
		invariant(rows.length === 1, `fokos/partition-store.upsertItem: RETURNING expected 1 row, got ${rows.length}`);
		const version = rows[0].v;
		invariant(
			typeof version === "number" && Number.isInteger(version) && version >= 1,
			`fokos/partition-store.upsertItem: unexpected version value: ${version}`,
		);

		const kseRow = this.#storage.sql
			.exec<{ est_bytes: number }>(
				`INSERT INTO key_size_estimates (hk, est_bytes) VALUES (?, ?)
				 ON CONFLICT(hk) DO UPDATE SET est_bytes = MAX(0, est_bytes + excluded.est_bytes - ?)
				 RETURNING est_bytes`,
				opts.hk,
				newEst,
				oldEst,
			)
			.toArray()[0];

		return { version, keyEstBytes: kseRow?.est_bytes ?? newEst, rowsRead: writeRes.rowsRead, rowsWritten: writeRes.rowsWritten };
	}

	/**
	 * Deletes an item, keeping the deletion watermark and key-size estimate consistent.
	 * `bumpWatermarkAlways` preserves the transactional-delete behavior (watermark and estimate
	 * are updated even when the row was already absent); the non-transactional path updates them
	 * only when a row was actually deleted.
	 * Metrics cover ONLY the DELETE statement (matching the DO's previous meta math).
	 */
	deleteItem(opts: { hk: string; sk: string; watermarkTs: number; bumpWatermarkAlways?: boolean }): {
		deleted: boolean;
		rowsRead: number;
		rowsWritten: number;
	} {
		const delEstRow = this.#storage.sql
			.exec<{ est_row_bytes: number }>(`SELECT est_row_bytes FROM items WHERE hk = ? AND sk = ? LIMIT 1`, opts.hk, opts.sk)
			.toArray()[0];
		const delEst = delEstRow?.est_row_bytes ?? 0;

		const writeRes = this.#storage.sql.exec(`DELETE FROM items WHERE hk = ? AND sk = ?`, opts.hk, opts.sk);
		const deleted = writeRes.rowsWritten > 0;
		if (deleted || opts.bumpWatermarkAlways) {
			this.bumpMaxDeletedTs(opts.watermarkTs);
			this.#storage.sql.exec(`UPDATE key_size_estimates SET est_bytes = MAX(0, est_bytes - ?) WHERE hk = ?`, delEst, opts.hk);
		}
		return { deleted, rowsRead: writeRes.rowsRead, rowsWritten: writeRes.rowsWritten };
	}

	/** The transactional "check" operation: bumps the item's timestamp without changing data. */
	bumpItemLastTransactionTs(hk: string, sk: string, ts: number): void {
		this.#storage.sql.exec(`UPDATE items SET last_transaction_ts = MAX(last_transaction_ts, ?) WHERE hk = ? AND sk = ?`, ts, hk, sk);
	}

	/**
	 * Migration ingestion: INSERT OR IGNORE rather than OR REPLACE — all writes to a migrating
	 * partition are rejected with 503 while migration_migrating, so no user write can have
	 * arrived yet. IGNORE is safer for retries: if a batch was already written before a crash we
	 * skip re-inserting those items rather than overwriting them unnecessarily.
	 */
	insertItemIfAbsent(item: MigratedItem): void {
		this.#storage.sql.exec(
			`INSERT OR IGNORE INTO items (hk, sk, data, ttl_epoch_utc_seconds, v, last_transaction_ts, est_row_bytes) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			item.hk,
			item.sk,
			item.data,
			item.ttl_epoch_utc_seconds ?? null,
			item.v,
			item.last_transaction_ts,
			estimateRowBytes(item.data, item.hk, item.sk),
		);
	}

	hasItemsForHashKey(hk: string): boolean {
		return this.#storage.sql.exec(`SELECT 1 FROM items WHERE hk = ? LIMIT 1`, hk).toArray().length > 0;
	}

	// Computes N-1 strictly-increasing split boundaries (count-quantiles) within [start, end) in one
	// transactionSync snapshot. Returns null if there are fewer than N items (so every child stays non-empty).
	// Each boundary is shortened to the minimum prefix that still separates adjacent data keys (the
	// "shortest separator" of the predecessor and boundary key), keeping doNames and topology encoding compact.
	// A data query on the items table — it lives with the store; the DO passes the result into the
	// range split policy's prepareSplit.
	computeRangeSplitBoundaries(hashKey: string, start: string | null, end: string | null, N: number): string[] | null {
		return this.#storage.transactionSync(() => {
			const lower = start ?? ""; // −∞ ⇒ sk >= ''
			const cntRow =
				end === null
					? this.#storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM items WHERE hk = ? AND sk >= ?`, hashKey, lower).toArray()[0]
					: this.#storage.sql
							.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM items WHERE hk = ? AND sk >= ? AND sk < ?`, hashKey, lower, end)
							.toArray()[0];
			const cnt = cntRow?.n ?? 0;
			if (cnt < N) return null; // need ≥ N items so each of the N children gets ≥ 1

			const boundaries: string[] = [];
			for (let i = 1; i < N; i++) {
				const offset = Math.floor((cnt * i) / N);
				// Fetch the predecessor (offset - 1) and the boundary key (offset) in one scan.
				// offset >= 1 always because cnt >= N guarantees floor(cnt * 1 / N) >= 1.
				const rows =
					end === null
						? this.#storage.sql
								.exec<{
									sk: string;
								}>(`SELECT sk FROM items WHERE hk = ? AND sk >= ? ORDER BY sk LIMIT 2 OFFSET ?`, hashKey, lower, offset - 1)
								.toArray()
						: this.#storage.sql
								.exec<{
									sk: string;
								}>(`SELECT sk FROM items WHERE hk = ? AND sk >= ? AND sk < ? ORDER BY sk LIMIT 2 OFFSET ?`, hashKey, lower, end, offset - 1)
								.toArray();
				invariant(
					rows.length === 2,
					"fokos/range.computeRangeSplitBoundaries: expected predecessor + boundary rows at the computed offset",
				);
				boundaries.push(shortestSeparator(rows[0].sk, rows[1].sk));
			}
			// Boundaries must be strictly above the lower bound and strictly increasing (distinct, non-empty children).
			for (let i = 0; i < boundaries.length; i++) {
				if (boundaries[i] <= lower) return null;
				if (i > 0 && boundaries[i] <= boundaries[i - 1]) return null;
			}
			return boundaries;
		});
	}

	/** Promotion GC: deletes up to `limit` rows of a promoted key per call (bounded work per cycle). */
	deleteItemsBatchForHashKey(hk: string, limit: number): void {
		this.#storage.sql.exec(
			`DELETE FROM items WHERE hk = ? AND sk IN (SELECT sk FROM items WHERE hk = ? ORDER BY sk LIMIT ?)`,
			hk,
			hk,
			limit,
		);
	}

	/** Pages the items table in (hk, sk) order, strictly after `cursor`. */
	queryItemsPage(cursor: MigrationCursor | null, limit: number): MigratedItem[] {
		type Row = {
			hk: string;
			sk: string;
			data: string | ArrayBuffer;
			ttl_epoch_utc_seconds: number | null;
			v: number;
			last_transaction_ts: number;
		};

		let sqlCursor: SqlStorageCursor<Row>;
		if (!cursor) {
			sqlCursor = this.#storage.sql.exec<Row>(
				`SELECT hk, sk, data, ttl_epoch_utc_seconds, v, last_transaction_ts FROM items ORDER BY hk, sk LIMIT ?`,
				limit,
			);
		} else {
			sqlCursor = this.#storage.sql.exec<Row>(
				`SELECT hk, sk, data, ttl_epoch_utc_seconds, v, last_transaction_ts FROM items WHERE hk > ? OR (hk = ? AND sk > ?) ORDER BY hk, sk LIMIT ?`,
				cursor.hk,
				cursor.hk,
				cursor.sk,
				limit,
			);
		}

		const items: MigratedItem[] = [];
		for (const row of sqlCursor) {
			items.push({ ...row, data: fromSqlData(row.data) });
		}
		return items;
	}

	/**
	 * Pages one hashKey's items within the slice [lower, end) in sk order. Resumes strictly after
	 * `cursor` when provided, otherwise starts at the slice's lower bound; `end === null` = unbounded.
	 */
	queryRangeItemsPage(opts: {
		hk: string;
		lower: string;
		end: string | null;
		cursor: MigrationCursor | null;
		limit: number;
	}): MigratedItem[] {
		type Row = {
			hk: string;
			sk: string;
			data: string | ArrayBuffer;
			ttl_epoch_utc_seconds: number | null;
			v: number;
			last_transaction_ts: number;
		};
		const conds: string[] = ["hk = ?"];
		const params: unknown[] = [opts.hk];
		if (opts.cursor) {
			conds.push("sk > ?");
			params.push(opts.cursor.sk);
		} else {
			conds.push("sk >= ?");
			params.push(opts.lower);
		}
		if (opts.end !== null) {
			conds.push("sk < ?");
			params.push(opts.end);
		}
		const page = this.#storage.sql
			.exec<Row>(
				`SELECT hk, sk, data, ttl_epoch_utc_seconds, v, last_transaction_ts FROM items WHERE ${conds.join(" AND ")} ORDER BY sk LIMIT ?`,
				...params,
				opts.limit,
			)
			.toArray();
		return page.map((row) => ({ ...row, data: fromSqlData(row.data) }));
	}

	// ─── pending_transactions ───────────────────────────────────────────────

	pendingLockFor(hk: string, sk: string): { transaction_id: string } | undefined {
		return this.#storage.sql
			.exec<{ transaction_id: string }>(`SELECT transaction_id FROM pending_transactions WHERE hk = ? AND sk = ? LIMIT 1`, hk, sk)
			.toArray()[0];
	}

	/** Idempotent lock insertion — used by prepare and by migration ingestion of parent locks. */
	insertPendingLock(row: PendingTransactionRow): void {
		this.#storage.sql.exec(
			`INSERT OR IGNORE INTO pending_transactions
			   (hk, sk, transaction_id, transaction_ts, operation, data, conditions_json, coordinator_do_id, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			row.hk,
			row.sk,
			row.transaction_id,
			row.transaction_ts,
			row.operation,
			row.data,
			row.conditions_json,
			row.coordinator_do_id,
			row.created_at,
		);
	}

	pendingTxCountFor(transactionId: string): number {
		return (
			this.#storage.sql
				.exec<{ n: number }>(`SELECT COUNT(*) as n FROM pending_transactions WHERE transaction_id = ?`, transactionId)
				.toArray()[0]?.n ?? 0
		);
	}

	pendingTxTotalCount(): number {
		return this.#storage.sql.exec<{ n: number }>(`SELECT COUNT(*) as n FROM pending_transactions`).toArray()[0]?.n ?? 0;
	}

	pendingLockCountForHashKey(hk: string): number {
		return this.#storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM pending_transactions WHERE hk = ?`, hk).toArray()[0]?.n ?? 0;
	}

	listPendingTxKeys(transactionId: string): { hk: string; sk: string }[] {
		return this.#storage.sql
			.exec<{ hk: string; sk: string }>(`SELECT hk, sk FROM pending_transactions WHERE transaction_id = ?`, transactionId)
			.toArray();
	}

	getPendingTxOp(hk: string, sk: string, transactionId: string): { operation: string; data: string | Uint8Array | null } | undefined {
		const row = this.#storage.sql
			.exec<{
				operation: string;
				data: string | ArrayBuffer | null;
			}>(`SELECT operation, data FROM pending_transactions WHERE hk = ? AND sk = ? AND transaction_id = ? LIMIT 1`, hk, sk, transactionId)
			.toArray()[0];
		return row ? { operation: row.operation, data: fromSqlData(row.data) } : undefined;
	}

	/** Stale-transaction recovery: the locked items of one transaction, data converted. */
	listPendingTxItems(
		transactionId: string,
	): { hk: string; sk: string; transaction_ts: number; operation: string; data: string | Uint8Array | null }[] {
		return this.#storage.sql
			.exec<{ hk: string; sk: string; transaction_ts: number; operation: string; data: string | ArrayBuffer | null }>(
				`SELECT hk, sk, transaction_ts, operation, data FROM pending_transactions WHERE transaction_id = ?`,
				transactionId,
			)
			.toArray()
			.map((row) => ({ ...row, data: fromSqlData(row.data) }));
	}

	listStalePendingTx(staleBeforeTs: number, limit: number): { transaction_id: string; coordinator_do_id: string }[] {
		return this.#storage.sql
			.exec<{ transaction_id: string; coordinator_do_id: string }>(
				`SELECT DISTINCT transaction_id, coordinator_do_id
                     FROM pending_transactions WHERE created_at < ? LIMIT ?`,
				staleBeforeTs,
				limit,
			)
			.toArray();
	}

	deletePendingTx(transactionId: string): void {
		this.#storage.sql.exec(`DELETE FROM pending_transactions WHERE transaction_id = ?`, transactionId);
	}

	/** Promotion GC: a fully-promoted key can have no live locks here anymore. */
	deletePendingTxForHashKey(hk: string): void {
		this.#storage.sql.exec(`DELETE FROM pending_transactions WHERE hk = ?`, hk);
	}

	/** Split completion: children own authoritative copies; the parent's locks are redundant. */
	deleteAllPendingTx(): void {
		this.#storage.sql.exec(`DELETE FROM pending_transactions`);
	}

	/** Pages pending_transactions in (hk, sk, transaction_id) order, strictly after `cursor`. */
	queryPendingTxPage(cursor: PendingTransactionCursor | null, limit: number): PendingTransactionRow[] {
		type Row = {
			hk: string;
			sk: string;
			transaction_id: string;
			transaction_ts: number;
			operation: string;
			data: string | ArrayBuffer | null;
			conditions_json: string | null;
			coordinator_do_id: string;
			created_at: number;
		};

		const cols = `hk, sk, transaction_id, transaction_ts, operation, data, conditions_json, coordinator_do_id, created_at`;
		let sqlCursor: SqlStorageCursor<Row>;
		if (!cursor) {
			sqlCursor = this.#storage.sql.exec<Row>(`SELECT ${cols} FROM pending_transactions ORDER BY hk, sk, transaction_id LIMIT ?`, limit);
		} else {
			sqlCursor = this.#storage.sql.exec<Row>(
				`SELECT ${cols} FROM pending_transactions
				 WHERE hk > ? OR (hk = ? AND (sk > ? OR (sk = ? AND transaction_id > ?)))
				 ORDER BY hk, sk, transaction_id LIMIT ?`,
				cursor.hk,
				cursor.hk,
				cursor.sk,
				cursor.sk,
				cursor.transaction_id,
				limit,
			);
		}

		const rows: PendingTransactionRow[] = [];
		for (const row of sqlCursor) {
			rows.push({ ...row, data: fromSqlData(row.data) });
		}
		return rows;
	}

	// ─── deletion_metadata ──────────────────────────────────────────────────

	getMaxDeletedTs(): number {
		return (
			this.#storage.sql.exec<{ max_deleted_ts: number }>(`SELECT max_deleted_ts FROM deletion_metadata WHERE id = 1`).toArray()[0]
				?.max_deleted_ts ?? 0
		);
	}

	/** The single definition of the deletion-watermark update (monotonic MAX). */
	bumpMaxDeletedTs(ts: number): void {
		this.#storage.sql.exec(`UPDATE deletion_metadata SET max_deleted_ts = MAX(max_deleted_ts, ?) WHERE id = 1`, ts);
	}

	// ─── key_size_estimates ─────────────────────────────────────────────────

	deleteKeySizeEstimate(hk: string): void {
		this.#storage.sql.exec(`DELETE FROM key_size_estimates WHERE hk = ?`, hk);
	}

	/** Post-migration rebuild: recomputes every key's estimate from the ingested rows. */
	rebuildKeySizeEstimates(): void {
		this.#storage.sql.exec(
			`INSERT INTO key_size_estimates (hk, est_bytes)
			 SELECT hk, SUM(est_row_bytes) FROM items GROUP BY hk
			 ON CONFLICT(hk) DO UPDATE SET est_bytes = excluded.est_bytes`,
		);
	}

	// ─── promoted_keys ──────────────────────────────────────────────────────

	listPromotedKeys(): PromotedKeyRow[] {
		return this.#storage.sql.exec<PromotedKeyRow>(`SELECT hash_key, status FROM promoted_keys`).toArray();
	}

	getPromotedKeyStatus(hk: string): PromotedKeyStatus | undefined {
		return this.#storage.sql.exec<{ status: PromotedKeyStatus }>(`SELECT status FROM promoted_keys WHERE hash_key = ?`, hk).toArray()[0]
			?.status;
	}

	hasInFlightPromotedKeys(): boolean {
		return (
			this.#storage.sql.exec<{ one: 1 }>(`SELECT 1 AS one FROM promoted_keys WHERE status IN ('queued', 'promoting') LIMIT 1`).toArray()
				.length > 0
		);
	}

	hasResidualItemsForPromotedKeys(): boolean {
		return (
			this.#storage.sql.exec<{ one: 1 }>(`SELECT 1 AS one FROM promoted_keys WHERE status = 'promoted' AND gc_done = 0 LIMIT 1`).toArray()
				.length > 0
		);
	}

	listPromotedKeysNeedingGC(limit?: number): string[] {
		return this.#storage.sql
			.exec<{ hash_key: string }>(
				limit != null
					? `SELECT hash_key FROM promoted_keys WHERE status = 'promoted' AND gc_done = 0 LIMIT ?`
					: `SELECT hash_key FROM promoted_keys WHERE status = 'promoted' AND gc_done = 0`,
				...(limit != null ? [limit] : []),
			)
			.toArray()
			.map((r) => r.hash_key);
	}

	markPromotedKeyGcDone(hk: string): void {
		this.#storage.sql.exec(`UPDATE promoted_keys SET gc_done = 1 WHERE hash_key = ?`, hk);
	}

	/**
	 * Idempotent: used both when queueing a new promotion and when inheriting entries on hash
	 * split. Returns whether a new row was actually inserted — false means the key already had a
	 * row (whose status may differ from `status`), so callers keeping an in-memory cache must
	 * resync from storage instead of assuming `status` was written.
	 */
	insertPromotedKey(hk: string, status: PromotedKeyStatus, now: number): { inserted: boolean } {
		const res = this.#storage.sql.exec(
			`INSERT OR IGNORE INTO promoted_keys (hash_key, status, created_at, updated_at) VALUES (?, ?, ?, ?)`,
			hk,
			status,
			now,
			now,
		);
		return { inserted: res.rowsWritten > 0 };
	}

	/**
	 * Guarded transition: only updates when the row is currently in `fromStatus`. Returns whether
	 * a row actually transitioned — false means the key was absent or in a different status, so
	 * callers keeping an in-memory cache must resync from storage instead of assuming `toStatus`.
	 */
	updatePromotedKeyStatus(hk: string, fromStatus: PromotedKeyStatus, toStatus: PromotedKeyStatus, updatedAt: number): { updated: boolean } {
		const res = this.#storage.sql.exec(
			`UPDATE promoted_keys SET status = ?, updated_at = ? WHERE hash_key = ? AND status = ?`,
			toStatus,
			updatedAt,
			hk,
			fromStatus,
		);
		return { updated: res.rowsWritten > 0 };
	}

	/** Pages promoted_keys in hash_key order, strictly after `cursor`. */
	queryPromotedKeysPage(cursor: PromotedKeyCursor | null, limit: number): PromotedKeyRow[] {
		return (
			cursor
				? this.#storage.sql.exec<PromotedKeyRow>(
						`SELECT hash_key, status FROM promoted_keys WHERE hash_key > ? ORDER BY hash_key LIMIT ?`,
						cursor.hashKey,
						limit,
					)
				: this.#storage.sql.exec<PromotedKeyRow>(`SELECT hash_key, status FROM promoted_keys ORDER BY hash_key LIMIT ?`, limit)
		).toArray();
	}
}
