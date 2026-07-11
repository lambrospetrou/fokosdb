import { SQLSchemaMigration, SQLSchemaMigrations } from "durable-utils/sql-migrations";
import type { ItemCondition, RangeAncestorInfo } from "../types.js";
import { KeyCodec, type KeyBytes } from "../partition-topology/key-codec.js";
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

// hk/sk are canonical KeyBytes everywhere in the store: they bind to SQLite BLOB columns and compare
// by memcmp (the same total order as KeyCodec.compare). The ONLY producer of KeyBytes is KeyCodec.
export type MigratedItem = {
	hk: KeyBytes;
	sk: KeyBytes;
	data: string | Uint8Array;
	ttl_epoch_utc_seconds: number | null;
	v: number;
	last_transaction_ts: number;
};

export type PendingTransactionRow = {
	hk: KeyBytes;
	sk: KeyBytes;
	transaction_id: string;
	transaction_ts: number;
	operation: string;
	data: string | Uint8Array | null;
	conditions_json: string | null;
	coordinator_do_id: string;
	created_at: number;
};

export type PendingTransactionCursor = { hk: KeyBytes; sk: KeyBytes; transaction_id: string };

export type ScanCursor = { hk: KeyBytes; sk: KeyBytes; inclusive?: boolean };

export type PromotedKeyCursor = { hashKey: KeyBytes };

export type PromotedKeyStatus = "queued" | "promoting" | "promoted";

export type PromotedKeyRow = { hash_key: KeyBytes; status: PromotedKeyStatus };

export type SqlMetrics = { rowsRead: number; rowsWritten: number };

export type ItemSnapshot = { hk: KeyBytes; sk: KeyBytes; found: true; v: number } | { hk: KeyBytes; sk: KeyBytes; found: false };

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

// Boundary synthesis lives in KeyCodec.shortestSeparator (byte space), keeping JS-side splits and
// SQLite BLOB scans on the same total order.

/**
 * SQLite returns BLOB key columns as ArrayBuffer (or Uint8Array). Materialize them as KeyBytes via a
 * trusted re-brand — they were written as canonical bytes, so this is the asKeyBytes path (no copy of
 * a Uint8Array; a thin view over an ArrayBuffer). Every store row-reading method funnels hk/sk here.
 */
function fromSqlKey(value: ArrayBuffer | Uint8Array): KeyBytes {
	return KeyCodec.asKeyBytes(value instanceof Uint8Array ? value : new Uint8Array(value));
}

/**
 * Pure condition evaluation shared by the non-transactional putItem/deleteItem and the
 * transactional prepare path. Lives with the store (NOT the transaction participant) because
 * both paths evaluate the same conditions against the same item snapshot shape.
 */
export function evaluateConditionsOnItem(item: ItemSnapshot, conditions: ItemCondition[], operationName: string): void {
	const where = () => `hk=${KeyCodec.keyForLog(item.hk)}, sk=${KeyCodec.keyForLog(item.sk)}`;
	for (const condition of conditions) {
		if (condition.type === "item_exists") {
			if (!item.found) {
				throw new Error(`fokos/${operationName}: condition item_exists failed — item does not exist (${where()})`);
			}
		} else if (condition.type === "item_not_exists") {
			if (item.found) {
				throw new Error(`fokos/${operationName}: condition item_not_exists failed — item already exists with v=${item.v} (${where()})`);
			}
		} else if (condition.type === "attribute_equals") {
			const actual = item.found ? item[condition.attribute] : null;
			if (actual !== condition.value) {
				throw new Error(
					`fokos/${operationName}: condition attribute_equals failed — attribute "${condition.attribute}" expected ${condition.value}, found ${actual} (${where()})`,
				);
			}
		}
	}
}

export function estimateRowBytes(data: string | Uint8Array, hk: KeyBytes, sk: KeyBytes): number {
	const dataBytes = typeof data === "string" ? data.length : data.byteLength;
	// hk and sk are variable-length and physically stored in every row (WITHOUT ROWID PK),
	// so a long hk with many sort keys contributes significant storage that must be counted per-row.
	// Keys are canonical bytes now, so byteLength is the exact stored size.
	// 40 = fixed overhead: integer columns (v, ttl_epoch_utc_seconds, last_transaction_ts, est_row_bytes ≈ 4×8 = 32 bytes)
	//      + SQLite B-tree record metadata (header varints, null bitmap ≈ 8 bytes).
	return dataBytes + hk.byteLength + sk.byteLength + 40;
}

export function estimateItemBytes(item: MigratedItem): number {
	const dataSize = typeof item.data === "string" ? item.data.length * 2 : item.data.byteLength;
	return item.hk.byteLength + item.sk.byteLength + dataSize + 8 + 64;
}

export function estimatePendingTxBytes(row: PendingTransactionRow): number {
	const dataSize = row.data == null ? 0 : typeof row.data === "string" ? row.data.length * 2 : row.data.byteLength;
	return row.hk.byteLength + row.sk.byteLength + 32 + 8 + 8 + dataSize + (row.conditions_json?.length ?? 0) * 2 + 64;
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
                hk                    BLOB    NOT NULL,
                sk                    BLOB    NOT NULL DEFAULT x'',
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
                hk                    BLOB    NOT NULL,
                sk                    BLOB    NOT NULL DEFAULT x'',
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
                hash_key   BLOB    NOT NULL PRIMARY KEY,
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
                hk        BLOB    NOT NULL PRIMARY KEY,
                est_bytes INTEGER NOT NULL DEFAULT 0
            ) WITHOUT ROWID, STRICT;`,
	},
	{
		idMonotonicInc: 5,
		description: "Add range_hierarchy table for this range partition's ancestor and descendant boundaries",
		sql: `
            CREATE TABLE IF NOT EXISTS range_hierarchy (
				hk			      BLOB    NOT NULL DEFAULT x'',
                sk_start_boundary BLOB    NOT NULL DEFAULT x'',
                sk_end_boundary   BLOB    NOT NULL DEFAULT x'',
                depth             INTEGER NOT NULL,
				PRIMARY KEY (hk, sk_start_boundary, sk_end_boundary)
            ) WITHOUT ROWID, STRICT;

			CREATE INDEX IF NOT EXISTS idx_range_hierarchy_depth ON range_hierarchy (hk, depth, sk_start_boundary, sk_end_boundary);`,
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
		hk: KeyBytes,
		sk: KeyBytes,
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
	getItemStamp(hk: KeyBytes, sk: KeyBytes): { row?: { v: number; last_transaction_ts: number }; rowsRead: number; rowsWritten: number } {
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
	upsertItem(opts: {
		hk: KeyBytes;
		sk: KeyBytes;
		data: string | Uint8Array;
		ttlEpochUtcSeconds: number | null;
		lastTransactionTs: number;
	}): {
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
	deleteItem(opts: { hk: KeyBytes; sk: KeyBytes; watermarkTs: number; bumpWatermarkAlways?: boolean }): {
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
	bumpItemLastTransactionTs(hk: KeyBytes, sk: KeyBytes, ts: number): void {
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

	hasItemsForHashKey(hk: KeyBytes): boolean {
		return this.#storage.sql.exec(`SELECT 1 FROM items WHERE hk = ? LIMIT 1`, hk).toArray().length > 0;
	}

	// Computes N-1 strictly-increasing split boundaries (count-quantiles) within [start, end) in one
	// transactionSync snapshot. Returns null if there are fewer than N items (so every child stays non-empty).
	// Each boundary is shortened to the minimum prefix that still separates adjacent data keys (the
	// "shortest separator" of the predecessor and boundary key), keeping doNames and topology encoding compact.
	// A data query on the items table — it lives with the store; the DO passes the result into the
	// range split policy's prepareSplit.
	computeRangeSplitBoundaries(hashKey: KeyBytes, start: KeyBytes | null, end: KeyBytes | null, N: number): KeyBytes[] | null {
		return this.#storage.transactionSync(() => {
			const lower = start ?? KeyCodec.encodeOptional(undefined); // −∞ ⇒ sk >= x'' (the empty sentinel)
			const cntRow =
				end === null
					? this.#storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM items WHERE hk = ? AND sk >= ?`, hashKey, lower).toArray()[0]
					: this.#storage.sql
							.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM items WHERE hk = ? AND sk >= ? AND sk < ?`, hashKey, lower, end)
							.toArray()[0];
			const cnt = cntRow?.n ?? 0;
			if (cnt < N) return null; // need ≥ N items so each of the N children gets ≥ 1

			const boundaries: KeyBytes[] = [];
			for (let i = 1; i < N; i++) {
				const offset = Math.floor((cnt * i) / N);
				// Fetch the predecessor (offset - 1) and the boundary key (offset) in one scan.
				// offset >= 1 always because cnt >= N guarantees floor(cnt * 1 / N) >= 1.
				const rows = (
					end === null
						? this.#storage.sql
								.exec<{
									sk: ArrayBuffer;
								}>(`SELECT sk FROM items WHERE hk = ? AND sk >= ? ORDER BY sk LIMIT 2 OFFSET ?`, hashKey, lower, offset - 1)
								.toArray()
						: this.#storage.sql
								.exec<{
									sk: ArrayBuffer;
								}>(`SELECT sk FROM items WHERE hk = ? AND sk >= ? AND sk < ? ORDER BY sk LIMIT 2 OFFSET ?`, hashKey, lower, end, offset - 1)
								.toArray()
				).map((r) => fromSqlKey(r.sk));
				invariant(
					rows.length === 2,
					"fokos/range.computeRangeSplitBoundaries: expected predecessor + boundary rows at the computed offset",
				);
				// Byte-space separator: the boundary's UTF-8 position now matches the SQL scans that migrate the data.
				boundaries.push(KeyCodec.shortestSeparator(rows[0], rows[1]));
			}
			// Boundaries must be strictly above the lower bound and strictly increasing (distinct, non-empty children).
			for (let i = 0; i < boundaries.length; i++) {
				if (KeyCodec.compare(boundaries[i], lower) <= 0) return null;
				if (i > 0 && KeyCodec.compare(boundaries[i], boundaries[i - 1]) <= 0) return null;
			}
			return boundaries;
		});
	}

	/** Promotion GC: deletes up to `limit` rows of a promoted key per call (bounded work per cycle). */
	deleteItemsBatchForHashKey(hk: KeyBytes, limit: number): void {
		this.#storage.sql.exec(
			`DELETE FROM items WHERE hk = ? AND sk IN (SELECT sk FROM items WHERE hk = ? ORDER BY sk LIMIT ?)`,
			hk,
			hk,
			limit,
		);
	}

	/** Pages the items table in (hk, sk) order, strictly after `cursor`. */
	queryItemsPage(cursor: ScanCursor | null, limit: number): MigratedItem[] {
		type Row = {
			hk: ArrayBuffer;
			sk: ArrayBuffer;
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
			items.push({ ...row, hk: fromSqlKey(row.hk), sk: fromSqlKey(row.sk), data: fromSqlData(row.data) });
		}
		return items;
	}

	/**
	 * Pages one hashKey's items in the given direction with explicit per-end inclusivity.
	 *
	 * - `lower`: start bound (value + inclusive flag). When cursor is absent, emits `sk >= lower`
	 *   (inclusive) or `sk > lower` (exclusive). When cursor is present, resumes after the cursor
	 *   (`sk > cursor.sk`, or `sk >= cursor.sk` when `cursorInclusive`) — the lower bound is ignored.
	 * - `upper`: end bound (value + inclusive flag), or `null` for unbounded. Emits `sk <= upper`
	 *   (inclusive) or `sk < upper` (exclusive).
	 * - `cursorInclusive`: when a cursor is present, include the cursor row itself instead of
	 *   resuming strictly past it. Used by the range-walk's boundary continuation cursor.
	 *
	 * Callers that always want lower-inclusive / upper-exclusive (e.g. migration) pass
	 * `lowerInclusive: true, upperInclusive: false`.
	 */
	queryRangeItemsPage(opts: {
		hk: KeyBytes;
		lower: KeyBytes;
		lowerInclusive: boolean;
		upper: KeyBytes | null;
		upperInclusive: boolean;
		cursor: ScanCursor | null;
		limit: number;
		direction: "asc" | "desc";
	}): MigratedItem[] {
		type Row = {
			hk: ArrayBuffer;
			sk: ArrayBuffer;
			data: string | ArrayBuffer;
			ttl_epoch_utc_seconds: number | null;
			v: number;
			last_transaction_ts: number;
		};
		const conds: string[] = ["hk = ?"];
		const params: unknown[] = [opts.hk];

		if (opts.direction === "asc") {
			// Near-bound (start): cursor wins; else use lower bound.
			if (opts.cursor) {
				conds.push(opts.cursor.inclusive ? "sk >= ?" : "sk > ?");
				params.push(opts.cursor.sk);
			} else {
				conds.push(opts.lowerInclusive ? "sk >= ?" : "sk > ?");
				params.push(opts.lower);
			}
			// Far-bound (end): upper.
			if (opts.upper !== null) {
				conds.push(opts.upperInclusive ? "sk <= ?" : "sk < ?");
				params.push(opts.upper);
			}
		} else {
			// Near-bound (start descending): cursor wins; else use upper bound.
			if (opts.cursor) {
				conds.push(opts.cursor.inclusive ? "sk <= ?" : "sk < ?");
				params.push(opts.cursor.sk);
			} else if (opts.upper !== null) {
				conds.push(opts.upperInclusive ? "sk <= ?" : "sk < ?");
				params.push(opts.upper);
			}
			// Far-bound (end descending): lower. Skip the condition when it's the zero-length
			// sentinel with inclusive=true — that matches all keys and adds nothing to the query.
			if (opts.lower.byteLength > 0 || !opts.lowerInclusive) {
				conds.push(opts.lowerInclusive ? "sk >= ?" : "sk > ?");
				params.push(opts.lower);
			}
		}

		const page = this.#storage.sql
			.exec<Row>(
				`SELECT hk, sk, data, ttl_epoch_utc_seconds, v, last_transaction_ts FROM items WHERE ${conds.join(" AND ")} ORDER BY sk ${opts.direction === "asc" ? "ASC" : "DESC"} LIMIT ?`,
				...params,
				opts.limit,
			)
			.toArray();
		return page.map((row) => ({ ...row, hk: fromSqlKey(row.hk), sk: fromSqlKey(row.sk), data: fromSqlData(row.data) }));
	}

	// ─── pending_transactions ───────────────────────────────────────────────

	pendingLockFor(hk: KeyBytes, sk: KeyBytes): { transaction_id: string } | undefined {
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

	pendingLockCountForHashKey(hk: KeyBytes): number {
		return this.#storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM pending_transactions WHERE hk = ?`, hk).toArray()[0]?.n ?? 0;
	}

	listPendingTxKeys(transactionId: string): { hk: KeyBytes; sk: KeyBytes }[] {
		return this.#storage.sql
			.exec<{ hk: ArrayBuffer; sk: ArrayBuffer }>(`SELECT hk, sk FROM pending_transactions WHERE transaction_id = ?`, transactionId)
			.toArray()
			.map((r) => ({ hk: fromSqlKey(r.hk), sk: fromSqlKey(r.sk) }));
	}

	getPendingTxOp(hk: KeyBytes, sk: KeyBytes, transactionId: string): { operation: string; data: string | Uint8Array | null } | undefined {
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
	): { hk: KeyBytes; sk: KeyBytes; transaction_ts: number; operation: string; data: string | Uint8Array | null }[] {
		return this.#storage.sql
			.exec<{ hk: ArrayBuffer; sk: ArrayBuffer; transaction_ts: number; operation: string; data: string | ArrayBuffer | null }>(
				`SELECT hk, sk, transaction_ts, operation, data FROM pending_transactions WHERE transaction_id = ?`,
				transactionId,
			)
			.toArray()
			.map((row) => ({ ...row, hk: fromSqlKey(row.hk), sk: fromSqlKey(row.sk), data: fromSqlData(row.data) }));
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
	deletePendingTxForHashKey(hk: KeyBytes): void {
		this.#storage.sql.exec(`DELETE FROM pending_transactions WHERE hk = ?`, hk);
	}

	/** Split completion: children own authoritative copies; the parent's locks are redundant. */
	deleteAllPendingTx(): void {
		this.#storage.sql.exec(`DELETE FROM pending_transactions`);
	}

	/** Pages pending_transactions in (hk, sk, transaction_id) order, strictly after `cursor`. */
	queryPendingTxPage(cursor: PendingTransactionCursor | null, limit: number): PendingTransactionRow[] {
		type Row = {
			hk: ArrayBuffer;
			sk: ArrayBuffer;
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
			rows.push({ ...row, hk: fromSqlKey(row.hk), sk: fromSqlKey(row.sk), data: fromSqlData(row.data) });
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

	deleteKeySizeEstimate(hk: KeyBytes): void {
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

	listPromotedKeys(status?: PromotedKeyStatus): PromotedKeyRow[] {
		let sql = `SELECT hash_key, status FROM promoted_keys`;
		const params: any[] = [];
		if (status) {
			sql += ` WHERE status = ?`;
			params.push(status);
		}
		return this.#storage.sql
			.exec<{ hash_key: ArrayBuffer; status: PromotedKeyStatus }>(sql, ...params)
			.toArray()
			.map((r) => ({ hash_key: fromSqlKey(r.hash_key), status: r.status }));
	}

	getPromotedKeyStatus(hk: KeyBytes): PromotedKeyStatus | undefined {
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

	listPromotedKeysNeedingGC(limit?: number): KeyBytes[] {
		return this.#storage.sql
			.exec<{ hash_key: ArrayBuffer }>(
				limit != null
					? `SELECT hash_key FROM promoted_keys WHERE status = 'promoted' AND gc_done = 0 LIMIT ?`
					: `SELECT hash_key FROM promoted_keys WHERE status = 'promoted' AND gc_done = 0`,
				...(limit != null ? [limit] : []),
			)
			.toArray()
			.map((r) => fromSqlKey(r.hash_key));
	}

	markPromotedKeyGcDone(hk: KeyBytes): void {
		this.#storage.sql.exec(`UPDATE promoted_keys SET gc_done = 1 WHERE hash_key = ?`, hk);
	}

	/**
	 * Idempotent: used both when queueing a new promotion and when inheriting entries on hash
	 * split. Returns whether a new row was actually inserted — false means the key already had a
	 * row (whose status may differ from `status`), so callers keeping an in-memory cache must
	 * resync from storage instead of assuming `status` was written.
	 */
	insertPromotedKey(hk: KeyBytes, status: PromotedKeyStatus, now: number): { inserted: boolean } {
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
	updatePromotedKeyStatus(
		hk: KeyBytes,
		fromStatus: PromotedKeyStatus,
		toStatus: PromotedKeyStatus,
		updatedAt: number,
	): { updated: boolean } {
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
				? this.#storage.sql.exec<{ hash_key: ArrayBuffer; status: PromotedKeyStatus }>(
						`SELECT hash_key, status FROM promoted_keys WHERE hash_key > ? ORDER BY hash_key LIMIT ?`,
						cursor.hashKey,
						limit,
					)
				: this.#storage.sql.exec<{ hash_key: ArrayBuffer; status: PromotedKeyStatus }>(
						`SELECT hash_key, status FROM promoted_keys ORDER BY hash_key LIMIT ?`,
						limit,
					)
		)
			.toArray()
			.map((r) => ({ hash_key: fromSqlKey(r.hash_key), status: r.status }));
	}

	// ─── range_hierarchy ────────────────────────────────────────────────────

	/**
	 * Called exactly once, from initFromSplit, before any concurrent request can reach this DO.
	 * Boundaries are already decoded to the public wire representation (see `RangeAncestorInfo`).
	 */
	setRangeAncestors(ancestors: RangeAncestorInfo[]): void {
		for (const a of ancestors) {
			this.#storage.sql.exec(
				`INSERT INTO range_hierarchy (hk, depth, sk_start_boundary, sk_end_boundary) VALUES (?, ?, ?, ?)`,
				// For range partitions the hash key is always the empty sentinel (the partition's data keyspace is a range of a single hash key).
				KeyCodec.encodeOptional(undefined),
				a.depth,
				a.startBoundary,
				a.endBoundary,
			);
		}
	}

	/**
	 * Only rows with depth strictly less than `ltDepth` are ancestors — filtering here (rather than
	 * relying on callers) keeps this method correct once `range_hierarchy` also holds descendant-side
	 * cache entries (depth > ltDepth), which this table is named generically to support later.
	 */
	getRangeAncestors(ltDepth: number): RangeAncestorInfo[] {
		return this.#storage.sql
			.exec<{ depth: number; sk_start_boundary: ArrayBuffer; sk_end_boundary: ArrayBuffer }>(
				`SELECT depth, sk_start_boundary, sk_end_boundary FROM range_hierarchy WHERE depth < ? ORDER BY depth ASC`,
				ltDepth,
			)
			.toArray()
			.map((r) => ({ depth: r.depth, startBoundary: fromSqlKey(r.sk_start_boundary), endBoundary: fromSqlKey(r.sk_end_boundary) }));
	}

	insertRangePartitionBoundary(hk: KeyBytes, startBoundary: KeyBytes, endBoundary: KeyBytes, depth: number): void {
		// FIXME: Add a limit on the storage we use for this table, or a TTL, or a cleanup policy.
		// The range router can learn many boundaries over time, and we don't want to keep them or grow forever.
		//
		// We use INSERT OR IGNORE here to avoid causing writes for the same boundaries.
		// This is called on every forwarded request so we need to avoid unnecessary writes.
		this.#storage.sql.exec(
			`INSERT OR IGNORE INTO range_hierarchy (hk, depth, sk_start_boundary, sk_end_boundary) VALUES (?, ?, ?, ?)`,
			hk,
			depth,
			startBoundary,
			endBoundary,
		);
	}

	/**
	 * Returns the deepest learned range slice (from `range_hierarchy`) that contains `sortKey` for the
	 * given hash key, or `null` when nothing is known that covers it. Used to skip intermediate range
	 * router hops: the returned `[startBoundary, endBoundary)` slice resolves deterministically to a DO.
	 */
	findDeepestKnownRangeSlice(
		hk: KeyBytes,
		sortKey: KeyBytes,
	): { depth: number; startBoundary: KeyBytes | null; endBoundary: KeyBytes | null } | null {
		// Boundaries are stored with the empty sentinel `[]` for unbounded edges (consistent with the start
		// side and `getRangeAncestors`). `[]` is the byte minimum, which is correct for an unbounded start
		// (`start <= sortKey` always holds) but NOT for an unbounded end — hence the explicit sentinel check
		// in the WHERE clause. Real keys are never empty (KeyCodec rejects empty input), so `[]` is an
		// unambiguous "unbounded" tag. The sentinel semantics stay encapsulated here: the result decodes
		// `[]` back to `null` for both edges, so callers can feed `resolveRangePartitionContext` directly.
		const unbounded = KeyCodec.encodeOptional(undefined);
		const row = this.#storage.sql
			.exec<{ depth: number; sk_start_boundary: ArrayBuffer; sk_end_boundary: ArrayBuffer }>(
				`SELECT depth, sk_start_boundary, sk_end_boundary
				 FROM range_hierarchy
				 WHERE hk = ?
				   AND sk_start_boundary <= ?
				   AND (sk_end_boundary > ? OR sk_end_boundary = ?)
				 ORDER BY depth DESC
				 LIMIT 1`,
				hk,
				sortKey,
				sortKey,
				unbounded,
			)
			.toArray()[0];
		if (!row) return null;

		const start = fromSqlKey(row.sk_start_boundary);
		const end = fromSqlKey(row.sk_end_boundary);
		return {
			depth: row.depth,
			// Decode the empty sentinel back to null (unbounded) so callers feed resolveRangePartitionContext directly.
			startBoundary: start.length === 0 ? null : start,
			endBoundary: end.length === 0 ? null : end,
		};
	}
}
