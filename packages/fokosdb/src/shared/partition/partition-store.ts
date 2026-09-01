import { SQLSchemaMigration, SQLSchemaMigrations } from "durable-utils/sql-migrations";
import { DATA_KINDS, type DataKind } from "../types.js";
import type { RangeAncestorInfo } from "../partition-topology/types.js";
import { KeyCodec, type KeyBytes } from "../partition-topology/key-codec.js";
import invariant from "../invariant.js";
import type { CompiledConditionPlan } from "../expression/plan.js";
import { evaluateConditionPlan, type ConditionEvaluationResult } from "../expression/runtime.js";

// The on-disk `data_kind` code for json rows. json is stored as JSONB (a BLOB); a public read must
// decode it to JSON text in SQL (`json(data)`) so JS never touches raw JSONB, while a migration read
// copies the JSONB blob verbatim. This fixed integer is safe to interpolate into SQL.
const JSON_KIND_CODE = DATA_KINDS.indexOf("json");

// Public-read data projection: json rows decode to JSON text; bytes/text pass through untouched.
const DATA_SELECT_DECODED = `CASE WHEN data_kind = ${JSON_KIND_CODE} THEN json(data) ELSE data END AS data`;

// Fixed per-row overhead added to est_row_bytes. See the "items" migration for what K covers.
export const EST_ROW_BYTES_K = 100;

/**
 * The ONLY definition of the est_row_bytes formula. `dataExpr` must be the same SQL that produces the
 * stored `data` value (e.g. `jsonb(?6)`), so the size is always measured on what SQLite writes.
 */
function estRowBytesExpr(dataExpr: string, hkParam: string, skParam: string): string {
	return `octet_length(${dataExpr}) + octet_length(${hkParam}) + octet_length(${skParam}) + ${EST_ROW_BYTES_K}`;
}

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
	// Public reads decode json to JSON text; migration reads carry the raw JSONB blob (Uint8Array).
	data: string | Uint8Array;
	kind: DataKind;
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
	// data and its kind are absent together: null for delete/check ops, present for put.
	data: string | Uint8Array | null;
	kind: DataKind | null;
	conditions_json: string | null;
	ttl_epoch_utc_seconds: number | null;
	coordinator_do_id: string;
	created_at: number;
	guarded_at: number | null;
};

export type PendingTransactionCursor = { hk: KeyBytes; sk: KeyBytes; transaction_id: string };

export type ScanCursor = { hk: KeyBytes; sk: KeyBytes; inclusive?: boolean };

export type PromotedKeyCursor = { hashKey: KeyBytes };

export type PromotedKeyStatus = "queued" | "promoting" | "promoted";

export type PromotedKeyRow = { hash_key: KeyBytes; status: PromotedKeyStatus };

export type SqlMetrics = { rowsRead: number; rowsWritten: number };

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

// Maps the on-disk integer `data_kind` code back to its string discriminant. The SELECTs cast the
// column to number; index math (never a lookup table) keeps it drift-proof with DATA_KINDS.
function kindFromCode(code: number): DataKind {
	const kind = DATA_KINDS[code];
	invariant(kind !== undefined, `fokos/partition-store: unknown data_kind code`);
	return kind;
}

// pending_transactions/tc_items rows for delete/check ops carry no data, so their data_kind is NULL —
// kind and data are absent together. `null` code ⇒ `null` kind; a real code maps through kindFromCode.
function kindFromNullableCode(code: number | null): DataKind | null {
	return code === null ? null : kindFromCode(code);
}
function codeFromNullableKind(kind: DataKind | null): number | null {
	if (kind === null) return null;
	const code = DATA_KINDS.indexOf(kind);
	invariant(code !== -1, `fokos/partition-store: unknown data kind`);
	return code;
}

export function estimateItemBytes(item: MigratedItem): number {
	const dataSize = typeof item.data === "string" ? item.data.length * 2 : item.data.byteLength;
	return item.hk.byteLength + item.sk.byteLength + dataSize + 8 + 64;
}

export function estimatePendingTxBytes(row: PendingTransactionRow): number {
	const dataSize = row.data == null ? 0 : typeof row.data === "string" ? row.data.length * 2 : row.data.byteLength;
	return row.hk.byteLength + row.sk.byteLength + 32 + 8 + 8 + 8 + dataSize + (row.conditions_json?.length ?? 0) * 2 + 64;
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
		// `data` is ANY so SQLite retains the physical storage class: TEXT for text, BLOB for bytes, and
		// BLOB for json (SQLite's JSONB binary form). `data_kind` is the discriminant that tells the three
		// apart (JSONB and bytes are both blobs).
		//
		// This is a rowid table on purpose. Do NOT add WITHOUT ROWID back. WITHOUT ROWID stores rows in an
		// index B-tree, whose inline payload limit is ~1002 bytes on a 4 KiB page. Every item above that
		// limit then takes a private overflow page it cannot share. Measured on Durable Object storage:
		// 1000-byte data costs 4683 physical bytes per row (4.7x), and 2000-byte data costs 2.3x.
		// `PRIMARY KEY (hk, sk)` still enforces uniqueness through sqlite_autoindex_items_1. hk/sk keep
		// their explicit NOT NULL because a rowid table does NOT imply it from the primary key.
		//
		// `est_row_bytes` is the true encoded byte size (octet_length: UTF-8 bytes for TEXT, blob bytes for
		// BLOB/JSONB). It is a plain column, NOT a generated column: SQLite refuses to treat an index that
		// holds a generated column as covering, which would send the est_row_bytes scans back to every wide
		// item row. Both writers build it with estRowBytesExpr, so SQLite still measures the stored value
		// and JS never estimates it.
		//
		// octet_length covers the variable part (data + keys); K covers the fixed per-row remainder: the
		// four wide integer columns, the data_kind enum, the B-tree record header, the rowid, and the two
		// index entries each row carries (the PK autoindex + idx_items_scan).
		// K is a rough size-accounting knob (feeds promotion/split), not a precise figure.
		//
		// SQLite reads pages for each row until it satisfies the columns needed for the query.
		// Moving the "data" column to the end of the table definition keeps the hot-path SELECTs
		// from reading the potentially large data column (overflow pages) when they only need the small metadata columns.
		//
		// idx_items_scan makes the est_row_bytes scans (computeRangeSplitBoundaries, rebuildKeySizeEstimates)
		// index-only, so they never touch the wide item rows.
		sql: `
            CREATE TABLE IF NOT EXISTS items (
                hk                    BLOB    NOT NULL,
                sk                    BLOB    NOT NULL DEFAULT x'',
                data_kind             INTEGER NOT NULL DEFAULT 0,
                v                     INTEGER NOT NULL,
                last_transaction_ts   INTEGER NOT NULL DEFAULT 0,
				ttl_epoch_utc_seconds INTEGER,
                est_row_bytes         INTEGER NOT NULL,
				data                  ANY     NOT NULL,

                PRIMARY KEY (hk, sk)
            ) STRICT;

            CREATE INDEX IF NOT EXISTS idx_items_scan ON items (hk, sk, est_row_bytes);
            CREATE INDEX IF NOT EXISTS idx_items_ttl ON items (ttl_epoch_utc_seconds, hk, sk)
                WHERE ttl_epoch_utc_seconds IS NOT NULL;`,
	},
	{
		idMonotonicInc: 2,
		description: "Add last_transaction_ts to items and create transaction support tables",
		// pending_transactions is a rowid table on purpose. Do NOT add WITHOUT ROWID back — it was
		// WITHOUT ROWID until 2026-08-16, for the same reason `items` was, and it has the same defect:
		// WITHOUT ROWID stores rows in an index B-tree with a ~1002-byte inline payload limit on a 4 KiB
		// page, so every row whose `data` exceeds that takes a private overflow page it cannot share.
		// Measured on Durable Object storage, 2000 rows of 1500-byte data against 3.00 MB logical:
		// 9.41 MB WITHOUT ROWID (3.1x) against 4.20 MB as a rowid table (1.4x). `PRIMARY KEY
		// (hk, sk, transaction_id)` still enforces uniqueness through sqlite_autoindex_pending_transactions_1,
		// and hk/sk/transaction_id keep their explicit NOT NULL because a rowid table does NOT imply it
		// from the primary key.
		//
		// pending_transactions_transaction_id exists because `transaction_id` is the THIRD primary key
		// column and so cannot be seeked on its own. Every whole-transaction operation filters by it —
		// pendingTxCountFor, listPendingTxKeys, listPendingTxItems, deletePendingTx. Also, deletePendingTx
		// and listPendingTxKeys run on every commit and abort, so without this index the cost of
		// committing ONE transaction is O(all pending rows in the partition). Measured over 20k pending
		// rows: 147 page reads drop to 3, and listPendingTxItems drops from 2859 to 8.
		//
		// Its key carries (hk, sk) EXPLICITLY, and that is what pays for the rowid change. While the
		// table was WITHOUT ROWID, SQLite appended the primary key to every index entry, so a key of
		// `transaction_id` alone happened to cover listPendingTxKeys for free. A rowid table appends
		// only the rowid, so the narrow key would have sent listPendingTxKeys — a commit-and-abort path —
		// back to a table fetch per row, and SQLite also stopped choosing pending_transactions_created_at
		// for listStalePendingTx, scanning this index instead. Spelling (hk, sk) out restores both plans.
		sql: `
            CREATE TABLE IF NOT EXISTS pending_transactions (
                hk                    BLOB    NOT NULL,
                sk                    BLOB    NOT NULL DEFAULT x'',
                transaction_id        TEXT    NOT NULL,
                transaction_ts        INTEGER NOT NULL,
				created_at            INTEGER NOT NULL,
				coordinator_do_id     TEXT    NOT NULL DEFAULT '',
                operation             TEXT    NOT NULL,
                data_kind             INTEGER, -- NULL for delete/check (no data); set for put
                conditions_json       TEXT,
                ttl_epoch_utc_seconds INTEGER,
				guarded_at            INTEGER,
				data                  ANY,
                PRIMARY KEY (hk, sk, transaction_id)
            ) STRICT;

            CREATE INDEX IF NOT EXISTS pending_transactions_created_at ON pending_transactions (created_at);
            CREATE INDEX IF NOT EXISTS pending_transactions_transaction_id ON pending_transactions (transaction_id, hk, sk);

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
		description: "Add per-hash key size estimate and key-level size summary for efficient promotion detection",
		// FIXME: Add also number of items per hash key.
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

	runMigrations(): void {
		this.#migrations.runAllSync();
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
		row?: { data: string | Uint8Array; kind: DataKind; ttl_epoch_utc_seconds: number | null; v: number; last_transaction_ts: number };
		rowsRead: number;
		rowsWritten: number;
	} {
		const res = this.#storage.sql.exec<{
			data: string | ArrayBuffer;
			data_kind: number;
			ttl_epoch_utc_seconds: number | null;
			v: number;
			last_transaction_ts: number;
		}>(
			`SELECT ${DATA_SELECT_DECODED}, data_kind, ttl_epoch_utc_seconds, v, last_transaction_ts FROM items WHERE hk = ? AND sk = ? LIMIT 1`,
			hk,
			sk,
		);
		const row = res.toArray()[0];
		if (!row) return { row: undefined, rowsRead: res.rowsRead, rowsWritten: res.rowsWritten };
		const { data_kind, ...rest } = row; // data_kind → the readable `kind`; don't leak the raw code
		return {
			row: { ...rest, data: fromSqlData(row.data), kind: kindFromCode(data_kind) },
			rowsRead: res.rowsRead,
			rowsWritten: res.rowsWritten,
		};
	}

	/** Lightweight existence and timestamp read for an unconditional transaction prepare. */
	getItemStamp(hk: KeyBytes, sk: KeyBytes): { row?: { last_transaction_ts: number }; rowsRead: number; rowsWritten: number } {
		const res = this.#storage.sql.exec<{ last_transaction_ts: number }>(
			`SELECT last_transaction_ts FROM items WHERE hk = ? AND sk = ? LIMIT 1`,
			hk,
			sk,
		);
		const row = res.toArray()[0];
		return { row, rowsRead: res.rowsRead, rowsWritten: res.rowsWritten };
	}

	evaluateCondition(plan: CompiledConditionPlan, hk: KeyBytes, sk: KeyBytes): ConditionEvaluationResult {
		return evaluateConditionPlan(this.#storage, plan, hk, sk);
	}

	/**
	 * The row's currently stored `est_row_bytes`, or 0 when the row is absent. `upsertItem` and
	 * `deleteItem` both need it to compute the `key_size_estimates` delta, and it is the only read
	 * either of them does, so it sits on both write paths.
	 *
	 * `INDEXED BY idx_items_scan` is a deliberate plan pin, not decoration. `idx_items_scan
	 * (hk, sk, est_row_bytes)` covers this query exactly, but SQLite prefers
	 * `sqlite_autoindex_items_1` — the primary-key index, which stops at `(hk, sk)` — and then
	 * fetches the table row for `est_row_bytes`. Measured plans:
	 *
	 *   without the hint: SEARCH items USING INDEX sqlite_autoindex_items_1 (hk=? AND sk=?)
	 *   with the hint:    SEARCH items USING COVERING INDEX idx_items_scan (hk=? AND sk=?)
	 *
	 * The hint removes that table-row fetch from every put and every delete. It saves page reads, not
	 * billed rows — SQLite reports one row read either way. `INDEXED BY` makes the dependency on
	 * `idx_items_scan` hard: dropping or renaming that index fails this query loudly rather than
	 * silently regressing both write paths. A query-plan test asserts the pin still works AND that it
	 * is still needed, so it can be removed if SQLite ever picks the covering index unaided.
	 *
	 * Folding into the DELETE with `DELETE ... RETURNING` measured that reports the returned row
	 * as an extra read, so it costs the same on a hit and one more on a miss.
	 *
	 * Do NOT replace this read with `AFTER INSERT/UPDATE/DELETE` triggers on items that maintain
	 * key_size_estimates. A trigger fires for EVERY writer of items, including the two that do their
	 * size accounting in bulk: insertItemIfAbsent (migration ingest, followed by one
	 * rebuildKeySizeEstimates) and deleteItemsBatchForHashKey (GC, 1000 rows per call, followed by one
	 * deleteKeySizeEstimate). It would add one row WRITE per row on those paths to save one row READ here.
	 */
	#storedEstRowBytes(hk: KeyBytes, sk: KeyBytes): number {
		const row = this.#storage.sql
			.exec<{ est_row_bytes: number }>(`SELECT est_row_bytes FROM items INDEXED BY idx_items_scan WHERE hk = ? AND sk = ? LIMIT 1`, hk, sk)
			.toArray()[0];
		return row?.est_row_bytes ?? 0;
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
		/** json ⇒ `data` is JSON text, wrapped with jsonb() into the binary form on write. */
		data: string | Uint8Array;
		kind: DataKind;
		ttlAt: number | null;
		lastTransactionTs: number;
	}): {
		version: number;
		keyEstBytes: number;
		rowsRead: number;
		rowsWritten: number;
	} {
		const oldEst = this.#storedEstRowBytes(opts.hk, opts.sk);

		// json text is encoded to JSONB inside the DO; bytes/text bind verbatim. This is a fixed SQL
		// fragment chosen by the kind discriminant, never user input, so it is injection-safe.
		// data binds last (?6) because est_row_bytes must measure this same expression.
		const dataExpr = opts.kind === "json" ? "jsonb(?6)" : "?6";

		// INVARIANT: last_transaction_ts is monotonic per item — it must never move backwards.
		//
		// `prepare` accepts a transaction only when its timestamp is above this value
		// (transaction-participant.ts), so a lower value here would let an already-superseded
		// transaction commit over newer data. The two writers disagree on whose clock they read: a
		// non-transactional put stamps `Date.now()` on this partition's clock, while a committed
		// transaction stamps its coordinator's, which prepare accepts up to MAX_CLOCK_SKEW_MS ahead.
		// MAX is what reconciles them.
		//
		// MAX also cannot drift ahead of the wall clock: it only ever keeps the larger of two values
		// that already exist. Do NOT turn it into an increment (`MAX(last_transaction_ts + 1, ?)`) —
		// that would run the timestamp forward under sustained writes.
		//
		// bumpItemLastTransactionTs applies the same rule for the transactional "check" operation.
		const writeRes = this.#storage.sql.exec<{ v: number; est_row_bytes: number }>(
			`INSERT INTO items (hk, sk, data_kind, ttl_epoch_utc_seconds, v, last_transaction_ts, est_row_bytes, data)
			 VALUES (?1, ?2, ?3, ?4, 1, ?5, ${estRowBytesExpr(dataExpr, "?1", "?2")}, ${dataExpr})
			 ON CONFLICT(hk, sk) DO UPDATE SET
			   data = excluded.data,
			   data_kind = excluded.data_kind,
			   ttl_epoch_utc_seconds = excluded.ttl_epoch_utc_seconds,
			   est_row_bytes = excluded.est_row_bytes,
			   v = v + 1,
			   last_transaction_ts = MAX(last_transaction_ts, excluded.last_transaction_ts)
			 RETURNING v, est_row_bytes`,
			opts.hk,
			opts.sk,
			DATA_KINDS.indexOf(opts.kind),
			opts.ttlAt,
			opts.lastTransactionTs,
			opts.data,
		);
		const rows = writeRes.toArray();
		invariant(rows.length === 1, `fokos/partition-store.upsertItem: RETURNING expected 1 row, got ${rows.length}`);
		const version = rows[0].v;
		invariant(
			typeof version === "number" && Number.isInteger(version) && version >= 1,
			`fokos/partition-store.upsertItem: unexpected version value: ${version}`,
		);
		// Exact stored size, measured by SQLite in the statement above (drives the key_size_estimates delta).
		const newEst = rows[0].est_row_bytes;

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
		const delEst = this.#storedEstRowBytes(opts.hk, opts.sk);

		const writeRes = this.#storage.sql.exec(`DELETE FROM items WHERE hk = ? AND sk = ?`, opts.hk, opts.sk);
		const deleted = writeRes.rowsWritten > 0;
		if (deleted || opts.bumpWatermarkAlways) {
			this.bumpMaxDeletedTs(opts.watermarkTs);
			this.#storage.sql.exec(`UPDATE key_size_estimates SET est_bytes = MAX(0, est_bytes - ?) WHERE hk = ?`, delEst, opts.hk);
		}
		return { deleted, rowsRead: writeRes.rowsRead, rowsWritten: writeRes.rowsWritten };
	}

	/**
	 * Deletes one bounded chunk of expired, unlocked items and updates all deletion bookkeeping in the
	 * same storage transaction. The victim scan uses only metadata columns and never reads item data.
	 */
	deleteExpiredItems(nowSeconds: number, limit: number): { deletedRows: number; deletedBytes: number } {
		return this.transactionSync(() => {
			const rows = this.#storage.sql
				.exec<{ hk: ArrayBuffer; est_row_bytes: number; ttl_epoch_utc_seconds: number }>(
					`DELETE FROM items
					 WHERE rowid IN (
					     SELECT i.rowid FROM items i INDEXED BY idx_items_ttl
					      WHERE i.ttl_epoch_utc_seconds IS NOT NULL
					        AND i.ttl_epoch_utc_seconds <= ?1
					        AND NOT EXISTS (SELECT 1 FROM pending_transactions p WHERE p.hk = i.hk AND p.sk = i.sk)
					        AND NOT EXISTS (SELECT 1 FROM promoted_keys pk WHERE pk.hash_key = i.hk)
					      ORDER BY i.ttl_epoch_utc_seconds, i.hk, i.sk
					      LIMIT ?2
					 )
					 RETURNING hk, est_row_bytes, ttl_epoch_utc_seconds`,
					nowSeconds,
					limit,
				)
				.toArray();

			const bytesByHashKey = new Map<string, { hk: KeyBytes; bytes: number }>();
			let deletedBytes = 0;
			let maxExpirySeconds = 0;
			for (const row of rows) {
				const hk = fromSqlKey(row.hk);
				const key = hk.toBase64({ alphabet: "base64url" });
				const current = bytesByHashKey.get(key);
				if (current) current.bytes += row.est_row_bytes;
				else bytesByHashKey.set(key, { hk, bytes: row.est_row_bytes });
				deletedBytes += row.est_row_bytes;
				maxExpirySeconds = Math.max(maxExpirySeconds, row.ttl_epoch_utc_seconds);
			}

			for (const { hk, bytes } of bytesByHashKey.values()) {
				this.#storage.sql.exec(`UPDATE key_size_estimates SET est_bytes = MAX(0, est_bytes - ?) WHERE hk = ?`, bytes, hk);
			}
			if (rows.length > 0) this.bumpMaxDeletedTs(maxExpirySeconds * 1000);

			return { deletedRows: rows.length, deletedBytes };
		});
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
		// Migration copies the stored representation verbatim: for json rows `item.data` is the raw
		// JSONB blob, bound directly (no jsonb() re-encode). data binds last (?7) so est_row_bytes can
		// measure the same parameter.
		this.#storage.sql.exec(
			`INSERT OR IGNORE INTO items (hk, sk, data_kind, ttl_epoch_utc_seconds, v, last_transaction_ts, est_row_bytes, data)
			 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ${estRowBytesExpr("?7", "?1", "?2")}, ?7)`,
			item.hk,
			item.sk,
			DATA_KINDS.indexOf(item.kind),
			item.ttl_epoch_utc_seconds ?? null,
			item.v,
			item.last_transaction_ts,
			item.data,
		);
	}

	hasItemsForHashKey(hk: KeyBytes): boolean {
		return this.#storage.sql.exec(`SELECT 1 FROM items WHERE hk = ? LIMIT 1`, hk).toArray().length > 0;
	}

	/**
	 * Computes N-1 strictly-increasing split boundaries (byte-quantiles) within [start, end) in one
	 * transactionSync snapshot. Returns null if the slice cannot yield N non-empty children.
	 *
	 * Each boundary is shortened to the minimum prefix that still separates adjacent data keys (the
	 * "shortest separator" of the predecessor and crossing key), keeping doNames and topology encoding
	 * compact. A data query on the items table — it lives with the store; the DO passes the result into
	 * the range split policy's prepareSplit.
	 *
	 * Assumes the caller (do-partition startSplit) passes this partition's own [start, end) ownership,
	 * so the slice being split == all items this DO holds for `hashKey`. That is why the O(1) whole-key
	 * est_bytes total is a valid byte basis: a splitting parent owns its entire slice and its children
	 * pull sub-slices during migration, so est_bytes[hk] equals the slice's bytes. The start/end SQL
	 * filter is retained as a defensive bound on the scan.
	 */
	computeRangeSplitBoundaries(hashKey: KeyBytes, start: KeyBytes | null, end: KeyBytes | null, N: number): KeyBytes[] | null {
		// Rather than a COUNT(*) pass plus N-1 OFFSET re-walks (~2.5·cnt row touches, all count-balanced),
		// this reads the O(1) est_bytes total and does a single early-stopping streaming scan that emits a
		// boundary each time the running est_row_bytes total crosses the next byte threshold, breaking after
		// the (N-1)th boundary (~0.75·cnt at N=4). Byte-balance — not count-balance — is the right metric
		// because the split is triggered by size; it also isolates a heavy row into its own child.
		return this.#storage.transactionSync(() => {
			const lower = start ?? KeyCodec.encodeOptional(undefined); // −∞ ⇒ sk >= x'' (the empty sentinel)

			// Total bytes in O(1) from the maintained per-hk estimate. Nothing to split ⇒ null.
			const B =
				this.#storage.sql.exec<{ est_bytes: number }>(`SELECT est_bytes FROM key_size_estimates WHERE hk = ?`, hashKey).toArray()[0]
					?.est_bytes ?? 0;
			if (B <= 0) return null;

			// Cheap "≥ N items" guard, O(N) not O(cnt): each child needs ≥ 1 item, so probe with a bounded
			// count rather than a full pass. Fewer than N items ⇒ cannot split into N non-empty children.
			const guardRow =
				end === null
					? this.#storage.sql
							.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM (SELECT 1 FROM items WHERE hk = ? AND sk >= ? LIMIT ?)`, hashKey, lower, N)
							.toArray()[0]
					: this.#storage.sql
							.exec<{
								n: number;
							}>(`SELECT COUNT(*) AS n FROM (SELECT 1 FROM items WHERE hk = ? AND sk >= ? AND sk < ? LIMIT ?)`, hashKey, lower, end, N)
							.toArray()[0];
			if ((guardRow?.n ?? 0) < N) {
				console.warn({
					message: "fokos/partition-store.computeRangeSplitBoundaries: cannot split, fewer than N items",
					hashKey: KeyCodec.keyForLog(hashKey),
					start: start ? KeyCodec.keyForLog(start) : null,
					end: end ? KeyCodec.keyForLog(end) : null,
					N,
					itemCount: guardRow?.n ?? 0,
				});
				return null;
			}

			// Single streaming scan, accumulating est_row_bytes and emitting a boundary at each byte threshold.
			const step = B / N;
			const cursor =
				end === null
					? this.#storage.sql.exec<{ sk: ArrayBuffer; est_row_bytes: number }>(
							`SELECT sk, est_row_bytes FROM items WHERE hk = ? AND sk >= ? ORDER BY sk`,
							hashKey,
							lower,
						)
					: this.#storage.sql.exec<{ sk: ArrayBuffer; est_row_bytes: number }>(
							`SELECT sk, est_row_bytes FROM items WHERE hk = ? AND sk >= ? AND sk < ? ORDER BY sk`,
							hashKey,
							lower,
							end,
						);

			const boundaries: KeyBytes[] = [];
			let acc = 0;
			let threshold = step;
			let prev: KeyBytes | null = null;
			for (const row of cursor) {
				const sk = fromSqlKey(row.sk);
				acc += row.est_row_bytes;
				// prev !== null: the first row can never be a boundary, so child 0 always owns ≥ 1 row.
				if (prev !== null && acc >= threshold && boundaries.length < N - 1) {
					// Byte-space separator: the boundary's UTF-8 position matches the SQL scans that migrate the data.
					// The crossing row (sk) falls into the upper child; prev is its predecessor.
					boundaries.push(KeyCodec.shortestSeparator(prev, sk));
					// Relative bump (acc + step, not threshold += step): if one oversized row pushes acc past
					// several thresholds at once, we still emit only one boundary and re-anchor here, so no two
					// boundaries land on the same adjacent-key pair. Also guarantees ≥ 1 row per child.
					threshold = acc + step;
					if (boundaries.length === N - 1) break;
				}
				prev = sk;
			}

			// Boundaries must be strictly above the lower bound and strictly increasing (distinct, non-empty
			// children). On skewed data the scan may yield fewer than N-1 boundaries; treat any shortfall or
			// validation failure as "cannot split yet" and return null (the split retries later). This
			// is the safety net that makes estimate inaccuracy harmless.
			if (boundaries.length !== N - 1) return null;
			for (let i = 0; i < boundaries.length; i++) {
				invariant(
					KeyCodec.compare(boundaries[i], lower) > 0,
					`fokos/partition-store.computeRangeSplitBoundaries: boundary is not above lower bound`,
				);
				if (i > 0) {
					invariant(
						KeyCodec.compare(boundaries[i], boundaries[i - 1]) > 0,
						`fokos/partition-store.computeRangeSplitBoundaries: boundaries are not strictly increasing`,
					);
				}
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

	/**
	 * Pages the items table in (hk, sk) order, strictly after `cursor`. This is a migration read: json
	 * rows return the raw JSONB blob verbatim (no `json()` decode) so the child re-inserts it unchanged.
	 *
	 * The cursor MUST stay a row-value comparison `(hk, sk) > (?, ?)`. Do NOT rewrite it as
	 * `hk > ? OR (hk = ? AND sk > ?)`: SQLite cannot seek on that form — it takes only `hk > ?` as the
	 * index bound and re-checks every remaining row. A range partition holds one hash key, so each page
	 * would restart at that key's first row and a full pass would be quadratic. Measured over 50k rows
	 * under one hk: 337 page reads for a late page against 4 for the row-value form.
	 *
	 * Row values are only correct because hk/sk are NOT NULL (see the items migration). A NULL on either
	 * side makes the comparison NULL instead of true, which drops rows silently. A key with no sort key
	 * stores the empty blob, which is the byte minimum and compares like any other value.
	 */
	queryItemsPage(cursor: ScanCursor | null, limit: number): MigratedItem[] {
		type Row = {
			hk: ArrayBuffer;
			sk: ArrayBuffer;
			data: string | ArrayBuffer;
			data_kind: number;
			ttl_epoch_utc_seconds: number | null;
			v: number;
			last_transaction_ts: number;
		};

		let sqlCursor: SqlStorageCursor<Row>;
		if (!cursor) {
			sqlCursor = this.#storage.sql.exec<Row>(
				`SELECT hk, sk, data, data_kind, ttl_epoch_utc_seconds, v, last_transaction_ts FROM items ORDER BY hk, sk LIMIT ?`,
				limit,
			);
		} else {
			sqlCursor = this.#storage.sql.exec<Row>(
				`SELECT hk, sk, data, data_kind, ttl_epoch_utc_seconds, v, last_transaction_ts FROM items WHERE (hk, sk) > (?, ?) ORDER BY hk, sk LIMIT ?`,
				cursor.hk,
				cursor.sk,
				limit,
			);
		}

		const items: MigratedItem[] = [];
		for (const { data_kind, ...row } of sqlCursor) {
			items.push({
				...row,
				hk: fromSqlKey(row.hk),
				sk: fromSqlKey(row.sk),
				data: fromSqlData(row.data),
				kind: kindFromCode(data_kind),
			});
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
	 *
	 * `decodeJson` selects the data projection: public reads (queryItems) pass `true` to decode json
	 * rows to JSON text in SQL; migration reads pass `false` to copy the raw JSONB blob verbatim.
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
		decodeJson: boolean;
	}): MigratedItem[] {
		type Row = {
			hk: ArrayBuffer;
			sk: ArrayBuffer;
			data: string | ArrayBuffer;
			data_kind: number;
			ttl_epoch_utc_seconds: number | null;
			v: number;
			last_transaction_ts: number;
		};
		const dataProjection = opts.decodeJson ? DATA_SELECT_DECODED : "data";
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
				`SELECT hk, sk, ${dataProjection}, data_kind, ttl_epoch_utc_seconds, v, last_transaction_ts FROM items WHERE ${conds.join(" AND ")} ORDER BY sk ${opts.direction === "asc" ? "ASC" : "DESC"} LIMIT ?`,
				...params,
				opts.limit,
			)
			.toArray();
		return page.map(({ data_kind, ...row }) => ({
			...row,
			hk: fromSqlKey(row.hk),
			sk: fromSqlKey(row.sk),
			data: fromSqlData(row.data),
			kind: kindFromCode(data_kind),
		}));
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
			// pending_transactions is never queried by JSON path, so json data is stored raw (as text),
			// not JSONB, the data_kind tag lets commit reconstruct the kind for upsertItem.
			`INSERT OR IGNORE INTO pending_transactions
			   (hk, sk, transaction_id, transaction_ts, operation, data, data_kind, conditions_json, ttl_epoch_utc_seconds, coordinator_do_id, created_at, guarded_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			row.hk,
			row.sk,
			row.transaction_id,
			row.transaction_ts,
			row.operation,
			row.data,
			codeFromNullableKind(row.kind),
			row.conditions_json,
			row.ttl_epoch_utc_seconds,
			row.coordinator_do_id,
			row.created_at,
			row.guarded_at,
		);
	}

	pendingTxCountFor(transactionId: string): number {
		return (
			this.#storage.sql
				.exec<{ n: number }>(`SELECT COUNT(*) as n FROM pending_transactions WHERE transaction_id = ?`, transactionId)
				.toArray()[0]?.n ?? 0
		);
	}

	/** Does this partition hold any pending lock? */
	hasAnyPendingTx(): boolean {
		return this.#storage.sql.exec(`SELECT 1 FROM pending_transactions LIMIT 1`).toArray().length > 0;
	}

	/**
	 * Does this partition hold any unguarded lock? The alarm scheduler asks once per background pass to
	 * decide whether to arm stale-transaction recovery.
	 *
	 * `LIMIT 1` is intentional. A count walks every matching entry, while this query stops at the first
	 * unguarded row.
	 */
	hasAnyUnguardedPendingTx(): boolean {
		return this.#storage.sql.exec(`SELECT 1 FROM pending_transactions WHERE guarded_at IS NULL LIMIT 1`).toArray().length > 0;
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

	getPendingTxOp(
		hk: KeyBytes,
		sk: KeyBytes,
		transactionId: string,
	): { operation: string; data: string | Uint8Array | null; kind: DataKind | null; ttl_epoch_utc_seconds: number | null } | undefined {
		const row = this.#storage.sql
			.exec<{
				operation: string;
				data: string | ArrayBuffer | null;
				data_kind: number | null;
				ttl_epoch_utc_seconds: number | null;
			}>(
				`SELECT operation, data, data_kind, ttl_epoch_utc_seconds FROM pending_transactions WHERE hk = ? AND sk = ? AND transaction_id = ? LIMIT 1`,
				hk,
				sk,
				transactionId,
			)
			.toArray()[0];
		return row
			? {
					operation: row.operation,
					data: fromSqlData(row.data),
					kind: kindFromNullableCode(row.data_kind),
					ttl_epoch_utc_seconds: row.ttl_epoch_utc_seconds,
				}
			: undefined;
	}

	/** Stale-transaction recovery: the locked items of one transaction, data converted. */
	listPendingTxItems(transactionId: string): {
		hk: KeyBytes;
		sk: KeyBytes;
		transaction_ts: number;
		operation: string;
		data: string | Uint8Array | null;
		kind: DataKind | null;
		ttl_epoch_utc_seconds: number | null;
		created_at: number;
		guarded_at: number | null;
	}[] {
		return this.#storage.sql
			.exec<{
				hk: ArrayBuffer;
				sk: ArrayBuffer;
				transaction_ts: number;
				operation: string;
				data: string | ArrayBuffer | null;
				data_kind: number | null;
				ttl_epoch_utc_seconds: number | null;
				created_at: number;
				guarded_at: number | null;
			}>(
				`SELECT hk, sk, transaction_ts, operation, data, data_kind, ttl_epoch_utc_seconds, created_at, guarded_at
				 FROM pending_transactions WHERE transaction_id = ?`,
				transactionId,
			)
			.toArray()
			.map(({ data_kind, ...row }) => ({
				...row,
				hk: fromSqlKey(row.hk),
				sk: fromSqlKey(row.sk),
				data: fromSqlData(row.data),
				kind: kindFromNullableCode(data_kind),
			}));
	}

	/**
	 * Transactions whose locks are older than `staleBeforeTs`, at most `limit` of them.
	 *
	 * `DISTINCT` with a `LIMIT` is not the trap it looks like. SQLite streams it: each row is probed
	 * against the temp B-tree, a new tuple is emitted immediately, and `LIMIT` stops the scan. The
	 * B-tree therefore holds at most `limit` tuples, not every matching row. Measured over 20k pending
	 * rows with only 50 stale: **10 rows read** for `limit = 10`.
	 *
	 * The residual cost is one transaction's width, not the table's: all rows of one prepare share a
	 * `created_at`, so they sit together in the index, and the scan must cross whole transactions to
	 * collect distinct ids. Worst case measured, 20 transactions of 1000 keys each, all stale:
	 * 9,000 rows read for 10 results — `limit` x rows-per-transaction.
	 *
	 * Two alternatives were measured and are worse. Widening `pending_transactions_created_at` to
	 * `(created_at, transaction_id, coordinator_do_id)` makes the plan covering but reads the same
	 * 9,000 rows, and costs 1.35 MB per 20k rows because `coordinator_do_id` is a long string. A
	 * `GROUP BY transaction_id ... HAVING MIN(created_at) < ?` walks the `transaction_id` index, which
	 * cannot use `created_at` at all: 10,000 rows read in the same case, and the whole table in the
	 * common case where few rows are stale.
	 */
	listStalePendingTx(staleBeforeTs: number, limit: number): { transaction_id: string; coordinator_do_id: string }[] {
		return this.#storage.sql
			.exec<{ transaction_id: string; coordinator_do_id: string }>(
				`SELECT DISTINCT transaction_id, coordinator_do_id
                     FROM pending_transactions WHERE created_at < ? AND guarded_at IS NULL LIMIT ?`,
				staleBeforeTs,
				limit,
			)
			.toArray();
	}

	guardPendingTx(transactionId: string, guardedAt: number): boolean {
		return (
			this.#storage.sql.exec(
				`UPDATE pending_transactions SET guarded_at = ? WHERE transaction_id = ? AND guarded_at IS NULL`,
				guardedAt,
				transactionId,
			).rowsWritten > 0
		);
	}

	clearPendingTxGuard(transactionId: string): void {
		this.#storage.sql.exec(`UPDATE pending_transactions SET guarded_at = NULL WHERE transaction_id = ?`, transactionId);
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

	/**
	 * Pages pending_transactions in (hk, sk, transaction_id) order, strictly after `cursor`.
	 *
	 * The cursor MUST stay a row-value comparison, for the reason spelled out on queryItemsPage: the
	 * equivalent nested `hk > ? OR (hk = ? AND (...))` form cannot seek, so each page rescans from the
	 * start of the hash key. All three key columns are NOT NULL, which is what makes row values correct.
	 */
	queryPendingTxPage(cursor: PendingTransactionCursor | null, limit: number): PendingTransactionRow[] {
		type Row = {
			hk: ArrayBuffer;
			sk: ArrayBuffer;
			transaction_id: string;
			transaction_ts: number;
			operation: string;
			data: string | ArrayBuffer | null;
			data_kind: number | null;
			conditions_json: string | null;
			ttl_epoch_utc_seconds: number | null;
			coordinator_do_id: string;
			created_at: number;
			guarded_at: number | null;
		};

		const cols = `hk, sk, transaction_id, transaction_ts, operation, data, data_kind, conditions_json, ttl_epoch_utc_seconds, coordinator_do_id, created_at, guarded_at`;
		let sqlCursor: SqlStorageCursor<Row>;
		if (!cursor) {
			sqlCursor = this.#storage.sql.exec<Row>(`SELECT ${cols} FROM pending_transactions ORDER BY hk, sk, transaction_id LIMIT ?`, limit);
		} else {
			sqlCursor = this.#storage.sql.exec<Row>(
				`SELECT ${cols} FROM pending_transactions
				 WHERE (hk, sk, transaction_id) > (?, ?, ?)
				 ORDER BY hk, sk, transaction_id LIMIT ?`,
				cursor.hk,
				cursor.sk,
				cursor.transaction_id,
				limit,
			);
		}

		const rows: PendingTransactionRow[] = [];
		for (const { data_kind, ...row } of sqlCursor) {
			rows.push({
				...row,
				hk: fromSqlKey(row.hk),
				sk: fromSqlKey(row.sk),
				data: fromSqlData(row.data),
				kind: kindFromNullableCode(data_kind),
			});
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

	/**
	 * Post-migration rebuild: recomputes every key's estimate from the ingested rows, and drops the
	 * estimate of every key that has no rows left.
	 *
	 * The refresh alone touches only keys that still have rows, so a key whose rows have all gone would
	 * keep its old estimate forever. That number is a running total — `upsertItem` adds its delta to
	 * whatever is already there — so a stale base makes every later value for that key too large, and
	 * the key promotes too early. The prune is what makes the method match its name.
	 *
	 * Refresh first, then prune: neither statement needs a transaction, because each one on its own
	 * leaves the table no worse than it found it. `DELETE` everything and re-`INSERT` would need one,
	 * because a crash between the two would zero every estimate.
	 *
	 * The prune uses `NOT EXISTS`, not `hk NOT IN (SELECT hk FROM items)`: `NOT EXISTS` is one index
	 * seek per estimate row, while `NOT IN` materialises every `hk` in items.
	 */
	rebuildKeySizeEstimates(): void {
		this.#storage.sql.exec(
			`INSERT INTO key_size_estimates (hk, est_bytes)
			 SELECT hk, SUM(est_row_bytes) FROM items GROUP BY hk
			 ON CONFLICT(hk) DO UPDATE SET est_bytes = excluded.est_bytes`,
		);
		this.#storage.sql.exec(
			`DELETE FROM key_size_estimates
			 WHERE NOT EXISTS (SELECT 1 FROM items WHERE items.hk = key_size_estimates.hk)`,
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
	 *
	 * `hk` is the partition's own hash key. Every row in this table is written under the real hash key
	 * it describes — ancestors here, learned router boundaries in `insertRangePartitionBoundary` — so
	 * a single convention covers both.
	 */
	setRangeAncestors(hk: KeyBytes, ancestors: RangeAncestorInfo[]): void {
		for (const a of ancestors) {
			this.#storage.sql.exec(
				`INSERT OR IGNORE INTO range_hierarchy (hk, depth, sk_start_boundary, sk_end_boundary) VALUES (?, ?, ?, ?)`,
				hk,
				a.depth,
				a.startBoundary,
				a.endBoundary,
			);
		}
	}

	/**
	 * Ancestor partitions that own `hk`:
	 *
	 * - `hk`: For hash partitions, this table also holds learned router boundaries for other hash keys
	 * 	 (see `insertRangePartitionBoundary`), which are not ancestors of anything here.
	 * - `depth < ltDepth`: For range partitions filtering here rather than relying on callers keeps this method correct
	 *   now that the table also holds descendant-side cache entries (depth >= ltDepth).
	 *
	 * Together they match `idx_range_hierarchy_depth (hk, depth, ...)`, so the query is a covering
	 * seek and needs no sort step. Dropping either one degrades it to a scan plus a temp B-tree.
	 */
	getRangeAncestors(hk: KeyBytes, ltDepth: number): RangeAncestorInfo[] {
		return this.#storage.sql
			.exec<{ depth: number; sk_start_boundary: ArrayBuffer; sk_end_boundary: ArrayBuffer }>(
				`SELECT depth, sk_start_boundary, sk_end_boundary FROM range_hierarchy WHERE hk = ? AND depth < ? ORDER BY depth ASC`,
				hk,
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
