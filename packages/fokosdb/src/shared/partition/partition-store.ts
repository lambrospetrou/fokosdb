import { SQLSchemaMigration, SQLSchemaMigrations } from "durable-utils/sql-migrations";
import { DATA_KINDS, type DataKind, type QuerySelect } from "../types.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import invariant from "../invariant.js";
import { exists, one, tryOne } from "../sql-cursor.js";
import {
	composeQueryStatement,
	UPDATE_MAX_TRAILING_BINDING_COUNT,
	type CompiledConditionPlan,
	type CompiledProjectionPlan,
	type CompiledQueryPlan,
	type CompiledUpdatePlan,
} from "../expression/plan.js";
import { materializedPlanBindings } from "../expression/bindings.js";
import { decodeProjectedRow, type ProjectedWireRow } from "../expression/projection.js";
import {
	evaluateConditionPlan,
	probeUpdatePlan,
	readProjectedItem,
	validateQueryPlan,
	validateUpdatePlan,
	type ConditionEvaluationResult,
	type ProjectedReadResult,
	type UpdateProbeResult,
} from "../expression/runtime.js";
import { MAX_ITEM_BYTES, TX_ORDER_TS_UNITS_PER_MS, decodeItemKeys } from "../transaction-limits.js";
import { FokosValidationError, VALIDATION_CODES } from "../errors.js";
import { withExpressionErrors } from "../errors-operations.js";
import { estRowBytesExpr, itemDataExpr, JSON_KIND_CODE } from "./item-size.js";

// Public-read data projection: json rows decode to JSON text; bytes/text pass through untouched.
const DATA_SELECT_DECODED = `CASE WHEN data_kind = ${JSON_KIND_CODE} THEN json(data) ELSE data END AS data`;

/**
 * The statement-local parameters that follow an update plan's own.
 *
 * `?1` and `?2` are the keys, and the plan owns up to `completeBindingCount`; a statement appends its
 * parameters after that. This numbers them in call order and collects their values in the same order,
 * so the SQL and the bound list cannot drift apart, and a statement that needs one parameter fewer
 * simply asks for one fewer. The widest tail any statement builds sets
 * `UPDATE_MAX_TRAILING_BINDING_COUNT`, which the compiler charges to every update plan.
 *
 * BIND a value that changes between calls; interpolate one that does not. Workers SQLite keeps a
 * prepared statement for each distinct SQL string, so a literal that varies per call compiles a new
 * statement every time. Measured over 2000 runs of one 826-byte UPDATE in the Workers runtime: 56 ms
 * with a bound parameter, 52 ms with a constant literal, and 169 ms with a literal that changed each
 * run. That is why the TTL and the timestamps bind, while the json kind code is interpolated. A
 * compiled document expression varies per update EXPRESSION and not per call, so it keeps its
 * statement across the calls that repeat it.
 */
class StatementTail {
	readonly #offset: number;
	readonly #planBindings: readonly unknown[];
	readonly #values: unknown[] = [];

	constructor(plan: CompiledUpdatePlan) {
		this.#offset = plan.completeBindingCount;
		this.#planBindings = materializedPlanBindings(plan);
	}

	/** Reserves the next parameter for `value` and returns its `?N` reference. */
	param(value: unknown): string {
		// The compiler charges UPDATE_MAX_TRAILING_BINDING_COUNT to EVERY update plan, so a statement
		// that binds a wider tail has silently lowered the budget for every update expression, and the
		// plans already compiled against the old number can overflow the cap on this statement alone.
		invariant(
			this.#values.length < UPDATE_MAX_TRAILING_BINDING_COUNT,
			`fokos/partition-store: statement tail exceeds UPDATE_MAX_TRAILING_BINDING_COUNT (${UPDATE_MAX_TRAILING_BINDING_COUNT})`,
		);
		return `?${this.#offset + this.#values.push(value)}`;
	}

	/** The complete bound list: the keys, then the plan's parameters, then this statement's own. */
	bindings(hk: KeyBytes, sk: KeyBytes): unknown[] {
		// A statement that embeds only documentSql still binds every plan parameter, so a parameter that
		// only applicableSql uses is a gap in the numbering. SQLite counts the parameters of a statement
		// by the HIGHEST index it references, so the count matches this list only while the tail reaches
		// past completeBindingCount. With an empty tail the highest index is whatever documentSql happens
		// to use, and the statement would be given more values than it has parameters.
		invariant(this.#values.length > 0, "fokos/partition-store: an update statement needs at least one tail parameter");
		return [hk, sk, ...this.#planBindings, ...this.#values];
	}
}

/**
 * The size guard that every user write statement carries is an INVARIANT, not a decision.
 *
 * Both transactional write paths measure the row in their check pass, with the same SQL the write
 * uses, and reject there — before anything is written. Only the non-transactional `putItem` has no
 * earlier pass and can legitimately reach this. Raising, rather than reporting zero rows to every
 * caller, keeps that asymmetry in one place: `putItem` lets the error reach its caller, and anywhere
 * else the throw rolls back the storage transaction the write runs inside, which is what an
 * unreachable state deserves.
 */
function throwItemTooLarge(hk: KeyBytes, sk: KeyBytes): never {
	throw new FokosValidationError(VALIDATION_CODES.item_too_large, {
		message: `stored item exceeds ${MAX_ITEM_BYTES / 1024} KB`,
		attributes: decodeItemKeys(hk, sk),
	});
}

/**
 * PartitionStore owns ALL SQL on the partition's data tables: items, pending_transactions,
 * deletion_metadata and key_size_estimates, plus their schema migrations and the row-size
 * estimators. No other class touches these tables. The `fokos_` tables belong to
 * `FokosShardingStore`, and no statement here may name one.
 *
 * Design rules:
 * - Single-purpose methods named for intent; raw SQL is fine because it lives only here.
 * - The CALLER composes multi-statement atomicity with `transactionSync`. The store does not decide
 *   the transaction boundaries.
 * - Row-reading methods return already-converted data (`string | Uint8Array`, never ArrayBuffer).
 * - A method that feeds the RPC `meta` returns `{ rowsRead, rowsWritten }` for the statements named
 *   in its own doc, and for no others.
 */

// ---------------------------------------------------------------------------
// Row, cursor, and snapshot types
// ---------------------------------------------------------------------------

// hk/sk are canonical KeyBytes everywhere in the store: they bind to SQLite BLOB columns and compare
// by memcmp (the same total order as KeyCodec.compare). The ONLY producer of KeyBytes is KeyCodec.
export type StoredItem = {
	hk: KeyBytes;
	sk: KeyBytes;
	// Public reads decode json to JSON text; migration reads carry the raw JSONB blob (Uint8Array).
	data: string | Uint8Array;
	kind: DataKind;
	ttl_epoch_utc_seconds: number | null;
	v: number;
	last_read_ts: number;
	last_write_ts: number;
};

/**
 * The brand of ItemLinkId. It exists only for the TypeScript type checker.
 *
 * `declare const` tells TypeScript that a constant with this name exists, but it does not create one.
 * The compiled JavaScript does not contain it. The type `unique symbol` makes the property key
 * different from all other keys. This file does not export the constant, so no other module can use
 * the key. KeyBytes in key-codec.ts uses the same method.
 */
declare const ITEM_LINK_ID_BRAND: unique symbol;

/**
 * The value of the `items.item_id` column. It links an item to its rows in other tables.
 *
 * ItemLinkId is a branded type:
 * - At run time, the value is a normal JavaScript number.
 * - At compile time, the type also has the property `[ITEM_LINK_ID_BRAND]`. No real value has this
 *   property. Thus, TypeScript does not accept a plain `number` where the code needs an ItemLinkId.
 * - The brand does not change the value. Workers RPC sends the number, and the MigratedItem type at
 *   the receiver gives the brand back.
 *
 * Rules:
 * - Only PartitionStore creates an ItemLinkId. It reads the value from the `items` table, and the row
 *   type of the query gives the value the brand.
 * - Other code only moves the value. The migration is the only user: the parent reads a MigratedItem,
 *   RPC sends it to the child, and the child gives it to insertItemIfAbsent.
 * - Do NOT cast a number to ItemLinkId (`as ItemLinkId`) outside PartitionStore. Tests that stand in
 *   for a parent partition are the only exception.
 * - Do NOT use the value to identify an item outside PartitionStore. The brand cannot stop code that
 *   reads the value as a number, so code review must enforce this rule.
 */
export type ItemLinkId = number & { readonly [ITEM_LINK_ID_BRAND]: true };

/** A stored item as migration copies it: the item and its link id. Query results never carry the id. */
export type MigratedItem = StoredItem & { item_id: ItemLinkId };

/** The facts of one transaction that are the same for each of its locks: one `pending_tx_info` row. */
export type PendingTxInfo = {
	transaction_id: string;
	transaction_ts: number;
	/** The JSON `CoordinatorRef` of the coordinator that drives the transaction. */
	coordinator_json: string;
	created_at: number;
	guarded_at: number | null;
	/** The earliest time at which the stale-transaction job asks the coordinator about this transaction. */
	next_recovery_at: number;
};

/** One lock and the facts of its transaction, as prepare writes them and as migration copies them. */
export type PendingTxItem = PendingTxInfo & {
	hk: KeyBytes;
	sk: KeyBytes;
	operation: string;
	// data and its kind are absent together: null for delete/check ops, present for put.
	data: string | Uint8Array | null;
	kind: DataKind | null;
	ttl_epoch_utc_seconds: number | null;
};

/** One stale transaction, with what the recovery job needs to reach its coordinator and to set its next attempt. */
export type StalePendingTx = Pick<PendingTxInfo, "transaction_id" | "coordinator_json" | "created_at">;

export type PendingTransactionCursor = { hk: KeyBytes; sk: KeyBytes; transaction_id: string };

/** One key of a lock row, as the transaction paths hold it. */
export type PendingTxKey = { hashKey: KeyBytes; sortKey: KeyBytes };

export type ScanCursor = { hk: KeyBytes; sk: KeyBytes; inclusive?: boolean };

/** One hash key, and its sort keys from `start` (inclusive) to `end` (exclusive; null = to the last row of the key). */
export type KeyRange = { hk: KeyBytes; start: KeyBytes; end: KeyBytes | null };

/**
 * The position of a hash-key walk (`walkItemsByHashKey`, `walkPendingTxByHashKey`). `row` continues
 * strictly after that row. `after_key` continues after every row of `hk`: the walk skipped that hash key.
 */
export type HashKeyWalkCursor<C extends { hk: KeyBytes; sk: KeyBytes }> = { kind: "row"; row: C } | { kind: "after_key"; hk: KeyBytes };

/** One entry of a hash-key walk: a row of an owned hash key, or one hash key that the walk skipped. */
export type HashKeyWalkEntry<T> = { kind: "row"; row: T } | { kind: "skipped"; hk: KeyBytes };

/** The tables that a hash-key walk can read. Both have a unique index that starts with `(hk, sk)`. */
export type HashKeyWalkTable = "items" | "pending_transactions";

/** The bounds of one sort-key range scan of the items table under a single hash key. */
export type RangeScanBounds = {
	hk: KeyBytes;
	lower: KeyBytes;
	lowerInclusive: boolean;
	upper: KeyBytes | null;
	upperInclusive: boolean;
	cursor: ScanCursor | null;
	direction: "asc" | "desc";
};

/**
 * Receives one candidate of a queryItems leaf scan and returns `false` to stop the scan. `matched`
 * is false when the plan's filter rejected the candidate. `decodePayload` builds the payload of a
 * matched candidate on demand: the complete item when the request has no projection, the projected
 * wire row when it has one. It must be called at most once per candidate, only for a matched
 * candidate of a `"projection"` selection, and only before the consumer returns.
 */
export type QueryCandidateConsumer = (
	sk: KeyBytes,
	estRowBytes: number,
	matched: boolean,
	decodePayload: () => StoredItem | ProjectedWireRow,
) => boolean;

/** The promotion lifecycle as `status()` reports it, derived from a repartition's own state. */
export type PromotedKeyStatus = "queued" | "promoting" | "promoted";

export type SqlMetrics = { rowsRead: number; rowsWritten: number };

/**
 * The range split of one slice: its boundaries, or why it has none.
 * - `fewer_items`: the slice holds fewer items than children.
 * - `skewed_bytes`: the bytes are so skewed that the scan finds fewer boundaries than it needs.
 */
export type RangeSplitPlan = { boundaries: KeyBytes[] } | { floor: "fewer_items" | "skewed_bytes" };

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
	if (kind === null) {
		return null;
	}
	const code = DATA_KINDS.indexOf(kind);
	invariant(code !== -1, `fokos/partition-store: unknown data kind`);
	return code;
}

/**
 * The bytes the RPC serialization adds around one item, complete item or projected row, on top of
 * the item's own key, data, and cell bytes. It stands for the object or array itself, its property
 * names or cell slots, and one type tag and one length prefix per field or cell. It is an allowance,
 * not a measurement: a StoredItem has 8 named fields and a projected row has at most
 * `EXPRESSION_LIMITS.projectionEntries` cells, and 64 bytes is in the range both produce.
 *
 * The estimate only feeds the response-byte budget, which keeps one RPC response under the 32 MiB
 * cap of Workers RPC. `MAX_RESPONSE_BYTES_PER_PAGE` (16 MiB) leaves a 2x margin: even an error of 64
 * bytes per item on a page of 100,000 tiny items is about 6 MiB, inside that margin. Change this
 * value only when the per-item wire shape grows well past what it stands for (many more named
 * fields, or a much larger cell limit) or when that margin shrinks. Changing it moves page boundaries
 * only; a cursor does not depend on it.
 */
const ITEM_ENVELOPE_BYTES = 64;

export function estimateItemBytes(item: StoredItem): number {
	const dataSize = typeof item.data === "string" ? item.data.length * 2 : item.data.byteLength;
	return item.hk.byteLength + item.sk.byteLength + dataSize + 16 + ITEM_ENVELOPE_BYTES;
}

/**
 * Estimated RPC bytes of one projected row: the item envelope plus per-cell sizes, `length * 2`
 * for a string and for the JSON text of an array or an object, `byteLength` for a `Uint8Array`, and
 * 8 for every other cell. It does not use `est_row_bytes`: a projected row carries only its cells.
 */
export function estimateProjectedRowBytes(row: ProjectedWireRow): number {
	let bytes = ITEM_ENVELOPE_BYTES;
	for (const cell of row) {
		if (typeof cell === "string") {
			bytes += cell.length * 2;
		} else if (cell instanceof Uint8Array) {
			bytes += cell.byteLength;
		} else if (cell !== null && typeof cell === "object") {
			bytes += cell.json.length * 2;
		} else {
			bytes += 8;
		}
	}
	return bytes;
}

export function estimatePendingTxBytes(row: PendingTxItem): number {
	const dataSize = row.data == null ? 0 : typeof row.data === "string" ? row.data.length * 2 : row.data.byteLength;
	return row.hk.byteLength + row.sk.byteLength + 32 + 8 + 8 + 8 + dataSize + row.coordinator_json.length * 2 + 64;
}

/**
 * SQLite returns BLOB columns as ArrayBuffer; the public API speaks `string | Uint8Array`.
 * Every store row-reading method funnels data columns through this.
 */
export function fromSqlData(value: string | ArrayBuffer): string | Uint8Array;
export function fromSqlData(value: string | ArrayBuffer | null): string | Uint8Array | null;
export function fromSqlData(value: string | ArrayBuffer | null): string | Uint8Array | null {
	if (value === null) {
		return null;
	}
	return typeof value === "string" ? value : new Uint8Array(value);
}

/** The `WHERE` conditions and their bound values of one sort-key range scan, in the order the SQL text names them. */
function rangeScanConditions(opts: RangeScanBounds): { conds: string[]; params: unknown[] } {
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

	return { conds, params };
}

/**
 * The SQL of one queryItems leaf scan. Without a plan, the count selection reads only `sk` and
 * `est_row_bytes` from the covering `idx_items_scan` index; the `INDEXED BY` pin is needed for the
 * same reason as in `#storedEstRowBytes`. The projection selection reads the complete item with json
 * decoded to text. `limit` binds as given.
 *
 * With a plan the statement is the composed query statement, which gives every scan parameter an
 * explicit number from `?2`. The bound values therefore start with the pool parameter `?1`, which is
 * bound always. The materializer returns the text `[]` when the plan has no descriptor.
 */
export function queryScanStatement(opts: RangeScanBounds & { limit: number; select: QuerySelect; plan: CompiledQueryPlan | null }): {
	sql: string;
	params: unknown[];
} {
	const { conds, params } = rangeScanConditions(opts);
	if (opts.plan !== null) {
		return {
			sql: composeQueryStatement(opts.plan, { select: opts.select, direction: opts.direction, scanConditions: conds }),
			params: [...materializedPlanBindings(opts.plan, "pool"), ...params, opts.limit],
		};
	}
	const order = `ORDER BY sk ${opts.direction === "asc" ? "ASC" : "DESC"} LIMIT ?`;
	const sql =
		opts.select === "count"
			? `SELECT sk, est_row_bytes FROM items INDEXED BY idx_items_scan WHERE ${conds.join(" AND ")} ${order}`
			: `SELECT hk, sk, est_row_bytes, ${DATA_SELECT_DECODED}, data_kind, ttl_epoch_utc_seconds, v, last_read_ts, last_write_ts FROM items WHERE ${conds.join(" AND ")} ${order}`;
	return { sql, params: [...params, opts.limit] };
}

/**
 * The SQL of one migration page of `items`, in (hk, sk) order, strictly after `cursor`. A migration
 * read returns json rows as the raw JSONB blob (no `json()` decode), so the target inserts it unchanged.
 * `range` limits the read to one hash key and one sort-key interval. A null `range` reads the whole table.
 *
 * Without a range, the cursor MUST stay a row-value comparison `(hk, sk) > (?, ?)`. Do NOT rewrite it
 * as `hk > ? OR (hk = ? AND sk > ?)`: SQLite cannot seek on that form. It takes only `hk > ?` as the
 * index bound and checks each remaining row. A range partition holds one hash key, so each page would
 * start again at the first row of that key, and a full pass would be quadratic. Measured over 50k rows
 * under one hk: 337 page reads for a late page against 4 for the row-value form.
 *
 * With a range, the cursor replaces `start`. Do NOT add both bounds: SQLite can then seek on `start`
 * and check each row after it, and each page starts again at the first row of the range.
 *
 * Row values are correct only because hk and sk are NOT NULL (see the items migration). A NULL on
 * either side makes the comparison NULL, not true, and the read drops rows with no error. A key with
 * no sort key stores the empty blob, which is the byte minimum and compares like any other value.
 */
export function itemsPageStatement(cursor: ScanCursor | null, limit: number, range: KeyRange | null): { sql: string; params: unknown[] } {
	const conds: string[] = [];
	const params: unknown[] = [];
	if (range === null) {
		if (cursor) {
			conds.push("(hk, sk) > (?, ?)");
			params.push(cursor.hk, cursor.sk);
		}
	} else {
		conds.push("hk = ?", cursor ? "sk > ?" : "sk >= ?");
		params.push(range.hk, cursor ? cursor.sk : range.start);
		if (range.end !== null) {
			conds.push("sk < ?");
			params.push(range.end);
		}
	}
	const where = conds.length > 0 ? `WHERE ${conds.join(" AND ")} ` : "";
	return {
		sql: `SELECT item_id, hk, sk, data, data_kind, ttl_epoch_utc_seconds, v, last_read_ts, last_write_ts FROM items ${where}ORDER BY hk, sk LIMIT ?`,
		params: [...params, limit],
	};
}

/**
 * The SQL that finds the first row of the next hash key after `after` (null = the first key of the
 * table). It is one seek on a key index, and it reads one index entry, also when `after` has many rows.
 */
export function nextHashKeyStatement(table: HashKeyWalkTable, after: KeyBytes | null): { sql: string; params: unknown[] } {
	return after === null
		? { sql: `SELECT hk, sk FROM ${table} ORDER BY hk, sk LIMIT 1`, params: [] }
		: { sql: `SELECT hk, sk FROM ${table} WHERE hk > ? ORDER BY hk, sk LIMIT 1`, params: [after] };
}

/**
 * The SQL of one migration page of `pending_transactions`, in (hk, sk, transaction_id) order,
 * strictly after `cursor`. `range` limits the read as in `itemsPageStatement`.
 *
 * The cursor MUST stay a row-value comparison, for the reason that `itemsPageStatement` gives. With a
 * range, the cursor replaces `start`, and it stays a row value on `(sk, transaction_id)` after
 * `hk = ?`: two locks on one key can be on the two sides of a page boundary. All three key columns
 * are NOT NULL, which makes row values correct.
 *
 * Each row carries the `pending_tx_info` row of its transaction, so the target can write both from one
 * page. A transaction whose locks span pages sends its row again, and the insert merges it.
 */
export function pendingTxPageStatement(
	cursor: PendingTransactionCursor | null,
	limit: number,
	range: KeyRange | null,
): { sql: string; params: unknown[] } {
	const conds: string[] = [];
	const params: unknown[] = [];
	if (range === null) {
		if (cursor) {
			conds.push("(p.hk, p.sk, p.transaction_id) > (?, ?, ?)");
			params.push(cursor.hk, cursor.sk, cursor.transaction_id);
		}
	} else {
		conds.push("p.hk = ?");
		params.push(range.hk);
		if (cursor) {
			conds.push("(p.sk, p.transaction_id) > (?, ?)");
			params.push(cursor.sk, cursor.transaction_id);
		} else {
			conds.push("p.sk >= ?");
			params.push(range.start);
		}
		if (range.end !== null) {
			conds.push("p.sk < ?");
			params.push(range.end);
		}
	}
	const where = conds.length > 0 ? `WHERE ${conds.join(" AND ")} ` : "";
	return {
		sql: `SELECT p.hk, p.sk, p.transaction_id, p.operation, p.data, p.data_kind, p.ttl_epoch_utc_seconds,
		             t.transaction_ts, t.coordinator_json, t.created_at, t.guarded_at, t.next_recovery_at
		        FROM pending_transactions p JOIN pending_tx_info t ON t.transaction_id = p.transaction_id
		       ${where}ORDER BY p.hk, p.sk, p.transaction_id LIMIT ?`,
		params: [...params, limit],
	};
}

// ---------------------------------------------------------------------------
// Schema migrations
// ---------------------------------------------------------------------------

const sqlMigrations: SQLSchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: "Create items table",
		// Column `data`:
		// - The type is ANY, so SQLite keeps the storage class of each value: TEXT for text, BLOB for bytes,
		//   and BLOB for json (the SQLite JSONB binary format).
		// - JSONB and bytes are both BLOB values. `data_kind` tells the three kinds apart.
		// - `data` is the last column. SQLite reads a record from the start until it has all the columns
		//   that the query needs. Thus, a query that reads only metadata columns does not read the
		//   overflow pages of a large `data` value.
		//
		// Table type:
		// - This is a rowid table. Do NOT change it to WITHOUT ROWID.
		// - A WITHOUT ROWID table keeps its rows in an index B-tree. On a 4 KiB page, the inline payload
		//   limit of an index B-tree is approximately 1002 bytes. Each row above that limit gets its own
		//   overflow page, and no other row can use the free space on that page.
		// - Measured on Durable Object storage: 1000-byte data used 4683 physical bytes per row (4.7x), and
		//   2000-byte data used 2.3x.
		//
		// Item key:
		// - `UNIQUE (hk, sk)` makes sure that each item key occurs only one time. SQLite creates the index
		//   sqlite_autoindex_items_1 for this constraint.
		// - hk and sk have an explicit NOT NULL, because a UNIQUE constraint does not make a column NOT NULL.
		//
		// Column `item_id`:
		// - `item_id` links an item to its rows in other tables. Only PartitionStore uses it (see ItemLinkId).
		// - A table that links to an item must name its link column `item_id` too.
		// - `item_id` is the INTEGER PRIMARY KEY, so it is the rowid of the row. SQLite does not store the
		//   value a second time. The cost is one byte in the record header.
		// - SQLite gives a new row the value MAX(item_id) + 1. Thus, new rows go to the end of the table
		//   B-tree.
		// - Do NOT use a random or hash value for `item_id`. Such values put rows at random positions in the
		//   B-tree. Inserts then become slower, and the pages contain more empty space.
		// - VACUUM does not change an explicit INTEGER PRIMARY KEY. SQLite does not give this guarantee for
		//   an implicit rowid.
		// - Migration copies `item_id` without change. A child partition receives items only from its
		//   parent, and the ids in the parent are unique. Thus, the ids in the child are also unique. After
		//   the migration, a new item in the child gets an id that is higher than all copied ids.
		// - The value is unique only in this partition and in the partitions that copy from it. Do NOT send
		//   `item_id` through the API, in cursors, or in transaction rows.
		// - When SQLite deletes the row with the highest id, it can give that id to the next new row. Thus,
		//   each operation that deletes an item must also delete the rows linked to its id, in the same
		//   storage transaction.
		//
		// Column `est_row_bytes`:
		// - The value is the encoded size from octet_length: UTF-8 bytes for TEXT, and blob bytes for BLOB
		//   and JSONB.
		// - It is a normal column, NOT a generated column. SQLite does not use an index that contains a
		//   generated column as a covering index. The est_row_bytes scans would then read each full row.
		// - Both writers calculate the value with estRowBytesExpr. Thus, SQLite measures the stored value,
		//   and JavaScript does not estimate it.
		// - octet_length gives the variable part of the row (data and keys). The constant K gives the fixed
		//   part: the five integer columns, the data_kind value, the record header, the rowid, and the two
		//   index entries of each row (sqlite_autoindex_items_1 and idx_items_scan).
		// - K is an approximate value for size accounting (promotion and split). It is not an exact value.
		//
		// Index `idx_items_scan`:
		// - The index contains (hk, sk, est_row_bytes). Thus, the est_row_bytes scans
		//   (computeRangeSplitBoundaries) read only the index, and never read the
		//   item rows.
		sql: `
            CREATE TABLE IF NOT EXISTS items (
                item_id               INTEGER PRIMARY KEY,

                hk                    BLOB    NOT NULL,
                sk                    BLOB    NOT NULL DEFAULT x'',
                data_kind             INTEGER NOT NULL DEFAULT 0,
                v                     INTEGER NOT NULL,
                last_read_ts          INTEGER NOT NULL DEFAULT 0,
                last_write_ts         INTEGER NOT NULL DEFAULT 0,
				ttl_epoch_utc_seconds INTEGER,
                est_row_bytes         INTEGER NOT NULL,
				data                  ANY     NOT NULL,

                UNIQUE (hk, sk)
            ) STRICT;

            CREATE INDEX IF NOT EXISTS idx_items_scan ON items (hk, sk, est_row_bytes);
            CREATE INDEX IF NOT EXISTS idx_items_ttl ON items (ttl_epoch_utc_seconds, hk, sk)
                WHERE ttl_epoch_utc_seconds IS NOT NULL;`,
	},
	{
		idMonotonicInc: 2,
		description: "Create transaction support tables",
		// A transaction has one `pending_tx_info` row and one `pending_transactions` row for each key that it
		// locks here. The facts that are the same for each key of the transaction are stored one time.
		// A `pending_tx_info` row stays while a lock row of its transaction stays, copies of a moved key
		// included, and the statement that deletes the last lock row also deletes it.
		//
		// `pending_tx_info_due` holds only the unguarded transactions. The stale-transaction job reads its
		// deadline and its batch from this index, one entry for each transaction. Each attempt moves
		// `next_recovery_at` forward, so a lock that stays does not keep the deadline in the past.
		//
		// pending_transactions is a rowid table on purpose. Do NOT add WITHOUT ROWID back: it has the
		// same defect here as in `items`.
		// WITHOUT ROWID stores rows in an index B-tree with a ~1002-byte inline payload limit on a 4 KiB
		// page, so every row whose `data` exceeds that takes a private overflow page it cannot share.
		// Measured on Durable Object storage, 2000 rows of 1500-byte data against 3.00 MB logical:
		// 9.41 MB WITHOUT ROWID (3.1x) against 4.20 MB as a rowid table (1.4x). `PRIMARY KEY
		// (hk, sk, transaction_id)` still enforces uniqueness through sqlite_autoindex_pending_transactions_1,
		// and hk/sk/transaction_id keep their explicit NOT NULL because a rowid table does NOT imply it
		// from the primary key.
		//
		// pending_transactions_transaction_id exists because `transaction_id` is the THIRD primary key
		// column and so cannot be seeked on its own. Every whole-transaction operation filters by it:
		// listPendingTxItems, and the delete of the `pending_tx_info` row. listPendingTxItems runs on every
		// commit, so without this index the cost of committing ONE transaction is O(all pending rows in
		// the partition). A per-key statement, deletePendingTxKeys, seeks the primary key instead.
		//
		// Its key carries (hk, sk) EXPLICITLY. A rowid table appends only the rowid to an index entry, so
		// a key of `transaction_id` alone would make the existence test of the last lock row and the
		// key list of a commit read the table.
		sql: `
            CREATE TABLE IF NOT EXISTS pending_tx_info (
                transaction_id   TEXT    NOT NULL PRIMARY KEY,
                transaction_ts   INTEGER NOT NULL,
                created_at       INTEGER NOT NULL,
                coordinator_json TEXT    NOT NULL,
                guarded_at       INTEGER,
                next_recovery_at INTEGER NOT NULL
            ) STRICT;

            CREATE INDEX IF NOT EXISTS pending_tx_info_due ON pending_tx_info (next_recovery_at) WHERE guarded_at IS NULL;

            CREATE TABLE IF NOT EXISTS pending_transactions (
                hk                    BLOB    NOT NULL,
                sk                    BLOB    NOT NULL DEFAULT x'',
                transaction_id        TEXT    NOT NULL,
                operation             TEXT    NOT NULL,
                data_kind             INTEGER, -- NULL for delete/check (no data); set for put
                ttl_epoch_utc_seconds INTEGER,
                data                  ANY,

                PRIMARY KEY (hk, sk, transaction_id)
            ) STRICT;

            CREATE INDEX IF NOT EXISTS pending_transactions_transaction_id ON pending_transactions (transaction_id, hk, sk);

            -- deletion_metadata: one row (id = 1) with the delete values of the whole partition. The values stay after the
            -- delete of an item row, so they describe keys that have no row.
            --
            -- max_delete_tx_order_ts : the highest transaction order timestamp of a delete or of a TTL expiry, and at least
            --                          the last_read_ts of each removed row. The prepare of an absent item rejects a
            --                          transaction timestamp that is not greater than this value. A new item row starts with
            --                          last_read_ts and last_write_ts at least this value. Thus the timestamp watermark of a
            --                          key never goes down, and a late prepare of a committed transaction gets a conflict.
            -- max_deleted_v          : the highest v of an item row that a delete or a TTL expiry removed. A new item row
            --                          starts at max_deleted_v + 1, so the v of a key never repeats, also after a delete and a
            --                          recreate. A read transaction compares this value for an item that is absent in both
            --                          phases. A read compares v for an item that is found in both phases.
            CREATE TABLE IF NOT EXISTS deletion_metadata (
                id                     INTEGER PRIMARY KEY CHECK (id = 1),
                max_delete_tx_order_ts INTEGER NOT NULL DEFAULT 0,
                max_deleted_v          INTEGER NOT NULL DEFAULT 0
            ) STRICT;
            INSERT OR IGNORE INTO deletion_metadata (id, max_delete_tx_order_ts, max_deleted_v) VALUES (1, 0, 0);`,
	},
	{
		idMonotonicInc: 3,
		description: "Add per-hash key size estimate and key-level size summary for efficient promotion detection",
		// FIXME: Add also number of items per hash key.
		sql: `
            CREATE TABLE IF NOT EXISTS key_size_estimates (
                hk        BLOB    NOT NULL PRIMARY KEY,
                est_bytes INTEGER NOT NULL DEFAULT 0
            ) WITHOUT ROWID, STRICT;

            CREATE INDEX IF NOT EXISTS key_size_estimates_by_bytes ON key_size_estimates (est_bytes);`,
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
			keyNameTrackingLastMigrationID: "__fokosdb/partition/sql_schema_version",
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
		row?: {
			data: string | Uint8Array;
			kind: DataKind;
			ttl_epoch_utc_seconds: number | null;
			v: number;
			last_read_ts: number;
			last_write_ts: number;
		};
		rowsRead: number;
		rowsWritten: number;
	} {
		const res = this.#storage.sql.exec<{
			data: string | ArrayBuffer;
			data_kind: number;
			ttl_epoch_utc_seconds: number | null;
			v: number;
			last_read_ts: number;
			last_write_ts: number;
		}>(
			`SELECT ${DATA_SELECT_DECODED}, data_kind, ttl_epoch_utc_seconds, v, last_read_ts, last_write_ts FROM items WHERE hk = ? AND sk = ? LIMIT 1`,
			hk,
			sk,
		);
		const row = tryOne(res);
		if (!row) {
			return { row: undefined, rowsRead: res.rowsRead, rowsWritten: res.rowsWritten };
		}
		const { data_kind, ...rest } = row; // data_kind → the readable `kind`; don't leak the raw code
		return {
			row: { ...rest, data: fromSqlData(row.data), kind: kindFromCode(data_kind) },
			rowsRead: res.rowsRead,
			rowsWritten: res.rowsWritten,
		};
	}

	/**
	 * Secondary read run only after a failed condition on an existing row when the caller
	 * asked for an image.
	 */
	getItemImage(
		hk: KeyBytes,
		sk: KeyBytes,
	): {
		row?: { data: string | Uint8Array; kind: DataKind; version: number; ttlAt?: number; imageBytes: number };
		rowsRead: number;
		rowsWritten: number;
	} {
		const res = this.#storage.sql.exec<{
			data: string | ArrayBuffer;
			data_kind: number;
			v: number;
			ttl_epoch_utc_seconds: number | null;
			image_bytes: number;
		}>(
			`SELECT ${DATA_SELECT_DECODED}, data_kind, v, ttl_epoch_utc_seconds, octet_length(CASE WHEN data_kind = ${JSON_KIND_CODE} THEN json(data) ELSE data END) AS image_bytes FROM items WHERE hk = ? AND sk = ? LIMIT 1`,
			hk,
			sk,
		);
		const row = tryOne(res);
		if (!row) {
			return { row: undefined, rowsRead: res.rowsRead, rowsWritten: res.rowsWritten };
		}
		return {
			row: {
				data: fromSqlData(row.data),
				kind: kindFromCode(row.data_kind),
				version: row.v,
				...(row.ttl_epoch_utc_seconds != null ? { ttlAt: row.ttl_epoch_utc_seconds } : {}),
				imageBytes: row.image_bytes,
			},
			rowsRead: res.rowsRead,
			rowsWritten: res.rowsWritten,
		};
	}

	/** Lightweight existence and timestamp read for an unconditional transaction prepare. */
	getItemStamp(
		hk: KeyBytes,
		sk: KeyBytes,
	): {
		row?: { last_read_ts: number; last_write_ts: number };
		rowsRead: number;
		rowsWritten: number;
	} {
		const res = this.#storage.sql.exec<{ last_read_ts: number; last_write_ts: number }>(
			`SELECT last_read_ts, last_write_ts FROM items WHERE hk = ? AND sk = ? LIMIT 1`,
			hk,
			sk,
		);
		const row = tryOne(res);
		return { row, rowsRead: res.rowsRead, rowsWritten: res.rowsWritten };
	}

	evaluateCondition(plan: CompiledConditionPlan, hk: KeyBytes, sk: KeyBytes): ConditionEvaluationResult {
		return withExpressionErrors(() => evaluateConditionPlan(this.#storage, plan, hk, sk));
	}

	getItemProjected(plan: CompiledProjectionPlan, hk: KeyBytes, sk: KeyBytes): ProjectedReadResult {
		return withExpressionErrors(() => readProjectedItem(this.#storage, plan, hk, sk));
	}

	probeUpdate(plan: CompiledUpdatePlan, hk: KeyBytes, sk: KeyBytes): UpdateProbeResult {
		return withExpressionErrors(() => probeUpdatePlan(this.#storage, plan, hk, sk));
	}

	/**
	 * The row's currently stored `est_row_bytes`, or 0 when the row is absent. `upsertItem` and
	 * `updateItemSingleShot` need it to compute the `key_size_estimates` delta. `deleteItem` reads the
	 * size together with the `v` and the `last_read_ts` of the row, so it does not use this read.
	 *
	 * `INDEXED BY idx_items_scan` is a deliberate plan pin, not decoration. `idx_items_scan
	 * (hk, sk, est_row_bytes)` covers this query exactly, but SQLite prefers
	 * `sqlite_autoindex_items_1` — the primary-key index, which stops at `(hk, sk)` — and then
	 * fetches the table row for `est_row_bytes`. Measured plans:
	 *
	 *   without the hint: SEARCH items USING INDEX sqlite_autoindex_items_1 (hk=? AND sk=?)
	 *   with the hint:    SEARCH items USING COVERING INDEX idx_items_scan (hk=? AND sk=?)
	 *
	 * The hint removes that table-row fetch from every put. It saves page reads, not
	 * billed rows — SQLite reports one row read either way. `INDEXED BY` makes the dependency on
	 * `idx_items_scan` hard: dropping or renaming that index fails this query loudly rather than
	 * silently regressing both write paths. A query-plan test asserts the pin still works AND that it
	 * is still needed, so it can be removed if SQLite ever picks the covering index unaided.
	 *
	 * A fold into the DELETE with `DELETE ... RETURNING` measured the returned row as an extra read, so
	 * it costs the same on a hit and one more on a miss.
	 *
	 * Do NOT replace this read with `AFTER INSERT/UPDATE/DELETE` triggers on items that maintain
	 * key_size_estimates. A trigger fires for EVERY writer of items, including the two that do their
	 * size accounting in bulk: insertItemIfAbsent (migration ingest, one addKeySizeEstimate per key per
	 * page) and deleteItemsBatchForHashKey (cleanup, 1000 rows per call, followed by one
	 * deleteKeySizeEstimate). It would add one row WRITE per row on those paths to save one row READ here.
	 */
	#storedEstRowBytes(hk: KeyBytes, sk: KeyBytes): number {
		const row = tryOne(
			this.#storage.sql.exec<{ est_row_bytes: number }>(
				`SELECT est_row_bytes FROM items INDEXED BY idx_items_scan WHERE hk = ? AND sk = ? LIMIT 1`,
				hk,
				sk,
			),
		);
		return row?.est_row_bytes ?? 0;
	}

	/**
	 * The exact `est_row_bytes` that `upsertItem` would store for this value, measured by SQLite over
	 * the same expression the write uses. It reads no row: the statement has no FROM clause, so the
	 * cost is the encode that the write would pay anyway.
	 *
	 * Both transactional write paths call it in their CHECK pass. A write that cannot fit is then
	 * rejected before anything is written, which is what keeps the size guard below unreachable on a
	 * two-phase commit — the pass that is not allowed to fail.
	 */
	measureItemBytes(opts: { hk: KeyBytes; sk: KeyBytes; data: string | Uint8Array; kind: DataKind }): number {
		const dataExpr = itemDataExpr(opts.kind, opts.data, "?3");
		return this.#storage.sql
			.exec<{ est_row_bytes: number }>(`SELECT ${estRowBytesExpr(dataExpr, "?1", "?2")} AS est_row_bytes`, opts.hk, opts.sk, opts.data)
			.one().est_row_bytes;
	}

	/**
	 * The items upsert with est_row_bytes / key_size_estimates bookkeeping — the single
	 * definition used by BOTH the non-transactional putItem and the transactional commit apply.
	 * Returns the new item version and the key's updated size estimate (feeds promotion checks).
	 * Throws when the row would exceed MAX_ITEM_BYTES, which writes nothing — see throwItemTooLarge.
	 * Metrics cover ONLY the items upsert statement (matching the DO's previous meta math —
	 * the old-estimate read and the key_size_estimates upsert were never counted).
	 */
	upsertItem(opts: {
		hk: KeyBytes;
		sk: KeyBytes;
		/** json ⇒ JSON text from a client put, or a raw JSONB blob from a commit apply. */
		data: string | Uint8Array;
		kind: DataKind;
		ttlAt: number | null;
		/** The transaction order timestamp a content mutation stamps on the item: both watermarks advance to it. */
		txOrderTs: number;
	}): {
		version: number;
		keyEstBytes: number;
		rowsRead: number;
		rowsWritten: number;
	} {
		const oldEst = this.#storedEstRowBytes(opts.hk, opts.sk);

		// data binds last (?6) because est_row_bytes must measure this same expression.
		const dataExpr = itemDataExpr(opts.kind, opts.data, "?6");

		// INVARIANT: last_read_ts and last_write_ts are monotonic per item — neither must ever move
		// backwards.
		//
		// `prepare` accepts a transaction only when its timestamp is above the item's read watermark
		// (transaction-participant.ts), so a lower value here would let an already-superseded
		// transaction commit over newer data. The two writers disagree on whose clock they read: a
		// non-transactional put stamps this partition's clock, while a committed transaction stamps
		// its coordinator's, which prepare accepts up to `maxClockSkewMs` ahead. MAX is what
		// reconciles them.
		//
		// MAX also cannot drift ahead of the wall clock: it only ever keeps the larger of two values
		// that already exist. Do NOT turn it into an increment (`MAX(last_write_ts + 1, ?)`) —
		// that would run the timestamps forward under sustained writes.
		//
		// bumpItemReadTs applies the same rule to the read watermark alone, for the transactional
		// "check" operation.
		//
		// A new row starts at max_deleted_v + 1 with its stamps at least max_delete_tx_order_ts, so its v
		// and its timestamp watermark never go below those of an earlier, deleted row of the key. The
		// conflict branch uses ?5 and not `excluded`, because `excluded` holds the stamps of the insert
		// branch, which include the partition delete timestamp.
		const writeRes = this.#storage.sql.exec<{ v: number; est_row_bytes: number }>(
			`INSERT INTO items (hk, sk, data_kind, ttl_epoch_utc_seconds, v, last_read_ts, last_write_ts, est_row_bytes, data)
			 SELECT ?1, ?2, ?3, ?4, d.max_deleted_v + 1, MAX(?5, d.max_delete_tx_order_ts), MAX(?5, d.max_delete_tx_order_ts),
			        ${estRowBytesExpr(dataExpr, "?1", "?2")}, ${dataExpr}
			   FROM deletion_metadata AS d
			  WHERE d.id = 1 AND ${estRowBytesExpr(dataExpr, "?1", "?2")} <= ?7
			 ON CONFLICT(hk, sk) DO UPDATE SET
			   data = excluded.data,
			   data_kind = excluded.data_kind,
			   ttl_epoch_utc_seconds = excluded.ttl_epoch_utc_seconds,
			   est_row_bytes = excluded.est_row_bytes,
			   v = v + 1,
			   last_read_ts = MAX(last_read_ts, ?5),
			   last_write_ts = MAX(last_write_ts, ?5)
			 RETURNING v, est_row_bytes`,
			opts.hk,
			opts.sk,
			DATA_KINDS.indexOf(opts.kind),
			opts.ttlAt,
			opts.txOrderTs,
			opts.data,
			MAX_ITEM_BYTES,
		);
		const rows = writeRes.toArray();
		if (rows.length === 0) {
			throwItemTooLarge(opts.hk, opts.sk);
		}
		const version = rows[0].v;
		invariant(
			typeof version === "number" && Number.isInteger(version) && version >= 1,
			`fokos/partition-store.upsertItem: unexpected version value: ${version}`,
		);
		// Exact stored size, measured by SQLite in the statement above (drives the key_size_estimates delta).
		const newEst = rows[0].est_row_bytes;

		const kseRow = tryOne(
			this.#storage.sql.exec<{ est_bytes: number }>(
				`INSERT INTO key_size_estimates (hk, est_bytes) VALUES (?, ?)
				 ON CONFLICT(hk) DO UPDATE SET est_bytes = MAX(0, est_bytes + excluded.est_bytes - ?)
				 RETURNING est_bytes`,
				opts.hk,
				newEst,
				oldEst,
			),
		);

		return { version, keyEstBytes: kseRow?.est_bytes ?? newEst, rowsRead: writeRes.rowsRead, rowsWritten: writeRes.rowsWritten };
	}

	/**
	 * Deletes an item, keeping the deletion metadata and key-size estimate consistent.
	 * `bumpTxOrderTsAlways` gives the transactional-delete behavior: it advances the transaction order watermark and
	 * the estimate even when the row was already absent. The non-transactional path updates them only
	 * when the statement deleted a row. `max_deleted_v` advances only when a row was removed, because
	 * an absent row has no `v`; `max_delete_tx_order_ts` also advances for an absent row when
	 * `bumpTxOrderTsAlways`. The metrics cover the DELETE statement ONLY.
	 */
	deleteItem(opts: { hk: KeyBytes; sk: KeyBytes; txOrderTs: number; bumpTxOrderTsAlways?: boolean }): {
		deleted: boolean;
		rowsRead: number;
		rowsWritten: number;
	} {
		// One read gives the size and the values that the deletion metadata records. The DELETE runs in
		// the same synchronous call, so it removes this same row. Do not move the read into a RETURNING
		// clause of the DELETE: RETURNING adds one row read to each delete, also when no row exists.
		const before = tryOne(
			this.#storage.sql.exec<{ est_row_bytes: number; v: number; last_read_ts: number }>(
				`SELECT est_row_bytes, v, last_read_ts FROM items WHERE hk = ? AND sk = ?`,
				opts.hk,
				opts.sk,
			),
		);
		const delEst = before?.est_row_bytes ?? 0;

		const writeRes = this.#storage.sql.exec(`DELETE FROM items WHERE hk = ? AND sk = ?`, opts.hk, opts.sk);
		const deleted = writeRes.rowsWritten > 0;
		if (deleted) {
			invariant(before !== undefined, "fokos/partition-store.deleteItem: the DELETE removed a row that the read before it did not find");
			this.#raiseDeletionMetadata(Math.max(opts.txOrderTs, before.last_read_ts), before.v);
		} else if (opts.bumpTxOrderTsAlways) {
			this.bumpMaxDeleteTxOrderTs(opts.txOrderTs);
		}
		if (deleted || opts.bumpTxOrderTsAlways) {
			this.#subtractKeySizeEstimate(opts.hk, delEst);
		}
		return { deleted, rowsRead: writeRes.rowsRead, rowsWritten: writeRes.rowsWritten };
	}

	/**
	 * Deletes one bounded chunk of expired, unlocked items and updates all deletion bookkeeping in the
	 * same storage transaction. The victim scan uses only metadata columns and never reads item data.
	 *
	 * The sweep does not test the owner of each row. An expired row of a key that a promotion moves is
	 * an owned row or a copy. The promotion cleanup deletes a copy in all cases, so an early delete
	 * changes no result. A lock row, owned or a copy, keeps its item.
	 */
	deleteExpiredItems(nowSeconds: number, limit: number): { deletedRows: number; deletedBytes: number } {
		return this.transactionSync(() => {
			const rows = this.#storage.sql
				.exec<{ hk: ArrayBuffer; est_row_bytes: number; ttl_epoch_utc_seconds: number; v: number; last_read_ts: number }>(
					`DELETE FROM items
					 WHERE item_id IN (
					     SELECT i.item_id FROM items i INDEXED BY idx_items_ttl
					      WHERE i.ttl_epoch_utc_seconds IS NOT NULL
					        AND i.ttl_epoch_utc_seconds <= ?1
					        AND NOT EXISTS (SELECT 1 FROM pending_transactions p WHERE p.hk = i.hk AND p.sk = i.sk)
					      ORDER BY i.ttl_epoch_utc_seconds, i.hk, i.sk
					      LIMIT ?2
					 )
					 RETURNING hk, est_row_bytes, ttl_epoch_utc_seconds, v, last_read_ts`,
					nowSeconds,
					limit,
				)
				.toArray();

			const bytesByHashKey = new Map<string, { hk: KeyBytes; bytes: number }>();
			let deletedBytes = 0;
			let maxDeleteTxOrderTs = 0;
			let maxDeletedV = 0;
			for (const row of rows) {
				const hk = fromSqlKey(row.hk);
				const key = hk.toBase64({ alphabet: "base64url" });
				const current = bytesByHashKey.get(key);
				if (current) {
					current.bytes += row.est_row_bytes;
				} else {
					bytesByHashKey.set(key, { hk, bytes: row.est_row_bytes });
				}
				deletedBytes += row.est_row_bytes;
				maxDeleteTxOrderTs = Math.max(maxDeleteTxOrderTs, row.ttl_epoch_utc_seconds * 1000 * TX_ORDER_TS_UNITS_PER_MS, row.last_read_ts);
				maxDeletedV = Math.max(maxDeletedV, row.v);
			}

			for (const { hk, bytes } of bytesByHashKey.values()) {
				this.#subtractKeySizeEstimate(hk, bytes);
			}
			// The logical delete of an expired row happened at its expiry. The sweep records the removed
			// rows the same as a user delete, so a later create of the key starts above their v and their
			// stamps.
			if (rows.length > 0) {
				this.#raiseDeletionMetadata(maxDeleteTxOrderTs, maxDeletedV);
			}

			return { deletedRows: rows.length, deletedBytes };
		});
	}

	/** The transactional "check" operation: advances only the item's read watermark. */
	bumpItemReadTs(hk: KeyBytes, sk: KeyBytes, ts: number): void {
		this.#storage.sql.exec(`UPDATE items SET last_read_ts = MAX(last_read_ts, ?) WHERE hk = ? AND sk = ?`, ts, hk, sk);
	}

	/**
	 * Migration ingestion: keeps an existing row rather than replacing it. A migrating partition refuses
	 * every write with 503 while it is migration_migrating, so no user write can have arrived yet.
	 * Keeping the row is the safer form on a retry: when a crash followed a written batch, the retry
	 * keeps those rows instead of writing them again.
	 *
	 * The conflict target MUST stay `(hk, sk)`. Do NOT use `INSERT OR IGNORE`: it also ignores a conflict
	 * on `item_id`, which would drop a copied item without an error. With the explicit target, a retried
	 * row is skipped, and an `item_id` that another item already holds fails the statement.
	 *
	 * It reports whether it inserted the row and the exact `est_row_bytes` SQLite stored, so the caller
	 * adds the same number to `key_size_estimates` in the same transaction. A skipped row reports
	 * `inserted: false` and zero bytes, because a retry must not count a row twice. Maintaining the
	 * estimates page by page is what removes the unbounded whole-table rebuild at the end of an import.
	 */
	insertItemIfAbsent(item: MigratedItem): { inserted: boolean; estRowBytes: number } {
		// Migration copies the stored representation verbatim: for json rows `item.data` is the raw
		// JSONB blob, bound directly (no jsonb() re-encode). data binds last (?9) so est_row_bytes can
		// measure the same parameter.
		const row = tryOne(
			this.#storage.sql.exec<{ est_row_bytes: number }>(
				`INSERT INTO items (item_id, hk, sk, data_kind, ttl_epoch_utc_seconds, v, last_read_ts, last_write_ts, est_row_bytes, data)
			 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ${estRowBytesExpr("?9", "?2", "?3")}, ?9)
			 ON CONFLICT (hk, sk) DO NOTHING
			 RETURNING est_row_bytes`,
				item.item_id,
				item.hk,
				item.sk,
				DATA_KINDS.indexOf(item.kind),
				item.ttl_epoch_utc_seconds ?? null,
				item.v,
				item.last_read_ts,
				item.last_write_ts,
				item.data,
			),
		);
		return row ? { inserted: true, estRowBytes: row.est_row_bytes } : { inserted: false, estRowBytes: 0 };
	}

	hasItemsForHashKey(hk: KeyBytes): boolean {
		return this.#storage.sql.exec(`SELECT 1 FROM items WHERE hk = ? LIMIT 1`, hk).toArray().length > 0;
	}

	/**
	 * The first hash key of `table`, and whether another hash key follows it. Two seeks of the index
	 * that leads with `hk`: `idx_items_scan`, or the primary key of `pending_transactions`. The second
	 * seek starts past every row of the first key, so the cost does not depend on the row count.
	 */
	probeHashKeys(table: "items" | "pending_transactions"): { first: KeyBytes; more: boolean } | null {
		const sql = this.#storage.sql;
		const first = tryOne(sql.exec<{ hk: ArrayBuffer }>(`SELECT hk FROM ${table} ORDER BY hk LIMIT 1`));
		if (!first) {
			return null;
		}
		return { first: fromSqlKey(first.hk), more: exists(sql.exec(`SELECT 1 FROM ${table} WHERE hk > ? LIMIT 1`, first.hk)) };
	}

	/** `planRangeSplit` without the reason: the boundaries, or null. */
	computeRangeSplitBoundaries(hashKey: KeyBytes, start: KeyBytes | null, end: KeyBytes | null, N: number): KeyBytes[] | null {
		const plan = this.planRangeSplit(hashKey, start, end, N);
		return "boundaries" in plan ? plan.boundaries : null;
	}

	/**
	 * Computes N-1 strictly-increasing split boundaries (byte-quantiles) within [start, end) in one
	 * transactionSync snapshot. When the slice cannot yield N non-empty children, it returns the
	 * reason instead: fewer than N items, or bytes so skewed that the scan finds fewer boundaries.
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
	planRangeSplit(hashKey: KeyBytes, start: KeyBytes | null, end: KeyBytes | null, N: number): RangeSplitPlan {
		// Rather than a COUNT(*) pass plus N-1 OFFSET re-walks (~2.5·cnt row touches, all count-balanced),
		// this reads the O(1) est_bytes total and does a single early-stopping streaming scan that emits a
		// boundary each time the running est_row_bytes total crosses the next byte threshold, breaking after
		// the (N-1)th boundary (~0.75·cnt at N=4). Byte-balance — not count-balance — is the right metric
		// because the split is triggered by size; it also isolates a heavy row into its own child.
		return this.#storage.transactionSync((): RangeSplitPlan => {
			const lower = start ?? KeyCodec.encodeOptional(undefined); // −∞ ⇒ sk >= x'' (the empty sentinel)

			// Total bytes in O(1) from the maintained per-hk estimate. No bytes ⇒ no items to split.
			const B =
				tryOne(this.#storage.sql.exec<{ est_bytes: number }>(`SELECT est_bytes FROM key_size_estimates WHERE hk = ?`, hashKey))
					?.est_bytes ?? 0;
			if (B <= 0) {
				return { floor: "fewer_items" };
			}

			// Cheap "≥ N items" guard, O(N) not O(cnt): each child needs ≥ 1 item, so probe with a bounded
			// count rather than a full pass. Fewer than N items ⇒ cannot split into N non-empty children.
			const guardRow = one(
				end === null
					? this.#storage.sql.exec<{ n: number }>(
							`SELECT COUNT(*) AS n FROM (SELECT 1 FROM items WHERE hk = ? AND sk >= ? LIMIT ?)`,
							hashKey,
							lower,
							N,
						)
					: this.#storage.sql.exec<{ n: number }>(
							`SELECT COUNT(*) AS n FROM (SELECT 1 FROM items WHERE hk = ? AND sk >= ? AND sk < ? LIMIT ?)`,
							hashKey,
							lower,
							end,
							N,
						),
			);
			if (guardRow.n < N) {
				return { floor: "fewer_items" };
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
					// Relative bump (acc + step, not threshold += step): when one oversized row pushes acc past
					// several thresholds at once, the scan still emits one boundary and re-anchors here, so no
					// two boundaries land on the same adjacent-key pair. It also gives each child ≥ 1 row.
					threshold = acc + step;
					if (boundaries.length === N - 1) {
						break;
					}
				}
				prev = sk;
			}

			// Boundaries must be strictly above the lower bound and strictly increasing (distinct, non-empty
			// children). On skewed data the scan may yield fewer than N-1 boundaries; treat any shortfall or
			// validation failure as "cannot split yet" (the split retries later). This is the safety net
			// that makes estimate inaccuracy harmless.
			if (boundaries.length !== N - 1) {
				return { floor: "skewed_bytes" };
			}
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
			return { boundaries };
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
	 * Pages `items` with the statement of `itemsPageStatement`. The generator reads and decodes one row
	 * each time the caller asks for the next row, so a caller that stops early reads no payload past
	 * its stop. Read it in one synchronous block, and do not write to `items` while it is open.
	 */
	*queryItemsPage(cursor: ScanCursor | null, limit: number, range: KeyRange | null): Generator<MigratedItem> {
		type Row = {
			item_id: ItemLinkId;
			hk: ArrayBuffer;
			sk: ArrayBuffer;
			data: string | ArrayBuffer;
			data_kind: number;
			ttl_epoch_utc_seconds: number | null;
			v: number;
			last_read_ts: number;
			last_write_ts: number;
		};
		const { sql, params } = itemsPageStatement(cursor, limit, range);
		for (const { data_kind, ...row } of this.#storage.sql.exec<Row>(sql, ...params)) {
			yield {
				...row,
				hk: fromSqlKey(row.hk),
				sk: fromSqlKey(row.sk),
				data: fromSqlData(row.data),
				kind: kindFromCode(data_kind),
			};
		}
	}

	/** The hash-key walk of `items`, with the rules of `#walkByHashKey`. */
	walkItemsByHashKey(
		cursor: HashKeyWalkCursor<ScanCursor> | null,
		limit: number,
		owns: (hk: KeyBytes, firstSk: KeyBytes) => boolean,
	): Generator<HashKeyWalkEntry<MigratedItem>> {
		return this.#walkByHashKey("items", cursor, limit, owns, (after, n, range) => this.queryItemsPage(after, n, range));
	}

	/** The hash-key walk of `pending_transactions`, with the rules of `#walkByHashKey`. */
	walkPendingTxByHashKey(
		cursor: HashKeyWalkCursor<PendingTransactionCursor> | null,
		limit: number,
		owns: (hk: KeyBytes, firstSk: KeyBytes) => boolean,
	): Generator<HashKeyWalkEntry<PendingTxItem>> {
		return this.#walkByHashKey("pending_transactions", cursor, limit, owns, (after, n, range) => this.queryPendingTxPage(after, n, range));
	}

	/**
	 * Walks `table` one hash key at a time, strictly after `cursor`, for a read whose rows are not one
	 * key range (a hash child). `owns` decides each hash key one time, from its first row. The walk
	 * gives each row of an owned key, which `readKey` reads with a range on that key. For a key that it
	 * does not own, it gives one `skipped` entry: one seek on the key index steps past all the rows of
	 * that key, and reads no `data`.
	 *
	 * Do NOT find the next hash key with `SELECT DISTINCT hk ... LIMIT ?`. SQLite reads each index entry
	 * of each key for DISTINCT, and does not skip. The walk does one `hk > ?` seek for each key.
	 *
	 * `limit` counts the entries of both kinds, so the scan limit of a caller also stops a run of keys
	 * that it does not own. The walk gives fewer than `limit` entries only at the end of the table. The
	 * caller rules of `queryItemsPage` apply.
	 */
	*#walkByHashKey<T, C extends { hk: KeyBytes; sk: KeyBytes }>(
		table: HashKeyWalkTable,
		cursor: HashKeyWalkCursor<C> | null,
		limit: number,
		owns: (hk: KeyBytes, firstSk: KeyBytes) => boolean,
		readKey: (after: C | null, limit: number, range: KeyRange) => Iterable<T>,
	): Generator<HashKeyWalkEntry<T>> {
		let given = 0;
		if (cursor?.kind === "row") {
			// The cursor stops on a row only inside an owned key, so the rest of that key is owned.
			const { row: at } = cursor;
			for (const row of readKey(at, limit, { hk: at.hk, start: at.sk, end: null })) {
				given++;
				yield { kind: "row", row };
			}
		}
		let after = cursor === null ? null : cursor.kind === "row" ? cursor.row.hk : cursor.hk;
		while (given < limit) {
			const { sql, params } = nextHashKeyStatement(table, after);
			const first = tryOne(this.#storage.sql.exec<{ hk: ArrayBuffer; sk: ArrayBuffer }>(sql, ...params));
			if (first === undefined) {
				return;
			}
			const hk = fromSqlKey(first.hk);
			const firstSk = fromSqlKey(first.sk);
			if (owns(hk, firstSk)) {
				for (const row of readKey(null, limit - given, { hk, start: firstSk, end: null })) {
					given++;
					yield { kind: "row", row };
				}
			} else {
				given++;
				yield { kind: "skipped", hk };
			}
			after = hk;
		}
	}

	/**
	 * Runs one queryItems leaf scan in scan order and hands every candidate to `consumer`, which stops
	 * the scan by returning `false`. The loop allocates nothing per row on its own: the payload of a
	 * candidate is built only when the consumer calls `decodePayload`, after its budgets admitted the
	 * candidate. The returned metrics are the physical reads of the statement up to the stop. A plan
	 * statement selects `matched` plus the gated columns its mode needs.
	 */
	scanQueryPage(
		opts: RangeScanBounds & { limit: number; select: QuerySelect; plan: CompiledQueryPlan | null },
		consumer: QueryCandidateConsumer,
	): SqlMetrics {
		const plan = opts.plan;
		if (plan !== null) {
			withExpressionErrors(() => validateQueryPlan(plan));
		}
		const { sql, params } = queryScanStatement(opts);
		const cursor = this.#storage.sql.exec<Record<string, SqlStorageValue>>(sql, ...params);
		const entryCount = plan?.projection?.names.length ?? 0;
		// The mode is fixed per request. A count scan carries no payload, a complete-item scan carries
		// the item, and a projected scan carries the projected row. A payload exists on a matched row only.
		const mode: "none" | "item" | "projected" = opts.select !== "projection" ? "none" : plan?.projection ? "projected" : "item";

		// One closure for the whole scan reads the row the loop is on, so a consumer that never asks for
		// a payload (count mode, a rejected candidate, a full budget) costs no allocation per row.
		let current: Record<string, SqlStorageValue> | undefined;
		let currentSk: KeyBytes | undefined;
		const decodePayload = (): StoredItem | ProjectedWireRow => {
			const row = current!;
			invariant(mode !== "none", "fokos/partition-store.scanQueryPage: a count scan has no payload to decode");
			if (mode === "projected") {
				return decodeProjectedRow(row, entryCount);
			}
			return {
				hk: fromSqlKey(row.hk as ArrayBuffer),
				sk: currentSk!,
				data: fromSqlData(row.data as string | ArrayBuffer),
				kind: kindFromCode(row.data_kind as number),
				ttl_epoch_utc_seconds: row.ttl_epoch_utc_seconds as number | null,
				v: row.v as number,
				last_read_ts: row.last_read_ts as number,
				last_write_ts: row.last_write_ts as number,
			};
		};

		for (const row of cursor) {
			current = row;
			currentSk = fromSqlKey(row.sk as ArrayBuffer);
			// Without a plan the statement yields no `matched` column; every candidate matches.
			const matched = plan === null ? true : row.matched === 1;
			if (!consumer(currentSk, row.est_row_bytes as number, matched, decodePayload)) {
				break;
			}
		}
		return { rowsRead: cursor.rowsRead, rowsWritten: cursor.rowsWritten };
	}

	// ─── pending_transactions ───────────────────────────────────────────────

	pendingLockFor(hk: KeyBytes, sk: KeyBytes): { transaction_id: string; operation: string } | undefined {
		return tryOne(
			this.#storage.sql.exec<{
				transaction_id: string;
				operation: string;
			}>(`SELECT transaction_id, operation FROM pending_transactions WHERE hk = ? AND sk = ? LIMIT 1`, hk, sk),
		);
	}

	/**
	 * Writes the `pending_tx_info` row of a transaction, or merges `tx` into the row that exists. A merge
	 * keeps a guard and the earlier next attempt, and writes nothing when neither changes. Each
	 * lock insert calls it, so a migration page that repeats the row of a transaction is safe.
	 */
	#upsertPendingTx(tx: PendingTxInfo): void {
		this.#storage.sql.exec(
			`INSERT INTO pending_tx_info (transaction_id, transaction_ts, created_at, coordinator_json, guarded_at, next_recovery_at)
			 VALUES (?, ?, ?, ?, ?, ?)
			 ON CONFLICT (transaction_id) DO UPDATE SET
			   guarded_at = COALESCE(guarded_at, excluded.guarded_at),
			   next_recovery_at = MIN(next_recovery_at, excluded.next_recovery_at)
			 WHERE (guarded_at IS NULL AND excluded.guarded_at IS NOT NULL) OR excluded.next_recovery_at < next_recovery_at`,
			tx.transaction_id,
			tx.transaction_ts,
			tx.created_at,
			tx.coordinator_json,
			tx.guarded_at,
			tx.next_recovery_at,
		);
	}

	/**
	 * Idempotent lock insertion — used by prepare and by migration ingestion of parent locks. Returns
	 * false when a lock row of the key and the transaction exists already.
	 */
	insertPendingLock(row: PendingTxItem): boolean {
		this.#upsertPendingTx(row);
		const res = this.#storage.sql.exec(
			// pending_transactions is never queried by JSON path, so a put's json data is stored raw, as
			// the client's JSON text; the data_kind tag lets commit reconstruct the kind for upsertItem.
			// An update's row instead holds JSONB, which insertPendingUpdateLock explains.
			`INSERT OR IGNORE INTO pending_transactions (hk, sk, transaction_id, operation, data, data_kind, ttl_epoch_utc_seconds)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			row.hk,
			row.sk,
			row.transaction_id,
			row.operation,
			row.data,
			codeFromNullableKind(row.kind),
			row.ttl_epoch_utc_seconds,
		);
		return res.rowsWritten > 0;
	}

	/**
	 * Locks an item and materializes the complete new document into its pending row, so that commit
	 * applies a payload that is already a plain put.
	 *
	 * The source is a LEFT JOIN, not a plain scan of `items`: an update of an absent item creates it,
	 * so the statement must write a lock for a key that has no row yet. A prepare that accepted an
	 * item and wrote no lock would make commit find an empty key set and report success for a write
	 * it never applied.
	 *
	 * The row stores the raw JSONB blob, NOT `json(...)` text. A JSONB-to-text-to-JSONB round trip is
	 * not size-stable: `jsonb_set` keeps a string element unescaped, while re-parsing the rendered
	 * text bakes the escapes into the blob, so `{"k":"he said \"hi\""}` grows by 4 bytes. Storing the
	 * blob makes the bytes that `probeUpdate` measured at prepare the exact bytes commit writes.
	 */
	insertPendingUpdateLock(opts: { hk: KeyBytes; sk: KeyBytes; tx: PendingTxInfo; plan: CompiledUpdatePlan; ttlAt?: number }): {
		rowsRead: number;
		rowsWritten: number;
	} {
		validateUpdatePlan(opts.plan);
		this.#upsertPendingTx(opts.tx);
		const tail = new StatementTail(opts.plan);
		const transactionIdParam = tail.param(opts.tx.transaction_id);
		// The TTL of the pre-image survives unless the operation sets one. WHICH branch applies is known
		// here, so the statement carries the branch it needs instead of testing a flag at run time. The
		// VALUE still binds — see StatementTail for why a per-call value must not be interpolated.
		const ttlExpr = opts.ttlAt === undefined ? "i.ttl_epoch_utc_seconds" : tail.param(opts.ttlAt);

		const res = this.#storage.sql.exec(
			`INSERT OR IGNORE INTO pending_transactions (hk, sk, transaction_id, operation, data_kind, ttl_epoch_utc_seconds, data)
			SELECT ?1, ?2, ${transactionIdParam}, 'update', ${JSON_KIND_CODE}, ${ttlExpr}, ${opts.plan.documentSql}
			FROM (VALUES (1)) LEFT JOIN items AS i ON i.hk = ?1 AND i.sk = ?2`,
			...tail.bindings(opts.hk, opts.sk),
		);
		return { rowsRead: res.rowsRead, rowsWritten: res.rowsWritten };
	}

	/**
	 * Applies an update in one statement, and creates the item when it is absent — the same answer
	 * DynamoDB gives, with the empty document as the pre-image the actions write into. A caller that
	 * needs the item to exist says so with a condition, which prepare evaluates before this runs.
	 *
	 * Throws when the new document would exceed MAX_ITEM_BYTES — see throwItemTooLarge.
	 */
	updateItemSingleShot(opts: { hk: KeyBytes; sk: KeyBytes; plan: CompiledUpdatePlan; ttlAt?: number; txOrderTs: number }): {
		version: number;
		keyEstBytes: number;
		rowsRead: number;
		rowsWritten: number;
	} {
		validateUpdatePlan(opts.plan);
		// Zero means the row is absent, never a row of zero size: est_row_bytes always carries both keys
		// and EST_ROW_BYTES_K. It is also the right old value for the key_size_estimates delta of an
		// item this statement creates. The size guard is then the only cause of a statement that writes
		// no row, which is what lets it report that one cause.
		const oldEst = this.#storedEstRowBytes(opts.hk, opts.sk);
		const docExpr = opts.plan.documentSql;
		const hkParam = "?1";
		const skParam = "?2";
		const tail = new StatementTail(opts.plan);
		const txOrderTsParam = tail.param(opts.txOrderTs);
		// The TTL of the pre-image survives unless the operation sets one: the insert then carries the
		// joined row's TTL, which is NULL for an item this statement creates, and the conflict branch
		// assigns that same value back. WHICH branch applies is known here, so the statement carries the
		// branch it needs instead of testing a flag at run time. The value still binds.
		const ttlExpr = opts.ttlAt === undefined ? "i.ttl_epoch_utc_seconds" : tail.param(opts.ttlAt);
		const limitParam = tail.param(MAX_ITEM_BYTES);

		// The source is a LEFT JOIN over items, so the document expression reads the stored row when
		// there is one and the empty pre-image when there is not, and one statement covers both. The
		// WHERE clause holds the size guard: when it removes the source row, neither branch runs and the
		// statement returns nothing. The one deletion_metadata row gives the start values of a new row,
		// with the same rules and the same reason as upsertItem.
		const writeRes = this.#storage.sql.exec<{ v: number; est_row_bytes: number }>(
			`INSERT INTO items (hk, sk, data_kind, ttl_epoch_utc_seconds, v, last_read_ts, last_write_ts, est_row_bytes, data)
			 SELECT ${hkParam}, ${skParam}, ${JSON_KIND_CODE}, ${ttlExpr}, d.max_deleted_v + 1,
			        MAX(${txOrderTsParam}, d.max_delete_tx_order_ts), MAX(${txOrderTsParam}, d.max_delete_tx_order_ts),
			        ${estRowBytesExpr(docExpr, hkParam, skParam)}, ${docExpr}
			   FROM deletion_metadata AS d LEFT JOIN items AS i ON i.hk = ${hkParam} AND i.sk = ${skParam}
			  WHERE d.id = 1 AND ${estRowBytesExpr(docExpr, hkParam, skParam)} <= ${limitParam}
			 ON CONFLICT(hk, sk) DO UPDATE SET
			   data = excluded.data,
			   data_kind = excluded.data_kind,
			   ttl_epoch_utc_seconds = excluded.ttl_epoch_utc_seconds,
			   est_row_bytes = excluded.est_row_bytes,
			   v = v + 1,
			   last_read_ts = MAX(last_read_ts, ${txOrderTsParam}),
			   last_write_ts = MAX(last_write_ts, ${txOrderTsParam})
			 RETURNING v, est_row_bytes`,
			...tail.bindings(opts.hk, opts.sk),
		);
		const rows = writeRes.toArray();
		if (rows.length === 0) {
			throwItemTooLarge(opts.hk, opts.sk);
		}
		const version = rows[0].v;
		invariant(
			typeof version === "number" && Number.isInteger(version) && version >= 1,
			`fokos/partition-store.updateItemSingleShot: unexpected version value: ${version}`,
		);
		const newEst = rows[0].est_row_bytes;
		const kseRow = tryOne(
			this.#storage.sql.exec<{ est_bytes: number }>(
				`INSERT INTO key_size_estimates (hk, est_bytes) VALUES (?, ?)
				 ON CONFLICT(hk) DO UPDATE SET est_bytes = MAX(0, est_bytes + excluded.est_bytes - ?)
				 RETURNING est_bytes`,
				opts.hk,
				newEst,
				oldEst,
			),
		);

		return {
			version,
			keyEstBytes: kseRow?.est_bytes ?? newEst,
			rowsRead: writeRes.rowsRead,
			rowsWritten: writeRes.rowsWritten,
		};
	}

	/** Does this partition hold any pending lock? */
	hasAnyPendingTx(): boolean {
		return this.#storage.sql.exec(`SELECT 1 FROM pending_transactions LIMIT 1`).toArray().length > 0;
	}

	/**
	 * The earliest next attempt of an unguarded transaction, or null when there is none. The scheduler
	 * reads it as the deadline of the stale-transaction job. One seek of `pending_tx_info_due`.
	 *
	 * A transaction whose locks here are only copies of a moved key also counts. Its attempt finds no
	 * owned row and moves the time forward, so it costs one attempt per interval and does not keep the
	 * deadline in the past.
	 */
	earliestPendingTxRecoveryAt(): number | null {
		return (
			tryOne(
				this.#storage.sql.exec<{ next_recovery_at: number }>(
					`SELECT next_recovery_at FROM pending_tx_info WHERE guarded_at IS NULL ORDER BY next_recovery_at LIMIT 1`,
				),
			)?.next_recovery_at ?? null
		);
	}

	/** The keys that one transaction locks here. It reads only `pending_transactions_transaction_id`. */
	listPendingTxKeys(transactionId: string): { hk: KeyBytes; sk: KeyBytes }[] {
		return this.#storage.sql
			.exec<{ hk: ArrayBuffer; sk: ArrayBuffer }>(`SELECT hk, sk FROM pending_transactions WHERE transaction_id = ?`, transactionId)
			.toArray()
			.map((r) => ({ hk: fromSqlKey(r.hk), sk: fromSqlKey(r.sk) }));
	}

	/**
	 * The locked items of one transaction, data converted, each with the facts of its transaction.
	 * Commit, stale recovery and forced resolution read it. One seek of `pending_tx_info` and one range of
	 * `pending_transactions_transaction_id`.
	 */
	listPendingTxItems(transactionId: string): Omit<PendingTxItem, "transaction_id" | "coordinator_json">[] {
		const tx = tryOne(
			this.#storage.sql.exec<{ transaction_ts: number; created_at: number; guarded_at: number | null; next_recovery_at: number }>(
				`SELECT transaction_ts, created_at, guarded_at, next_recovery_at FROM pending_tx_info WHERE transaction_id = ?`,
				transactionId,
			),
		);
		if (!tx) {
			return [];
		}
		const cursor = this.#storage.sql.exec<{
			hk: ArrayBuffer;
			sk: ArrayBuffer;
			operation: string;
			data: string | ArrayBuffer | null;
			data_kind: number | null;
			ttl_epoch_utc_seconds: number | null;
		}>(
			`SELECT hk, sk, operation, data, data_kind, ttl_epoch_utc_seconds FROM pending_transactions WHERE transaction_id = ?`,
			transactionId,
		);
		const items: Omit<PendingTxItem, "transaction_id" | "coordinator_json">[] = [];
		for (const row of cursor) {
			items.push({
				transaction_ts: tx.transaction_ts,
				created_at: tx.created_at,
				guarded_at: tx.guarded_at,
				next_recovery_at: tx.next_recovery_at,
				hk: fromSqlKey(row.hk),
				sk: fromSqlKey(row.sk),
				operation: row.operation,
				data: fromSqlData(row.data),
				kind: kindFromNullableCode(row.data_kind),
				ttl_epoch_utc_seconds: row.ttl_epoch_utc_seconds,
			});
		}
		return items;
	}

	/**
	 * The unguarded transactions whose next attempt is at or before `dueAt`, earliest first, at most
	 * `limit` of them. One range of `pending_tx_info_due`, one entry for each transaction. The caller
	 * moves the next attempt of each one forward, so a transaction that stays does not block the others.
	 */
	listStalePendingTx(dueAt: number, limit: number): StalePendingTx[] {
		return this.#storage.sql
			.exec<StalePendingTx>(
				`SELECT transaction_id, coordinator_json, created_at FROM pending_tx_info
				  WHERE guarded_at IS NULL AND next_recovery_at <= ? ORDER BY next_recovery_at LIMIT ?`,
				dueAt,
				limit,
			)
			.toArray();
	}

	/** Sets the next attempt of one transaction. */
	deferPendingTxRecovery(transactionId: string, nextRecoveryAt: number): void {
		this.#storage.sql.exec(`UPDATE pending_tx_info SET next_recovery_at = ? WHERE transaction_id = ?`, nextRecoveryAt, transactionId);
	}

	/**
	 * Quarantines one transaction, and answers whether it changed the row. The guard applies to each
	 * lock of the transaction here, copies of a moved key included. The migration carries it to the
	 * new owner of a moved key. This is correct because the cause of a guard, a coordinator that
	 * answers `not_found` for a lock older than the idempotency window, is the same for each key.
	 */
	guardPendingTx(transactionId: string, guardedAt: number): boolean {
		return (
			this.#storage.sql.exec(
				`UPDATE pending_tx_info SET guarded_at = ? WHERE transaction_id = ? AND guarded_at IS NULL`,
				guardedAt,
				transactionId,
			).rowsWritten > 0
		);
	}

	/** Deletes the `pending_tx_info` row of a transaction that has no lock row left. */
	#deletePendingTxIfUnlocked(transactionId: string): void {
		this.#storage.sql.exec(
			`DELETE FROM pending_tx_info WHERE transaction_id = ?1
			   AND NOT EXISTS (SELECT 1 FROM pending_transactions WHERE transaction_id = ?1)`,
			transactionId,
		);
	}

	/**
	 * Releases the locks of one transaction under `keys`, one statement per key. A transaction can hold
	 * up to MAX_ITEMS_PER_TX (100) keys here, and one statement binds at most 100 parameters.
	 *
	 * Do not delete by transaction id alone. That delete also removes the copies of a key that a
	 * promotion moved away, and the source must keep them until the new owner acknowledges its import.
	 */
	deletePendingTxKeys(transactionId: string, keys: readonly PendingTxKey[]): void {
		for (const key of keys) {
			this.#storage.sql.exec(
				`DELETE FROM pending_transactions WHERE hk = ? AND sk = ? AND transaction_id = ?`,
				key.hashKey,
				key.sortKey,
				transactionId,
			);
		}
		this.#deletePendingTxIfUnlocked(transactionId);
	}

	/**
	 * Deletes the lock copies that a completed repartition left here, until it deletes `maxRows` rows
	 * or no copy remains. With `hk`, the copies are the rows of that hash key (a promotion). With null,
	 * every lock row is a copy (a split). Returns true when no copy remains.
	 *
	 * One statement deletes all copies of one transaction, so a transaction never keeps only a part of
	 * its copies. A transaction has at most MAX_ITEMS_PER_TX (100) rows, thus one call can delete up
	 * to 99 rows more than `maxRows`.
	 */
	deletePendingTxCopiesBatch(hk: KeyBytes | null, maxRows: number): boolean {
		const sql = this.#storage.sql;
		let deleted = 0;
		while (deleted < maxRows) {
			const next = tryOne(
				hk === null
					? sql.exec<{ transaction_id: string }>(`SELECT transaction_id FROM pending_transactions LIMIT 1`)
					: sql.exec<{ transaction_id: string }>(`SELECT transaction_id FROM pending_transactions WHERE hk = ? LIMIT 1`, hk),
			);
			if (!next) {
				return true;
			}
			const rows =
				hk === null
					? sql.exec(`DELETE FROM pending_transactions WHERE transaction_id = ? RETURNING 1`, next.transaction_id)
					: sql.exec(`DELETE FROM pending_transactions WHERE transaction_id = ? AND hk = ? RETURNING 1`, next.transaction_id, hk);
			deleted += rows.toArray().length;
			this.#deletePendingTxIfUnlocked(next.transaction_id);
		}
		return false;
	}

	/**
	 * Pages `pending_transactions` with the statement of `pendingTxPageStatement`. The generator reads
	 * one row at a time, as queryItemsPage does, with the same rules for the caller. Do not write to
	 * `pending_transactions` or `pending_tx_info` while it is open.
	 */
	*queryPendingTxPage(cursor: PendingTransactionCursor | null, limit: number, range: KeyRange | null): Generator<PendingTxItem> {
		type Row = Omit<PendingTxItem, "hk" | "sk" | "data" | "kind"> & {
			hk: ArrayBuffer;
			sk: ArrayBuffer;
			data: string | ArrayBuffer | null;
			data_kind: number | null;
		};
		const { sql, params } = pendingTxPageStatement(cursor, limit, range);
		for (const { data_kind, ...row } of this.#storage.sql.exec<Row>(sql, ...params)) {
			yield {
				...row,
				hk: fromSqlKey(row.hk),
				sk: fromSqlKey(row.sk),
				data: fromSqlData(row.data),
				kind: kindFromNullableCode(data_kind),
			};
		}
	}

	// ─── deletion_metadata ──────────────────────────────────────────────────

	getMaxDeleteTxOrderTs(): number {
		return (
			tryOne(
				this.#storage.sql.exec<{ max_delete_tx_order_ts: number }>(`SELECT max_delete_tx_order_ts FROM deletion_metadata WHERE id = 1`),
			)?.max_delete_tx_order_ts ?? 0
		);
	}

	/** The single definition of the deletion transaction-order-watermark update (monotonic MAX). */
	bumpMaxDeleteTxOrderTs(ts: number): void {
		this.#storage.sql.exec(`UPDATE deletion_metadata SET max_delete_tx_order_ts = MAX(max_delete_tx_order_ts, ?) WHERE id = 1`, ts);
	}

	/**
	 * Raises both deletion-metadata values with MAX in one statement. After a delete, `maxDeleteTxOrderTs`
	 * is the highest of the delete timestamps and of the `last_read_ts` of the removed rows, and
	 * `maxDeletedV` is the highest `v` of the removed rows.
	 */
	#raiseDeletionMetadata(maxDeleteTxOrderTs: number, maxDeletedV: number): void {
		this.#storage.sql.exec(
			`UPDATE deletion_metadata SET max_delete_tx_order_ts = MAX(max_delete_tx_order_ts, ?), max_deleted_v = MAX(max_deleted_v, ?) WHERE id = 1`,
			maxDeleteTxOrderTs,
			maxDeletedV,
		);
	}

	/** The `max_deleted_v` that a transactional read reports for an absent item. */
	getMaxDeletedV(): number {
		return (
			tryOne(this.#storage.sql.exec<{ max_deleted_v: number }>(`SELECT max_deleted_v FROM deletion_metadata WHERE id = 1`))
				?.max_deleted_v ?? 0
		);
	}

	/** Both deletion-metadata values in one read: each `pending_tx` migration page carries them. */
	getDeletionMetadata(): { maxDeleteTxOrderTs: number; maxDeletedV: number } {
		const row = tryOne(
			this.#storage.sql.exec<{
				max_delete_tx_order_ts: number;
				max_deleted_v: number;
			}>(`SELECT max_delete_tx_order_ts, max_deleted_v FROM deletion_metadata WHERE id = 1`),
		);
		return { maxDeleteTxOrderTs: row?.max_delete_tx_order_ts ?? 0, maxDeletedV: row?.max_deleted_v ?? 0 };
	}

	/** Migration ingest: idempotent, merges both values with MAX. */
	mergeDeletionMetadata(meta: { maxDeleteTxOrderTs: number; maxDeletedV: number }): void {
		this.#raiseDeletionMetadata(meta.maxDeleteTxOrderTs, meta.maxDeletedV);
	}

	// ─── key_size_estimates ─────────────────────────────────────────────────

	deleteKeySizeEstimate(hk: KeyBytes): void {
		this.#storage.sql.exec(`DELETE FROM key_size_estimates WHERE hk = ?`, hk);
	}

	/**
	 * Removes the bytes of deleted rows from a key's estimate. When the deleted rows held all the
	 * bytes of the key, the key has no committed item, so the estimate row and its index entry are
	 * deleted. Without this, a partition keeps one estimate row for each key it ever stored. The
	 * estimate is exact and every item row has more than 0 bytes, so a total at or below `bytes`
	 * means that no item of the key remains. A key that is written again gets a new row from the
	 * upsert.
	 */
	#subtractKeySizeEstimate(hk: KeyBytes, bytes: number): void {
		// `est_bytes <= bytes` is the same test as `est_bytes - bytes <= 0`: the deleted rows were the
		// last items of the key. We use `<=` and not `=` so that a total that is too low also goes,
		// and does not stay as a row of 0 bytes.
		const removed = this.#storage.sql.exec(`DELETE FROM key_size_estimates WHERE hk = ? AND est_bytes <= ?`, hk, bytes);
		if (removed.rowsWritten === 0) {
			this.#storage.sql.exec(`UPDATE key_size_estimates SET est_bytes = MAX(0, est_bytes - ?) WHERE hk = ?`, bytes, hk);
		}
	}

	/**
	 * Up to `limit` hash keys whose estimate is at least `minBytes`, largest first. One seek of
	 * `key_size_estimates_by_bytes`, which reads at most `limit` rows.
	 */
	largestKeysAtLeast(minBytes: number, limit: number): KeyBytes[] {
		return this.#storage.sql
			.exec<{ hk: ArrayBuffer }>(`SELECT hk FROM key_size_estimates WHERE est_bytes >= ? ORDER BY est_bytes DESC LIMIT ?`, minBytes, limit)
			.toArray()
			.map((row) => fromSqlKey(row.hk));
	}

	/**
	 * Adds the bytes of ingested rows to a key's running estimate. The import calls it once per page,
	 * with the exact sizes `insertItemIfAbsent` measured, so the estimate stays correct without a
	 * whole-table rebuild at the end. Zero bytes writes nothing, which keeps a page of already-present
	 * rows free.
	 */
	addKeySizeEstimate(hk: KeyBytes, bytes: number): void {
		if (bytes <= 0) {
			return;
		}
		this.#storage.sql.exec(
			`INSERT INTO key_size_estimates (hk, est_bytes) VALUES (?, ?)
			 ON CONFLICT(hk) DO UPDATE SET est_bytes = est_bytes + excluded.est_bytes`,
			hk,
			bytes,
		);
	}
}
