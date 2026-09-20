import { SQLSchemaMigration, SQLSchemaMigrations } from "durable-utils/sql-migrations";
import { DATA_KINDS, type DataKind, type QuerySelect } from "../types.js";
import type { RangeAncestorInfo } from "../partition-topology/types.js";
import { KeyCodec, type KeyBytes } from "../partition-topology/key-codec.js";
import invariant from "../invariant.js";
import { one, tryOne } from "../sql-cursor.js";
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
 * PartitionStore owns ALL SQL on the partition's tables: items, pending_transactions,
 * deletion_metadata, key_size_estimates, range_hierarchy, and the three repartition tables —
 * fokos_repartitions, fokos_repartition_targets and fokos_route_overrides — plus the schema
 * migrations and the row-size estimators. No other class touches these tables.
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

/** Scan checkpoint of the route-override stream, in `hash_key` order. */
export type PromotedKeyCursor = { hashKey: KeyBytes };

/** The promotion lifecycle as `status()` reports it, derived from a repartition's own state. */
export type PromotedKeyStatus = "queued" | "promoting" | "promoted";

export type SqlMetrics = { rowsRead: number; rowsWritten: number };

// ─── repartitions ───────────────────────────────────────────────────────────

/** What one repartition moves: every key, one sort-key interval, or one hash key. */
export type RepartitionKind = "hash_split" | "range_split" | "key_promotion";

/**
 * The source lifecycle, identical for all three kinds. `completed` means every target acknowledged
 * and source cleanup is pending; `cleaned` means that cleanup finished and is terminal. A split has
 * nothing to reclaim, so it passes through `completed` and reaches `cleaned` on the next pass.
 */
export type RepartitionState = "queued" | "planned" | "cutover" | "completed" | "cleaned";

/** Where one target stands in the source's initialization fan-out. */
export type TargetInitialization = "pending" | "initializing" | "initialized";

/**
 * The stored part of a target's slice. A hash child carries no depth: its depth is the source depth
 * plus one, which only the source knows, so the flow adds it when it builds an import record or a
 * migration filter.
 */
export type RepartitionSlice =
	| { kind: "hash_child"; childIndex: number }
	| { kind: "range"; hashKey: KeyBytes; start: KeyBytes | null; end: KeyBytes | null }
	| { kind: "promoted_key"; hashKey: KeyBytes };

export type RepartitionRow = {
	id: string;
	seq: number;
	kind: RepartitionKind;
	state: RepartitionState;
	/** The promoted key of a `key_promotion`; null for a split. */
	hashKey: KeyBytes | null;
	// The lifecycle stamps, in the order the state machine reaches them.
	queuedAt: number;
	cutoverAt: number | null;
	completedAt: number | null;
	attempts: number;
	nextAttemptAt: number;
};

export type RepartitionTargetRow = {
	repartitionId: string;
	partitionId: string;
	doName: string;
	targetIndex: number;
	slice: RepartitionSlice;
	initialization: TargetInitialization;
	startNotified: boolean;
	acknowledged: boolean;
	attempts: number;
	nextAttemptAt: number;
};

/** Used by each source step to decide whether the repartition can advance. */
export type RepartitionTargetCounts = {
	total: number;
	initialized: number;
	startNotified: number;
	acknowledged: number;
};

/** One `(repartition, target)` pair of the paginated administration view; `targetIndex` −1 means no target. */
export type RepartitionStatusRow = {
	id: string;
	seq: number;
	kind: RepartitionKind;
	state: RepartitionState;
	targetIndex: number;
	partitionId: string | null;
	doName: string | null;
	initialization: TargetInitialization | null;
	acknowledged: boolean;
};

export type RepartitionStatusCursor = { seq: number; targetIndex: number };

/**
 * A repartition row has a source step to run when this predicate holds, and the alarm and the due-row
 * selection must agree on it. A `cutover` row whose targets are all notified waits for their
 * acknowledgements instead, which arrive as RPCs — selecting it would count an empty step as progress
 * and reschedule the pass forever.
 */
const REPARTITION_HAS_STEP = `(
	r.state IN ('queued', 'planned')
	OR (r.state = 'cutover'
	    AND EXISTS (SELECT 1 FROM fokos_repartition_targets t WHERE t.repartition_id = r.id AND t.start_notified = 0))
)`;

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

// ─── repartition row mapping ────────────────────────────────────────────────

const REPARTITION_SELECT = `SELECT r.id, r.seq, r.kind, r.state, r.queued_at, r.cutover_at, r.completed_at,
	r.attempts, r.next_attempt_at, r.hash_key FROM fokos_repartitions r`;

const TARGET_SELECT = `SELECT repartition_id, target_index, initialization, start_notified, acknowledged, attempts, next_attempt_at,
	slice_child_idx, partition_id, do_name, slice_hash_key, slice_start, slice_end FROM fokos_repartition_targets`;

/** The slice kind each repartition kind hands its targets. The one place the relation is written down. */
const SLICE_KIND_OF: Record<RepartitionKind, RepartitionSlice["kind"]> = {
	hash_split: "hash_child",
	range_split: "range",
	key_promotion: "promoted_key",
};

type SqlRepartitionRow = {
	id: string;
	seq: number;
	kind: RepartitionKind;
	state: RepartitionState;
	queued_at: number;
	cutover_at: number | null;
	completed_at: number | null;
	attempts: number;
	next_attempt_at: number;
	hash_key: ArrayBuffer | null;
};

type SqlTargetRow = {
	repartition_id: string;
	target_index: number;
	initialization: TargetInitialization;
	start_notified: number;
	acknowledged: number;
	attempts: number;
	next_attempt_at: number;
	slice_child_idx: number | null;
	partition_id: string;
	do_name: string;
	slice_hash_key: ArrayBuffer | null;
	slice_start: ArrayBuffer | null;
	slice_end: ArrayBuffer | null;
};

function toRepartitionRow(r: SqlRepartitionRow): RepartitionRow {
	return {
		id: r.id,
		seq: r.seq,
		kind: r.kind,
		state: r.state,
		hashKey: r.hash_key === null ? null : fromSqlKey(r.hash_key),
		queuedAt: r.queued_at,
		cutoverAt: r.cutover_at,
		completedAt: r.completed_at,
		attempts: r.attempts,
		nextAttemptAt: r.next_attempt_at,
	};
}

function toTargetSlice(r: SqlTargetRow, kind: RepartitionKind): RepartitionSlice {
	switch (SLICE_KIND_OF[kind]) {
		case "hash_child":
			invariant(r.slice_child_idx !== null, "fokos/partition-store: hash_child target slice has no child index");
			return { kind: "hash_child", childIndex: r.slice_child_idx };
		case "range":
			invariant(r.slice_hash_key !== null, "fokos/partition-store: range target slice has no hash key");
			return {
				kind: "range",
				hashKey: fromSqlKey(r.slice_hash_key),
				start: r.slice_start === null ? null : fromSqlKey(r.slice_start),
				end: r.slice_end === null ? null : fromSqlKey(r.slice_end),
			};
		case "promoted_key":
			invariant(r.slice_hash_key !== null, "fokos/partition-store: promoted_key target slice has no hash key");
			return { kind: "promoted_key", hashKey: fromSqlKey(r.slice_hash_key) };
	}
}

function toTargetRow(r: SqlTargetRow, kind: RepartitionKind): RepartitionTargetRow {
	return {
		repartitionId: r.repartition_id,
		partitionId: r.partition_id,
		doName: r.do_name,
		targetIndex: r.target_index,
		slice: toTargetSlice(r, kind),
		initialization: r.initialization,
		startNotified: r.start_notified === 1,
		acknowledged: r.acknowledged === 1,
		attempts: r.attempts,
		nextAttemptAt: r.next_attempt_at,
	};
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
		if (typeof cell === "string") bytes += cell.length * 2;
		else if (cell instanceof Uint8Array) bytes += cell.byteLength;
		else if (cell !== null && typeof cell === "object") bytes += cell.json.length * 2;
		else bytes += 8;
	}
	return bytes;
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
		// column and so cannot be seeked on its own. Every whole-transaction operation filters by it —
		// pendingTxCountFor, listPendingTxKeys, listPendingTxItems, deletePendingTx. Also, deletePendingTx
		// and listPendingTxKeys run on every commit and abort, so without this index the cost of
		// committing ONE transaction is O(all pending rows in the partition). Measured over 20k pending
		// rows: 147 page reads drop to 3, and listPendingTxItems drops from 2859 to 8.
		//
		// Its key carries (hk, sk) EXPLICITLY, and that is what pays for the rowid table. A rowid table
		// appends only the rowid to an index entry, so a key of `transaction_id` alone would send
		// listPendingTxKeys — a commit-and-abort path — back to one table fetch per row, and SQLite would
		// also stop choosing pending_transactions_created_at for listStalePendingTx and scan this index
		// instead. The explicit (hk, sk) keeps both plans. (A WITHOUT ROWID table appends the whole
		// primary key instead, which is what covered these queries for free before.)
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
                id                     INTEGER PRIMARY KEY CHECK (id = 1),
                max_delete_tx_order_ts INTEGER NOT NULL DEFAULT 0,
                delete_revision        INTEGER NOT NULL DEFAULT 0
            ) STRICT;
            INSERT OR IGNORE INTO deletion_metadata (id, max_delete_tx_order_ts, delete_revision) VALUES (1, 0, 0);`,
	},
	{
		idMonotonicInc: 3,
		description: "Add repartition support: the source plan, its targets, and the route overrides of promoted keys",
		// One durable model carries hash splits, range splits and key promotions, so one transaction can
		// arbitrate between them. Two separate models cannot: each one decides without seeing the other,
		// and a split record and a promotion record can then exist at the same time for one key.
		//
		// Column order in both rowid tables: SQLite reads a record from the start until it has every
		// column the query needs, so the wide columns go LAST and a query that reads only the small ones
		// never touches their overflow pages. The wide columns are the keys and the derived names: a hash
		// key reaches MAX_HASH_KEY_BYTES, and a range partition_id and doName both encode the two
		// boundaries, so each can be kilobytes. The columns the hot paths read — the due-row scan, the
		// alarm deadline, and the target counts — all sit before them. The order of a PRIMARY KEY or
		// UNIQUE declaration is independent of it, so the indexes are unaffected.
		//
		// fokos_repartitions:
		// - `id` is `r<seq>`, local to this partition. `seq` comes from MAX(seq) + 1 and never repeats,
		//   because rows are permanent. The source doName plus this id is a global identity.
		// - The lifecycle stamps are declared in the order the state machine reaches them.
		// - Every kind ends the same way: `completed` means every target acknowledged and source cleanup
		//   is pending, and `cleaned` means that cleanup finished. A split reclaims no item rows, so its
		//   cleanup step deletes nothing and reports itself done, which keeps one code path for all three
		//   kinds. No separate "cleanup has started" flag: `completed` already implies it.
		// - `hash_key` is the promoted key of a key_promotion, and NULL for a split: a hash split moves
		//   every key, and a range split moves an interval of the key the partition identity already holds.
		// - `next_attempt_at` is the deadline of this row's NEXT source step, so the due-row scan and the
		//   alarm both read one column. idx_fokos_repartitions_due serves that ordered scan.
		// - idx_fokos_repartitions_split makes "does a split row exist?" — asked by every arbitration —
		//   one partial-index seek rather than a scan of every promotion row.
		//
		// fokos_repartition_targets:
		// - The slice is columns, not one blob: migration filters rows by it and range routing orders
		//   children by it, and both are SQL.
		// - No slice_kind column. A repartition never mixes slice kinds, so the parent row's `kind` gives
		//   it: hash_split means hash_child, range_split means range, key_promotion means promoted_key.
		//   Storing it again on every target would also make a range slice with two unbounded edges
		//   indistinguishable from a promoted_key slice by its columns alone.
		// - `target_index` defines target order. Range partition IDs do not sort by boundary.
		// - No depth column for a hash_child slice: the depth is the source depth plus one, and the
		//   target partition_id already encodes it.
		//
		// fokos_route_overrides:
		// - The forward pointer for one hash key whose data now lives in a range tree. It joins to its
		//   repartition row for the state, so a point lookup on the routing path is one indexed join.
		// - WITHOUT ROWID, as key_size_estimates is: the row is its own index entry, so a rowid table
		//   would store the hash key twice. There is nothing to move last — the key must lead the
		//   primary key, and the only other column is the short repartition id.
		sql: `
            CREATE TABLE IF NOT EXISTS fokos_repartitions (
                id              TEXT    NOT NULL PRIMARY KEY,
                seq             INTEGER NOT NULL,
                kind            TEXT    NOT NULL,
                state           TEXT    NOT NULL,
                queued_at       INTEGER NOT NULL,
                cutover_at      INTEGER,
                completed_at    INTEGER,
                attempts        INTEGER NOT NULL DEFAULT 0,
                next_attempt_at INTEGER NOT NULL,
                hash_key        BLOB
            ) STRICT;
            CREATE UNIQUE INDEX IF NOT EXISTS idx_fokos_repartitions_seq
                ON fokos_repartitions (seq);
            CREATE INDEX IF NOT EXISTS idx_fokos_repartitions_due
                ON fokos_repartitions (state, next_attempt_at, seq);
            CREATE INDEX IF NOT EXISTS idx_fokos_repartitions_split
                ON fokos_repartitions (kind) WHERE kind IN ('hash_split', 'range_split');

            CREATE TABLE IF NOT EXISTS fokos_repartition_targets (
                repartition_id     TEXT    NOT NULL,
                target_index       INTEGER NOT NULL,
                initialization     TEXT    NOT NULL DEFAULT 'pending',
                start_notified     INTEGER NOT NULL DEFAULT 0,
                acknowledged       INTEGER NOT NULL DEFAULT 0,
                attempts           INTEGER NOT NULL DEFAULT 0,
                next_attempt_at    INTEGER NOT NULL,
                slice_child_idx    INTEGER,
                partition_id       TEXT    NOT NULL,
                do_name            TEXT    NOT NULL,
                slice_hash_key     BLOB,
                slice_start        BLOB,
                slice_end          BLOB,
                PRIMARY KEY (repartition_id, partition_id),
                UNIQUE (repartition_id, target_index)
            ) STRICT;

            CREATE TABLE IF NOT EXISTS fokos_route_overrides (
                hash_key       BLOB NOT NULL PRIMARY KEY,
                repartition_id TEXT NOT NULL
            ) WITHOUT ROWID, STRICT;`,
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
		if (!row) return { row: undefined, rowsRead: res.rowsRead, rowsWritten: res.rowsWritten };
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
		if (!row) return { row: undefined, rowsRead: res.rowsRead, rowsWritten: res.rowsWritten };
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
		// its coordinator's, which prepare accepts up to MAX_CLOCK_SKEW_MS ahead. MAX is what
		// reconciles them.
		//
		// MAX also cannot drift ahead of the wall clock: it only ever keeps the larger of two values
		// that already exist. Do NOT turn it into an increment (`MAX(last_write_ts + 1, ?)`) —
		// that would run the timestamps forward under sustained writes.
		//
		// bumpItemReadTs applies the same rule to the read watermark alone, for the transactional
		// "check" operation.
		const writeRes = this.#storage.sql.exec<{ v: number; est_row_bytes: number }>(
			`INSERT INTO items (hk, sk, data_kind, ttl_epoch_utc_seconds, v, last_read_ts, last_write_ts, est_row_bytes, data)
			 SELECT ?1, ?2, ?3, ?4, 1, ?5, ?5, ${estRowBytesExpr(dataExpr, "?1", "?2")}, ${dataExpr}
			 WHERE ${estRowBytesExpr(dataExpr, "?1", "?2")} <= ?7
			 ON CONFLICT(hk, sk) DO UPDATE SET
			   data = excluded.data,
			   data_kind = excluded.data_kind,
			   ttl_epoch_utc_seconds = excluded.ttl_epoch_utc_seconds,
			   est_row_bytes = excluded.est_row_bytes,
			   v = v + 1,
			   last_read_ts = MAX(last_read_ts, excluded.last_read_ts),
			   last_write_ts = MAX(last_write_ts, excluded.last_write_ts)
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
		if (rows.length === 0) throwItemTooLarge(opts.hk, opts.sk);
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
	 * when the statement deleted a row. `delete_revision` advances only when a row was removed;
	 * `max_delete_tx_order_ts` also advances for an absent row when `bumpTxOrderTsAlways`.
	 * The metrics cover the DELETE statement ONLY.
	 */
	deleteItem(opts: { hk: KeyBytes; sk: KeyBytes; txOrderTs: number; bumpTxOrderTsAlways?: boolean }): {
		deleted: boolean;
		rowsRead: number;
		rowsWritten: number;
	} {
		const delEst = this.#storedEstRowBytes(opts.hk, opts.sk);

		const writeRes = this.#storage.sql.exec(`DELETE FROM items WHERE hk = ? AND sk = ?`, opts.hk, opts.sk);
		const deleted = writeRes.rowsWritten > 0;
		if (deleted) {
			this.#storage.sql.exec(
				`UPDATE deletion_metadata SET max_delete_tx_order_ts = MAX(max_delete_tx_order_ts, ?), delete_revision = delete_revision + 1 WHERE id = 1`,
				opts.txOrderTs,
			);
		} else if (opts.bumpTxOrderTsAlways) {
			this.bumpMaxDeleteTxOrderTs(opts.txOrderTs);
		}
		if (deleted || opts.bumpTxOrderTsAlways) {
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
					 WHERE item_id IN (
					     SELECT i.item_id FROM items i INDEXED BY idx_items_ttl
					      WHERE i.ttl_epoch_utc_seconds IS NOT NULL
					        AND i.ttl_epoch_utc_seconds <= ?1
					        AND NOT EXISTS (SELECT 1 FROM pending_transactions p WHERE p.hk = i.hk AND p.sk = i.sk)
					        AND NOT EXISTS (SELECT 1 FROM fokos_route_overrides o WHERE o.hash_key = i.hk)
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
			// The sweep reclaims rows whose logical deletion happened at expiry, so it advances the
			// transaction order watermark only and never touches delete_revision.
			if (rows.length > 0) {
				this.bumpMaxDeleteTxOrderTs(maxExpirySeconds * 1000 * TX_ORDER_TS_UNITS_PER_MS);
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
				tryOne(this.#storage.sql.exec<{ est_bytes: number }>(`SELECT est_bytes FROM key_size_estimates WHERE hk = ?`, hashKey))
					?.est_bytes ?? 0;
			if (B <= 0) return null;

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
					// Relative bump (acc + step, not threshold += step): when one oversized row pushes acc past
					// several thresholds at once, the scan still emits one boundary and re-anchors here, so no
					// two boundaries land on the same adjacent-key pair. It also gives each child ≥ 1 row.
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

		let sqlCursor: SqlStorageCursor<Row>;
		if (!cursor) {
			sqlCursor = this.#storage.sql.exec<Row>(
				`SELECT item_id, hk, sk, data, data_kind, ttl_epoch_utc_seconds, v, last_read_ts, last_write_ts FROM items ORDER BY hk, sk LIMIT ?`,
				limit,
			);
		} else {
			sqlCursor = this.#storage.sql.exec<Row>(
				`SELECT item_id, hk, sk, data, data_kind, ttl_epoch_utc_seconds, v, last_read_ts, last_write_ts FROM items WHERE (hk, sk) > (?, ?) ORDER BY hk, sk LIMIT ?`,
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
		const dataProjection = opts.decodeJson ? DATA_SELECT_DECODED : "data";
		const { conds, params } = rangeScanConditions(opts);

		const page = this.#storage.sql
			.exec<Row>(
				`SELECT item_id, hk, sk, ${dataProjection}, data_kind, ttl_epoch_utc_seconds, v, last_read_ts, last_write_ts FROM items WHERE ${conds.join(" AND ")} ORDER BY sk ${opts.direction === "asc" ? "ASC" : "DESC"} LIMIT ?`,
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
			if (mode === "projected") return decodeProjectedRow(row, entryCount);
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
			if (!consumer(currentSk, row.est_row_bytes as number, matched, decodePayload)) break;
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

	/** Idempotent lock insertion — used by prepare and by migration ingestion of parent locks. */
	insertPendingLock(row: PendingTransactionRow): void {
		this.#storage.sql.exec(
			// pending_transactions is never queried by JSON path, so a put's json data is stored raw, as
			// the client's JSON text; the data_kind tag lets commit reconstruct the kind for upsertItem.
			// An update's row instead holds JSONB, which insertPendingUpdateLock explains.
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
	insertPendingUpdateLock(opts: {
		hk: KeyBytes;
		sk: KeyBytes;
		transaction_id: string;
		transaction_ts: number;
		created_at: number;
		coordinator_do_id: string;
		plan: CompiledUpdatePlan;
		conditions_json: string | null;
		ttlAt?: number;
	}): { rowsRead: number; rowsWritten: number } {
		validateUpdatePlan(opts.plan);
		const tail = new StatementTail(opts.plan);
		const transactionIdParam = tail.param(opts.transaction_id);
		const transactionTsParam = tail.param(opts.transaction_ts);
		const createdAtParam = tail.param(opts.created_at);
		const coordinatorParam = tail.param(opts.coordinator_do_id);
		const conditionsParam = tail.param(opts.conditions_json);
		// The TTL of the pre-image survives unless the operation sets one. WHICH branch applies is known
		// here, so the statement carries the branch it needs instead of testing a flag at run time. The
		// VALUE still binds — see StatementTail for why a per-call value must not be interpolated.
		const ttlExpr = opts.ttlAt === undefined ? "i.ttl_epoch_utc_seconds" : tail.param(opts.ttlAt);

		const res = this.#storage.sql.exec(
			`INSERT OR IGNORE INTO pending_transactions (
				hk, sk, transaction_id, transaction_ts, created_at, coordinator_do_id,
				operation, data_kind, conditions_json, ttl_epoch_utc_seconds, guarded_at, data
			)
			SELECT ?1, ?2, ${transactionIdParam}, ${transactionTsParam}, ${createdAtParam}, ${coordinatorParam},
			       'update', ${JSON_KIND_CODE}, ${conditionsParam},
			       ${ttlExpr},
			       NULL,
			       ${opts.plan.documentSql}
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
		// statement returns nothing.
		const writeRes = this.#storage.sql.exec<{ v: number; est_row_bytes: number }>(
			`INSERT INTO items (hk, sk, data_kind, ttl_epoch_utc_seconds, v, last_read_ts, last_write_ts, est_row_bytes, data)
			 SELECT ${hkParam}, ${skParam}, ${JSON_KIND_CODE}, ${ttlExpr}, 1, ${txOrderTsParam}, ${txOrderTsParam},
			        ${estRowBytesExpr(docExpr, hkParam, skParam)}, ${docExpr}
			   FROM (VALUES (1)) LEFT JOIN items AS i ON i.hk = ${hkParam} AND i.sk = ${skParam}
			  WHERE ${estRowBytesExpr(docExpr, hkParam, skParam)} <= ${limitParam}
			 ON CONFLICT(hk, sk) DO UPDATE SET
			   data = excluded.data,
			   data_kind = excluded.data_kind,
			   ttl_epoch_utc_seconds = excluded.ttl_epoch_utc_seconds,
			   est_row_bytes = excluded.est_row_bytes,
			   v = v + 1,
			   last_read_ts = MAX(last_read_ts, excluded.last_read_ts),
			   last_write_ts = MAX(last_write_ts, excluded.last_write_ts)
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

	pendingTxCountFor(transactionId: string): number {
		return one(
			this.#storage.sql.exec<{ n: number }>(`SELECT COUNT(*) as n FROM pending_transactions WHERE transaction_id = ?`, transactionId),
		).n;
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
		return one(this.#storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM pending_transactions WHERE hk = ?`, hk)).n;
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
		const row = tryOne(
			this.#storage.sql.exec<{
				operation: string;
				data: string | ArrayBuffer | null;
				data_kind: number | null;
				ttl_epoch_utc_seconds: number | null;
			}>(
				`SELECT operation, data, data_kind, ttl_epoch_utc_seconds FROM pending_transactions WHERE hk = ? AND sk = ? AND transaction_id = ? LIMIT 1`,
				hk,
				sk,
				transactionId,
			),
		);
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
	 * The delete revision a transactional read reports for `hk`. The hash key parameter is not used yet,
	 * but it will be used when we have per-bucket revision; today one partition-wide counter serves every key.
	 */
	deleteRevisionFor(_hk: KeyBytes): number {
		return (
			tryOne(this.#storage.sql.exec<{ delete_revision: number }>(`SELECT delete_revision FROM deletion_metadata WHERE id = 1`))
				?.delete_revision ?? 0
		);
	}

	/** Both deletion-metadata values in one read: the migration metadata RPC serves them together. */
	getDeletionMetadata(): { maxDeleteTxOrderTs: number; deleteRevision: number } {
		const row = tryOne(
			this.#storage.sql.exec<{
				max_delete_tx_order_ts: number;
				delete_revision: number;
			}>(`SELECT max_delete_tx_order_ts, delete_revision FROM deletion_metadata WHERE id = 1`),
		);
		return { maxDeleteTxOrderTs: row?.max_delete_tx_order_ts ?? 0, deleteRevision: row?.delete_revision ?? 0 };
	}

	/** Migration ingest: idempotent, merges both values with MAX. */
	mergeDeletionMetadata(meta: { maxDeleteTxOrderTs: number; deleteRevision: number }): void {
		this.#storage.sql.exec(
			`UPDATE deletion_metadata SET max_delete_tx_order_ts = MAX(max_delete_tx_order_ts, ?), delete_revision = MAX(delete_revision, ?) WHERE id = 1`,
			meta.maxDeleteTxOrderTs,
			meta.deleteRevision,
		);
	}

	// ─── key_size_estimates ─────────────────────────────────────────────────

	deleteKeySizeEstimate(hk: KeyBytes): void {
		this.#storage.sql.exec(`DELETE FROM key_size_estimates WHERE hk = ?`, hk);
	}

	/**
	 * Adds the bytes of ingested rows to a key's running estimate. The import calls it once per page,
	 * with the exact sizes `insertItemIfAbsent` measured, so the estimate stays correct without a
	 * whole-table rebuild at the end. Zero bytes writes nothing, which keeps a page of already-present
	 * rows free.
	 */
	addKeySizeEstimate(hk: KeyBytes, bytes: number): void {
		if (bytes <= 0) return;
		this.#storage.sql.exec(
			`INSERT INTO key_size_estimates (hk, est_bytes) VALUES (?, ?)
			 ON CONFLICT(hk) DO UPDATE SET est_bytes = est_bytes + excluded.est_bytes`,
			hk,
			bytes,
		);
	}

	// ─── fokos_repartitions ─────────────────────────────────────────────────

	/** The sequence of the next local repartition. Rows are permanent, so a value never repeats. */
	nextRepartitionSeq(): number {
		const row = this.#storage.sql.exec<{ next: number }>(`SELECT COALESCE(MAX(seq), 0) + 1 AS next FROM fokos_repartitions`).one();
		return row.next;
	}

	insertRepartition(row: {
		id: string;
		seq: number;
		kind: RepartitionKind;
		state: RepartitionState;
		hashKey: KeyBytes | null;
		queuedAt: number;
		cutoverAt?: number | null;
		completedAt?: number | null;
		nextAttemptAt: number;
	}): void {
		this.#storage.sql.exec(
			`INSERT INTO fokos_repartitions (id, seq, kind, state, queued_at, cutover_at, completed_at, attempts, next_attempt_at, hash_key)
			 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, ?8, ?9)`,
			row.id,
			row.seq,
			row.kind,
			row.state,
			row.queuedAt,
			row.cutoverAt ?? null,
			row.completedAt ?? null,
			row.nextAttemptAt,
			row.hashKey,
		);
	}

	getRepartition(id: string): RepartitionRow | undefined {
		const row = tryOne(this.#storage.sql.exec<SqlRepartitionRow>(`${REPARTITION_SELECT} WHERE r.id = ?`, id));
		return row && toRepartitionRow(row);
	}

	/**
	 * The one split row of this source, if it has ever queued one. A split source becomes a router and
	 * owns nothing, so it never queues a second split.
	 */
	getSplitRepartition(): RepartitionRow | undefined {
		const row = tryOne(
			this.#storage.sql.exec<SqlRepartitionRow>(`${REPARTITION_SELECT} WHERE r.kind IN ('hash_split', 'range_split') LIMIT 1`),
		);
		return row && toRepartitionRow(row);
	}

	/** True while any key promotion has not reached `completed`. It blocks a hash split. */
	hasUnfinishedPromotion(): boolean {
		return (
			this.#storage.sql
				.exec(`SELECT 1 FROM fokos_repartitions WHERE kind = 'key_promotion' AND state IN ('queued', 'planned', 'cutover') LIMIT 1`)
				.toArray().length > 0
		);
	}

	setRepartitionState(id: string, state: RepartitionState, stamps?: { cutoverAt?: number; completedAt?: number }): void {
		this.#storage.sql.exec(
			`UPDATE fokos_repartitions
			    SET state = ?2,
			        cutover_at = COALESCE(?3, cutover_at),
			        completed_at = COALESCE(?4, completed_at)
			  WHERE id = ?1`,
			id,
			state,
			stamps?.cutoverAt ?? null,
			stamps?.completedAt ?? null,
		);
	}

	setRepartitionAttempt(id: string, attempts: number, nextAttemptAt: number): void {
		this.#storage.sql.exec(`UPDATE fokos_repartitions SET attempts = ?2, next_attempt_at = ?3 WHERE id = ?1`, id, attempts, nextAttemptAt);
	}

	/**
	 * Recomputes the deadline of a row's next source step from its targets: the earliest deadline of a
	 * target that still needs a call, or `now` when no target does and a state step remains.
	 */
	refreshRepartitionDue(id: string, now: number): void {
		this.#storage.sql.exec(
			`UPDATE fokos_repartitions
			    SET next_attempt_at = COALESCE(
			        (SELECT MIN(t.next_attempt_at) FROM fokos_repartition_targets t
			          WHERE t.repartition_id = fokos_repartitions.id
			            AND ((fokos_repartitions.state = 'planned' AND t.initialization != 'initialized')
			              OR (fokos_repartitions.state = 'cutover' AND t.start_notified = 0))),
			        ?2)
			  WHERE id = ?1`,
			id,
			now,
		);
	}

	/**
	 * Makes every unfinished promotion due now, with every target of one that still needs a call.
	 *
	 * A promotion that cannot move a locked key parks itself 5 seconds out. The release of that lock is
	 * the event it waits for, so the caller brings the deadline forward. The retry interval must not
	 * decide how long the key stays where it is.
	 */
	markPromotionsDueNow(now: number): void {
		this.#storage.sql.exec(
			`UPDATE fokos_repartitions SET next_attempt_at = ?1 WHERE kind = 'key_promotion' AND state IN ('queued', 'planned')`,
			now,
		);
		this.#storage.sql.exec(
			`UPDATE fokos_repartition_targets SET next_attempt_at = ?1
			  WHERE initialization != 'initialized'
			    AND repartition_id IN (SELECT id FROM fokos_repartitions WHERE kind = 'key_promotion' AND state IN ('queued', 'planned'))`,
			now,
		);
	}

	/** The earliest due repartition that has a source step. Ordered so a failed row falls behind another. */
	selectDueRepartition(now: number): RepartitionRow | undefined {
		const row = tryOne(
			this.#storage.sql.exec<SqlRepartitionRow>(
				`${REPARTITION_SELECT} WHERE ${REPARTITION_HAS_STEP} AND r.next_attempt_at <= ?1 ORDER BY r.next_attempt_at, r.seq LIMIT 1`,
				now,
			),
		);
		return row && toRepartitionRow(row);
	}

	/** The earliest deadline of any repartition that has a source step, now or later. */
	earliestRepartitionDeadline(): number | null {
		return one(
			this.#storage.sql.exec<{
				deadline: number | null;
			}>(`SELECT MIN(r.next_attempt_at) AS deadline FROM fokos_repartitions r WHERE ${REPARTITION_HAS_STEP}`),
		).deadline;
	}

	/**
	 * The earliest repartition whose source cleanup is still pending, of any kind. A split reclaims no
	 * item rows, so its step reports itself done at once and the caller needs no branch on the kind.
	 */
	selectDueCleanup(now: number): RepartitionRow | undefined {
		const row = tryOne(
			this.#storage.sql.exec<SqlRepartitionRow>(
				`${REPARTITION_SELECT} WHERE r.state = 'completed' AND r.next_attempt_at <= ?1 ORDER BY r.next_attempt_at, r.seq LIMIT 1`,
				now,
			),
		);
		return row && toRepartitionRow(row);
	}

	earliestCleanupDeadline(): number | null {
		return one(
			this.#storage.sql.exec<{ deadline: number | null }>(
				`SELECT MIN(next_attempt_at) AS deadline FROM fokos_repartitions WHERE state = 'completed'`,
			),
		).deadline;
	}

	// ─── fokos_repartition_targets ──────────────────────────────────────────

	insertRepartitionTarget(target: {
		repartitionId: string;
		/** The parent repartition's kind. It fixes the slice kind, which the target row does not store. */
		kind: RepartitionKind;
		partitionId: string;
		doName: string;
		targetIndex: number;
		slice: RepartitionSlice;
		initialization?: TargetInitialization;
		startNotified?: boolean;
		acknowledged?: boolean;
		nextAttemptAt: number;
	}): void {
		const s = target.slice;
		invariant(
			s.kind === SLICE_KIND_OF[target.kind],
			`fokos/partition-store.insertRepartitionTarget: a ${target.kind} takes a ${SLICE_KIND_OF[target.kind]} slice, got ${s.kind}`,
		);
		this.#storage.sql.exec(
			`INSERT INTO fokos_repartition_targets
			   (repartition_id, target_index, initialization, start_notified, acknowledged, attempts, next_attempt_at,
			    slice_child_idx, partition_id, do_name, slice_hash_key, slice_start, slice_end)
			 VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7, ?8, ?9, ?10, ?11, ?12)`,
			target.repartitionId,
			target.targetIndex,
			target.initialization ?? "pending",
			target.startNotified ? 1 : 0,
			target.acknowledged ? 1 : 0,
			target.nextAttemptAt,
			s.kind === "hash_child" ? s.childIndex : null,
			target.partitionId,
			target.doName,
			s.kind === "hash_child" ? null : s.hashKey,
			s.kind === "range" ? s.start : null,
			s.kind === "range" ? s.end : null,
		);
	}

	/**
	 * Every target of a repartition, in `target_index` order — the order range children tile their
	 * interval. `kind` comes from the repartition row, which every caller reads first; it is what names
	 * the slice kind, because the target rows do not store it.
	 */
	listRepartitionTargets(repartitionId: string, kind: RepartitionKind): RepartitionTargetRow[] {
		return this.#storage.sql
			.exec<SqlTargetRow>(`${TARGET_SELECT} WHERE repartition_id = ? ORDER BY target_index`, repartitionId)
			.toArray()
			.map((row) => toTargetRow(row, kind));
	}

	getRepartitionTarget(repartitionId: string, partitionId: string, kind: RepartitionKind): RepartitionTargetRow | undefined {
		const row = tryOne(
			this.#storage.sql.exec<SqlTargetRow>(`${TARGET_SELECT} WHERE repartition_id = ?1 AND partition_id = ?2`, repartitionId, partitionId),
		);
		return row && toTargetRow(row, kind);
	}

	/**
	 * The due targets of one step, at most `limit` of them. `init` selects the targets that still need
	 * a `fokosInit`; `start` selects the ones that still need a start notification.
	 */
	selectDueTargets(
		repartitionId: string,
		kind: RepartitionKind,
		phase: "init" | "start",
		now: number,
		limit: number,
	): RepartitionTargetRow[] {
		const cond = phase === "init" ? `initialization != 'initialized'` : `start_notified = 0`;
		return this.#storage.sql
			.exec<SqlTargetRow>(
				`${TARGET_SELECT} WHERE repartition_id = ?1 AND ${cond} AND next_attempt_at <= ?2 ORDER BY next_attempt_at, target_index LIMIT ?3`,
				repartitionId,
				now,
				limit,
			)
			.toArray()
			.map((row) => toTargetRow(row, kind));
	}

	setTargetInitialization(
		repartitionId: string,
		partitionId: string,
		initialization: TargetInitialization,
		attempts: number,
		nextAttemptAt: number,
	): void {
		this.#storage.sql.exec(
			`UPDATE fokos_repartition_targets SET initialization = ?3, attempts = ?4, next_attempt_at = ?5
			  WHERE repartition_id = ?1 AND partition_id = ?2`,
			repartitionId,
			partitionId,
			initialization,
			attempts,
			nextAttemptAt,
		);
	}

	setTargetAttempt(repartitionId: string, partitionId: string, attempts: number, nextAttemptAt: number): void {
		this.#storage.sql.exec(
			`UPDATE fokos_repartition_targets SET attempts = ?3, next_attempt_at = ?4 WHERE repartition_id = ?1 AND partition_id = ?2`,
			repartitionId,
			partitionId,
			attempts,
			nextAttemptAt,
		);
	}

	setTargetStartNotified(repartitionId: string, partitionId: string, nextAttemptAt: number): void {
		this.#storage.sql.exec(
			`UPDATE fokos_repartition_targets SET start_notified = 1, attempts = 0, next_attempt_at = ?3
			  WHERE repartition_id = ?1 AND partition_id = ?2`,
			repartitionId,
			partitionId,
			nextAttemptAt,
		);
	}

	/** Records one target's acknowledgement. Returns false when it had already acknowledged. */
	setTargetAcknowledged(repartitionId: string, partitionId: string): boolean {
		const res = this.#storage.sql.exec(
			`UPDATE fokos_repartition_targets SET acknowledged = 1 WHERE repartition_id = ?1 AND partition_id = ?2 AND acknowledged = 0`,
			repartitionId,
			partitionId,
		);
		return res.rowsWritten > 0;
	}

	countRepartitionTargets(repartitionId: string): RepartitionTargetCounts {
		const row = this.#storage.sql
			.exec<{ total: number; initialized: number; start_notified: number; acknowledged: number }>(
				`SELECT COUNT(*) AS total,
				        SUM(initialization = 'initialized') AS initialized,
				        SUM(start_notified) AS start_notified,
				        SUM(acknowledged) AS acknowledged
				   FROM fokos_repartition_targets WHERE repartition_id = ?`,
				repartitionId,
			)
			.one();
		return {
			total: row.total,
			initialized: row.initialized ?? 0,
			startNotified: row.start_notified ?? 0,
			acknowledged: row.acknowledged ?? 0,
		};
	}

	// ─── fokos_route_overrides ──────────────────────────────────────────────

	insertRouteOverride(hashKey: KeyBytes, repartitionId: string): void {
		this.#storage.sql.exec(`INSERT OR IGNORE INTO fokos_route_overrides (hash_key, repartition_id) VALUES (?, ?)`, hashKey, repartitionId);
	}

	/**
	 * The routing answer for one hash key: the promotion that owns it and how far that promotion has
	 * got. One indexed join on the hot point-read path — the same shape the previous promoted-key
	 * lookup had.
	 */
	routeOverrideFor(hashKey: KeyBytes): { repartitionId: string; state: RepartitionState } | undefined {
		const row = tryOne(
			this.#storage.sql.exec<{ repartition_id: string; state: RepartitionState }>(
				`SELECT o.repartition_id, r.state FROM fokos_route_overrides o
				   JOIN fokos_repartitions r ON r.id = o.repartition_id
				  WHERE o.hash_key = ?`,
				hashKey,
			),
		);
		return row && { repartitionId: row.repartition_id, state: row.state };
	}

	hasRouteOverride(hashKey: KeyBytes): boolean {
		return this.#storage.sql.exec(`SELECT 1 FROM fokos_route_overrides WHERE hash_key = ? LIMIT 1`, hashKey).toArray().length > 0;
	}

	/**
	 * A terminal override is one whose promotion reached `completed` or `cleaned`. A hash split exports
	 * exactly these to its children and excludes exactly their keys from the item stream.
	 */
	hasTerminalRouteOverride(hashKey: KeyBytes): boolean {
		return (
			this.#storage.sql
				.exec(
					`SELECT 1 FROM fokos_route_overrides o JOIN fokos_repartitions r ON r.id = o.repartition_id
					  WHERE o.hash_key = ? AND r.state IN ('completed', 'cleaned') LIMIT 1`,
					hashKey,
				)
				.toArray().length > 0
		);
	}

	/** Pages the terminal overrides in `hash_key` order, strictly after `cursor`. */
	queryTerminalRouteOverridesPage(cursor: PromotedKeyCursor | null, limit: number): { hashKey: KeyBytes }[] {
		const where = cursor ? `AND o.hash_key > ?2` : ``;
		const params: unknown[] = cursor ? [limit, cursor.hashKey] : [limit];
		return this.#storage.sql
			.exec<{ hash_key: ArrayBuffer }>(
				`SELECT o.hash_key FROM fokos_route_overrides o JOIN fokos_repartitions r ON r.id = o.repartition_id
				  WHERE r.state IN ('completed', 'cleaned') ${where} ORDER BY o.hash_key LIMIT ?1`,
				...params,
			)
			.toArray()
			.map((r) => ({ hashKey: fromSqlKey(r.hash_key) }));
	}

	// ─── repartition status view ────────────────────────────────────────────

	/**
	 * One page of the administration view, ordered by `(seq, target_index)` and resumed strictly after
	 * `cursor`. A repartition with no target appears once with `target_index` −1, so destroy traversal
	 * and status both see every row exactly once.
	 */
	queryRepartitionStatusPage(cursor: RepartitionStatusCursor | null, limit: number): RepartitionStatusRow[] {
		const after = cursor ? `WHERE seq > ?2 OR (seq = ?2 AND target_index > ?3)` : ``;
		const params: unknown[] = cursor ? [limit, cursor.seq, cursor.targetIndex] : [limit];
		return this.#storage.sql
			.exec<{
				id: string;
				seq: number;
				kind: RepartitionKind;
				state: RepartitionState;
				target_index: number;
				partition_id: string | null;
				do_name: string | null;
				initialization: TargetInitialization | null;
				acknowledged: number | null;
			}>(
				`SELECT * FROM (
				    SELECT r.id, r.seq, r.kind, r.state, -1 AS target_index,
				           NULL AS partition_id, NULL AS do_name, NULL AS initialization, NULL AS acknowledged
				      FROM fokos_repartitions r
				     WHERE NOT EXISTS (SELECT 1 FROM fokos_repartition_targets t WHERE t.repartition_id = r.id)
				    UNION ALL
				    SELECT r.id, r.seq, r.kind, r.state, t.target_index,
				           t.partition_id, t.do_name, t.initialization, t.acknowledged
				      FROM fokos_repartitions r
				      JOIN fokos_repartition_targets t ON t.repartition_id = r.id
				 ) ${after}
				 ORDER BY seq, target_index LIMIT ?1`,
				...params,
			)
			.toArray()
			.map((r) => ({
				id: r.id,
				seq: r.seq,
				kind: r.kind,
				state: r.state,
				targetIndex: r.target_index,
				partitionId: r.partition_id,
				doName: r.do_name,
				initialization: r.initialization,
				acknowledged: r.acknowledged === 1,
			}));
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
		// FIXME: Bound this table with a size limit, a TTL, or a cleanup policy. A range router learns
		// more boundaries over time, and nothing removes them.
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
		const row = tryOne(
			this.#storage.sql.exec<{ depth: number; sk_start_boundary: ArrayBuffer; sk_end_boundary: ArrayBuffer }>(
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
			),
		);
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
