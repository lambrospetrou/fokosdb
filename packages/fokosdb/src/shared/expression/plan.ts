import { FokosValidationError, VALIDATION_CODES } from "../errors.js";
import type { JsonPrimitive } from "../json-types.js";
import { estRowBytesExpr, JSON_KIND_CODE } from "../partition/item-size.js";
import type { QuerySelect } from "../types.js";
import type { ExpressionRequiredColumn } from "./semantic.js";

export const CONDITION_PLAN_VERSION = 1 as const;
export const CONDITION_FIXED_BINDING_COUNT = 2;

export const UPDATE_PLAN_VERSION = 1 as const;

export const PROJECTION_PLAN_VERSION = 1 as const;
export const QUERY_PLAN_VERSION = 1 as const;

/** hk and sk, bound after the pool in a projected point read. */
export const PROJECTION_FIXED_BINDING_COUNT = 2;
/**
 * The parameter every pool-layout plan owns, bound always, with the text "[]" when the plan has no
 * descriptor. Workers SQLite requires the bound value count to equal the statement's parameter
 * count, and a statement that runs a pool plan numbers its own parameters explicitly from ?2, so
 * an unused ?1 still counts. Direct-layout plans (condition, update) are unchanged: their
 * parameters follow the statement's fixed head.
 */
export const POOL_PARAM = 1;
/** hk, near bound, far bound, LIMIT: the widest scan tail after the pool of a query plan. */
export const QUERY_MAX_TRAILING_BINDING_COUNT = 4;
/** The scan terms of the widest query statement, used for the SQL size check. */
export const QUERY_WIDEST_SCAN_CONDITIONS: readonly string[] = ["hk = ?", "sk >= ?", "sk <= ?"];

/**
 * Parameters every statement that runs an update plan binds BEFORE the plan's own, in this order:
 * the hash key and the sort key. The compiler offsets the plan's parameters past them, exactly as it
 * does for a condition, so every such statement shares one numbering and one binding order.
 */
export const UPDATE_FIXED_BINDING_COUNT = 2;

/**
 * The widest statement-local tail any statement appends AFTER an update plan's parameters — today
 * `PartitionStore.insertPendingUpdateLock`, with the transaction id, its timestamp, the created-at
 * stamp, the coordinator reference, the condition JSON, and the TTL of an operation that sets one.
 *
 * The compiler charges it to every plan, because one plan is embedded by every statement and its
 * parameter numbering is fixed when it compiles. Raising a statement's tail therefore lowers the
 * budget for every update expression, which is why the number lives here and not at the call site.
 */
export const UPDATE_MAX_TRAILING_BINDING_COUNT = 6;

export function composeConditionStatement(predicateSql: string): string {
	return `WITH requested(requested_hk, requested_sk) AS (VALUES (?, ?))
SELECT i.hk IS NOT NULL AS item_present,
       CASE WHEN (${predicateSql}) THEN 1 ELSE 0 END AS condition_ok,
       i.last_read_ts,
       i.last_write_ts
FROM requested
LEFT JOIN items AS i ON i.hk = requested.requested_hk AND i.sk = requested.requested_sk`;
}

/**
 * The statement that reads what an update plan gives for one item, and writes nothing.
 *
 * ?1 and ?2 are the keys, as they are in every statement that runs an update plan.
 *
 * value_type_ok names ONE cause of an inapplicable update, so a caller learns that its value was
 * bytes for this item instead of only that the update did not apply. It runs over a JSON pre-image
 * only: the fragment can read a JSON path, and json_type over a text or bytes row raises. An absent
 * row has a JSON pre-image, because an update creates the item.
 */
export function composeUpdateProbeStatement(plan: Pick<CompiledUpdatePlan, "documentSql" | "applicableSql" | "valueTypeSql">): string {
	return `WITH requested(requested_hk, requested_sk) AS (VALUES (?1, ?2))
SELECT i.hk IS NOT NULL AS item_present,
       i.applicable,
       CASE WHEN i.hk IS NULL OR i.data_kind = ${JSON_KIND_CODE} THEN (${plan.valueTypeSql}) ELSE 1 END AS value_type_ok,
       CASE WHEN i.applicable = 1 THEN (${estRowBytesExpr(plan.documentSql, "?1", "?2")}) ELSE NULL END AS new_size,
       i.last_read_ts,
       i.last_write_ts
FROM (SELECT i.*, (${plan.applicableSql}) AS applicable
      FROM requested
      LEFT JOIN items AS i ON i.hk = requested.requested_hk AND i.sk = requested.requested_sk) AS i`;
}

/** The TTL of the stored row. The `ttlSql` of an update write that sets no TTL. */
export const UPDATE_STORED_TTL_SQL = "i.ttl_epoch_utc_seconds";

/**
 * The statement that applies an update plan to one item, and creates the item when it is absent.
 *
 * ?1 and ?2 are the keys. `txOrderTsParam` and `limitParam` are parameters of the statement tail.
 * `ttlSql` is a tail parameter for an operation that sets a TTL, and UPDATE_STORED_TTL_SQL for one
 * that does not: the TTL of the pre-image then survives. The insert carries the TTL of the joined
 * row, which is NULL for an item that this statement creates, and the conflict branch assigns that
 * same value back.
 *
 * The source is a LEFT JOIN over items, so the document expression reads the stored row when there is
 * one and the empty pre-image when there is not, and one statement covers both. The WHERE clause
 * holds the size guard: when it removes the source row, neither branch runs and the statement returns
 * nothing. The one deletion_metadata row gives the start values of a new row.
 */
export function composeUpdateWriteStatement(
	plan: Pick<CompiledUpdatePlan, "documentSql">,
	tail: { txOrderTsParam: string; ttlSql: string; limitParam: string },
): string {
	const { txOrderTsParam, ttlSql, limitParam } = tail;
	const estRowBytes = estRowBytesExpr(plan.documentSql, "?1", "?2");
	return `INSERT INTO items (hk, sk, data_kind, ttl_epoch_utc_seconds, v, last_read_ts, last_write_ts, est_row_bytes, data)
SELECT ?1, ?2, ${JSON_KIND_CODE}, ${ttlSql}, d.max_deleted_v + 1,
       MAX(${txOrderTsParam}, d.max_delete_tx_order_ts), MAX(${txOrderTsParam}, d.max_delete_tx_order_ts),
       ${estRowBytes}, ${plan.documentSql}
  FROM deletion_metadata AS d LEFT JOIN items AS i ON i.hk = ?1 AND i.sk = ?2
 WHERE d.id = 1 AND ${estRowBytes} <= ${limitParam}
ON CONFLICT(hk, sk) DO UPDATE SET
  data = excluded.data,
  data_kind = excluded.data_kind,
  ttl_epoch_utc_seconds = excluded.ttl_epoch_utc_seconds,
  est_row_bytes = excluded.est_row_bytes,
  v = v + 1,
  last_read_ts = MAX(last_read_ts, ${txOrderTsParam}),
  last_write_ts = MAX(last_write_ts, ${txOrderTsParam})
RETURNING v, est_row_bytes`;
}

export function composeProjectionStatement(plan: Pick<CompiledProjectionPlan, "valueSql" | "typeSql">): string {
	const columns = plan.valueSql.flatMap((valueSql, index) => [`${valueSql} AS p${index}`, `${plan.typeSql[index]} AS t${index}`]);
	return `SELECT i.v, i.ttl_epoch_utc_seconds, ${columns.join(", ")}
FROM items AS i
WHERE i.hk = ?2 AND i.sk = ?3
LIMIT 1`;
}

export function composeQueryStatement(
	plan: Pick<CompiledQueryPlan, "filterSql" | "projection">,
	opts: { select: QuerySelect; direction: "asc" | "desc"; scanConditions: readonly string[] },
): string {
	// SQLite numbers an unnumbered ? as one above the largest number seen so far, so a bare ? here
	// would take ?2 only when the pool ?1 precedes it in the text. A filter reads the pool in the
	// select list before the WHERE, a projection reads it only after, so whether it precedes depends
	// on the plan. Every scan parameter is numbered explicitly from ?2 instead.
	let nextParam = POOL_PARAM + 1;
	const conds = opts.scanConditions.map((condition) => condition.replace(/\?/g, () => `?${nextParam++}`)).join(" AND ");
	const limitParam = `?${nextParam}`;
	const order = opts.direction === "asc" ? "ASC" : "DESC";
	const matchedSql = plan.filterSql === null ? "1 AS matched" : `CASE WHEN (${plan.filterSql}) THEN 1 ELSE 0 END AS matched`;
	if (opts.select === "count") {
		return `SELECT sk, est_row_bytes, ${matchedSql}
FROM items AS i
WHERE ${conds}
ORDER BY sk ${order}
LIMIT ${limitParam}`;
	}
	const projection = plan.projection;
	const outerColumns =
		projection === null
			? [
					"CASE WHEN matched THEN i.hk END AS hk",
					`CASE WHEN matched THEN (CASE WHEN i.data_kind = ${JSON_KIND_CODE} THEN json(i.data) ELSE i.data END) END AS data`,
					"CASE WHEN matched THEN i.data_kind END AS data_kind",
					"CASE WHEN matched THEN i.ttl_epoch_utc_seconds END AS ttl_epoch_utc_seconds",
					"CASE WHEN matched THEN i.v END AS v",
					"CASE WHEN matched THEN i.last_read_ts END AS last_read_ts",
					"CASE WHEN matched THEN i.last_write_ts END AS last_write_ts",
				]
			: projection.valueSql.flatMap((valueSql, index) => [
					`CASE WHEN matched THEN ${valueSql} END AS p${index}`,
					`CASE WHEN matched THEN ${projection.typeSql[index]} END AS t${index}`,
				]);
	return `WITH candidates AS (
SELECT hk, sk, est_row_bytes, v, ttl_epoch_utc_seconds, data_kind, data, last_read_ts, last_write_ts, ${matchedSql}
FROM items AS i
WHERE ${conds}
ORDER BY sk ${order}
LIMIT ${limitParam})
SELECT sk, est_row_bytes, matched, ${outerColumns.join(", ")}
FROM candidates AS i
ORDER BY sk ${order}`;
}

export type ExpressionBindingDescriptor =
	| { kind: "val"; value: JsonPrimitive }
	| { kind: "keyText"; value: string }
	| { kind: "keyB64"; value: string }
	| { kind: "b64"; value: string }
	| { kind: "path"; value: string };

/**
 * How a plan's value descriptors reach the statement. "direct" binds one parameter per descriptor;
 * "pool" binds one JSON array parameter and each descriptor reads its element with json_extract.
 */
export type ExpressionBindingLayout = "direct" | "pool";

/**
 * Compiled SQL plan for evaluating a write condition expression.
 * The plan lives for one request in the node that compiled it. No request and no table carries it.
 */
export type CompiledConditionPlan = {
	/** Plan schema version number. */
	version: typeof CONDITION_PLAN_VERSION;
	/** Discriminant for condition expression plans. */
	kind: "condition";
	/** Compiled SQL predicate expression evaluating the condition. */
	sql: string;
	/** Descriptors for expression values bound to SQL statement parameters. */
	bindings: readonly ExpressionBindingDescriptor[];
	/** Number of parameter bindings in this plan. */
	bindingCount: number;
	/** Total bindings including fixed statement parameters (requested_hk, requested_sk). */
	completeBindingCount: number;
	/** Storage columns required to execute the condition statement. */
	requiredColumns: readonly ExpressionRequiredColumn[];
	/** Item data dependencies needed by the condition. */
	dataDependencies: {
		/** True if the condition accesses the complete data column rather than specific paths. */
		completeData: boolean;
		/** List of distinct JSON paths accessed in item data. */
		paths: readonly string[];
	};
	/** Static type analysis guarantees for the condition result. */
	result: {
		nativeTypes: readonly ["boolean"];
		canBeMissing: false;
	};
};

/**
 * Compiled SQL plan for applying an update expression to a JSON item.
 * The plan lives for one request in the node that compiled it. No request and no table carries it.
 */
export type CompiledUpdatePlan = {
	/** Plan schema version number. */
	version: typeof UPDATE_PLAN_VERSION;
	/** Discriminant for update expression plans. */
	kind: "update";
	/**
	 * SQL expression computing the complete new JSONB document from stored `i.data`.
	 * Actions wrap accumulator starting at `i.data`; values evaluate against pre-image `i.data`.
	 */
	documentSql: string;
	/**
	 * SQL boolean expression checking if the update can apply to the target item.
	 * Evaluates to 1 when item exists, data is JSON, targets exist, operands are present,
	 * and result remains a valid document. Evaluates to 0 otherwise.
	 */
	applicableSql: string;
	/**
	 * SQL boolean expression checking that every `set` value has a type a JSON document can hold.
	 * Evaluates to 1 when no value is bytes, and to 0 when one is — a key reference over a binary
	 * key, or a SQLite function that returned a blob for this item.
	 *
	 * It is also a term of `applicableSql`, so it never decides on its own whether an update applies.
	 * It exists so the probe can separate this ONE cause from the rest and report it to the caller,
	 * which a single applicability bit cannot do. `1` when no value needs the test.
	 */
	valueTypeSql: string;
	/**
	 * The statement of `composeUpdateProbeStatement` for this plan. The compile composes it to count
	 * its bytes, and the count makes its text flat. The probe runs this same text, so that SQLite gets
	 * it with no second copy.
	 */
	probeSql: string;
	/** Descriptors for expression values bound to SQL statement parameters. */
	bindings: readonly ExpressionBindingDescriptor[];
	/** Number of parameter bindings in this plan. */
	bindingCount: number;
	/**
	 * `UPDATE_FIXED_BINDING_COUNT + bindingCount`: the keys plus the plan's own parameters, which
	 * together occupy `?1` to here. A statement appends its own tail starting at the next number.
	 */
	completeBindingCount: number;
	/** Storage columns required to execute the update statement. */
	requiredColumns: readonly ExpressionRequiredColumn[];
	/** Item data dependencies needed by the update. */
	dataDependencies: {
		/** True if the update accesses the complete data column rather than specific paths. */
		completeData: boolean;
		/** List of distinct JSON paths modified or accessed in item data. */
		paths: readonly string[];
	};
};

/**
 * Compiled SQL plan for a projected point read.
 * The plan lives for one request in the node that compiled it. No request and no table carries it.
 */
export type CompiledProjectionPlan = {
	/** Plan schema version number. */
	version: typeof PROJECTION_PLAN_VERSION;
	/** Discriminant for projection plans. */
	kind: "projection";
	/** Descriptors bind as one JSON array parameter, never as one parameter each. */
	bindingLayout: "pool";
	/** Resolved output names, in entry order. */
	names: readonly string[];
	/** One value SQL fragment per entry, over alias `i`. */
	valueSql: readonly string[];
	/** One type SQL fragment per entry, over alias `i`. */
	typeSql: readonly string[];
	/** Descriptors for expression values bound through the pool parameter. */
	bindings: readonly ExpressionBindingDescriptor[];
	/** Number of binding descriptors in this plan. */
	bindingCount: number;
	/** POOL_PARAM plus PROJECTION_FIXED_BINDING_COUNT (2: hk, sk); the pool is bound also when the plan has no descriptor. */
	completeBindingCount: number;
	/** Storage columns required to execute the projection statement. */
	requiredColumns: readonly ExpressionRequiredColumn[];
	/** Item data dependencies needed by the projection. */
	dataDependencies: {
		/** True if the projection accesses the complete data column rather than specific paths. */
		completeData: boolean;
		/** List of distinct JSON paths accessed in item data. */
		paths: readonly string[];
	};
};

/**
 * Compiled SQL plan for one queryItems leaf scan with a filter, a projection, or both.
 * The plan lives for one request in the node that compiled it. No request and no table carries it.
 */
export type CompiledQueryPlan = {
	/** Plan schema version number. */
	version: typeof QUERY_PLAN_VERSION;
	/** Discriminant for query plans. */
	kind: "query";
	/** Descriptors bind as one JSON array parameter, never as one parameter each. */
	bindingLayout: "pool";
	/** The predicate over alias `i`, or null when the request has no filter. */
	filterSql: string | null;
	/** The projection fragments, or null when the request returns complete items. */
	projection: { names: readonly string[]; valueSql: readonly string[]; typeSql: readonly string[] } | null;
	/** Descriptors for expression values bound through the pool parameter. */
	bindings: readonly ExpressionBindingDescriptor[];
	/** Number of binding descriptors in this plan. */
	bindingCount: number;
	/** POOL_PARAM plus QUERY_MAX_TRAILING_BINDING_COUNT: the pool plus the widest scan tail. */
	completeBindingCount: number;
	/** Storage columns required to execute the query statement. */
	requiredColumns: readonly ExpressionRequiredColumn[];
	/** Item data dependencies needed by the filter and the projection. */
	dataDependencies: {
		/** True if the plan accesses the complete data column rather than specific paths. */
		completeData: boolean;
		/** List of distinct JSON paths accessed in item data. */
		paths: readonly string[];
	};
};

/**
 * Throws when a request carries a compiled plan in place of an expression tree. A partition runs only
 * the SQL that it compiled from a tree. Only a plan has a `kind` field and a `version` field.
 */
export function refuseCompiledPlan(expression: object): void {
	if ("kind" in expression && "version" in expression) {
		throw new FokosValidationError(VALIDATION_CODES.compiled_plan_refused, {
			message: "a request must carry an expression tree, not a compiled plan",
		});
	}
}
