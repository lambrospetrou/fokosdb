import { type KeyBytes } from "../partition-topology/key-codec.js";
import { materializedPlanBindings } from "./bindings.js";
import { ExpressionError } from "./errors.js";
import { decodeProjectedRow, type ProjectedWireRow } from "./projection.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import { estRowBytesExpr, JSON_KIND_CODE } from "../partition/item-size.js";
import { tryOne } from "../sql-cursor.js";
import {
	composeConditionStatement,
	composeProjectionStatement,
	composeQueryStatement,
	CONDITION_FIXED_BINDING_COUNT,
	CONDITION_PLAN_VERSION,
	POOL_PARAM,
	PROJECTION_FIXED_BINDING_COUNT,
	PROJECTION_PLAN_VERSION,
	QUERY_MAX_TRAILING_BINDING_COUNT,
	QUERY_PLAN_VERSION,
	QUERY_WIDEST_SCAN_CONDITIONS,
	UPDATE_FIXED_BINDING_COUNT,
	UPDATE_MAX_TRAILING_BINDING_COUNT,
	UPDATE_PLAN_VERSION,
	type CompiledConditionPlan,
	type CompiledProjectionPlan,
	type CompiledQueryPlan,
	type CompiledUpdatePlan,
} from "./plan.js";
import { utf8WithinLimit } from "./utf8.js";

export type ConditionEvaluationResult = {
	itemPresent: boolean;
	conditionOk: boolean;
	lastReadTs: number | null;
	lastWriteTs: number | null;
	rowsRead: number;
	rowsWritten: number;
};

export type ProjectedReadResult = {
	row?: { projected: ProjectedWireRow; version: number; ttlAt?: number };
	rowsRead: number;
	rowsWritten: number;
};

export type UpdateProbeResult = {
	itemPresent: boolean;
	applicable: boolean;
	/** False when a `set` value evaluated to bytes for this item, which a JSON document cannot hold. */
	valueTypeOk: boolean;
	newSize: number | null;
	lastReadTs: number | null;
	lastWriteTs: number | null;
	rowsRead: number;
	rowsWritten: number;
};

export function evaluateConditionPlan(
	storage: DurableObjectStorage,
	plan: CompiledConditionPlan,
	hashKey: KeyBytes,
	sortKey: KeyBytes,
): ConditionEvaluationResult {
	const statement = validateConditionPlan(plan);
	if (plan.completeBindingCount !== plan.bindingCount + CONDITION_FIXED_BINDING_COUNT) {
		throw new ExpressionError("sql_limit", "condition plan has an invalid complete binding count");
	}
	try {
		const cursor = storage.sql.exec<{
			item_present: number;
			condition_ok: number;
			last_read_ts: number | null;
			last_write_ts: number | null;
		}>(statement, hashKey, sortKey, ...materializedPlanBindings(plan));
		const row = cursor.one();
		return {
			itemPresent: row.item_present === 1,
			conditionOk: row.condition_ok === 1,
			lastReadTs: row.last_read_ts,
			lastWriteTs: row.last_write_ts,
			rowsRead: cursor.rowsRead,
			rowsWritten: cursor.rowsWritten,
		};
	} catch (error) {
		if (error instanceof ExpressionError) throw error;
		throw new ExpressionError("runtime_capability", "Workers SQLite could not evaluate the compiled expression", { cause: error });
	}
}

/**
 * Runs a projected point read: one row of the items table through the plan's value and type
 * columns. The pool is `?1`, `hk` is `?2`, and `sk` is `?3` of the composed statement. An absent row
 * is a `found: false` read. A projection has nothing to return for one, so no LEFT JOIN is needed.
 */
export function readProjectedItem(
	storage: DurableObjectStorage,
	plan: CompiledProjectionPlan,
	hashKey: KeyBytes,
	sortKey: KeyBytes,
): ProjectedReadResult {
	const statement = validateProjectionPlan(plan);
	try {
		const cursor = storage.sql.exec<Record<string, SqlStorageValue>>(
			statement,
			...materializedPlanBindings(plan, "pool"),
			hashKey,
			sortKey,
		);
		const row = tryOne(cursor);
		if (row === undefined) return { row: undefined, rowsRead: cursor.rowsRead, rowsWritten: cursor.rowsWritten };
		return {
			row: {
				projected: decodeProjectedRow(row, plan.names.length),
				version: row.v as number,
				ttlAt: (row.ttl_epoch_utc_seconds as number | null) ?? undefined,
			},
			rowsRead: cursor.rowsRead,
			rowsWritten: cursor.rowsWritten,
		};
	} catch (error) {
		if (error instanceof ExpressionError) throw error;
		throw new ExpressionError("runtime_capability", "Workers SQLite could not evaluate the compiled expression", { cause: error });
	}
}

/** Validates the plan and returns the composed statement so the caller does not compose it again. */
export function validateConditionPlan(plan: CompiledConditionPlan): string {
	if (plan.version !== CONDITION_PLAN_VERSION || plan.kind !== "condition") {
		throw new ExpressionError("runtime_capability", "unsupported condition plan version or kind");
	}
	const statement = composeConditionStatement(plan.sql);
	if (!utf8WithinLimit(statement, EXPRESSION_LIMITS.compiledSqlBytes)) {
		throw new ExpressionError("sql_limit", "compiled SQL exceeds the SQL limit");
	}
	if (plan.bindings.length !== plan.bindingCount || plan.completeBindingCount > EXPRESSION_LIMITS.completeStatementBindings) {
		throw new ExpressionError("sql_limit", "condition plan has an invalid binding count");
	}
	return statement;
}

function assertProjectionShape(projection: { names: readonly string[]; valueSql: readonly string[]; typeSql: readonly string[] }): void {
	if (
		projection.names.length !== projection.valueSql.length ||
		projection.names.length !== projection.typeSql.length ||
		projection.names.length < 1 ||
		projection.names.length > EXPRESSION_LIMITS.projectionEntries
	) {
		throw new ExpressionError("runtime_capability", "projection plan has an invalid shape");
	}
}

/** Validates the plan and returns the composed statement so the caller does not compose it again. */
export function validateProjectionPlan(plan: CompiledProjectionPlan): string {
	if (plan.version !== PROJECTION_PLAN_VERSION || plan.kind !== "projection") {
		throw new ExpressionError("runtime_capability", "unsupported projection plan version or kind");
	}
	if (plan.bindingLayout !== "pool") {
		throw new ExpressionError("runtime_capability", "unsupported projection plan binding layout");
	}
	assertProjectionShape(plan);
	const statement = composeProjectionStatement(plan);
	if (!utf8WithinLimit(statement, EXPRESSION_LIMITS.compiledSqlBytes)) {
		throw new ExpressionError("sql_limit", "compiled SQL exceeds the SQL limit");
	}
	if (
		plan.bindings.length !== plan.bindingCount ||
		plan.completeBindingCount !== POOL_PARAM + PROJECTION_FIXED_BINDING_COUNT ||
		plan.completeBindingCount > EXPRESSION_LIMITS.completeStatementBindings
	) {
		throw new ExpressionError("sql_limit", "projection plan has an invalid binding count");
	}
	return statement;
}

export function validateQueryPlan(plan: CompiledQueryPlan): void {
	if (plan.version !== QUERY_PLAN_VERSION || plan.kind !== "query") {
		throw new ExpressionError("runtime_capability", "unsupported query plan version or kind");
	}
	if (plan.bindingLayout !== "pool") {
		throw new ExpressionError("runtime_capability", "unsupported query plan binding layout");
	}
	if (plan.filterSql === null && plan.projection === null) {
		throw new ExpressionError("runtime_capability", "query plan has neither a filter nor a projection");
	}
	if (plan.projection !== null) assertProjectionShape(plan.projection);
	const widest = composeQueryStatement(plan, {
		select: "projection",
		direction: "desc",
		scanConditions: QUERY_WIDEST_SCAN_CONDITIONS,
	});
	if (!utf8WithinLimit(widest, EXPRESSION_LIMITS.compiledSqlBytes)) {
		throw new ExpressionError("sql_limit", "compiled SQL exceeds the SQL limit");
	}
	if (
		plan.bindings.length !== plan.bindingCount ||
		plan.completeBindingCount !== POOL_PARAM + QUERY_MAX_TRAILING_BINDING_COUNT ||
		plan.completeBindingCount > EXPRESSION_LIMITS.completeStatementBindings
	) {
		throw new ExpressionError("sql_limit", "query plan has an invalid binding count");
	}
}

export function validateUpdatePlan(plan: CompiledUpdatePlan): void {
	if (plan.version !== UPDATE_PLAN_VERSION || plan.kind !== "update") {
		throw new ExpressionError("runtime_capability", "unsupported update plan version or kind");
	}
	if (
		!utf8WithinLimit(plan.documentSql, EXPRESSION_LIMITS.compiledSqlBytes) ||
		!utf8WithinLimit(plan.applicableSql, EXPRESSION_LIMITS.compiledSqlBytes) ||
		!utf8WithinLimit(plan.valueTypeSql, EXPRESSION_LIMITS.compiledSqlBytes)
	) {
		throw new ExpressionError("sql_limit", "compiled SQL exceeds the SQL limit");
	}
	if (plan.bindings.length !== plan.bindingCount || plan.completeBindingCount !== UPDATE_FIXED_BINDING_COUNT + plan.bindingCount) {
		throw new ExpressionError("sql_limit", "update plan has an invalid binding count");
	}
	// The compiler charged the widest tail to the plan; re-check it here, because the plan crossed the
	// wire and a statement that binds its tail past the cap fails with no useful error.
	if (plan.completeBindingCount + UPDATE_MAX_TRAILING_BINDING_COUNT > EXPRESSION_LIMITS.completeStatementBindings) {
		throw new ExpressionError("sql_limit", "update plan exceeds the complete statement binding limit");
	}
}

export function composeUpdateProbeStatement(plan: CompiledUpdatePlan): string {
	// ?1 and ?2 are the keys, as they are in every statement that runs an update plan.
	const hkParam = "?1";
	const skParam = "?2";
	// value_type_ok names ONE cause of an inapplicable update, so a caller learns that its value was
	// bytes for this item instead of only that the update did not apply. It runs over a JSON pre-image
	// only: the fragment can read a JSON path, and json_type over a text or bytes row raises. An absent
	// row has a JSON pre-image, because an update creates the item.
	return `WITH requested(requested_hk, requested_sk) AS (VALUES (${hkParam}, ${skParam}))
SELECT i.hk IS NOT NULL AS item_present,
       (${plan.applicableSql}) AS applicable,
       CASE WHEN i.hk IS NULL OR i.data_kind = ${JSON_KIND_CODE} THEN (${plan.valueTypeSql}) ELSE 1 END AS value_type_ok,
       CASE WHEN (${plan.applicableSql}) = 1 THEN (${estRowBytesExpr(plan.documentSql, hkParam, skParam)}) ELSE NULL END AS new_size,
       i.last_read_ts,
       i.last_write_ts
FROM requested
LEFT JOIN items AS i ON i.hk = requested.requested_hk AND i.sk = requested.requested_sk`;
}

export function probeUpdatePlan(
	storage: DurableObjectStorage,
	plan: CompiledUpdatePlan,
	hashKey: KeyBytes,
	sortKey: KeyBytes,
): UpdateProbeResult {
	validateUpdatePlan(plan);
	const statement = composeUpdateProbeStatement(plan);
	try {
		const cursor = storage.sql.exec<{
			item_present: number;
			applicable: number;
			value_type_ok: number;
			new_size: number | null;
			last_read_ts: number | null;
			last_write_ts: number | null;
		}>(statement, hashKey, sortKey, ...materializedPlanBindings(plan));
		const row = cursor.one();
		return {
			itemPresent: row.item_present === 1,
			applicable: row.applicable === 1,
			valueTypeOk: row.value_type_ok === 1,
			newSize: row.new_size,
			lastReadTs: row.last_read_ts,
			lastWriteTs: row.last_write_ts,
			rowsRead: cursor.rowsRead,
			rowsWritten: cursor.rowsWritten,
		};
	} catch (error) {
		if (error instanceof ExpressionError) throw error;
		throw new ExpressionError("runtime_capability", "Workers SQLite could not evaluate the compiled expression", { cause: error });
	}
}
