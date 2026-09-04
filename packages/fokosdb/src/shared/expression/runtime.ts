import { KeyCodec, type KeyBytes } from "../partition-topology/key-codec.js";
import { decodeBase64Bytes } from "./byte-literal.js";
import { ExpressionError } from "./errors.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import { estRowBytesExpr, JSON_KIND_CODE } from "../partition/item-size.js";
import {
	composeConditionStatement,
	CONDITION_FIXED_BINDING_COUNT,
	CONDITION_PLAN_VERSION,
	UPDATE_FIXED_BINDING_COUNT,
	UPDATE_MAX_TRAILING_BINDING_COUNT,
	UPDATE_PLAN_VERSION,
	type CompiledConditionPlan,
	type CompiledUpdatePlan,
	type ExpressionBindingDescriptor,
} from "./plan.js";
import { utf8WithinLimit } from "./utf8.js";

export type ConditionEvaluationResult = {
	itemPresent: boolean;
	conditionOk: boolean;
	lastTransactionTs: number | null;
	rowsRead: number;
	rowsWritten: number;
};

export type UpdateProbeResult = {
	itemPresent: boolean;
	applicable: boolean;
	/** False when a `set` value evaluated to bytes for this item, which a JSON document cannot hold. */
	valueTypeOk: boolean;
	newSize: number | null;
	lastTransactionTs: number | null;
	rowsRead: number;
	rowsWritten: number;
};

export function materializeExpressionBindings(descriptors: readonly ExpressionBindingDescriptor[]): unknown[] {
	return descriptors.map((descriptor) => {
		switch (descriptor.kind) {
			case "val":
				return typeof descriptor.value === "boolean" ? Number(descriptor.value) : descriptor.value;
			case "path":
				return descriptor.value;
			case "keyText":
				return KeyCodec.encode(descriptor.value);
			case "keyB64":
				return KeyCodec.encode(decodeBase64Bytes(descriptor.value));
			case "b64":
				return decodeBase64Bytes(descriptor.value);
		}
	});
}

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
		const cursor = storage.sql.exec<{ item_present: number; condition_ok: number; last_transaction_ts: number | null }>(
			statement,
			hashKey,
			sortKey,
			...materializeExpressionBindings(plan.bindings),
		);
		const row = cursor.one();
		return {
			itemPresent: row.item_present === 1,
			conditionOk: row.condition_ok === 1,
			lastTransactionTs: row.last_transaction_ts,
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
	// bytes for this item instead of only that the update did not apply. It runs on a json row only:
	// the fragment can read a JSON path, and json_type over a text or bytes row raises.
	return `WITH requested(requested_hk, requested_sk) AS (VALUES (${hkParam}, ${skParam}))
SELECT i.hk IS NOT NULL AS item_present,
       (${plan.applicableSql}) AS applicable,
       CASE WHEN i.hk IS NOT NULL AND i.data_kind = ${JSON_KIND_CODE} THEN (${plan.valueTypeSql}) ELSE 1 END AS value_type_ok,
       CASE WHEN (${plan.applicableSql}) = 1 THEN (${estRowBytesExpr(plan.documentSql, hkParam, skParam)}) ELSE NULL END AS new_size,
       i.last_transaction_ts
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
			last_transaction_ts: number | null;
		}>(statement, hashKey, sortKey, ...materializeExpressionBindings(plan.bindings));
		const row = cursor.one();
		return {
			itemPresent: row.item_present === 1,
			applicable: row.applicable === 1,
			valueTypeOk: row.value_type_ok === 1,
			newSize: row.new_size,
			lastTransactionTs: row.last_transaction_ts,
			rowsRead: cursor.rowsRead,
			rowsWritten: cursor.rowsWritten,
		};
	} catch (error) {
		if (error instanceof ExpressionError) throw error;
		throw new ExpressionError("runtime_capability", "Workers SQLite could not evaluate the compiled expression", { cause: error });
	}
}
