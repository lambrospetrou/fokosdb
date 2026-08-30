import { KeyCodec, type KeyBytes } from "../partition-topology/key-codec.js";
import { ExpressionError } from "./errors.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import {
	composeConditionStatement,
	CONDITION_FIXED_BINDING_COUNT,
	CONDITION_PLAN_VERSION,
	type CompiledConditionPlan,
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

export function materializeExpressionBindings(descriptors: readonly ExpressionBindingDescriptor[]): unknown[] {
	return descriptors.map((descriptor) => {
		switch (descriptor.kind) {
			case "val":
				return typeof descriptor.value === "boolean" ? Number(descriptor.value) : descriptor.value;
			case "path":
				return descriptor.value;
			case "keyText":
				return KeyCodec.encode(descriptor.value);
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
