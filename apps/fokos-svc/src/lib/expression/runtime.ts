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

const textEncoder = new TextEncoder();

export type ConditionEvaluationResult = {
	itemPresent: boolean;
	conditionOk: boolean;
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
	validateConditionPlan(plan);
	if (plan.completeBindingCount !== plan.bindingCount + CONDITION_FIXED_BINDING_COUNT) {
		throw new ExpressionError("sql_limit", "condition plan has an invalid complete binding count");
	}
	const statement = composeConditionStatement(plan.sql);
	try {
		const cursor = storage.sql.exec<{ item_present: number; condition_ok: number }>(
			statement,
			hashKey,
			sortKey,
			...materializeExpressionBindings(plan.bindings),
		);
		const row = cursor.one();
		return {
			itemPresent: row.item_present === 1,
			conditionOk: row.condition_ok === 1,
			rowsRead: cursor.rowsRead,
			rowsWritten: cursor.rowsWritten,
		};
	} catch (error) {
		if (error instanceof ExpressionError) throw error;
		throw new ExpressionError("runtime_capability", "Workers SQLite could not evaluate the compiled expression");
	}
}

export function validateConditionPlan(plan: CompiledConditionPlan): void {
	if (plan.version !== CONDITION_PLAN_VERSION || plan.kind !== "condition") {
		throw new ExpressionError("runtime_capability", "unsupported condition plan version or kind");
	}
	if (textEncoder.encode(composeConditionStatement(plan.sql)).byteLength > EXPRESSION_LIMITS.compiledSqlBytes) {
		throw new ExpressionError("sql_limit", "compiled SQL exceeds the SQL limit");
	}
	if (plan.bindings.length !== plan.bindingCount || plan.completeBindingCount > EXPRESSION_LIMITS.completeStatementBindings) {
		throw new ExpressionError("sql_limit", "condition plan has an invalid binding count");
	}
}
