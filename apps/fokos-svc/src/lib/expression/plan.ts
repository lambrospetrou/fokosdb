import type { JsonPrimitive } from "../json-types.js";
import type { ExpressionRequiredColumn } from "./semantic.js";

export const CONDITION_PLAN_VERSION = 1 as const;
export const CONDITION_FIXED_BINDING_COUNT = 2;

export function composeConditionStatement(predicateSql: string): string {
	return `WITH requested(requested_hk, requested_sk) AS (VALUES (?, ?))
SELECT i.hk IS NOT NULL AS item_present, CASE WHEN (${predicateSql}) THEN 1 ELSE 0 END AS condition_ok
FROM requested
LEFT JOIN items AS i ON i.hk = requested.requested_hk AND i.sk = requested.requested_sk`;
}

export type ExpressionBindingDescriptor =
	| { kind: "val"; value: JsonPrimitive }
	| { kind: "keyText"; value: string }
	| { kind: "path"; value: string };

export type CompiledConditionPlan = {
	version: typeof CONDITION_PLAN_VERSION;
	kind: "condition";
	sql: string;
	bindings: readonly ExpressionBindingDescriptor[];
	bindingCount: number;
	completeBindingCount: number;
	requiredColumns: readonly ExpressionRequiredColumn[];
	dataDependencies: {
		completeData: boolean;
		paths: readonly string[];
	};
	result: {
		nativeTypes: readonly ["boolean"];
		canBeMissing: false;
	};
	identity: string;
};
