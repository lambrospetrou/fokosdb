import type { JsonValue } from "../json-types.js";

export const EXPRESSION_NATIVE_TYPES = ["missing", "null", "boolean", "number", "text", "bytes", "array", "object"] as const;

export type ExpressionNativeType = (typeof EXPRESSION_NATIVE_TYPES)[number];

export type ExpressionReference =
	| { ref: "hashKey" }
	| { ref: "sortKey" }
	| { ref: "v" }
	| { ref: "ttlAt" }
	| { ref: "data"; path?: string };

export type ExpressionValue = { val: JsonValue } | { b64: string } | ExpressionReference | { fn: string; args: readonly ExpressionValue[] };

export type ConditionExpression =
	| {
			op: "eq" | "ne" | "lt" | "lte" | "gt" | "gte";
			args: readonly [left: ExpressionValue, right: ExpressionValue];
	  }
	| {
			op: "between";
			args: readonly [value: ExpressionValue, lower: ExpressionValue, upper: ExpressionValue];
	  }
	| {
			op: "in";
			args: readonly [value: ExpressionValue, choice: ExpressionValue, ...choices: ExpressionValue[]];
	  }
	| {
			op: "and" | "or";
			args: readonly [ConditionExpression, ConditionExpression, ...ConditionExpression[]];
	  }
	| { op: "not"; args: readonly [condition: ConditionExpression] }
	| { op: "exists" | "not_exists"; args: readonly [reference: ExpressionReference] }
	| {
			op: "begins_with";
			args: readonly [value: ExpressionValue, prefix: ExpressionValue];
	  }
	| {
			op: "contains";
			args: readonly [value: ExpressionValue, search: ExpressionValue];
	  };

export type ProjectionExpression = {
	expr: ExpressionValue;
	as?: string;
};

export type UpdateTarget = { ref: "data"; path: string };

export type UpdateAction = { action: "set"; target: UpdateTarget; value: ExpressionValue } | { action: "remove"; target: UpdateTarget };

export type UpdateExpression = readonly UpdateAction[];
