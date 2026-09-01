import type { JsonValue } from "../json-types.js";
import type { DataKind } from "../types.js";
import type { ExpressionLimitName } from "./limits.js";
import type { ConditionExpression, ProjectionExpression } from "./types.js";

export type ExpressionSemanticItem = {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;
	v: number;
	ttlAt?: number;
	data: string | Uint8Array | JsonValue;
	kind: DataKind;
};

export type ConditionSemanticFixture = {
	name: string;
	item: ExpressionSemanticItem | null;
	condition: ConditionExpression;
	expected: boolean;
};

export type ProjectionPresenceFixture = {
	name: string;
	item: ExpressionSemanticItem;
	projection: ProjectionExpression;
	expected: { present: false } | { present: true; value: JsonValue | Uint8Array };
};

export type ExpressionLimitFixture = {
	name: ExpressionLimitName;
	atLimit: number;
	aboveLimit: number;
};

const jsonItem = (data: JsonValue): ExpressionSemanticItem => ({ hashKey: "hk", v: 1, data, kind: "json" });
const textItem: ExpressionSemanticItem = { hashKey: "hk", v: 1, data: "text", kind: "text" };
const bytesItem: ExpressionSemanticItem = { hashKey: new Uint8Array([1]), v: 1, data: new Uint8Array([2]), kind: "bytes" };

export const MISSING_NULL_SEMANTIC_FIXTURES = [
	{
		name: "exists is false for a missing item",
		item: null,
		condition: { op: "exists", args: [{ ref: "hashKey" }] },
		expected: false,
	},
	{
		name: "not_exists is true for a missing item",
		item: null,
		condition: { op: "not_exists", args: [{ ref: "hashKey" }] },
		expected: true,
	},
	{
		name: "null does not equal a missing item reference",
		item: null,
		condition: { op: "eq", args: [{ ref: "data" }, { val: null }] },
		expected: false,
	},
	{
		name: "an absent sort key is missing",
		item: textItem,
		condition: { op: "not_exists", args: [{ ref: "sortKey" }] },
		expected: true,
	},
	{
		name: "an absent TTL is missing",
		item: textItem,
		condition: { op: "not_exists", args: [{ ref: "ttlAt" }] },
		expected: true,
	},
	{
		name: "a JSON path on text data is missing",
		item: textItem,
		condition: { op: "not_exists", args: [{ ref: "data", path: "$.value" }] },
		expected: true,
	},
	{
		name: "a JSON path on byte data is missing",
		item: bytesItem,
		condition: { op: "not_exists", args: [{ ref: "data", path: "$.value" }] },
		expected: true,
	},
	{
		name: "exists is true for JSON null",
		item: jsonItem({ value: null }),
		condition: { op: "exists", args: [{ ref: "data", path: "$.value" }] },
		expected: true,
	},
	{
		name: "not_exists is false for JSON null",
		item: jsonItem({ value: null }),
		condition: { op: "not_exists", args: [{ ref: "data", path: "$.value" }] },
		expected: false,
	},
	{
		name: "JSON null equals null",
		item: jsonItem({ value: null }),
		condition: { op: "eq", args: [{ ref: "data", path: "$.value" }, { val: null }] },
		expected: true,
	},
	{
		name: "exists is false for a missing JSON path",
		item: jsonItem({}),
		condition: { op: "exists", args: [{ ref: "data", path: "$.value" }] },
		expected: false,
	},
	{
		name: "not_exists is true for a missing JSON path",
		item: jsonItem({}),
		condition: { op: "not_exists", args: [{ ref: "data", path: "$.value" }] },
		expected: true,
	},
	{
		name: "null does not equal a missing JSON path",
		item: jsonItem({}),
		condition: { op: "eq", args: [{ ref: "data", path: "$.value" }, { val: null }] },
		expected: false,
	},
] as const satisfies readonly ConditionSemanticFixture[];

export const PROJECTION_PRESENCE_FIXTURES = [
	{
		name: "a direct projection omits a missing path",
		item: jsonItem({}),
		projection: { expr: { ref: "data", path: "$.value" } },
		expected: { present: false },
	},
	{
		name: "a direct projection includes JSON null",
		item: jsonItem({ value: null }),
		projection: { expr: { ref: "data", path: "$.value" } },
		expected: { present: true, value: null },
	},
] as const satisfies readonly ProjectionPresenceFixture[];

export const VALID_CONDITION_SHAPE_FIXTURES = [
	{ name: "eq", condition: { op: "eq", args: [{ ref: "v" }, { val: 1 }] } },
	{ name: "ne", condition: { op: "ne", args: [{ ref: "v" }, { val: 1 }] } },
	{ name: "lt", condition: { op: "lt", args: [{ ref: "v" }, { val: 1 }] } },
	{ name: "lte", condition: { op: "lte", args: [{ ref: "v" }, { val: 1 }] } },
	{ name: "gt", condition: { op: "gt", args: [{ ref: "v" }, { val: 1 }] } },
	{ name: "gte", condition: { op: "gte", args: [{ ref: "v" }, { val: 1 }] } },
	{ name: "between", condition: { op: "between", args: [{ ref: "v" }, { val: 1 }, { val: 3 }] } },
	{ name: "in with one choice", condition: { op: "in", args: [{ ref: "v" }, { val: 1 }] } },
	{
		name: "and with two conditions",
		condition: {
			op: "and",
			args: [
				{ op: "gte", args: [{ ref: "v" }, { val: 1 }] },
				{ op: "lte", args: [{ ref: "v" }, { val: 3 }] },
			],
		},
	},
	{
		name: "or with two conditions",
		condition: {
			op: "or",
			args: [
				{ op: "eq", args: [{ ref: "v" }, { val: 1 }] },
				{ op: "eq", args: [{ ref: "v" }, { val: 2 }] },
			],
		},
	},
	{ name: "not", condition: { op: "not", args: [{ op: "exists", args: [{ ref: "ttlAt" }] }] } },
	{ name: "exists", condition: { op: "exists", args: [{ ref: "data", path: "$.value" }] } },
	{ name: "not_exists", condition: { op: "not_exists", args: [{ ref: "sortKey" }] } },
	{ name: "begins_with", condition: { op: "begins_with", args: [{ ref: "data" }, { val: "prefix" }] } },
	{ name: "contains", condition: { op: "contains", args: [{ ref: "data", path: "$.tags" }, { val: "tag" }] } },
] as const satisfies readonly { name: string; condition: ConditionExpression }[];

export const INVALID_CONDITION_SHAPE_FIXTURES = [
	{ name: "null root", condition: null },
	{ name: "unknown operator", condition: { op: "unknown", args: [] } },
	{ name: "unknown condition field", condition: { op: "exists", args: [{ ref: "v" }], extra: true } },
	{ name: "comparison with one argument", condition: { op: "eq", args: [{ ref: "v" }] } },
	{ name: "comparison with three arguments", condition: { op: "eq", args: [{ ref: "v" }, { val: 1 }, { val: 2 }] } },
	{ name: "between with two arguments", condition: { op: "between", args: [{ ref: "v" }, { val: 1 }] } },
	{ name: "between with four arguments", condition: { op: "between", args: [{ ref: "v" }, { val: 1 }, { val: 2 }, { val: 3 }] } },
	{ name: "in without a choice", condition: { op: "in", args: [{ ref: "v" }] } },
	{ name: "and with no conditions", condition: { op: "and", args: [] } },
	{ name: "and with one condition", condition: { op: "and", args: [{ op: "exists", args: [{ ref: "v" }] }] } },
	{ name: "or with no conditions", condition: { op: "or", args: [] } },
	{ name: "or with one condition", condition: { op: "or", args: [{ op: "exists", args: [{ ref: "v" }] }] } },
	{ name: "not with no condition", condition: { op: "not", args: [] } },
	{
		name: "not with two conditions",
		condition: {
			op: "not",
			args: [
				{ op: "exists", args: [{ ref: "v" }] },
				{ op: "exists", args: [{ ref: "ttlAt" }] },
			],
		},
	},
	{ name: "exists with no reference", condition: { op: "exists", args: [] } },
	{ name: "exists with two references", condition: { op: "exists", args: [{ ref: "v" }, { ref: "ttlAt" }] } },
	{ name: "not_exists with no reference", condition: { op: "not_exists", args: [] } },
	{ name: "not_exists with two references", condition: { op: "not_exists", args: [{ ref: "v" }, { ref: "ttlAt" }] } },
	{ name: "exists with a value", condition: { op: "exists", args: [{ val: 1 }] } },
	{ name: "begins_with with one argument", condition: { op: "begins_with", args: [{ ref: "data" }] } },
	{ name: "begins_with with three arguments", condition: { op: "begins_with", args: [{ ref: "data" }, { val: "a" }, { val: "b" }] } },
	{ name: "contains with one argument", condition: { op: "contains", args: [{ ref: "data" }] } },
	{ name: "contains with three arguments", condition: { op: "contains", args: [{ ref: "data" }, { val: "a" }, { val: "b" }] } },
] as const satisfies readonly { name: string; condition: unknown }[];

export const EXPRESSION_LIMIT_FIXTURES = [
	{ name: "operatorsAndFunctions", atLimit: 300, aboveLimit: 301 },
	{ name: "astDepth", atLimit: 32, aboveLimit: 33 },
	{ name: "jsonPathDereferences", atLimit: 32, aboveLimit: 33 },
	{ name: "inChoices", atLimit: 100, aboveLimit: 101 },
	{ name: "sqliteFunctionArguments", atLimit: 32, aboveLimit: 33 },
	{ name: "sqlitePatternBytes", atLimit: 50, aboveLimit: 51 },
	{ name: "jsonPathBytes", atLimit: 4 * 1024, aboveLimit: 4 * 1024 + 1 },
	{ name: "canonicalPayloadBytes", atLimit: 512 * 1024, aboveLimit: 512 * 1024 + 1 },
	{ name: "compiledSqlBytes", atLimit: 100_000, aboveLimit: 100_001 },
	{ name: "completeStatementBindings", atLimit: 100, aboveLimit: 101 },
] as const satisfies readonly ExpressionLimitFixture[];
