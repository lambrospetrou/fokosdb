import { describe, expect, it } from "vitest";
import { EXPRESSION_LIMITS, EXPRESSION_NATIVE_TYPES, type ConditionExpression, type ProjectionExpression } from "../types.js";

const condition: ConditionExpression = {
	op: "and",
	args: [
		{ op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "active" }] },
		{ op: "gte", args: [{ fn: "size", args: [{ ref: "data", path: "$.tags" }] }, { val: 2 }] },
	],
};

const filter: ConditionExpression = {
	op: "in",
	args: [{ ref: "data", path: "$.status" }, { val: "pending" }, { val: "processing" }],
};

const projection: readonly ProjectionExpression[] = [
	{ expr: { ref: "hashKey" }, as: "id" },
	{ expr: { ref: "data", path: "$.profile.email" } },
	{ expr: { fn: "sqlite.lower", args: [{ ref: "data", path: "$.profile.email" }] }, as: "normalizedEmail" },
];

describe("expression public types", () => {
	it("accepts representative condition, filter, and projection objects", () => {
		const examples = { condition, filter, projection };
		expect(JSON.parse(JSON.stringify(examples))).toEqual(examples);
	});

	it("exports the complete native type vocabulary", () => {
		expect(EXPRESSION_NATIVE_TYPES).toEqual(["missing", "null", "boolean", "number", "text", "bytes", "array", "object"]);
	});

	it("exports the version-one limits", () => {
		expect(EXPRESSION_LIMITS).toEqual({
			operatorsAndFunctions: 300,
			astDepth: 32,
			jsonPathDereferences: 32,
			inChoices: 100,
			sqliteFunctionArguments: 32,
			sqlitePatternBytes: 50,
			jsonPathBytes: 4 * 1024,
			canonicalPayloadBytes: 512 * 1024,
			compiledSqlBytes: 100_000,
			completeStatementBindings: 100,
		});
	});
});
