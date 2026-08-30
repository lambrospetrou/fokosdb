import { describe, expect, it } from "vitest";
import { EXPRESSION_LIMITS } from "./limits.js";
import { analyzeExpressionValueValibot, validateConditionExpressionValibot } from "./semantic-valibot.js";
import { analyzeExpressionValue, validateConditionExpression } from "./semantic.js";
import { SQLITE_SCALAR_FUNCTIONS } from "./sqlite-functions.js";
import { INVALID_CONDITION_SHAPE_FIXTURES, VALID_CONDITION_SHAPE_FIXTURES } from "./test-fixtures.js";

function accepts(validate: (expression: unknown) => unknown, expression: unknown): boolean {
	try {
		validate(expression);
		return true;
	} catch {
		return false;
	}
}

function nestedNot(depth: number): unknown {
	let result: unknown = { op: "exists", args: [{ ref: "v" }] };
	for (let i = 1; i < depth; i++) result = { op: "not", args: [result] };
	return result;
}

function repeatedConditions(count: number): unknown {
	return {
		op: "and",
		args: Array.from({ length: count - 1 }, () => ({ op: "exists", args: [{ ref: "v" }] })),
	};
}

describe("Valibot semantic implementation", () => {
	it.each(VALID_CONDITION_SHAPE_FIXTURES)("accepts $name", ({ condition }) => {
		expect(() => validateConditionExpressionValibot(condition)).not.toThrow();
	});

	it.each(INVALID_CONDITION_SHAPE_FIXTURES)("rejects $name", ({ condition }) => {
		expect(() => validateConditionExpressionValibot(condition)).toThrow();
	});

	it.each([
		[true, { op: "eq", args: [{ ref: "v" }, { val: 1 }] }],
		[true, { op: "eq", args: [{ ref: "data", path: "$.value" }, { val: true }] }],
		[true, { op: "contains", args: [{ ref: "data", path: "$.items" }, { val: null }] }],
		[false, { op: "eq", args: [{ ref: "v" }, { val: "1" }] }],
		[false, { op: "lt", args: [{ val: null }, { val: null }] }],
		[false, { op: "begins_with", args: [{ ref: "v" }, { val: 1 }] }],
		[false, { op: "eq", args: [{ ref: "hashKey" }, { val: "" }] }],
		[false, { op: "eq", args: [{ ref: "data" }, { val: [1] }] }],
		[false, { op: "eq", args: [{ fn: "size", args: [{ val: true }] }, { val: 1 }] }],
		[false, { op: "eq", args: [{ fn: "sqlite.random", args: [] }, { val: 1 }] }],
		[false, { op: "eq", args: [{ fn: "sqlite.like", args: [{ ref: "data" }, { val: "value" }] }, { val: 1 }] }],
		[
			false,
			{
				op: "eq",
				args: [{ fn: "sqlite.like", args: [{ val: "x".repeat(EXPRESSION_LIMITS.sqlitePatternBytes + 1) }, { val: "value" }] }, { val: 1 }],
			},
		],
	] as const)("matches the manual validator for semantic case %#", (expected, expression) => {
		expect(accepts(validateConditionExpression, expression)).toBe(expected);
		expect(accepts(validateConditionExpressionValibot, expression)).toBe(expected);
	});

	it.each([...SQLITE_SCALAR_FUNCTIONS])("accepts sqlite.%s", (name) => {
		const args = name === "glob" ? [{ val: "*" }, { val: "value" }] : name === "like" ? [{ val: "%" }, { val: "value" }] : [];
		expect(() => analyzeExpressionValueValibot({ fn: `sqlite.${name}`, args })).not.toThrow();
	});

	it.each([
		{ fn: "size", args: [{ ref: "data" }] },
		{ fn: "attribute_type", args: [{ ref: "data" }] },
		{ fn: "sqlite.lower", args: [{ ref: "data" }] },
		{ fn: "sqlite.abs", args: [{ ref: "data" }] },
		{ fn: "sqlite.coalesce", args: [{ ref: "data" }, { val: "fallback" }] },
	] as const)("returns the same value analysis for %#", (expression) => {
		expect(analyzeExpressionValueValibot(expression)).toEqual(analyzeExpressionValue(expression));
	});

	it("returns the same required-column analysis", () => {
		const expression = {
			op: "and",
			args: [
				{ op: "eq", args: [{ ref: "hashKey" }, { val: "key" }] },
				{ op: "eq", args: [{ ref: "sortKey" }, { val: "sort" }] },
				{ op: "gte", args: [{ ref: "v" }, { val: 1 }] },
				{ op: "exists", args: [{ ref: "ttl" }] },
				{ op: "exists", args: [{ ref: "data", path: "$.value" }] },
			],
		};
		expect(validateConditionExpressionValibot(expression)).toEqual(validateConditionExpression(expression));
	});

	it("enforces the same global limits", () => {
		expect(() => validateConditionExpressionValibot(nestedNot(EXPRESSION_LIMITS.astDepth - 1))).not.toThrow();
		expect(() => validateConditionExpressionValibot(nestedNot(EXPRESSION_LIMITS.astDepth))).toThrow(/AST depth/);
		expect(() => validateConditionExpressionValibot(repeatedConditions(EXPRESSION_LIMITS.operatorsAndFunctions))).not.toThrow();
		expect(() => validateConditionExpressionValibot(repeatedConditions(EXPRESSION_LIMITS.operatorsAndFunctions + 1))).toThrow(
			/operator and function limit/,
		);
	});
});
