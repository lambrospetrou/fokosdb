import { describe, expect, it } from "vitest";
import { EXPRESSION_LIMITS } from "./limits.js";
import { SQLITE_CORE_FUNCTIONS, SQLITE_FUNCTION_ARITY, SQLITE_MATH_FUNCTIONS, SQLITE_SCALAR_FUNCTIONS } from "./sqlite-functions.js";
import { analyzeExpressionValue, validateConditionExpression } from "./semantic.js";
import { INVALID_CONDITION_SHAPE_FIXTURES, VALID_CONDITION_SHAPE_FIXTURES } from "./test-fixtures.js";
import type { ConditionExpression } from "./types.js";

const value = (expression: unknown) => analyzeExpressionValue(expression);
const condition = (expression: unknown) => validateConditionExpression(expression);

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

describe("condition shape validation", () => {
	it.each(VALID_CONDITION_SHAPE_FIXTURES)("accepts $name", ({ condition: expression }) => {
		expect(() => condition(expression)).not.toThrow();
	});

	it.each(INVALID_CONDITION_SHAPE_FIXTURES)("rejects $name", ({ condition: expression }) => {
		expect(() => condition(expression)).toThrow();
	});

	it.each([
		["missing args", { op: "eq" }],
		["non-array args", { op: "eq", args: {} }],
		["a scalar logical argument", { op: "and", args: [{ val: true }, { op: "exists", args: [{ ref: "v" }] }] }],
		["an unknown value field", { op: "eq", args: [{ ref: "v", extra: true }, { val: 1 }] }],
		["an unknown literal field", { op: "eq", args: [{ val: 1, extra: true }, { val: 1 }] }],
		["an unknown function field", { op: "eq", args: [{ fn: "size", args: [{ ref: "data" }], extra: true }, { val: 1 }] }],
		["an unknown reference", { op: "exists", args: [{ ref: "internal" }] }],
		["a path on a non-data reference", { op: "exists", args: [{ ref: "v", path: "$" }] }],
		["a malformed data path", { op: "exists", args: [{ ref: "data", path: "$." }] }],
	] as const)("rejects %s", (_name, expression) => {
		expect(() => condition(expression)).toThrow();
	});
});

describe("function validation", () => {
	it("exports the accepted deterministic SQLite functions", () => {
		expect([...SQLITE_SCALAR_FUNCTIONS]).toEqual([...SQLITE_CORE_FUNCTIONS, ...SQLITE_MATH_FUNCTIONS]);
		expect(SQLITE_CORE_FUNCTIONS.size).toBe(28);
		expect(SQLITE_MATH_FUNCTIONS.size).toBe(26);
	});

	it("declares an arity for every allowed SQLite function", () => {
		expect([...SQLITE_FUNCTION_ARITY.keys()].sort()).toEqual([...SQLITE_SCALAR_FUNCTIONS].sort());
	});

	it.each([...SQLITE_SCALAR_FUNCTIONS])("accepts sqlite.%s at its minimum arity", (name) => {
		const [minimum = 0] = SQLITE_FUNCTION_ARITY.get(name) ?? [];
		const args = Array.from({ length: minimum }, () => ({ val: "a" }));
		expect(() => value({ fn: `sqlite.${name}`, args })).not.toThrow();
	});

	it("rejects allowed SQLite functions outside their arity", () => {
		expect(() => value({ fn: "sqlite.abs", args: [] })).toThrow(/argument/);
		expect(() => value({ fn: "sqlite.abs", args: [{ val: 1 }, { val: 2 }] })).toThrow(/argument/);
		expect(() => value({ fn: "sqlite.pi", args: [{ val: 1 }] })).toThrow(/argument/);
		expect(() => value({ fn: "sqlite.replace", args: [{ val: "a" }, { val: "b" }] })).toThrow(/argument/);
		expect(() => value({ fn: "sqlite.coalesce", args: [{ val: "a" }] })).toThrow(/argument/);
		expect(() => value({ fn: "sqlite.round", args: [{ val: 1 }, { val: 2 }, { val: 3 }] })).toThrow(/argument/);
	});

	it.each(["size", "attribute_type"])("accepts %s", (name) => {
		expect(() => value({ fn: name, args: [{ ref: "data" }] })).not.toThrow();
	});

	it.each([
		["unknown", "unknown"],
		["unqualified SQLite", "lower"],
		["aggregate", "sqlite.count"],
		["window", "sqlite.row_number"],
		["table-valued", "sqlite.json_each"],
		["connection state", "sqlite.changes"],
		["extension loading", "sqlite.load_extension"],
		["nondeterministic", "sqlite.random"],
		["date and time", "sqlite.datetime"],
		["planner hint", "sqlite.likelihood"],
	] as const)("rejects the %s function", (_name, name) => {
		expect(() => value({ fn: name, args: [] })).toThrow(/function/);
	});

	it.each(["size", "attribute_type"])("requires one argument for %s", (name) => {
		expect(() => value({ fn: name, args: [] })).toThrow(/argument/);
		expect(() => value({ fn: name, args: [{ val: "a" }, { val: "b" }] })).toThrow(/argument/);
	});

	it("enforces the SQLite function argument limit", () => {
		expect(() =>
			value({ fn: "sqlite.concat", args: Array.from({ length: EXPRESSION_LIMITS.sqliteFunctionArguments }, () => ({ val: "a" })) }),
		).not.toThrow();
		expect(() =>
			value({ fn: "sqlite.concat", args: Array.from({ length: EXPRESSION_LIMITS.sqliteFunctionArguments + 1 }, () => ({ val: "a" })) }),
		).toThrow(/argument limit/);
	});

	it.each([null, true, 1])("rejects size on the statically unsupported value %j", (unsupported) => {
		expect(() => value({ fn: "size", args: [{ val: unsupported }] })).toThrow(/type/);
	});

	it("accepts size on text, bytes-capable references, arrays, and objects", () => {
		expect(() => value({ fn: "size", args: [{ val: "text" }] })).not.toThrow();
		expect(() => value({ fn: "size", args: [{ ref: "hashKey" }] })).not.toThrow();
		expect(() => value({ fn: "size", args: [{ ref: "data" }] })).not.toThrow();
	});

	it("compiles Fokos function result types explicitly", () => {
		expect(value({ fn: "size", args: [{ ref: "data" }] }).nativeTypes).toEqual(["missing", "number"]);
		expect(value({ fn: "attribute_type", args: [{ ref: "data" }] }).nativeTypes).toEqual(["text"]);
		expect(value({ fn: "sqlite.lower", args: [{ ref: "data" }] }).nativeTypes).toEqual(["null", "text"]);
		expect(value({ fn: "sqlite.abs", args: [{ ref: "data" }] }).nativeTypes).toEqual(["null", "number"]);
	});
});

describe("glob and like validation", () => {
	it.each([
		["glob pattern", { fn: "sqlite.glob", args: [{ ref: "data" }, { val: "value" }] }],
		["like pattern", { fn: "sqlite.like", args: [{ ref: "data" }, { val: "value" }] }],
		["like escape", { fn: "sqlite.like", args: [{ val: "%" }, { val: "value" }, { ref: "data" }] }],
	] as const)("rejects a non-literal %s", (_name, expression) => {
		expect(() => value(expression)).toThrow(/literal/);
	});

	it("accepts a pattern at the byte limit and rejects one byte above it", () => {
		expect(() =>
			value({ fn: "sqlite.like", args: [{ val: "x".repeat(EXPRESSION_LIMITS.sqlitePatternBytes) }, { val: "value" }] }),
		).not.toThrow();
		expect(() =>
			value({ fn: "sqlite.like", args: [{ val: "x".repeat(EXPRESSION_LIMITS.sqlitePatternBytes + 1) }, { val: "value" }] }),
		).toThrow(/pattern limit/);
		expect(() => value({ fn: "sqlite.glob", args: [{ val: "é".repeat(25) }, { val: "value" }] })).not.toThrow();
		expect(() => value({ fn: "sqlite.glob", args: [{ val: `${"é".repeat(25)}x` }, { val: "value" }] })).toThrow(/pattern limit/);
	});

	it("requires the supported glob and like arities", () => {
		expect(() => value({ fn: "sqlite.glob", args: [{ val: "*" }] })).toThrow(/argument/);
		expect(() => value({ fn: "sqlite.like", args: [{ val: "%" }] })).toThrow(/argument/);
		expect(() => value({ fn: "sqlite.like", args: [{ val: "%" }, { val: "value" }, { val: "!" }] })).not.toThrow();
		expect(() => value({ fn: "sqlite.like", args: [{ val: "%" }, { val: "value" }, { val: "!" }, { val: "extra" }] })).toThrow(/argument/);
	});
});

describe("static condition types", () => {
	it.each([
		["different equality types", { op: "eq", args: [{ ref: "v" }, { val: "1" }] }],
		["Boolean and number equality", { op: "eq", args: [{ val: true }, { val: 1 }] }],
		["null ordering", { op: "lt", args: [{ ref: "data", path: "$.value" }, { val: null }] }],
		["Boolean ordering", { op: "gte", args: [{ val: true }, { val: false }] }],
		["incompatible between bounds", { op: "between", args: [{ ref: "v" }, { val: 1 }, { val: "3" }] }],
		["an incompatible in choice", { op: "in", args: [{ ref: "v" }, { val: 1 }, { val: "2" }] }],
		["a numeric prefix", { op: "begins_with", args: [{ ref: "v" }, { val: 1 }] }],
		["a null text search", { op: "contains", args: [{ val: "text" }, { val: null }] }],
	] as const)("rejects %s", (_name, expression) => {
		expect(() => condition(expression)).toThrow(/type/);
	});

	it.each([
		["number equality", { op: "eq", args: [{ ref: "v" }, { val: 1 }] }],
		["text ordering", { op: "lt", args: [{ val: "a" }, { val: "b" }] }],
		["a dynamic scalar comparison", { op: "eq", args: [{ ref: "data", path: "$.value" }, { val: true }] }],
		["a text prefix", { op: "begins_with", args: [{ ref: "hashKey" }, { val: "prefix" }] }],
		["a dynamic array search", { op: "contains", args: [{ ref: "data", path: "$.items" }, { val: null }] }],
	] as const)("accepts %s", (_name, expression) => {
		expect(() => condition(expression)).not.toThrow();
	});

	it.each([
		["array", [1]],
		["object", { value: 1 }],
	] as const)("rejects a composite %s literal", (_name, literal) => {
		expect(() => condition({ op: "eq", args: [{ ref: "data" }, { val: literal }] })).toThrow(/scalar literal/);
	});
});

describe("key literal validation", () => {
	it.each([
		{ op: "eq", args: [{ ref: "hashKey" }, { val: "" }] },
		{ op: "ne", args: [{ val: "" }, { ref: "sortKey" }] },
		{ op: "between", args: [{ ref: "hashKey" }, { val: "" }, { val: "z" }] },
		{ op: "in", args: [{ ref: "sortKey" }, { val: "a" }, { val: "" }] },
		{ op: "begins_with", args: [{ ref: "hashKey" }, { val: "" }] },
		{ op: "contains", args: [{ ref: "sortKey" }, { val: "" }] },
	] as const)("rejects an empty string literal against a key", (expression) => {
		expect(() => condition(expression)).toThrow(/empty key literal/);
	});

	it("allows an empty string literal when it is not encoded as a key", () => {
		expect(() => condition({ op: "eq", args: [{ ref: "data" }, { val: "" }] })).not.toThrow();
	});
});

describe("expression limits and required columns", () => {
	it("enforces the AST depth limit", () => {
		expect(() => condition(nestedNot(EXPRESSION_LIMITS.astDepth - 1))).not.toThrow();
		expect(() => condition(nestedNot(EXPRESSION_LIMITS.astDepth))).toThrow(/AST depth/);
	});

	it("enforces the operator and function limit", () => {
		expect(() => condition(repeatedConditions(EXPRESSION_LIMITS.operatorsAndFunctions))).not.toThrow();
		expect(() => condition(repeatedConditions(EXPRESSION_LIMITS.operatorsAndFunctions + 1))).toThrow(/operator and function limit/);
	});

	it("enforces the in choice limit", () => {
		const makeIn = (choices: number) => ({
			op: "in",
			args: [{ ref: "v" }, ...Array.from({ length: choices }, (_, index) => ({ val: index }))],
		});
		expect(() => condition(makeIn(EXPRESSION_LIMITS.inChoices))).not.toThrow();
		expect(() => condition(makeIn(EXPRESSION_LIMITS.inChoices + 1))).toThrow(/choice limit/);
	});

	it("reports required columns in schema order without duplicates", () => {
		const expression: ConditionExpression = {
			op: "and",
			args: [
				{ op: "eq", args: [{ ref: "hashKey" }, { val: "key" }] },
				{ op: "eq", args: [{ ref: "sortKey" }, { val: "sort" }] },
				{ op: "gte", args: [{ ref: "v" }, { val: 1 }] },
				{ op: "exists", args: [{ ref: "ttlAt" }] },
				{ op: "eq", args: [{ fn: "attribute_type", args: [{ ref: "data", path: "$.value" }] }, { val: "text" }] },
				{ op: "exists", args: [{ ref: "data" }] },
			],
		};
		expect(condition(expression).requiredColumns).toEqual(["hk", "sk", "v", "ttl_epoch_utc_seconds", "data_kind", "data"]);
	});

	it("does not require columns for a literal-only expression value", () => {
		expect(value({ fn: "sqlite.lower", args: [{ val: "VALUE" }] }).requiredColumns).toEqual([]);
	});
});

describe("byte literal validation", () => {
	it.each([
		["key equality", { op: "eq", args: [{ ref: "sortKey" }, { b64: "YWI=" }] }],
		["key ordering", { op: "gt", args: [{ ref: "sortKey" }, { b64: "YWI=" }] }],
		["key bounds", { op: "between", args: [{ ref: "sortKey" }, { b64: "YQ==" }, { b64: "eg==" }] }],
		["a key choice list", { op: "in", args: [{ ref: "hashKey" }, { b64: "YQ==" }, { b64: "Yg==" }] }],
		["a key prefix", { op: "begins_with", args: [{ ref: "hashKey" }, { b64: "YQ==" }] }],
		["a key subsequence", { op: "contains", args: [{ ref: "sortKey" }, { b64: "Yg==" }] }],
		["byte data equality", { op: "eq", args: [{ ref: "data" }, { b64: "AQID" }] }],
		["a byte data prefix", { op: "begins_with", args: [{ ref: "data" }, { b64: "AQI=" }] }],
		["size of a byte literal", { op: "eq", args: [{ fn: "size", args: [{ b64: "AQID" }] }, { val: 3 }] }],
	] as const)("accepts %s", (_name, expression) => {
		expect(() => condition(expression)).not.toThrow();
	});

	it.each([
		["an empty literal", { b64: "" }],
		["text that is not base64", { b64: "not base64!" }],
		["base64url text", { b64: "-_8=" }],
		["a non-string literal", { b64: 1 }],
	] as const)("rejects %s", (_name, literal) => {
		expect(() => condition({ op: "eq", args: [{ ref: "data" }, literal] })).toThrow(/byte literal/);
	});

	it("rejects a byte literal above the payload limit", () => {
		const atLimit = { b64: "A".repeat(EXPRESSION_LIMITS.canonicalPayloadBytes - 1) + "=" };
		const aboveLimit = { b64: "A".repeat(EXPRESSION_LIMITS.canonicalPayloadBytes) + "=" };
		expect(() => condition({ op: "eq", args: [{ ref: "data" }, atLimit] })).not.toThrow();
		expect(() => condition({ op: "eq", args: [{ ref: "data" }, aboveLimit] })).toThrow(/payload limit/);
	});

	it.each([
		["a number", { op: "eq", args: [{ ref: "v" }, { b64: "YQ==" }] }],
		["text", { op: "eq", args: [{ val: "ab" }, { b64: "YWI=" }] }],
		["a byte search on a JSON array", { op: "contains", args: [{ ref: "data", path: "$.tags" }, { b64: "YQ==" }] }],
	] as const)("rejects a byte literal against %s", (_name, expression) => {
		expect(() => condition(expression)).toThrow(/type/);
	});

	it("rejects a byte literal mixed with a scalar literal in one in choice list", () => {
		expect(() => condition({ op: "in", args: [{ ref: "sortKey" }, { b64: "YQ==" }, { val: "b" }] })).toThrow(/mix byte and scalar/);
	});
});
