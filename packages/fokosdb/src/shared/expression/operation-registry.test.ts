import { describe, expect, it } from "vitest";
import {
	EXPRESSION_CONTEXT_ALL,
	EXPRESSION_CONTEXT_UPDATE_VALUE,
	OPERATION_REGISTRY,
	getOperationDefinition,
	matchesContext,
} from "./operation-registry.js";
import { analyzeExpressionValue, validateConditionExpression } from "./semantic.js";
import { SQLITE_SCALAR_FUNCTIONS } from "./sqlite-functions.js";

describe("operation registry", () => {
	it("contains exactly 60 operations", () => {
		expect(OPERATION_REGISTRY.size).toBe(60);
	});

	it("contains all 54 SQLite scalar functions", () => {
		for (const name of SQLITE_SCALAR_FUNCTIONS) {
			const op = getOperationDefinition(`sqlite.${name}`);
			expect(op).toBeDefined();
			expect(op?.name).toBe(`sqlite.${name}`);
			expect(op?.contexts).toBe(EXPRESSION_CONTEXT_ALL);
		}
	});

	it("contains Fokos semantic operations size and attribute_type for all contexts", () => {
		const size = getOperationDefinition("size");
		expect(size).toBeDefined();
		expect(size?.contexts).toBe(EXPRESSION_CONTEXT_ALL);
		expect(size?.arity).toEqual([1, 1]);

		const attrType = getOperationDefinition("attribute_type");
		expect(attrType).toBeDefined();
		expect(attrType?.contexts).toBe(EXPRESSION_CONTEXT_ALL);
		expect(attrType?.arity).toEqual([1, 1]);
	});

	it("narrows if_not_exists, +, -, and * to update-value context only", () => {
		for (const name of ["if_not_exists", "+", "-", "*"]) {
			const op = getOperationDefinition(name);
			expect(op).toBeDefined();
			expect(op?.contexts).toBe(EXPRESSION_CONTEXT_UPDATE_VALUE);
			expect(matchesContext(op!.contexts!, "condition")).toBe(false);
			expect(matchesContext(op!.contexts!, "update-value")).toBe(true);
		}
	});

	it("rejects update-value operations when evaluated in condition context", () => {
		expect(() => validateConditionExpression({ op: "eq", args: [{ fn: "+", args: [{ val: 1 }, { val: 2 }] }, { val: 3 }] })).toThrow(
			/unknown expression function/,
		);
		expect(() =>
			validateConditionExpression({
				op: "eq",
				args: [{ fn: "if_not_exists", args: [{ ref: "data", path: "$.a" }, { val: 0 }] }, { val: 0 }],
			}),
		).toThrow(/unknown expression function/);
	});

	it("accepts arithmetic operations in update-value context with valid types", () => {
		const result = analyzeExpressionValue({ fn: "+", args: [{ val: 1 }, { val: 2 }] }, "update-value");
		expect(result.nativeTypes).toEqual(["number"]);

		const subResult = analyzeExpressionValue({ fn: "-", args: [{ val: 5 }, { val: 2 }] }, "update-value");
		expect(subResult.nativeTypes).toEqual(["number"]);

		const mulResult = analyzeExpressionValue({ fn: "*", args: [{ val: 3 }, { val: 4 }] }, "update-value");
		expect(mulResult.nativeTypes).toEqual(["number"]);
	});

	it("rejects arithmetic operations with non-number operands in update-value context", () => {
		expect(() => analyzeExpressionValue({ fn: "+", args: [{ val: "a" }, { val: 2 }] }, "update-value")).toThrow(
			/incompatible expression types/,
		);
		expect(() => analyzeExpressionValue({ fn: "*", args: [{ val: true }, { val: 2 }] }, "update-value")).toThrow(
			/incompatible expression types/,
		);
	});

	it("accepts if_not_exists in update-value context", () => {
		const result = analyzeExpressionValue({ fn: "if_not_exists", args: [{ ref: "data", path: "$.count" }, { val: 0 }] }, "update-value");
		expect(result.nativeTypes).toContain("number");
	});
});
