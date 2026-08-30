import { describe, expect, it } from "vitest";
import { KeyCodec } from "../partition-topology/key-codec.js";
import { compileConditionExpression } from "./compiler.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import { composeConditionStatement, CONDITION_PLAN_VERSION } from "./plan.js";
import { materializeExpressionBindings } from "./runtime.js";
import type { ConditionExpression } from "./types.js";

describe("condition SQLite compiler", () => {
	it("creates a versioned JSON-safe condition plan", () => {
		const condition: ConditionExpression = { op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "active" }] };
		const plan = compileConditionExpression(condition);
		expect(plan.version).toBe(CONDITION_PLAN_VERSION);
		expect(plan.kind).toBe("condition");
		expect(plan.requiredColumns).toEqual(["data_kind", "data"]);
		expect(plan.dataDependencies).toEqual({ completeData: false, paths: ["$.status"] });
		expect(plan.result).toEqual({ nativeTypes: ["boolean"], canBeMissing: false });
		expect(JSON.parse(JSON.stringify(plan))).toEqual(plan);
	});

	it("keeps literal and path text out of generated SQL", () => {
		const path = `$."x'); DROP TABLE items; --"`;
		const literal = "value'); SELECT random(); --";
		const plan = compileConditionExpression({ op: "eq", args: [{ ref: "data", path }, { val: literal }] });
		expect(plan.sql).not.toContain(path);
		expect(plan.sql).not.toContain(literal);
		expect(plan.bindings.some((binding) => binding.kind === "path" && binding.value === path)).toBe(true);
		expect(plan.bindings.some((binding) => binding.kind === "val" && binding.value === literal)).toBe(true);
	});

	it("uses keyText descriptors for direct key literals", () => {
		const plan = compileConditionExpression({ op: "gt", args: [{ ref: "sortKey" }, { val: "order#10" }] });
		expect(plan.bindings).toContainEqual({ kind: "keyText", value: "order#10" });
		expect(materializeExpressionBindings([{ kind: "keyText", value: "order#10" }])).toEqual([KeyCodec.encode("order#10")]);
	});

	it("emits direct placeholders for a homogeneous in expression", () => {
		const plan = compileConditionExpression({ op: "in", args: [{ ref: "v" }, { val: 1 }, { val: 2 }, { val: 3 }] });
		expect(plan.sql).toMatch(/ IN \(\?3, \?4, \?5\)/);
		expect(plan.bindings.filter((binding) => binding.kind === "val")).toEqual([
			{ kind: "val", value: 1 },
			{ kind: "val", value: 2 },
			{ kind: "val", value: 3 },
		]);
	});

	it("accepts 100 complete bindings and rejects 101", () => {
		const makeIn = (choices: number) =>
			({
				op: "in",
				args: [{ ref: "v" }, ...Array.from({ length: choices }, (_, value) => ({ val: value }))],
			}) as unknown as ConditionExpression;
		const plan = compileConditionExpression(makeIn(EXPRESSION_LIMITS.completeStatementBindings - 2));
		expect(plan.completeBindingCount).toBe(EXPRESSION_LIMITS.completeStatementBindings);
		expect(() => compileConditionExpression(makeIn(EXPRESSION_LIMITS.completeStatementBindings - 1))).toThrow(/binding limit/);
	});

	it("normalizes negative zero for a lossless JSON round trip", () => {
		const plan = compileConditionExpression({ op: "eq", args: [{ ref: "v" }, { val: -0 }] });
		expect(plan.bindings).toContainEqual({ kind: "val", value: 0 });
		expect(JSON.parse(JSON.stringify(plan))).toEqual(plan);
	});

	it("reports complete-data and JSON-path dependencies", () => {
		const plan = compileConditionExpression({
			op: "and",
			args: [
				{ op: "exists", args: [{ ref: "data" }] },
				{ op: "exists", args: [{ ref: "data", path: "$.a" }] },
				{ op: "exists", args: [{ ref: "data", path: "$.a" }] },
			],
		});
		expect(plan.dataDependencies).toEqual({ completeData: true, paths: ["$.a"] });
	});

	it("numbers parameters densely after the two fixed key bindings", () => {
		for (const condition of [
			{ op: "eq", args: [{ ref: "data", path: "$.value" }, { val: null }] },
			{ op: "begins_with", args: [{ ref: "data", path: "$.value" }, { val: "prefix" }] },
			{ op: "contains", args: [{ ref: "data", path: "$.values" }, { val: true }] },
			{ op: "eq", args: [{ fn: "size", args: [{ ref: "data", path: "$.values" }] }, { val: 3 }] },
			// The scalar branch of contains folds away for a number search.
			{ op: "contains", args: [{ ref: "data", path: "$.tags" }, { val: 3 }] },
			// The array branch of contains folds away for a text-literal container.
			{ op: "contains", args: [{ val: "abc" }, { val: "b" }] },
		] as const satisfies readonly ConditionExpression[]) {
			const message = JSON.stringify(condition);
			const plan = compileConditionExpression(condition);
			const indexes = new Set([...plan.sql.matchAll(/\?(\d+)/g)].map((match) => Number(match[1])));
			expect(indexes.size, message).toBe(plan.bindingCount);
			for (let i = 0; i < plan.bindingCount; i++) expect(indexes.has(3 + i), message).toBe(true);
			expect(composeConditionStatement(plan.sql).match(/\?(?!\d)/g)?.length, message).toBe(2);
		}
	});

	it("shares one binding for a fragment rendered many times", () => {
		const plan = compileConditionExpression({ op: "between", args: [{ ref: "data", path: "$.count" }, { val: 2 }, { val: 4 }] });
		expect(plan.bindings).toEqual([
			{ kind: "path", value: "$.count" },
			{ kind: "val", value: 2 },
			{ kind: "val", value: 4 },
		]);
	});

	it("folds literal-side guards out of the generated SQL", () => {
		const plan = compileConditionExpression({ op: "eq", args: [{ ref: "v" }, { val: 1 }] });
		expect(plan.bindings).toEqual([{ kind: "val", value: 1 }]);
		expect(plan.sql).not.toContain("1 AND");
		expect(plan.sql).not.toContain("IN ('null', 'boolean', 'number', 'text', 'bytes')");
		expect(plan.sql).toContain("= 'number'");
	});

	it("materializes Boolean literals as Workers SQLite integers", () => {
		expect(
			materializeExpressionBindings([
				{ kind: "val", value: true },
				{ kind: "val", value: false },
			]),
		).toEqual([1, 0]);
	});

	it("rejects generated statements above the SQL byte limit", () => {
		const condition = {
			op: "and",
			args: Array.from({ length: EXPRESSION_LIMITS.operatorsAndFunctions - 1 }, () => ({
				op: "contains",
				args: [{ ref: "data", path: "$.values" }, { val: "value" }],
			})),
		} as unknown as ConditionExpression;
		expect(() => compileConditionExpression(condition)).toThrow(/SQL limit/);
	});
});
