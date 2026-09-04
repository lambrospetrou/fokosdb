import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../../server/do-partition.js";
import { KeyCodec } from "../partition-topology/key-codec.js";
import { compileConditionExpression, compileUpdateExpression } from "./compiler.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import {
	composeConditionStatement,
	CONDITION_PLAN_VERSION,
	UPDATE_FIXED_BINDING_COUNT,
	UPDATE_MAX_TRAILING_BINDING_COUNT,
	UPDATE_PLAN_VERSION,
	type CompiledUpdatePlan,
} from "./plan.js";
import { materializeExpressionBindings } from "./runtime.js";
import type { ConditionExpression, UpdateExpression } from "./types.js";

/**
 * Runs a compiled update plan against a literal document, with no items row.
 *
 * The plan numbers its own parameters after the two the real statements reserve for the keys, so the
 * harness binds those two slots — unused here — before the plan's values, and the document last.
 */
function runUpdatePlan(state: DurableObjectState, plan: CompiledUpdatePlan, documentJson: string) {
	const dataParam = `?${plan.completeBindingCount + 1}`;
	return state.storage.sql
		.exec<{ doc: string; applicable: number }>(
			`WITH i AS (SELECT 1 AS hk, 2 AS data_kind, jsonb(${dataParam}) AS data)
			 SELECT json(${plan.documentSql}) AS doc, (${plan.applicableSql}) AS applicable FROM i`,
			...new Array(UPDATE_FIXED_BINDING_COUNT).fill(null),
			...materializeExpressionBindings(plan.bindings),
			documentJson,
		)
		.one();
}

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

describe("byte literal compilation", () => {
	it("binds a key byte literal as canonical tagged key bytes", () => {
		const plan = compileConditionExpression({ op: "gt", args: [{ ref: "sortKey" }, { b64: "YWI" }] });
		expect(plan.bindings).toContainEqual({ kind: "keyB64", value: "YWI=" });
		expect(materializeExpressionBindings([{ kind: "keyB64", value: "YWI=" }])).toEqual([KeyCodec.encode(new Uint8Array([0x61, 0x62]))]);
		expect(JSON.parse(JSON.stringify(plan))).toEqual(plan);
	});

	it("binds a byte data literal as untagged bytes", () => {
		const plan = compileConditionExpression({ op: "begins_with", args: [{ ref: "data" }, { b64: "AQI=" }] });
		expect(plan.bindings).toContainEqual({ kind: "b64", value: "AQI=" });
		expect(materializeExpressionBindings([{ kind: "b64", value: "AQI=" }])).toEqual([new Uint8Array([1, 2])]);
		expect(JSON.parse(JSON.stringify(plan))).toEqual(plan);
	});

	it("binds a key containment search as untagged bytes", () => {
		const plan = compileConditionExpression({ op: "contains", args: [{ ref: "sortKey" }, { b64: "Yg==" }] });
		expect(plan.bindings).toContainEqual({ kind: "b64", value: "Yg==" });
		expect(plan.bindings.some((binding) => binding.kind === "keyB64")).toBe(false);
	});

	it("emits direct placeholders for byte choices against a key", () => {
		const plan = compileConditionExpression({ op: "in", args: [{ ref: "hashKey" }, { b64: "YQ==" }, { b64: "Yg==" }] });
		expect(plan.sql).toMatch(/ IN \(\?3, \?4\)/);
		expect(plan.bindings).toEqual([
			{ kind: "keyB64", value: "YQ==" },
			{ kind: "keyB64", value: "Yg==" },
		]);
	});

	it("keeps base64 text out of generated SQL", () => {
		const plan = compileConditionExpression({ op: "eq", args: [{ ref: "data" }, { b64: "AQID" }] });
		expect(plan.sql).not.toContain("AQID");
	});
});

describe("update SQLite compiler", () => {
	it("creates a versioned JSON-safe update plan", () => {
		const update: UpdateExpression = [
			{ action: "set", target: { ref: "data", path: "$.status" }, value: { val: "active" } },
			{ action: "remove", target: { ref: "data", path: "$.tempToken" } },
		];
		const plan = compileUpdateExpression(update);
		expect(plan.version).toBe(UPDATE_PLAN_VERSION);
		expect(plan.kind).toBe("update");
		expect(plan.requiredColumns).toContain("data_kind");
		expect(plan.requiredColumns).toContain("data");
		expect(plan.dataDependencies.paths).toEqual(["$.status", "$.tempToken"]);
		expect(JSON.parse(JSON.stringify(plan))).toEqual(plan);
	});

	it("nests document expressions with accumulator starting at i.data", () => {
		const update: UpdateExpression = [
			{ action: "set", target: { ref: "data", path: "$.a" }, value: { val: 1 } },
			{ action: "remove", target: { ref: "data", path: "$.b" } },
		];
		const plan = compileUpdateExpression(update);
		expect(plan.documentSql).toMatch(/^jsonb_remove\(jsonb_set\(i\.data, \?\d+, (?:json_quote\()?\?\d+\)?\), \?\d+\)$/);
	});

	it("renders boolean and null literals accurately for JSONB document", () => {
		const update: UpdateExpression = [
			{ action: "set", target: { ref: "data", path: "$.active" }, value: { val: true } },
			{ action: "set", target: { ref: "data", path: "$.disabled" }, value: { val: false } },
			{ action: "set", target: { ref: "data", path: "$.none" }, value: { val: null } },
		];
		const plan = compileUpdateExpression(update);
		expect(plan.documentSql).toContain("jsonb('true')");
		expect(plan.documentSql).toContain("jsonb('false')");
		expect(plan.documentSql).toContain("NULL");
	});

	it("sorts removals of plain array indices under same parent in descending index order", () => {
		const update: UpdateExpression = [
			{ action: "remove", target: { ref: "data", path: "$.list[1]" } },
			{ action: "remove", target: { ref: "data", path: "$.list[2]" } },
			{ action: "remove", target: { ref: "data", path: "$.list[0]" } },
		];
		const plan = compileUpdateExpression(update);
		const pathsInOrder = plan.bindings.filter((b) => b.kind === "path").map((b) => b.value);
		expect(pathsInOrder).toEqual(["$.list[2]", "$.list[1]", "$.list[0]"]);
	});

	it("builds applicability guards for target validity and document result type", () => {
		const update: UpdateExpression = [
			{ action: "set", target: { ref: "data", path: "$.user.name" }, value: { val: "Alice" } },
			{ action: "set", target: { ref: "data", path: "$.tags[#]" }, value: { val: "new" } },
			{ action: "set", target: { ref: "data", path: "$.scores[0]" }, value: { val: 100 } },
		];
		const plan = compileUpdateExpression(update);
		expect(plan.applicableSql).toContain("i.hk IS NOT NULL");
		expect(plan.applicableSql).toContain("i.data_kind = 2");
		expect(plan.applicableSql).toContain("json_type(i.data, ?");
		expect(plan.applicableSql).toContain("= 'object'");
		expect(plan.applicableSql).toContain("= 'array'");
		expect(plan.applicableSql).toContain("IS NOT NULL");
		expect(plan.applicableSql).toContain("json_type(");
		expect(plan.applicableSql).toContain("IN ('array', 'object')");
	});

	it("compiles if_not_exists and arithmetic expressions", () => {
		const update: UpdateExpression = [
			{
				action: "set",
				target: { ref: "data", path: "$.loginCount" },
				value: {
					fn: "+",
					args: [{ fn: "if_not_exists", args: [{ ref: "data", path: "$.loginCount" }, { val: 0 }] }, { val: 1 }],
				},
			},
		];
		const plan = compileUpdateExpression(update);
		expect(plan.documentSql).toContain("+");
		expect(plan.documentSql).toContain("CASE WHEN");
		expect(plan.applicableSql).toContain("abs(");
		expect(plan.applicableSql).toContain("< 1e999");
	});

	// The keys take ?1 and ?2 in every statement that runs the plan, so the plan's own parameters start
	// after them and run densely to completeBindingCount. A statement appends its tail from there.
	it("numbers parameters densely, after the reserved key parameters", () => {
		const update: UpdateExpression = [
			{ action: "set", target: { ref: "data", path: "$.status" }, value: { val: "active" } },
			{ action: "set", target: { ref: "data", path: "$.count" }, value: { val: 5 } },
		];
		const plan = compileUpdateExpression(update);
		const allParams = new Set<number>();
		for (const match of plan.documentSql.matchAll(/\?(\d+)/g)) allParams.add(Number(match[1]));
		for (const match of plan.applicableSql.matchAll(/\?(\d+)/g)) allParams.add(Number(match[1]));
		expect(plan.completeBindingCount).toBe(UPDATE_FIXED_BINDING_COUNT + plan.bindingCount);
		expect([...allParams].sort((a, b) => a - b)).toEqual(
			Array.from({ length: plan.bindingCount }, (_, i) => UPDATE_FIXED_BINDING_COUNT + i + 1),
		);
	});

	// Workers SQLite caps a query at 100 parameters, and the plan is only part of one: the keys precede
	// it and the widest statement appends its own tail. Charging both at compile time turns what would
	// be an opaque failure inside a transaction into a limit error the caller gets before it starts.
	it("rejects an update whose widest statement would exceed the binding limit", () => {
		const planCap = EXPRESSION_LIMITS.completeStatementBindings - UPDATE_FIXED_BINDING_COUNT - UPDATE_MAX_TRAILING_BINDING_COUNT;
		// One binding for each target path, one shared binding for the "$" parent that the target guards
		// test, and one for each distinct literal. Only the literal count is free to vary.
		const literalsAtCap = planCap - EXPRESSION_LIMITS.updateActions - 1;

		// Spreads `total` distinct literals over the maximum number of actions.
		const withLiterals = (total: number): UpdateExpression => {
			const paired = total - EXPRESSION_LIMITS.updateActions;
			let next = 0;
			return Array.from({ length: EXPRESSION_LIMITS.updateActions }, (_, i) => ({
				action: "set" as const,
				target: { ref: "data" as const, path: `$.f${i}` },
				value: i < paired ? { fn: "+" as const, args: [{ val: next++ }, { val: next++ }] } : { val: next++ },
			}));
		};

		const atCap = compileUpdateExpression(withLiterals(literalsAtCap));
		expect(atCap.completeBindingCount + UPDATE_MAX_TRAILING_BINDING_COUNT).toBe(EXPRESSION_LIMITS.completeStatementBindings);

		expect(() => compileUpdateExpression(withLiterals(literalsAtCap + 1))).toThrow(/complete statement exceeds the binding limit/);
	});

	it("evaluates pre-image document expression in Workers SQLite", async () => {
		const stub = PartitionDO.getByName(env.PARTITION_DO, `update-compiler-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const update: UpdateExpression = [
				{ action: "remove", target: { ref: "data", path: "$.a" } },
				{ action: "set", target: { ref: "data", path: "$.b" }, value: { ref: "data", path: "$.a" } },
				{ action: "set", target: { ref: "data", path: "$.c" }, value: { ref: "data", path: "$.b" } },
			];
			const plan = compileUpdateExpression(update);
			const row = runUpdatePlan(state, plan, JSON.stringify({ a: 1, b: 2, c: 3 }));
			expect(JSON.parse(row.doc)).toEqual({ b: 1, c: 2 });
			expect(row.applicable).toBe(1);
		});
	});

	it("evaluates plain array index removal order in Workers SQLite", async () => {
		const stub = PartitionDO.getByName(env.PARTITION_DO, `update-compiler-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const update: UpdateExpression = [
				{ action: "remove", target: { ref: "data", path: "$.r[1]" } },
				{ action: "remove", target: { ref: "data", path: "$.r[2]" } },
			];
			const plan = compileUpdateExpression(update);
			const row = runUpdatePlan(state, plan, JSON.stringify({ r: ["c", "h", "n", "s", "x"] }));
			expect(JSON.parse(row.doc)).toEqual({ r: ["c", "s", "x"] });
			expect(row.applicable).toBe(1);
		});
	});

	it("rejects invalid target in applicability check in Workers SQLite", async () => {
		const stub = PartitionDO.getByName(env.PARTITION_DO, `update-compiler-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const update: UpdateExpression = [{ action: "set", target: { ref: "data", path: "$.missing.child" }, value: { val: 1 } }];
			const plan = compileUpdateExpression(update);
			const row = runUpdatePlan(state, plan, JSON.stringify({ a: 1 }));
			expect(row.applicable).toBe(0);
		});
	});

	it("evaluates if_not_exists and arithmetic increment in Workers SQLite", async () => {
		const stub = PartitionDO.getByName(env.PARTITION_DO, `update-compiler-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const update: UpdateExpression = [
				{
					action: "set",
					target: { ref: "data", path: "$.loginCount" },
					value: {
						fn: "+",
						args: [{ fn: "if_not_exists", args: [{ ref: "data", path: "$.loginCount" }, { val: 0 }] }, { val: 1 }],
					},
				},
			];
			const plan = compileUpdateExpression(update);
			const row1 = runUpdatePlan(state, plan, JSON.stringify({}));
			expect(JSON.parse(row1.doc)).toEqual({ loginCount: 1 });
			expect(row1.applicable).toBe(1);

			const row2 = runUpdatePlan(state, plan, JSON.stringify({ loginCount: 5 }));
			expect(JSON.parse(row2.doc)).toEqual({ loginCount: 6 });
			expect(row2.applicable).toBe(1);
		});
	});

	it("preserves boolean values copied from data references", async () => {
		const stub = PartitionDO.getByName(env.PARTITION_DO, `update-boolean-ref-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const update: UpdateExpression = [
				{ action: "set", target: { ref: "data", path: "$.flag" }, value: { ref: "data", path: "$.source" } },
			];
			const plan = compileUpdateExpression(update);
			const row = runUpdatePlan(state, plan, JSON.stringify({ source: true }));
			expect(JSON.parse(row.doc)).toEqual({ source: true, flag: true });
			expect(row.applicable).toBe(1);
		});
	});

	it("preserves boolean literals in if_not_exists fallbacks", async () => {
		const stub = PartitionDO.getByName(env.PARTITION_DO, `update-boolean-fallback-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const update: UpdateExpression = [
				{
					action: "set",
					target: { ref: "data", path: "$.flag" },
					value: { fn: "if_not_exists", args: [{ ref: "data", path: "$.missing" }, { val: true }] },
				},
			];
			const plan = compileUpdateExpression(update);
			const row = runUpdatePlan(state, plan, JSON.stringify({}));
			expect(JSON.parse(row.doc)).toEqual({ flag: true });
			expect(row.applicable).toBe(1);
		});
	});

	it("preserves string values that look like JSON literals", async () => {
		const stub = PartitionDO.getByName(env.PARTITION_DO, `update-string-literal-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const update: UpdateExpression = [
				{ action: "set", target: { ref: "data", path: "$.number" }, value: { val: "5" } },
				{ action: "set", target: { ref: "data", path: "$.boolean" }, value: { val: "true" } },
				{ action: "set", target: { ref: "data", path: "$.array" }, value: { val: "[1,2]" } },
			];
			const plan = compileUpdateExpression(update);
			const row = runUpdatePlan(state, plan, JSON.stringify({}));
			expect(JSON.parse(row.doc)).toEqual({ number: "5", boolean: "true", array: "[1,2]" });
			expect(row.applicable).toBe(1);
		});
	});

	it("preserves string values copied from data references", async () => {
		const stub = PartitionDO.getByName(env.PARTITION_DO, `update-string-ref-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const update: UpdateExpression = [
				{ action: "set", target: { ref: "data", path: "$.copy" }, value: { ref: "data", path: "$.source" } },
			];
			const plan = compileUpdateExpression(update);
			const row = runUpdatePlan(state, plan, JSON.stringify({ source: "5" }));
			expect(JSON.parse(row.doc)).toEqual({ source: "5", copy: "5" });
			expect(row.applicable).toBe(1);
		});
	});

	it("preserves booleans copied through a value-passing function", async () => {
		const stub = PartitionDO.getByName(env.PARTITION_DO, `update-passthrough-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			// SQLite carries a JSON boolean as 1 or 0, so a function that returns one of its arguments
			// must receive that argument already encoded as JSON. Otherwise `true` lands as the number 1.
			const update: UpdateExpression = [
				{ action: "set", target: { ref: "data", path: "$.viaCoalesce" }, value: { fn: "sqlite.coalesce", args: [{ ref: "data", path: "$.flag" }, { val: 0 }] } },
				{ action: "set", target: { ref: "data", path: "$.viaIfnull" }, value: { fn: "sqlite.ifnull", args: [{ ref: "data", path: "$.off" }, { val: 0 }] } },
				{ action: "set", target: { ref: "data", path: "$.viaIfNotExists" }, value: { fn: "if_not_exists", args: [{ ref: "data", path: "$.flag" }, { val: 0 }] } },
			];
			const plan = compileUpdateExpression(update);
			const row = runUpdatePlan(state, plan, JSON.stringify({ flag: true, off: false }));
			expect(JSON.parse(row.doc)).toEqual({ flag: true, off: false, viaCoalesce: true, viaIfnull: false, viaIfNotExists: true });
			expect(row.applicable).toBe(1);
		});
	});

	it("rejects an update value that can only be bytes", () => {
		// A JSON document has no byte type, so a value whose only non-null result is bytes can never
		// write a valid document. SQLite would otherwise read the blob back as JSONB: a blob that
		// happens to be valid JSONB becomes a silently wrong member, and one that is not leaves a
		// document that no read can decode.
		for (const value of [{ b64: "AQID" }, { fn: "sqlite.unhex" as const, args: [{ val: "01" }] }]) {
			const update = [{ action: "set", target: { ref: "data", path: "$.x" }, value }] as UpdateExpression;
			expect(() => compileUpdateExpression(update)).toThrow(/must not be bytes/);
		}
	});

	it("accepts a byte literal as the argument of a value that is not bytes", () => {
		// The test is over the type of the whole value, not over the nodes inside it: the size of a byte
		// literal is a number, and a number is a value a document can hold.
		const update: UpdateExpression = [
			{ action: "set", target: { ref: "data", path: "$.x" }, value: { fn: "size", args: [{ b64: "AQID" }] } },
		];
		expect(compileUpdateExpression(update).valueTypeSql).toBe("1");
	});

	it("carries a per-item byte test only for a value whose type is not known at compile time", () => {
		// A key reference is text for a text key and bytes for a binary one, and a SQLite function is
		// typed by what it returns for the row. Neither is decidable here, so the test moves into the
		// statement. A literal is decided here and costs no SQL.
		const dynamic: UpdateExpression = [
			{ action: "set", target: { ref: "data", path: "$.k" }, value: { ref: "hashKey" } },
			{ action: "set", target: { ref: "data", path: "$.f" }, value: { fn: "sqlite.ifnull", args: [{ ref: "sortKey" }, { val: "x" }] } },
		];
		const plan = compileUpdateExpression(dynamic);
		expect(plan.valueTypeSql).toContain("<> 'bytes'");
		expect(plan.applicableSql).toContain(plan.valueTypeSql);

		const literal: UpdateExpression = [{ action: "set", target: { ref: "data", path: "$.x" }, value: { val: "text" } }];
		expect(compileUpdateExpression(literal).valueTypeSql).toBe("1");
	});

	it("accepts a plan at the binding budget and refuses one binding more", () => {
		// The budget belongs to the widest statement that embeds the plan, so the compiler charges the
		// keys before the plan and the widest statement tail after it. One binding more must fail here,
		// in the client, rather than at the partition with no useful error.
		const budget = EXPRESSION_LIMITS.completeStatementBindings - UPDATE_FIXED_BINDING_COUNT - UPDATE_MAX_TRAILING_BINDING_COUNT;
		// The action count is capped well below the binding budget, so the bindings come from the values.
		// One action at every allowed slot binds its own target path, one literal, and the parent path
		// they all share. An arithmetic value binds two literals instead of one, so each one adds exactly
		// one binding. Every literal is distinct, because equal bindings are deduplicated into one.
		const planWithBindings = (bindings: number): UpdateExpression => {
			const paired = bindings - (2 * EXPRESSION_LIMITS.updateActions + 1);
			let next = 0;
			return Array.from({ length: EXPRESSION_LIMITS.updateActions }, (_, i) => ({
				action: "set" as const,
				target: { ref: "data" as const, path: `$.f${i}` },
				value: i < paired ? { fn: "+" as const, args: [{ val: next++ }, { val: next++ }] } : { val: next++ },
			}));
		};

		const plan = compileUpdateExpression(planWithBindings(budget));
		expect(plan.bindingCount).toBe(budget);
		expect(plan.completeBindingCount + UPDATE_MAX_TRAILING_BINDING_COUNT).toBe(EXPRESSION_LIMITS.completeStatementBindings);
		expect(() => compileUpdateExpression(planWithBindings(budget + 1))).toThrow(/binding limit/);
	});
});
