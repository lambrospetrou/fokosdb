import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../../server/do-partition.js";
import { KeyCodec } from "../partition-topology/key-codec.js";
import { compileConditionExpression, compileUpdateExpression } from "./compiler.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import { composeConditionStatement, CONDITION_PLAN_VERSION, UPDATE_PLAN_VERSION } from "./plan.js";
import { materializeExpressionBindings } from "./runtime.js";
import type { ConditionExpression, UpdateExpression } from "./types.js";

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
		expect(plan.documentSql).toMatch(/^jsonb_remove\(jsonb_set\(i\.data, \?\d+, \?\d+\), \?\d+\)$/);
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

	it("numbers parameters densely starting from ?1", () => {
		const update: UpdateExpression = [
			{ action: "set", target: { ref: "data", path: "$.status" }, value: { val: "active" } },
			{ action: "set", target: { ref: "data", path: "$.count" }, value: { val: 5 } },
		];
		const plan = compileUpdateExpression(update);
		const allParams = new Set<number>();
		for (const match of plan.documentSql.matchAll(/\?(\d+)/g)) allParams.add(Number(match[1]));
		for (const match of plan.applicableSql.matchAll(/\?(\d+)/g)) allParams.add(Number(match[1]));
		for (let i = 1; i <= plan.bindingCount; i++) {
			expect(allParams.has(i)).toBe(true);
		}
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
			const bindings = materializeExpressionBindings(plan.bindings);
			const initialJson = JSON.stringify({ a: 1, b: 2, c: 3 });
			const dataParam = `?${plan.bindings.length + 1}`;
			const row = state.storage.sql
				.exec<{ doc: string; applicable: number }>(
					`WITH i AS (SELECT 1 AS hk, 2 AS data_kind, jsonb(${dataParam}) AS data)
					 SELECT json(${plan.documentSql}) AS doc, (${plan.applicableSql}) AS applicable FROM i`,
					...bindings,
					initialJson,
				)
				.one();
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
			const bindings = materializeExpressionBindings(plan.bindings);
			const initialJson = JSON.stringify({ r: ["c", "h", "n", "s", "x"] });
			const dataParam = `?${plan.bindings.length + 1}`;
			const row = state.storage.sql
				.exec<{ doc: string; applicable: number }>(
					`WITH i AS (SELECT 1 AS hk, 2 AS data_kind, jsonb(${dataParam}) AS data)
					 SELECT json(${plan.documentSql}) AS doc, (${plan.applicableSql}) AS applicable FROM i`,
					...bindings,
					initialJson,
				)
				.one();
			expect(JSON.parse(row.doc)).toEqual({ r: ["c", "s", "x"] });
			expect(row.applicable).toBe(1);
		});
	});

	it("rejects invalid target in applicability check in Workers SQLite", async () => {
		const stub = PartitionDO.getByName(env.PARTITION_DO, `update-compiler-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const update: UpdateExpression = [{ action: "set", target: { ref: "data", path: "$.missing.child" }, value: { val: 1 } }];
			const plan = compileUpdateExpression(update);
			const bindings = materializeExpressionBindings(plan.bindings);
			const initialJson = JSON.stringify({ a: 1 });
			const dataParam = `?${plan.bindings.length + 1}`;
			const row = state.storage.sql
				.exec<{ applicable: number }>(
					`WITH i AS (SELECT 1 AS hk, 2 AS data_kind, jsonb(${dataParam}) AS data)
					 SELECT (${plan.applicableSql}) AS applicable FROM i`,
					...bindings,
					initialJson,
				)
				.one();
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
			const bindings = materializeExpressionBindings(plan.bindings);
			const dataParam = `?${plan.bindings.length + 1}`;

			const row1 = state.storage.sql
				.exec<{ doc: string; applicable: number }>(
					`WITH i AS (SELECT 1 AS hk, 2 AS data_kind, jsonb(${dataParam}) AS data)
					 SELECT json(${plan.documentSql}) AS doc, (${plan.applicableSql}) AS applicable FROM i`,
					...bindings,
					JSON.stringify({}),
				)
				.one();
			expect(JSON.parse(row1.doc)).toEqual({ loginCount: 1 });
			expect(row1.applicable).toBe(1);

			const row2 = state.storage.sql
				.exec<{ doc: string; applicable: number }>(
					`WITH i AS (SELECT 1 AS hk, 2 AS data_kind, jsonb(${dataParam}) AS data)
					 SELECT json(${plan.documentSql}) AS doc, (${plan.applicableSql}) AS applicable FROM i`,
					...bindings,
					JSON.stringify({ loginCount: 5 }),
				)
				.one();
			expect(JSON.parse(row2.doc)).toEqual({ loginCount: 6 });
			expect(row2.applicable).toBe(1);
		});
	});
});
