import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { JsonValue } from "../json-types.js";
import { PartitionDO } from "../do-partition.js";
import { KeyCodec, type KeyBytes } from "../partition-topology/key-codec.js";
import { PartitionStore } from "../partition/partition-store.js";
import type { DataKind } from "../types.js";
import { compileConditionExpression } from "./compiler.js";
import { composeConditionStatement } from "./plan.js";
import { evaluateConditionPlan, materializeExpressionBindings } from "./runtime.js";
import { MISSING_NULL_SEMANTIC_FIXTURES, type ExpressionSemanticItem } from "./test-fixtures.js";
import type { ConditionExpression } from "./types.js";

type StoredFixture = {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;
	data: string | Uint8Array | JsonValue;
	kind: DataKind;
	ttl?: number;
};

async function evaluate(item: StoredFixture | null, condition: ConditionExpression) {
	const stub = PartitionDO.getByName(env.PARTITION_DO, `expression-runtime.${crypto.randomUUID()}`);
	return await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
		const hashKey = KeyCodec.encode(item?.hashKey ?? "missing-hash-key");
		const sortKey = item?.sortKey === undefined ? KeyCodec.encodeOptional(undefined) : KeyCodec.encode(item.sortKey);
		if (item) putFixture(state.storage, hashKey, sortKey, item);
		return evaluateConditionPlan(state.storage, compileConditionExpression(condition), hashKey, sortKey);
	});
}

function putFixture(storage: DurableObjectStorage, hashKey: KeyBytes, sortKey: KeyBytes, item: StoredFixture): void {
	new PartitionStore(storage).upsertItem({
		hk: hashKey,
		sk: sortKey,
		data: item.kind === "json" ? JSON.stringify(item.data) : (item.data as string | Uint8Array),
		kind: item.kind,
		ttlEpochUtcSeconds: item.ttl ?? null,
		lastTransactionTs: 1,
	});
}

describe("compiled condition runtime", () => {
	it.each(MISSING_NULL_SEMANTIC_FIXTURES)("$name", async ({ item, condition, expected }) => {
		const result = await evaluate(item as ExpressionSemanticItem | null, condition);
		expect(result.conditionOk).toBe(expected);
	});

	it("implements scalar, existence, and logical semantics", async () => {
		const item: StoredFixture = {
			hashKey: "item",
			sortKey: "sort",
			kind: "json",
			data: { nullValue: null, enabled: true, count: 3, ratio: 1.5, text: "café", nested: { value: "yes" } },
			ttl: 2_000_000_000,
		};
		for (const [condition, expected] of [
			[{ op: "eq", args: [{ ref: "hashKey" }, { val: "item" }] }, true],
			[{ op: "eq", args: [{ ref: "sortKey" }, { val: "sort" }] }, true],
			[{ op: "eq", args: [{ ref: "v" }, { val: 1 }] }, true],
			[{ op: "eq", args: [{ ref: "ttl" }, { val: 2_000_000_000 }] }, true],
			[{ op: "eq", args: [{ ref: "data", path: "$.nullValue" }, { val: null }] }, true],
			[{ op: "eq", args: [{ ref: "data", path: "$.enabled" }, { val: true }] }, true],
			[{ op: "ne", args: [{ ref: "data", path: "$.enabled" }, { val: false }] }, true],
			[{ op: "between", args: [{ ref: "data", path: "$.count" }, { val: 2 }, { val: 4 }] }, true],
			[{ op: "in", args: [{ ref: "data", path: "$.count" }, { val: 1 }, { val: 3 }, { val: 5 }] }, true],
			[{ op: "lt", args: [{ ref: "data", path: "$.ratio" }, { val: 2 }] }, true],
			[{ op: "eq", args: [{ ref: "data", path: "$.count" }, { val: "3" }] }, false],
			[{ op: "not", args: [{ op: "exists", args: [{ ref: "data", path: "$.missing" }] }] }, true],
		] as const satisfies readonly [ConditionExpression, boolean][]) {
			expect((await evaluate(item, condition)).conditionOk, JSON.stringify(condition)).toBe(expected);
		}
	});

	it("implements prefix, containment, size, type, and reverse-path semantics", async () => {
		const item: StoredFixture = {
			hashKey: "item",
			kind: "json",
			data: { text: "héllo world", tags: ["blue", true, null, 3], object: { a: 1, b: 2 }, values: [1, 2, 3] },
		};
		for (const condition of [
			{ op: "begins_with", args: [{ ref: "data", path: "$.text" }, { val: "hé" }] },
			{ op: "contains", args: [{ ref: "data", path: "$.text" }, { val: "llo" }] },
			{ op: "contains", args: [{ ref: "data", path: "$.tags" }, { val: "blue" }] },
			{ op: "contains", args: [{ ref: "data", path: "$.tags" }, { val: true }] },
			{ op: "contains", args: [{ ref: "data", path: "$.tags" }, { val: null }] },
			{ op: "contains", args: [{ ref: "data", path: "$.tags" }, { val: 3 }] },
			{ op: "eq", args: [{ fn: "size", args: [{ ref: "data", path: "$.text" }] }, { val: 12 }] },
			{ op: "eq", args: [{ fn: "size", args: [{ ref: "data", path: "$.tags" }] }, { val: 4 }] },
			{ op: "eq", args: [{ fn: "size", args: [{ ref: "data", path: "$.object" }] }, { val: 2 }] },
			{ op: "eq", args: [{ fn: "attribute_type", args: [{ ref: "data", path: "$.object" }] }, { val: "object" }] },
			{ op: "eq", args: [{ fn: "attribute_type", args: [{ ref: "data", path: "$.missing" }] }, { val: "missing" }] },
			{ op: "eq", args: [{ ref: "data", path: "$.values[#-1]" }, { val: 3 }] },
		] as const satisfies readonly ConditionExpression[]) {
			expect((await evaluate(item, condition)).conditionOk, JSON.stringify(condition)).toBe(true);
		}
	});

	it("keeps complete text and byte data types distinct from JSONB", async () => {
		expect(
			(
				await evaluate(
					{ hashKey: "text", kind: "text", data: "héllo" },
					{ op: "eq", args: [{ fn: "size", args: [{ ref: "data" }] }, { val: 6 }] },
				)
			).conditionOk,
		).toBe(true);
		expect(
			(
				await evaluate(
					{ hashKey: "bytes", kind: "bytes", data: new Uint8Array([1, 2, 3]) },
					{ op: "begins_with", args: [{ ref: "data" }, { ref: "data" }] },
				)
			).conditionOk,
		).toBe(true);
		expect(
			(await evaluate({ hashKey: "json", kind: "json", data: [1, 2, 3] }, { op: "begins_with", args: [{ ref: "data" }, { ref: "data" }] }))
				.conditionOk,
		).toBe(false);
	});

	it("calls allowlisted SQLite scalar functions", async () => {
		const item: StoredFixture = { hashKey: "item", kind: "json", data: { email: " USER@EXAMPLE.COM ", score: -4 } };
		for (const condition of [
			{
				op: "eq",
				args: [
					{ fn: "sqlite.lower", args: [{ fn: "sqlite.trim", args: [{ ref: "data", path: "$.email" }] }] },
					{ val: "user@example.com" },
				],
			},
			{ op: "eq", args: [{ fn: "sqlite.abs", args: [{ ref: "data", path: "$.score" }] }, { val: 4 }] },
			{ op: "eq", args: [{ fn: "sqlite.typeof", args: [{ ref: "data" }] }, { val: "blob" }] },
		] as const satisfies readonly ConditionExpression[]) {
			expect((await evaluate(item, condition)).conditionOk, JSON.stringify(condition)).toBe(true);
		}
	});

	it.each(["gt", "gte", "ne", "contains"] as const)("does not match a binary key with text %s", async (op) => {
		const item: StoredFixture = { hashKey: "item", sortKey: new Uint8Array([0x61, 0x62]), kind: "text", data: "value" };
		const condition =
			op === "contains"
				? ({ op, args: [{ ref: "sortKey" }, { val: "a" }] } as const)
				: ({ op, args: [{ ref: "sortKey" }, { val: "a" }] } as const);
		expect((await evaluate(item, condition)).conditionOk).toBe(false);
	});

	it.each([
		{ op: "eq", args: [{ ref: "sortKey" }, { val: "a" }] },
		{ op: "lt", args: [{ ref: "sortKey" }, { val: "z" }] },
		{ op: "between", args: [{ ref: "sortKey" }, { val: "a" }, { val: "z" }] },
		{ op: "begins_with", args: [{ ref: "sortKey" }, { val: "a" }] },
	] as const satisfies readonly ConditionExpression[])("does not match an incompatible binary key for $op", async (condition) => {
		const item: StoredFixture = { hashKey: "item", sortKey: new Uint8Array([0x61, 0x62]), kind: "text", data: "value" };
		expect((await evaluate(item, condition)).conditionOk).toBe(false);
	});

	it("keeps the SQLite failure as the runtime error cause", async () => {
		const stub = PartitionDO.getByName(env.PARTITION_DO, `expression-runtime.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const plan = { ...compileConditionExpression({ op: "exists", args: [{ ref: "hashKey" }] }), sql: "no_such_function()" };
			const error = (() => {
				try {
					evaluateConditionPlan(state.storage, plan, KeyCodec.encode("item"), KeyCodec.encodeOptional(undefined));
					return undefined;
				} catch (caught) {
					return caught as Error;
				}
			})();
			expect(error?.name).toBe("ExpressionError");
			expect(error?.message).toMatch(/could not evaluate/);
			expect(error?.cause).toBeInstanceOf(Error);
		});
	});

	it("uses a primary-key lookup and reports existing versus missing row reads", async () => {
		const condition: ConditionExpression = { op: "exists", args: [{ ref: "hashKey" }] };
		const plan = compileConditionExpression(condition);
		const stub = PartitionDO.getByName(env.PARTITION_DO, `expression-plan.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const hashKey = KeyCodec.encode("item");
			const sortKey = KeyCodec.encodeOptional(undefined);
			putFixture(state.storage, hashKey, sortKey, { hashKey: "item", kind: "text", data: "value" });
			const queryPlan = state.storage.sql
				.exec<{
					detail: string;
				}>(`EXPLAIN QUERY PLAN ${composeConditionStatement(plan.sql)}`, hashKey, sortKey, ...materializeExpressionBindings(plan.bindings))
				.toArray();
			expect(
				queryPlan.some((row) => /SEARCH i USING (?:COVERING )?INDEX .*\(hk=\? AND sk=\?\)/.test(row.detail)),
				JSON.stringify(queryPlan),
			).toBe(true);
			const existing = evaluateConditionPlan(state.storage, plan, hashKey, sortKey);
			const missing = evaluateConditionPlan(state.storage, plan, KeyCodec.encode("missing"), sortKey);
			expect(existing.rowsRead).toBeGreaterThan(missing.rowsRead);
			expect(missing.rowsRead).toBe(0);
		});
	});
});
