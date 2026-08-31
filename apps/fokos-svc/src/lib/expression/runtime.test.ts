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
	ttlAt?: number;
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
		ttlAt: item.ttlAt ?? null,
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
			ttlAt: 2_000_000_000,
		};
		for (const [condition, expected] of [
			[{ op: "eq", args: [{ ref: "hashKey" }, { val: "item" }] }, true],
			[{ op: "eq", args: [{ ref: "sortKey" }, { val: "sort" }] }, true],
			[{ op: "eq", args: [{ ref: "v" }, { val: 1 }] }, true],
			[{ op: "eq", args: [{ ref: "ttlAt" }, { val: 2_000_000_000 }] }, true],
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

	it("matches a text key and never matches a binary key with the same content", async () => {
		const textItem: StoredFixture = { hashKey: "item", sortKey: "ab", kind: "text", data: "value" };
		const binaryItem: StoredFixture = { hashKey: "item", sortKey: new Uint8Array([0x61, 0x62]), kind: "text", data: "value" };
		// Every condition holds for the text sort key "ab". The binary sort key carries the same bytes but
		// is 0xFF-tagged and reports the "bytes" type, so a text literal can never match it.
		for (const condition of [
			{ op: "eq", args: [{ ref: "sortKey" }, { val: "ab" }] },
			{ op: "ne", args: [{ ref: "sortKey" }, { val: "a" }] },
			{ op: "lt", args: [{ ref: "sortKey" }, { val: "z" }] },
			{ op: "gt", args: [{ ref: "sortKey" }, { val: "a" }] },
			{ op: "gte", args: [{ ref: "sortKey" }, { val: "ab" }] },
			{ op: "between", args: [{ ref: "sortKey" }, { val: "a" }, { val: "z" }] },
			{ op: "begins_with", args: [{ ref: "sortKey" }, { val: "a" }] },
			{ op: "contains", args: [{ ref: "sortKey" }, { val: "b" }] },
		] as const satisfies readonly ConditionExpression[]) {
			const label = JSON.stringify(condition);
			expect((await evaluate(textItem, condition)).conditionOk, label).toBe(true);
			expect((await evaluate(binaryItem, condition)).conditionOk, label).toBe(false);
		}
	});

	it("compares binary keys against each other on untagged content", async () => {
		// The hash key is a prefix of the sort key once both lose the 0xFF tag.
		const item: StoredFixture = { hashKey: new Uint8Array([0x61]), sortKey: new Uint8Array([0x61, 0x62]), kind: "text", data: "value" };
		for (const condition of [
			{ op: "exists", args: [{ ref: "sortKey" }] },
			{ op: "eq", args: [{ fn: "attribute_type", args: [{ ref: "sortKey" }] }, { val: "bytes" }] },
			{ op: "eq", args: [{ fn: "size", args: [{ ref: "sortKey" }] }, { val: 2 }] },
			{ op: "eq", args: [{ ref: "sortKey" }, { ref: "sortKey" }] },
			{ op: "gt", args: [{ ref: "sortKey" }, { ref: "hashKey" }] },
			{ op: "begins_with", args: [{ ref: "sortKey" }, { ref: "hashKey" }] },
			{ op: "contains", args: [{ ref: "sortKey" }, { ref: "hashKey" }] },
		] as const satisfies readonly ConditionExpression[]) {
			expect((await evaluate(item, condition)).conditionOk, JSON.stringify(condition)).toBe(true);
		}
	});

	it("matches binary key content through a blob literal", async () => {
		const item: StoredFixture = { hashKey: new Uint8Array([0x61]), sortKey: new Uint8Array([0x61, 0x62]), kind: "text", data: "value" };
		for (const condition of [
			{ op: "eq", args: [{ ref: "sortKey" }, { fn: "sqlite.unhex", args: [{ val: "6162" }] }] },
			{ op: "eq", args: [{ ref: "hashKey" }, { fn: "sqlite.unhex", args: [{ val: "61" }] }] },
			{ op: "begins_with", args: [{ ref: "sortKey" }, { fn: "sqlite.unhex", args: [{ val: "61" }] }] },
			{ op: "contains", args: [{ ref: "sortKey" }, { fn: "sqlite.unhex", args: [{ val: "62" }] }] },
			{ op: "eq", args: [{ fn: "sqlite.hex", args: [{ ref: "sortKey" }] }, { val: "6162" }] },
			{ op: "begins_with", args: [{ fn: "sqlite.hex", args: [{ ref: "sortKey" }] }, { val: "61" }] },
		] as const satisfies readonly ConditionExpression[]) {
			expect((await evaluate(item, condition)).conditionOk, JSON.stringify(condition)).toBe(true);
		}
		for (const condition of [
			{ op: "eq", args: [{ ref: "sortKey" }, { fn: "sqlite.unhex", args: [{ val: "6163" }] }] },
			{ op: "begins_with", args: [{ ref: "sortKey" }, { fn: "sqlite.unhex", args: [{ val: "62" }] }] },
			{ op: "eq", args: [{ ref: "sortKey" }, { fn: "sqlite.unhex", args: [{ val: "zz" }] }] },
		] as const satisfies readonly ConditionExpression[]) {
			expect((await evaluate(item, condition)).conditionOk, JSON.stringify(condition)).toBe(false);
		}
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

	it("matches binary keys through byte literals and never matches a text key", async () => {
		const bytes = (...values: number[]) => new Uint8Array(values).toBase64();
		// The text sort key "ab" and the binary sort key carry identical content. Every condition below
		// holds for the binary key only: the logical key type keeps a byte literal off a text key.
		const binaryItem: StoredFixture = { hashKey: "item", sortKey: new Uint8Array([0x61, 0x62]), kind: "text", data: "value" };
		const textItem: StoredFixture = { hashKey: "item", sortKey: "ab", kind: "text", data: "value" };
		for (const condition of [
			{ op: "eq", args: [{ ref: "sortKey" }, { b64: bytes(0x61, 0x62) }] },
			{ op: "ne", args: [{ ref: "sortKey" }, { b64: bytes(0x61) }] },
			{ op: "lt", args: [{ ref: "sortKey" }, { b64: bytes(0x7a) }] },
			{ op: "lte", args: [{ ref: "sortKey" }, { b64: bytes(0x61, 0x62) }] },
			{ op: "gt", args: [{ ref: "sortKey" }, { b64: bytes(0x61) }] },
			{ op: "gte", args: [{ ref: "sortKey" }, { b64: bytes(0x61, 0x62) }] },
			{ op: "between", args: [{ ref: "sortKey" }, { b64: bytes(0x61) }, { b64: bytes(0x7a) }] },
			{ op: "in", args: [{ ref: "sortKey" }, { b64: bytes(0x63) }, { b64: bytes(0x61, 0x62) }] },
			{ op: "begins_with", args: [{ ref: "sortKey" }, { b64: bytes(0x61) }] },
			{ op: "contains", args: [{ ref: "sortKey" }, { b64: bytes(0x62) }] },
		] as const satisfies readonly ConditionExpression[]) {
			const label = JSON.stringify(condition);
			expect((await evaluate(binaryItem, condition)).conditionOk, label).toBe(true);
			expect((await evaluate(textItem, condition)).conditionOk, label).toBe(false);
		}
	});

	it("searches binary key content without the 0xFF tag", async () => {
		// The needle sits at the end of the key, so a tagged search would find nothing.
		const item: StoredFixture = { hashKey: "item", sortKey: new Uint8Array([0x61, 0x62]), kind: "text", data: "value" };
		expect(
			(await evaluate(item, { op: "contains", args: [{ ref: "sortKey" }, { b64: new Uint8Array([0x62]).toBase64() }] })).conditionOk,
		).toBe(true);
		expect(
			(await evaluate(item, { op: "contains", args: [{ ref: "sortKey" }, { b64: new Uint8Array([0xff, 0x61]).toBase64() }] })).conditionOk,
		).toBe(false);
	});

	it("matches byte data and never matches a JSONB row", async () => {
		const byteItem: StoredFixture = { hashKey: "item", kind: "bytes", data: new Uint8Array([1, 2, 3]) };
		const jsonItem: StoredFixture = { hashKey: "item", kind: "json", data: [1, 2, 3] };
		for (const condition of [
			{ op: "eq", args: [{ ref: "data" }, { b64: "AQID" }] },
			{ op: "ne", args: [{ ref: "data" }, { b64: "AQI=" }] },
			{ op: "begins_with", args: [{ ref: "data" }, { b64: "AQI=" }] },
			{ op: "contains", args: [{ ref: "data" }, { b64: "Ag==" }] },
		] as const satisfies readonly ConditionExpression[]) {
			const label = JSON.stringify(condition);
			expect((await evaluate(byteItem, condition)).conditionOk, label).toBe(true);
			expect((await evaluate(jsonItem, condition)).conditionOk, label).toBe(false);
		}
	});

	it("uses a primary-key lookup for a byte key literal", async () => {
		const plan = compileConditionExpression({ op: "eq", args: [{ ref: "sortKey" }, { b64: "YWI=" }] });
		const stub = PartitionDO.getByName(env.PARTITION_DO, `expression-plan.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const hashKey = KeyCodec.encode("item");
			const sortKey = KeyCodec.encode(new Uint8Array([0x61, 0x62]));
			const queryPlan = state.storage.sql
				.exec<{
					detail: string;
				}>(`EXPLAIN QUERY PLAN ${composeConditionStatement(plan.sql)}`, hashKey, sortKey, ...materializeExpressionBindings(plan.bindings))
				.toArray();
			expect(
				queryPlan.some((row) => /SEARCH i USING (?:COVERING )?INDEX .*\(hk=\? AND sk=\?\)/.test(row.detail)),
				JSON.stringify(queryPlan),
			).toBe(true);
		});
	});
});
