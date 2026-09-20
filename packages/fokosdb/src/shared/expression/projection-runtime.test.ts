import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { JsonValue } from "../json-types.js";
import type { PartitionDO } from "../../server/do-partition.js";
import { testPartitionStub } from "../../../test/stub-helpers.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import { PartitionStore } from "../partition/partition-store.js";
import type { DataKind, QuerySelect } from "../types.js";
import { materializeExpressionBindings } from "./bindings.js";
import { compileConditionExpression, compileProjectionExpression, compileQueryExpression } from "./compiler.js";
import { composeProjectionStatement, composeQueryStatement, type CompiledProjectionPlan, type CompiledQueryPlan } from "./plan.js";
import { decodeProjectedRow, projectedItemFromWireRow, type ProjectedItem } from "./projection.js";
import { evaluateConditionPlan } from "./runtime.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import { estimateProjectedRowBytes } from "../partition/partition-store.js";
import { MAX_ITEM_BYTES } from "../transaction-limits.js";
import { MISSING_NULL_SEMANTIC_FIXTURES, PROJECTION_PRESENCE_FIXTURES } from "./test-fixtures.js";
import type { ConditionExpression, ProjectionExpression } from "./types.js";

type StoredFixture = {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;
	data: string | Uint8Array | JsonValue;
	kind: DataKind;
	ttlAt?: number;
};

function putFixture(storage: DurableObjectStorage, hashKey: KeyBytes, sortKey: KeyBytes, item: StoredFixture): void {
	new PartitionStore(storage).upsertItem({
		hk: hashKey,
		sk: sortKey,
		data: item.kind === "json" ? JSON.stringify(item.data) : (item.data as string | Uint8Array),
		kind: item.kind,
		ttlAt: item.ttlAt ?? null,
		txOrderTs: 1,
	});
}

function readProjected(state: DurableObjectState, plan: CompiledProjectionPlan, hk: KeyBytes, sk: KeyBytes): ProjectedItem | undefined {
	const row = state.storage.sql
		.exec<
			Record<string, SqlStorageValue>
		>(composeProjectionStatement(plan), ...materializeExpressionBindings(plan.bindings, "pool"), hk, sk)
		.toArray()[0];
	if (row === undefined) return undefined;
	return projectedItemFromWireRow(plan.names, decodeProjectedRow(row, plan.names.length));
}

async function project(item: StoredFixture | null, projection: readonly ProjectionExpression[]): Promise<ProjectedItem | undefined> {
	const stub = testPartitionStub(`expression-projection.${crypto.randomUUID()}`);
	return await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
		const hashKey = KeyCodec.encode(item?.hashKey ?? "missing-hash-key");
		const sortKey = item?.sortKey === undefined ? KeyCodec.encodeOptional(undefined) : KeyCodec.encode(item.sortKey);
		if (item) putFixture(state.storage, hashKey, sortKey, item);
		return readProjected(state, compileProjectionExpression(projection), hashKey, sortKey);
	});
}

describe("projected point read", () => {
	it.each(PROJECTION_PRESENCE_FIXTURES)("$name", async ({ item, projection, expected }) => {
		const stub = testPartitionStub(`expression-projection.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const hashKey = KeyCodec.encode(item.hashKey);
			const sortKey = item.sortKey === undefined ? KeyCodec.encodeOptional(undefined) : KeyCodec.encode(item.sortKey);
			putFixture(state.storage, hashKey, sortKey, item);
			const plan = compileProjectionExpression([projection]);
			const record = readProjected(state, plan, hashKey, sortKey);
			expect(record).toBeDefined();
			if (expected.present) {
				expect(record![plan.names[0]]).toEqual(expected.value);
			} else {
				expect(plan.names[0] in record!).toBe(false);
			}
		});
	});

	it("returns undefined for an absent row", async () => {
		expect(await project(null, [{ expr: { ref: "hashKey" } }])).toBeUndefined();
	});

	it("decodes every JSON kind through the cell pair", async () => {
		const item: StoredFixture = {
			hashKey: "item",
			sortKey: "s",
			kind: "json",
			data: {
				text: "héllo",
				integer: 7,
				real: 1.5,
				flag: true,
				off: false,
				none: null,
				nested: { a: 1 },
				values: [1, 2, 3],
			},
		};
		const projection: readonly ProjectionExpression[] = [
			{ expr: { ref: "data", path: "$.text" } },
			{ expr: { ref: "data", path: "$.integer" } },
			{ expr: { ref: "data", path: "$.real" } },
			{ expr: { ref: "data", path: "$.flag" } },
			{ expr: { ref: "data", path: "$.off" } },
			{ expr: { ref: "data", path: "$.none" } },
			{ expr: { ref: "data", path: "$.nested" } },
			{ expr: { ref: "data", path: "$.values" } },
			{ expr: { ref: "data", path: "$.missing" } },
			{ expr: { ref: "data", path: "$.values[#-1]" } },
			{ expr: { ref: "data" } },
		];
		const record = await project(item, projection);
		expect(record).toEqual({
			"$.text": "héllo",
			"$.integer": 7,
			"$.real": 1.5,
			"$.flag": true,
			"$.off": false,
			"$.none": null,
			"$.nested": { a: 1 },
			"$.values": [1, 2, 3],
			"$.values[#-1]": 3,
			data: item.data,
		});
		expect("$.missing" in record!).toBe(false);
	});

	it("decodes text and byte data and omits a path on either", async () => {
		expect(await project({ hashKey: "item", kind: "text", data: "héllo" }, [{ expr: { ref: "data" } }])).toEqual({ data: "héllo" });
		expect(await project({ hashKey: "item", kind: "text", data: "héllo" }, [{ expr: { ref: "data", path: "$.x" } }])).toEqual({});
		const bytes = new Uint8Array([1, 2, 3]);
		expect(await project({ hashKey: "item", kind: "bytes", data: bytes }, [{ expr: { ref: "data" } }])).toEqual({ data: bytes });
		expect(await project({ hashKey: "item", kind: "bytes", data: bytes }, [{ expr: { ref: "data", path: "$.x" } }])).toEqual({});
	});

	it("decodes keys, version, and TTL at the public boundary", async () => {
		const projection: readonly ProjectionExpression[] = [
			{ expr: { ref: "hashKey" } },
			{ expr: { ref: "sortKey" } },
			{ expr: { ref: "v" } },
			{ expr: { ref: "ttlAt" } },
		];
		expect(await project({ hashKey: "item", sortKey: "s", kind: "text", data: "v", ttlAt: 2_000_000_000 }, projection)).toEqual({
			hashKey: "item",
			sortKey: "s",
			v: 1,
			ttlAt: 2_000_000_000,
		});

		// A binary key crosses the boundary as untagged content bytes.
		expect(
			await project({ hashKey: new Uint8Array([0x61]), sortKey: new Uint8Array([0x61, 0x62]), kind: "text", data: "v" }, [
				{ expr: { ref: "hashKey" } },
				{ expr: { ref: "sortKey" } },
			]),
		).toEqual({ hashKey: new Uint8Array([0x61]), sortKey: new Uint8Array([0x61, 0x62]) });

		expect(await project({ hashKey: "item", kind: "text", data: "v" }, [{ expr: { ref: "sortKey" } }])).toEqual({});
		expect(await project({ hashKey: "item", kind: "text", data: "v" }, [{ expr: { ref: "ttlAt" } }])).toEqual({});
	});

	it("evaluates functions and literals", async () => {
		const item: StoredFixture = { hashKey: "item", kind: "json", data: { text: "héllo", values: [1, 2, 3], n: 7 } };
		const record = await project(item, [
			{ expr: { fn: "sqlite.upper", args: [{ ref: "data", path: "$.text" }] }, as: "upper" },
			{ expr: { fn: "sqlite.upper", args: [{ ref: "data", path: "$.missing" }] }, as: "upperMissing" },
			{ expr: { fn: "size", args: [{ ref: "data", path: "$.values" }] }, as: "count" },
			{ expr: { fn: "size", args: [{ ref: "data", path: "$.n" }] }, as: "sizeOfNumber" },
			{ expr: { fn: "attribute_type", args: [{ ref: "data", path: "$.values" }] }, as: "type" },
			{ expr: { fn: "sqlite.unhex", args: [{ val: "6162" }] }, as: "blob" },
			{ expr: { val: "x" }, as: "text" },
			{ expr: { val: 3 }, as: "number" },
			{ expr: { val: true }, as: "boolean" },
			{ expr: { val: null }, as: "null" },
			{ expr: { b64: "AQI=" }, as: "literal" },
		]);
		expect(record).toEqual({
			// SQLite upper() folds ASCII only.
			upper: "HéLLO",
			upperMissing: null,
			count: 3,
			type: "array",
			blob: new Uint8Array([0x61, 0x62]),
			text: "x",
			number: 3,
			boolean: true,
			null: null,
			literal: new Uint8Array([1, 2]),
		});
		expect("sizeOfNumber" in record!).toBe(false);
	});

	it("passes a value through the pass-through functions with its own type", async () => {
		const record = await project({ hashKey: "item", kind: "json", data: { flag: true, none: null, arr: [1, "x"], obj: { a: 1 }, n: 7 } }, [
			{ expr: { fn: "sqlite.coalesce", args: [{ ref: "data", path: "$.missing" }, { val: false }] }, as: "coalesceFalse" },
			{ expr: { fn: "sqlite.coalesce", args: [{ ref: "data", path: "$.arr" }, { val: 0 }] }, as: "coalesceArr" },
			{ expr: { fn: "sqlite.ifnull", args: [{ ref: "data", path: "$.none" }, { val: true }] }, as: "ifnullNone" },
			{ expr: { fn: "sqlite.nullif", args: [{ ref: "data", path: "$.n" }, { val: 7 }] }, as: "nullifHit" },
			{ expr: { fn: "sqlite.nullif", args: [{ ref: "data", path: "$.n" }, { val: 8 }] }, as: "nullifMiss" },
			{ expr: { fn: "sqlite.iif", args: [{ val: 1 }, { ref: "data", path: "$.flag" }, { val: 0 }] }, as: "iifTrue" },
			{ expr: { fn: "sqlite.iif", args: [{ val: 0 }, { ref: "data", path: "$.flag" }, { ref: "data", path: "$.obj" }] }, as: "iifObj" },
		]);
		expect(record).toEqual({
			coalesceFalse: false,
			coalesceArr: [1, "x"],
			ifnullNone: true,
			nullifHit: null,
			nullifMiss: 7,
			iifTrue: true,
			iifObj: { a: 1 },
		});
	});

	it("evaluates a pass-through function inside a condition", async () => {
		const stub = testPartitionStub(`expression-projection.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const hashKey = KeyCodec.encode("item");
			const sortKey = KeyCodec.encodeOptional(undefined);
			putFixture(state.storage, hashKey, sortKey, { hashKey: "item", kind: "json", data: { f: 1 } });
			const condition: ConditionExpression = {
				op: "eq",
				args: [{ fn: "sqlite.coalesce", args: [{ ref: "data", path: "$.missing" }, { val: false }] }, { val: false }],
			};
			expect(evaluateConditionPlan(state.storage, compileConditionExpression(condition), hashKey, sortKey).conditionOk).toBe(true);
		});
	});

	it("sees a missing reference as NULL inside a function argument", async () => {
		const record = await project({ hashKey: "item", kind: "json", data: { n: 7 }, ttlAt: 2_000_000_000 }, [
			{ expr: { fn: "sqlite.nullif", args: [{ ref: "v" }, { val: "1" }] }, as: "nullifAffinity" },
			{ expr: { fn: "sqlite.nullif", args: [{ ref: "ttlAt" }, { val: "2000000000" }] }, as: "nullifTtl" },
			{ expr: { fn: "sqlite.nullif", args: [{ val: 1 }, { val: 1 }] }, as: "nullifEqual" },
			{ expr: { fn: "sqlite.iif", args: [{ val: 1 }, { ref: "data", path: "$.missing" }, { val: 0 }] }, as: "iifMissing" },
			{ expr: { fn: "sqlite.iif", args: [{ val: 1 }, { ref: "data", path: "$.missing" }] }, as: "iifTwoArgs" },
			{ expr: { fn: "sqlite.iif", args: [{ val: 0 }, { val: 0 }, { ref: "data", path: "$.missing" }] }, as: "iifFalseBranch" },
			{ expr: { fn: "sqlite.coalesce", args: [{ ref: "sortKey" }, { val: "x" }] }, as: "coalesceKey" },
			{ expr: { fn: "sqlite.ifnull", args: [{ ref: "sortKey" }, { val: 7 }] }, as: "ifnullKey" },
			{ expr: { fn: "sqlite.length", args: [{ ref: "sortKey" }] }, as: "lengthKey" },
		]);
		expect(record).toEqual({
			nullifAffinity: 1,
			nullifTtl: 2_000_000_000,
			nullifEqual: null,
			iifMissing: null,
			iifTwoArgs: null,
			iifFalseBranch: null,
			coalesceKey: "x",
			ifnullKey: 7,
			lengthKey: null,
		});
	});

	it("evaluates pass-through conditions against an absent sort key", async () => {
		const stub = testPartitionStub(`expression-projection.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const hashKey = KeyCodec.encode("item");
			const sortKey = KeyCodec.encodeOptional(undefined);
			putFixture(state.storage, hashKey, sortKey, { hashKey: "item", kind: "json", data: { n: 7 }, ttlAt: 2_000_000_000 });
			const conditions: readonly ConditionExpression[] = [
				{ op: "eq", args: [{ fn: "sqlite.nullif", args: [{ ref: "v" }, { val: "1" }] }, { val: 1 }] },
				{ op: "eq", args: [{ fn: "sqlite.iif", args: [{ val: 1 }, { ref: "data", path: "$.missing" }, { val: 0 }] }, { val: null }] },
				{ op: "eq", args: [{ fn: "sqlite.coalesce", args: [{ ref: "sortKey" }, { val: "x" }] }, { val: "x" }] },
			];
			for (const condition of conditions) {
				expect(
					evaluateConditionPlan(state.storage, compileConditionExpression(condition), hashKey, sortKey).conditionOk,
					JSON.stringify(condition),
				).toBe(true);
			}
		});
	});

	it("decodes a root JSON scalar and a root array through the whole-data cell", async () => {
		const wholeData: readonly ProjectionExpression[] = [{ expr: { ref: "data" } }];
		expect(await project({ hashKey: "i", kind: "json", data: 5 }, wholeData)).toEqual({ data: 5 });
		expect(await project({ hashKey: "i", kind: "json", data: "str" }, wholeData)).toEqual({ data: "str" });
		expect(await project({ hashKey: "i", kind: "json", data: true }, wholeData)).toEqual({ data: true });
		expect(await project({ hashKey: "i", kind: "json", data: null }, wholeData)).toEqual({ data: null });
		expect(await project({ hashKey: "i", kind: "json", data: [1, "x"] }, wholeData)).toEqual({ data: [1, "x"] });
	});

	it("reads a plan with no descriptor and binds the empty pool", async () => {
		const stub = testPartitionStub(`expression-projection.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const hashKey = KeyCodec.encode("item");
			const sortKey = KeyCodec.encodeOptional(undefined);
			putFixture(state.storage, hashKey, sortKey, { hashKey: "item", kind: "text", data: "v" });
			const plan = compileProjectionExpression([{ expr: { ref: "hashKey" } }]);
			expect(plan.bindingCount).toBe(0);
			expect(materializeExpressionBindings(plan.bindings, "pool")).toEqual(["[]"]);
			expect(readProjected(state, plan, hashKey, sortKey)).toEqual({ hashKey: "item" });
		});
	});
});

describe("pool and direct layouts agree", () => {
	const jsonItem = (data: JsonValue): StoredFixture => ({ hashKey: "hk", data, kind: "json" });
	const binarySortKeyItem: StoredFixture = { hashKey: "item", sortKey: new Uint8Array([0x61, 0x62]), kind: "text", data: "value" };
	const byteDataItem: StoredFixture = { hashKey: "item", kind: "bytes", data: new Uint8Array([1, 2, 3]) };

	const cases: readonly { name: string; item: StoredFixture; condition: ConditionExpression; expected?: boolean }[] = [
		...MISSING_NULL_SEMANTIC_FIXTURES.filter((fixture) => fixture.item !== null).map((fixture) => ({
			name: fixture.name,
			item: fixture.item as StoredFixture,
			condition: fixture.condition,
		})),
		// keyText: a text literal against a key.
		{
			name: "text sortKey eq",
			item: { hashKey: "i", sortKey: "ab", kind: "text", data: "v" },
			condition: { op: "eq", args: [{ ref: "sortKey" }, { val: "ab" }] },
		},
		// keyB64: a byte literal against a binary key binds encoded key bytes.
		{ name: "binary sortKey eq b64", item: binarySortKeyItem, condition: { op: "eq", args: [{ ref: "sortKey" }, { b64: "YWI=" }] } },
		{
			name: "binary sortKey in b64",
			item: binarySortKeyItem,
			condition: { op: "in", args: [{ ref: "sortKey" }, { b64: "YWM=" }, { b64: "YWI=" }] },
		},
		// b64: a byte literal against byte data binds untagged content bytes.
		{ name: "byte data eq b64", item: byteDataItem, condition: { op: "eq", args: [{ ref: "data" }, { b64: "AQID" }] } },
		{ name: "byte data contains b64", item: byteDataItem, condition: { op: "contains", args: [{ ref: "data" }, { b64: "Ag==" }] } },
		{ name: "eq val true", item: jsonItem({ f: true }), condition: { op: "eq", args: [{ ref: "data", path: "$.f" }, { val: true }] } },
		{ name: "eq val null", item: jsonItem({ f: null }), condition: { op: "eq", args: [{ ref: "data", path: "$.f" }, { val: null }] } },
		{ name: "eq val 3", item: jsonItem({ f: 3 }), condition: { op: "eq", args: [{ ref: "data", path: "$.f" }, { val: 3 }] } },
		{ name: "eq val 1.5", item: jsonItem({ f: 1.5 }), condition: { op: "eq", args: [{ ref: "data", path: "$.f" }, { val: 1.5 }] } },
		{
			name: "a path comparison",
			item: jsonItem({ a: 1, b: 1 }),
			condition: {
				op: "eq",
				args: [
					{ ref: "data", path: "$.a" },
					{ ref: "data", path: "$.b" },
				],
			},
		},
		{
			name: "a text in with three choices",
			item: jsonItem({ s: "b" }),
			condition: { op: "in", args: [{ ref: "data", path: "$.s" }, { val: "a" }, { val: "b" }, { val: "c" }] },
		},
		// A text literal never equals a binary key: the stored key carries the binary tag.
		{
			name: "keyText against a binary sortKey",
			item: binarySortKeyItem,
			condition: { op: "eq", args: [{ ref: "sortKey" }, { val: "ab" }] },
			expected: false,
		},
		...[0.1, 5e-324, 1.7976931348623157e308, 1e21, -1.5, 2 ** 53].map(
			(n): { name: string; item: StoredFixture; condition: ConditionExpression; expected: boolean } => ({
				name: `eq number ${n}`,
				item: jsonItem({ f: n }),
				condition: { op: "eq", args: [{ ref: "data", path: "$.f" }, { val: n }] },
				expected: true,
			}),
		),
	];

	it.each(cases)("$name", async ({ item, condition, expected }) => {
		const stub = testPartitionStub(`expression-pool.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const hashKey = KeyCodec.encode(item.hashKey);
			const sortKey = item.sortKey === undefined ? KeyCodec.encodeOptional(undefined) : KeyCodec.encode(item.sortKey);
			putFixture(state.storage, hashKey, sortKey, item);
			const direct = evaluateConditionPlan(state.storage, compileConditionExpression(condition), hashKey, sortKey);
			if (expected !== undefined) expect(direct.conditionOk).toBe(expected);
			const poolPlan = compileQueryExpression({ filter: condition });
			// Every plan parameter under the pool layout reads the one pool parameter ?1.
			expect([...poolPlan.filterSql!.matchAll(/\?(\d+)/g)].map((match) => match[1]).every((n) => n === "1")).toBe(true);
			const rows = state.storage.sql
				.exec<{
					sk: ArrayBuffer;
					matched: number;
				}>(
					composeQueryStatement(poolPlan, { select: "count", direction: "asc", scanConditions: ["hk = ?"] }),
					...materializeExpressionBindings(poolPlan.bindings, "pool"),
					hashKey,
					10,
				)
				.toArray();
			expect(rows).toHaveLength(1);
			expect(rows[0].matched === 1).toBe(direct.conditionOk);
		});
	});
});

describe("query statement", () => {
	const item = (sortKey: string, data: JsonValue, ttlAt?: number): StoredFixture => ({ hashKey: "h", sortKey, data, kind: "json", ttlAt });

	function scan(state: DurableObjectState, plan: CompiledQueryPlan, select: QuerySelect): Record<string, SqlStorageValue>[] {
		return state.storage.sql
			.exec<
				Record<string, SqlStorageValue>
			>(composeQueryStatement(plan, { select, direction: "asc", scanConditions: ["hk = ?"] }), ...materializeExpressionBindings(plan.bindings, "pool"), KeyCodec.encode("h"), 10)
			.toArray();
	}

	it("marks rows by the filter and nulls projected cells on misses", async () => {
		const stub = testPartitionStub(`expression-query.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			for (const fixture of [item("a", { status: "x", n: 1 }), item("b", { status: "y", n: 2 }), item("c", { status: "x", n: 3 })]) {
				putFixture(state.storage, KeyCodec.encode("h"), KeyCodec.encode(fixture.sortKey!), fixture);
			}
			const plan = compileQueryExpression({
				filter: { op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "x" }] },
				projection: [{ expr: { ref: "sortKey" } }, { expr: { ref: "data", path: "$.n" } }, { expr: { ref: "data", path: "$.missing" } }],
			});
			const scanned = scan(state, plan, "projection");
			expect(scanned.map((row) => row.matched)).toEqual([1, 0, 1]);
			const names = plan.projection!.names;
			expect(projectedItemFromWireRow(names, decodeProjectedRow(scanned[0], names.length))).toEqual({ sortKey: "a", "$.n": 1 });
			expect(projectedItemFromWireRow(names, decodeProjectedRow(scanned[2], names.length))).toEqual({ sortKey: "c", "$.n": 3 });
			for (const column of ["p0", "t0", "p1", "t1", "p2", "t2"]) expect(scanned[1][column]).toBeNull();

			const filterOnly = compileQueryExpression({ filter: { op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "x" }] } });
			const complete = scan(state, filterOnly, "projection");
			expect(complete.map((row) => row.matched)).toEqual([1, 0, 1]);
			for (const column of ["hk", "data", "data_kind", "v"]) expect(complete[1][column]).toBeNull();
			expect(complete[0].data).toBe(JSON.stringify({ status: "x", n: 1 }));
			expect(complete[0].v).toBe(1);

			expect(scan(state, plan, "count").map((row) => row.matched)).toEqual([1, 0, 1]);
		});
	});

	it("runs a 100-choice in filter with a maximum-size projection in one statement", async () => {
		const stub = testPartitionStub(`expression-query.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			putFixture(state.storage, KeyCodec.encode("h"), KeyCodec.encode("a"), item("a", { n: 5 }));
			const plan = compileQueryExpression({
				filter: {
					op: "in",
					args: [{ ref: "v" }, ...Array.from({ length: EXPRESSION_LIMITS.inChoices }, (_, i) => ({ val: i }))],
				} as unknown as ConditionExpression,
				projection: [
					{ expr: { ref: "sortKey" }, as: "k0" },
					{ expr: { ref: "data", path: "$.n" }, as: "k1" },
					{ expr: { ref: "v" }, as: "k2" },
					...Array.from({ length: EXPRESSION_LIMITS.projectionEntries - 3 }, (_, i) => ({
						expr: { val: i + 3 } as const,
						as: `k${i + 3}`,
					})),
				],
			});
			expect(plan.completeBindingCount).toBeLessThanOrEqual(EXPRESSION_LIMITS.completeStatementBindings);

			const scanned = scan(state, plan, "projection");
			expect(scanned.map((row) => row.matched)).toEqual([1]);
			// The CTE has 3 fixed columns plus 2 per entry: 3 + 2 × 48 = 99, under the 100-column cap.
			const record = projectedItemFromWireRow(plan.projection!.names, decodeProjectedRow(scanned[0], plan.projection!.names.length));
			expect(Object.keys(record)).toHaveLength(EXPRESSION_LIMITS.projectionEntries);
			expect(record).toMatchObject({ k0: "a", k1: 5, k2: 1, k47: 47 });
		});
	});

	it("binds an empty pool for a plan with no descriptor", async () => {
		const stub = testPartitionStub(`expression-query.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			putFixture(state.storage, KeyCodec.encode("h"), KeyCodec.encode("a"), item("a", "v", 2_000_000_000));
			putFixture(state.storage, KeyCodec.encode("h"), KeyCodec.encode("b"), item("b", "v"));
			const plan = compileQueryExpression({
				filter: { op: "exists", args: [{ ref: "ttlAt" }] },
				projection: [{ expr: { ref: "hashKey" } }],
			});
			expect(plan.bindingCount).toBe(0);
			expect(materializeExpressionBindings(plan.bindings, "pool")).toEqual(["[]"]);
			expect(scan(state, plan, "projection").map((row) => row.matched)).toEqual([1, 0]);
		});
	});

	it.each(["asc", "desc"] as const)(
		"the CTE statement searches the items index once, without materializing or sorting, in %s order",
		async (direction) => {
			const stub = testPartitionStub(`expression-query.${crypto.randomUUID()}`);
			await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
				for (const fixture of [item("a", { n: 1 }), item("b", { n: 2 }), item("c", { n: 3 })]) {
					putFixture(state.storage, KeyCodec.encode("h"), KeyCodec.encode(fixture.sortKey!), fixture);
				}
				const plan = compileQueryExpression({
					projection: [{ expr: { ref: "sortKey" } }, { expr: { ref: "data", path: "$.n" } }],
				});
				const sql = composeQueryStatement(plan, {
					select: "projection",
					direction,
					scanConditions: ["hk = ?", "sk >= ?"],
				});
				const details = state.storage.sql
					.exec<{ detail: string }>(
						`EXPLAIN QUERY PLAN ${sql}`,
						...materializeExpressionBindings(plan.bindings, "pool"),
						KeyCodec.encode("h"),
						KeyCodec.encode("a"),
						10,
					)
					.toArray()
					.map((row) => row.detail);
				// The statement aliases the table to `i`, so the index search reads `SEARCH i`.
				expect(
					details.filter((detail) => /SEARCH (items|i)\b/.test(detail)),
					details.join(" | "),
				).toHaveLength(1);
				expect(
					details.some((detail) => /MATERIALIZE/.test(detail)),
					details.join(" | "),
				).toBe(false);
				expect(
					details.some((detail) => /USE TEMP B-TREE FOR ORDER BY/.test(detail)),
					details.join(" | "),
				).toBe(false);
			});
		},
	);
});

describe("projected point read — limits", () => {
	it("runs a maximum-size projection in one statement", async () => {
		const stub = testPartitionStub(`expression-projection.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const hashKey = KeyCodec.encode("h");
			const sortKey = KeyCodec.encode("a");
			putFixture(state.storage, hashKey, sortKey, { hashKey: "h", sortKey: "a", data: { n: 5 }, kind: "json" });
			// Workers SQLite allows 100 result columns; the point-read statement has 2 fixed columns
			// (v, ttl) plus 2 per entry, and the CTE query has 3 fixed plus 2 per entry. 48 entries is
			// the largest count under both caps.
			const plan = compileProjectionExpression([
				{ expr: { ref: "sortKey" }, as: "k0" },
				{ expr: { ref: "data", path: "$.n" }, as: "k1" },
				{ expr: { ref: "v" }, as: "k2" },
				...Array.from({ length: EXPRESSION_LIMITS.projectionEntries - 3 }, (_, i) => ({
					expr: { val: i + 3 } as const,
					as: `k${i + 3}`,
				})),
			]);
			expect(plan.completeBindingCount).toBeLessThanOrEqual(EXPRESSION_LIMITS.completeStatementBindings);

			const record = readProjected(state, plan, hashKey, sortKey);
			expect(Object.keys(record!)).toHaveLength(EXPRESSION_LIMITS.projectionEntries);
			expect(record).toMatchObject({ k0: "a", k1: 5, k2: 1, k47: 47 });
		});
	});

	it("a projected read of a 400 KiB item returns a wire row two orders of magnitude smaller", async () => {
		const stub = testPartitionStub(`expression-projection.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const hashKey = KeyCodec.encode("h");
			const sortKey = KeyCodec.encode("a");
			// The pad is sized so the measured row lands just under the item byte cap.
			const doc = { n: 7, pad: "x".repeat(MAX_ITEM_BYTES - 1024) };
			const data = JSON.stringify(doc);
			const store = new PartitionStore(state.storage);
			const storedBytes = store.measureItemBytes({ hk: hashKey, sk: sortKey, data, kind: "json" });
			expect(storedBytes).toBeLessThanOrEqual(MAX_ITEM_BYTES);
			expect(storedBytes).toBeGreaterThan(MAX_ITEM_BYTES - 4 * 1024);
			store.upsertItem({ hk: hashKey, sk: sortKey, data, kind: "json", ttlAt: null, txOrderTs: 0 });

			const plan = compileProjectionExpression([{ expr: { ref: "data", path: "$.n" } }, { expr: { ref: "sortKey" } }]);
			const row = state.storage.sql
				.exec<
					Record<string, SqlStorageValue>
				>(composeProjectionStatement(plan), ...materializeExpressionBindings(plan.bindings, "pool"), hashKey, sortKey)
				.toArray()[0];
			expect(row).toBeDefined();
			const wire = decodeProjectedRow(row, plan.names.length);
			expect(projectedItemFromWireRow(plan.names, wire)).toEqual({ "$.n": 7, sortKey: "a" });

			// Measured in workerd: 408,704 stored bytes against a 74-byte wire row, about 5,500 times
			// smaller. The projected read touches the same row. Only the returned payload shrinks.
			const projectedBytes = estimateProjectedRowBytes(wire);
			expect(projectedBytes * 100).toBeLessThan(storedBytes);
		});
	});
});
