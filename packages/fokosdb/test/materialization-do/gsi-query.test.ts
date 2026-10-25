import { describe, expect, it, vi } from "vitest";
import { env } from "cloudflare:test";
import { FokosDB } from "../../src/client/db.js";
import { materializationStubByName, partitionStubByName } from "../../src/shared/do-stubs.js";
import { gsiItemSortKey } from "../../src/shared/gsi/sort-key.js";
import { createTableConfig, gsiTableConfig, materializationTableConfig } from "../../src/shared/partition-context.js";
import { FokosShardingClient } from "../../src/sharding/client.js";
import { FokosRouter } from "../../src/sharding/router.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";

function makeDB() {
	return new FokosDB({
		table: {
			name: `gsiq.${crypto.randomUUID()}`,
			ns: "PARTITION_DO",
			nsTx: "TRANSACTION_COORDINATOR_DO",
			nsMat: "GSI_MATERIALIZATION_DO",
			rootTreesN: 2,
			hashSplitN: 2,
		},
		captureChanges: true,
	});
}

const keysOf = (items: Array<{ sortKey?: unknown; baseHashKey?: unknown }>) =>
	items.map((i) => `${String(i.sortKey)}/${String(i.baseHashKey)}`);

describe("db.gsi(indexName).queryItems", () => {
	it("queries the GSI sort key with each condition, and decodes the base keys", async () => {
		const db = makeDB();
		const orders = [
			["o1", "t1"],
			["o2", "t2"],
			["o3", "t2"],
			["o4", "t3"],
		] as const;
		for (const [hashKey, at] of orders) {
			await db.putItem({ hashKey, sortKey: "s", data: { status: "OPEN", at, owner: "u" } });
		}
		const query = (sortKeyCondition?: Parameters<FokosDB["queryItems"]>[0]["queries"][number]["sortKeyCondition"]) =>
			db.gsi("byStatus").queryItems({ queries: [{ hashKey: "OPEN", sortKeyCondition }] });
		await vi.waitFor(async () => expect((await query()).items).toHaveLength(4));

		const all = await query();
		expect(all.items[0]).toMatchObject({ hashKey: "OPEN", sortKey: "t1", baseHashKey: "o1", baseSortKey: "s", data: { at: "t1" } });
		expect(keysOf(all.items)).toEqual(["t1/o1", "t2/o2", "t2/o3", "t3/o4"]);
		expect(keysOf((await query({ op: "eq", value: "t2" })).items)).toEqual(["t2/o2", "t2/o3"]);
		expect(keysOf((await query({ op: "gt", value: "t2" })).items)).toEqual(["t3/o4"]);
		expect(keysOf((await query({ op: "gte", value: "t2" })).items)).toEqual(["t2/o2", "t2/o3", "t3/o4"]);
		expect(keysOf((await query({ op: "lt", value: "t2" })).items)).toEqual(["t1/o1"]);
		expect(keysOf((await query({ op: "lte", value: "t2" })).items)).toEqual(["t1/o1", "t2/o2", "t2/o3"]);
		expect(keysOf((await query({ op: "between", lower: "t2", upper: "t3" })).items)).toEqual(["t2/o2", "t2/o3", "t3/o4"]);
		expect(keysOf((await query({ op: "begins_with", prefix: "t" })).items)).toHaveLength(4);

		// One item per page: the cursor keeps the physical sort key, so the entries of one GSI sort key do not repeat.
		const seen: string[] = [];
		let cursor: string | undefined;
		do {
			const page = await db
				.gsi("byStatus")
				.queryItems({ queries: [{ hashKey: "OPEN", sortKeyCondition: { op: "eq", value: "t2" } }], limit: 1, cursor });
			seen.push(...keysOf(page.items));
			cursor = page.cursor;
		} while (cursor !== undefined);
		expect(seen).toEqual(["t2/o2", "t2/o3"]);
	});

	it('a filter and a projection read the GSI sort key with { ref: "sortKey" }', async () => {
		const db = makeDB();
		await db.putItem({ hashKey: "o1", sortKey: "s", data: { status: "OPEN", at: "t1", owner: "u" } });
		await db.putItem({ hashKey: "o2", sortKey: "s", data: { status: "OPEN", at: "t2", owner: "u" } });
		await vi.waitFor(async () => expect((await db.gsi("byStatus").queryItems({ queries: [{ hashKey: "OPEN" }] })).items).toHaveLength(2));
		const page = await db.gsi("byStatus").queryItems({
			queries: [{ hashKey: "OPEN" }],
			filter: { op: "eq", args: [{ ref: "sortKey" }, { val: "t2" }] },
			projection: [
				{ expr: { ref: "sortKey" }, as: "at" },
				{ expr: { ref: "data", path: "$.owner" }, as: "owner" },
			],
		});
		expect(page.items).toEqual([{ at: "t2", owner: "u" }]);
		// A GSI with no sort key has no sortKey value.
		await vi.waitFor(async () => expect((await db.gsi("byOwner").queryItems({ queries: [{ hashKey: "u" }] })).items).toHaveLength(2));
		const missing = await db
			.gsi("byOwner")
			.queryItems({ queries: [{ hashKey: "u" }], filter: { op: "not_exists", args: [{ ref: "sortKey" }] } });
		expect(missing.items).toHaveLength(2);
	});

	it("destroy() deletes the base, the materialization, and each GSI table", async () => {
		const db = makeDB();
		await db.putItem({ hashKey: "o1", sortKey: "s", data: { status: "OPEN", at: "t1", owner: "u" } });
		await vi.waitFor(async () => expect((await db.gsi("byOwner").queryItems({ queries: [{ hashKey: "u" }] })).items).toHaveLength(1));
		await expect(db.destroy()).resolves.toEqual({ ok: true });
		expect((await db.gsi("byStatus").queryItems({ queries: [{ hashKey: "OPEN" }] })).items).toEqual([]);
		expect((await db.gsi("byOwner").queryItems({ queries: [{ hashKey: "u" }] })).items).toEqual([]);
		expect((await db.getItem({ hashKey: "o1", sortKey: "s" })).found).toBe(false);
		await db.destroy();
	});

	it("a destroy that stops after the first GSI table deletes the other GSI table when it runs again", async () => {
		const db = makeDB();
		await db.putItem({ hashKey: "o1", sortKey: "s", data: { status: "OPEN", at: "t1", owner: "u" } });
		await vi.waitFor(async () => {
			expect((await db.gsi("byStatus").queryItems({ queries: [{ hashKey: "OPEN" }] })).items).toHaveLength(1);
			expect((await db.gsi("byOwner").queryItems({ queries: [{ hashKey: "u" }] })).items).toHaveLength(1);
		});
		// The state of a destroy that stopped after the first GSI table: the base partitions are gone, the
		// materialization partitions are fenced, and only the byStatus GSI table is gone.
		const { topology, rangeConfig, policy, policyVersion } = createTableConfig({ table: db.options().table });
		const table = { topology, rangeConfig, policy, policyVersion };
		await new FokosShardingClient({ ...table, stub: (ctx, doName) => partitionStubByName(env, ctx, doName) }).destroy();
		const materializations = new FokosShardingClient({
			...materializationTableConfig(table),
			stub: (ctx, doName) => materializationStubByName(env, ctx, doName),
		});
		for await (const node of materializations.walk()) {
			await materializationStubByName(env, node.ctx, node.ctx.doName).fokosPrepareDestroy({
				rootContext: node.parent === null ? node.ctx : undefined,
			});
		}
		await new FokosShardingClient({
			...gsiTableConfig(table, "byStatus"),
			stub: (ctx, doName) => partitionStubByName(env, ctx, doName),
		}).destroy();
		expect((await db.gsi("byStatus").queryItems({ queries: [{ hashKey: "OPEN" }] })).items).toEqual([]);
		expect((await db.gsi("byOwner").queryItems({ queries: [{ hashKey: "u" }] })).items).toHaveLength(1);

		await expect(db.destroy()).resolves.toEqual({ ok: true });
		expect((await db.gsi("byStatus").queryItems({ queries: [{ hashKey: "OPEN" }] })).items).toEqual([]);
		expect((await db.gsi("byOwner").queryItems({ queries: [{ hashKey: "u" }] })).items).toEqual([]);
	});
});

describe("db.gsi(indexName).queryItems — sort key encodings", () => {
	/** A table whose GSI `idx` gets its entries straight from `gsiWriteEntries`. */
	async function seeded(entries: Array<{ gsiHK: string; gsiSK?: string | Uint8Array; baseHK: string }>) {
		const db = makeDB();
		const { topology, rangeConfig, policy, policyVersion } = createTableConfig({
			table: db.options().table,
		});
		const table = gsiTableConfig({ topology, rangeConfig, policy, policyVersion }, "idx");
		const router = new FokosRouter(table.topology, table.rangeConfig, table.policy, table.policyVersion);
		for (const [opIndex, e] of entries.entries()) {
			const ctx = router.rootContext(KeyCodec.encode(e.gsiHK));
			await partitionStubByName(env, ctx, ctx.doName).gsiWriteEntries(ctx, {
				producerId: "aa".repeat(32),
				epoch: 1,
				ancestorFences: [],
				items: [
					{
						opIndex,
						op: "put",
						hashKey: KeyCodec.encode(e.gsiHK),
						sortKey: gsiItemSortKey(KeyCodec.encodeOptional(e.gsiSK), KeyCodec.encode(e.baseHK), KeyCodec.encodeOptional(undefined)),
						data: JSON.stringify({ n: opIndex }),
						kind: "json",
						baseRevision: 1,
					},
				],
			});
		}
		return db.gsi("idx");
	}
	const bytes = (...b: number[]) => Uint8Array.from(b);

	it("binary GSI sort keys, with and without a 0x00 byte, sort, filter, and project", async () => {
		const gsi = await seeded([
			{ gsiHK: "h", gsiSK: bytes(1, 0, 2), baseHK: "a" },
			{ gsiHK: "h", gsiSK: bytes(1), baseHK: "b" },
			{ gsiHK: "h", gsiSK: bytes(1, 0), baseHK: "c" },
			{ gsiHK: "h", gsiSK: bytes(2), baseHK: "d" },
		]);
		const all = await gsi.queryItems({ queries: [{ hashKey: "h" }] });
		expect(all.items.map((i) => i.baseHashKey)).toEqual(["b", "c", "a", "d"]);
		expect(all.items.map((i) => [...(i.sortKey as Uint8Array)])).toEqual([[1], [1, 0], [1, 0, 2], [2]]);

		const projected = await gsi.queryItems({
			queries: [{ hashKey: "h" }],
			filter: { op: "eq", args: [{ ref: "sortKey" }, { b64: bytes(1, 0, 2).toBase64() }] },
			projection: [{ expr: { ref: "sortKey" }, as: "sk" }],
		});
		expect(projected.items).toEqual([{ sk: bytes(1, 0, 2) }]);
		const prefixed = await gsi.queryItems({ queries: [{ hashKey: "h", sortKeyCondition: { op: "begins_with", prefix: bytes(1, 0) } }] });
		expect(prefixed.items.map((i) => i.baseHashKey)).toEqual(["c", "a"]);
	});

	it("range conditions, descending scans, count, and more than one query", async () => {
		const gsi = await seeded([
			{ gsiHK: "h", gsiSK: "a", baseHK: "1" },
			{ gsiHK: "h", gsiSK: "b", baseHK: "2" },
			{ gsiHK: "h", gsiSK: "b", baseHK: "3" },
			{ gsiHK: "h", gsiSK: "c", baseHK: "4" },
			{ gsiHK: "g", gsiSK: "z", baseHK: "5" },
		]);
		const base = (page: { items: Array<{ baseHashKey: unknown }> }) => page.items.map((i) => i.baseHashKey);
		const range = (lower?: { value: string; inclusive: boolean }, upper?: { value: string; inclusive: boolean }) =>
			gsi.queryItems({ queries: [{ hashKey: "h", sortKeyCondition: { op: "range", lower, upper } }] });
		expect(base(await range({ value: "b", inclusive: false }))).toEqual(["4"]);
		expect(base(await range({ value: "b", inclusive: true }, { value: "b", inclusive: true }))).toEqual(["2", "3"]);
		expect(base(await range(undefined, { value: "b", inclusive: false }))).toEqual(["1"]);
		expect(base(await gsi.queryItems({ queries: [{ hashKey: "h", scanIndexForward: false }] }))).toEqual(["4", "3", "2", "1"]);
		expect((await gsi.queryItems({ queries: [{ hashKey: "h", sortKeyCondition: { op: "eq", value: "b" } }], select: "count" })).count).toBe(
			2,
		);
		expect(
			base(await gsi.queryItems({ queries: [{ hashKey: "h", sortKeyCondition: { op: "eq", value: "c" } }, { hashKey: "g" }] })),
		).toEqual(["4", "5"]);

		// A cursor in the middle of the entries of one GSI sort key, in each direction.
		for (const scanIndexForward of [true, false]) {
			const seen: unknown[] = [];
			let cursor: string | undefined;
			do {
				const page = await gsi.queryItems({ queries: [{ hashKey: "h", scanIndexForward }], limit: 1, cursor });
				seen.push(...base(page));
				cursor = page.cursor;
			} while (cursor !== undefined);
			expect(seen).toEqual(scanIndexForward ? ["1", "2", "3", "4"] : ["4", "3", "2", "1"]);
		}
	});
});
