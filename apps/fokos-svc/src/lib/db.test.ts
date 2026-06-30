import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { FokosDB, type FokosDBOptions } from "./db.js";
import type { BatchGetItemsRpcResult } from "./batch-types.js";
import { MAX_BATCH_FORWARDED_SUB_BATCH_BYTES, MAX_BATCH_GET_ITEMS, MAX_BATCH_WRITE_ITEMS } from "./transaction-limits.js";
import { KeyCodec } from "./partition-topology/key-codec.js";
import { PartitionContextCreator } from "./partition-topology/partition-context.js";
import { hashRootIndex } from "./partition-topology/partition-id.js";
import { PartitionTopologyRouterImpl } from "./partition-topology/router.js";

describe("FokosDB batch item operations", () => {
	it("rejects invalid batchGetItems input before the native routing boundary", async () => {
		const db = makeDB();
		await expect(db.batchGetItems({ items: [] })).rejects.toThrow(/batchGetItems requires at least 1 item/);
		await expect(
			db.batchGetItems({ items: Array.from({ length: MAX_BATCH_GET_ITEMS + 1 }, (_, i) => ({ hashKey: `hk-${i}` })) }),
		).rejects.toThrow(/batchGetItems supports at most 100 items/);
		await expect(db.batchGetItems({ items: [{ hashKey: "a" }, { hashKey: "a" }] })).rejects.toThrow(/batchGetItems duplicate key/);
	});

	it("batchGetItems returns found and missing items in request order with inputIndex correlation", async () => {
		const db = makeDB();
		await db.putItem({ hashKey: "a", sortKey: "1", data: "a1" });
		await db.putItem({ hashKey: "b", data: "b0" });

		const result = await db.batchGetItems({
			items: [{ hashKey: "missing" }, { hashKey: "a", sortKey: "1" }, { hashKey: "b" }],
		});

		expect(result.unprocessedKeys).toEqual([]);
		expect(result.items.map((item) => item.inputIndex)).toEqual([0, 1, 2]);
		expect(result.items[0]).toMatchObject({ inputIndex: 0, found: false, item: { hashKey: "missing", sortKey: undefined } });
		expect(result.items[1]).toMatchObject({ inputIndex: 1, found: true, item: { hashKey: "a", sortKey: "1", data: "a1", version: 1 } });
		expect(result.items[2]).toMatchObject({
			inputIndex: 2,
			found: true,
			item: { hashKey: "b", sortKey: undefined, data: "b0", version: 1 },
		});
		expect(result.meta).toMatchObject({
			requestedCount: 3,
			processedCount: 3,
			unprocessedCount: 0,
			rowsWritten: 0,
			forwardCount: 0,
			partitionsVisited: 1,
		});
		expect(result.meta.rowsRead).toBe(2);
		expect(result.partitionMetas).toHaveLength(1);
		expect(result.partitionMetas[0]).toMatchObject({ rowsRead: 2, rowsWritten: 0, forwardCount: 0 });
	});

	it("batchGetItems fails loudly when an RPC returns an unknown processed inputIndex", async () => {
		const db = makeDBReturningBatchGet({
			items: [{ inputIndex: 1, found: false, item: { hashKey: "corrupt" } }],
			unprocessedKeys: [],
			meta: batchRpcMeta(),
			partitionMetas: [],
		});

		await expect(db.batchGetItems({ items: [{ hashKey: "only" }] })).rejects.toThrow(
			/fokos\/batchGetItems: missing original item for inputIndex 1/,
		);
	});

	it("batchGetItems fails loudly when an RPC returns an unknown unprocessed inputIndex", async () => {
		const db = makeDBReturningBatchGet({
			items: [],
			unprocessedKeys: [{ inputIndex: 1, item: { hashKey: "corrupt" }, reason: { type: "transient_error" } }],
			meta: batchRpcMeta(),
			partitionMetas: [],
		});

		await expect(db.batchGetItems({ items: [{ hashKey: "only" }] })).rejects.toThrow(
			/fokos\/batchGetItems: missing original item for inputIndex 1/,
		);
	});

	it("rejects invalid batchWriteItems input before the native routing boundary", async () => {
		const db = makeDB();
		await expect(db.batchWriteItems({ operations: [] })).rejects.toThrow(/batchWriteItems requires at least 1 item/);
		await expect(
			db.batchWriteItems({
				operations: Array.from({ length: MAX_BATCH_WRITE_ITEMS + 1 }, (_, i) => ({ operation: "put", hashKey: `hk-${i}`, data: "x" })),
			}),
		).rejects.toThrow(/batchWriteItems supports at most 25 items/);
		await expect(
			db.batchWriteItems({
				operations: [
					{ operation: "put", hashKey: "a", data: "x" },
					{ operation: "delete", hashKey: "a" },
				],
			}),
		).rejects.toThrow(/batchWriteItems duplicate key/);
		await expect(db.batchWriteItems({ operations: [{ operation: "check", hashKey: "a" }] as never })).rejects.toThrow(
			/operation must be "put" or "delete"/,
		);
		await expect(
			db.batchWriteItems({ operations: [{ operation: "put", hashKey: "a", data: "x", conditions: [{ type: "item_exists" }] }] as never }),
		).rejects.toThrow(/does not support conditions/);
		await expect(
			db.batchWriteItems({
				operations: Array.from({ length: 5 }, (_, i) => ({
					operation: "put",
					hashKey: `payload-too-large-${i}`,
					data: new Uint8Array(MAX_BATCH_FORWARDED_SUB_BATCH_BYTES - 128),
				})),
			}),
		).rejects.toThrow(/batchWriteItems total payload exceeds 4 MB/);
		await expect(
			db.batchWriteItems({
				operations: [{ operation: "put", hashKey: "forwarded-too-large", data: new Uint8Array(MAX_BATCH_FORWARDED_SUB_BATCH_BYTES + 1) }],
			}),
		).rejects.toThrow(/batchWriteItems operation payload exceeds 1 MB forwarded sub-batch limit/);
	});

	it("batchWriteItems applies mixed put/delete on a single root and omits per-item versions", async () => {
		const db = makeDB();
		await db.putItem({ hashKey: "existing", data: "old" });

		const result = await db.batchWriteItems({
			operations: [
				{ operation: "put", hashKey: "created", data: "new" },
				{ operation: "delete", hashKey: "existing" },
			],
		});

		expect(result.unprocessedItems).toEqual([]);
		expect(result.processedItems).toEqual([
			{ inputIndex: 0, operation: "put", item: { hashKey: "created", sortKey: undefined } },
			{ inputIndex: 1, operation: "delete", item: { hashKey: "existing", sortKey: undefined } },
		]);
		expect("version" in result.processedItems[0]).toBe(false);
		await expect(db.getItem({ hashKey: "created" })).resolves.toMatchObject({ found: true, item: { data: "new" } });
		await expect(db.getItem({ hashKey: "existing" })).resolves.toMatchObject({ found: false });
		expect(result.meta).toMatchObject({
			requestedCount: 2,
			processedCount: 2,
			unprocessedCount: 0,
			forwardCount: 0,
			partitionsVisited: 1,
		});
		expect(result.meta.rowsWritten).toBeGreaterThan(0);
		expect(result.partitionMetas).toHaveLength(1);
		expect(result.partitionMetas[0]).toMatchObject({ forwardCount: 0 });
		expect(result.partitionMetas[0].rowsWritten).toBeGreaterThan(0);
	});

	it("batchWriteItems preserves sibling successes when one local item has a pending transaction lock", async () => {
		const { db, topology } = makeDBHarness();
		const lockedHashKey = KeyCodec.encode("locked");
		const lockedSortKey = KeyCodec.encodeOptional(undefined);
		const { doId, partitionContext } = topology.pickPartition(lockedHashKey, lockedSortKey);
		const stub = env.PARTITION_DO.get(doId);
		const txId = crypto.randomUUID();
		const prepareResult = await stub.prepare(partitionContext, {
			transactionId: txId,
			transactionTimestamp: Date.now(),
			coordinatorDoId: env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString(),
			items: [{ hashKey: lockedHashKey, sortKey: lockedSortKey, operation: "put", data: "pending" }],
		});
		expect(prepareResult).toEqual({ outcome: "accepted" });

		const result = await db.batchWriteItems({
			operations: [
				{ operation: "put", hashKey: "locked", data: "blocked" },
				{ operation: "put", hashKey: "unlocked", data: "visible" },
			],
		});

		expect(result.processedItems).toEqual([{ inputIndex: 1, operation: "put", item: { hashKey: "unlocked", sortKey: undefined } }]);
		expect(result.unprocessedItems).toEqual([
			{
				inputIndex: 0,
				operation: "put",
				item: { hashKey: "locked", sortKey: undefined },
				reason: { type: "pending_lock", conflictingTransactionId: txId },
			},
		]);
		await expect(db.getItem({ hashKey: "unlocked" })).resolves.toMatchObject({ found: true, item: { data: "visible" } });
		await expect(db.getItem({ hashKey: "locked" })).resolves.toMatchObject({ found: false });
		expect(result.meta).toMatchObject({
			requestedCount: 2,
			processedCount: 1,
			unprocessedCount: 1,
			forwardCount: 0,
			partitionsVisited: 1,
		});
		expect(result.meta.rowsWritten).toBeGreaterThan(0);
		expect(result.partitionMetas).toHaveLength(1);
		expect(result.partitionMetas[0].rowsWritten).toBeGreaterThan(0);

		await stub.cancel(partitionContext, { transactionId: txId });
		const retryResult = await db.batchWriteItems({ operations: [{ operation: "put", hashKey: "locked", data: "blocked" }] });
		expect(retryResult.unprocessedItems).toEqual([]);
		expect(retryResult.processedItems).toEqual([{ inputIndex: 0, operation: "put", item: { hashKey: "locked", sortKey: undefined } }]);
		await expect(db.getItem({ hashKey: "locked" })).resolves.toMatchObject({ found: true, item: { data: "blocked" } });
	});

	it("batchGetItems succeeds across more than ten initial roots and aggregates partition metadata", async () => {
		const rootTreesN = 64;
		const db = makeDB(rootTreesN);
		const hashKeys = keysForDistinctRootPartitions(11, rootTreesN);
		for (const hashKey of hashKeys) {
			await db.putItem({ hashKey, data: `data-${hashKey}` });
		}

		const result = await db.batchGetItems({ items: hashKeys.map((hashKey) => ({ hashKey })) });

		expect(result.unprocessedKeys).toEqual([]);
		expect(result.items.map((item) => item.inputIndex)).toEqual(hashKeys.map((_, index) => index));
		expect(result.items).toHaveLength(hashKeys.length);
		expect(result.meta).toMatchObject({
			requestedCount: hashKeys.length,
			processedCount: hashKeys.length,
			unprocessedCount: 0,
			rowsRead: hashKeys.length,
			rowsWritten: 0,
			forwardCount: 0,
			partitionsVisited: hashKeys.length,
		});
		expect(result.partitionMetas).toHaveLength(hashKeys.length);
		expect(result.partitionMetas.reduce((sum, meta) => sum + meta.rowsRead, 0)).toBe(hashKeys.length);
	});

	it("batchWriteItems succeeds across more than ten initial roots without a client fan-out cap", async () => {
		const rootTreesN = 64;
		const db = makeDB(rootTreesN);
		const operations = keysForDistinctRootPartitions(11, rootTreesN).map((hashKey) => ({
			operation: "put" as const,
			hashKey,
			data: "x",
		}));

		const result = await db.batchWriteItems({ operations });

		expect(result.unprocessedItems).toEqual([]);
		expect(result.processedItems.map((item) => item.inputIndex)).toEqual(operations.map((_, index) => index));
		expect(result.meta).toMatchObject({
			requestedCount: operations.length,
			processedCount: operations.length,
			unprocessedCount: 0,
			forwardCount: 0,
			partitionsVisited: operations.length,
		});
		expect(result.meta.rowsWritten).toBeGreaterThanOrEqual(operations.length);
		expect(result.partitionMetas).toHaveLength(operations.length);
	});
});

describe("FokosDB.queryItems — multi sub-query fan-out", () => {
	it("groups results per sub-query in request order, sk-ordered within each group", async () => {
		const db = makeDB();
		for (const sk of ["a3", "a1", "a2"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
		for (const sk of ["b2", "b1"]) await db.putItem({ hashKey: "bob", sortKey: sk, data: "x" });

		const res = await db.queryItems({ queries: [{ hashKey: "alice" }, { hashKey: "bob" }] });

		// alice's group (sorted) precedes bob's group (sorted) — list order across groups, sk order within.
		expect(sksOf(res)).toEqual(["a1", "a2", "a3", "b1", "b2"]);
		expect(res.count).toBe(5);
		expect(res.cursor).toBeUndefined();
		// One leaf scan per sub-query (both route to the same single root DO, listed once per RPC).
		expect(res.partitionMetas).toHaveLength(2);
		expect(res.meta.rowsReturned).toBe(5);
	});

	it("reverses both the group contents and applies sk DESC within each group", async () => {
		const db = makeDB();
		for (const sk of ["a1", "a2"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
		for (const sk of ["b1", "b2"]) await db.putItem({ hashKey: "bob", sortKey: sk, data: "x" });

		const res = await db.queryItems({
			queries: [
				{ hashKey: "alice", scanIndexForward: false },
				{ hashKey: "bob", scanIndexForward: false },
			],
		});

		// Groups stay in request order; only sk order within each group flips.
		expect(sksOf(res)).toEqual(["a2", "a1", "b2", "b1"]);
	});

	it("supports mixed directions: one sub-query ascending, another descending", async () => {
		const db = makeDB();
		for (const sk of ["a1", "a2", "a3"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
		for (const sk of ["b1", "b2", "b3"]) await db.putItem({ hashKey: "bob", sortKey: sk, data: "x" });

		const res = await db.queryItems({
			queries: [
				{ hashKey: "alice", scanIndexForward: true },
				{ hashKey: "bob", scanIndexForward: false },
			],
		});

		expect(sksOf(res)).toEqual(["a1", "a2", "a3", "b3", "b2", "b1"]);
	});

	it("allows duplicate hash keys → two consecutive groups (union of disjoint ranges)", async () => {
		const db = makeDB();
		for (const sk of ["s1", "s2", "s3", "s4"]) await db.putItem({ hashKey: "k", sortKey: sk, data: "x" });

		const res = await db.queryItems({
			queries: [
				{ hashKey: "k", sort: { op: "lte", value: "s2" } },
				{ hashKey: "k", sort: { op: "gte", value: "s3" } },
			],
		});

		expect(sksOf(res)).toEqual(["s1", "s2", "s3", "s4"]);
	});

	it("skips an empty-interval sub-query but keeps the others in list order", async () => {
		const db = makeDB();
		for (const sk of ["a1", "a2"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
		for (const sk of ["b1"]) await db.putItem({ hashKey: "bob", sortKey: sk, data: "x" });

		const res = await db.queryItems({
			queries: [
				{ hashKey: "alice" },
				{ hashKey: "zzz", sort: { op: "between", lower: "z9", upper: "z1" } }, // lower > upper → empty
				{ hashKey: "bob" },
			],
		});

		expect(sksOf(res)).toEqual(["a1", "a2", "b1"]);
		expect(res.cursor).toBeUndefined();
	});

	it("paginates across sub-queries with a global limit, resuming without gaps or duplicates", async () => {
		const db = makeDB();
		const aliceSks = ["a1", "a2", "a3"];
		const bobSks = ["b1", "b2", "b3"];
		for (const sk of aliceSks) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
		for (const sk of bobSks) await db.putItem({ hashKey: "bob", sortKey: sk, data: "x" });

		const queries = [{ hashKey: "alice" }, { hashKey: "bob" }];
		const got: Array<string | Uint8Array | undefined> = [];
		let cursor: string | undefined;
		let pages = 0;
		for (;;) {
			const res = await db.queryItems({ queries, limit: 2, cursor });
			got.push(...sksOf(res));
			pages++;
			if (res.cursor === undefined) break;
			cursor = res.cursor;
			expect(pages).toBeLessThan(50);
		}

		expect(got).toEqual([...aliceSks, ...bobSks]);
		expect(pages).toBeGreaterThan(1); // genuinely multi-page across the sub-query boundary
		expect(new Set(got.map(String)).size).toBe(got.length); // no duplicates
	});

	it("paginates across the sub-query boundary under a tight byte budget", async () => {
		const db = makeDB();
		const big = "x".repeat(20 * 1024);
		const aliceSks = ["a1", "a2", "a3"];
		const bobSks = ["b1", "b2"];
		for (const sk of aliceSks) await db.putItem({ hashKey: "alice", sortKey: sk, data: big });
		for (const sk of bobSks) await db.putItem({ hashKey: "bob", sortKey: sk, data: big });

		const queries = [{ hashKey: "alice" }, { hashKey: "bob" }];
		const got: Array<string | Uint8Array | undefined> = [];
		let cursor: string | undefined;
		let pages = 0;
		for (;;) {
			const res = await db.queryItems({ queries, maxPageBytes: 25 * 1024, cursor });
			got.push(...sksOf(res));
			pages++;
			if (res.cursor === undefined) break;
			cursor = res.cursor;
			expect(pages).toBeLessThan(50);
		}

		expect(got).toEqual([...aliceSks, ...bobSks]);
		expect(pages).toBeGreaterThan(1);
		expect(new Set(got.map(String)).size).toBe(got.length);
	});

	it("rejects a cursor whose request fingerprint differs from the resumed request", async () => {
		const db = makeDB();
		for (const sk of ["a1", "a2", "a3"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
		await db.putItem({ hashKey: "bob", sortKey: "b1", data: "x" });

		const first = await db.queryItems({ queries: [{ hashKey: "alice" }], limit: 2 });
		expect(first.cursor).toBeDefined();

		// Same cursor, different queries[] → fingerprint mismatch.
		await expect(db.queryItems({ queries: [{ hashKey: "bob" }], cursor: first.cursor })).rejects.toThrow(/fingerprint mismatch/);
	});

	it("rejects a cursor whose direction differs from the resumed request", async () => {
		const db = makeDB();
		for (const sk of ["a1", "a2", "a3"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });

		const first = await db.queryItems({ queries: [{ hashKey: "alice" }], limit: 2 });
		expect(first.cursor).toBeDefined();

		await expect(db.queryItems({ queries: [{ hashKey: "alice", scanIndexForward: false }], cursor: first.cursor })).rejects.toThrow(
			/direction mismatch/,
		);
	});

	it("rejects a malformed cursor", async () => {
		const db = makeDB();
		await expect(db.queryItems({ queries: [{ hashKey: "alice" }], cursor: "not-a-real-cursor!!" })).rejects.toThrow(/cursor/);
	});

	it("errors on an empty queries list", async () => {
		const db = makeDB();
		await expect(db.queryItems({ queries: [] })).rejects.toThrow(/must not be empty/);
	});
});

describe("FokosDB.queryItems — sort-key condition operators", () => {
	const ALL_SKS = ["a", "ab", "abc", "b", "ba", "c", "d"];

	async function populateAndQuery(sort: Parameters<FokosDB["queryItems"]>[0]["queries"][0]["sort"]) {
		const db = makeDB();
		for (const sk of ALL_SKS) await db.putItem({ hashKey: "k", sortKey: sk, data: "x" });
		return db.queryItems({ queries: [{ hashKey: "k", sort }] });
	}

	it("eq: returns only the exact match", async () => {
		const res = await populateAndQuery({ op: "eq", value: "b" });
		expect(sksOf(res)).toEqual(["b"]);
	});

	it("gt: returns items strictly greater", async () => {
		const res = await populateAndQuery({ op: "gt", value: "b" });
		expect(sksOf(res)).toEqual(["ba", "c", "d"]);
	});

	it("gte: returns items greater or equal", async () => {
		const res = await populateAndQuery({ op: "gte", value: "b" });
		expect(sksOf(res)).toEqual(["b", "ba", "c", "d"]);
	});

	it("lt: returns items strictly less", async () => {
		const res = await populateAndQuery({ op: "lt", value: "b" });
		expect(sksOf(res)).toEqual(["a", "ab", "abc"]);
	});

	it("lte: returns items less or equal", async () => {
		const res = await populateAndQuery({ op: "lte", value: "b" });
		expect(sksOf(res)).toEqual(["a", "ab", "abc", "b"]);
	});

	it("between: returns items in the inclusive range", async () => {
		const res = await populateAndQuery({ op: "between", lower: "ab", upper: "c" });
		expect(sksOf(res)).toEqual(["ab", "abc", "b", "ba", "c"]);
	});

	it("between: empty when lower > upper", async () => {
		const res = await populateAndQuery({ op: "between", lower: "z", upper: "a" });
		expect(sksOf(res)).toEqual([]);
	});

	it("begins_with: matches the prefix", async () => {
		const res = await populateAndQuery({ op: "begins_with", prefix: "a" });
		expect(sksOf(res)).toEqual(["a", "ab", "abc"]);
	});

	it("begins_with: single-character prefix that is also an exact key", async () => {
		const res = await populateAndQuery({ op: "begins_with", prefix: "b" });
		expect(sksOf(res)).toEqual(["b", "ba"]);
	});

	it("begins_with: multi-character prefix", async () => {
		const res = await populateAndQuery({ op: "begins_with", prefix: "ab" });
		expect(sksOf(res)).toEqual(["ab", "abc"]);
	});

	it("begins_with: empty prefix matches all", async () => {
		const res = await populateAndQuery({ op: "begins_with", prefix: "" });
		expect(sksOf(res)).toEqual(ALL_SKS);
	});

	it("begins_with: no matching prefix returns empty", async () => {
		const res = await populateAndQuery({ op: "begins_with", prefix: "zzz" });
		expect(sksOf(res)).toEqual([]);
	});

	it("range: exclusive lower, inclusive upper", async () => {
		const res = await populateAndQuery({
			op: "range",
			lower: { value: "a", inclusive: false },
			upper: { value: "b", inclusive: true },
		});
		expect(sksOf(res)).toEqual(["ab", "abc", "b"]);
	});

	it("range: open-ended (lower only)", async () => {
		const res = await populateAndQuery({ op: "range", lower: { value: "c", inclusive: true } });
		expect(sksOf(res)).toEqual(["c", "d"]);
	});

	it("range: open-ended (upper only)", async () => {
		const res = await populateAndQuery({ op: "range", upper: { value: "b", inclusive: false } });
		expect(sksOf(res)).toEqual(["a", "ab", "abc"]);
	});

	it("no sort condition: returns all items for the hash key", async () => {
		const res = await populateAndQuery(undefined);
		expect(sksOf(res)).toEqual(ALL_SKS);
	});

	it("begins_with works correctly with scanIndexForward=false", async () => {
		const db = makeDB();
		for (const sk of ALL_SKS) await db.putItem({ hashKey: "k", sortKey: sk, data: "x" });
		const res = await db.queryItems({
			queries: [{ hashKey: "k", sort: { op: "begins_with", prefix: "a" }, scanIndexForward: false }],
		});
		expect(sksOf(res)).toEqual(["abc", "ab", "a"]);
	});
});

// Builds a FokosDB over a fresh, isolated table. Generous split thresholds keep every key on a
// single root partition so these tests exercise FokosDB.queryItems' cross-sub-query fan-out and
// pagination, not the DO-level range-tree walk (covered in do-partition.test.ts).
function makeDBHarness(rootTreesN = 1) {
	const tableName = `test.${crypto.randomUUID()}`;
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		tableName,
		rootTreesN,
		hashSplitN: 2,
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: 500 },
		rangeSplitConditions: { maxSizeMb: 500 },
	});
	const topology = new PartitionTopologyRouterImpl(base);
	const db = new FokosDB({
		ns: env.PARTITION_DO,
		topology,
		transactionCoordinatorNs: env.TRANSACTION_COORDINATOR_DO,
	});
	return { db, topology };
}

function makeDB(rootTreesN = 1) {
	return makeDBHarness(rootTreesN).db;
}

function makeDBReturningBatchGet(result: BatchGetItemsRpcResult) {
	const tableName = `test.${crypto.randomUUID()}`;
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		tableName,
		rootTreesN: 1,
		hashSplitN: 2,
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: 500 },
		rangeSplitConditions: { maxSizeMb: 500 },
	});
	const doName = `${tableName}.fake`;
	const doId = env.PARTITION_DO.idFromName(doName);
	const partitionContext = {
		...base,
		doName,
		primaryDoIdStr: doId.toString(),
		partitionId: "00",
	};
	const topology: FokosDBOptions["topology"] = {
		partitionContext: () => base,
		pickPartition: () => ({ doId, partitionContext }),
		rootPartitionContexts: () => [partitionContext],
		traverseForDestroy: async () => {},
	};
	const ns = {
		get: () => ({
			batchGetItems: async () => result,
		}),
	} as unknown as FokosDBOptions["ns"];
	return new FokosDB({
		ns,
		topology,
		transactionCoordinatorNs: env.TRANSACTION_COORDINATOR_DO,
	});
}

function batchRpcMeta(): BatchGetItemsRpcResult["meta"] {
	return {
		rowsRead: 0,
		rowsWritten: 0,
		databaseSize: 0,
		servedByActorId: "actor-id",
		servedByActorName: "actor-name",
		servedByPartitionId: "partition-id",
		forwardCount: 0,
		hashDepth: 0,
	};
}

function sksOf(res: { items: Array<{ sortKey?: string | Uint8Array }> }) {
	return res.items.map((i) => i.sortKey);
}

function keysForDistinctRootPartitions(count: number, rootTreesN: number): string[] {
	const keysByRoot = new Map<number, string>();
	for (let i = 0; keysByRoot.size < count && i < 10_000; i++) {
		const hashKey = `batch-fanout-${i}`;
		const rootIdx = hashRootIndex(KeyCodec.encode(hashKey), rootTreesN);
		if (!keysByRoot.has(rootIdx)) {
			keysByRoot.set(rootIdx, hashKey);
		}
	}
	expect(keysByRoot.size).toBe(count);
	return [...keysByRoot.values()];
}
