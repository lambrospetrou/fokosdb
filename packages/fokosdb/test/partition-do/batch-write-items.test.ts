/**
 * `batchWriteItems`: the partition operation `apiBatchWriteItems` and the client method over it.
 */
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { FokosStd } from "../../src/client/fokos-std.js";
import type {
	BatchWriteItemsRpcRequest,
	BatchWriteItemsRpcResponse,
	BatchWriteRpcItem,
	PartitionDO,
} from "../../src/server/do-partition.js";
import type { ItemBatcher } from "../../src/shared/batch-api-types.js";
import type { FokosDBRouteContext } from "../../src/shared/partition-context.js";
import { MAX_ITEM_BYTES, MAX_ITEMS_PER_BATCH } from "../../src/shared/transaction-limits.js";
import type { FokosEnvelope } from "../../src/sharding/runtime-types.js";
import { recordedTxCalls } from "../controlled-partition-do.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { betweenPhases, controlledPartition, holdPendingLock, keysAcrossPartitions, makeDB } from "../transactions/tx-helpers.js";
import { kb } from "./helpers.js";
import { CONTROLLED_NS, makePartition, TestPartition } from "./partition-harness.js";

const MB = 1024 * 1024;

const putItem = (opIndex: number, hashKey: string, sortKey: string, data: string): BatchWriteRpcItem => ({
	opIndex,
	hashKey: kb(hashKey),
	sortKey: kb(sortKey),
	operation: "put",
	data,
	kind: "text",
});
const deleteItem = (opIndex: number, hashKey: string, sortKey: string): BatchWriteRpcItem => ({
	opIndex,
	hashKey: kb(hashKey),
	sortKey: kb(sortKey),
	operation: "delete",
});

async function batchWrite(partition: TestPartition, req: BatchWriteItemsRpcRequest): Promise<FokosEnvelope<BatchWriteItemsRpcResponse>> {
	return (await partition.stub.apiBatchWriteItems(partition.ctx, req)) as unknown as FokosEnvelope<BatchWriteItemsRpcResponse>;
}

async function readData(partition: TestPartition, hashKey: string, sortKey: string): Promise<string | undefined> {
	const res = await partition.get({ hashKey: kb(hashKey), sortKey: kb(sortKey) });
	return res.found && res.item.kind === "text" ? (res.item.data as string) : undefined;
}

/** The same partition with a hash cap that its current file exceeds by 1.3 times, so it refuses writes. */
async function overSize(partition: TestPartition): Promise<TestPartition> {
	const size = await runInDurableObject(
		partition.stub,
		(_instance: PartitionDO, state: DurableObjectState) => state.storage.sql.databaseSize,
	);
	const ctx: FokosDBRouteContext = {
		...partition.ctx,
		policy: { ...partition.ctx.policy, hashSplitConditions: { ...partition.ctx.policy.hashSplitConditions, maxSizeMb: size / 1.3 / MB } },
	};
	return TestPartition.at(ctx, partition.stub);
}

describe.concurrent("PartitionDO — apiBatchWriteItems", () => {
	it("applies puts and deletes with the version and the deleted flag of each", async () => {
		const partition = makePartition();
		await partition.put({ hashKey: kb("a"), sortKey: kb("1"), data: "old", kind: "text" });

		const { value } = await batchWrite(partition, {
			items: [putItem(0, "a", "1", "new"), deleteItem(1, "a", "1"), deleteItem(2, "b", "1"), putItem(3, "b", "2", "two")],
		});

		// The operations apply in request order. The delete of `a/1` at version 2 raises `max_deleted_v`,
		// so the new row `b/2` starts at version 3.
		expect(value.appliedPuts).toEqual([
			{ opIndex: 0, version: 2, partitionIndex: 0 },
			{ opIndex: 3, version: 3, partitionIndex: 0 },
		]);
		expect(value.appliedDeletes).toEqual([
			{ opIndex: 1, deleted: true, partitionIndex: 0 },
			{ opIndex: 2, deleted: false, partitionIndex: 0 },
		]);
		expect(value.failed).toEqual([]);
		expect(value.partitions).toHaveLength(1);
		expect(value.partitions[0].rowsWritten).toBeGreaterThan(0);
		expect(await readData(partition, "a", "1")).toBeUndefined();
		expect(await readData(partition, "b", "2")).toBe("two");
	});

	it("rejects a put whose stored row is above the item limit, and applies the other items", async () => {
		const partition = makePartition();
		const { value } = await batchWrite(partition, {
			items: [putItem(0, "a", "big", "x".repeat(MAX_ITEM_BYTES)), putItem(1, "a", "ok", "v")],
		});

		expect(value.failed).toEqual([
			{ outcome: "rejected", code: "item_too_large", error_id: expect.any(String), opIndexes: [0], partitionIndex: 0 },
		]);
		expect(value.appliedPuts.map((p) => p.opIndex)).toEqual([1]);
		expect(await readData(partition, "a", "big")).toBeUndefined();
	});

	it("refuses the puts of an over-size partition, applies its deletes, and starts the split decision", async () => {
		const partition = makePartition();
		for (let i = 0; i < 4; i++) {
			await partition.put({ hashKey: kb("big"), sortKey: kb(`sk${i}`), data: "x".repeat(64 * 1024), kind: "text" });
		}
		const low = await overSize(partition);

		const { value } = await batchWrite(low, {
			items: [putItem(0, "big", "new", "v"), deleteItem(1, "big", "sk0"), putItem(2, "other", "x", "v")],
		});

		expect(value.failed).toEqual([
			{ outcome: "unprocessed", code: "partition_over_size", error_id: expect.any(String), opIndexes: [0, 2], partitionIndex: 0 },
		]);
		expect(value.appliedDeletes).toEqual([{ opIndex: 1, deleted: true, partitionIndex: 0 }]);
		expect(value.appliedPuts).toEqual([]);
		// A leaf with one hash key promotes that key.
		expect(await low.promotedKeyStatus("big")).toBeDefined();
	});

	it("answers unknown for the items of a child whose answer is lost after it applied", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		await partition.splitHash();
		const child = await partition.childOwning("lost");
		await child.controlled.testTxResponse("apiBatchWriteItems", { error: "Network connection lost.", afterWork: true, times: 1 });

		const { value } = await batchWrite(partition, { items: [putItem(0, "lost", "1", "v")] });

		expect(value.failed).toEqual([{ outcome: "unknown", code: "foreign_error", error_id: expect.any(String), opIndexes: [0] }]);
		expect(value.partitions).toEqual([]);
		expect(await readData(partition, "lost", "1"), "the item applied before the answer was lost").toBe("v");
	});
});

describe.concurrent("FokosDB.batchWriteItems", () => {
	it("answers an empty request with an empty result", async () => {
		const db = makeDB();
		await expect(db.batchWriteItems({ items: [] })).resolves.toEqual({
			results: [],
			unprocessedItems: [],
			meta: { forwardCount: 0, partitions: [] },
		});
	});

	it("refuses a request that is not valid, before any I/O", async () => {
		const db = makeDB();
		const put = { operation: "put" as const, hashKey: "a", data: "v" };
		const tooMany = Array.from({ length: MAX_ITEMS_PER_BATCH + 1 }, (_, i) => ({ ...put, hashKey: `k-${i}` }));
		await expect(db.batchWriteItems({ items: tooMany })).rejects.toThrow(fokosErrorWith("batch_items_too_many"));
		await expect(db.batchWriteItems({ items: [put, { operation: "delete", hashKey: "a" }] })).rejects.toThrow(
			fokosErrorWith("batch_duplicate_key", { opIndex: 1 }),
		);
		const notValid = [
			{ operation: "update", hashKey: "a" },
			{ operation: "put", hashKey: "a" },
			{ operation: "delete", hashKey: "a", data: "v" },
			{ operation: "delete", hashKey: "a", ttlAt: 10 },
			{ ...put, condition: { op: "exists", args: [{ ref: "hashKey" }] } },
			{ ...put, returnValuesOnConditionCheckFailure: "all_old" },
		];
		for (const item of notValid) {
			await expect(db.batchWriteItems({ items: [item as never] })).rejects.toThrow(fokosErrorWith("batch_operation_fields_invalid"));
		}
		await expect(db.batchWriteItems({ items: [{ ...put, ttlAt: -1 }] })).rejects.toThrow(fokosErrorWith("ttl_at_invalid"));
		await expect(db.batchWriteItems({ items: [{ ...put, data: 3 as never }] })).rejects.toThrow(fokosErrorWith("item_data_wrong_type"));
		await expect(db.batchWriteItems({ items: [{ ...put, data: "x".repeat(MAX_ITEM_BYTES + 1) }] })).rejects.toThrow(
			fokosErrorWith("item_data_too_large"),
		);
	});

	it("answers each operation in request order across partitions", async () => {
		const db = makeDB();
		const keys = keysAcrossPartitions(db, 3, "batch-write");
		await db.putItem({ ...keys[1], data: "old" });

		const write = await db.batchWriteItems({
			items: [
				{ operation: "put", ...keys[0], data: { n: 0 }, ttlAt: 4_000_000_000 },
				{ operation: "delete", ...keys[1] },
				{ operation: "delete", ...keys[2] },
			],
		});

		expect(write.results).toMatchObject([
			{ outcome: "ok", operation: "put", version: 1 },
			{ outcome: "ok", operation: "delete", deleted: true },
			{ outcome: "ok", operation: "delete", deleted: false },
		]);
		expect(write.unprocessedItems).toEqual([]);
		expect(write.meta.partitions).toHaveLength(3);
		const read = await db.getItem({ ...keys[0] });
		expect(read).toMatchObject({ found: true, item: { data: { n: 0 }, ttlAt: 4_000_000_000, version: 1 } });
		expect((await db.getItem(keys[1])).found).toBe(false);
	});

	it("answers a locked item unprocessed, as putItem refuses it, and applies the others", async () => {
		const db = makeDB();
		const [locked, free] = keysAcrossPartitions(db, 2, "batch-locked");
		const release = await holdPendingLock(db, locked, { operation: "put", data: "tx", kind: "text" });
		try {
			const write = await db.batchWriteItems({
				items: [
					{ operation: "put", ...locked, data: "batch" },
					{ operation: "put", ...free, data: "batch" },
				],
			});
			expect(write.results[0]).toMatchObject({ outcome: "unprocessed", code: "item_locked_by_transaction", partitionIndex: 0 });
			expect(write.results[1]).toMatchObject({ outcome: "ok" });
			expect(write.unprocessedItems).toEqual([{ operation: "put", ...locked, data: "batch" }]);
		} finally {
			await release();
		}
	});

	it("makes a read transaction that overlaps it fail with read_conflict", async () => {
		const db = makeDB({ controlled: true, singlePartitionFastPath: false });
		const [key, other] = keysAcrossPartitions(db, 2, "batch-conflict");
		await db.putItem({ ...key, data: "v1" });

		const read = betweenPhases(
			db,
			key,
			async (instance, _state, pCtx) => {
				await instance.apiBatchWriteItems(pCtx, {
					items: [{ opIndex: 0, hashKey: kb(key.hashKey), sortKey: kb(key.sortKey), operation: "delete" }],
				});
			},
			() => db.transactGetItems({ items: [key, other] }),
		);
		await expect(read).rejects.toThrow(fokosErrorWith("read_conflict"));
	});

	it("answers the items of a failed call unknown, and lists them to send again", async () => {
		const db = makeDB({ controlled: true });
		const [failing, ok] = keysAcrossPartitions(db, 2, "batch-write-fail");
		await controlledPartition(db, failing).testTxResponse("apiBatchWriteItems", { error: "Network connection lost.", times: 1 });

		const items = [
			{ operation: "put" as const, ...failing, data: "v" },
			{ operation: "put" as const, ...ok, data: "v" },
		];
		const write = await db.batchWriteItems({ items });

		expect(write.results[0]).toEqual({ outcome: "unknown", code: "foreign_error", error_id: expect.any(String) });
		expect(write.results[1]).toMatchObject({ outcome: "ok" });
		expect(write.unprocessedItems).toEqual([items[0]]);
	});
});

describe.concurrent("FokosStd.batchWriteItemsWithRetry", () => {
	it("sends the unprocessed items again until each applies, and joins the meta", async () => {
		const db = makeDB({ controlled: true });
		const [failing, ok] = keysAcrossPartitions(db, 2, "retry-ok");
		await controlledPartition(db, failing).testTxResponse("apiBatchWriteItems", { error: "Network connection lost.", times: 1 });

		const write = await FokosStd.batchWriteItemsWithRetry(
			db,
			{
				items: [
					{ operation: "put", ...failing, data: "v" },
					{ operation: "put", ...ok, data: "v" },
				],
			},
			{ baseDelayMs: 1, maxDelayMs: 2 },
		);

		expect(write.results).toMatchObject([{ outcome: "ok" }, { outcome: "ok" }]);
		expect(write.unprocessedItems).toEqual([]);
		expect(await recordedTxCalls(controlledPartition(db, failing), "apiBatchWriteItems")).toHaveLength(2);
		expect(await recordedTxCalls(controlledPartition(db, ok), "apiBatchWriteItems")).toHaveLength(1);
		// One entry for each partition, and each result names its own.
		expect(write.meta.partitions).toHaveLength(2);
		const names = write.results.map((r) => (r.outcome === "ok" ? write.meta.partitions[r.partitionIndex].servedByActorName : undefined));
		expect(new Set(names).size).toBe(2);
	});

	it("stops at maxAttempts and keeps the items that stay unprocessed", async () => {
		const db = makeDB({ controlled: true });
		const [key] = keysAcrossPartitions(db, 1, "retry-max");
		await controlledPartition(db, key).testTxResponse("apiBatchWriteItems", { error: "Network connection lost." });

		const items = [{ operation: "put" as const, ...key, data: "v" }];
		const write = await FokosStd.batchWriteItemsWithRetry(db, { items }, { baseDelayMs: 1, maxDelayMs: 2, maxAttempts: 3 });

		expect(write.results).toMatchObject([{ outcome: "unknown" }]);
		expect(write.unprocessedItems).toEqual(items);
		expect(await recordedTxCalls(controlledPartition(db, key), "apiBatchWriteItems")).toHaveLength(3);
	});

	it("keeps unknown when a later call does not apply the item, and never sends a rejected item again", async () => {
		const db = makeDB({ controlled: true });
		const [unknown, rejected] = keysAcrossPartitions(db, 2, "retry-keep");
		// The first call applies and loses its answer. A lock then refuses the second call.
		await controlledPartition(db, unknown).testTxResponse("apiBatchWriteItems", {
			error: "Network connection lost.",
			afterWork: true,
			times: 1,
		});
		let release: (() => Promise<unknown>) | undefined;
		const batcher: ItemBatcher = {
			batchGetItems: (opts) => db.batchGetItems(opts),
			batchWriteItems: async (opts) => {
				const answer = await db.batchWriteItems(opts);
				release ??= await holdPendingLock(db, unknown, { operation: "put", data: "tx", kind: "text" });
				return answer;
			},
		};
		try {
			const write = await FokosStd.batchWriteItemsWithRetry(
				batcher,
				{
					items: [
						{ operation: "put", ...unknown, data: "v" },
						{ operation: "put", ...rejected, data: "x".repeat(MAX_ITEM_BYTES) },
					],
				},
				{ baseDelayMs: 1, maxDelayMs: 2, maxAttempts: 2 },
			);
			expect(write.results).toMatchObject([
				{ outcome: "unknown", code: "foreign_error" },
				{ outcome: "rejected", code: "item_too_large" },
			]);
			expect(write.unprocessedItems).toEqual([{ operation: "put", ...unknown, data: "v" }]);
			expect(await recordedTxCalls(controlledPartition(db, unknown), "apiBatchWriteItems")).toHaveLength(2);
			expect(await recordedTxCalls(controlledPartition(db, rejected), "apiBatchWriteItems")).toHaveLength(1);
		} finally {
			await release?.();
		}
	});

	it("refuses retry options that are not valid, before any I/O", async () => {
		const db = makeDB({ controlled: true });
		const [key] = keysAcrossPartitions(db, 1, "retry-options");
		await expect(
			FokosStd.batchWriteItemsWithRetry(db, { items: [{ operation: "put", ...key, data: "v" }] }, { baseDelayMs: 10, maxDelayMs: 5 }),
		).rejects.toThrow(fokosErrorWith("fokosdb_options_invalid"));
		expect(await recordedTxCalls(controlledPartition(db, key), "apiBatchWriteItems")).toHaveLength(0);
	});
});

describe.concurrent("FokosStd.batchGetItemsWithRetry", () => {
	it("answers an empty request with an empty result, for reads and writes", async () => {
		const db = makeDB();
		const meta = { forwardCount: 0, partitions: [] };
		await expect(FokosStd.batchGetItemsWithRetry(db, { items: [] })).resolves.toEqual({ items: [], unprocessedKeys: [], meta });
		await expect(FokosStd.batchWriteItemsWithRetry(db, { items: [] })).resolves.toEqual({ results: [], unprocessedItems: [], meta });
	});

	it("sends the unprocessed keys again with the projection of the request", async () => {
		const db = makeDB({ controlled: true });
		const [failing, ok] = keysAcrossPartitions(db, 2, "retry-get");
		await db.putItem({ ...failing, data: { name: "f", score: 1 } });
		await db.putItem({ ...ok, data: { name: "o", score: 2 } });
		await controlledPartition(db, failing).testTxResponse("apiBatchGetItems", { error: "Network connection lost.", times: 1 });

		const read = await FokosStd.batchGetItemsWithRetry(
			db,
			{ items: [failing, ok], projection: [{ expr: { ref: "data", path: "$.name" }, as: "name" }] },
			{ baseDelayMs: 1, maxDelayMs: 2 },
		);

		expect(read.items).toMatchObject([
			{ outcome: "found", ...failing, data: { name: "f" } },
			{ outcome: "found", ...ok, data: { name: "o" } },
		]);
		expect(read.unprocessedKeys).toEqual([]);
		const calls = await recordedTxCalls(controlledPartition(db, failing), "apiBatchGetItems");
		expect(calls).toHaveLength(2);
		expect(calls[1].projection).toEqual([{ expr: { ref: "data", path: "$.name" }, as: "name" }]);
		expect(calls[1].items.map((item) => item.opIndex)).toEqual([0]);
	});
});

// One request of about 40 MB. It runs alone, so the memory of the other tests does not add to it.
describe("FokosDB.batchWriteItems — the RPC limit", () => {
	it("rejects each item of a call above 32 MiB, applies none of them, and the helper sends it once", async () => {
		const db = makeDB({ controlled: true });
		const data = "x".repeat(400_000);
		const items = Array.from({ length: 100 }, (_, i) => ({ operation: "put" as const, hashKey: "big", sortKey: `sk-${i}`, data }));
		const calls: number[] = [];
		const batcher: ItemBatcher = {
			batchGetItems: (opts) => db.batchGetItems(opts),
			batchWriteItems: (opts) => {
				calls.push(opts.items.length);
				return db.batchWriteItems(opts);
			},
		};

		const write = await FokosStd.batchWriteItemsWithRetry(batcher, { items }, { baseDelayMs: 1, maxDelayMs: 2 });

		expect(calls).toEqual([100]);
		for (const result of write.results) {
			expect(result).toMatchObject({ outcome: "rejected", code: "batch_request_too_large", error_id: expect.stringMatching(/^e_nhncs4_/) });
		}
		expect(write.unprocessedItems).toEqual([]);
		expect(await recordedTxCalls(controlledPartition(db, { hashKey: "big", sortKey: "sk-0" }), "apiBatchWriteItems")).toHaveLength(0);
		const read = await db.batchGetItems({ items: items.slice(0, 5).map(({ hashKey, sortKey }) => ({ hashKey, sortKey })) });
		expect(read.items.map((r) => r.outcome)).toEqual(["not_found", "not_found", "not_found", "not_found", "not_found"]);
	});
});
