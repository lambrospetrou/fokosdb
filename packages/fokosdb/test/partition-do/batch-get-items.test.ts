/**
 * `batchGetItems`: the partition operation `apiBatchGetItems` and the client method over it.
 */
import { describe, expect, it } from "vitest";
import type { BatchGetItemsRpcRequest, BatchGetItemsRpcResponse } from "../../src/server/do-partition.js";
import type { FokosEnvelope } from "../../src/sharding/runtime-types.js";
import { MAX_BATCH_GET_RESPONSE_BYTES, MAX_ITEMS_PER_BATCH } from "../../src/shared/transaction-limits.js";
import { FokosStd } from "../../src/client/fokos-std.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { controlledPartition, keysAcrossPartitions, makeDB, partitionNameOf } from "../transactions/tx-helpers.js";
import { kb } from "./helpers.js";
import { makePartition, makeRangeRoot, type TestPartition } from "./partition-harness.js";

const NAME = [{ expr: { ref: "data" as const, path: "$.name" }, as: "name" }];
const SCORE = [{ expr: { ref: "data" as const, path: "$.score" }, as: "score" }];

/** The estimate counts about 2,100 bytes for an item with this data: 2 bytes for each character. */
const DATA = "x".repeat(1000);

function batchRequest(keys: Array<[string, string]>, maxResponseBytes = MAX_BATCH_GET_RESPONSE_BYTES): BatchGetItemsRpcRequest {
	return { items: keys.map(([hashKey, sortKey], opIndex) => ({ opIndex, hashKey: kb(hashKey), sortKey: kb(sortKey) })), maxResponseBytes };
}

/** The value of `maxResponseBytes` that holds exactly the found items of opIndex 0 to `count - 1`. */
const bytesOfFirst = (found: BatchGetItemsRpcResponse["found"], count: number) =>
	found.filter((f) => f.opIndex < count).reduce((sum, f) => sum + f.bytes, 0);

const tooLarge = (opIndex: number, partitionIndex: number) => ({
	outcome: "unprocessed",
	code: "response_too_large",
	opIndexes: [opIndex],
	partitionIndex,
});

async function batchGet(partition: TestPartition, req: BatchGetItemsRpcRequest): Promise<FokosEnvelope<BatchGetItemsRpcResponse>> {
	return (await partition.stub.apiBatchGetItems(partition.ctx, req)) as unknown as FokosEnvelope<BatchGetItemsRpcResponse>;
}

const put = (partition: TestPartition, hashKey: string, sortKey: string, data: string) =>
	partition.put({ hashKey: kb(hashKey), sortKey: kb(sortKey), data, kind: "text" });

describe.concurrent("PartitionDO — apiBatchGetItems", () => {
	it("answers found and absent items by opIndex, with no key", async () => {
		const partition = makePartition();
		await put(partition, "a", "1", "one");

		const { value } = await batchGet(
			partition,
			batchRequest([
				["missing", "x"],
				["a", "1"],
			]),
		);

		expect(value.found).toEqual([
			{ opIndex: 1, item: { data: "one", kind: "text", version: 1 }, bytes: expect.any(Number), partitionIndex: 0 },
		]);
		expect(value.notFound).toEqual([{ opIndex: 0, partitionIndex: 0 }]);
		expect(value.failed).toEqual([]);
		expect(value.partitions).toHaveLength(1);
		expect(value.partitions[0]).toMatchObject({ rowsRead: expect.any(Number), databaseSize: expect.any(Number) });
	});

	it("trims its answer to maxResponseBytes, and keeps the item of opIndex 0", async () => {
		const partition = makePartition();
		const keys = Array.from({ length: 4 }, (_, i): [string, string] => [`big-${i}`, "1"]);
		for (const [hashKey, sortKey] of keys) {
			await put(partition, hashKey, sortKey, DATA);
		}
		const all = await batchGet(partition, batchRequest(keys));
		expect(all.value.found).toHaveLength(4);

		const two = await batchGet(partition, batchRequest(keys, bytesOfFirst(all.value.found, 2)));
		expect(two.value.found.map((f) => f.opIndex)).toEqual([0, 1]);
		expect(two.value.failed).toEqual([tooLarge(2, 0), tooLarge(3, 0)]);

		const one = await batchGet(partition, batchRequest(keys, 1));
		expect(
			one.value.found.map((f) => f.opIndex),
			"opIndex 0 is above the limit and still comes back",
		).toEqual([0]);
		expect(one.value.failed).toEqual([tooLarge(1, 0), tooLarge(2, 0), tooLarge(3, 0)]);
	});

	it("reads each key on the child that owns it after a hash split", async () => {
		const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		const keys = ["k-0", "k-1", "k-2", "k-3", "k-4", "k-5"];
		for (const key of keys) {
			await put(partition, key, "sk", `v-${key}`);
		}
		await partition.splitHash();
		const owners = await Promise.all(keys.map(async (key) => (await partition.childOwning(key)).doName));
		expect(new Set(owners).size, "the keys should span both children").toBe(2);

		const { value, routing } = await batchGet(partition, batchRequest(keys.map((key) => [key, "sk"])));

		expect(value.failed).toEqual([]);
		expect(value.found.map((f) => f.opIndex).sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5]);
		// The router reads nothing, so the list holds the two children. Each item names its owner.
		expect(value.partitions).toHaveLength(2);
		const names = new Map(routing.servedBy.map((node) => [node.actorId, node.ref.doName]));
		for (const { opIndex, item, partitionIndex } of value.found) {
			expect(item).toMatchObject({ data: `v-${keys[opIndex]}` });
			expect(names.get(value.partitions[partitionIndex].actorId)).toBe(owners[opIndex]);
		}

		// Each child answers all of its keys within the limit, and the router trims the joined answer.
		const limit = bytesOfFirst(value.found, 3);
		const trimmed = await batchGet(
			partition,
			batchRequest(
				keys.map((key) => [key, "sk"]),
				limit,
			),
		);
		expect(trimmed.value.found.map((f) => f.opIndex)).toEqual([0, 1, 2]);
		expect(trimmed.value.failed.map((f) => f.opIndexes[0]).sort((a, b) => a - b)).toEqual([3, 4, 5]);
		for (const failure of trimmed.value.failed) {
			expect(failure).toMatchObject({ outcome: "unprocessed", code: "response_too_large" });
			const name = names.get(trimmed.value.partitions[failure.partitionIndex!].actorId);
			expect(name, "the failure names the child that read the item").toBe(owners[failure.opIndexes[0]]);
		}
	});

	it("reads a promoted key on its range tree, and the other keys here", async () => {
		const { root, hashPartition } = await makeRangeRoot(2);
		await put(hashPartition, "alice", "a1", "alice-1");
		await put(hashPartition, "bob", "b1", "bob-1");

		const { value } = await batchGet(
			hashPartition,
			batchRequest([
				["bob", "b1"],
				["alice", "a1"],
			]),
		);

		expect(value.failed).toEqual([]);
		const byOp = new Map(value.found.map((f) => [f.opIndex, f]));
		expect(byOp.get(0)?.item).toMatchObject({ data: "bob-1" });
		expect(byOp.get(1)?.item).toMatchObject({ data: "alice-1" });
		const actorOf = (opIndex: number) => value.partitions[byOp.get(opIndex)!.partitionIndex].actorId;
		expect(actorOf(1)).toBe(root.stub.id.toString());
		expect(actorOf(0)).toBe(hashPartition.stub.id.toString());
	});
});

describe.concurrent("FokosDB.batchGetItems", () => {
	it("refuses a request that is not valid, before any I/O", async () => {
		const db = makeDB();
		const tooMany = Array.from({ length: MAX_ITEMS_PER_BATCH + 1 }, (_, i) => ({ hashKey: `k-${i}` }));
		await expect(db.batchGetItems({ items: tooMany })).rejects.toThrow(fokosErrorWith("batch_items_too_many"));
		await expect(
			db.batchGetItems({
				items: [
					{ hashKey: "a", sortKey: "1" },
					{ hashKey: "a", sortKey: "1" },
				],
			}),
		).rejects.toThrow(fokosErrorWith("batch_duplicate_key", { opIndex: 1 }));
		await expect(db.batchGetItems({ items: [{ hashKey: "" }] })).rejects.toThrow(fokosErrorWith("hash_key_empty"));
		await expect(db.batchGetItems({ items: [{ hashKey: "a" }], projection: [] })).rejects.toThrow(fokosErrorWith("expression_invalid"));
		for (const maxResponseBytes of [0, -1, 1.5]) {
			await expect(db.batchGetItems({ items: [{ hashKey: "a" }], maxResponseBytes })).rejects.toThrow(
				fokosErrorWith("batch_max_response_bytes_invalid"),
			);
		}
	});

	it("answers an empty request with an empty result", async () => {
		const db = makeDB();
		await expect(db.batchGetItems({ items: [] })).resolves.toEqual({
			items: [],
			unprocessedKeys: [],
			meta: { forwardCount: 0, partitions: [] },
		});
	});

	it("answers each key in request order across partitions, with its own partition", async () => {
		const db = makeDB();
		const keys = keysAcrossPartitions(db, 3, "batch-get");
		await db.putItem({ ...keys[0], data: { name: "zero" } });
		await db.putItem({ ...keys[2], data: { name: "two" } });

		const read = await db.batchGetItems({ items: keys });

		expect(read.items.map((item) => item.outcome)).toEqual(["found", "not_found", "found"]);
		expect(read.items[0]).toMatchObject({ ...keys[0], kind: "json", data: { name: "zero" }, version: 1 });
		expect(read.items[1]).toMatchObject(keys[1]);
		expect(read.unprocessedKeys).toEqual([]);
		expect(read.meta.forwardCount).toBe(0);
		expect(read.meta.partitions).toHaveLength(3);
		read.items.forEach((item, i) => {
			invariantPartition(item);
			expect(read.meta.partitions[item.partitionIndex].servedByActorName).toBe(partitionNameOf(db, keys[i]));
		});
	});

	it("applies the projection of an item in place of the projection of the request", async () => {
		const db = makeDB();
		await db.putItem({ hashKey: "p", sortKey: "1", data: { name: "one", score: 1 } });
		await db.putItem({ hashKey: "p", sortKey: "2", data: { name: "two", score: 2 } });

		const read = await db.batchGetItems({
			items: [
				{ hashKey: "p", sortKey: "1" },
				{ hashKey: "p", sortKey: "2", projection: SCORE },
				{ hashKey: "p", sortKey: "3" },
			],
			projection: NAME,
		});

		expect(read.items[0]).toMatchObject({ outcome: "found", kind: "projected", data: { name: "one" } });
		expect(read.items[1]).toMatchObject({ outcome: "found", kind: "projected", data: { score: 2 } });
		expect(read.items[2]).toMatchObject({ outcome: "not_found" });
	});

	it("trims the joined answers of its calls to maxResponseBytes", async () => {
		const db = makeDB();
		const keys = keysAcrossPartitions(db, 4, "batch-get-limit");
		await Promise.all(keys.map((key) => db.putItem({ ...key, data: DATA })));

		// Each partition answers one item, which fits in the limit. Two items fit in the limit together,
		// and three do not, so only the client trims.
		const maxResponseBytes = 5000;
		const read = await db.batchGetItems({ items: keys, maxResponseBytes });

		expect(read.items.map((item) => item.outcome)).toEqual(["found", "found", "unprocessed", "unprocessed"]);
		for (const item of read.items.slice(2)) {
			expect(item).toMatchObject({ outcome: "unprocessed", code: "response_too_large", partitionIndex: expect.any(Number) });
			expect(item).not.toHaveProperty("error_id");
		}
		expect(read.unprocessedKeys).toEqual(keys.slice(2));

		const retried = await FokosStd.batchGetItemsWithRetry(db, { items: keys, maxResponseBytes }, { baseDelayMs: 1, maxDelayMs: 2 });
		expect(retried.items.map((item) => item.outcome)).toEqual(keys.map(() => "found"));
		expect(retried.unprocessedKeys).toEqual([]);
	});

	it("answers the keys of a failed call unprocessed, and still reads the keys of the other calls", async () => {
		const db = makeDB({ controlled: true });
		const [failing, ok] = keysAcrossPartitions(db, 2, "batch-get-fail");
		await db.putItem({ ...ok, data: "ok" });
		await controlledPartition(db, failing).testTxResponse("apiBatchGetItems", { error: "Network connection lost.", times: 1 });

		const items = [{ ...failing, projection: NAME }, ok];
		const read = await db.batchGetItems({ items });

		expect(read.items[0]).toMatchObject({ outcome: "unprocessed", code: "foreign_error", error_id: expect.any(String), ...failing });
		expect(read.items[0]).not.toHaveProperty("partitionIndex");
		expect(read.items[1]).toMatchObject({ outcome: "found", data: "ok" });
		// The entry keeps its own projection.
		expect(read.unprocessedKeys).toEqual([items[0]]);
		expect(read.meta.partitions).toHaveLength(1);
	});
});

function invariantPartition(item: { partitionIndex?: number }): asserts item is { partitionIndex: number } {
	expect(item.partitionIndex).toEqual(expect.any(Number));
}
