// A transaction with more than one operation for one item, through a partition that routes: the
// operations of one item reach one owner in one sub-request, also with the Bloom filter on, and a
// lock row moves with its operation list and its last-write data in a promotion and in a hash split.
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { PartitionDO } from "../../src/server/do-partition.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { hashChildIndex } from "../../src/sharding/partition-id.js";
import { updateTree } from "../../src/shared/expression/test-fixtures.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import type { PrepareRequest, TransactionItem, TransactionItemKey } from "../../src/shared/transaction-wire-types.js";
import { testCoordinatorRef } from "../stub-helpers.js";
import { kb, lockKeys, withOpIndex } from "./helpers.js";
import {
	CONTROLLED_NS,
	PROMOTION_TEST_MAX_SIZE_MB,
	TestPartition,
	drainUntil,
	findKey,
	makePartition,
	storedBloom,
	useSmallBloom,
} from "./partition-harness.js";

type Op = Omit<TransactionItem, "opIndex">;

const incN = updateTree([
	{
		action: "set",
		target: { ref: "data", path: "$.n" },
		value: { fn: "+", args: [{ fn: "if_not_exists", args: [{ ref: "data", path: "$.n" }, { val: 0 }] }, { val: 1 }] },
	},
]);
const put = (hk: string, sk: string, data: unknown): Op => ({
	hashKey: kb(hk),
	sortKey: kb(sk),
	operation: "put",
	data: JSON.stringify(data),
	kind: "json",
});
const update = (hk: string, sk: string): Op => ({ hashKey: kb(hk), sortKey: kb(sk), operation: "update", update: incN });
const del = (hk: string, sk: string): Op => ({ hashKey: kb(hk), sortKey: kb(sk), operation: "delete" });

function prepareOf(ops: Op[]): PrepareRequest {
	return {
		transactionId: crypto.randomUUID(),
		transactionTimestamp: Date.now(),
		coordinator: testCoordinatorRef(),
		items: withOpIndex(ops),
	};
}

function keysOf(request: PrepareRequest): TransactionItemKey[] {
	const seen = new Map<string, TransactionItemKey>();
	for (const { hashKey, sortKey } of request.items) {
		seen.set(`${KeyCodec.keyForLog(hashKey)}/${KeyCodec.keyForLog(sortKey)}`, { hashKey, sortKey });
	}
	return [...seen.values()];
}

/** The lock rows of one transaction on one partition: key, operation list, and whether the row has data. */
async function locksOf(partition: TestPartition, transactionId: string) {
	return await runInDurableObject(partition.stub, (_instance: PartitionDO, state: DurableObjectState) =>
		new PartitionStore(state.storage)
			.listPendingTxItems(transactionId)
			.map((row) => ({
				key: `${KeyCodec.decode(row.hk) as string}/${KeyCodec.decode(row.sk) as string}`,
				opList: row.op_list,
				hasData: row.data !== null,
			}))
			.sort((a, b) => a.key.localeCompare(b.key)),
	);
}

/**
 * The lock rows of one transaction on the leaf that owns `hashKey` below `root`. A child of a split with
 * a small cap can split again, and its lock rows then move one level further down.
 */
async function locksAtLeaf(root: TestPartition, hashKey: string, transactionId: string) {
	let rows: Awaited<ReturnType<typeof locksOf>> = [];
	await drainUntil(
		[root, ...(await root.children())],
		async () => {
			let leaf = root;
			while ((await leaf.status()).splitStatus?.status === "split_completed") {
				leaf = await leaf.childOwning(hashKey);
			}
			rows = await locksOf(leaf, transactionId);
			return rows.length > 0;
		},
		`the lock row of ${hashKey} on its leaf`,
	);
	return rows;
}

async function dataOf(partition: TestPartition, hk: string, sk: string): Promise<unknown> {
	const res = await partition.get({ hashKey: kb(hk), sortKey: kb(sk) });
	return res.found && "data" in res.item ? JSON.parse(res.item.data as string) : undefined;
}

describe("PartitionDO — the operations of one item reach one owner", () => {
	// When two operations of one item went to the owner in two sub-requests, the second sub-request
	// would find the lock of the first and write nothing. The lock row would then miss operations.
	it("sends the equal keys of one txPrepare to one owner in one sub-request, with the Bloom filter on", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		await useSmallBloom(partition);
		await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("bob") });
		const rangeRoot = await partition.awaitPromoted("bob");
		await partition.get({ hashKey: kb("bob"), sortKey: kb("sk1") });
		expect((await storedBloom(partition)).maybePromoted(kb("bob"))).toBe(true);

		const request = prepareOf([
			put("bob", "x", { n: 1 }),
			put("carol", "x", { c: 1 }),
			update("bob", "x"),
			del("bob", "y"),
			put("bob", "y", 2),
		]);
		expect(await partition.rpc.txPrepare(partition.ctx, request)).toMatchObject({ outcome: "accepted" });

		expect(await locksOf(rangeRoot, request.transactionId)).toEqual([
			{
				key: "bob/x",
				opList: [
					[0, "put"],
					[2, "update"],
				],
				hasData: true,
			},
			{
				key: "bob/y",
				opList: [
					[3, "delete"],
					[4, "put"],
				],
				hasData: true,
			},
		]);
		expect(await locksOf(partition, request.transactionId)).toEqual([{ key: "carol/x", opList: [[1, "put"]], hasData: true }]);

		const commit = { transactionId: request.transactionId, transactionTimestamp: request.transactionTimestamp, items: keysOf(request) };
		expect(await partition.rpc.txCommit(partition.ctx, commit)).toEqual({ outcome: "committed" });
		expect(await dataOf(partition, "bob", "x")).toEqual({ n: 2 });
		expect(await dataOf(partition, "bob", "y")).toBe(2);
		expect(await dataOf(partition, "carol", "x")).toEqual({ c: 1 });
	});
});

describe("PartitionDO — a lock row of an item with more than one operation moves with its key", () => {
	it("moves the operation list and the last-write data in a promotion, and the range root applies them", async () => {
		const partition = makePartition({ hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		const request = prepareOf([put("alice", "locked", { n: 1 }), put("bob", "sk1", "b"), update("alice", "locked")]);
		expect(await partition.rpc.txPrepare(partition.ctx, request)).toMatchObject({ outcome: "accepted" });

		await partition.triggerPromotion("alice");
		const rangeRoot = await partition.awaitPromoted("alice");
		expect(await locksOf(rangeRoot, request.transactionId)).toEqual([
			{
				key: "alice/locked",
				opList: [
					[0, "put"],
					[2, "update"],
				],
				hasData: true,
			},
		]);
		await drainUntil(
			[partition],
			async () => (await lockKeys(partition.stub, request.transactionId)).join() === "bob/sk1",
			"the copy of the moved lock deleted",
		);

		const commit = { transactionId: request.transactionId, transactionTimestamp: request.transactionTimestamp, items: keysOf(request) };
		expect(await partition.rpc.txCommit(partition.ctx, commit)).toEqual({ outcome: "committed" });
		expect(await dataOf(partition, "alice", "locked")).toEqual({ n: 2 });
		expect(await dataOf(partition, "bob", "sk1")).toBe("b");
	}, 30_000);

	it("moves each lock row to its child in a hash split, and each child applies its own entries", async () => {
		const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		const left = findKey("ordered-split", (k) => hashChildIndex(kb(k), 0, 2) === 0);
		const right = findKey("ordered-split", (k) => hashChildIndex(kb(k), 0, 2) === 1);
		const request = prepareOf([
			put(left, "s", { n: 1 }),
			del(right, "s"),
			update(left, "s"),
			put(right, "s", { r: 1 }),
			update(right, "s"),
		]);
		expect(await partition.rpc.txPrepare(partition.ctx, request)).toMatchObject({ outcome: "accepted" });

		await partition.splitHash();
		expect((await partition.childOwning(left)).doName).not.toBe((await partition.childOwning(right)).doName);
		expect(await locksAtLeaf(partition, left, request.transactionId)).toEqual([
			{
				key: `${left}/s`,
				opList: [
					[0, "put"],
					[2, "update"],
				],
				hasData: true,
			},
		]);
		expect(await locksAtLeaf(partition, right, request.transactionId)).toEqual([
			{
				key: `${right}/s`,
				opList: [
					[1, "delete"],
					[3, "put"],
					[4, "update"],
				],
				hasData: true,
			},
		]);

		const commit = { transactionId: request.transactionId, transactionTimestamp: request.transactionTimestamp, items: keysOf(request) };
		expect(await partition.rpc.txCommit(partition.ctx, commit)).toEqual({ outcome: "committed" });
		expect(await dataOf(partition, left, "s")).toEqual({ n: 2 });
		expect(await dataOf(partition, right, "s")).toEqual({ r: 1, n: 1 });
	}, 30_000);
});
