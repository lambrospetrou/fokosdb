/**
 * `apiScanItems`: one step of a scan. A step reads the owned slice that holds its start position, in
 * the storage order `(routingHash, hk, sk)`, and answers with the start of the next slice.
 */
import { describe, expect, it } from "vitest";
import type { ScanItemsRpcRequest, ScanItemsRpcResponse } from "../../src/server/do-partition.js";
import { MAX_EVALUATED_BYTES_PER_PAGE, MAX_EVALUATED_ITEMS_PER_PAGE } from "../../src/shared/query/page-budget.js";
import type { StoredItem } from "../../src/shared/partition/partition-store.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { PartitionIdHelper } from "../../src/sharding/partition-id.js";
import { compareStorageKeys } from "../../src/sharding/repartition-slice.js";
import type { FokosEnvelope, FokosStorageKey } from "../../src/sharding/runtime-types.js";
import { kb } from "./helpers.js";
import { CONTROLLED_NS, makePartition, withMigrationHeld, type TestPartition } from "./partition-harness.js";

const ZERO = new Uint8Array(8);

const stepRequest = (start: FokosStorageKey, overrides: Partial<ScanItemsRpcRequest> = {}): ScanItemsRpcRequest => ({
	start,
	startInclusive: true,
	end: null,
	remainingEvaluatedItems: MAX_EVALUATED_ITEMS_PER_PAGE,
	remainingEvaluatedBytes: MAX_EVALUATED_BYTES_PER_PAGE,
	remainingResponseBytes: 64 * 1024 * 1024,
	allowOversizedFirstItem: true,
	select: "projection",
	plan: null,
	...overrides,
});

async function step(partition: TestPartition, req: ScanItemsRpcRequest): Promise<FokosEnvelope<ScanItemsRpcResponse>> {
	return (await partition.stub.apiScanItems(partition.ctx, req)) as unknown as FokosEnvelope<ScanItemsRpcResponse>;
}

/** The keys of the items of a step, as `hk/sk` text. */
const keysOf = (answer: ScanItemsRpcResponse) =>
	(answer.items as StoredItem[]).map((item) => `${KeyCodec.decode(item.hk) as string}/${KeyCodec.decode(item.sk) as string}`);

/** The `hk/sk` text of each key, in the storage order of `partition`. */
function inStorageOrder(partition: TestPartition, keys: Array<[string, string]>): string[] {
	const position = ([hk, sk]: [string, string]) => ({ routingHash: partition.routingHashOf(hk), hk: kb(hk), sk: kb(sk) });
	return [...keys].sort((a, b) => compareStorageKeys(position(a), position(b))).map(([hk, sk]) => `${hk}/${sk}`);
}

async function putAll(partition: TestPartition, keys: Array<[string, string]>): Promise<void> {
	for (const [hk, sk] of keys) {
		await partition.put({ hashKey: kb(hk), sortKey: kb(sk), data: `${hk}/${sk}`, kind: "text" });
	}
}

const KEYS: Array<[string, string]> = Array.from({ length: 12 }, (_, i) => [`user-${i % 5}`, `s${i}`]);

describe("FokosDBPartitionDO — apiScanItems", () => {
	it("reads the whole slice of an owner in the storage order, and ends the table with no next slice", async () => {
		const partition = makePartition();
		await putAll(partition, KEYS);
		const { value, routing } = await step(partition, stepRequest({ routingHash: ZERO }));
		expect(keysOf(value)).toEqual(inStorageOrder(partition, KEYS));
		expect(value).toMatchObject({ count: KEYS.length, scannedCount: KEYS.length, sliceComplete: true, next: null });
		expect(routing.forwardCount).toBe(0);
	});

	it("stops at the budget with the key of the last evaluated row, and the next step starts after it", async () => {
		const partition = makePartition();
		await putAll(partition, KEYS);
		const expected = inStorageOrder(partition, KEYS);
		const first = (await step(partition, stepRequest({ routingHash: ZERO }, { remainingEvaluatedItems: 5 }))).value;
		expect(keysOf(first)).toEqual(expected.slice(0, 5));
		expect(first.sliceComplete).toBe(false);
		expect(first.next).toBeNull();
		const last = first.lastEvaluated!;
		expect(`${KeyCodec.decode(last.hk) as string}/${KeyCodec.decode(last.sk) as string}`).toBe(expected[4]);

		const after = { routingHash: partition.routingHashOf(KeyCodec.decode(last.hk) as string), hk: last.hk, sk: last.sk };
		const second = (await step(partition, stepRequest(after, { startInclusive: false }))).value;
		expect(keysOf(second)).toEqual(expected.slice(5));
		expect(second.sliceComplete).toBe(true);
	});

	it("gives no last key when the response budget stops the step before its first row", async () => {
		const partition = makePartition();
		await putAll(partition, KEYS);
		const { value } = await step(
			partition,
			stepRequest({ routingHash: ZERO }, { remainingResponseBytes: 1, allowOversizedFirstItem: false }),
		);
		expect(value).toMatchObject({ items: [], scannedCount: 0, lastEvaluated: null, sliceComplete: false, next: null });
	});

	it("ends at the end of the segment, with no next slice", async () => {
		const partition = makePartition();
		await putAll(partition, KEYS);
		const expected = inStorageOrder(partition, KEYS);
		const middle = partition.routingHashOf(expected[6].split("/")[0]);
		const { value } = await step(partition, stepRequest({ routingHash: ZERO }, { end: { routingHash: middle } }));
		expect(keysOf(value)).toEqual(
			expected.filter(
				(key) => compareStorageKeys({ routingHash: partition.routingHashOf(key.split("/")[0]) }, { routingHash: middle }) < 0,
			),
		);
		expect(value).toMatchObject({ sliceComplete: true, next: null });
	});

	it("ends a hash slice at a promoted routingHash, reads the range tree for it, and continues after it", async () => {
		const partition = makePartition();
		const keys: Array<[string, string]> = [...KEYS, ["alice", "a1"], ["alice", "a2"]];
		await putAll(partition, keys);
		await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("alice") });
		const rangeRoot = await partition.awaitPromoted("alice");
		const promoted = partition.routingHashOf("alice");

		const first = (await step(partition, stepRequest({ routingHash: ZERO }))).value;
		expect(first.next).toEqual({ routingHash: promoted });
		const tree = await step(partition, stepRequest(first.next!));
		expect(tree.routing.servedBy[0].ref.doName).toBe(rangeRoot.doName);
		expect(keysOf(tree.value)).toEqual(["alice/a1", "alice/a2"]);
		const next = tree.value.next!;
		expect(compareStorageKeys(next, { routingHash: promoted })).toBeGreaterThan(0);
		expect(next.hk).toBeUndefined();
		const rest = (await step(partition, stepRequest(next))).value;
		expect(rest.next).toBeNull();
		// The hash partition keeps stale copies of the promoted rows until its cleanup, and no step reads them.
		expect([...keysOf(first), ...keysOf(tree.value), ...keysOf(rest)]).toEqual(inStorageOrder(partition, keys));
	});

	it("reads only the caller slice when a hash child reads through its source", { concurrent: false }, async () => {
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		await putAll(partition, KEYS);
		await withMigrationHeld(partition, async (waitForAllChildRequests) => {
			const filler = await partition.triggerHashSplit();
			await partition.awaitSplitStarted();
			await waitForAllChildRequests();
			const all: Array<[string, string]> = [
				...KEYS,
				...filler.map((item): [string, string] => [KeyCodec.decode(item.hashKey) as string, KeyCodec.decode(item.sortKey!) as string]),
			];
			for (const child of await partition.children()) {
				const { start, end } = PartitionIdHelper.routingHashInterval(child.ctx.partitionId, child.ctx.topology);
				const { value, routing } = await step(child, stepRequest({ routingHash: start }, { select: "count" }));
				expect(routing.servedBy.find((n) => n.ref.doName === child.doName)?.role).toBe("read_through");
				const inChild = all.filter(([hk]) => {
					const rh = { routingHash: partition.routingHashOf(hk) };
					return compareStorageKeys(rh, { routingHash: start }) >= 0 && (end === null || compareStorageKeys(rh, { routingHash: end }) < 0);
				});
				expect(value).toMatchObject({ scannedCount: inChild.length, sliceComplete: true, next: end && { routingHash: end } });
			}
		});
	});
});
