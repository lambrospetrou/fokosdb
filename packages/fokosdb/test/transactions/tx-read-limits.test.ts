import { describe, expect, it } from "vitest";
import { MAX_ITEM_BYTES, MAX_ITEMS_PER_REQUEST, MAX_PAYLOAD_BYTES_PER_TX } from "../../src/shared/transaction-limits.js";
import { recordedTxCalls } from "../controlled-partition-do.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { controlledPartition, keysInOnePartition, makeDB, partitionNameOf } from "./tx-helpers.js";

/** `count` keys of one hash key, so they are in one partition. */
const keysOfHashKey = (hashKey: string, count: number) => Array.from({ length: count }, (_, i) => ({ hashKey, sortKey: `sk-${i}` }));

// 42 items of binary data just under the item limit count more than MAX_PAYLOAD_BYTES_PER_TX.
const LARGE_ITEMS = 42;
const largeData = () => new Uint8Array(MAX_ITEM_BYTES - 1024);

describe("transactGetItems — the limits", () => {
	it("sends the data only in the first phase", async () => {
		const db = makeDB({ singlePartitionFastPath: false, controlled: true });
		const key = keysInOnePartition(db, 1, "versions-only")[0];
		await db.putItem({ ...key, data: "value" });

		const result = await db.transactGetItems({ items: [key] });

		expect(result.items).toMatchObject([{ found: true, data: "value", version: 1 }]);
		const calls = await recordedTxCalls(controlledPartition(db, key), "txReadForTransaction");
		expect(calls.map((call) => call.items[0].projection)).toEqual([undefined, [{ expr: { ref: "hashKey" } }]]);
	});

	it("reads MAX_ITEMS_PER_REQUEST items", async () => {
		const db = makeDB();
		const keys = Array.from({ length: MAX_ITEMS_PER_REQUEST }, (_, i) => ({ hashKey: `max-items-${i}`, sortKey: "sk" }));
		const result = await db.transactGetItems({ items: keys });
		expect(result.items).toHaveLength(MAX_ITEMS_PER_REQUEST);
		expect(result.items.every((item) => !item.found)).toBe(true);
	});

	// The partition stops when its items are above the limit, and the read does not send again.
	for (const singlePartitionFastPath of [true, false]) {
		it(`refuses a response above MAX_PAYLOAD_BYTES_PER_TX from one partition, with no retry (fast path: ${singlePartitionFastPath})`, async () => {
			const db = makeDB({ singlePartitionFastPath, controlled: true });
			const keys = keysOfHashKey(`large-one-${singlePartitionFastPath}`, LARGE_ITEMS);
			const data = largeData();
			await db.batchWriteItems({ items: keys.map((key) => ({ ...key, operation: "put" as const, data })) });

			await expect(db.transactGetItems({ items: keys })).rejects.toThrow(
				fokosErrorWith("transact_response_too_large", { limitBytes: MAX_PAYLOAD_BYTES_PER_TX }),
			);
			const partition = controlledPartition(db, keys[0]);
			expect(await recordedTxCalls(partition, singlePartitionFastPath ? "txReadSnapshot" : "txReadForTransaction")).toHaveLength(1);
		});
	}

	// Each partition is below the limit, and the client refuses the total before the second phase.
	it("refuses a response above MAX_PAYLOAD_BYTES_PER_TX across partitions", async () => {
		const db = makeDB();
		// Half of the items in each of two partitions, so each partition is below the limit.
		const first = keysOfHashKey("large-across-a", LARGE_ITEMS / 2);
		let second = first;
		for (let i = 0; partitionNameOf(db, second[0]) === partitionNameOf(db, first[0]); i++) {
			second = keysOfHashKey(`large-across-b${i}`, LARGE_ITEMS / 2);
		}
		const keys = [...first, ...second];
		const data = largeData();
		await db.batchWriteItems({ items: keys.map((key) => ({ ...key, operation: "put" as const, data })) });

		await expect(db.transactGetItems({ items: keys })).rejects.toThrow(fokosErrorWith("transact_response_too_large"));
		// A projection makes the same read small.
		const projected = await db.transactGetItems({ items: keys.map((key) => ({ ...key, projection: [{ expr: { ref: "hashKey" } }] })) });
		expect(projected.items).toHaveLength(LARGE_ITEMS);
	});
});
