/**
 * `apiBatchWriteItems` while a child of a hash split imports. The test holds the migration on a
 * partition of `ControlledPartitionDO`, so the tests of this file run one after the other.
 */
import { describe, expect, it } from "vitest";
import type { BatchWriteItemsRpcResponse } from "../../src/server/do-partition.js";
import type { FokosEnvelope } from "../../src/sharding/runtime-types.js";
import { kb } from "./helpers.js";
import { CONTROLLED_NS, makePartition, withMigrationHeld } from "./partition-harness.js";

describe("FokosDBPartitionDO — apiBatchWriteItems on an importing child", () => {
	it("answers unprocessed with partition_migrating and applies nothing", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		await withMigrationHeld(partition, async (waitForAllChildRequests) => {
			await partition.triggerHashSplit();
			await partition.awaitSplitStarted();
			await waitForAllChildRequests();
			const child = await partition.childOwning("k");

			const { value } = (await partition.stub.apiBatchWriteItems(partition.ctx, {
				items: [{ opIndex: 0, hashKey: kb("k"), sortKey: kb("1"), operation: "put", data: "v", kind: "text" }],
			})) as unknown as FokosEnvelope<BatchWriteItemsRpcResponse>;

			expect(value.failed).toEqual([
				{ outcome: "unprocessed", code: "partition_migrating", error_id: expect.any(String), opIndexes: [0], partitionIndex: 0 },
			]);
			expect(value.partitions).toEqual([{ actorId: child.stub.id.toString(), hashDepth: 1 }]);
		});
		expect((await partition.get({ hashKey: kb("k"), sortKey: kb("1") })).found).toBe(false);
	});
});
