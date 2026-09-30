/**
 * A promotion copies every row of its key into a range root, and the import does not check the cap
 * of the range root. A range cap far below the hash cap can therefore leave the range root above its
 * admission limit when its import completes. The first write that the range root refuses queues its
 * range split.
 */
import { describe, expect, it, vi } from "vitest";
import { fokosErrorWith } from "../errors-matchers.js";
import { kb } from "../partition-do/helpers.js";
import { makePartition } from "../partition-do/partition-harness.js";

describe("Repartition - a range root that a promotion leaves over its cap", () => {
	it("refuses the first write, then splits, and a later write succeeds", async () => {
		const partition = makePartition({ rangeSplitN: 4, rangeSplitConditions: { maxSizeMb: 0.3 } });
		for (let i = 0; i < 40; i++) {
			const sortKey = `sk${String(i).padStart(2, "0")}`;
			await partition.put({ hashKey: kb("alice"), sortKey: kb(sortKey), data: "x".repeat(16 * 1024), kind: "text" });
		}
		await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("alice") });
		const root = await partition.awaitPromoted("alice");
		expect((await root.status()).splitStatus).toBeUndefined();

		const write = () => partition.put({ hashKey: kb("alice"), sortKey: kb("new"), data: "v", kind: "text" });
		await expect(write()).rejects.toThrow(fokosErrorWith("partition_over_size"));
		await vi.waitFor(async () => expect((await root.status()).splitStatus).toBeDefined(), { timeout: 5_000, interval: 10 });
		await root.awaitSplitCompleted();

		await write();
		expect(await partition.get({ hashKey: kb("alice"), sortKey: kb("sk39") })).toMatchObject({ found: true });
	});
});
