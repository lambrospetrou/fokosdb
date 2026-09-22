/**
 * The drive loop of the partition harness. A wait for a split or a migration fails when the tree
 * makes no progress, and not when a period of time ends.
 */
import { describe, expect, it } from "vitest";
import { drainUntil, makePartition } from "./partition-harness.js";

describe("drainUntil", () => {
	it("fails when a partition has no work, before the test timeout", async () => {
		const partition = makePartition();
		await partition.status();

		await expect(drainUntil([partition], async () => false, "a condition that never holds")).rejects.toThrow(
			/^a condition that never holds: no progress in \d+ rounds; /,
		);
	});

	it("drives a split to completion", async () => {
		const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		await partition.triggerHashSplit();

		await drainUntil(
			[partition, ...partition.hashChildren()],
			async () => (await partition.status()).splitStatus?.status === "split_completed",
			"split completion",
		);
	});
});
