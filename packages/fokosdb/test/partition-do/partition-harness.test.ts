/**
 * The partition harness. A wait for a split or a migration fails when the tree makes no progress, and
 * not when a period of time ends. A test control needs a partition of `ControlledPartitionDO`.
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
});

describe("TestPartition.controlled", () => {
	it("refuses a partition outside the controlled namespace", () => {
		const partition = makePartition();

		expect(() => partition.controlled).toThrow(/a test control needs a partition in CONTROLLED_PARTITION_DO/);
	});
});
