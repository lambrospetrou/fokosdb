import { env } from "cloudflare:workers";
import { describe, expect, it, vi } from "vitest";
import { FokosDB } from "../../src/client/db.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { keysAcrossPartitions, partitionNameOf, writeOutcomeWithClockRetry } from "./tx-helpers.js";

/** A read transaction must not return items that never held a common committed state. */
describe("transactions - TTL expiry during a multi-partition read", () => {
	it("rejects a mixed read after TTL expiry and recreation", async () => {
		vi.useRealTimers();
		const db = new FokosDB({
			table: {
				name: `txtest.${crypto.randomUUID()}`,
				ns: "READ_TTL_RECREATE_PARTITION_DO",
				nsTx: "CONTROLLED_TRANSACTION_COORDINATOR_DO",
				rootTreesN: 100,
				hashSplitN: 2,
				coordinatorRootsN: 1,
			},
			rangeSplitN: 2,
			hashSplitConditions: { maxSizeMb: 100 },
			rangeSplitConditions: { maxSizeMb: 500 },
			singlePartitionFastPath: false,
		});
		const [a, b] = keysAcrossPartitions(db, 2, "ttl-recreate-read");
		const ttlAt = Math.floor(Date.now() / 1000) + 5;
		await db.putItem({ ...a, data: "old-a", ttlAt });
		await db.putItem({ ...b, data: "old-b" });

		const partitionA = env.READ_TTL_RECREATE_PARTITION_DO.getByName(partitionNameOf(db, a));
		const partitionB = env.READ_TTL_RECREATE_PARTITION_DO.getByName(partitionNameOf(db, b));
		await partitionA.testHoldReadPhase();
		await partitionB.testHoldReadBefore();
		const read = db.transactGetItems({ items: [a, b] });
		read.catch(() => {});
		try {
			await vi.waitFor(async () => expect(await partitionA.testReadPhaseParked()).toBe(true), { timeout: 5_000, interval: 10 });
			await vi.waitFor(async () => expect(await partitionB.testReadBeforeParked()).toBe(true), { timeout: 5_000, interval: 10 });
			expect(await db.getItem(a)).toMatchObject({ found: true, item: { data: "old-a" } });
			await partitionA.testReleaseReadPhase();

			await vi.waitFor(() => expect(Math.floor(Date.now() / 1000)).toBeGreaterThan(ttlAt), { timeout: 10_000, interval: 100 });
			await vi.waitFor(async () => expect(await db.getItem(a)).toMatchObject({ found: false }), {
				timeout: 15_000,
				interval: 100,
			});

			const write = await writeOutcomeWithClockRetry(db, {
				items: [
					{ ...a, operation: "put", data: "new-a" },
					{ ...b, operation: "put", data: "new-b" },
				],
			});
			expect(write.outcome).toBe("committed");
			expect(await db.getItem(a)).toMatchObject({ found: true, item: { data: "new-a" } });
			expect(await db.getItem(b)).toMatchObject({ found: true, item: { data: "new-b" } });

			await partitionB.testReleaseReadBefore();
			await expect(read).rejects.toThrow(fokosErrorWith("read_conflict", { hashKey: a.hashKey }));
		} finally {
			await partitionA.testReleaseReadPhase();
			await partitionB.testReleaseReadBefore();
		}
	});
});
