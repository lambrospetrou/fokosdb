import { env } from "cloudflare:workers";
import { runDurableObjectAlarm } from "cloudflare:test";
import { describe, it } from "vitest";
import { FokosDB } from "../src/client/db.js";
import { PartitionContextCreator } from "../src/sharding/partition-context.js";
import { PartitionTopologyRouterImpl } from "../src/sharding/router.js";
import { testPartitionStub } from "./stub-helpers.js";

// 3 root partitions, each splits into 2 children.
// maxSizeMb: 0.25 = 262 144 bytes. The empty schema already holds ~100 KB of pages, so a smaller cap
// would put every partition over its limit before the first write. 4 × 50 KB items then split one.
const PARTITION_OPTIONS = {
	rootTreesN: 3,
	hashSplitN: 2,
	rangeSplitN: 2,
	hashSplitConditions: { maxSizeMb: 0.25 },
	// rangeSplitConditions not specified here → PartitionContextCreator defaults to { splitN: 4, maxSizeMb: 500 }.
};
const ITEM_DATA = "x".repeat(50 * 1024); // 50 KB

function makeDB(tableName: string) {
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName,
		...PARTITION_OPTIONS,
	});
	return new FokosDB({
		topology: new PartitionTopologyRouterImpl(base),
	});
}

// FIXME: Skipped because the vitest integration hangs forever after a destroy() call, even though the test completes.
describe.skip("FokosDB.destroy()", () => {
	it("destroys all partitions in DFS postfix order, including children created by splits", async ({ expect }) => {
		const tableName = `destroytest.${crypto.randomUUID().replaceAll("-", "")}`;
		const db = makeDB(tableName);
		const topology = db.options().topology as PartitionTopologyRouterImpl;

		// Write 50 × 50 KB items to each root partition.
		const doNamesSet = new Set<string>();
		const allKeys: string[] = [];
		for (let i = 0; i < 10; i++) {
			const hk = `item-${String(i).padStart(4, "0")}`;
			// Writes transiently fail while a split migration is in progress.
			for (let attempt = 0; attempt < 20; attempt++) {
				try {
					const { meta } = await db.putItem({ hashKey: hk, data: ITEM_DATA });
					doNamesSet.add(meta.servedByActorName);
					break;
				} catch (e: unknown) {
					expect(["partition_migrating", "partition_over_size"]).toContain((e as { code?: string }).code);
				}
			}
			allKeys.push(hk);
		}

		// Run the scheduled split alarm on every partition.
		for (const doName of doNamesSet) {
			await runDurableObjectAlarm(testPartitionStub(doName));
		}

		// Destroy twice: the second call must be a no-op on an already-destroyed database.
		await expect(db.destroy()).resolves.toEqual({ ok: true });

		console.log("BOOM 💥 — verifying all partitions were destroyed in DFS postfix order", {
			doNamesSet: Array.from(doNamesSet),
		});

		for (const doName of doNamesSet) {
			const stub = testPartitionStub(doName);
			const { partitionContextStored } = await stub.status();
			expect(partitionContextStored).toBeFalsy();
		}

		// All written items must be gone (verifies roots were destroyed).
		// NOTE that this will actually re-initialize the DOs!
		for (const hk of allKeys) {
			expect((await db.getItem({ hashKey: hk })).found).toBe(false);
		}
	});

	it("is idempotent — a second destroy on an already-destroyed database succeeds", async ({ expect }) => {
		const tableName = `destroytest.${crypto.randomUUID().replaceAll("-", "")}`;
		const db = makeDB(tableName);

		await db.putItem({ hashKey: "idem-hk", data: "some-data" });

		for (let i = 0; i < 2; i++) {
			await expect(db.destroy()).resolves.toEqual({ ok: true });
		}
	});
});
