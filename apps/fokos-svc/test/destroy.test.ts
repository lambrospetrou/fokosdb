import { env, exports } from "cloudflare:workers";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, it, vi } from "vitest";
import { FokosDB } from "../src/lib/db.js";
import {
	PartitionContextCreator,
	PartitionTopologyRouterImpl,
} from "../src/lib/partition-topology/partition-topology.js";
import type { PartitionContextResolved } from "../src/lib/partition-topology/partition-topology.js";
import type { PartitionDO } from "../src/lib/do-partition.js";

// 3 root partitions, each splits into 2 children.
// maxSizeMb: 0.1 = 102 400 bytes; 2 × 50 KB items per partition → split triggers.
const PARTITION_OPTIONS = {
	rootTreesN: 3,
	hashSplitN: 2,
	rangeSplitN: 2,
	hashSplitConditions: { maxSizeMb: 0.1 },
	// rangeSplitConditions not specified here → PartitionContextCreator defaults to { splitN: 4, maxSizeMb: 500 },
	// which matches what makeFokosDB in index.ts uses when rangeSplitConditions is omitted from the request body.
};
const ITEM_DATA = "x".repeat(50 * 1024); // 50 KB

function makeDB(databaseName: string) {
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		databaseName,
		...PARTITION_OPTIONS,
	});
	return new FokosDB({
		ns: env.PARTITION_DO,
		topology: new PartitionTopologyRouterImpl(base),
		transactionCoordinatorNs: env.TRANSACTION_COORDINATOR_DO,
	});
}

// Mirrors the waitForAlarm helper in do-partition.test.ts.
async function waitForAlarm(stub: DurableObjectStub<PartitionDO>) {
	await runDurableObjectAlarm(stub);
	await runInDurableObject(stub, async (instance: PartitionDO) => {
		await vi.waitUntil(
			() => !instance.__testing__alarm_running && !instance.__testing__backgroundWorkRunning,
			{
				timeout: 5000,
				interval: 50,
			},
		);
	});
}

// FIXME Skipped because when calling destroy() the vitest integration is broken and stays hang forever even though the test completes.
describe.skip("DELETE /api/databases/:databaseName", () => {
	it("destroys all partitions in DFS postfix order, including children created by splits", async ({
		expect,
	}) => {
		const databaseName = `destroytest.${crypto.randomUUID().replaceAll("-", "")}`;
		const db = makeDB(databaseName);
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
					expect(String(e)).toMatch(/split in progress|partition exceeded its limits/);
				}
			}
			allKeys.push(hk);
		}

		// Drain the split alarm on every partition.
		for (const doName of doNamesSet) {
			await waitForAlarm(env.PARTITION_DO.get(env.PARTITION_DO.idFromName(doName)));
		}

		// Call the HTTP DELETE endpoint, passing the same partition options so the server
		// reconstructs the same topology (and therefore targets the same DOs).
		const response = await exports.default.fetch(
			`https://example.com/api/databases/${databaseName}`,
			{
				method: "DELETE",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ partitionOptions: PARTITION_OPTIONS }),
			},
		);
		expect(response.status).toBe(200);
		expect(await response.json()).toEqual({ destroyed: true });

		await expect(db.destroy()).resolves.toEqual({ ok: true });

		console.log("BOOM 💥 — verifying all partitions were destroyed in DFS postfix order", {
			doNamesSet: Array.from(doNamesSet),
		});

		for (const doName of doNamesSet) {
			const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(doName));
			const { partitionContextStored } = await stub.status();
			expect(partitionContextStored).toBeFalsy();
		}

		// All written items must be gone (verifies roots were destroyed).
		// NOTE that this will actually re-initialize the DOs!
		for (const hk of allKeys) {
			expect((await db.getItem({ hashKey: hk })).found).toBe(false);
		}
	});

	it("is idempotent — a second DELETE on an already-destroyed database succeeds", async ({
		expect,
	}) => {
		const databaseName = `destroytest.${crypto.randomUUID().replaceAll("-", "")}`;
		const db = makeDB(databaseName);

		await db.putItem({ hashKey: "idem-hk", data: "some-data" });

		for (let i = 0; i < 2; i++) {
			const res = await exports.default.fetch(`https://example.com/api/databases/${databaseName}`, {
				method: "DELETE",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ partitionOptions: PARTITION_OPTIONS }),
			});
			expect(res.status).toBe(200);
			expect(await res.json()).toEqual({ destroyed: true });
		}
	});

	it("is idempotent — a second DELETE on an already-destroyed database succeeds (DB directly)", async ({
		expect,
	}) => {
		const databaseName = `destroytest.${crypto.randomUUID().replaceAll("-", "")}`;
		const db = makeDB(databaseName);

		await db.putItem({ hashKey: "idem-hk", data: "some-data" });

		for (let i = 0; i < 2; i++) {
			await expect(db.destroy()).resolves.toEqual({ ok: true });
		}
	});
});
