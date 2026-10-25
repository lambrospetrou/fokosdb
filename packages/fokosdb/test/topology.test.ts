import { runDurableObjectAlarm } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it, vi } from "vitest";
import { FokosDB, type FokosDBOptions } from "../src/client/db.js";
import { topologyAdminName } from "../src/sharding/topology-codec.js";
import { parseHashBody } from "../src/sharding/topology-codec.js";
import type { FokosTopologyRead } from "../src/sharding/topology-keeper.js";
import { hashSplitReport } from "./sharding/topology-events.js";
import { testPartitionStub } from "./stub-helpers.js";

// maxSizeMb 0.25 is 262,144 bytes. The empty schema holds about 100 KB, so a few 50 KB items split a root.
const ITEM_DATA = "x".repeat(50 * 1024);
const WAIT = { timeout: 20_000, interval: 100 };

function options(tableName: string, extra: Partial<FokosDBOptions> = {}): FokosDBOptions {
	return {
		table: { name: tableName, ns: "PARTITION_DO", nsTx: "TRANSACTION_COORDINATOR_DO", rootTreesN: 1, hashSplitN: 2 },
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: 0.25 },
		nsAdmin: "FOKOS_ADMIN_DO",
		topologyEntrypoint: "FokosTopologyCache",
		...extra,
	};
}

async function servedBitCount(shardGroup: string): Promise<number> {
	const read = (await env.FOKOS_ADMIN_DO.getByName(topologyAdminName(shardGroup)).getTopology(shardGroup)) as FokosTopologyRead;
	return parseHashBody((read as { body: Uint8Array }).body).bitCount;
}

/** Writes items until the root of the table splits, and returns the keys. */
async function splitRoot(db: FokosDB): Promise<string[]> {
	const keys: string[] = [];
	const doNames = new Set<string>();
	for (let i = 0; i < 10; i++) {
		const hashKey = `item-${i}`;
		for (let attempt = 0; attempt < 20; attempt++) {
			try {
				const { meta } = await db.putItem({ hashKey, data: ITEM_DATA });
				doNames.add(meta.partitions[0].servedByActorName!);
				break;
			} catch (e) {
				expect(["partition_migrating", "partition_over_size"]).toContain((e as { code?: string }).code);
			}
		}
		keys.push(hashKey);
	}
	for (const doName of doNames) {
		await runDurableObjectAlarm(testPartitionStub(doName));
	}
	return keys;
}

describe("FokosDB — encoded topology", () => {
	it("reports a split to the admin, and a new client enters at the leaf with no forward", async () => {
		const tableName = `topotest.${crypto.randomUUID().replaceAll("-", "")}`;
		const shardGroup = `fokos.p.${tableName}`;
		const db = new FokosDB(options(tableName));
		const keys = await splitRoot(db);
		await vi.waitFor(async () => expect(await servedBitCount(shardGroup)).toBeGreaterThanOrEqual(1 + 2), WAIT);

		const reader = new FokosDB(options(tableName, { topologyRefreshMs: 0 }));
		// The first read enters at the root and starts the fetch. A later read enters at the leaf.
		await vi.waitFor(async () => {
			const { meta, found } = await reader.getItem({ hashKey: keys[0] });
			expect(found).toBe(true);
			expect(meta.forwardCount).toBe(0);
			expect(meta.partitions[0].hashDepth).toBeGreaterThanOrEqual(1);
		}, WAIT);
	});

	it("with useTopologyCache false, a request enters at the root", async () => {
		const tableName = `topotest.${crypto.randomUUID().replaceAll("-", "")}`;
		const shardGroup = `fokos.p.${tableName}`;
		const db = new FokosDB(options(tableName));
		const keys = await splitRoot(db);
		await vi.waitFor(async () => expect(await servedBitCount(shardGroup)).toBeGreaterThanOrEqual(1 + 2), WAIT);

		const reader = new FokosDB(options(tableName, { useTopologyCache: false, topologyRefreshMs: 0 }));
		for (let i = 0; i < 3; i++) {
			const { meta } = await reader.getItem({ hashKey: keys[0] });
			expect(meta.forwardCount).toBeGreaterThanOrEqual(1);
			await scheduler.wait(100);
		}
	});

	it("destroy resets the topology of the table", async () => {
		const tableName = `topotest.${crypto.randomUUID().replaceAll("-", "")}`;
		const shardGroup = `fokos.p.${tableName}`;
		const db = new FokosDB(options(tableName));
		await splitRoot(db);
		await vi.waitFor(async () => expect(await servedBitCount(shardGroup)).toBeGreaterThanOrEqual(1 + 2), WAIT);
		await db.destroy();
		expect(await servedBitCount(shardGroup)).toBe(0);
	});

	it("a stale split hint does not cancel a transaction that a coordinator drives", async () => {
		const tableName = `topotest.${crypto.randomUUID().replaceAll("-", "")}`;
		const shardGroup = `fokos.p.${tableName}`;
		// A report of a split that did not occur: the children of the root do not exist.
		await env.FOKOS_ADMIN_DO.getByName(topologyAdminName(shardGroup)).registerRepartition(
			hashSplitReport(shardGroup, { rootTreesN: 1, hashSplitN: 2 }, 0, []),
		);
		await vi.waitFor(async () => expect(await servedBitCount(shardGroup)).toBe(1 + 2), WAIT);
		// The client reads no topology, so only the coordinator and the partitions can read the hint.
		const db = new FokosDB(options(tableName, { useTopologyCache: false, singlePartitionFastPath: false }));
		const write = (n: number) =>
			db.transactWriteItems({
				items: [
					{ hashKey: `tx-${n}-a`, operation: "put", data: "a" },
					{ hashKey: `tx-${n}-b`, operation: "put", data: "b" },
				],
			});
		// The first transaction starts the topology fetch of the coordinator and of the root.
		await write(0);
		await scheduler.wait(500);
		for (let n = 1; n <= 3; n++) {
			await write(n);
			expect((await db.getItem({ hashKey: `tx-${n}-b` })).found).toBe(true);
		}
	});

	it("refuses topologyEntrypoint without nsAdmin", () => {
		expect(() => new FokosDB(options("topotest.x", { nsAdmin: undefined }))).toThrow(/topologyEntrypoint needs nsAdmin/);
	});
});
