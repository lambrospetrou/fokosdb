import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it, vi } from "vitest";
import { FokosDB, type FokosDBOptions } from "../src/client/db.js";
import type { FokosDBAutoAdminDO } from "../src/server/do-auto-admin.js";
import { KeyCodec } from "../src/sharding/key-codec.js";
import { maybePromoted, parseHashBody, topologyAdminName } from "../src/sharding/topology-codec.js";
import { TOPOLOGY_BLOOM_MIN_CAPACITY } from "../src/sharding/topology-encode.js";
import type { FokosTopologyRead } from "../src/sharding/topology-keeper.js";
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
		topologyEntrypoint: "FokosDBTopologyCache",
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

	it("a coordinator sends each participant to its deepest known partition", async () => {
		const tableName = `topotest.${crypto.randomUUID().replaceAll("-", "")}`;
		const shardGroup = `fokos.p.${tableName}`;
		const db = new FokosDB(options(tableName));
		const keys = await splitRoot(db);
		await vi.waitFor(async () => expect(await servedBitCount(shardGroup)).toBeGreaterThanOrEqual(1 + 2), WAIT);

		// The client reads no topology and has no fast path, so the coordinator resolves the participants.
		const writer = new FokosDB(options(tableName, { useTopologyCache: false, singlePartitionFastPath: false }));
		const write = (n: number) =>
			writer.transactWriteItems({ items: keys.slice(0, 3).map((hashKey) => ({ hashKey, operation: "put" as const, data: `v${n}` })) });
		// The first transaction starts the topology fetch of the coordinator. A later one goes to the leaves.
		await vi.waitFor(async () => {
			const { meta } = await write(1);
			expect(meta.forwardCount).toBe(0);
			expect(meta.partitions.every((p) => (p.hashDepth ?? 0) >= 1)).toBe(true);
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

	it("a Bloom false positive does not cancel a transaction that a coordinator drives", async () => {
		const tableName = `topotest.${crypto.randomUUID().replaceAll("-", "")}`;
		const shardGroup = `fokos.p.${tableName}`;
		// Fill the promoted-key Bloom filter to its capacity, as a table with 16,384 promoted keys has it.
		// The test writes the range roots into the keeper tables, because 16,384 reports take too long.
		const admin = env.FOKOS_ADMIN_DO.getByName(topologyAdminName(shardGroup));
		await runInDurableObject(admin, async (instance: FokosDBAutoAdminDO, state) => {
			state.storage.transactionSync(() => {
				for (let i = 0; i < TOPOLOGY_BLOOM_MIN_CAPACITY; i++) {
					state.storage.sql.exec(
						`INSERT INTO fokos_topo_range_intervals (shard_group, hash_key, start, end_is_max, end, seq) VALUES (?1, ?2, x'', 1, x'', 1)`,
						shardGroup,
						KeyCodec.encode(`promoted-${i}`),
					);
				}
				state.storage.sql.exec(
					`INSERT INTO fokos_topo_groups (shard_group, root_trees_n, hash_split_n, version, hash_version, build_due_at, promoted_n)
					 VALUES (?1, 1, 2, 1, 1, 0, ?2)`,
					shardGroup,
					TOPOLOGY_BLOOM_MIN_CAPACITY,
				);
			});
			await instance.alarm();
		});
		const read = (await admin.getTopology(shardGroup)) as FokosTopologyRead;
		const view = parseHashBody((read as { body: Uint8Array }).body);
		// A key that no repartition promoted, and that the Bloom filter holds: a false positive.
		const candidates = Array.from({ length: 5_000 }, (_, i) => `fp-${i}`);
		const falsePositive = candidates.find((key) => maybePromoted(view, KeyCodec.encode(key)));
		expect(falsePositive).toBeDefined();
		const plain = candidates.find((key) => !maybePromoted(view, KeyCodec.encode(key)))!;

		// The client reads no topology, so only the coordinator and the partitions read it.
		const db = new FokosDB(options(tableName, { useTopologyCache: false, singlePartitionFastPath: false }));
		// The first transaction starts the topology fetch of the coordinator.
		await db.transactWriteItems({
			items: [
				{ hashKey: plain, operation: "put", data: "a" },
				{ hashKey: `${plain}-2`, operation: "put", data: "b" },
			],
		});
		await scheduler.wait(500);
		// The coordinator sees the false positive for the first time, before it has the range entry of the key.
		await db.transactWriteItems({
			items: [
				{ hashKey: falsePositive!, operation: "put", data: "c" },
				{ hashKey: plain, operation: "put", data: "d" },
			],
		});
		expect((await db.getItem({ hashKey: falsePositive! })).found).toBe(true);
	});

	it("refuses topologyEntrypoint without nsAdmin", () => {
		expect(() => new FokosDB(options("topotest.x", { nsAdmin: undefined }))).toThrow(/topologyEntrypoint needs nsAdmin/);
	});
});
