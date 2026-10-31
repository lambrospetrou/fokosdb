import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it, vi } from "vitest";
import { FokosDB, type FokosDBOptions } from "../src/client/db.js";
import type { FokosDBAutoAdminDO } from "../src/server/do-auto-admin.js";
import { KeyCodec } from "../src/sharding/key-codec.js";
import { namespaceFingerprint, type DataNamespaceKey } from "../src/shared/do-stubs.js";
import { topologyAdminName } from "../src/shared/partition-context.js";
import { maybePromoted, parseHashBody } from "../src/sharding/topology-codec.js";
import { TOPOLOGY_BLOOM_MIN_CAPACITY } from "../src/sharding/topology-encode.js";
import type { FokosTopologyRead } from "../src/sharding/topology-keeper.js";
import { TARGET_WAIT } from "./partition-do/helpers.js";
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

/** The admin Durable Object of `shardGroup`, which the namespace `binding` holds. */
function adminOf(shardGroup: string, binding: DataNamespaceKey = "PARTITION_DO") {
	return env.FOKOS_ADMIN_DO.getByName(topologyAdminName(shardGroup, namespaceFingerprint(env, binding, undefined)));
}

async function servedBitCount(shardGroup: string): Promise<number> {
	const read = (await adminOf(shardGroup).getTopology(shardGroup)) as FokosTopologyRead;
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
		const admin = adminOf(shardGroup);
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

	it("destroy resets the topology of the coordinator, materialization, and GSI shard groups", async () => {
		const tableName = `topotest.${crypto.randomUUID().replaceAll("-", "")}`;
		const db = new FokosDB(
			options(tableName, {
				table: { ...options(tableName).table, nsMat: "GSI_MATERIALIZATION_DO" },
				captureChanges: true,
			}),
		);
		await db.putItem({ hashKey: "o1", sortKey: "s", data: { status: "OPEN", at: "t1", owner: "u" } });
		await vi.waitFor(
			async () => expect((await db.gsi("byStatus").queryItems({ queries: [{ hashKey: "OPEN" }] })).items).toHaveLength(1),
			TARGET_WAIT,
		);
		await db.destroy();

		const fp = namespaceFingerprint(env, "PARTITION_DO", undefined);
		const shardGroups: Array<[string, DataNamespaceKey]> = [
			[`fokos.p.${tableName}`, "PARTITION_DO"],
			[`fokos.tc.${tableName}.${fp}`, "TRANSACTION_COORDINATOR_DO"],
			[`fokos.m.${tableName}.${fp}`, "GSI_MATERIALIZATION_DO"],
			[`fokos.gsiv1.${tableName.length}.${tableName}.byStatus`, "PARTITION_DO"],
		];
		for (const [shardGroup, binding] of shardGroups) {
			const epoch = await runInDurableObject(
				adminOf(shardGroup, binding),
				(_: FokosDBAutoAdminDO, state) =>
					state.storage.sql.exec<{ epoch: number }>(`SELECT epoch FROM fokos_topo_groups WHERE shard_group = ?1`, shardGroup).one().epoch,
			);
			expect(epoch, shardGroup).toBe(1);
		}
	});

	it("refuses topologyEntrypoint without nsAdmin", () => {
		expect(() => new FokosDB(options("topotest.x", { nsAdmin: undefined }))).toThrow(/topologyEntrypoint needs nsAdmin/);
	});
});

describe("FokosDB — two tables with the same name on two ns namespaces", () => {
	// Both tables share nsTx, nsMat, and nsAdmin. Only the ns namespace is different.
	function sameNameTables() {
		const tableName = `topotest.${crypto.randomUUID().replaceAll("-", "")}`;
		const make = (ns: "PARTITION_DO" | "CUSTOM_PARTITION_DO") =>
			new FokosDB(
				options(tableName, {
					table: { ...options(tableName).table, ns, nsMat: "GSI_MATERIALIZATION_DO" },
					captureChanges: true,
				}),
			);
		return { tableName, a: make("PARTITION_DO"), b: make("CUSTOM_PARTITION_DO") };
	}

	it("each table reports its splits to its own admin Durable Object", async () => {
		const { tableName, a } = sameNameTables();
		const shardGroup = `fokos.p.${tableName}`;
		await splitRoot(a);
		await vi.waitFor(async () => expect(await servedBitCount(shardGroup)).toBeGreaterThanOrEqual(1 + 2), WAIT);
		const read = (await adminOf(shardGroup, "CUSTOM_PARTITION_DO").getTopology(shardGroup)) as FokosTopologyRead;
		expect(parseHashBody((read as { body: Uint8Array }).body).bitCount).toBe(0);
	});

	it("one idempotency token with the same items applies the writes of each table", async () => {
		const { a, b } = sameNameTables();
		const items = [{ hashKey: "k1", operation: "put" as const, data: "v" }];
		await a.transactWriteItems({ items, clientRequestToken: "same-token" });
		await b.transactWriteItems({ items, clientRequestToken: "same-token" });
		expect((await a.getItem({ hashKey: "k1" })).found).toBe(true);
		expect((await b.getItem({ hashKey: "k1" })).found).toBe(true);
	});

	it("the GSI entries of each table stay in its own ns namespace", async () => {
		const { a, b } = sameNameTables();
		await a.putItem({ hashKey: "oa", sortKey: "s", data: { status: "OPEN", at: "t1", owner: "a" } });
		await b.putItem({ hashKey: "ob", sortKey: "s", data: { status: "OPEN", at: "t1", owner: "b" } });
		for (const [db, baseHashKey] of [
			[a, "oa"],
			[b, "ob"],
		] as const) {
			await vi.waitFor(async () => {
				const { items } = await db.gsi("byStatus").queryItems({ queries: [{ hashKey: "OPEN" }] });
				expect(items.map((item) => item.baseHashKey)).toEqual([baseHashKey]);
			}, TARGET_WAIT);
		}
	});
});
