import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionContextCreator, PartitionIdHelper, PartitionTopologyRouterImpl, rangePartitionDoName } from "./partition-topology.js";
import type { PartitionContext, PartitionContextResolved, SplitStatusKVItem } from "./partition-topology.js";
import type { PartitionDO } from "../do-partition.js";

function makeBase(): PartitionContext {
	return PartitionContextCreator.create({
		ns: "PARTITION_DO",
		databaseName: "testdb",
		rootTreesN: 1,
		hashSplitN: 4,
		hashSplitConditions: { maxSizeMb: 100 },
	});
}

describe("rangePartitionDoName", () => {
	it("produces root name (null startBoundary)", () => {
		expect(rangePartitionDoName("mydb", "alice", null)).toBe("mydb.r.alice.");
	});

	it("produces child name with sortKey boundary", () => {
		expect(rangePartitionDoName("mydb", "alice", "b1")).toBe("mydb.r.alice.b1");
	});

	it("percent-encodes dots in hashKey and boundary", () => {
		expect(rangePartitionDoName("mydb", "a.b", "c.d")).toBe("mydb.r.a%2Eb.c%2Ed");
	});

	it("percent-encodes slashes", () => {
		expect(rangePartitionDoName("mydb", "a/b", "c/d")).toBe("mydb.r.a%2Fb.c%2Fd");
	});

	it("leaves [A-Za-z0-9_-] unchanged", () => {
		expect(rangePartitionDoName("db", "Hello_World-123", "sk_value-99")).toBe("db.r.Hello_World-123.sk_value-99");
	});

	it("keeps range names disjoint from hash names (.r. vs .h.)", () => {
		const rangeName = rangePartitionDoName("db", "0", null);
		expect(rangeName).toBe("db.r.0.");
		// Hash root 0 is "db.h.0" — no collision.
		expect(rangeName).not.toBe("db.h.0");
	});
});

describe("PartitionIdHelper — range schema (SCHEMA_RANGE_V1)", () => {
	it("fromRangePartition root: encode then decode round-trips", () => {
		const base = makeBase();
		const helper = PartitionIdHelper.fromRangePartition(base, "alice", null);
		const { bytes, opaque, doName } = helper.encode(true);

		expect(bytes[0]).toBe(PartitionIdHelper.SCHEMA_RANGE_V1);
		expect(doName).toBe("testdb.r.alice.");

		const decoded = PartitionIdHelper.decode(bytes);
		expect(decoded.schema).toBe(1);
		if (decoded.schema === 1) {
			expect(decoded.hashKey).toBe("alice");
			expect(decoded.startBoundary).toBeNull();
		}

		// Opaque round-trip.
		const decoded2 = PartitionIdHelper.decode(Uint8Array.fromHex(opaque));
		expect(decoded2).toEqual(decoded);
	});

	it("fromRangePartition child: encode then decode round-trips with boundary", () => {
		const base = makeBase();
		const helper = PartitionIdHelper.fromRangePartition(base, "alice", "b1");
		const { bytes, doName } = helper.encode(true);

		expect(bytes[0]).toBe(PartitionIdHelper.SCHEMA_RANGE_V1);
		expect(doName).toBe("testdb.r.alice.b1");

		const decoded = PartitionIdHelper.decode(bytes);
		expect(decoded.schema).toBe(1);
		if (decoded.schema === 1) {
			expect(decoded.hashKey).toBe("alice");
			expect(decoded.startBoundary).toBe("b1");
		}
	});

	it("handles unicode in hashKey and startBoundary", () => {
		const base = makeBase();
		const { bytes } = PartitionIdHelper.fromRangePartition(base, "café☕", "töst").encode(false);
		const decoded = PartitionIdHelper.decode(bytes);
		expect(decoded.schema).toBe(1);
		if (decoded.schema === 1) {
			expect(decoded.hashKey).toBe("café☕");
			expect(decoded.startBoundary).toBe("töst");
		}
	});

	it("doName dispatches correctly for range ID loaded from opaque hex", () => {
		const base = makeBase();
		const { opaque } = PartitionIdHelper.fromRangePartition(base, "mykey", "start1").encode(false);
		const bytes = Uint8Array.fromHex(opaque);
		expect(PartitionIdHelper.doName(base, bytes)).toBe("testdb.r.mykey.start1");
	});
});

describe("PartitionIdHelper — hash schema (SCHEMA_HASH_V1) unchanged", () => {
	it("fromHashIdxs root: encode then decode", () => {
		const base = makeBase();
		const { bytes, opaque, doName } = PartitionIdHelper.fromHashIdxs(base, [0]).encode(true);

		expect(bytes[0]).toBe(PartitionIdHelper.SCHEMA_HASH_V1);
		expect(doName).toBe("testdb.h.0");

		const decoded = PartitionIdHelper.decode(bytes);
		expect(decoded.schema).toBe(0);
		if (decoded.schema === 0) {
			expect(decoded.rootIdx).toBe(0);
			expect(decoded.depth).toBe(0);
		}

		const decoded2 = PartitionIdHelper.decode(Uint8Array.fromHex(opaque));
		expect(decoded2).toEqual(decoded);
	});

	it("fromHashIdxs child: appendHashIdx extends depth", () => {
		const base = makeBase();
		const { bytes, doName } = PartitionIdHelper.fromHashIdxs(base, [2]).appendHashIdx(1).encode(true);

		expect(bytes[0]).toBe(PartitionIdHelper.SCHEMA_HASH_V1);
		expect(doName).toBe("testdb.h.2.1");
		expect(PartitionIdHelper.depth(bytes)).toBe(1);
		expect(PartitionIdHelper.lastChildIdx(bytes)).toBe(1);
	});

	it("rootIdx, depth, lastChildIdx assert SCHEMA_HASH_V1", () => {
		const base = makeBase();
		const { bytes } = PartitionIdHelper.fromRangePartition(base, "k", null).encode(false);
		expect(() => PartitionIdHelper.rootIdx(bytes)).toThrow();
		expect(() => PartitionIdHelper.depth(bytes)).toThrow();
		expect(() => PartitionIdHelper.lastChildIdx(bytes)).toThrow();
	});
});

// ─── Phase 2 helpers ──────────────────────────────────────────────────────────

// Each Phase 2 test must use a unique base to avoid DO name collisions between tests.
function makeUniqueBase(overrides?: Partial<PartitionContext>): PartitionContext {
	return PartitionContextCreator.create({
		ns: "PARTITION_DO",
		databaseName: `testdb-${crypto.randomUUID()}`,
		rootTreesN: 1,
		hashSplitN: 4,
		hashSplitConditions: { maxSizeMb: 100 },
		...overrides,
	});
}

function makeRangeCtx(base: PartitionContext, hashKey: string, startBoundary: string | null): PartitionContextResolved {
	const { opaque, doName } = PartitionIdHelper.fromRangePartition(base, hashKey, startBoundary).encode(true);
	const doId = env.PARTITION_DO.idFromName(doName!);
	return {
		...base,
		doName: doName!,
		primaryDoIdStr: doId.toString(),
		partitionId: opaque,
		rangePartition: { hashKey, startBoundary },
	};
}

function makeHashCtx(base: PartitionContext): PartitionContextResolved {
	return new PartitionTopologyRouterImpl("", base).pickPartition("dummyKey").partitionContext;
}

// Sets up a range DO via initFromSplit, immediately marks migration complete,
// and optionally injects an initial split status — all over RPC, no private access.
async function setupRangeDO(
	stub: DurableObjectStub<PartitionDO>,
	pCtx: PartitionContextResolved,
	parentCtx: PartitionContextResolved,
	endBoundary: string | null,
	splitStatus?: SplitStatusKVItem,
): Promise<void> {
	await stub.initFromSplit(
		{ parentPartitionContext: parentCtx, newPartitionContext: pCtx, splitType: "range", rangeEndBoundary: endBoundary },
		true, // __testing__completeMigration
		splitStatus, // __testing__splitStatus
	);
}

// ─── Phase 2: RangePartitionTopologyImpl ──────────────────────────────────────

describe("RangePartitionTopologyImpl — serves/rejects/forwards by sort-key range", () => {
	it("serves sort keys within [start, end) — putItem succeeds locally (forwardCount=0)", async () => {
		const base = makeUniqueBase();
		const rootCtx = makeRangeCtx(base, "alice", null);
		const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		await setupRangeDO(stub, rootCtx, makeHashCtx(base), "m");

		const r = await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "a", data: "v1" });
		expect(r.meta.forwardCount).toBe(0);

		const g = await stub.getItem(rootCtx, { hashKey: "alice", sortKey: "a" });
		expect(g).toMatchObject({ found: true, item: { data: "v1" }, meta: { forwardCount: 0 } });
	});

	it("rejects sort keys outside [start, end) when not split", async () => {
		const base = makeUniqueBase();
		const rootCtx = makeRangeCtx(base, "alice", null);
		const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		await setupRangeDO(stub, rootCtx, makeHashCtx(base), "m");

		await runInDurableObject(stub, async (doInstance: PartitionDO) => {
			await expect(doInstance.putItem(rootCtx, { hashKey: "alice", sortKey: "m", data: "x" })).rejects.toThrow(/exceeded its limits/);
			await expect(doInstance.putItem(rootCtx, { hashKey: "alice", sortKey: "z", data: "x" })).rejects.toThrow(/exceeded its limits/);
		});
	});

	it("unbounded range (endBoundary=null) accepts any sort key", async () => {
		const base = makeUniqueBase();
		const rootCtx = makeRangeCtx(base, "alice", null);
		const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		await setupRangeDO(stub, rootCtx, makeHashCtx(base), null);

		await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "", data: "empty-sk" });
		await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "zzzzz", data: "last" });

		const g = await stub.getItem(rootCtx, { hashKey: "alice", sortKey: "zzzzz" });
		expect(g).toMatchObject({ found: true });
	});

	it("forwards out-of-range sk to child when split is active (forwardCount=1)", async () => {
		const base = makeUniqueBase();
		const rootCtx = makeRangeCtx(base, "alice", null);
		const childCtx = makeRangeCtx(base, "alice", "m");
		const rootStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));

		// Child owns ["m", ∅).
		await setupRangeDO(childStub, childCtx, rootCtx, null);

		// FIXME Remove the internal injection with a proper test!
		// Root owns [∅, "m"); inject the split state so it routes sk >= "m" to the child.
		await setupRangeDO(rootStub, rootCtx, makeHashCtx(base), "m", {
			status: "split_started",
			splitType: "range",
			createdAt: Date.now(),
			partitionContext: rootCtx,
			childPartitionContexts: [childCtx],
			migratedChildDoNames: [],
			history: [],
		} satisfies SplitStatusKVItem);

		// sk in retained slice [∅,"m") — served locally.
		const local = await rootStub.putItem(rootCtx, { hashKey: "alice", sortKey: "a", data: "local" });
		expect(local.meta.forwardCount).toBe(0);

		// sk in child slice ["m",∅) — forwarded once.
		const fwd = await rootStub.putItem(rootCtx, { hashKey: "alice", sortKey: "z", data: "remote" });
		expect(fwd.meta.forwardCount).toBe(1);

		// Item landed in the child, not the root.
		const inChild = await childStub.getItem(childCtx, { hashKey: "alice", sortKey: "z" });
		expect(inChild).toMatchObject({ found: true, item: { data: "remote" } });
	});
});

describe("RangePartitionTopologyImpl — maybeQueueSplit", () => {
	it("queues a range split when db exceeds rangeSplitConditions.maxSizeMb", async () => {
		const base = makeUniqueBase({ rangeSplitN: 2, rangeSplitConditions: { maxSizeMb: 0.1 } });
		const rootCtx = makeRangeCtx(base, "alice", null);
		const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		await setupRangeDO(stub, rootCtx, makeHashCtx(base), null);

		const bigData = "x".repeat(50 * 1024);
		await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "sk1", data: bigData });
		await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "sk2", data: bigData });

		const s = await stub.status(rootCtx);
		expect(s.splitStatus?.status).toBe("split_queued");
		expect(s.splitStatus?.splitType).toBe("range");
	});

	it("does not queue a split when db is within limits", async () => {
		const base = makeUniqueBase(); // rangeSplitConditions.maxSizeMb=500 by default
		const rootCtx = makeRangeCtx(base, "alice", null);
		const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		await setupRangeDO(stub, rootCtx, makeHashCtx(base), null);

		await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "sk", data: "x" });

		const s = await stub.status(rootCtx);
		expect(s.splitStatus).toBeUndefined();
	});
});
