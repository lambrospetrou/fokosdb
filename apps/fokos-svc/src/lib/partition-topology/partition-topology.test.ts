import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
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
	it("produces root name (null start/end → ~min/~max sentinels)", () => {
		expect(rangePartitionDoName("mydb", "alice", null, null)).toBe("mydb.r.alice.~min.~max");
	});

	it("produces child name with explicit start and end boundaries", () => {
		expect(rangePartitionDoName("mydb", "alice", "b1", "b2")).toBe("mydb.r.alice.b1.b2");
	});

	it("renders half-bounded edges with one sentinel (leftmost / rightmost child)", () => {
		expect(rangePartitionDoName("mydb", "alice", null, "m")).toBe("mydb.r.alice.~min.m");
		expect(rangePartitionDoName("mydb", "alice", "m", null)).toBe("mydb.r.alice.m.~max");
	});

	it("escapes a real boundary that looks like a sentinel (collision-proofness)", () => {
		// A literal "~min" boundary value is escaped (~ → %7E), so it can never collide with the sentinel.
		expect(rangePartitionDoName("mydb", "k", "~min", null)).toBe("mydb.r.k.%7Emin.~max");
	});

	it("percent-encodes dots in hashKey and boundaries", () => {
		expect(rangePartitionDoName("mydb", "a.b", "c.d", "e.f")).toBe("mydb.r.a%2Eb.c%2Ed.e%2Ef");
	});

	it("percent-encodes slashes", () => {
		expect(rangePartitionDoName("mydb", "a/b", "c/d", "e/f")).toBe("mydb.r.a%2Fb.c%2Fd.e%2Ff");
	});

	it("leaves [A-Za-z0-9_-] unchanged", () => {
		expect(rangePartitionDoName("db", "Hello_World-123", "sk_value-99", "sk_value-zz")).toBe("db.r.Hello_World-123.sk_value-99.sk_value-zz");
	});

	it("keeps range names disjoint from hash names (.r. vs .h.)", () => {
		const rangeName = rangePartitionDoName("db", "0", null, null);
		expect(rangeName).toBe("db.r.0.~min.~max");
		// Hash root 0 is "db.h.0" — no collision.
		expect(rangeName).not.toBe("db.h.0");
	});
});

describe("PartitionIdHelper — range schema (SCHEMA_RANGE_V1)", () => {
	it("fromRangePartition root: encode then decode round-trips (both boundaries null)", () => {
		const base = makeBase();
		const helper = PartitionIdHelper.fromRangePartition(base, "alice", null, null);
		const { bytes, opaque, doName } = helper.encode(true);

		expect(bytes[0]).toBe(PartitionIdHelper.SCHEMA_RANGE_V1);
		expect(doName).toBe("testdb.r.alice.~min.~max");

		const decoded = PartitionIdHelper.decode(bytes);
		expect(decoded.schema).toBe(1);
		if (decoded.schema === 1) {
			expect(decoded.hashKey).toBe("alice");
			expect(decoded.startBoundary).toBeNull();
			expect(decoded.endBoundary).toBeNull();
		}

		// Opaque round-trip.
		const decoded2 = PartitionIdHelper.decode(Uint8Array.fromHex(opaque));
		expect(decoded2).toEqual(decoded);
	});

	it("fromRangePartition child: encode then decode round-trips with both boundaries", () => {
		const base = makeBase();
		const helper = PartitionIdHelper.fromRangePartition(base, "alice", "b1", "b2");
		const { bytes, doName } = helper.encode(true);

		expect(bytes[0]).toBe(PartitionIdHelper.SCHEMA_RANGE_V1);
		expect(doName).toBe("testdb.r.alice.b1.b2");

		const decoded = PartitionIdHelper.decode(bytes);
		expect(decoded.schema).toBe(1);
		if (decoded.schema === 1) {
			expect(decoded.hashKey).toBe("alice");
			expect(decoded.startBoundary).toBe("b1");
			expect(decoded.endBoundary).toBe("b2");
		}
	});

	it("round-trips half-bounded edges (leftmost: null start; rightmost: null end)", () => {
		const base = makeBase();
		for (const [start, end, name] of [
			[null, "m", "testdb.r.x.~min.m"],
			["m", null, "testdb.r.x.m.~max"],
		] as const) {
			const { bytes, doName } = PartitionIdHelper.fromRangePartition(base, "x", start, end).encode(true);
			expect(doName).toBe(name);
			const decoded = PartitionIdHelper.decode(bytes);
			expect(decoded.schema).toBe(1);
			if (decoded.schema === 1) {
				expect(decoded.startBoundary).toBe(start);
				expect(decoded.endBoundary).toBe(end);
			}
		}
	});

	it("handles unicode in hashKey and boundaries", () => {
		const base = makeBase();
		const { bytes } = PartitionIdHelper.fromRangePartition(base, "café☕", "töst", "zünd").encode(false);
		const decoded = PartitionIdHelper.decode(bytes);
		expect(decoded.schema).toBe(1);
		if (decoded.schema === 1) {
			expect(decoded.hashKey).toBe("café☕");
			expect(decoded.startBoundary).toBe("töst");
			expect(decoded.endBoundary).toBe("zünd");
		}
	});

	it("doName dispatches correctly for range ID loaded from opaque hex", () => {
		const base = makeBase();
		const { opaque } = PartitionIdHelper.fromRangePartition(base, "mykey", "start1", "end1").encode(false);
		const bytes = Uint8Array.fromHex(opaque);
		expect(PartitionIdHelper.doName(base, bytes)).toBe("testdb.r.mykey.start1.end1");
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
		const { bytes } = PartitionIdHelper.fromRangePartition(base, "k", null, null).encode(false);
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

function makeRangeCtx(
	base: PartitionContext,
	hashKey: string,
	startBoundary: string | null,
	endBoundary: string | null,
): PartitionContextResolved {
	const { opaque, doName } = PartitionIdHelper.fromRangePartition(base, hashKey, startBoundary, endBoundary).encode(true);
	const doId = env.PARTITION_DO.idFromName(doName!);
	return {
		...base,
		doName: doName!,
		primaryDoIdStr: doId.toString(),
		partitionId: opaque,
		rangePartition: { hashKey, startBoundary, endBoundary },
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
	splitStatus?: SplitStatusKVItem,
): Promise<void> {
	// The DO's owned range comes from pCtx.rangePartition (immutable identity); there is no separate end boundary.
	await stub.initFromSplit(
		{ parentPartitionContext: parentCtx, newPartitionContext: pCtx, splitType: "range" },
		true, // __testing__completeMigration
		splitStatus, // __testing__splitStatus
	);
}

// ─── Phase 2: RangePartitionTopologyImpl ──────────────────────────────────────

describe("RangePartitionTopologyImpl — serves/rejects/forwards by sort-key range", () => {
	it("serves sort keys within [start, end) — putItem succeeds locally (forwardCount=0)", async () => {
		const base = makeUniqueBase();
		const rootCtx = makeRangeCtx(base, "alice", null, "m"); // owns [∅, "m")
		const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		await setupRangeDO(stub, rootCtx, makeHashCtx(base));

		const r = await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "a", data: "v1" });
		expect(r.meta.forwardCount).toBe(0);

		const g = await stub.getItem(rootCtx, { hashKey: "alice", sortKey: "a" });
		expect(g).toMatchObject({ found: true, item: { data: "v1" }, meta: { forwardCount: 0 } });
	});

	it("rejects sort keys outside [start, end) when not split", async () => {
		const base = makeUniqueBase();
		const rootCtx = makeRangeCtx(base, "alice", null, "m"); // owns [∅, "m")
		const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		await setupRangeDO(stub, rootCtx, makeHashCtx(base));

		await runInDurableObject(stub, async (doInstance: PartitionDO) => {
			await expect(doInstance.putItem(rootCtx, { hashKey: "alice", sortKey: "m", data: "x" })).rejects.toThrow(/exceeded its limits/);
			await expect(doInstance.putItem(rootCtx, { hashKey: "alice", sortKey: "z", data: "x" })).rejects.toThrow(/exceeded its limits/);
		});
	});

	it("unbounded range (endBoundary=null) accepts any sort key", async () => {
		const base = makeUniqueBase();
		const rootCtx = makeRangeCtx(base, "alice", null, null); // owns [∅, +∞)
		const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		await setupRangeDO(stub, rootCtx, makeHashCtx(base));

		await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "", data: "empty-sk" });
		await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "zzzzz", data: "last" });

		const g = await stub.getItem(rootCtx, { hashKey: "alice", sortKey: "zzzzz" });
		expect(g).toMatchObject({ found: true });
	});

	it("once split, the node is a pure router — forwards EVERY sort key to the owning child", async () => {
		const base = makeUniqueBase();
		const rootCtx = makeRangeCtx(base, "alice", null, null); // pre-split: owned [∅, +∞)
		// Split at "m" into a NEW leftmost child [∅,"m") and a right child ["m",+∞). The root keeps no slice.
		const leftCtx = makeRangeCtx(base, "alice", null, "m");
		const rightCtx = makeRangeCtx(base, "alice", "m", null);
		const rootStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		const leftStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(leftCtx.doName));
		const rightStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rightCtx.doName));

		await setupRangeDO(leftStub, leftCtx, rootCtx);
		await setupRangeDO(rightStub, rightCtx, rootCtx);

		// FIXME Remove the internal injection with a proper test!
		// Root becomes a pure router over both children.
		await setupRangeDO(rootStub, rootCtx, makeHashCtx(base), {
			status: "split_started",
			splitType: "range",
			createdAt: Date.now(),
			partitionContext: rootCtx,
			childPartitionContexts: [leftCtx, rightCtx],
			migratedChildDoNames: [],
			history: [],
		} satisfies SplitStatusKVItem);

		// sk in the leftmost slice [∅,"m") — forwarded to the new left child (NOT served locally).
		const left = await rootStub.putItem(rootCtx, { hashKey: "alice", sortKey: "a", data: "left" });
		expect(left.meta.forwardCount).toBe(1);

		// sk in the right slice ["m",∅) — forwarded to the right child.
		const right = await rootStub.putItem(rootCtx, { hashKey: "alice", sortKey: "z", data: "right" });
		expect(right.meta.forwardCount).toBe(1);

		// Each item landed in its owning child, none on the router.
		expect(await leftStub.getItem(leftCtx, { hashKey: "alice", sortKey: "a" })).toMatchObject({ found: true, item: { data: "left" } });
		expect(await rightStub.getItem(rightCtx, { hashKey: "alice", sortKey: "z" })).toMatchObject({ found: true, item: { data: "right" } });
	});
});

describe("RangePartitionTopologyImpl — maybeQueueSplit", () => {
	it("queues a range split when db exceeds rangeSplitConditions.maxSizeMb", async () => {
		const base = makeUniqueBase({ rangeSplitN: 2, rangeSplitConditions: { maxSizeMb: 0.1 } });
		const rootCtx = makeRangeCtx(base, "alice", null, null);
		const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		await setupRangeDO(stub, rootCtx, makeHashCtx(base));

		const bigData = "x".repeat(50 * 1024);
		await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "sk1", data: bigData });
		await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "sk2", data: bigData });

		const s = await stub.status(rootCtx);
		expect(s.splitStatus?.status).toBe("split_queued");
		expect(s.splitStatus?.splitType).toBe("range");

		await vi.waitFor(async () => {
			const s = await stub.status(rootCtx);
			return s.splitStatus?.status === "split_completed" ? Promise.resolve() : Promise.reject(new Error("Split not completed yet"));
		}, { timeout: 5000, interval: 100 });
	});

	it("does not queue a split when db is within limits", async () => {
		const base = makeUniqueBase(); // rangeSplitConditions.maxSizeMb=500 by default
		const rootCtx = makeRangeCtx(base, "alice", null, null);
		const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		await setupRangeDO(stub, rootCtx, makeHashCtx(base));

		await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "sk", data: "x" });

		const s = await stub.status(rootCtx);
		expect(s.splitStatus).toBeUndefined();
	});
});
