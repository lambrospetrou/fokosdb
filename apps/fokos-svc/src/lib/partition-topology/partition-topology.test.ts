import { env } from "cloudflare:workers";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import { PartitionContextCreator } from "./partition-context.js";
import type { PartitionContext, PartitionContextLivePartition, PartitionContextResolved } from "./partition-context.js";
import { PartitionIdHelper } from "./partition-id.js";
import { PartitionTopologyRouterImpl } from "./router.js";
import type { SplitStatusKVItem } from "./split-state.js";
import type { PartitionDO } from "../do-partition.js";
import { KeyCodec } from "./key-codec.js";

const kb = (s: string) => KeyCodec.encode(s);

type SplitStartedOrCompleted = Extract<SplitStatusKVItem, { status: "split_started" | "split_completed" }>;

// ─── RangePartitionTopologyImpl helpers ───────────────────────────────────────

// Each RangePartitionTopologyImpl test must use a unique base to avoid DO name collisions between tests.
function makeUniqueBase(overrides?: Partial<PartitionContext>): PartitionContext {
	return PartitionContextCreator.create({
		ns: "PARTITION_DO",
		tableName: `testdb-${crypto.randomUUID()}`,
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
	const hashKeyBytes = kb(hashKey);
	const startBytes = startBoundary === null ? null : kb(startBoundary);
	const endBytes = endBoundary === null ? null : kb(endBoundary);
	const { opaque, doName } = PartitionIdHelper.fromRangePartition(base, hashKeyBytes, startBytes, endBytes).encode(true);
	const doId = env.PARTITION_DO.idFromName(doName!);
	return {
		...base,
		doName: doName!,
		primaryDoIdStr: doId.toString(),
		partitionId: opaque,
		rangePartition: { hashKey: hashKeyBytes, startBoundary: startBytes, endBoundary: endBytes },
	};
}

function makeHashCtx(base: PartitionContext): PartitionContextResolved {
	return new PartitionTopologyRouterImpl(base).pickPartition(kb("dummyKey")).partitionContext;
}

// Sets up a range DO via initFromSplit and immediately marks migration complete,
// so the DO serves requests locally without needing a real parent to pull data from.
async function setupRootRangeDO(
	stub: DurableObjectStub<PartitionDO>,
	pCtx: PartitionContextResolved,
	parentCtx: PartitionContextResolved,
): Promise<void> {
	await stub.initFromSplit(
		{ parentPartitionContext: parentCtx, newPartitionContext: pCtx, newPartitionRangeDepth: 0, splitType: "range", rangeAncestors: [] },
		true, // __testing__completeMigration
	);
}

// ─── RangePartitionTopologyImpl — routing and split behavior ──────────────────

describe("RangePartitionTopologyImpl — serves/rejects/forwards by sort-key range", () => {
	it("serves sort keys within [start, end) — putItem succeeds locally (forwardCount=0)", async () => {
		const base = makeUniqueBase();
		const rootCtx = makeRangeCtx(base, "alice", null, "m"); // owns [∅, "m")
		const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		await setupRootRangeDO(stub, rootCtx, makeHashCtx(base));

		const r = await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "a", data: "v1", kind: "text" });
		expect(r.meta.forwardCount).toBe(0);

		const g = await stub.getItem(rootCtx, { hashKey: "alice", sortKey: "a" });
		expect(g).toMatchObject({ found: true, item: { data: "v1" }, meta: { forwardCount: 0 } });
	});

	it("rejects sort keys outside [start, end) when not split", async () => {
		const base = makeUniqueBase();
		const rootCtx = makeRangeCtx(base, "alice", null, "m"); // owns [∅, "m")
		const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		await setupRootRangeDO(stub, rootCtx, makeHashCtx(base));

		await runInDurableObject(stub, async (doInstance: PartitionDO) => {
			await expect(doInstance.putItem(rootCtx, { hashKey: "alice", sortKey: "m", data: "x", kind: "text" })).rejects.toThrow(
				/exceeded its limits/,
			);
			await expect(doInstance.putItem(rootCtx, { hashKey: "alice", sortKey: "z", data: "x", kind: "text" })).rejects.toThrow(
				/exceeded its limits/,
			);
		});
	});

	it("unbounded range (endBoundary=null) accepts any sort key", async () => {
		const base = makeUniqueBase();
		const rootCtx = makeRangeCtx(base, "alice", null, null); // owns [∅, +∞)
		const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		await setupRootRangeDO(stub, rootCtx, makeHashCtx(base));

		await stub.putItem(rootCtx, { hashKey: "alice", sortKey: undefined, data: "empty-sk", kind: "text" });
		await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "zzzzz", data: "last", kind: "text" });

		const g = await stub.getItem(rootCtx, { hashKey: "alice", sortKey: "zzzzz" });
		expect(g).toMatchObject({ found: true });
	});

	it("once split, the node is a pure router — forwards EVERY sort key to the owning child", async () => {
		const base = makeUniqueBase({ rangeSplitN: 2, rangeSplitConditions: { maxSizeMb: 0.1 } });
		const rootCtx = makeRangeCtx(base, "alice", null, null);
		const rootStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		await setupRootRangeDO(rootStub, rootCtx, makeHashCtx(base));

		// Write ~50 KB items until a range split is queued (0.1 MB threshold → ~3 items).
		const bigData = "x".repeat(50 * 1024);
		for (let i = 0; i < 10; i++) {
			await rootStub.putItem(rootCtx, {
				hashKey: "alice",
				sortKey: `sk${String(i).padStart(3, "0")}`,
				data: bigData,
				kind: "text",
			});
			if ((await rootStub.status(rootCtx)).splitStatus) break;
		}

		// Run alarms until split_completed: root splits, children migrate.
		await vi.waitFor(
			async () => {
				await runDurableObjectAlarm(rootStub);
				const s = await rootStub.status(rootCtx);
				if (s.splitStatus?.status === "split_started") {
					for (const childCtx of (s.splitStatus as SplitStartedOrCompleted).childPartitionContexts) {
						const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));
						await childStub.triggerMigration();
						await runDurableObjectAlarm(childStub);
					}
				}
				if ((await rootStub.status(rootCtx)).splitStatus?.status !== "split_completed") throw new Error("split not completed yet");
			},
			{ timeout: 8000, interval: 100 },
		);

		const split = (await rootStub.status(rootCtx)).splitStatus as SplitStartedOrCompleted;
		const children = [...split.childPartitionContexts].sort((a, b) =>
			(a.rangePartition!.startBoundary ?? "") < (b.rangePartition!.startBoundary ?? "") ? -1 : 1,
		);
		expect(children).toHaveLength(2);
		const boundary = children[0].rangePartition!.endBoundary!;

		// sk below boundary — forwarded to left child (forwardCount=1, not served locally by the router).
		const left = await rootStub.putItem(rootCtx, {
			hashKey: "alice",
			sortKey: "sk000",
			data: "left",
			kind: "text",
		});
		expect(left.meta.forwardCount).toBe(1);

		// sk at boundary — forwarded to right child.
		const right = await rootStub.putItem(rootCtx, {
			hashKey: "alice",
			sortKey: boundary,
			data: "right",
			kind: "text",
		});
		expect(right.meta.forwardCount).toBe(1);

		// Items landed in their owning children, not on the router.
		const leftStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(children[0].doName));
		const rightStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(children[1].doName));
		expect(await leftStub.getItem(children[0], { hashKey: "alice", sortKey: "sk000" })).toMatchObject({
			found: true,
			item: { data: "left" },
		});
		expect(await rightStub.getItem(children[1], { hashKey: "alice", sortKey: boundary })).toMatchObject({
			found: true,
			item: { data: "right" },
		});
	});
});

describe("RangePartitionTopologyImpl — maybeQueueSplit", () => {
	it("queues a range split when db exceeds rangeSplitConditions.maxSizeMb", async () => {
		const base = makeUniqueBase({ rangeSplitN: 2, rangeSplitConditions: { maxSizeMb: 0.1 } });
		const rootCtx = makeRangeCtx(base, "alice", null, null);
		const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		await setupRootRangeDO(stub, rootCtx, makeHashCtx(base));

		const bigData = "x".repeat(50 * 1024);
		await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "sk1", data: bigData, kind: "text" });
		await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "sk2", data: bigData, kind: "text" });

		const s = await stub.status(rootCtx);
		expect(s.splitStatus?.status).toBe("split_queued");
		expect(s.splitStatus?.splitType).toBe("range");

		await vi.waitFor(
			async () => {
				const s = await stub.status(rootCtx);
				return s.splitStatus?.status === "split_completed" ? Promise.resolve() : Promise.reject(new Error("Split not completed yet"));
			},
			{ timeout: 5000, interval: 100 },
		);
	});

	it("does not queue a split when db is within limits", async () => {
		const base = makeUniqueBase(); // rangeSplitConditions.maxSizeMb=500 by default
		const rootCtx = makeRangeCtx(base, "alice", null, null);
		const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));
		await setupRootRangeDO(stub, rootCtx, makeHashCtx(base));

		await stub.putItem(rootCtx, { hashKey: "alice", sortKey: "sk", data: "x", kind: "text" });

		const s = await stub.status(rootCtx);
		expect(s.splitStatus).toBeUndefined();
	});
});
