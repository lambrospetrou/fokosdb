import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionContextCreator } from "./partition-context.js";
import type { PartitionContext, PartitionContextResolved } from "./partition-context.js";
import { PartitionIdHelper } from "./partition-id.js";
import { RangePartitionTopologyImpl } from "./split-policy.js";
import { PartitionDO } from "../../server/do-partition.js";
import { PartitionStore } from "../partition/partition-store.js";
import { KeyCodec } from "./key-codec.js";

const kb = (s?: string) => KeyCodec.encodeOptional(s);

// Each test uses a unique base so its Durable Object names never collide with another test's.
function makeUniqueBase(overrides?: Partial<PartitionContext>): PartitionContext {
	return PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
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

/**
 * Runs `body` against a live `RangePartitionTopologyImpl` for `rangeCtx`.
 *
 * The topology reads its split status from KV and its size from SQLite, so it needs real storage —
 * but nothing else. The Durable Object here is only the storage container: no split, no migration
 * and no partition context of its own, which is why these stay unit tests of the routing decision
 * rather than tests of a partition that had to be brought to a serving state first.
 */
async function withRangeTopology(
	rangeCtx: PartitionContextResolved,
	body: (topology: RangePartitionTopologyImpl, store: PartitionStore) => void | Promise<void>,
): Promise<void> {
	const stub = PartitionDO.getByName(env.PARTITION_DO, rangeCtx.doName);
	await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
		const store = new PartitionStore(state.storage);
		await body(new RangePartitionTopologyImpl(rangeCtx, state, store), store);
	});
}

/** Writes `count` rows of ~24 KB under `alice`, enough to move `storage.sql.databaseSize`. */
function fillRows(store: PartitionStore, count: number): void {
	const data = "x".repeat(24 * 1024);
	for (let i = 0; i < count; i++) {
		store.upsertItem({ hk: kb("alice"), sk: kb(`sk${String(i).padStart(3, "0")}`), data, kind: "text", ttlAt: null, lastTransactionTs: 0 });
	}
}

describe("RangePartitionTopologyImpl — shouldAllow by sort-key range", () => {
	it("serves sort keys inside [start, end)", async () => {
		const rangeCtx = makeRangeCtx(makeUniqueBase(), "alice", null, "m"); // owns [∅, "m")
		await withRangeTopology(rangeCtx, (topology) => {
			expect(topology.shouldAllow(kb("alice"), kb(), "write")).toBe("ok");
			expect(topology.shouldAllow(kb("alice"), kb("a"), "write")).toBe("ok");
			expect(topology.shouldAllow(kb("alice"), kb("lzzzz"), "read")).toBe("ok");
		});
	});

	it("reports a sort key outside [start, end) as mis-routed, not as backpressure", async () => {
		// The two rejections are not interchangeable: backpressure is retryable, mis-routing can never
		// succeed on this node. "m" is the exclusive upper bound, so it is already outside.
		const rangeCtx = makeRangeCtx(makeUniqueBase(), "alice", null, "m");
		await withRangeTopology(rangeCtx, (topology) => {
			expect(topology.shouldAllow(kb("alice"), kb("m"), "write")).toBe("reject_out_of_range");
			expect(topology.shouldAllow(kb("alice"), kb("z"), "write")).toBe("reject_out_of_range");
		});
	});

	it("accepts any sort key when the range is unbounded", async () => {
		const rangeCtx = makeRangeCtx(makeUniqueBase(), "alice", null, null); // owns [∅, +∞)
		await withRangeTopology(rangeCtx, (topology) => {
			expect(topology.shouldAllow(kb("alice"), kb(), "write")).toBe("ok");
			expect(topology.shouldAllow(kb("alice"), kb("zzzzz"), "write")).toBe("ok");
		});
	});

	it("forwards every sort key once the node has split, owning nothing itself", async () => {
		const base = makeUniqueBase({ rangeSplitN: 2, rangeSplitConditions: { maxSizeMb: 0.1 } });
		const rangeCtx = makeRangeCtx(base, "alice", null, null);
		await withRangeTopology(rangeCtx, async (topology, store) => {
			fillRows(store, 10);
			expect(await topology.maybeQueueSplitNoKey({ hasInFlightPromotions: false })).toMatchObject({ status: "split_queued" });

			const children = topology.prepareSplit({ parentDepth: 0, boundaries: [kb("m")] });
			expect(children).toHaveLength(2);
			topology.commitSplitStarted(children!.map((child) => child.newPartitionContext));

			// A router owns no key range of its own, so even a sort key it used to serve is forwarded.
			expect(topology.shouldAllow(kb("alice"), kb("a"), "write")).toBe("forward");
			expect(topology.shouldAllow(kb("alice"), kb("zzzzz"), "write")).toBe("forward");
		});
	});
});

describe("RangePartitionTopologyImpl — maybeQueueSplit", () => {
	it("queues a range split when the database exceeds rangeSplitConditions.maxSizeMb", async () => {
		const base = makeUniqueBase({ rangeSplitN: 2, rangeSplitConditions: { maxSizeMb: 0.1 } });
		const rangeCtx = makeRangeCtx(base, "alice", null, null);
		await withRangeTopology(rangeCtx, async (topology, store) => {
			fillRows(store, 10);
			expect(await topology.maybeQueueSplitNoKey({ hasInFlightPromotions: false })).toMatchObject({
				status: "split_queued",
				splitType: "range",
			});
			expect(topology.splitStatus()).toMatchObject({ status: "split_queued", splitType: "range" });
		});
	});

	it("does not queue a split when the database is within limits", async () => {
		// The default rangeSplitConditions.maxSizeMb is 500, which these rows come nowhere near.
		const rangeCtx = makeRangeCtx(makeUniqueBase(), "alice", null, null);
		await withRangeTopology(rangeCtx, async (topology, store) => {
			fillRows(store, 1);
			expect(await topology.maybeQueueSplitNoKey({ hasInFlightPromotions: false })).toBeUndefined();
			expect(topology.splitStatus()).toBeUndefined();
		});
	});
});
