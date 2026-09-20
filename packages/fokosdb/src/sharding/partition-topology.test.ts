import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionContextCreator, type FokosDbRouteContext } from "../shared/partition-context.js";
import { partitionIdentityFrom, resolveRangePartitionContext } from "./partition-id.js";
import { FokosRouter } from "./router.js";
import { RangePartitionTopologyImpl } from "./split-policy.js";
import type { PartitionDO } from "../server/do-partition.js";
import { testPartitionStub } from "../../test/stub-helpers.js";
import { PartitionStore } from "../shared/partition/partition-store.js";
import { FokosShardingStore } from "./sharding-store.js";
import { KeyCodec } from "./key-codec.js";
import type { RepartitionRouting } from "./repartition-types.js";

const kb = (s?: string) => KeyCodec.encodeOptional(s);

type CreateOptions = Parameters<typeof PartitionContextCreator.create>[0];

// Each test uses a unique table so its Durable Object names never collide with another test's.
function makeUniqueBase(overrides?: Partial<CreateOptions>): FokosDbRouteContext {
	const cfg = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName: `testdb-${crypto.randomUUID()}`,
		rootTreesN: 1,
		hashSplitN: 4,
		hashSplitConditions: { maxSizeMb: 100 },
		...overrides,
	});
	return new FokosRouter(cfg.topology, cfg.rangeConfig, cfg.policy).allRoots()[0];
}

function makeRangeCtx(
	base: FokosDbRouteContext,
	hashKey: string,
	startBoundary: string | null,
	endBoundary: string | null,
): FokosDbRouteContext {
	return resolveRangePartitionContext(
		base,
		kb(hashKey),
		startBoundary === null ? null : kb(startBoundary),
		endBoundary === null ? null : kb(endBoundary),
	);
}

/**
 * Runs `body` against a live `RangePartitionTopologyImpl` for `rangeCtx`.
 *
 * The topology reads its size from SQLite, so it needs real storage and nothing else. The Durable
 * Object here is the storage container only: it has no split, no migration and no partition context
 * of its own. These therefore stay unit tests of the routing decision, and not tests of a partition
 * that a case must first bring to a serving state.
 *
 * These tests stub the repartition rows the topology reads. Whether this node is a router, and which
 * targets it holds, is state of the flow, and `repartition-flow.test.ts` drives the real rows.
 */
async function withRangeTopology(
	rangeCtx: FokosDbRouteContext,
	body: (topology: RangePartitionTopologyImpl, store: PartitionStore) => void | Promise<void>,
	routing: Partial<RepartitionRouting> = {},
): Promise<void> {
	const stub = testPartitionStub(rangeCtx.doName);
	await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
		const stubRouting: RepartitionRouting = {
			routerRole: () => false,
			splitTargets: () => [],
			overrideFor: () => undefined,
			ownedByRangeTree: () => false,
			...routing,
		};
		const identity = partitionIdentityFrom(rangeCtx, { depth: 0, ancestors: [] });
		const topology = new RangePartitionTopologyImpl(rangeCtx, identity, state, new FokosShardingStore(state.storage), stubRouting);
		await body(topology, new PartitionStore(state.storage));
	});
}

/** Writes `count` rows of ~24 KB under `alice`, enough to move `storage.sql.databaseSize`. */
function fillRows(store: PartitionStore, count: number): void {
	const data = "x".repeat(24 * 1024);
	for (let i = 0; i < count; i++) {
		store.upsertItem({ hk: kb("alice"), sk: kb(`sk${String(i).padStart(3, "0")}`), data, kind: "text", ttlAt: null, txOrderTs: 0 });
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

	it("forwards every sort key once the node has become a router, owning nothing itself", async () => {
		const rangeCtx = makeRangeCtx(makeUniqueBase({ rangeSplitN: 2 }), "alice", null, null);
		await withRangeTopology(
			rangeCtx,
			async (topology) => {
				// A router owns no key range of its own, so it forwards even a sort key it used to serve.
				expect(topology.shouldAllow(kb("alice"), kb("a"), "write")).toBe("forward");
				expect(topology.shouldAllow(kb("alice"), kb("zzzzz"), "write")).toBe("forward");
			},
			{ routerRole: () => true },
		);
	});
});

describe("RangePartitionTopologyImpl — shouldSplit", () => {
	it("calls for a range split when the database exceeds rangeSplitConditions.maxSizeMb", async () => {
		const base = makeUniqueBase({ rangeSplitN: 2, rangeSplitConditions: { maxSizeMb: 0.1 } });
		const rangeCtx = makeRangeCtx(base, "alice", null, null);
		await withRangeTopology(rangeCtx, async (topology, store) => {
			fillRows(store, 10);
			// A size answer only. Arbitration in the repartition flow decides whether the split starts.
			expect(topology.shouldSplit()).toBe("range");
		});
	});

	it("does not call for a split when the database is within limits", async () => {
		// The default rangeSplitConditions.maxSizeMb is 500, which these rows come nowhere near.
		const rangeCtx = makeRangeCtx(makeUniqueBase(), "alice", null, null);
		await withRangeTopology(rangeCtx, async (topology, store) => {
			fillRows(store, 1);
			expect(topology.shouldSplit()).toBeNull();
		});
	});
});
