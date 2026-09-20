import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { PartitionDO } from "../server/do-partition.js";
import { testPartitionStub } from "../../test/stub-helpers.js";
import { FokosError, INTERNAL_CODES, UNAVAILABLE_CODES } from "../shared/errors.js";
import { invariantFailure } from "../../test/errors-matchers.js";
import { PartitionStore } from "../shared/partition/partition-store.js";
import { KeyCodec, type KeyBytes } from "./key-codec.js";
import { PartitionContextCreator, type FokosDbRouteContext } from "../shared/partition-context.js";
import { partitionIdentityFrom, PartitionIdHelper, resolveRangePartitionContext } from "./partition-id.js";
import { FokosRouter } from "./router.js";
import { isRangePartition } from "./route-context.js";
import { HashPartitionTopologyImpl, RangePartitionTopologyImpl, type OperationIntent } from "./split-policy.js";
import type { RepartitionRouting } from "./repartition-types.js";

// An empty SQLite database already occupies several KB, so any partition built with this cap is
// over its 10% backpressure threshold from the first request — no data has to be written.
const OVER_SIZE_MB = 0.000_001;
const HK = KeyCodec.encode("hk");
const SK = KeyCodec.encode("sk");

// Listed explicitly rather than derived, so adding a value to OperationIntent forces a decision here
// about which side of the backpressure gate it belongs on.
const NON_GROWING = ["read", "delete", "ignore_size_reject"] as const satisfies readonly OperationIntent[];
const ALL_INTENTS = ["write", ...NON_GROWING] as const satisfies readonly OperationIntent[];

describe("shouldAllow size backpressure applies to growing writes only", () => {
	// Only "write" can grow a partition. Refusing a read removes availability with no benefit;
	// refusing a delete is worse than useless, because a delete is how a client brings an over-size
	// partition back under its cap; and refusing a commit wedges a transaction the coordinator has
	// already decided, for bytes that prepare has already written.
	it("hash partition over its size cap rejects a write but serves every non-growing intent", async () => {
		await withHashTopology(hashContext(OVER_SIZE_MB), (topology) => {
			expect(topology.shouldAllow(HK, SK, "write")).toBe("reject_over_size");
			for (const intent of NON_GROWING) {
				expect(topology.shouldAllow(HK, SK, intent), intent).toBe("ok");
			}
		});
	});

	it("range partition over its size cap rejects a write but serves every non-growing intent", async () => {
		await withRangeTopology(rangeContext(OVER_SIZE_MB), (topology) => {
			expect(topology.shouldAllow(HK, SK, "write")).toBe("reject_over_size");
			for (const intent of NON_GROWING) {
				expect(topology.shouldAllow(HK, SK, intent), intent).toBe("ok");
			}
		});
	});

	// The range partition rejects for two unrelated reasons. Only the size one is backpressure; an
	// out-of-range sort key is a routing bug, and serving it would touch data this DO does not own.
	// "ignore_size_reject" names the SIZE reject only — it must not buy its way past this one.
	it("range partition rejects an out-of-range sort key whatever the intent", async () => {
		await withRangeTopology(rangeContext(100, KeyCodec.encode("m"), null), (topology) => {
			for (const intent of ALL_INTENTS) {
				expect(topology.shouldAllow(HK, KeyCodec.encode("a"), intent), intent).toBe("reject_out_of_range");
				expect(topology.shouldAllow(HK, KeyCodec.encode("z"), intent), intent).toBe("ok");
			}
		});
	});

	it("a partition under its size cap allows every intent", async () => {
		await withHashTopology(hashContext(100), (topology) => {
			for (const intent of ALL_INTENTS) {
				expect(topology.shouldAllow(HK, SK, intent), intent).toBe("ok");
			}
		});
	});
});

/** The one root of a fresh table whose two split caps are `maxSizeMb`. */
function hashContext(maxSizeMb: number): FokosDbRouteContext {
	const cfg = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName: `splitpolicy-${crypto.randomUUID()}`,
		rootTreesN: 1,
		hashSplitN: 2,
		hashSplitConditions: { maxSizeMb },
		rangeSplitN: 2,
		rangeSplitConditions: { maxSizeMb },
	});
	return new FokosRouter(cfg.topology, cfg.rangeConfig, cfg.policy).allRoots()[0];
}

function rangeContext(maxSizeMb: number, startBoundary: KeyBytes | null = null, endBoundary: KeyBytes | null = null): FokosDbRouteContext {
	return resolveRangePartitionContext(hashContext(maxSizeMb), HK, startBoundary, endBoundary);
}

/** A range partition here is always a root: the tests never split one. */
const identityOf = (pCtx: FokosDbRouteContext) =>
	partitionIdentityFrom(pCtx, isRangePartition(pCtx) ? { depth: 0, ancestors: [] } : undefined);

// Runs `fn` against a topology backed by REAL Durable Object storage, so `sql.databaseSize` is real.
async function withTopology<T>(
	make: (pCtx: FokosDbRouteContext, state: DurableObjectState, store: PartitionStore) => T,
	pCtx: FokosDbRouteContext,
	fn: (topology: T) => void,
): Promise<void> {
	const stub = testPartitionStub(`splitpolicy-${crypto.randomUUID()}`);
	await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
		fn(make(pCtx, state, new PartitionStore(state.storage)));
	});
}

// These tests cover backpressure and range membership only, so the partition is never a router and
// owns no override. `repartition-flow.test.ts` drives the real repartition rows.
const NOT_REPARTITIONING: RepartitionRouting = {
	routerRole: () => false,
	splitTargets: () => [],
	overrideFor: () => undefined,
	ownedByRangeTree: () => false,
};

function withHashTopology(pCtx: FokosDbRouteContext, fn: (t: HashPartitionTopologyImpl) => void): Promise<void> {
	return withTopology((c, state, store) => new HashPartitionTopologyImpl(c, identityOf(c), state, store, NOT_REPARTITIONING), pCtx, fn);
}

function withRangeTopology(pCtx: FokosDbRouteContext, fn: (t: RangePartitionTopologyImpl) => void): Promise<void> {
	return withTopology((c, state, store) => new RangePartitionTopologyImpl(c, identityOf(c), state, store, NOT_REPARTITIONING), pCtx, fn);
}

describe("updatePartitionContext replaces the mutable options of the context a topology holds", () => {
	it("a hash topology answers shouldAllow from the latest hashSplitConditions", async () => {
		const pCtx = hashContext(100);
		await withHashTopology(pCtx, (topology) => {
			expect(topology.shouldAllow(HK, SK, "write")).toBe("ok");
			topology.updatePartitionContext({ ...pCtx, policy: { ...pCtx.policy, hashSplitConditions: { maxSizeMb: OVER_SIZE_MB } } });
			expect(topology.shouldAllow(HK, SK, "write")).toBe("reject_over_size");
		});
	});

	it("a range topology answers shouldAllow from the latest rangeSplitConditions", async () => {
		const pCtx = rangeContext(100);
		await withRangeTopology(pCtx, (topology) => {
			expect(topology.shouldAllow(HK, SK, "write")).toBe("ok");
			topology.updatePartitionContext({ ...pCtx, policy: { ...pCtx.policy, rangeSplitConditions: { maxSizeMb: OVER_SIZE_MB } } });
			expect(topology.shouldAllow(HK, SK, "write")).toBe("reject_over_size");
		});
	});

	it("a hash topology rejects a changed partitionId", async () => {
		const pCtx = hashContext(100);
		const otherId = PartitionIdHelper.fromHashIdxs(pCtx.topology.shardGroup, [1]).encode(true).opaque;
		await withHashTopology(pCtx, (topology) => {
			expect(() => topology.updatePartitionContext({ ...pCtx, partitionId: otherId })).toThrow(
				invariantFailure("HashPartitionTopologyImpl partition identity changed"),
			);
		});
	});

	it("a range topology rejects a changed doName", async () => {
		const pCtx = rangeContext(100);
		await withRangeTopology(pCtx, (topology) => {
			expect(() => topology.updatePartitionContext({ ...pCtx, doName: `other-${crypto.randomUUID()}` })).toThrow(
				invariantFailure("RangePartitionTopologyImpl partition identity changed"),
			);
		});
	});

	it("a partition applies the latest context to the topology it already built", async () => {
		const pCtx = hashContext(100);
		const stub = testPartitionStub(pCtx.doName);

		await stub.apiPutItem(pCtx, { hashKey: HK, sortKey: SK, data: "v", kind: "text" });

		let failure: unknown;
		try {
			await stub.apiPutItem(
				{ ...pCtx, policy: { ...pCtx.policy, hashSplitConditions: { maxSizeMb: OVER_SIZE_MB } } },
				{ hashKey: HK, sortKey: SK, data: "v", kind: "text" },
			);
		} catch (error) {
			failure = error;
		}
		expect(FokosError.isCode(failure, UNAVAILABLE_CODES.partition_over_size)).toBe(true);
	});

	it("a partition rejects a request whose topology differs from the stored one", async () => {
		const pCtx = hashContext(100);
		const stub = testPartitionStub(pCtx.doName);

		await stub.apiPutItem(pCtx, { hashKey: HK, sortKey: SK, data: "v", kind: "text" });

		let failure: unknown;
		try {
			await stub.apiPutItem(
				{ ...pCtx, topology: { ...pCtx.topology, hashSplitN: 4 } },
				{ hashKey: HK, sortKey: SK, data: "v", kind: "text" },
			);
		} catch (error) {
			failure = error;
		}
		expect(FokosError.isCode(failure, INTERNAL_CODES.partition_context_mismatch)).toBe(true);
	});
});
