import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../do-partition.js";
import { PartitionStore } from "../partition/partition-store.js";
import { KeyCodec, type KeyBytes } from "./key-codec.js";
import { PartitionContextCreator, type PartitionContextResolved } from "./partition-context.js";
import { PartitionIdHelper, resolveRangePartitionContext } from "./partition-id.js";
import { HashPartitionTopologyImpl, RangePartitionTopologyImpl, type OperationIntent } from "./split-policy.js";

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

function baseContext(maxSizeMb: number) {
	return PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName: `splitpolicy-${crypto.randomUUID()}`,
		rootTreesN: 1,
		hashSplitN: 2,
		hashSplitConditions: { maxSizeMb },
		rangeSplitN: 2,
		rangeSplitConditions: { maxSizeMb },
	});
}

function hashContext(maxSizeMb: number): PartitionContextResolved {
	const base = baseContext(maxSizeMb);
	const { opaque, doName } = PartitionIdHelper.fromHashIdxs(base, [0]).encode(true);
	return { ...base, doName: doName!, primaryDoIdStr: "", partitionId: opaque };
}

function rangeContext(
	maxSizeMb: number,
	startBoundary: KeyBytes | null = null,
	endBoundary: KeyBytes | null = null,
): PartitionContextResolved {
	return resolveRangePartitionContext(baseContext(maxSizeMb), HK, startBoundary, endBoundary).partitionContext;
}

// Runs `fn` against a topology backed by REAL Durable Object storage, so `sql.databaseSize` is real.
async function withTopology<T>(
	make: (pCtx: PartitionContextResolved, state: DurableObjectState, store: PartitionStore) => T,
	pCtx: PartitionContextResolved,
	fn: (topology: T) => void,
): Promise<void> {
	const stub = PartitionDO.getByName(env.PARTITION_DO, `splitpolicy-${crypto.randomUUID()}`);
	await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
		fn(make(pCtx, state, new PartitionStore(state.storage)));
	});
}

function withHashTopology(pCtx: PartitionContextResolved, fn: (t: HashPartitionTopologyImpl) => void): Promise<void> {
	return withTopology((c, state, store) => new HashPartitionTopologyImpl(c, state, store), pCtx, fn);
}

function withRangeTopology(pCtx: PartitionContextResolved, fn: (t: RangePartitionTopologyImpl) => void): Promise<void> {
	return withTopology((c, state, store) => new RangePartitionTopologyImpl(c, state, store), pCtx, fn);
}
