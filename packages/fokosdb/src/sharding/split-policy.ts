import { HashTopology, HashTopologySnapshot } from "./hash-topology.js";
import { hashChildIndex } from "./hash-primitives.js";
import { KeyCodec, type KeyBytes } from "./key-codec.js";
import type { SplitType } from "./types.js";
import {
	areImmutableOptionsEqual,
	isHashPartition,
	isRangePartition,
	PartitionContextLivePartition,
	type PartitionContextResolved,
} from "./partition-context.js";
import { PartitionIdHelper, resolveDescendantHashPartitionContext, resolveRangePartitionContext } from "./partition-id.js";
import invariant from "../shared/invariant.js";
import type { PartitionInfoInternal, RangeAncestorInfo } from "./types.js";
import { PartitionStore } from "../shared/partition/partition-store.js";
// Type-only. The emit erases it, so the topology and the repartition flow make no runtime cycle.
import type { RepartitionRouting } from "./repartition-types.js";

/**
 * What the operation asking to be routed does to the partition's size. Size backpressure gates on
 * "write" alone, because only a write can grow a partition:
 * - "read" cannot, so refusing it costs availability and buys nothing;
 * - "delete" cannot either, and it is exactly how a client brings an over-size partition back under
 *   its cap — refusing deletes would leave the partition with no way to recover;
 * - "ignore_size_reject" is a transaction commit. `prepare` already persisted the payload into
 *   pending_transactions, so commit only moves those bytes into `items` and drops the pending row.
 *   The size high-water mark is at prepare, never at commit, and refusing it would wedge a
 *   transaction whose outcome the coordinator has already decided.
 *
 * Only "write" takes the backpressure branch. The other three stay distinct so a call site states
 * what it is doing rather than pre-computing the policy's answer.
 *
 * NONE of them bypass routing. "forward" and the range partition's out-of-range "reject" apply to
 * every intent, "ignore_size_reject" included — an out-of-range item is a routing bug, not load, and
 * serving it would touch data this partition does not own.
 */
export type OperationIntent = "read" | "write" | "delete" | "ignore_size_reject";

/**
 * Where one item should be handled. The two rejects are unrelated failures and callers MUST keep
 * them apart:
 * - "reject_over_size" is load. The partition is healthy, just past its cap; the caller retries
 *   later, and a split will bring it back under.
 * - "reject_out_of_range" is a bug. The item reached a range partition that does not own its sort
 *   key, so serving it would touch data belonging to another partition. Retrying cannot fix it.
 *
 * A single "reject" would make an overloaded partition indistinguishable from a broken router in
 * logs, and would make the caller retry a request that can never succeed.
 */
export type RoutingDecision = "ok" | "forward" | "reject_over_size" | "reject_out_of_range";

/**
 * The routing policy of a partition: where an operation goes, and whether the partition has grown
 * past its cap. It decides nothing about the lifecycle of a split. `RepartitionSource` owns that
 * lifecycle, and this class reads the part routing needs through `RepartitionRouting`.
 *
 * Boundary rule: policies decide; only DO classes (and FokosDB) hold stubs and make RPCs.
 */
export interface PartitionTopologySplitter {
	/**
	 * Says where the keys must be handled. The DO calls it before every operation, so it must stay
	 * fast. It is also where backpressure applies.
	 *
	 * `intent` gates "reject_over_size" only: an over-size partition still serves every intent that
	 * does not grow it. "forward" and "reject_out_of_range" are about correctness, not load, so they
	 * do not depend on it.
	 */
	shouldAllow(hashKey: KeyBytes, sortKey: KeyBytes | undefined, intent: OperationIntent): RoutingDecision;

	/**
	 * Whether the partition has grown past its cap, and which kind of split that needs. It is a size
	 * decision only. The DO asks the repartition flow to queue the split, and arbitration there decides
	 * whether it can start. The DO calls this after every write, so it must stay fast.
	 */
	shouldSplit(hashKey?: KeyBytes, sortKey?: KeyBytes): SplitType | null;

	/**
	 * Picks the child partition that owns the keys. A parent uses it during a lazy split migration, so
	 * it can serve requests while the data moves.
	 */
	pickChildPartition(
		partitionContext: PartitionContextResolved,
		hashKey: KeyBytes,
		sortKey?: KeyBytes,
	): { doId: DurableObjectId; partitionContext: PartitionContextResolved };

	/**
	 * Called after a forwarded request returns. Updates the topology cache from the response.
	 */
	recordForwardResult(
		hashKey: KeyBytes,
		fromCtx: PartitionContextResolved,
		toCtx: PartitionContextResolved,
		responsePartitionInfo: PartitionInfoInternal,
	): void;

	updatePartitionContext(partitionContext: PartitionContextLivePartition): void;
}

// The fraction of hashSplitConditions.maxSizeMb that one key must reach to become a promotion candidate.
export const RANGE_PROMOTION_FRACTION = 0.25;

/**
 * Used by the Partition Durable Objects.
 */
export class HashPartitionTopologyImpl implements PartitionTopologySplitter {
	private partitionContext: PartitionContextLivePartition;

	#storage: DurableObjectStorage;
	#routing: RepartitionRouting;
	#partitionStore: PartitionStore;
	#ownerAbsDepth: number;
	#_hashTopology: HashTopology | null = null;

	constructor(
		partitionContext: PartitionContextLivePartition,
		doCtx: DurableObjectState,
		partitionStore: PartitionStore,
		routing: RepartitionRouting,
	) {
		this.partitionContext = partitionContext;
		this.#partitionStore = partitionStore;
		this.#storage = doCtx.storage;
		this.#routing = routing;
		// Load the topology cache eagerly. The constructor is called from ensureTopology() on the
		// first request, after blockConcurrencyWhile has completed, so synchronous KV reads are safe.
		const ownerAbsDepth = PartitionIdHelper.depth(partitionContext._partitionIdBytes ?? Uint8Array.fromHex(partitionContext.partitionId));
		this.#ownerAbsDepth = ownerAbsDepth;
		const snapshot = doCtx.storage.kv.get<HashTopologySnapshot>("__topo_cache");
		if (snapshot) {
			this.#_hashTopology = HashTopology.fromSnapshot(snapshot);
		}
	}

	updatePartitionContext(partitionContext: PartitionContextLivePartition): void {
		invariant(isHashPartition(partitionContext), "fokos/topology: HashPartitionTopologyImpl requires a hash partition context");
		invariant(
			areImmutableOptionsEqual(this.partitionContext, partitionContext) &&
				this.partitionContext.partitionId === partitionContext.partitionId &&
				this.partitionContext.doName === partitionContext.doName,
			"fokos/topology: HashPartitionTopologyImpl partition identity changed",
		);
		this.partitionContext = partitionContext;
	}

	/**
	 * The routing cache, created the first time this partition is a router.
	 *
	 * The constructor cannot decide it. The topology is built on the first request, which is long
	 * before this partition splits. The cache is created on demand, so a router can learn leaf depths
	 * from the responses it forwards.
	 */
	#hashTopology(): HashTopology | null {
		if (this.#_hashTopology) return this.#_hashTopology;
		if (!this.#routing.routerRole()) return null;
		this.#_hashTopology = HashTopology.create(this.partitionContext.hashSplitN, this.#ownerAbsDepth);
		return this.#_hashTopology;
	}

	shouldAllow(_hashKey: KeyBytes, _sortKey: KeyBytes | undefined, intent: OperationIntent): RoutingDecision {
		// From cutover onwards the children own the keys. This partition must not serve them, because it
		// would return data that has moved on. Every request forwards.
		if (this.#routing.routerRole()) return "forward";

		const dbSize = this.#storage.sql.databaseSize;
		// The partition accepts up to 10% above the maximum size before it rejects a write. The margin
		// stops the decision from flapping at the threshold, and it lets the requests that trigger the
		// split complete.
		// Writes only — see OperationIntent for why no other intent can grow the partition.
		if (
			intent === "write" &&
			this.partitionContext.hashSplitConditions.maxSizeMb &&
			dbSize > this.partitionContext.hashSplitConditions.maxSizeMb * 1.1 * 1024 * 1024
		) {
			return "reject_over_size";
		}

		return "ok";
	}

	shouldSplit(_hashKey?: KeyBytes, _sortKey?: KeyBytes): SplitType | null {
		const dbSize = this.#storage.sql.databaseSize;
		if (this.partitionContext.hashSplitConditions.maxSizeMb && dbSize > this.partitionContext.hashSplitConditions.maxSizeMb * 1024 * 1024) {
			// This method does NOT decide mutual exclusion with an unfinished promotion. That question is
			// about the durable repartition rows, and only the arbitration transaction answers it without
			// a race against another queue request.
			return "hash";
		}
		// TODO Track some statistics per hashKey/sortKey in memory to track heavy hitter items.

		// TODO Add more conditions based on the partitionContext.
		return null;
	}

	/**
	 * Internally used by the Partition DOs to route requests to their children after a split happened.
	 * This routes to a descendant partition directly according to the specified relative depth.
	 *
	 * Skips `relativeDepthToLeaf` levels in one shot, computing the descendant partition ID deterministically from the hash key and the owner's depth.
	 * Used by the topology cache to skip known intermediate router hops.
	 */
	pickDescendantHashPartition(
		partitionContext: PartitionContextLivePartition,
		hashKey: KeyBytes,
		relativeDepthToLeaf: number,
	): { doId: DurableObjectId; partitionContext: PartitionContextResolved } {
		const partitionIdBytes = partitionContext._partitionIdBytes ?? Uint8Array.fromHex(partitionContext.partitionId);
		const parentDepth = PartitionIdHelper.depth(partitionIdBytes);

		const hashIdxs: number[] = [];
		for (let i = 0; i < relativeDepthToLeaf; i++) {
			hashIdxs.push(hashChildIndex(hashKey, parentDepth + i, partitionContext.hashSplitN));
		}

		return resolveDescendantHashPartitionContext(this.partitionContext, partitionContext, partitionIdBytes, hashIdxs);
	}

	pickChildPartition(
		partitionContext: PartitionContextLivePartition,
		hashKey: KeyBytes,
		_sortKey?: KeyBytes,
	): { doId: DurableObjectId; partitionContext: PartitionContextResolved } {
		const cache = this.#hashTopology();
		if (cache) {
			// Returns the relative depth of the descendant partition that is non-split according to our cached topology,
			// or 0 if the cache is not populated at all yet.
			const cachedDepth = cache.findLeaf(hashKey);
			if (cachedDepth > 0) {
				return this.pickDescendantHashPartition(partitionContext, hashKey, cachedDepth);
			}
		}
		// Default to immediate child partitions.
		return this.pickDescendantHashPartition(partitionContext, hashKey, 1);
	}

	recordForwardResult(
		hashKey: KeyBytes,
		fromCtx: PartitionContextLivePartition,
		toCtx: PartitionContextLivePartition,
		responsePartitionInfo: PartitionInfoInternal,
	): void {
		if (responsePartitionInfo._internal.rangeAncestors.length > 0) {
			// TODO(perf) Keep in-memory cache of the range ancestor tree so we don't have to re-insert every ancestor on every forward result.
			for (const ancestor of responsePartitionInfo._internal.rangeAncestors) {
				this.#partitionStore.insertRangePartitionBoundary(hashKey, ancestor.startBoundary, ancestor.endBoundary, ancestor.depth);
			}
		}

		// This logic only makes sense for both being hash partitions.
		// FIXME Support learning during when a hash partition forwards to a range partition,
		// which can happen with promoted hash keys.
		if (!isHashPartition(fromCtx) || !isHashPartition(toCtx)) return;

		// targetRelDepth: how many hash-tree levels this single RPC hop crossed.
		// pickChildPartition may have skipped the cache (e.g. depth-2 skip goes straight to the
		// grandchild), so we derive the actual skip from the partition IDs rather than assuming 1.
		const fromAbsDepth = PartitionIdHelper.depth(fromCtx._partitionIdBytes ?? Uint8Array.fromHex(fromCtx.partitionId));
		const toAbsDepth = PartitionIdHelper.depth(toCtx._partitionIdBytes ?? Uint8Array.fromHex(toCtx.partitionId));
		invariant(
			toAbsDepth > fromAbsDepth,
			`fokos/topology.recordForwardResult: toCtx must be a descendant of fromCtx, got fromAbsDepth ${fromAbsDepth} and toAbsDepth ${toAbsDepth}`,
		);
		// The actual response hash depth may be larger than the targetRelDepth
		// if the target partition is itself a router that forwarded further.
		// It could also be the case that the target hash partition forwarded to a range partition,
		// and in that case the responseHashDepth would be equal to the target partition depth.
		const responseHashDepth = responsePartitionInfo.hashDepth;
		invariant(
			responseHashDepth >= toAbsDepth,
			`fokos/topology.recordForwardResult: responseHashDepth must be >= toAbsDepth, got responseHashDepth ${responseHashDepth} and toAbsDepth ${toAbsDepth}`,
		);

		const targetRelDepth = responseHashDepth - fromAbsDepth;
		const cache = this.#hashTopology();
		if (cache && targetRelDepth > 0) {
			if (cache.updateFromHint(hashKey, targetRelDepth)) {
				this.#storage.kv.put<HashTopologySnapshot>("__topo_cache", cache.toSnapshot());
			}
		}
	}
}

function sameOptionalKey(a: KeyBytes | null | undefined, b: KeyBytes | null | undefined): boolean {
	return a == null || b == null ? a == b : KeyCodec.compare(a, b) === 0;
}

/**
 * Topology splitter for range-structure DOs. A range DO owns exactly one hashKey and a fixed,
 * immutable [startBoundary, endBoundary) slice of the sortKey axis. On split it becomes a pure
 * router (owns nothing locally) and creates N children that tile [start, end) — including a new
 * leftmost child — then forwards every sort key to the owning child. A leaf is never also a router.
 */
export class RangePartitionTopologyImpl implements PartitionTopologySplitter {
	#storage: DurableObjectStorage;
	#routing: RepartitionRouting;
	#partitionStore: PartitionStore;

	private partitionContext: PartitionContextLivePartition & {
		rangePartition: NonNullable<PartitionContextLivePartition["rangePartition"]>;
	};

	constructor(pCtx: PartitionContextLivePartition, ctx: DurableObjectState, partitionStore: PartitionStore, routing: RepartitionRouting) {
		invariant(isRangePartition(pCtx), "fokos/topology: RangePartitionTopologyImpl must be initialized with a range partition context");
		this.partitionContext = pCtx;
		this.#storage = ctx.storage;
		this.#partitionStore = partitionStore;
		this.#routing = routing;
	}

	updatePartitionContext(partitionContext: PartitionContextLivePartition): void {
		invariant(isRangePartition(partitionContext), "fokos/topology: RangePartitionTopologyImpl requires a range partition context");
		invariant(
			areImmutableOptionsEqual(this.partitionContext, partitionContext) &&
				this.partitionContext.partitionId === partitionContext.partitionId &&
				this.partitionContext.doName === partitionContext.doName &&
				sameOptionalKey(this.partitionContext.rangePartition.hashKey, partitionContext.rangePartition.hashKey) &&
				sameOptionalKey(this.partitionContext.rangePartition.startBoundary, partitionContext.rangePartition.startBoundary) &&
				sameOptionalKey(this.partitionContext.rangePartition.endBoundary, partitionContext.rangePartition.endBoundary),
			"fokos/topology: RangePartitionTopologyImpl partition identity changed",
		);
		this.partitionContext = partitionContext;
	}

	shouldAllow(_hashKey: KeyBytes, sortKey: KeyBytes | undefined, intent: OperationIntent): RoutingDecision {
		const sk = sortKey ?? KeyCodec.encodeOptional(undefined);

		// After the split, this DO is a pure router and owns nothing locally. Everything goes to a child.
		if (this.#routing.routerRole()) return "forward";

		// Boundaries are immutable identity. null = unbounded edge.
		const start = this.partitionContext.rangePartition!.startBoundary ?? KeyCodec.encodeOptional(undefined);
		const end = this.partitionContext.rangePartition!.endBoundary;
		const inRange = KeyCodec.compare(sk, start) >= 0 && (end === null || KeyCodec.compare(sk, end) < 0);
		if (!inRange) {
			// Out of the owned range. Correct routing never reaches this, so it is a routing defect.
			return "reject_out_of_range";
		}

		// Size-based backpressure (10% overage allowed, writes only — consistent with the hash partition).
		if (
			intent === "write" &&
			this.partitionContext.rangeSplitConditions?.maxSizeMb &&
			this.#storage.sql.databaseSize > this.partitionContext.rangeSplitConditions.maxSizeMb * 1.1 * 1024 * 1024
		) {
			return "reject_over_size";
		}

		return "ok";
	}

	shouldSplit(_hashKey?: KeyBytes, _sortKey?: KeyBytes): SplitType | null {
		if (!this.partitionContext.rangeSplitConditions) return null;
		const dbSize = this.#storage.sql.databaseSize;
		if (
			this.partitionContext.rangeSplitConditions.maxSizeMb &&
			dbSize > this.partitionContext.rangeSplitConditions.maxSizeMb * 1024 * 1024
		) {
			return "range";
		}
		return null;
	}

	pickChildPartition(
		partitionContext: PartitionContextResolved,
		_hashKey: KeyBytes,
		sortKey?: KeyBytes,
	): { doId: DurableObjectId; partitionContext: PartitionContextResolved } {
		const sk = sortKey ?? KeyCodec.encodeOptional(undefined);
		const targets = this.#routing.splitTargets();
		invariant(targets.length > 0, "fokos/range: pickChildPartition called without an active split");

		// TODO: The targets arrive in target_index order, which is ascending boundary order. This loop
		// can therefore stop when childStart > sk, or use a binary search above about 10 children.
		// The N children tile the whole owned range; route to the one with the largest startBoundary <= sk.
		let best: { start: KeyBytes | null; end: KeyBytes | null } | null = null;
		let bestStart: KeyBytes | null = null;
		for (const target of targets) {
			invariant(target.slice.kind === "range", "fokos/range: a range split target must carry a range slice");
			const childStart = target.slice.start ?? KeyCodec.encodeOptional(undefined);
			if (KeyCodec.compare(childStart, sk) <= 0) {
				if (best === null || bestStart === null || KeyCodec.compare(childStart, bestStart) > 0) {
					best = { start: target.slice.start, end: target.slice.end };
					bestStart = childStart;
				}
			}
		}
		// The children tile the whole range, so at least the leftmost child has startBoundary <= sk.
		invariant(best !== null, () => `fokos/range: no child found for sortKey ${KeyCodec.keyForLog(sk)}`);

		// Skip intermediate router hops: if we have learned (from prior forward results) a deeper slice
		// that is a strict sub-slice of the immediate child and still contains sk, jump straight to it.
		// Boundaries are immutable identity, so a stale hint at worst lands on a router that forwards on;
		// the target's shouldAllow validates range membership, so a bad hint can never corrupt data.
		const hashKey = this.partitionContext.rangePartition.hashKey;
		const learned = this.#partitionStore.findDeepestKnownRangeSlice(hashKey, sk);
		if (learned && isStrictSubSlice(learned, best.start, best.end)) {
			return resolveRangePartitionContext(partitionContext, hashKey, learned.startBoundary, learned.endBoundary);
		}

		// Rebuild the child context from THIS router's current context plus the child's stored immutable
		// boundaries. The stored child context is a snapshot taken at split time, so forwarding it would
		// hand the child split thresholds that an operator has since changed, and the child would persist
		// those stale values as its own. Boundaries, hashKey, ns and tableName are immutable, so the
		// rebuilt identity (doName, partitionId) is byte-for-byte the stored one.
		return resolveRangePartitionContext(partitionContext, hashKey, best.start, best.end);
	}

	recordForwardResult(
		hashKey: KeyBytes,
		_fromCtx: PartitionContextResolved,
		_toCtx: PartitionContextResolved,
		responsePartitionInfo: PartitionInfoInternal,
	): void {
		// TODO(perf) Remove for optimization.
		invariant(
			KeyCodec.compare(hashKey, this.partitionContext.rangePartition.hashKey) === 0,
			"fokos/range.recordForwardResult: hashKey mismatch",
		);

		// TODO(perf) Keep in-memory cache of the range ancestor tree so we don't have to re-insert every ancestor on every forward result.
		for (const ancestor of responsePartitionInfo._internal.rangeAncestors) {
			this.#partitionStore.insertRangePartitionBoundary(
				// Always the real hash key, never the empty sentinel — `setRangeAncestors` writes ancestors
				// under the same key, so the two writers share one convention and the primary key dedupes
				// a learned boundary against the identical ancestor row. Storing this tree under the empty
				// key to save bytes would put one keyspace under two labels, and `getRangeAncestors` could
				// no longer tell its own ancestors from learned rows.
				this.partitionContext.rangePartition.hashKey,
				ancestor.startBoundary,
				ancestor.endBoundary,
				ancestor.depth,
			);
		}

		return;
	}
}

/**
 * Selects the bounded ancestor set a splitting range partition passes to its children.
 * shallowest `fromRoot` + deepest `fromLeaf` of the parent's own candidate list
 * (parent's stored ancestors plus the parent itself), deduped by depth.
 * Called once per split — identical for every child produced by that split.
 */
// Compares range start boundaries where null = -∞ (unbounded lower edge).
function startCmp(a: KeyBytes | null, b: KeyBytes | null): number {
	if (a === null && b === null) return 0;
	if (a === null) return -1;
	if (b === null) return 1;
	return KeyCodec.compare(a, b);
}

// Compares range end boundaries where null = +∞ (unbounded upper edge).
function endCmp(a: KeyBytes | null, b: KeyBytes | null): number {
	if (a === null && b === null) return 0;
	if (a === null) return 1;
	if (b === null) return -1;
	return KeyCodec.compare(a, b);
}

/**
 * True when [slice.start, slice.end) is strictly contained within [childStart, childEnd) — i.e. a
 * deeper, narrower descendant. In a range tree any two slices containing the same key are nested, so
 * a strict sub-slice of the immediate child is always a valid deeper skip target. An equal or wider
 * slice (an ancestor) fails this check and the caller falls back to the immediate child.
 */
function isStrictSubSlice(
	slice: { startBoundary: KeyBytes | null; endBoundary: KeyBytes | null },
	childStart: KeyBytes | null,
	childEnd: KeyBytes | null,
): boolean {
	const startRel = startCmp(slice.startBoundary, childStart);
	const endRel = endCmp(slice.endBoundary, childEnd);
	return startRel >= 0 && endRel <= 0 && (startRel > 0 || endRel < 0);
}

export function selectRangeAncestors(
	parentDepth: number,
	parentAncestors: RangeAncestorInfo[],
	parentAsAncestor: RangeAncestorInfo,
	config: { fromRoot: number; fromLeaf: number },
): RangeAncestorInfo[] {
	const candidates = parentDepth === 0 ? [] : [...parentAncestors, parentAsAncestor];

	// Candidates are already sorted by depth ascending (parentAncestors is stored sorted, and
	// parentAsAncestor.depth === parentDepth is strictly greater than every stored ancestor's depth).
	const shallowest = candidates.slice(0, config.fromRoot);
	// Must NOT use candidates.slice(-fromLeaf): slice(-0) === slice(0), which would return the
	// entire array instead of [] when fromLeaf === 0.
	const deepest = candidates.slice(Math.max(0, candidates.length - config.fromLeaf));

	const byDepth = new Map<number, RangeAncestorInfo>();
	for (const c of [...shallowest, ...deepest]) {
		byDepth.set(c.depth, c);
	}
	return [...byDepth.values()].sort((a, b) => a.depth - b.depth);
}
