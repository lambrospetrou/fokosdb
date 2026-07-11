import { HashTopology, HashTopologySnapshot } from "./hash-topology.js";
import { hashChildIndex } from "../hash-primitives.js";
import { KeyCodec, type KeyBytes } from "./key-codec.js";
import type { SplitType } from "./types.js";
import {
	assertCtxHasIdBytes,
	isHashPartition,
	isRangePartition,
	PartitionContextLivePartition,
	type InitFromSplitOptions,
	type PartitionContextResolved,
} from "./partition-context.js";
import {
	PartitionIdHelper,
	resolveDescendantHashPartitionContext,
	resolveDoId,
	resolveHashChildPartitionContexts,
	resolveRangePartitionContext,
} from "./partition-id.js";
import { SplitStateMachine, type SplitStatusKVItem } from "./split-state.js";
import invariant from "../invariant.js";
import type { PartitionInfo, RangeAncestorInfo } from "../types.js";
import { PartitionStore } from "../partition/partition-store.js";

// Re-exported here as well: the plan files SplitStatusKVItem under split-policy; it is defined
// with the state machine that owns it in split-state.ts.
export type { SplitStatusKVItem };

/**
 * Inputs the split decision needs from the DO. The policies must not query partition-owned
 * tables (boundary rule); the DO answers these from its own state.
 */
export type SplitDecisionInputs = {
	/**
	 * Promotion⇄hash-split mutual exclusion: true when any promoted key is queued or promoting
	 * (answered from the DO's in-memory promoted-keys cache).
	 */
	hasInFlightPromotions: boolean;
};

export type PrepareSplitParams = {
	/**
	 * The depth of the partition that is preparing to split.
	 * This is used to determine the depth of the child partitions that will be created as a result of the split.
	 * 0 = root partition, 1 = first-level child, etc.
	 * 0 can refer to the root range partition and the root hash partition.
	 */
	parentDepth: number;

	/**
	 * Range splits only: precomputed split boundaries from
	 * PartitionStore.computeRangeSplitBoundaries (a data query, so it lives with the store).
	 * null = not enough distinct items to split yet; prepareSplit returns null and the caller
	 * retries on a later cycle.
	 */
	boundaries?: KeyBytes[] | null;
	/**
	 * Range splits only: the SPLITTING PARENT's own currently-stored ancestor list — i.e. the
	 * partition calling prepareSplit, not the children being created (local-state read, so it lives
	 * with the DO — same boundary rule as `boundaries`). Combined with the parent's own depth and
	 * boundaries, this is the input selectRangeAncestors uses to compute the children's ancestor
	 * list. Root: [].
	 */
	parentRangeAncestors?: RangeAncestorInfo[];
};

/**
 * The split policy of a partition: pure decisions (shouldAllow / shouldSplit / prepareSplit /
 * pickChildPartition) plus delegation to its KV-backed SplitStateMachine.
 *
 * Boundary rule: policies decide; only DO classes (and FokosDB) hold stubs and make RPCs.
 * `prepareSplit` returns the child contexts to create; the DO performs the initFromSplit /
 * triggerMigration fan-out and calls `commitSplitStarted` between the two.
 */
export interface PartitionTopologySplitter {
	childPartitionContexts(): PartitionContextResolved[] | undefined;

	splitStatus(): SplitStatusKVItem | undefined;

	/**
	 * Called before every operation to check if the partition can accept the request based on the provided context, storage, and keys.
	 * This can be used to implement backpressure or to prevent writes to certain partitions based on custom logic.
	 *
	 * This should be extremely fast since it's called in every request!
	 */
	shouldAllow(hashKey: KeyBytes, sortKey?: KeyBytes): "forward" | "reject" | "ok";

	/**
	 * Determines whether a partition should be split based on the provided context, storage, and keys.
	 * This method is called after every write operation to check if the partition needs to be split.
	 *
	 * This should be extremely fast since it's called in every request!
	 *
	 * Basic checks according to the conditions and potentially do more expensive things in a periodic check.
	 *
	 * Automatically queues a split if the conditions are met, so the caller doesn't need to worry about it.
	 */
	maybeQueueSplit(hashKey: KeyBytes, sortKey: KeyBytes | undefined, inputs: SplitDecisionInputs): Promise<SplitStatusKVItem | undefined>;

	/**
	 * Computes and validates the child partition contexts for the queued split. Pure decision —
	 * performs no RPC and no KV transition. Returns null when there is nothing to do (no queued
	 * split, or a range split without enough items yet). The DO then performs the initFromSplit
	 * fan-out and, on success, calls commitSplitStarted.
	 */
	prepareSplit(inputs: PrepareSplitParams): InitFromSplitOptions[] | null;

	/**
	 * Transitions split_queued → split_started after ALL children initialized successfully.
	 * Idempotent.
	 */
	commitSplitStarted(children: PartitionContextResolved[]): void;

	/**
	 * Used only internally by the Partition DOs to determine which of their children should received a request based on the provided context and keys.
	 * Used during the lazy split migration of data to avoid blocking wholesale migration of the data before requests can be handled.
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
		responsePartitionInfo: PartitionInfo,
	): void;

	/**
	 * Called by a child partition after it has fully migrated its share of data from the parent.
	 * Idempotent. Transitions the parent to split_completed once all children have acknowledged.
	 */
	acknowledgeChildMigration(childDoName: string): void;
}

// Fraction of rangeSplitConditions.maxSizeMb a single key must reach before it is a promotion candidate.
export const RANGE_PROMOTION_FRACTION = 0.25;

/**
 * Used by the Partition Durable Objects.
 */
export class HashPartitionTopologyImpl implements PartitionTopologySplitter {
	private static readonly KV_KEYS = {
		SPLIT_STATUS: "__split_status",
	};

	private partitionContext: PartitionContextLivePartition;

	#storage: DurableObjectStorage;
	#splitState: SplitStateMachine;
	#partitionStore: PartitionStore;
	#_hashTopology: HashTopology | null = null;

	constructor(partitionContext: PartitionContextLivePartition, doCtx: DurableObjectState, partitionStore: PartitionStore) {
		this.partitionContext = partitionContext;
		this.#partitionStore = partitionStore;
		this.#storage = doCtx.storage;
		this.#splitState = new SplitStateMachine(doCtx.storage, HashPartitionTopologyImpl.KV_KEYS.SPLIT_STATUS);
		// Load the topology cache eagerly. The constructor is called from ensureTopology() on the
		// first request, after blockConcurrencyWhile has completed, so synchronous KV reads are safe.
		const ownerAbsDepth = PartitionIdHelper.depth(partitionContext._partitionIdBytes ?? Uint8Array.fromHex(partitionContext.partitionId));
		const snapshot = doCtx.storage.kv.get<HashTopologySnapshot>("__topo_cache");
		if (snapshot) {
			this.#_hashTopology = HashTopology.fromSnapshot(snapshot);
		} else {
			const splitStatus = this.#splitState.splitStatus();
			if (splitStatus?.status === "split_started" || splitStatus?.status === "split_completed") {
				this.#_hashTopology = HashTopology.create(partitionContext.hashSplitN, ownerAbsDepth);
			}
		}
	}

	shouldAllow(_hashKey: KeyBytes, _sortKey?: KeyBytes): "forward" | "reject" | "ok" {
		// If the split has started but not completed, we should reject requests to the partition to avoid data loss or returning wrong data.
		// TODO - Keep this in memory to avoid reading it all the time from storage.
		const splitStatus = this.#splitState.splitStatus();
		if (splitStatus && splitStatus.status !== "split_queued") {
			return "forward";
		}

		const dbSize = this.#storage.sql.databaseSize;
		// We allow up to 10% over the max size before we start rejecting requests to avoid flapping around the threshold,
		// and to allow the requests to complete and trigger the split.
		if (
			this.partitionContext.hashSplitConditions.maxSizeMb &&
			dbSize > this.partitionContext.hashSplitConditions.maxSizeMb * 1.1 * 1024 * 1024
		) {
			return "reject";
		}

		// All good!
		return "ok";
	}

	childPartitionContexts(): PartitionContextResolved[] | undefined {
		return this.#splitState.childPartitionContexts();
	}

	splitStatus(): SplitStatusKVItem | undefined {
		return this.#splitState.splitStatus();
	}

	async maybeQueueSplit(
		hashKey: KeyBytes,
		sortKey: KeyBytes | undefined,
		inputs: SplitDecisionInputs,
	): Promise<SplitStatusKVItem | undefined> {
		const splitType = this.shouldSplit(hashKey, sortKey, inputs);
		if (splitType) {
			return this.#splitState.queueSplit(splitType, this.partitionContext);
		}
	}

	shouldSplit(_hashKey: KeyBytes, _sortKey: KeyBytes | undefined, inputs: SplitDecisionInputs): SplitType | null {
		const dbSize = this.#storage.sql.databaseSize;
		if (this.partitionContext.hashSplitConditions.maxSizeMb && dbSize > this.partitionContext.hashSplitConditions.maxSizeMb * 1024 * 1024) {
			// Mutual exclusion: no hash split while any promoted key is queued or promoting.
			// A split while promotion is in-flight would leave the range root acking a parent
			// that has become a router, stranding the key's status.
			if (inputs.hasInFlightPromotions) return null;
			return "hash";
		}
		// TODO Track some statistics per hashKey/sortKey in memory to track heavy hitter items.

		// TODO Add more conditions based on the partitionContext.
		return null;
	}

	prepareSplit(_inputs: PrepareSplitParams): InitFromSplitOptions[] | null {
		const splitStatus = this.splitStatus();
		if (!splitStatus || splitStatus.status !== "split_queued") {
			// Already started or completed — idempotent no-op.
			return null;
		}
		// Hash-partition DOs never queue a range split; range splits are handled by RangePartitionTopologyImpl.
		invariant(splitStatus.splitType === "hash", "fokos/topology.prepareSplit: unexpected splitType 'range' on a hash-partition topology");

		const childContexts = resolveHashChildPartitionContexts(this.partitionContext);
		invariant(
			childContexts.length === this.partitionContext.hashSplitN,
			`fokos/topology.prepareSplit: expected ${this.partitionContext.hashSplitN} children, got ${childContexts.length}`,
		);
		const uniqueChildNames = new Set(childContexts.map((c) => c.doName));
		invariant(uniqueChildNames.size === childContexts.length, "fokos/topology.prepareSplit: duplicate child doNames detected");

		return childContexts.map((newPartitionContext) => ({
			parentPartitionContext: this.partitionContext,
			newPartitionContext,
			splitType: "hash" as const,
		}));
	}

	commitSplitStarted(children: PartitionContextResolved[]): void {
		this.#splitState.commitSplitStarted(children);

		// Initialize the topology cache so forwarded requests can learn and skip hops.
		if (!this.#_hashTopology) {
			const ownerAbsDepth = PartitionIdHelper.depth(
				this.partitionContext._partitionIdBytes ?? Uint8Array.fromHex(this.partitionContext.partitionId),
			);
			this.#_hashTopology = HashTopology.create(this.partitionContext.hashSplitN, ownerAbsDepth);
		}
	}

	acknowledgeChildMigration(childDoName: string): void {
		this.#splitState.acknowledgeChildMigration(childDoName);
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
		if (this.#_hashTopology) {
			// Returns the relative depth of the descendant partition that is non-split according to our cached topology,
			// or 0 if the cache is not populated at all yet.
			const cachedDepth = this.#_hashTopology.findLeaf(hashKey);
			if (cachedDepth > 0) {
				return this.pickDescendantHashPartition(partitionContext, hashKey, cachedDepth);
			}
		}
		// Default to immediate child partitions.
		return this.pickDescendantHashPartition(partitionContext, hashKey, 1);
	}

	makeIsCorrectChildHashPartition(
		_parentContext: PartitionContextLivePartition,
		childContext: PartitionContextLivePartition,
	): (hashKey: KeyBytes, sortKey?: KeyBytes) => boolean {
		const childPartitionIdBytes = childContext._partitionIdBytes ?? Uint8Array.fromHex(childContext.partitionId);
		const childLevel = PartitionIdHelper.depth(childPartitionIdBytes);
		invariant(childLevel >= 1, `fokos/topology.makeIsCorrectChildHashPartition: childLevel must be >= 1, got ${childLevel}`);
		const childIdx = PartitionIdHelper.lastChildIdx(childPartitionIdBytes);
		invariant(
			childIdx < childContext.hashSplitN,
			`fokos/topology.makeIsCorrectChildHashPartition: childIdx ${childIdx} out of range for splitN ${childContext.hashSplitN}`,
		);
		return (hashKey: KeyBytes, _sortKey?: KeyBytes) => {
			const hashedIdx = hashChildIndex(hashKey, childLevel - 1, childContext.hashSplitN);
			return hashedIdx === childIdx;
		};
	}

	recordForwardResult(
		hashKey: KeyBytes,
		fromCtx: PartitionContextLivePartition,
		toCtx: PartitionContextLivePartition,
		responsePartitionInfo: PartitionInfo,
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
		if (this.#_hashTopology && targetRelDepth > 0) {
			if (this.#_hashTopology.updateFromHint(hashKey, targetRelDepth)) {
				this.#storage.kv.put<HashTopologySnapshot>("__topo_cache", this.#_hashTopology.toSnapshot());
			}
		}
	}
}

/**
 * Topology splitter for range-structure DOs. A range DO owns exactly one hashKey and a fixed,
 * immutable [startBoundary, endBoundary) slice of the sortKey axis. On split it becomes a pure
 * router (owns nothing locally) and creates N children that tile [start, end) — including a new
 * leftmost child — then forwards every sort key to the owning child. A leaf is never also a router.
 */
export class RangePartitionTopologyImpl implements PartitionTopologySplitter {
	private static readonly KV_KEYS = {
		SPLIT_STATUS: "__split_status",
	};

	#storage: DurableObjectStorage;
	#splitState: SplitStateMachine;
	#partitionStore: PartitionStore;

	private partitionContext: PartitionContextLivePartition & {
		rangePartition: NonNullable<PartitionContextLivePartition["rangePartition"]>;
	};

	constructor(pCtx: PartitionContextLivePartition, ctx: DurableObjectState, partitionStore: PartitionStore) {
		invariant(isRangePartition(pCtx), "fokos/topology: RangePartitionTopologyImpl must be initialized with a range partition context");
		this.partitionContext = pCtx;
		this.#storage = ctx.storage;
		this.#partitionStore = partitionStore;
		this.#splitState = new SplitStateMachine(ctx.storage, RangePartitionTopologyImpl.KV_KEYS.SPLIT_STATUS);
	}

	splitStatus(): SplitStatusKVItem | undefined {
		return this.#splitState.splitStatus();
	}

	childPartitionContexts(): PartitionContextResolved[] | undefined {
		return this.#splitState.childPartitionContexts();
	}

	shouldAllow(_hashKey: KeyBytes, sortKey?: KeyBytes): "forward" | "reject" | "ok" {
		const sk = sortKey ?? KeyCodec.encodeOptional(undefined);

		const splitStatus = this.splitStatus();
		if (splitStatus?.status === "split_started" || splitStatus?.status === "split_completed") {
			// Once split, this DO is a pure router (owns nothing locally) — forward everything to a child.
			return "forward";
		}

		// Boundaries are immutable identity. null = unbounded edge.
		const start = this.partitionContext.rangePartition!.startBoundary ?? KeyCodec.encodeOptional(undefined);
		const end = this.partitionContext.rangePartition!.endBoundary;
		const inRange = KeyCodec.compare(sk, start) >= 0 && (end === null || KeyCodec.compare(sk, end) < 0);
		if (!inRange) {
			// Out of owned range — routing bug; should not happen via correct routing.
			return "reject";
		}

		// Size-based backpressure (10% overage allowed, consistent with hash partition).
		if (
			this.partitionContext.rangeSplitConditions?.maxSizeMb &&
			this.#storage.sql.databaseSize > this.partitionContext.rangeSplitConditions.maxSizeMb * 1.1 * 1024 * 1024
		) {
			return "reject";
		}

		return "ok";
	}

	async maybeQueueSplit(
		_hashKey: KeyBytes,
		_sortKey: KeyBytes | undefined,
		_inputs: SplitDecisionInputs,
	): Promise<SplitStatusKVItem | undefined> {
		const splitType = this.shouldSplit();
		if (splitType) {
			return this.#splitState.queueSplit(splitType, this.partitionContext);
		}
	}

	private shouldSplit(): SplitType | null {
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

	prepareSplit(params: PrepareSplitParams): InitFromSplitOptions[] | null {
		const splitStatus = this.splitStatus();
		if (!splitStatus || splitStatus.status !== "split_queued") {
			// Already started or completed — idempotent no-op.
			return null;
		}

		if (!isRangePartition(this.partitionContext)) {
			throw new Error("fokos/range.prepareSplit: called on a non-range partition");
		}
		const rp = this.partitionContext.rangePartition;
		invariant(rp, "fokos/range.prepareSplit: missing rangePartition identity");
		const N = this.partitionContext.rangeSplitN;
		invariant(N != null && N >= 2, "fokos/range.prepareSplit: rangeSplitN must be >= 2");

		// Boundaries are a data query (PartitionStore.computeRangeSplitBoundaries); the DO passes
		// them in. null = not enough distinct items to split into N non-empty children yet.
		const boundaries = params.boundaries;
		if (!boundaries) return null;
		invariant(boundaries.length === N - 1, `fokos/range.prepareSplit: expected ${N - 1} boundaries, got ${boundaries.length}`);

		// The N children tile [start, end): child i owns [starts[i], ends[i]). The leftmost child
		// (start, B1) is a brand-new DO — this node retains no slice and becomes a pure router.
		const starts: (KeyBytes | null)[] = [rp.startBoundary, ...boundaries];
		const ends: (KeyBytes | null)[] = [...boundaries, rp.endBoundary];

		// Ancestor selection: computed once, identical for every child produced by
		// this split, depends only on the splitting parent's own depth/ancestors/boundaries.
		const parentAncestors = params.parentRangeAncestors ?? [];
		const parentAsAncestor: RangeAncestorInfo = {
			depth: params.parentDepth,
			startBoundary: rp.startBoundary ?? KeyCodec.encodeOptional(undefined),
			endBoundary: rp.endBoundary ?? KeyCodec.encodeOptional(undefined),
		};
		const config = this.partitionContext.rangeAncestorsConfig;
		const childAncestors = selectRangeAncestors(params.parentDepth, parentAncestors, parentAsAncestor, config);
		const childDepth = params.parentDepth + 1;

		const childInits: InitFromSplitOptions[] = [];
		for (let i = 0; i < N; i++) {
			const { partitionContext: childCtx } = resolveRangePartitionContext(this.partitionContext, rp.hashKey, starts[i], ends[i]);
			childInits.push({
				splitType: "range",
				parentPartitionContext: this.partitionContext,
				newPartitionContext: { ...childCtx },
				newPartitionRangeDepth: childDepth,
				rangeAncestors: childAncestors,
			});
		}
		const uniqueNames = new Set(childInits.map((c) => c.newPartitionContext.doName));
		invariant(uniqueNames.size === childInits.length, "fokos/range.prepareSplit: duplicate child doNames detected");
		return childInits;
	}

	commitSplitStarted(children: PartitionContextResolved[]): void {
		// Become a pure router: split_started is persisted with exactly the N children (set once, never accumulates).
		this.#splitState.commitSplitStarted(children);
	}

	acknowledgeChildMigration(childDoName: string): void {
		this.#splitState.acknowledgeChildMigration(childDoName);
	}

	pickChildPartition(
		partitionContext: PartitionContextResolved,
		_hashKey: KeyBytes,
		sortKey?: KeyBytes,
	): { doId: DurableObjectId; partitionContext: PartitionContextResolved } {
		const sk = sortKey ?? KeyCodec.encodeOptional(undefined);
		const splitStatus = this.splitStatus();
		invariant(
			splitStatus?.status === "split_started" || splitStatus?.status === "split_completed",
			"fokos/range: pickChildPartition called without an active split",
		);

		// TODO: Keep childPartitionContexts sorted by startBoundary so we can break early once
		// childStart > sk, or use binary search for arrays larger than ~10 entries.
		// The N children tile the whole owned range; route to the one with the largest startBoundary <= sk.
		let best: PartitionContextResolved | null = null;
		let bestStart: KeyBytes | null = null;
		for (const childCtx of splitStatus.childPartitionContexts) {
			const childStart = childCtx.rangePartition!.startBoundary ?? KeyCodec.encodeOptional(undefined);
			if (KeyCodec.compare(childStart, sk) <= 0) {
				if (best === null || bestStart === null || KeyCodec.compare(childStart, bestStart) > 0) {
					best = childCtx;
					bestStart = childStart;
				}
			}
		}
		if (best === null) {
			// This should never happen: the children tile the whole range, so at least the leftmost
			// child must have startBoundary <= sk.
			throw new Error(`fokos/range: no child found for sortKey ${KeyCodec.keyForLog(sk)}`);
		}

		// Skip intermediate router hops: if we have learned (from prior forward results) a deeper slice
		// that is a strict sub-slice of the immediate child and still contains sk, jump straight to it.
		// Boundaries are immutable identity, so a stale hint at worst lands on a router that forwards on;
		// the target's shouldAllow validates range membership, so a bad hint can never corrupt data.
		const hashKey = this.partitionContext.rangePartition.hashKey;
		const learned = this.#partitionStore.findDeepestKnownRangeSlice(hashKey, sk);
		if (learned && isStrictSubSlice(learned, best.rangePartition!.startBoundary ?? null, best.rangePartition!.endBoundary ?? null)) {
			return resolveRangePartitionContext(partitionContext, hashKey, learned.startBoundary, learned.endBoundary);
		}

		return { doId: resolveDoId(partitionContext.ns, best.doName), partitionContext: best };
	}

	recordForwardResult(
		hashKey: KeyBytes,
		_fromCtx: PartitionContextResolved,
		_toCtx: PartitionContextResolved,
		responsePartitionInfo: PartitionInfo,
	): void {
		// TODO(perf) Remove for optimization.
		invariant(
			KeyCodec.compare(hashKey, this.partitionContext.rangePartition.hashKey) === 0,
			"fokos/range.recordForwardResult: hashKey mismatch",
		);

		// TODO(perf) Keep in-memory cache of the range ancestor tree so we don't have to re-insert every ancestor on every forward result.
		for (const ancestor of responsePartitionInfo._internal.rangeAncestors) {
			this.#partitionStore.insertRangePartitionBoundary(
				// TODO: We can optimize this by inserting an empty hash key since this range partition tree is about a single hash key.
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
