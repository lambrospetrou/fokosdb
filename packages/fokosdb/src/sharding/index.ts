/**
 * Sharding entry point: identity, routing, topology caches, and the repartition flow that moves
 * ownership between partitions.
 *
 * Nothing here reads FokosDB items, expressions, or transactions. The `check-client-bundle` plugin in
 * `tsdown.config.ts` fails the build when a module of this entry reaches one of those.
 */

// ─── Keys and hashing ─────────────────────────────────────────────────────────

export { KeyCodec } from "./key-codec.js";
export type { KeyBytes } from "./key-codec.js";
export { GOLDEN_RATIO, GOLDEN_RATIO_BIGINT, hash32, hash64, hashChildIndex, hashRootIndex } from "./hash-primitives.js";
export { AddResult, BloomFilter } from "./bloom-filter.js";
export type { BloomFilterSnapshot } from "./bloom-filter.js";

// ─── Identity and context ─────────────────────────────────────────────────────

export {
	PartitionContextCreator,
	areImmutableOptionsEqual,
	areMutableOptionsEqual,
	assertCtxHasIdBytes,
	isHashPartition,
	isRangePartition,
	pCtxForLog,
} from "./partition-context.js";
export type {
	InitFromSplitOptions,
	PartitionContext,
	PartitionContextLivePartition,
	PartitionContextResolved,
	PartitionNamespaceKey,
	SplitConditions,
	TransactionCoordinatorNamespaceKey,
} from "./partition-context.js";
export {
	PartitionIdHelper,
	RANGE_MAX,
	RANGE_MIN,
	resolveDescendantHashPartitionContext,
	resolveDoId,
	resolveHashChildPartitionContexts,
	resolveRangePartitionContext,
} from "./partition-id.js";
export type {
	PartitionInfoInternal,
	PartitionNodeId,
	RangeAncestorInfo,
	RegisterSplitOptions,
	SplitStatus,
	SplitType,
	TopologyKVItem,
	TopologyNode,
	TopologyTree,
} from "./types.js";

// ─── Routing and caches ───────────────────────────────────────────────────────

export { PartitionTopologyRouterImpl } from "./router.js";
export type { PartitionTopologyRouter } from "./router.js";
export { forwardedMeta, learnFromErrorMeta, routedError, stampRoutingMeta } from "./forward-meta.js";
export type { RoutedError } from "./forward-meta.js";
export { HashTopology } from "./hash-topology.js";
export type { HashTopologySnapshot } from "./hash-topology.js";
export { PartialRangeTopology } from "./partial-range-topology.js";
export type { PartialRangeTopologySnapshot } from "./partial-range-topology.js";
export { HashPartitionTopologyImpl, RangePartitionTopologyImpl, RANGE_PROMOTION_FRACTION, selectRangeAncestors } from "./split-policy.js";
export type { OperationIntent, PartitionTopologySplitter, RoutingDecision } from "./split-policy.js";
export {
	clipToChildRange,
	cursorFallsInChild,
	isChildFullyBeforeCursor,
	makeBoundaryCursor,
	normalizeSkInterval,
	rangeIntersects,
} from "./sk-interval.js";
export type { SkInterval } from "./sk-interval.js";

// ─── Repartition flow ─────────────────────────────────────────────────────────

export { REPARTITION_KV_KEYS, REPARTITION_RPC_CONCURRENCY, RepartitionSource, RepartitionTarget } from "./repartition-flow.js";
export type {
	RepartitionCommonDeps,
	RepartitionIdentity,
	RepartitionPlan,
	RepartitionSourceDeps,
	RepartitionTargetDeps,
	StepOutcome,
} from "./repartition-flow.js";
export { assertPointInSlice, clipQueryToSlice, sliceIncludesHashKey, sliceIncludesItem } from "./repartition-slice.js";
export type {
	FokosExecuteLocalRequest,
	FokosImportRecord,
	FokosImportState,
	FokosInitRequest,
	FokosMigrationAckRequest,
	FokosMigrationCursor,
	FokosMigrationPage,
	FokosMigrationPullRequest,
	FokosPartitionControlRpc,
	FokosPartitionRef,
	FokosPartitionStatusRpc,
	FokosPrepareDestroyRequest,
	FokosRepartitionPeer,
	FokosSlice,
	FokosStartImportRequest,
	FokosStatusCursor,
	FokosStatusEntry,
	FokosStatusPage,
	FokosStatusRequest,
	MigrationHost,
	RepartitionRouting,
} from "./repartition-types.js";
export { collectBatch } from "./batch-scan.js";
export type { CollectBatchOptions, CollectBatchResult } from "./batch-scan.js";
