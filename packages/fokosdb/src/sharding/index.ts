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
	isHashPartition,
	isRangePartition,
	refOf,
	structurallyEqual,
	topologiesEqual,
	validateRangeConfig,
	validateTopology,
} from "./route-context.js";
export type {
	FokosPartitionIdentity,
	FokosPartitionRef,
	FokosRangeConfig,
	FokosRouteContext,
	FokosStoredPolicy,
	FokosTopology,
} from "./route-context.js";
export {
	PartitionIdHelper,
	RANGE_MAX,
	RANGE_MIN,
	identityDepth,
	partitionIdentityFrom,
	resolveDescendantHashPartitionContext,
	resolveHashChildPartitionContexts,
	resolveRangePartitionContext,
} from "./partition-id.js";
export type { PartitionNodeId, RangeAncestorInfo, SplitStatus, SplitType } from "./types.js";

// ─── The runtime ──────────────────────────────────────────────────────────────

export { FokosShardingRuntime } from "./runtime.js";
export type { FokosRuntimeConstructorOptions } from "./runtime.js";
export type {
	FokosChild,
	FokosEnvelope,
	FokosGroupPart,
	FokosJob,
	FokosLifecycle,
	FokosLocalCall,
	FokosOperation,
	FokosOperationBase,
	FokosOperationSpec,
	FokosOperations,
	FokosOwner,
	FokosPublicRoute,
	FokosPublicRouting,
	FokosRangeInput,
	FokosRangeVisit,
	FokosRepartitionPlan,
	FokosRequestPromotionResult,
	FokosRouteNode,
	FokosRouting,
	FokosServedRole,
	FokosRuntimeConfigOverrides,
	FokosRuntimeOptions,
	FokosShardingHooks,
	FokosSignals,
} from "./runtime-types.js";
export { ROUTE_EVIDENCE_MAX_BYTES, RouteCollector, attachRouting, routedError, routeNodeBytes } from "./envelope.js";
export type { FokosRoutedError } from "./envelope.js";
export { planRangeFrontier } from "./range-frontier.js";
export type { FrontierBase, PlannedVisit } from "./range-frontier.js";
export { FokosScheduler } from "./scheduler.js";
export type { FokosSchedulerDeps } from "./scheduler.js";
export { FOKOS_SHARDING_CODE_TABLES, SHARDING_INTERNAL_CODES, SHARDING_ROUTING_CODES, SHARDING_UNAVAILABLE_CODES } from "./errors.js";
export type { FokosShardingError } from "./errors.js";

// ─── Routing and caches ───────────────────────────────────────────────────────

export { FokosRouter } from "./router.js";
export type { FokosWalkStub } from "./router.js";
export { HashTopology } from "./hash-topology.js";
export type { HashTopologySnapshot } from "./hash-topology.js";
export { PartialRangeTopology } from "./partial-range-topology.js";
export type { PartialRangeTopologySnapshot } from "./partial-range-topology.js";
export { selectRangeAncestors } from "./range-ancestors.js";
export {
	clipToChildRange,
	cursorFallsInChild,
	isChildFullyBeforeCursor,
	makeBoundaryCursor,
	normalizeSkInterval,
	rangeIntersects,
} from "./sk-interval.js";
export type { SkInterval } from "./sk-interval.js";

// ─── Sharding store ───────────────────────────────────────────────────────────

export { FOKOS_KV_KEYS, FokosShardingStore, RANGE_HIERARCHY_MAX_ROWS } from "./sharding-store.js";
export type {
	FokosShardingStoreOptions,
	LearnedRangeSlice,
	PromotedKeyCursor,
	RepartitionKind,
	RepartitionRow,
	RepartitionSlice,
	RepartitionState,
	RepartitionStatusCursor,
	RepartitionStatusRow,
	RepartitionTargetCounts,
	RepartitionTargetRow,
	TargetInitialization,
} from "./sharding-store.js";

// ─── Repartition flow ─────────────────────────────────────────────────────────

export {
	FOKOS_PAGE_BYTES,
	FOKOS_PAGE_ROWS,
	FOKOS_SCAN_ROWS,
	REPARTITION_RPC_CONCURRENCY,
	RepartitionSource,
	RepartitionTarget,
} from "./repartition-flow.js";
export type {
	RepartitionCommonDeps,
	RepartitionIdentity,
	RepartitionSourceDeps,
	RepartitionTargetDeps,
	StepOutcome,
} from "./repartition-flow.js";
export { sliceIncludesHashKey, sliceIncludesItem } from "./repartition-slice.js";
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
	FokosPrepareDestroyRequest,
	FokosRepartitionPeer,
	FokosRequestPromotionRequest,
	FokosShardingRpc,
	FokosSlice,
	FokosStartImportRequest,
	FokosStoredRepartitionPlan,
	FokosStatusCursor,
	FokosStatusEntry,
	FokosStatusPage,
	FokosStatusRequest,
	MigrationHost,
	RouteKey,
} from "./repartition-types.js";
export { collectBatch } from "./batch-scan.js";
export type { CollectBatchOptions, CollectBatchResult } from "./batch-scan.js";
