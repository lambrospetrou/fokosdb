/**
 * Sharding server entry point, `fokosdb/sharding/server`: what a host Durable Object needs. It holds
 * the runtime, the store, the scheduler, the repartition flow, and the host option and hook types. It
 * also re-exports `fokosdb/sharding/client`, so a host imports one entry.
 *
 * Nothing here reads FokosDB items, expressions, or transactions. The `check-client-bundle` plugin in
 * `tsdown.config.ts` fails the build when a module of this entry reaches one of those.
 */

export * from "./exports-client.js";

// ─── Bloom filter ─────────────────────────────────────────────────────────────

export { AddResult, BloomFilter } from "./bloom-filter.js";
export type { BloomFilterSnapshot } from "./bloom-filter.js";

// ─── The runtime ──────────────────────────────────────────────────────────────

export { FokosShardingRuntime } from "./runtime.js";
export { isDestroyAbortError } from "../shared/cf-utils.js";
export type { FokosRuntimeConstructorOptions } from "./runtime.js";
export type {
	FokosChild,
	FokosGroupPart,
	FokosJob,
	FokosLifecycle,
	FokosLocalCall,
	FokosOperation,
	FokosOperationBase,
	FokosOperations,
	FokosOwner,
	FokosRangeVisit,
	FokosRepartitionPlan,
	FokosRequestPromotionResult,
	FokosRuntimeOptions,
	FokosShardingHooks,
	FokosSignals,
} from "./runtime-types.js";
export { DEFAULT_RUNTIME_CONFIG, resolveRuntimeConfig } from "./runtime-config.js";
export type { FokosMigrationPageBudget, FokosRuntimeConfig, FokosRuntimeConfigOverrides } from "./runtime-config.js";
export { ROUTE_EVIDENCE_MAX_BYTES, RouteCollector, attachRouting, routedError, routeNodeBytes } from "./envelope.js";
export type { FokosRoutedError } from "./envelope.js";
export { planRangeFrontier } from "./range-frontier.js";
export type { FrontierBase, PlannedVisit } from "./range-frontier.js";
export { FokosScheduler } from "./scheduler.js";
export type { FokosSchedulerDeps } from "./scheduler.js";

// ─── Routing and caches ───────────────────────────────────────────────────────

export { HashTopology } from "./hash-topology.js";
export type { HashTopologySnapshot } from "./hash-topology.js";
export { PartialRangeTopology } from "./partial-range-topology.js";
export type { PartialRangeTopologySnapshot } from "./partial-range-topology.js";
export { selectRangeAncestors } from "./range-ancestors.js";

// ─── Sharding store ───────────────────────────────────────────────────────────

export { FOKOS_KV_KEYS, FokosShardingStore } from "./sharding-store.js";
export type {
	FokosShardingStoreOptions,
	LearnedRangeSlice,
	PromotedKeyCursor,
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

export { REPARTITION_RPC_CONCURRENCY, RepartitionSource, RepartitionTarget } from "./repartition-flow.js";
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
} from "./repartition-types.js";
export { collectBatch } from "./batch-scan.js";
export type { CollectBatchOptions, CollectBatchResult } from "./batch-scan.js";
