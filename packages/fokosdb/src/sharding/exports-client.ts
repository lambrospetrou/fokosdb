/**
 * The export list of `fokosdb/sharding/client`. Both sharding entries re-export this module, so the
 * list exists in one place and the bundler does not emit an empty chunk for the client entry.
 */

// ─── The client ───────────────────────────────────────────────────────────────

export { FokosShardingClient } from "./client.js";
export type {
	FokosCallCost,
	FokosCallOptions,
	FokosCallResult,
	FokosResolvedGroup,
	FokosRetryPolicy,
	FokosShardingClientOptions,
	FokosWalkNode,
} from "./client.js";
export { FokosRouter } from "./router.js";

// ─── Keys and hashing ─────────────────────────────────────────────────────────

export { KeyCodec } from "./key-codec.js";
export type { KeyBytes } from "./key-codec.js";
export { GOLDEN_RATIO, GOLDEN_RATIO_BIGINT, hash32, hash64, hashChildIndex, hashRootIndex } from "./hash-primitives.js";

// ─── Identity and context ─────────────────────────────────────────────────────

export { refOf, structurallyEqual, topologiesEqual, validateRangeConfig, validateTopology } from "./route-context.js";
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
	isHashPartition,
	isRangePartition,
	partitionIdentityFrom,
	resolveDescendantHashPartitionContext,
	resolveHashChildPartitionContexts,
	resolveRangePartitionContext,
} from "./partition-id.js";
export type { PartitionNodeId, RangeAncestorInfo, SplitStatus, SplitType } from "./types.js";

// ─── Envelopes, routing, and operations ───────────────────────────────────────

export type {
	FokosEnvelope,
	FokosOperationSpec,
	FokosPublicRoute,
	FokosPublicRouting,
	FokosRangeInput,
	FokosRouteNode,
	FokosRouting,
	FokosServedRole,
} from "./runtime-types.js";
export type { FokosImportState, RouteKey } from "./repartition-types.js";
export type { RepartitionKind } from "./sharding-store.js";

// ─── Sort-key intervals ───────────────────────────────────────────────────────

export {
	clipToChildRange,
	cursorFallsInChild,
	isChildFullyBeforeCursor,
	makeBoundaryCursor,
	normalizeSkInterval,
	rangeIntersects,
} from "./sk-interval.js";
export type { SkInterval } from "./sk-interval.js";

// ─── Errors ───────────────────────────────────────────────────────────────────

export {
	FOKOS_SHARDING_CODE_TABLES,
	SHARDING_INTERNAL_CODES,
	SHARDING_ROUTING_CODES,
	SHARDING_UNAVAILABLE_CODES,
	SHARDING_VALIDATION_CODES,
} from "./errors.js";
export type { FokosShardingError } from "./errors.js";
export {
	CORE_INTERNAL_CODES,
	FokosError,
	FokosInternalError,
	FokosRoutingError,
	FokosUnavailableError,
	FokosValidationError,
	isRuntimeRetryableError,
} from "../shared/errors.js";
export type { FokosErrorOrigin, FokosErrorWire } from "../shared/errors.js";
