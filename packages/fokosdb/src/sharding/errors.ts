/**
 * The error codes that only partitions exchange: routing, identity, and the repartition protocol.
 *
 * They extend the categories of `shared/errors.ts` with `defineCodes`, as `shared/errors-operations.ts`
 * does for the FokosDB operations. A client never matches on one of these codes directly: the codes a
 * client handles (`partition_migrating`, `partition_over_size`) stay in `shared/errors.ts`.
 */
import { defineCodes, type FokosCodesOf, FokosInternalError, FokosRoutingError, FokosUnavailableError } from "../shared/errors.js";

export const SHARDING_ROUTING_CODES = defineCodes("FokosRoutingError", "internal", 500, {
	/** The key cannot belong to this partition or to the caller slice. A routing defect, not backpressure. */
	partition_misrouted: "6ddzyj",
	/** A non-root hash partition has no identity. A caller that used a cache invalidates that hint and falls back. */
	hash_partition_not_initialized: "3njh4c",
	/** A range partition has no identity. A speculative caller falls back to the range root. */
	range_partition_not_initialized: "6ue24c",
});

export const SHARDING_UNAVAILABLE_CODES = defineCodes("FokosUnavailableError", "service", 503, {
	/** The repartition source still owns the slice, so the target must ask again after cutover. */
	repartition_not_cut_over: "spf2v8",
});

export const SHARDING_INTERNAL_CODES = defineCodes("FokosInternalError", "internal", 500, {
	/** The route context disagrees with the stored identity. */
	partition_context_mismatch: "8hv63q",
	/** An `attempt_all` group had a failed remote group. */
	partition_fanout_failed: "f3aqhc",
	/** The named repartition does not exist on this partition. */
	repartition_unknown: "ns5maa",
	/** A read-through or migration caller is not a target of any repartition this partition owns. */
	repartition_target_unknown: "8hqw3n",
	/** A read-through caller asked the source for a slice whose rows the source already gave back. */
	repartition_slice_reclaimed: "7tjj5t",
	/** A synchronous `local` handler returned a thenable. */
	sharding_local_must_be_sync: "5zt4je",
	/** A descriptor is inconsistent, or `fokosExecuteLocal` named an unknown or non-`readOnly` operation. */
	sharding_operation_invalid: "fdqcab",
});

/** Every code table of this module. */
export const FOKOS_SHARDING_CODE_TABLES = [SHARDING_ROUTING_CODES, SHARDING_UNAVAILABLE_CODES, SHARDING_INTERNAL_CODES] as const;

/** Every error the sharding runtime raises with a code of its own. */
export type FokosShardingError =
	| FokosRoutingError<FokosCodesOf<typeof SHARDING_ROUTING_CODES>>
	| FokosUnavailableError<FokosCodesOf<typeof SHARDING_UNAVAILABLE_CODES>>
	| FokosInternalError<FokosCodesOf<typeof SHARDING_INTERNAL_CODES>>;
