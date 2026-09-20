// Primitive types shared across the partition-topology modules.
// This file must not import from those modules, because that makes a circular dependency.

import type { KeyBytes } from "./key-codec.js";
import type { PartitionInfo } from "../shared/types.js";

/**
 * INTERNAL ONLY - never reaches a public response.
 * The boundaries of a range partition's ancestors, used to seed the routing cache that lets a later
 * request skip the router chain. Boundaries stay in their encoded form (KeyBytes) so they can be
 * compared and stored as bytes.
 */
export type RangeAncestorInfo = {
	depth: number;
	startBoundary: KeyBytes;
	endBoundary: KeyBytes;
};

/**
 * INTERNAL ONLY - the partition-to-partition form of PartitionInfo.
 *
 * `_internal` is routing state, not an observability field: each response carries the serving leaf's
 * ancestor boundaries up through every router so `recordForwardResult` can cache them. It is declared
 * only on the RPC types, and `db.ts` drops it at the public boundary.
 */
export type PartitionInfoInternal = PartitionInfo & {
	/**
	 * Bounded set of this range partition's ancestor boundaries (excludes root) including self (last).
	 * Always empty for hash partitions.
	 */
	_internal: { rangeAncestors: RangeAncestorInfo[] };
};

// PartitionNodeId is an opaque identifier for a partition node in the topology.
// It is only used within the topology logic and should not be interpreted by external code.
export type PartitionNodeId = string;

export type SplitType = "hash" | "range";
export type SplitStatus = "split_queued" | "split_started" | "split_completed";
