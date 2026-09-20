// Primitive types shared across the sharding modules.
// This file must not import from those modules, because that makes a circular dependency.

import type { KeyBytes } from "./key-codec.js";

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

// PartitionNodeId is an opaque identifier for a partition node in the topology.
// It is only used within the topology logic and should not be interpreted by external code.
export type PartitionNodeId = string;

export type SplitType = "hash" | "range";
export type SplitStatus = "split_queued" | "split_started" | "split_completed";
