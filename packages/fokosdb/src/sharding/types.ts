// Primitive types shared across the sharding modules.
// This file must not import from those modules, because that makes a circular dependency.

import type { FokosCompositeKey } from "./key-codec.js";

/**
 * INTERNAL ONLY - never reaches a public response.
 * The boundaries of a range partition's ancestors, used to seed the routing cache that lets a later
 * request skip the router chain. A boundary is a composite key in its encoded form. An unbounded
 * edge has the empty hash key, which `KeyCodec` never makes for a real key.
 */
export type RangeAncestorInfo = {
	depth: number;
	startBoundary: FokosCompositeKey;
	endBoundary: FokosCompositeKey;
};

// PartitionNodeId is an opaque identifier for a partition node in the topology.
// It is only used within the topology logic and should not be interpreted by external code.
export type PartitionNodeId = string;

export type SplitType = "hash" | "range";
export type SplitStatus = "split_queued" | "split_started" | "split_completed";
