import { compareRoutingHashes, routingHashOf, RoutingHashDigitCursor, type RoutingHash } from "./hash-primitives.js";
import { KeyCodec, type FokosCompositeKey, type KeyBytes } from "./key-codec.js";
import type { FokosTopology } from "./route-context.js";
import type { FokosStorageInterval } from "./runtime-types.js";
import invariant from "../shared/invariant.js";

/**
 * The part of a source partition's keyspace that one repartition target owns.
 *
 * A target reads through its source until its own copy is complete, and the source answers only for
 * the slice it handed that target. Without the slice the source would serve any key the target
 * asked for, including keys a sibling target owns and keys the source has already promoted away.
 *
 * - `hash_child`: the keys whose routingHash has the digit `childIndex` at `depth`.
 * - `range`: the keys of one routingHash in the immutable composite interval `[start, end)`
 *   of `(hk, sk)` (null = unbounded edge).
 * - `promoted_routing_hash`: every key of one routingHash.
 */
export type FokosSlice =
	| { kind: "hash_child"; childIndex: number; depth: number }
	| { kind: "range"; routingHash: RoutingHash; start: FokosCompositeKey | null; end: FokosCompositeKey | null }
	| { kind: "promoted_routing_hash"; routingHash: RoutingHash };

/** The topology fields that the digits of a routingHash need. */
export type SliceTopology = Pick<FokosTopology, "shardGroup" | "rootTreesN" | "hashSplitN">;

/** True when a routingHash falls in the slice. A range slice also checks its composite interval. */
export function sliceIncludesRoutingHash(slice: FokosSlice, routingHash: RoutingHash, topology: SliceTopology): boolean {
	switch (slice.kind) {
		case "hash_child": {
			// A child at depth D owns the keys of its parent whose digit at level D is its index.
			const cursor = new RoutingHashDigitCursor(routingHash, topology.rootTreesN);
			let digit = -1;
			for (let d = 0; d < slice.depth; d++) {
				digit = cursor.next(topology.hashSplitN);
			}
			return digit === slice.childIndex;
		}
		case "range":
		case "promoted_routing_hash":
			return compareRoutingHashes(routingHash, slice.routingHash) === 0;
	}
}

/** True when `key` is inside the composite interval `[start, end)`. A null edge is unbounded. */
export function compositeInInterval(key: FokosCompositeKey, start: FokosCompositeKey | null, end: FokosCompositeKey | null): boolean {
	return (start === null || KeyCodec.compareComposite(key, start) >= 0) && (end === null || KeyCodec.compareComposite(key, end) < 0);
}

/**
 * Says whether one stored item belongs to the slice. Migration filters its source rows with it, so a
 * target receives exactly the rows it owns and nothing a sibling owns.
 */
export function sliceIncludesItem(slice: FokosSlice, hashKey: KeyBytes, sortKey: KeyBytes, topology: SliceTopology): boolean {
	if (!sliceIncludesRoutingHash(slice, routingHashOf(hashKey, topology.shardGroup), topology)) {
		return false;
	}
	return slice.kind !== "range" || compositeInInterval({ hk: hashKey, sk: sortKey }, slice.start, slice.end);
}

/** Says whether one hash key belongs to the slice, ignoring the sort-key axis. */
export function sliceIncludesHashKey(slice: FokosSlice, hashKey: KeyBytes, topology: SliceTopology): boolean {
	return sliceIncludesRoutingHash(slice, routingHashOf(hashKey, topology.shardGroup), topology);
}

/** The one routingHash of a promotion plan: its single target carries a `promoted_routing_hash` slice. */
export function promotedRoutingHashOf(plan: { targets: ReadonlyArray<{ slice: FokosSlice }> }): RoutingHash {
	const slice = plan.targets[0]?.slice;
	invariant(slice?.kind === "promoted_routing_hash", "fokos/repartition: a promotion plan carries one promoted_routing_hash slice");
	return slice.routingHash;
}

/** The storage interval of a hash node interval `[start, end)`. A null `end` has no upper bound. */
export function hashStorageInterval(start: RoutingHash, end: RoutingHash | null): FokosStorageInterval {
	return { start: { prefix: start }, end: end === null ? null : { prefix: end } };
}

/** The next routingHash in numeric order, or null after the last one. */
function nextRoutingHash(routingHash: RoutingHash): RoutingHash | null {
	const next = routingHash.slice();
	for (let i = next.length - 1; i >= 0; i--) {
		if (next[i] < 0xff) {
			next[i] += 1;
			return next as RoutingHash;
		}
		next[i] = 0;
	}
	return null;
}

/**
 * The storage interval of the composite interval `[start, end)` of one routingHash. An unbounded
 * start is the first row of the routingHash. An unbounded end is the start of the next
 * routingHash, and only the last routingHash has no upper bound.
 */
export function rangeStorageInterval(
	routingHash: RoutingHash,
	start: FokosCompositeKey | null,
	end: FokosCompositeKey | null,
): FokosStorageInterval {
	const next = end === null ? nextRoutingHash(routingHash) : null;
	return {
		start: start === null ? { prefix: routingHash } : { prefix: routingHash, hk: start.hk, sk: start.sk },
		end: end !== null ? { prefix: routingHash, hk: end.hk, sk: end.sk } : next === null ? null : { prefix: next },
	};
}
