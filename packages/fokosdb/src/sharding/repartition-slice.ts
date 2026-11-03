import { compareHashPrefixBuckets, hashPrefixBucketOf, HashPrefixBucketDigitCursor, type HashPrefixBucket } from "./hash-primitives.js";
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
 * - `hash_child`: the keys whose hashPrefixBucket has the digit `childIndex` at `depth`.
 * - `range`: the keys of one hashPrefixBucket in the immutable composite interval `[start, end)`
 *   of `(hk, sk)` (null = unbounded edge).
 * - `promoted_hash_prefix_bucket`: every key of one hashPrefixBucket.
 */
export type FokosSlice =
	| { kind: "hash_child"; childIndex: number; depth: number }
	| { kind: "range"; hashPrefixBucket: HashPrefixBucket; start: FokosCompositeKey | null; end: FokosCompositeKey | null }
	| { kind: "promoted_hash_prefix_bucket"; hashPrefixBucket: HashPrefixBucket };

/** The topology fields that the digits of a hashPrefixBucket need. */
export type SliceTopology = Pick<FokosTopology, "shardGroup" | "rootTreesN" | "hashSplitN">;

/** True when a hashPrefixBucket falls in the slice. A range slice also checks its composite interval. */
export function sliceIncludesHashPrefixBucket(slice: FokosSlice, hashPrefixBucket: HashPrefixBucket, topology: SliceTopology): boolean {
	switch (slice.kind) {
		case "hash_child": {
			// A child at depth D owns the keys of its parent whose digit at level D is its index.
			const cursor = new HashPrefixBucketDigitCursor(hashPrefixBucket, topology.rootTreesN);
			let digit = -1;
			for (let d = 0; d < slice.depth; d++) {
				digit = cursor.next(topology.hashSplitN);
			}
			return digit === slice.childIndex;
		}
		case "range":
		case "promoted_hash_prefix_bucket":
			return compareHashPrefixBuckets(hashPrefixBucket, slice.hashPrefixBucket) === 0;
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
	if (!sliceIncludesHashPrefixBucket(slice, hashPrefixBucketOf(hashKey, topology.shardGroup), topology)) {
		return false;
	}
	return slice.kind !== "range" || compositeInInterval({ hk: hashKey, sk: sortKey }, slice.start, slice.end);
}

/** Says whether one hash key belongs to the slice, ignoring the sort-key axis. */
export function sliceIncludesHashKey(slice: FokosSlice, hashKey: KeyBytes, topology: SliceTopology): boolean {
	return sliceIncludesHashPrefixBucket(slice, hashPrefixBucketOf(hashKey, topology.shardGroup), topology);
}

/** The one hashPrefixBucket of a promotion plan: its single target carries a `promoted_hash_prefix_bucket` slice. */
export function promotedHashPrefixBucketOf(plan: { targets: ReadonlyArray<{ slice: FokosSlice }> }): HashPrefixBucket {
	const slice = plan.targets[0]?.slice;
	invariant(
		slice?.kind === "promoted_hash_prefix_bucket",
		"fokos/repartition: a promotion plan carries one promoted_hash_prefix_bucket slice",
	);
	return slice.hashPrefixBucket;
}

/** The storage interval of a hash node interval `[start, end)`. A null `end` has no upper bound. */
export function hashStorageInterval(start: HashPrefixBucket, end: HashPrefixBucket | null): FokosStorageInterval {
	return { start: { prefix: start }, end: end === null ? null : { prefix: end } };
}

/** The next hashPrefixBucket in numeric order, or null after the last one. */
function nextHashPrefixBucket(hashPrefixBucket: HashPrefixBucket): HashPrefixBucket | null {
	const next = hashPrefixBucket.slice();
	for (let i = next.length - 1; i >= 0; i--) {
		if (next[i] < 0xff) {
			next[i] += 1;
			return next as HashPrefixBucket;
		}
		next[i] = 0;
	}
	return null;
}

/**
 * The storage interval of the composite interval `[start, end)` of one hashPrefixBucket. An unbounded
 * start is the first row of the hashPrefixBucket. An unbounded end is the start of the next
 * hashPrefixBucket, and only the last hashPrefixBucket has no upper bound.
 */
export function rangeStorageInterval(
	hashPrefixBucket: HashPrefixBucket,
	start: FokosCompositeKey | null,
	end: FokosCompositeKey | null,
): FokosStorageInterval {
	const next = end === null ? nextHashPrefixBucket(hashPrefixBucket) : null;
	return {
		start: start === null ? { prefix: hashPrefixBucket } : { prefix: hashPrefixBucket, hk: start.hk, sk: start.sk },
		end: end !== null ? { prefix: hashPrefixBucket, hk: end.hk, sk: end.sk } : next === null ? null : { prefix: next },
	};
}
