import { hashChildIndex } from "./hash-primitives.js";
import { KeyCodec, type KeyBytes } from "./key-codec.js";

/**
 * The part of a source partition's keyspace that one repartition target owns.
 *
 * A target reads through its source until its own copy is complete, and the source answers only for
 * the slice it handed that target. Without the slice the source would serve any key the target
 * asked for, including keys a sibling target owns and keys the source has already promoted away.
 *
 * - `hash_child`: the keys that hash to `childIndex` at `depth`, over the whole sort-key axis.
 * - `range`: one hash key and one immutable `[start, end)` sort-key interval (null = unbounded edge).
 * - `promoted_key`: one hash key over the whole sort-key axis.
 */
export type FokosSlice =
	| { kind: "hash_child"; childIndex: number; depth: number }
	| { kind: "range"; hashKey: KeyBytes; start: KeyBytes | null; end: KeyBytes | null }
	| { kind: "promoted_key"; hashKey: KeyBytes };

/** The lower edge of a range slice as a comparable key; a null start is the unbounded edge. */
function sliceStart(start: KeyBytes | null): KeyBytes {
	return start ?? KeyCodec.encodeOptional(undefined);
}

/**
 * Says whether `hashKey` falls in the slice. Sort keys never change the answer for a hash-child or a
 * promoted-key slice, and a range slice checks its interval separately.
 */
function hashKeyInSlice(slice: FokosSlice, hashKey: KeyBytes, hashSplitN: number): boolean {
	switch (slice.kind) {
		case "hash_child":
			// A child at depth D owns the keys its parent (at D-1) hashes into its own bucket.
			return hashChildIndex(hashKey, slice.depth - 1, hashSplitN) === slice.childIndex;
		case "range":
		case "promoted_key":
			return KeyCodec.compare(hashKey, slice.hashKey) === 0;
	}
}

/**
 * Says whether one stored item belongs to the slice. Migration filters its source rows with it, so a
 * target receives exactly the rows it owns and nothing a sibling owns.
 */
export function sliceIncludesItem(slice: FokosSlice, hashKey: KeyBytes, sortKey: KeyBytes, hashSplitN: number): boolean {
	if (!hashKeyInSlice(slice, hashKey, hashSplitN)) {
		return false;
	}
	if (slice.kind !== "range") {
		return true;
	}
	return KeyCodec.compare(sortKey, sliceStart(slice.start)) >= 0 && (slice.end === null || KeyCodec.compare(sortKey, slice.end) < 0);
}

/** Says whether one hash key belongs to the slice, ignoring the sort-key axis. */
export function sliceIncludesHashKey(slice: FokosSlice, hashKey: KeyBytes, hashSplitN: number): boolean {
	return hashKeyInSlice(slice, hashKey, hashSplitN);
}
