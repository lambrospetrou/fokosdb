import { hashChildIndex } from "../../hash-primitives.js";
import { KeyCodec, type KeyBytes } from "../../partition-topology/key-codec.js";
import type { ScanCursor } from "../partition-store.js";
import { clipToChildRange, cursorFallsInChild, rangeIntersects, type SkInterval } from "../../query/sk-interval.js";
import { FokosRoutingError, ROUTING_CODES } from "../../errors.js";

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

/** Never transient: the key reached a source that does not serve it for this caller. */
function misrouted(operationName: string, reason: string): FokosRoutingError {
	return new FokosRoutingError(ROUTING_CODES.partition_misrouted, {
		message: "read-through key is outside the caller's slice",
		attributes: { operation: operationName, reason },
	});
}

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
	if (!hashKeyInSlice(slice, hashKey, hashSplitN)) return false;
	if (slice.kind !== "range") return true;
	return KeyCodec.compare(sortKey, sliceStart(slice.start)) >= 0 && (slice.end === null || KeyCodec.compare(sortKey, slice.end) < 0);
}

/** Says whether one hash key belongs to the slice, ignoring the sort-key axis. */
export function sliceIncludesHashKey(slice: FokosSlice, hashKey: KeyBytes, hashSplitN: number): boolean {
	return hashKeyInSlice(slice, hashKey, hashSplitN);
}

/** Throws `partition_misrouted` when the point is outside the slice. */
export function assertPointInSlice(
	slice: FokosSlice,
	hashKey: KeyBytes,
	sortKey: KeyBytes,
	hashSplitN: number,
	operationName: string,
): void {
	if (!hashKeyInSlice(slice, hashKey, hashSplitN)) throw misrouted(operationName, "hash key outside slice");
	if (slice.kind !== "range") return;
	const inInterval =
		KeyCodec.compare(sortKey, sliceStart(slice.start)) >= 0 && (slice.end === null || KeyCodec.compare(sortKey, slice.end) < 0);
	if (!inInterval) throw misrouted(operationName, "sort key outside slice interval");
}

/**
 * Clips a query interval to the slice and returns the interval the source may actually scan.
 *
 * A caller can legitimately ask for more than it owns — a range child inherits the client's whole
 * interval and relies on its router to clip — so an overlapping interval is narrowed rather than
 * rejected. An interval that shares nothing with the slice, or a resume cursor that sits outside it,
 * is a routing defect and throws.
 */
export function clipQueryToSlice(
	slice: FokosSlice,
	req: { hashKey: KeyBytes; interval: SkInterval; cursor: ScanCursor | null },
	hashSplitN: number,
	operationName: string,
): SkInterval {
	if (!hashKeyInSlice(slice, req.hashKey, hashSplitN)) throw misrouted(operationName, "hash key outside slice");
	// A hash-child or promoted-key slice owns the whole sort-key axis of its keys, so there is
	// nothing to clip and every cursor under those keys is in range.
	if (slice.kind !== "range") return req.interval;

	const start = sliceStart(slice.start);
	if (!rangeIntersects(start, slice.end, req.interval)) throw misrouted(operationName, "interval disjoint from slice");
	if (req.cursor && !cursorFallsInChild(start, slice.end, req.cursor)) throw misrouted(operationName, "cursor outside slice interval");
	return clipToChildRange(req.interval, slice.start, slice.end);
}
