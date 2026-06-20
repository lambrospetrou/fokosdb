import { KeyCodec, type KeyBytes } from "../partition-topology/key-codec.js";
import type { ScanCursor } from "../partition/partition-store.js";
import type { SortKeyCondition } from "../types.js";

/** Byte-space sort-key interval. Both ends are optional (absent = unbounded). */
export type SkInterval = {
	lower?: { value: KeyBytes; inclusive: boolean };
	upper?: { value: KeyBytes; inclusive: boolean };
};

/**
 * Check whether a child range [childStart, childEnd) intersects an SkInterval.
 * Child ownership is start-inclusive, end-exclusive: the child owns keys where
 * `childStart <= sk < childEnd` (null childEnd = unbounded above).
 */
export function rangeIntersects(childStart: KeyBytes, childEnd: KeyBytes | null, interval: SkInterval): boolean {
	if (interval.upper !== undefined) {
		const cmp = KeyCodec.compare(interval.upper.value, childStart);
		if (cmp < 0 || (cmp === 0 && !interval.upper.inclusive)) return false;
	}
	if (interval.lower !== undefined && childEnd !== null) {
		if (KeyCodec.compare(interval.lower.value, childEnd) >= 0) return false;
	}
	return true;
}

/**
 * Clip an SkInterval to a child's [childStart, childEnd) ownership range.
 * The lower bound is raised to childStart (inclusive) if the interval extends below it,
 * and the upper bound is lowered to childEnd (exclusive) if the interval extends above it.
 */
export function clipToChildRange(interval: SkInterval, childStart: KeyBytes | null, childEnd: KeyBytes | null): SkInterval {
	const csEff = childStart ?? KeyCodec.encodeOptional(undefined);

	let lower: SkInterval["lower"];
	if (interval.lower === undefined || KeyCodec.compare(interval.lower.value, csEff) < 0) {
		lower = { value: csEff, inclusive: true };
	} else {
		lower = interval.lower;
	}

	let upper: SkInterval["upper"];
	if (childEnd === null) {
		upper = interval.upper;
	} else if (interval.upper === undefined) {
		upper = { value: childEnd, inclusive: false };
	} else {
		const cmp = KeyCodec.compare(interval.upper.value, childEnd);
		upper = cmp < 0 || (cmp === 0 && !interval.upper.inclusive) ? interval.upper : { value: childEnd, inclusive: false };
	}

	return { lower, upper };
}

/**
 * Check whether a child range has been fully passed by the cursor in the given scan direction.
 * Returns true if the child should be skipped.
 *
 * asc: skip if childEnd <= cursor.sk (every key in the child is at or before the resume point).
 * desc: skip if childStart >= cursor.sk (desc cursors resume strictly below cursor.sk, so a
 *   child whose start is at or above the cursor holds nothing below it).
 */
export function isChildFullyBeforeCursor(
	childStart: KeyBytes,
	childEnd: KeyBytes | null,
	cursor: ScanCursor,
	direction: "asc" | "desc",
): boolean {
	if (direction === "asc") {
		return childEnd !== null && KeyCodec.compare(cursor.sk, childEnd) >= 0;
	}
	return KeyCodec.compare(childStart, cursor.sk) >= 0;
}

/**
 * Check whether a cursor position falls within a child's [childStart, childEnd) range.
 * If true, the child should receive the cursor for a resumed scan; otherwise the child
 * starts a fresh scan from the interval's bound.
 */
export function cursorFallsInChild(childStart: KeyBytes, childEnd: KeyBytes | null, cursor: ScanCursor): boolean {
	return KeyCodec.compare(cursor.sk, childStart) >= 0 && (childEnd === null || KeyCodec.compare(cursor.sk, childEnd) < 0);
}

/**
 * Build a boundary continuation cursor when the partition-visit cap is reached.
 *
 * asc: resume inclusively at childEnd (== next child's start boundary, which owns that key
 *   and hasn't been scanned yet).
 * desc: resume exclusively below childStart (that boundary key belongs to this already-visited
 *   child, so exclude it).
 */
export function makeBoundaryCursor(
	hashKey: KeyBytes,
	childStart: KeyBytes,
	childEnd: KeyBytes | null,
	direction: "asc" | "desc",
): ScanCursor {
	if (direction === "asc") {
		return { hk: hashKey, sk: childEnd ?? KeyCodec.encodeOptional(undefined), inclusive: true };
	}
	return { hk: hashKey, sk: childStart, inclusive: false };
}

/**
 * Normalize a public `SortKeyCondition` into a byte-space `SkInterval`, or `null` for an
 * unsatisfiable (empty) interval. The `encode` callback handles key encoding + size validation
 * (the interval module is agnostic to key-length policy).
 */
export function normalizeSkInterval(sort: SortKeyCondition | undefined, encode: (k: string | Uint8Array) => KeyBytes): SkInterval | null {
	if (sort === undefined) {
		return { lower: { value: KeyCodec.encodeOptional(undefined), inclusive: true } };
	}
	switch (sort.op) {
		case "eq": {
			const v = encode(sort.value);
			return { lower: { value: v, inclusive: true }, upper: { value: v, inclusive: true } };
		}
		case "gt":
			return { lower: { value: encode(sort.value), inclusive: false } };
		case "gte":
			return { lower: { value: encode(sort.value), inclusive: true } };
		case "lt":
			return { upper: { value: encode(sort.value), inclusive: false } };
		case "lte":
			return { upper: { value: encode(sort.value), inclusive: true } };
		case "between": {
			const lo = encode(sort.lower);
			const hi = encode(sort.upper);
			if (KeyCodec.compare(lo, hi) > 0) return null;
			return { lower: { value: lo, inclusive: true }, upper: { value: hi, inclusive: true } };
		}
		case "begins_with": {
			const prefix = sort.prefix;
			if ((typeof prefix === "string" && prefix.length === 0) || (prefix instanceof Uint8Array && prefix.byteLength === 0)) {
				return { lower: { value: KeyCodec.encodeOptional(undefined), inclusive: true } };
			}
			const p = encode(prefix);
			const succ = KeyCodec.successor(p);
			return { lower: { value: p, inclusive: true }, upper: succ ? { value: succ, inclusive: false } : undefined };
		}
		case "range": {
			const lo = sort.lower ? { value: encode(sort.lower.value), inclusive: sort.lower.inclusive } : undefined;
			const up = sort.upper ? { value: encode(sort.upper.value), inclusive: sort.upper.inclusive } : undefined;
			if (lo && up) {
				const cmp = KeyCodec.compare(lo.value, up.value);
				if (cmp > 0 || (cmp === 0 && (!lo.inclusive || !up.inclusive))) return null;
			}
			return { lower: lo, upper: up };
		}
	}
}
