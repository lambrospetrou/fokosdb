import { KeyCodec, type KeyBytes } from "./key-codec.js";

/**
 * A resume position of a range read inside one hash key. `hk` and `sk` are encoded keys. With
 * `inclusive: true`, the read starts at `sk`. Without it, the read starts after `sk` in the read direction.
 */
export type FokosRangeCursor = { hk: KeyBytes; sk: KeyBytes; inclusive?: boolean };

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
		if (cmp < 0 || (cmp === 0 && !interval.upper.inclusive)) {
			return false;
		}
	}
	if (interval.lower !== undefined && childEnd !== null) {
		if (KeyCodec.compare(interval.lower.value, childEnd) >= 0) {
			return false;
		}
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
 * Says if the cursor has already passed the whole child range in the scan direction.
 * True means the scan skips the child.
 *
 * asc: skip if childEnd <= cursor.sk. An unevaluated candidate is always strictly below its
 *   child end boundary, because the end is exclusive.
 * desc: skip if childStart > cursor.sk, or if childStart == cursor.sk and the cursor is
 *   exclusive. An exclusive descending cursor resumes strictly below its key; an inclusive one
 *   still owes that key, so a child that starts exactly there must be visited.
 */
export function isChildFullyBeforeCursor(
	childStart: KeyBytes,
	childEnd: KeyBytes | null,
	cursor: FokosRangeCursor,
	direction: "asc" | "desc",
): boolean {
	if (direction === "asc") {
		return childEnd !== null && KeyCodec.compare(cursor.sk, childEnd) >= 0;
	}
	const cmp = KeyCodec.compare(childStart, cursor.sk);
	return cmp > 0 || (cmp === 0 && !cursor.inclusive);
}

/**
 * Says if the cursor position falls in the child's [childStart, childEnd) range.
 * True means the child receives the cursor and resumes the scan. False means the child starts a new
 * scan from the bound of the interval.
 */
export function cursorFallsInChild(childStart: KeyBytes, childEnd: KeyBytes | null, cursor: FokosRangeCursor): boolean {
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
): FokosRangeCursor {
	if (direction === "asc") {
		return { hk: hashKey, sk: childEnd ?? KeyCodec.encodeOptional(undefined), inclusive: true };
	}
	return { hk: hashKey, sk: childStart, inclusive: false };
}
