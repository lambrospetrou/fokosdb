/**
 * The interval frontier of one range request: a disjoint, ordered cover of the requested composite
 * interval with the deepest partition this partition knows for each segment.
 *
 * The planner is pure. The runtime gives it the base cover its durable topology defines, the learned
 * descendant boundaries of the range tree, and the request; it returns the visits. A partial cache
 * cannot create a gap, because every segment that no learned slice covers keeps its base target.
 *
 * A range tree orders its rows by the composite key `(hk, sk)`, and the boundaries of its partitions
 * are composite keys. The planner plans on that axis: the request is a half-open composite interval,
 * and a visit is a composite segment.
 */
import { KeyCodec, type FokosCompositeKey, type KeyBytes } from "./key-codec.js";
import type { FokosPartitionRef } from "./route-context.js";
import type { LearnedRangeSlice } from "./sharding-store.js";
import type { SkInterval } from "./sk-interval.js";

/** One partition of the durable cover: a direct child of a range router, a range root, or this partition. */
export type FrontierBase = {
	target: FokosPartitionRef | "local";
	start: FokosCompositeKey | null;
	end: FokosCompositeKey | null;
	speculative: boolean;
};

/** A half-open interval `[start, end)` of composite keys inside one routingHash. A null end has no upper bound. */
export type CompositeInterval = { start: FokosCompositeKey; end: FokosCompositeKey | null };

/** One segment of the cover: its target, and the bounds of the partition or learned slice that serves it. A null bound is open. */
export type FrontierSegment = {
	target: FokosPartitionRef | "local";
	start: FokosCompositeKey | null;
	end: FokosCompositeKey | null;
	speculative: boolean;
};

/** One planned visit with the facts the runtime needs when the visit fails: its base, and the learned slice it used. */
export type PlannedVisit = {
	segment: FrontierSegment;
	base: FrontierBase;
	learned: LearnedRangeSlice | null;
};

/** Compares two start boundaries, where null is the unbounded lower edge. */
export function startCmp(a: FokosCompositeKey | null, b: FokosCompositeKey | null): number {
	if (a === null || b === null) {
		return a === b ? 0 : a === null ? -1 : 1;
	}
	return KeyCodec.compareComposite(a, b);
}

/** Compares two end boundaries, where null is the unbounded upper edge. */
export function endCmp(a: FokosCompositeKey | null, b: FokosCompositeKey | null): number {
	if (a === null || b === null) {
		return a === b ? 0 : a === null ? 1 : -1;
	}
	return KeyCodec.compareComposite(a, b);
}

/** True when `slice` is inside the base interval and narrower than it: a deeper descendant, never an ancestor. */
export function isStrictSubSlice(
	slice: LearnedRangeSlice,
	baseStart: FokosCompositeKey | null,
	baseEnd: FokosCompositeKey | null,
): boolean {
	const startRel = startCmp(slice.startBoundary, baseStart);
	const endRel = endCmp(slice.endBoundary, baseEnd);
	return startRel >= 0 && endRel <= 0 && (startRel > 0 || endRel < 0);
}

/** A sort-key interval `[start, end)` of one hash key. A null edge is unbounded. */
export type SkSegment = { start: KeyBytes | null; end: KeyBytes | null };

/**
 * The sort keys of `hashKey` inside the composite interval `[start, end)`, or null when no sort key of
 * `hashKey` is inside it. An edge of another hash key becomes unbounded, and the empty sort key as a
 * start is the unbounded start.
 */
export function projectOnHashKey(hashKey: KeyBytes, start: FokosCompositeKey | null, end: FokosCompositeKey | null): SkSegment | null {
	let skStart: KeyBytes | null = null;
	if (start !== null) {
		const cmp = KeyCodec.compare(start.hk, hashKey);
		if (cmp > 0) {
			return null;
		}
		if (cmp === 0 && start.sk.length > 0) {
			skStart = start.sk;
		}
	}
	let skEnd: KeyBytes | null = null;
	if (end !== null) {
		const cmp = KeyCodec.compare(end.hk, hashKey);
		if (cmp < 0) {
			return null;
		}
		if (cmp === 0) {
			// No sort key is below the empty sort key.
			if (end.sk.length === 0 || (skStart !== null && KeyCodec.compare(skStart, end.sk) >= 0)) {
				return null;
			}
			skEnd = end.sk;
		}
	}
	return { start: skStart, end: skEnd };
}

/**
 * The composite interval of the sort keys of `hashKey` inside `interval`. An inclusive bound becomes
 * a half-open bound with `KeyCodec.after`: no key is between `k` and `after(k)`. An open lower bound
 * starts at the empty sort key, and an open upper bound ends before the next hash key, `after(hashKey)`.
 */
export function hashKeyInterval(hashKey: KeyBytes, interval: SkInterval): CompositeInterval {
	const { lower, upper } = interval;
	const startSk = lower === undefined ? NO_KEY : lower.inclusive ? lower.value : KeyCodec.after(lower.value);
	const end =
		upper === undefined
			? { hk: KeyCodec.after(hashKey), sk: NO_KEY }
			: { hk: hashKey, sk: upper.inclusive ? KeyCodec.after(upper.value) : upper.value };
	return { start: { hk: hashKey, sk: startSk }, end };
}

/** True when `[start, end)` and `interval` have a key in common. A null bound is open. */
export function segmentIntersects(start: FokosCompositeKey | null, end: FokosCompositeKey | null, interval: CompositeInterval): boolean {
	return (
		(end === null || KeyCodec.compareComposite(interval.start, end) < 0) &&
		(start === null || interval.end === null || KeyCodec.compareComposite(start, interval.end) < 0)
	);
}

/** True when `[start, end)` holds the whole `[innerStart, innerEnd)`. A null bound is open. */
function holds(
	start: FokosCompositeKey | null,
	end: FokosCompositeKey | null,
	innerStart: FokosCompositeKey | null,
	innerEnd: FokosCompositeKey | null,
): boolean {
	return startCmp(start, innerStart) <= 0 && endCmp(end, innerEnd) >= 0;
}

const NO_KEY = KeyCodec.encodeOptional(undefined);

/**
 * Plans the visits of one request for the composite interval `interval`.
 *
 * 1. Keep the base partitions that intersect the requested interval.
 * 2. Overlay the learned slices that are strict sub-slices of a base partition: split its interval at
 *    every learned boundary inside it.
 * 3. For each segment that intersects the interval, select the deepest learned slice that contains the
 *    whole segment, or keep the base target.
 * 4. Merge adjacent segments with one target, and order the visits by `descending`.
 *
 * `bases` must be disjoint and in ascending boundary order. `refOf` builds the reference of a learned
 * slice, because only the runtime holds the route context that names a range partition. A segment is
 * not clipped to the interval: it is the part of its target that the cover gives it.
 */
export function planRangeFrontier(
	bases: readonly FrontierBase[],
	learned: readonly LearnedRangeSlice[],
	interval: CompositeInterval,
	descending: boolean,
	refOf: (slice: LearnedRangeSlice) => FokosPartitionRef,
): PlannedVisit[] {
	const visits: PlannedVisit[] = [];
	// The learned slices inside each base. The bases are disjoint and in order, so a slice can only be
	// inside the last base that starts at or before it, and a binary search finds that base. A slice
	// outside the interval can serve no segment of it, so the planner drops it.
	const insideOf: LearnedRangeSlice[][] = bases.map(() => []);
	for (const slice of learned) {
		if (!segmentIntersects(slice.startBoundary, slice.endBoundary, interval)) {
			continue;
		}
		let lo = 0;
		let hi = bases.length - 1;
		let at = -1;
		while (lo <= hi) {
			const mid = (lo + hi) >> 1;
			if (startCmp(bases[mid].start, slice.startBoundary) <= 0) {
				at = mid;
				lo = mid + 1;
			} else {
				hi = mid - 1;
			}
		}
		if (at >= 0 && isStrictSubSlice(slice, bases[at].start, bases[at].end)) {
			insideOf[at].push(slice);
		}
	}
	for (const [b, base] of bases.entries()) {
		if (!segmentIntersects(base.start, base.end, interval)) {
			continue;
		}
		const inside = insideOf[b];
		if (inside.length === 0) {
			visits.push({
				segment: { target: base.target, start: base.start, end: base.end, speculative: base.speculative },
				base,
				learned: null,
			});
			continue;
		}

		// Every learned boundary strictly inside the base interval, once, in order.
		const points: FokosCompositeKey[] = [];
		for (const slice of inside) {
			for (const point of [slice.startBoundary, slice.endBoundary]) {
				if (point !== null && startCmp(point, base.start) > 0 && endCmp(point, base.end) < 0) {
					points.push(point);
				}
			}
		}
		points.sort(KeyCodec.compareComposite);
		const bounds: (FokosCompositeKey | null)[] = [base.start];
		for (const point of points) {
			const last = bounds[bounds.length - 1];
			if (last === null || KeyCodec.compareComposite(last, point) !== 0) {
				bounds.push(point);
			}
		}
		bounds.push(base.end);

		let previous: PlannedVisit | null = null;
		for (let i = 0; i + 1 < bounds.length; i++) {
			const start = bounds[i];
			const end = bounds[i + 1];
			if (!segmentIntersects(start, end, interval)) {
				// The pieces are in order, so after the first piece above the interval no piece touches it.
				if (start !== null && interval.end !== null && KeyCodec.compareComposite(start, interval.end) >= 0) {
					break;
				}
				previous = null;
				continue;
			}
			let deepest: LearnedRangeSlice | null = null;
			for (const slice of inside) {
				if ((deepest === null || slice.depth > deepest.depth) && holds(slice.startBoundary, slice.endBoundary, start, end)) {
					deepest = slice;
				}
			}
			const target = deepest ? refOf(deepest) : base.target;
			if (previous && sameTarget(previous.segment.target, target)) {
				// The pieces of one base touch, so a merge extends the previous segment to the end of this one.
				previous.segment.end = end;
				continue;
			}
			previous = { segment: { target, start, end, speculative: base.speculative }, base, learned: deepest };
			visits.push(previous);
		}
	}
	return descending ? visits.reverse() : visits;
}

function sameTarget(a: FokosPartitionRef | "local", b: FokosPartitionRef | "local"): boolean {
	if (a === "local" || b === "local") {
		return a === b;
	}
	return a.partitionId === b.partitionId;
}
