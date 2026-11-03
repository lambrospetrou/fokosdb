/**
 * The interval frontier of one range request: a disjoint, ordered cover of the requested sort-key
 * interval with the deepest partition this partition knows for each segment.
 *
 * The planner is pure. The runtime gives it the base cover its durable topology defines, the learned
 * descendant boundaries of the range tree, and the request; it returns the visits. A partial cache
 * cannot create a gap, because every segment that no learned slice covers keeps its base target.
 *
 * A range tree orders its rows by the composite key `(hk, sk)`, so the boundaries of its partitions
 * are composite keys. A request reads one hash key. The planner projects each boundary onto the
 * sort keys of that hash key (`projectOnHashKey`), and plans on the sort-key axis.
 */
import { KeyCodec, type FokosCompositeKey, type KeyBytes } from "./key-codec.js";
import type { FokosPartitionRef } from "./route-context.js";
import type { FokosRangeVisit } from "./runtime-types.js";
import type { LearnedRangeSlice } from "./sharding-store.js";
import { rangeIntersects, type SkInterval } from "./sk-interval.js";

/** One partition of the durable cover: a direct child of a range router, a range root, or this partition. */
export type FrontierBase = {
	target: FokosPartitionRef | "local";
	start: FokosCompositeKey | null;
	end: FokosCompositeKey | null;
	speculative: boolean;
};

/** One planned visit with the facts the runtime needs when the visit fails: its base, and the learned slice it used. */
export type PlannedVisit = {
	visit: FokosRangeVisit;
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

function skStartCmp(a: KeyBytes | null, b: KeyBytes | null): number {
	if (a === null || b === null) {
		return a === b ? 0 : a === null ? -1 : 1;
	}
	return KeyCodec.compare(a, b);
}

function skEndCmp(a: KeyBytes | null, b: KeyBytes | null): number {
	if (a === null || b === null) {
		return a === b ? 0 : a === null ? 1 : -1;
	}
	return KeyCodec.compare(a, b);
}

/** True when `[start, end)` is inside `[outerStart, outerEnd)`. */
function contains(outer: SkSegment, inner: SkSegment): boolean {
	return skStartCmp(outer.start, inner.start) <= 0 && skEndCmp(outer.end, inner.end) >= 0;
}

/** True when `inner` is inside `outer` and narrower than it. */
function isStrictSubSegment(inner: SkSegment, outer: SkSegment): boolean {
	const startRel = skStartCmp(inner.start, outer.start);
	const endRel = skEndCmp(inner.end, outer.end);
	return startRel >= 0 && endRel <= 0 && (startRel > 0 || endRel < 0);
}

const NO_KEY = KeyCodec.encodeOptional(undefined);

/**
 * Plans the visits of one request for the sort keys of `hashKey`.
 *
 * 1. Keep the base partitions that intersect the requested interval.
 * 2. Overlay the learned slices that are strict sub-slices of a base partition: split its interval at
 *    every learned boundary inside it.
 * 3. For each segment, select the deepest learned slice that contains it, or keep the base target.
 * 4. Merge adjacent segments with one target, and order the visits by `descending`.
 *
 * `bases` must be disjoint and in ascending boundary order. `refOf` builds the reference of a learned
 * slice, because only the runtime holds the route context that names a range partition. Each visit
 * holds a sort-key segment of `hashKey`.
 */
export function planRangeFrontier(
	hashKey: KeyBytes,
	bases: readonly FrontierBase[],
	learned: readonly LearnedRangeSlice[],
	interval: SkInterval,
	descending: boolean,
	refOf: (slice: LearnedRangeSlice) => FokosPartitionRef,
): PlannedVisit[] {
	const learnedSegments: Array<{ slice: LearnedRangeSlice; segment: SkSegment }> = [];
	for (const slice of learned) {
		const segment = projectOnHashKey(hashKey, slice.startBoundary, slice.endBoundary);
		if (segment !== null) {
			learnedSegments.push({ slice, segment });
		}
	}
	const visits: PlannedVisit[] = [];
	for (const base of bases) {
		const baseSegment = projectOnHashKey(hashKey, base.start, base.end);
		if (baseSegment === null || !rangeIntersects(baseSegment.start ?? NO_KEY, baseSegment.end, interval)) {
			continue;
		}
		const inside = learnedSegments.filter(
			({ slice, segment }) => isStrictSubSlice(slice, base.start, base.end) && isStrictSubSegment(segment, baseSegment),
		);
		if (inside.length === 0) {
			visits.push({
				visit: { target: base.target, start: baseSegment.start, end: baseSegment.end, speculative: base.speculative },
				base,
				learned: null,
			});
			continue;
		}

		// Every learned boundary strictly inside the base interval, once, in order.
		const points: KeyBytes[] = [];
		for (const { segment } of inside) {
			for (const point of [segment.start, segment.end]) {
				if (point === null || skStartCmp(point, baseSegment.start) <= 0 || skEndCmp(point, baseSegment.end) >= 0) {
					continue;
				}
				if (!points.some((p) => KeyCodec.compare(p, point) === 0)) {
					points.push(point);
				}
			}
		}
		points.sort(KeyCodec.compare);
		const bounds: (KeyBytes | null)[] = [baseSegment.start, ...points, baseSegment.end];

		let previous: PlannedVisit | null = null;
		for (let i = 0; i + 1 < bounds.length; i++) {
			const piece: SkSegment = { start: bounds[i], end: bounds[i + 1] };
			if (!rangeIntersects(piece.start ?? NO_KEY, piece.end, interval)) {
				previous = null;
				continue;
			}
			let deepest: LearnedRangeSlice | null = null;
			for (const { slice, segment } of inside) {
				if (contains(segment, piece) && (deepest === null || slice.depth > deepest.depth)) {
					deepest = slice;
				}
			}
			const target = deepest ? refOf(deepest) : base.target;
			if (previous && sameTarget(previous.visit.target, target) && skEndCmp(previous.visit.end, piece.start) === 0) {
				previous.visit.end = piece.end;
				continue;
			}
			previous = { visit: { target, start: piece.start, end: piece.end, speculative: base.speculative }, base, learned: deepest };
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
