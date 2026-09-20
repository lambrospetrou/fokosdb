/**
 * The interval frontier of one range request: a disjoint, ordered cover of the requested sort-key
 * interval with the deepest partition this partition knows for each segment.
 *
 * The planner is pure. The runtime gives it the base cover its durable topology defines, the learned
 * descendant boundaries of the range tree, and the request; it returns the visits. A partial cache
 * cannot create a gap, because every segment that no learned slice covers keeps its base target.
 */
import { KeyCodec, type KeyBytes } from "./key-codec.js";
import type { FokosPartitionRef } from "./route-context.js";
import type { FokosRangeVisit } from "./runtime-types.js";
import type { LearnedRangeSlice } from "./sharding-store.js";
import { rangeIntersects, type SkInterval } from "./sk-interval.js";

/** One partition of the durable cover: a direct child of a range router, a range root, or this partition. */
export type FrontierBase = {
	target: FokosPartitionRef | "local";
	start: KeyBytes | null;
	end: KeyBytes | null;
	speculative: boolean;
};

/** One planned visit with the facts the runtime needs when the visit fails: its base, and the learned slice it used. */
export type PlannedVisit = {
	visit: FokosRangeVisit;
	base: FrontierBase;
	learned: LearnedRangeSlice | null;
};

/** Compares two start boundaries, where null is the unbounded lower edge. */
export function startCmp(a: KeyBytes | null, b: KeyBytes | null): number {
	if (a === null || b === null) return a === b ? 0 : a === null ? -1 : 1;
	return KeyCodec.compare(a, b);
}

/** Compares two end boundaries, where null is the unbounded upper edge. */
export function endCmp(a: KeyBytes | null, b: KeyBytes | null): number {
	if (a === null || b === null) return a === b ? 0 : a === null ? 1 : -1;
	return KeyCodec.compare(a, b);
}

/** True when `[start, end)` is inside `[outerStart, outerEnd)`. */
function contains(outerStart: KeyBytes | null, outerEnd: KeyBytes | null, start: KeyBytes | null, end: KeyBytes | null): boolean {
	return startCmp(outerStart, start) <= 0 && endCmp(outerEnd, end) >= 0;
}

/** True when `slice` is inside the base interval and narrower than it: a deeper descendant, never an ancestor. */
export function isStrictSubSlice(slice: LearnedRangeSlice, baseStart: KeyBytes | null, baseEnd: KeyBytes | null): boolean {
	const startRel = startCmp(slice.startBoundary, baseStart);
	const endRel = endCmp(slice.endBoundary, baseEnd);
	return startRel >= 0 && endRel <= 0 && (startRel > 0 || endRel < 0);
}

const NO_KEY = KeyCodec.encodeOptional(undefined);

/**
 * Plans the visits of one request.
 *
 * 1. Keep the base partitions that intersect the requested interval.
 * 2. Overlay the learned slices that are strict sub-slices of a base partition: split its interval at
 *    every learned boundary inside it.
 * 3. For each segment, select the deepest learned slice that contains it, or keep the base target.
 * 4. Merge adjacent segments with one target, and order the visits by `descending`.
 *
 * `bases` must be disjoint and in ascending boundary order. `refOf` builds the reference of a learned
 * slice, because only the runtime holds the route context that names a range partition.
 */
export function planRangeFrontier(
	bases: readonly FrontierBase[],
	learned: readonly LearnedRangeSlice[],
	interval: SkInterval,
	descending: boolean,
	refOf: (start: KeyBytes | null, end: KeyBytes | null) => FokosPartitionRef,
): PlannedVisit[] {
	const visits: PlannedVisit[] = [];
	for (const base of bases) {
		if (!rangeIntersects(base.start ?? NO_KEY, base.end, interval)) continue;
		const inside = learned.filter((slice) => isStrictSubSlice(slice, base.start, base.end));
		if (inside.length === 0) {
			visits.push({ visit: { target: base.target, start: base.start, end: base.end, speculative: base.speculative }, base, learned: null });
			continue;
		}

		// Every learned boundary strictly inside the base interval, once, in order.
		const points: KeyBytes[] = [];
		for (const slice of inside) {
			for (const point of [slice.startBoundary, slice.endBoundary]) {
				if (point === null || startCmp(point, base.start) <= 0 || endCmp(point, base.end) >= 0) continue;
				if (!points.some((p) => KeyCodec.compare(p, point) === 0)) points.push(point);
			}
		}
		points.sort(KeyCodec.compare);
		const bounds: (KeyBytes | null)[] = [base.start, ...points, base.end];

		let previous: PlannedVisit | null = null;
		for (let i = 0; i + 1 < bounds.length; i++) {
			const start = bounds[i];
			const end = bounds[i + 1];
			if (!rangeIntersects(start ?? NO_KEY, end, interval)) {
				previous = null;
				continue;
			}
			let deepest: LearnedRangeSlice | null = null;
			for (const slice of inside) {
				if (contains(slice.startBoundary, slice.endBoundary, start, end) && (deepest === null || slice.depth > deepest.depth))
					deepest = slice;
			}
			const target = deepest ? refOf(deepest.startBoundary, deepest.endBoundary) : base.target;
			if (previous && sameTarget(previous.visit.target, target) && endCmp(previous.visit.end, start) === 0) {
				previous.visit.end = end;
				continue;
			}
			previous = { visit: { target, start, end, speculative: base.speculative }, base, learned: deepest };
			visits.push(previous);
		}
	}
	return descending ? visits.reverse() : visits;
}

function sameTarget(a: FokosPartitionRef | "local", b: FokosPartitionRef | "local"): boolean {
	if (a === "local" || b === "local") return a === b;
	return a.partitionId === b.partitionId;
}
