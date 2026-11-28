import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { KeyCodec, type FokosCompositeKey, type KeyBytes } from "./key-codec.js";
import {
	hashKeyInterval,
	planRangeFrontier,
	projectOnHashKey,
	segmentIntersects,
	startCmp,
	endCmp,
	type CompositeInterval,
	type FrontierBase,
	type PlannedVisit,
} from "./range-frontier.js";
import type { FokosPartitionRef } from "./route-context.js";
import type { LearnedRangeSlice } from "./sharding-store.js";
import type { SkInterval } from "./sk-interval.js";

const kb = (s: string) => KeyCodec.encode(s);
const k = (s: string | null): KeyBytes | null => (s === null ? null : kb(s));
/** The one hash key of each test request and boundary. */
const HK = kb("h");
const ck = (s: string | null): FokosCompositeKey | null => (s === null ? null : { hk: HK, sk: kb(s) });
const keyLabel = (key: KeyBytes): string => {
	const decoded = KeyCodec.decode(key);
	return typeof decoded === "string" ? decoded : `b64:${decoded.toBase64({ alphabet: "base64url" })}`;
};

/** A readable reference for one interval, so a test compares names and not bytes. */
function refOf(start: KeyBytes | null, end: KeyBytes | null): FokosPartitionRef {
	const name = `r.${start === null ? "min" : keyLabel(start)}.${end === null ? "max" : keyLabel(end)}`;
	return { partitionId: name, doName: name };
}

function base(start: string | null, end: string | null, target: "local" | "ref" = "ref", speculative = false): FrontierBase {
	return { target: target === "local" ? "local" : refOf(k(start), k(end)), start: ck(start), end: ck(end), speculative };
}

function learned(start: string | null, end: string | null, depth: number): LearnedRangeSlice {
	return { startBoundary: ck(start), endBoundary: ck(end), depth };
}

const ALL: SkInterval = {};

/** `planRangeFrontier` for the hash key `HK`, with a reference from the sort keys of a learned slice. */
function plan(
	bases: readonly FrontierBase[],
	slices: readonly LearnedRangeSlice[],
	interval: SkInterval,
	descending: boolean,
	ref: (start: KeyBytes | null, end: KeyBytes | null) => FokosPartitionRef,
) {
	return planRangeFrontier(bases, slices, hashKeyInterval(HK, interval), descending, (slice) =>
		ref(slice.startBoundary?.sk ?? null, slice.endBoundary?.sk ?? null),
	).map((p) => ({ ...p, visit: onHashKey(HK, p) }));
}

/** The visit of `hashKey` that a planned segment gives: the target and the sort-key segment of the hash key. */
function onHashKey(hashKey: KeyBytes, planned: PlannedVisit) {
	const segment = projectOnHashKey(hashKey, planned.segment.start, planned.segment.end);
	expect(segment).not.toBeNull();
	return { target: planned.segment.target, start: segment!.start, end: segment!.end, speculative: planned.segment.speculative };
}

/** The visits as `[target, start, end]` names. */
function shape(visits: ReturnType<typeof plan>) {
	return visits.map(({ visit }) => [
		visit.target === "local" ? "local" : visit.target.doName,
		visit.start === null ? null : KeyCodec.decode(visit.start),
		visit.end === null ? null : KeyCodec.decode(visit.end),
	]);
}

describe("planRangeFrontier", () => {
	it("returns the base cover unchanged when nothing is learned", () => {
		const visits = plan([base(null, "m"), base("m", null)], [], ALL, false, refOf);
		expect(shape(visits)).toEqual([
			["r.min.m", null, "m"],
			["r.m.max", "m", null],
		]);
		expect(visits.every((v) => v.learned === null)).toBe(true);
	});

	it("drops the bases that the requested interval does not touch, and honours an inclusive upper bound on a start boundary", () => {
		const bases = [base(null, "m"), base("m", null)];
		expect(shape(plan(bases, [], { upper: { value: kb("m"), inclusive: false } }, false, refOf))).toEqual([["r.min.m", null, "m"]]);
		// `<= m` needs the child that starts at m: it owns the key m.
		expect(shape(plan(bases, [], { upper: { value: kb("m"), inclusive: true } }, false, refOf))).toHaveLength(2);
	});

	it("overlays learned descendants and keeps the base target for every gap", () => {
		// The root covers everything. Two learned leaves cover [c, f) and [f, k); the rest stays on the root.
		const visits = plan([base(null, null)], [learned("c", "f", 2), learned("f", "k", 2)], ALL, false, refOf);
		expect(shape(visits)).toEqual([
			["r.min.max", null, "c"],
			["r.c.f", "c", "f"],
			["r.f.k", "f", "k"],
			["r.min.max", "k", null],
		]);
		expect(visits[1].learned).toEqual(learned("c", "f", 2));
		expect(visits[0].learned).toBeNull();
	});

	it("selects the deepest learned slice that contains a segment", () => {
		const visits = plan([base(null, null)], [learned("c", "k", 1), learned("f", "k", 2)], ALL, false, refOf);
		expect(shape(visits)).toEqual([
			["r.min.max", null, "c"],
			["r.c.k", "c", "f"],
			["r.f.k", "f", "k"],
			["r.min.max", "k", null],
		]);
	});

	it("bypasses the root when the learned leaves cover the whole interval", () => {
		const visits = plan([base(null, null)], [learned(null, "m", 1), learned("m", null, 1)], ALL, false, refOf);
		expect(shape(visits)).toEqual([
			["r.min.m", null, "m"],
			["r.m.max", "m", null],
		]);
	});

	it("ignores a learned slice that is not a strict sub-slice of a base, such as an ancestor or the base itself", () => {
		const visits = plan([base("c", "k")], [learned(null, null, 0), learned("c", "k", 1)], ALL, false, refOf);
		expect(shape(visits)).toEqual([["r.c.k", "c", "k"]]);
	});

	it("keeps the visits disjoint, in order, and equal to the requested interval inside the cover", () => {
		const bases = [base(null, "m"), base("m", null)];
		const rows = [learned("c", "f", 2), learned("f", "m", 2), learned("m", "p", 2), learned("x", null, 2)];
		const visits = plan(
			bases,
			rows,
			{ lower: { value: kb("d"), inclusive: true }, upper: { value: kb("y"), inclusive: false } },
			false,
			refOf,
		);
		// Every segment touches the interval, and consecutive segments share their boundary.
		for (let i = 1; i < visits.length; i++) {
			expect(KeyCodec.compare(visits[i - 1].visit.end!, visits[i].visit.start!)).toBe(0);
		}
		expect(shape(visits)).toEqual([
			["r.c.f", "c", "f"],
			["r.f.m", "f", "m"],
			["r.m.p", "m", "p"],
			["r.m.max", "p", "x"],
			["r.x.max", "x", null],
		]);
	});

	it("reverses the order for a descending request", () => {
		const visits = plan([base(null, "m"), base("m", null)], [learned("c", "f", 2)], ALL, true, refOf);
		expect(shape(visits)).toEqual([
			["r.m.max", "m", null],
			["r.min.m", "f", "m"],
			["r.c.f", "c", "f"],
			["r.min.m", null, "c"],
		]);
	});

	it("marks every visit of a speculative base as speculative", () => {
		const visits = plan([base(null, null, "ref", true)], [learned("c", "f", 2)], ALL, false, refOf);
		expect(visits.map((v) => v.visit.speculative)).toEqual([true, true, true]);
		expect(visits.every((v) => v.base.speculative)).toBe(true);
	});

	// A point read fills the same learned table that this planner reads, and it teaches only the one
	// slice that holds its own key. The two cases below are that cross-feed on the LEFT edge, which is
	// the shape that matters: a slice with an unbounded start is the only one a byte-minimum key
	// matches, so a planner that entered by a single key would enter there and answer for it alone.
	// The reader is interval-based instead, and a slice narrower than the request can only ever
	// overlay its own segment.
	it("keeps the base cover beside a left-edge slice that a point read taught", () => {
		const visits = plan([base(null, null)], [learned(null, "c", 2)], ALL, false, refOf);
		expect(shape(visits)).toEqual([
			["r.min.c", null, "c"],
			["r.min.max", "c", null],
		]);
		// The learned slice serves its own segment and nothing above it.
		expect(visits[0].learned).toEqual(learned(null, "c", 2));
		expect(visits[1].learned).toBeNull();
	});

	it("picks the deepest left-edge slice that fully contains each segment, and covers the rest from the base", () => {
		// Two levels on the left edge, as a tree whose first leaf split again would teach them.
		const visits = plan([base(null, null)], [learned(null, "f", 1), learned(null, "c", 2)], ALL, false, refOf);
		expect(shape(visits)).toEqual([
			["r.min.c", null, "c"],
			["r.min.f", "c", "f"],
			["r.min.max", "f", null],
		]);
		// [c, f) is not inside the depth-2 slice, so the deeper slice must not claim it.
		expect(visits[1].learned).toEqual(learned(null, "f", 1));
	});

	it("plans one local visit for a leaf", () => {
		expect(shape(plan([base("c", "k", "local")], [], ALL, false, refOf))).toEqual([["local", "c", "k"]]);
	});
});

describe("planRangeFrontier with two hash keys in one routingHash", () => {
	const split: FokosCompositeKey = { hk: kb("b"), sk: KeyCodec.encodeOptional(undefined) };
	const bases: FrontierBase[] = [
		{ target: { partitionId: "left", doName: "left" }, start: null, end: split, speculative: false },
		{ target: { partitionId: "right", doName: "right" }, start: split, end: null, speculative: false },
	];
	const targets = (hashKey: string, interval: SkInterval) =>
		planRangeFrontier(bases, [], hashKeyInterval(kb(hashKey), interval), false, () => {
			throw new Error("no learned slice");
		})
			.map((p) => onHashKey(kb(hashKey), p))
			.map((visit) => [visit.target === "local" ? "local" : visit.target.doName, visit.start, visit.end]);

	it("sends each hash key only to the child of its own sort keys, over the whole sort-key axis", () => {
		expect(targets("a", ALL)).toEqual([["left", null, null]]);
		expect(targets("b", ALL)).toEqual([["right", null, null]]);
		expect(targets("b", { lower: { value: kb("x"), inclusive: true } })).toEqual([["right", null, null]]);
	});

	it("sends the request to a learned slice that holds every sort key of the hash key, also when the slice is narrower than its base", () => {
		// The slice [(g), (i)) holds the whole hash key h, but other hash keys of the routingHash are outside it.
		const root: FrontierBase = { target: { partitionId: "root", doName: "root" }, start: null, end: null, speculative: false };
		const slice: LearnedRangeSlice = {
			startBoundary: { hk: kb("g"), sk: KeyCodec.encodeOptional(undefined) },
			endBoundary: { hk: kb("i"), sk: KeyCodec.encodeOptional(undefined) },
			depth: 1,
		};
		const visits = planRangeFrontier([root], [slice], hashKeyInterval(kb("h"), ALL), false, () => ({ partitionId: "g-i", doName: "g-i" }));
		expect(visits.map((v) => v.segment.target)).toEqual([{ partitionId: "g-i", doName: "g-i" }]);
		expect(visits[0].learned).toBe(slice);
	});

	it("splits the sort keys of one hash key at a boundary inside it", () => {
		const inside: FrontierBase[] = [
			{ ...bases[0], end: { hk: kb("a"), sk: kb("m") } },
			{ ...bases[1], start: { hk: kb("a"), sk: kb("m") } },
		];
		const visits = planRangeFrontier(inside, [], hashKeyInterval(kb("a"), ALL), false, () => {
			throw new Error("no learned slice");
		}).map((p) => ({ visit: onHashKey(kb("a"), p) }));
		expect(
			visits.map(({ visit }) => [
				visit.start === null ? null : KeyCodec.decode(visit.start),
				visit.end === null ? null : KeyCodec.decode(visit.end),
			]),
		).toEqual([
			[null, "m"],
			["m", null],
		]);
		// The other hash key is fully above the boundary.
		expect(
			planRangeFrontier(inside, [], hashKeyInterval(kb("b"), ALL), false, () => ({ partitionId: "", doName: "" })).map(
				(v) => v.segment.target,
			),
		).toEqual([inside[1].target]);
	});
});

describe("planRangeFrontier against a brute-force cover", () => {
	const HASH_KEYS = ["a", "b", "c"];
	const SORT_KEYS = ["", "d", "e", "f", "g"];
	/** Every composite key of the small key space, in order. */
	const UNIVERSE: FokosCompositeKey[] = HASH_KEYS.flatMap((hk) =>
		SORT_KEYS.map((sk) => ({ hk: kb(hk), sk: sk === "" ? KeyCodec.encodeOptional(undefined) : kb(sk) })),
	).sort(KeyCodec.compareComposite);

	type Node = { start: FokosCompositeKey | null; end: FokosCompositeKey | null; depth: number; children: Node[] };

	/** A range tree: each node splits at most three times, at keys of the universe strictly inside it. */
	function tree(choices: number[]): Node {
		let next = 0;
		const pick = (n: number) => (n <= 0 ? 0 : choices[next++ % choices.length] % n);
		const grow = (node: Node): Node => {
			if (node.depth >= 3 || pick(3) === 0) {
				return node;
			}
			const inside = UNIVERSE.filter((key) => startCmp(node.start, key) < 0 && endCmp(node.end, key) > 0);
			const cuts = [...new Set(Array.from({ length: 1 + pick(3) }, () => pick(inside.length)))]
				.sort((x, y) => x - y)
				.map((i) => inside[i])
				.filter((key) => key !== undefined);
			if (cuts.length === 0) {
				return node;
			}
			const bounds = [node.start, ...cuts, node.end];
			node.children = bounds.slice(0, -1).map((start, i) => grow({ start, end: bounds[i + 1], depth: node.depth + 1, children: [] }));
			return node;
		};
		return grow({ start: null, end: null, depth: 0, children: [] });
	}

	const nameOf = (start: FokosCompositeKey | null, end: FokosCompositeKey | null) =>
		`${start ? KeyCodec.compositeForLog(start) : "min"}|${end ? KeyCodec.compositeForLog(end) : "max"}`;
	const ref = (start: FokosCompositeKey | null, end: FokosCompositeKey | null): FokosPartitionRef => ({
		partitionId: nameOf(start, end),
		doName: nameOf(start, end),
	});
	const holdsKey = (start: FokosCompositeKey | null, end: FokosCompositeKey | null, key: FokosCompositeKey) =>
		startCmp(start, key) <= 0 && endCmp(end, key) > 0;
	const holdsSegment = (
		start: FokosCompositeKey | null,
		end: FokosCompositeKey | null,
		inner: { start: FokosCompositeKey | null; end: FokosCompositeKey | null },
	) => startCmp(start, inner.start) <= 0 && endCmp(end, inner.end) >= 0;

	const bound = fc.option(fc.record({ sk: fc.constantFrom(...SORT_KEYS), inclusive: fc.boolean() }), { nil: undefined });
	const arbitrary = fc.record({
		choices: fc.array(fc.nat(1000), { minLength: 1, maxLength: 40 }),
		baseLevel: fc.constantFrom(0, 1),
		learnedMask: fc.array(fc.boolean(), { minLength: 1, maxLength: 40 }),
		hashKey: fc.constantFrom(...HASH_KEYS, "bb"),
		lower: bound,
		upper: bound,
		descending: fc.boolean(),
	});

	it("covers each key of the interval exactly once, in order, with the deepest learned slice that holds each segment", () => {
		fc.assert(
			fc.property(arbitrary, ({ choices, baseLevel, learnedMask, hashKey, lower, upper, descending }) => {
				const root = tree(choices);
				const baseNodes = baseLevel === 0 || root.children.length === 0 ? [root] : root.children;
				const bases: FrontierBase[] = baseNodes.map((n) => ({
					target: ref(n.start, n.end),
					start: n.start,
					end: n.end,
					speculative: false,
				}));
				// The learned slices: some nodes below the bases, as the learned table of a router holds them.
				const below: Node[] = [];
				const collect = (n: Node) => {
					for (const child of n.children) {
						below.push(child);
						collect(child);
					}
				};
				baseNodes.forEach(collect);
				const learnedSlices: LearnedRangeSlice[] = below
					.filter((_, i) => learnedMask[i % learnedMask.length])
					.map((n) => ({ startBoundary: n.start, endBoundary: n.end, depth: n.depth }));
				const toBound = (b: { sk: string; inclusive: boolean } | undefined) =>
					b === undefined ? undefined : { value: b.sk === "" ? KeyCodec.encodeOptional(undefined) : kb(b.sk), inclusive: b.inclusive };
				const interval: CompositeInterval = hashKeyInterval(kb(hashKey), { lower: toBound(lower), upper: toBound(upper) });

				const visits = planRangeFrontier(bases, learnedSlices, interval, descending, (s) => ref(s.startBoundary, s.endBoundary));
				const ordered = descending ? [...visits].reverse() : visits;

				// Each visit intersects the interval, and the visits are disjoint and in order.
				for (const [i, v] of ordered.entries()) {
					expect(segmentIntersects(v.segment.start, v.segment.end, interval)).toBe(true);
					if (i > 0) {
						expect(ordered[i - 1].segment.end).not.toBeNull();
						expect(startCmp(v.segment.start, ordered[i - 1].segment.end)).toBeGreaterThanOrEqual(0);
					}
				}
				// Each key of the interval is in exactly one visit.
				const inInterval = UNIVERSE.filter(
					(key) =>
						KeyCodec.compareComposite(interval.start, key) <= 0 &&
						(interval.end === null || KeyCodec.compareComposite(key, interval.end) < 0),
				);
				for (const key of inInterval) {
					expect(visits.filter((v) => holdsKey(v.segment.start, v.segment.end, key))).toHaveLength(1);
				}
				// Each visit is inside its base, and its target is the deepest learned slice that holds the whole segment.
				for (const v of visits) {
					expect(holdsSegment(v.base.start, v.base.end, v.segment)).toBe(true);
					const candidates = learnedSlices.filter(
						(s) =>
							holdsSegment(v.base.start, v.base.end, { start: s.startBoundary, end: s.endBoundary }) &&
							!(startCmp(s.startBoundary, v.base.start) === 0 && endCmp(s.endBoundary, v.base.end) === 0) &&
							holdsSegment(s.startBoundary, s.endBoundary, v.segment),
					);
					const deepest = candidates.reduce<LearnedRangeSlice | null>((d, s) => (d === null || s.depth > d.depth ? s : d), null);
					if (deepest === null) {
						expect(v.learned).toBeNull();
						expect(v.segment.target).toEqual(v.base.target);
					} else {
						expect(v.learned?.depth).toBe(deepest.depth);
						expect(holdsSegment(v.learned!.startBoundary, v.learned!.endBoundary, v.segment)).toBe(true);
						expect(v.segment.target).toEqual(ref(v.learned!.startBoundary, v.learned!.endBoundary));
					}
				}
			}),
			{ numRuns: 500 },
		);
	});
});
