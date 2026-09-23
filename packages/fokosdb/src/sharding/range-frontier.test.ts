import { describe, expect, it } from "vitest";
import { KeyCodec, type KeyBytes } from "./key-codec.js";
import { planRangeFrontier, type FrontierBase } from "./range-frontier.js";
import type { FokosPartitionRef } from "./route-context.js";
import type { LearnedRangeSlice } from "./sharding-store.js";
import type { SkInterval } from "./sk-interval.js";

const kb = (s: string) => KeyCodec.encode(s);
const k = (s: string | null): KeyBytes | null => (s === null ? null : kb(s));
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
	return { target: target === "local" ? "local" : refOf(k(start), k(end)), start: k(start), end: k(end), speculative };
}

function learned(start: string | null, end: string | null, depth: number): LearnedRangeSlice {
	return { startBoundary: k(start), endBoundary: k(end), depth };
}

const ALL: SkInterval = {};

/** The visits as `[target, start, end]` names. */
function shape(visits: ReturnType<typeof planRangeFrontier>) {
	return visits.map(({ visit }) => [
		visit.target === "local" ? "local" : visit.target.doName,
		visit.start === null ? null : KeyCodec.decode(visit.start),
		visit.end === null ? null : KeyCodec.decode(visit.end),
	]);
}

describe("planRangeFrontier", () => {
	it("returns the base cover unchanged when nothing is learned", () => {
		const visits = planRangeFrontier([base(null, "m"), base("m", null)], [], ALL, false, refOf);
		expect(shape(visits)).toEqual([
			["r.min.m", null, "m"],
			["r.m.max", "m", null],
		]);
		expect(visits.every((v) => v.learned === null)).toBe(true);
	});

	it("drops the bases that the requested interval does not touch, and honours an inclusive upper bound on a start boundary", () => {
		const bases = [base(null, "m"), base("m", null)];
		expect(shape(planRangeFrontier(bases, [], { upper: { value: kb("m"), inclusive: false } }, false, refOf))).toEqual([
			["r.min.m", null, "m"],
		]);
		// `<= m` needs the child that starts at m: it owns the key m.
		expect(shape(planRangeFrontier(bases, [], { upper: { value: kb("m"), inclusive: true } }, false, refOf))).toHaveLength(2);
	});

	it("overlays learned descendants and keeps the base target for every gap", () => {
		// The root covers everything. Two learned leaves cover [c, f) and [f, k); the rest stays on the root.
		const visits = planRangeFrontier([base(null, null)], [learned("c", "f", 2), learned("f", "k", 2)], ALL, false, refOf);
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
		const visits = planRangeFrontier([base(null, null)], [learned("c", "k", 1), learned("f", "k", 2)], ALL, false, refOf);
		expect(shape(visits)).toEqual([
			["r.min.max", null, "c"],
			["r.c.k", "c", "f"],
			["r.f.k", "f", "k"],
			["r.min.max", "k", null],
		]);
	});

	it("bypasses the root when the learned leaves cover the whole interval", () => {
		const visits = planRangeFrontier([base(null, null)], [learned(null, "m", 1), learned("m", null, 1)], ALL, false, refOf);
		expect(shape(visits)).toEqual([
			["r.min.m", null, "m"],
			["r.m.max", "m", null],
		]);
	});

	it("ignores a learned slice that is not a strict sub-slice of a base, such as an ancestor or the base itself", () => {
		const visits = planRangeFrontier([base("c", "k")], [learned(null, null, 0), learned("c", "k", 1)], ALL, false, refOf);
		expect(shape(visits)).toEqual([["r.c.k", "c", "k"]]);
	});

	it("keeps the visits disjoint, in order, and equal to the requested interval inside the cover", () => {
		const bases = [base(null, "m"), base("m", null)];
		const rows = [learned("c", "f", 2), learned("f", "m", 2), learned("m", "p", 2), learned("x", null, 2)];
		const visits = planRangeFrontier(
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
		const visits = planRangeFrontier([base(null, "m"), base("m", null)], [learned("c", "f", 2)], ALL, true, refOf);
		expect(shape(visits)).toEqual([
			["r.m.max", "m", null],
			["r.min.m", "f", "m"],
			["r.c.f", "c", "f"],
			["r.min.m", null, "c"],
		]);
	});

	it("marks every visit of a speculative base as speculative", () => {
		const visits = planRangeFrontier([base(null, null, "ref", true)], [learned("c", "f", 2)], ALL, false, refOf);
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
		const visits = planRangeFrontier([base(null, null)], [learned(null, "c", 2)], ALL, false, refOf);
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
		const visits = planRangeFrontier([base(null, null)], [learned(null, "f", 1), learned(null, "c", 2)], ALL, false, refOf);
		expect(shape(visits)).toEqual([
			["r.min.c", null, "c"],
			["r.min.f", "c", "f"],
			["r.min.max", "f", null],
		]);
		// [c, f) is not inside the depth-2 slice, so the deeper slice must not claim it.
		expect(visits[1].learned).toEqual(learned(null, "f", 1));
	});

	it("plans one local visit for a leaf", () => {
		expect(shape(planRangeFrontier([base("c", "k", "local")], [], ALL, false, refOf))).toEqual([["local", "c", "k"]]);
	});
});
