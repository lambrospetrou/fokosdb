import { describe, expect, it } from "vitest";
import { KeyCodec } from "../partition-topology/key-codec.js";
import {
	clipToChildRange,
	cursorFallsInChild,
	isChildFullyBeforeCursor,
	makeBoundaryCursor,
	rangeIntersects,
	type SkInterval,
} from "./sk-interval.js";
import type { ScanCursor } from "../partition/partition-store.js";

const kb = (s: string) => KeyCodec.encode(s);
const sentinel = () => KeyCodec.encodeOptional(undefined);

// ─── rangeIntersects ──────────────────────────────────────────────────────────

describe("rangeIntersects — child [b, d)", () => {
	const childStart = kb("b");
	const childEnd = kb("d");

	it("returns true when interval is fully inside the child", () => {
		const interval: SkInterval = { lower: { value: kb("b"), inclusive: true }, upper: { value: kb("c"), inclusive: true } };
		expect(rangeIntersects(childStart, childEnd, interval)).toBe(true);
	});

	it("returns true when interval straddles the child's lower boundary", () => {
		const interval: SkInterval = { lower: { value: kb("a"), inclusive: true }, upper: { value: kb("c"), inclusive: true } };
		expect(rangeIntersects(childStart, childEnd, interval)).toBe(true);
	});

	it("returns true when interval straddles the child's upper boundary", () => {
		const interval: SkInterval = { lower: { value: kb("c"), inclusive: true }, upper: { value: kb("e"), inclusive: true } };
		expect(rangeIntersects(childStart, childEnd, interval)).toBe(true);
	});

	it("returns false when interval upper is below child start", () => {
		const interval: SkInterval = { lower: { value: kb("a"), inclusive: true }, upper: { value: kb("a"), inclusive: true } };
		expect(rangeIntersects(childStart, childEnd, interval)).toBe(false);
	});

	it("returns false when interval upper (exclusive) equals child start", () => {
		const interval: SkInterval = { lower: { value: kb("a"), inclusive: true }, upper: { value: kb("b"), inclusive: false } };
		expect(rangeIntersects(childStart, childEnd, interval)).toBe(false);
	});

	it("returns true when interval upper (inclusive) equals child start", () => {
		const interval: SkInterval = { lower: { value: kb("a"), inclusive: true }, upper: { value: kb("b"), inclusive: true } };
		expect(rangeIntersects(childStart, childEnd, interval)).toBe(true);
	});

	it("returns false when interval lower is at child end (child end is exclusive)", () => {
		const interval: SkInterval = { lower: { value: kb("d"), inclusive: true } };
		expect(rangeIntersects(childStart, childEnd, interval)).toBe(false);
	});

	it("returns false when interval lower is above child end", () => {
		const interval: SkInterval = { lower: { value: kb("e"), inclusive: true } };
		expect(rangeIntersects(childStart, childEnd, interval)).toBe(false);
	});

	it("returns true when interval lower is just below child end", () => {
		const interval: SkInterval = { lower: { value: kb("c"), inclusive: true } };
		expect(rangeIntersects(childStart, childEnd, interval)).toBe(true);
	});

	it("returns true for unbounded interval (no lower, no upper)", () => {
		expect(rangeIntersects(childStart, childEnd, {})).toBe(true);
	});
});

describe("rangeIntersects — unbounded child [d, ∞)", () => {
	it("returns true when interval lower is above child start", () => {
		const interval: SkInterval = { lower: { value: kb("z"), inclusive: true } };
		expect(rangeIntersects(kb("d"), null, interval)).toBe(true);
	});

	it("returns false when interval upper is below child start", () => {
		const interval: SkInterval = { upper: { value: kb("c"), inclusive: true } };
		expect(rangeIntersects(kb("d"), null, interval)).toBe(false);
	});
});

// ─── clipToChildRange ─────────────────────────────────────────────────────────

describe("clipToChildRange", () => {
	it("clips interval lower to child start when interval extends below", () => {
		const interval: SkInterval = { lower: { value: kb("a"), inclusive: true }, upper: { value: kb("c"), inclusive: true } };
		const result = clipToChildRange(interval, kb("b"), kb("d"));
		expect(KeyCodec.compare(result.lower!.value, kb("b"))).toBe(0);
		expect(result.lower!.inclusive).toBe(true);
		expect(KeyCodec.compare(result.upper!.value, kb("c"))).toBe(0);
		expect(result.upper!.inclusive).toBe(true);
	});

	it("clips interval upper to child end (exclusive) when interval extends above", () => {
		const interval: SkInterval = { lower: { value: kb("c"), inclusive: true }, upper: { value: kb("f"), inclusive: true } };
		const result = clipToChildRange(interval, kb("b"), kb("d"));
		expect(KeyCodec.compare(result.lower!.value, kb("c"))).toBe(0);
		expect(result.lower!.inclusive).toBe(true);
		expect(KeyCodec.compare(result.upper!.value, kb("d"))).toBe(0);
		expect(result.upper!.inclusive).toBe(false);
	});

	it("preserves interval bounds when fully within child", () => {
		const interval: SkInterval = { lower: { value: kb("b"), inclusive: false }, upper: { value: kb("c"), inclusive: false } };
		const result = clipToChildRange(interval, kb("a"), kb("d"));
		expect(KeyCodec.compare(result.lower!.value, kb("b"))).toBe(0);
		expect(result.lower!.inclusive).toBe(false);
		expect(KeyCodec.compare(result.upper!.value, kb("c"))).toBe(0);
		expect(result.upper!.inclusive).toBe(false);
	});

	it("passes through interval upper when child end is null", () => {
		const interval: SkInterval = { lower: { value: kb("c"), inclusive: true }, upper: { value: kb("z"), inclusive: true } };
		const result = clipToChildRange(interval, kb("b"), null);
		expect(KeyCodec.compare(result.upper!.value, kb("z"))).toBe(0);
		expect(result.upper!.inclusive).toBe(true);
	});

	it("leaves upper undefined when both interval and child are unbounded above", () => {
		const interval: SkInterval = { lower: { value: kb("c"), inclusive: true } };
		const result = clipToChildRange(interval, kb("b"), null);
		expect(result.upper).toBeUndefined();
	});

	it("uses sentinel for null child start", () => {
		const interval: SkInterval = {};
		const result = clipToChildRange(interval, null, kb("d"));
		expect(KeyCodec.compare(result.lower!.value, sentinel())).toBe(0);
		expect(result.lower!.inclusive).toBe(true);
		expect(KeyCodec.compare(result.upper!.value, kb("d"))).toBe(0);
		expect(result.upper!.inclusive).toBe(false);
	});

	it("preserves exclusive upper when interval upper equals child end exclusively", () => {
		const interval: SkInterval = { lower: { value: kb("b"), inclusive: true }, upper: { value: kb("d"), inclusive: false } };
		const result = clipToChildRange(interval, kb("a"), kb("d"));
		expect(KeyCodec.compare(result.upper!.value, kb("d"))).toBe(0);
		expect(result.upper!.inclusive).toBe(false);
	});

	it("clips both ends symmetrically when interval is wider than child", () => {
		const interval: SkInterval = { lower: { value: kb("a"), inclusive: false }, upper: { value: kb("z"), inclusive: false } };
		const result = clipToChildRange(interval, kb("c"), kb("f"));
		expect(KeyCodec.compare(result.lower!.value, kb("c"))).toBe(0);
		expect(result.lower!.inclusive).toBe(true);
		expect(KeyCodec.compare(result.upper!.value, kb("f"))).toBe(0);
		expect(result.upper!.inclusive).toBe(false);
	});
});

// ─── isChildFullyBeforeCursor ─────────────────────────────────────────────────

describe("isChildFullyBeforeCursor", () => {
	describe("ascending", () => {
		it("skips when child end equals cursor sk", () => {
			expect(isChildFullyBeforeCursor(kb("b"), kb("d"), { hk: kb("h"), sk: kb("d") }, "asc")).toBe(true);
		});

		it("skips when child end is below cursor sk", () => {
			expect(isChildFullyBeforeCursor(kb("b"), kb("d"), { hk: kb("h"), sk: kb("e") }, "asc")).toBe(true);
		});

		it("does not skip when child end is above cursor sk", () => {
			expect(isChildFullyBeforeCursor(kb("b"), kb("d"), { hk: kb("h"), sk: kb("c") }, "asc")).toBe(false);
		});

		it("never skips when child end is null (unbounded)", () => {
			expect(isChildFullyBeforeCursor(kb("b"), null, { hk: kb("h"), sk: kb("z") }, "asc")).toBe(false);
		});
	});

	describe("descending", () => {
		it("skips when child start equals cursor sk", () => {
			expect(isChildFullyBeforeCursor(kb("b"), kb("d"), { hk: kb("h"), sk: kb("b") }, "desc")).toBe(true);
		});

		it("skips when child start is above cursor sk", () => {
			expect(isChildFullyBeforeCursor(kb("b"), kb("d"), { hk: kb("h"), sk: kb("a") }, "desc")).toBe(true);
		});

		it("does not skip when child start is below cursor sk", () => {
			expect(isChildFullyBeforeCursor(kb("b"), kb("d"), { hk: kb("h"), sk: kb("c") }, "desc")).toBe(false);
		});
	});
});

// ─── cursorFallsInChild ───────────────────────────────────────────────────────

describe("cursorFallsInChild", () => {
	it("returns true when cursor is inside the child range", () => {
		expect(cursorFallsInChild(kb("b"), kb("d"), { hk: kb("h"), sk: kb("c") })).toBe(true);
	});

	it("returns true when cursor is at child start (start is inclusive)", () => {
		expect(cursorFallsInChild(kb("b"), kb("d"), { hk: kb("h"), sk: kb("b") })).toBe(true);
	});

	it("returns false when cursor is at child end (end is exclusive)", () => {
		expect(cursorFallsInChild(kb("b"), kb("d"), { hk: kb("h"), sk: kb("d") })).toBe(false);
	});

	it("returns false when cursor is below child start", () => {
		expect(cursorFallsInChild(kb("b"), kb("d"), { hk: kb("h"), sk: kb("a") })).toBe(false);
	});

	it("returns false when cursor is above child end", () => {
		expect(cursorFallsInChild(kb("b"), kb("d"), { hk: kb("h"), sk: kb("e") })).toBe(false);
	});

	it("returns true for any cursor >= start when child end is null", () => {
		expect(cursorFallsInChild(kb("b"), null, { hk: kb("h"), sk: kb("z") })).toBe(true);
	});

	it("returns false when cursor is below start even with null end", () => {
		expect(cursorFallsInChild(kb("b"), null, { hk: kb("h"), sk: kb("a") })).toBe(false);
	});
});

// ─── makeBoundaryCursor ───────────────────────────────────────────────────────

describe("makeBoundaryCursor", () => {
	it("asc: positions at childEnd with inclusive=true", () => {
		const c = makeBoundaryCursor(kb("h"), kb("b"), kb("d"), "asc");
		expect(KeyCodec.compare(c.sk, kb("d"))).toBe(0);
		expect(c.inclusive).toBe(true);
	});

	it("asc: falls back to sentinel when childEnd is null", () => {
		const c = makeBoundaryCursor(kb("h"), kb("b"), null, "asc");
		expect(KeyCodec.compare(c.sk, sentinel())).toBe(0);
		expect(c.inclusive).toBe(true);
	});

	it("desc: positions at childStart with inclusive=false", () => {
		const c = makeBoundaryCursor(kb("h"), kb("b"), kb("d"), "desc");
		expect(KeyCodec.compare(c.sk, kb("b"))).toBe(0);
		expect(c.inclusive).toBe(false);
	});

	it("carries the provided hashKey", () => {
		const c = makeBoundaryCursor(kb("mykey"), kb("b"), kb("d"), "asc");
		expect(KeyCodec.compare(c.hk, kb("mykey"))).toBe(0);
	});
});
