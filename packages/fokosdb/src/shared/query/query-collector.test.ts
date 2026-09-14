import { describe, expect, it, vi } from "vitest";
import { KeyCodec } from "../partition-topology/key-codec.js";
import type { ProjectedWireRow } from "../expression/projection.js";
import type { QueryScanRow, StoredItem } from "../partition/partition-store.js";
import type { QueryPageBudgetState } from "./page-budget.js";
import { collectQueryPage } from "./query-collector.js";

const hk = KeyCodec.encode("hk");

// A matched scan row. `item` is present on complete-item projection rows only.
function row(sk: string, estRowBytes: number, item?: Partial<StoredItem>): QueryScanRow {
	const skBytes = KeyCodec.encode(sk);
	return {
		sk: skBytes,
		estRowBytes,
		matched: true,
		item:
			item === undefined
				? null
				: { hk, sk: skBytes, data: "x", kind: "text", ttl_epoch_utc_seconds: null, v: 1, last_read_ts: 0, last_write_ts: 0, ...item },
		projected: null,
	};
}

// A scan row that carries a projected wire row instead of a complete item.
function projectedRow(sk: string, estRowBytes: number, projected: ProjectedWireRow): QueryScanRow {
	return { sk: KeyCodec.encode(sk), estRowBytes, matched: true, item: null, projected };
}

// Records how many rows the consumer pulled, to prove the stream stops at a rejected candidate.
function tracked(rows: QueryScanRow[]) {
	let pulled = 0;
	const iterable = function* () {
		for (const r of rows) {
			pulled++;
			yield r;
		}
	};
	return { rows: iterable(), pulled: () => pulled };
}

type Budget = Pick<
	QueryPageBudgetState,
	"remainingEvaluatedItems" | "remainingEvaluatedBytes" | "remainingResponseBytes" | "allowOversizedFirstItem"
>;
const budget = (overrides: Partial<Budget> = {}): Budget => ({
	remainingEvaluatedItems: 1_000,
	remainingEvaluatedBytes: 1_000_000,
	remainingResponseBytes: 1_000_000,
	allowOversizedFirstItem: true,
	...overrides,
});

describe("collectQueryPage", () => {
	it("stops at the evaluated-item budget and resumes inclusively at the rejected candidate", () => {
		const { rows, pulled } = tracked([row("a", 10, {}), row("b", 20, {}), row("c", 30, {})]);
		const state = collectQueryPage({
			rows,
			hashKey: hk,
			select: "projection",
			budget: budget({ remainingEvaluatedItems: 2 }),
			estimateResponseBytes: () => 5,
		});
		expect(state.items).toHaveLength(2);
		expect(state.count).toBe(2);
		expect(state.scannedCount).toBe(2);
		expect(state.evaluatedBytes).toBe(30);
		expect(state.responseBytes).toBe(10);
		expect(state.rowsReturned).toBe(3);
		expect(state.nextCursor).toEqual({ hk, sk: KeyCodec.encode("c"), inclusive: true });
		expect(state.lastEvaluatedCursor).toEqual({ hk, sk: KeyCodec.encode("b") });
		expect(pulled()).toBe(3);
	});

	it("returns a null cursor when the stream drains inside the budgets", () => {
		const { rows, pulled } = tracked([row("a", 10, {}), row("b", 20, {})]);
		const state = collectQueryPage({
			rows,
			hashKey: hk,
			select: "projection",
			budget: budget({ remainingEvaluatedItems: 2 }),
			estimateResponseBytes: () => 5,
		});
		expect(state.nextCursor).toBeNull();
		expect(state.rowsReturned).toBe(2);
		expect(pulled()).toBe(2);
	});

	it("count mode materializes no items, consumes no response bytes, and never estimates", () => {
		const { rows, pulled } = tracked([row("a", 10), row("b", 20), row("c", 30)]);
		const estimate = vi.fn();
		const state = collectQueryPage({ rows, hashKey: hk, select: "count", budget: budget(), estimateResponseBytes: estimate });
		expect(state.items).toEqual([]);
		expect(state.responseBytes).toBe(0);
		expect(state.count).toBe(3);
		expect(state.scannedCount).toBe(3);
		expect(state.evaluatedBytes).toBe(60);
		expect(state.rowsReturned).toBe(3);
		expect(state.nextCursor).toBeNull();
		expect(estimate).not.toHaveBeenCalled();
		expect(pulled()).toBe(3);
	});

	it("stops before a candidate that does not fit the evaluated-byte budget", () => {
		const { rows, pulled } = tracked([row("a", 100, {}), row("b", 100, {}), row("c", 100, {})]);
		const state = collectQueryPage({
			rows,
			hashKey: hk,
			select: "projection",
			budget: budget({ remainingEvaluatedBytes: 250 }),
			estimateResponseBytes: () => 5,
		});
		expect(state.scannedCount).toBe(2);
		expect(state.evaluatedBytes).toBe(200);
		expect(state.count).toBe(2);
		expect(state.rowsReturned).toBe(3);
		expect(state.nextCursor).toEqual({ hk, sk: KeyCodec.encode("c"), inclusive: true });
		expect(pulled()).toBe(3);
	});

	it("lets the first materialized item exceed the response budget, then stops at the next", () => {
		const { rows, pulled } = tracked([row("a", 10, {}), row("b", 10, {}), row("c", 10, {})]);
		const sizes = new Map([
			["a", 500],
			["b", 50],
			["c", 50],
		]);
		const state = collectQueryPage({
			rows,
			hashKey: hk,
			select: "projection",
			budget: budget({ remainingResponseBytes: 100, allowOversizedFirstItem: true }),
			estimateResponseBytes: (item) => sizes.get(KeyCodec.decode((item as StoredItem).sk) as string)!,
		});
		expect(state.items).toHaveLength(1);
		expect(state.responseBytes).toBe(500);
		expect(state.scannedCount).toBe(1);
		expect(state.count).toBe(1);
		expect(state.allowOversizedFirstItem).toBe(false);
		expect(state.rowsReturned).toBe(2);
		expect(state.nextCursor).toEqual({ hk, sk: KeyCodec.encode("b"), inclusive: true });
		expect(pulled()).toBe(2);
	});

	it("rejects the first row when the response budget is spent and no oversized item is allowed", () => {
		const { rows, pulled } = tracked([row("a", 10, {}), row("b", 10, {})]);
		const state = collectQueryPage({
			rows,
			hashKey: hk,
			select: "projection",
			budget: budget({ remainingResponseBytes: 10, allowOversizedFirstItem: false }),
			estimateResponseBytes: () => 50,
		});
		expect(state.items).toEqual([]);
		expect(state.scannedCount).toBe(0);
		expect(state.evaluatedBytes).toBe(0);
		expect(state.count).toBe(0);
		expect(state.responseBytes).toBe(0);
		expect(state.rowsReturned).toBe(1);
		expect(state.lastEvaluatedCursor).toBeNull();
		expect(state.nextCursor).toEqual({ hk, sk: KeyCodec.encode("a"), inclusive: true });
		expect(pulled()).toBe(1);
	});

	it("count mode ignores the response budget entirely", () => {
		const { rows } = tracked([row("a", 10), row("b", 20), row("c", 30)]);
		const state = collectQueryPage({
			rows,
			hashKey: hk,
			select: "count",
			budget: budget({ remainingResponseBytes: 0, allowOversizedFirstItem: false }),
			estimateResponseBytes: () => 50,
		});
		expect(state.count).toBe(3);
		expect(state.scannedCount).toBe(3);
		expect(state.responseBytes).toBe(0);
		expect(state.nextCursor).toBeNull();
	});

	it("pushes a projected wire row as-is and charges its estimate", () => {
		const wire: ProjectedWireRow = ["a", 7, undefined];
		const { rows } = tracked([projectedRow("a", 10, wire)]);
		const estimate = vi.fn().mockReturnValue(42);
		const state = collectQueryPage({ rows, hashKey: hk, select: "projection", budget: budget(), estimateResponseBytes: estimate });
		expect(state.items[0]).toBe(wire);
		expect(estimate).toHaveBeenCalledWith(wire);
		expect(state.responseBytes).toBe(42);
		expect(state.count).toBe(1);
		expect(state.scannedCount).toBe(1);
	});

	it("an unmatched candidate consumes the evaluated budgets and advances the cursor but counts nothing", () => {
		const unmatched: QueryScanRow = { sk: KeyCodec.encode("a"), estRowBytes: 10, matched: false, item: null, projected: null };
		const { rows } = tracked([unmatched]);
		const estimate = vi.fn();
		const state = collectQueryPage({ rows, hashKey: hk, select: "projection", budget: budget(), estimateResponseBytes: estimate });
		expect(state.items).toEqual([]);
		expect(state.count).toBe(0);
		expect(state.scannedCount).toBe(1);
		expect(state.evaluatedBytes).toBe(10);
		expect(state.responseBytes).toBe(0);
		expect(state.lastEvaluatedCursor).toEqual({ hk, sk: KeyCodec.encode("a") });
		expect(state.nextCursor).toBeNull();
		expect(estimate).not.toHaveBeenCalled();
	});

	it("admits an oversized projected first row once, then stops before the next", () => {
		const { rows, pulled } = tracked([projectedRow("a", 10, ["x"]), projectedRow("b", 10, ["y"])]);
		const state = collectQueryPage({
			rows,
			hashKey: hk,
			select: "projection",
			budget: budget({ remainingResponseBytes: 5, allowOversizedFirstItem: true }),
			estimateResponseBytes: () => 10,
		});
		expect(state.items).toEqual([["x"]]);
		expect(state.responseBytes).toBe(10);
		expect(state.count).toBe(1);
		expect(state.nextCursor).toEqual({ hk, sk: KeyCodec.encode("b"), inclusive: true });
		expect(pulled()).toBe(2);
	});
});
