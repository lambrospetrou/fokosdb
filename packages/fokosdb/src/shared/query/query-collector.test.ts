import { describe, expect, it, vi } from "vitest";
import { KeyCodec } from "../../sharding/key-codec.js";
import type { ProjectedWireRow } from "../expression/projection.js";
import type { StoredItem } from "../partition/partition-store.js";
import type { QueryPageBudgetState } from "./page-budget.js";
import { createQueryPageCollector } from "./query-collector.js";

const hk = KeyCodec.encode("hk");

// One scan candidate as the store hands it to the consumer, with the payload it would decode.
type Candidate = { sk: string; estRowBytes: number; matched: boolean; payload: StoredItem | ProjectedWireRow | null };

// A matched candidate that decodes to a complete item.
function row(sk: string, estRowBytes: number, item: Partial<StoredItem> = {}): Candidate {
	const skBytes = KeyCodec.encode(sk);
	return {
		sk,
		estRowBytes,
		matched: true,
		payload: { hk, sk: skBytes, data: "x", kind: "text", ttl_epoch_utc_seconds: null, v: 1, last_read_ts: 0, last_write_ts: 0, ...item },
	};
}

// A matched candidate that decodes to a projected wire row instead of a complete item.
function projectedRow(sk: string, estRowBytes: number, projected: ProjectedWireRow): Candidate {
	return { sk, estRowBytes, matched: true, payload: projected };
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

// Drives the collector as the store does: one consumer call per candidate until it returns false.
// Records how many candidates it handed over and how many payloads the collector decoded.
function collect(
	candidates: Candidate[],
	opts: { select: "count" | "projection"; budget?: Budget; estimateResponseBytes?: (item: StoredItem | ProjectedWireRow) => number },
) {
	const collector = createQueryPageCollector({
		hashKey: hk,
		select: opts.select,
		budget: opts.budget ?? budget(),
		estimateResponseBytes: opts.estimateResponseBytes ?? (() => 5),
	});
	let pulled = 0;
	let decoded = 0;
	for (const c of candidates) {
		pulled++;
		const decode = () => {
			decoded++;
			if (c.payload === null) throw new Error("decoded a candidate without a payload");
			return c.payload;
		};
		if (!collector.consume(KeyCodec.encode(c.sk), c.estRowBytes, c.matched, decode)) break;
	}
	return { state: collector.state, pulled, decoded };
}

describe("createQueryPageCollector", () => {
	it("stops at the evaluated-item budget and resumes inclusively at the rejected candidate", () => {
		const { state, pulled, decoded } = collect([row("a", 10), row("b", 20), row("c", 30)], {
			select: "projection",
			budget: budget({ remainingEvaluatedItems: 2 }),
		});
		expect(state.items).toHaveLength(2);
		expect(state.count).toBe(2);
		expect(state.scannedCount).toBe(2);
		expect(state.evaluatedBytes).toBe(30);
		expect(state.responseBytes).toBe(10);
		expect(state.rowsReturned).toBe(3);
		expect(state.nextCursor).toEqual({ hk, sk: KeyCodec.encode("c"), inclusive: true });
		expect(state.lastEvaluatedCursor).toEqual({ hk, sk: KeyCodec.encode("b") });
		expect(pulled).toBe(3);
		// The rejected candidate is never materialized.
		expect(decoded).toBe(2);
	});

	it("returns a null cursor when the scan drains inside the budgets", () => {
		const { state, pulled } = collect([row("a", 10), row("b", 20)], {
			select: "projection",
			budget: budget({ remainingEvaluatedItems: 2 }),
		});
		expect(state.nextCursor).toBeNull();
		expect(state.rowsReturned).toBe(2);
		expect(pulled).toBe(2);
	});

	it("count mode decodes no payload, materializes no items, consumes no response bytes, and never estimates", () => {
		const estimate = vi.fn();
		const { state, pulled, decoded } = collect(
			[
				{ sk: "a", estRowBytes: 10, matched: true, payload: null },
				{ sk: "b", estRowBytes: 20, matched: true, payload: null },
				{ sk: "c", estRowBytes: 30, matched: true, payload: null },
			],
			{ select: "count", estimateResponseBytes: estimate },
		);
		expect(state.items).toEqual([]);
		expect(state.responseBytes).toBe(0);
		expect(state.count).toBe(3);
		expect(state.scannedCount).toBe(3);
		expect(state.evaluatedBytes).toBe(60);
		expect(state.rowsReturned).toBe(3);
		expect(state.nextCursor).toBeNull();
		expect(estimate).not.toHaveBeenCalled();
		expect(pulled).toBe(3);
		expect(decoded).toBe(0);
	});

	it("stops before a candidate that does not fit the evaluated-byte budget, without decoding it", () => {
		const { state, pulled, decoded } = collect([row("a", 100), row("b", 100), row("c", 100)], {
			select: "projection",
			budget: budget({ remainingEvaluatedBytes: 250 }),
		});
		expect(state.scannedCount).toBe(2);
		expect(state.evaluatedBytes).toBe(200);
		expect(state.count).toBe(2);
		expect(state.rowsReturned).toBe(3);
		expect(state.nextCursor).toEqual({ hk, sk: KeyCodec.encode("c"), inclusive: true });
		expect(pulled).toBe(3);
		expect(decoded).toBe(2);
	});

	it("lets the first materialized item exceed the response budget, then stops at the next", () => {
		const sizes = new Map([
			["a", 500],
			["b", 50],
			["c", 50],
		]);
		const { state, pulled } = collect([row("a", 10), row("b", 10), row("c", 10)], {
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
		expect(pulled).toBe(2);
	});

	it("rejects the first row when the response budget is spent and no oversized item is allowed", () => {
		const { state, pulled } = collect([row("a", 10), row("b", 10)], {
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
		expect(pulled).toBe(1);
	});

	it("count mode ignores the response budget entirely", () => {
		const { state } = collect(
			[
				{ sk: "a", estRowBytes: 10, matched: true, payload: null },
				{ sk: "b", estRowBytes: 20, matched: true, payload: null },
				{ sk: "c", estRowBytes: 30, matched: true, payload: null },
			],
			{ select: "count", budget: budget({ remainingResponseBytes: 0, allowOversizedFirstItem: false }), estimateResponseBytes: () => 50 },
		);
		expect(state.count).toBe(3);
		expect(state.scannedCount).toBe(3);
		expect(state.responseBytes).toBe(0);
		expect(state.nextCursor).toBeNull();
	});

	it("pushes a projected wire row as-is and charges its estimate", () => {
		const wire: ProjectedWireRow = ["a", 7, undefined];
		const estimate = vi.fn().mockReturnValue(42);
		const { state } = collect([projectedRow("a", 10, wire)], { select: "projection", estimateResponseBytes: estimate });
		expect(state.items[0]).toBe(wire);
		expect(estimate).toHaveBeenCalledWith(wire);
		expect(state.responseBytes).toBe(42);
		expect(state.count).toBe(1);
		expect(state.scannedCount).toBe(1);
	});

	it("an unmatched candidate consumes the evaluated budgets and advances the cursor but counts nothing and decodes nothing", () => {
		const estimate = vi.fn();
		const { state, decoded } = collect([{ sk: "a", estRowBytes: 10, matched: false, payload: null }], {
			select: "projection",
			estimateResponseBytes: estimate,
		});
		expect(state.items).toEqual([]);
		expect(state.count).toBe(0);
		expect(state.scannedCount).toBe(1);
		expect(state.evaluatedBytes).toBe(10);
		expect(state.responseBytes).toBe(0);
		expect(state.lastEvaluatedCursor).toEqual({ hk, sk: KeyCodec.encode("a") });
		expect(state.nextCursor).toBeNull();
		expect(estimate).not.toHaveBeenCalled();
		expect(decoded).toBe(0);
	});

	it("admits an oversized projected first row once, then stops before the next", () => {
		const { state, pulled } = collect([projectedRow("a", 10, ["x"]), projectedRow("b", 10, ["y"])], {
			select: "projection",
			budget: budget({ remainingResponseBytes: 5, allowOversizedFirstItem: true }),
			estimateResponseBytes: () => 10,
		});
		expect(state.items).toEqual([["x"]]);
		expect(state.responseBytes).toBe(10);
		expect(state.count).toBe(1);
		expect(state.nextCursor).toEqual({ hk, sk: KeyCodec.encode("b"), inclusive: true });
		expect(pulled).toBe(2);
	});
});
