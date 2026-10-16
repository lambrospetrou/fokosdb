import { describe, expect, it } from "vitest";
import { QueryPageBudget } from "./page-budget.js";

const makeBudget = () =>
	new QueryPageBudget({
		remainingEvaluatedItems: 10,
		remainingEvaluatedBytes: 1_000,
		remainingResponseBytes: 1_000,
		remainingPartitionVisits: 5,
		allowOversizedFirstItem: true,
	});

const res = (
	overrides: {
		scannedCount?: number;
		evaluatedBytes?: number;
		responseBytes?: number;
		items?: readonly unknown[];
		partitions?: readonly unknown[];
	} = {},
) => ({
	scannedCount: 0,
	evaluatedBytes: 0,
	responseBytes: 0,
	items: [],
	partitions: [{}],
	...overrides,
});

describe("QueryPageBudget", () => {
	it("is not exhausted while every counter has room", () => {
		const budget = makeBudget();
		budget.consume(res({ scannedCount: 1, evaluatedBytes: 100, responseBytes: 100, items: [{}] }));
		expect(budget.budgetExhausted).toBe(false);
		expect(budget.visitsExhausted).toBe(false);
		expect(budget.exhausted).toBe(false);
	});

	it("reports exhausted when a counter lands exactly on zero", () => {
		for (const r of [
			res({ scannedCount: 10 }),
			res({ evaluatedBytes: 1_000 }),
			res({ responseBytes: 1_000 }),
			res({ partitions: [{}, {}, {}, {}, {}] }),
		]) {
			const budget = makeBudget();
			budget.consume(r);
			expect(budget.exhausted).toBe(true);
		}
	});

	// A caller can overshoot a counter when a response lands past the remaining budget. A negative
	// counter is more exhausted than a zero one, and must never read as "keep going".
	it("stays exhausted when a counter overshoots past zero", () => {
		for (const r of [res({ scannedCount: 11 }), res({ evaluatedBytes: 1_001 }), res({ partitions: [{}, {}, {}, {}, {}, {}] })]) {
			const budget = makeBudget();
			budget.consume(r);
			expect(budget.exhausted).toBe(true);
		}
	});

	it("keeps allowOversizedFirstItem until a response carries an item", () => {
		const budget = makeBudget();
		budget.consume(res({ scannedCount: 3, evaluatedBytes: 30 }));
		expect(budget.allowOversizedFirstItem).toBe(true);
		budget.consume(res({ scannedCount: 1, evaluatedBytes: 10, responseBytes: 10, items: [{}] }));
		expect(budget.allowOversizedFirstItem).toBe(false);
	});
});
