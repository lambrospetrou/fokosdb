import { describe, expect, it } from "vitest";
import { PageBudget } from "./page-budget.js";

describe("PageBudget", () => {
	it("is not exhausted while every counter has room", () => {
		const budget = new PageBudget(1_000, 10, 5);
		budget.consume(100, 1, 1);
		expect(budget.budgetExhausted).toBe(false);
		expect(budget.visitsExhausted).toBe(false);
		expect(budget.exhausted).toBe(false);
	});

	it("reports exhausted when a counter lands exactly on zero", () => {
		expect(exhaustedAfter(new PageBudget(100, 10, 5), [100, 0, 0])).toBe(true);
		expect(exhaustedAfter(new PageBudget(100, 10, 5), [0, 10, 0])).toBe(true);
		expect(exhaustedAfter(new PageBudget(100, 10, 5), [0, 0, 5])).toBe(true);
	});

	// The leaf caps its scan with `maxItems: remainingLimit`, so today nothing overshoots. That makes
	// this the guard against a future caller that does: a negative counter is more exhausted than a
	// zero one, and must never read as "keep going".
	it("stays exhausted when a counter overshoots past zero", () => {
		expect(exhaustedAfter(new PageBudget(100, 10, 5), [0, 11, 0])).toBe(true);
		expect(exhaustedAfter(new PageBudget(100, 10, 5), [101, 0, 0])).toBe(true);
		expect(exhaustedAfter(new PageBudget(100, 10, 5), [0, 0, 6])).toBe(true);
	});

	// `null` means "no item cap". It needs an explicit check because JS coerces null to 0 in a
	// relational comparison, so `null <= 0` is true — the bare comparison would exhaust the budget
	// immediately on every unlimited query.
	it("treats a null item limit as unlimited, however many items are consumed", () => {
		const budget = new PageBudget(1_000, null, 5);
		expect(budget.budgetExhausted).toBe(false);
		budget.consume(1, 10_000, 1);
		expect(budget.remainingLimit).toBeNull();
		expect(budget.budgetExhausted).toBe(false);
	});

	it("still exhausts a null-limit budget on bytes and visits", () => {
		expect(exhaustedAfter(new PageBudget(100, null, 5), [100, 999, 0])).toBe(true);
		expect(exhaustedAfter(new PageBudget(100, null, 5), [0, 999, 5])).toBe(true);
	});
});

function exhaustedAfter(budget: PageBudget, [bytes, items, visits]: [number, number, number]): boolean {
	budget.consume(bytes, items, visits);
	return budget.exhausted;
}
