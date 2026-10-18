import { describe, expect, it } from "vitest";
import { fitBatchResponse } from "./batch-response.js";
import { MAX_BATCH_GET_RESPONSE_BYTES } from "./transaction-limits.js";

const entry = (opIndex: number, bytes: number) => ({ opIndex, bytes });
const indexes = (entries: Array<{ opIndex: number }>) => entries.map((e) => e.opIndex);

describe("fitBatchResponse", () => {
	it("keeps every item that fits, in opIndex order", () => {
		const { kept, dropped } = fitBatchResponse([entry(2, 10), entry(0, 10), entry(1, 10)], 30);
		expect(indexes(kept)).toEqual([0, 1, 2]);
		expect(dropped).toEqual([]);
	});

	it("keeps the item of opIndex 0 above the budget, and then drops what does not fit", () => {
		const { kept, dropped } = fitBatchResponse([entry(0, 50), entry(1, 1)], 30);
		expect(indexes(kept)).toEqual([0]);
		expect(indexes(dropped)).toEqual([1]);
	});

	it("does not let an item that does not fit stop a smaller item after it", () => {
		const { kept, dropped } = fitBatchResponse([entry(0, 10), entry(1, 25), entry(2, 15), entry(3, 5)], 30);
		expect(indexes(kept)).toEqual([0, 2, 3]);
		expect(indexes(dropped)).toEqual([1]);
	});

	it("gives the first item no extra room when the answer has no item of opIndex 0", () => {
		const { kept, dropped } = fitBatchResponse([entry(3, 20), entry(5, 20)], 30);
		expect(indexes(kept)).toEqual([3]);
		expect(indexes(dropped)).toEqual([5]);
	});

	it("clamps the limit to MAX_BATCH_GET_RESPONSE_BYTES", () => {
		const half = MAX_BATCH_GET_RESPONSE_BYTES / 2;
		const { kept, dropped } = fitBatchResponse([entry(1, half), entry(2, half), entry(3, 1)], Number.MAX_SAFE_INTEGER);
		expect(indexes(kept)).toEqual([1, 2]);
		expect(indexes(dropped)).toEqual([3]);
	});
});
