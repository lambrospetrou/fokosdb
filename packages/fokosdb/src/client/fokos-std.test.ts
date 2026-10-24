import { describe, expect, it } from "vitest";
import { FokosStd } from "./fokos-std.js";
import type { BatchWriteItem, BatchWriteItemResult, ItemBatcher } from "../shared/batch-api-types.js";
import { fokosErrorWith } from "../../test/errors-matchers.js";

// The value cases live in key-codec.test.ts with `KeyCodec.publicSuccessor`; here only the
// validation and the overload typing that FokosStd adds on top are pinned.
describe("FokosStd.sortKeySuccessor — validation and typing", () => {
	it("rejects a NUL in a string prefix", () => {
		expect(() => FokosStd.sortKeySuccessor("s\0k")).toThrow(fokosErrorWith("key_contains_nul", { key: "sortKey" }));
	});

	it("keeps the input type on the result", () => {
		const s: string | undefined = FokosStd.sortKeySuccessor("x");
		const b: Uint8Array | undefined = FokosStd.sortKeySuccessor(new Uint8Array([1]));
		expect(s).toBe("y");
		expect(b).toEqual(new Uint8Array([2]));
	});
});

describe("FokosStd.notBeginsWith", () => {
	it("builds the two complement ranges in scan order", () => {
		expect(FokosStd.notBeginsWith("hk", "order#")).toEqual([
			{ hashKey: "hk", sortKeyCondition: { op: "lt", value: "order#" }, scanIndexForward: true },
			{ hashKey: "hk", sortKeyCondition: { op: "gte", value: "order$" }, scanIndexForward: true },
		]);
	});

	it("reverses the ranges with scanIndexForward=false", () => {
		expect(FokosStd.notBeginsWith("hk", "order#", { scanIndexForward: false })).toEqual([
			{ hashKey: "hk", sortKeyCondition: { op: "gte", value: "order$" }, scanIndexForward: false },
			{ hashKey: "hk", sortKeyCondition: { op: "lt", value: "order#" }, scanIndexForward: false },
		]);
	});

	it("drops the upper range when the prefix has no successor", () => {
		expect(FokosStd.notBeginsWith("hk", "\u{10FFFF}")).toEqual([
			{ hashKey: "hk", sortKeyCondition: { op: "lt", value: "\u{10FFFF}" }, scanIndexForward: true },
		]);
	});

	it("returns no sub-queries for an empty prefix", () => {
		expect(FokosStd.notBeginsWith("hk", "")).toEqual([]);
		expect(FokosStd.notBeginsWith("hk", new Uint8Array([]))).toEqual([]);
	});
});

describe("FokosStd.batchWriteItemsWithRetry — a condition after unknown", () => {
	it("keeps unknown when the resend fails its condition, because the first try can have applied", async () => {
		const answers: BatchWriteItemResult[] = [
			{ outcome: "unknown", code: "foreign_error", error_id: "e_1" },
			{ outcome: "rejected", code: "condition_failed", error_id: "e_2", partitionIndex: 0 },
		];
		let calls = 0;
		const batcher: ItemBatcher = {
			batchGetItems: () => Promise.reject(new Error("not used")),
			batchWriteItems: (opts) => {
				const result = answers[calls++];
				return Promise.resolve({
					results: [result],
					unprocessedItems: result.outcome === "unknown" ? opts.items : [],
					meta: { forwardCount: 0, partitions: [] },
				});
			},
		};
		const item: BatchWriteItem = { operation: "put", hashKey: "k", data: "v", condition: { op: "not_exists", args: [{ ref: "hashKey" }] } };

		const write = await FokosStd.batchWriteItemsWithRetry(batcher, { items: [item] }, { baseDelayMs: 1, maxDelayMs: 2, maxAttempts: 3 });

		expect(calls).toBe(2);
		expect(write.results).toEqual([answers[0]]);
		expect(write.unprocessedItems).toEqual([item]);
	});
});
