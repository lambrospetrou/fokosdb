import { describe, expect, it } from "vitest";
import { FokosDBStd } from "./fokos-std.js";
import type { BatchGetItemKey, BatchGetItemResult, BatchWriteItem, BatchWriteItemResult, ItemBatcher } from "../shared/batch-api-types.js";
import { systemPlatform, type FokosPlatform } from "../shared/platform.js";
import type { FokosDBRetryOptions } from "./db.js";
import { fokosErrorWith } from "../../test/errors-matchers.js";

// The value cases live in key-codec.test.ts with `KeyCodec.publicSuccessor`; here only the
// validation and the overload typing that FokosDBStd adds on top are pinned.
describe("FokosDBStd.sortKeySuccessor — validation and typing", () => {
	it("rejects a NUL in a string prefix", () => {
		expect(() => FokosDBStd.sortKeySuccessor("s\0k")).toThrow(fokosErrorWith("key_contains_nul", { key: "sortKey" }));
	});

	it("keeps the input type on the result", () => {
		const s: string | undefined = FokosDBStd.sortKeySuccessor("x");
		const b: Uint8Array | undefined = FokosDBStd.sortKeySuccessor(new Uint8Array([1]));
		expect(s).toBe("y");
		expect(b).toEqual(new Uint8Array([2]));
	});
});

describe("FokosDBStd.notBeginsWith", () => {
	it("builds the two complement ranges in scan order", () => {
		expect(FokosDBStd.notBeginsWith("hk", "order#")).toEqual([
			{ hashKey: "hk", sortKeyCondition: { op: "lt", value: "order#" }, scanIndexForward: true },
			{ hashKey: "hk", sortKeyCondition: { op: "gte", value: "order$" }, scanIndexForward: true },
		]);
	});

	it("reverses the ranges with scanIndexForward=false", () => {
		expect(FokosDBStd.notBeginsWith("hk", "order#", { scanIndexForward: false })).toEqual([
			{ hashKey: "hk", sortKeyCondition: { op: "gte", value: "order$" }, scanIndexForward: false },
			{ hashKey: "hk", sortKeyCondition: { op: "lt", value: "order#" }, scanIndexForward: false },
		]);
	});

	it("drops the upper range when the prefix has no successor", () => {
		expect(FokosDBStd.notBeginsWith("hk", "\u{10FFFF}")).toEqual([
			{ hashKey: "hk", sortKeyCondition: { op: "lt", value: "\u{10FFFF}" }, scanIndexForward: true },
		]);
	});

	it("returns no sub-queries for an empty prefix", () => {
		expect(FokosDBStd.notBeginsWith("hk", "")).toEqual([]);
		expect(FokosDBStd.notBeginsWith("hk", new Uint8Array([]))).toEqual([]);
	});
});

describe("FokosDBStd.batchWriteItemsWithRetry — a condition after unknown", () => {
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

		const write = await FokosDBStd.batchWriteItemsWithRetry(batcher, { items: [item] }, { baseDelayMs: 1, maxDelayMs: 2, maxAttempts: 3 });

		expect(calls).toBe(2);
		expect(write.results).toEqual([answers[0]]);
		expect(write.unprocessedItems).toEqual([item]);
	});
});

/**
 * A batcher whose read answers each call with `answer(keys)`. It records the keys of each call and
 * the waits of the backoff, and it never sleeps.
 */
function readBatcher(answer: (keys: BatchGetItemKey[]) => BatchGetItemResult[], retry?: FokosDBRetryOptions) {
	const calls: string[][] = [];
	const waits: number[] = [];
	const platform: FokosPlatform = {
		...systemPlatform,
		wait: (ms) => {
			waits.push(ms);
			return Promise.resolve();
		},
	};
	const batcher: ItemBatcher = {
		batchGetItems: (opts) => {
			const keys = opts.items as BatchGetItemKey[];
			calls.push(keys.map((key) => key.hashKey as string));
			const items = answer(keys);
			const unprocessedKeys = keys.filter((_, i) => items[i].outcome === "unprocessed");
			return Promise.resolve({ items, unprocessedKeys, meta: { forwardCount: 0, partitions: [{ servedByActorId: "p" }] } } as never);
		},
		batchWriteItems: () => Promise.reject(new Error("not used")),
		platform,
		retry,
	};
	return { batcher, calls, waits };
}

const OK: BatchGetItemResult = { outcome: "ok", partitionIndex: 0 };
const TOO_LARGE: BatchGetItemResult = { outcome: "unprocessed", code: "response_too_large" };
const FAULT: BatchGetItemResult = { outcome: "unprocessed", code: "foreign_error", error_id: "e_1" };
const SHORT_RETRY = { baseDelayMs: 1, maxDelayMs: 2 };

describe("FokosDBStd.batchGetItemsWithRetry — pages and attempts", () => {
	const keys: BatchGetItemKey[] = ["a", "b", "c", "d"].map((hashKey) => ({ hashKey }));

	it("reads every page at once, and a page does not count as an attempt", async () => {
		// Each call answers only its first key, as `batchGetItems` does with a small `maxResponseBytes`.
		const { batcher, calls, waits } = readBatcher((page) => page.map((_, i) => (i === 0 ? OK : TOO_LARGE)));

		const read = await FokosDBStd.batchGetItemsWithRetry(batcher, { items: keys }, { ...SHORT_RETRY, maxAttempts: 1 });

		expect(calls).toEqual([["a", "b", "c", "d"], ["b", "c", "d"], ["c", "d"], ["d"]]);
		expect(waits).toEqual([]);
		expect(read.items).toEqual([OK, OK, OK, OK]);
		expect(read.unprocessedKeys).toEqual([]);
	});

	it("counts a call with an error, and waits before the next call", async () => {
		// Key "a" always fails. Of the other keys, each call answers only the first one.
		const { batcher, calls, waits } = readBatcher((page) => {
			const answered = page.find((key) => key.hashKey !== "a");
			return page.map((key) => (key.hashKey === "a" ? FAULT : key === answered ? OK : TOO_LARGE));
		});

		const read = await FokosDBStd.batchGetItemsWithRetry(batcher, { items: keys }, { ...SHORT_RETRY, maxAttempts: 2 });

		// Call 1 has an error and counts. Call 2 has an error again and reaches `maxAttempts`.
		expect(calls).toEqual([
			["a", "b", "c", "d"],
			["a", "c", "d"],
		]);
		expect(waits).toHaveLength(1);
		expect(read.items).toEqual([FAULT, OK, OK, TOO_LARGE]);
		expect(read.unprocessedKeys).toEqual([{ hashKey: "a" }, { hashKey: "d" }]);
	});

	it("counts a call that answers no key, so a batcher that never answers stops", async () => {
		const { batcher, calls } = readBatcher((page) => page.map(() => TOO_LARGE));

		const read = await FokosDBStd.batchGetItemsWithRetry(batcher, { items: keys }, { ...SHORT_RETRY, maxAttempts: 3 });

		expect(calls).toHaveLength(3);
		expect(read.unprocessedKeys).toEqual(keys);
	});

	it("uses the retry options of the batcher when the call gives none", async () => {
		const { batcher, calls } = readBatcher((page) => page.map(() => FAULT), { ...SHORT_RETRY, maxAttempts: 2 });

		await FokosDBStd.batchGetItemsWithRetry(batcher, { items: keys });

		expect(calls).toHaveLength(2);
	});
});
