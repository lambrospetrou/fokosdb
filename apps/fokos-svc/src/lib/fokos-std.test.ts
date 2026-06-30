import { describe, expect, it } from "vitest";
import { FokosStd } from "./fokos-std.js";
import { MAX_BATCH_FORWARDED_SUB_BATCH_BYTES, MAX_BATCH_GET_ITEMS, MAX_BATCH_WRITE_ITEMS } from "./transaction-limits.js";
import type {
	BatchGetItemsOptions,
	BatchGetItemsResult,
	BatchWriteItemOperation,
	BatchWriteItemsOptions,
	BatchWriteItemsResult,
	DeleteItemOptions,
	DeleteItemResult,
	FokosDBAPI,
	GetItemOptions,
	GetItemResult,
	ItemKey,
	OperationMetrics,
	PartitionInfo,
	PutItemOptions,
	PutItemResult,
} from "./types.js";

type PartitionMeta = OperationMetrics & PartitionInfo;

class FakeDB implements FokosDBAPI {
	batchGetCalls: BatchGetItemsOptions[] = [];
	batchWriteCalls: BatchWriteItemsOptions[] = [];
	#batchGetHandler: (opts: BatchGetItemsOptions, callIndex: number) => Promise<BatchGetItemsResult>;
	#batchWriteHandler: (opts: BatchWriteItemsOptions, callIndex: number) => Promise<BatchWriteItemsResult>;

	constructor(opts: {
		batchGetItems?: (opts: BatchGetItemsOptions, callIndex: number) => Promise<BatchGetItemsResult>;
		batchWriteItems?: (opts: BatchWriteItemsOptions, callIndex: number) => Promise<BatchWriteItemsResult>;
	}) {
		this.#batchGetHandler =
			opts.batchGetItems ?? (async () => ({ items: [], unprocessedKeys: [], meta: batchMeta(0, 0, 0), partitionMetas: [] }));
		this.#batchWriteHandler =
			opts.batchWriteItems ?? (async () => ({ processedItems: [], unprocessedItems: [], meta: batchMeta(0, 0, 0), partitionMetas: [] }));
	}

	async batchGetItems(opts: BatchGetItemsOptions): Promise<BatchGetItemsResult> {
		const callIndex = this.batchGetCalls.length;
		this.batchGetCalls.push(opts);
		return this.#batchGetHandler(opts, callIndex);
	}

	async batchWriteItems(opts: BatchWriteItemsOptions): Promise<BatchWriteItemsResult> {
		const callIndex = this.batchWriteCalls.length;
		this.batchWriteCalls.push(opts);
		return this.#batchWriteHandler(opts, callIndex);
	}

	putItem(_opts: PutItemOptions): Promise<PutItemResult> {
		throw new Error("unused");
	}

	getItem(_opts: GetItemOptions): Promise<GetItemResult> {
		throw new Error("unused");
	}

	deleteItem(_opts: DeleteItemOptions): Promise<DeleteItemResult> {
		throw new Error("unused");
	}
}

describe("FokosStd batch helpers", () => {
	it("batchGetAll chunks over native item-count limits and preserves global inputIndex values", async () => {
		const items = Array.from({ length: MAX_BATCH_GET_ITEMS + 1 }, (_, index) => ({ hashKey: `get-${index}` }));
		const db = new FakeDB({
			batchGetItems: async (opts, callIndex) => {
				return {
					items: opts.items.map((item, inputIndex) => ({
						inputIndex,
						found: true,
						item: { ...item, data: `data-${item.hashKey}`, version: 1 },
					})),
					unprocessedKeys: [],
					meta: batchMeta(opts.items.length, opts.items.length, 0, { rowsRead: opts.items.length }),
					partitionMetas: [partitionMeta(`get-${callIndex}`, { rowsRead: opts.items.length })],
				};
			},
		});

		const result = await new FokosStd(db, noWaitOptions()).batchGetAll({ items });

		expect(db.batchGetCalls.map((call) => call.items.length)).toEqual([MAX_BATCH_GET_ITEMS, 1]);
		expect(result.items.map((item) => item.inputIndex)).toEqual(items.map((_, index) => index));
		expect(result.unprocessedKeys).toEqual([]);
		expect(result.meta).toEqual({
			requestedCount: items.length,
			processedCount: items.length,
			unprocessedCount: 0,
			rowsRead: items.length,
			rowsWritten: 0,
			forwardCount: 0,
			partitionsVisited: 2,
		});
		expect(result.partitionMetas.map((meta) => meta.servedByActorName)).toEqual(["get-0", "get-1"]);
	});

	it("batchGetAll retries unprocessed keys and remaps retry-local indexes to original indexes", async () => {
		const sleeps: number[] = [];
		const db = new FakeDB({
			batchGetItems: async (opts, callIndex) => {
				if (callIndex === 0) {
					return {
						items: [
							{ inputIndex: 0, found: true, item: { ...opts.items[0], data: "first", version: 1 } },
							{ inputIndex: 1, found: true, item: { ...opts.items[1], data: "second", version: 1 } },
						],
						unprocessedKeys: [{ inputIndex: 2, item: opts.items[2], reason: { type: "transient_error", message: "retry me" } }],
						meta: batchMeta(3, 2, 1, { rowsRead: 2, forwardCount: 1 }),
						partitionMetas: [partitionMeta("get-first", { rowsRead: 2, forwardCount: 1 })],
					};
				}
				return {
					items: [{ inputIndex: 0, found: true, item: { ...opts.items[0], data: "third", version: 1 } }],
					unprocessedKeys: [],
					meta: batchMeta(1, 1, 0, { rowsRead: 1 }),
					partitionMetas: [partitionMeta("get-retry", { rowsRead: 1 })],
				};
			},
		});

		const result = await new FokosStd(db, noWaitOptions(sleeps)).batchGetAll({
			items: [{ hashKey: "first" }, { hashKey: "second" }, { hashKey: "third" }],
		});

		expect(sleeps).toEqual([10]);
		expect(db.batchGetCalls.map((call) => call.items.map((item) => item.hashKey))).toEqual([["first", "second", "third"], ["third"]]);
		expect(result.items.map((item) => item.inputIndex)).toEqual([0, 1, 2]);
		expect(result.unprocessedKeys).toEqual([]);
		expect(result.meta).toMatchObject({ requestedCount: 3, processedCount: 3, unprocessedCount: 0, rowsRead: 3, forwardCount: 1 });
		expect(result.partitionMetas.map((meta) => meta.servedByActorName)).toEqual(["get-first", "get-retry"]);
	});

	it("batchGetAll leaves exhausted unprocessed keys at their original indexes", async () => {
		const db = new FakeDB({
			batchGetItems: async (opts, callIndex) => ({
				items:
					callIndex === 0
						? [
								{ inputIndex: 0, found: true, item: { ...opts.items[0], data: "first", version: 1 } },
								{ inputIndex: 1, found: true, item: { ...opts.items[1], data: "second", version: 1 } },
							]
						: [],
				unprocessedKeys: [
					{
						inputIndex: callIndex === 0 ? 2 : 0,
						item: callIndex === 0 ? opts.items[2] : opts.items[0],
						reason: { type: "transient_error", message: `still retrying ${callIndex}` },
					},
				],
				meta: batchMeta(opts.items.length, callIndex === 0 ? 2 : 0, 1, { rowsRead: callIndex === 0 ? 2 : 0 }),
				partitionMetas: [partitionMeta(`get-${callIndex}`, { rowsRead: callIndex === 0 ? 2 : 0 })],
			}),
		});

		const result = await new FokosStd(db, noWaitOptions()).batchGetAll({
			items: [{ hashKey: "first" }, { hashKey: "second" }, { hashKey: "third" }],
		});

		expect(db.batchGetCalls).toHaveLength(2);
		expect(result.items.map((item) => item.inputIndex)).toEqual([0, 1]);
		expect(result.unprocessedKeys).toEqual([
			{ inputIndex: 2, item: { hashKey: "third" }, reason: { type: "transient_error", message: "still retrying 1" } },
		]);
		expect(result.meta).toMatchObject({ requestedCount: 3, processedCount: 2, unprocessedCount: 1, rowsRead: 2, partitionsVisited: 2 });
	});

	it("batchGetAll retries retryable thrown batch calls only", async () => {
		const retryable = Object.assign(new Error("temporary rpc failure"), { retryable: true });
		const db = new FakeDB({
			batchGetItems: async (opts, callIndex) => {
				if (callIndex === 0) throw retryable;
				return {
					items: [{ inputIndex: 0, found: true, item: { ...opts.items[0], data: "ok", version: 1 } }],
					unprocessedKeys: [],
					meta: batchMeta(1, 1, 0, { rowsRead: 1 }),
					partitionMetas: [partitionMeta("after-throw", { rowsRead: 1 })],
				};
			},
		});

		const result = await new FokosStd(db, noWaitOptions()).batchGetAll({ items: [{ hashKey: "retryable" }] });

		expect(db.batchGetCalls).toHaveLength(2);
		expect(result.items).toHaveLength(1);
		expect(result.unprocessedKeys).toEqual([]);

		const hardFailure = new FakeDB({
			batchGetItems: async () => {
				throw new Error("not retryable");
			},
		});
		await expect(new FokosStd(hardFailure, noWaitOptions()).batchGetAll({ items: [{ hashKey: "hard" }] })).rejects.toThrow(/not retryable/);
		expect(hardFailure.batchGetCalls).toHaveLength(1);
	});

	it("batchWriteAll chunks, retries unprocessed items, and does not add versions", async () => {
		const operations = Array.from({ length: MAX_BATCH_WRITE_ITEMS + 1 }, (_, index) => ({
			operation: "put" as const,
			hashKey: `write-${index}`,
			data: `data-${index}`,
		}));
		const db = new FakeDB({
			batchWriteItems: async (opts, callIndex) => {
				if (callIndex === 0) {
					return {
						processedItems: opts.operations.slice(0, -1).map((operation, inputIndex) => ({
							inputIndex,
							operation: operation.operation,
							item: { hashKey: operation.hashKey, sortKey: operation.sortKey },
						})),
						unprocessedItems: [
							{
								inputIndex: MAX_BATCH_WRITE_ITEMS - 1,
								operation: "put",
								item: { hashKey: `write-${MAX_BATCH_WRITE_ITEMS - 1}` },
								reason: { type: "pending_lock", conflictingTransactionId: "tx-1" },
							},
						],
						meta: batchMeta(MAX_BATCH_WRITE_ITEMS, MAX_BATCH_WRITE_ITEMS - 1, 1, { rowsWritten: MAX_BATCH_WRITE_ITEMS - 1 }),
						partitionMetas: [partitionMeta("write-first", { rowsWritten: MAX_BATCH_WRITE_ITEMS - 1 })],
					};
				}
				return {
					processedItems: opts.operations.map((operation, inputIndex) => ({
						inputIndex,
						operation: operation.operation,
						item: { hashKey: operation.hashKey, sortKey: operation.sortKey },
					})),
					unprocessedItems: [],
					meta: batchMeta(opts.operations.length, opts.operations.length, 0, { rowsWritten: opts.operations.length }),
					partitionMetas: [partitionMeta(`write-${callIndex}`, { rowsWritten: opts.operations.length })],
				};
			},
		});

		const result = await new FokosStd(db, noWaitOptions()).batchWriteAll({ operations });

		expect(db.batchWriteCalls.map((call) => call.operations.map((operation) => operation.hashKey))).toEqual([
			Array.from({ length: MAX_BATCH_WRITE_ITEMS }, (_, index) => `write-${index}`),
			[`write-${MAX_BATCH_WRITE_ITEMS}`],
			[`write-${MAX_BATCH_WRITE_ITEMS - 1}`],
		]);
		expect(result.processedItems.map((item) => item.inputIndex)).toEqual(operations.map((_, index) => index));
		expect(result.processedItems.some((item) => "version" in item)).toBe(false);
		expect(result.unprocessedItems).toEqual([]);
		expect(result.meta).toEqual({
			requestedCount: operations.length,
			processedCount: operations.length,
			unprocessedCount: 0,
			rowsRead: 0,
			rowsWritten: operations.length,
			forwardCount: 0,
			partitionsVisited: 3,
		});
		expect(result.partitionMetas.map((meta) => meta.servedByActorName)).toEqual(["write-first", "write-1", "write-2"]);
	});

	it("batchWriteAll chunks by native payload byte limit before calling the core API", async () => {
		const data = new Uint8Array(MAX_BATCH_FORWARDED_SUB_BATCH_BYTES - 128);
		const operations = Array.from({ length: 5 }, (_, index) => ({
			operation: "put" as const,
			hashKey: `large-write-${index}`,
			data,
		}));
		const db = new FakeDB({
			batchWriteItems: async (opts, callIndex) => ({
				processedItems: opts.operations.map((operation, inputIndex) => ({
					inputIndex,
					operation: operation.operation,
					item: { hashKey: operation.hashKey, sortKey: operation.sortKey },
				})),
				unprocessedItems: [],
				meta: batchMeta(opts.operations.length, opts.operations.length, 0, { rowsWritten: opts.operations.length }),
				partitionMetas: [partitionMeta(`large-write-${callIndex}`, { rowsWritten: opts.operations.length })],
			}),
		});

		const result = await new FokosStd(db, noWaitOptions()).batchWriteAll({ operations });

		expect(db.batchWriteCalls.map((call) => call.operations.length)).toEqual([4, 1]);
		expect(result.processedItems.map((item) => item.inputIndex)).toEqual([0, 1, 2, 3, 4]);
		expect(result.meta).toMatchObject({ requestedCount: 5, processedCount: 5, unprocessedCount: 0, rowsWritten: 5, partitionsVisited: 2 });
		expect(result.partitionMetas.map((meta) => meta.servedByActorName)).toEqual(["large-write-0", "large-write-1"]);
	});

	it("rejects duplicate keys across the full logical request before chunking", async () => {
		const db = new FakeDB({});
		const getItems = Array.from({ length: MAX_BATCH_GET_ITEMS + 1 }, (_, index) => ({ hashKey: `get-dupe-${index}` }));
		getItems[MAX_BATCH_GET_ITEMS] = { hashKey: "get-dupe-0" };

		await expect(new FokosStd(db, noWaitOptions()).batchGetAll({ items: getItems })).rejects.toThrow(/batchGetItems duplicate key/);
		expect(db.batchGetCalls).toEqual([]);

		const operations: BatchWriteItemOperation[] = Array.from({ length: MAX_BATCH_WRITE_ITEMS + 1 }, (_, index) => ({
			operation: "put" as const,
			hashKey: `write-dupe-${index}`,
			data: "x",
		}));
		operations[MAX_BATCH_WRITE_ITEMS] = { operation: "delete", hashKey: "write-dupe-0" };

		await expect(new FokosStd(db, noWaitOptions()).batchWriteAll({ operations })).rejects.toThrow(/batchWriteItems duplicate key/);
		expect(db.batchWriteCalls).toEqual([]);
	});
});

function noWaitOptions(sleeps: number[] = []) {
	return {
		maxAttempts: 2,
		baseDelayMs: 10,
		maxDelayMs: 100,
		random: () => 1,
		sleep: async (delayMs: number) => {
			sleeps.push(delayMs);
		},
	};
}

function batchMeta(
	requestedCount: number,
	processedCount: number,
	unprocessedCount: number,
	work: Partial<Pick<BatchGetItemsResult["meta"], "rowsRead" | "rowsWritten" | "forwardCount">> = {},
): BatchGetItemsResult["meta"] {
	return {
		requestedCount,
		processedCount,
		unprocessedCount,
		rowsRead: work.rowsRead ?? 0,
		rowsWritten: work.rowsWritten ?? 0,
		forwardCount: work.forwardCount ?? 0,
		partitionsVisited: 1,
	};
}

function partitionMeta(
	servedByActorName: string,
	work: Partial<Pick<PartitionMeta, "rowsRead" | "rowsWritten" | "forwardCount">> = {},
): PartitionMeta {
	return {
		rowsRead: work.rowsRead ?? 0,
		rowsWritten: work.rowsWritten ?? 0,
		databaseSize: 1024,
		servedByActorId: `actor-${servedByActorName}`,
		servedByActorName,
		servedByPartitionId: `partition-${servedByActorName}`,
		forwardCount: work.forwardCount ?? 0,
		hashDepth: 0,
	};
}
