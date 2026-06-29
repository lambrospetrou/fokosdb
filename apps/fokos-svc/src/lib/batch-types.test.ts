import { describe, expect, it } from "vitest";
import type { BatchGetItemsResult, BatchWriteItemsResult } from "./types.js";

const meta = {
	requestedCount: 2,
	processedCount: 1,
	unprocessedCount: 1,
	rowsRead: 3,
	rowsWritten: 4,
	forwardCount: 5,
	partitionsVisited: 1,
};

const partitionMeta = {
	rowsRead: 3,
	rowsWritten: 4,
	databaseSize: 1024,
	servedByActorId: "actor-id",
	servedByActorName: "actor-name",
	servedByPartitionId: "partition-id",
	forwardCount: 5,
	hashDepth: 0,
};

describe("batch item public result shapes", () => {
	it("correlates batchGet processed and unprocessed entries by inputIndex", () => {
		const result = {
			items: [{ inputIndex: 1, found: false, item: { hashKey: "missing" } }],
			unprocessedKeys: [{ inputIndex: 0, item: { hashKey: "retry" }, reason: { type: "transient_error" } }],
			meta,
			partitionMetas: [partitionMeta],
		} satisfies BatchGetItemsResult;

		type ProcessedHasInputIndex = "inputIndex" extends keyof BatchGetItemsResult["items"][number] ? true : false;
		type UnprocessedHasInputIndex = "inputIndex" extends keyof BatchGetItemsResult["unprocessedKeys"][number] ? true : false;
		type MetaHasPartitionsVisited = "partitionsVisited" extends keyof BatchGetItemsResult["meta"] ? true : false;
		type MetaHasPartitionCount = "partitionCount" extends keyof BatchGetItemsResult["meta"] ? true : false;
		const processedHasInputIndex: ProcessedHasInputIndex = true;
		const unprocessedHasInputIndex: UnprocessedHasInputIndex = true;
		const metaHasPartitionsVisited: MetaHasPartitionsVisited = true;
		const metaHasPartitionCount: MetaHasPartitionCount = false;

		expect(processedHasInputIndex).toBe(true);
		expect(unprocessedHasInputIndex).toBe(true);
		expect(metaHasPartitionsVisited).toBe(true);
		expect(metaHasPartitionCount).toBe(false);
		expect(result.items[0].inputIndex).toBe(1);
		expect(result.unprocessedKeys[0].inputIndex).toBe(0);
		expect(result.meta.partitionsVisited).toBe(1);
		expect(result.partitionMetas).toEqual([partitionMeta]);
	});

	it("correlates batchWrite entries by inputIndex and omits per-applied versions", () => {
		const result = {
			processedItems: [{ inputIndex: 1, operation: "put", item: { hashKey: "written" } }],
			unprocessedItems: [{ inputIndex: 0, operation: "delete", item: { hashKey: "retry" }, reason: { type: "pending_lock" } }],
			meta,
			partitionMetas: [partitionMeta],
		} satisfies BatchWriteItemsResult;

		type ProcessedHasInputIndex = "inputIndex" extends keyof BatchWriteItemsResult["processedItems"][number] ? true : false;
		type UnprocessedHasInputIndex = "inputIndex" extends keyof BatchWriteItemsResult["unprocessedItems"][number] ? true : false;
		type ProcessedHasVersion = "version" extends keyof BatchWriteItemsResult["processedItems"][number] ? true : false;
		type MetaHasPartitionsVisited = "partitionsVisited" extends keyof BatchWriteItemsResult["meta"] ? true : false;
		type MetaHasPartitionCount = "partitionCount" extends keyof BatchWriteItemsResult["meta"] ? true : false;
		const processedHasInputIndex: ProcessedHasInputIndex = true;
		const unprocessedHasInputIndex: UnprocessedHasInputIndex = true;
		const processedHasVersion: ProcessedHasVersion = false;
		const metaHasPartitionsVisited: MetaHasPartitionsVisited = true;
		const metaHasPartitionCount: MetaHasPartitionCount = false;

		expect(processedHasInputIndex).toBe(true);
		expect(unprocessedHasInputIndex).toBe(true);
		expect(processedHasVersion).toBe(false);
		expect(metaHasPartitionsVisited).toBe(true);
		expect(metaHasPartitionCount).toBe(false);
		expect(result.processedItems[0].inputIndex).toBe(1);
		expect("version" in result.processedItems[0]).toBe(false);
		expect(result.meta.partitionsVisited).toBe(1);
		expect(result.partitionMetas).toEqual([partitionMeta]);
	});
});
