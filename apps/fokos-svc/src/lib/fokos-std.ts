import { isErrorRetryable } from "durable-utils/do-utils";
import {
	encodedPairIdentity,
	estimateEncodedKeyPayloadBytes,
	keyForError,
	MAX_BATCH_GET_ITEMS,
	MAX_BATCH_PAYLOAD_BYTES,
	MAX_BATCH_WRITE_ITEMS,
	payloadBytes,
	validateBatchGetItems,
	validateBatchWriteOperations,
} from "./transaction-limits.js";
import type {
	BatchGetItemsOptions,
	BatchGetItemsResult,
	BatchGetProcessedItem,
	BatchGetUnprocessedKey,
	BatchItemsMeta,
	BatchWriteItemOperation,
	BatchWriteItemsOptions,
	BatchWriteItemsResult,
	BatchWriteProcessedItem,
	BatchWriteUnprocessedItem,
	FokosDBAPI,
	ItemKey,
	OperationMetrics,
	PartitionInfo,
} from "./types.js";

export type FokosStdOptions = {
	maxAttempts?: number;
	baseDelayMs?: number;
	maxDelayMs?: number;
	sleep?: (delayMs: number) => Promise<void>;
	random?: () => number;
	isRetryableError?: (error: unknown) => boolean;
};

type BatchGetPending = {
	inputIndex: number;
	item: ItemKey;
	reason?: BatchGetUnprocessedKey["reason"];
};

type BatchWritePending = {
	inputIndex: number;
	operation: BatchWriteItemOperation;
	reason?: BatchWriteUnprocessedItem["reason"];
};

type FokosStdResolvedOptions = Required<FokosStdOptions>;

const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_BASE_DELAY_MS = 25;
const DEFAULT_MAX_DELAY_MS = 1_000;

export class FokosStd {
	#db: FokosDBAPI;
	#options: FokosStdResolvedOptions;

	constructor(db: FokosDBAPI, options: FokosStdOptions = {}) {
		const maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
		if (!Number.isSafeInteger(maxAttempts) || maxAttempts < 1) {
			throw new Error("fokos/std: maxAttempts must be an integer greater or equal to 1");
		}
		const baseDelayMs = options.baseDelayMs ?? DEFAULT_BASE_DELAY_MS;
		const maxDelayMs = options.maxDelayMs ?? DEFAULT_MAX_DELAY_MS;
		if (!Number.isFinite(baseDelayMs) || baseDelayMs < 0) {
			throw new Error("fokos/std: baseDelayMs must be a finite non-negative number");
		}
		if (!Number.isFinite(maxDelayMs) || maxDelayMs < 0) {
			throw new Error("fokos/std: maxDelayMs must be a finite non-negative number");
		}
		this.#db = db;
		this.#options = {
			maxAttempts,
			baseDelayMs,
			maxDelayMs,
			sleep: options.sleep ?? defaultSleep,
			random: options.random ?? Math.random,
			isRetryableError: options.isRetryableError ?? isErrorRetryable,
		};
	}

	async batchGetAll(opts: BatchGetItemsOptions): Promise<BatchGetItemsResult> {
		validateBatchGetAllInput(opts.items);
		let pending: BatchGetPending[] = opts.items.map((item, inputIndex) => ({ inputIndex, item }));
		const items: BatchGetProcessedItem[] = [];
		const partitionMetas: Array<OperationMetrics & PartitionInfo> = [];
		const work = emptyWorkMeta(opts.items.length);

		for (let attempt = 1; attempt <= this.#options.maxAttempts && pending.length > 0; attempt++) {
			const nextPending: BatchGetPending[] = [];
			for (const chunk of chunkBatchGetPending(pending)) {
				const localItems = chunk.map((entry) => entry.item);
				try {
					const result = await this.#db.batchGetItems({ items: localItems });
					addWork(work, result.meta);
					partitionMetas.push(...result.partitionMetas);
					for (const item of result.items) {
						const entry = chunk[item.inputIndex];
						items.push(remapBatchGetProcessedItem(item, entry));
					}
					for (const item of result.unprocessedKeys) {
						const entry = chunk[item.inputIndex];
						nextPending.push({ inputIndex: entry.inputIndex, item: entry.item, reason: item.reason });
					}
				} catch (error) {
					if (!this.#options.isRetryableError(error)) throw error;
					const reason = retryableErrorReason(error);
					nextPending.push(...chunk.map((entry) => ({ ...entry, reason })));
				}
			}
			pending = nextPending;
			if (pending.length > 0 && attempt < this.#options.maxAttempts) {
				await this.#options.sleep(this.#backoffDelayMs(attempt));
			}
		}

		items.sort(compareInputIndex);
		const unprocessedKeys = pending.map((entry) => batchGetUnprocessedFromPending(entry)).sort(compareInputIndex);
		return {
			items,
			unprocessedKeys,
			meta: finalizeMeta(work, opts.items.length, items.length, unprocessedKeys.length, partitionMetas.length),
			partitionMetas,
		};
	}

	async batchWriteAll(opts: BatchWriteItemsOptions): Promise<BatchWriteItemsResult> {
		validateBatchWriteAllInput(opts.operations);
		let pending: BatchWritePending[] = opts.operations.map((operation, inputIndex) => ({ inputIndex, operation }));
		const processedItems: BatchWriteProcessedItem[] = [];
		const partitionMetas: Array<OperationMetrics & PartitionInfo> = [];
		const work = emptyWorkMeta(opts.operations.length);

		for (let attempt = 1; attempt <= this.#options.maxAttempts && pending.length > 0; attempt++) {
			const nextPending: BatchWritePending[] = [];
			for (const chunk of chunkBatchWritePending(pending)) {
				const operations = chunk.map((entry) => entry.operation);
				try {
					const result = await this.#db.batchWriteItems({ operations });
					addWork(work, result.meta);
					partitionMetas.push(...result.partitionMetas);
					for (const item of result.processedItems) {
						const entry = chunk[item.inputIndex];
						processedItems.push(remapBatchWriteProcessedItem(item, entry));
					}
					// Retry only entries the core reported unprocessed. Retrying writes is safe
					// but not version-idempotent: a re-applied put/delete can bump versions again.
					for (const item of result.unprocessedItems) {
						const entry = chunk[item.inputIndex];
						nextPending.push({ inputIndex: entry.inputIndex, operation: entry.operation, reason: item.reason });
					}
				} catch (error) {
					if (!this.#options.isRetryableError(error)) throw error;
					const reason = retryableErrorReason(error);
					nextPending.push(...chunk.map((entry) => ({ ...entry, reason })));
				}
			}
			pending = nextPending;
			if (pending.length > 0 && attempt < this.#options.maxAttempts) {
				await this.#options.sleep(this.#backoffDelayMs(attempt));
			}
		}

		processedItems.sort(compareInputIndex);
		const unprocessedItems = pending.map((entry) => batchWriteUnprocessedFromPending(entry)).sort(compareInputIndex);
		return {
			processedItems,
			unprocessedItems,
			meta: finalizeMeta(work, opts.operations.length, processedItems.length, unprocessedItems.length, partitionMetas.length),
			// Ordered visit trail across chunks and retries. This intentionally is not de-duplicated:
			// retry work and multi-chunk work are operationally meaningful to callers.
			partitionMetas,
		};
	}

	#backoffDelayMs(attempt: number): number {
		const cap = Math.min(this.#options.maxDelayMs, this.#options.baseDelayMs * 2 ** (attempt - 1));
		const random = this.#options.random();
		const jitter = Number.isFinite(random) ? Math.max(0, Math.min(1, random)) : 0;
		return Math.floor(cap * jitter);
	}
}

function validateBatchGetAllInput(items: readonly ItemKey[]): void {
	const seen = new Set<string>();
	for (const item of items) {
		validateBatchGetItems([item]);
		const identity = encodedPairIdentity(item);
		if (seen.has(identity)) {
			const { hashKey, sortKey } = keyForError(item);
			throw new Error(`fokos: batchGetItems duplicate key (${hashKey}, ${sortKey})`);
		}
		seen.add(identity);
	}
	if (items.length === 0) {
		validateBatchGetItems(items);
	}
}

function validateBatchWriteAllInput(operations: readonly BatchWriteItemOperation[]): void {
	const seen = new Set<string>();
	for (const operation of operations) {
		validateBatchWriteOperations([operation]);
		const identity = encodedPairIdentity(operation);
		if (seen.has(identity)) {
			const { hashKey, sortKey } = keyForError(operation);
			throw new Error(`fokos: batchWriteItems duplicate key (${hashKey}, ${sortKey})`);
		}
		seen.add(identity);
	}
	if (operations.length === 0) {
		validateBatchWriteOperations(operations);
	}
}

function chunkBatchGetPending(pending: readonly BatchGetPending[]): BatchGetPending[][] {
	return chunkByLimits(pending, MAX_BATCH_GET_ITEMS, (entry) => batchGetPayloadBytes(entry.item));
}

function chunkBatchWritePending(pending: readonly BatchWritePending[]): BatchWritePending[][] {
	return chunkByLimits(pending, MAX_BATCH_WRITE_ITEMS, (entry) => batchWritePayloadBytes(entry.operation));
}

function chunkByLimits<T>(entries: readonly T[], maxCount: number, bytesForEntry: (entry: T) => number): T[][] {
	const chunks: T[][] = [];
	let current: T[] = [];
	let currentBytes = 0;
	const flush = () => {
		if (current.length === 0) return;
		chunks.push(current);
		current = [];
		currentBytes = 0;
	};

	for (const entry of entries) {
		const entryBytes = bytesForEntry(entry);
		if (entryBytes > MAX_BATCH_PAYLOAD_BYTES) {
			throw new Error(`fokos/std: single batch entry exceeds ${MAX_BATCH_PAYLOAD_BYTES / (1024 * 1024)} MB payload limit`);
		}
		if (current.length >= maxCount || (current.length > 0 && currentBytes + entryBytes > MAX_BATCH_PAYLOAD_BYTES)) {
			flush();
		}
		current.push(entry);
		currentBytes += entryBytes;
	}
	flush();
	return chunks;
}

function batchGetPayloadBytes(item: ItemKey): number {
	return estimateEncodedKeyPayloadBytes(item);
}

function batchWritePayloadBytes(operation: BatchWriteItemOperation): number {
	return estimateEncodedKeyPayloadBytes(operation) + payloadBytes(operation.operation === "put" ? operation.data : undefined);
}

function remapBatchGetProcessedItem(item: BatchGetProcessedItem, entry: BatchGetPending): BatchGetProcessedItem {
	if (!item.found) {
		return { inputIndex: entry.inputIndex, found: false, item: entry.item };
	}
	return {
		inputIndex: entry.inputIndex,
		found: true,
		item: { ...item.item, hashKey: entry.item.hashKey, sortKey: entry.item.sortKey },
	};
}

function remapBatchWriteProcessedItem(item: BatchWriteProcessedItem, entry: BatchWritePending): BatchWriteProcessedItem {
	return {
		inputIndex: entry.inputIndex,
		operation: item.operation,
		item: { hashKey: entry.operation.hashKey, sortKey: entry.operation.sortKey },
	};
}

function batchGetUnprocessedFromPending(entry: BatchGetPending): BatchGetUnprocessedKey {
	return {
		inputIndex: entry.inputIndex,
		item: entry.item,
		reason: entry.reason ?? { type: "transient_error", message: "fokos/std: retry attempts exhausted" },
	};
}

function batchWriteUnprocessedFromPending(entry: BatchWritePending): BatchWriteUnprocessedItem {
	return {
		inputIndex: entry.inputIndex,
		operation: entry.operation.operation,
		item: { hashKey: entry.operation.hashKey, sortKey: entry.operation.sortKey },
		reason: entry.reason ?? { type: "transient_error", message: "fokos/std: retry attempts exhausted" },
	};
}

function retryableErrorReason(error: unknown): BatchGetUnprocessedKey["reason"] {
	return {
		type: "transient_error",
		message: error instanceof Error ? error.message : String(error),
	};
}

function emptyWorkMeta(requestedCount: number): BatchItemsMeta {
	return {
		requestedCount,
		processedCount: 0,
		unprocessedCount: requestedCount,
		rowsRead: 0,
		rowsWritten: 0,
		forwardCount: 0,
		partitionsVisited: 0,
	};
}

function addWork(target: BatchItemsMeta, source: BatchItemsMeta): void {
	target.rowsRead += source.rowsRead;
	target.rowsWritten += source.rowsWritten;
	target.forwardCount += source.forwardCount;
}

function finalizeMeta(
	work: BatchItemsMeta,
	requestedCount: number,
	processedCount: number,
	unprocessedCount: number,
	partitionsVisited: number,
): BatchItemsMeta {
	return {
		requestedCount,
		processedCount,
		unprocessedCount,
		rowsRead: work.rowsRead,
		rowsWritten: work.rowsWritten,
		forwardCount: work.forwardCount,
		partitionsVisited,
	};
}

function compareInputIndex<T extends { inputIndex: number }>(a: T, b: T): number {
	return a.inputIndex - b.inputIndex;
}

function defaultSleep(delayMs: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, delayMs));
}
