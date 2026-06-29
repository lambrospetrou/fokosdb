import type {
	BatchGetProcessedItem,
	BatchGetUnprocessedKey,
	BatchWriteProcessedItem,
	BatchWriteUnprocessedItem,
	OperationMetrics,
	PartitionInfo,
} from "./types.js";
import type { KeyBytes } from "./partition-topology/key-codec.js";

export type BatchGetRpcItem = {
	inputIndex: number;
	hashKey: KeyBytes;
	sortKey: KeyBytes;
};

export type BatchGetItemsRpcRequest = {
	items: BatchGetRpcItem[];
};

export type BatchGetItemsRpcResult = {
	items: BatchGetProcessedItem[];
	unprocessedKeys: BatchGetUnprocessedKey[];
	meta: OperationMetrics & PartitionInfo;
	partitionMetas: Array<OperationMetrics & PartitionInfo>;
};

export type BatchWriteRpcOperation =
	| {
			inputIndex: number;
			operation: "put";
			hashKey: KeyBytes;
			sortKey: KeyBytes;
			data: Uint8Array | string;
			ttlSeconds?: number;
			ttlEpochUTCSeconds?: number;
	  }
	| {
			inputIndex: number;
			operation: "delete";
			hashKey: KeyBytes;
			sortKey: KeyBytes;
	  };

export type BatchWriteItemsRpcRequest = {
	operations: BatchWriteRpcOperation[];
};

export type BatchWriteItemsRpcResult = {
	processedItems: BatchWriteProcessedItem[];
	unprocessedItems: BatchWriteUnprocessedItem[];
	meta: OperationMetrics & PartitionInfo;
	partitionMetas: Array<OperationMetrics & PartitionInfo>;
};
