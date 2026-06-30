export interface FokosDBAPI extends ItemPutter, ItemGetter, ItemDeleter, BatchItemGetter, BatchItemWriter {}

export interface ItemPutter {
	putItem(opts: PutItemOptions): Promise<PutItemResult>;
}

export interface ItemDeleter {
	deleteItem(opts: DeleteItemOptions): Promise<DeleteItemResult>;
}

export type ItemCondition =
	| { type: "item_exists" }
	| { type: "item_not_exists" }
	| { type: "attribute_equals"; attribute: "v"; value: number };

export type PutItemOptions = {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;
	ttlSeconds?: number;
	ttlEpochUTCSeconds?: number;

	data: Uint8Array | string;

	conditions?: ItemCondition[];
};

export type DeleteItemOptions = {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;

	conditions?: ItemCondition[];
};

export type ItemKey = {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;
};

export type PutItemResult = {
	item: ItemKey;
	version: number;
	meta: OperationMetrics & PartitionInfo & {};
};

export type DeleteItemResult = {
	item: ItemKey;
	deleted: boolean;
	meta: OperationMetrics & PartitionInfo & {};
};

export interface ItemGetter {
	getItem(opts: GetItemOptions): Promise<GetItemResult>;
}

export type GetItemOptions = {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;
};

export type GetItemResult =
	| {
			found: true;
			item: {
				hashKey: string | Uint8Array;
				sortKey?: string | Uint8Array;
				data: Uint8Array | string;
				ttlEpochUTCSeconds?: number;
				version: number;
			};
			meta: OperationMetrics & PartitionInfo & {};
	  }
	| {
			found: false;
			item: ItemKey;
			meta: OperationMetrics & PartitionInfo & {};
	  };

export interface BatchItemGetter {
	batchGetItems(opts: BatchGetItemsOptions): Promise<BatchGetItemsResult>;
}

export interface BatchItemWriter {
	batchWriteItems(opts: BatchWriteItemsOptions): Promise<BatchWriteItemsResult>;
}

export type BatchGetItemsOptions = {
	items: ItemKey[];
};

export type BatchWriteItemOperation =
	| {
			operation: "put";
			hashKey: string | Uint8Array;
			sortKey?: string | Uint8Array;
			ttlSeconds?: number;
			ttlEpochUTCSeconds?: number;
			data: Uint8Array | string;
	  }
	| {
			operation: "delete";
			hashKey: string | Uint8Array;
			sortKey?: string | Uint8Array;
	  };

export type BatchWriteItemsOptions = {
	operations: BatchWriteItemOperation[];
};

export type BatchRetryableFailureReason =
	| { type: "pending_lock"; conflictingTransactionId?: string }
	| { type: "partition_over_limit" }
	| { type: "transient_error"; message?: string };

export type BatchItemsMeta = {
	requestedCount: number;
	processedCount: number;
	unprocessedCount: number;
	rowsRead: number;
	rowsWritten: number;
	forwardCount: number;
	partitionsVisited: number;
};

export type BatchGetProcessedItem =
	| {
			inputIndex: number;
			found: true;
			item: {
				hashKey: string | Uint8Array;
				sortKey?: string | Uint8Array;
				data: Uint8Array | string;
				ttlEpochUTCSeconds?: number;
				version: number;
			};
	  }
	| {
			inputIndex: number;
			found: false;
			item: ItemKey;
	  };

export type BatchGetUnprocessedKey = {
	inputIndex: number;
	item: ItemKey;
	reason: BatchRetryableFailureReason;
};

export type BatchGetItemsResult = {
	items: BatchGetProcessedItem[];
	unprocessedKeys: BatchGetUnprocessedKey[];
	meta: BatchItemsMeta;
	partitionMetas: Array<OperationMetrics & PartitionInfo>;
};

export type BatchWriteProcessedItem = {
	inputIndex: number;
	operation: BatchWriteItemOperation["operation"];
	item: ItemKey;
};

export type BatchWriteUnprocessedItem = {
	inputIndex: number;
	operation: BatchWriteItemOperation["operation"];
	item: ItemKey;
	reason: BatchRetryableFailureReason;
};

export type BatchWriteItemsResult = {
	processedItems: BatchWriteProcessedItem[];
	unprocessedItems: BatchWriteUnprocessedItem[];
	meta: BatchItemsMeta;
	partitionMetas: Array<OperationMetrics & PartitionInfo>;
};

export type PartitionInfo = {
	/**
	 * The DurableObjectId of the partition that served the request.
	 * Useful for correlating with partition topology information in logs,
	 * and to debug the underlying Durable Objects.
	 */
	servedByActorId: string;
	/** The human-readable name of the partition that served the request, if available. */
	servedByActorName: string;
	/** Opaque identifier for the partition that served the request.
	 * Useful for correlating with partition topology information in logs, but not meaningful to clients.
	 */
	servedByPartitionId: string;
	/**
	 * The number of times the request was forwarded between partitions before reaching the final partition that served it.
	 */
	forwardCount: number;
	/**
	 * The depth of the hash partition in the partition topology tree. Root partitions have depth 0, their children have depth 1, and so on.
	 * Will be zero for range partitions since they don't have a depth in the hash partition tree,
	 * but can be useful for debugging and monitoring the partition topology.
	 */
	hashDepth: number;
};

export type OperationMetrics = {
	rowsRead: number;
	rowsWritten: number;
	databaseSize: number;
	timings?: {};
};

export type {
	InitiateWriteRequest,
	InitiateWriteResponse,
	InitiateReadRequest,
	InitiateReadResponse,
	TCWriteOperation,
	TCReadItem,
} from "./transaction-types.js";

// ─── queryItems public API ────────────────────────────────────────────────────

export type SortKeyCondition =
	| { op: "eq"; value: string | Uint8Array }
	| { op: "lt" | "lte" | "gt" | "gte"; value: string | Uint8Array }
	| { op: "between"; lower: string | Uint8Array; upper: string | Uint8Array }
	| { op: "begins_with"; prefix: string | Uint8Array }
	| {
			op: "range";
			lower?: { value: string | Uint8Array; inclusive: boolean };
			upper?: { value: string | Uint8Array; inclusive: boolean };
	  };

export type QueryItemsOptions = {
	queries: Array<{ hashKey: string | Uint8Array; sort?: SortKeyCondition; scanIndexForward?: boolean }>;
	limit?: number;
	maxPageBytes?: number;
	cursor?: string;
};

export type QueryItemsMeta = {
	rowsRead: number;
	rowsReturned: number;
	forwardCount: number;
	partitionsVisited: number;
};

export type QueryItemsResult = {
	// FIXME: `data` is the raw stored representation (string | Uint8Array) — the HTTP layer
	// re-encodes it via `encodeData`. Consider aligning this type with the wire format or
	// introducing a separate HTTP response type.
	items: Array<{
		hashKey: string | Uint8Array;
		sortKey?: string | Uint8Array;
		data: string | Uint8Array;
		ttlEpochUTCSeconds?: number;
		version: number;
	}>;
	count: number;
	cursor?: string;
	meta: QueryItemsMeta;
	partitionMetas: Array<OperationMetrics & PartitionInfo>;
};
