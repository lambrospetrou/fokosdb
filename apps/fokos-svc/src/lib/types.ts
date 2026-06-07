export interface FokosDBAPI extends ItemPutter, ItemGetter, ItemDeleter {}

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
	hashKey: string;
	sortKey?: string;
	ttlSeconds?: number;
	ttlEpochUTCSeconds?: number;

	data: Uint8Array | string;

	conditions?: ItemCondition[];
};

export type DeleteItemOptions = {
	hashKey: string;
	sortKey?: string;

	conditions?: ItemCondition[];
};

export type ItemKey = {
	hashKey: string;
	sortKey?: string;
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
	hashKey: string;
	sortKey?: string;
};

export type GetItemResult =
	| {
			found: true;
			item: {
				hashKey: string;
				sortKey?: string;
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
