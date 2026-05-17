export interface FokosDBAPI extends ItemPutter, ItemGetter {}

export interface ItemPutter {
	putItem(opts: PutItemOptions): Promise<PutItemResult>;
}

export type PutItemCondition =
	| { type: "item_not_exists" }
	| { type: "attribute_equals"; attribute: "v"; value: number };

export type PutItemOptions = {
	hashKey: string;
	sortKey?: string;
	ttlSeconds?: number;
	ttlEpochUTCSeconds?: number;

	data: Uint8Array | string;

	conditions?: PutItemCondition[];
};

export type PutItemResult = {
	version: number;
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

			hashKey: string;
			sortKey?: string;
			data: Uint8Array | string;
			ttlEpochUTCSeconds?: number;
			version: number;

			meta: OperationMetrics & PartitionInfo & {};
	  }
	| {
			found: false;

			meta: OperationMetrics & PartitionInfo & {};
	  };

export type PartitionInfo = {
	servedByActorId: string;
	servedByActorName: string;
	forwardCount: number;
};

export type OperationMetrics = {
	rowsRead: number;
	rowsWritten: number;
	databaseSize: number;
	timings?: {};
};
