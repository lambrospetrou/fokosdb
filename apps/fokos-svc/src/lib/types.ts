export interface FokosDBAPI extends ItemPutter, ItemGetter {}

export interface ItemPutter {
	putItem(opts: PutItemOptions): Promise<PutItemResult>;
}

export type PutItemOptions = {
	// TODO Add options for conditional puts, TTL, etc.
	hashKey: string;
	sortKey?: string;
	ttlSeconds?: number;
	ttlEpochUTCSeconds?: number;

	data: Uint8Array | string;
};

export type PutItemResult = {
	version: number;
	forwarded: number;
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
			forwarded: number;

			meta: OperationMetrics & PartitionInfo & {};
	  }
	| {
			found: false;
			forwarded: number;

			meta: OperationMetrics & PartitionInfo & {};
	  };

export type PartitionInfo = {
	servedByActorId: string;
	servedByActorName: string;
};

export type OperationMetrics = {
	rowsRead: number;
	rowsWritten: number;
	databaseSize: number;
	timings?: {
		total: number;
	};
};

