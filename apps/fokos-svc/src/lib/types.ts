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
	meta: OperationMetrics & {};
	__debug?: DebugInfo;
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

			meta: OperationMetrics & {};

			__debug?: DebugInfo;
	  }
	| {
			found: false;

			__debug?: DebugInfo;
	  };

export type OperationMetrics = {
	rowsRead: number;
	rowsWritten: number;
	databaseSize: number;
	timings?: {
		total: number;
	};
};

export type DebugInfo = {
	splitStatus?: Record<string, any>;
};
