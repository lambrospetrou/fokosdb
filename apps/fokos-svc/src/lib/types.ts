import { KeyBytes } from "./partition-topology/key-codec.js";

// ─── Item data kinds ────────────────────────────────────────────────────────────

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
// Top-level accepted composites only (start restricted; top-level primitives excluded initially).
export type JsonComposite = JsonValue[] | { [key: string]: JsonValue };

// ONE source of truth: the array. The on-disk `data_kind` column stores the compact integer code =
// the array index; the TS/public discriminant is the readable string literal. Both lookups are index
// math (`DATA_KINDS.indexOf(kind)` / `DATA_KINDS[code]`), so nothing can drift.
//
// ATTENTION: NEVER change the order of this array. The index is the on-disk code, so reordering would break existing data.
export const DATA_KINDS = ["bytes", "text", "json"] as const; // index = on-disk code
export type DataKind = (typeof DATA_KINDS)[number]; // "bytes" | "text" | "json"

// Encoded for the wire / store WRITE — JSON already stringified at the db.ts boundary, so the DO
// only ever sees `string | Uint8Array`. JSON text → store as jsonb(data)
export type EncodedItemData = { kind: "bytes"; data: Uint8Array } | { kind: "text"; data: string } | { kind: "json"; data: string };

// Decoded for public READ — json rebuilt at the db.ts boundary.
export type DecodedItemData = { kind: "bytes"; data: Uint8Array } | { kind: "text"; data: string } | { kind: "json"; data: JsonValue };

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
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;

	ttlSeconds?: number;
	ttlEpochUTCSeconds?: number;

	data: string | Uint8Array | JsonComposite;

	conditions?: ItemCondition[];
};

// Wire-IN type (db.ts → PartitionDO): the public `data` has been encoded to `string | Uint8Array`
// plus a `kind` discriminant, so the DO never sees a JS object.
export type EncodedPutItemOptions = Omit<PutItemOptions, "data"> & {
	data: string | Uint8Array;
	kind: DataKind;
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

type GetItemResultOf<D> =
	| {
			found: true;
			item: {
				hashKey: string | Uint8Array;
				sortKey?: string | Uint8Array;
				data: D;
				kind: DataKind;
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

// DO→db.ts RPC result: json arrives as JSON text, so data is `string | Uint8Array`. Deliberately free
// of the recursive JsonValue so the Workers-RPC type machinery does not instantiate infinitely deep.
export type GetItemResultEncoded = GetItemResultOf<string | Uint8Array>;

// Public result surfaced by FokosDB.getItem: db.ts has parsed json text into a JsonValue.
export type GetItemResult = GetItemResultOf<string | Uint8Array | JsonValue>;

export type PartitionInfo = {
	/**
	 * The DurableObjectId of the partition that served the request.
	 * Useful for correlating with partition topology information in logs,
	 * and to debug the underlying Durable Objects.
	 */
	servedByActorId: string;
	/**
	 * The human-readable name of the partition that served the request, if available.
	 */
	servedByActorName: string;
	/**
	 * Opaque identifier for the partition that served the request.
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
	 *
	 * ONLY FOR DEBUGGING PURPOSES: This is not a stable API and may change in future versions. Clients should not rely on this value for any logic.
	 */
	hashDepth: number;
	/**
	 * This range partition's own depth (root = 0). Always 0 for hash partitions (mirrors hashDepth's 0-for-range convention).
	 *
	 * ONLY FOR DEBUGGING PURPOSES: This is not a stable API and may change in future versions. Clients should not rely on this value for any logic.
	 */
	rangeDepth: number;

	////////////////////////////////////////////////////////////
	// INTERNAL_ONLY: Not to be exposed to the final responses.
	////////////////////////////////////////////////////////////

	_internal: {
		/**
		 * Bounded set of this range partition's ancestor boundaries (excludes root) including self (last).
		 * Always empty for hash partitions.
		 */
		rangeAncestors: RangeAncestorInfo[];
	};
};

/**
 * INTERNAL ONLY - Not to be exposed to the final responses.
 * Used to track the boundaries of ancestor range partitions for a given partition in the partition topology tree.
 * This information is useful for routing and debugging purposes, but should not be exposed to clients.
 * The boundaries are stored in their encoded form (KeyBytes) for efficient comparisons and storage.
 */
export type RangeAncestorInfo = {
	depth: number;
	startBoundary: KeyBytes;
	endBoundary: KeyBytes;
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
	InitiateReadResponseEncoded,
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
		data: string | Uint8Array | JsonValue;
		kind: DataKind;
		ttlEpochUTCSeconds?: number;
		version: number;
	}>;
	count: number;
	cursor?: string;
	meta: QueryItemsMeta;
	partitionMetas: Array<OperationMetrics & PartitionInfo>;
};
