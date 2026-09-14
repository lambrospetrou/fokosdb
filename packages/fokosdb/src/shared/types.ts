import type {
	InitiateReadResponse,
	InitiateWriteResponse,
	TransactGetItemsOptions,
	TransactWriteItemsOptions,
} from "./transaction-types.js";
import type { JsonComposite, JsonValue } from "./json-types.js";
import type { ConditionExpression, ProjectionExpression } from "./expression/types.js";
import type { ProjectedItem } from "./expression/projection.js";

// ─── Item data kinds ────────────────────────────────────────────────────────────

export type { JsonComposite, JsonPrimitive, JsonValue } from "./json-types.js";

export { EXPRESSION_LIMITS } from "./expression/limits.js";
export type { ExpressionLimitName } from "./expression/limits.js";
export { EXPRESSION_NATIVE_TYPES } from "./expression/types.js";
export type {
	ConditionExpression,
	ExpressionNativeType,
	ExpressionReference,
	ExpressionValue,
	ProjectionExpression,
	UpdateAction,
	UpdateExpression,
	UpdateTarget,
} from "./expression/types.js";
export type { ProjectedItem, ProjectedValue } from "./expression/projection.js";

// ONE source of truth: the array. The on-disk `data_kind` column stores the compact integer code =
// the array index; the TS/public discriminant is the readable string literal. Both lookups are index
// math (`DATA_KINDS.indexOf(kind)` / `DATA_KINDS[code]`), so nothing can drift.
//
// ATTENTION: NEVER change the order of this array. The index is the on-disk code, so reordering would break existing data.
export const DATA_KINDS = ["bytes", "text", "json"] as const; // index = on-disk code
export type DataKind = (typeof DATA_KINDS)[number]; // "bytes" | "text" | "json"

export type ReturnValuesOnConditionCheckFailure = "none" | "all_old";

export type ConditionCheckImageOf<D> = {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;
	data: D;
	kind: DataKind;
	version: number;
	/** Epoch UTC seconds. Absent when the item has no expiry instant. */
	ttlAt?: number;
};

/** Wire variant. json data is JSON text, which db.ts parses once at the public boundary. */
export type ConditionCheckImageEncoded = ConditionCheckImageOf<string | Uint8Array>;

/** Public variant, surfaced by db.ts. */
export type ConditionCheckImage = ConditionCheckImageOf<string | Uint8Array | JsonValue>;

// Encoded for the wire / store WRITE — JSON already stringified at the db.ts boundary, so the DO
// only ever sees `string | Uint8Array`. JSON text → store as jsonb(data)
export type EncodedItemData = { kind: "bytes"; data: Uint8Array } | { kind: "text"; data: string } | { kind: "json"; data: string };

// Decoded for public READ — json rebuilt at the db.ts boundary.
export type DecodedItemData = { kind: "bytes"; data: Uint8Array } | { kind: "text"; data: string } | { kind: "json"; data: JsonValue };

export interface FokosDBAPI extends ItemPutter, ItemGetter, ItemDeleter, ItemQuerier, ItemTransactor {}

export interface ItemPutter {
	putItem(opts: PutItemOptions): Promise<PutItemResult>;
}

export interface ItemDeleter {
	deleteItem(opts: DeleteItemOptions): Promise<DeleteItemResult>;
}

export type PutItemOptions = {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;

	/** Epoch UTC seconds. Reads can return the item after this instant until background deletion. */
	ttlAt?: number;

	data: string | Uint8Array | JsonComposite;

	condition?: ConditionExpression;

	returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;
};

export type DeleteItemOptions = {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;

	condition?: ConditionExpression;

	returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;
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

// Public result surfaced by FokosDB.getItem. The keys are the caller's own, and db.ts has parsed json
// text into a JsonValue. The DO's counterpart is GetItemRpcResponse, which carries no keys at all.
export type GetItemResult =
	| {
			found: true;
			item: {
				hashKey: string | Uint8Array;
				sortKey?: string | Uint8Array;
				data: string | Uint8Array | JsonValue;
				kind: DataKind;
				/** Epoch UTC seconds. The item can remain visible after this instant until background deletion. */
				ttlAt?: number;
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
	 * The depth of the hash partition in the topology tree. A root partition has depth 0, its children
	 * have depth 1, and so on. It is 0 for a range partition, which has no depth in the hash tree.
	 *
	 * FOR DEBUGGING ONLY: this is not a stable API. A client must not use the value in its logic.
	 */
	hashDepth: number;
	/**
	 * The depth of the range partition in its own tree. A root has depth 0. It is 0 for a hash
	 * partition, which mirrors the convention of hashDepth.
	 *
	 * FOR DEBUGGING ONLY: this is not a stable API. A client must not use the value in its logic.
	 */
	rangeDepth: number;
};

export type OperationMetrics = {
	rowsRead: number;
	rowsWritten: number;
	databaseSize: number;
	timings?: {};
};

export interface ItemTransactor {
	transactWriteItems(opts: TransactWriteItemsOptions): Promise<InitiateWriteResponse>;
	transactGetItems(opts: TransactGetItemsOptions): Promise<InitiateReadResponse>;
}

export type {
	InitiateWriteRequest,
	InitiateWriteResponse,
	InitiateReadRequest,
	InitiateReadResponseEncoded,
	InitiateReadResponse,
	TCWriteOperation,
	TCReadItem,
	TransactWriteItem,
	TransactWriteItemsOptions,
	TransactWriteOperationResult,
	RejectionReason,
	TransactGetItemsOptions,
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

export interface ItemQuerier {
	queryItems(opts: QueryItemsProjectedOptions): Promise<QueryItemsProjectedResult>;
	queryItems(opts: QueryItemsOptions): Promise<QueryItemsResult>;
}

/** The selection of a queryItems page: materialized items, or the matched count only. */
export type QuerySelect = "projection" | "count";

// The field names what the list contains: `queries` here, `items` on the two transaction methods.
export type QueryItemsOptions = {
	queries: Array<{ hashKey: string | Uint8Array; sortKeyCondition?: SortKeyCondition; scanIndexForward?: boolean }>;
	/** Evaluated items per page. Defaults to DEFAULT_EVALUATED_ITEMS_PER_PAGE, clamped to MAX_EVALUATED_ITEMS_PER_PAGE. */
	limit?: number;
	/** Materialized item bytes per page. Defaults to DEFAULT_RESPONSE_BYTES_PER_PAGE, clamped to MAX_RESPONSE_BYTES_PER_PAGE. */
	maxResponseBytes?: number;
	cursor?: string;
	/** Defaults to "projection". "count" returns `items: []` and the matched count of one page. */
	select?: QuerySelect;
	/**
	 * SQLite evaluates the filter on every candidate. It does not change candidate selection, routing, or the
	 * sort-key interval. A rejected candidate still consumes the page's evaluated budgets and advances the
	 * cursor. Valid with `select: "count"`, which then returns the matched count of the page.
	 */
	filter?: ConditionExpression;
	/** A projection returns a flat record per item in place of the complete item. Not valid with `select: "count"`. */
	projection?: readonly ProjectionExpression[];
};

/** A `queryItems` request that carries a projection. The projected overload resolves on this type. */
export type QueryItemsProjectedOptions = QueryItemsOptions & {
	projection: readonly ProjectionExpression[];
	select?: "projection";
};

/** The `queryItems` result of a projected request: one flat record per item, by resolved name. */
export type QueryItemsProjectedResult = Omit<QueryItemsResult, "items"> & { items: ProjectedItem[] };

export type QueryItemsMeta = {
	/** Physical SQLite rows read by the leaf query statements. */
	rowsRead: number;
	/** SQL result rows the leaf collectors consumed in JavaScript. */
	rowsReturned: number;
	forwardCount: number;
	partitionsVisited: number;
};

// Public result surfaced by FokosDB.queryItems. The keys are decoded back to the caller's own form and
// db.ts has parsed json text into a JsonValue. The DO's counterpart is QueryItemsRpcResponse, which
// carries raw key bytes and the stored data representation.
export type QueryItemsResult = {
	items: Array<{
		hashKey: string | Uint8Array;
		sortKey?: string | Uint8Array;
		data: string | Uint8Array | JsonValue;
		kind: DataKind;
		/** Epoch UTC seconds. The item can remain visible after this instant until background deletion. */
		ttlAt?: number;
		version: number;
	}>;
	/** Matched items in this page. */
	count: number;
	/** Evaluated items in this page. Equal to `count` for a request with no filter; with a filter, `count <= scannedCount`. */
	scannedCount: number;
	cursor?: string;
	meta: QueryItemsMeta;
	partitionMetas: Array<OperationMetrics & PartitionInfo>;
};
