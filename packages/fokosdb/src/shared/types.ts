import type {
	TransactGetItemsResult,
	TransactWriteItemsResult,
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

/**
 * The type a read gives to its json value and to its projected record: the caller's own `T` when the
 * method names one, and the widest type the library can return when it does not.
 */
export type CallerType<T, Widest> = [T] extends [never] ? Widest : T;

// Decoded for public READ — json rebuilt at the db.ts boundary.
export type DecodedItemData<T = never> =
	| { kind: "bytes"; data: Uint8Array }
	| { kind: "text"; data: string }
	| { kind: "json"; data: CallerType<T, JsonValue> };

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

/**
 * The value half of a read result. `kind` discriminates it, so a caller that tests `kind` also gets
 * the type of `data`. `"projected"` is a read-result tag and never a `DataKind`: that array indexes
 * the on-disk `data_kind` code, and no item is stored projected.
 */
export type ReadItemValue<T = never> = DecodedItemData<T> | { kind: "projected"; data: CallerType<T, ProjectedItem> };

/**
 * One item of a read result: the caller's own keys, the value, and the item metadata. A projection
 * returns its flat record in `data`, in this same envelope, so one shape serves every read: a caller
 * reaches the value through `item.data` and tests `kind` for its type, never the presence of a field.
 *
 * `T` is the caller's own type of the json value and of the projected record, for example
 * `getItem<{ name: string }>({ hashKey, projection: [{ expr: { ref: "data", path: "$.name" }, as: "name" }] })`.
 * The library never checks `T` against the stored item.
 */
export type ReadItem<T = never> = ItemKey &
	ReadItemValue<T> & {
		/** Epoch UTC seconds. The item can remain visible after this instant until background deletion. */
		ttlAt?: number;
		version: number;
	};

export interface ItemGetter {
	getItem<T = never>(opts: GetItemOptions): Promise<GetItemResult<T>>;
}

export type GetItemOptions = {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;
	/** The read returns a flat record in `data`, with `kind: "projected"`, in place of the complete item. */
	projection?: readonly ProjectionExpression[];
};

// Public result surfaced by FokosDB.getItem. The keys are the caller's own, and db.ts has parsed json
// text into a JsonValue. The DO's counterpart is GetItemRpcResponse, which carries no keys at all.
export type GetItemResult<T = never> =
	| {
			found: true;
			item: ReadItem<T>;
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
	transactWriteItems(opts: TransactWriteItemsOptions): Promise<TransactWriteItemsResult>;
	transactGetItems<Ts extends readonly unknown[] = never[]>(
		opts: NoInfer<TransactGetItemsOptions<Ts>>,
	): Promise<TransactGetItemsResult<Ts>>;
}

// Only what a caller of FokosDB needs. The 2PC wire types — the coordinator requests, the driver items,
// and the encoded read results that carry `projected` as a positional row — stay internal to `db.ts`
// and the participant, so the public surface shows one read envelope and never the wire beneath it.
export type {
	TransactWriteItemsResult,
	TransactGetItemsResult,
	MaybeReadItem,
	TransactWriteItem,
	TransactWriteItemsOptions,
	TransactWriteOperationResult,
	RejectionReason,
	TransactGetItemKey,
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
	queryItems<T = never>(opts: QueryItemsProjectedOptions): Promise<QueryItemsProjectedResult<T>>;
	queryItems<T = never>(opts: QueryItemsOptions): Promise<QueryItemsResult<T>>;
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
	 * SQLite evaluates the filter on each candidate. The filter does not change candidate selection, routing,
	 * or the sort-key interval. A rejected candidate still consumes the evaluated budgets of the page, and it
	 * still advances the cursor. The filter is valid with `select: "count"`. Count mode then returns the
	 * matched count of the page.
	 */
	filter?: ConditionExpression;
	/** The page returns a flat record for each item in place of the complete item. It is not valid with `select: "count"`. */
	projection?: readonly ProjectionExpression[];
};

/** A `queryItems` request that carries a projection. The projected overload resolves on this type. */
export type QueryItemsProjectedOptions = QueryItemsOptions & {
	projection: readonly ProjectionExpression[];
	select?: "projection";
};

/**
 * The `queryItems` result of a projected request: one flat record per item, by resolved name. The page
 * keeps bare records, because a projected request projects every item of the page, and its overload
 * therefore types every element exactly.
 */
export type QueryItemsProjectedResult<T = never> = QueryItemsPage<CallerType<T, ProjectedItem>>;

export type QueryItemsMeta = {
	/** Physical SQLite rows read by the leaf query statements. */
	rowsRead: number;
	/** SQL result rows the leaf collectors consumed in JavaScript. */
	rowsReturned: number;
	forwardCount: number;
	partitionsVisited: number;
};

// One page of a query, around whichever element the request asks for. Only that element differs
// between a complete page and a projected one, so the counts, the cursor, and the metas are declared
// here once and both result types below name this page.
export type QueryItemsPage<Item> = {
	items: Item[];
	/** Matched items in this page. */
	count: number;
	/** Evaluated items in this page. Equal to `count` for a request with no filter. With a filter, `count <= scannedCount`. */
	scannedCount: number;
	cursor?: string;
	meta: QueryItemsMeta;
	partitionMetas: Array<OperationMetrics & PartitionInfo>;
};

// Public result surfaced by FokosDB.queryItems. The keys are decoded back to the caller's own form and
// db.ts has parsed json text into a JsonValue. The DO's counterpart is QueryItemsRpcResponse, which
// carries raw key bytes and the stored data representation.
export type QueryItemsResult<T = never> = QueryItemsPage<ReadItem<T>>;
