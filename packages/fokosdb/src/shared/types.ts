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

/**
 * "all_old" returns the item image when the condition fails. In a `transactWriteItems` with
 * "ordered_per_item" mode, the image can show the state that the earlier operations of the same
 * transaction left. The transaction cancelled, so that state never committed.
 */
export type ReturnValuesOnConditionCheckFailure = "none" | "all_old";

/** What a `putItem` or `deleteItem` that succeeds returns in `item`: nothing, or the old item. */
export type ReturnValues = "none" | "all_old";

/**
 * Empty by default, so `HashKey`/`SortKey` below are `string | Uint8Array` for every caller. An
 * application that only ever uses one of the two can narrow both across its whole codebase — every
 * FokosDB method, and every type this package exports that carries a key — by augmenting this
 * interface once, via TypeScript module augmentation on the client entry point:
 *
 * ```ts
 * import "fokosdb/client";
 *
 * declare module "fokosdb/client" {
 *   interface FokosTypeOverrides {
 *     HashKey: string;
 *     SortKey: string;
 *   }
 * }
 * ```
 *
 * The `import` line is not optional: without it, this file has no top-level import/export, so
 * TypeScript treats the `declare module` block as a brand new ambient module instead of an
 * augmentation of the real one — silently replacing every other export of "fokosdb/client" with
 * nothing but this interface.
 *
 * This is a compile-time narrowing only: `putItem`/`getItem`/etc. still accept and validate
 * `string | Uint8Array` at runtime regardless of this interface.
 */
export interface FokosTypeOverrides {}

export type HashKey = FokosTypeOverrides extends { HashKey: infer H } ? H : string | Uint8Array;
export type SortKey = FokosTypeOverrides extends { SortKey: infer S } ? S : string | Uint8Array;

export type ConditionCheckImageOf<D> = {
	hashKey: HashKey;
	sortKey?: SortKey;
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

export interface ItemPutter {
	putItem<T = never>(opts: PutItemOptions): Promise<PutItemResult<T>>;
}

export interface ItemDeleter {
	deleteItem<T = never>(opts: DeleteItemOptions): Promise<DeleteItemResult<T>>;
}

export type PutItemOptions = {
	hashKey: HashKey;
	sortKey?: SortKey;

	/** Epoch UTC seconds. Reads can return the item after this instant until background deletion. */
	ttlAt?: number;

	data: string | Uint8Array | JsonComposite;

	condition?: ConditionExpression;

	returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;

	/** With "all_old", the result has the old item in `item`, when one existed. Defaults to "none". */
	returnValues?: ReturnValues;
};

export type DeleteItemOptions = {
	hashKey: HashKey;
	sortKey?: SortKey;

	condition?: ConditionExpression;

	returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;

	/** With "all_old", the result has the deleted item in `item`, when one existed. Defaults to "none". */
	returnValues?: ReturnValues;
};

export type ItemKey = {
	hashKey: HashKey;
	sortKey?: SortKey;
};

/**
 * The result of `putItem`. `item` is the old item, present only with `returnValues: "all_old"` when an
 * old item existed. `T` is the caller's own type of its json value. The result has no keys: the caller
 * has them.
 */
export type PutItemResult<T = never> = {
	item?: ReadItem<T>;
	version: number;
	meta: SinglePartitionMeta;
};

/** The result of `deleteItem`. `item` is as in `PutItemResult`. `deleted` tells whether an item existed. */
export type DeleteItemResult<T = never> = {
	item?: ReadItem<T>;
	deleted: boolean;
	meta: SinglePartitionMeta;
};

/** The value half of a complete read result. `kind` discriminates it, so a caller that tests `kind` also gets the type of `data`. */
export type ReadItemValue<T = never> = DecodedItemData<T>;

/**
 * One complete item of a read result: the keys, the value, and the item metadata. A read without a
 * projection returns this shape. A read with a projection returns `ProjectedReadItem`, with no keys and
 * no `version`. A caller that needs them names them in the projection, for example
 * `{ expr: { ref: "v" }, as: "version" }`.
 *
 * `T` is the caller's own type of the json value, for example `getItem<{ name: string }>({ hashKey })`.
 * The library never checks `T` against the stored item.
 */
export type ReadItem<T = never> = ItemKey &
	ReadItemValue<T> & {
		/** Epoch UTC seconds. The item can remain visible after this instant until background deletion. */
		ttlAt?: number;
		/**
		 * The `v` of the item. Every write increments it, and it never repeats for a key. In a
		 * `transactWriteItems` with "ordered_per_item" mode, a condition or an update value that reads
		 * `v` must come before every put, update, and delete of the same item, or the request fails
		 * with `transact_version_after_write`.
		 */
		version: number;
	};

/**
 * One item of a read with a projection: the flat record of the projection in `data`. The record holds
 * only what the projection names. `kind` tells this shape from a complete item, so code that does not
 * know the request can still handle the item with `switch (item.kind)`. A record field named `kind`
 * causes no confusion, because the record is inside `data`.
 */
export type ProjectedReadItem<T = never> = { kind: "projected"; data: CallerType<T, ProjectedItem> };

/**
 * One item of a read result: the complete item or the projected item. Each shape has `kind` and
 * `data`, and `kind` tells them apart. `T` types the json value of a complete item and the projected
 * record.
 */
export type ReadResultItem<T = never> = ReadItem<T> | ProjectedReadItem<T>;

/**
 * Each read method that takes one request has three overloads. TypeScript uses the first that matches:
 * a request with a projection array gets the projected type, a request with no projection gets the
 * complete type, and a request whose projection can be `undefined` gets the union of both.
 */
export interface ItemGetter {
	getItem<T = never>(opts: GetItemProjectedOptions): Promise<GetItemProjectedResult<T>>;
	getItem<T = never>(opts: GetItemOptions & { projection?: undefined }): Promise<GetItemResult<T>>;
	getItem<T = never>(opts: GetItemOptions): Promise<GetItemResult<T> | GetItemProjectedResult<T>>;
}

export type GetItemOptions = {
	hashKey: HashKey;
	sortKey?: SortKey;
	/** The read returns `ProjectedReadItem` in place of the complete item. */
	projection?: readonly ProjectionExpression[];
};

/** A `getItem` request that carries a projection. The projected overload resolves on this type. */
export type GetItemProjectedOptions = GetItemOptions & { projection: readonly ProjectionExpression[] };

/** The public result of `getItem`. `item` is absent when the item does not exist. */
export type GetItemResult<T = never> = {
	item?: ReadItem<T>;
	meta: SinglePartitionMeta;
};

/** The public result of a projected `getItem`. `item` is absent when the item does not exist. */
export type GetItemProjectedResult<T = never> = {
	item?: ProjectedReadItem<T>;
	meta: SinglePartitionMeta;
};

/** One partition that ran a request. */
export type PartitionMeta = {
	/** The DurableObjectId of the partition, 64 hex characters. */
	servedByActorId: string;
	/** The name of the partition. Absent when the client does not know it. */
	servedByActorName?: string;
	/** FOR DEBUGGING ONLY. The depth in the hash tree. Absent for a range partition, which has no hash depth. */
	hashDepth?: number;
	/** FOR DEBUGGING ONLY. The depth in the range tree. Absent for a hash partition, which has no range depth. */
	rangeDepth?: number;
	/** Absent when the partition did not answer, or when its answer carries no counts. */
	rowsRead?: number;
	/** Absent when the partition did not answer, or when its answer carries no counts. */
	rowsWritten?: number;
	/** Absent when the partition did not answer, or when its answer carries no counts. */
	databaseSize?: number;
};

/** The `meta` of every result and every error of a FokosDB method. */
export type FokosMeta<P extends PartitionMeta = PartitionMeta> = {
	/** The partition-to-partition forwards of the whole request. */
	forwardCount: number;
	/** One entry for each partition that ran the request. */
	partitions: P[];
	/** The transaction coordinator that answered. Present only on the coordinator path of `transactWriteItems`. */
	txCoordinator?: PartitionMeta;
};

/** An entry with every count, for a result where every partition answered. */
export type AnsweredPartitionMeta = PartitionMeta & OperationMetrics;

/** The `meta` of a method that one partition runs. */
export type SinglePartitionMeta = FokosMeta<AnsweredPartitionMeta> & { partitions: [AnsweredPartitionMeta] };

/**
 * The entry that a partition writes about itself into its answer. It holds no name and no partition ID,
 * because both can hold the keys, so the size of an entry does not depend on the keys.
 */
export type WirePartitionMeta = {
	actorId: string;
	/** Absent in the entry that a range partition writes. */
	hashDepth?: number;
	/** Absent in the entry that a hash partition writes. */
	rangeDepth?: number;
	rowsRead?: number;
	rowsWritten?: number;
	databaseSize?: number;
};

export type OperationMetrics = {
	rowsRead: number;
	rowsWritten: number;
	databaseSize: number;
};

// ─── queryItems public API ────────────────────────────────────────────────────

export type SortKeyCondition =
	| { op: "eq"; value: SortKey }
	| { op: "lt" | "lte" | "gt" | "gte"; value: SortKey }
	| { op: "between"; lower: SortKey; upper: SortKey }
	| { op: "begins_with"; prefix: SortKey }
	| {
			op: "range";
			lower?: { value: SortKey; inclusive: boolean };
			upper?: { value: SortKey; inclusive: boolean };
	  };

/** The overloads follow the rule of `ItemGetter`. */
export interface ItemQuerier {
	queryItems<T = never>(opts: QueryItemsProjectedOptions): Promise<QueryItemsProjectedResult<T>>;
	queryItems<T = never>(opts: QueryItemsOptions & { projection?: undefined }): Promise<QueryItemsResult<T>>;
	queryItems<T = never>(opts: QueryItemsOptions): Promise<QueryItemsResult<T> | QueryItemsProjectedResult<T>>;
}

/** The selection of a queryItems page: materialized items, or the matched count only. */
export type QuerySelect = "projection" | "count";

// The field names what the list contains: `queries` here, `items` on the two transaction methods.
export type QueryItemsOptions = {
	queries: Array<{ hashKey: HashKey; sortKeyCondition?: SortKeyCondition; scanIndexForward?: boolean }>;
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
	/** The page returns a `ProjectedReadItem` for each item in place of the complete item. It is not valid with `select: "count"`. */
	projection?: readonly ProjectionExpression[];
};

/** A `queryItems` request that carries a projection. The projected overload resolves on this type. */
export type QueryItemsProjectedOptions = QueryItemsOptions & {
	projection: readonly ProjectionExpression[];
	select?: "projection";
};

/** The `queryItems` and `scanItems` result of a projected request: one `ProjectedReadItem` for each item. */
export type QueryItemsProjectedResult<T = never> = QueryItemsPage<ProjectedReadItem<T>>;

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
	/**
	 * `meta.partitions` has one entry for each leaf partition that the page visited, in visit order. The
	 * items of entry `i` are the `itemCount` items after the items of entries `0` to `i - 1`.
	 */
	meta: FokosMeta<AnsweredPartitionMeta & { itemCount: number }> & {
		/** SQL result rows that the leaf scans consumed in JavaScript. */
		rowsReturned: number;
	};
};

// Public result surfaced by FokosDB.queryItems. The keys are decoded back to the caller's own form and
// db.ts has parsed json text into a JsonValue. The DO's counterpart is QueryItemsRpcResponse, which
// carries raw key bytes and the stored data representation.
export type QueryItemsResult<T = never> = QueryItemsPage<ReadItem<T>>;

/**
 * One GSI entry of `db.gsi(indexName).queryItems`. `hashKey` and `sortKey` are the GSI keys, and
 * `baseHashKey` and `baseSortKey` are the keys of the base item. `version` is the version of the GSI
 * entry, not of the base item.
 */
export type GsiReadItem<T = never> = ReadItem<T> & { baseHashKey: HashKey; baseSortKey?: SortKey };

export type GsiQueryItemsResult<T = never> = QueryItemsPage<GsiReadItem<T>>;

/** The read view of one GSI of a table. */
export interface FokosGsi {
	queryItems<T = never>(opts: QueryItemsProjectedOptions): Promise<QueryItemsProjectedResult<T>>;
	queryItems<T = never>(opts: QueryItemsOptions & { projection?: undefined }): Promise<GsiQueryItemsResult<T>>;
	queryItems<T = never>(opts: QueryItemsOptions): Promise<GsiQueryItemsResult<T> | QueryItemsProjectedResult<T>>;
	scanItems<T = never>(opts: GsiScanItemsProjectedOptions): Promise<QueryItemsProjectedResult<T>>;
	scanItems<T = never>(opts: GsiScanItemsOptions & { projection?: undefined }): Promise<GsiQueryItemsResult<T>>;
	scanItems<T = never>(opts: GsiScanItemsOptions): Promise<GsiQueryItemsResult<T> | QueryItemsProjectedResult<T>>;
}

// ─── scanItems public API ─────────────────────────────────────────────────────

/**
 * One page of the items of a table, in the storage order. The items of one hash key come together, in
 * sort-key order. `limit`, `maxResponseBytes`, `select`, `filter`, and `projection` have the rules of
 * `queryItems`, and they can change between pages.
 */
export type ScanItemsOptions = Omit<QueryItemsOptions, "queries" | "cursor"> & {
	/** The `cursor` of the previous page, sent back unchanged with the same `segment` and `totalSegments`. */
	cursor?: string;
	/** The first page starts after this key. Not valid with `cursor`. */
	exclusiveStartKey?: { hashKey: HashKey; sortKey?: SortKey };
	/** From 0 to `totalSegments - 1`. Give `segment` and `totalSegments` together, or neither. */
	segment?: number;
	/** From 1 to 1,000,000. */
	totalSegments?: number;
};

/** A `scanItems` request that carries a projection. The projected overload resolves on this type. */
export type ScanItemsProjectedOptions = ScanItemsOptions & { projection: readonly ProjectionExpression[]; select?: "projection" };

/**
 * The options of `db.gsi(indexName).scanItems`. The start key can name one entry of the GSI. A start key
 * with no base keys starts before each entry with its GSI keys.
 */
export type GsiScanItemsOptions = Omit<ScanItemsOptions, "exclusiveStartKey"> & {
	exclusiveStartKey?: { hashKey: HashKey; sortKey?: SortKey; baseHashKey?: HashKey; baseSortKey?: SortKey };
};

export type GsiScanItemsProjectedOptions = GsiScanItemsOptions & { projection: readonly ProjectionExpression[]; select?: "projection" };

/** A page of `scanItems`. `cursor` is absent when the scan of the table, or of the segment, is complete. */
export type ScanItemsResult<T = never> = QueryItemsPage<ReadItem<T>>;

/** The overloads follow the rule of `ItemGetter`. */
export interface ItemScanner {
	scanItems<T = never>(opts: ScanItemsProjectedOptions): Promise<QueryItemsProjectedResult<T>>;
	scanItems<T = never>(opts: ScanItemsOptions & { projection?: undefined }): Promise<ScanItemsResult<T>>;
	scanItems<T = never>(opts: ScanItemsOptions): Promise<ScanItemsResult<T> | QueryItemsProjectedResult<T>>;
}

/** The read view of a GSI of the table. */
export interface GsiReader {
	gsi(indexName: string): FokosGsi;
}
