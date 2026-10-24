/**
 * The batch shapes a caller of FokosDB writes and receives: `batchGetItems` and `batchWriteItems`. A
 * batch is not atomic. Each item applies or reads atomically, and each result says what happened to
 * its own item. Nothing here describes the wire: the partition RPC types live with the item RPC types
 * in `server/do-partition.ts`.
 */
import type { FokosErrorCode } from "./errors-operations.js";
import type { ConditionExpression, ProjectionExpression } from "./expression/types.js";
import type { JsonComposite } from "./json-types.js";
import type { FokosMeta, HashKey, ItemKey, ReadItem, SortKey } from "./types.js";

/** One key of `batchGetItems`. Its projection replaces the projection of the request. */
export type BatchGetItemKey = ItemKey & { projection?: readonly ProjectionExpression[] };

/**
 * `Ts` names the type of each item by position, as `TransactGetItemsOptions` does. A call that names
 * no `Ts` keeps the widest types.
 */
export type BatchGetItemsOptions<Ts extends readonly unknown[] = never[]> = {
	/** 0 to 100 keys. An empty list sends no RPC. Two keys of one item fail the request with `batch_duplicate_key`. */
	items: { [K in keyof Ts]: BatchGetItemKey };
	/** The projection of each key that has no projection of its own. */
	projection?: readonly ProjectionExpression[];
	/**
	 * The most bytes of found items in the result. A key that does not fit is `unprocessed` with
	 * `response_too_large`. Defaults to `MAX_BATCH_GET_RESPONSE_BYTES`, and is clamped to it.
	 */
	maxResponseBytes?: number;
};

/** `code` is a FokosDB error code, or `response_too_large` when the item did not fit in `maxResponseBytes`. */
export type BatchUnprocessedCode = FokosErrorCode | "response_too_large";

/**
 * The answer for one key of `batchGetItems`. `partitionIndex` is the index of the partition of the
 * item in `meta.partitions`. It is absent when no partition answered for the item.
 *
 * - `found` and `not_found`: the read answered with the committed state of the item.
 * - `unprocessed`: the read did not answer. Send the key again. Only `response_too_large` has no `error_id`.
 * - `rejected`: the same key fails the same way again, for example with a projection that is not valid.
 */
export type BatchGetItemResult<T = never> =
	| ({ outcome: "found"; partitionIndex: number } & ReadItem<T>)
	| ({ outcome: "not_found"; partitionIndex: number } & ItemKey)
	| ({ outcome: "rejected"; code: FokosErrorCode; error_id: string; partitionIndex?: number } & ItemKey)
	| ({ outcome: "unprocessed"; code: BatchUnprocessedCode; error_id?: string; partitionIndex?: number } & ItemKey);

export type BatchGetItemsResult<Ts extends readonly unknown[] = never[]> = {
	/** `items[i]` answers `items[i]` of the request, with the type `Ts[i]`. */
	items: { [K in keyof Ts]: BatchGetItemResult<Ts[K]> };
	/**
	 * The request entries of the `unprocessed` results, in request order. Each entry keeps its own
	 * projection. A caller that sends the entries again must also send the `projection` of the request.
	 */
	unprocessedKeys: BatchGetItemKey[];
	meta: FokosMeta;
};

/**
 * One operation of `batchWriteItems`. It has no update expression. A `condition` runs in the same
 * transaction as the write of its item, as in `putItem` and `deleteItem`, and a failed condition
 * gives the operation `rejected` with the code `condition_failed`.
 */
export type BatchWriteItem =
	| {
			operation: "put";
			hashKey: HashKey;
			sortKey?: SortKey;
			data: string | Uint8Array | JsonComposite;
			/** Epoch UTC seconds. Reads can return the item after this instant until background deletion. */
			ttlAt?: number;
			condition?: ConditionExpression;
	  }
	| { operation: "delete"; hashKey: HashKey; sortKey?: SortKey; condition?: ConditionExpression };

export type BatchWriteItemsOptions = {
	/**
	 * 0 to 100 operations. An empty list sends no RPC. Two operations on one item fail the request with
	 * `batch_duplicate_key`. Conditions above `MAX_PAYLOAD_BYTES_PER_TX` in total fail it with
	 * `batch_conditions_too_large`.
	 */
	items: BatchWriteItem[];
};

/**
 * The answer for one operation of `batchWriteItems`. It carries no key, as
 * `TransactWriteOperationResult`. `partitionIndex` is as in `BatchGetItemResult`.
 *
 * - `applied`: the operation committed.
 * - `unprocessed`: nothing of the operation applied. Send it again.
 * - `rejected`: nothing of the operation applied, and the same operation against the same state of the
 *   item fails the same way again. A failed condition has the code `condition_failed`. After an
 *   `unknown` answer, a resend can fail its condition because the first try applied.
 * - `unknown`: the operation can have applied. A caller can send it again: then the item gets a new
 *   `version`, and the write replaces a write that another client made between the two.
 */
export type BatchWriteItemResult =
	| { outcome: "ok"; operation: "put"; version: number; partitionIndex: number }
	| { outcome: "ok"; operation: "delete"; deleted: boolean; partitionIndex: number }
	| { outcome: "rejected"; code: FokosErrorCode; error_id: string; partitionIndex?: number }
	| { outcome: "unprocessed"; code: FokosErrorCode; error_id: string; partitionIndex?: number }
	| { outcome: "unknown"; code: FokosErrorCode; error_id: string; partitionIndex?: number };

export type BatchWriteItemsResult = {
	/** `results[i]` answers `items[i]` of the request. */
	results: BatchWriteItemResult[];
	/** The request entries of the `unprocessed` and `unknown` results, in request order. */
	unprocessedItems: BatchWriteItem[];
	meta: FokosMeta;
};

/** The batch half of the FokosDB contract. Neither method retries, and neither throws after I/O starts. */
export interface ItemBatcher {
	batchGetItems<Ts extends readonly unknown[] = never[]>(opts: NoInfer<BatchGetItemsOptions<Ts>>): Promise<BatchGetItemsResult<Ts>>;
	batchWriteItems(opts: BatchWriteItemsOptions): Promise<BatchWriteItemsResult>;
}
