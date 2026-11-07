/**
 * The size definitions of an item, and the storage model that prices a group of items.
 *
 * - `item_bytes` (`item-bytes.ts`) is the exact size of one item: the stored data plus the key bytes.
 *   The item limit, the query budget, and the export budgets use only this value.
 * - The facts of a group of items are exact sums (`ItemFacts`). The storage model multiplies each
 *   fact by a coefficient of its table and gives an estimate of the storage bytes of the group.
 *   Promotion and range splits use this estimate.
 *
 * No other module holds a coefficient. A change of a coefficient changes each estimate that the
 * planner calculates at once, and it changes no stored row.
 */

import { MAX_ITEM_BYTES } from "../transaction-limits.js";

/**
 * The storage model of one item table. An indexed item is an item with an entry in the partial
 * index of its table: an item with a TTL in `items`, and a tombstone in `latest_items`.
 *
 * test/storage-bench/item-size-model.workerd-bench.ts measures each value on a 4 KiB page.
 */
export type StorageModel = Readonly<{
	/** The key copy in the unique `item_id` index, and the part-full pages of the table. */
	keyIndexFactor: number;
	/** The routingHash copies, the item id, the integer columns, and the record and cell headers. */
	baseOverhead: number;
	/** The key copy in the partial index. */
	indexedKeyFactor: number;
	/** The routingHash copy and the fixed columns of the partial index entry. */
	indexedOverhead: number;
	/** The factor of `overflow_bytes`. */
	overflowCoefficient: number;
	/**
	 * The payload bytes of the columns that are not in `item_bytes`. It is not a coefficient: it decides
	 * which items have overflow pages, and thus what each bucket stores in `overflow_bytes`.
	 */
	rowPayloadOverhead: number;
	/**
	 * The bytes that a row with overflow pages uses on the tree page more than the other terms count.
	 * It is not a coefficient, for the same reason as `rowPayloadOverhead`.
	 */
	overflowRowOverhead: number;
}>;

/** The storage model of `items`. */
export const ITEMS_MODEL: StorageModel = Object.freeze({
	keyIndexFactor: 1.25,
	baseOverhead: 76,
	indexedKeyFactor: 1.25,
	indexedOverhead: 25,
	overflowCoefficient: 1,
	rowPayloadOverhead: 40,
	overflowRowOverhead: 61,
});

/** The storage model of `latest_items`. */
export const LATEST_ITEMS_MODEL: StorageModel = Object.freeze({
	keyIndexFactor: 1.25,
	baseOverhead: 67.25,
	indexedKeyFactor: 1.25,
	indexedOverhead: 24.75,
	overflowCoefficient: 1,
	rowPayloadOverhead: 29,
	overflowRowOverhead: 69,
});

const COEFFICIENTS = ["keyIndexFactor", "baseOverhead", "indexedKeyFactor", "indexedOverhead", "overflowCoefficient"] as const;

/**
 * Throws when a coefficient is negative or not a multiple of 1/4. With multiples of 1/4, a double
 * holds each sum of integer facts exactly, up to 2^51, so `ceil` rounds the exact estimate.
 */
export function validateStorageModel(model: StorageModel): void {
	for (const name of COEFFICIENTS) {
		const value = model[name];
		if (!(value >= 0) || !Number.isInteger(value * 4)) {
			throw new Error(`fokos/item-size: the coefficient ${name} must be 0 or more and a multiple of 1/4, got ${value}`);
		}
	}
	for (const name of ["rowPayloadOverhead", "overflowRowOverhead"] as const) {
		if (!Number.isInteger(model[name]) || model[name] < 0) {
			throw new Error(`fokos/item-size: ${name} must be an integer of 0 or more, got ${model[name]}`);
		}
	}
}

validateStorageModel(ITEMS_MODEL);
validateStorageModel(LATEST_ITEMS_MODEL);

/** The exact facts of a group of items. Each value is a sum over the items of the group. */
export type ItemFacts = {
	itemCount: number;
	itemBytes: number;
	keyBytes: number;
	indexedItemCount: number;
	indexedKeyBytes: number;
	overflowBytes: number;
};

/** The facts of no item. */
export const NO_FACTS: Readonly<ItemFacts> = Object.freeze({
	itemCount: 0,
	itemBytes: 0,
	keyBytes: 0,
	indexedItemCount: 0,
	indexedKeyBytes: 0,
	overflowBytes: 0,
});

/** The fact names in the column order of `item_stats`. */
export const FACT_NAMES = ["itemCount", "itemBytes", "keyBytes", "indexedItemCount", "indexedKeyBytes", "overflowBytes"] as const;

// The SQLite values of an index B-tree on a 4 KiB page with a usable size of 4,096 bytes: the largest
// payload on the tree page, the smallest part on the tree page of a larger payload, and the bytes of
// one overflow page.
const MAX_LOCAL = 1002;
const MIN_LOCAL = 489;
const OVERFLOW_PAGE_DATA = 4092;
const PAGE_SIZE = 4096;

/**
 * The bytes of the overflow pages of one row that the other terms of the model do not count, from its
 * `item_bytes` only. A delete then subtracts the same value that the write of the item added.
 * `overflowBytesSql` gives the same value in SQL.
 */
export function overflowBytes(model: StorageModel, itemBytes: number): number {
	const payload = itemBytes + model.rowPayloadOverhead;
	if (payload <= MAX_LOCAL) {
		return 0;
	}
	let local = MIN_LOCAL + ((payload - MIN_LOCAL) % OVERFLOW_PAGE_DATA);
	if (local > MAX_LOCAL) {
		local = MIN_LOCAL;
	}
	const moved = payload - local;
	return Math.ceil(moved / OVERFLOW_PAGE_DATA) * PAGE_SIZE - moved + model.overflowRowOverhead;
}

/** `overflowBytes` as an SQL expression over the integer `item_bytes` expression `itemBytes`. */
export function overflowBytesSql(model: StorageModel, itemBytes: string): string {
	const payload = `(CAST(${itemBytes} AS INTEGER) + ${model.rowPayloadOverhead})`;
	const rest = `(${MIN_LOCAL} + (${payload} - ${MIN_LOCAL}) % ${OVERFLOW_PAGE_DATA})`;
	const moved = `(${payload} - CASE WHEN ${rest} > ${MAX_LOCAL} THEN ${MIN_LOCAL} ELSE ${rest} END)`;
	return `(CASE WHEN ${payload} <= ${MAX_LOCAL} THEN 0 ELSE
	         ((${moved} + ${OVERFLOW_PAGE_DATA - 1}) / ${OVERFLOW_PAGE_DATA}) * ${PAGE_SIZE} - ${moved} + ${model.overflowRowOverhead} END)`;
}

/** The facts of one item. `keyBytes` is the encoded bytes of the hash key plus those of the sort key. */
export function itemFacts(model: StorageModel, item: { itemBytes: number; keyBytes: number; indexed: boolean }): ItemFacts {
	return {
		itemCount: 1,
		itemBytes: item.itemBytes,
		keyBytes: item.keyBytes,
		indexedItemCount: item.indexed ? 1 : 0,
		indexedKeyBytes: item.indexed ? item.keyBytes : 0,
		overflowBytes: overflowBytes(model, item.itemBytes),
	};
}

/** The estimate of the storage bytes of a group of items, with no rounding. It equals the sum of the item estimates. */
export function estimate(model: StorageModel, f: ItemFacts): number {
	return (
		f.itemBytes +
		model.keyIndexFactor * f.keyBytes +
		model.baseOverhead * f.itemCount +
		model.indexedKeyFactor * f.indexedKeyBytes +
		model.indexedOverhead * f.indexedItemCount +
		model.overflowCoefficient * f.overflowBytes
	);
}

/**
 * An upper bound of the estimate of one item. `key_bytes <= item_bytes <= MAX_ITEM_BYTES`, the bound
 * includes the indexed terms, and the overflow bytes of a row are less than one overflow page plus 4
 * bytes for each overflow page plus `overflowRowOverhead`.
 */
export function maxItemEstimate(model: StorageModel): number {
	const pages = Math.ceil((MAX_ITEM_BYTES + model.rowPayloadOverhead) / OVERFLOW_PAGE_DATA);
	return (
		MAX_ITEM_BYTES * (1 + model.keyIndexFactor + model.indexedKeyFactor) +
		model.baseOverhead +
		model.indexedOverhead +
		model.overflowCoefficient * (OVERFLOW_PAGE_DATA + 4 * pages + model.overflowRowOverhead)
	);
}

/** The largest bucket threshold `m` of a hash leaf. It limits the items that the count of an insert reads. */
export const BUCKET_THRESHOLD_MAX = 16;

/**
 * The bucket threshold `m` of a hash leaf: a routingHash with fewer than `m` items can have no bucket.
 * Then `(m - 1)` items have an estimate below the promotion floor. A null floor (no promotion) gives
 * `BUCKET_THRESHOLD_MAX`.
 */
export function bucketThreshold(model: StorageModel, floor: number | null): number {
	if (floor === null) {
		return BUCKET_THRESHOLD_MAX;
	}
	return Math.min(BUCKET_THRESHOLD_MAX, Math.max(1, Math.floor(floor / maxItemEstimate(model))));
}

/** Six SQL expressions for the facts, in the order of `FACT_NAMES`. */
export type FactsSql = readonly [string, string, string, string, string, string];

/**
 * The estimate over the facts in `f`, rounded up to a whole byte. `ceil` and not `ceiling`: the
 * authorizer of Workers SQLite refuses `ceiling`.
 */
export function estSql(model: StorageModel, f: FactsSql): string {
	const [itemCount, itemBytes, keyBytes, indexedItemCount, indexedKeyBytes, overflow] = f;
	return `ceil((${itemBytes}) + ${model.keyIndexFactor} * (${keyBytes}) + ${model.baseOverhead} * (${itemCount})
	         + ${model.indexedKeyFactor} * (${indexedKeyBytes}) + ${model.indexedOverhead} * (${indexedItemCount})
	         + ${model.overflowCoefficient} * (${overflow}))`;
}

/**
 * The stored `promotion_est_bytes` over the facts in `f`: the rounded estimate when it is at or above
 * the bound promotion floor `floor`, else NULL. A NULL floor gives NULL.
 */
export function promotionEstSql(model: StorageModel, f: FactsSql, floor: string): string {
	const est = estSql(model, f);
	return `(CASE WHEN ${est} >= ${floor} THEN ${est} ELSE NULL END)`;
}
