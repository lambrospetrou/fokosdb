/**
 * This module defines two sizes of an item and how to calculate them.
 *
 * 1. `item_bytes` (`item-bytes.ts`) is the exact size of one item: the stored data plus the bytes of
 *    the hash key and of the sort key. It is the size that a user sees. The item limit, the query
 *    budget, and the export budgets use only this value.
 * 2. The estimate is the number of bytes that a group of items uses in the SQLite file. It includes
 *    the index entries, the row headers, and the unused space of the pages. Promotion and range
 *    splits use it, because they must make the file of a partition smaller.
 *
 * A bucket of `item_stats` stores the facts of its items (`ItemFacts`). Each fact is an exact sum,
 * for example the number of items or the sum of their key bytes. The storage model (`StorageModel`)
 * multiplies each fact by a coefficient and adds the results. This gives the estimate.
 *
 * The coefficients are only in this module. The stored rows hold facts and no coefficient. Thus, a
 * change of a coefficient changes each estimate at once, and it needs no data migration.
 */

import { MAX_ITEM_BYTES } from "../transaction-limits.js";

/**
 * The storage model of one item table. It tells how many bytes of the SQLite file one item uses. The
 * estimate of one item is:
 *
 *     item_bytes
 *     + keyIndexFactor * key_bytes + baseOverhead
 *     + (when the item is indexed) indexedKeyFactor * key_bytes + indexedOverhead
 *     + overflowCoefficient * overflow_bytes
 *
 * - `key_bytes` is the bytes of the hash key plus the bytes of the sort key.
 * - An indexed item has an entry in the partial index of its table. In `items`, it is an item with a
 *   TTL (`idx_items_ttl`). In `latest_items`, it is a tombstone (`idx_latest_items_deleted_gc`).
 * - `overflow_bytes` is the space that a large row uses on its overflow pages and that `item_bytes`
 *   does not count (`overflowBytes`).
 *
 * Example for `items` (`ITEMS_MODEL`), with a hash key of 10 bytes and a sort key of 10 bytes:
 *
 * - An item with 100 bytes of data and no TTL: `item_bytes` is 120, and the estimate is
 *   120 + 1.25 * 20 + 76 = 221 bytes.
 * - The same item with a TTL also has an entry in `idx_items_ttl`: 221 + 1.25 * 20 + 25 = 271 bytes.
 * - An item with 10,000 bytes of data and no TTL has overflow pages. `overflow_bytes` is 2,778, and
 *   the estimate is 10,020 + 25 + 76 + 2,778 = 12,899 bytes.
 *
 * test/storage-bench/item-size-model.workerd-bench.ts measures each value on a page of 4 KiB. Measure
 * the values again when a schema change adds a column or an index to the table.
 */
export type StorageModel = Readonly<{
	/**
	 * The bytes for each key byte, more than the one copy in `item_bytes`. The unique `item_id` index
	 * holds one more copy of the key (1.0). The B-tree pages are not full, which adds about 0.25.
	 */
	keyIndexFactor: number;
	/**
	 * The fixed bytes of each item. It includes the routingHash in the row and in the `item_id` index
	 * entry, the `item_id`, the integer columns, the record headers, and the cell headers.
	 */
	baseOverhead: number;
	/** The bytes for each key byte of an indexed item, for the copy of the key in the partial index entry. */
	indexedKeyFactor: number;
	/** The fixed bytes of the partial index entry of an indexed item: its routingHash and its other columns. */
	indexedOverhead: number;
	/** The bytes for each byte of `overflow_bytes`. */
	overflowCoefficient: number;
	/**
	 * The payload bytes of the row columns that `item_bytes` does not count, for example the routingHash
	 * and the integer columns. SQLite moves a part of a row to overflow pages when the payload is more
	 * than 1,002 bytes. This value gives the payload, so it tells which items have overflow pages.
	 * It is not a coefficient: a change of it changes the `overflow_bytes` of each stored bucket.
	 */
	rowPayloadOverhead: number;
	/**
	 * The extra bytes that a row with overflow pages uses on its tree page, more than the other terms
	 * count. It is part of `overflow_bytes`, so it is not a coefficient, for the same reason as
	 * `rowPayloadOverhead`.
	 */
	overflowRowOverhead: number;
}>;

/**
 * The storage model of `items`, the item table of `FokosDBPartitionDO`. An indexed item is an item with
 * a TTL. The fixed bytes are larger than those of `LATEST_ITEMS_MODEL`, because a row of `items` has
 * more integer columns (`v`, `last_read_ts`, `last_write_ts`).
 */
export const ITEMS_MODEL: StorageModel = Object.freeze({
	keyIndexFactor: 1.25,
	baseOverhead: 76,
	indexedKeyFactor: 1.25,
	indexedOverhead: 25,
	overflowCoefficient: 1,
	rowPayloadOverhead: 40,
	overflowRowOverhead: 61,
});

/**
 * The storage model of `latest_items`, the item table of `FokosDBMaterializationDO`. An indexed item is
 * a tombstone. A tombstone has no data, so its `item_bytes` is its key bytes.
 */
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
 * Throws when a coefficient is negative or is not a multiple of 1/4, or when `rowPayloadOverhead` or
 * `overflowRowOverhead` is not an integer of 0 or more. The facts are integers. When each coefficient
 * is a multiple of 1/4, a double holds the exact estimate up to 2^51. Thus `ceil` in JavaScript and
 * `ceil` in SQL give the same rounded estimate.
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

// The SQLite values of an index B-tree on a page of 4 KiB, with 4,096 usable bytes. A WITHOUT ROWID
// table is an index B-tree.
// - MAX_LOCAL: the largest payload that stays on the tree page.
// - MIN_LOCAL: the smallest part of a larger payload that stays on the tree page.
// - OVERFLOW_PAGE_DATA: the payload bytes of one overflow page. The other 4 bytes point to the next page.
const MAX_LOCAL = 1002;
const MIN_LOCAL = 489;
const OVERFLOW_PAGE_DATA = 4092;
const PAGE_SIZE = 4096;

/**
 * The bytes of the overflow pages of one row that the other terms of the model do not count: the
 * unused space of the last overflow page, the 4-byte pointer of each overflow page, and
 * `overflowRowOverhead`. A row with a payload of 1,002 bytes or less has no overflow page, so the
 * value is 0.
 *
 * The value uses only `item_bytes`. Thus a delete subtracts the same value that the write of the
 * item added. `overflowBytesSql` gives the same value in SQL.
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

/**
 * The estimate of the storage bytes of a group of items, with no rounding. Each term is linear, so the
 * estimate of a group equals the sum of the estimates of its items.
 */
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
 * The largest estimate that one item can have. The bound uses these facts:
 *
 * - `key_bytes <= item_bytes <= MAX_ITEM_BYTES`.
 * - The bound adds the terms of an indexed item.
 * - The `overflow_bytes` of a row are less than one overflow page, plus 4 bytes for each overflow
 *   page, plus `overflowRowOverhead`.
 *
 * For `ITEMS_MODEL` the bound is about 1.37 MiB.
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

/**
 * The largest bucket threshold `m` of a hash leaf. When an insert adds an item to a routingHash with no
 * bucket, the insert counts the items of the routingHash. This value limits that count to 17 rows.
 */
export const BUCKET_THRESHOLD_MAX = 16;

/**
 * The bucket threshold `m` of a hash leaf: a routingHash with fewer than `m` items can have no bucket.
 * `m` is `floor(floor / maxItemEstimate)`, from 1 to `BUCKET_THRESHOLD_MAX`. Thus `m - 1` items have
 * an estimate below the promotion floor, and the promotion lookup does not need their bucket. A null
 * floor (no promotion) gives `BUCKET_THRESHOLD_MAX`.
 *
 * Example: with a floor of 62.5 MiB and `ITEMS_MODEL`, `m` is 16. A table with one item for each hash
 * key then writes no bucket, so each write of an item writes one row less.
 */
export function bucketThreshold(model: StorageModel, floor: number | null): number {
	if (floor === null) {
		return BUCKET_THRESHOLD_MAX;
	}
	return Math.min(BUCKET_THRESHOLD_MAX, Math.max(1, Math.floor(floor / maxItemEstimate(model))));
}

/** The fact columns of a facts row in SQL, in the order of `FACT_NAMES`. */
export const FACT_COLUMNS = ["item_count", "item_bytes", "key_bytes", "indexed_item_count", "indexed_key_bytes", "overflow_bytes"] as const;

/** One row with the six fact columns of `FACT_COLUMNS`. */
export type ItemFactsRow = { [C in (typeof FACT_COLUMNS)[number]]: number };

/** The facts of a row with the columns of `FACT_COLUMNS`. */
export function factsOfRow(row: ItemFactsRow): ItemFacts {
	return {
		itemCount: row.item_count,
		itemBytes: row.item_bytes,
		keyBytes: row.key_bytes,
		indexedItemCount: row.indexed_item_count,
		indexedKeyBytes: row.indexed_key_bytes,
		overflowBytes: row.overflow_bytes,
	};
}

/**
 * The select list that sums the facts of a group of items, with the columns of `FACT_COLUMNS`. The
 * source of the `FROM` clause must give these four columns:
 *
 * - `hk` and `sk`: the encoded hash key and sort key.
 * - `item_bytes`: the stored `item_bytes`.
 * - `indexed`: 1 for an indexed item, else 0.
 *
 * Each store writes the source over its own table, and renames its columns to these names. Thus the
 * facts of a count equal the facts that `itemFacts` gives for each item.
 */
export function factsAggregateSql(model: StorageModel): string {
	const keyBytes = "octet_length(hk) + octet_length(sk)";
	return `COUNT(*) AS item_count,
	        COALESCE(SUM(item_bytes), 0) AS item_bytes,
	        COALESCE(SUM(${keyBytes}), 0) AS key_bytes,
	        COALESCE(SUM(indexed), 0) AS indexed_item_count,
	        COALESCE(SUM(CASE WHEN indexed = 1 THEN ${keyBytes} ELSE 0 END), 0) AS indexed_key_bytes,
	        COALESCE(SUM(${overflowBytesSql(model, "item_bytes")}), 0) AS overflow_bytes`;
}

/** Six SQL expressions for the facts, in the order of `FACT_NAMES`. */
export type FactsSql = readonly [string, string, string, string, string, string];

/**
 * The SQL of the estimate over the fact expressions in `f`, rounded up to a whole byte. It uses `ceil`
 * and not `ceiling`, because the authorizer of Workers SQLite refuses `ceiling`.
 */
export function estSql(model: StorageModel, f: FactsSql): string {
	const [itemCount, itemBytes, keyBytes, indexedItemCount, indexedKeyBytes, overflow] = f;
	return `ceil((${itemBytes}) + ${model.keyIndexFactor} * (${keyBytes}) + ${model.baseOverhead} * (${itemCount})
	         + ${model.indexedKeyFactor} * (${indexedKeyBytes}) + ${model.indexedOverhead} * (${indexedItemCount})
	         + ${model.overflowCoefficient} * (${overflow}))`;
}

/**
 * The SQL of the stored `promotion_est_bytes` over the fact expressions in `f`. It is the rounded
 * estimate when the estimate is at or above the promotion floor in the parameter `floor`. Else it is
 * NULL, and the row has no entry in the partial index `item_stats_promotion`. A NULL floor gives NULL.
 */
export function promotionEstSql(model: StorageModel, f: FactsSql, floor: string): string {
	const est = estSql(model, f);
	return `(CASE WHEN ${est} >= ${floor} THEN ${est} ELSE NULL END)`;
}
