// The key oracle and the page rules that the queryItems properties compare against.
//
// The oracle is a second implementation of the key order: a string key is raw UTF-8, a binary key
// gets a 0xFF tag, an absent sort key is the empty byte string, and the order is an unsigned byte
// compare. It never calls `KeyCodec`, so a broken codec cannot agree with it. Keep it that way.
import { expect } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import type { QueryItemsOptions, QueryItemsResult, SortKeyCondition } from "../../src/shared/types.js";
import type { DataKind, ItemData } from "./arbitraries.js";
import { untilAvailable } from "./model.js";

export type QueryKey = string | Uint8Array;
export type OptionalQueryKey = QueryKey | undefined;
export type Query = QueryItemsOptions["queries"][number];

// ─── The key oracle ─────────────────────────────────────────────────────────────

/** The canonical bytes of a key: raw UTF-8 for a string, a 0xFF tag ahead of a binary key, empty for an absent one. */
export function encodeKey(key: OptionalQueryKey): Uint8Array {
	if (key === undefined) return new Uint8Array(0);
	if (typeof key === "string") return new TextEncoder().encode(key);
	const out = new Uint8Array(key.byteLength + 1);
	out[0] = 0xff;
	out.set(key, 1);
	return out;
}

/** Unsigned byte compare. The store orders keys this way, so the model must order them the same. */
export function compareBytes(a: Uint8Array, b: Uint8Array): number {
	const min = Math.min(a.length, b.length);
	for (let i = 0; i < min; i++) {
		if (a[i] !== b[i]) return a[i] - b[i];
	}
	return a.length - b.length;
}

const startsWith = (value: Uint8Array, prefix: Uint8Array): boolean =>
	value.length >= prefix.length && compareBytes(value.subarray(0, prefix.length), prefix) === 0;

const isEmptyKey = (key: QueryKey): boolean => (typeof key === "string" ? key.length === 0 : key.byteLength === 0);

/** Whether one stored sort key satisfies the sort-key condition of a query. */
export function matchesSortKeyCondition(sortKey: Uint8Array, condition: SortKeyCondition | undefined): boolean {
	if (condition === undefined) return true;
	const cmp = (bound: QueryKey) => compareBytes(sortKey, encodeKey(bound));
	switch (condition.op) {
		case "eq":
			return cmp(condition.value) === 0;
		case "lt":
			return cmp(condition.value) < 0;
		case "lte":
			return cmp(condition.value) <= 0;
		case "gt":
			return cmp(condition.value) > 0;
		case "gte":
			return cmp(condition.value) >= 0;
		case "between":
			return cmp(condition.lower) >= 0 && cmp(condition.upper) <= 0;
		case "begins_with":
			// An empty prefix selects every sort key, and an absent sort key is a prefix of everything.
			return isEmptyKey(condition.prefix) || startsWith(sortKey, encodeKey(condition.prefix));
		case "range": {
			if (condition.lower !== undefined) {
				const c = cmp(condition.lower.value);
				if (c < 0 || (c === 0 && !condition.lower.inclusive)) return false;
			}
			if (condition.upper !== undefined) {
				const c = cmp(condition.upper.value);
				if (c > 0 || (c === 0 && !condition.upper.inclusive)) return false;
			}
			return true;
		}
	}
}

// ─── The model ──────────────────────────────────────────────────────────────────

export type ModelItem = {
	hashKey: QueryKey;
	sortKey: OptionalQueryKey;
	hashKeyBytes: Uint8Array;
	sortKeyBytes: Uint8Array;
	data: ItemData;
	kind: DataKind;
	version: number;
};

export type Model = Map<string, ModelItem>;

export const itemId = (hashKey: QueryKey, sortKey: OptionalQueryKey) => `${encodeKey(hashKey).toHex()}:${encodeKey(sortKey).toHex()}`;

/** The item a query page must return for one model item. */
export const publicItem = (item: ModelItem) => ({
	hashKey: item.hashKey,
	sortKey: item.sortKey,
	data: item.data,
	kind: item.kind,
	version: item.version,
});

/** The items of one sub-query, in the order the page must return them. */
export function expectedItems(model: Model, query: Query): ModelItem[] {
	const hashKeyBytes = encodeKey(query.hashKey);
	const matched = [...model.values()].filter(
		(item) => compareBytes(item.hashKeyBytes, hashKeyBytes) === 0 && matchesSortKeyCondition(item.sortKeyBytes, query.sortKeyCondition),
	);
	matched.sort((a, b) => compareBytes(a.sortKeyBytes, b.sortKeyBytes));
	if (query.scanIndexForward === false) matched.reverse();
	return matched;
}

// ─── Paging ─────────────────────────────────────────────────────────────────────

/** The rules every page obeys, whatever the request asked for. */
export function expectPageInvariants(page: QueryItemsResult, opts: QueryItemsOptions): void {
	expect(page.count).toBeLessThanOrEqual(page.scannedCount);
	expect(page.meta.partitionsVisited).toBe(page.partitionMetas.length);
	if (opts.limit !== undefined) expect(page.scannedCount).toBeLessThanOrEqual(opts.limit);
	if (opts.select === "count") expect(page.items).toHaveLength(0);
	else expect(page.items).toHaveLength(page.count);
}

/**
 * Follows the cursor to the end and returns the whole answer of the request. `onPage` sees each page
 * as it arrives, for a rule that holds of one page rather than of the drained answer.
 */
export async function drainQuery(db: FokosDB, opts: QueryItemsOptions, maxPages: number, onPage?: (page: QueryItemsResult) => void) {
	const items: QueryItemsResult["items"] = [];
	let count = 0;
	let scannedCount = 0;
	let pages = 0;
	// A caller can hand the drain a cursor of its own, to follow an answer it started elsewhere.
	let cursor = opts.cursor;
	do {
		// A partition that splits, or a child that still imports its share, can answer 503. A client
		// retries that, and a query changes nothing, so asking for the same page again is safe.
		const page: QueryItemsResult = await untilAvailable(() => db.queryItems({ ...opts, cursor }));
		pages++;
		expectPageInvariants(page, opts);
		onPage?.(page);
		items.push(...page.items);
		count += page.count;
		scannedCount += page.scannedCount;
		cursor = page.cursor;
		// A page that returns a cursor must make progress. This stops a cursor loop with the request
		// in the failure report instead of the vitest timeout.
		expect(pages, "the cursor did not drain the request").toBeLessThanOrEqual(maxPages);
	} while (cursor !== undefined);
	return { items, count, scannedCount, pages };
}

/** A page can stop between two sub-queries, so the bound holds one page per item and per sub-query. */
export const pageBudget = (expectedCount: number, queries: number) => expectedCount + queries + 8;
