// The harness of the `queryItems` suites: the key order oracle, the model the pages are compared
// with, the drain of one request, and the range tree that the split suites read.
//
// The oracle is a second implementation of the key order: a string key is raw UTF-8, a binary key
// gets a 0xFF tag, an absent sort key is the empty byte string, and the order is an unsigned byte
// compare. It never calls `KeyCodec`, so a broken codec cannot agree with it. Keep it that way.
//
// harness.ts holds everything else the suites share: the table, the waits, and the item model of
// the write suites.
import fc from "fast-check";
import { expect } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import type { QueryItemsOptions, QueryItemsResult, SortKeyCondition } from "../../src/shared/types.js";
import { expectedDataKind, makeTestDB, sleep, textEncoder, untilAvailable, type DataKind, type ItemData } from "./harness.js";

export type QueryKey = string | Uint8Array;
export type OptionalQueryKey = QueryKey | undefined;
export type Query = QueryItemsOptions["queries"][number];

// ─── The key oracle ───────────────────────────────────────────────────────────

/** The canonical bytes of a key: raw UTF-8 for a string, a 0xFF tag ahead of a binary key, empty for an absent one. */
function encodeKey(key: OptionalQueryKey): Uint8Array {
	if (key === undefined) return new Uint8Array(0);
	if (typeof key === "string") return textEncoder.encode(key);
	const out = new Uint8Array(key.byteLength + 1);
	out[0] = 0xff;
	out.set(key, 1);
	return out;
}

/** Unsigned byte compare. The store orders keys this way, so the model must order them the same way. */
function compareBytes(a: Uint8Array, b: Uint8Array): number {
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
function matchesSortKeyCondition(sortKey: Uint8Array, condition: SortKeyCondition | undefined): boolean {
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

// ─── The model ────────────────────────────────────────────────────────────────

export type QueryModelItem = {
	hashKey: QueryKey;
	sortKey: OptionalQueryKey;
	hashKeyBytes: Uint8Array;
	sortKeyBytes: Uint8Array;
	data: ItemData;
	kind: DataKind;
	version: number;
};

export type QueryModel = Map<string, QueryModelItem>;

export const itemId = (hashKey: QueryKey, sortKey: OptionalQueryKey) => `${encodeKey(hashKey).toHex()}:${encodeKey(sortKey).toHex()}`;

/** Records one written item in the model. The caller passes the version that the write reported. */
export function recordItem(model: QueryModel, hashKey: QueryKey, sortKey: OptionalQueryKey, data: ItemData, version: number): void {
	model.set(itemId(hashKey, sortKey), {
		hashKey,
		sortKey,
		hashKeyBytes: encodeKey(hashKey),
		sortKeyBytes: encodeKey(sortKey),
		data,
		kind: expectedDataKind(data),
		version,
	});
}

/** The item a query page must return for one model item. */
export const publicItem = (item: QueryModelItem) => ({
	hashKey: item.hashKey,
	sortKey: item.sortKey,
	data: item.data,
	kind: item.kind,
	version: item.version,
});

/** The items of one sub-query, in the order the page must return them. */
export function expectedItems(model: QueryModel, query: Query): QueryModelItem[] {
	const hashKeyBytes = encodeKey(query.hashKey);
	const matched = [...model.values()].filter(
		(item) => compareBytes(item.hashKeyBytes, hashKeyBytes) === 0 && matchesSortKeyCondition(item.sortKeyBytes, query.sortKeyCondition),
	);
	matched.sort((a, b) => compareBytes(a.sortKeyBytes, b.sortKeyBytes));
	if (query.scanIndexForward === false) matched.reverse();
	return matched;
}

/** The whole answer of a request: the items of each sub-query, in the order the request names them. */
export const expectedAnswer = (model: QueryModel, queries: readonly Query[]): QueryModelItem[] =>
	queries.flatMap((query) => expectedItems(model, query));

// ─── The pages ────────────────────────────────────────────────────────────────

/** The rules every page obeys, whatever the request asked for. */
function expectPageInvariants(page: QueryItemsResult, opts: QueryItemsOptions): void {
	expect(page.count).toBeLessThanOrEqual(page.scannedCount);
	// The route evidence of an envelope is capped in bytes, so a deep or wide tree can visit a leaf
	// that the evidence could not name. The client skips such a leaf instead of reporting a
	// partition it cannot identify, so a page names no more partitions than it visited.
	expect(page.meta.partitionsVisited).toBeGreaterThanOrEqual(page.partitionMetas.length);
	if (opts.limit !== undefined) expect(page.scannedCount).toBeLessThanOrEqual(opts.limit);
	if (opts.select === "count") expect(page.items).toHaveLength(0);
	else expect(page.items).toHaveLength(page.count);
}

/** A page can stop between two sub-queries, so the bound holds one page per item and per sub-query. */
const pageBudget = (expectedCount: number, queries: number) => expectedCount + queries + 8;

/**
 * Follows the cursor to the end and returns the whole answer of the request. `onPage` sees each
 * page as it arrives, for a rule that holds of one page rather than of the drained answer.
 */
async function drainQuery(db: FokosDB, opts: QueryItemsOptions, maxPages: number, onPage?: (page: QueryItemsResult) => void) {
	const items: QueryItemsResult["items"] = [];
	let count = 0;
	let scannedCount = 0;
	let pages = 0;
	// A caller can hand the drain a cursor of its own, to follow an answer it started elsewhere.
	let cursor = opts.cursor;
	do {
		// A partition that splits, or a child that still imports its share, can answer 503. A client
		// retries that, and a query changes nothing, so the same page is safe to ask for again.
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

/**
 * Drains one request and compares the whole answer with `expected`. A request in count mode counts
 * the same items and returns none of them, so one function compares both modes. No suite sends a
 * filter, so every evaluated candidate is also a matched one and `scannedCount` is the same number.
 */
export async function expectDrainedAnswer(
	db: FokosDB,
	expected: readonly QueryModelItem[],
	opts: QueryItemsOptions,
	onPage?: (page: QueryItemsResult) => void,
): Promise<void> {
	const drained = await drainQuery(db, opts, pageBudget(expected.length, opts.queries.length), onPage);
	expect(drained.items).toEqual(opts.select === "count" ? [] : expected.map(publicItem));
	expect(drained.count).toBe(expected.length);
	expect(drained.scannedCount).toBe(expected.length);
}

/** Drains a request that starts from `cursor`, and returns its items. */
export async function drainFromCursor(
	db: FokosDB,
	opts: QueryItemsOptions,
	cursor: string,
	expectedCount: number,
): Promise<QueryItemsResult["items"]> {
	const drained = await drainQuery(db, { ...opts, cursor }, pageBudget(expectedCount, opts.queries.length));
	return drained.items;
}

// ─── The range tree fixture ───────────────────────────────────────────────────
//
// The fixture promotes one hash key into a range tree of several leaves, empties one middle leaf,
// and adds a second hash key on an ordinary partition. The build costs several hundred writes, so a
// suite builds it once in `beforeAll`.
//
// `query-items-split.test.ts` queries the tree after it settles. `query-items-active-split.test.ts`
// keeps writing to it, so a query runs while a leaf splits and its children still import.

// A key moves into a range tree once it holds `hashSplitMaxSizeMb * RANGE_PROMOTION_FRACTION`
// bytes, and a range partition splits once it holds `rangeSplitMaxSizeMb`. A range partition also
// REFUSES a write above 1.1 times its own threshold, and only a write that applies can queue the
// split that brings it back under. The seed therefore runs in two phases: it writes just past the
// promotion threshold, waits for the range root to exist, and only then writes the rest. One phase
// would hand the fresh range root every byte at once, far above the refusal point, and the tree
// would never grow past its root.
const TREE_HASH_SPLIT_MAX_SIZE_MB = 1;
export const RANGE_SPLIT_MAX_SIZE_MB = 0.5;
const PROMOTE_ITEMS = 72;
export const HOT_ITEMS = 340;
const ITEM_BYTES = 4 * 1024;

export const HOT_HASH_KEY = "range:hot";
// A second hash key on an ordinary leaf of the same table. A request that names both reads a range
// tree and a plain partition in one page, which is where the client carries one budget across two
// sub-queries of different shapes.
const COLD_HASH_KEY = "plain:cold";

export const hotSortKey = (i: number) => `sk${String(i).padStart(4, "0")}`;
// A binary sort key carries a 0xFF tag, so these sort above every string key and land in the last
// leaf of the tree. They keep the byte order of the walk from being a pure string comparison.
const BINARY_SORT_KEYS: Uint8Array[] = [
	new Uint8Array([0x01]),
	new Uint8Array([0x01, 0x02]),
	new Uint8Array([0x7f]),
	new Uint8Array([0xff]),
];
// The cold partition holds few items, and one of them has no sort key at all, which is the byte minimum.
const COLD_SORT_KEYS: OptionalQueryKey[] = [undefined, "a", "aa", "ab", "b", "b#1", "b#2", "c", "d", new Uint8Array([0x02])];

const label = (sortKey: OptionalQueryKey) => (typeof sortKey === "string" ? sortKey : encodeKey(sortKey).toHex());
/** The payload names its own key, so an item that a page puts in the wrong place fails the comparison. */
const hotData = (sortKey: OptionalQueryKey): string => `${label(sortKey)}:`.padEnd(ITEM_BYTES, "x");
const coldData = (sortKey: OptionalQueryKey, index: number): ItemData =>
	index % 3 === 0 ? { key: label(sortKey), n: index } : index % 3 === 1 ? `cold:${label(sortKey)}` : new Uint8Array([index, 0x00, 0xff]);

export type Fixture = {
	db: FokosDB;
	model: QueryModel;
	hotSortKeys: OptionalQueryKey[];
	leaves: string[];
	boundaries: QueryKey[];
	emptiedKeys: OptionalQueryKey[];
};

/**
 * The sort keys of the leaf the fixture empties: everything between one start boundary and the
 * next. It is a leaf at an odd position, so in a tree of two-way splits it is the SECOND child of
 * its own router — the child whose empty answer the router must carry past instead of an end of the
 * page.
 */
function middleLeafKeys(sorted: readonly OptionalQueryKey[], boundaries: readonly QueryKey[]): OptionalQueryKey[] {
	const index = boundaries.length >= 3 ? 2 : Math.floor(boundaries.length / 2);
	const start = encodeKey(boundaries[index]);
	const end = encodeKey(boundaries[index + 1]);
	return sorted.filter((key) => {
		const bytes = encodeKey(key);
		return compareBytes(bytes, start) >= 0 && compareBytes(bytes, end) < 0;
	});
}

/** Writes one item and records it in the model. The model holds the version the write returned. */
const putAndRecord = (db: FokosDB, model: QueryModel, hashKey: QueryKey, sortKey: OptionalQueryKey, data: ItemData) =>
	untilAvailable(async () => {
		const res = await db.putItem({ hashKey, sortKey, data });
		recordItem(model, hashKey, sortKey, data, res.version);
	});

/** The leaves one request reads, in the order the walk visits them. */
export async function leavesOf(db: FokosDB, opts: QueryItemsOptions): Promise<string[]> {
	const page = await untilAvailable(() => db.queryItems({ ...opts, select: "count", limit: 100_000 }));
	return page.partitionMetas.map((m) => m.servedByActorName);
}

/** Polls the leaves of the hot key until `accept` holds. It returns them, or undefined after the last try. */
export async function pollLeaves(db: FokosDB, accept: (leaves: string[]) => boolean, attempts = 600): Promise<string[] | undefined> {
	for (let attempt = 0; attempt < attempts; attempt++) {
		const leaves = await leavesOf(db, { queries: [{ hashKey: HOT_HASH_KEY }] });
		if (accept(leaves)) return leaves;
		await sleep(25);
	}
	return undefined;
}

/** Polls as `pollLeaves` does, and fails the suite with `goal` when the tree never gets there. */
async function awaitLeaves(db: FokosDB, accept: (leaves: string[]) => boolean, goal: string): Promise<string[]> {
	const leaves = await pollLeaves(db, accept);
	if (leaves === undefined) throw new Error(`the fixture never reached: ${goal}`);
	return leaves;
}

/**
 * The leaf that owns one sort key. The walk visits the leaves in key order, so this is monotone
 * over the sorted keys, and a bisection finds the boundaries below with fewer calls than one call
 * per key.
 */
async function ownerRank(db: FokosDB, leaves: readonly string[], sortKey: OptionalQueryKey): Promise<number> {
	const value = sortKey ?? "";
	const owners = await leavesOf(db, { queries: [{ hashKey: HOT_HASH_KEY, sortKeyCondition: { op: "eq", value } }] });
	expect(owners, `one leaf owns ${label(sortKey)}`).toHaveLength(1);
	const rank = leaves.indexOf(owners[0]);
	expect(rank, `${owners[0]} is a known leaf`).toBeGreaterThanOrEqual(0);
	return rank;
}

/** The first sort key of every leaf but the first: the child start boundaries of the tree. */
async function findBoundaries(db: FokosDB, leaves: readonly string[], sorted: readonly OptionalQueryKey[]): Promise<QueryKey[]> {
	const boundaries: QueryKey[] = [];
	let low = 0;
	for (let rank = 1; rank < leaves.length; rank++) {
		let lo = low;
		let hi = sorted.length - 1;
		while (lo < hi) {
			const mid = (lo + hi) >> 1;
			if ((await ownerRank(db, leaves, sorted[mid])) >= rank) hi = mid;
			else lo = mid + 1;
		}
		const boundary = sorted[lo];
		expect(boundary, "a boundary key is never the absent sort key").toBeDefined();
		boundaries.push(boundary as QueryKey);
		low = lo;
	}
	return boundaries;
}

export async function buildFixture(): Promise<Fixture> {
	const db = makeTestDB({ hashSplitMaxSizeMb: TREE_HASH_SPLIT_MAX_SIZE_MB, rangeSplitMaxSizeMb: RANGE_SPLIT_MAX_SIZE_MB });
	const model: QueryModel = new Map();

	// Writes land a batch at a time: one put per round trip would spend the build on waiting, and
	// two puts of one batch never name the same key.
	const BUILD_BATCH = 16;
	const putRange = async (start: number, end: number) => {
		for (let i = start; i < end; i += BUILD_BATCH) {
			await Promise.all(
				Array.from({ length: Math.min(BUILD_BATCH, end - i) }, (_, j) => {
					const sortKey = hotSortKey(i + j);
					return putAndRecord(db, model, HOT_HASH_KEY, sortKey, hotData(sortKey));
				}),
			);
		}
	};

	await putRange(0, PROMOTE_ITEMS);
	// The range root must exist before the rest of the payload arrives. Its name carries ".r.".
	await awaitLeaves(db, (leaves) => leaves.some((name) => name.includes(".r.")), "the hot key is promoted");

	await putRange(PROMOTE_ITEMS, HOT_ITEMS);
	await Promise.all([
		...BINARY_SORT_KEYS.map((sortKey) => putAndRecord(db, model, HOT_HASH_KEY, sortKey, hotData(sortKey))),
		...COLD_SORT_KEYS.map((sortKey, index) => putAndRecord(db, model, COLD_HASH_KEY, sortKey, coldData(sortKey, index))),
	]);

	// The tree must grow past its root, and it must stop growing before the properties read it: a
	// split that runs under a property makes a page race the topology it walks.
	let leaves = await awaitLeaves(db, (names) => new Set(names).size >= 4, "the range tree has four leaves");
	let previous: string[] = [];
	for (let attempt = 0; attempt < 40 && previous.join() !== leaves.join(); attempt++) {
		previous = leaves;
		await sleep(100);
		leaves = await leavesOf(db, { queries: [{ hashKey: HOT_HASH_KEY }] });
	}
	expect(leaves.join(), "the tree must settle before the properties run").toBe(previous.join());

	const hotSortKeys: OptionalQueryKey[] = [...Array.from({ length: HOT_ITEMS }, (_, i) => hotSortKey(i)), ...BINARY_SORT_KEYS];
	const sorted = [...hotSortKeys].sort((a, b) => compareBytes(encodeKey(a), encodeKey(b)));
	const boundaries = await findBoundaries(db, leaves, sorted);

	// The fixture empties one middle leaf. A split only ever creates children that hold rows, so an
	// empty leaf in the middle of the walk comes from deletes. It is the child that returns no cursor
	// of its own and carries none of the page, and it must still not break the position the next
	// child resumes from.
	const empty = middleLeafKeys(sorted, boundaries);
	for (let i = 0; i < empty.length; i += BUILD_BATCH) {
		await Promise.all(
			empty.slice(i, i + BUILD_BATCH).map(async (sortKey) => {
				await untilAvailable(() => db.deleteItem({ hashKey: HOT_HASH_KEY, sortKey }));
				model.delete(itemId(HOT_HASH_KEY, sortKey));
			}),
		);
	}
	const emptiedLeaf = await leavesOf(db, {
		queries: [
			{
				hashKey: HOT_HASH_KEY,
				sortKeyCondition: { op: "between", lower: empty[0] as QueryKey, upper: empty[empty.length - 1] as QueryKey },
			},
		],
	});
	expect(emptiedLeaf, "the emptied keys must have belonged to one leaf").toHaveLength(1);

	return { db, model, hotSortKeys: sorted, leaves, boundaries, emptiedKeys: empty };
}

// ─── The request arbitrary of the split suites ────────────────────────────────

/**
 * The bounds a query is built from. A boundary key and its two neighbours are the positions where a
 * page stops on a child edge, and a truncated or extended boundary is the same position inside a
 * gap. The rest are ordinary stored keys, so a bound also lands well inside one leaf.
 */
function boundPool(fixture: Fixture): QueryKey[] {
	const { boundaries, hotSortKeys } = fixture;
	const pool: QueryKey[] = [];
	for (const boundary of boundaries) {
		const index = hotSortKeys.findIndex((key) => compareBytes(encodeKey(key), encodeKey(boundary)) === 0);
		pool.push(boundary);
		for (const neighbour of [hotSortKeys[index - 1], hotSortKeys[index + 1]]) if (neighbour !== undefined) pool.push(neighbour);
		if (typeof boundary === "string") {
			// One bound just inside the previous child, and one just inside this one.
			pool.push(boundary.slice(0, -1), `${boundary}~`);
		}
	}
	// A few ordinary keys, spread over the tree, and the ends of the key space. The pool also draws
	// from the emptied leaf, so a bound lands inside the stretch that holds nothing.
	for (let i = 0; i < hotSortKeys.length; i += Math.ceil(hotSortKeys.length / 6)) pool.push(hotSortKeys[i] as QueryKey);
	const emptied = fixture.emptiedKeys;
	pool.push(emptied[0] as QueryKey, emptied[Math.floor(emptied.length / 2)] as QueryKey, emptied[emptied.length - 1] as QueryKey);
	pool.push("sk", "sz", new Uint8Array([0x01]), new Uint8Array([0xff]));
	return pool;
}

/**
 * The request arbitrary of a split suite. `extraBounds` adds positions the fixture cannot know,
 * such as the keys a suite writes after the build.
 */
export function makeArbRequest(fixture: Fixture, extraBounds: readonly QueryKey[] = []) {
	const arbBound = fc.constantFrom(...boundPool(fixture), ...extraBounds);
	const arbRangeEnd = fc.option(fc.record({ value: arbBound, inclusive: fc.boolean() }), { nil: undefined });
	const arbSortKeyCondition: fc.Arbitrary<SortKeyCondition> = fc.oneof(
		fc
			.tuple(fc.constantFrom("eq" as const, "lt" as const, "lte" as const, "gt" as const, "gte" as const), arbBound)
			.map(([op, value]) => ({ op, value })),
		fc.tuple(arbBound, arbBound).map(([lower, upper]) => ({ op: "between" as const, lower, upper })),
		arbBound.map((prefix) => ({ op: "begins_with" as const, prefix })),
		fc.tuple(arbRangeEnd, arbRangeEnd).map(([lower, upper]) => ({ op: "range" as const, lower, upper })),
	);

	const arbQuery: fc.Arbitrary<Query> = fc.record({
		// The hot key is the tree. The arbitrary draws the cold key too, so a request mixes a walk
		// over several children with a sub-query that one leaf answers on its own.
		hashKey: fc.oneof({ arbitrary: fc.constant(HOT_HASH_KEY), weight: 5 }, { arbitrary: fc.constant(COLD_HASH_KEY), weight: 1 }),
		// A narrowing condition is drawn far more often than a whole-tree scan: a bound near a child
		// edge is the case this suite exists for, and a whole-tree scan costs every item of the tree.
		sortKeyCondition: fc.oneof({ arbitrary: fc.constant(undefined), weight: 1 }, { arbitrary: arbSortKeyCondition, weight: 5 }),
		scanIndexForward: fc.boolean(),
	});

	return fc.record({
		queries: fc.array(arbQuery, { minLength: 1, maxLength: 2 }),
		budget: fc.nat({ max: 5 }),
	});
}

// ─── The page budget of the split suites ──────────────────────────────────────

/** About what one stored value costs in a response. It sizes a byte budget, so an estimate is enough. */
const dataBytes = (data: ItemData): number =>
	typeof data === "string" ? data.length : data instanceof Uint8Array ? data.byteLength : JSON.stringify(data).length;

// A page smaller than this never needs more round trips than `pageBudget` allows, whatever the
// request matched. Above it, the budgets below widen so a whole-tree scan stays a few pages.
const TIGHT_PAGE_MAX_ITEMS = 40;

/**
 * The page budget of one run. It depends on what the request matched, so the run chooses it here
 * instead of a draw: the tightest page holds one item, and a whole-tree scan under it would need
 * one round trip per item of the tree.
 *
 * Choices 3 and 4 size the page from the answer itself, so a page holds far more items than one
 * child owns: those are the pages that stop in one child and then carry the walk into the next
 * children of the same page. Choice 5 parks the stop on the empty leaf, which is the child that
 * returns no position of its own and that the walk must still carry a page past.
 */
export function budgetFor(
	choice: number,
	expected: readonly QueryModelItem[],
	fixture: Fixture,
): { limit?: number; maxResponseBytes?: number } {
	const expectedCount = expected.length;
	const tight = expectedCount <= TIGHT_PAGE_MAX_ITEMS;
	const share = (pages: number) => Math.max(2, Math.ceil(expectedCount / pages));
	// The mean size of what the request matched. A byte budget is written as a number of items, so it
	// admits about as many as the item budget beside it whatever sizes the answer mixes. A fixed size
	// would turn a page of large items into one item, and a whole scan into one page per item.
	const meanBytes =
		expectedCount === 0 ? ITEM_BYTES : Math.max(1, Math.ceil(expected.reduce((n, i) => n + dataBytes(i.data), 0) / expectedCount));
	switch (choice) {
		case 0:
			return {};
		case 1:
			// One evaluated item per page, so every page stops, and most stops are inside a child.
			return { limit: tight ? 1 : 16 };
		case 2:
			// `maxResponseBytes: 1` admits only the one item that the oversized-first-item rule always
			// allows, and that rule applies once per page and not once per child.
			return { limit: tight ? 3 : 32, maxResponseBytes: tight ? 1 : 16 * meanBytes };
		case 3:
			return { limit: share(2) };
		case 4:
			return { limit: share(3), maxResponseBytes: share(3) * meanBytes };
		default: {
			const upToTheEmptyLeaf = countBeforeEmptyLeaf(expected, fixture);
			return upToTheEmptyLeaf > 0 && upToTheEmptyLeaf < expectedCount ? { limit: upToTheEmptyLeaf } : { limit: share(2) };
		}
	}
}

/**
 * How many of the matched items the walk returns before it reaches the empty leaf. A page limited
 * to exactly that many stops with the empty leaf next, which is the position a later child must
 * still be reachable from.
 */
function countBeforeEmptyLeaf(expected: readonly QueryModelItem[], fixture: Fixture): number {
	const emptied = fixture.emptiedKeys;
	const start = encodeKey(emptied[0]);
	const end = encodeKey(emptied[emptied.length - 1]);
	const index = expected.findIndex((item) => compareBytes(item.sortKeyBytes, start) >= 0 && compareBytes(item.sortKeyBytes, end) <= 0);
	// The answer never enters the emptied stretch, so no page can stop on it.
	if (index >= 0) return 0;
	const before = expected.filter((item) => compareBytes(item.sortKeyBytes, start) < 0).length;
	const after = expected.filter((item) => compareBytes(item.sortKeyBytes, end) > 0).length;
	// The walk reaches the empty leaf from below when it scans upwards, and from above when it does not.
	return expected.length > 0 && compareBytes(expected[0].sortKeyBytes, end) > 0 ? after : before;
}
