// Property-based tests for queryItems over a key whose partition has split into a range tree.
//
// The single-partition suite (query-items.test.ts) never leaves one leaf, so it never runs the
// client's range walk. This suite promotes one hash key into a range tree of several leaves and
// then queries it. Every page therefore crosses child boundaries: the walk clips the interval to
// each child, carries one page budget across the children, and turns a stopped child into a cursor
// the next page must resume from exactly. A gap, a duplicate, a lost boundary item, or a cursor
// that does not resume shows up as a mismatch with the model.
//
// The fixture is built once. Queries change nothing, so every run and every property shares it, and
// one run costs only the pages it drains. The model and the key oracle are the ones the
// single-partition suite uses, in query-model.ts.
//
// A failure prints `seed` and `path`. See item-crud.test.ts for how to replay them.
import fc from "fast-check";
import { beforeAll, describe, expect, it } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import type { QueryItemsOptions, QueryItemsResult, SortKeyCondition } from "../../src/shared/types.js";
import { expectedDataKind, makeTestDB, propertyRuns, type ItemData } from "./arbitraries.js";
import { untilAvailable } from "./model.js";
import {
	compareBytes,
	drainQuery,
	encodeKey,
	expectedItems,
	itemId,
	pageBudget,
	publicItem,
	type ModelItem,
	type Model,
	type OptionalQueryKey,
	type Query,
	type QueryKey,
} from "./query-model.js";

const SUITE_TIMEOUT_MS = 600_000;

// ─── The fixture ────────────────────────────────────────────────────────────────

// A key is promoted into a range tree once it holds `hashSplitMaxSizeMb * RANGE_PROMOTION_FRACTION`
// bytes, and a range partition splits once it holds `rangeSplitMaxSizeMb`. A range partition also
// REFUSES a write above 1.1 times its own threshold, and only a write that applies can queue the
// split that would bring it back under. The seed therefore runs in two phases: it writes just past
// the promotion threshold, waits for the range root to exist, and only then writes the rest. A
// single phase would hand the fresh range root every byte at once, far above the refusal point,
// and the tree would never grow past its root.
const HASH_SPLIT_MAX_SIZE_MB = 1;
const RANGE_SPLIT_MAX_SIZE_MB = 0.5;
const PROMOTE_ITEMS = 72;
const HOT_ITEMS = 340;
const ITEM_BYTES = 4 * 1024;

const HOT_HASH_KEY = "range:hot";
// A second hash key on an ordinary leaf of the same table. A request that names both reads a range
// tree and a plain partition in one page, which is where the client carries one budget across two
// sub-queries of different shapes.
const COLD_HASH_KEY = "plain:cold";

const hotSortKey = (i: number) => `sk${String(i).padStart(4, "0")}`;
// A binary sort key carries a 0xFF tag, so these sort above every string key and land in the last
// leaf of the tree. They keep the byte order of the walk from being a pure string comparison.
const BINARY_SORT_KEYS: Uint8Array[] = [
	new Uint8Array([0x01]),
	new Uint8Array([0x01, 0x02]),
	new Uint8Array([0x7f]),
	new Uint8Array([0xff]),
];
// The cold partition holds few items, and one of them has no sort key at all (the byte minimum).
const COLD_SORT_KEYS: OptionalQueryKey[] = [undefined, "a", "aa", "ab", "b", "b#1", "b#2", "c", "d", new Uint8Array([0x02])];

const label = (sortKey: OptionalQueryKey) => (typeof sortKey === "string" ? sortKey : encodeKey(sortKey).toHex());
/** The payload names its own key, so an item that a page puts in the wrong place fails the comparison. */
const hotData = (sortKey: OptionalQueryKey): string => `${label(sortKey)}:`.padEnd(ITEM_BYTES, "x");
const coldData = (sortKey: OptionalQueryKey, index: number): ItemData =>
	index % 3 === 0 ? { key: label(sortKey), n: index } : index % 3 === 1 ? `cold:${label(sortKey)}` : new Uint8Array([index, 0x00, 0xff]);

type Fixture = {
	db: FokosDB;
	model: Model;
	hotSortKeys: OptionalQueryKey[];
	leaves: string[];
	boundaries: QueryKey[];
	emptiedKeys: OptionalQueryKey[];
};

/**
 * The sort keys of the leaf the fixture empties: everything between one start boundary and the next.
 * It is a leaf at an odd position, so in a tree of two-way splits it is the SECOND child of its own
 * router — the child whose empty answer the router must carry past instead of ending the page on.
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

const put = (db: FokosDB, model: Model, hashKey: QueryKey, sortKey: OptionalQueryKey, data: ItemData) =>
	untilAvailable(async () => {
		const res = await db.putItem({ hashKey, sortKey, data });
		model.set(itemId(hashKey, sortKey), {
			hashKey,
			sortKey,
			hashKeyBytes: encodeKey(hashKey),
			sortKeyBytes: encodeKey(sortKey),
			data,
			kind: expectedDataKind(data),
			version: res.version,
		});
	});

/** The leaves one request reads, in the order the walk visits them. */
async function leavesOf(db: FokosDB, opts: QueryItemsOptions): Promise<string[]> {
	const page = await untilAvailable(() => db.queryItems({ ...opts, select: "count", limit: 100_000 }));
	return page.partitionMetas.map((m) => m.servedByActorName);
}

/** Polls until `check` holds on the leaves of a full scan of the hot key, and returns them. */
async function awaitLeaves(db: FokosDB, check: (leaves: string[]) => boolean, goal: string): Promise<string[]> {
	for (let attempt = 0; attempt < 600; attempt++) {
		const leaves = await leavesOf(db, { queries: [{ hashKey: HOT_HASH_KEY }] });
		if (check(leaves)) return leaves;
		await new Promise((resolve) => setTimeout(resolve, 25));
	}
	throw new Error(`the fixture never reached: ${goal}`);
}

/**
 * The leaf that owns one sort key. The walk visits the leaves in key order, so this is monotone over
 * the sorted keys and the boundaries below can be found by bisection instead of one call per key.
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

async function buildFixture(): Promise<Fixture> {
	const db = makeTestDB({ hashSplitMaxSizeMb: HASH_SPLIT_MAX_SIZE_MB, rangeSplitMaxSizeMb: RANGE_SPLIT_MAX_SIZE_MB });
	const model: Model = new Map();

	for (let i = 0; i < PROMOTE_ITEMS; i++) await put(db, model, HOT_HASH_KEY, hotSortKey(i), hotData(hotSortKey(i)));
	// The range root must exist before the rest of the payload arrives. Its name carries ".r.".
	await awaitLeaves(db, (leaves) => leaves.some((name) => name.includes(".r.")), "the hot key is promoted");

	for (let i = PROMOTE_ITEMS; i < HOT_ITEMS; i++) await put(db, model, HOT_HASH_KEY, hotSortKey(i), hotData(hotSortKey(i)));
	for (const sortKey of BINARY_SORT_KEYS) await put(db, model, HOT_HASH_KEY, sortKey, hotData(sortKey));
	for (const [index, sortKey] of COLD_SORT_KEYS.entries()) await put(db, model, COLD_HASH_KEY, sortKey, coldData(sortKey, index));

	// The tree must have grown past its root, and it must stop growing before the properties read it:
	// a split that runs under a property would make a page race the topology it is walking.
	let leaves = await awaitLeaves(db, (names) => new Set(names).size >= 4, "the range tree has four leaves");
	let previous: string[] = [];
	for (let attempt = 0; attempt < 40 && previous.join() !== leaves.join(); attempt++) {
		previous = leaves;
		await new Promise((resolve) => setTimeout(resolve, 100));
		leaves = await leavesOf(db, { queries: [{ hashKey: HOT_HASH_KEY }] });
	}
	expect(leaves.join(), "the tree must settle before the properties run").toBe(previous.join());

	const hotSortKeys: OptionalQueryKey[] = [...Array.from({ length: HOT_ITEMS }, (_, i) => hotSortKey(i)), ...BINARY_SORT_KEYS];
	const sorted = [...hotSortKeys].sort((a, b) => compareBytes(encodeKey(a), encodeKey(b)));
	const boundaries = await findBoundaries(db, leaves, sorted);

	// One middle leaf is emptied. A split only ever creates children that hold rows, so an empty leaf
	// in the middle of the walk comes from deletes — and it is the child that returns no cursor of its
	// own, carries none of the page, and must still not break the position the next child resumes from.
	const empty = middleLeafKeys(sorted, boundaries);
	for (const sortKey of empty) {
		await untilAvailable(() => db.deleteItem({ hashKey: HOT_HASH_KEY, sortKey }));
		model.delete(itemId(HOT_HASH_KEY, sortKey));
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

// ─── The scenario ───────────────────────────────────────────────────────────────

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
	// A few ordinary keys, spread over the tree, and the ends of the key space. The emptied leaf is
	// drawn from too, so a bound also lands inside the stretch that holds nothing.
	for (let i = 0; i < hotSortKeys.length; i += Math.ceil(hotSortKeys.length / 6)) pool.push(hotSortKeys[i] as QueryKey);
	const emptied = fixture.emptiedKeys;
	pool.push(emptied[0] as QueryKey, emptied[Math.floor(emptied.length / 2)] as QueryKey, emptied[emptied.length - 1] as QueryKey);
	pool.push("sk", "sz", new Uint8Array([0x01]), new Uint8Array([0xff]));
	return pool;
}

function makeArbRequest(fixture: Fixture) {
	const arbBound = fc.constantFrom(...boundPool(fixture));
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
		// The hot key is the tree. The cold key is drawn too, so a request mixes a walk over several
		// children with a sub-query that one leaf answers on its own.
		hashKey: fc.oneof({ arbitrary: fc.constant(HOT_HASH_KEY), weight: 5 }, { arbitrary: fc.constant(COLD_HASH_KEY), weight: 1 }),
		// A narrowing condition is drawn far more often than a whole-tree scan: a bound near a child
		// edge is the case this suite exists for, and a whole-tree scan costs 340 items per drain.
		sortKeyCondition: fc.oneof({ arbitrary: fc.constant(undefined), weight: 1 }, { arbitrary: arbSortKeyCondition, weight: 5 }),
		scanIndexForward: fc.boolean(),
	});

	return fc.record({
		queries: fc.array(arbQuery, { minLength: 1, maxLength: 2 }),
		budget: fc.nat({ max: 5 }),
	});
}

// A page smaller than this never needs more round trips than `pageBudget` allows, whatever the
// request matched. Above it, the budgets below widen so a whole-tree scan stays a few pages.
const TIGHT_PAGE_MAX_ITEMS = 40;

/**
 * The page budget of one run. It depends on what the request matched, so it is chosen here and not
 * drawn: the tightest page holds one item, and a whole-tree scan under it would need one round trip
 * per item of the tree.
 *
 * Choices 3 and 4 size the page from the answer itself, so a page holds far more items than one
 * child owns: those are the pages that stop in one child and then carry the walk into the next
 * children of the same page. Choice 5 parks the stop on the empty leaf, which is the child that
 * returns no position of its own and that the walk must still carry a page past.
 */
function budgetFor(choice: number, expected: readonly ModelItem[], fixture: Fixture): { limit?: number; maxResponseBytes?: number } {
	const expectedCount = expected.length;
	const tight = expectedCount <= TIGHT_PAGE_MAX_ITEMS;
	const share = (pages: number) => Math.max(2, Math.ceil(expectedCount / pages));
	switch (choice) {
		case 0:
			return {};
		case 1:
			// One evaluated item per page, so every page stops, and most stops are inside a child.
			return { limit: tight ? 1 : 16 };
		case 2:
			// `maxResponseBytes: 1` admits only the one item the oversized-first-item rule always
			// allows, and that rule applies once per page and not once per child.
			return { limit: tight ? 3 : 32, maxResponseBytes: tight ? 1 : 16 * ITEM_BYTES };
		case 3:
			return { limit: share(2) };
		case 4:
			return { limit: share(3), maxResponseBytes: share(3) * ITEM_BYTES };
		default: {
			const upToTheEmptyLeaf = countBeforeEmptyLeaf(expected, fixture);
			return upToTheEmptyLeaf > 0 && upToTheEmptyLeaf < expectedCount ? { limit: upToTheEmptyLeaf } : { limit: share(2) };
		}
	}
}

/**
 * How many of the matched items the walk returns before it reaches the empty leaf. A page limited to
 * exactly that many stops with the empty leaf next, which is the position a later child must still
 * be reachable from.
 */
function countBeforeEmptyLeaf(expected: readonly ModelItem[], fixture: Fixture): number {
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

/**
 * A cursor must not promise a page that cannot exist. The walk emits one when a later child of the
 * tree could still hold rows; once it has reached the last child of its direction AND the request
 * has matched everything the model holds, no later child is left and the answer is complete, so a
 * cursor there buys a round trip that returns nothing.
 *
 * It applies to a request of ONE sub-query. A request that names several carries its cursor from one
 * to the next, and no page can know whether the next sub-query matches anything.
 */
function expectCursorHasSomewhereToGo(
	fixture: Fixture,
	queries: readonly Query[],
	page: QueryItemsResult,
	matchedSoFar: number,
	expectedCount: number,
): void {
	if (queries.length !== 1 || page.cursor === undefined || matchedSoFar < expectedCount) return;
	const lastChildOfTheWalk = queries[0].scanIndexForward === false ? fixture.leaves[0] : fixture.leaves[fixture.leaves.length - 1];
	expect(
		page.partitionMetas.map((meta) => meta.servedByActorName),
		"a cursor was emitted at the last child of the walk, with the whole answer already returned",
	).not.toContain(lastChildOfTheWalk);
}

describe("FokosDB queryItems over a range tree — model-based properties", () => {
	let fixture: Fixture;

	beforeAll(async () => {
		fixture = await buildFixture();
		// Without this the suite would silently fall back to the single-partition case that
		// query-items.test.ts already covers.
		expect(new Set(fixture.leaves).size, "the hot key must be spread over several range leaves").toBeGreaterThanOrEqual(4);
		expect(fixture.boundaries).toHaveLength(new Set(fixture.leaves).size - 1);
	}, SUITE_TIMEOUT_MS);

	it("a drained request returns exactly the model's items, in key order", { timeout: SUITE_TIMEOUT_MS }, async () => {
		await fc.assert(
			fc.asyncProperty(makeArbRequest(fixture), async ({ queries, budget }) => {
				const expected = queries.flatMap((query) => expectedItems(fixture.model, query));
				const opts: QueryItemsOptions = { queries, ...budgetFor(budget, expected, fixture) };
				let matched = 0;
				const drained = await drainQuery(fixture.db, opts, pageBudget(expected.length, queries.length), (page) => {
					matched += page.count;
					expectCursorHasSomewhereToGo(fixture, queries, page, matched, expected.length);
				});

				expect(drained.items).toEqual(expected.map(publicItem));
				expect(drained.count).toBe(expected.length);
				// No filter is sent, so every evaluated candidate is also a matched one.
				expect(drained.scannedCount).toBe(expected.length);
			}),
			{ numRuns: propertyRuns(40) },
		);
	});

	it("count mode counts the same items and materializes none", { timeout: SUITE_TIMEOUT_MS }, async () => {
		await fc.assert(
			fc.asyncProperty(makeArbRequest(fixture), async ({ queries, budget }) => {
				const expected = queries.flatMap((query) => expectedItems(fixture.model, query));
				const opts: QueryItemsOptions = { queries, ...budgetFor(budget, expected, fixture), select: "count" };
				let matched = 0;
				const drained = await drainQuery(fixture.db, opts, pageBudget(expected.length, queries.length), (page) => {
					matched += page.count;
					expectCursorHasSomewhereToGo(fixture, queries, page, matched, expected.length);
				});

				expect(drained.items).toHaveLength(0);
				expect(drained.count).toBe(expected.length);
				expect(drained.scannedCount).toBe(expected.length);
			}),
			{ numRuns: propertyRuns(25) },
		);
	});

	it("a cursor answers the same page every time it is sent", { timeout: SUITE_TIMEOUT_MS }, async () => {
		await fc.assert(
			fc.asyncProperty(makeArbRequest(fixture), async ({ queries, budget }) => {
				const expected = queries.flatMap((query) => expectedItems(fixture.model, query));
				const opts: QueryItemsOptions = { queries, ...budgetFor(budget, expected, fixture) };

				// Nothing writes to the fixture, so a cursor names one position for as long as it exists.
				// A cursor that carries a child boundary must resolve to that same position on every send,
				// whichever child the walk enters it from.
				const first = await fixture.db.queryItems(opts);
				if (first.cursor === undefined) return;
				const again: QueryItemsResult = await fixture.db.queryItems({ ...opts, cursor: first.cursor });
				const onceMore: QueryItemsResult = await fixture.db.queryItems({ ...opts, cursor: first.cursor });

				expect(again.items).toEqual(onceMore.items);
				expect(again.count).toBe(onceMore.count);
				expect(again.scannedCount).toBe(onceMore.scannedCount);
				expect(again.cursor).toBe(onceMore.cursor);
				// The two pages must also be the right two pages. A request can name one hash key twice, so
				// the same item can appear more than once in the answer; the model's sequence is compared by
				// position, and the resumed page must continue exactly where the first one stopped.
				const expectedItemsOfPage = expected.map(publicItem);
				expect(first.items).toEqual(expectedItemsOfPage.slice(0, first.items.length));
				expect(again.items).toEqual(expectedItemsOfPage.slice(first.items.length, first.items.length + again.items.length));
			}),
			{ numRuns: propertyRuns(25) },
		);
	});
});
