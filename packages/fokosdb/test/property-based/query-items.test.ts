// Property-based tests for queryItems. A query page is where the client-side paging logic lives:
// one budget carries across the sub-queries, a cursor carries the position between pages, and the
// keys and the json data are decoded at this boundary. Each property seeds a table with a random
// write sequence, drains every page of a random request, and compares the whole sequence with an
// in-memory model. A gap, a duplicate, a wrong order, or a lost boundary item fails the run.
//
// The key oracle and the model live in query-model.ts, and query-items-split.test.ts compares
// against the same ones.
//
// A failure prints `seed` and `path`. See item-crud.test.ts for how to replay them.
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import type { QueryItemsOptions, SortKeyCondition } from "../../src/shared/types.js";
import { arbItemData, expectedDataKind, makeTestDB, propertyRuns, type ItemData } from "./arbitraries.js";
import {
	drainQuery,
	encodeKey,
	expectedItems,
	itemId,
	pageBudget,
	publicItem,
	type Model,
	type OptionalQueryKey,
	type Query,
	type QueryKey,
} from "./query-model.js";

// Every property runs many scenarios against real Durable Objects, and shrinking a failure runs
// many more. The default 5 s vitest timeout would hide the counterexample.
const PROPERTY_TIMEOUT_MS = 300_000;

// ─── The scenario ───────────────────────────────────────────────────────────────

const bin = (...bytes: number[]) => new Uint8Array(bytes);

// Two string hash keys and one binary hash key, so a query decodes both key types back. The seed
// draws the first one far more often, so one partition holds enough sort keys to fill many pages.
const HASH_KEYS: QueryKey[] = ["hk-a", "hk-b", bin(0x2a)];

// Sort keys of one partition. They share prefixes and include an absent key (the byte minimum) and
// binary keys (which the 0xFF tag sorts above every string), so the order of a page is not trivial.
const SORT_KEYS: OptionalQueryKey[] = [
	undefined,
	"a",
	"aa",
	"ab",
	"abc",
	"b",
	"b#1",
	"b#2",
	"bb",
	"c",
	"cc",
	"d",
	bin(0x01),
	bin(0x01, 0x02),
	bin(0x02),
];

// Query bounds. Half of them are stored sort keys and half fall between two of them, so a bound
// lands on an item as often as it lands in a gap.
const BOUNDS: QueryKey[] = ["a", "aa", "ab", "b", "b#", "b#1", "c", "d", bin(0x01), bin(0x01, 0x01), bin(0x02)];

const arbHashKey = fc.oneof(
	{ arbitrary: fc.constant(HASH_KEYS[0]), weight: 4 },
	{ arbitrary: fc.constantFrom(...HASH_KEYS.slice(1)), weight: 1 },
);
const arbSortKey = fc.constantFrom(...SORT_KEYS);
const arbBound = fc.constantFrom(...BOUNDS);
const arbRangeEnd = fc.option(fc.record({ value: arbBound, inclusive: fc.boolean() }), { nil: undefined });

const arbSortKeyCondition: fc.Arbitrary<SortKeyCondition> = fc.oneof(
	fc
		.tuple(fc.constantFrom("eq" as const, "lt" as const, "lte" as const, "gt" as const, "gte" as const), arbBound)
		.map(([op, value]) => ({ op, value })),
	fc.tuple(arbBound, arbBound).map(([lower, upper]) => ({ op: "between" as const, lower, upper })),
	// An empty prefix is the one legitimate empty bound: it selects everything.
	fc.oneof(arbBound, fc.constant(""), fc.constant(bin())).map((prefix) => ({ op: "begins_with" as const, prefix })),
	fc.tuple(arbRangeEnd, arbRangeEnd).map(([lower, upper]) => ({ op: "range" as const, lower, upper })),
);

type SeedOp =
	| { op: "put"; hashKey: QueryKey; sortKey: OptionalQueryKey; data: ItemData }
	| { op: "delete"; hashKey: QueryKey; sortKey: OptionalQueryKey };

// A put carries a random payload and repeats a key often, so a query reads overwritten versions. A
// delete makes a query skip a key that the partition once held.
const arbSeedOp: fc.Arbitrary<SeedOp> = fc.oneof(
	{
		arbitrary: fc
			.tuple(arbHashKey, arbSortKey, arbItemData)
			.map(([hashKey, sortKey, data]) => ({ op: "put" as const, hashKey, sortKey, data })),
		weight: 4,
	},
	{ arbitrary: fc.tuple(arbHashKey, arbSortKey).map(([hashKey, sortKey]) => ({ op: "delete" as const, hashKey, sortKey })), weight: 1 },
);

const arbSeed = fc.array(arbSeedOp, { minLength: 1, maxLength: 26 });

const arbQuery: fc.Arbitrary<Query> = fc.record({
	hashKey: arbHashKey,
	// A query with no condition reads the whole partition, which is the request that fills the most
	// pages, so it is drawn as often as all the narrowing conditions together.
	sortKeyCondition: fc.oneof({ arbitrary: fc.constant(undefined), weight: 1 }, { arbitrary: arbSortKeyCondition, weight: 2 }),
	scanIndexForward: fc.boolean(),
});

// A small limit and a small byte budget stop a page in the middle of a partition, which is the
// position a cursor must resume from exactly. `maxResponseBytes: 1` also holds the page to the one
// item that the oversized-first-item rule always admits.
const arbRequest = fc.record({
	queries: fc.array(arbQuery, { minLength: 1, maxLength: 3 }),
	limit: fc.oneof({ arbitrary: fc.constant(undefined), weight: 1 }, { arbitrary: fc.constantFrom(1, 2, 3, 7), weight: 3 }),
	maxResponseBytes: fc.oneof({ arbitrary: fc.constant(undefined), weight: 1 }, { arbitrary: fc.constantFrom(1, 64, 4096), weight: 3 }),
});

const arbScenario = fc.record({ seed: arbSeed, requests: fc.array(arbRequest, { minLength: 1, maxLength: 4 }) });

/** Runs the write sequence against the database and returns the state it must leave behind. */
async function applySeed(db: FokosDB, ops: readonly SeedOp[]): Promise<Model> {
	const model: Model = new Map();
	for (const op of ops) {
		const id = itemId(op.hashKey, op.sortKey);
		if (op.op === "put") {
			const version = (model.get(id)?.version ?? 0) + 1;
			const res = await db.putItem({ hashKey: op.hashKey, sortKey: op.sortKey, data: op.data });
			expect(res.version).toBe(version);
			model.set(id, {
				hashKey: op.hashKey,
				sortKey: op.sortKey,
				hashKeyBytes: encodeKey(op.hashKey),
				sortKeyBytes: encodeKey(op.sortKey),
				data: op.data,
				kind: expectedDataKind(op.data),
				version,
			});
		} else {
			const res = await db.deleteItem({ hashKey: op.hashKey, sortKey: op.sortKey });
			expect(res.deleted).toBe(model.has(id));
			model.delete(id);
		}
	}
	return model;
}

describe("FokosDB queryItems — model-based properties", () => {
	it("a drained request returns exactly the model's items, in key order", { timeout: PROPERTY_TIMEOUT_MS }, async () => {
		await fc.assert(
			fc.asyncProperty(arbScenario, async ({ seed, requests }) => {
				const db = makeTestDB();
				const model = await applySeed(db, seed);

				for (const request of requests) {
					const expected = request.queries.flatMap((query) => expectedItems(model, query));
					const drained = await drainQuery(db, request, pageBudget(expected.length, request.queries.length));

					expect(drained.items).toEqual(expected.map(publicItem));
					expect(drained.count).toBe(expected.length);
					// No filter is sent, so every evaluated candidate is also a matched one.
					expect(drained.scannedCount).toBe(expected.length);
				}
			}),
			{ numRuns: propertyRuns(25) },
		);
	});

	it("count mode counts the same items and materializes none", { timeout: PROPERTY_TIMEOUT_MS }, async () => {
		await fc.assert(
			fc.asyncProperty(arbScenario, async ({ seed, requests }) => {
				const db = makeTestDB();
				const model = await applySeed(db, seed);

				for (const request of requests) {
					const expected = request.queries.flatMap((query) => expectedItems(model, query));
					const opts: QueryItemsOptions = { ...request, select: "count" };
					const drained = await drainQuery(db, opts, pageBudget(expected.length, request.queries.length));

					expect(drained.items).toHaveLength(0);
					expect(drained.count).toBe(expected.length);
					expect(drained.scannedCount).toBe(expected.length);
				}
			}),
			{ numRuns: propertyRuns(15) },
		);
	});
});
