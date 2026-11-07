import { runInDurableObject } from "cloudflare:test";
import fc from "fast-check";
import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from "vitest";
import type { FokosDBPartitionDO } from "../../server/do-partition.js";
import { testPartitionStub } from "../../../test/stub-helpers.js";
import { collidingHashKeys } from "../../../test/hash-digits.js";
import { propertyRuns } from "../../../test/property-based/harness.js";
import { KeyCodec, type FokosCompositeKey, type KeyBytes } from "../../sharding/key-codec.js";
import { routingHashOf, type RoutingHash } from "../../sharding/hash-primitives.js";
import { estimate, itemFacts, ITEMS_MODEL, overflowBytesSql, type ItemFacts } from "./item-size.js";
import { ItemStats } from "./item-stats.js";
import { PartitionStore, type ItemLinkId } from "./partition-store.js";
import { planRangeBoundaries, SplitPlanner } from "./split-planner.js";

const GROUP = "fokos.p.item-stats-range-test";
const DEFECT = "fokos/partition: the facts of a bucket disagree with its items, so they are counted again";
const kb = (v: string | Uint8Array) => KeyCodec.encode(v);

// The two hash keys of the range leaf. They have one routingHash, as two hash keys of one promoted
// routingHash do.
const [HK_A, HK_B] = collidingHashKeys(GROUP).map((raw) => KeyCodec.encode(raw));
const RH: RoutingHash = routingHashOf(HK_A, GROUP);
const [HK_LOW, HK_HIGH] = KeyCodec.compare(HK_A, HK_B) < 0 ? [HK_A, HK_B] : [HK_B, HK_A];

// Each test of this file fails when the defect log appears, except a test that breaks a bucket on
// purpose and sets `expectedDefects`.
let errors: MockInstance<typeof console.error>;
let expectedDefects = 0;
beforeEach(() => {
	errors = vi.spyOn(console, "error");
	expectedDefects = 0;
});
afterEach(() => {
	const defects = errors.mock.calls.filter(([entry]) => (entry as { message?: string } | undefined)?.message === DEFECT);
	expect(defects).toHaveLength(expectedDefects);
});

type Ctx = { state: DurableObjectState; storeWith: (K: number) => PartitionStore };

/** Runs `fn` over one storage. `storeWith(K)` gives a store of a range leaf with the bucket size `K`. */
async function withRangeLeaf(fn: (ctx: Ctx) => void | Promise<void>): Promise<void> {
	const stub = testPartitionStub(`item-stats-range-test.${crypto.randomUUID()}`);
	await runInDurableObject(stub, async (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
		await fn({ state, storeWith: (K) => new PartitionStore(state.storage, { rangeLeaf: () => true, bucketItems: K }) });
	});
}

type BucketRow = { bound: FokosCompositeKey; facts: ItemFacts; promotionEst: number | null };

function buckets(state: DurableObjectState): BucketRow[] {
	return state.storage.sql
		.exec<{
			lower_hk: ArrayBuffer;
			lower_sk: ArrayBuffer;
			item_count: number;
			item_bytes: number;
			key_bytes: number;
			indexed_item_count: number;
			indexed_key_bytes: number;
			overflow_bytes: number;
			promotion_est_bytes: number | null;
		}>(`SELECT * FROM item_stats WHERE routing_hash = ? ORDER BY lower_hk, lower_sk`, RH)
		.toArray()
		.map((r) => ({
			bound: { hk: KeyCodec.asKeyBytes(new Uint8Array(r.lower_hk)), sk: KeyCodec.asKeyBytes(new Uint8Array(r.lower_sk)) },
			facts: {
				itemCount: r.item_count,
				itemBytes: r.item_bytes,
				keyBytes: r.key_bytes,
				indexedItemCount: r.indexed_item_count,
				indexedKeyBytes: r.indexed_key_bytes,
				overflowBytes: r.overflow_bytes,
			},
			promotionEst: r.promotion_est_bytes,
		}));
}

/** The sums over the items in `[from, to)`, from SQL over the item rows. */
function sqlSums(state: DurableObjectState, from: FokosCompositeKey, to: FokosCompositeKey | null): ItemFacts {
	const r = state.storage.sql
		.exec<{ c: number; b: number; k: number; ic: number; ik: number; o: number }>(
			`SELECT COUNT(*) AS c, COALESCE(SUM(item_bytes), 0) AS b, COALESCE(SUM(octet_length(hk) + octet_length(sk)), 0) AS k,
			        COALESCE(SUM(ttl_epoch_utc_seconds IS NOT NULL), 0) AS ic,
			        COALESCE(SUM(CASE WHEN ttl_epoch_utc_seconds IS NOT NULL THEN octet_length(hk) + octet_length(sk) ELSE 0 END), 0) AS ik,
			        COALESCE(SUM(${overflowBytesSql(ITEMS_MODEL, "item_bytes")}), 0) AS o
			   FROM items WHERE routing_hash = ?1 AND (hk, sk) >= (?2, ?3) ${to === null ? "" : "AND (hk, sk) < (?4, ?5)"}`,
			RH,
			from.hk,
			from.sk,
			...(to === null ? [] : [to.hk, to.sk]),
		)
		.one();
	return { itemCount: r.c, itemBytes: r.b, keyBytes: r.k, indexedItemCount: r.ic, indexedKeyBytes: r.ik, overflowBytes: r.o };
}

/**
 * Checks the buckets of the range leaf: the first bucket starts at the smallest key while items
 * exist, each bucket equals the SQL sums of its key range (so the buckets cover the items with no gap
 * and no overlap), each bucket holds at most `maxItems` items, two neighbours hold at least `K` items
 * together, and no bucket stores a promotion estimate.
 */
function expectBuckets(state: DurableObjectState, K: number, maxItems = 2 * K): BucketRow[] {
	const rows = buckets(state);
	const first = { hk: KeyCodec.asKeyBytes(new Uint8Array()), sk: KeyCodec.asKeyBytes(new Uint8Array()) };
	const total = sqlSums(state, first, null);
	if (total.itemCount === 0) {
		expect(rows).toEqual([]);
		return rows;
	}
	expect(rows[0].bound).toEqual(first);
	for (const [i, row] of rows.entries()) {
		const next = rows[i + 1]?.bound ?? null;
		expect(row.facts, `bucket ${i}`).toEqual(sqlSums(state, i === 0 ? first : row.bound, next));
		expect(row.facts.itemCount).toBeLessThanOrEqual(maxItems);
		expect(row.promotionEst).toBeNull();
		if (i > 0) {
			expect(rows[i - 1].facts.itemCount + row.facts.itemCount, `buckets ${i - 1} and ${i}`).toBeGreaterThanOrEqual(K);
		}
	}
	return rows;
}

const put = (store: PartitionStore, hk: KeyBytes, sk: string, size: number, ttlAt: number | null = null) =>
	store.upsertItem({ routingHash: RH, hk, sk: kb(sk), data: "x".repeat(size), kind: "text", ttlAt, txOrderTs: 1 });
const del = (store: PartitionStore, hk: KeyBytes, sk: string | KeyBytes) =>
	store.deleteItem({ routingHash: RH, hk, sk: typeof sk === "string" ? kb(sk) : sk, txOrderTs: 2 });

/** Imports `count` items of `hk` above every item that the store holds, as one migration page. */
let importId = 1_000_000;
function importPage(
	store: PartitionStore,
	hk: KeyBytes,
	prefix: string,
	count: number,
	ttlAt = (i: number): number | null => (i % 3 === 0 ? 2_000_000_000 : null),
): void {
	const rows = Array.from({ length: count }, (_, i) => ({
		item_id: importId++ as ItemLinkId,
		routingHash: RH,
		hk,
		sk: kb(`${prefix}${String(i).padStart(4, "0")}`),
		data: "x".repeat(8),
		kind: "text" as const,
		ttl_epoch_utc_seconds: ttlAt(i),
		v: 1,
		last_read_ts: 1,
		last_write_ts: 1,
	}));
	store.itemStats.addRows(
		rows.flatMap((row) => {
			const facts = store.insertItemIfAbsent(row);
			return facts ? [{ routingHash: RH, hk: row.hk, sk: row.sk, facts }] : [];
		}),
	);
}

describe("ItemStats on a range leaf", () => {
	it("splits a bucket above 2K items at its middle item, and the new bound is the key of that item", async () => {
		await withRangeLeaf(({ state, storeWith }) => {
			const store = storeWith(2);
			for (let i = 0; i < 5; i++) {
				put(store, HK_A, `s${i}`, 10);
			}
			const [lower, upper] = expectBuckets(state, 2);
			expect([lower.facts.itemCount, upper.facts.itemCount]).toEqual([2, 3]);
			expect(upper.bound).toEqual({ hk: HK_A, sk: kb("s2") });
		});
	});

	it("keeps the buckets exact and inside the band through an append-only sequence and a delete of most items", async () => {
		await withRangeLeaf(({ state, storeWith }) => {
			const K = 3;
			const store = storeWith(K);
			for (let i = 0; i < 60; i++) {
				put(store, HK_A, `s${String(i).padStart(3, "0")}`, 5 + (i % 7), i % 4 === 0 ? 2_000_000_000 : null);
				expectBuckets(state, K);
			}
			for (let i = 0; i < 58; i++) {
				del(store, HK_A, `s${String(i).padStart(3, "0")}`);
				expectBuckets(state, K);
			}
			expect(buckets(state)).toHaveLength(1);
			del(store, HK_A, "s058");
			del(store, HK_A, "s059");
			expect(buckets(state)).toEqual([]);
		});
	});

	it("builds import buckets of K items, except the last, and reads no item for them", async () => {
		await withRangeLeaf(({ state, storeWith }) => {
			const K = 4;
			const store = storeWith(K);
			const queries: string[] = [];
			const recording = {
				sql: {
					exec: (query: string, ...bindings: unknown[]) => {
						queries.push(query);
						return state.storage.sql.exec(query, ...bindings);
					},
				},
			} as unknown as DurableObjectStorage;
			const stats = new ItemStats(recording, {
				table: "items",
				hkColumn: "hk",
				skColumn: "sk",
				indexedCondition: "ttl_epoch_utc_seconds IS NOT NULL",
				model: ITEMS_MODEL,
				promotionFloor: () => null,
				rangeLeaf: () => true,
				bucketItems: K,
				logName: "fokos/partition",
				logParams: () => ({}),
			});
			for (const [page, count] of [4, 3, 2, 6].entries()) {
				const rows = Array.from({ length: count }, (_, i) => ({
					item_id: importId++ as ItemLinkId,
					routingHash: RH,
					// Each page is above the items before it, as the pages of a migration are. The second page
					// starts on a full bucket.
					hk: page === 3 ? HK_HIGH : HK_LOW,
					sk: kb(`p${page}-${i}`),
					data: "x",
					kind: "text" as const,
					ttl_epoch_utc_seconds: null,
					v: 1,
					last_read_ts: 1,
					last_write_ts: 1,
				}));
				stats.addRows(
					rows.flatMap((row) => {
						const facts = store.insertItemIfAbsent(row);
						return facts ? [{ routingHash: RH, hk: row.hk, sk: row.sk, facts }] : [];
					}),
				);
				// A retried page inserts no row, so it changes no bucket.
				const before = buckets(state);
				expect(rows.map((row) => store.insertItemIfAbsent(row))).toEqual(rows.map(() => undefined));
				stats.addRows([]);
				expect(buckets(state)).toEqual(before);
			}
			const rows = expectBuckets(state, K);
			expect(rows.map((r) => r.facts.itemCount)).toEqual([4, 4, 4, 3]);
			expect(queries.filter((q) => /FROM items\b/.test(q))).toEqual([]);
		});
	});

	it("subtracts an expiry chunk from each of its buckets, and merges the neighbours of a bucket that it empties", async () => {
		await withRangeLeaf(({ state, storeWith }) => {
			const K = 4;
			const store = storeWith(K);
			// Three import buckets of 4 items. The chunk expires the middle bucket and all but one item of
			// each neighbour, so the neighbours then hold fewer than 4 items together.
			importPage(store, HK_A, "a", 12, (i) => (i >= 1 && i <= 10 ? 10 + (i % 3) : null));
			expect(expectBuckets(state, K).map((r) => r.facts.itemCount)).toEqual([4, 4, 4]);
			expect(store.deleteExpiredItems(100, 100, () => true)).toMatchObject({ deletedRows: 10 });
			expect(expectBuckets(state, K).map((r) => r.facts.itemCount)).toEqual([2]);
		});
	});

	it("follows a decrease of K: deletes leave ten buckets of half the new K, inside the new row bound and the old occupancy bound", async () => {
		await withRangeLeaf(({ state, storeWith }) => {
			const [oldK, newK] = [8, 4];
			importPage(storeWith(oldK), HK_A, "a", 10 * oldK);
			expect(expectBuckets(state, oldK)).toHaveLength(10);
			const store = storeWith(newK);
			for (let b = 0; b < 10; b++) {
				for (let i = 2; i < oldK; i++) {
					del(store, HK_A, `a${String(b * oldK + i).padStart(4, "0")}`);
				}
			}
			const rows = expectBuckets(state, newK, 2 * oldK);
			expect(rows.map((r) => r.facts.itemCount)).toEqual(Array.from({ length: 10 }, () => 2));
			const items = 20;
			expect(rows.length).toBeLessThanOrEqual((2 * items) / newK + 1);
		});
	});

	it("repairs a bucket whose facts disagree with its items from its key range, in the same write", async () => {
		expectedDefects = 1;
		await withRangeLeaf(({ state, storeWith }) => {
			const K = 2;
			const store = storeWith(K);
			for (let i = 0; i < 8; i++) {
				put(store, HK_A, `s${i}`, 10);
			}
			const [, second] = expectBuckets(state, K);
			state.storage.sql.exec(
				`UPDATE item_stats SET item_count = 0, item_bytes = 0 WHERE routing_hash = ? AND lower_hk = ? AND lower_sk = ?`,
				RH,
				second.bound.hk,
				second.bound.sk,
			);
			del(store, second.bound.hk, second.bound.sk);
			expectBuckets(state, K);
		});
	});

	it("counts all items again into the first bucket when a present item has no bucket, and keeps no other bucket", async () => {
		expectedDefects = 1;
		await withRangeLeaf(({ state, storeWith }) => {
			const K = 2;
			const store = storeWith(K);
			for (let i = 0; i < 8; i++) {
				put(store, HK_A, `s${i}`, 10);
			}
			// The first bucket is lost, and the buckets above it stay.
			state.storage.sql.exec(`DELETE FROM item_stats WHERE routing_hash = ? AND lower_hk = x'' AND lower_sk = x''`, RH);
			expect(buckets(state).length).toBeGreaterThan(0);
			// An overwrite of an item below each remaining bound finds no bucket.
			put(store, HK_A, "s0", 20);
			expect(buckets(state)).toHaveLength(1);
			expectBuckets(state, K, 8);
		});
	});

	it("writes 1 bucket row for each write of a range leaf, also for the first bucket", async () => {
		await withRangeLeaf(({ state }) => {
			const cursors: SqlStorageCursor<Record<string, SqlStorageValue>>[] = [];
			const recording = {
				sql: {
					exec: (query: string, ...bindings: unknown[]) => {
						const cursor = state.storage.sql.exec(query, ...bindings);
						cursors.push(cursor);
						return cursor;
					},
				},
			} as unknown as DurableObjectStorage;
			const stats = new ItemStats(recording, {
				table: "items",
				hkColumn: "hk",
				skColumn: "sk",
				indexedCondition: "ttl_epoch_utc_seconds IS NOT NULL",
				model: ITEMS_MODEL,
				promotionFloor: () => null,
				rangeLeaf: () => true,
				bucketItems: 2048,
				logName: "fokos/partition",
				logParams: () => ({}),
			});
			const one = (itemBytes: number) => itemFacts(ITEMS_MODEL, { itemBytes, keyBytes: 4, indexed: false });
			const written = () => {
				const sum = cursors.reduce((n, c) => n + c.rowsWritten, 0);
				cursors.length = 0;
				return sum;
			};
			expect(stats.change(RH, HK_A, kb("a"), null, one(10))).toBeNull();
			expect(written()).toBe(1);
			stats.change(RH, HK_A, kb("b"), null, one(10));
			expect(written()).toBe(1);
			stats.change(RH, HK_A, kb("a"), one(10), one(20));
			expect(written()).toBe(1);
			expect(buckets(state).map((r) => r.promotionEst)).toEqual([null]);
		});
	});

	it("uses a primary-key search for the bucket seek, the split, the recount, and the planner reads", async () => {
		await withRangeLeaf(({ state }) => {
			const plan = (query: string, ...params: unknown[]) =>
				state.storage.sql
					.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`, ...params)
					.toArray()
					.map((r) => r.detail);
			const k = [RH, kb("a"), kb("b")];
			for (const detail of [
				...plan(
					`UPDATE item_stats SET item_count = item_count + 1 WHERE routing_hash = ?1 AND (lower_hk, lower_sk) = (
					   SELECT lower_hk, lower_sk FROM item_stats WHERE routing_hash = ?1 AND (lower_hk, lower_sk) <= (?2, ?3)
					   ORDER BY lower_hk DESC, lower_sk DESC LIMIT 1)`,
					...k,
				),
				...plan(
					`SELECT hk, sk FROM items WHERE routing_hash = ?1 AND (hk, sk) >= (?2, ?3) AND (hk, sk) < (?2, ?3) ORDER BY hk, sk LIMIT 1 OFFSET 4`,
					...k,
				),
				...plan(
					`SELECT COUNT(*), SUM(item_bytes) FROM items WHERE routing_hash = ?1 AND (hk, sk) >= (?2, ?3) AND (hk, sk) < (?2, ?3)`,
					...k,
				),
				...plan(`SELECT hk, sk, item_bytes FROM items WHERE routing_hash = ?1 AND (hk, sk) >= (?2, ?3) ORDER BY hk DESC, sk DESC`, ...k),
				...plan(
					`SELECT lower_hk, lower_sk FROM item_stats WHERE routing_hash = ?1 AND (lower_hk, lower_sk) > (?2, ?3) ORDER BY lower_hk, lower_sk LIMIT 1`,
					...k,
				),
			]) {
				expect(detail).toMatch(/^(SEARCH (items|item_stats) USING PRIMARY KEY|SCALAR SUBQUERY)/);
			}
		});
	});

	it("keeps exact buckets inside the band through random imports, writes, deletes, and TTL expiries, and the planner paths agree", async () => {
		const K = 3;
		const sks = Array.from({ length: 24 }, (_, i) => `s${String(i).padStart(2, "0")}`);
		const arbOp = fc.oneof(
			fc.record({
				op: fc.constant("put" as const),
				b: fc.boolean(),
				sk: fc.constantFrom(...sks),
				size: fc.oneof(fc.nat(40), fc.integer({ min: 900, max: 6000 })),
				ttl: fc.option(fc.integer({ min: 1, max: 100 }), { nil: null }),
			}),
			fc.record({ op: fc.constant("delete" as const), b: fc.boolean(), sk: fc.constantFrom(...sks) }),
			fc.record({ op: fc.constant("expire" as const), now: fc.integer({ min: 1, max: 100 }), limit: fc.integer({ min: 1, max: 20 }) }),
		);
		await withRangeLeaf(async ({ state, storeWith }) => {
			const store = storeWith(K);
			await fc.assert(
				fc.asyncProperty(
					fc.nat(12),
					fc.array(arbOp, { minLength: 1, maxLength: 40 }),
					fc.integer({ min: 2, max: 5 }),
					async (imported, ops, N) => {
						state.storage.sql.exec(`DELETE FROM items`);
						state.storage.sql.exec(`DELETE FROM item_stats`);
						// An import page arrives first, into an empty leaf.
						importPage(store, HK_A, "i", imported);
						expectBuckets(state, K);
						for (const op of ops) {
							if (op.op === "put") {
								put(store, op.b ? HK_B : HK_A, op.sk, op.size, op.ttl);
							} else if (op.op === "delete") {
								del(store, op.b ? HK_B : HK_A, op.sk);
							} else {
								store.deleteExpiredItems(op.now, op.limit, () => true);
							}
							expectBuckets(state, K);
						}
						// The bucket path gives the boundaries of the streaming path over the same items.
						if (store.countItems(RH, null, null, N) >= N) {
							const planner = new SplitPlanner({
								itemStats: store.itemStats,
								source: {
									probeRoutingHashes: () => null,
									compositeKeyFacts: (rh, from, to, order) => store.compositeKeyFacts(rh, from, to, order),
									countItems: (rh, from, to, limit) => store.countItems(rh, from, to, limit),
								},
								databaseSize: () => 0,
								promotionFraction: () => 0.5,
								rangeSplitN: () => N,
								logName: "test",
								logParams: () => ({}),
							});
							const total = estimate(ITEMS_MODEL, store.itemStats.factsOf(RH));
							expect(planner.rangeBoundaries(RH, null, null, N)).toEqual(
								planRangeBoundaries(ITEMS_MODEL, total, N, null, store.compositeKeyFacts(RH, null, null)),
							);
						}
					},
				),
				{ numRuns: propertyRuns(40) },
			);
		});
	});
});
