import { runInDurableObject } from "cloudflare:test";
import fc from "fast-check";
import { afterEach, beforeEach, describe, expect, it, vi, type MockInstance } from "vitest";
import type { FokosDBPartitionDO } from "../../server/do-partition.js";
import { testPartitionStub } from "../../../test/stub-helpers.js";
import { collidingHashKeys } from "../../../test/hash-digits.js";
import { propertyRuns } from "../../../test/property-based/harness.js";
import { updateTree } from "../expression/test-fixtures.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import { routingHashOf, type RoutingHash } from "../../sharding/hash-primitives.js";
import type { FokosPartitionIdentity } from "../../sharding/route-context.js";
import type { FokosDBPolicy } from "../partition-context.js";
import { MAX_ITEM_BYTES } from "../transaction-limits.js";
import {
	estimate,
	BUCKET_THRESHOLD_MAX,
	bucketThreshold,
	ITEMS_MODEL,
	itemFacts,
	LATEST_ITEMS_MODEL,
	maxItemEstimate,
	NO_FACTS,
	overflowBytes,
	overflowBytesSql,
	validateStorageModel,
	type ItemFacts,
	type StorageModel,
} from "./item-size.js";
import { addFacts, ItemStats } from "./item-stats.js";
import { PartitionStore, type ItemLinkId, type KeyRange } from "./partition-store.js";
import { SplitPlanner } from "./split-planner.js";

const GROUP = "fokos.p.item-stats-test";
const P = (hk: KeyBytes) => routingHashOf(hk, GROUP);
const kb = (v: string | Uint8Array) => KeyCodec.encode(v);
const NO_SK = KeyCodec.encodeOptional(undefined);
const ALL: KeyRange = { start: { routingHash: new Uint8Array() }, end: null };
const DEFECT = "fokos/partition: the facts of a bucket disagree with its items, so they are counted again";
// A promotion floor with the bucket threshold m = 1: each routingHash with an item has a bucket. A
// bucket with an estimate below about 1.4 MB stores no estimate.
const ONE_ITEM_FLOOR = maxItemEstimate(ITEMS_MODEL);

// Each test of this file fails when the defect log appears, except the tests that break a bucket on
// purpose and set `expectedDefects`.
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

type Ctx = { store: PartitionStore; state: DurableObjectState; setFloor: (floor: number | null) => void };

async function withStore(floor: number | null, fn: (ctx: Ctx) => void | Promise<void>): Promise<void> {
	const stub = testPartitionStub(`item-stats-test.${crypto.randomUUID()}`);
	await runInDurableObject(stub, async (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
		let current = floor;
		const store = new PartitionStore(state.storage, { promotionFloor: () => current });
		await fn({ store, state, setFloor: (f) => (current = f) });
	});
}

type BucketRow = ItemFacts & { lowerHk: Uint8Array; lowerSk: Uint8Array; promotionEst: number | null; promotionType: string };

/** Every bucket of `routingHash`, as the table stores it. */
function buckets(state: DurableObjectState, routingHash: RoutingHash): BucketRow[] {
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
			t: string;
		}>(`SELECT *, typeof(promotion_est_bytes) AS t FROM item_stats WHERE routing_hash = ? ORDER BY lower_hk, lower_sk`, routingHash)
		.toArray()
		.map((r) => ({
			lowerHk: new Uint8Array(r.lower_hk),
			lowerSk: new Uint8Array(r.lower_sk),
			itemCount: r.item_count,
			itemBytes: r.item_bytes,
			keyBytes: r.key_bytes,
			indexedItemCount: r.indexed_item_count,
			indexedKeyBytes: r.indexed_key_bytes,
			overflowBytes: r.overflow_bytes,
			promotionEst: r.promotion_est_bytes,
			promotionType: r.t,
		}));
}

/** The sums over the items of `routingHash`, from SQL over the item rows. */
function sqlSums(state: DurableObjectState, routingHash: RoutingHash): ItemFacts {
	const r = state.storage.sql
		.exec<{ c: number; b: number; k: number; ic: number; ik: number; o: number }>(
			`SELECT COUNT(*) AS c, COALESCE(SUM(item_bytes), 0) AS b, COALESCE(SUM(octet_length(hk) + octet_length(sk)), 0) AS k,
			        COALESCE(SUM(ttl_epoch_utc_seconds IS NOT NULL), 0) AS ic,
			        COALESCE(SUM(CASE WHEN ttl_epoch_utc_seconds IS NOT NULL THEN octet_length(hk) + octet_length(sk) ELSE 0 END), 0) AS ik,
			        COALESCE(SUM(${overflowBytesSql(ITEMS_MODEL, "item_bytes")}), 0) AS o
			   FROM items WHERE routing_hash = ?`,
			routingHash,
		)
		.one();
	return { itemCount: r.c, itemBytes: r.b, keyBytes: r.k, indexedItemCount: r.ic, indexedKeyBytes: r.ik, overflowBytes: r.o };
}

const rounded = (facts: ItemFacts) => Math.ceil(estimate(ITEMS_MODEL, facts));

/**
 * Checks invariants I1, I3 and I4 for `routingHash`: one first bucket while it has items, facts equal
 * to the SQL sums, and `promotion_est_bytes` from the facts and `floor`.
 */
function expectExact(state: DurableObjectState, routingHash: RoutingHash, floor: number | null): void {
	const sums = sqlSums(state, routingHash);
	const rows = buckets(state, routingHash);
	if (sums.itemCount === 0) {
		expect(rows).toEqual([]);
		return;
	}
	expect(rows).toHaveLength(1);
	const [{ lowerHk, lowerSk, promotionEst, promotionType, ...facts }] = rows;
	expect([lowerHk.byteLength, lowerSk.byteLength]).toEqual([0, 0]);
	expect(facts).toEqual(sums);
	const est = rounded(sums);
	expect(promotionEst).toBe(floor !== null && est >= floor ? est : null);
	expect(promotionType).toBe(promotionEst === null ? "null" : "integer");
}

const put = (
	store: PartitionStore,
	hk: KeyBytes,
	sk: KeyBytes,
	data: string | Uint8Array,
	opts: { kind?: "text" | "bytes" | "json"; ttlAt?: number | null } = {},
) =>
	store.upsertItem({
		routingHash: P(hk),
		hk,
		sk,
		data,
		kind: opts.kind ?? (typeof data === "string" ? "text" : "bytes"),
		ttlAt: opts.ttlAt ?? null,
		txOrderTs: 1,
	});
const del = (store: PartitionStore, hk: KeyBytes, sk: KeyBytes) => store.deleteItem({ routingHash: P(hk), hk, sk, txOrderTs: 2 });

let nextItemId = 1_000_000;
/** Inserts an item row with no fact change, as the item write does before it calls `ItemStats.change`. */
function insertRow(state: DurableObjectState, hk: KeyBytes, itemBytes: number, opts: { sk?: KeyBytes; ttl?: number | null } = {}): void {
	state.storage.sql.exec(
		`INSERT INTO items (routing_hash, hk, sk, item_id, v, ttl_epoch_utc_seconds, item_bytes, data) VALUES (?, ?, ?, ?, 1, ?, ?, x'')`,
		P(hk),
		hk,
		opts.sk ?? NO_SK,
		nextItemId++,
		opts.ttl ?? null,
		itemBytes,
	);
}

describe("item-size", () => {
	it("throws for a coefficient that is not a multiple of 1/4, or is negative", () => {
		expect(() => validateStorageModel({ ...ITEMS_MODEL, keyIndexFactor: 1.3 })).toThrow(/keyIndexFactor .* multiple of 1\/4/);
		expect(() => validateStorageModel({ ...ITEMS_MODEL, baseOverhead: -1 })).toThrow(/baseOverhead/);
		expect(() => validateStorageModel({ ...ITEMS_MODEL, rowPayloadOverhead: 1.5 })).toThrow(/rowPayloadOverhead/);
		expect(() => validateStorageModel({ ...ITEMS_MODEL, keyIndexFactor: 1.75 })).not.toThrow();
	});

	it("gives overflow bytes from the payload size, the same in JavaScript and in SQL", async () => {
		const rpo = ITEMS_MODEL.rowPayloadOverhead;
		const oro = ITEMS_MODEL.overflowRowOverhead;
		// A payload of 1,002 bytes stays on the tree page.
		expect(overflowBytes(ITEMS_MODEL, 1002 - rpo)).toBe(0);
		// 1,003 bytes: 489 bytes stay, 514 go to one overflow page of 4,096 bytes.
		expect(overflowBytes(ITEMS_MODEL, 1003 - rpo)).toBe(4096 - 514 + oro);
		// 5,094 bytes: 1,002 bytes stay, and 4,092 bytes fill one overflow page.
		expect(overflowBytes(ITEMS_MODEL, 5094 - rpo)).toBe(4 + oro);
		// 5,095 bytes: 489 bytes stay, and 4,606 bytes need two overflow pages.
		expect(overflowBytes(ITEMS_MODEL, 5095 - rpo)).toBe(2 * 4096 - 4606 + oro);

		const sizes = [0, 1, 1002 - rpo, 1003 - rpo, 5094 - rpo, 5095 - rpo, MAX_ITEM_BYTES];
		for (let n = 0; n < 30_000; n += 37) {
			sizes.push(n);
		}
		await withStore(null, ({ state }) => {
			for (const model of [ITEMS_MODEL, { ...ITEMS_MODEL, rowPayloadOverhead: 29, overflowRowOverhead: 69 }]) {
				for (const size of sizes) {
					const sql = state.storage.sql.exec<{ o: number }>(`SELECT ${overflowBytesSql(model, "?1")} AS o`, size).one().o;
					expect(sql, `item_bytes ${size}`).toBe(overflowBytes(model, size));
				}
			}
		});
	});

	it("gives a group the sum of the estimates of its items", () => {
		const items = [
			{ itemBytes: 10, keyBytes: 5, indexed: false },
			{ itemBytes: 3000, keyBytes: 401, indexed: true },
			{ itemBytes: MAX_ITEM_BYTES, keyBytes: 9, indexed: false },
		];
		const facts = items.map((item) => itemFacts(ITEMS_MODEL, item));
		const group = facts.reduce(addFacts, { ...NO_FACTS });
		expect(estimate(ITEMS_MODEL, group)).toBe(facts.reduce((sum, f) => sum + estimate(ITEMS_MODEL, f), 0));
	});
});

describe("ItemStats on a hash leaf", () => {
	it("keeps the exact facts through inserts, overwrites, compiled updates, TTL changes, and deletes", async () => {
		await withStore(1, ({ store, state }) => {
			const hk = kb("h");
			const rh = P(hk);
			const check = () => expectExact(state, rh, 1);
			put(store, hk, kb("multibyte"), "héllo—✓");
			check();
			put(store, hk, kb(new Uint8Array([0, 1, 2])), new Uint8Array([1, 2, 3, 4]));
			check();
			put(store, hk, NO_SK, JSON.stringify({ a: [1, 2, 3], b: "text" }), { kind: "json" });
			check();
			expect(buckets(state, rh)[0].itemCount).toBe(3);
			// An overwrite keeps the count, and an equal size changes nothing.
			put(store, hk, kb("multibyte"), "a longer value than before");
			check();
			put(store, hk, kb("multibyte"), "a longer value than before");
			check();
			// TTL changes: null to a value, a value to another value, a value to null.
			put(store, hk, kb("multibyte"), "x", { ttlAt: 2_000_000_000 });
			check();
			expect(buckets(state, rh)[0].indexedItemCount).toBe(1);
			put(store, hk, kb("multibyte"), "x", { ttlAt: 2_000_000_001 });
			check();
			put(store, hk, kb("multibyte"), "x", { ttlAt: null });
			check();
			expect(buckets(state, rh)[0].indexedItemCount).toBe(0);
			// A compiled update that creates an item, and one that changes it and keeps its TTL.
			const plan = updateTree([{ action: "set", target: { ref: "data", path: "$.n" }, value: { val: "a long value" } }]);
			store.updateItemSingleShot({ routingHash: rh, hk, sk: kb("created"), plan, txOrderTs: 3 });
			check();
			store.updateItemSingleShot({ routingHash: rh, hk, sk: kb("created"), plan, ttlAt: 2_000_000_000, txOrderTs: 4 });
			check();
			store.updateItemSingleShot({ routingHash: rh, hk, sk: kb("created"), plan, txOrderTs: 5 });
			check();
			expect(buckets(state, rh)[0].indexedItemCount).toBe(1);
			// Deletes, and deletes of absent items in both forms.
			expect(del(store, hk, kb("multibyte")).deleted).toBe(true);
			check();
			del(store, hk, kb("absent"));
			store.deleteItem({ routingHash: rh, hk, sk: kb("absent"), txOrderTs: 6, bumpTxOrderTsAlways: true });
			check();
			for (const sk of [kb(new Uint8Array([0, 1, 2])), NO_SK, kb("created")]) {
				del(store, hk, sk);
				check();
			}
			expect(buckets(state, rh)).toEqual([]);
		});
	});

	it("counts the overflow pages of large items, and returns to no bucket when the items go", async () => {
		await withStore(ONE_ITEM_FLOOR, ({ store, state }) => {
			const hk = kb("big");
			const rh = P(hk);
			const keyBytes = hk.byteLength + kb("s0").byteLength;
			const rpo = ITEMS_MODEL.rowPayloadOverhead;
			const dataFor = (itemBytes: number) => "x".repeat(itemBytes - keyBytes);
			const sizes = [1002 - rpo, 1003 - rpo, 5094 - rpo, 5095 - rpo, MAX_ITEM_BYTES];
			for (const [i, size] of sizes.entries()) {
				put(store, hk, kb(`s${i}`), dataFor(size));
				expectExact(state, rh, ONE_ITEM_FLOOR);
			}
			expect(buckets(state, rh)[0].overflowBytes).toBe(sizes.reduce((sum, size) => sum + overflowBytes(ITEMS_MODEL, size), 0));
			// Updates across the overflow limit, in both directions.
			put(store, hk, kb("s0"), dataFor(1003 - rpo));
			expectExact(state, rh, ONE_ITEM_FLOOR);
			put(store, hk, kb("s1"), dataFor(1002 - rpo));
			expectExact(state, rh, ONE_ITEM_FLOOR);
			for (let i = 0; i < sizes.length; i++) {
				del(store, hk, kb(`s${i}`));
				expectExact(state, rh, ONE_ITEM_FLOOR);
			}
			expect(buckets(state, rh)).toEqual([]);
		});
	});

	it("subtracts the facts of expired items, with one bucket for each routingHash", async () => {
		await withStore(ONE_ITEM_FLOOR, ({ store, state }) => {
			const [a, b] = [kb("a"), kb("b")];
			put(store, a, kb("1"), "x", { ttlAt: 10 });
			put(store, a, kb("2"), "yy", { ttlAt: 20 });
			put(store, a, kb("3"), "zzz", { ttlAt: 1_000 });
			put(store, a, kb("4"), "w");
			put(store, b, kb("1"), "x", { ttlAt: 15 });
			expect(store.deleteExpiredItems(100, 10, () => true)).toMatchObject({ deletedRows: 3 });
			expectExact(state, P(a), ONE_ITEM_FLOOR);
			expectExact(state, P(b), ONE_ITEM_FLOOR);
			expect(buckets(state, P(a))[0]).toMatchObject({ itemCount: 2, indexedItemCount: 1 });
			expect(buckets(state, P(b))).toEqual([]);
		});
	});

	it("shares one bucket between two hash keys of one routingHash", async () => {
		await withStore(ONE_ITEM_FLOOR, ({ store, state }) => {
			const [a, b] = collidingHashKeys(GROUP).map((raw) => KeyCodec.encode(raw));
			expect(P(b)).toEqual(P(a));
			put(store, a, kb("s"), "a-data");
			put(store, b, kb("s"), "b-data");
			put(store, b, kb("t"), "b-data");
			expectExact(state, P(a), ONE_ITEM_FLOOR);
			expect(buckets(state, P(a))[0].itemCount).toBe(3);
			del(store, a, kb("s"));
			expectExact(state, P(a), ONE_ITEM_FLOOR);
		});
	});

	it("adds the facts of an import page one time, and a retried page adds nothing", async () => {
		await withStore(ONE_ITEM_FLOOR, async ({ store: source }) => {
			const hks = [kb("p"), kb("q")];
			for (const hk of hks) {
				for (const sk of ["1", "2", "3"]) {
					put(source, hk, kb(sk), `data-${sk}`, { ttlAt: sk === "2" ? 2_000_000_000 : null });
				}
			}
			const page = [...source.queryItemsPage(null, 100, ALL)];
			await withStore(ONE_ITEM_FLOOR, ({ store, state }) => {
				const apply = () =>
					store.itemStats.addRows(
						page.flatMap((item) => {
							const facts = store.insertItemIfAbsent(item);
							return facts ? [{ routingHash: item.routingHash, hk: item.hk, sk: item.sk, facts }] : [];
						}),
					);
				apply();
				apply();
				for (const hk of hks) {
					expectExact(state, P(hk), ONE_ITEM_FLOOR);
					expect(buckets(state, P(hk))[0]).toMatchObject({ itemCount: 3, indexedItemCount: 1 });
				}
			});
		});
	});

	it("after a promotion removes the buckets, a delete of a copy creates no bucket and changes no other bucket", async () => {
		await withStore(ONE_ITEM_FLOOR, ({ store, state }) => {
			const [moved, stays] = [kb("moved"), kb("stays")];
			put(store, moved, kb("1"), "x", { ttlAt: 10 });
			put(store, moved, kb("2"), "x");
			put(store, stays, kb("1"), "x");
			const before = buckets(state, P(stays));
			store.itemStats.remove(P(moved));
			del(store, moved, kb("2"));
			store.deleteExpiredItems(100, 10, () => true);
			store.deleteItemsBatchForRoutingHash(P(moved), 10);
			expect(buckets(state, P(moved))).toEqual([]);
			expect(buckets(state, P(stays))).toEqual(before);
		});
	});

	it("repairs a bucket whose facts disagree with its items, in the same write, and logs the defect one time", async () => {
		expectedDefects = 1;
		await withStore(ONE_ITEM_FLOOR, ({ store, state }) => {
			const hk = kb("drift");
			const rh = P(hk);
			put(store, hk, kb("1"), "aaaa");
			put(store, hk, kb("2"), "bbbb");
			// The bucket says one item: the delete of both would make the count negative.
			state.storage.sql.exec(`UPDATE item_stats SET item_count = 1, item_bytes = 1 WHERE routing_hash = ?`, rh);
			del(store, hk, kb("1"));
			expectExact(state, rh, ONE_ITEM_FLOOR);
			expect(errors.mock.calls.filter(([e]) => (e as { message?: string }).message === DEFECT)).toEqual([
				[expect.objectContaining({ routingHash: rh.toHex(), stored: expect.objectContaining({ itemCount: 1, itemBytes: 1 }) })],
			]);
			// A routingHash with no bucket: an overwrite adds no item, so it changes no facts.
			store.itemStats.remove(rh);
			put(store, hk, kb("2"), "a longer value");
			expect(buckets(state, rh)).toEqual([]);
		});
	});

	it("keeps exact facts through random writes, deletes, TTL expiries, and import pages", async () => {
		const [ca, cb] = collidingHashKeys(GROUP).map((raw) => KeyCodec.encode(raw));
		const hks = [kb("a"), kb("b"), ca, cb];
		const sks = [NO_SK, kb("1"), kb("2"), kb(new Uint8Array([7]))];
		const arbOp = fc.oneof(
			fc.record({
				op: fc.constant("put" as const),
				hk: fc.nat(hks.length - 1),
				sk: fc.nat(sks.length - 1),
				size: fc.oneof(fc.nat(50), fc.integer({ min: 900, max: 6000 })),
				ttl: fc.option(fc.integer({ min: 1, max: 100 }), { nil: null }),
			}),
			fc.record({ op: fc.constant("delete" as const), hk: fc.nat(hks.length - 1), sk: fc.nat(sks.length - 1) }),
			fc.record({ op: fc.constant("expire" as const), now: fc.integer({ min: 1, max: 100 }) }),
			fc.record({ op: fc.constant("import" as const), hk: fc.nat(hks.length - 1), count: fc.integer({ min: 1, max: 3 }) }),
		);
		await withStore(200, async ({ store, state }) => {
			let importId = 1_000_000;
			await fc.assert(
				fc.asyncProperty(fc.array(arbOp, { minLength: 1, maxLength: 25 }), async (ops) => {
					state.storage.sql.exec(`DELETE FROM items`);
					state.storage.sql.exec(`DELETE FROM item_stats`);
					for (const op of ops) {
						if (op.op === "put") {
							put(store, hks[op.hk], sks[op.sk], "x".repeat(op.size), { ttlAt: op.ttl });
						} else if (op.op === "delete") {
							del(store, hks[op.hk], sks[op.sk]);
						} else if (op.op === "expire") {
							store.deleteExpiredItems(op.now, 100, () => true);
						} else {
							const hk = hks[op.hk];
							const rows = Array.from({ length: op.count }, (_, i) => ({
								item_id: importId++ as ItemLinkId,
								routingHash: P(hk),
								hk,
								sk: kb(`import-${importId}-${i}`),
								data: "imported",
								kind: "text" as const,
								ttl_epoch_utc_seconds: null,
								v: 1,
								last_read_ts: 1,
								last_write_ts: 1,
							}));
							store.itemStats.addRows(
								rows.flatMap((row) => {
									const facts = store.insertItemIfAbsent(row);
									return facts ? [{ routingHash: row.routingHash, hk: row.hk, sk: row.sk, facts }] : [];
								}),
							);
						}
						for (const hk of hks) {
							expectExact(state, P(hk), 200);
						}
					}
				}),
				{ numRuns: propertyRuns(30) },
			);
		});
	});
});

describe("ItemStats statements", () => {
	/** An `ItemStats` over the storage of `state` that records the cursor of each statement. */
	function recorded(state: DurableObjectState, floor: number | null, model: StorageModel = ITEMS_MODEL) {
		const cursors: Array<{ query: string; bindings: unknown[]; cursor: SqlStorageCursor<Record<string, SqlStorageValue>> }> = [];
		const storage = {
			sql: {
				exec: (query: string, ...bindings: unknown[]) => {
					const cursor = state.storage.sql.exec(query, ...bindings);
					cursors.push({ query, bindings, cursor });
					return cursor;
				},
			},
		} as unknown as DurableObjectStorage;
		const stats = new ItemStats(storage, {
			table: "items",
			hkColumn: "hk",
			skColumn: "sk",
			indexedCondition: "ttl_epoch_utc_seconds IS NOT NULL",
			model,
			promotionFloor: () => floor,
			rangeLeaf: () => false,
			bucketItems: 2048,
			logName: "fokos/partition",
			logParams: () => ({}),
		});
		const take = () => {
			const metrics = cursors.map(({ query, bindings, cursor }) => ({
				query,
				bindings,
				rowsRead: cursor.rowsRead,
				rowsWritten: cursor.rowsWritten,
			}));
			cursors.length = 0;
			return metrics;
		};
		return { stats, take };
	}
	const one = (itemBytes: number) => itemFacts(ITEMS_MODEL, { itemBytes, keyBytes: 4, indexed: false });

	it("writes 1 row below the promotion floor, 2 at or above it, and none for a routingHash with no bucket", async () => {
		await withStore(ONE_ITEM_FLOOR, ({ state }) => {
			// The first insert of each routingHash creates its bucket. The test then measures an insert and
			// an update of a routingHash with a bucket.
			const below = recorded(state, 1_000_000);
			insertRow(state, kb("below"), 100);
			below.stats.change(P(kb("below")), kb("below"), NO_SK, null, one(100));
			below.take();
			below.stats.change(P(kb("below")), kb("below"), NO_SK, null, one(100));
			below.stats.change(P(kb("below")), kb("below"), NO_SK, one(100), one(200));
			expect(below.take().map((m) => m.rowsWritten)).toEqual([1, 1]);

			const above = recorded(state, 10);
			insertRow(state, kb("above"), 100);
			above.stats.change(P(kb("above")), kb("above"), NO_SK, null, one(100));
			above.take();
			above.stats.change(P(kb("above")), kb("above"), NO_SK, null, one(100));
			above.stats.change(P(kb("above")), kb("above"), NO_SK, one(100), one(200));
			expect(above.take().map((m) => m.rowsWritten)).toEqual([2, 2]);

			// A value that falls below the floor removes its index entry only.
			const falls = recorded(state, 400);
			falls.stats.change(P(kb("above")), kb("above"), NO_SK, one(200), one(100));
			expect(falls.take().map((m) => m.rowsWritten)).toEqual([1]);

			// With no floor, m is 16: a routingHash of one item has no bucket, and its writes write no row.
			const none = recorded(state, null);
			insertRow(state, kb("none"), 100);
			none.stats.change(P(kb("none")), kb("none"), NO_SK, null, one(100));
			none.stats.change(P(kb("none")), kb("none"), NO_SK, one(100), one(200));
			expect(none.take().every((m) => m.rowsWritten === 0)).toBe(true);
			expect(buckets(state, P(kb("none")))).toEqual([]);
		});
	});

	it("stores the facts of two coefficient sets the same, and only the stored estimate differs", async () => {
		await withStore(ONE_ITEM_FLOOR, ({ state }) => {
			const other: StorageModel = { ...ITEMS_MODEL, baseOverhead: 152, keyIndexFactor: 2.5 };
			const a = recorded(state, 0, ITEMS_MODEL);
			const b = recorded(state, 0, other);
			const [ha, hb] = [kb("a".repeat(21)), kb("b".repeat(21))];
			const facts = itemFacts(ITEMS_MODEL, { itemBytes: 300, keyBytes: ha.byteLength, indexed: true });
			insertRow(state, ha, 300, { ttl: 2_000_000_000 });
			insertRow(state, hb, 300, { ttl: 2_000_000_000 });
			a.stats.change(P(ha), ha, NO_SK, null, facts);
			b.stats.change(P(hb), hb, NO_SK, null, facts);
			const [ra, rb] = [buckets(state, P(ha))[0], buckets(state, P(hb))[0]];
			const strip = ({ promotionEst: _p, promotionType: _t, ...rest }: BucketRow) => rest;
			expect(strip(ra)).toEqual(strip(rb));
			expect(ra.promotionEst).toBe(Math.ceil(estimate(ITEMS_MODEL, facts)));
			expect(rb.promotionEst).toBe(Math.ceil(estimate(other, facts)));
		});
	});

	it("binds the deltas as JavaScript numbers and stores the rounded estimate as an integer", async () => {
		await withStore(ONE_ITEM_FLOOR, ({ state }) => {
			const { stats } = recorded(state, 0);
			// 1.25 * 5 key bytes gives an estimate with a part of a byte.
			const hk = kb("fffff");
			const facts = itemFacts(ITEMS_MODEL, { itemBytes: 10, keyBytes: hk.byteLength, indexed: false });
			const exact = estimate(ITEMS_MODEL, facts);
			expect(exact % 1).toBe(0.25);
			insertRow(state, hk, 10);
			expect(stats.change(P(hk), hk, NO_SK, null, facts)).toBe(Math.ceil(exact));
			expect(stats.change(P(hk), hk, NO_SK, facts, { ...facts, itemBytes: 11 })).toBe(Math.ceil(exact + 1));
			const [row] = buckets(state, P(hk));
			expect(row).toMatchObject({ promotionEst: Math.ceil(exact + 1), promotionType: "integer" });
		});
	});

	it("writes no bucket row below m, and one row at the insert that takes a routingHash to m items", async () => {
		await withStore(ONE_ITEM_FLOOR, ({ state }) => {
			const m = 4;
			const floor = m * maxItemEstimate(ITEMS_MODEL);
			const { stats, take } = recorded(state, floor);
			const written = () => take().reduce((sum, metric) => sum + metric.rowsWritten, 0);
			const hk = kb("small");
			for (let i = 1; i <= m; i++) {
				const sk = kb(`${i}`);
				insertRow(state, hk, 100, { sk });
				stats.change(
					P(hk),
					hk,
					sk,
					null,
					itemFacts(ITEMS_MODEL, { itemBytes: 100, keyBytes: hk.byteLength + sk.byteLength, indexed: false }),
				);
				expect(written()).toBe(i < m ? 0 : 1);
			}
			expectExact(state, P(hk), floor);
			// An update and a delete of a routingHash with no bucket write no bucket row.
			const other = kb("other");
			insertRow(state, other, 100);
			stats.change(P(other), other, NO_SK, null, one(100));
			stats.change(P(other), other, NO_SK, one(100), one(200));
			stats.change(P(other), other, NO_SK, one(200), null);
			expect(written()).toBe(0);
			expect(buckets(state, P(other))).toEqual([]);
		});
	});

	it("counts and sums the items of a routingHash with no bucket with a primary-key search", async () => {
		await withStore(ONE_ITEM_FLOOR, ({ state }) => {
			const { stats, take } = recorded(state, ONE_ITEM_FLOOR);
			const hk = kb("plan");
			insertRow(state, hk, 100);
			stats.change(P(hk), hk, NO_SK, null, itemFacts(ITEMS_MODEL, { itemBytes: 100, keyBytes: hk.byteLength, indexed: false }));
			const reads = take().filter(({ query }) => query.includes("FROM (SELECT"));
			const plans = reads.map(({ query, bindings }) =>
				state.storage.sql
					.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`, ...bindings)
					.toArray()
					.map((r) => r.detail)
					.join(" | "),
			);
			// The count, then the aggregate of the facts.
			expect(plans).toEqual([
				"CO-ROUTINE (subquery-1) | SEARCH items USING PRIMARY KEY (routing_hash=?) | SCAN (subquery-1)",
				"CO-ROUTINE (subquery-1) | SEARCH items USING PRIMARY KEY (routing_hash=? AND (hk,sk)>(?,?)) | SCAN (subquery-1)",
			]);
		});
	});

	it("creates the bucket of an imported routingHash at the page that takes it to m items, and reads only the earlier items for it", async () => {
		await withStore(ONE_ITEM_FLOOR, ({ state }) => {
			const floor = BUCKET_THRESHOLD_MAX * maxItemEstimate(ITEMS_MODEL);
			const { stats, take } = recorded(state, floor);
			const hk = kb("imported");
			// The items of a page are inserted before their facts are added, as in the import.
			const page = (from: number) =>
				Array.from({ length: 10 }, (_, i) => {
					const sk = kb(`p${String(from + i).padStart(2, "0")}`);
					insertRow(state, hk, 100, { sk });
					return {
						routingHash: P(hk),
						hk,
						sk,
						facts: itemFacts(ITEMS_MODEL, { itemBytes: 100, keyBytes: hk.byteLength + sk.byteLength, indexed: false }),
					};
				});
			stats.addRows(page(0));
			expect(buckets(state, P(hk))).toEqual([]);
			take();
			stats.addRows(page(10));
			expectExact(state, P(hk), floor);
			expect(buckets(state, P(hk))[0].itemCount).toBe(20);
			// The reference: a count of the 10 items of the first page only.
			const firstPage = state.storage.sql.exec(
				`SELECT COUNT(*) AS n FROM items WHERE routing_hash = ?1 AND (hk, sk) < (?2, ?3)`,
				P(hk),
				hk,
				kb("p10"),
			);
			expect(firstPage.one().n).toBe(10);
			const aggregates = take().filter(({ query }) => query.includes("AS item_count"));
			expect(aggregates.map((metric) => metric.rowsRead)).toEqual([firstPage.rowsRead]);
		});
	});
});

describe("ItemStats bucket threshold on a hash leaf", () => {
	const MAX_EST = maxItemEstimate(ITEMS_MODEL);
	const MiB = 1024 * 1024;

	it("bounds the estimate of each item, and gives m from the promotion floor", () => {
		expect(MAX_EST).toBe(1_438_258);
		expect(maxItemEstimate(LATEST_ITEMS_MODEL)).toBe(1_438_257);
		for (const model of [ITEMS_MODEL, LATEST_ITEMS_MODEL]) {
			let largest = 0;
			for (let itemBytes = 0; itemBytes <= MAX_ITEM_BYTES; itemBytes++) {
				largest = Math.max(largest, estimate(model, itemFacts(model, { itemBytes, keyBytes: itemBytes, indexed: true })));
			}
			expect(largest).toBeLessThanOrEqual(maxItemEstimate(model));
		}
		// The floors of the caps 10, 50, 100, and 176 MiB with the default fractions.
		expect([1.25, 6.25, 12.5, 22].map((mb) => bucketThreshold(ITEMS_MODEL, mb * MiB))).toEqual([1, 4, 9, 16]);
		expect(bucketThreshold(ITEMS_MODEL, 0)).toBe(1);
		expect(bucketThreshold(ITEMS_MODEL, null)).toBe(BUCKET_THRESHOLD_MAX);
	});

	it("creates the bucket with the facts of all m items at the insert that reaches m, and keeps it after a delete below m", async () => {
		const floor = 4 * MAX_EST;
		await withStore(floor, ({ store, state }) => {
			const hk = kb("grows");
			for (let i = 0; i < 3; i++) {
				put(store, hk, kb(`${i}`), "x".repeat(10 * (i + 1)), { ttlAt: i === 1 ? 2_000_000_000 : null });
				put(store, hk, kb(`${i}`), "y".repeat(10 * (i + 1)));
				expect(buckets(state, P(hk))).toEqual([]);
			}
			put(store, hk, kb("3"), "z");
			expectExact(state, P(hk), floor);
			expect(buckets(state, P(hk))[0].itemCount).toBe(4);
			del(store, hk, kb("0"));
			expectExact(state, P(hk), floor);
			expect(buckets(state, P(hk))[0].itemCount).toBe(3);
		});
	});

	it("creates the bucket at the next insert after a policy change takes m from 16 to 4", async () => {
		await withStore(null, ({ store, state, setFloor }) => {
			const hk = kb("policy");
			for (let i = 0; i < 10; i++) {
				put(store, hk, kb(`${i}`), "x");
			}
			expect(buckets(state, P(hk))).toEqual([]);
			setFloor(4 * MAX_EST);
			// An overwrite adds no item, so it creates no bucket.
			put(store, hk, kb("0"), "a longer value");
			expect(buckets(state, P(hk))).toEqual([]);
			put(store, hk, kb("10"), "x");
			expectExact(state, P(hk), 4 * MAX_EST);
			expect(buckets(state, P(hk))[0].itemCount).toBe(11);
		});
	});

	it("logs the defect one time and counts the whole routingHash when more than 16 items have no bucket", async () => {
		expectedDefects = 1;
		await withStore(null, ({ store, state }) => {
			const hk = kb("many");
			for (let i = 0; i <= BUCKET_THRESHOLD_MAX; i++) {
				const sk = kb(`${String(i).padStart(2, "0")}`);
				insertRow(state, hk, hk.byteLength + sk.byteLength, { sk });
			}
			put(store, hk, kb("last"), "x");
			expectExact(state, P(hk), null);
			expect(buckets(state, P(hk))[0].itemCount).toBe(BUCKET_THRESHOLD_MAX + 2);
		});
	});

	it("after a cap reduction of 3 times, the import of a hash child creates the bucket that the leaf did not have, and finds the routingHash", async () => {
		// The old T gives the floor 4 * MAX_EST, so m = 4. The new T is a third of the old T, so the
		// child has m = 1. Three items near MAX_EST are below the old floor and above the new T.
		const oldT = 8 * MAX_EST;
		const newT = oldT / 3;
		const hk = KeyCodec.asKeyBytes(new Uint8Array(MAX_ITEM_BYTES - 1).fill(97));
		const rows = [1, 2, 3].map((i) => {
			const sk = KeyCodec.asKeyBytes(new Uint8Array([i]));
			const itemBytes = hk.byteLength + sk.byteLength;
			return { routingHash: P(hk), hk, sk, facts: itemFacts(ITEMS_MODEL, { itemBytes, keyBytes: itemBytes, indexed: true }) };
		});
		expect(rounded(rows.map((r) => r.facts).reduce(addFacts, { ...NO_FACTS }))).toBeGreaterThanOrEqual(newT);
		const insertAll = (state: DurableObjectState) => {
			for (const row of rows) {
				insertRow(state, hk, row.facts.itemBytes, { sk: row.sk, ttl: 2_000_000_000 });
			}
		};
		await withStore(oldT / 2, ({ store, state, setFloor }) => {
			insertAll(state);
			for (const row of rows) {
				store.itemStats.change(row.routingHash, row.hk, row.sk, null, row.facts);
			}
			setFloor(newT / 2);
			expect(buckets(state, P(hk))).toEqual([]);
			expect(store.itemStats.largestAtLeast(newT, 5)).toEqual([]);
		});
		await withStore(newT / 2, ({ store, state }) => {
			insertAll(state);
			store.itemStats.addRows(rows);
			expectExact(state, P(hk), newT / 2);
			expect(store.itemStats.largestAtLeast(newT, 5)).toEqual([hk]);
		});
	});

	it("keeps invariant I14 and exact buckets through random writes, deletes, and TTL expiries, for m from 1 to 16", async () => {
		const [ca, cb] = collidingHashKeys(GROUP).map((raw) => KeyCodec.encode(raw));
		const hks = [kb("a"), kb("b"), ca, cb];
		const sks = Array.from({ length: 20 }, (_, i) => kb(`${i}`));
		const arbOp = fc.oneof(
			{
				weight: 4,
				arbitrary: fc.record({
					op: fc.constant("put" as const),
					hk: fc.nat(hks.length - 1),
					sk: fc.nat(sks.length - 1),
					size: fc.oneof(fc.nat(50), fc.integer({ min: 900, max: 6000 })),
					ttl: fc.option(fc.integer({ min: 1, max: 100 }), { nil: null }),
				}),
			},
			{ weight: 1, arbitrary: fc.record({ op: fc.constant("delete" as const), hk: fc.nat(hks.length - 1), sk: fc.nat(sks.length - 1) }) },
			{ weight: 1, arbitrary: fc.record({ op: fc.constant("expire" as const), now: fc.integer({ min: 1, max: 100 }) }) },
		);
		await withStore(null, async ({ store, state, setFloor }) => {
			await fc.assert(
				fc.asyncProperty(
					fc.integer({ min: 1, max: BUCKET_THRESHOLD_MAX }),
					fc.array(arbOp, { minLength: 1, maxLength: 60 }),
					async (m, ops) => {
						state.storage.sql.exec(`DELETE FROM items`);
						state.storage.sql.exec(`DELETE FROM item_stats`);
						const floor = m * MAX_EST;
						expect(bucketThreshold(ITEMS_MODEL, floor)).toBe(m);
						setFloor(floor);
						for (const op of ops) {
							if (op.op === "put") {
								put(store, hks[op.hk], sks[op.sk], "x".repeat(op.size), { ttlAt: op.ttl });
							} else if (op.op === "delete") {
								del(store, hks[op.hk], sks[op.sk]);
							} else {
								store.deleteExpiredItems(op.now, 100, () => true);
							}
							for (const hk of hks) {
								if (buckets(state, P(hk)).length === 0) {
									expect(sqlSums(state, P(hk)).itemCount).toBeLessThan(m);
								} else {
									expectExact(state, P(hk), floor);
								}
							}
						}
					},
				),
				{ numRuns: propertyRuns(30) },
			);
		});
	});
});

describe("ItemStats promotion lookup", () => {
	const MB = 1024 * 1024;
	const hashIdentity = {
		schema: 1,
		ref: { partitionId: "00", doName: "leaf" },
		kind: "hash",
		hash: { rootIndex: 0, path: [], depth: 0, start: new Uint8Array(8), end: null },
		topology: { shardGroup: GROUP, rootTreesN: 4, hashSplitN: 4 },
		routingHashSeed: "",
	} as unknown as FokosPartitionIdentity;
	const policy = { hashSplitConditions: { maxSizeMb: 1 }, rangeSplitConditions: { maxSizeMb: 1 } } as FokosDBPolicy;

	/** The reference: each first bucket at or above `threshold` by its estimate from the facts, largest first, up to 5. */
	function reference(state: DurableObjectState, threshold: number): RoutingHash[] {
		return state.storage.sql
			.exec<{ routing_hash: ArrayBuffer }>(`SELECT routing_hash FROM item_stats`)
			.toArray()
			.map(({ routing_hash }) => new Uint8Array(routing_hash) as RoutingHash)
			.map((rh) => ({ rh, est: rounded(sqlSums(state, rh)) }))
			.filter(({ est }) => est >= threshold)
			.sort((x, y) => y.est - x.est)
			.slice(0, 5)
			.map(({ rh }) => rh);
	}
	const lookup = (store: PartitionStore, threshold: number) => store.itemStats.largestAtLeast(threshold, 5).map((hk) => P(hk));
	/** The lookup equals the reference. Two routingHashes with equal estimates can come back in either order. */
	function expectLookup(store: PartitionStore, state: DurableObjectState, threshold: number): void {
		const ests = (rhs: RoutingHash[]) => rhs.map((rh) => rounded(sqlSums(state, rh)));
		expect(ests(lookup(store, threshold))).toEqual(ests(reference(state, threshold)));
	}

	it("finds the routingHashes at or above T by estimate, and not by item bytes", async () => {
		await withStore(0, ({ store, state }) => {
			// Many small items with long keys: few item bytes, but a large estimate from the key copies and
			// the fixed bytes of each item.
			const longKey = (i: number) => kb(`${"k".repeat(200)}-${i}`);
			for (let i = 0; i < 40; i++) {
				put(store, kb("small"), longKey(i), "");
			}
			// A few large items: more item bytes, a smaller estimate.
			for (let i = 0; i < 2; i++) {
				put(store, kb("large"), kb(`${i}`), "x".repeat(8000));
			}
			// Items of 1 byte with no data.
			for (let i = 0; i < 300; i++) {
				put(store, kb("tiny"), kb(String.fromCharCode(33 + (i % 90)) + String.fromCharCode(33 + Math.floor(i / 90))), "");
			}
			const small = sqlSums(state, P(kb("small")));
			const large = sqlSums(state, P(kb("large")));
			expect(small.itemBytes).toBeLessThan(large.itemBytes);
			expect(rounded(small)).toBeGreaterThan(rounded(large));
			for (const threshold of [
				0,
				rounded(large),
				rounded(large) + 1,
				rounded(sqlSums(state, P(kb("tiny")))),
				rounded(small),
				rounded(small) + 1,
			]) {
				expectLookup(store, state, threshold);
			}
		});
	});

	it("reports a rounded estimate against a T with a part of a byte, the same on the write path and in the lookup", async () => {
		await withStore(0, ({ store }) => {
			const hk = kb("frac");
			// An odd count of key bytes on an item with a TTL gives an estimate with a part of a byte.
			const sk = kb("abc");
			const keyBytes = hk.byteLength + sk.byteLength;
			expect(keyBytes % 2).toBe(1);
			const { keyEstBytes } = put(store, hk, sk, "0123456789", { ttlAt: 2_000_000_000 });
			const exact = estimate(ITEMS_MODEL, itemFacts(ITEMS_MODEL, { itemBytes: 10 + keyBytes, keyBytes, indexed: true }));
			expect(exact % 1).not.toBe(0);
			expect(keyEstBytes).toBe(Math.ceil(exact));
			const planner = (threshold: number) =>
				new SplitPlanner({
					itemStats: store.itemStats,
					source: { probeRoutingHashes: () => null, compositeKeyFacts: () => [], countItems: () => 0 },
					databaseSize: () => 0,
					promotionFraction: () => threshold / MB,
					rangeSplitN: () => 4,
					logName: "test",
					logParams: () => ({}),
				});
			// T between the exact estimate and its rounded value: both checks report it.
			const below = (exact + Math.ceil(exact)) / 2;
			expect(planner(below).promotionCandidates(hashIdentity, policy, [{ hashKey: hk, keyEstBytes }])).toEqual([{ hashKey: hk }]);
			expect(store.itemStats.largestAtLeast(below, 5)).toEqual([hk]);
			// T above the rounded value: neither reports it.
			const above = Math.ceil(exact) + 0.25;
			expect(planner(above).promotionCandidates(hashIdentity, policy, [{ hashKey: hk, keyEstBytes }])).toEqual([]);
			expect(store.itemStats.largestAtLeast(above, 5)).toEqual([]);
		});
	});

	it("stores the estimate only at or above the floor, from NULL to a value when a write reaches the floor", async () => {
		await withStore(ONE_ITEM_FLOOR, ({ store, state, setFloor }) => {
			const hk = kb("grows");
			put(store, hk, kb("1"), "x".repeat(100));
			const floor = rounded(sqlSums(state, P(hk))) + 50;
			setFloor(floor);
			put(store, hk, kb("1"), "x".repeat(110));
			expect(buckets(state, P(hk))[0].promotionEst).toBeNull();
			put(store, hk, kb("1"), "x".repeat(200));
			expectExact(state, P(hk), floor);
			expect(buckets(state, P(hk))[0].promotionEst).toBe(rounded(sqlSums(state, P(hk))));
		});
	});

	it("follows a cap reduction of 2 times with no write, and finds a routingHash below the old floor after its next write", async () => {
		await withStore(ONE_ITEM_FLOOR, ({ store, state, setFloor }) => {
			const T = 20_000;
			setFloor(T / 2);
			// Estimates of about 0.3 T to 1.5 T.
			const sizes = [6, 8, 12, 16, 22, 30];
			for (const [i, n] of sizes.entries()) {
				for (let j = 0; j < n; j++) {
					put(store, kb(`rh${i}`), kb(`${j}`), "x".repeat(900));
				}
			}
			expectLookup(store, state, T);
			// A cap reduction of 2 times: the new T is the old floor.
			expectLookup(store, state, T / 2);
			// A cap reduction of 3 times: a routingHash between the new T and the old floor keeps NULL.
			const third = T / 3;
			const missed = reference(state, third).filter((rh) => !lookup(store, third).some((x) => x.toHex() === rh.toHex()));
			expect(missed.length).toBeGreaterThan(0);
			for (const rh of missed) {
				expect(buckets(state, rh)[0].promotionEst).toBeNull();
			}
			// The next write of that routingHash binds the floor of the new policy.
			setFloor(third / 2);
			const i = sizes.findIndex((_, k) => P(kb(`rh${k}`)).toHex() === missed[0].toHex());
			put(store, kb(`rh${i}`), kb("new"), "x");
			expect(lookup(store, third).map((rh) => rh.toHex())).toContain(missed[0].toHex());
			// A raise of the cap: the lookup filters on T, so it returns no routingHash below the new T.
			for (const rh of lookup(store, 4 * T)) {
				expect(rounded(sqlSums(state, rh))).toBeGreaterThanOrEqual(4 * T);
			}
			expectLookup(store, state, 4 * T);
		});
	});

	it("takes the next write or import to replace a stale estimate", async () => {
		await withStore(0, async ({ store, state }) => {
			const hk = kb("stale");
			put(store, hk, kb("1"), "x".repeat(500));
			put(store, hk, kb("2"), "x".repeat(500));
			// A value of other coefficients, as after a coefficient change.
			state.storage.sql.exec(`UPDATE item_stats SET promotion_est_bytes = 7 WHERE routing_hash = ?`, P(hk));
			const page = [...store.queryItemsPage(null, 100, ALL)];
			put(store, hk, kb("1"), "x".repeat(501));
			expectExact(state, P(hk), 0);
			// An import builds its buckets with the coefficients of the code.
			await withStore(0, ({ store: child, state: childState }) => {
				child.itemStats.addRows(
					page.flatMap((item) => {
						const facts = child.insertItemIfAbsent(item);
						return facts ? [{ routingHash: item.routingHash, hk: item.hk, sk: item.sk, facts }] : [];
					}),
				);
				expectExact(childState, P(hk), 0);
			});
		});
	});

	it("reads at most 5 index entries and 5 seeks of one row, for each leaf size", async () => {
		await withStore(0, ({ store, state }) => {
			for (let i = 0; i < 40; i++) {
				put(store, kb(`rh${i}`), kb("1"), "x".repeat(100 + i));
				put(store, kb(`rh${i}`), kb("2"), "x");
			}
			const sql = state.storage.sql;
			const index = sql.exec(
				`SELECT routing_hash FROM item_stats WHERE promotion_est_bytes >= ?1 ORDER BY promotion_est_bytes DESC LIMIT ?2`,
				0,
				5,
			);
			const rows = index.toArray();
			expect(rows).toHaveLength(5);
			expect(index.rowsRead).toBeLessThanOrEqual(5);
			for (const { routing_hash } of rows) {
				const seek = sql.exec(`SELECT hk FROM items WHERE routing_hash = ?1 ORDER BY hk, sk LIMIT 1`, routing_hash);
				seek.toArray();
				expect(seek.rowsRead).toBeLessThanOrEqual(1);
			}
			expectLookup(store, state, 0);
		});
	});

	it("uses the partial index for the lookup and the primary key for the cleanup and the recount", async () => {
		await withStore(0, ({ state }) => {
			const plan = (query: string, ...params: unknown[]) =>
				state.storage.sql
					.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`, ...params)
					.toArray()
					.map((r) => r.detail)
					.join(" | ");
			expect(
				plan(`SELECT routing_hash FROM item_stats WHERE promotion_est_bytes >= ?1 ORDER BY promotion_est_bytes DESC LIMIT ?2`, 1, 5),
			).toBe("SEARCH item_stats USING COVERING INDEX item_stats_promotion (promotion_est_bytes>?)");
			expect(plan(`DELETE FROM item_stats WHERE routing_hash = ?1`, P(kb("a")))).toBe(
				"SEARCH item_stats USING PRIMARY KEY (routing_hash=?)",
			);
			expect(plan(`SELECT COUNT(*), SUM(item_bytes) FROM items WHERE routing_hash = ?1`, P(kb("a")))).toBe(
				"SEARCH items USING PRIMARY KEY (routing_hash=?)",
			);
			expect(plan(`SELECT hk FROM items WHERE routing_hash = ?1 ORDER BY hk, sk LIMIT 1`, P(kb("a")))).toBe(
				"SEARCH items USING PRIMARY KEY (routing_hash=?)",
			);
		});
	});

	it("returns the sixth routingHash first after deletes reduce the first five", async () => {
		await withStore(0, ({ store, state }) => {
			const counts = [10, 9, 8, 7, 6, 5];
			for (const [i, n] of counts.entries()) {
				for (let j = 0; j < n; j++) {
					put(store, kb(`rh${i}`), kb(`${j}`), "x".repeat(10 * 1024));
				}
			}
			const T = 10 * 1024;
			expect(lookup(store, T).map((rh) => rh.toHex())).toEqual(counts.slice(0, 5).map((_, i) => P(kb(`rh${i}`)).toHex()));
			for (const [i, n] of counts.slice(0, 5).entries()) {
				for (let j = 1; j < n; j++) {
					del(store, kb(`rh${i}`), kb(`${j}`));
				}
			}
			expect(lookup(store, T)[0].toHex()).toBe(P(kb("rh5")).toHex());
			expectLookup(store, state, T);
		});
	});

	it("reports the same estimate on the write path and in the lookup, for long sort keys and TTLs", async () => {
		await withStore(0, ({ store }) => {
			const hk = kb("ttl");
			let last = 0;
			for (let i = 0; i < 20; i++) {
				last = put(store, hk, kb(`${"s".repeat(300)}${i}`), "x".repeat(i * 37), { ttlAt: i % 2 === 0 ? 2_000_000_000 : null }).keyEstBytes;
			}
			expect(store.itemStats.largestAtLeast(last, 5)).toEqual([hk]);
			expect(store.itemStats.largestAtLeast(last + 1, 5)).toEqual([]);
		});
	});
});

describe("SplitPlanner decision of a range leaf", () => {
	const policy = { hashSplitConditions: { maxSizeMb: 1 }, rangeSplitConditions: { maxSizeMb: 1 } } as FokosDBPolicy;
	const rangeIdentity = {
		schema: 1,
		ref: { partitionId: "00", doName: "leaf" },
		kind: "range",
		range: { routingHash: P(kb("r")), start: null, end: null },
		topology: { shardGroup: GROUP, rootTreesN: 4, hashSplitN: 4 },
		routingHashSeed: "",
	} as unknown as FokosPartitionIdentity;

	it("answers a split with at least N items and the floor with fewer, and does not scan the slice", () => {
		const counted: number[] = [];
		const planner = (items: number) =>
			new SplitPlanner({
				itemStats: {} as ItemStats,
				source: {
					probeRoutingHashes: () => null,
					compositeKeyFacts: () => {
						throw new Error("the decision must not scan the slice");
					},
					countItems: (_rh, _start, _end, limit) => {
						counted.push(limit);
						return Math.min(items, limit);
					},
				},
				databaseSize: () => 2 * 1024 * 1024,
				promotionFraction: () => 0.25,
				rangeSplitN: () => 4,
				logName: "test",
				logParams: () => ({}),
			});
		const error = vi.spyOn(console, "error").mockImplementation(() => {});
		expect(planner(10).evaluateSplit({ identity: rangeIdentity, policy })).toEqual({});
		expect(planner(3).evaluateSplit({ identity: rangeIdentity, policy })).toBe(false);
		expect(error).toHaveBeenCalledWith(expect.objectContaining({ reason: "fewer_items" }));
		expect(counted).toEqual([4, 4]);
	});
});
