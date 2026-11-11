import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { FokosDBMaterializationDO } from "./do-materialization.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import { MaterializationStore, type LatestImage, type TargetPositions } from "./materialization-store.js";
import { routingHashFor } from "../../../test/routing-hash.js";
import { itemFacts, LATEST_ITEMS_MODEL, maxItemEstimate, NO_FACTS } from "../../shared/partition/item-size.js";
import { addFacts } from "../../shared/partition/item-stats.js";

/** The routingHash of a hash key, as the partition stores it. */
const P = (hk: Parameters<typeof routingHashFor>[0]) => routingHashFor(hk);

const kb = (value: string) => KeyCodec.encode(value);
const NO_SK = KeyCodec.encodeOptional(undefined);

type Harness = {
	store: MaterializationStore;
	state: DurableObjectState;
	/** Applies revision `revision` of item `hk` as the ingest does, with the positions of the targets. */
	ingest: (hk: string, revision: number, targets: TargetPositions[], deleted?: boolean) => void;
	/** The positions of the partition, in `seq` order. */
	positions: () => Array<{ seq: number; item_id: number; prev_seq: number | null }>;
	newestSeq: (hk: string) => number | null | undefined;
};

async function withStore(fn: (h: Harness) => void): Promise<void> {
	const stub = env.MATERIALIZATION_DO.getByName(`mat-store.${crypto.randomUUID()}`);
	await runInDurableObject(stub, (_instance: FokosDBMaterializationDO, state: DurableObjectState) => {
		const store = new MaterializationStore(state.storage, {
			promotionFloor: () => maxItemEstimate(LATEST_ITEMS_MODEL),
			isRangeLeaf: () => false,
			logParams: () => ({}),
		});
		const image = (hk: string, revision: number, deleted: boolean): LatestImage =>
			deleted
				? { opIndex: 0, hashKey: kb(hk), sortKey: NO_SK, revision, deleted: true }
				: { opIndex: 0, hashKey: kb(hk), sortKey: NO_SK, revision, deleted: false, data: `v${revision}`, kind: "text", ttlAt: null };
		fn({
			store,
			state,
			ingest: (hk, revision, targets, deleted = false) =>
				store.putLatestImage(
					P(image(hk, revision, deleted).hashKey),
					image(hk, revision, deleted),
					1_000,
					store.storedImage(P(kb(hk)), kb(hk), NO_SK),
					targets,
				),
			positions: () =>
				state.storage.sql
					.exec<{
						seq: number;
						item_id: number;
						prev_seq: number | null;
					}>(`SELECT seq, item_id, prev_seq FROM change_positions ORDER BY seq`)
					.toArray(),
			newestSeq: (hk) => store.storedImage(P(kb(hk)), kb(hk), NO_SK)?.newestSeq,
		});
	});
}

/** A target that read up to `cursor` and has no batch in flight. */
const at = (cursor: number): TargetPositions => ({ cursor, read: cursor });

describe("MaterializationStore — change positions", () => {
	it("writes no position when no target exists", async () => {
		await withStore((h) => {
			h.ingest("x", 1, []);
			expect(h.positions()).toEqual([]);
			expect(h.newestSeq("x")).toBeNull();
		});
	});

	it("keeps one position for each group of cursors while a target lags (steps 1, 4, and 3)", async () => {
		await withStore((h) => {
			const lake = at(10);
			// Step 1: X has no position. The new position is above each cursor.
			h.ingest("x", 1, [at(10), lake]);
			const itemId = h.positions()[0].item_id;
			expect(h.positions()).toEqual([{ seq: 11, item_id: itemId, prev_seq: null }]);

			// The GSI read 11. Step 4: lake needs 11, and 11 has no older position.
			h.ingest("x", 2, [at(11), lake]);
			expect(h.positions()).toEqual([
				{ seq: 11, item_id: itemId, prev_seq: null },
				{ seq: 12, item_id: itemId, prev_seq: 11 },
			]);
			expect(h.newestSeq("x")).toBe(12);

			// The GSI read 12. Step 3: lake reaches 11 first, so 12 moves.
			h.ingest("x", 3, [at(12), lake]);
			expect(h.positions()).toEqual([
				{ seq: 11, item_id: itemId, prev_seq: null },
				{ seq: 13, item_id: itemId, prev_seq: 11 },
			]);
			h.ingest("x", 4, [at(13), lake]);
			expect(h.positions().map((p) => p.seq)).toEqual([11, 14]);
			expect(h.newestSeq("x")).toBe(14);
		});
	});

	it("writes no position when no target read the newest position (step 2)", async () => {
		await withStore((h) => {
			h.ingest("x", 1, [at(0)]);
			h.ingest("x", 2, [at(0)]);
			h.ingest("x", 3, [at(0)], true);
			expect(h.positions().map((p) => p.seq)).toEqual([1]);
			expect(h.newestSeq("x")).toBe(1);
		});
	});

	it("gives a new position when a batch in flight read the newest position", async () => {
		await withStore((h) => {
			h.ingest("x", 1, [at(0)]);
			// The batch in flight read position 1, and its cursor is still 0. If it fails, it reads 1
			// again; if it applies, the cursor moves past 1, so the new image needs a new position.
			h.ingest("x", 2, [{ cursor: 0, read: 1 }]);
			expect(h.positions().map((p) => [p.seq, p.prev_seq])).toEqual([
				[1, null],
				[2, 1],
			]);
		});
	});

	it("counts a position at or below the lowest cursor as absent, and puts a new position above each cursor", async () => {
		await withStore((h) => {
			h.ingest("x", 1, [at(0)]);
			h.store.deletePositionsUpTo(1, 10);
			// The cursor is above each stored position, so the next position is above the cursor.
			h.ingest("x", 2, [at(20)]);
			expect(h.positions().map((p) => [p.seq, p.prev_seq])).toEqual([[21, null]]);
		});
	});
});

describe("MaterializationStore — item size", () => {
	it("counts the data and the keys for a live image, and only the keys for an indexed tombstone", async () => {
		await withStore((h) => {
			const keys = kb("x").byteLength + NO_SK.byteLength;
			h.ingest("x", 1, []);
			const live = {
				itemCount: 1,
				itemBytes: "v1".length + keys,
				keyBytes: keys,
				indexedItemCount: 0,
				indexedKeyBytes: 0,
				overflowBytes: 0,
			};
			expect(h.store.storedImage(P(kb("x")), kb("x"), NO_SK)!.facts).toEqual(live);
			expect(h.store.itemStats.factsOf(P(kb("x")))).toEqual(live);
			h.ingest("x", 2, [], true);
			const tombstone = { itemCount: 1, itemBytes: keys, keyBytes: keys, indexedItemCount: 1, indexedKeyBytes: keys, overflowBytes: 0 };
			expect(h.store.storedImage(P(kb("x")), kb("x"), NO_SK)!.facts).toEqual(tombstone);
			expect(h.store.itemStats.factsOf(P(kb("x")))).toEqual(tombstone);
		});
	});
});

describe("MaterializationStore — buckets on a range leaf", () => {
	it("keeps each bucket equal to the sums of its key range through live images, tombstones, and tombstone collection", async () => {
		const stub = env.MATERIALIZATION_DO.getByName(`mat-store.${crypto.randomUUID()}`);
		await runInDurableObject(stub, (_instance: FokosDBMaterializationDO, state: DurableObjectState) => {
			const K = 2;
			const store = new MaterializationStore(state.storage, {
				promotionFloor: () => null,
				isRangeLeaf: () => true,
				logParams: () => ({}),
				rangeBucketMinItems: K,
			});
			const hk = kb("r");
			const rh = P(hk);
			const image = (sk: string, revision: number, deleted: boolean): LatestImage =>
				deleted
					? { opIndex: 0, hashKey: hk, sortKey: kb(sk), revision, deleted: true }
					: {
							opIndex: 0,
							hashKey: hk,
							sortKey: kb(sk),
							revision,
							deleted: false,
							data: "v".repeat(revision * 5),
							kind: "text",
							ttlAt: null,
						};
			const apply = (sk: string, revision: number, deleted: boolean) =>
				store.putLatestImage(rh, image(sk, revision, deleted), 1_000, store.storedImage(rh, hk, kb(sk)), []);
			const expectExact = () => {
				const rows = state.storage.sql
					.exec<{
						lower_hk: ArrayBuffer;
						lower_sk: ArrayBuffer;
						item_count: number;
						item_bytes: number;
						indexed_item_count: number;
						promotion_est_bytes: number | null;
					}>(
						`SELECT lower_hk, lower_sk, item_count, item_bytes, indexed_item_count, promotion_est_bytes FROM item_stats WHERE routing_hash = ? ORDER BY lower_hk, lower_sk`,
						rh,
					)
					.toArray();
				for (const [i, row] of rows.entries()) {
					const next = rows[i + 1];
					const sums = state.storage.sql
						.exec<{ c: number; b: number; t: number }>(
							`SELECT COUNT(*) AS c, COALESCE(SUM(item_bytes), 0) AS b, COALESCE(SUM(deleted_at_ms IS NOT NULL), 0) AS t FROM latest_items
							  WHERE routing_hash = ?1 AND (base_hk, base_sk) >= (?2, ?3) ${next ? "AND (base_hk, base_sk) < (?4, ?5)" : ""}`,
							rh,
							row.lower_hk,
							row.lower_sk,
							...(next ? [next.lower_hk, next.lower_sk] : []),
						)
						.one();
					expect([row.item_count, row.item_bytes, row.indexed_item_count]).toEqual([sums.c, sums.b, sums.t]);
					expect(row.item_count).toBeLessThanOrEqual(2 * K);
					expect(row.promotion_est_bytes).toBeNull();
				}
				return rows;
			};
			const sks = Array.from({ length: 9 }, (_, i) => `s${i}`);
			for (const sk of sks) {
				apply(sk, 1, false);
				expectExact();
			}
			expect(expectExact().length).toBeGreaterThan(1);
			for (const sk of sks) {
				apply(sk, 2, true);
				expectExact();
			}
			for (const sk of sks) {
				expect(store.deleteTombstone(store.storedImage(rh, hk, kb(sk))!.itemId)).toBe(true);
				expectExact();
			}
			expect(expectExact()).toEqual([]);
		});
	});
});

describe("MaterializationStore — the item-id counter", () => {
	const put = (store: MaterializationStore, hk: string, sk = "s", deleted = false) => {
		const image: LatestImage = deleted
			? { opIndex: 0, hashKey: kb(hk), sortKey: kb(sk), revision: 2, deleted: true }
			: { opIndex: 0, hashKey: kb(hk), sortKey: kb(sk), revision: 1, deleted: false, data: "d", kind: "text", ttlAt: null };
		store.putLatestImage(P(kb(hk)), image, 1_000, store.storedImage(P(kb(hk)), kb(hk), kb(sk)), []);
	};
	const idOf = (h: Harness, hk: string, sk = "s") => h.store.storedImage(P(kb(hk)), kb(hk), kb(sk))!.itemId;
	const highWaterMark = (h: Harness) => h.state.storage.sql.exec<{ hwm: number }>(`SELECT hwm FROM item_id_counter WHERE id = 1`).one().hwm;

	it("an overwrite keeps the id, and a new item after the delete of its tombstone gets a higher id", async () => {
		await withStore((h) => {
			put(h.store, "a");
			put(h.store, "b");
			const a = idOf(h, "a");
			const b = idOf(h, "b");
			expect(b).toBeGreaterThan(a);
			put(h.store, "a");
			expect(idOf(h, "a")).toBe(a);
			// A tombstone keeps the id of its item.
			put(h.store, "b", "s", true);
			expect(idOf(h, "b")).toBe(b);
			// The row with the highest id goes, and its id never comes back.
			expect(h.store.deleteTombstone(b)).toBe(true);
			put(h.store, "b");
			expect(idOf(h, "b")).toBeGreaterThan(b);
		});
	});

	it("reserves a block of 1024 ids with one row written, and a new store reserves above it", async () => {
		await withStore((h) => {
			expect(highWaterMark(h)).toBe(1);
			put(h.store, "a");
			expect(idOf(h, "a")).toBe(1);
			expect(highWaterMark(h)).toBe(1025);
			put(h.store, "b");
			expect(highWaterMark(h)).toBe(1025);
			// A restart loses the rest of the block, and the next item takes an id above it.
			put(new MaterializationStore(h.state.storage), "c");
			expect(idOf(h, "c")).toBe(1025);
			expect(highWaterMark(h)).toBe(2049);
		});
	});

	// A reservation that rolls back must not stay in memory: the ids of that block are not reserved in
	// storage, and a later reservation, also by another instance, gives them out again.
	it("gives no id two times after a transaction that rolls back its reservation", async () => {
		await withStore((h) => {
			expect(() =>
				h.store.transactionSync(() => {
					put(h.store, "rolled-back");
					throw new Error("roll back");
				}),
			).toThrow("roll back");
			expect(highWaterMark(h)).toBe(1);
			h.store.transactionSync(() => put(h.store, "a"));
			h.store.transactionSync(() => put(h.store, "b"));
			const restarted = new MaterializationStore(h.state.storage);
			put(restarted, "c");
			put(restarted, "d");
			const ids = ["a", "b", "c", "d"].map((hk) => idOf(h, hk));
			expect(new Set(ids).size).toBe(4);
		});
	});

	it("an ingest raises the mark above its copied ids, and the next item takes an id above them", async () => {
		await withStore((h) => {
			// The block in memory holds the id that the copied item has.
			put(h.store, "local");
			expect(idOf(h, "local")).toBe(1);
			h.store.transactionSync(() => {
				h.store.insertMigratedItem({
					item_id: 2,
					routingHash: P(kb("copied")),
					hk: kb("copied"),
					sk: kb("s"),
					revision: 1,
					deleted_at_ms: null,
					newest_seq: null,
					data_kind: 0,
					ttl_at: null,
					item_bytes: 1,
					data: "d",
					positions: [],
					retries: [],
				});
				h.store.raiseItemIdMark(2);
			});
			expect(highWaterMark(h)).toBe(1025);
			put(h.store, "next");
			expect(idOf(h, "next")).toBe(1025);
			// A lower mark does not lower it.
			h.store.raiseItemIdMark(10);
			expect(highWaterMark(h)).toBe(2049);
		});
	});

	it("fails the insert of a copied item whose id another item holds, and skips a copy of the same item", async () => {
		await withStore((h) => {
			put(h.store, "local");
			const copy = (hk: string) => ({
				item_id: idOf(h, "local"),
				routingHash: P(kb(hk)),
				hk: kb(hk),
				sk: kb("s"),
				revision: 1,
				deleted_at_ms: null,
				newest_seq: null,
				data_kind: 0,
				ttl_at: null,
				item_bytes: 1,
				data: "d",
				positions: [],
				retries: [],
			});
			expect(h.store.insertMigratedItem(copy("local"))).toBeUndefined();
			expect(() => h.store.insertMigratedItem(copy("other"))).toThrow(/UNIQUE constraint failed: latest_items\.item_id/);
		});
	});
});

/**
 * A store whose storage records the text and the parameters of each `sql.exec` call. `queries` returns
 * the text of each recorded statement since its last call, and clears the record. Each call runs
 * on the real storage without change. `plans` returns the query plan of each recorded statement on
 * `latest_items` since its last call, and clears the record.
 */
function planRecorder(storage: DurableObjectStorage): { store: MaterializationStore; plans: () => string[]; queries: () => string[] } {
	const statements: Array<{ query: string; params: unknown[] }> = [];
	// Native methods need their own object as `this`, so the proxy binds each method it forwards.
	const forward = <T extends object>(target: T, own: (prop: string | symbol) => unknown): T =>
		new Proxy(target, {
			get: (t, prop) => {
				const value = own(prop) ?? Reflect.get(t, prop, t);
				return typeof value === "function" ? value.bind(t) : value;
			},
		});
	const sql = forward(storage.sql, (prop) =>
		prop === "exec"
			? (query: string, ...params: unknown[]) => {
					statements.push({ query, params });
					return storage.sql.exec(query, ...params);
				}
			: undefined,
	);
	const store = new MaterializationStore(forward(storage, (prop) => (prop === "sql" ? sql : undefined)));
	const plans = () =>
		statements
			.splice(0)
			.filter((s) => s.query.includes("latest_items"))
			.map((s) =>
				storage.sql
					.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${s.query}`, ...s.params)
					.toArray()
					.map((r) => r.detail)
					.join(" | "),
			);
	plans();
	const queries = () => statements.splice(0).map((s) => s.query);
	return { store, plans, queries };
}

describe("MaterializationStore — plans of the storage order", () => {
	const live = (hk: string): LatestImage => ({
		opIndex: 0,
		hashKey: kb(hk),
		sortKey: NO_SK,
		revision: 1,
		deleted: false,
		data: "d",
		kind: "text",
		ttlAt: null,
	});
	const tombstone = (hk: string): LatestImage => ({ opIndex: 0, hashKey: kb(hk), sortKey: NO_SK, revision: 1, deleted: true });
	const byItemId = "USING INDEX sqlite_autoindex_latest_items_2 (item_id=?)";

	it("seeks the primary key for a point read and for the promotion cleanup", async () => {
		await withStore((h) => {
			const { store, plans } = planRecorder(h.state.storage);
			store.putLatestImage(P(kb("x")), live("x"), 1_000, undefined, []);
			plans();

			store.storedImage(P(kb("x")), kb("x"), NO_SK);
			expect(plans()).toEqual(["SEARCH latest_items USING PRIMARY KEY (routing_hash=? AND base_hk=? AND base_sk=?)"]);

			expect(store.transactionSync(() => store.deleteItemsOfRoutingHash(P(kb("x")), 10))).toBe(true);
			expect(plans()).toEqual([
				"SEARCH latest_items USING PRIMARY KEY (routing_hash=?)",
				"SEARCH latest_items USING PRIMARY KEY (routing_hash=? AND (base_hk,base_sk)<(?,?))",
			]);
		});
	});

	it("seeks UNIQUE (item_id) for the target reads, the retries, and the tombstone delete", async () => {
		await withStore((h) => {
			const { store, plans } = planRecorder(h.state.storage);
			h.state.storage.sql.exec(`INSERT INTO materialization_targets (target_id, kind) VALUES ('t1', 'gsi')`);
			store.putLatestImage(P(kb("x")), live("x"), 1_000, undefined, [at(0)]);
			const x = store.storedImage(P(kb("x")), kb("x"), NO_SK)!.itemId;
			store.recordTargetRetry("t1", x, 1_000, 10, 100, null);
			store.putLatestImage(P(kb("y")), tombstone("y"), 1_000, undefined, []);
			const y = store.storedImage(P(kb("y")), kb("y"), NO_SK)!.itemId;
			plans();

			expect([...store.positionsAfter("t1", 0, 10)].map((p) => p.itemId)).toEqual([x]);
			const [positions] = plans();
			expect(positions).toContain("SEARCH p USING INTEGER PRIMARY KEY (rowid>?)");
			expect(positions).toContain(`SEARCH li ${byItemId}`);

			expect(store.dueRetryItems("t1", 10_000, 10).map((r) => r.itemId)).toEqual([x]);
			const [retries] = plans();
			expect(retries).toContain(`SEARCH li ${byItemId}`);

			expect(store.itemRevision(x)).toBe(1);
			expect(store.targetImage(x)?.revision).toBe(1);
			expect(plans()).toEqual([`SEARCH latest_items ${byItemId}`, `SEARCH latest_items ${byItemId}`]);

			expect(store.deleteTombstone(y)).toBe(true);
			expect(plans()).toEqual([`SEARCH latest_items ${byItemId}`]);
		});
	});

	// The split planner and the split decision read a slice with these two methods. Each bound shape and
	// each order must seek the primary key, never scan the table, and never sort in a temporary B-tree.
	it("seeks the primary key for each slice read of compositeKeyFacts and countItems", async () => {
		await withStore((h) => {
			const { store, plans } = planRecorder(h.state.storage);
			const hk = kb("hk");
			const rh = P(hk);
			const sks = ["a", "b", "c", "d", "e"];
			for (const sk of sks) {
				store.putLatestImage(rh, { ...live("hk"), sortKey: kb(sk) }, 1_000, undefined, []);
			}
			plans();
			const at = (sk: string) => ({ hk, sk: kb(sk) });
			const search = (start: boolean, end: boolean) =>
				`SEARCH latest_items USING PRIMARY KEY (routing_hash=?${start ? " AND (base_hk,base_sk)>(?,?)" : ""}${end ? " AND (base_hk,base_sk)<(?,?)" : ""})`;
			const cases = [
				{ start: null, end: null, keys: sks },
				{ start: at("b"), end: null, keys: ["b", "c", "d", "e"] },
				{ start: null, end: at("d"), keys: ["a", "b", "c"] },
				{ start: at("b"), end: at("d"), keys: ["b", "c"] },
			];
			for (const { start, end, keys } of cases) {
				const expected = search(start !== null, end !== null);
				for (const order of ["asc", "desc"] as const) {
					const read = [...store.compositeKeyFacts(rh, start, end, order)].map((item) => KeyCodec.decode(item.sk));
					expect(read).toEqual(order === "asc" ? keys : [...keys].reverse());
					expect(plans()).toEqual([expected]);
				}
				expect(store.countItems(rh, start, end, 100)).toBe(keys.length);
				const [count, ...rest] = plans();
				expect(rest).toEqual([]);
				expect(count).toContain(expected);
				expect(count).not.toMatch(/SCAN latest_items|TEMP B-TREE/);

				// The reads of ItemStats: the facts with and with no limit, and the item at an offset.
				const factsOf = (items: Iterable<{ hk: KeyBytes; sk: KeyBytes; itemBytes: number; indexed: boolean }>) =>
					[...items].reduce(
						(sum, item) =>
							addFacts(
								sum,
								itemFacts(store.itemStats.model, {
									itemBytes: item.itemBytes,
									keyBytes: item.hk.byteLength + item.sk.byteLength,
									indexed: item.indexed,
								}),
							),
						{ ...NO_FACTS },
					);
				const all = [...store.compositeKeyFacts(rh, start, end)];
				plans();
				expect(store.countFacts(rh, start, end)).toEqual(factsOf(all));
				expect(store.countFacts(rh, start, end, 2)).toEqual(factsOf(all.slice(0, 2)));
				expect(KeyCodec.decode(store.itemAt(rh, start, end, 1)!.sk)).toBe(keys[1]);
				expect(store.itemAt(rh, start, end, keys.length)).toBeUndefined();
				const reads = plans();
				expect(reads).toHaveLength(4);
				for (const read of reads) {
					expect(read).toContain(expected);
					expect(read).not.toMatch(/SCAN latest_items|TEMP B-TREE/);
				}
			}
			expect(store.firstHashKey(rh)).toEqual(hk);
			expect(plans()).toEqual(["SEARCH latest_items USING PRIMARY KEY (routing_hash=?)"]);
		});
	});

	it("reads and writes no bucket when an image keeps the facts of the stored image", async () => {
		await withStore((h) => {
			const { store, queries } = planRecorder(h.state.storage);
			const rh = P(kb("x"));
			const image = (revision: number, data: string): LatestImage => ({
				opIndex: 0,
				hashKey: kb("x"),
				sortKey: NO_SK,
				revision,
				deleted: false,
				data,
				kind: "text",
				ttlAt: null,
			});
			store.putLatestImage(rh, image(1, "aaaa"), 1_000, undefined, []);
			queries();
			// The same size: the facts stay the same.
			expect(store.putLatestImage(rh, image(2, "bbbb"), 1_000, store.storedImage(rh, kb("x"), NO_SK), [])).toBeUndefined();
			expect(queries().filter((q) => q.includes("item_stats"))).toEqual([]);
			// A larger size changes the facts, so the write goes to the bucket.
			store.putLatestImage(rh, image(3, "cccccccc"), 1_000, store.storedImage(rh, kb("x"), NO_SK), []);
			expect(queries().filter((q) => q.includes("item_stats")).length).toBeGreaterThan(0);
		});
	});

	it("seeks the tombstone index from the (deleted_at_ms, item_id) cursor", async () => {
		await withStore((h) => {
			const { store, plans } = planRecorder(h.state.storage);
			store.putLatestImage(P(kb("y")), tombstone("y"), 1_000, undefined, []);
			plans();

			expect(store.tombstonesAfter({ deletedAtMs: 0, itemId: 0 }, 10)).toHaveLength(1);
			expect(plans()).toEqual(["SEARCH latest_items USING INDEX idx_latest_items_deleted_gc ((deleted_at_ms,item_id)>(?,?))"]);
		});
	});
});

describe("MaterializationStore — the cleanup of a promoted routingHash", () => {
	it("deletes only the items of its routingHash, in primary-key order, with their positions, retries, and GSI keys", async () => {
		await withStore((h) => {
			const sql = h.state.storage.sql;
			sql.exec(`INSERT INTO materialization_targets (target_id, kind) VALUES ('t1', 'gsi')`);
			const promoted = kb("promoted");
			const other = kb("other");
			expect(P(promoted).join()).not.toBe(P(other).join());
			const items: Array<[KeyBytes, string]> = [
				[promoted, "s3"],
				[promoted, "s1"],
				[promoted, "s2"],
				[other, "s1"],
			];
			for (const [hk, sk] of items) {
				const image: LatestImage = {
					opIndex: 0,
					hashKey: hk,
					sortKey: kb(sk),
					revision: 1,
					deleted: false,
					data: "d",
					kind: "text",
					ttlAt: null,
				};
				h.store.putLatestImage(P(hk), image, 1_000, undefined, [at(0)]);
				const itemId = h.store.storedImage(P(hk), hk, kb(sk))!.itemId;
				h.store.recordTargetRetry("t1", itemId, 1_000, 10, 100, null);
				h.store.addIndexKey("t1", itemId, { hk: kb(`gsi-${itemId}`), sk: NO_SK });
			}
			const itemIdOf = (hk: KeyBytes, sk: string) => h.store.storedImage(P(hk), hk, kb(sk))?.itemId;
			const otherId = itemIdOf(other, "s1");
			const linked = (table: string) => sql.exec<{ item_id: number }>(`SELECT DISTINCT item_id FROM ${table} ORDER BY item_id`).toArray();

			// The first step deletes the first two items in (base_hk, base_sk) order.
			expect(h.store.transactionSync(() => h.store.deleteItemsOfRoutingHash(P(promoted), 2))).toBe(false);
			expect([itemIdOf(promoted, "s1"), itemIdOf(promoted, "s2")]).toEqual([undefined, undefined]);
			const s3 = itemIdOf(promoted, "s3");
			expect(s3).toBeDefined();
			for (const table of ["change_positions", "target_retry", "target_index_keys"]) {
				expect(linked(table)).toEqual([{ item_id: s3 }, { item_id: otherId }].sort((x, y) => x.item_id! - y.item_id!));
			}

			expect(h.store.transactionSync(() => h.store.deleteItemsOfRoutingHash(P(promoted), 2))).toBe(true);
			expect(sql.exec(`SELECT item_id FROM latest_items`).toArray()).toEqual([{ item_id: otherId }]);
			for (const table of ["change_positions", "target_retry", "target_index_keys"]) {
				expect(linked(table)).toEqual([{ item_id: otherId }]);
			}
		});
	});
});
