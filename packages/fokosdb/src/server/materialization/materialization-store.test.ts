import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { FokosDBMaterializationDO } from "./do-materialization.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import { MaterializationStore, type LatestImage, type TargetPositions } from "./materialization-store.js";
import { routingHashFor } from "../../../test/routing-hash.js";

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
		const store = new MaterializationStore(state.storage);
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

describe("MaterializationStore — row size estimate", () => {
	it("counts the data, the keys two times, and the fixed part for a live image, and the keys three times for a tombstone", async () => {
		await withStore((h) => {
			// The keys and the 8-byte routingHash.
			const keys = 8 + kb("x").byteLength + NO_SK.byteLength;
			h.ingest("x", 1, []);
			expect(h.store.storedImage(P(kb("x")), kb("x"), NO_SK)!.estRowBytes).toBe("v1".length + 2 * keys + 70);
			expect(h.store.keySizes.bytesOf(P(kb("x")))).toBe("v1".length + 2 * keys + 70);
			h.ingest("x", 2, [], true);
			expect(h.store.storedImage(P(kb("x")), kb("x"), NO_SK)!.estRowBytes).toBe(3 * keys + 70);
			expect(h.store.keySizes.bytesOf(P(kb("x")))).toBe(3 * keys + 70);
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
					est_row_bytes: 1,
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
				est_row_bytes: 1,
				data: "d",
				positions: [],
				retries: [],
			});
			expect(h.store.insertMigratedItem(copy("local"))).toBe(false);
			expect(() => h.store.insertMigratedItem(copy("other"))).toThrow(/UNIQUE constraint failed: latest_items\.item_id/);
		});
	});
});

/**
 * A store whose storage records the text and the parameters of each `sql.exec` call. Each call runs
 * on the real storage without change. `plans` returns the query plan of each recorded statement on
 * `latest_items` since its last call, and clears the record.
 */
function planRecorder(storage: DurableObjectStorage): { store: MaterializationStore; plans: () => string[] } {
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
	return { store, plans };
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
