import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { FokosDBMaterializationDO } from "./do-materialization.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import type { FokosSlice } from "../../sharding/repartition-slice.js";
import type { RouteKey } from "../../sharding/repartition-types.js";
import { MaterializationStore, type LatestImage } from "./materialization-store.js";
import { MaterializationMigrationHost, type MaterializationHostPage } from "./materialization-migration-host.js";
import type { PushSinkTargetDefinition } from "./targets.js";
import { ALL_ROWS, prefixOf } from "../../../test/storage-prefix.js";
import { collidingHashKeys } from "../../../test/hash-digits.js";
import { compareHashPrefixBuckets } from "../../sharding/hash-primitives.js";
import { rangeStorageInterval } from "../../sharding/repartition-slice.js";

/** The shard group of `prefixOf`. */
const STORE_GROUP = "fokos.p.store-test";

/** The storage prefix of a hash key, as the partition stores it. */
const P = (hk: KeyBytes) => prefixOf(hk);

const kb = (value: string) => KeyCodec.encode(value);
const SOURCE_ID = "aa".repeat(32);
const GRANDPARENT_ID = "cc".repeat(32);
const BUDGET = { pageBytes: 1024 * 1024, pageRows: 2, scanRows: 100 };

const targets: PushSinkTargetDefinition[] = ["t1", "t2"].map((targetId) => ({
	kind: "push_sink",
	targetId,
	binding: "SINK",
	method: "push",
}));

function image(hk: string, sk: string): LatestImage {
	return { opIndex: 0, hashKey: kb(hk), sortKey: kb(sk), revision: 1, deleted: false, data: `${hk}/${sk}`, kind: "text", ttlAt: null };
}

async function inPartition<T>(name: string, fn: (store: MaterializationStore, sql: SqlStorage) => T): Promise<T> {
	const stub = env.MATERIALIZATION_DO.getByName(`${name}.${crypto.randomUUID()}`);
	return await runInDurableObject(stub, (_instance: FokosDBMaterializationDO, state: DurableObjectState) =>
		fn(new MaterializationStore(state.storage), state.storage.sql),
	);
}

describe("MaterializationMigrationHost", () => {
	it("moves a promoted key with its positions, its retry rows, the snapshot cursors, and the fences", async () => {
		const slice: FokosSlice = { kind: "promoted_hash_prefix_bucket", hashPrefixBucket: P(kb("hot")) };
		const inSlice = (key: RouteKey) => KeyCodec.compare(key.hashKey, kb("hot")) === 0;

		const pages = await inPartition("mat-source", (store, sql) => {
			store.storeTargets(targets);
			const ingest = (hk: string, sk: string) => {
				const positions = store.targetRows().map((row) => ({ cursor: row.cursorSeq, read: row.cursorSeq }));
				store.putLatestImage(P(image(hk, sk).hashKey), image(hk, sk), 1_000, store.storedImage(P(kb(hk)), kb(hk), kb(sk)), positions);
			};
			// Positions: hot/s1 = 1, cold = 2, hot/s2 = 3, hot/s3 = 4.
			ingest("hot", "s1");
			ingest("cold", "s");
			ingest("hot", "s2");
			ingest("hot", "s3");
			store.updateTarget("t1", 3, false);
			store.updateTarget("t2", 1, false);
			store.recordTargetRetry("t1", store.storedImage(P(kb("hot")), kb("hot"), kb("s1"))!.itemId, 1_000, 1_000, 10_000, "down");
			store.raiseProducerEpoch("p1", 4);
			store.raiseProducerEpoch("p2", 1);
			store.raiseProducerEpoch("p3", 2);
			sql.exec(`INSERT INTO ancestor_fences (target_id, ancestor_id, min_epoch, until_id) VALUES ('t1', ?, 5, 9)`, GRANDPARENT_ID);

			// The cutover raises the epochs and stores the snapshot. Then the claims of the source skip
			// the moved items, and the cursors move past them.
			store.raiseAllTargetEpochs();
			store.storeCursorSnapshot(P(kb("hot")));
			store.updateTarget("t1", 4, false);
			store.updateTarget("t2", 4, false);

			const host = new MaterializationMigrationHost({ store, selfId: () => SOURCE_ID, storagePrefix: P });
			const out: MaterializationHostPage[] = [];
			let cursor: unknown = null;
			for (let i = 0; i < 20; i++) {
				const { page, nextCursor } = host.buildPage(cursor, slice, inSlice, BUDGET, ALL_ROWS);
				host.validatePage(cursor, page, nextCursor, inSlice);
				out.push(page);
				if (nextCursor === null) {
					return out;
				}
				cursor = nextCursor;
			}
			throw new Error("the stream did not end");
		});
		expect(pages.map((p) => p.stream)).toEqual(["items", "items", "producer_fences", "producer_fences", "state"]);
		const items = pages.flatMap((p) => (p.stream === "items" ? p.items : []));
		// A position at or below the lowest snapshot cursor (1) is done for each target.
		expect(items.map((item) => [KeyCodec.decode(item.sk), item.positions])).toEqual([
			["s1", []],
			["s2", [{ seq: 3, prev_seq: null }]],
			["s3", [{ seq: 4, prev_seq: null }]],
		]);

		const child = await inPartition("mat-child", (store, sql) => {
			const host = new MaterializationMigrationHost({ store, selfId: () => "bb".repeat(32), storagePrefix: P });
			for (const page of pages) {
				store.transactionSync(() => host.applyPage(page, slice));
			}
			// A retried page changes nothing.
			store.transactionSync(() => host.applyPage(pages[0], slice));
			const rows = (query: string) => sql.exec(query).toArray();
			return {
				items: rows(`SELECT item_id, base_sk, newest_seq FROM latest_items ORDER BY base_sk`).map((r) => ({
					...r,
					base_sk: KeyCodec.decode(new Uint8Array(r.base_sk as ArrayBuffer) as KeyBytes),
				})),
				positions: rows(`SELECT seq, prev_seq FROM change_positions ORDER BY seq`),
				retries: rows(`SELECT target_id, kind, is_inherited, last_error FROM target_retry`),
				targets: rows(`SELECT target_id, cursor_seq, epoch, is_batch_open FROM materialization_targets ORDER BY target_id`),
				fences: rows(`SELECT target_id, ancestor_id, min_epoch, until_id FROM ancestor_fences ORDER BY target_id, ancestor_id`),
				producers: rows(`SELECT producer_id, epoch FROM producer_fences ORDER BY producer_id`),
				storedTargets: store.storedTargets(),
				keyBytes: store.keySizes.bytesOf(P(kb("hot"))),
			};
		});
		expect(child.items).toEqual(
			items.map((item) => ({ item_id: item.item_id, base_sk: KeyCodec.decode(item.sk), newest_seq: item.newest_seq })),
		);
		expect(child.positions).toEqual([
			{ seq: 3, prev_seq: null },
			{ seq: 4, prev_seq: null },
		]);
		expect(child.retries).toEqual([{ target_id: "t1", kind: "retry", is_inherited: 1, last_error: "down" }]);
		// The child starts from the snapshot cursors, with the epoch 1.
		expect(child.targets).toEqual([
			{ target_id: "t1", cursor_seq: 3, epoch: 1, is_batch_open: 0 },
			{ target_id: "t2", cursor_seq: 1, epoch: 1, is_batch_open: 0 },
		]);
		// Each target gets the fences of the source and the source at its raised epoch, up to the highest copied position.
		expect(child.fences).toEqual([
			{ target_id: "t1", ancestor_id: SOURCE_ID, min_epoch: 2, until_id: 4 },
			{ target_id: "t1", ancestor_id: GRANDPARENT_ID, min_epoch: 5, until_id: 4 },
			{ target_id: "t2", ancestor_id: SOURCE_ID, min_epoch: 2, until_id: 4 },
		]);
		expect(child.producers).toEqual([
			{ producer_id: "p1", epoch: 4 },
			{ producer_id: "p2", epoch: 1 },
			{ producer_id: "p3", epoch: 2 },
		]);
		expect(child.storedTargets).toEqual(targets);
		expect(child.keyBytes).toBe(items.reduce((sum, item) => sum + item.est_row_bytes, 0));
	});

	it("moves every hash key of the promoted hashPrefixBucket, and the source cleanup deletes them all", async () => {
		const [a, b] = collidingHashKeys(STORE_GROUP).map((raw) => KeyCodec.encode(raw));
		const bucket = prefixOf(a);
		expect(prefixOf(b)).toEqual(bucket);
		const slice: FokosSlice = { kind: "promoted_hash_prefix_bucket", hashPrefixBucket: bucket };
		const inSlice = (key: RouteKey) => compareHashPrefixBuckets(prefixOf(key.hashKey), bucket) === 0;
		const bytesImage = (hashKey: KeyBytes, sk: string): LatestImage => ({ ...image("x", sk), hashKey });

		const source = await inPartition("mat-source", (store) => {
			store.storeTargets(targets);
			for (const [hashKey, sk] of [
				[a, "s1"],
				[b, "s1"],
				[b, "s2"],
				[kb("cold"), "s"],
			] as const) {
				const img = bytesImage(hashKey, sk);
				store.putLatestImage(prefixOf(hashKey), img, 1_000, store.storedImage(prefixOf(hashKey), hashKey, img.sortKey), []);
			}
			store.storeCursorSnapshot(bucket);
			const host = new MaterializationMigrationHost({ store, selfId: () => SOURCE_ID, storagePrefix: P });
			const pages: MaterializationHostPage[] = [];
			let cursor: unknown = null;
			do {
				const built = host.buildPage(cursor, slice, inSlice, BUDGET, rangeStorageInterval(bucket, null, null));
				pages.push(built.page);
				cursor = built.nextCursor;
			} while (cursor !== null);
			const bucketBytes = store.keySizes.bytesOf(bucket);
			while (!store.deleteItemsOfHashPrefixBucket(bucket, 1)) {
				// One bounded step at a time, as the cleanup runs.
			}
			const has = (hashKey: KeyBytes, sk: string) => store.storedImage(prefixOf(hashKey), hashKey, kb(sk)) !== undefined;
			return { pages, bucketBytes, left: [has(a, "s1"), has(b, "s1"), has(b, "s2"), has(kb("cold"), "s")] };
		});
		const moved = source.pages.flatMap((p) => (p.stream === "items" ? p.items : []));
		// The pages are in storage order: by hash key, then by sort key.
		const expected: Array<[KeyBytes, string]> = [
			[a, "s1"],
			[b, "s1"],
			[b, "s2"],
		];
		expected.sort((x, y) => KeyCodec.compare(x[0], y[0]) || x[1].localeCompare(y[1]));
		expect(moved.map((item) => [item.hk, KeyCodec.decode(item.sk)])).toEqual(expected);
		expect(source.left).toEqual([false, false, false, true]);

		const childBytes = await inPartition("mat-child", (store) => {
			const host = new MaterializationMigrationHost({ store, selfId: () => "bb".repeat(32), storagePrefix: P });
			for (const page of source.pages) {
				store.transactionSync(() => host.applyPage(page, slice));
			}
			// A retried page adds no bytes.
			store.transactionSync(() => host.applyPage(source.pages[0], slice));
			return store.keySizes.bytesOf(bucket);
		});
		// The child has one total for the two hash keys, the same as the source.
		expect(childBytes).toBe(source.bucketBytes);
	});

	it("gives no fence to a target that has no inherited work", async () => {
		const slice: FokosSlice = { kind: "promoted_hash_prefix_bucket", hashPrefixBucket: P(kb("hot")) };
		const inSlice = (key: RouteKey) => KeyCodec.compare(key.hashKey, kb("hot")) === 0;
		const pages = await inPartition("mat-source", (store) => {
			store.storeTargets(targets);
			const positions = store.targetRows().map((row) => ({ cursor: row.cursorSeq, read: row.cursorSeq }));
			store.putLatestImage(P(image("hot", "s1").hashKey), image("hot", "s1"), 1_000, undefined, positions);
			// t1 read the item, and t2 did not.
			store.updateTarget("t1", 1, false);
			store.raiseAllTargetEpochs();
			store.storeCursorSnapshot(P(kb("hot")));
			const host = new MaterializationMigrationHost({ store, selfId: () => SOURCE_ID, storagePrefix: P });
			const out: MaterializationHostPage[] = [];
			for (let cursor: unknown = null; ; ) {
				const { page, nextCursor } = host.buildPage(cursor, slice, inSlice, BUDGET, ALL_ROWS);
				out.push(page);
				if (nextCursor === null) {
					return out;
				}
				cursor = nextCursor;
			}
		});
		const fences = await inPartition("mat-child", (store, sql) => {
			const host = new MaterializationMigrationHost({ store, selfId: () => "bb".repeat(32), storagePrefix: P });
			for (const page of pages) {
				store.transactionSync(() => host.applyPage(page, slice));
			}
			return sql.exec(`SELECT target_id, ancestor_id, until_id FROM ancestor_fences`).toArray();
		});
		expect(fences).toEqual([{ target_id: "t2", ancestor_id: SOURCE_ID, until_id: 1 }]);
	});

	it("moves the state of a target whose targetId is __proto__", async () => {
		const protoTargets: PushSinkTargetDefinition[] = [{ kind: "push_sink", targetId: "__proto__", binding: "SINK", method: "push" }];
		const slice: FokosSlice = { kind: "promoted_hash_prefix_bucket", hashPrefixBucket: P(kb("hot")) };
		const inSlice = (key: RouteKey) => KeyCodec.compare(key.hashKey, kb("hot")) === 0;
		const pages = await inPartition("mat-source", (store) => {
			store.storeTargets(protoTargets);
			const positions = store.targetRows().map((row) => ({ cursor: row.cursorSeq, read: row.cursorSeq }));
			store.putLatestImage(P(image("hot", "s1").hashKey), image("hot", "s1"), 1_000, undefined, positions);
			store.raiseAllTargetEpochs();
			store.storeCursorSnapshot(P(kb("hot")));
			const host = new MaterializationMigrationHost({ store, selfId: () => SOURCE_ID, storagePrefix: P });
			const out: MaterializationHostPage[] = [];
			for (let cursor: unknown = null; ; ) {
				const { page, nextCursor } = host.buildPage(cursor, slice, inSlice, BUDGET, ALL_ROWS);
				out.push(page);
				if (nextCursor === null) {
					return out;
				}
				cursor = nextCursor;
			}
		});
		const child = await inPartition("mat-child", (store, sql) => {
			const host = new MaterializationMigrationHost({ store, selfId: () => "bb".repeat(32), storagePrefix: P });
			for (const page of pages) {
				store.transactionSync(() => host.applyPage(page, slice));
			}
			return {
				positions: sql.exec(`SELECT seq, prev_seq FROM change_positions`).toArray(),
				targets: sql.exec(`SELECT target_id, cursor_seq, epoch FROM materialization_targets`).toArray(),
				fences: sql.exec(`SELECT target_id, ancestor_id, min_epoch, until_id FROM ancestor_fences`).toArray(),
			};
		});
		expect(child.positions).toEqual([{ seq: 1, prev_seq: null }]);
		expect(child.targets).toEqual([{ target_id: "__proto__", cursor_seq: 0, epoch: 1 }]);
		expect(child.fences).toEqual([{ target_id: "__proto__", ancestor_id: SOURCE_ID, min_epoch: 2, until_id: 1 }]);
	});

	it("moves the GSI key sets of each item with the item", async () => {
		const slice: FokosSlice = { kind: "promoted_hash_prefix_bucket", hashPrefixBucket: P(kb("hot")) };
		const inSlice = (key: RouteKey) => KeyCodec.compare(key.hashKey, kb("hot")) === 0;
		const keys = [
			{ hk: kb("A"), sk: KeyCodec.encodeOptional(undefined) },
			{ hk: kb("B"), sk: kb("b") },
		];
		const pages = await inPartition("mat-keys-source", (store) => {
			store.storeTargets(targets);
			store.putLatestImage(P(image("hot", "s").hashKey), image("hot", "s"), 1_000, undefined, []);
			for (const key of keys) {
				store.addIndexKey("t1", store.storedImage(P(kb("hot")), kb("hot"), kb("s"))!.itemId, key);
			}
			store.storeCursorSnapshot(P(kb("hot")));
			const host = new MaterializationMigrationHost({ store, selfId: () => SOURCE_ID, storagePrefix: P });
			const out: MaterializationHostPage[] = [];
			let cursor: unknown = null;
			do {
				const { page, nextCursor } = host.buildPage(cursor, slice, inSlice, BUDGET, ALL_ROWS);
				out.push(page);
				cursor = nextCursor;
			} while (cursor !== null);
			return out;
		});
		const copied = await inPartition("mat-keys-target", (store) => {
			const host = new MaterializationMigrationHost({ store, selfId: () => "bb".repeat(32), storagePrefix: P });
			for (const page of pages) {
				host.applyPage(page, slice);
			}
			return store.indexKeys("t1", store.storedImage(P(kb("hot")), kb("hot"), kb("s"))!.itemId);
		});
		expect(copied).toEqual(keys);
	});
});
