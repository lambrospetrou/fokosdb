import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { FokosDBPartitionDO } from "../../server/do-partition.js";
import { testPartitionStub } from "../../../test/stub-helpers.js";
import { projectionTree, updateTree } from "../expression/test-fixtures.js";
import type { UpdateExpression } from "../expression/types.js";
import { type KeyBytes, KeyCodec } from "../../sharding/key-codec.js";
import invariant from "../invariant.js";
import {
	estimateItemBytes,
	estimateProjectedRowBytes,
	itemsPageStatement,
	outboxPageStatement,
	PartitionStore,
	pendingTxPageStatement,
	queryScanStatement,
	type ItemLinkId,
	type KeyRange,
	type MigratedItem,
	type PendingTransactionCursor,
	type PendingTxItem,
	type StorageCursor,
	type SqlMetrics,
	type StoredItem,
} from "./partition-store.js";
import type { ProjectedWireRow } from "../expression/projection.js";
import { planRangeBoundaries } from "./split-planner.js";
import { estimate, ITEMS_MODEL, maxItemEstimate } from "./item-size.js";
import { TX_ORDER_TS_UNITS_PER_MS } from "../transaction-limits.js";
import { MAX_ITEM_BYTES } from "../transaction-limits.js";
import { fokosErrorWith } from "../../../test/errors-matchers.js";
import { routingHashOf } from "../../sharding/hash-primitives.js";
import { rangeStorageInterval } from "../../sharding/repartition-slice.js";

const kb = (s: string | Uint8Array) => KeyCodec.encode(s);
/** The routingHash of a hash key. The store treats it as opaque bytes, so the shard group of the helper does not matter. */
const STORE_TEST_GROUP = "fokos.p.store-test";
const P = (hk: KeyBytes) => routingHashOf(hk, STORE_TEST_GROUP);
/**
 * The storage interval of the sort keys `[start, end)` of one hash key: a composite interval of its
 * routingHash. With no end, it stops at `hk || 0x00`, the first key above every key of `hk`.
 */
function keyInterval(hk: KeyBytes, start: KeyBytes | null, end: KeyBytes | null): KeyRange {
	const next = KeyCodec.asKeyBytes(Uint8Array.from([...hk, 0]));
	return rangeStorageInterval(
		P(hk),
		{ hk, sk: start ?? KeyCodec.encodeOptional(undefined) },
		{ hk: end === null ? next : hk, sk: end ?? KeyCodec.encodeOptional(undefined) },
	);
}

/** Every row of the table. */
const ALL: KeyRange = { start: { routingHash: new Uint8Array() }, end: null };
const keyLabel = (key: KeyBytes): string => {
	const decoded = KeyCodec.decode(key);
	return typeof decoded === "string" ? decoded : `b64:${decoded.toBase64({ alphabet: "base64url" })}`;
};

// Only the store creates link ids; a test stands in for the parent partition that read one.
const linkId = (n: number) => n as ItemLinkId;
const ownsAll = () => true;

// Mirrors items.item_bytes: octet_length(data)+octet_length(hk)+octet_length(sk). octet_length is
// UTF-8 bytes for text and byteLength for blobs; keys are already canonical bytes. item_bytes is a
// plain column written by several statements, so these tests are what keeps the formula from drifting.
function expectedRowBytes(data: string | Uint8Array, hk: KeyBytes, sk: KeyBytes): number {
	const dataBytes = typeof data === "string" ? new TextEncoder().encode(data).length : data.byteLength;
	return dataBytes + hk.byteLength + sk.byteLength;
}

// Runs `fn` against a PartitionStore over REAL Durable Object storage (vitest-pool-workers).
// The FokosDBPartitionDO constructor has already run the schema migrations by the time the callback runs;
// constructing a second PartitionStore over the same storage is safe (migrations are idempotent).
// The floor gives the bucket threshold m = 1, so each routingHash with an item has a bucket.
async function withStore(fn: (store: PartitionStore, state: DurableObjectState) => void | Promise<void>): Promise<void> {
	const stub = testPartitionStub(`store-test.${crypto.randomUUID()}`);
	await runInDurableObject(stub, async (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
		await fn(new PartitionStore(state.storage, { promotionFloor: () => maxItemEstimate(ITEMS_MODEL) }), state);
	});
}

// One candidate of a scan as the consumer received it, with its payload decoded when the selection
// materializes one.
type ScanRow = { sk: KeyBytes; itemBytes: number; matched: boolean; payload: StoredItem | ProjectedWireRow | null };

// Runs a query scan and records every candidate; `take` bounds how many candidates the consumer
// accepts before it stops the scan. Returns the rows and the metrics the scan reported.
function scanRows(
	store: PartitionStore,
	opts: Parameters<PartitionStore["scanQueryPage"]>[0],
	take = Number.POSITIVE_INFINITY,
): { rows: ScanRow[]; metrics: SqlMetrics } {
	const rows: ScanRow[] = [];
	const metrics = store.scanQueryPage(opts, (sk, itemBytes, matched, decodePayload) => {
		rows.push({ sk, itemBytes, matched, payload: matched && opts.select === "projection" ? decodePayload() : null });
		return rows.length < take;
	});
	return { rows, metrics };
}

type BucketRow = {
	item_count: number;
	item_bytes: number;
	key_bytes: number;
	indexed_item_count: number;
	indexed_key_bytes: number;
	overflow_bytes: number;
};

/** The facts of the bucket of the routingHash of `hk`, or undefined when it has none. */
function bucketRow(state: DurableObjectState, hk: string): BucketRow | undefined {
	return state.storage.sql
		.exec<BucketRow>(
			`SELECT item_count, item_bytes, key_bytes, indexed_item_count, indexed_key_bytes, overflow_bytes FROM item_stats WHERE routing_hash = ?`,
			P(kb(hk)),
		)
		.toArray()[0];
}

/** The sum of the `item_bytes` of the bucket of `hk`. */
function bucketBytes(state: DurableObjectState, hk: string): number | undefined {
	return bucketRow(state, hk)?.item_bytes;
}

/** The estimate of the bucket of `hk` with the `items` coefficients, rounded up as the write path reports it. */
function bucketEst(state: DurableObjectState, hk: string): number | undefined {
	const row = bucketRow(state, hk);
	return (
		row &&
		Math.ceil(
			estimate(ITEMS_MODEL, {
				itemCount: row.item_count,
				itemBytes: row.item_bytes,
				keyBytes: row.key_bytes,
				indexedItemCount: row.indexed_item_count,
				indexedKeyBytes: row.indexed_key_bytes,
				overflowBytes: row.overflow_bytes,
			}),
		)
	);
}

describe("PartitionStore - items", () => {
	it("upsertItem inserts with version 1 and increments on every overwrite", async () => {
		await withStore((store) => {
			const first = store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("sk"),
				data: "v1",
				kind: "text",
				ttlAt: null,
				txOrderTs: 1,
			});
			expect(first.version).toBe(1);
			const second = store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("sk"),
				data: "v2",
				kind: "text",
				ttlAt: null,
				txOrderTs: 2,
			});
			expect(second.version).toBe(2);
			const third = store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("sk"),
				data: "v3",
				kind: "text",
				ttlAt: null,
				txOrderTs: 3,
			});
			expect(third.version).toBe(3);
		});
	});

	it("upsertItem never lowers the item timestamps, but still applies the write", async () => {
		await withStore((store) => {
			// As if committed by a transaction whose coordinator clock ran ahead of this partition's.
			store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("sk"),
				data: "from-tx",
				kind: "text",
				ttlAt: null,
				txOrderTs: 5_000,
			});

			// A non-transactional put stamps the partition's own (lower) clock.
			const second = store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("sk"),
				data: "from-put",
				kind: "text",
				ttlAt: null,
				txOrderTs: 1_000,
			});

			// The write lands in full — only the timestamps are held at the high-water mark.
			expect(second.version).toBe(2);
			expect(store.getItem(P(kb("hk")), kb("hk"), kb("sk")).row).toMatchObject({
				data: "from-put",
				last_read_ts: 5_000,
				last_write_ts: 5_000,
			});

			// A later timestamp still moves them forward.
			store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("sk"),
				data: "newer",
				kind: "text",
				ttlAt: null,
				txOrderTs: 9_000,
			});
			expect(store.getItem(P(kb("hk")), kb("hk"), kb("sk")).row).toMatchObject({ last_read_ts: 9_000, last_write_ts: 9_000 });
		});
	});

	it("getItem returns converted data, ttl, version, and item timestamps", async () => {
		await withStore((store) => {
			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("s"), data: "hello", kind: "text", ttlAt: 1234, txOrderTs: 42 });
			const str = store.getItem(P(kb("hk")), kb("hk"), kb("s"));
			expect(str.row).toEqual({ data: "hello", kind: "text", ttl_epoch_utc_seconds: 1234, v: 1, last_read_ts: 42, last_write_ts: 42 });
			expect(str.rowsRead).toBe(1);

			const bin = new Uint8Array([1, 2, 3]);
			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("b"), data: bin, kind: "bytes", ttlAt: null, txOrderTs: 0 });
			const got = store.getItem(P(kb("hk")), kb("hk"), kb("b"));
			expect(got.row?.data).toBeInstanceOf(Uint8Array);
			expect(got.row?.data).toEqual(bin);

			expect(store.getItem(P(kb("hk")), kb("hk"), kb("missing")).row).toBeUndefined();
		});
	});

	it("measureItemBytesWithStamp returns the measured size and the item stamps, and reads at most one row", async () => {
		await withStore((store) => {
			const value = JSON.stringify({ a: 1 });
			const absent = store.measureItemBytesWithStamp({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("s"), data: value, kind: "json" });
			expect(absent).toMatchObject({ itemPresent: false, lastReadTs: null, lastWriteTs: null });
			expect(absent.itemBytes).toBe(store.measureItemBytes({ hk: kb("hk"), sk: kb("s"), data: value, kind: "json" }));
			expect(absent.rowsRead).toBeLessThanOrEqual(1);

			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("s"), data: "old", kind: "text", ttlAt: null, txOrderTs: 42 });
			store.bumpItemReadTs(P(kb("hk")), kb("hk"), kb("s"), 50);
			const present = store.measureItemBytesWithStamp({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("s"), data: value, kind: "json" });
			expect(present).toMatchObject({ itemBytes: absent.itemBytes, itemPresent: true, lastReadTs: 50, lastWriteTs: 42 });
			expect(present.rowsRead).toBe(1);
		});
	});

	it("getItemProjected returns the wire cells, the version, and the ttl of one row", async () => {
		await withStore((store) => {
			const doc = { n: 7, s: "x", none: null, k: [1, 2] };
			store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("j"),
				data: JSON.stringify(doc),
				kind: "json",
				ttlAt: 1234,
				txOrderTs: 0,
			});
			const plan = projectionTree([
				{ expr: { ref: "data", path: "$.n" } },
				{ expr: { ref: "data", path: "$.absent" }, as: "missing" },
				{ expr: { ref: "data", path: "$.none" } },
				{ expr: { ref: "data", path: "$.k" }, as: "k" },
				{ expr: { ref: "v" }, as: "ver" },
			]);

			const res = store.getItemProjected(plan, P(kb("hk")), kb("hk"), kb("j"));
			expect(res.rowsRead).toBe(1);
			expect(res.row).toEqual({
				// Cells are positional in plan order: number, missing, JSON null, array as JSON text, version.
				projected: [7, undefined, null, { json: "[1,2]" }, 1],
				version: 1,
				ttlAt: 1234,
			});

			const bin = new Uint8Array([9, 8]);
			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("b"), data: bin, kind: "bytes", ttlAt: null, txOrderTs: 0 });
			const binPlan = projectionTree([{ expr: { ref: "data" } }]);
			expect(store.getItemProjected(binPlan, P(kb("hk")), kb("hk"), kb("b")).row?.projected).toEqual([bin]);

			expect(store.getItemProjected(plan, P(kb("hk")), kb("hk"), kb("missing")).row).toBeUndefined();
		});
	});

	it("getItemImage returns imageBytes equal to byte length of data across data kinds and non-ASCII characters", async () => {
		await withStore((store) => {
			const unicodeText = "hello € and Ω";
			const expectedTextBytes = new TextEncoder().encode(unicodeText).byteLength;
			store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("text"),
				data: unicodeText,
				kind: "text",
				ttlAt: 1234,
				txOrderTs: 1,
			});
			const textRes = store.getItemImage(P(kb("hk")), kb("hk"), kb("text"));
			expect(textRes.row).toEqual({
				data: unicodeText,
				kind: "text",
				version: 1,
				ttlAt: 1234,
				imageBytes: expectedTextBytes,
			});
			expect(textRes.rowsRead).toBe(1);

			const bin = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);
			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("bytes"), data: bin, kind: "bytes", ttlAt: null, txOrderTs: 1 });
			const bytesRes = store.getItemImage(P(kb("hk")), kb("hk"), kb("bytes"));
			expect(bytesRes.row).toEqual({
				data: bin,
				kind: "bytes",
				version: 1,
				imageBytes: 4,
			});

			const jsonText = JSON.stringify({ key: "value", num: 42 });
			const expectedJsonBytes = new TextEncoder().encode(jsonText).byteLength;
			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("json"), data: jsonText, kind: "json", ttlAt: 5678, txOrderTs: 1 });
			const jsonRes = store.getItemImage(P(kb("hk")), kb("hk"), kb("json"));
			expect(jsonRes.row).toEqual({
				data: jsonText,
				kind: "json",
				version: 1,
				ttlAt: 5678,
				imageBytes: expectedJsonBytes,
			});

			expect(store.getItemImage(P(kb("hk")), kb("hk"), kb("absent")).row).toBeUndefined();
		});
	});

	it("maintains the bucket facts across put, overwrite, and delete", async () => {
		await withStore((store, state) => {
			const est1 = expectedRowBytes("aaaa", kb("hk"), kb("s1"));
			const r1 = store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("s1"),
				data: "aaaa",
				kind: "text",
				ttlAt: null,
				txOrderTs: 1,
			});
			expect(bucketBytes(state, "hk")).toBe(est1);
			expect(r1.keyEstBytes).toBe(bucketEst(state, "hk"));

			// Second sort key accumulates on the same hash key.
			const est2 = expectedRowBytes("bb", kb("hk"), kb("s2"));
			const r2 = store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("s2"),
				data: "bb",
				kind: "text",
				ttlAt: null,
				txOrderTs: 2,
			});
			expect(bucketBytes(state, "hk")).toBe(est1 + est2);
			expect(bucketRow(state, "hk")?.item_count).toBe(2);
			expect(r2.keyEstBytes).toBe(bucketEst(state, "hk"));

			// Overwrite replaces the old row's contribution, not adds to it.
			const est1b = expectedRowBytes("aaaaaaaa", kb("hk"), kb("s1"));
			const r3 = store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("s1"),
				data: "aaaaaaaa",
				kind: "text",
				ttlAt: null,
				txOrderTs: 3,
			});
			expect(bucketBytes(state, "hk")).toBe(est1b + est2);
			expect(bucketRow(state, "hk")?.item_count).toBe(2);
			expect(r3.keyEstBytes).toBe(bucketEst(state, "hk"));

			// Delete removes its contribution.
			const del = store.deleteItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("s1"), txOrderTs: 10 });
			expect(del.deleted).toBe(true);
			expect(bucketBytes(state, "hk")).toBe(est2);

			// Deleting the last item of the key deletes its estimate row.
			store.deleteItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("s2"), txOrderTs: 11 });
			expect(bucketBytes(state, "hk")).toBeUndefined();

			// A new write of the key starts a new estimate.
			const r4 = store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("s3"),
				data: "c",
				kind: "text",
				ttlAt: null,
				txOrderTs: 12,
			});
			expect(bucketBytes(state, "hk")).toBe(expectedRowBytes("c", kb("hk"), kb("s3")));
			expect(r4.keyEstBytes).toBe(bucketEst(state, "hk"));
		});
	});

	it("keeps no bucket for a routingHash that has no items", async () => {
		await withStore((store, state) => {
			const estimateRows = () => state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM item_stats`).one().n;
			for (let i = 0; i < 20; i++) {
				store.upsertItem({ routingHash: P(kb(`k${i}`)), hk: kb(`k${i}`), sk: kb("s"), data: "d", kind: "text", ttlAt: null, txOrderTs: 1 });
			}
			for (let i = 0; i < 10; i++) {
				store.deleteItem({ routingHash: P(kb(`k${i}`)), hk: kb(`k${i}`), sk: kb("s"), txOrderTs: 2 });
			}
			// The transactional delete form, also for a row that is already gone.
			for (let i = 10; i < 20; i++) {
				store.deleteItem({ routingHash: P(kb(`k${i}`)), hk: kb(`k${i}`), sk: kb("s"), txOrderTs: 3, bumpTxOrderTsAlways: true });
				store.deleteItem({ routingHash: P(kb(`k${i}`)), hk: kb(`k${i}`), sk: kb("s"), txOrderTs: 4, bumpTxOrderTsAlways: true });
			}
			expect(estimateRows()).toBe(0);
		});
	});

	it("queryItemsPage pages in (routingHash, hk, sk) order and resumes strictly after the cursor", async () => {
		await withStore((store) => {
			// The first hash key in storage order has two rows, and the other has one.
			const [first, second] =
				KeyCodec.compare(KeyCodec.asKeyBytes(P(kb("a"))), KeyCodec.asKeyBytes(P(kb("b")))) < 0 ? ["a", "b"] : ["b", "a"];
			for (const [hk, sk] of [
				[first, "1"],
				[first, "2"],
				[second, "1"],
			] as const) {
				store.upsertItem({ routingHash: P(kb(hk)), hk: kb(hk), sk: kb(sk), data: "d", kind: "text", ttlAt: null, txOrderTs: 1 });
			}
			const page1 = [...store.queryItemsPage(null, 2, ALL)];
			expect(page1.map((r) => [KeyCodec.decode(r.hk), KeyCodec.decode(r.sk)])).toEqual([
				[first, "1"],
				[first, "2"],
			]);
			expect(page1.map((r) => r.routingHash)).toEqual([P(kb(first)), P(kb(first))]);
			const page2 = [...store.queryItemsPage({ routingHash: P(kb(first)), hk: kb(first), sk: kb("2") }, 2, ALL)];
			expect(page2.map((r) => [KeyCodec.decode(r.hk), KeyCodec.decode(r.sk)])).toEqual([[second, "1"]]);
		});
	});

	// Keys with no sort key store the empty blob. Paging must step across the empty/non-empty boundary
	// without skipping or repeating a row — the failure mode of a mis-written keyset cursor.
	it("queryItemsPage pages across keys that have no sort key", async () => {
		await withStore((store) => {
			const EMPTY = KeyCodec.encodeOptional(undefined);
			const keys: [KeyBytes, KeyBytes][] = [
				[kb("a"), EMPTY],
				[kb("a"), kb("m")],
				[kb("b"), EMPTY],
				[kb("c"), kb("q")],
			];
			for (const [hk, sk] of keys) {
				store.upsertItem({ routingHash: P(hk), hk, sk, data: "d", kind: "text", ttlAt: null, txOrderTs: 1 });
			}
			// Walk the whole table one row at a time, the way migration does.
			const seen: string[] = [];
			let cursor: StorageCursor | null = null;
			for (;;) {
				const page: MigratedItem[] = [...store.queryItemsPage(cursor, 1, ALL)];
				if (page.length === 0) {
					break;
				}
				const row = page[0];
				seen.push(`${keyLabel(row.hk)}/${keyLabel(row.sk)}`);
				cursor = { routingHash: row.routingHash, hk: row.hk, sk: row.sk };
			}
			expect([...seen].sort()).toEqual(["a/", "a/m", "b/", "c/q"]);
			// Each key keeps its rows together, with the empty sort key first.
			expect(seen.indexOf("a/m")).toBe(seen.indexOf("a/") + 1);
		});
	});

	it("stores json as JSONB, persists data_kind, and decodes to JSON text on read", async () => {
		await withStore((store, state) => {
			const jsonText = JSON.stringify({ a: 1, b: ["x", true, null] });
			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("j"), data: jsonText, kind: "json", ttlAt: null, txOrderTs: 1 });

			// getItem decodes JSONB → JSON text and surfaces the kind.
			const got = store.getItem(P(kb("hk")), kb("hk"), kb("j"));
			expect(got.row?.kind).toBe("json");
			expect(JSON.parse(got.row!.data as string)).toEqual({ a: 1, b: ["x", true, null] });

			// Physically stored as JSONB (a BLOB), not TEXT, and queryable by JSON path.
			const raw = state.storage.sql
				.exec<{
					kind: number;
					is_blob: number;
					a: number;
				}>(
					`SELECT data_kind AS kind, typeof(data) = 'blob' AS is_blob, jsonb_extract(data, '$.a') AS a FROM items WHERE hk = ? AND sk = ?`,
					kb("hk"),
					kb("j"),
				)
				.toArray()[0];
			expect(raw).toMatchObject({ kind: 2, is_blob: 1, a: 1 });
		});
	});

	it("item_bytes equals octet_length(data)+octet_length(hk)+octet_length(sk) per kind", async () => {
		await withStore((store, state) => {
			const readEst = (sk: string) =>
				state.storage.sql.exec<{ e: number }>(`SELECT item_bytes AS e FROM items WHERE hk = ? AND sk = ?`, kb("hk"), kb(sk)).toArray()[0]
					?.e;
			const octetLen = (sk: string, data: string | Uint8Array) =>
				(typeof data === "string" ? new TextEncoder().encode(data).length : data.byteLength) + kb("hk").byteLength + kb(sk).byteLength;

			const multibyte = "héllo—✓"; // multi-byte UTF-8: octet_length > .length
			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("t"), data: multibyte, kind: "text", ttlAt: null, txOrderTs: 1 });
			expect(readEst("t")).toBe(octetLen("t", multibyte));

			const bytes = new Uint8Array([1, 2, 3, 4, 5]);
			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("b"), data: bytes, kind: "bytes", ttlAt: null, txOrderTs: 1 });
			expect(readEst("b")).toBe(octetLen("b", bytes));

			// For json, the stored JSONB blob size is what octet_length(data) measures — read it from SQL
			// rather than the input text (the two differ).
			const jsonText = JSON.stringify({ hello: "world", n: 12345 });
			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("j"), data: jsonText, kind: "json", ttlAt: null, txOrderTs: 1 });
			const jsonBlobLen = state.storage.sql
				.exec<{ n: number }>(`SELECT octet_length(data) AS n FROM items WHERE hk = ? AND sk = ?`, kb("hk"), kb("j"))
				.toArray()[0].n;
			expect(readEst("j")).toBe(jsonBlobLen + kb("hk").byteLength + kb("j").byteLength);
		});
	});

	it("insertItemIfAbsent writes the same item_bytes as upsertItem", async () => {
		await withStore((store, state) => {
			const readEst = (sk: string) =>
				state.storage.sql.exec<{ e: number }>(`SELECT item_bytes AS e FROM items WHERE hk = ? AND sk = ?`, kb("hk"), kb(sk)).toArray()[0]
					?.e;

			// The migration writer carries the formula independently of upsertItem — both must agree.
			const jsonText = JSON.stringify({ hello: "world", n: 12345 });
			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("j"), data: jsonText, kind: "json", ttlAt: null, txOrderTs: 1 });
			const migrated = [...store.queryItemsPage(null, 10, ALL)][0];
			store.insertItemIfAbsent({ ...migrated, item_id: linkId(migrated.item_id + 1), sk: kb("j2") });
			// Same row under a longer sk: the only difference must be octet_length(sk).
			expect(readEst("j2")).toBe(readEst("j")! - kb("j").byteLength + kb("j2").byteLength);

			const bytes = new Uint8Array([1, 2, 3, 4, 5]);
			store.insertItemIfAbsent({
				item_id: linkId(100),
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("b"),
				data: bytes,
				kind: "bytes",
				ttl_epoch_utc_seconds: null,
				v: 1,
				last_read_ts: 1,
				last_write_ts: 1,
			});
			expect(readEst("b")).toBe(bytes.byteLength + kb("hk").byteLength + kb("b").byteLength);
		});
	});

	// The size reads use the primary key, which is in storage order and holds each column before
	// `data`. No covering index exists, so a plan that leaves the primary key reads the table twice.
	it("the size reads, the count, and the old-facts read search the primary key", async () => {
		await withStore((_store, state) => {
			const plan = (sql: string, ...params: unknown[]) =>
				state.storage.sql
					.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${sql}`, ...params)
					.toArray()
					.map((r) => r.detail)
					.join(" | ");
			const rh = P(kb("hk"));

			expect(
				plan(`SELECT item_bytes, ttl_epoch_utc_seconds FROM items WHERE routing_hash = ? AND hk = ? AND sk = ?`, rh, kb("hk"), kb("sk")),
			).toBe("SEARCH items USING PRIMARY KEY (routing_hash=? AND hk=? AND sk=?)");
			expect(
				plan(
					`SELECT hk, sk, item_bytes, ttl_epoch_utc_seconds IS NOT NULL AS indexed FROM items
					  WHERE routing_hash = ? AND (hk, sk) >= (?, ?) AND (hk, sk) < (?, ?) ORDER BY hk, sk`,
					rh,
					kb("a"),
					kb("a"),
					kb("z"),
					kb("z"),
				),
			).toBe("SEARCH items USING PRIMARY KEY (routing_hash=? AND (hk,sk)>(?,?) AND (hk,sk)<(?,?))");
			expect(
				plan(
					`SELECT COUNT(*) AS n FROM (SELECT 1 FROM items WHERE routing_hash = ? AND (hk, sk) >= (?, ?) LIMIT ?)`,
					rh,
					kb("a"),
					kb("a"),
					4,
				),
			).toContain("SEARCH items USING PRIMARY KEY (routing_hash=? AND (hk,sk)>(?,?))");
		});
	});

	it("scanQueryPage count rows carry the stored item_bytes and no item", async () => {
		await withStore((store, state) => {
			const hk = kb("hk");
			const jsonText = JSON.stringify({ a: 1 });
			store.upsertItem({ routingHash: P(hk), hk, sk: kb("t"), data: "hello", kind: "text", ttlAt: null, txOrderTs: 1 });
			store.upsertItem({ routingHash: P(hk), hk, sk: kb("b"), data: new Uint8Array([1, 2, 3]), kind: "bytes", ttlAt: null, txOrderTs: 1 });
			store.upsertItem({ routingHash: P(hk), hk, sk: kb("j"), data: jsonText, kind: "json", ttlAt: null, txOrderTs: 1 });

			const storedEst = (sk: string) =>
				state.storage.sql.exec<{ e: number }>(`SELECT item_bytes AS e FROM items WHERE hk = ? AND sk = ?`, hk, kb(sk)).toArray()[0]!.e;

			const { rows } = scanRows(store, {
				routingHash: P(hk),
				hk,
				lower: KeyCodec.encodeOptional(undefined),
				lowerInclusive: true,
				upper: null,
				upperInclusive: false,
				cursor: null,
				direction: "asc",
				limit: 100,
				select: "count",
				plan: null,
			});
			expect(rows.map((r) => KeyCodec.decode(r.sk))).toEqual(["b", "j", "t"]);
			for (const r of rows) {
				expect(r.payload).toBeNull();
				expect(r.matched).toBe(true);
				expect(r.itemBytes).toBe(storedEst(KeyCodec.decode(r.sk) as string));
			}
			expect(rows[0].itemBytes).toBe(expectedRowBytes(new Uint8Array([1, 2, 3]), hk, kb("b")));
			expect(rows[2].itemBytes).toBe(expectedRowBytes("hello", hk, kb("t")));
		});
	});

	it("scanQueryPage projection rows carry the item and the same item_bytes", async () => {
		await withStore((store) => {
			const hk = kb("hk");
			const jsonText = JSON.stringify({ a: 1 });
			store.upsertItem({ routingHash: P(hk), hk, sk: kb("t"), data: "hello", kind: "text", ttlAt: null, txOrderTs: 1 });
			store.upsertItem({ routingHash: P(hk), hk, sk: kb("b"), data: new Uint8Array([1, 2, 3]), kind: "bytes", ttlAt: null, txOrderTs: 1 });
			store.upsertItem({ routingHash: P(hk), hk, sk: kb("j"), data: jsonText, kind: "json", ttlAt: null, txOrderTs: 1 });

			const scan = (select: "count" | "projection") =>
				scanRows(store, {
					routingHash: P(hk),
					hk,
					lower: KeyCodec.encodeOptional(undefined),
					lowerInclusive: true,
					upper: null,
					upperInclusive: false,
					cursor: null,
					direction: "asc",
					limit: 100,
					select,
					plan: null,
				}).rows;
			const countRows = scan("count");
			const projRows = scan("projection");

			expect(projRows.map((r) => r.itemBytes)).toEqual(countRows.map((r) => r.itemBytes));
			const items = new Map(projRows.map((r) => [KeyCodec.decode(r.sk) as string, r.payload as StoredItem]));
			expect(items.get("t")).toMatchObject({ data: "hello", kind: "text" });
			expect(items.get("b")?.data).toEqual(new Uint8Array([1, 2, 3]));
			// The public-read projection decodes JSONB back to JSON text.
			expect(items.get("j")).toMatchObject({ data: jsonText, kind: "json" });
			// The items.item_id link key stays inside the store: a query result never carries it.
			for (const r of projRows) {
				expect(r.payload).not.toHaveProperty("item_id");
			}
		});
	});

	it.each(["asc", "desc"] as const)(
		"scanQueryPage honors bounds, row cursors, and the sentinel sort key in %s order",
		async (direction) => {
			await withStore((store) => {
				const hk = kb("hk");
				const EMPTY = KeyCodec.encodeOptional(undefined);
				for (const sk of [EMPTY, kb("a"), kb("b"), kb("c"), kb("d")]) {
					store.upsertItem({ routingHash: P(hk), hk, sk, data: "x", kind: "text", ttlAt: null, txOrderTs: 1 });
				}
				const sksOf = (opts: {
					lower?: KeyBytes;
					lowerInclusive?: boolean;
					upper?: KeyBytes | null;
					upperInclusive?: boolean;
					cursor?: { hk: KeyBytes; sk: KeyBytes; inclusive?: boolean } | null;
				}) =>
					scanRows(store, {
						routingHash: P(hk),
						hk,
						direction,
						limit: 100,
						select: "count",
						plan: null,
						lower: opts.lower ?? EMPTY,
						lowerInclusive: opts.lowerInclusive ?? true,
						upper: opts.upper ?? null,
						upperInclusive: opts.upperInclusive ?? false,
						cursor: opts.cursor ?? null,
					}).rows.map((r) => (r.sk.byteLength === 0 ? "" : (KeyCodec.decode(r.sk) as string)));

				if (direction === "asc") {
					expect(sksOf({ lower: kb("b"), lowerInclusive: true, upper: kb("d"), upperInclusive: false })).toEqual(["b", "c"]);
					expect(sksOf({ lower: kb("b"), lowerInclusive: false })).toEqual(["c", "d"]);
					expect(sksOf({ cursor: { hk, sk: kb("b") } })).toEqual(["c", "d"]);
					expect(sksOf({ cursor: { hk, sk: kb("b"), inclusive: true } })).toEqual(["b", "c", "d"]);
					expect(sksOf({})).toEqual(["", "a", "b", "c", "d"]);
				} else {
					expect(sksOf({ lower: kb("b"), lowerInclusive: true, upper: kb("d"), upperInclusive: false })).toEqual(["c", "b"]);
					expect(sksOf({ lower: kb("b"), lowerInclusive: false })).toEqual(["d", "c"]);
					expect(sksOf({ cursor: { hk, sk: kb("b") } })).toEqual(["a", ""]);
					expect(sksOf({ cursor: { hk, sk: kb("b"), inclusive: true } })).toEqual(["b", "a", ""]);
					expect(sksOf({})).toEqual(["d", "c", "b", "a", ""]);
				}
			});
		},
	);

	it("scanQueryPage binds limit as given and sqlMetrics reports the rows read so far", async () => {
		await withStore((store) => {
			const hk = kb("hk");
			for (let i = 0; i < 10; i++) {
				store.upsertItem({
					routingHash: P(hk),
					hk,
					sk: kb(String(i).padStart(2, "0")),
					data: "x",
					kind: "text",
					ttlAt: null,
					txOrderTs: 1,
				});
			}
			const bounds = {
				routingHash: P(hk),
				hk,
				lower: KeyCodec.encodeOptional(undefined),
				lowerInclusive: true,
				upper: null,
				upperInclusive: false,
				cursor: null,
				direction: "asc" as const,
				plan: null,
			};

			const limited = scanRows(store, { ...bounds, limit: 3, select: "count" });
			expect(limited.rows).toHaveLength(3);

			const full = scanRows(store, { ...bounds, limit: 10, select: "count" });
			expect(full.rows).toHaveLength(10);
			const fullReads = full.metrics.rowsRead;
			expect(fullReads).toBeGreaterThanOrEqual(10);

			// The consumer stops after one row; the statement reads no further.
			const partial = scanRows(store, { ...bounds, limit: 10, select: "count" }, 1);
			expect(partial.rows).toHaveLength(1);
			expect(partial.metrics.rowsRead).toBeGreaterThanOrEqual(1);
			expect(partial.metrics.rowsRead).toBeLessThan(fullReads);
		});
	});

	it("the count scan and the projection scan search the primary key in its order", async () => {
		await withStore((_store, state) => {
			const plan = (sql: string, ...params: unknown[]) =>
				state.storage.sql
					.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${sql}`, ...params)
					.toArray()
					.map((r) => r.detail)
					.join(" | ");

			const bounds = {
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				lower: kb("a"),
				lowerInclusive: true,
				upper: kb("z"),
				upperInclusive: false,
				plan: null,
			};
			for (const direction of ["asc", "desc"] as const) {
				for (const cursor of [null, { hk: kb("hk"), sk: kb("m") }]) {
					const { sql, params } = queryScanStatement({ ...bounds, direction, cursor, limit: 10, select: "count" });
					const countPlan = plan(sql, ...params);
					expect(countPlan).toContain("SEARCH items USING PRIMARY KEY (routing_hash=? AND hk=?");
					expect(countPlan).not.toContain("USE TEMP B-TREE FOR ORDER BY");
				}
			}

			const proj = queryScanStatement({ ...bounds, direction: "asc", cursor: null, limit: 10, select: "projection" });
			const projPlan = plan(proj.sql, ...proj.params);
			expect(projPlan).toContain("SEARCH items USING PRIMARY KEY (routing_hash=? AND hk=?");
			expect(projPlan).not.toContain("USE TEMP B-TREE FOR ORDER BY");
		});
	});

	it("estimateProjectedRowBytes charges the envelope plus per-cell sizes", () => {
		// string "abc" → 3*2, Uint8Array(5) → 5, { json: "[1]" } → 3*2, the other four cells → 8 each.
		expect(estimateProjectedRowBytes([undefined, null, true, 2, "abc", new Uint8Array(5), { json: "[1]" }])).toBe(64 + 8 * 4 + 6 + 5 + 6);
	});

	it("estimateItemBytes grows with text, bytes, and JSON text payloads", () => {
		const base = {
			hk: kb("hk"),
			sk: kb("sk"),
			kind: "text" as const,
			ttl_epoch_utc_seconds: null,
			v: 1,
			last_read_ts: 0,
			last_write_ts: 0,
		};
		const expected = (data: string | Uint8Array) =>
			kb("hk").byteLength + kb("sk").byteLength + (typeof data === "string" ? data.length * 2 : data.byteLength) + 16 + 64;
		const jsonText = JSON.stringify({ a: 1 });
		expect(estimateItemBytes({ ...base, data: "hello" })).toBe(expected("hello"));
		expect(estimateItemBytes({ ...base, data: new Uint8Array([1, 2, 3]), kind: "bytes" })).toBe(expected(new Uint8Array([1, 2, 3])));
		expect(estimateItemBytes({ ...base, data: jsonText, kind: "json" })).toBe(expected(jsonText));
		expect(estimateItemBytes({ ...base, data: "x".repeat(100) })).toBeGreaterThan(estimateItemBytes({ ...base, data: "x" }));
	});

	it("migration reads json verbatim (raw JSONB) and re-inserts it queryable by jsonb_extract", async () => {
		await withStore((store, state) => {
			const jsonText = JSON.stringify({ status: "ok", count: 7 });
			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("j"), data: jsonText, kind: "json", ttlAt: null, txOrderTs: 1 });

			// Migration-style read: no json() decode, so json data is the raw JSONB blob.
			const migrated = [...store.queryItemsPage(null, 10, ALL)][0];
			expect(migrated.kind).toBe("json");
			expect(migrated.data).toBeInstanceOf(Uint8Array);

			// Re-insert verbatim under a new key and confirm the JSONB is still path-queryable.
			store.insertItemIfAbsent({ ...migrated, item_id: linkId(migrated.item_id + 1), sk: kb("j2") });
			const count = state.storage.sql
				.exec<{ c: number }>(`SELECT jsonb_extract(data, '$.count') AS c FROM items WHERE hk = ? AND sk = ?`, kb("hk"), kb("j2"))
				.toArray()[0].c;
			expect(count).toBe(7);
		});
	});

	it("updateItemSingleShot creates the absent item, then increments the version it created", async () => {
		await withStore((store, state) => {
			const update: UpdateExpression = [{ action: "set", target: { ref: "data", path: "$.x" }, value: { val: 1 } }];
			const plan = updateTree(update);
			const hk = kb("created-hk");
			const sk = kb("created-sk");

			const first = store.updateItemSingleShot({ routingHash: P(hk), hk, sk, plan, txOrderTs: 5 });
			expect(first.version).toBe(1);
			const created = store.getItem(P(hk), hk, sk).row;
			expect(created).toMatchObject({ kind: "json", ttl_epoch_utc_seconds: null, last_read_ts: 5, last_write_ts: 5 });
			expect(JSON.parse(created?.data as string)).toEqual({ x: 1 });
			// The created row is measured and accounted for exactly as an upsert of the same document is.
			expect(bucketEst(state, "created-hk")).toBe(first.keyEstBytes);

			// The second call finds the row it created and takes the conflict branch.
			const second = store.updateItemSingleShot({ routingHash: P(hk), hk, sk, plan, txOrderTs: 6 });
			expect(second.version).toBe(2);
			expect(bucketEst(state, "created-hk")).toBe(second.keyEstBytes);
		});
	});

	it("updateItemSingleShot never lowers the item timestamps, but still applies the write", async () => {
		await withStore((store) => {
			const hk = kb("hk");
			const sk = kb("sk");
			const plan = (x: number) => updateTree([{ action: "set", target: { ref: "data", path: "$.x" }, value: { val: x } }]);

			// As if committed by a transaction whose coordinator clock ran ahead of this partition's.
			store.upsertItem({ routingHash: P(hk), hk, sk, data: JSON.stringify({}), kind: "json", ttlAt: null, txOrderTs: 5_000 });

			// A single-shot update stamps the partition's own (lower) clock.
			const second = store.updateItemSingleShot({ routingHash: P(hk), hk, sk, plan: plan(2), txOrderTs: 1_000 });

			// The write lands in full — only the timestamps are held at the high-water mark.
			expect(second.version).toBe(2);
			const afterLag = store.getItem(P(hk), hk, sk).row;
			expect(afterLag).toMatchObject({ last_read_ts: 5_000, last_write_ts: 5_000 });
			expect(JSON.parse(afterLag?.data as string)).toEqual({ x: 2 });

			// A later timestamp still moves them forward.
			store.updateItemSingleShot({ routingHash: P(hk), hk, sk, plan: plan(3), txOrderTs: 9_000 });
			expect(store.getItem(P(hk), hk, sk).row).toMatchObject({ last_read_ts: 9_000, last_write_ts: 9_000 });
		});
	});

	it("updateItemSingleShot stores an integer literal and integer arithmetic as JSON integers", async () => {
		await withStore((store, state) => {
			const hk = kb("hk");
			const sk = kb("sk");
			store.upsertItem({ routingHash: P(hk), hk, sk, data: JSON.stringify({ count: 5 }), kind: "json", ttlAt: null, txOrderTs: 1 });
			const plan = updateTree([
				{
					action: "set",
					target: { ref: "data", path: "$.count" },
					value: { fn: "+", args: [{ fn: "if_not_exists", args: [{ ref: "data", path: "$.count" }, { val: 0 }] }, { val: 1 }] },
				},
				{ action: "set", target: { ref: "data", path: "$.at" }, value: { val: 1_788_000_000 } },
				{ action: "set", target: { ref: "data", path: "$.ratio" }, value: { val: 1.5 } },
			]);
			store.updateItemSingleShot({ routingHash: P(hk), hk, sk, plan, txOrderTs: 2 });

			// JSON.parse reads 6.0 as 6, so the test reads the stored text and the JSON type of each member.
			const row = state.storage.sql
				.exec<{ text: string; count: string; at: string; ratio: string }>(
					`SELECT json(data) AS text, json_type(data, '$.count') AS count, json_type(data, '$.at') AS at,
					        json_type(data, '$.ratio') AS ratio
					   FROM items WHERE hk = ? AND sk = ?`,
					hk,
					sk,
				)
				.one();
			expect(row).toEqual({ text: '{"count":6,"at":1788000000,"ratio":1.5}', count: "integer", at: "integer", ratio: "real" });
		});
	});

	it("bumpItemReadTs advances only the read watermark and never lowers it", async () => {
		await withStore((store) => {
			const hk = kb("hk");
			const sk = kb("sk");
			store.upsertItem({ routingHash: P(hk), hk, sk, data: "d", kind: "text", ttlAt: null, txOrderTs: 100 });

			store.bumpItemReadTs(P(hk), hk, sk, 200);
			expect(store.getItem(P(hk), hk, sk).row).toMatchObject({ last_read_ts: 200, last_write_ts: 100 });

			// An out-of-order check must not move the watermark back.
			store.bumpItemReadTs(P(hk), hk, sk, 150);
			expect(store.getItem(P(hk), hk, sk).row).toMatchObject({ last_read_ts: 200, last_write_ts: 100 });

			store.bumpItemReadTs(P(hk), hk, sk, 300);
			expect(store.getItem(P(hk), hk, sk).row).toMatchObject({ last_read_ts: 300, last_write_ts: 100 });
		});
	});

	it("keeps last_read_ts at or above last_write_ts after every writer, including a write behind a check", async () => {
		await withStore((store) => {
			const hk = kb("hk");
			const sk = kb("sk");
			const timestamps = () => {
				const row = store.getItem(P(hk), hk, sk).row;
				invariant(row, "the item must exist");
				expect(row.last_read_ts).toBeGreaterThanOrEqual(row.last_write_ts);
				return row;
			};

			store.upsertItem({ routingHash: P(hk), hk, sk, data: JSON.stringify({}), kind: "json", ttlAt: null, txOrderTs: 500 });
			expect(timestamps()).toMatchObject({ last_read_ts: 500, last_write_ts: 500 });

			// A committed check orders the item ahead of the write that created it.
			store.bumpItemReadTs(P(hk), hk, sk, 700);
			expect(timestamps()).toMatchObject({ last_read_ts: 700, last_write_ts: 500 });

			// A put stamped by a lagging clock lands below the read watermark.
			store.upsertItem({ routingHash: P(hk), hk, sk, data: JSON.stringify({}), kind: "json", ttlAt: null, txOrderTs: 600 });
			expect(timestamps()).toMatchObject({ last_read_ts: 700, last_write_ts: 600 });

			// A single-shot update stamped between the two watermarks lands between them too.
			const plan = updateTree([{ action: "set", target: { ref: "data", path: "$.x" }, value: { val: 1 } }]);
			store.updateItemSingleShot({ routingHash: P(hk), hk, sk, plan, txOrderTs: 650 });
			expect(timestamps()).toMatchObject({ last_read_ts: 700, last_write_ts: 650 });
		});
	});

	it("updateItemSingleShot reports an oversized result, and writes nothing", async () => {
		await withStore((store) => {
			const hk = kb("too-big");
			const sk = kb("s");
			const update: UpdateExpression = [
				{ action: "set", target: { ref: "data", path: "$.x" }, value: { val: "a".repeat(MAX_ITEM_BYTES) } },
			];
			const plan = updateTree(update);
			expect(() => store.updateItemSingleShot({ routingHash: P(hk), hk, sk, plan, txOrderTs: 1 })).toThrow(
				fokosErrorWith("item_too_large"),
			);
			expect(store.getItem(P(hk), hk, sk).row).toBeUndefined();
		});
	});

	it("stores text hashKey and sortKey references as JSON strings", async () => {
		await withStore((store) => {
			const hk = kb("my-hash-key");
			const sk = kb("my-sort-key");
			store.upsertItem({ routingHash: P(hk), hk, sk, data: JSON.stringify({}), kind: "json", ttlAt: null, txOrderTs: 1 });
			const update: UpdateExpression = [
				{ action: "set", target: { ref: "data", path: "$.hk" }, value: { ref: "hashKey" } },
				{ action: "set", target: { ref: "data", path: "$.sk" }, value: { ref: "sortKey" } },
			];
			const plan = updateTree(update);
			store.updateItemSingleShot({ routingHash: P(hk), hk, sk, plan, txOrderTs: 2 });
			const { row } = store.getItem(P(hk), hk, sk);
			expect(row).toBeDefined();
			const data = JSON.parse(row!.data as string);
			expect(data).toEqual({ hk: "my-hash-key", sk: "my-sort-key" });
		});
	});

	it("rejects hashKey and sortKey references when keys are binary, and names the cause", async () => {
		await withStore((store) => {
			const hk = kb(new Uint8Array([1, 2, 3]));
			const sk = kb(new Uint8Array([4, 5, 6]));
			store.upsertItem({ routingHash: P(hk), hk, sk, data: JSON.stringify({}), kind: "json", ttlAt: null, txOrderTs: 1 });
			// A function around the reference must not escape the test: SQLite carries the untagged key
			// bytes through, and the result is still a blob that a JSON document cannot hold.
			const values = [
				{ ref: "hashKey" as const },
				{ ref: "sortKey" as const },
				{ fn: "sqlite.ifnull" as const, args: [{ ref: "hashKey" as const }, { val: "fallback" }] },
				{ fn: "sqlite.coalesce" as const, args: [{ ref: "sortKey" as const }, { val: "fallback" }] },
			];
			for (const value of values) {
				const plan = updateTree([{ action: "set", target: { ref: "data", path: "$.x" }, value }] as UpdateExpression);
				const probe = store.probeUpdate(plan, P(hk), hk, sk);
				expect(probe.applicable).toBe(false);
				expect(probe.valueTypeOk).toBe(false);
			}
			// The item is untouched: an inapplicable update writes nothing.
			expect(JSON.parse(store.getItem(P(hk), hk, sk).row?.data as string)).toEqual({});
		});
	});

	it("updateItemSingleShot keeps item_bytes and the bucket facts in step with the stored document", async () => {
		await withStore((store, state) => {
			const hk = kb("hk");
			const sk = kb("s1");
			store.upsertItem({ routingHash: P(hk), hk, sk, data: JSON.stringify({ note: "short" }), kind: "json", ttlAt: null, txOrderTs: 1 });
			const before = bucketBytes(state, "hk");

			const plan = updateTree([
				{ action: "set", target: { ref: "data", path: "$.note" }, value: { val: "a considerably longer note than the first one" } },
			]);
			const res = store.updateItemSingleShot({ routingHash: P(hk), hk, sk, plan, txOrderTs: 2 });

			// The size the update stored is measured over the document the update wrote, and the key's
			// estimate carries exactly that row. An update grows an item without carrying its bytes, so
			// this accounting is what keeps promotion and split decisions correct.
			const row = state.storage.sql
				.exec<{ e: number; d: number }>(`SELECT item_bytes AS e, octet_length(data) AS d FROM items WHERE hk = ? AND sk = ?`, hk, sk)
				.one();
			expect(row.e).toBe(row.d + hk.byteLength + sk.byteLength);
			expect(bucketBytes(state, "hk")).toBe(row.e);
			expect(res.keyEstBytes).toBe(bucketEst(state, "hk"));
			expect(row.e).toBeGreaterThan(before!);
		});
	});

	it("insertItemIfAbsent ingests a row above the item size limit, which upsertItem refuses", async () => {
		await withStore((store) => {
			const hk = kb("hk");
			const sk = kb("big");
			// A migration copies rows that were accepted under the limit of their time, so the ingest
			// path carries no size guard. A guard there would drop an item when the limit falls.
			const oversized = "x".repeat(MAX_ITEM_BYTES + 1);
			expect(() => store.upsertItem({ routingHash: P(hk), hk, sk, data: oversized, kind: "text", ttlAt: null, txOrderTs: 1 })).toThrow(
				fokosErrorWith("item_too_large"),
			);

			store.insertItemIfAbsent({
				item_id: linkId(1),
				routingHash: P(hk),
				hk,
				sk,
				data: oversized,
				kind: "text",
				ttl_epoch_utc_seconds: null,
				v: 7,
				last_read_ts: 1,
				last_write_ts: 1,
			});
			expect(store.getItem(P(hk), hk, sk).row).toMatchObject({ v: 7, data: oversized });
		});
	});

	it("insertItemIfAbsent keeps the copied id, skips a retried key, and fails on an id another item holds", async () => {
		await withStore((store) => {
			const copied = (id: number, sk: string, data: string) => ({
				item_id: linkId(id),
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb(sk),
				data,
				kind: "text" as const,
				ttl_epoch_utc_seconds: null,
				v: 1,
				last_read_ts: 1,
				last_write_ts: 1,
			});
			const rows = () => [...store.queryItemsPage(null, 10, ALL)].map((r) => ({ item_id: r.item_id, data: r.data }));

			store.insertItemIfAbsent(copied(1000, "a", "first"));
			expect(rows()).toEqual([{ item_id: 1000, data: "first" }]);

			// A retried batch serves the same key again: the stored row stays.
			store.insertItemIfAbsent(copied(1000, "a", "retry"));
			expect(rows()).toEqual([{ item_id: 1000, data: "first" }]);

			// A different item with an id that is already taken must fail, not disappear.
			expect(() => store.insertItemIfAbsent(copied(1000, "b", "other"))).toThrow(/UNIQUE constraint failed: items\.item_id/);
			expect(rows()).toEqual([{ item_id: 1000, data: "first" }]);

			// The ingest raises the mark after its page. A local write after the copy takes an id above
			// every copied id.
			store.raiseItemIdMark(1000);
			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("c"), data: "local", kind: "text", ttlAt: null, txOrderTs: 2 });
			expect(rows()).toEqual([
				{ item_id: 1000, data: "first" },
				{ item_id: 1001, data: "local" },
			]);

			// An overwrite keeps the id of the item.
			store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("a"),
				data: "overwritten",
				kind: "text",
				ttlAt: null,
				txOrderTs: 3,
			});
			expect(rows()).toEqual([
				{ item_id: 1000, data: "overwritten" },
				{ item_id: 1001, data: "local" },
			]);
		});
	});

	it("separates a byte value from every other inapplicable update", async () => {
		await withStore((store) => {
			const hk = kb("text-key");
			const sk = kb("text-sort");
			store.upsertItem({ routingHash: P(hk), hk, sk, data: JSON.stringify({ a: 1 }), kind: "json", ttlAt: null, txOrderTs: 1 });
			// A missing target parent is inapplicable, but the values are fine, so the cause is not the
			// value type. Only that distinction lets a caller tell a fixable value from a stale item.
			const missingParent = updateTree([{ action: "set", target: { ref: "data", path: "$.absent.x" }, value: { val: 1 } }]);
			const probe = store.probeUpdate(missingParent, P(hk), hk, sk);
			expect(probe.applicable).toBe(false);
			expect(probe.valueTypeOk).toBe(true);

			// A text key is a valid update value, so the same plan that fails over a binary key applies here.
			const keyValue = updateTree([{ action: "set", target: { ref: "data", path: "$.k" }, value: { ref: "hashKey" } }]);
			expect(store.probeUpdate(keyValue, P(hk), hk, sk)).toMatchObject({ applicable: true, valueTypeOk: true });
		});
	});
});

describe("PartitionStore - TTL deletion", () => {
	function put(store: PartitionStore, hk: string, sk: string, ttlAt: number | null, data = "d"): number {
		store.upsertItem({ routingHash: P(kb(hk)), hk: kb(hk), sk: kb(sk), data, kind: "text", ttlAt, txOrderTs: 1 });
		return expectedRowBytes(data, kb(hk), kb(sk));
	}

	it("uses the bounded covering-index candidate plan and deletes the owned rows by primary key", async () => {
		await withStore((_store, state) => {
			const explain = (sql: string, ...bindings: unknown[]) =>
				state.storage.sql
					.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${sql}`, ...bindings)
					.toArray()
					.map((row) => row.detail)
					.join(" | ");
			const candidates = (indexHint: string, position: string, ...positionBindings: unknown[]) =>
				explain(
					`SELECT i.ttl_epoch_utc_seconds AS ttl, i.routing_hash, i.hk, i.sk FROM items i${indexHint}
					  WHERE i.ttl_epoch_utc_seconds IS NOT NULL
					    AND i.ttl_epoch_utc_seconds <= ?1
					    ${position}
					    AND NOT EXISTS (SELECT 1 FROM pending_transactions p
					                     WHERE p.routing_hash = i.routing_hash AND p.hk = i.hk AND p.sk = i.sk)
					  ORDER BY i.ttl_epoch_utc_seconds, i.routing_hash, i.hk, i.sk
					  LIMIT ?2`,
					100,
					10,
					...positionBindings,
				);
			const afterPosition = "AND (i.ttl_epoch_utc_seconds, i.routing_hash, i.hk, i.sk) > (?3, ?4, ?5, ?6)";

			for (const details of [
				candidates(" INDEXED BY idx_items_ttl", ""),
				candidates("", ""),
				candidates(" INDEXED BY idx_items_ttl", afterPosition, 5, P(kb("a")), kb("a"), kb("s")),
			]) {
				expect(details).toContain("SEARCH i USING COVERING INDEX idx_items_ttl");
				expect(details).toMatch(/SEARCH p USING COVERING INDEX sqlite_autoindex_pending_transactions_1/);
				expect(details).not.toMatch(/SCAN p|USE TEMP B-TREE FOR/);
			}
			// The scan seeks to the position, and does not read the rows before it.
			expect(candidates(" INDEXED BY idx_items_ttl", afterPosition, 5, P(kb("a")), kb("a"), kb("s"))).toContain(
				"idx_items_ttl ((ttl_epoch_utc_seconds,routing_hash,hk,sk)>(?,?,?,?) AND ttl_epoch_utc_seconds<?)",
			);
			expect(
				explain(
					`DELETE FROM items WHERE routing_hash = ? AND hk = ? AND sk = ?
					 RETURNING item_bytes, ttl_epoch_utc_seconds, v, last_read_ts`,
					P(kb("a")),
					kb("a"),
					kb("s"),
				),
			).toMatch(/SEARCH items USING PRIMARY KEY \(routing_hash=\? AND hk=\? AND sk=\?\)/);
		});
	});

	it("deletes oldest eligible rows and updates each key account", async () => {
		await withStore((store, state) => {
			const oldBytes = put(store, "a", "old", 10, "a");
			const lockedBytes = put(store, "locked", "s", 11, "locked");
			const secondBytes = put(store, "b", "s", 20, "bb");
			const nextBytes = put(store, "a", "next", 30, "ccc");
			const exactBytes = put(store, "c", "exact", 100, "dddd");
			const nullBytes = put(store, "a", "null", null, "null");
			const futureBytes = put(store, "a", "future", 101, "future");

			store.insertPendingLock({
				routingHash: P(kb("locked")),
				hk: kb("locked"),
				sk: kb("s"),
				transaction_id: "tx-locked",
				transaction_ts: 1,
				operation: "put",
				op_list: [[0, "put"] as [number, "put"]],
				data: "pending",
				kind: "text",
				ttl_epoch_utc_seconds: null,
				coordinator_json: "{}",
				created_at: 1,
				guarded_at: null,
				next_recovery_at: 1,
			});

			const first = store.deleteExpiredItems(100, 2, ownsAll);
			expect(first).toMatchObject({ deletedRows: 2, deletedBytes: oldBytes + secondBytes });
			expect(store.getItem(P(kb("a")), kb("a"), kb("old")).row).toBeUndefined();
			expect(store.getItem(P(kb("b")), kb("b"), kb("s")).row).toBeUndefined();
			expect(store.getItem(P(kb("a")), kb("a"), kb("next")).row).toBeDefined();
			expect(bucketBytes(state, "a")).toBe(nextBytes + nullBytes + futureBytes);
			// The sweep removed the last item of "b", so its estimate row is gone.
			expect(bucketBytes(state, "b")).toBeUndefined();
			expect(store.getMaxDeleteTxOrderTs()).toBe(20 * 1000 * TX_ORDER_TS_UNITS_PER_MS);
			// The sweep advances the transaction order watermark to the largest expiry it reclaimed, and
			// max_deleted_v to the largest delete revision (v + 1) it removed.
			expect(store.getDeletionMetadata()).toEqual({ maxDeleteTxOrderTs: 20 * 1000 * TX_ORDER_TS_UNITS_PER_MS, maxDeletedV: 2 });

			store.bumpMaxDeleteTxOrderTs(200_000 * TX_ORDER_TS_UNITS_PER_MS);
			const second = store.deleteExpiredItems(100, 2, ownsAll);
			expect(second).toMatchObject({ deletedRows: 2, deletedBytes: nextBytes + exactBytes });
			expect(store.getItem(P(kb("a")), kb("a"), kb("next")).row).toBeUndefined();
			expect(store.getItem(P(kb("c")), kb("c"), kb("exact")).row).toBeUndefined();
			expect(bucketBytes(state, "a")).toBe(nullBytes + futureBytes);
			expect(bucketBytes(state, "c")).toBeUndefined();
			expect(store.getMaxDeleteTxOrderTs()).toBe(200_000 * TX_ORDER_TS_UNITS_PER_MS);

			expect(store.getItem(P(kb("locked")), kb("locked"), kb("s")).row).toBeDefined();
			expect(bucketBytes(state, "locked")).toBe(lockedBytes);
			expect(store.getItem(P(kb("a")), kb("a"), kb("null")).row).toBeDefined();
			expect(store.getItem(P(kb("a")), kb("a"), kb("future")).row).toBeDefined();
			expect(store.deleteExpiredItems(100, 2, ownsAll)).toEqual({ deletedRows: 0, deletedBytes: 0, resumeAfter: null });
			expect(store.getDeletionMetadata().maxDeletedV).toBe(2);
		});
	});
});

describe("PartitionStore - deletion watermark", () => {
	it("bumpMaxDeleteTxOrderTs is monotonic", async () => {
		await withStore((store) => {
			expect(store.getMaxDeleteTxOrderTs()).toBe(0);
			store.bumpMaxDeleteTxOrderTs(100);
			expect(store.getMaxDeleteTxOrderTs()).toBe(100);
			store.bumpMaxDeleteTxOrderTs(50);
			expect(store.getMaxDeleteTxOrderTs()).toBe(100);
			store.bumpMaxDeleteTxOrderTs(150);
			expect(store.getMaxDeleteTxOrderTs()).toBe(150);
		});
	});

	it("bumpMaxDeleteTxOrderTs writes a row only when it raises the value", async () => {
		await withStore((store) => {
			expect(store.bumpMaxDeleteTxOrderTs(100).rowsWritten).toBe(1);
			expect(store.bumpMaxDeleteTxOrderTs(100).rowsWritten).toBe(0);
			expect(store.bumpMaxDeleteTxOrderTs(50).rowsWritten).toBe(0);
			expect(store.getMaxDeleteTxOrderTs()).toBe(100);
		});
	});

	it("deleteItem bumps the watermark only when a row was deleted, unless bumpTxOrderTsAlways", async () => {
		await withStore((store) => {
			// Absent row, default behavior: no bump.
			const miss = store.deleteItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("absent"), txOrderTs: 100 });
			expect(miss.deleted).toBe(false);
			expect(miss.deleteVersion).toBeUndefined();
			expect(store.getDeletionMetadata()).toEqual({ maxDeleteTxOrderTs: 0, maxDeletedV: 0 });

			// Absent row, transactional behavior: bump the watermark regardless. No row means no v to record.
			store.deleteItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("absent"), txOrderTs: 100, bumpTxOrderTsAlways: true });
			expect(store.getDeletionMetadata()).toEqual({ maxDeleteTxOrderTs: 100, maxDeletedV: 0 });

			// Present row at v = 1: bump both. The delete revision is v + 1.
			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("s"), data: "d", kind: "text", ttlAt: null, txOrderTs: 1 });
			const hit = store.deleteItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("s"), txOrderTs: 200 });
			expect(hit.deleted).toBe(true);
			expect(hit.deleteVersion).toBe(2);
			expect(store.getDeletionMetadata()).toEqual({ maxDeleteTxOrderTs: 200, maxDeletedV: 2 });
		});
	});

	it("a real user delete updates both deletion-metadata fields in one statement", async () => {
		const stub = testPartitionStub(`store-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
			// Records every statement the store issues. The migrations already ran in the FokosDBPartitionDO
			// constructor, so the wrapped storage never sees a migration statement.
			const statements: string[] = [];
			const recordingStorage = {
				sql: {
					exec: (sql: string, ...params: unknown[]) => {
						statements.push(sql);
						return state.storage.sql.exec(sql, ...params);
					},
					get databaseSize() {
						return state.storage.sql.databaseSize;
					},
				},
				transactionSync: <T>(fn: () => T): T => state.storage.transactionSync(fn),
			} as unknown as DurableObjectStorage;
			const store = new PartitionStore(recordingStorage);
			const metadataStatements = () => statements.filter((sql) => sql.includes("deletion_metadata"));

			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("s"), data: "d", kind: "text", ttlAt: null, txOrderTs: 1 });

			statements.length = 0;
			const hit = store.deleteItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("s"), txOrderTs: 200 });
			expect(hit.deleted).toBe(true);
			const hitWrites = metadataStatements();
			expect(hitWrites).toHaveLength(1);
			expect(hitWrites[0]).toContain("max_delete_tx_order_ts");
			expect(hitWrites[0]).toContain("max_deleted_v");
			expect(store.getDeletionMetadata()).toEqual({ maxDeleteTxOrderTs: 200, maxDeletedV: 2 });

			// The transactional absent delete advances the watermark only, in one statement.
			statements.length = 0;
			const miss = store.deleteItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("absent"),
				txOrderTs: 300,
				bumpTxOrderTsAlways: true,
			});
			expect(miss.deleted).toBe(false);
			const missWrites = metadataStatements();
			expect(missWrites).toHaveLength(1);
			expect(missWrites[0]).not.toContain("max_deleted_v");
			expect(store.getDeletionMetadata()).toEqual({ maxDeleteTxOrderTs: 300, maxDeletedV: 2 });

			// The non-transactional absent delete writes no metadata at all.
			statements.length = 0;
			store.deleteItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("absent"), txOrderTs: 400 });
			expect(metadataStatements()).toHaveLength(0);
			expect(store.getDeletionMetadata()).toEqual({ maxDeleteTxOrderTs: 300, maxDeletedV: 2 });
		});
	});

	it("promotion cleanup changes neither deletion-metadata value", async () => {
		await withStore((store) => {
			store.upsertItem({ routingHash: P(kb("alice")), hk: kb("alice"), sk: kb("s1"), data: "a1", kind: "text", ttlAt: null, txOrderTs: 1 });
			store.upsertItem({ routingHash: P(kb("alice")), hk: kb("alice"), sk: kb("s2"), data: "a2", kind: "text", ttlAt: null, txOrderTs: 1 });
			store.upsertItem({ routingHash: P(kb("bob")), hk: kb("bob"), sk: kb("s"), data: "b", kind: "text", ttlAt: null, txOrderTs: 1 });

			// A real user delete first, so both metadata fields are non-zero.
			store.deleteItem({ routingHash: P(kb("bob")), hk: kb("bob"), sk: kb("s"), txOrderTs: 555 });
			const before = store.getDeletionMetadata();
			expect(before).toEqual({ maxDeleteTxOrderTs: 555, maxDeletedV: 2 });

			// The cleanup a promotion runs over the routingHash it is moving.
			store.deleteItemsBatchForRoutingHash(P(kb("alice")), 1000);
			store.itemStats.remove(P(kb("alice")));

			expect(store.hasItemsForRoutingHash(P(kb("alice")))).toBe(false);
			expect(store.getDeletionMetadata()).toEqual(before);
		});
	});
});

describe("PartitionStore - max_deleted_v and the timestamp watermark of a key", () => {
	const text = (store: PartitionStore, hk: string, txOrderTs: number) =>
		store.upsertItem({ routingHash: P(kb(hk)), hk: kb(hk), sk: kb("s"), data: "d", kind: "text", ttlAt: null, txOrderTs }).version;
	const setA = updateTree([{ action: "set", target: { ref: "data", path: "$.a" }, value: { val: 1 } }]);
	const row = (store: PartitionStore, hk: string) => store.getItem(P(kb(hk)), kb(hk), kb("s")).row;

	it("starts the first item row of a new partition at v = 1", async () => {
		await withStore((store) => {
			expect(text(store, "x", 1)).toBe(1);
			expect(store.updateItemSingleShot({ routingHash: P(kb("y")), hk: kb("y"), sk: kb("s"), plan: setA, txOrderTs: 1 }).version).toBe(1);
		});
	});

	it("starts a recreated row above the delete revision of the earlier lifetime, for both create statements", async () => {
		await withStore((store) => {
			text(store, "x", 1);
			text(store, "x", 1);
			expect(text(store, "x", 1)).toBe(3);
			expect(store.deleteItem({ routingHash: P(kb("x")), hk: kb("x"), sk: kb("s"), txOrderTs: 1 }).deleteVersion).toBe(4);
			expect(store.getDeletionMetadata().maxDeletedV).toBe(4);
			expect(text(store, "x", 1)).toBe(5);
			// An update of the existing row keeps v + 1.
			expect(text(store, "x", 1)).toBe(6);

			expect(store.deleteItem({ routingHash: P(kb("x")), hk: kb("x"), sk: kb("s"), txOrderTs: 1 }).deleteVersion).toBe(7);
			expect(store.updateItemSingleShot({ routingHash: P(kb("x")), hk: kb("x"), sk: kb("s"), plan: setA, txOrderTs: 1 }).version).toBe(8);
			expect(store.updateItemSingleShot({ routingHash: P(kb("x")), hk: kb("x"), sk: kb("s"), plan: setA, txOrderTs: 1 }).version).toBe(9);
		});
	});

	it("never lowers max_deleted_v on a delete of a row with a lower v", async () => {
		await withStore((store) => {
			text(store, "old", 1);
			text(store, "hot", 1);
			text(store, "hot", 1);
			store.deleteItem({ routingHash: P(kb("hot")), hk: kb("hot"), sk: kb("s"), txOrderTs: 1 });
			store.deleteItem({ routingHash: P(kb("old")), hk: kb("old"), sk: kb("s"), txOrderTs: 1 });
			expect(store.getDeletionMetadata().maxDeletedV).toBe(3);
		});
	});

	it("deletes only the expired rows that the owner check accepts, and keeps the copies", async () => {
		await withStore((store, state) => {
			const expiring = (hk: string, sk: string) =>
				store.upsertItem({ routingHash: P(kb(hk)), hk: kb(hk), sk: kb(sk), data: "d", kind: "text", ttlAt: 10, txOrderTs: 1 }).version;
			expiring("copy", "s1");
			expiring("copy", "s2");
			const ownedVersion = expiring("owned", "s");
			const copyBytes = bucketBytes(state, "copy");
			const ownsByKey = (row: { hk: KeyBytes }) => keyLabel(row.hk) !== "copy";

			// The chunk holds the 2 copies and the owned row. Only the owned row is deleted and counted.
			expect(store.deleteExpiredItems(100, 3, ownsByKey)).toMatchObject({ deletedRows: 1 });
			expect(store.getItem(P(kb("owned")), kb("owned"), kb("s")).row).toBeUndefined();
			expect(store.getItem(P(kb("copy")), kb("copy"), kb("s1")).row).toBeDefined();
			expect(store.getItem(P(kb("copy")), kb("copy"), kb("s2")).row).toBeDefined();
			expect(bucketBytes(state, "copy")).toBe(copyBytes);
			expect(store.getDeletionMetadata().maxDeletedV).toBe(ownedVersion + 1);

			// A chunk of copies only deletes nothing and writes no deletion metadata.
			expect(store.deleteExpiredItems(100, 3, ownsByKey)).toEqual({ deletedRows: 0, deletedBytes: 0, resumeAfter: null });
			expect(store.getDeletionMetadata().maxDeletedV).toBe(ownedVersion + 1);
		});
	});

	it("continues after the returned position, so a long run of copies does not stop the owned rows after it", async () => {
		await withStore((store) => {
			// 12 copies expire first, and the owned row expires last. One chunk of limit 1 reads at most 10 candidates.
			for (let n = 0; n < 12; n++) {
				store.upsertItem({
					routingHash: P(kb("copy")),
					hk: kb("copy"),
					sk: kb(`s${String(n).padStart(2, "0")}`),
					data: "d",
					kind: "text",
					ttlAt: 10,
					txOrderTs: 1,
				});
			}
			store.upsertItem({ routingHash: P(kb("owned")), hk: kb("owned"), sk: kb("s"), data: "d", kind: "text", ttlAt: 20, txOrderTs: 1 });
			const ownsByKey = (row: { hk: KeyBytes }) => keyLabel(row.hk) !== "copy";

			const first = store.deleteExpiredItems(100, 1, ownsByKey);
			expect(first).toMatchObject({ deletedRows: 0, resumeAfter: { ttlAt: 10, hk: kb("copy"), sk: kb("s09") } });
			// The same chunk from the start reads the same copies again.
			expect(store.deleteExpiredItems(100, 1, ownsByKey)).toEqual(first);

			const second = store.deleteExpiredItems(100, 1, ownsByKey, first.resumeAfter);
			expect(second).toMatchObject({ deletedRows: 1, resumeAfter: { ttlAt: 20, hk: kb("owned"), sk: kb("s") } });
			expect(store.getItem(P(kb("owned")), kb("owned"), kb("s")).row).toBeUndefined();
			expect(store.deleteExpiredItems(100, 1, ownsByKey, second.resumeAfter)).toEqual({
				deletedRows: 0,
				deletedBytes: 0,
				resumeAfter: null,
			});
		});
	});

	it("raises max_deleted_v to the highest delete revision (v + 1) of a sweep chunk, and a recreate starts above it", async () => {
		await withStore((store) => {
			const expiring = (hk: string) =>
				store.upsertItem({ routingHash: P(kb(hk)), hk: kb(hk), sk: kb("s"), data: "d", kind: "text", ttlAt: 10, txOrderTs: 1 });
			expiring("a");
			expiring("b");
			expiring("b");
			expect(expiring("b").version).toBe(3);
			expect(store.deleteExpiredItems(100, 10, ownsAll).deletedRows).toBe(2);
			expect(store.getDeletionMetadata().maxDeletedV).toBe(4);
			expect(text(store, "b", 1)).toBe(5);
		});
	});

	it("raises max_delete_tx_order_ts to the last_read_ts of a removed row", async () => {
		await withStore((store) => {
			text(store, "x", 100);
			store.bumpItemReadTs(P(kb("x")), kb("x"), kb("s"), 200);
			store.deleteItem({ routingHash: P(kb("x")), hk: kb("x"), sk: kb("s"), txOrderTs: 160 });
			expect(store.getMaxDeleteTxOrderTs()).toBe(200);
		});
	});

	it("raises max_delete_tx_order_ts to the last_read_ts of an expired row above its expiry time", async () => {
		await withStore((store) => {
			const expiryTs = 10 * 1000 * TX_ORDER_TS_UNITS_PER_MS;
			store.upsertItem({ routingHash: P(kb("x")), hk: kb("x"), sk: kb("s"), data: "d", kind: "text", ttlAt: 10, txOrderTs: 1 });
			store.bumpItemReadTs(P(kb("x")), kb("x"), kb("s"), expiryTs + 5);
			expect(store.deleteExpiredItems(100, 10, ownsAll).deletedRows).toBe(1);
			expect(store.getMaxDeleteTxOrderTs()).toBe(expiryTs + 5);
		});
	});

	it("stamps a new row with at least max_delete_tx_order_ts, for both create statements", async () => {
		await withStore((store) => {
			store.bumpMaxDeleteTxOrderTs(200);
			text(store, "put", 160);
			expect(row(store, "put")).toMatchObject({ last_read_ts: 200, last_write_ts: 200 });
			store.updateItemSingleShot({ routingHash: P(kb("upd")), hk: kb("upd"), sk: kb("s"), plan: setA, txOrderTs: 160 });
			expect(row(store, "upd")).toMatchObject({ last_read_ts: 200, last_write_ts: 200 });

			// A create with a higher timestamp keeps its own.
			text(store, "later", 300);
			expect(row(store, "later")).toMatchObject({ last_read_ts: 300, last_write_ts: 300 });
		});
	});

	it("does not take max_delete_tx_order_ts into the stamps of an update of an existing row", async () => {
		await withStore((store) => {
			text(store, "put", 100);
			store.updateItemSingleShot({ routingHash: P(kb("upd")), hk: kb("upd"), sk: kb("s"), plan: setA, txOrderTs: 100 });
			store.bumpMaxDeleteTxOrderTs(500);

			text(store, "put", 150);
			expect(row(store, "put")).toMatchObject({ v: 2, last_read_ts: 150, last_write_ts: 150 });
			store.updateItemSingleShot({ routingHash: P(kb("upd")), hk: kb("upd"), sk: kb("s"), plan: setA, txOrderTs: 150 });
			expect(row(store, "upd")).toMatchObject({ v: 2, last_read_ts: 150, last_write_ts: 150 });
		});
	});

	it("mergeDeletionMetadata takes the MAX of each value, and a retry gives the same result", async () => {
		await withStore((store) => {
			store.mergeDeletionMetadata({ maxDeleteTxOrderTs: 100, maxDeletedV: 7 });
			store.mergeDeletionMetadata({ maxDeleteTxOrderTs: 100, maxDeletedV: 7 });
			expect(store.getDeletionMetadata()).toEqual({ maxDeleteTxOrderTs: 100, maxDeletedV: 7 });

			store.mergeDeletionMetadata({ maxDeleteTxOrderTs: 50, maxDeletedV: 3 });
			expect(store.getDeletionMetadata()).toEqual({ maxDeleteTxOrderTs: 100, maxDeletedV: 7 });
			expect(text(store, "x", 1)).toBe(8);
		});
	});
});

describe("PartitionStore - pending transactions", () => {
	function lockRow(hk: string, sk: string, transactionId: string) {
		return {
			routingHash: P(kb(hk)),
			hk: kb(hk),
			sk: kb(sk),
			transaction_id: transactionId,
			transaction_ts: 123,
			operation: "put",
			op_list: [[0, "put"] as [number, "put"]],
			data: "d",
			kind: "text" as const,
			ttl_epoch_utc_seconds: null,
			coordinator_json: '{"doName":"tc-1"}',
			created_at: 1000,
			guarded_at: null,
			next_recovery_at: 6000,
		};
	}

	/** The `pending_tx_info` row of one transaction, or undefined when it does not exist. */
	function pendingTxRow(state: DurableObjectState, transactionId: string) {
		return state.storage.sql
			.exec<{
				guarded_at: number | null;
				next_recovery_at: number;
			}>(`SELECT guarded_at, next_recovery_at FROM pending_tx_info WHERE transaction_id = ?`, transactionId)
			.toArray()[0];
	}

	it("insertPendingLock is idempotent and pendingLockFor finds the lock", async () => {
		await withStore((store) => {
			store.insertPendingLock(lockRow("hk", "s", "tx1"));
			store.insertPendingLock(lockRow("hk", "s", "tx1")); // retry — INSERT OR IGNORE
			expect(store.listPendingTxKeys("tx1").length).toBe(1);
			expect(store.pendingLockFor(P(kb("hk")), kb("hk"), kb("s"))?.transaction_id).toBe("tx1");
			expect(store.pendingLockFor(P(kb("hk")), kb("hk"), kb("other"))).toBeUndefined();

			store.deletePendingTxKeys("tx1", [{ routingHash: P(kb("hk")), hashKey: kb("hk"), sortKey: kb("s") }]);
			expect(store.pendingLockFor(P(kb("hk")), kb("hk"), kb("s"))).toBeUndefined();
			expect(store.hasAnyPendingTx()).toBe(false);
		});
	});

	it("returns the prepared TTL on commit and recovery reads", async () => {
		await withStore((store) => {
			const row = { ...lockRow("hk", "s", "tx-ttl"), ttl_epoch_utc_seconds: 777 };
			store.insertPendingLock(row);

			expect(store.listPendingTxItems(row.transaction_id)[0].ttl_epoch_utc_seconds).toBe(777);
			expect([...store.queryPendingTxPage(null, 1, ALL)][0].ttl_epoch_utc_seconds).toBe(777);
		});
	});

	it("returns the operation list on commit and migration reads, and refuses a list that is not valid", async () => {
		await withStore((store, state) => {
			const opList: [number, "put" | "delete" | "check"][] = [
				[0, "delete"],
				[3, "put"],
				[4, "check"],
			];
			store.insertPendingLock({ ...lockRow("hk", "s", "tx-ops"), op_list: opList });
			expect(store.listPendingTxItems("tx-ops")[0].op_list).toEqual(opList);
			expect([...store.queryPendingTxPage(null, 1, ALL)][0].op_list).toEqual(opList);

			for (const bad of ["[]", '[[0,"frobnicate"]]', '[[0.5,"put"]]', "[[0]]", '{"0":"put"}']) {
				state.storage.sql.exec(`UPDATE pending_transactions SET op_list = ?`, bad);
				expect(() => store.listPendingTxItems("tx-ops")).toThrow(fokosErrorWith("invariant_failed"));
			}
		});
	});

	it("readItemData returns the stored JSONB of a json item, which upsertItem stores again unchanged", async () => {
		await withStore((store, state) => {
			store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("s"),
				data: '{"k":"he said \\"hi\\""}',
				kind: "json",
				ttlAt: 42,
				txOrderTs: 1,
			});
			const read = store.readItemData(P(kb("hk")), kb("hk"), kb("s"));
			expect(read).toMatchObject({ kind: "json", ttl_epoch_utc_seconds: 42 });
			expect(read?.data).toBeInstanceOf(Uint8Array);
			const stored = () => new Uint8Array(state.storage.sql.exec<{ data: ArrayBuffer }>(`SELECT data FROM items`).one().data);
			expect(stored()).toEqual(read?.data);

			store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("s"),
				data: read!.data,
				kind: read!.kind,
				ttlAt: 42,
				txOrderTs: 2,
			});
			expect(stored()).toEqual(read?.data);
			expect(store.readItemData(P(kb("hk")), kb("hk"), kb("absent"))).toBeUndefined();
		});
	});

	it("hasAnyPendingTx answers from a single row", async () => {
		await withStore((store, _state) => {
			expect(store.hasAnyPendingTx()).toBe(false);
			for (const sk of ["1", "2", "3"]) {
				store.insertPendingLock(lockRow("hk", sk, "tx1"));
			}
			expect(store.hasAnyPendingTx()).toBe(true);

			expect(store.deletePendingTxCopiesBatch(P(kb("hk")), 10)).toBe(true);
			expect(store.hasAnyPendingTx()).toBe(false);
		});
	});

	it("listStalePendingTx returns the due transactions, earliest next attempt first", async () => {
		await withStore((store) => {
			store.insertPendingLock({ ...lockRow("a", "1", "tx-later"), next_recovery_at: 3000 });
			store.insertPendingLock({ ...lockRow("b", "1", "tx-earlier"), next_recovery_at: 2000 });
			store.insertPendingLock({ ...lockRow("c", "1", "tx-new"), next_recovery_at: 9000 });
			expect(store.earliestPendingTxRecoveryAt()).toBe(2000);
			expect(store.listStalePendingTx(5000, 10).map((row) => row.transaction_id)).toEqual(["tx-earlier", "tx-later"]);
			expect(store.listStalePendingTx(5000, 1)).toEqual([
				{ transaction_id: "tx-earlier", coordinator_json: '{"doName":"tc-1"}', created_at: 1000 },
			]);
			expect(store.listStalePendingTx(1000, 10)).toEqual([]);

			// A transaction that the job tried moves behind the others.
			store.deferPendingTxRecovery("tx-earlier", 4000);
			expect(store.earliestPendingTxRecoveryAt()).toBe(3000);
			expect(store.listStalePendingTx(5000, 10).map((row) => row.transaction_id)).toEqual(["tx-later", "tx-earlier"]);
		});
	});

	it("keeps one pending_tx_info row while any lock of its transaction stays", async () => {
		await withStore((store, state) => {
			for (const [hk, sk] of [
				["a", "1"],
				["a", "2"],
				["b", "1"],
			]) {
				store.insertPendingLock(lockRow(hk, sk, "tx1"));
			}
			store.insertPendingLock(lockRow("b", "2", "tx2"));
			expect(state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM pending_tx_info`).one().n).toBe(2);

			store.deletePendingTxKeys("tx1", [{ routingHash: P(kb("a")), hashKey: kb("a"), sortKey: kb("1") }]);
			expect(pendingTxRow(state, "tx1")).toBeDefined();
			// The cleanup after a promotion deletes the copies of one hash key.
			expect(store.deletePendingTxCopiesBatch(P(kb("a")), 10)).toBe(true);
			expect(pendingTxRow(state, "tx1")).toBeDefined();
			expect(store.deletePendingTxCopiesBatch(P(kb("b")), 10)).toBe(true);
			expect(pendingTxRow(state, "tx1")).toBeUndefined();
			expect(pendingTxRow(state, "tx2")).toBeUndefined();

			store.insertPendingLock(lockRow("c", "1", "tx3"));
			expect(store.deletePendingTxCopiesBatch(null, 10)).toBe(true);
			expect(pendingTxRow(state, "tx3")).toBeUndefined();
			expect(store.hasAnyPendingTx()).toBe(false);
		});
	});

	it("deletes the lock copies in steps of whole transactions", async () => {
		await withStore((store, state) => {
			// tx1 and tx2 lock three keys each. tx3 locks one key of "a" and one key of "b".
			for (const [hk, sk, tx] of [
				["a", "1", "tx1"],
				["a", "2", "tx1"],
				["a", "3", "tx1"],
				["a", "4", "tx2"],
				["a", "5", "tx2"],
				["a", "6", "tx2"],
				["a", "7", "tx3"],
				["b", "1", "tx3"],
			]) {
				store.insertPendingLock(lockRow(hk, sk, tx));
			}
			const remaining = () => ["tx1", "tx2", "tx3"].map((tx) => store.listPendingTxKeys(tx).length);

			// A budget of 1 row still deletes all copies of the transaction that the step starts: tx1
			// holds the first key of "a".
			expect(store.deletePendingTxCopiesBatch(P(kb("a")), 1)).toBe(false);
			expect(remaining()).toEqual([0, 3, 2]);

			expect(store.deletePendingTxCopiesBatch(P(kb("a")), 5)).toBe(true);
			// The row of "b" is not a copy, so tx3 keeps it and its transaction row.
			expect(remaining()).toEqual([0, 0, 1]);
			expect(pendingTxRow(state, "tx3")).toBeDefined();

			expect(store.deletePendingTxCopiesBatch(null, 1)).toBe(false);
			expect(store.deletePendingTxCopiesBatch(null, 1)).toBe(true);
			expect(store.hasAnyPendingTx()).toBe(false);
			expect(state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM pending_tx_info`).one().n).toBe(0);
		});
	});

	// A migration page repeats the transaction row for each lock row, and a transaction can span pages.
	it("merges a repeated transaction row: it keeps a guard and the earlier next attempt", async () => {
		await withStore((store, state) => {
			store.insertPendingLock({ ...lockRow("a", "1", "tx1"), next_recovery_at: 5000 });
			store.insertPendingLock({ ...lockRow("a", "2", "tx1"), next_recovery_at: 7000, guarded_at: 4000 });
			expect(pendingTxRow(state, "tx1")).toEqual({ guarded_at: 4000, next_recovery_at: 5000 });
			store.insertPendingLock({ ...lockRow("a", "3", "tx1"), next_recovery_at: 3000, guarded_at: null });
			expect(pendingTxRow(state, "tx1")).toEqual({ guarded_at: 4000, next_recovery_at: 3000 });
			expect(store.listPendingTxItems("tx1").map((row) => row.guarded_at)).toEqual([4000, 4000, 4000]);
		});
	});

	it("quarantines a transaction once and excludes it from the stale scan", async () => {
		await withStore((store) => {
			store.insertPendingLock(lockRow("a", "1", "tx-guarded"));
			const guarded = [{ routingHash: P(kb("a")), hashKey: kb("a"), sortKey: kb("1") }];
			expect(store.guardPendingTx("tx-guarded", 2000)).toBe(true);
			expect(store.guardPendingTx("tx-guarded", 3000)).toBe(false);
			expect(store.listPendingTxItems("tx-guarded")[0].guarded_at).toBe(2000);
			expect(store.hasAnyPendingTx()).toBe(true);
			expect(store.earliestPendingTxRecoveryAt()).toBeNull();
			expect(store.listStalePendingTx(10_000, 10)).toEqual([]);

			// Only a forced resolution ends a quarantine, and it deletes the rows.
			store.deletePendingTxKeys("tx-guarded", guarded);
			expect(store.hasAnyPendingTx()).toBe(false);
		});
	});

	it("does not let ten quarantined transactions hide an unguarded stale transaction", async () => {
		await withStore((store) => {
			for (let i = 0; i < 11; i++) {
				const transactionId = `tx-${i}`;
				store.insertPendingLock(lockRow(`hk-${i}`, "1", transactionId));
				if (i < 10) {
					store.guardPendingTx(transactionId, 2000);
				}
			}
			expect(store.listStalePendingTx(10_000, 10)).toEqual([
				{ transaction_id: "tx-10", coordinator_json: '{"doName":"tc-1"}', created_at: 1000 },
			]);
		});
	});

	it("queryPendingTxPage orders by (routingHash, hk, sk, transaction_id) and resumes strictly after the cursor", async () => {
		await withStore((store) => {
			// The first hash key in storage order has two locks, and the other has one.
			const [first, second] =
				KeyCodec.compare(KeyCodec.asKeyBytes(P(kb("a"))), KeyCodec.asKeyBytes(P(kb("b")))) < 0 ? ["a", "b"] : ["b", "a"];
			store.insertPendingLock(lockRow(first, "1", "tx2"));
			store.insertPendingLock(lockRow(first, "1", "tx1"));
			store.insertPendingLock(lockRow(second, "1", "tx3"));
			const page1 = [...store.queryPendingTxPage(null, 2, ALL)];
			expect(page1.map((r) => r.transaction_id)).toEqual(["tx1", "tx2"]);
			const page2 = [...store.queryPendingTxPage({ routingHash: P(kb(first)), hk: kb(first), sk: kb("1"), transaction_id: "tx2" }, 2, ALL)];
			expect(page2.map((r) => r.transaction_id)).toEqual(["tx3"]);
		});
	});

	// transaction_id is the third primary key column, so these queries can only seek through
	// pending_transactions_transaction_id. Without it they scan the whole table on every commit and
	// abort. The results stay correct either way, so only the query plan catches the regression.
	it("whole-transaction queries seek on transaction_id", async () => {
		await withStore((_store, state) => {
			const plan = (sql: string) =>
				state.storage.sql
					.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${sql}`, "tx1")
					.toArray()
					.map((r) => r.detail)
					.join(" | ");

			for (const sql of [
				`SELECT 1 FROM pending_transactions WHERE transaction_id = ?`,
				`SELECT hk, sk FROM pending_transactions WHERE transaction_id = ?`,
				`SELECT hk, sk, operation, data, data_kind, ttl_epoch_utc_seconds FROM pending_transactions WHERE transaction_id = ?`,
			]) {
				// "SEARCH … (transaction_id=?)" is the seek. Asserting only the index name would also
				// pass for a full SCAN that happens to walk the same index.
				expect(plan(sql), sql).toContain("SEARCH");
				expect(plan(sql), sql).toContain("pending_transactions_transaction_id (transaction_id=?)");
			}
		});
	});

	// The stale-transaction job reads one index entry and one table row for each transaction, in the
	// order of the index, and it reads no guarded transaction. The deadline stops at the first entry.
	it("the stale scan and its deadline read the partial index of pending_tx_info", async () => {
		await withStore((_store, state) => {
			const plan = (sql: string, ...params: number[]) =>
				state.storage.sql
					.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${sql}`, ...params)
					.toArray()
					.map((r) => r.detail)
					.join(" | ");

			const deadline = plan(`SELECT next_recovery_at FROM pending_tx_info WHERE guarded_at IS NULL ORDER BY next_recovery_at LIMIT 1`);
			expect(deadline).toContain("USING INDEX pending_tx_info_due");
			expect(deadline).not.toContain("TEMP B-TREE");
			const scan = plan(
				`SELECT transaction_id, coordinator_json, created_at FROM pending_tx_info
				  WHERE guarded_at IS NULL AND next_recovery_at <= ? ORDER BY next_recovery_at LIMIT 1`,
				1,
			);
			expect(scan).toContain("SEARCH pending_tx_info USING INDEX pending_tx_info_due (next_recovery_at<?)");
			expect(scan).not.toContain("TEMP B-TREE");
		});
	});

	// The (hk, sk) tail of pending_transactions_transaction_id is what makes these two index-only now
	// that the table is a rowid table. While it was WITHOUT ROWID the primary key rode along in every
	// index entry and a narrow key covered them for free; narrowing the index back would silently put
	// a table fetch per row on the commit-and-abort path.
	it("the transaction_id key listing and count stay index-only", async () => {
		await withStore((_store, state) => {
			const plan = (sql: string) =>
				state.storage.sql
					.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${sql}`, "tx1")
					.toArray()
					.map((r) => r.detail)
					.join(" | ");

			expect(plan(`SELECT hk, sk FROM pending_transactions WHERE transaction_id = ?`)).toContain("COVERING INDEX");
			expect(plan(`SELECT COUNT(*) AS n FROM pending_transactions WHERE transaction_id = ?`)).toContain("COVERING INDEX");
		});
	});

	// pending_transactions must stay a rowid table: WITHOUT ROWID gives every row whose `data` exceeds
	// the ~1002-byte inline limit a private overflow page, measured at 3.1x physical against logical.
	// sqlite_master is the only thing that states this, and no behavioural test would notice.
	it("pending_transactions is a rowid table", async () => {
		await withStore((_store, state) => {
			const ddl =
				state.storage.sql
					.exec<{ sql: string }>(`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'pending_transactions'`)
					.toArray()[0]?.sql ?? "";
			expect(ddl).not.toContain("WITHOUT ROWID");
			// A rowid table does not imply NOT NULL from the primary key, so the key columns must say so.
			expect(ddl).toContain("hk                    BLOB    NOT NULL");
			expect(ddl).toContain("sk                    BLOB    NOT NULL");
			expect(ddl).toContain("transaction_id        TEXT    NOT NULL");
		});
	});
});

describe("PartitionStore - range split boundaries", () => {
	/**
	 * The sort keys of the boundaries that the split planner computes from the items of
	 * `compositeKeyFacts` and the facts of the bucket, or null. The partition holds one hash key, so each
	 * boundary has it.
	 */
	function computeBoundaries(
		store: PartitionStore,
		hk: KeyBytes,
		start: KeyBytes | null,
		end: KeyBytes | null,
		N: number,
	): KeyBytes[] | null {
		const edge = (sk: KeyBytes | null) => (sk === null ? null : { hk, sk });
		const model = store.itemStats.model;
		const total = estimate(model, store.itemStats.factsOf(P(hk)));
		const boundaries = planRangeBoundaries(model, total, N, edge(start), store.compositeKeyFacts(P(hk), edge(start), edge(end)));
		if (boundaries === null) {
			return null;
		}
		expect(boundaries.every((b) => KeyCodec.compare(b.hk, hk) === 0)).toBe(true);
		return boundaries.map((b) => b.sk);
	}

	function put(store: PartitionStore, hk: string, ...sks: string[]) {
		for (const sk of sks) {
			store.upsertItem({ routingHash: P(kb(hk)), hk: kb(hk), sk: kb(sk), data: "d", kind: "text", ttlAt: null, txOrderTs: 1 });
		}
	}

	it("returns null when fewer than N items exist", async () => {
		await withStore((store) => {
			put(store, "hk", "a");
			expect(computeBoundaries(store, kb("hk"), null, null, 2)).toBeNull();
		});
	});

	it("returns null when the hash key has no items", async () => {
		await withStore((store) => {
			expect(computeBoundaries(store, kb("hk"), null, null, 2)).toBeNull();
		});
	});

	it("N=2: cuts at the gap nearest the byte midpoint, and the lower child gets only the items before it", async () => {
		await withStore((store) => {
			// 4 items of almost the same estimate; the target is B/2. The gap after "cherry" holds about
			// B/2 before it, so the boundary lands between "cherry" and "mango".
			// shortestSeparator("cherry","mango"): 'c'!='m' at i=0 → "m".
			put(store, "hk", "apple", "cherry", "mango", "peach");
			expect(computeBoundaries(store, kb("hk"), null, null, 2)).toEqual([kb("m")]);
		});
	});

	it("returns boundary unchanged when no prefix shortening is possible (single-char keys)", async () => {
		await withStore((store) => {
			// predecessor="a", boundary="b" → shortestSeparator → "b" (already minimal)
			put(store, "hk", "a", "b");
			expect(computeBoundaries(store, kb("hk"), null, null, 2)).toEqual([kb("b")]);
		});
	});

	it("shortens a long common prefix (only last char differs)", async () => {
		await withStore((store) => {
			// predecessor="prefix_aaa", boundary="prefix_bbb"
			// shortestSeparator: first diff at i=7 ('a'!='b') → "prefix_b"
			put(store, "hk", "prefix_aaa", "prefix_bbb");
			expect(computeBoundaries(store, kb("hk"), null, null, 2)).toEqual([kb("prefix_b")]);
		});
	});

	it("shortens when predecessor is a prefix of the boundary", async () => {
		await withStore((store) => {
			// predecessor="app", boundary="apple" (predecessor is proper prefix)
			// shortestSeparator: loop exhausts at minLen=3, return "apple".substring(0,4) = "appl"
			put(store, "hk", "app", "apple");
			expect(computeBoundaries(store, kb("hk"), null, null, 2)).toEqual([kb("appl")]);
		});
	});

	it("N=3: produces two strictly-increasing shortened byte-quantile boundaries", async () => {
		await withStore((store) => {
			// 6 roughly-equal items; the targets are B/3 and 2·B/3. sorted: "aardvark","cherry","mango","strawberry","vanilla","zebra"
			// b1: the gap after "cherry" is nearest B/3 → shortestSeparator("cherry","mango") → "m"
			// b2: the gap after "strawberry" is nearest 2·B/3 → shortestSeparator("strawberry","vanilla") → "v"
			put(store, "hk", "aardvark", "cherry", "mango", "strawberry", "vanilla", "zebra");
			expect(computeBoundaries(store, kb("hk"), null, null, 3)).toEqual([kb("m"), kb("v")]);
		});
	});

	it("honors explicit start/end range bounds on the scan", async () => {
		await withStore((store) => {
			// A splitting range parent owns exactly its [start, end) slice, so its items == the slice and
			// the bucket holds the facts of the slice. Here the DO holds "banana","cherry","mango" and splits
			// [banana, peach). N=2: "mango" is the smallest item, so the gap after "banana" is nearest B/2
			// → shortestSeparator("banana","cherry") → "c".
			put(store, "hk", "banana", "cherry", "mango");
			expect(computeBoundaries(store, kb("hk"), kb("banana"), kb("peach"), 2)).toEqual([kb("c")]);
		});
	});

	it("returns null when range slice has fewer than N items", async () => {
		await withStore((store) => {
			put(store, "hk", "apple", "banana", "cherry", "mango", "peach");
			// Range [cherry, mango) → only "cherry" qualifies (mango excluded) — 1 item < N=2
			expect(computeBoundaries(store, kb("hk"), kb("cherry"), kb("mango"), 2)).toBeNull();
		});
	});

	it("handles astral (U+FFFF vs emoji) and binary sort keys in byte order", async () => {
		// The canonical byte-order divergence: UTF-8 for U+FFFF is EF BF BF (3 bytes),
		// UTF-8 for 😀 (U+1F600) is F0 9F 98 80 (4 bytes, leading F0 > EF).
		// KeyCodec.compare and SQLite BLOB ORDER BY must both place "￿" before "😀".
		// This was broken by the old UTF-16 shortestSeparator and is now a regression guard.
		const uFFFF = "￿"; // U+FFFF, UTF-8: EF BF BF
		const emoji = "😀"; // U+1F600, UTF-8: F0 9F 98 80
		const bin = new Uint8Array([0x01, 0x02]); // binary key, 0xFF-tagged → FF 01 02

		await withStore(async (store) => {
			// Insert in a non-sorted order so the DB sort is doing real work.
			for (const sk of [emoji, bin, uFFFF]) {
				store.upsertItem({
					routingHash: P(kb("hk")),
					hk: kb("hk"),
					sk: kb(sk),
					data: new Uint8Array(0),
					kind: "bytes",
					ttlAt: null,
					txOrderTs: 0,
				});
			}
			// N=2 → 1 boundary between the 3 items in byte order: uFFFF < emoji < binary
			// Boundary should lie between uFFFF and emoji (both are sort keys).
			const boundaries = computeBoundaries(store, kb("hk"), null, null, 2);
			expect(boundaries).not.toBeNull();
			expect(boundaries!.length).toBe(1);
			// Boundary must satisfy: encode(uFFFF) < boundary <= encode(emoji)
			expect(KeyCodec.compare(kb(uFFFF), boundaries![0])).toBeLessThan(0);
			expect(KeyCodec.compare(boundaries![0], kb(emoji))).toBeLessThanOrEqual(0);
		});
	});

	// Groups known sorted keys into the N children defined by `boundaries` and returns the count per child.
	// Child i owns [b_{i-1}, b_i); mirrors the byte-space [start, end) routing the migration scans use.
	function childCounts(sortedKeys: string[], boundaries: KeyBytes[]): number[] {
		const counts = Array.from({ length: boundaries.length + 1 }, () => 0);
		for (const key of sortedKeys) {
			let child = boundaries.length; // last child unless an earlier boundary claims it
			for (let i = 0; i < boundaries.length; i++) {
				if (KeyCodec.compare(kb(key), boundaries[i]) < 0) {
					child = i;
					break;
				}
			}
			counts[child]++;
		}
		return counts;
	}

	it("uniform rows: byte-balanced children are non-empty and roughly equal, boundaries strictly increasing", async () => {
		await withStore((store) => {
			// 40 equal-size rows → with N=4 each child should get ~10; byte-balance ≈ count-balance here.
			const keys = Array.from({ length: 40 }, (_, i) => `k${String(i).padStart(3, "0")}`);
			for (const sk of keys) {
				store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb(sk), data: "payload", kind: "text", ttlAt: null, txOrderTs: 1 });
			}
			const boundaries = computeBoundaries(store, kb("hk"), null, null, 4);
			expect(boundaries).not.toBeNull();
			expect(boundaries!.length).toBe(3);
			// Strictly increasing.
			for (let i = 1; i < boundaries!.length; i++) {
				expect(KeyCodec.compare(boundaries![i - 1], boundaries![i])).toBeLessThan(0);
			}
			// Every child non-empty and within a loose band of the ideal 10.
			const counts = childCounts(keys, boundaries!);
			expect(counts.length).toBe(4);
			for (const c of counts) {
				expect(c).toBeGreaterThanOrEqual(5);
				expect(c).toBeLessThanOrEqual(15);
			}
			expect(counts.reduce((a, b) => a + b, 0)).toBe(40);
		});
	});

	it("one heavy row: the heavy row is isolated into its own child", async () => {
		await withStore((store) => {
			// 10 light rows plus one heavy row (data far larger than the light rows' combined bytes),
			// keyed to sort last. With N=2, the target B/2 is below the heavy weight, so the gap before
			// the heavy row is nearest it → boundary lands between them.
			for (let i = 0; i < 10; i++) {
				store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb(`k${i}`), data: "x", kind: "text", ttlAt: null, txOrderTs: 1 });
			}
			store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("zheavy"),
				data: "H".repeat(5000),
				kind: "text",
				ttlAt: null,
				txOrderTs: 1,
			});
			const boundaries = computeBoundaries(store, kb("hk"), null, null, 2);
			expect(boundaries).not.toBeNull();
			expect(boundaries!.length).toBe(1);
			// Boundary sits above the last light key and at/below the heavy key: lights in child 0, heavy alone in child 1.
			expect(KeyCodec.compare(kb("k9"), boundaries![0])).toBeLessThan(0);
			expect(KeyCodec.compare(boundaries![0], kb("zheavy"))).toBeLessThanOrEqual(0);
		});
	});

	it("a dominant row between two light rows gets a child of its own", async () => {
		await withStore((store) => {
			// With N=3 the targets are B/3 and 2·B/3. The gap after "a" is nearest B/3, and the only gap
			// left for 2·B/3 is the gap after "m". Each child holds one row.
			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("a"), data: "x", kind: "text", ttlAt: null, txOrderTs: 1 });
			store.upsertItem({
				routingHash: P(kb("hk")),
				hk: kb("hk"),
				sk: kb("m"),
				data: "H".repeat(5000),
				kind: "text",
				ttlAt: null,
				txOrderTs: 1,
			});
			store.upsertItem({ routingHash: P(kb("hk")), hk: kb("hk"), sk: kb("z"), data: "x", kind: "text", ttlAt: null, txOrderTs: 1 });
			expect(computeBoundaries(store, kb("hk"), null, null, 3)).toEqual([kb("m"), kb("z")]);
		});
	});
});

describe("PartitionStore - migration pages", () => {
	const EMPTY = KeyCodec.encodeOptional(undefined);

	function lockRow(hk: KeyBytes, sk: KeyBytes, transactionId: string): PendingTxItem {
		return {
			routingHash: P(hk),
			hk,
			sk,
			transaction_id: transactionId,
			transaction_ts: 123,
			operation: "put",
			op_list: [[0, "put"]],
			data: "d",
			kind: "text",
			ttl_epoch_utc_seconds: null,
			coordinator_json: '{"doName":"tc-1"}',
			created_at: 1000,
			guarded_at: null,
			next_recovery_at: 6000,
		};
	}

	const pairs = (rows: Iterable<{ hk: KeyBytes; sk: KeyBytes }>) => [...rows].map((r) => `${keyLabel(r.hk)}/${keyLabel(r.sk)}`);
	/** The rows of one hash key in `[start, end)`, as a range target reads them. */
	const keyRange = (hk: string, start: KeyBytes | null, end: KeyBytes | null) => keyInterval(kb(hk), start, end);
	const itemCursor = (hk: string, sk: KeyBytes): StorageCursor => ({ routingHash: P(kb(hk)), hk: kb(hk), sk });
	/** The hash keys in storage order: by routingHash, then by key. */
	const inStorageOrder = (hks: string[]) =>
		[...hks].sort(
			(a, b) => KeyCodec.compare(KeyCodec.asKeyBytes(P(kb(a))), KeyCodec.asKeyBytes(P(kb(b)))) || KeyCodec.compare(kb(a), kb(b)),
		);

	// A cursor or a bound must SEEK. A statement that seeks only on the lower bound returns the same
	// rows, so only the plan finds the regression: each page then reads all the rows before it again.
	it("each page statement seeks on its lower bound or cursor, and on its upper bound", async () => {
		await withStore((_store, state) => {
			const plan = ({ sql, params }: { sql: string; params: unknown[] }) =>
				state.storage.sql
					.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${sql}`, ...params)
					.toArray()
					.map((r) => r.detail)
					.join(" | ");
			const closed = keyRange("h", kb("a"), kb("z"));
			const open: KeyRange = { start: { routingHash: P(kb("h")) }, end: null };
			const cursor = itemCursor("h", kb("b"));
			const lockCursor = { ...cursor, transaction_id: "tx" };
			const items = "SEARCH items USING PRIMARY KEY";
			const locks = "SEARCH p USING INDEX sqlite_autoindex_pending_transactions_1";
			const outbox = "SEARCH materialization_outbox USING INDEX sqlite_autoindex_materialization_outbox_1";
			const info = " | SEARCH t USING INDEX sqlite_autoindex_pending_tx_info_1 (transaction_id=?)";
			const both = "((routing_hash,hk,sk)>(?,?,?) AND (routing_hash,hk,sk)<(?,?,?))";
			const lower = "((routing_hash,hk,sk)>(?,?,?))";

			expect(plan(itemsPageStatement(null, 5, closed))).toBe(`${items} ${both}`);
			expect(plan(itemsPageStatement(cursor, 5, closed))).toBe(`${items} ${both}`);
			expect(plan(itemsPageStatement(cursor, 5, open))).toBe(`${items} ${lower}`);
			expect(plan(outboxPageStatement(null, 5, closed))).toBe(`${outbox} ${both}`);
			expect(plan(outboxPageStatement(cursor, 5, closed))).toBe(`${outbox} ${both}`);
			expect(plan(pendingTxPageStatement(null, 5, closed))).toBe(`${locks} ${both}${info}`);
			expect(plan(pendingTxPageStatement(lockCursor, 5, closed))).toBe(
				`${locks} ((routing_hash,hk,sk,transaction_id)>(?,?,?,?) AND (routing_hash,hk,sk)<(?,?,?))${info}`,
			);
		});
	});

	// The plan proves the seek. This test proves the cost: a page reads the rows that it returns and
	// the one row after them, and no row of the other keys or of the sort keys above the range.
	it("a range page reads only the rows that it returns", async () => {
		await withStore((store, state) => {
			const sk = (i: number) => kb(`k${String(i).padStart(4, "0")}`);
			for (const hk of ["a", "b", "c"]) {
				for (let i = 0; i < (hk === "b" ? 600 : 500); i++) {
					store.upsertItem({ routingHash: P(kb(hk)), hk: kb(hk), sk: sk(i), data: "d", kind: "text", ttlAt: null, txOrderTs: 1 });
					store.insertPendingLock(lockRow(kb(hk), sk(i), `tx-${hk}-${i}`));
				}
			}
			const range = keyRange("b", sk(50), sk(100));
			const run = ({ sql, params }: { sql: string; params: unknown[] }) => {
				const cursor = state.storage.sql.exec(sql, ...params);
				const returned = cursor.toArray().length;
				return { returned, rowsRead: cursor.rowsRead };
			};

			for (const statement of [itemsPageStatement(null, 1_000, range), itemsPageStatement(itemCursor("b", sk(60)), 1_000, range)]) {
				const { returned, rowsRead } = run(statement);
				expect(returned).toBeGreaterThan(0);
				// The seek reads a few rows at its bounds and at the cursor, past the rows that it returns.
				expect(rowsRead).toBeLessThanOrEqual(returned + 3);
			}
			// The JOIN reads one `pending_tx_info` row for each lock that it returns.
			{
				const { returned, rowsRead } = run(pendingTxPageStatement(null, 1_000, range));
				expect(returned).toBe(50);
				expect(rowsRead).toBeLessThanOrEqual(2 * returned + 2);
			}
			{
				const { returned, rowsRead } = run(pendingTxPageStatement({ ...itemCursor("b", sk(60)), transaction_id: "tx-b-60" }, 1_000, range));
				expect(returned).toBe(39);
				expect(rowsRead).toBeLessThanOrEqual(2 * returned + 3);
			}
		});
	});

	it("a key range page returns only the rows of its hash key in [start, end), and continues after its cursor", async () => {
		await withStore((store) => {
			for (const [hk, sk] of [
				["a", EMPTY],
				["a", kb("m")],
				["b", EMPTY],
				["b", kb("f")],
				["b", kb("m")],
				["b", kb("t")],
				["b\0", EMPTY],
				["c", EMPTY],
			] as const) {
				store.upsertItem({ routingHash: P(kb(hk)), hk: kb(hk), sk, data: "d", kind: "text", ttlAt: null, txOrderTs: 1 });
			}
			expect(pairs(store.queryItemsPage(null, 10, keyRange("b", null, null)))).toEqual(["b/", "b/f", "b/m", "b/t"]);
			expect(pairs(store.queryItemsPage(null, 10, keyRange("b", kb("f"), kb("t"))))).toEqual(["b/f", "b/m"]);
			expect(pairs(store.queryItemsPage(null, 10, keyRange("b", null, kb("f"))))).toEqual(["b/"]);
			expect(pairs(store.queryItemsPage(itemCursor("b", EMPTY), 10, keyRange("b", null, null)))).toEqual(["b/f", "b/m", "b/t"]);
			expect(pairs(store.queryItemsPage(itemCursor("b", kb("f")), 10, keyRange("b", kb("f"), kb("t"))))).toEqual(["b/m"]);
		});
	});

	it("a routingHash range page returns the rows of each routingHash in [start, end) in storage order, and continues after its cursor", async () => {
		await withStore((store) => {
			const hks = inStorageOrder(["a", "b", "c", "d", "e", "f", "g", "h"]);
			for (const hk of hks) {
				for (const sk of [EMPTY, kb("x")]) {
					store.upsertItem({ routingHash: P(kb(hk)), hk: kb(hk), sk, data: "d", kind: "text", ttlAt: null, txOrderTs: 1 });
				}
			}
			const rowsOf = (keys: string[]) => keys.flatMap((hk) => [`${hk}/`, `${hk}/x`]);
			const interval: KeyRange = { start: { routingHash: P(kb(hks[2])) }, end: { routingHash: P(kb(hks[5])) } };
			expect(pairs(store.queryItemsPage(null, 100, interval))).toEqual(rowsOf(hks.slice(2, 5)));
			expect(pairs(store.queryItemsPage(itemCursor(hks[3], EMPTY), 100, interval))).toEqual([`${hks[3]}/x`, ...rowsOf([hks[4]])]);
			// A null end has no upper bound.
			expect(pairs(store.queryItemsPage(null, 100, { start: interval.start, end: null }))).toEqual(rowsOf(hks.slice(2)));
		});
	});

	it("a key range page of locks returns only the locks of its hash key in [start, end), and continues after its cursor", async () => {
		await withStore((store) => {
			for (const [hk, sk, tx] of [
				["a", kb("m"), "tx1"],
				["b", EMPTY, "tx1"],
				["b", kb("f"), "tx2"],
				["b", kb("m"), "tx3"],
				["b", kb("t"), "tx4"],
				["c", EMPTY, "tx5"],
			] as const) {
				store.insertPendingLock(lockRow(kb(hk), sk, tx));
			}
			expect(pairs(store.queryPendingTxPage(null, 10, keyRange("b", null, null)))).toEqual(["b/", "b/f", "b/m", "b/t"]);
			expect(pairs(store.queryPendingTxPage(null, 10, keyRange("b", kb("f"), kb("t"))))).toEqual(["b/f", "b/m"]);
			expect(
				pairs(store.queryPendingTxPage({ ...itemCursor("b", kb("f")), transaction_id: "tx2" }, 10, keyRange("b", kb("f"), null))),
			).toEqual(["b/m", "b/t"]);
		});
	});

	// Two transactions lock the same key. With a page of one row, the page boundary falls between them,
	// so the cursor must compare the transaction id too, or the second lock is lost.
	it("a page of locks keeps two locks of one key that a page boundary separates", async () => {
		await withStore((store) => {
			store.insertPendingLock(lockRow(kb("b"), kb("m"), "tx2"));
			store.insertPendingLock(lockRow(kb("b"), kb("m"), "tx1"));
			store.insertPendingLock(lockRow(kb("b"), kb("t"), "tx3"));
			const range = keyRange("b", null, null);
			const seen: string[] = [];
			let cursor: PendingTransactionCursor | null = null;
			for (;;) {
				const page: PendingTxItem[] = [...store.queryPendingTxPage(cursor, 1, range)];
				if (page.length === 0) {
					break;
				}
				const row = page[0];
				seen.push(`${keyLabel(row.sk)}/${row.transaction_id}`);
				cursor = { routingHash: row.routingHash, hk: row.hk, sk: row.sk, transaction_id: row.transaction_id };
			}
			expect(seen).toEqual(["m/tx1", "m/tx2", "t/tx3"]);
		});
	});
});

describe("PartitionStore - materialization outbox", () => {
	type OutboxRow = { outbox_id: number; revision: number; is_deleted: number; is_local_only: number; next_attempt_at: number };

	async function withCaptureStore(capture: boolean, fn: (store: PartitionStore, state: DurableObjectState) => void): Promise<void> {
		const stub = testPartitionStub(`store-outbox-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
			fn(new PartitionStore(state.storage, { captureChanges: () => capture }), state);
		});
	}
	function outboxRow(state: DurableObjectState, hk: string, sk = "s"): OutboxRow | undefined {
		return state.storage.sql
			.exec<OutboxRow>(
				`SELECT outbox_id, revision, is_deleted, is_local_only, next_attempt_at FROM materialization_outbox WHERE hk = ? AND sk = ?`,
				kb(hk),
				kb(sk),
			)
			.toArray()[0];
	}
	function outboxCount(state: DurableObjectState): number {
		return state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM materialization_outbox`).one().n;
	}
	/** The export of a row: no receiver can be sure to lack it any more. */
	function claim(state: DurableObjectState, hk: string): void {
		state.storage.sql.exec(`UPDATE materialization_outbox SET is_local_only = 0, next_attempt_at = 99 WHERE hk = ?`, kb(hk));
	}
	const put = (store: PartitionStore, hk: string, ttlAt: number | null = null) =>
		store.upsertItem({ routingHash: P(kb(hk)), hk: kb(hk), sk: kb("s"), data: "d", kind: "text", ttlAt, txOrderTs: 1 }).version;
	const del = (store: PartitionStore, hk: string) => store.deleteItem({ routingHash: P(kb(hk)), hk: kb(hk), sk: kb("s"), txOrderTs: 1 });

	it("writes no outbox row when capture is off", async () => {
		await withCaptureStore(false, (store, state) => {
			put(store, "x");
			put(store, "x");
			del(store, "x");
			expect(outboxCount(state)).toBe(0);
		});
	});

	it("a create is local only, an update keeps the row, and a delete before the export removes it", async () => {
		await withCaptureStore(true, (store, state) => {
			expect(put(store, "x")).toBe(1);
			const created = outboxRow(state, "x")!;
			expect(created).toMatchObject({ revision: 1, is_deleted: 0, is_local_only: 1, next_attempt_at: 0 });

			expect(put(store, "x")).toBe(2);
			expect(outboxRow(state, "x")).toEqual({ ...created, revision: 2 });

			expect(del(store, "x").deleteVersion).toBe(3);
			expect(outboxRow(state, "x")).toBeUndefined();
		});
	});

	it("a delete after the export writes a tombstone at v + 1, and a recreate keeps the row and its outbox_id", async () => {
		await withCaptureStore(true, (store, state) => {
			put(store, "x");
			claim(state, "x");
			const { outbox_id } = outboxRow(state, "x")!;

			expect(del(store, "x").deleteVersion).toBe(2);
			expect(outboxRow(state, "x")).toEqual({ outbox_id, revision: 2, is_deleted: 1, is_local_only: 0, next_attempt_at: 0 });

			expect(put(store, "x")).toBe(3);
			expect(outboxRow(state, "x")).toEqual({ outbox_id, revision: 3, is_deleted: 0, is_local_only: 0, next_attempt_at: 0 });
		});
	});

	it("an update of an item with no outbox row writes a row that is not local only", async () => {
		await withCaptureStore(true, (store, state) => {
			put(store, "x");
			// The export acknowledged revision 1 and deleted the row.
			state.storage.sql.exec(`DELETE FROM materialization_outbox`);
			put(store, "x");
			expect(outboxRow(state, "x")).toMatchObject({ revision: 2, is_deleted: 0, is_local_only: 0 });
			// A receiver can hold revision 1, so the delete must reach it.
			del(store, "x");
			expect(outboxRow(state, "x")).toMatchObject({ revision: 3, is_deleted: 1, is_local_only: 0 });
		});
	});

	it("updateItemSingleShot captures a create as local only and an update with its version", async () => {
		await withCaptureStore(true, (store, state) => {
			const plan = updateTree([{ action: "set", target: { ref: "data", path: "$.x" }, value: { val: 1 } }]);
			store.updateItemSingleShot({ routingHash: P(kb("x")), hk: kb("x"), sk: kb("s"), plan, txOrderTs: 1 });
			expect(outboxRow(state, "x")).toMatchObject({ revision: 1, is_deleted: 0, is_local_only: 1 });
			claim(state, "x");
			store.updateItemSingleShot({ routingHash: P(kb("x")), hk: kb("x"), sk: kb("s"), plan, txOrderTs: 2 });
			expect(outboxRow(state, "x")).toMatchObject({ revision: 2, is_deleted: 0, is_local_only: 0, next_attempt_at: 0 });
		});
	});

	it("a delete of an absent item writes no outbox row", async () => {
		await withCaptureStore(true, (store, state) => {
			store.deleteItem({ routingHash: P(kb("x")), hk: kb("x"), sk: kb("s"), txOrderTs: 1, bumpTxOrderTsAlways: true });
			del(store, "y");
			expect(outboxCount(state)).toBe(0);
		});
	});

	it("the TTL sweep writes a tombstone for an exported row and removes a local only row", async () => {
		await withCaptureStore(true, (store, state) => {
			put(store, "exported", 10);
			put(store, "local", 10);
			claim(state, "exported");
			expect(store.deleteExpiredItems(100, 10, () => true).deletedRows).toBe(2);
			expect(outboxRow(state, "exported")).toMatchObject({ revision: 2, is_deleted: 1, is_local_only: 0, next_attempt_at: 0 });
			expect(outboxRow(state, "local")).toBeUndefined();
		});
	});

	it("pages the outbox rows of an interval, and the copy is due and not local only", async () => {
		await withCaptureStore(true, (store, state) => {
			for (const hk of ["a", "b", "c"]) {
				put(store, hk);
			}
			claim(state, "b");
			del(store, "b");
			const range = [...store.queryOutboxPage(null, 10, keyInterval(kb("b"), null, null))];
			expect(range).toEqual([{ routingHash: P(kb("b")), hk: kb("b"), sk: kb("s"), revision: 2, is_deleted: 1 }]);
			expect([...store.queryOutboxPage(null, 10, ALL)].map((row) => keyLabel(row.hk)).sort()).toEqual(["a", "b", "c"]);

			// The copy on a child: a retried page keeps the first row.
			state.storage.sql.exec(`DELETE FROM materialization_outbox`);
			store.insertMigratedOutboxRow({ routingHash: P(kb("b")), hk: kb("b"), sk: kb("s"), revision: 2, is_deleted: 1 });
			store.insertMigratedOutboxRow({ routingHash: P(kb("b")), hk: kb("b"), sk: kb("s"), revision: 1, is_deleted: 0 });
			expect(outboxRow(state, "b")).toMatchObject({ revision: 2, is_deleted: 1, is_local_only: 0, next_attempt_at: 0 });
		});
	});

	it("deletes the outbox copies of one hash key or of all keys in bounded batches", async () => {
		await withCaptureStore(true, (store, state) => {
			for (const sk of ["1", "2", "3"]) {
				store.upsertItem({ routingHash: P(kb("moved")), hk: kb("moved"), sk: kb(sk), data: "d", kind: "text", ttlAt: null, txOrderTs: 1 });
			}
			put(store, "stays");
			expect(store.deleteOutboxCopiesBatch(P(kb("moved")), 2)).toBe(false);
			expect(store.deleteOutboxCopiesBatch(P(kb("moved")), 2)).toBe(true);
			expect(outboxCount(state)).toBe(1);
			expect(store.deleteOutboxCopiesBatch(null, 1)).toBe(true);
			expect(outboxCount(state)).toBe(0);
		});
	});
});
describe("PartitionStore - the item-id counter", () => {
	const put = (store: PartitionStore, hk: string, sk = "s", ttlAt: number | null = null) =>
		store.upsertItem({ routingHash: P(kb(hk)), hk: kb(hk), sk: kb(sk), data: "d", kind: "text", ttlAt, txOrderTs: 1 });
	const idOf = (state: DurableObjectState, hk: string, sk = "s") =>
		state.storage.sql
			.exec<{ item_id: number }>(`SELECT item_id FROM items WHERE routing_hash = ? AND hk = ? AND sk = ?`, P(kb(hk)), kb(hk), kb(sk))
			.one().item_id;
	const highWaterMark = (state: DurableObjectState) =>
		state.storage.sql.exec<{ hwm: number }>(`SELECT hwm FROM item_id_counter WHERE id = 1`).one().hwm;

	it("an overwrite keeps the id, and a new row after a delete gets a higher id", async () => {
		await withStore((store, state) => {
			put(store, "a");
			put(store, "b");
			const a = idOf(state, "a");
			const b = idOf(state, "b");
			expect(b).toBeGreaterThan(a);
			put(store, "a");
			expect(idOf(state, "a")).toBe(a);
			// The row with the highest id goes, and its id never comes back.
			store.deleteItem({ routingHash: P(kb("b")), hk: kb("b"), sk: kb("s"), txOrderTs: 2 });
			put(store, "b");
			expect(idOf(state, "b")).toBeGreaterThan(b);
			store.deleteItem({ routingHash: P(kb("a")), hk: kb("a"), sk: kb("s"), txOrderTs: 3 });
			put(store, "a");
			expect(idOf(state, "a")).toBeGreaterThan(idOf(state, "b"));
		});
	});

	it("reserves a block of 1024 ids with one row written, and a new store reserves above it", async () => {
		await withStore((store, state) => {
			expect(highWaterMark(state)).toBe(1);
			put(store, "a");
			expect(idOf(state, "a")).toBe(1);
			expect(highWaterMark(state)).toBe(1025);
			put(store, "b");
			expect(highWaterMark(state)).toBe(1025);
			// A restart loses the rest of the block, and the next row takes an id above it.
			const restarted = new PartitionStore(state.storage);
			put(restarted, "c");
			expect(idOf(state, "c")).toBe(1025);
			expect(highWaterMark(state)).toBe(2049);
		});
	});

	// A reservation that rolls back must not stay in memory: the ids of that block are not reserved in
	// storage, and a later reservation, also by another instance, gives them out again.
	it("gives no id two times after a transaction that rolls back its reservation", async () => {
		await withStore((store, state) => {
			expect(() =>
				store.transactionSync(() => {
					put(store, "rolled-back");
					throw new Error("roll back");
				}),
			).toThrow("roll back");
			expect(highWaterMark(state)).toBe(1);
			put(store, "a");
			put(store, "b");
			const restarted = new PartitionStore(state.storage);
			put(restarted, "c");
			put(restarted, "d");
			const ids = ["a", "b", "c", "d"].map((hk) => idOf(state, hk));
			expect(new Set(ids).size).toBe(4);
		});
	});

	it("an ingest raises the mark above its copied ids, and the next row takes an id above them", async () => {
		await withStore((store, state) => {
			put(store, "local");
			store.insertItemIfAbsent({
				item_id: linkId(5_000),
				routingHash: P(kb("copied")),
				hk: kb("copied"),
				sk: kb("s"),
				data: "d",
				kind: "text",
				ttl_epoch_utc_seconds: null,
				v: 1,
				last_read_ts: 1,
				last_write_ts: 1,
			});
			store.raiseItemIdMark(5_000);
			expect(highWaterMark(state)).toBe(5_001);
			put(store, "next");
			expect(idOf(state, "next")).toBe(5_001);
			// A lower mark does not lower it.
			store.raiseItemIdMark(10);
			expect(highWaterMark(state)).toBe(6_025);
		});
	});

	// One row for the table and one for `idx_items_item_id`, and one more for `idx_items_ttl` when the
	// row has a TTL. The reservation of a block is a statement of its own.
	it("an insert writes 2 rows without a TTL and 3 with a TTL", async () => {
		await withStore((store) => {
			put(store, "reserves-the-block");
			expect(put(store, "a").rowsWritten).toBe(2);
			expect(put(store, "b", "s", 2_000_000_000).rowsWritten).toBe(3);
		});
	});
});

describe("PartitionStore - plans of the storage order", () => {
	const plan = (state: DurableObjectState, sql: string, ...params: unknown[]) =>
		state.storage.sql
			.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${sql}`, ...params)
			.toArray()
			.map((r) => r.detail)
			.join(" | ");

	it("the promotion delete uses the primary key", async () => {
		await withStore((_store, state) => {
			const hk = kb("hk");
			const deletePlan = plan(
				state,
				`DELETE FROM items WHERE routing_hash = ?1 AND hk = ?2
				    AND sk IN (SELECT sk FROM items WHERE routing_hash = ?1 AND hk = ?2 ORDER BY sk LIMIT ?3)`,
				P(hk),
				hk,
				10,
			);
			expect(deletePlan).toContain("SEARCH items USING PRIMARY KEY (routing_hash=? AND hk=? AND sk=?)");
			expect(deletePlan).not.toMatch(/SCAN items/);
		});
	});

	it("the point reads use the primary key", async () => {
		await withStore((_store, state) => {
			const hk = kb("hk");
			for (const sql of [
				`SELECT data, data_kind, v FROM items WHERE routing_hash = ? AND hk = ? AND sk = ? LIMIT 1`,
				`DELETE FROM items WHERE routing_hash = ? AND hk = ? AND sk = ?`,
			]) {
				expect(plan(state, sql, P(hk), hk, kb("s"))).toBe("SEARCH items USING PRIMARY KEY (routing_hash=? AND hk=? AND sk=?)");
			}
		});
	});
});
