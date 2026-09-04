import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../../server/do-partition.js";
import { compileUpdateExpression } from "../expression/compiler.js";
import type { UpdateExpression } from "../expression/types.js";
import { type KeyBytes, KeyCodec } from "../partition-topology/key-codec.js";
import { PartitionStore } from "./partition-store.js";
import { EST_ROW_BYTES_K } from "./item-size.js";
import { MAX_ITEM_BYTES } from "../transaction-limits.js";

const kb = (s: string | Uint8Array) => KeyCodec.encode(s);

// Mirrors items.est_row_bytes: octet_length(data)+octet_length(hk)+octet_length(sk)+K. octet_length is
// UTF-8 bytes for text and byteLength for blobs; keys are already canonical bytes. est_row_bytes is a
// plain column written by two statements, so these tests are what keeps the formula from drifting.
function expectedRowBytes(data: string | Uint8Array, hk: KeyBytes, sk: KeyBytes): number {
	const dataBytes = typeof data === "string" ? new TextEncoder().encode(data).length : data.byteLength;
	return dataBytes + hk.byteLength + sk.byteLength + EST_ROW_BYTES_K;
}

// Runs `fn` against a PartitionStore over REAL Durable Object storage (vitest-pool-workers).
// The PartitionDO constructor has already run the schema migrations by the time the callback runs;
// constructing a second PartitionStore over the same storage is safe (migrations are idempotent).
async function withStore(fn: (store: PartitionStore, state: DurableObjectState) => void | Promise<void>): Promise<void> {
	const stub = PartitionDO.getByName(env.PARTITION_DO, `store-test.${crypto.randomUUID()}`);
	await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
		await fn(new PartitionStore(state.storage), state);
	});
}

function kseBytes(state: DurableObjectState, hk: string): number | undefined {
	return state.storage.sql.exec<{ est_bytes: number }>(`SELECT est_bytes FROM key_size_estimates WHERE hk = ?`, kb(hk)).toArray()[0]
		?.est_bytes;
}

describe("PartitionStore - items", () => {
	it("upsertItem inserts with version 1 and increments on every overwrite", async () => {
		await withStore((store) => {
			const first = store.upsertItem({
				hk: kb("hk"),
				sk: kb("sk"),
				data: "v1",
				kind: "text",
				ttlAt: null,
				lastTransactionTs: 1,
			});
			expect(first.version).toBe(1);
			const second = store.upsertItem({
				hk: kb("hk"),
				sk: kb("sk"),
				data: "v2",
				kind: "text",
				ttlAt: null,
				lastTransactionTs: 2,
			});
			expect(second.version).toBe(2);
			const third = store.upsertItem({
				hk: kb("hk"),
				sk: kb("sk"),
				data: "v3",
				kind: "text",
				ttlAt: null,
				lastTransactionTs: 3,
			});
			expect(third.version).toBe(3);
		});
	});

	it("upsertItem never lowers last_transaction_ts, but still applies the write", async () => {
		await withStore((store) => {
			// As if committed by a transaction whose coordinator clock ran ahead of this partition's.
			store.upsertItem({ hk: kb("hk"), sk: kb("sk"), data: "from-tx", kind: "text", ttlAt: null, lastTransactionTs: 5_000 });

			// A non-transactional put stamps the partition's own (lower) clock.
			const second = store.upsertItem({
				hk: kb("hk"),
				sk: kb("sk"),
				data: "from-put",
				kind: "text",
				ttlAt: null,
				lastTransactionTs: 1_000,
			});

			// The write lands in full — only the timestamp is held at the high-water mark.
			expect(second.version).toBe(2);
			expect(store.getItem(kb("hk"), kb("sk")).row).toMatchObject({ data: "from-put", last_transaction_ts: 5_000 });

			// A later timestamp still moves it forward.
			store.upsertItem({ hk: kb("hk"), sk: kb("sk"), data: "newer", kind: "text", ttlAt: null, lastTransactionTs: 9_000 });
			expect(store.getItem(kb("hk"), kb("sk")).row?.last_transaction_ts).toBe(9_000);
		});
	});

	it("getItem returns converted data, ttl, version, and last_transaction_ts", async () => {
		await withStore((store) => {
			store.upsertItem({ hk: kb("hk"), sk: kb("s"), data: "hello", kind: "text", ttlAt: 1234, lastTransactionTs: 42 });
			const str = store.getItem(kb("hk"), kb("s"));
			expect(str.row).toEqual({ data: "hello", kind: "text", ttl_epoch_utc_seconds: 1234, v: 1, last_transaction_ts: 42 });
			expect(str.rowsRead).toBe(1);

			const bin = new Uint8Array([1, 2, 3]);
			store.upsertItem({ hk: kb("hk"), sk: kb("b"), data: bin, kind: "bytes", ttlAt: null, lastTransactionTs: 0 });
			const got = store.getItem(kb("hk"), kb("b"));
			expect(got.row?.data).toBeInstanceOf(Uint8Array);
			expect(got.row?.data).toEqual(bin);

			expect(store.getItem(kb("hk"), kb("missing")).row).toBeUndefined();
		});
	});

	it("maintains key_size_estimates across put, overwrite, and delete", async () => {
		await withStore((store, state) => {
			const est1 = expectedRowBytes("aaaa", kb("hk"), kb("s1"));
			const r1 = store.upsertItem({
				hk: kb("hk"),
				sk: kb("s1"),
				data: "aaaa",
				kind: "text",
				ttlAt: null,
				lastTransactionTs: 1,
			});
			expect(r1.keyEstBytes).toBe(est1);
			expect(kseBytes(state, "hk")).toBe(est1);

			// Second sort key accumulates on the same hash key.
			const est2 = expectedRowBytes("bb", kb("hk"), kb("s2"));
			const r2 = store.upsertItem({ hk: kb("hk"), sk: kb("s2"), data: "bb", kind: "text", ttlAt: null, lastTransactionTs: 2 });
			expect(r2.keyEstBytes).toBe(est1 + est2);

			// Overwrite replaces the old row's contribution, not adds to it.
			const est1b = expectedRowBytes("aaaaaaaa", kb("hk"), kb("s1"));
			const r3 = store.upsertItem({
				hk: kb("hk"),
				sk: kb("s1"),
				data: "aaaaaaaa",
				kind: "text",
				ttlAt: null,
				lastTransactionTs: 3,
			});
			expect(r3.keyEstBytes).toBe(est1b + est2);

			// Delete removes its contribution.
			const del = store.deleteItem({ hk: kb("hk"), sk: kb("s1"), watermarkTs: 10 });
			expect(del.deleted).toBe(true);
			expect(kseBytes(state, "hk")).toBe(est2);
		});
	});

	it("rebuildKeySizeEstimates recomputes estimates from the rows", async () => {
		await withStore((store, state) => {
			store.upsertItem({ hk: kb("hk"), sk: kb("s1"), data: "xx", kind: "text", ttlAt: null, lastTransactionTs: 1 });
			store.upsertItem({ hk: kb("hk"), sk: kb("s2"), data: "yyyy", kind: "text", ttlAt: null, lastTransactionTs: 2 });
			// Corrupt the summary, then rebuild.
			state.storage.sql.exec(`UPDATE key_size_estimates SET est_bytes = 0 WHERE hk = ?`, kb("hk"));
			store.rebuildKeySizeEstimates();
			expect(kseBytes(state, "hk")).toBe(expectedRowBytes("xx", kb("hk"), kb("s1")) + expectedRowBytes("yyyy", kb("hk"), kb("s2")));
		});
	});

	it("rebuildKeySizeEstimates drops the estimate of a key that has no rows left", async () => {
		await withStore((store, state) => {
			store.upsertItem({ hk: kb("keeps"), sk: kb("s1"), data: "xx", kind: "text", ttlAt: null, lastTransactionTs: 1 });
			// A key whose rows are all gone. The estimate survives the deletes (deleteItem only
			// decrements it), so only the rebuild can remove the row.
			store.upsertItem({ hk: kb("empties"), sk: kb("s1"), data: "yyyy", kind: "text", ttlAt: null, lastTransactionTs: 2 });
			state.storage.sql.exec(`DELETE FROM items WHERE hk = ?`, kb("empties"));
			expect(kseBytes(state, "empties")).toBeGreaterThan(0);

			store.rebuildKeySizeEstimates();

			expect(kseBytes(state, "empties")).toBeUndefined();
			expect(kseBytes(state, "keeps")).toBe(expectedRowBytes("xx", kb("keeps"), kb("s1")));
		});
	});

	// The prune must seek items per estimate row. `hk NOT IN (SELECT hk FROM items)` would instead
	// materialise every hk in the table, doubling the work the refresh already did.
	it("the rebuild prune seeks items rather than scanning it", async () => {
		await withStore((store, state) => {
			store.upsertItem({ hk: kb("hk"), sk: kb("s1"), data: "xx", kind: "text", ttlAt: null, lastTransactionTs: 1 });
			const plan = state.storage.sql
				.exec<{ detail: string }>(
					`EXPLAIN QUERY PLAN DELETE FROM key_size_estimates
					 WHERE NOT EXISTS (SELECT 1 FROM items WHERE items.hk = key_size_estimates.hk)`,
				)
				.toArray()
				.map((r) => r.detail)
				.join(" | ");

			expect(plan).toContain("SEARCH items");
		});
	});

	it("queryItemsPage pages in (hk, sk) order and resumes strictly after the cursor", async () => {
		await withStore((store) => {
			for (const [hk, sk] of [
				["a", "1"],
				["a", "2"],
				["b", "1"],
			] as const) {
				store.upsertItem({ hk: kb(hk), sk: kb(sk), data: "d", kind: "text", ttlAt: null, lastTransactionTs: 1 });
			}
			const page1 = store.queryItemsPage(null, 2);
			expect(page1.map((r) => [KeyCodec.decode(r.hk), KeyCodec.decode(r.sk)])).toEqual([
				["a", "1"],
				["a", "2"],
			]);
			const page2 = store.queryItemsPage({ hk: kb("a"), sk: kb("2") }, 2);
			expect(page2.map((r) => [KeyCodec.decode(r.hk), KeyCodec.decode(r.sk)])).toEqual([["b", "1"]]);
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
				store.upsertItem({ hk, sk, data: "d", kind: "text", ttlAt: null, lastTransactionTs: 1 });
			}
			// Walk the whole table one row at a time, the way migration does.
			const seen: string[] = [];
			let cursor: { hk: KeyBytes; sk: KeyBytes } | null = null;
			for (;;) {
				const page = store.queryItemsPage(cursor, 1);
				if (page.length === 0) break;
				const row = page[0];
				seen.push(`${KeyCodec.decode(row.hk)}/${row.sk.byteLength === 0 ? "" : KeyCodec.decode(row.sk)}`);
				cursor = { hk: row.hk, sk: row.sk };
			}
			expect(seen).toEqual(["a/", "a/m", "b/", "c/q"]);
		});
	});

	// The keyset cursors must SEEK on the full key tuple. An `hk > ? OR (hk = ? AND sk > ?)` rewrite
	// still returns correct rows, so only the query plan catches the regression: SQLite would bind just
	// `hk>?` and re-check the rest, making every page O(rows already passed).
	it("keyset cursors seek on the whole key tuple", async () => {
		await withStore((_store, state) => {
			const plan = (sql: string, ...params: unknown[]) =>
				state.storage.sql
					.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${sql}`, ...params)
					.toArray()
					.map((r) => r.detail)
					.join(" | ");

			expect(plan(`SELECT hk, sk FROM items WHERE (hk, sk) > (?, ?) ORDER BY hk, sk LIMIT 1`, kb("a"), kb("1"))).toContain("(hk,sk)>(?,?)");
			expect(
				plan(
					`SELECT hk, sk FROM pending_transactions WHERE (hk, sk, transaction_id) > (?, ?, ?) ORDER BY hk, sk, transaction_id LIMIT 1`,
					kb("a"),
					kb("1"),
					"tx",
				),
			).toContain("(hk,sk,transaction_id)>(?,?,?)");
		});
	});

	it("stores json as JSONB, persists data_kind, and decodes to JSON text on read", async () => {
		await withStore((store, state) => {
			const jsonText = JSON.stringify({ a: 1, b: ["x", true, null] });
			store.upsertItem({ hk: kb("hk"), sk: kb("j"), data: jsonText, kind: "json", ttlAt: null, lastTransactionTs: 1 });

			// getItem decodes JSONB → JSON text and surfaces the kind.
			const got = store.getItem(kb("hk"), kb("j"));
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

	it("est_row_bytes equals octet_length(data)+octet_length(hk)+octet_length(sk)+K per kind", async () => {
		await withStore((store, state) => {
			const readEst = (sk: string) =>
				state.storage.sql.exec<{ e: number }>(`SELECT est_row_bytes AS e FROM items WHERE hk = ? AND sk = ?`, kb("hk"), kb(sk)).toArray()[0]
					?.e;
			const octetLen = (sk: string, data: string | Uint8Array) =>
				(typeof data === "string" ? new TextEncoder().encode(data).length : data.byteLength) +
				kb("hk").byteLength +
				kb(sk).byteLength +
				EST_ROW_BYTES_K;

			const multibyte = "héllo—✓"; // multi-byte UTF-8: octet_length > .length
			store.upsertItem({ hk: kb("hk"), sk: kb("t"), data: multibyte, kind: "text", ttlAt: null, lastTransactionTs: 1 });
			expect(readEst("t")).toBe(octetLen("t", multibyte));

			const bytes = new Uint8Array([1, 2, 3, 4, 5]);
			store.upsertItem({ hk: kb("hk"), sk: kb("b"), data: bytes, kind: "bytes", ttlAt: null, lastTransactionTs: 1 });
			expect(readEst("b")).toBe(octetLen("b", bytes));

			// For json, the stored JSONB blob size is what octet_length(data) measures — read it from SQL
			// rather than the input text (the two differ), then confirm the constant folds in.
			const jsonText = JSON.stringify({ hello: "world", n: 12345 });
			store.upsertItem({ hk: kb("hk"), sk: kb("j"), data: jsonText, kind: "json", ttlAt: null, lastTransactionTs: 1 });
			const jsonBlobLen = state.storage.sql
				.exec<{ n: number }>(`SELECT octet_length(data) AS n FROM items WHERE hk = ? AND sk = ?`, kb("hk"), kb("j"))
				.toArray()[0].n;
			expect(readEst("j")).toBe(jsonBlobLen + kb("hk").byteLength + kb("j").byteLength + EST_ROW_BYTES_K);
		});
	});

	it("insertItemIfAbsent writes the same est_row_bytes as upsertItem", async () => {
		await withStore((store, state) => {
			const readEst = (sk: string) =>
				state.storage.sql.exec<{ e: number }>(`SELECT est_row_bytes AS e FROM items WHERE hk = ? AND sk = ?`, kb("hk"), kb(sk)).toArray()[0]
					?.e;

			// The migration writer carries the formula independently of upsertItem — both must agree.
			const jsonText = JSON.stringify({ hello: "world", n: 12345 });
			store.upsertItem({ hk: kb("hk"), sk: kb("j"), data: jsonText, kind: "json", ttlAt: null, lastTransactionTs: 1 });
			const migrated = store.queryItemsPage(null, 10)[0];
			store.insertItemIfAbsent({ ...migrated, sk: kb("j2") });
			// Same row under a longer sk: the only difference must be octet_length(sk).
			expect(readEst("j2")).toBe(readEst("j")! - kb("j").byteLength + kb("j2").byteLength);

			const bytes = new Uint8Array([1, 2, 3, 4, 5]);
			store.insertItemIfAbsent({
				hk: kb("hk"),
				sk: kb("b"),
				data: bytes,
				kind: "bytes",
				ttl_epoch_utc_seconds: null,
				v: 1,
				last_transaction_ts: 1,
			});
			expect(readEst("b")).toBe(bytes.byteLength + kb("hk").byteLength + kb("b").byteLength + EST_ROW_BYTES_K);
		});
	});

	// idx_items_scan is what keeps computeRangeSplitBoundaries and rebuildKeySizeEstimates off the wide
	// item rows. A generated est_row_bytes column, or a narrower index, silently loses "COVERING" and
	// costs ~20x more page reads without failing any other test.
	it("the est_row_bytes scans stay index-only", async () => {
		await withStore((_store, state) => {
			const plan = (sql: string, ...params: unknown[]) =>
				state.storage.sql
					.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${sql}`, ...params)
					.toArray()
					.map((r) => r.detail)
					.join(" | ");

			expect(plan(`SELECT sk, est_row_bytes FROM items WHERE hk = ? AND sk >= ? ORDER BY sk`, kb("hk"), kb("a"))).toContain(
				"COVERING INDEX",
			);
			expect(plan(`SELECT hk, SUM(est_row_bytes) FROM items GROUP BY hk`)).toContain("COVERING INDEX");
		});
	});

	// The old-estimate lookup that upsertItem and deleteItem share. Without INDEXED BY, SQLite picks
	// sqlite_autoindex_items_1 and fetches the table row for est_row_bytes — correct, but one row
	// fetch per put and per delete. Both halves are asserted: that the hint still works, and that it
	// is still needed, so the pin can be dropped if SQLite ever starts choosing the covering index.
	it("the old-estimate lookup stays index-only, and needs the INDEXED BY hint to do so", async () => {
		await withStore((_store, state) => {
			const plan = (sql: string) =>
				state.storage.sql
					.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${sql}`, kb("hk"), kb("sk"))
					.toArray()
					.map((r) => r.detail)
					.join(" | ");

			expect(plan(`SELECT est_row_bytes FROM items INDEXED BY idx_items_scan WHERE hk = ? AND sk = ? LIMIT 1`)).toContain(
				"COVERING INDEX idx_items_scan (hk=? AND sk=?)",
			);
			expect(plan(`SELECT est_row_bytes FROM items WHERE hk = ? AND sk = ? LIMIT 1`)).toContain("sqlite_autoindex_items_1");
		});
	});

	it("migration reads json verbatim (raw JSONB) and re-inserts it queryable by jsonb_extract", async () => {
		await withStore((store, state) => {
			const jsonText = JSON.stringify({ status: "ok", count: 7 });
			store.upsertItem({ hk: kb("hk"), sk: kb("j"), data: jsonText, kind: "json", ttlAt: null, lastTransactionTs: 1 });

			// Migration-style read: no json() decode, so json data is the raw JSONB blob.
			const migrated = store.queryItemsPage(null, 10)[0];
			expect(migrated.kind).toBe("json");
			expect(migrated.data).toBeInstanceOf(Uint8Array);

			// Re-insert verbatim under a new key and confirm the JSONB is still path-queryable.
			store.insertItemIfAbsent({ ...migrated, sk: kb("j2") });
			const count = state.storage.sql
				.exec<{ c: number }>(`SELECT jsonb_extract(data, '$.count') AS c FROM items WHERE hk = ? AND sk = ?`, kb("hk"), kb("j2"))
				.toArray()[0].c;
			expect(count).toBe(7);
		});
	});

	it("updateItemSingleShot distinguishes a missing item from an oversized result", async () => {
		await withStore((store) => {
			const update: UpdateExpression = [{ action: "set", target: { ref: "data", path: "$.x" }, value: { val: 1 } }];
			const plan = compileUpdateExpression(update);
			expect(() =>
				store.updateItemSingleShot({
					hk: kb("missing-hk"),
					sk: kb("missing-sk"),
					plan,
					lastTransactionTs: 1,
				}),
			).toThrow(/not found/);
		});
	});

	it("stores text hashKey and sortKey references as JSON strings", async () => {
		await withStore((store) => {
			const hk = kb("my-hash-key");
			const sk = kb("my-sort-key");
			store.upsertItem({ hk, sk, data: JSON.stringify({}), kind: "json", ttlAt: null, lastTransactionTs: 1 });
			const update: UpdateExpression = [
				{ action: "set", target: { ref: "data", path: "$.hk" }, value: { ref: "hashKey" } },
				{ action: "set", target: { ref: "data", path: "$.sk" }, value: { ref: "sortKey" } },
			];
			const plan = compileUpdateExpression(update);
			store.updateItemSingleShot({ hk, sk, plan, lastTransactionTs: 2 });
			const { row } = store.getItem(hk, sk);
			expect(row).toBeDefined();
			const data = JSON.parse(row!.data as string);
			expect(data).toEqual({ hk: "my-hash-key", sk: "my-sort-key" });
		});
	});

	it("rejects hashKey and sortKey references when keys are binary, and names the cause", async () => {
		await withStore((store) => {
			const hk = kb(new Uint8Array([1, 2, 3]));
			const sk = kb(new Uint8Array([4, 5, 6]));
			store.upsertItem({ hk, sk, data: JSON.stringify({}), kind: "json", ttlAt: null, lastTransactionTs: 1 });
			// A function around the reference must not escape the test: SQLite carries the untagged key
			// bytes through, and the result is still a blob that a JSON document cannot hold.
			const values = [
				{ ref: "hashKey" as const },
				{ ref: "sortKey" as const },
				{ fn: "sqlite.ifnull" as const, args: [{ ref: "hashKey" as const }, { val: "fallback" }] },
				{ fn: "sqlite.coalesce" as const, args: [{ ref: "sortKey" as const }, { val: "fallback" }] },
			];
			for (const value of values) {
				const plan = compileUpdateExpression([{ action: "set", target: { ref: "data", path: "$.x" }, value }] as UpdateExpression);
				const probe = store.probeUpdate(plan, hk, sk);
				expect(probe.applicable).toBe(false);
				expect(probe.valueTypeOk).toBe(false);
			}
			// The item is untouched: an inapplicable update writes nothing.
			expect(JSON.parse(store.getItem(hk, sk).row?.data as string)).toEqual({});
		});
	});

	it("updateItemSingleShot keeps est_row_bytes and key_size_estimates in step with the stored document", async () => {
		await withStore((store, state) => {
			const hk = kb("hk");
			const sk = kb("s1");
			store.upsertItem({ hk, sk, data: JSON.stringify({ note: "short" }), kind: "json", ttlAt: null, lastTransactionTs: 1 });
			const before = kseBytes(state, "hk");

			const plan = compileUpdateExpression([
				{ action: "set", target: { ref: "data", path: "$.note" }, value: { val: "a considerably longer note than the first one" } },
			]);
			const res = store.updateItemSingleShot({ hk, sk, plan, lastTransactionTs: 2 });

			// The size the update stored is measured over the document the update wrote, and the key's
			// estimate carries exactly that row. An update grows an item without carrying its bytes, so
			// this accounting is what keeps promotion and split decisions correct.
			const row = state.storage.sql
				.exec<{ e: number; d: number }>(`SELECT est_row_bytes AS e, octet_length(data) AS d FROM items WHERE hk = ? AND sk = ?`, hk, sk)
				.one();
			expect(row.e).toBe(row.d + hk.byteLength + sk.byteLength + EST_ROW_BYTES_K);
			expect(res.keyEstBytes).toBe(row.e);
			expect(kseBytes(state, "hk")).toBe(row.e);
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
			expect(() => store.upsertItem({ hk, sk, data: oversized, kind: "text", ttlAt: null, lastTransactionTs: 1 })).toThrow(/exceeds/);

			store.insertItemIfAbsent({ hk, sk, data: oversized, kind: "text", ttl_epoch_utc_seconds: null, v: 7, last_transaction_ts: 1 });
			expect(store.getItem(hk, sk).row).toMatchObject({ v: 7, data: oversized });
		});
	});

	it("separates a byte value from every other inapplicable update", async () => {
		await withStore((store) => {
			const hk = kb("text-key");
			const sk = kb("text-sort");
			store.upsertItem({ hk, sk, data: JSON.stringify({ a: 1 }), kind: "json", ttlAt: null, lastTransactionTs: 1 });
			// A missing target parent is inapplicable, but the values are fine, so the cause is not the
			// value type. Only that distinction lets a caller tell a fixable value from a stale item.
			const missingParent = compileUpdateExpression([{ action: "set", target: { ref: "data", path: "$.absent.x" }, value: { val: 1 } }]);
			const probe = store.probeUpdate(missingParent, hk, sk);
			expect(probe.applicable).toBe(false);
			expect(probe.valueTypeOk).toBe(true);

			// A text key is a valid update value, so the same plan that fails over a binary key applies here.
			const keyValue = compileUpdateExpression([{ action: "set", target: { ref: "data", path: "$.k" }, value: { ref: "hashKey" } }]);
			expect(store.probeUpdate(keyValue, hk, sk)).toMatchObject({ applicable: true, valueTypeOk: true });
		});
	});
});

describe("PartitionStore - TTL deletion", () => {
	function put(store: PartitionStore, hk: string, sk: string, ttlAt: number | null, data = "d"): number {
		store.upsertItem({ hk: kb(hk), sk: kb(sk), data, kind: "text", ttlAt, lastTransactionTs: 1 });
		return expectedRowBytes(data, kb(hk), kb(sk));
	}

	it("uses the bounded covering-index deletion plan", async () => {
		await withStore((_store, state) => {
			const plan = (indexHint: string) =>
				state.storage.sql
					.exec<{ detail: string }>(
						`EXPLAIN QUERY PLAN DELETE FROM items
						 WHERE rowid IN (
						     SELECT i.rowid FROM items i${indexHint}
						      WHERE i.ttl_epoch_utc_seconds IS NOT NULL
						        AND i.ttl_epoch_utc_seconds <= ?1
						        AND NOT EXISTS (SELECT 1 FROM pending_transactions p WHERE p.hk = i.hk AND p.sk = i.sk)
						        AND NOT EXISTS (SELECT 1 FROM promoted_keys pk WHERE pk.hash_key = i.hk)
						      ORDER BY i.ttl_epoch_utc_seconds, i.hk, i.sk
						      LIMIT ?2
						 )
						 RETURNING hk, est_row_bytes, ttl_epoch_utc_seconds`,
						100,
						10,
					)
					.toArray()
					.map((row) => row.detail)
					.join(" | ");

			for (const details of [plan(" INDEXED BY idx_items_ttl"), plan("")]) {
				expect(details).toContain("LIST SUBQUERY");
				expect(details).toContain("SEARCH i USING COVERING INDEX idx_items_ttl");
				expect(details).toMatch(/SEARCH p USING COVERING INDEX sqlite_autoindex_pending_transactions_1/);
				expect(details).toMatch(/SEARCH pk USING PRIMARY KEY/);
				expect(details).not.toMatch(/SCAN (?:p|pk)|USE TEMP B-TREE FOR ORDER BY/);
			}
		});
	});

	it("deletes oldest eligible rows and updates each key account", async () => {
		await withStore((store, state) => {
			const oldBytes = put(store, "a", "old", 10, "a");
			const lockedBytes = put(store, "locked", "s", 11, "locked");
			const queuedBytes = put(store, "queued", "s", 12, "queued");
			const promotingBytes = put(store, "promoting", "s", 13, "promoting");
			const promotedBytes = put(store, "promoted", "s", 14, "promoted");
			const secondBytes = put(store, "b", "s", 20, "bb");
			const nextBytes = put(store, "a", "next", 30, "ccc");
			const exactBytes = put(store, "c", "exact", 100, "dddd");
			const nullBytes = put(store, "a", "null", null, "null");
			const futureBytes = put(store, "a", "future", 101, "future");

			store.insertPendingLock({
				hk: kb("locked"),
				sk: kb("s"),
				transaction_id: "tx-locked",
				transaction_ts: 1,
				operation: "put",
				data: "pending",
				kind: "text",
				conditions_json: null,
				ttl_epoch_utc_seconds: null,
				coordinator_do_id: "tc",
				created_at: 1,
				guarded_at: null,
			});
			store.insertPromotedKey(kb("queued"), "queued", 1);
			store.insertPromotedKey(kb("promoting"), "promoting", 1);
			store.insertPromotedKey(kb("promoted"), "promoted", 1);

			const first = store.deleteExpiredItems(100, 2);
			expect(first).toEqual({ deletedRows: 2, deletedBytes: oldBytes + secondBytes });
			expect(store.getItem(kb("a"), kb("old")).row).toBeUndefined();
			expect(store.getItem(kb("b"), kb("s")).row).toBeUndefined();
			expect(store.getItem(kb("a"), kb("next")).row).toBeDefined();
			expect(kseBytes(state, "a")).toBe(nextBytes + nullBytes + futureBytes);
			expect(kseBytes(state, "b")).toBe(0);
			expect(store.getMaxDeletedTs()).toBe(20_000);

			store.bumpMaxDeletedTs(200_000);
			const second = store.deleteExpiredItems(100, 2);
			expect(second).toEqual({ deletedRows: 2, deletedBytes: nextBytes + exactBytes });
			expect(store.getItem(kb("a"), kb("next")).row).toBeUndefined();
			expect(store.getItem(kb("c"), kb("exact")).row).toBeUndefined();
			expect(kseBytes(state, "a")).toBe(nullBytes + futureBytes);
			expect(kseBytes(state, "c")).toBe(0);
			expect(store.getMaxDeletedTs()).toBe(200_000);

			for (const [hk, bytes] of [
				["locked", lockedBytes],
				["queued", queuedBytes],
				["promoting", promotingBytes],
				["promoted", promotedBytes],
			] as const) {
				expect(store.getItem(kb(hk), kb("s")).row).toBeDefined();
				expect(kseBytes(state, hk)).toBe(bytes);
			}
			expect(store.getItem(kb("a"), kb("null")).row).toBeDefined();
			expect(store.getItem(kb("a"), kb("future")).row).toBeDefined();
			expect(store.deleteExpiredItems(100, 2)).toEqual({ deletedRows: 0, deletedBytes: 0 });
		});
	});
});

describe("PartitionStore - deletion watermark", () => {
	it("bumpMaxDeletedTs is monotonic", async () => {
		await withStore((store) => {
			expect(store.getMaxDeletedTs()).toBe(0);
			store.bumpMaxDeletedTs(100);
			expect(store.getMaxDeletedTs()).toBe(100);
			store.bumpMaxDeletedTs(50);
			expect(store.getMaxDeletedTs()).toBe(100);
			store.bumpMaxDeletedTs(150);
			expect(store.getMaxDeletedTs()).toBe(150);
		});
	});

	it("deleteItem bumps the watermark only when a row was deleted, unless bumpWatermarkAlways", async () => {
		await withStore((store) => {
			// Absent row, default behavior: no bump.
			const miss = store.deleteItem({ hk: kb("hk"), sk: kb("absent"), watermarkTs: 100 });
			expect(miss.deleted).toBe(false);
			expect(store.getMaxDeletedTs()).toBe(0);

			// Absent row, transactional behavior: bump regardless.
			store.deleteItem({ hk: kb("hk"), sk: kb("absent"), watermarkTs: 100, bumpWatermarkAlways: true });
			expect(store.getMaxDeletedTs()).toBe(100);

			// Present row: bump.
			store.upsertItem({ hk: kb("hk"), sk: kb("s"), data: "d", kind: "text", ttlAt: null, lastTransactionTs: 1 });
			const hit = store.deleteItem({ hk: kb("hk"), sk: kb("s"), watermarkTs: 200 });
			expect(hit.deleted).toBe(true);
			expect(store.getMaxDeletedTs()).toBe(200);
		});
	});
});

describe("PartitionStore - pending transactions", () => {
	function lockRow(hk: string, sk: string, transactionId: string) {
		return {
			hk: kb(hk),
			sk: kb(sk),
			transaction_id: transactionId,
			transaction_ts: 123,
			operation: "put",
			data: "d",
			kind: "text" as const,
			conditions_json: null,
			ttl_epoch_utc_seconds: null,
			coordinator_do_id: "tc-1",
			created_at: 1000,
			guarded_at: null,
		};
	}

	it("insertPendingLock is idempotent and pendingLockFor finds the lock", async () => {
		await withStore((store) => {
			store.insertPendingLock(lockRow("hk", "s", "tx1"));
			store.insertPendingLock(lockRow("hk", "s", "tx1")); // retry — INSERT OR IGNORE
			expect(store.pendingTxCountFor("tx1")).toBe(1);
			expect(store.pendingLockFor(kb("hk"), kb("s"))?.transaction_id).toBe("tx1");
			expect(store.pendingLockFor(kb("hk"), kb("other"))).toBeUndefined();

			store.deletePendingTx("tx1");
			expect(store.pendingLockFor(kb("hk"), kb("s"))).toBeUndefined();
			expect(store.hasAnyPendingTx()).toBe(false);
		});
	});

	it("returns the prepared TTL on commit and recovery reads", async () => {
		await withStore((store) => {
			const row = { ...lockRow("hk", "s", "tx-ttl"), ttl_epoch_utc_seconds: 777 };
			store.insertPendingLock(row);

			expect(store.getPendingTxOp(row.hk, row.sk, row.transaction_id)?.ttl_epoch_utc_seconds).toBe(777);
			expect(store.listPendingTxItems(row.transaction_id)[0].ttl_epoch_utc_seconds).toBe(777);
			expect(store.queryPendingTxPage(null, 1)[0].ttl_epoch_utc_seconds).toBe(777);
		});
	});

	it("hasAnyPendingTx answers from a single row", async () => {
		await withStore((store, state) => {
			expect(store.hasAnyPendingTx()).toBe(false);
			for (const sk of ["1", "2", "3"]) store.insertPendingLock(lockRow("hk", sk, "tx1"));
			expect(store.hasAnyPendingTx()).toBe(true);

			store.deletePendingTxForHashKey(kb("hk"));
			expect(store.hasAnyPendingTx()).toBe(false);
		});
	});

	it("listStalePendingTx returns only locks created before the threshold", async () => {
		await withStore((store) => {
			store.insertPendingLock({ ...lockRow("a", "1", "tx-old"), created_at: 1000 });
			store.insertPendingLock({ ...lockRow("b", "1", "tx-new"), created_at: 5000 });
			const stale = store.listStalePendingTx(2000, 10);
			expect(stale).toEqual([{ transaction_id: "tx-old", coordinator_do_id: "tc-1" }]);
		});
	});

	it("quarantines a transaction once and excludes it from the stale scan", async () => {
		await withStore((store) => {
			store.insertPendingLock(lockRow("a", "1", "tx-guarded"));
			expect(store.guardPendingTx("tx-guarded", 2000)).toBe(true);
			expect(store.guardPendingTx("tx-guarded", 3000)).toBe(false);
			expect(store.listPendingTxItems("tx-guarded")[0].guarded_at).toBe(2000);
			expect(store.hasAnyPendingTx()).toBe(true);
			expect(store.hasAnyUnguardedPendingTx()).toBe(false);
			expect(store.listStalePendingTx(5000, 10)).toEqual([]);

			store.clearPendingTxGuard("tx-guarded");
			expect(store.listStalePendingTx(5000, 10)).toEqual([{ transaction_id: "tx-guarded", coordinator_do_id: "tc-1" }]);
		});
	});

	it("does not let ten quarantined transactions hide an unguarded stale transaction", async () => {
		await withStore((store) => {
			for (let i = 0; i < 11; i++) {
				const transactionId = `tx-${i}`;
				store.insertPendingLock(lockRow(`hk-${i}`, "1", transactionId));
				if (i < 10) store.guardPendingTx(transactionId, 2000);
			}
			expect(store.listStalePendingTx(5000, 10)).toEqual([{ transaction_id: "tx-10", coordinator_do_id: "tc-1" }]);
		});
	});

	it("queryPendingTxPage orders by (hk, sk, transaction_id) and resumes strictly after the cursor", async () => {
		await withStore((store) => {
			store.insertPendingLock(lockRow("a", "1", "tx2"));
			store.insertPendingLock(lockRow("a", "1", "tx1"));
			store.insertPendingLock(lockRow("b", "1", "tx3"));
			const page1 = store.queryPendingTxPage(null, 2);
			expect(page1.map((r) => r.transaction_id)).toEqual(["tx1", "tx2"]);
			const page2 = store.queryPendingTxPage({ hk: kb("a"), sk: kb("1"), transaction_id: "tx2" }, 2);
			expect(page2.map((r) => r.transaction_id)).toEqual(["tx3"]);
		});
	});

	// transaction_id is the third primary key column, so these four queries can only seek through
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
				`SELECT COUNT(*) AS n FROM pending_transactions WHERE transaction_id = ?`,
				`SELECT hk, sk FROM pending_transactions WHERE transaction_id = ?`,
				`SELECT hk, sk, transaction_ts, operation, data, data_kind FROM pending_transactions WHERE transaction_id = ?`,
				`DELETE FROM pending_transactions WHERE transaction_id = ?`,
			]) {
				// "SEARCH … (transaction_id=?)" is the seek. Asserting only the index name would also
				// pass for a full SCAN that happens to walk the same index.
				expect(plan(sql), sql).toContain("SEARCH");
				expect(plan(sql), sql).toContain("pending_transactions_transaction_id (transaction_id=?)");
			}
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

describe("PartitionStore - promoted keys", () => {
	it("insertPromotedKey is idempotent and updatePromotedKeyStatus is guarded by fromStatus", async () => {
		await withStore((store) => {
			expect(store.insertPromotedKey(kb("hk"), "queued", 1000)).toEqual({ inserted: true });
			// Ignored — already present; callers must resync any cache from storage.
			expect(store.insertPromotedKey(kb("hk"), "promoting", 2000)).toEqual({ inserted: false });
			expect(store.getPromotedKeyStatus(kb("hk"))).toBe("queued");

			// Wrong fromStatus — no-op, reported so cache holders can resync.
			expect(store.updatePromotedKeyStatus(kb("hk"), "promoting", "promoted", 3000)).toEqual({ updated: false });
			expect(store.getPromotedKeyStatus(kb("hk"))).toBe("queued");

			expect(store.updatePromotedKeyStatus(kb("hk"), "queued", "promoting", 3000)).toEqual({ updated: true });
			expect(store.getPromotedKeyStatus(kb("hk"))).toBe("promoting");

			// Absent key — also reported as not updated.
			expect(store.updatePromotedKeyStatus(kb("missing"), "queued", "promoting", 3000)).toEqual({ updated: false });
			expect(store.listPromotedKeys()).toEqual([{ hash_key: kb("hk"), status: "promoting" }]);
		});
	});

	it("queryPromotedKeysPage pages in hash_key order with cursor resume", async () => {
		await withStore((store) => {
			store.insertPromotedKey(kb("b"), "queued", 1);
			store.insertPromotedKey(kb("a"), "queued", 1);
			store.insertPromotedKey(kb("c"), "queued", 1);
			const page1 = store.queryPromotedKeysPage(null, 2);
			expect(page1.map((r) => KeyCodec.decode(r.hash_key))).toEqual(["a", "b"]);
			const page2 = store.queryPromotedKeysPage({ hashKey: kb("b") }, 2);
			expect(page2.map((r) => KeyCodec.decode(r.hash_key))).toEqual(["c"]);
		});
	});
});

describe("PartitionStore - computeRangeSplitBoundaries", () => {
	function put(store: PartitionStore, hk: string, ...sks: string[]) {
		for (const sk of sks) {
			store.upsertItem({ hk: kb(hk), sk: kb(sk), data: "d", kind: "text", ttlAt: null, lastTransactionTs: 1 });
		}
	}

	it("returns null when fewer than N items exist", async () => {
		await withStore((store) => {
			put(store, "hk", "a");
			expect(store.computeRangeSplitBoundaries(kb("hk"), null, null, 2)).toBeNull();
		});
	});

	it("returns null when the hash key has no items", async () => {
		await withStore((store) => {
			expect(store.computeRangeSplitBoundaries(kb("hk"), null, null, 2)).toBeNull();
		});
	});

	it("N=2: emits a boundary at the byte midpoint (crossing row goes to the upper child)", async () => {
		await withStore((store) => {
			// 4 equal-size items; total bytes B, step = B/2. Accumulating est_row_bytes: after "apple"
			// acc ≈ B/4 (< step); "cherry" tips acc over B/2, so the boundary lands between "apple" and
			// "cherry" (the crossing row "cherry" falls into the upper child).
			// shortestSeparator("apple","cherry"): 'a'!='c' at i=0 → "c".
			put(store, "hk", "apple", "cherry", "mango", "peach");
			expect(store.computeRangeSplitBoundaries(kb("hk"), null, null, 2)).toEqual([kb("c")]);
		});
	});

	it("returns boundary unchanged when no prefix shortening is possible (single-char keys)", async () => {
		await withStore((store) => {
			// predecessor="a", boundary="b" → shortestSeparator → "b" (already minimal)
			put(store, "hk", "a", "b");
			expect(store.computeRangeSplitBoundaries(kb("hk"), null, null, 2)).toEqual([kb("b")]);
		});
	});

	it("shortens a long common prefix (only last char differs)", async () => {
		await withStore((store) => {
			// predecessor="prefix_aaa", boundary="prefix_bbb"
			// shortestSeparator: first diff at i=7 ('a'!='b') → "prefix_b"
			put(store, "hk", "prefix_aaa", "prefix_bbb");
			expect(store.computeRangeSplitBoundaries(kb("hk"), null, null, 2)).toEqual([kb("prefix_b")]);
		});
	});

	it("shortens when predecessor is a prefix of the boundary", async () => {
		await withStore((store) => {
			// predecessor="app", boundary="apple" (predecessor is proper prefix)
			// shortestSeparator: loop exhausts at minLen=3, return "apple".substring(0,4) = "appl"
			put(store, "hk", "app", "apple");
			expect(store.computeRangeSplitBoundaries(kb("hk"), null, null, 2)).toEqual([kb("appl")]);
		});
	});

	it("N=3: produces two strictly-increasing shortened byte-quantile boundaries", async () => {
		await withStore((store) => {
			// 6 roughly-equal items; step = B/3. sorted: "aardvark","cherry","mango","strawberry","vanilla","zebra"
			// b1: "cherry" tips acc over B/3 → shortestSeparator("aardvark","cherry") → "c"
			// b2: "strawberry" tips acc over 2·B/3 → shortestSeparator("mango","strawberry") → "s"
			put(store, "hk", "aardvark", "cherry", "mango", "strawberry", "vanilla", "zebra");
			expect(store.computeRangeSplitBoundaries(kb("hk"), null, null, 3)).toEqual([kb("c"), kb("s")]);
		});
	});

	it("honors explicit start/end range bounds on the scan", async () => {
		await withStore((store) => {
			// A splitting range parent owns exactly its [start, end) slice, so its items == the slice and
			// est_bytes[hk] is the slice's byte total. Here the DO holds "banana","cherry","mango" and splits
			// [banana, peach). N=2: "cherry" tips acc over B/2 → shortestSeparator("banana","cherry") → "c".
			put(store, "hk", "banana", "cherry", "mango");
			expect(store.computeRangeSplitBoundaries(kb("hk"), kb("banana"), kb("peach"), 2)).toEqual([kb("c")]);
		});
	});

	it("returns null when range slice has fewer than N items", async () => {
		await withStore((store) => {
			put(store, "hk", "apple", "banana", "cherry", "mango", "peach");
			// Range [cherry, mango) → only "cherry" qualifies (mango excluded) — 1 item < N=2
			expect(store.computeRangeSplitBoundaries(kb("hk"), kb("cherry"), kb("mango"), 2)).toBeNull();
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
					hk: kb("hk"),
					sk: kb(sk),
					data: new Uint8Array(0),
					kind: "bytes",
					ttlAt: null,
					lastTransactionTs: 0,
				});
			}
			// N=2 → 1 boundary between the 3 items in byte order: uFFFF < emoji < binary
			// Boundary should lie between uFFFF and emoji (both are sort keys).
			const boundaries = store.computeRangeSplitBoundaries(kb("hk"), null, null, 2);
			expect(boundaries).not.toBeNull();
			expect(boundaries!.length).toBe(1);
			// Boundary must satisfy: encode(uFFFF) < boundary <= encode(emoji)
			expect(KeyCodec.compare(kb(uFFFF), boundaries![0])).toBeLessThan(0);
			expect(KeyCodec.compare(boundaries![0], kb(emoji))).toBeLessThanOrEqual(0);
		});
	});

	// Buckets known sorted keys into the N children defined by `boundaries` and returns the count per child.
	// Child i owns [b_{i-1}, b_i); mirrors the byte-space [start, end) routing the migration scans use.
	function bucketCounts(sortedKeys: string[], boundaries: KeyBytes[]): number[] {
		const counts = new Array(boundaries.length + 1).fill(0);
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
				store.upsertItem({ hk: kb("hk"), sk: kb(sk), data: "payload", kind: "text", ttlAt: null, lastTransactionTs: 1 });
			}
			const boundaries = store.computeRangeSplitBoundaries(kb("hk"), null, null, 4);
			expect(boundaries).not.toBeNull();
			expect(boundaries!.length).toBe(3);
			// Strictly increasing.
			for (let i = 1; i < boundaries!.length; i++) {
				expect(KeyCodec.compare(boundaries![i - 1], boundaries![i])).toBeLessThan(0);
			}
			// Every child non-empty and within a loose band of the ideal 10.
			const counts = bucketCounts(keys, boundaries!);
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
			// keyed to sort last. With N=2, step = B/2 < heavy weight, so the light rows all fall below
			// the threshold and the heavy row alone tips it over → boundary lands between them.
			for (let i = 0; i < 10; i++) {
				store.upsertItem({ hk: kb("hk"), sk: kb(`k${i}`), data: "x", kind: "text", ttlAt: null, lastTransactionTs: 1 });
			}
			store.upsertItem({
				hk: kb("hk"),
				sk: kb("zheavy"),
				data: "H".repeat(5000),
				kind: "text",
				ttlAt: null,
				lastTransactionTs: 1,
			});
			const boundaries = store.computeRangeSplitBoundaries(kb("hk"), null, null, 2);
			expect(boundaries).not.toBeNull();
			expect(boundaries!.length).toBe(1);
			// Boundary sits above the last light key and at/below the heavy key: lights in child 0, heavy alone in child 1.
			expect(KeyCodec.compare(kb("k9"), boundaries![0])).toBeLessThan(0);
			expect(KeyCodec.compare(boundaries![0], kb("zheavy"))).toBeLessThanOrEqual(0);
		});
	});

	it("skewed data that cannot form N-1 boundaries returns null (retry contract)", async () => {
		await withStore((store) => {
			// A single dominant row between two light rows. With N=3 the heavy row crosses the first
			// threshold and the relative bump pushes the next threshold past the remaining bytes, so only
			// one boundary is emitted (< N-1) → null, and the split retries on a later cycle.
			store.upsertItem({ hk: kb("hk"), sk: kb("a"), data: "x", kind: "text", ttlAt: null, lastTransactionTs: 1 });
			store.upsertItem({ hk: kb("hk"), sk: kb("m"), data: "H".repeat(5000), kind: "text", ttlAt: null, lastTransactionTs: 1 });
			store.upsertItem({ hk: kb("hk"), sk: kb("z"), data: "x", kind: "text", ttlAt: null, lastTransactionTs: 1 });
			expect(store.computeRangeSplitBoundaries(kb("hk"), null, null, 3)).toBeNull();
		});
	});
});

describe("PartitionStore - range_hierarchy", () => {
	const OWN_HK = kb("own");

	it("returns [] when no rows are stored", async () => {
		await withStore((store) => {
			expect(store.getRangeAncestors(OWN_HK, 10)).toEqual([]);
		});
	});

	it("setRangeAncestors([]) is a no-op (does not throw on a zero-tuple INSERT)", async () => {
		await withStore((store) => {
			expect(() => store.setRangeAncestors(OWN_HK, [])).not.toThrow();
			expect(store.getRangeAncestors(OWN_HK, 10)).toEqual([]);
		});
	});

	it("round-trips a populated set, ordered by depth ascending", async () => {
		await withStore((store) => {
			store.setRangeAncestors(OWN_HK, [
				{ depth: 2, startBoundary: kb("b2"), endBoundary: kb("e2") },
				{ depth: 1, startBoundary: kb("b1"), endBoundary: KeyCodec.encodeOptional(undefined) },
			]);
			expect(store.getRangeAncestors(OWN_HK, 10)).toEqual([
				{ depth: 1, startBoundary: kb("b1"), endBoundary: KeyCodec.encodeOptional(undefined) },
				{ depth: 2, startBoundary: kb("b2"), endBoundary: kb("e2") },
			]);
		});
	});

	it("round-trips Uint8Array boundaries", async () => {
		await withStore((store) => {
			const bin = new Uint8Array([1, 2, 3]);
			store.setRangeAncestors(OWN_HK, [{ depth: 1, startBoundary: kb(bin), endBoundary: KeyCodec.encodeOptional(undefined) }]);
			const got = store.getRangeAncestors(OWN_HK, 10);
			const decodedBin = KeyCodec.decode(got[0].startBoundary);
			expect(got).toHaveLength(1);
			expect(decodedBin).toBeInstanceOf(Uint8Array);
			expect(decodedBin).toEqual(bin);
			expect(got[0].endBoundary).toEqual(KeyCodec.encodeOptional(undefined));
		});
	});

	it("excludes rows at or beyond ownDepth (future-proofing for descendant-side entries)", async () => {
		await withStore((store) => {
			store.setRangeAncestors(OWN_HK, [
				{ depth: 1, startBoundary: kb("b1"), endBoundary: KeyCodec.encodeOptional(undefined) },
				{ depth: 5, startBoundary: kb("b5"), endBoundary: kb("e5") },
			]);
			expect(store.getRangeAncestors(OWN_HK, 5)).toEqual([
				{ depth: 1, startBoundary: kb("b1"), endBoundary: KeyCodec.encodeOptional(undefined) },
			]);
		});
	});

	// Regression: `range_hierarchy` holds ancestors AND learned router boundaries for other hash keys
	// (a hash partition that forwards to a promoted key's range tree writes the latter on every
	// forwarded request). Without the `hk` filter, getRangeAncestors returned those as ancestors.
	it("ignores learned boundaries stored under a different hash key", async () => {
		await withStore((store) => {
			store.setRangeAncestors(OWN_HK, [{ depth: 1, startBoundary: kb("b1"), endBoundary: kb("e1") }]);
			store.insertRangePartitionBoundary(kb("other"), kb("x"), kb("z"), 1);
			store.insertRangePartitionBoundary(KeyCodec.encodeOptional(undefined), kb("p"), kb("q"), 1);

			expect(store.getRangeAncestors(OWN_HK, 10)).toEqual([{ depth: 1, startBoundary: kb("b1"), endBoundary: kb("e1") }]);
		});
	});

	// Both writers use the real hash key, so PRIMARY KEY (hk, sk_start_boundary, sk_end_boundary)
	// collapses a re-learned ancestor onto the row setRangeAncestors already wrote.
	it("does not duplicate an ancestor that is later re-learned for the same hash key", async () => {
		await withStore((store) => {
			store.setRangeAncestors(OWN_HK, [{ depth: 1, startBoundary: kb("b1"), endBoundary: kb("e1") }]);
			store.insertRangePartitionBoundary(OWN_HK, kb("b1"), kb("e1"), 1);

			expect(store.getRangeAncestors(OWN_HK, 10)).toEqual([{ depth: 1, startBoundary: kb("b1"), endBoundary: kb("e1") }]);
		});
	});

	// This one regresses silently: the query stays correct while degrading to a scan plus a temp
	// B-tree over a table that has no TTL. Both predicates are needed to hit the index.
	it("getRangeAncestors seeks on idx_range_hierarchy_depth", async () => {
		await withStore((store, state) => {
			store.setRangeAncestors(OWN_HK, [{ depth: 1, startBoundary: kb("b1"), endBoundary: kb("e1") }]);
			const plan = state.storage.sql
				.exec<{ detail: string }>(
					`EXPLAIN QUERY PLAN SELECT depth, sk_start_boundary, sk_end_boundary FROM range_hierarchy WHERE hk = ? AND depth < ? ORDER BY depth ASC`,
					OWN_HK,
					10,
				)
				.toArray()
				.map((r) => r.detail)
				.join(" | ");

			expect(plan).toContain("SEARCH");
			expect(plan).toContain("idx_range_hierarchy_depth (hk=? AND depth<?)");
			expect(plan).not.toContain("TEMP B-TREE");
		});
	});
});

describe("PartitionStore - findDeepestKnownRangeSlice", () => {
	const UNBOUNDED = KeyCodec.encodeOptional(undefined);

	// A single hash key's learned range tree:
	//   depth 1: [-∞,"m") , ["m",+∞)
	//   depth 2 (within ["m",+∞)): ["m","t") , ["t",+∞)
	function seedTree(store: PartitionStore, hk = kb("h")) {
		store.insertRangePartitionBoundary(hk, UNBOUNDED, kb("m"), 1);
		store.insertRangePartitionBoundary(hk, kb("m"), UNBOUNDED, 1);
		store.insertRangePartitionBoundary(hk, kb("m"), kb("t"), 2);
		store.insertRangePartitionBoundary(hk, kb("t"), UNBOUNDED, 2);
	}

	it("returns null when nothing is stored", async () => {
		await withStore((store) => {
			expect(store.findDeepestKnownRangeSlice(kb("h"), kb("p"))).toBeNull();
		});
	});

	it("returns the deepest slice containing the key", async () => {
		await withStore((store) => {
			seedTree(store);
			// "p" is in ["m","t") at depth 2, a strict sub-slice of ["m",+∞) at depth 1.
			expect(store.findDeepestKnownRangeSlice(kb("h"), kb("p"))).toEqual({
				depth: 2,
				startBoundary: kb("m"),
				endBoundary: kb("t"),
			});
		});
	});

	it("selects an unbounded-end slice via the empty sentinel (decoded to null)", async () => {
		await withStore((store) => {
			seedTree(store);
			// "z" is in ["t",+∞) at depth 2 — only matched because the end sentinel is treated as +∞.
			expect(store.findDeepestKnownRangeSlice(kb("h"), kb("z"))).toEqual({
				depth: 2,
				startBoundary: kb("t"),
				endBoundary: null,
			});
		});
	});

	it("selects an unbounded-start slice (decoded to null)", async () => {
		await withStore((store) => {
			seedTree(store);
			// "a" only falls in [-∞,"m") at depth 1.
			expect(store.findDeepestKnownRangeSlice(kb("h"), kb("a"))).toEqual({
				depth: 1,
				startBoundary: null,
				endBoundary: kb("m"),
			});
		});
	});

	it("falls back to a shallower covering slice when the deeper slice lies to the side of the key", async () => {
		await withStore((store) => {
			const hk = kb("h");
			// Only a depth-1 ["m",+∞) and a depth-2 ["t",+∞) are known; nothing at depth 2 covers ["m","t").
			store.insertRangePartitionBoundary(hk, kb("m"), UNBOUNDED, 1);
			store.insertRangePartitionBoundary(hk, kb("t"), UNBOUNDED, 2);
			// "p" is left of "t", so the depth-2 slice does not contain it — fall back to depth 1.
			expect(store.findDeepestKnownRangeSlice(hk, kb("p"))).toEqual({
				depth: 1,
				startBoundary: kb("m"),
				endBoundary: null,
			});
		});
	});

	it("returns null when no stored slice covers the key", async () => {
		await withStore((store) => {
			const hk = kb("h");
			// Only the right half is known; "a" is left of every stored start.
			store.insertRangePartitionBoundary(hk, kb("m"), UNBOUNDED, 1);
			expect(store.findDeepestKnownRangeSlice(hk, kb("a"))).toBeNull();
		});
	});

	it("isolates by hash key", async () => {
		await withStore((store) => {
			seedTree(store, kb("h"));
			expect(store.findDeepestKnownRangeSlice(kb("other"), kb("p"))).toBeNull();
		});
	});
});
