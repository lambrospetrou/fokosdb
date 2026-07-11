import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../do-partition.js";
import { type KeyBytes, KeyCodec } from "../partition-topology/key-codec.js";
import { estimateRowBytes, PartitionStore } from "./partition-store.js";

const kb = (s: string | Uint8Array) => KeyCodec.encode(s);

// Runs `fn` against a PartitionStore over REAL Durable Object storage (vitest-pool-workers).
// The PartitionDO constructor has already run the schema migrations by the time the callback runs;
// constructing a second PartitionStore over the same storage is safe (migrations are idempotent).
async function withStore(fn: (store: PartitionStore, state: DurableObjectState) => void | Promise<void>): Promise<void> {
	const id = env.PARTITION_DO.idFromName(`store-test.${crypto.randomUUID()}`);
	const stub = env.PARTITION_DO.get(id);
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
			const first = store.upsertItem({ hk: kb("hk"), sk: kb("sk"), data: "v1", ttlEpochUtcSeconds: null, lastTransactionTs: 1 });
			expect(first.version).toBe(1);
			const second = store.upsertItem({ hk: kb("hk"), sk: kb("sk"), data: "v2", ttlEpochUtcSeconds: null, lastTransactionTs: 2 });
			expect(second.version).toBe(2);
			const third = store.upsertItem({ hk: kb("hk"), sk: kb("sk"), data: "v3", ttlEpochUtcSeconds: null, lastTransactionTs: 3 });
			expect(third.version).toBe(3);
		});
	});

	it("getItem returns converted data, ttl, version, and last_transaction_ts", async () => {
		await withStore((store) => {
			store.upsertItem({ hk: kb("hk"), sk: kb("s"), data: "hello", ttlEpochUtcSeconds: 1234, lastTransactionTs: 42 });
			const str = store.getItem(kb("hk"), kb("s"));
			expect(str.row).toEqual({ data: "hello", ttl_epoch_utc_seconds: 1234, v: 1, last_transaction_ts: 42 });
			expect(str.rowsRead).toBe(1);

			const bin = new Uint8Array([1, 2, 3]);
			store.upsertItem({ hk: kb("hk"), sk: kb("b"), data: bin, ttlEpochUtcSeconds: null, lastTransactionTs: 0 });
			const got = store.getItem(kb("hk"), kb("b"));
			expect(got.row?.data).toBeInstanceOf(Uint8Array);
			expect(got.row?.data).toEqual(bin);

			expect(store.getItem(kb("hk"), kb("missing")).row).toBeUndefined();
		});
	});

	it("maintains key_size_estimates across put, overwrite, and delete", async () => {
		await withStore((store, state) => {
			const est1 = estimateRowBytes("aaaa", kb("hk"), kb("s1"));
			const r1 = store.upsertItem({ hk: kb("hk"), sk: kb("s1"), data: "aaaa", ttlEpochUtcSeconds: null, lastTransactionTs: 1 });
			expect(r1.keyEstBytes).toBe(est1);
			expect(kseBytes(state, "hk")).toBe(est1);

			// Second sort key accumulates on the same hash key.
			const est2 = estimateRowBytes("bb", kb("hk"), kb("s2"));
			const r2 = store.upsertItem({ hk: kb("hk"), sk: kb("s2"), data: "bb", ttlEpochUtcSeconds: null, lastTransactionTs: 2 });
			expect(r2.keyEstBytes).toBe(est1 + est2);

			// Overwrite replaces the old row's contribution, not adds to it.
			const est1b = estimateRowBytes("aaaaaaaa", kb("hk"), kb("s1"));
			const r3 = store.upsertItem({ hk: kb("hk"), sk: kb("s1"), data: "aaaaaaaa", ttlEpochUtcSeconds: null, lastTransactionTs: 3 });
			expect(r3.keyEstBytes).toBe(est1b + est2);

			// Delete removes its contribution.
			const del = store.deleteItem({ hk: kb("hk"), sk: kb("s1"), watermarkTs: 10 });
			expect(del.deleted).toBe(true);
			expect(kseBytes(state, "hk")).toBe(est2);
		});
	});

	it("rebuildKeySizeEstimates recomputes estimates from the rows", async () => {
		await withStore((store, state) => {
			store.upsertItem({ hk: kb("hk"), sk: kb("s1"), data: "xx", ttlEpochUtcSeconds: null, lastTransactionTs: 1 });
			store.upsertItem({ hk: kb("hk"), sk: kb("s2"), data: "yyyy", ttlEpochUtcSeconds: null, lastTransactionTs: 2 });
			// Corrupt the summary, then rebuild.
			state.storage.sql.exec(`UPDATE key_size_estimates SET est_bytes = 0 WHERE hk = ?`, kb("hk"));
			store.rebuildKeySizeEstimates();
			expect(kseBytes(state, "hk")).toBe(estimateRowBytes("xx", kb("hk"), kb("s1")) + estimateRowBytes("yyyy", kb("hk"), kb("s2")));
		});
	});

	it("queryItemsPage pages in (hk, sk) order and resumes strictly after the cursor", async () => {
		await withStore((store) => {
			for (const [hk, sk] of [
				["a", "1"],
				["a", "2"],
				["b", "1"],
			] as const) {
				store.upsertItem({ hk: kb(hk), sk: kb(sk), data: "d", ttlEpochUtcSeconds: null, lastTransactionTs: 1 });
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
			store.upsertItem({ hk: kb("hk"), sk: kb("s"), data: "d", ttlEpochUtcSeconds: null, lastTransactionTs: 1 });
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
			conditions_json: null,
			coordinator_do_id: "tc-1",
			created_at: 1000,
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
			expect(store.pendingTxTotalCount()).toBe(0);
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
			store.upsertItem({ hk: kb(hk), sk: kb(sk), data: "d", ttlEpochUtcSeconds: null, lastTransactionTs: 1 });
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
					ttlEpochUtcSeconds: null,
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
				store.upsertItem({ hk: kb("hk"), sk: kb(sk), data: "payload", ttlEpochUtcSeconds: null, lastTransactionTs: 1 });
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
				store.upsertItem({ hk: kb("hk"), sk: kb(`k${i}`), data: "x", ttlEpochUtcSeconds: null, lastTransactionTs: 1 });
			}
			store.upsertItem({ hk: kb("hk"), sk: kb("zheavy"), data: "H".repeat(5000), ttlEpochUtcSeconds: null, lastTransactionTs: 1 });
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
			store.upsertItem({ hk: kb("hk"), sk: kb("a"), data: "x", ttlEpochUtcSeconds: null, lastTransactionTs: 1 });
			store.upsertItem({ hk: kb("hk"), sk: kb("m"), data: "H".repeat(5000), ttlEpochUtcSeconds: null, lastTransactionTs: 1 });
			store.upsertItem({ hk: kb("hk"), sk: kb("z"), data: "x", ttlEpochUtcSeconds: null, lastTransactionTs: 1 });
			expect(store.computeRangeSplitBoundaries(kb("hk"), null, null, 3)).toBeNull();
		});
	});
});

describe("PartitionStore - range_hierarchy", () => {
	it("returns [] when no rows are stored", async () => {
		await withStore((store) => {
			expect(store.getRangeAncestors(10)).toEqual([]);
		});
	});

	it("setRangeAncestors([]) is a no-op (does not throw on a zero-tuple INSERT)", async () => {
		await withStore((store) => {
			expect(() => store.setRangeAncestors([])).not.toThrow();
			expect(store.getRangeAncestors(10)).toEqual([]);
		});
	});

	it("round-trips a populated set, ordered by depth ascending", async () => {
		await withStore((store) => {
			store.setRangeAncestors([
				{ depth: 2, startBoundary: kb("b2"), endBoundary: kb("e2") },
				{ depth: 1, startBoundary: kb("b1"), endBoundary: KeyCodec.encodeOptional(undefined) },
			]);
			expect(store.getRangeAncestors(10)).toEqual([
				{ depth: 1, startBoundary: kb("b1"), endBoundary: KeyCodec.encodeOptional(undefined) },
				{ depth: 2, startBoundary: kb("b2"), endBoundary: kb("e2") },
			]);
		});
	});

	it("round-trips Uint8Array boundaries", async () => {
		await withStore((store) => {
			const bin = new Uint8Array([1, 2, 3]);
			store.setRangeAncestors([{ depth: 1, startBoundary: kb(bin), endBoundary: KeyCodec.encodeOptional(undefined) }]);
			const got = store.getRangeAncestors(10);
			const decodedBin = KeyCodec.decode(got[0].startBoundary);
			expect(got).toHaveLength(1);
			expect(decodedBin).toBeInstanceOf(Uint8Array);
			expect(decodedBin).toEqual(bin);
			expect(got[0].endBoundary).toEqual(KeyCodec.encodeOptional(undefined));
		});
	});

	it("excludes rows at or beyond ownDepth (future-proofing for descendant-side entries)", async () => {
		await withStore((store) => {
			store.setRangeAncestors([
				{ depth: 1, startBoundary: kb("b1"), endBoundary: KeyCodec.encodeOptional(undefined) },
				{ depth: 5, startBoundary: kb("b5"), endBoundary: kb("e5") },
			]);
			expect(store.getRangeAncestors(5)).toEqual([{ depth: 1, startBoundary: kb("b1"), endBoundary: KeyCodec.encodeOptional(undefined) }]);
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
