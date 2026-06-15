import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../do-partition.js";
import { KeyCodec } from "../partition-topology/key-codec.js";
import { estimateRowBytes, PartitionStore } from "./partition-store.js";

const kb = (s: string) => KeyCodec.encode(s);

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

	it("N=2: returns the shortest separator between the two halves", async () => {
		await withStore((store) => {
			// 4 items; boundary at offset floor(4/2)=2 → predecessor="cherry", boundary="mango"
			// shortestSeparator("cherry","mango"): 'c'!='m' at i=0 → "m"
			put(store, "hk", "apple", "cherry", "mango", "peach");
			expect(store.computeRangeSplitBoundaries(kb("hk"), null, null, 2)).toEqual([kb("m")]);
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

	it("N=3: produces two strictly-increasing shortened boundaries", async () => {
		await withStore((store) => {
			// 6 items; boundary1 at offset 2, boundary2 at offset 4
			// sorted: "aardvark","cherry","mango","strawberry","vanilla","zebra"
			// b1: predecessor="cherry", boundary="mango" → shortestSeparator → "m"
			// b2: predecessor="strawberry", boundary="vanilla" → shortestSeparator → "v"
			put(store, "hk", "aardvark", "cherry", "mango", "strawberry", "vanilla", "zebra");
			expect(store.computeRangeSplitBoundaries(kb("hk"), null, null, 3)).toEqual([kb("m"), kb("v")]);
		});
	});

	it("respects start/end range bounds and excludes out-of-range items", async () => {
		await withStore((store) => {
			// All items: "apple","banana","cherry","mango","peach","strawberry"
			// Range [banana, peach) → in-range items: "banana","cherry","mango" (3 items)
			// N=2: boundary at offset 1 → predecessor="banana", boundary="cherry" → "c"
			put(store, "hk", "apple", "banana", "cherry", "mango", "peach", "strawberry");
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
});
