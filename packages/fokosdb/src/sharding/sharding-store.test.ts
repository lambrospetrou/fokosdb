import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { FokosDBPartitionDO } from "../server/do-partition.js";
import { testPartitionStub } from "../../test/stub-helpers.js";
import { KeyCodec } from "./key-codec.js";
import { invariantFailure } from "../../test/errors-matchers.js";
import { asHashPrefixBucket } from "./hash-primitives.js";
import { FOKOS_KV_KEYS, FokosShardingStore, type FokosShardingStoreOptions } from "./sharding-store.js";
import { DEFAULT_RUNTIME_CONFIG } from "./runtime-config.js";

const REFRESH_MS = DEFAULT_RUNTIME_CONFIG.rangeHierarchyRefreshMs;

const kb = (s: string) => KeyCodec.encode(s);
const UNBOUNDED = KeyCodec.encodeOptional(undefined);
const BUCKET = asHashPrefixBucket(Uint8Array.fromHex("0102030405060708"));
const OTHER_BUCKET = asHashPrefixBucket(Uint8Array.fromHex("0807060504030201"));
/** A boundary of the hash key "h" with the sort key `sk`. */
const c = (sk: string) => ({ hk: kb("h"), sk: kb(sk) });

// Runs `fn` against a FokosShardingStore over REAL Durable Object storage. The FokosDBPartitionDO constructor
// has already run the sharding migrations by the time the callback runs, and they are idempotent.
async function withStore(
	fn: (store: FokosShardingStore, state: DurableObjectState) => void | Promise<void>,
	options?: FokosShardingStoreOptions,
): Promise<void> {
	const stub = testPartitionStub(`sharding-store-test.${crypto.randomUUID()}`);
	await runInDurableObject(stub, async (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
		const store = new FokosShardingStore(state.storage, options);
		store.runMigrations();
		await fn(store, state);
	});
}

describe("FokosShardingStore - migrations", () => {
	it("tracks its schema version under its own key and owns every fokos_ table", async () => {
		await withStore((_store, state) => {
			expect(state.storage.kv.get(FOKOS_KV_KEYS.SCHEMA_VERSION)).toBe(2);
			const tables = state.storage.sql
				.exec<{ name: string }>(`SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'fokos_%' ORDER BY name`)
				.toArray()
				.map((r) => r.name);
			expect(tables).toEqual([
				"fokos_admin_outbox",
				"fokos_range_hierarchy",
				"fokos_repartition_targets",
				"fokos_repartitions",
				"fokos_route_overrides",
			]);
		});
	});
});

describe("FokosShardingStore - learnRangeBoundary", () => {
	// The learned range tree of one hashPrefixBucket, with boundaries of the hash key "h":
	//   depth 1: [-∞,("h","m")) , [("h","m"),+∞)
	//   depth 2 (within [("h","m"),+∞)): [("h","m"),("h","t")) , [("h","t"),+∞)
	function seedTree(store: FokosShardingStore, hk = BUCKET) {
		store.learnRangeBoundary(hk, null, c("m"), 1, REFRESH_MS);
		store.learnRangeBoundary(hk, c("m"), null, 1, REFRESH_MS);
		store.learnRangeBoundary(hk, c("m"), c("t"), 2, REFRESH_MS);
		store.learnRangeBoundary(hk, c("t"), null, 2, REFRESH_MS);
	}

	it("returns null when nothing is stored", async () => {
		await withStore((store) => {
			expect(store.findDeepestKnownRangeSlice(BUCKET, c("p"))).toBeNull();
		});
	});

	it("returns the deepest slice containing the key", async () => {
		await withStore((store) => {
			seedTree(store);
			// "p" is in ["m","t") at depth 2, a strict sub-slice of ["m",+∞) at depth 1.
			expect(store.findDeepestKnownRangeSlice(BUCKET, c("p"))).toEqual({ depth: 2, startBoundary: c("m"), endBoundary: c("t") });
		});
	});

	it("selects an unbounded-end slice via the empty sentinel (decoded to null)", async () => {
		await withStore((store) => {
			seedTree(store);
			// "z" is in ["t",+∞) at depth 2 — only matched because the end sentinel is treated as +∞.
			expect(store.findDeepestKnownRangeSlice(BUCKET, c("z"))).toEqual({ depth: 2, startBoundary: c("t"), endBoundary: null });
		});
	});

	it("selects an unbounded-start slice (decoded to null)", async () => {
		await withStore((store) => {
			seedTree(store);
			// "a" only falls in [-∞,"m") at depth 1.
			expect(store.findDeepestKnownRangeSlice(BUCKET, c("a"))).toEqual({ depth: 1, startBoundary: null, endBoundary: c("m") });
		});
	});

	it("falls back to a shallower covering slice when the deeper slice lies to the side of the key", async () => {
		await withStore((store) => {
			const hk = BUCKET;
			// Only a depth-1 ["m",+∞) and a depth-2 ["t",+∞) are known; nothing at depth 2 covers ["m","t").
			store.learnRangeBoundary(hk, c("m"), null, 1, REFRESH_MS);
			store.learnRangeBoundary(hk, c("t"), null, 2, REFRESH_MS);
			// "p" is left of "t", so the depth-2 slice does not contain it — fall back to depth 1.
			expect(store.findDeepestKnownRangeSlice(hk, c("p"))).toEqual({ depth: 1, startBoundary: c("m"), endBoundary: null });
		});
	});

	it("goes up one depth at a time when the nearest slice at a depth ends before the key", async () => {
		await withStore((store) => {
			const hk = BUCKET;
			store.learnRangeBoundary(hk, c("m"), null, 1, REFRESH_MS);
			store.learnRangeBoundary(hk, c("m"), c("t"), 2, REFRESH_MS);
			store.learnRangeBoundary(hk, c("m"), c("p"), 3, REFRESH_MS);
			// The depth-3 slice ["m","p") is the nearest start for "r" but ends before it; depth 2 covers it.
			expect(store.findDeepestKnownRangeSlice(hk, c("r"))).toEqual({ depth: 2, startBoundary: c("m"), endBoundary: c("t") });
			// For "u", neither depth 3 nor depth 2 covers it, so the result is depth 1.
			expect(store.findDeepestKnownRangeSlice(hk, c("u"))).toEqual({ depth: 1, startBoundary: c("m"), endBoundary: null });
		});
	});

	it("returns null when no stored slice covers the key", async () => {
		await withStore((store) => {
			const hk = BUCKET;
			// Only the right half is known; "a" is left of every stored start.
			store.learnRangeBoundary(hk, c("m"), null, 1, REFRESH_MS);
			expect(store.findDeepestKnownRangeSlice(hk, c("a"))).toBeNull();
		});
	});

	it("isolates by hashPrefixBucket", async () => {
		await withStore((store) => {
			seedTree(store, BUCKET);
			expect(store.findDeepestKnownRangeSlice(OTHER_BUCKET, c("p"))).toBeNull();
		});
	});

	it("learns a known boundary again without a write, and refreshes its stamp once it is old", async () => {
		await withStore((store, state) => {
			const learnedAt = (start: string) =>
				state.storage.sql.exec<{ learned_at: number }>(`SELECT learned_at FROM fokos_range_hierarchy WHERE start_sk = ?`, kb(start)).one()
					.learned_at;
			store.learnRangeBoundary(BUCKET, c("m"), null, 1, REFRESH_MS, 1_000);
			// Within the refresh interval the row keeps its stamp: a hot boundary costs one seek and no write.
			store.learnRangeBoundary(BUCKET, c("m"), null, 1, REFRESH_MS, 30_000);
			expect(learnedAt("m")).toBe(1_000);
			// Past the interval the stamp moves, so eviction order follows use and not first sight.
			store.learnRangeBoundary(BUCKET, c("m"), null, 1, REFRESH_MS, 100_000);
			expect(learnedAt("m")).toBe(100_000);
			expect(store.countRangeHierarchyRows()).toBe(1);
		});
	});

	it("bounds the table: a learn past the bound evicts the oldest rows, deepest first", async () => {
		await withStore(
			(store, state) => {
				const hk = BUCKET;
				const rows = () =>
					state.storage.sql
						.exec<{ s: ArrayBuffer }>(`SELECT start_sk AS s FROM fokos_range_hierarchy ORDER BY s`)
						.toArray()
						.map((r) => KeyCodec.decode(KeyCodec.asKeyBytes(new Uint8Array(r.s))));
				// Two rows share the oldest stamp; the deeper one goes first.
				store.learnRangeBoundary(hk, c("a"), c("b"), 1, REFRESH_MS, 1_000);
				store.learnRangeBoundary(hk, c("b"), c("c"), 2, REFRESH_MS, 1_000);
				store.learnRangeBoundary(hk, c("c"), c("d"), 1, REFRESH_MS, 2_000);
				expect(store.countRangeHierarchyRows()).toBe(3);

				store.learnRangeBoundary(hk, c("d"), c("e"), 1, REFRESH_MS, 3_000);
				expect(store.countRangeHierarchyRows()).toBe(3);
				expect(rows()).toEqual(["a", "c", "d"]);

				store.learnRangeBoundary(hk, c("b"), c("c"), 2, REFRESH_MS, 4_000);
				expect(store.countRangeHierarchyRows()).toBe(3);
				expect(rows()).toEqual(["b", "c", "d"]);
			},
			{ rangeHierarchyMaxRows: () => 3 },
		);
	});

	it("evicts down to 90 % of the bound, so the next inserts do not evict", async () => {
		await withStore(
			(store, state) => {
				const hk = BUCKET;
				const learn = (i: number) => store.learnRangeBoundary(hk, c(`k${String(i).padStart(2, "0")}`), null, 1, REFRESH_MS, i);
				for (let i = 0; i < 20; i++) {
					learn(i);
				}
				expect(store.countRangeHierarchyRows()).toBe(20);

				learn(20);
				expect(store.countRangeHierarchyRows()).toBe(18);
				// The three oldest rows went.
				const oldest = state.storage.sql
					.exec<{ learned_at: number }>(`SELECT MIN(learned_at) AS learned_at FROM fokos_range_hierarchy`)
					.one().learned_at;
				expect(oldest).toBe(3);

				learn(21);
				learn(22);
				expect(store.countRangeHierarchyRows()).toBe(20);
				learn(23);
				expect(store.countRangeHierarchyRows()).toBe(18);
			},
			{ rangeHierarchyMaxRows: () => 20 },
		);
	});

	it("corrects the row count at eviction after rows were deleted", async () => {
		await withStore(
			(store) => {
				const hk = BUCKET;
				store.learnRangeBoundary(hk, c("a"), c("b"), 1, REFRESH_MS, 1_000);
				store.learnRangeBoundary(hk, c("b"), c("c"), 1, REFRESH_MS, 2_000);
				store.learnRangeBoundary(hk, c("c"), c("d"), 1, REFRESH_MS, 3_000);
				store.deleteLearnedRangeSlice(hk, { depth: 1, startBoundary: c("b"), endBoundary: c("c") });
				// Three rows fit the bound again, so this insert evicts nothing.
				store.learnRangeBoundary(hk, c("d"), c("e"), 1, REFRESH_MS, 4_000);
				expect(store.countRangeHierarchyRows()).toBe(3);
				store.learnRangeBoundary(hk, c("e"), c("f"), 1, REFRESH_MS, 5_000);
				expect(store.countRangeHierarchyRows()).toBe(3);
				expect(store.findDeepestKnownRangeSlice(hk, c("a"))).toBeNull();
			},
			{ rangeHierarchyMaxRows: () => 3 },
		);
	});

	it("reads the bound at each eviction, so a lower bound applies at the next write", async () => {
		let maxRows = 5;
		await withStore(
			(store) => {
				const hk = BUCKET;
				store.learnRangeBoundary(hk, c("a"), c("b"), 1, REFRESH_MS, 1_000);
				store.learnRangeBoundary(hk, c("b"), c("c"), 1, REFRESH_MS, 2_000);
				store.learnRangeBoundary(hk, c("c"), c("d"), 1, REFRESH_MS, 3_000);
				expect(store.countRangeHierarchyRows()).toBe(3);

				maxRows = 2;
				store.learnRangeBoundary(hk, c("d"), c("e"), 1, REFRESH_MS, 4_000);
				expect(store.countRangeHierarchyRows()).toBe(2);

				maxRows = 0;
				expect(() => store.learnRangeBoundary(hk, c("e"), c("f"), 1, REFRESH_MS, 5_000)).toThrow(
					invariantFailure(/rangeHierarchyMaxRows must be at least 1/),
				);
			},
			{ rangeHierarchyMaxRows: () => maxRows },
		);
	});
});

describe("FokosShardingStore - composite range boundaries", () => {
	it("finds the slice of a key between the boundaries of two hash keys of one hashPrefixBucket", async () => {
		await withStore((store) => {
			// Two hash keys share the hashPrefixBucket. A boundary between them has the empty sort key.
			const split = { hk: kb("b"), sk: UNBOUNDED };
			store.learnRangeBoundary(BUCKET, null, split, 1, REFRESH_MS);
			store.learnRangeBoundary(BUCKET, split, null, 1, REFRESH_MS);
			expect(store.findDeepestKnownRangeSlice(BUCKET, { hk: kb("a"), sk: kb("z") })).toEqual({
				depth: 1,
				startBoundary: null,
				endBoundary: split,
			});
			expect(store.findDeepestKnownRangeSlice(BUCKET, { hk: kb("b"), sk: UNBOUNDED })).toEqual({
				depth: 1,
				startBoundary: split,
				endBoundary: null,
			});
			expect(store.findDeepestKnownRangeSlice(BUCKET, { hk: kb("b"), sk: kb("a") })).toEqual({
				depth: 1,
				startBoundary: split,
				endBoundary: null,
			});
			expect(store.listLearnedRangeSlices(BUCKET)).toEqual([
				{ depth: 1, startBoundary: null, endBoundary: split },
				{ depth: 1, startBoundary: split, endBoundary: null },
			]);
		});
	});
});

describe("FokosShardingStore - KV records", () => {
	it("stores the import record, the plan, and the destroy fence under __fokos/ keys", async () => {
		await withStore((store, state) => {
			expect(store.getImport()).toBeUndefined();
			expect(store.getPlanHead("r1")).toBeUndefined();
			expect(store.isDestroying()).toBe(false);

			const source = { partitionId: "00", doName: "t.h.0" };
			const head = { schema: 1 as const, queue: { policy: { maxSizeMb: 1 }, data: { reason: "test" } }, planned: null, nextKey: null };
			store.putPlanHead("r1", head);
			expect(store.getPlanHead("r1")).toEqual(head);
			expect(state.storage.kv.get(FOKOS_KV_KEYS.planHead("r1"))).toBeDefined();
			// The chain deletes every linked item, so a head that names a second key leaves nothing behind.
			state.storage.kv.put("__fokos/repartition/r1/plan/00000002", { nextKey: null });
			store.putPlanHead("r1", { ...head, nextKey: "__fokos/repartition/r1/plan/00000002" });
			store.deletePlanChain("r1");
			expect(store.getPlanHead("r1")).toBeUndefined();
			expect(state.storage.kv.get("__fokos/repartition/r1/plan/00000002")).toBeUndefined();

			expect(store.getJobs()).toEqual({});
			store.putJobs({ stale_tx_recovery: { nextRunAt: 5 } });
			expect(store.getJobs()).toEqual({ stale_tx_recovery: { nextRunAt: 5 } });
			store.putJobs({});
			expect(state.storage.kv.get(FOKOS_KV_KEYS.JOBS)).toBeUndefined();

			store.putImport({
				schema: 2,
				state: "awaiting_data",
				repartitionId: "r1",
				source,
				slice: { kind: "hash_child", childIndex: 0, depth: 1 },
				cursor: null,
				attempts: 0,
				nextAttemptAt: 1,
				updatedAt: 1,
			});
			expect(store.getImport()?.state).toBe("awaiting_data");
			expect(state.storage.kv.get(FOKOS_KV_KEYS.IMPORT)).toBeDefined();

			store.setDestroying();
			expect(store.isDestroying()).toBe(true);
			expect(state.storage.kv.get(FOKOS_KV_KEYS.DESTROYING)).toBe(true);
		});
	});

	it("gives every store on one storage the destroy fence of the last write", async () => {
		await withStore((store, state) => {
			const other = new FokosShardingStore(state.storage);
			expect(other.isDestroying()).toBe(false);
			store.setDestroying();
			expect(other.isDestroying()).toBe(true);
		});
	});

	it("reads the destroy fence from storage again after a transaction that rolls back", async () => {
		await withStore((store) => {
			expect(store.isDestroying()).toBe(false);
			expect(() =>
				store.transactionSync(() => {
					store.setDestroying();
					throw new Error("roll back");
				}),
			).toThrow("roll back");
			// A fence that stayed true in memory would refuse every request of a partition that is not fenced.
			expect(store.isDestroying()).toBe(false);
		});
	});
});
