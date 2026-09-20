import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { PartitionDO } from "../server/do-partition.js";
import { testPartitionStub } from "../../test/stub-helpers.js";
import { KeyCodec } from "./key-codec.js";
import { FOKOS_KV_KEYS, FokosShardingStore, type FokosShardingStoreOptions } from "./sharding-store.js";

const kb = (s: string) => KeyCodec.encode(s);
const UNBOUNDED = KeyCodec.encodeOptional(undefined);

// Runs `fn` against a FokosShardingStore over REAL Durable Object storage. The PartitionDO constructor
// has already run the sharding migrations by the time the callback runs, and they are idempotent.
async function withStore(
	fn: (store: FokosShardingStore, state: DurableObjectState) => void | Promise<void>,
	options?: FokosShardingStoreOptions,
): Promise<void> {
	const stub = testPartitionStub(`sharding-store-test.${crypto.randomUUID()}`);
	await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
		const store = new FokosShardingStore(state.storage, options);
		store.runMigrations();
		await fn(store, state);
	});
}

describe("FokosShardingStore - migrations", () => {
	it("tracks its schema version under its own key and owns every fokos_ table", async () => {
		await withStore((_store, state) => {
			expect(state.storage.kv.get(FOKOS_KV_KEYS.SCHEMA_VERSION)).toBe(1);
			const tables = state.storage.sql
				.exec<{ name: string }>(`SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'fokos_%' ORDER BY name`)
				.toArray()
				.map((r) => r.name);
			expect(tables).toEqual(["fokos_range_hierarchy", "fokos_repartition_targets", "fokos_repartitions", "fokos_route_overrides"]);
		});
	});
});

describe("FokosShardingStore - learnRangeBoundary", () => {
	// A single hash key's learned range tree:
	//   depth 1: [-∞,"m") , ["m",+∞)
	//   depth 2 (within ["m",+∞)): ["m","t") , ["t",+∞)
	function seedTree(store: FokosShardingStore, hk = kb("h")) {
		store.learnRangeBoundary(hk, UNBOUNDED, kb("m"), 1);
		store.learnRangeBoundary(hk, kb("m"), UNBOUNDED, 1);
		store.learnRangeBoundary(hk, kb("m"), kb("t"), 2);
		store.learnRangeBoundary(hk, kb("t"), UNBOUNDED, 2);
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
			expect(store.findDeepestKnownRangeSlice(kb("h"), kb("p"))).toEqual({ depth: 2, startBoundary: kb("m"), endBoundary: kb("t") });
		});
	});

	it("selects an unbounded-end slice via the empty sentinel (decoded to null)", async () => {
		await withStore((store) => {
			seedTree(store);
			// "z" is in ["t",+∞) at depth 2 — only matched because the end sentinel is treated as +∞.
			expect(store.findDeepestKnownRangeSlice(kb("h"), kb("z"))).toEqual({ depth: 2, startBoundary: kb("t"), endBoundary: null });
		});
	});

	it("selects an unbounded-start slice (decoded to null)", async () => {
		await withStore((store) => {
			seedTree(store);
			// "a" only falls in [-∞,"m") at depth 1.
			expect(store.findDeepestKnownRangeSlice(kb("h"), kb("a"))).toEqual({ depth: 1, startBoundary: null, endBoundary: kb("m") });
		});
	});

	it("falls back to a shallower covering slice when the deeper slice lies to the side of the key", async () => {
		await withStore((store) => {
			const hk = kb("h");
			// Only a depth-1 ["m",+∞) and a depth-2 ["t",+∞) are known; nothing at depth 2 covers ["m","t").
			store.learnRangeBoundary(hk, kb("m"), UNBOUNDED, 1);
			store.learnRangeBoundary(hk, kb("t"), UNBOUNDED, 2);
			// "p" is left of "t", so the depth-2 slice does not contain it — fall back to depth 1.
			expect(store.findDeepestKnownRangeSlice(hk, kb("p"))).toEqual({ depth: 1, startBoundary: kb("m"), endBoundary: null });
		});
	});

	it("returns null when no stored slice covers the key", async () => {
		await withStore((store) => {
			const hk = kb("h");
			// Only the right half is known; "a" is left of every stored start.
			store.learnRangeBoundary(hk, kb("m"), UNBOUNDED, 1);
			expect(store.findDeepestKnownRangeSlice(hk, kb("a"))).toBeNull();
		});
	});

	it("isolates by hash key", async () => {
		await withStore((store) => {
			seedTree(store, kb("h"));
			expect(store.findDeepestKnownRangeSlice(kb("other"), kb("p"))).toBeNull();
		});
	});

	it("learns a known boundary again without a write, and refreshes its stamp once it is old", async () => {
		await withStore((store, state) => {
			const learnedAt = (start: string) =>
				state.storage.sql
					.exec<{ learned_at: number }>(`SELECT learned_at FROM fokos_range_hierarchy WHERE sk_start_boundary = ?`, kb(start))
					.one().learned_at;
			store.learnRangeBoundary(kb("h"), kb("m"), UNBOUNDED, 1, 1_000);
			// Within the refresh interval the row keeps its stamp: a hot boundary costs one seek and no write.
			store.learnRangeBoundary(kb("h"), kb("m"), UNBOUNDED, 1, 30_000);
			expect(learnedAt("m")).toBe(1_000);
			// Past the interval the stamp moves, so eviction order follows use and not first sight.
			store.learnRangeBoundary(kb("h"), kb("m"), UNBOUNDED, 1, 100_000);
			expect(learnedAt("m")).toBe(100_000);
			expect(store.countRangeHierarchyRows()).toBe(1);
		});
	});

	it("bounds the table: a learn past the bound evicts the oldest rows, deepest first", async () => {
		await withStore(
			(store, state) => {
				const hk = kb("h");
				const rows = () =>
					state.storage.sql
						.exec<{ s: ArrayBuffer }>(`SELECT sk_start_boundary AS s FROM fokos_range_hierarchy ORDER BY s`)
						.toArray()
						.map((r) => KeyCodec.decode(KeyCodec.asKeyBytes(new Uint8Array(r.s))));
				// Two rows share the oldest stamp; the deeper one goes first.
				store.learnRangeBoundary(hk, kb("a"), kb("b"), 1, 1_000);
				store.learnRangeBoundary(hk, kb("b"), kb("c"), 2, 1_000);
				store.learnRangeBoundary(hk, kb("c"), kb("d"), 1, 2_000);
				expect(store.countRangeHierarchyRows()).toBe(3);

				store.learnRangeBoundary(hk, kb("d"), kb("e"), 1, 3_000);
				expect(store.countRangeHierarchyRows()).toBe(3);
				expect(rows()).toEqual(["a", "c", "d"]);

				store.learnRangeBoundary(hk, kb("b"), kb("c"), 2, 4_000);
				expect(store.countRangeHierarchyRows()).toBe(3);
				expect(rows()).toEqual(["b", "c", "d"]);
			},
			{ rangeHierarchyMaxRows: 3 },
		);
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
});
