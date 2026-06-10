import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../do-partition.js";
import { PartitionContextCreator, type PartitionContext, type PartitionContextResolved } from "../partition-topology/partition-context.js";
import { PartitionIdHelper, rangePartitionDoName } from "../partition-topology/partition-id.js";
import { RANGE_PROMOTION_FRACTION } from "../partition-topology/split-policy.js";
import type { SplitStatusKVItem } from "../partition-topology/split-state.js";
import { PromotionManager, type PromotionPeer } from "./hash-key-promotion.js";
import { PartitionStore } from "./partition-store.js";
import type { InitFromSplitOptions } from "../partition-topology/partition-context.js";

const MAX_SIZE_MB = 1;
const THRESHOLD_BYTES = MAX_SIZE_MB * RANGE_PROMOTION_FRACTION * 1024 * 1024;

describe("PromotionManager — queue threshold", () => {
	it("queues a key at the maxSizeMb * RANGE_PROMOTION_FRACTION threshold and not below", async () => {
		await withPromotionEnv(async (penv) => {
			const { manager, pCtx, store, scheduled } = penv;

			manager.maybeQueuePromotion(pCtx, "alice", THRESHOLD_BYTES - 1);
			expect(manager.statusFor("alice")).toBeUndefined();
			expect(store.getPromotedKeyStatus("alice")).toBeUndefined();
			expect(scheduled).toEqual([]);

			manager.maybeQueuePromotion(pCtx, "alice", THRESHOLD_BYTES);
			expect(manager.statusFor("alice")).toBe("queued");
			expect(store.getPromotedKeyStatus("alice")).toBe("queued");
			expect(scheduled).toEqual([10]);
			expect(manager.hasInFlightPromotions()).toBe(true);
			expect(manager.needsBackgroundWork()).toBe(true);
		});
	});

	it("is a no-op on non-hash partitions and for already-tracked keys", async () => {
		await withPromotionEnv(async (penv) => {
			const { manager, pCtx, base, store, scheduled } = penv;

			manager.maybeQueuePromotion(rangeCtx(base, "alice", null, null), "alice", THRESHOLD_BYTES * 10);
			expect(store.getPromotedKeyStatus("alice")).toBeUndefined();

			manager.maybeQueuePromotion(pCtx, "alice", THRESHOLD_BYTES);
			manager.maybeQueuePromotion(pCtx, "alice", THRESHOLD_BYTES * 10);
			expect(scheduled).toEqual([10]); // only the first call queued anything
		});
	});

	it("resyncs a stale cache from storage instead of clobbering an advanced status back to queued", async () => {
		await withPromotionEnv(async (penv) => {
			const { manager, pCtx, store, scheduled } = penv;
			// The row advanced in storage (e.g. before a crash) but this manager's cache is empty.
			store.insertPromotedKey("alice", "queued", 1);
			store.updatePromotedKeyStatus("alice", "queued", "promoting", 2);

			manager.maybeQueuePromotion(pCtx, "alice", THRESHOLD_BYTES);
			expect(manager.statusFor("alice")).toBe("promoting");
			expect(store.getPromotedKeyStatus("alice")).toBe("promoting");
			expect(scheduled).toEqual([]);
		});
	});
});

describe("PromotionManager — drive and cutover", () => {
	it("drives a queued key: init range root → cutover to 'promoting' → trigger migration", async () => {
		await withPromotionEnv(async (penv) => {
			const { manager, pCtx, store, calls } = penv;
			manager.maybeQueuePromotion(pCtx, "alice", THRESHOLD_BYTES);

			await manager.drive(pCtx, () => undefined);

			expect(calls.inits).toHaveLength(1);
			expect(calls.inits[0].splitType).toBe("range");
			expect(calls.inits[0].parentPartitionContext.doName).toBe(pCtx.doName);
			expect(calls.inits[0].newPartitionContext.rangePartition).toEqual({ hashKey: "alice", startBoundary: null, endBoundary: null });
			expect(calls.triggers).toBe(1);
			expect(manager.statusFor("alice")).toBe("promoting");
			expect(store.getPromotedKeyStatus("alice")).toBe("promoting");
			expect(manager.activeRangeRootHashKeys()).toEqual(["alice"]);
		});
	});

	it("defers cutover while the key holds a pending lock, then completes on re-drive", async () => {
		await withPromotionEnv(async (penv) => {
			const { manager, pCtx, store, calls } = penv;
			manager.maybeQueuePromotion(pCtx, "alice", THRESHOLD_BYTES);
			insertLock(store, "alice", "tx1");

			await manager.drive(pCtx, () => undefined);
			expect(calls.inits).toHaveLength(1); // root init happens before the cutover check
			expect(calls.triggers).toBe(0);
			expect(manager.statusFor("alice")).toBe("queued");
			expect(store.getPromotedKeyStatus("alice")).toBe("queued");
			expect(manager.needsBackgroundWork()).toBe(true); // retried on a later cycle

			store.deletePendingTx("tx1");
			await manager.drive(pCtx, () => undefined);
			expect(calls.triggers).toBe(1);
			expect(manager.statusFor("alice")).toBe("promoting");
		});
	});

	it("re-drive after a successful cutover is a no-op (idempotent)", async () => {
		await withPromotionEnv(async (penv) => {
			const { manager, pCtx, calls } = penv;
			manager.maybeQueuePromotion(pCtx, "alice", THRESHOLD_BYTES);

			await manager.drive(pCtx, () => undefined);
			await manager.drive(pCtx, () => undefined);

			expect(calls.inits).toHaveLength(1);
			expect(calls.triggers).toBe(1);
			expect(manager.statusFor("alice")).toBe("promoting");
		});
	});

	it("skips promotion entirely while a hash split is queued or started (mutual exclusion)", async () => {
		await withPromotionEnv(async (penv) => {
			const { manager, pCtx, calls } = penv;
			manager.maybeQueuePromotion(pCtx, "alice", THRESHOLD_BYTES);
			const splitQueued: SplitStatusKVItem = { status: "split_queued", splitType: "hash", createdAt: 0, partitionContext: pCtx };

			await manager.drive(pCtx, () => splitQueued);

			expect(calls.inits).toHaveLength(0);
			expect(calls.triggers).toBe(0);
			expect(manager.statusFor("alice")).toBe("queued");
		});
	});

	it("a per-key failure is logged and does not block the remaining queued keys", async () => {
		await withPromotionEnv(async (penv) => {
			const { manager, pCtx, store, calls } = penv;
			manager.maybeQueuePromotion(pCtx, "alice", THRESHOLD_BYTES);
			manager.maybeQueuePromotion(pCtx, "bob", THRESHOLD_BYTES);
			calls.failInitFor.add("alice"); // every init for alice fails (exhausts the ≤5 retries)

			await manager.drive(pCtx, () => undefined);

			expect(manager.statusFor("alice")).toBe("queued");
			expect(store.getPromotedKeyStatus("bob")).toBe("promoting");
			expect(calls.triggers).toBe(1);
		});
	});

	it("cutover races: storage no longer 'queued' → cache resynced, no migration trigger", async () => {
		await withPromotionEnv(async (penv) => {
			const { manager, pCtx, store, calls } = penv;
			manager.maybeQueuePromotion(pCtx, "alice", THRESHOLD_BYTES);
			// The status advanced in storage behind this cache (e.g. a concurrent ack path).
			store.updatePromotedKeyStatus("alice", "queued", "promoting", 2);
			store.updatePromotedKeyStatus("alice", "promoting", "promoted", 3);

			await manager.drive(pCtx, () => undefined);

			expect(calls.triggers).toBe(0);
			expect(manager.statusFor("alice")).toBe("promoted");
		});
	});
});

describe("PromotionManager — acknowledgePromotionComplete", () => {
	it("transitions promoting → promoted and schedules the GC cycle; re-ack is idempotent", async () => {
		await withPromotionEnv(async (penv) => {
			const { manager, pCtx, store, scheduled } = penv;
			manager.maybeQueuePromotion(pCtx, "alice", THRESHOLD_BYTES);
			await manager.drive(pCtx, () => undefined);
			scheduled.length = 0;

			manager.acknowledgePromotionComplete("alice");
			expect(manager.statusFor("alice")).toBe("promoted");
			expect(store.getPromotedKeyStatus("alice")).toBe("promoted");
			expect(scheduled).toEqual([1_000]);
			expect(manager.hasInFlightPromotions()).toBe(false);
			expect(manager.activeRangeRootHashKeys()).toEqual(["alice"]);

			manager.acknowledgePromotionComplete("alice");
			expect(manager.statusFor("alice")).toBe("promoted");
			expect(store.getPromotedKeyStatus("alice")).toBe("promoted");
		});
	});
});

describe("PromotionManager — GC", () => {
	it("deletes a promoted key's items in bounded batches across cycles, leaving other keys intact", async () => {
		await withPromotionEnv(async (penv) => {
			const { store, pCtx, calls, scheduled } = penv;
			const manager = new PromotionManager({
				store,
				getRangeRootPeer: () => makePeer(calls),
				scheduleWork: (opts) => scheduled.push(opts.delayMs),
				logParams: () => ({ test: "promotion.test.ts" }),
				gcBatchLimit: 2,
			});
			for (const sk of ["1", "2", "3"]) {
				store.upsertItem({ hk: "alice", sk, data: "x", ttlEpochUtcSeconds: null, lastTransactionTs: 0 });
			}
			store.upsertItem({ hk: "bob", sk: "1", data: "y", ttlEpochUtcSeconds: null, lastTransactionTs: 0 });
			insertLock(store, "alice", "tx1");
			store.insertPromotedKey("alice", "promoted", 1);
			manager.loadFromStorage();

			// Cycle 1: bounded batch (2 of 3 rows) — residual work remains.
			manager.runGC();
			expect(store.hasItemsForHashKey("alice")).toBe(true);
			expect(store.pendingLockFor("alice", "1")).toBeUndefined();
			expect(manager.needsBackgroundWork()).toBe(true);

			// Cycle 2: drains the residue.
			manager.runGC();
			expect(store.hasItemsForHashKey("alice")).toBe(false);
			expect(store.hasItemsForHashKey("bob")).toBe(true);
			expect(manager.needsBackgroundWork()).toBe(false);
		});
	});
});

describe("PromotionManager — cache views", () => {
	it("loadFromStorage + snapshot + inheritKey expose the storage truth", async () => {
		await withPromotionEnv(async (penv) => {
			const { manager, store } = penv;
			store.insertPromotedKey("alice", "promoted", 1);
			store.insertPromotedKey("bob", "queued", 1);
			manager.loadFromStorage();

			expect(manager.snapshot().sort((a, b) => a.hashKey.localeCompare(b.hashKey))).toEqual([
				{ hashKey: "alice", status: "promoted" },
				{ hashKey: "bob", status: "queued" },
			]);
			expect(manager.has("alice")).toBe(true);
			expect(manager.statusFor("bob")).toBe("queued");

			manager.inheritKey("carol", "promoting");
			expect(manager.statusFor("carol")).toBe("promoting");
			expect(manager.activeRangeRootHashKeys().sort()).toEqual(["alice", "carol"]);
		});
	});
});

// ─── Harness ──────────────────────────────────────────────────────────────────

function makeBase(): PartitionContext {
	return PartitionContextCreator.create({
		ns: "PARTITION_DO",
		databaseName: `promotion-${crypto.randomUUID()}`,
		rootTreesN: 1,
		hashSplitN: 2,
		hashSplitConditions: { maxSizeMb: MAX_SIZE_MB },
	});
}

function hashCtx(base: PartitionContext, idxs: number[]): PartitionContextResolved {
	const { opaque, doName } = PartitionIdHelper.fromHashIdxs(base, idxs).encode(true);
	return { ...base, doName: doName!, primaryDoIdStr: "", partitionId: opaque };
}

function rangeCtx(base: PartitionContext, hashKey: string, start: string | null, end: string | null): PartitionContextResolved {
	const { opaque } = PartitionIdHelper.fromRangePartition(base, hashKey, start, end).encode(false);
	return {
		...base,
		doName: rangePartitionDoName(base.databaseName, hashKey, start, end),
		primaryDoIdStr: "",
		partitionId: opaque,
		rangePartition: { hashKey, startBoundary: start, endBoundary: end },
	};
}

function insertLock(store: PartitionStore, hk: string, transactionId: string): void {
	store.insertPendingLock({
		hk,
		sk: "1",
		transaction_id: transactionId,
		transaction_ts: 123,
		operation: "put",
		data: "d",
		conditions_json: null,
		coordinator_do_id: "tc-1",
		created_at: 1000,
	});
}

type PeerCalls = {
	inits: InitFromSplitOptions[];
	triggers: number;
	/** Hash keys whose range-root initFromSplit should keep failing (exhausting the retry loop). */
	failInitFor: Set<string>;
};

function makePeer(calls: PeerCalls): PromotionPeer {
	return {
		async initFromSplit(opts) {
			const hk = opts.newPartitionContext.rangePartition?.hashKey ?? "";
			if (calls.failInitFor.has(hk)) throw new Error("simulated range-root init failure");
			calls.inits.push(opts);
		},
		async triggerMigration() {
			calls.triggers++;
		},
	};
}

type PromotionEnv = {
	base: PartitionContext;
	pCtx: PartitionContextResolved;
	store: PartitionStore;
	manager: PromotionManager;
	calls: PeerCalls;
	scheduled: number[];
};

// Real DO storage (vitest-pool-workers); the range-root peer is the only fake — the gateway
// pattern makes the lifecycle testable without spinning up a real range root.
async function withPromotionEnv(fn: (penv: PromotionEnv) => Promise<void>): Promise<void> {
	const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(`promotion-test.${crypto.randomUUID()}`));
	await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
		const base = makeBase();
		const pCtx = hashCtx(base, [0]);
		const store = new PartitionStore(state.storage);
		const calls: PeerCalls = { inits: [], triggers: 0, failInitFor: new Set() };
		const scheduled: number[] = [];
		const manager = new PromotionManager({
			store,
			getRangeRootPeer: () => makePeer(calls),
			scheduleWork: (opts) => scheduled.push(opts.delayMs),
			logParams: () => ({ test: "promotion.test.ts" }),
		});
		await fn({ base, pCtx, store, manager, calls, scheduled });
	});
}
