/**
 * `RepartitionSource` and `RepartitionTarget` driven step by step, over real `PartitionStore` instances.
 *
 * Every test drives the source and the target by hand and inspects the rows between steps, so a
 * failure names the transition that broke rather than the split that did not finish. The suite needs
 * no Durable Object of its own beyond storage: the peer adapter in the harness lands each control
 * call on the receiving half directly.
 */
import { describe, expect, it } from "vitest";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { hashChildIndex } from "../../src/sharding/hash-primitives.js";
import { REPARTITION_KV_KEYS, type RepartitionPlan } from "../../src/sharding/repartition-flow.js";
import { kb, keySizeEstimate, makeCluster, putItem, putLock, storedBytes, T0, type Node } from "./repartition-harness.js";
import type { KeyBytes } from "../../src/sharding/key-codec.js";
import type { PartitionStore, RepartitionKind } from "../../src/shared/partition/partition-store.js";

/**
 * Asserts a jittered retry deadline. `jitterBackoff` picks uniformly from [0, 2^attempt * base) and
 * caps the result at `max`. A test can therefore assert the window only, which is the part that
 * matters: the delay grows with the attempt and never passes the cap.
 */
function expectBackoffWindow(actual: number, now: number, attempt: number, base = 5_000, max = 300_000): void {
	const upper = Math.min(2 ** attempt * base, max);
	expect(actual).toBeGreaterThanOrEqual(now);
	expect(actual).toBeLessThan(now + upper);
}

/** A hash key each test can point at a known child of the root. */
function keyForChild(childIndex: number, hashSplitN: number, prefix = "k"): string {
	for (let i = 0; i < 100_000; i++) {
		const key = `${prefix}-${i}`;
		if (hashChildIndex(kb(key), 0, hashSplitN) === childIndex) return key;
	}
	throw new Error("no key found for child");
}

describe("Repartition — arbitration", () => {
	it("queues a hash split, and refuses a second one", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await root.enter(({ source, store }) => {
			expect(source.queue({ kind: "hash_split" })).toMatchObject({ id: "r1", kind: "hash_split", state: "queued" });
			expect(source.queue({ kind: "hash_split" })).toBeUndefined();
			expect(store.getSplitRepartition()?.id).toBe("r1");
		});
	});

	it("lets an unfinished promotion block a hash split, and a finished one through", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await root.enter(({ source, store }) => {
			expect(source.queue({ kind: "key_promotion", hashKey: kb("hot") })).toMatchObject({ id: "r1" });
			// queued, planned and cutover all still own the key's move.
			expect(source.queue({ kind: "hash_split" })).toBeUndefined();
			store.setRepartitionState("r1", "cutover");
			expect(source.queue({ kind: "hash_split" })).toBeUndefined();

			store.setRepartitionState("r1", "completed");
			expect(source.queue({ kind: "hash_split" })).toMatchObject({ id: "r2", kind: "hash_split" });
		});
	});

	it("lets a split row in any state block every later promotion", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await root.enter(({ source, store }) => {
			expect(source.queue({ kind: "hash_split" })).toMatchObject({ id: "r1" });
			for (const state of ["queued", "planned", "cutover", "completed", "cleaned"] as const) {
				store.setRepartitionState("r1", state);
				expect(source.queue({ kind: "key_promotion", hashKey: kb("hot") }), `blocked at ${state}`).toBeUndefined();
			}
		});
	});

	it("refuses a second promotion of one key, and allows two keys at once", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await root.enter(({ source, store }) => {
			expect(source.queue({ kind: "key_promotion", hashKey: kb("alice") })).toMatchObject({ id: "r1" });
			expect(source.queue({ kind: "key_promotion", hashKey: kb("alice") })).toBeUndefined();
			// A different key is unrelated: two promotions progress at the same time.
			expect(source.queue({ kind: "key_promotion", hashKey: kb("bob") })).toMatchObject({ id: "r2" });
			expect(store.routeOverrideFor(kb("alice"))).toEqual({ repartitionId: "r1", state: "queued" });
			expect(store.routeOverrideFor(kb("bob"))).toEqual({ repartitionId: "r2", state: "queued" });
		});
	});

	it("refuses a range split on a hash partition and a promotion on a range partition", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		const rangeRoot = c.rangeNode(root.ctx, kb("alice"), null, null);
		await root.enter(({ source }) => expect(source.queue({ kind: "range_split" })).toBeUndefined());
		// The range node has no stored context yet, so it answers from the context the harness gave it.
		await rangeRoot.enter(({ source }) => {
			expect(source.queue({ kind: "key_promotion", hashKey: kb("alice") })).toBeUndefined();
			expect(source.queue({ kind: "range_split" })).toMatchObject({ kind: "range_split", state: "queued" });
		});
	});
});

describe("Repartition — planning", () => {
	it("writes the plan, every target and planned, in target_index order", async () => {
		const c = makeCluster({ hashSplitN: 3 });
		const root = c.hashNode([0]);
		await root.enter(({ source }) => source.queue({ kind: "hash_split" }));
		await root.enter(async ({ source, store, storage }) => {
			expect(await source.sourceStep()).toBe("progressed");

			const row = store.getRepartition("r1")!;
			expect(row.state).toBe("planned");
			const targets = store.listRepartitionTargets("r1", "hash_split");
			expect(targets.map((t) => t.targetIndex)).toEqual([0, 1, 2]);
			expect(targets.map((t) => t.slice)).toEqual([
				{ kind: "hash_child", childIndex: 0 },
				{ kind: "hash_child", childIndex: 1 },
				{ kind: "hash_child", childIndex: 2 },
			]);
			expect(targets.every((t) => t.initialization === "pending")).toBe(true);

			const plan = storage.kv.get<RepartitionPlan>(REPARTITION_KV_KEYS.plan("r1"))!;
			expect(plan.source).toEqual({ partitionId: root.ctx.partitionId, doName: root.ctx.doName });
			// The boundaries ARE the target slices, so the plan does not repeat them.
			expect(Object.keys(plan).sort()).toEqual(["schema", "source"]);
		});
	});

	it("keeps a range split queued and backs off when the interval cannot yield N children", async () => {
		const c = makeCluster({ rangeSplitN: 4 });
		const root = c.hashNode([0]);
		const rangeRoot = c.rangeNode(root.ctx, kb("alice"), null, null);
		await rangeRoot.enter(({ source, store }) => {
			putItem(store, "alice", "s1");
			source.queue({ kind: "range_split" }, T0);
		});
		await rangeRoot.enter(async ({ source, store }) => {
			expect(await source.sourceStep(T0)).toBe("progressed");
			// Only a new write can change the answer, so the row waits rather than failing.
			const row = store.getRepartition("r1")!;
			expect(row.state).toBe("queued");
			expect(row.attempts).toBe(1);
			expectBackoffWindow(row.nextAttemptAt, T0, 0);
			expect(store.listRepartitionTargets("r1", "range_split")).toEqual([]);
		});
		// The window doubles while the answer stays the same, and it stops at 5 minutes.
		await rangeRoot.enter(async ({ source, store }) => {
			store.setRepartitionAttempt("r1", 1, 0);
			await source.sourceStep(T0 + 5_000);
			expectBackoffWindow(store.getRepartition("r1")!.nextAttemptAt, T0 + 5_000, 1);

			store.setRepartitionAttempt("r1", 20, 0);
			await source.sourceStep(T0 + 20_000);
			expectBackoffWindow(store.getRepartition("r1")!.nextAttemptAt, T0 + 20_000, 20);
		});
	});

	it("plans a range split with boundaries, child slices that tile the interval, and its ancestors", async () => {
		const c = makeCluster({ rangeSplitN: 2 });
		const root = c.hashNode([0]);
		const rangeRoot = c.rangeNode(root.ctx, kb("alice"), null, null);
		await rangeRoot.enter(async ({ source, store }) => {
			for (const sk of ["s1", "s2", "s3", "s4"]) putItem(store, "alice", sk, "x".repeat(200));
			source.queue({ kind: "range_split" });
			expect(await source.sourceStep()).toBe("progressed");

			const targets = store.listRepartitionTargets("r1", "range_split");
			expect(targets).toHaveLength(2);
			const first = targets[0].slice as { start: KeyBytes | null; end: KeyBytes };
			const second = targets[1].slice as { start: KeyBytes; end: KeyBytes | null };
			// The children tile [start, end) with no gap: the first ends exactly where the second begins.
			expect(first.start).toBeNull();
			expect(second.end).toBeNull();
			expect(KeyCodec.compare(first.end, second.start)).toBe(0);
		});
	});
});

describe("Repartition — initialization and cutover", () => {
	it("marks a target initializing before its call and initialized after it", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await plan(root, { kind: "hash_split" });
		await root.enter(async ({ source, store }) => {
			expect(await source.sourceStep(T0)).toBe("progressed");
			const targets = store.listRepartitionTargets("r1", "hash_split");
			expect(targets.map((t) => t.initialization)).toEqual(["initialized", "initialized"]);
			// Every target is initialized, so the next step is the cutover.
			expect(store.getRepartition("r1")!.state).toBe("planned");
			expect(await source.sourceStep(T0)).toBe("progressed");
			expect(store.getRepartition("r1")!.state).toBe("cutover");
		});
	});

	it("keeps going for the other targets when one init fails, and retries only that one", async () => {
		const c = makeCluster({ hashSplitN: 3 });
		const root = c.hashNode([0]);
		await plan(root, { kind: "hash_split" });
		const failing = await root.enter(({ store }) => store.listRepartitionTargets("r1", "hash_split")[1].doName);
		c.failNextInit(failing);
		let failedDeadline = 0;

		await root.enter(async ({ source, store }) => {
			expect(await source.sourceStep(T0)).toBe("progressed");
			const byName = new Map(store.listRepartitionTargets("r1", "hash_split").map((t) => [t.doName, t]));
			for (const [name, t] of byName) {
				if (name === failing) {
					// It stays `initializing`: the call may have arrived and lost its reply, so the retry
					// repeats the same idempotent fokosInit rather than treating it as never started.
					expect(t.initialization).toBe("initializing");
					expect(t.attempts).toBe(1);
					expectBackoffWindow(t.nextAttemptAt, T0, 1);
					failedDeadline = t.nextAttemptAt;
				} else {
					expect(t.initialization, name).toBe("initialized");
				}
			}
			// The repartition's own deadline follows the one target that still needs a call.
			expect(store.getRepartition("r1")!.nextAttemptAt).toBe(failedDeadline);
		});

		await root.enter(async ({ source, store }) => {
			expect(await source.sourceStep(failedDeadline)).toBe("progressed");
			expect(store.listRepartitionTargets("r1", "hash_split").every((t) => t.initialization === "initialized")).toBe(true);
		});
	});

	it("holds a lock-blocked promotion at its target, and moves on once the lock goes", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await root.enter(({ source, store }) => {
			putItem(store, "alice", "s1");
			putLock(store, "alice", "s1");
			source.queue({ kind: "key_promotion", hashKey: kb("alice") }, T0);
		});
		await root.enter(async ({ source }) => void (await source.sourceStep(T0)));

		await root.enter(async ({ source, store }) => {
			expect(await source.sourceStep(T0)).toBe("progressed");
			const targetRow = store.listRepartitionTargets("r1", "key_promotion")[0];
			// The target is never created while a lock is held, and the retry is flat: only a commit or a
			// cancel can change the answer, so backing off would only slow the promotion down.
			expect(targetRow.initialization).toBe("pending");
			expect(targetRow.nextAttemptAt).toBe(T0 + 5_000);
			expect(targetRow.attempts).toBe(0);
		});

		await root.enter(async ({ source, store }) => {
			store.deletePendingTx("tx-1");
			expect(await source.sourceStep(T0 + 5_000)).toBe("progressed");
			expect(store.listRepartitionTargets("r1", "key_promotion")[0].initialization).toBe("initialized");
		});
	});

	it("checks the lock count again at cutover, so a lock that appears during init defers it", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await root.enter(({ source, store }) => {
			putItem(store, "alice", "s1");
			source.queue({ kind: "key_promotion", hashKey: kb("alice") }, T0);
		});
		await root.enter(async ({ source }) => void (await source.sourceStep(T0)));
		await root.enter(async ({ source }) => void (await source.sourceStep(T0)));

		await root.enter(async ({ source, store }) => {
			// The lock arrives after the range root exists but before routing moved.
			putLock(store, "alice", "s1");
			expect(await source.sourceStep(T0)).toBe("progressed");
			expect(store.getRepartition("r1")!.state).toBe("planned");
			expect(store.getRepartition("r1")!.nextAttemptAt).toBe(T0 + 5_000);

			store.deletePendingTx("tx-1");
			expect(await source.sourceStep(T0 + 5_000)).toBe("progressed");
			expect(store.getRepartition("r1")!.state).toBe("cutover");
		});
	});

	it("deletes the plan at cutover and reuses it after a partial initialization", async () => {
		const c = makeCluster({ hashSplitN: 3 });
		const root = c.hashNode([0]);
		await plan(root, { kind: "hash_split" });
		const failing = await root.enter(({ store }) => store.listRepartitionTargets("r1", "hash_split")[2].doName);
		c.failNextInit(failing);

		await root.enter(async ({ source, storage }) => {
			await source.sourceStep(T0);
			// The plan survives a partial fan-out; the retry initializes against exactly the same one.
			expect(storage.kv.get<RepartitionPlan>(REPARTITION_KV_KEYS.plan("r1"))).toBeDefined();
		});
		await root.enter(async ({ source }) => void (await source.sourceStep(T0 + 300_000)));
		await root.enter(async ({ source, store, storage }) => {
			await source.sourceStep(T0 + 300_000);
			expect(store.getRepartition("r1")!.state).toBe("cutover");
			// Spent: every target is initialized and the target rows hold every routing slice.
			expect(storage.kv.get<RepartitionPlan>(REPARTITION_KV_KEYS.plan("r1"))).toBeUndefined();
		});
	});
});

describe("Repartition — the migration protocol", () => {
	it("runs a hash split end to end: pages, imported, ack, completed, cleaned", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		const keyA = keyForChild(0, c.base.hashSplitN, "a");
		const keyB = keyForChild(1, c.base.hashSplitN, "b");

		await root.enter(({ source, store }) => {
			putItem(store, keyA, "s1");
			putItem(store, keyA, "s2");
			putItem(store, keyB, "s1");
			putLock(store, keyB, "s2", "tx-b");
			source.queue({ kind: "hash_split" });
		});
		await cutOver(root);

		const targets = await root.enter(({ store }) => store.listRepartitionTargets("r1", "hash_split"));
		const childA = c.node({ ...c.base, doName: targets[0].doName, primaryDoIdStr: "", partitionId: targets[0].partitionId });
		const childB = c.node({ ...c.base, doName: targets[1].doName, primaryDoIdStr: "", partitionId: targets[1].partitionId });

		// Each child pulls one page at a time until its own record says imported.
		for (const child of [childA, childB]) await drainImport(child);

		await childA.enter(({ store, target }) => {
			expect(target.importState()).toBe("imported");
			expect(store.queryItemsPage(null, 100).map((r) => KeyCodec.decode(r.sk))).toEqual(["s1", "s2"]);
		});
		await childA.enter(({ storage }) => {
			// The estimate is maintained page by page, from the sizes SQLite measured on each insert, so
			// no whole-table rebuild closes the import.
			expect(keySizeEstimate(storage, keyA)).toBe(storedBytes(storage, keyA));
		});
		await childB.enter(({ store }) => {
			expect(store.queryItemsPage(null, 100)).toHaveLength(1);
			// A lock inside the slice follows its key, so commit or cancel can still find it.
			expect(store.queryPendingTxPage(null, 10).map((r) => r.transaction_id)).toEqual(["tx-b"]);
		});

		// The acknowledgements complete the source, which then drops its now-redundant lock copies.
		for (const child of [childA, childB]) await child.enter(async ({ target }) => void (await target.sendAck()));
		await childA.enter(({ target }) => expect(target.importState()).toBe("active"));
		await root.enter(({ store }) => {
			expect(store.getRepartition("r1")!.state).toBe("completed");
			expect(store.queryPendingTxPage(null, 10)).toEqual([]);
			// A split keeps its item rows: only a promotion gives them back.
			expect(store.queryItemsPage(null, 100)).toHaveLength(3);
		});

		await root.enter(({ source, store }) => {
			expect(source.sourceCleanupStep()).toBe("progressed");
			expect(store.getRepartition("r1")!.state).toBe("cleaned");
			expect(store.queryItemsPage(null, 100)).toHaveLength(3);
			expect(source.sourceCleanupStep()).toBe("idle");
		});
	});

	it("refuses a pull before cutover and after completion", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await plan(root, { kind: "hash_split" });
		const target = await root.enter(({ store }) => store.listRepartitionTargets("r1", "hash_split")[0]);
		const ref = { partitionId: target.partitionId, doName: target.doName };

		await root.enter(({ source }) => {
			expect(() => source.servePage({ repartitionId: "r1", target: ref, cursor: null })).toThrow(
				fokosErrorWith("repartition_not_cut_over"),
			);
		});
		await root.enter(({ source, store }) => {
			store.setRepartitionState("r1", "completed");
			// The rows may already have gone back, so the source stops answering rather than serving a
			// page that is short of what the target asked for.
			expect(() => source.servePage({ repartitionId: "r1", target: ref, cursor: null })).toThrow(fokosErrorWith("partition_migrating"));
		});
	});

	it("resolves a read-through slice after a split completes, but not after a promotion reclaimed its rows", async () => {
		const split = makeCluster().hashNode([0]);
		await plan(split, { kind: "hash_split" });
		const splitTarget = await split.enter(({ store }) => store.listRepartitionTargets("r1", "hash_split")[0]);

		await split.enter(({ source, store }) => {
			store.setRepartitionState("r1", "completed");
			// A split source keeps its item rows for life, so a read-through caller still gets its slice.
			expect(source.resolveCallerSlice("r1", splitTarget)).toMatchObject({ kind: "hash_child" });
		});

		const promo = makeCluster().hashNode([0]);
		const key = kb("alice");
		await plan(promo, { kind: "key_promotion", hashKey: key });
		const promoTarget = await promo.enter(({ store }) => store.listRepartitionTargets("r1", "key_promotion")[0]);

		await promo.enter(({ source, store }) => {
			store.setRepartitionState("r1", "cleaned");
			// The rows of the key went back to the range tree, so nothing here is left to read. No correct
			// target can ask, because it reaches `imported` before its acknowledgement completes the
			// promotion. This is a protocol defect, and not a condition that a retry can clear.
			expect(() => source.resolveCallerSlice("r1", promoTarget)).toThrow(fokosErrorWith("repartition_slice_reclaimed"));
		});
	});

	it("refuses an unknown repartition and an unknown target on both pull and ack", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await cutOver(root, { kind: "hash_split" });
		const targetRow = await root.enter(({ store }) => store.listRepartitionTargets("r1", "hash_split")[0]);

		await root.enter(({ source }) => {
			expect(() => source.servePage({ repartitionId: "nope", target: targetRow, cursor: null })).toThrow(
				fokosErrorWith("repartition_unknown"),
			);
			expect(() => source.acceptAck({ repartitionId: "nope", target: targetRow })).toThrow(fokosErrorWith("repartition_unknown"));

			// Both halves of the identity must match: a name alone is a value the caller chose.
			const wrongId = { partitionId: "00ff", doName: targetRow.doName };
			const wrongName = { partitionId: targetRow.partitionId, doName: "someone-else" };
			expect(() => source.servePage({ repartitionId: "r1", target: wrongId, cursor: null })).toThrow(
				fokosErrorWith("repartition_target_unknown"),
			);
			expect(() => source.acceptAck({ repartitionId: "r1", target: wrongName })).toThrow(fokosErrorWith("repartition_target_unknown"));
		});
	});

	it("does no more work once the import is complete", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await root.enter(({ source, store }) => {
			putItem(store, keyForChild(0, c.base.hashSplitN, "a"), "s1");
			source.queue({ kind: "hash_split" });
		});
		await cutOver(root);
		const child = await firstChild(c, root);
		await drainImport(child);

		await child.enter(async ({ target, store }) => {
			expect(target.importState()).toBe("imported");
			// A user delete lands on the finished copy.
			store.deleteItem({ hk: kb(keyForChild(0, c.base.hashSplitN, "a")), sk: kb("s1"), txOrderTs: 99 });
			// A page that arrives now must not put the row back: the record says the import is over, and
			// the ingest inserts an absent row rather than failing on it.
			expect(await target.importOnePage()).toBe("idle");
			expect(store.queryItemsPage(null, 10)).toHaveLength(0);
		});
	});

	it("rejects a page of the wrong phase and applies nothing", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await root.enter(({ source, store }) => {
			putItem(store, keyForChild(0, c.base.hashSplitN, "a"), "s1");
			source.queue({ kind: "hash_split" });
		});
		await cutOver(root);
		const child = await firstChild(c, root);

		// The record is at the overrides phase, so a host page answers a question it did not ask.
		c.nextPullPage(root.doName, { phase: "host", page: { stream: "items", items: [] }, nextCursor: null });
		await child.enter(async ({ target, store }) => {
			expect(await target.importOnePage(T0)).toBe("stopped");
			const rec = target.importRecord()!;
			expect(rec.state).toBe("awaiting_data");
			expect(rec.cursor).toBeNull();
			expect(store.queryItemsPage(null, 10)).toHaveLength(0);
		});

		// A page whose cursor walks back to an earlier phase is refused for the same reason.
		c.nextPullPage(root.doName, { phase: "overrides", overrides: [], nextCursor: { phase: "overrides", inner: null } });
		await child.enter(async ({ target, storage }) => {
			const rec = target.importRecord()!;
			storage.kv.put(REPARTITION_KV_KEYS.IMPORT, { ...rec, cursor: { phase: "host", inner: null }, nextAttemptAt: 0 });
			expect(await target.importOnePage(T0)).toBe("stopped");
			expect(target.importRecord()!.cursor).toEqual({ phase: "host", inner: null });
		});
	});

	it("rejects a host page of the wrong stream and applies nothing", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await root.enter(({ source, store }) => {
			putItem(store, keyForChild(0, c.base.hashSplitN, "a"), "s1");
			source.queue({ kind: "hash_split" });
		});
		await cutOver(root);
		const child = await firstChild(c, root);

		c.nextPullPage(root.doName, {
			phase: "host",
			page: { stream: "pending_tx", pendingTransactions: [], deletionMetadata: { maxDeleteTxOrderTs: 0, deleteRevision: 0 } },
			nextCursor: null,
		});
		await child.enter(async ({ target, storage }) => {
			const rec = target.importRecord()!;
			storage.kv.put(REPARTITION_KV_KEYS.IMPORT, {
				...rec,
				cursor: { phase: "host", inner: { stream: "items", cursor: null } },
				nextAttemptAt: 0,
			});
			expect(await target.importOnePage(T0)).toBe("stopped");
			expect(target.importRecord()!.cursor).toEqual({ phase: "host", inner: { stream: "items", cursor: null } });
			expect(target.importState()).not.toBe("imported");
		});
	});

	it("retries a pull before cutover at a flat interval and backs off on anything else", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await plan(root, { kind: "hash_split" });
		await root.enter(async ({ source }) => void (await source.sourceStep(T0)));
		const child = await firstChild(c, root);

		await child.enter(async ({ target }) => {
			// The source is still `planned`, so it owns the slice and will serve later.
			expect(await target.importOnePage(T0)).toBe("stopped");
			const rec = target.importRecord()!;
			expect(rec.attempts).toBe(1);
			expect(rec.nextAttemptAt).toBe(T0 + 10_000);
		});
		await child.enter(async ({ target, storage }) => {
			// A second attempt keeps the same flat interval rather than doubling it.
			const rec = target.importRecord()!;
			storage.kv.put(REPARTITION_KV_KEYS.IMPORT, { ...rec, nextAttemptAt: 0 });
			expect(await target.importOnePage(T0 + 20_000)).toBe("stopped");
			expect(target.importRecord()!.nextAttemptAt).toBe(T0 + 20_000 + 10_000);
		});
	});
});

describe("Repartition — fokosInit and fokosStartImport", () => {
	it("is idempotent and restores the fallback alarm on every call", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await cutOver(root, { kind: "hash_split" });
		const child = await firstChild(c, root);

		const before = c.alarms(child.doName).length;
		expect(before).toBeGreaterThan(0);
		const req = await initRequestFor(root, child);
		await child.peer.fokosInit(req);
		// A lost reply leaves the source believing the target is initialized; without a restored alarm
		// the target would have nothing to start itself with.
		expect(c.alarms(child.doName).length).toBe(before + 1);
	});

	it("refuses an init that conflicts with the import it already holds", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await cutOver(root, { kind: "hash_split" });
		const child = await firstChild(c, root);
		const req = await initRequestFor(root, child);

		await expect(child.peer.fokosInit({ ...req, repartitionId: "r99" })).rejects.toThrow(fokosErrorWith("partition_context_mismatch"));
		await expect(child.peer.fokosInit({ ...req, slice: { kind: "hash_child", childIndex: 9, depth: 1 } })).rejects.toThrow(
			fokosErrorWith("partition_context_mismatch"),
		);
	});

	it("refuses a start-import for a repartition or a source it does not hold", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await cutOver(root, { kind: "hash_split" });
		const child = await firstChild(c, root);

		await expect(child.peer.fokosStartImport({ repartitionId: "r99", source: root.ref })).rejects.toThrow(
			fokosErrorWith("repartition_unknown"),
		);
		await expect(
			child.peer.fokosStartImport({ repartitionId: "r1", source: { partitionId: root.ctx.partitionId, doName: "impostor" } }),
		).rejects.toThrow(fokosErrorWith("partition_context_mismatch"));
	});
});

describe("Repartition — promotions", () => {
	it("moves one key to its range root, then reclaims the source rows in batches", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		await root.enter(({ source, store }) => {
			for (const sk of ["s1", "s2", "s3"]) putItem(store, "alice", sk);
			putItem(store, "bob", "s1");
			source.queue({ kind: "key_promotion", hashKey: kb("alice") });
		});
		await cutOver(root);
		const rangeRoot = c.rangeNode(root.ctx, kb("alice"), null, null);
		await drainImport(rangeRoot);

		await rangeRoot.enter(({ store }) => {
			expect(store.queryItemsPage(null, 10).map((r) => KeyCodec.decode(r.sk))).toEqual(["s1", "s2", "s3"]);
		});
		await rangeRoot.enter(async ({ target }) => void (await target.sendAck()));

		await root.enter(({ source, store }) => {
			expect(store.getRepartition("r1")!.state).toBe("completed");
			// The batch is smaller than the key, so cleanup takes more than one step. An unfinished step
			// puts the row five seconds out rather than spinning on the rows it has not reached yet.
			expect(source.sourceCleanupStep(T0)).toBe("progressed");
			expect(store.getRepartition("r1")!.state).toBe("completed");
			expect(store.getRepartition("r1")!.nextAttemptAt).toBe(T0 + 5_000);
			expect(source.sourceCleanupStep(T0)).toBe("idle");

			expect(source.sourceCleanupStep(T0 + 5_000)).toBe("progressed");
			expect(store.getRepartition("r1")!.state).toBe("cleaned");
			// Only the promoted key went back; every other key this partition owns stayed.
			expect(store.queryItemsPage(null, 10).map((r) => KeyCodec.decode(r.hk))).toEqual(["bob"]);
		});
	});

	it("hands a finished promotion to the hash child that inherits the key, with no item copy", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		const promoted = keyForChild(0, c.base.hashSplitN, "p");
		const plain = keyForChild(0, c.base.hashSplitN, "q");

		await root.enter(({ source, store }) => {
			putItem(store, promoted, "s1");
			putItem(store, plain, "s1");
			// A promotion that finished long ago, with its source rows already reclaimed.
			source.queue({ kind: "key_promotion", hashKey: kb(promoted) });
			store.setRepartitionState("r1", "cleaned", { cutoverAt: 1, completedAt: 2 });
			store.deleteItemsBatchForHashKey(kb(promoted), 100);
			source.queue({ kind: "hash_split" });
		});
		await cutOver(root);
		const child = await firstChild(c, root);
		await drainImport(child);

		await child.enter(({ store }) => {
			// The forward pointer came across as a finished promotion with an initialized, acknowledged
			// target, so routing, status and destroy traversal all see the link.
			const override = store.routeOverrideFor(kb(promoted))!;
			expect(override.state).toBe("cleaned");
			const inherited = store.getRepartition(override.repartitionId)!;
			expect(inherited).toMatchObject({ kind: "key_promotion", state: "cleaned" });
			expect(store.listRepartitionTargets(inherited.id, "key_promotion")[0]).toMatchObject({
				initialization: "initialized",
				startNotified: true,
				acknowledged: true,
				targetIndex: 0,
			});
			// The data lives in a range tree that neither partition owns, so no item copy came with it.
			expect(store.queryItemsPage(null, 10).map((r) => KeyCodec.decode(r.hk))).toEqual([plain]);
		});
	});

	it("gives a hash child only the overrides inside its own slice", async () => {
		const c = makeCluster();
		const root = c.hashNode([0]);
		const mine = keyForChild(0, c.base.hashSplitN, "m");
		const sibling = keyForChild(1, c.base.hashSplitN, "s");

		await root.enter(({ source, store }) => {
			source.queue({ kind: "key_promotion", hashKey: kb(mine) });
			store.setRepartitionState("r1", "cleaned");
			source.queue({ kind: "key_promotion", hashKey: kb(sibling) });
			store.setRepartitionState("r2", "cleaned");
			source.queue({ kind: "hash_split" });
		});
		await cutOver(root);
		const child = await firstChild(c, root);
		await drainImport(child);

		await child.enter(({ store }) => {
			expect(store.hasRouteOverride(kb(mine))).toBe(true);
			expect(store.hasRouteOverride(kb(sibling))).toBe(false);
		});
	});
});

describe("Repartition — the paginated status view", () => {
	it("orders every repartition and target by (seq, target_index), with -1 for a repartition with none", async () => {
		const c = makeCluster({ hashSplitN: 3 });
		const root = c.hashNode([0]);
		await root.enter(({ source }) => {
			source.queue({ kind: "key_promotion", hashKey: kb("hot") });
			// Still queued after the step below, so it has no target row. It is one entry with no target.
			source.queue({ kind: "key_promotion", hashKey: kb("warm") });
		});
		await plan(root);

		const page = await root.enter(({ source }) => source.statusEntries(null, 1_000));
		expect(page.nextCursor).toBeNull();
		expect(
			page.entries.map((e) => [
				e.repartition.seq,
				e.repartition.kind,
				e.repartition.state,
				e.target?.index ?? -1,
				e.target?.initialization,
			]),
		).toEqual([
			[1, "key_promotion", "planned", 0, "pending"],
			[2, "key_promotion", "queued", -1, undefined],
		]);
	});

	it("stops a page on the entry budget and on the byte budget, and resumes strictly after it", async () => {
		const c = makeCluster({ hashSplitN: 3 });
		const root = c.hashNode([0]);
		await plan(root, { kind: "hash_split" });

		// Three target rows. Two entries per page by count, then one per page by size. A budget below
		// one entry still emits one entry, or the view never drains.
		const byCount = await root.enter(({ source }) => source.statusEntries(null, 2));
		expect(byCount.entries).toHaveLength(2);
		expect(byCount.nextCursor).toEqual({ seq: 1, targetIndex: 1 });

		const rest = await root.enter(({ source }) => source.statusEntries(byCount.nextCursor, 2));
		expect(rest.entries.map((e) => e.target?.index)).toEqual([2]);
		expect(rest.nextCursor).toBeNull();

		const byBytes = await root.enter(({ source }) => source.statusEntries(null, 1_000, 1));
		expect(byBytes.entries.map((e) => e.target?.index)).toEqual([0]);
		expect(byBytes.nextCursor).toEqual({ seq: 1, targetIndex: 0 });

		const after = await root.enter(({ source }) => source.statusEntries(byBytes.nextCursor, 1_000, 1));
		expect(after.entries.map((e) => e.target?.index)).toEqual([1]);
	});
});

// ─── helpers ──────────────────────────────────────────────────────────────────

type QueueRequest = { kind: RepartitionKind; hashKey?: KeyBytes };

/** Queues if needed, then plans, leaving every target `pending`. */
async function plan(node: Node, request?: QueueRequest, now = T0): Promise<void> {
	if (request) await node.enter(({ source }) => void source.queue(request, now));
	await node.enter(async ({ source }) => void (await source.sourceStep(now)));
}

/**
 * Drives a source until routing has moved and every target has been told to start.
 *
 * It drives to a STATE, not for a fixed number of steps, and it advances the clock on each pass. A
 * step that fails leaves its target behind a retry deadline, and a fixed-step driver would then stop
 * short and let the test assert against a source that never cut over.
 */
async function cutOver(node: Node, request?: QueueRequest, now = T0): Promise<void> {
	if (request) await node.enter(({ source }) => void source.queue(request, now));
	for (let i = 0; i < 20; i++) {
		const at = now + i * 30_000;
		const state = await node.enter(({ store }) => {
			const id = activeRepartitionId(store);
			if (!id) return "none";
			const row = store.getRepartition(id)!;
			const counts = store.countRepartitionTargets(id);
			return row.state === "cutover" && counts.total > 0 && counts.startNotified === counts.total ? "done" : "pending";
		});
		if (state === "done") return;
		await node.enter(async ({ source }) => void (await source.sourceStep(at)));
	}
	const rows = await node.enter(({ store }) => store.queryRepartitionStatusPage(null, 50));
	throw new Error(`${node.doName}: the source did not reach cutover; ${JSON.stringify(rows)}`);
}

/** The one repartition this source is still working on, if any. */
function activeRepartitionId(store: PartitionStore): string | undefined {
	for (const row of store.queryRepartitionStatusPage(null, 500)) {
		if (row.state === "queued" || row.state === "planned" || row.state === "cutover") return row.id;
	}
	return undefined;
}

/**
 * Pulls and applies pages until the target's own record says the import is complete.
 *
 * "idle" is not success on its own: it also means "no import record" and "not due yet", so the final
 * state is asserted rather than assumed. Without that, a target the source never initialized reads as
 * a finished import and every assertion after it fails somewhere else.
 */
async function drainImport(node: Node): Promise<void> {
	for (let i = 0; i < 40; i++) {
		const outcome = await node.enter(async ({ target }) => await target.importOnePage());
		if (outcome === "stopped") throw new Error(`${node.doName}: the import stopped before it completed`);
		if (outcome === "idle") break;
	}
	const state = await node.enter(({ target }) => target.importState());
	if (state !== "imported" && state !== "active") {
		throw new Error(`${node.doName}: the import did not complete; state is ${state ?? "absent"}`);
	}
}

/** The first target of this source's split, as a node. */
async function firstChild(c: ReturnType<typeof makeCluster>, source: Node): Promise<Node> {
	return await targetNode(c, source, 0);
}

/** The target at `index` of this source's split, as a node. */
async function targetNode(c: ReturnType<typeof makeCluster>, source: Node, index: number): Promise<Node> {
	const row = await source.enter(({ store }) => {
		const split = store.getSplitRepartition()!;
		return store.listRepartitionTargets(split.id, split.kind)[index];
	});
	return c.node({ ...c.base, doName: row.doName, primaryDoIdStr: "", partitionId: row.partitionId });
}

/** The request the source would send, rebuilt from its own rows. */
async function initRequestFor(source: Node, target: Node) {
	return await source.enter(({ store, source: src, ctx }) => {
		const split = store.getSplitRepartition()!;
		const row = store.listRepartitionTargets(split.id, split.kind).find((t) => t.doName === target.doName)!;
		return { repartitionId: split.id, source: ctx, target: target.ctx, slice: src.materializeSlice(row.slice) };
	});
}
