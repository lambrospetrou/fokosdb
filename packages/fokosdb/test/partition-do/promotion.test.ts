import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import type { PartitionDO } from "../../src/server/do-partition.js";
import { PartialRangeTopology } from "../../src/sharding/partial-range-topology.js";
import { FokosError } from "../../src/shared/errors.js";
import { SHARDING_INTERNAL_CODES, SHARDING_UNAVAILABLE_CODES } from "../../src/sharding/errors.js";
import { HashTopology } from "../../src/sharding/hash-topology.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { hashChildIndex, resolveDescendantHashPartitionContext } from "../../src/sharding/partition-id.js";
import { FokosShardingStore } from "../../src/sharding/sharding-store.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { testCoordinatorRef } from "../stub-helpers.js";
import { executedBy, kb, rangeAncestorsOf, withOpIndex } from "./helpers.js";
import {
	PROMOTION_BIG_DATA,
	PROMOTION_TEST_MAX_SIZE_MB,
	TestPartition,
	assertSplitTreeComplete,
	CONTROLLED_NS,
	drainUntil,
	makePartition,
	rangeOf,
} from "./partition-harness.js";

describe.concurrent("PartitionDO — promotion detection and queuing", () => {
	it("detects a heavy key and immediately cuts over to 'promoting' when no locks are held", async () => {
		const partition = makePartition({ hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		await partition.put({ hashKey: kb("alice"), sortKey: kb("sk1"), data: PROMOTION_BIG_DATA, kind: "text" as const });

		// Migration may complete in the same background cycle as cutover, so accept 'promoted' too.
		await partition.awaitPromotedKeyStatus("alice", ["promoting", "promoted"]);
	});

	it("does not detect any key when the DB is well below the promotion threshold", async () => {
		const partition = makePartition(); // default maxSizeMb=100; promotion threshold is 25 MB
		await partition.put({ hashKey: kb("alice"), sortKey: kb("sk1"), data: "tiny", kind: "text" as const });

		await partition.runAlarm(); // an alarm may not be set at all; running it is a no-op if so
		const s = await partition.status();
		expect(s.promotedKeys).toHaveLength(0);
	});

	it("blocks hash split while a key is being promoted (mutual exclusion, inverse direction)", async () => {
		// shouldSplit must return null while a key is in-flight, even though the database is over
		// hashSplitConditions.maxSizeMb and a split would otherwise be warranted.
		//
		// Only 'queued' and 'promoting' block a split (hasInFlightPromotedKeys), so this pins alice at
		// 'queued' with a transaction lock rather than waiting for a cutover that races the migration to
		// 'promoted'. Landing on 'promoted' would leave nothing in flight and make the assertion pass for
		// the wrong reason.
		const partition = makePartition({ hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });

		// Lock alice/sk1 so startPromotion defers the cutover and alice stays 'queued' throughout.
		const txId = crypto.randomUUID();
		const lockResult = await partition.rpc.txPrepare(partition.ctx, {
			transactionId: txId,
			transactionTimestamp: Date.now(),
			coordinator: testCoordinatorRef(),
			items: withOpIndex([{ hashKey: kb("alice"), sortKey: kb("sk1"), operation: "put", data: "pending", kind: "text" }]),
		});
		expect(lockResult.outcome).toBe("accepted");
		try {
			await partition.triggerPromotion("alice");
			await partition.runAlarm();
			await partition.awaitPromotedKeyStatus("alice", ["queued"]);

			// Now make a split genuinely warranted. Every one of these writes runs checkSplits.
			const databaseSize = await partition.growPastSplitThreshold();

			// Both halves matter: no split queued, AND the preconditions that make that meaningful.
			expect(databaseSize).toBeGreaterThan(PROMOTION_TEST_MAX_SIZE_MB * 1024 * 1024);
			expect(await partition.promotedKeyStatus("alice")).toBe("queued");
			expect((await partition.status()).splitStatus).toBeUndefined();
		} finally {
			await partition.rpc.txCancel(partition.ctx, { transactionId: txId, items: [{ hashKey: kb("alice"), sortKey: kb("sk1") }] });
			await partition.awaitPromoted("alice");
		}
	});
});

describe.concurrent("PartitionDO — promotion cutover deferral and routing", () => {
	it("defers cutover to 'promoting' while the key has a pending transaction lock", async () => {
		const partition = makePartition({ hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });

		// Lock alice/sk1 with a prepare so the lock-free check in startPromotion defers.
		const txId = crypto.randomUUID();
		const lockResult = await partition.rpc.txPrepare(partition.ctx, {
			transactionId: txId,
			transactionTimestamp: Date.now(),
			coordinator: testCoordinatorRef(),
			items: withOpIndex([{ hashKey: kb("alice"), sortKey: kb("sk1"), operation: "put", data: "pending", kind: "text" }]),
		});
		expect(lockResult.outcome).toBe("accepted");
		try {
			await partition.triggerPromotion("alice");
			await partition.runAlarm();

			// Detection queued alice but cutover was deferred — key must still be 'queued'.
			await partition.awaitPromotedKeyStatus("alice", ["queued"]);

			// A write to alice while 'queued' is still served locally.
			const r = await partition.put({ hashKey: kb("alice"), sortKey: kb("sk2"), data: "still-local", kind: "text" as const });
			expect(r.meta.forwardCount).toBe(0);
		} finally {
			// Release the lock; next background cycle should complete the cutover.
			await partition.rpc.txCancel(partition.ctx, { transactionId: txId, items: [{ hashKey: kb("alice"), sortKey: kb("sk1") }] });
			await partition.awaitPromoted("alice");
		}
	});

	it("forwards reads and writes to the range root after cutover ('promoting')", async () => {
		const partition = makePartition({ hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		await partition.put({ hashKey: kb("alice"), sortKey: kb("sk1"), data: PROMOTION_BIG_DATA, kind: "text" as const });

		// Wait for detection + cutover to 'promoting'. Migration may complete in the same background
		// cycle as cutover, so accept 'promoted' too.
		const rangeRoot = partition.rangeRoot("alice");
		await partition.awaitPromotedKeyStatus("alice", ["promoting", "promoted"]);

		// Drain the range root migration; hash DO transitions to 'promoted'.
		await partition.awaitPromotedKeyStatus("alice", ["promoted"], { drive: [rangeRoot] });

		// Writes via the hash partition are forwarded to the range root.
		const w = await partition.stub.apiPutItem(partition.ctx, {
			hashKey: kb("alice"),
			sortKey: kb("sk2"),
			data: "in-range",
			kind: "text" as const,
		});
		expect(w.routing.forwardCount).toBe(1);
		// The response must surface the serving range root's own rangeDepth/rangeAncestors (0/[] for a
		// fresh root), not an empty/zero value from the forwarding hash partition's own context.
		expect(executedBy(w).rangeDepth).toBe(0);
		expect(rangeAncestorsOf(w)).toEqual([]);

		// Item is in the range root.
		const g = await rangeRoot.stub.apiGetItem(rangeRoot.ctx, { hashKey: kb("alice"), sortKey: kb("sk2") });
		expect(g.value).toMatchObject({ found: true, item: { data: "in-range" } });
		expect(executedBy(g).rangeDepth).toBe(0);
		expect(rangeAncestorsOf(g)).toEqual([]);
	});
});

describe.concurrent("PartitionDO — hash-child migration excludes promoted keys", () => {
	it("items belonging to a promoted key are not migrated to hash split children", async () => {
		// Use maxSizeMb=1 so the per-child reject threshold (1.1MB) stays well above what any
		// child receives after migration, avoiding fragile databaseSize comparisons.
		const partition = makePartition({ hashSplitConditions: { maxSizeMb: 1 } });

		// Grow Alice through normal writes until the partition queues promotion.
		await partition.triggerPromotion("alice", (i) => `sk${i + 1}`);

		// Wait for: detect → 'promoting' → range-root migration → 'promoted' → GC clears local alice items.
		const rangeRoot = await partition.awaitPromoted("alice");
		await drainUntil(
			[partition, rangeRoot],
			async () => (await partition.localItemCount("alice")) === 0,
			"alice to be garbage-collected from the hash DO",
		);

		// Trigger the hash split with spread data; none of it exceeds the per-key promotion threshold.
		// alice is 'promoted' by now, so mutual exclusion lets the hash split proceed.
		const splitItems = await partition.triggerHashSplit();
		await partition.awaitSplitCompleted();
		const children = await partition.children();
		await assertSplitTreeComplete(partition);

		// alice's DATA must not be migrated into any hash child (the child inherits only the forward-pointer
		// entry, never the data — which lives in the range structure). The child that owns alice must:
		//   (a) hold no local copy of alice's data (no item rows for the key in its own storage), and
		//   (b) inherit alice's promoted-key entry, so a normal read forwards to the range structure.
		const aliceChildren: TestPartition[] = [];
		for (const child of children) {
			expect(await child.localItemCount("alice"), `alice's data must not be migrated locally into hash child ${child.doName}`).toBe(0);
			if ((await child.promotedKeyStatus("alice")) === "promoted") {
				aliceChildren.push(child);
			}
		}

		// Exactly one hash child inherited alice's promoted-key entry; reading alice through THAT child
		// forwards to the range structure (the range root serves it), proving the inherited forward-pointer
		// works. Reading via the parent would forward through the parent's own cache and bypass the child,
		// so we read on the child directly to actually exercise inheritance.
		expect(aliceChildren, "exactly one hash child should inherit alice's promoted entry").toHaveLength(1);
		expect(aliceChildren[0].doName).toBe((await partition.childOwning("alice")).doName);

		const aliceRead = await aliceChildren[0].get({ hashKey: kb("alice"), sortKey: kb("sk1") });
		expect(aliceRead.found, "alice must be reachable through its hash child via the inherited forward-pointer").toBe(true);
		expect(aliceRead.meta.forwardCount).toBeGreaterThanOrEqual(1);
		expect(aliceRead.meta.servedByActorName, "alice must be served by the range structure, not the hash child").toBe(rangeRoot.doName);

		// A split-trigger key must be reachable via the hash DO (forwarded to the owning child).
		const trigResult = await partition.get(splitItems[0]);
		expect(trigResult).toMatchObject({
			found: true,
			meta: { forwardCount: 1 },
		});
	});
});

describe.concurrent("PartitionDO — debugForcePromoteKey", () => {
	it("forwards to the owning child on a split parent", async () => {
		const root = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		await root.splitHash();

		const res = await root.rpc.debugForcePromoteKey(root.ctx, { hashKey: kb("alice") });
		expect(res.queued).toBe(true);
		// The forward queues on the child; an early background cycle may already have advanced it.
		expect(["queued", "promoting", "promoted"]).toContain(res.status);

		// The router queued nothing locally; the owning child holds the entry.
		expect(await root.promotedKeyStatus("alice")).toBeUndefined();
		const owner = await root.childOwning("alice");
		expect(await owner.promotedKeyStatus("alice")).toBeDefined();

		await owner.awaitPromoted("alice");
	}, 30_000);

	it("returns the existing status without queueing again", async () => {
		const partition = makePartition();

		const first = await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("alice") });
		expect(first.queued).toBe(true);

		const second = await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("alice") });
		expect(second.queued).toBe(false);
		expect(second.status).toBeDefined();

		await partition.awaitPromoted("alice");
	}, 30_000);

	it("restores the fallback alarm when it reports an in-flight promotion", async () => {
		const partition = makePartition({ hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });

		const txId = crypto.randomUUID();
		const lockResult = await partition.rpc.txPrepare(partition.ctx, {
			transactionId: txId,
			transactionTimestamp: Date.now(),
			coordinator: testCoordinatorRef(),
			items: withOpIndex([{ hashKey: kb("alice"), sortKey: kb("sk1"), operation: "put", data: "pending", kind: "text" }]),
		});
		expect(lockResult.outcome).toBe("accepted");
		try {
			await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("alice") });
			await partition.awaitPromotedKeyStatus("alice", ["queued"]);

			await runInDurableObject(partition.stub, async (_i: PartitionDO, state: DurableObjectState) => {
				state.storage.sql.exec(`UPDATE fokos_repartitions SET next_attempt_at = ?`, Date.now() + 60_000);
				await state.storage.deleteAlarm();
				expect(await state.storage.getAlarm()).toBeNull();
			});

			const again = await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("alice") });
			expect(again.queued).toBe(false);

			await runInDurableObject(partition.stub, async (_i: PartitionDO, state: DurableObjectState) => {
				expect(await state.storage.getAlarm(), "the status report must restore the fallback alarm").not.toBeNull();
				state.storage.sql.exec(`UPDATE fokos_repartitions SET next_attempt_at = ?`, Date.now());
			});
		} finally {
			await partition.rpc.txCancel(partition.ctx, { transactionId: txId, items: [{ hashKey: kb("alice"), sortKey: kb("sk1") }] });
			await partition.awaitPromoted("alice");
		}
	}, 30_000);
});

describe("PartitionDO — a range root before its first page", () => {
	it("serves a read and a write at the source when a Bloom false positive names a range root before its cutover", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitN: 2, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		await partition.splitHash();
		const owner = await partition.childOwning("alice");

		await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("bob") });
		const bobOwner = await partition.childOwning("bob");
		await bobOwner.awaitPromoted("bob");
		const warm = await partition.get({ hashKey: kb("bob"), sortKey: kb("sk1") });
		expect(warm.meta.servedByActorName).toBe(bobOwner.rangeRoot("bob").doName);

		await partition.put({ hashKey: kb("alice"), sortKey: kb("sk1"), data: "v", kind: "text" });

		// This spy is on a prototype, thus each test in the isolate sees it, and `vi.restoreAllMocks()` of
		// another test can remove it. The class is private to the runtime, thus no test control can replace the
		// spy. The spy is safe only because this `describe` is sequential and at the top level: vitest
		// runs no other test of this file at the same time. Do not make it concurrent.
		// The spy makes every Bloom filter say that the key is promoted, the filter of the source too. The
		// range root answers `partition_awaiting_data`, and each sender resolves the key again without
		// its filter, so the source runs the write itself.
		const maybePromotedSpy = vi.spyOn(PartialRangeTopology.prototype, "maybePromoted").mockReturnValue(true);

		const aliceRangeRoot = partition.rangeRoot("alice").controlled;
		await aliceRangeRoot.testHoldInit();

		try {
			await owner.rpc.debugForcePromoteKey(owner.ctx, { hashKey: kb("alice") });
			await vi.waitFor(async () => expect(await aliceRangeRoot.testInitCalls()).toBeGreaterThan(0), { timeout: 5000, interval: 10 });

			const g = await partition.get({ hashKey: kb("alice"), sortKey: kb("sk1") });
			expect(g).toMatchObject({ found: true, item: { data: "v" } });

			// The source runs the write.
			const put = await partition.put({ hashKey: kb("alice"), sortKey: kb("sk2"), data: "v2", kind: "text" });
			expect(put.meta.servedByActorName).toBe(owner.doName);
			expect(await owner.localItemCount("alice")).toBe(2);
		} finally {
			await aliceRangeRoot.testReleaseInit();
			maybePromotedSpy.mockRestore();
		}

		await owner.awaitPromoted("alice");
		// The import after the cutover brings the write to the range tree.
		expect(await partition.get({ hashKey: kb("alice"), sortKey: kb("sk2") })).toMatchObject({ found: true, item: { data: "v2" } });
	}, 30_000);

	it("answers partition_migrating to a write sent directly to a range root before its first page", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		await partition.put({ hashKey: kb("alice"), sortKey: kb("sk1"), data: "v", kind: "text" });
		const rangeRoot = partition.rangeRoot("alice");
		// The source cuts over only after the range root answers `fokosInit`.
		await rangeRoot.controlled.testHoldInit();
		try {
			await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("alice") });
			await vi.waitFor(async () => expect(await rangeRoot.controlled.testInitCalls()).toBeGreaterThan(0), { timeout: 5000, interval: 10 });

			await runInDurableObject(rangeRoot.stub, async (instance: PartitionDO) => {
				await expect(
					instance.apiPutItem(rangeRoot.ctx, { hashKey: kb("alice"), sortKey: kb("sk2"), data: "v", kind: "text" }),
				).rejects.toThrow(fokosErrorWith("partition_migrating", { importState: "awaiting_data" }));
			});
			// The range root applied nothing, and it sent nothing to the source.
			expect(await partition.localItemCount("alice")).toBe(1);
		} finally {
			await rangeRoot.controlled.testReleaseInit();
		}
		await partition.awaitPromoted("alice");
	}, 30_000);

	it("does not resolve to the source again after the cutover, when the range root has no data yet", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		await partition.put({ hashKey: kb("alice"), sortKey: kb("sk1"), data: "v", kind: "text" });
		const rangeRoot = partition.rangeRoot("alice");
		// The first pull of the range root waits here, so the range root has no page after the cutover.
		await partition.controlled.testHoldPulls({ stream: "overrides", target: rangeRoot.doName });
		try {
			await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("alice") });
			await partition.awaitPromotedKeyStatus("alice", ["promoting"]);

			// The route override of the source names the range root now, so the source forwards the write
			// and does not run it itself.
			await runInDurableObject(partition.stub, async (instance: PartitionDO) => {
				await expect(
					instance.apiPutItem(partition.ctx, { hashKey: kb("alice"), sortKey: kb("sk2"), data: "v", kind: "text" }),
				).rejects.toThrow(fokosErrorWith("partition_migrating", { importState: "awaiting_data" }));
			});
			expect(await partition.localItemCount("alice")).toBe(1);
		} finally {
			await partition.controlled.testReleasePulls();
		}
		await partition.awaitPromoted("alice");
	}, 30_000);
});

describe("PartitionDO — transaction commit and promotion candidates", () => {
	it("keeps the local promotion candidates when a forwarded child commit fails", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("alice") });
		const rangeRoot = await partition.awaitPromoted("alice");

		const txId = crypto.randomUUID();
		const txTs = Date.now();
		const prepare = await partition.rpc.txPrepare(partition.ctx, {
			transactionId: txId,
			transactionTimestamp: txTs,
			coordinator: testCoordinatorRef(),
			items: withOpIndex([
				{ hashKey: kb("alice"), sortKey: kb("sk1"), operation: "put", data: "v", kind: "text" },
				{ hashKey: kb("hot"), sortKey: kb("sk1"), operation: "put", data: PROMOTION_BIG_DATA, kind: "text" },
			]),
		});
		expect(prepare.outcome).toBe("accepted");

		await rangeRoot.controlled.testTxResponse("txCommit", { error: "simulated child commit failure", times: 1 });

		const commit = {
			transactionId: txId,
			transactionTimestamp: txTs,
			items: [
				{ hashKey: kb("alice"), sortKey: kb("sk1") },
				{ hashKey: kb("hot"), sortKey: kb("sk1") },
			],
		};
		try {
			await runInDurableObject(partition.stub, async (instance: PartitionDO) => {
				// Commit attempts every group, so a failed remote group surfaces as the fan-out error.
				await expect(instance.txCommit(partition.ctx, commit)).rejects.toThrow(fokosErrorWith("partition_fanout_failed"));
			});

			expect(await partition.promotedKeyStatus("hot"), "the local hot key must be queued for promotion").toBeDefined();
		} finally {
			await rangeRoot.controlled.testClearTxResponse("txCommit");
		}

		// The promotion of the local hot key is queued off the request path, so the new range root can
		// already be importing when the coordinator retries this decided transaction. The retry then
		// gets the retryable `partition_migrating`, exactly as the coordinator does in production, and
		// commits once the import completes.
		await drainUntil(
			[partition, partition.rangeRoot("hot")],
			async () => {
				try {
					return (await partition.rpc.txCommit(partition.ctx, commit)).outcome === "committed";
				} catch (error) {
					// The commit goes through the parent to the new range root, and that group can fail while
					// the range root imports. The parent then attempts every group and wraps the failure as
					// `partition_fanout_failed`, with `partition_migrating` as its cause. The coordinator
					// retries both errors, thus the test accepts both.
					const migrating = FokosError.isCode(error, SHARDING_INTERNAL_CODES.partition_fanout_failed) ? error.cause : error;
					if (!FokosError.isCode(migrating, SHARDING_UNAVAILABLE_CODES.partition_migrating)) {
						throw error;
					}
					return false;
				}
			},
			"the retried commit to commit",
		);
		await partition.awaitPromoted("hot");
	}, 30_000);
});

/**
 * The partition behind a context with the hash split cap raised to 100 MB. After a hash split, a child
 * is still above a 1 MB cap in SQLite pages, and it would split again during a test. The operator
 * raises the cap, and every later call carries the new policy: a call with the old context stores the
 * old cap again.
 */
function raised(p: TestPartition): TestPartition {
	return TestPartition.at({
		...p.ctx,
		policy: { ...p.ctx.policy, hashSplitConditions: { ...p.ctx.policy.hashSplitConditions, maxSizeMb: 100 } },
	});
}

/** Splits the hash root once, then raises the cap on the root and on each child. Every later call uses the answer. */
async function splitHashOnce(opts: Parameters<typeof makePartition>[0]): Promise<TestPartition> {
	const split = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB }, ...opts });
	await split.splitHash();
	const partition = raised(split);
	for (const child of await partition.children()) {
		await raised(child).status();
	}
	return partition;
}

describe("PartitionDO — a transaction through a hash jump to a partition that does not exist", () => {
	it("falls back from the jump, keeps Bloom off, and sends the keys of one owner in one call", async () => {
		const hashSplitN = 2;
		const partition = await splitHashOnce({ ns: CONTROLLED_NS, hashSplitN });

		// The root holds a Bloom filter only after it learns a promotion. The spy below has no effect without one.
		await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("bob") });
		await raised(await partition.childOwning("bob")).awaitPromoted("bob");
		await partition.get({ hashKey: kb("bob"), sortKey: kb("sk1") });

		// Two hash keys with the same child index at depths 0 and 1: one hash leaf owns both, and a
		// depth-2 hint for one of them names the same grandchild for the other.
		const path = (key: string) => [0, 1].map((depth) => hashChildIndex(kb(key), depth, hashSplitN));
		const h1 = "tx-path-0";
		let h2 = "";
		for (let i = 1; h2 === ""; i++) {
			if (path(`tx-path-${i}`).join() === path(h1).join()) {
				h2 = `tx-path-${i}`;
			}
		}
		const owner = await partition.childOwning(h1);
		const missing = TestPartition.at(resolveDescendantHashPartitionContext(partition.ctx, path(h1)));

		const transactionId = crypto.randomUUID();
		const transactionTimestamp = Date.now();
		const prepare = await partition.rpc.txPrepare(partition.ctx, {
			transactionId,
			transactionTimestamp,
			coordinator: testCoordinatorRef(),
			items: withOpIndex([
				{ hashKey: kb(h1), sortKey: kb("a"), operation: "put", data: "v1", kind: "text" },
				{ hashKey: kb(h2), sortKey: kb("b"), operation: "put", data: "v2", kind: "text" },
			]),
		});
		expect(prepare.outcome).toBe("accepted");
		const keys = [
			{ hashKey: kb(h1), sortKey: kb("a") },
			{ hashKey: kb(h2), sortKey: kb("b") },
		];

		// The root learns a hint to a grandchild that no split created. Then the root restarts, so the
		// next instance reads the hint from storage. A stub of an aborted object stays broken, thus the
		// answer is the root behind a new stub.
		const plantMissingGrandchild = async (): Promise<TestPartition> => {
			const root = TestPartition.at(partition.ctx);
			await runInDurableObject(root.stub, (_instance: PartitionDO, state: DurableObjectState) => {
				const arena = HashTopology.create(hashSplitN, 0);
				arena.updateFromHint(kb(h1), 2);
				new FokosShardingStore(state.storage).putHashArena(arena.toSnapshot());
			});
			await runInDurableObject(root.stub, (_instance: PartitionDO, state: DurableObjectState) => {
				state.abort("reload the hash arena");
			}).catch(() => {});
			return TestPartition.at(partition.ctx);
		};

		// This spy is on a prototype, thus the rules of the spy in "serves a read and a write at the source
		// when a Bloom false positive names a range root before its cutover" apply: keep this `describe`
		// sequential and at the top level.
		const maybePromotedSpy = vi
			.spyOn(PartialRangeTopology.prototype, "maybePromoted")
			.mockImplementation((hashKey) => KeyCodec.compare(hashKey, kb(h1)) === 0);
		try {
			const root = await plantMissingGrandchild();
			expect(await root.rpc.txCommit(root.ctx, { transactionId, transactionTimestamp, items: keys })).toMatchObject({
				outcome: "committed",
			});

			const restarted = await plantMissingGrandchild();
			expect(await restarted.rpc.txReadSnapshot(restarted.ctx, { items: keys })).toMatchObject({
				outcome: "committed",
				items: [
					{ found: true, data: "v1" },
					{ found: true, data: "v2" },
				],
			});
		} finally {
			maybePromotedSpy.mockRestore();
		}

		// The root tried the grandchild first, and then sent both keys to their owner in one call. The order
		// of the items in a sub-request is not part of the contract.
		expect(await missing.controlled.testTxCalls("txCommit")).toHaveLength(1);
		expect(await missing.controlled.testTxCalls("txReadSnapshot")).toHaveLength(1);
		const commits = await owner.controlled.testTxCalls("txCommit");
		expect(commits).toHaveLength(1);
		expect(commits[0].items.map((item) => [KeyCodec.decode(item.hashKey), KeyCodec.decode(item.sortKey)])).toEqual(
			expect.arrayContaining([
				[h1, "a"],
				[h2, "b"],
			]),
		);
		expect(commits[0].items).toHaveLength(2);
		expect(await owner.controlled.testTxCalls("txReadSnapshot")).toHaveLength(1);
		expect(await owner.localItemCount(h1)).toBe(1);
		expect(await owner.localItemCount(h2)).toBe(1);
		// No split below the root runs during the test or after it.
		expect((await raised(owner).status()).splitStatus).toBeUndefined();
	}, 30_000);
});

// Two tests spy on a prototype, thus the rules of the spy in "serves a read and a write at the source
// when a Bloom false positive names a range root before its cutover" apply: keep this `describe`
// sequential and at the top level.
describe("PartitionDO — the Bloom step of the transaction shapes", () => {
	/** A Bloom filter at `partition`: it promotes `bob` and reads it once. The spies below have no effect without one. */
	const learnOnePromotion = async (partition: TestPartition) => {
		await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("bob") });
		await partition.awaitPromoted("bob");
		await partition.get({ hashKey: kb("bob"), sortKey: kb("sk1") });
	};
	const spyBloomFor = (...hashKeys: string[]) =>
		vi
			.spyOn(PartialRangeTopology.prototype, "maybePromoted")
			.mockImplementation((key) => hashKeys.some((hashKey) => KeyCodec.compare(key, kb(hashKey)) === 0));

	it("jumps from the hash root to the range owner of a promoted key in one forward", async () => {
		const partition = await splitHashOnce({ rangeSplitN: 2, rangeSplitConditions: { maxSizeMb: 1 } });
		// A depth-1 hash leaf promotes `alice`, and the range tree of `alice` grows to depth 2.
		await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("alice") });
		const rangeRoot = await raised(await partition.childOwning("alice")).awaitPromoted("alice");
		await rangeRoot.triggerRangeSplit((i) => `sk${String(i).padStart(3, "0")}`);
		await rangeRoot.awaitSplitCompleted();
		const leftChild = (await rangeRoot.children()).find((c) => rangeOf(c.ctx).startBoundary === null)!;
		const owner = (await leftChild.splitRange("aa")).find((c) => rangeOf(c.ctx).startBoundary === null)!;

		// One read goes through the hash leaf, the range root, and the depth-1 range child. The hash root
		// learns the promotion of `alice` and the slice of the owner.
		const read = await partition.stub.apiGetItem(partition.ctx, { hashKey: kb("alice"), sortKey: kb("aa0000") });
		expect(read.routing.forwardCount).toBe(4);
		expect(executedBy(read).ref.doName).toBe(owner.doName);

		// "a0" sorts before each "aa…" key, so the owner of "aa0000" owns it too.
		const key = { hashKey: kb("alice"), sortKey: kb("a0") };
		const transactionId = crypto.randomUUID();
		const transactionTimestamp = Date.now();
		const prepare = await partition.stub.txPrepare(partition.ctx, {
			transactionId,
			transactionTimestamp,
			coordinator: testCoordinatorRef(),
			items: withOpIndex([{ ...key, operation: "put", data: "v", kind: "text" }]),
		});
		expect(prepare.value.outcome).toBe("accepted");
		expect(prepare.routing.forwardCount).toBe(1);
		expect(executedBy(prepare).ref.doName).toBe(owner.doName);

		const commit = await partition.stub.txCommit(partition.ctx, { transactionId, transactionTimestamp, items: [key] });
		expect(commit.value.outcome).toBe("committed");
		expect(commit.routing.forwardCount).toBe(1);
		expect(executedBy(commit).ref.doName).toBe(owner.doName);

		const snapshot = await partition.stub.txReadSnapshot(partition.ctx, { items: [key] });
		expect(snapshot.value).toMatchObject({ outcome: "committed", items: [{ found: true, data: "v" }] });
		expect(snapshot.routing.forwardCount).toBe(1);
		expect(executedBy(snapshot).ref.doName).toBe(owner.doName);
	}, 30_000);

	it("drops a Bloom hit that would split the keys of one owner, for a group and for a single owner", async () => {
		const partition = makePartition();
		await learnOnePromotion(partition);

		// The partition owns `alice` and `carol`. A Bloom false positive names the range root of its hash
		// key, which does not exist. Without the guard, the commit sends `alice` and `carol` in two calls
		// to the same partition, and it fails with `commit_keyset_mismatch`. In the first case the other
		// hash key is local. In the second case both hash keys have a Bloom hit and one exact target.
		for (const [i, bloomHits] of [["alice"], ["alice", "carol"]].entries()) {
			const keys = [
				{ hashKey: kb("alice"), sortKey: kb(`a${i}`) },
				{ hashKey: kb("carol"), sortKey: kb(`b${i}`) },
			];
			const transactionId = crypto.randomUUID();
			const transactionTimestamp = Date.now();
			const maybePromotedSpy = spyBloomFor(...bloomHits);
			try {
				const prepare = await partition.rpc.txPrepare(partition.ctx, {
					transactionId,
					transactionTimestamp,
					coordinator: testCoordinatorRef(),
					items: withOpIndex(keys.map((key) => ({ ...key, operation: "put" as const, data: "v", kind: "text" as const }))),
				});
				expect(prepare.outcome).toBe("accepted");
				expect(await partition.rpc.txCommit(partition.ctx, { transactionId, transactionTimestamp, items: keys })).toMatchObject({
					outcome: "committed",
				});

				// A single owner holds both keys, thus the request runs here and does not answer `not_applicable`.
				const shot = await partition.rpc.txExecuteSingleShot(partition.ctx, {
					items: withOpIndex(keys.map((key) => ({ ...key, operation: "put" as const, data: "v2", kind: "text" as const }))),
				});
				expect(shot.outcome).toBe("committed");
			} finally {
				maybePromotedSpy.mockRestore();
			}
			for (const key of keys) {
				expect(await partition.get(key)).toMatchObject({ found: true, item: { data: "v2" } });
			}
		}
		expect(await partition.localItemCount("alice")).toBe(2);
		expect(await partition.localItemCount("carol")).toBe(2);
	}, 30_000);

	it("prepares and commits at the hash leaf when a Bloom hit names a range root before its cutover", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		await learnOnePromotion(partition);
		await partition.put({ hashKey: kb("alice"), sortKey: kb("sk1"), data: "v", kind: "text" });

		const rangeRoot = partition.rangeRoot("alice");
		await rangeRoot.controlled.testHoldInit();
		const maybePromotedSpy = spyBloomFor("alice");
		const key = { hashKey: kb("alice"), sortKey: kb("sk2") };
		try {
			// The range root has its identity and no page yet, because the source cuts over only after
			// `fokosInit` answers.
			await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("alice") });
			await vi.waitFor(async () => expect(await rangeRoot.controlled.testInitCalls()).toBeGreaterThan(0), { timeout: 5000, interval: 10 });

			// The range root answers `partition_migrating` with `awaiting_data`, and the source runs each
			// request itself.
			const transactionId = crypto.randomUUID();
			const transactionTimestamp = Date.now();
			const prepare = await partition.stub.txPrepare(partition.ctx, {
				transactionId,
				transactionTimestamp,
				coordinator: testCoordinatorRef(),
				items: withOpIndex([{ ...key, operation: "put", data: "v2", kind: "text" }]),
			});
			expect(prepare.value.outcome).toBe("accepted");
			expect(prepare.routing.forwardCount).toBe(1);
			expect(executedBy(prepare).ref.doName).toBe(partition.doName);

			const commit = await partition.stub.txCommit(partition.ctx, { transactionId, transactionTimestamp, items: [key] });
			expect(commit.value.outcome).toBe("committed");
			expect(commit.routing.forwardCount).toBe(1);
			expect(executedBy(commit).ref.doName).toBe(partition.doName);
			expect(await partition.localItemCount("alice")).toBe(2);
		} finally {
			await rangeRoot.controlled.testReleaseInit();
			maybePromotedSpy.mockRestore();
		}

		// The import after the cutover brings the committed item to the range tree.
		await partition.awaitPromoted("alice");
		expect(await partition.get(key)).toMatchObject({ found: true, item: { data: "v2" } });
	}, 30_000);
});
