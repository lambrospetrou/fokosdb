import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import type { PartitionDO } from "../../src/server/do-partition.js";
import type { PartialRangeTopology } from "../../src/sharding/partial-range-topology.js";
import { FokosError } from "../../src/shared/errors.js";
import { SHARDING_INTERNAL_CODES, SHARDING_UNAVAILABLE_CODES } from "../../src/sharding/errors.js";
import { HashTopology } from "../../src/sharding/hash-topology.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { hashChildIndex, resolveDescendantHashPartitionContext } from "../../src/sharding/partition-id.js";
import { FokosShardingStore } from "../../src/sharding/sharding-store.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { testCoordinatorRef } from "../stub-helpers.js";
import { executedBy, kb, lockKeys, rangeAncestorsOf, withOpIndex } from "./helpers.js";
import {
	PROMOTION_BIG_DATA,
	PROMOTION_TEST_MAX_SIZE_MB,
	TestPartition,
	assertSplitTreeComplete,
	CONTROLLED_NS,
	drainUntil,
	findKey,
	keepTestLocks,
	makePartition,
	rangeOf,
	storedBloom,
	useSmallBloom,
} from "./partition-harness.js";

describe.concurrent("PartitionDO — promotion detection and queuing", () => {
	it("detects a heavy key and cuts it over to 'promoting'", async () => {
		const partition = makePartition({ hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		await partition.put({ hashKey: kb("alice"), sortKey: kb("sk1"), data: PROMOTION_BIG_DATA, kind: "text" as const });

		// The migration can complete in the same background pass as the cutover, so 'promoted' is also correct.
		await partition.awaitPromotedKeyStatus("alice", ["promoting", "promoted"]);
	});

	it("does not detect a key when the database is well below the promotion threshold", async () => {
		const partition = makePartition(); // default maxSizeMb=100; promotion threshold is 25 MB
		await partition.put({ hashKey: kb("alice"), sortKey: kb("sk1"), data: "tiny", kind: "text" as const });

		await partition.runAlarm(); // an alarm may not be set at all; running it is a no-op if so
		expect(await partition.promotedKeyStatus("alice")).toBeUndefined();
	});

	it("does not queue a hash split while a promotion is unfinished", async () => {
		// The database grows past `hashSplitConditions.maxSizeMb`, so each write asks for a split. The
		// queue refuses the split while a promotion is `queued`, `planned`, or `cutover`.
		//
		// The test holds the pages of the range root, so alice stays at 'promoting'. At 'promoted' no
		// promotion is unfinished, and the assertion would pass for the wrong reason.
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		const rangeRoot = partition.rangeRoot("alice");
		await partition.controlled.testHoldPulls({ stream: "items", target: rangeRoot.doName });
		try {
			await partition.triggerPromotion("alice");
			await partition.awaitPromotedKeyStatus("alice", ["promoting"]);

			const databaseSize = await partition.growPastSplitThreshold();

			// The database is over the cap and alice is still 'promoting', and no split is queued.
			expect(databaseSize).toBeGreaterThan(PROMOTION_TEST_MAX_SIZE_MB * 1024 * 1024);
			expect(await partition.promotedKeyStatus("alice")).toBe("promoting");
			expect((await partition.status()).splitStatus).toBeUndefined();
		} finally {
			await partition.controlled.testReleasePulls();
		}
	});
});

describe.concurrent("PartitionDO — promotion cutover and routing", () => {
	/** Prepares a put of alice/sk1 on `partition`, and returns the transaction id and timestamp. */
	async function prepareAlice(partition: TestPartition): Promise<{ transactionId: string; transactionTimestamp: number }> {
		const transactionId = crypto.randomUUID();
		const transactionTimestamp = Date.now();
		const prepare = await partition.rpc.txPrepare(partition.ctx, {
			transactionId,
			transactionTimestamp,
			coordinator: testCoordinatorRef(),
			items: withOpIndex([{ hashKey: kb("alice"), sortKey: kb("sk1"), operation: "put", data: "from-the-lock", kind: "text" }]),
		});
		expect(prepare).toMatchObject({ outcome: "accepted" });
		return { transactionId, transactionTimestamp };
	}

	/**
	 * Checks that the range root holds the lock of alice/sk1 and the source holds no copy of it, then
	 * commits the transaction through the source. The commit goes to the range root, which applies the
	 * payload of the prepare.
	 */
	async function commitAliceOnRangeRoot(
		partition: TestPartition,
		rangeRoot: TestPartition,
		{ transactionId, transactionTimestamp }: { transactionId: string; transactionTimestamp: number },
	): Promise<void> {
		// The import brought the lock, and the source cleanup deletes the copy on the source.
		expect(await lockKeys(rangeRoot.stub, transactionId)).toEqual(["alice/sk1"]);
		await drainUntil(
			[partition],
			async () => (await lockKeys(partition.stub, transactionId)).length === 0,
			"the copy of alice/sk1 deleted",
		);

		await expect(
			partition.rpc.txCommit(partition.ctx, {
				transactionId,
				transactionTimestamp,
				items: [{ hashKey: kb("alice"), sortKey: kb("sk1") }],
			}),
		).resolves.toEqual({ outcome: "committed" });
		expect(await rangeRoot.get({ hashKey: kb("alice"), sortKey: kb("sk1") })).toMatchObject({
			found: true,
			item: { data: "from-the-lock" },
		});
		expect(await lockKeys(rangeRoot.stub, transactionId)).toEqual([]);
	}

	it("moves a lock that a prepare took before the promotion, and the range root commits it", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		const rangeRoot = partition.rangeRoot("alice");
		await keepTestLocks(partition, rangeRoot);

		const prepared = await prepareAlice(partition);
		await partition.triggerPromotion("alice");
		expect((await partition.awaitPromoted("alice")).doName).toBe(rangeRoot.doName);

		await commitAliceOnRangeRoot(partition, rangeRoot, prepared);
	});

	it("forwards a write and a read of a promoted key to the range root", async () => {
		const partition = makePartition({ hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		await partition.put({ hashKey: kb("alice"), sortKey: kb("sk1"), data: PROMOTION_BIG_DATA, kind: "text" as const });

		const rangeRoot = partition.rangeRoot("alice");
		await partition.awaitPromotedKeyStatus("alice", ["promoted"], { drive: [rangeRoot] });

		const w = await partition.stub.apiPutItem(partition.ctx, {
			hashKey: kb("alice"),
			sortKey: kb("sk2"),
			data: "in-range",
			kind: "text" as const,
		});
		expect(w.routing.forwardCount).toBe(1);
		expect(executedBy(w).ref.doName).toBe(rangeRoot.doName);
		// The response gives the rangeDepth and rangeAncestors of the range root that ran the write (0 and []
		// for a new root). It does not give the values of the hash partition that forwarded it.
		expect(executedBy(w).rangeDepth).toBe(0);
		expect(rangeAncestorsOf(w)).toEqual([]);

		const g = await partition.stub.apiGetItem(partition.ctx, { hashKey: kb("alice"), sortKey: kb("sk2") });
		expect(g.value).toMatchObject({ found: true, item: { data: "in-range" } });
		expect(g.routing.forwardCount).toBe(1);
		expect(executedBy(g).ref.doName).toBe(rangeRoot.doName);
		expect(executedBy(g).rangeDepth).toBe(0);
		expect(rangeAncestorsOf(g)).toEqual([]);
	});
});

describe.concurrent("PartitionDO — a hash split after a promotion", () => {
	it("does not copy the items of a promoted key to the hash split children", async () => {
		// With maxSizeMb=1, the size limit of each child (1.1 MB) stays well above the data that a child
		// gets from the split, so no size check of the test depends on a small margin.
		const partition = makePartition({ hashSplitConditions: { maxSizeMb: 1 } });

		// Normal writes make alice large until the partition queues its promotion.
		await partition.triggerPromotion("alice", (i) => `sk${i + 1}`);

		// The promotion completes, and the cleanup deletes the items of alice on the source.
		const rangeRoot = await partition.awaitPromoted("alice");
		await drainUntil(
			[partition, rangeRoot],
			async () => (await partition.localItemCount("alice")) === 0,
			"the cleanup to delete the items of alice on the hash partition",
		);

		// The split data goes to many keys, and no key gets past the promotion threshold. No promotion
		// is unfinished now, so the hash split can queue.
		const splitItems = await partition.triggerHashSplit();
		await partition.awaitSplitCompleted();
		const children = await partition.children();
		await assertSplitTreeComplete(partition);

		// The items of alice are in the range tree. A hash child gets only the promoted-key entry of
		// alice, and no item. The child that owns alice must:
		//   (a) hold no item of alice in its own storage, and
		//   (b) hold the promoted-key entry of alice, so that a normal read goes to the range tree.
		const aliceChildren: TestPartition[] = [];
		for (const child of children) {
			expect(await child.localItemCount("alice"), `alice's data must not be migrated locally into hash child ${child.doName}`).toBe(0);
			if ((await child.promotedKeyStatus("alice")) === "promoted") {
				aliceChildren.push(child);
			}
		}

		// Exactly one hash child holds the promoted-key entry of alice. The read goes to that child
		// directly: a read through the parent uses the route cache of the parent, and does not use the
		// entry of the child. The range root answers the read.
		expect(aliceChildren, "exactly one hash child must hold the promoted-key entry of alice").toHaveLength(1);
		expect(aliceChildren[0].doName).toBe((await partition.childOwning("alice")).doName);

		const aliceRead = await aliceChildren[0].get({ hashKey: kb("alice"), sortKey: kb("sk1") });
		expect(aliceRead.found, "the hash child must forward a read of alice to the range tree").toBe(true);
		expect(aliceRead.meta.forwardCount).toBeGreaterThanOrEqual(1);
		expect(aliceRead.meta.servedByActorName, "the range root must answer the read of alice, not the hash child").toBe(rangeRoot.doName);

		// The hash partition forwards a read of a split key to the child that owns it.
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
		// The child queues the promotion. A background pass can already have moved it to a later state.
		expect(["queued", "promoting", "promoted"]).toContain(res.status);

		// The parent queued nothing. The child that owns alice holds the entry.
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
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		const rangeRoot = partition.rangeRoot("alice");

		// The held `fokosInit` of the range root keeps the promotion unfinished. The background pass of
		// the source waits inside that call, so it cannot set the alarm again between the two checks below.
		await rangeRoot.controlled.testHoldInit();
		try {
			await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("alice") });
			await vi.waitFor(async () => expect(await rangeRoot.controlled.testInitCalls()).toBeGreaterThan(0), { timeout: 5000, interval: 10 });
			expect(await partition.promotedKeyStatus("alice")).toBe("queued");

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
			await rangeRoot.controlled.testReleaseInit();
			await partition.awaitPromoted("alice");
		}
	}, 30_000);
});

describe.concurrent("PartitionDO — a range root before its first page", () => {
	it("serves a read and a write at the source when a Bloom false positive names a range root before its cutover", async () => {
		// A sender learns a promoted key only from an answer of its range root, and that answer comes
		// after the cutover. Thus before the cutover, only a false positive of the filter names the range
		// root. This test makes the rare case: the false positive is on a key whose promotion waits for
		// the `fokosInit` reply, thus the range root answers `partition_awaiting_data`. The usual false
		// positive names a range root that does not exist. The Bloom tests of the transaction shapes cover it.
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitN: 2, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		await partition.splitHash();
		await useSmallBloom(partition, ...(await partition.children()));

		// A read of bob through the root puts bob into the filter of the root and of the child that owns bob.
		await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("bob") });
		const bobOwner = await partition.childOwning("bob");
		await bobOwner.awaitPromoted("bob");
		const warm = await partition.get({ hashKey: kb("bob"), sortKey: kb("sk1") });
		expect(warm.meta.servedByActorName).toBe(bobOwner.rangeRoot("bob").doName);

		// A hash key that the child of bob owns, and that no promotion moved. The filters of the root and
		// of the child both report it as promoted. Each sender sends the key to its range root, the range
		// root answers `partition_awaiting_data`, and each sender resolves the key again without its
		// filter. Thus the source runs the write itself.
		const [rootBloom, ownerBloom] = [await storedBloom(partition), await storedBloom(bobOwner)];
		const key = findKey(
			"false-positive",
			(k) =>
				hashChildIndex(kb(k), 0, 2) === hashChildIndex(kb("bob"), 0, 2) &&
				rootBloom.maybePromoted(kb(k)) &&
				ownerBloom.maybePromoted(kb(k)),
		);
		const owner = await partition.childOwning(key);
		expect(owner.doName).toBe(bobOwner.doName);
		await partition.put({ hashKey: kb(key), sortKey: kb("sk1"), data: "v", kind: "text" });

		const keyRangeRoot = partition.rangeRoot(key).controlled;
		await keyRangeRoot.testHoldInit();
		try {
			await owner.rpc.debugForcePromoteKey(owner.ctx, { hashKey: kb(key) });
			await vi.waitFor(async () => expect(await keyRangeRoot.testInitCalls()).toBeGreaterThan(0), { timeout: 5000, interval: 10 });

			const g = await partition.get({ hashKey: kb(key), sortKey: kb("sk1") });
			expect(g).toMatchObject({ found: true, item: { data: "v" } });

			// The source runs the write.
			const put = await partition.put({ hashKey: kb(key), sortKey: kb("sk2"), data: "v2", kind: "text" });
			expect(put.meta.servedByActorName).toBe(owner.doName);
			expect(await owner.localItemCount(key)).toBe(2);
		} finally {
			await keyRangeRoot.testReleaseInit();
		}

		await owner.awaitPromoted(key);
		// The import after the cutover brings the write to the range tree.
		expect(await partition.get({ hashKey: kb(key), sortKey: kb("sk2") })).toMatchObject({ found: true, item: { data: "v2" } });
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
	it("queues the promotion of a large local key when the forwarded part of the commit fails", async () => {
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
		expect(prepare).toMatchObject({ outcome: "accepted" });

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
				// The commit tries every group, so the failed remote group gives the fan-out error.
				await expect(instance.txCommit(partition.ctx, commit)).rejects.toThrow(fokosErrorWith("partition_fanout_failed"));
			});

			expect(await partition.promotedKeyStatus("hot"), "the local hot key must be queued for promotion").toBeDefined();
		} finally {
			await rangeRoot.controlled.testClearTxResponse("txCommit");
		}

		// The promotion of the local hot key runs in the background, so the new range root can import
		// when the coordinator sends this decided transaction again. The retry then gets
		// `partition_migrating`, as the coordinator does in production, and commits after the import.
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

describe.concurrent("PartitionDO — a transaction through a hash jump to a partition that does not exist", () => {
	it("falls back from the jump, keeps Bloom off, and sends the keys of one owner in one call", async () => {
		const hashSplitN = 2;
		const partition = await splitHashOnce({ ns: CONTROLLED_NS, hashSplitN });
		await useSmallBloom(partition);

		// The root holds a Bloom filter only after it learns a promotion.
		await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("bob") });
		await raised(await partition.childOwning("bob")).awaitPromoted("bob");
		await partition.get({ hashKey: kb("bob"), sortKey: kb("sk1") });
		const rootBloom = await storedBloom(partition);

		// Two hash keys with the same child index at depths 0 and 1: one hash leaf owns both, and a
		// depth-2 hint for one of them names the same grandchild for the other. The filter of the root
		// reports h1 as promoted and h2 as not promoted, and no promotion moved either key.
		const path = (key: string) => [0, 1].map((depth) => hashChildIndex(kb(key), depth, hashSplitN));
		const h1 = findKey("tx-path", (k) => rootBloom.maybePromoted(kb(k)));
		const h2 = findKey("tx-path", (k) => path(k).join() === path(h1).join() && k !== h1 && !rootBloom.maybePromoted(kb(k)));
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
		expect(prepare).toMatchObject({ outcome: "accepted" });
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

describe.concurrent("PartitionDO — the Bloom step of the transaction shapes", () => {
	/**
	 * Gives `partition` a small Bloom filter, promotes `bob`, and reads it once, so the filter holds bob.
	 * Returns the stored filter. Other hash keys that it reports as promoted are false positives.
	 */
	const learnOnePromotion = async (partition: TestPartition): Promise<PartialRangeTopology> => {
		await useSmallBloom(partition);
		await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("bob") });
		await partition.awaitPromoted("bob");
		await partition.get({ hashKey: kb("bob"), sortKey: kb("sk1") });
		return await storedBloom(partition);
	};

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
		expect(prepare.value).toMatchObject({ outcome: "accepted" });
		expect(prepare.routing.forwardCount).toBe(1);
		expect(executedBy(prepare).ref.doName).toBe(owner.doName);

		const commit = await partition.stub.txCommit(partition.ctx, { transactionId, transactionTimestamp, items: [key] });
		expect(commit.value).toMatchObject({ outcome: "committed" });
		expect(commit.routing.forwardCount).toBe(1);
		expect(executedBy(commit).ref.doName).toBe(owner.doName);

		const snapshot = await partition.stub.txReadSnapshot(partition.ctx, { items: [key] });
		expect(snapshot.value).toMatchObject({ outcome: "committed", items: [{ found: true, data: "v" }] });
		expect(snapshot.routing.forwardCount).toBe(1);
		expect(executedBy(snapshot).ref.doName).toBe(owner.doName);
	}, 30_000);

	it("drops a Bloom hit that would split the keys of one owner, for a group and for a single owner", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS });
		const bloom = await learnOnePromotion(partition);

		// The partition owns every hash key below. A Bloom false positive names the range root of its
		// hash key, which does not exist. Without the guard, the commit sends the two hash keys in two
		// calls to the same partition, and it fails with `commit_keyset_mismatch`. In the first case the
		// second hash key has no Bloom hit and is local. In the second case both hash keys have a Bloom
		// hit and one exact target.
		const hit = findKey("hit", (k) => bloom.maybePromoted(kb(k)));
		const otherHit = findKey("hit", (k) => k !== hit && bloom.maybePromoted(kb(k)));
		const miss = findKey("miss", (k) => !bloom.maybePromoted(kb(k)));
		for (const [first, second] of [
			[hit, miss],
			[hit, otherHit],
		]) {
			const keys = [
				{ hashKey: kb(first), sortKey: kb(`a-${second}`) },
				{ hashKey: kb(second), sortKey: kb("b") },
			];
			const transactionId = crypto.randomUUID();
			const transactionTimestamp = Date.now();
			const prepare = await partition.rpc.txPrepare(partition.ctx, {
				transactionId,
				transactionTimestamp,
				coordinator: testCoordinatorRef(),
				items: withOpIndex(keys.map((key) => ({ ...key, operation: "put" as const, data: "v", kind: "text" as const }))),
			});
			expect(prepare).toMatchObject({ outcome: "accepted" });
			expect(await partition.rpc.txCommit(partition.ctx, { transactionId, transactionTimestamp, items: keys })).toMatchObject({
				outcome: "committed",
			});

			// A single owner holds both keys, thus the request runs here and does not answer `not_applicable`.
			const shot = await partition.rpc.txExecuteSingleShot(partition.ctx, {
				items: withOpIndex(keys.map((key) => ({ ...key, operation: "put" as const, data: "v2", kind: "text" as const }))),
			});
			expect(shot).toMatchObject({ outcome: "committed" });
			for (const key of keys) {
				expect(await partition.get(key)).toMatchObject({ found: true, item: { data: "v2" } });
			}
		}
		expect(await partition.localItemCount(hit)).toBe(2);
		expect(await partition.localItemCount(otherHit)).toBe(1);
		expect(await partition.localItemCount(miss)).toBe(1);
	}, 30_000);

	it("runs each request at the source when a Bloom false positive names a range root that does not exist", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS });
		const bloom = await learnOnePromotion(partition);
		// The filter reports this hash key as promoted, and no promotion started for it. Its range root
		// answers `range_partition_not_initialized`, and the source resolves the key again without the filter.
		const hashKey = findKey("false-positive", (k) => bloom.maybePromoted(kb(k)));
		const rangeRoot = partition.rangeRoot(hashKey);

		const put = await partition.put({ hashKey: kb(hashKey), sortKey: kb("sk1"), data: "v", kind: "text" });
		expect(put.meta.servedByActorName).toBe(partition.doName);
		expect(await partition.get({ hashKey: kb(hashKey), sortKey: kb("sk1") })).toMatchObject({
			found: true,
			item: { data: "v" },
			meta: { servedByActorName: partition.doName },
		});

		const key = { hashKey: kb(hashKey), sortKey: kb("sk2") };
		const transactionId = crypto.randomUUID();
		const transactionTimestamp = Date.now();
		const prepare = await partition.stub.txPrepare(partition.ctx, {
			transactionId,
			transactionTimestamp,
			coordinator: testCoordinatorRef(),
			items: withOpIndex([{ ...key, operation: "put", data: "v2", kind: "text" }]),
		});
		expect(prepare.value).toMatchObject({ outcome: "accepted" });
		expect(executedBy(prepare).ref.doName).toBe(partition.doName);

		const commit = await partition.stub.txCommit(partition.ctx, { transactionId, transactionTimestamp, items: [key] });
		expect(commit.value).toMatchObject({ outcome: "committed" });
		expect(executedBy(commit).ref.doName).toBe(partition.doName);
		expect(await partition.localItemCount(hashKey)).toBe(2);

		// The commit went to the range root first, and the range root stays without an identity.
		expect(await rangeRoot.controlled.testTxCalls("txCommit")).toHaveLength(1);
		expect(await rangeRoot.stub.fokosStatus({})).toMatchObject({ initialized: false });
	}, 30_000);

	it("prepares and commits at the hash leaf when a Bloom hit names a range root before its cutover", async () => {
		// The rare case of a false positive: the promotion of the key waits for the `fokosInit` reply, thus
		// the range root answers `partition_awaiting_data`. The previous test covers the usual case, a range
		// root that does not exist.
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		const bloom = await learnOnePromotion(partition);
		// The filter reports this hash key as promoted, and no promotion moved it.
		const hashKey = findKey("false-positive", (k) => bloom.maybePromoted(kb(k)));
		await partition.put({ hashKey: kb(hashKey), sortKey: kb("sk1"), data: "v", kind: "text" });

		const rangeRoot = partition.rangeRoot(hashKey);
		await rangeRoot.controlled.testHoldInit();
		const key = { hashKey: kb(hashKey), sortKey: kb("sk2") };
		try {
			// The range root has its identity and no page yet, because the source cuts over only after
			// `fokosInit` answers.
			await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb(hashKey) });
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
			expect(prepare.value).toMatchObject({ outcome: "accepted" });
			expect(prepare.routing.forwardCount).toBe(1);
			expect(executedBy(prepare).ref.doName).toBe(partition.doName);

			const commit = await partition.stub.txCommit(partition.ctx, { transactionId, transactionTimestamp, items: [key] });
			expect(commit.value).toMatchObject({ outcome: "committed" });
			expect(commit.routing.forwardCount).toBe(1);
			expect(executedBy(commit).ref.doName).toBe(partition.doName);
			expect(await partition.localItemCount(hashKey)).toBe(2);
		} finally {
			await rangeRoot.controlled.testReleaseInit();
		}

		// The import after the cutover brings the committed item to the range tree.
		await partition.awaitPromoted(hashKey);
		expect(await partition.get(key)).toMatchObject({ found: true, item: { data: "v2" } });
	}, 30_000);
});
