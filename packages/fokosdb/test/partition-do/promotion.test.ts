import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { kb, withOpIndex } from "./helpers.js";
import {
	PROMOTION_BIG_DATA,
	PROMOTION_TEST_MAX_SIZE_MB,
	type TestPartition,
	assertSplitTreeComplete,
	drainUntil,
	makePartition,
} from "./partition-harness.js";

describe("PartitionDO — promotion detection and queuing", () => {
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
		const lockResult = await partition.stub.txPrepare(partition.ctx, {
			transactionId: txId,
			transactionTimestamp: Date.now(),
			coordinatorDoId: env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString(),
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
			await partition.stub.txCancel(partition.ctx, { transactionId: txId, items: [{ hashKey: kb("alice"), sortKey: kb("sk1") }] });
			await partition.awaitPromoted("alice");
		}
	});
});

describe("PartitionDO — promotion cutover deferral and routing", () => {
	it("defers cutover to 'promoting' while the key has a pending transaction lock", async () => {
		const partition = makePartition({ hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });

		// Lock alice/sk1 with a prepare so the lock-free check in startPromotion defers.
		const txId = crypto.randomUUID();
		const lockResult = await partition.stub.txPrepare(partition.ctx, {
			transactionId: txId,
			transactionTimestamp: Date.now(),
			coordinatorDoId: env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString(),
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
			await partition.stub.txCancel(partition.ctx, { transactionId: txId, items: [{ hashKey: kb("alice"), sortKey: kb("sk1") }] });
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
		const w = await partition.put({ hashKey: kb("alice"), sortKey: kb("sk2"), data: "in-range", kind: "text" as const });
		expect(w.meta.forwardCount).toBe(1);
		// The response must surface the serving range root's own rangeDepth/rangeAncestors (0/[] for a
		// fresh root), not an empty/zero value from the forwarding hash partition's own context.
		expect(w.meta.rangeDepth).toBe(0);
		expect(w.meta._internal.rangeAncestors).toEqual([]);

		// Item is in the range root.
		const g = await rangeRoot.get({ hashKey: kb("alice"), sortKey: kb("sk2") });
		expect(g).toMatchObject({ found: true, item: { data: "in-range" } });
		expect(g.meta.rangeDepth).toBe(0);
		expect(g.meta._internal.rangeAncestors).toEqual([]);
	});
});

describe("PartitionDO — hash-child migration excludes promoted keys", () => {
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
			async () => !(await partition.stub.internalGetItemDirect({ hashKey: kb("alice"), sortKey: kb("sk1") })).found,
			"alice to be garbage-collected from the hash DO",
			8000,
		);

		// Trigger the hash split with spread data; none of it exceeds the per-key promotion threshold.
		// alice is 'promoted' by now, so mutual exclusion lets the hash split proceed.
		const splitItems = await partition.triggerHashSplit();
		await partition.awaitSplitCompleted();
		const children = await partition.children();
		await assertSplitTreeComplete(partition);

		// alice's DATA must not be migrated into any hash child (the child inherits only the forward-pointer
		// entry, never the data — which lives in the range structure). The child that owns alice must:
		//   (a) hold no local copy of alice's data (strictly-local getItemDirect → not found), and
		//   (b) inherit alice's promoted-key entry, so a normal read forwards to the range structure.
		const aliceChildren: TestPartition[] = [];
		for (const child of children) {
			const local = await child.stub.internalGetItemDirect({ hashKey: kb("alice"), sortKey: kb("sk1") });
			expect(local.found, `alice's data must not be migrated locally into hash child ${child.doName}`).toBe(false);
			if ((await child.promotedKeyStatus("alice")) === "promoted") aliceChildren.push(child);
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
