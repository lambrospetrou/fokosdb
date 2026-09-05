import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import { resolveRangePartitionContext } from "../../src/shared/partition-topology/partition-id.js";
import { KeyCodec } from "../../src/shared/partition-topology/key-codec.js";
import type { KeyBytes } from "../../src/shared/partition-topology/key-codec.js";
import {
	PROMOTION_BIG_DATA,
	PROMOTION_TEST_MAX_SIZE_MB,
	assertSplitTreeComplete,
	drainSplitTree,
	drainUntil,
	growPastSplitThreshold,
	kb,
	makeStub,
	splitStatusOf,
	triggerHashSplitThreshold,
	waitForAlarm,
	waitForPromotedKeyStatus,
} from "./helpers.js";

describe("PartitionDO — promotion detection and queuing", () => {
	it("detects a heavy key and immediately cuts over to 'promoting' when no locks are held", async () => {
		const { ctx, stub } = makeStub({
			hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB },
		});
		await stub.apiPutItem(ctx, { hashKey: kb("alice"), sortKey: kb("sk1"), data: PROMOTION_BIG_DATA, kind: "text" as const });

		// Migration may complete in the same background cycle as cutover, so accept 'promoted' too.
		await waitForPromotedKeyStatus(stub, "alice", ["promoting", "promoted"]);
	});

	it("does not detect any key when the DB is well below the promotion threshold", async () => {
		const { ctx, stub } = makeStub(); // default hashSplitConditions.maxSizeMb=100 → threshold 50 MB
		await stub.apiPutItem(ctx, { hashKey: kb("alice"), sortKey: kb("sk1"), data: "tiny", kind: "text" as const });

		await waitForAlarm(stub); // alarm may not be set at all; waitForAlarm is a no-op if so
		const s = await stub.status();
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
		const { ctx, stub } = makeStub({
			hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB },
		});
		await stub.apiPutItem(ctx, { hashKey: kb("alice"), sortKey: kb("sk1"), data: PROMOTION_BIG_DATA, kind: "text" as const });

		// Lock alice/sk1 so startPromotion defers the cutover and alice stays 'queued' throughout.
		const lockResult = await stub.txPrepare(ctx, {
			transactionId: crypto.randomUUID(),
			transactionTimestamp: Date.now(),
			coordinatorDoId: env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString(),
			items: [{ hashKey: kb("alice"), sortKey: kb("sk1"), operation: "put", data: "pending", kind: "text" }],
		});
		expect(lockResult.outcome).toBe("accepted");

		await waitForPromotedKeyStatus(stub, "alice", ["queued"]);

		// Now make a split genuinely warranted. Every one of these writes runs checkSplits.
		const databaseSize = await growPastSplitThreshold(stub, ctx, PROMOTION_TEST_MAX_SIZE_MB);

		const s = await stub.status();
		// Both halves matter: no split queued, AND the preconditions that make that meaningful.
		expect(databaseSize).toBeGreaterThan(PROMOTION_TEST_MAX_SIZE_MB * 1024 * 1024);
		expect(s.promotedKeys.find((e) => KeyCodec.compare(e.hashKey, kb("alice")) === 0)?.status).toBe("queued");
		expect(s.splitStatus).toBeUndefined();
	});
});

describe("PartitionDO — promotion cutover deferral and routing", () => {
	it("defers cutover to 'promoting' while the key has a pending transaction lock", async () => {
		const { ctx, stub } = makeStub({
			hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB },
		});
		await stub.apiPutItem(ctx, { hashKey: kb("alice"), sortKey: kb("sk1"), data: PROMOTION_BIG_DATA, kind: "text" as const });

		// Lock alice/sk1 with a prepare so the lock-free check in startPromotion defers.
		const txId = crypto.randomUUID();
		const coordId = env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString();
		const lockResult = await stub.txPrepare(ctx, {
			transactionId: txId,
			transactionTimestamp: Date.now(),
			coordinatorDoId: coordId,
			items: [{ hashKey: kb("alice"), sortKey: kb("sk1"), operation: "put", data: "pending", kind: "text" }],
		});
		expect(lockResult.outcome).toBe("accepted");

		// Detection queued alice but cutover was deferred — key must still be 'queued'.
		await waitForPromotedKeyStatus(stub, "alice", ["queued"]);

		// A write to alice while 'queued' is still served locally.
		const r = await stub.apiPutItem(ctx, { hashKey: kb("alice"), sortKey: kb("sk2"), data: "still-local", kind: "text" as const });
		expect(r.meta.forwardCount).toBe(0);

		// Release the lock; next background cycle should complete the cutover.
		await stub.txCancel(ctx, { transactionId: txId, items: [{ hashKey: kb("alice"), sortKey: kb("sk1") }] });
		await waitForPromotedKeyStatus(stub, "alice", ["promoting"]);
	});

	it("forwards reads and writes to the range root after cutover ('promoting')", async () => {
		const { ctx, stub } = makeStub({
			hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB },
		});
		await stub.apiPutItem(ctx, { hashKey: kb("alice"), sortKey: kb("sk1"), data: PROMOTION_BIG_DATA, kind: "text" as const });

		// Wait for detection + cutover to 'promoting'.
		const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(ctx, kb("alice"), null, null);
		const rangeRootStub = PartitionDO.getByName(env.PARTITION_DO, rangeRootCtx.doName);
		// Migration may complete in the same background cycle as cutover, so accept 'promoted' too.
		await waitForPromotedKeyStatus(stub, "alice", ["promoting", "promoted"]);

		// Drain the range root migration; hash DO transitions to 'promoted'.
		await waitForPromotedKeyStatus(stub, "alice", ["promoted"], { drain: [rangeRootStub] });

		// Writes via the hash partition are forwarded to the range root.
		const w = await stub.apiPutItem(ctx, { hashKey: kb("alice"), sortKey: kb("sk2"), data: "in-range", kind: "text" as const });
		expect(w.meta.forwardCount).toBe(1);
		// The response must surface the serving range root's own rangeDepth/rangeAncestors (0/[] for a
		// fresh root), not an empty/zero value from the forwarding hash partition's own context.
		expect(w.meta.rangeDepth).toBe(0);
		expect(w.meta._internal.rangeAncestors).toEqual([]);

		// Item is in the range root.
		const g = await rangeRootStub.apiGetItem(rangeRootCtx, { hashKey: kb("alice"), sortKey: kb("sk2") });
		expect(g).toMatchObject({ found: true, item: { data: "in-range" } });
		expect(g.meta.rangeDepth).toBe(0);
		expect(g.meta._internal.rangeAncestors).toEqual([]);
	});
});

describe("PartitionDO — hash-child migration excludes promoted keys", () => {
	it("items belonging to a promoted key are not migrated to hash split children", async () => {
		// Use maxSizeMb=1 so the per-child reject threshold (1.1MB) stays well above what any
		// child receives after migration, avoiding fragile databaseSize comparisons.
		const { ctx, stub } = makeStub({ hashSplitConditions: { maxSizeMb: 1 } });

		// Alice data exceeds the 512KB promotion threshold for maxSizeMb=1 across two items under the 400KB cap.
		const aliceChunk = "x".repeat(300 * 1024);
		await stub.apiPutItem(ctx, { hashKey: kb("alice"), sortKey: kb("sk1"), data: aliceChunk, kind: "text" as const });
		await stub.apiPutItem(ctx, { hashKey: kb("alice"), sortKey: kb("sk2"), data: aliceChunk, kind: "text" as const });

		// Wait for: detect → 'promoting' → range-root migration → 'promoted' → GC clears local alice items.
		const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(ctx, kb("alice"), null, null);
		const rangeRootStub = PartitionDO.getByName(env.PARTITION_DO, rangeRootCtx.doName);
		await drainUntil(
			[stub, rangeRootStub],
			async () => !(await stub.internalGetItemDirect({ hashKey: kb("alice"), sortKey: kb("sk1") })).found,
			"alice to be garbage-collected from the hash DO",
			8000,
		);

		// Trigger hash split with spread data; none exceeds the per-key promotion threshold.
		await triggerHashSplitThreshold(stub, ctx, 1);

		// Background: alice='promoted' so mutual exclusion allows the hash split to proceed.
		await drainSplitTree(stub);
		await assertSplitTreeComplete(stub);

		// alice's DATA must not be migrated into any hash child (the child inherits only the forward-pointer
		// entry, never the data — which lives in the range structure). The child that owns alice must:
		//   (a) hold no local copy of alice's data (strictly-local getItemDirect → not found), and
		//   (b) inherit alice's promoted-key entry, so a normal read forwards to the range structure.
		const splitStatus = await splitStatusOf(stub);
		for (const childCtx of splitStatus.childPartitionContexts) {
			const childStub = PartitionDO.getByName(env.PARTITION_DO, childCtx.doName);
			const local = await childStub.internalGetItemDirect({ hashKey: kb("alice"), sortKey: kb("sk1") });
			expect(local.found, `alice's data must not be migrated locally into hash child ${childCtx.doName}`).toBe(false);
		}

		// Exactly one hash child inherited alice's promoted-key entry; reading alice through THAT child
		// forwards to the range structure (the range root serves it), proving the inherited forward-pointer
		// works. Reading via the parent would forward through the parent's own cache and bypass the child,
		// so we read on the child directly to actually exercise inheritance.
		const aliceChildCtxs = [] as typeof splitStatus.childPartitionContexts;
		for (const childCtx of splitStatus.childPartitionContexts) {
			const childStub = PartitionDO.getByName(env.PARTITION_DO, childCtx.doName);
			const pk = (await childStub.status(childCtx)).promotedKeys;
			if (pk.some((e: { hashKey: KeyBytes; status: string }) => KeyCodec.compare(e.hashKey, kb("alice")) === 0 && e.status === "promoted"))
				aliceChildCtxs.push(childCtx);
		}
		expect(aliceChildCtxs, "exactly one hash child should inherit alice's promoted entry").toHaveLength(1);

		const aliceChildCtx = aliceChildCtxs[0];
		const aliceChildStub = PartitionDO.getByName(env.PARTITION_DO, aliceChildCtx.doName);
		const aliceRead = await aliceChildStub.apiGetItem(aliceChildCtx, {
			hashKey: kb("alice"),
			sortKey: kb("sk1"),
		});
		expect(aliceRead.found, "alice must be reachable through its hash child via the inherited forward-pointer").toBe(true);
		expect(aliceRead.meta.forwardCount).toBeGreaterThanOrEqual(1);
		expect(aliceRead.meta.servedByActorName, "alice must be served by the range structure, not the hash child").toBe(rangeRootCtx.doName);

		// A split-trigger key must be reachable via the hash DO (forwarded to the owning child).
		const trigResult = await stub.apiGetItem(ctx, { hashKey: kb("_split_trig_0"), sortKey: kb("sk") });
		expect(trigResult).toMatchObject({
			found: true,
			meta: { forwardCount: 1 },
		});
	});
});
