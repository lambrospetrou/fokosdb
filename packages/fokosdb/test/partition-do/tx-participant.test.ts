import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import { isPartitionExceededDatabaseSizeError, isSinglePartitionFastPathFallbackError } from "../../src/shared/partition-errors.js";
import type { PartitionContextResolved } from "../../src/shared/partition-topology/partition-context.js";
import { resolveRangePartitionContext } from "../../src/shared/partition-topology/partition-id.js";
import { KeyCodec } from "../../src/shared/partition-topology/key-codec.js";
import invariant from "../../src/shared/invariant.js";
import {
	PROMOTION_BIG_DATA,
	PROMOTION_TEST_MAX_SIZE_MB,
	compiledCondition,
	drainSplitTree,
	kb,
	makeStub,
	triggerHashSplitThreshold,
	waitForPromotedKeyStatus,
} from "./helpers.js";

describe("PartitionDO — transactions spanning local and promoted keys", () => {
	it("prepare+commit spanning a local key and a promoted key both commit", async () => {
		// Promote alice, leave bob local.
		const { ctx, stub } = makeStub({
			hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB },
		});
		await stub.apiPutItem(ctx, { hashKey: kb("alice"), sortKey: kb("sk1"), data: PROMOTION_BIG_DATA, kind: "text" as const });
		const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(ctx, kb("alice"), null, null);
		const rangeRootStub = PartitionDO.getByName(env.PARTITION_DO, rangeRootCtx.doName);
		await waitForPromotedKeyStatus(stub, "alice", ["promoted"], { drain: [stub, rangeRootStub] });

		// Transaction touches alice/sk2 (forwarded to range root) and bob/sk1 (local).
		const txId = crypto.randomUUID();
		const coordId = env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString();
		const prepareResp = await stub.txPrepare(ctx, {
			transactionId: txId,
			transactionTimestamp: Date.now(),
			coordinatorDoId: coordId,
			items: [
				{ hashKey: kb("alice"), sortKey: kb("sk2"), operation: "put", data: "from-txn", kind: "text" },
				{ hashKey: kb("bob"), sortKey: kb("sk1"), operation: "put", data: "bob-data", kind: "text" },
			],
		});
		expect(prepareResp.outcome).toBe("accepted");

		await stub.txCommit(ctx, {
			transactionId: txId,
			transactionTimestamp: Date.now(),
			// Keys only: the participant applies the payload from its own pending_transactions rows.
			items: [
				{ hashKey: kb("alice"), sortKey: kb("sk2") },
				{ hashKey: kb("bob"), sortKey: kb("sk1") },
			],
		});

		// alice/sk2 must be in the range root; bob/sk1 must be local on the hash DO.
		const aliceResult = await rangeRootStub.apiGetItem(rangeRootCtx, {
			hashKey: kb("alice"),
			sortKey: kb("sk2"),
		});
		expect(aliceResult).toMatchObject({ found: true, item: { data: "from-txn" } });

		const bobResult = await stub.apiGetItem(ctx, { hashKey: kb("bob"), sortKey: kb("sk1") });
		expect(bobResult).toMatchObject({ found: true, item: { data: "bob-data" } });
	});

	it("cancel via hash DO releases both local and promoted-key locks", async () => {
		const { ctx, stub } = makeStub({
			hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB },
		});
		await stub.apiPutItem(ctx, { hashKey: kb("alice"), sortKey: kb("sk1"), data: PROMOTION_BIG_DATA, kind: "text" as const });
		const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(ctx, kb("alice"), null, null);
		const rangeRootStub = PartitionDO.getByName(env.PARTITION_DO, rangeRootCtx.doName);
		await waitForPromotedKeyStatus(stub, "alice", ["promoted"], { drain: [stub, rangeRootStub] });

		const txId = crypto.randomUUID();
		const coordId = env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString();
		const prepareResp = await stub.txPrepare(ctx, {
			transactionId: txId,
			transactionTimestamp: Date.now(),
			coordinatorDoId: coordId,
			items: [
				{ hashKey: kb("alice"), sortKey: kb("sk2"), operation: "put", data: "alice-data", kind: "text" },
				{ hashKey: kb("bob"), sortKey: kb("sk1"), operation: "put", data: "bob-data", kind: "text" },
			],
		});
		expect(prepareResp.outcome).toBe("accepted");

		// Cancel via the hash DO. alice is promoted, so its lock lives on the range root and only the
		// routed fan-out can release it; bob's lock is local. Both must be gone below.
		await stub.txCancel(ctx, {
			transactionId: txId,
			items: [
				{ hashKey: kb("alice"), sortKey: kb("sk2") },
				{ hashKey: kb("bob"), sortKey: kb("sk1") },
			],
		});

		// Both locks must be gone — a new prepare for the same keys must succeed.
		const txId2 = crypto.randomUUID();
		const prepareResp2 = await stub.txPrepare(ctx, {
			transactionId: txId2,
			transactionTimestamp: Date.now() + 1,
			coordinatorDoId: coordId,
			items: [
				{ hashKey: kb("alice"), sortKey: kb("sk2"), operation: "put", data: "retried", kind: "text" },
				{ hashKey: kb("bob"), sortKey: kb("sk1"), operation: "put", data: "retried", kind: "text" },
			],
		});
		expect(prepareResp2.outcome).toBe("accepted");
		await stub.txCancel(ctx, {
			transactionId: txId2,
			items: [
				{ hashKey: kb("alice"), sortKey: kb("sk2") },
				{ hashKey: kb("bob"), sortKey: kb("sk1") },
			],
		});
	});
});

describe("PartitionDO — transaction routing separates backpressure from mis-routing", () => {
	// An empty SQLite database is already several KB, so this cap is exceeded before anything is
	// written and every "write" is refused for size.
	const OVER_SIZE = { hashSplitConditions: { maxSizeMb: 0.000_001 } };

	const txItems = [{ hashKey: kb("alice"), sortKey: kb("sk1"), operation: "put" as const, data: "d", kind: "text" as const }];

	// An over-size partition is healthy, just full.
	// The isPartitionOverSizeError assertion is the load-bearing one: the coordinator uses it to skip
	// retries, and it sees this error only AFTER a Durable Object RPC hop, which keeps the message but
	// drops the class. Asserting it here, on a genuinely remote error, is what proves the skip fires.
	it("prepare on an over-size partition reports backpressure", async () => {
		const { ctx, stub } = makeStub(OVER_SIZE);
		const error = await stub
			.txPrepare(ctx, {
				transactionId: crypto.randomUUID(),
				transactionTimestamp: Date.now(),
				coordinatorDoId: env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString(),
				items: txItems,
			})
			.then(
				() => null,
				(e: unknown) => e,
			);
		expect(String(error)).toMatch(/partition exceeded its limits/);
		expect(String(error)).not.toMatch(/mis-routed/);
		expect(isPartitionExceededDatabaseSizeError(error)).toBe(true);
	});

	// Commit is non-growing (prepare already persisted the payload) and its outcome is already
	// decided, so an over-size partition must not refuse it — that would wedge the transaction.
	it("commit is not refused by an over-size partition", async () => {
		const { ctx, stub } = makeStub(OVER_SIZE);
		// No prepare ran, so commit finds no pending rows and is a no-op — enough to prove it routed.
		await expect(
			stub.txCommit(ctx, {
				transactionId: crypto.randomUUID(),
				transactionTimestamp: Date.now(),
				items: [{ hashKey: kb("alice"), sortKey: kb("sk1") }],
			}),
		).resolves.toEqual({ outcome: "committed" });
	});

	// Reads cannot grow a partition either, so they stay available.
	it("readForTransaction is not refused by an over-size partition", async () => {
		const { ctx, stub } = makeStub(OVER_SIZE);
		const res = await stub.txReadForTransaction(ctx, {
			transactionId: crypto.randomUUID(),
			items: [{ hashKey: kb("alice"), sortKey: kb("sk1") }],
		});
		expect(res.items).toHaveLength(1);
	});
});

describe("PartitionDO — single-shot transaction", () => {
	/** Counts the lock rows this partition holds, whatever transaction owns them. */
	async function pendingLockCount(stub: DurableObjectStub<PartitionDO>): Promise<number> {
		return await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) => {
			return state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM pending_transactions`).one().n;
		});
	}

	it("applies puts, deletes and checks in one shot, and takes no lock", async () => {
		const { ctx, stub } = makeStub();
		await stub.apiPutItem(ctx, { hashKey: kb("shot-gone"), sortKey: kb("sk"), data: "old", kind: "text" as const });
		await stub.apiPutItem(ctx, { hashKey: kb("shot-checked"), sortKey: kb("sk"), data: "keep", kind: "text" as const });

		const res = await stub.txExecuteSingleShot(ctx, {
			items: [
				{ hashKey: kb("shot-new"), sortKey: kb("sk"), operation: "put", data: "written", kind: "text" },
				{ hashKey: kb("shot-gone"), sortKey: kb("sk"), operation: "delete" },
				{
					hashKey: kb("shot-checked"),
					sortKey: kb("sk"),
					operation: "check",
					condition: compiledCondition({ op: "exists", args: [{ ref: "hashKey" }] }),
				},
			],
		});

		expect(res).toEqual({ outcome: "committed" });
		expect(await stub.apiGetItem(ctx, { hashKey: kb("shot-new"), sortKey: kb("sk") })).toMatchObject({
			found: true,
			item: { data: "written" },
		});
		expect(await stub.apiGetItem(ctx, { hashKey: kb("shot-gone"), sortKey: kb("sk") })).toMatchObject({ found: false });
		expect(await stub.apiGetItem(ctx, { hashKey: kb("shot-checked"), sortKey: kb("sk") })).toMatchObject({
			found: true,
			item: { data: "keep" },
		});
		// Nothing to cancel, nothing to recover: this path holds no lock at any point.
		expect(await pendingLockCount(stub)).toBe(0);
	});

	it("rejects on a failed condition, leaving no partial write and no lock", async () => {
		const { ctx, stub } = makeStub();
		await stub.apiPutItem(ctx, { hashKey: kb("atomic-existing"), sortKey: kb("sk"), data: "v1", kind: "text" as const });

		// The failing item is LAST, so a non-atomic implementation would already have written the first.
		const res = await stub.txExecuteSingleShot(ctx, {
			items: [
				{ hashKey: kb("atomic-existing"), sortKey: kb("sk"), operation: "put", data: "v2", kind: "text" },
				{ hashKey: kb("atomic-other"), sortKey: kb("sk"), operation: "put", data: "never", kind: "text" },
				{
					hashKey: kb("atomic-absent"),
					sortKey: kb("sk"),
					operation: "check",
					condition: compiledCondition({ op: "exists", args: [{ ref: "hashKey" }] }),
				},
			],
		});

		expect(res).toEqual({ outcome: "rejected", reason: { type: "condition_failed", hashKey: "atomic-absent", sortKey: "sk" } });
		expect(await stub.apiGetItem(ctx, { hashKey: kb("atomic-existing"), sortKey: kb("sk") })).toMatchObject({
			found: true,
			item: { data: "v1", version: 1 },
		});
		expect(await stub.apiGetItem(ctx, { hashKey: kb("atomic-other"), sortKey: kb("sk") })).toMatchObject({ found: false });
		expect(await pendingLockCount(stub)).toBe(0);
	});

	it("rejects with pending_conflict against a two-phase transaction that holds a lock", async () => {
		const { ctx, stub } = makeStub();
		const transactionId = crypto.randomUUID();
		const prepared = await stub.txPrepare(ctx, {
			transactionId,
			transactionTimestamp: Date.now(),
			coordinatorDoId: env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString(),
			items: [{ hashKey: kb("shot-locked"), sortKey: kb("sk"), operation: "put", data: "two-phase", kind: "text" }],
		});
		expect(prepared.outcome).toBe("accepted");

		const res = await stub.txExecuteSingleShot(ctx, {
			items: [
				{ hashKey: kb("shot-free"), sortKey: kb("sk"), operation: "put", data: "never", kind: "text" },
				{ hashKey: kb("shot-locked"), sortKey: kb("sk"), operation: "put", data: "never", kind: "text" },
			],
		});

		// The two-phase transaction may still commit, so this one loses rather than overwriting it.
		expect(res).toMatchObject({
			outcome: "rejected",
			reason: { type: "pending_conflict", hashKey: "shot-locked", conflictingTransactionId: transactionId },
		});
		expect(await stub.apiGetItem(ctx, { hashKey: kb("shot-free"), sortKey: kb("sk") })).toMatchObject({ found: false });
		// Only the two-phase lock, and this path added none of its own.
		expect(await pendingLockCount(stub)).toBe(1);

		await stub.txCancel(ctx, { transactionId, items: [{ hashKey: kb("shot-locked"), sortKey: kb("sk") }] });
	});

	it("reports backpressure from an over-size partition", async () => {
		// An empty SQLite database is already several KB, so this cap is exceeded before anything is
		// written and every write is refused for size.
		const { ctx, stub } = makeStub({ hashSplitConditions: { maxSizeMb: 0.000_001 } });
		const error = await stub
			.txExecuteSingleShot(ctx, { items: [{ hashKey: kb("over-size"), sortKey: kb("sk"), operation: "put", data: "d", kind: "text" }] })
			.then(
				() => null,
				(e: unknown) => e,
			);
		expect(isPartitionExceededDatabaseSizeError(error)).toBe(true);
	});

	it("queues a split once its writes push the partition over the threshold", async () => {
		const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		const data = "x".repeat(64 * 1024);

		for (let i = 0; i < 40; i++) {
			const res = await stub.txExecuteSingleShot(ctx, {
				items: [{ hashKey: kb(`shot-split-${i}`), sortKey: kb("sk"), operation: "put", data, kind: "bytes" }],
			});
			expect(res.outcome).toBe("committed");
			if ((await stub.status()).splitStatus) break;
		}

		// Only the write paths that call checkSplits can queue a split — the background job runs one
		// that is already queued, it never queues one itself.
		const { splitStatus } = await stub.status();
		expect(splitStatus).toBeDefined();
		expect(["split_queued", "split_started", "split_completed"]).toContain(splitStatus?.status);
		await drainSplitTree(stub);
	});
});

describe("PartitionDO — two-phase commit queues splits", () => {
	it("commits a prepared TTL put after its pending lock migrates through a hash split", async () => {
		const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		const transactionId = crypto.randomUUID();
		const transactionTimestamp = Date.now();
		const ttlAt = Math.floor(Date.now() / 1000) + 3600;
		const items = [
			{ hashKey: kb("split-ttl-put"), sortKey: kb("sk"), operation: "put" as const, data: "value", kind: "text" as const, ttlAt },
		];
		expect(
			await stub.txPrepare(ctx, {
				transactionId,
				transactionTimestamp,
				coordinatorDoId: env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString(),
				items,
			}),
		).toEqual({ outcome: "accepted" });

		await triggerHashSplitThreshold(stub, ctx, 1);
		await drainSplitTree(stub);
		expect((await stub.status()).splitStatus?.status).toBe("split_completed");
		expect(
			await stub.txCommit(ctx, {
				transactionId,
				transactionTimestamp,
				// Keys only: the split parent routes them to the children, which apply from their own
				// pending_transactions rows.
				items: items.map(({ hashKey, sortKey }) => ({ hashKey, sortKey })),
			}),
		).toEqual({ outcome: "committed" });
		expect(await stub.apiGetItem(ctx, { hashKey: items[0].hashKey, sortKey: items[0].sortKey })).toMatchObject({
			found: true,
			item: { data: "value", ttlAt },
		});
	});

	it("queues a split once committed transactions push the partition over the threshold", async () => {
		const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		const data = "x".repeat(64 * 1024);
		const coordinatorDoId = env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString();

		for (let i = 0; i < 40; i++) {
			const transactionId = crypto.randomUUID();
			const items = [{ hashKey: kb(`commit-split-${i}`), sortKey: kb("sk"), operation: "put" as const, data, kind: "bytes" as const }];
			const transactionTimestamp = Date.now() + i;
			expect(await stub.txPrepare(ctx, { transactionId, transactionTimestamp, coordinatorDoId, items })).toEqual({ outcome: "accepted" });
			await stub.txCommit(ctx, { transactionId, transactionTimestamp, items: items.map(({ hashKey, sortKey }) => ({ hashKey, sortKey })) });
			if ((await stub.status()).splitStatus) break;
		}

		// The background job only RUNS a queued split, so a status here proves commit queued one.
		const { splitStatus } = await stub.status();
		expect(splitStatus).toBeDefined();
		expect(["split_queued", "split_started", "split_completed"]).toContain(splitStatus?.status);
		await drainSplitTree(stub);
	});
});

describe("PartitionDO — single-partition read snapshot", () => {
	it("answers every key from local storage, positionally matched to the request", async () => {
		const { ctx, stub } = makeStub();
		await stub.apiPutItem(ctx, { hashKey: kb("snap-a"), sortKey: kb("sk"), data: "a", kind: "text" as const });
		await stub.apiPutItem(ctx, { hashKey: kb("snap-b"), sortKey: kb("sk"), data: "b", kind: "text" as const });

		// A missing key and a duplicate: each requested position gets its own answer.
		const requested = [
			{ hashKey: kb("snap-b"), sortKey: kb("sk") },
			{ hashKey: kb("snap-missing"), sortKey: kb("sk") },
			{ hashKey: kb("snap-a"), sortKey: kb("sk") },
			{ hashKey: kb("snap-b"), sortKey: kb("sk") },
		];
		const res = await stub.txReadSnapshot(ctx, { items: requested });

		invariant(res.outcome === "committed");
		expect(res.items.map((i) => i.found)).toEqual([true, false, true, true]);
		expect(res.items.map((i) => KeyCodec.decode(i.hashKey))).toEqual(["snap-b", "snap-missing", "snap-a", "snap-b"]);
		expect(res.items.filter((i) => i.found).map((i) => (i.found ? i.data : null))).toEqual(["b", "a", "b"]);
	});

	it("aborts with pending_write when a two-phase transaction holds a lock on one of the keys", async () => {
		const { ctx, stub } = makeStub();
		await stub.apiPutItem(ctx, { hashKey: kb("snap-free"), sortKey: kb("sk"), data: "free", kind: "text" as const });

		const prepared = await stub.txPrepare(ctx, {
			transactionId: crypto.randomUUID(),
			transactionTimestamp: Date.now(),
			coordinatorDoId: env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString(),
			items: [{ hashKey: kb("snap-locked"), sortKey: kb("sk"), operation: "put", data: "pending", kind: "text" }],
		});
		expect(prepared.outcome).toBe("accepted");

		const res = await stub.txReadSnapshot(ctx, {
			items: [
				{ hashKey: kb("snap-free"), sortKey: kb("sk") },
				{ hashKey: kb("snap-locked"), sortKey: kb("sk") },
			],
		});
		expect(res).toEqual({ outcome: "aborted", reason: "pending_write" });
	});

	describe("after a hash split", () => {
		/** Groups probe keys by the child DO that serves them, asking the split root who answered. */
		async function keysByServingChild(
			stub: DurableObjectStub<PartitionDO>,
			ctx: PartitionContextResolved,
			count: number,
		): Promise<Map<string, string[]>> {
			const byChild = new Map<string, string[]>();
			for (let i = 0; i < count; i++) {
				const hashKey = `probe-${i}`;
				const res = await stub.apiGetItem(ctx, { hashKey: kb(hashKey), sortKey: kb("sk") });
				const child = res.meta.servedByActorName;
				expect(child).not.toBe(ctx.doName);
				byChild.set(child, [...(byChild.get(child) ?? []), hashKey]);
			}
			return byChild;
		}

		it("hands the whole request to the one child that owns every key, and falls back when the keys span two", async () => {
			const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
			await triggerHashSplitThreshold(stub, ctx, 1);
			await drainSplitTree(stub);

			// Routing is a pure hash of the key bytes, so this grouping is deterministic, not flaky.
			const byChild = await keysByServingChild(stub, ctx, 10);
			expect(byChild.size).toBe(2);
			const [childA, childB] = [...byChild.values()];

			// One destination: the split root owns nothing itself and forwards the whole set.
			const oneChild = await stub.txReadSnapshot(ctx, { items: childA.slice(0, 2).map((hk) => ({ hashKey: kb(hk), sortKey: kb("sk") })) });
			invariant(oneChild.outcome === "committed");
			expect(oneChild.items.map((i) => KeyCodec.decode(i.hashKey))).toEqual(childA.slice(0, 2));

			// Two destinations: no single DO can answer, so the fallback error comes back with nothing
			// touched. It crosses a real RPC hop here, which keeps the message but drops the class —
			// which is what makes the sentinel predicate the thing under test.
			const error = await stub
				.txReadSnapshot(ctx, {
					items: [
						{ hashKey: kb(childA[0]), sortKey: kb("sk") },
						{ hashKey: kb(childB[0]), sortKey: kb("sk") },
					],
				})
				.then(
					() => null,
					(e: unknown) => e,
				);
			expect(isSinglePartitionFastPathFallbackError(error)).toBe(true);
		});
	});
});
