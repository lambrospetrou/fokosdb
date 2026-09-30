import { env } from "cloudflare:workers";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import type { PartitionDO } from "../../src/server/do-partition.js";
import type { TransactionCoordinatorDO } from "../../src/server/do-transaction-coordinator.js";
import * as doStubs from "../../src/shared/do-stubs.js";
import { testCoordinatorContext, testCoordinatorStubByName, testPartitionStub } from "../stub-helpers.js";
import type { FokosDBRouteContext } from "../../src/shared/partition-context.js";
import { PartitionIdHelper } from "../../src/sharding/partition-id.js";
import { refOf } from "../../src/sharding/route-context.js";
import { DEFAULT_STALE_TRANSACTION_MS, IDEMPOTENCY_WINDOW_MS } from "../../src/shared/transaction-limits.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { FOKOS_KV_KEYS, FokosShardingStore } from "../../src/sharding/sharding-store.js";
import type { FokosImportRecord } from "../../src/sharding/repartition-types.js";
import { captureConsoleError, kb, makeStub } from "./helpers.js";

const LOCK_AGE_GUARD_LOG = "fokos/partition: lock-age guard: over-age lock with not_found";

describe("PartitionDO — stale transaction recovery", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	function insertStalePendingLock(
		state: DurableObjectState,
		transactionId: string,
		coordinator: FokosDBRouteContext,
		options?: { createdAt?: number; hashKey?: string; data?: string; guardedAt?: number | null; transactionTimestamp?: number },
	): PartitionStore {
		const createdAt = options?.createdAt ?? Date.now() - 10_000;
		const store = new PartitionStore(state.storage);
		store.insertPendingLock({
			hk: kb(options?.hashKey ?? `stale-${transactionId}`),
			sk: kb("sk"),
			transaction_id: transactionId,
			transaction_ts: options?.transactionTimestamp ?? createdAt,
			operation: "put",
			data: options?.data ?? "value",
			kind: "text",
			ttl_epoch_utc_seconds: null,
			coordinator_json: JSON.stringify({ v: 1, doName: coordinator.doName, idempotencyToken: `token-${transactionId}` }),
			created_at: createdAt,
			guarded_at: options?.guardedAt ?? null,
			next_recovery_at: createdAt + DEFAULT_STALE_TRANSACTION_MS,
		});
		return store;
	}

	/** Releases every remaining row of one transaction, to leave the partition without an alarm. */
	function releasePendingLock(store: PartitionStore, transactionId: string): void {
		store.deletePendingTxKeys(
			transactionId,
			store.listPendingTxKeys(transactionId).map((row) => ({ hashKey: row.hk, sortKey: row.sk })),
		);
	}

	/**
	 * Substitutes the coordinator stub with a fake that answers `not_found`.
	 *
	 * The substitution is at the `txCoordinatorStubForParticipant` helper, not at the class prototype: the
	 * helper returns an RPC stub whose target runs outside this isolate, so a prototype spy records
	 * nothing. The partial fake is why the cast is here.
	 */
	function mockCoordinatorRecovery() {
		const recoverTransaction = vi.fn(async () => ({ state: "not_found" as const }));
		vi.spyOn(doStubs, "txCoordinatorStubForParticipant").mockReturnValue({
			recoverTransactionForParticipant: recoverTransaction,
		} as unknown as DurableObjectStub<TransactionCoordinatorDO>);
		return recoverTransaction;
	}

	// A source that has cut over is a router. Its targets own the keys and hold the true locks.
	it.each(["cutover", "completed"] as const)("skips stale recovery on a source in %s", async (state_) => {
		const { ctx, stub, rpc } = makeStub({ hashSplitN: 2 });
		await rpc.status(ctx);
		const recoverTransaction = mockCoordinatorRecovery();
		const transactionId = crypto.randomUUID();
		const coordinator = testCoordinatorContext();
		const childPartitionContexts: FokosDBRouteContext[] = PartitionIdHelper.calculateHashChildPartitionIds(ctx).map((child) => ({
			...ctx,
			doName: child.doName,
			partitionId: child.partitionIdOpaque,
		}));

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			const sharding = new FokosShardingStore(state.storage);
			const now = Date.now();
			sharding.insertRepartition({ id: "r1", seq: 1, kind: "hash_split", state: state_, hashKey: null, queuedAt: now, nextAttemptAt: now });
			childPartitionContexts.forEach((child, index) => {
				sharding.insertRepartitionTarget({
					repartitionId: "r1",
					kind: "hash_split",
					partitionId: child.partitionId,
					doName: child.doName,
					targetIndex: index,
					slice: { kind: "hash_child", childIndex: index },
					initialization: "initialized",
					startNotified: true,
					acknowledged: state_ === "completed",
					nextAttemptAt: now,
				});
			});
			const store = insertStalePendingLock(state, transactionId, coordinator);

			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: Date.now() });

			expect(recoverTransaction).not.toHaveBeenCalled();
			expect(store.listPendingTxKeys(transactionId).length).toBe(1);
			expect(await state.storage.getAlarm()).toBeNull();
		});
	});

	// An incomplete target holds only some of the rows and only some of the inherited locks.
	it.each(["awaiting_data", "importing"] as const)("skips stale recovery on a target in %s", async (importState) => {
		const { ctx: parentCtx } = makeStub({ hashSplitN: 2 });
		const child = PartitionIdHelper.calculateHashChildPartitionIds(parentCtx)[0];
		const childId = env.PARTITION_DO.idFromName(child.doName);
		const childCtx: FokosDBRouteContext = {
			...parentCtx,
			doName: child.doName,
			partitionId: child.partitionIdOpaque,
		};
		const childStub = testPartitionStub(childId);
		await childStub.fokosInit({
			repartitionId: "r1",
			source: refOf(parentCtx),
			target: childCtx,
			slice: { kind: "hash_child", childIndex: 0, depth: 1 },
		});
		const recoverTransaction = mockCoordinatorRecovery();
		const transactionId = crypto.randomUUID();
		const coordinator = testCoordinatorContext();

		await runInDurableObject(childStub, async (instance: PartitionDO, state: DurableObjectState) => {
			const record = state.storage.kv.get<FokosImportRecord>(FOKOS_KV_KEYS.IMPORT)!;
			state.storage.kv.put<FokosImportRecord>(FOKOS_KV_KEYS.IMPORT, { ...record, state: importState });
			// This test cannot reach the source, so the import step fails and logs. The case is about
			// recovery staying away, and not about how far the import gets.
			const store = insertStalePendingLock(state, transactionId, coordinator);

			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: Date.now() });

			expect(recoverTransaction).not.toHaveBeenCalled();
			expect(store.listPendingTxKeys(transactionId).length).toBe(1);
			state.storage.kv.put<FokosImportRecord>(FOKOS_KV_KEYS.IMPORT, { ...record, state: "active" });
			releasePendingLock(store, transactionId);
			await state.storage.deleteAlarm();
		});
	});

	it("releases a not_found lock at the exact idempotency-window boundary", async () => {
		const now = Date.now();
		const { ctx, stub, rpc } = makeStub();
		await rpc.status(ctx);
		mockCoordinatorRecovery();
		const consoleError = captureConsoleError();
		const transactionId = crypto.randomUUID();

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			vi.spyOn(instance, "fokosNow").mockReturnValue(now);
			const store = insertStalePendingLock(state, transactionId, testCoordinatorContext(), { createdAt: now - IDEMPOTENCY_WINDOW_MS });
			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			expect(store.listPendingTxKeys(transactionId).length).toBe(0);
		});
		expect(consoleError.withMessage(LOCK_AGE_GUARD_LOG).filter((log) => log.transactionId === transactionId)).toHaveLength(0);
	});

	it("quarantines an over-age owned lock and logs the transition once", async () => {
		const now = Date.now();
		const { ctx, stub, rpc } = makeStub();
		await rpc.status(ctx);
		const recoverTransaction = mockCoordinatorRecovery();
		const consoleError = captureConsoleError();
		const transactionId = crypto.randomUUID();
		const coordinator = testCoordinatorContext();
		const hashKey = `guard-${transactionId}`;

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			vi.spyOn(instance, "fokosNow").mockReturnValue(now);
			const store = insertStalePendingLock(state, transactionId, coordinator, {
				createdAt: now - IDEMPOTENCY_WINDOW_MS - 1,
				hashKey,
			});
			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			expect(store.listPendingTxKeys(transactionId).length).toBe(1);
			expect(store.listPendingTxItems(transactionId)[0].guarded_at).toBe(now);
			expect(await state.storage.getAlarm()).toBeNull();
		});

		expect(recoverTransaction).toHaveBeenCalledTimes(1);
		const guardLogs = consoleError.withMessage(LOCK_AGE_GUARD_LOG).filter((log) => log.transactionId === transactionId);
		expect(guardLogs).toHaveLength(1);
		expect(guardLogs[0]).toMatchObject({
			transactionId,
			coordinatorDoName: coordinator.doName,
			idempotencyToken: `token-${transactionId}`,
			keys: [KeyCodec.pairForLog(kb(hashKey), kb("sk"))],
			lockCreatedAt: now - IDEMPOTENCY_WINDOW_MS - 1,
			lockAgeMs: IDEMPOTENCY_WINDOW_MS + 1,
			windowMs: IDEMPOTENCY_WINDOW_MS,
			doName: ctx.doName,
			partitionId: ctx.partitionId,
		});
	});

	it("does not quarantine or release a lock when the coordinator RPC fails", async () => {
		const now = Date.now();
		const { ctx, stub, rpc } = makeStub();
		await rpc.status(ctx);
		const recoverTransaction = vi.fn(async () => {
			throw new Error("coordinator unavailable");
		});
		vi.spyOn(doStubs, "txCoordinatorStubForParticipant").mockReturnValue({
			recoverTransactionForParticipant: recoverTransaction,
		} as unknown as DurableObjectStub<TransactionCoordinatorDO>);
		const consoleError = captureConsoleError();
		const transactionId = crypto.randomUUID();

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			vi.spyOn(instance, "fokosNow").mockReturnValue(now);
			const store = insertStalePendingLock(state, transactionId, testCoordinatorContext(), { createdAt: now - IDEMPOTENCY_WINDOW_MS - 1 });
			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			expect(store.listPendingTxKeys(transactionId).length).toBe(1);
			expect(store.listPendingTxItems(transactionId)[0].guarded_at).toBeNull();
			releasePendingLock(store, transactionId);
			await state.storage.deleteAlarm();
		});

		expect(recoverTransaction).toHaveBeenCalledTimes(1);
		expect(consoleError.spy).toHaveBeenCalledWith(
			expect.objectContaining({ message: "fokos/partition: failed to poke stale TC", transactionId }),
		);
	});

	// The coordinator keeps the transaction and drives it itself, so the lock stays. The job moves the
	// next attempt forward before the call. Without that, the deadline stays in the past and the
	// scheduler runs the job again after each pass, with one coordinator call each time.
	it("asks a coordinator that answers driving one time, and arms the alarm at the next attempt", async () => {
		const now = Date.now();
		const { ctx, stub, rpc } = makeStub();
		await rpc.status(ctx);
		const recoverTransaction = vi.fn(async () => ({ state: "driving" as const }));
		vi.spyOn(doStubs, "txCoordinatorStubForParticipant").mockReturnValue({
			recoverTransactionForParticipant: recoverTransaction,
		} as unknown as DurableObjectStub<TransactionCoordinatorDO>);
		const transactionId = crypto.randomUUID();

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			vi.spyOn(instance, "fokosNow").mockReturnValue(now);
			const store = insertStalePendingLock(state, transactionId, testCoordinatorContext(), { createdAt: now - 10_000 });
			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			// The next pass, as the fast path of the scheduler runs it after a pass that ends with a due job.
			await instance.fokos.runDueWork();

			expect(recoverTransaction).toHaveBeenCalledTimes(1);
			expect(store.listPendingTxItems(transactionId)[0].next_recovery_at).toBe(now + DEFAULT_STALE_TRANSACTION_MS);
			expect(await state.storage.getAlarm()).toBe(now + DEFAULT_STALE_TRANSACTION_MS);
			releasePendingLock(store, transactionId);
			await state.storage.deleteAlarm();
		});
	});

	it("stops starting coordinator calls when the budget of the step ends", async () => {
		const now = Date.now();
		const { ctx, stub, rpc } = makeStub();
		await rpc.status(ctx);
		const transactionIds = [crypto.randomUUID(), crypto.randomUUID()];

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			const clock = vi.spyOn(instance, "fokosNow").mockReturnValue(now);
			// The first call takes the whole budget of the step.
			const recoverTransaction = vi.fn(async () => {
				clock.mockReturnValue(now + 10_000);
				return { state: "driving" as const };
			});
			vi.spyOn(doStubs, "txCoordinatorStubForParticipant").mockReturnValue({
				recoverTransactionForParticipant: recoverTransaction,
			} as unknown as DurableObjectStub<TransactionCoordinatorDO>);
			const store = new PartitionStore(state.storage);
			for (const [index, transactionId] of transactionIds.entries()) {
				insertStalePendingLock(state, transactionId, testCoordinatorContext(), {
					createdAt: now - 10_000 - index,
					hashKey: `budget-${index}`,
				});
			}

			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });

			expect(recoverTransaction).toHaveBeenCalledTimes(1);
			// The claim moved both transactions forward, so the one that this step did not reach waits.
			for (const transactionId of transactionIds) {
				expect(store.listPendingTxItems(transactionId)[0].next_recovery_at).toBeGreaterThan(now);
				releasePendingLock(store, transactionId);
			}
			await state.storage.deleteAlarm();
		});
	});

	it("keeps a lock whose coordinator reference it cannot read, and logs why", async () => {
		const now = Date.now();
		const { ctx, stub, rpc } = makeStub();
		await rpc.status(ctx);
		const recoverTransaction = mockCoordinatorRecovery();
		const consoleError = captureConsoleError();
		const transactionId = crypto.randomUUID();

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			vi.spyOn(instance, "fokosNow").mockReturnValue(now);
			const store = insertStalePendingLock(state, transactionId, testCoordinatorContext(), { createdAt: now - 10_000 });
			// A reference that a later version of the code wrote.
			state.storage.sql.exec(
				`UPDATE pending_tx_info SET coordinator_json = json_set(coordinator_json, '$.v', 2) WHERE transaction_id = ?`,
				transactionId,
			);
			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			expect(store.listPendingTxKeys(transactionId).length).toBe(1);
			releasePendingLock(store, transactionId);
			await state.storage.deleteAlarm();
		});

		expect(recoverTransaction).not.toHaveBeenCalled();
		expect(consoleError.spy).toHaveBeenCalledWith(
			expect.objectContaining({
				message: "fokos/partition: failed to poke stale TC",
				transactionId,
				error: expect.stringContaining("unexpected_transaction_state"),
			}),
		);
	});

	it("keeps a not_found lock whose keys all route away, and quarantines none of it", async () => {
		const now = Date.now();
		const { ctx, stub, rpc } = makeStub();
		await rpc.status(ctx);
		mockCoordinatorRecovery();
		const transactionId = crypto.randomUUID();

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			vi.spyOn(instance, "fokosNow").mockReturnValue(now);
			const store = insertStalePendingLock(state, transactionId, testCoordinatorContext(), { createdAt: now - IDEMPOTENCY_WINDOW_MS - 1 });
			// Every key of the lock now belongs to another partition, which holds the true lock. The
			// rows here are copies, and the cleanup of the migration deletes them.
			const owns = vi.spyOn(instance.fokos, "owns").mockReturnValue(false);
			const cancel = vi.spyOn(instance, "txCancel");
			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			expect(owns).toHaveBeenCalled();
			expect(cancel).not.toHaveBeenCalled();
			expect(store.listPendingTxItems(transactionId)[0]).toMatchObject({ guarded_at: null });
			releasePendingLock(store, transactionId);
			await state.storage.deleteAlarm();
		});
	});

	it("quarantined transactions do not starve a younger stale transaction", async () => {
		const now = Date.now();
		const { ctx, stub, rpc } = makeStub();
		await rpc.status(ctx);
		const recoverTransaction = mockCoordinatorRecovery();
		const consoleError = captureConsoleError();
		const transactionIds = Array.from({ length: 11 }, () => crypto.randomUUID());

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			vi.spyOn(instance, "fokosNow").mockReturnValue(now);
			const store = new PartitionStore(state.storage);
			for (const [index, transactionId] of transactionIds.entries()) {
				insertStalePendingLock(state, transactionId, testCoordinatorContext(), {
					createdAt: index < 10 ? now - IDEMPOTENCY_WINDOW_MS - 1 : now - 5_001,
					hashKey: `starvation-${index}`,
				});
			}
			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			expect(store.listPendingTxKeys(transactionIds[10]).length).toBe(1);
			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			expect(store.listPendingTxKeys(transactionIds[10]).length).toBe(0);
			for (const transactionId of transactionIds.slice(0, 10)) {
				releasePendingLock(store, transactionId);
			}
			await state.storage.deleteAlarm();
		});

		expect(recoverTransaction).toHaveBeenCalledTimes(11);
		expect(
			consoleError
				.withMessage(LOCK_AGE_GUARD_LOG)
				.filter((log) => log.transactionId !== undefined && transactionIds.includes(log.transactionId)),
		).toHaveLength(10);
	});

	it.each(["commit", "cancel"] as const)("debugForceResolveTransaction resolves a quarantined transaction with %s", async (outcome) => {
		const now = Date.now();
		const { ctx, stub, rpc } = makeStub();
		await rpc.status(ctx);
		const transactionId = crypto.randomUUID();
		const hashKey = `debug-${outcome}-${transactionId}`;
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const store = insertStalePendingLock(state, transactionId, testCoordinatorContext(), {
				createdAt: now - IDEMPOTENCY_WINDOW_MS - 1,
				hashKey,
				data: "resolved-value",
				guardedAt: now,
				transactionTimestamp: now,
			});
			expect(store.listPendingTxItems(transactionId)[0].guarded_at).toBe(now);
		});

		await expect(rpc.debugForceResolveTransaction(ctx, { transactionId, outcome })).resolves.toEqual({
			outcome: outcome === "commit" ? "committed" : "cancelled",
			resolvedLocally: 1,
			forwarded: 0,
		});
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			expect(new PartitionStore(state.storage).listPendingTxKeys(transactionId).length).toBe(0);
		});
		expect(await rpc.apiGetItem(ctx, { hashKey: kb(hashKey), sortKey: kb("sk") })).toMatchObject({
			found: outcome === "commit",
			...(outcome === "commit" ? { item: { data: "resolved-value" } } : {}),
		});
	});

	it("recovers through the stored coordinator name and commits the TTL in a stale pending row", async () => {
		const { ctx, stub, rpc } = makeStub();
		await rpc.status(ctx);
		const transactionId = crypto.randomUUID();
		const transactionTimestamp = Date.now() - 10_000;
		const ttlAt = Math.floor(Date.now() / 1000) + 3600;
		const coordinator = testCoordinatorContext();
		const tcStub = testCoordinatorStubByName(coordinator.doName);
		// A coordinator gets its identity from its first routed request, as `initiateWrite` gives it in
		// production. A coordinator with no identity answers `not_found`.
		await tcStub.recoverTransaction(coordinator, { transactionId: "no-such-transaction", idempotencyToken: `stale-${transactionId}` });

		await runInDurableObject(tcStub, async (_instance: TransactionCoordinatorDO, state: DurableObjectState) => {
			state.storage.sql.exec(
				`INSERT INTO tc_state (idempotency_token, transaction_id, state, transaction_ts, created_at, completed_at, operations_hash)
				 VALUES (?, ?, 'COMMITTED', ?, ?, ?, ?)`,
				`stale-${transactionId}`,
				transactionId,
				transactionTimestamp,
				transactionTimestamp,
				transactionTimestamp,
				"0000000000000000",
			);
		});
		const getCoordinatorStub = vi.spyOn(doStubs, "txCoordinatorStubForParticipant");
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const store = new PartitionStore(state.storage);
			store.insertPendingLock({
				hk: kb("stale-ttl"),
				sk: kb("sk"),
				transaction_id: transactionId,
				transaction_ts: transactionTimestamp,
				operation: "put",
				data: "value",
				kind: "text",
				ttl_epoch_utc_seconds: ttlAt,
				coordinator_json: JSON.stringify({ v: 1, doName: coordinator.doName, idempotencyToken: `stale-${transactionId}` }),
				created_at: transactionTimestamp,
				guarded_at: null,
				next_recovery_at: transactionTimestamp + DEFAULT_STALE_TRANSACTION_MS,
			});
			await state.storage.setAlarm(Date.now());
		});

		await runDurableObjectAlarm(stub);
		await vi.waitFor(async () => {
			// The namespace and the jurisdiction come from the partition, and only the name from the lock.
			expect(getCoordinatorStub).toHaveBeenCalledWith(
				env,
				{ nsTx: ctx.policy.nsTx, jurisdiction: ctx.topology.jurisdiction },
				coordinator.doName,
			);
			expect(await rpc.apiGetItem(ctx, { hashKey: kb("stale-ttl"), sortKey: kb("sk") })).toMatchObject({
				found: true,
				item: { data: "value", ttlAt },
			});
		});
	});
});
