import { env } from "cloudflare:workers";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { PartitionDO } from "../../src/server/do-partition.js";
import type { TransactionCoordinatorDO } from "../../src/server/do-transaction-coordinator.js";
import * as doStubs from "../../src/shared/do-stubs.js";
import { testCoordinatorStub, testPartitionStub } from "../stub-helpers.js";
import type { PartitionContextResolved } from "../../src/sharding/partition-context.js";
import { PartitionIdHelper } from "../../src/sharding/partition-id.js";
import { IDEMPOTENCY_WINDOW_MS } from "../../src/shared/transaction-limits.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { REPARTITION_KV_KEYS } from "../../src/sharding/repartition-flow.js";
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
		coordinatorDoId: string,
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
			conditions_json: null,
			ttl_epoch_utc_seconds: null,
			coordinator_do_id: coordinatorDoId,
			created_at: createdAt,
			guarded_at: options?.guardedAt ?? null,
		});
		return store;
	}

	/**
	 * Substitutes the coordinator stub with a fake that answers `not_found`.
	 *
	 * The substitution is at the `txCoordinatorStub` helper, not at the class prototype: the helper
	 * returns an RPC stub whose target runs outside this isolate, so a prototype spy records nothing.
	 * It also lets these tests store sentinel coordinator ids such as "missing-tc" in a lock row,
	 * which a real `idFromString` would reject as malformed. The partial fake is why the cast is here.
	 */
	function mockCoordinatorRecovery() {
		const recoverTransaction = vi.fn(async () => ({ state: "not_found" as const }));
		vi.spyOn(doStubs, "txCoordinatorStub").mockReturnValue({
			recoverTransaction,
		} as unknown as DurableObjectStub<TransactionCoordinatorDO>);
		return recoverTransaction;
	}

	// A source that has cut over is a router. Its targets own the keys and hold the true locks.
	it.each(["cutover", "completed"] as const)("skips stale recovery on a source in %s", async (state_) => {
		const { ctx, stub } = makeStub({ hashSplitN: 2 });
		await stub.status(ctx);
		const recoverTransaction = mockCoordinatorRecovery();
		const transactionId = crypto.randomUUID();
		const coordinatorDoId = env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString();
		const childPartitionContexts: PartitionContextResolved[] = PartitionIdHelper.calculateHashChildPartitionIds(ctx).map((child) => ({
			...ctx,
			doName: child.doName,
			partitionId: child.partitionIdOpaque,
			primaryDoIdStr: env.PARTITION_DO.idFromName(child.doName).toString(),
		}));

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			const store = new PartitionStore(state.storage);
			const now = Date.now();
			store.insertRepartition({ id: "r1", seq: 1, kind: "hash_split", state: state_, hashKey: null, queuedAt: now, nextAttemptAt: now });
			childPartitionContexts.forEach((child, index) => {
				store.insertRepartitionTarget({
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
			insertStalePendingLock(state, transactionId, coordinatorDoId);

			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: Date.now() });

			expect(recoverTransaction).not.toHaveBeenCalled();
			expect(store.pendingTxCountFor(transactionId)).toBe(1);
			expect(await state.storage.getAlarm()).toBeNull();
		});
	});

	// An incomplete target holds only some of the rows and only some of the inherited locks.
	it.each(["awaiting_data", "importing"] as const)("skips stale recovery on a target in %s", async (importState) => {
		const { ctx: parentCtx } = makeStub({ hashSplitN: 2 });
		const child = PartitionIdHelper.calculateHashChildPartitionIds(parentCtx)[0];
		const childId = env.PARTITION_DO.idFromName(child.doName);
		const childCtx: PartitionContextResolved = {
			...parentCtx,
			doName: child.doName,
			partitionId: child.partitionIdOpaque,
			primaryDoIdStr: childId.toString(),
		};
		const childStub = testPartitionStub(childId);
		await childStub.fokosInit({
			repartitionId: "r1",
			source: parentCtx,
			target: childCtx,
			slice: { kind: "hash_child", childIndex: 0, depth: 1 },
		});
		const recoverTransaction = mockCoordinatorRecovery();
		const transactionId = crypto.randomUUID();
		const coordinatorDoId = env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString();

		await runInDurableObject(childStub, async (instance: PartitionDO, state: DurableObjectState) => {
			const record = state.storage.kv.get<FokosImportRecord>(REPARTITION_KV_KEYS.IMPORT)!;
			state.storage.kv.put<FokosImportRecord>(REPARTITION_KV_KEYS.IMPORT, { ...record, state: importState });
			// This test cannot reach the source, so it stubs the import step out. The case is about
			// recovery staying away, and not about how far the import gets.
			vi.spyOn(instance as unknown as { runBackgroundWork(): Promise<void> }, "runBackgroundWork");
			const store = insertStalePendingLock(state, transactionId, coordinatorDoId);

			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: Date.now() });

			expect(recoverTransaction).not.toHaveBeenCalled();
			expect(store.pendingTxCountFor(transactionId)).toBe(1);
			state.storage.kv.put<FokosImportRecord>(REPARTITION_KV_KEYS.IMPORT, { ...record, state: "active" });
			store.deletePendingTx(transactionId);
			await state.storage.deleteAlarm();
		});
	});

	it("releases a not_found lock at the exact idempotency-window boundary", async () => {
		const now = 2_000_000_000_000;
		vi.spyOn(Date, "now").mockReturnValue(now);
		const { ctx, stub } = makeStub();
		await stub.status(ctx);
		mockCoordinatorRecovery();
		const consoleError = captureConsoleError();
		const transactionId = crypto.randomUUID();

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			const store = insertStalePendingLock(state, transactionId, "missing-tc", { createdAt: now - IDEMPOTENCY_WINDOW_MS });
			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			expect(store.pendingTxCountFor(transactionId)).toBe(0);
		});
		expect(consoleError.withMessage(LOCK_AGE_GUARD_LOG).filter((log) => log.transactionId === transactionId)).toHaveLength(0);
	});

	it("quarantines an over-age owned lock and logs the transition once", async () => {
		const now = 2_000_000_000_000;
		vi.spyOn(Date, "now").mockReturnValue(now);
		const { ctx, stub } = makeStub();
		await stub.status(ctx);
		const recoverTransaction = mockCoordinatorRecovery();
		const consoleError = captureConsoleError();
		const transactionId = crypto.randomUUID();
		const coordinatorDoId = "missing-tc";
		const hashKey = `guard-${transactionId}`;

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			const store = insertStalePendingLock(state, transactionId, coordinatorDoId, {
				createdAt: now - IDEMPOTENCY_WINDOW_MS - 1,
				hashKey,
			});
			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			expect(store.pendingTxCountFor(transactionId)).toBe(1);
			expect(store.listPendingTxItems(transactionId)[0].guarded_at).toBe(now);
			expect(await state.storage.getAlarm()).toBeNull();
		});

		expect(recoverTransaction).toHaveBeenCalledTimes(1);
		const guardLogs = consoleError.withMessage(LOCK_AGE_GUARD_LOG).filter((log) => log.transactionId === transactionId);
		expect(guardLogs).toHaveLength(1);
		expect(guardLogs[0]).toMatchObject({
			transactionId,
			coordinatorDoId,
			keys: [
				{
					hashKey: kb(hashKey).toBase64({ alphabet: "base64url" }),
					sortKey: kb("sk").toBase64({ alphabet: "base64url" }),
				},
			],
			lockCreatedAt: now - IDEMPOTENCY_WINDOW_MS - 1,
			lockAgeMs: IDEMPOTENCY_WINDOW_MS + 1,
			windowMs: IDEMPOTENCY_WINDOW_MS,
			doName: ctx.doName,
			partitionId: ctx.partitionId,
		});
	});

	it("does not quarantine or release a lock when the coordinator RPC fails", async () => {
		const now = 2_000_000_000_000;
		vi.spyOn(Date, "now").mockReturnValue(now);
		const { ctx, stub } = makeStub();
		await stub.status(ctx);
		const recoverTransaction = vi.fn(async () => {
			throw new Error("coordinator unavailable");
		});
		vi.spyOn(doStubs, "txCoordinatorStub").mockReturnValue({
			recoverTransaction,
		} as unknown as DurableObjectStub<TransactionCoordinatorDO>);
		const consoleError = captureConsoleError();
		const transactionId = crypto.randomUUID();

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			const store = insertStalePendingLock(state, transactionId, "unreachable-tc", { createdAt: now - IDEMPOTENCY_WINDOW_MS - 1 });
			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			expect(store.pendingTxCountFor(transactionId)).toBe(1);
			expect(store.listPendingTxItems(transactionId)[0].guarded_at).toBeNull();
			store.deletePendingTx(transactionId);
			await state.storage.deleteAlarm();
		});

		expect(recoverTransaction).toHaveBeenCalledTimes(1);
		expect(consoleError.spy).toHaveBeenCalledWith(
			expect.objectContaining({ message: "fokos/partition: failed to poke stale TC", transactionId }),
		);
	});

	it("deletes a not_found lock directly when all its keys route away", async () => {
		const now = 2_000_000_000_000;
		vi.spyOn(Date, "now").mockReturnValue(now);
		const { ctx, stub } = makeStub();
		await stub.status(ctx);
		mockCoordinatorRecovery();
		const transactionId = crypto.randomUUID();

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			const store = insertStalePendingLock(state, transactionId, "missing-tc", { createdAt: now - IDEMPOTENCY_WINDOW_MS - 1 });
			const routing = vi
				.spyOn(
					instance as unknown as {
						groupItemsByRouting(): { local: unknown[]; forwarded: Map<string, unknown> };
					},
					"groupItemsByRouting",
				)
				.mockReturnValue({ local: [], forwarded: new Map() });
			const cancel = vi.spyOn(instance, "txCancel");
			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			expect(routing).toHaveBeenCalled();
			expect(cancel).not.toHaveBeenCalled();
			expect(store.pendingTxCountFor(transactionId)).toBe(0);
		});
	});

	it("quarantined transactions do not starve a younger stale transaction", async () => {
		const now = 2_000_000_000_000;
		vi.spyOn(Date, "now").mockReturnValue(now);
		const { ctx, stub } = makeStub();
		await stub.status(ctx);
		const recoverTransaction = mockCoordinatorRecovery();
		const consoleError = captureConsoleError();
		const transactionIds = Array.from({ length: 11 }, () => crypto.randomUUID());

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			const store = new PartitionStore(state.storage);
			for (const [index, transactionId] of transactionIds.entries()) {
				insertStalePendingLock(state, transactionId, "missing-tc", {
					createdAt: index < 10 ? now - IDEMPOTENCY_WINDOW_MS - 1 : now - 5_001,
					hashKey: `starvation-${index}`,
				});
			}
			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			expect(store.pendingTxCountFor(transactionIds[10])).toBe(1);
			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			expect(store.pendingTxCountFor(transactionIds[10])).toBe(0);
			for (const transactionId of transactionIds.slice(0, 10)) store.deletePendingTx(transactionId);
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
		const { ctx, stub } = makeStub();
		await stub.status(ctx);
		const transactionId = crypto.randomUUID();
		const hashKey = `debug-${outcome}-${transactionId}`;
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			const store = insertStalePendingLock(state, transactionId, "missing-tc", {
				createdAt: now - IDEMPOTENCY_WINDOW_MS - 1,
				hashKey,
				data: "resolved-value",
				guardedAt: now,
				transactionTimestamp: now,
			});
			expect(store.listPendingTxItems(transactionId)[0].guarded_at).toBe(now);
		});

		await expect(stub.debugForceResolveTransaction(ctx, { transactionId, outcome })).resolves.toEqual({
			outcome: outcome === "commit" ? "committed" : "cancelled",
		});
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			expect(new PartitionStore(state.storage).pendingTxCountFor(transactionId)).toBe(0);
		});
		expect(await stub.apiGetItem(ctx, { hashKey: kb(hashKey), sortKey: kb("sk") })).toMatchObject({
			found: outcome === "commit",
			...(outcome === "commit" ? { item: { data: "resolved-value" } } : {}),
		});
	});

	it("recovers by stored coordinator ID and commits the TTL in a stale pending row", async () => {
		const { ctx, stub } = makeStub();
		await stub.status(ctx);
		const transactionId = crypto.randomUUID();
		const transactionTimestamp = Date.now() - 10_000;
		const ttlAt = Math.floor(Date.now() / 1000) + 3600;
		const tcId = env.TRANSACTION_COORDINATOR_DO.newUniqueId();
		const tcStub = testCoordinatorStub(tcId);

		await runInDurableObject(tcStub, async (_instance: TransactionCoordinatorDO, state: DurableObjectState) => {
			state.storage.sql.exec(
				`INSERT INTO tc_state (idempotency_token, transaction_id, state, transaction_ts, created_at, operations_hash)
				 VALUES (?, ?, 'COMMITTED', ?, ?, ?)`,
				`stale-${transactionId}`,
				transactionId,
				transactionTimestamp,
				transactionTimestamp,
				"0000000000000000",
			);
		});
		const getCoordinatorById = vi.spyOn(doStubs, "txCoordinatorStub");
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
				conditions_json: null,
				ttl_epoch_utc_seconds: ttlAt,
				coordinator_do_id: tcId.toString(),
				created_at: transactionTimestamp,
				guarded_at: null,
			});
			await state.storage.setAlarm(Date.now());
		});

		await runDurableObjectAlarm(stub);
		await vi.waitFor(async () => {
			expect(getCoordinatorById).toHaveBeenCalledWith(env, expect.objectContaining({ doName: ctx.doName }), tcId.toString());
			expect(await stub.apiGetItem(ctx, { hashKey: kb("stale-ttl"), sortKey: kb("sk") })).toMatchObject({
				found: true,
				item: { data: "value", ttlAt },
			});
		});
	});
});
