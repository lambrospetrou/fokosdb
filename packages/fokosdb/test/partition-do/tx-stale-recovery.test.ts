import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import { TransactionCoordinatorDO } from "../../src/server/do-transaction-coordinator.js";
import type { PartitionContextResolved } from "../../src/shared/partition-topology/partition-context.js";
import { PartitionIdHelper } from "../../src/shared/partition-topology/partition-id.js";
import type { SplitStatusKVItem } from "../../src/shared/partition-topology/split-state.js";
import { IDEMPOTENCY_WINDOW_MS } from "../../src/shared/transaction-limits.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { MIGRATION_KV_KEYS, type PartitionSplitMigrationStatus } from "../../src/shared/partition/migration.js";
import { captureConsoleError, kb, makeStub, waitForAlarm } from "./helpers.js";

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
	 * The substitution is at `TransactionCoordinatorDO.get`, not at the class prototype: `get` returns
	 * an RPC stub whose target runs outside this isolate, so a prototype spy records nothing. It also
	 * lets these tests store sentinel coordinator ids such as "missing-tc" in a lock row, which the
	 * real `get` would reject as malformed. The partial fake is why the cast is here.
	 */
	function mockCoordinatorRecovery() {
		const recoverTransaction = vi.fn(async () => ({ state: "not_found" as const }));
		vi.spyOn(TransactionCoordinatorDO, "get").mockReturnValue({
			recoverTransaction,
		} as unknown as DurableObjectStub<TransactionCoordinatorDO>);
		return recoverTransaction;
	}

	it.each(["split_started", "split_completed"] as const)("skips stale recovery on a parent in %s", async (splitStatus) => {
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
			state.storage.kv.put<SplitStatusKVItem>("__split_status", {
				status: splitStatus,
				splitType: "hash",
				createdAt: Date.now(),
				partitionContext: ctx,
				childPartitionContexts,
				migratedChildDoNames: splitStatus === "split_completed" ? childPartitionContexts.map((child) => child.doName) : [],
				history: [],
			});
			const store = insertStalePendingLock(state, transactionId, coordinatorDoId);

			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: Date.now() });

			expect(recoverTransaction).not.toHaveBeenCalled();
			expect(store.pendingTxCountFor(transactionId)).toBe(1);
			expect(await state.storage.getAlarm()).toBeNull();
		});
	});

	it.each(["migration_initialized", "migration_migrating"] as const)("skips stale recovery on a child in %s", async (migrationStatus) => {
		const { ctx: parentCtx } = makeStub({ hashSplitN: 2 });
		const child = PartitionIdHelper.calculateHashChildPartitionIds(parentCtx)[0];
		const childId = env.PARTITION_DO.idFromName(child.doName);
		const childCtx: PartitionContextResolved = {
			...parentCtx,
			doName: child.doName,
			partitionId: child.partitionIdOpaque,
			primaryDoIdStr: childId.toString(),
		};
		const childStub = PartitionDO.get(env.PARTITION_DO, childId);
		await childStub.internalInitFromSplit({ parentPartitionContext: parentCtx, newPartitionContext: childCtx, splitType: "hash" });
		const recoverTransaction = mockCoordinatorRecovery();
		const transactionId = crypto.randomUUID();
		const coordinatorDoId = env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString();

		await runInDurableObject(childStub, async (instance: PartitionDO, state: DurableObjectState) => {
			state.storage.kv.put<PartitionSplitMigrationStatus>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_STATUS, migrationStatus);
			const migration = vi.spyOn(instance as unknown as { runMigration(): Promise<void> }, "runMigration").mockResolvedValue();
			const store = insertStalePendingLock(state, transactionId, coordinatorDoId);

			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: Date.now() });

			expect(recoverTransaction).not.toHaveBeenCalled();
			expect(store.pendingTxCountFor(transactionId)).toBe(1);
			state.storage.kv.put<PartitionSplitMigrationStatus>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_completed");
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
		vi.spyOn(TransactionCoordinatorDO, "get").mockReturnValue({
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
		const tcStub = TransactionCoordinatorDO.get(env.TRANSACTION_COORDINATOR_DO, tcId);

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

		const getCoordinatorById = vi.spyOn(TransactionCoordinatorDO, "get");
		await waitForAlarm(stub);
		expect(getCoordinatorById).toHaveBeenCalledWith(env.TRANSACTION_COORDINATOR_DO, tcId.toString());
		expect(await stub.apiGetItem(ctx, { hashKey: kb("stale-ttl"), sortKey: kb("sk") })).toMatchObject({
			found: true,
			item: { data: "value", ttlAt },
		});
	});
});
