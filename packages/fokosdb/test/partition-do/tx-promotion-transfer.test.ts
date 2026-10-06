/**
 * A promotion source keeps the lock rows of the key it moved until the source cleanup after the
 * completion deletes them, and it makes no local decision about them.
 *
 * Most tests build the state that a cutover leaves, and do not run a cutover: a `key_promotion` row
 * in `cutover` with its route override, and lock rows under the moved key. The repartition row is
 * parked far in the future, so no scheduler pass advances it. The tests at the end of the file run a
 * real promotion or a real hash split.
 */
import { env, runInDurableObject } from "cloudflare:test";
import { hashChildIndex } from "../../src/sharding/hash-primitives.js";
import { afterEach, describe, expect, it, vi } from "vitest";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import type { PartitionDO } from "../../src/server/do-partition.js";
import { testCoordinatorContext, testCoordinatorRef } from "../stub-helpers.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { FokosShardingStore } from "../../src/sharding/sharding-store.js";
import { DEFAULT_STALE_TRANSACTION_MS, IDEMPOTENCY_WINDOW_MS, MAX_ITEMS_PER_TX } from "../../src/shared/transaction-limits.js";
import { captureConsoleError, kb, lockKeys, makeStub, withOpIndex } from "./helpers.js";
import {
	CONTROLLED_NS,
	drainUntil,
	keepTestLocks,
	makePartition,
	PROMOTION_TEST_MAX_SIZE_MB,
	type TestPartition,
} from "./partition-harness.js";

const LOCK_AGE_GUARD_LOG = "fokos/partition: lock-age guard: over-age lock with not_found";
const REPAIR_FAILED_LOG = "fokos/partition: forced resolution failed";
const PARKED_MS = 3_600_000;

/** Records the promotion of `hashKey` as a durable cutover, with no step due. */
function cutOverPromotion(state: DurableObjectState, hashKey: string | null): void {
	const sharding = new FokosShardingStore(state.storage);
	const now = Date.now();
	const seq = sharding.nextRepartitionSeq();
	const id = `r${seq}`;
	sharding.insertRepartition({
		id,
		seq,
		kind: "key_promotion",
		state: "cutover",
		hashKey: hashKey === null ? null : kb(hashKey),
		queuedAt: now,
		cutoverAt: now,
		nextAttemptAt: now + PARKED_MS,
	});
	if (hashKey !== null) {
		sharding.insertRouteOverride(kb(hashKey), id);
	}
}

/** Writes the lock row a prepare left before the cutover of its key. */
function insertLock(
	state: DurableObjectState,
	transactionId: string,
	hashKey: string,
	options?: { sortKey?: string; createdAt?: number; data?: string; guardedAt?: number | null; coordinatorDoName?: string },
): PartitionStore {
	const createdAt = options?.createdAt ?? Date.now() - 10_000;
	const store = new PartitionStore(state.storage);
	store.insertPendingLock({
		hk: kb(hashKey),
		sk: kb(options?.sortKey ?? "sk"),
		transaction_id: transactionId,
		transaction_ts: createdAt,
		operation: "put",
		op_list: [[0, "put"] as [number, "put"]],
		data: options?.data ?? "moved-value",
		kind: "text",
		ttl_epoch_utc_seconds: null,
		coordinator_json: JSON.stringify({
			v: 1,
			doName: options?.coordinatorDoName ?? testCoordinatorContext().doName,
			idempotencyToken: `token-${transactionId}`,
		}),
		created_at: createdAt,
		guarded_at: options?.guardedAt ?? null,
		next_recovery_at: createdAt + DEFAULT_STALE_TRANSACTION_MS,
	});
	return store;
}

function prepareOf(transactionId: string, keys: { hashKey: string; sortKey: string }[], data = "v1") {
	return {
		transactionId,
		transactionTimestamp: Date.now(),
		coordinator: testCoordinatorRef(),
		items: withOpIndex(
			keys.map((key) => ({ hashKey: kb(key.hashKey), sortKey: kb(key.sortKey), operation: "put" as const, data, kind: "text" as const })),
		),
	};
}

/** Drives the source cleanup until `partition` holds no lock row of `transactionId`. */
async function awaitNoCopies(partition: TestPartition, transactionId: string): Promise<void> {
	await drainUntil(
		[partition],
		async () => (await lockKeys(partition.stub, transactionId)).length === 0,
		`${transactionId} copies deleted`,
	);
}

describe("PartitionDO — a promotion source and the locks of the key it moved", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	it("commits its owned rows and keeps the copies of the moved key", async () => {
		const { ctx, stub, rpc } = makeStub();
		const transactionId = crypto.randomUUID();
		const prepare = prepareOf(transactionId, [{ hashKey: "bob", sortKey: "sk" }], "bob-v1");
		expect(await rpc.txPrepare(ctx, prepare)).toMatchObject({ outcome: "accepted" });
		await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) => {
			insertLock(state, transactionId, "alice");
			cutOverPromotion(state, "alice");
		});

		await expect(
			rpc.txCommit(ctx, {
				transactionId,
				transactionTimestamp: prepare.transactionTimestamp,
				items: [{ hashKey: kb("bob"), sortKey: kb("sk") }],
			}),
		).resolves.toEqual({ outcome: "committed" });

		expect(await lockKeys(stub, transactionId)).toEqual(["alice/sk"]);
		expect(await rpc.apiGetItem(ctx, { hashKey: kb("bob"), sortKey: kb("sk") })).toMatchObject({
			found: true,
			item: { data: "bob-v1", version: 1 },
		});
	});

	it("answers committed and applies nothing when only the copies remain", async () => {
		const { ctx, stub, rpc } = makeStub();
		const transactionId = crypto.randomUUID();
		await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) => {
			insertLock(state, transactionId, "alice");
			cutOverPromotion(state, "alice");
		});

		// The retry of a commit whose owned part is already applied. The copies are not local work.
		await expect(
			rpc.txCommit(ctx, { transactionId, transactionTimestamp: Date.now(), items: [{ hashKey: kb("bob"), sortKey: kb("sk") }] }),
		).resolves.toEqual({ outcome: "committed" });

		expect(await lockKeys(stub, transactionId)).toEqual(["alice/sk"]);
		// The payload of the copy belongs to the target, which applies it. A read of the key goes
		// to the range root, so the source storage answers this.
		await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) => {
			expect(state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM items WHERE hk = ?`, kb("alice")).one().n).toBe(0);
		});
	});

	it("still reports a commit request that does not match its owned rows", async () => {
		const { ctx, stub, rpc } = makeStub();
		const transactionId = crypto.randomUUID();
		const prepared = await rpc.txPrepare(
			ctx,
			prepareOf(transactionId, [
				{ hashKey: "bob", sortKey: "sk" },
				{ hashKey: "carol", sortKey: "sk" },
			]),
		);
		expect(prepared).toMatchObject({ outcome: "accepted" });
		await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) => {
			insertLock(state, transactionId, "alice");
			cutOverPromotion(state, "alice");
		});

		await expect(
			rpc.txCommit(ctx, { transactionId, transactionTimestamp: Date.now(), items: [{ hashKey: kb("bob"), sortKey: kb("sk") }] }),
		).rejects.toThrow(/commit_keyset_mismatch/);
		expect(await lockKeys(stub, transactionId)).toHaveLength(3);
	});

	it("releases nothing for a cancel with no key", async () => {
		const { ctx, stub, rpc } = makeStub();
		const transactionId = crypto.randomUUID();
		expect(await rpc.txPrepare(ctx, prepareOf(transactionId, [{ hashKey: "bob", sortKey: "sk" }]))).toMatchObject({ outcome: "accepted" });
		await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) => {
			insertLock(state, transactionId, "alice");
			cutOverPromotion(state, "alice");
		});

		await expect(rpc.txCancel(ctx, { transactionId, items: [] })).resolves.toEqual({ outcome: "cancelled" });

		expect(await lockKeys(stub, transactionId)).toEqual(["alice/sk", "bob/sk"]);
	});

	it("releases the rows of the keys a cancel names and keeps the copies", async () => {
		const { ctx, stub, rpc } = makeStub();
		const transactionId = crypto.randomUUID();
		expect(await rpc.txPrepare(ctx, prepareOf(transactionId, [{ hashKey: "bob", sortKey: "sk" }]))).toMatchObject({ outcome: "accepted" });
		await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) => {
			insertLock(state, transactionId, "alice");
			cutOverPromotion(state, "alice");
		});

		await expect(rpc.txCancel(ctx, { transactionId, items: [{ hashKey: kb("bob"), sortKey: kb("sk") }] })).resolves.toEqual({
			outcome: "cancelled",
		});

		expect(await lockKeys(stub, transactionId)).toEqual(["alice/sk"]);
	});

	it("commits a transaction of MAX_ITEMS_PER_TX owned keys next to a copy", async () => {
		const { ctx, stub, rpc } = makeStub();
		const transactionId = crypto.randomUUID();
		const keys = Array.from({ length: MAX_ITEMS_PER_TX }, (_, i) => ({ hashKey: "bob", sortKey: `sk${String(i).padStart(3, "0")}` }));
		const prepare = prepareOf(transactionId, keys, "many");
		expect(await rpc.txPrepare(ctx, prepare)).toMatchObject({ outcome: "accepted" });
		await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) => {
			insertLock(state, transactionId, "alice");
			cutOverPromotion(state, "alice");
		});

		// One statement for all keys would bind 200 parameters, over the limit of a Durable Object.
		await expect(
			rpc.txCommit(ctx, {
				transactionId,
				transactionTimestamp: prepare.transactionTimestamp,
				items: keys.map((key) => ({ hashKey: kb(key.hashKey), sortKey: kb(key.sortKey) })),
			}),
		).resolves.toEqual({ outcome: "committed" });

		expect(await lockKeys(stub, transactionId)).toEqual(["alice/sk"]);
	});
});

describe("PartitionDO — emergency repair on a promotion source", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	it("resolves its owned rows, forwards the copies, and keeps their quarantine", async () => {
		const now = Date.now();
		const { ctx, stub, rpc } = makeStub();
		const transactionId = crypto.randomUUID();
		await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) => {
			insertLock(state, transactionId, "bob", { guardedAt: now, data: "bob-v1" });
			insertLock(state, transactionId, "alice", { guardedAt: now });
			cutOverPromotion(state, "alice");
		});
		const consoleError = captureConsoleError();

		// The range root of the moved key is not initialized, so the forwarded key fails the call. The
		// local part ran first: the operator repeats the call once the target is ready.
		await expect(rpc.debugForceResolveTransaction(ctx, { transactionId, outcome: "commit" })).rejects.toThrow(/partition_fanout_failed/);

		expect(await lockKeys(stub, transactionId)).toEqual(["alice/sk"]);
		await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) => {
			// The copy keeps its quarantine: only its owner can end it.
			expect(new PartitionStore(state.storage).listPendingTxItems(transactionId)[0].guarded_at).toBe(now);
		});
		expect(await rpc.apiGetItem(ctx, { hashKey: kb("bob"), sortKey: kb("sk") })).toMatchObject({ found: true });

		const failures = consoleError
			.withMessage("fokos/partition: forced resolution failed")
			.filter((log) => log.transactionId === transactionId);
		expect(failures).toHaveLength(1);
		expect(failures[0]).toMatchObject({ outcome: "commit", resolvedLocally: 1, forwarded: 1 });
	});

	it("counts what it resolved and logs one line for a call with no forward", async () => {
		const { ctx, stub, rpc } = makeStub();
		const transactionId = crypto.randomUUID();
		await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) => {
			insertLock(state, transactionId, "bob", { guardedAt: Date.now() });
		});
		const logged = vi.spyOn(console, "info").mockImplementation(() => {});

		await expect(rpc.debugForceResolveTransaction(ctx, { transactionId, outcome: "cancel" })).resolves.toEqual({
			outcome: "cancelled",
			resolvedLocally: 1,
			forwarded: 0,
		});
		// A repeat of the call finds nothing of the transaction and writes no row.
		await expect(rpc.debugForceResolveTransaction(ctx, { transactionId, outcome: "cancel" })).resolves.toEqual({
			outcome: "cancelled",
			resolvedLocally: 0,
			forwarded: 0,
		});

		const applied = logged.mock.calls
			.map(([entry]) => entry as { message?: string; transactionId?: string })
			.filter((log) => log.message === "fokos/partition: forced resolution applied" && log.transactionId === transactionId);
		expect(applied).toHaveLength(2);
	});
});

describe("PartitionDO — stale recovery on a promotion source", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	/**
	 * A partition that calls a coordinator with test controls, and the coordinator of its locks. The
	 * coordinator holds no record, so it answers `not_found`: the answer that can delete or quarantine
	 * a row. Pass `coordinatorDoName` to `insertLock`. A stub works only in the context that made it,
	 * thus `coordinator()` makes a new stub for each context: the test, or a `runInDurableObject` callback.
	 */
	async function partitionWithCoordinator() {
		const partition = makeStub({ nsTx: "CONTROLLED_TRANSACTION_COORDINATOR_DO" });
		await partition.rpc.status(partition.ctx);
		const coordinatorDoName = testCoordinatorContext().doName;
		const coordinator = () => env.CONTROLLED_TRANSACTION_COORDINATOR_DO.getByName(coordinatorDoName);
		return { ...partition, coordinator, coordinatorDoName };
	}

	it("reads the owner again after the coordinator answers, and changes no row that moved", async () => {
		const now = Date.now();
		const { stub, coordinator, coordinatorDoName } = await partitionWithCoordinator();
		const transactionId = crypto.randomUUID();
		await coordinator().testRecoverResponse({ value: { state: "COMMITTED" }, hold: true });

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			vi.spyOn(instance, "fokosNow").mockReturnValue(now);
			const store = insertLock(state, transactionId, "alice", { createdAt: now - IDEMPOTENCY_WINDOW_MS - 1, coordinatorDoName });
			const dispatch = vi.spyOn(instance.fokos, "dispatch");

			// The promotion of the key cuts over while the coordinator holds its answer.
			const pass = instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			try {
				await vi.waitFor(async () => expect(await coordinator().testRecoverParked()).toBe(true), { timeout: 5000, interval: 10 });
				cutOverPromotion(state, "alice");
			} finally {
				await coordinator().testReleaseRecover();
			}
			await pass;

			// The row is a copy now. The new owner commits its own row after its import.
			expect(dispatch).not.toHaveBeenCalled();
			expect(store.listPendingTxItems(transactionId)[0]).toMatchObject({ guarded_at: null, data: "moved-value" });
			expect(store.getItem(kb("alice"), kb("sk")).row).toBeUndefined();
		});
	});

	it("leaves an over-age copy alone and sets no alarm in the past", async () => {
		const now = Date.now();
		const { stub, coordinator, coordinatorDoName } = await partitionWithCoordinator();
		const transactionId = crypto.randomUUID();

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			vi.spyOn(instance, "fokosNow").mockReturnValue(now);
			const store = insertLock(state, transactionId, "alice", { createdAt: now - IDEMPOTENCY_WINDOW_MS - 1, coordinatorDoName });
			cutOverPromotion(state, "alice");

			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });

			// The copy carries its payload to the target, which resolves it after its import.
			expect(await coordinator().testRecoverCalls()).toBe(0);
			expect(store.listPendingTxItems(transactionId)[0]).toMatchObject({ guarded_at: null, data: "moved-value" });
			const alarm = await state.storage.getAlarm();
			expect(alarm === null || alarm > now).toBe(true);
		});
	});

	// The guard is a fact of the transaction, so it also covers the copy. The migration carries it to
	// the new owner of the moved key.
	it("quarantines a transaction with an owned row, and the guard covers its copies", async () => {
		const now = Date.now();
		const { stub, coordinator, coordinatorDoName } = await partitionWithCoordinator();
		const consoleError = captureConsoleError();
		const transactionId = crypto.randomUUID();

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			vi.spyOn(instance, "fokosNow").mockReturnValue(now);
			const store = insertLock(state, transactionId, "bob", { createdAt: now - IDEMPOTENCY_WINDOW_MS - 1, coordinatorDoName });
			insertLock(state, transactionId, "alice", { createdAt: now - IDEMPOTENCY_WINDOW_MS - 1, coordinatorDoName });
			cutOverPromotion(state, "alice");

			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });
			expect(await coordinator().testRecoverCalls()).toBe(1);

			const rows = store.listPendingTxItems(transactionId);
			expect(rows.map((row) => row.guarded_at)).toEqual([now, now]);
		});

		const guardLogs = consoleError.withMessage(LOCK_AGE_GUARD_LOG).filter((log) => log.transactionId === transactionId);
		expect(guardLogs).toHaveLength(1);
		expect(guardLogs[0]).toMatchObject({
			keys: [KeyCodec.pairForLog(kb("bob"), kb("sk"))],
		});
	});
});

/** Prepares one transaction on `keys` at `partition`, and returns its id. */
async function prepareOn(partition: TestPartition, keys: { hashKey: string; sortKey: string }[]): Promise<string> {
	const transactionId = crypto.randomUUID();
	expect(await partition.rpc.txPrepare(partition.ctx, prepareOf(transactionId, keys, `v-${keys[0].sortKey}`))).toMatchObject({
		outcome: "accepted",
	});
	return transactionId;
}

/** Quarantines every row of the transactions at `partition`, as the stale job does for an over-age lock. */
async function quarantine(partition: TestPartition, guardedAt: number, ...transactionIds: string[]): Promise<void> {
	await runInDurableObject(partition.stub, (_instance: PartitionDO, state: DurableObjectState) => {
		const store = new PartitionStore(state.storage);
		for (const transactionId of transactionIds) {
			expect(store.guardPendingTx(transactionId, guardedAt)).toBe(true);
		}
	});
}

/** The `guarded_at` of each lock row of one transaction at `partition`. */
async function guardsOf(partition: TestPartition, transactionId: string): Promise<(number | null)[]> {
	return await runInDurableObject(partition.stub, (_instance: PartitionDO, state: DurableObjectState) =>
		new PartitionStore(state.storage).listPendingTxItems(transactionId).map((row) => row.guarded_at),
	);
}

async function forceCommit(partition: TestPartition, transactionId: string) {
	return await partition.rpc.debugForceResolveTransaction(partition.ctx, { transactionId, outcome: "commit" });
}

describe("PartitionDO — the locks of a key that a real promotion moves", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	/**
	 * Starts the promotion of "alice" and stops it after the import of the range root: the source
	 * refuses each acknowledgement. The range root retries at once, so the release of the refusal
	 * completes the promotion without a long wait.
	 */
	async function promoteAndHoldAck(partition: TestPartition): Promise<TestPartition> {
		const rangeRoot = partition.rangeRoot("alice");
		await rangeRoot.controlled.testRuntimeConfig({ importRetryBaseMs: 1, importRetryMaxMs: 1 });
		await partition.controlled.testRefuseAcks(true);
		await partition.triggerPromotion("alice");
		await partition.awaitPromotedKeyStatus("alice", ["promoting"]);
		await rangeRoot.awaitMigrationCompleted();
		expect(await partition.promotedKeyStatus("alice")).toBe("promoting");
		return rangeRoot;
	}

	it("commits both parts of a transaction after the import and before the completion", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		const keys = [
			{ hashKey: "alice", sortKey: "sk1" },
			{ hashKey: "bob", sortKey: "sk1" },
		];
		const prepare = prepareOf(crypto.randomUUID(), keys, "v-sk1");
		expect(await partition.rpc.txPrepare(partition.ctx, prepare)).toMatchObject({ outcome: "accepted" });
		const { transactionId } = prepare;
		const commit = {
			transactionId,
			transactionTimestamp: prepare.transactionTimestamp,
			items: keys.map((key) => ({ hashKey: kb(key.hashKey), sortKey: kb(key.sortKey) })),
		};
		try {
			const rangeRoot = await promoteAndHoldAck(partition);

			// The source commits "bob" and sends "alice" to the range root, which commits its imported row.
			await expect(partition.rpc.txCommit(partition.ctx, commit)).resolves.toEqual({ outcome: "committed" });
			expect(await partition.get({ hashKey: kb("bob"), sortKey: kb("sk1") })).toMatchObject({ found: true, item: { data: "v-sk1" } });
			expect(await rangeRoot.get({ hashKey: kb("alice"), sortKey: kb("sk1") })).toMatchObject({ found: true, item: { data: "v-sk1" } });
			expect(await lockKeys(rangeRoot.stub, transactionId)).toEqual([]);
			// The source keeps the copy until the acknowledgement, and a retry of the commit applies
			// nothing a second time.
			expect(await lockKeys(partition.stub, transactionId)).toEqual(["alice/sk1"]);
			await expect(partition.rpc.txCommit(partition.ctx, commit)).resolves.toEqual({ outcome: "committed" });
			expect(await partition.get({ hashKey: kb("bob"), sortKey: kb("sk1") })).toMatchObject({ item: { version: 1 } });

			await partition.controlled.testRefuseAcks(false);
			await partition.awaitPromoted("alice");
			await awaitNoCopies(partition, transactionId);
		} finally {
			await partition.controlled.testRefuseAcks(false);
		}
	});

	it("repairs a moved lock during the import, after the import, and on the range root after the completion", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		const rangeRoot = partition.rangeRoot("alice");
		const spanning = await prepareOn(partition, [
			{ hashKey: "alice", sortKey: "sk1" },
			{ hashKey: "bob", sortKey: "sk1" },
		]);
		const moved = await prepareOn(partition, [{ hashKey: "alice", sortKey: "sk2" }]);
		await quarantine(partition, 77, spanning, moved);
		const consoleError = captureConsoleError();

		await partition.controlled.testHoldPulls({ stream: "items", target: rangeRoot.doName });
		try {
			await partition.triggerPromotion("alice");
			await partition.awaitPromotedKeyStatus("alice", ["promoting"]);

			// The range root imports, so the call fails after the source commits its own row.
			await expect(forceCommit(partition, spanning)).rejects.toThrow(/partition_fanout_failed/);
			await expect(forceCommit(partition, moved)).rejects.toThrow(/partition_fanout_failed/);
			expect(consoleError.withMessage(REPAIR_FAILED_LOG)).toEqual([
				expect.objectContaining({ transactionId: spanning, resolvedLocally: 1, forwarded: 1, causeCode: "partition_migrating" }),
				expect.objectContaining({ transactionId: moved, resolvedLocally: 0, forwarded: 1, causeCode: "partition_migrating" }),
			]);
			expect(await partition.get({ hashKey: kb("bob"), sortKey: kb("sk1") })).toMatchObject({ found: true });
			expect(await lockKeys(partition.stub, spanning)).toEqual(["alice/sk1"]);
			expect(await lockKeys(partition.stub, moved)).toEqual(["alice/sk2"]);
			// The range root refuses a repair while it imports, because it does not hold all its rows.
			await expect(forceCommit(rangeRoot, moved)).rejects.toThrow(/partition_migrating/);
		} finally {
			await partition.controlled.testReleasePulls();
		}

		try {
			// The source refuses the acknowledgement, so the copies stay after the import.
			await partition.controlled.testRefuseAcks(true);
			await rangeRoot.controlled.testRuntimeConfig({ importRetryBaseMs: 1, importRetryMaxMs: 1 });
			await rangeRoot.awaitMigrationCompleted();
			expect(await partition.promotedKeyStatus("alice")).toBe("promoting");

			// A repeat on the source sends the copy to the range root, which commits its row. The copy
			// keeps its quarantine, because only its owner can end it.
			await expect(forceCommit(partition, spanning)).resolves.toEqual({ outcome: "committed", resolvedLocally: 0, forwarded: 1 });
			expect(await lockKeys(rangeRoot.stub, spanning)).toEqual([]);
			expect(await rangeRoot.get({ hashKey: kb("alice"), sortKey: kb("sk1") })).toMatchObject({ found: true });
			expect(await guardsOf(partition, spanning)).toEqual([77]);

			await partition.controlled.testRefuseAcks(false);
			await partition.awaitPromoted("alice");
		} finally {
			await partition.controlled.testRefuseAcks(false);
		}

		// After the cleanup the source holds no copy. The range root resolves the rows it owns.
		await awaitNoCopies(partition, moved);
		await expect(forceCommit(partition, moved)).resolves.toEqual({ outcome: "committed", resolvedLocally: 0, forwarded: 0 });
		expect(await guardsOf(rangeRoot, moved)).toEqual([77]);
		await expect(forceCommit(rangeRoot, moved)).resolves.toEqual({ outcome: "committed", resolvedLocally: 1, forwarded: 0 });
		expect(await rangeRoot.get({ hashKey: kb("alice"), sortKey: kb("sk2") })).toMatchObject({ found: true, item: { data: "v-sk2" } });
	});
});

describe("PartitionDO — the locks on a hash split router", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	/** A hash key that the hash child at `childIndex` of a root owns. */
	function keyForChild(partition: TestPartition, childIndex: number): string {
		for (let i = 0; i < 10_000; i++) {
			const key = `k-${i}`;
			if (hashChildIndex(kb(key), 0, partition.ctx.topology.hashSplitN) === childIndex) {
				return key;
			}
		}
		throw new Error(`no key for child ${childIndex}`);
	}

	it("keeps every copy until the last acknowledgement, and each child resolves its own rows", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		const [first, second] = [keyForChild(partition, 0), keyForChild(partition, 1)];
		const [firstChild, secondChild] = partition.hashChildren();
		await keepTestLocks(partition, firstChild, secondChild);
		const transactionId = await prepareOn(partition, [
			{ hashKey: first, sortKey: "sk" },
			{ hashKey: second, sortKey: "sk" },
		]);
		const allKeys = [`${first}/sk`, `${second}/sk`].sort();
		const consoleError = captureConsoleError();

		await partition.controlled.testHoldPulls({ stream: "items", target: secondChild.doName });
		try {
			await partition.triggerHashSplit();
			await partition.awaitSplitStarted();
			await firstChild.awaitMigrationCompleted();

			// A cancel with no key releases nothing on the router.
			await expect(partition.rpc.txCancel(partition.ctx, { transactionId, items: [] })).resolves.toEqual({ outcome: "cancelled" });
			expect((await lockKeys(partition.stub, transactionId)).sort()).toEqual(allKeys);

			// The router sends each row to its child and changes no row of its own. The second child
			// imports, so the call fails.
			await expect(partition.rpc.debugForceResolveTransaction(partition.ctx, { transactionId, outcome: "cancel" })).rejects.toThrow(
				/partition_fanout_failed/,
			);
			expect(consoleError.withMessage(REPAIR_FAILED_LOG)).toEqual([
				expect.objectContaining({ transactionId, resolvedLocally: 0, forwarded: 2, causeCode: "partition_migrating" }),
			]);
			expect(await lockKeys(firstChild.stub, transactionId)).toEqual([]);
			expect((await lockKeys(partition.stub, transactionId)).sort()).toEqual(allKeys);
		} finally {
			await partition.controlled.testReleasePulls();
		}

		await partition.awaitSplitCompleted();
		// The cleanup deletes the copies of the router. The second child keeps its lock.
		await awaitNoCopies(partition, transactionId);
		expect(await lockKeys(secondChild.stub, transactionId)).toEqual([`${second}/sk`]);
		await expect(partition.rpc.debugForceResolveTransaction(partition.ctx, { transactionId, outcome: "cancel" })).resolves.toEqual({
			outcome: "cancelled",
			resolvedLocally: 0,
			forwarded: 0,
		});
		await expect(secondChild.rpc.debugForceResolveTransaction(secondChild.ctx, { transactionId, outcome: "cancel" })).resolves.toEqual({
			outcome: "cancelled",
			resolvedLocally: 1,
			forwarded: 0,
		});
		expect(await lockKeys(secondChild.stub, transactionId)).toEqual([]);
	});
});
