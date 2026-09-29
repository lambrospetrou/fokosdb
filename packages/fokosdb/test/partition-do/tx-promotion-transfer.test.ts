/**
 * A promotion source keeps the lock rows of the key it moved until the completion transaction
 * deletes them, and it makes no local decision about them.
 *
 * Each test builds the state a cutover leaves, without running one: a `key_promotion` row in
 * `cutover` with its route override, and lock rows under the moved key. The repartition row is
 * parked far in the future, so no scheduler pass advances it. `promotion.test.ts` drives the same
 * transfer through a real cutover.
 */
import { runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { PartitionDO } from "../../src/server/do-partition.js";
import type { TransactionCoordinatorDO } from "../../src/server/do-transaction-coordinator.js";
import * as doStubs from "../../src/shared/do-stubs.js";
import { testCoordinatorContext, testCoordinatorRef } from "../stub-helpers.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { FokosShardingStore } from "../../src/sharding/sharding-store.js";
import { IDEMPOTENCY_WINDOW_MS, MAX_ITEMS_PER_TX } from "../../src/shared/transaction-limits.js";
import { captureConsoleError, kb, lockKeys, makeStub, withOpIndex } from "./helpers.js";

const LOCK_AGE_GUARD_LOG = "fokos/partition: lock-age guard: over-age lock with not_found";
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
	options?: { sortKey?: string; createdAt?: number; data?: string; guardedAt?: number | null },
): PartitionStore {
	const createdAt = options?.createdAt ?? Date.now() - 10_000;
	const store = new PartitionStore(state.storage);
	store.insertPendingLock({
		hk: kb(hashKey),
		sk: kb(options?.sortKey ?? "sk"),
		transaction_id: transactionId,
		transaction_ts: createdAt,
		operation: "put",
		data: options?.data ?? "moved-value",
		kind: "text",
		conditions_json: null,
		ttl_epoch_utc_seconds: null,
		coordinator_json: JSON.stringify({ v: 1, doName: testCoordinatorContext().doName, idempotencyToken: `token-${transactionId}` }),
		created_at: createdAt,
		guarded_at: options?.guardedAt ?? null,
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

describe("PartitionDO — a promotion source and the locks of the key it moved", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	it("commits its owned rows and keeps the copies of the moved key", async () => {
		const { ctx, stub, rpc } = makeStub();
		const transactionId = crypto.randomUUID();
		expect((await rpc.txPrepare(ctx, prepareOf(transactionId, [{ hashKey: "bob", sortKey: "sk" }], "bob-v1"))).outcome).toBe("accepted");
		await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) => {
			insertLock(state, transactionId, "alice");
			cutOverPromotion(state, "alice");
			// A promotion of another partition's key leaves a row with no hash key. It must not make
			// the `NOT IN` predicate NULL for every row.
			cutOverPromotion(state, null);
		});

		await expect(
			rpc.txCommit(ctx, { transactionId, transactionTimestamp: Date.now(), items: [{ hashKey: kb("bob"), sortKey: kb("sk") }] }),
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
		expect(prepared.outcome).toBe("accepted");
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
		expect((await rpc.txPrepare(ctx, prepareOf(transactionId, [{ hashKey: "bob", sortKey: "sk" }]))).outcome).toBe("accepted");
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
		expect((await rpc.txPrepare(ctx, prepareOf(transactionId, [{ hashKey: "bob", sortKey: "sk" }]))).outcome).toBe("accepted");
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
		expect((await rpc.txPrepare(ctx, prepareOf(transactionId, keys, "many"))).outcome).toBe("accepted");
		await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) => {
			insertLock(state, transactionId, "alice");
			cutOverPromotion(state, "alice");
		});

		// One statement per key would bind 200 parameters, over the limit of a Durable Object.
		await expect(
			rpc.txCommit(ctx, {
				transactionId,
				transactionTimestamp: Date.now(),
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

		// The range root of the moved key has no data yet, so the forwarded key fails the call. The
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

	/** A coordinator that has forgotten the transaction: the answer that can delete or quarantine a row. */
	function mockNotFound() {
		const recoverTransaction = vi.fn(async () => ({ state: "not_found" as const }));
		vi.spyOn(doStubs, "txCoordinatorStubForParticipant").mockReturnValue({
			recoverTransactionForParticipant: recoverTransaction,
		} as unknown as DurableObjectStub<TransactionCoordinatorDO>);
		return recoverTransaction;
	}

	it("leaves an over-age copy alone and sets no alarm in the past", async () => {
		const now = Date.now();
		const { ctx, stub, rpc } = makeStub();
		await rpc.status(ctx);
		const recoverTransaction = mockNotFound();
		const transactionId = crypto.randomUUID();

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			vi.spyOn(instance, "fokosNow").mockReturnValue(now);
			const store = insertLock(state, transactionId, "alice", { createdAt: now - IDEMPOTENCY_WINDOW_MS - 1 });
			cutOverPromotion(state, "alice");

			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });

			// The copy carries its payload to the target, which resolves it after its import.
			expect(recoverTransaction).not.toHaveBeenCalled();
			expect(store.listPendingTxItems(transactionId)[0]).toMatchObject({ guarded_at: null, data: "moved-value" });
			const alarm = await state.storage.getAlarm();
			expect(alarm === null || alarm > now).toBe(true);
		});
	});

	it("quarantines the owned rows of a transaction and leaves its copies unguarded", async () => {
		const now = Date.now();
		const { ctx, stub, rpc } = makeStub();
		await rpc.status(ctx);
		const consoleError = captureConsoleError();
		mockNotFound();
		const transactionId = crypto.randomUUID();

		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			vi.spyOn(instance, "fokosNow").mockReturnValue(now);
			const store = insertLock(state, transactionId, "bob", { createdAt: now - IDEMPOTENCY_WINDOW_MS - 1 });
			insertLock(state, transactionId, "alice", { createdAt: now - IDEMPOTENCY_WINDOW_MS - 1 });
			cutOverPromotion(state, "alice");

			await instance.alarm({ isRetry: false, retryCount: 0, scheduledTime: now });

			const rows = store.listPendingTxItems(transactionId);
			expect(rows.find((row) => new TextDecoder().decode(row.hk) === "bob")?.guarded_at).toBe(now);
			expect(rows.find((row) => new TextDecoder().decode(row.hk) === "alice")?.guarded_at).toBeNull();
		});

		const guardLogs = consoleError.withMessage(LOCK_AGE_GUARD_LOG).filter((log) => log.transactionId === transactionId);
		expect(guardLogs).toHaveLength(1);
		expect(guardLogs[0]).toMatchObject({
			keys: [{ hashKey: kb("bob").toBase64({ alphabet: "base64url" }), sortKey: kb("sk").toBase64({ alphabet: "base64url" }) }],
		});
	});
});
