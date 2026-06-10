import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../do-partition.js";
import { PartitionStore } from "./partition-store.js";
import { TransactionParticipant } from "./transaction-participant.js";
import type { PrepareRequest } from "../transaction-types.js";

const BASE_NOW = 1_000_000;

type Harness = {
	participant: TransactionParticipant;
	store: PartitionStore;
	clock: { now: number };
	upserts: Array<{ hashKey: string; keyEstBytes: number }>;
};

// Runs `fn` against a TransactionParticipant over REAL Durable Object storage (vitest-pool-workers).
// The PartitionDO constructor has already run the schema migrations by the time the callback runs;
// constructing a second PartitionStore over the same storage is safe (migrations are idempotent).
async function withParticipant(fn: (h: Harness) => void | Promise<void>): Promise<void> {
	const id = env.PARTITION_DO.idFromName(`participant-test.${crypto.randomUUID()}`);
	const stub = env.PARTITION_DO.get(id);
	await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
		const store = new PartitionStore(state.storage);
		const clock = { now: BASE_NOW };
		const upserts: Array<{ hashKey: string; keyEstBytes: number }> = [];
		const participant = new TransactionParticipant({
			store,
			now: () => clock.now,
			onItemUpserted: (hashKey, keyEstBytes) => upserts.push({ hashKey, keyEstBytes }),
		});
		await fn({ participant, store, clock, upserts });
	});
}

function prepareReq(overrides: Partial<PrepareRequest> & Pick<PrepareRequest, "items">): PrepareRequest {
	return {
		transactionId: overrides.transactionId ?? crypto.randomUUID(),
		coordinatorDoId: overrides.coordinatorDoId ?? "tc-test",
		transactionTimestamp: overrides.transactionTimestamp ?? BASE_NOW + 100,
		items: overrides.items,
	};
}

describe("TransactionParticipant - prepare", () => {
	it("accepts and locks every item, and re-prepare of the same transaction is idempotent", async () => {
		await withParticipant(({ participant, store }) => {
			const request = prepareReq({
				items: [
					{ hashKey: "hk1", sortKey: "sk1", operation: "put", data: "v1" },
					{ hashKey: "hk2", operation: "delete" },
				],
			});

			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			expect(store.pendingLockFor("hk1", "sk1")?.transaction_id).toBe(request.transactionId);
			expect(store.pendingLockFor("hk2", "")?.transaction_id).toBe(request.transactionId);

			// Idempotent re-prepare: accepted again, no duplicate locks.
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			expect(store.pendingTxCountFor(request.transactionId)).toBe(2);
		});
	});

	it("rejects with pending_conflict when another transaction holds the lock", async () => {
		await withParticipant(({ participant, store }) => {
			const first = prepareReq({ items: [{ hashKey: "hk", sortKey: "sk", operation: "put", data: "v1" }] });
			expect(participant.prepareLocal(first)).toEqual({ outcome: "accepted" });

			const second = prepareReq({ items: [{ hashKey: "hk", sortKey: "sk", operation: "put", data: "v2" }] });
			expect(participant.prepareLocal(second)).toEqual({
				outcome: "rejected",
				reason: {
					type: "pending_conflict",
					hashKey: "hk",
					sortKey: "sk",
					conflictingTransactionId: first.transactionId,
				},
			});
			expect(store.pendingTxCountFor(second.transactionId)).toBe(0);
		});
	});

	it("rejects with condition_failed when an item condition does not hold", async () => {
		await withParticipant(({ participant, store }) => {
			store.upsertItem({ hk: "hk", sk: "sk", data: "existing", ttlEpochUtcSeconds: null, lastTransactionTs: 1 });

			const request = prepareReq({
				items: [{ hashKey: "hk", sortKey: "sk", operation: "put", data: "v", conditions: [{ type: "item_not_exists" }] }],
			});
			expect(participant.prepareLocal(request)).toEqual({
				outcome: "rejected",
				reason: { type: "condition_failed", hashKey: "hk", sortKey: "sk" },
			});
			expect(store.pendingTxCountFor(request.transactionId)).toBe(0);
		});
	});

	it("rejects with timestamp_conflict when the item's last transaction is not older", async () => {
		await withParticipant(({ participant, store }) => {
			store.upsertItem({ hk: "hk", sk: "sk", data: "v", ttlEpochUtcSeconds: null, lastTransactionTs: BASE_NOW + 50 });

			const atTs = prepareReq({
				transactionTimestamp: BASE_NOW + 50, // equal to last_transaction_ts → conflict
				items: [{ hashKey: "hk", sortKey: "sk", operation: "put", data: "v2" }],
			});
			expect(participant.prepareLocal(atTs)).toEqual({
				outcome: "rejected",
				reason: { type: "timestamp_conflict", hashKey: "hk", sortKey: "sk" },
			});

			const aboveTs = prepareReq({
				transactionTimestamp: BASE_NOW + 51,
				items: [{ hashKey: "hk", sortKey: "sk", operation: "put", data: "v2" }],
			});
			expect(participant.prepareLocal(aboveTs)).toEqual({ outcome: "accepted" });
		});
	});

	it("rejects with timestamp_conflict for an ABSENT item via the deletion watermark", async () => {
		await withParticipant(({ participant, store }) => {
			// A transactional delete bumps the watermark even though the row never existed.
			store.deleteItem({ hk: "gone", sk: "", watermarkTs: BASE_NOW + 200, bumpWatermarkAlways: true });

			const atWatermark = prepareReq({
				transactionTimestamp: BASE_NOW + 200,
				items: [{ hashKey: "absent", operation: "check" }],
			});
			expect(participant.prepareLocal(atWatermark)).toEqual({
				outcome: "rejected",
				reason: { type: "timestamp_conflict", hashKey: "absent", sortKey: undefined },
			});

			const aboveWatermark = prepareReq({
				transactionTimestamp: BASE_NOW + 201,
				items: [{ hashKey: "absent", operation: "check" }],
			});
			expect(participant.prepareLocal(aboveWatermark)).toEqual({ outcome: "accepted" });
		});
	});

	it("rejects with clock_skew when the transaction timestamp is too far ahead of the injected clock", async () => {
		await withParticipant(({ participant, clock }) => {
			const skewed = prepareReq({
				transactionTimestamp: clock.now + TransactionParticipant.MAX_CLOCK_SKEW_MS + 1,
				items: [{ hashKey: "hk", operation: "put", data: "v" }],
			});
			expect(participant.prepareLocal(skewed)).toEqual({
				outcome: "rejected",
				reason: {
					type: "clock_skew",
					serverTimestampMs: clock.now,
					transactionTimestampMs: skewed.transactionTimestamp,
				},
			});

			// Exactly at the skew bound is allowed.
			const atBound = prepareReq({
				transactionTimestamp: clock.now + TransactionParticipant.MAX_CLOCK_SKEW_MS,
				items: [{ hashKey: "hk", operation: "put", data: "v" }],
			});
			expect(participant.prepareLocal(atBound)).toEqual({ outcome: "accepted" });
		});
	});

	it("locks nothing when any item in the request is rejected", async () => {
		await withParticipant(({ participant, store }) => {
			store.upsertItem({ hk: "conflicting", sk: "", data: "v", ttlEpochUtcSeconds: null, lastTransactionTs: BASE_NOW + 500 });

			const request = prepareReq({
				transactionTimestamp: BASE_NOW + 100,
				items: [
					{ hashKey: "fine", operation: "put", data: "v" },
					{ hashKey: "conflicting", operation: "put", data: "v" },
				],
			});
			expect(participant.prepareLocal(request)).toEqual({
				outcome: "rejected",
				reason: { type: "timestamp_conflict", hashKey: "conflicting", sortKey: undefined },
			});
			expect(store.pendingTxCountFor(request.transactionId)).toBe(0);
			expect(store.pendingLockFor("fine", "")).toBeUndefined();
		});
	});
});

describe("TransactionParticipant - commit", () => {
	it("applies put, delete, and check operations and clears the locks", async () => {
		await withParticipant(({ participant, store, upserts }) => {
			store.upsertItem({ hk: "to-delete", sk: "", data: "old", ttlEpochUtcSeconds: null, lastTransactionTs: 1 });
			store.upsertItem({ hk: "to-check", sk: "", data: "kept", ttlEpochUtcSeconds: null, lastTransactionTs: 1 });

			const request = prepareReq({
				items: [
					{ hashKey: "to-put", operation: "put", data: "new-value" },
					{ hashKey: "to-delete", operation: "delete" },
					{ hashKey: "to-check", operation: "check" },
				],
			});
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });

			const commitTs = request.transactionTimestamp;
			expect(
				participant.commitLocal({ transactionId: request.transactionId, transactionTimestamp: commitTs, items: request.items }),
			).toEqual({ outcome: "committed" });

			// put: written with the commit timestamp, and the promotion hook saw the upsert.
			const put = store.getItem("to-put", "").row;
			expect(put).toMatchObject({ data: "new-value", last_transaction_ts: commitTs });
			expect(upserts).toEqual([{ hashKey: "to-put", keyEstBytes: expect.any(Number) }]);

			// delete: row gone and the deletion watermark advanced to the commit timestamp.
			expect(store.getItem("to-delete", "").row).toBeUndefined();
			expect(store.getMaxDeletedTs()).toBe(commitTs);

			// check: data untouched, timestamp bumped.
			expect(store.getItem("to-check", "").row).toMatchObject({ data: "kept", last_transaction_ts: commitTs });

			// All locks for the transaction are gone.
			expect(store.pendingTxCountFor(request.transactionId)).toBe(0);
		});
	});

	it("is idempotent: committing a transaction with no pending locks is a no-op", async () => {
		await withParticipant(({ participant, store, upserts }) => {
			expect(participant.commitLocal({ transactionId: "unknown-tx", transactionTimestamp: BASE_NOW, items: [] })).toEqual({
				outcome: "committed",
			});
			expect(store.getMaxDeletedTs()).toBe(0);
			expect(upserts).toEqual([]);
		});
	});

	it("throws when the request item count does not match the pending locks", async () => {
		await withParticipant(({ participant }) => {
			const request = prepareReq({
				items: [
					{ hashKey: "a", operation: "put", data: "v" },
					{ hashKey: "b", operation: "put", data: "v" },
				],
			});
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });

			expect(() =>
				participant.commitLocal({
					transactionId: request.transactionId,
					transactionTimestamp: request.transactionTimestamp,
					items: [request.items[0]],
				}),
			).toThrow(/pending_transactions has 2 items but request has 1/);
		});
	});

	it("throws when a request item is not among the pending locks", async () => {
		await withParticipant(({ participant }) => {
			const request = prepareReq({
				items: [
					{ hashKey: "a", operation: "put", data: "v" },
					{ hashKey: "b", operation: "put", data: "v" },
				],
			});
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });

			expect(() =>
				participant.commitLocal({
					transactionId: request.transactionId,
					transactionTimestamp: request.transactionTimestamp,
					items: [request.items[0], { hashKey: "c", operation: "put", data: "v" }],
				}),
			).toThrow(/not found in pending_transactions/);
		});
	});
});

describe("TransactionParticipant - cancel", () => {
	it("removes every lock of the transaction so the items become preparable again", async () => {
		await withParticipant(({ participant, store }) => {
			const request = prepareReq({
				items: [
					{ hashKey: "a", operation: "put", data: "v" },
					{ hashKey: "b", operation: "put", data: "v" },
				],
			});
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });

			participant.cancelLocal(request.transactionId);
			expect(store.pendingTxCountFor(request.transactionId)).toBe(0);

			const retry = prepareReq({ items: [{ hashKey: "a", operation: "put", data: "v2" }] });
			expect(participant.prepareLocal(retry)).toEqual({ outcome: "accepted" });
		});
	});
});

describe("TransactionParticipant - readForTransaction", () => {
	it("returns data, lastCommittedTs, and hasPendingWrite per item", async () => {
		await withParticipant(({ participant, store }) => {
			store.upsertItem({ hk: "existing", sk: "sk", data: "value", ttlEpochUtcSeconds: null, lastTransactionTs: 42 });
			const lock = prepareReq({ items: [{ hashKey: "locked-absent", operation: "put", data: "v" }] });
			expect(participant.prepareLocal(lock)).toEqual({ outcome: "accepted" });

			const response = participant.readForTransactionLocal({
				transactionId: "read-tx",
				items: [{ hashKey: "existing", sortKey: "sk" }, { hashKey: "locked-absent" }, { hashKey: "missing" }],
			});
			expect(response.items).toEqual([
				{ found: true, hashKey: "existing", sortKey: "sk", data: "value", lastCommittedTs: 42, hasPendingWrite: false },
				{ found: false, hashKey: "locked-absent", sortKey: undefined, lastCommittedTs: 0, hasPendingWrite: true },
				{ found: false, hashKey: "missing", sortKey: undefined, lastCommittedTs: 0, hasPendingWrite: false },
			]);
		});
	});
});

describe("TransactionParticipant - stale transactions", () => {
	it("lists a transaction only once its locks age past the staleness bound (injected clock)", async () => {
		await withParticipant(({ participant, clock }) => {
			const request = prepareReq({
				items: [
					{ hashKey: "a", operation: "put", data: "v" },
					{ hashKey: "b", operation: "put", data: "v" },
				],
			});
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });

			// Locks were created at clock.now — not yet stale.
			expect(participant.listStaleTransactions(5_000, 10)).toEqual([]);

			clock.now += 5_001;
			expect(participant.listStaleTransactions(5_000, 10)).toEqual([
				{ transaction_id: request.transactionId, coordinator_do_id: "tc-test" },
			]);
		});
	});
});
