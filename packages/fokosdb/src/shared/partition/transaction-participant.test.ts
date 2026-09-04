import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../../server/do-partition.js";
import { PartitionStore } from "./partition-store.js";
import { TransactionParticipant } from "./transaction-participant.js";
import type { PrepareRequest } from "../transaction-types.js";
import { KeyCodec, type KeyBytes } from "../partition-topology/key-codec.js";
import invariant from "../invariant.js";
import { compileConditionExpression, compileUpdateExpression } from "../expression/compiler.js";
import { EST_ROW_BYTES_K } from "./item-size.js";

const kb = (s: string) => KeyCodec.encode(s);

const BASE_NOW = 1_000_000;

type Harness = {
	participant: TransactionParticipant;
	store: PartitionStore;
	clock: { now: number };
	upserts: Array<{ hashKey: KeyBytes; keyEstBytes: number }>;
};

// Runs `fn` against a TransactionParticipant over REAL Durable Object storage (vitest-pool-workers).
// The PartitionDO constructor has already run the schema migrations by the time the callback runs;
// constructing a second PartitionStore over the same storage is safe (migrations are idempotent).
async function withParticipant(fn: (h: Harness) => void | Promise<void>): Promise<void> {
	const stub = PartitionDO.getByName(env.PARTITION_DO, `participant-test.${crypto.randomUUID()}`);
	await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
		const store = new PartitionStore(state.storage);
		const clock = { now: BASE_NOW };
		const upserts: Array<{ hashKey: KeyBytes; keyEstBytes: number }> = [];
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
	// A lock is released only by the outcome of its transaction, and the recovery job reaches that
	// outcome through these two ids alone. A lock created without them can never be released, so
	// prepare must refuse rather than store one.
	it.each([
		["transactionId", { transactionId: "" }],
		["coordinatorDoId", { coordinatorDoId: "" }],
	])("refuses to lock an item when %s is empty, so no unreleasable lock is created", async (_name, override) => {
		await withParticipant(({ participant, store }) => {
			const request = prepareReq({
				...override,
				items: [{ hashKey: kb("hk1"), sortKey: kb("sk1"), operation: "put", data: "v1", kind: "text" }],
			});

			expect(() => participant.prepareLocal(request)).toThrow(/is required/);
			expect(store.pendingLockFor(kb("hk1"), kb("sk1"))).toBeUndefined();
		});
	});

	it("accepts and locks every item, and re-prepare of the same transaction is idempotent", async () => {
		await withParticipant(({ participant, store }) => {
			const request = prepareReq({
				items: [
					{ hashKey: kb("hk1"), sortKey: kb("sk1"), operation: "put", data: "v1", kind: "text" },
					{ hashKey: kb("hk2"), sortKey: KeyCodec.encodeOptional(undefined), operation: "delete" },
				],
			});

			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			expect(store.pendingLockFor(kb("hk1"), kb("sk1"))?.transaction_id).toBe(request.transactionId);
			expect(store.pendingLockFor(kb("hk2"), KeyCodec.encodeOptional(undefined))?.transaction_id).toBe(request.transactionId);

			// Idempotent re-prepare: accepted again, no duplicate locks.
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			expect(store.pendingTxCountFor(request.transactionId)).toBe(2);
		});
	});

	it("rejects with pending_conflict when another transaction holds the lock", async () => {
		await withParticipant(({ participant, store }) => {
			const first = prepareReq({ items: [{ hashKey: kb("hk"), sortKey: kb("sk"), operation: "put", data: "v1", kind: "text" }] });
			expect(participant.prepareLocal(first)).toEqual({ outcome: "accepted" });

			const second = prepareReq({ items: [{ hashKey: kb("hk"), sortKey: kb("sk"), operation: "put", data: "v2", kind: "text" }] });
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
			store.upsertItem({ hk: kb("hk"), sk: kb("sk"), data: "existing", kind: "text", ttlAt: null, lastTransactionTs: 1 });

			const request = prepareReq({
				items: [
					{
						hashKey: kb("hk"),
						sortKey: kb("sk"),
						operation: "put",
						data: "v",
						kind: "text",
						condition: compileConditionExpression({ op: "not_exists", args: [{ ref: "hashKey" }] }),
					},
				],
			});
			expect(participant.prepareLocal(request)).toEqual({
				outcome: "rejected",
				reason: { type: "condition_failed", hashKey: "hk", sortKey: "sk" },
			});
			expect(store.pendingTxCountFor(request.transactionId)).toBe(0);
		});
	});

	it("rejects with update_not_applicable when item is missing or not json", async () => {
		await withParticipant(({ participant, store }) => {
			const updatePlan = compileUpdateExpression([{ action: "set", target: { ref: "data", path: "$.field" }, value: { val: "new" } }]);

			// Missing item
			const missingReq = prepareReq({
				items: [{ hashKey: kb("missing-item"), sortKey: KeyCodec.encodeOptional(undefined), operation: "update", update: updatePlan }],
			});
			expect(participant.prepareLocal(missingReq)).toEqual({
				outcome: "rejected",
				reason: { type: "update_not_applicable", hashKey: "missing-item", sortKey: undefined },
			});

			// Item exists but is kind: "text", not json
			store.upsertItem({
				hk: kb("text-item"),
				sk: KeyCodec.encodeOptional(undefined),
				data: "text content",
				kind: "text",
				ttlAt: null,
				lastTransactionTs: 1,
			});
			const textReq = prepareReq({
				items: [{ hashKey: kb("text-item"), sortKey: KeyCodec.encodeOptional(undefined), operation: "update", update: updatePlan }],
			});
			expect(participant.prepareLocal(textReq)).toEqual({
				outcome: "rejected",
				reason: { type: "update_not_applicable", hashKey: "text-item", sortKey: undefined },
			});
		});
	});

	it("rejects with update_value_is_bytes when a value evaluates to bytes for this item", async () => {
		await withParticipant(({ participant, store }) => {
			// The same plan is valid for a text key and not for a binary one, so the cause belongs to the
			// item, not to the expression. A caller that only saw "not applicable" could not tell this
			// apart from a missing item or a missing path, and could not act on it.
			const plan = compileUpdateExpression([{ action: "set", target: { ref: "data", path: "$.k" }, value: { ref: "hashKey" } }]);
			const sk = KeyCodec.encodeOptional(undefined);
			const binaryKey = KeyCodec.encode(new Uint8Array([1, 2, 3]));
			store.upsertItem({ hk: binaryKey, sk, data: JSON.stringify({}), kind: "json", ttlAt: null, lastTransactionTs: 1 });

			const request = prepareReq({ items: [{ hashKey: binaryKey, sortKey: sk, operation: "update", update: plan }] });
			expect(participant.prepareLocal(request)).toEqual({
				outcome: "rejected",
				reason: { type: "update_value_is_bytes", hashKey: KeyCodec.decode(binaryKey), sortKey: undefined },
			});
			// The rejection took no lock and wrote nothing.
			expect(store.pendingTxCountFor(request.transactionId)).toBe(0);
			expect(store.getItem(binaryKey, sk).row?.v).toBe(1);

			// The single-shot path answers with the same reason.
			expect(participant.executeSingleShot({ items: [{ hashKey: binaryKey, sortKey: sk, operation: "update", update: plan }] })).toEqual({
				outcome: "rejected",
				reason: { type: "update_value_is_bytes", hashKey: KeyCodec.decode(binaryKey), sortKey: undefined },
			});
		});
	});

	it("materializes update document in pending_transactions at prepare and applies it at commit", async () => {
		await withParticipant(({ participant, store, upserts }) => {
			const sk = KeyCodec.encodeOptional(undefined);
			store.upsertItem({
				hk: kb("user"),
				sk,
				data: JSON.stringify({ name: "Alice", score: 10 }),
				kind: "json",
				ttlAt: 555,
				lastTransactionTs: 10,
			});

			const updatePlan = compileUpdateExpression([
				{ action: "set", target: { ref: "data", path: "$.score" }, value: { val: 20 } },
				{ action: "set", target: { ref: "data", path: "$.role" }, value: { val: "admin" } },
			]);

			const request = prepareReq({
				items: [{ hashKey: kb("user"), sortKey: sk, operation: "update", update: updatePlan }],
			});
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });

			// The lock row holds the complete new document as JSONB, and inherits the pre-image TTL. It is
			// the binary form, NOT JSON text, so commit binds exactly the bytes the probe measured at
			// prepare: a JSONB-to-text-to-JSONB round trip is not size-stable.
			const pending = store.getPendingTxOp(kb("user"), sk, request.transactionId);
			expect(pending?.operation).toBe("update");
			expect(pending?.kind).toBe("json");
			expect(pending?.ttl_epoch_utc_seconds).toBe(555);
			expect(pending?.data).toBeInstanceOf(Uint8Array);
			const materializedBytes = (pending?.data as Uint8Array).byteLength;

			// Commit applies the materialized document
			expect(
				participant.commitLocal({
					transactionId: request.transactionId,
					transactionTimestamp: request.transactionTimestamp,
					items: [{ hashKey: kb("user"), sortKey: sk }],
				}),
			).toEqual({ outcome: "committed" });

			const committed = store.getItem(kb("user"), sk);
			expect(committed.row?.v).toBe(2);
			expect(committed.row?.ttl_epoch_utc_seconds).toBe(555);
			expect(JSON.parse(committed.row?.data as string)).toEqual({ name: "Alice", score: 20, role: "admin" });

			// Commit stored the pending bytes verbatim, so the size prepare accepted is the size on disk.
			expect(store.measureItemBytes({ hk: kb("user"), sk, data: pending?.data as Uint8Array, kind: "json" })).toBe(
				materializedBytes + kb("user").byteLength + sk.byteLength + EST_ROW_BYTES_K,
			);

			// An update reports its new key size like a put does, so promotion and split accounting see
			// the growth of an item that the request itself never carried.
			expect(upserts).toEqual([
				{ hashKey: kb("user"), keyEstBytes: materializedBytes + kb("user").byteLength + sk.byteLength + EST_ROW_BYTES_K },
			]);
		});
	});

	it("persists the compiled condition plan and TTL after prepare accepts it", async () => {
		await withParticipant(({ participant, store }) => {
			const condition = compileConditionExpression({ op: "not_exists", args: [{ ref: "hashKey" }] });
			const request = prepareReq({
				items: [{ hashKey: kb("new"), sortKey: kb("sk"), operation: "put", data: "v", kind: "text", ttlAt: 777, condition }],
			});

			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			expect(store.queryPendingTxPage(null, 1)[0]).toMatchObject({
				conditions_json: JSON.stringify(condition),
				ttl_epoch_utc_seconds: 777,
			});
		});
	});

	it("rejects with timestamp_conflict when the item's last transaction is not older", async () => {
		await withParticipant(({ participant, store }) => {
			store.upsertItem({ hk: kb("hk"), sk: kb("sk"), data: "v", kind: "text", ttlAt: null, lastTransactionTs: BASE_NOW + 50 });

			const atTs = prepareReq({
				transactionTimestamp: BASE_NOW + 50, // equal to last_transaction_ts → conflict
				items: [{ hashKey: kb("hk"), sortKey: kb("sk"), operation: "put", data: "v2", kind: "text" }],
			});
			expect(participant.prepareLocal(atTs)).toEqual({
				outcome: "rejected",
				reason: { type: "timestamp_conflict", hashKey: "hk", sortKey: "sk" },
			});

			const aboveTs = prepareReq({
				transactionTimestamp: BASE_NOW + 51,
				items: [{ hashKey: kb("hk"), sortKey: kb("sk"), operation: "put", data: "v2", kind: "text" }],
			});
			expect(participant.prepareLocal(aboveTs)).toEqual({ outcome: "accepted" });
		});
	});

	it("still rejects a superseded transaction after a non-transactional write with a lower timestamp", async () => {
		await withParticipant(({ participant, store }) => {
			// 1. A transaction committed with a coordinator clock running ahead of this partition's.
			store.upsertItem({
				hk: kb("hk"),
				sk: kb("sk"),
				data: "from-tx",
				kind: "text",
				ttlAt: null,
				lastTransactionTs: BASE_NOW + 4_000,
			});

			// 2. A non-transactional put lands next, stamped with the partition's own (lower) clock.
			store.upsertItem({
				hk: kb("hk"),
				sk: kb("sk"),
				data: "from-put",
				kind: "text",
				ttlAt: null,
				lastTransactionTs: BASE_NOW,
			});

			// 3. A transaction stamped between the two must NOT be able to overwrite the newer put.
			//    It could, if step 2 had lowered the item's watermark.
			const superseded = prepareReq({
				transactionTimestamp: BASE_NOW + 2_000,
				items: [{ hashKey: kb("hk"), sortKey: kb("sk"), operation: "put", data: "stale", kind: "text" }],
			});
			expect(participant.prepareLocal(superseded)).toEqual({
				outcome: "rejected",
				reason: { type: "timestamp_conflict", hashKey: "hk", sortKey: "sk" },
			});
			expect(store.getItem(kb("hk"), kb("sk")).row?.data).toBe("from-put");
		});
	});

	it("rejects with timestamp_conflict for an ABSENT item via the deletion watermark", async () => {
		await withParticipant(({ participant, store }) => {
			// A transactional delete bumps the watermark even though the row never existed.
			store.deleteItem({ hk: kb("gone"), sk: KeyCodec.encodeOptional(undefined), watermarkTs: BASE_NOW + 200, bumpWatermarkAlways: true });

			const atWatermark = prepareReq({
				transactionTimestamp: BASE_NOW + 200,
				items: [{ hashKey: kb("absent"), sortKey: KeyCodec.encodeOptional(undefined), operation: "check" }],
			});
			expect(participant.prepareLocal(atWatermark)).toEqual({
				outcome: "rejected",
				reason: { type: "timestamp_conflict", hashKey: "absent", sortKey: undefined },
			});

			const aboveWatermark = prepareReq({
				transactionTimestamp: BASE_NOW + 201,
				items: [{ hashKey: kb("absent"), sortKey: KeyCodec.encodeOptional(undefined), operation: "check" }],
			});
			expect(participant.prepareLocal(aboveWatermark)).toEqual({ outcome: "accepted" });
		});
	});

	it("rejects with clock_skew when the transaction timestamp is too far ahead of the injected clock", async () => {
		await withParticipant(({ participant, clock }) => {
			const skewed = prepareReq({
				transactionTimestamp: clock.now + TransactionParticipant.MAX_CLOCK_SKEW_MS + 1,
				items: [{ hashKey: kb("hk"), sortKey: KeyCodec.encodeOptional(undefined), operation: "put", data: "v", kind: "text" }],
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
				items: [{ hashKey: kb("hk"), sortKey: KeyCodec.encodeOptional(undefined), operation: "put", data: "v", kind: "text" }],
			});
			expect(participant.prepareLocal(atBound)).toEqual({ outcome: "accepted" });
		});
	});

	it("locks nothing when any item in the request is rejected", async () => {
		await withParticipant(({ participant, store }) => {
			store.upsertItem({
				hk: kb("conflicting"),
				sk: KeyCodec.encodeOptional(undefined),
				data: "v",
				kind: "text",
				ttlAt: null,
				lastTransactionTs: BASE_NOW + 500,
			});

			const request = prepareReq({
				transactionTimestamp: BASE_NOW + 100,
				items: [
					{ hashKey: kb("fine"), sortKey: KeyCodec.encodeOptional(undefined), operation: "put", data: "v", kind: "text" },
					{ hashKey: kb("conflicting"), sortKey: KeyCodec.encodeOptional(undefined), operation: "put", data: "v", kind: "text" },
				],
			});
			expect(participant.prepareLocal(request)).toEqual({
				outcome: "rejected",
				reason: { type: "timestamp_conflict", hashKey: "conflicting", sortKey: undefined },
			});
			expect(store.pendingTxCountFor(request.transactionId)).toBe(0);
			expect(store.pendingLockFor(kb("fine"), KeyCodec.encodeOptional(undefined))).toBeUndefined();
		});
	});
});

describe("TransactionParticipant - commit", () => {
	it("applies put, delete, and check operations and clears the locks", async () => {
		await withParticipant(({ participant, store, upserts }) => {
			store.upsertItem({
				hk: kb("to-delete"),
				sk: KeyCodec.encodeOptional(undefined),
				data: "old",
				kind: "text",
				ttlAt: null,
				lastTransactionTs: 1,
			});
			store.upsertItem({
				hk: kb("to-check"),
				sk: KeyCodec.encodeOptional(undefined),
				data: "kept",
				kind: "text",
				ttlAt: null,
				lastTransactionTs: 1,
			});

			const request = prepareReq({
				items: [
					{
						hashKey: kb("to-put"),
						sortKey: KeyCodec.encodeOptional(undefined),
						operation: "put",
						data: "new-value",
						kind: "text",
						ttlAt: 777,
					},
					{ hashKey: kb("to-delete"), sortKey: KeyCodec.encodeOptional(undefined), operation: "delete" },
					{ hashKey: kb("to-check"), sortKey: KeyCodec.encodeOptional(undefined), operation: "check" },
				],
			});
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });

			const commitTs = request.transactionTimestamp;
			const commitItems = request.items.map((item) => (item.operation === "put" ? { ...item, ttlAt: 999 } : item));
			expect(participant.commitLocal({ transactionId: request.transactionId, transactionTimestamp: commitTs, items: commitItems })).toEqual(
				{
					outcome: "committed",
				},
			);

			// Commit applies the expiry instant from the prepared row, not from the commit request.
			const put = store.getItem(kb("to-put"), KeyCodec.encodeOptional(undefined)).row;
			expect(put).toMatchObject({ data: "new-value", ttl_epoch_utc_seconds: 777, last_transaction_ts: commitTs });
			expect(upserts).toEqual([{ hashKey: kb("to-put"), keyEstBytes: expect.any(Number) }]);

			// delete: row gone and the deletion watermark advanced to the commit timestamp.
			expect(store.getItem(kb("to-delete"), KeyCodec.encodeOptional(undefined)).row).toBeUndefined();
			expect(store.getMaxDeletedTs()).toBe(commitTs);

			// check: data untouched, timestamp bumped.
			expect(store.getItem(kb("to-check"), KeyCodec.encodeOptional(undefined)).row).toMatchObject({
				data: "kept",
				last_transaction_ts: commitTs,
			});

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
					{ hashKey: kb("a"), sortKey: KeyCodec.encodeOptional(undefined), operation: "put", data: "v", kind: "text" },
					{ hashKey: kb("b"), sortKey: KeyCodec.encodeOptional(undefined), operation: "put", data: "v", kind: "text" },
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
					{ hashKey: kb("a"), sortKey: KeyCodec.encodeOptional(undefined), operation: "put", data: "v", kind: "text" },
					{ hashKey: kb("b"), sortKey: KeyCodec.encodeOptional(undefined), operation: "put", data: "v", kind: "text" },
				],
			});
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });

			expect(() =>
				participant.commitLocal({
					transactionId: request.transactionId,
					transactionTimestamp: request.transactionTimestamp,
					items: [request.items[0], { hashKey: kb("c"), sortKey: KeyCodec.encodeOptional(undefined) }],
				}),
			).toThrow(/not found in pending_transactions/);
		});
	});
});

describe("TransactionParticipant - single shot", () => {
	it("stores the TTL of a put", async () => {
		await withParticipant(({ participant, store }) => {
			const sortKey = KeyCodec.encodeOptional(undefined);
			expect(
				participant.executeSingleShot({
					items: [{ hashKey: kb("ttl-put"), sortKey, operation: "put", data: "value", kind: "text", ttlAt: 777 }],
				}),
			).toEqual({ outcome: "committed" });
			expect(store.getItem(kb("ttl-put"), sortKey).row?.ttl_epoch_utc_seconds).toBe(777);
		});
	});

	it("executes single-shot update and preserves TTL when omitted", async () => {
		await withParticipant(({ participant, store }) => {
			const sortKey = KeyCodec.encodeOptional(undefined);
			store.upsertItem({
				hk: kb("u1"),
				sk: sortKey,
				data: JSON.stringify({ count: 1, name: "item1" }),
				kind: "json",
				ttlAt: 999,
				lastTransactionTs: 10,
			});

			const updatePlan = compileUpdateExpression([{ action: "set", target: { ref: "data", path: "$.count" }, value: { val: 2 } }]);

			const res = participant.executeSingleShot({
				items: [{ hashKey: kb("u1"), sortKey, operation: "update", update: updatePlan }],
			});
			expect(res).toEqual({ outcome: "committed" });

			const updated = store.getItem(kb("u1"), sortKey);
			expect(updated.row?.v).toBe(2);
			expect(JSON.parse(updated.row?.data as string)).toEqual({ count: 2, name: "item1" });
			expect(updated.row?.ttl_epoch_utc_seconds).toBe(999);
		});
	});

	it("rejects single-shot update with update_not_applicable when item is missing", async () => {
		await withParticipant(({ participant }) => {
			const sortKey = KeyCodec.encodeOptional(undefined);
			const updatePlan = compileUpdateExpression([{ action: "set", target: { ref: "data", path: "$.a" }, value: { val: 1 } }]);

			const res = participant.executeSingleShot({
				items: [{ hashKey: kb("missing-u"), sortKey, operation: "update", update: updatePlan }],
			});
			expect(res).toEqual({
				outcome: "rejected",
				reason: { type: "update_not_applicable", hashKey: "missing-u", sortKey: undefined },
			});
		});
	});
});

describe("TransactionParticipant - cancel", () => {
	it("removes every lock of the transaction so the items become preparable again", async () => {
		await withParticipant(({ participant, store }) => {
			const request = prepareReq({
				items: [
					{ hashKey: kb("a"), sortKey: KeyCodec.encodeOptional(undefined), operation: "put", data: "v", kind: "text" },
					{ hashKey: kb("b"), sortKey: KeyCodec.encodeOptional(undefined), operation: "put", data: "v", kind: "text" },
				],
			});
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });

			participant.cancelLocal(request.transactionId);
			expect(store.pendingTxCountFor(request.transactionId)).toBe(0);

			const retry = prepareReq({
				items: [{ hashKey: kb("a"), sortKey: KeyCodec.encodeOptional(undefined), operation: "put", data: "v2", kind: "text" }],
			});
			expect(participant.prepareLocal(retry)).toEqual({ outcome: "accepted" });
		});
	});

	// A cancelled update must leave no trace of the document prepare materialized: not the item, not
	// its version, and not the size accounting that a materialized pending row would otherwise skew.
	it("discards the materialized document of an update and leaves the item untouched", async () => {
		await withParticipant(({ participant, store, upserts }) => {
			const sk = KeyCodec.encodeOptional(undefined);
			store.upsertItem({ hk: kb("user"), sk, data: JSON.stringify({ score: 10 }), kind: "json", ttlAt: null, lastTransactionTs: 10 });

			const plan = compileUpdateExpression([{ action: "set", target: { ref: "data", path: "$.score" }, value: { val: 999 } }]);
			const request = prepareReq({ items: [{ hashKey: kb("user"), sortKey: sk, operation: "update", update: plan }] });
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });

			participant.cancelLocal(request.transactionId);

			expect(store.pendingTxCountFor(request.transactionId)).toBe(0);
			const item = store.getItem(kb("user"), sk);
			expect(item.row?.v).toBe(1);
			expect(JSON.parse(item.row?.data as string)).toEqual({ score: 10 });
			// Nothing was applied, so the split and promotion accounting was never told of a new size.
			expect(upserts).toEqual([]);

			// The key is preparable again, and the second update sees the ORIGINAL pre-image.
			const retry = prepareReq({ items: [{ hashKey: kb("user"), sortKey: sk, operation: "update", update: plan }] });
			expect(participant.prepareLocal(retry)).toEqual({ outcome: "accepted" });
			participant.commitLocal({
				transactionId: retry.transactionId,
				transactionTimestamp: retry.transactionTimestamp,
				items: [{ hashKey: kb("user"), sortKey: sk }],
			});
			const committed = store.getItem(kb("user"), sk);
			expect(committed.row?.v).toBe(2);
			expect(JSON.parse(committed.row?.data as string)).toEqual({ score: 999 });
			// Applied exactly once, for this key. The byte arithmetic of the estimate is partition-store's
			// own test: est_row_bytes measures the stored JSONB, which is not the length of the JSON text.
			expect(upserts).toHaveLength(1);
			expect(upserts[0].hashKey).toEqual(kb("user"));
		});
	});
});

describe("TransactionParticipant - readForTransaction", () => {
	it("returns version and ttl per found item, so the TC can compare `v` and the caller gets the version", async () => {
		await withParticipant(({ participant, store }) => {
			const sk = KeyCodec.encodeOptional(undefined);
			// Two writes inside the SAME millisecond: last_transaction_ts is identical, only `v` moves.
			// This is the pair the two-phase read must be able to tell apart.
			store.upsertItem({ hk: kb("k"), sk, data: "first", kind: "text", ttlAt: 777, lastTransactionTs: 100 });
			const before = participant.readForTransactionLocal({ items: [{ hashKey: kb("k"), sortKey: sk }] }).items[0];

			store.upsertItem({ hk: kb("k"), sk, data: "second", kind: "text", ttlAt: 777, lastTransactionTs: 100 });
			const after = participant.readForTransactionLocal({ items: [{ hashKey: kb("k"), sortKey: sk }] }).items[0];

			invariant(before.found && after.found);
			expect(before.ttlAt).toBe(777);
			// The timestamps are identical, so a timestamp-only comparison cannot see the write at all.
			expect(before.lastCommittedTs).toBe(after.lastCommittedTs);
			// `v` does, which is why it is the primary conflict datum.
			expect(before.version).toBe(1);
			expect(after.version).toBe(2);
		});
	});

	it("echoes canonical KeyBytes and returns data, lastCommittedTs, and hasPendingWrite per item", async () => {
		await withParticipant(({ participant, store }) => {
			store.upsertItem({
				hk: kb("existing"),
				sk: KeyCodec.encodeOptional(undefined),
				data: "value",
				kind: "text",
				ttlAt: null,
				lastTransactionTs: 42,
			});
			const lock = prepareReq({
				items: [{ hashKey: kb("locked-absent"), sortKey: KeyCodec.encodeOptional(undefined), operation: "put", data: "v", kind: "text" }],
			});
			expect(participant.prepareLocal(lock)).toEqual({ outcome: "accepted" });

			const response = participant.readForTransactionLocal({
				items: [
					{ hashKey: kb("existing"), sortKey: KeyCodec.encodeOptional(undefined) },
					{ hashKey: kb("locked-absent"), sortKey: KeyCodec.encodeOptional(undefined) },
					{ hashKey: kb("missing"), sortKey: KeyCodec.encodeOptional(undefined) },
				],
			});
			// Keys come back as canonical KeyBytes (sortKey [] = absent), not decoded: the TC pairs the
			// two read phases by bytes, and db.ts decodes once at the public exit.
			const ABSENT = KeyCodec.encodeOptional(undefined);
			expect(response.items).toEqual([
				{
					found: true,
					hashKey: kb("existing"),
					sortKey: ABSENT,
					data: "value",
					kind: "text",
					version: 1,
					ttlAt: undefined,
					lastCommittedTs: 42,
					hasPendingWrite: false,
				},
				{
					found: false,
					hashKey: kb("locked-absent"),
					sortKey: ABSENT,
					lastCommittedTs: 0,
					hasPendingWrite: true,
				},
				{ found: false, hashKey: kb("missing"), sortKey: ABSENT, lastCommittedTs: 0, hasPendingWrite: false },
			]);
		});
	});
});

describe("TransactionParticipant - stale transactions", () => {
	it("lists a transaction only once its locks age past the staleness bound (injected clock)", async () => {
		await withParticipant(({ participant, clock }) => {
			const request = prepareReq({
				items: [
					{ hashKey: kb("a"), sortKey: KeyCodec.encodeOptional(undefined), operation: "put", data: "v", kind: "text" },
					{ hashKey: kb("b"), sortKey: KeyCodec.encodeOptional(undefined), operation: "put", data: "v", kind: "text" },
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
