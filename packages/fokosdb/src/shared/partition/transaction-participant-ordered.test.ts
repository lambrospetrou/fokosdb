// Ordered per-item execution in TransactionParticipant: a request with more than one operation for
// one item, on the single-partition path (executeSingleShot) and on the two-phase path (prepareLocal
// and commitLocal). The reference run applies the same operations one by one as one-operation
// transactions with the same timestamp, from the same start state. Both paths must give its result.
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import type { PartitionDO } from "../../server/do-partition.js";
import { DEFAULT_PARTITION_CONFIG } from "../../server/host-config.js";
import { testCoordinatorRef, testPartitionStub } from "../../../test/stub-helpers.js";
import { fokosErrorWith, invariantFailure } from "../../../test/errors-matchers.js";
import { KeyCodec, KeyPairMap } from "../../sharding/key-codec.js";
import { conditionTree, updateTree } from "../expression/test-fixtures.js";
import { MAX_ITEM_BYTES, TX_ORDER_TS_UNITS_PER_MS } from "../transaction-limits.js";
import type { PrepareRequest, TransactionItem, TransactionItemKey } from "../transaction-wire-types.js";
import { PartitionStore } from "./partition-store.js";
import { TransactionParticipant } from "./transaction-participant.js";

const kb = (s: string) => KeyCodec.encode(s);
const SK = kb("sk");
const BASE_NOW = 1_000_000;
/** The transaction timestamp of every run in this file. The seeded rows are older. */
const T = (BASE_NOW + 100) * TX_ORDER_TS_UNITS_PER_MS;

type Harness = { participant: TransactionParticipant; store: PartitionStore; storage: DurableObjectStorage };

async function withPartition<R>(fn: (h: Harness) => R): Promise<R> {
	const stub = testPartitionStub(`ordered-test.${crypto.randomUUID()}`);
	return await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) => {
		const store = new PartitionStore(state.storage);
		const participant = new TransactionParticipant({
			store,
			now: () => BASE_NOW,
			maxClockSkewMs: () => DEFAULT_PARTITION_CONFIG.maxClockSkewMs,
			staleTransactionMs: () => DEFAULT_PARTITION_CONFIG.staleTransactionMs,
			txOrderTimestamp: () => T,
			ownerCheck: () => () => true,
		});
		return fn({ participant, store, storage: state.storage });
	});
}

type Op = Omit<TransactionItem, "opIndex">;

const exists = conditionTree({ op: "exists", args: [{ ref: "hashKey" }] });
const notExists = conditionTree({ op: "not_exists", args: [{ ref: "hashKey" }] });
const incN = updateTree([
	{
		action: "set",
		target: { ref: "data", path: "$.n" },
		value: { fn: "+", args: [{ fn: "if_not_exists", args: [{ ref: "data", path: "$.n" }, { val: 0 }] }, { val: 1 }] },
	},
]);

const put = (key: string, data: unknown, ttlAt?: number): Op => ({
	hashKey: kb(key),
	sortKey: SK,
	operation: "put",
	data: JSON.stringify(data),
	kind: "json",
	...(ttlAt === undefined ? {} : { ttlAt }),
});
const update = (key: string, ttlAt?: number): Op => ({
	hashKey: kb(key),
	sortKey: SK,
	operation: "update",
	update: incN,
	...(ttlAt === undefined ? {} : { ttlAt }),
});
const del = (key: string): Op => ({ hashKey: kb(key), sortKey: SK, operation: "delete" });
const check = (key: string, present: boolean): Op => ({
	hashKey: kb(key),
	sortKey: SK,
	operation: "check",
	condition: present ? exists : notExists,
});

function withOpIndex(ops: Op[]): TransactionItem[] {
	return ops.map((op, opIndex) => ({ ...op, opIndex }));
}

function prepareReq(items: TransactionItem[], transactionId: string = crypto.randomUUID()): PrepareRequest {
	return { transactionId, coordinator: testCoordinatorRef("tok-ordered"), transactionTimestamp: T, items };
}

function uniqueKeys(items: readonly TransactionItem[]): TransactionItemKey[] {
	const keys = new KeyPairMap<TransactionItemKey>();
	for (const { hashKey, sortKey } of items) {
		keys.set(hashKey, sortKey, { hashKey, sortKey });
	}
	return [...keys.values()];
}

/**
 * The start state of each run: item "A" exists with v = 2, and the partition has max_deleted_v = 3
 * from a deleted item. Every stamp is older than T.
 */
function seed(store: PartitionStore): void {
	store.upsertItem({ hk: kb("A"), sk: SK, data: JSON.stringify({ n: 1 }), kind: "json", ttlAt: null, txOrderTs: 1 });
	store.upsertItem({ hk: kb("A"), sk: SK, data: JSON.stringify({ n: 1, a: true }), kind: "json", ttlAt: 500, txOrderTs: 2 });
	store.upsertItem({ hk: kb("B"), sk: SK, data: JSON.stringify({ b: 1 }), kind: "json", ttlAt: null, txOrderTs: 3 });
	for (let i = 0; i < 3; i++) {
		store.upsertItem({ hk: kb("gone"), sk: SK, data: "x", kind: "text", ttlAt: null, txOrderTs: 4 });
	}
	store.deleteItem({ hk: kb("gone"), sk: SK, txOrderTs: 5 });
}

const b64 = (value: unknown) => (typeof value === "string" ? value : new Uint8Array(value as ArrayBuffer).toBase64());

/** Every visible value of the partition: items, deletion metadata, size estimates, and the lock count. */
function snapshot(storage: DurableObjectStorage) {
	return {
		items: storage.sql
			.exec(
				`SELECT item_id, hk, sk, v, data, data_kind, ttl_epoch_utc_seconds, last_read_ts, last_write_ts, est_row_bytes FROM items ORDER BY hk, sk`,
			)
			.toArray()
			.map((r) => ({ ...r, hk: b64(r.hk), sk: b64(r.sk), data: b64(r.data) })),
		deletion: storage.sql.exec(`SELECT max_delete_tx_order_ts, max_deleted_v FROM deletion_metadata`).one(),
		sizes: storage.sql
			.exec(`SELECT hk, est_bytes FROM key_size_estimates ORDER BY hk`)
			.toArray()
			.map((r) => ({ ...r, hk: b64(r.hk) })),
		locks: storage.sql.exec(`SELECT COUNT(*) AS n FROM pending_transactions`).one().n,
	};
}

type Snapshot = ReturnType<typeof snapshot>;

/** Applies each operation alone, as a one-operation transaction at T. */
function referenceRun(ops: Op[]): Promise<Snapshot> {
	return withPartition(({ participant, store, storage }) => {
		seed(store);
		for (const op of ops) {
			expect(participant.executeSingleShot({ items: [{ ...op, opIndex: 0 }] }).response).toEqual({ outcome: "committed" });
		}
		return snapshot(storage);
	});
}

function singlePartitionRun(ops: Op[]): Promise<Snapshot> {
	return withPartition(({ participant, store, storage }) => {
		seed(store);
		expect(participant.executeSingleShot({ items: withOpIndex(ops) }).response).toEqual({ outcome: "committed" });
		return snapshot(storage);
	});
}

function twoPhaseRun(ops: Op[]): Promise<Snapshot> {
	return withPartition(({ participant, store, storage }) => {
		seed(store);
		const request = prepareReq(withOpIndex(ops));
		expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
		participant.commitLocal({ transactionId: request.transactionId, transactionTimestamp: T, items: uniqueKeys(request.items) });
		return snapshot(storage);
	});
}

async function expectSameAsReference(ops: Op[]): Promise<void> {
	const reference = await referenceRun(ops);
	expect(reference.locks).toBe(0);
	expect(await singlePartitionRun(ops)).toEqual(reference);
	expect(await twoPhaseRun(ops)).toEqual(reference);
}

/** The sequences of one item. `present` says whether the item exists before the transaction. */
function sequencesOf(key: string, present: boolean): [string, Op[]][] {
	return [
		["put → update", [put(key, { n: 5 }), update(key)]],
		["put → check", [put(key, { n: 5 }), check(key, true)]],
		["delete → put", [del(key), put(key, { n: 7 }, 900)]],
		["put → delete", [put(key, { n: 5 }), del(key)]],
		["put → delete → put", [put(key, { big: "x".repeat(1000) }, 777), del(key), put(key, { n: 9 })]],
		["check → check", [check(key, present), check(key, present)]],
		[
			"a long sequence",
			[
				update(key),
				update(key, 600),
				del(key),
				check(key, false),
				put(key, { n: 2 }),
				update(key),
				check(key, true),
				put(key, { n: 3, s: "y".repeat(500) }),
				update(key),
			],
		],
	];
}

describe("ordered per-item execution: the paths give the result of the reference run", () => {
	it.each(sequencesOf("A", true))("%s on an existing item", async (_name, ops) => {
		await expectSameAsReference(ops);
	});

	it.each(sequencesOf("X", false))("%s on an absent item", async (_name, ops) => {
		await expectSameAsReference(ops);
	});

	it.each(sequencesOf("A", true))("%s on an existing item, with a delete and a put of other items", async (_name, ops) => {
		await expectSameAsReference([del("B"), ...ops, put("C", { c: 1 })]);
	});

	// The keys are in the reverse byte order of the request order, so an apply in key order gives B a
	// different v than an apply in opIndex order.
	it("applies the operations of different items in opIndex order", async () => {
		await expectSameAsReference([put("0-new", { n: 1 }), del("A")]);
		await expectSameAsReference([del("A"), put("0-new", { n: 1 })]);
	});

	it("gives the same result for a request with no repeated item", async () => {
		await expectSameAsReference([put("C", { c: 1 }), update("A"), del("B"), check("X", false)]);
	});
});

describe("ordered per-item execution: evaluate", () => {
	it("reports the first failure of an item, not_evaluated after it, and the outcomes of the other items", async () => {
		const failsOnTemporaryState = conditionTree({ op: "eq", args: [{ ref: "data", path: "$.n" }, { val: 1 }] });
		const ops = withOpIndex([
			put("A", { n: 5 }),
			{ ...check("A", true), condition: failsOnTemporaryState },
			put("A", { n: 6 }),
			put("C", { c: 1 }),
		]);
		const expected = [
			{ outcome: "passed", opIndex: 0 },
			{ outcome: "rejected", opIndex: 1, reason: { code: "condition_failed", hashKey: "A", sortKey: "sk" } },
			{ outcome: "not_evaluated", opIndex: 2 },
			{ outcome: "passed", opIndex: 3 },
		];
		await withPartition(({ participant, store, storage }) => {
			seed(store);
			const before = snapshot(storage);
			expect(participant.executeSingleShot({ items: ops }).response).toEqual({ outcome: "rejected", results: expected });
			expect(snapshot(storage)).toEqual(before);
			expect(participant.prepareLocal(prepareReq(ops))).toEqual({ outcome: "rejected", results: expected });
			expect(snapshot(storage)).toEqual(before);
		});
	});

	it("gives not_evaluated to every later operation of an item whose first operation fails", async () => {
		await withPartition(({ participant, store, storage }) => {
			seed(store);
			const before = snapshot(storage);
			const ops = withOpIndex([check("A", false), del("A"), put("A", { n: 1 })]);
			expect(participant.prepareLocal(prepareReq(ops))).toMatchObject({
				outcome: "rejected",
				results: [
					{ outcome: "rejected", opIndex: 0, reason: { code: "condition_failed" } },
					{ outcome: "not_evaluated", opIndex: 1 },
					{ outcome: "not_evaluated", opIndex: 2 },
				],
			});
			expect(snapshot(storage)).toEqual(before);
		});
	});

	it("evaluates in opIndex order, whatever the order of the request array", async () => {
		await withPartition(({ participant, store }) => {
			seed(store);
			const ops = withOpIndex([del("A"), check("A", false)]).reverse();
			expect(participant.executeSingleShot({ items: ops }).response).toEqual({ outcome: "committed" });
		});
	});

	it("checks the timestamp against the committed stamps, at the first operation whose rule fails", async () => {
		await withPartition(({ participant, store }) => {
			seed(store);
			// A newer read of A: a check orders against last_write_ts and passes, a put orders against
			// last_read_ts and fails.
			store.bumpItemReadTs(kb("A"), SK, T + 1);
			const ops = withOpIndex([check("A", true), put("A", { n: 2 }), check("A", true)]);
			expect(participant.prepareLocal(prepareReq(ops))).toMatchObject({
				outcome: "rejected",
				results: [
					{ outcome: "passed", opIndex: 0 },
					{ outcome: "rejected", opIndex: 1, reason: { code: "timestamp_conflict" } },
					{ outcome: "not_evaluated", opIndex: 2 },
				],
			});
		});
	});

	// A temporary delete of A raises the deletion watermark of the partition to T. The absent item B
	// must still compare with the watermark from before the transaction.
	it.each([
		["A first", [del("A"), put("A", { n: 2 }), put("X", { x: 1 })]],
		["X first", [put("X", { x: 1 }), del("A"), put("A", { n: 2 })]],
	])("commits a delete → put of one item and a put of an absent item (%s)", async (_name, ops) => {
		await expectSameAsReference(ops);
	});

	it("shows the temporary state before the failed operation in an all_old image, and no image after a delete", async () => {
		await withPartition(({ participant, store }) => {
			seed(store);
			const afterPut = withOpIndex([put("A", { n: 42 }), { ...check("A", false), returnValuesOnConditionCheckFailure: "all_old" }]);
			expect(participant.prepareLocal(prepareReq(afterPut))).toMatchObject({
				outcome: "rejected",
				results: [
					{ outcome: "passed" },
					{ outcome: "rejected", reason: { code: "condition_failed", item: { data: '{"n":42}', kind: "json", version: 3 } } },
				],
			});

			const afterDelete = withOpIndex([del("A"), { ...check("A", true), returnValuesOnConditionCheckFailure: "all_old" }]);
			const response = participant.prepareLocal(prepareReq(afterDelete));
			invariant(response.outcome === "rejected");
			expect(response.results[1]).toMatchObject({ outcome: "rejected", reason: { code: "condition_failed" } });
			expect(response.results[1]).not.toHaveProperty("reason.item");
		});
	});

	it.each<[string, Op[], number]>([
		["a single delete", [del("A")], 51],
		["a final delete after writes", [put("A", { n: 5 }), update("A"), del("A")], 53],
	])("returns the same failure image on both paths after %s of another item", async (_name, beforeDelete, version) => {
		await withPartition(({ participant, store, storage }) => {
			for (let i = 0; i < 50; i++) {
				store.upsertItem({ hk: kb("A"), sk: SK, data: '{"n":1}', kind: "json", ttlAt: null, txOrderTs: 1 });
			}
			const before = snapshot(storage);
			const ops = withOpIndex([
				...beforeDelete,
				put("X", { n: 7 }),
				{ ...check("X", false), returnValuesOnConditionCheckFailure: "all_old" },
			]);
			const single = participant.executeSingleShot({ items: ops }).response;
			invariant(single.outcome === "rejected");
			expect(single.results[ops.length - 1]).toMatchObject({
				outcome: "rejected",
				reason: { code: "condition_failed", item: { data: '{"n":7}', kind: "json", version } },
			});
			expect(snapshot(storage)).toEqual(before);
			expect(participant.prepareLocal(prepareReq(ops))).toEqual(single);
			expect(snapshot(storage)).toEqual(before);
			expect(storage.sql.exec(`SELECT COUNT(*) AS n FROM pending_tx_info`).one().n).toBe(0);
		});
	});

	it("fails the operation that makes an intermediate state above MAX_ITEM_BYTES", async () => {
		await withPartition(({ participant, store }) => {
			seed(store);
			const ops = withOpIndex([put("A", { big: "x".repeat(MAX_ITEM_BYTES) }), put("A", { n: 1 })]);
			expect(participant.executeSingleShot({ items: ops }).response).toMatchObject({
				outcome: "rejected",
				results: [{ outcome: "rejected", reason: { code: "item_too_large" } }, { outcome: "not_evaluated" }],
			});
		});
	});

	it("makes no temporary write and runs one storage transaction for a prepare with no repeated item", async () => {
		await withPartition(({ participant, store }) => {
			seed(store);
			const writes = [vi.spyOn(store, "upsertItem"), vi.spyOn(store, "updateItemSingleShot"), vi.spyOn(store, "deleteItem")];
			const blocks = vi.spyOn(store, "transactionSync");
			const ops = withOpIndex([put("C", { c: 1 }), update("A"), del("B")]);
			expect(participant.prepareLocal(prepareReq(ops))).toEqual({ outcome: "accepted" });
			expect(writes.map((w) => w.mock.calls.length)).toEqual([0, 0, 0]);
			expect(blocks).toHaveBeenCalledTimes(1);
		});
	});

	it("rolls back the evaluate block of a repeated prepare and writes the locks in a second block", async () => {
		await withPartition(({ participant, store, storage }) => {
			seed(store);
			const before = snapshot(storage);
			const blocks = vi.spyOn(store, "transactionSync");
			const ops = withOpIndex([put("A", { n: 5 }), update("A"), del("B")]);
			expect(participant.prepareLocal(prepareReq(ops))).toEqual({ outcome: "accepted" });
			expect(blocks).toHaveBeenCalledTimes(2);
			expect(snapshot(storage)).toEqual({ ...before, locks: 2 });
		});
	});

	// A: put → update → check, where the check is last and is not the last write. B and C have one
	// operation each. X: put → delete, where the delete is last. Only the put of A, the update of A,
	// and the put of X have a later reader.
	it("makes temporary writes in a prepare for later readers and every delete", async () => {
		const ops = [put("A", { n: 5 }), update("A"), check("A", true), del("B"), put("C", { c: 1 }), put("X", { x: 1 }), del("X")];
		await withPartition(({ participant, store }) => {
			seed(store);
			const writes = [
				vi.spyOn(store, "upsertItem"),
				vi.spyOn(store, "updateItemSingleShot"),
				vi.spyOn(store, "deleteItem"),
				vi.spyOn(store, "bumpItemReadTs"),
			];
			expect(participant.prepareLocal(prepareReq(withOpIndex(ops)))).toEqual({ outcome: "accepted" });
			expect(writes.map((w) => w.mock.calls.length)).toEqual([2, 1, 2, 0]);
		});
		await expectSameAsReference(ops);
	});

	it.each([
		["present", "A", true],
		["absent", "X", false],
	] as const)("uses one prepare block for check-only sequences when the item is %s", async (_name, key, present) => {
		const ops = [check(key, present), check(key, present)];
		const reference = await referenceRun(ops);
		expect(await singlePartitionRun(ops)).toEqual(reference);
		await withPartition(({ participant, store, storage }) => {
			seed(store);
			const before = snapshot(storage);
			const checks = vi.spyOn(store, "bumpItemReadTs");
			const blocks = vi.spyOn(store, "transactionSync");
			const request = prepareReq(withOpIndex(ops));
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			expect(checks).not.toHaveBeenCalled();
			expect(blocks).toHaveBeenCalledTimes(1);
			expect(snapshot(storage)).toEqual({ ...before, locks: 1 });
			const locks = store.listPendingTxItems(request.transactionId);
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			expect(checks).not.toHaveBeenCalled();
			expect(blocks).toHaveBeenCalledTimes(2);
			expect(store.listPendingTxItems(request.transactionId)).toEqual(locks);
			const commit = { transactionId: request.transactionId, transactionTimestamp: T, items: uniqueKeys(request.items) };
			participant.commitLocal(commit);
			expect(checks).toHaveBeenCalledTimes(2);
			expect(snapshot(storage)).toEqual(reference);
			participant.commitLocal(commit);
			expect(checks).toHaveBeenCalledTimes(2);
			expect(snapshot(storage)).toEqual(reference);
		});
	});

	it.each<[string, Op[], number]>([
		["check → update → check", [check("A", true), update("A"), check("A", true)], 2],
		["check → delete", [check("A", true), del("A")], 1],
	])("skips temporary check writes in %s, but applies every check at commit", async (_name, ops, checkWrites) => {
		const reference = await referenceRun(ops);
		expect(await singlePartitionRun(ops)).toEqual(reference);
		await withPartition(({ participant, store, storage }) => {
			seed(store);
			const before = snapshot(storage);
			const checks = vi.spyOn(store, "bumpItemReadTs");
			const request = prepareReq(withOpIndex(ops));
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			expect(checks).not.toHaveBeenCalled();
			expect(snapshot(storage)).toEqual({ ...before, locks: 1 });
			participant.commitLocal({ transactionId: request.transactionId, transactionTimestamp: T, items: uniqueKeys(request.items) });
			expect(checks).toHaveBeenCalledTimes(checkWrites);
			expect(snapshot(storage)).toEqual(reference);
		});
	});

	it("reports no growth of a rolled-back block, and one candidate for each item whose final state is present", async () => {
		await withPartition(({ participant, store }) => {
			seed(store);
			const rejected = participant.executeSingleShot({
				items: withOpIndex([put("A", { n: 5 }), update("A"), put("C", { c: 1 }), check("X", true)]),
			});
			expect(rejected).toMatchObject({ response: { outcome: "rejected" }, promotionCandidates: [] });

			const committed = participant.executeSingleShot({
				items: withOpIndex([put("A", { n: 5 }), update("A"), put("C", { c: 1 }), del("B")]),
			});
			expect(committed.response).toEqual({ outcome: "committed" });
			expect(committed.promotionCandidates.map((c) => KeyCodec.decode(c.hashKey))).toEqual(["A", "C"]);
		});
	});

	it("copies the last-write data on the two-phase path only", async () => {
		const ops = withOpIndex([put("A", { n: 5 }), update("A"), put("C", { c: 1 })]);
		await withPartition(({ participant, store }) => {
			seed(store);
			const reads = vi.spyOn(store, "readItemData");
			expect(participant.executeSingleShot({ items: ops }).response).toEqual({ outcome: "committed" });
			expect(reads).not.toHaveBeenCalled();
		});
		await withPartition(({ participant, store }) => {
			seed(store);
			const reads = vi.spyOn(store, "readItemData");
			expect(participant.prepareLocal(prepareReq(ops))).toEqual({ outcome: "accepted" });
			expect(reads).toHaveBeenCalledTimes(1);
		});
	});
});

describe("ordered per-item execution: versions", () => {
	it("gives a row that the transaction creates a v above the max_deleted_v of the commit", async () => {
		await withPartition(({ participant, store, storage }) => {
			seed(store);
			const request = prepareReq(withOpIndex([put("X", { n: 1 }), update("X"), check("X", true)]));
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });

			// A delete of another item between prepare and commit raises max_deleted_v to 50.
			storage.sql.exec(`UPDATE items SET v = 50 WHERE hk = ?`, kb("B"));
			store.deleteItem({ hk: kb("B"), sk: SK, txOrderTs: 10 });
			expect(store.getMaxDeletedV()).toBe(50);

			participant.commitLocal({ transactionId: request.transactionId, transactionTimestamp: T, items: uniqueKeys(request.items) });
			const row = store.getItem(kb("X"), SK).row;
			expect(row?.v).toBe(52);
			expect(JSON.parse(row?.data as string)).toEqual({ n: 2 });
		});
	});

	it("refuses a plan that reads v after a write of its item, on both paths, and writes nothing", async () => {
		const setPrevVersion = updateTree([{ action: "set", target: { ref: "data", path: "$.prevVersion" }, value: { ref: "v" } }]);
		const ops = withOpIndex([put("X", { n: 1 }), { ...update("X"), update: setPrevVersion }]);
		await withPartition(({ participant, store, storage }) => {
			seed(store);
			const before = snapshot(storage);
			const refusal = fokosErrorWith("transact_version_after_write", { opIndex: 1, earlierOpIndex: 0, hashKey: "X", sortKey: "sk" });
			expect(() => participant.prepareLocal(prepareReq(ops))).toThrow(refusal);
			// The refusal comes before the clock-skew answer.
			const skewed = (BASE_NOW + DEFAULT_PARTITION_CONFIG.maxClockSkewMs + 1000) * TX_ORDER_TS_UNITS_PER_MS;
			expect(() => participant.prepareLocal({ ...prepareReq(ops), transactionTimestamp: skewed })).toThrow(refusal);
			expect(() => participant.executeSingleShot({ items: ops })).toThrow(refusal);
			expect(snapshot(storage)).toEqual(before);
			expect(storage.sql.exec(`SELECT COUNT(*) AS n FROM pending_tx_info`).one().n).toBe(0);
		});
	});
});

describe("ordered per-item execution: lock rows and commit", () => {
	it("stores the operation list and the stored JSONB of the last write, and commit writes the same bytes", async () => {
		await withPartition(({ participant, store, storage }) => {
			seed(store);
			const request = prepareReq(withOpIndex([update("A"), check("A", true), update("A", 800), check("A", true)]));
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });

			const [lock] = store.listPendingTxItems(request.transactionId);
			expect(lock).toMatchObject({
				operation: "update",
				op_list: [
					[0, "update"],
					[1, "check"],
					[2, "update"],
					[3, "check"],
				],
				kind: "json",
				ttl_epoch_utc_seconds: 800,
			});
			expect(lock.data).toBeInstanceOf(Uint8Array);

			participant.commitLocal({ transactionId: request.transactionId, transactionTimestamp: T, items: uniqueKeys(request.items) });
			const stored = storage.sql.exec<{ data: ArrayBuffer; v: number }>(`SELECT data, v FROM items WHERE hk = ?`, kb("A")).one();
			expect(new Uint8Array(stored.data)).toEqual(lock.data);
			expect(stored.v).toBe(4);
			expect(JSON.parse(store.getItem(kb("A"), SK).row?.data as string)).toEqual({ n: 3, a: true });
		});
	});

	it("stores no data for a sequence with no write", async () => {
		await withPartition(({ participant, store }) => {
			seed(store);
			const request = prepareReq(withOpIndex([check("A", true), del("A"), check("A", false)]));
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			expect(store.listPendingTxItems(request.transactionId)).toMatchObject([
				{
					operation: "delete",
					data: null,
					kind: null,
					op_list: [
						[0, "check"],
						[1, "delete"],
						[2, "check"],
					],
				},
			]);
		});
	});

	it("refuses a commit request that names one key two times, and changes no row", async () => {
		await withPartition(({ participant, store, storage }) => {
			seed(store);
			const request = prepareReq(withOpIndex([put("A", { n: 5 }), update("A")]));
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			const before = snapshot(storage);
			const key = { hashKey: kb("A"), sortKey: SK };
			expect(() => participant.commitLocal({ transactionId: request.transactionId, transactionTimestamp: T, items: [key, key] })).toThrow(
				invariantFailure(/two times/),
			);
			expect(snapshot(storage)).toEqual(before);
		});
	});

	it("refuses lock rows that hold one opIndex two times, and changes no row", async () => {
		await withPartition(({ participant, store, storage }) => {
			seed(store);
			const tx = {
				transaction_id: "tx-dup",
				transaction_ts: T,
				coordinator_json: JSON.stringify(testCoordinatorRef("tok-dup")),
				created_at: BASE_NOW,
				guarded_at: null,
				next_recovery_at: BASE_NOW + 1000,
			};
			for (const key of ["A", "C"]) {
				store.insertPendingLock({
					...tx,
					hk: kb(key),
					sk: SK,
					operation: "put",
					op_list: [[0, "put"]],
					data: "v",
					kind: "text",
					ttl_epoch_utc_seconds: null,
				});
			}
			const before = snapshot(storage);
			const items = [kb("A"), kb("C")].map((hashKey) => ({ hashKey, sortKey: SK }));
			expect(() => participant.commitLocal({ transactionId: "tx-dup", transactionTimestamp: T, items })).toThrow(
				invariantFailure(/two times/),
			);
			expect(snapshot(storage)).toEqual(before);
		});
	});

	it("answers a repeated prepare with accepted, changes no lock row, and commits once", async () => {
		await withPartition(({ participant, store, storage }) => {
			seed(store);
			const request = prepareReq(withOpIndex([put("A", { n: 5 }), update("A"), del("B"), put("B", { b: 2 })]));
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			const locks = store.listPendingTxItems(request.transactionId);
			const info = storage.sql.exec(`SELECT * FROM pending_tx_info`).toArray();
			const blocks = vi.spyOn(store, "transactionSync");

			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			expect(blocks).toHaveBeenCalledTimes(1);
			expect(store.listPendingTxItems(request.transactionId)).toEqual(locks);
			expect(storage.sql.exec(`SELECT * FROM pending_tx_info`).toArray()).toEqual(info);

			const commit = { transactionId: request.transactionId, transactionTimestamp: T, items: uniqueKeys(request.items) };
			participant.commitLocal(commit);
			const after = snapshot(storage);
			// A repeated commit finds no lock row and applies nothing.
			participant.commitLocal(commit);
			expect(snapshot(storage)).toEqual(after);
			expect(JSON.parse(store.getItem(kb("A"), SK).row?.data as string)).toEqual({ n: 6 });
		});
	});

	it("warns when this transaction holds the locks of only some items of a repeated prepare", async () => {
		await withPartition(({ participant, store }) => {
			seed(store);
			const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
			const request = prepareReq(withOpIndex([put("A", { n: 5 }), update("A"), put("C", { c: 1 })]));
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			expect(warn).not.toHaveBeenCalled();

			store.deletePendingTxKeys(request.transactionId, [{ hashKey: kb("C"), sortKey: SK }]);
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			expect(warn).toHaveBeenCalledTimes(1);
			expect(warn.mock.calls[0][0]).toMatchObject({ transactionId: request.transactionId, lockedItems: 1, items: 2, outcome: "accepted" });
			expect(store.listPendingTxItems(request.transactionId)).toHaveLength(2);
		});
	});

	/** Prepares and commits `ops`, then sends the same prepare again. */
	function latePrepare(ops: Op[]) {
		return withPartition(({ participant, store }) => {
			seed(store);
			const request = prepareReq(withOpIndex(ops));
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			participant.commitLocal({ transactionId: request.transactionId, transactionTimestamp: T, items: uniqueKeys(request.items) });
			return participant.prepareLocal(request);
		});
	}

	it.each([
		["put → update", [put("A", { n: 5 }), update("A")]],
		["delete → put", [del("A"), put("A", { n: 1 })]],
		["put → delete", [put("X", { n: 1 }), del("X")]],
	])("rejects a late prepare of a committed %s", async (_name, ops) => {
		expect(await latePrepare(ops)).toMatchObject({
			outcome: "rejected",
			results: [{ outcome: "rejected", reason: { code: "timestamp_conflict" } }, { outcome: "not_evaluated" }],
		});
	});

	it.each([
		["a present item", [check("A", true), check("A", true)]],
		["an absent item", [check("X", false), check("X", false)]],
	])("accepts a late prepare of a committed check → check on %s", async (_name, ops) => {
		expect(await latePrepare(ops)).toEqual({ outcome: "accepted" });
	});

	// Above 8 keys the hash part of KeyPairMap answers. Each key must stay its own item at each site.
	it("keeps more than 8 keys that differ only in their last byte apart in prepare and commit", async () => {
		await withPartition(({ participant, store }) => {
			const keys = Array.from({ length: 12 }, (_, i) => KeyCodec.encode(new Uint8Array([9, 9, 9, i])));
			const ops: Op[] = keys.map((sortKey, i) => ({ hashKey: kb("H"), sortKey, operation: "put", data: `v-${i}`, kind: "text" }));
			const request = prepareReq(withOpIndex(ops));
			expect(participant.prepareLocal(request)).toEqual({ outcome: "accepted" });
			expect(store.listPendingTxKeys(request.transactionId)).toHaveLength(12);
			participant.commitLocal({ transactionId: request.transactionId, transactionTimestamp: T, items: uniqueKeys(request.items) });
			keys.forEach((sortKey, i) => expect(store.getItem(kb("H"), sortKey).row?.data).toBe(`v-${i}`));
		});
	});
});

function invariant(condition: boolean): asserts condition {
	expect(condition).toBe(true);
}
