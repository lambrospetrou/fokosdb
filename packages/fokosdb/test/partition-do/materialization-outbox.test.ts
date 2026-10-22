import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { PartitionDO } from "../../src/server/do-partition.js";
import { KeyCodec, type KeyBytes } from "../../src/sharding/key-codec.js";
import { testCoordinatorRef } from "../stub-helpers.js";
import { conditionTree, kb, makeStub, withOpIndex } from "./helpers.js";
import { PROMOTION_TEST_MAX_SIZE_MB, TestPartition, drainUntil, makePartition } from "./partition-harness.js";

type OutboxRow = { hk: string; revision: number; is_deleted: number; is_local_only: number };

async function outboxRows(stub: DurableObjectStub<PartitionDO>, hashKey?: string): Promise<OutboxRow[]> {
	return await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) =>
		state.storage.sql
			.exec<{ hk: ArrayBuffer; revision: number; is_deleted: number; is_local_only: number }>(
				`SELECT hk, revision, is_deleted, is_local_only FROM materialization_outbox ${hashKey === undefined ? "" : "WHERE hk = ?"} ORDER BY hk, sk`,
				...(hashKey === undefined ? [] : [kb(hashKey)]),
			)
			.toArray()
			.map((row) => ({ ...row, hk: KeyCodec.decode(new Uint8Array(row.hk) as KeyBytes) as string })),
	);
}

describe("PartitionDO — outbox capture", () => {
	it("a table without capture writes no outbox row", async () => {
		const { ctx, stub, rpc } = makeStub();
		await rpc.apiPutItem(ctx, { hashKey: kb("a"), sortKey: kb("s"), data: "d", kind: "text" });
		await rpc.apiDeleteItem(ctx, { hashKey: kb("a"), sortKey: kb("s") });
		expect(await outboxRows(stub)).toEqual([]);
	});

	it("captures put, update and delete, and writes nothing for a failed condition", async () => {
		const { ctx, stub, rpc } = makeStub({ captureChanges: true });
		await rpc.apiPutItem(ctx, { hashKey: kb("a"), sortKey: kb("s"), data: "d", kind: "text" });
		await rpc.txExecuteSingleShot(ctx, {
			items: withOpIndex([
				{
					hashKey: kb("b"),
					sortKey: kb("s"),
					operation: "update",
					update: [{ action: "set", target: { ref: "data", path: "$.x" }, value: { val: 1 } }],
				},
			]),
		});
		expect(await outboxRows(stub)).toEqual([
			{ hk: "a", revision: 1, is_deleted: 0, is_local_only: 1 },
			{ hk: "b", revision: 1, is_deleted: 0, is_local_only: 1 },
		]);

		const rejected = await rpc.apiPutItem(ctx, {
			hashKey: kb("a"),
			sortKey: kb("s"),
			data: "other",
			kind: "text",
			condition: conditionTree({ op: "not_exists", args: [{ ref: "hashKey" }] }),
		});
		expect(rejected).toMatchObject({ outcome: "rejected" });
		expect(await outboxRows(stub, "a")).toEqual([{ hk: "a", revision: 1, is_deleted: 0, is_local_only: 1 }]);

		// A delete before any export removes the local only row, and sends nothing.
		await rpc.apiDeleteItem(ctx, { hashKey: kb("a"), sortKey: kb("s") });
		expect(await outboxRows(stub, "a")).toEqual([]);
	});

	it("a prepare and a cancel write no outbox row, and a commit writes the revision it applies", async () => {
		const { ctx, stub, rpc } = makeStub({ captureChanges: true });
		const coordinator = testCoordinatorRef();
		const prepare = async (transactionId: string, transactionTimestamp: number) =>
			await rpc.txPrepare(ctx, {
				transactionId,
				transactionTimestamp,
				coordinator,
				items: withOpIndex([{ hashKey: kb("a"), sortKey: kb("s"), operation: "put", data: "tx", kind: "text" }]),
			});
		const keys = [{ hashKey: kb("a"), sortKey: kb("s") }];

		const cancelled = crypto.randomUUID();
		expect(await prepare(cancelled, Date.now())).toMatchObject({ outcome: "accepted" });
		expect(await outboxRows(stub)).toEqual([]);
		await rpc.txCancel(ctx, { transactionId: cancelled, items: keys });
		expect(await outboxRows(stub)).toEqual([]);

		const committed = crypto.randomUUID();
		const transactionTimestamp = Date.now() + 1;
		expect(await prepare(committed, transactionTimestamp)).toMatchObject({ outcome: "accepted" });
		await rpc.txCommit(ctx, { transactionId: committed, transactionTimestamp, items: keys });
		expect(await outboxRows(stub)).toEqual([{ hk: "a", revision: 1, is_deleted: 0, is_local_only: 1 }]);
	});
});

describe("PartitionDO — outbox migration", () => {
	it("a hash split copies the outbox rows to the children as not local only, and the cleanup deletes the copies", async () => {
		const partition = makePartition({ captureChanges: true, hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		await partition.triggerHashSplit();
		await partition.awaitSplitCompleted();
		const children = await partition.children();

		await drainUntil([partition], async () => (await outboxRows(partition.stub)).length === 0, "source cleanup of the outbox copies");
		const copied = (await Promise.all(children.map(async (child) => await outboxRows(child.stub)))).flat();
		// Each filler write made one row. The filler keys are distinct, so the children hold one row for each.
		expect(copied.length).toBeGreaterThan(0);
		expect(new Set(copied.map((row) => row.hk)).size).toBe(copied.length);
		expect(copied.every((row) => row.is_local_only === 0 && row.is_deleted === 0)).toBe(true);
	});

	it("a promotion copies the outbox rows of the key to the range root, and the cleanup deletes them on the source", async () => {
		const partition = makePartition({ captureChanges: true, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		await partition.put({ hashKey: kb("bob"), sortKey: kb("s"), data: "b", kind: "text" });
		const written = await partition.triggerPromotion("alice");
		const root: TestPartition = await partition.awaitPromoted("alice");

		const copied = await outboxRows(root.stub);
		expect(copied).toHaveLength(written.length);
		expect(copied.every((row) => row.hk === "alice" && row.is_local_only === 0)).toBe(true);

		await drainUntil([partition], async () => (await outboxRows(partition.stub, "alice")).length === 0, "source cleanup of alice");
		// The source keeps the rows of the keys that it still owns.
		expect(await outboxRows(partition.stub)).toEqual([{ hk: "bob", revision: 1, is_deleted: 0, is_local_only: 1 }]);
	});
});
