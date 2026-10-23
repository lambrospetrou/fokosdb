import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import type { PartitionDO } from "../../src/server/do-partition.js";
import type { MaterializationDO } from "../../src/server/materialization/do-materialization.js";
import { testCoordinatorRef } from "../stub-helpers.js";
import { conditionTree, kb, latestImage, makeStub, materializationOf, withOpIndex } from "./helpers.js";

async function outboxSize(stub: DurableObjectStub<PartitionDO>): Promise<number> {
	return await runInDurableObject(
		stub,
		(_instance: PartitionDO, state: DurableObjectState) =>
			state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM materialization_outbox`).one().n,
	);
}

describe("PartitionDO — materialization export", () => {
	it("exports each change to the materialization partition of its key, and empties the outbox", async () => {
		// The push target refuses each item, so its cursor stays, and the tombstone stays for the read.
		const { ctx, stub, rpc } = makeStub({ captureChanges: true, nsMat: "PUSH_MATERIALIZATION_DO" });
		await env.PUSH_SINK_DO.getByName("sink").refuse(materializationOf(ctx, "a").id.toString());
		await rpc.apiPutItem(ctx, { hashKey: kb("a"), sortKey: kb("s"), data: "one", kind: "text" });
		await rpc.apiPutItem(ctx, { hashKey: kb("a"), sortKey: kb("s"), data: "two", kind: "text" });
		await rpc.apiPutItem(ctx, { hashKey: kb("b"), sortKey: kb("s"), data: JSON.stringify({ n: 1 }), kind: "json" });
		await vi.waitFor(async () => expect(await outboxSize(stub)).toBe(0));

		const mat = materializationOf(ctx, "a");
		expect(await latestImage(mat, "a")).toEqual({ revision: 2, deleted: false, data: "two" });
		// A json item travels as its stored JSONB blob.
		expect(await latestImage(materializationOf(ctx, "b"), "b")).toMatchObject({ revision: 1, deleted: false });

		// The export of the delete is a tombstone at the delete revision.
		await rpc.apiDeleteItem(ctx, { hashKey: kb("a"), sortKey: kb("s") });
		await vi.waitFor(async () => expect(await latestImage(mat, "a")).toEqual({ revision: 3, deleted: true, data: null }));
		await vi.waitFor(async () => expect(await outboxSize(stub)).toBe(0));

		// The receiver stored the export epoch of the base partition.
		const producerId = stub.id.toString();
		const epoch = await runInDurableObject(
			mat,
			(_instance: MaterializationDO, state: DurableObjectState) =>
				state.storage.sql.exec<{ epoch: number }>(`SELECT epoch FROM producer_fences WHERE producer_id = ?`, producerId).toArray()[0]
					?.epoch,
		);
		expect(epoch).toBe(1);
		const exportState = await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) =>
			state.storage.sql.exec(`SELECT epoch, is_call_open FROM materialization_export_state`).one(),
		);
		expect(exportState).toEqual({ epoch: 1, is_call_open: 0 });
	});

	it("exports an update and a delete, and nothing of a failed condition", async () => {
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
		const rejected = await rpc.apiPutItem(ctx, {
			hashKey: kb("a"),
			sortKey: kb("s"),
			data: "other",
			kind: "text",
			condition: conditionTree({ op: "not_exists", args: [{ ref: "hashKey" }] }),
		});
		expect(rejected).toMatchObject({ outcome: "rejected" });
		await rpc.apiDeleteItem(ctx, { hashKey: kb("a"), sortKey: kb("s") });
		await vi.waitFor(async () => expect(await outboxSize(stub)).toBe(0));

		expect(await latestImage(materializationOf(ctx, "b"), "b")).toMatchObject({ revision: 1, deleted: false });
		// The delete of a at v = 1 has the revision 2. A delete before the first export of a sends
		// nothing, so the partition holds the tombstone or no row. It never holds a live image of a.
		const a = await latestImage(materializationOf(ctx, "a"), "a");
		expect(a === undefined || (a.deleted && a.revision === 2)).toBe(true);
	});

	it("exports the revision that a commit applies, and nothing of a cancelled transaction", async () => {
		const { ctx, stub, rpc } = makeStub({ captureChanges: true });
		const coordinator = testCoordinatorRef();
		const prepare = async (transactionId: string, transactionTimestamp: number, data: string) =>
			await rpc.txPrepare(ctx, {
				transactionId,
				transactionTimestamp,
				coordinator,
				items: withOpIndex([{ hashKey: kb("a"), sortKey: kb("s"), operation: "put", data, kind: "text" }]),
			});
		const keys = [{ hashKey: kb("a"), sortKey: kb("s") }];

		const cancelled = crypto.randomUUID();
		expect(await prepare(cancelled, Date.now(), "cancelled")).toMatchObject({ outcome: "accepted" });
		expect(await outboxSize(stub)).toBe(0);
		await rpc.txCancel(ctx, { transactionId: cancelled, items: keys });
		expect(await outboxSize(stub)).toBe(0);

		const committed = crypto.randomUUID();
		const transactionTimestamp = Date.now() + 1;
		expect(await prepare(committed, transactionTimestamp, "committed")).toMatchObject({ outcome: "accepted" });
		await rpc.txCommit(ctx, { transactionId: committed, transactionTimestamp, items: keys });
		await vi.waitFor(async () =>
			expect(await latestImage(materializationOf(ctx, "a"), "a")).toEqual({ revision: 1, deleted: false, data: "committed" }),
		);
	});

	it("a table without capture sends nothing", async () => {
		const { ctx, stub, rpc } = makeStub();
		await rpc.apiPutItem(ctx, { hashKey: kb("a"), sortKey: kb("s"), data: "one", kind: "text" });
		expect(await outboxSize(stub)).toBe(0);
		const exportState = await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) =>
			state.storage.sql.exec(`SELECT epoch, is_call_open FROM materialization_export_state`).one(),
		);
		expect(exportState).toEqual({ epoch: 1, is_call_open: 0 });
	});
});
