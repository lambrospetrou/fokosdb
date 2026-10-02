/**
 * `fokosPrepareDestroy`, `fokosStatus` and `fokosPromotions` on a real `PartitionDO`.
 *
 * A destroy traversal makes these RPCs before it deletes anything. The fence stops every background
 * transition. The status gives the split targets, and the paginated promotions listing gives the range
 * roots. Together they tell the traversal which partitions exist below this one. The router suite
 * covers the traversal order.
 */
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, it, vi } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import { testPartitionStub } from "../stub-helpers.js";
import { FOKOS_KV_KEYS } from "../../src/sharding/sharding-store.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { kb, makeStub } from "./helpers.js";
import { CONTROLLED_NS, makePartition, TestPartition } from "./partition-harness.js";

/** Every entry of every promotions page, so a test reads the whole listing the traversal would walk. */
async function allPromotionEntries(partition: TestPartition) {
	const entries = [];
	let cursor = null;
	do {
		const page = await partition.stub.fokosPromotions({ cursor });
		entries.push(...page.entries);
		cursor = page.nextCursor;
	} while (cursor !== null);
	return entries;
}

describe.concurrent("PartitionDO — fokosStatus", () => {
	it("bootstraps an empty root from its context and reports no repartition", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const page = await stub.fokosStatus({ rootContext: ctx });

		expect(page).toMatchObject({ initialized: true, destroying: false, importState: null, split: [] });
		expect(await stub.fokosPromotions({ cursor: null })).toEqual({ entries: [], nextCursor: null });
		expect(page.ref).toEqual({ partitionId: ctx.partitionId, doName: ctx.doName });
	});

	it("reports an uninitialized target as a leaf and does not bring it to life", async ({ expect }) => {
		const { ctx } = makeStub();
		const targetName = `test.fokosstatus-uninitialized.${crypto.randomUUID()}`;
		const stub = testPartitionStub(targetName);

		const page = await stub.fokosStatus({});

		expect(page).toEqual({
			initialized: false,
			destroying: false,
			ref: null,
			role: null,
			importState: null,
			split: [],
		});
		expect(await stub.fokosPromotions({ cursor: null })).toEqual({ entries: [], nextCursor: null });
		// A target request carries no context, so the partition must still have none of its own.
		await runInDurableObject(stub, (_i: PartitionDO, state: DurableObjectState) => {
			expect(state.storage.kv.get(FOKOS_KV_KEYS.IDENTITY)).toBeUndefined();
		});
		expect(ctx.doName).not.toBe(targetName);
	});

	it("reports every target of a split, with its initialization state and its import state", async ({ expect }) => {
		const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		await partition.triggerHashSplit();
		await partition.awaitSplitCompleted();

		const entries = (await partition.stub.fokosStatus({})).split;
		expect(entries).toHaveLength(2);
		const children = await partition.children();
		expect(entries.map((e) => e.target?.ref.doName)).toEqual(children.map((c) => c.doName));
		for (const entry of entries) {
			expect(entry.repartition.kind).toBe("hash_split");
			expect(entry.target).toMatchObject({ initialization: "initialized", acknowledged: true });
			expect(entry.target?.ref.partitionId).toBeTruthy();
		}

		// A child reports the same links from its own side: no repartition of its own, and the import it
		// finished. That answer tells the traversal the child is a leaf.
		const childPage = await children[0].stub.fokosStatus({});
		expect(childPage).toMatchObject({ initialized: true, split: [], importState: "active" });
	});

	it("reports the range root a promotion created, so destroy reaches a tree no context names", async ({ expect }) => {
		const partition = makePartition({ hashSplitN: 2 });
		await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("alice") });
		const rangeRoot = await partition.awaitPromoted("alice");

		expect((await partition.stub.fokosStatus({})).split).toEqual([]);
		const entries = await allPromotionEntries(partition);
		expect(entries).toHaveLength(1);
		expect(entries[0].repartition.kind).toBe("key_promotion");
		expect(entries[0].repartition.hashKey).toEqual(kb("alice"));
		expect(entries[0].target?.ref.doName).toBe(rangeRoot.doName);
	});

	it("rejects a root context that does not match the partition", async ({ expect }) => {
		const partition = makePartition();
		await partition.put({ hashKey: kb("hk"), sortKey: kb("sk"), data: "v", kind: "text" });
		const { ctx: otherCtx } = makeStub();

		await runInDurableObject(partition.stub, async (instance: PartitionDO) => {
			await expect(instance.fokosStatus({ rootContext: otherCtx })).rejects.toThrow(fokosErrorWith("partition_context_mismatch"));
		});
	});
});

describe("PartitionDO — fokosPrepareDestroy", () => {
	it("sets the fence, cancels the alarm, and adds no target when the DO wakes", async ({ expect }) => {
		const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		await partition.triggerHashSplit();

		await partition.stub.fokosPrepareDestroy({});

		const fenced = await partition.stub.fokosStatus({});
		expect(fenced.destroying).toBe(true);
		// The fence is durable, so nothing the DO wakes up to do can move the state on.
		await runInDurableObject(partition.stub, async (_i: PartitionDO, state: DurableObjectState) => {
			expect(await state.storage.getAlarm()).toBeNull();
		});
		expect(await runDurableObjectAlarm(partition.stub)).toBe(false);
		await runInDurableObject(partition.stub, async (instance: PartitionDO) => {
			await instance.alarm({ retryCount: 0, isRetry: false, scheduledTime: Date.now() });
		});

		// The traversal walks the target links, so the fence must hold that SET still. Only a planning
		// step adds a target row, and the fence stops that step. The children are a different matter.
		// They live until the traversal reaches them, which it does before this partition, and a child
		// that finishes its import acknowledges here. That acknowledgement moves the split state and one
		// flag. It adds no link, so it cannot make the traversal miss a partition.
		const after = await partition.stub.fokosStatus({});
		expect(after.destroying).toBe(true);
		expect(after.split.map((e) => e.target?.ref)).toEqual(fenced.split.map((e) => e.target?.ref));
	});

	it("rejects every other RPC once fenced, while the destroy traversal calls still answer", async ({ expect }) => {
		const partition = makePartition();
		await partition.put({ hashKey: kb("hk"), sortKey: kb("sk"), data: "v", kind: "text" });

		await partition.stub.fokosPrepareDestroy({});

		await runInDurableObject(partition.stub, async (instance: PartitionDO) => {
			await expect(instance.apiPutItem(partition.ctx, { hashKey: kb("hk"), sortKey: kb("sk2"), data: "v", kind: "text" })).rejects.toThrow(
				fokosErrorWith("partition_migrating"),
			);
			await expect(instance.fokosMigrationAck({ repartitionId: "r1", target: { partitionId: "00", doName: "nobody" } })).rejects.toThrow(
				fokosErrorWith("partition_migrating"),
			);
		});

		const page = await partition.stub.fokosStatus({});
		expect(page.destroying).toBe(true);
		expect(await partition.stub.fokosPromotions({ cursor: null })).toEqual({ entries: [], nextCursor: null });
		expect(await partition.promotedKeyStatus("hk")).toBeUndefined();
	});

	it("is idempotent, and a repeated call leaves no alarm behind", async ({ expect }) => {
		const partition = makePartition();
		await partition.put({ hashKey: kb("hk"), sortKey: kb("sk"), data: "v", kind: "text" });

		await expect(partition.stub.fokosPrepareDestroy({})).resolves.toBeUndefined();
		await expect(partition.stub.fokosPrepareDestroy({})).resolves.toBeUndefined();

		await runInDurableObject(partition.stub, async (_i: PartitionDO, state: DurableObjectState) => {
			expect(await state.storage.getAlarm()).toBeNull();
		});
	});

	it("bootstraps a root from its context, so an empty database can still be destroyed", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.fokosPrepareDestroy({ rootContext: ctx });

		const page = await stub.fokosStatus({});
		expect(page).toMatchObject({ initialized: true, destroying: true });
	});

	it("waits for the target RPC a source step is parked in, and lets no transition follow it", async ({ expect }) => {
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		const children = partition.hashChildren().map((c) => c.controlled);
		for (const child of children) {
			await child.testHoldInit();
		}
		const release = async () => {
			for (const child of children) {
				await child.testReleaseInit();
			}
		};
		const initCalls = async () => (await Promise.all(children.map((c) => c.testInitCalls()))).reduce((a, b) => a + b, 0);

		try {
			await partition.triggerHashSplit();
			// The background pass of the parent is now inside the child call and cannot take its next step.
			await vi.waitFor(async () => expect(await initCalls()).toBeGreaterThan(0), { timeout: 5000, interval: 10 });

			let settled = false;
			const prepare = partition.stub.fokosPrepareDestroy({}).then(() => void (settled = true));
			// Every chance to settle, which it must not take, because the fence waits for the parked pass.
			// guard: allow-timer. A negative check: no state change can show that the call is still blocked.
			await new Promise((resolve) => setTimeout(resolve, 100));
			expect(settled, "fokosPrepareDestroy returned while a source step was still in flight").toBe(false);

			await release();
			await prepare;
		} finally {
			await release();
		}

		// The parked step recorded its own result, and nothing ran after it. The split never cut over.
		const after = await partition.stub.fokosStatus({});
		expect(after.destroying).toBe(true);
		expect(after.split.every((e) => e.repartition.state === "queued" || e.repartition.state === "planned")).toBe(true);
		await runInDurableObject(partition.stub, async (_i: PartitionDO, state: DurableObjectState) => {
			expect(await state.storage.getAlarm()).toBeNull();
		});
	});
});
