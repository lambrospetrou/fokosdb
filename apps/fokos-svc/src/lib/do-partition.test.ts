import { env } from "cloudflare:workers";
import { listDurableObjectIds, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, it, vi } from "vitest";
import type { InitFromSplitOptions, PartitionDO } from "./do-partition.js";
import { PartitionContextCreator } from "./partition-topology.js";
import type { PartitionContextResolved } from "./partition-topology.js";

function makeStub(opts?: Partial<Parameters<typeof PartitionContextCreator.create>[0]>) {
	const name = `test.partition.${crypto.randomUUID()}`;
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsPrefix: "test",
		rootTreesN: 1,
		hashSplitConditions: { splitN: 2, maxSizeMb: 100 },
		rangeSplitConditions: { splitN: 2, maxSizeMb: 500 },
		...opts,
	});
	const id = env.PARTITION_DO.idFromName(name);
	const ctx: PartitionContextResolved = {
		...base,
		doName: name,
		primaryDoIdStr: id.toString(),
	};
	return { ctx, stub: env.PARTITION_DO.get(id) };
}

describe("PartitionDO - putItem / getItem", () => {
	it("returns found:false for a missing key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const result = await stub.getItem(ctx, { hashKey: "missing", sortKey: "sk" });
		expect(result).toEqual({ found: false });
	});

	it("stores and retrieves a string value", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "hello" });
		const result = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });

		expect(result).toMatchObject({ found: true, hashKey: "hk", sortKey: "sk", data: "hello" });
	});

	it("stores and retrieves binary data", async ({ expect }) => {
		const { ctx, stub } = makeStub();
		const data = new Uint8Array([1, 2, 3, 4, 5]);

		await stub.putItem(ctx, { hashKey: "hk-bin", sortKey: "sk-bin", data });
		const result = await stub.getItem(ctx, { hashKey: "hk-bin", sortKey: "sk-bin" });

		expect(result).toMatchObject({ found: true, data });
	});

	it("overwrites an existing item on repeated put", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "first" });
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "second" });
		const result = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });

		expect(result).toMatchObject({ found: true, data: "second" });
	});

	it("isolates items by (hashKey, sortKey) composite key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk1", sortKey: "sk1", data: "a" });
		await stub.putItem(ctx, { hashKey: "hk1", sortKey: "sk2", data: "b" });
		await stub.putItem(ctx, { hashKey: "hk2", sortKey: "sk1", data: "c" });

		const r1 = await stub.getItem(ctx, { hashKey: "hk1", sortKey: "sk1" });
		const r2 = await stub.getItem(ctx, { hashKey: "hk1", sortKey: "sk2" });
		const r3 = await stub.getItem(ctx, { hashKey: "hk2", sortKey: "sk1" });

		expect(r1).toMatchObject({ found: true, data: "a" });
		expect(r2).toMatchObject({ found: true, data: "b" });
		expect(r3).toMatchObject({ found: true, data: "c" });
	});

	it("includes operation metrics in putItem result", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const result = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "data" });

		expect(result.meta).toMatchObject({
			rowsRead: expect.any(Number),
			rowsWritten: expect.any(Number),
			databaseSize: expect.any(Number),
			served_by_instance: expect.any(String),
		});
	});

	it("includes operation metrics in getItem result", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "data" });
		const result = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });

		expect(result).toMatchObject({
			found: true,
			meta: {
				rowsRead: expect.any(Number),
				rowsWritten: expect.any(Number),
				databaseSize: expect.any(Number),
				served_by_instance: expect.any(String),
			},
		});
	});
});

describe("PartitionDO - splitting", () => {
	it("reports no split status before any threshold is crossed", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 100 } });

		const result = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "small" });

		expect(result.__debug?.splitStatus).toBeUndefined();
	});

	it("sets split_pending status when data exceeds maxSizeMb", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });

		const result = await stub.putItem(ctx, {
			hashKey: `hk.${stub.id.name!}`,
			sortKey: "sk",
			// Slightly over 1 MB to trigger the split condition.
			data: "x".repeat(1 * 1024 * 1024 + 10),
		});

		expect(result.__debug?.splitStatus).toBeDefined();
		expect(result.__debug?.splitStatus?.status).toBe("split_pending");
	});

	it("preserves split_pending status across subsequent writes", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });
		const hashKey = `hk.${stub.id.name!}`;

		await stub.putItem(ctx, {
			hashKey,
			sortKey: "sk1",
			data: "x".repeat(1 * 1024 * 1024 + 10),
		});

		const followUp = await stub.putItem(ctx, { hashKey, sortKey: "sk2", data: "small" });
		expect(followUp.__debug?.splitStatus?.status).toBe("split_pending");
	});

	it("alarm triggers startSplit and initializes child partitions", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });
		const hashKey = `hk.${stub.id.name!}`;

		await stub.putItem(ctx, {
			hashKey,
			sortKey: "sk",
			data: "x".repeat(1 * 1024 * 1024 + 10),
		});

		// The alarm is set to Date.now() in queueSplit(), so miniflare fires it automatically
		// in the background after putItem returns. runDurableObjectAlarm drains any remaining
		// scheduled alarm, but the auto-fired one may still be in progress.
		// We do this hack to ensure that the alarm is complete before we check the state, without relying on arbitrary timers.
		await runDurableObjectAlarm(stub);
		await runInDurableObject(stub, async (instance: PartitionDO) => {
			await vi.waitUntil(() => !instance.__testing__alarm_running, { timeout: 5000, interval: 100 });
		});
		const parentState = await stub.__internalState();
		expect(parentState.splitStatus?.status).toBe("split_in_progress");
		expect(parentState.partitionContext).toMatchObject({ ns: "PARTITION_DO", nsPrefix: "test" });

		// startSplit derives child names as `${nsPrefix}.${hashKey}.${i}` via the placeholder router.
		// splitN=2 → two children named "test.${hashKey}.0" and "test.${hashKey}.1".
		const childNames = [`test.${hashKey}.0`, `test.${hashKey}.1`];

		// Each child should have been initialized with the parent's context and a child-specific partition context.
		for (const name of childNames) {
			const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(name));
			const childState = await childStub.__internalState();

			expect(childState.partitionContext).toMatchObject({
				ns: "PARTITION_DO",
				nsPrefix: "test",
				doName: name,
			});
			expect(childState.parentPartitionContext).toMatchObject({
				doName: ctx.doName,
				primaryDoIdStr: ctx.primaryDoIdStr,
			});
			expect(childState.parentSplitType).toBe("hash");
			// Children haven't crossed any split threshold of their own.
			expect(childState.splitStatus).toBeUndefined();
		}
	});

	it("initFromSplit is idempotent when called with identical options", async ({ expect }) => {
		const { ctx: parentCtx } = makeStub();
		const childName = `test.initfromsplit-idempotent.${crypto.randomUUID()}`;
		const childId = env.PARTITION_DO.idFromName(childName);
		const childCtx: PartitionContextResolved = { ...parentCtx, doName: childName, primaryDoIdStr: childId.toString() };
		const childStub = env.PARTITION_DO.get(childId);

		const opts: InitFromSplitOptions = { parentPartitionContext: parentCtx, newPartitionContext: childCtx, splitType: "hash" };

		await childStub.initFromSplit(opts);
		// Calling again with identical opts must not throw.
		await expect(childStub.initFromSplit(opts)).resolves.not.toThrow();

		// State must reflect the first (and only) initialization.
		const state = await childStub.__internalState();
		expect(state.partitionContext?.primaryDoIdStr).toBe(childCtx.primaryDoIdStr);
		expect(state.parentPartitionContext?.primaryDoIdStr).toBe(parentCtx.primaryDoIdStr);
		expect(state.parentSplitType).toBe("hash");
	});

	it("initFromSplit throws when called with a conflicting child context", async ({ expect }) => {
		const { ctx: parentCtx } = makeStub();
		const childName = `test.initfromsplit-conflict.${crypto.randomUUID()}`;
		const childId = env.PARTITION_DO.idFromName(childName);
		const childCtx: PartitionContextResolved = { ...parentCtx, doName: childName, primaryDoIdStr: childId.toString() };
		const childStub = env.PARTITION_DO.get(childId);

		await childStub.initFromSplit({ parentPartitionContext: parentCtx, newPartitionContext: childCtx, splitType: "hash" });

		// Use runInDurableObject so the caught rejection stays inside the DO's execution context
		// and doesn't leak as an unhandled rejection at the worker level.
		const { ctx: otherCtx } = makeStub();
		await runInDurableObject(childStub, async (instance: PartitionDO) => {
			await expect(
				instance.initFromSplit({ parentPartitionContext: parentCtx, newPartitionContext: otherCtx, splitType: "hash" }),
			).rejects.toThrow("conflicting options");
		});
	});

	it("initFromSplit throws when called with a conflicting parent context", async ({ expect }) => {
		const { ctx: parentCtx } = makeStub();
		const childName = `test.initfromsplit-conflict-parent.${crypto.randomUUID()}`;
		const childId = env.PARTITION_DO.idFromName(childName);
		const childCtx: PartitionContextResolved = { ...parentCtx, doName: childName, primaryDoIdStr: childId.toString() };
		const childStub = env.PARTITION_DO.get(childId);

		await childStub.initFromSplit({ parentPartitionContext: parentCtx, newPartitionContext: childCtx, splitType: "hash" });

		const { ctx: differentParentCtx } = makeStub();
		await runInDurableObject(childStub, async (instance: PartitionDO) => {
			await expect(
				instance.initFromSplit({ parentPartitionContext: differentParentCtx, newPartitionContext: childCtx, splitType: "hash" }),
			).rejects.toThrow("conflicting options");
		});
	});

	it("initFromSplit throws when called with a conflicting splitType", async ({ expect }) => {
		const { ctx: parentCtx } = makeStub();
		const childName = `test.initfromsplit-conflict-splittype.${crypto.randomUUID()}`;
		const childId = env.PARTITION_DO.idFromName(childName);
		const childCtx: PartitionContextResolved = { ...parentCtx, doName: childName, primaryDoIdStr: childId.toString() };
		const childStub = env.PARTITION_DO.get(childId);

		await childStub.initFromSplit({ parentPartitionContext: parentCtx, newPartitionContext: childCtx, splitType: "hash" });

		await runInDurableObject(childStub, async (instance: PartitionDO) => {
			await expect(
				instance.initFromSplit({ parentPartitionContext: parentCtx, newPartitionContext: childCtx, splitType: "range" }),
			).rejects.toThrow("conflicting options");
		});
	});

	it("exposes split status via status()", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });

		await stub.putItem(ctx, {
			hashKey: `hk.${stub.id.name!}`,
			sortKey: "sk",
			data: "x".repeat(1 * 1024 * 1024 + 10),
		});

		const { splitStatus } = await stub.status();
		expect(splitStatus).toBeDefined();
		expect(splitStatus?.status).toBe("split_pending");
		expect(splitStatus?.createdAt).toBeTypeOf("number");
	});
});
