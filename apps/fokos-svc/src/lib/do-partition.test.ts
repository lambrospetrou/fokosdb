import { env } from "cloudflare:workers";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, it, vi } from "vitest";
import type { InitFromSplitOptions, PartitionDO } from "./do-partition.js";
import {
	__encodePartitionIdOpaque,
	PartitionContextCreator,
	PartitionTopologyRouterImpl,
} from "./partition-topology/partition-topology.js";
import type { PartitionContextResolved, SplitStatusKVItem } from "./partition-topology/partition-topology.js";

type SplitStartedOrCompleted = Extract<SplitStatusKVItem, { status: "split_started" | "split_completed" }>;

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

	it("returns version 1 on first write", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const result = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "hello" });

		expect(result.version).toBe(1);
	});

	it("increments version on each subsequent write to the same key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const r1 = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v1" });
		const r2 = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v2" });
		const r3 = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v3" });

		expect(r1.version).toBe(1);
		expect(r2.version).toBe(2);
		expect(r3.version).toBe(3);
	});

	it("getItem returns the current version", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v1" });
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v2" });
		const result = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });

		expect(result).toMatchObject({ found: true, version: 2 });
	});

	it("versions are independent per (hashKey, sortKey) key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk1", data: "a" });
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk1", data: "a2" });
		const r1 = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk2", data: "b" });

		const get1 = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk1" });
		const get2 = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk2" });

		expect(r1.version).toBe(1);
		expect(get1).toMatchObject({ found: true, version: 2 });
		expect(get2).toMatchObject({ found: true, version: 1 });
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
		expect(result.__debug?.splitStatus?.status).toBe("split_queued");
	});

	it("preserves split_queued status across subsequent writes", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });
		const hashKey = `hk.${stub.id.name!}`;

		await stub.putItem(ctx, {
			hashKey,
			sortKey: "sk1",
			data: "x".repeat(1 * 1024 * 1024 + 10),
		});

		const followUp = await stub.putItem(ctx, { hashKey, sortKey: "sk2", data: "small" });
		expect(followUp.__debug?.splitStatus?.status).toBe("split_queued");
	});

	it("alarm triggers startSplit and initializes child partitions", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });
		const topologyRouter = new PartitionTopologyRouterImpl("", ctx);
		const hashKey = `hk.1`;

		await stub.putItem(ctx, {
			hashKey,
			sortKey: "sk",
			data: "x".repeat(1 * 1024 * 1024 + 10),
		});
		await waitForAlarm(stub);

		const parentState = await stub.__internalState();
		expect(parentState.splitStatus?.status).toBe("split_started");
		expect(parentState.partitionContext).toMatchObject({ ns: "PARTITION_DO", nsPrefix: ctx.nsPrefix });

		const childNames = topologyRouter.calculateChildPartitionIds(parentState.partitionContext.partitionId, 2).map((c) => c.doName);

		// Each child should have been initialized with the parent's context and a child-specific partition context.
		for (const name of childNames) {
			const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(name));
			const childState = await childStub.__internalState();

			expect(childState.partitionContext).toMatchObject({
				ns: "PARTITION_DO",
				nsPrefix: ctx.nsPrefix,
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
		expect(splitStatus?.status).toBe("split_queued");
		expect(splitStatus?.createdAt).toBeTypeOf("number");
	});

	describe("forwarding during splits", async () => {
		it("forwards requests to children after split starts", async ({ expect }) => {
			// TODO
		});
	});

	describe("migration", () => {
		it("rejects requests to child while migrating, migrates all data to the correct child, and completes", async ({ expect }) => {
			const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 10, maxSizeMb: 1 } });

			// Seed items with varied hash keys so they spread across children.
			const seedItems = [
				{ hashKey: "alpha", sortKey: "s1", data: "data-alpha-1" },
				{ hashKey: "alpha", sortKey: "s2", data: "data-alpha-2" },
				{ hashKey: "banana", sortKey: "s1", data: "data-banana-1" },
				{ hashKey: "cherry", sortKey: "s1", data: "data-cherry-1" },
				{ hashKey: "delta", sortKey: "s1", data: "data-delta-1" },
				{ hashKey: "echo", sortKey: "s1", data: "data-echo-1" },
			];
			for (const item of seedItems) {
				await stub.putItem(ctx, item);
			}

			// Trigger the split condition.
			await stub.putItem(ctx, { hashKey: "trigger", sortKey: "sk", data: "x".repeat(1 * 1024 * 1024 + 10) });
			await waitForAlarm(stub);

			const parentState = await stub.__internalState();
			expect(parentState.splitStatus?.status).toBe("split_started");
			const childContexts = (parentState.splitStatus as SplitStartedOrCompleted).childPartitionContexts;
			expect(childContexts).toHaveLength(10);

			// Each child is initialized but migration has not started yet.
			for (const childCtx of childContexts) {
				const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));
				const state = await childStub.__internalState();
				expect(state.migrationStatus).toBe("migration_initialized");
			}

			// Requests to children are rejected while migration is in progress.
			// The first request also transitions the child to migration_migrating and schedules the alarm.
			for (const childCtx of childContexts) {
				const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));
				await expect(
					runInDurableObject(childStub, async (instance: PartitionDO) => {
						return instance.getItem(childCtx, { hashKey: "any", sortKey: "any" });
					}),
				).rejects.toThrow("split in progress");
			}

			// Run each child's migration alarm.
			// Note: miniflare fires alarms set to Date.now() automatically in the background, so
			// the alarm may already be running or complete by the time we reach here. waitForAlarm handles both.
			for (const childCtx of childContexts) {
				const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));
				await waitForAlarm(childStub);
				const state = await childStub.__internalState();
				expect(state.migrationStatus).toBe("migration_completed");
			}

			// Parent acknowledges all children and transitions to split_completed.
			const finalParent = await stub.__internalState();
			expect(finalParent.splitStatus?.status).toBe("split_completed");
			const finalSplit = finalParent.splitStatus as SplitStartedOrCompleted;
			expect(finalSplit.migratedChildDoNames).toHaveLength(10);

			// Every seed item is found in exactly one child with the correct data.
			const foundIds = new Set<string>();
			for (const item of seedItems) {
				let foundInDoName: string | undefined;
				for (const childCtx of childContexts) {
					const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));
					const result = await childStub.getItem(childCtx, { hashKey: item.hashKey, sortKey: item.sortKey });
					if (result.found) {
						expect(foundInDoName, `"${item.hashKey}/${item.sortKey}" found in multiple children`).toBeUndefined();
						expect(result).toMatchObject({ data: item.data });
						foundInDoName = childCtx.doName;
						foundIds.add(foundInDoName);
					}
				}
				expect(foundInDoName, `"${item.hashKey}/${item.sortKey}" not found in any child`).toBeDefined();
			}
			// This might be flaky - but ideally we should have items across more than 1 children.
			expect(foundIds.size).toBeGreaterThan(1);
		});
	});
});

async function waitForAlarm(stub: DurableObjectStub<PartitionDO>) {
	// The alarm is set to Date.now() in queueSplit(), so miniflare fires it automatically
	// in the background after putItem returns. runDurableObjectAlarm drains any remaining
	// scheduled alarm, but the auto-fired one may still be in progress.
	// We do this hack to ensure that the alarm is complete before we check the state, without relying on arbitrary timers.
	await runDurableObjectAlarm(stub);
	await runInDurableObject(stub, async (instance: PartitionDO) => {
		await vi.waitUntil(() => !instance.__testing__alarm_running, { timeout: 5000, interval: 100 });
	});
}

function makeStub(opts?: Partial<Parameters<typeof PartitionContextCreator.create>[0]>) {
	const prefix = `test.${crypto.randomUUID()}`;
	const rootName = `${prefix}.r.0`;
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsPrefix: prefix,
		rootTreesN: 1,
		hashSplitConditions: { splitN: 2, maxSizeMb: 100 },
		rangeSplitConditions: { splitN: 2, maxSizeMb: 500 },
		...opts,
	});
	const id = env.PARTITION_DO.idFromName(rootName);
	const ctx: PartitionContextResolved = {
		...base,
		doName: rootName,
		primaryDoIdStr: id.toString(),
		partitionId: __encodePartitionIdOpaque({ hashIdxs: [0] }),
	};
	return { ctx, stub: env.PARTITION_DO.get(id) };
}
