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

	describe("multi-level splits", async () => {
		it("keeps all items accessible after splits at multiple tree depths (~10 items per partition trigger threshold)", async ({
			expect,
		}) => {
			// Items are ~1 KB each so ~10 fill 0.2 MB and trigger a split at any given partition.
			// With splitN=3 and 100 total items, the root and multiple generations of children
			// all split, creating a deep tree that exercises the full forwarding chain.
			const ITEM_SIZE_BYTES = 10 * 1024;
			const dummyData = "x".repeat(ITEM_SIZE_BYTES);
			const TOTAL_ITEMS = 100;
			const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 3, maxSizeMb: 0.1 } });

			const allItems: Array<{ hashKey: string; sortKey: string; data: string }> = [];

			for (let i = 0; i < TOTAL_ITEMS; i++) {
				const hashKey = `item-${String(i).padStart(4, "0")}`;
				const sortKey = "sk";
				allItems.push({ hashKey, sortKey, data: dummyData });

				// Writes transiently fail while a split migration is in progress.
				// Drain the full split tree and retry until the write lands.
				for (let attempt = 0; attempt < 20; attempt++) {
					try {
						await stub.putItem(ctx, { hashKey, sortKey, data: dummyData });
						break;
					} catch (e: unknown) {
						const msg = String(e);
						if (msg.includes("split in progress") || msg.includes("exceeded its limits")) {
							await drainSplitTree(stub);
						} else {
							throw e;
						}
					}
				}
			}

			// Flush any in-flight splits triggered by the last few writes.
			await drainSplitTree(stub);

			// Verify every item is reachable through the root (which forwards through the tree)
			// and record the DO instance that actually served each read.
			const servedByInstances = new Set<string>();
			for (const item of allItems) {
				const result = await stub.getItem(ctx, { hashKey: item.hashKey, sortKey: item.sortKey });
				expect(result).toMatchObject({ found: true, hashKey: item.hashKey, sortKey: item.sortKey, data: dummyData });
				if (result.found) {
					servedByInstances.add(result.meta.served_by_instance);
				}
			}

			// With 100 items and splitN=3, the tree reaches ~4 levels deep (~16 leaf DOs).
			// Even with hash skew, at least 4 distinct instances must serve reads.
			expect(servedByInstances.size, "many distinct partition instances should have served requests").toBeGreaterThan(4);

			// Recursively walk the entire split tree and assert every non-leaf node reached
			// split_completed, confirming correctness at every level of the hierarchy.
			async function assertSplitTreeComplete(nodeStub: DurableObjectStub<PartitionDO>): Promise<number> {
				const state = await nodeStub.__internalState();
				if (!state.splitStatus) return 0;
				expect(state.splitStatus.status, `DO ${state.partitionContext?.doName} should be split_completed`).toBe("split_completed");
				const split = state.splitStatus as SplitStartedOrCompleted;
				let count = 1;
				for (const childCtx of split.childPartitionContexts) {
					count += await assertSplitTreeComplete(env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName)));
				}
				return count;
			}

			const totalSplitNodes = await assertSplitTreeComplete(stub);
			expect(totalSplitNodes, "multiple levels of splits should have occurred").toBeGreaterThan(2);
		});
	}, 30_000);

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

describe("PartitionDO - partitionId encoding", () => {
	it("hex bytes encode depth+hashIdxs correctly and _partitionIdBytes is cached in the DO", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });
		const topologyRouter = new PartitionTopologyRouterImpl("", ctx);

		// Root: [depth=1, hashIdx=0]
		const rootBytes = Uint8Array.fromHex(ctx.partitionId);
		expect(rootBytes).toEqual(new Uint8Array([1, 0]));

		// After the first request ensurePartitionContext stores the context with _partitionIdBytes populated.
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v" });
		const rootState = await stub.__internalState();
		expect(rootState.partitionContext?._partitionIdBytes).toBeInstanceOf(Uint8Array);
		expect(rootState.partitionContext?._partitionIdBytes).toEqual(rootBytes);

		// Trigger a split so we can verify child IDs.
		await stub.putItem(ctx, { hashKey: "hk2", sortKey: "sk2", data: "x".repeat(1 * 1024 * 1024 + 10) });
		await waitForAlarm(stub);

		const children = topologyRouter.calculateChildPartitionIds(ctx.partitionId, 2);
		for (let i = 0; i < children.length; i++) {
			const child = children[i];

			// Child: [depth=2, hashIdx[0]=0 (root), hashIdx[1]=i]
			const childBytes = Uint8Array.fromHex(child.partitionIdOpaque);
			expect(childBytes).toEqual(new Uint8Array([2, 0, i]));

			// doName encodes the same path as text.
			expect(child.doName).toBe(`${ctx.nsPrefix}.h.0.${i}`);

			// _partitionIdBytes is also cached inside each child DO after initFromSplit.
			const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(child.doName));
			const childState = await childStub.__internalState();
			expect(childState.partitionContext?._partitionIdBytes).toBeInstanceOf(Uint8Array);
			expect(childState.partitionContext?._partitionIdBytes).toEqual(childBytes);
		}
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

/**
 * Recursively drains all pending alarms in the split tree rooted at `stub`.
 * For each node: runs any pending alarm (startSplit or nothing if already done),
 * then for each child still awaiting migration triggers its alarm via a dummy request
 * (which sets the alarm and throws "split in progress" — the error is swallowed),
 * and finally recurses into each child to drain migration and any further splits.
 */
async function drainSplitTree(stub: DurableObjectStub<PartitionDO>): Promise<void> {
	await waitForAlarm(stub);
	const state = await stub.__internalState();

	if (!state.splitStatus || state.splitStatus.status === "split_queued") return;

	const splitStatus = state.splitStatus as SplitStartedOrCompleted;
	for (const childCtx of splitStatus.childPartitionContexts) {
		const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));

		// A child in migration_initialized has no alarm yet; any request to it transitions
		// it to migration_migrating and schedules the alarm. The error is expected.
		const childState = await childStub.__internalState();
		if (childState.migrationStatus === "migration_initialized" || childState.migrationStatus === "migration_migrating") {
			await childStub.getItem(childCtx, { hashKey: "_", sortKey: "_" }).catch(() => {});
		}

		await drainSplitTree(childStub);
	}
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
		partitionId: __encodePartitionIdOpaque([0]),
	};
	return { ctx, stub: env.PARTITION_DO.get(id) };
}
