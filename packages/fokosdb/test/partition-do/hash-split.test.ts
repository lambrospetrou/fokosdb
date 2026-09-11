import { env } from "cloudflare:workers";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, it } from "vitest";
import { InitFromSplitOptions, PartitionDO } from "../../src/server/do-partition.js";
import type { PartitionContextResolved } from "../../src/shared/partition-topology/partition-context.js";
import { PartitionIdHelper } from "../../src/shared/partition-topology/partition-id.js";
import { compiledCondition, expectSplitStatus, kb, makeStub } from "./helpers.js";
import { fokosErrorWith } from "../errors-matchers.js";
import {
	assertSplitTreeComplete,
	drainUntil,
	makePartition,
	TestPartition,
	withMigrationBatchCap,
	withMigrationHeld,
} from "./partition-harness.js";

describe("PartitionDO - splitting", () => {
	it("reports no split status before any threshold is crossed", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 100 } });

		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "small", kind: "text" as const });

		const { splitStatus } = await stub.status();
		expect(splitStatus).toBeUndefined();
	});

	it("sets split_pending status when data exceeds maxSizeMb", async ({ expect }) => {
		const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });

		await partition.triggerHashSplit();

		const { splitStatus } = await partition.status();
		expect(splitStatus).toBeDefined();
		// By the time of the assertion the split could be in any of these states.
		expect(["split_queued", "split_started", "split_completed"]).toContain(splitStatus?.status);
	});

	it("preserves split_queued status across subsequent writes before the alarm runs", async ({ expect }) => {
		const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		try {
			await runInDurableObject(partition.stub, async (instance: PartitionDO) => {
				await partition.triggerHashSplit(instance);
				expect((await instance.status()).splitStatus?.status).toBe("split_queued");
				await instance.apiPutItem(partition.ctx, { hashKey: kb("extra"), sortKey: kb("sk2"), data: "small", kind: "text" });
				expect((await instance.status()).splitStatus?.status).toBe("split_queued");
			});
		} finally {
			await partition.awaitSplitCompleted();
		}
	});

	it("alarm triggers startSplit and initializes child partitions", async ({ expect }) => {
		const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		const { ctx, stub } = partition;

		await partition.triggerHashSplit();
		await partition.runAlarm();
		await partition.awaitSplitStarted();

		const parentState = await partition.status();
		expect(["split_started", "split_completed"]).toContain(parentState.splitStatus?.status);
		expect(parentState.partitionContext).toMatchObject({
			ns: "PARTITION_DO",
			tableName: ctx.tableName,
		});

		const childNames = PartitionIdHelper.calculateHashChildPartitionIds(parentState.partitionContext!).map((c) => c.doName);

		// Each child should have been initialized with the parent's context and a child-specific partition context.
		for (const name of childNames) {
			const childStub = PartitionDO.getByName(env.PARTITION_DO, name);
			const childState = await childStub.status();

			expect(childState.partitionContext).toMatchObject({
				ns: "PARTITION_DO",
				tableName: ctx.tableName,
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
		const childCtx: PartitionContextResolved = {
			...parentCtx,
			doName: childName,
			primaryDoIdStr: childId.toString(),
		};
		const childStub = PartitionDO.get(env.PARTITION_DO, childId);

		const opts: InitFromSplitOptions = {
			parentPartitionContext: parentCtx,
			newPartitionContext: childCtx,
			splitType: "hash",
		};

		await childStub.internalInitFromSplit(opts);
		// Calling again with identical opts must not throw.
		await expect(childStub.internalInitFromSplit(opts)).resolves.not.toThrow();

		// State must reflect the first (and only) initialization.
		const state = await childStub.status();
		expect(state.partitionContext?.primaryDoIdStr).toBe(childCtx.primaryDoIdStr);
		expect(state.parentPartitionContext?.primaryDoIdStr).toBe(parentCtx.primaryDoIdStr);
		expect(state.parentSplitType).toBe("hash");
	});

	it("initFromSplit throws when called with a conflicting child context", async ({ expect }) => {
		const { ctx: parentCtx } = makeStub();
		const childName = `test.initfromsplit-conflict.${crypto.randomUUID()}`;
		const childId = env.PARTITION_DO.idFromName(childName);
		const childCtx: PartitionContextResolved = {
			...parentCtx,
			doName: childName,
			primaryDoIdStr: childId.toString(),
		};
		const childStub = PartitionDO.get(env.PARTITION_DO, childId);

		await childStub.internalInitFromSplit({
			parentPartitionContext: parentCtx,
			newPartitionContext: childCtx,
			splitType: "hash",
		});

		// Use runInDurableObject so the caught rejection stays inside the DO's execution context
		// and doesn't leak as an unhandled rejection at the worker level.
		const { ctx: otherCtx } = makeStub();
		await runInDurableObject(childStub, async (instance: PartitionDO) => {
			await expect(
				instance.internalInitFromSplit({
					parentPartitionContext: parentCtx,
					newPartitionContext: otherCtx,
					splitType: "hash",
				}),
			).rejects.toThrow(fokosErrorWith("partition_context_mismatch"));
		});
	});

	it("initFromSplit throws when called with a conflicting parent context", async ({ expect }) => {
		const { ctx: parentCtx } = makeStub();
		const childName = `test.initfromsplit-conflict-parent.${crypto.randomUUID()}`;
		const childId = env.PARTITION_DO.idFromName(childName);
		const childCtx: PartitionContextResolved = {
			...parentCtx,
			doName: childName,
			primaryDoIdStr: childId.toString(),
		};
		const childStub = PartitionDO.get(env.PARTITION_DO, childId);

		await childStub.internalInitFromSplit({
			parentPartitionContext: parentCtx,
			newPartitionContext: childCtx,
			splitType: "hash",
		});

		const { ctx: differentParentCtx } = makeStub();
		await runInDurableObject(childStub, async (instance: PartitionDO) => {
			await expect(
				instance.internalInitFromSplit({
					parentPartitionContext: differentParentCtx,
					newPartitionContext: childCtx,
					splitType: "hash",
				}),
			).rejects.toThrow(fokosErrorWith("partition_context_mismatch"));
		});
	});

	it("initFromSplit throws when called with a conflicting splitType", async ({ expect }) => {
		const { ctx: parentCtx } = makeStub();
		const childName = `test.initfromsplit-conflict-splittype.${crypto.randomUUID()}`;
		const childId = env.PARTITION_DO.idFromName(childName);
		const childCtx: PartitionContextResolved = {
			...parentCtx,
			doName: childName,
			primaryDoIdStr: childId.toString(),
		};
		const childStub = PartitionDO.get(env.PARTITION_DO, childId);

		await childStub.internalInitFromSplit({
			parentPartitionContext: parentCtx,
			newPartitionContext: childCtx,
			splitType: "hash",
		});

		await runInDurableObject(childStub, async (instance: PartitionDO) => {
			await expect(
				instance.internalInitFromSplit({
					parentPartitionContext: parentCtx,
					newPartitionContext: childCtx,
					splitType: "range",
				}),
			).rejects.toThrow(fokosErrorWith("partition_context_mismatch"));
		});
	});

	it("exposes split status via status()", async ({ expect }) => {
		const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });

		await partition.triggerHashSplit();

		const { splitStatus } = await partition.status();
		expect(splitStatus).toBeDefined();
		// Background work may advance split past split_queued before status() is called.
		expect(["split_queued", "split_started", "split_completed"]).toContain(splitStatus?.status);
		expect(splitStatus?.createdAt).toBeTypeOf("number");
	});

	it("alarm with no split queued and no migration in progress does nothing", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 100 } });

		// Write something small — well below the split threshold — to initialize the partition context.
		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "small", kind: "text" as const });

		// No split should have been queued.
		const { splitStatus: before } = await stub.status();
		expect(before).toBeUndefined();

		// Manually schedule an alarm to simulate a stale alarm (e.g. after a crash with no pending work).
		await runInDurableObject(stub, async (instance: PartitionDO, ctx: DurableObjectState) => {
			await ctx.storage.setAlarm(Date.now());
		});

		// The alarm must complete without throwing, and leave the partition unchanged.
		await expect(runDurableObjectAlarm(stub)).resolves.not.toThrow();

		const { splitStatus: after } = await stub.status();
		expect(after).toBeUndefined();
	});

	describe("forwarding during splits", async () => {
		it("forwards putItem and getItem to a child after split, reporting forwardCount=1 and consistent servedByActorName", async ({
			expect,
		}) => {
			const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
			const { ctx, stub } = partition;

			// Trigger the split condition and drain the tree so all migrations complete.
			await partition.splitHash();

			const childNames = PartitionIdHelper.calculateHashChildPartitionIds(ctx).map((c) => c.doName);

			const hashKey = "forwarded-key";
			const putResult = await stub.apiPutItem(ctx, {
				hashKey: kb(hashKey),
				sortKey: kb("sk"),
				data: "val",
				kind: "text" as const,
				condition: compiledCondition({ op: "not_exists", args: [{ ref: "hashKey" }] }),
			});
			expect(putResult.meta.forwardCount).toBe(1);
			expect(putResult.meta.servedByActorName).not.toBe(ctx.doName);
			expect(childNames).toContain(putResult.meta.servedByActorName);

			const getResult = await stub.apiGetItem(ctx, { hashKey: kb(hashKey), sortKey: kb("sk") });
			expect(getResult.found).toBe(true);
			expect(getResult.meta.forwardCount).toBe(1);
			// Same child serves both the write and the subsequent read.
			expect(getResult.meta.servedByActorName).toBe(putResult.meta.servedByActorName);
		});

		it("returns found:false with forwardCount=1 for a missing key looked up through root after split", async ({ expect }) => {
			const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
			const { ctx, stub } = partition;

			await partition.splitHash();

			const result = await stub.apiGetItem(ctx, { hashKey: kb("definitely-missing"), sortKey: kb("sk") });
			expect(result.found).toBe(false);
			expect(result.meta.forwardCount).toBe(1);
			expect(result.meta.servedByActorName).not.toBe(ctx.doName);
		});
	});

	describe("multi-level splits", async () => {
		it("keeps all items accessible after splits at multiple tree depths", async ({ expect }) => {
			// One row stays below the 10% overage band, so a successful crossing write can queue each split.
			const ITEM_SIZE_BYTES = 4 * 1024;
			const dummyData = "x".repeat(ITEM_SIZE_BYTES);
			const TOTAL_ITEMS = 50;
			const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 0.1 } });
			const { ctx, stub } = partition;

			const allItems: Array<{ hashKey: string; sortKey: string; data: string }> = [];

			for (let i = 0; i < TOTAL_ITEMS; i++) {
				const hashKey = `item-${String(i).padStart(4, "0")}`;
				const sortKey = "sk";
				allItems.push({ hashKey, sortKey, data: dummyData });

				// Writes transiently fail while a split migration is in progress.
				// Drain the full split tree and retry until the write lands.
				let written = false;
				for (let attempt = 0; attempt < 20; attempt++) {
					try {
						await stub.apiPutItem(ctx, { hashKey: kb(hashKey), sortKey: kb(sortKey), data: dummyData, kind: "text" as const });
						written = true;
						break;
					} catch (e: unknown) {
						expect(["partition_migrating", "partition_over_size"]).toContain((e as { code?: string }).code);
						await partition.awaitTreeSettled();
					}
				}
				expect(written, `write did not land for ${hashKey}`).toBe(true);
				await partition.awaitTreeSettled();
				// console.log("BOOM 1 - end", { item: hashKey });
			}

			// Flush any in-flight splits triggered by the last few writes.
			await partition.awaitTreeSettled();

			// console.log("BOOM 2");

			// Verify every item is reachable through the root (which forwards through the tree)
			// and record the actor name that actually served each read.
			const servedByActorNames = new Set<string>();
			for (const item of allItems) {
				const result = await stub.apiGetItem(ctx, { hashKey: kb(item.hashKey), sortKey: kb(item.sortKey) });
				expect(result).toMatchObject({
					found: true,
					item: { data: dummyData },
				});
				if (result.found) {
					servedByActorNames.add(result.meta.servedByActorName);
					// hashDepth is constant (total tree levels) whether the cache skips hops or not.
					expect(result.meta.hashDepth, "root reads should span at least 2 hash tree levels").toBeGreaterThanOrEqual(2);
					// forwardCount starts at hashDepth (cold cache, one RPC per level) and
					// converges to 1 (warm cache, single RPC directly to the leaf). The invariant
					// that proves the cache is reducing latency is forwardCount ≤ hashDepth.
					expect(result.meta.forwardCount, "topology cache should reduce RPC hops to at most one per tree level").toBeLessThanOrEqual(
						result.meta.hashDepth,
					);
					expect(result.meta.forwardCount, "always at least one RPC hop from the root").toBeGreaterThanOrEqual(1);
				}
			}

			// With 50 items and splitN=2, the tree reaches ~3 levels deep (~8 leaf DOs).
			// Even with hash skew, at least 3 distinct instances must serve reads.
			expect(servedByActorNames.size, "many distinct partition instances should have served requests").toBeGreaterThan(3);

			const totalSplitNodes = await assertSplitTreeComplete(partition);
			expect(totalSplitNodes, "multiple levels of splits should have occurred").toBeGreaterThan(2);
		});
	}, 30_000);

	describe("hash topology cache", async () => {
		// A fixed probe key used to trace a deterministic path through the tree.
		const hashKey = "probe-key";

		it("propagates hashDepth=1 after one hash split and hashDepth=2 after two", async ({ expect }) => {
			const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
			const { ctx, stub } = partition;

			// Root splits into two children.
			await partition.splitHash();

			// root → child (leaf): hashDepth=1, forwardCount=1. Cache stays cold (child returns hashDepth=0).
			const r1 = await stub.apiGetItem(ctx, { hashKey: kb(hashKey), sortKey: kb("sk") });
			expect(r1.meta.hashDepth).toBe(1);
			expect(r1.meta.forwardCount).toBe(1);

			// Split the child that owns hashKey.
			await (await partition.childOwning(hashKey)).splitHash();

			// root → child → grandchild: hashDepth=2. Cache is cold so forwardCount=2 (two RPC hops).
			const r2 = await stub.apiGetItem(ctx, { hashKey: kb(hashKey), sortKey: kb("sk") });
			expect(r2.meta.hashDepth).toBe(2);
			expect(r2.meta.forwardCount).toBe(2);
		});

		it("reduces forwardCount to 1 after learning a depth-2 path from the first response", async ({ expect }) => {
			const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
			const { ctx, stub } = partition;

			await partition.splitHash();
			await (await partition.childOwning(hashKey)).splitHash();

			// First request: cold cache — root→child→grandchild (two hops). Root learns depth=2.
			const r1 = await stub.apiGetItem(ctx, { hashKey: kb(hashKey), sortKey: kb("sk") });
			expect(r1.meta.hashDepth).toBe(2);
			expect(r1.meta.forwardCount).toBe(2);

			// Second request: warm cache — root skips directly to grandchild (one hop).
			const r2 = await stub.apiGetItem(ctx, { hashKey: kb(hashKey), sortKey: kb("sk") });
			expect(r2.meta.hashDepth).toBe(2);
			expect(r2.meta.forwardCount).toBe(1);
		});

		it("recovers from stale cache when grandchild splits: updates to depth=3 then skips directly", async ({ expect }) => {
			const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
			const { ctx, stub } = partition;

			// Build a two-level tree: root → child → grandchild.
			await partition.splitHash();
			await (await partition.childOwning(hashKey)).splitHash();

			// Warm root's cache to depth=2 with one request (root→child→grandchild).
			const r1 = await stub.apiGetItem(ctx, { hashKey: kb(hashKey), sortKey: kb("sk") });
			expect(r1.meta.hashDepth).toBe(2);

			// Now split the grandchild, making it a router for great-grandchildren.
			await (await partition.leafOwning(hashKey)).splitHash();

			// Stale-cache request: root targets grandchild (cached depth=2) but it is now a router.
			// Grandchild forwards one more level → root receives hashDepth=1, updates cache to depth=3,
			// and returns hashDepth=3. forwardCount=2: one RPC root→grandchild + grandchild→great-grandchild.
			const r2 = await stub.apiGetItem(ctx, { hashKey: kb(hashKey), sortKey: kb("sk") });
			expect(r2.meta.hashDepth).toBe(3);
			expect(r2.meta.forwardCount).toBe(2);

			// Subsequent request: root skips directly to great-grandchild (one RPC hop).
			const r3 = await stub.apiGetItem(ctx, { hashKey: kb(hashKey), sortKey: kb("sk") });
			expect(r3.meta.hashDepth).toBe(3);
			expect(r3.meta.forwardCount).toBe(1);
		});
	}, 30_000);

	describe("migration", () => {
		it("migrates each item to exactly one child and preserves reads through the parent", async ({ expect }) => {
			const partition = makePartition({ hashSplitN: 10, hashSplitConditions: { maxSizeMb: 1 } });
			const { ctx, stub } = partition;

			// Seed items with varied hash keys so they spread across children.
			const seedItems = [
				{ hashKey: kb("alpha"), sortKey: kb("s1"), data: "data-alpha-1", kind: "text" as const },
				{ hashKey: kb("alpha"), sortKey: kb("s2"), data: "data-alpha-2", kind: "text" as const },
				{ hashKey: kb("banana"), sortKey: kb("s1"), data: "data-banana-1", kind: "text" as const },
				{ hashKey: kb("cherry"), sortKey: kb("s1"), data: "data-cherry-1", kind: "text" as const },
				{ hashKey: kb("delta"), sortKey: kb("s1"), data: "data-delta-1", kind: "text" as const },
				{ hashKey: kb("echo"), sortKey: kb("s1"), data: "data-echo-1", kind: "text" as const },
			];
			for (const item of seedItems) {
				await stub.apiPutItem(ctx, item);
			}

			// Trigger the split condition.
			await partition.triggerHashSplit();
			await partition.runAlarm();
			await partition.awaitSplitStarted();

			const parentState = await stub.status();
			expect(["split_started", "split_completed"]).toContain(parentState.splitStatus?.status);
			const childContexts = expectSplitStatus(parentState.splitStatus).childPartitionContexts;
			expect(childContexts).toHaveLength(10);

			// Run each child's migration to completion.
			// startSplit fire-and-forget already triggered migration on each child, so their alarms
			// may already be running or complete by the time we reach here. awaitMigrationCompleted
			// handles both.
			for (const childCtx of childContexts) {
				const childStub = PartitionDO.getByName(env.PARTITION_DO, childCtx.doName);
				await TestPartition.at(childCtx).awaitMigrationCompleted();
				const state = await childStub.status();
				expect(state.migrationStatus).toBe("migration_completed");
			}

			// Parent acknowledges all children and transitions to split_completed.
			await partition.awaitSplitCompleted();
			const finalParent = await stub.status();
			expect(finalParent.splitStatus?.status).toBe("split_completed");
			const finalSplit = expectSplitStatus(finalParent.splitStatus);
			expect(finalSplit.migratedChildDoNames).toHaveLength(10);

			// All migrations complete: root successfully forwards each item to the correct child.
			for (const item of seedItems) {
				const result = await stub.apiGetItem(ctx, { hashKey: item.hashKey, sortKey: item.sortKey });
				expect(result).toMatchObject({
					found: true,
					item: { data: item.data },
					meta: { forwardCount: 1 },
				});
			}

			// Every seed item is found in exactly one child with the correct data.
			const foundIds = new Set<string>();
			for (const item of seedItems) {
				let foundInDoName: string | undefined;
				for (const childCtx of childContexts) {
					const childStub = PartitionDO.getByName(env.PARTITION_DO, childCtx.doName);
					const result = await childStub.apiGetItem(childCtx, {
						hashKey: item.hashKey,
						sortKey: item.sortKey,
					});
					if (result.found) {
						expect(foundInDoName, `"${item.hashKey}/${item.sortKey}" found in multiple children`).toBeUndefined();
						expect(result).toMatchObject({ item: { data: item.data } });
						foundInDoName = childCtx.doName;
						foundIds.add(foundInDoName);
					}
				}
				expect(foundInDoName, `"${item.hashKey}/${item.sortKey}" not found in any child`).toBeDefined();
			}
			// This might be flaky - but ideally we should have items across more than 1 children.
			expect(foundIds.size).toBeGreaterThan(1);
		});

		it("arms TTL deletion after child migration completes", async ({ expect }) => {
			const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
			await partition.put({ hashKey: kb("ttl-migration"), sortKey: kb("sk"), data: "value", kind: "text" });

			let children: TestPartition[] = [];
			const countExpired = async () => {
				let count = 0;
				for (const child of children) {
					count += await runInDurableObject(
						child.stub,
						(_instance: PartitionDO, state: DurableObjectState) =>
							state.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM items WHERE ttl_epoch_utc_seconds IS NOT NULL").one().n,
					);
				}
				return count;
			};
			await withMigrationHeld(partition, async (waitForAllChildRequests) => {
				await partition.triggerHashSplit();
				await partition.awaitSplitStarted();
				await waitForAllChildRequests();
				children = await partition.children();
				for (const child of children) expect((await child.status()).migrationStatus).toBe("migration_migrating");

				// Simulate the item expiring mid-migration: the sweep must not run while a child is
				// still migrating, so the expired row is still there until the hold releases.
				for (const child of children) {
					await runInDurableObject(child.stub, (_instance: PartitionDO, state: DurableObjectState) =>
						state.storage.sql.exec("UPDATE items SET ttl_epoch_utc_seconds = 1 WHERE hk = ?", kb("ttl-migration")),
					);
				}
				expect(await countExpired()).toBe(1);
			});
			await drainUntil(children, async () => (await countExpired()) === 0, "TTL sweep after migration", 10_000);
		}, 15_000);

		it("putItem is rejected while migration is in progress", async ({ expect }) => {
			const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
			await partition.put({ hashKey: kb("key1"), sortKey: kb("sk"), data: "value1", kind: "text" });

			// Install the RPC delay before the write that triggers the split.
			// Each child must read transaction metadata from the parent before it completes.
			// Holding that response keeps the production migration pending while requests run.
			// The original RPC executes after release, without changing its result.
			await withMigrationHeld(partition, async (waitForAllChildRequests) => {
				await partition.triggerHashSplit();
				await partition.awaitSplitStarted();
				await waitForAllChildRequests();
				const child = await partition.childOwning("key1");
				expect((await child.status()).migrationStatus).toBe("migration_migrating");

				// Call putItem directly on the instance (not via RPC stub) so the error stays local.
				// Going through the stub would cause workerd to log the remote throw as an uncaught
				// exception, which Vitest surfaces as an unhandled rejection even though we catch it.
				await runInDurableObject(child.stub, async (instance: PartitionDO) => {
					await expect(
						instance.apiPutItem(child.ctx, {
							hashKey: kb("key1"),
							sortKey: kb("sk"),
							data: "new-value",
							kind: "text",
						}),
					).rejects.toThrow(fokosErrorWith("partition_migrating", { operation: "putItem" }));
				});
			});
			expect((await (await partition.childOwning("key1")).status()).migrationStatus).toBe("migration_completed");
		});

		it("getItem on a child reads through to the parent while migration is in progress", async ({ expect }) => {
			const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
			const { ctx, stub } = partition;

			const seedItems = [
				{ hashKey: kb("alpha"), sortKey: kb("s1"), data: "data-alpha-1", kind: "text" as const },
				{ hashKey: kb("banana"), sortKey: kb("s1"), data: "data-banana-1", kind: "text" as const },
			];
			for (const item of seedItems) {
				await stub.apiPutItem(ctx, item);
			}

			// Install the migration RPC delay before triggering the split.
			await withMigrationHeld(partition, async (waitForAllChildRequests) => {
				await partition.triggerHashSplit();
				await partition.awaitSplitStarted();
				await waitForAllChildRequests();
				const child = (await partition.children())[0];

				// Child migration is blocked at the gate — verify it is still migrating.
				expect((await child.status()).migrationStatus).toBe("migration_migrating");

				// While migration is in progress, getItem on the child must read through to the parent
				// so callers can read data that has not yet been copied to the child.
				for (const item of seedItems) {
					const result = await child.get(item);
					expect(result).toMatchObject({ found: true, item: { data: item.data }, meta: { servedByActorName: partition.doName } });
				}
			});
			await assertSplitTreeComplete(partition);
		});

		it("migrates all items correctly when the parent sends data in multiple cursor-paginated batches", async ({ expect }) => {
			const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
			const { ctx, stub } = partition;

			// Items with a mix of null and non-null sort keys to exercise the null-sk cursor boundary.
			const seedItems = [
				{ hashKey: kb("alpha"), sortKey: kb(), data: "data-alpha-nosort", kind: "text" as const },
				{ hashKey: kb("alpha"), sortKey: kb("s1"), data: "data-alpha-s1", kind: "text" as const },
				{ hashKey: kb("banana"), sortKey: kb("s1"), data: "data-banana-1", kind: "text" as const },
				{ hashKey: kb("cherry"), sortKey: kb("s1"), data: "data-cherry-1", kind: "text" as const },
				{ hashKey: kb("delta"), sortKey: kb("s1"), data: "data-delta-1", kind: "text" as const },
			];
			for (const item of seedItems) {
				await stub.apiPutItem(ctx, item);
			}

			// One row per batch response forces a cursor-paginated round trip per item on every
			// migration stream (items, pending transactions, promoted keys).
			await withMigrationBatchCap(partition, 1, async ({ truncated }) => {
				await partition.splitHash();
				expect(truncated(), "the batch cap should have forced extra round trips").toBeGreaterThan(0);
			});

			// Every item is reachable through root via forwarding.
			for (const item of seedItems) {
				const result = await stub.apiGetItem(ctx, { hashKey: item.hashKey, sortKey: item.sortKey });
				expect(result).toMatchObject({
					found: true,
					meta: { forwardCount: 1 },
					item: { data: item.data },
				});
			}

			await assertSplitTreeComplete(partition);
		});

		it("getItemDirect bypasses split forwarding and reads from local storage", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "direct-value", kind: "text" as const });

			const result = await stub.internalGetItemDirect({ hashKey: kb("hk"), sortKey: kb("sk") });
			expect(result).toMatchObject({
				found: true,
				item: { data: "direct-value", kind: "text" as const },
			});

			const miss = await stub.internalGetItemDirect({ hashKey: kb("missing"), sortKey: kb("sk") });
			expect(miss.found).toBe(false);
		});
	});
});
