import { runInDurableObject } from "cloudflare:test";
import { describe, it } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import type { PartitionContextResolved } from "../../src/shared/partition-topology/partition-context.js";
import { PartitionIdHelper } from "../../src/shared/partition-topology/partition-id.js";
import { HashPartitionTopologyImpl } from "../../src/shared/partition-topology/split-policy.js";
import invariant from "../../src/shared/invariant.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { kb, makeStub } from "./helpers.js";
import { makePartition } from "./partition-harness.js";

describe("PartitionDO - partitionId encoding", () => {
	it("encodes root and child partition IDs with correct byte layout and text doNames", ({ expect }) => {
		// makeStub uses rootTreesN=1, so pickPartition always lands on rootIdx=0.
		const { ctx } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });

		// Root: [version=0, rootIdx=0 (2 bytes big-endian), depth=0]
		expect(Uint8Array.fromHex(ctx.partitionId)).toEqual(new Uint8Array([0, 0, 0, 0]));

		// Children: [version=0, rootIdx=0 (2 bytes), depth=1, childIdx=i]
		const children = PartitionIdHelper.calculateHashChildPartitionIds(ctx);
		for (let i = 0; i < children.length; i++) {
			expect(Uint8Array.fromHex(children[i].partitionIdOpaque)).toEqual(new Uint8Array([0, 0, 0, 1, i]));
			expect(children[i].doName).toBe(`${ctx.tableName}.h.0.${i}`);
		}
	});

	it("pickChildPartition and makeIsCorrectChildHashPartition agree at every tree level", async ({ expect }) => {
		// This test guards the entropy consistency between the two methods.
		// If the depth offset used in one changes without the other, routing will silently
		// assign keys to different partitions than the migration check expects.
		const { ctx: pCtx, stub } = makeStub({
			hashSplitN: 4,
			hashSplitConditions: { maxSizeMb: 100 },
		});
		let topology: HashPartitionTopologyImpl;
		await runInDurableObject(stub, async (instance: PartitionDO, ctx: DurableObjectState) => {
			topology = new HashPartitionTopologyImpl(pCtx, ctx, new PartitionStore(ctx.storage));
		});
		invariant(topology!, "topology should be initialized in the DO instance");
		const hashKey = "routing-consistency-key";

		// Depth 0 → 1: pickChildPartition must select exactly the sibling that makeIsCorrectChildHashPartition identifies.
		const { partitionContext: child } = topology.pickChildPartition(pCtx, kb(hashKey));
		const level1Siblings = PartitionIdHelper.calculateHashChildPartitionIds(pCtx);
		for (const sib of level1Siblings) {
			const sibCtx: PartitionContextResolved = {
				...pCtx,
				doName: sib.doName,
				partitionId: sib.partitionIdOpaque,
				primaryDoIdStr: "",
			};
			expect(topology.makeIsCorrectChildHashPartition(pCtx, sibCtx)(kb(hashKey))).toBe(sib.doName === child.doName);
		}

		// Depth 1 → 2: same invariant one level deeper.
		const { partitionContext: grandchild } = topology.pickChildPartition(child, kb(hashKey));
		const level2Siblings = PartitionIdHelper.calculateHashChildPartitionIds(child);
		for (const sib of level2Siblings) {
			const sibCtx: PartitionContextResolved = {
				...child,
				doName: sib.doName,
				partitionId: sib.partitionIdOpaque,
				primaryDoIdStr: "",
			};
			expect(topology.makeIsCorrectChildHashPartition(child, sibCtx)(kb(hashKey))).toBe(sib.doName === grandchild.doName);
		}
	});

	it("caches _partitionIdBytes in the DO's stored partition context for root and children", async ({ expect }) => {
		const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		// After the first request, ensurePartitionContext stores the context with _partitionIdBytes populated.
		await partition.put({ hashKey: kb("hk"), sortKey: kb("sk"), data: "v", kind: "text" as const });
		const rootState = await partition.status();
		expect(rootState.partitionContext?._partitionIdBytes).toBeInstanceOf(Uint8Array);
		expect(rootState.partitionContext?._partitionIdBytes).toEqual(Uint8Array.fromHex(partition.ctx.partitionId));

		// Split so children are initialized with their own cached bytes. splitHash drains the whole
		// tree, rather than only the parent's alarm, so no child is still migrating when this file
		// ends: background migration work that outlives the test worker breaks its teardown.
		for (const child of await partition.splitHash()) {
			const childState = await child.status();
			expect(childState.partitionContext?._partitionIdBytes).toBeInstanceOf(Uint8Array);
			expect(childState.partitionContext?._partitionIdBytes).toEqual(Uint8Array.fromHex(child.ctx.partitionId));
		}
	});
});
