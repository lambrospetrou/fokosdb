import { runInDurableObject } from "cloudflare:test";
import { describe, it } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import { PartitionIdHelper } from "../../src/sharding/partition-id.js";
import { sliceIncludesHashKey } from "../../src/sharding/repartition-slice.js";
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
			expect(children[i].doName).toBe(`${ctx.topology.shardGroup}.h.0.${i}`);
		}
	});

	it("owner resolution and the hash-child slice agree", async ({ expect }) => {
		// This test guards the entropy consistency between routing and migration filtering. If the depth
		// offset used in one changes without the other, routing silently assigns keys to different
		// partitions than the migration filter expects.
		const partition = makePartition({ hashSplitN: 4, hashSplitConditions: { maxSizeMb: 1 } });
		const children = await partition.splitHash();
		const hashKey = "routing-consistency-key";
		const owner = await runInDurableObject(partition.stub, (instance: PartitionDO) =>
			instance.fokos.resolveOwner({ hashKey: kb(hashKey), sortKey: kb("sk") }),
		);
		expect(owner.kind).toBe("remote");
		const ownerName = owner.kind === "remote" ? owner.target.doName : undefined;

		for (const child of children) {
			const idBytes = Uint8Array.fromHex(child.ctx.partitionId);
			const slice = {
				kind: "hash_child" as const,
				childIndex: PartitionIdHelper.lastChildIdx(idBytes),
				depth: PartitionIdHelper.depth(idBytes),
			};
			expect(sliceIncludesHashKey(slice, kb(hashKey), child.ctx.topology.hashSplitN)).toBe(child.doName === ownerName);
		}
	});

	it("stores the decoded hash identity of the root and of every child", async ({ expect }) => {
		const partition = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		// The first request bootstraps the root: it stores the identity its route context decodes to.
		await partition.put({ hashKey: kb("hk"), sortKey: kb("sk"), data: "v", kind: "text" as const });
		const rootState = await partition.status();
		expect(rootState.identityStored).toEqual({
			schema: 1,
			ref: { partitionId: partition.ctx.partitionId, doName: partition.ctx.doName },
			kind: "hash",
			hash: { rootIndex: 0, path: [] },
			topology: partition.ctx.topology,
		});

		// Split so children are initialized through fokosInit. splitHash drains the whole tree, rather
		// than only the parent's alarm, so no child is still migrating when this file ends: background
		// migration work that outlives the test worker breaks its teardown.
		const children = await partition.splitHash();
		for (let i = 0; i < children.length; i++) {
			const childState = await children[i].status();
			expect(childState.identityStored).toMatchObject({
				kind: "hash",
				hash: { rootIndex: 0, path: [i] },
				ref: { doName: children[i].doName },
			});
			expect(childState.depth).toBe(1);
		}
	});
});
