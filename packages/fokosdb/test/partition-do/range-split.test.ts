import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import type { PartitionContextResolved } from "../../src/shared/partition-topology/partition-context.js";
import { resolveRangePartitionContext } from "../../src/shared/partition-topology/partition-id.js";
import { HashPartitionTopologyImpl } from "../../src/shared/partition-topology/split-policy.js";
import { KeyCodec } from "../../src/shared/partition-topology/key-codec.js";
import invariant from "../../src/shared/invariant.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import {
	PROMOTION_BIG_DATA,
	PROMOTION_TEST_MAX_SIZE_MB,
	drainSplitTree,
	kb,
	makeQueuedRangeRoot,
	makeStub,
	splitRangePartition,
	splitStatusOf,
	triggerHashSplitThreshold,
	waitForPromotedKeyStatus,
	waitForSplitCompleted,
} from "./helpers.js";

describe("PartitionDO — range split", () => {
	it("splits a populated leaf into N contiguous children covering [−∞, +∞); the node becomes a pure router", async () => {
		const N = 4;
		const { rootCtx, rootStub, sks } = await makeQueuedRangeRoot(N);
		expect(sks.length).toBeGreaterThanOrEqual(N);

		await waitForSplitCompleted(rootStub);

		const status = await splitStatusOf(rootStub);
		expect(status.status).toBe("split_completed");
		expect(status.childPartitionContexts).toHaveLength(N);

		// Children tile [−∞, +∞): sorted by start, first.start = null, last.end = null, end[i] === start[i+1].
		const children = [...status.childPartitionContexts].sort((a, b) =>
			(a.rangePartition!.startBoundary ?? "") < (b.rangePartition!.startBoundary ?? "") ? -1 : 1,
		);
		expect(children[0].rangePartition!.startBoundary).toBeNull();
		expect(children[N - 1].rangePartition!.endBoundary).toBeNull();
		for (let i = 0; i < N - 1; i++) {
			expect(children[i].rangePartition!.endBoundary).not.toBeNull();
			expect(children[i].rangePartition!.endBoundary).toBe(children[i + 1].rangePartition!.startBoundary);
		}
		expect(children.map((c) => c.rangePartition!.endBoundary)).toEqual([kb("sk004"), kb("sk009"), kb("sk014"), null]);

		// Every item is still readable through the router, and each read forwards exactly once.
		for (const sk of sks) {
			const g = await rootStub.apiGetItem(rootCtx, { hashKey: kb("alice"), sortKey: kb(sk) });
			expect(g.found, `sk ${sk} readable through router`).toBe(true);
			expect(g.meta.forwardCount).toBe(1);
		}
	});

	it("partitions every sort key into exactly one child and the router serves each via that child", async () => {
		const N = 4;
		const { rootCtx, rootStub, sks } = await makeQueuedRangeRoot(N);
		await waitForSplitCompleted(rootStub);
		const status = await splitStatusOf(rootStub);

		for (const sk of sks) {
			// The N children form a total partition of the sort-key axis: each written sk is owned by exactly one.
			const owners = status.childPartitionContexts.filter((c) => {
				const start = c.rangePartition!.startBoundary ?? KeyCodec.encodeOptional(undefined);
				const end = c.rangePartition!.endBoundary;
				return KeyCodec.compare(kb(sk), start) >= 0 && (end === null || KeyCodec.compare(kb(sk), end) < 0);
			});
			expect(owners, `sk ${sk} must be owned by exactly one child`).toHaveLength(1);

			// Reading through the router resolves to that exact child (servedByActorName = owning child's doName).
			const g = await rootStub.apiGetItem(rootCtx, { hashKey: kb("alice"), sortKey: kb(sk) });
			expect(g.found).toBe(true);
			expect(g.meta.servedByActorName, `sk ${sk} should be served by its owning child`).toBe(owners[0].doName);
		}
	});

	it("creates a brand-new leftmost child distinct from the router (no retain-leftmost)", async () => {
		const { rootCtx, rootStub } = await makeQueuedRangeRoot(4);
		await waitForSplitCompleted(rootStub);
		const status = await splitStatusOf(rootStub);

		const leftmost = status.childPartitionContexts.find((c) => c.rangePartition!.startBoundary === null);
		expect(leftmost, "a leftmost child [−∞, B1) must exist").toBeDefined();
		// The router keeps no slice: the leftmost child is a different DO than the splitting node.
		expect(leftmost!.doName).not.toBe(rootCtx.doName);
	});

	describe("rangeAncestors / rangeDepth propagation", () => {
		it("propagates rangeDepth and the ancestor set across two levels of splits", async () => {
			const N = 2;
			const { rootCtx, rootStub } = await makeQueuedRangeRoot(N);
			await waitForSplitCompleted(rootStub);
			const status = await splitStatusOf(rootStub);

			// Every depth-1 child: rangeDepth=1, rangeAncestors=[] (matches the M1 table: depth 1 → []).
			for (const childCtx of status.childPartitionContexts) {
				const childStub = PartitionDO.getByName(env.PARTITION_DO, childCtx.doName);
				const childRead = await childStub.apiGetItem(childCtx, {
					hashKey: kb("alice"),
					sortKey: childCtx.rangePartition!.startBoundary ?? kb(),
				});
				expect(childRead.meta.rangeDepth).toBe(1);
				expect(childRead.meta._internal.rangeAncestors).toEqual([]);
			}

			// A depth-2 grandchild's expected ancestor is its depth-1 parent's own boundaries, decoded to
			// wire form. Split both the leftmost child (start=null) and a non-leftmost one (start=KeyBytes)
			// so both the null and the decode-from-KeyBytes startBoundary paths are exercised.
			const expectAncestor = (childCtx: PartitionContextResolved) => {
				return {
					depth: 1,
					startBoundary: childCtx.rangePartition!.startBoundary ?? KeyCodec.encodeOptional(undefined),
					endBoundary: childCtx.rangePartition!.endBoundary ?? KeyCodec.encodeOptional(undefined),
				};
			};

			for (const childCtx of status.childPartitionContexts) {
				const childStub = PartitionDO.getByName(env.PARTITION_DO, childCtx.doName);
				const start = childCtx.rangePartition!.startBoundary;
				// Keys keyed to the child's own start land inside it; '~' (0x7E) sorts after alnum so they
				// stay >= a non-null start. The leftmost child (start=null) takes plain "aa…" keys.
				const keyPrefix = start === null ? "aa" : `${KeyCodec.decode(start) as string}~`;
				await splitRangePartition(childStub, childCtx, keyPrefix);

				const childSplit = await splitStatusOf(childStub);
				for (const grandchildCtx of childSplit.childPartitionContexts) {
					const grandchildStub = PartitionDO.getByName(env.PARTITION_DO, grandchildCtx.doName);
					expect((await grandchildStub.status()).depth).toBe(2);
				}

				// Reading through the root router surfaces the serving grandchild's own rangeDepth/rangeAncestors.
				const g = await rootStub.apiGetItem(rootCtx, { hashKey: kb("alice"), sortKey: kb(`${keyPrefix}0000`) });
				expect(g.found).toBe(true);
				expect(g.meta.rangeDepth).toBe(2);
				expect(g.meta._internal.rangeAncestors[0]).toEqual(expectAncestor(childCtx));
				// The last ancestor is the grandchild itself.
				expect(g.meta._internal.rangeAncestors).toHaveLength(2);
			}
		}, 30_000);

		it("is fully inert when rangeAncestorsConfig={fromRoot:0,fromLeaf:0}: every response has rangeAncestors:[]", async () => {
			const N = 2;
			const { rootCtx, rootStub } = await makeQueuedRangeRoot(N, { rangeAncestorsConfig: { fromRoot: 0, fromLeaf: 0 } });
			await waitForSplitCompleted(rootStub);
			const status = await splitStatusOf(rootStub);

			const leftChildCtx = status.childPartitionContexts.find((c) => c.rangePartition!.startBoundary === null)!;
			const leftChildStub = PartitionDO.getByName(env.PARTITION_DO, leftChildCtx.doName);
			await splitRangePartition(leftChildStub, leftChildCtx, "aa");

			// Depth-2 grandchild reached through the root: rangeDepth is still tracked, but rangeAncestors
			// stays [] regardless of depth — the feature is fully inert when the config is zeroed out.
			const g = await rootStub.apiGetItem(rootCtx, { hashKey: kb("alice"), sortKey: kb("aa0000") });
			expect(g.found).toBe(true);
			expect(g.meta.rangeDepth).toBe(2);
			expect(g.meta._internal.rangeAncestors).toEqual([]);
		});
	});

	describe("PartialRangeTopology", () => {
		it("reduces getItem forwardCount from 2 to 1 on second access when bloom filter has learned a key promoted from a hash-depth-2 leaf", async () => {
			const hashKey = "probe-key";
			const { ctx, stub } = makeStub({
				hashSplitN: 2,
				hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB },
			});
			let topology: HashPartitionTopologyImpl;
			await runInDurableObject(stub, async (instance: PartitionDO, doCtx: DurableObjectState) => {
				topology = new HashPartitionTopologyImpl(ctx, doCtx, new PartitionStore(doCtx.storage));
			});
			invariant(topology!, "topology should be initialized in the DO instance");

			// Build a two-level hash tree: root → child → grandchild (leaf at depth=2).
			await triggerHashSplitThreshold(stub, ctx, PROMOTION_TEST_MAX_SIZE_MB);
			await drainSplitTree(stub);
			const { partitionContext: childCtx } = topology.pickChildPartition(ctx, kb(hashKey));
			const childStub = PartitionDO.getByName(env.PARTITION_DO, childCtx.doName);
			await triggerHashSplitThreshold(childStub, childCtx, PROMOTION_TEST_MAX_SIZE_MB);
			await drainSplitTree(childStub);

			// Warm the root's hash topology cache so it can reach the depth-2 leaf in a single hop.
			// Cold: root→child→leaf (forwardCount=2). Warm: root→leaf directly (forwardCount=1).
			const rCold = await stub.apiGetItem(ctx, { hashKey: kb(hashKey), sortKey: kb("sk1") });
			expect(rCold.meta.forwardCount).toBe(2);
			const rWarm = await stub.apiGetItem(ctx, { hashKey: kb(hashKey), sortKey: kb("sk1") });
			expect(rWarm.meta.forwardCount).toBe(1);

			// Promote hashKey on the leaf (depth=2) that owns it.
			const { partitionContext: leafCtx } = topology.pickDescendantHashPartition(ctx, kb(hashKey), 2);
			const leafStub = PartitionDO.getByName(env.PARTITION_DO, leafCtx.doName);
			await leafStub.apiPutItem(leafCtx, { hashKey: kb(hashKey), sortKey: kb("sk1"), data: PROMOTION_BIG_DATA, kind: "text" as const });

			// Wait for promotion to complete: leaf detects heavy key → cutover ("promoting") →
			// range root migrates data → leaf acknowledges ("promoted").
			const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(leafCtx, kb(hashKey), null, null);
			const rangeRootStub = PartitionDO.getByName(env.PARTITION_DO, rangeRootCtx.doName);
			// Migration may complete in the same background cycle as cutover, so accept 'promoted' too.
			await waitForPromotedKeyStatus(leafStub, hashKey, ["promoting", "promoted"]);
			await waitForPromotedKeyStatus(leafStub, hashKey, ["promoted"], { drain: [rangeRootStub] });

			// First getItem through root after promotion:
			// Topology cache is warm → root goes directly to the leaf (1 hash hop).
			// Leaf's PromotionManager says "promoted" → forwards to range root (1 more hop).
			// Total forwardCount=2. Root also learns hashKey in its PartialRangeTopology bloom filter.
			const r1 = await stub.apiGetItem(ctx, { hashKey: kb(hashKey), sortKey: kb("sk1") });
			expect(r1.found).toBe(true);
			expect(r1.meta.forwardCount).toBe(2);

			// Second getItem through root:
			// Bloom filter now has hashKey → root bypasses the hash tree and goes directly to
			// the range root (1 hop) instead of the usual 2 hops (leaf → range root).
			const r2 = await stub.apiGetItem(ctx, { hashKey: kb(hashKey), sortKey: kb("sk1") });
			expect(r2.found).toBe(true);
			expect(r2.meta.forwardCount).toBe(1);
		}, 30_000);
	});
});
