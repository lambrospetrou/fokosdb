import { runInDurableObject } from "cloudflare:test";
import { beforeAll, describe, expect, it } from "vitest";
import type { PartitionDO } from "../../src/server/do-partition.js";
import type { FokosDBRouteContext } from "../../src/shared/partition-context.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { testCoordinatorRef } from "../stub-helpers.js";
import { executedBy, kb, rangeAncestorsOf, withOpIndex } from "./helpers.js";
import {
	PROMOTION_BIG_DATA,
	PROMOTION_TEST_MAX_SIZE_MB,
	type TestPartition,
	makePartition,
	makeTriggeredRangeRoot,
	rangeOf,
} from "./partition-harness.js";

describe.concurrent("PartitionDO — range split", () => {
	// The read-only tests walk the same settled N=4 tree, so it is built once. A test that needs
	// another N, another config, or that changes a child's stored policy builds its own.
	let shared: Awaited<ReturnType<typeof makeTriggeredRangeRoot>>;
	beforeAll(async () => {
		const built = await makeTriggeredRangeRoot(4);
		await built.root.awaitSplitCompleted();
		shared = built;
	});

	it("splits a populated leaf into N contiguous children covering [−∞, +∞); the node becomes a pure router", async () => {
		const N = 4;
		const { root, sks } = shared;
		expect(sks.length).toBeGreaterThanOrEqual(N);

		const status = await root.splitStatus();
		expect(status.status).toBe("split_completed");
		expect(status.childPartitionContexts).toHaveLength(N);

		// Children tile [−∞, +∞): sorted by start, first.start = null, last.end = null, end[i] === start[i+1].
		const children = [...status.childPartitionContexts].sort((a, b) =>
			(rangeOf(a).startBoundary ?? "") < (rangeOf(b).startBoundary ?? "") ? -1 : 1,
		);
		expect(rangeOf(children[0]).startBoundary).toBeNull();
		expect(rangeOf(children[N - 1]).endBoundary).toBeNull();
		for (let i = 0; i < N - 1; i++) {
			expect(rangeOf(children[i]).endBoundary).not.toBeNull();
			// Compared by value. A router builds each child context from its current context on every
			// call, so adjacent boundaries are equal bytes and not one shared object.
			expect(rangeOf(children[i]).endBoundary).toStrictEqual(rangeOf(children[i + 1]).startBoundary);
		}
		expect(children.map((c) => rangeOf(c).endBoundary)).toEqual([kb("sk004"), kb("sk009"), kb("sk014"), null]);

		// Every item is still readable through the router, and each read forwards exactly once.
		for (const sk of sks) {
			const g = await root.get({ hashKey: kb("alice"), sortKey: kb(sk) });
			expect(g.found, `sk ${sk} readable through router`).toBe(true);
			expect(g.meta.forwardCount).toBe(1);
		}
	});

	it("forwards its CURRENT mutable context to a child, not the snapshot taken at split time", async () => {
		// The split record stores a full child context, captured when the split ran. Forwarding that
		// snapshot hands the child split thresholds an operator has since changed, and the child then
		// persists the stale values as its own — a silent downgrade that survives every later request.
		const { root, sks } = await makeTriggeredRangeRoot(2);
		await root.awaitSplitCompleted();

		const child = (await root.children())[0];
		const splitTimeMaxSizeMb = child.ctx.policy.rangeSplitConditions.maxSizeMb!;
		const raisedMaxSizeMb = splitTimeMaxSizeMb * 4;

		// An operator raises the range split threshold; the new value travels with every request.
		const updatedCtx: FokosDBRouteContext = {
			...root.ctx,
			policy: { ...root.ctx.policy, rangeSplitConditions: { ...root.ctx.policy.rangeSplitConditions, maxSizeMb: raisedMaxSizeMb } },
		};
		const ownedSk = [...sks].sort()[0];
		const read = await root.rpc.apiGetItem(updatedCtx, { hashKey: kb("alice"), sortKey: kb(ownedSk) });
		expect(read.found, "the read must actually reach the child").toBe(true);
		expect(read.meta.servedByActorName).toBe(child.doName);

		// Read the child's stored policy WITHOUT a request, so the assertion observes what the router
		// forwarded rather than writing the threshold itself.
		const stored = await runInDurableObject(child.stub, (instance: PartitionDO) => instance.fokos.policy());
		expect(stored.rangeSplitConditions.maxSizeMb).toBe(raisedMaxSizeMb);
	});

	it("partitions every sort key into exactly one child and the router serves each via that child", async () => {
		const { root, sks } = shared;
		const status = await root.splitStatus();

		for (const sk of sks) {
			// The N children form a total partition of the sort-key axis: each written sk is owned by exactly one.
			const owners = status.childPartitionContexts.filter((c) => {
				const start = rangeOf(c).startBoundary ?? KeyCodec.encodeOptional(undefined);
				const end = rangeOf(c).endBoundary;
				return KeyCodec.compare(kb(sk), start) >= 0 && (end === null || KeyCodec.compare(kb(sk), end) < 0);
			});
			expect(owners, `sk ${sk} must be owned by exactly one child`).toHaveLength(1);

			// Reading through the router resolves to that exact child (servedByActorName = owning child's doName).
			const g = await root.get({ hashKey: kb("alice"), sortKey: kb(sk) });
			expect(g.found).toBe(true);
			expect(g.meta.servedByActorName, `sk ${sk} should be served by its owning child`).toBe(owners[0].doName);
		}
	});

	it("creates a brand-new leftmost child distinct from the router (no retain-leftmost)", async () => {
		const { root } = shared;
		const status = await root.splitStatus();

		const leftmost = status.childPartitionContexts.find((c) => rangeOf(c).startBoundary === null);
		expect(leftmost, "a leftmost child [−∞, B1) must exist").toBeDefined();
		// The router keeps no slice: the leftmost child is a different DO than the splitting node.
		expect(leftmost!.doName).not.toBe(root.doName);
	});

	describe("rangeAncestors / rangeDepth propagation", () => {
		it("propagates rangeDepth and the ancestor set across two levels of splits", async () => {
			const N = 2;
			const { root } = await makeTriggeredRangeRoot(N);
			await root.awaitSplitCompleted();

			// Every depth-1 child: rangeDepth=1, rangeAncestors=[] (a depth-1 partition has no ancestor entry).
			const children = await root.children();
			for (const child of children) {
				const childRead = await child.stub.apiGetItem(child.ctx, {
					hashKey: kb("alice"),
					sortKey: rangeOf(child.ctx).startBoundary ?? kb(),
				});
				expect(executedBy(childRead).rangeDepth).toBe(1);
				expect(rangeAncestorsOf(childRead)).toEqual([]);
			}

			// A depth-2 grandchild's expected ancestor is its depth-1 parent's own boundaries, decoded to
			// wire form. Split both the leftmost child (start=null) and a non-leftmost one (start=KeyBytes)
			// so both the null and the decode-from-KeyBytes startBoundary paths are exercised.
			const expectAncestor = (childCtx: FokosDBRouteContext) => {
				return {
					depth: 1,
					startBoundary: rangeOf(childCtx).startBoundary ?? KeyCodec.encodeOptional(undefined),
					endBoundary: rangeOf(childCtx).endBoundary ?? KeyCodec.encodeOptional(undefined),
				};
			};

			for (const child of children) {
				const start = rangeOf(child.ctx).startBoundary;
				// Keys keyed to the child's own start land inside it; '~' (0x7E) sorts after alnum so they
				// stay >= a non-null start. The leftmost child (start=null) takes plain "aa…" keys.
				const keyPrefix = start === null ? "aa" : `${KeyCodec.decode(start) as string}~`;
				for (const grandchild of await child.splitRange(keyPrefix)) {
					expect((await grandchild.status()).depth).toBe(2);
				}

				// Reading through the root router surfaces the serving grandchild's own rangeDepth/rangeAncestors.
				const g = await root.stub.apiGetItem(root.ctx, { hashKey: kb("alice"), sortKey: kb(`${keyPrefix}0000`) });
				expect(g.value.found).toBe(true);
				expect(executedBy(g).rangeDepth).toBe(2);
				// The only ancestor is the depth-1 parent. The own slice of the grandchild is in its partition ID.
				expect(rangeAncestorsOf(g)).toEqual([expectAncestor(child.ctx)]);
			}
		}, 30_000);

		it("is fully inert when rangeAncestorsConfig={fromRoot:0,fromLeaf:0}: every response has rangeAncestors:[]", async () => {
			const N = 2;
			const { root } = await makeTriggeredRangeRoot(N, { rangeAncestorsConfig: { fromRoot: 0, fromLeaf: 0 } });
			await root.awaitSplitCompleted();

			const children = await root.children();
			const leftChild = children.find((c) => rangeOf(c.ctx).startBoundary === null)!;
			await leftChild.splitRange("aa");

			// Depth-2 grandchild reached through the root: rangeDepth is still tracked, but rangeAncestors
			// stays [] regardless of depth — the feature is fully inert when the config is zeroed out.
			const g = await root.stub.apiGetItem(root.ctx, { hashKey: kb("alice"), sortKey: kb("aa0000") });
			expect(g.value.found).toBe(true);
			expect(executedBy(g).rangeDepth).toBe(2);
			expect(rangeAncestorsOf(g)).toEqual([]);
		});
	});

	describe("learned range slices", () => {
		/** The depths of the rows in `fokos_range_hierarchy` of `partition`. */
		const learnedDepths = async (partition: TestPartition) =>
			await runInDurableObject(partition.stub, (_instance: PartitionDO, state: DurableObjectState) =>
				state.storage.sql
					.exec<{ depth: number }>(`SELECT depth FROM fokos_range_hierarchy`)
					.toArray()
					.map((row) => row.depth),
			);

		it("a range root stores only the slices strictly inside one of its children", async () => {
			const { root } = await makeTriggeredRangeRoot(2);
			await root.awaitSplitCompleted();
			const leftChild = (await root.children()).find((c) => rangeOf(c.ctx).startBoundary === null)!;
			const leftGrandchild = (await leftChild.splitRange("aa")).find((c) => rangeOf(c.ctx).startBoundary === null)!;
			// "a0000" sorts before each "aa…" key, so it lands in the leftmost grandchild.
			await leftGrandchild.splitRange("a");

			const g = await root.stub.apiGetItem(root.ctx, { hashKey: kb("alice"), sortKey: kb("a0000") });
			expect(g.value.found).toBe(true);
			expect(executedBy(g).rangeDepth).toBe(3);

			const depths = await learnedDepths(root);
			expect(depths).toContain(3);
			expect(depths.every((depth) => depth >= 2)).toBe(true);
		}, 30_000);

		it("a hash partition jumps to a depth-1 owner of a promoted key on the second read", async () => {
			const { root, sks, hashPartition } = await makeTriggeredRangeRoot(2);
			await root.awaitSplitCompleted();

			const first = await hashPartition.get({ hashKey: kb("alice"), sortKey: kb(sks[0]) });
			expect(first.found).toBe(true);
			const second = await hashPartition.get({ hashKey: kb("alice"), sortKey: kb(sks[0]) });
			expect(second.found).toBe(true);
			// The first read goes through the range root. The second goes directly to the depth-1 owner.
			expect(first.meta.forwardCount).toBe(2);
			expect(second.meta.forwardCount).toBe(1);
		});

		it("a transaction through the partition that holds the route override jumps to the depth-2 owner", async () => {
			const { root, hashPartition } = await makeTriggeredRangeRoot(2);
			await root.awaitSplitCompleted();
			const leftChild = (await root.children()).find((c) => rangeOf(c.ctx).startBoundary === null)!;
			const owner = (await leftChild.splitRange("aa")).find((c) => rangeOf(c.ctx).startBoundary === null)!;

			// One read goes through the range root and the depth-1 child, and it teaches the hash partition
			// the slice of the owner.
			const read = await hashPartition.stub.apiGetItem(hashPartition.ctx, { hashKey: kb("alice"), sortKey: kb("aa0000") });
			expect(read.routing.forwardCount).toBe(3);
			expect(executedBy(read).ref.doName).toBe(owner.doName);

			// "a0" sorts before each "aa…" key, so the owner of "aa0000" owns it too.
			const key = { hashKey: kb("alice"), sortKey: kb("a0") };
			const transactionId = crypto.randomUUID();
			const transactionTimestamp = Date.now();
			const prepare = await hashPartition.stub.txPrepare(hashPartition.ctx, {
				transactionId,
				transactionTimestamp,
				coordinator: testCoordinatorRef(),
				items: withOpIndex([{ ...key, operation: "put", data: "v", kind: "text" }]),
			});
			expect(prepare.value).toMatchObject({ outcome: "accepted" });
			expect(prepare.routing.forwardCount).toBe(1);
			expect(executedBy(prepare).ref.doName).toBe(owner.doName);

			const commit = await hashPartition.stub.txCommit(hashPartition.ctx, { transactionId, transactionTimestamp, items: [key] });
			expect(commit.value).toMatchObject({ outcome: "committed" });
			expect(commit.routing.forwardCount).toBe(1);
			expect(executedBy(commit).ref.doName).toBe(owner.doName);

			const snapshot = await hashPartition.stub.txReadSnapshot(hashPartition.ctx, { items: [key] });
			expect(snapshot.value).toMatchObject({ outcome: "committed", items: [{ found: true, data: "v" }] });
			expect(snapshot.routing.forwardCount).toBe(1);
			expect(executedBy(snapshot).ref.doName).toBe(owner.doName);
		}, 30_000);

		it("a hash partition jumps to a depth-2 owner when rangeAncestorsConfig={fromRoot:0,fromLeaf:0}", async () => {
			const { root, hashPartition } = await makeTriggeredRangeRoot(2, { rangeAncestorsConfig: { fromRoot: 0, fromLeaf: 0 } });
			await root.awaitSplitCompleted();
			const leftChild = (await root.children()).find((c) => rangeOf(c.ctx).startBoundary === null)!;
			await leftChild.splitRange("aa");

			const first = await hashPartition.get({ hashKey: kb("alice"), sortKey: kb("aa0000") });
			expect(first.found).toBe(true);
			const second = await hashPartition.get({ hashKey: kb("alice"), sortKey: kb("aa0000") });
			expect(second.found).toBe(true);
			// The first read goes through the range root and the depth-1 child. The second goes directly to the owner.
			expect(first.meta.forwardCount).toBe(3);
			expect(second.meta.forwardCount).toBe(1);
		}, 30_000);
	});

	describe("PartialRangeTopology", () => {
		it("reduces getItem forwardCount from 2 to 1 on second access when bloom filter has learned a key promoted from a hash-depth-2 leaf", async () => {
			const hashKey = "probe-key";
			const root = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });

			// Build a two-level hash tree: root → child → grandchild (leaf at depth=2).
			await root.splitHash();
			await (await root.childOwning(hashKey)).splitHash();

			// Warm the root's hash topology cache so it can reach the depth-2 leaf in a single hop.
			// Cold: root→child→leaf (forwardCount=2). Warm: root→leaf directly (forwardCount=1).
			const rCold = await root.get({ hashKey: kb(hashKey), sortKey: kb("sk1") });
			expect(rCold.meta.forwardCount).toBe(2);
			const rWarm = await root.get({ hashKey: kb(hashKey), sortKey: kb("sk1") });
			expect(rWarm.meta.forwardCount).toBe(1);

			// Promote hashKey on the leaf (depth=2) that owns it.
			const leaf = await root.leafOwning(hashKey);
			await leaf.put({ hashKey: kb(hashKey), sortKey: kb("sk1"), data: PROMOTION_BIG_DATA, kind: "text" as const });

			// Wait for promotion to complete: leaf detects heavy key → cutover ("promoting") →
			// range root migrates data → leaf acknowledges ("promoted"). Migration may complete in the
			// same background cycle as cutover, so the first wait accepts "promoted" too.
			const rangeRoot = leaf.rangeRoot(hashKey);
			await leaf.awaitPromotedKeyStatus(hashKey, ["promoting", "promoted"]);
			await leaf.awaitPromotedKeyStatus(hashKey, ["promoted"], { drive: [rangeRoot] });

			// First getItem through root after promotion:
			// Topology cache is warm → root goes directly to the leaf (1 hash hop).
			// Leaf's PromotionManager says "promoted" → forwards to range root (1 more hop).
			// Total forwardCount=2. Root also learns hashKey in its PartialRangeTopology bloom filter.
			const r1 = await root.get({ hashKey: kb(hashKey), sortKey: kb("sk1") });
			expect(r1.found).toBe(true);
			expect(r1.meta.forwardCount).toBe(2);

			// Second getItem through root:
			// Bloom filter now has hashKey → root bypasses the hash tree and goes directly to
			// the range root (1 hop) instead of the usual 2 hops (leaf → range root).
			const r2 = await root.get({ hashKey: kb(hashKey), sortKey: kb("sk1") });
			expect(r2.found).toBe(true);
			expect(r2.meta.forwardCount).toBe(1);
		}, 30_000);
	});
});
