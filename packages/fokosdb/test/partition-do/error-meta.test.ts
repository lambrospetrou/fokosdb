import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import type { KeyBytes } from "../../src/shared/partition-topology/key-codec.js";
import type { PartitionInfoInternal } from "../../src/shared/partition-topology/types.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { kb, withOpIndex } from "./helpers.js";
import { PROMOTION_BIG_DATA, PROMOTION_TEST_MAX_SIZE_MB, type TestPartition, makePartition } from "./partition-harness.js";

/**
 * A partition stamps its routing meta on an error, and each forwarding level learns from it and changes
 * it as it does with the meta of a result. So an error and a result that take the same route carry the
 * same routing meta, at every level.
 */

type ItemKeys = { hashKey: KeyBytes; sortKey: KeyBytes };

/** Holds a prepared transaction lock on the item, so a write to it fails where the item lives. */
async function lockItem(node: TestPartition, keys: ItemKeys): Promise<() => Promise<unknown>> {
	const transactionId = crypto.randomUUID();
	const res = await node.stub.txPrepare(node.ctx, {
		transactionId,
		transactionTimestamp: Date.now(),
		coordinatorDoId: env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString(),
		items: withOpIndex([{ ...keys, operation: "put", data: "pending", kind: "text" }]),
	});
	expect(res.outcome).toBe("accepted");
	return () => node.stub.txCancel(node.ctx, { transactionId, items: [keys] });
}

/** The routing meta on the error of a write to a locked item. */
async function stampedMeta(node: TestPartition, keys: ItemKeys): Promise<unknown> {
	const err = await node.put({ ...keys, data: "blocked", kind: "text" }).then(
		() => {
			throw new Error(`${node.doName}: the write did not fail`);
		},
		(e: unknown) => e,
	);
	expect(err).toEqual(fokosErrorWith("item_locked_by_transaction"));
	return (err as { meta?: unknown }).meta;
}

/** The routing part of a result meta, without the metrics of the work. */
function routingOf(meta: PartitionInfoInternal): PartitionInfoInternal {
	const { servedByActorId, servedByActorName, servedByPartitionId, forwardCount, hashDepth, rangeDepth, _internal } = meta;
	return { servedByActorId, servedByActorName, servedByPartitionId, forwardCount, hashDepth, rangeDepth, _internal };
}

/** Reads the item and then writes it through `node`, and expects the same routing meta on both. */
async function expectSameMeta(node: TestPartition, keys: ItemKeys): Promise<void> {
	const { meta } = await node.get(keys);
	expect(await stampedMeta(node, keys), node.doName).toEqual(routingOf(meta));
}

describe("PartitionDO — routing meta on errors", () => {
	it("matches the result meta at each hash forwarding level, and the root learns the leaf depth from an error", async () => {
		const hashKey = "probe-key";
		const root = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		await root.splitHash();
		const child = await root.childOwning(hashKey);
		await child.splitHash();
		const leaf = await root.leafOwning(hashKey);
		const keys = { hashKey: kb(hashKey), sortKey: kb("sk") };
		const leafMeta = routingOf((await leaf.get(keys)).meta);
		const release = await lockItem(leaf, keys);
		try {
			// Cold cache: root → child → leaf. Each of the two forwarding levels adds one forward, as on a result.
			expect(await stampedMeta(root, keys)).toEqual({ ...leafMeta, forwardCount: 2 });
			// The root learned the depth of the leaf from the error, so it now skips the child.
			expect((await root.get(keys)).meta.forwardCount).toBe(1);

			for (const node of [leaf, child, root]) await expectSameMeta(node, keys);
		} finally {
			await release();
		}
	}, 30_000);

	it("matches the result meta through a hash-to-range hop and a range router", async () => {
		const hash = makePartition({ rangeSplitN: 2, rangeSplitConditions: { maxSizeMb: 1 } });
		await hash.stub.debugForcePromoteKey(hash.ctx, kb("alice"));
		const rangeRoot = await hash.awaitPromoted("alice");
		const children = await rangeRoot.splitRange("sk");
		const keys = { hashKey: kb("alice"), sortKey: kb("sk0000") };
		const { servedByActorName } = (await rangeRoot.get(keys)).meta;
		const leaf = children.find((c) => c.doName === servedByActorName)!;
		// A first read settles what the hash partition learns about the range tree, so both calls below take one route.
		await hash.get(keys);
		const release = await lockItem(leaf, keys);
		try {
			for (const node of [leaf, rangeRoot, hash]) await expectSameMeta(node, keys);
		} finally {
			await release();
		}
	}, 30_000);

	it("teaches a hash router that a key below it is promoted, from an error", async () => {
		const hashKey = "probe-key";
		const root = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		await root.splitHash();
		const child = await root.childOwning(hashKey);
		await child.put({ hashKey: kb(hashKey), sortKey: kb("sk1"), data: PROMOTION_BIG_DATA, kind: "text" });
		const rangeRoot = await child.awaitPromoted(hashKey);
		const keys = { hashKey: kb(hashKey), sortKey: kb("sk1") };
		const release = await lockItem(rangeRoot, keys);
		try {
			// root → child → range root. The child answers with its own hash depth, as on a result, so the
			// root can learn from the error.
			expect(await stampedMeta(root, keys)).toMatchObject({
				servedByActorName: rangeRoot.doName,
				forwardCount: 2,
				hashDepth: 1,
				rangeDepth: 0,
			});
			// The root learned the promotion, so it now forwards to the range root directly.
			expect((await root.get(keys)).meta.forwardCount).toBe(1);

			await expectSameMeta(root, keys);
		} finally {
			await release();
		}
	}, 30_000);
});
