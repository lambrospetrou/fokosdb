import { describe, expect, it } from "vitest";
import type { KeyBytes } from "../../src/sharding/key-codec.js";
import { routedError } from "../../src/sharding/envelope.js";
import type { FokosRouting } from "../../src/sharding/runtime-types.js";
import { txOrderTimestampNow } from "../../src/shared/transaction-limits.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { testCoordinatorRef } from "../stub-helpers.js";
import { kb, withOpIndex } from "./helpers.js";
import { PROMOTION_BIG_DATA, PROMOTION_TEST_MAX_SIZE_MB, type TestPartition, makePartition } from "./partition-harness.js";

/**
 * A partition attaches its routing to an error, and each forwarding level learns from it and merges
 * it as it does with the routing of a result. So an error and a result that take the same route list
 * the same partitions and count the same hops, at every level.
 */

type ItemKeys = { hashKey: KeyBytes; sortKey: KeyBytes };

/** Holds a prepared transaction lock on the item, so a write to it fails where the item lives. */
async function lockItem(node: TestPartition, keys: ItemKeys): Promise<() => Promise<unknown>> {
	const transactionId = crypto.randomUUID();
	const res = await node.rpc.txPrepare(node.ctx, {
		transactionId,
		transactionTimestamp: txOrderTimestampNow(),
		coordinator: testCoordinatorRef(),
		items: withOpIndex([{ ...keys, operation: "put", data: "pending", kind: "text" }]),
	});
	expect(res.outcome).toBe("accepted");
	return () => node.rpc.txCancel(node.ctx, { transactionId, items: [keys] });
}

/** The routing facts an error and a result share: the partitions that served it, and the hops to reach them. */
type Route = Pick<FokosRouting, "servedBy" | "forwardCount">;

function routeOf(routing: FokosRouting | undefined): Route | undefined {
	return routing && { servedBy: routing.servedBy, forwardCount: routing.forwardCount };
}

/** The routing on the error of a write to a locked item. */
async function erroredRouting(node: TestPartition, keys: ItemKeys): Promise<Route | undefined> {
	const err = await node.stub.apiPutItem(node.ctx, { ...keys, data: "blocked", kind: "text" }).then(
		() => {
			throw new Error(`${node.doName}: the write did not fail`);
		},
		(e: unknown) => e,
	);
	expect(err).toEqual(fokosErrorWith("item_locked_by_transaction"));
	return routeOf(routedError(err)?.routing);
}

/** The routing of a read of the item through `node`. */
async function readRouting(node: TestPartition, keys: ItemKeys): Promise<Route> {
	return routeOf((await node.stub.apiGetItem(node.ctx, keys)).routing)!;
}

/** Reads the item and then writes it through `node`, and expects the same routing on both. */
async function expectSameRouting(node: TestPartition, keys: ItemKeys): Promise<void> {
	expect(await erroredRouting(node, keys), node.doName).toEqual(await readRouting(node, keys));
}

describe.concurrent("PartitionDO — routing on errors", () => {
	it("matches the result routing at each hash forwarding level, and the root learns the leaf depth from an error", async () => {
		const hashKey = "probe-key";
		const root = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		await root.splitHash();
		const child = await root.childOwning(hashKey);
		await child.splitHash();
		const leaf = await root.leafOwning(hashKey);
		const keys = { hashKey: kb(hashKey), sortKey: kb("sk") };
		const leafRouting = await readRouting(leaf, keys);
		const release = await lockItem(leaf, keys);
		try {
			// Cold cache: root → child → leaf. Each of the two forwarding levels adds one forward, as on a result.
			expect(await erroredRouting(root, keys)).toEqual({ ...leafRouting, forwardCount: 2 });
			// The root learned the depth of the leaf from the error, so it now skips the child.
			expect((await readRouting(root, keys)).forwardCount).toBe(1);

			for (const node of [leaf, child, root]) await expectSameRouting(node, keys);
		} finally {
			await release();
		}
	}, 30_000);

	it("matches the result routing through a hash-to-range hop and a range router", async () => {
		const hash = makePartition({ rangeSplitN: 2, rangeSplitConditions: { maxSizeMb: 1 } });
		await hash.rpc.debugForcePromoteKey(hash.ctx, { hashKey: kb("alice") });
		const rangeRoot = await hash.awaitPromoted("alice");
		const children = await rangeRoot.splitRange("sk");
		const keys = { hashKey: kb("alice"), sortKey: kb("sk0000") };
		const { servedByActorName } = (await rangeRoot.get(keys)).meta;
		const leaf = children.find((c) => c.doName === servedByActorName)!;
		// A first read settles what the hash partition learns about the range tree, so both calls below take one route.
		await hash.get(keys);
		const release = await lockItem(leaf, keys);
		try {
			for (const node of [leaf, rangeRoot, hash]) await expectSameRouting(node, keys);
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
			// root → child → range root. The child stamps its own hash depth on the range evidence, as on a
			// result, so the root can learn from the error.
			expect(await erroredRouting(root, keys)).toMatchObject({
				servedBy: [{ ref: { doName: rangeRoot.doName }, hashDepth: 1, rangeDepth: 0, role: "executed" }],
				forwardCount: 2,
			});
			// The root learned the promotion, so it now forwards to the range root directly.
			expect((await readRouting(root, keys)).forwardCount).toBe(1);

			await expectSameRouting(root, keys);
		} finally {
			await release();
		}
	}, 30_000);
});
