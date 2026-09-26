import { env } from "cloudflare:workers";
import { expect } from "vitest";
import {
	FokosError,
	FokosShardingClient,
	KeyCodec,
	SHARDING_UNAVAILABLE_CODES,
	type FokosPartitionRef,
	type RouteKey,
} from "../../src/sharding/index.js";
import type { CounterOps, CounterPartitionDO, CounterPolicy, CounterStats } from "./counter-host.js";

/** How long a test waits for the runtime to finish a split. The fallback alarm of the runtime is 5 seconds. */
const SETTLE_TIMEOUT_MS = 20_000;
const POLL_MS = 50;

export function stub(doName: string): DurableObjectStub<CounterPartitionDO> {
	return env.COUNTER_PARTITION_DO.getByName(doName);
}

/** A counter key has no sort key. */
export function counterKey(key: string): RouteKey {
	return { hashKey: KeyCodec.encode(key), sortKey: KeyCodec.encodeOptional(undefined) };
}

export type TreeNode = { ref: FokosPartitionRef; parent: FokosPartitionRef | null; stats: CounterStats };

/**
 * One counter table of the example host, in its own shard group, so no two tests share partitions. The
 * table keeps the value that each key must have, to compare with the rows of the tree.
 */
export function makeCounterTable(maxRequests = 5) {
	const client = new FokosShardingClient<CounterPolicy, CounterOps>({
		topology: { shardGroup: `counter_${crypto.randomUUID()}`, rootTreesN: 1, hashSplitN: 4 },
		rangeConfig: { rangeSplitN: 4, rangeAncestors: { fromRoot: 0, fromLeaf: 3 } },
		policy: { maxRequests },
		stub: (_ctx, doName) => stub(doName),
	});
	// The table has one root, so every key resolves to it.
	const root = client.resolve(counterKey("root"));
	const expected = new Map<string, number>();

	/** Sends one write through the root. It sends the write again while the owner of the key imports. */
	async function increment(key: string, amount = 1) {
		const hashKey = KeyCodec.encode(key);
		const deadline = Date.now() + SETTLE_TIMEOUT_MS;
		const result = await client.point(
			"increment",
			counterKey(key),
			{ hashKey, amount },
			{
				retry: {
					shouldRetry: (err) => FokosError.isCode(err, SHARDING_UNAVAILABLE_CODES.partition_migrating) && Date.now() < deadline,
					baseDelayMs: POLL_MS,
					maxDelayMs: POLL_MS * 2,
				},
			},
		);
		expected.set(key, (expected.get(key) ?? 0) + amount);
		return result;
	}

	/** Reads every partition of the tree. A parent comes before its children. */
	async function tree(): Promise<TreeNode[]> {
		const nodes: TreeNode[] = [];
		for await (const node of client.walk()) {
			const ref = { partitionId: node.ctx.partitionId, doName: node.ctx.doName };
			nodes.push({ ref, parent: node.parent?.ref ?? null, stats: await stub(ref.doName).getCounterStats() });
		}
		return nodes;
	}

	/** The partitions that `parent` links as its targets. */
	function childrenOf(nodes: TreeNode[], parent: TreeNode): TreeNode[] {
		return nodes.filter((n) => n.parent?.doName === parent.ref.doName);
	}

	/**
	 * Waits until no partition splits or imports and `done` is true. Then it checks that the owners hold
	 * each key exactly once, with the value of every write that the test saw succeed.
	 */
	async function settle(done: (nodes: TreeNode[]) => boolean = () => true): Promise<TreeNode[]> {
		let nodes: TreeNode[] = [];
		const deadline = Date.now() + SETTLE_TIMEOUT_MS;
		for (;;) {
			nodes = await tree();
			const idle = nodes.every((n) => n.stats.repartitionState === null && (n.stats.importState ?? "active") === "active");
			if (idle && done(nodes)) {
				break;
			}
			if (Date.now() > deadline) {
				throw new Error(`the tree did not settle: ${JSON.stringify(nodes.map((n) => [n.ref.doName, n.stats]))}`);
			}
			await scheduler.wait(POLL_MS);
		}
		const rows = nodes.filter((n) => n.stats.role === "owner").flatMap((n) => n.stats.rows);
		expect(rows.map((r) => r.key).sort()).toEqual([...expected.keys()].sort());
		expect(new Map(rows.map((r) => [r.key, r.val]))).toEqual(expected);
		return nodes;
	}

	return { client, root, increment, tree, childrenOf, settle };
}
