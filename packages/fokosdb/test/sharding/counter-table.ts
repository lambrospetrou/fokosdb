import { env } from "cloudflare:workers";
import { expect } from "vitest";
import { FokosRouter, KeyCodec, type FokosPartitionRef } from "../../src/sharding/index.js";
import { FokosError } from "../../src/shared/errors.js";
import type { CounterPartitionDO, CounterPolicy, CounterStats } from "./counter-host.js";

/** How long a test waits for the runtime to finish a split. The fallback alarm of the runtime is 5 seconds. */
const SETTLE_TIMEOUT_MS = 20_000;
const POLL_MS = 50;

export function stub(doName: string): DurableObjectStub<CounterPartitionDO> {
	return env.COUNTER_PARTITION_DO.getByName(doName);
}

export type TreeNode = { ref: FokosPartitionRef; stats: CounterStats };

/**
 * One counter table of the example host, in its own shard group, so no two tests share partitions. The
 * table keeps the value that each key must have, to compare with the rows of the tree.
 */
export function makeCounterTable(maxRequests = 5) {
	const router = new FokosRouter<CounterPolicy>(
		{ shardGroup: `counter_${crypto.randomUUID()}`, rootTreesN: 1, hashSplitN: 4 },
		{ rangeSplitN: 4, rangeAncestors: { fromRoot: 0, fromLeaf: 3 } },
		{ maxRequests },
	);
	const root = router.allRoots()[0];
	const expected = new Map<string, number>();

	/** Sends one write through the root. It sends the write again while the owner of the key imports. */
	async function increment(key: string, amount = 1) {
		const hashKey = KeyCodec.encode(key);
		const ctx = router.rootContext(hashKey);
		const deadline = Date.now() + SETTLE_TIMEOUT_MS;
		for (;;) {
			try {
				const result = router.unwrap(await stub(ctx.doName).increment(ctx, { hashKey, amount }));
				expected.set(key, (expected.get(key) ?? 0) + amount);
				return result;
			} catch (err) {
				if (Date.now() > deadline || !FokosError.isCode(err, "partition_migrating")) {
					throw err;
				}
				await scheduler.wait(POLL_MS);
			}
		}
	}

	/** Reads every partition of the tree. A parent comes before its children. */
	async function tree(): Promise<TreeNode[]> {
		const nodes: TreeNode[] = [];
		const walk = async (ref: FokosPartitionRef) => {
			const stats = await stub(ref.doName).getCounterStats();
			nodes.push({ ref, stats });
			for (const child of stats.children) {
				await walk(child);
			}
		};
		await walk(root);
		return nodes;
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

	return { router, root, increment, tree, settle };
}
