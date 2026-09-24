import { Hono } from "hono";
import { FokosRouter } from "fokosdb/client";
import { KeyCodec } from "fokosdb/sharding";
import type { CounterPolicy, CounterStats } from "./counter-host.js";
import { collectTree, jsonBody, resetTree, retryWhileMigrating, topologyOf, traceOf, type ActionTrace, type TreeNode } from "./shared.js";

/** Demo 1 routes: counter writes, the kill of a splitting partition, and the reconciliation of the writes. */

const MAX_REQUESTS = 5;
const router = new FokosRouter<CounterPolicy>(
	{ shardGroup: "counter_demo", rootTreesN: 1, hashSplitN: 4 },
	{ rangeSplitN: 4, rangeAncestors: { fromRoot: 0, fromLeaf: 3 } },
	{ maxRequests: MAX_REQUESTS },
);
const root = router.allRoots()[0];

/**
 * A write goes to a random key of 32 keys, so the writes spread over the partitions. The hash of a
 * fixed key set always selects the same children, and then only one branch of the tree grows.
 */
const KEY_COUNT = 32;
const randomKey = () => `counter-${Math.floor(Math.random() * KEY_COUNT)}`;

function stub(env: Env, doName: string) {
	return env.COUNTER_PARTITION_DO.get(env.COUNTER_PARTITION_DO.idFromName(doName));
}

function collectCounterTree(env: Env): Promise<TreeNode<CounterStats>[]> {
	return collectTree(root, (doName) => stub(env, doName).getCounterStats());
}

/**
 * Compares the writes that the Worker saw succeed with the sum of the rows in the tree. A router
 * keeps an old copy of its rows until every child has acknowledged its import. A child comes after
 * its parent in the list, so the last value for a key is the current value.
 */
async function reconcile(env: Env, nodes: TreeNode<CounterStats>[]) {
	const acknowledgedWrites = await stub(env, root.doName).getAcknowledged();
	const values = new Map<string, number>();
	for (const node of nodes) for (const r of node.stats.rows) values.set(r.key, r.val);
	const presentWrites = [...values.values()].reduce((a, b) => a + b, 0);
	return { acknowledgedWrites, presentWrites, reconciled: acknowledgedWrites === presentWrites };
}

async function increment(env: Env, key: string, amount: number) {
	const hashKey = KeyCodec.encode(key);
	const ctx = router.rootContext(hashKey);
	const { value, routing } = await retryWhileMigrating(async () =>
		router.unwrap<{ key: string; val: number }>(await stub(env, ctx.doName).increment(ctx, { hashKey, amount })),
	);
	await stub(env, root.doName).recordAcknowledged(amount);
	return { value, trace: traceOf(ctx, routing) };
}

export const counterRoutes = new Hono<{ Bindings: Env }>();

counterRoutes.get("/topology", async (c) => {
	const nodes = await collectCounterTree(c.env);
	const topology = topologyOf("demo1", nodes, ({ stats }) => ({
		role: stats.role === "router" ? "router" : "leaf",
		kind: "hash",
		status: stats.repartitionState ?? "active",
		importState: stats.importState,
		// A hash split cannot divide one key, so the host keeps a partition with one hot key as it is.
		label: stats.role === "owner" && stats.rows.length === 1 && stats.requestCount >= MAX_REQUESTS ? "1 key: no split" : null,
		itemCount: stats.rows.length,
		requestCount: stats.requestCount,
	}));
	return c.json({ ...topology, reconciliation: await reconcile(c.env, nodes) });
});

counterRoutes.post("/increment", async (c) => {
	const { key = randomKey(), amount = 1 } = await jsonBody<{ key: string; amount: number }>(c);
	const { value, trace } = await increment(c.env, key, amount);
	return c.json({ success: true, result: value, trace });
});

counterRoutes.post("/batch-increment", async (c) => {
	const { count = 5 } = await jsonBody<{ count: number }>(c);
	let trace: ActionTrace | undefined;
	for (let i = 0; i < count; i++) ({ trace } = await increment(c.env, randomKey(), 1));
	return c.json({ success: true, result: { count }, trace });
});

counterRoutes.post("/kill", async (c) => {
	// The source of a split holds the pages that its children still pull, so the demo crashes the source.
	const victim = (await collectCounterTree(c.env)).find((n) => n.stats.repartitionState !== null);
	if (!victim) return c.json({ success: false, error: "no partition is splitting now" });
	// `ctx.abort()` ends the call with an error, so the error means success.
	await stub(c.env, victim.ref.doName)
		.debugAbort()
		.catch(() => {});
	return c.json({ success: true, result: { killed: victim.ref.doName } });
});

counterRoutes.post("/reset", async (c) => {
	await resetTree(router, (doName) => stub(c.env, doName));
	return c.json({ success: true });
});
