import { runInDurableObject } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it, vi } from "vitest";
import {
	FokosShardingClient,
	KeyCodec,
	hashChildIndex,
	resolveDescendantHashPartitionContext,
	fokosTopologyEntrypointFetch,
	topologyAdminName,
	type FokosTopologyRead,
} from "../../src/sharding/index-server.js";
import { parseHashBody } from "../../src/sharding/topology-codec.js";
import type { CounterOps, CounterPolicy } from "./counter-host.js";
import { counterKey, makeCounterTable, stub } from "./counter-table.js";
import { hashSplitReport } from "./topology-events.js";

const WAIT = { timeout: 20_000, interval: 50 };

function admin(shardGroup: string) {
	return env.TOPOLOGY_ADMIN_DO.getByName(topologyAdminName(shardGroup));
}

/** The hash body that the admin of the shard group serves now. */
async function servedTopology(shardGroup: string) {
	const read = (await admin(shardGroup).getTopology(shardGroup)) as FokosTopologyRead;
	return parseHashBody((read as { body: Uint8Array }).body);
}

/** A table whose root split once. The five writes go to more than one key. */
async function splitTable() {
	const table = makeCounterTable();
	return { table, shardGroup: table.root.topology.shardGroup };
}

async function splitRoot(table: ReturnType<typeof makeCounterTable>) {
	for (let i = 0; i < 5; i++) {
		await table.increment(`k-${i}`);
	}
	return await table.settle((n) => n[0].stats.role === "router");
}

describe.concurrent("Sharding runtime — admin outbox and topology", () => {
	it("reports a completed split with the actor IDs, and a client with the topology enters at the leaf", async () => {
		const { table, shardGroup } = await splitTable();
		const nodes = await splitRoot(table);
		await vi.waitFor(async () => expect((await servedTopology(shardGroup)).bitCount).toBe(1 + 4), WAIT);

		const reports = await runInDurableObject(admin(shardGroup), async (_i, state) =>
			state.storage.sql
				.exec<{ actor_id: string; data: string }>("SELECT actor_id, json(data) AS data FROM fokos_topo_repartitions")
				.toArray(),
		);
		expect(reports).toHaveLength(1);
		expect(reports[0].actor_id).toBe(env.COUNTER_PARTITION_DO.idFromName(table.root.doName).toString());
		const event = JSON.parse(reports[0].data) as { targets: Array<{ ref: { doName: string }; actorId: string }> };
		expect(event.targets.map((t) => t.ref.doName).sort()).toEqual(
			table
				.childrenOf(nodes, nodes[0])
				.map((c) => c.ref.doName)
				.sort(),
		);
		for (const t of event.targets) {
			expect(t.actorId).toBe(env.COUNTER_PARTITION_DO.idFromName(t.ref.doName).toString());
		}
		await vi.waitFor(async () => expect((await root(table).debugOutbox()).rows).toEqual([]), WAIT);

		const pending: Promise<unknown>[] = [];
		const client = new FokosShardingClient<CounterPolicy, CounterOps>({
			topology: table.root.topology,
			rangeConfig: table.root.rangeConfig,
			policy: table.root.policy,
			stub: (_ctx, doName) => stub(doName),
			topologyCache: {
				fetch: (path) =>
					fokosTopologyEntrypointFetch(new Request(`http://topology${path}`), {
						admin: (sg) => admin(sg),
						maxAgeS: 10,
						staleWhileRevalidateS: 600,
					}),
				waitUntil: (p) => pending.push(p),
			},
		});
		// The first request of an isolate enters at the root and starts the fetch.
		expect(client.resolve(counterKey("k-1")).doName).toBe(table.root.doName);
		await Promise.all(pending);
		const hashKey = KeyCodec.encode("k-1");
		const result = await client.point("increment", counterKey("k-1"), { hashKey, amount: 1 });
		expect(result.routing.forwardCount).toBe(0);
		expect(result.routing.servedBy[0].hashDepth).toBe(1);
		expect(result.value.val).toBe(2);
	});

	it("a root forwards with the topology to a leaf two levels down that its own forwards never showed", async () => {
		const { table, shardGroup } = await splitTable();
		await withRootTopologyCache(table);
		await splitRoot(table);
		// The writes go directly to the child, so the root never forwards through it and its arena knows depth 1 only.
		const [inside, outside] = keysByChild(0);
		const child = resolveDescendantHashPartitionContext(table.root, [0]);
		for (const key of inside) {
			await table.client.send("increment", child, { hashKey: KeyCodec.encode(key), amount: 1 }, [counterKey(key)]);
		}
		await vi.waitFor(async () => expect((await servedTopology(shardGroup)).bitCount).toBe(1 + 4 + 4), WAIT);
		await refreshRootTopology(table, outside);

		const result = await table.increment(inside[0]);
		expect(result.routing.servedBy[0].hashDepth).toBe(2);
		expect(result.routing.forwardCount).toBe(1);
		expect(result.value.val).toBe(2);
	});

	it("a root that misses at a topology hint cuts the hint and forwards to its arena hint", async () => {
		const { table, shardGroup } = await splitTable();
		await withRootTopologyCache(table);
		await splitRoot(table);
		// A report of a split that did not occur: the children of child 0 do not exist.
		await admin(shardGroup).registerRepartition(hashSplitReport(shardGroup, table.root.topology, 0, [0]));
		await vi.waitFor(async () => expect((await servedTopology(shardGroup)).bitCount).toBe(1 + 4 + 4), WAIT);
		const [inside, outside] = keysByChild(0);
		await refreshRootTopology(table, outside);

		for (const key of inside.slice(0, 2)) {
			const result = await table.increment(key);
			expect(result.routing.servedBy[0].hashDepth).toBe(1);
			expect(result.value.val).toBe(1);
		}
	});

	it("keeps a report whose send throws, and sends it after a restart of the source", async () => {
		const { table, shardGroup } = await splitTable();
		// Every send throws until the test has seen the stored retry.
		await root(table).debugSetOutbox({ failures: 1_000 });
		await splitRoot(table);
		await vi.waitFor(async () => expect((await root(table).debugOutbox()).rows[0]?.attempt).toBeGreaterThanOrEqual(1), WAIT);
		await root(table).debugSetOutbox({ failures: 0 });
		const failedSends = (await root(table).debugOutbox()).sends;
		await root(table)
			.debugAbort()
			.catch(() => {});
		await vi.waitFor(async () => expect((await servedTopology(shardGroup)).bitCount).toBe(5), WAIT);
		const outbox = await root(table).debugOutbox();
		expect(outbox.rows).toEqual([]);
		expect(outbox.sends).toBe(failedSends + 1);
	});

	it("deletes the outbox rows when the host removes the hook", async () => {
		const { table, shardGroup } = await splitTable();
		await root(table).debugSetOutbox({ failures: 1_000 });
		await splitRoot(table);
		await vi.waitFor(async () => expect((await root(table).debugOutbox()).rows).toHaveLength(1), WAIT);
		await root(table).debugSetOutbox({ disabled: true });
		await root(table)
			.debugAbort()
			.catch(() => {});
		await vi.waitFor(async () => expect((await root(table).debugOutbox()).rows).toEqual([]), WAIT);
		expect((await servedTopology(shardGroup)).bitCount).toBe(0);
	});

	it("sends nothing behind the destroy fence", async () => {
		const { table } = await splitTable();
		await root(table).debugSetOutbox({ failures: 1_000 });
		await splitRoot(table);
		await vi.waitFor(async () => expect((await root(table).debugOutbox()).rows).toHaveLength(1), WAIT);
		await root(table).fokosPrepareDestroy({});
		const before = await root(table).debugOutbox();
		// Longer than the first backoff delay of the outbox job (at most 3 s).
		await scheduler.wait(3_500);
		expect(await root(table).debugOutbox()).toEqual(before);
	});
});

function root(table: ReturnType<typeof makeCounterTable>) {
	return stub(table.root.doName);
}

/** Turns on the topology cache of the root, and restarts the root so the flag takes effect. */
async function withRootTopologyCache(table: ReturnType<typeof makeCounterTable>) {
	await root(table).debugSetOutbox({ topologyCache: true });
	await root(table)
		.debugAbort()
		.catch(() => {});
}

/** Six keys under the hash child `c` of the root, and six keys under the other children. */
function keysByChild(c: number): [string[], string[]] {
	const inside: string[] = [];
	const outside: string[] = [];
	for (let i = 0; inside.length < 6 || outside.length < 6; i++) {
		const key = `t-${i}`;
		(hashChildIndex(KeyCodec.encode(key), 0, 4) === c ? inside : outside).push(key);
	}
	return [inside.slice(0, 6), outside.slice(0, 6)];
}

/**
 * Writes keys that are not under the changed child through the root, so the root starts a refresh of
 * its topology, and waits for the refresh. The root has a refresh time of 50 ms.
 */
async function refreshRootTopology(table: ReturnType<typeof makeCounterTable>, outside: string[]) {
	await scheduler.wait(100);
	await table.increment(outside[0]);
	await scheduler.wait(500);
}
