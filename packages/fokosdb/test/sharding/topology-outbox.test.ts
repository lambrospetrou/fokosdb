import { runInDurableObject } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it, vi } from "vitest";
import {
	FokosShardingClient,
	KeyCodec,
	fokosTopologyEntrypointFetch,
	topologyAdminName,
	type FokosTopologyRead,
} from "../../src/sharding/index-server.js";
import { parseHashBody } from "../../src/sharding/topology-codec.js";
import type { CounterOps, CounterPolicy } from "./counter-host.js";
import { counterKey, makeCounterTable, stub } from "./counter-table.js";

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
