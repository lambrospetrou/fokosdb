import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { FokosError, FokosShardingStore, HashTopology, KeyCodec, SHARDING_UNAVAILABLE_CODES } from "../../src/sharding/index-server.js";
import { counterKey, makeCounterTable, stub } from "./counter-table.js";

describe.concurrent("Sharding runtime — counter host", () => {
	it("splits the root on request volume and routes the next write through it", async () => {
		const table = makeCounterTable();
		const first = await table.increment("k-1", 2);
		expect(first.value).toEqual({ key: "k-1", val: 2 });
		expect(first.routing.forwardCount).toBe(0);

		// Five writes to more than one key get to the split threshold of the root.
		for (let i = 0; i < 4; i++) {
			await table.increment(`k-${i}`);
		}
		const nodes = await table.settle((n) => n[0].stats.role === "router");
		expect(table.childrenOf(nodes, nodes[0])).toHaveLength(4);
		expect(nodes).toHaveLength(5);

		const routed = await table.increment("k-1");
		expect(routed.value).toEqual({ key: "k-1", val: 4 });
		expect(routed.routing.forwardCount).toBe(1);
		expect(routed.routing.servedBy).toHaveLength(1);
		expect(routed.routing.servedBy[0].role).toBe("executed");
		expect(routed.routing.servedBy[0].hashDepth).toBe(1);
		expect(table.childrenOf(nodes, nodes[0]).map((c) => c.ref.doName)).toContain(routed.routing.servedBy[0].ref.doName);
		await table.settle();
	});

	it("keeps every write when the children split again", async () => {
		const table = makeCounterTable();
		for (let i = 0; i < 30; i++) {
			await table.increment(`user-${i % 8}`);
		}
		const nodes = await table.settle((n) => n.length > 5);
		expect(nodes.some((n) => n.ref.doName !== table.root.doName && n.stats.role === "router")).toBe(true);
	});

	it("keeps every write when the source of a split is killed", async () => {
		const table = makeCounterTable();
		for (let i = 0; i < 5; i++) {
			await table.increment(`k-${i}`);
		}

		// The fifth write queued the split, so the root is now the source of a split.
		const victim = (await table.tree()).find((n) => n.stats.repartitionState !== null);
		expect(victim).toBeDefined();
		// `ctx.abort()` ends the call with an error.
		await stub(victim!.ref.doName)
			.debugAbort()
			.catch(() => {});

		for (let i = 0; i < 10; i++) {
			await table.increment(`after-${i}`);
		}
		await table.settle((n) => n[0].stats.role === "router");
	});

	it("does not split a partition that holds one hot key", async () => {
		const table = makeCounterTable();
		for (let i = 0; i < 10; i++) {
			await table.increment("hot");
		}
		const [root, ...rest] = await table.settle();
		expect(rest).toEqual([]);
		expect(root.stats.role).toBe("owner");
		expect(root.stats.requestCount).toBe(10);
	});

	it("walks a table that no request used, and creates no identity", async () => {
		const table = makeCounterTable();
		const nodes = [];
		for await (const node of table.client.walk()) {
			nodes.push(node);
		}

		expect(nodes).toEqual([]);
		expect(await stub(table.root.doName).fokosStatus({ cursor: null })).toMatchObject({ initialized: false, role: null });
	});

	it("walks only the owners of a split root with scope owners", async () => {
		const table = makeCounterTable();
		for (let i = 0; i < 5; i++) {
			await table.increment(`k-${i}`);
		}
		const nodes = await table.settle((n) => n[0].stats.role === "router");

		const owners = [];
		for await (const node of table.client.walk({ scope: "owners" })) {
			owners.push(node);
		}

		expect(owners.map((n) => n.ctx.doName).sort()).toEqual(
			table
				.childrenOf(nodes, nodes[0])
				.map((n) => n.ref.doName)
				.sort(),
		);
		expect(owners.every((n) => n.role === "owner" && n.kind === "hash" && n.parent?.via === "hash_split")).toBe(true);
	});

	it("retries by a policy that reads the code of an error that crossed an RPC hop", async () => {
		const table = makeCounterTable();
		await table.increment("k");
		// The destroy fence refuses every operation with `partition_migrating`.
		await stub(table.root.doName).fokosPrepareDestroy({});
		const seen: boolean[] = [];

		const err = await table.client
			.point(
				"increment",
				counterKey("k"),
				{ hashKey: KeyCodec.encode("k"), amount: 1 },
				{
					retry: {
						shouldRetry: (e, nextAttempt) => {
							seen.push(FokosError.isCode(e, SHARDING_UNAVAILABLE_CODES.partition_migrating));
							return nextAttempt <= 2;
						},
						baseDelayMs: 1,
						maxDelayMs: 2,
					},
				},
			)
			.catch((e: unknown) => e);

		expect(seen).toEqual([true, true]);
		expect(err).toMatchObject({ code: "partition_migrating", clientRpcs: 2 });
	});

	it("counts a forward to a hinted child without an identity, after the fallback succeeds", async () => {
		const table = makeCounterTable();
		for (let i = 0; i < 5; i++) {
			await table.increment(`k-${i}`);
		}
		await table.settle((n) => n[0].stats.role === "router");

		// The root learns a hint to a grandchild of `k-1` that no split created.
		const key = KeyCodec.encode("k-1");
		await runInDurableObject(stub(table.root.doName), (_instance, state) => {
			const arena = HashTopology.create(4, 0);
			arena.updateFromHint(key, 2);
			new FokosShardingStore(state.storage).putHashArena(arena.toSnapshot());
		});
		// The next instance reads the hint from storage.
		await stub(table.root.doName)
			.debugAbort()
			.catch(() => {});

		const routed = await table.increment("k-1");

		// One forward to the grandchild, which refused without routing, and one to the child.
		expect(routed.routing.forwardCount).toBe(2);
		expect(routed.routing.servedBy.map((n) => n.hashDepth)).toEqual([1]);
		await table.settle();
	});
});
