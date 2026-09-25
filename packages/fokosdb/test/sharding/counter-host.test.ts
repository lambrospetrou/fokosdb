import { describe, expect, it } from "vitest";
import { makeCounterTable, stub } from "./counter-table.js";

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
		expect(nodes[0].stats.children).toHaveLength(4);
		expect(nodes).toHaveLength(5);

		const routed = await table.increment("k-1");
		expect(routed.value).toEqual({ key: "k-1", val: 4 });
		expect(routed.routing.forwardCount).toBe(1);
		expect(routed.routing.servedBy).toHaveLength(1);
		expect(routed.routing.servedBy[0].role).toBe("executed");
		expect(routed.routing.servedBy[0].hashDepth).toBe(1);
		expect(nodes[0].stats.children.map((c) => c.doName)).toContain(routed.routing.servedBy[0].ref.doName);
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
});
