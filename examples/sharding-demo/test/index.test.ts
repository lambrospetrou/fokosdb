import { env, runInDurableObject, SELF } from "cloudflare:test";
import type { CounterPartitionDO } from "../counter-host.js";
import { describe, expect, it } from "vitest";

async function post(tile: string, action: string, body: object = {}): Promise<any> {
	const res = await SELF.fetch(`https://example.com/api/action/${tile}/${action}`, {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: JSON.stringify(body),
	});
	expect(res.status).toBe(200);
	return res.json();
}

async function topology(): Promise<any> {
	const res = await SELF.fetch("https://example.com/api/topology/demo1");
	expect(res.status).toBe(200);
	return res.json();
}

/** The runtime moves splits and imports on by itself, so a test waits for the state it expects. */
async function waitForTopology(done: (t: any) => boolean): Promise<any> {
	let top: any;
	const deadline = Date.now() + 10_000;
	while (Date.now() < deadline) {
		top = await topology();
		if (done(top)) return top;
		await scheduler.wait(50);
	}
	throw new Error(`the topology did not settle: ${JSON.stringify(top.summary)} ${JSON.stringify(top.reconciliation)}`);
}

describe("sharding-demo worker and topology endpoints", () => {
	it("responds to /api/health", async () => {
		const res = await SELF.fetch("https://example.com/api/health");
		expect(res.status).toBe(200);
		expect(await res.json()).toEqual({ status: "ok" });
	});

	it("routes writes through the root after it splits on request volume", async () => {
		await post("demo1", "reset");

		const top1 = await topology();
		expect(top1.roots[0].role).toBe("leaf");
		expect(top1.roots[0].children.length).toBe(0);

		const inc = await post("demo1", "increment", { key: "k-1", amount: 2 });
		expect(inc.result.val).toBe(2);
		expect(inc.trace.forwardCount).toBe(0);

		// Five writes over more than one key cross the threshold of the root.
		await post("demo1", "batch-increment", { count: 5 });
		const top2 = await waitForTopology((t) => t.roots[0].role === "router" && t.reconciliation.reconciled);
		expect(top2.roots[0].children.length).toBe(4);

		const routed = await post("demo1", "increment", { key: "k-1", amount: 1 });
		expect(routed.result.val).toBe(3);
		expect(routed.trace.forwardCount).toBe(1);
		expect(routed.trace.servedBy[0].role).toBe("router");

		const top3 = await waitForTopology((t) => t.reconciliation.reconciled);
		expect(top3.reconciliation.acknowledgedWrites).toBe(8);
	});

	it("reconciles across multi-level splits when children split again", async () => {
		await post("demo1", "reset");
		for (let i = 0; i < 30; i++) await post("demo1", "increment", { key: `user-${i % 8}`, amount: 1 });

		const top = await waitForTopology((t) => t.reconciliation.reconciled && t.summary.totalPartitions > 5);
		expect(top.reconciliation.acknowledgedWrites).toBe(30);
		expect(top.reconciliation.presentWrites).toBe(30);
	});

	it("loses no write when the splitting partition is killed", async () => {
		await post("demo1", "reset");
		await post("demo1", "batch-increment", { count: 5 });

		// The fifth write queued the split, so the root is the source of a split in progress.
		const kill = await post("demo1", "kill");
		expect(kill.success).toBe(true);

		for (let i = 0; i < 10; i++) await post("demo1", "increment", { key: `after-${i}`, amount: 1 });
		const top = await waitForTopology((t) => t.roots[0].role === "router" && t.reconciliation.reconciled);
		expect(top.reconciliation.acknowledgedWrites).toBe(15);

		const idle = await post("demo1", "reset").then(() => post("demo1", "kill"));
		expect(idle.success).toBe(false);
	}, 20_000);

	it("reset deletes the storage and the alarm of every partition", async () => {
		await post("demo1", "reset");
		await post("demo1", "batch-increment", { count: 5 });
		const top = await waitForTopology((t) => t.roots[0].role === "router" && t.reconciliation.reconciled);
		const names: string[] = [];
		const collect = (item: any) => {
			names.push(item.doName);
			item.children.forEach(collect);
		};
		top.roots.forEach(collect);
		expect(names.length).toBe(5);

		await post("demo1", "reset");

		for (const name of names) {
			const stub = env.COUNTER_PARTITION_DO.get(env.COUNTER_PARTITION_DO.idFromName(name));
			await runInDurableObject(stub, async (instance: CounterPartitionDO, state) => {
				expect(instance.fokos.initialized()).toBe(false);
				expect(await state.storage.getAlarm()).toBeNull();
				expect(state.storage.sql.exec("SELECT * FROM counters").toArray()).toEqual([]);
				expect(state.storage.sql.exec("SELECT * FROM counter_meta").toArray()).toEqual([]);
			});
		}
	});

	it("returns topology for Demo 2 and supports promotion", async () => {
		await SELF.fetch("https://example.com/api/action/demo2/reset", { method: "POST" });

		// Add docs
		const addRes = await SELF.fetch("https://example.com/api/action/demo2/add-doc", {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ tenantId: "acme", title: "Doc 1", body: "database sharding" }),
		});
		expect(addRes.status).toBe(200);
		const addJson = (await addRes.json()) as any;
		expect(addJson.success).toBe(true);

		// Promote tenant
		const promoRes = await SELF.fetch("https://example.com/api/action/demo2/promote", {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ tenantId: "acme" }),
		});
		expect(promoRes.status).toBe(200);
		const promoJson = (await promoRes.json()) as any;
		expect(promoJson.promoted).toBe(true);

		// Check topology has promoted tenant range root
		const topRes = await SELF.fetch("https://example.com/api/topology/demo2");
		const top = (await topRes.json()) as any;
		expect(top.roots[0].children.length).toBeGreaterThanOrEqual(1);

		// Search routed to promoted tenant
		const searchRes = await SELF.fetch("https://example.com/api/action/demo2/search", {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ tenantId: "acme", query: "database" }),
		});
		expect(searchRes.status).toBe(200);
		const searchJson = (await searchRes.json()) as any;
		expect(searchJson.hits.length).toBeGreaterThanOrEqual(1);
		expect(searchJson.trace.forwardCount).toBe(1);
	});

	it("returns topology for Demo 3 and performs FokosDB actions", async () => {
		const topRes = await SELF.fetch("https://example.com/api/topology/demo3");
		expect(topRes.status).toBe(200);
		const top = (await topRes.json()) as any;
		expect(top.tile).toBe("demo3");
		expect(top.roots.length).toBe(2); // 2 root trees

		// Put item into FokosDB
		const putRes = await SELF.fetch("https://example.com/api/action/demo3/put-item", {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ hashKey: "user#1", sortKey: "profile", data: "test data" }),
		});
		expect(putRes.status).toBe(200);
		const putJson = (await putRes.json()) as any;
		expect(putJson.success).toBe(true);
		expect(putJson.trace.forwardCount).toBe(0);

		// Get item back
		const getRes = await SELF.fetch("https://example.com/api/action/demo3/get-item", {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ hashKey: "user#1", sortKey: "profile" }),
		});
		expect(getRes.status).toBe(200);
		const getJson = (await getRes.json()) as any;
		expect(getJson.success).toBe(true);
		expect(getJson.found).toBe(true);
		expect(getJson.item.data).toBe("test data");

		// Seed items
		const seedRes = await SELF.fetch("https://example.com/api/action/demo3/seed", {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ count: 4 }),
		});
		expect(seedRes.status).toBe(200);
		const seedJson = (await seedRes.json()) as any;
		expect(seedJson.seeded).toBe(4);
	});
});
