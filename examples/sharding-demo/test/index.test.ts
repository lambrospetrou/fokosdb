import { SELF } from "cloudflare:test";
import { describe, expect, it } from "vitest";

describe("sharding-demo worker and topology endpoints", () => {
	it("responds to /api/health", async () => {
		const res = await SELF.fetch("https://example.com/api/health");
		expect(res.status).toBe(200);
		expect(await res.json()).toEqual({ status: "ok" });
	});

	it("returns topology for Demo 1 and executes routed writes", async () => {
		// Reset first
		await SELF.fetch("https://example.com/api/action/demo1/reset", { method: "POST" });

		// Initial topology should be single root leaf
		const topRes1 = await SELF.fetch("https://example.com/api/topology/demo1");
		expect(topRes1.status).toBe(200);
		const top1 = (await topRes1.json()) as any;
		expect(top1.tile).toBe("demo1");
		expect(top1.roots.length).toBe(1);
		expect(top1.roots[0].role).toBe("leaf");
		expect(top1.roots[0].children.length).toBe(0);

		// Increment on single leaf
		const incRes = await SELF.fetch("https://example.com/api/action/demo1/increment", {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ key: "k-1", amount: 2 }),
		});
		expect(incRes.status).toBe(200);
		const incJson = (await incRes.json()) as any;
		expect(incJson.success).toBe(true);
		expect(incJson.result.val).toBe(2);
		expect(incJson.trace.forwardCount).toBe(0);
		expect(incJson.trace.servedBy.length).toBe(1);

		// Trigger split
		const splitRes = await SELF.fetch("https://example.com/api/action/demo1/split", {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ childCount: 4 }),
		});
		expect(splitRes.status).toBe(200);
		const splitJson = (await splitRes.json()) as any;
		expect(splitJson.success).toBe(true);
		expect(splitJson.children.length).toBe(4);

		// Topology after split
		const topRes2 = await SELF.fetch("https://example.com/api/topology/demo1");
		const top2 = (await topRes2.json()) as any;
		expect(top2.roots[0].role).toBe("router");
		expect(top2.roots[0].children.length).toBe(4);
		expect(top2.summary.routerCount).toBe(1);
		expect(top2.summary.leafCount).toBe(4);

		// Increment on routed topology
		const routedIncRes = await SELF.fetch("https://example.com/api/action/demo1/increment", {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ key: "k-1", amount: 1 }),
		});
		expect(routedIncRes.status).toBe(200);
		const routedIncJson = (await routedIncRes.json()) as any;
		expect(routedIncJson.success).toBe(true);
		expect(routedIncJson.trace.forwardCount).toBe(1);
		expect(routedIncJson.trace.servedBy[0].role).toBe("router");
		expect(routedIncJson.trace.servedBy[1].role).toBe("leaf");

		// Batch increment
		const batchRes = await SELF.fetch("https://example.com/api/action/demo1/batch-increment", {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ count: 5 }),
		});
		expect(batchRes.status).toBe(200);
		const batchJson = (await batchRes.json()) as any;
		expect(batchJson.success).toBe(true);
		expect(batchJson.count).toBe(5);

		// Kill parent action (evicts DO)
		const killRes = await SELF.fetch("https://example.com/api/action/demo1/kill", {
			method: "POST",
		});
		expect(killRes.status).toBe(200);
		const killJson = (await killRes.json()) as any;
		expect(killJson.killed).toBe(true);

		// Reset Demo 1
		const resetRes = await SELF.fetch("https://example.com/api/action/demo1/reset", {
			method: "POST",
		});
		expect(resetRes.status).toBe(200);
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
