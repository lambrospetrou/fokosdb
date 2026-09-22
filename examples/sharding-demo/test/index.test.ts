import { env, runInDurableObject, SELF } from "cloudflare:test";
import type { CounterPartitionDO } from "../counter-host.js";
import { describe, expect, it } from "vitest";

/** Runs one control-panel action, as a button of the UI does. */
async function post(tile: string, action: string, body: object = {}): Promise<any> {
	const res = await SELF.fetch(`https://example.com/api/${tile}/${action}`, {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: JSON.stringify(body),
	});
	expect(res.status).toBe(200);
	return res.json();
}

async function topology(tile = "demo1"): Promise<any> {
	const res = await SELF.fetch(`https://example.com/api/${tile}/topology`);
	expect(res.status).toBe(200);
	return res.json();
}

/** Every sample document has one of these topic words, so this query matches every document. */
const EVERY_TOPIC = "failover OR invoice OR roadmap OR hiring OR outage OR stampede";

/** The runtime moves splits and imports on by itself, so a test waits until the topology gets to the state it expects. */
async function waitForTopology(tile: string, done: (t: any) => boolean): Promise<any> {
	let top: any;
	const deadline = Date.now() + 10_000;
	while (Date.now() < deadline) {
		top = await topology(tile);
		if (done(top)) return top;
		await scheduler.wait(50);
	}
	throw new Error(`the topology did not settle: ${JSON.stringify(top.summary)} ${JSON.stringify(top.reconciliation)}`);
}

it("responds to /api/health", async () => {
	const res = await SELF.fetch("https://example.com/api/health");
	expect(res.status).toBe(200);
	expect(await res.json()).toEqual({ status: "ok" });
});

describe("Demo 1: counter host", () => {
	it("routes writes through the root after it splits on request volume", async () => {
		await post("demo1", "reset");

		const top1 = await topology();
		expect(top1.roots[0].role).toBe("leaf");
		expect(top1.roots[0].children.length).toBe(0);

		const inc = await post("demo1", "increment", { key: "k-1", amount: 2 });
		expect(inc.result.val).toBe(2);
		expect(inc.trace.forwardCount).toBe(0);

		// Five writes to more than one key get to the split threshold of the root.
		await post("demo1", "batch-increment", { count: 5 });
		const top2 = await waitForTopology("demo1", (t) => t.roots[0].role === "router" && t.reconciliation.reconciled);
		expect(top2.roots[0].children.length).toBe(4);

		const routed = await post("demo1", "increment", { key: "k-1", amount: 1 });
		expect(routed.result.val).toBe(3);
		expect(routed.trace.forwardCount).toBe(1);
		expect(routed.trace.servedBy[0].role).toBe("router");

		const top3 = await waitForTopology("demo1", (t) => t.reconciliation.reconciled);
		expect(top3.reconciliation.acknowledgedWrites).toBe(8);
	});

	it("reconciles across multi-level splits when children split again", async () => {
		await post("demo1", "reset");
		for (let i = 0; i < 30; i++) await post("demo1", "increment", { key: `user-${i % 8}`, amount: 1 });

		const top = await waitForTopology("demo1", (t) => t.reconciliation.reconciled && t.summary.totalPartitions > 5);
		expect(top.reconciliation.acknowledgedWrites).toBe(30);
		expect(top.reconciliation.presentWrites).toBe(30);
	});

	it("loses no write when the splitting partition is killed", async () => {
		await post("demo1", "reset");
		await post("demo1", "batch-increment", { count: 5 });

		// The fifth write queued the split, so the root is now the source of a split.
		const kill = await post("demo1", "kill");
		expect(kill.success).toBe(true);

		for (let i = 0; i < 10; i++) await post("demo1", "increment", { key: `after-${i}`, amount: 1 });
		const top = await waitForTopology("demo1", (t) => t.roots[0].role === "router" && t.reconciliation.reconciled);
		expect(top.reconciliation.acknowledgedWrites).toBe(15);

		const idle = await post("demo1", "reset").then(() => post("demo1", "kill"));
		expect(idle.success).toBe(false);
	}, 20_000);

	it("reset deletes the storage and the alarm of every partition", async () => {
		await post("demo1", "reset");
		await post("demo1", "batch-increment", { count: 5 });
		const top = await waitForTopology("demo1", (t) => t.roots[0].role === "router" && t.reconciliation.reconciled);
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
});

describe("Demo 2: search host", () => {
	it("promotes a large tenant out of the shared partition, and its search follows", async () => {
		await post("demo2", "reset");
		const empty = await topology("demo2");
		expect(empty.roots[0].children).toEqual([]);
		expect(empty.roots[0].itemCount).toBe(0);

		await post("demo2", "add-docs", { tenant: "beta", count: 1 });
		await post("demo2", "add-docs", { tenant: "acme", count: 5 });

		// The shared partition keeps beta, and acme's five documents move to a range root of its own.
		const top = await waitForTopology("demo2", (t) => {
			const [rangeRoot] = t.roots[0].children;
			return t.roots[0].itemCount === 1 && rangeRoot?.kind === "range" && rangeRoot.itemCount === 5 && rangeRoot.importState === "active";
		});
		expect(top.roots[0].label).toBe("beta:1");
		const rangeRoot = top.roots[0].children[0].doName;

		const acme = await post("demo2", "search", { tenant: "acme", query: EVERY_TOPIC });
		expect(acme.result.hits.length).toBe(5);
		expect(acme.result.visited).toEqual([rangeRoot]);
		expect(acme.result.skipped).toEqual([]);
		expect(acme.trace.forwardCount).toBe(1);

		const beta = await post("demo2", "search", { tenant: "beta", query: EVERY_TOPIC });
		expect(beta.result.hits.length).toBe(1);
		expect(beta.trace.forwardCount).toBe(0);
	});

	it("searches only the date partitions that a window or a limit needs", async () => {
		await post("demo2", "reset");
		await post("demo2", "add-docs", { tenant: "acme", count: 5 });
		await waitForTopology("demo2", (t) => t.roots[0].children[0]?.importState === "active");

		// Ten documents are more than the range split threshold of 8, so the range root of acme splits by date.
		await post("demo2", "add-docs", { tenant: "acme", count: 5 });
		const top = await waitForTopology("demo2", (t) => {
			const rangeRoot = t.roots[0].children[0];
			const leaves = rangeRoot?.children ?? [];
			return (
				rangeRoot?.role === "router" &&
				leaves.length === 2 &&
				leaves.every((l: any) => l.importState === "active") &&
				rangeRoot.itemCount === 0
			);
		});
		const leaves = top.roots[0].children[0].children.map((l: any) => l.doName);

		// A search without a window or a limit visits every partition. The hits come newest first.
		const all = await post("demo2", "search", { tenant: "acme", query: EVERY_TOPIC });
		const keys = all.result.hits.map((h: any) => h.sortKey);
		expect(keys).toEqual([...keys].sort().reverse());
		expect(keys.length).toBe(10);
		expect(all.result.visited.sort()).toEqual([...leaves].sort());
		expect(all.result.stoppedEarly).toBe(false);

		// The split boundary is the lowest sort key of the newer partition, so a window that starts there
		// overlaps only that partition.
		const newer = all.result.hits[0].partition;
		const older = leaves.find((l: string) => l !== newer);
		const newerHits = all.result.hits.filter((h: any) => h.partition === newer);
		const boundary = newerHits.at(-1).sortKey;
		const windowed = await post("demo2", "search", { tenant: "acme", query: EVERY_TOPIC, from: boundary });
		expect(windowed.result.hits).toEqual(newerHits);
		expect(windowed.result.visited).toEqual([newer]);
		expect(windowed.result.skipped).toEqual([older]);

		// Each partition holds 4 or more documents, so the newest 3 come from the newer partition, and the search stops there.
		const newest = await post("demo2", "search", { tenant: "acme", query: EVERY_TOPIC, limit: 3 });
		expect(newest.result.hits).toEqual(all.result.hits.slice(0, 3));
		expect(newest.result.stoppedEarly).toBe(true);
		expect(newest.result.visited).toEqual([newer]);
		expect(newest.result.skipped).toEqual([older]);
	}, 30_000);

	it("answers an invalid FTS5 query with a clean error", async () => {
		await post("demo2", "reset");
		await post("demo2", "add-docs", { tenant: "beta", count: 1 });
		const res = await post("demo2", "search", { tenant: "beta", query: "failover AND" });
		expect(res.success).toBe(false);
		expect(res.error).toMatch(/syntax error/);
	});
});

describe("Demo 3: FokosDB table", () => {
	it("draws the root partitions and writes and reads an item", async () => {
		const top = await topology("demo3");
		expect(top.roots.length).toBe(2);

		const put = await post("demo3", "put-item", { hashKey: "user#1", sortKey: "profile", data: "test data" });
		expect(put.trace.forwardCount).toBe(0);

		const get = await post("demo3", "get-item", { hashKey: "user#1", sortKey: "profile" });
		expect(get.result.found).toBe(true);
		expect(get.result.item.data).toBe("test data");

		const seed = await post("demo3", "seed", { count: 4 });
		expect(seed.result.seeded).toBe(4);
	});
});
