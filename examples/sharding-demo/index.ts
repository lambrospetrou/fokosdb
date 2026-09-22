import { DurableObject } from "cloudflare:workers";
import { Hono } from "hono";
import { FokosDB, PartitionContextCreator, FokosRouter } from "fokosdb/client";
import { KeyCodec } from "fokosdb/sharding";
import type { CounterPolicy, CounterStats } from "./counter-host.js";

export { PartitionDO } from "fokosdb/server";
export { CounterPartitionDO } from "./counter-host.js";

export type RouteHop = {
	doName: string;
	role: string;
	partitionId: string;
};

export type ActionTrace = {
	servedBy: RouteHop[];
	forwardCount: number;
};

export type TopologyItem = {
	id: string;
	doName: string;
	role: "root" | "router" | "leaf";
	kind: "hash" | "range";
	depth: number;
	status: string;
	importState: string | null;
	hashKey: string | null;
	itemCount: number;
	requestCount: number;
	children: TopologyItem[];
};

export type TileTopology = {
	tile: string;
	roots: TopologyItem[];
	summary: {
		totalPartitions: number;
		routerCount: number;
		leafCount: number;
		splitCount: number;
	};
	reconciliation?: {
		acknowledgedWrites: number;
		presentWrites: number;
		presentKeys: number;
		reconciled: boolean;
	};
};

// ── Demo 1: Counter host ─────────────────────────────────────────────────────

const counterRouter = new FokosRouter<CounterPolicy>(
	{ shardGroup: "counter_demo", rootTreesN: 1, hashSplitN: 4 },
	{ rangeSplitN: 4, rangeAncestors: { fromRoot: 0, fromLeaf: 3 } },
	{ maxRequests: 5 },
);
const counterRoot = counterRouter.allRoots()[0];

function counterStub(env: Env, doName: string) {
	return env.COUNTER_PARTITION_DO.get(env.COUNTER_PARTITION_DO.idFromName(doName));
}

type CounterNode = {
	ref: { doName: string; partitionId: string };
	stats: CounterStats;
	depth: number;
};

/** Every partition of the counter tree, each parent before its children. */
async function collectCounterTree(env: Env): Promise<CounterNode[]> {
	const nodes: CounterNode[] = [];
	const walk = async (ref: CounterNode["ref"], depth: number): Promise<void> => {
		const stats = await counterStub(env, ref.doName).getCounterStats();
		nodes.push({ ref, stats, depth });
		for (const child of stats.children) await walk(child, depth + 1);
	};
	await walk(counterRoot, 0);
	return nodes;
}

/**
 * Compares the writes the Worker saw succeed with the sum of the rows in the tree. A router keeps a
 * stale copy of its rows until every child acknowledged its import, and a child comes after its
 * parent in the list, so the last value seen for a key is the current one.
 */
async function reconcileCounters(env: Env, nodes: CounterNode[]) {
	const acknowledgedWrites = await counterStub(env, counterRoot.doName).getAcknowledged();
	const values = new Map<string, number>();
	for (const node of nodes) for (const r of node.stats.rows) values.set(r.key, r.val);
	const presentWrites = [...values.values()].reduce((a, b) => a + b, 0);
	return { acknowledgedWrites, presentWrites, presentKeys: values.size, reconciled: acknowledgedWrites === presentWrites };
}

/**
 * One counter write through the root. A partition that still imports refuses the write with
 * `partition_migrating`; the runtime moves the import on by itself, so the write waits and tries again.
 * After a kill, the runtime's fallback alarm restarts the partition within 5 seconds, so the write
 * tries for 15 seconds.
 */
async function sendCounterIncrement(
	env: Env,
	key: string,
	amount: number,
): Promise<{ value: { key: string; val: number }; trace: ActionTrace }> {
	const hashKey = KeyCodec.encode(key);
	const ctx = counterRouter.rootContext(hashKey);
	for (let attempt = 1; ; attempt++) {
		try {
			const envelope = await counterStub(env, ctx.doName).increment(ctx, { hashKey, amount });
			const { value, routing } = counterRouter.unwrap<{ key: string; val: number }>(envelope);
			await counterStub(env, counterRoot.doName).recordAcknowledged(amount);
			// A router does not list itself in `servedBy`, so the entry hop is added here.
			const executors = routing.servedBy.map((s) => ({ doName: s.ref.doName, partitionId: s.ref.partitionId, role: s.role }));
			const entry = { doName: ctx.doName, partitionId: ctx.partitionId, role: "router" };
			return {
				value,
				trace: { servedBy: routing.forwardCount > 0 ? [entry, ...executors] : executors, forwardCount: routing.forwardCount },
			};
		} catch (err) {
			if (attempt >= 150 || !String(err).includes("partition_migrating")) throw err;
			await scheduler.wait(100);
		}
	}
}

// ── Demo 2: Search Partition DO ──────────────────────────────────────────────

type PromotedTenant = {
	tenantId: string;
	partitionId: string;
	doName: string;
	subRanges: string[];
};

export class SearchPartitionDO extends DurableObject<Env> {
	private docCount = 0;
	private tenantCounts: Map<string, number> = new Map();
	private promotedTenants: Map<string, PromotedTenant> = new Map();

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.ctx.blockConcurrencyWhile(async () => {
			this.ctx.storage.sql.exec(
				"CREATE TABLE IF NOT EXISTS docs (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL)",
			);
			this.ctx.storage.sql.exec(
				"CREATE TABLE IF NOT EXISTS promotions (tenant_id TEXT PRIMARY KEY, partition_id TEXT NOT NULL, do_name TEXT NOT NULL, sub_ranges TEXT NOT NULL)",
			);

			const docRows = this.ctx.storage.sql.exec<{ tenant_id: string }>("SELECT tenant_id FROM docs").toArray();
			for (const r of docRows) {
				this.docCount += 1;
				this.tenantCounts.set(r.tenant_id, (this.tenantCounts.get(r.tenant_id) ?? 0) + 1);
			}

			const promoRows = this.ctx.storage.sql
				.exec<{
					tenant_id: string;
					partition_id: string;
					do_name: string;
					sub_ranges: string;
				}>("SELECT tenant_id, partition_id, do_name, sub_ranges FROM promotions")
				.toArray();
			for (const r of promoRows) {
				this.promotedTenants.set(r.tenant_id, {
					tenantId: r.tenant_id,
					partitionId: r.partition_id,
					doName: r.do_name,
					subRanges: JSON.parse(r.sub_ranges),
				});
			}
		});
	}

	async fokosStatus() {
		const entries = Array.from(this.promotedTenants.values()).map((p, idx) => ({
			repartition: {
				id: `promo-${p.tenantId}`,
				seq: idx + 1,
				kind: "key_promotion" as const,
				state: "completed" as const,
				hashKey: new TextEncoder().encode(p.tenantId),
			},
			target: {
				index: idx,
				ref: { partitionId: p.partitionId, doName: p.doName },
				initialization: { kind: "range_root" as const },
				acknowledged: true,
			},
			subRanges: p.subRanges,
		}));

		return {
			initialized: true,
			destroying: false,
			ref: { partitionId: "search-root", doName: "search-root" },
			importState: null,
			entries,
			nextCursor: null,
			docCount: this.docCount,
			tenants: Array.from(this.tenantCounts.entries()).map(([k, v]) => ({ tenantId: k, count: v })),
		};
	}

	async addDoc(tenantId: string, title: string, body: string, callingHop?: RouteHop): Promise<{ id: string; trace: ActionTrace }> {
		const promo = this.promotedTenants.get(tenantId);
		if (promo && !callingHop) {
			const childStub = this.env.SEARCH_PARTITION_DO.get(this.env.SEARCH_PARTITION_DO.idFromName(promo.doName));
			const routerHop: RouteHop = {
				doName: "search-root",
				role: "router",
				partitionId: "search-root",
			};
			const res = await childStub.addDoc(tenantId, title, body, routerHop);
			return {
				id: res.id,
				trace: {
					servedBy: [routerHop, ...res.trace.servedBy],
					forwardCount: res.trace.forwardCount + 1,
				},
			};
		}

		const id = `doc-${crypto.randomUUID()}`;
		this.ctx.storage.sql.exec("INSERT INTO docs (id, tenant_id, title, body) VALUES (?, ?, ?, ?)", id, tenantId, title, body);
		this.docCount += 1;
		this.tenantCounts.set(tenantId, (this.tenantCounts.get(tenantId) ?? 0) + 1);

		const selfHop: RouteHop = {
			doName: callingHop ? (promo?.doName ?? "search-promoted") : "search-root",
			role: "leaf",
			partitionId: callingHop ? (promo?.partitionId ?? "search-promoted") : "search-root",
		};
		return {
			id,
			trace: {
				servedBy: [selfHop],
				forwardCount: 0,
			},
		};
	}

	async search(
		tenantId: string,
		query: string,
		callingHop?: RouteHop,
	): Promise<{ hits: Array<{ id: string; title: string; body: string }>; trace: ActionTrace }> {
		const promo = this.promotedTenants.get(tenantId);
		if (promo && !callingHop) {
			const childStub = this.env.SEARCH_PARTITION_DO.get(this.env.SEARCH_PARTITION_DO.idFromName(promo.doName));
			const routerHop: RouteHop = {
				doName: "search-root",
				role: "router",
				partitionId: "search-root",
			};
			const res = await childStub.search(tenantId, query, routerHop);
			return {
				hits: res.hits,
				trace: {
					servedBy: [routerHop, ...res.trace.servedBy],
					forwardCount: res.trace.forwardCount + 1,
				},
			};
		}

		const hits = this.ctx.storage.sql
			.exec<{
				id: string;
				title: string;
				body: string;
			}>(
				"SELECT id, title, body FROM docs WHERE tenant_id = ? AND (title LIKE ? OR body LIKE ?) LIMIT 10",
				tenantId,
				`%${query}%`,
				`%${query}%`,
			)
			.toArray();

		const selfHop: RouteHop = {
			doName: callingHop ? (promo?.doName ?? "search-promoted") : "search-root",
			role: "leaf",
			partitionId: callingHop ? (promo?.partitionId ?? "search-promoted") : "search-root",
		};
		return {
			hits,
			trace: {
				servedBy: [selfHop],
				forwardCount: 0,
			},
		};
	}

	async promoteTenant(tenantId: string, subRanges: string[] = ["2025", "2026"]): Promise<{ promoted: boolean; tenant: PromotedTenant }> {
		const existing = this.promotedTenants.get(tenantId);
		if (existing) return { promoted: false, tenant: existing };

		const promo: PromotedTenant = {
			tenantId,
			partitionId: `search-promo-${tenantId}`,
			doName: `search-promo-${tenantId}`,
			subRanges,
		};
		this.promotedTenants.set(tenantId, promo);
		this.ctx.storage.sql.exec(
			"INSERT OR REPLACE INTO promotions (tenant_id, partition_id, do_name, sub_ranges) VALUES (?, ?, ?, ?)",
			promo.tenantId,
			promo.partitionId,
			promo.doName,
			JSON.stringify(promo.subRanges),
		);

		// Migrate tenant docs to promoted partition
		const tenantDocs = this.ctx.storage.sql
			.exec<{ id: string; title: string; body: string }>("SELECT id, title, body FROM docs WHERE tenant_id = ?", tenantId)
			.toArray();
		if (tenantDocs.length > 0) {
			const childStub = this.env.SEARCH_PARTITION_DO.get(this.env.SEARCH_PARTITION_DO.idFromName(promo.doName));
			for (const d of tenantDocs) {
				await childStub.addDoc(tenantId, d.title, d.body);
			}
			this.ctx.storage.sql.exec("DELETE FROM docs WHERE tenant_id = ?", tenantId);
		}

		return { promoted: true, tenant: promo };
	}

	async reset(): Promise<{ reset: boolean }> {
		this.docCount = 0;
		this.tenantCounts.clear();
		this.promotedTenants.clear();
		this.ctx.storage.sql.exec("DELETE FROM docs");
		this.ctx.storage.sql.exec("DELETE FROM promotions");
		return { reset: true };
	}

	async getStats() {
		return {
			docCount: this.docCount,
			tenantCount: this.tenantCounts.size,
			promotedCount: this.promotedTenants.size,
		};
	}
}

// ── Application API ──────────────────────────────────────────────────────────

const app = new Hono<{ Bindings: Env }>();

app.get("/api/health", (c) => c.json({ status: "ok" }));

// ── Topology endpoint ────────────────────────────────────────────────────────

app.get("/api/topology/:tile", async (c) => {
	const tile = c.req.param("tile");

	if (tile === "demo1") {
		const nodes = await collectCounterTree(c.env);
		const toItem = (node: CounterNode): TopologyItem => ({
			id: node.ref.partitionId,
			doName: node.ref.doName,
			role: node.stats.role === "router" ? "router" : "leaf",
			kind: "hash",
			depth: node.depth,
			status: node.stats.repartitionState ?? "active",
			importState: node.stats.importState,
			hashKey: null,
			itemCount: node.stats.rows.length,
			requestCount: node.stats.requestCount,
			children: nodes.filter((n) => node.stats.children.some((ch) => ch.doName === n.ref.doName)).map(toItem),
		});
		const routerCount = nodes.filter((n) => n.stats.role === "router").length;
		const response: TileTopology = {
			tile: "demo1",
			roots: [toItem(nodes[0])],
			summary: {
				totalPartitions: nodes.length,
				routerCount,
				leafCount: nodes.length - routerCount,
				splitCount: routerCount,
			},
			reconciliation: await reconcileCounters(c.env, nodes),
		};
		return c.json(response);
	}

	if (tile === "demo2") {
		const stub = c.env.SEARCH_PARTITION_DO.get(c.env.SEARCH_PARTITION_DO.idFromName("search-root"));
		const status = await stub.fokosStatus();
		const stats = await stub.getStats();

		const rootNode: TopologyItem = {
			id: "search-root",
			doName: "search-root",
			role: status.entries.length > 0 ? "router" : "leaf",
			kind: "hash",
			depth: 0,
			status: "active",
			importState: status.importState,
			hashKey: null,
			itemCount: stats.docCount,
			requestCount: stats.docCount,
			children: [],
		};

		for (const entry of status.entries) {
			if (entry.target) {
				const promoNode: TopologyItem = {
					id: entry.target.ref.partitionId,
					doName: entry.target.ref.doName,
					role: entry.subRanges && entry.subRanges.length > 0 ? "router" : "leaf",
					kind: "range",
					depth: 1,
					status: "active",
					importState: null,
					hashKey: entry.repartition.hashKey ? new TextDecoder().decode(entry.repartition.hashKey) : null,
					itemCount: Math.round(stats.docCount / (status.entries.length + 1)),
					requestCount: Math.round(stats.docCount / (status.entries.length + 1)),
					children: [],
				};

				if (entry.subRanges) {
					for (const r of entry.subRanges) {
						promoNode.children.push({
							id: `${entry.target.ref.partitionId}-${r}`,
							doName: `${entry.target.ref.doName}-${r}`,
							role: "leaf",
							kind: "range",
							depth: 2,
							status: "active",
							importState: null,
							hashKey: r,
							itemCount: Math.round(stats.docCount / (status.entries.length * 2)),
							requestCount: Math.round(stats.docCount / (status.entries.length * 2)),
							children: [],
						});
					}
				}

				rootNode.children.push(promoNode);
			}
		}

		let totalPartitions = 1;
		let routerCount = rootNode.role === "router" ? 1 : 0;
		let leafCount = rootNode.role === "leaf" ? 1 : 0;

		for (const ch of rootNode.children) {
			totalPartitions += 1;
			if (ch.role === "router") routerCount += 1;
			else leafCount += 1;
			for (const g of ch.children) {
				totalPartitions += 1;
				if (g.role === "router") routerCount += 1;
				else leafCount += 1;
			}
		}

		const response: TileTopology = {
			tile: "demo2",
			roots: [rootNode],
			summary: {
				totalPartitions,
				routerCount,
				leafCount,
				splitCount: status.entries.length,
			},
		};
		return c.json(response);
	}

	if (tile === "demo3") {
		// Real PartitionDO topology
		const tableConfig = PartitionContextCreator.create({
			ns: "PARTITION_DO" as any,
			nsTx: "PARTITION_DO" as any,
			tableName: "demo3_table",
			rootTreesN: 2,
			hashSplitN: 2,
			rangeSplitN: 2,
			hashSplitConditions: { maxSizeMb: 100 },
			rangeSplitConditions: { maxSizeMb: 100 },
		});
		const router = new FokosRouter(tableConfig.topology, tableConfig.rangeConfig, tableConfig.policy);
		const rootContexts = router.allRoots();
		const roots: TopologyItem[] = [];

		for (let i = 0; i < rootContexts.length; i++) {
			const ctx = rootContexts[i];
			const stub = c.env.PARTITION_DO.get(c.env.PARTITION_DO.idFromName(ctx.doName));
			let status;
			try {
				status = await stub.fokosStatus({ cursor: null, rootContext: ctx });
			} catch {
				status = {
					initialized: false,
					destroying: false,
					ref: { partitionId: ctx.partitionId, doName: ctx.doName },
					importState: null,
					entries: [],
					nextCursor: null,
				};
			}

			const isRouter = status.entries.length > 0;
			const rootNode: TopologyItem = {
				id: ctx.partitionId,
				doName: ctx.doName,
				role: isRouter ? "router" : "leaf",
				kind: "hash",
				depth: 0,
				status: isRouter ? "split_started" : "active",
				importState: status.importState,
				hashKey: null,
				itemCount: 0,
				requestCount: 0,
				children: [],
			};

			for (const entry of status.entries) {
				if (entry.target) {
					rootNode.children.push({
						id: entry.target.ref.partitionId,
						doName: entry.target.ref.doName,
						role: "leaf",
						kind: entry.repartition.kind === "key_promotion" ? "range" : "hash",
						depth: 1,
						status: entry.repartition.state,
						importState: null,
						hashKey: entry.repartition.hashKey ? new TextDecoder().decode(entry.repartition.hashKey) : null,
						itemCount: 0,
						requestCount: 0,
						children: [],
					});
				}
			}
			roots.push(rootNode);
		}

		let totalPartitions = roots.length;
		let routerCount = 0;
		let leafCount = 0;
		for (const r of roots) {
			if (r.role === "router") routerCount += 1;
			else leafCount += 1;
			for (const ch of r.children) {
				totalPartitions += 1;
				if (ch.role === "router") routerCount += 1;
				else leafCount += 1;
			}
		}

		const response: TileTopology = {
			tile: "demo3",
			roots,
			summary: {
				totalPartitions,
				routerCount,
				leafCount,
				splitCount: routerCount,
			},
		};
		return c.json(response);
	}

	return c.json({ error: `Unknown tile: ${tile}` }, 404);
});

// ── Synthetic load and action endpoints ──────────────────────────────────────

app.post("/api/action/:tile/:action", async (c) => {
	const tile = c.req.param("tile");
	const action = c.req.param("action");
	let body: Record<string, unknown> = {};
	try {
		body = await c.req.json();
	} catch {
		// Empty body is acceptable for no-param actions
	}

	if (tile === "demo1") {
		if (action === "increment") {
			const key = (body.key as string) ?? `counter-${Math.floor(Math.random() * 8)}`;
			const { value, trace } = await sendCounterIncrement(c.env, key, (body.amount as number) ?? 1);
			return c.json({ success: true, action, result: value, trace });
		}

		if (action === "batch-increment") {
			const count = (body.count as number) ?? 5;
			let trace: ActionTrace | undefined;
			for (let i = 0; i < count; i++) ({ trace } = await sendCounterIncrement(c.env, `counter-${i % 8}`, 1));
			return c.json({ success: true, action, result: { count }, trace });
		}

		if (action === "kill") {
			// The source of a split holds the pages its children still pull, so it is the partition to crash.
			const nodes = await collectCounterTree(c.env);
			const victim = nodes.find((n) => n.stats.repartitionState !== null);
			if (!victim) return c.json({ success: false, action, error: "no partition is splitting now" });
			// The abort ends the call with an error, so the error means success.
			await counterStub(c.env, victim.ref.doName)
				.debugAbort()
				.catch(() => {});
			return c.json({ success: true, action, result: { killed: victim.ref.doName } });
		}

		if (action === "reset") {
			// Read the tree first, because a reset parent forgets its children.
			const nodes = await collectCounterTree(c.env);
			// Reset each parent before its children: a split source that is still alive can initialize a
			// child again after that child was reset. Each reset ends its call with an error, so the error
			// means success.
			for (const node of nodes)
				await counterStub(c.env, node.ref.doName)
					.resetAll()
					.catch(() => {});
			return c.json({ success: true, action });
		}
	}

	if (tile === "demo2") {
		const stub = c.env.SEARCH_PARTITION_DO.get(c.env.SEARCH_PARTITION_DO.idFromName("search-root"));

		if (action === "add-doc") {
			const tenantId = (body.tenantId as string) ?? "tenant-acme";
			const title = (body.title as string) ?? `Document ${Date.now()}`;
			const text = (body.body as string) ?? "Searchable full text content in SQLite virtual table";
			const res = await stub.addDoc(tenantId, title, text);
			const stats = await stub.getStats();
			return c.json({ success: true, action, result: res, trace: res.trace, stats });
		}

		if (action === "promote") {
			const tenantId = (body.tenantId as string) ?? "tenant-acme";
			const res = await stub.promoteTenant(tenantId);
			const stats = await stub.getStats();
			return c.json({ success: true, action, ...res, stats });
		}

		if (action === "search") {
			const tenantId = (body.tenantId as string) ?? "tenant-acme";
			const query = (body.query as string) ?? "content";
			const res = await stub.search(tenantId, query);
			const stats = await stub.getStats();
			return c.json({ success: true, action, hits: res.hits, trace: res.trace, stats });
		}

		if (action === "reset") {
			const res = await stub.reset();
			return c.json({ success: true, action, ...res });
		}
	}

	if (tile === "demo3") {
		const tableConfig = PartitionContextCreator.create({
			ns: "PARTITION_DO" as any,
			nsTx: "PARTITION_DO" as any,
			tableName: "demo3_table",
			rootTreesN: 2,
			hashSplitN: 2,
			rangeSplitN: 2,
			hashSplitConditions: { maxSizeMb: 100 },
			rangeSplitConditions: { maxSizeMb: 100 },
		});
		const router = new FokosRouter(tableConfig.topology, tableConfig.rangeConfig, tableConfig.policy);
		const db = new FokosDB({
			topology: router,
			numTxCoordinators: 1,
		});

		if (action === "put-item") {
			const hashKey = (body.hashKey as string) ?? `user#${Math.floor(Math.random() * 50)}`;
			const sortKey = (body.sortKey as string) ?? "meta";
			const data = (body.data as string) ?? "sample item data";
			const res = await db.putItem({ hashKey, sortKey, data });
			const trace: ActionTrace = {
				servedBy: [
					{
						doName: res.meta.servedByActorName,
						partitionId: res.meta.servedByPartitionId,
						role: "leaf",
					},
				],
				forwardCount: res.meta.forwardCount,
			};
			return c.json({ success: true, action, meta: res.meta, trace });
		}

		if (action === "get-item") {
			const hashKey = (body.hashKey as string) ?? `user#1`;
			const sortKey = (body.sortKey as string) ?? "meta";
			const res = await db.getItem({ hashKey, sortKey });
			const trace: ActionTrace = {
				servedBy: [
					{
						doName: res.meta.servedByActorName,
						partitionId: res.meta.servedByPartitionId,
						role: "leaf",
					},
				],
				forwardCount: res.meta.forwardCount,
			};
			return c.json({ success: true, action, found: res.found, item: res.item, trace });
		}

		if (action === "seed") {
			const count = (body.count as number) ?? 5;
			const results = [];
			for (let i = 0; i < count; i++) {
				const res = await db.putItem({
					hashKey: `seed#${i}`,
					sortKey: "entry",
					data: `Seed record ${i}`,
				});
				results.push({
					hashKey: `seed#${i}`,
					servedBy: res.meta.servedByActorName,
				});
			}
			return c.json({ success: true, action, seeded: results.length, items: results });
		}

		if (action === "reset") {
			await db.destroy();
			return c.json({ success: true, action, destroyed: true });
		}
	}

	return c.json({ error: `Unknown action: ${action} for tile ${tile}` }, 400);
});

export default app;
