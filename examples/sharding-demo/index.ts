import { DurableObject } from "cloudflare:workers";
import { Hono } from "hono";
import { FokosDB, PartitionContextCreator, FokosRouter } from "fokosdb/client";
import { PartitionDO } from "fokosdb/server";

export { PartitionDO } from "fokosdb/server";

export type RouteHop = {
	doName: string;
	role: "root" | "router" | "leaf";
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
};

// ── Demo 1: Counter Partition DO ─────────────────────────────────────────────

type ChildTarget = {
	index: number;
	partitionId: string;
	doName: string;
	acknowledged: boolean;
};

export class CounterPartitionDO extends DurableObject<Env> {
	private requestCount = 0;
	private writeCount = 0;
	private isRouter = false;
	private isKilled = false;
	private childrenList: ChildTarget[] = [];
	private splitState: string = "active";
	private counterStore: Map<string, number> = new Map();

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.ctx.blockConcurrencyWhile(async () => {
			this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, val TEXT)");
			this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS counters (key TEXT PRIMARY KEY, val INTEGER NOT NULL)");
			this.ctx.storage.sql.exec(
				"CREATE TABLE IF NOT EXISTS children (idx INTEGER PRIMARY KEY, partition_id TEXT NOT NULL, do_name TEXT NOT NULL, acked INTEGER NOT NULL)",
			);

			const metaRows = this.ctx.storage.sql.exec<{ key: string; val: string }>("SELECT key, val FROM meta").toArray();
			for (const row of metaRows) {
				if (row.key === "requestCount") this.requestCount = Number(row.val);
				if (row.key === "writeCount") this.writeCount = Number(row.val);
				if (row.key === "isRouter") this.isRouter = row.val === "true";
				if (row.key === "splitState") this.splitState = row.val;
			}

			const counterRows = this.ctx.storage.sql.exec<{ key: string; val: number }>("SELECT key, val FROM counters").toArray();
			for (const row of counterRows) {
				this.counterStore.set(row.key, row.val);
			}

			const childRows = this.ctx.storage.sql
				.exec<{
					idx: number;
					partition_id: string;
					do_name: string;
					acked: number;
				}>("SELECT idx, partition_id, do_name, acked FROM children ORDER BY idx")
				.toArray();
			this.childrenList = childRows.map((r) => ({
				index: r.idx,
				partitionId: r.partition_id,
				doName: r.do_name,
				acknowledged: r.acked === 1,
			}));
		});
	}

	private saveMeta(): void {
		this.ctx.storage.sql.exec(
			"INSERT OR REPLACE INTO meta (key, val) VALUES ('requestCount', ?), ('writeCount', ?), ('isRouter', ?), ('splitState', ?)",
			String(this.requestCount),
			String(this.writeCount),
			String(this.isRouter),
			this.splitState,
		);
	}

	async fokosStatus() {
		const entries = this.childrenList.map((target) => ({
			repartition: {
				id: `split-${target.index}`,
				seq: 1,
				kind: "hash_split" as const,
				state: this.splitState as "queued" | "planned" | "cutover" | "completed",
				hashKey: null,
			},
			target: {
				index: target.index,
				ref: { partitionId: target.partitionId, doName: target.doName },
				initialization: { kind: "hash_child" as const, index: target.index },
				acknowledged: target.acknowledged,
			},
		}));

		return {
			initialized: true,
			destroying: false,
			ref: { partitionId: "counter-root", doName: "counter-root" },
			importState: null,
			entries,
			nextCursor: null,
			requestCount: this.requestCount,
			writeCount: this.writeCount,
			isRouter: this.isRouter,
			splitState: this.splitState,
		};
	}

	async increment(key: string, amount = 1, callingHop?: RouteHop): Promise<{ key: string; val: number; trace: ActionTrace }> {
		this.requestCount += 1;

		if (this.isRouter && this.childrenList.length > 0) {
			this.saveMeta();
			// Route to target child based on key hash
			let hash = 0;
			for (let i = 0; i < key.length; i++) hash = (hash * 31 + key.charCodeAt(i)) >>> 0;
			const targetIdx = hash % this.childrenList.length;
			const childTarget = this.childrenList[targetIdx];
			const childStub = this.env.COUNTER_PARTITION_DO.get(this.env.COUNTER_PARTITION_DO.idFromName(childTarget.doName));
			const currentHop: RouteHop = {
				doName: "counter-root",
				role: "router",
				partitionId: "counter-root",
			};
			const res = await childStub.increment(key, amount, currentHop);
			return {
				key: res.key,
				val: res.val,
				trace: {
					servedBy: [currentHop, ...res.trace.servedBy],
					forwardCount: res.trace.forwardCount + 1,
				},
			};
		}

		// Local leaf increment
		const prev = this.counterStore.get(key) ?? 0;
		const next = prev + amount;
		this.counterStore.set(key, next);
		this.writeCount += 1;
		this.ctx.storage.sql.exec("INSERT OR REPLACE INTO counters (key, val) VALUES (?, ?)", key, next);
		this.saveMeta();

		const selfHop: RouteHop = {
			doName: callingHop ? "counter-child" : "counter-root",
			role: "leaf",
			partitionId: callingHop ? "counter-child" : "counter-root",
		};
		return {
			key,
			val: next,
			trace: {
				servedBy: [selfHop],
				forwardCount: 0,
			},
		};
	}

	async triggerSplit(childCount = 4): Promise<{ split: boolean; children: ChildTarget[] }> {
		if (this.isRouter) {
			return { split: false, children: this.childrenList };
		}
		this.isRouter = true;
		this.splitState = "cutover";
		this.childrenList = [];

		this.ctx.storage.sql.exec("DELETE FROM children");
		for (let i = 0; i < childCount; i++) {
			const target: ChildTarget = {
				index: i,
				partitionId: `counter-c-${i}`,
				doName: `counter-c-${i}`,
				acknowledged: true,
			};
			this.childrenList.push(target);
			this.ctx.storage.sql.exec(
				"INSERT INTO children (idx, partition_id, do_name, acked) VALUES (?, ?, ?, 1)",
				target.index,
				target.partitionId,
				target.doName,
			);
		}
		this.saveMeta();
		return { split: true, children: this.childrenList };
	}

	async debugAbort(): Promise<{ aborted: true }> {
		this.isKilled = true;
		// Evicts the Durable Object instance from memory.
		this.ctx.abort();
		return { aborted: true };
	}

	async reset(): Promise<{ reset: boolean }> {
		this.requestCount = 0;
		this.writeCount = 0;
		this.isRouter = false;
		this.splitState = "active";
		this.childrenList = [];
		this.counterStore.clear();
		this.ctx.storage.sql.exec("DELETE FROM meta");
		this.ctx.storage.sql.exec("DELETE FROM counters");
		this.ctx.storage.sql.exec("DELETE FROM children");
		return { reset: true };
	}

	async getStats() {
		return {
			requestCount: this.requestCount,
			writeCount: this.writeCount,
			isRouter: this.isRouter,
			isKilled: this.isKilled,
			childCount: this.childrenList.length,
			itemCount: this.counterStore.size,
		};
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
			}>("SELECT id, title, body FROM docs WHERE tenant_id = ? AND (title LIKE ? OR body LIKE ?) LIMIT 10", tenantId, `%${query}%`, `%${query}%`)
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
		const stub = c.env.COUNTER_PARTITION_DO.get(c.env.COUNTER_PARTITION_DO.idFromName("counter-root"));
		const status = await stub.fokosStatus();
		const stats = await stub.getStats();

		const rootNode: TopologyItem = {
			id: "counter-root",
			doName: "counter-root",
			role: status.isRouter ? "router" : "leaf",
			kind: "hash",
			depth: 0,
			status: status.splitState,
			importState: status.importState,
			hashKey: null,
			itemCount: stats.itemCount,
			requestCount: stats.requestCount,
			children: [],
		};

		if (status.isRouter && status.entries.length > 0) {
			for (const entry of status.entries) {
				if (entry.target) {
					rootNode.children.push({
						id: entry.target.ref.partitionId,
						doName: entry.target.ref.doName,
						role: "leaf",
						kind: "hash",
						depth: 1,
						status: "active",
						importState: null,
						hashKey: null,
						itemCount: Math.round(stats.itemCount / status.entries.length),
						requestCount: Math.round(stats.requestCount / status.entries.length),
						children: [],
					});
				}
			}
		}

		const totalPartitions = 1 + rootNode.children.length;
		const routerCount = status.isRouter ? 1 : 0;
		const leafCount = status.isRouter ? rootNode.children.length : 1;

		const response: TileTopology = {
			tile: "demo1",
			roots: [rootNode],
			summary: {
				totalPartitions,
				routerCount,
				leafCount,
				splitCount: status.isRouter ? 1 : 0,
			},
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
		const stub = c.env.COUNTER_PARTITION_DO.get(c.env.COUNTER_PARTITION_DO.idFromName("counter-root"));

		if (action === "increment") {
			const key = (body.key as string) ?? `counter-${Math.floor(Math.random() * 20)}`;
			const amount = (body.amount as number) ?? 1;
			const res = await stub.increment(key, amount);
			const stats = await stub.getStats();
			return c.json({
				success: true,
				action,
				result: res,
				trace: res.trace,
				stats,
			});
		}

		if (action === "batch-increment") {
			const count = (body.count as number) ?? 10;
			const traces: ActionTrace[] = [];
			for (let i = 0; i < count; i++) {
				const key = `counter-${Math.floor(Math.random() * 20)}`;
				const res = await stub.increment(key, 1);
				traces.push(res.trace);
			}
			const stats = await stub.getStats();
			return c.json({
				success: true,
				action,
				count,
				trace: traces[traces.length - 1],
				allTraces: traces,
				stats,
			});
		}

		if (action === "split") {
			const childCount = (body.childCount as number) ?? 4;
			const res = await stub.triggerSplit(childCount);
			const stats = await stub.getStats();
			return c.json({ success: true, action, ...res, stats });
		}

		if (action === "kill") {
			// Forces Cloudflare to evict the Durable Object
			try {
				await stub.debugAbort();
			} catch {
				// abort terminates the DO execution context
			}
			return c.json({ success: true, action, killed: true });
		}

		if (action === "reset") {
			const res = await stub.reset();
			return c.json({ success: true, action, ...res });
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
