import { Hono } from "hono";
import { FokosShardingClient, KeyCodec } from "fokosdb/sharding";
import type { SearchOps, SearchPolicy, SearchStats } from "./search-host.js";
import { collectTree, jsonBody, retryWhileMigrating, topologyOf, traceOf, type TreeNode } from "./shared.js";

/** Demo 2 routes: document writes, the full-text search, and the topology of the tenants. */

function client(env: Env) {
	return new FokosShardingClient<SearchPolicy, SearchOps>({
		topology: { shardGroup: "search_demo", rootTreesN: 1, hashSplitN: 2 },
		rangeConfig: { rangeSplitN: 2, rangeAncestors: { fromRoot: 0, fromLeaf: 3 } },
		policy: { promoteAtDocs: 5, rangeSplitAtDocs: 8 },
		stub: (_ctx, doName) => stub(env, doName),
	});
}

/** Each topic has its own words, so different queries find different documents. */
const SAMPLE_DOCS = [
	{ title: "Database failover drill", body: "the primary database failover took four minutes and replication lag caused stale reads" },
	{ title: "Monthly invoice", body: "invoice for storage and compute, payment due in thirty days" },
	{ title: "Search roadmap", body: "roadmap for search features: prefix queries, snippets and ranking" },
	{ title: "Hiring update", body: "hiring two backend engineers and one database specialist" },
	{ title: "Network outage report", body: "outage in the eu region, traffic moved to the secondary region after failover" },
	{ title: "Postmortem: cache stampede", body: "a cache stampede after the deploy overloaded the database" },
];

/** The maximum number of hits that one search returns. */
const MAX_HITS = 100;
const DAY_MS = 86_400_000;

function stub(env: Env, doName: string) {
	return env.SEARCH_PARTITION_DO.getByName(doName);
}

function collectSearchTree(env: Env): Promise<TreeNode<SearchStats>[]> {
	const search = client(env);
	// The table has one root, so every tenant resolves to it.
	const root = search.resolve({ hashKey: KeyCodec.encode("root"), sortKey: KeyCodec.encodeOptional(undefined) });
	return collectTree(search, root, (doName) => stub(env, doName).getSearchStats());
}

/** Adds one sample document with a random date in the last 730 days. The date starts the sort key. */
async function addDoc(env: Env, tenant: string) {
	const day = new Date(Date.now() - Math.floor(Math.random() * 730) * DAY_MS).toISOString().slice(0, 10);
	const hashKey = KeyCodec.encode(tenant);
	const sortKey = KeyCodec.encode(`${day}#${crypto.randomUUID().slice(0, 8)}`);
	const doc = SAMPLE_DOCS[Math.floor(Math.random() * SAMPLE_DOCS.length)];
	const search = client(env);
	const { value, routing } = await search.point(
		"addDoc",
		{ hashKey, sortKey },
		{ hashKey, sortKey, ...doc },
		{ retry: retryWhileMigrating() },
	);
	return { value, trace: traceOf(search.resolve({ hashKey, sortKey }), routing) };
}

/** A shared hash partition shows its tenants. A range partition shows its tenant and its dates. */
function label(stats: SearchStats): string | null {
	if (stats.range) return `${stats.range.tenant} ${stats.range.start?.slice(0, 10) ?? "…"} → ${stats.range.end?.slice(0, 10) ?? "…"}`;
	return stats.tenants.map((t) => `${t.tenant}:${t.docs}`).join(" ") || null;
}

export const searchRoutes = new Hono<{ Bindings: Env }>();

searchRoutes.get("/topology", async (c) => {
	const nodes = await collectSearchTree(c.env);
	return c.json(
		topologyOf("demo2", nodes, ({ stats }) => ({
			role: stats.role === "router" ? "router" : "leaf",
			kind: stats.kind,
			status: stats.repartitionState ?? "active",
			importState: stats.importState,
			label: label(stats),
			itemCount: stats.tenants.reduce((sum, t) => sum + t.docs, 0),
		})),
	);
});

searchRoutes.post("/add-docs", async (c) => {
	const { tenant = "acme", count = 1 } = await jsonBody<{ tenant: string; count: number }>(c);
	let last: Awaited<ReturnType<typeof addDoc>> | undefined;
	for (let i = 0; i < count; i++) last = await addDoc(c.env, tenant);
	return c.json({ success: true, result: { tenant, added: count, tenantDocs: last?.value.tenantDocs }, trace: last?.trace });
});

searchRoutes.post("/search", async (c) => {
	const body = await jsonBody<{ tenant: string; query: string; from: string | null; to: string | null; limit: number }>(c);
	const tenant = body.tenant ?? "acme";
	const hashKey = KeyCodec.encode(tenant);
	const search = client(c.env);
	const req = {
		hashKey,
		query: body.query ?? "failover",
		start: body.from ?? null,
		end: body.to ?? null,
		limit: Math.min(body.limit ?? MAX_HITS, MAX_HITS),
	};
	// The same range that the `search` operation of the host reads from the request.
	const input = {
		hashKey,
		interval: {
			lower: req.start === null ? undefined : { value: KeyCodec.encode(req.start), inclusive: true },
			upper: req.end === null ? undefined : { value: KeyCodec.encode(req.end), inclusive: false },
		},
		descending: true,
	};
	const { value, routing } = await search.range("search", input, req);
	if (value.error) return c.json({ success: false, error: `invalid query: ${value.error}` });

	// The visited partitions are the leaf partitions that ran the search.
	// The skipped partitions are the other leaf partitions of the tenant.
	const visited = routing.servedBy.filter((s) => s.role !== "merged").map((s) => s.ref.doName);
	const holdsTenant = (stats: SearchStats) =>
		stats.range ? stats.range.tenant === tenant : stats.tenants.some((t) => t.tenant === tenant);
	const leaves = (await collectSearchTree(c.env)).filter(({ stats }) => stats.role === "owner" && holdsTenant(stats));
	const skipped = leaves.map((n) => n.ref.doName).filter((doName) => !visited.includes(doName));
	return c.json({ success: true, result: { tenant, ...value, visited, skipped }, trace: traceOf(search.resolveRange(input), routing) });
});

searchRoutes.post("/reset", async (c) => {
	await client(c.env).destroy();
	return c.json({ success: true });
});
