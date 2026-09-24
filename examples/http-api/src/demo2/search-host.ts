import {
	FokosShardingRuntime,
	KeyCodec,
	type FokosEnvelope,
	type FokosOperations,
	type FokosPartitionRef,
	type FokosRangeVisit,
	type FokosRouteContext,
	type FokosShardingHooks,
	type FokosStatusCursor,
	type KeyBytes,
} from "fokosdb/sharding";
import { ShardedDurableObject } from "./sharded-do.js";

/**
 * Demo 2: a multi-tenant full-text search host.
 *
 * The tenant id is the hash key, so all tenants start in one shared hash partition. The sort key
 * is `<date>#<id>`. When a tenant gets to `promoteAtDocs` documents, the host asks the runtime to
 * promote the tenant to a range tree of its own. A range partition splits by sort key, so a
 * tenant's documents divide by date. A search with a date window visits only the date partitions
 * of that window.
 */

export type SearchPolicy = {
	/** A tenant with this number of documents in a shared hash partition moves to a range tree of its own. */
	promoteAtDocs: number;
	/** A range partition with this number of documents splits by sort key. */
	rangeSplitAtDocs: number;
};

export type SearchOps = {
	addDoc: {
		req: { hashKey: KeyBytes; sortKey: KeyBytes; title: string; body: string };
		res: { tenantDocs: number };
	};
	search: {
		/**
		 * `query` is an FTS5 query. `start` (inclusive) and `end` (exclusive) are the sort-key window,
		 * and null is an open edge. A date such as "2026-01-01" is a valid edge, because each sort key
		 * starts with a date. The hits come newest first, and the search stops after `limit` hits.
		 */
		req: { hashKey: KeyBytes; query: string; start: string | null; end: string | null; limit: number };
		/**
		 * `stoppedEarly` is true when the limit stopped the search before it visited all planned partitions.
		 * When FTS5 cannot parse the query, `error` holds the message and `hits` is empty.
		 */
		res: { hits: SearchHit[]; stoppedEarly: boolean; error?: string };
	};
};

/** `snippet` has U+E000 before each matched term and U+E001 after it. `partition` is the DO name that found the hit. */
export type SearchHit = { sortKey: string; title: string; snippet: string; score: number; partition: string };

/** What the Worker reads from one partition to draw the tree. */
export type SearchStats = {
	role: "owner" | "router";
	kind: "hash" | "range";
	importState: string | null;
	repartitionState: string | null;
	tenants: Array<{ tenant: string; docs: number }>;
	/** The tenant and the sort-key interval of a range partition. Null is an open edge. */
	range: { tenant: string; start: string | null; end: string | null } | null;
	/** The split children first, then the range roots of the tenants that this partition promoted. */
	children: FokosPartitionRef[];
};

type DocRow = { tenant_id: string; sort_key: string; title: string; body: string };
type SearchReq = SearchOps["search"]["req"];

const PAGE_ROWS = 20;

/** A sort-key edge as a string. Null and the empty key are the open edge. */
function decodeEdge(k: KeyBytes | null): string | null {
	return k === null || k.length === 0 ? null : (KeyCodec.decode(k) as string);
}

/**
 * Limits a search to the part of its window inside one planned visit. A visit covers the interval
 * of its partition, and that interval can be wider than the window.
 */
function clipToVisit(req: SearchReq, visit: FokosRangeVisit): SearchReq {
	const visitStart = decodeEdge(visit.start);
	const visitEnd = decodeEdge(visit.end);
	const start = req.start === null || (visitStart !== null && visitStart > req.start) ? visitStart : req.start;
	const end = req.end === null || (visitEnd !== null && visitEnd < req.end) ? visitEnd : req.end;
	return { ...req, start, end };
}

export class SearchPartitionDO extends ShardedDurableObject<SearchPolicy, SearchOps> {
	readonly fokos: FokosShardingRuntime<SearchPolicy, SearchOps>;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		ctx.storage.sql.exec("CREATE VIRTUAL TABLE IF NOT EXISTS docs USING fts5(tenant_id UNINDEXED, sort_key UNINDEXED, title, body)");
		this.fokos = new FokosShardingRuntime<SearchPolicy, SearchOps>({
			ctx,
			stub: (_routeCtx, doName) => env.SEARCH_PARTITION_DO.get(env.SEARCH_PARTITION_DO.idFromName(doName)),
			hooks: this.hooks(),
			operations: this.operations(),
		});
	}

	async addDoc(ctx: FokosRouteContext<SearchPolicy>, req: SearchOps["addDoc"]["req"]): Promise<FokosEnvelope<SearchOps["addDoc"]["res"]>> {
		return this.fokos.dispatch("addDoc", ctx, req);
	}

	async search(ctx: FokosRouteContext<SearchPolicy>, req: SearchOps["search"]["req"]): Promise<FokosEnvelope<SearchOps["search"]["res"]>> {
		return this.fokos.dispatch("search", ctx, req);
	}

	// ── Demo controls ─────────────────────────────────────────────────────────

	async getSearchStats(): Promise<SearchStats> {
		const identity = this.fokos.initialized() ? this.fokos.identity() : null;
		const lifecycle = identity ? this.fokos.lifecycle() : null;
		const range = identity?.range;
		return {
			role: lifecycle?.role ?? "owner",
			kind: identity?.kind ?? "hash",
			importState: lifecycle?.import?.state ?? null,
			repartitionState: lifecycle?.activeRepartition?.state ?? null,
			tenants: this.ctx.storage.sql
				.exec<{
					tenant: string;
					docs: number;
				}>("SELECT tenant_id AS tenant, COUNT(*) AS docs FROM docs GROUP BY tenant_id ORDER BY tenant_id")
				.toArray(),
			range: range
				? { tenant: KeyCodec.decode(range.hashKey) as string, start: decodeEdge(range.start), end: decodeEdge(range.end) }
				: null,
			children: identity ? [...this.fokos.children().map((c) => c.ref), ...(await this.promotedRangeRoots())] : [],
		};
	}

	// ── Storage helpers ───────────────────────────────────────────────────────

	private docCount(): number {
		return this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM docs").one().n;
	}

	private tenantDocCount(tenant: string): number {
		return this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM docs WHERE tenant_id = ?", tenant).one().n;
	}

	/** The range roots that this partition created by promotion. The status pages of the runtime list them. */
	private async promotedRangeRoots(): Promise<FokosPartitionRef[]> {
		const refs: FokosPartitionRef[] = [];
		let cursor: FokosStatusCursor | null = null;
		do {
			const page = await this.fokos.fokosStatus({ cursor });
			for (const e of page.entries) if (e.repartition.kind === "key_promotion" && e.target) refs.push(e.target.ref);
			cursor = page.nextCursor;
		} while (cursor);
		return refs;
	}

	// ── Runtime wiring ────────────────────────────────────────────────────────

	private operations(): FokosOperations<SearchOps> {
		const sql = this.ctx.storage.sql;
		return {
			addDoc: {
				shape: "point",
				whileMigrating: "retry",
				key: (req) => ({ hashKey: req.hashKey, sortKey: req.sortKey }),
				local: (req, call) => {
					const tenant = KeyCodec.decode(req.hashKey) as string;
					const sortKey = KeyCodec.decode(req.sortKey) as string;
					sql.exec("INSERT INTO docs (tenant_id, sort_key, title, body) VALUES (?, ?, ?, ?)", tenant, sortKey, req.title, req.body);
					const tenantDocs = this.tenantDocCount(tenant);
					// A hash partition gives a large tenant its own range tree. A range partition is already that tree.
					if (this.fokos.identity().kind === "hash" && tenantDocs >= this.fokos.policy().promoteAtDocs) {
						call.signal({ promotionCandidates: [{ hashKey: req.hashKey }] });
					}
					call.signal({ evaluateSplit: true });
					return { tenantDocs };
				},
			},
			search: {
				shape: "range",
				whileMigrating: "read_source",
				readOnly: true,
				// A forwarded visit carries its clipped window, so the partition that gets it plans that window only.
				range: (req) => ({
					hashKey: req.hashKey,
					interval: {
						lower: req.start === null ? undefined : { value: KeyCodec.encode(req.start), inclusive: true },
						upper: req.end === null ? undefined : { value: KeyCodec.encode(req.end), inclusive: false },
					},
					descending: true,
				}),
				clip: clipToVisit,
				local: (req) => {
					const partition = this.fokos.identity().ref.doName;
					try {
						const rows = sql
							.exec<{ sort_key: string; title: string; snippet: string; score: number }>(
								`SELECT sort_key, title, snippet(docs, 3, char(57344), char(57345), '…', 12) AS snippet, bm25(docs) AS score
								 FROM docs
								 WHERE docs MATCH ? AND tenant_id = ? AND (? IS NULL OR sort_key >= ?) AND (? IS NULL OR sort_key < ?)
								 ORDER BY sort_key DESC LIMIT ?`,
								req.query,
								KeyCodec.decode(req.hashKey) as string,
								req.start,
								req.start,
								req.end,
								req.end,
								req.limit,
							)
							.toArray();
						const hits = rows.map((r) => ({ sortKey: r.sort_key, title: r.title, snippet: r.snippet, score: r.score, partition }));
						return { hits, stoppedEarly: false };
					} catch (err) {
						// The query text is the only part of the statement that changes, so it caused the SQL error.
						// The error goes back as a value, because the runtime replaces the message of a thrown error.
						return { hits: [], stoppedEarly: false, error: err instanceof Error ? err.message : String(err) };
					}
				},
				// The visits do not overlap and come newest first. When the limit is full, the walk skips the older visits.
				walk: async ({ request, visits, local, forward }) => {
					const hits: SearchHit[] = [];
					for (const [i, visit] of visits.entries()) {
						const sub = { ...clipToVisit(request, visit), limit: request.limit - hits.length };
						const part = visit.target === "local" ? await local(sub) : await forward(visit, sub);
						if (part.error) return part;
						hits.push(...part.hits);
						if (hits.length >= request.limit) return { hits, stoppedEarly: part.stoppedEarly || i < visits.length - 1 };
					}
					return { hits, stoppedEarly: false };
				},
			},
		};
	}

	private hooks(): FokosShardingHooks<SearchPolicy> {
		const sql = this.ctx.storage.sql;
		return {
			// A shared hash partition does not split. A large tenant leaves it by promotion.
			evaluateSplit: ({ identity, policy }) => (identity.kind === "range" && this.docCount() >= policy.rangeSplitAtDocs ? {} : false),
			// The boundaries divide the documents of this partition into equal parts in sort-key order.
			// A range partition holds the documents of one tenant only.
			computeRangeBoundaries: ({ childCount }) => {
				const keys = sql
					.exec<{ sort_key: string }>("SELECT sort_key FROM docs ORDER BY sort_key")
					.toArray()
					.map((r) => r.sort_key);
				if (keys.length < childCount) return null;
				return Array.from({ length: childCount - 1 }, (_, i) => KeyCodec.encode(keys[Math.floor(((i + 1) * keys.length) / childCount)]));
			},
			migration: {
				buildPage: (cursor, _slice, belongsToTarget) => {
					const [tenant, sortKey] = (cursor as [string, string] | null) ?? ["", ""];
					const rows = sql
						.exec<DocRow>(
							"SELECT tenant_id, sort_key, title, body FROM docs WHERE (tenant_id, sort_key) > (?, ?) ORDER BY tenant_id, sort_key LIMIT ?",
							tenant,
							sortKey,
							PAGE_ROWS,
						)
						.toArray();
					const last = rows.at(-1);
					return {
						page: rows.filter((r) => belongsToTarget({ hashKey: KeyCodec.encode(r.tenant_id), sortKey: KeyCodec.encode(r.sort_key) })),
						nextCursor: rows.length === PAGE_ROWS && last ? [last.tenant_id, last.sort_key] : null,
					};
				},
				applyPage: (page) => {
					for (const r of page as DocRow[]) {
						sql.exec("INSERT INTO docs (tenant_id, sort_key, title, body) VALUES (?, ?, ?, ?)", r.tenant_id, r.sort_key, r.title, r.body);
					}
				},
				validatePage: (_cursor, page) => {
					if (!Array.isArray(page)) throw new Error("search migration page must be an array");
				},
			},
			// Every target has acknowledged its import. A promotion moved one tenant, and a range split moved all rows.
			cleanupSourceStep: (plan) => {
				const slice = plan.targets[0]?.slice;
				if (plan.kind === "key_promotion" && slice?.kind === "promoted_key") {
					sql.exec("DELETE FROM docs WHERE tenant_id = ?", KeyCodec.decode(slice.hashKey) as string);
				} else {
					sql.exec("DELETE FROM docs");
				}
				return true;
			},
		};
	}
}
