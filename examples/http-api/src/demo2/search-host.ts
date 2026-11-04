import {
	FokosShardingRuntime,
	KeyCodec,
	type FokosCompositeKey,
	type FokosEnvelope,
	type FokosOperations,
	type FokosRangeVisit,
	type FokosRouteContext,
	type FokosShardingHooks,
	type KeyBytes,
} from "fokosdb/sharding/server";
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
};

type DocRow = { tenant_id: string; sort_key: string; title: string; body: string };

/** A migrated document carries its storage key: the routingHash, the hash key, and the sort key. */
type MigratedDoc = DocRow & { routingHash: Uint8Array; hk: KeyBytes; sk: KeyBytes };
type DocCursor = { routingHash: Uint8Array; hk: KeyBytes; sk: KeyBytes };
type SearchReq = SearchOps["search"]["req"];

const PAGE_ROWS = 20;

/** The empty key: the minimum of the hash-key and sort-key columns. */
const NO_KEY = KeyCodec.asKeyBytes(new Uint8Array());

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
		// An FTS5 table has no ordered key. `doc_keys` gives each document its storage key, in the order
		// (routingHash, hash key, sort key), and points to the `docs` row. A migration reads the interval
		// of its target with one seek, and a promotion deletes the rows of one routingHash with one seek.
		ctx.storage.sql.exec(
			"CREATE TABLE IF NOT EXISTS doc_keys (routing_hash BLOB NOT NULL, hk BLOB NOT NULL, sk BLOB NOT NULL, doc_id INTEGER NOT NULL, PRIMARY KEY (routing_hash, hk, sk)) WITHOUT ROWID",
		);
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
		const tenants = this.ctx.storage.sql
			.exec<{
				tenant: string;
				docs: number;
			}>("SELECT tenant_id AS tenant, COUNT(*) AS docs FROM docs GROUP BY tenant_id ORDER BY tenant_id")
			.toArray();
		// A range edge is a composite key: the tenant, then the sort key.
		const edgeTenant = range?.start?.hk ?? range?.end?.hk;
		const sortEdge = (k: FokosCompositeKey | null | undefined) => (k ? decodeEdge(k.sk) : null);
		return {
			role: lifecycle?.role ?? "owner",
			kind: identity?.kind ?? "hash",
			importState: lifecycle?.import?.state ?? null,
			repartitionState: lifecycle?.activeRepartition?.state ?? null,
			tenants,
			range: range
				? {
						tenant: edgeTenant ? (KeyCodec.decode(edgeTenant) as string) : (tenants[0]?.tenant ?? ""),
						start: sortEdge(range.start),
						end: sortEdge(range.end),
					}
				: null,
		};
	}

	// ── Storage helpers ───────────────────────────────────────────────────────

	private docCount(): number {
		return this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM docs").one().n;
	}

	private tenantDocCount(tenant: string): number {
		return this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM docs WHERE tenant_id = ?", tenant).one().n;
	}

	/** Writes one document and its storage key. The caller runs it inside a transaction. */
	private insertDoc(doc: MigratedDoc): void {
		const sql = this.ctx.storage.sql;
		sql.exec("INSERT INTO docs (tenant_id, sort_key, title, body) VALUES (?, ?, ?, ?)", doc.tenant_id, doc.sort_key, doc.title, doc.body);
		sql.exec("INSERT INTO doc_keys (routing_hash, hk, sk, doc_id) VALUES (?, ?, ?, last_insert_rowid())", doc.routingHash, doc.hk, doc.sk);
	}

	// ── Runtime wiring ────────────────────────────────────────────────────────

	private operations(): FokosOperations<SearchOps> {
		const sql = this.ctx.storage.sql;
		return {
			addDoc: {
				shape: "point",
				whileMigrating: "throw",
				key: (req) => ({ hashKey: req.hashKey, sortKey: req.sortKey }),
				local: (req, call) => {
					const tenant = KeyCodec.decode(req.hashKey) as string;
					const sortKey = KeyCodec.decode(req.sortKey) as string;
					const routingHash = this.fokos.routingHashForKey(req.hashKey);
					this.ctx.storage.transactionSync(() =>
						this.insertDoc({
							routingHash,
							hk: req.hashKey,
							sk: req.sortKey,
							tenant_id: tenant,
							sort_key: sortKey,
							title: req.title,
							body: req.body,
						}),
					);
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
			// The boundaries divide the documents of this partition into equal parts in (tenant, sort key)
			// order. A range partition holds the tenants of one routingHash, normally one tenant.
			// It reads the keys of the partition interval from `doc_keys` in key order, so SQLite does
			// not sort and does not read the FTS5 rows. It reads one row at a time and stops at the
			// last boundary, so it keeps only the boundaries in memory.
			computeRangeBoundaries: ({ routingHash, start, end, childCount }) => {
				const params: unknown[] = [routingHash];
				let where = "routing_hash = ?1";
				if (start) {
					where += ` AND (hk, sk) >= (?${params.length + 1}, ?${params.length + 2})`;
					params.push(start.hk, start.sk);
				}
				if (end) {
					where += ` AND (hk, sk) < (?${params.length + 1}, ?${params.length + 2})`;
					params.push(end.hk, end.sk);
				}
				const count = sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM doc_keys WHERE ${where}`, ...params).one().n;
				if (count < childCount) return null;
				// Boundary i is the key at position i * count / childCount. Positions are distinct and above 0,
				// and keys are unique, so the boundaries increase and each child gets at least one document.
				const boundaries: FokosCompositeKey[] = [];
				let position = 0;
				for (const row of sql.exec<{ hk: ArrayBuffer; sk: ArrayBuffer }>(
					`SELECT hk, sk FROM doc_keys WHERE ${where} ORDER BY hk, sk`,
					...params,
				)) {
					if (position === Math.floor(((boundaries.length + 1) * count) / childCount)) {
						boundaries.push({ hk: KeyCodec.asKeyBytes(new Uint8Array(row.hk)), sk: KeyCodec.asKeyBytes(new Uint8Array(row.sk)) });
						if (boundaries.length === childCount - 1) break;
					}
					position++;
				}
				return boundaries;
			},
			migration: {
				// The read seeks to `interval`, the storage interval of the target, and stops at its end.
				// An absent `hk` or `sk` of an edge is the empty key, the minimum. `belongsToTarget` still
				// filters each row: it removes the keys that a range tree owns.
				buildPage: (cursor, _slice, belongsToTarget, _budget, interval) => {
					const edge = (k: { routingHash: Uint8Array; hk?: KeyBytes; sk?: KeyBytes }) => [k.routingHash, k.hk ?? NO_KEY, k.sk ?? NO_KEY];
					const after = cursor as DocCursor | null;
					const params: unknown[] = [PAGE_ROWS];
					let where = after ? "(k.routing_hash, k.hk, k.sk) > (?2, ?3, ?4)" : "(k.routing_hash, k.hk, k.sk) >= (?2, ?3, ?4)";
					params.push(...(after ? [after.routingHash, after.hk, after.sk] : edge(interval.start)));
					if (interval.end) {
						where += " AND (k.routing_hash, k.hk, k.sk) < (?5, ?6, ?7)";
						params.push(...edge(interval.end));
					}
					const rows = sql
						.exec<{ routing_hash: ArrayBuffer; hk: ArrayBuffer; sk: ArrayBuffer } & DocRow>(
							`SELECT k.routing_hash, k.hk, k.sk, d.tenant_id, d.sort_key, d.title, d.body
							   FROM doc_keys k JOIN docs d ON d.rowid = k.doc_id
							  WHERE ${where} ORDER BY k.routing_hash, k.hk, k.sk LIMIT ?1`,
							...params,
						)
						.toArray()
						.map(
							(r): MigratedDoc => ({
								routingHash: new Uint8Array(r.routing_hash),
								hk: KeyCodec.asKeyBytes(new Uint8Array(r.hk)),
								sk: KeyCodec.asKeyBytes(new Uint8Array(r.sk)),
								tenant_id: r.tenant_id,
								sort_key: r.sort_key,
								title: r.title,
								body: r.body,
							}),
						);
					const last = rows.at(-1);
					return {
						page: rows.filter((r) => belongsToTarget({ hashKey: r.hk, sortKey: r.sk })),
						nextCursor:
							rows.length === PAGE_ROWS && last ? ({ routingHash: last.routingHash, hk: last.hk, sk: last.sk } satisfies DocCursor) : null,
					};
				},
				// It runs inside the page transaction of the runtime.
				applyPage: (page) => {
					for (const doc of page as MigratedDoc[]) this.insertDoc(doc);
				},
				validatePage: (_cursor, page) => {
					if (!Array.isArray(page)) throw new Error("search migration page must be an array");
				},
			},
			// Every target has acknowledged its import. A promotion moved the tenants of one
			// routingHash, and a range split moved all rows.
			cleanupSourceStep: (plan) => {
				const slice = plan.targets[0]?.slice;
				if (plan.kind === "key_promotion" && slice?.kind === "promoted_routing_hash") {
					sql.exec("DELETE FROM docs WHERE rowid IN (SELECT doc_id FROM doc_keys WHERE routing_hash = ?)", slice.routingHash);
					sql.exec("DELETE FROM doc_keys WHERE routing_hash = ?", slice.routingHash);
				} else {
					sql.exec("DELETE FROM docs");
					sql.exec("DELETE FROM doc_keys");
				}
				return true;
			},
		};
	}
}
