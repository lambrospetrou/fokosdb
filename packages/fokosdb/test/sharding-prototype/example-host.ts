/**
 * PROTOTYPE. The smallest host: one SQL table `docs(hk, sk, body)`, a put, a get, a scan of one hash key, and
 * a custom traversal that counts rows on every node of the tree. It imports nothing from FokosDB. This is the
 * host that milestone M5 turns into a real test.
 */
import { DurableObject } from "cloudflare:workers";
import { FokosShardingRuntime, todo } from "./api.js";
import type {
	FokosEnvelope,
	FokosOperations,
	FokosRouteContext,
	FokosShardingHooks,
	FokosShardingRpc,
	KeyBytes,
	RouteKey,
	SkInterval,
} from "./api.js";
import type * as Rpc from "./api.js";

type DocPolicy = { ns: string; maxRows: number };
type DocCtx = FokosRouteContext<DocPolicy>;

export type DocsDOOps = DocOps;
type DocOps = {
	put: { req: RouteKey & { body: string }; res: { written: true } };
	get: { req: RouteKey; res: { body: string | null } };
	scan: {
		req: { hashKey: KeyBytes; interval: SkInterval; descending: boolean; limit: number };
		res: { rows: Array<{ sk: KeyBytes; body: string }>; more: boolean };
	};
	/** A custom traversal: every node reports its own row count, and a router adds its children. */
	countAll: { req: Record<string, never>; res: { rows: number; nodes: number } };
};

type DocRpc = FokosShardingRpc & { [K in keyof DocOps]: (ctx: DocCtx, req: DocOps[K]["req"]) => Promise<FokosEnvelope<DocOps[K]["res"]>> };

export class DocsDO extends DurableObject<Env> implements DocRpc {
	readonly fokos: FokosShardingRuntime<DocPolicy, DocOps>;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.fokos = new FokosShardingRuntime<DocPolicy, DocOps>({
			ctx,
			stub: (routeCtx, doName) => {
				const ns = (env as unknown as Record<string, DurableObjectNamespace>)[routeCtx.policy.ns];
				return ns.get(ns.idFromName(doName), { locationHint: undefined });
			},
			hooks: this.hooks(),
			operations: this.operations(),
		});
		ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS docs (hk BLOB NOT NULL, sk BLOB NOT NULL, body TEXT NOT NULL, PRIMARY KEY (hk, sk))");
	}

	put = (ctx: DocCtx, req: DocOps["put"]["req"]) => this.fokos.dispatch("put", ctx, req);
	get = (ctx: DocCtx, req: DocOps["get"]["req"]) => this.fokos.dispatch("get", ctx, req);
	scan = (ctx: DocCtx, req: DocOps["scan"]["req"]) => this.fokos.dispatch("scan", ctx, req);
	countAll = (ctx: DocCtx, req: DocOps["countAll"]["req"]) => this.fokos.dispatch("countAll", ctx, req);

	fokosInit = (req: Rpc.FokosInitRequest) => this.fokos.fokosInit(req);
	fokosStartImport = (req: Rpc.FokosStartImportRequest) => this.fokos.fokosStartImport(req);
	fokosMigrationPull = (req: Rpc.FokosMigrationPullRequest) => this.fokos.fokosMigrationPull(req);
	fokosMigrationAck = (req: Rpc.FokosMigrationAckRequest) => this.fokos.fokosMigrationAck(req);
	fokosExecuteLocal = (req: Rpc.FokosExecuteLocalRequest) => this.fokos.fokosExecuteLocal(req);
	fokosRequestPromotion = (req: Rpc.FokosRequestPromotionRequest) => this.fokos.fokosRequestPromotion(req);
	fokosStatus = (req: Rpc.FokosStatusRequest) => this.fokos.fokosStatus(req);
	fokosPrepareDestroy = (req: Rpc.FokosPrepareDestroyRequest) => this.fokos.fokosPrepareDestroy(req);
	fokosDestroy = () => this.fokos.fokosDestroy();
	alarm = (info: AlarmInvocationInfo) => this.fokos.alarm(info);

	private rowCount(): number {
		return Number(this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM docs").one().n);
	}

	private operations(): FokosOperations<DocOps> {
		const sql = this.ctx.storage.sql;
		return {
			put: {
				shape: "point",
				whileMigrating: "throw",
				key: (req) => req,
				local: (req, call) => {
					sql.exec("INSERT OR REPLACE INTO docs (hk, sk, body) VALUES (?, ?, ?)", req.hashKey, req.sortKey, req.body);
					call.signal({ evaluateSplit: true, promotionCandidates: req.body.length > 1_000 ? [{ hashKey: req.hashKey }] : [] });
					return { written: true };
				},
			},
			get: {
				shape: "point",
				whileMigrating: "read_source",
				readOnly: true,
				key: (req) => req,
				local: (req) => ({
					body:
						sql.exec<{ body: string }>("SELECT body FROM docs WHERE hk = ? AND sk = ?", req.hashKey, req.sortKey).toArray()[0]?.body ??
						null,
				}),
			},
			scan: {
				shape: "range",
				whileMigrating: "read_source",
				readOnly: true,
				range: (req) => ({ hashKey: req.hashKey, interval: req.interval, descending: req.descending }),
				clip: (req, visit) => ({
					...req,
					interval: todo(
						`clipToChildRange(${JSON.stringify(req.interval) ?? "null"}, ${JSON.stringify(visit.start) ?? "null"}, ${JSON.stringify(visit.end) ?? "null"})`,
					),
				}),
				local: (req) => ({ rows: todo(`SELECT sk, body ... LIMIT ${req.limit + 1}`), more: false }),
				walk: async ({ request, visits, local, forward }) => {
					const rows: Array<{ sk: KeyBytes; body: string }> = [];
					for (const visit of visits) {
						const sub = { ...request, limit: request.limit - rows.length };
						const page = visit.target === "local" ? await local(sub) : await forward(visit, sub);
						rows.push(...page.rows);
						if (page.more || rows.length >= request.limit) {
							return { rows: rows.slice(0, request.limit), more: true };
						}
					}
					return { rows, more: false };
				},
			},
			// The host traversal of the FAQ: a `local` shape that reads `children()` and forwards the same operation.
			countAll: {
				shape: "local",
				local: async (req) => {
					const parts = await Promise.all(this.fokos.children().map((child) => this.fokos.forward(child.ref, "countAll", req)));
					return parts.reduce((acc, p) => ({ rows: acc.rows + p.value.rows, nodes: acc.nodes + p.value.nodes }), {
						rows: this.rowCount(),
						nodes: 1,
					});
				},
			},
		};
	}

	private hooks(): FokosShardingHooks<DocPolicy> {
		const sql = this.ctx.storage.sql;
		return {
			evaluateSplit: ({ policy }) => (this.rowCount() > policy.maxRows ? {} : false),
			computeRangeBoundaries: ({ hashKey, start, end, childCount }) =>
				todo(
					`NTILE(${childCount}) over sk of ${JSON.stringify(hashKey) ?? "null"} in [${JSON.stringify(start) ?? "null"}, ${JSON.stringify(end) ?? "null"})`,
				),
			migration: {
				buildPage: (cursor, _slice, belongsToTarget) => {
					const rows = sql
						.exec<{ hk: ArrayBuffer; sk: ArrayBuffer; body: string }>(
							"SELECT hk, sk, body FROM docs WHERE (hk, sk) > (?, ?) ORDER BY hk, sk LIMIT 1000",
							...todo<[KeyBytes, KeyBytes]>(JSON.stringify(cursor) ?? "null"),
						)
						.toArray()
						.map((r) => ({ hk: new Uint8Array(r.hk) as KeyBytes, sk: new Uint8Array(r.sk) as KeyBytes, body: r.body }));
					const page = rows.filter((r) => belongsToTarget({ hashKey: r.hk, sortKey: r.sk }));
					return { page, nextCursor: rows.length < 1000 ? null : [rows.at(-1)!.hk, rows.at(-1)!.sk] };
				},
				applyPage: (page) => {
					for (const r of page as Array<{ hk: KeyBytes; sk: KeyBytes; body: string }>) {
						sql.exec("INSERT OR REPLACE INTO docs (hk, sk, body) VALUES (?, ?, ?)", r.hk, r.sk, r.body);
					}
				},
				validatePage: (_cursor, page) => {
					if (!Array.isArray(page)) {
						throw new Error("docs page must be an array");
					}
				},
			},
			cleanupSourceStep: (plan) => {
				if (plan.kind !== "key_promotion") {
					return true;
				}
				const slice = plan.targets[0].slice;
				if (slice.kind !== "promoted_key") {
					return true;
				}
				sql.exec("DELETE FROM docs WHERE hk = ?", slice.hashKey);
				return true;
			},
		};
	}
}
