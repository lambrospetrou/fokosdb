import { DurableObject } from "cloudflare:workers";
import {
	FokosShardingRuntime,
	KeyCodec,
	type FokosEnvelope,
	type FokosExecuteLocalRequest,
	type FokosInitRequest,
	type FokosMigrationAckRequest,
	type FokosMigrationPage,
	type FokosMigrationPullRequest,
	type FokosOperations,
	type FokosPrepareDestroyRequest,
	type FokosRequestPromotionRequest,
	type FokosRequestPromotionResult,
	type FokosRouteContext,
	type FokosShardingHooks,
	type FokosShardingRpc,
	type FokosStartImportRequest,
	type FokosStatusPage,
	type FokosStatusRequest,
	type KeyBytes,
} from "fokosdb/sharding";

export type CounterPolicy = {
	/** A partition that holds more than one key splits after this many local writes. */
	maxRequests: number;
};

export type CounterOps = {
	increment: {
		req: { hashKey: KeyBytes; amount: number };
		res: { key: string; val: number };
	};
};

export type CounterStats = {
	role: "owner" | "router";
	importState: string | null;
	repartitionState: string | null;
	requestCount: number;
	rows: Array<{ key: string; val: number }>;
	children: Array<{ doName: string; partitionId: string }>;
};

type CounterRow = { key: string; val: number };

const NO_SORT_KEY = KeyCodec.asKeyBytes(new Uint8Array());

export class CounterPartitionDO extends DurableObject<Env> implements FokosShardingRpc {
	readonly fokos: FokosShardingRuntime<CounterPolicy, CounterOps>;
	private requestCount = 0;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS counters (key TEXT PRIMARY KEY, val INTEGER NOT NULL)");
		ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS counter_meta (key TEXT PRIMARY KEY, val INTEGER NOT NULL)");
		this.requestCount = this.readMeta("requestCount");

		this.fokos = new FokosShardingRuntime<CounterPolicy, CounterOps>({
			ctx,
			stub: (_routeCtx, doName) => env.COUNTER_PARTITION_DO.get(env.COUNTER_PARTITION_DO.idFromName(doName)),
			hooks: this.hooks(),
			operations: this.operations(),
		});
	}

	async increment(
		ctx: FokosRouteContext<CounterPolicy>,
		req: CounterOps["increment"]["req"],
	): Promise<FokosEnvelope<CounterOps["increment"]["res"]>> {
		return this.fokos.dispatch("increment", ctx, req);
	}

	// ── Demo controls ─────────────────────────────────────────────────────────

	/** Evicts this instance, as a crash does. Storage stays, and an interrupted alarm retries. */
	async debugAbort(): Promise<void> {
		this.ctx.abort("demo kill");
	}

	async getCounterStats(): Promise<CounterStats> {
		const lifecycle = this.fokos.initialized() ? this.fokos.lifecycle() : null;
		return {
			role: lifecycle?.role ?? "owner",
			importState: lifecycle?.import?.state ?? null,
			repartitionState: lifecycle?.activeRepartition?.state ?? null,
			requestCount: this.requestCount,
			rows: this.ctx.storage.sql.exec<CounterRow>("SELECT key, val FROM counters").toArray(),
			children: lifecycle ? this.fokos.children().map((c) => c.ref) : [],
		};
	}

	/** The root keeps the count of writes the Worker saw succeed. Reconciliation compares the rows with it. */
	async recordAcknowledged(writes: number): Promise<void> {
		this.writeMeta("acknowledgedWrites", this.readMeta("acknowledgedWrites") + writes);
	}

	async getAcknowledged(): Promise<number> {
		return this.readMeta("acknowledgedWrites");
	}

	/**
	 * Deletes all storage and the alarm (compatibility date 2026-02-24 or later), then evicts the
	 * instance, so its in-memory state goes too and an interrupted alarm does not run again.
	 */
	async resetAll(): Promise<void> {
		await this.ctx.storage.deleteAll();
		this.ctx.abort("demo reset", { retryAlarm: false });
	}

	// ── FokosShardingRpc ──────────────────────────────────────────────────────

	async fokosInit(req: FokosInitRequest): Promise<void> {
		return this.fokos.fokosInit(req);
	}

	async fokosStartImport(req: FokosStartImportRequest): Promise<void> {
		return this.fokos.fokosStartImport(req);
	}

	async fokosMigrationPull(req: FokosMigrationPullRequest): Promise<FokosMigrationPage> {
		return this.fokos.fokosMigrationPull(req);
	}

	async fokosMigrationAck(req: FokosMigrationAckRequest): Promise<void> {
		return this.fokos.fokosMigrationAck(req);
	}

	async fokosExecuteLocal(req: FokosExecuteLocalRequest): Promise<FokosEnvelope<unknown>> {
		return this.fokos.fokosExecuteLocal(req);
	}

	async fokosRequestPromotion(req: FokosRequestPromotionRequest): Promise<FokosRequestPromotionResult> {
		return this.fokos.fokosRequestPromotion(req);
	}

	async fokosStatus(req: FokosStatusRequest): Promise<FokosStatusPage> {
		return this.fokos.fokosStatus(req);
	}

	async fokosPrepareDestroy(req: FokosPrepareDestroyRequest): Promise<void> {
		return this.fokos.fokosPrepareDestroy(req);
	}

	async fokosDestroy(): Promise<void> {
		return this.fokos.fokosDestroy();
	}

	async alarm(info: AlarmInvocationInfo): Promise<void> {
		return this.fokos.alarm(info);
	}

	// ── Storage helpers ───────────────────────────────────────────────────────

	private readMeta(key: string): number {
		return this.ctx.storage.sql.exec<{ val: number }>("SELECT val FROM counter_meta WHERE key = ?", key).toArray()[0]?.val ?? 0;
	}

	private writeMeta(key: string, val: number): void {
		this.ctx.storage.sql.exec("INSERT OR REPLACE INTO counter_meta (key, val) VALUES (?, ?)", key, val);
	}

	private keyCount(): number {
		return this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM counters").one().n;
	}

	// ── Runtime wiring ────────────────────────────────────────────────────────

	private operations(): FokosOperations<CounterOps> {
		const sql = this.ctx.storage.sql;
		return {
			increment: {
				shape: "point",
				whileMigrating: "retry",
				key: (req) => ({ hashKey: req.hashKey, sortKey: NO_SORT_KEY }),
				local: (req, call) => {
					const key = KeyCodec.decode(req.hashKey) as string;
					const current = sql.exec<{ val: number }>("SELECT val FROM counters WHERE key = ?", key).toArray()[0]?.val ?? 0;
					const val = current + req.amount;
					sql.exec("INSERT OR REPLACE INTO counters (key, val) VALUES (?, ?)", key, val);
					this.requestCount += 1;
					this.writeMeta("requestCount", this.requestCount);
					call.signal({ evaluateSplit: true });
					return { key, val };
				},
			},
		};
	}

	private hooks(): FokosShardingHooks<CounterPolicy> {
		const sql = this.ctx.storage.sql;
		return {
			// A hash split cannot divide one key, so a partition with a single hot key never splits.
			evaluateSplit: ({ policy }) => (this.requestCount >= policy.maxRequests && this.keyCount() > 1 ? {} : false),
			migration: {
				// Two rows per page, so even a small demo split takes several pages.
				buildPage: (cursor, _slice, belongsToTarget) => {
					const pageSize = 2;
					const rows = sql
						.exec<CounterRow>(
							"SELECT key, val FROM counters WHERE key > ? ORDER BY key LIMIT ?",
							(cursor as string | null) ?? "",
							pageSize + 1,
						)
						.toArray();
					const page = rows.slice(0, pageSize);
					return {
						page: page.filter((r) => belongsToTarget({ hashKey: KeyCodec.encode(r.key), sortKey: NO_SORT_KEY })),
						nextCursor: rows.length > pageSize ? page[pageSize - 1].key : null,
					};
				},
				applyPage: (page) => {
					for (const r of page as CounterRow[]) {
						sql.exec("INSERT OR REPLACE INTO counters (key, val) VALUES (?, ?)", r.key, r.val);
					}
				},
				validatePage: (_cursor, page) => {
					if (!Array.isArray(page)) throw new Error("counter migration page must be an array");
				},
			},
			// Every target acknowledged its import, so the rows of this router are stale copies.
			cleanupSourceStep: () => {
				sql.exec("DELETE FROM counters");
				return true;
			},
		};
	}
}
