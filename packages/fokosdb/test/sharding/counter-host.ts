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
	type FokosPartitionRef,
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
} from "../../src/sharding/index.js";

/**
 * An example host of `FokosShardingRuntime` that uses no FokosDB module. It imports only the
 * sharding entry, so its tests prove that the runtime operates without FokosDB.
 *
 * Each hash key is one counter in one SQL table. A partition counts its local writes, and it splits
 * when the count gets to the threshold of its policy. The host chooses this split rule, not the runtime.
 */

export type CounterPolicy = {
	/** A partition that holds more than one key splits after this number of local writes. */
	maxRequests: number;
};

export type CounterOps = {
	increment: {
		req: { hashKey: KeyBytes; amount: number };
		res: { key: string; val: number };
	};
};

/** What a test reads from one partition. */
export type CounterStats = {
	role: "owner" | "router";
	importState: string | null;
	repartitionState: string | null;
	requestCount: number;
	rows: CounterRow[];
	children: FokosPartitionRef[];
};

type CounterRow = { key: string; val: number };

/** A hash-only host uses no sort key. */
const NO_SORT_KEY = KeyCodec.asKeyBytes(new Uint8Array());

/** Two rows in each migration page, so that a small split takes more than one page. */
const PAGE_ROWS = 2;

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
			stub: (_routeCtx, doName) => env.COUNTER_PARTITION_DO.getByName(doName),
			hooks: this.hooks(),
			operations: this.operations(),
		});
	}

	async increment(
		ctx: FokosRouteContext<CounterPolicy>,
		req: CounterOps["increment"]["req"],
	): Promise<FokosEnvelope<CounterOps["increment"]["res"]>> {
		return await this.fokos.dispatch("increment", ctx, req);
	}

	// ── The runtime RPCs ──────────────────────────────────────────────────────
	// The runtime calls these methods on the other partitions of this class.

	async fokosInit(req: FokosInitRequest): Promise<void> {
		return await this.fokos.fokosInit(req);
	}

	async fokosStartImport(req: FokosStartImportRequest): Promise<void> {
		return await this.fokos.fokosStartImport(req);
	}

	async fokosMigrationPull(req: FokosMigrationPullRequest): Promise<FokosMigrationPage> {
		return await this.fokos.fokosMigrationPull(req);
	}

	async fokosMigrationAck(req: FokosMigrationAckRequest): Promise<void> {
		return await this.fokos.fokosMigrationAck(req);
	}

	async fokosExecuteLocal(req: FokosExecuteLocalRequest): Promise<FokosEnvelope<unknown>> {
		return await this.fokos.fokosExecuteLocal(req);
	}

	async fokosRequestPromotion(req: FokosRequestPromotionRequest): Promise<FokosRequestPromotionResult> {
		return await this.fokos.fokosRequestPromotion(req);
	}

	async fokosStatus(req: FokosStatusRequest): Promise<FokosStatusPage> {
		return await this.fokos.fokosStatus(req);
	}

	async fokosPrepareDestroy(req: FokosPrepareDestroyRequest): Promise<void> {
		return await this.fokos.fokosPrepareDestroy(req);
	}

	async fokosDestroy(): Promise<void> {
		return await this.fokos.fokosDestroy();
	}

	async alarm(info: AlarmInvocationInfo): Promise<void> {
		return await this.fokos.alarm(info);
	}

	// ── Test controls ─────────────────────────────────────────────────────────

	/** Evicts this instance, as a crash does. The storage stays, and an interrupted alarm runs again. */
	async debugAbort(): Promise<void> {
		this.ctx.abort("test kill");
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
				whileMigrating: "throw",
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
			// A hash split cannot divide one key, so a partition with one hot key does not split.
			evaluateSplit: ({ policy }) => (this.requestCount >= policy.maxRequests && this.keyCount() > 1 ? {} : false),
			migration: {
				buildPage: (cursor, _slice, belongsToTarget) => {
					const rows = sql
						.exec<CounterRow>(
							"SELECT key, val FROM counters WHERE key > ? ORDER BY key LIMIT ?",
							(cursor as string | null) ?? "",
							PAGE_ROWS + 1,
						)
						.toArray();
					const page = rows.slice(0, PAGE_ROWS);
					return {
						page: page.filter((r) => belongsToTarget({ hashKey: KeyCodec.encode(r.key), sortKey: NO_SORT_KEY })),
						nextCursor: rows.length > PAGE_ROWS ? page[PAGE_ROWS - 1].key : null,
					};
				},
				applyPage: (page) => {
					for (const r of page as CounterRow[]) sql.exec("INSERT OR REPLACE INTO counters (key, val) VALUES (?, ?)", r.key, r.val);
				},
				validatePage: (_cursor, page) => {
					if (!Array.isArray(page)) throw new Error("counter migration page must be an array");
				},
			},
			// Every child has acknowledged its import, so the rows of this router are old copies.
			cleanupSourceStep: () => {
				sql.exec("DELETE FROM counters");
				return true;
			},
		};
	}
}
