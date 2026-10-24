import { DurableObject } from "cloudflare:workers";
import {
	FokosShardingRuntime,
	KeyCodec,
	routedError,
	type FokosEnvelope,
	type FokosExecuteLocalRequest,
	type FokosInitRequest,
	type FokosMigrationAckRequest,
	type FokosMigrationPage,
	type FokosMigrationPullRequest,
	type FokosOperations,
	type FokosPrepareDestroyRequest,
	type FokosPromotionsPage,
	type FokosPromotionsRequest,
	type FokosRequestPromotionRequest,
	type FokosRequestPromotionResult,
	type FokosRouteContext,
	type FokosShardingHooks,
	type FokosShardingRpc,
	type FokosStartImportRequest,
	type FokosStatusPage,
	type FokosStatusRequest,
	type KeyBytes,
	topologyAdminName,
} from "../../src/sharding/index-server.js";

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
	/**
	 * Increments many counters with the failure policy `collect`. Admission refuses a part with a key
	 * that starts with `refuse-`, and the local handler throws, before any write, for a part with a key
	 * that starts with `throw-`.
	 */
	incrementMany: {
		req: { items: Array<{ hashKey: KeyBytes; amount: number }> };
		res: { applied: CounterRow[]; failed: CounterFailure[] };
	};
};

/** One failed part of `incrementMany`: its target, its keys, and the partition that raised the error. */
export type CounterFailure = { target: string; keys: string[]; message: string; raiser: string | null };

/** What a test reads from one partition. */
export type CounterStats = {
	role: "owner" | "router";
	importState: string | null;
	repartitionState: string | null;
	requestCount: number;
	rows: CounterRow[];
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
			// The constructor reads the test control, so a restart can remove the hook.
			...(this.readMeta("outboxDisabled") === 1 ? {} : { adminOutbox: { send: (entry) => this.sendToAdmin(entry) } }),
		});
	}

	/**
	 * Sends a topology report to the admin of the shard group. The test control `outboxFailures` makes
	 * the next sends throw.
	 */
	private async sendToAdmin(entry: { eventType: string; id: string; event: unknown }): Promise<void> {
		this.writeMeta("outboxSends", this.readMeta("outboxSends") + 1);
		const failures = this.readMeta("outboxFailures");
		if (failures > 0) {
			this.writeMeta("outboxFailures", failures - 1);
			throw new Error("counter host: the admin outbox send fails");
		}
		const shardGroup = this.fokos.routeContext().topology.shardGroup;
		await this.env.TOPOLOGY_ADMIN_DO.getByName(topologyAdminName(shardGroup)).registerRepartition(entry.event);
	}

	async increment(
		ctx: FokosRouteContext<CounterPolicy>,
		req: CounterOps["increment"]["req"],
	): Promise<FokosEnvelope<CounterOps["increment"]["res"]>> {
		return await this.fokos.dispatch("increment", ctx, req);
	}

	async incrementMany(
		ctx: FokosRouteContext<CounterPolicy>,
		req: CounterOps["incrementMany"]["req"],
	): Promise<FokosEnvelope<CounterOps["incrementMany"]["res"]>> {
		return await this.fokos.dispatch("incrementMany", ctx, req);
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

	async fokosPromotions(req: FokosPromotionsRequest): Promise<FokosPromotionsPage> {
		return await this.fokos.fokosPromotions(req);
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

	/**
	 * `disabled` takes effect at the next start of the instance. `failures` is the number of the next
	 * outbox sends that throw.
	 */
	async debugSetOutbox(opts: { disabled?: boolean; failures?: number }): Promise<void> {
		if (opts.disabled !== undefined) {
			this.writeMeta("outboxDisabled", opts.disabled ? 1 : 0);
		}
		if (opts.failures !== undefined) {
			this.writeMeta("outboxFailures", opts.failures);
		}
	}

	/** The rows of the admin outbox, and the number of sends that the hook received. */
	async debugOutbox(): Promise<{ rows: Array<{ id: string; attempt: number }>; sends: number }> {
		return {
			rows: this.ctx.storage.sql.exec<{ id: string; attempt: number }>("SELECT id, attempt FROM fokos_admin_outbox").toArray(),
			sends: this.readMeta("outboxSends"),
		};
	}

	async getCounterStats(): Promise<CounterStats> {
		const lifecycle = this.fokos.initialized() ? this.fokos.lifecycle() : null;
		return {
			role: lifecycle?.role ?? "owner",
			importState: lifecycle?.import?.state ?? null,
			repartitionState: lifecycle?.activeRepartition?.state ?? null,
			requestCount: this.requestCount,
			rows: this.ctx.storage.sql.exec<CounterRow>("SELECT key, val FROM counters").toArray(),
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
			incrementMany: {
				shape: "group",
				whileMigrating: "throw",
				admissionTag: "increment_many",
				failurePolicy: "collect",
				items: (req) => req.items.map((item) => ({ key: { hashKey: item.hashKey, sortKey: NO_SORT_KEY }, item })),
				subRequest: (req, items) => ({ items: items as CounterOps["incrementMany"]["req"]["items"] }),
				local: (req) => {
					const keys = req.items.map((item) => KeyCodec.decode(item.hashKey) as string);
					if (keys.some((key) => key.startsWith("throw-"))) {
						throw new Error("counter host: the local handler throws");
					}
					const applied = req.items.map((item, i) => {
						const val = (sql.exec<{ val: number }>("SELECT val FROM counters WHERE key = ?", keys[i]).toArray()[0]?.val ?? 0) + item.amount;
						sql.exec("INSERT OR REPLACE INTO counters (key, val) VALUES (?, ?)", keys[i], val);
						return { key: keys[i], val };
					});
					return { applied, failed: [] };
				},
				merge: (parts) => {
					const out: CounterOps["incrementMany"]["res"] = { applied: [], failed: [] };
					for (const part of parts) {
						if ("result" in part) {
							out.applied.push(...part.result.applied);
							out.failed.push(...part.result.failed);
							continue;
						}
						out.failed.push({
							target: part.target === "local" ? "local" : part.target.doName,
							keys: part.request.items.map((item) => KeyCodec.decode(item.hashKey) as string),
							message: String((part.error as Error | undefined)?.message),
							raiser: routedError(part.error)?.routing.servedBy[0]?.ref.doName ?? null,
						});
					}
					return out;
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
					for (const r of page as CounterRow[]) {
						sql.exec("INSERT OR REPLACE INTO counters (key, val) VALUES (?, ?)", r.key, r.val);
					}
				},
				validatePage: (_cursor, page) => {
					if (!Array.isArray(page)) {
						throw new Error("counter migration page must be an array");
					}
				},
			},
			admit: ({ admissionTag, keys }) =>
				admissionTag === "increment_many" && keys.some((key) => (KeyCodec.decode(key.hashKey) as string).startsWith("refuse-"))
					? { reject: new Error("counter host: admission refuses the part") }
					: "allow",
			// Every child has acknowledged its import, so the rows of this router are old copies.
			cleanupSourceStep: () => {
				sql.exec("DELETE FROM counters");
				return true;
			},
		};
	}
}
