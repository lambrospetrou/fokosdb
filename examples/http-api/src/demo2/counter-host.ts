import {
	FokosShardingRuntime,
	KeyCodec,
	type FokosEnvelope,
	type FokosOperations,
	type FokosRouteContext,
	type FokosShardingHooks,
	type KeyBytes,
} from "fokosdb/sharding/server";
import { ShardedDurableObject } from "./sharded-do.js";

/**
 * Demo 1: a hash-only counter host.
 *
 * Each hash key is one counter. A partition counts its local writes, and it splits when the count
 * gets to the threshold. The host chooses this split rule, not the runtime.
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

/** What the Worker reads from one partition to draw the tree and to reconcile the writes. */
export type CounterStats = {
	role: "owner" | "router";
	importState: string | null;
	repartitionState: string | null;
	requestCount: number;
	rows: CounterRow[];
};

type CounterRow = { key: string; val: number };

/** A migrated row and a migration cursor carry the routingHash, the first key column of `counters`. */
type MigratedCounterRow = CounterRow & { routingHash: Uint8Array };
type CounterCursor = { routingHash: Uint8Array; key: string };

/** A hash-only host uses no sort key. */
const NO_SORT_KEY = KeyCodec.asKeyBytes(new Uint8Array());

/** Two rows in each migration page, so that a small demo split takes more than one page. */
const PAGE_ROWS = 2;

export class CounterPartitionDO extends ShardedDurableObject<CounterPolicy, CounterOps> {
	readonly fokos: FokosShardingRuntime<CounterPolicy, CounterOps>;
	private requestCount = 0;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		// The routingHash of the key is the first key column, so a migration reads the interval of its
		// target with one seek and does not read the rows of other targets.
		ctx.storage.sql.exec(
			"CREATE TABLE IF NOT EXISTS counters (routing_hash BLOB NOT NULL, key TEXT NOT NULL, val INTEGER NOT NULL, PRIMARY KEY (routing_hash, key)) WITHOUT ROWID",
		);
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

	/** Evicts this instance, as a crash does. The storage stays, and an interrupted alarm runs again. */
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
		};
	}

	/** The root keeps the count of the writes that the Worker saw succeed. Reconciliation compares the rows with it. */
	async recordAcknowledged(writes: number): Promise<void> {
		this.writeMeta("acknowledgedWrites", this.readMeta("acknowledgedWrites") + writes);
	}

	async getAcknowledged(): Promise<number> {
		return this.readMeta("acknowledgedWrites");
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
					const routingHash = this.fokos.routingHashForKey(req.hashKey);
					const current =
						sql.exec<{ val: number }>("SELECT val FROM counters WHERE routing_hash = ? AND key = ?", routingHash, key).toArray()[0]?.val ??
						0;
					const val = current + req.amount;
					sql.exec("INSERT OR REPLACE INTO counters (routing_hash, key, val) VALUES (?, ?, ?)", routingHash, key, val);
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
				// The read seeks to `interval`, the routingHash interval of the target, and stops at its end.
				// `belongsToTarget` still filters each row: it removes the keys that a range tree owns.
				buildPage: (cursor, _slice, belongsToTarget, _budget, interval) => {
					// This host splits by hash only, so each edge of an interval is a whole routingHash.
					if (interval.start.hk !== undefined || interval.end?.hk !== undefined) {
						throw new Error("the counter host splits by hash only");
					}
					const after = cursor as CounterCursor | null;
					const params: unknown[] = [PAGE_ROWS + 1];
					let where: string;
					if (after) {
						where = "(routing_hash, key) > (?2, ?3)";
						params.push(after.routingHash, after.key);
					} else {
						where = "routing_hash >= ?2";
						params.push(interval.start.routingHash);
					}
					if (interval.end) {
						where += ` AND routing_hash < ?${params.length + 1}`;
						params.push(interval.end.routingHash);
					}
					const rows = sql
						.exec<{ routing_hash: ArrayBuffer; key: string; val: number }>(
							`SELECT routing_hash, key, val FROM counters WHERE ${where} ORDER BY routing_hash, key LIMIT ?1`,
							...params,
						)
						.toArray()
						.map((r): MigratedCounterRow => ({ routingHash: new Uint8Array(r.routing_hash), key: r.key, val: r.val }));
					const page = rows.slice(0, PAGE_ROWS);
					const last = page[PAGE_ROWS - 1];
					return {
						page: page.filter((r) => belongsToTarget({ hashKey: KeyCodec.encode(r.key), sortKey: NO_SORT_KEY })),
						nextCursor: rows.length > PAGE_ROWS ? ({ routingHash: last.routingHash, key: last.key } satisfies CounterCursor) : null,
					};
				},
				applyPage: (page) => {
					for (const r of page as MigratedCounterRow[]) {
						sql.exec("INSERT OR REPLACE INTO counters (routing_hash, key, val) VALUES (?, ?, ?)", r.routingHash, r.key, r.val);
					}
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
