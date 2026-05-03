import { DurableObject, RpcTarget } from "cloudflare:workers";
import { SQLSchemaMigration, SQLSchemaMigrations } from "durable-utils/sql-migrations";
import { GetItemOptions, GetItemResult, PutItemOptions, PutItemResult } from "./types.js";
import { PartitionContext, PartitionTopologyImpl, PartitionTopologySplitter, SplitStatusKVItem } from "./partition-topology.js";

export interface PartitionAPI {
	putItem(ctx: PartitionContext, opts: PutItemOptions): Promise<PutItemResult>;
	getItem(ctx: PartitionContext, opts: GetItemOptions): Promise<GetItemResult>;
}

export class PartitionRpcTarget extends RpcTarget {
	constructor(
		private readonly partitionDO: PartitionDO,
		private readonly ctx: DurableObjectState,
		private readonly storage: DurableObjectStorage,
		private readonly partitionCtx: PartitionContext,
	) {
		super();
	}

	async putItem(opts: PutItemOptions): Promise<PutItemResult> {
		return await this.partitionDO.putItem(this.partitionCtx, opts);
	}
	async getItem(opts: GetItemOptions): Promise<GetItemResult> {
		return await this.partitionDO.getItem(this.partitionCtx, opts);
	}
}

export class PartitionDO extends DurableObject implements PartitionAPI {
	private static readonly KV_KEYS = {
		PARTITION_CONTEXT: "__partition_context",
	};

	#_migrations: SQLSchemaMigrations;
	#_partitionContext?: PartitionContext;
	#_topology?: PartitionTopologySplitter;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.#_migrations = new SQLSchemaMigrations({
			migrations: sqlMigrations,
			doStorage: ctx.storage,
		});
		void ctx.blockConcurrencyWhile(async () => {
			await this.#_migrations.runAll();

			// Load partition context from storage.
			const storedContext = ctx.storage.kv.get<PartitionContext>(PartitionDO.KV_KEYS.PARTITION_CONTEXT);
			if (storedContext) {
				this.#_partitionContext = storedContext;
			}
		});
	}

	async putItem(ctx: PartitionContext, opts: PutItemOptions): Promise<PutItemResult> {
		this.ensurePartitionContext(ctx);

		const { rowsRead, rowsWritten } = this.ctx.storage.sql.exec(
			`INSERT OR REPLACE INTO items (hk, sk, data, ttl_epoch_utc_seconds) VALUES (?, ?, ?, ?)`,
			opts.hashKey,
			opts.sortKey ?? null,
			opts.data,
			opts.ttlEpochUTCSeconds ?? null,
		);

		const splitStatus = await this.checkSplits(ctx, opts.hashKey, opts.sortKey);

		return { meta: { rowsRead, rowsWritten, databaseSize: this.ctx.storage.sql.databaseSize }, __debug: { splitStatus } };
	}

	async getItem(ctx: PartitionContext, opts: GetItemOptions): Promise<GetItemResult> {
		this.ensurePartitionContext(ctx);

		const res = this.ctx.storage.sql.exec<{
			data: string | ArrayBuffer;
			ttl_epoch_utc_seconds: number | null;
		}>(`SELECT data, ttl_epoch_utc_seconds FROM items WHERE hk = ? AND sk = ?`, opts.hashKey, opts.sortKey ?? null);
		const result = res.one();
		const { rowsRead, rowsWritten } = res;
		if (!result) {
			return { found: false };
		}
		return {
			found: true,
			hashKey: opts.hashKey,
			sortKey: opts.sortKey,
			data: typeof result.data === "string" ? result.data : new Uint8Array(result.data),
			ttlEpochUTCSeconds: result.ttl_epoch_utc_seconds ? Number(result.ttl_epoch_utc_seconds) : undefined,

			meta: { rowsRead, rowsWritten, databaseSize: this.ctx.storage.sql.databaseSize },
		};
	}

	async status() {
		const splitStatus = this.ensureTopology(this.pCtx()).splitStatus();
		return {
			splitStatus,
		};
	}

	async alarm(alarmInfo: AlarmInvocationInfo): Promise<void> {
		const topologyRouter = this.ensureTopology(this.pCtx());
		const splitStatus = topologyRouter.splitStatus();
		console.log({
			...this.logParams(),
			message: "fokos/partition-do: Alarm triggered for split process.",
			alarmInfo,
			splitStatus,
		});
	}

	private async checkSplits(pCtx: PartitionContext, hashKey: string, sortKey?: string): Promise<SplitStatusKVItem | undefined> {
		const topologyRouter = this.ensureTopology(pCtx);
		const shouldSplit = topologyRouter.shouldSplit(hashKey, sortKey);
		if (shouldSplit) {
			console.log({
				...this.logParams(),
				message: "fokos/partition-do: Split conditions met, initiating split process.",
			});
			await topologyRouter.queueSplit();
		}

		return topologyRouter.splitStatus();
	}

	private pCtx(): PartitionContext {
		if (!this.#_partitionContext) {
			throw new Error("Partition context not initialized");
		}
		return this.#_partitionContext;
	}

	private ensurePartitionContext(pCtx: PartitionContext) {
		if (this.#_partitionContext) {
			// We need to check if the provided context matches the stored one to avoid inconsistencies.
			// In a real implementation, we might want to allow some flexibility here (e.g. for certain fields)
			// or have a more robust way to handle context updates.
			if (this.#_partitionContext.version !== pCtx.version) {
				throw new Error(
					`Provided partition context does not match the stored context: ${this.#_partitionContext.version} vs ${pCtx.version}`,
				);
			}
			return this.#_partitionContext;
		}
		this.#_partitionContext = pCtx;
		this.ctx.storage.kv.put<PartitionContext>(PartitionDO.KV_KEYS.PARTITION_CONTEXT, pCtx);
		return pCtx;
	}

	private ensureTopology(pCtx: PartitionContext): PartitionTopologySplitter {
		if (!this.#_topology) {
			// TODO Load the topology configuration from storage or env variables instead of hardcoding it here.
			// We can also consider having a separate DO to manage the topology and have the partition DOs fetch the configuration from it.
			this.#_topology = new PartitionTopologyImpl("", pCtx, this.ctx);
		}
		return this.#_topology;
	}

	private logParams() {
		return {
			actorId: this.ctx.id.toString(),
			actorName: this.ctx.id.name,
			partitionContext: this.pCtx(),
		};
	}
}

const sqlMigrations: SQLSchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: "Create items table",
		// The type of `data` is ANY to allow SQLite to retain the input type (e.g. BLOB vs TEXT) and avoid unnecessary conversions.
		// The Durable Object API only accepts strings and Uint8Arrays, so we can safely store them as-is and retrieve them with the correct type.
		sql: `
            CREATE TABLE IF NOT EXISTS items (
                hk TEXT NOT NULL,
                sk TEXT,
                data ANY NOT NULL,
                ttl_epoch_utc_seconds INTEGER,
                PRIMARY KEY (hk, sk)
            ) WITHOUT ROWID, STRICT;`,
	},
];
