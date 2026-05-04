import { DurableObject, RpcTarget } from "cloudflare:workers";
import { SQLSchemaMigration, SQLSchemaMigrations } from "durable-utils/sql-migrations";
import { GetItemOptions, GetItemResult, PutItemOptions, PutItemResult } from "./types.js";
import {
	PartitionContext,
	PartitionContextResolved,
	PartitionTopologyImpl,
	PartitionTopologySplitter,
	SplitStatusKVItem,
} from "./partition-topology/partition-topology.js";
import type { SplitType } from "./partition-topology/types.js";
import { tryWhile } from "durable-utils/retries";

export interface PartitionAPI {
	putItem(ctx: PartitionContext, opts: PutItemOptions): Promise<PutItemResult>;
	getItem(ctx: PartitionContext, opts: GetItemOptions): Promise<GetItemResult>;
}

export class PartitionRpcTarget extends RpcTarget {
	constructor(
		private readonly partitionDO: PartitionDO,
		private readonly ctx: DurableObjectState,
		private readonly storage: DurableObjectStorage,
		private readonly partitionCtx: PartitionContextResolved,
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

export type InitFromSplitOptions = {
	parentPartitionContext: PartitionContextResolved;
	newPartitionContext: PartitionContextResolved;
	splitType: SplitType;
};

export class PartitionDO extends DurableObject implements PartitionAPI {
	private static readonly KV_KEYS = {
		PARTITION_CONTEXT: "__partition_context",
		PARENT_PARTITION_CONTEXT: "__parent_partition_context",
		PARENT_SPLIT_TYPE: "__parent_split_type",
	};

	#_migrations: SQLSchemaMigrations;
	#_partitionContext?: PartitionContextResolved;
	#_topology?: PartitionTopologySplitter;

	// ONLY USED FOR TESTING! DO NOT DEPEND ON THIS FLAG FOR ANY LOGIC IN THE DO.
	__testing__alarm_running = false;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.#_migrations = new SQLSchemaMigrations({
			migrations: sqlMigrations,
			doStorage: ctx.storage,
		});
		void ctx.blockConcurrencyWhile(async () => {
			await this.#_migrations.runAll();

			// Load partition context from storage.
			const storedContext = ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARTITION_CONTEXT);
			if (storedContext) {
				this.#_partitionContext = storedContext;
			}
		});
	}

	/**
	 * Only called from the parent partition during the split process to initialize the new child partition
	 * with the right context and its parent partition info that it can use to get data during migration.
	 *
	 * This is not meant to be called directly by clients.
	 */
	async initFromSplit(opts: InitFromSplitOptions) {
		const { parentPartitionContext, newPartitionContext, splitType } = opts;

		if (this.#_partitionContext) {
			const storedParent = this.ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARENT_PARTITION_CONTEXT);
			const storedSplitType = this.ctx.storage.kv.get<SplitType>(PartitionDO.KV_KEYS.PARENT_SPLIT_TYPE);
			if (
				this.#_partitionContext.primaryDoIdStr !== newPartitionContext.primaryDoIdStr ||
				storedParent?.primaryDoIdStr !== parentPartitionContext.primaryDoIdStr ||
				storedSplitType !== splitType
			) {
				throw new Error(
					`fokos: initFromSplit called with conflicting options. ` +
						`child: ${this.#_partitionContext.primaryDoIdStr} vs ${newPartitionContext.primaryDoIdStr}, ` +
						`parent: ${storedParent?.primaryDoIdStr} vs ${parentPartitionContext.primaryDoIdStr}, ` +
						`splitType: ${storedSplitType} vs ${splitType}`,
				);
			}
			// All options match — idempotent retry, nothing to do.
			return;
		}

		this.ensurePartitionContext(newPartitionContext);
		this.ctx.storage.kv.put<PartitionContextResolved>(PartitionDO.KV_KEYS.PARENT_PARTITION_CONTEXT, parentPartitionContext);
		this.ctx.storage.kv.put<SplitType>(PartitionDO.KV_KEYS.PARENT_SPLIT_TYPE, splitType);

		// TODO Set an alarm to start migrating data from the parent partition to the new partition even though we are doing lazy migration.
	}

	async putItem(ctx: PartitionContextResolved, opts: PutItemOptions): Promise<PutItemResult> {
		this.ensurePartitionContext(ctx);

		const { rowsRead, rowsWritten } = this.ctx.storage.sql.exec(
			`INSERT OR REPLACE INTO items (hk, sk, data, ttl_epoch_utc_seconds) VALUES (?, ?, ?, ?)`,
			opts.hashKey,
			opts.sortKey ?? null,
			opts.data,
			opts.ttlEpochUTCSeconds ?? null,
		);

		const splitStatus = await this.checkSplits(ctx, opts.hashKey, opts.sortKey);

		return {
			meta: {
				rowsRead,
				rowsWritten,
				databaseSize: this.ctx.storage.sql.databaseSize,
				served_by_instance: this.ctx.id.toString(),
			},
			__debug: { splitStatus },
		};
	}

	async getItem(ctx: PartitionContextResolved, opts: GetItemOptions): Promise<GetItemResult> {
		this.ensurePartitionContext(ctx);

		const res = this.ctx.storage.sql.exec<{
			data: string | ArrayBuffer;
			ttl_epoch_utc_seconds: number | null;
		}>(`SELECT data, ttl_epoch_utc_seconds FROM items WHERE hk = ? AND sk = ? LIMIT 1`, opts.hashKey, opts.sortKey ?? null);
		const rows = res.toArray();
		const { rowsRead, rowsWritten } = res;
		const result = rows[0];
		if (!result) {
			return { found: false };
		}
		return {
			found: true,
			hashKey: opts.hashKey,
			sortKey: opts.sortKey,
			data: typeof result.data === "string" ? result.data : new Uint8Array(result.data),
			ttlEpochUTCSeconds: result.ttl_epoch_utc_seconds ? Number(result.ttl_epoch_utc_seconds) : undefined,

			meta: { rowsRead, rowsWritten, databaseSize: this.ctx.storage.sql.databaseSize, served_by_instance: this.ctx.id.toString() },
		};
	}

	async status() {
		const splitStatus = this.ensureTopology(this.pCtx()).splitStatus();
		return {
			splitStatus,
		};
	}

	async __internalState() {
		return {
			partitionContext: this.ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARTITION_CONTEXT),
			parentPartitionContext: this.ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARENT_PARTITION_CONTEXT),
			parentSplitType: this.ctx.storage.kv.get<SplitType>(PartitionDO.KV_KEYS.PARENT_SPLIT_TYPE),
			splitStatus: this.ensureTopology(this.pCtx()).splitStatus(),
		};
	}

	async alarm(alarmInfo: AlarmInvocationInfo): Promise<void> {
		const topologyRouter = this.ensureTopology(this.pCtx());
		const splitStatus = topologyRouter.splitStatus();
		console.log({
			...this.logParams(),
			message: "fokos/partition: Alarm triggered for partition split process.",
			alarmInfo,
			splitStatus,
		});

		// We catch the exception to control when it gets retried. We will retry on the next request.
		try {
			this.__testing__alarm_running = true;
			await tryWhile(
				async () => {
					await topologyRouter.startSplit();
				},
				(_error, nextAttempt) => {
					return nextAttempt <= 5; // Retry up to 5 times
				},
			);
		} catch (error) {
			console.error({
				...this.logParams(),
				message: "fokos/partition: Split process failed, will retry on the next request.",
				error: String(error),
				errorProps: error,
			});
		} finally {
			this.__testing__alarm_running = false;
		}
	}

	private async checkSplits(pCtx: PartitionContextResolved, hashKey: string, sortKey?: string): Promise<SplitStatusKVItem | undefined> {
		const topologyRouter = this.ensureTopology(pCtx);
		const splitStatus = await topologyRouter.maybeQueueSplit(hashKey, sortKey);
		if (splitStatus) {
			console.log({
				...this.logParams(),
				message: "fokos/partition: Split conditions met.",
				splitStatus,
			});
		}
		return splitStatus;
	}

	private pCtx(): PartitionContextResolved {
		if (!this.#_partitionContext) {
			throw new Error("Partition context not initialized");
		}
		return this.#_partitionContext;
	}

	private ensurePartitionContext(pCtx: PartitionContextResolved): PartitionContextResolved {
		if (this.#_partitionContext) {
			// We need to check if the provided context matches the stored one to avoid inconsistencies.
			// In a real implementation, we might want to allow some flexibility here (e.g. for certain fields)
			// or have a more robust way to handle context updates.
			if (this.#_partitionContext.signature !== pCtx.signature) {
				throw new Error(
					`Provided partition context does not match the stored context: ${this.#_partitionContext.signature} vs ${pCtx.signature}`,
				);
			}
			return this.#_partitionContext;
		}
		this.#_partitionContext = pCtx;
		this.ctx.storage.kv.put<PartitionContextResolved>(PartitionDO.KV_KEYS.PARTITION_CONTEXT, pCtx);
		return pCtx;
	}

	private ensureTopology(pCtx: PartitionContextResolved): PartitionTopologySplitter {
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
