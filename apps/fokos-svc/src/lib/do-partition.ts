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

// Minimal structural type used in withSplitForwarding to avoid a recursive type cycle:
// DurableObjectStub<PartitionDO> → PartitionDO → withSplitForwarding → DurableObjectStub<PartitionDO>.
type PartitionDOStub = {
	putItem(ctx: PartitionContextResolved, opts: PutItemOptions): Promise<PutItemResult>;
	getItem(ctx: PartitionContextResolved, opts: GetItemOptions): Promise<GetItemResult>;
};

type MigratedItem = {
	hk: string;
	sk: string | null;
	data: string | Uint8Array;
	ttl_epoch_utc_seconds: number | null;
	v: number;
};

type MigrationCursor = { hk: string; sk: string | null };

type GetItemsBatchResult = {
	items: MigratedItem[];
	nextCursor: MigrationCursor | null;
};

// Structural type for child→parent RPC calls during migration.
type ParentPartitionDOStub = {
	getItemsBatch(opts: { childPartitionContext: PartitionContextResolved; cursor: MigrationCursor | null }): Promise<GetItemsBatchResult>;
	acknowledgeChildMigrationComplete(childDoName: string): Promise<void>;
};

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

type PartitionSplitMigrationStatus = "migration_initialized" | "migration_migrating" | "migration_completed";

export class PartitionDO extends DurableObject implements PartitionAPI {
	private static readonly KV_KEYS = {
		PARTITION_CONTEXT: "__partition_context",
		PARENT_PARTITION_CONTEXT: "__parent_partition_context",
		PARENT_SPLIT_TYPE: "__parent_split_type",
		SPLIT_MIGRATION_STATUS: "__split_migration_status",
		SPLIT_MIGRATION_CURSOR: "__split_migration_cursor",
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
				storedContext._partitionIdBytes = Uint8Array.fromHex(storedContext.partitionId);
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
		this.ctx.storage.kv.put<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_initialized");
	}

	async putItem(ctx: PartitionContextResolved, opts: PutItemOptions): Promise<PutItemResult> {
		this.ensurePartitionContext(ctx);
		await this.ensureMigration();
		return await this.withSplitForwarding<PutItemResult>({
			ctx,
			keys: { hashKey: opts.hashKey, sortKey: opts.sortKey },
			operationName: "putItem",
			forward: async (stub, pCtx) => await stub.putItem(pCtx, opts),
			local: async () => {
				const res = this.ctx.storage.sql.exec<{ v: number }>(
					`INSERT INTO items (hk, sk, data, ttl_epoch_utc_seconds, v)
					 VALUES (?, ?, ?, ?, 1)
					 ON CONFLICT(hk, sk) DO UPDATE SET
					   data = excluded.data,
					   ttl_epoch_utc_seconds = excluded.ttl_epoch_utc_seconds,
					   v = v + 1
					 RETURNING v`,
					opts.hashKey,
					opts.sortKey ?? null,
					opts.data,
					opts.ttlEpochUTCSeconds ?? null,
				);
				const { rowsRead, rowsWritten } = res;
				const rows = res.toArray();
				if (rows.length !== 1) {
					throw new Error(`fokos/partition: putItem: RETURNING expected 1 row, got ${rows.length}`);
				}
				const version = rows[0].v;
				if (typeof version !== "number" || !Number.isInteger(version) || version < 1) {
					throw new Error(`fokos/partition: putItem: unexpected version value: ${version}`);
				}
				const splitStatus = await this.checkSplits(ctx, opts.hashKey, opts.sortKey);
				return {
					version,
					meta: {
						rowsRead,
						rowsWritten,
						databaseSize: this.ctx.storage.sql.databaseSize,
						servedByInstance: this.ctx.id.toString(),
					},
					__debug: { splitStatus },
				};
			},
		});
	}

	async getItem(ctx: PartitionContextResolved, opts: GetItemOptions): Promise<GetItemResult> {
		this.ensurePartitionContext(ctx);
		await this.ensureMigration();
		return await this.withSplitForwarding<GetItemResult>({
			ctx,
			keys: { hashKey: opts.hashKey, sortKey: opts.sortKey },
			operationName: "getItem",
			forward: async (stub, pCtx) => await stub.getItem(pCtx, opts),
			local: async () => {
				const res = this.ctx.storage.sql.exec<{
					data: string | ArrayBuffer;
					ttl_epoch_utc_seconds: number | null;
					v: number;
				}>(`SELECT data, ttl_epoch_utc_seconds, v FROM items WHERE hk = ? AND sk = ? LIMIT 1`, opts.hashKey, opts.sortKey ?? null);
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
					version: result.v,
					meta: { rowsRead, rowsWritten, databaseSize: this.ctx.storage.sql.databaseSize, servedByInstance: this.ctx.id.toString() },
				};
			},
		});
	}

	async getItemsBatch(opts: {
		childPartitionContext: PartitionContextResolved;
		cursor: MigrationCursor | null;
	}): Promise<GetItemsBatchResult> {
		const splitStatus = this.ensureTopology(this.pCtx()).splitStatus();
		if (splitStatus?.status !== "split_started") {
			throw new Error(`fokos/partition: getItemsBatch: not in split_started status.`);
		}
		const isKnownChild = splitStatus.childPartitionContexts.some((c) => c.doName === opts.childPartitionContext.doName);
		if (!isKnownChild) {
			throw new Error(`fokos/partition: getItemsBatch: unknown child partition "${opts.childPartitionContext.doName}".`);
		}

		// Workers RPC has a 32MB limit, and each DO is 128MB memory, so we try to be lean around 20MB here.
		const BATCH_LIMIT_BYTES = 20 * 1024 * 1024;
		const PAGE_SIZE = 1000;
		const items: MigratedItem[] = [];
		let totalBytes = 0;
		let tableCursor = opts.cursor;
		let reachedLimit = false;
		const pCtx = this.pCtx();
		const topology = this.ensureTopology(pCtx);

		const isCorrectHashChildPartition = topology.makeIsCorrectChildHashPartition(pCtx, opts.childPartitionContext);

		while (true) {
			const page = this.queryPage(tableCursor, PAGE_SIZE);
			if (page.length === 0) break;

			for (const row of page) {
				// Filter: only send items that belong to the requesting child partition.
				if (isCorrectHashChildPartition(row.hk, row.sk ?? undefined)) {
					const rowBytes = estimateItemBytes(row);
					if (items.length > 0 && totalBytes + rowBytes > BATCH_LIMIT_BYTES) {
						reachedLimit = true;
						break;
					}
					items.push(row);
					totalBytes += rowBytes;
				}
				// Always advance the table cursor regardless of whether the row matched.
				tableCursor = { hk: row.hk, sk: row.sk };
			}
			if (reachedLimit) break;

			if (page.length < PAGE_SIZE) break;
		}

		return { items, nextCursor: reachedLimit ? tableCursor : null };
	}

	async acknowledgeChildMigrationComplete(childDoName: string): Promise<void> {
		this.ensureTopology(this.pCtx()).acknowledgeChildMigration(childDoName);
	}

	async status() {
		const splitStatus = this.ensureTopology(this.pCtx()).splitStatus();
		return {
			splitStatus,
		};
	}

	async __internalState() {
		const partitionContext = this.ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARTITION_CONTEXT);
		return {
			partitionContext,
			parentPartitionContext: this.ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARENT_PARTITION_CONTEXT),
			parentSplitType: this.ctx.storage.kv.get<SplitType>(PartitionDO.KV_KEYS.PARENT_SPLIT_TYPE),
			migrationStatus: this.ctx.storage.kv.get<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS),
			splitStatus: partitionContext ? this.ensureTopology(partitionContext).splitStatus() : undefined,
		};
	}

	async alarm(alarmInfo: AlarmInvocationInfo): Promise<void> {
		const topologyRouter = this.ensureTopology(this.pCtx());
		const splitStatus = topologyRouter.splitStatus();
		const migrationStatus = this.ctx.storage.kv.get<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS);
		console.log({
			...this.logParams(),
			message: "fokos/partition: Alarm triggered.",
			alarmInfo,
			migrationStatus,
			splitStatus,
		});

		try {
			this.__testing__alarm_running = true;

			if (migrationStatus === "migration_initialized" || migrationStatus === "migration_migrating") {
				if (migrationStatus === "migration_initialized") {
					this.ctx.storage.kv.put<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_migrating");
				}
				await tryWhile(
					async () => {
						await this.runMigration();
					},
					(_error, nextAttempt) => nextAttempt <= 5,
				);
				return;
			}

			// FIXME Add a special flag in KV for this too, to allow future alarm uses too.
			console.log({
				...this.logParams(),
				message: "fokos/partition: Running split process.",
				splitStatus,
			});
			await tryWhile(
				async () => {
					await topologyRouter.startSplit();
				},
				(_error, nextAttempt) => nextAttempt <= 5,
			);
		} catch (error) {
			console.error({
				...this.logParams(),
				message: "fokos/partition: Alarm process failed, will retry on the next request.",
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
		pCtx._partitionIdBytes = Uint8Array.fromHex(pCtx.partitionId);
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

	private async ensureMigration(): Promise<void> {
		// TODO Optimize this away by keeping it in memory.
		const migrationStatus = this.ctx.storage.kv.get<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS);
		if (!migrationStatus || migrationStatus === "migration_completed") {
			return;
		}
		if (migrationStatus === "migration_initialized") {
			this.ctx.storage.kv.put<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_migrating");
		}
		// Ensure the background alarm is running in case it crashed or hasn't fired yet.
		if (!(await this.ctx.storage.getAlarm())) {
			await this.ctx.storage.setAlarm(Date.now());
		}
		// TODO This will reach user requests, so refactor the callers to show something nicer.
		// We can also consider doing a selective migration of the requested keys only.
		throw new Error("fokos/partition: Partition split in progress, please retry later.");
	}

	private async withSplitForwarding<T>(opts: {
		ctx: PartitionContextResolved;
		keys: { hashKey: string; sortKey?: string };
		operationName: string;
		forward: (stub: PartitionDOStub, pCtx: PartitionContextResolved) => Promise<T>;
		local: () => Promise<T>;
	}): Promise<T> {
		const {
			ctx,
			keys: { hashKey, sortKey },
			operationName,
			forward,
			local,
		} = opts;
		const topology = this.ensureTopology(ctx);
		switch (topology.shouldAllow(hashKey, sortKey)) {
			case "ok":
				return await local();
			case "forward": {
				const { doId, partitionContext } = topology.pickChildPartition(ctx, hashKey, sortKey);
				const stub = this.env[this.pCtx().ns].get(doId);
				return await forward(stub, partitionContext);
			}
			case "reject":
				throw new Error(`fokos/partition: partition exceeded its limits, please retry later (${operationName}).`);
		}
	}

	private queryPage(cursor: MigrationCursor | null, limit: number): MigratedItem[] {
		type Row = { hk: string; sk: string | null; data: string | ArrayBuffer; ttl_epoch_utc_seconds: number | null; v: number };

		let sqlCursor: SqlStorageCursor<Row>;
		if (!cursor) {
			sqlCursor = this.ctx.storage.sql.exec<Row>(`SELECT hk, sk, data, ttl_epoch_utc_seconds, v FROM items ORDER BY hk, sk LIMIT ?`, limit);
		} else if (cursor.sk === null) {
			sqlCursor = this.ctx.storage.sql.exec<Row>(
				`SELECT hk, sk, data, ttl_epoch_utc_seconds, v FROM items WHERE hk > ? OR (hk = ? AND sk IS NOT NULL) ORDER BY hk, sk LIMIT ?`,
				cursor.hk,
				cursor.hk,
				limit,
			);
		} else {
			sqlCursor = this.ctx.storage.sql.exec<Row>(
				`SELECT hk, sk, data, ttl_epoch_utc_seconds, v FROM items WHERE hk > ? OR (hk = ? AND sk > ?) ORDER BY hk, sk LIMIT ?`,
				cursor.hk,
				cursor.hk,
				cursor.sk,
				limit,
			);
		}

		const items: MigratedItem[] = [];
		for (const row of sqlCursor) {
			items.push({
				hk: row.hk,
				sk: row.sk,
				data: typeof row.data === "string" ? row.data : new Uint8Array(row.data),
				ttl_epoch_utc_seconds: row.ttl_epoch_utc_seconds,
				v: row.v,
			});
		}
		return items;
	}

	private async runMigration(): Promise<void> {
		const parentCtx = this.ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARENT_PARTITION_CONTEXT);
		if (!parentCtx) {
			throw new Error("fokos/partition: runMigration called but no parent partition context stored.");
		}

		const parentId = this.env[parentCtx.ns].idFromName(parentCtx.doName);
		const parentStub = this.env[parentCtx.ns].get(parentId) as unknown as ParentPartitionDOStub;

		let cursor = this.ctx.storage.kv.get<MigrationCursor>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_CURSOR) ?? null;

		const pCtx = this.pCtx();
		while (true) {
			const { items, nextCursor } = await parentStub.getItemsBatch({ childPartitionContext: pCtx, cursor });

			if (items.length > 0) {
				for (const item of items) {
					// INSERT OR IGNORE rather than OR REPLACE: all writes to this partition are rejected
					// with 503 while migration_migrating, so no user write can have arrived yet.
					// IGNORE is safer for retries — if a batch was already written before a crash we
					// skip re-inserting those items rather than overwriting them unnecessarily.
					this.ctx.storage.sql.exec(
						`INSERT OR IGNORE INTO items (hk, sk, data, ttl_epoch_utc_seconds, v) VALUES (?, ?, ?, ?, ?)`,
						item.hk,
						item.sk ?? null,
						item.data,
						item.ttl_epoch_utc_seconds ?? null,
						item.v,
					);
				}
			}

			// Checkpoint cursor after each batch so we can resume if interrupted.
			cursor = nextCursor;
			this.ctx.storage.kv.put<MigrationCursor | null>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_CURSOR, cursor);

			if (!nextCursor) break;
		}

		this.ctx.storage.kv.put<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_completed");
		this.ctx.storage.kv.delete(PartitionDO.KV_KEYS.SPLIT_MIGRATION_CURSOR);
		await parentStub.acknowledgeChildMigrationComplete(pCtx.doName);

		console.log({ ...this.logParams(), message: "fokos/partition: Data migration from parent completed." });
	}

	private logParams() {
		return {
			actorId: this.ctx.id.toString(),
			// This might truncated to 1024 bytes in Cloudflare Workers, but the full one should be inside partitionContext.doName.
			actorName: this.ctx.id.name,
			// Always put the raw partition context in the logs for better debugging, even if it's undefined.
			partitionContext: this.#_partitionContext ?? "",
		};
	}
}

function estimateItemBytes(item: MigratedItem): number {
	const dataSize = typeof item.data === "string" ? item.data.length * 2 : item.data.byteLength;
	return item.hk.length * 2 + (item.sk?.length ?? 0) * 2 + dataSize + 8 + 64;
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
                v INTEGER NOT NULL,
                PRIMARY KEY (hk, sk)
            ) WITHOUT ROWID, STRICT;`,
	},
];
