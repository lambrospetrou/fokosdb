import { DurableObject, RpcTarget } from "cloudflare:workers";
import { SQLSchemaMigration, SQLSchemaMigrations } from "durable-utils/sql-migrations";
import {
	DeleteItemOptions,
	DeleteItemResult,
	GetItemOptions,
	GetItemResult,
	ItemCondition,
	PutItemOptions,
	PutItemResult,
} from "./types.js";
import type {
	CancelRequest,
	CancelResponse,
	CommitRequest,
	CommitResponse,
	PrepareRequest,
	PrepareResponse,
	ReadForTransactionItemResult,
	ReadForTransactionRequest,
	ReadForTransactionResponse,
	TransactionItem,
} from "./transaction-types.js";
import {
	PartitionContext,
	PartitionContextResolved,
	PartitionTopologyImpl,
	PartitionTopologySplitter,
	SplitStatusKVItem,
} from "./partition-topology/partition-topology.js";
import type { SplitType } from "./partition-topology/types.js";
import { tryWhile } from "durable-utils/retries";
import invariant from "./invariant.js";

type ItemSnapshot = { hk: string; sk: string; found: true; v: number } | { hk: string; sk: string; found: false };

function evaluateConditionsOnItem(item: ItemSnapshot, conditions: ItemCondition[], operationName: string): void {
	for (const condition of conditions) {
		if (condition.type === "item_exists") {
			if (!item.found) {
				throw new Error(`fokos/${operationName}: condition "item_exists" failed — item does not exist (hk=${item.hk}, sk=${item.sk})`);
			}
		} else if (condition.type === "item_not_exists") {
			if (item.found) {
				throw new Error(
					`fokos/${operationName}: condition "item_not_exists" failed — item already exists with v=${item.v} (hk=${item.hk}, sk=${item.sk})`,
				);
			}
		} else if (condition.type === "attribute_equals") {
			const actual = item.found ? item[condition.attribute] : null;
			if (actual !== condition.value) {
				throw new Error(
					`fokos/${operationName}: condition "attribute_equals" failed — attribute "${condition.attribute}" expected ${condition.value}, found ${actual} (hk=${item.hk}, sk=${item.sk})`,
				);
			}
		}
	}
}

function sumSqlMetrics(...results: Array<{ rowsRead: number; rowsWritten: number }>) {
	let rowsRead = 0;
	let rowsWritten = 0;
	for (const r of results) {
		rowsRead += r.rowsRead;
		rowsWritten += r.rowsWritten;
	}
	return { rowsRead, rowsWritten };
}

export interface PartitionAPI {
	putItem(ctx: PartitionContext, opts: PutItemOptions): Promise<PutItemResult>;
	getItem(ctx: PartitionContext, opts: GetItemOptions): Promise<GetItemResult>;
	deleteItem(ctx: PartitionContext, opts: DeleteItemOptions): Promise<DeleteItemResult>;
}

// Minimal structural type used in withSplitForwarding to avoid a recursive type cycle:
// DurableObjectStub<PartitionDO> → PartitionDO → withSplitForwarding → DurableObjectStub<PartitionDO>.
type PartitionDOStub = {
	putItem(ctx: PartitionContextResolved, opts: PutItemOptions): Promise<PutItemResult>;
	getItem(ctx: PartitionContextResolved, opts: GetItemOptions): Promise<GetItemResult>;
	deleteItem(ctx: PartitionContextResolved, opts: DeleteItemOptions): Promise<DeleteItemResult>;
	prepare(ctx: PartitionContextResolved, request: PrepareRequest): Promise<PrepareResponse>;
	commit(ctx: PartitionContextResolved, request: CommitRequest): Promise<CommitResponse>;
	cancel(ctx: PartitionContextResolved, request: CancelRequest): Promise<CancelResponse>;
	readForTransaction(ctx: PartitionContextResolved, request: ReadForTransactionRequest): Promise<ReadForTransactionResponse>;
};

type MigratedItem = {
	hk: string;
	sk: string;
	data: string | Uint8Array;
	ttl_epoch_utc_seconds: number | null;
	v: number;
	last_transaction_ts: number;
};

type PendingTransactionRow = {
	hk: string;
	sk: string;
	transaction_id: string;
	transaction_ts: number;
	operation: string;
	data: string | Uint8Array | null;
	conditions_json: string | null;
	coordinator_do_id: string;
	created_at: number;
};

type PendingTransactionCursor = { hk: string; sk: string; transaction_id: string };

type GetPartitionTransactionMetadataResult = {
	maxDeletedTs: number;
	pendingTransactions: PendingTransactionRow[];
	nextCursor: PendingTransactionCursor | null;
};

type MigrationCursor = { hk: string; sk: string };

type GetItemsBatchResult = {
	items: MigratedItem[];
	nextCursor: MigrationCursor | null;
};

// Structural type for child→parent RPC calls during migration.
type ParentPartitionDOStub = {
	getItemsBatch(opts: { childPartitionContext: PartitionContextResolved; cursor: MigrationCursor | null }): Promise<GetItemsBatchResult>;
	getPartitionTransactionMetadata(opts: {
		childPartitionContext: PartitionContextResolved;
		cursor: PendingTransactionCursor | null;
	}): Promise<GetPartitionTransactionMetadataResult>;
	acknowledgeChildMigrationComplete(childDoName: string): Promise<void>;
	getItemDirect(opts: GetItemOptions): Promise<GetItemResult>;
};

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

	// ONLY USED FOR TESTING! DO NOT DEPEND ON THESE FIELDS FOR ANY LOGIC IN THE DO.
	__testing__alarm_running = false;
	__testing__migrationBatchLimitBytes?: number;
	__testing__beforeMigrationComplete?: () => Promise<void>;

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

	async triggerMigration(): Promise<void> {
		await this.ensureMigration("triggerMigration", false);
	}

	async putItem(pCtx: PartitionContextResolved, opts: PutItemOptions): Promise<PutItemResult> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("putItem");
		return await this.withSplitForwarding<PutItemResult>({
			ctx: pCtx,
			keys: { hashKey: opts.hashKey, sortKey: opts.sortKey },
			operationName: "putItem",
			forward: async (stub, pCtx) => await stub.putItem(pCtx, opts),
			local: async () => {
				const sk = opts.sortKey ?? "";

				const pendingRow = this.ctx.storage.sql
					.exec<{
						transaction_id: string;
					}>(`SELECT transaction_id FROM pending_transactions WHERE hk = ? AND sk = ? LIMIT 1`, opts.hashKey, sk)
					.toArray()[0];
				if (pendingRow) {
					// FIXME: ATC §4 describes optimizations where a non-tx write can proceed using a
					// higher timestamp to force the pending tx to abort on commit, avoiding this rejection.
					throw new Error(
						`fokos/putItem: item is locked by an in-progress transaction (transactionId=${pendingRow.transaction_id}), retry later.`,
					);
				}

				let conditionRes: { rowsRead: number; rowsWritten: number } | null = null;
				if (opts.conditions && opts.conditions.length > 0) {
					const cRes = this.ctx.storage.sql.exec<{ v: number }>(`SELECT v FROM items WHERE hk = ? AND sk = ? LIMIT 1`, opts.hashKey, sk);
					const row = cRes.toArray()[0];
					conditionRes = cRes;
					const item: ItemSnapshot = row ? { found: true, hk: opts.hashKey, sk, v: row.v } : { found: false, hk: opts.hashKey, sk };
					evaluateConditionsOnItem(item, opts.conditions, "putItem");
				}

				const writeRes = this.ctx.storage.sql.exec<{ v: number }>(
					`INSERT INTO items (hk, sk, data, ttl_epoch_utc_seconds, v, last_transaction_ts)
					 VALUES (?, ?, ?, ?, 1, ?)
					 ON CONFLICT(hk, sk) DO UPDATE SET
					   data = excluded.data,
					   ttl_epoch_utc_seconds = excluded.ttl_epoch_utc_seconds,
					   v = v + 1,
					   last_transaction_ts = excluded.last_transaction_ts
					 RETURNING v`,
					opts.hashKey,
					sk,
					opts.data,
					opts.ttlEpochUTCSeconds ?? null,
					Date.now(),
				);
				const { rowsRead, rowsWritten } = conditionRes ? sumSqlMetrics(conditionRes, writeRes) : writeRes;
				const rows = writeRes.toArray();
				invariant(rows.length === 1, `fokos/partition.putItem: RETURNING expected 1 row, got ${rows.length}`);
				const version = rows[0].v;
				invariant(
					typeof version === "number" && Number.isInteger(version) && version >= 1,
					`fokos/partition.putItem: unexpected version value: ${version}`,
				);
				await this.checkSplits(pCtx, opts.hashKey, opts.sortKey);
				return {
					item: { hashKey: opts.hashKey, sortKey: opts.sortKey },
					version,
					meta: {
						rowsRead,
						rowsWritten,
						databaseSize: this.ctx.storage.sql.databaseSize,
						servedByActorId: this.ctx.id.toString(),
						servedByActorName: pCtx.doName,
						forwardCount: 0,
					},
				};
			},
		});
	}

	async deleteItem(pCtx: PartitionContextResolved, opts: DeleteItemOptions): Promise<DeleteItemResult> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("deleteItem");
		return await this.withSplitForwarding<DeleteItemResult>({
			ctx: pCtx,
			keys: { hashKey: opts.hashKey, sortKey: opts.sortKey },
			operationName: "deleteItem",
			forward: async (stub, pCtx) => await stub.deleteItem(pCtx, opts),
			local: async () => {
				const sk = opts.sortKey ?? "";

				const pendingRow = this.ctx.storage.sql
					.exec<{
						transaction_id: string;
					}>(`SELECT transaction_id FROM pending_transactions WHERE hk = ? AND sk = ? LIMIT 1`, opts.hashKey, sk)
					.toArray()[0];
				if (pendingRow) {
					// FIXME: ATC §4 optimization — see same comment in putItem.
					throw new Error(
						`fokos/deleteItem: item is locked by an in-progress transaction (transactionId=${pendingRow.transaction_id}), retry later.`,
					);
				}

				let conditionRes: { rowsRead: number; rowsWritten: number } | null = null;
				if (opts.conditions && opts.conditions.length > 0) {
					const cRes = this.ctx.storage.sql.exec<{ v: number }>(`SELECT v FROM items WHERE hk = ? AND sk = ? LIMIT 1`, opts.hashKey, sk);
					const row = cRes.toArray()[0];
					conditionRes = cRes;
					const item: ItemSnapshot = row ? { found: true, hk: opts.hashKey, sk, v: row.v } : { found: false, hk: opts.hashKey, sk };
					evaluateConditionsOnItem(item, opts.conditions, "deleteItem");
				}

				const writeRes = this.ctx.storage.sql.exec(`DELETE FROM items WHERE hk = ? AND sk = ?`, opts.hashKey, sk);
				const { rowsRead, rowsWritten } = conditionRes ? sumSqlMetrics(conditionRes, writeRes) : writeRes;
				return {
					item: { hashKey: opts.hashKey, sortKey: opts.sortKey },
					deleted: writeRes.rowsWritten > 0,
					meta: {
						rowsRead,
						rowsWritten,
						databaseSize: this.ctx.storage.sql.databaseSize,
						servedByActorId: this.ctx.id.toString(),
						servedByActorName: pCtx.doName,
						forwardCount: 0,
					},
				};
			},
		});
	}

	async getItem(pCtx: PartitionContextResolved, opts: GetItemOptions): Promise<GetItemResult> {
		this.ensurePartitionContext(pCtx);

		if (await this.ensureMigration("getItem", false)) {
			// Read directly from parent while this child is still migrating its share of the data.
			const parentCtx = this.ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARENT_PARTITION_CONTEXT);
			invariant(parentCtx, "fokos/partition.getItem: no parent partition context stored during migration");
			const parentId = this.env[parentCtx.ns].idFromName(parentCtx.doName);
			const parentStub = this.env[parentCtx.ns].get(parentId);
			return await parentStub.getItemDirect(opts);
		}

		return await this.withSplitForwarding<GetItemResult>({
			ctx: pCtx,
			keys: { hashKey: opts.hashKey, sortKey: opts.sortKey },
			operationName: "getItem",
			forward: async (stub, pCtx) => await stub.getItem(pCtx, opts),
			local: async () => await this.readItemLocally(pCtx, opts),
		});
	}

	// Internal RPC: reads directly from local storage, bypassing split forwarding.
	// Called by child partitions during migration to avoid a forwarding loop back into the child.
	async getItemDirect(opts: GetItemOptions): Promise<GetItemResult> {
		return await this.readItemLocally(this.pCtx(), opts);
	}

	async getItemsBatch(opts: {
		childPartitionContext: PartitionContextResolved;
		cursor: MigrationCursor | null;
	}): Promise<GetItemsBatchResult> {
		const splitStatus = this.ensureTopology(this.pCtx()).splitStatus();
		invariant(splitStatus?.status === "split_started", `fokos/partition.getItemsBatch: expected split_started, got ${splitStatus?.status}`);
		const isKnownChild = splitStatus.childPartitionContexts.some((c) => c.doName === opts.childPartitionContext.doName);
		invariant(isKnownChild, `fokos/partition.getItemsBatch: unknown child partition "${opts.childPartitionContext.doName}"`);

		// Workers RPC has a 32MB limit, and each DO is 128MB memory, so we try to be lean around 20MB here.
		const BATCH_LIMIT_BYTES = this.__testing__migrationBatchLimitBytes ?? 20 * 1024 * 1024;
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
				if (isCorrectHashChildPartition(row.hk, row.sk === "" ? undefined : row.sk)) {
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

	async getPartitionTransactionMetadata(opts: {
		childPartitionContext: PartitionContextResolved;
		cursor: PendingTransactionCursor | null;
	}): Promise<GetPartitionTransactionMetadataResult> {
		const pCtx = this.pCtx();
		const topology = this.ensureTopology(pCtx);
		const splitStatus = topology.splitStatus();
		invariant(
			splitStatus?.status === "split_started",
			`fokos/partition.getPartitionTransactionMetadata: expected split_started, got ${splitStatus?.status}`,
		);
		const isKnownChild = splitStatus.childPartitionContexts.some((c) => c.doName === opts.childPartitionContext.doName);
		invariant(
			isKnownChild,
			`fokos/partition.getPartitionTransactionMetadata: unknown child partition "${opts.childPartitionContext.doName}"`,
		);

		const isCorrectHashChildPartition = topology.makeIsCorrectChildHashPartition(pCtx, opts.childPartitionContext);

		const BATCH_LIMIT_BYTES = this.__testing__migrationBatchLimitBytes ?? 20 * 1024 * 1024;
		const PAGE_SIZE = 1000;
		const rows: PendingTransactionRow[] = [];
		let totalBytes = 0;
		let tableCursor = opts.cursor;
		let reachedLimit = false;

		const maxDeletedTs =
			this.ctx.storage.sql.exec<{ max_deleted_ts: number }>(`SELECT max_deleted_ts FROM deletion_metadata WHERE id = 1`).toArray()[0]
				?.max_deleted_ts ?? 0;

		while (true) {
			const page = this.queryPendingTxPage(tableCursor, PAGE_SIZE);
			if (page.length === 0) break;

			for (const row of page) {
				if (isCorrectHashChildPartition(row.hk, row.sk === "" ? undefined : row.sk)) {
					const rowBytes = estimatePendingTxBytes(row);
					if (rows.length > 0 && totalBytes + rowBytes > BATCH_LIMIT_BYTES) {
						reachedLimit = true;
						break;
					}
					rows.push(row);
					totalBytes += rowBytes;
				}
				tableCursor = { hk: row.hk, sk: row.sk, transaction_id: row.transaction_id };
			}
			if (reachedLimit) break;

			if (page.length < PAGE_SIZE) break;
		}

		return { maxDeletedTs, pendingTransactions: rows, nextCursor: reachedLimit ? tableCursor : null };
	}

	async acknowledgeChildMigrationComplete(childDoName: string): Promise<void> {
		this.ensureTopology(this.pCtx()).acknowledgeChildMigration(childDoName);
	}

	/**
	 * INTERNAL ONLY FOR TESTING.
	 */
	async status() {
		return {
			partitionContext: this.pCtx(),
			partitionContextStored: this.ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARTITION_CONTEXT),
			splitStatus: this.ensureTopology(this.pCtx()).splitStatus(),
			migrationStatus: this.ctx.storage.kv.get<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS),
			parentPartitionContext: this.ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARENT_PARTITION_CONTEXT),
			parentSplitType: this.ctx.storage.kv.get<SplitType>(PartitionDO.KV_KEYS.PARENT_SPLIT_TYPE),
		};
	}

	async prepare(pCtx: PartitionContextResolved, request: PrepareRequest): Promise<PrepareResponse> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("prepare");

		const { local, forwarded } = this.groupItemsByRouting(request.items);
		if (forwarded.size > 0) {
			invariant(local.length === 0, "fokos/partition.prepare: split routing must not mix local and forwarded items");
			for (const [, { pCtx: childPCtx, items }] of forwarded) {
				const r = await this.getChildStub(childPCtx).prepare(childPCtx, { ...request, items });
				if (r.outcome === "rejected") return r;
			}
			return { outcome: "accepted" };
		}

		return await this.prepareLocal(request);
	}

	private async prepareLocal(request: PrepareRequest): Promise<PrepareResponse> {
		const now = Date.now();

		if (request.transactionTimestamp > now + 5_000) {
			return {
				outcome: "rejected",
				reason: { type: "clock_skew", serverTimestampMs: now, transactionTimestampMs: request.transactionTimestamp },
			};
		}

		let response: PrepareResponse = this.ctx.storage.transactionSync<PrepareResponse>(() => {
			for (const item of request.items) {
				const sk = item.sortKey ?? "";

				const pendingRow = this.ctx.storage.sql
					.exec<{
						transaction_id: string;
					}>(`SELECT transaction_id FROM pending_transactions WHERE hk = ? AND sk = ? LIMIT 1`, item.hashKey, sk)
					.toArray()[0];

				if (pendingRow) {
					if (pendingRow.transaction_id === request.transactionId) {
						continue; // idempotent re-prepare for this item
					}
					return {
						outcome: "rejected",
						reason: {
							type: "pending_conflict",
							hashKey: item.hashKey,
							sortKey: item.sortKey,
							conflictingTransactionId: pendingRow.transaction_id,
						},
					};
				}

				const itemRow = this.ctx.storage.sql
					.exec<{
						last_transaction_ts: number;
						v: number;
					}>(`SELECT last_transaction_ts, v FROM items WHERE hk = ? AND sk = ? LIMIT 1`, item.hashKey, sk)
					.toArray()[0];

				if (item.conditions && item.conditions.length > 0) {
					const snapshot: ItemSnapshot = itemRow
						? { found: true, hk: item.hashKey, sk, v: itemRow.v }
						: { found: false, hk: item.hashKey, sk };
					try {
						evaluateConditionsOnItem(snapshot, item.conditions, "prepare");
					} catch {
						return {
							outcome: "rejected",
							reason: { type: "condition_failed", hashKey: item.hashKey, sortKey: item.sortKey },
						};
					}
				}

				if (itemRow) {
					if (request.transactionTimestamp <= itemRow.last_transaction_ts) {
						return {
							outcome: "rejected",
							reason: { type: "timestamp_conflict", hashKey: item.hashKey, sortKey: item.sortKey },
						};
					}
				} else if (item.operation === "put" || item.operation === "delete") {
					const metaRow = this.ctx.storage.sql
						.exec<{ max_deleted_ts: number }>(`SELECT max_deleted_ts FROM deletion_metadata WHERE id = 1`)
						.toArray()[0];
					if (request.transactionTimestamp <= (metaRow?.max_deleted_ts ?? 0)) {
						return {
							outcome: "rejected",
							reason: { type: "timestamp_conflict", hashKey: item.hashKey, sortKey: item.sortKey },
						};
					}
				}
			}

			// All checks passed — lock every item.
			for (const item of request.items) {
				const sk = item.sortKey ?? "";
				this.ctx.storage.sql.exec(
					`INSERT OR IGNORE INTO pending_transactions
						   (hk, sk, transaction_id, transaction_ts, operation, data, conditions_json, coordinator_do_id, created_at)
						 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					item.hashKey,
					sk,
					request.transactionId,
					request.transactionTimestamp,
					item.operation,
					item.data ?? null,
					item.conditions ? JSON.stringify(item.conditions) : null,
					request.coordinatorDoId,
					Date.now(),
				);
			}

			return { outcome: "accepted" };
		});

		if (response.outcome === "accepted" && !(await this.ctx.storage.getAlarm())) {
			await this.ctx.storage.setAlarm(Date.now() + 5_000);
		}

		return response;
	}

	async commit(pCtx: PartitionContextResolved, request: CommitRequest): Promise<CommitResponse> {
		this.ensurePartitionContext(pCtx);

		const { local, forwarded } = this.groupItemsByRouting(request.items);
		if (forwarded.size > 0) {
			invariant(local.length === 0, "fokos/partition.commit: split routing must not mix local and forwarded items");
			for (const [, { pCtx: childPCtx, items }] of forwarded) {
				await this.getChildStub(childPCtx).commit(childPCtx, { ...request, items });
			}
			this.ctx.storage.sql.exec(`DELETE FROM pending_transactions WHERE transaction_id = ?`, request.transactionId);
			return { outcome: "committed" };
		}

		return this.commitLocal(request);
	}

	private commitLocal(request: CommitRequest): CommitResponse {
		const pendingCount =
			this.ctx.storage.sql
				.exec<{ n: number }>(`SELECT COUNT(*) as n FROM pending_transactions WHERE transaction_id = ?`, request.transactionId)
				.toArray()[0]?.n ?? 0;

		if (pendingCount === 0) {
			return { outcome: "committed" };
		}

		this.ctx.storage.transactionSync(() => {
			const pendingRows = this.ctx.storage.sql
				.exec<{ hk: string; sk: string }>(`SELECT hk, sk FROM pending_transactions WHERE transaction_id = ?`, request.transactionId)
				.toArray();
			const pendingKeySet = new Set(pendingRows.map((r) => `${r.hk}\0${r.sk}`));
			const requestKeySet = new Set(request.items.map((i) => `${i.hashKey}\0${i.sortKey ?? ""}`));
			invariant(
				pendingKeySet.size === requestKeySet.size,
				`fokos/partition.commit: pending_transactions has ${pendingKeySet.size} items but request has ${requestKeySet.size} for transaction ${request.transactionId}`,
			);
			for (const key of requestKeySet) {
				invariant(
					pendingKeySet.has(key),
					`fokos/partition.commit: request item ${key} not found in pending_transactions for transaction ${request.transactionId}`,
				);
			}

			this.applyCommitItems(request.transactionId, request.transactionTimestamp, request.items);
			this.ctx.storage.sql.exec(`DELETE FROM pending_transactions WHERE transaction_id = ?`, request.transactionId);
		});

		return { outcome: "committed" };
	}

	private applyCommitItems(transactionId: string, transactionTimestamp: number, items: TransactionItem[]): void {
		for (const item of items) {
			const sk = item.sortKey ?? "";
			const pendingRow = this.ctx.storage.sql
				.exec<{
					operation: string;
					data: string | ArrayBuffer | null;
				}>(
					`SELECT operation, data FROM pending_transactions WHERE hk = ? AND sk = ? AND transaction_id = ? LIMIT 1`,
					item.hashKey,
					sk,
					transactionId,
				)
				.toArray()[0];

			if (!pendingRow) continue;

			if (pendingRow.operation === "put") {
				const data = typeof pendingRow.data === "string" ? pendingRow.data : pendingRow.data ? new Uint8Array(pendingRow.data) : null;
				this.ctx.storage.sql.exec(
					`INSERT INTO items (hk, sk, data, ttl_epoch_utc_seconds, v, last_transaction_ts)
					 VALUES (?, ?, ?, NULL, 1, ?)
					 ON CONFLICT(hk, sk) DO UPDATE SET
					   data = excluded.data,
					   ttl_epoch_utc_seconds = excluded.ttl_epoch_utc_seconds,
					   v = v + 1,
					   last_transaction_ts = excluded.last_transaction_ts`,
					item.hashKey,
					sk,
					data,
					transactionTimestamp,
				);
			} else if (pendingRow.operation === "delete") {
				this.ctx.storage.sql.exec(`DELETE FROM items WHERE hk = ? AND sk = ?`, item.hashKey, sk);
				this.ctx.storage.sql.exec(
					`UPDATE deletion_metadata SET max_deleted_ts = MAX(max_deleted_ts, ?) WHERE id = 1`,
					transactionTimestamp,
				);
			} else if (pendingRow.operation === "check") {
				this.ctx.storage.sql.exec(
					`UPDATE items SET last_transaction_ts = MAX(last_transaction_ts, ?) WHERE hk = ? AND sk = ?`,
					transactionTimestamp,
					item.hashKey,
					sk,
				);
			}
		}
	}

	async cancel(pCtx: PartitionContextResolved, request: CancelRequest): Promise<CancelResponse> {
		this.ensurePartitionContext(pCtx);
		this.ctx.storage.sql.exec(`DELETE FROM pending_transactions WHERE transaction_id = ?`, request.transactionId);

		const topology = this.ensureTopology(pCtx);
		const splitStatus = topology.splitStatus();
		if (splitStatus?.status === "split_started") {
			for (const childPCtx of splitStatus.childPartitionContexts) {
				try {
					await this.getChildStub(childPCtx).cancel(childPCtx, request);
				} catch (e) {
					console.error({
						...this.logParams(),
						message: "fokos/partition.cancel: failed to forward cancel to child",
						childDoName: childPCtx.doName,
						transactionId: request.transactionId,
						error: String(e),
					});
				}
			}
		}

		return { outcome: "cancelled" };
	}

	async readForTransaction(pCtx: PartitionContextResolved, request: ReadForTransactionRequest): Promise<ReadForTransactionResponse> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("readForTransaction");

		const { local, forwarded } = this.groupItemsByRouting(request.items);
		if (forwarded.size > 0) {
			invariant(local.length === 0, "fokos/partition.readForTransaction: split routing must not mix local and forwarded items");
			const allItems: ReadForTransactionItemResult[] = [];
			for (const [, { pCtx: childPCtx, items }] of forwarded) {
				const r = await this.getChildStub(childPCtx).readForTransaction(childPCtx, { ...request, items });
				allItems.push(...r.items);
			}
			return { items: allItems };
		}

		return this.readForTransactionLocal(request);
	}

	private readForTransactionLocal(request: ReadForTransactionRequest): ReadForTransactionResponse {
		const results: ReadForTransactionItemResult[] = [];

		for (const item of request.items) {
			const sk = item.sortKey ?? "";

			const itemRow = this.ctx.storage.sql
				.exec<{
					data: string | ArrayBuffer;
					last_transaction_ts: number;
					v: number;
				}>(`SELECT data, last_transaction_ts, v FROM items WHERE hk = ? AND sk = ? LIMIT 1`, item.hashKey, sk)
				.toArray()[0];

			const pendingRow = this.ctx.storage.sql
				.exec<{
					transaction_id: string;
				}>(`SELECT transaction_id FROM pending_transactions WHERE hk = ? AND sk = ? LIMIT 1`, item.hashKey, sk)
				.toArray()[0];

			const hasPendingWrite = pendingRow != null;
			const lastCommittedTs = itemRow?.last_transaction_ts ?? 0;

			if (itemRow) {
				results.push({
					found: true,
					hashKey: item.hashKey,
					sortKey: item.sortKey,
					data: typeof itemRow.data === "string" ? itemRow.data : new Uint8Array(itemRow.data),
					lastCommittedTs,
					hasPendingWrite,
				});
			} else {
				results.push({ found: false, hashKey: item.hashKey, sortKey: item.sortKey, lastCommittedTs, hasPendingWrite });
			}
		}

		return { items: results };
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
			} else if (splitStatus?.status === "split_queued") {
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
			} else {
				console.log({
					...this.logParams(),
					message: "fokos/partition: Alarm fired but no split is queued, nothing to do.",
					splitStatus,
				});
			}

			// Poke TCs for any stale pending transaction locks so they can drive recovery.
			const STALE_TX_MS = 5_000;
			const staleTxRows = this.ctx.storage.sql
				.exec<{ transaction_id: string; coordinator_do_id: string }>(
					`SELECT DISTINCT transaction_id, coordinator_do_id
                     FROM pending_transactions WHERE created_at < ? LIMIT 10`,
					Date.now() - STALE_TX_MS,
				)
				.toArray();
			for (const row of staleTxRows) {
				if (!row.coordinator_do_id) continue;
				try {
					const tcId = this.env.TRANSACTION_COORDINATOR_DO.idFromString(row.coordinator_do_id);
					await (
						this.env.TRANSACTION_COORDINATOR_DO.get(tcId) as unknown as { recoverTransaction(txId: string): Promise<void> }
					).recoverTransaction(row.transaction_id);
				} catch (e) {
					console.error({
						...this.logParams(),
						message: "fokos/partition: failed to poke stale TC",
						transactionId: row.transaction_id,
						error: String(e),
					});
				}
			}

			// Re-arm alarm if any pending transaction locks remain.
			const pendingCount = this.ctx.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) as n FROM pending_transactions`).toArray()[0]?.n ?? 0;
			if (pendingCount > 0 && !(await this.ctx.storage.getAlarm())) {
				await this.ctx.storage.setAlarm(Date.now() + 5_000);
			}
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
		invariant(this.#_partitionContext, "fokos/partition: Partition context not initialized");
		return this.#_partitionContext;
	}

	private ensurePartitionContext(pCtx: PartitionContextResolved): PartitionContextResolved {
		if (this.#_partitionContext) {
			// We need to check if the provided context matches the stored one to avoid inconsistencies.
			// In a real implementation, we might want to allow some flexibility here (e.g. for certain fields)
			// or have a more robust way to handle context updates.
			invariant(
				this.#_partitionContext.signature === pCtx.signature,
				`fokos/partition.ensurePartitionContext: context mismatch: ${this.#_partitionContext.signature} vs ${pCtx.signature}`,
			);
			return this.#_partitionContext;
		}
		invariant(pCtx.partitionId.length > 0, "fokos/partition.ensurePartitionContext: partitionId must not be empty");
		this.#_partitionContext = pCtx;
		this.ctx.storage.kv.put<PartitionContextResolved>(PartitionDO.KV_KEYS.PARTITION_CONTEXT, pCtx);
		pCtx._partitionIdBytes = Uint8Array.fromHex(pCtx.partitionId);
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

	private async ensureMigration(op: string, throwIfMigrating = true): Promise<boolean> {
		// TODO Optimize this away by keeping it in memory.
		const migrationStatus = this.ctx.storage.kv.get<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS);
		if (!migrationStatus || migrationStatus === "migration_completed") {
			return false;
		}
		if (migrationStatus === "migration_initialized") {
			this.ctx.storage.kv.put<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_migrating");
		}
		// Ensure the background alarm is running in case it crashed or hasn't fired yet.
		if (!(await this.ctx.storage.getAlarm())) {
			await this.ctx.storage.setAlarm(Date.now());
		}
		if (throwIfMigrating) {
			// TODO This will reach user requests, so refactor the callers to show something nicer.
			// We can also consider doing a selective migration of the requested keys only.
			throw new Error(`fokos/partition:${op}: Partition split in progress, please retry later.`);
		}
		return true;
	}

	private async withSplitForwarding<T extends { meta: { forwardCount: number } }>(opts: {
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
		const decision = topology.shouldAllow(hashKey, sortKey);
		switch (decision) {
			case "ok":
				return await local();
			case "forward": {
				const { doId, partitionContext } = topology.pickChildPartition(ctx, hashKey, sortKey);
				const stub = this.env[this.pCtx().ns].get(doId);
				const result = await forward(stub, partitionContext);
				return { ...result, meta: { ...result.meta, forwardCount: result.meta.forwardCount + 1 } } as T;
			}
			case "reject":
				throw new Error(`fokos/partition: partition exceeded its limits, please retry later (${operationName}).`);
			default: {
				const _exhaustive: never = decision;
				invariant(false, `fokos/partition.withSplitForwarding: unexpected decision value: ${_exhaustive}`);
			}
		}
	}

	private groupItemsByRouting<T extends { hashKey: string; sortKey?: string }>(
		items: T[],
	): { local: T[]; forwarded: Map<string, { pCtx: PartitionContextResolved; items: T[] }> } {
		const pCtx = this.pCtx();
		const topology = this.ensureTopology(pCtx);
		const local: T[] = [];
		const forwarded = new Map<string, { pCtx: PartitionContextResolved; items: T[] }>();

		for (const item of items) {
			const decision = topology.shouldAllow(item.hashKey, item.sortKey);
			if (decision === "ok") {
				local.push(item);
			} else if (decision === "forward") {
				const { partitionContext } = topology.pickChildPartition(pCtx, item.hashKey, item.sortKey);
				let entry = forwarded.get(partitionContext.doName);
				if (!entry) {
					entry = { pCtx: partitionContext, items: [] };
					forwarded.set(partitionContext.doName, entry);
				}
				entry.items.push(item);
			} else {
				throw new Error("fokos/partition: partition exceeded its limits during transaction forwarding");
			}
		}

		return { local, forwarded };
	}

	private getChildStub(childPCtx: PartitionContextResolved): PartitionDOStub {
		const childId = this.env[this.pCtx().ns].idFromName(childPCtx.doName);
		return this.env[this.pCtx().ns].get(childId);
	}

	private readItemLocally(pCtx: PartitionContextResolved, opts: GetItemOptions): GetItemResult {
		const res = this.ctx.storage.sql.exec<{
			data: string | ArrayBuffer;
			ttl_epoch_utc_seconds: number | null;
			v: number;
		}>(`SELECT data, ttl_epoch_utc_seconds, v FROM items WHERE hk = ? AND sk = ? LIMIT 1`, opts.hashKey, opts.sortKey ?? "");
		const rows = res.toArray();
		const { rowsRead, rowsWritten } = sumSqlMetrics(res);
		const result = rows[0];
		const actorMeta = {
			rowsRead,
			rowsWritten,
			databaseSize: this.ctx.storage.sql.databaseSize,
			servedByActorId: this.ctx.id.toString(),
			servedByActorName: pCtx.doName,
			forwardCount: 0,
		};
		const itemKey = { hashKey: opts.hashKey, sortKey: opts.sortKey };
		if (!result) {
			return { found: false, item: itemKey, meta: actorMeta };
		}
		return {
			found: true,
			item: {
				...itemKey,
				data: typeof result.data === "string" ? result.data : new Uint8Array(result.data),
				ttlEpochUTCSeconds: result.ttl_epoch_utc_seconds ? Number(result.ttl_epoch_utc_seconds) : undefined,
				version: result.v,
			},
			meta: actorMeta,
		};
	}

	private queryPage(cursor: MigrationCursor | null, limit: number): MigratedItem[] {
		type Row = {
			hk: string;
			sk: string;
			data: string | ArrayBuffer;
			ttl_epoch_utc_seconds: number | null;
			v: number;
			last_transaction_ts: number;
		};

		let sqlCursor: SqlStorageCursor<Row>;
		if (!cursor) {
			sqlCursor = this.ctx.storage.sql.exec<Row>(
				`SELECT hk, sk, data, ttl_epoch_utc_seconds, v, last_transaction_ts FROM items ORDER BY hk, sk LIMIT ?`,
				limit,
			);
		} else {
			sqlCursor = this.ctx.storage.sql.exec<Row>(
				`SELECT hk, sk, data, ttl_epoch_utc_seconds, v, last_transaction_ts FROM items WHERE hk > ? OR (hk = ? AND sk > ?) ORDER BY hk, sk LIMIT ?`,
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
				last_transaction_ts: row.last_transaction_ts,
			});
		}
		return items;
	}

	private queryPendingTxPage(cursor: PendingTransactionCursor | null, limit: number): PendingTransactionRow[] {
		type Row = {
			hk: string;
			sk: string;
			transaction_id: string;
			transaction_ts: number;
			operation: string;
			data: string | ArrayBuffer | null;
			conditions_json: string | null;
			coordinator_do_id: string;
			created_at: number;
		};

		const cols = `hk, sk, transaction_id, transaction_ts, operation, data, conditions_json, coordinator_do_id, created_at`;
		let sqlCursor: SqlStorageCursor<Row>;
		if (!cursor) {
			sqlCursor = this.ctx.storage.sql.exec<Row>(`SELECT ${cols} FROM pending_transactions ORDER BY hk, sk, transaction_id LIMIT ?`, limit);
		} else {
			sqlCursor = this.ctx.storage.sql.exec<Row>(
				`SELECT ${cols} FROM pending_transactions
				 WHERE hk > ? OR (hk = ? AND (sk > ? OR (sk = ? AND transaction_id > ?)))
				 ORDER BY hk, sk, transaction_id LIMIT ?`,
				cursor.hk,
				cursor.hk,
				cursor.sk,
				cursor.sk,
				cursor.transaction_id,
				limit,
			);
		}

		const rows: PendingTransactionRow[] = [];
		for (const row of sqlCursor) {
			rows.push({
				...row,
				data: typeof row.data === "string" ? row.data : row.data ? new Uint8Array(row.data) : null,
			});
		}
		return rows;
	}

	private async runMigration(): Promise<void> {
		const parentCtx = this.ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARENT_PARTITION_CONTEXT);
		invariant(parentCtx, "fokos/partition.runMigration: no parent partition context stored");

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
						`INSERT OR IGNORE INTO items (hk, sk, data, ttl_epoch_utc_seconds, v, last_transaction_ts) VALUES (?, ?, ?, ?, ?, ?)`,
						item.hk,
						item.sk,
						item.data,
						item.ttl_epoch_utc_seconds ?? null,
						item.v,
						item.last_transaction_ts,
					);
				}
			}

			// Checkpoint cursor after each batch so we can resume if interrupted.
			cursor = nextCursor;
			this.ctx.storage.kv.put<MigrationCursor | null>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_CURSOR, cursor);

			if (!nextCursor) break;
		}
		invariant(cursor === null, "fokos/partition.runMigration: loop exited with non-null cursor — data may be incomplete");

		// Migrate transaction metadata: pending locks and deletion high-water mark.
		let txCursor: PendingTransactionCursor | null = null;
		while (true) {
			const { maxDeletedTs, pendingTransactions, nextCursor } = await parentStub.getPartitionTransactionMetadata({
				childPartitionContext: pCtx,
				cursor: txCursor,
			});

			this.ctx.storage.transactionSync(() => {
				for (const row of pendingTransactions) {
					this.ctx.storage.sql.exec(
						`INSERT OR IGNORE INTO pending_transactions
						   (hk, sk, transaction_id, transaction_ts, operation, data, conditions_json, coordinator_do_id, created_at)
						 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
						row.hk,
						row.sk,
						row.transaction_id,
						row.transaction_ts,
						row.operation,
						row.data,
						row.conditions_json,
						row.coordinator_do_id,
						row.created_at,
					);
				}
				this.ctx.storage.sql.exec(`UPDATE deletion_metadata SET max_deleted_ts = MAX(max_deleted_ts, ?) WHERE id = 1`, maxDeletedTs);
			});

			if (!nextCursor) break;
			txCursor = nextCursor;
		}

		await this.__testing__beforeMigrationComplete?.();
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
	return item.hk.length * 2 + item.sk.length * 2 + dataSize + 8 + 64;
}

function estimatePendingTxBytes(row: PendingTransactionRow): number {
	const dataSize = row.data == null ? 0 : typeof row.data === "string" ? row.data.length * 2 : row.data.byteLength;
	return row.hk.length * 2 + row.sk.length * 2 + 32 + 8 + 8 + dataSize + (row.conditions_json?.length ?? 0) * 2 + 64;
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
                sk TEXT NOT NULL DEFAULT '',
                data ANY NOT NULL,
                ttl_epoch_utc_seconds INTEGER,
                v INTEGER NOT NULL,
                PRIMARY KEY (hk, sk)
            ) WITHOUT ROWID, STRICT;`,
	},
	{
		idMonotonicInc: 2,
		description: "Add last_transaction_ts to items and create transaction support tables",
		sql: `
            ALTER TABLE items ADD COLUMN last_transaction_ts INTEGER NOT NULL DEFAULT 0;

            CREATE TABLE IF NOT EXISTS pending_transactions (
                hk                    TEXT    NOT NULL,
                sk                    TEXT    NOT NULL DEFAULT '',
                transaction_id        TEXT    NOT NULL,
                transaction_ts        INTEGER NOT NULL,
                operation             TEXT    NOT NULL,
                data                  ANY,
                conditions_json       TEXT,
                coordinator_do_id   TEXT    NOT NULL DEFAULT '',
                created_at            INTEGER NOT NULL,
                PRIMARY KEY (hk, sk, transaction_id)
            ) WITHOUT ROWID, STRICT;

            CREATE INDEX IF NOT EXISTS pending_transactions_created_at
                ON pending_transactions (created_at);

            CREATE TABLE IF NOT EXISTS deletion_metadata (
                id              INTEGER PRIMARY KEY CHECK (id = 1),
                max_deleted_ts  INTEGER NOT NULL DEFAULT 0
            ) STRICT;
            INSERT OR IGNORE INTO deletion_metadata (id, max_deleted_ts) VALUES (1, 0);

`,
	},
];
