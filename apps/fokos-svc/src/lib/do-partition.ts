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
	RecoverTransactionResult,
	TransactionItem,
} from "./transaction-types.js";
import {
	areImmutableOptionsEqual,
	areMutableOptionsEqual,
	PartitionContext,
	PartitionContextResolved,
	PartitionTopologyImpl,
	PartitionTopologySplitter,
	RangePartitionTopologyImpl,
	SplitStatusKVItem,
	resolveRangePartitionContext,
	RANGE_PROMOTION_FRACTION,
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
	acknowledgePromotionComplete(hashKey: string): Promise<void>;
	getItemDirect(opts: GetItemOptions): Promise<GetItemResult>;
};

type PromotedKeyStatus = "queued" | "promoting" | "promoted";

export type InitFromSplitOptions = {
	parentPartitionContext: PartitionContextResolved;
	newPartitionContext: PartitionContextResolved;
	splitType: SplitType;
	// Initial end boundary for a range DO (mutable local state, NOT part of identity).
	// null = unbounded (range root). Omitted for hash children.
	rangeEndBoundary?: string | null;
};

type PartitionSplitMigrationStatus = "migration_initialized" | "migration_migrating" | "migration_completed";

export class PartitionDO extends DurableObject implements PartitionAPI {
	private static readonly KV_KEYS = {
		PARTITION_CONTEXT: "__partition_context",
		PARENT_PARTITION_CONTEXT: "__parent_partition_context",
		PARENT_SPLIT_TYPE: "__parent_split_type",
		SPLIT_MIGRATION_STATUS: "__split_migration_status",
		SPLIT_MIGRATION_CURSOR: "__split_migration_cursor",
		RANGE_END_BOUNDARY: "__range_end_boundary",
	};

	private static readonly STALE_TX_MS = 5_000;
	private static readonly MIGRATION_FALLBACK_ALARM_MS = 10_000;
	private static readonly SPLIT_FALLBACK_ALARM_MS = 5_000;

	#_migrations: SQLSchemaMigrations;
	#_partitionContext?: PartitionContextResolved;
	#_topology?: PartitionTopologySplitter;
	#_backgroundWorkScheduledAt: number | null = null;
	// In-memory cache of promoted_keys, loaded at DO startup. Hash DOs only.
	#_promotedKeys: Map<string, PromotedKeyStatus> = new Map();
	// Epoch timestamp of the last promotion detection run (rate-limited to ≤30s).
	#_lastPromotionDetectionAt: number = 0;

	// ONLY USED FOR TESTING! DO NOT DEPEND ON THESE FIELDS FOR ANY LOGIC IN THE DO.
	__testing__alarm_running = false;
	__testing__backgroundWorkRunning = false;
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
			const pCtx = ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARTITION_CONTEXT);
			if (pCtx) {
				pCtx._partitionIdBytes = Uint8Array.fromHex(pCtx.partitionId);
				this.#_partitionContext = pCtx;
			}

			// Load promoted keys into in-memory cache (hash DOs only; range DOs never have rows).
			const promotedRows = ctx.storage.sql
				.exec<{ hash_key: string; status: string }>(`SELECT hash_key, status FROM promoted_keys`)
				.toArray();
			for (const row of promotedRows) {
				this.#_promotedKeys.set(row.hash_key, row.status as PromotedKeyStatus);
			}
		});
	}

	/**
	 * Only called from the parent partition during the split process to initialize the new child partition
	 * with the right context and its parent partition info that it can use to get data during migration.
	 *
	 * This is not meant to be called directly by clients.
	 */
	async initFromSplit(opts: InitFromSplitOptions, __testing__completeMigration?: boolean, __testing__splitStatus?: SplitStatusKVItem) {
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
		if (newPartitionContext.rangePartition) {
			this.ctx.storage.kv.put<string | null>(PartitionDO.KV_KEYS.RANGE_END_BOUNDARY, opts.rangeEndBoundary ?? null);
		}

		// FIXME - 	Improve the state machine of the migration process so that each child partition can immediately start migration
		// 	       	since now the parent has to be the one triggering the migration by calling triggerMigration() after initFromSplit.
		//          This is OK but if any other flow runs the background job in the child partition, the migration job will also run.
		// Fallback: alarm fires if the DO is evicted before setTimeout runs.
		// await this.ensureAlarmSet(Date.now() + PartitionDO.MIGRATION_FALLBACK_ALARM_MS);
		// Fast path: begin migration in this request's event loop turn.
		// this.scheduleBackgroundWork(0);

		if (__testing__completeMigration) {
			this.ctx.storage.kv.put<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_completed");
		}
		if (__testing__splitStatus) {
			this.ctx.storage.kv.put<SplitStatusKVItem>("__split_status", __testing__splitStatus);
		}
	}

	async triggerMigration(): Promise<void> {
		const isMigrating = await this.ensureMigration("triggerMigration", false);
		if (isMigrating) {
			this.scheduleBackgroundWork({ delayMs: 0, forceSchedule: true });
		}
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
				// Bug 4: keep deletion watermark consistent with transactional deletes
				if (writeRes.rowsWritten > 0) {
					this.ctx.storage.sql.exec(`UPDATE deletion_metadata SET max_deleted_ts = MAX(max_deleted_ts, ?) WHERE id = 1`, Date.now());
				}
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
		const pCtx = this.pCtx();

		// Range-child migration (promotion or range-split).
		if (opts.childPartitionContext.rangePartition) {
			const hk = opts.childPartitionContext.rangePartition.hashKey;
			if (!pCtx.rangePartition) {
				// I am a hash DO; authorize via promoted_keys[hk] === 'promoting'.
				const statusRow = this.ctx.storage.sql
					.exec<{ status: string }>(`SELECT status FROM promoted_keys WHERE hash_key = ?`, hk)
					.toArray()[0];
				invariant(statusRow?.status === "promoting", `fokos/partition.getItemsBatch: key "${hk}" is not in promoting state (got ${statusRow?.status})`);
				return this.getItemsBatchForPromotion(hk, opts.cursor);
			}
			// Range-split child path — Phase 5.
			invariant(false, "fokos/partition.getItemsBatch: range-split child migration not yet implemented (Phase 5)");
		}

		// Hash-child migration.
		const topology = this.ensureTopology(pCtx);
		const splitStatus = topology.splitStatus();
		// Allowed at split_completed: the items table is not deleted at split_completed (only pending_transactions is),
		// so children with racy migration jobs can still fetch item batches after the last sibling has acknowledged.
		invariant(
			splitStatus?.status === "split_started" || splitStatus?.status === "split_completed",
			`fokos/partition.getItemsBatch: expected split_started or split_completed, got ${splitStatus?.status}`,
		);
		const isKnownChild = splitStatus.childPartitionContexts.some((c) => c.doName === opts.childPartitionContext.doName);
		invariant(isKnownChild, `fokos/partition.getItemsBatch: unknown child partition "${opts.childPartitionContext.doName}"`);

		// Workers RPC has a 32MB limit, and each DO is 128MB memory, so we try to be lean around 20MB here.
		const BATCH_LIMIT_BYTES = this.__testing__migrationBatchLimitBytes ?? 20 * 1024 * 1024;
		const PAGE_SIZE = 1000;
		const items: MigratedItem[] = [];
		let totalBytes = 0;
		let tableCursor = opts.cursor;
		let reachedLimit = false;

		const isCorrectHashChildPartition = topology.makeIsCorrectChildHashPartition(pCtx, opts.childPartitionContext);

		while (true) {
			const page = this.queryPage(tableCursor, PAGE_SIZE);
			if (page.length === 0) break;

			for (const row of page) {
				// Filter: only items for the requesting hash child, excluding promoted keys
				// (their data lives in range structures — hash children must not inherit local copies).
				if (isCorrectHashChildPartition(row.hk, row.sk === "" ? undefined : row.sk) && !this.#_promotedKeys.has(row.hk)) {
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

	private getItemsBatchForPromotion(hashKey: string, cursor: MigrationCursor | null): GetItemsBatchResult {
		const BATCH_LIMIT_BYTES = this.__testing__migrationBatchLimitBytes ?? 20 * 1024 * 1024;
		const PAGE_SIZE = 1000;
		type Row = { hk: string; sk: string; data: string | ArrayBuffer; ttl_epoch_utc_seconds: number | null; v: number; last_transaction_ts: number };
		const items: MigratedItem[] = [];
		let totalBytes = 0;
		let tableCursor = cursor;
		let reachedLimit = false;

		while (true) {
			let page: Row[];
			if (!tableCursor) {
				page = this.ctx.storage.sql
					.exec<Row>(
						`SELECT hk, sk, data, ttl_epoch_utc_seconds, v, last_transaction_ts FROM items WHERE hk = ? ORDER BY sk LIMIT ?`,
						hashKey,
						PAGE_SIZE,
					)
					.toArray();
			} else {
				page = this.ctx.storage.sql
					.exec<Row>(
						`SELECT hk, sk, data, ttl_epoch_utc_seconds, v, last_transaction_ts FROM items WHERE hk = ? AND sk > ? ORDER BY sk LIMIT ?`,
						hashKey,
						tableCursor.sk,
						PAGE_SIZE,
					)
					.toArray();
			}

			if (page.length === 0) break;

			for (const row of page) {
				const item: MigratedItem = {
					hk: row.hk,
					sk: row.sk,
					data: typeof row.data === "string" ? row.data : new Uint8Array(row.data),
					ttl_epoch_utc_seconds: row.ttl_epoch_utc_seconds,
					v: row.v,
					last_transaction_ts: row.last_transaction_ts,
				};
				const rowBytes = estimateItemBytes(item);
				if (items.length > 0 && totalBytes + rowBytes > BATCH_LIMIT_BYTES) {
					reachedLimit = true;
					break;
				}
				items.push(item);
				totalBytes += rowBytes;
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
		const maxDeletedTs =
			this.ctx.storage.sql.exec<{ max_deleted_ts: number }>(`SELECT max_deleted_ts FROM deletion_metadata WHERE id = 1`).toArray()[0]
				?.max_deleted_ts ?? 0;

		// Range-child migration (promotion or range-split).
		if (opts.childPartitionContext.rangePartition) {
			const hk = opts.childPartitionContext.rangePartition.hashKey;
			if (!pCtx.rangePartition) {
				// Hash DO serving a promotion: lock-free cutover guarantees no pending_transactions for this key.
				// Return only the deletion watermark so the range root can sync it.
				const statusRow = this.ctx.storage.sql
					.exec<{ status: string }>(`SELECT status FROM promoted_keys WHERE hash_key = ?`, hk)
					.toArray()[0];
				invariant(statusRow?.status === "promoting", `fokos/partition.getPartitionTransactionMetadata: key "${hk}" is not in promoting state (got ${statusRow?.status})`);
				return { maxDeletedTs, pendingTransactions: [], nextCursor: null };
			}
			// Range-split child path — Phase 5.
			invariant(false, "fokos/partition.getPartitionTransactionMetadata: range-split child migration not yet implemented (Phase 5)");
		}

		// Hash-child migration.
		const topology = this.ensureTopology(pCtx);
		const splitStatus = topology.splitStatus();
		// Allowed at split_completed: pending_transactions is deleted atomically with the split_completed transition
		// (acknowledgeChildMigrationComplete), so a call at split_completed returns empty results, which is correct —
		// all children already fetched their rows before the last ack landed.
		invariant(
			splitStatus?.status === "split_started" || splitStatus?.status === "split_completed",
			`fokos/partition.getPartitionTransactionMetadata: expected split_started or split_completed, got ${splitStatus?.status}`,
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
		const topology = this.ensureTopology(this.pCtx());
		// Bug 3: atomically transition topology and clean up parent's pending_transactions when
		// all children have migrated. Children now own authoritative copies; parent's are redundant.
		this.ctx.storage.transactionSync(() => {
			topology.acknowledgeChildMigration(childDoName);
			if (topology.splitStatus()?.status === "split_completed") {
				this.ctx.storage.sql.exec(`DELETE FROM pending_transactions`);
			}
		});
	}

	// Called by a promoted range root once its item migration is complete.
	async acknowledgePromotionComplete(hashKey: string): Promise<void> {
		this.ctx.storage.sql.exec(`UPDATE promoted_keys SET status = 'promoted', updated_at = ? WHERE hash_key = ?`, Date.now(), hashKey);
		this.#_promotedKeys.set(hashKey, "promoted");
		this.scheduleBackgroundWork({ delayMs: 1_000 });
		console.log({ ...this.logParams(), message: "fokos/partition: Promotion complete.", hashKey });
	}

	/**
	 * INTERNAL ONLY FOR TESTING.
	 */
	async status(pCtx?: PartitionContextResolved) {
		// The pCtx is only provided during tests, since any other use-case in production should initialize the DO already as part of the public API.
		pCtx = pCtx ? this.ensurePartitionContext(pCtx) : this.#_partitionContext;
		return {
			partitionContext: pCtx,
			partitionContextStored: this.ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARTITION_CONTEXT),
			splitStatus: pCtx ? this.ensureTopology(pCtx).splitStatus() : undefined,
			migrationStatus: this.ctx.storage.kv.get<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS),
			parentPartitionContext: this.ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARENT_PARTITION_CONTEXT),
			parentSplitType: this.ctx.storage.kv.get<SplitType>(PartitionDO.KV_KEYS.PARENT_SPLIT_TYPE),
			promotedKeys: Array.from(this.#_promotedKeys.entries()).map(([hashKey, status]) => ({ hashKey, status })),
		};
	}

	async prepare(pCtx: PartitionContextResolved, request: PrepareRequest): Promise<PrepareResponse> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("prepare");

		const { local, forwarded, unplaceable } = this.groupItemsByRouting(request.items);
		invariant(unplaceable.length === 0, "fokos/partition.prepare: mis-routed item this node can neither own nor route");

		const tasks: Promise<PrepareResponse>[] = [];
		for (const [, { pCtx: childPCtx, items }] of forwarded) {
			tasks.push(this.getChildStub(childPCtx).prepare(childPCtx, { ...request, items }));
		}
		if (local.length > 0) {
			tasks.push(this.prepareLocal({ ...request, items: local }));
		}
		if (tasks.length === 0) return { outcome: "accepted" };
		const results = await Promise.all(tasks);
		return results.find((r) => r.outcome === "rejected") ?? { outcome: "accepted" };
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
				} else if (item.operation === "put" || item.operation === "delete" || item.operation === "check") {
					// Bug 5: check on a non-existent item must also respect the deletion watermark
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

		if (response.outcome === "accepted") {
			await this.ensureAlarmSet(Date.now() + PartitionDO.STALE_TX_MS);
		}

		return response;
	}

	async commit(pCtx: PartitionContextResolved, request: CommitRequest): Promise<CommitResponse> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("commit"); // Bug 1: reject while THIS partition is migrating

		const { local, forwarded, unplaceable } = this.groupItemsByRouting(request.items);
		invariant(unplaceable.length === 0, "fokos/partition.commit: mis-routed item this node can neither own nor route");

		const tasks: Promise<CommitResponse>[] = [];
		for (const [, { pCtx: childPCtx, items }] of forwarded) {
			tasks.push(this.getChildStub(childPCtx).commit(childPCtx, { ...request, items }));
		}
		if (local.length > 0) {
			tasks.push(Promise.resolve(this.commitLocal({ ...request, items: local })));
		}
		await Promise.all(tasks);
		return { outcome: "committed" };
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
		await this.ensureMigration("cancel"); // Bug 6: reject while THIS partition is migrating
		this.ctx.storage.sql.exec(`DELETE FROM pending_transactions WHERE transaction_id = ?`, request.transactionId);

		const childContexts: PartitionContextResolved[] = [];

		// Split children (hash or range, via existing split_status).
		const topology = this.ensureTopology(pCtx);
		const splitStatus = topology.splitStatus();
		if (splitStatus?.status === "split_started" || splitStatus?.status === "split_completed") {
			childContexts.push(...splitStatus.childPartitionContexts);
		}

		// Promoted-key range roots (hash DOs only).
		if (!pCtx.rangePartition) {
			for (const [hashKey, status] of this.#_promotedKeys) {
				if (status === "promoting" || status === "promoted") {
					const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(pCtx, hashKey, null);
					childContexts.push(rangeRootCtx);
				}
			}
		}

		if (childContexts.length > 0) {
			const results = await Promise.allSettled(
				childContexts.map((childPCtx) => this.getChildStub(childPCtx).cancel(childPCtx, request)),
			);
			const failures = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");
			if (failures.length > 0) {
				console.error({
					...this.logParams(),
					message: "fokos/partition.cancel: child cancel(s) failed",
					transactionId: request.transactionId,
					failureCount: failures.length,
				});
				throw new Error(`fokos/partition.cancel: ${failures.length} child cancel(s) failed for transaction ${request.transactionId}`);
			}
		}

		return { outcome: "cancelled" };
	}

	async readForTransaction(pCtx: PartitionContextResolved, request: ReadForTransactionRequest): Promise<ReadForTransactionResponse> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("readForTransaction");

		const { local, forwarded, unplaceable } = this.groupItemsByRouting(request.items);
		invariant(unplaceable.length === 0, "fokos/partition.readForTransaction: mis-routed item this node can neither own nor route");

		const tasks: Promise<ReadForTransactionResponse>[] = [];
		for (const [, { pCtx: childPCtx, items }] of forwarded) {
			tasks.push(this.getChildStub(childPCtx).readForTransaction(childPCtx, { ...request, items }));
		}
		if (local.length > 0) {
			tasks.push(Promise.resolve(this.readForTransactionLocal({ ...request, items: local })));
		}
		const results = await Promise.all(tasks);
		return { items: results.flatMap((r) => r.items) };
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
		console.log({ ...this.logParams(), message: "fokos/partition: Alarm triggered.", alarmInfo, status: await this.status() });
		this.__testing__alarm_running = true;
		try {
			await this.runBackgroundWork();
		} finally {
			this.__testing__alarm_running = false;
		}
	}

	private async checkSplits(pCtx: PartitionContextResolved, hashKey: string, sortKey?: string): Promise<SplitStatusKVItem | undefined> {
		const topologyRouter = this.ensureTopology(pCtx);
		const splitStatus = await topologyRouter.maybeQueueSplit(hashKey, sortKey);
		if (splitStatus) {
			console.log({ ...this.logParams(), message: "fokos/partition: Split conditions met.", splitStatus });
			await this.ensureAlarmSet(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
			this.scheduleBackgroundWork({ delayMs: 10 });
		}

		// Optimization: on hash partitions, schedule background work when the DB crosses the
		// promotion detection threshold so detection runs promptly rather than waiting for
		// the next hash-split alarm. The detection job would still run eventually (the hash
		// split background job fires its own alarm when it queues a split), but this avoids
		// the latency of waiting for that unrelated trigger.
		//
		// FIXME: Move this in PartitionTopologyImpl.
		if (!pCtx.rangePartition && pCtx.hashSplitConditions.maxSizeMb) {
			const threshold = pCtx.hashSplitConditions.maxSizeMb * RANGE_PROMOTION_FRACTION * 1024 * 1024;
			if (this.ctx.storage.sql.databaseSize > threshold) {
				await this.ensureAlarmSet(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
				this.scheduleBackgroundWork({ delayMs: 10 });
			}
		}

		return splitStatus;
	}

	private pCtx(): PartitionContextResolved {
		invariant(
			this.#_partitionContext,
			// FIXME Optimize this to be statically generated once only since we call pCtx() often.
			`fokos/partition: partition context not initialized for ${this.ctx.id.toString()}[${this.ctx.id.name}]`,
		);
		return this.#_partitionContext;
	}

	private ensurePartitionContext(pCtx: PartitionContextResolved): PartitionContextResolved {
		if (this.#_partitionContext) {
			// We need to check if the provided context matches the stored one to avoid inconsistencies.
			invariant(
				areImmutableOptionsEqual(this.#_partitionContext, pCtx) &&
				this.#_partitionContext.partitionId === pCtx.partitionId &&
				this.#_partitionContext.doName === pCtx.doName &&
				this.#_partitionContext.rangePartition?.hashKey === pCtx.rangePartition?.hashKey &&
				this.#_partitionContext.rangePartition?.startBoundary === pCtx.rangePartition?.startBoundary,
				`fokos/partition.ensurePartitionContext: partition context mismatch`,
			);
			// Fall through to update to the latest version if there are changes.
			if (areMutableOptionsEqual(this.#_partitionContext, pCtx)) {
				return this.#_partitionContext;
			}
		}
		invariant(pCtx.partitionId.length > 0, "fokos/partition.ensurePartitionContext: partitionId must not be empty");
		this.#_partitionContext = { ...pCtx };
		this.#_partitionContext._partitionIdBytes = undefined;
		this.ctx.storage.kv.put<PartitionContextResolved>(PartitionDO.KV_KEYS.PARTITION_CONTEXT, this.#_partitionContext);
		this.#_partitionContext._partitionIdBytes = Uint8Array.fromHex(this.#_partitionContext.partitionId);
		return this.#_partitionContext;
	}

	private ensureTopology(pCtx: PartitionContextResolved): PartitionTopologySplitter {
		if (!this.#_topology) {
			this.#_topology = pCtx.rangePartition
				? new RangePartitionTopologyImpl("", pCtx, this.ctx)
				: new PartitionTopologyImpl("", pCtx, this.ctx);
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
		await this.ensureAlarmSet(Date.now() + PartitionDO.MIGRATION_FALLBACK_ALARM_MS);
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

		// On hash partitions only: forward promoted/promoting keys to their range root.
		// The range root routes by sortKey from there. "queued" still serves locally.
		if (!ctx.rangePartition) {
			const promotedStatus = this.#_promotedKeys.get(hashKey);
			if (promotedStatus === "promoting" || promotedStatus === "promoted") {
				const { doId, partitionContext: rangeRootCtx } = resolveRangePartitionContext(ctx, hashKey, null);
				const rangeRootStub = this.env[ctx.ns].get(doId);
				const result = await forward(rangeRootStub, rangeRootCtx);
				return { ...result, meta: { ...result.meta, forwardCount: result.meta.forwardCount + 1 } } as T;
			}
		}

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
	): { local: T[]; forwarded: Map<string, { pCtx: PartitionContextResolved; items: T[] }>; unplaceable: T[] } {
		const pCtx = this.pCtx();
		const topology = this.ensureTopology(pCtx);
		const local: T[] = [];
		const forwarded = new Map<string, { pCtx: PartitionContextResolved; items: T[] }>();
		const unplaceable: T[] = [];

		const addForwarded = (destPCtx: PartitionContextResolved, item: T) => {
			let entry = forwarded.get(destPCtx.doName);
			if (!entry) {
				entry = { pCtx: destPCtx, items: [] };
				forwarded.set(destPCtx.doName, entry);
			}
			entry.items.push(item);
		};

		for (const item of items) {
			// On hash partitions only: forward promoted/promoting keys to their range root.
			if (!pCtx.rangePartition) {
				const promotedStatus = this.#_promotedKeys.get(item.hashKey);
				if (promotedStatus === "promoting" || promotedStatus === "promoted") {
					const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(pCtx, item.hashKey, null);
					addForwarded(rangeRootCtx, item);
					continue;
				}
			}

			const decision = topology.shouldAllow(item.hashKey, item.sortKey);
			if (decision === "ok") {
				local.push(item);
			} else if (decision === "forward") {
				const { partitionContext } = topology.pickChildPartition(pCtx, item.hashKey, item.sortKey);
				addForwarded(partitionContext, item);
			} else {
				unplaceable.push(item);
			}
		}

		return { local, forwarded, unplaceable };
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
		const migrationStatus = this.ctx.storage.kv.get<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS);
		if (migrationStatus !== "migration_migrating") {
			console.log({ ...this.logParams(), message: "fokos/partition.runMigration: migration not migrating.", migrationStatus });
			return;
		}

		const pCtx = this.pCtx();
		const parentCtx = this.ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARENT_PARTITION_CONTEXT);
		invariant(parentCtx, "fokos/partition.runMigration: no parent partition context stored");

		if (pCtx.rangePartition) {
			await this.runRangeChildMigration(pCtx, parentCtx);
		} else {
			await this.runHashChildMigration(pCtx, parentCtx);
		}
	}

	private async runHashChildMigration(pCtx: PartitionContextResolved, parentCtx: PartitionContextResolved): Promise<void> {
		const parentId = this.env[parentCtx.ns].idFromName(parentCtx.doName);
		const parentStub = this.env[parentCtx.ns].get(parentId) as unknown as ParentPartitionDOStub;

		let cursor = this.ctx.storage.kv.get<MigrationCursor>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_CURSOR) ?? null;

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
		invariant(cursor === null, "fokos/partition.runHashChildMigration: loop exited with non-null cursor — data may be incomplete");

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

		console.log({ ...this.logParams(), message: "fokos/partition: Hash child migration completed." });
	}

	private async runRangeChildMigration(pCtx: PartitionContextResolved, parentCtx: PartitionContextResolved): Promise<void> {
		const parentId = this.env[parentCtx.ns].idFromName(parentCtx.doName);
		const parentStub = this.env[parentCtx.ns].get(parentId) as unknown as ParentPartitionDOStub;

		// Migrate items for this range DO's owned slice.
		// For a promotion root the parent is a hash DO and the filter is WHERE hk = ?;
		// for a range-split child the parent is a range DO (Phase 5).
		let cursor = this.ctx.storage.kv.get<MigrationCursor>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_CURSOR) ?? null;

		while (true) {
			const { items, nextCursor } = await parentStub.getItemsBatch({ childPartitionContext: pCtx, cursor });

			if (items.length > 0) {
				for (const item of items) {
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

			cursor = nextCursor;
			this.ctx.storage.kv.put<MigrationCursor | null>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_CURSOR, cursor);

			if (!nextCursor) break;
		}
		invariant(cursor === null, "fokos/partition.runRangeChildMigration: loop exited with non-null cursor");

		// Sync deletion watermark from parent (no pending_transactions migration — lock-free cutover guarantees none for a promotion).
		const { maxDeletedTs } = await parentStub.getPartitionTransactionMetadata({ childPartitionContext: pCtx, cursor: null });
		this.ctx.storage.sql.exec(`UPDATE deletion_metadata SET max_deleted_ts = MAX(max_deleted_ts, ?) WHERE id = 1`, maxDeletedTs);

		await this.__testing__beforeMigrationComplete?.();
		this.ctx.storage.kv.put<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_completed");
		this.ctx.storage.kv.delete(PartitionDO.KV_KEYS.SPLIT_MIGRATION_CURSOR);

		// Notify the parent: promotion root → acknowledgePromotionComplete; range-split child → acknowledgeChildMigrationComplete (Phase 5).
		if (!parentCtx.rangePartition) {
			await parentStub.acknowledgePromotionComplete(pCtx.rangePartition!.hashKey);
		} else {
			await parentStub.acknowledgeChildMigrationComplete(pCtx.doName);
		}

		console.log({ ...this.logParams(), message: "fokos/partition: Range child migration completed." });
	}

	private async ensureAlarmSet(targetMs: number): Promise<void> {
		const existing = await this.ctx.storage.getAlarm();
		if (existing === null || targetMs < existing) {
			await this.ctx.storage.setAlarm(targetMs);
		}
	}

	private scheduleBackgroundWork(ops: { delayMs: number; forceSchedule?: boolean }): void {
		const delayMs = ops.delayMs ?? 10;
		const targetTime = Date.now() + delayMs;
		if (!ops.forceSchedule && this.#_backgroundWorkScheduledAt !== null && this.#_backgroundWorkScheduledAt <= targetTime) {
			return;
		}
		if (ops.forceSchedule && this.#_backgroundWorkScheduledAt === targetTime) {
			// This means a background work is already scheduled for the same target time, so we can skip scheduling another one.
			// Avoid lots of timers set for the same time which can cause a thundering herd problem and unnecessary resource usage.
			return;
		}
		this.#_backgroundWorkScheduledAt = targetTime;
		setTimeout(() => {
			// FIXME We reset the timestamp for the timer after 1 second to avoid many concurrent runs
			// when the background work takes longer than the delayMs (which is always), to avoid overhead and extra memory usage!
			// We should consider using a more robust scheduling mechanism that allows N overlaps to avoid a stuck background job from progressing.
			void Promise.race([
				this.runBackgroundWork(),
				new Promise((resolve) =>
					setTimeout(() => {
						// Only reset the schedule if it's the same one we set to avoid racing with a newly scheduled background work.
						if (this.#_backgroundWorkScheduledAt === targetTime) {
							this.#_backgroundWorkScheduledAt = null;
							// console.debug({
							// 	...this.logParams(),
							// 	message: "fokos/partition: background work timed out, resetting schedule to allow future runs.",
							// });
						}
						resolve(null);
					}, 1_000),
				),
			]);
		}, delayMs);
	}

	// Drives the queued→promoting→migrated lifecycle for a single hash key.
	// Idempotent — safe to call repeatedly across background cycles.
	private async startPromotion(hashKey: string): Promise<void> {
		const pCtx = this.pCtx();

		// Mutual exclusion: skip if a hash split is already in progress.
		const splitStatus = this.ensureTopology(pCtx).splitStatus();
		if (splitStatus?.status === "split_queued" || splitStatus?.status === "split_started") return;

		// A. Build identity for the range root.
		const { doId: rangeRootId, partitionContext: rangeRootCtx } = resolveRangePartitionContext(pCtx, hashKey, null);
		const rangeRootStub = this.env[pCtx.ns].get(rangeRootId);

		// B. Initialize the range root (idempotent, retry ≤5). No forwarding yet — status is still 'queued'.
		await tryWhile(
			() =>
				rangeRootStub.initFromSplit({
					parentPartitionContext: pCtx,
					newPartitionContext: rangeRootCtx,
					splitType: "range",
					rangeEndBoundary: null,
				}),
			(_err, attempt) => attempt <= 5,
		);

		// C. Cutover queued→promoting in one transactionSync, only if the key has no pending locks.
		const didCutover = this.ctx.storage.transactionSync(() => {
			const lockCount =
				this.ctx.storage.sql
					.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM pending_transactions WHERE hk = ?`, hashKey)
					.toArray()[0]?.n ?? 0;
			if (lockCount > 0) return false; // deferred — retry next background cycle
			this.ctx.storage.sql.exec(`UPDATE promoted_keys SET status = 'promoting', updated_at = ? WHERE hash_key = ?`, Date.now(), hashKey);
			this.#_promotedKeys.set(hashKey, "promoting");
			return true;
		});

		if (!didCutover) {
			console.log({ ...this.logParams(), message: "fokos/partition: Promotion cutover deferred (pending locks).", hashKey });
			return;
		}

		// D. Trigger migration on the range root (fire-and-forget).
		try {
			await rangeRootStub.triggerMigration();
		} catch (e) {
			console.error({ ...this.logParams(), message: "fokos/partition: Failed to trigger promotion migration.", hashKey, error: String(e) });
		}
	}

	private async runBackgroundWork(): Promise<void> {
		invariant(this.#_partitionContext, "fokos/partition.runBackgroundWork: partition context not initialized");
		/**
		 * INVARIANTS FOR ALL BACKGROUND JOBS:
		 * - They should be idempotent and safe to run concurrently (e.g. if the alarm fires again while a previous run is still ongoing) to avoid issues with retries and overlapping runs.
		 * - They should be crash-safe, meaning that if they crash they should not cause the rest jobs to not run and they should be able to resume or retry their work without causing inconsistencies or data loss.
		 * - If they encounter an error, they should log it and reschedule the next run for some time in the future ensuring progress is made eventually.
		 */
		this.__testing__backgroundWorkRunning = true;

		try {
			////////////////////////////////////////////////////////
			// ── Job: Partition migration (for child partitions)
			try {
				const migrationStatus = this.ctx.storage.kv.get<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS);
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
				}
			} catch (error) {
				console.error({ ...this.logParams(), message: "fokos/partition: Migration job failed.", error: String(error), errorProps: error });
			}

			/////////////////////////////////////////////////////
			// ── Job: Partition split (for parent partitions)
			const topology = this.ensureTopology(this.pCtx());
			try {
				const splitStatus = topology.splitStatus();
				if (splitStatus?.status === "split_queued") {
					console.log({ ...this.logParams(), message: "fokos/partition: Running split process.", splitStatus });
					await tryWhile(
						async () => {
							await topology.startSplit();
						},
						(_error, nextAttempt) => nextAttempt <= 5,
					);
				}
			} catch (error) {
				console.error({ ...this.logParams(), message: "fokos/partition: Split job failed.", error: String(error), errorProps: error });
			}

			///////////////////////////////////////////////////////////////////////////////////
			// ── Jobs: Promotion detection, drive, and GC (hash partitions only, not routers)
			const pCtx = this.pCtx();
			if (!pCtx.rangePartition) {
				const topology = this.ensureTopology(pCtx);
				const topSplitStatus = topology.splitStatus();
				// Any non-null split status (queued, started, completed) means this DO has split or is
				// splitting — no promotion detection should run (mutual exclusion or router guard).
				const isHashSplitActive = topSplitStatus != null;

				// Detection: find heavy keys and mark them 'queued'. Rate-limited to once per 30s.
				try {
					const now = Date.now();
					const threshold = (pCtx.hashSplitConditions.maxSizeMb ?? 0) * RANGE_PROMOTION_FRACTION * 1024 * 1024;
					if (
						!isHashSplitActive &&
						threshold > 0 &&
						this.ctx.storage.sql.databaseSize > threshold &&
						now - this.#_lastPromotionDetectionAt >= 30_000
					) {
						this.#_lastPromotionDetectionAt = now;
						// FIXME: This is a full table scan (GROUP BY hk over all items) and is very
						// expensive at scale. Replace with a per-key size counter maintained
						// incrementally on every write, stored in a separate summary table.
						const heavyKeys = this.ctx.storage.sql
							.exec<{ hk: string }>(
								`SELECT hk, SUM(LENGTH(CAST(data AS BLOB)) + LENGTH(sk) + 80) AS est_bytes
								 FROM items GROUP BY hk HAVING est_bytes >= ? ORDER BY est_bytes DESC LIMIT 5`,
								threshold,
							)
							.toArray();
						for (const { hk } of heavyKeys) {
							if (!this.#_promotedKeys.has(hk)) {
								this.ctx.storage.sql.exec(
									`INSERT OR IGNORE INTO promoted_keys (hash_key, status, created_at, updated_at) VALUES (?, 'queued', ?, ?)`,
									hk,
									now,
									now,
								);
								this.#_promotedKeys.set(hk, "queued");
							}
						}
					}
				} catch (error) {
					console.error({ ...this.logParams(), message: "fokos/partition: Promotion detection job failed.", error: String(error) });
				}

				// Drive: advance each queued key through init → cutover → migrate.
				for (const [hashKey, status] of this.#_promotedKeys) {
					if (status === "queued") {
						try {
							await this.startPromotion(hashKey);
						} catch (error) {
							console.error({ ...this.logParams(), message: "fokos/partition: Promotion drive job failed.", hashKey, error: String(error) });
						}
					}
				}

				// GC: delete local items and pending_transactions for fully-promoted keys.
				for (const [hashKey, status] of this.#_promotedKeys) {
					if (status === "promoted") {
						try {
							this.ctx.storage.sql.exec(
								`DELETE FROM items WHERE hk = ? AND sk IN (SELECT sk FROM items WHERE hk = ? ORDER BY sk LIMIT 1000)`,
								hashKey,
								hashKey,
							);
							this.ctx.storage.sql.exec(`DELETE FROM pending_transactions WHERE hk = ?`, hashKey);
						} catch (error) {
							console.error({ ...this.logParams(), message: "fokos/partition: Promotion GC job failed.", hashKey, error: String(error) });
						}
					}
				}
			}

			////////////////////////////////////////
			// ── Job: Stale transaction recovery
			try {
				const staleTxRows = this.ctx.storage.sql
					.exec<{ transaction_id: string; coordinator_do_id: string }>(
						`SELECT DISTINCT transaction_id, coordinator_do_id
                     FROM pending_transactions WHERE created_at < ? LIMIT 10`,
						Date.now() - PartitionDO.STALE_TX_MS,
					)
					.toArray();
				for (const row of staleTxRows) {
					if (!row.coordinator_do_id) continue;
					try {
						const tcId = this.env.TRANSACTION_COORDINATOR_DO.idFromString(row.coordinator_do_id);
						const result = await this.env.TRANSACTION_COORDINATOR_DO.get(tcId).recoverTransaction(row.transaction_id);

						if (result.state === "COMMITTED") {
							const pendingRows = this.ctx.storage.sql
								.exec<{
									hk: string;
									sk: string;
									transaction_ts: number;
									operation: string;
									data: string | ArrayBuffer | null;
								}>(`SELECT hk, sk, transaction_ts, operation, data FROM pending_transactions WHERE transaction_id = ?`, row.transaction_id)
								.toArray();
							if (pendingRows.length > 0) {
								const transactionTimestamp = pendingRows[0].transaction_ts;
								const items: TransactionItem[] = pendingRows.map((r) => ({
									hashKey: r.hk,
									sortKey: r.sk || undefined,
									operation: r.operation as TransactionItem["operation"],
									data: r.data == null ? undefined : typeof r.data === "string" ? r.data : new Uint8Array(r.data),
								}));
								await this.commit(this.pCtx(), { transactionId: row.transaction_id, transactionTimestamp, items });
							}
						} else if (result.state === "CANCELLED" || result.state === "not_found") {
							await this.cancel(this.pCtx(), { transactionId: row.transaction_id });
						}
					} catch (e) {
						console.error({
							...this.logParams(),
							message: "fokos/partition: failed to poke stale TC",
							transactionId: row.transaction_id,
							error: String(e),
						});
					}
				}
			} catch (error) {
				console.error({
					...this.logParams(),
					message: "fokos/partition: Stale TX recovery job failed.",
					error: String(error),
					errorProps: error,
				});
			}
		} catch (error) {
			console.error({
				...this.logParams(),
				message: "fokos/partition: Background work failed with unexpected error.",
				error: String(error),
				errorProps: error,
				status: await this.status(),
			});
		} finally {
			/////////////////////////////////////////////////
			// Check if any job needs to set the next alarm!
			/////////////////////////////////////////////////

			let nextAlarmMs: number | null = null;
			const wantAlarm = (ms: number) => {
				if (nextAlarmMs === null || ms < nextAlarmMs) nextAlarmMs = ms;
			};
			this.ctx.storage.transactionSync(() => {
				// Job: Partition migration for child partitions.
				const postStatus = this.ctx.storage.kv.get<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS);
				if (postStatus === "migration_migrating") {
					wantAlarm(Date.now() + PartitionDO.MIGRATION_FALLBACK_ALARM_MS);
				}

				// Job: Split process for parent partitions.
				if (this.ensureTopology(this.pCtx()).splitStatus()?.status === "split_queued") {
					wantAlarm(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
				}

				// Jobs: Promotion drive (queued keys) and GC (promoted keys with residual items).
				for (const [hashKey, status] of this.#_promotedKeys) {
					if (status === "queued" || status === "promoting") {
						wantAlarm(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
					} else if (status === "promoted") {
						const residual =
							this.ctx.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM items WHERE hk = ?`, hashKey).toArray()[0]?.n ?? 0;
						if (residual > 0) wantAlarm(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
					}
				}

				// Job: Stale transaction recovery.
				const pendingCount =
					this.ctx.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) as n FROM pending_transactions`).toArray()[0]?.n ?? 0;
				if (pendingCount > 0) {
					wantAlarm(Date.now() + PartitionDO.STALE_TX_MS);
				}
			});

			if (nextAlarmMs !== null) {
				await this.ensureAlarmSet(nextAlarmMs);
				// Schedule background work to ensure progress without waiting for the alarm.
				this.scheduleBackgroundWork({ delayMs: 10, forceSchedule: true });
			} else {
				console.log({ ...this.logParams(), message: "fokos/partition: Background work ran, nothing to schedule forward." });
			}

			this.__testing__backgroundWorkRunning = false;
		}
	}

	async destroyPartition(): Promise<void> {
		console.warn({ ...this.logParams(), message: "fokos/partition: Destroying partition — deleting all storage." });

		await this.ctx.blockConcurrencyWhile(async () => {
			// Hack to clear all timeouts.
			// setTimeout returns a numeric ID which increments with each call, so we can get the highest ID and clear all timeouts up to that ID.
			const highestId = setTimeout(() => {
				for (let i = Number(highestId); i >= 0; i--) {
					clearTimeout(i);
				}
			}, 0);
			// Cancel the fallback alarm before wiping storage so Miniflare doesn't try to fire it
			// on the freshly-evicted instance and produce an uncaught alarm-handler error.
			await this.ctx.storage.deleteAlarm();
			await this.ctx.storage.deleteAll();
			console.warn({ ...this.logParams(), message: "fokos/partition: Partition destroyed." });
		});

		// Evict the DO instance so the next caller gets a fresh one with re-run migrations.
		// This throws on the caller side with the sentinel message, which FokosDB.destroy() catches and ignores.
		this.ctx.abort("__special_destroy_sentinel");
		// await this.ctx.blockConcurrencyWhile(async () => {
		// 	throw new Error("__special_destroy_sentinel");
		// });
	}

	private logParams() {
		return {
			actorId: this.ctx.id.toString(),
			// This might truncated to 1024 bytes in Cloudflare Workers, but the full one should be inside partitionContext.doName.
			actorName: this.ctx.id.name,
			// Always put the raw partition context in the logs for better debugging, even if it's undefined.
			partitionContext: this.#_partitionContext ?? null,
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
            INSERT OR IGNORE INTO deletion_metadata (id, max_deleted_ts) VALUES (1, 0);`,
	},
	{
		idMonotonicInc: 3,
		description: "Add range partition support: promoted_keys table",
		sql: `
            CREATE TABLE IF NOT EXISTS promoted_keys (
                hash_key   TEXT NOT NULL PRIMARY KEY,
                status     TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            ) STRICT;`,
	},
];
