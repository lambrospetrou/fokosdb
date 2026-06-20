import { DurableObject } from "cloudflare:workers";
import {
	DeleteItemOptions,
	DeleteItemResult,
	GetItemOptions,
	GetItemResult,
	OperationMetrics,
	PartitionInfo,
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
	ReadForTransactionRequest,
	ReadForTransactionResponse,
	TransactionItem,
} from "./transaction-types.js";
import {
	areImmutableOptionsEqual,
	areMutableOptionsEqual,
	isHashPartition,
	isRangePartition,
	pCtxForLog,
	PartitionContext,
	PartitionContextResolved,
	type InitFromSplitOptions,
} from "./partition-topology/partition-context.js";
import { PartitionIdHelper, resolveRangePartitionContext } from "./partition-topology/partition-id.js";
import { KeyCodec, type KeyBytes } from "./partition-topology/key-codec.js";
import { HashPartitionTopologyImpl, PartitionTopologySplitter, RangePartitionTopologyImpl } from "./partition-topology/split-policy.js";
import { SplitStatusKVItem } from "./partition-topology/split-state.js";
import type { SplitType } from "./partition-topology/types.js";
import { tryWhile } from "durable-utils/retries";
import invariant from "./invariant.js";
import { collectBatch } from "./partition/batch-scan.js";
import {
	estimateItemBytes,
	estimatePendingTxBytes,
	evaluateConditionsOnItem,
	PartitionStore,
	type ItemSnapshot,
	type MigratedItem,
	type ScanCursor,
	type PendingTransactionCursor,
	type PendingTransactionRow,
	type PromotedKeyCursor,
	type PromotedKeyStatus,
} from "./partition/partition-store.js";
import {
	type GetItemsBatchResult,
	type GetPartitionTransactionMetadataResult,
	type GetPromotedKeysBatchResult,
	type PartitionPeer,
} from "./partition/partition-peer.js";
import { MIGRATION_KV_KEYS, SplitMigration, type PartitionSplitMigrationStatus } from "./partition/migration.js";
import { PromotionManager } from "./partition/hash-key-promotion.js";
import { TransactionParticipant } from "./partition/transaction-participant.js";
import { AddResult } from "./bloom-filter.js";
import { PartialRangeTopology, type PartialRangeTopologySnapshot } from "./partition-topology/partial-range-topology.js";
import {
	clipToChildRange,
	cursorFallsInChild,
	isChildFullyBeforeCursor,
	makeBoundaryCursor,
	rangeIntersects,
	type SkInterval,
} from "./query/sk-interval.js";
import { PageBudget } from "./query/page-budget.js";

function isPhantomBounceError(e: unknown): boolean {
	return e instanceof Error && e.message.includes("phantom-bounce");
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

// ─── queryItems internal types ────────────────────────────────────────────────

export type { SkInterval } from "./query/sk-interval.js";
export type { ScanCursor } from "./partition/partition-store.js";

export type QueryItemsRpcRequest = {
	hashKey: KeyBytes;
	interval: SkInterval;
	direction: "asc" | "desc";
	budgetBytes: number;
	remainingLimit: number | null;
	/**
	 * Max number of leaf partitions this request may scan before stopping and returning a
	 * continuation cursor. Bounds the cross-DO subrequest fan-out of a single page over a
	 * heavily-split (or sparse) range subtree. Decremented as the walk descends.
	 */
	maxPartitionVisits: number;
	cursor: ScanCursor | null;
};

export type QueryItemsRpcResult = {
	items: MigratedItem[];
	nextCursor: ScanCursor | null;
	bytesConsumed: number;
	/**
	 * The serving DO's own bookkeeping record (servedBy*, hashDepth) — NOT part of the public
	 * partitionMetas. Its `forwardCount` is subtree-cumulative: withSplitForwarding adds 1 per hash hop,
	 * and a range router adds its child fan-out plus every descendant router's forwards.
	 */
	meta: OperationMetrics & PartitionInfo;
	/** Leaf-only debugging trail: hash leaves and non-split range partitions that actually scanned rows. Routers (hash or range) are excluded. */
	partitionMetas: Array<OperationMetrics & PartitionInfo>;
};

// ─────────────────────────────────────────────────────────────────────────────

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
	queryItems(ctx: PartitionContextResolved, req: QueryItemsRpcRequest): Promise<QueryItemsRpcResult>;
	queryItemsDirect(req: QueryItemsRpcRequest): Promise<QueryItemsRpcResult>;
};

// Re-exported for existing importers (tests, FokosDB); the type itself is context-level and
// lives in partition-topology/partition-context.ts.
export type { InitFromSplitOptions };

export class PartitionDO extends DurableObject implements PartitionAPI {
	private static readonly KV_KEYS = {
		PARTITION_CONTEXT: "__partition_context",
		PARENT_PARTITION_CONTEXT: "__parent_partition_context",
		PARENT_SPLIT_TYPE: "__parent_split_type",
		PARTIAL_RANGE_TOPOLOGY: "__partial_range_topology",
	};

	private static readonly STALE_TX_MS = 5_000;
	private static readonly MIGRATION_FALLBACK_ALARM_MS = 10_000;
	private static readonly SPLIT_FALLBACK_ALARM_MS = 5_000;

	#store: PartitionStore;
	#participant: TransactionParticipant;
	#promotion: PromotionManager;
	#_partitionContext?: PartitionContextResolved;
	#_topology?: PartitionTopologySplitter;
	#_partialRangeTopology: PartialRangeTopology | null = null;
	#_backgroundWorkScheduledAt: number | null = null;

	// ONLY USED FOR TESTING! DO NOT DEPEND ON THESE FIELDS FOR ANY LOGIC IN THE DO.
	__testing__alarm_running = false;
	__testing__backgroundWorkRunning = false;
	__testing__migrationBatchLimitBytes?: number;
	__testing__beforeMigrationComplete?: () => Promise<void>;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.#store = new PartitionStore(ctx.storage);
		this.#participant = new TransactionParticipant({
			store: this.#store,
			// Committed transactional puts feed the same promotion queue check as non-transactional puts.
			onItemUpserted: (hashKey, keyEstBytes) => this.#promotion.maybeQueuePromotion(this.pCtx(), hashKey, keyEstBytes),
		});
		this.#promotion = new PromotionManager({
			store: this.#store,
			// Boundary rule: only the DO acquires stubs — the manager receives this factory.
			getRangeRootPeer: (rangeRootCtx) => this.env[rangeRootCtx.ns].get(this.env[rangeRootCtx.ns].idFromName(rangeRootCtx.doName)),
			scheduleWork: async (opts) => {
				this.scheduleBackgroundWork(opts);
				await this.ensureAlarmSet(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
			},
			logParams: () => this.logParams(),
		});
		void ctx.blockConcurrencyWhile(async () => {
			await this.#store.runMigrations();

			// Load partition context from storage.
			const pCtx = ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARTITION_CONTEXT);
			if (pCtx) {
				pCtx._partitionIdBytes = Uint8Array.fromHex(pCtx.partitionId);
				this.#_partitionContext = pCtx;
			}

			const prtSnap = ctx.storage.kv.get<PartialRangeTopologySnapshot>(PartitionDO.KV_KEYS.PARTIAL_RANGE_TOPOLOGY);
			if (prtSnap) {
				this.#_partialRangeTopology = PartialRangeTopology.fromSnapshot(prtSnap);
			}

			// Load promoted keys into in-memory cache (hash DOs only; range DOs never have rows).
			this.#promotion.loadFromStorage();
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

		this.ensurePartitionContext(newPartitionContext, /* isInit */ true);
		this.ctx.storage.kv.put<PartitionContextResolved>(PartitionDO.KV_KEYS.PARENT_PARTITION_CONTEXT, parentPartitionContext);
		this.ctx.storage.kv.put<SplitType>(PartitionDO.KV_KEYS.PARENT_SPLIT_TYPE, splitType);
		this.ctx.storage.kv.put<PartitionSplitMigrationStatus>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_initialized");

		// FIXME - 	Improve the state machine of the migration process so that each child partition can immediately start migration
		// 	       	since now the parent has to be the one triggering the migration by calling triggerMigration() after initFromSplit.
		//          This is OK but if any other flow runs the background job in the child partition, the migration job will also run.
		// Fallback: alarm fires if the DO is evicted before setTimeout runs.
		// await this.ensureAlarmSet(Date.now() + PartitionDO.MIGRATION_FALLBACK_ALARM_MS);
		// Fast path: begin migration in this request's event loop turn.
		// this.scheduleBackgroundWork(0);

		// FIXME Remove this shit and test properly through the public API.
		if (__testing__completeMigration) {
			this.ctx.storage.kv.put<PartitionSplitMigrationStatus>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_completed");
		}
		if (__testing__splitStatus) {
			this.ctx.storage.kv.put<SplitStatusKVItem>("__split_status", __testing__splitStatus);
		}
	}

	async triggerMigration(): Promise<void> {
		invariant(this.pCtx(), "fokos/partition.triggerMigration: partition context is required");
		const isMigrating = await this.ensureMigration("triggerMigration", false);
		if (isMigrating) {
			this.scheduleBackgroundWork({ delayMs: 0, forceSchedule: true });
		}
	}

	async putItem(pCtx: PartitionContextResolved, opts: PutItemOptions): Promise<PutItemResult> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("putItem");
		const hashKey = PartitionDO.keyIn(opts.hashKey);
		const sortKey = PartitionDO.optKeyIn(opts.sortKey);
		return await this.withSplitForwarding<PutItemResult>({
			ctx: pCtx,
			keys: { hashKey, sortKey },
			operationName: "putItem",
			forward: async (stub, pCtx) => await stub.putItem(pCtx, opts),
			local: async () => {
				const pendingRow = this.#store.pendingLockFor(hashKey, sortKey);
				if (pendingRow) {
					// FIXME: ATC §4 describes optimizations where a non-tx write can proceed using a
					// higher timestamp to force the pending tx to abort on commit, avoiding this rejection.
					throw new Error(
						`fokos/putItem: item is locked by an in-progress transaction (transactionId=${pendingRow.transaction_id}), retry later.`,
					);
				}

				let conditionRes: { rowsRead: number; rowsWritten: number } | null = null;
				if (opts.conditions && opts.conditions.length > 0) {
					const stamp = this.#store.getItemStamp(hashKey, sortKey);
					conditionRes = stamp;
					const item: ItemSnapshot = stamp.row
						? { found: true, hk: hashKey, sk: sortKey, v: stamp.row.v }
						: { found: false, hk: hashKey, sk: sortKey };
					evaluateConditionsOnItem(item, opts.conditions, "putItem");
				}

				const writeRes = this.#store.upsertItem({
					hk: hashKey,
					sk: sortKey,
					data: opts.data,
					ttlEpochUtcSeconds: opts.ttlEpochUTCSeconds ?? null,
					lastTransactionTs: Date.now(),
				});
				const { rowsRead, rowsWritten } = conditionRes ? sumSqlMetrics(conditionRes, writeRes) : writeRes;
				this.#promotion.maybeQueuePromotion(pCtx, hashKey, writeRes.keyEstBytes);

				await this.checkSplits(pCtx, hashKey, sortKey);
				return {
					item: { hashKey: opts.hashKey, sortKey: opts.sortKey },
					version: writeRes.version,
					meta: {
						rowsRead,
						rowsWritten,
						databaseSize: this.#store.databaseSize,
						servedByActorId: this.ctx.id.toString(),
						servedByActorName: pCtx.doName,
						servedByPartitionId: pCtx.partitionId,
						forwardCount: 0,
						hashDepth: isHashPartition(pCtx) ? PartitionIdHelper.depth(this.pCtx()._partitionIdBytes!) : 0,
					},
				};
			},
		});
	}

	async deleteItem(pCtx: PartitionContextResolved, opts: DeleteItemOptions): Promise<DeleteItemResult> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("deleteItem");
		const hashKey = PartitionDO.keyIn(opts.hashKey);
		const sortKey = PartitionDO.optKeyIn(opts.sortKey);
		return await this.withSplitForwarding<DeleteItemResult>({
			ctx: pCtx,
			keys: { hashKey, sortKey },
			operationName: "deleteItem",
			forward: async (stub, pCtx) => await stub.deleteItem(pCtx, opts),
			local: async () => {
				const pendingRow = this.#store.pendingLockFor(hashKey, sortKey);
				if (pendingRow) {
					// FIXME: ATC §4 optimization — see same comment in putItem.
					throw new Error(
						`fokos/deleteItem: item is locked by an in-progress transaction (transactionId=${pendingRow.transaction_id}), retry later.`,
					);
				}

				let conditionRes: { rowsRead: number; rowsWritten: number } | null = null;
				if (opts.conditions && opts.conditions.length > 0) {
					const stamp = this.#store.getItemStamp(hashKey, sortKey);
					conditionRes = stamp;
					const item: ItemSnapshot = stamp.row
						? { found: true, hk: hashKey, sk: sortKey, v: stamp.row.v }
						: { found: false, hk: hashKey, sk: sortKey };
					evaluateConditionsOnItem(item, opts.conditions, "deleteItem");
				}

				// Keep deletion watermark consistent with transactional deletes.
				const writeRes = this.#store.deleteItem({ hk: hashKey, sk: sortKey, watermarkTs: Date.now() });
				const { rowsRead, rowsWritten } = conditionRes ? sumSqlMetrics(conditionRes, writeRes) : writeRes;
				return {
					item: { hashKey: opts.hashKey, sortKey: opts.sortKey },
					deleted: writeRes.deleted,
					meta: {
						rowsRead,
						rowsWritten,
						databaseSize: this.#store.databaseSize,
						servedByActorId: this.ctx.id.toString(),
						servedByActorName: pCtx.doName,
						servedByPartitionId: pCtx.partitionId,
						forwardCount: 0,
						hashDepth: isHashPartition(pCtx) ? PartitionIdHelper.depth(this.pCtx()._partitionIdBytes!) : 0,
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
			const result = await parentStub.getItemDirect(opts);
			// The parent returns its own hashDepth, but the caller forwarded to this child partition.
			// recordForwardResult on the caller requires responseHashDepth >= toAbsDepth (this child's depth).
			if (isHashPartition(pCtx)) {
				return {
					...result,
					meta: { ...result.meta, hashDepth: PartitionIdHelper.depth(this.pCtx()._partitionIdBytes!) },
				};
			}
			return result;
		}

		return await this.withSplitForwarding<GetItemResult>({
			ctx: pCtx,
			keys: { hashKey: PartitionDO.keyIn(opts.hashKey), sortKey: PartitionDO.optKeyIn(opts.sortKey) },
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

	async queryItems(pCtx: PartitionContextResolved, req: QueryItemsRpcRequest): Promise<QueryItemsRpcResult> {
		this.ensurePartitionContext(pCtx);

		// If still migrating, read directly from the parent (mirrors getItem / getItemDirect).
		if (await this.ensureMigration("queryItems", false)) {
			const parentCtx = this.ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARENT_PARTITION_CONTEXT);
			invariant(parentCtx, "fokos/partition.queryItems: no parent partition context stored during migration");
			const parentId = this.env[parentCtx.ns].idFromName(parentCtx.doName);
			const parentStub = this.env[parentCtx.ns].get(parentId) as PartitionDOStub;
			const result = await parentStub.queryItemsDirect(req);
			if (isHashPartition(pCtx)) {
				const myDepth = PartitionIdHelper.depth(this.pCtx()._partitionIdBytes!);
				return { ...result, meta: { ...result.meta, hashDepth: myDepth } };
			}
			return result;
		}

		// Range partitions (the range root reached via promotion-forward, or a range child reached via
		// walkRangeChildren) must NOT go through withSplitForwarding: its range-topology shouldAllow
		// returns "forward" for a split router and would single-child-route by the sentinel sort key,
		// bypassing the fan-out. The range-tree walk owns multi-leaf traversal instead.
		if (isRangePartition(pCtx)) {
			return await this.queryItemsAsRangeNode(pCtx, req);
		}

		// Hash partitions: withSplitForwarding handles promotion (forward to the range root), the
		// learned-promotion bloom filter, and hash-split forwarding. The sentinel sort key routes by
		// hash key only — all sks of a non-promoted key live on one leaf, so `local` is a leaf scan.
		return await this.withSplitForwarding<QueryItemsRpcResult>({
			ctx: pCtx,
			keys: { hashKey: PartitionDO.keyIn(req.hashKey), sortKey: KeyCodec.encodeOptional(undefined) },
			operationName: "queryItems",
			forward: async (stub, childPCtx) => await stub.queryItems(childPCtx, req),
			local: async () => await this.queryItemsLocal(this.pCtx(), req),
		});
	}

	// Direct read bypassing split forwarding — used by migrating children to avoid forwarding loops
	// (same rationale as getItemDirect). Must always read local rows only: a range router that fans
	// out to children via queryItemsAsRangeNode would route back to the calling migrating child,
	// causing an infinite loop (child → queryItemsDirect → walkRangeChildren → child.queryItems → …).
	async queryItemsDirect(req: QueryItemsRpcRequest): Promise<QueryItemsRpcResult> {
		return this.queryItemsLocal(this.pCtx(), req);
	}

	private queryItemsLocal(pCtx: PartitionContextResolved, req: QueryItemsRpcRequest): QueryItemsRpcResult {
		const hk = PartitionDO.keyIn(req.hashKey);
		const { interval, cursor, budgetBytes, remainingLimit } = req;

		const lower = interval.lower?.value ?? KeyCodec.encodeOptional(undefined);
		const lowerInclusive = interval.lower?.inclusive ?? true;
		const upper = interval.upper?.value ?? null;
		const upperInclusive = interval.upper?.inclusive ?? false;

		let rowsScanned = 0;
		const PAGE_SIZE = 1000;

		const {
			rows,
			nextCursor,
			totalBytes: bytesConsumed,
		} = collectBatch<MigratedItem, ScanCursor>({
			fetchPage: (pageCursor, pageSize) => {
				const page = this.#store.queryRangeItemsPage({
					hk,
					lower,
					lowerInclusive,
					upper,
					upperInclusive,
					cursor: pageCursor,
					limit: pageSize,
					direction: req.direction,
				});
				rowsScanned += page.length;
				return page;
			},
			advanceCursor: (row) => ({ hk: row.hk, sk: row.sk }),
			estimateBytes: estimateItemBytes,
			budgetBytes,
			maxItems: remainingLimit ?? undefined,
			pageSize: PAGE_SIZE,
			startCursor: cursor,
		});

		// A leaf (hash leaf or non-split range partition) is the only kind of DO that scans rows, so it
		// is the only kind that contributes a `partitionMetas` entry. Routers (hash or range) are
		// excluded — they appear only numerically via `forwardCount`.
		const meta: OperationMetrics & PartitionInfo = {
			rowsRead: rowsScanned,
			rowsWritten: 0,
			databaseSize: this.#store.databaseSize,
			servedByActorId: this.ctx.id.toString(),
			servedByActorName: pCtx.doName,
			servedByPartitionId: pCtx.partitionId,
			forwardCount: 0,
			hashDepth: isHashPartition(pCtx) ? PartitionIdHelper.depth(this.pCtx()._partitionIdBytes!) : 0,
		};

		return { items: rows, nextCursor, bytesConsumed, meta, partitionMetas: [meta] };
	}

	private async queryItemsAsRangeNode(pCtx: PartitionContextResolved, req: QueryItemsRpcRequest): Promise<QueryItemsRpcResult> {
		const topology = this.ensureTopology(pCtx);
		const splitStatus = topology.splitStatus();

		if (splitStatus?.status === "split_started" || splitStatus?.status === "split_completed") {
			return await this.walkRangeChildren(pCtx, splitStatus.childPartitionContexts, req);
		}

		return this.queryItemsLocal(pCtx, req);
	}

	private async walkRangeChildren(
		pCtx: PartitionContextResolved,
		children: PartitionContextResolved[],
		req: QueryItemsRpcRequest,
	): Promise<QueryItemsRpcResult> {
		const { interval, cursor, direction } = req;
		const budget = new PageBudget(req.budgetBytes, req.remainingLimit, req.maxPartitionVisits);

		const allItems: MigratedItem[] = [];
		// Only leaf entries accumulate here — a range router (this node) and any deeper routers
		// contribute nothing of their own; they're captured numerically via `forwardCount`.
		const leafMetas: Array<OperationMetrics & PartitionInfo> = [];
		let nextCursor: ScanCursor | null = null;
		let totalBytesConsumed = 0;
		let childrenCalled = 0;
		// Sum of forwards performed by descendant routers, so this node's `forwardCount` is cumulative.
		let descendantForwards = 0;

		// Children are stored in ascending boundary order; reverse for desc.
		const orderedChildren = direction === "desc" ? [...children].reverse() : children;

		for (let i = 0; i < orderedChildren.length; i++) {
			const childCtx = orderedChildren[i];
			const rp = childCtx.rangePartition;
			invariant(rp, "fokos/partition.walkRangeChildren: child has no rangePartition context");
			const childStart = rp.startBoundary ?? KeyCodec.encodeOptional(undefined);
			const childEnd = rp.endBoundary;

			if (!rangeIntersects(childStart, childEnd, interval)) continue;
			if (cursor && isChildFullyBeforeCursor(childStart, childEnd, cursor, direction)) continue;

			const childCursor = cursor && cursorFallsInChild(childStart, childEnd, cursor) ? cursor : null;
			const clippedInterval = clipToChildRange(interval, rp.startBoundary, childEnd);
			const childStub = this.getChildStub(childCtx);
			const childResult = await childStub.queryItems(childCtx, {
				...req,
				interval: clippedInterval,
				budgetBytes: budget.remainingBytes,
				remainingLimit: budget.remainingLimit,
				maxPartitionVisits: budget.remainingVisits,
				cursor: childCursor,
			});

			allItems.push(...childResult.items);
			leafMetas.push(...childResult.partitionMetas);
			descendantForwards += childResult.meta.forwardCount;
			totalBytesConsumed += childResult.bytesConsumed;
			budget.consume(childResult.bytesConsumed, childResult.items.length, childResult.partitionMetas.length);
			childrenCalled++;

			if (childResult.nextCursor !== null) {
				nextCursor = childResult.nextCursor;
				break;
			}
			if (budget.budgetExhausted) {
				const lastItem = allItems[allItems.length - 1];
				if (lastItem) nextCursor = { hk: lastItem.hk, sk: lastItem.sk };
				break;
			}
			if (budget.visitsExhausted && i < orderedChildren.length - 1) {
				console.warn(`fokos/partition.walkRangeChildren: maxPartitionVisits reached (${req.maxPartitionVisits}), emitting boundary cursor`);
				nextCursor = makeBoundaryCursor(req.hashKey, childStart, childEnd, direction);
				break;
			}
		}

		// This range router is a pure router: it reads no rows and is NOT listed in `partitionMetas`.
		// Its `meta` exists only for routing bookkeeping (servedBy*, hashDepth) and to carry the
		// subtree-cumulative `forwardCount` (its own child fan-out plus every descendant router's).
		const meta: OperationMetrics & PartitionInfo = {
			rowsRead: 0,
			rowsWritten: 0,
			databaseSize: this.#store.databaseSize,
			servedByActorId: this.ctx.id.toString(),
			servedByActorName: pCtx.doName,
			servedByPartitionId: pCtx.partitionId,
			forwardCount: childrenCalled + descendantForwards,
			hashDepth: 0,
		};

		return { items: allItems, nextCursor, bytesConsumed: totalBytesConsumed, meta, partitionMetas: leafMetas };
	}

	async getItemsBatch(opts: { childPartitionContext: PartitionContextResolved; cursor: ScanCursor | null }): Promise<GetItemsBatchResult> {
		const pCtx = this.pCtx();

		// Range-child migration (promotion or range-split).
		const childPartitionContext = opts.childPartitionContext;
		if (isRangePartition(childPartitionContext)) {
			const hk = childPartitionContext.rangePartition.hashKey;
			if (isHashPartition(pCtx)) {
				// I am a hash DO; authorize via promoted_keys[hk] === 'promoting'.
				const status = this.#promotion.statusFor(hk);
				invariant(
					status === "promoting",
					() => `fokos/partition.getItemsBatch: key ${KeyCodec.keyForLog(hk)} is not in promoting state (got ${status})`,
				);
				return this.getItemsBatchForRange(hk, null, null, opts.cursor);
			}
			// Range-split: I am a range DO becoming a router; authorize the child and stream its [start, end) slice.
			const topology = this.ensureTopology(pCtx);
			const splitStatus = topology.splitStatus();
			invariant(
				splitStatus?.status === "split_started" || splitStatus?.status === "split_completed",
				`fokos/partition.getItemsBatch: expected split_started or split_completed, got ${splitStatus?.status}`,
			);
			const isKnownChild = splitStatus.childPartitionContexts.some((c) => c.doName === childPartitionContext.doName);
			invariant(isKnownChild, `fokos/partition.getItemsBatch: unknown range child partition "${childPartitionContext.doName}"`);
			return this.getItemsBatchForRange(
				hk,
				childPartitionContext.rangePartition.startBoundary,
				childPartitionContext.rangePartition.endBoundary,
				opts.cursor,
			);
		}

		// Hash-child migration.
		const topology = this.ensureHashTopology(pCtx);
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

		const isCorrectHashChildPartition = topology.makeIsCorrectChildHashPartition(pCtx, opts.childPartitionContext);

		const { rows, nextCursor } = collectBatch<MigratedItem, ScanCursor>({
			fetchPage: (cursor, pageSize) => this.#store.queryItemsPage(cursor, pageSize),
			advanceCursor: (row) => ({ hk: row.hk, sk: row.sk }),
			// Filter: only items for the requesting hash child, excluding promoted keys
			// (their data lives in range structures — hash children must not inherit local copies).
			include: (row) => isCorrectHashChildPartition(row.hk, row.sk.length === 0 ? undefined : row.sk) && !this.#promotion.hasStatus(row.hk),
			estimateBytes: estimateItemBytes,
			budgetBytes: BATCH_LIMIT_BYTES,
			pageSize: PAGE_SIZE,
			startCursor: opts.cursor,
		});
		return { items: rows, nextCursor };
	}

	// Streams items for a range DO child's owned slice [start, end) (start/end null = unbounded edge).
	// Used by both promotion (start=end=null: the whole hashKey) and range-split (the child's sub-slice).
	private getItemsBatchForRange(
		hashKey: KeyBytes,
		start: KeyBytes | null,
		end: KeyBytes | null,
		cursor: ScanCursor | null,
	): GetItemsBatchResult {
		const BATCH_LIMIT_BYTES = this.__testing__migrationBatchLimitBytes ?? 20 * 1024 * 1024;
		const PAGE_SIZE = 1000;
		const lower = start ?? KeyCodec.encodeOptional(undefined);

		const { rows, nextCursor } = collectBatch<MigratedItem, ScanCursor>({
			// Resume strictly after the cursor; otherwise start from the range's lower bound. Always bound by `end`.
			fetchPage: (pageCursor, pageSize) =>
				this.#store.queryRangeItemsPage({
					hk: hashKey,
					lower,
					lowerInclusive: true,
					upper: end,
					upperInclusive: false,
					cursor: pageCursor,
					limit: pageSize,
					direction: "asc",
				}),
			advanceCursor: (row) => ({ hk: row.hk, sk: row.sk }),
			estimateBytes: estimateItemBytes,
			budgetBytes: BATCH_LIMIT_BYTES,
			pageSize: PAGE_SIZE,
			startCursor: cursor,
		});
		return { items: rows, nextCursor };
	}

	async getPartitionTransactionMetadata(opts: {
		childPartitionContext: PartitionContextResolved;
		cursor: PendingTransactionCursor | null;
	}): Promise<GetPartitionTransactionMetadataResult> {
		const pCtx = this.pCtx();
		const maxDeletedTs = this.#store.getMaxDeletedTs();

		// Range-child migration (promotion or range-split).
		const childPartitionContext = opts.childPartitionContext;
		if (isRangePartition(childPartitionContext)) {
			const hk = childPartitionContext.rangePartition.hashKey;
			if (isHashPartition(pCtx)) {
				// Hash DO serving a promotion: lock-free cutover guarantees no pending_transactions for this key.
				// Return only the deletion watermark so the range root can sync it.
				const status = this.#promotion.statusFor(hk);
				invariant(
					status === "promoting",
					() => `fokos/partition.getPartitionTransactionMetadata: key ${KeyCodec.keyForLog(hk)} is not in promoting state (got ${status})`,
				);
				return { maxDeletedTs, pendingTransactions: [], nextCursor: null };
			}
			// Range-split: stream the child's pending locks (sk ∈ [start, end)) so commit/cancel can follow.
			const topology = this.ensureTopology(pCtx);
			const splitStatus = topology.splitStatus();
			invariant(
				splitStatus?.status === "split_started" || splitStatus?.status === "split_completed",
				`fokos/partition.getPartitionTransactionMetadata: expected split_started or split_completed, got ${splitStatus?.status}`,
			);
			const isKnownChild = splitStatus.childPartitionContexts.some((c) => c.doName === childPartitionContext.doName);
			invariant(
				isKnownChild,
				`fokos/partition.getPartitionTransactionMetadata: unknown range child partition "${childPartitionContext.doName}"`,
			);

			const lower = childPartitionContext.rangePartition.startBoundary ?? KeyCodec.encodeOptional(undefined);
			const upper = childPartitionContext.rangePartition.endBoundary; // null = unbounded
			const inChildRange = (sk: KeyBytes) => KeyCodec.compare(sk, lower) >= 0 && (upper === null || KeyCodec.compare(sk, upper) < 0);

			const { rows, nextCursor } = collectBatch<PendingTransactionRow, PendingTransactionCursor>({
				fetchPage: (cursor, pageSize) => this.#store.queryPendingTxPage(cursor, pageSize),
				advanceCursor: (row) => ({ hk: row.hk, sk: row.sk, transaction_id: row.transaction_id }),
				include: (row) => KeyCodec.compare(row.hk, hk) === 0 && inChildRange(row.sk),
				estimateBytes: estimatePendingTxBytes,
				budgetBytes: this.__testing__migrationBatchLimitBytes ?? 20 * 1024 * 1024,
				pageSize: 1000,
				startCursor: opts.cursor,
			});
			return {
				maxDeletedTs,
				pendingTransactions: rows,
				nextCursor,
			};
		}

		// Hash-child migration.
		const topology = this.ensureHashTopology(pCtx);
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

		const { rows, nextCursor } = collectBatch<PendingTransactionRow, PendingTransactionCursor>({
			fetchPage: (cursor, pageSize) => this.#store.queryPendingTxPage(cursor, pageSize),
			advanceCursor: (row) => ({ hk: row.hk, sk: row.sk, transaction_id: row.transaction_id }),
			include: (row) => isCorrectHashChildPartition(row.hk, row.sk.length === 0 ? undefined : row.sk),
			estimateBytes: estimatePendingTxBytes,
			budgetBytes: this.__testing__migrationBatchLimitBytes ?? 20 * 1024 * 1024,
			pageSize: 1000,
			startCursor: opts.cursor,
		});

		return {
			maxDeletedTs,
			pendingTransactions: rows,
			nextCursor,
		};
	}

	async acknowledgeChildMigrationComplete(childDoName: string): Promise<void> {
		const topology = this.ensureTopology(this.pCtx());
		// Atomically transition topology and clean up parent's pending_transactions when
		// all children have migrated. Children now own authoritative copies; parent's are redundant.
		this.#store.transactionSync(() => {
			topology.acknowledgeChildMigration(childDoName);
			if (topology.splitStatus()?.status === "split_completed") {
				this.#store.deleteAllPendingTx();
			}
		});
	}

	// Paginated promoted_keys for hash-split inheritance: a hash child pulls the promoted-key entries
	// (forward-pointers) for the keys it now owns. Only the set transfers — never the data, which lives
	// in the autonomous range structure (the range-root name is recomputable from the hashKey).
	async getPromotedKeysBatch(opts: {
		childPartitionContext: PartitionContextResolved;
		cursor: PromotedKeyCursor | null;
	}): Promise<GetPromotedKeysBatchResult> {
		const pCtx = this.pCtx();
		invariant(isHashPartition(pCtx), "fokos/partition.getPromotedKeysBatch: only hash partitions have promoted keys");

		const isCorrectChild = this.ensureHashTopology(pCtx).makeIsCorrectChildHashPartition(pCtx, opts.childPartitionContext);
		// promoted_keys rows are small (≤ ~1 KB each given the hash_key length cap), so 10K rows ≈ 10 MB,
		// comfortably under the 32 MB RPC limit — one page usually drains the whole table.
		const SCAN_LIMIT = 10_000;
		const { rows, nextCursor } = collectBatch<{ hash_key: KeyBytes; status: PromotedKeyStatus }, PromotedKeyCursor>({
			fetchPage: (cursor, pageSize) => this.#store.queryPromotedKeysPage(cursor, pageSize),
			advanceCursor: (row) => ({ hashKey: row.hash_key }),
			include: (row) => isCorrectChild(row.hash_key),
			estimateBytes: (row) => row.hash_key.byteLength + 16,
			budgetBytes: this.__testing__migrationBatchLimitBytes ?? 20 * 1024 * 1024,
			pageSize: SCAN_LIMIT,
			startCursor: opts.cursor,
		});
		return { rows, nextCursor };
	}

	// Called by a promoted range root once its item migration is complete.
	async acknowledgePromotionComplete(hashKey: KeyBytes): Promise<void> {
		const pCtx = this.pCtx();
		invariant(isHashPartition(pCtx), "fokos/partition.acknowledgePromotionComplete: only hash partitions can have promoted keys");
		// RPC erases the brand; re-brand the incoming bytes before passing inward.
		await this.#promotion.acknowledgePromotionComplete(PartitionDO.keyIn(hashKey));
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
			migrationStatus: this.ctx.storage.kv.get<PartitionSplitMigrationStatus>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_STATUS),
			parentPartitionContext: this.ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARENT_PARTITION_CONTEXT),
			parentSplitType: this.ctx.storage.kv.get<SplitType>(PartitionDO.KV_KEYS.PARENT_SPLIT_TYPE),
			promotedKeys: this.#promotion.snapshot(),
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
		const response = this.#participant.prepareLocal(request);

		if (response.outcome === "accepted") {
			await this.ensureAlarmSet(Date.now() + PartitionDO.STALE_TX_MS);
		}

		return response;
	}

	async commit(pCtx: PartitionContextResolved, request: CommitRequest): Promise<CommitResponse> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("commit"); // reject while this partition is migrating

		const { local, forwarded, unplaceable } = this.groupItemsByRouting(request.items);
		invariant(unplaceable.length === 0, "fokos/partition.commit: mis-routed item this node can neither own nor route");

		const tasks: Promise<CommitResponse>[] = [];
		for (const [, { pCtx: childPCtx, items }] of forwarded) {
			tasks.push(this.getChildStub(childPCtx).commit(childPCtx, { ...request, items }));
		}
		if (local.length > 0) {
			tasks.push(Promise.resolve(this.#participant.commitLocal({ ...request, items: local })));
		}
		await Promise.all(tasks);
		return { outcome: "committed" };
	}

	async cancel(pCtx: PartitionContextResolved, request: CancelRequest): Promise<CancelResponse> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("cancel"); // reject while this partition is migrating
		this.#participant.cancelLocal(request.transactionId);

		const childContexts: PartitionContextResolved[] = [];

		// Split children (hash or range, via existing split_status).
		const topology = this.ensureTopology(pCtx);
		const splitStatus = topology.splitStatus();
		if (splitStatus?.status === "split_started" || splitStatus?.status === "split_completed") {
			childContexts.push(...splitStatus.childPartitionContexts);
		}

		// Promoted-key range roots (hash DOs only).
		// FIXME(perf): We should only forward to the range roots (hash keys) that actually have pending locks for this transaction.
		if (isHashPartition(pCtx)) {
			for (const hashKey of this.#promotion.activeRangeRootHashKeys()) {
				const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(pCtx, hashKey, null, null);
				childContexts.push(rangeRootCtx);
			}
		}

		if (childContexts.length > 0) {
			const results = await Promise.allSettled(childContexts.map((childPCtx) => this.getChildStub(childPCtx).cancel(childPCtx, request)));
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
			tasks.push(Promise.resolve(this.#participant.readForTransactionLocal({ ...request, items: local })));
		}
		const results = await Promise.all(tasks);
		return { items: results.flatMap((r) => r.items) };
	}

	async alarm(alarmInfo: AlarmInvocationInfo): Promise<void> {
		console.log({
			...this.logParams(),
			message: "fokos/partition: Alarm triggered.",
			alarmInfo,
		});
		this.__testing__alarm_running = true;
		try {
			await this.runBackgroundWork();
		} finally {
			this.__testing__alarm_running = false;
		}
	}

	private async checkSplits(pCtx: PartitionContextResolved, hashKey: KeyBytes, sortKey?: KeyBytes): Promise<SplitStatusKVItem | undefined> {
		const topology = this.ensureTopology(pCtx);
		const splitStatus = await topology.maybeQueueSplit(hashKey, sortKey, {
			hasInFlightPromotions: this.#promotion.hasInFlightPromotions(),
		});
		if (splitStatus) {
			console.log({
				...this.logParams(),
				message: "fokos/partition: Split conditions met.",
				splitStatus: { status: splitStatus.status, splitType: splitStatus.splitType },
			});
			await this.ensureAlarmSet(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
			this.scheduleBackgroundWork({ delayMs: 10 });
		}

		return splitStatus;
	}

	/**
	 * Orchestrates the split fan-out: the policy decides (prepareSplit), the DO performs the RPCs
	 * (boundary rule: only DO classes and FokosDB hold stubs). Failure ordering matches the old
	 * topology startSplit exactly — if any child init fails we abort BEFORE the split_started KV
	 * transition, so the retry path is unchanged.
	 */
	private async runSplit(topology: PartitionTopologySplitter): Promise<void> {
		const splitStatus = topology.splitStatus();
		if (!splitStatus || splitStatus.status !== "split_queued") {
			// Already started or completed — idempotent no-op.
			return;
		}

		// Range splits need boundaries computed from the data (a store query); the policy receives them as input.
		let boundaries: KeyBytes[] | null = null;
		if (splitStatus.splitType === "range") {
			const pCtx = this.pCtx();
			const rp = pCtx.rangePartition;
			invariant(rp, "fokos/range.startSplit: missing rangePartition identity");
			const N = pCtx.rangeSplitN;
			invariant(N != null && N >= 2, "fokos/range.startSplit: rangeSplitN must be >= 2");
			// Compute N-1 split boundaries within the owned slice [start, end) in one snapshot.
			boundaries = this.#store.computeRangeSplitBoundaries(rp.hashKey, rp.startBoundary, rp.endBoundary, N);
			if (!boundaries) {
				// Not enough distinct items to split into N non-empty children — retry on a later cycle.
				console.error({
					...this.logParams(),
					message: "fokos/range.startSplit: insufficient items to split into N children; will retry.",
					hashKey: KeyCodec.keyForLog(rp.hashKey),
					startBoundary: rp.startBoundary === null ? null : KeyCodec.keyForLog(rp.startBoundary),
					endBoundary: rp.endBoundary === null ? null : KeyCodec.keyForLog(rp.endBoundary),
				});
				return;
			}
		}

		const childInits = topology.prepareSplit({ boundaries });
		if (!childInits) return;

		// Call the new DOs at `initFromSplit()` to initialize them with the right context and their
		// parent partition info that they will use to get data during migration (retry ≤5 each).
		const promises = childInits.map(async (childContext) => {
			const doId = this.env[childContext.newPartitionContext.ns].idFromName(childContext.newPartitionContext.doName);
			try {
				return await tryWhile(
					async () => {
						const childDo = this.env[childContext.newPartitionContext.ns].get(doId);
						return await childDo.initFromSplit(childContext);
					},
					(_error, nextAttempt) => {
						return nextAttempt <= 5; // Retry up to 5 times
					},
				);
			} catch (error) {
				// Handle initialization errors
				console.error({
					message: "fokos/topology: Split initialization failed, aborting split process. Will retry later.",
					error: String(error),
					errorProps: error,
					doId: doId.toString(),
					childContext: {
						parentPartitionContext: pCtxForLog(childContext.parentPartitionContext),
						newPartitionContext: pCtxForLog(childContext.newPartitionContext),
						splitType: childContext.splitType,
					},
				});
				throw error; // Rethrow to be caught by the outer try-catch and trigger a retry of the split process.
			}
		});

		// If any of the initializations fail we abort for now, and retry later.
		// Ideally even partial initializations should be handled gracefully, but for now we can just rely on retries to get to a consistent state.
		// The partition DOs should be the source of truth for everything so until the split initialization succeeds, this parent DO is the owner.
		// FIXME Improve this by allowing some child partitions to not be initialized, which will need a topology router functionality to ask the parent for the context again, which is doable!
		try {
			await Promise.all(promises);
		} catch (error) {
			console.error({
				message: "fokos/topology: Some split initialization failed, aborting split process. Will retry later.",
				error: String(error),
				errorProps: error,
				parentPartitionContext: pCtxForLog(this.pCtx()),
			});

			// By throwing here we stop the split process. The next request will call `queueSplit()` again
			// setting a new alarm, which will retry the split process and hopefully succeed if the errors were transient.
			throw error;
		}

		// Mark the split status as `split_started`: the new partitions now handle requests and this
		// partition is just a proxy that forwards to them until migration completes (split_completed).
		topology.commitSplitStarted(childInits.map((c) => c.newPartitionContext));

		// Kick off migration on each child immediately so it doesn't wait for the first user request.
		// Fire-and-forget: failures are logged but do not fail the split — the child will
		// start migrating on its first incoming request if this doesn't reach it.
		// We do not use this.ctx.waitUntil(...) since it causes vitest errors with tangling log messages.
		await Promise.allSettled(
			childInits.map(async (childContext) => {
				try {
					const doId = this.env[childContext.newPartitionContext.ns].idFromName(childContext.newPartitionContext.doName);
					const childDo = this.env[childContext.newPartitionContext.ns].get(doId);
					await childDo.triggerMigration();
				} catch (error) {
					console.error({
						message: "fokos/topology: Failed to trigger migration on child partition; will start on the next request.",
						error: String(error),
						errorProps: error,
						childDoName: childContext.newPartitionContext.doName,
					});
				}
			}),
		);

		console.log({
			message: "fokos/topology: Split process completed successfully.",
			childPartitionContexts: childInits.map((c) => ({
				parentPartitionContext: pCtxForLog(c.parentPartitionContext),
				newPartitionContext: pCtxForLog(c.newPartitionContext),
				splitType: c.splitType,
			})),
		});
	}

	// RPC erases the KeyBytes brand: keys reach the DO already-encoded as Uint8Array (db.ts encodes at
	// the public entry). Re-brand on this trust boundary without re-encoding. A raw string (e.g. a direct
	// in-process test call) is encoded so the DO always works on canonical KeyBytes.
	private static keyIn(k: string | Uint8Array): KeyBytes {
		return typeof k === "string" ? KeyCodec.encode(k) : KeyCodec.asKeyBytes(k);
	}

	// As keyIn, but an absent key maps to the empty KeyBytes sentinel ([]).
	private static optKeyIn(k: string | Uint8Array | undefined): KeyBytes {
		return k === undefined ? KeyCodec.encodeOptional(undefined) : PartitionDO.keyIn(k);
	}

	private pCtx(): PartitionContextResolved {
		invariant(
			this.#_partitionContext,
			// FIXME Optimize this to be statically generated once only since we call pCtx() often.
			`fokos/partition: partition context not initialized for ${this.ctx.id.toString()}[${this.ctx.id.name}]`,
		);
		return this.#_partitionContext;
	}

	private ensurePartitionContext(pCtx: PartitionContextResolved, isInit = false): PartitionContextResolved {
		// Phantom-bounce guard: a range DO is born ONLY through initFromSplit (promotion creates the root,
		// a split creates children). A request reaching an uninitialized range DO means a caller resolved a
		// fabricated (start,end) name that never existed — never lazy-init it; bounce so the caller falls back
		// to the range root and traverses. (A hash DO may still lazy-init, as today.)
		if (!isInit && !this.#_partitionContext && isRangePartition(pCtx)) {
			throw new Error(
				`fokos/partition: range DO "${pCtx.doName}" is not initialized; route via the range root and traverse (phantom-bounce).`,
			);
		}
		if (this.#_partitionContext) {
			// rangePartition boundaries are KeyBytes — compare by bytes (null = unbounded), never by reference.
			const keyEq = (a: KeyBytes | null | undefined, b: KeyBytes | null | undefined): boolean =>
				a == null || b == null ? a == b : KeyCodec.compare(a, b) === 0;
			// We need to check if the provided context matches the stored one to avoid inconsistencies.
			invariant(
				areImmutableOptionsEqual(this.#_partitionContext, pCtx) &&
					this.#_partitionContext.partitionId === pCtx.partitionId &&
					this.#_partitionContext.doName === pCtx.doName &&
					keyEq(this.#_partitionContext.rangePartition?.hashKey, pCtx.rangePartition?.hashKey) &&
					keyEq(this.#_partitionContext.rangePartition?.startBoundary, pCtx.rangePartition?.startBoundary) &&
					keyEq(this.#_partitionContext.rangePartition?.endBoundary, pCtx.rangePartition?.endBoundary),
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

	private ensureHashTopology(pCtx: PartitionContextResolved): HashPartitionTopologyImpl {
		const topology = this.ensureTopology(pCtx);
		invariant(topology instanceof HashPartitionTopologyImpl, "fokos/partition: expected hash partition topology");
		return topology;
	}

	private ensureTopology(pCtx: PartitionContextResolved): PartitionTopologySplitter {
		if (!this.#_topology) {
			this.#_topology = isRangePartition(pCtx)
				? new RangePartitionTopologyImpl(pCtx, this.ctx)
				: new HashPartitionTopologyImpl(pCtx, this.ctx);
		}
		return this.#_topology;
	}

	private async ensureMigration(op: string, throwIfMigrating = true): Promise<boolean> {
		// TODO Optimize this away by keeping it in memory.
		const migrationStatus = this.ctx.storage.kv.get<PartitionSplitMigrationStatus>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_STATUS);
		if (!migrationStatus || migrationStatus === "migration_completed") {
			return false;
		}
		if (migrationStatus === "migration_initialized") {
			this.ctx.storage.kv.put<PartitionSplitMigrationStatus>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_migrating");
		}
		await this.ensureAlarmSet(Date.now() + PartitionDO.MIGRATION_FALLBACK_ALARM_MS);
		if (throwIfMigrating) {
			// TODO This will reach user requests, so refactor the callers to show something nicer.
			// We can also consider doing a selective migration of the requested keys only.
			throw new Error(`fokos/partition:${op}: Partition split in progress, please retry later.`);
		}
		return true;
	}

	private async forwardToRangeRootPartition<T extends { meta: PartitionInfo }>(
		ctx: PartitionContextResolved,
		hashKey: KeyBytes,
		forward: (stub: PartitionDOStub, pCtx: PartitionContextResolved) => Promise<T>,
	): Promise<T> {
		const { doId, partitionContext: rangeRootCtx } = resolveRangePartitionContext(ctx, hashKey, null, null);
		const rangeRootStub = this.env[ctx.ns].get(doId);
		const result = await forward(rangeRootStub, rangeRootCtx);
		return {
			...result,
			meta: {
				...result.meta,
				forwardCount: result.meta.forwardCount + 1,
				...(isHashPartition(ctx) ? { hashDepth: PartitionIdHelper.depth(this.pCtx()._partitionIdBytes!) } : {}),
			},
		} as T;
	}

	private async maybeForwardToRangeRootPartition<T extends { meta: PartitionInfo }>(
		ctx: PartitionContextResolved,
		hashKey: KeyBytes,
		forward: (stub: PartitionDOStub, pCtx: PartitionContextResolved) => Promise<T>,
	): Promise<T | null> {
		try {
			return await this.forwardToRangeRootPartition(ctx, hashKey, forward);
		} catch (e) {
			if (isPhantomBounceError(e)) {
				return null;
			}
			throw e;
		}
	}

	private async withSplitForwarding<T extends { meta: PartitionInfo }>(opts: {
		ctx: PartitionContextResolved;
		keys: { hashKey: KeyBytes; sortKey: KeyBytes };
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

		if (isHashPartition(ctx)) {
			// Step 1: Authoritative promotion check for keys we promoted or inherited.
			const promotedStatus = this.#promotion.statusFor(hashKey);
			if (promotedStatus === "promoting" || promotedStatus === "promoted") {
				return await this.forwardToRangeRootPartition(ctx, hashKey, forward);
			}

			// Step 2: Speculative bloom filter check — learned promotions from descendants.
			const prt = this.#_partialRangeTopology;
			if (prt?.maybePromoted(hashKey)) {
				const result = await this.maybeForwardToRangeRootPartition(ctx, hashKey, forward);
				if (result) return result;
			}
		}

		const topology = this.ensureTopology(ctx);
		const decision = topology.shouldAllow(hashKey, sortKey);
		switch (decision) {
			case "ok":
				return await local();
			case "forward": {
				const { doId, partitionContext } = topology.pickChildPartition(ctx, hashKey, sortKey);
				const stub = this.env[ctx.ns].get(doId);
				const result = await forward(stub, partitionContext);
				topology.recordForwardResult(hashKey, ctx, partitionContext, result.meta.hashDepth);

				if (isHashPartition(ctx) && PartitionIdHelper.isRangePartition(result.meta.servedByPartitionId)) {
					const prt = this.getOrCreatePartialRangeTopology();
					const learnResult = prt.learnPromotedKey(hashKey);
					if (learnResult === AddResult.Added) {
						this.persistPartialRangeTopology();
					} else if (learnResult === AddResult.Full) {
						console.info({
							...this.logParams(),
							message: "fokos/partition: partial range topology bloom filter is full, " + "cannot learn promoted key.",
							hashKey: KeyCodec.keyForLog(hashKey),
						});
					}
				}

				return {
					...result,
					meta: {
						...result.meta,
						forwardCount: result.meta.forwardCount + 1,
					},
				} as T;
			}
			case "reject":
				throw new Error(`fokos/partition: partition exceeded its limits, please retry later (${operationName}).`);
			default: {
				const _exhaustive: never = decision;
				invariant(false, `fokos/partition.withSplitForwarding: unexpected decision value: ${_exhaustive}`);
			}
		}
	}

	// FIXME: Add PartialRangeTopology bloom filter check for promoted keys in transaction routing
	// (prepare/commit/readForTransaction). Currently only the authoritative PromotionManager is
	// checked. The bloom filter would save hops for keys promoted by descendant partitions, but
	// false positives need careful handling in multi-item transaction flows.
	private groupItemsByRouting<T extends { hashKey: KeyBytes; sortKey?: KeyBytes }>(
		items: T[],
	): {
		local: T[];
		forwarded: Map<string, { pCtx: PartitionContextResolved; items: T[] }>;
		unplaceable: T[];
	} {
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
			if (isHashPartition(pCtx)) {
				const promotedStatus = this.#promotion.statusFor(item.hashKey);
				if (promotedStatus === "promoting" || promotedStatus === "promoted") {
					const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(pCtx, item.hashKey, null, null);
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
		const res = this.#store.getItem(PartitionDO.keyIn(opts.hashKey), PartitionDO.optKeyIn(opts.sortKey));
		const { rowsRead, rowsWritten } = res;
		const result = res.row;
		const actorMeta = {
			rowsRead,
			rowsWritten,
			databaseSize: this.#store.databaseSize,
			servedByActorId: this.ctx.id.toString(),
			servedByActorName: pCtx.doName,
			servedByPartitionId: pCtx.partitionId,
			forwardCount: 0,
			hashDepth: isHashPartition(pCtx) ? PartitionIdHelper.depth(this.pCtx()._partitionIdBytes!) : 0,
		};
		const itemKey = { hashKey: opts.hashKey, sortKey: opts.sortKey };
		if (!result) {
			return { found: false, item: itemKey, meta: actorMeta };
		}
		return {
			found: true,
			item: {
				...itemKey,
				data: result.data,
				ttlEpochUTCSeconds: result.ttl_epoch_utc_seconds ? Number(result.ttl_epoch_utc_seconds) : undefined,
				version: result.v,
			},
			meta: actorMeta,
		};
	}

	// The driver loops live in partition/migration.ts (SplitMigration); the DO only resolves the
	// parent stub (boundary rule: only DO classes and FokosDB acquire stubs) and wires the deps.
	private async runMigration(): Promise<void> {
		const pCtx = this.pCtx();
		const parentCtx = this.ctx.storage.kv.get<PartitionContextResolved>(PartitionDO.KV_KEYS.PARENT_PARTITION_CONTEXT);
		invariant(parentCtx, "fokos/partition.runMigration: no parent partition context stored");

		const parentId = this.env[parentCtx.ns].idFromName(parentCtx.doName);
		const parent: PartitionPeer = this.env[parentCtx.ns].get(parentId);

		const migration = new SplitMigration({
			store: this.#store,
			storage: this.ctx.storage,
			parent,
			logParams: () => this.logParams(),
			onPromotedKeyInherited: (_hashKey, _status) => {},
			beforeComplete: async () => {
				await this.__testing__beforeMigrationComplete?.();
			},
		});
		await migration.runMigration(pCtx, parentCtx);
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
				const migrationStatus = this.ctx.storage.kv.get<PartitionSplitMigrationStatus>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_STATUS);
				if (migrationStatus === "migration_initialized" || migrationStatus === "migration_migrating") {
					if (migrationStatus === "migration_initialized") {
						this.ctx.storage.kv.put<PartitionSplitMigrationStatus>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_migrating");
					}
					await tryWhile(
						async () => {
							await this.runMigration();
						},
						(_error, nextAttempt) => nextAttempt <= 5,
					);
				}
			} catch (error) {
				console.error({
					...this.logParams(),
					message: "fokos/partition: Migration job failed.",
					error: String(error),
					errorProps: error,
				});
			}

			/////////////////////////////////////////////////////
			// ── Job: Partition split (for parent partitions)
			const topology = this.ensureTopology(this.pCtx());
			try {
				const splitStatus = topology.splitStatus();
				if (splitStatus?.status === "split_queued") {
					console.log({
						...this.logParams(),
						message: "fokos/partition: Running split process.",
						splitStatus: { status: splitStatus.status, splitType: splitStatus.splitType },
					});
					await tryWhile(
						async () => {
							await this.runSplit(topology);
						},
						(_error, nextAttempt) => nextAttempt <= 5,
					);
				}
			} catch (error) {
				console.error({
					...this.logParams(),
					message: "fokos/partition: Split job failed.",
					error: String(error),
					errorProps: error,
				});
			}

			////////////////////////////////////////
			// ── Job: Stale transaction recovery
			try {
				const staleTxRows = this.#participant.listStaleTransactions(PartitionDO.STALE_TX_MS, 10);
				for (const row of staleTxRows) {
					if (!row.coordinator_do_id) continue;
					try {
						const tcId = this.env.TRANSACTION_COORDINATOR_DO.idFromString(row.coordinator_do_id);
						const result = await this.env.TRANSACTION_COORDINATOR_DO.get(tcId).recoverTransaction(row.transaction_id);

						if (result.state === "COMMITTED") {
							const pendingRows = this.#store.listPendingTxItems(row.transaction_id);
							if (pendingRows.length > 0) {
								const transactionTimestamp = pendingRows[0].transaction_ts;
								const items: TransactionItem[] = pendingRows.map((r) => ({
									hashKey: r.hk,
									sortKey: r.sk,
									operation: r.operation as TransactionItem["operation"],
									data: r.data ?? undefined,
								}));
								await this.commit(this.pCtx(), {
									transactionId: row.transaction_id,
									transactionTimestamp,
									items,
								});
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

			///////////////////////////////////////////////////////////////////////////
			// ── Jobs: Promotion drive and GC (hash partitions only, not routers)
			const pCtx = this.pCtx();
			if (isHashPartition(pCtx)) {
				// Drive: advance each queued key through init → cutover → migrate.
				await this.#promotion.drive(pCtx, () => this.ensureTopology(pCtx).splitStatus());

				// GC: delete local items and pending_transactions for fully-promoted keys.
				this.#promotion.runGC();
			}
		} catch (error) {
			console.error({
				...this.logParams(),
				message: "fokos/partition: Background work failed with unexpected error.",
				error: String(error),
				errorProps: error,
			});
		} finally {
			/////////////////////////////////////////////////
			// Check if any job needs to set the next alarm!
			/////////////////////////////////////////////////

			let nextAlarmMs: number | null = null;
			const wantAlarm = (ms: number) => {
				if (nextAlarmMs === null || ms < nextAlarmMs) nextAlarmMs = ms;
			};
			this.#store.transactionSync(() => {
				// Job: Partition migration for child partitions.
				const postStatus = this.ctx.storage.kv.get<PartitionSplitMigrationStatus>(MIGRATION_KV_KEYS.SPLIT_MIGRATION_STATUS);
				if (postStatus === "migration_migrating") {
					wantAlarm(Date.now() + PartitionDO.MIGRATION_FALLBACK_ALARM_MS);
				}

				// Job: Split process for parent partitions.
				if (this.ensureTopology(this.pCtx()).splitStatus()?.status === "split_queued") {
					wantAlarm(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
				}

				// Jobs: Promotion drive (queued keys) and GC (promoted keys with residual items).
				if (this.#promotion.needsBackgroundWork()) {
					wantAlarm(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
				}

				// Job: Stale transaction recovery.
				if (this.#store.pendingTxTotalCount() > 0) {
					wantAlarm(Date.now() + PartitionDO.STALE_TX_MS);
				}
			});

			if (nextAlarmMs !== null) {
				await this.ensureAlarmSet(nextAlarmMs);
				// Schedule background work to ensure progress without waiting for the alarm.
				this.scheduleBackgroundWork({ delayMs: 10, forceSchedule: true });
			} else {
				console.log({
					...this.logParams(),
					message: "fokos/partition: Background work ran, nothing to schedule forward.",
				});
			}

			this.__testing__backgroundWorkRunning = false;
		}
	}

	async destroyPartition(): Promise<void> {
		console.warn({
			...this.logParams(),
			message: "fokos/partition: Destroying partition — deleting all storage.",
		});

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

	private getOrCreatePartialRangeTopology(): PartialRangeTopology {
		if (!this.#_partialRangeTopology) {
			this.#_partialRangeTopology = PartialRangeTopology.create({
				errorRate: 0.01,
				// I want this to not be more than about 1MB, but we give 1.5MB for extra buffer.
				// It has to be less than 2MB to fit into the SQLite row size limit (2MB) for the serialized bloom filter.
				//
				// Here's the growth until we cross 1 MB:
				//    node ./tools/bloom-filter-sizing.js 300000 2MB
				//
				// Initial capacity: 300,000 items | Max size: 1.00 MB | Error rate: 0.01
				//
				// Layer      Capacity   Per-layer FPR        Size   Running Total  k
				// -------------------------------------------------------------------
				// 0           300,000         0.5000%    403.8 KB        403.8 KB   8
				// 1           600,000         0.2500%    913.4 KB         1.29 MB   9
				// 2         1,200,000         0.1250%     1.99 MB         3.28 MB  10
				//
				maxSizeBytes: 1.5 * 1024 * 1024,
				// WARNING: This cannot change after the first addition of something in the bloom filter!
				initialCapacityN: 300_000,
			});
		}
		return this.#_partialRangeTopology;
	}

	private persistPartialRangeTopology(): void {
		if (this.#_partialRangeTopology) {
			this.ctx.storage.kv.put<PartialRangeTopologySnapshot>(
				PartitionDO.KV_KEYS.PARTIAL_RANGE_TOPOLOGY,
				this.#_partialRangeTopology.toSnapshot(),
			);
		}
	}

	private logParams() {
		return {
			actorId: this.ctx.id.toString(),
			// This might truncated to 1024 bytes in Cloudflare Workers, but the full one should be inside partitionContext.doName.
			actorName: this.ctx.id.name,
			// Always put the partition context in the logs for better debugging, even if it's undefined.
			// KeyBytes fields are rendered via keyForLog so they never appear as bare Uint8Array.
			partitionContext: pCtxForLog(this.#_partitionContext),
		};
	}
}
