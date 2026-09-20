import { DurableObject } from "cloudflare:workers";
import { DataKind, OperationMetrics, type QuerySelect, type ReturnValuesOnConditionCheckFailure } from "../shared/types.js";
import type { CompiledConditionPlan } from "../shared/expression/plan.js";
import type {
	CancelRequest,
	CancelResponse,
	CommitRequest,
	CommitResponse,
	DebugForceResolveTransactionRequest,
	DebugForceResolveTransactionResponse,
	ParticipantOperationResultEncoded,
	PrepareRequest,
	PrepareResponse,
	ReadForTransactionRequest,
	ReadForTransactionResponse,
	ReadSnapshotRequest,
	ReadSnapshotResponse,
	RejectionReasonEncoded,
	SingleShotRequest,
	SingleShotResponse,
	TransactionItem,
} from "../shared/transaction-wire-types.js";
import {
	areImmutableOptionsEqual,
	areMutableOptionsEqual,
	assertCtxHasIdBytes,
	isHashPartition,
	isRangePartition,
	pCtxForLog,
	PartitionContext,
	PartitionContextResolved,
	PartitionContextLivePartition,
} from "../shared/partition-topology/partition-context.js";
import {
	PartitionIdHelper,
	resolveHashChildPartitionContexts,
	resolveRangePartitionContext,
} from "../shared/partition-topology/partition-id.js";
import { KeyCodec, type KeyBytes } from "../shared/partition-topology/key-codec.js";
import {
	HashPartitionTopologyImpl,
	PartitionTopologySplitter,
	RANGE_PROMOTION_FRACTION,
	RangePartitionTopologyImpl,
	type OperationIntent,
} from "../shared/partition-topology/split-policy.js";
import type { PartitionInfoInternal, RangeAncestorInfo, SplitType } from "../shared/partition-topology/types.js";
import { forwardedMeta, learnFromErrorMeta, routedError, stampRoutingMeta } from "../shared/partition-topology/forward-meta.js";
import { tryWhile } from "durable-utils/retries";
import invariant from "../shared/invariant.js";
import { collectBatch } from "../shared/partition/batch-scan.js";
import type { CompiledProjectionPlan, CompiledQueryPlan } from "../shared/expression/plan.js";
import type { ProjectedWireRow } from "../shared/expression/projection.js";
import {
	estimateItemBytes,
	estimatePendingTxBytes,
	estimateProjectedRowBytes,
	PartitionStore,
	type StoredItem,
	type ScanCursor,
	type PromotedKeyStatus,
	type RepartitionState,
	type RepartitionTargetRow,
} from "../shared/partition/partition-store.js";
import { TransactionParticipant } from "../shared/partition/transaction-participant.js";
import type { PromotionCandidate } from "../shared/partition/transaction-participant.js";
import { TtlExpiry, type TtlSweepConfig } from "../shared/partition/ttl-expiry.js";
import { assertPointInSlice, clipQueryToSlice } from "../shared/partition/repartition/repartition-slice.js";
import { FokosMigrationHost } from "../shared/partition/fokos-migration-host.js";
import {
	RepartitionSource,
	RepartitionTarget,
	REPARTITION_KV_KEYS,
	type RepartitionCommonDeps,
	type RepartitionSourceDeps,
	type RepartitionTargetDeps,
} from "../shared/partition/repartition/repartition-flow.js";
import type {
	FokosImportState,
	FokosInitRequest,
	FokosMigrationAckRequest,
	FokosMigrationPage,
	FokosMigrationPullRequest,
	FokosPartitionStatusRpc,
	FokosPrepareDestroyRequest,
	FokosStartImportRequest,
	FokosStatusPage,
	FokosStatusRequest,
} from "../shared/partition/repartition/repartition-types.js";
import { AddResult } from "../shared/bloom-filter.js";
import { PartialRangeTopology, type PartialRangeTopologySnapshot } from "../shared/partition-topology/partial-range-topology.js";
import {
	clipToChildRange,
	cursorFallsInChild,
	isChildFullyBeforeCursor,
	makeBoundaryCursor,
	rangeIntersects,
	type SkInterval,
} from "../shared/query/sk-interval.js";
import { QueryPageBudget } from "../shared/query/page-budget.js";
import { collectQueryPage } from "../shared/query/query-collector.js";
import { DESTROY_ABORT_SENTINEL, getColoInfo, type ColoInfo } from "../shared/cf-utils.js";
import { partitionStub, partitionStubByName, txCoordinatorStub } from "../shared/do-stubs.js";
import {
	applyImageCap,
	conditionFailedReason,
	decodeItemKeys,
	IDEMPOTENCY_WINDOW_MS,
	txOrderTimestampNow,
} from "../shared/transaction-limits.js";
import {
	CONFLICT_CODES,
	FokosConflictError,
	FokosError,
	FokosInternalError,
	FokosRoutingError,
	FokosUnavailableError,
	INTERNAL_CODES,
	ROUTING_CODES,
	UNAVAILABLE_CODES,
} from "../shared/errors.js";

export interface PartitionAPI {
	apiPutItem(ctx: PartitionContext, req: PutItemRpcRequest): Promise<PutItemRpcResponse>;
	apiGetItem(ctx: PartitionContext, req: GetItemRpcRequest): Promise<GetItemRpcResponse>;
	apiDeleteItem(ctx: PartitionContext, req: DeleteItemRpcRequest): Promise<DeleteItemRpcResponse>;
	apiQueryItems(ctx: PartitionContext, req: QueryItemsRpcRequest): Promise<QueryItemsRpcResponse>;
}

// ─── item RPC types ───────────────────────────────────────────────────────────

/**
 * Wire types for the item RPCs (db.ts → PartitionDO). Keys are canonical KeyBytes, encoded at the
 * db.ts entry, and `sortKey` is always present — the empty KeyBytes ([]) is the absent sentinel.
 * This matches the transaction and query RPCs, so every key crossing into a DO has one form.
 *
 * No response carries a key: `db.ts` answers with the caller's own keys, which are the only ones the
 * caller can recognise.
 */
export type ItemRpcKeys = { hashKey: KeyBytes; sortKey: KeyBytes };

export type PutItemRpcRequest = ItemRpcKeys & {
	/** Encoded at the db.ts boundary (json ⇒ JSON text). */
	data: string | Uint8Array;
	kind: DataKind;
	ttlAt?: number;
	condition?: CompiledConditionPlan;
	returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;
};

export type PutItemRpcResponse =
	| { outcome: "ok"; version: number; meta: OperationMetrics & PartitionInfoInternal }
	| {
			outcome: "rejected";
			reason: RejectionReasonEncoded;
			meta: OperationMetrics & PartitionInfoInternal;
	  };

export type DeleteItemRpcRequest = ItemRpcKeys & {
	condition?: CompiledConditionPlan;
	returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;
};

export type DeleteItemRpcResponse =
	| { outcome: "ok"; deleted: boolean; meta: OperationMetrics & PartitionInfoInternal }
	| {
			outcome: "rejected";
			reason: RejectionReasonEncoded;
			meta: OperationMetrics & PartitionInfoInternal;
	  };

export type GetItemRpcRequest = ItemRpcKeys & { projection?: CompiledProjectionPlan };

// json data is JSON text here; db.ts parses it once at the public boundary. The type is free of the
// recursive JsonValue so the Workers-RPC type machinery does not instantiate infinitely deep.
//
// A projected read carries the positional row inside `item`, so `kind`, `version`, and `ttlAt` are
// common to both found variants. `kind` is then `"projected"`, which is a read-result tag and never
// a stored `data_kind`.
export type GetItemRpcResponse =
	| {
			found: true;
			item: { data: string | Uint8Array; kind: DataKind; ttlAt?: number; version: number };
			meta: OperationMetrics & PartitionInfoInternal;
	  }
	| {
			found: true;
			item: { projected: ProjectedWireRow; kind: "projected"; ttlAt?: number; version: number };
			meta: OperationMetrics & PartitionInfoInternal;
	  }
	| { found: false; meta: OperationMetrics & PartitionInfoInternal };

// ─── queryItems internal types ────────────────────────────────────────────────

export type { SkInterval } from "../shared/query/sk-interval.js";
export type { ScanCursor } from "../shared/partition/partition-store.js";
export type { ProjectedWireRow } from "../shared/expression/projection.js";

export type QueryItemsRpcRequest = {
	hashKey: KeyBytes;
	interval: SkInterval;
	direction: "asc" | "desc";
	remainingEvaluatedItems: number;
	remainingEvaluatedBytes: number;
	remainingResponseBytes: number;
	/** Leaf partitions this request may still visit before it stops with a boundary cursor. Bounds the cross-DO fan-out of one page. */
	remainingPartitionVisits: number;
	/** True until any leaf of the page materialized an item. Lets the first item of a page exceed the response budget. */
	allowOversizedFirstItem: boolean;
	cursor: ScanCursor | null;
	select: QuerySelect;
	/** The compiled filter/projection plan, or null for a request with neither. */
	plan: CompiledQueryPlan | null;
};

export type QueryItemsRpcResponse = {
	/** One shape per request: complete items, or positional projected rows. The client narrows by the plan it sent. */
	items: Array<StoredItem | ProjectedWireRow>;
	/** Matched items in this response. */
	count: number;
	/** Evaluated items in this response. */
	scannedCount: number;
	/** Stored bytes of the evaluated items, charged to the evaluated-byte budget. */
	evaluatedBytes: number;
	/** Estimated RPC bytes of the materialized items, charged to the response-byte budget. */
	responseBytes: number;
	/** SQL result rows that the leaf scans consumed in JavaScript. */
	rowsReturned: number;
	/** The last candidate that entered the logical page, or null when none did. */
	lastEvaluatedCursor: ScanCursor | null;
	nextCursor: ScanCursor | null;
	/**
	 * The serving DO's own bookkeeping record (servedBy*, hashDepth) — NOT part of the public
	 * partitionMetas. Its `forwardCount` is subtree-cumulative: withSplitForwarding adds 1 per hash hop,
	 * and a range router adds its child fan-out plus every descendant router's forwards.
	 */
	meta: OperationMetrics & PartitionInfoInternal;
	/** Leaf-only debugging trail: hash leaves and non-split range partitions that actually scanned rows. Routers (hash or range) are excluded. */
	partitionMetas: Array<OperationMetrics & PartitionInfoInternal>;
};

// ─── read-through types ───────────────────────────────────────────────────────

export type { FokosPartitionRef, FokosExecuteLocalRequest } from "../shared/partition/repartition/repartition-types.js";
import type { FokosExecuteLocalRequest } from "../shared/partition/repartition/repartition-types.js";

// ─────────────────────────────────────────────────────────────────────────────

// Minimal structural type used in withSplitForwarding to avoid a recursive type cycle:
// DurableObjectStub<PartitionDO> → PartitionDO → withSplitForwarding → DurableObjectStub<PartitionDO>.
export type PartitionDOStub = {
	apiPutItem(ctx: PartitionContextResolved, req: PutItemRpcRequest): Promise<PutItemRpcResponse>;
	apiGetItem(ctx: PartitionContextResolved, req: GetItemRpcRequest): Promise<GetItemRpcResponse>;
	apiDeleteItem(ctx: PartitionContextResolved, req: DeleteItemRpcRequest): Promise<DeleteItemRpcResponse>;
	apiQueryItems(ctx: PartitionContextResolved, req: QueryItemsRpcRequest): Promise<QueryItemsRpcResponse>;

	txPrepare(ctx: PartitionContextResolved, request: PrepareRequest): Promise<PrepareResponse>;
	txCommit(ctx: PartitionContextResolved, request: CommitRequest): Promise<CommitResponse>;
	txCancel(ctx: PartitionContextResolved, request: CancelRequest): Promise<CancelResponse>;
	txReadForTransaction(ctx: PartitionContextResolved, request: ReadForTransactionRequest): Promise<ReadForTransactionResponse>;
	txReadSnapshot(ctx: PartitionContextResolved, request: ReadSnapshotRequest): Promise<ReadSnapshotResponse>;
	txExecuteSingleShot(ctx: PartitionContextResolved, request: SingleShotRequest): Promise<SingleShotResponse>;
	debugForceResolveTransaction(
		ctx: PartitionContextResolved,
		request: DebugForceResolveTransactionRequest,
	): Promise<DebugForceResolveTransactionResponse>;
	debugForcePromoteKey(ctx: PartitionContextResolved, hashKey: KeyBytes): Promise<DebugForcePromoteKeyResponse>;
};

export type DebugForcePromoteKeyResponse = {
	/** False when the key already had a promotion entry, so this call changed nothing. */
	queued: boolean;
	/** The key's promotion status after the call. */
	status: PromotedKeyStatus | undefined;
};

export class PartitionDO extends DurableObject implements PartitionAPI, FokosPartitionStatusRpc {
	private static readonly KV_KEYS = {
		PARTITION_CONTEXT: "__partition_context",

		// Updated on splits and key promotions.
		PARTITION_DEPTH: "__partition_depth",

		PARTIAL_RANGE_TOPOLOGY: "__partial_range_topology",
	};

	private static readonly STALE_TX_MS = 5_000;
	private static readonly MIGRATION_FALLBACK_ALARM_MS = 10_000;
	private static readonly SPLIT_FALLBACK_ALARM_MS = 5_000;
	/** How far ahead a work pass arms its fallback, before it changes state or calls an RPC. */
	private static readonly WORK_FALLBACK_ALARM_MS = 5_000;
	private static readonly IMPORT_PAGES_PER_PASS = 16;
	/** Both bounds of one `fokosStatus` page: the entry count, and the estimated serialized size. */
	private static readonly STATUS_PAGE_ENTRIES = 1_000;
	private static readonly STATUS_PAGE_BYTES = 20 * 1024 * 1024;

	private readonly STRING_PCTX_INIT_ERROR = `fokos/partition: partition context not initialized for ${this.ctx.id.toString()}[${this.ctx.id.name}]`;

	#store: PartitionStore;
	#participant: TransactionParticipant;
	#source: RepartitionSource;
	#target: RepartitionTarget;
	#ttl: TtlExpiry;

	#_partitionContext?: PartitionContextLivePartition;
	#_topology?: PartitionTopologySplitter;
	#_partialRangeTopology: PartialRangeTopology | null = null;
	#_backgroundWorkScheduledAt: number | null = null;
	/**
	 * The one background pass in flight.
	 *
	 * A timer, an alarm and a request all reach `runBackgroundWork`. Two passes over one import each
	 * hold a page the other has moved past. The promise lives in memory only, so an eviction loses it.
	 * The durable guards inside each transition make the work safe. This field only stops the waste.
	 */
	#_backgroundInFlight: Promise<void> | null = null;
	// Best-effort telemetry: which Cloudflare colo this isolate runs in. Populated
	// non-blocking from the constructor, so it may be undefined for the first few
	// requests after the DO wakes. Never gate correctness on it.
	#_coloInfo?: ColoInfo;

	// Local-only, per-DO state (never sent as ordinary routing context): applies uniformly to hash
	// DOs too — they simply keep [] forever, since nothing ever writes this for a hash partition.
	#_rangeAncestors: RangeAncestorInfo[] = [];

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.#store = new PartitionStore(ctx.storage);
		this.#participant = new TransactionParticipant({ store: this.#store });
		const repartitionDeps = this.repartitionDeps();
		this.#source = new RepartitionSource(this.#store, ctx.storage, repartitionDeps);
		this.#target = new RepartitionTarget(this.#store, ctx.storage, repartitionDeps);
		this.#ttl = new TtlExpiry({
			store: this.#store,
			canSweep: () => this.ttlCanSweep(),
			logParams: () => this.logParams(),
			config: () => this.fokosTtlConfig(),
		});
		void ctx.blockConcurrencyWhile(async () => {
			this.#store.runMigrations();

			// Load partition context from storage.
			const pCtx = ctx.storage.kv.get<PartitionContextLivePartition>(PartitionDO.KV_KEYS.PARTITION_CONTEXT);
			if (pCtx) {
				pCtx._partitionIdBytes = Uint8Array.fromHex(pCtx.partitionId);
				this.#_partitionContext = pCtx;

				if (isRangePartition(pCtx) && this.depth() > 0) {
					// Append non-root "self".
					this.#_rangeAncestors = this.#store.getRangeAncestors(pCtx.rangePartition.hashKey, this.depth()).concat({
						depth: this.depth(),
						startBoundary: pCtx.rangePartition.startBoundary ?? KeyCodec.encodeOptional(undefined),
						endBoundary: pCtx.rangePartition.endBoundary ?? KeyCodec.encodeOptional(undefined),
					});
				}

				const prtSnap = ctx.storage.kv.get<PartialRangeTopologySnapshot>(PartitionDO.KV_KEYS.PARTIAL_RANGE_TOPOLOGY);
				if (prtSnap) {
					this.#_partialRangeTopology = PartialRangeTopology.fromSnapshot(prtSnap);
				}
			}
		});
		this.#ttl.arm(this.fokosTtlConfig().initialDelayMs);

		// Best-effort, non-blocking: record the colo this isolate lives in for telemetry.
		// It swallows the errors, because telemetry must never affect the lifecycle of the DO.
		setTimeout(() => {
			void this.fokosGetColoInfo()
				.then((info) => {
					this.#_coloInfo = info;
				})
				.catch(() => {});
		}, 0);
	}

	// ═══ the repartition control RPCs ════════════════════════════════════════

	/**
	 * Creates this partition as the target of a repartition, or confirms an identical earlier call.
	 *
	 * Only this call creates a range partition. Only this call tells a hash child that it exists,
	 * before its first user request arrives. A client must not call it.
	 */
	async fokosInit(req: FokosInitRequest): Promise<void> {
		return await this.#rpc("fokosInit", async () => await this.#target.initAsTarget(req));
	}

	/** Asks this target to start its import now, instead of at its own fallback alarm. */
	async fokosStartImport(req: FokosStartImportRequest): Promise<void> {
		return await this.#rpc("fokosStartImport", async () => await this.#target.startImport(req));
	}

	/** Serves one bounded migration page to a target that is still catching up. */
	async fokosMigrationPull(req: FokosMigrationPullRequest): Promise<FokosMigrationPage> {
		return await this.#rpc("fokosMigrationPull", async () => this.#source.servePage(req));
	}

	/** Records that one target holds a complete copy of its slice. */
	async fokosMigrationAck(req: FokosMigrationAckRequest): Promise<void> {
		return await this.#rpc("fokosMigrationAck", async () => this.#source.acceptAck(req));
	}

	/**
	 * Fences this partition for destroy. Every background transition stops, and no new target appears.
	 *
	 * The traversal reads the target links after this call returns. The fence must therefore hold
	 * before the pass that can add a target ends. One transaction writes the fence and the optional
	 * root bootstrap. The call then waits for the pass in flight. That pass re-reads the fence before
	 * each remaining step, so the alarm this method cancels stays cancelled. A repeated call succeeds.
	 */
	async fokosPrepareDestroy(req: FokosPrepareDestroyRequest): Promise<void> {
		return await this.#rpc("fokosPrepareDestroy", async () => await this.#fokosPrepareDestroy(req));
	}

	async #fokosPrepareDestroy(req: FokosPrepareDestroyRequest): Promise<void> {
		this.#store.transactionSync(() => {
			if (req.rootContext) this.ensurePartitionContext(req.rootContext);
			this.ctx.storage.kv.put<boolean>(REPARTITION_KV_KEYS.DESTROYING, true);
		});
		// A failed pass is a stopped pass, and the fence is already durable. Its error must not fail the
		// destroy that deletes this partition whole.
		await this.#_backgroundInFlight?.catch(() => {});
		// After the pass, never before it: the end of a pass can re-arm both of these.
		this.#ttl.disarm();
		await this.ctx.storage.deleteAlarm();
	}

	/**
	 * One bounded page of every repartition this partition holds, with the target links inside it.
	 *
	 * A destroy traversal walks this view, so it reports every target row. A `pending` target has had
	 * no initialization call and is a leaf. An `initializing` target can already hold storage of its
	 * own. A root request carries its context and bootstraps an empty root. A target request omits the
	 * context and must never create an empty partition.
	 */
	async fokosStatus(req: FokosStatusRequest): Promise<FokosStatusPage> {
		return await this.#rpc("fokosStatus", async () => this.#fokosStatus(req));
	}

	#fokosStatus(req: FokosStatusRequest): FokosStatusPage {
		if (req.rootContext) this.ensurePartitionContext(req.rootContext);
		const destroying = this.isDestroying();
		const pCtx = this.#_partitionContext;
		if (!pCtx) {
			return { initialized: false, destroying, partitionContext: null, importState: null, entries: [], nextCursor: null };
		}
		const { entries, nextCursor } = this.#source.statusEntries(req.cursor, PartitionDO.STATUS_PAGE_ENTRIES, PartitionDO.STATUS_PAGE_BYTES);
		return { initialized: true, destroying, partitionContext: pCtx, importState: this.#target.importState(), entries, nextCursor };
	}

	/**
	 * What both halves of the repartition flow need from this Durable Object.
	 *
	 * One object serves both halves. The source reads the parts it declares, and the target reads its
	 * own. The constructor type of each half keeps them apart. Everything that acquires a stub, reads
	 * application data, or sets the alarm lives here, because the flow does none of that.
	 */
	private repartitionDeps(): RepartitionSourceDeps & RepartitionTargetDeps {
		const common: RepartitionCommonDeps = {
			// Boundary rule: only DO classes and FokosDB hold stubs.
			getPeer: (ref) => partitionStubByName(this.env, this.pCtx(), ref.doName),
			host: new FokosMigrationHost({ store: this.#store, hashSplitN: () => this.pCtx().hashSplitN }),
			identity: () => ({ pCtx: this.pCtx(), depth: this.depth(), rangeAncestors: this.#_rangeAncestors }),
			// Forced, because the flow calls this when work has just become due: a queued repartition, an
			// acknowledgement that completed one, or a start notification. The scheduler drops an unforced
			// request while another one is pending, which leaves the new work until the next alarm.
			scheduleWork: () => this.scheduleBackgroundWork({ delayMs: 10, forceSchedule: true }),
			logParams: () => this.logParams(),
		};
		return {
			...common,
			hasIdentity: () => this.#_partitionContext !== undefined,
			applyTargetIdentity: (req) => this.applyTargetIdentity(req),
			ensureAlarmSet: async (targetMs) => await this.ensureAlarmSet(targetMs),

			computeRangeBoundaries: (hashKey, start, end, n) => this.#store.computeRangeSplitBoundaries(hashKey, start, end, n),
			lockCountForKey: (hashKey) => this.#store.pendingLockCountForHashKey(hashKey),
			cleanupStep: (hashKey) => {
				this.#store.deleteItemsBatchForHashKey(hashKey, 1000);
				this.#store.deletePendingTxForHashKey(hashKey);
				if (this.#store.hasItemsForHashKey(hashKey)) return false;
				this.#store.deleteKeySizeEstimate(hashKey);
				return true;
			},
			onSplitCompleted: () => this.#store.deleteAllPendingTx(),
		};
	}

	/**
	 * Writes this partition's identity, depth and range ancestors from a `fokosInit`. It is
	 * synchronous by contract: the flow calls it inside the transaction that writes the import record.
	 */
	private applyTargetIdentity(req: FokosInitRequest): void {
		const pCtx = this.ensurePartitionContext(req.target, /* isInit */ true);
		if (isRangePartition(pCtx)) {
			invariant(req.rangeDepth !== undefined, "fokos/partition.fokosInit: a range target needs its depth");
			this.ctx.storage.kv.put<number>(PartitionDO.KV_KEYS.PARTITION_DEPTH, req.rangeDepth);
			this.#_depth = req.rangeDepth;
			if (req.rangeAncestors && req.rangeAncestors.length > 0) {
				invariant(req.rangeDepth > 0, "fokos/partition.fokosInit: only a non-root range partition has ancestors");
				this.#store.setRangeAncestors(pCtx.rangePartition.hashKey, req.rangeAncestors);
				// Append non-root "self".
				this.#_rangeAncestors = req.rangeAncestors.concat({
					depth: req.rangeDepth,
					startBoundary: pCtx.rangePartition.startBoundary ?? KeyCodec.encodeOptional(undefined),
					endBoundary: pCtx.rangePartition.endBoundary ?? KeyCodec.encodeOptional(undefined),
				});
			}
		}
		this.depth(); // populate #_depth
	}

	//////////////////////////////
	// User overridable methods.
	//////////////////////////////

	/**
	 * How long a prepared transaction may sit on this partition before the stale sweep asks its
	 * coordinator to resolve it, and how far ahead the sweep's alarm is set. Read at each use, so a
	 * subclass can vary it.
	 */
	fokosStaleTransactionMs(): number {
		return PartitionDO.STALE_TX_MS;
	}

	/**
	 * Overrideable method to get the location info.
	 */
	async fokosGetColoInfo(): Promise<ColoInfo> {
		if (this.env.FOKOS_SHOULD_FETCH_COLO_INFO) {
			return await getColoInfo();
		}
		return { cfColo: "", cfLoc: "", cfFl: "" };
	}

	protected fokosTtlConfig(): TtlSweepConfig {
		return {
			chunkSize: 100,
			sleepMs: 1000,
			maxRowsBeforeSleep: 10_000,
			maxBytesBeforeSleep: 50 * 1024 * 1024,
			maxRowsPerCycle: 100_000,
			initialDelayMs: 500,
		};
	}

	/**
	 * Both sweeps ask the same questions, because both need complete local state. An importing target
	 * does not hold it yet. A split router no longer holds it: its targets own the keys and sweep
	 * their own rows. A fenced partition is on its way out and must make no transition.
	 */
	private canSweepLocally(): boolean {
		if (!this.#_partitionContext) return false;
		if (this.isDestroying()) return false;
		return !this.#target.isImporting() && !this.#source.routerRole();
	}

	private ttlCanSweep(): boolean {
		return this.canSweepLocally();
	}

	private txPendingCanSweep(): boolean {
		return this.canSweepLocally();
	}

	///////////////////////////////
	// API methods (PartitionAPI)
	///////////////////////////////

	/**
	 * INTERNAL ONLY FOR TESTING.
	 */
	async status(pCtx?: PartitionContextLivePartition) {
		return await this.#rpc("status", async () => await this.#status(pCtx));
	}

	/**
	 * The compatibility view. It keeps the shape the partition suites already read.
	 *
	 * Nothing persists this view. Every field below comes from `fokos_repartitions`, its target rows,
	 * and `__fokos/import`. A destroy traversal reads the paginated `fokosStatus` instead, which
	 * reports every target row and not only the targets of a split.
	 */
	async #status(pCtx?: PartitionContextLivePartition) {
		// Only a test passes pCtx. In production the public API initializes the DO before this call.
		pCtx = pCtx ? this.ensurePartitionContext(pCtx) : this.#_partitionContext;
		const importRecord = this.#target.importRecord();
		return {
			depth: this.depth(),
			partitionContext: pCtx,
			partitionContextStored: this.ctx.storage.kv.get<PartitionContextLivePartition>(PartitionDO.KV_KEYS.PARTITION_CONTEXT),
			splitStatus: pCtx ? this.derivedSplitStatus(pCtx) : undefined,
			migrationStatus: derivedMigrationStatus(importRecord?.state),
			parentPartitionContext: importRecord?.source,
			parentSplitType: importRecord ? (importRecord.slice.kind === "hash_child" ? "hash" : "range") : undefined,
			promotedKeys: this.derivedPromotedKeys(),
		};
	}

	/** The split lifecycle, as the old KV record described it. */
	private derivedSplitStatus(pCtx: PartitionContextLivePartition): SplitStatusView | undefined {
		const row = this.#source.splitRepartition();
		if (!row) return undefined;
		const splitType: SplitType = row.kind === "hash_split" ? "hash" : "range";
		const status =
			row.state === "queued" || row.state === "planned" ? "split_queued" : row.state === "cutover" ? "split_started" : "split_completed";
		if (status === "split_queued") {
			return { status, splitType, createdAt: row.queuedAt, partitionContext: pCtx };
		}
		const targets = this.#source.splitTargets();
		const history: Extract<SplitStatusView, { history: unknown }>["history"] = [
			{ status: "split_queued", splitType, createdAt: row.queuedAt, partitionContext: pCtx },
		];
		if (status === "split_completed" && row.cutoverAt !== null) {
			history.push({ status: "split_started", splitType, createdAt: row.cutoverAt, partitionContext: pCtx });
		}
		return {
			status,
			splitType,
			createdAt: (status === "split_started" ? row.cutoverAt : row.completedAt) ?? row.queuedAt,
			partitionContext: pCtx,
			// Built from THIS partition's current context, never from a snapshot taken at split time.
			childPartitionContexts: targets.map((t) => this.targetContext(pCtx, t)),
			migratedChildDoNames: targets.filter((t) => t.acknowledged).map((t) => t.doName),
			history,
		};
	}

	/** The promotion lifecycle of every key this partition has moved or inherited. */
	private derivedPromotedKeys(): { hashKey: KeyBytes; status: PromotedKeyStatus }[] {
		const out: { hashKey: KeyBytes; status: PromotedKeyStatus }[] = [];
		let cursor = null as { seq: number; targetIndex: number } | null;
		for (;;) {
			const page = this.#source.statusEntries(cursor, 1000);
			for (const entry of page.entries) {
				if (entry.repartition.kind !== "key_promotion") continue;
				if (entry.target !== null && entry.target.index !== 0) continue;
				const row = this.#store.getRepartition(entry.repartition.id);
				if (!row?.hashKey) continue;
				out.push({ hashKey: row.hashKey, status: promotedKeyStatusOf(entry.repartition.state) });
			}
			if (!page.nextCursor) return out;
			cursor = page.nextCursor;
		}
	}

	/** Rebuilds one target's context from this partition's CURRENT context and the target's stored slice. */
	private targetContext(pCtx: PartitionContextLivePartition, target: RepartitionTargetRow): PartitionContextResolved {
		if (target.slice.kind === "hash_child") {
			const child = resolveHashChildPartitionContexts(pCtx).find((c) => c.partitionId === target.partitionId);
			invariant(child, () => `fokos/partition: no hash child matches target ${target.doName}`);
			return child;
		}
		const slice = target.slice;
		const start = slice.kind === "range" ? slice.start : null;
		const end = slice.kind === "range" ? slice.end : null;
		return resolveRangePartitionContext(pCtx, slice.hashKey, start, end).partitionContext;
	}

	async apiPutItem(pCtx: PartitionContextResolved, req: PutItemRpcRequest): Promise<PutItemRpcResponse> {
		return await this.#rpc("apiPutItem", async () => await this.#apiPutItem(pCtx, req));
	}

	async #apiPutItem(pCtx: PartitionContextResolved, req: PutItemRpcRequest): Promise<PutItemRpcResponse> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("putItem");
		const { hashKey, sortKey } = req;
		return await this.withSplitForwarding<PutItemRpcResponse>({
			ctx: pCtx,
			keys: { hashKey, sortKey },
			operationName: "putItem",
			intent: "write",
			forward: async (stub, pCtx) => await stub.apiPutItem(pCtx, req),
			local: async () => {
				const wantsImage = req.returnValuesOnConditionCheckFailure === "all_old";
				const localRes = this.#store.transactionSync(() => {
					const pendingRow = this.#store.pendingLockFor(hashKey, sortKey);
					if (pendingRow) {
						// FIXME: ATC §4 describes optimizations where a non-tx write can proceed using a
						// higher timestamp to force the pending tx to abort on commit, avoiding this rejection.
						throw itemLockedError(pendingRow.transaction_id, hashKey, sortKey);
					}

					const conditionRes = req.condition ? this.#store.evaluateCondition(req.condition, hashKey, sortKey) : null;
					if (conditionRes && !conditionRes.conditionOk) {
						const image = wantsImage && conditionRes.itemPresent ? this.#store.getItemImage(hashKey, sortKey) : undefined;
						return { outcome: "rejected" as const, conditionRes, image };
					}

					const writeRes = this.#store.upsertItem({
						hk: hashKey,
						sk: sortKey,
						data: req.data,
						kind: req.kind,
						ttlAt: req.ttlAt ?? null,
						txOrderTs: txOrderTimestampNow(),
					});
					return { outcome: "ok" as const, writeRes, conditionRes };
				});

				if (localRes.outcome === "rejected") {
					return {
						outcome: "rejected",
						reason: conditionFailedReason(decodeItemKeys(hashKey, sortKey), localRes.image?.row),
						meta: this.localMeta(pCtx, localRes.image ? sumSqlMetrics(localRes.conditionRes, localRes.image) : localRes.conditionRes),
					};
				}

				const { writeRes, conditionRes } = localRes;
				// The row is committed, so this method logs and drops a failure of the signals below. The
				// next write to this partition repeats both checks. A failure raised here would instead
				// tell the caller that a write which DID apply did not.
				//
				// Promotions run before splits: a key that has grown past its own cap must get its own
				// range tree, and an unfinished promotion blocks the split behind it.
				try {
					await this.queuePromotionIfOverThreshold(pCtx, hashKey, writeRes.keyEstBytes);
					await this.checkSplits(pCtx, hashKey, sortKey);
				} catch (error) {
					console.error({
						...this.logParams(),
						message: "fokos/partition.putItem: the post-write split check failed after the write applied.",
						error: String(error),
						errorProps: error,
					});
				}
				return {
					outcome: "ok",
					version: writeRes.version,
					meta: this.localMeta(pCtx, conditionRes ? sumSqlMetrics(conditionRes, writeRes) : writeRes),
				};
			},
		});
	}

	async apiDeleteItem(pCtx: PartitionContextResolved, req: DeleteItemRpcRequest): Promise<DeleteItemRpcResponse> {
		return await this.#rpc("apiDeleteItem", async () => await this.#apiDeleteItem(pCtx, req));
	}

	async #apiDeleteItem(pCtx: PartitionContextResolved, req: DeleteItemRpcRequest): Promise<DeleteItemRpcResponse> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("deleteItem");
		const { hashKey, sortKey } = req;
		return await this.withSplitForwarding<DeleteItemRpcResponse>({
			ctx: pCtx,
			keys: { hashKey, sortKey },
			operationName: "deleteItem",
			intent: "delete",
			forward: async (stub, pCtx) => await stub.apiDeleteItem(pCtx, req),
			local: async () => {
				const wantsImage = req.returnValuesOnConditionCheckFailure === "all_old";
				const localRes = this.#store.transactionSync(() => {
					const pendingRow = this.#store.pendingLockFor(hashKey, sortKey);
					if (pendingRow) {
						// FIXME: ATC §4 optimization — see same comment in putItem.
						throw itemLockedError(pendingRow.transaction_id, hashKey, sortKey);
					}

					const conditionRes = req.condition ? this.#store.evaluateCondition(req.condition, hashKey, sortKey) : null;
					if (conditionRes && !conditionRes.conditionOk) {
						const image = wantsImage && conditionRes.itemPresent ? this.#store.getItemImage(hashKey, sortKey) : undefined;
						return { outcome: "rejected" as const, conditionRes, image };
					}

					// Keep the deletion transaction order watermark consistent with transactional deletes.
					const writeRes = this.#store.deleteItem({ hk: hashKey, sk: sortKey, txOrderTs: txOrderTimestampNow() });
					return { outcome: "ok" as const, writeRes, conditionRes };
				});

				if (localRes.outcome === "rejected") {
					return {
						outcome: "rejected",
						reason: conditionFailedReason(decodeItemKeys(hashKey, sortKey), localRes.image?.row),
						meta: this.localMeta(pCtx, localRes.image ? sumSqlMetrics(localRes.conditionRes, localRes.image) : localRes.conditionRes),
					};
				}

				const { writeRes, conditionRes } = localRes;
				return {
					outcome: "ok",
					deleted: writeRes.deleted,
					meta: this.localMeta(pCtx, conditionRes ? sumSqlMetrics(conditionRes, writeRes) : writeRes),
				};
			},
		});
	}

	async apiGetItem(pCtx: PartitionContextResolved, req: GetItemRpcRequest): Promise<GetItemRpcResponse> {
		return await this.#rpc("apiGetItem", async () => await this.#apiGetItem(pCtx, req));
	}

	async #apiGetItem(pCtx: PartitionContextResolved, req: GetItemRpcRequest): Promise<GetItemRpcResponse> {
		this.ensurePartitionContext(pCtx);

		if (await this.ensureMigration("getItem", false)) {
			// Read through the source while this target still imports its share of the data.
			const record = this.#target.importRecord();
			invariant(record, "fokos/partition.getItem: no import record while importing");
			const sourceStub = partitionStubByName(this.env, record.source, record.source.doName);
			const result = (await sourceStub.fokosExecuteLocal({
				op: "getItem",
				repartitionId: record.repartitionId,
				caller: { partitionId: pCtx.partitionId, doName: pCtx.doName },
				request: req,
			})) as GetItemRpcResponse;
			// The parent returns its own hashDepth, but the caller forwarded to this child partition.
			// recordForwardResult on the caller requires responseHashDepth >= toAbsDepth (this child's depth).
			if (isHashPartition(pCtx)) {
				return {
					...result,
					meta: { ...result.meta, hashDepth: this.depth() },
				};
			}
			return result;
		}

		return await this.withSplitForwarding<GetItemRpcResponse>({
			ctx: pCtx,
			keys: { hashKey: req.hashKey, sortKey: req.sortKey },
			operationName: "getItem",
			intent: "read",
			forward: async (stub, pCtx) => await stub.apiGetItem(pCtx, req),
			local: async () => await this.readItemLocally(pCtx, req),
		});
	}

	/**
	 * Serves one read for a repartition target that is still importing from this partition.
	 *
	 * A target cannot serve its own reads until its copy is complete, and it cannot ask this source
	 * through the ordinary API either: the source would route the request straight back to the target
	 * that sent it. This method reads local rows only, with no forwarding and no lifecycle gate.
	 *
	 * That makes the caller check load-bearing rather than cosmetic. The source resolves, from its own
	 * durable records, which slice the caller owns, and answers only for that slice — otherwise a
	 * target could read a sibling's keys, or read keys this partition has already promoted away, whose
	 * local rows are stale or already collected. A caller it cannot place is rejected outright.
	 */
	async fokosExecuteLocal(req: FokosExecuteLocalRequest): Promise<GetItemRpcResponse | QueryItemsRpcResponse> {
		return await this.#rpc("fokosExecuteLocal", async () => await this.#fokosExecuteLocal(req));
	}

	async #fokosExecuteLocal(req: FokosExecuteLocalRequest): Promise<GetItemRpcResponse | QueryItemsRpcResponse> {
		const pCtx = this.pCtx();
		const slice = this.#source.resolveCallerSlice(req.repartitionId, req.caller);

		if (req.op === "getItem") {
			const getReq = req.request;
			assertPointInSlice(slice, getReq.hashKey, getReq.sortKey, pCtx.hashSplitN, "fokosExecuteLocal");
			// A promoted key's rows live in the range tree; the local copies are stale or already collected.
			// Follow the promotion with ordinary forwarding rather than reading them. Only a hash-child
			// caller can reach one — a range or promoted-key slice is itself inside a range tree.
			if (slice.kind === "hash_child" && this.#source.ownedByRangeTree(getReq.hashKey)) {
				return await this.forwardToRangeRootPartition<GetItemRpcResponse>(
					pCtx,
					getReq.hashKey,
					async (stub, toCtx) => await stub.apiGetItem(toCtx, getReq),
					getReq.sortKey,
				);
			}
			return this.readItemLocally(pCtx, getReq);
		}

		const queryReq = req.request;
		const interval = clipQueryToSlice(slice, queryReq, pCtx.hashSplitN, "fokosExecuteLocal");
		if (slice.kind === "hash_child" && this.#source.ownedByRangeTree(queryReq.hashKey)) {
			// A query spans sort keys, so it carries no single key that could resolve a deeper range
			// slice; it enters at the range root and the routers below it fan out.
			return await this.forwardToRangeRootPartition<QueryItemsRpcResponse>(
				pCtx,
				queryReq.hashKey,
				async (stub, toCtx) => await stub.apiQueryItems(toCtx, { ...queryReq, interval }),
			);
		}
		return this.queryItemsLocal(pCtx, { ...queryReq, interval });
	}

	async apiQueryItems(pCtx: PartitionContextResolved, req: QueryItemsRpcRequest): Promise<QueryItemsRpcResponse> {
		return await this.#rpc("apiQueryItems", async () => await this.#apiQueryItems(pCtx, req));
	}

	async #apiQueryItems(pCtx: PartitionContextResolved, req: QueryItemsRpcRequest): Promise<QueryItemsRpcResponse> {
		this.ensurePartitionContext(pCtx);

		// If still migrating, read directly from the parent (mirrors getItem / getItemDirect).
		if (await this.ensureMigration("queryItems", false)) {
			const record = this.#target.importRecord();
			invariant(record, "fokos/partition.queryItems: no import record while importing");
			const sourceStub = partitionStubByName(this.env, record.source, record.source.doName);
			const result = (await sourceStub.fokosExecuteLocal({
				op: "queryItems",
				repartitionId: record.repartitionId,
				caller: { partitionId: pCtx.partitionId, doName: pCtx.doName },
				request: req,
			})) as QueryItemsRpcResponse;
			if (isHashPartition(pCtx)) {
				const myDepth = this.depth();
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
		return await this.withSplitForwarding<QueryItemsRpcResponse>({
			ctx: pCtx,
			keys: { hashKey: req.hashKey, sortKey: KeyCodec.encodeOptional(undefined) },
			operationName: "queryItems",
			intent: "read",
			forward: async (stub, childPCtx) => await stub.apiQueryItems(childPCtx, req),
			local: async () => await this.queryItemsLocal(this.pCtx(), req),
		});
	}

	private queryItemsLocal(pCtx: PartitionContextResolved, req: QueryItemsRpcRequest): QueryItemsRpcResponse {
		const hk = req.hashKey;
		const { interval, cursor } = req;

		const lower = interval.lower?.value ?? KeyCodec.encodeOptional(undefined);
		const lowerInclusive = interval.lower?.inclusive ?? true;
		const upper = interval.upper?.value ?? null;
		const upperInclusive = interval.upper?.inclusive ?? false;

		const scan = this.#store.scanQueryPage({
			hk,
			lower,
			lowerInclusive,
			upper,
			upperInclusive,
			cursor,
			direction: req.direction,
			// One row beyond the budget tells a stopped page from a drained interval.
			limit: Math.max(0, req.remainingEvaluatedItems) + 1,
			select: req.select,
			plan: req.plan,
		});
		const page = collectQueryPage({
			rows: scan.rows,
			hashKey: hk,
			select: req.select,
			budget: req,
			estimateResponseBytes: (item) => (Array.isArray(item) ? estimateProjectedRowBytes(item) : estimateItemBytes(item)),
		});
		const { rowsRead, rowsWritten } = scan.sqlMetrics();

		// A leaf (hash leaf or non-split range partition) is the only kind of DO that scans rows, so it
		// is the only kind that contributes a `partitionMetas` entry. Routers (hash or range) are
		// excluded — they appear only numerically via `forwardCount`.
		const meta: OperationMetrics & PartitionInfoInternal = {
			rowsRead,
			rowsWritten,
			databaseSize: this.#store.databaseSize,
			servedByActorId: this.ctx.id.toString(),
			servedByActorName: pCtx.doName,
			servedByPartitionId: pCtx.partitionId,
			forwardCount: 0,
			hashDepth: isHashPartition(pCtx) ? this.depth() : 0,
			rangeDepth: isRangePartition(pCtx) ? this.depth() : 0,
			_internal: {
				rangeAncestors: this.#_rangeAncestors,
			},
		};

		return {
			items: page.items,
			count: page.count,
			scannedCount: page.scannedCount,
			evaluatedBytes: page.evaluatedBytes,
			responseBytes: page.responseBytes,
			rowsReturned: page.rowsReturned,
			lastEvaluatedCursor: page.lastEvaluatedCursor,
			nextCursor: page.nextCursor,
			meta,
			partitionMetas: [meta],
		};
	}

	private async queryItemsAsRangeNode(pCtx: PartitionContextResolved, req: QueryItemsRpcRequest): Promise<QueryItemsRpcResponse> {
		if (!this.#source.routerRole()) return this.queryItemsLocal(pCtx, req);
		// Built from this router's CURRENT context and each target's stored boundaries, in target_index
		// order, which is ascending boundary order. A stored context would hand the child the split
		// thresholds of an earlier operator setting, and the child would then persist them as its own.
		const children = this.#source.splitTargets().map((t) => this.targetContext(this.pCtx(), t));
		return await this.walkRangeChildren(pCtx, children, req);
	}

	private async walkRangeChildren(
		pCtx: PartitionContextResolved,
		children: PartitionContextResolved[],
		req: QueryItemsRpcRequest,
	): Promise<QueryItemsRpcResponse> {
		const { interval, cursor, direction } = req;
		const budget = new QueryPageBudget(req);

		const allItems: Array<StoredItem | ProjectedWireRow> = [];
		// Only leaf entries accumulate here — a range router (this node) and any deeper routers
		// contribute nothing of their own; they're captured numerically via `forwardCount`.
		const leafMetas: Array<OperationMetrics & PartitionInfoInternal> = [];
		let nextCursor: ScanCursor | null = null;
		let count = 0;
		let scannedCount = 0;
		let evaluatedBytes = 0;
		let responseBytes = 0;
		let rowsReturned = 0;
		let lastEvaluatedCursor: ScanCursor | null = null;
		let childrenCalled = 0;
		// Sum of forwards performed by descendant routers, so this node's `forwardCount` is cumulative.
		let descendantForwards = 0;

		// Children are stored in ascending boundary order; reverse for desc.
		const orderedChildren = direction === "desc" ? [...children].reverse() : children;

		// The children that can contribute to this page: they intersect the query interval, and they are
		// not entirely behind the resume cursor. Selecting them up front — instead of skipping inside the
		// scan loop — turns "could a later child still contribute?" into a plain index test, which is the
		// question BOTH budget exits must answer before they emit a continuation cursor.
		const candidates = orderedChildren.flatMap((childCtx) => {
			const rp = childCtx.rangePartition;
			invariant(rp, "fokos/partition.walkRangeChildren: child has no rangePartition context");
			const childStart = rp.startBoundary ?? KeyCodec.encodeOptional(undefined);
			const childEnd = rp.endBoundary;
			if (!rangeIntersects(childStart, childEnd, interval)) return [];
			if (cursor && isChildFullyBeforeCursor(childStart, childEnd, cursor, direction)) return [];
			return [{ childCtx, rp, childStart, childEnd }];
		});

		for (let i = 0; i < candidates.length; i++) {
			const { childCtx, rp, childStart, childEnd } = candidates[i];
			// A cursor is honest only if a later child still holds rows for this query. Without this, a
			// budget exhausted by the LAST child — one that drained itself and reported no cursor of its
			// own — would still hand the client a cursor, buying it one more round trip that returns zero
			// items. `db.ts:queryItems` applies the same rule across sub-queries.
			const hasLaterCandidate = i < candidates.length - 1;

			const childCursor = cursor && cursorFallsInChild(childStart, childEnd, cursor) ? cursor : null;
			const clippedInterval = clipToChildRange(interval, rp.startBoundary, childEnd);
			const childStub = this.getChildStub(childCtx);
			const childResult = await childStub.apiQueryItems(childCtx, {
				...req,
				interval: clippedInterval,
				remainingEvaluatedItems: budget.remainingEvaluatedItems,
				remainingEvaluatedBytes: budget.remainingEvaluatedBytes,
				remainingResponseBytes: budget.remainingResponseBytes,
				remainingPartitionVisits: budget.remainingPartitionVisits,
				allowOversizedFirstItem: budget.allowOversizedFirstItem,
				cursor: childCursor,
			});

			if (req.select === "projection") {
				allItems.push(...childResult.items);
			}
			leafMetas.push(...childResult.partitionMetas);
			descendantForwards += childResult.meta.forwardCount;
			count += childResult.count;
			scannedCount += childResult.scannedCount;
			evaluatedBytes += childResult.evaluatedBytes;
			responseBytes += childResult.responseBytes;
			rowsReturned += childResult.rowsReturned;
			lastEvaluatedCursor = childResult.lastEvaluatedCursor ?? lastEvaluatedCursor;
			budget.consume(childResult);
			childrenCalled++;

			if (childResult.nextCursor !== null) {
				nextCursor = childResult.nextCursor;
				break;
			}
			// The child drained as a shared budget reached zero: resume strictly after the last
			// evaluated candidate (a leaf cursor carries no `inclusive` flag).
			if (budget.budgetExhausted) {
				if (hasLaterCandidate && lastEvaluatedCursor) nextCursor = lastEvaluatedCursor;
				break;
			}
			if (budget.visitsExhausted && hasLaterCandidate) {
				console.warn(
					`fokos/partition.walkRangeChildren: remainingPartitionVisits reached (${req.remainingPartitionVisits}), emitting boundary cursor`,
				);
				nextCursor = makeBoundaryCursor(req.hashKey, childStart, childEnd, direction);
				break;
			}
		}

		// This range router is a pure router: it reads no rows and is NOT listed in `partitionMetas`.
		// Its `meta` exists only for routing bookkeeping (servedBy*, hashDepth) and to carry the
		// subtree-cumulative `forwardCount` (its own child fan-out plus every descendant router's).
		const meta: OperationMetrics & PartitionInfoInternal = {
			rowsRead: 0,
			rowsWritten: 0,
			databaseSize: this.#store.databaseSize,
			servedByActorId: this.ctx.id.toString(),
			servedByActorName: pCtx.doName,
			servedByPartitionId: pCtx.partitionId,
			forwardCount: childrenCalled + descendantForwards,
			hashDepth: isHashPartition(pCtx) ? this.depth() : 0,
			rangeDepth: isRangePartition(pCtx) ? this.depth() : 0,
			_internal: {
				rangeAncestors: this.#_rangeAncestors,
			},
		};

		return {
			items: allItems,
			count,
			scannedCount,
			evaluatedBytes,
			responseBytes,
			rowsReturned,
			lastEvaluatedCursor,
			nextCursor,
			meta,
			partitionMetas: leafMetas,
		};
	}

	/**
	 * Asks the flow to queue a split when this partition has grown past its cap.
	 *
	 * The topology decides the size question. Arbitration decides whether the split can start, and
	 * only the transaction inside `queue` answers that without a race against another queue request.
	 * A refused request is normal, because an unfinished promotion still owns the move of a key. The
	 * next write asks again.
	 */
	private async checkSplits(pCtx: PartitionContextResolved, hashKey?: KeyBytes, sortKey?: KeyBytes): Promise<void> {
		const splitType = this.ensureTopology(pCtx).shouldSplit(hashKey, sortKey);
		if (!splitType) return;
		await this.ensureAlarmSet(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
		const row = this.#source.queue({ kind: splitType === "hash" ? "hash_split" : "range_split" });
		if (!row) return;
		console.log({ ...this.logParams(), message: "fokos/partition: Split conditions met.", repartitionId: row.id, kind: row.kind });
		await this.wakeRepartitionWork();
	}

	/**
	 * Queues a promotion for a key that has grown past its share of the cap of the partition.
	 *
	 * It runs after the item transaction commits, never inside it, because `queue` opens a transaction
	 * of its own. The write has already succeeded, so the caller logs and drops a failure here. The
	 * next write to the same key asks again.
	 */
	private async queuePromotionIfOverThreshold(pCtx: PartitionContextResolved, hashKey: KeyBytes, keyEstBytes: number): Promise<void> {
		if (!isHashPartition(pCtx)) return;
		const threshold = (pCtx.hashSplitConditions.maxSizeMb ?? 0) * RANGE_PROMOTION_FRACTION * 1024 * 1024;
		if (threshold <= 0 || keyEstBytes < threshold) return;
		await this.ensureAlarmSet(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
		const row = this.#source.queue({ kind: "key_promotion", hashKey });
		if (!row) return;
		console.log({
			...this.logParams(),
			message: "fokos/partition: Key queued for promotion.",
			hashKey: KeyCodec.keyForLog(hashKey),
			repartitionId: row.id,
			keyEstBytes,
		});
		await this.wakeRepartitionWork();
	}

	/**
	 * Wakes the background pass for a repartition this request just queued.
	 *
	 * Both halves are needed. The timer runs the pass in this isolate within milliseconds, and that
	 * pass then arms the real deadline of the queued row. The durable alarm survives an eviction
	 * between this method and that timer. A later write must not stand in for the alarm, because
	 * `queue` refuses a second repartition and returns before it reaches this method.
	 */
	private async wakeRepartitionWork(): Promise<void> {
		await this.ensureAlarmSet(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
		this.scheduleBackgroundWork({ delayMs: 10, forceSchedule: true });
	}

	/**
	 * Queues a promotion for each key that the transaction which just applied grew past its share.
	 *
	 * The candidates arrive as an argument, so they belong to THIS transaction and to no other. An
	 * apply that rolled back or threw returns none, and nothing outlives the call.
	 *
	 * It keeps the largest candidate per key: one transaction can write many sort keys of one hash
	 * key, and each upsert reports the running total after its own row.
	 */
	private async drainPromotionCandidates(pCtx: PartitionContextResolved, candidates: readonly PromotionCandidate[]): Promise<void> {
		if (candidates.length === 0) return;
		const largest = new Map<string, PromotionCandidate>();
		for (const candidate of candidates) {
			const id = candidate.hashKey.toBase64({ alphabet: "base64url" });
			const seen = largest.get(id);
			if (!seen || candidate.keyEstBytes > seen.keyEstBytes) largest.set(id, candidate);
		}
		for (const { hashKey, keyEstBytes } of largest.values()) {
			await this.queuePromotionIfOverThreshold(pCtx, hashKey, keyEstBytes);
		}
	}

	async destroyPartition(): Promise<void> {
		return await this.#rpc("destroyPartition", async () => await this.#destroyPartition());
	}

	async #destroyPartition(): Promise<void> {
		this.#ttl.disarm();
		console.warn({
			...this.logParams(),
			message: "fokos/partition: Destroying partition — deleting all storage.",
		});

		await this.ctx.blockConcurrencyWhile(async () => {
			// Clears all the timeouts: setTimeout returns a numeric ID that increments on each call, so the
			// newest ID gives the upper bound to clear from.
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

		// Evict the DO instance so the next caller gets a fresh one with re-ran migrations.
		// This throws on the caller side with the sentinel message, which FokosDB.destroy() catches and ignores.
		this.ctx.abort(DESTROY_ABORT_SENTINEL);
		// await this.ctx.blockConcurrencyWhile(async () => {
		// 	throw new Error("__special_destroy_sentinel");
		// });
	}

	////////////////////////
	// TRANSACTION HELPERS
	////////////////////////

	async txPrepare(pCtx: PartitionContextResolved, request: PrepareRequest): Promise<PrepareResponse> {
		return await this.#rpc("txPrepare", async () => await this.#txPrepare(pCtx, request));
	}

	async #txPrepare(pCtx: PartitionContextResolved, request: PrepareRequest): Promise<PrepareResponse> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("prepare");

		const { local, forwarded } = this.groupItemsByRouting(request.items, "write", "prepare");

		type SubTask = { items: TransactionItem[]; promise: Promise<PrepareResponse> };
		const tasks: SubTask[] = [];
		for (const [, { pCtx: childPCtx, items }] of forwarded) {
			tasks.push({
				items,
				promise: this.getChildStub(childPCtx).txPrepare(childPCtx, { ...request, items }),
			});
		}
		if (local.length > 0) {
			tasks.push({
				items: local,
				promise: this.prepareLocal({ ...request, items: local }),
			});
		}
		if (tasks.length === 0) return { outcome: "accepted" };

		const responses = await Promise.all(tasks.map((t) => t.promise));

		// An execution failure belongs to no operation and outranks every per-operation rejection, so
		// it travels up as it arrived, without an array. It is the one answer that carries none.
		const executionFailure = responses.find((r) => r.outcome === "rejected" && !r.results);
		if (executionFailure) return executionFailure;

		if (!responses.some((r) => r.outcome === "rejected")) return { outcome: "accepted" };

		// This node answers for every operation it was given, whichever child evaluated it. A child
		// that accepted sends no array, so its operations passed.
		const mergedResults: ParticipantOperationResultEncoded[] = [];
		for (let i = 0; i < tasks.length; i++) {
			const resp = responses[i];
			if (resp.outcome === "accepted") {
				for (const item of tasks[i].items) {
					mergedResults.push({ outcome: "passed", opIndex: item.opIndex });
				}
			} else {
				mergedResults.push(...resp.results);
			}
		}

		applyImageCap(mergedResults);
		return { outcome: "rejected", results: mergedResults };
	}

	private async prepareLocal(request: PrepareRequest): Promise<PrepareResponse> {
		const response = this.#participant.prepareLocal(request);

		if (response.outcome === "accepted") {
			await this.ensureAlarmSet(Date.now() + this.fokosStaleTransactionMs());
		}

		return response;
	}

	async txCommit(pCtx: PartitionContextResolved, request: CommitRequest): Promise<CommitResponse> {
		return await this.#rpc("txCommit", async () => await this.#txCommit(pCtx, request));
	}

	async #txCommit(pCtx: PartitionContextResolved, request: CommitRequest): Promise<CommitResponse> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("commit"); // reject while this partition is migrating

		// The coordinator has already decided this transaction, and commit cannot grow the partition:
		// prepare persisted the payload into pending_transactions, so commit moves those bytes into
		// `items` and drops the pending row. Size backpressure here would wedge a decided transaction.
		const { local, forwarded } = this.groupItemsByRouting(request.items, "ignore_size_reject", "commit");

		const tasks: Promise<CommitResponse>[] = [];
		for (const [, { pCtx: childPCtx, items }] of forwarded) {
			tasks.push(this.getChildStub(childPCtx).txCommit(childPCtx, { ...request, items }));
		}
		const localResult = local.length > 0 ? this.#participant.commitLocal({ ...request, items: local }) : undefined;
		if (localResult) {
			this.ctx.waitUntil(
				(async () => {
					// Transactional writes grow a partition, so they have to be able
					// to queue a split too — the background job only RUNS a split that is already queued, it
					// never queues one. Without this, a workload that writes only through transactions grows
					// without ever splitting.
					//
					// This block absorbs a throw. The coordinator has already decided this transaction and the
					// items are already applied, so a failed commit would wedge a decided transaction over
					// bookkeeping that the next write repeats.
					try {
						this.wakeLockBlockedPromotion();
						await this.drainPromotionCandidates(pCtx, localResult.promotionCandidates);
						await this.checkSplits(pCtx);
					} catch (error) {
						console.error({
							...this.logParams(),
							message: "fokos/partition.commit: split check failed after the transaction applied.",
							transactionId: request.transactionId,
							error: String(error),
							errorProps: error,
						});
					}
				})(),
			);
		}

		// Wait for the child commit tasks to settle before returning the overall outcome.
		const childResults = await Promise.allSettled(tasks);

		const childFailure = childResults.find((r): r is PromiseRejectedResult => r.status === "rejected");
		if (childFailure) throw childFailure.reason;
		return { outcome: "committed" };
	}

	/**
	 * Releases this transaction's locks in this partition and descendants that own `request.items`.
	 *
	 * The release is by transaction id, not by key, so this node is fully cleared regardless of which
	 * keys it owns — including a parent mid-split, which is also the routing entry point, so routing
	 * the fan-out opens no split-window gap. The keys only decide WHERE ELSE the cancel goes, and
	 * routing is exact: a lock follows its key through a split, and a promotion cutover cannot happen
	 * while a key is locked. With no keys (see CancelRequest.items) the cancel is local-only and any
	 * descendant lock waits for its own stale-tx recovery alarm.
	 */
	async txCancel(pCtx: PartitionContextResolved, request: CancelRequest): Promise<CancelResponse> {
		return await this.#rpc("txCancel", async () => await this.#txCancel(pCtx, request));
	}

	async #txCancel(pCtx: PartitionContextResolved, request: CancelRequest): Promise<CancelResponse> {
		this.ensurePartitionContext(pCtx);
		// reject while this partition is migrating - it will recover it on its own.
		await this.ensureMigration("cancel");
		// First, so that the local lock is released even when a child cancel fails and throws below.
		this.#participant.cancelLocal(request.transactionId);
		this.ctx.waitUntil(
			(async () => {
				// The lock is gone either way, so a failed wake must not stop the child cancels below. The
				// promotion still moves on its own retry deadline.
				try {
					this.wakeLockBlockedPromotion();
				} catch (error) {
					console.error({
						...this.logParams(),
						message: "fokos/partition.cancel: waking the lock-blocked promotion failed after the local cancel.",
						transactionId: request.transactionId,
						error: String(error),
						errorProps: error,
					});
				}
			})(),
		);

		// Cancel only DELETEs pending rows, so size backpressure must not wedge it — same reasoning as
		// txCommit, and cancel is the path that BRINGS an over-size partition back under its cap.
		const { forwarded } = this.groupItemsByRouting(request.items, "ignore_size_reject", "cancel");
		if (forwarded.size === 0) {
			return { outcome: "cancelled" };
		}

		const results = await Promise.allSettled(
			[...forwarded.values()].map(({ pCtx: childPCtx, items }) => this.getChildStub(childPCtx).txCancel(childPCtx, { ...request, items })),
		);
		const failures = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");
		if (failures.length > 0) {
			console.error({
				...this.logParams(),
				message: "fokos/partition.cancel: some child txCancel failed",
				transactionId: request.transactionId,
				failureCount: failures.length,
			});
			throw new FokosInternalError(INTERNAL_CODES.partition_fanout_failed, {
				message: "some child txCancel failed",
				cause: failures[0].reason,
				attributes: { transactionId: request.transactionId, failureCount: failures.length },
			});
		}

		return { outcome: "cancelled" };
	}

	async debugForceResolveTransaction(
		pCtx: PartitionContextResolved,
		request: DebugForceResolveTransactionRequest,
	): Promise<DebugForceResolveTransactionResponse> {
		return await this.#rpc("debugForceResolveTransaction", async () => {
			this.ensurePartitionContext(pCtx);
			await this.ensureMigration("debugForceResolveTransaction");
			const pendingRows = this.#store.listPendingTxItems(request.transactionId);
			const items = pendingRows.map((pending) => ({ hashKey: pending.hk, sortKey: pending.sk }));
			const response =
				request.outcome === "commit"
					? await this.txCommit(pCtx, {
							transactionId: request.transactionId,
							transactionTimestamp: pendingRows[0]?.transaction_ts ?? txOrderTimestampNow(),
							items,
						})
					: await this.txCancel(pCtx, { transactionId: request.transactionId, items });
			this.#store.clearPendingTxGuard(request.transactionId);
			return response;
		});
	}

	/**
	 * Promotes `hashKey` to its own range structure now, instead of waiting for the key to grow past
	 * `hashSplitConditions.maxSizeMb * RANGE_PROMOTION_FRACTION`.
	 *
	 * This is the deterministic entry point to a flow that is otherwise driven by a size heuristic: an
	 * operator can move a known hot key ahead of its growth. It only queues the work — the same
	 * background cycle performs the cutover, the range root migration and the acknowledgement, so the
	 * key reaches "promoted" through exactly the path a size-triggered promotion takes.
	 *
	 * Idempotent: a key that already has a promotion entry comes back with `queued: false`.
	 *
	 * Only the partition that owns the rows of the key can queue it. A split parent forwards to the
	 * child that owns the key, because a promotion from a router would migrate a stale snapshot and
	 * then shadow the live rows in the child. An importing child rejects the call with
	 * `partition_migrating` and creates no row. The caller can retry after that import completes.
	 */
	async debugForcePromoteKey(pCtx: PartitionContextResolved, hashKey: KeyBytes): Promise<DebugForcePromoteKeyResponse> {
		return await this.#rpc("debugForcePromoteKey", async () => {
			this.ensurePartitionContext(pCtx);
			await this.ensureMigration("debugForcePromoteKey");
			const existing = this.#source.overrideFor(hashKey);
			if (existing !== undefined) {
				if (existing === "queued" || existing === "planned" || existing === "cutover") {
					await this.ensureAlarmSet(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
				}
				return { queued: false, status: promotedKeyStatusOf(existing) };
			}

			// routeSingleDestination, not the local record. After cutover the key belongs to a hash child,
			// and a promotion queued on the router would migrate a snapshot and then shadow the live rows.
			const route = this.routeSingleDestination([{ hashKey }], "ignore_size_reject", "debugForcePromoteKey");
			if (route.destination === "child") {
				return await this.getChildStub(route.pCtx).debugForcePromoteKey(route.pCtx, hashKey);
			}

			await this.ensureAlarmSet(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
			const row = this.#source.queue({ kind: "key_promotion", hashKey });
			if (!row) {
				// This partition still owns the key, and arbitration refused: a split row exists, so the key
				// moves soon. This is the answer a queued split has always given.
				throw errExceededDatabaseSize("debugForcePromoteKey");
			}
			await this.ensureAlarmSet(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
			this.scheduleBackgroundWork({ delayMs: 10, forceSchedule: true });
			return { queued: true, status: promotedKeyStatusOf(row.state) };
		});
	}

	async txReadForTransaction(pCtx: PartitionContextResolved, request: ReadForTransactionRequest): Promise<ReadForTransactionResponse> {
		return await this.#rpc("txReadForTransaction", async () => await this.#txReadForTransaction(pCtx, request));
	}

	async #txReadForTransaction(pCtx: PartitionContextResolved, request: ReadForTransactionRequest): Promise<ReadForTransactionResponse> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("readForTransaction");

		const { local, forwarded } = this.groupItemsByRouting(request.items, "read", "readForTransaction");

		const tasks: Promise<ReadForTransactionResponse>[] = [];
		for (const [, { pCtx: childPCtx, items }] of forwarded) {
			tasks.push(this.getChildStub(childPCtx).txReadForTransaction(childPCtx, { ...request, items }));
		}
		if (local.length > 0) {
			tasks.push(Promise.resolve(this.#participant.readForTransactionLocal({ ...request, items: local })));
		}
		const results = await Promise.all(tasks);
		return { items: results.flatMap((r) => r.items) };
	}

	/**
	 * The single-partition fast path for `transactGetItems`: one round trip, no coordinator, no
	 * locks, and nothing persisted.
	 *
	 * A partition DO is single-threaded and reads the whole set with no `await` in between, so the
	 * result already IS a consistent snapshot — the second phase of the coordinator's read exists
	 * only to detect interleaving ACROSS partitions, and here there is none to detect.
	 */
	async txReadSnapshot(pCtx: PartitionContextResolved, request: ReadSnapshotRequest): Promise<ReadSnapshotResponse> {
		return await this.#rpc("txReadSnapshot", async () => await this.#txReadSnapshot(pCtx, request));
	}

	async #txReadSnapshot(pCtx: PartitionContextResolved, request: ReadSnapshotRequest): Promise<ReadSnapshotResponse> {
		this.ensurePartitionContext(pCtx);
		await this.ensureMigration("readSnapshot");

		const route = this.routeSingleDestination(request.items, "read", "readSnapshot");
		if (route.destination === "none") {
			return { outcome: "not_applicable" };
		}
		if (route.destination === "child") {
			return await this.getChildStub(route.pCtx).txReadSnapshot(route.pCtx, { items: route.items });
		}

		const { items } = this.#participant.readForTransactionLocal({ items: route.items });
		// Parity with the two-phase path: an item locked by an in-progress transaction has a write that
		// may or may not land, so the read cannot claim a committed snapshot.
		if (items.some((item) => item.hasPendingWrite)) {
			return { outcome: "aborted", reason: "pending_write" };
		}
		return { outcome: "committed", items };
	}

	/**
	 * The single-partition fast path for `transactWriteItems`: one round trip, no coordinator, no
	 * lock rows, no alarms and no state left behind. The partition validates and applies the whole
	 * set inside one storage transaction, which is where its atomicity comes from.
	 *
	 * There is no `await` between the routing decision and the apply. A split can only advance at an
	 * input-gate point, so an unbroken synchronous block closes the split and promotion races: the
	 * items cannot start belonging to another DO between the check and the write.
	 */
	async txExecuteSingleShot(pCtx: PartitionContextResolved, request: SingleShotRequest): Promise<SingleShotResponse> {
		return await this.#rpc("txExecuteSingleShot", async () => await this.#txExecuteSingleShot(pCtx, request));
	}

	async #txExecuteSingleShot(pCtx: PartitionContextResolved, request: SingleShotRequest): Promise<SingleShotResponse> {
		this.ensurePartitionContext(pCtx);
		invariant(request.items.length > 0, "fokos/partition.executeSingleShot: at least one item is required");
		await this.ensureMigration("executeSingleShot");

		const route = this.routeSingleDestination(request.items, "write", "executeSingleShot");
		if (route.destination === "none") {
			return { outcome: "not_applicable" };
		}
		if (route.destination === "child") {
			return await this.getChildStub(route.pCtx).txExecuteSingleShot(route.pCtx, request);
		}
		const { response, promotionCandidates } = this.#participant.executeSingleShot(request);
		if (response.outcome === "rejected") {
			return response;
		}

		// ONCE per transaction, not once per item.
		// A throw here is absorbed, as txCommit absorbs it: the items are already applied, and db.ts reads
		// any error of this path as "nothing applied", so it must not throw after the apply commits.
		this.ctx.waitUntil(
			(async () => {
				try {
					await this.drainPromotionCandidates(pCtx, promotionCandidates);
					await this.checkSplits(pCtx);
				} catch (error) {
					console.error({
						...this.logParams(),
						message: "fokos/partition.executeSingleShot: split check failed after the transaction applied.",
						error: String(error),
						errorProps: error,
					});
				}
			})(),
		);

		return response;
	}

	/**
	 * The server-side authority for the single-partition fast paths. One DO must execute every item:
	 * either this one owns them all, or exactly one child does and the whole request is handed over.
	 * Anything else is `none`: the caller answers `not_applicable` and touches nothing, so db.ts runs
	 * the two-phase path.
	 *
	 * Forwarding hops cost latency but not correctness — the nodes in between own nothing and do
	 * nothing.
	 *
	 * SYNCHRONOUS BY CONTRACT: the caller must not `await` between this decision and the work it
	 * authorises. A split can only advance at an input-gate point, so an unbroken synchronous block
	 * closes the split and promotion races.
	 */
	private routeSingleDestination<T extends { hashKey: KeyBytes; sortKey?: KeyBytes }>(
		items: T[],
		intent: OperationIntent,
		operationName: string,
	): { destination: "local"; items: T[] } | { destination: "child"; pCtx: PartitionContextResolved; items: T[] } | { destination: "none" } {
		const { local, forwarded } = this.groupItemsByRouting(items, intent, operationName);
		if (forwarded.size === 0) {
			return { destination: "local", items: local };
		}
		if (forwarded.size === 1 && local.length === 0) {
			const [entry] = [...forwarded.values()];
			return { destination: "child", pCtx: entry.pCtx, items: entry.items };
		}
		// The items span more than one partition. A value and not an error: on a split table this is the
		// ordinary answer for such a set, and it carries no side effects at any depth of a forwarding chain.
		return { destination: "none" };
	}

	/////////////////////////////////////////
	// ALARM / BACKGROUND WORK / INTERNALs
	/////////////////////////////////////////

	async alarm(alarmInfo: AlarmInvocationInfo): Promise<void> {
		console.log({
			...this.logParams(),
			message: "fokos/partition: Alarm triggered.",
			alarmInfo,
		});
		await this.runBackgroundWork();
	}

	// RPC erases the KeyBytes brand: keys reach the DO already-encoded as Uint8Array (db.ts encodes at
	// the public entry). Re-brand on this trust boundary without re-encoding. A raw string (e.g. a direct
	// in-process test call) is encoded so the DO always works on canonical KeyBytes.
	private pCtx(): PartitionContextLivePartition & { _partitionIdBytes: Uint8Array } {
		const pCtx = this.#_partitionContext;
		invariant(pCtx, this.STRING_PCTX_INIT_ERROR);
		assertCtxHasIdBytes(pCtx);
		return pCtx;
	}

	// The depth of this partition in the topology tree.
	// A hash partition: 0 is the root, 1 is a first-level child, and so on.
	// A range partition: 0 is the root range partition, 1 is a first-level child, and so on.
	#_depth: number | undefined = undefined;

	private depth(): number {
		if (this.#_depth !== undefined) return this.#_depth;

		const pCtx = this.pCtx();
		if (isHashPartition(pCtx)) {
			this.#_depth = PartitionIdHelper.depth(pCtx._partitionIdBytes);
		} else {
			const rangeDepth = this.kvDepth();
			invariant(
				rangeDepth !== undefined,
				"fokos/partition: rangeDepth must be set on a range partition (key promotion or range split did not initialize it)",
			);
			this.#_depth = rangeDepth;
		}
		return this.#_depth;
	}

	private kvDepth(): number | undefined {
		return this.ctx.storage.kv.get<number>(PartitionDO.KV_KEYS.PARTITION_DEPTH);
	}

	private ensurePartitionContext(
		pCtx: PartitionContextResolved | PartitionContextLivePartition,
		isInit = false,
	): PartitionContextLivePartition {
		if (this.ctx.id.jurisdiction !== pCtx.jurisdiction) {
			throw new FokosInternalError(INTERNAL_CODES.partition_context_mismatch, {
				message: "partition context mismatch",
				attributes: { doName: pCtx.doName, jurisdictionReq: pCtx.jurisdiction, jurisdictionActual: this.ctx.id.jurisdiction },
			});
		}
		// Phantom-bounce guard: a range DO is born ONLY through initFromSplit (promotion creates the root,
		// a split creates children). A request reaching an uninitialized range DO means a caller resolved a
		// fabricated (start,end) name that never existed — never lazy-init it; bounce so the caller falls back
		// to the range root and traverses. (A hash DO may still lazy-init, as today.)
		if (!isInit && !this.#_partitionContext && isRangePartition(pCtx)) {
			throw new FokosRoutingError(ROUTING_CODES.range_partition_not_initialized, {
				message: "range partition is not initialized; route via the range root and traverse",
				attributes: { doName: pCtx.doName },
			});
		}
		if (this.#_partitionContext) {
			// rangePartition boundaries are KeyBytes — compare by bytes (null = unbounded), never by reference.
			const keyEq = (a: KeyBytes | null | undefined, b: KeyBytes | null | undefined): boolean =>
				a == null || b == null ? a == b : KeyCodec.compare(a, b) === 0;
			// The given context must match the stored one, or the partition serves data it does not own.
			if (
				!areImmutableOptionsEqual(this.#_partitionContext, pCtx) ||
				this.#_partitionContext.partitionId !== pCtx.partitionId ||
				this.#_partitionContext.doName !== pCtx.doName ||
				!keyEq(this.#_partitionContext.rangePartition?.hashKey, pCtx.rangePartition?.hashKey) ||
				!keyEq(this.#_partitionContext.rangePartition?.startBoundary, pCtx.rangePartition?.startBoundary) ||
				!keyEq(this.#_partitionContext.rangePartition?.endBoundary, pCtx.rangePartition?.endBoundary)
			) {
				throw new FokosInternalError(INTERNAL_CODES.partition_context_mismatch, {
					message: "partition context mismatch",
					attributes: { doName: pCtx.doName },
				});
			}
			// Fall through to update to the latest version if there are changes.
			if (areMutableOptionsEqual(this.#_partitionContext, pCtx)) {
				return this.#_partitionContext;
			}
		}
		invariant(pCtx.partitionId.length > 0, "fokos/partition.ensurePartitionContext: partitionId must not be empty");
		this.#_partitionContext = { ...pCtx };
		this.#_partitionContext._partitionIdBytes = undefined;
		this.ctx.storage.kv.put<PartitionContextLivePartition>(PartitionDO.KV_KEYS.PARTITION_CONTEXT, this.#_partitionContext);
		this.#_partitionContext._partitionIdBytes = Uint8Array.fromHex(this.#_partitionContext.partitionId);
		this.#_topology?.updatePartitionContext(this.#_partitionContext);
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
				? new RangePartitionTopologyImpl(pCtx, this.ctx, this.#store, this.#source)
				: new HashPartitionTopologyImpl(pCtx, this.ctx, this.#store, this.#source);
		}
		return this.#_topology;
	}

	/**
	 * The import gate. An incomplete target holds only some of the rows and some of the inherited
	 * locks, so a write cannot apply and this partition cannot answer a read locally.
	 *
	 * A request that reaches an incomplete target also asks for one more import step and restores the
	 * fallback alarm. The partition then makes progress even when no start notification arrived.
	 */
	private async ensureMigration(op: string, throwIfMigrating = true): Promise<boolean> {
		// TODO Optimize this away by keeping it in memory.
		if (!this.#target.isImporting()) return false;
		this.scheduleBackgroundWork({ delayMs: 0, forceSchedule: true });
		await this.ensureAlarmSet(Date.now() + PartitionDO.MIGRATION_FALLBACK_ALARM_MS);
		if (throwIfMigrating) {
			// TODO: Migrate only the requested keys.
			throw new FokosUnavailableError(UNAVAILABLE_CODES.partition_migrating, {
				message: "partition split in progress, please retry later",
				attributes: { operation: op },
			});
		}
		return true;
	}

	private async forwardToRangeRootPartition<T extends { meta: PartitionInfoInternal }>(
		ctx: PartitionContextResolved,
		hashKey: KeyBytes,
		forward: (stub: PartitionDOStub, pCtx: PartitionContextResolved) => Promise<T>,
		sortKey?: KeyBytes,
	): Promise<T> {
		// Default entry is the range root (null, null). If this DO has already learned deeper range
		// boundaries for this hash key (from prior forward results), jump straight to the deepest known
		// slice that contains sortKey, skipping the root router chain. Immutable boundary identity makes
		// a stale hint safe: the target validates range membership and re-forwards if it has split
		// further. Multi-item paths that lack a single sortKey pass undefined and stay on the root.
		let entry: ReturnType<typeof resolveRangePartitionContext> | null = null;
		if (sortKey !== undefined) {
			const learned = this.#store.findDeepestKnownRangeSlice(hashKey, sortKey);
			if (learned && (learned.startBoundary !== null || learned.endBoundary !== null)) {
				entry = resolveRangePartitionContext(ctx, hashKey, learned.startBoundary, learned.endBoundary);
			}
		}
		if (!entry) {
			entry = resolveRangePartitionContext(ctx, hashKey, null, null);
		}
		const { doId, partitionContext: toCtx } = entry;
		const topology = this.ensureTopology(ctx);

		// Learn the range subtree boundaries from the response so future entries can skip the root chain.
		// The response meta carries the serving leaf's rangeAncestors (propagated up through each range
		// router), so this feeds the same range_hierarchy cache that the skip above reads. Without this,
		// the steady-state promoted-key path (which always enters here) would never populate that cache.
		// On a hash `fromCtx` → range `toCtx`, recordForwardResult inserts the ancestors and no-ops the
		// hash-topology update.
		const learn = (meta: PartitionInfoInternal) => topology.recordForwardResult(hashKey, ctx, toCtx, meta);
		// A hash partition answers with its own hash depth: its caller forwarded to it, and checks that depth.
		const hashDepth = isHashPartition(ctx) ? this.depth() : undefined;
		const result = await forward(partitionStub(this.env, ctx, doId), toCtx).catch((e: unknown) => {
			learnFromErrorMeta(e, learn, hashDepth);
			throw e;
		});
		learn(result.meta);
		return { ...result, meta: forwardedMeta(result.meta, hashDepth) } as T;
	}

	private async maybeForwardToRangeRootPartition<T extends { meta: PartitionInfoInternal }>(
		ctx: PartitionContextResolved,
		hashKey: KeyBytes,
		forward: (stub: PartitionDOStub, pCtx: PartitionContextResolved) => Promise<T>,
		sortKey?: KeyBytes,
		fallbackOnNotCutOver: boolean = false,
	): Promise<T | null> {
		try {
			return await this.forwardToRangeRootPartition(ctx, hashKey, forward, sortKey);
		} catch (e) {
			// A range DO that was never initialized bounces the request, and the caller falls back to the range root.
			if (FokosError.isCode(e, ROUTING_CODES.range_partition_not_initialized)) {
				return null;
			}
			if (fallbackOnNotCutOver && FokosError.isCode(e, UNAVAILABLE_CODES.repartition_not_cut_over)) {
				return null;
			}
			throw e;
		}
	}

	private async withSplitForwarding<T extends { meta: PartitionInfoInternal }>(opts: {
		ctx: PartitionContextResolved;
		keys: { hashKey: KeyBytes; sortKey: KeyBytes };
		operationName: string;
		intent: OperationIntent;
		forward: (stub: PartitionDOStub, pCtx: PartitionContextResolved) => Promise<T>;
		local: () => Promise<T>;
	}): Promise<T> {
		const {
			ctx,
			keys: { hashKey, sortKey },
			operationName,
			intent,
			forward,
			local,
		} = opts;

		if (isHashPartition(ctx)) {
			// Step 1: the override check for the keys this partition promoted or inherited. It is final.
			if (this.#source.ownedByRangeTree(hashKey)) {
				return await this.forwardToRangeRootPartition(ctx, hashKey, forward, sortKey);
			}

			// Step 2: Speculative bloom filter check — learned promotions from descendants.
			const prt = this.#_partialRangeTopology;
			if (prt?.maybePromoted(hashKey)) {
				const result = await this.maybeForwardToRangeRootPartition(ctx, hashKey, forward, sortKey, intent === "read");
				if (result) return result;
			}
		}

		const topology = this.ensureTopology(ctx);
		const decision = topology.shouldAllow(hashKey, sortKey, intent);
		switch (decision) {
			case "ok":
				return await local();
			case "forward": {
				const { doId, partitionContext } = topology.pickChildPartition(ctx, hashKey, sortKey);
				const stub = partitionStub(this.env, ctx, doId);
				// The result and the error of the forward both carry the routing meta of the target.
				const learn = (meta: PartitionInfoInternal) => {
					topology.recordForwardResult(hashKey, ctx, partitionContext, meta);

					if (isHashPartition(ctx) && PartitionIdHelper.isRangePartition(meta.servedByPartitionId)) {
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
				};
				const result = await forward(stub, partitionContext).catch((e: unknown) => {
					learnFromErrorMeta(e, learn);
					throw e;
				});
				learn(result.meta);
				return { ...result, meta: forwardedMeta(result.meta) } as T;
			}
			case "reject_over_size":
				throw errExceededDatabaseSize(operationName);
			case "reject_out_of_range":
				throw errInvalidPartitionRouting(operationName);
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
	/**
	 * Routes a transaction's items. Throws on either reject, and the two are NOT interchangeable:
	 * "reject_over_size" is retryable backpressure from a healthy partition, so it raises the same
	 * error the non-transactional path raises; "reject_out_of_range" means the item reached a
	 * partition that cannot own it, which is a bug, so it keeps the invariant.
	 */
	private groupItemsByRouting<T extends { hashKey: KeyBytes; sortKey?: KeyBytes }>(
		items: T[],
		intent: OperationIntent,
		operationName: string,
	): {
		local: T[];
		forwarded: Map<string, { pCtx: PartitionContextResolved; items: T[] }>;
	} {
		const pCtx = this.pCtx();
		const topology = this.ensureTopology(pCtx);
		const local: T[] = [];
		const forwarded = new Map<string, { pCtx: PartitionContextResolved; items: T[] }>();

		const addForwarded = (destPCtx: PartitionContextResolved, item: T) => {
			let entry = forwarded.get(destPCtx.doName);
			if (!entry) {
				entry = { pCtx: destPCtx, items: [] };
				forwarded.set(destPCtx.doName, entry);
			}
			entry.items.push(item);
		};

		for (const item of items) {
			// On a hash partition only: forward a key the range tree now owns to its range root.
			if (isHashPartition(pCtx) && this.#source.ownedByRangeTree(item.hashKey)) {
				const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(pCtx, item.hashKey, null, null);
				addForwarded(rangeRootCtx, item);
				continue;
			}

			const decision = topology.shouldAllow(item.hashKey, item.sortKey, intent);
			if (decision === "ok") {
				local.push(item);
			} else if (decision === "forward") {
				const { partitionContext } = topology.pickChildPartition(pCtx, item.hashKey, item.sortKey);
				addForwarded(partitionContext, item);
			} else if (decision === "reject_over_size") {
				throw errExceededDatabaseSize(operationName);
			} else {
				throw errInvalidPartitionRouting(operationName);
			}
		}

		return { local, forwarded };
	}

	private getChildStub(childPCtx: PartitionContextResolved): PartitionDOStub {
		return partitionStubByName(this.env, this.pCtx(), childPCtx.doName);
	}

	/**
	 * The metrics and routing information for work this node did itself. `forwardCount` is 0 because a
	 * node that answers locally forwarded nothing; a router builds its own meta with its fan-out count.
	 */
	private localMeta(
		pCtx: PartitionContextResolved,
		counts: { rowsRead: number; rowsWritten: number },
	): OperationMetrics & PartitionInfoInternal {
		return {
			rowsRead: counts.rowsRead,
			rowsWritten: counts.rowsWritten,
			databaseSize: this.#store.databaseSize,
			...this.routingMeta(pCtx),
		};
	}

	/** The routing part of the meta of this node. `localMeta` adds the metrics of the work, and `#rpc` stamps it on an error. */
	private routingMeta(pCtx: PartitionContextResolved): PartitionInfoInternal {
		return {
			servedByActorId: this.ctx.id.toString(),
			servedByActorName: pCtx.doName,
			servedByPartitionId: pCtx.partitionId,
			forwardCount: 0,
			hashDepth: isHashPartition(pCtx) ? this.depth() : 0,
			rangeDepth: isRangePartition(pCtx) ? this.depth() : 0,
			_internal: {
				rangeAncestors: this.#_rangeAncestors,
			},
		};
	}

	private readItemLocally(pCtx: PartitionContextResolved, req: GetItemRpcRequest): GetItemRpcResponse {
		const res =
			req.projection === undefined
				? this.#store.getItem(req.hashKey, req.sortKey)
				: this.#store.getItemProjected(req.projection, req.hashKey, req.sortKey);
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
			hashDepth: isHashPartition(pCtx) ? this.depth() : 0,
			rangeDepth: isRangePartition(pCtx) ? this.depth() : 0,
			_internal: {
				rangeAncestors: this.#_rangeAncestors,
			},
		};
		if (!result) {
			return { found: false, meta: actorMeta };
		}
		if ("projected" in result) {
			return {
				found: true,
				item: {
					projected: result.projected,
					kind: "projected",
					...(result.ttlAt === undefined ? {} : { ttlAt: result.ttlAt }),
					version: result.version,
				},
				meta: actorMeta,
			};
		}
		return {
			found: true,
			item: {
				// json arrives here as JSON text (decoded in SQL); db.ts parses it once at the public boundary.
				data: result.data,
				kind: result.kind,
				ttlAt: result.ttl_epoch_utc_seconds ?? undefined,
				version: result.v,
			},
			meta: actorMeta,
		};
	}

	/**
	 * How many import pages one background pass applies, one at a time. A subclass can override it, as
	 * it can override `fokosStaleTransactionMs`. Import throughput matters more than the other jobs,
	 * because every write to this partition waits for it. A request that arrives during an import asks
	 * for one step, not for a whole pass.
	 */
	protected fokosImportPagesPerPass(): number {
		return Math.max(1, PartitionDO.IMPORT_PAGES_PER_PASS);
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
			// A background run is already scheduled for the same target time, so this call adds nothing.
			// Many timers on the same instant cause a thundering herd and waste resources.
			return;
		}
		this.#_backgroundWorkScheduledAt = targetTime;
		setTimeout(() => {
			// FIXME: The schedule timestamp resets after 1 second. The background work always takes longer
			// than delayMs, and this keeps the concurrent runs, the overhead, and the memory down. A
			// scheduler that allows N overlaps would stop one stuck job from blocking the progress.
			void Promise.race([
				this.runBackgroundWork(),
				new Promise((resolve) =>
					setTimeout(() => {
						// Reset the schedule only when it is still this one, so a newer schedule is not lost.
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

	/**
	 * Runs one background pass. It joins the pass in flight instead of starting a second one.
	 *
	 * A timer, an alarm and an incoming request all reach here. Two passes over one import each hold a
	 * page the other has moved past, and such a page re-inserts rows a user deleted after the import
	 * finished. The promise lives in memory only, so an eviction loses it. The durable guards inside
	 * each transition make the work safe. This method only stops the waste.
	 */
	private runBackgroundWork(): Promise<void> {
		if (this.#_backgroundInFlight) return this.#_backgroundInFlight;
		const run = this.#runBackgroundWorkOnce().finally(() => {
			this.#_backgroundInFlight = null;
		});
		this.#_backgroundInFlight = run;
		return run;
	}

	async #runBackgroundWorkOnce(): Promise<void> {
		invariant(this.#_partitionContext, "fokos/partition.runBackgroundWork: partition context not initialized");
		/**
		 * INVARIANTS FOR ALL BACKGROUND JOBS:
		 * - A job must read its durable state before it writes. The in-flight promise above is not
		 *   durable progress, and a revived instance can still hold stale work.
		 * - A job must be crash-safe. A crash must let the other jobs run, and the job must resume or
		 *   retry its own work with no data loss and no inconsistency.
		 * - On an error, a job must log it and leave a durable deadline behind, so the work goes on.
		 */
		if (this.isDestroying()) return;

		// Armed BEFORE the pass changes state or calls an RPC. A crash inside the pass then leaves an
		// alarm that can read the new durable state. The end of the pass replaces it with the earliest
		// real deadline.
		await this.ensureAlarmSet(Date.now() + PartitionDO.WORK_FALLBACK_ALARM_MS);

		try {
			////////////////////////////////////////////////////////
			// ── Job: target import (this partition is catching up)
			try {
				for (let i = 0; i < this.fokosImportPagesPerPass(); i++) {
					if (this.isDestroying()) break;
					const outcome = await this.#target.importOnePage();
					if (outcome !== "progressed") break;
				}
				if (!this.#target.isImporting()) this.#ttl.arm();
			} catch (error) {
				this.logJobFailure("target import", error);
			}

			////////////////////////////////////////////////
			// ── Job: target acknowledgement
			try {
				if (!this.isDestroying()) await this.#target.sendAck();
			} catch (error) {
				this.logJobFailure("target ack", error);
			}

			/////////////////////////////////////////////////////
			// ── Job: source repartition (one due row, one step)
			try {
				if (!this.isDestroying()) await this.#source.sourceStep();
			} catch (error) {
				this.logJobFailure("source repartition", error);
			}

			///////////////////////////////////////////////////
			// ── Job: source cleanup (one bounded batch)
			try {
				if (!this.isDestroying()) this.#source.sourceCleanupStep();
			} catch (error) {
				this.logJobFailure("source cleanup", error);
			}

			////////////////////////////////////////
			// ── Job: Stale transaction recovery
			try {
				if (!this.isDestroying()) await this.recoverStaleTransactions();
			} catch (error) {
				this.logJobFailure("stale transaction recovery", error);
			}
		} catch (error) {
			console.error({
				...this.logParams(),
				message: "fokos/partition: Background work failed with unexpected error.",
				error: String(error),
				errorProps: error,
			});
		} finally {
			await this.scheduleNextPass();
		}
	}

	/**
	 * Moves the alarm to the earliest deadline that a durable job still holds.
	 *
	 * This write REPLACES the fallback the pass armed, and it can move the alarm later. The pass is
	 * over here, so the earlier fallback protects nothing. Without the replacement, the alarm fires at
	 * the fallback interval for as long as durable work sits further out.
	 */
	private async scheduleNextPass(): Promise<void> {
		// A fenced pass schedules nothing: a destroy has started, and the partition must stop.
		if (this.isDestroying()) return;

		let nextAlarmMs: number | null = null;
		const wantAlarm = (ms: number | null) => {
			if (ms === null) return;
			if (nextAlarmMs === null || ms < nextAlarmMs) nextAlarmMs = ms;
		};
		this.#store.transactionSync(() => {
			wantAlarm(this.#target.importDeadline());
			wantAlarm(this.#source.sourceDeadline());
			if (this.txPendingCanSweep() && this.#store.hasAnyUnguardedPendingTx()) {
				wantAlarm(Date.now() + this.fokosStaleTransactionMs());
			}
		});

		if (nextAlarmMs !== null) {
			await this.ctx.storage.setAlarm(nextAlarmMs);
			// Do not wait for the alarm when the work is already due.
			if (nextAlarmMs <= Date.now()) this.scheduleBackgroundWork({ delayMs: 10, forceSchedule: true });
		} else {
			// No durable work is left, so the fallback this pass armed must go. It would otherwise wake
			// every idle partition at the fallback interval for ever, with no work to do.
			await this.ctx.storage.deleteAlarm();
			console.log({ ...this.logParams(), message: "fokos/partition: Background work ran, nothing to schedule forward." });
		}
	}

	/**
	 * Wakes a promotion that waits for a lock this transaction can have just released.
	 *
	 * A promotion cannot move a locked key, and only a commit and a cancel clear a lock. Without this
	 * call the source learns of the release on its own 5-second retry, which is the fallback and not
	 * the signal. One indexed seek gates it, so an ordinary transaction pays nothing.
	 */
	private wakeLockBlockedPromotion(): void {
		if (!this.#store.hasUnfinishedPromotion()) return;
		this.#source.onLockReleased();
		this.scheduleBackgroundWork({ delayMs: 10, forceSchedule: true });
	}

	/** True after `fokosPrepareDestroy` fences this partition. Every transition must then stop. */
	private isDestroying(): boolean {
		return this.ctx.storage.kv.get<boolean>(REPARTITION_KV_KEYS.DESTROYING) === true;
	}

	private logJobFailure(job: string, error: unknown): void {
		console.error({ ...this.logParams(), message: `fokos/partition: ${job} job failed.`, error: String(error), errorProps: error });
	}

	/** Asks the coordinator of each stale transaction to resolve it, and applies the answer. */
	private async recoverStaleTransactions(): Promise<void> {
		if (!this.txPendingCanSweep()) return;
		const staleTxRows = this.#participant.listStaleTransactions(this.fokosStaleTransactionMs(), 10);
		for (const row of staleTxRows) {
			if (!row.coordinator_do_id) continue;
			try {
				const tcStub = txCoordinatorStub(this.env, this.pCtx(), row.coordinator_do_id);
				const result = await tcStub.recoverTransaction(row.transaction_id);

				const pendingRows = this.#store.listPendingTxItems(row.transaction_id);
				if (pendingRows.length === 0) continue;
				const items = pendingRows.map((pending) => ({ hashKey: pending.hk, sortKey: pending.sk }));

				if (result.state === "COMMITTED") {
					await this.txCommit(this.pCtx(), {
						transactionId: row.transaction_id,
						transactionTimestamp: pendingRows[0].transaction_ts,
						items,
					});
				} else if (result.state === "CANCELLED") {
					await this.txCancel(this.pCtx(), { transactionId: row.transaction_id, items });
				} else if (result.state === "not_found") {
					const { local } = this.groupItemsByRouting(items, "read", "staleTransactionRecovery");
					if (local.length === 0) {
						this.#store.deletePendingTx(row.transaction_id);
						continue;
					}

					const now = Date.now();
					const lockCreatedAt = Math.min(...pendingRows.map((pending) => pending.created_at));
					const lockAgeMs = now - lockCreatedAt;
					if (lockAgeMs > IDEMPOTENCY_WINDOW_MS) {
						if (this.#store.guardPendingTx(row.transaction_id, now)) {
							const pCtx = this.pCtx();
							console.error({
								...this.logParams(),
								message: "fokos/partition: lock-age guard: over-age lock with not_found",
								transactionId: row.transaction_id,
								coordinatorDoId: row.coordinator_do_id,
								keys: pendingRows.map((pending) => ({
									hashKey: pending.hk.toBase64({ alphabet: "base64url" }),
									sortKey: pending.sk.toBase64({ alphabet: "base64url" }),
								})),
								lockCreatedAt,
								lockAgeMs,
								windowMs: IDEMPOTENCY_WINDOW_MS,
								doName: pCtx.doName,
								partitionId: pCtx.partitionId,
							});
						}
						continue;
					}

					await this.txCancel(this.pCtx(), { transactionId: row.transaction_id, items });
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
	}

	private getOrCreatePartialRangeTopology(): PartialRangeTopology {
		if (!this.#_partialRangeTopology) {
			this.#_partialRangeTopology = PartialRangeTopology.create({
				errorRate: 0.01,
				// The target is about 1MB, with 1.5MB as the cap for extra headroom. The serialized bloom
				// filter is one SQLite row, so it must stay below the 2MB row size limit.
				//
				// The growth up to 1 MB:
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
				// WARNING: This must not change after the first key enters the bloom filter.
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

	async #rpc<T>(_name: string, fn: () => Promise<T>): Promise<T> {
		// TODO Add observability and canonical logs.
		const destroying = this.isDestroying();
		const allowedWhileDestroying =
			_name === "status" || _name === "fokosStatus" || _name === "fokosPrepareDestroy" || _name === "destroyPartition";
		if (!destroying) this.#ttl.arm();
		try {
			if (destroying && !allowedWhileDestroying) {
				throw new FokosUnavailableError(UNAVAILABLE_CODES.partition_migrating, {
					message: "partition destroy in progress, please retry later",
					attributes: { operation: _name },
				});
			}
			return await fn();
		} catch (e) {
			// Every error that leaves a partition is a FokosError, so a caller classifies it by its code.
			const err = FokosError.wrap(e);
			this.#stampRoutingMeta(err);
			throw err;
		}
	}

	/**
	 * Attaches the routing meta of this partition to an error that carries none, as the own data property
	 * `meta`, so each forwarding level learns from it as it learns from the meta of a result. The node
	 * that raises the error stamps it, and each forwarding level changes it as it changes a result meta.
	 * It skips a partition without a context, whose meta would mean nothing. Best effort: a failed stamp
	 * must never replace the error.
	 */
	#stampRoutingMeta(err: FokosError): void {
		const pCtx = this.#_partitionContext;
		if (!pCtx || routedError(err)) return;
		try {
			stampRoutingMeta(err, this.routingMeta(pCtx));
		} catch {}
	}

	private logParams() {
		const info = {
			...this.#_coloInfo,
			actorId: this.ctx.id.toString(),
			// Cloudflare Workers can truncate this to 1024 bytes. partitionContext.doName holds the full name.
			actorName: this.ctx.id.name,
			databaseSize: this.#store.databaseSize,
			depth: this.#_depth,
			// Always put the partition context in the logs for better debugging, even if it's undefined.
			// KeyBytes fields are rendered via keyForLog so they never appear as bare Uint8Array.
			partitionContext: pCtxForLog(this.#_partitionContext),
		};
		const importSource = this.#target.importRecord()?.source;
		if (importSource) {
			Object.assign(info, {
				importSource: { actorName: importSource.doName, actorId: importSource.primaryDoIdStr },
			});
		}
		return info;
	}
}

/**
 * The shape `status()` has always reported for a split. Every call derives it from the repartition
 * rows, and nothing writes it. The partition suites read it.
 */
export type SplitStatusView =
	| { status: "split_queued"; splitType: SplitType; createdAt: number; partitionContext: PartitionContextResolved }
	| {
			status: "split_started" | "split_completed";
			splitType: SplitType;
			createdAt: number;
			partitionContext: PartitionContextResolved;
			childPartitionContexts: PartitionContextResolved[];
			migratedChildDoNames: string[];
			history: {
				status: "split_queued" | "split_started";
				splitType: SplitType;
				createdAt: number;
				partitionContext: PartitionContextResolved;
			}[];
	  };

/** The migration status the old KV key reported, taken from the import record that replaced it. */
function derivedMigrationStatus(
	state: FokosImportState | undefined,
): "migration_initialized" | "migration_migrating" | "migration_completed" | undefined {
	switch (state) {
		case undefined:
			return undefined;
		case "awaiting_data":
			return "migration_initialized";
		case "importing":
			return "migration_migrating";
		default:
			return "migration_completed";
	}
}

/** The promotion status the old table reported, taken from the repartition state that replaced it. */
function promotedKeyStatusOf(state: RepartitionState): PromotedKeyStatus {
	if (state === "queued" || state === "planned") return "queued";
	if (state === "cutover") return "promoting";
	return "promoted";
}

/** Transient: the partition is healthy but past its cap, and a split will bring it back under. */
function errExceededDatabaseSize(operationName: string): FokosUnavailableError {
	return new FokosUnavailableError(UNAVAILABLE_CODES.partition_over_size, {
		message: "partition exceeded its limits, please retry later",
		attributes: { operation: operationName },
	});
}

/**
 * Never transient: the item reached a partition that can neither own nor route it. Serving it would
 * touch data another partition owns, and no amount of retrying changes the answer.
 */
function errInvalidPartitionRouting(operationName: string): FokosRoutingError {
	return new FokosRoutingError(ROUTING_CODES.partition_misrouted, {
		message: "mis-routed item this node can neither own nor route",
		attributes: { operation: operationName },
	});
}

/** A non-transactional write reached an item that an in-progress transaction holds. */
function itemLockedError(transactionId: string, hashKey: KeyBytes, sortKey: KeyBytes): FokosConflictError {
	return new FokosConflictError(CONFLICT_CODES.item_locked_by_transaction, {
		message: "item is locked by an in-progress transaction, retry later",
		attributes: { transactionId, ...decodeItemKeys(hashKey, sortKey) },
	});
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
