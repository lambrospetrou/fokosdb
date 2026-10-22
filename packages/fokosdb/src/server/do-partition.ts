import { DurableObject } from "cloudflare:workers";
import type { DataKind, OperationMetrics, QuerySelect, ReturnValuesOnConditionCheckFailure, WirePartitionMeta } from "../shared/types.js";
import type { ConditionExpression, ProjectionExpression, QueryExpressions } from "../shared/expression/types.js";
import type { ProjectedWireRow } from "../shared/expression/projection.js";
import type {
	CancelRequest,
	CancelResponse,
	CommitRequest,
	CommitResponse,
	DebugForceResolveTransactionRequest,
	DebugForceResolveTransactionResponse,
	IndexedReadItemEncoded,
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
	TransactionItemKey,
	TransactionPartitionsAnswer,
	TransactionReadItem,
} from "../shared/transaction-wire-types.js";
import type { FokosPartitionIdentity, FokosPartitionRef } from "../sharding/route-context.js";
import type { FokosDBPolicy, FokosDBRouteContext } from "../shared/partition-context.js";
import { identityDepth, isRangePartition } from "../sharding/partition-id.js";
import { KeyCodec, type KeyBytes } from "../sharding/key-codec.js";
import type { SplitType } from "../sharding/types.js";
import invariant from "../shared/invariant.js";
import {
	estimateItemBytes,
	estimateProjectedRowBytes,
	PartitionStore,
	type ItemImageRead,
	type StoredItem,
	type ScanCursor,
	type PromotedKeyStatus,
} from "../shared/partition/partition-store.js";
import type { RepartitionState } from "../sharding/sharding-store.js";
import {
	ownsByHashKey,
	parseCoordinatorRef,
	TransactionParticipant,
	type PromotionCandidate,
} from "../shared/partition/transaction-participant.js";
import { TtlExpiry } from "../shared/partition/ttl-expiry.js";
import { resolvePartitionConfig, type PartitionDOConfig, type PartitionDOConfigOverrides } from "./host-config.js";
import { FokosMigrationHost } from "../shared/partition/fokos-migration-host.js";
import { FokosShardingRuntime } from "../sharding/runtime.js";
import type { FokosRuntimeConfigOverrides } from "../sharding/runtime-config.js";
import type {
	FokosEnvelope,
	FokosGroupFailure,
	FokosGroupPart,
	FokosLifecycle,
	FokosLocalCall,
	FokosOperations,
	FokosRangeVisit,
	FokosRepartitionPlan,
	FokosShardingHooks,
} from "../sharding/runtime-types.js";
import type {
	FokosExecuteLocalRequest,
	FokosImportState,
	FokosInitRequest,
	FokosMigrationAckRequest,
	FokosMigrationPage,
	FokosMigrationPullRequest,
	FokosPrepareDestroyRequest,
	FokosPromotionsPage,
	FokosPromotionsRequest,
	FokosRequestPromotionRequest,
	FokosShardingRpc,
	FokosStartImportRequest,
	FokosStatusPage,
	FokosStatusRequest,
} from "../sharding/repartition-types.js";
import {
	clipToChildRange,
	cursorFallsInChild,
	isChildFullyBeforeCursor,
	makeBoundaryCursor,
	rangeIntersects,
	type SkInterval,
} from "../sharding/sk-interval.js";
import { QueryPageBudget } from "../shared/query/page-budget.js";
import { createQueryPageCollector } from "../shared/query/query-collector.js";
import { getColoInfo, type ColoInfo } from "../shared/cf-utils.js";
import { partitionStubByName, txCoordinatorStubForParticipant } from "../shared/do-stubs.js";
import {
	ADMISSION_MARGIN,
	applyBatchImageCap,
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
	FokosRoutingError,
	FokosUnavailableError,
	newErrorId,
	UNAVAILABLE_CODES,
} from "../shared/errors.js";
import { CONDITION_CHECK_CODES } from "../shared/errors-operations.js";
import type { ConditionEvaluationResult } from "../shared/expression/runtime.js";
import { SHARDING_INTERNAL_CODES, SHARDING_ROUTING_CODES } from "../sharding/errors.js";
import { routedError } from "../sharding/envelope.js";
import type { BatchUnprocessedCode } from "../shared/batch-api-types.js";
import { classifyBatchFailure } from "../shared/batch-failure.js";
import { fitBatchResponse } from "../shared/batch-response.js";

// ─── item RPC types ───────────────────────────────────────────────────────────

/**
 * Wire types for the item RPCs (db.ts → PartitionDO). Keys are canonical KeyBytes, encoded at the
 * db.ts entry, and `sortKey` is always present — the empty KeyBytes ([]) is the absent sentinel.
 * This matches the transaction and query RPCs, so every key crossing into a DO has one form.
 *
 * No response carries a key: `db.ts` answers with the caller's own keys, which are the only ones the
 * caller can recognise. No response carries a routing fact either: the envelope around it does. Each
 * response carries the entry of the partition that ran it, with its counts.
 */
export type ItemRpcKeys = { hashKey: KeyBytes; sortKey: KeyBytes };

/** The entry of the one partition that ran an item RPC. */
export type ItemRpcPartitions = [WirePartitionMeta & OperationMetrics];

export type PutItemRpcRequest = ItemRpcKeys & {
	/** Encoded at the db.ts boundary (json ⇒ JSON text). */
	data: string | Uint8Array;
	kind: DataKind;
	ttlAt?: number;
	condition?: ConditionExpression;
	returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;
};

export type PutItemRpcResponse =
	| { outcome: "ok"; version: number; partitions: ItemRpcPartitions }
	| { outcome: "rejected"; reason: RejectionReasonEncoded; partitions: ItemRpcPartitions };

export type DeleteItemRpcRequest = ItemRpcKeys & {
	condition?: ConditionExpression;
	returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;
};

export type DeleteItemRpcResponse =
	| { outcome: "ok"; deleted: boolean; partitions: ItemRpcPartitions }
	| { outcome: "rejected"; reason: RejectionReasonEncoded; partitions: ItemRpcPartitions };

export type GetItemRpcRequest = ItemRpcKeys & { projection?: readonly ProjectionExpression[] };

// json data is JSON text here; db.ts parses it once at the public boundary. The type is free of the
// recursive JsonValue so the Workers-RPC type machinery does not instantiate infinitely deep.
//
// A projected read carries the positional row inside `item`, so `kind`, `version`, and `ttlAt` are
// common to both found variants. `kind` is then `"projected"`, which is a read-result tag and never
// a stored `data_kind`.
export type GetItemRpcItem =
	| { data: string | Uint8Array; kind: DataKind; ttlAt?: number; version: number }
	| { projected: ProjectedWireRow; kind: "projected"; ttlAt?: number; version: number };

export type GetItemRpcResponse =
	| { found: true; item: GetItemRpcItem; partitions: ItemRpcPartitions }
	| { found: false; partitions: ItemRpcPartitions };

// ─── batch item RPC types ─────────────────────────────────────────────────────

/**
 * Wire types for the batch item RPCs. Each item carries its `opIndex`, the position of the item in the
 * batch, and every answer names its items by `opIndex` and carries no key: `db.ts` builds the public
 * results from its own request. An answer lists its partitions as a `group` answer does, and each
 * `partitionIndex` points into that list.
 */
export type BatchGetItemsRpcRequest = {
	items: Array<{ opIndex: number; hashKey: KeyBytes; sortKey: KeyBytes; projection?: readonly ProjectionExpression[] }>;
	/** The projection of each item that has no projection of its own. */
	projection?: readonly ProjectionExpression[];
	/** The `maxResponseBytes` of the batch. Every part gets the same value. */
	maxResponseBytes: number;
};

export type BatchGetItemsRpcResponse = {
	/** `bytes` is the estimate of the size of `item`, which `fitBatchResponse` adds up. */
	found: Array<{ opIndex: number; item: GetItemRpcItem; bytes: number; partitionIndex: number }>;
	notFound: Array<{ opIndex: number; partitionIndex: number }>;
	failed: BatchFailure[];
	partitions: WirePartitionMeta[];
};

export type BatchWriteRpcItem = {
	opIndex: number;
	hashKey: KeyBytes;
	sortKey: KeyBytes;
	condition?: ConditionExpression;
	returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;
} & ({ operation: "put"; data: string | Uint8Array; kind: DataKind; ttlAt?: number } | { operation: "delete" });

export type BatchWriteItemsRpcRequest = { items: BatchWriteRpcItem[] };

export type BatchWriteItemsRpcResponse = {
	appliedPuts: Array<{ opIndex: number; version: number; partitionIndex: number }>;
	appliedDeletes: Array<{ opIndex: number; deleted: boolean; partitionIndex: number }>;
	failed: BatchFailure[];
	partitions: WirePartitionMeta[];
};

/** The answer of `conditionalWrite`. `image` is present only when the caller asked for it and the item exists. */
type ConditionalWriteResult<W> =
	| { outcome: "rejected"; conditionRes: ConditionEvaluationResult; image?: ItemImageRead }
	| { outcome: "ok"; writeRes: W; conditionRes: ConditionEvaluationResult | null };

/** One error that stopped one or more items. */
export type BatchFailure = {
	outcome: "rejected" | "unprocessed" | "unknown";
	code: BatchUnprocessedCode;
	/** Absent only for `response_too_large`, which is no error. */
	error_id?: string;
	/** Absent when no partition answered. */
	partitionIndex?: number;
	opIndexes: number[];
	/** Only for `condition_failed`, which stops one operation. `reason.item` is the old image. */
	reason?: RejectionReasonEncoded;
	/** The byte length of the image in `reason.item`, which `applyBatchImageCap` adds up. */
	imageBytes?: number;
	itemOmitted?: "response_too_large";
};

// ─── queryItems internal types ────────────────────────────────────────────────

export type { SkInterval } from "../sharding/sk-interval.js";
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
	/** The filter and the projection, as expression trees, or null for a request with neither. */
	plan: QueryExpressions | null;
};

/** The entry of one leaf scan, with its counts. `itemCount` is the number of items of the response that the leaf gave. */
export type LeafMetrics = WirePartitionMeta & OperationMetrics & { itemCount: number };

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
	 * Leaf-only: hash leaves and non-split range partitions that scanned rows, in visit order. The items of
	 * a leaf are one contiguous block of `items`. Routers appear in the envelope only.
	 */
	partitions: LeafMetrics[];
};

export type { FokosPartitionRef } from "../sharding/repartition-types.js";

export type DebugForcePromoteKeyRequest = { hashKey: KeyBytes };

export type DebugForcePromoteKeyResponse = {
	/** False when the key already had a promotion entry, so this call changed nothing. */
	queued: boolean;
	/** The key's promotion status after the call. */
	status: PromotedKeyStatus | undefined;
};

// ─── the test view ────────────────────────────────────────────────────────────

/** The split lifecycle as the partition suites read it. Derived from the runtime on every call, and never stored. */
export type SplitStatusView =
	| { status: "split_queued"; splitType: SplitType }
	| {
			status: "split_started" | "split_completed";
			splitType: SplitType;
			/** Built from THIS partition's current context, never from a snapshot taken at split time. */
			childPartitionContexts: FokosDBRouteContext[];
			migratedChildDoNames: string[];
	  };

export type PartitionStatusView = {
	depth: number;
	partitionContext: FokosDBRouteContext;
	identityStored: FokosPartitionIdentity;
	splitStatus: SplitStatusView | undefined;
	migrationStatus: "migration_initialized" | "migration_migrating" | "migration_completed" | undefined;
	parentPartitionContext: FokosPartitionRef | undefined;
	parentSplitType: SplitType | undefined;
};

// ─── the operation spec ───────────────────────────────────────────────────────

/** The operations of the partition. The registry, `dispatch`, `forward`, and the RPC surface derive from it. */
export type PartitionOps = {
	apiPutItem: { req: PutItemRpcRequest; res: PutItemRpcResponse };
	apiGetItem: { req: GetItemRpcRequest; res: GetItemRpcResponse };
	apiDeleteItem: { req: DeleteItemRpcRequest; res: DeleteItemRpcResponse };
	apiQueryItems: { req: QueryItemsRpcRequest; res: QueryItemsRpcResponse };
	apiBatchGetItems: { req: BatchGetItemsRpcRequest; res: BatchGetItemsRpcResponse };
	apiBatchWriteItems: { req: BatchWriteItemsRpcRequest; res: BatchWriteItemsRpcResponse };
	txPrepare: { req: PrepareRequest; res: PrepareResponse };
	txCommit: { req: CommitRequest; res: CommitResponse };
	txCancel: { req: CancelRequest; res: CancelResponse };
	txReadForTransaction: { req: ReadForTransactionRequest; res: ReadForTransactionResponse };
	txReadSnapshot: { req: ReadSnapshotRequest; res: ReadSnapshotResponse };
	txExecuteSingleShot: { req: SingleShotRequest; res: SingleShotResponse };
	debugForceResolveTransaction: { req: DebugForceResolveTransactionRequest; res: DebugForceResolveTransactionResponse };
	debugForcePromoteKey: { req: DebugForcePromoteKeyRequest; res: DebugForcePromoteKeyResponse };
	/** INTERNAL ONLY FOR TESTING. */
	status: { req: null; res: PartitionStatusView };
	/** INTERNAL ONLY FOR TESTING. The promotion status this partition holds for one hash key. */
	promotedKeyStatus: { req: { hashKey: KeyBytes }; res: PromotedKeyStatus | null };
};

/** The RPC surface of the class. `db.ts` and the coordinator type their stubs with it. */
export type PartitionRpc = FokosShardingRpc & {
	[K in keyof PartitionOps]: (ctx: FokosDBRouteContext, req: PartitionOps[K]["req"]) => Promise<FokosEnvelope<PartitionOps[K]["res"]>>;
};

// ─────────────────────────────────────────────────────────────────────────────

/** The host job that asks the coordinator of each stale lock to resolve it. */
const JOB_STALE_TX_RECOVERY = "stale_tx_recovery";

/**
 * The longest time that one step of the stale-transaction job starts coordinator calls. The passes of
 * the scheduler run one at a time, so a long step delays the import and repartition jobs.
 */
const STALE_RECOVERY_STEP_BUDGET_MS = 10_000;

/**
 * The most keys that one split decision asks to promote. When this many keys all have promotions
 * that cannot finish, the next large key waits until one of them finishes.
 */
const PROMOTION_CANDIDATES_MAX = 5;

const NO_SORT_KEY = KeyCodec.encodeOptional(undefined);

/** The committed state of one item, and its response bytes. `found` is absent for an absent item. */
type ItemRead = { found?: { item: GetItemRpcItem; bytes: number }; rowsRead: number; rowsWritten: number };

const keyOf = (item: TransactionItemKey) => ({ hashKey: item.hashKey, sortKey: item.sortKey });
const rowKeyOf = (row: { hk: KeyBytes; sk: KeyBytes }) => ({ hashKey: row.hk, sortKey: row.sk });

export class PartitionDO extends DurableObject implements PartitionRpc {
	/** The sharding runtime: identity, routing, repartitions, and the alarm. Every public method is one `dispatch`. */
	readonly fokos: FokosShardingRuntime<FokosDBPolicy, PartitionOps>;
	#store: PartitionStore;
	#participant: TransactionParticipant;
	#ttl: TtlExpiry;
	/** True after the split floor was logged. Every refused write runs the decision, so it logs once. */
	#splitFloorLogged = false;
	// Best-effort telemetry: which Cloudflare colo this isolate runs in. Populated
	// non-blocking from the constructor, so it may be undefined for the first few
	// requests after the DO wakes. Never gate correctness on it.
	#_coloInfo?: ColoInfo;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		// The runtime is created after this store. The callbacks run only when a statement or a
		// decision needs the answer.
		this.#store = new PartitionStore(ctx.storage);
		this.#participant = new TransactionParticipant({
			store: this.#store,
			now: () => this.fokosNow(),
			maxClockSkewMs: () => this.config().maxClockSkewMs,
			staleTransactionMs: () => this.config().staleTransactionMs,
			ownerCheck: () => this.fokos.ownerCheck(),
		});
		this.#ttl = new TtlExpiry({
			store: this.#store,
			canSweep: () => this.canSweepLocally(this.fokos.lifecycle()),
			ownerCheck: () => ownsByHashKey(this.fokos.ownerCheck()),
			logParams: () => this.logParams(),
			config: () => this.config().ttlSweep,
		});
		// The runtime runs the sharding migrations in its own blockConcurrencyWhile, before the host's.
		this.fokos = new FokosShardingRuntime<FokosDBPolicy, PartitionOps>({
			ctx,
			// Host code: it reads the binding and the location hint from the policy and applies the jurisdiction.
			stub: (routeCtx, doName) => partitionStubByName(env, routeCtx, doName),
			hooks: this.hooks(),
			operations: this.operations(),
			config: () => this.fokosRuntimeConfig(),
		});
		void ctx.blockConcurrencyWhile(async () => this.#store.runMigrations());
		this.#ttl.arm(this.config().ttlSweep.ttlSweepDelayMs);

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

	// ═══ the RPC surface: one dispatch per method ════════════════════════════

	async apiPutItem(ctx: FokosDBRouteContext, req: PutItemRpcRequest) {
		return await this.#api("apiPutItem", ctx, req);
	}
	async apiGetItem(ctx: FokosDBRouteContext, req: GetItemRpcRequest) {
		return await this.#api("apiGetItem", ctx, req);
	}
	async apiDeleteItem(ctx: FokosDBRouteContext, req: DeleteItemRpcRequest) {
		return await this.#api("apiDeleteItem", ctx, req);
	}
	async apiQueryItems(ctx: FokosDBRouteContext, req: QueryItemsRpcRequest) {
		return await this.#api("apiQueryItems", ctx, req);
	}
	async apiBatchGetItems(ctx: FokosDBRouteContext, req: BatchGetItemsRpcRequest) {
		return await this.#api("apiBatchGetItems", ctx, req);
	}
	async apiBatchWriteItems(ctx: FokosDBRouteContext, req: BatchWriteItemsRpcRequest) {
		return await this.#api("apiBatchWriteItems", ctx, req);
	}
	async txPrepare(ctx: FokosDBRouteContext, req: PrepareRequest) {
		return await this.#api("txPrepare", ctx, req);
	}
	async txCommit(ctx: FokosDBRouteContext, req: CommitRequest) {
		return await this.#api("txCommit", ctx, req);
	}
	/**
	 * Releases this transaction's locks in this partition and in the descendants that own `req.items`.
	 * Each hop releases the rows of the keys it owns, so a router releases nothing and a source keeps
	 * the copies of a key it moved away.
	 */
	async txCancel(ctx: FokosDBRouteContext, req: CancelRequest) {
		return await this.#api("txCancel", ctx, req);
	}
	async txReadForTransaction(ctx: FokosDBRouteContext, req: ReadForTransactionRequest) {
		return await this.#api("txReadForTransaction", ctx, req);
	}
	/** The single-partition fast path for `transactGetItems`: one round trip, no coordinator, nothing persisted. */
	async txReadSnapshot(ctx: FokosDBRouteContext, req: ReadSnapshotRequest) {
		return await this.#api("txReadSnapshot", ctx, req);
	}
	/** The single-partition fast path for `transactWriteItems`: one storage transaction applies the whole set. */
	async txExecuteSingleShot(ctx: FokosDBRouteContext, req: SingleShotRequest) {
		return await this.#api("txExecuteSingleShot", ctx, req);
	}
	async debugForceResolveTransaction(ctx: FokosDBRouteContext, req: DebugForceResolveTransactionRequest) {
		return await this.#api("debugForceResolveTransaction", ctx, req);
	}
	/**
	 * Promotes `hashKey` to its own range structure now, instead of waiting for the key to grow past
	 * `hashSplitConditions.maxSizeMb * promotionFraction`. It only queues the work: the same
	 * background pass performs the cutover, the migration and the acknowledgement. Idempotent: a key
	 * that already has a promotion entry comes back with `queued: false`.
	 */
	async debugForcePromoteKey(ctx: FokosDBRouteContext, req: DebugForcePromoteKeyRequest) {
		return await this.#api("debugForcePromoteKey", ctx, req);
	}
	/** INTERNAL ONLY FOR TESTING. */
	async status(ctx: FokosDBRouteContext) {
		return await this.#api("status", ctx, null);
	}
	/** INTERNAL ONLY FOR TESTING. */
	async promotedKeyStatus(ctx: FokosDBRouteContext, req: { hashKey: KeyBytes }) {
		return await this.#api("promotedKeyStatus", ctx, req);
	}

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
	async fokosRequestPromotion(req: FokosRequestPromotionRequest) {
		return await this.fokos.fokosRequestPromotion(req);
	}
	async fokosStatus(req: FokosStatusRequest): Promise<FokosStatusPage> {
		return await this.fokos.fokosStatus(req);
	}
	async fokosPromotions(req: FokosPromotionsRequest): Promise<FokosPromotionsPage> {
		return await this.fokos.fokosPromotions(req);
	}
	async fokosPrepareDestroy(req: FokosPrepareDestroyRequest): Promise<void> {
		await this.fokos.fokosPrepareDestroy(req);
		// After the pass the fence waited for, never before it: the pass can arm the sweep.
		this.#ttl.disarm();
	}
	async fokosDestroy(): Promise<void> {
		this.#ttl.disarm();
		return await this.fokos.fokosDestroy();
	}

	/**
	 * The background pass, and then the TTL sweep.
	 *
	 * The sweep runs from a timer, and its cycle stops without re-arming when it cannot sweep. A target
	 * that woke while it was still importing therefore holds no timer once its import finishes, and a
	 * partition that serves no public request would keep its expired rows until one arrived. The pass
	 * runs first, so the state the sweep reads is the state the pass left.
	 */
	async alarm(info: AlarmInvocationInfo): Promise<void> {
		await this.fokos.alarm(info);
		if (!this.fokos.isFenced()) {
			this.#ttl.arm();
		}
	}

	/**
	 * Every request arms the TTL sweep, so a partition that serves traffic expires its rows. A fenced
	 * partition arms nothing: `fokosPrepareDestroy` disarms the sweep, and a request that arrives
	 * between the fence and `fokosDestroy` must not start the timer again.
	 */
	#api<K extends keyof PartitionOps>(op: K, ctx: FokosDBRouteContext, req: PartitionOps[K]["req"]) {
		if (!this.fokos.isFenced()) {
			this.#ttl.arm();
		}
		return this.fokos.dispatch(op, ctx, req);
	}

	//////////////////////////////
	// User overridable methods.
	//////////////////////////////

	/**
	 * Override to change the settings of this class. The class merges the result with
	 * `DEFAULT_PARTITION_CONFIG`, validates it, and reads it at each use. The constructor of this class
	 * calls it, before `this.fokos` is assigned and before the fields of a subclass exist. An override
	 * that reads the identity or the policy of the partition must first check
	 * `this.fokos?.initialized()`, and return a fallback value when it is false.
	 */
	protected fokosConfig(): PartitionDOConfigOverrides {
		return {};
	}

	/**
	 * Override to change the settings of the sharding runtime of this class. The runtime merges the
	 * result with `DEFAULT_RUNTIME_CONFIG` and validates it. It calls this method in its own
	 * constructor, before `this.fokos` is assigned, and again at each use of a setting. An override that
	 * reads the identity or the policy of the partition must first check `this.fokos?.initialized()`,
	 * and return a fallback value when it is false.
	 */
	protected fokosRuntimeConfig(): FokosRuntimeConfigOverrides {
		return {};
	}

	/**
	 * The clock of the transaction locks and of the stale transaction recovery, in milliseconds. Read at
	 * each use, so a test can replace it on one instance and avoid global mocks.
	 */
	fokosNow(): number {
		return Date.now();
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

	/** The active settings: the overrides of `fokosConfig()` over the defaults, validated. */
	private __cachedConfig?: PartitionDOConfig;
	private config(): PartitionDOConfig {
		if (!this.__cachedConfig) {
			this.__cachedConfig = resolvePartitionConfig(this.fokosConfig());
		}
		return this.__cachedConfig;
	}

	/**
	 * Both sweeps ask the same question, because both need complete local state. An uninitialized
	 * partition has none, and asking it for a route context throws. An importing target does not hold
	 * it yet. A split router no longer holds it: its targets own the keys and sweep their own rows. A
	 * fenced partition is on its way out and must make no transition.
	 *
	 * The identity check comes first and reads only memory. The TTL sweep runs from a timer, so it can
	 * ask this before any request gave the partition an identity, and a partition without one can hold
	 * no import and no repartition: reading storage to learn that is work the answer does not need.
	 * `lifecycle` reads storage only for the fields that this check reads, so a partition without an
	 * identity reads nothing.
	 */
	private canSweepLocally(lifecycle: FokosLifecycle): boolean {
		if (!this.fokos.initialized()) {
			return false;
		}
		if (lifecycle.destroying || lifecycle.role === "router") {
			return false;
		}
		return lifecycle.import === null || (lifecycle.import.state !== "awaiting_data" && lifecycle.import.state !== "importing");
	}

	// ═══ operations ══════════════════════════════════════════════════════════

	private operations(): FokosOperations<PartitionOps> {
		return {
			apiPutItem: {
				shape: "point",
				whileMigrating: "throw",
				admissionTag: "write",
				key: (req) => ({ hashKey: req.hashKey, sortKey: req.sortKey }),
				local: (req, call) => this.putItemLocal(req, call),
			},
			apiDeleteItem: {
				shape: "point",
				whileMigrating: "throw",
				admissionTag: "delete",
				key: (req) => ({ hashKey: req.hashKey, sortKey: req.sortKey }),
				local: (req) => this.deleteItemLocal(req),
			},
			apiGetItem: {
				shape: "point",
				whileMigrating: "read_source",
				readOnly: true,
				admissionTag: "read",
				key: (req) => ({ hashKey: req.hashKey, sortKey: req.sortKey }),
				local: (req) => this.readItemLocally(req),
			},
			apiQueryItems: {
				shape: "range",
				whileMigrating: "read_source",
				readOnly: true,
				admissionTag: "read",
				range: (req) => ({ hashKey: req.hashKey, interval: req.interval, descending: req.direction === "desc", cursor: req.cursor }),
				clip: (req, visit) => clipQueryToVisit(req, visit),
				local: (req) => this.queryItemsLocal(req),
				walk: ({ request, visits, local, forward }) => walkRangeVisits(request, visits, local, forward),
			},
			// A batch is not atomic, so a part that fails does not fail the request: `merge` gives each of
			// its items an outcome from the error.
			apiBatchGetItems: {
				shape: "group",
				whileMigrating: "read_source",
				readOnly: true,
				admissionTag: "read",
				failurePolicy: "collect",
				items: (req) => req.items.map((item) => ({ key: keyOf(item), item })),
				subRequest: (req, items) => ({ ...req, items: items as BatchGetItemsRpcRequest["items"] }),
				local: (req) => this.batchGetLocal(req),
				// Each part fits in the limit, but the parts together can go above it. Every node trims its
				// answer, so no answer that crosses an RPC goes above it.
				merge: (parts) => {
					const merged = this.mergeBatch(parts, false, { found: [], notFound: [], failed: [], partitions: [] }, (out, result, offset) => {
						out.found.push(...result.found.map((entry) => withOffset(entry, offset)));
						out.notFound.push(...result.notFound.map((entry) => withOffset(entry, offset)));
					});
					const { kept, dropped } = fitBatchResponse(merged.found, Math.min(...parts.map((part) => part.request.maxResponseBytes)));
					merged.found = kept;
					for (const { opIndex, partitionIndex } of dropped) {
						merged.failed.push({ outcome: "unprocessed", code: "response_too_large", opIndexes: [opIndex], partitionIndex });
					}
					return merged;
				},
			},
			// The handler checks the size of the partition itself, so a partition above its cap still
			// applies the deletes of a batch: the admission tag is one that `admit` allows.
			apiBatchWriteItems: {
				shape: "group",
				whileMigrating: "throw",
				admissionTag: "batch_write",
				failurePolicy: "collect",
				items: (req) => req.items.map((item) => ({ key: keyOf(item), item })),
				subRequest: (req, items) => ({ items: items as BatchWriteRpcItem[] }),
				local: (req, call) => this.batchWriteLocal(req, call),
				merge: (parts) => {
					const merged = this.mergeBatch(
						parts,
						true,
						{ appliedPuts: [], appliedDeletes: [], failed: [], partitions: [] },
						(out, result, offset) => {
							out.appliedPuts.push(...result.appliedPuts.map((entry) => withOffset(entry, offset)));
							out.appliedDeletes.push(...result.appliedDeletes.map((entry) => withOffset(entry, offset)));
						},
					);
					applyBatchImageCap(merged.failed);
					return merged;
				},
			},
			txPrepare: {
				shape: "group",
				whileMigrating: "throw",
				admissionTag: "write",
				failurePolicy: "fail_fast",
				items: (req) => req.items.map((item) => ({ key: keyOf(item), item })),
				subRequest: (req, items) => ({ ...req, items: items as TransactionItem[] }),
				local: (req, call) => {
					if (req.items.length === 0) {
						return this.ownAnswer({ outcome: "accepted" });
					}
					const response = this.#participant.prepareLocal(req);
					// The lock is durable, so its recovery deadline must be too, even when the coordinator never returns.
					if (response.outcome === "accepted") {
						call.signal({ jobs: [{ name: JOB_STALE_TX_RECOVERY, runAt: this.fokosNow() + this.config().staleTransactionMs }] });
						return this.ownAnswer(response);
					}
					// This partition rejected each rejected operation, and it is the first entry of its own list.
					const results = response.results.map((r) => (r.outcome === "rejected" ? { ...r, partitionIndex: 0 } : r));
					return this.ownAnswer({ ...response, results });
				},
				merge: mergePrepare,
			},
			// The coordinator has already decided this transaction, and commit cannot grow the partition:
			// prepare persisted the payload into pending_transactions, so commit moves those bytes into
			// `items` and drops the pending row. Size backpressure here would wedge a decided transaction.
			txCommit: {
				shape: "group",
				whileMigrating: "throw",
				admissionTag: "ignore_size_reject",
				failurePolicy: "attempt_all",
				items: (req) => req.items.map((item) => ({ key: keyOf(item), item })),
				subRequest: (req, items) => ({ ...req, items: items as TransactionItemKey[] }),
				local: (req, call) => {
					const { response, promotionCandidates } = this.#participant.commitLocal(req);
					this.signalGrowth(call, promotionCandidates);
					return this.ownAnswer(response);
				},
				merge: (parts) => ({ outcome: "committed", partitions: parts.flatMap((p) => p.result.partitions) }),
			},
			// Cancel only DELETEs pending rows, so size backpressure must not wedge it: cancel is the path
			// that brings an over-size partition back under its cap.
			txCancel: {
				shape: "group",
				whileMigrating: "throw",
				admissionTag: "ignore_size_reject",
				failurePolicy: "attempt_all",
				items: (req) => req.items.map((item) => ({ key: keyOf(item), item })),
				subRequest: (req, items) => ({ ...req, items: items as TransactionItemKey[] }),
				// The release takes the owned part of the request, which the runtime hands this handler, so
				// a router releases nothing and a source keeps the copies of the keys it forwards.
				local: (req) => {
					this.#participant.cancelLocal(req.transactionId, req.items);
					return this.ownAnswer({ outcome: "cancelled" });
				},
				merge: (parts) => ({ outcome: "cancelled", partitions: parts.flatMap((p) => p.result.partitions) }),
			},
			txReadForTransaction: {
				shape: "group",
				whileMigrating: "throw",
				admissionTag: "read",
				failurePolicy: "fail_fast",
				items: (req) => req.items.map((item) => ({ key: keyOf(item), item })),
				subRequest: (req, items) => ({ ...req, items: items as TransactionReadItem[] }),
				// This partition read each item, and it is the first entry of its own list.
				local: (req) =>
					this.ownAnswer({ items: this.#participant.readForTransactionLocal(req).items.map((item) => ({ ...item, partitionIndex: 0 })) }),
				merge: (parts) => {
					const items: IndexedReadItemEncoded[] = [];
					let offset = 0;
					for (const { result } of parts) {
						items.push(...result.items.map((item) => withOffset(item, offset)));
						offset += result.partitions.length;
					}
					return { items, partitions: parts.flatMap((p) => p.result.partitions) };
				},
			},
			// A partition DO is single-threaded and reads the whole set with no `await` in between, so the
			// result already IS a consistent snapshot; the second phase of the coordinator's read exists
			// only to detect interleaving ACROSS partitions, and here there is none to detect.
			txReadSnapshot: {
				shape: "single_owner",
				whileMigrating: "throw",
				admissionTag: "read",
				items: (req) => req.items.map((item) => ({ key: keyOf(item) })),
				notApplicable: { outcome: "not_applicable" },
				local: (req) => {
					const { items } = this.#participant.readForTransactionLocal(req);
					// Parity with the two-phase path: an item locked by an in-progress transaction has a write
					// that may or may not land, so the read cannot claim a committed snapshot.
					if (items.some((item) => item.hasPendingWrite)) {
						return this.ownAnswer({ outcome: "aborted", reason: "pending_write" });
					}
					return this.ownAnswer({ outcome: "committed", items });
				},
			},
			txExecuteSingleShot: {
				shape: "single_owner",
				whileMigrating: "throw",
				admissionTag: "write",
				items: (req) => req.items.map((item) => ({ key: keyOf(item) })),
				notApplicable: { outcome: "not_applicable" },
				local: (req, call) => {
					invariant(req.items.length > 0, "fokos/partition.executeSingleShot: at least one item is required");
					const { response, promotionCandidates } = this.#participant.executeSingleShot(req);
					if (response.outcome !== "rejected") {
						this.signalGrowth(call, promotionCandidates);
					}
					return this.ownAnswer(response);
				},
			},
			// Re-enters `dispatch`, so the commit or cancel of each key applies on its current owner. A
			// row of a key that moved is a copy: its key sends the outcome to the new owner, and the
			// routed operation writes here only for a key that this partition owns.
			debugForceResolveTransaction: {
				shape: "local",
				whileMigrating: "throw",
				local: async (req) => {
					const pendingRows = this.#store.listPendingTxItems(req.transactionId);
					const items = pendingRows.map(rowKeyOf);
					const resolvedLocally = this.#ownedRows(pendingRows).length;
					const counts = { resolvedLocally, forwarded: items.length - resolvedLocally };
					const ctx = this.fokos.routeContext();
					try {
						const response =
							req.outcome === "commit"
								? await this.fokos.dispatch("txCommit", ctx, {
										transactionId: req.transactionId,
										// Prepare writes one timestamp to every row of the transaction.
										transactionTimestamp: pendingRows[0]?.transaction_ts ?? txOrderTimestampNow(),
										items,
									})
								: await this.fokos.dispatch("txCancel", ctx, { transactionId: req.transactionId, items });
						console.info({
							...this.logParams(),
							...this.fokos.identity().ref,
							message: "fokos/partition: forced resolution applied",
							transactionId: req.transactionId,
							outcome: req.outcome,
							...counts,
						});
						return { outcome: response.value.outcome, ...counts };
					} catch (e) {
						// The runtime throws `partition_fanout_failed` only after the local part committed,
						// so the line keeps the local count. With every other code, no row changed here.
						const localApplied = FokosError.isCode(e, SHARDING_INTERNAL_CODES.partition_fanout_failed);
						console.error({
							...this.logParams(),
							...this.fokos.identity().ref,
							message: "fokos/partition: forced resolution failed",
							transactionId: req.transactionId,
							outcome: req.outcome,
							resolvedLocally: localApplied ? counts.resolvedLocally : 0,
							forwarded: counts.forwarded,
							...(FokosError.is(e) ? { errorCode: e.code, causeCode: e.attributes.causeCode } : { error: String(e) }),
						});
						throw e;
					}
				},
			},
			debugForcePromoteKey: {
				shape: "local",
				whileMigrating: "throw",
				local: async (req) => {
					const result = await this.fokos.requestPromotion(req.hashKey);
					if (result.queued) {
						return { queued: true, status: promotedKeyStatusOf(result.state) };
					}
					if (result.reason === "already_promoted") {
						return { queued: false, status: promotedKeyStatusOf(result.state) };
					}
					// The owner still holds the key and a split row refused the promotion: the key moves soon.
					// This is the answer a queued split has always given.
					throw errExceededDatabaseSize("debugForcePromoteKey");
				},
			},
			status: {
				shape: "local",
				local: async () => await this.statusView(),
				// A read-only test view stays available behind the destroy fence.
				allowedWhileDestroying: true,
			},
			promotedKeyStatus: {
				shape: "local",
				local: async (req) => {
					const state = this.fokos.promotionState(req.hashKey);
					return state === undefined ? null : promotedKeyStatusOf(state);
				},
				allowedWhileDestroying: true,
			},
		};
	}

	// ═══ hooks ═══════════════════════════════════════════════════════════════

	private hooks(): FokosShardingHooks<FokosDBPolicy> {
		return {
			evaluateSplit: (input) => {
				const decision = this.splitDecision(input.identity, input.policy);
				if ("answer" in decision) {
					this.#splitFloorLogged = false;
					return decision.answer;
				}
				// The partition refuses every write until an operator raises the cap or deletes data.
				if (!this.#splitFloorLogged) {
					this.#splitFloorLogged = true;
					console.error({
						...input.identity.ref,
						...this.logParams(),
						message: "fokos/partition: over the split threshold, but no repartition can make this partition smaller",
						reason: decision.floor,
						maxSizeBytes: decision.maxSizeBytes,
					});
				}
				return false;
			},
			computeRangeBoundaries: ({ hashKey, start, end, childCount }) =>
				this.#store.computeRangeSplitBoundaries(hashKey, start, end, childCount),
			migration: new FokosMigrationHost({
				store: this.#store,
				logParams: () => ({ ...this.logParams(), ...this.fokos.identity().ref }),
			}),
			// A promotion deletes the size estimate of its key. Every new write of the key goes to the
			// range tree, so nothing here reads the estimate again. Without the delete, the key stays in
			// the promotion list of the split decision until the cleanup deletes its last row, and the
			// split behind it waits. The cleanup keeps its own delete of the estimate as a guard.
			beforeComplete: (plan) => {
				if (plan.kind === "key_promotion") {
					this.#store.deleteKeySizeEstimate(promotedKeyOf(plan));
				}
			},
			// Each target now holds its own locks, so the lock copies here are not necessary. The
			// cleanup deletes them in steps of a limited size, because one large delete holds every
			// request of this partition until its write is durable. Each step deletes complete
			// transactions, so no call can send on only a part of one transaction. A promotion deletes
			// the copies of its key only, because the other keys stay here. A split deletes all copies.
			//
			// A split keeps its item rows. Only a promotion has item rows to give back: its key moved,
			// and the rest of its keys stay here.
			cleanupSourceStep: (plan) => {
				const { cleanupTxLockCopyRows, cleanupPromotedKeyRows } = this.config();
				if (plan.kind !== "key_promotion") {
					// FIXME: A split keeps its item rows. A delete of the rows writes rows, and each row written
					// has a cost. Compare that cost with the storage cost before a split deletes them.
					return this.#store.deletePendingTxCopiesBatch(null, cleanupTxLockCopyRows);
				}
				const hashKey = promotedKeyOf(plan);
				if (!this.#store.deletePendingTxCopiesBatch(hashKey, cleanupTxLockCopyRows)) {
					return false;
				}
				this.#store.deleteItemsBatchForHashKey(hashKey, cleanupPromotedKeyRows);
				if (this.#store.hasItemsForHashKey(hashKey)) {
					return false;
				}
				this.#store.deleteKeySizeEstimate(hashKey);
				return true;
			},
			// Size backpressure gates on "write" alone, because only a write can grow a partition: a read
			// cannot; a delete is how a client brings an over-size partition back under its cap; and a
			// commit or cancel resolves a transaction the coordinator has already decided. The partition
			// accepts up to 10% above the maximum size, so the decision does not flap at the threshold and
			// the requests that trigger the split complete.
			admit: ({ op, admissionTag, keys, policy }) => {
				// An empty request grows nothing, so size cannot refuse it. The hook still runs for it,
				// because an admission that does not look at keys can still apply.
				if (admissionTag !== "write" || keys.length === 0) {
					return "allow";
				}
				const refusal = this.sizeRefusal(op, policy);
				return refusal ? { reject: refusal } : "allow";
			},
			jobs: [
				{
					name: JOB_STALE_TX_RECOVERY,
					canRun: (lifecycle) => this.canSweepLocally(lifecycle),
					deadline: () => this.#store.earliestPendingTxRecoveryAt(),
					runStep: async () => {
						await this.recoverStaleTransactions();
						return { nextRunAt: null };
					},
				},
			],
		};
	}

	/**
	 * Reports what a write handler learned and its response does not carry: the keys that grew past
	 * their share of the cap, and that the partition may need a split. Promotions are reported first,
	 * because a key that has grown past its own cap must get its own range tree, and an unfinished
	 * promotion blocks the split behind it.
	 */
	private signalGrowth(call: FokosLocalCall, candidates: readonly PromotionCandidate[]): void {
		const promotionCandidates = this.promotionCandidates(candidates);
		if (promotionCandidates.length > 0) {
			call.signal({ promotionCandidates });
		}
		call.signal({ evaluateSplit: true });
	}

	/**
	 * The keys of `candidates` that a hash partition must promote. It keeps the largest candidate per
	 * key: one transaction can write many sort keys of one hash key, and each upsert reports the running
	 * total after its own row.
	 */
	private promotionCandidates(candidates: readonly PromotionCandidate[]): { hashKey: KeyBytes }[] {
		if (candidates.length === 0 || this.fokos.identity().kind !== "hash") {
			return [];
		}
		const threshold = (this.fokos.policy().hashSplitConditions.maxSizeMb ?? 0) * this.config().promotionFraction * 1024 * 1024;
		if (threshold <= 0) {
			return [];
		}
		const largest = new Map<string, PromotionCandidate>();
		for (const candidate of candidates) {
			const id = candidate.hashKey.toBase64({ alphabet: "base64url" });
			const seen = largest.get(id);
			if (!seen || candidate.keyEstBytes > seen.keyEstBytes) {
				largest.set(id, candidate);
			}
		}
		return [...largest.values()].filter((c) => c.keyEstBytes >= threshold).map(({ hashKey }) => ({ hashKey }));
	}

	/**
	 * The answer of `evaluateSplit`, or `floor` when this leaf is over its cap and no repartition can
	 * make it smaller. Under the cap it reads the database size only.
	 *
	 * A hash leaf probes `items` for two hash keys, and `pending_transactions` when `items` is empty,
	 * because a pending payload moves with its key. With two or more keys, the leaf promotes its keys
	 * at or above the promotion threshold, up to `PROMOTION_CANDIDATES_MAX` and largest first, and
	 * splits when it has none. One key is promoted whatever its size estimate: the cap reads the
	 * physical file, which can be much larger than the estimate. A range leaf splits when the planner
	 * finds its boundaries, from the same arguments the planner passes.
	 */
	private splitDecision(
		identity: FokosPartitionIdentity,
		policy: FokosDBPolicy,
	):
		| { answer: ReturnType<FokosShardingHooks<FokosDBPolicy>["evaluateSplit"]> }
		| { floor: "no_hash_key" | "fewer_items" | "skewed_bytes"; maxSizeBytes: number } {
		const maxSizeMb = (identity.kind === "hash" ? policy.hashSplitConditions : policy.rangeSplitConditions)?.maxSizeMb;
		const maxSizeBytes = (maxSizeMb ?? 0) * 1024 * 1024;
		if (!maxSizeMb || this.#store.databaseSize <= maxSizeBytes) {
			return { answer: false };
		}
		if (identity.kind === "range") {
			invariant(identity.range, `fokos/do-partition: range identity must have a range`);
			const { hashKey, start, end } = identity.range;
			const plan = this.#store.planRangeSplit(hashKey, start, end, this.fokos.routeContext().rangeConfig.rangeSplitN);
			return "boundaries" in plan ? { answer: {} } : { floor: plan.floor, maxSizeBytes };
		}
		const keys = this.#store.probeHashKeys("items") ?? this.#store.probeHashKeys("pending_transactions");
		if (!keys) {
			return { floor: "no_hash_key", maxSizeBytes };
		}
		if (!keys.more) {
			return { answer: { promote: [keys.first] } };
		}
		// A split copies a large key into a child, and the child promotes it with a second copy. A
		// promotion that cannot finish blocks the split, and a refused write signals no promotion.
		// So the large keys are promoted first, and each promotion answers independently.
		const large = this.#store.largestKeysAtLeast(maxSizeBytes * this.config().promotionFraction, PROMOTION_CANDIDATES_MAX);
		// We have enough items to split.
		return { answer: large.length > 0 ? { promote: large } : {} };
	}

	/**
	 * The size gate of a write: the error that refuses it when this partition is above 1.1 times its
	 * cap, or undefined. The `admit` hook and the batch write handler share it.
	 */
	private sizeRefusal(op: string, policy: FokosDBPolicy): FokosUnavailableError | undefined {
		const identity = this.fokos.identity();
		const maxSizeMb = (identity.kind === "hash" ? policy.hashSplitConditions : policy.rangeSplitConditions)?.maxSizeMb;
		if (maxSizeMb && this.#store.databaseSize > maxSizeMb * ADMISSION_MARGIN * 1024 * 1024) {
			return errExceededDatabaseSize(op);
		}
		return undefined;
	}

	// ═══ the write steps of the non-transactional writes ═════════════════════
	// `putItem`, `deleteItem`, and the batch write share them. Each runs inside the `transactionSync`
	// of its caller.

	/** Refuses a non-transactional write to an item that an in-progress transaction holds. It does not wait for the lock. */
	private assertNotLocked(hashKey: KeyBytes, sortKey: KeyBytes): void {
		const pendingRow = this.#store.pendingLockFor(hashKey, sortKey);
		if (pendingRow) {
			// FIXME: ATC §4 describes optimizations where a non-tx write can proceed using a
			// higher timestamp to force the pending tx to abort on commit, avoiding this rejection.
			throw itemLockedError(pendingRow.transaction_id, hashKey, sortKey);
		}
	}

	private upsertNow(item: { hashKey: KeyBytes; sortKey: KeyBytes; data: string | Uint8Array; kind: DataKind; ttlAt?: number }) {
		return this.#store.upsertItem({
			hk: item.hashKey,
			sk: item.sortKey,
			data: item.data,
			kind: item.kind,
			ttlAt: item.ttlAt ?? null,
			txOrderTs: txOrderTimestampNow(),
		});
	}

	/** The delete stamps the transaction order watermark, as a transactional delete does. */
	private deleteNow(hashKey: KeyBytes, sortKey: KeyBytes) {
		return this.#store.deleteItem({ hk: hashKey, sk: sortKey, txOrderTs: txOrderTimestampNow() });
	}

	/**
	 * Writes one item in its own `transactionSync`: the lock check, then the condition, then `write`.
	 * A failed condition writes nothing. It reads the old image when `wantsImage` is true and the item
	 * exists. `putItem`, `deleteItem`, and each operation of a batch write share it.
	 */
	private conditionalWrite<W>(
		hashKey: KeyBytes,
		sortKey: KeyBytes,
		condition: ConditionExpression | undefined,
		wantsImage: boolean,
		write: () => W,
	): ConditionalWriteResult<W> {
		return this.#store.transactionSync((): ConditionalWriteResult<W> => {
			this.assertNotLocked(hashKey, sortKey);

			const conditionRes = condition ? this.#store.evaluateCondition(condition, hashKey, sortKey) : null;
			if (conditionRes && !conditionRes.conditionOk) {
				const image = wantsImage && conditionRes.itemPresent ? this.#store.getItemImage(hashKey, sortKey) : undefined;
				return { outcome: "rejected", conditionRes, image };
			}

			return { outcome: "ok", writeRes: write(), conditionRes };
		});
	}

	// ═══ local handlers ══════════════════════════════════════════════════════

	private putItemLocal(req: PutItemRpcRequest, call: FokosLocalCall): PutItemRpcResponse {
		const { hashKey, sortKey } = req;
		const wantsImage = req.returnValuesOnConditionCheckFailure === "all_old";
		const localRes = this.conditionalWrite(hashKey, sortKey, req.condition, wantsImage, () => this.upsertNow(req));

		if (localRes.outcome === "rejected") {
			return {
				outcome: "rejected",
				reason: conditionFailedReason(decodeItemKeys(hashKey, sortKey), localRes.image?.row),
				partitions: this.answered(localRes.image ? sumSqlMetrics(localRes.conditionRes, localRes.image) : localRes.conditionRes),
			};
		}

		const { writeRes, conditionRes } = localRes;
		// The row is committed. The handler learns `keyEstBytes` from the upsert result, and the response
		// does not carry it, so the promotion candidate is reported from here.
		this.signalGrowth(call, [{ hashKey, keyEstBytes: writeRes.keyEstBytes }]);
		return {
			outcome: "ok",
			version: writeRes.version,
			partitions: this.answered(conditionRes ? sumSqlMetrics(conditionRes, writeRes) : writeRes),
		};
	}

	private deleteItemLocal(req: DeleteItemRpcRequest): DeleteItemRpcResponse {
		const { hashKey, sortKey } = req;
		const wantsImage = req.returnValuesOnConditionCheckFailure === "all_old";
		const localRes = this.conditionalWrite(hashKey, sortKey, req.condition, wantsImage, () => this.deleteNow(hashKey, sortKey));

		if (localRes.outcome === "rejected") {
			return {
				outcome: "rejected",
				reason: conditionFailedReason(decodeItemKeys(hashKey, sortKey), localRes.image?.row),
				partitions: this.answered(localRes.image ? sumSqlMetrics(localRes.conditionRes, localRes.image) : localRes.conditionRes),
			};
		}

		const { writeRes, conditionRes } = localRes;
		return {
			outcome: "ok",
			deleted: writeRes.deleted,
			partitions: this.answered(conditionRes ? sumSqlMetrics(conditionRes, writeRes) : writeRes),
		};
	}

	private readItemLocally(req: GetItemRpcRequest): GetItemRpcResponse {
		const { found, rowsRead, rowsWritten } = this.readItem(req.hashKey, req.sortKey, req.projection);
		const partitions = this.answered({ rowsRead, rowsWritten });
		return found ? { found: true, item: found.item, partitions } : { found: false, partitions };
	}

	/**
	 * The committed state of one item, with the response bytes that `estimateItemBytes` or
	 * `estimateProjectedRowBytes` counts for it. `getItem` and the batch read share it.
	 */
	private readItem(hashKey: KeyBytes, sortKey: KeyBytes, projection: readonly ProjectionExpression[] | undefined): ItemRead {
		if (projection !== undefined) {
			const { row, rowsRead, rowsWritten } = this.#store.getItemProjected(projection, hashKey, sortKey);
			if (!row) {
				return { rowsRead, rowsWritten };
			}
			const item: GetItemRpcItem = {
				projected: row.projected,
				kind: "projected",
				...(row.ttlAt === undefined ? {} : { ttlAt: row.ttlAt }),
				version: row.version,
			};
			return { found: { item, bytes: estimateProjectedRowBytes(row.projected) }, rowsRead, rowsWritten };
		}
		const { row, rowsRead, rowsWritten } = this.#store.getItem(hashKey, sortKey);
		if (!row) {
			return { rowsRead, rowsWritten };
		}
		const item: GetItemRpcItem = {
			// json arrives here as JSON text (decoded in SQL); db.ts parses it once at the public boundary.
			data: row.data,
			kind: row.kind,
			...(row.ttl_epoch_utc_seconds === null ? {} : { ttlAt: row.ttl_epoch_utc_seconds }),
			version: row.v,
		};
		return { found: { item, bytes: estimateItemBytes({ hk: hashKey, sk: sortKey, ...row }) }, rowsRead, rowsWritten };
	}

	/**
	 * Reads the items of one part of a batch read, in request order, as one snapshot of this partition.
	 * It does not apply the response limit: `merge` does, after it joins this part with the others.
	 */
	private batchGetLocal(req: BatchGetItemsRpcRequest): BatchGetItemsRpcResponse {
		const out: BatchGetItemsRpcResponse = { found: [], notFound: [], failed: [], partitions: [] };
		let rowsRead = 0;
		let rowsWritten = 0;
		for (const { opIndex, hashKey, sortKey, projection } of req.items) {
			let read: ItemRead;
			try {
				read = this.readItem(hashKey, sortKey, projection ?? req.projection);
			} catch (e) {
				out.failed.push({ ...classifyBatchFailure(e, { write: false, local: true }), opIndexes: [opIndex], partitionIndex: 0 });
				continue;
			}
			rowsRead += read.rowsRead;
			rowsWritten += read.rowsWritten;
			if (read.found) {
				out.found.push({ opIndex, ...read.found, partitionIndex: 0 });
			} else {
				out.notFound.push({ opIndex, partitionIndex: 0 });
			}
		}
		out.partitions = this.answered({ rowsRead, rowsWritten });
		return out;
	}

	/**
	 * Applies the operations of one part of a batch write, in request order, each in its own
	 * `transactionSync` block. A block that throws rolls back its own writes, and the next operation
	 * runs. A failed condition writes nothing and gives its operation `rejected` with `condition_failed`.
	 * After a block commits, the handler only appends to its answer, and that code does not throw, so the
	 * handler never throws after its first write. All blocks run in one synchronous call, so no other
	 * request runs between two operations.
	 *
	 * Above 1.1 times the cap, the puts are refused and the deletes still apply: a delete is how a
	 * client brings an over-size partition back under its cap.
	 */
	private batchWriteLocal(req: BatchWriteItemsRpcRequest, call: FokosLocalCall): BatchWriteItemsRpcResponse {
		const out: BatchWriteItemsRpcResponse = { appliedPuts: [], appliedDeletes: [], failed: [], partitions: [] };
		const puts = req.items.filter((item) => item.operation === "put").map((item) => item.opIndex);
		const overSize = puts.length > 0 ? this.sizeRefusal("apiBatchWriteItems", this.fokos.policy()) : undefined;
		if (overSize) {
			call.signal({ evaluateSplit: true });
			out.failed.push({ ...classifyBatchFailure(overSize, { write: true, local: true }), opIndexes: puts, partitionIndex: 0 });
		}
		const candidates: PromotionCandidate[] = [];
		let rowsRead = 0;
		let rowsWritten = 0;
		for (const item of req.items) {
			if (item.operation === "put" && overSize) {
				continue;
			}
			const { opIndex, hashKey, sortKey } = item;
			let local: ConditionalWriteResult<ReturnType<PartitionDO["upsertNow"]> | ReturnType<PartitionDO["deleteNow"]>>;
			try {
				// TODO: This might be performance slowdown instead of doing a transaction for all the items.
				//       The only reason we do this is to allow partial application of the batch, but might reconsider.
				local = this.conditionalWrite(hashKey, sortKey, item.condition, item.returnValuesOnConditionCheckFailure === "all_old", () =>
					item.operation === "put" ? this.upsertNow(item) : this.deleteNow(hashKey, sortKey),
				);
			} catch (e) {
				out.failed.push({ ...classifyBatchFailure(e, { write: true, local: true }), opIndexes: [opIndex], partitionIndex: 0 });
				continue;
			}
			rowsRead += local.conditionRes?.rowsRead ?? 0;
			rowsWritten += local.conditionRes?.rowsWritten ?? 0;
			if (local.outcome === "rejected") {
				const code = CONDITION_CHECK_CODES.condition_failed;
				const row = local.image?.row;
				rowsRead += local.image?.rowsRead ?? 0;
				rowsWritten += local.image?.rowsWritten ?? 0;
				out.failed.push({
					outcome: "rejected",
					code: code.code,
					error_id: newErrorId(code),
					opIndexes: [opIndex],
					partitionIndex: 0,
					reason: conditionFailedReason(decodeItemKeys(hashKey, sortKey), row),
					...(row ? { imageBytes: row.imageBytes } : {}),
				});
				continue;
			}
			const res = local.writeRes;
			rowsRead += res.rowsRead;
			rowsWritten += res.rowsWritten;
			if (!("deleted" in res)) {
				out.appliedPuts.push({ opIndex, version: res.version, partitionIndex: 0 });
				candidates.push({ hashKey, keyEstBytes: res.keyEstBytes });
			} else {
				out.appliedDeletes.push({ opIndex, deleted: res.deleted, partitionIndex: 0 });
			}
		}
		if (candidates.length > 0) {
			this.signalGrowth(call, candidates);
		}
		applyBatchImageCap(out.failed);
		out.partitions = this.answered({ rowsRead, rowsWritten });
		return out;
	}

	/**
	 * Joins the parts of a batch operation. A part that answered adds its lists, with each
	 * `partitionIndex` moved by the entries of the parts before it. A failed part gives all its items one
	 * failure, and adds the entry of the partition that raised the error with no counts: this partition
	 * for the local part, else the head of the routing of the error. A remote error with no routing adds
	 * no entry. It never throws.
	 */
	private mergeBatch<
		Req extends { items: Array<{ opIndex: number }> },
		Res extends { failed: BatchFailure[]; partitions: WirePartitionMeta[] },
	>(
		parts: Array<FokosGroupPart<Req, Res> | FokosGroupFailure<Req>>,
		write: boolean,
		out: Res,
		addLists: (out: Res, result: Res, offset: number) => void,
	): Res {
		for (const part of parts) {
			const offset = out.partitions.length;
			if ("result" in part) {
				addLists(out, part.result, offset);
				out.failed.push(...part.result.failed.map((entry) => withOffset(entry, offset)));
				out.partitions.push(...part.result.partitions);
				continue;
			}
			const local = part.target === "local";
			const entry = local ? this.selfPartition() : raiserPartition(part.error);
			if (entry) {
				out.partitions.push(entry);
			}
			out.failed.push({
				...classifyBatchFailure(part.error, { write, local }),
				opIndexes: part.request.items.map((item) => item.opIndex),
				...(entry ? { partitionIndex: offset } : {}),
			});
		}
		return out;
	}

	private queryItemsLocal(req: QueryItemsRpcRequest): QueryItemsRpcResponse {
		const hk = req.hashKey;
		const { interval, cursor } = req;

		const lower = interval.lower?.value ?? NO_SORT_KEY;
		const lowerInclusive = interval.lower?.inclusive ?? true;
		const upper = interval.upper?.value ?? null;
		const upperInclusive = interval.upper?.inclusive ?? false;

		const collector = createQueryPageCollector({
			hashKey: hk,
			select: req.select,
			budget: req,
			estimateResponseBytes: (item) => (Array.isArray(item) ? estimateProjectedRowBytes(item) : estimateItemBytes(item)),
		});
		const { rowsRead, rowsWritten } = this.#store.scanQueryPage(
			{
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
			},
			collector.consume,
		);
		const page = collector.state;

		// A leaf (hash leaf or non-split range partition) is the only kind of DO that scans rows, so it
		// is the only kind that contributes a `partitions` entry. Routers appear in the envelope only.
		return {
			items: page.items,
			count: page.count,
			scannedCount: page.scannedCount,
			evaluatedBytes: page.evaluatedBytes,
			responseBytes: page.responseBytes,
			rowsReturned: page.rowsReturned,
			lastEvaluatedCursor: page.lastEvaluatedCursor,
			nextCursor: page.nextCursor,
			partitions: [{ ...this.answered({ rowsRead, rowsWritten })[0], itemCount: page.items.length }],
		};
	}

	/** The entry of this partition, with the counts of the work it did itself. */
	private answered(counts: { rowsRead: number; rowsWritten: number }): ItemRpcPartitions {
		return [
			{ ...this.selfPartition(), rowsRead: counts.rowsRead, rowsWritten: counts.rowsWritten, databaseSize: this.#store.databaseSize },
		];
	}

	/**
	 * The entry of this partition in the `partitions` of an answer. Each partition writes only the depth
	 * that it has: a hash partition its hash depth, and a range partition its range depth.
	 */
	private selfPartition(): WirePartitionMeta {
		const identity = this.fokos.identity();
		const depth = identityDepth(identity);
		return { actorId: this.ctx.id.toString(), ...(identity.kind === "hash" ? { hashDepth: depth } : { rangeDepth: depth }) };
	}

	/** The answer of a local transaction handler, which lists this partition only. */
	private ownAnswer<T>(outcome: T): T & TransactionPartitionsAnswer {
		return { ...outcome, partitions: [this.selfPartition()] };
	}

	// ═══ the test view ═══════════════════════════════════════════════════════

	/**
	 * The view the partition suites read. Every field derives from the runtime's public surface on
	 * each call, and nothing persists it. The suites read the promotion of one key with
	 * `promotedKeyStatus`.
	 */
	private async statusView(): Promise<PartitionStatusView> {
		const identity = this.fokos.identity();
		const routeContext = this.fokos.routeContext();
		const importInfo = this.fokos.lifecycle().import;
		const entries = (await this.fokos.fokosStatus({})).split;

		const split = entries[0]?.repartition;
		let splitStatus: SplitStatusView | undefined;
		if (split) {
			const splitType: SplitType = split.kind === "hash_split" ? "hash" : "range";
			if (split.state === "queued" || split.state === "planned") {
				splitStatus = { status: "split_queued", splitType };
			} else {
				splitStatus = {
					status: split.state === "cutover" ? "split_started" : "split_completed",
					splitType,
					childPartitionContexts: this.fokos.children().map((child) => ({ ...routeContext, ...child.ref })),
					migratedChildDoNames: entries.filter((e) => e.target?.acknowledged).map((e) => e.target!.ref.doName),
				};
			}
		}

		return {
			depth: identityDepth(identity),
			partitionContext: routeContext,
			identityStored: identity,
			splitStatus,
			migrationStatus: derivedMigrationStatus(importInfo?.state),
			parentPartitionContext: importInfo?.source,
			parentSplitType: importInfo ? (importInfo.slice.kind === "hash_child" ? "hash" : "range") : undefined,
		};
	}

	// ═══ stale transaction recovery ══════════════════════════════════════════

	/**
	 * Asks the coordinator of each stale transaction to resolve it, and applies the answer to the rows
	 * that this partition owns. The answer goes through `dispatch`, which resolves the owner of each
	 * key again in the same synchronous block as the write.
	 */
	private async recoverStaleTransactions(): Promise<void> {
		const startedAt = this.fokosNow();
		const deadline = startedAt + STALE_RECOVERY_STEP_BUDGET_MS;
		// Each iteration claims one transaction and then asks its coordinator. A transaction that the
		// step does not reach keeps its place.
		const { staleLockScanRows } = this.config();
		for (let claimed = 0; claimed < staleLockScanRows; claimed++) {
			if (this.fokosNow() >= deadline) {
				return;
			}
			// FIXME: claim a batch of transactions in one storage transaction and drive them concurrently,
			// with a bounded fan-out.
			const [row] = this.#participant.claimStaleTransactions(startedAt, 1);
			if (!row) {
				return;
			}
			// A transaction whose rows here are all copies of a moved key belongs to the new owner, so
			// this partition does not ask the coordinator about it.
			if (this.#ownedRows(this.#store.listPendingTxKeys(row.transaction_id)).length === 0) {
				continue;
			}
			try {
				const ctx = this.fokos.routeContext();
				// The coordinator that drove the prepare. It forwards the call to the child that now owns the token.
				const { doName: coordinatorDoName, idempotencyToken } = parseCoordinatorRef(row.coordinator_json, row.transaction_id);
				const address = { nsTx: ctx.policy.nsTx, jurisdiction: ctx.topology.jurisdiction };
				const result = await txCoordinatorStubForParticipant(this.env, address, coordinatorDoName).recoverTransactionForParticipant({
					transactionId: row.transaction_id,
					idempotencyToken,
				});

				// This partition reads the rows and their owners again after the await, because a
				// promotion can cut over during the call to the coordinator. The decision applies only to
				// the rows that this partition owns now. The new owner of a moved key recovers its own
				// row of the lock after its import.
				const ownedRows = this.#ownedRows(this.#store.listPendingTxItems(row.transaction_id));
				if (ownedRows.length === 0) {
					continue;
				}
				const items = ownedRows.map(rowKeyOf);

				if (result.state === "COMMITTED") {
					await this.fokos.dispatch("txCommit", ctx, {
						transactionId: row.transaction_id,
						transactionTimestamp: ownedRows[0].transaction_ts,
						items,
					});
				} else if (result.state === "CANCELLED") {
					await this.fokos.dispatch("txCancel", ctx, { transactionId: row.transaction_id, items });
				} else if (result.state === "not_found") {
					const now = this.fokosNow();
					const lockCreatedAt = Math.min(...ownedRows.map((pending) => pending.created_at));
					const lockAgeMs = now - lockCreatedAt;
					if (lockAgeMs > IDEMPOTENCY_WINDOW_MS) {
						// A late prepare of a committed check can leave a lock that holds no decision:
						//
						//   1. The coordinator sends the prepare of a check of a present item. The answer gets
						//      lost, and the coordinator sends the prepare again. The first request is still in
						//      flight.
						//   2. The second prepare locks the key. The transaction commits, and this partition
						//      applies the check (last_read_ts rises to the timestamp T) and deletes the lock.
						//   3. The first prepare arrives now. It finds no lock of its transaction, so it is a
						//      new prepare to this partition. A check compares T only with last_write_ts, and
						//      the commit raised only last_read_ts, so the prepare passes and locks the key
						//      again.
						//
						// The stale job normally gets COMMITTED and releases this lock. But when the
						// coordinator has swept the transaction and the lock is older than the idempotency
						// window, the answer is not_found, and the lock must not be quarantined for an
						// operator: it refuses every writer of the key and has nothing to resolve. A put, an
						// update, or a delete in the same request makes the late prepare fail
						// (timestamp_conflict), so only a lock whose rows here are all checks gets here.
						//
						// Such a lock can be released with no decision. While a lock exists, no other request
						// changes its item, so the stamps below are the stamps from before the lock, plus the
						// commit of this transaction if it applied here. A check writes only its stamp. When
						// the stamp is already at or above T, a commit changes nothing, and a cancel changes
						// nothing, so a cancel is correct for either outcome of the transaction. When the
						// stamp is below T, the check did not apply here, and the quarantine stays.
						if (this.#checksAlreadyStamped(ownedRows)) {
							console.warn({
								...this.logParams(),
								message: "fokos/partition: released an over-age not_found lock of checks that are already stamped",
								transactionId: row.transaction_id,
								coordinatorDoName,
								idempotencyToken,
								keys: ownedRows.map((pending) => KeyCodec.pairForLog(pending.hk, pending.sk)),
								lockAgeMs,
							});
							await this.fokos.dispatch("txCancel", ctx, { transactionId: row.transaction_id, items });
							continue;
						}
						if (this.#store.guardPendingTx(row.transaction_id, now)) {
							console.error({
								...this.logParams(),
								message: "fokos/partition: lock-age guard: over-age lock with not_found",
								transactionId: row.transaction_id,
								coordinatorDoName,
								idempotencyToken,
								keys: ownedRows.map((pending) => KeyCodec.pairForLog(pending.hk, pending.sk)),
								lockCreatedAt,
								lockAgeMs,
								windowMs: IDEMPOTENCY_WINDOW_MS,
								doName: ctx.doName,
								partitionId: ctx.partitionId,
							});
						}
						continue;
					}

					await this.fokos.dispatch("txCancel", ctx, { transactionId: row.transaction_id, items });
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

	/**
	 * True when each row holds only checks, and the stamp that a check of its item records is at or above
	 * the timestamp of the transaction: last_read_ts of a present item, and max_delete_tx_order_ts of an
	 * absent item. The caller must not await between this read and the release that depends on it.
	 */
	#checksAlreadyStamped(rows: ReturnType<PartitionStore["listPendingTxItems"]>): boolean {
		let maxDeleteTxOrderTs: number | undefined;
		return rows.every((pending) => {
			if (!pending.op_list.every(([, operation]) => operation === "check")) {
				return false;
			}
			const stamp = this.#store.getItemStamp(pending.hk, pending.sk).row;
			const recorded = stamp ? stamp.last_read_ts : (maxDeleteTxOrderTs ??= this.#store.getMaxDeleteTxOrderTs());
			return recorded >= pending.transaction_ts;
		});
	}

	/** The rows of `rows` that this partition owns now, with one owner check for each hash key. */
	#ownedRows<Row extends { hk: KeyBytes; sk: KeyBytes }>(rows: readonly Row[]): Row[] {
		return rows.filter(ownsByHashKey(this.fokos.ownerCheck()));
	}

	private logParams() {
		return {
			...this.#_coloInfo,
			actorId: this.ctx.id.toString(),
			// Cloudflare Workers can truncate this to 1024 bytes. The runtime logs the full doName with its own lines.
			actorName: this.ctx.id.name,
			databaseSize: this.#store.databaseSize,
		};
	}
}

// ─── the range walk ───────────────────────────────────────────────────────────

/**
 * Restricts a query to one planned visit: the interval is clipped to the visit, and the cursor is kept
 * when it falls inside the visit, or dropped when the visit lies entirely after it. A visit that lies
 * entirely before the cursor is a routing defect: the walk filters those out first, and a read-through
 * caller that names one asks for rows it has already consumed.
 */
function clipQueryToVisit(req: QueryItemsRpcRequest, visit: { start: KeyBytes | null; end: KeyBytes | null }): QueryItemsRpcRequest {
	const start = visit.start ?? NO_SORT_KEY;
	let cursor: ScanCursor | null = null;
	if (req.cursor) {
		if (isChildFullyBeforeCursor(start, visit.end, req.cursor, req.direction)) {
			throw new FokosRoutingError(SHARDING_ROUTING_CODES.partition_misrouted, {
				message: "query cursor lies beyond the visited interval",
				attributes: { operation: "apiQueryItems" },
			});
		}
		cursor = cursorFallsInChild(start, visit.end, req.cursor) ? req.cursor : null;
	}
	return { ...req, interval: clipToChildRange(req.interval, visit.start, visit.end), cursor };
}

/**
 * Walks the frontier the runtime planned, in the order it planned it. The runtime owns the visits and
 * counts the forwards; this function owns the shared page budget, the cursor, and the early exits.
 */
async function walkRangeVisits(
	req: QueryItemsRpcRequest,
	visits: readonly FokosRangeVisit[],
	local: (req: QueryItemsRpcRequest) => QueryItemsRpcResponse | Promise<QueryItemsRpcResponse>,
	forward: (visit: FokosRangeVisit, req: QueryItemsRpcRequest) => Promise<QueryItemsRpcResponse>,
): Promise<QueryItemsRpcResponse> {
	const { interval, cursor, direction } = req;
	const budget = new QueryPageBudget(req);
	const out: QueryItemsRpcResponse = {
		items: [],
		count: 0,
		scannedCount: 0,
		evaluatedBytes: 0,
		responseBytes: 0,
		rowsReturned: 0,
		lastEvaluatedCursor: null,
		nextCursor: null,
		partitions: [],
	};

	// The visits that can contribute to this page: they intersect the query interval, and they are not
	// entirely behind the resume cursor. Selecting them up front turns "could a later visit still
	// contribute?" into a plain index test, which both budget exits must answer before they emit a
	// continuation cursor.
	const candidates = visits.filter((visit) => {
		const start = visit.start ?? NO_SORT_KEY;
		return rangeIntersects(start, visit.end, interval) && !(cursor && isChildFullyBeforeCursor(start, visit.end, cursor, direction));
	});

	for (let i = 0; i < candidates.length; i++) {
		const visit = candidates[i];
		// A cursor is honest only if a later visit still holds rows for this query. Without this, a
		// budget exhausted by the LAST visit — one that drained itself and reported no cursor of its
		// own — would still hand the client a cursor, buying it one more round trip that returns zero
		// items. `db.ts:queryItems` applies the same rule across sub-queries.
		const hasLaterCandidate = i < candidates.length - 1;
		const sub: QueryItemsRpcRequest = {
			...clipQueryToVisit(req, visit),
			remainingEvaluatedItems: budget.remainingEvaluatedItems,
			remainingEvaluatedBytes: budget.remainingEvaluatedBytes,
			remainingResponseBytes: budget.remainingResponseBytes,
			remainingPartitionVisits: budget.remainingPartitionVisits,
			allowOversizedFirstItem: budget.allowOversizedFirstItem,
		};
		const part = visit.target === "local" ? await local(sub) : await forward(visit, sub);

		if (req.select === "projection") {
			out.items.push(...part.items);
		}
		out.partitions.push(...part.partitions);
		out.count += part.count;
		out.scannedCount += part.scannedCount;
		out.evaluatedBytes += part.evaluatedBytes;
		out.responseBytes += part.responseBytes;
		out.rowsReturned += part.rowsReturned;
		out.lastEvaluatedCursor = part.lastEvaluatedCursor ?? out.lastEvaluatedCursor;
		budget.consume(part);

		if (part.nextCursor !== null) {
			out.nextCursor = part.nextCursor;
			break;
		}
		// The visit drained as a shared budget reached zero: resume strictly after the last evaluated
		// candidate (a leaf cursor carries no `inclusive` flag).
		if (budget.budgetExhausted) {
			if (hasLaterCandidate && out.lastEvaluatedCursor) {
				out.nextCursor = out.lastEvaluatedCursor;
			}
			break;
		}
		if (budget.visitsExhausted && hasLaterCandidate) {
			console.warn(
				`fokos/partition.walkRangeVisits: remainingPartitionVisits reached (${req.remainingPartitionVisits}), emitting boundary cursor`,
			);
			out.nextCursor = makeBoundaryCursor(req.hashKey, visit.start ?? NO_SORT_KEY, visit.end, direction);
			break;
		}
	}
	return out;
}

// ─── helpers ──────────────────────────────────────────────────────────────────

/**
 * This node answers for every operation it was given, whichever part evaluated it. An execution
 * failure belongs to no operation and outranks every per-operation rejection, so it travels up as it
 * arrived. An accepted part sends no array, so its operations passed. The answer lists the partitions
 * of every part, in part order.
 */
function mergePrepare(parts: Array<FokosGroupPart<PrepareRequest, PrepareResponse>>): PrepareResponse {
	const partitions = parts.flatMap((p) => p.result.partitions);
	const executionFailure = parts.find((p) => p.result.outcome === "rejected" && !p.result.results);
	if (executionFailure) {
		return { ...executionFailure.result, partitions };
	}
	if (!parts.some((p) => p.result.outcome === "rejected")) {
		return { outcome: "accepted", partitions };
	}
	const merged: ParticipantOperationResultEncoded[] = [];
	let offset = 0;
	for (const { request, result } of parts) {
		if (result.outcome === "accepted") {
			merged.push(...request.items.map((item) => ({ outcome: "passed" as const, opIndex: item.opIndex })));
		} else {
			merged.push(...result.results.map((r) => withOffset(r, offset)));
		}
		offset += result.partitions.length;
	}
	applyImageCap(merged);
	return { outcome: "rejected", results: merged, partitions };
}

/**
 * An entry of one part of a merged answer, with its `partitionIndex` moved by `offset`: the length of
 * the lists of the parts before it.
 */
function withOffset<T extends { partitionIndex?: number }>(entry: T, offset: number): T {
	return entry.partitionIndex === undefined ? entry : { ...entry, partitionIndex: offset + entry.partitionIndex };
}

/** The entry of the partition that raised a forwarded error, from the head of its routing, with no counts. */
function raiserPartition(error: unknown): WirePartitionMeta | undefined {
	const raiser = routedError(error)?.routing.servedBy[0];
	if (!raiser) {
		return undefined;
	}
	return {
		actorId: raiser.actorId,
		...(isRangePartition(raiser.ref) ? { rangeDepth: raiser.rangeDepth } : { hashDepth: raiser.hashDepth }),
	};
}

/** The one key of a promotion plan: its single target carries a `promoted_key` slice. */
function promotedKeyOf(plan: FokosRepartitionPlan): KeyBytes {
	const slice = plan.targets[0]?.slice;
	invariant(slice?.kind === "promoted_key", "fokos/partition: a promotion plan carries one promoted_key slice");
	return slice.hashKey;
}

/** The migration status the partition suites read, taken from the import state. */
function derivedMigrationStatus(state: FokosImportState | undefined): PartitionStatusView["migrationStatus"] {
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

/** The promotion status the partition suites read, taken from the repartition state. */
function promotedKeyStatusOf(state: RepartitionState): PromotedKeyStatus {
	if (state === "queued" || state === "planned") {
		return "queued";
	}
	if (state === "cutover") {
		return "promoting";
	}
	return "promoted";
}

/** Transient: the partition is healthy but past its cap, and a split will bring it back under. */
function errExceededDatabaseSize(operationName: string): FokosUnavailableError {
	return new FokosUnavailableError(UNAVAILABLE_CODES.partition_over_size, {
		message: "partition exceeded its limits, please retry later",
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
