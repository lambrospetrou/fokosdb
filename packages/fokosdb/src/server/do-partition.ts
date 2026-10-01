import { DurableObject } from "cloudflare:workers";
import { DataKind, OperationMetrics, type QuerySelect, type ReturnValuesOnConditionCheckFailure } from "../shared/types.js";
import type { CompiledConditionPlan, CompiledProjectionPlan, CompiledQueryPlan } from "../shared/expression/plan.js";
import type { ProjectedWireRow } from "../shared/expression/projection.js";
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
	TransactionItemKey,
	TransactionReadItem,
} from "../shared/transaction-wire-types.js";
import type { FokosPartitionIdentity, FokosPartitionRef } from "../sharding/route-context.js";
import type { FokosDBPolicy, FokosDBRouteContext } from "../shared/partition-context.js";
import { identityDepth } from "../sharding/partition-id.js";
import { KeyCodec, type KeyBytes } from "../sharding/key-codec.js";
import type { SplitType } from "../sharding/types.js";
import invariant from "../shared/invariant.js";
import {
	estimateItemBytes,
	estimateProjectedRowBytes,
	PartitionStore,
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
	FokosRequestPromotionRequest,
	FokosShardingRpc,
	FokosStartImportRequest,
	FokosStatusCursor,
	FokosStatusEntry,
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
	UNAVAILABLE_CODES,
} from "../shared/errors.js";
import { SHARDING_INTERNAL_CODES, SHARDING_ROUTING_CODES } from "../sharding/errors.js";

// ─── item RPC types ───────────────────────────────────────────────────────────

/**
 * Wire types for the item RPCs (db.ts → PartitionDO). Keys are canonical KeyBytes, encoded at the
 * db.ts entry, and `sortKey` is always present — the empty KeyBytes ([]) is the absent sentinel.
 * This matches the transaction and query RPCs, so every key crossing into a DO has one form.
 *
 * No response carries a key: `db.ts` answers with the caller's own keys, which are the only ones the
 * caller can recognise. No response carries a routing fact either: the envelope around it does.
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
	| { outcome: "ok"; version: number; meta: OperationMetrics }
	| { outcome: "rejected"; reason: RejectionReasonEncoded; meta: OperationMetrics };

export type DeleteItemRpcRequest = ItemRpcKeys & {
	condition?: CompiledConditionPlan;
	returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;
};

export type DeleteItemRpcResponse =
	| { outcome: "ok"; deleted: boolean; meta: OperationMetrics }
	| { outcome: "rejected"; reason: RejectionReasonEncoded; meta: OperationMetrics };

export type GetItemRpcRequest = ItemRpcKeys & { projection?: CompiledProjectionPlan };

// json data is JSON text here; db.ts parses it once at the public boundary. The type is free of the
// recursive JsonValue so the Workers-RPC type machinery does not instantiate infinitely deep.
//
// A projected read carries the positional row inside `item`, so `kind`, `version`, and `ttlAt` are
// common to both found variants. `kind` is then `"projected"`, which is a read-result tag and never
// a stored `data_kind`.
export type GetItemRpcResponse =
	| { found: true; item: { data: string | Uint8Array; kind: DataKind; ttlAt?: number; version: number }; meta: OperationMetrics }
	| { found: true; item: { projected: ProjectedWireRow; kind: "projected"; ttlAt?: number; version: number }; meta: OperationMetrics }
	| { found: false; meta: OperationMetrics };

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
	/** The compiled filter/projection plan, or null for a request with neither. */
	plan: CompiledQueryPlan | null;
};

/** The metrics of one leaf scan, named by the leaf. `db.ts` pairs it with the route list of the envelope. */
export type LeafMetrics = OperationMetrics & { partitionId: string };

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
	/** Leaf-only: hash leaves and non-split range partitions that scanned rows. Routers appear in the envelope only. */
	partitionMetas: LeafMetrics[];
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
	promotedKeys: { hashKey: KeyBytes; status: PromotedKeyStatus }[];
};

// ─── the operation spec ───────────────────────────────────────────────────────

/** The operations of the partition. The registry, `dispatch`, `forward`, and the RPC surface derive from it. */
export type PartitionOps = {
	apiPutItem: { req: PutItemRpcRequest; res: PutItemRpcResponse };
	apiGetItem: { req: GetItemRpcRequest; res: GetItemRpcResponse };
	apiDeleteItem: { req: DeleteItemRpcRequest; res: DeleteItemRpcResponse };
	apiQueryItems: { req: QueryItemsRpcRequest; res: QueryItemsRpcResponse };
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
			txPrepare: {
				shape: "group",
				whileMigrating: "throw",
				admissionTag: "write",
				failurePolicy: "fail_fast",
				items: (req) => req.items.map((item) => ({ key: keyOf(item), item })),
				subRequest: (req, items) => ({ ...req, items: items as TransactionItem[] }),
				local: (req, call) => {
					if (req.items.length === 0) {
						return { outcome: "accepted" };
					}
					const response = this.#participant.prepareLocal(req);
					// The lock is durable, so its recovery deadline must be too, even when the coordinator never returns.
					if (response.outcome === "accepted") {
						call.signal({ jobs: [{ name: JOB_STALE_TX_RECOVERY, runAt: this.fokosNow() + this.config().staleTransactionMs }] });
					}
					return response;
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
					return response;
				},
				merge: () => ({ outcome: "committed" }),
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
					return { outcome: "cancelled" };
				},
				merge: () => ({ outcome: "cancelled" }),
			},
			txReadForTransaction: {
				shape: "group",
				whileMigrating: "throw",
				admissionTag: "read",
				failurePolicy: "fail_fast",
				items: (req) => req.items.map((item) => ({ key: keyOf(item), item })),
				subRequest: (req, items) => ({ ...req, items: items as TransactionReadItem[] }),
				local: (req) => this.#participant.readForTransactionLocal(req),
				merge: (parts) => ({ items: parts.flatMap((p) => p.result.items) }),
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
						return { outcome: "aborted", reason: "pending_write" };
					}
					return { outcome: "committed", items };
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
					return response;
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
						return { ...response.value, ...counts };
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
			// Each target now holds its own locks, so the copies here are not necessary. One delete
			// removes them all, so no call can send on only a part of one transaction. A promotion
			// deletes the copies of its key only, because the other keys stay here. A split deletes
			// all copies.
			//
			// A promotion also deletes the size estimate of its key. Every new write of the key goes to
			// the range tree, so nothing here reads the estimate again. Without the delete, the key stays
			// in the promotion list of the split decision until the cleanup deletes its last row, and
			// the split behind it waits. The cleanup keeps its own delete of the estimate as a guard.
			beforeComplete: (plan) => {
				if (plan.kind === "key_promotion") {
					const hashKey = promotedKeyOf(plan);
					this.#store.deletePendingTxForHashKey(hashKey);
					this.#store.deleteKeySizeEstimate(hashKey);
					return;
				}
				this.#store.deleteAllPendingTx();
			},
			// A split keeps its item rows. Only a promotion has rows to give back: its key moved, and
			// the rest of its keys stay here.
			cleanupSourceStep: (plan) => {
				if (plan.kind !== "key_promotion") {
					return true;
				}
				const hashKey = promotedKeyOf(plan);
				this.#store.deleteItemsBatchForHashKey(hashKey, this.config().promotedKeyCleanupRows);
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
				const identity = this.fokos.identity();
				const maxSizeMb = (identity.kind === "hash" ? policy.hashSplitConditions : policy.rangeSplitConditions)?.maxSizeMb;
				if (maxSizeMb && this.#store.databaseSize > maxSizeMb * ADMISSION_MARGIN * 1024 * 1024) {
					return { reject: errExceededDatabaseSize(op) };
				}
				return "allow";
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

	// ═══ local handlers ══════════════════════════════════════════════════════

	private putItemLocal(req: PutItemRpcRequest, call: FokosLocalCall): PutItemRpcResponse {
		const { hashKey, sortKey } = req;
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
				meta: this.metrics(localRes.image ? sumSqlMetrics(localRes.conditionRes, localRes.image) : localRes.conditionRes),
			};
		}

		const { writeRes, conditionRes } = localRes;
		// The row is committed. The handler learns `keyEstBytes` from the upsert result, and the response
		// does not carry it, so the promotion candidate is reported from here.
		this.signalGrowth(call, [{ hashKey, keyEstBytes: writeRes.keyEstBytes }]);
		return {
			outcome: "ok",
			version: writeRes.version,
			meta: this.metrics(conditionRes ? sumSqlMetrics(conditionRes, writeRes) : writeRes),
		};
	}

	private deleteItemLocal(req: DeleteItemRpcRequest): DeleteItemRpcResponse {
		const { hashKey, sortKey } = req;
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
				meta: this.metrics(localRes.image ? sumSqlMetrics(localRes.conditionRes, localRes.image) : localRes.conditionRes),
			};
		}

		const { writeRes, conditionRes } = localRes;
		return {
			outcome: "ok",
			deleted: writeRes.deleted,
			meta: this.metrics(conditionRes ? sumSqlMetrics(conditionRes, writeRes) : writeRes),
		};
	}

	private readItemLocally(req: GetItemRpcRequest): GetItemRpcResponse {
		const res =
			req.projection === undefined
				? this.#store.getItem(req.hashKey, req.sortKey)
				: this.#store.getItemProjected(req.projection, req.hashKey, req.sortKey);
		const { rowsRead, rowsWritten } = res;
		const result = res.row;
		const meta = this.metrics({ rowsRead, rowsWritten });
		if (!result) {
			return { found: false, meta };
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
				meta,
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
			meta,
		};
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
		// is the only kind that contributes a `partitionMetas` entry. Routers appear in the envelope only.
		return {
			items: page.items,
			count: page.count,
			scannedCount: page.scannedCount,
			evaluatedBytes: page.evaluatedBytes,
			responseBytes: page.responseBytes,
			rowsReturned: page.rowsReturned,
			lastEvaluatedCursor: page.lastEvaluatedCursor,
			nextCursor: page.nextCursor,
			partitionMetas: [{ ...this.metrics({ rowsRead, rowsWritten }), partitionId: this.fokos.identity().ref.partitionId }],
		};
	}

	/** The metrics of work this node did itself. */
	private metrics(counts: { rowsRead: number; rowsWritten: number }): OperationMetrics {
		return { rowsRead: counts.rowsRead, rowsWritten: counts.rowsWritten, databaseSize: this.#store.databaseSize };
	}

	// ═══ the test view ═══════════════════════════════════════════════════════

	/**
	 * The view the partition suites read. Every field derives from the runtime's public surface on
	 * each call, and nothing persists it. A destroy traversal reads the paginated `fokosStatus`
	 * instead.
	 */
	private async statusView(): Promise<PartitionStatusView> {
		const identity = this.fokos.identity();
		const routeContext = this.fokos.routeContext();
		const importInfo = this.fokos.lifecycle().import;
		const entries: FokosStatusEntry[] = [];
		let cursor: FokosStatusCursor | null = null;
		do {
			const page = await this.fokos.fokosStatus({ cursor });
			entries.push(...page.entries);
			cursor = page.nextCursor;
		} while (cursor !== null);

		const split = entries.find((e) => e.repartition.kind !== "key_promotion")?.repartition;
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
					migratedChildDoNames: entries
						.filter((e) => e.repartition.id === split.id && e.target?.acknowledged)
						.map((e) => e.target!.ref.doName),
				};
			}
		}

		const promotedKeys: PartitionStatusView["promotedKeys"] = [];
		const seen = new Set<string>();
		for (const { repartition } of entries) {
			if (repartition.kind !== "key_promotion" || repartition.hashKey === null || seen.has(repartition.id)) {
				continue;
			}
			seen.add(repartition.id);
			promotedKeys.push({ hashKey: repartition.hashKey, status: promotedKeyStatusOf(repartition.state) });
		}

		return {
			depth: identityDepth(identity),
			partitionContext: routeContext,
			identityStored: identity,
			splitStatus,
			migrationStatus: derivedMigrationStatus(importInfo?.state),
			parentPartitionContext: importInfo?.source,
			parentSplitType: importInfo ? (importInfo.slice.kind === "hash_child" ? "hash" : "range") : undefined,
			promotedKeys,
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
		partitionMetas: [],
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
		out.partitionMetas.push(...part.partitionMetas);
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
 * arrived. An accepted part sends no array, so its operations passed.
 */
function mergePrepare(parts: Array<FokosGroupPart<PrepareRequest, PrepareResponse>>): PrepareResponse {
	const executionFailure = parts.find((p) => p.result.outcome === "rejected" && !p.result.results);
	if (executionFailure) {
		return executionFailure.result;
	}
	if (!parts.some((p) => p.result.outcome === "rejected")) {
		return { outcome: "accepted" };
	}
	const merged: ParticipantOperationResultEncoded[] = [];
	for (const { request, result } of parts) {
		if (result.outcome === "accepted") {
			merged.push(...request.items.map((item) => ({ outcome: "passed" as const, opIndex: item.opIndex })));
		} else {
			merged.push(...result.results);
		}
	}
	applyImageCap(merged);
	return { outcome: "rejected", results: merged };
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
