/**
 * PROTOTYPE. `PartitionDO` written as a host of `FokosShardingRuntime`, against the real FokosDB wire types.
 * Storage calls are `todo()`; every routing, merge, and walk decision is written out, because those are what
 * the runtime API must carry. This file is type-checked and never runs.
 */
import { DurableObject } from "cloudflare:workers";
import type { CompiledQueryPlan } from "../../src/shared/expression/plan.js";
import type { ProjectedWireRow } from "../../src/shared/expression/projection.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import type { PartitionStore, PromotedKeyStatus, ScanCursor, StoredItem } from "../../src/shared/partition/partition-store.js";
import type { PromotionCandidate, TransactionParticipant } from "../../src/shared/partition/transaction-participant.js";
import { QueryPageBudget } from "../../src/shared/query/page-budget.js";
import {
	clipToChildRange,
	cursorFallsInChild,
	isChildFullyBeforeCursor,
	makeBoundaryCursor,
	rangeIntersects,
} from "../../src/sharding/sk-interval.js";
import type {
	CancelRequest,
	CancelResponse,
	CommitRequest,
	CommitResponse,
	DebugForceResolveTransactionRequest,
	DebugForceResolveTransactionResponse,
	ParticipantOperationResultEncoded,
	PrepareResponse,
	ReadForTransactionRequest,
	ReadForTransactionResponse,
	ReadSnapshotRequest,
	ReadSnapshotResponse,
	SingleShotRequest,
	SingleShotResponse,
	TransactionItem,
	TransactionItemKey,
	TransactionTimestamp,
} from "../../src/shared/transaction-wire-types.js";
import type { OperationMetrics, QuerySelect } from "../../src/shared/types.js";
import type { PutItemRpcRequest, DeleteItemRpcRequest, GetItemRpcRequest } from "../../src/server/do-partition.js";
import type { PutItemRpcResponse, DeleteItemRpcResponse, GetItemRpcResponse } from "../../src/server/do-partition.js";
import { FokosShardingRuntime, todo } from "./api.js";
import type {
	FokosEnvelope,
	FokosGroupPart,
	FokosLocalCall,
	FokosOperations,
	FokosPartitionRef,
	FokosRangeVisit,
	FokosRouteContext,
	FokosShardingHooks,
	FokosShardingRpc,
	KeyBytes,
	RouteKey,
	SkInterval,
} from "./api.js";
import type * as Rpc from "./api.js";

// ─── policy ──────────────────────────────────────────────────────────────────

/**
 * Everything of the old `PartitionContext` that is not identity or topology. Opaque to the runtime. Both FokosDB
 * hosts share it. Neither needs the topology of the other group: every cross-class call carries the full route
 * context of its target, and a lock row stores the coordinator's.
 */
export type FokosDbPolicy = {
	ns: string;
	nsTx: string;
	locationHint?: DurableObjectLocationHint;
	hashSplitConditions: { maxSizeMb: number };
	rangeSplitConditions: { maxSizeMb: number };
};

export type FokosDbRouteContext = FokosRouteContext<FokosDbPolicy>;

// ─── wire types that change ──────────────────────────────────────────────────

/** `meta` keeps the operation metrics only. Routing facts move to the envelope. */
type WithMetrics<R> = R extends { meta: unknown } ? Omit<R, "meta"> & { meta: OperationMetrics } : R;

export type PutRes = WithMetrics<PutItemRpcResponse>;
export type DeleteRes = WithMetrics<DeleteItemRpcResponse>;
export type GetRes = WithMetrics<GetItemRpcResponse>;

export type QueryReq = {
	hashKey: KeyBytes;
	interval: SkInterval;
	direction: "asc" | "desc";
	remainingEvaluatedItems: number;
	remainingEvaluatedBytes: number;
	remainingResponseBytes: number;
	remainingPartitionVisits: number;
	allowOversizedFirstItem: boolean;
	cursor: ScanCursor | null;
	select: QuerySelect;
	plan: CompiledQueryPlan | null;
};

/** One leaf's metrics keep the leaf identity, so the client can pair them with the route list. */
export type LeafMetrics = OperationMetrics & { partitionId: string };

export type QueryRes = {
	items: Array<StoredItem | ProjectedWireRow>;
	count: number;
	scannedCount: number;
	evaluatedBytes: number;
	responseBytes: number;
	rowsReturned: number;
	lastEvaluatedCursor: ScanCursor | null;
	nextCursor: ScanCursor | null;
	partitionMetas: LeafMetrics[];
};

/**
 * `coordinatorDoId` becomes the coordinator's route context and the token it is keyed by. The stale recovery
 * job calls that coordinator through its own runtime, which forwards when the coordinator has split.
 */
export type PrepareReq = {
	transactionId: string;
	idempotencyToken: string;
	coordinator: FokosRouteContext<FokosDbPolicy>;
	transactionTimestamp: TransactionTimestamp;
	items: TransactionItem[];
};

export type DebugForcePromoteKeyRes = { queued: boolean; status: PromotedKeyStatus | undefined };

// ─── the operation spec: one place, and every signature derives from it ──────

export type PartitionOps = {
	apiPutItem: { req: PutItemRpcRequest; res: PutRes };
	apiGetItem: { req: GetItemRpcRequest; res: GetRes };
	apiDeleteItem: { req: DeleteItemRpcRequest; res: DeleteRes };
	apiQueryItems: { req: QueryReq; res: QueryRes };
	txPrepare: { req: PrepareReq; res: PrepareResponse };
	txCommit: { req: CommitRequest; res: CommitResponse };
	txCancel: { req: CancelRequest; res: CancelResponse };
	txReadForTransaction: { req: ReadForTransactionRequest; res: ReadForTransactionResponse };
	txReadSnapshot: { req: ReadSnapshotRequest; res: ReadSnapshotResponse };
	txExecuteSingleShot: { req: SingleShotRequest; res: SingleShotResponse };
	debugForceResolveTransaction: { req: DebugForceResolveTransactionRequest; res: DebugForceResolveTransactionResponse };
	debugForcePromoteKey: { req: { hashKey: KeyBytes }; res: DebugForcePromoteKeyRes };
};

/** The RPC surface of the class, derived from the spec. `db.ts` and the coordinator type their stubs with it. */
export type PartitionRpc = FokosShardingRpc & {
	[K in keyof PartitionOps]: (ctx: FokosDbRouteContext, req: PartitionOps[K]["req"]) => Promise<FokosEnvelope<PartitionOps[K]["res"]>>;
};

// ─── constants the host owns ─────────────────────────────────────────────────

const STALE_TX_MS = 60_000;
const RANGE_PROMOTION_FRACTION = 0.5;
const JOB_STALE_TX = "stale_tx_recovery";

// ─── the host ────────────────────────────────────────────────────────────────

type Runtime = FokosShardingRuntime<FokosDbPolicy, PartitionOps>;

export class PartitionDO extends DurableObject<Env> implements PartitionRpc {
	readonly fokos: Runtime;
	declare readonly store: PartitionStore;
	declare readonly participant: TransactionParticipant;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.fokos = new FokosShardingRuntime<FokosDbPolicy, PartitionOps>({
			ctx,
			// Host code: it reads the binding and the location hint from its own policy.
			stub: (routeCtx, doName) => todo(`partitionStubByName(${routeCtx.policy.ns}, ${doName})`),
			hooks: partitionHooks(this),
			operations: partitionOperations(this),
		});
		void ctx.blockConcurrencyWhile(async () => todo("host migrations"));
	}

	// One line per public method. The name of the method is the name of the operation.
	apiPutItem = (ctx: FokosDbRouteContext, req: PutItemRpcRequest) => this.fokos.dispatch("apiPutItem", ctx, req);
	apiGetItem = (ctx: FokosDbRouteContext, req: GetItemRpcRequest) => this.fokos.dispatch("apiGetItem", ctx, req);
	apiDeleteItem = (ctx: FokosDbRouteContext, req: DeleteItemRpcRequest) => this.fokos.dispatch("apiDeleteItem", ctx, req);
	apiQueryItems = (ctx: FokosDbRouteContext, req: QueryReq) => this.fokos.dispatch("apiQueryItems", ctx, req);
	txPrepare = (ctx: FokosDbRouteContext, req: PrepareReq) => this.fokos.dispatch("txPrepare", ctx, req);
	txCommit = (ctx: FokosDbRouteContext, req: CommitRequest) => this.fokos.dispatch("txCommit", ctx, req);
	txCancel = (ctx: FokosDbRouteContext, req: CancelRequest) => this.fokos.dispatch("txCancel", ctx, req);
	txReadForTransaction = (ctx: FokosDbRouteContext, req: ReadForTransactionRequest) =>
		this.fokos.dispatch("txReadForTransaction", ctx, req);
	txReadSnapshot = (ctx: FokosDbRouteContext, req: ReadSnapshotRequest) => this.fokos.dispatch("txReadSnapshot", ctx, req);
	txExecuteSingleShot = (ctx: FokosDbRouteContext, req: SingleShotRequest) => this.fokos.dispatch("txExecuteSingleShot", ctx, req);
	debugForceResolveTransaction = (ctx: FokosDbRouteContext, req: DebugForceResolveTransactionRequest) =>
		this.fokos.dispatch("debugForceResolveTransaction", ctx, req);
	debugForcePromoteKey = (ctx: FokosDbRouteContext, req: { hashKey: KeyBytes }) => this.fokos.dispatch("debugForcePromoteKey", ctx, req);

	fokosInit = (req: Rpc.FokosInitRequest) => this.fokos.fokosInit(req);
	fokosStartImport = (req: Rpc.FokosStartImportRequest) => this.fokos.fokosStartImport(req);
	fokosMigrationPull = (req: Rpc.FokosMigrationPullRequest) => this.fokos.fokosMigrationPull(req);
	fokosMigrationAck = (req: Rpc.FokosMigrationAckRequest) => this.fokos.fokosMigrationAck(req);
	fokosExecuteLocal = (req: Rpc.FokosExecuteLocalRequest) => this.fokos.fokosExecuteLocal(req);
	fokosRequestPromotion = (req: Rpc.FokosRequestPromotionRequest) => this.fokos.fokosRequestPromotion(req);
	fokosStatus = (req: Rpc.FokosStatusRequest) => this.fokos.fokosStatus(req);
	fokosPrepareDestroy = (req: Rpc.FokosPrepareDestroyRequest) => this.fokos.fokosPrepareDestroy(req);
	fokosDestroy = () => this.fokos.fokosDestroy();
	alarm = (info: AlarmInvocationInfo) => this.fokos.alarm(info);

	fokosStaleTransactionMs(): number {
		return STALE_TX_MS;
	}
}

// ─── operations ──────────────────────────────────────────────────────────────

const noSortKey = KeyCodec.encodeOptional(undefined);
const keyOf = (item: TransactionItemKey): RouteKey => ({ hashKey: item.hashKey, sortKey: item.sortKey });

/** Signals a write handler reports after its storage transaction committed. */
function writeSignals(call: FokosLocalCall, candidates: PromotionCandidate[], policy: FokosDbPolicy): void {
	const cap = policy.hashSplitConditions.maxSizeMb * 1024 * 1024 * RANGE_PROMOTION_FRACTION;
	call.signal({
		evaluateSplit: true,
		promotionCandidates: candidates.filter((c) => c.keyEstBytes > cap).map((c) => ({ hashKey: c.hashKey })),
	});
}

function partitionOperations(host: PartitionDO): FokosOperations<PartitionOps> {
	const { fokos, store, participant } = host;
	return {
		apiPutItem: {
			shape: "point",
			whileMigrating: "retry",
			admissionTag: "write",
			key: (req) => ({ hashKey: req.hashKey, sortKey: req.sortKey }),
			local: (req, call) => {
				// The handler learns `keyEstBytes` from the upsert result. The response does not carry it, so the
				// promotion candidate can only be reported from inside the handler.
				const res = todo<{ response: PutRes; candidate: PromotionCandidate }>(`store.putItem(${req.kind})`);
				writeSignals(call, [res.candidate], fokos.policy());
				return res.response;
			},
		},
		apiDeleteItem: {
			shape: "point",
			whileMigrating: "retry",
			admissionTag: "ignore_size_reject",
			key: (req) => ({ hashKey: req.hashKey, sortKey: req.sortKey }),
			local: (_req, call) => {
				const response = todo<DeleteRes>("store.deleteItem");
				call.signal({ evaluateSplit: true });
				return response;
			},
		},
		apiGetItem: {
			shape: "point",
			whileMigrating: "read_source",
			readOnly: true,
			admissionTag: "read",
			key: (req) => ({ hashKey: req.hashKey, sortKey: req.sortKey }),
			local: (req) => todo<GetRes>(`store.getItem(${req.projection ? "projected" : "full"})`),
		},
		apiQueryItems: {
			shape: "range",
			whileMigrating: "read_source",
			readOnly: true,
			admissionTag: "read",
			range: (req) => ({ hashKey: req.hashKey, interval: req.interval, descending: req.direction === "desc" }),
			clip: (req, visit) => ({
				...req,
				interval: clipToChildRange(req.interval, visit.start, visit.end),
				cursor: req.cursor && cursorFallsInChild(visit.start ?? noSortKey, visit.end, req.cursor) ? req.cursor : null,
			}),
			local: (req) => queryLocal(store, req),
			walk: ({ request, visits, local, forward }) => walkRange(request, visits, local, forward),
		},
		txPrepare: {
			shape: "group",
			whileMigrating: "retry",
			admissionTag: "write",
			failurePolicy: "fail_fast",
			items: (req) => req.items.map((item) => ({ key: keyOf(item), item })),
			subRequest: (req, items) => ({ ...req, items: items as TransactionItem[] }),
			local: (req, call) => {
				if (req.items.length === 0) return { outcome: "accepted" };
				const response = participant.prepareLocal(todo("PrepareReq to PrepareRequest, the coordinator field differs"));
				if (response.outcome === "accepted")
					call.signal({ jobs: [{ name: JOB_STALE_TX, runAt: Date.now() + host.fokosStaleTransactionMs() }] });
				return response;
			},
			merge: mergePrepare,
		},
		txCommit: {
			shape: "group",
			whileMigrating: "retry",
			admissionTag: "ignore_size_reject",
			failurePolicy: "attempt_all",
			items: (req) => req.items.map((item) => ({ key: keyOf(item), item })),
			subRequest: (req, items) => ({ ...req, items: items as TransactionItemKey[] }),
			local: (req, call) => {
				const { response, promotionCandidates } = participant.commitLocal(req);
				writeSignals(call, promotionCandidates, fokos.policy());
				call.signal({ repartitionUnblocked: true });
				return response;
			},
			merge: () => ({ outcome: "committed" }),
		},
		txCancel: {
			shape: "group",
			whileMigrating: "retry",
			admissionTag: "ignore_size_reject",
			failurePolicy: "attempt_all",
			items: (req) => req.items.map((item) => ({ key: keyOf(item), item })),
			subRequest: (req, items) => ({ ...req, items: items as TransactionItemKey[] }),
			// Every hop releases by transaction id, owner or router, before the remote groups start.
			beforeForward: (req, call) => {
				participant.cancelLocal(req.transactionId);
				call.signal({ repartitionUnblocked: true });
			},
			local: () => ({ outcome: "cancelled" }),
			merge: () => ({ outcome: "cancelled" }),
		},
		txReadForTransaction: {
			shape: "group",
			whileMigrating: "retry",
			admissionTag: "read",
			failurePolicy: "fail_fast",
			items: (req) => req.items.map((item) => ({ key: keyOf(item), item })),
			subRequest: (req, items) => ({ ...req, items: items as ReadForTransactionRequest["items"] }),
			local: (req) => participant.readForTransactionLocal(req),
			merge: (parts) => ({ items: parts.flatMap((p) => p.result.items) }),
		},
		txReadSnapshot: {
			shape: "single_owner",
			whileMigrating: "retry",
			admissionTag: "read",
			items: (req) => req.items.map((item) => ({ key: keyOf(item) })),
			notApplicable: { outcome: "not_applicable" },
			local: (req) => {
				const { items } = participant.readForTransactionLocal(req);
				return items.some((item) => item.hasPendingWrite)
					? { outcome: "aborted", reason: "pending_write" }
					: { outcome: "committed", items };
			},
		},
		txExecuteSingleShot: {
			shape: "single_owner",
			whileMigrating: "retry",
			admissionTag: "write",
			items: (req) => req.items.map((item) => ({ key: keyOf(item) })),
			notApplicable: { outcome: "not_applicable" },
			local: (req, call) => {
				const { response, promotionCandidates } = participant.executeSingleShot(req);
				if (response.outcome !== "rejected") writeSignals(call, promotionCandidates, fokos.policy());
				return response;
			},
		},
		// A `local` operation that re-enters `dispatch`: the commit or cancel routes to the current owner of each key.
		debugForceResolveTransaction: {
			shape: "local",
			local: async (req) => {
				const pendingRows = store.listPendingTxItems(req.transactionId);
				const items = pendingRows.map((p) => ({ hashKey: p.hk, sortKey: p.sk }));
				const ctx = fokos.routeContext();
				const response =
					req.outcome === "commit"
						? await fokos.dispatch("txCommit", ctx, {
								transactionId: req.transactionId,
								transactionTimestamp: pendingRows[0]?.transaction_ts ?? 0,
								items,
							})
						: await fokos.dispatch("txCancel", ctx, { transactionId: req.transactionId, items });
				store.clearPendingTxGuard(req.transactionId);
				return response.value;
			},
		},
		debugForcePromoteKey: {
			shape: "local",
			local: async (req) => {
				const result = await fokos.requestPromotion(req.hashKey);
				if (result.queued) return { queued: true, status: promotedKeyStatusOf(result.state) };
				if (result.reason === "already_promoted") return { queued: false, status: promotedKeyStatusOf(result.state) };
				throw todo("errExceededDatabaseSize");
			},
		},
	};
}

function promotedKeyStatusOf(state: Rpc.RepartitionState): PromotedKeyStatus {
	return state === "queued" ? "queued" : state === "cleaned" ? "promoted" : "promoting";
}

function mergePrepare(parts: Array<FokosGroupPart<PrepareReq, PrepareResponse>>): PrepareResponse {
	const executionFailure = parts.find((p) => p.result.outcome === "rejected" && !p.result.results);
	if (executionFailure) return executionFailure.result;
	if (!parts.some((p) => p.result.outcome === "rejected")) return { outcome: "accepted" };
	// This node answers for every operation it was given. An accepted part sends no array, so its operations passed.
	const merged: ParticipantOperationResultEncoded[] = [];
	for (const { request, result } of parts) {
		if (result.outcome === "accepted") merged.push(...request.items.map((item) => ({ outcome: "passed" as const, opIndex: item.opIndex })));
		else merged.push(...result.results);
	}
	return { outcome: "rejected", results: merged };
}

function queryLocal(_store: PartitionStore, req: QueryReq): QueryRes {
	return todo(`store.scanQueryPage(${req.select})`);
}

/**
 * `walkRangeChildren` over the planned frontier. The runtime computed `visits` (children, learned descendants,
 * the local leaf) and counts the forwards; this function owns the budget, the cursor, and the early exits.
 */
async function walkRange(
	req: QueryReq,
	visits: readonly FokosRangeVisit[],
	local: (req: QueryReq) => QueryRes | Promise<QueryRes>,
	forward: (visit: FokosRangeVisit, req: QueryReq) => Promise<QueryRes>,
): Promise<QueryRes> {
	const { interval, cursor, direction } = req;
	const budget = new QueryPageBudget(req);
	const out: QueryRes = {
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

	// The visits are ordered by `descending` already. Drop those that cannot contribute to this page.
	const candidates = visits.filter((v) => {
		const start = v.start ?? noSortKey;
		return rangeIntersects(start, v.end, interval) && !(cursor && isChildFullyBeforeCursor(start, v.end, cursor, direction));
	});

	for (let i = 0; i < candidates.length; i++) {
		const visit = candidates[i];
		const hasLaterCandidate = i < candidates.length - 1;
		const sub: QueryReq = {
			...req,
			interval: clipToChildRange(interval, visit.start, visit.end),
			cursor: cursor && cursorFallsInChild(visit.start ?? noSortKey, visit.end, cursor) ? cursor : null,
			remainingEvaluatedItems: budget.remainingEvaluatedItems,
			remainingEvaluatedBytes: budget.remainingEvaluatedBytes,
			remainingResponseBytes: budget.remainingResponseBytes,
			remainingPartitionVisits: budget.remainingPartitionVisits,
			allowOversizedFirstItem: budget.allowOversizedFirstItem,
		};
		const part = visit.target === "local" ? await local(sub) : await forward(visit, sub);

		if (req.select === "projection") out.items.push(...part.items);
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
		if (budget.budgetExhausted) {
			if (hasLaterCandidate && out.lastEvaluatedCursor) out.nextCursor = out.lastEvaluatedCursor;
			break;
		}
		if (budget.visitsExhausted && hasLaterCandidate) {
			out.nextCursor = makeBoundaryCursor(req.hashKey, visit.start ?? noSortKey, visit.end, direction);
			break;
		}
	}
	return out;
}

// ─── hooks ───────────────────────────────────────────────────────────────────

function partitionHooks(host: PartitionDO): FokosShardingHooks<FokosDbPolicy> {
	const { fokos, store, participant } = host;
	return {
		evaluateSplit: ({ identity, policy }) => {
			const cap = (identity.kind === "hash" ? policy.hashSplitConditions : policy.rangeSplitConditions).maxSizeMb * 1024 * 1024;
			return store.databaseSize > cap ? {} : false;
		},
		computeRangeBoundaries: ({ hashKey, start, end, childCount }) =>
			todo(`store.rangeBoundaries(${hashKey.length}, ${childCount}, ${JSON.stringify([start, end]) ?? "null"})`),
		migration: todo("FokosDbMigrationHost, unchanged"),
		// A promotion cannot move a locked key. A split never holds: every lock follows its key to the child.
		beforeCutover: (plan) => plan.kind !== "key_promotion" || todo<number>("store.pendingLockCountForHashKey") === 0,
		beforeComplete: (plan) => (plan.kind === "key_promotion" ? undefined : todo("onSplitCompleted")),
		cleanupSourceStep: (plan) => (plan.kind === "key_promotion" ? todo<boolean>("delete promoted rows, one bounded step") : true),
		admit: ({ admissionTag, policy }) => {
			if (admissionTag !== "write") return "allow";
			const cap = policy.hashSplitConditions.maxSizeMb * 1024 * 1024 * 1.1;
			return store.databaseSize > cap ? { reject: todo("errExceededDatabaseSize") } : "allow";
		},
		jobs: [
			{
				name: JOB_STALE_TX,
				canRun: () => {
					const lc = fokos.lifecycle();
					return lc.role === "owner" && (lc.import === null || lc.import.state === "active");
				},
				deadline: () => todo<number | null>("store.earliestPendingTxCreatedAt() + STALE_TX_MS"),
				runStep: async () => {
					// Each stale lock is resolved by its own coordinator, then applied through `dispatch`, because the
					// keys of the lock can have moved to a child since the lock was written.
					for (const row of participant.listStaleTransactions(host.fokosStaleTransactionMs(), 10)) {
						const pending = todo<{ coordinator: FokosDbRouteContext; idempotencyToken: string }>(`pending row of ${row.transaction_id}`);
						// A stub to another class. The coordinator's runtime forwards when that coordinator has split.
						const tc = todo<import("./fokosdb-coordinator-host.js").CoordinatorRpc>(
							"txCoordinatorStub(env, ctx, pending.coordinator.doName)",
						);
						const result = await tc.recoverTransaction(pending.coordinator, {
							transactionId: row.transaction_id,
							idempotencyToken: pending.idempotencyToken,
						});
						const rows = store.listPendingTxItems(row.transaction_id);
						if (rows.length === 0) continue;
						const items = rows.map((p) => ({ hashKey: p.hk, sortKey: p.sk }));
						const ctx = fokos.routeContext();
						if (result.value.state === "COMMITTED") {
							await fokos.dispatch("txCommit", ctx, {
								transactionId: row.transaction_id,
								transactionTimestamp: rows[0].transaction_ts,
								items,
							});
						} else if (result.value.state === "CANCELLED" || result.value.state === "not_found") {
							await fokos.dispatch("txCancel", ctx, { transactionId: row.transaction_id, items });
						}
					}
					return { nextRunAt: Date.now() + host.fokosStaleTransactionMs() };
				},
			},
		],
	};
}

/** `PartitionDO` is a host of its own class only when `stub` returns one of its own kind. This keeps the type honest. */
export type PartitionStub = DurableObjectStub<PartitionDO>;
export type { FokosPartitionRef };
