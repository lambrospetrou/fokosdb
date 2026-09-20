/**
 * PROTOTYPE. `TransactionCoordinatorDO` written as a second host of `FokosShardingRuntime`, so the coordinator
 * pool grows by hash splits instead of a fixed `numTxCoordinators`. A coordinator is keyed by the idempotency
 * token: `{ hashKey: token, sortKey: empty }`. It has no range tree, no promotions, and no read-through.
 *
 * Two things make this host different from the partition host, and both are what this file tests:
 *
 * 1. Its handlers are asynchronous. A 2PC driver awaits partitions between durable transitions, so a hash
 *    split can cut over while a transaction is in flight. Every transition therefore runs `fokos.owns(key)`
 *    inside its `transactionSync`, and a transition that finds the key gone stops driving and answers the
 *    caller with a retryable error. The retry reaches the new owner through the router, and the token replay
 *    resumes the transaction from the migrated ledger row.
 * 2. Partitions call `recoverTransaction` on it directly, with the route context the coordinator gave them
 *    at prepare time. It is an ordinary `point` operation, so a coordinator that has become a router forwards
 *    it like any other request.
 */
import { DurableObject } from "cloudflare:workers";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { encodeHashKey } from "../../src/shared/transaction-limits.js";
import type {
	InitiateWriteResponseEncoded,
	PrepareResponse,
	RecoverTransactionResult,
	TCState,
	TCWriteOperation,
} from "../../src/shared/transaction-wire-types.js";
import { FokosShardingRuntime, todo } from "./api.js";
import type { FokosEnvelope, FokosOperations, FokosRouteContext, FokosShardingHooks, FokosShardingRpc, RouteKey } from "./api.js";
import type * as Rpc from "./api.js";
import type { FokosDbPolicy, FokosDbRouteContext, PartitionRpc, PrepareReq } from "./fokosdb-partition-host.js";

// ─── wire types that change ──────────────────────────────────────────────────

export type TcWriteOperation = Omit<TCWriteOperation, "partitionContext"> & { partitionContext: FokosDbRouteContext };
export type InitiateWriteReq = { clientRequestToken?: string; items: TcWriteOperation[] };
export type RecoverTransactionReq = { transactionId: string; idempotencyToken: string };

export type CoordinatorOps = {
	initiateWrite: { req: InitiateWriteReq; res: InitiateWriteResponseEncoded };
	recoverTransaction: { req: RecoverTransactionReq; res: RecoverTransactionResult };
};

export type CoordinatorRpc = FokosShardingRpc & {
	[K in keyof CoordinatorOps]: (
		ctx: FokosDbRouteContext,
		req: CoordinatorOps[K]["req"],
	) => Promise<FokosEnvelope<CoordinatorOps[K]["res"]>>;
};

// ─── the host ────────────────────────────────────────────────────────────────

const STALE_THRESHOLD_MS = 30_000;
const IDEMPOTENCY_WINDOW_MS = 10 * 60_000;
const JOB_RECOVERY = "tx_recovery";
const JOB_SWEEP = "idempotency_sweep";

const noSortKey = KeyCodec.encodeOptional(undefined);
const tokenKey = (token: string): RouteKey => ({ hashKey: encodeHashKey(token), sortKey: noSortKey });

type Runtime = FokosShardingRuntime<FokosDbPolicy, CoordinatorOps>;

type TcStateRow = { transaction_id: string; idempotency_token: string; state: TCState; created_at: number; completed_at: number | null };

export class TransactionCoordinatorDO extends DurableObject<Env> implements CoordinatorRpc {
	readonly fokos: Runtime;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.fokos = new FokosShardingRuntime<FokosDbPolicy, CoordinatorOps>({
			ctx,
			stub: (routeCtx, doName) => todo(`txCoordinatorStubByName(${routeCtx.policy.nsTx}, ${doName})`),
			// A coordinator needs no range tree and no Bloom cache. The hash arena is the only cache it learns.
			hooks: coordinatorHooks(this),
			operations: coordinatorOperations(this),
		});
		void ctx.blockConcurrencyWhile(async () => todo("tc_state, tc_items, tc_participants, tc_results migrations"));
	}

	initiateWrite = (ctx: FokosDbRouteContext, req: InitiateWriteReq) => this.fokos.dispatch("initiateWrite", ctx, req);
	recoverTransaction = (ctx: FokosDbRouteContext, req: RecoverTransactionReq) => this.fokos.dispatch("recoverTransaction", ctx, req);

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

	// ─── the 2PC driver, unchanged in shape; each transition is guarded by ownership ───

	databaseSize(): number {
		return this.ctx.storage.sql.databaseSize;
	}

	/** A stub to a partition of another class. The host reads the binding from the participant's own policy. */
	partition(ctx: FokosDbRouteContext): PartitionRpc {
		return todo(`partitionStubByName(${ctx.policy.ns}, ${ctx.doName})`);
	}

	/**
	 * One durable transition. The ownership test runs inside the same synchronous block as the write, so a
	 * cutover cannot land between them. A key that moved raises the retryable error the client retries on.
	 */
	transition(token: string, write: () => void): void {
		this.ctx.storage.transactionSync(() => {
			if (!this.fokos.owns(tokenKey(token))) throw todo("FokosUnavailableError(coordinator_moved), retryable");
			write();
		});
	}

	async drive(row: TcStateRow): Promise<InitiateWriteResponseEncoded> {
		const token = row.idempotency_token;
		const participants = todo<Array<{ context: FokosDbRouteContext; request: PrepareReq }>>("tc_participants and tc_items of the row");
		switch (row.state) {
			case "CREATED":
			case "PREPARING": {
				this.transition(token, () => todo("state = PREPARING"));
				const settled = await Promise.allSettled(
					participants.map(async (p) => (await this.partition(p.context).txPrepare(p.context, p.request)).value),
				);
				const answers = settled.map((s) => (s.status === "fulfilled" ? s.value : null));
				const allAccepted = answers.every((a): a is PrepareResponse => a?.outcome === "accepted");
				this.transition(token, () => todo(allAccepted ? "state = PREPARED" : "state = CANCELLING, results"));
				return await this.drive({ ...row, state: allAccepted ? "PREPARED" : "CANCELLING" });
			}
			case "PREPARED":
			case "COMMITTING": {
				this.transition(token, () => todo("state = COMMITTING"));
				await Promise.all(
					participants.map(async (p) => (await this.partition(p.context).txCommit(p.context, todo("CommitRequest of p"))).value),
				);
				this.transition(token, () => todo("state = COMMITTED, completed_at"));
				return { outcome: "committed", transactionId: row.transaction_id, idempotencyToken: token };
			}
			case "CANCELLING": {
				await Promise.allSettled(
					participants.map(async (p) => (await this.partition(p.context).txCancel(p.context, todo("CancelRequest of p"))).value),
				);
				this.transition(token, () => todo("state = CANCELLED, completed_at"));
				return { outcome: "cancelled", transactionId: row.transaction_id, idempotencyToken: token, results: todo("results_json") };
			}
			case "COMMITTED":
				return { outcome: "committed", transactionId: row.transaction_id, idempotencyToken: token };
			case "CANCELLED":
				return { outcome: "cancelled", transactionId: row.transaction_id, idempotencyToken: token, results: todo("results_json") };
		}
	}
}

// ─── operations ──────────────────────────────────────────────────────────────

function coordinatorOperations(host: TransactionCoordinatorDO): FokosOperations<CoordinatorOps> {
	return {
		initiateWrite: {
			shape: "point",
			whileMigrating: "retry",
			localMode: "async",
			admissionTag: "write",
			key: (req) => tokenKey(req.clientRequestToken ?? todo("a token generated by db.ts; the key must be known before dispatch")),
			local: async (req, call) => {
				const token = req.clientRequestToken!;
				const existing = todo<TcStateRow | undefined>("loadStateRowByToken(token)");
				if (existing) return await host.drive(existing);
				// The insert is the first transition, so it is ownership-guarded like the others. The prepare
				// request hands every participant this coordinator's own route context, which the partition
				// stores in its lock row and calls back on recovery.
				const row = todo<TcStateRow>(`insert CREATED for ${token} with coordinator = fokos.routeContext()`);
				host.transition(token, () => todo("insert tc_state, tc_items, tc_participants"));
				call.signal({ evaluateSplit: true, jobs: [{ name: JOB_RECOVERY, runAt: Date.now() + STALE_THRESHOLD_MS }] });
				return await host.drive(row);
			},
		},
		recoverTransaction: {
			shape: "point",
			whileMigrating: "retry",
			localMode: "async",
			key: (req) => tokenKey(req.idempotencyToken),
			local: async (req) => {
				const row = todo<TcStateRow | undefined>(`loadStateRow(${req.transactionId})`);
				if (!row) return { state: "not_found" };
				if (row.state === "COMMITTED" || row.state === "CANCELLED") return { state: row.state };
				void host.drive(row).catch(() => host.fokos.scheduleJob(JOB_RECOVERY, Date.now()));
				return { state: "driving" };
			},
		},
	};
}

// ─── hooks ───────────────────────────────────────────────────────────────────

function coordinatorHooks(host: TransactionCoordinatorDO): FokosShardingHooks<FokosDbPolicy> {
	const { fokos } = host;
	const active = () => {
		const lc = fokos.lifecycle();
		return lc.role === "owner" && (lc.import === null || lc.import.state === "active");
	};
	return {
		// The coordinator reuses the hash split threshold of the table. A dedicated field is a policy change, not a runtime change.
		evaluateSplit: ({ policy }) => (host.databaseSize() > policy.hashSplitConditions.maxSizeMb * 1024 * 1024 ? {} : false),
		admit: ({ admissionTag, policy }) =>
			admissionTag === "write" && host.databaseSize() > policy.hashSplitConditions.maxSizeMb * 1024 * 1024 * 1.1
				? { reject: todo("FokosUnavailableError(coordinator_over_size)") }
				: "allow",
		migration: {
			// A page is a batch of transactions with their items, participants, and results, keyed by token.
			buildPage: (cursor, _slice, belongsToTarget) => {
				const rows = todo<TcStateRow[]>(`tc_state after ${cursor}, bounded`);
				const mine = rows.filter((r) => belongsToTarget(tokenKey(r.idempotency_token)));
				return { page: todo(`rows of ${mine.length} transactions`), nextCursor: rows.length === 0 ? null : rows.at(-1)!.transaction_id };
			},
			applyPage: () => todo("INSERT OR REPLACE the four tables"),
			validatePage: () => undefined,
		},
		jobs: [
			{
				name: JOB_RECOVERY,
				canRun: active,
				deadline: () => todo<number | null>("MIN(created_at) + STALE_THRESHOLD_MS over non-terminal rows"),
				runStep: async () => {
					for (const row of todo<TcStateRow[]>("stale non-terminal rows, LIMIT 100")) {
						await host.drive(row).catch(() => undefined);
					}
					return { nextRunAt: todo<number | null>("MIN(created_at) + STALE_THRESHOLD_MS, or null") };
				},
			},
			{
				name: JOB_SWEEP,
				canRun: active,
				deadline: () => todo<number | null>("MIN(completed_at) + IDEMPOTENCY_WINDOW_MS"),
				runStep: () => {
					todo(`DELETE rows with completed_at < now - ${IDEMPOTENCY_WINDOW_MS}, bounded`);
					return { nextRunAt: todo<number | null>("next MIN(completed_at) + IDEMPOTENCY_WINDOW_MS") };
				},
			},
		],
	};
}

export type CoordinatorRouteContext = FokosRouteContext<FokosDbPolicy>;
