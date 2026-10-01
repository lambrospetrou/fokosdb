import { DurableObject } from "cloudflare:workers";
import { SQLSchemaMigration, SQLSchemaMigrations } from "durable-utils/sql-migrations";
import type { FokosDBPolicy, FokosDBRouteContext, FokosDBTableConfig } from "../shared/partition-context.js";
import { KeyCodec, type KeyBytes } from "../sharding/key-codec.js";
import { FokosShardingRuntime } from "../sharding/runtime.js";
import type { FokosMigrationPageBudget, FokosRuntimeConfigOverrides } from "../sharding/runtime-config.js";
import type { FokosEnvelope, FokosLifecycle, FokosOperations, FokosShardingHooks, RouteKey } from "../sharding/runtime-types.js";
import type {
	FokosExecuteLocalRequest,
	FokosInitRequest,
	FokosMigrationAckRequest,
	FokosMigrationPage,
	FokosMigrationPullRequest,
	FokosPrepareDestroyRequest,
	FokosRequestPromotionRequest,
	FokosShardingRpc,
	FokosStartImportRequest,
	FokosStatusRequest,
} from "../sharding/repartition-types.js";
import { SHARDING_UNAVAILABLE_CODES } from "../sharding/errors.js";
import { FokosShardingClient, dropCallCost, type FokosRetryPolicy } from "../sharding/client.js";
import type { PartitionOps } from "./do-partition.js";
import { DATA_KINDS, type DataKind } from "../shared/types.js";
import type { ExecutionFailureCode } from "../shared/transaction-api-types.js";
import {
	COORDINATOR_REF_VERSION,
	type CoordinatorRef,
	type InitiateWriteRequest,
	type InitiateWriteResponseEncoded,
	type ParticipantOperationResultEncoded,
	type PrepareResponse,
	type RecoverTransactionRequest,
	type RecoverTransactionResult,
	type RejectionReasonEncoded,
	type TCState,
	type TransactWriteOperationResultEncoded,
	type TransactionItem,
	type TransactionItemKey,
} from "../shared/transaction-wire-types.js";
import { partitionStubByName, txCoordinatorStubByName } from "../shared/do-stubs.js";
import {
	FokosError,
	FokosInternalError,
	FokosTransactionPendingError,
	FokosUnavailableError,
	FokosValidationError,
	INTERNAL_CODES,
	TRANSACTION_PENDING_CODES,
	UNAVAILABLE_CODES,
	VALIDATION_CODES,
	type FokosErrorWire,
} from "../shared/errors.js";
import invariant from "../shared/invariant.js";
import { exists, one, tryOne } from "../shared/sql-cursor.js";
import { hashTransactionOperations } from "../shared/transaction-idempotency.js";
import { unexpectedTransactionStateError } from "../shared/errors-operations.js";
import {
	ADMISSION_MARGIN,
	applyImageCap,
	decodeItemKeys,
	DEFAULT_LIMITS,
	encodeHashKey,
	IDEMPOTENCY_WINDOW_MS,
	nextRecoveryAt,
	txOrderTimestampNow,
} from "../shared/transaction-limits.js";
import {
	maxPreparingHoldMs,
	resolveCoordinatorConfig,
	type ParticipantRetryConfig,
	type TransactionCoordinatorDOConfig,
	type TransactionCoordinatorDOConfigOverrides,
} from "./host-config.js";
import { CompiledConditionPlan, CompiledUpdatePlan } from "../shared/expression/plan.js";
import { parseJSONTrusted } from "../shared/tsutils.js";

type TcStateRow = {
	transaction_id: string;
	idempotency_token: string;
	state: TCState;
	transaction_ts: number;
	created_at: number;
	completed_at: number | null;
	/** The earliest time at which the `tx_recovery` job drives the transaction. NULL once it is complete. */
	next_recovery_at: number | null;
	/**
	 * Per-operation outcome array (TransactWriteOperationResultEncoded[]), ordered by request opIndex.
	 * Stores outcome codes, reasons (keys only), and itemOmitted markers.
	 * Item images are omitted and stored in tc_results to prevent exceeding the 2 MB SQLite row limit.
	 */
	results_json: string | null;
	/**
	 * Fingerprint of the operation set in request order.
	 * Detects token reuse across different operations and prevents mismatched replays.
	 */
	operations_hash: string;
};

type TcParticipantRow = {
	transaction_id: string;
	partition_do_name: string;
	partition_context_json: string;
	prepare_outcome: string | null;
	commit_outcome: string | null;
	cancel_outcome: string | null;
	/**
	 * Serialized PrepareResponse from a rejected participant with item images stripped.
	 * Retains opIndex, rejection reason (keys only), and imageBytes for coordinator capping.
	 * NULL for an accepted participant.
	 */
	answer_json: string | null;
	/** The FokosErrorWire of a prepare that threw after its retries, read only while prepare_outcome is NULL. */
	error_json: string | null;
};

/** One participant of the prepare fan-out: the partition and the items of the transaction it owns. */
type PrepareParticipant = {
	doName: string;
	context: FokosDBRouteContext;
	items: TransactionItem[];
};

type PrepareFanout = {
	transactionTs: number;
	participants: PrepareParticipant[];
};

type TcItemRow = {
	transaction_id: string;
	hk: ArrayBuffer;
	sk: ArrayBuffer;
	/** Request-order index of this operation, carried through prepare so nodes merge by index. */
	op_index: number;
	operation: string;
	data: string | ArrayBuffer | null;
	// Persisted so the reconstructed TransactionItem carries the kind through prepare/commit; for json,
	// `data` is the JSON text (the DO re-encodes to JSONB on commit). NULL for delete/check (no data).
	data_kind: number | null;
	ttl_epoch_utc_seconds: number | null;
	conditions_json: string | null;
	update_json: string | null;
	partition_do_name: string;
	/** 1 for "all_old", 0 for "none". Reconstructs TransactionItem during initial prepare and recovery. */
	return_values_on_condition_check_failure: number;
};

type TcResultRow = {
	transaction_id: string;
	op_index: number;
	image_kind: number;
	image_version: number;
	image_ttl_epoch_utc_seconds: number | null;
	/**
	 * Raw image payload (TEXT for text/json, BLOB for bytes).
	 * Stored in individual rows to avoid JSON encoding expansion and protect tc_state row size limits.
	 */
	image_data: string | ArrayBuffer;
};

// Every JSON column here — rejection reasons, participant answers, and the results array — can hold
// binary (Uint8Array) keys, which plain JSON.stringify mangles. These tag and restore them, so a
// cancelled transaction over binary keys still reports the exact key after reload.
function stringifyTagged(value: unknown): string {
	return JSON.stringify(value, (_k, v) => (v instanceof Uint8Array ? { $u8: Array.from(v) } : v));
}

function parseTagged<T>(json: string): T {
	return JSON.parse(json, (_k, v) =>
		v && typeof v === "object" && Array.isArray((v as { $u8?: unknown }).$u8) ? new Uint8Array((v as { $u8: number[] }).$u8) : v,
	) as T;
}

function reasonWithoutImage(reason: RejectionReasonEncoded): RejectionReasonEncoded {
	if (reason.code !== "condition_failed" || reason.item === undefined) {
		return reason;
	}
	const stripped = { ...reason };
	delete stripped.item;
	return stripped;
}

/**
 * The answer as it is stored: every item image removed, every `imageBytes` kept.
 *
 * The images go to tc_results instead, because a `$u8` tag costs about four characters for each byte
 * and one answer can carry MAX_ITEMS_PER_TX of them. `imageBytes` stays so the coordinator can apply
 * the cap at CANCELLING without reading the images back.
 */
function stripImagesFromPrepareResponse(r: PrepareResponse): PrepareResponse {
	if (r.outcome === "accepted") {
		return r;
	}
	return {
		outcome: "rejected",
		results: r.results.map((res) => (res.outcome === "rejected" ? { ...res, reason: reasonWithoutImage(res.reason) } : res)),
	};
}

// tc_items hk/sk are BLOB; materialize a read column as KeyBytes (trusted re-brand, no copy of bytes).
function keyFromBlob(value: ArrayBuffer): KeyBytes {
	return KeyCodec.asKeyBytes(new Uint8Array(value));
}

/** The host job that drives the non-terminal transactions that no request drives. */
const JOB_TX_RECOVERY = "tx_recovery";
/** The host job that deletes the transactions whose idempotency window has passed. */
const JOB_IDEMPOTENCY_SWEEP = "idempotency_sweep";

const NO_SORT_KEY = KeyCodec.encodeOptional(undefined);

/**
 * The route key of a coordinator. The idempotency token selects the coordinator that owns a transaction.
 * A token is at most `MAX_CLIENT_REQUEST_TOKEN_BYTES`, so the default limits always hold it.
 */
function tokenKey(token: string): RouteKey {
	return { hashKey: encodeHashKey(token, DEFAULT_LIMITS), sortKey: NO_SORT_KEY };
}

/**
 * The operations of the coordinator. Both are keyed by the idempotency token, and both handlers
 * await partitions, so each durable transition tests ownership itself (see `transition`).
 */
export type CoordinatorOps = {
	initiateWrite: { req: InitiateWriteRequest; res: InitiateWriteResponseEncoded };
	recoverTransaction: { req: RecoverTransactionRequest; res: RecoverTransactionResult };
};

/** The RPC surface of the class. */
type CoordinatorRpc = FokosShardingRpc & {
	[K in keyof CoordinatorOps]: (
		ctx: FokosDBRouteContext,
		req: CoordinatorOps[K]["req"],
	) => Promise<FokosEnvelope<CoordinatorOps[K]["res"]>>;
};

/** One transaction and its rows in the four tables, as a migration page carries it. */
type MigratedTransaction = {
	state: TcStateRow;
	items: TcItemRow[];
	participants: TcParticipantRow[];
	results: TcResultRow[];
};

const sqlMigrations: SQLSchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: "Create TC state machine tables",
		sql: `
            CREATE TABLE IF NOT EXISTS tc_state (
                transaction_id          TEXT    NOT NULL PRIMARY KEY,
                idempotency_token       TEXT    NOT NULL,
                state                   TEXT    NOT NULL,
                transaction_ts          INTEGER NOT NULL,
                created_at              INTEGER NOT NULL,
                completed_at            INTEGER,
                -- The earliest time at which the tx_recovery job drives this transaction. NULL once the
                -- transaction is complete. Each attempt moves it forward, so a transaction that stays
                -- goes behind the others and does not use each step of the job.
                next_recovery_at        INTEGER,
                -- Positional TransactWriteOperationResultEncoded array. Item images are omitted
                -- and stored in tc_results to prevent exceeding the 2 MB SQLite row limit.
                results_json            TEXT,
                -- Fingerprint of the operation set this token was first used for. A replay whose
                -- operations hash differently is a different request wearing the same token, and is
                -- rejected instead of being answered with this transaction's outcome.
                -- TEXT because DO SQL cannot bind a JS bigint.
                operations_hash         TEXT    NOT NULL
			) WITHOUT ROWID, STRICT;

			CREATE UNIQUE INDEX IF NOT EXISTS tc_state_idempotency_token ON tc_state (idempotency_token);
			CREATE INDEX IF NOT EXISTS idx_tc_state_completed_at ON tc_state (completed_at) WHERE completed_at IS NOT NULL;
			CREATE INDEX IF NOT EXISTS idx_tc_state_recovery ON tc_state (next_recovery_at) WHERE completed_at IS NULL;

            CREATE TABLE IF NOT EXISTS tc_participants (
                transaction_id          TEXT    NOT NULL,
                partition_do_name       TEXT    NOT NULL,
                partition_context_json  TEXT    NOT NULL DEFAULT '',
                prepare_outcome         TEXT,
                commit_outcome          TEXT,
                cancel_outcome          TEXT,
                -- Serialized PrepareResponse with item images stripped (imageBytes kept for capping).
                answer_json             TEXT,
                -- The FokosErrorWire of the error of the last prepare attempt that threw. It is written only
                -- while prepare_outcome is NULL, so a later answer replaces it.
                error_json              TEXT,
                PRIMARY KEY (transaction_id, partition_do_name)
            ) WITHOUT ROWID, STRICT;

            CREATE TABLE IF NOT EXISTS tc_items (
                transaction_id      TEXT    NOT NULL,
                hk                  BLOB    NOT NULL,
                sk                  BLOB    NOT NULL DEFAULT x'',
                -- Request-order index of this operation for positional result merging.
                op_index            INTEGER NOT NULL,
                operation           TEXT    NOT NULL,
                data                ANY,
                data_kind           INTEGER,
                ttl_epoch_utc_seconds INTEGER,
                conditions_json     TEXT,
                update_json         TEXT,
                partition_do_name   TEXT    NOT NULL,
                -- 1 for "all_old", 0 for "none". Reconstructs TransactionItem during prepare recovery.
                return_values_on_condition_check_failure INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (transaction_id, hk, sk)
            ) WITHOUT ROWID, STRICT;

            -- Stores raw item images for rejected operations requesting all_old.
            -- Stored as separate rows to avoid JSON encoding expansion and prevent tc_state row size blowup.
            CREATE TABLE IF NOT EXISTS tc_results (
                transaction_id  TEXT    NOT NULL,
                op_index        INTEGER NOT NULL,
                image_kind      INTEGER NOT NULL,
                image_version   INTEGER NOT NULL,
                image_ttl_epoch_utc_seconds INTEGER,
                image_data      ANY     NOT NULL,
                PRIMARY KEY (transaction_id, op_index)
            ) WITHOUT ROWID, STRICT;
        `,
	},
];

export class TransactionCoordinatorDO extends DurableObject<Env> implements CoordinatorRpc {
	/** The sharding runtime: identity, routing, splits, and the alarm. The pool grows by hash splits. */
	readonly fokos: FokosShardingRuntime<FokosDBPolicy, CoordinatorOps>;
	#migrations: SQLSchemaMigrations;
	/** True after the split floor was logged. Every refused transaction runs the decision, so it logs once. */
	#splitFloorLogged = false;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.#migrations = new SQLSchemaMigrations({
			migrations: sqlMigrations,
			doStorage: ctx.storage,
			keyNameTrackingLastMigrationID: "__fokosdb/tc/sql_schema_version",
		});
		// The runtime runs the sharding migrations in its own blockConcurrencyWhile, before the host's.
		this.fokos = new FokosShardingRuntime<FokosDBPolicy, CoordinatorOps>({
			ctx,
			stub: (routeCtx, doName) => txCoordinatorStubByName(env, routeCtx, doName),
			hooks: this.hooks(),
			operations: this.operations(),
			config: () => this.fokosRuntimeConfig(),
		});
		void ctx.blockConcurrencyWhile(async () => {
			this.#migrations.runAllSync();
		});
	}

	// ═══ the RPC surface: one dispatch per method ════════════════════════════

	initiateWrite(ctx: FokosDBRouteContext, req: InitiateWriteRequest): Promise<FokosEnvelope<InitiateWriteResponseEncoded>> {
		return this.fokos.dispatch("initiateWrite", ctx, req);
	}

	/** The routed operation. A coordinator that has split forwards the call to the child that owns the token. */
	recoverTransaction(ctx: FokosDBRouteContext, req: RecoverTransactionRequest): Promise<FokosEnvelope<RecoverTransactionResult>> {
		return this.fokos.dispatch("recoverTransaction", ctx, req);
	}

	/**
	 * Called by a partition whose lock is stale. The lock stores only the name of this coordinator and
	 * the token, so this coordinator routes the call with its own stored route context. A coordinator
	 * with no identity has no ledger, so it has no record of the transaction.
	 */
	async recoverTransactionForParticipant(req: RecoverTransactionRequest): Promise<RecoverTransactionResult> {
		if (!this.fokos.initialized()) {
			return { state: "not_found" };
		}
		return (await this.recoverTransaction(this.fokos.routeContext(), req)).value;
	}

	fokosInit(req: FokosInitRequest): Promise<void> {
		return this.fokos.fokosInit(req);
	}
	fokosStartImport(req: FokosStartImportRequest): Promise<void> {
		return this.fokos.fokosStartImport(req);
	}
	fokosMigrationPull(req: FokosMigrationPullRequest): Promise<FokosMigrationPage> {
		return this.fokos.fokosMigrationPull(req);
	}
	fokosMigrationAck(req: FokosMigrationAckRequest): Promise<void> {
		return this.fokos.fokosMigrationAck(req);
	}
	fokosExecuteLocal(req: FokosExecuteLocalRequest): Promise<FokosEnvelope<unknown>> {
		return this.fokos.fokosExecuteLocal(req);
	}
	fokosRequestPromotion(req: FokosRequestPromotionRequest) {
		return this.fokos.fokosRequestPromotion(req);
	}
	fokosStatus(req: FokosStatusRequest) {
		return this.fokos.fokosStatus(req);
	}
	fokosPrepareDestroy(req: FokosPrepareDestroyRequest): Promise<void> {
		return this.fokos.fokosPrepareDestroy(req);
	}
	/**
	 * Wipes this coordinator. `FokosDB.destroy()` calls it for every coordinator of the table. The
	 * idempotency window lives in `tc_state`, so a coordinator that survives a destroy answers a replayed
	 * `clientRequestToken` with the OLD transaction's outcome — "committed" for data that no longer
	 * exists. That is why destroy must reach the coordinators and not the partitions alone.
	 */
	fokosDestroy(): Promise<void> {
		return this.fokos.fokosDestroy();
	}

	/** The runtime owns the alarm. The host work runs as the jobs `tx_recovery` and `idempotency_sweep`. */
	async alarm(info: AlarmInvocationInfo): Promise<void> {
		await this.fokos.alarm(info);
	}

	// ═══ operations ══════════════════════════════════════════════════════════

	private operations(): FokosOperations<CoordinatorOps> {
		return {
			initiateWrite: {
				shape: "point",
				whileMigrating: "throw",
				localMode: "async",
				admissionTag: "write",
				key: (req) => tokenKey(req.clientRequestToken),
				local: async (req) => await this.initiateWriteLocal(req),
			},
			recoverTransaction: {
				shape: "point",
				whileMigrating: "throw",
				localMode: "async",
				key: (req) => tokenKey(req.idempotencyToken),
				local: async (req) => await this.recoverTransactionLocal(req.transactionId),
			},
		};
	}

	// ═══ hooks ═══════════════════════════════════════════════════════════════

	private hooks(): FokosShardingHooks<FokosDBPolicy> {
		const sql = this.ctx.storage.sql;
		// The split threshold. A missing or zero `maxSizeMb` gives only the size limit of the coordinator.
		// The limit is divided by the admission margin, so the admission below stops at `maxDatabaseBytes`.
		const maxBytes = (policy: FokosDBPolicy) =>
			Math.min((policy.hashSplitConditions.maxSizeMb || Infinity) * 1024 * 1024, this.config().maxDatabaseBytes / ADMISSION_MARGIN);
		return {
			// The coordinator splits above the hash split threshold of its table. It also splits above
			// its own size limit, when that limit is smaller. A hash split moves whole tokens, so it
			// cannot make a coordinator with one token or none smaller: that is the floor.
			evaluateSplit: ({ identity, policy }) => {
				const databaseSize = sql.databaseSize;
				const maxSizeBytes = maxBytes(policy);
				if (databaseSize <= maxSizeBytes) {
					this.#splitFloorLogged = false;
					return false;
				}
				if (this.hasAtLeastTwoTransactions()) {
					this.#splitFloorLogged = false;
					return {};
				}
				if (!this.#splitFloorLogged) {
					this.#splitFloorLogged = true;
					console.error({
						message: "fokos/tc: over the split threshold, but a split cannot make this coordinator smaller",
						...identity.ref,
						reason: "fewer than two idempotency tokens",
						databaseSize,
						maxSizeBytes,
					});
				}
				return false;
			},

			// A coordinator accepts up to 10% above its split threshold, so the requests that trigger the
			// split complete. Above that it refuses a NEW transaction only: a replay reads the ledger and
			// writes nothing, so it still gets its answer. The ledger read runs only above the threshold.
			admit: ({ admissionTag, keys, policy }) => {
				if (admissionTag !== "write" || sql.databaseSize <= maxBytes(policy) * ADMISSION_MARGIN) {
					return "allow";
				}
				const token = KeyCodec.decode(keys[0].hashKey) as string;
				if (this.hasStateRowForToken(token)) {
					return "allow";
				}
				return {
					reject: new FokosUnavailableError(UNAVAILABLE_CODES.coordinator_over_size, {
						message: "transaction coordinator exceeded its storage limit, please retry later",
					}),
				};
			},
			migration: {
				buildPage: (cursor, _slice, belongsToTarget, budget) => this.buildMigrationPage(cursor as string | null, belongsToTarget, budget),
				applyPage: (page) => this.applyMigrationPage(page as MigratedTransaction[]),
				validatePage: (_cursor, page) => {
					invariant(Array.isArray(page), "fokos/tc: a migration page must be an array of transactions");
				},
			},
			// Every child has acknowledged its import, so the rows of this router are old copies.
			cleanupSourceStep: () => this.deleteMigratedRowsStep(),
			jobs: [
				{
					name: JOB_TX_RECOVERY,
					canRun: (lifecycle) => this.canDriveLocally(lifecycle),
					// The step moves `next_recovery_at` of each transaction that it takes forward, so the
					// deadline does not stay in the past while a participant is down.
					deadline: () => this.earliestRecoveryAt(),
					runStep: async () => {
						await this.recoverStaleTransactions();
						return { nextRunAt: null };
					},
				},
				{
					name: JOB_IDEMPOTENCY_SWEEP,
					canRun: (lifecycle) => this.canDriveLocally(lifecycle),
					deadline: () => {
						const earliest = this.earliestCompletedAt();
						return earliest === null ? null : sweepDueAt(earliest);
					},
					runStep: () => ({ nextRunAt: this.sweepExpiredTransactions() }),
				},
			],
		};
	}

	/**
	 * True when this coordinator drives its own transactions. A router owns no token, a target that
	 * still imports holds an incomplete ledger, and a fenced coordinator makes no transition.
	 */
	private canDriveLocally(lifecycle: FokosLifecycle): boolean {
		if (!this.fokos.initialized()) {
			return false;
		}
		if (lifecycle.destroying || lifecycle.role === "router") {
			return false;
		}
		return lifecycle.import === null || lifecycle.import.state === "active" || lifecycle.import.state === "imported";
	}

	/**
	 * One durable transition of the state machine. The ownership test runs in the same synchronous
	 * block as the write, so a split cannot cut over between them. After a cutover the target owns the
	 * token, and it pulls the rows of this coordinator after the cutover, so it receives the last state
	 * this coordinator wrote. A transition here would be lost, so it writes nothing and throws
	 * `partition_migrating`. The client retries with the same token, and the target resumes the
	 * transaction from its copy of the row.
	 */
	private transition<T>(idempotencyToken: string, write: () => T): T {
		return this.ctx.storage.transactionSync(() => {
			if (!this.fokos.owns(tokenKey(idempotencyToken))) {
				throw new FokosUnavailableError(SHARDING_UNAVAILABLE_CODES.partition_migrating, {
					message: "the transaction coordinator split, retry with the same clientRequestToken",
					attributes: { idempotencyToken },
				});
			}
			return write();
		});
	}

	//////////////////////////////
	// User overridable methods.
	//////////////////////////////

	/**
	 * Override to change the settings of the sharding runtime of this class. The runtime merges the
	 * result with `DEFAULT_RUNTIME_CONFIG` and validates it. It calls this method in its own
	 * constructor, before `this.fokos` is assigned, and again at each use of a setting. An override that
	 * reads the identity or the policy of the coordinator must first check `this.fokos?.initialized()`,
	 * and return a fallback value when it is false.
	 */
	protected fokosRuntimeConfig(): FokosRuntimeConfigOverrides {
		return {};
	}

	/**
	 * Override to change the settings of this class. The class merges the result with
	 * `DEFAULT_COORDINATOR_CONFIG`, validates it, and reads it at each use. An override that reads the
	 * identity or the policy of the coordinator must first check `this.fokos?.initialized()`, and return
	 * a fallback value when it is false.
	 */
	protected fokosConfig(): TransactionCoordinatorDOConfigOverrides {
		return {};
	}

	/**
	 * The clock of this coordinator, in milliseconds. Read at each use, so a test can replace it on one
	 * instance and avoid global mocks.
	 */
	fokosNow(): number {
		return Date.now();
	}

	/** The active settings: the overrides of `fokosConfig()` over the defaults, validated. */
	private config(): TransactionCoordinatorDOConfig {
		return resolveCoordinatorConfig(this.fokosConfig());
	}

	//////////////////////////////
	// Transaction methods.
	//////////////////////////////

	/** The local handler of `initiateWrite`. The runtime has already resolved this coordinator as the owner of the token. */
	private async initiateWriteLocal(request: InitiateWriteRequest): Promise<InitiateWriteResponseEncoded> {
		const transactionId = crypto.randomUUID().replaceAll("-", "");
		const idempotencyToken = request.clientRequestToken;

		// Computed once and used twice: to validate a replay, and as the stored fingerprint below.
		const operationsHash = hashTransactionOperations(request.items);

		const existingRow = this.loadStateRowByToken(idempotencyToken);
		if (existingRow) {
			if (existingRow.operations_hash !== operationsHash) {
				// Answering with the stored outcome here would report "committed" for operations that
				// were never executed, so this must fail loudly. DynamoDB calls it
				// IdempotentParameterMismatch.
				throw new FokosValidationError(VALIDATION_CODES.idempotent_parameter_mismatch, {
					message: "transactWriteItems clientRequestToken was already used for a different set of operations",
					attributes: { clientRequestToken: idempotencyToken },
				});
			}
			return await this.resumeTransaction(existingRow, idempotencyToken);
		}

		// Key/operation validation is the client's single boundary (FokosDB.transactWriteItems); the TC
		// receives already-validated, already-encoded operations.

		// The low three decimal digits stay zero; a later change can allocate them to tie-breaking.
		const transactionTs = txOrderTimestampNow();

		// Group the operations by the root partition of each key. The same grouping feeds the tc_items and
		// tc_participants rows below and the prepare fan-out, so the happy path never reads the rows it has
		// just written back from SQLite. The client validates the topology and the range config of the
		// table before anything is written.
		const partitions = this.partitionClient(request.table);
		const participants: PrepareParticipant[] = partitions.resolveAll(request.items).map(({ ctx, indexes }) => ({
			doName: ctx.doName,
			context: ctx,
			items: indexes.map((i) => request.items[i]),
		}));
		const participantOf: string[] = [];
		for (const p of participants) {
			for (const item of p.items) {
				participantOf[item.opIndex] = p.doName;
			}
		}

		const createdAt = this.fokosNow();
		this.transition(idempotencyToken, () => {
			this.ctx.storage.sql.exec(
				`INSERT INTO tc_state (transaction_id, idempotency_token, state, transaction_ts, created_at, next_recovery_at, operations_hash)
                 VALUES (?, ?, 'CREATED', ?, ?, ?, ?)`,
				transactionId,
				idempotencyToken,
				transactionTs,
				createdAt,
				createdAt + this.config().staleTransactionMs,
				operationsHash,
			);
			for (const op of request.items) {
				this.ctx.storage.sql.exec(
					`INSERT INTO tc_items (transaction_id, hk, sk, op_index, operation, data, data_kind, ttl_epoch_utc_seconds, conditions_json, update_json, partition_do_name, return_values_on_condition_check_failure)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					transactionId,
					op.hashKey,
					op.sortKey,
					op.opIndex,
					op.operation,
					op.data ?? null,
					// data and kind travel together: put carries both; delete/check/update carry neither (NULL kind).
					op.kind === undefined ? null : DATA_KINDS.indexOf(op.kind),
					op.ttlAt ?? null,
					op.condition ? JSON.stringify(op.condition) : null,
					op.update ? JSON.stringify(op.update) : null,
					participantOf[op.opIndex],
					op.returnValuesOnConditionCheckFailure === "all_old" ? 1 : 0,
				);
			}
			for (const p of participants) {
				this.ctx.storage.sql.exec(
					`INSERT INTO tc_participants (transaction_id, partition_do_name, partition_context_json) VALUES (?, ?, ?)`,
					transactionId,
					p.doName,
					JSON.stringify(p.context),
				);
			}
		});

		// Durable before the first prepare, so a coordinator that stops after this point still resumes the
		// transaction. The handler can end with a thrown answer, which drops the signals of a local call,
		// so these calls do not go through `call.signal`.
		await this.fokos.scheduleJob(JOB_TX_RECOVERY, this.fokosNow() + this.config().staleTransactionMs);
		this.fokos.requestSplitEvaluation();

		return await this.drivePrepare(transactionId, idempotencyToken, this.config().fanoutRequestBudgetMs, {
			transactionTs,
			participants,
		});
	}

	private async resumeTransaction(existingRow: TcStateRow, idempotencyToken: string): Promise<InitiateWriteResponseEncoded> {
		const { transaction_id: transactionId } = existingRow;
		switch (existingRow.state) {
			case "COMMITTED":
				return this.loadFinalResponse(transactionId, idempotencyToken, existingRow);
			case "CANCELLED":
				return this.loadFinalResponse(transactionId, idempotencyToken, existingRow);
			case "PREPARING": {
				await this.runPrepareRecovery(transactionId, idempotencyToken, this.config().fanoutRequestBudgetMs);
				return this.loadFinalResponse(transactionId, idempotencyToken);
			}
			case "PREPARED":
			case "COMMITTING": {
				await this.runCommit(transactionId, idempotencyToken, this.config().fanoutRequestBudgetMs);
				return this.loadFinalResponse(transactionId, idempotencyToken);
			}
			case "CANCELLING": {
				await this.runCancel(transactionId, idempotencyToken, this.config().fanoutRequestBudgetMs);
				return this.loadFinalResponse(transactionId, idempotencyToken);
			}
			case "CREATED":
				return await this.drivePrepare(transactionId, idempotencyToken, this.config().fanoutRequestBudgetMs);
		}
	}

	/**
	 * Maps the state machine to the client's answer.
	 *
	 * - COMMITTED: every participant confirmed the commit, so "committed" also promises
	 *   read-your-writes — a caller that receives it can read what it wrote on every participant.
	 * - PREPARED / COMMITTING: the decision is durable and PREPARED is the point of no return
	 *   (nothing transitions PREPARED → CANCELLING; both writers of CANCELLING guard on state =
	 *   'PREPARING'), so the transaction WILL commit — but some participant has not applied it yet.
	 *   Answering "committed" would let a caller read a stale value from that participant, so these
	 *   states throw the commit-pending error instead. The `tx_recovery` job finishes the fan-out, and a retry
	 *   with the same token answers "committed" once the last participant confirms.
	 * - CANCELLING / CANCELLED: a cancelled transaction applied nothing anywhere, so outstanding
	 *   cleanup cannot change what the caller observes, and the answer follows the decision. Only
	 *   the lock release lags, and the `tx_recovery` job drives that too.
	 * - CREATED / PREPARING: genuinely undecided — those, and only those, ask the caller to retry.
	 */
	private loadFinalResponse(transactionId: string, idempotencyToken: string, existingRow?: TcStateRow): InitiateWriteResponseEncoded {
		const row = existingRow ?? this.loadStateRow(transactionId)!;
		switch (row.state) {
			case "COMMITTED":
				// The transaction is all-or-nothing, so "committed" already says every operation applied.
				// Nothing per-item to report, and so no tc_items read on this path.
				return { outcome: "committed", transactionId, idempotencyToken };
			case "PREPARED":
			case "COMMITTING":
				// The outcome is decided and final, but not every participant has applied it yet, so
				// this is not a terminal answer for the caller: retry with the same token.
				throw new FokosTransactionPendingError(TRANSACTION_PENDING_CODES.transaction_commit_pending, {
					message: `transaction commit is pending: the decision is durable and the transaction will commit, but not every participant has applied it yet — retry with the same clientRequestToken`,
					attributes: { transactionId, state: row.state },
				});
			case "CANCELLING":
			case "CANCELLED": {
				// CANCELLING writes results_json in the same statement as the state, so a row without it is torn
				// and holds no answer to give.
				if (!row.results_json) {
					throw unexpectedTransactionStateError("a cancelled transaction has no stored results", { transactionId, state: row.state });
				}
				const results = parseTagged<TransactWriteOperationResultEncoded[]>(row.results_json);
				// results_json is positional to the request and tc_results.op_index is that same request
				// index, so entry i owns the image row with op_index i. The cap already applied before
				// the array was stored, so a replay answers with exactly the images the first call did.
				for (const img of this.loadResultImages(transactionId)) {
					const res = results[img.op_index];
					if (res?.outcome !== "rejected" || res.reason.code !== "condition_failed") {
						continue;
					}
					res.reason.item = {
						hashKey: res.reason.hashKey,
						...(res.reason.sortKey !== undefined ? { sortKey: res.reason.sortKey } : {}),
						data: img.image_data instanceof ArrayBuffer ? new Uint8Array(img.image_data) : img.image_data,
						kind: DATA_KINDS[img.image_kind],
						version: img.image_version,
						...(img.image_ttl_epoch_utc_seconds != null ? { ttlAt: img.image_ttl_epoch_utc_seconds } : {}),
					};
				}
				return { outcome: "cancelled", transactionId, idempotencyToken, results };
			}
			case "CREATED":
			case "PREPARING":
				// No decision yet — the `tx_recovery` job drives it. The outcome can still go either way, so
				// this answer promises nothing and only asks the caller to retry.
				throw new FokosTransactionPendingError(TRANSACTION_PENDING_CODES.transaction_undecided, {
					message: "transaction outcome is not yet decided, retry later",
					attributes: { transactionId, state: row.state },
				});
			default: {
				const _exhaustive: never = row.state;
				throw new FokosInternalError(INTERNAL_CODES.unexpected_transaction_state, {
					message: "unexpected transaction state",
					attributes: { transactionId, state: _exhaustive },
				});
			}
		}
	}

	/**
	 * Removes the payload that no step needs after PREPARED or CANCELLING. The request path does not
	 * call it: a transaction that completes deletes its rows a few milliseconds later, and a strip
	 * before that only adds one write for each item. The `tx_recovery` claim calls it, so a transaction
	 * that does not complete keeps its payload only until its first claim. It writes no row when the
	 * payload is already removed.
	 */
	private stripPayload(transactionId: string): void {
		this.ctx.storage.sql.exec(
			`UPDATE tc_items SET data = NULL, data_kind = NULL, conditions_json = NULL, update_json = NULL
			  WHERE transaction_id = ? AND (data IS NOT NULL OR data_kind IS NOT NULL OR conditions_json IS NOT NULL OR update_json IS NOT NULL)`,
			transactionId,
		);
	}

	/**
	 * Moves the transaction to its terminal state, and schedules the `idempotency_sweep` job for the
	 * end of its idempotency window. It does nothing when the transaction already left its
	 * COMMITTING or CANCELLING state.
	 */
	private async completeTransaction(
		transactionId: string,
		idempotencyToken: string,
		terminalState: Extract<TCState, "COMMITTED" | "CANCELLED">,
	): Promise<void> {
		const expectedState = terminalState === "COMMITTED" ? "COMMITTING" : "CANCELLING";
		const completedAt = this.fokosNow();
		let transitioned = false;
		this.transition(idempotencyToken, () => {
			const transition = this.ctx.storage.sql.exec(
				`UPDATE tc_state SET state = ?, completed_at = ?, next_recovery_at = NULL WHERE transaction_id = ? AND state = ?`,
				terminalState,
				completedAt,
				transactionId,
				expectedState,
			);
			if (transition.rowsWritten === 0) {
				return;
			}
			this.ctx.storage.sql.exec(`DELETE FROM tc_items WHERE transaction_id = ?`, transactionId);
			this.ctx.storage.sql.exec(`DELETE FROM tc_participants WHERE transaction_id = ?`, transactionId);
			transitioned = true;
		});
		if (transitioned) {
			await this.fokos.scheduleJob(JOB_IDEMPOTENCY_SWEEP, sweepDueAt(completedAt));
		}
	}

	/**
	 * Records one participant's prepare answer, and its images, in the same storage transaction as the
	 * prepare outcome they belong to. No part of an answer then lives only in memory, so a coordinator
	 * evicted between a participant's answer and the transaction's decision still reads back every
	 * outcome, reason, and image that participant reported.
	 *
	 * It writes only while the transaction is PREPARING. An answer can arrive after another drive has
	 * decided the transaction. That decision has stored its results and deleted the images that the cap
	 * dropped, so a late answer must not change the answers or add an image again.
	 *
	 * The first answer of a participant stays. Each later answer is ignored:
	 * - After an accepted answer, the participant holds its locks until the decision, and it answers each
	 *   later prepare of the transaction with accepted.
	 * - After a rejection, the transaction can only be cancelled, and the cancel releases a lock that a
	 *   later prepare took.
	 *
	 * So the stored images of a transaction only grow until the decision. The combined image cap keeps
	 * the images in opIndex order up to the cap, so an image that is above the cap now stays above it in
	 * the merge of `cancelTransactionInStore`. This method deletes those images, and does not write them,
	 * so a transaction never stores more than MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX of images. Without
	 * this bound, one PREPARING transaction can be larger than the RPC limit of a migration page.
	 */
	private storePrepareAnswer(transactionId: string, partitionDoName: string, answer: PrepareResponse): void {
		this.ctx.storage.transactionSync(() => {
			if (this.loadStateRow(transactionId)?.state !== "PREPARING") {
				return;
			}
			const stored = this.ctx.storage.sql.exec(
				`UPDATE tc_participants SET prepare_outcome = ?, answer_json = ?
				  WHERE transaction_id = ? AND partition_do_name = ? AND prepare_outcome IS NULL`,
				answer.outcome,
				answer.outcome === "rejected" ? stringifyTagged(stripImagesFromPrepareResponse(answer)) : null,
				transactionId,
				partitionDoName,
			);
			if (stored.rowsWritten === 0 || answer.outcome !== "rejected") {
				return;
			}
			// An answer with no image adds no bytes, so the cap drops no new image.
			if (!answer.results.some((res) => res.outcome === "rejected" && res.imageBytes !== undefined)) {
				return;
			}
			const aboveCap = this.imagesAboveCap(transactionId);
			for (const opIndex of aboveCap) {
				this.ctx.storage.sql.exec(`DELETE FROM tc_results WHERE transaction_id = ? AND op_index = ?`, transactionId, opIndex);
			}
			for (const res of answer.results) {
				if (res.outcome !== "rejected" || res.reason.code !== "condition_failed" || !res.reason.item || aboveCap.has(res.opIndex)) {
					continue;
				}
				const img = res.reason.item;
				// image_data is an ANY column: text and JSON text bind as TEXT, bytes bind as a BLOB, exactly
				// as the partition returned them. Nothing JSON-encodes an image anywhere on this path.
				this.ctx.storage.sql.exec(
					`INSERT OR REPLACE INTO tc_results (transaction_id, op_index, image_kind, image_version, image_ttl_epoch_utc_seconds, image_data)
					 VALUES (?, ?, ?, ?, ?, ?)`,
					transactionId,
					res.opIndex,
					DATA_KINDS.indexOf(img.kind),
					img.version,
					img.ttlAt ?? null,
					img.data,
				);
			}
		});
	}

	/** The operations whose image the combined cap drops, from the prepare answers that are stored now. */
	private imagesAboveCap(transactionId: string): Set<number> {
		const results: ParticipantOperationResultEncoded[] = [];
		for (const { answer_json } of this.ctx.storage.sql.exec<{ answer_json: string }>(
			`SELECT answer_json FROM tc_participants WHERE transaction_id = ? AND answer_json IS NOT NULL`,
			transactionId,
		)) {
			const answer = parseTagged<PrepareResponse>(answer_json);
			if (answer.outcome === "rejected") {
				results.push(...answer.results);
			}
		}
		applyImageCap(results);
		return new Set(results.filter((r) => r.outcome === "rejected" && r.itemOmitted === "response_too_large").map((r) => r.opIndex));
	}

	/**
	 * Records why a participant's prepare threw after its retries, so the cancel reports the cause. A
	 * participant that has answered keeps its answer, and recovery can still re-prepare one that has not.
	 * It writes only while the transaction is PREPARING: after the decision, the stored results already
	 * hold the cause.
	 */
	private storePrepareError(transactionId: string, partitionDoName: string, err: unknown): void {
		dropCallCost(err);
		this.ctx.storage.sql.exec(
			`UPDATE tc_participants SET error_json = ?
			  WHERE transaction_id = ? AND partition_do_name = ? AND prepare_outcome IS NULL
			    AND EXISTS (SELECT 1 FROM tc_state WHERE transaction_id = ? AND state = 'PREPARING')`,
			stringifyTagged(FokosError.toWire(err)),
			transactionId,
			partitionDoName,
			transactionId,
		);
	}

	/**
	 * Merges every participant's stored answer into the transaction's outcome and records the durable
	 * transition to CANCELLING.
	 *
	 * Both drivePrepare and runPrepareRecovery call it, so the two writers of CANCELLING cannot merge
	 * differently: the answers come from storage, never from what the caller still holds in memory.
	 */
	private cancelTransactionInStore(transactionId: string, idempotencyToken: string): void {
		this.transition(idempotencyToken, () => {
			const items = this.loadItems(transactionId);
			const itemsByPartition = groupByPartition(items);

			// results_json is positional to the request: entry i answers the operation the caller sent at
			// index i. db.ts assigns op_index from the request array, so a transaction of n operations
			// fills 0..n-1, and loadFinalResponse joins a stored image to its entry by that same index.
			// An operation no participant reported stays not_evaluated rather than shifting its neighbours.
			const merged: ParticipantOperationResultEncoded[] = items.map((_item, i) => ({ outcome: "not_evaluated", opIndex: i }));
			for (const p of this.loadParticipants(transactionId)) {
				const owned = itemsByPartition.get(p.partition_do_name) ?? [];
				const answer = p.prepare_outcome === "rejected" && p.answer_json ? parseTagged<PrepareResponse>(p.answer_json) : null;
				let answered: ParticipantOperationResultEncoded[];
				if (p.prepare_outcome === "accepted") {
					// An accepted participant sends no array: the lock it holds is the proof that its check
					// pass accepted every operation it owns.
					answered = owned.map((item) => ({ outcome: "passed", opIndex: item.op_index }));
				} else if (answer?.outcome === "rejected") {
					answered = answer.results;
				} else {
					// The participant could not run its operations, so each of them reports why.
					const failure = this.participantFailure(p);
					answered = owned.map((item) => ({
						outcome: "rejected",
						opIndex: item.op_index,
						reason: {
							code: failure.code as ExecutionFailureCode,
							...decodeItemKeys(keyFromBlob(item.hk), keyFromBlob(item.sk)),
							error_id: failure.error_id,
						},
					}));
				}
				for (const r of answered) {
					if (r.opIndex >= 0 && r.opIndex < merged.length) {
						merged[r.opIndex] = r;
					}
				}
			}

			// Two participants can each answer under the cap and together exceed it, so the coordinator
			// caps the whole array once more before it stores one.
			applyImageCap(merged);
			// The operations whose image the cap dropped, so their tc_results rows go once the write wins.
			const cappedOutOpIndexes = merged
				.filter((r) => r.outcome === "rejected" && r.itemOmitted === "response_too_large")
				.map((r) => r.opIndex);

			// results_json holds outcome codes, reasons (keys only), and itemOmitted. opIndex is implied
			// by the position, imageBytes was only ever for the cap, and the images live in tc_results.
			const finalResults: TransactWriteOperationResultEncoded[] = merged.map((r) =>
				r.outcome === "rejected"
					? {
							outcome: "rejected" as const,
							reason: reasonWithoutImage(r.reason),
							...(r.itemOmitted ? { itemOmitted: r.itemOmitted } : {}),
						}
					: { outcome: r.outcome },
			);

			const transition = this.ctx.storage.sql.exec(
				`UPDATE tc_state SET state = 'CANCELLING', results_json = ? WHERE transaction_id = ? AND state = 'PREPARING'`,
				stringifyTagged(finalResults),
				transactionId,
			);
			// Another writer already decided this transaction. Its results_json names the images that are
			// on disk, so deleting any of them here would strand its answer without one.
			if (transition.rowsWritten === 0) {
				return;
			}

			for (const opIndex of cappedOutOpIndexes) {
				this.ctx.storage.sql.exec(`DELETE FROM tc_results WHERE transaction_id = ? AND op_index = ?`, transactionId, opIndex);
			}
		});
	}

	/**
	 * Why a participant could not run its operations, read only once the transaction is cancelling, when
	 * nothing re-prepares it. A NULL row is the prepare that threw after its retries, and error_json holds
	 * why; with no error, the coordinator stopped between the throw and the write. A rejected row whose
	 * answer cannot be read back reports unexpected_transaction_state.
	 */
	private participantFailure(p: TcParticipantRow): FokosErrorWire {
		if (p.prepare_outcome === "rejected") {
			return FokosError.toWire(unexpectedTransactionStateError("a rejected prepare answer cannot be read back"));
		}
		if (p.error_json) {
			return parseTagged<FokosErrorWire>(p.error_json);
		}
		return FokosError.toWire(
			new FokosUnavailableError(UNAVAILABLE_CODES.prepare_unanswered, {
				message: "a participant did not answer the prepare",
				attributes: { partitionDoName: p.partition_do_name },
			}),
		);
	}

	/** The client of the partitions of the table that `ctx` names. The client is cheap to make. */
	private partitionClient(ctx: FokosDBTableConfig): FokosShardingClient<FokosDBPolicy, PartitionOps> {
		// TODO: Consider caching the client for repeated use to avoid creating a new instance each time.
		// TODO: The client should exploit cache of topology hierarchy to leapfrog partitions.
		return new FokosShardingClient<FokosDBPolicy, PartitionOps>({
			topology: ctx.topology,
			rangeConfig: ctx.rangeConfig,
			policy: ctx.policy,
			stub: (c, doName) => partitionStubByName(this.env, c, doName),
		});
	}

	/**
	 * The retry rule of a commit or a cancel fan-out. It retries until the deadline, on the wall clock and
	 * on the clock of this coordinator, and a participant past the deadline stays unconfirmed for the
	 * `tx_recovery` job.
	 */
	private fanoutRetry(deadlineMs: number): FokosRetryPolicy {
		const { baseDelayMs, maxDelayMs } = this.config().participantRetry;
		return { shouldRetry: () => this.fokosNow() <= deadlineMs, baseDelayMs, maxDelayMs };
	}

	/** The reference that each participant stores in its lock, and calls back on recovery. */
	private coordinatorRef(idempotencyToken: string): CoordinatorRef {
		return { v: COORDINATOR_REF_VERSION, doName: this.fokos.routeContext().doName, idempotencyToken };
	}

	/**
	 * Moves PREPARING to PREPARED, the point of no return. The stored answers decide, not the answers
	 * that the caller holds in memory: every participant must have a stored accepted answer.
	 */
	private markPrepared(transactionId: string, idempotencyToken: string): void {
		this.transition(idempotencyToken, () =>
			this.ctx.storage.sql.exec(
				`UPDATE tc_state SET state = 'PREPARED' WHERE transaction_id = ? AND state = 'PREPARING'
				   AND NOT EXISTS (SELECT 1 FROM tc_participants WHERE transaction_id = ? AND prepare_outcome IS NOT 'accepted')`,
				transactionId,
				transactionId,
			),
		);
	}

	/**
	 * `fanout` is the in-memory form of the rows `initiateWrite` has just written. It is passed only
	 * on that path, where the rows are already durable and identical to it, so the prepare does not
	 * read them back and parse every plan and context again. Every other caller (a resumed CREATED
	 * transaction, the `tx_recovery` job, recovery) has no in-memory copy and loads the fan-out from SQLite.
	 */
	private async drivePrepare(
		transactionId: string,
		idempotencyToken: string,
		requestBudgetMs: number,
		fanout?: PrepareFanout,
	): Promise<InitiateWriteResponseEncoded> {
		const row = this.transition(idempotencyToken, () => {
			this.ctx.storage.sql.exec(`UPDATE tc_state SET state = 'PREPARING' WHERE transaction_id = ? AND state = 'CREATED'`, transactionId);
			return this.loadStateRow(transactionId);
		});
		// Another drive can start between the insert of the transaction and this call, for example a
		// retry with the same token. When that drive has already decided, a prepare from this drive can
		// lock keys after the cancel released them.
		if (row?.state !== "PREPARING") {
			return this.loadFinalResponse(transactionId, idempotencyToken, row);
		}

		const { transactionTs, participants } = fanout ?? this.loadPrepareFanout(transactionId);
		// Each participant stores this reference in its lock, and calls it back on recovery.
		const coordinator = this.coordinatorRef(idempotencyToken);
		const retry = this.config().participantRetry;

		const prepareResults = await Promise.allSettled(
			participants.map(async (p) => {
				const { value: result } = await this.partitionClient(p.context)
					.send(
						"txPrepare",
						p.context,
						{ transactionId, coordinator, transactionTimestamp: transactionTs, items: p.items },
						p.items,
						// Backpressure is deterministic for the life of this transaction: the partition is over
						// its cap, and a split will not land inside a retry budget of a few seconds. Retrying
						// only adds latency before the same cancellation.
						{ retry: prepareRetry(retry, retry.prepareMaxAttempts) },
					)
					.catch((err: unknown) => {
						this.storePrepareError(transactionId, p.doName, err);
						throw err;
					});
				this.storePrepareAnswer(transactionId, p.doName, result);
				return { partitionDoName: p.doName, result };
			}),
		);

		const allAccepted = prepareResults.every((r) => r.status === "fulfilled" && r.value.result.outcome === "accepted");

		if (allAccepted) {
			// All accepted — PREPARED is the point of no return
			this.markPrepared(transactionId, idempotencyToken);
			await this.runCommit(transactionId, idempotencyToken, requestBudgetMs).catch((e: unknown) => {
				// A split moved the token: the client retries, and the new owner commits.
				if (FokosError.isCode(e, SHARDING_UNAVAILABLE_CODES.partition_migrating)) {
					throw e;
				}
				console.error({
					message: "fokos/tc: background commit failed",
					transactionId,
					idempotencyToken,
					error: String(e),
				});
			});
			return this.loadFinalResponse(transactionId, idempotencyToken);
		}

		this.cancelTransactionInStore(transactionId, idempotencyToken);
		await this.runCancel(transactionId, idempotencyToken, requestBudgetMs);
		return this.loadFinalResponse(transactionId, idempotencyToken);
	}

	/**
	 * `requestBudgetMs` bounds the retries of the fan-out: `fanoutRequestBudgetMs` when a request waits
	 * on it, and the rest of `alarmRecoveryBudgetMs` for the `tx_recovery` job. A participant with no answer at the deadline
	 * stays unconfirmed, and the transaction stays non-terminal for the `tx_recovery` job.
	 */
	private async runCommit(transactionId: string, idempotencyToken: string, requestBudgetMs: number): Promise<void> {
		// More than one drive of a transaction can run at the same time: the request, a retry with the
		// same token, the `tx_recovery` job, and a participant's recovery call. Each one sends commits
		// only when the stored decision is commit. A drive whose own decision lost sends nothing.
		const decisionWinner = this.transition(
			idempotencyToken,
			() =>
				this.ctx.storage.sql.exec(
					`UPDATE tc_state SET state = 'COMMITTING' WHERE transaction_id = ? AND state IN ('PREPARED', 'COMMITTING')`,
					transactionId,
				).rowsWritten > 0,
		);
		if (!decisionWinner) {
			return;
		}

		const stateRow = this.loadStateRow(transactionId)!;
		// Keys only, as in runCancel: every participant applies the payload from its own
		// pending_transactions rows, which prepare wrote, so the commit RPC carries routing
		// information and never up to MAX_PAYLOAD_BYTES_PER_TX of data the participant already holds.
		const keysByPartition = groupByPartition(this.loadItemKeys(transactionId));
		const deadlineMs = this.fokosNow() + requestBudgetMs;

		const pendingParticipants = this.ctx.storage.sql
			.exec<TcParticipantRow>(
				`SELECT transaction_id, partition_do_name, partition_context_json, prepare_outcome, commit_outcome, cancel_outcome
                 FROM tc_participants WHERE transaction_id = ? AND commit_outcome IS NULL`,
				transactionId,
			)
			.toArray();

		await Promise.allSettled(
			pendingParticipants.map(async (p) => {
				// Past the request budget, stop dispatching: this participant stays unconfirmed, the
				// transaction stays in COMMITTING, the caller receives the commit-pending error, and the
				// `tx_recovery` job finishes the fan-out.
				if (this.fokosNow() > deadlineMs) {
					return;
				}
				const pCtx = deserializePartitionContext(p.partition_context_json);
				const keys = toTransactionItemKeys(keysByPartition.get(p.partition_do_name) ?? []);
				await this.partitionClient(pCtx).send(
					"txCommit",
					pCtx,
					{ transactionId, transactionTimestamp: stateRow.transaction_ts, items: keys },
					keys,
					{ retry: this.fanoutRetry(deadlineMs) },
				);
				this.ctx.storage.sql.exec(
					`UPDATE tc_participants SET commit_outcome = 'committed' WHERE transaction_id = ? AND partition_do_name = ?`,
					transactionId,
					p.partition_do_name,
				);
			}),
		);

		// Defensive: only advance to COMMITTED when all participants confirmed.
		const uncommitted = one(
			this.ctx.storage.sql.exec<{ n: number }>(
				`SELECT COUNT(*) as n FROM tc_participants WHERE transaction_id = ? AND commit_outcome IS NULL`,
				transactionId,
			),
		).n;
		if (uncommitted === 0) {
			await this.completeTransaction(transactionId, idempotencyToken, "COMMITTED");
		}
	}

	/** `requestBudgetMs` bounds the fan-out exactly as it does in runCommit. */
	private async runCancel(transactionId: string, idempotencyToken: string, requestBudgetMs: number): Promise<void> {
		// Sends cancels only when the stored decision is cancel, to avoid racy runs breaking atomicity.
		// A cancel after a commit decision makes a participant release a lock that the commit must apply,
		// and the participant then answers that commit with the idempotent success.
		if (this.loadStateRow(transactionId)?.state !== "CANCELLING") {
			return;
		}
		// Keys only: cancel routes on them but never reads the payload, and this path runs on every
		// contended transaction, so loading up to MAX_PAYLOAD_BYTES of item data would be pure waste.
		// tc_items is written before any prepare RPC, so a NULL-outcome participant still gets its keys.
		const keysByPartition = groupByPartition(this.loadItemKeys(transactionId));
		const deadlineMs = this.fokosNow() + requestBudgetMs;

		// Cancel any participant not yet committed and not yet cancelled — this includes both
		// confirmed 'accepted' and NULL-outcome participants that may have silently locked items
		// (e.g., response lost in transit). PartitionDO.cancel is a no-op DELETE, so sending it
		// to a participant that never prepared is safe.
		const pendingParticipants = this.ctx.storage.sql
			.exec<TcParticipantRow>(
				`SELECT transaction_id, partition_do_name, partition_context_json, prepare_outcome, commit_outcome, cancel_outcome
                 FROM tc_participants WHERE transaction_id = ? AND commit_outcome IS NULL AND cancel_outcome IS NULL`,
				transactionId,
			)
			.toArray();

		await Promise.allSettled(
			pendingParticipants.map(async (p) => {
				// Past the request budget, stop dispatching: this participant stays unconfirmed, the
				// transaction stays in CANCELLING, and the `tx_recovery` job finishes the fan-out. The caller
				// still receives the cancelled outcome, which applied nothing anywhere.
				if (this.fokosNow() > deadlineMs) {
					return;
				}
				const pCtx = deserializePartitionContext(p.partition_context_json);
				const keys = toTransactionItemKeys(keysByPartition.get(p.partition_do_name) ?? []);
				await this.partitionClient(pCtx).send("txCancel", pCtx, { transactionId, items: keys }, keys, {
					retry: this.fanoutRetry(deadlineMs),
				});
				this.ctx.storage.sql.exec(
					`UPDATE tc_participants SET cancel_outcome = 'cancelled' WHERE transaction_id = ? AND partition_do_name = ?`,
					transactionId,
					p.partition_do_name,
				);
			}),
		);

		// Only advance to CANCELLED once every eligible participant is confirmed — otherwise leave
		// in CANCELLING so the `tx_recovery` job retries the remaining ones.
		const stillPending = one(
			this.ctx.storage.sql.exec<{ n: number }>(
				`SELECT COUNT(*) as n FROM tc_participants WHERE transaction_id = ? AND commit_outcome IS NULL AND cancel_outcome IS NULL`,
				transactionId,
			),
		).n;
		if (stillPending === 0) {
			await this.completeTransaction(transactionId, idempotencyToken, "CANCELLED");
		}
	}

	private async runPrepareRecovery(transactionId: string, idempotencyToken: string, requestBudgetMs: number): Promise<void> {
		const stateRow = this.loadStateRow(transactionId);
		if (stateRow?.state !== "PREPARING") {
			return;
		}

		const items = this.loadItems(transactionId);
		const itemsByPartition = groupByPartition(items);
		const coordinator = this.coordinatorRef(idempotencyToken);

		const existingParticipants = this.loadParticipants(transactionId);

		const nullParticipants = existingParticipants.filter((p) => p.prepare_outcome === null);
		const retry = this.config().participantRetry;

		await Promise.allSettled(
			nullParticipants.map(async (p) => {
				const pCtx = deserializePartitionContext(p.partition_context_json);
				const partitionItems = toTransactionItems(itemsByPartition.get(p.partition_do_name) ?? []);
				const { value: r } = await this.partitionClient(pCtx)
					.send(
						"txPrepare",
						pCtx,
						{ transactionId, coordinator, transactionTimestamp: stateRow.transaction_ts, items: partitionItems },
						partitionItems,
						// Same as the first prepare pass: an over-size partition will not clear by retrying.
						{ retry: prepareRetry(retry, retry.prepareRecoveryMaxAttempts) },
					)
					.catch((err: unknown) => {
						this.storePrepareError(transactionId, p.partition_do_name, err);
						throw err;
					});
				this.storePrepareAnswer(transactionId, p.partition_do_name, r);
			}),
		);

		const allParticipants = this.loadParticipants(transactionId);
		// A participant that is still NULL threw again, and a throw is retryable: on its own it decides
		// nothing, so the transaction stays PREPARING for the `tx_recovery` job to drive later.
		// Only a real rejection or exceeding the hold deadline commits the transaction to cancelling;
		// cancelTransactionInStore then reports a still-NULL participant with the error it stored.
		const anyRejected = allParticipants.some((p) => p.prepare_outcome === "rejected");
		const allAccepted = allParticipants.every((p) => p.prepare_outcome === "accepted");
		const heldTooLong = this.fokosNow() - stateRow.created_at > maxPreparingHoldMs(this.config());

		if (allAccepted) {
			this.markPrepared(transactionId, idempotencyToken);
			await this.runCommit(transactionId, idempotencyToken, requestBudgetMs);
		} else if (anyRejected || heldTooLong) {
			this.cancelTransactionInStore(transactionId, idempotencyToken);
			await this.runCancel(transactionId, idempotencyToken, requestBudgetMs);
		}
		// If some participants are still NULL and under the hold deadline, leave in PREPARING; the `tx_recovery` job retries it.
	}

	/**
	 * One step of the `tx_recovery` job: drives the due transactions one at a time, earliest
	 * `next_recovery_at` first, for at most `recoveryScanRows` transactions and `alarmRecoveryBudgetMs`.
	 *
	 * Each iteration claims one transaction and then drives it. The claim moves `next_recovery_at`
	 * forward with `nextRecoveryAt`, so a transaction that the drive does not finish, for example
	 * because a participant is down, goes behind the others. A transaction that the step does not reach
	 * keeps its place. Each drive gets at most `fanoutRequestBudgetMs`, so one participant that does not
	 * answer cannot use the whole step.
	 */
	private async recoverStaleTransactions(): Promise<void> {
		const startedAt = this.fokosNow();
		const { staleTransactionMs, recoveryScanRows, alarmRecoveryBudgetMs, fanoutRequestBudgetMs } = this.config();
		// FIXME: claim a batch of transactions in one storage transaction and drive them concurrently,
		// with a bounded fan-out.
		for (let driven = 0; driven < recoveryScanRows; driven++) {
			const remainingMs = startedAt + alarmRecoveryBudgetMs - this.fokosNow();
			if (remainingMs <= 0) {
				return;
			}
			const [row] = this.claimDueTransactions(startedAt, 1, staleTransactionMs);
			if (!row) {
				return;
			}
			try {
				await this.driveTransaction(row.transaction_id, row.idempotency_token, row.state, Math.min(fanoutRequestBudgetMs, remainingMs));
			} catch (e) {
				console.error({
					message: "fokos/tc: recovery failed",
					transactionId: row.transaction_id,
					state: row.state,
					error: String(e),
				});
			}
		}
	}

	/**
	 * The transactions whose `next_recovery_at` is at or before `dueAt`, earliest first, at most `limit`
	 * of them, each with its `next_recovery_at` moved forward in the same storage transaction. The same
	 * storage transaction removes the payload of a claimed transaction that is past its prepare. The step
	 * passes its start time as `dueAt`, and a claim moves the time past it, so one step claims a
	 * transaction at most one time.
	 */
	private claimDueTransactions(
		dueAt: number,
		limit: number,
		staleTransactionMs: number,
	): { transaction_id: string; idempotency_token: string; state: TCState }[] {
		const now = this.fokosNow();
		return this.ctx.storage.transactionSync(() => {
			const rows = this.ctx.storage.sql
				.exec<{ transaction_id: string; idempotency_token: string; state: TCState; created_at: number }>(
					`SELECT transaction_id, idempotency_token, state, created_at FROM tc_state
					  WHERE completed_at IS NULL AND next_recovery_at <= ? ORDER BY next_recovery_at LIMIT ?`,
					dueAt,
					limit,
				)
				.toArray();
			for (const row of rows) {
				this.ctx.storage.sql.exec(
					`UPDATE tc_state SET next_recovery_at = ? WHERE transaction_id = ?`,
					nextRecoveryAt(now, row.created_at, staleTransactionMs),
					row.transaction_id,
				);
				if (isPastPrepare(row.state)) {
					this.stripPayload(row.transaction_id);
				}
			}
			return rows;
		});
	}

	/** The earliest `next_recovery_at` of a transaction that is not complete, or null. One seek of `idx_tc_state_recovery`. */
	private earliestRecoveryAt(): number | null {
		return (
			tryOne(
				this.ctx.storage.sql.exec<{ next_recovery_at: number }>(
					`SELECT next_recovery_at FROM tc_state WHERE completed_at IS NULL ORDER BY next_recovery_at LIMIT 1`,
				),
			)?.next_recovery_at ?? null
		);
	}

	/** Drives one non-terminal transaction from its stored state. `budgetMs` bounds the participant retries. */
	private async driveTransaction(transactionId: string, idempotencyToken: string, state: TCState, budgetMs: number): Promise<void> {
		switch (state) {
			case "CREATED":
				await this.drivePrepare(transactionId, idempotencyToken, budgetMs);
				break;
			case "PREPARING":
				await this.runPrepareRecovery(transactionId, idempotencyToken, budgetMs);
				break;
			case "PREPARED":
			case "COMMITTING":
				await this.runCommit(transactionId, idempotencyToken, budgetMs);
				break;
			case "CANCELLING":
				await this.runCancel(transactionId, idempotencyToken, budgetMs);
				break;
		}
	}

	/**
	 * One step of the `idempotency_sweep` job: deletes one batch of the transactions whose idempotency
	 * window has passed. Returns now while expired rows remain, else null: the `deadline` of the job
	 * gives the next expiry.
	 */
	private sweepExpiredTransactions(): number | null {
		const cutoff = this.fokosNow() - IDEMPOTENCY_WINDOW_MS;
		const { sweepBatchRows, sweepDeleteChunkRows } = this.config();
		const expiredBatch = this.ctx.storage.sql
			.exec<{
				transaction_id: string;
			}>(`SELECT transaction_id FROM tc_state WHERE completed_at < ? ORDER BY completed_at, transaction_id LIMIT ?`, cutoff, sweepBatchRows)
			.toArray();
		if (expiredBatch.length > 0) {
			const ids = expiredBatch.map((r) => r.transaction_id);
			this.ctx.storage.transactionSync(() => {
				for (let i = 0; i < ids.length; i += sweepDeleteChunkRows) {
					const chunk = ids.slice(i, i + sweepDeleteChunkRows);
					const placeholders = chunk.map(() => "?").join(",");
					this.ctx.storage.sql.exec(`DELETE FROM tc_results WHERE transaction_id IN (${placeholders})`, ...chunk);
					this.ctx.storage.sql.exec(`DELETE FROM tc_state WHERE transaction_id IN (${placeholders})`, ...chunk);
				}
			});
		}

		const hasExpiredRows = exists(this.ctx.storage.sql.exec(`SELECT 1 FROM tc_state WHERE completed_at < ? LIMIT 1`, cutoff));
		return hasExpiredRows ? this.fokosNow() : null;
	}

	private earliestCompletedAt(): number | null {
		return one(
			this.ctx.storage.sql.exec<{ completed_at: number | null }>(
				`SELECT MIN(completed_at) AS completed_at FROM tc_state WHERE completed_at IS NOT NULL`,
			),
		).completed_at;
	}

	/**
	 * The local handler of `recoverTransaction`. It answers from the ledger and does not drive the
	 * transaction. For a transaction that is not complete, it sets `next_recovery_at` of the transaction
	 * to now, makes the `tx_recovery` job due now, and answers `driving`. The job sends the commit or the cancel to the participant, so the participant
	 * does not need to wait for the fan-out.
	 *
	 * A drive here would run beside the other drives of the transaction, one for each call. The job
	 * runs in the passes of the scheduler, one at a time, so calls from many participants give one drive.
	 */
	private async recoverTransactionLocal(transactionId: string): Promise<RecoverTransactionResult> {
		const state = tryOne(
			this.ctx.storage.sql.exec<{ state: TCState }>(`SELECT state FROM tc_state WHERE transaction_id = ?`, transactionId),
		)?.state;
		if (state === undefined) {
			return { state: "not_found" };
		}
		if (state === "COMMITTED" || state === "CANCELLED") {
			return { state };
		}
		const now = this.fokosNow();
		this.ctx.storage.sql.exec(
			`UPDATE tc_state SET next_recovery_at = MIN(next_recovery_at, ?) WHERE transaction_id = ? AND completed_at IS NULL`,
			now,
			transactionId,
		);
		await this.fokos.scheduleJob(JOB_TX_RECOVERY, now);
		return { state: "driving" };
	}

	// ═══ migration ═══════════════════════════════════════════════════════════

	/**
	 * One page of the transactions that a split target owns, with their rows in all four tables. The
	 * cursor is the last `transaction_id` read. A page reads at most `budget.pageRows` ledger rows and
	 * keeps its payload near `budget.pageBytes`: it stops before the transaction that would cross that
	 * budget, and always holds at least one.
	 */
	private buildMigrationPage(
		cursor: string | null,
		belongsToTarget: (key: RouteKey) => boolean,
		budget: FokosMigrationPageBudget,
	): { page: MigratedTransaction[]; nextCursor: string | null } {
		const rows = this.ctx.storage.sql.exec<TcStateRow>(
			`SELECT transaction_id, idempotency_token, state, transaction_ts, created_at, completed_at, next_recovery_at, results_json, operations_hash
             FROM tc_state WHERE transaction_id > ? ORDER BY transaction_id LIMIT ?`,
			cursor ?? "",
			budget.pageRows + 1,
		);
		const page: MigratedTransaction[] = [];
		let bytes = 0;
		let scanned = 0;
		let last: string | null = null;
		for (const row of rows) {
			// The extra row only shows that the ledger continues after this page.
			if (scanned === budget.pageRows) {
				return { page, nextCursor: last };
			}
			if (belongsToTarget(tokenKey(row.idempotency_token))) {
				const tx: MigratedTransaction = {
					state: row,
					// A transaction past its prepare needs only the keys. Its payload can still be on disk
					// before its first recovery claim, and the page does not carry it.
					items: this.loadItems(row.transaction_id, !isPastPrepare(row.state)),
					participants: this.loadParticipants(row.transaction_id),
					results: this.loadResultImages(row.transaction_id),
				};
				const txBytes = migratedTransactionBytes(tx);
				if (page.length > 0 && bytes + txBytes > budget.pageBytes) {
					return { page, nextCursor: last };
				}
				page.push(tx);
				bytes += txBytes;
			}
			scanned += 1;
			last = row.transaction_id;
		}
		return { page, nextCursor: null };
	}

	/**
	 * Writes the rows of one page. Idempotent: a page applied twice leaves the same rows. A
	 * non-terminal transaction keeps its `next_recovery_at`, which the `deadline()` of the `tx_recovery`
	 * job reads, so the job drives it after the import.
	 */
	private applyMigrationPage(page: MigratedTransaction[]): void {
		const sql = this.ctx.storage.sql;
		for (const { state, items, participants, results } of page) {
			sql.exec(
				`INSERT OR REPLACE INTO tc_state (transaction_id, idempotency_token, state, transaction_ts, created_at, completed_at, next_recovery_at, results_json, operations_hash)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				state.transaction_id,
				state.idempotency_token,
				state.state,
				state.transaction_ts,
				state.created_at,
				state.completed_at,
				state.next_recovery_at,
				state.results_json,
				state.operations_hash,
			);
			for (const r of items) {
				sql.exec(
					`INSERT OR REPLACE INTO tc_items (transaction_id, hk, sk, op_index, operation, data, data_kind, ttl_epoch_utc_seconds, conditions_json, update_json, partition_do_name, return_values_on_condition_check_failure)
					 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					r.transaction_id,
					r.hk,
					r.sk,
					r.op_index,
					r.operation,
					r.data,
					r.data_kind,
					r.ttl_epoch_utc_seconds,
					r.conditions_json,
					r.update_json,
					r.partition_do_name,
					r.return_values_on_condition_check_failure,
				);
			}
			for (const r of participants) {
				sql.exec(
					`INSERT OR REPLACE INTO tc_participants (transaction_id, partition_do_name, partition_context_json, prepare_outcome, commit_outcome, cancel_outcome, answer_json, error_json)
					 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
					r.transaction_id,
					r.partition_do_name,
					r.partition_context_json,
					r.prepare_outcome,
					r.commit_outcome,
					r.cancel_outcome,
					r.answer_json,
					r.error_json,
				);
			}
			for (const r of results) {
				sql.exec(
					`INSERT OR REPLACE INTO tc_results (transaction_id, op_index, image_kind, image_version, image_ttl_epoch_utc_seconds, image_data)
					 VALUES (?, ?, ?, ?, ?, ?)`,
					r.transaction_id,
					r.op_index,
					r.image_kind,
					r.image_version,
					r.image_ttl_epoch_utc_seconds,
					r.image_data,
				);
			}
		}
	}

	/** One bounded step of the cleanup of a split source. Returns true when no ledger row is left. */
	private deleteMigratedRowsStep(): boolean {
		const sql = this.ctx.storage.sql;
		const batch = `SELECT transaction_id FROM tc_state ORDER BY transaction_id LIMIT ${this.config().sweepBatchRows}`;
		for (const table of ["tc_items", "tc_participants", "tc_results", "tc_state"]) {
			sql.exec(`DELETE FROM ${table} WHERE transaction_id IN (${batch})`);
		}
		return !exists(sql.exec(`SELECT 1 FROM tc_state LIMIT 1`));
	}

	private loadStateRow(transactionId: string): TcStateRow | undefined {
		return tryOne(
			this.ctx.storage.sql.exec<TcStateRow>(
				`SELECT transaction_id, idempotency_token, state, transaction_ts, created_at, completed_at, next_recovery_at, results_json, operations_hash
                 FROM tc_state WHERE transaction_id = ?`,
				transactionId,
			),
		);
	}

	/** One seek of the unique token index, and no row read: the admission hook needs only to know that a row exists. */
	private hasStateRowForToken(idempotencyToken: string): boolean {
		return exists(this.ctx.storage.sql.exec(`SELECT 1 FROM tc_state WHERE idempotency_token = ? LIMIT 1`, idempotencyToken));
	}

	/** Two seeks of the unique token index. The second seeks past the first token. */
	private hasAtLeastTwoTransactions(): boolean {
		const sql = this.ctx.storage.sql;
		const first = tryOne(
			sql.exec<{ idempotency_token: string }>(`SELECT idempotency_token FROM tc_state ORDER BY idempotency_token LIMIT 1`),
		);
		return first !== undefined && exists(sql.exec(`SELECT 1 FROM tc_state WHERE idempotency_token > ? LIMIT 1`, first.idempotency_token));
	}

	private loadStateRowByToken(idempotencyToken: string): TcStateRow | undefined {
		return tryOne(
			this.ctx.storage.sql.exec<TcStateRow>(
				`SELECT transaction_id, idempotency_token, state, transaction_ts, created_at, completed_at, next_recovery_at, results_json, operations_hash
                 FROM tc_state WHERE idempotency_token = ?`,
				idempotencyToken,
			),
		);
	}

	/** The prepare fan-out of a stored transaction: the state, the participants, and their items. */
	private loadPrepareFanout(transactionId: string): PrepareFanout {
		const stateRow = this.loadStateRow(transactionId)!;
		const itemsByPartition = groupByPartition(this.loadItems(transactionId));
		return {
			transactionTs: stateRow.transaction_ts,
			participants: this.loadParticipants(transactionId).map((p) => ({
				doName: p.partition_do_name,
				context: deserializePartitionContext(p.partition_context_json),
				items: toTransactionItems(itemsByPartition.get(p.partition_do_name) ?? []),
			})),
		};
	}

	/** With `withPayload` false, the payload columns are NULL, so the rows do not hold the payload in memory. */
	private loadItems(transactionId: string, withPayload = true): TcItemRow[] {
		const payload = withPayload
			? "data, data_kind, ttl_epoch_utc_seconds, conditions_json, update_json"
			: "NULL AS data, NULL AS data_kind, ttl_epoch_utc_seconds, NULL AS conditions_json, NULL AS update_json";
		return this.ctx.storage.sql
			.exec<TcItemRow>(
				`SELECT transaction_id, hk, sk, op_index, operation, ${payload}, partition_do_name, return_values_on_condition_check_failure
                 FROM tc_items WHERE transaction_id = ? ORDER BY op_index`,
				transactionId,
			)
			.toArray();
	}

	/** The routing half of loadItems: no data, no conditions — see runCancel. */
	private loadItemKeys(transactionId: string): Pick<TcItemRow, "hk" | "sk" | "partition_do_name">[] {
		return this.ctx.storage.sql
			.exec<
				Pick<TcItemRow, "hk" | "sk" | "partition_do_name">
			>(`SELECT hk, sk, partition_do_name FROM tc_items WHERE transaction_id = ?`, transactionId)
			.toArray();
	}

	private loadResultImages(transactionId: string): TcResultRow[] {
		return this.ctx.storage.sql
			.exec<TcResultRow>(
				`SELECT transaction_id, op_index, image_kind, image_version, image_ttl_epoch_utc_seconds, image_data
                 FROM tc_results WHERE transaction_id = ? ORDER BY op_index`,
				transactionId,
			)
			.toArray();
	}

	private loadParticipants(transactionId: string): TcParticipantRow[] {
		return this.ctx.storage.sql
			.exec<TcParticipantRow>(
				`SELECT transaction_id, partition_do_name, partition_context_json, prepare_outcome, commit_outcome, cancel_outcome, answer_json, error_json
                 FROM tc_participants WHERE transaction_id = ? ORDER BY partition_do_name`,
				transactionId,
			)
			.toArray();
	}
}

/** The retry rule of a prepare: every error except `partition_over_size`, up to `maxAttempts` attempts. */
function prepareRetry(retry: ParticipantRetryConfig, maxAttempts: number): FokosRetryPolicy {
	return {
		shouldRetry: (err, nextAttempt) => !FokosError.isCode(err, UNAVAILABLE_CODES.partition_over_size) && nextAttempt <= maxAttempts,
		baseDelayMs: retry.baseDelayMs,
		maxDelayMs: retry.maxDelayMs,
	};
}

/** The time at which the `idempotency_sweep` job can delete a transaction that completed at `completedAt`. */
function sweepDueAt(completedAt: number): number {
	return completedAt + IDEMPOTENCY_WINDOW_MS + 1;
}

/**
 * True for every state after the prepare, the terminal states included. No step of these states reads the
 * payload. A terminal transaction has no `tc_items` rows, because the completion deletes them.
 */
function isPastPrepare(state: TCState): boolean {
	switch (state) {
		case "CREATED":
		case "PREPARING":
			return false;
		case "PREPARED":
		case "COMMITTING":
		case "COMMITTED":
		case "CANCELLING":
		case "CANCELLED":
			return true;
		default: {
			const _exhaustive: never = state;
			return _exhaustive;
		}
	}
}

/** The size of one migrated transaction, near its serialized size: the payloads and the text columns. */
function migratedTransactionBytes(tx: MigratedTransaction): number {
	const size = (v: string | ArrayBuffer | null) => (v === null ? 0 : typeof v === "string" ? v.length : v.byteLength);
	let bytes = 256 + size(tx.state.results_json);
	for (const r of tx.items) {
		bytes += 128 + r.hk.byteLength + r.sk.byteLength + size(r.data) + size(r.conditions_json) + size(r.update_json);
	}
	for (const r of tx.participants) {
		bytes += 128 + size(r.partition_context_json) + size(r.answer_json) + size(r.error_json);
	}
	for (const r of tx.results) {
		bytes += 64 + size(r.image_data);
	}
	return bytes;
}

function deserializePartitionContext(json: string): FokosDBRouteContext {
	return JSON.parse(json) as FokosDBRouteContext;
}

function groupByPartition<T extends Pick<TcItemRow, "partition_do_name">>(items: T[]): Map<string, T[]> {
	const map = new Map<string, T[]>();
	for (const item of items) {
		let arr = map.get(item.partition_do_name);
		if (!arr) {
			arr = [];
			map.set(item.partition_do_name, arr);
		}
		arr.push(item);
	}
	return map;
}

function toTransactionItemKeys(rows: Pick<TcItemRow, "hk" | "sk">[]): TransactionItemKey[] {
	return rows.map((row) => ({
		hashKey: keyFromBlob(row.hk),
		sortKey: keyFromBlob(row.sk), // empty KeyBytes ([]) is the absent sentinel
	}));
}

function toTransactionItems(rows: TcItemRow[]): TransactionItem[] {
	return rows.map((row) => ({
		opIndex: row.op_index,
		hashKey: keyFromBlob(row.hk),
		sortKey: keyFromBlob(row.sk), // empty KeyBytes ([]) is the absent sentinel
		operation: row.operation as TransactionItem["operation"],
		data: row.data instanceof ArrayBuffer ? new Uint8Array(row.data) : (row.data ?? undefined),
		kind: row.data_kind === null ? undefined : (DATA_KINDS[row.data_kind] as DataKind),
		ttlAt: row.ttl_epoch_utc_seconds ?? undefined,
		condition: row.conditions_json ? parseJSONTrusted<CompiledConditionPlan>(row.conditions_json) : undefined,
		update: row.update_json ? parseJSONTrusted<CompiledUpdatePlan>(row.update_json) : undefined,
		returnValuesOnConditionCheckFailure: row.return_values_on_condition_check_failure === 1 ? "all_old" : undefined,
	}));
}
