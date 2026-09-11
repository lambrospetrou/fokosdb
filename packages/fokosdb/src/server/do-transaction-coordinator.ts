import { DurableObject } from "cloudflare:workers";
import { SQLSchemaMigration, SQLSchemaMigrations } from "durable-utils/sql-migrations";
import { tryWhile } from "durable-utils/retries";
import type { PartitionContextResolved } from "../shared/partition-topology/partition-context.js";
import { KeyCodec, type KeyBytes } from "../shared/partition-topology/key-codec.js";
import { DATA_KINDS, type DataKind } from "../shared/types.js";
import type {
	ExecutionFailureCode,
	InitiateWriteRequest,
	InitiateWriteResponseEncoded,
	ParticipantOperationResultEncoded,
	PrepareResponse,
	RecoverTransactionResult,
	RejectionReasonEncoded,
	TCState,
	TransactionItem,
	TransactionItemKey,
	TransactWriteOperationResultEncoded,
} from "../shared/transaction-types.js";
import { PartitionDO } from "./do-partition.js";
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
import { DESTROY_ABORT_SENTINEL } from "../shared/cf-utils.js";
import { hashTransactionOperations } from "../shared/transaction-idempotency.js";
import { unexpectedTransactionStateError } from "../shared/errors-operations.js";
import {
	ALARM_RECOVERY_BUDGET_MS,
	applyImageCap,
	decodeItemKeys,
	IDEMPOTENCY_WINDOW_MS,
	MAX_TC_DATABASE_BYTES,
	SWEEP_BATCH_ROWS,
} from "../shared/transaction-limits.js";

type TcStateRow = {
	transaction_id: string;
	idempotency_token: string;
	state: TCState;
	transaction_ts: number;
	created_at: number;
	completed_at: number | null;
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
	if (reason.code !== "condition_failed" || reason.item === undefined) return reason;
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
	if (r.outcome === "accepted") return r;
	return {
		outcome: "rejected",
		results: r.results.map((res) => (res.outcome === "rejected" ? { ...res, reason: reasonWithoutImage(res.reason) } : res)),
	};
}

// tc_items hk/sk are BLOB; materialize a read column as KeyBytes (trusted re-brand, no copy of bytes).
function keyFromBlob(value: ArrayBuffer): KeyBytes {
	return KeyCodec.asKeyBytes(new Uint8Array(value));
}

const STALE_THRESHOLD_MS = 5_000;
const MAX_PREPARING_HOLD_MS = Math.min(5 * STALE_THRESHOLD_MS, IDEMPOTENCY_WINDOW_MS);

/**
 * Wall-clock budget for a participant fan-out that a request waits on, commit and cancel alike.
 * Past this deadline the coordinator stops dispatching participant RPCs and leaves the unconfirmed
 * participant behind: a commit leaves the transaction in COMMITTING and the caller receives the
 * commit-pending error, a cancel leaves it in CANCELLING and the caller receives the cancelled
 * outcome it is already entitled to. The alarm then finishes the fan-out with the full retry
 * budget, because nothing waits on it. Without the budget, one unreachable participant would hold
 * the request, and the shard, for tens of seconds.
 */
export const TX_FANOUT_REQUEST_BUDGET_MS = 5_000;

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

export class TransactionCoordinatorDO extends DurableObject<Env> {
	#migrations: SQLSchemaMigrations;

	static get(
		ns: DurableObjectNamespace<TransactionCoordinatorDO>,
		id: DurableObjectId | string,
	): DurableObjectStub<TransactionCoordinatorDO> {
		if (typeof id === "string") {
			id = ns.idFromString(id);
		}
		return ns.get(id);
	}
	static getByName(ns: DurableObjectNamespace<TransactionCoordinatorDO>, doName: string): DurableObjectStub<TransactionCoordinatorDO> {
		return ns.getByName(doName);
	}

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.#migrations = new SQLSchemaMigrations({
			migrations: sqlMigrations,
			doStorage: ctx.storage,
		});
		void ctx.blockConcurrencyWhile(async () => {
			this.#migrations.runAllSync();
		});
	}

	//////////////////////////////
	// User overridable methods.
	//////////////////////////////

	/**
	 * The wall-clock budget a request-driven fan-out gets, read at each use so a subclass can vary
	 * it. See TX_FANOUT_REQUEST_BUDGET_MS for what the deadline means for the caller.
	 */
	fokosFanoutRequestBudgetMs(): number {
		return TX_FANOUT_REQUEST_BUDGET_MS;
	}

	//////////////////////////////
	// Transaction methods.
	//////////////////////////////

	private async ensureAlarmAt(targetMs: number): Promise<void> {
		const existing = await this.ctx.storage.getAlarm();
		if (existing === null || targetMs < existing) await this.ctx.storage.setAlarm(targetMs);
	}

	async initiateWrite(request: InitiateWriteRequest): Promise<InitiateWriteResponseEncoded> {
		const transactionId = crypto.randomUUID().replaceAll("-", "");
		const idempotencyToken = request.clientRequestToken ?? transactionId;
		const coordinatorDoId = this.ctx.id.toString();

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

		if (this.ctx.storage.sql.databaseSize > MAX_TC_DATABASE_BYTES) {
			throw new FokosUnavailableError(UNAVAILABLE_CODES.coordinator_over_size, {
				message: "transaction coordinator exceeded its storage limit, please retry later",
			});
		}

		// Key/operation validation is the client's single boundary (FokosDB.transactWriteItems); the TC
		// receives already-validated, already-encoded operations.

		// TODO: append DO shard suffix for tie-breaking when TC pooling is introduced
		const transactionTs = Date.now();

		// Collect one partitionContext per distinct partition (doName → context).
		const partitionContextByDoName = new Map<string, PartitionContextResolved>();
		for (const op of request.items) {
			partitionContextByDoName.set(op.partitionContext.doName, op.partitionContext);
		}

		this.ctx.storage.transactionSync(() => {
			this.ctx.storage.sql.exec(
				`INSERT INTO tc_state (transaction_id, idempotency_token, state, transaction_ts, created_at, operations_hash)
                 VALUES (?, ?, 'CREATED', ?, ?, ?)`,
				transactionId,
				idempotencyToken,
				transactionTs,
				Date.now(),
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
					op.partitionContext.doName,
					op.returnValuesOnConditionCheckFailure === "all_old" ? 1 : 0,
				);
			}
			for (const [partitionDoName, pCtx] of partitionContextByDoName) {
				this.ctx.storage.sql.exec(
					`INSERT INTO tc_participants (transaction_id, partition_do_name, partition_context_json) VALUES (?, ?, ?)`,
					transactionId,
					partitionDoName,
					JSON.stringify(pCtx),
				);
			}
		});

		await this.ensureAlarmAt(Date.now() + STALE_THRESHOLD_MS);

		return await this.drivePrepare(transactionId, idempotencyToken, coordinatorDoId, this.fokosFanoutRequestBudgetMs());
	}

	private async resumeTransaction(existingRow: TcStateRow, idempotencyToken: string): Promise<InitiateWriteResponseEncoded> {
		const { transaction_id: transactionId } = existingRow;
		switch (existingRow.state) {
			case "COMMITTED":
				return this.loadFinalResponse(transactionId, idempotencyToken, existingRow);
			case "CANCELLED":
				return this.loadFinalResponse(transactionId, idempotencyToken, existingRow);
			case "PREPARING": {
				await this.runPrepareRecovery(transactionId, idempotencyToken, this.fokosFanoutRequestBudgetMs());
				return this.loadFinalResponse(transactionId, idempotencyToken);
			}
			case "PREPARED":
			case "COMMITTING": {
				await this.runCommit(transactionId, idempotencyToken, this.fokosFanoutRequestBudgetMs());
				return this.loadFinalResponse(transactionId, idempotencyToken);
			}
			case "CANCELLING": {
				await this.runCancel(transactionId, idempotencyToken, this.fokosFanoutRequestBudgetMs());
				return this.loadFinalResponse(transactionId, idempotencyToken);
			}
			case "CREATED": {
				const coordinatorDoId = this.ctx.id.toString();
				return await this.drivePrepare(transactionId, idempotencyToken, coordinatorDoId, this.fokosFanoutRequestBudgetMs());
			}
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
	 *   states throw the commit-pending error instead. `alarm()` finishes the fan-out, and a retry
	 *   with the same token answers "committed" once the last participant confirms.
	 * - CANCELLING / CANCELLED: a cancelled transaction applied nothing anywhere, so outstanding
	 *   cleanup cannot change what the caller observes, and the answer follows the decision. Only
	 *   the lock release lags, and `alarm()` drives that too.
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
					if (res?.outcome !== "rejected" || res.reason.code !== "condition_failed") continue;
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
				// No decision yet — the alarm will drive it. The outcome can still go either way, so
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

	private stripPayload(transactionId: string): void {
		this.ctx.storage.sql.exec(
			`UPDATE tc_items SET data = NULL, data_kind = NULL, conditions_json = NULL, update_json = NULL WHERE transaction_id = ?`,
			transactionId,
		);
	}

	private completeTransaction(transactionId: string, terminalState: Extract<TCState, "COMMITTED" | "CANCELLED">): number | null {
		const expectedState = terminalState === "COMMITTED" ? "COMMITTING" : "CANCELLING";
		const completedAt = Date.now();
		let transitioned = false;
		this.ctx.storage.transactionSync(() => {
			const transition = this.ctx.storage.sql.exec(
				`UPDATE tc_state SET state = ?, completed_at = ? WHERE transaction_id = ? AND state = ?`,
				terminalState,
				completedAt,
				transactionId,
				expectedState,
			);
			if (transition.rowsWritten === 0) return;
			this.ctx.storage.sql.exec(`DELETE FROM tc_items WHERE transaction_id = ?`, transactionId);
			this.ctx.storage.sql.exec(`DELETE FROM tc_participants WHERE transaction_id = ?`, transactionId);
			transitioned = true;
		});
		return transitioned ? completedAt : null;
	}

	/**
	 * Records one participant's prepare answer, and its images, in the same storage transaction as the
	 * prepare outcome they belong to. No part of an answer then lives only in memory, so a coordinator
	 * evicted between a participant's answer and the transaction's decision still reads back every
	 * outcome, reason, and image that participant reported.
	 */
	private storePrepareAnswer(transactionId: string, partitionDoName: string, answer: PrepareResponse): void {
		this.ctx.storage.transactionSync(() => {
			this.ctx.storage.sql.exec(
				`UPDATE tc_participants SET prepare_outcome = ?, answer_json = ? WHERE transaction_id = ? AND partition_do_name = ?`,
				answer.outcome,
				answer.outcome === "rejected" ? stringifyTagged(stripImagesFromPrepareResponse(answer)) : null,
				transactionId,
				partitionDoName,
			);
			if (answer.outcome !== "rejected") return;
			for (const res of answer.results) {
				if (res.outcome !== "rejected" || res.reason.code !== "condition_failed" || !res.reason.item) continue;
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

	/**
	 * Records why a participant's prepare threw after its retries, so the cancel reports the cause. A
	 * participant that has answered keeps its answer, and recovery can still re-prepare one that has not.
	 */
	private storePrepareError(transactionId: string, partitionDoName: string, err: unknown): void {
		this.ctx.storage.sql.exec(
			`UPDATE tc_participants SET error_json = ? WHERE transaction_id = ? AND partition_do_name = ? AND prepare_outcome IS NULL`,
			stringifyTagged(FokosError.toWire(err)),
			transactionId,
			partitionDoName,
		);
	}

	/**
	 * Merges every participant's stored answer into the transaction's outcome and records the durable
	 * transition to CANCELLING.
	 *
	 * Both drivePrepare and runPrepareRecovery call it, so the two writers of CANCELLING cannot merge
	 * differently: the answers come from storage, never from what the caller still holds in memory.
	 */
	private cancelTransactionInStore(transactionId: string): void {
		this.ctx.storage.transactionSync(() => {
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
					if (r.opIndex >= 0 && r.opIndex < merged.length) merged[r.opIndex] = r;
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
			if (transition.rowsWritten === 0) return;

			this.stripPayload(transactionId);
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
		if (p.error_json) return parseTagged<FokosErrorWire>(p.error_json);
		return FokosError.toWire(
			new FokosUnavailableError(UNAVAILABLE_CODES.prepare_unanswered, {
				message: "a participant did not answer the prepare",
				attributes: { partitionDoName: p.partition_do_name },
			}),
		);
	}

	private async drivePrepare(
		transactionId: string,
		idempotencyToken: string,
		coordinatorDoId: string,
		requestBudgetMs?: number,
	): Promise<InitiateWriteResponseEncoded> {
		this.ctx.storage.sql.exec(`UPDATE tc_state SET state = 'PREPARING' WHERE transaction_id = ? AND state = 'CREATED'`, transactionId);

		const stateRow = this.loadStateRow(transactionId)!;
		const items = this.loadItems(transactionId);
		const participants = this.loadParticipants(transactionId);
		const itemsByPartition = groupByPartition(items);

		const prepareResults = await Promise.allSettled(
			participants.map(async (p) => {
				const pCtx = deserializePartitionContext(p.partition_context_json);
				const partitionItems = itemsByPartition.get(p.partition_do_name) ?? [];
				const result = await tryWhile(
					async () => {
						const r = await PartitionDO.getByName(this.env[pCtx.ns], p.partition_do_name).txPrepare(pCtx, {
							transactionId,
							coordinatorDoId,
							transactionTimestamp: stateRow.transaction_ts,
							items: toTransactionItems(partitionItems),
						});
						this.storePrepareAnswer(transactionId, p.partition_do_name, r);
						return r;
					},
					// Backpressure is deterministic for the life of this transaction: the partition is over
					// its cap, and a split will not land inside a retry budget of a few seconds. Retrying
					// only adds latency before the same cancellation.
					(err, nextAttempt) => !FokosError.isCode(err, UNAVAILABLE_CODES.partition_over_size) && nextAttempt <= 3,
					{ baseDelayMs: 100, maxDelayMs: 2_000 },
				).catch((err: unknown) => {
					this.storePrepareError(transactionId, p.partition_do_name, err);
					throw err;
				});
				return { partitionDoName: p.partition_do_name, result };
			}),
		);

		const allAccepted = prepareResults.every((r) => r.status === "fulfilled" && r.value.result.outcome === "accepted");

		if (allAccepted) {
			// All accepted — PREPARED is the point of no return
			this.ctx.storage.transactionSync(() => {
				const transition = this.ctx.storage.sql.exec(
					`UPDATE tc_state SET state = 'PREPARED' WHERE transaction_id = ? AND state = 'PREPARING'`,
					transactionId,
				);
				if (transition.rowsWritten > 0) this.stripPayload(transactionId);
			});
			await this.runCommit(transactionId, idempotencyToken, requestBudgetMs).catch((e) =>
				console.error({
					message: "fokos/tc: background commit failed",
					transactionId,
					idempotencyToken,
					error: String(e),
				}),
			);
			return this.loadFinalResponse(transactionId, idempotencyToken);
		}

		this.cancelTransactionInStore(transactionId);
		await this.runCancel(transactionId, idempotencyToken, requestBudgetMs);
		return this.loadFinalResponse(transactionId, idempotencyToken);
	}

	/**
	 * `requestBudgetMs` bounds the fan-out when a request waits on it (see
	 * TX_FANOUT_REQUEST_BUDGET_MS). Undefined — the alarm and the recovery paths — means no
	 * deadline, so those keep the full retry budget.
	 */
	private async runCommit(transactionId: string, idempotencyToken: string, requestBudgetMs?: number): Promise<void> {
		this.ctx.storage.sql.exec(
			`UPDATE tc_state SET state = 'COMMITTING' WHERE transaction_id = ? AND state IN ('PREPARED', 'COMMITTING')`,
			transactionId,
		);

		const stateRow = this.loadStateRow(transactionId)!;
		// Keys only, as in runCancel: every participant applies the payload from its own
		// pending_transactions rows, which prepare wrote, so the commit RPC carries routing
		// information and never up to MAX_PAYLOAD_BYTES_PER_TX of data the participant already holds.
		const keysByPartition = groupByPartition(this.loadItemKeys(transactionId));
		const deadlineMs = requestBudgetMs === undefined ? Number.POSITIVE_INFINITY : Date.now() + requestBudgetMs;

		const pendingParticipants = this.ctx.storage.sql
			.exec<TcParticipantRow>(
				`SELECT transaction_id, partition_do_name, partition_context_json, prepare_outcome, commit_outcome, cancel_outcome
                 FROM tc_participants WHERE transaction_id = ? AND commit_outcome IS NULL`,
				transactionId,
			)
			.toArray();

		await Promise.allSettled(
			pendingParticipants.map(async (p) => {
				const pCtx = deserializePartitionContext(p.partition_context_json);
				const partitionKeys = keysByPartition.get(p.partition_do_name) ?? [];
				await tryWhile(
					async () => {
						// Past the request budget, stop dispatching: this participant stays unconfirmed,
						// the transaction stays in COMMITTING, the caller receives the commit-pending
						// error, and the alarm finishes the fan-out.
						if (Date.now() > deadlineMs) return;
						await PartitionDO.getByName(this.env[pCtx.ns], p.partition_do_name).txCommit(pCtx, {
							transactionId,
							transactionTimestamp: stateRow.transaction_ts,
							items: toTransactionItemKeys(partitionKeys),
						});
						this.ctx.storage.sql.exec(
							`UPDATE tc_participants SET commit_outcome = 'committed' WHERE transaction_id = ? AND partition_do_name = ?`,
							transactionId,
							p.partition_do_name,
						);
					},
					(_err, nextAttempt) => nextAttempt <= 10,
					{ baseDelayMs: 100, maxDelayMs: 2_000 },
				);
			}),
		);

		// Defensive: only advance to COMMITTED when all participants confirmed.
		const uncommitted =
			this.ctx.storage.sql
				.exec<{ n: number }>(`SELECT COUNT(*) as n FROM tc_participants WHERE transaction_id = ? AND commit_outcome IS NULL`, transactionId)
				.toArray()[0]?.n ?? 0;
		if (uncommitted === 0) {
			const completedAt = this.completeTransaction(transactionId, "COMMITTED");
			if (completedAt !== null) await this.ensureAlarmAt(completedAt + IDEMPOTENCY_WINDOW_MS + 1);
		}
	}

	/** `requestBudgetMs` bounds the fan-out exactly as it does in runCommit. */
	private async runCancel(transactionId: string, idempotencyToken: string, requestBudgetMs?: number): Promise<void> {
		// Keys only: cancel routes on them but never reads the payload, and this path runs on every
		// contended transaction, so loading up to MAX_PAYLOAD_BYTES of item data would be pure waste.
		// tc_items is written before any prepare RPC, so a NULL-outcome participant still gets its keys.
		const keysByPartition = groupByPartition(this.loadItemKeys(transactionId));
		const deadlineMs = requestBudgetMs === undefined ? Number.POSITIVE_INFINITY : Date.now() + requestBudgetMs;

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
				const pCtx = deserializePartitionContext(p.partition_context_json);
				await tryWhile(
					async () => {
						// Past the request budget, stop dispatching: this participant stays unconfirmed,
						// the transaction stays in CANCELLING, and the alarm finishes the fan-out. The
						// caller still receives the cancelled outcome, which applied nothing anywhere.
						if (Date.now() > deadlineMs) return;
						await PartitionDO.getByName(this.env[pCtx.ns], p.partition_do_name).txCancel(pCtx, {
							transactionId,
							items: toTransactionItemKeys(keysByPartition.get(p.partition_do_name) ?? []),
						});
						this.ctx.storage.sql.exec(
							`UPDATE tc_participants SET cancel_outcome = 'cancelled' WHERE transaction_id = ? AND partition_do_name = ?`,
							transactionId,
							p.partition_do_name,
						);
					},
					(_err, nextAttempt) => nextAttempt <= 10,
					{ baseDelayMs: 100, maxDelayMs: 2_000 },
				);
			}),
		);

		// Only advance to CANCELLED once every eligible participant is confirmed — otherwise leave
		// in CANCELLING so the alarm retries the remaining ones.
		const stillPending =
			this.ctx.storage.sql
				.exec<{
					n: number;
				}>(
					`SELECT COUNT(*) as n FROM tc_participants WHERE transaction_id = ? AND commit_outcome IS NULL AND cancel_outcome IS NULL`,
					transactionId,
				)
				.toArray()[0]?.n ?? 0;
		if (stillPending === 0) {
			const completedAt = this.completeTransaction(transactionId, "CANCELLED");
			if (completedAt !== null) await this.ensureAlarmAt(completedAt + IDEMPOTENCY_WINDOW_MS + 1);
		}
	}

	private async runPrepareRecovery(transactionId: string, idempotencyToken: string, requestBudgetMs?: number): Promise<void> {
		const stateRow = this.loadStateRow(transactionId);
		if (!stateRow) return;

		const items = this.loadItems(transactionId);
		const itemsByPartition = groupByPartition(items);
		const coordinatorDoId = this.ctx.id.toString();

		const existingParticipants = this.loadParticipants(transactionId);

		const nullParticipants = existingParticipants.filter((p) => p.prepare_outcome === null);

		await Promise.allSettled(
			nullParticipants.map(async (p) => {
				const pCtx = deserializePartitionContext(p.partition_context_json);
				const partitionItems = itemsByPartition.get(p.partition_do_name) ?? [];
				await tryWhile(
					async () => {
						const r = await PartitionDO.getByName(this.env[pCtx.ns], p.partition_do_name).txPrepare(pCtx, {
							transactionId,
							coordinatorDoId,
							transactionTimestamp: stateRow.transaction_ts,
							items: toTransactionItems(partitionItems),
						});
						this.storePrepareAnswer(transactionId, p.partition_do_name, r);
						return r;
					},
					// Same as the first prepare pass: an over-size partition will not clear by retrying.
					(err, nextAttempt) => !FokosError.isCode(err, UNAVAILABLE_CODES.partition_over_size) && nextAttempt <= 5,
					{ baseDelayMs: 100, maxDelayMs: 2_000 },
				).catch((err: unknown) => {
					this.storePrepareError(transactionId, p.partition_do_name, err);
					throw err;
				});
			}),
		);

		const allParticipants = this.loadParticipants(transactionId);
		// A participant that is still NULL threw again, and a throw is retryable: on its own it decides
		// nothing, so the transaction stays PREPARING for the alarm to drive with the full retry budget.
		// Only a real rejection or exceeding the hold deadline commits the transaction to cancelling;
		// cancelTransactionInStore then reports a still-NULL participant with the error it stored.
		const anyRejected = allParticipants.some((p) => p.prepare_outcome === "rejected");
		const allAccepted = allParticipants.every((p) => p.prepare_outcome === "accepted");
		const heldTooLong = Date.now() - stateRow.created_at > MAX_PREPARING_HOLD_MS;

		if (allAccepted) {
			this.ctx.storage.transactionSync(() => {
				const transition = this.ctx.storage.sql.exec(
					`UPDATE tc_state SET state = 'PREPARED' WHERE transaction_id = ? AND state = 'PREPARING'`,
					transactionId,
				);
				if (transition.rowsWritten > 0) this.stripPayload(transactionId);
			});
			await this.runCommit(transactionId, idempotencyToken, requestBudgetMs);
		} else if (anyRejected || heldTooLong) {
			this.cancelTransactionInStore(transactionId);
			await this.runCancel(transactionId, idempotencyToken, requestBudgetMs);
		}
		// If some participants still NULL and under the hold deadline, leave in PREPARING; alarm will retry
	}

	async alarm(): Promise<void> {
		const recoveryStartedAt = Date.now();
		const rows = this.ctx.storage.sql
			.exec<{
				idempotency_token: string;
				transaction_id: string;
				state: TCState;
			}>(
				`SELECT idempotency_token, transaction_id, state
                 FROM tc_state
                 WHERE state NOT IN ('COMMITTED', 'CANCELLED') AND created_at <= ?
                 ORDER BY created_at, transaction_id LIMIT 100`,
				recoveryStartedAt - STALE_THRESHOLD_MS,
			)
			.toArray();

		// FIXME: drive these transactions concurrently with a bounded fan-out.
		for (const row of rows) {
			if (Date.now() - recoveryStartedAt >= ALARM_RECOVERY_BUDGET_MS) break;
			try {
				const coordinatorDoId = this.ctx.id.toString();
				switch (row.state) {
					case "CREATED":
						await this.drivePrepare(row.transaction_id, row.idempotency_token, coordinatorDoId);
						break;
					case "PREPARING":
						await this.runPrepareRecovery(row.transaction_id, row.idempotency_token);
						break;
					case "PREPARED":
					case "COMMITTING":
						await this.runCommit(row.transaction_id, row.idempotency_token);
						break;
					case "CANCELLING":
						await this.runCancel(row.transaction_id, row.idempotency_token);
						break;
				}
			} catch (e) {
				console.error({
					message: "fokos/tc: alarm recovery failed",
					transactionId: row.transaction_id,
					state: row.state,
					error: String(e),
				});
			}
		}

		const now = Date.now();
		const cutoff = now - IDEMPOTENCY_WINDOW_MS;
		const expiredBatch = this.ctx.storage.sql
			.exec<{
				transaction_id: string;
			}>(
				`SELECT transaction_id FROM tc_state WHERE completed_at < ? ORDER BY completed_at, transaction_id LIMIT ?`,
				cutoff,
				SWEEP_BATCH_ROWS,
			)
			.toArray();
		if (expiredBatch.length > 0) {
			const ids = expiredBatch.map((r) => r.transaction_id);
			const CHUNK_SIZE = 100;
			this.ctx.storage.transactionSync(() => {
				for (let i = 0; i < ids.length; i += CHUNK_SIZE) {
					const chunk = ids.slice(i, i + CHUNK_SIZE);
					const placeholders = chunk.map(() => "?").join(",");
					this.ctx.storage.sql.exec(`DELETE FROM tc_results WHERE transaction_id IN (${placeholders})`, ...chunk);
					this.ctx.storage.sql.exec(`DELETE FROM tc_state WHERE transaction_id IN (${placeholders})`, ...chunk);
				}
			});
		}

		const hasExpiredRows =
			this.ctx.storage.sql
				.exec<{ found: number }>(`SELECT 1 AS found FROM tc_state WHERE completed_at < ? LIMIT 1`, cutoff)
				.toArray()[0] !== undefined;
		const hasNonTerminalRows =
			this.ctx.storage.sql
				.exec<{ found: number }>(`SELECT 1 AS found FROM tc_state WHERE state NOT IN ('COMMITTED', 'CANCELLED') LIMIT 1`)
				.toArray()[0] !== undefined;
		const earliestCompletedAt = this.ctx.storage.sql
			.exec<{ completed_at: number | null }>(`SELECT MIN(completed_at) AS completed_at FROM tc_state WHERE completed_at IS NOT NULL`)
			.toArray()[0]?.completed_at;

		let nextAlarmAt: number | null = hasNonTerminalRows ? now + STALE_THRESHOLD_MS : null;
		if (earliestCompletedAt != null) {
			const sweepAt = earliestCompletedAt + IDEMPOTENCY_WINDOW_MS + 1;
			nextAlarmAt = nextAlarmAt === null ? sweepAt : Math.min(nextAlarmAt, sweepAt);
		}
		if (hasExpiredRows) nextAlarmAt = now;
		if (nextAlarmAt !== null) {
			await this.ensureAlarmAt(Math.max(now, nextAlarmAt));
		}
	}

	/**
	 * Wipes this coordinator shard. Called by `FokosDB.destroy()` for every shard of the table.
	 *
	 * The idempotency window lives in `tc_state`, so a shard that survives a destroy answers a replayed
	 * `clientRequestToken` with the OLD transaction's outcome — "committed" for data that no longer
	 * exists. That is why destroy must reach the coordinators and not the partitions alone.
	 *
	 * Mirrors `PartitionDO.destroyPartition`, including the `abort()` eviction: the migration bookkeeping
	 * lives in the storage being wiped, and the in-memory `#migrations` would otherwise still believe the
	 * tables exist.
	 */
	async destroyCoordinator(): Promise<void> {
		console.warn({ message: "fokos/tc: Destroying transaction coordinator — deleting all storage.", doId: this.ctx.id.toString() });

		await this.ctx.blockConcurrencyWhile(async () => {
			// Cancel the recovery alarm before wiping storage, so it cannot fire on the evicted instance
			// and try to drive transactions whose rows are gone.
			await this.ctx.storage.deleteAlarm();
			await this.ctx.storage.deleteAll();
		});

		// Evict the instance so the next caller gets a fresh one with re-ran migrations. This throws on
		// the caller side with the sentinel message, which FokosDB.destroy() catches and ignores.
		this.ctx.abort(DESTROY_ABORT_SENTINEL);
	}

	async recoverTransaction(transactionId: string): Promise<RecoverTransactionResult> {
		const row = this.ctx.storage.sql
			.exec<{
				idempotency_token: string;
				state: TCState;
			}>(`SELECT idempotency_token, state FROM tc_state WHERE transaction_id = ?`, transactionId)
			.toArray()[0];

		if (!row) return { state: "not_found" };
		if (row.state === "COMMITTED" || row.state === "CANCELLED") return { state: row.state };

		try {
			const coordinatorDoId = this.ctx.id.toString();
			switch (row.state) {
				case "CREATED":
					await this.drivePrepare(transactionId, row.idempotency_token, coordinatorDoId);
					break;
				case "PREPARING":
					await this.runPrepareRecovery(transactionId, row.idempotency_token);
					break;
				case "PREPARED":
				case "COMMITTING":
					await this.runCommit(transactionId, row.idempotency_token);
					break;
				case "CANCELLING":
					await this.runCancel(transactionId, row.idempotency_token);
					break;
			}
		} catch (e) {
			console.error({
				message: "fokos/tc: recoverTransaction failed, scheduling alarm",
				transactionId,
				error: String(e),
			});
			if (!(await this.ctx.storage.getAlarm())) {
				await this.ctx.storage.setAlarm(Date.now());
			}
		}
		return { state: "driving" };
	}

	private loadStateRow(transactionId: string): TcStateRow | undefined {
		return this.ctx.storage.sql
			.exec<TcStateRow>(
				`SELECT transaction_id, idempotency_token, state, transaction_ts, created_at, completed_at, results_json, operations_hash
                 FROM tc_state WHERE transaction_id = ?`,
				transactionId,
			)
			.toArray()[0];
	}

	private loadStateRowByToken(idempotencyToken: string): TcStateRow | undefined {
		return this.ctx.storage.sql
			.exec<TcStateRow>(
				`SELECT transaction_id, idempotency_token, state, transaction_ts, created_at, completed_at, results_json, operations_hash
                 FROM tc_state WHERE idempotency_token = ?`,
				idempotencyToken,
			)
			.toArray()[0];
	}

	private loadItems(transactionId: string): TcItemRow[] {
		return this.ctx.storage.sql
			.exec<TcItemRow>(
				`SELECT transaction_id, hk, sk, op_index, operation, data, data_kind, ttl_epoch_utc_seconds, conditions_json, update_json, partition_do_name, return_values_on_condition_check_failure
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

function deserializePartitionContext(json: string): PartitionContextResolved {
	return JSON.parse(json) as PartitionContextResolved;
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
		condition: row.conditions_json ? JSON.parse(row.conditions_json) : undefined,
		update: row.update_json ? JSON.parse(row.update_json) : undefined,
		returnValuesOnConditionCheckFailure: row.return_values_on_condition_check_failure === 1 ? "all_old" : undefined,
	}));
}
