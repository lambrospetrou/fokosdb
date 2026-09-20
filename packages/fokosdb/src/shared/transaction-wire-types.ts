/**
 * The 2PC wire: what `db.ts`, the TransactionCoordinatorDO and the PartitionDO participants send each
 * other. None of it reaches a caller of FokosDB — the caller-facing shapes are in
 * `transaction-api-types.ts`, which this module builds on and never imports back from.
 */
import type { CompiledConditionPlan, CompiledProjectionPlan, CompiledUpdatePlan } from "./expression/plan.js";
import type { ProjectedWireRow } from "./expression/projection.js";
import type { KeyBytes } from "../sharding/key-codec.js";
import type { FokosDbRouteContext } from "./partition-context.js";
import type { IdempotencyToken, RejectionReasonOf, TransactionId } from "./transaction-api-types.js";
import type { ConditionCheckImageEncoded, DataKind, ReturnValuesOnConditionCheckFailure } from "./types.js";

// ─── Shared primitives ────────────────────────────────────────────────────────

// Transaction order timestamp: Date.now() * TX_ORDER_TS_UNITS_PER_MS (see txOrderTimestampNow in shared/transaction-limits.ts).
export type TransactionTimestamp = number;

// ─── PartitionDO — Prepare ────────────────────────────────────────────────────

export type TransactionOperationType = "put" | "delete" | "check" | "update";

/**
 * The key half of a wire-IN item (db.ts/TC → PartitionDO): canonical KeyBytes, with sortKey always
 * present — the empty KeyBytes ([]) is the absent sentinel. This is all the routing needs, so the
 * operations that only have to REACH the owning partition (cancel, read) carry this and nothing else.
 */
export type TransactionItemKey = {
	hashKey: KeyBytes;
	sortKey: KeyBytes;
};

// Wire-IN type (db.ts/TC → PartitionDO): keys are canonical KeyBytes (encoded at the db.ts entry).
// sortKey is always present — the empty KeyBytes ([]) is the absent sentinel.
export type TransactionItem = TransactionItemKey & {
	/**
	 * Position of this operation in the caller's request. Every result carries it back, so a node
	 * that owns a subset of the operations still merges by request order and never by arrival order.
	 */
	opIndex: number;
	operation: TransactionOperationType;
	/** Required for "put". Already encoded (JSON stringified at the db.ts boundary), so string | Uint8Array. */
	data?: Uint8Array | string;
	/** Data kind discriminant; present for "put" (json ⇒ data is JSON text). */
	kind?: DataKind;
	/** Epoch UTC seconds. Present only for a put that has an expiry instant. */
	ttlAt?: number;
	/** Optional for put and delete; required for check. */
	condition?: CompiledConditionPlan;
	/** Compiled update plan; present for "update". */
	update?: CompiledUpdatePlan;
	returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;
};

export type PrepareRequest = {
	transactionId: TransactionId;
	/** DO name of the TC. Stored in pending_transactions so the recovery alarm can call it. */
	coordinatorDoId: string;
	transactionTimestamp: TransactionTimestamp;
	/** All items in this partition that the transaction touches. */
	items: TransactionItem[];
};

export type RejectionReasonEncoded = RejectionReasonOf<ConditionCheckImageEncoded>;

/** Wire variant. `imageBytes` is coordinator bookkeeping, which db.ts strips. */
export type TransactWriteOperationResultEncoded =
	| { outcome: "passed" }
	| { outcome: "not_evaluated" }
	| {
			outcome: "rejected";
			reason: RejectionReasonEncoded;
			imageBytes?: number;
			itemOmitted?: "response_too_large";
	  };

/** What a participant answers: the same result, labelled with the index the request gave it. */
export type ParticipantOperationResultEncoded = TransactWriteOperationResultEncoded & { opIndex: number };

export type PrepareResponse =
	| { outcome: "accepted" }
	| {
			outcome: "rejected";
			/** One result for each operation that the participant answers for. */
			results: ParticipantOperationResultEncoded[];
	  };

// ─── PartitionDO — Commit ─────────────────────────────────────────────────────

export type CommitRequest = {
	transactionId: TransactionId;
	transactionTimestamp: TransactionTimestamp;
	/**
	 * The keys this transaction locked, the same shape `CancelRequest.items` uses. Each participant
	 * applies the payload from its own `pending_transactions` rows, which prepare wrote, so the wire
	 * items carry routing information and nothing else. The keys stay because routing derives the
	 * owning child partition from the key itself.
	 */
	items: TransactionItemKey[];
};

export type CommitResponse = { outcome: "committed" };

// ─── PartitionDO — Cancel ─────────────────────────────────────────────────────

export type CancelRequest = {
	transactionId: TransactionId;
	/**
	 * The keys this transaction locked, used ONLY to route the cancel to the partitions that can hold
	 * a lock — the release itself is by transaction id, so every node the cancel passes through is
	 * cleared whether or not it owns one of these keys.
	 *
	 * An empty list is legal and means "release locally, do not fan out". Correctness does not depend
	 * on this list: a lock is always released eventually by the node holding it, via the stale-tx
	 * recovery alarm. The keys only make that happen in milliseconds instead of STALE_TX_MS, which
	 * matters because a held lock makes non-transactional writes to that key throw.
	 */
	items: TransactionItemKey[];
};

export type CancelResponse = { outcome: "cancelled" };

export type DebugForceResolveTransactionRequest = {
	transactionId: TransactionId;
	outcome: "commit" | "cancel";
};

export type DebugForceResolveTransactionResponse = CommitResponse | CancelResponse;

// ─── PartitionDO — ReadForTransaction ─────────────────────────────────────────

/** A read item on the wire: the canonical keys plus the item's compiled projection plan, when it has one. */
export type TransactionReadItem = TransactionItemKey & { projection?: CompiledProjectionPlan };

export type ReadForTransactionRequest = {
	transactionId: TransactionId;
	items: TransactionReadItem[];
};

/**
 * RPC result (participant→Worker read driver). Keys are canonical KeyBytes and sortKey is always
 * present (the empty KeyBytes [] is the absent sentinel), matching the request side. Per the KeyCodec
 * contract ("encode at entry, decode at exit, compare bytes in between"), the driver compares bytes
 * and never decodes. `db.ts` decodes at the public exit.
 *
 * `deleteRevision` / `hasPendingWrite` are read-driver bookkeeping and are stripped by `db.ts`.
 * `version` detects every write to a live row, because `v = v + 1` runs on every upsert.
 * `deleteRevision` is the owner partition's user-delete counter; it detects a delete and recreate
 * that returns `v` to its first value, and an absent-create-delete sequence.
 *
 * json data is JSON text here, and the type is free of the recursive JsonValue so the Workers-RPC type
 * machinery does not instantiate infinitely deep.
 *
 * The projected result is its own member. The wire carries the positional row and never a record,
 * because only the client holds the resolved names of the projection. `kind` is
 * `"projected"`, which is a read-result tag and never a `DataKind`: that array indexes the on-disk
 * `data_kind` code. `version` stays on the wire, because `sameCommittedState` in `db.ts` compares it
 * for each found item.
 */
export type ReadForTransactionItemResultEncoded = {
	hashKey: KeyBytes;
	sortKey: KeyBytes;
	deleteRevision: number;
	hasPendingWrite: boolean;
} & (
	| {
			found: true;
			data: string | Uint8Array;
			kind: DataKind;
			version: number;
			/** Epoch UTC seconds. The item can remain visible after this instant until background deletion. */
			ttlAt?: number;
	  }
	| { found: true; projected: ProjectedWireRow; kind: "projected"; version: number; ttlAt?: number }
	| { found: false }
);

export type ReadForTransactionResponse = {
	items: ReadForTransactionItemResultEncoded[];
};

// ─── PartitionDO — SingleShot (single-partition fast path) ───────────────────

/**
 * A whole `transactWriteItems` handed to ONE partition, which validates and applies it inside one
 * storage transaction.
 *
 * It carries no transaction id and no timestamp. Nothing is locked and nothing outlives the call, so
 * there is no outcome for a recovery job to resolve later; and with no coordinator in the protocol
 * there is no second clock, so the partition stamps the write with its own — exactly as a
 * non-transactional put does.
 */
export type SingleShotRequest = {
	items: TransactionItem[];
};

/**
 * Every rejection this path returns comes from its check pass, so the reasons are those of
 * `TransactionParticipant.#precheckWrite` plus the pending-lock and condition tests that run before
 * it. Listing them here would drift; what holds instead is which reasons CANNOT appear:
 * `timestamp_conflict` and `clock_skew` order a transaction against writes that interleave between
 * its prepare and its commit, and this path has no such window — one DO validates and applies the
 * whole set serially inside one storage transaction, so serializability comes from the execution
 * order.
 *
 * `not_applicable` says no single DO owns every item, so no DO touched anything and the caller must
 * run the coordinator path. It is a value, not an error: on a split table it is the ordinary answer to
 * a set that straddles two children, and every forwarding hop passes it up unchanged.
 */
export type SingleShotResponse =
	| { outcome: "committed" }
	| {
			outcome: "rejected";
			/**
			 * One result for each operation of the request. One DO evaluates the whole set, so every
			 * rejection comes from its check pass, which answers for every operation it looked at.
			 */
			results: ParticipantOperationResultEncoded[];
	  }
	| { outcome: "not_applicable" };

// ─── PartitionDO — ReadSnapshot (single-partition fast path) ─────────────────

/**
 * A whole `transactGetItems` handed to ONE partition. It carries no transaction id: nothing is
 * locked, nothing is persisted, and there is no second phase to correlate with.
 */
export type ReadSnapshotRequest = {
	items: TransactionReadItem[];
};

/**
 * `items` is positionally matched to the request, one entry per requested key.
 *
 * There is no `read_conflict`: a partition DO is single-threaded and reads the whole set with no
 * `await` in between, so the result already IS a consistent snapshot and no second phase can
 * disagree with the first. `pending_write` stays, so a lock held by an in-progress two-phase
 * transaction aborts the read exactly as it does on the two-phase path.
 *
 * `not_applicable` has the meaning it has on `SingleShotResponse`: no single DO owns every key, nothing
 * was read, and the caller runs the two-phase path.
 */
export type ReadSnapshotResponse =
	| { outcome: "committed"; items: ReadForTransactionItemResultEncoded[] }
	| { outcome: "aborted"; reason: "pending_write" }
	| { outcome: "not_applicable" };

// ─── TC State Machine ─────────────────────────────────────────────────────────

export type TCState = "CREATED" | "PREPARING" | "PREPARED" | "COMMITTING" | "COMMITTED" | "CANCELLING" | "CANCELLED";

// ─── TransactionCoordinatorDO — recoverTransaction ───────────────────────────

export type TCTerminalState = Extract<TCState, "COMMITTED" | "CANCELLED">;

export type RecoverTransactionResult =
	| { state: TCTerminalState }
	/** TC has no record of this transaction — caller should treat it as cancelled. */
	| { state: "not_found" }
	/** TC found a non-terminal state and has taken over recovery. */
	| { state: "driving" };

// ─── TC RPC (called by Client Worker / FokosDB) ───────────────────────────────

// Wire-IN type (db.ts → TC): keys are canonical KeyBytes (sortKey [] = absent).
export type TCWriteOperation = {
	/** Position of this operation in the caller's request; see TransactionItem.opIndex. */
	opIndex: number;
	hashKey: KeyBytes;
	sortKey: KeyBytes;
	operation: TransactionOperationType;
	/** Encoded at the db.ts boundary (json ⇒ JSON text). */
	data?: Uint8Array | string;
	kind?: DataKind;
	ttlAt?: number;
	condition?: CompiledConditionPlan;
	update?: CompiledUpdatePlan;
	returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;
	/** Resolved partition context for the PartitionDO that owns this key. */
	partitionContext: FokosDbRouteContext;
};

export type InitiateWriteRequest = {
	/** When provided, used as idempotencyToken and TC DO name for deduplication. */
	clientRequestToken?: string;
	items: TCWriteOperation[];
};

export type InitiateWriteResponseEncoded =
	| {
			outcome: "committed";
			transactionId: TransactionId;
			idempotencyToken: IdempotencyToken;
	  }
	| {
			outcome: "cancelled";
			transactionId: TransactionId;
			idempotencyToken: IdempotencyToken;
			/** One result for each operation, in request order. Each rejected entry carries its own reason. */
			results: TransactWriteOperationResultEncoded[];
	  };

// Worker read-driver item: keys are canonical KeyBytes (sortKey [] = absent).
export type TCReadItem = TransactionReadItem & {
	/** Resolved partition context for the PartitionDO that owns this key. */
	partitionContext: FokosDbRouteContext;
};

export type InitiateReadRequest = {
	items: TCReadItem[];
};

/**
 * On "committed", `items` is positionally matched to the request: `items[i]` answers
 * `request.items[i]`, one entry per requested key.
 */
export type InitiateReadResponseEncoded = { outcome: "committed"; items: ReadForTransactionItemResultEncoded[] };
