import type { PartitionContextResolved } from "./partition-topology/partition-context.js";
import type { KeyBytes } from "./partition-topology/key-codec.js";
import type { DataKind, JsonValue } from "./types.js";

// ─── Shared primitives ────────────────────────────────────────────────────────

/** Internal per-attempt identifier. Always a UUID, never reused across retries. */
export type TransactionId = string;

/** External idempotency key. = clientRequestToken when provided, else transactionId. */
export type IdempotencyToken = string;

export type TransactionTimestamp = number; // Date.now() ms

// ─── PartitionDO — Prepare ────────────────────────────────────────────────────

export type TransactionOperationType = "put" | "delete" | "check";

// Wire-IN type (db.ts/TC → PartitionDO): keys are canonical KeyBytes (encoded at the db.ts entry).
// sortKey is always present — the empty KeyBytes ([]) is the absent sentinel.
export type TransactionItem = {
	hashKey: KeyBytes;
	sortKey: KeyBytes;
	operation: TransactionOperationType;
	/** Required for "put". Already encoded (JSON stringified at the db.ts boundary), so string | Uint8Array. */
	data?: Uint8Array | string;
	/** Data kind discriminant; present for "put" (json ⇒ data is JSON text). */
	kind?: DataKind;
	/** Optional for all operation types. */
	conditions?: import("./types.js").ItemCondition[];
};

export type PrepareRequest = {
	transactionId: TransactionId;
	/** DO name of the TC. Stored in pending_transactions so the recovery alarm can call it. */
	coordinatorDoId: string;
	transactionTimestamp: TransactionTimestamp;
	/** All items in this partition that the transaction touches. */
	items: TransactionItem[];
};

// Result/OUT type: keys are decoded to the public form (string for UTF-8, Uint8Array for binary) by
// the producing participant, so rejections are user-readable and JSON-serializable for the TC.
export type RejectionReason =
	| { type: "condition_failed"; hashKey: string | Uint8Array; sortKey?: string | Uint8Array }
	| { type: "timestamp_conflict"; hashKey: string | Uint8Array; sortKey?: string | Uint8Array }
	| {
			type: "pending_conflict";
			hashKey: string | Uint8Array;
			sortKey?: string | Uint8Array;
			conflictingTransactionId: TransactionId;
	  }
	| { type: "clock_skew"; serverTimestampMs: number; transactionTimestampMs: number }
	| { type: "transient_error" };

export type PrepareResponse = { outcome: "accepted" } | { outcome: "rejected"; reason: RejectionReason };

// ─── PartitionDO — Commit ─────────────────────────────────────────────────────

export type CommitRequest = {
	transactionId: TransactionId;
	transactionTimestamp: TransactionTimestamp;
	/** Items to apply. Same items as those accepted in prepare. */
	items: TransactionItem[];
};

export type CommitResponse = { outcome: "committed" };

// ─── PartitionDO — Cancel ─────────────────────────────────────────────────────

export type CancelRequest = {
	transactionId: TransactionId;
};

export type CancelResponse = { outcome: "cancelled" };

// ─── PartitionDO — ReadForTransaction ─────────────────────────────────────────

export type ReadForTransactionRequest = {
	transactionId: TransactionId;
	items: Array<{ hashKey: KeyBytes; sortKey: KeyBytes }>;
};

// Result/OUT type: keys decoded to the public form by the producing participant.
type ReadForTransactionItemResultOf<D> =
	| {
			found: true;
			hashKey: string | Uint8Array;
			sortKey?: string | Uint8Array;
			data: D;
			kind: DataKind;
			lastCommittedTs: TransactionTimestamp;
			hasPendingWrite: boolean;
	  }
	| {
			found: false;
			hashKey: string | Uint8Array;
			sortKey?: string | Uint8Array;
			lastCommittedTs: TransactionTimestamp;
			hasPendingWrite: boolean;
	  };

// RPC result (participant→TC→db.ts): json is JSON text. Free of the recursive JsonValue so the
// Workers-RPC type machinery does not instantiate infinitely deep.
export type ReadForTransactionItemResultEncoded = ReadForTransactionItemResultOf<string | Uint8Array>;

// Public variant surfaced by FokosDB.transactGetItems: db.ts has parsed json text into a JsonValue.
export type ReadForTransactionItemResult = ReadForTransactionItemResultOf<string | Uint8Array | JsonValue>;

export type ReadForTransactionResponse = {
	items: ReadForTransactionItemResultEncoded[];
};

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
	hashKey: KeyBytes;
	sortKey: KeyBytes;
	operation: TransactionOperationType;
	/** Encoded at the db.ts boundary (json ⇒ JSON text). */
	data?: Uint8Array | string;
	kind?: DataKind;
	conditions?: import("./types.js").ItemCondition[];
	/** Resolved partition context for the PartitionDO that owns this key. */
	partitionContext: PartitionContextResolved;
};

export type InitiateWriteRequest = {
	/** When provided, used as idempotencyToken and TC DO name for deduplication. */
	clientRequestToken?: string;
	operations: TCWriteOperation[];
};

export type InitiateWriteResponse =
	| {
			outcome: "committed";
			transactionId: TransactionId;
			idempotencyToken: IdempotencyToken;
			// Decoded to the public form by the TC when assembling the final response.
			items: Array<{ hashKey: string | Uint8Array; sortKey?: string | Uint8Array }>;
	  }
	| {
			outcome: "cancelled";
			transactionId: TransactionId;
			idempotencyToken: IdempotencyToken;
			reason: RejectionReason;
	  };

// Wire-IN type (db.ts → TC): keys are canonical KeyBytes (sortKey [] = absent).
export type TCReadItem = {
	hashKey: KeyBytes;
	sortKey: KeyBytes;
	/** Resolved partition context for the PartitionDO that owns this key. */
	partitionContext: PartitionContextResolved;
};

export type InitiateReadRequest = {
	items: TCReadItem[];
};

export type InitiateReadResponseEncoded =
	| { outcome: "committed"; items: ReadForTransactionItemResultEncoded[] }
	| { outcome: "aborted"; reason: "read_conflict" | "pending_write" | "transient_error" };

// Public variant surfaced by FokosDB.transactGetItems: json items decoded to JsonValue at the db.ts boundary.
export type InitiateReadResponse =
	| { outcome: "committed"; items: ReadForTransactionItemResult[] }
	| { outcome: "aborted"; reason: "read_conflict" | "pending_write" | "transient_error" };
