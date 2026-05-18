import type { PartitionContextResolved } from "./partition-topology/partition-topology.js";

// ─── Shared primitives ────────────────────────────────────────────────────────

/** Internal per-attempt identifier. Always a UUID, never reused across retries. */
export type TransactionId = string;

/** External idempotency key. = clientRequestToken when provided, else transactionId. */
export type IdempotencyToken = string;

export type TransactionTimestamp = number; // Date.now() ms

// ─── PartitionDO — Prepare ────────────────────────────────────────────────────

export type TransactionOperationType = "put" | "delete" | "check";

export type TransactionItem = {
	hashKey: string;
	sortKey?: string;
	operation: TransactionOperationType;
	/** Required for "put". */
	data?: Uint8Array | string;
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

export type RejectionReason =
	| { type: "condition_failed"; hashKey: string; sortKey?: string }
	| { type: "timestamp_conflict"; hashKey: string; sortKey?: string }
	| { type: "pending_conflict"; hashKey: string; sortKey?: string; conflictingTransactionId: TransactionId }
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
	items: Array<{ hashKey: string; sortKey?: string }>;
};

export type ReadForTransactionItemResult =
	| {
			found: true;
			hashKey: string;
			sortKey?: string;
			data: Uint8Array | string;
			lastCommittedTs: TransactionTimestamp;
			hasPendingWrite: boolean;
	  }
	| { found: false; hashKey: string; sortKey?: string; lastCommittedTs: TransactionTimestamp; hasPendingWrite: boolean };

export type ReadForTransactionResponse = {
	items: ReadForTransactionItemResult[];
};

// ─── TC State Machine ─────────────────────────────────────────────────────────

export type TCState = "CREATED" | "PREPARING" | "PREPARED" | "COMMITTING" | "COMMITTED" | "CANCELLING" | "CANCELLED";

// ─── TC RPC (called by Client Worker / FokosDB) ───────────────────────────────

export type TCWriteOperation = {
	hashKey: string;
	sortKey?: string;
	operation: TransactionOperationType;
	data?: Uint8Array | string;
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
	| { outcome: "committed"; transactionId: TransactionId; idempotencyToken: IdempotencyToken }
	| { outcome: "cancelled"; transactionId: TransactionId; idempotencyToken: IdempotencyToken; reason: RejectionReason };

export type TCReadItem = {
	hashKey: string;
	sortKey?: string;
	/** Resolved partition context for the PartitionDO that owns this key. */
	partitionContext: PartitionContextResolved;
};

export type InitiateReadRequest = {
	items: TCReadItem[];
};

export type InitiateReadResponse =
	| { outcome: "committed"; items: ReadForTransactionItemResult[] }
	| { outcome: "aborted"; reason: "read_conflict" | "pending_write" | "transient_error" };
