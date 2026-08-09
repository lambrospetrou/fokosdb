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

// The value half of a read result, shared by the RPC and public variants. Only `data` differs
// between them (JSON text on the wire, parsed JsonValue in public), so it is the one type parameter.
type ReadForTransactionItemValueOf<D> =
	| { found: true; data: D; kind: DataKind; version: number; ttlEpochUTCSeconds?: number }
	| { found: false };

/**
 * RPC result (participant→TC). Keys are canonical KeyBytes and sortKey is always present (the empty
 * KeyBytes [] is the absent sentinel), matching the request side: the TC is an INTERNAL hop, and per
 * the KeyCodec contract ("encode at entry, decode at exit, compare bytes in between") it compares
 * bytes and never decodes. `db.ts` decodes at the public exit.
 *
 * `lastCommittedTs` / `hasPendingWrite` are TC-only 2PC bookkeeping and are stripped by `db.ts`.
 * `lastCommittedTs` is NOT the conflict datum on its own — it is wall-clock milliseconds, so two
 * writes inside one millisecond are indistinguishable. The item's `version` (`v`) is the monotonic
 * per-item counter that decides a conflict; the timestamp is a second signal that catches a
 * delete+recreate landing back on the same version.
 *
 * json data is JSON text here, and the type is free of the recursive JsonValue so the Workers-RPC type
 * machinery does not instantiate infinitely deep.
 */
export type ReadForTransactionItemResultEncoded = ReadForTransactionItemValueOf<string | Uint8Array> & {
	hashKey: KeyBytes;
	sortKey: KeyBytes;
	lastCommittedTs: TransactionTimestamp;
	hasPendingWrite: boolean;
};

/**
 * Public variant surfaced by FokosDB.transactGetItems: `db.ts` has decoded the keys (the empty
 * sentinel maps back to an absent sortKey), parsed json text into a JsonValue, and dropped the
 * 2PC internals.
 */
export type ReadForTransactionItemResult = ReadForTransactionItemValueOf<string | Uint8Array | JsonValue> & {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;
};

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

/**
 * RPC result (TC→db.ts). Item keys stay canonical KeyBytes (sortKey [] = absent), matching the read
 * path and the `tc_items` BLOB columns they are read from: the TC is an INTERNAL hop, so it never
 * decodes. `db.ts` decodes at the public exit.
 *
 * `RejectionReason` is the deliberate exception — its keys are already in public form because the
 * reason is PERSISTED in `tc_state.rejection_reason_json` and replayed verbatim on an idempotent
 * retry. Storing bytes there would need the same `$u8` JSON tagging plus a decode on every replay.
 */
export type InitiateWriteResponseEncoded =
	| {
			outcome: "committed";
			transactionId: TransactionId;
			idempotencyToken: IdempotencyToken;
			items: Array<{ hashKey: KeyBytes; sortKey: KeyBytes }>;
	  }
	| {
			outcome: "cancelled";
			transactionId: TransactionId;
			idempotencyToken: IdempotencyToken;
			reason: RejectionReason;
	  };

/** Public variant surfaced by FokosDB.transactWriteItems: db.ts has decoded the item keys. */
export type InitiateWriteResponse =
	| {
			outcome: "committed";
			transactionId: TransactionId;
			idempotencyToken: IdempotencyToken;
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

/**
 * On "committed", `items` is positionally matched to the request: `items[i]` answers
 * `request.items[i]`, one entry per requested key, duplicates included.
 */
export type InitiateReadResponseEncoded =
	| { outcome: "committed"; items: ReadForTransactionItemResultEncoded[] }
	| { outcome: "aborted"; reason: "read_conflict" | "pending_write" | "transient_error" };

// Public variant surfaced by FokosDB.transactGetItems: json items decoded to JsonValue at the db.ts
// boundary. Same positional guarantee as InitiateReadResponseEncoded.
export type InitiateReadResponse =
	| { outcome: "committed"; items: ReadForTransactionItemResult[] }
	| { outcome: "aborted"; reason: "read_conflict" | "pending_write" | "transient_error" };
