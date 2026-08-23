import type { PartitionContextResolved } from "./partition-topology/partition-context.js";
import type { KeyBytes } from "./partition-topology/key-codec.js";
import type { DataKind, ItemCondition, ItemKey, JsonComposite, JsonValue } from "./types.js";

// ─── Shared primitives ────────────────────────────────────────────────────────

/** Internal per-attempt identifier. Always a UUID, never reused across retries. */
export type TransactionId = string;

/** External idempotency key. = clientRequestToken when provided, else transactionId. */
export type IdempotencyToken = string;

export type TransactionTimestamp = number; // Date.now() ms

// ─── PartitionDO — Prepare ────────────────────────────────────────────────────

export type TransactionOperationType = "put" | "delete" | "check";

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
	operation: TransactionOperationType;
	/** Required for "put". Already encoded (JSON stringified at the db.ts boundary), so string | Uint8Array. */
	data?: Uint8Array | string;
	/** Data kind discriminant; present for "put" (json ⇒ data is JSON text). */
	kind?: DataKind;
	/** Optional for all operation types. */
	conditions?: ItemCondition[];
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

// ─── PartitionDO — ReadForTransaction ─────────────────────────────────────────

export type ReadForTransactionRequest = {
	transactionId: TransactionId;
	items: TransactionItemKey[];
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
 * Only two rejection reasons can reach a caller here: `condition_failed` and `pending_conflict`
 * against a two-phase transaction that holds a lock. `timestamp_conflict` and `clock_skew` order a
 * transaction against writes that interleave between its prepare and its commit, and this path has
 * no such window — one DO validates and applies the whole set serially inside one storage
 * transaction, so serializability comes from the execution order.
 */
export type SingleShotResponse = { outcome: "committed" } | { outcome: "rejected"; reason: RejectionReason };

// ─── PartitionDO — ReadSnapshot (single-partition fast path) ─────────────────

/**
 * A whole `transactGetItems` handed to ONE partition. It carries no transaction id: nothing is
 * locked, nothing is persisted, and there is no second phase to correlate with.
 */
export type ReadSnapshotRequest = {
	items: TransactionItemKey[];
};

/**
 * `items` is positionally matched to the request, one entry per requested key, duplicates included.
 *
 * There is no `read_conflict`: a partition DO is single-threaded and reads the whole set with no
 * `await` in between, so the result already IS a consistent snapshot and no second phase can
 * disagree with the first. `pending_write` stays, so a lock held by an in-progress two-phase
 * transaction aborts the read exactly as it does on the coordinator path.
 */
export type ReadSnapshotResponse =
	| { outcome: "committed"; items: ReadForTransactionItemResultEncoded[] }
	| { outcome: "aborted"; reason: "pending_write" };

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

// ─── Public API (FokosDB entry shapes) ────────────────────────────────────────

/**
 * One operation of `FokosDB.transactWriteItems`, as the caller writes it: public keys, unencoded
 * `data`. `validateTransactWriteOperations` enforces the same rules at runtime, for the HTTP surface.
 */
export type TransactWriteItem =
	| {
			operation: "put";
			hashKey: string | Uint8Array;
			sortKey?: string | Uint8Array;
			data: string | Uint8Array | JsonComposite;
			conditions?: ItemCondition[];
	  }
	| {
			operation: "delete";
			hashKey: string | Uint8Array;
			sortKey?: string | Uint8Array;
			conditions?: ItemCondition[];
	  }
	| {
			operation: "check";
			hashKey: string | Uint8Array;
			sortKey?: string | Uint8Array;
			/** Required and non-empty: a check with no conditions asserts nothing. */
			conditions: ItemCondition[];
	  };

export type TransactWriteItemsOptions = {
	items: TransactWriteItem[];
	/** Idempotency key. Reusing one for a DIFFERENT item set is rejected. */
	clientRequestToken?: string;
};

export type TransactGetItemsOptions = {
	items: ItemKey[];
};

// ─── TC RPC (called by Client Worker / FokosDB) ───────────────────────────────

// Wire-IN type (db.ts → TC): keys are canonical KeyBytes (sortKey [] = absent).
export type TCWriteOperation = {
	hashKey: KeyBytes;
	sortKey: KeyBytes;
	operation: TransactionOperationType;
	/** Encoded at the db.ts boundary (json ⇒ JSON text). */
	data?: Uint8Array | string;
	kind?: DataKind;
	conditions?: ItemCondition[];
	/** Resolved partition context for the PartitionDO that owns this key. */
	partitionContext: PartitionContextResolved;
};

export type InitiateWriteRequest = {
	/** When provided, used as idempotencyToken and TC DO name for deduplication. */
	clientRequestToken?: string;
	items: TCWriteOperation[];
};

/**
 * Result of TC.initiateWrite, and the public result of FokosDB.transactWriteItems — ONE type, because
 * it carries no keys for `db.ts` to decode. There is deliberately no item array: a write transaction
 * is all-or-nothing, so echoing the keys back tells the caller only what it already sent. DynamoDB's
 * TransactWriteItems answers the same way, returning consumed capacity and nothing item-shaped.
 *
 * `RejectionReason` keys are in public form, unlike every other TC→db.ts value, because the reason is
 * PERSISTED in `tc_state.rejection_reason_json` and replayed verbatim on an idempotent retry. Storing
 * bytes there would need the same `$u8` JSON tagging plus a decode on every replay.
 *
 * FIXME: `cancelled` reports ONE reason for the whole transaction, so a caller with 100 operations
 * cannot tell which one failed unless the reason happens to carry a key (`transient_error` and
 * `clock_skew` carry none). DynamoDB returns `CancellationReasons` — one entry per operation, in
 * REQUEST ORDER, with `Code: "None"` for the operations that were fine — and raises it as a typed
 * `TransactionCanceledException`. We should do both: a positional per-operation reason array, and
 * typed errors that share the `RejectionReason` union with the non-transactional path. That also
 * lets a size-rejected prepare say so instead of reporting `transient_error`.
 */
export type InitiateWriteResponse =
	| {
			outcome: "committed";
			transactionId: TransactionId;
			idempotencyToken: IdempotencyToken;
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
