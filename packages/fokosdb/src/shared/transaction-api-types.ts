/**
 * The transaction shapes a caller of FokosDB writes and receives. Nothing here describes the wire:
 * the 2PC requests, responses and driver items live in `transaction-wire-types.ts`, which imports
 * this module and is never imported back.
 */
import type { FokosErrorCode } from "./errors-operations.js";
import type { ConditionExpression, ProjectionExpression, UpdateExpression } from "./expression/types.js";
import type {
	ConditionCheckImage,
	HashKey,
	ItemDeleter,
	ItemGetter,
	ItemKey,
	ItemPutter,
	ItemQuerier,
	JsonComposite,
	ReadItem,
	ReturnValuesOnConditionCheckFailure,
	SortKey,
} from "./types.js";

// ─── Transaction identity ─────────────────────────────────────────────────────

/** Internal per-attempt identifier. Always a UUID, never reused across retries. */
export type TransactionId = string;

/** External idempotency key. = clientRequestToken when provided, else transactionId. */
export type IdempotencyToken = string;

// ─── Rejection reasons ────────────────────────────────────────────────────────

// Result/OUT type: keys are decoded to the public form (string for UTF-8, Uint8Array for binary) by
// the producing participant, so rejections are user-readable and JSON-serializable for the TC.
export type RejectionReasonOf<I = ConditionCheckImage> =
	| { code: "condition_failed"; hashKey: HashKey; sortKey?: SortKey; item?: I }
	| { code: "timestamp_conflict"; hashKey: HashKey; sortKey?: SortKey }
	| {
			code: "pending_conflict";
			hashKey: HashKey;
			sortKey?: SortKey;
			conflictingTransactionId: TransactionId;
	  }
	/** The transaction timestamp is too far ahead of the clock of the partition that owns the operation. */
	| {
			code: "clock_skew";
			hashKey: HashKey;
			sortKey?: SortKey;
			/** Both timestamps are in the transaction order unit: the partition wall clock and the transaction timestamp. */
			serverTimestampMicros: number;
			transactionTimestampMicros: number;
	  }
	| { code: "update_not_applicable"; hashKey: HashKey; sortKey?: SortKey }
	/**
	 * A `set` value evaluated to bytes for this item, and a JSON document cannot hold bytes. It is one
	 * cause of an inapplicable update, reported on its own because the caller can act on it: a key
	 * reference is a valid update value for a text key and not for a binary one, and a SQLite function
	 * can return a blob for one item and text for the next.
	 */
	| { code: "update_value_is_bytes"; hashKey: HashKey; sortKey?: SortKey }
	| { code: "item_too_large"; hashKey: HashKey; sortKey?: SortKey }
	/**
	 * The partition that owns the operation could not run it: the error it raised, or no answer at all.
	 * Every operation of that partition carries the same code and the same `error_id`, which names the
	 * error in the logs.
	 */
	| { code: ExecutionFailureCode; hashKey: HashKey; sortKey?: SortKey; error_id: string };

/** The codes of the rejections that say a premise of an operation did not hold. */
export type PremiseRejectionCode =
	| "condition_failed"
	| "timestamp_conflict"
	| "pending_conflict"
	| "clock_skew"
	| "update_not_applicable"
	| "update_value_is_bytes"
	| "item_too_large";

/** The codes of the errors that stop the partition of an operation from running it. */
export type ExecutionFailureCode = Exclude<FokosErrorCode, PremiseRejectionCode>;

export type RejectionReason = RejectionReasonOf<ConditionCheckImage>;

/** The reason `putItem` and `deleteItem` carry when their condition fails. */
export type ConditionFailedReason = Extract<RejectionReason, { code: "condition_failed" }>;

/**
 * What happened to one operation of a cancelled `transactWriteItems`, positional to the request:
 * `results[i]` answers the operation sent at index `i`.
 *
 * `passed` means the operation was acceptable, `not_evaluated` that no participant judged it, and
 * `rejected` that one failed it — the reason says why, and carries the old item image when the operation
 * asked for one and the item exists.
 *
 * `itemOmitted` says the image did not fit in some answer on the way back, NOT that every later
 * image is absent: every node caps the image bytes it sends over the operations it owns, so an
 * entry marked `itemOmitted` can be followed by one that carries an image, and the same operation
 * set can return a different image set when the operations spread differently over partitions. The
 * outcome codes never depend on that spread.
 */
export type TransactWriteOperationResult =
	| { outcome: "passed" }
	| { outcome: "not_evaluated" }
	| {
			outcome: "rejected";
			reason: RejectionReason;
			itemOmitted?: "response_too_large";
	  };

// ─── transactWriteItems ───────────────────────────────────────────────────────

/**
 * One operation of `FokosDB.transactWriteItems`, as the caller writes it: public keys, unencoded
 * `data`. `validateTransactWriteOperations` enforces the same rules at runtime, for the HTTP surface.
 */
export type TransactWriteItem =
	| {
			operation: "put";
			hashKey: HashKey;
			sortKey?: SortKey;
			data: string | Uint8Array | JsonComposite;
			/** Epoch UTC seconds. Reads can return the item after this instant until background deletion. */
			ttlAt?: number;
			condition?: ConditionExpression;
			returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;
	  }
	| {
			operation: "delete";
			hashKey: HashKey;
			sortKey?: SortKey;
			condition?: ConditionExpression;
			returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;
	  }
	| {
			operation: "check";
			hashKey: HashKey;
			sortKey?: SortKey;
			/** A check must have one condition because it does not write. */
			condition: ConditionExpression;
			returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;
	  }
	| {
			/**
			 * Applies the actions to the item's JSON `data`. Creates the item when it is absent, with the
			 * empty document as the pre-image: add a condition, such as `exists` over `hashKey`, when the
			 * operation must instead fail on an absent item.
			 */
			operation: "update";
			hashKey: HashKey;
			sortKey?: SortKey;
			update: UpdateExpression;
			ttlAt?: number;
			condition?: ConditionExpression;
			returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;
	  };

export type TransactWriteItemsOptions = {
	items: TransactWriteItem[];
	/**
	 * Idempotency key, 1 to 64 UTF-8 bytes. A completed outcome is retained for 10 minutes. Reusing the
	 * token for a different item set during that window is rejected.
	 */
	clientRequestToken?: string;
};

/**
 * The public result of FokosDB.transactWriteItems. It carries no outcome: this value exists only when
 * the transaction committed, because a cancelled one raises `FokosTransactionCancelledError` instead.
 */
export type TransactWriteItemsResult = {
	transactionId: TransactionId;
	idempotencyToken: IdempotencyToken;
};

// ─── transactGetItems ─────────────────────────────────────────────────────────

/** One requested key, with the projection that item asks for. Each item chooses its own. */
export type TransactGetItemKey = ItemKey & { projection?: readonly ProjectionExpression[] };

/**
 * `Ts` names the type of each item, by position: `transactGetItems<[Profile, Stats]>` says item 0 holds
 * a Profile and item 1 a Stats. One request reads unrelated items, so one type for the whole call would
 * describe none of them. The tuple also fixes the item count, and an array type such as `Profile[]`
 * gives one type to every position of a request of any length.
 *
 * A call that names no `Ts` keeps the widest types, so `NoInfer` guards the options: it keeps `Ts` off
 * the inference path, where the keys alone would otherwise infer it as `unknown` per position.
 */
export type TransactGetItemsOptions<Ts extends readonly unknown[] = never[]> = {
	items: { [K in keyof Ts]: TransactGetItemKey };
};

/**
 * One answer of a read: the item, or the keys that found nothing. Public variant surfaced by
 * FokosDB.transactGetItems, where `db.ts` has decoded the keys (the empty sentinel maps back to an
 * absent sortKey), parsed json text into a JsonValue, and dropped the 2PC internals. A found item is
 * the ordinary `ReadItem` envelope, so a projected item and a complete item have one shape and `T`
 * types both, exactly as `getItem` returns them.
 */
export type MaybeReadItem<T = never> = ({ found: true } & ReadItem<T>) | ({ found: false } & ItemKey);

// Public result surfaced by FokosDB.transactGetItems: json items decoded to JsonValue at the db.ts
// boundary. Same positional guarantee as the wire response, and no outcome for the same reason the
// write result carries none — a read that cannot answer raises a FokosError instead, for example
// `FokosConflictError` with `read_conflict` or `pending_write`.
export type TransactGetItemsResult<Ts extends readonly unknown[] = never[]> = {
	items: { [K in keyof Ts]: MaybeReadItem<Ts[K]> };
};

// ─── The FokosDB contract ─────────────────────────────────────────────────────

export interface ItemTransactor {
	transactWriteItems(opts: TransactWriteItemsOptions): Promise<TransactWriteItemsResult>;
	transactGetItems<Ts extends readonly unknown[] = never[]>(
		opts: NoInfer<TransactGetItemsOptions<Ts>>,
	): Promise<TransactGetItemsResult<Ts>>;
}

// The whole contract composes the single-item halves in `types.ts` with the transaction half above,
// so it lives here: `types.ts` holds the item primitives that this module builds on and must not
// import back from it.
export interface FokosDBAPI extends ItemPutter, ItemGetter, ItemDeleter, ItemQuerier, ItemTransactor {}
