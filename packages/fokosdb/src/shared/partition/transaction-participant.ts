import {
	COORDINATOR_REF_VERSION,
	type CommitRequest,
	type CommitResponse,
	type CoordinatorRef,
	type ParticipantOperationResultEncoded,
	type PrepareRequest,
	type PrepareResponse,
	type ReadForTransactionItemResultEncoded,
	type ReadForTransactionRequest,
	type ReadForTransactionResponse,
	type RejectionReasonEncoded,
	type SingleShotRequest,
	type SingleShotResponse,
	type TransactionItem,
	type TransactionItemKey,
	type TransactionOperationType,
	type TransactionTimestamp,
} from "../transaction-wire-types.js";
import invariant from "../invariant.js";
import { FokosInternalError, INTERNAL_CODES } from "../errors.js";
import { unexpectedTransactionStateError } from "../errors-operations.js";
import { KeyCodec, KeyPairMap, type KeyBytes } from "../../sharding/key-codec.js";
import type { OpList, PartitionStore, PendingTxInfo, StalePendingTx, StoredItemData } from "./partition-store.js";
import {
	applyImageCap,
	conditionFailedReason,
	decodeItemKeys,
	MAX_ITEM_BYTES,
	nextRecoveryAt,
	txOrderTimestampNow,
	TX_ORDER_TS_UNITS_PER_MS,
	validateVersionReferences,
} from "../transaction-limits.js";

/**
 * Reads the coordinator reference of a lock row. The value comes from storage, and a lock row can
 * outlive the code that wrote it, so this refuses a version it does not know and a reference without
 * a field that a call to the coordinator uses. A reference it cannot read throws
 * `unexpected_transaction_state`: nothing can reach the coordinator of that lock, so the lock stays
 * until an operator resolves it with `debugForceResolveTransaction`.
 */
export function parseCoordinatorRef(json: string, transactionId: string): CoordinatorRef {
	const invalid = (detail: string, attributes: Record<string, unknown> = {}) =>
		unexpectedTransactionStateError(`the coordinator reference of a lock ${detail}`, { transactionId, ...attributes });

	let ref: Partial<CoordinatorRef> | null;
	try {
		ref = JSON.parse(json);
	} catch {
		throw invalid("is not JSON");
	}
	if (ref?.v !== COORDINATOR_REF_VERSION) {
		throw invalid("has an unknown version", { v: ref?.v });
	}
	if (!ref.idempotencyToken) {
		throw invalid("has no idempotency token");
	}
	if (!ref.doName) {
		throw invalid("has no coordinator name");
	}
	return ref as CoordinatorRef;
}

/**
 * Wraps an owner check so that it runs one time for each hash key of the rows it tests. On a partition that
 * holds lock rows of a hash key, all sort keys of that hash key have the same owner: a promotion and a
 * hash split move a full hash key, and a range split moves all keys of the source.
 */
export function ownsByHashKey(
	owns: (key: { hashKey: KeyBytes; sortKey: KeyBytes }) => boolean,
): (row: { hk: KeyBytes; sk: KeyBytes }) => boolean {
	const answers = new KeyPairMap<boolean>();
	return (row) => {
		let owned = answers.get(row.hk, NO_SORT_KEY);
		if (owned === undefined) {
			owned = owns({ hashKey: row.hk, sortKey: row.sk });
			answers.set(row.hk, NO_SORT_KEY, owned);
		}
		return owned;
	};
}

const NO_SORT_KEY = KeyCodec.encodeOptional(undefined);

/** True for an operation that writes the data of its item. */
function isWrite(operation: string): boolean {
	return operation === "put" || operation === "update";
}

/** The operations of one item in a request, in `opIndex` order, and the state of their evaluation. */
type ItemSequence = {
	hashKey: KeyBytes;
	sortKey: KeyBytes;
	ops: TransactionItem[];
	/** The last put or update of a sequence of two or more operations. */
	lastWrite: TransactionItem | undefined;
	/** An operation of the item failed. The later operations of the item get `not_evaluated`. */
	failed: boolean;
	/** This transaction held the lock of the item before this prepare. */
	lockedBefore: boolean;
	/** The item stamps before the transaction: undefined until the first operation reads them, null for an absent item. */
	committedStamp: ItemStamp | null | undefined;
	/** The stored data after `lastWrite`, which the evaluate step reads. */
	lastWriteData: StoredItemData | undefined;
};

/** The operations of a request in `opIndex` order. `seqs[i]` is the sequence of the item of `ops[i]`. */
type SequencePlan = {
	ops: TransactionItem[];
	seqs: ItemSequence[];
	sequences: KeyPairMap<ItemSequence>;
	/** The request has an item with two or more operations. */
	repeated: boolean;
};

/**
 * Sorts the operations by `opIndex` and groups them by item with `KeyPairMap`. It also runs the
 * version-reference check, before the first SQL statement, so a refusal writes nothing.
 */
function sequencePlanOf(items: readonly TransactionItem[]): SequencePlan {
	const ops = [...items].sort((a, b) => a.opIndex - b.opIndex);
	const sequences = new KeyPairMap<ItemSequence>();
	const seqs = ops.map((op, i) => {
		invariant(i === 0 || ops[i - 1].opIndex !== op.opIndex, () => `fokos/partition: opIndex ${op.opIndex} occurs two times`);
		let seq = sequences.get(op.hashKey, op.sortKey);
		if (seq) {
			seq.ops.push(op);
		} else {
			seq = {
				hashKey: op.hashKey,
				sortKey: op.sortKey,
				ops: [op],
				lastWrite: undefined,
				failed: false,
				lockedBefore: false,
				committedStamp: undefined,
				lastWriteData: undefined,
			};
			sequences.set(op.hashKey, op.sortKey, seq);
		}
		return seq;
	});
	for (const seq of sequences.values()) {
		seq.lastWrite = seq.ops.length > 1 ? seq.ops.findLast((op) => isWrite(op.operation)) : undefined;
	}
	validateVersionReferences(ops);
	return { ops, seqs, sequences, repeated: sequences.size < ops.length };
}

/** The lock row `operation` of a sequence: its last operation that is not a check, or check. */
function lockOperationOf(ops: readonly TransactionItem[]): TransactionOperationType {
	return ops.findLast((op) => op.operation !== "check")?.operation ?? "check";
}

/** Where a store write of apply takes the data of a put or an update. */
type WriteSource = { from: "request"; item: TransactionItem } | { from: "lock"; row: PendingLock };

type ApplyEntry = TransactionItemKey & { opIndex: number; operation: TransactionOperationType; source: WriteSource };

/**
 * The promotion candidate of the last write of each item. A later delete of the item drops it,
 * because only an item whose final state is present can grow its key.
 */
class ItemGrowth {
	#last = new KeyPairMap<PromotionCandidate | null>();

	record(entry: TransactionItemKey & { operation: string }, keyEstBytes: number | undefined): void {
		if (entry.operation === "delete") {
			this.#last.set(entry.hashKey, entry.sortKey, null);
		} else if (keyEstBytes !== undefined) {
			this.#last.set(entry.hashKey, entry.sortKey, { hashKey: entry.hashKey, keyEstBytes });
		}
	}

	candidates(): PromotionCandidate[] {
		return [...this.#last.values()].filter((c) => c !== null);
	}
}

/** What the evaluate step returns. `results` holds one result for each operation, in `opIndex` order. */
type Evaluation = {
	results: ParticipantOperationResultEncoded[];
	rejected: boolean;
	/** The evaluate step wrote to `items`, so its storage transaction must roll back or be the apply. */
	temporaryWrites: boolean;
	/** The promotion candidates of the temporary writes. Only the single-partition path keeps them. */
	growth: PromotionCandidate[];
};

/**
 * The path that runs the evaluate step. `twoPhase` alone selects the behavior of the path, and no
 * other field does.
 *
 * - `twoPhase: false` is the single-partition path. Its storage transaction keeps the temporary
 *   writes as the writes of the transaction. It has no lock of its own, no timestamp check, and no
 *   lock block.
 * - `twoPhase: true` is the prepare of the two-phase path. A lock of `transactionId` is a lock of
 *   this transaction. The step checks the timestamps, and it copies the last-write data for the
 *   lock block.
 */
type EvaluatePath =
	| { twoPhase: false; txTimestamp: TransactionTimestamp }
	| { twoPhase: true; txTimestamp: TransactionTimestamp; transactionId: string };

/** Thrown inside a storage transaction to roll back its writes, with the evaluation that the block made. */
class EvaluationRollback extends Error {
	constructor(readonly evaluation: Evaluation) {
		super("fokos/partition: the evaluate block rolls back");
	}
}

// A pending check cannot change the item, so a transactional read may serialize on either side of it.
// Allowlist the read-only operations: an operation the code does not know counts as a pending write.
const READ_ONLY_PENDING_OPERATIONS: ReadonlySet<string> = new Set(["check"]);

type ItemStamp = { last_read_ts: number; last_write_ts: number };

/** A read of one item row that also returned the item timestamps. */
type ItemRead = { itemPresent: boolean; lastReadTs: number | null; lastWriteTs: number | null };

/** One lock row of a transaction, as commit reads it. */
type PendingLock = ReturnType<PartitionStore["listPendingTxItems"]>[number];

/**
 * The item timestamps of the row that a condition, an update probe or a put measure read, or
 * undefined when it found no row. Both columns are NOT NULL, so a live row always carries both values.
 */
function itemStampOf(read: ItemRead): ItemStamp | undefined {
	if (!read.itemPresent) {
		return undefined;
	}
	invariant(read.lastReadTs !== null && read.lastWriteTs !== null, "fokos/partition.prepare: a live row has no item timestamps");
	return { last_read_ts: read.lastReadTs, last_write_ts: read.lastWriteTs };
}

export type TransactionParticipantDeps = {
	store: PartitionStore;
	/** Injectable wall clock (epoch milliseconds) for skew/staleness tests; defaults to Date.now. */
	now?: () => number;
	/** A prepare whose timestamp is more than this far ahead of the local clock is rejected (clock_skew). Read at each prepare. */
	maxClockSkewMs: () => number;
	/** How long a lock waits before the stale-transaction job asks its coordinator about it. Read at each use. */
	staleTransactionMs: () => number;
	/**
	 * The transaction order timestamp of a single-shot transaction. Injectable so tests can pin it;
	 * defaults to txOrderTimestampNow.
	 */
	txOrderTimestamp?: () => TransactionTimestamp;
	/**
	 * Returns a check that is true when this partition owns the key now. A lock row of a key that a
	 * promotion moved away is a copy. The new owner resolves it, and no local decision applies to it.
	 * The participant gets a new check for each synchronous block.
	 */
	ownerCheck: () => (key: { hashKey: KeyBytes; sortKey: KeyBytes }) => boolean;
};

/**
 * A key that one applied write grew, with its updated size estimate.
 *
 * The apply runs in one storage transaction, and `queue` opens a transaction of its own. The decision
 * therefore cannot happen where the number is measured. The applying method collects these keys and
 * returns them, and the caller decides after the storage transaction commits. An apply that rolled
 * back or threw returns none, because the array lives only as long as the call.
 */
export type PromotionCandidate = { hashKey: KeyBytes; keyEstBytes: number };

/** What a local apply returns: the protocol answer, and the keys it grew. */
export type CommitLocalResult = { response: CommitResponse; promotionCandidates: PromotionCandidate[] };
export type SingleShotResult = { response: SingleShotResponse; promotionCandidates: PromotionCandidate[] };

/**
 * The 2PC participant: prepare/commit/cancel/read for the items this partition owns locally.
 * Routing fan-out (groupItemsByRouting), child RPCs, alarm scheduling, and stale-tx recovery
 * driving stay in PartitionDO — this class only implements the local protocol semantics over
 * the PartitionStore.
 */
export class TransactionParticipant {
	#store: PartitionStore;
	#now: () => number;
	#maxClockSkewMs: () => number;
	#staleTransactionMs: () => number;
	#txOrderTimestamp: () => TransactionTimestamp;
	#ownerCheck: () => (key: { hashKey: KeyBytes; sortKey: KeyBytes }) => boolean;

	constructor(deps: TransactionParticipantDeps) {
		this.#store = deps.store;
		this.#now = deps.now ?? (() => Date.now());
		this.#maxClockSkewMs = deps.maxClockSkewMs;
		this.#staleTransactionMs = deps.staleTransactionMs;
		this.#txOrderTimestamp = deps.txOrderTimestamp ?? txOrderTimestampNow;
		this.#ownerCheck = deps.ownerCheck;
	}

	/**
	 * The old item image for an operation whose condition just failed, or undefined when the caller
	 * asked for none or the item does not exist. It runs in the same storage transaction as the
	 * condition, with no `await` between them, so it returns the row the condition compared.
	 */
	#imageForFailedCondition(item: TransactionItem, sk: KeyBytes, itemPresent: boolean) {
		if (item.returnValuesOnConditionCheckFailure !== "all_old" || !itemPresent) {
			return undefined;
		}
		return this.#store.getItemImage(item.hashKey, sk).row;
	}

	/**
	 * The applicability and size tests of ONE write item — the single definition both write paths run
	 * in their CHECK pass, before either path writes anything.
	 *
	 * Every test that can reject a write belongs here, because neither path can reject later. The
	 * two-phase path must not fail at commit: prepare has already answered "accepted" and the
	 * coordinator is entitled to commit. The single-shot path must not reject after its apply loop has
	 * started: `transactionSync` rolls back on a throw, not on a returned rejection, so a rejection
	 * from the apply loop would keep the writes the loop had already made.
	 *
	 * The sizes are measured against the same SQL the write stores, so the answer here is exact:
	 * `measureItemBytes` evaluates the put's own data expression, and the update probe evaluates the
	 * document expression that both the pending row and the single-shot UPDATE store verbatim.
	 *
	 * Returns the row read as well, because prepare reuses its item timestamps instead of reading the
	 * row a second time. An update always reads the row with its probe. A put reads the row only when
	 * `readStamp` is true, in the same statement that measures it.
	 */
	#precheckWrite(item: TransactionItem, readStamp: boolean): { reason: RejectionReasonEncoded | null; read: ItemRead | null } {
		const { hashKey: hk, sortKey: sk } = item;
		if (item.operation === "put") {
			// A put always carries both data and kind; assert together so the measure gets a real kind.
			invariant(
				item.data !== undefined && item.kind !== undefined,
				() => `fokos/partition.precheck: "put" item has no data/kind (${KeyCodec.pairForLog(hk, sk)})`,
			);
			const measureOpts = { hk, sk, data: item.data, kind: item.kind };
			const read = readStamp ? this.#store.measureItemBytesWithStamp(measureOpts) : null;
			const bytes = read ? read.estRowBytes : this.#store.measureItemBytes(measureOpts);
			return { reason: bytes > MAX_ITEM_BYTES ? { code: "item_too_large", ...decodeItemKeys(hk, sk) } : null, read };
		}

		if (item.operation === "update") {
			invariant(item.update, "fokos/partition.precheck: update item missing update plan");
			const probe = this.#store.probeUpdate(item.update, hk, sk);
			if (!probe.applicable) {
				// A value that evaluated to bytes is the one cause the probe separates out, because the
				// caller can act on it. Every other cause — a non-json item, a missing target path, a
				// missing operand — is reported as one answer, as DynamoDB reports its own.
				const code = probe.valueTypeOk ? "update_not_applicable" : "update_value_is_bytes";
				return { reason: { code, ...decodeItemKeys(hk, sk) }, read: probe };
			}
			// An applicable update always measured its result; the probe returns NULL only when it is not.
			invariant(probe.newSize !== null, "fokos/partition.precheck: applicable update reported no size");
			return { reason: probe.newSize > MAX_ITEM_BYTES ? { code: "item_too_large", ...decodeItemKeys(hk, sk) } : null, read: probe };
		}

		// delete and check write no data, so neither has a size to test.
		return { reason: null, read: null };
	}

	prepareLocal(request: PrepareRequest): PrepareResponse {
		// A lock is only ever released by the outcome of its transaction, and the values below are the
		// whole thread back to that outcome: the recovery job selects locks by transaction_id and calls
		// the coordinator that the stored name and token address (nothing else in the system can
		// supply them). A lock missing one is therefore unreleasable — it would block every
		// non-transactional write to its key for the life of the partition. Refuse to create it.
		invariant(request.transactionId.length > 0, "fokos/partition.prepare: transactionId is required");
		invariant(request.coordinator?.v === COORDINATOR_REF_VERSION, "fokos/partition.prepare: the coordinator reference version is required");
		invariant(request.coordinator.idempotencyToken, "fokos/partition.prepare: the coordinator idempotencyToken is required");
		invariant(request.coordinator.doName, "fokos/partition.prepare: the coordinator doName is required");
		const coordinatorJson = JSON.stringify(request.coordinator);
		// The version-reference check refuses a request that is not valid, before every other answer.
		const plan = sequencePlanOf(request.items);

		const now = this.#now();

		// The clock of this partition rejects the whole request, so every operation it owns reports it.
		// The transaction order timestamp carries sub-millisecond digits, so the comparison is on physical milliseconds.
		if (Math.floor(request.transactionTimestamp / TX_ORDER_TS_UNITS_PER_MS) > now + this.#maxClockSkewMs()) {
			return {
				outcome: "rejected",
				results: request.items.map((item) => ({
					outcome: "rejected",
					opIndex: item.opIndex,
					reason: {
						code: "clock_skew",
						...decodeItemKeys(item.hashKey, item.sortKey),
						serverTimestampMicros: now * TX_ORDER_TS_UNITS_PER_MS,
						transactionTimestampMicros: request.transactionTimestamp,
					},
				})),
			};
		}

		const lockBlock = () => this.#writeLocks(plan, request, coordinatorJson, now);

		// The evaluate block. When it made a temporary write, it throws, and SQLite rolls back every
		// write of the block. The lock block then runs as a second storage transaction. No `await` runs
		// between the two blocks, so no other request reads `items` between them. When the evaluate block
		// made no temporary write, the locks go into the same block.
		let evaluation: Evaluation;
		try {
			evaluation = this.#store.transactionSync(() => {
				const ev = this.#evaluate(plan, {
					twoPhase: true,
					txTimestamp: request.transactionTimestamp,
					transactionId: request.transactionId,
				});
				if (ev.temporaryWrites) {
					throw new EvaluationRollback(ev);
				}
				if (!ev.rejected) {
					lockBlock();
				}
				return ev;
			});
		} catch (err) {
			if (!(err instanceof EvaluationRollback)) {
				throw err;
			}
			evaluation = err.evaluation;
			if (!evaluation.rejected) {
				this.#store.transactionSync(lockBlock);
			}
		}

		if (evaluation.rejected) {
			applyImageCap(evaluation.results);
			return { outcome: "rejected", results: evaluation.results };
		}
		return { outcome: "accepted" };
	}

	/**
	 * Runs the checks of each operation in `opIndex` order. `path.twoPhase` selects what the path
	 * adds (see `EvaluatePath`).
	 *
	 * In a request with an item of two or more operations, each passed operation is also applied as a
	 * temporary write, so the next operation sees its effect. Every item gets temporary writes, also an
	 * item with one operation, so the state at each operation is the state that apply gives at the
	 * same position. In a request with no repeated item, this writes nothing.
	 *
	 * Conditions, update checks, and size checks read `items`, which holds the state that the earlier
	 * operations left. The timestamp check uses only the state from before the transaction: all
	 * operations of a transaction use one timestamp, so a check against a temporary state rejects valid
	 * sequences such as `put → check` and `delete → put`.
	 */
	#evaluate(plan: SequencePlan, path: EvaluatePath): Evaluation {
		const { ops, seqs } = plan;
		// An accepted prepare and a committed single-shot transaction return no result for each operation.
		// Thus `results` stays empty until the first rejection. That rejection first adds `passed` for
		// each earlier operation, because no operation before it failed.
		const results: ParticipantOperationResultEncoded[] = [];
		const growth = new ItemGrowth();
		let rejected = false;
		let temporaryWrites = false;
		// The deletion watermark is one value for the whole partition, and a temporary delete of any item
		// raises it. Thus a repeated request reads it before the first temporary write.
		let committedMaxDeleteTs = path.twoPhase && plan.repeated ? this.#store.getMaxDeleteTxOrderTs() : undefined;

		const pass = (i: number) => {
			if (rejected) {
				results.push({ outcome: "passed", opIndex: ops[i].opIndex });
			}
		};
		const reject = (i: number, reason: RejectionReasonEncoded, imageBytes?: number) => {
			if (!rejected) {
				rejected = true;
				for (let j = 0; j < i; j++) {
					results.push({ outcome: "passed", opIndex: ops[j].opIndex });
				}
			}
			results.push({ outcome: "rejected", opIndex: ops[i].opIndex, reason, ...(imageBytes === undefined ? {} : { imageBytes }) });
			seqs[i].failed = true;
		};

		for (let i = 0; i < ops.length; i++) {
			const op = ops[i];
			const seq = seqs[i];
			if (seq.failed) {
				results.push({ outcome: "not_evaluated", opIndex: op.opIndex });
				continue;
			}
			const first = op === seq.ops[0];

			if (first) {
				const pendingRow = this.#store.pendingLockFor(op.hashKey, op.sortKey);
				if (pendingRow && path.twoPhase && pendingRow.transaction_id === path.transactionId) {
					// A repeated prepare: the lock row holds the result of the first prepare. The
					// single-partition path writes no lock, so every lock is a conflict there.
					seq.lockedBefore = true;
				} else if (pendingRow) {
					const keys = decodeItemKeys(op.hashKey, op.sortKey);
					reject(i, { code: "pending_conflict", ...keys, conflictingTransactionId: pendingRow.transaction_id });
					continue;
				}
			}
			if (seq.lockedBefore) {
				pass(i);
				continue;
			}

			const conditionResult = op.condition ? this.#store.evaluateCondition(op.condition, op.hashKey, op.sortKey) : null;
			if (conditionResult && !conditionResult.conditionOk) {
				const image = this.#imageForFailedCondition(op, op.sortKey, conditionResult.itemPresent);
				reject(i, conditionFailedReason(decodeItemKeys(op.hashKey, op.sortKey), image), image?.imageBytes);
				continue;
			}

			// A prepare reads the item stamps at the first operation of the item. When no condition read
			// the row, a put reads the stamps in the statement that measures it.
			const { reason: writeReason, read } = this.#precheckWrite(op, path.twoPhase && first && !conditionResult);
			if (writeReason) {
				reject(i, writeReason);
				continue;
			}

			if (path.twoPhase) {
				if (first) {
					// The condition, the update probe, or the put measure already read the row, so its
					// timestamps come from that read. Only an operation that did none of these reads the item
					// here. Only the operations of this item write its row, so the later operations of the item
					// use these stamps.
					const itemRead = conditionResult ?? read;
					seq.committedStamp = (itemRead ? itemStampOf(itemRead) : this.#store.getItemStamp(op.hashKey, op.sortKey).row) ?? null;
				}
				const stamp = seq.committedStamp;
				invariant(stamp !== undefined, "fokos/partition.prepare: the first operation of an item read no stamps");
				// A check reads the item and does not change it, so only a newer write orders against it. A
				// content mutation must stay above every earlier read and write. No stamp means no live item,
				// so the deletion watermark is the only ordering signal left. This holds for every
				// operation: a check, which writes nothing but still orders itself against later
				// transactions, and an update of an absent item, which creates it.
				const watermark =
					stamp === null
						? (committedMaxDeleteTs ??= this.#store.getMaxDeleteTxOrderTs())
						: op.operation === "check"
							? stamp.last_write_ts
							: stamp.last_read_ts;
				if (path.txTimestamp <= watermark) {
					reject(i, { code: "timestamp_conflict", ...decodeItemKeys(op.hashKey, op.sortKey) });
					continue;
				}
			}

			pass(i);

			if (plan.repeated) {
				growth.record(op, this.#writeRequest(op, path.txTimestamp));
				temporaryWrites = true;
				// Only the lock block of a prepare reads the last-write data. The single-partition path keeps
				// its writes, so it needs no copy.
				if (op === seq.lastWrite && path.twoPhase) {
					seq.lastWriteData = this.#store.readItemData(op.hashKey, op.sortKey);
					invariant(seq.lastWriteData, () => `fokos/partition: no row after a write of ${KeyCodec.pairForLog(op.hashKey, op.sortKey)}`);
				}
			}
		}

		return { results, rejected, temporaryWrites, growth: growth.candidates() };
	}

	/**
	 * Writes the `pending_tx_info` row and one lock row for each item that this transaction does not
	 * lock yet. A lock row holds the operation list of its item and the data of its last write.
	 */
	#writeLocks(plan: SequencePlan, request: PrepareRequest, coordinatorJson: string, now: number): void {
		const tx: PendingTxInfo = {
			transaction_id: request.transactionId,
			transaction_ts: request.transactionTimestamp,
			coordinator_json: coordinatorJson,
			created_at: now,
			guarded_at: null,
			next_recovery_at: now + this.#staleTransactionMs(),
		};
		for (const seq of plan.sequences.values()) {
			if (seq.lockedBefore) {
				continue;
			}
			const { hashKey: hk, sortKey: sk, ops } = seq;
			const opList: OpList = ops.map((op) => [op.opIndex, op.operation]);
			const [only] = ops;
			let inserted: boolean;
			if (ops.length === 1 && only.operation === "update") {
				invariant(only.update, "fokos/partition.prepare: update item missing update plan");
				inserted = this.#store.insertPendingUpdateLock({ hk, sk, tx, plan: only.update, ttlAt: only.ttlAt, opList }).rowsWritten > 0;
			} else {
				// One operation takes its data from the request: a put carries data and kind, and a delete or
				// a check carries neither. A longer sequence takes the data that the evaluate step read after
				// its last write, because the rollback discarded that row.
				invariant(
					ops.length === 1 || (seq.lastWrite === undefined) === (seq.lastWriteData === undefined),
					() => `fokos/partition.prepare: no last-write data for ${KeyCodec.pairForLog(hk, sk)}`,
				);
				const data =
					ops.length === 1
						? { data: only.data ?? null, kind: only.kind ?? null, ttl_epoch_utc_seconds: only.ttlAt ?? null }
						: (seq.lastWriteData ?? { data: null, kind: null, ttl_epoch_utc_seconds: null });
				inserted = this.#store.insertPendingLock({ ...tx, ...data, hk, sk, operation: lockOperationOf(ops), op_list: opList });
			}
			// The evaluate step found no lock of another transaction on this key. An accepted item with no
			// lock row makes the commit find no row to apply, and report success for a write it skipped.
			invariant(inserted, () => `fokos/partition.prepare: no lock row written for ${KeyCodec.pairForLog(hk, sk)}`);
		}
	}

	/**
	 * One store write of one operation at `ts`. A put or an update takes its data from `source`: the
	 * request operation, or the last-write data of a lock row. Returns the size estimate of the key
	 * after a put or an update.
	 */
	#write(hk: KeyBytes, sk: KeyBytes, operation: TransactionOperationType, source: WriteSource, ts: number): number | undefined {
		if (operation === "delete") {
			this.#store.deleteItem({ hk, sk, txOrderTs: ts, bumpTxOrderTsAlways: true });
			return undefined;
		}
		if (operation === "check") {
			// A check writes nothing, but it still orders this transaction against later writes through
			// the item's read watermark.
			this.#store.bumpItemReadTs(hk, sk, ts);
			return undefined;
		}
		invariant(isWrite(operation), () => `fokos/partition: unknown operation ${operation} (${KeyCodec.pairForLog(hk, sk)})`);
		if (source.from === "lock") {
			const { row } = source;
			// A lock row with a write always persisted both data and kind; assert together so upsertItem gets a real kind.
			invariant(
				row.data !== null && row.kind !== null,
				() => `fokos/partition.commit: pending "${operation}" row has no data/kind (${KeyCodec.pairForLog(hk, sk)})`,
			);
			// For kind=json, a put's row holds JSON text, which upsertItem encodes to JSONB, and the row of an
			// update or of a longer sequence holds the stored JSONB, which binds verbatim.
			return this.#store.upsertItem({ hk, sk, data: row.data, kind: row.kind, ttlAt: row.ttl_epoch_utc_seconds, txOrderTs: ts })
				.keyEstBytes;
		}
		const { item } = source;
		if (operation === "update") {
			invariant(item.update, "fokos/partition: update item missing update plan");
			return this.#store.updateItemSingleShot({ hk, sk, plan: item.update, ttlAt: item.ttlAt, txOrderTs: ts }).keyEstBytes;
		}
		// A put always carries both data and kind; assert together so upsertItem gets a real kind.
		invariant(
			item.data != null && item.kind != null,
			() => `fokos/partition: "put" item has no data/kind (${KeyCodec.pairForLog(hk, sk)})`,
		);
		// For kind=json, data is raw JSON text; upsertItem encodes it to JSONB.
		return this.#store.upsertItem({ hk, sk, data: item.data, kind: item.kind, ttlAt: item.ttlAt ?? null, txOrderTs: ts }).keyEstBytes;
	}

	/** One store write of one request operation at `ts`. */
	#writeRequest(op: TransactionItem, ts: number): number | undefined {
		return this.#write(op.hashKey, op.sortKey, op.operation, { from: "request", item: op }, ts);
	}

	/**
	 * Sorts `entries` in place by `opIndex`, and makes one store write for each entry in that order, at
	 * `ts`. Returns the promotion candidate of the last write of each item whose final state is present.
	 *
	 * Each write of an item uses the same data, also a write in the middle of its sequence. That data is
	 * invisible: the writes run in one storage transaction, and a later write or delete replaces it. The
	 * values that other requests see depend only on the order and the types of the writes: `v`,
	 * `max_deleted_v`, the timestamps, and `key_size_estimates`.
	 */
	#apply(entries: ApplyEntry[], ts: number): PromotionCandidate[] {
		entries.sort((a, b) => a.opIndex - b.opIndex);
		const growth = new ItemGrowth();
		for (let i = 0; i < entries.length; i++) {
			const entry = entries[i];
			invariant(i === 0 || entries[i - 1].opIndex !== entry.opIndex, () => `fokos/partition: apply got opIndex ${entry.opIndex} two times`);
			growth.record(entry, this.#write(entry.hashKey, entry.sortKey, entry.operation, entry.source, ts));
		}
		return growth.candidates();
	}

	/**
	 * Applies the part of a commit that this partition owns. Each decision uses the owned rows of the
	 * transaction: whether local work remains, whether the request matches, and which rows to apply
	 * and release. A row of a key that a promotion moved away is a copy. The new owner resolves it,
	 * and the source cleanup after the promotion deletes it here.
	 */
	commitLocal(request: CommitRequest): CommitLocalResult {
		const promotionCandidates = this.#store.transactionSync((): PromotionCandidate[] => {
			// The coordinator sends each key one time. A key two times passes the size comparison below,
			// and apply then applies the entries of one lock row two times. Thus it is a defect in the code.
			const requestKeys = new KeyPairMap<true>();
			for (const item of request.items) {
				invariant(
					!requestKeys.has(item.hashKey, item.sortKey),
					() =>
						`fokos/partition.commit: transaction ${request.transactionId} names ${KeyCodec.pairForLog(item.hashKey, item.sortKey)} two times`,
				);
				requestKeys.set(item.hashKey, item.sortKey, true);
			}
			// Each key of the request is owned: the runtime resolved it to this partition in this
			// synchronous block. A row outside the request is owned only when the owner check says so. On the
			// usual path no row is outside the request, and the method does not call the owner check.
			const ownsRow = ownsByHashKey(this.#ownerCheck());
			const ownedRows = new KeyPairMap<PendingLock>();
			for (const row of this.#store.listPendingTxItems(request.transactionId)) {
				if (requestKeys.has(row.hk, row.sk) || ownsRow(row)) {
					ownedRows.set(row.hk, row.sk, row);
				}
			}
			// No owned row remains: the rows are gone, or only copies remain. This partition has no
			// local work, and the answer is the idempotent success.
			if (ownedRows.size === 0) {
				return [];
			}
			if (ownedRows.size !== requestKeys.size) {
				throw new FokosInternalError(INTERNAL_CODES.commit_keyset_mismatch, {
					message: "pending_transactions and the commit request hold a different number of items",
					attributes: { transactionId: request.transactionId, pendingItems: ownedRows.size, requestItems: requestKeys.size },
				});
			}
			for (const { hashKey, sortKey } of requestKeys.entries()) {
				if (!ownedRows.has(hashKey, sortKey)) {
					throw new FokosInternalError(INTERNAL_CODES.commit_keyset_mismatch, {
						message: "a commit request item is not found in pending_transactions",
						attributes: { transactionId: request.transactionId, key: KeyCodec.pairForLog(hashKey, sortKey) },
					});
				}
			}
			// Each lock row of a transaction reads its timestamp from the same pending_tx_info row. The
			// prepare compared that timestamp with the item watermarks, so the commit must stamp the same one.
			const [first] = ownedRows.values();
			invariant(
				first.transaction_ts === request.transactionTimestamp,
				() =>
					`fokos/partition.commit: transaction ${request.transactionId} prepared at ${first.transaction_ts} commits at ${request.transactionTimestamp}`,
			);

			// The request items are keys only: every operation and its data come from the lock rows that
			// prepare wrote. One apply call applies the entries of all owned rows in opIndex order.
			const entries: ApplyEntry[] = [];
			for (const row of ownedRows.values()) {
				const source: WriteSource = { from: "lock", row };
				for (const [opIndex, operation] of row.op_list) {
					entries.push({ opIndex, operation, hashKey: row.hk, sortKey: row.sk, source });
				}
			}
			const candidates = this.#apply(entries, request.transactionTimestamp);
			// The owned set and the request have the same keys here. The release deletes these keys one
			// by one, and keeps the copies of a moved key.
			this.#store.deletePendingTxKeys(request.transactionId, request.items);
			return candidates;
		});

		return { response: { outcome: "committed" }, promotionCandidates };
	}

	/**
	 * Validates and applies a whole transaction that this partition owns end to end, in one storage
	 * transaction. It is the transactional equivalent of the non-transactional write path, not a
	 * phase of the two-phase protocol: it takes no lock, so it needs no cancel, no stale-transaction
	 * alarm and no recovery, and it can never be the cause of another transaction's pending conflict.
	 *
	 * A lock held by a two-phase transaction still wins: that transaction may yet commit, so this one
	 * is rejected rather than allowed to overwrite the decision.
	 *
	 * The timestamp is this partition's own clock, as `apiPutItem` stamps it. `PartitionStore` keeps
	 * per-item monotonicity in SQL, so a stamp from a lagging clock is absorbed, not applied.
	 */
	executeSingleShot(request: SingleShotRequest): SingleShotResult {
		const transactionTimestamp = this.#txOrderTimestamp();
		const plan = sequencePlanOf(request.items);

		// One storage transaction. When one operation failed, the block throws, and SQLite rolls back
		// every write. In a request with an item of two or more operations, the temporary writes of the
		// evaluate step are the writes of every operation in opIndex order, so the block keeps them. In a
		// request with no such item, the evaluate step wrote nothing, and apply writes the request.
		//
		// Nothing after the evaluate step may RETURN a rejection. transactionSync commits whatever the
		// callback wrote when the callback returns. Every rejectable test therefore runs in the evaluate
		// step, and the store raises on a size guard it can no longer reach, which rolls the whole set back.
		try {
			return this.#store.transactionSync((): SingleShotResult => {
				const ev = this.#evaluate(plan, { twoPhase: false, txTimestamp: transactionTimestamp });
				if (ev.rejected) {
					throw new EvaluationRollback(ev);
				}
				if (ev.temporaryWrites) {
					return { response: { outcome: "committed" }, promotionCandidates: ev.growth };
				}
				// `plan.ops` is in `opIndex` order, and each `opIndex` occurs one time in it.
				const growth = new ItemGrowth();
				for (const op of plan.ops) {
					growth.record(op, this.#writeRequest(op, transactionTimestamp));
				}
				return { response: { outcome: "committed" }, promotionCandidates: growth.candidates() };
			});
		} catch (err) {
			if (!(err instanceof EvaluationRollback)) {
				throw err;
			}
			applyImageCap(err.evaluation.results);
			return { response: { outcome: "rejected", results: err.evaluation.results }, promotionCandidates: [] };
		}
	}

	/**
	 * Releases the locks of a cancelled transaction under `ownedKeys`. The caller owns these keys, so
	 * the method keeps the copies of a key that this partition moved away. With no key, it releases
	 * nothing: a router and a promotion source send the moved keys on, and the partition that imports
	 * the copies releases its own rows.
	 */
	cancelLocal(transactionId: string, ownedKeys: readonly TransactionItemKey[]): void {
		this.#store.deletePendingTxKeys(transactionId, ownedKeys);
	}

	/**
	 * Reads every requested key from local storage. Takes only the keys: it holds no lock and
	 * writes nothing, so the single-shot read path can call it without inventing a transaction id.
	 */
	readForTransactionLocal(request: Pick<ReadForTransactionRequest, "items">): ReadForTransactionResponse {
		const results: ReadForTransactionItemResultEncoded[] = [];

		// At most one deletion-metadata read per RPC, shared by every absent item of the request. A
		// found item needs no value: the read compares its `v`.
		let maxDeletedV: number | undefined;
		const readMaxDeletedV = () => (maxDeletedV ??= this.#store.getMaxDeletedV());

		for (const item of request.items) {
			const sk = item.sortKey;

			const itemRow = item.projection
				? this.#store.getItemProjected(item.projection, item.hashKey, sk).row
				: this.#store.getItem(item.hashKey, sk).row;
			const pendingRow = this.#store.pendingLockFor(item.hashKey, sk);

			const hasPendingWrite = pendingRow != null && !READ_ONLY_PENDING_OPERATIONS.has(pendingRow.operation);
			// Echo the requested keys as canonical KeyBytes: the TC pairs phase 1 with phase 2 by bytes,
			// and db.ts decodes once at the public exit.
			const hashKey = item.hashKey;
			const sortKey = sk;

			if (itemRow && "projected" in itemRow) {
				results.push({
					found: true,
					hashKey,
					sortKey,
					projected: itemRow.projected,
					kind: "projected",
					// `v` stays: the two-phase driver compares it for every found item.
					version: itemRow.version,
					...(itemRow.ttlAt === undefined ? {} : { ttlAt: itemRow.ttlAt }),
					hasPendingWrite,
				});
			} else if (itemRow) {
				results.push({
					found: true,
					hashKey,
					sortKey,
					// json arrives as JSON text (decoded in SQL); db.ts parses it once at the public boundary.
					data: itemRow.data,
					kind: itemRow.kind,
					// `v` is the conflict datum for the TC's two-phase read AND the public version, so the
					// caller can feed it straight back into an attribute_equals condition.
					version: itemRow.v,
					ttlAt: itemRow.ttl_epoch_utc_seconds ?? undefined,
					hasPendingWrite,
				});
			} else {
				results.push({
					found: false,
					hashKey,
					sortKey,
					maxDeletedV: readMaxDeletedV(),
					hasPendingWrite,
				});
			}
		}

		return { items: results };
	}

	/**
	 * The stale transactions whose next attempt is at or before `dueAt`, earliest first, at most `limit`
	 * of them. It moves the next attempt of each one forward in the same storage transaction, before the
	 * caller asks a coordinator. So a lock that stays, because the coordinator answers `driving` or does
	 * not answer, does not keep the deadline of the job in the past, and does not block the other
	 * transactions. `nextRecoveryAt` gives the wait.
	 *
	 * The job passes the start time of its step as `dueAt`, and a claim moves the time past it, so one
	 * step claims a transaction at most one time.
	 */
	claimStaleTransactions(dueAt: number, limit: number): StalePendingTx[] {
		const now = this.#now();
		const staleMs = this.#staleTransactionMs();
		return this.#store.transactionSync(() => {
			const rows = this.#store.listStalePendingTx(dueAt, limit);
			for (const row of rows) {
				this.#store.deferPendingTxRecovery(row.transaction_id, nextRecoveryAt(now, row.created_at, staleMs));
			}
			return rows;
		});
	}
}
