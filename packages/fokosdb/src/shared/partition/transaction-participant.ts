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
	type TransactionTimestamp,
} from "../transaction-wire-types.js";
import invariant from "../invariant.js";
import { FokosInternalError, INTERNAL_CODES } from "../errors.js";
import { unexpectedTransactionStateError } from "../errors-operations.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import type { PartitionStore, PendingTxInfo, StalePendingTx } from "./partition-store.js";
import {
	applyImageCap,
	conditionFailedReason,
	decodeItemKeys,
	MAX_ITEM_BYTES,
	nextRecoveryAt,
	txOrderTimestampNow,
	TX_ORDER_TS_UNITS_PER_MS,
} from "../transaction-limits.js";
import type { UpdateProbeResult } from "../expression/runtime.js";

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
	const answers = new Map<bigint, boolean>();
	return (row) => {
		const id = KeyCodec.mapKey(row.hk);
		let owned = answers.get(id);
		if (owned === undefined) {
			owned = owns({ hashKey: row.hk, sortKey: row.sk });
			answers.set(id, owned);
		}
		return owned;
	};
}

// A pending check cannot change the item, so a transactional read may serialize on either side of it.
// Allowlist the read-only operations: an operation the code does not know counts as a pending write.
const READ_ONLY_PENDING_OPERATIONS: ReadonlySet<string> = new Set(["check"]);

type ItemStamp = { last_read_ts: number; last_write_ts: number };

/** One lock row of a transaction, as commit reads it. */
type PendingLock = ReturnType<PartitionStore["listPendingTxItems"]>[number];

/**
 * The item timestamps of the row that a condition or an update probe read, or undefined when it
 * found no row. Both columns are NOT NULL, so a live row always carries both values.
 */
function itemStampOf(read: { itemPresent: boolean; lastReadTs: number | null; lastWriteTs: number | null }): ItemStamp | undefined {
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
	 * Returns the update probe as well, because prepare reuses its item timestamps instead of
	 * reading the row a second time.
	 */
	#precheckWrite(
		item: TransactionItem,
		sk: KeyBytes,
		rejectionKeys: { hashKey: string | Uint8Array; sortKey?: string | Uint8Array },
	): { reason: RejectionReasonEncoded | null; probe: UpdateProbeResult | null } {
		if (item.operation === "put") {
			// A put always carries both data and kind; assert together so the measure gets a real kind.
			invariant(
				item.data !== undefined && item.kind !== undefined,
				() => `fokos/partition.precheck: "put" item has no data/kind (${KeyCodec.pairForLog(item.hashKey, sk)})`,
			);
			const bytes = this.#store.measureItemBytes({ hk: item.hashKey, sk, data: item.data, kind: item.kind });
			return { reason: bytes > MAX_ITEM_BYTES ? { code: "item_too_large", ...rejectionKeys } : null, probe: null };
		}

		if (item.operation === "update") {
			invariant(item.update, "fokos/partition.precheck: update item missing update plan");
			const probe = this.#store.probeUpdate(item.update, item.hashKey, sk);
			if (!probe.applicable) {
				// A value that evaluated to bytes is the one cause the probe separates out, because the
				// caller can act on it. Every other cause — a non-json item, a missing target path, a
				// missing operand — is reported as one answer, as DynamoDB reports its own.
				const code = probe.valueTypeOk ? "update_not_applicable" : "update_value_is_bytes";
				return { reason: { code, ...rejectionKeys }, probe };
			}
			// An applicable update always measured its result; the probe returns NULL only when it is not.
			invariant(probe.newSize !== null, "fokos/partition.precheck: applicable update reported no size");
			return { reason: probe.newSize > MAX_ITEM_BYTES ? { code: "item_too_large", ...rejectionKeys } : null, probe };
		}

		// delete and check write no data, so neither has a size to test.
		return { reason: null, probe: null };
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
		// One key has one lock row. A second item of the same key passes the checks, and its lock insert
		// is ignored, so a commit would apply one of the two operations.
		const requestKeys = new Set(request.items.map((item) => KeyCodec.pairKey(item.hashKey, item.sortKey)));
		invariant(
			requestKeys.size === request.items.length,
			() => `fokos/partition.prepare: transaction ${request.transactionId} names a key twice`,
		);
		const coordinatorJson = JSON.stringify(request.coordinator);

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

		return this.#store.transactionSync<PrepareResponse>(() => {
			const results: ParticipantOperationResultEncoded[] = [];
			// The opIndexes of the items that this transaction has locked already.
			const lockedBefore = new Set<number>();
			for (const item of request.items) {
				const { opIndex } = item;
				const sk = item.sortKey;
				const rejectionKeys = decodeItemKeys(item.hashKey, sk);

				const pendingRow = this.#store.pendingLockFor(item.hashKey, sk);

				if (pendingRow) {
					if (pendingRow.transaction_id === request.transactionId) {
						lockedBefore.add(opIndex);
						results.push({ outcome: "passed", opIndex });
						continue; // idempotent re-prepare for this item
					}
					results.push({
						outcome: "rejected",
						opIndex,
						reason: {
							code: "pending_conflict",
							...rejectionKeys,
							conflictingTransactionId: pendingRow.transaction_id,
						},
					});
					continue;
				}

				const conditionResult = item.condition ? this.#store.evaluateCondition(item.condition, item.hashKey, sk) : null;
				if (conditionResult && !conditionResult.conditionOk) {
					const image = this.#imageForFailedCondition(item, sk, conditionResult.itemPresent);
					results.push({
						outcome: "rejected",
						opIndex,
						reason: conditionFailedReason(rejectionKeys, image),
						...(image ? { imageBytes: image.imageBytes } : {}),
					});
					continue;
				}

				const { reason: writeReason, probe } = this.#precheckWrite(item, sk, rejectionKeys);
				if (writeReason) {
					results.push({ outcome: "rejected", opIndex, reason: writeReason });
					continue;
				}

				// The condition or the update probe already read the row, so its timestamps come from that
				// read. Only an operation that did neither reads the item here.
				const itemRead = conditionResult ?? probe;
				const itemStamp = itemRead ? itemStampOf(itemRead) : this.#store.getItemStamp(item.hashKey, sk).row;

				if (itemStamp) {
					// A check reads the item and does not change it, so only a newer write orders against
					// it. A content mutation must stay above every earlier read and write.
					// The last_read_ts is always greater than or equal to the last_write_ts of any previous write,
					// so using it as the watermark for updates ensures proper ordering against all prior operations.
					const watermark = item.operation === "check" ? itemStamp.last_write_ts : itemStamp.last_read_ts;
					if (request.transactionTimestamp <= watermark) {
						results.push({
							outcome: "rejected",
							opIndex,
							reason: { code: "timestamp_conflict", ...rejectionKeys },
						});
						continue;
					}
				} else {
					// No stamp means no live item, so the deletion watermark is the only ordering signal left.
					// This holds for every operation: a check, which writes nothing but still orders itself
					// against later transactions, and an update of an absent item, which creates it.
					if (request.transactionTimestamp <= this.#store.getMaxDeleteTxOrderTs()) {
						results.push({
							outcome: "rejected",
							opIndex,
							reason: { code: "timestamp_conflict", ...rejectionKeys },
						});
						continue;
					}
				}

				results.push({ outcome: "passed", opIndex });
			}

			if (results.some((r) => r.outcome === "rejected")) {
				applyImageCap(results);
				return { outcome: "rejected", results };
			}

			// All checks passed — lock every item.
			const tx: PendingTxInfo = {
				transaction_id: request.transactionId,
				transaction_ts: request.transactionTimestamp,
				coordinator_json: coordinatorJson,
				created_at: now,
				guarded_at: null,
				next_recovery_at: now + this.#staleTransactionMs(),
			};
			for (const item of request.items) {
				const sk = item.sortKey;
				let inserted: boolean;
				if (item.operation === "update") {
					invariant(item.update, "fokos/partition.prepare: update item missing update plan");
					inserted =
						this.#store.insertPendingUpdateLock({ hk: item.hashKey, sk, tx, plan: item.update, ttlAt: item.ttlAt }).rowsWritten > 0;
				} else {
					inserted = this.#store.insertPendingLock({
						...tx,
						hk: item.hashKey,
						sk,
						operation: item.operation,
						data: item.data ?? null,
						// data and kind travel together: put carries both; delete/check carry neither (NULL kind).
						kind: item.kind ?? null,
						ttl_epoch_utc_seconds: item.ttlAt ?? null,
					});
				}
				// The check pass found no lock of another transaction on this key. An accepted item with no
				// lock row makes the commit find no row to apply, and report success for a write it skipped.
				invariant(
					inserted || lockedBefore.has(item.opIndex),
					() => `fokos/partition.prepare: no lock row written for ${KeyCodec.pairForLog(item.hashKey, sk)}`,
				);
			}

			return { outcome: "accepted" };
		});
	}

	/**
	 * Applies the part of a commit that this partition owns. Each decision uses the owned rows of the
	 * transaction: whether local work remains, whether the request matches, and which rows to apply
	 * and release. A row of a key that a promotion moved away is a copy. The new owner resolves it,
	 * and the source cleanup after the promotion deletes it here.
	 */
	commitLocal(request: CommitRequest): CommitLocalResult {
		const promotionCandidates: PromotionCandidate[] = [];

		this.#store.transactionSync(() => {
			const requestKeySet = new Set(request.items.map((i) => KeyCodec.pairKey(i.hashKey, i.sortKey)));
			// Each key of the request is owned: the runtime resolved it to this partition in this
			// synchronous block. A row outside the request is owned only when the owner check says so. On the
			// usual path no row is outside the request, and the method does not call the owner check.
			const ownsRow = ownsByHashKey(this.#ownerCheck());
			const ownedRows = new Map<bigint, PendingLock>();
			for (const row of this.#store.listPendingTxItems(request.transactionId)) {
				const key = KeyCodec.pairKey(row.hk, row.sk);
				if (requestKeySet.has(key) || ownsRow(row)) {
					ownedRows.set(key, row);
				}
			}
			// No owned row remains: the rows are gone, or only copies remain. This partition has no
			// local work, and the answer is the idempotent success.
			if (ownedRows.size === 0) {
				return;
			}
			if (ownedRows.size !== requestKeySet.size) {
				throw new FokosInternalError(INTERNAL_CODES.commit_keyset_mismatch, {
					message: "pending_transactions and the commit request hold a different number of items",
					attributes: { transactionId: request.transactionId, pendingItems: ownedRows.size, requestItems: requestKeySet.size },
				});
			}
			for (const key of requestKeySet) {
				if (!ownedRows.has(key)) {
					throw new FokosInternalError(INTERNAL_CODES.commit_keyset_mismatch, {
						message: "a commit request item is not found in pending_transactions",
						attributes: { transactionId: request.transactionId, key: String(key) },
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

			this.#applyCommitItems(request.items, ownedRows, request.transactionTimestamp, promotionCandidates);
			// The owned set and the request have the same keys here. The release deletes these keys one
			// by one, and keeps the copies of a moved key.
			this.#store.deletePendingTxKeys(request.transactionId, request.items);
		});

		return { response: { outcome: "committed" }, promotionCandidates };
	}

	// Items are keys only: every per-item fact applied here (operation, data, kind) comes from the
	// partition's own pending_transactions row that prepare wrote. The caller read these rows once.
	#applyCommitItems(
		items: TransactionItemKey[],
		pendingRows: ReadonlyMap<ReturnType<typeof KeyCodec.pairKey>, PendingLock>,
		transactionTimestamp: number,
		promotionCandidates: PromotionCandidate[],
	): void {
		for (const item of items) {
			const sk = item.sortKey;
			const pendingRow = pendingRows.get(KeyCodec.pairKey(item.hashKey, sk));
			// The caller has proved that the request and the owned lock rows hold the same keys.
			invariant(pendingRow, () => `fokos/partition.commit: no lock row for ${KeyCodec.pairForLog(item.hashKey, sk)}`);

			if (pendingRow.operation === "put" || pendingRow.operation === "update") {
				// A put/update always persisted both data and kind; assert together so upsertItem gets a real kind.
				invariant(
					pendingRow.data !== null && pendingRow.kind !== null,
					() => `fokos/partition.commit: pending "${pendingRow.operation}" row has no data/kind (${KeyCodec.pairForLog(item.hashKey, sk)})`,
				);
				const res = this.#store.upsertItem({
					hk: item.hashKey,
					sk,
					// For kind=json a put's row holds JSON text, which upsertItem encodes to JSONB, and an
					// update's row holds the JSONB that prepare materialized, which binds verbatim.
					data: pendingRow.data,
					kind: pendingRow.kind,
					ttlAt: pendingRow.ttl_epoch_utc_seconds,
					txOrderTs: transactionTimestamp,
				});
				promotionCandidates.push({ hashKey: item.hashKey, keyEstBytes: res.keyEstBytes });
			} else if (pendingRow.operation === "delete") {
				this.#store.deleteItem({ hk: item.hashKey, sk, txOrderTs: transactionTimestamp, bumpTxOrderTsAlways: true });
			} else {
				// The release after this loop deletes the lock, so an operation that applies nothing here is a lost write.
				invariant(
					pendingRow.operation === "check",
					() => `fokos/partition.commit: unknown operation ${pendingRow.operation} (${KeyCodec.pairForLog(item.hashKey, sk)})`,
				);
				this.#store.bumpItemReadTs(item.hashKey, sk, transactionTimestamp);
			}
		}
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
		const promotionCandidates: PromotionCandidate[] = [];

		const response = this.#store.transactionSync<SingleShotResponse>(() => {
			const results: ParticipantOperationResultEncoded[] = [];
			for (const item of request.items) {
				const { opIndex } = item;
				const sk = item.sortKey;
				const rejectionKeys = decodeItemKeys(item.hashKey, sk);

				const pendingRow = this.#store.pendingLockFor(item.hashKey, sk);
				if (pendingRow) {
					results.push({
						outcome: "rejected",
						opIndex,
						reason: {
							code: "pending_conflict",
							...rejectionKeys,
							conflictingTransactionId: pendingRow.transaction_id,
						},
					});
					continue;
				}

				const conditionRes = item.condition ? this.#store.evaluateCondition(item.condition, item.hashKey, sk) : null;
				if (conditionRes && !conditionRes.conditionOk) {
					const image = this.#imageForFailedCondition(item, sk, conditionRes.itemPresent);
					results.push({
						outcome: "rejected",
						opIndex,
						reason: conditionFailedReason(rejectionKeys, image),
						...(image ? { imageBytes: image.imageBytes } : {}),
					});
					continue;
				}

				const { reason: writeReason } = this.#precheckWrite(item, sk, rejectionKeys);
				if (writeReason) {
					results.push({ outcome: "rejected", opIndex, reason: writeReason });
					continue;
				}

				results.push({ outcome: "passed", opIndex });
			}

			if (results.some((r) => r.outcome === "rejected")) {
				applyImageCap(results);
				return { outcome: "rejected", results };
			}

			// Every item passed, so the whole set applies. Reaching this point inside transactionSync is
			// what makes the transaction atomic: a throw below rolls the statements above back with it.
			//
			// Nothing below may RETURN a rejection. transactionSync commits whatever the callback wrote
			// when the callback returns, so a rejection here would keep the writes of the items already
			// applied. Every rejectable test therefore ran in the check pass above, and the store raises
			// on a size guard it can no longer reach, which rolls the whole set back.
			for (const item of request.items) {
				const sk = item.sortKey;
				if (item.operation === "put") {
					// A put always carries both data and kind; assert together so upsertItem gets a real kind.
					invariant(
						item.data != null && item.kind != null,
						() => `fokos/partition.singleShot: "put" item has no data/kind (${KeyCodec.pairForLog(item.hashKey, sk)})`,
					);
					const res = this.#store.upsertItem({
						hk: item.hashKey,
						sk,
						data: item.data,
						// For kind=json -> data is raw JSON text; upsertItem re-encodes it to JSONB.
						kind: item.kind,
						ttlAt: item.ttlAt ?? null,
						txOrderTs: transactionTimestamp,
					});
					promotionCandidates.push({ hashKey: item.hashKey, keyEstBytes: res.keyEstBytes });
				} else if (item.operation === "update") {
					invariant(item.update, "fokos/partition.singleShot: update item missing update plan");
					const res = this.#store.updateItemSingleShot({
						hk: item.hashKey,
						sk,
						plan: item.update,
						ttlAt: item.ttlAt,
						txOrderTs: transactionTimestamp,
					});
					promotionCandidates.push({ hashKey: item.hashKey, keyEstBytes: res.keyEstBytes });
				} else if (item.operation === "delete") {
					this.#store.deleteItem({ hk: item.hashKey, sk, txOrderTs: transactionTimestamp, bumpTxOrderTsAlways: true });
				} else {
					// A check writes nothing, but it still orders this transaction against later writes
					// through the item's read watermark.
					this.#store.bumpItemReadTs(item.hashKey, sk, transactionTimestamp);
				}
			}

			return { outcome: "committed" };
		});
		// No guard on the outcome. The check pass above returns a rejection BEFORE the apply loop runs,
		// so a rejected answer grew no key and the array is empty. A throw discards it with the frame.
		return { response, promotionCandidates };
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
