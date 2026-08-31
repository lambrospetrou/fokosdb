import type {
	CommitRequest,
	CommitResponse,
	PrepareRequest,
	PrepareResponse,
	ReadForTransactionItemResultEncoded,
	ReadForTransactionRequest,
	ReadForTransactionResponse,
	SingleShotRequest,
	SingleShotResponse,
	TransactionItem,
} from "../transaction-types.js";
import invariant from "../invariant.js";
import { KeyCodec, type KeyBytes } from "../partition-topology/key-codec.js";
import type { PartitionStore } from "./partition-store.js";

// Decode a sort key for a user-facing result: the empty sentinel ([]) maps back to an absent sortKey.
function decodeSortKey(sk: KeyBytes): string | Uint8Array | undefined {
	return sk.length === 0 ? undefined : KeyCodec.decode(sk);
}

export type TransactionParticipantDeps = {
	store: PartitionStore;
	/** Injectable clock for skew/staleness tests; defaults to Date.now. */
	now?: () => number;
	/**
	 * Called after a committed "put" lands, with the key's updated size estimate — the DO wires
	 * this to the promotion manager's queue check so the participant stays promotion-agnostic.
	 */
	onItemUpserted?: (hashKey: KeyBytes, keyEstBytes: number) => void;
};

/**
 * The 2PC participant: prepare/commit/cancel/read for the items this partition owns locally.
 * Routing fan-out (groupItemsByRouting), child RPCs, alarm scheduling, and stale-tx recovery
 * driving stay in PartitionDO — this class only implements the local protocol semantics over
 * the PartitionStore.
 */
export class TransactionParticipant {
	/** Prepares arriving more than this far ahead of the local clock are rejected (clock_skew). */
	static readonly MAX_CLOCK_SKEW_MS = 5_000;

	#store: PartitionStore;
	#now: () => number;
	#onItemUpserted?: (hashKey: KeyBytes, keyEstBytes: number) => void;

	constructor(deps: TransactionParticipantDeps) {
		this.#store = deps.store;
		this.#now = deps.now ?? (() => Date.now());
		this.#onItemUpserted = deps.onItemUpserted;
	}

	prepareLocal(request: PrepareRequest): PrepareResponse {
		// A lock is only ever released by the outcome of its transaction, and the two ids below are the
		// whole thread back to that outcome: the recovery job selects locks by transaction_id and calls
		// the TC named by coordinator_do_id (it skips a row whose id is empty, and nothing else in the
		// system can supply one). A lock missing either is therefore unreleasable — it would block every
		// non-transactional write to its key for the life of the partition. Refuse to create it.
		invariant(request.transactionId.length > 0, "fokos/partition.prepare: transactionId is required");
		invariant(request.coordinatorDoId.length > 0, "fokos/partition.prepare: coordinatorDoId is required");

		const now = this.#now();

		if (request.transactionTimestamp > now + TransactionParticipant.MAX_CLOCK_SKEW_MS) {
			return {
				outcome: "rejected",
				reason: {
					type: "clock_skew",
					serverTimestampMs: now,
					transactionTimestampMs: request.transactionTimestamp,
				},
			};
		}

		return this.#store.transactionSync<PrepareResponse>(() => {
			for (const item of request.items) {
				const sk = item.sortKey;
				const rejectionKeys = { hashKey: KeyCodec.decode(item.hashKey), sortKey: decodeSortKey(sk) };

				const pendingRow = this.#store.pendingLockFor(item.hashKey, sk);

				if (pendingRow) {
					if (pendingRow.transaction_id === request.transactionId) {
						continue; // idempotent re-prepare for this item
					}
					return {
						outcome: "rejected",
						reason: {
							type: "pending_conflict",
							...rejectionKeys,
							conflictingTransactionId: pendingRow.transaction_id,
						},
					};
				}

				const conditionResult = item.condition ? this.#store.evaluateCondition(item.condition, item.hashKey, sk) : null;
				if (conditionResult && !conditionResult.conditionOk) {
					return {
						outcome: "rejected",
						reason: { type: "condition_failed", ...rejectionKeys },
					};
				}
				const itemStamp = conditionResult
					? conditionResult.itemPresent
						? { last_transaction_ts: conditionResult.lastTransactionTs! }
						: undefined
					: this.#store.getItemStamp(item.hashKey, sk).row;

				if (itemStamp) {
					if (request.transactionTimestamp <= itemStamp.last_transaction_ts) {
						return {
							outcome: "rejected",
							reason: { type: "timestamp_conflict", ...rejectionKeys },
						};
					}
				} else if (item.operation === "put" || item.operation === "delete" || item.operation === "check") {
					// A check on a non-existent item must also respect the deletion watermark.
					if (request.transactionTimestamp <= this.#store.getMaxDeletedTs()) {
						return {
							outcome: "rejected",
							reason: { type: "timestamp_conflict", ...rejectionKeys },
						};
					}
				}
			}

			// All checks passed — lock every item.
			for (const item of request.items) {
				const sk = item.sortKey;
				this.#store.insertPendingLock({
					hk: item.hashKey,
					sk,
					transaction_id: request.transactionId,
					transaction_ts: request.transactionTimestamp,
					operation: item.operation,
					data: item.data ?? null,
					// data and kind travel together: put carries both; delete/check carry neither (NULL kind).
					kind: item.kind ?? null,
					conditions_json: item.condition ? JSON.stringify(item.condition) : null,
					ttl_epoch_utc_seconds: item.ttlAt ?? null,
					coordinator_do_id: request.coordinatorDoId,
					created_at: this.#now(),
				});
			}

			return { outcome: "accepted" };
		});
	}

	commitLocal(request: CommitRequest): CommitResponse {
		const pendingCount = this.#store.pendingTxCountFor(request.transactionId);

		if (pendingCount === 0) {
			return { outcome: "committed" };
		}

		this.#store.transactionSync(() => {
			const pendingRows = this.#store.listPendingTxKeys(request.transactionId);
			const pendingKeySet = new Set(pendingRows.map((r) => KeyCodec.pairKey(r.hk, r.sk)));
			const requestKeySet = new Set(request.items.map((i) => KeyCodec.pairKey(i.hashKey, i.sortKey)));
			if (pendingKeySet.size !== requestKeySet.size) {
				throw new Error(
					`fokos/partition.commit: pending_transactions has ${pendingKeySet.size} items but request has ${requestKeySet.size} for transaction ${request.transactionId}`,
				);
			}
			for (const key of requestKeySet) {
				if (!pendingKeySet.has(key)) {
					throw new Error(
						`fokos/partition.commit: request item ${key} not found in pending_transactions for transaction ${request.transactionId}`,
					);
				}
			}

			this.#applyCommitItems(request.transactionId, request.transactionTimestamp, request.items);
			this.#store.deletePendingTx(request.transactionId);
		});

		return { outcome: "committed" };
	}

	#applyCommitItems(transactionId: string, transactionTimestamp: number, items: TransactionItem[]): void {
		for (const item of items) {
			const sk = item.sortKey;
			const pendingRow = this.#store.getPendingTxOp(item.hashKey, sk, transactionId);

			if (!pendingRow) continue;

			if (pendingRow.operation === "put") {
				// A put always persisted both data and kind; assert together so upsertItem gets a real kind.
				invariant(
					pendingRow.data !== null && pendingRow.kind !== null,
					() => `fokos/partition.commit: pending "put" row has no data/kind (${KeyCodec.pairForLog(item.hashKey, sk)})`,
				);
				const res = this.#store.upsertItem({
					hk: item.hashKey,
					sk,
					data: pendingRow.data,
					// For kind=json -> pendingRow.data is raw JSON text; upsertItem re-encodes it to JSONB.
					kind: pendingRow.kind,
					ttlAt: pendingRow.ttl_epoch_utc_seconds,
					lastTransactionTs: transactionTimestamp,
				});
				this.#onItemUpserted?.(item.hashKey, res.keyEstBytes);
			} else if (pendingRow.operation === "delete") {
				this.#store.deleteItem({ hk: item.hashKey, sk, watermarkTs: transactionTimestamp, bumpWatermarkAlways: true });
			} else if (pendingRow.operation === "check") {
				this.#store.bumpItemLastTransactionTs(item.hashKey, sk, transactionTimestamp);
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
	executeSingleShot(request: SingleShotRequest): SingleShotResponse {
		const transactionTimestamp = this.#now();

		return this.#store.transactionSync<SingleShotResponse>(() => {
			for (const item of request.items) {
				const sk = item.sortKey;
				const rejectionKeys = { hashKey: KeyCodec.decode(item.hashKey), sortKey: decodeSortKey(sk) };

				const pendingRow = this.#store.pendingLockFor(item.hashKey, sk);
				if (pendingRow) {
					return {
						outcome: "rejected",
						reason: {
							type: "pending_conflict",
							...rejectionKeys,
							conflictingTransactionId: pendingRow.transaction_id,
						},
					};
				}

				if (item.condition && !this.#store.evaluateCondition(item.condition, item.hashKey, sk).conditionOk) {
					return {
						outcome: "rejected",
						reason: { type: "condition_failed", ...rejectionKeys },
					};
				}
			}

			// Every item passed, so the whole set applies. Reaching this point inside transactionSync is
			// what makes the transaction atomic: a throw below rolls the statements above back with it.
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
						lastTransactionTs: transactionTimestamp,
					});
					this.#onItemUpserted?.(item.hashKey, res.keyEstBytes);
				} else if (item.operation === "delete") {
					this.#store.deleteItem({ hk: item.hashKey, sk, watermarkTs: transactionTimestamp, bumpWatermarkAlways: true });
				} else {
					// A check writes nothing, but it still orders this transaction against later ones:
					// transactGetItems uses last_transaction_ts as its second conflict signal.
					this.#store.bumpItemLastTransactionTs(item.hashKey, sk, transactionTimestamp);
				}
			}

			return { outcome: "committed" };
		});
	}

	cancelLocal(transactionId: string): void {
		this.#store.deletePendingTx(transactionId);
	}

	/**
	 * Reads every requested key from local storage. Takes only the keys: it holds no lock and
	 * writes nothing, so the single-shot read path can call it without inventing a transaction id.
	 */
	readForTransactionLocal(request: Pick<ReadForTransactionRequest, "items">): ReadForTransactionResponse {
		const results: ReadForTransactionItemResultEncoded[] = [];

		for (const item of request.items) {
			const sk = item.sortKey;

			const itemRow = this.#store.getItem(item.hashKey, sk).row;
			const pendingRow = this.#store.pendingLockFor(item.hashKey, sk);

			const hasPendingWrite = pendingRow != null;
			const lastCommittedTs = itemRow?.last_transaction_ts ?? 0;
			// Echo the requested keys as canonical KeyBytes: the TC pairs phase 1 with phase 2 by bytes,
			// and db.ts decodes once at the public exit.
			const hashKey = item.hashKey;
			const sortKey = sk;

			if (itemRow) {
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
					lastCommittedTs,
					hasPendingWrite,
				});
			} else {
				results.push({
					found: false,
					hashKey,
					sortKey,
					lastCommittedTs,
					hasPendingWrite,
				});
			}
		}

		return { items: results };
	}

	/** Transactions whose locks are older than `staleMs` — the DO drives recovery via the TC. */
	listStaleTransactions(staleMs: number, limit: number): { transaction_id: string; coordinator_do_id: string }[] {
		return this.#store.listStalePendingTx(this.#now() - staleMs, limit);
	}
}
