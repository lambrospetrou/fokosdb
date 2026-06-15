import type {
	CommitRequest,
	CommitResponse,
	PrepareRequest,
	PrepareResponse,
	ReadForTransactionItemResult,
	ReadForTransactionRequest,
	ReadForTransactionResponse,
	TransactionItem,
} from "../transaction-types.js";
import invariant from "../invariant.js";
import { KeyCodec, type KeyBytes } from "../partition-topology/key-codec.js";
import { evaluateConditionsOnItem, type ItemSnapshot, type PartitionStore } from "./partition-store.js";

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

				const itemRow = this.#store.getItemStamp(item.hashKey, sk).row;

				if (item.conditions && item.conditions.length > 0) {
					const snapshot: ItemSnapshot = itemRow
						? { found: true, hk: item.hashKey, sk, v: itemRow.v }
						: { found: false, hk: item.hashKey, sk };
					try {
						evaluateConditionsOnItem(snapshot, item.conditions, "prepare");
					} catch {
						return {
							outcome: "rejected",
							reason: { type: "condition_failed", ...rejectionKeys },
						};
					}
				}

				if (itemRow) {
					if (request.transactionTimestamp <= itemRow.last_transaction_ts) {
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
					conditions_json: item.conditions ? JSON.stringify(item.conditions) : null,
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
			invariant(
				pendingKeySet.size === requestKeySet.size,
				`fokos/partition.commit: pending_transactions has ${pendingKeySet.size} items but request has ${requestKeySet.size} for transaction ${request.transactionId}`,
			);
			for (const key of requestKeySet) {
				invariant(
					pendingKeySet.has(key),
					`fokos/partition.commit: request item ${key} not found in pending_transactions for transaction ${request.transactionId}`,
				);
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
				invariant(
					pendingRow.data !== null,
					() =>
						`fokos/partition.commit: pending "put" row has no data (hk=${KeyCodec.keyForLog(item.hashKey)}, sk=${KeyCodec.keyForLog(sk)})`,
				);
				const res = this.#store.upsertItem({
					hk: item.hashKey,
					sk,
					data: pendingRow.data,
					ttlEpochUtcSeconds: null,
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

	cancelLocal(transactionId: string): void {
		this.#store.deletePendingTx(transactionId);
	}

	readForTransactionLocal(request: ReadForTransactionRequest): ReadForTransactionResponse {
		const results: ReadForTransactionItemResult[] = [];

		for (const item of request.items) {
			const sk = item.sortKey;

			const itemRow = this.#store.getItem(item.hashKey, sk).row;
			const pendingRow = this.#store.pendingLockFor(item.hashKey, sk);

			const hasPendingWrite = pendingRow != null;
			const lastCommittedTs = itemRow?.last_transaction_ts ?? 0;
			// Decode the requested keys back to the public form for the result echo.
			const hashKey = KeyCodec.decode(item.hashKey);
			const sortKey = decodeSortKey(sk);

			if (itemRow) {
				results.push({
					found: true,
					hashKey,
					sortKey,
					data: itemRow.data,
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
