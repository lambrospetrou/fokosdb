/**
 * Shared transaction limits and validation for transact-write operations.
 * Single source of truth used by both the FokosDB client (db.ts) and the
 * TransactionCoordinatorDO — keep client-side and coordinator-side validation in lockstep.
 */

import type { TransactionOperationType } from "./transaction-types.js";

export const MAX_ITEMS_PER_TRANSACTION = 100;
export const MAX_PAYLOAD_BYTES = 4 * 1024 * 1024; // 4 MB

/**
 * The minimal shape validation needs. Both TCWriteOperation (client/TC wire type) and the
 * client-facing operations input satisfy this structurally.
 */
export type TransactWriteOperationLike = {
	hashKey: string;
	sortKey?: string;
	operation: TransactionOperationType;
	data?: Uint8Array | string;
};

/**
 * Item keys must never contain the NUL character: composite keys are encoded as
 * `${hashKey}\0${sortKey}` in duplicate detection here and in the 2PC keyset comparisons
 * (PartitionDO.commitLocal, TransactionCoordinatorDO.initiateRead). Rejecting NUL at the API
 * boundary is what makes that encoding collision-proof — without it, a NUL inside a key could
 * shift the separator boundary and make two distinct (hashKey, sortKey) pairs indistinguishable.
 */
export function validateItemKeys(hashKey: string, sortKey?: string): void {
	if (hashKey.includes("\0")) {
		throw new Error("fokos: hashKey must not contain the NUL (\\0) character");
	}
	if (sortKey?.includes("\0")) {
		throw new Error("fokos: sortKey must not contain the NUL (\\0) character");
	}
}

/**
 * Validates a transact-write operation set: NUL-free keys, item count, duplicate keys,
 * total payload bytes, and that every "put" carries data. Throws on the first violation.
 */
export function validateTransactWriteOperations(ops: readonly TransactWriteOperationLike[]): void {
	if (ops.length === 0) {
		throw new Error("fokos: transactWriteItems requires at least 1 item");
	}
	if (ops.length > MAX_ITEMS_PER_TRANSACTION) {
		throw new Error(`fokos: transactWriteItems supports at most ${MAX_ITEMS_PER_TRANSACTION} items`);
	}
	const seen = new Set<string>();
	let totalBytes = 0;
	for (const op of ops) {
		validateItemKeys(op.hashKey, op.sortKey);
		if (op.operation === "put" && op.data == null) {
			throw new Error(`fokos: transactWriteItems "put" operation requires data (${op.hashKey}${op.sortKey ? `, ${op.sortKey}` : ""})`);
		}
		const key = `${op.hashKey}\0${op.sortKey ?? ""}`;
		if (seen.has(key)) {
			throw new Error(`fokos: transactWriteItems duplicate key (${op.hashKey}, ${op.sortKey ?? ""})`);
		}
		seen.add(key);
		if (op.data) {
			totalBytes += typeof op.data === "string" ? op.data.length * 2 : op.data.byteLength;
		}
	}
	if (totalBytes > MAX_PAYLOAD_BYTES) {
		throw new Error(`fokos: transactWriteItems total payload exceeds ${MAX_PAYLOAD_BYTES / (1024 * 1024)} MB`);
	}
}
