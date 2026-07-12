/**
 * Shared transaction limits and validation for transact-write operations.
 * Single source of truth used by both the FokosDB client (db.ts) and the
 * TransactionCoordinatorDO — keep client-side and coordinator-side validation in lockstep.
 */

import type { TransactionOperationType } from "./transaction-types.js";
import { KeyCodec } from "./partition-topology/key-codec.js";

export const MAX_ITEMS_PER_TRANSACTION = 100;
export const MAX_PAYLOAD_BYTES = 4 * 1024 * 1024; // 4 MB

/**
 * The minimal shape validation needs. Both TCWriteOperation (client/TC wire type) and the
 * client-facing operations input satisfy this structurally.
 */
export type TransactWriteOperationLike = {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;
	operation: TransactionOperationType;
	// Already-encoded data (json stringified upstream), so payload accounting is a plain byte/char count.
	data?: Uint8Array | string;
};

function isEmptyKey(k: string | Uint8Array): boolean {
	return typeof k === "string" ? k.length === 0 : k.byteLength === 0;
}

/**
 * The single key-validation boundary, run on public keys before encoding. Rejects:
 * - empty hashKey / empty sortKey (key attributes cannot be empty); an absent sortKey is allowed,
 * - lone-surrogate strings (invalid UTF-16),
 * - the NUL character in STRING keys. Binary (Uint8Array) keys may contain any byte, including 0x00.
 */
export function validateItemKeys(hashKey: string | Uint8Array, sortKey?: string | Uint8Array): void {
	if (isEmptyKey(hashKey)) {
		throw new Error("fokos: hashKey must not be empty");
	}
	if (sortKey !== undefined && isEmptyKey(sortKey)) {
		throw new Error("fokos: sortKey must not be empty (omit it for a single-key item)");
	}
	for (const [name, k] of [
		["hashKey", hashKey],
		["sortKey", sortKey],
	] as const) {
		if (typeof k !== "string") continue;
		if (k.includes("\0")) {
			throw new Error(`fokos: ${name} must not contain the NUL (\\0) character`);
		}
		if (k.isWellFormed?.() === false) {
			throw new Error(`fokos: ${name} string contains a lone surrogate (not well-formed UTF-16)`);
		}
	}
}

/**
 * Validates a transact-write operation set: valid keys, item count, duplicate keys, total payload
 * bytes, and that every "put" carries data. Throws on the first violation. Runs on RAW public keys.
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
			throw new Error(
				`fokos: transactWriteItems "put" operation requires data (${KeyCodec.keyForLog(KeyCodec.encode(op.hashKey))}${op.sortKey ? `, ${KeyCodec.keyForLog(KeyCodec.encode(op.sortKey))}` : ""})`,
			);
		}
		// Collision-proof composite identity for arbitrary key bytes.
		const key = `${op.hashKey.length}:${op.hashKey}:${op.sortKey ?? ""}`;
		if (seen.has(key)) {
			throw new Error(
				`fokos: transactWriteItems duplicate key (${KeyCodec.keyForLog(KeyCodec.encode(op.hashKey))}, ${op.sortKey ? KeyCodec.keyForLog(KeyCodec.encode(op.sortKey)) : ""})`,
			);
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
