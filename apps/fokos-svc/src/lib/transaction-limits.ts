/**
 * Shared transaction limits and validation for transact-write operations.
 * Single source of truth used by both the FokosDB client (db.ts) and the
 * TransactionCoordinatorDO — keep client-side and coordinator-side validation in lockstep.
 */

import type { TransactionOperationType } from "./transaction-types.js";
import { KeyCodec, type KeyBytes } from "./partition-topology/key-codec.js";

export const MAX_ITEMS_PER_TRANSACTION = 100;
export const MAX_PAYLOAD_BYTES = 4 * 1024 * 1024; // 4 MB
export const MAX_BATCH_GET_ITEMS = 100;
export const MAX_BATCH_WRITE_ITEMS = 25;
export const MAX_BATCH_PAYLOAD_BYTES = 4 * 1024 * 1024; // 4 MB; conservative first native ceiling.
// Single forwarded write ops reject in public validation; the DO-side guard defends internal chunks.
export const MAX_BATCH_FORWARDED_SUB_BATCH_BYTES = 1024 * 1024; // 1 MB

const batchWriteTextEncoder = new TextEncoder();

/**
 * The minimal shape validation needs. Both TCWriteOperation (client/TC wire type) and the
 * client-facing operations input satisfy this structurally.
 */
export type TransactWriteOperationLike = {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;
	operation: TransactionOperationType;
	data?: Uint8Array | string;
};

export type ItemKeyLike = {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;
};

export type BatchWriteOperationLike = ItemKeyLike & {
	operation: string;
	data?: Uint8Array | string;
	ttlSeconds?: number;
	ttlEpochUTCSeconds?: number;
	conditions?: unknown;
};

export type EncodedBatchWriteOperationLike = {
	operation: string;
	hashKey: KeyBytes;
	sortKey: KeyBytes;
	data?: Uint8Array | string;
	ttlSeconds?: number;
	ttlEpochUTCSeconds?: number;
};

function isEmptyKey(k: string | Uint8Array): boolean {
	return typeof k === "string" ? k.length === 0 : k.byteLength === 0;
}

export function payloadBytes(data: Uint8Array | string | undefined): number {
	if (data === undefined) return 0;
	return typeof data === "string" ? data.length * 2 : data.byteLength;
}

function validateItemCount(count: number, max: number, emptyMessage: string, tooManyMessage: string): void {
	if (count === 0) {
		throw new Error(emptyMessage);
	}
	if (count > max) {
		throw new Error(tooManyMessage);
	}
}

function encodedKeyParts(item: ItemKeyLike) {
	validateItemKeys(item.hashKey, item.sortKey);
	const hashKey = KeyCodec.encode(item.hashKey);
	const sortKey = KeyCodec.encodeOptional(item.sortKey);
	return { hashKey, sortKey };
}

export function encodedPairIdentity(item: ItemKeyLike): string {
	const { hashKey, sortKey } = encodedKeyParts(item);
	return `${hashKey.byteLength}:${hashKey.toBase64({ alphabet: "base64url" })}|${sortKey.byteLength}:${sortKey.toBase64({ alphabet: "base64url" })}`;
}

function validateNoDuplicateKeys(items: readonly ItemKeyLike[], duplicateMessage: (item: ItemKeyLike) => string): void {
	const seen = new Set<string>();
	for (const item of items) {
		const identity = encodedPairIdentity(item);
		if (seen.has(identity)) {
			throw new Error(duplicateMessage(item));
		}
		seen.add(identity);
	}
}

export function keyForError(item: ItemKeyLike): { hashKey: string; sortKey: string } {
	return {
		hashKey: KeyCodec.keyForLog(KeyCodec.encode(item.hashKey)),
		sortKey: item.sortKey ? KeyCodec.keyForLog(KeyCodec.encode(item.sortKey)) : "",
	};
}

export function estimateEncodedKeyPayloadBytes(item: ItemKeyLike): number {
	const { hashKey, sortKey } = encodedKeyParts(item);
	return hashKey.byteLength + sortKey.byteLength;
}

function batchWriteDataBytes(data: Uint8Array | string): number {
	return typeof data === "string" ? batchWriteTextEncoder.encode(data).byteLength : data.byteLength;
}

export function estimateBatchWriteForwardedOperationBytes(op: BatchWriteOperationLike): number {
	const { hashKey, sortKey } = encodedKeyParts(op);
	return estimateEncodedBatchWriteForwardedOperationBytes({ ...op, hashKey, sortKey });
}

export function estimateEncodedBatchWriteForwardedOperationBytes(op: EncodedBatchWriteOperationLike): number {
	const operationOverheadBytes = 32;
	const keyBytes = op.hashKey.byteLength + op.sortKey.byteLength;
	if (op.operation === "delete") return operationOverheadBytes + keyBytes;
	const ttlBytes = op.ttlSeconds !== undefined || op.ttlEpochUTCSeconds !== undefined ? 16 : 0;
	return operationOverheadBytes + ttlBytes + keyBytes + batchWriteDataBytes(op.data ?? "");
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
	validateItemCount(
		ops.length,
		MAX_ITEMS_PER_TRANSACTION,
		"fokos: transactWriteItems requires at least 1 item",
		`fokos: transactWriteItems supports at most ${MAX_ITEMS_PER_TRANSACTION} items`,
	);
	const seen = new Set<string>();
	let totalBytes = 0;
	for (const op of ops) {
		validateItemKeys(op.hashKey, op.sortKey);
		if (op.operation === "put" && op.data == null) {
			const { hashKey, sortKey } = keyForError(op);
			throw new Error(`fokos: transactWriteItems "put" operation requires data (${hashKey}${op.sortKey ? `, ${sortKey}` : ""})`);
		}
		const identity = encodedPairIdentity(op);
		if (seen.has(identity)) {
			const { hashKey, sortKey } = keyForError(op);
			throw new Error(`fokos: transactWriteItems duplicate key (${hashKey}, ${sortKey})`);
		}
		seen.add(identity);
		totalBytes += payloadBytes(op.data);
	}
	if (totalBytes > MAX_PAYLOAD_BYTES) {
		throw new Error(`fokos: transactWriteItems total payload exceeds ${MAX_PAYLOAD_BYTES / (1024 * 1024)} MB`);
	}
}

export function validateBatchGetItems(items: readonly ItemKeyLike[]): void {
	validateItemCount(
		items.length,
		MAX_BATCH_GET_ITEMS,
		"fokos: batchGetItems requires at least 1 item",
		`fokos: batchGetItems supports at most ${MAX_BATCH_GET_ITEMS} items`,
	);
	validateNoDuplicateKeys(items, (item) => {
		const { hashKey, sortKey } = keyForError(item);
		return `fokos: batchGetItems duplicate key (${hashKey}, ${sortKey})`;
	});
	const totalBytes = items.reduce((sum, item) => sum + estimateEncodedKeyPayloadBytes(item), 0);
	if (totalBytes > MAX_BATCH_PAYLOAD_BYTES) {
		throw new Error(`fokos: batchGetItems total payload exceeds ${MAX_BATCH_PAYLOAD_BYTES / (1024 * 1024)} MB`);
	}
}

export function validateBatchWriteOperations(ops: readonly BatchWriteOperationLike[]): void {
	validateItemCount(
		ops.length,
		MAX_BATCH_WRITE_ITEMS,
		"fokos: batchWriteItems requires at least 1 item",
		`fokos: batchWriteItems supports at most ${MAX_BATCH_WRITE_ITEMS} items`,
	);
	let totalBytes = 0;
	for (const op of ops) {
		validateItemKeys(op.hashKey, op.sortKey);
		if (op.operation !== "put" && op.operation !== "delete") {
			throw new Error(`fokos: batchWriteItems operation must be "put" or "delete" (got ${JSON.stringify(op.operation)})`);
		}
		if ("conditions" in op && op.conditions !== undefined) {
			throw new Error("fokos: batchWriteItems does not support conditions; use transactWriteItems for conditional writes");
		}
		if (op.operation === "put" && op.data == null) {
			const { hashKey, sortKey } = keyForError(op);
			throw new Error(`fokos: batchWriteItems "put" operation requires data (${hashKey}${op.sortKey ? `, ${sortKey}` : ""})`);
		}
		const forwardedBytes = estimateBatchWriteForwardedOperationBytes(op);
		if (forwardedBytes > MAX_BATCH_FORWARDED_SUB_BATCH_BYTES) {
			throw new Error(
				`fokos: batchWriteItems operation payload exceeds ${MAX_BATCH_FORWARDED_SUB_BATCH_BYTES / (1024 * 1024)} MB forwarded sub-batch limit`,
			);
		}
		totalBytes += estimateEncodedKeyPayloadBytes(op) + payloadBytes(op.data);
	}
	validateNoDuplicateKeys(ops, (op) => {
		const { hashKey, sortKey } = keyForError(op);
		return `fokos: batchWriteItems duplicate key (${hashKey}, ${sortKey})`;
	});
	if (totalBytes > MAX_BATCH_PAYLOAD_BYTES) {
		throw new Error(`fokos: batchWriteItems total payload exceeds ${MAX_BATCH_PAYLOAD_BYTES / (1024 * 1024)} MB`);
	}
}
