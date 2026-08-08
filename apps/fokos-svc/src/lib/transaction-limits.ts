/**
 * Shared transaction limits and validation for transact-write operations.
 * Single source of truth used by both the FokosDB client (db.ts) and the
 * TransactionCoordinatorDO — keep client-side and coordinator-side validation in lockstep.
 */

import type { TransactionOperationType } from "./transaction-types.js";
import { KeyCodec, type KeyBytes } from "./partition-topology/key-codec.js";

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
 * The caller's key encoders. Injected so the byte-size caps stay owned by the public boundary in
 * db.ts and this module stays agnostic to key-length policy — the same pattern
 * `normalizeSkInterval(sort, encodeSortKey)` uses.
 */
export type KeyEncoders = {
	encodeHashKey: (k: string | Uint8Array) => KeyBytes;
	encodeSortKey: (k: string | Uint8Array | undefined) => KeyBytes;
};

/**
 * Validates a transact-write operation set: valid keys, item count, duplicate keys, total payload
 * bytes, and that every "put" carries data. Throws on the first violation.
 *
 * Key policy checks run on the RAW public keys (NUL, lone surrogates). Each key is then encoded
 * EXACTLY ONCE, and the canonical bytes are returned in input order for the caller to reuse — so
 * `transactWriteItems` must build its operations from the returned bytes, never re-encode.
 */
export function validateTransactWriteOperations(
	ops: readonly TransactWriteOperationLike[],
	encoders: KeyEncoders,
): Array<{ hashKey: KeyBytes; sortKey: KeyBytes }> {
	if (ops.length === 0) {
		throw new Error("fokos: transactWriteItems requires at least 1 item");
	}
	if (ops.length > MAX_ITEMS_PER_TRANSACTION) {
		throw new Error(`fokos: transactWriteItems supports at most ${MAX_ITEMS_PER_TRANSACTION} items`);
	}
	const seen = new Set<bigint>();
	const encodedKeys: Array<{ hashKey: KeyBytes; sortKey: KeyBytes }> = [];
	let totalBytes = 0;
	for (const op of ops) {
		validateItemKeys(op.hashKey, op.sortKey);
		const hashKey = encoders.encodeHashKey(op.hashKey);
		const sortKey = encoders.encodeSortKey(op.sortKey);
		if (op.operation === "put" && op.data == null) {
			throw new Error(
				`fokos: transactWriteItems "put" operation requires data (${KeyCodec.keyForLog(hashKey)}${sortKey.byteLength > 0 ? `, ${KeyCodec.keyForLog(sortKey)}` : ""})`,
			);
		}
		// KeyCodec.pairKey is the ONE identity primitive for a (hashKey, sortKey) pair — the same one
		// commitLocal's keyset check and the TC's two-phase read pairing use.
		//
		// Do NOT substitute a template string built from the public keys: `${Uint8Array}` renders as a
		// comma-joined decimal list, so the string sortKey "9,9" and the binary sortKey [9,9] produce the
		// same text, and two distinct items (KeyCodec 0xFF-tags binary keys) would be rejected as a
		// duplicate. Identity must be taken over the canonical bytes.
		const identity = KeyCodec.pairKey(hashKey, sortKey);
		if (seen.has(identity)) {
			throw new Error(`fokos: transactWriteItems duplicate key (${KeyCodec.keyForLog(hashKey)}, ${KeyCodec.keyForLog(sortKey)})`);
		}
		seen.add(identity);
		if (op.data) {
			totalBytes += typeof op.data === "string" ? op.data.length * 2 : op.data.byteLength;
		}
		encodedKeys.push({ hashKey, sortKey });
	}
	if (totalBytes > MAX_PAYLOAD_BYTES) {
		throw new Error(`fokos: transactWriteItems total payload exceeds ${MAX_PAYLOAD_BYTES / (1024 * 1024)} MB`);
	}
	return encodedKeys;
}
