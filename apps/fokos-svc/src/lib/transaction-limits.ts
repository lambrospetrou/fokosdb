/**
 * The single home for item validation: key rules, key size caps, data size caps, and the
 * transaction count/payload caps. Every public path goes through these — putItem, getItem,
 * deleteItem, queryItems, transactWriteItems, transactGetItems — so a rule cannot apply through one
 * API and not another. Used by both the FokosDB client (db.ts) and the TransactionCoordinatorDO,
 * which keeps client-side and coordinator-side validation in lockstep.
 *
 * Encoding lives here too: a key's size cap is measured on the ENCODED bytes, so capping and
 * encoding are one step and cannot drift apart.
 */

import type { PartitionContextResolved } from "./partition-topology/partition-context.js";
import type { TransactionOperationType } from "./transaction-types.js";
import type { ItemCondition } from "./types.js";
import { KeyCodec, type KeyBytes } from "./partition-topology/key-codec.js";

// DynamoDB-style encoded-byte ceilings. Measured on KeyBytes (after UTF-8 encoding / 0xFF tagging).
// DynamoDB uses 2KB for hashKey and 1KB for sortKey.
// We start stricter and we can raise later.
export const MAX_HASH_KEY_BYTES = 1024;
export const MAX_SORT_KEY_BYTES = 512;

/**
 * Per-item data ceiling, DynamoDB parity. Applies to EVERY write path — `putItem` and each operation
 * in a transaction — so one item cannot be larger through one API than the other. Without it a
 * single transactional put could carry the whole 4 MB transaction budget while `putItem` had no
 * ceiling at all.
 */
export const MAX_ITEM_BYTES = 400 * 1024; // 400 KB

export const MAX_ITEMS_PER_TX = 100;
export const MAX_PAYLOAD_BYTES_PER_TX = 4 * 1024 * 1024; // 4 MB, summed over a transaction

/**
 * Lower bound on the stored size of one item's data: exact for binary, UTF-16 code units for text.
 *
 * A string's UTF-8 size is at least its `length` (every code unit is one or more bytes) and at most
 * `length * 3`, so this NEVER over-counts and the limits built on it never reject a string that
 * would have fit. The cost is the other direction: text above U+07FF is 3 UTF-8 bytes per code unit,
 * so a 400 KB check can admit 1.2 MB of CJK. The store measures the truth with `octet_length` when it
 * writes `est_row_bytes` (`partition/partition-store.ts`).
 *
 * FIXME: implement the real size accounting — exact UTF-8 length for values, and the KEYS counted
 * into the item's budget rather than capped separately, per
 * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html#limits-data-types
 */
export function itemDataBytes(data: Uint8Array | string): number {
	return typeof data === "string" ? data.length : data.byteLength;
}

/** Throws when a single item's data exceeds MAX_ITEM_BYTES. `where` names the calling API. */
export function validateItemDataSize(data: Uint8Array | string, where: string): void {
	const bytes = itemDataBytes(data);
	if (bytes > MAX_ITEM_BYTES) {
		throw new Error(`fokos: ${where} item data exceeds ${MAX_ITEM_BYTES / 1024} KB (got ${bytes})`);
	}
}

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
	conditions?: ItemCondition[];
};

function isEmptyKey(k: string | Uint8Array): boolean {
	return typeof k === "string" ? k.length === 0 : k.byteLength === 0;
}

/**
 * The content rules for one key, independent of whether it may be empty. Rejects:
 * - lone-surrogate strings (invalid UTF-16),
 * - the NUL character in STRING keys. Binary (Uint8Array) keys may contain any byte, including 0x00.
 *
 * Separate from `validateItemKeys` because a query's sort-key BOUND is not an item key: `begins_with`
 * accepts an empty prefix (it means "everything"), so the emptiness rule must not apply to it — but
 * the content rules must, or a key rejected on write would be accepted as a query bound.
 */
export function validateKeyContent(name: "hashKey" | "sortKey", k: string | Uint8Array): void {
	if (typeof k !== "string") return;
	if (k.includes("\0")) {
		throw new Error(`fokos: ${name} must not contain the NUL (\\0) character`);
	}
	if (k.isWellFormed?.() === false) {
		throw new Error(`fokos: ${name} string contains a lone surrogate (not well-formed UTF-16)`);
	}
}

/**
 * The single key-validation boundary, run on public keys before encoding. Adds to the content rules:
 * empty hashKey / empty sortKey are rejected (key attributes cannot be empty); an absent sortKey is
 * allowed.
 */
export function validateItemKeys(hashKey: string | Uint8Array, sortKey?: string | Uint8Array): void {
	if (isEmptyKey(hashKey)) {
		throw new Error("fokos: hashKey must not be empty");
	}
	if (sortKey !== undefined && isEmptyKey(sortKey)) {
		throw new Error("fokos: sortKey must not be empty (omit it for a single-key item)");
	}
	validateKeyContent("hashKey", hashKey);
	if (sortKey !== undefined) {
		validateKeyContent("sortKey", sortKey);
	}
}

/** Encodes a hash key to canonical bytes, enforcing the size cap on the encoded form. */
export function encodeHashKey(k: string | Uint8Array): KeyBytes {
	const bytes = KeyCodec.encode(k);
	if (bytes.byteLength > MAX_HASH_KEY_BYTES) {
		throw new Error(`fokos: hashKey exceeds ${MAX_HASH_KEY_BYTES} bytes when encoded (got ${bytes.byteLength})`);
	}
	return bytes;
}

/** Encodes a sort key to canonical bytes (absent ⇒ the empty sentinel), enforcing the size cap. */
export function encodeSortKey(k: string | Uint8Array | undefined): KeyBytes {
	if (k === undefined) return KeyCodec.encodeOptional(undefined);
	const bytes = KeyCodec.encode(k);
	if (bytes.byteLength > MAX_SORT_KEY_BYTES) {
		throw new Error(`fokos: sortKey exceeds ${MAX_SORT_KEY_BYTES} bytes when encoded (got ${bytes.byteLength})`);
	}
	return bytes;
}

/**
 * Encodes one sort-key BOUND of a query. Bounds get the content rules but not the emptiness rule,
 * and passing this to `normalizeSkInterval` checks every bound exactly once wherever that function
 * uses it (`between` and `range` each carry two).
 */
export function encodeSortBound(k: string | Uint8Array): KeyBytes {
	validateKeyContent("sortKey", k);
	return encodeSortKey(k);
}

/**
 * Validates a transact-write operation set: valid keys, item count, duplicate keys, total payload
 * bytes, and the per-operation rules of `TransactWriteItem` — "put" carries data, "delete" and
 * "check" carry none, "check" carries at least one condition. Throws on the first violation.
 *
 * Key policy checks run on the RAW public keys (NUL, lone surrogates). Each key is then encoded
 * EXACTLY ONCE, and the canonical bytes are returned in input order for the caller to reuse — so
 * `transactWriteItems` must build its operations from the returned bytes, never re-encode.
 */
export function validateTransactWriteOperations(
	ops: readonly TransactWriteOperationLike[],
): Array<{ hashKey: KeyBytes; sortKey: KeyBytes }> {
	if (ops.length === 0) {
		throw new Error("fokos: transactWriteItems requires at least 1 item");
	}
	if (ops.length > MAX_ITEMS_PER_TX) {
		throw new Error(`fokos: transactWriteItems supports at most ${MAX_ITEMS_PER_TX} items`);
	}
	const seen = new Set<bigint>();
	const encodedKeys: Array<{ hashKey: KeyBytes; sortKey: KeyBytes }> = [];
	let totalBytes = 0;
	for (const op of ops) {
		validateItemKeys(op.hashKey, op.sortKey);
		const hashKey = encodeHashKey(op.hashKey);
		const sortKey = encodeSortKey(op.sortKey);
		const at = KeyCodec.pairForLog(hashKey, sortKey);
		if (op.operation === "put") {
			if (op.data == null) {
				throw new Error(`fokos: transactWriteItems "put" operation requires data (${at})`);
			}
		} else if (op.data != null) {
			throw new Error(`fokos: transactWriteItems "${op.operation}" operation must not carry data (${at})`);
		}
		if (op.operation === "check" && !op.conditions?.length) {
			throw new Error(`fokos: transactWriteItems "check" operation requires at least one condition (${at})`);
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
			throw new Error(`fokos: transactWriteItems duplicate key (${at})`);
		}
		seen.add(identity);
		if (op.data) {
			validateItemDataSize(op.data, "transactWriteItems");
			totalBytes += itemDataBytes(op.data);
		}
		encodedKeys.push({ hashKey, sortKey });
	}
	if (totalBytes > MAX_PAYLOAD_BYTES_PER_TX) {
		throw new Error(`fokos: transactWriteItems total payload exceeds ${MAX_PAYLOAD_BYTES_PER_TX / (1024 * 1024)} MB`);
	}
	return encodedKeys;
}

/**
 * The read-side counterpart of the count checks above. `transactGetItems` fans out to every partition
 * holding a requested key and does it TWICE (the two-phase read), so an unbounded item list is an
 * unbounded fan-out — the same reason the write path is capped.
 *
 * Keys are validated per item by the caller as it encodes them; this runs first so an oversized
 * request fails before any of that work.
 */
export function validateTransactGetItemCount(itemCount: number): void {
	if (itemCount === 0) {
		throw new Error("fokos: transactGetItems requires at least 1 item");
	}
	if (itemCount > MAX_ITEMS_PER_TX) {
		throw new Error(`fokos: transactGetItems supports at most ${MAX_ITEMS_PER_TX} items`);
	}
}

/**
 * The single-partition eligibility hint used by the transaction fast paths: returns the shared
 * partition context when EVERY item resolves to the same PartitionDO, else null.
 *
 * This is a client-side hint only. It is necessary but not sufficient — the resolved context names
 * the partition at the top of a forwarding chain, and a split or a promotion below that node can
 * still spread the items over several DOs. The partition itself is the authority and raises a
 * fallback error when it cannot execute the whole set alone.
 */
export function singlePartitionTarget<T extends { partitionContext: PartitionContextResolved }>(
	items: readonly T[],
): PartitionContextResolved | null {
	if (items.length === 0) return null;
	const target = items[0].partitionContext;
	for (const item of items) {
		if (item.partitionContext.doName !== target.doName) return null;
	}
	return target;
}
