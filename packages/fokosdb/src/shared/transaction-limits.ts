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
import type { ParticipantOperationResultEncoded, RejectionReasonEncoded, TransactionOperationType } from "./transaction-types.js";
import type { CompiledConditionPlan, CompiledUpdatePlan } from "./expression/plan.js";
import type { DataKind, ReturnValuesOnConditionCheckFailure } from "./types.js";
import { KeyCodec, type KeyBytes } from "./partition-topology/key-codec.js";
import { FokosValidationError, VALIDATION_CODES } from "./errors.js";

// DynamoDB-style encoded-byte ceilings. Measured on KeyBytes (after UTF-8 encoding / 0xFF tagging).
// DynamoDB uses 2KB for hashKey and 1KB for sortKey. These limits are stricter and can go up later.
export const MAX_HASH_KEY_BYTES = 1024;
export const MAX_SORT_KEY_BYTES = 512;

/**
 * Per-item ceiling, DynamoDB parity. Applies to EVERY write path — `putItem` and each operation in a
 * transaction — so one item cannot be larger through one API than the other. Without it a single
 * transactional put could carry the whole 4 MB transaction budget while `putItem` had no ceiling at
 * all.
 *
 * Two checks hold it, and they measure different things. `itemDataBytes` counts a client's data as a
 * lower bound, before the request leaves. The store measures the STORED row — the encoded data plus
 * both keys plus the fixed per-row overhead — and that one is the truth, because an update writes
 * bytes that no client ever sent. A write between the two ceilings therefore passes validation and is
 * rejected by the partition, which is the intended order: the client check is a cheap early answer,
 * not the definition.
 */
export const MAX_ITEM_BYTES = 400 * 1024; // 400 KB

export const MAX_ITEMS_PER_TX = 100;
export const MAX_PAYLOAD_BYTES_PER_TX = 4 * 1024 * 1024; // 4 MB, summed over a transaction
export const MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX = 10 * 1024 * 1024; // 10 MiB
/**
 * The size above which a coordinator refuses to start a new transaction. It is half of the 10 GB a
 * Durable Object holds, and the other half is headroom the refusal needs to be useful: a coordinator
 * that stopped taking work must still drive every transaction it already accepted to a terminal
 * state, and each of those writes state before it sends its outbound RPCs. A guard near the ceiling
 * would refuse new work and then wedge on the old.
 *
 * FIXME: Implement coordinator auto scaling out.
 */
export const MAX_TC_DATABASE_BYTES = 5 * 1024 * 1024 * 1024; // 5 GB
export const MAX_CLIENT_REQUEST_TOKEN_BYTES = 64;
export const IDEMPOTENCY_WINDOW_MS = 10 * 60 * 1000;
export const SWEEP_BATCH_ROWS = 1_000;
export const ALARM_RECOVERY_BUDGET_MS = 30_000;

const textEncoder = new TextEncoder();

export function validateClientRequestToken(token: string): void {
	if (token.trim().length === 0) {
		throw new FokosValidationError(VALIDATION_CODES.client_request_token_invalid, {
			message: "clientRequestToken must be a non-empty string when provided",
		});
	}
	const bytes = textEncoder.encode(token).byteLength;
	if (bytes > MAX_CLIENT_REQUEST_TOKEN_BYTES) {
		throw new FokosValidationError(VALIDATION_CODES.client_request_token_invalid, {
			message: `clientRequestToken exceeds ${MAX_CLIENT_REQUEST_TOKEN_BYTES} bytes when UTF-8 encoded`,
			attributes: { limitBytes: MAX_CLIENT_REQUEST_TOKEN_BYTES, bytes },
		});
	}
}

/**
 * Lower bound on the stored size of one item's data: exact for binary, UTF-16 code units for text.
 *
 * A string's UTF-8 size is at least its `length` (every code unit is one or more bytes) and at most
 * `length * 3`, so this NEVER over-counts and the limits built on it never reject a string that
 * would have fit. The cost is the other direction: text above U+07FF is 3 UTF-8 bytes per code unit,
 * so a 400 KB check can admit 1.2 MB of CJK.
 *
 * Being a lower bound is what makes it safe to keep. The store measures the truth with `octet_length`
 * over the value it writes, and it counts both keys and the per-row overhead as DynamoDB does
 * (`estRowBytesExpr` in `partition/item-size.ts`). Every transactional write path measures that exact
 * size in its check pass before it writes anything, so this function only has to avoid rejecting an
 * item the store would have accepted. It never has to agree with the store.
 */
export function itemDataBytes(data: Uint8Array | string): number {
	return typeof data === "string" ? data.length : data.byteLength;
}

/** Throws when a single item's data exceeds MAX_ITEM_BYTES. `where` names the calling API. */
export function validateItemDataSize(data: Uint8Array | string, where: string): void {
	const bytes = itemDataBytes(data);
	if (bytes > MAX_ITEM_BYTES) {
		throw new FokosValidationError(VALIDATION_CODES.item_data_too_large, {
			message: `item data exceeds ${MAX_ITEM_BYTES / 1024} KB`,
			attributes: { api: where, limitBytes: MAX_ITEM_BYTES, bytes },
		});
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
	condition?: CompiledConditionPlan;
	update?: CompiledUpdatePlan;
	returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure;
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
		throw new FokosValidationError(VALIDATION_CODES.key_contains_nul, {
			message: `${name} must not contain the NUL (\\0) character`,
			attributes: { key: name },
		});
	}
	if (k.isWellFormed?.() === false) {
		throw new FokosValidationError(VALIDATION_CODES.key_not_well_formed_utf16, {
			message: `${name} string contains a lone surrogate (not well-formed UTF-16)`,
			attributes: { key: name },
		});
	}
}

/**
 * The single key-validation boundary, run on public keys before encoding. Adds to the content rules:
 * empty hashKey / empty sortKey are rejected (key attributes cannot be empty); an absent sortKey is
 * allowed.
 */
export function validateItemKeys(hashKey: string | Uint8Array, sortKey?: string | Uint8Array): void {
	if (isEmptyKey(hashKey)) {
		throw new FokosValidationError(VALIDATION_CODES.hash_key_empty, { message: "hashKey must not be empty" });
	}
	if (sortKey !== undefined && isEmptyKey(sortKey)) {
		throw new FokosValidationError(VALIDATION_CODES.sort_key_empty, {
			message: "sortKey must not be empty (omit it for a hash-key only item)",
		});
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
		throw new FokosValidationError(VALIDATION_CODES.hash_key_too_large, {
			message: `hashKey exceeds ${MAX_HASH_KEY_BYTES} bytes when encoded`,
			attributes: { limitBytes: MAX_HASH_KEY_BYTES, bytes: bytes.byteLength },
		});
	}
	return bytes;
}

/** Encodes a sort key to canonical bytes (absent ⇒ the empty sentinel), enforcing the size cap. */
export function encodeSortKey(k: string | Uint8Array | undefined): KeyBytes {
	if (k === undefined) return KeyCodec.encodeOptional(undefined);
	const bytes = KeyCodec.encode(k);
	if (bytes.byteLength > MAX_SORT_KEY_BYTES) {
		throw new FokosValidationError(VALIDATION_CODES.sort_key_too_large, {
			message: `sortKey exceeds ${MAX_SORT_KEY_BYTES} bytes when encoded`,
			attributes: { limitBytes: MAX_SORT_KEY_BYTES, bytes: bytes.byteLength },
		});
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
		throw new FokosValidationError(VALIDATION_CODES.transact_items_empty, { message: "transactWriteItems requires at least 1 item" });
	}
	if (ops.length > MAX_ITEMS_PER_TX) {
		throw new FokosValidationError(VALIDATION_CODES.transact_items_too_many, {
			message: `transactWriteItems supports at most ${MAX_ITEMS_PER_TX} items`,
			attributes: { limit: MAX_ITEMS_PER_TX, count: ops.length },
		});
	}
	const seen = new Set<bigint>();
	const encodedKeys: Array<{ hashKey: KeyBytes; sortKey: KeyBytes }> = [];
	let totalBytes = 0;
	for (const [opIndex, op] of ops.entries()) {
		validateItemKeys(op.hashKey, op.sortKey);
		const hashKey = encodeHashKey(op.hashKey);
		const sortKey = encodeSortKey(op.sortKey);
		const invalidFields = (message: string) =>
			new FokosValidationError(VALIDATION_CODES.transact_operation_fields_invalid, {
				message,
				attributes: { opIndex, operation: op.operation, hashKey: op.hashKey, sortKey: op.sortKey },
			});
		if (op.operation === "put") {
			if (op.data == null) throw invalidFields(`transactWriteItems "put" operation requires data`);
		} else if (op.data != null) {
			throw invalidFields(`transactWriteItems "${op.operation}" operation must not carry data`);
		}
		if (op.operation === "check" && !op.condition) {
			throw invalidFields(`transactWriteItems "check" operation requires a condition`);
		}
		if (op.operation === "update" && !op.update) {
			throw invalidFields(`transactWriteItems "update" operation requires an update plan`);
		}
		if (op.operation !== "update" && op.update) {
			throw invalidFields(`transactWriteItems "${op.operation}" operation must not carry an update plan`);
		}
		validateReturnValuesOnConditionCheckFailure(op.returnValuesOnConditionCheckFailure);
		// KeyCodec.pairKey is the ONE identity primitive for a (hashKey, sortKey) pair — the same one
		// commitLocal's keyset check and the TC's two-phase read pairing use.
		//
		// Do NOT substitute a template string built from the public keys: `${Uint8Array}` renders as a
		// comma-joined decimal list, so the string sortKey "9,9" and the binary sortKey [9,9] produce the
		// same text, and two distinct items (KeyCodec 0xFF-tags binary keys) would be rejected as a
		// duplicate. Identity must be taken over the canonical bytes.
		const identity = KeyCodec.pairKey(hashKey, sortKey);
		if (seen.has(identity)) {
			throw new FokosValidationError(VALIDATION_CODES.transact_duplicate_key, {
				message: "transactWriteItems duplicate key",
				attributes: { opIndex, hashKey: op.hashKey, sortKey: op.sortKey },
			});
		}
		seen.add(identity);
		if (op.data !== undefined) {
			validateItemDataSize(op.data, "transactWriteItems");
			totalBytes += itemDataBytes(op.data);
		}
		if (op.condition) totalBytes += itemDataBytes(JSON.stringify(op.condition));
		if (op.update) totalBytes += itemDataBytes(JSON.stringify(op.update));
		encodedKeys.push({ hashKey, sortKey });
	}
	if (totalBytes > MAX_PAYLOAD_BYTES_PER_TX) {
		throw new FokosValidationError(VALIDATION_CODES.transact_payload_too_large, {
			message: `transactWriteItems total payload exceeds ${MAX_PAYLOAD_BYTES_PER_TX / (1024 * 1024)} MB`,
			attributes: { limitBytes: MAX_PAYLOAD_BYTES_PER_TX, bytes: totalBytes },
		});
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
		throw new FokosValidationError(VALIDATION_CODES.transact_items_empty, { message: "transactGetItems requires at least 1 item" });
	}
	if (itemCount > MAX_ITEMS_PER_TX) {
		throw new FokosValidationError(VALIDATION_CODES.transact_items_too_many, {
			message: `transactGetItems supports at most ${MAX_ITEMS_PER_TX} items`,
			attributes: { limit: MAX_ITEMS_PER_TX, count: itemCount },
		});
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

export function validateReturnValuesOnConditionCheckFailure(value?: string): void {
	if (value !== undefined && value !== "none" && value !== "all_old") {
		throw new FokosValidationError(VALIDATION_CODES.return_values_option_invalid, {
			message: "returnValuesOnConditionCheckFailure must be 'none' or 'all_old'",
			attributes: { value },
		});
	}
}

/**
 * The public form of an item's keys, for a result the caller reads. The empty sort-key sentinel maps
 * back to an absent sortKey, and an absent sortKey is left off the object entirely rather than
 * carried as an explicit `undefined`, so a result compares equal whichever path produced it.
 */
export function decodeItemKeys(hashKey: KeyBytes, sortKey: KeyBytes): { hashKey: string | Uint8Array; sortKey?: string | Uint8Array } {
	return {
		hashKey: KeyCodec.decode(hashKey),
		...(sortKey.length > 0 ? { sortKey: KeyCodec.decode(sortKey) } : {}),
	};
}

/**
 * The `condition_failed` reason, with the old item image when the caller asked for one and the row
 * exists. Every path that evaluates a condition — the item RPCs, a prepare, and a single-shot
 * transaction — builds the reason here, so the image carries the same keys as the reason that holds
 * it and no path can drift.
 *
 * `imageRow` is `PartitionStore.getItemImage().row`; its `imageBytes` is cap bookkeeping and belongs
 * on the result, not on the image, so this takes the image fields one by one.
 */
export function conditionFailedReason(
	keys: { hashKey: string | Uint8Array; sortKey?: string | Uint8Array },
	imageRow?: { data: string | Uint8Array; kind: DataKind; version: number; ttlAt?: number },
): RejectionReasonEncoded {
	if (!imageRow) return { code: "condition_failed", ...keys };
	return {
		code: "condition_failed",
		...keys,
		item: {
			...keys,
			data: imageRow.data,
			kind: imageRow.kind,
			version: imageRow.version,
			...(imageRow.ttlAt !== undefined ? { ttlAt: imageRow.ttlAt } : {}),
		},
	};
}

/**
 * Enforces MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX over one answer, in request order.
 *
 * Sorts by opIndex, then walks: an image that would take the running total above the cap is dropped
 * and marked, and so is every later image. A result that already carries `itemOmitted` was dropped
 * one level down, so it is left alone and its bytes are not counted — they are not being sent.
 *
 * `imageBytes` is the byte count the partition measured with the image itself, so every level caps
 * on one number and no two levels measure the same image differently.
 */
export function applyImageCap(
	results: ParticipantOperationResultEncoded[],
	cap: number = MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX,
): ParticipantOperationResultEncoded[] {
	results.sort((a, b) => a.opIndex - b.opIndex);
	let runningBytes = 0;
	let exceeded = false;
	for (const r of results) {
		if (r.outcome !== "rejected" || r.itemOmitted || r.imageBytes === undefined) continue;
		if (exceeded || runningBytes + r.imageBytes > cap) {
			exceeded = true;
			if (r.reason.code === "condition_failed") delete r.reason.item;
			r.itemOmitted = "response_too_large";
		} else {
			runningBytes += r.imageBytes;
		}
	}
	return results;
}
