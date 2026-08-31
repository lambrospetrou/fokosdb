/**
 * Fingerprint of a transact-write operation set, used to detect a client that reuses one
 * `clientRequestToken` for different work. Without it the coordinator replays the FIRST
 * transaction's outcome and the new operations are silently dropped — a "committed" for writes that
 * never happened.
 */

import { hash64 } from "./hash-primitives.js";
import type { TCWriteOperation } from "./transaction-types.js";

const U64_MASK = (1n << 64n) - 1n;

// Domain separation: nothing else in the codebase hashes with this seed, so an operation fingerprint
// can never be confused with a routing hash.
const OPERATIONS_SEED = 0x666f6b6f735f7478n; // "fokos_tx"

/**
 * Hashes one operation by CHAINING each field through xxhash's seed rather than concatenating the
 * fields into a buffer. `data` can be megabytes, and chaining hashes it where it already lives — no
 * copy, and no second pass over the payload.
 */
function hashOperation(op: TCWriteOperation): bigint {
	let h = hash64(op.hashKey, OPERATIONS_SEED);
	h = hash64(op.sortKey, h);
	// One token carrying the operation, the kind, and WHICH optional fields are present. Without the
	// presence flags an absent field and a present-but-empty one would chain identically, so
	// `data: ""` would fingerprint the same as no data at all.
	h = hash64(
		`${op.operation}|${op.kind ?? ""}|${op.data === undefined ? 0 : 1}${op.condition === undefined ? 0 : 1}${op.ttlAt === undefined ? 0 : 1}`,
		h,
	);
	if (op.data !== undefined) {
		// string → UTF-8 inside xxhash; Uint8Array → hashed in place. `kind` is already chained above,
		// so text "5" and the bytes 0x35 cannot collide.
		h = hash64(op.data, h);
	}
	if (op.ttlAt !== undefined) {
		h = hash64(String(op.ttlAt), h);
	}
	if (op.condition !== undefined) {
		h = hash64(op.condition.identity, h);
	}
	return h;
}

/**
 * Fingerprints a whole operation set as a 16-char hex string.
 *
 * The fold across operations is a wrapping ADD, which is commutative: the same items in a different
 * order are the same request and must produce the same fingerprint. Sorting first would give the
 * same property but costs a sorted copy of the set on every call, including the common path where
 * nothing is being compared.
 *
 * Hex, not INTEGER, because Durable Object SQL cannot bind a JS bigint ("Cannot convert a BigInt
 * value to a number") and `Number()` would silently drop precision above 2^53.
 *
 * xxHash64 is not cryptographic, which is the right trade here: this catches a client mistake, and a
 * deliberately crafted collision would only mislead the client that crafted it.
 */
export function hashTransactionOperations(ops: readonly TCWriteOperation[]): string {
	let total = 0n;
	for (const op of ops) {
		total = (total + hashOperation(op)) & U64_MASK;
	}
	// Final chain through the item count: it avalanches the plain sum and pins the set size, so a set
	// whose members happen to sum to another set's total is still distinguished.
	return hash64(`${ops.length}`, total).toString(16).padStart(16, "0");
}
