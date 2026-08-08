import { describe, expect, it } from "vitest";
import {
	MAX_ITEMS_PER_TRANSACTION,
	MAX_PAYLOAD_BYTES,
	validateItemKeys,
	validateTransactWriteOperations,
	type TransactWriteOperationLike,
} from "./transaction-limits.js";
import { KeyCodec } from "./partition-topology/key-codec.js";

// The byte-size caps live in db.ts; the validator only needs canonical encoding for its duplicate
// detection and error messages, so these plain KeyCodec encoders are enough here.
const ENCODERS = { encodeHashKey: KeyCodec.encode, encodeSortKey: KeyCodec.encodeOptional };
const validate = (ops: readonly TransactWriteOperationLike[]) => validateTransactWriteOperations(ops, ENCODERS);

function putOp(hashKey: string, sortKey?: string, data: Uint8Array | string = "x"): TransactWriteOperationLike {
	return { hashKey, sortKey, operation: "put", data };
}

describe("validateItemKeys", () => {
	it("accepts ordinary keys", () => {
		expect(() => validateItemKeys("hk", "sk")).not.toThrow();
		expect(() => validateItemKeys("hk")).not.toThrow();
		expect(() => validateItemKeys("hk")).not.toThrow();
	});

	it("rejects a NUL character anywhere in the hashKey", () => {
		expect(() => validateItemKeys("\0hk")).toThrow(/hashKey must not contain the NUL/);
		expect(() => validateItemKeys("h\0k")).toThrow(/hashKey must not contain the NUL/);
		expect(() => validateItemKeys("hk\0")).toThrow(/hashKey must not contain the NUL/);
	});

	it("rejects a NUL character anywhere in the sortKey", () => {
		expect(() => validateItemKeys("hk", "\0sk")).toThrow(/sortKey must not contain the NUL/);
		expect(() => validateItemKeys("hk", "s\0k")).toThrow(/sortKey must not contain the NUL/);
		expect(() => validateItemKeys("hk", "sk\0")).toThrow(/sortKey must not contain the NUL/);
	});
});

describe("validateTransactWriteOperations", () => {
	it("rejects NUL characters in operation keys", () => {
		expect(() => validate([putOp("h\0k")])).toThrow(/hashKey must not contain the NUL/);
		expect(() => validate([putOp("hk", "s\0k")])).toThrow(/sortKey must not contain the NUL/);
	});

	it("accepts a typical valid operation set", () => {
		expect(() =>
			validate([putOp("a"), putOp("a", "s1"), { hashKey: "b", operation: "delete" }, { hashKey: "c", sortKey: "s", operation: "check" }]),
		).not.toThrow();
	});

	it("rejects an empty operation set", () => {
		expect(() => validate([])).toThrow(/at least 1 item/);
	});

	it("accepts exactly the max item count and rejects one more", () => {
		const ops = Array.from({ length: MAX_ITEMS_PER_TRANSACTION }, (_, i) => putOp(`hk-${i}`));
		expect(() => validate(ops)).not.toThrow();
		expect(() => validate([...ops, putOp("one-too-many")])).toThrow(/at most 100 items/);
	});

	it("rejects duplicate (hashKey, sortKey) pairs", () => {
		expect(() => validate([putOp("a", "s"), putOp("a", "s")])).toThrow(/duplicate key/);
	});

	it("treats a missing sortKey as the empty sortKey for duplicate detection", () => {
		expect(() => validate([putOp("a"), putOp("a")])).toThrow(/duplicate key/);
	});

	it("does not confuse a string sortKey with the binary sortKey that stringifies the same", () => {
		// These are two DISTINCT items: KeyCodec 0xFF-tags binary keys, so they encode differently.
		// A template-string identity would conflate them, because `${Uint8Array}` renders as a
		// comma-joined decimal list and both sides read "9,9". Duplicate detection must compare the
		// canonical bytes.
		const ops: TransactWriteOperationLike[] = [
			{ hashKey: "a", sortKey: "9,9", operation: "put", data: "x" },
			{ hashKey: "a", sortKey: new Uint8Array([9, 9]), operation: "put", data: "x" },
		];
		expect(() => validate(ops)).not.toThrow();
	});

	it("returns the canonical encoded keys in input order, so the caller never re-encodes", () => {
		const keys = validate([putOp("a", "s1"), putOp("b")]);
		expect(keys).toEqual([
			{ hashKey: KeyCodec.encode("a"), sortKey: KeyCodec.encode("s1") },
			// An absent sortKey encodes to the empty sentinel.
			{ hashKey: KeyCodec.encode("b"), sortKey: KeyCodec.encodeOptional(undefined) },
		]);
	});

	it("allows the same hashKey with different sortKeys", () => {
		expect(() => validate([putOp("a", "s1"), putOp("a", "s2")])).not.toThrow();
	});

	it("rejects a put without data", () => {
		expect(() => validate([{ hashKey: "a", operation: "put" }])).toThrow(/"put" operation requires data/);
	});

	it("allows delete and check without data", () => {
		expect(() =>
			validate([
				{ hashKey: "a", operation: "delete" },
				{ hashKey: "b", operation: "check" },
			]),
		).not.toThrow();
	});

	it("accepts a payload at the byte limit and rejects one over it", () => {
		// Uint8Array data counts byteLength; string data counts length * 2.
		expect(() => validate([putOp("a", undefined, new Uint8Array(MAX_PAYLOAD_BYTES))])).not.toThrow();
		expect(() => validate([putOp("a", undefined, new Uint8Array(MAX_PAYLOAD_BYTES + 1))])).toThrow(/total payload exceeds 4 MB/);
		expect(() => validate([putOp("a", undefined, "x".repeat(MAX_PAYLOAD_BYTES / 2 + 1))])).toThrow(/total payload exceeds 4 MB/);
	});

	it("sums payload bytes across operations", () => {
		const half = new Uint8Array(MAX_PAYLOAD_BYTES / 2);
		expect(() => validate([putOp("a", undefined, half), putOp("b", undefined, half)])).not.toThrow();
		expect(() => validate([putOp("a", undefined, half), putOp("b", undefined, half), putOp("c", undefined, "x")])).toThrow(
			/total payload exceeds 4 MB/,
		);
	});
});
