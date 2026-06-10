import { describe, expect, it } from "vitest";
import {
	MAX_ITEMS_PER_TRANSACTION,
	MAX_PAYLOAD_BYTES,
	validateItemKeys,
	validateTransactWriteOperations,
	type TransactWriteOperationLike,
} from "./transaction-limits.js";

function putOp(hashKey: string, sortKey?: string, data: Uint8Array | string = "x"): TransactWriteOperationLike {
	return { hashKey, sortKey, operation: "put", data };
}

describe("validateItemKeys", () => {
	it("accepts ordinary keys", () => {
		expect(() => validateItemKeys("hk", "sk")).not.toThrow();
		expect(() => validateItemKeys("hk")).not.toThrow();
		expect(() => validateItemKeys("hk", "")).not.toThrow();
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
		expect(() => validateTransactWriteOperations([putOp("h\0k")])).toThrow(/hashKey must not contain the NUL/);
		expect(() => validateTransactWriteOperations([putOp("hk", "s\0k")])).toThrow(/sortKey must not contain the NUL/);
	});

	it("accepts a typical valid operation set", () => {
		expect(() =>
			validateTransactWriteOperations([
				putOp("a"),
				putOp("a", "s1"),
				{ hashKey: "b", operation: "delete" },
				{ hashKey: "c", sortKey: "s", operation: "check" },
			]),
		).not.toThrow();
	});

	it("rejects an empty operation set", () => {
		expect(() => validateTransactWriteOperations([])).toThrow(/at least 1 item/);
	});

	it("accepts exactly the max item count and rejects one more", () => {
		const ops = Array.from({ length: MAX_ITEMS_PER_TRANSACTION }, (_, i) => putOp(`hk-${i}`));
		expect(() => validateTransactWriteOperations(ops)).not.toThrow();
		expect(() => validateTransactWriteOperations([...ops, putOp("one-too-many")])).toThrow(/at most 100 items/);
	});

	it("rejects duplicate (hashKey, sortKey) pairs", () => {
		expect(() => validateTransactWriteOperations([putOp("a", "s"), putOp("a", "s")])).toThrow(/duplicate key/);
	});

	it("treats a missing sortKey as the empty sortKey for duplicate detection", () => {
		expect(() => validateTransactWriteOperations([putOp("a"), putOp("a", "")])).toThrow(/duplicate key/);
	});

	it("allows the same hashKey with different sortKeys", () => {
		expect(() => validateTransactWriteOperations([putOp("a", "s1"), putOp("a", "s2")])).not.toThrow();
	});

	it("rejects a put without data", () => {
		expect(() => validateTransactWriteOperations([{ hashKey: "a", operation: "put" }])).toThrow(/"put" operation requires data/);
	});

	it("allows delete and check without data", () => {
		expect(() =>
			validateTransactWriteOperations([
				{ hashKey: "a", operation: "delete" },
				{ hashKey: "b", operation: "check" },
			]),
		).not.toThrow();
	});

	it("accepts a payload at the byte limit and rejects one over it", () => {
		// Uint8Array data counts byteLength; string data counts length * 2.
		expect(() => validateTransactWriteOperations([putOp("a", undefined, new Uint8Array(MAX_PAYLOAD_BYTES))])).not.toThrow();
		expect(() => validateTransactWriteOperations([putOp("a", undefined, new Uint8Array(MAX_PAYLOAD_BYTES + 1))])).toThrow(
			/total payload exceeds 4 MB/,
		);
		expect(() => validateTransactWriteOperations([putOp("a", undefined, "x".repeat(MAX_PAYLOAD_BYTES / 2 + 1))])).toThrow(
			/total payload exceeds 4 MB/,
		);
	});

	it("sums payload bytes across operations", () => {
		const half = new Uint8Array(MAX_PAYLOAD_BYTES / 2);
		expect(() => validateTransactWriteOperations([putOp("a", undefined, half), putOp("b", undefined, half)])).not.toThrow();
		expect(() =>
			validateTransactWriteOperations([putOp("a", undefined, half), putOp("b", undefined, half), putOp("c", undefined, "x")]),
		).toThrow(/total payload exceeds 4 MB/);
	});
});
