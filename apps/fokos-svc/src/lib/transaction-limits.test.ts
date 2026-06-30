import { describe, expect, it } from "vitest";
import {
	MAX_BATCH_GET_ITEMS,
	MAX_BATCH_FORWARDED_SUB_BATCH_BYTES,
	MAX_BATCH_PAYLOAD_BYTES,
	MAX_BATCH_WRITE_ITEMS,
	MAX_ITEMS_PER_TRANSACTION,
	MAX_PAYLOAD_BYTES,
	estimateBatchWriteForwardedOperationBytes,
	estimateEncodedBatchWriteForwardedOperationBytes,
	validateBatchGetItems,
	validateBatchWriteOperations,
	validateItemKeys,
	validateTransactWriteOperations,
	type BatchWriteOperationLike,
	type TransactWriteOperationLike,
} from "./transaction-limits.js";
import { KeyCodec } from "./partition-topology/key-codec.js";

function putOp(hashKey: string, sortKey?: string, data: Uint8Array | string = "x"): TransactWriteOperationLike {
	return { hashKey, sortKey, operation: "put", data };
}

function batchPutOp(hashKey: string | Uint8Array, sortKey?: string | Uint8Array, data: Uint8Array | string = "x"): BatchWriteOperationLike {
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
		expect(() => validateTransactWriteOperations([putOp("a"), putOp("a")])).toThrow(/duplicate key/);
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

describe("validateBatchGetItems", () => {
	it("rejects an empty item list", () => {
		expect(() => validateBatchGetItems([])).toThrow(/batchGetItems requires at least 1 item/);
	});

	it("accepts exactly the max item count and rejects one more", () => {
		const items = Array.from({ length: MAX_BATCH_GET_ITEMS }, (_, i) => ({ hashKey: `hk-${i}` }));
		expect(() => validateBatchGetItems(items)).not.toThrow();
		expect(() => validateBatchGetItems([...items, { hashKey: "one-too-many" }])).toThrow(/batchGetItems supports at most 100 items/);
	});

	it("rejects duplicate keys, treating absent sortKey as the empty key", () => {
		expect(() => validateBatchGetItems([{ hashKey: "a" }, { hashKey: "a" }])).toThrow(/batchGetItems duplicate key/);
		expect(() =>
			validateBatchGetItems([
				{ hashKey: "a", sortKey: "s" },
				{ hashKey: "a", sortKey: "s" },
			]),
		).toThrow(/batchGetItems duplicate key/);
		expect(() => validateBatchGetItems([{ hashKey: "a" }, { hashKey: "a", sortKey: "s" }])).not.toThrow();
	});

	it("dedupes by encoded key bytes, not string coercion", () => {
		expect(() => validateBatchGetItems([{ hashKey: "ÿ" }, { hashKey: new Uint8Array([0xff]) }])).not.toThrow();
		expect(() =>
			validateBatchGetItems([
				{ hashKey: "a", sortKey: "ÿ" },
				{ hashKey: "a", sortKey: new Uint8Array([0xff]) },
			]),
		).not.toThrow();
	});

	it("rejects oversized encoded key payloads", () => {
		const largeKey = "x".repeat(MAX_BATCH_PAYLOAD_BYTES + 1);
		expect(() => validateBatchGetItems([{ hashKey: largeKey }])).toThrow(/batchGetItems total payload exceeds 4 MB/);
	});
});

describe("validateBatchWriteOperations", () => {
	it("rejects an empty operation list", () => {
		expect(() => validateBatchWriteOperations([])).toThrow(/batchWriteItems requires at least 1 item/);
	});

	it("accepts exactly the max item count and rejects one more", () => {
		const ops = Array.from({ length: MAX_BATCH_WRITE_ITEMS }, (_, i) => batchPutOp(`hk-${i}`));
		expect(() => validateBatchWriteOperations(ops)).not.toThrow();
		expect(() => validateBatchWriteOperations([...ops, batchPutOp("one-too-many")])).toThrow(/batchWriteItems supports at most 25 items/);
	});

	it("rejects duplicate keys, treating absent sortKey as the empty key", () => {
		expect(() => validateBatchWriteOperations([batchPutOp("a"), { hashKey: "a", operation: "delete" }])).toThrow(
			/batchWriteItems duplicate key/,
		);
		expect(() => validateBatchWriteOperations([batchPutOp("a", "s"), { hashKey: "a", sortKey: "s", operation: "delete" }])).toThrow(
			/batchWriteItems duplicate key/,
		);
		expect(() => validateBatchWriteOperations([batchPutOp("a"), batchPutOp("a", "s")])).not.toThrow();
	});

	it("dedupes by encoded key bytes, not string coercion", () => {
		expect(() => validateBatchWriteOperations([batchPutOp("ÿ"), batchPutOp(new Uint8Array([0xff]))])).not.toThrow();
		expect(() => validateBatchWriteOperations([batchPutOp("a", "ÿ"), batchPutOp("a", new Uint8Array([0xff]))])).not.toThrow();
	});

	it("rejects unsupported operations", () => {
		expect(() => validateBatchWriteOperations([{ hashKey: "a", operation: "check" }])).toThrow(/operation must be "put" or "delete"/);
	});

	it("rejects conditions", () => {
		expect(() => validateBatchWriteOperations([{ ...batchPutOp("a"), conditions: [{ type: "item_exists" }] }])).toThrow(
			/does not support conditions/,
		);
	});

	it("rejects a put without data", () => {
		expect(() => validateBatchWriteOperations([{ hashKey: "a", operation: "put" }])).toThrow(/"put" operation requires data/);
	});

	it("rejects oversized encoded payloads", () => {
		const belowForwardedLimit = new Uint8Array(MAX_BATCH_FORWARDED_SUB_BATCH_BYTES - 128);
		expect(() =>
			validateBatchWriteOperations(Array.from({ length: 5 }, (_, i) => batchPutOp(`hk-${i}`, undefined, belowForwardedLimit))),
		).toThrow(/batchWriteItems total payload exceeds 4 MB/);
	});

	it("rejects a single operation that cannot fit in one forwarded sub-batch", () => {
		expect(() =>
			validateBatchWriteOperations([batchPutOp("fits", undefined, new Uint8Array(MAX_BATCH_FORWARDED_SUB_BATCH_BYTES - 128))]),
		).not.toThrow();
		expect(() =>
			validateBatchWriteOperations([batchPutOp("too-large", undefined, new Uint8Array(MAX_BATCH_FORWARDED_SUB_BATCH_BYTES + 1))]),
		).toThrow(/batchWriteItems operation payload exceeds 1 MB forwarded sub-batch limit/);
	});

	it("uses the same forwarded byte estimate for raw public keys and encoded KeyBytes", () => {
		const rawOp = {
			operation: "put" as const,
			hashKey: "hash",
			sortKey: new Uint8Array([0xff]),
			data: "é",
			ttlSeconds: 10,
		};
		const encodedOp = {
			...rawOp,
			hashKey: KeyCodec.encode(rawOp.hashKey),
			sortKey: KeyCodec.encodeOptional(rawOp.sortKey),
		};

		expect(estimateEncodedBatchWriteForwardedOperationBytes(encodedOp)).toBe(estimateBatchWriteForwardedOperationBytes(rawOp));
	});
});
