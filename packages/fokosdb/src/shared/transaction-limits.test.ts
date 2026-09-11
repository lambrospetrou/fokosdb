import { describe, expect, it } from "vitest";
import {
	MAX_CLIENT_REQUEST_TOKEN_BYTES,
	MAX_ITEM_BYTES,
	MAX_ITEMS_PER_TX,
	MAX_PAYLOAD_BYTES_PER_TX,
	validateClientRequestToken,
	validateItemKeys,
	validateTransactGetItemCount,
	validateTransactWriteOperations,
	type TransactWriteOperationLike,
} from "./transaction-limits.js";
import { KeyCodec } from "./partition-topology/key-codec.js";
import { compileConditionExpression, compileUpdateExpression } from "./expression/compiler.js";
import { fokosErrorWith } from "../../test/errors-matchers.js";

const validate = (ops: readonly TransactWriteOperationLike[]) => validateTransactWriteOperations(ops);
const itemExists = compileConditionExpression({ op: "exists", args: [{ ref: "hashKey" }] });

function putOp(hashKey: string, sortKey?: string, data: Uint8Array | string = "x"): TransactWriteOperationLike {
	return { hashKey, sortKey, operation: "put", data };
}

describe("validateClientRequestToken", () => {
	it("accepts exactly 64 UTF-8 bytes and rejects one more", () => {
		expect(() => validateClientRequestToken("é".repeat(MAX_CLIENT_REQUEST_TOKEN_BYTES / 2))).not.toThrow();
		expect(() => validateClientRequestToken(`${"é".repeat(MAX_CLIENT_REQUEST_TOKEN_BYTES / 2)}x`)).toThrow(
			fokosErrorWith("client_request_token_invalid", { limitBytes: 64 }),
		);
	});

	it("rejects an empty or whitespace-only token", () => {
		expect(() => validateClientRequestToken("")).toThrow(fokosErrorWith("client_request_token_invalid"));
		expect(() => validateClientRequestToken(" \t ")).toThrow(fokosErrorWith("client_request_token_invalid"));
	});
});

describe("validateItemKeys", () => {
	it("accepts ordinary keys", () => {
		expect(() => validateItemKeys("hk", "sk")).not.toThrow();
		expect(() => validateItemKeys("hk")).not.toThrow();
		expect(() => validateItemKeys("hk")).not.toThrow();
	});

	it("rejects a NUL character anywhere in the hashKey", () => {
		expect(() => validateItemKeys("\0hk")).toThrow(fokosErrorWith("key_contains_nul", { key: "hashKey" }));
		expect(() => validateItemKeys("h\0k")).toThrow(fokosErrorWith("key_contains_nul", { key: "hashKey" }));
		expect(() => validateItemKeys("hk\0")).toThrow(fokosErrorWith("key_contains_nul", { key: "hashKey" }));
	});

	it("rejects a NUL character anywhere in the sortKey", () => {
		expect(() => validateItemKeys("hk", "\0sk")).toThrow(fokosErrorWith("key_contains_nul", { key: "sortKey" }));
		expect(() => validateItemKeys("hk", "s\0k")).toThrow(fokosErrorWith("key_contains_nul", { key: "sortKey" }));
		expect(() => validateItemKeys("hk", "sk\0")).toThrow(fokosErrorWith("key_contains_nul", { key: "sortKey" }));
	});
});

describe("validateTransactWriteOperations", () => {
	it("rejects NUL characters in operation keys", () => {
		expect(() => validate([putOp("h\0k")])).toThrow(fokosErrorWith("key_contains_nul", { key: "hashKey" }));
		expect(() => validate([putOp("hk", "s\0k")])).toThrow(fokosErrorWith("key_contains_nul", { key: "sortKey" }));
	});

	it("accepts a typical valid operation set", () => {
		expect(() =>
			validate([
				putOp("a"),
				putOp("a", "s1"),
				{ hashKey: "b", operation: "delete" },
				{ hashKey: "c", sortKey: "s", operation: "check", condition: itemExists },
			]),
		).not.toThrow();
	});

	it("rejects an empty operation set", () => {
		expect(() => validate([])).toThrow(fokosErrorWith("transact_items_empty"));
	});

	it("accepts exactly the max item count and rejects one more", () => {
		const ops = Array.from({ length: MAX_ITEMS_PER_TX }, (_, i) => putOp(`hk-${i}`));
		expect(() => validate(ops)).not.toThrow();
		expect(() => validate([...ops, putOp("one-too-many")])).toThrow(fokosErrorWith("transact_items_too_many", { limit: 100 }));
	});

	it("rejects duplicate (hashKey, sortKey) pairs", () => {
		expect(() => validate([putOp("a", "s"), putOp("a", "s")])).toThrow(fokosErrorWith("transact_duplicate_key"));
	});

	it("treats a missing sortKey as the empty sortKey for duplicate detection", () => {
		expect(() => validate([putOp("a"), putOp("a")])).toThrow(fokosErrorWith("transact_duplicate_key"));
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
		expect(() => validate([{ hashKey: "a", operation: "put" }])).toThrow(
			fokosErrorWith("transact_operation_fields_invalid", { operation: "put" }),
		);
	});

	it("allows delete and check without data", () => {
		expect(() =>
			validate([
				{ hashKey: "a", operation: "delete" },
				{ hashKey: "b", operation: "check", condition: itemExists },
			]),
		).not.toThrow();
	});

	it("rejects data on a delete or a check", () => {
		expect(() => validate([{ hashKey: "a", operation: "delete", data: "x" }])).toThrow(
			fokosErrorWith("transact_operation_fields_invalid", { operation: "delete" }),
		);
		expect(() => validate([{ hashKey: "a", operation: "check", condition: itemExists, data: "x" }])).toThrow(
			fokosErrorWith("transact_operation_fields_invalid", { operation: "check" }),
		);
		// An empty string is still a data field.
		expect(() => validate([{ hashKey: "a", operation: "delete", data: "" }])).toThrow(
			fokosErrorWith("transact_operation_fields_invalid", { operation: "delete" }),
		);
	});

	it("rejects a check without a condition", () => {
		expect(() => validate([{ hashKey: "a", operation: "check" }])).toThrow(
			fokosErrorWith("transact_operation_fields_invalid", { operation: "check" }),
		);
	});

	it("accepts a valid update operation with an update plan and no data", () => {
		const updatePlan = compileUpdateExpression([{ action: "set", target: { ref: "data", path: "$.status" }, value: { val: "active" } }]);
		expect(() => validate([{ hashKey: "a", operation: "update", update: updatePlan }])).not.toThrow();
	});

	it("rejects an update operation without an update plan", () => {
		expect(() => validate([{ hashKey: "a", operation: "update" }])).toThrow(
			fokosErrorWith("transact_operation_fields_invalid", { operation: "update" }),
		);
	});

	it("rejects an update operation carrying data", () => {
		const updatePlan = compileUpdateExpression([{ action: "set", target: { ref: "data", path: "$.status" }, value: { val: "active" } }]);
		expect(() => validate([{ hashKey: "a", operation: "update", update: updatePlan, data: "forbidden" }])).toThrow(
			fokosErrorWith("transact_operation_fields_invalid", { operation: "update" }),
		);
	});

	it("rejects a non-update operation carrying an update plan", () => {
		const updatePlan = compileUpdateExpression([{ action: "set", target: { ref: "data", path: "$.status" }, value: { val: "active" } }]);
		expect(() => validate([{ hashKey: "a", operation: "put", data: "x", update: updatePlan }])).toThrow(
			fokosErrorWith("transact_operation_fields_invalid", { operation: "put" }),
		);
		expect(() => validate([{ hashKey: "b", operation: "delete", update: updatePlan }])).toThrow(
			fokosErrorWith("transact_operation_fields_invalid", { operation: "delete" }),
		);
	});

	it("accepts an item at the per-item byte limit and rejects one over it", () => {
		expect(() => validate([putOp("a", undefined, new Uint8Array(MAX_ITEM_BYTES))])).not.toThrow();
		expect(() => validate([putOp("a", undefined, new Uint8Array(MAX_ITEM_BYTES + 1))])).toThrow(fokosErrorWith("item_data_too_large"));
	});

	// A string counts its UTF-16 length, a lower bound on the UTF-8 bytes it stores. So the check only
	// fires once the string is over the limit in code units — it never rejects text that would fit, and
	// text above U+07FF slips through until the exact accounting in itemDataBytes' FIXME lands.
	it("counts a string by its length, so it rejects only what is certainly over", () => {
		expect(() => validate([putOp("a", undefined, "x".repeat(MAX_ITEM_BYTES))])).not.toThrow();
		expect(() => validate([putOp("a", undefined, "x".repeat(MAX_ITEM_BYTES + 1))])).toThrow(fokosErrorWith("item_data_too_large"));
		// Three UTF-8 bytes per code unit, but only `length` is counted, so this is currently accepted.
		expect(() => validate([putOp("a", undefined, "日".repeat(MAX_ITEM_BYTES))])).not.toThrow();
	});

	// The per-item cap alone does not bound a transaction: MAX_ITEMS_PER_TRANSACTION items at the item
	// limit would be far over the transaction budget, so the total is still checked separately.
	it("sums payload bytes across operations", () => {
		const maxItem = new Uint8Array(MAX_ITEM_BYTES);
		const atLimit = Math.floor(MAX_PAYLOAD_BYTES_PER_TX / MAX_ITEM_BYTES); // 10 items → 4000 KB, under 4 MB
		const ops = Array.from({ length: atLimit }, (_, i) => putOp(`hk-${i}`, undefined, maxItem));
		expect(() => validate(ops)).not.toThrow();
		expect(() => validate([...ops, putOp("one-more", undefined, maxItem)])).toThrow(fokosErrorWith("transact_payload_too_large"));
	});

	it("includes serialized condition plans in the transaction payload", () => {
		const condition = { ...itemExists, identity: "x".repeat(52 * 1024) };
		const ops: TransactWriteOperationLike[] = Array.from({ length: 80 }, (_, i) => ({
			hashKey: `condition-${i}`,
			operation: "check",
			condition,
		}));
		expect(() => validate(ops)).toThrow(fokosErrorWith("transact_payload_too_large"));
	});
});

describe("validateTransactGetItemCount", () => {
	it("rejects an empty item set", () => {
		expect(() => validateTransactGetItemCount(0)).toThrow(fokosErrorWith("transact_items_empty"));
	});

	// The read fans out to every partition holding a key, twice (two-phase read), so it carries the
	// same cap as the write path.
	it("accepts exactly the max item count and rejects one more", () => {
		expect(() => validateTransactGetItemCount(MAX_ITEMS_PER_TX)).not.toThrow();
		expect(() => validateTransactGetItemCount(MAX_ITEMS_PER_TX + 1)).toThrow(fokosErrorWith("transact_items_too_many", { limit: 100 }));
	});
});
