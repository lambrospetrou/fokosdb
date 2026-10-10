import { describe, expect, it, vi } from "vitest";
import {
	DEFAULT_LIMITS,
	encodeHashKey,
	encodeSortKey,
	MAX_CLIENT_REQUEST_TOKEN_BYTES,
	MAX_HASH_KEY_BYTES,
	MAX_SORT_KEY_BYTES,
	resolveLimits,
	type FokosDBLimitOverrides,
	MAX_ITEM_BYTES,
	MAX_ITEMS_PER_TX,
	MAX_PAYLOAD_BYTES_PER_TX,
	validateClientRequestToken,
	validateItemKeys,
	validateTransactGetItemCount,
	validateTransactGetItemKeys,
	validateTransactWriteOperations,
	validateVersionReferences,
	type TransactWriteOperationLike,
} from "./transaction-limits.js";
import { KeyCodec } from "../sharding/key-codec.js";
import { conditionTree, updateTree } from "./expression/test-fixtures.js";
import { fokosErrorWith } from "../../test/errors-matchers.js";

const validate = (ops: readonly TransactWriteOperationLike[]) => validateTransactWriteOperations(ops, DEFAULT_LIMITS);
const itemExists = conditionTree({ op: "exists", args: [{ ref: "hashKey" }] });

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

	// The hash part of KeyPairMap answers above 8 keys. Only equal bytes make a duplicate.
	it("accepts more than 8 keys that differ only in their last byte, and rejects a repeat of one", () => {
		const ops = Array.from({ length: 12 }, (_, i) => putOp("a", `sort-key-${String.fromCharCode(65 + i)}`));
		expect(() => validate(ops)).not.toThrow();
		expect(() => validate([...ops, putOp("a", "sort-key-E")])).toThrow(fokosErrorWith("transact_duplicate_key", { opIndex: 12 }));
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
		const updatePlan = updateTree([{ action: "set", target: { ref: "data", path: "$.status" }, value: { val: "active" } }]);
		expect(() => validate([{ hashKey: "a", operation: "update", update: updatePlan }])).not.toThrow();
	});

	it("rejects an update operation without an update plan", () => {
		expect(() => validate([{ hashKey: "a", operation: "update" }])).toThrow(
			fokosErrorWith("transact_operation_fields_invalid", { operation: "update" }),
		);
	});

	it("rejects an update operation carrying data", () => {
		const updatePlan = updateTree([{ action: "set", target: { ref: "data", path: "$.status" }, value: { val: "active" } }]);
		expect(() => validate([{ hashKey: "a", operation: "update", update: updatePlan, data: "forbidden" }])).toThrow(
			fokosErrorWith("transact_operation_fields_invalid", { operation: "update" }),
		);
	});

	it("rejects a non-update operation carrying an update plan", () => {
		const updatePlan = updateTree([{ action: "set", target: { ref: "data", path: "$.status" }, value: { val: "active" } }]);
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

	it("counts the JSON bytes of an expression tree in the transaction payload", () => {
		const condition = { op: "eq", args: [{ ref: "data", path: "$.text" }, { val: "x".repeat(50 * 1024) }] } as const;
		const update = [{ action: "set", target: { ref: "data", path: "$.text" }, value: { val: "y".repeat(50 * 1024) } }] as const;
		// Ten items at the item limit leave 96 KB of the transaction payload.
		const data = "d".repeat(MAX_ITEM_BYTES);
		const ops: TransactWriteOperationLike[] = Array.from({ length: 10 }, (_, i) => putOp(`tree-${i}`, undefined, data));
		expect(() => validate(ops)).not.toThrow();
		const treeBytes = JSON.stringify(condition).length + JSON.stringify(update).length;
		expect(() => validate([...ops, { hashKey: "tree-update", operation: "update", condition, update }])).toThrow(
			fokosErrorWith("transact_payload_too_large", { bytes: 10 * data.length + treeBytes }),
		);
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

describe("validateTransactGetItemKeys", () => {
	const key = (hashKey: string, sortKey = "") => ({
		hashKey: KeyCodec.encode(hashKey),
		sortKey: KeyCodec.encodeOptional(sortKey || undefined),
	});

	it("rejects two items that name the same key, with the decoded keys on the error", () => {
		expect(() => validateTransactGetItemKeys([key("a", "s1"), key("b"), key("a", "s1")])).toThrow(
			fokosErrorWith("transact_duplicate_key", { itemIndex: 2, hashKey: "a", sortKey: "s1" }),
		);
	});

	it("treats a missing sortKey as the empty sortKey for duplicate detection", () => {
		expect(() =>
			validateTransactGetItemKeys([
				{ hashKey: KeyCodec.encode("a"), sortKey: KeyCodec.encodeOptional(undefined) },
				{ hashKey: KeyCodec.encode("a"), sortKey: KeyCodec.encodeOptional(undefined) },
			]),
		).toThrow(fokosErrorWith("transact_duplicate_key"));
	});

	it("allows the same hashKey with different sortKeys", () => {
		expect(() => validateTransactGetItemKeys([key("a", "s1"), key("a", "s2"), key("b")])).not.toThrow();
	});
});

describe("resolveLimits", () => {
	it("returns the frozen defaults when the table overrides nothing", () => {
		expect(DEFAULT_LIMITS).toEqual({ maxHashKeyBytes: MAX_HASH_KEY_BYTES, maxSortKeyBytes: MAX_SORT_KEY_BYTES });
		expect(Object.isFrozen(DEFAULT_LIMITS)).toBe(true);
		expect(resolveLimits(undefined)).toBe(DEFAULT_LIMITS);
		expect(resolveLimits({})).toBe(DEFAULT_LIMITS);
	});

	it("applies each override, and ignores a key that a newer version can send", () => {
		const limits = resolveLimits({ maxSortKeyBytes: 1_024, maxFutureBytes: 7 } as FokosDBLimitOverrides);
		expect(limits).toEqual({ maxHashKeyBytes: MAX_HASH_KEY_BYTES, maxSortKeyBytes: 1_024 });
		expect(Object.isFrozen(limits)).toBe(true);
	});

	it("warns for a limit above 2 KiB, and not for a limit of exactly 2 KiB", () => {
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		resolveLimits({ maxHashKeyBytes: 2_048, maxSortKeyBytes: 2_048 });
		expect(warn).not.toHaveBeenCalled();

		resolveLimits({ maxHashKeyBytes: 4_096 });
		expect(warn).toHaveBeenCalledTimes(1);
		expect(warn.mock.calls[0][0]).toMatchObject({ limits: { maxHashKeyBytes: 4_096 } });
	});

	it.each<[FokosDBLimitOverrides, string]>([
		[{ maxHashKeyBytes: MAX_CLIENT_REQUEST_TOKEN_BYTES - 1 }, "limits.maxHashKeyBytes"],
		[{ maxHashKeyBytes: 1_500.5 }, "limits.maxHashKeyBytes"],
		[{ maxSortKeyBytes: 0 }, "limits.maxSortKeyBytes"],
	])("rejects %o as partition_context_options_invalid", (overrides, option) => {
		expect(() => resolveLimits(overrides)).toThrow(fokosErrorWith("partition_context_options_invalid", { option }));
	});

	it("checks the key sizes against the limits it is given", () => {
		const limits = resolveLimits({ maxHashKeyBytes: 2_000, maxSortKeyBytes: 1_000 });
		const hashKey = "h".repeat(1_500);
		const sortKey = "s".repeat(800);
		expect(() => encodeHashKey(hashKey, DEFAULT_LIMITS)).toThrow(fokosErrorWith("hash_key_too_large", { limitBytes: MAX_HASH_KEY_BYTES }));
		expect(() => encodeSortKey(sortKey, DEFAULT_LIMITS)).toThrow(fokosErrorWith("sort_key_too_large", { limitBytes: MAX_SORT_KEY_BYTES }));
		expect(encodeHashKey(hashKey, limits).byteLength).toBeGreaterThan(MAX_HASH_KEY_BYTES);
		expect(encodeSortKey(sortKey, limits).byteLength).toBeGreaterThan(MAX_SORT_KEY_BYTES);
	});
});

describe("validateVersionReferences", () => {
	const readsV = conditionTree({ op: "eq", args: [{ ref: "v" }, { val: 3 }] });
	const setPrevVersion = updateTree([{ action: "set", target: { ref: "data", path: "$.prevVersion" }, value: { ref: "v" } }]);
	const op = (
		opIndex: number,
		operation: TransactWriteOperationLike["operation"],
		extra: Pick<TransactWriteOperationLike, "condition" | "update"> = {},
	) => ({
		opIndex,
		hashKey: KeyCodec.encode("a"),
		sortKey: KeyCodec.encode("s"),
		operation,
		...extra,
	});

	it.each(["put", "update", "delete"] as const)("refuses a version reference after a %s of the same item", (earlier) => {
		expect(() => validateVersionReferences([op(0, earlier), op(1, "check", { condition: readsV })])).toThrow(
			fokosErrorWith("transact_version_after_write", { opIndex: 1, earlierOpIndex: 0, hashKey: "a", sortKey: "s" }),
		);
		expect(() => validateVersionReferences([op(0, earlier), op(1, "update", { update: setPrevVersion })])).toThrow(
			fokosErrorWith("transact_version_after_write", { opIndex: 1, earlierOpIndex: 0 }),
		);
	});

	it("refuses a version reference of an expression tree after a write of the same item", () => {
		const condition = { op: "eq", args: [{ ref: "v" }, { val: 3 }] } as const;
		const update = [{ action: "set", target: { ref: "data", path: "$.prevVersion" }, value: { ref: "v" } }] as const;
		expect(() => validateVersionReferences([op(0, "put"), op(1, "check", { condition })])).toThrow(
			fokosErrorWith("transact_version_after_write", { opIndex: 1, earlierOpIndex: 0 }),
		);
		expect(() => validateVersionReferences([op(0, "put"), op(1, "update", { update })])).toThrow(
			fokosErrorWith("transact_version_after_write", { opIndex: 1, earlierOpIndex: 0 }),
		);
		expect(() =>
			validateVersionReferences([op(0, "put"), op(1, "check", { condition: { op: "exists", args: [{ ref: "hashKey" }] } })]),
		).not.toThrow();
	});

	it("accepts a version reference on the first operation of an item, after a check, and on another item", () => {
		expect(() =>
			validateVersionReferences([
				op(0, "check", { condition: readsV }),
				op(1, "check", { condition: readsV }),
				op(2, "update", { update: setPrevVersion }),
				op(3, "put"),
				{ ...op(4, "check", { condition: readsV }), sortKey: KeyCodec.encode("other") },
			]),
		).not.toThrow();
	});
});
