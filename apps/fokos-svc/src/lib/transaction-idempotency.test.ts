import { describe, expect, it } from "vitest";
import { hashTransactionOperations } from "./transaction-idempotency.js";
import { KeyCodec } from "./partition-topology/key-codec.js";
import type { TCWriteOperation } from "./transaction-types.js";
import { compileConditionExpression } from "./expression/compiler.js";

describe("hashTransactionOperations", () => {
	it("is stable for the same operation set", () => {
		expect(hashTransactionOperations([put("a", "s", "v")])).toBe(hashTransactionOperations([put("a", "s", "v")]));
	});

	// The fold is a wrapping add precisely so this holds without sorting: the same items sent in a
	// different order are the same request, and must replay rather than be rejected.
	it("ignores operation order", () => {
		const ops = [put("a", "s1", "v1"), put("b", "s2", "v2"), put("c", "s3", "v3")];
		expect(hashTransactionOperations([...ops].reverse())).toBe(hashTransactionOperations(ops));
	});

	// The whole point of B8: same keys, different payload must NOT replay as committed.
	it("changes when only the data changes", () => {
		expect(hashTransactionOperations([put("a", "s", "v2")])).not.toBe(hashTransactionOperations([put("a", "s", "v1")]));
	});

	it("changes when the TTL is added or changed", () => {
		const base = put("a", "s", "v");
		expect(hashTransactionOperations([{ ...base, ttlAt: 100 }])).not.toBe(hashTransactionOperations([base]));
		expect(hashTransactionOperations([{ ...base, ttlAt: 100 }])).not.toBe(hashTransactionOperations([{ ...base, ttlAt: 101 }]));
	});

	it("changes when a key changes", () => {
		expect(hashTransactionOperations([put("a", "s", "v")])).not.toBe(hashTransactionOperations([put("b", "s", "v")]));
		expect(hashTransactionOperations([put("a", "s2", "v")])).not.toBe(hashTransactionOperations([put("a", "s", "v")]));
	});

	it("changes when the operation type changes", () => {
		const del: TCWriteOperation = { ...put("a", "s", "v"), operation: "delete", data: undefined, kind: undefined };
		expect(hashTransactionOperations([del])).not.toBe(hashTransactionOperations([put("a", "s", "v")]));
	});

	it("changes when the condition changes", () => {
		const base = put("a", "s", "v");
		const condition = compileConditionExpression({ op: "not_exists", args: [{ ref: "hashKey" }] });
		expect(hashTransactionOperations([{ ...base, condition }])).not.toBe(hashTransactionOperations([base]));
	});

	it("uses canonical condition identity instead of generated SQL", () => {
		const base = put("a", "s", "v");
		const condition = compileConditionExpression({ op: "eq", args: [{ ref: "v" }, { val: 1 }] });
		const reformatted = { ...condition, sql: `(${condition.sql})` };
		expect(hashTransactionOperations([{ ...base, condition: reformatted }])).toBe(hashTransactionOperations([{ ...base, condition }]));
	});

	// Presence flags exist for this: without them the empty string would chain like no field at all.
	it("distinguishes absent data from empty data", () => {
		const noData: TCWriteOperation = { ...put("a", "s", ""), data: undefined, kind: undefined };
		expect(hashTransactionOperations([put("a", "s", "")])).not.toBe(hashTransactionOperations([noData]));
	});

	// `kind` is chained, so the text "5" and the single byte 0x35 cannot fingerprint the same.
	it("distinguishes text data from the byte sequence that encodes it", () => {
		const asText = put("a", "s", "5");
		const asBytes: TCWriteOperation = { ...asText, data: new Uint8Array([0x35]), kind: "bytes" };
		expect(hashTransactionOperations([asBytes])).not.toBe(hashTransactionOperations([asText]));
	});

	// The final chain through the count pins the set size.
	it("changes when an operation is added", () => {
		const a = put("a", "s", "v");
		expect(hashTransactionOperations([a, put("b", "s", "v")])).not.toBe(hashTransactionOperations([a]));
	});

	it("returns a fixed-width hex string", () => {
		expect(hashTransactionOperations([put("a", "s", "v")])).toMatch(/^[0-9a-f]{16}$/);
	});
});

function put(hashKey: string, sortKey: string, data: string): TCWriteOperation {
	return {
		hashKey: KeyCodec.encode(hashKey),
		sortKey: KeyCodec.encode(sortKey),
		operation: "put",
		data,
		kind: "text",
		// Never read by the fingerprint — routing is not part of the request's identity.
		partitionContext: undefined as never,
	};
}
