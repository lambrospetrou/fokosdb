import { describe, expect, it } from "vitest";
import { hashTransactionOperations } from "./transaction-idempotency.js";
import { KeyCodec } from "../sharding/key-codec.js";
import type { TCWriteOperation } from "./transaction-wire-types.js";
import { compileConditionExpression } from "./expression/compiler.js";

const STANDARD_FINGERPRINT = "21d8dc16298317e5";
const hash = (ops: TCWriteOperation[]) => hashTransactionOperations(ops, "standard");

describe("hashTransactionOperations", () => {
	it("is stable for the same operation set", () => {
		expect(hash([put("a", "s", "v")])).toBe(hash([put("a", "s", "v")]));
	});

	it("changes when operation order changes", () => {
		const ops = [put("a", "s1", "v1"), put("b", "s2", "v2"), put("c", "s3", "v3")];
		expect(hash([...ops].reverse())).not.toBe(hash(ops));
	});

	it("changes when returnValuesOnConditionCheckFailure changes", () => {
		const base = put("a", "s", "v");
		expect(hash([{ ...base, returnValuesOnConditionCheckFailure: "all_old" }])).not.toBe(hash([base]));
		expect(hash([{ ...base, returnValuesOnConditionCheckFailure: "all_old" }])).not.toBe(
			hash([{ ...base, returnValuesOnConditionCheckFailure: "none" }]),
		);
	});

	// The whole point of B8: same keys, different payload must NOT replay as committed.
	it("changes when only the data changes", () => {
		expect(hash([put("a", "s", "v2")])).not.toBe(hash([put("a", "s", "v1")]));
	});

	it("changes when the TTL is added or changed", () => {
		const base = put("a", "s", "v");
		expect(hash([{ ...base, ttlAt: 100 }])).not.toBe(hash([base]));
		expect(hash([{ ...base, ttlAt: 100 }])).not.toBe(hash([{ ...base, ttlAt: 101 }]));
	});

	it("changes when a key changes", () => {
		expect(hash([put("a", "s", "v")])).not.toBe(hash([put("b", "s", "v")]));
		expect(hash([put("a", "s2", "v")])).not.toBe(hash([put("a", "s", "v")]));
	});

	it("changes when the operation type changes", () => {
		const del: TCWriteOperation = { ...put("a", "s", "v"), operation: "delete", data: undefined, kind: undefined };
		expect(hash([del])).not.toBe(hash([put("a", "s", "v")]));
	});

	it("changes when the condition changes", () => {
		const base = put("a", "s", "v");
		const condition = compileConditionExpression({ op: "not_exists", args: [{ ref: "hashKey" }] });
		expect(hash([{ ...base, condition }])).not.toBe(hash([base]));
	});

	it("uses canonical condition identity instead of generated SQL", () => {
		const base = put("a", "s", "v");
		const condition = compileConditionExpression({ op: "eq", args: [{ ref: "v" }, { val: 1 }] });
		const reformatted = { ...condition, sql: `(${condition.sql})` };
		expect(hash([{ ...base, condition: reformatted }])).toBe(hash([{ ...base, condition }]));
	});

	// Presence flags exist for this: without them the empty string would chain like no field at all.
	it("distinguishes absent data from empty data", () => {
		const noData: TCWriteOperation = { ...put("a", "s", ""), data: undefined, kind: undefined };
		expect(hash([put("a", "s", "")])).not.toBe(hash([noData]));
	});

	// `kind` is chained, so the text "5" and the single byte 0x35 cannot fingerprint the same.
	it("distinguishes text data from the byte sequence that encodes it", () => {
		const asText = put("a", "s", "5");
		const asBytes: TCWriteOperation = { ...asText, data: new Uint8Array([0x35]), kind: "bytes" };
		expect(hash([asBytes])).not.toBe(hash([asText]));
	});

	// The final chain through the count pins the set size.
	it("changes when an operation is added", () => {
		const a = put("a", "s", "v");
		expect(hash([a, put("b", "s", "v")])).not.toBe(hash([a]));
	});

	it("keeps the standard fingerprint and gives ordered_per_item a different one", () => {
		const ops = [put("a", "s", "v"), put("b", "s", "w")];
		expect(hashTransactionOperations(ops, "standard")).toBe(hash(ops));
		expect(hashTransactionOperations(ops, "ordered_per_item")).not.toBe(hashTransactionOperations(ops, "standard"));
		expect(hashTransactionOperations(ops, "ordered_per_item")).toBe(hashTransactionOperations(ops, "ordered_per_item"));
		// A fixed value: a change of the standard fingerprint makes a replay of an older request a mismatch.
		expect(hashTransactionOperations([put("a", "s", "v")], "standard")).toBe(STANDARD_FINGERPRINT);
	});

	it("returns a fixed-width hex string", () => {
		expect(hash([put("a", "s", "v")])).toMatch(/^[0-9a-f]{16}$/);
	});
});

function put(hashKey: string, sortKey: string, data: string): TCWriteOperation {
	return {
		hashKey: KeyCodec.encode(hashKey),
		sortKey: KeyCodec.encode(sortKey),
		operation: "put",
		data,
		kind: "text",
		// Never read by the fingerprint: the fold is ordered, so the position of the operation in the
		// array already carries the request order that opIndex repeats.
		opIndex: 0,
	};
}
