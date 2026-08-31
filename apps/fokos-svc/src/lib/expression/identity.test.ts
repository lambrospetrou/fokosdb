import { describe, expect, it } from "vitest";
import type { ConditionExpression, ExpressionValue, ProjectionExpression } from "./types.js";
import { EXPRESSION_LIMITS } from "./limits.js";
import { canonicalConditionIdentity, canonicalProjectionIdentity, canonicalValueIdentity } from "./identity.js";

const versionEqualsOne: ConditionExpression = { op: "eq", args: [{ ref: "v" }, { val: 1 }] };

describe("canonical expression identity", () => {
	it("uses fixed field order", () => {
		expect(canonicalConditionIdentity(versionEqualsOne)).toBe('{"op":"eq","args":[{"ref":"v"},{"val":1}]}');
	});

	it("does not depend on caller object field insertion order", () => {
		const reordered = { args: [{ ref: "v" }, { val: 1 }], op: "eq" } as ConditionExpression;
		expect(canonicalConditionIdentity(reordered)).toBe(canonicalConditionIdentity(versionEqualsOne));
	});

	it("preserves argument order", () => {
		const reversed: ConditionExpression = { op: "eq", args: [{ val: 1 }, { ref: "v" }] };
		expect(canonicalConditionIdentity(reversed)).not.toBe(canonicalConditionIdentity(versionEqualsOne));
	});

	it("uses fixed reference and function field order", () => {
		const expression: ConditionExpression = {
			op: "eq",
			args: [{ fn: "sqlite.lower", args: [{ path: "$.email", ref: "data" }] }, { val: "user@example.com" }],
		};
		expect(canonicalConditionIdentity(expression)).toBe(
			'{"op":"eq","args":[{"fn":"sqlite.lower","args":[{"ref":"data","path":"$.email"}]},{"val":"user@example.com"}]}',
		);
	});

	it("distinguishes a complete data reference from a root JSON path", () => {
		expect(canonicalValueIdentity({ ref: "data" })).not.toBe(canonicalValueIdentity({ ref: "data", path: "$" }));
	});

	it("generates stable projection identity with fixed field order", () => {
		const projection: readonly ProjectionExpression[] = [
			{ as: "id", expr: { ref: "hashKey" } },
			{ expr: { ref: "data", path: "$.email" } },
		];
		expect(canonicalProjectionIdentity(projection)).toBe('[{"expr":{"ref":"hashKey"},"as":"id"},{"expr":{"ref":"data","path":"$.email"}}]');
	});

	it.each([
		["string", "text"],
		["number", 1.5],
		["zero", 0],
		["negative zero", -0],
		["boolean", true],
		["null", null],
	] as const)("accepts the JSON scalar %s", (_name, value) => {
		expect(() => canonicalValueIdentity({ val: value })).not.toThrow();
	});

	it("canonicalizes negative zero as JSON zero", () => {
		expect(canonicalValueIdentity({ val: -0 })).toBe(canonicalValueIdentity({ val: 0 }));
	});

	it.each([
		["undefined", undefined],
		["NaN", Number.NaN],
		["positive infinity", Number.POSITIVE_INFINITY],
		["negative infinity", Number.NEGATIVE_INFINITY],
		["bigint", 1n],
		["bytes", new Uint8Array([1])],
		["array", [1]],
		["object", { value: 1 }],
	] as const)("rejects the non-scalar literal %s", (_name, value) => {
		const expression = { val: value } as unknown as ExpressionValue;
		expect(() => canonicalValueIdentity(expression)).toThrow(/scalar literal/);
	});

	it("accepts a canonical identity at the payload limit and rejects one byte above it", () => {
		const overheadBytes = 10;
		const atLimit = { val: "x".repeat(EXPRESSION_LIMITS.canonicalPayloadBytes - overheadBytes) };
		const aboveLimit = { val: "x".repeat(EXPRESSION_LIMITS.canonicalPayloadBytes - overheadBytes + 1) };
		expect(new TextEncoder().encode(canonicalValueIdentity(atLimit)).byteLength).toBe(EXPRESSION_LIMITS.canonicalPayloadBytes);
		expect(() => canonicalValueIdentity(aboveLimit)).toThrow(/payload limit/);
	});
});

describe("byte literal identity", () => {
	it("canonicalizes every encoding of one byte value", () => {
		const canonical = canonicalValueIdentity({ b64: "YWI=" });
		expect(canonical).toBe('{"b64":"YWI="}');
		expect(canonicalValueIdentity({ b64: "YWI" })).toBe(canonical);
	});

	it("keeps a byte literal distinct from the text literal with equal content", () => {
		expect(canonicalValueIdentity({ b64: "YWI=" })).not.toBe(canonicalValueIdentity({ val: "ab" }));
	});

	it("rejects an invalid byte literal", () => {
		expect(() => canonicalValueIdentity({ b64: "" } as unknown as ExpressionValue)).toThrow(/byte literal/);
	});
});
