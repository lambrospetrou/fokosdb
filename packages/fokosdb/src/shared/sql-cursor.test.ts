import { describe, expect, it } from "vitest";
import { one, tryOne } from "./sql-cursor.js";

function* rows(values: number[], onStop: () => void): Generator<{ n: number }> {
	try {
		for (const n of values) yield { n };
	} finally {
		onStop();
	}
}

describe("sql-cursor", () => {
	it("tryOne returns the first row and stops the iteration there", () => {
		let stopped = 0;
		expect(tryOne(rows([1, 2, 3], () => stopped++))).toEqual({ n: 1 });
		expect(stopped).toBe(1);
	});

	it("tryOne returns undefined for an empty cursor", () => {
		expect(tryOne(rows([], () => {}))).toBeUndefined();
	});

	it("one returns the first row", () => {
		expect(one(rows([7], () => {}))).toEqual({ n: 7 });
	});

	it("one raises invariant_failed for an empty cursor and carries the message as detail", () => {
		expect(() => one(rows([], () => {}))).toThrow(
			expect.objectContaining({ code: "invariant_failed", attributes: { detail: "expected at least one row from query" } }),
		);
		expect(() =>
			one(
				rows([], () => {}),
				"no count row",
			),
		).toThrow(expect.objectContaining({ attributes: { detail: "no count row" } }));
	});
});
