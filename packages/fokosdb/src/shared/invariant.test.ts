import { describe, expect, it } from "vitest";
import invariant from "./invariant.js";
import { FokosInternalError } from "./errors.js";

describe("invariant", () => {
	it("raises invariant_failed with a fixed message, and puts the text of the call site in attributes.detail", () => {
		let caught: unknown;
		try {
			invariant(false, 'unknown child partition "do-name-1"');
		} catch (e) {
			caught = e;
		}
		expect(FokosInternalError.is(caught)).toBe(true);
		expect(caught).toMatchObject({
			code: "invariant_failed",
			origin: "internal",
			httpStatusHint: 500,
			message: "fokos/invariant_failed: an internal invariant failed",
			attributes: { detail: 'unknown child partition "do-name-1"' },
		});
	});

	it("builds a lazy text only when the condition fails, and holds no detail without a text", () => {
		let built = 0;
		invariant(true, () => `built ${++built}`);
		expect(built).toBe(0);
		expect(() => invariant(false, () => `built ${++built}`)).toThrow(expect.objectContaining({ attributes: { detail: "built 1" } }));
		expect(() => invariant(null)).toThrow(expect.objectContaining({ code: "invariant_failed", attributes: {} }));
	});
});
