import { describe, expect, it } from "vitest";
import { DESTROY_ABORT_SENTINEL, isDestroyAbortError } from "./cf-utils.js";
import { FokosError } from "./errors.js";

describe("isDestroyAbortError", () => {
	const abort = new Error(`Durable Object was aborted: ${DESTROY_ABORT_SENTINEL}`);

	it("matches the abort error, and the abort error as the cause of a wrapper", () => {
		expect(isDestroyAbortError(abort)).toBe(true);
		expect(isDestroyAbortError(FokosError.wrap(abort))).toBe(true);
		expect(isDestroyAbortError(new Error("outer", { cause: FokosError.wrap(abort) }))).toBe(true);
	});

	it("does not match any other error, a cycle of causes included", () => {
		const cyclic = new Error("a") as Error & { cause?: unknown };
		cyclic.cause = cyclic;
		for (const e of [new Error("boom"), FokosError.wrap(new Error("boom")), cyclic, undefined, null]) {
			expect(isDestroyAbortError(e)).toBe(false);
		}
	});
});
