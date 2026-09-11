/**
 * Test matchers for FokosError, for `toThrow` and `rejects.toThrow`. A test asserts the code and the
 * attributes, because the message is not contractual.
 */
import { expect } from "vitest";
import type { FokosErrorCode } from "../src/shared/errors-operations.js";

/** Matches a FokosError with `code` whose attributes hold at least `attributes`. */
export function fokosErrorWith(code: FokosErrorCode, attributes: Record<string, unknown> = {}) {
	return expect.objectContaining({ code, attributes: expect.objectContaining(attributes) });
}

/** Matches the error `invariant()` raises, with a detail that matches `detail`. */
export function invariantFailure(detail: RegExp | string) {
	return fokosErrorWith("invariant_failed", { detail: expect.stringMatching(detail) });
}
