import { describe, expect, it } from "vitest";
import { invariantFailure } from "../../test/errors-matchers.js";
import {
	DEFAULT_RUNTIME_CONFIG,
	PROMOTION_BLOOM_MAX_BYTES,
	RPC_MESSAGE_MAX_BYTES,
	resolveRuntimeConfig,
	type FokosRuntimeConfigOverrides,
} from "./runtime-config.js";

describe("resolveRuntimeConfig", () => {
	it("returns the frozen defaults when no value is overridden", () => {
		expect(Object.isFrozen(DEFAULT_RUNTIME_CONFIG)).toBe(true);
		expect(resolveRuntimeConfig(undefined)).toBe(DEFAULT_RUNTIME_CONFIG);
		expect(resolveRuntimeConfig({})).toBe(DEFAULT_RUNTIME_CONFIG);
		expect(resolveRuntimeConfig({ fallbackAlarmMs: undefined })).toBe(DEFAULT_RUNTIME_CONFIG);
	});

	it("applies each override over the defaults, and ignores a key that it does not know", () => {
		const overrides = { migrationPageRows: 7, fastPathDelayMs: 0, unknownSetting: 1 } as FokosRuntimeConfigOverrides;
		const config = resolveRuntimeConfig(overrides);
		expect(config).toEqual({ ...DEFAULT_RUNTIME_CONFIG, migrationPageRows: 7, fastPathDelayMs: 0 });
		expect(config).not.toHaveProperty("unknownSetting");
		expect(Object.isFrozen(config)).toBe(true);
	});

	it.each<[FokosRuntimeConfigOverrides, RegExp]>([
		[{ fallbackAlarmMs: 0 }, /fallbackAlarmMs must be an integer of at least 1/],
		[{ fastPathDelayMs: -1 }, /fastPathDelayMs must be an integer of at least 0/],
		[{ importPagesPerPass: 1.5 }, /importPagesPerPass must be an integer/],
		[{ importPagesPerPass: Number.NaN }, /importPagesPerPass must be an integer/],
		[{ hashArenaBytes: 1_019 }, /hashArenaBytes must be an integer of at least 1020/],
		[{ rangeHierarchyMaxRows: 0 }, /rangeHierarchyMaxRows must be an integer of at least 1/],
		[{ maxForwardRetries: -1 }, /maxForwardRetries must be an integer of at least 0/],
		[{ cutoverHoldRetryMs: 0 }, /cutoverHoldRetryMs must be an integer of at least 1/],
		[{ migrationPageRows: 0 }, /migrationPageRows must be an integer of at least 1/],
		[{ statusPageEntries: 0 }, /statusPageEntries must be an integer of at least 1/],
	])("rejects a value below its range: %o", (overrides, detail) => {
		expect(() => resolveRuntimeConfig(overrides)).toThrow(invariantFailure(detail));
	});

	it.each([0, 1, -0.1, 1.5, Number.NaN])("rejects a promotion Bloom false positive rate of %d", (rate) => {
		expect(() => resolveRuntimeConfig({ promotionBloomFalsePositiveRate: rate })).toThrow(
			invariantFailure(/promotionBloomFalsePositiveRate must be in \(0, 1\)/),
		);
	});

	it("rejects a value above its platform ceiling", () => {
		expect(() => resolveRuntimeConfig({ migrationPageBytes: RPC_MESSAGE_MAX_BYTES + 1 })).toThrow(
			invariantFailure(/migrationPageBytes must be at most/),
		);
		expect(() => resolveRuntimeConfig({ statusPageBytes: RPC_MESSAGE_MAX_BYTES + 1 })).toThrow(
			invariantFailure(/statusPageBytes must be at most/),
		);
		expect(resolveRuntimeConfig({ migrationPageBytes: RPC_MESSAGE_MAX_BYTES }).migrationPageBytes).toBe(RPC_MESSAGE_MAX_BYTES);
	});

	it("rejects a promotion Bloom filter whose first layer is larger than one KV value can hold", () => {
		// The first layer needs about 11 bits for each key at 1%, so 2 million keys need about 2.8 MB.
		expect(() => resolveRuntimeConfig({ promotionBloomExpectedKeys: 2_000_000 })).toThrow(
			invariantFailure(new RegExp(`above its ceiling of ${PROMOTION_BLOOM_MAX_BYTES}`)),
		);
		// A higher false positive rate needs fewer bits, so the same key count then fits.
		expect(resolveRuntimeConfig({ promotionBloomExpectedKeys: 2_000_000, promotionBloomFalsePositiveRate: 0.2 })).toMatchObject({
			promotionBloomExpectedKeys: 2_000_000,
		});
	});

	it("rejects a retry base that is larger than its maximum", () => {
		expect(() => resolveRuntimeConfig({ sourceRetryBaseMs: 10_000, sourceRetryMaxMs: 9_999 })).toThrow(
			invariantFailure(/sourceRetryBaseMs must be at most sourceRetryMaxMs/),
		);
		expect(() => resolveRuntimeConfig({ importRetryMaxMs: DEFAULT_RUNTIME_CONFIG.importRetryBaseMs - 1 })).toThrow(
			invariantFailure(/importRetryBaseMs must be at most importRetryMaxMs/),
		);
		expect(resolveRuntimeConfig({ sourceRetryBaseMs: 7, sourceRetryMaxMs: 7 }).sourceRetryMaxMs).toBe(7);
	});
});
