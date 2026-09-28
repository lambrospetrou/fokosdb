import { describe, expect, it } from "vitest";
import { invariantFailure } from "../../test/errors-matchers.js";
import { IDEMPOTENCY_WINDOW_MS } from "../shared/transaction-limits.js";
import {
	DEFAULT_COORDINATOR_CONFIG,
	DEFAULT_PARTITION_CONFIG,
	maxPreparingHoldMs,
	resolveCoordinatorConfig,
	resolvePartitionConfig,
	type PartitionDOConfigOverrides,
	type TransactionCoordinatorDOConfigOverrides,
} from "./host-config.js";

describe("resolvePartitionConfig", () => {
	it("returns the frozen defaults when no value is overridden", () => {
		expect(Object.isFrozen(DEFAULT_PARTITION_CONFIG)).toBe(true);
		expect(Object.isFrozen(DEFAULT_PARTITION_CONFIG.ttlSweep)).toBe(true);
		expect(resolvePartitionConfig({})).toBe(DEFAULT_PARTITION_CONFIG);
		expect(resolvePartitionConfig({ staleTransactionMs: undefined, ttlSweep: {} })).toBe(DEFAULT_PARTITION_CONFIG);
	});

	it("merges a nested override field by field, and ignores a key that it does not know", () => {
		const config = resolvePartitionConfig({ ttlSweep: { sleepMs: 0 }, unknownSetting: 1 } as PartitionDOConfigOverrides);
		expect(config).toEqual({ ...DEFAULT_PARTITION_CONFIG, ttlSweep: { ...DEFAULT_PARTITION_CONFIG.ttlSweep, sleepMs: 0 } });
		expect(config).not.toHaveProperty("unknownSetting");
		expect(Object.isFrozen(config.ttlSweep)).toBe(true);
	});

	it("accepts a stale time below the default fan-out budget when the coordinators use the same lower budget", () => {
		expect(resolvePartitionConfig({ staleTransactionMs: 250, coordinatorFanoutBudgetMs: 250 })).toMatchObject({
			staleTransactionMs: 250,
			coordinatorFanoutBudgetMs: 250,
		});
	});

	it.each<[PartitionDOConfigOverrides, RegExp]>([
		[{ staleTransactionMs: 4_999 }, /staleTransactionMs \(4999\) must be at least coordinatorFanoutBudgetMs \(5000\)/],
		[{ staleTransactionMs: 5_000, coordinatorFanoutBudgetMs: 6_000 }, /must be at least coordinatorFanoutBudgetMs \(6000\)/],
		[{ staleTransactionMs: 0 }, /staleTransactionMs must be an integer of at least 1/],
		[{ promotionFraction: 0 }, /promotionFraction must be in \(0, 1\)/],
		[{ promotionFraction: 1 }, /promotionFraction must be in \(0, 1\)/],
		[{ promotionFraction: Number.NaN }, /promotionFraction must be in \(0, 1\)/],
		[{ maxClockSkewMs: -1 }, /maxClockSkewMs must be an integer of at least 0/],
		[{ staleLockScanRows: 0 }, /staleLockScanRows must be an integer of at least 1/],
		[{ promotedKeyCleanupRows: 2.5 }, /promotedKeyCleanupRows must be an integer/],
		[{ ttlSweep: { chunkSize: 0 } }, /chunkSize must be an integer greater than zero/],
	])("rejects %o", (overrides, detail) => {
		expect(() => resolvePartitionConfig(overrides)).toThrow(invariantFailure(detail));
	});
});

describe("resolveCoordinatorConfig", () => {
	it("returns the frozen defaults when no value is overridden", () => {
		expect(Object.isFrozen(DEFAULT_COORDINATOR_CONFIG.participantRetry)).toBe(true);
		expect(resolveCoordinatorConfig({})).toBe(DEFAULT_COORDINATOR_CONFIG);
		expect(resolveCoordinatorConfig({ participantRetry: {} })).toBe(DEFAULT_COORDINATOR_CONFIG);
	});

	it("merges a nested override field by field", () => {
		expect(resolveCoordinatorConfig({ participantRetry: { prepareMaxAttempts: 1 } }).participantRetry).toEqual({
			...DEFAULT_COORDINATOR_CONFIG.participantRetry,
			prepareMaxAttempts: 1,
		});
	});

	it.each<[TransactionCoordinatorDOConfigOverrides, RegExp]>([
		[{ fanoutRequestBudgetMs: 5_001 }, /staleTransactionMs \(5000\) must be at least fanoutRequestBudgetMs \(5001\)/],
		[{ staleTransactionMs: 250 }, /staleTransactionMs \(250\) must be at least fanoutRequestBudgetMs/],
		[{ participantRetry: { baseDelayMs: 2_000 } }, /participantRetry.baseDelayMs must be less than participantRetry.maxDelayMs/],
		[{ participantRetry: { prepareMaxAttempts: 0 } }, /participantRetry.prepareMaxAttempts must be an integer of at least 1/],
		[{ participantRetry: { prepareRecoveryMaxAttempts: 0 } }, /prepareRecoveryMaxAttempts must be an integer of at least 1/],
		[{ recoverTransactionBudgetMs: 0 }, /recoverTransactionBudgetMs must be an integer of at least 1/],
		[{ alarmRecoveryBudgetMs: 15 * 60_000 }, /alarmRecoveryBudgetMs must be less than 840000/],
		[{ sweepBatchRows: 0 }, /sweepBatchRows must be an integer of at least 1/],
		[{ sweepDeleteChunkRows: 101 }, /sweepDeleteChunkRows must be at most 100/],
		[{ recoveryScanRows: 0 }, /recoveryScanRows must be an integer of at least 1/],
		[{ maxDatabaseBytes: 10_000_000_001 }, /maxDatabaseBytes must be at most 9000000000/],
	])("rejects %o", (overrides, detail) => {
		expect(() => resolveCoordinatorConfig(overrides)).toThrow(invariantFailure(detail));
	});

	it("derives the longest PREPARING hold from the stale time, and caps it at the idempotency window", () => {
		expect(maxPreparingHoldMs(DEFAULT_COORDINATOR_CONFIG)).toBe(5 * DEFAULT_COORDINATOR_CONFIG.staleTransactionMs);
		expect(maxPreparingHoldMs(resolveCoordinatorConfig({ staleTransactionMs: IDEMPOTENCY_WINDOW_MS }))).toBe(IDEMPOTENCY_WINDOW_MS);
	});
});
