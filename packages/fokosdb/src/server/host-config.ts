/**
 * The settings of `PartitionDO` and `TransactionCoordinatorDO`. Each class has a `fokosConfig()` method
 * that returns overrides. The class merges them with the defaults below, validates the result, and
 * reads it at each use. A subclass can therefore return a value that depends on `this.env`, or on the
 * identity or the policy of the partition, and a changed value applies at its next read.
 */
import invariant from "../shared/invariant.js";
import { validateTtlSweepConfig, type TtlSweepConfig } from "../shared/partition/ttl-expiry.js";
import { DEFAULT_FANOUT_REQUEST_BUDGET_MS, DEFAULT_STALE_TRANSACTION_MS, IDEMPOTENCY_WINDOW_MS } from "../shared/transaction-limits.js";

/**
 * The longest time that one alarm handler of a Durable Object can run.
 * The real limit of Durable Object alarm handlers is 15 minutes, but we configure this max to be 14 minutes.
 * */
const DO_ALARM_MAX_WALL_TIME_MS = 14 * 60_000;
/**
 * The storage limit of one Durable Object: 9 GB.
 * The real limit of Durable Objects is 10 GB but we don't want ever to reach it.
 **/
const DO_STORAGE_MAX_BYTES = 9_000_000_000;
/** The bound parameters of one SQLite statement in a Durable Object. */
const SQL_MAX_BOUND_PARAMETERS = 100;

export type PartitionDOConfig = Readonly<{
	/**
	 * A hash key moves into a range partition of its own when its size reaches
	 * `hashSplitConditions.maxSizeMb` times this fraction. Valid values are in (0, 1).
	 */
	promotionFraction: number;
	/**
	 * The farthest into the future that the timestamp of a transaction can be when this partition
	 * accepts its prepare. It must be larger than the real clock difference between the caller Worker,
	 * the coordinator and the partition.
	 */
	maxClockSkewMs: number;
	/** The TTL sweep that deletes the expired items of this partition. */
	ttlSweep: Readonly<TtlSweepConfig>;

	/**
	 * How long a transaction lock waits on this partition before the partition asks the coordinator of
	 * the lock to finish the transaction. It must be at least `coordinatorFanoutBudgetMs`.
	 */
	staleTransactionMs: number;
	/**
	 * The `fanoutRequestBudgetMs` of the coordinators of this table. A partition cannot read a setting of
	 * a coordinator, so set this to the same value. It is used only to check `staleTransactionMs`.
	 */
	coordinatorFanoutBudgetMs: number;
	/** The stale transaction locks that one step of the stale transaction job reads. */
	staleLockScanRows: number;
	/** The items of a promoted hash key that one step of the source cleanup deletes. */
	cleanupPromotedKeyRows: number;
	/**
	 * The lock copies that one step of the source cleanup deletes after a split or a promotion. A step
	 * deletes all copies of one transaction together, so it can delete up to 99 rows more.
	 */
	cleanupTxLockCopyRows: number;
}>;

export type PartitionDOConfigOverrides = Partial<Omit<PartitionDOConfig, "ttlSweep">> & { ttlSweep?: Partial<TtlSweepConfig> };

export type ParticipantRetryConfig = Readonly<{
	/** The first retry waits a random time up to this value. */
	baseDelayMs: number;
	/** The longest random wait between two attempts. It must be larger than `baseDelayMs`. */
	maxDelayMs: number;
	/** The attempts to one participant in the first prepare of a transaction. */
	prepareMaxAttempts: number;
	/** The attempts to one participant when the coordinator sends the prepare again to the participants with no answer. */
	prepareRecoveryMaxAttempts: number;
}>;

export type TransactionCoordinatorDOConfig = Readonly<{
	/**
	 * How long a transaction that no request drives waits before the `tx_recovery` job drives it. It must
	 * be at least `fanoutRequestBudgetMs`.
	 */
	staleTransactionMs: number;
	/**
	 * How long the coordinator retries its participants while a request waits, for a commit and for a
	 * cancel. After this time, the coordinator stops and leaves the participants with no answer. A
	 * commit then answers `transaction_commit_pending`, and a cancel answers `cancelled`, because a
	 * cancelled transaction applied nothing. The `tx_recovery` job finishes the work later. Without this
	 * budget, one participant that cannot be reached holds the request for a long time.
	 */
	fanoutRequestBudgetMs: number;
	/** The retries of each call from the coordinator to a participant. */
	participantRetry: ParticipantRetryConfig;
	/**
	 * The longest time that one step of the `tx_recovery` job drives transactions. A step stops its
	 * participant retries when the time ends, and the next step continues. A step can end later by one
	 * retry wait and one RPC. It must be below the alarm time limit.
	 */
	alarmRecoveryBudgetMs: number;
	/** The rows that one step of the idempotency sweep and one step of the source cleanup read. */
	sweepBatchRows: number;
	/** The transaction IDs in one `DELETE` statement of the idempotency sweep. At most 100, the bound parameter limit. */
	sweepDeleteChunkRows: number;
	/** The non-terminal transactions that one step of the `tx_recovery` job reads. */
	recoveryScanRows: number;
	/** The size at which a coordinator splits when the table has no smaller split size. At most the storage limit. */
	maxDatabaseBytes: number;
}>;

export type TransactionCoordinatorDOConfigOverrides = Partial<Omit<TransactionCoordinatorDOConfig, "participantRetry">> & {
	participantRetry?: Partial<ParticipantRetryConfig>;
};

export const DEFAULT_PARTITION_CONFIG: PartitionDOConfig = deepFreeze({
	staleTransactionMs: DEFAULT_STALE_TRANSACTION_MS,
	coordinatorFanoutBudgetMs: DEFAULT_FANOUT_REQUEST_BUDGET_MS,
	promotionFraction: 0.25,
	maxClockSkewMs: 5_000,
	ttlSweep: {
		chunkSize: 100,
		sleepMs: 1_000,
		maxRowsBeforeSleep: 10_000,
		maxBytesBeforeSleep: 50 * 1024 * 1024,
		maxRowsPerCycle: 100_000,
		ttlSweepDelayMs: 500,
	},
	staleLockScanRows: 10,
	cleanupPromotedKeyRows: 10_000,
	cleanupTxLockCopyRows: 10_000,
});

export const DEFAULT_COORDINATOR_CONFIG: TransactionCoordinatorDOConfig = deepFreeze({
	staleTransactionMs: DEFAULT_STALE_TRANSACTION_MS,
	fanoutRequestBudgetMs: DEFAULT_FANOUT_REQUEST_BUDGET_MS,
	participantRetry: { baseDelayMs: 100, maxDelayMs: 2_000, prepareMaxAttempts: 3, prepareRecoveryMaxAttempts: 5 },
	alarmRecoveryBudgetMs: 30_000,
	sweepBatchRows: 1_000,
	sweepDeleteChunkRows: 100,
	recoveryScanRows: 100,
	maxDatabaseBytes: 5 * 1024 * 1024 * 1024,
});

/** Merges `overrides` with the defaults of `PartitionDO` and validates the result. */
export function resolvePartitionConfig(overrides: PartitionDOConfigOverrides): PartitionDOConfig {
	const c = merge(DEFAULT_PARTITION_CONFIG, overrides);
	if (c === DEFAULT_PARTITION_CONFIG) {
		return c;
	}
	checkInteger(c, "staleTransactionMs", 1);
	checkInteger(c, "coordinatorFanoutBudgetMs", 1);
	check(
		c.staleTransactionMs >= c.coordinatorFanoutBudgetMs,
		() => `staleTransactionMs (${c.staleTransactionMs}) must be at least coordinatorFanoutBudgetMs (${c.coordinatorFanoutBudgetMs})`,
	);
	check(c.promotionFraction > 0 && c.promotionFraction < 1, () => `promotionFraction must be in (0, 1), got ${c.promotionFraction}`);
	checkInteger(c, "maxClockSkewMs", 0);
	checkInteger(c, "staleLockScanRows", 1);
	checkInteger(c, "cleanupPromotedKeyRows", 1);
	checkInteger(c, "cleanupTxLockCopyRows", 1);
	validateTtlSweepConfig(c.ttlSweep);
	return c;
}

/** Merges `overrides` with the defaults of `TransactionCoordinatorDO` and validates the result. */
export function resolveCoordinatorConfig(overrides: TransactionCoordinatorDOConfigOverrides): TransactionCoordinatorDOConfig {
	const c = merge(DEFAULT_COORDINATOR_CONFIG, overrides);
	if (c === DEFAULT_COORDINATOR_CONFIG) {
		return c;
	}
	checkInteger(c, "staleTransactionMs", 1);
	checkInteger(c, "fanoutRequestBudgetMs", 1);
	check(
		c.staleTransactionMs >= c.fanoutRequestBudgetMs,
		() => `staleTransactionMs (${c.staleTransactionMs}) must be at least fanoutRequestBudgetMs (${c.fanoutRequestBudgetMs})`,
	);
	const retry = c.participantRetry;
	checkInteger(retry, "baseDelayMs", 1, "participantRetry.");
	checkInteger(retry, "maxDelayMs", 1, "participantRetry.");
	check(retry.baseDelayMs < retry.maxDelayMs, () => "participantRetry.baseDelayMs must be less than participantRetry.maxDelayMs");
	checkInteger(retry, "prepareMaxAttempts", 1, "participantRetry.");
	checkInteger(retry, "prepareRecoveryMaxAttempts", 1, "participantRetry.");
	checkInteger(c, "alarmRecoveryBudgetMs", 1);
	check(
		c.alarmRecoveryBudgetMs < DO_ALARM_MAX_WALL_TIME_MS,
		() => `alarmRecoveryBudgetMs must be less than ${DO_ALARM_MAX_WALL_TIME_MS}, got ${c.alarmRecoveryBudgetMs}`,
	);
	checkInteger(c, "sweepBatchRows", 1);
	checkInteger(c, "sweepDeleteChunkRows", 1);
	check(
		c.sweepDeleteChunkRows <= SQL_MAX_BOUND_PARAMETERS,
		() => `sweepDeleteChunkRows must be at most ${SQL_MAX_BOUND_PARAMETERS}, got ${c.sweepDeleteChunkRows}`,
	);
	checkInteger(c, "recoveryScanRows", 1);
	checkInteger(c, "maxDatabaseBytes", 1);
	check(
		c.maxDatabaseBytes <= DO_STORAGE_MAX_BYTES,
		() => `maxDatabaseBytes must be at most ${DO_STORAGE_MAX_BYTES}, got ${c.maxDatabaseBytes}`,
	);
	return c;
}

/**
 * How long a transaction can stay in PREPARING before the coordinator cancels it. It is five stale
 * times, and never longer than the idempotency window.
 */
export function maxPreparingHoldMs(config: TransactionCoordinatorDOConfig): number {
	return Math.min(5 * config.staleTransactionMs, IDEMPOTENCY_WINDOW_MS);
}

/**
 * Applies each override that is not `undefined` over `defaults`, one level deep. It ignores a key that
 * `defaults` does not have. It returns `defaults` itself when no value changes.
 */
function merge<T extends Readonly<Record<string, unknown>>>(defaults: T, overrides: object): T {
	// The hosts resolve at each use, and most return no overrides, so that path allocates nothing.
	if (isEmpty(overrides)) {
		return defaults;
	}
	const source = overrides as Record<string, unknown>;
	const result: Record<string, unknown> = {};
	let changed = false;
	for (const [key, value] of Object.entries(defaults)) {
		const override = source[key];
		if (override === undefined) {
			result[key] = value;
		} else if (typeof value === "object" && value !== null) {
			const nested = merge(value as Record<string, unknown>, override as object);
			changed ||= nested !== value;
			result[key] = nested;
		} else {
			changed = true;
			result[key] = override;
		}
	}
	return changed ? deepFreeze(result as T) : defaults;
}

function isEmpty(value: object): boolean {
	for (const _ in value) {
		return false;
	}
	return true;
}

function deepFreeze<T extends object>(value: T): T {
	for (const nested of Object.values(value)) {
		if (typeof nested === "object" && nested !== null) {
			Object.freeze(nested);
		}
	}
	return Object.freeze(value);
}

function checkInteger<T extends object>(config: T, key: keyof T & string, min: number, prefix = ""): void {
	const value = config[key];
	check(
		typeof value === "number" && Number.isSafeInteger(value) && value >= min,
		() => `${prefix}${key} must be an integer of at least ${min}, got ${String(value)}`,
	);
}

function check(condition: boolean, message: () => string): asserts condition {
	invariant(condition, () => `fokos/host-config: ${message()}`);
}
