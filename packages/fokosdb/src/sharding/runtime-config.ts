/**
 * The settings of `FokosShardingRuntime`. A host gives its overrides through the `config` callback of
 * the runtime constructor, and the runtime merges them with the defaults below and validates the result.
 *
 * The runtime reads a value that sizes a structure once, when it creates that structure. It reads every
 * other value at each use, so a changed override applies at its next read.
 */
import invariant from "../shared/invariant.js";
import { bloomInitialLayerBytes } from "./bloom-filter.js";

/**
 * The maximum serialized size of one Workers RPC message. A page must stay below it.
 * The actual RPC limit is 32MB, but we restrict everything to 20MB to stay well below the limit.
 **/
export const RPC_MESSAGE_MAX_BYTES = 20 * 1024 * 1024;

/** The serialized promotion Bloom filter is one KV value, which must stay below the 2 MB value limit. */
export const PROMOTION_BLOOM_MAX_BYTES = 1.5 * 1024 * 1024;

/**
 * The default number of keys in the first layer of the promotion Bloom filter.
 *
 * Each new layer holds 2x the keys of the layer before it, at half its false positive rate. The filter
 * does not add a layer that makes it larger than `PROMOTION_BLOOM_MAX_BYTES`. At a 1% false positive
 * rate (`tools/bloom-filter-sizing.js`):
 *
 *   First layer │ Layers │ First layer size │ Size when full │ Most keys
 *   ────────────┼────────┼──────────────────┼────────────────┼──────────
 *            1K │      9 │           1.3 KB │        1.29 MB │   511,000
 *            2K │      8 │           2.7 KB │        1.20 MB │   510,000
 *            4K │      7 │           5.4 KB │        1.11 MB │   508,000
 *            8K │      6 │          10.8 KB │        1.02 MB │   504,000
 *           16K │      5 │          21.5 KB │       943.8 KB │   496,000
 *           32K │      4 │          43.1 KB │       837.8 KB │   480,000
 *           64K │      3 │          86.2 KB │       715.8 KB │   448,000
 *          128K │      3 │         172.3 KB │        1.40 MB │   896,000
 *          256K │      2 │         344.6 KB │        1.10 MB │   768,000
 *          300K │      2 │         403.8 KB │        1.29 MB │   900,000
 *          512K │      1 │         689.2 KB │       689.2 KB │   512,000
 *
 * 128K holds almost as many keys as the largest option (300K), with a first layer of less than half
 * the size. The runtime writes the whole filter each time it learns a new promoted key, and reads it
 * whole at each start. In local workerd, one write costs about 3.5 ms at all sizes from 1.3 KB to
 * 1.3 MB, so the size has a small effect there. In production, the commit also waits for replication,
 * which can take longer for a large write, and then a smaller first layer helps. A lookup of a key that is not in the filter checks all layers, at about 130 ns each.
 */
export const PROMOTION_BLOOM_DEFAULT_EXPECTED_KEYS = 128_000;

/**
 * Words that the comments below use:
 *
 * - Background work: the split and migration steps and the host jobs. The runtime runs them after a
 *   request, and from the Durable Object alarm.
 * - Source: a partition that splits. Target: a new partition that receives a part of the data of its
 *   source. A target copies that data from its source one page at a time.
 * - Router: a hash partition that has split. It keeps no data and sends each request to a child.
 */
export type FokosRuntimeConfig = Readonly<{
	/**
	 * The safety alarm. Before the runtime starts background work, it sets the alarm to at most this
	 * far in the future. If the Durable Object stops during the work, the alarm starts the work again.
	 * A job step that throws also runs again after this time, and a request to a target that still
	 * copies its data sets the alarm to at most this far ahead.
	 */
	fallbackAlarmMs: number;
	/**
	 * When a request gives the runtime new background work, the runtime starts the work after this
	 * delay in the same isolate, with a timer. It does not wait for the alarm.
	 */
	fastPathDelayMs: number;
	/** The maximum number of pages that a target copies from its source in one run of background work. */
	importPagesPerPass: number;
	/**
	 * The memory of the router cache. A router keeps the deeper hash partitions that it learns from the
	 * responses of its children, and sends a later request directly to the deepest one it knows. Read
	 * when the router creates the cache.
	 */
	hashArenaBytes: number;
	/**
	 * The maximum number of rows in the stored table of learned range partition boundaries. A partition
	 * uses these boundaries to send a request directly to the deepest range partition it knows. When
	 * the table is full, the rows that were seen last the longest time ago are deleted first. Read at
	 * each deletion, so a lower value takes effect at the next write to the table.
	 */
	rangeHierarchyMaxRows: number;
	/**
	 * When a response teaches a boundary that is already stored, the runtime updates its last-seen time
	 * only when the stored time is older than this. A lower value gives more writes and a more exact
	 * delete order.
	 */
	rangeHierarchyRefreshMs: number;
	/**
	 * A hash partition keeps a Bloom filter of the hash keys that moved to a range partition of their
	 * own, and sends a request for such a key directly there. This is the number of keys that the
	 * filter holds at `promotionBloomFalsePositiveRate` before it grows. Read when the filter is created.
	 */
	promotionBloomExpectedKeys: number;
	/** The false positive rate of that Bloom filter. Read when the filter is created. */
	promotionBloomFalsePositiveRate: number;
	/**
	 * The longest time that a newly learned promoted key stays only in the memory of the Bloom filter.
	 * The runtime writes the whole filter at most once in this time, in a timer task of its own, so the
	 * write does not add to the commit of a request. A key that is not written when the instance stops
	 * costs one more forward until the partition learns it again. 0 writes the filter in the request
	 * that learns the key.
	 */
	promotionBloomFlushMs: number;
	/**
	 * A cache can send a request to a partition that no longer owns the key. The runtime then finds the
	 * owner again without that cache, and sends the request again. This is the maximum number of these
	 * retries for one request.
	 */
	maxForwardRetries: number;
	/**
	 * A source retries a failed step, for example a call to create or start a target, with a random
	 * delay. The maximum delay starts at `sourceRetryBaseMs`, doubles at each attempt, and stops at
	 * `sourceRetryMaxMs`.
	 */
	sourceRetryBaseMs: number;
	sourceRetryMaxMs: number;
	/**
	 * A target retries a failed page copy or a failed completion message in the same way, from
	 * `importRetryBaseMs` up to `importRetryMaxMs`. A new target also sets its alarm this far ahead, so
	 * that it starts to copy when the start message of its source does not arrive.
	 */
	importRetryBaseMs: number;
	importRetryMaxMs: number;
	/**
	 * When the `beforeCutover` hook of the host holds a repartition, the source asks the hook again
	 * after this time.
	 */
	cutoverHoldRetryMs: number;
	/** After a split is complete, the source deletes the data that moved, in steps. This is the time between two steps. */
	cleanupRetryMs: number;
	/**
	 * A source gives its keys to its targets only after it creates all of them. When a target asks for a
	 * page before that, it asks again after this fixed time.
	 */
	notCutOverRetryMs: number;
	/**
	 * When a source or a target names a split that the other side does not know, no retry can help. The
	 * runtime keeps the state, logs the error, and tries again only after this long time.
	 */
	nonRetryableRetryMs: number;
	/**
	 * The maximum estimated size of one page that a target copies from its source. One page is one RPC,
	 * so the value must stay below the RPC message limit.
	 */
	migrationPageBytes: number;
	/** The maximum number of rows in one page that a target copies from its source. */
	migrationPageRows: number;
	/**
	 * The maximum number of source rows that one page reads. A row that belongs to a different target is
	 * read and not sent, so this limit stops a page that finds few rows of its target.
	 */
	migrationScanRows: number;
	/** The maximum number of entries in one page of `fokosPromotions`, which lists the key promotions of a partition. */
	promotionsPageEntries: number;
	/** The maximum estimated size of one page of `fokosPromotions`. */
	promotionsPageBytes: number;
}>;

export type FokosRuntimeConfigOverrides = Partial<FokosRuntimeConfig>;

/** The budget of one migration page, which the runtime gives to `MigrationHost.buildPage`. */
export type FokosMigrationPageBudget = { pageBytes: number; pageRows: number; scanRows: number };

export const DEFAULT_RUNTIME_CONFIG: FokosRuntimeConfig = Object.freeze({
	fallbackAlarmMs: 5_000,
	fastPathDelayMs: 50,
	importPagesPerPass: 16,
	hashArenaBytes: 1024 * 1024,
	rangeHierarchyMaxRows: 10_000,
	rangeHierarchyRefreshMs: 60_000,
	promotionBloomExpectedKeys: PROMOTION_BLOOM_DEFAULT_EXPECTED_KEYS,
	promotionBloomFalsePositiveRate: 0.01,
	promotionBloomFlushMs: 5_000,
	maxForwardRetries: 8,
	sourceRetryBaseMs: 5_000,
	sourceRetryMaxMs: 5 * 60_000,
	importRetryBaseMs: 10_000,
	importRetryMaxMs: 5 * 60_000,
	cutoverHoldRetryMs: 5_000,
	cleanupRetryMs: 5_000,
	notCutOverRetryMs: 10_000,
	nonRetryableRetryMs: 5 * 60_000,
	migrationPageBytes: 20 * 1024 * 1024,
	migrationPageRows: 1_000,
	migrationScanRows: 10_000,
	promotionsPageEntries: 1_000,
	promotionsPageBytes: 20 * 1024 * 1024,
});

const RUNTIME_CONFIG_KEYS = Object.keys(DEFAULT_RUNTIME_CONFIG) as (keyof FokosRuntimeConfig)[];

/** The smallest valid value of each integer setting. */
const MINIMUMS: { [K in keyof FokosRuntimeConfig]?: number } = {
	fallbackAlarmMs: 1,
	fastPathDelayMs: 0,
	importPagesPerPass: 1,
	// One 4-byte slot for each child of the root block, with `hashSplitN` up to 255.
	hashArenaBytes: 4 * 255,
	rangeHierarchyMaxRows: 1,
	rangeHierarchyRefreshMs: 0,
	promotionBloomExpectedKeys: 1,
	promotionBloomFlushMs: 0,
	maxForwardRetries: 0,
	sourceRetryBaseMs: 1,
	sourceRetryMaxMs: 1,
	importRetryBaseMs: 1,
	importRetryMaxMs: 1,
	cutoverHoldRetryMs: 1,
	cleanupRetryMs: 1,
	notCutOverRetryMs: 1,
	nonRetryableRetryMs: 1,
	migrationPageBytes: 1,
	migrationPageRows: 1,
	migrationScanRows: 1,
	promotionsPageEntries: 1,
	promotionsPageBytes: 1,
};

/**
 * Merges `overrides` with the defaults and validates the result. It ignores a key that it does not
 * know and a value that is `undefined`. It throws on a value that is not valid.
 */
export function resolveRuntimeConfig(overrides: FokosRuntimeConfigOverrides | undefined): FokosRuntimeConfig {
	if (overrides === undefined) {
		return DEFAULT_RUNTIME_CONFIG;
	}
	// The runtime resolves at each use, so the copy happens only when a value is overridden.
	let config: Record<string, number> | undefined;
	for (const key of RUNTIME_CONFIG_KEYS) {
		const value = overrides[key];
		if (value !== undefined) {
			config ??= { ...DEFAULT_RUNTIME_CONFIG };
			config[key] = value;
		}
	}
	if (config === undefined) {
		return DEFAULT_RUNTIME_CONFIG;
	}
	const resolved = config as FokosRuntimeConfig;
	validate(resolved);
	return Object.freeze(resolved);
}

function validate(c: FokosRuntimeConfig): void {
	for (const [key, min] of Object.entries(MINIMUMS)) {
		const value = c[key as keyof FokosRuntimeConfig];
		check(Number.isSafeInteger(value) && value >= min, () => `${key} must be an integer of at least ${min}, got ${value}`);
	}
	const fpr = c.promotionBloomFalsePositiveRate;
	check(Number.isFinite(fpr) && fpr > 0 && fpr < 1, () => `promotionBloomFalsePositiveRate must be in (0, 1), got ${fpr}`);
	const bloomBytes = bloomInitialLayerBytes(fpr, c.promotionBloomExpectedKeys);
	check(
		bloomBytes <= PROMOTION_BLOOM_MAX_BYTES,
		() =>
			`the promotion Bloom filter needs ${bloomBytes} bytes for promotionBloomExpectedKeys and promotionBloomFalsePositiveRate, above its ceiling of ${PROMOTION_BLOOM_MAX_BYTES}`,
	);
	check(
		c.migrationPageBytes <= RPC_MESSAGE_MAX_BYTES,
		() => `migrationPageBytes must be at most ${RPC_MESSAGE_MAX_BYTES}, got ${c.migrationPageBytes}`,
	);
	check(
		c.promotionsPageBytes <= RPC_MESSAGE_MAX_BYTES,
		() => `promotionsPageBytes must be at most ${RPC_MESSAGE_MAX_BYTES}, got ${c.promotionsPageBytes}`,
	);
	check(c.sourceRetryBaseMs <= c.sourceRetryMaxMs, () => "sourceRetryBaseMs must be at most sourceRetryMaxMs");
	check(c.importRetryBaseMs <= c.importRetryMaxMs, () => "importRetryBaseMs must be at most importRetryMaxMs");
}

function check(condition: boolean, message: () => string): asserts condition {
	invariant(condition, () => `fokos/runtime-config: ${message()}`);
}
