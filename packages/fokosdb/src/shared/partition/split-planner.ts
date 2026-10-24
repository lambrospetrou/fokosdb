import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import type { FokosPartitionIdentity } from "../../sharding/route-context.js";
import type { FokosShardingHooks } from "../../sharding/runtime-types.js";
import invariant from "../invariant.js";
import type { FokosDBPolicy } from "../partition-context.js";
import type { KeySizeStats } from "./key-size-stats.js";

/**
 * The most keys that one split decision asks to promote. When this many keys all have promotions
 * that cannot finish, the next large key waits until one of them finishes.
 */
const PROMOTION_CANDIDATES_MAX = 5;

/** The data of the host that the split planner reads. */
export type SplitSource = {
	/** The first hash key of the partition, and whether another hash key follows it, or null when it has no key. */
	probeHashKeys(): { first: KeyBytes; more: boolean } | null;
	/**
	 * The rows of `hashKey` in `[start, end)` in sort-key order, each with its size. The size of each
	 * row is the size that the host gives to `KeySizeStats`. The planner stops early, so the host must
	 * read the rows one by one, and not load them all.
	 */
	sortKeySizes(hashKey: KeyBytes, start: KeyBytes | null, end: KeyBytes | null): Iterable<{ sk: KeyBytes; bytes: number }>;
};

/** Why no repartition can make a partition smaller. */
export type SplitFloor = "no_hash_key" | "fewer_items" | "skewed_bytes";

/**
 * The range split of one slice: its boundaries, or why it has none.
 * - `fewer_items`: the slice holds fewer items than children.
 * - `skewed_bytes`: the bytes are so skewed that the scan finds fewer boundaries than it needs.
 */
export type RangeSplitPlan = { boundaries: KeyBytes[] } | { floor: "fewer_items" | "skewed_bytes" };

/** A hash key that a write changed, with the estimate of the key after the write. */
type PromotionCandidate = { hashKey: KeyBytes; keyEstBytes: number };

type EvaluateSplitAnswer = ReturnType<FokosShardingHooks<FokosDBPolicy>["evaluateSplit"]>;

export type SplitPlannerOptions = {
	keySizes: KeySizeStats;
	source: SplitSource;
	databaseSize: () => number;
	/** The part of the hash split size at which a hash key gets its own partition. */
	promotionFraction: () => number;
	/** The number of children of a range split. */
	rangeSplitN: () => number;
	/** The prefix of the log message, for example `fokos/partition`. */
	logName: string;
	logParams: () => Record<string, unknown>;
};

/** The size above which a partition must split, from the policy of its kind, or 0 when it has no limit. */
export function maxSplitBytes(identity: FokosPartitionIdentity, policy: FokosDBPolicy): number {
	const maxSizeMb = (identity.kind === "hash" ? policy.hashSplitConditions : policy.rangeSplitConditions)?.maxSizeMb;
	return (maxSizeMb ?? 0) * 1024 * 1024;
}

/**
 * The split decisions of a partition that uses hash splits, promotions of large hash keys, and range
 * splits of a promoted key. The host owns the rows. The planner reads the size of each hash key from
 * `KeySizeStats`, and the rows of one key from `SplitSource`.
 */
export class SplitPlanner {
	#opts: SplitPlannerOptions;
	#floorLogged = false;

	constructor(opts: SplitPlannerOptions) {
		this.#opts = opts;
	}

	/**
	 * The answer of the `evaluateSplit` hook. At the floor it returns false, and it logs the floor one
	 * time until the partition can split again. At the floor the partition refuses each write until
	 * an operator raises the size limit or deletes data.
	 */
	evaluateSplit(input: { identity: FokosPartitionIdentity; policy: FokosDBPolicy }): EvaluateSplitAnswer {
		const decision = this.#decide(input.identity, input.policy);
		if ("answer" in decision) {
			this.#floorLogged = false;
			return decision.answer;
		}
		if (!this.#floorLogged) {
			this.#floorLogged = true;
			console.error({
				...input.identity.ref,
				...this.#opts.logParams(),
				message: `${this.#opts.logName}: over the split threshold, but no repartition can make this partition smaller`,
				reason: decision.floor,
				maxSizeBytes: decision.maxSizeBytes,
			});
		}
		return false;
	}

	/** The answer of the `computeRangeBoundaries` hook: the boundaries, or null. */
	rangeBoundaries(hashKey: KeyBytes, start: KeyBytes | null, end: KeyBytes | null, N: number): KeyBytes[] | null {
		const plan = this.#planRangeSplit(hashKey, start, end, N);
		return "boundaries" in plan ? plan.boundaries : null;
	}

	/**
	 * The keys of `candidates` that a hash partition must promote. It keeps the largest candidate of
	 * each key: one transaction can write many sort keys of one hash key, and each write reports the
	 * estimate after its own row.
	 */
	promotionCandidates(
		identity: FokosPartitionIdentity,
		policy: FokosDBPolicy,
		candidates: readonly PromotionCandidate[],
	): { hashKey: KeyBytes }[] {
		if (candidates.length === 0 || identity.kind !== "hash") {
			return [];
		}
		const threshold = maxSplitBytes(identity, policy) * this.#opts.promotionFraction();
		if (threshold <= 0) {
			return [];
		}
		const largest = new Map<string, PromotionCandidate>();
		for (const candidate of candidates) {
			const id = candidate.hashKey.toBase64({ alphabet: "base64url" });
			const seen = largest.get(id);
			if (!seen || candidate.keyEstBytes > seen.keyEstBytes) {
				largest.set(id, candidate);
			}
		}
		const promote: { hashKey: KeyBytes }[] = [];
		for (const { hashKey, keyEstBytes } of largest.values()) {
			if (keyEstBytes >= threshold) {
				promote.push({ hashKey });
			}
		}
		return promote;
	}

	/**
	 * The split answer, or the floor when this leaf is over its limit and no repartition can make it
	 * smaller. Under the limit it reads the database size only.
	 *
	 * A hash leaf probes for two hash keys. With two or more keys, the leaf promotes its keys at or
	 * above the promotion threshold, up to `PROMOTION_CANDIDATES_MAX` and largest first, and splits
	 * when it has none. One key is promoted whatever its estimate: the limit reads the physical file,
	 * which can be much larger than the estimate. A range leaf splits when the planner finds its
	 * boundaries, from the same arguments that the `computeRangeBoundaries` hook gets.
	 */
	#decide(
		identity: FokosPartitionIdentity,
		policy: FokosDBPolicy,
	): { answer: EvaluateSplitAnswer } | { floor: SplitFloor; maxSizeBytes: number } {
		const maxSizeBytes = maxSplitBytes(identity, policy);
		if (maxSizeBytes === 0 || this.#opts.databaseSize() <= maxSizeBytes) {
			return { answer: false };
		}
		if (identity.kind === "range") {
			invariant(identity.range, `fokos/split-planner: range identity must have a range`);
			const { hashKey, start, end } = identity.range;
			const plan = this.#planRangeSplit(hashKey, start, end, this.#opts.rangeSplitN());
			return "boundaries" in plan ? { answer: {} } : { floor: plan.floor, maxSizeBytes };
		}
		const keys = this.#opts.source.probeHashKeys();
		if (!keys) {
			return { floor: "no_hash_key", maxSizeBytes };
		}
		if (!keys.more) {
			return { answer: { promote: [keys.first] } };
		}
		// A split copies a large key into a child, and the child promotes it with a second copy. A
		// promotion that cannot finish blocks the split, and a refused write signals no promotion.
		// So the large keys are promoted first, and each promotion answers independently.
		const large = this.#opts.keySizes.largestAtLeast(maxSizeBytes * this.#opts.promotionFraction(), PROMOTION_CANDIDATES_MAX);
		return { answer: large.length > 0 ? { promote: large } : {} };
	}

	/**
	 * The range split plan of `[start, end)` of `hashKey`. The caller gives the slice that this
	 * partition owns, so the rows of the slice are all the rows of the key here, and the estimate of
	 * the key is the byte total of the slice.
	 */
	#planRangeSplit(hashKey: KeyBytes, start: KeyBytes | null, end: KeyBytes | null, N: number): RangeSplitPlan {
		return planRangeBoundaries(this.#opts.keySizes.bytesOf(hashKey), N, start, this.#opts.source.sortKeySizes(hashKey, start, end));
	}
}

/**
 * Computes N-1 strictly increasing boundaries at the byte quantiles of `rows`, which are in sort-key
 * order from `start`. When the rows cannot give N non-empty children, it returns the reason: fewer
 * than N rows, or bytes so skewed that the scan finds fewer boundaries.
 *
 * One scan adds the row sizes, and emits a boundary each time the total crosses the next threshold.
 * It stops at the last boundary. The split starts because of the size, so the children get equal
 * bytes, not equal row counts. Thus one large row gets its own child.
 *
 * Each boundary is the shortest separator of the row before it and the row that crosses the
 * threshold, so the DO names and the topology stay small.
 */
export function planRangeBoundaries(
	totalBytes: number,
	N: number,
	start: KeyBytes | null,
	rows: Iterable<{ sk: KeyBytes; bytes: number }>,
): RangeSplitPlan {
	if (totalBytes <= 0) {
		return { floor: "fewer_items" };
	}
	const step = totalBytes / N;
	const boundaries: KeyBytes[] = [];
	let rowCount = 0;
	let acc = 0;
	let threshold = step;
	let prev: KeyBytes | null = null;
	for (const { sk, bytes } of rows) {
		rowCount++;
		acc += bytes;
		// The first row is never a boundary, so child 0 always has at least one row.
		if (prev !== null && acc >= threshold) {
			// The row that crosses the threshold goes to the upper child.
			boundaries.push(KeyCodec.shortestSeparator(prev, sk));
			// The next threshold starts from this row (acc + step, not threshold + step). When one large
			// row crosses many thresholds, the scan emits one boundary, so no two boundaries fall
			// between the same two rows, and each child gets at least one row.
			threshold = acc + step;
			if (boundaries.length === N - 1) {
				break;
			}
		}
		prev = sk;
	}
	// Each boundary needs a new row after the first row, so N-1 boundaries mean at least N rows.
	if (boundaries.length !== N - 1) {
		return { floor: rowCount < N ? "fewer_items" : "skewed_bytes" };
	}
	const lower = start ?? KeyCodec.encodeOptional(undefined);
	for (let i = 0; i < boundaries.length; i++) {
		invariant(KeyCodec.compare(boundaries[i], lower) > 0, `fokos/split-planner: boundary is not above the lower bound`);
		invariant(
			i === 0 || KeyCodec.compare(boundaries[i], boundaries[i - 1]) > 0,
			`fokos/split-planner: boundaries are not strictly increasing`,
		);
	}
	return { boundaries };
}
