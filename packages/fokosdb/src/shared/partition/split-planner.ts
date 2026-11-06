import { maxHashDepth, type RoutingHash } from "../../sharding/hash-primitives.js";
import { KeyCodec, type FokosCompositeKey, type KeyBytes } from "../../sharding/key-codec.js";
import type { FokosPartitionIdentity } from "../../sharding/route-context.js";
import type { FokosShardingHooks } from "../../sharding/runtime-types.js";
import invariant from "../invariant.js";
import type { FokosDBPolicy } from "../partition-context.js";
import { estimate, itemFacts, type StorageModel } from "./item-size.js";
import type { ItemStats } from "./item-stats.js";

/**
 * The most routingHashes that one split decision asks to promote. When this many all have
 * promotions that cannot finish, the next large one waits until one of them finishes.
 */
const PROMOTION_CANDIDATES_MAX = 5;

/** One item of a slice, with the facts that the storage model prices. */
export type ItemSizeFacts = { hk: KeyBytes; sk: KeyBytes; itemBytes: number; indexed: boolean };

/** The data of the host that the split planner reads. */
export type SplitSource = {
	/**
	 * A hash key of the first routingHash of the partition, and whether another routingHash
	 * follows it, or null when the partition has no key.
	 */
	probeRoutingHashes(): { first: KeyBytes; more: boolean } | null;
	/**
	 * The items of `routingHash` in the composite interval `[start, end)` in `(hk, sk)` order, each
	 * with its size facts. The planner stops early, so the host must read the items one by one, and
	 * not load them all. It must not read the item data.
	 */
	compositeKeyFacts(routingHash: RoutingHash, start: FokosCompositeKey | null, end: FokosCompositeKey | null): Iterable<ItemSizeFacts>;
	/** The count of the items of `routingHash` in `[start, end)`, at most `limit`. It reads at most `limit` items. */
	countItems(routingHash: RoutingHash, start: FokosCompositeKey | null, end: FokosCompositeKey | null, limit: number): number;
};

/**
 * Why no repartition can make a partition smaller. `max_depth`: a hash leaf at the depth limit of its
 * topology has no large key to promote, and cannot hash-split. `fewer_items`: a range leaf holds
 * fewer items than the children of a range split.
 */
export type SplitFloor = "no_hash_key" | "fewer_items" | "max_depth";

/** A hash key that a write changed, with the estimate of its routingHash after the write. */
type PromotionCandidate = { hashKey: KeyBytes; keyEstBytes: number };

type EvaluateSplitAnswer = ReturnType<FokosShardingHooks<FokosDBPolicy>["evaluateSplit"]>;

export type SplitPlannerOptions = {
	itemStats: ItemStats;
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
 * The promotion floor of a partition: the promotion threshold `T` times `promotionIndexFraction`. A
 * bucket stores its estimate in `promotion_est_bytes` only at or above the floor. It is null on a
 * range leaf and on a partition with no size limit, because those have no promotion.
 *
 * It is 0 on a hash leaf at the depth limit. That leaf cannot split, so after a cap reduction the
 * lookup must find each routingHash at or above the new `T` with no write of it. A leaf that refuses
 * writes for size writes no bucket, so a positive floor could hide its large routingHash.
 */
export function promotionFloor(
	identity: FokosPartitionIdentity,
	policy: FokosDBPolicy,
	fractions: { promotionFraction: number; promotionIndexFraction: number },
): number | null {
	if (identity.kind !== "hash") {
		return null;
	}
	const threshold = maxSplitBytes(identity, policy) * fractions.promotionFraction;
	if (threshold <= 0) {
		return null;
	}
	invariant(identity.hash, "fokos/split-planner: hash identity must have a hash");
	const { rootTreesN, hashSplitN } = identity.topology;
	return identity.hash.depth >= maxHashDepth(rootTreesN, hashSplitN) ? 0 : threshold * fractions.promotionIndexFraction;
}

/**
 * The split decisions of a partition that uses hash splits, promotions of large routingHashes,
 * and range splits of a promoted routingHash. The host owns the rows. The planner reads the stored
 * promotion estimates and the bucket facts from `ItemStats`, and the items of a slice from `SplitSource`.
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

	/**
	 * The answer of the `computeRangeBoundaries` hook: the `N - 1` boundaries of the slice, or null when
	 * the slice holds fewer than `N` items. All reads run in one storage transaction.
	 */
	rangeBoundaries(routingHash: RoutingHash, start: FokosCompositeKey | null, end: FokosCompositeKey | null, N: number): FokosCompositeKey[] | null {
		const { itemStats, source } = this.#opts;
		if (source.countItems(routingHash, start, end, N) < N) {
			return null;
		}
		const model = itemStats.model;
		const total = estimate(model, itemStats.factsOf(routingHash));
		return planRangeBoundaries(model, total, N, start, source.compositeKeyFacts(routingHash, start, end));
	}

	/**
	 * The keys of `candidates` that a hash partition must promote. The estimate of a candidate is the
	 * rounded estimate of its routingHash, so colliding hash keys add up. It keeps the largest candidate
	 * of each key: one transaction can write many sort keys of one hash key, and each write reports the
	 * estimate after its own row. The runtime promotes the routingHash of a key one time.
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
	 * A hash leaf probes for two routingHashes. A hash split cannot separate two hash keys of one
	 * routingHash, so they count as one. With two or more routingHashes, the leaf promotes the
	 * ones at or above the promotion threshold, up to `PROMOTION_CANDIDATES_MAX` and largest first, and
	 * splits when it has none. At the hash depth limit it cannot split, so with no large one it is at
	 * the floor. One routingHash is promoted whatever its estimate: the limit reads the physical
	 * file, which can be much larger than the estimate.
	 *
	 * A range leaf splits when its slice holds at least `rangeSplitN` items, because the planner then
	 * always finds the boundaries. It counts at most `rangeSplitN` items and does not call the planner:
	 * the decision runs before a refused write gets its error.
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
			const { routingHash, start, end } = identity.range;
			const N = this.#opts.rangeSplitN();
			return this.#opts.source.countItems(routingHash, start, end, N) >= N ? { answer: {} } : { floor: "fewer_items", maxSizeBytes };
		}
		const keys = this.#opts.source.probeRoutingHashes();
		if (!keys) {
			return { floor: "no_hash_key", maxSizeBytes };
		}
		if (!keys.more) {
			return { answer: { promote: [keys.first] } };
		}
		// A split copies a large routingHash into a child, and the child promotes it with a second
		// copy. A promotion that cannot finish blocks the split, and a refused write signals no promotion.
		// So the large routingHashes are promoted first, and each promotion answers independently.
		const large = this.#opts.itemStats.largestAtLeast(maxSizeBytes * this.#opts.promotionFraction(), PROMOTION_CANDIDATES_MAX);
		if (large.length > 0) {
			return { answer: { promote: large } };
		}
		invariant(identity.hash, "fokos/split-planner: hash identity must have a hash");
		const { rootTreesN, hashSplitN } = identity.topology;
		return identity.hash.depth >= maxHashDepth(rootTreesN, hashSplitN) ? { floor: "max_depth", maxSizeBytes } : { answer: {} };
	}
}

/**
 * Computes the `N - 1` boundaries of a range split of the items in `items`, which are in `(hk, sk)`
 * order from `start`. It returns null when `items` holds fewer than `N` items.
 *
 * `total` is the estimate of the slice from its facts. The target of boundary `i` is `i * total / N`.
 * A gap is the position between two adjacent items, and a cut at a gap gives the lower child exactly
 * the items before it. For each target in order, the planner selects the gap whose estimate before it
 * is nearest the target, after the previous boundary. When two gaps have the same distance, it selects
 * the earlier gap. No gap exists before the first item or after the last, so each child holds at least
 * one item. The scan stops when it has `N - 1` boundaries.
 *
 * When the items end with fewer boundaries, a large item near the end took the remaining gaps. The
 * repair keeps the boundaries before the gaps of the last `N` items, and fills the missing boundaries
 * from those gaps, latest gap first.
 *
 * A wrong `total` gives a worse balance, but the boundaries stay valid. An item cannot cross a
 * boundary, so a dominant item makes equal children impossible. A child that is still over its cap
 * splits again.
 *
 * Each boundary is the shortest separator of the two items at its gap, so the DO names and the
 * topology stay small.
 */
export function planRangeBoundaries(
	model: StorageModel,
	total: number,
	N: number,
	start: FokosCompositeKey | null,
	items: Iterable<ItemSizeFacts>,
): FokosCompositeKey[] | null {
	invariant(N >= 2, `fokos/split-planner: a range split needs at least 2 children, got ${N}`);
	const step = total / N;
	// The gaps of the chosen boundaries. Gap `j` is after item `j`, counted from 1.
	const gaps: number[] = [];
	const boundaries: FokosCompositeKey[] = [];
	// The last N + 1 items: the two items at the gap before the current gap, and the last N items for
	// the repair. Item `count - recent.length + 1 + i` is `recent[i]`.
	const recent: FokosCompositeKey[] = [];
	let count = 0;
	// The estimate of the items before the item that the scan reads now.
	let before = 0;
	// The last gap before the current target, when it is after the previous boundary.
	let candidate: { gap: number; before: number } | null = null;

	const separatorAt = (gap: number) => {
		const lower = gap - count + recent.length - 1;
		return KeyCodec.shortestCompositeSeparator(recent[lower], recent[lower + 1]);
	};
	const choose = (gap: number) => {
		gaps.push(gap);
		boundaries.push(separatorAt(gap));
		candidate = null;
	};

	for (const item of items) {
		const key = { hk: item.hk, sk: item.sk };
		count++;
		recent.push(key);
		if (recent.length > N + 1) {
			recent.shift();
		}
		if (count > 1) {
			// Gap `count - 1` is now known: the estimate before it is `before`.
			const gap = count - 1;
			while (boundaries.length < N - 1) {
				const target = step * (boundaries.length + 1);
				if (before < target) {
					candidate = { gap, before };
					break;
				}
				const c: { gap: number; before: number } | null = candidate;
				if (c !== null && target - c.before <= before - target) {
					choose(c.gap);
					continue;
				}
				choose(gap);
				break;
			}
			if (boundaries.length === N - 1) {
				break;
			}
		}
		before += estimate(model, itemFacts(model, { itemBytes: item.itemBytes, keyBytes: item.hk.byteLength + item.sk.byteLength, indexed: item.indexed }));
	}
	if (count < N) {
		return null;
	}
	if (boundaries.length < N - 1) {
		const last: { gap: number; before: number } | null = candidate;
		if (last !== null) {
			choose(last.gap);
		}
	}
	if (boundaries.length < N - 1) {
		// The gaps of the last N items are `count - N + 1` to `count - 1`.
		const firstRecentGap = count - N + 1;
		let kept = 0;
		while (kept < gaps.length && gaps[kept] < firstRecentGap) {
			kept++;
		}
		const missing = N - 1 - kept;
		boundaries.length = kept;
		for (let gap = count - missing; gap < count; gap++) {
			boundaries.push(separatorAt(gap));
		}
	}
	for (let i = 0; i < boundaries.length; i++) {
		invariant(
			start === null || KeyCodec.compareComposite(boundaries[i], start) > 0,
			`fokos/split-planner: boundary is not above the lower bound`,
		);
		invariant(
			i === 0 || KeyCodec.compareComposite(boundaries[i], boundaries[i - 1]) > 0,
			`fokos/split-planner: boundaries are not strictly increasing`,
		);
	}
	return boundaries;
}
