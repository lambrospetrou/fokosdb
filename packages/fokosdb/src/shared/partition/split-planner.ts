import { maxHashDepth, type RoutingHash } from "../../sharding/hash-primitives.js";
import { KeyCodec, type FokosCompositeKey, type KeyBytes } from "../../sharding/key-codec.js";
import type { FokosPartitionIdentity } from "../../sharding/route-context.js";
import type { FokosShardingHooks } from "../../sharding/runtime-types.js";
import invariant from "../invariant.js";
import type { FokosDBPolicy } from "../partition-context.js";
import { estimate, itemFacts, type StorageModel } from "./item-size.js";
import type { Bucket, ItemStats } from "./item-stats.js";

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
	 * The items of `routingHash` in the composite interval `[start, end)` in `(hk, sk)` order, or in the
	 * reverse order for `desc`, each with its size facts. The planner stops early, so the host must
	 * read the items one by one, and not load them all. It must not read the item data.
	 */
	compositeKeyFacts(
		routingHash: RoutingHash,
		start: FokosCompositeKey | null,
		end: FokosCompositeKey | null,
		order?: "asc" | "desc",
	): Iterable<ItemSizeFacts>;
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
	rangeBoundaries(
		routingHash: RoutingHash,
		start: FokosCompositeKey | null,
		end: FokosCompositeKey | null,
		N: number,
	): FokosCompositeKey[] | null {
		const { itemStats, source } = this.#opts;
		if (source.countItems(routingHash, start, end, N) < N) {
			return null;
		}
		// Each read stays inside the slice: a bucket bound can be below `start`.
		const read: SliceRead = (from, to, order) => source.compositeKeyFacts(routingHash, laterKey(start, from), earlierKey(end, to), order);
		return planBucketBoundaries(itemStats.model, N, start, itemStats.buckets(routingHash, end), read);
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
		before += estimate(
			model,
			itemFacts(model, { itemBytes: item.itemBytes, keyBytes: item.hk.byteLength + item.sk.byteLength, indexed: item.indexed }),
		);
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
	return checkedBoundaries(boundaries, start);
}

/** Reads the items of a slice in `[from, to)`, in key order or in reverse order. A null edge is the edge of the slice. */
export type SliceRead = (from: FokosCompositeKey | null, to: FokosCompositeKey | null, order: "asc" | "desc") => Iterable<ItemSizeFacts>;

/** A gap between two adjacent items. An unknown item is read only when the planner needs it. */
type Gap = { lower: ItemSizeFacts | null; upper: ItemSizeFacts | null; before: number };

/**
 * The boundaries of `planRangeBoundaries` from the buckets of the slice. It gives the same boundaries
 * as `planRangeBoundaries` over the same items when the facts of the buckets are exact. The caller
 * checks before that the slice holds at least `N` items.
 *
 * The estimate before each bucket comes from the bucket facts, and its total is `B`. For each target
 * `i * B / N`, the planner scans the bucket that holds the target one time. The candidate gaps are the
 * start of the bucket, the gaps inside it, and the start of the next bucket. When the gap nearest the
 * target is at or before the previous boundary, the boundary is the first gap after the previous
 * boundary: one more item read. When the items end with fewer boundaries, the repair reads the last
 * `N` items, as in `planRangeBoundaries`.
 *
 * The planner identifies a gap by its items, not by a count from the facts. Thus wrong facts give a
 * worse balance, but the boundaries stay valid.
 */
export function planBucketBoundaries(
	model: StorageModel,
	N: number,
	start: FokosCompositeKey | null,
	buckets: readonly Bucket[],
	read: SliceRead,
): FokosCompositeKey[] {
	invariant(N >= 2, `fokos/split-planner: a range split needs at least 2 children, got ${N}`);
	const weight = (item: ItemSizeFacts) =>
		estimate(
			model,
			itemFacts(model, { itemBytes: item.itemBytes, keyBytes: item.hk.byteLength + item.sk.byteLength, indexed: item.indexed }),
		);
	const firstOf = (items: Iterable<ItemSizeFacts>) => {
		for (const item of items) {
			return item;
		}
		return null;
	};
	const itemBefore = (item: ItemSizeFacts) => firstOf(read(null, item, "desc"));
	// The smallest composite key above `(hk, sk)` is `(hk, sk || 0x00)`, so this read starts at the next item.
	const itemAfter = (item: ItemSizeFacts) =>
		firstOf(read({ hk: item.hk, sk: KeyCodec.asKeyBytes(new Uint8Array([...item.sk, 0])) }, null, "asc"));
	// Reads the unknown item of a gap. It returns false when no such item exists, so the gap does not exist.
	const resolve = (gap: Gap & { next?: FokosCompositeKey | null }) => {
		gap.lower ??= itemBefore(gap.upper!);
		if (gap.upper === null) {
			// The end gap of a bucket: its upper item is the first item at or above the next bucket bound.
			gap.upper = gap.next === null ? null : firstOf(read(gap.next!, null, "asc"));
		}
		return gap.lower !== null && gap.upper !== null;
	};
	const isAfter = (gap: Gap, previous: Gap) =>
		gap.lower !== null
			? KeyCodec.compareComposite(gap.lower, previous.lower!) > 0
			: KeyCodec.compareComposite(gap.upper!, previous.upper!) > 0;

	// The estimate before each bucket.
	const lowerEst: number[] = [];
	let total = 0;
	for (const bucket of buckets) {
		lowerEst.push(total);
		total += estimate(model, bucket.facts);
	}
	const step = total / N;
	// The candidate gaps of each scanned bucket. Each bucket is scanned at most one time.
	const scanned = new Map<number, Gap[]>();
	const gapsOf = (b: number): Gap[] => {
		let gaps = scanned.get(b);
		if (gaps === undefined) {
			const items = [...read(buckets[b].bound, buckets[b + 1]?.bound ?? null, "asc")];
			gaps = [];
			if (items.length > 0) {
				let before = lowerEst[b];
				gaps.push({ lower: null, upper: items[0], before });
				for (let i = 0; i + 1 < items.length; i++) {
					before += weight(items[i]);
					gaps.push({ lower: items[i], upper: items[i + 1], before });
				}
				const end: Gap & { next: FokosCompositeKey | null } = {
					lower: items[items.length - 1],
					upper: null,
					before: lowerEst[b] + estimate(model, buckets[b].facts),
					next: buckets[b + 1]?.bound ?? null,
				};
				gaps.push(end);
			}
			scanned.set(b, gaps);
		}
		return gaps;
	};

	let chosen: Gap[] = [];
	for (let k = 1; k < N; k++) {
		const target = step * k;
		const b = lowerEst.findIndex((lower, i) => lower < target && target <= lower + estimate(model, buckets[i].facts));
		if (b === -1) {
			break;
		}
		const gaps = gapsOf(b);
		let nearBelow = gaps.findLast((gap) => gap.before < target);
		let nearAbove = gaps.find((gap) => gap.before >= target);
		if (nearBelow !== undefined && !resolve(nearBelow)) {
			nearBelow = undefined;
		}
		if (nearAbove !== undefined && !resolve(nearAbove)) {
			nearAbove = undefined;
		}
		const previous = chosen.at(-1);
		if (nearBelow !== undefined && previous !== undefined && !isAfter(nearBelow, previous)) {
			nearBelow = undefined;
		}
		let pick: Gap | undefined;
		if (nearAbove !== undefined && previous !== undefined && !isAfter(nearAbove, previous)) {
			// Each gap near the target is at or before the previous boundary.
			const upper = itemAfter(previous.upper!);
			pick = upper === null ? undefined : { lower: previous.upper, upper, before: Number.NaN };
		} else if (nearAbove !== undefined) {
			pick = nearBelow !== undefined && target - nearBelow.before <= nearAbove.before - target ? nearBelow : nearAbove;
		} else {
			pick = nearBelow;
		}
		if (pick === undefined) {
			break;
		}
		chosen.push(pick);
		if (nearAbove === undefined) {
			break;
		}
	}
	if (chosen.length < N - 1) {
		const last: ItemSizeFacts[] = [];
		for (const item of read(null, null, "desc")) {
			last.unshift(item);
			if (last.length === N) {
				break;
			}
		}
		invariant(last.length === N, `fokos/split-planner: the slice holds fewer than ${N} items`);
		const kept = chosen.filter((gap) => KeyCodec.compareComposite(gap.lower!, last[0]) < 0);
		const missing = N - 1 - kept.length;
		for (let i = N - 1 - missing; i < N - 1; i++) {
			kept.push({ lower: last[i], upper: last[i + 1], before: Number.NaN });
		}
		chosen = kept;
	}
	return checkedBoundaries(
		chosen.map((gap) => KeyCodec.shortestCompositeSeparator(gap.lower!, gap.upper!)),
		start,
	);
}

/** Checks that each boundary is above `start` and that the boundaries strictly increase. */
function checkedBoundaries(boundaries: FokosCompositeKey[], start: FokosCompositeKey | null): FokosCompositeKey[] {
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

function laterKey(a: FokosCompositeKey | null, b: FokosCompositeKey | null): FokosCompositeKey | null {
	return a === null ? b : b === null || KeyCodec.compareComposite(a, b) >= 0 ? a : b;
}

function earlierKey(a: FokosCompositeKey | null, b: FokosCompositeKey | null): FokosCompositeKey | null {
	return a === null ? b : b === null || KeyCodec.compareComposite(a, b) <= 0 ? a : b;
}
