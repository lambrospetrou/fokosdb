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
 * Why no repartition can make a partition smaller:
 *
 * - `no_hash_key`: a hash leaf has no hash key in its items and in its pending transactions.
 * - `max_depth`: a hash leaf at the depth limit of its topology has no large routingHash to promote,
 *   and it cannot do a hash split.
 * - `fewer_items`: a range leaf holds fewer items than the number of children of a range split
 *   (`rangeSplitN`). Each child must get at least one item.
 */
export type SplitFloor = "no_hash_key" | "fewer_items" | "max_depth";

/** A hash key that a write changed, with the estimate of its routingHash after the write. */
type PromotionCandidate = { hashKey: KeyBytes; keyEstBytes: number };

type EvaluateSplitAnswer = ReturnType<FokosShardingHooks<FokosDBPolicy>["evaluateSplit"]>;

export type SplitPlannerOptions = {
	/** The buckets of the partition. The planner reads the promotion lookup and the bucket facts from it. */
	itemStats: ItemStats;
	/** The items of the partition. */
	source: SplitSource;
	/** The size of the SQLite file of the partition, in bytes. The split decision compares it with the cap. */
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
 * The promotion floor of a partition. A bucket stores its estimate in `promotion_est_bytes`, and so gets
 * an entry in the promotion index, only when the estimate is at or above the promotion floor. The
 * promotion floor is one of these values, in this order:
 *
 * 1. On a range leaf: null. A range leaf has no promotion.
 * 2. On a hash leaf at the depth limit: 0, so each bucket has an index entry. This also applies when
 *    the policy has no size limit.
 * 3. On a hash leaf below the depth limit, when the policy has no size limit: null.
 * 4. On a hash leaf below the depth limit: `T` times `promotionIndexFraction`, where `T` is the
 *    promotion threshold.
 *
 * Why 0 at the depth limit: that leaf cannot split, so a promotion is its only way to get smaller.
 * When it is over its size limit, it refuses each write, so no bucket gets a new index entry. When the
 * lookup then misses a large routingHash, the leaf refuses writes until an operator deletes data
 * (`max_depth` in `SplitFloor`). Thus each routingHash must have an index entry before the leaf gets
 * too large. This is also true when the policy has no size limit now, because an operator can add one
 * later.
 *
 * Why null below the depth limit with no size limit: that leaf has no promotion now. When an operator
 * adds a size limit, the lookup can miss a large routingHash until its next write. The leaf then does
 * a hash split, so it does not stay too large.
 */
export function promotionFloor(
	identity: FokosPartitionIdentity,
	policy: FokosDBPolicy,
	fractions: { promotionFraction: number; promotionIndexFraction: number },
): number | null {
	if (identity.kind !== "hash") {
		return null;
	}
	invariant(identity.hash, "fokos/split-planner: hash identity must have a hash");
	const { rootTreesN, hashSplitN } = identity.topology;
	if (identity.hash.depth >= maxHashDepth(rootTreesN, hashSplitN)) {
		return 0;
	}
	const threshold = maxSplitBytes(identity, policy) * fractions.promotionFraction;
	return threshold > 0 ? threshold * fractions.promotionIndexFraction : null;
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

/** Reads the items of a slice in `[from, to)`, in key order or in reverse order. A null edge is the edge of the slice. */
export type SliceRead = (from: FokosCompositeKey | null, to: FokosCompositeKey | null, order: "asc" | "desc") => Iterable<ItemSizeFacts>;

/**
 * One gap: the item below it (`lower`), the item above it (`upper`), and `estBelow`, the estimate
 * below it. The planner has no item numbers, so the two items identify the gap. A null item is not
 * read yet, and the planner reads it only when it needs the gap.
 */
type Gap = { lower: ItemSizeFacts | null; upper: ItemSizeFacts | null; estBelow: number };

/**
 * Computes the `N - 1` boundaries of a range split of a slice, from the buckets of the slice. The
 * caller checks before that the slice holds at least `N` items.
 *
 * The words:
 *
 * - A gap is the position between two adjacent items of the slice, in `(hk, sk)` order. A boundary at
 *   a gap puts the items below the gap in the lower child, and the items above it in the upper child.
 *   No gap exists before the first item or after the last item, so each child gets at least one item.
 * - `estBelow` of a gap is the estimate of the items below the gap: the running total of the item
 *   estimates, from the start of the slice up to the gap. The lower child of a boundary at the gap
 *   gets this estimate.
 * - `B` is the total estimate of the buckets. Target `i` is `i * B / N`, for `i` from 1 to `N - 1`.
 *   Boundary `i` must put about target `i` below it.
 *
 * For each target in order, the planner:
 *
 * 1. Finds the bucket that holds the target, with a binary search over the bucket estimates.
 * 2. Reads the items of that bucket, one time for each bucket. The gaps of the bucket are the gap at
 *    its start, the gaps between its items, and the gap at its end (the start of the next bucket).
 * 3. Finds two gaps after the previous boundary, with a binary search: the last gap with
 *    `estBelow < target`, and the first gap with `estBelow >= target`.
 * 4. Selects the gap with the smaller distance to the target. When the distances are equal, it
 *    selects the lower gap. When both gaps are at or below the previous boundary, the boundary is the
 *    first gap after the previous boundary: one more item read.
 *
 * Example: a range split into `N = 3` children, of a slice with four buckets. The numbers are
 * estimates in bytes. `B` is 120, so the targets are 40 and 80.
 *
 *     bucket      b0          b1                        b2              b3
 *     estimate    30          40                        20              30
 *               |-------|-------------------------|--------------|--------------|
 *     estBelow  0       30          ^             70      ^      90            120
 *                                target 40             target 80
 *
 * Target 40. The binary search over the bucket estimates finds b1, because `30 < 40 <= 70`. The
 * planner reads the four items of b1 (i1 to i4, with the estimates 12, 8, 15 and 5). The gaps of b1
 * and their `estBelow` are:
 *
 *                  i1     i2     i3     i4
 *               |------|------|------|------|
 *     estBelow  30     42     50     65     70
 *                  ^
 *              target 40
 *
 * The gap below the target is the start gap of b1 (30, distance 10). The gap above is the gap
 * between i1 and i2 (42, distance 2). The start gap has no lower item yet, so the planner reads one
 * item: the last item of b0. The gap above is nearer, so boundary 1 is the gap between i1 and i2.
 *
 * Target 80. The search finds b2, because `70 < 80 <= 90`. The planner reads the three items of b2
 * (j1 to j3, with the estimates 6, 9 and 5):
 *
 *                  j1     j2     j3
 *               |------|------|------|
 *     estBelow  70     76     85     90
 *                         ^
 *                     target 80
 *
 * The gap below is the gap between j1 and j2 (76, distance 4). The gap above is the gap between j2
 * and j3 (85, distance 5). The gap below is nearer, so boundary 2 is the gap between j1 and j2.
 *
 * The children get the estimates 42, 34 and 44. The planner read the items of b1 and b2, and one
 * item of b0. It read no item of b3. Each boundary is the shortest separator of the two items at its
 * gap, for example a key between i1 and i2.
 *
 * When the planner ends with fewer boundaries, a large item near the end took the remaining targets.
 * The repair reads the last `N` items, keeps the boundaries below them, and puts the missing boundaries
 * at the gaps between them.
 *
 * Cost: it reads the items of at most `min(N - 1, buckets)` buckets (each at most `2K` items), and
 * about `2N` single items. The work in JavaScript is linear in the items read, plus O(log) for each
 * target.
 *
 * Wrong bucket facts give a worse balance, but the boundaries stay valid, because the planner
 * identifies a gap by its items. An item cannot cross a boundary, so a dominant item makes equal
 * children impossible, and a child that is still over its cap splits again. Each boundary is the
 * shortest separator of the two items at its gap, so the DO names and the topology stay small.
 *
 * `test/range-planner-reference.ts` holds a reference planner that reads every item of the slice. The
 * tests check that this planner gives the same boundaries when the facts are exact.
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

	// The estimate below the start and below the end of each bucket. Both arrays never decrease.
	const estBelowStart: number[] = [];
	const estBelowEnd: number[] = [];
	let total = 0;
	for (const bucket of buckets) {
		estBelowStart.push(total);
		total += estimate(model, bucket.facts);
		estBelowEnd.push(total);
	}
	// The bucket that holds `target`: `estBelowStart < target <= estBelowEnd`, or -1. The first bucket
	// with `estBelowEnd >= target` is the only possible one, because each bucket before it ends below
	// the target.
	const bucketOf = (target: number) => {
		const b = firstIndex(estBelowEnd.length, (i) => estBelowEnd[i] >= target);
		return b < estBelowEnd.length && estBelowStart[b] < target ? b : -1;
	};
	const step = total / N;
	// The gaps of each bucket that the planner read. It reads each bucket at most one time. In the gaps
	// of a bucket, `estBelow` increases, except at the end gap: its `estBelow` comes from the bucket
	// facts, and wrong facts can make it smaller.
	const scanned = new Map<number, Gap[]>();
	const gapsOf = (b: number): Gap[] => {
		let gaps = scanned.get(b);
		if (gaps === undefined) {
			const items = [...read(buckets[b].bound, buckets[b + 1]?.bound ?? null, "asc")];
			gaps = [];
			if (items.length > 0) {
				let estBelow = estBelowStart[b];
				gaps.push({ lower: null, upper: items[0], estBelow });
				for (let i = 0; i + 1 < items.length; i++) {
					estBelow += weight(items[i]);
					gaps.push({ lower: items[i], upper: items[i + 1], estBelow });
				}
				const end: Gap & { next: FokosCompositeKey | null } = {
					lower: items[items.length - 1],
					upper: null,
					estBelow: estBelowStart[b] + estimate(model, buckets[b].facts),
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
		const b = bucketOf(target);
		if (b === -1) {
			break;
		}
		const gaps = gapsOf(b);
		// The gap below the target: the last gap with `estBelow < target`. The gap above: the first gap
		// with `estBelow >= target`. A binary search covers the gaps before the end gap, which are in
		// order, and the end gap gets its own check.
		const end = gaps.length - 1;
		const firstAtOrAbove = firstIndex(end, (i) => gaps[i].estBelow >= target);
		let nearBelow: Gap | undefined =
			end >= 0 && gaps[end].estBelow < target ? gaps[end] : firstAtOrAbove > 0 ? gaps[firstAtOrAbove - 1] : undefined;
		let nearAbove: Gap | undefined =
			firstAtOrAbove < end ? gaps[firstAtOrAbove] : end >= 0 && gaps[end].estBelow >= target ? gaps[end] : undefined;
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
			pick = upper === null ? undefined : { lower: previous.upper, upper, estBelow: Number.NaN };
		} else if (nearAbove !== undefined) {
			pick = nearBelow !== undefined && target - nearBelow.estBelow <= nearAbove.estBelow - target ? nearBelow : nearAbove;
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
			last.push(item);
			if (last.length === N) {
				break;
			}
		}
		last.reverse();
		invariant(last.length === N, `fokos/split-planner: the slice holds fewer than ${N} items`);
		const kept = chosen.filter((gap) => KeyCodec.compareComposite(gap.lower!, last[0]) < 0);
		const missing = N - 1 - kept.length;
		for (let i = N - 1 - missing; i < N - 1; i++) {
			kept.push({ lower: last[i], upper: last[i + 1], estBelow: Number.NaN });
		}
		chosen = kept;
	}
	return checkedBoundaries(
		chosen.map((gap) => KeyCodec.shortestCompositeSeparator(gap.lower!, gap.upper!)),
		start,
	);
}

/** The first index in `[0, length)` where `test` is true, or `length`. `test` must be false and then true. */
function firstIndex(length: number, test: (i: number) => boolean): number {
	let low = 0;
	let high = length;
	while (low < high) {
		const middle = (low + high) >>> 1;
		if (test(middle)) {
			high = middle;
		} else {
			low = middle + 1;
		}
	}
	return low;
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
