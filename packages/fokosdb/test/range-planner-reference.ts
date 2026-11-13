import { KeyCodec, type FokosCompositeKey } from "../src/sharding/key-codec.js";
import invariant from "../src/shared/invariant.js";
import { estimate, itemFacts, type StorageModel } from "../src/shared/partition/item-size.js";
import type { ItemSizeFacts } from "../src/shared/partition/split-planner.js";

/**
 * The reference range planner of the tests. It computes the `N - 1` boundaries of a range split of
 * `items`, which are in `(hk, sk)` order from `start`, with the rules of `planBucketBoundaries`
 * (`src/shared/partition/split-planner.ts`). It returns null when `items` holds fewer than `N` items.
 *
 * It reads every item until the last boundary, so the tests can check `planBucketBoundaries` against
 * it: both give the same boundaries when the bucket facts are exact. Production does not use it.
 *
 * Here the items have the numbers 1, 2, 3, ..., and gap `j` is the gap between item `j` and item
 * `j + 1`. `estBelow(j)` is the estimate of items 1 to `j`. `total` is the estimate of the slice, and
 * target `i` is `i * total / N`. The function reads
 * each item one time: O(items) time, and O(N) memory for the keys of the last `N + 1` items.
 */
export function planRangeBoundaries(
	model: StorageModel,
	total: number,
	N: number,
	start: FokosCompositeKey | null,
	items: Iterable<ItemSizeFacts>,
): FokosCompositeKey[] | null {
	invariant(N >= 2, `reference planner: a range split needs at least 2 children, got ${N}`);
	const step = total / N;
	// The gap number of each boundary, in order.
	const gaps: number[] = [];
	const boundaries: FokosCompositeKey[] = [];
	// The keys of the last N + 1 items, in a ring: item `n` is at `ring[(n - 1) % size]`. The planner
	// needs the two items of the gap below the target, and the last N items for the repair.
	const size = N + 1;
	const ring: FokosCompositeKey[] = [];
	const keyOf = (n: number) => ring[(n - 1) % size];
	// The number of items read.
	let count = 0;
	// estBelow(count): the estimate of the items read.
	let estBelow = 0;
	// The last gap below the current target, when it is after the previous boundary.
	let below: { gap: number; estBelow: number } | null = null;

	const choose = (gap: number) => {
		gaps.push(gap);
		boundaries.push(KeyCodec.shortestCompositeSeparator(keyOf(gap), keyOf(gap + 1)));
		below = null;
	};

	for (const item of items) {
		count++;
		ring[(count - 1) % size] = { hk: item.hk, sk: item.sk };
		if (count > 1) {
			// The planner knows gap `count - 1` now, and `estBelow` is estBelow(count - 1).
			const gap = count - 1;
			while (boundaries.length < N - 1) {
				const target = step * (boundaries.length + 1);
				if (estBelow < target) {
					// The gap is below the target. A later gap can be nearer.
					below = { gap, estBelow };
					break;
				}
				// The gap is at or above the target. The nearer of the two gaps is the boundary.
				const candidate: { gap: number; estBelow: number } | null = below;
				if (candidate !== null && target - candidate.estBelow <= estBelow - target) {
					// The gap below the target is the boundary. The next target can use this gap.
					choose(candidate.gap);
					continue;
				}
				choose(gap);
				break;
			}
			if (boundaries.length === N - 1) {
				break;
			}
		}
		estBelow += estimate(
			model,
			itemFacts(model, { itemBytes: item.itemBytes, keyBytes: item.hk.byteLength + item.sk.byteLength, indexed: item.indexed }),
		);
	}
	if (count < N) {
		return null;
	}
	if (boundaries.length < N - 1) {
		// The items ended below the next target, so the last gap below it is the nearest gap.
		const last: { gap: number; estBelow: number } | null = below;
		if (last !== null) {
			choose(last.gap);
		}
	}
	if (boundaries.length < N - 1) {
		// The repair. The gaps between the last N items are `count - N + 1` to `count - 1`.
		const firstRecentGap = count - N + 1;
		let kept = 0;
		while (kept < gaps.length && gaps[kept] < firstRecentGap) {
			kept++;
		}
		const missing = N - 1 - kept;
		boundaries.length = kept;
		for (let gap = count - missing; gap < count; gap++) {
			boundaries.push(KeyCodec.shortestCompositeSeparator(keyOf(gap), keyOf(gap + 1)));
		}
	}
	for (let i = 0; i < boundaries.length; i++) {
		invariant(
			start === null || KeyCodec.compareComposite(boundaries[i], start) > 0,
			"reference planner: boundary is not above the lower bound",
		);
		invariant(i === 0 || KeyCodec.compareComposite(boundaries[i], boundaries[i - 1]) > 0, "reference planner: boundaries do not increase");
	}
	return boundaries;
}
