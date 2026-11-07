import { runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { FokosDBPartitionDO } from "../../server/do-partition.js";
import { testPartitionStub } from "../../../test/stub-helpers.js";
import { collidingHashKeys } from "../../../test/hash-digits.js";
import { routingHashFor } from "../../../test/routing-hash.js";
import { PartitionStore } from "./partition-store.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import type { FokosPartitionIdentity } from "../../sharding/route-context.js";
import type { FokosDBPolicy } from "../partition-context.js";
import fc from "fast-check";
import type { FokosCompositeKey } from "../../sharding/key-codec.js";
import { itemFacts, NO_FACTS, type ItemFacts, type StorageModel } from "./item-size.js";
import { addFacts, type Bucket, type ItemStats } from "./item-stats.js";
import {
	planBucketBoundaries,
	planRangeBoundaries,
	promotionFloor,
	SplitPlanner,
	type ItemSizeFacts,
	type SliceRead,
} from "./split-planner.js";

const kb = (s: string) => KeyCodec.encode(s);

/** A model whose item estimate is the `item_bytes` of the item, so a test gives each item its weight. */
const WEIGHT_MODEL: StorageModel = {
	keyIndexFactor: 0,
	baseOverhead: 0,
	indexedKeyFactor: 0,
	indexedOverhead: 0,
	overflowCoefficient: 0,
	rowPayloadOverhead: 0,
	overflowRowOverhead: 0,
};
const items = (...weights: number[]): ItemSizeFacts[] =>
	weights.map((itemBytes, i) => ({ hk: kb("h"), sk: kb(`k${String(i).padStart(3, "0")}`), itemBytes, indexed: false }));
const sum = (weights: number[]) => weights.reduce((a, b) => a + b, 0);
const plan = (weights: number[], N: number, total = sum(weights)) => planRangeBoundaries(WEIGHT_MODEL, total, N, null, items(...weights));

/** The weights of each child of `boundaries`. */
function children(weights: number[], boundaries: FokosCompositeKey[]): number[][] {
	const all = items(...weights);
	const out: number[][] = Array.from({ length: boundaries.length + 1 }, () => []);
	for (const [i, item] of all.entries()) {
		const child = boundaries.filter((b) => KeyCodec.compareComposite(b, item) <= 0).length;
		out[child].push(weights[i]);
	}
	return out;
}

/**
 * The reference rule over the whole list: for each target, the gap nearest the target after the
 * previous boundary, the earlier gap on a tie; then the repair from the last N items. It gives the
 * gap of each boundary: gap `j` is after item `j`, from 1.
 */
function referenceGaps(weights: number[], N: number, total: number): number[] {
	const prefix: number[] = [];
	let acc = 0;
	for (let j = 1; j < weights.length; j++) {
		acc += weights[j - 1];
		prefix[j] = acc;
	}
	const gaps: number[] = [];
	for (let i = 1; i < N; i++) {
		const target = (i * total) / N;
		let best: number | null = null;
		for (let j = (gaps.at(-1) ?? 0) + 1; j < weights.length; j++) {
			if (best === null || Math.abs(prefix[j] - target) < Math.abs(prefix[best] - target)) {
				best = j;
			}
		}
		if (best === null) {
			break;
		}
		gaps.push(best);
	}
	if (gaps.length < N - 1) {
		const C = weights.length;
		const kept = gaps.filter((g) => g < C - N + 1);
		const missing = N - 1 - kept.length;
		return [...kept, ...Array.from({ length: missing }, (_, i) => C - missing + i)];
	}
	return gaps;
}

describe("planRangeBoundaries", () => {
	it("returns null when the items cannot make N children", () => {
		expect(plan([], 2)).toBeNull();
		expect(plan([5, 5], 3)).toBeNull();
	});

	it("gives each child the items before its cut: four equal weights and N = 2 give two items for each child", () => {
		const boundaries = plan([111, 111, 111, 111], 2)!;
		expect(children([111, 111, 111, 111], boundaries)).toEqual([
			[111, 111],
			[111, 111],
		]);
	});

	it.each([
		{ name: "start", weights: [5111, 111, 111], expected: [[5111], [111], [111]] },
		{ name: "middle", weights: [111, 5111, 111], expected: [[111], [5111], [111]] },
		{ name: "end, with the repair", weights: [111, 111, 5111], expected: [[111], [111], [5111]] },
	])("gives three non-empty children with a dominant item at the $name", ({ weights, expected }) => {
		expect(children(weights, plan(weights, 3)!)).toEqual(expected);
	});

	it("splits exactly N items into one item for each child", () => {
		expect(children([1, 2, 3, 4], plan([1, 2, 3, 4], 4)!)).toEqual([[1], [2], [3], [4]]);
	});

	it("stops reading the items at the last boundary", () => {
		let read = 0;
		function* source() {
			for (const item of items(10, 10, 10, 10, 10, 10, 10, 10)) {
				read++;
				yield item;
			}
		}
		// The cut after the fourth item is known when the fifth item is read.
		expect(planRangeBoundaries(WEIGHT_MODEL, 80, 2, null, source())).toEqual([{ hk: kb("h"), sk: kb("k004") }]);
		expect(read).toBe(5);
	});

	it("gives valid boundaries for a wrong total, with a worse balance", () => {
		for (const total of [0, 1, 1e12]) {
			const boundaries = plan([10, 10, 10, 10, 10], 3, total)!;
			expect(boundaries).toHaveLength(2);
			expect(children([10, 10, 10, 10, 10], boundaries).every((c) => c.length > 0)).toBe(true);
		}
	});

	it("prices each item with the model: the key copies, the TTL entry, and the overflow pages", () => {
		const model: StorageModel = { ...WEIGHT_MODEL, keyIndexFactor: 1, baseOverhead: 10, indexedOverhead: 100 };
		// Weights 10 + 4 + 10 = 24, 24 + 100 = 124 for the indexed item, and 24, 24.
		const four: ItemSizeFacts[] = [
			{ hk: kb("h"), sk: kb("a"), itemBytes: 10, indexed: true },
			{ hk: kb("h"), sk: kb("b"), itemBytes: 10, indexed: false },
			{ hk: kb("h"), sk: kb("c"), itemBytes: 10, indexed: false },
			{ hk: kb("h"), sk: kb("d"), itemBytes: 10, indexed: false },
		];
		expect(planRangeBoundaries(model, 196, 2, null, four)).toEqual([{ hk: kb("h"), sk: kb("b") }]);
	});

	it("follows the reference rule, gives N - 1 increasing boundaries, and leaves no child empty", () => {
		fc.assert(
			fc.property(
				fc.array(fc.integer({ min: 1, max: 6000 }), { minLength: 2, maxLength: 40 }),
				fc.integer({ min: 2, max: 8 }),
				(weights, N) => {
					fc.pre(weights.length >= N);
					const boundaries = plan(weights, N)!;
					expect(boundaries).toHaveLength(N - 1);
					const split = children(weights, boundaries);
					expect(split.every((c) => c.length > 0)).toBe(true);
					const gaps = referenceGaps(weights, N, sum(weights));
					expect(split.slice(0, -1).map((c, i) => split.slice(0, i).flat().length + c.length)).toEqual(gaps);
				},
			),
		);
	});
});

/** The bound of the first bucket: the smallest composite key. */
const FIRST: FokosCompositeKey = { hk: KeyCodec.asKeyBytes(new Uint8Array()), sk: KeyCodec.asKeyBytes(new Uint8Array()) };

/**
 * Buckets over `list` with `sizes` items each, in order: the first bucket starts at the smallest key,
 * and each other bucket at its first item. `facts` gives the facts of each bucket, exact by default.
 */
function bucketsOf(list: ItemSizeFacts[], sizes: number[], facts: (exact: ItemFacts, b: number) => ItemFacts = (exact) => exact): Bucket[] {
	const out: Bucket[] = [];
	let at = 0;
	for (const [b, size] of sizes.entries()) {
		const exact = list
			.slice(at, at + size)
			.map((item) =>
				itemFacts(WEIGHT_MODEL, { itemBytes: item.itemBytes, keyBytes: item.hk.byteLength + item.sk.byteLength, indexed: item.indexed }),
			)
			.reduce(addFacts, { ...NO_FACTS });
		out.push({ bound: b === 0 ? FIRST : { hk: list[at].hk, sk: list[at].sk }, facts: facts(exact, b) });
		at += size;
	}
	return out;
}

/** A read of `list` in `[from, to)` that counts the rows it gives. */
function reader(list: ItemSizeFacts[]): { read: SliceRead; rows: () => number } {
	let rows = 0;
	const read: SliceRead = function* (from, to, order) {
		const inside = list.filter(
			(item) => (from === null || KeyCodec.compareComposite(item, from) >= 0) && (to === null || KeyCodec.compareComposite(item, to) < 0),
		);
		for (const item of order === "asc" ? inside : inside.reverse()) {
			rows++;
			yield item;
		}
	};
	return { read, rows: () => rows };
}

/** Random bucket sizes from 1 to `max` items that cover `count` items. */
const arbSizes = (count: number, max: number) =>
	fc.array(fc.integer({ min: 1, max }), { minLength: count, maxLength: count }).map((raw) => {
		const sizes: number[] = [];
		let left = count;
		for (const size of raw) {
			if (left === 0) {
				break;
			}
			sizes.push(Math.min(size, left));
			left -= sizes.at(-1)!;
		}
		return sizes;
	});

describe("planBucketBoundaries", () => {
	const bucketPlan = (weights: number[], N: number, sizes: number[]) => {
		const list = items(...weights);
		return planBucketBoundaries(WEIGHT_MODEL, N, null, bucketsOf(list, sizes), reader(list).read);
	};

	it("gives the boundaries of the streaming path for the cases of a dominant item, equal weights, and exactly N items", () => {
		const cases: Array<[number[], number]> = [
			[[111, 111, 111, 111], 2],
			[[5111, 111, 111], 3],
			[[111, 5111, 111], 3],
			[[111, 111, 5111], 3],
			[[1, 2, 3, 4], 4],
		];
		for (const [weights, N] of cases) {
			for (const sizes of [[weights.length], weights.map(() => 1), [1, weights.length - 1]]) {
				expect(bucketPlan(weights, N, sizes), `${weights.join(",")} N=${N} sizes=${sizes.join(",")}`).toEqual(plan(weights, N));
			}
		}
		expect(children([111, 111, 5111], bucketPlan([111, 111, 5111], 3, [2, 1]))).toEqual([[111], [111], [5111]]);
	});

	it("gives the boundaries of the streaming path for each bucket layout, and reads at most the bucket bound of rows", () => {
		fc.assert(
			fc.property(
				fc.array(fc.oneof(fc.integer({ min: 1, max: 200 }), fc.integer({ min: 1000, max: 20_000 })), { minLength: 2, maxLength: 60 }),
				fc.integer({ min: 2, max: 8 }),
				fc.integer({ min: 1, max: 8 }),
				fc.boolean(),
				(weights, N, maxBucket, emptyFirst) => {
					fc.pre(weights.length >= N);
					const list = items(...weights);
					return fc.assert(
						fc.property(arbSizes(weights.length, maxBucket), (sizes) => {
							// An empty first bucket can stay in front of the others.
							const buckets = bucketsOf(list, sizes);
							const layout = emptyFirst
								? [
										{ bound: FIRST, facts: { ...NO_FACTS } },
										{ ...buckets[0], bound: { hk: list[0].hk, sk: list[0].sk } },
										...buckets.slice(1),
									]
								: buckets;
							const { read, rows } = reader(list);
							expect(planBucketBoundaries(WEIGHT_MODEL, N, null, layout, read)).toEqual(plan(weights, N));
							// Each target scans one bucket of at most `maxBucket` items, and reads at most 3 more
							// rows: the item before the bucket, the item after it, and the item after a boundary
							// that the target cannot use. The repair reads the last N items.
							expect(rows()).toBeLessThanOrEqual(Math.min(N - 1, layout.length) * maxBucket + 3 * (N - 1) + N);
						}),
						{ numRuns: 5 },
					);
				},
			),
			{ numRuns: 200 },
		);
	});

	it("gives N - 1 valid boundaries for wrong bucket facts, with a worse balance", () => {
		fc.assert(
			fc.property(
				fc.array(fc.integer({ min: 1, max: 5000 }), { minLength: 2, maxLength: 40 }),
				fc.integer({ min: 2, max: 6 }),
				fc.array(fc.integer({ min: 0, max: 100_000 }), { minLength: 40, maxLength: 40 }),
				(weights, N, wrong) => {
					fc.pre(weights.length >= N);
					const list = items(...weights);
					const sizes = weights.map(() => 1);
					const buckets = bucketsOf(list, sizes, (exact, b) => ({ ...exact, itemBytes: wrong[b], itemCount: wrong[b] % 7 }));
					const boundaries = planBucketBoundaries(WEIGHT_MODEL, N, null, buckets, reader(list).read);
					expect(boundaries).toHaveLength(N - 1);
					expect(children(weights, boundaries).every((c) => c.length > 0)).toBe(true);
				},
			),
		);
	});

	it("keeps each boundary inside the slice and above its lower bound", () => {
		const list = items(10, 10, 10, 10, 10, 10);
		const inSlice = list.slice(2);
		const start = { hk: list[2].hk, sk: list[2].sk };
		// The first bucket starts below the slice, as on a range child.
		const buckets = bucketsOf(inSlice, [2, 2]);
		const boundaries = planBucketBoundaries(WEIGHT_MODEL, 2, start, buckets, reader(inSlice).read);
		expect(boundaries).toEqual(planRangeBoundaries(WEIGHT_MODEL, 40, 2, start, inSlice));
	});
});

describe("planRangeBoundaries over two hash keys of one routingHash", () => {
	it("puts a boundary between the hash keys, with the empty sort key", () => {
		const two: ItemSizeFacts[] = [
			{ hk: kb("alpha"), sk: kb("1"), itemBytes: 20, indexed: false },
			{ hk: kb("beta"), sk: kb("1"), itemBytes: 10, indexed: false },
			{ hk: kb("beta"), sk: kb("2"), itemBytes: 10, indexed: false },
		];
		expect(planRangeBoundaries(WEIGHT_MODEL, 40, 2, null, two)).toEqual([{ hk: kb("b"), sk: KeyCodec.encodeOptional(undefined) }]);
	});

	it("separates the sort keys inside one hash key", () => {
		const one: ItemSizeFacts[] = [
			{ hk: kb("alpha"), sk: kb("aa"), itemBytes: 10, indexed: false },
			{ hk: kb("alpha"), sk: kb("ab"), itemBytes: 10, indexed: false },
		];
		expect(planRangeBoundaries(WEIGHT_MODEL, 20, 2, null, one)).toEqual([{ hk: kb("alpha"), sk: kb("ab") }]);
	});
});

describe("SplitPlanner at the hash depth limit", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	const MB = 1024 * 1024;
	const policy = { hashSplitConditions: { maxSizeMb: 1 }, rangeSplitConditions: { maxSizeMb: 1 } } as FokosDBPolicy;
	// rootTreesN 65,000 and hashSplitN 255 have the depth limit 6.
	const leafAt = (depth: number) =>
		({
			schema: 1,
			ref: { partitionId: "00", doName: "leaf" },
			kind: "hash",
			hash: { rootIndex: 0, path: Array.from({ length: depth }, () => 0), depth, start: new Uint8Array(8), end: null },
			topology: { shardGroup: "fokos.p.limit", rootTreesN: 65_000, hashSplitN: 255 },
			routingHashSeed: "",
		}) as FokosPartitionIdentity;
	/**
	 * A planner over the limit. `keys` holds one hash key for each routingHash, and `large` the
	 * representatives of the routingHashes whose total is above the threshold.
	 */
	const planner = (keys: KeyBytes[], large: KeyBytes[]) =>
		new SplitPlanner({
			itemStats: { largestAtLeast: () => large } as unknown as ItemStats,
			source: {
				probeRoutingHashes: () => (keys.length === 0 ? null : { first: keys[0], more: keys.length > 1 }),
				compositeKeyFacts: () => [],
				countItems: () => 0,
			},
			databaseSize: () => 2 * MB,
			promotionFraction: () => 0.5,
			rangeSplitN: () => 4,
			logName: "test",
			logParams: () => ({}),
		});

	it("returns the floor max_depth for a leaf at the limit with two hash keys and no large one", () => {
		const error = vi.spyOn(console, "error").mockImplementation(() => {});
		expect(planner([kb("a"), kb("b")], []).evaluateSplit({ identity: leafAt(6), policy })).toBe(false);
		expect(error).toHaveBeenCalledWith(expect.objectContaining({ reason: "max_depth" }));
	});

	// Each hash key of a routingHash can be below the threshold while the total is above it.
	it("promotes the large routingHashes of a leaf at the limit, and the one routingHash at any estimate", () => {
		expect(planner([kb("a"), kb("b")], [kb("b")]).evaluateSplit({ identity: leafAt(6), policy })).toEqual({ promote: [kb("b")] });
		expect(planner([kb("a")], []).evaluateSplit({ identity: leafAt(6), policy })).toEqual({ promote: [kb("a")] });
	});

	it("splits a leaf above the limit", () => {
		expect(planner([kb("a"), kb("b")], []).evaluateSplit({ identity: leafAt(5), policy })).toEqual({});
	});

	// A leaf at the limit cannot split, so each bucket must store its estimate for any later cap.
	it("gives the promotion floor 0 at the limit, T times the index fraction above it, and null with no cap", () => {
		const fractions = { promotionFraction: 0.5, promotionIndexFraction: 0.5 };
		expect(promotionFloor(leafAt(6), policy, fractions)).toBe(0);
		expect(promotionFloor(leafAt(5), policy, fractions)).toBe(0.25 * MB);
		expect(promotionFloor(leafAt(6), { ...policy, hashSplitConditions: {} }, fractions)).toBeNull();
	});
});

describe("SplitPlanner over a PartitionStore at the hash depth limit", () => {
	// Each hash key is below the promotion threshold, but the total of each routingHash is above it.
	it("promotes two routingHashes of small hash keys, and the write path reports the same totals", async () => {
		const group = "fokos.p.limit";
		const MB = 1024 * 1024;
		const policy = { hashSplitConditions: { maxSizeMb: 1 }, rangeSplitConditions: { maxSizeMb: 1 } } as FokosDBPolicy;
		const identity = {
			schema: 1,
			ref: { partitionId: "00", doName: "leaf" },
			kind: "hash",
			hash: { rootIndex: 0, path: [0, 0, 0, 0, 0, 0], depth: 6, start: new Uint8Array(8), end: null },
			topology: { shardGroup: group, rootTreesN: 65_000, hashSplitN: 255 },
			routingHashSeed: "",
		} as FokosPartitionIdentity;
		const threshold = MB * 0.5;
		const collidingPairs = [collidingHashKeys(group), collidingHashKeys(group)].map((pair) => pair.map((raw) => KeyCodec.encode(raw)));

		const stub = testPartitionStub(`split-planner.${crypto.randomUUID()}`);
		await runInDurableObject(stub, (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
			const store = new PartitionStore(state.storage, { promotionFloor: () => threshold / 2 });
			const planner = new SplitPlanner({
				itemStats: store.itemStats,
				source: {
					probeRoutingHashes: () => store.probeRoutingHashes("items"),
					compositeKeyFacts: (routingHash, start, end) => store.compositeKeyFacts(routingHash, start, end),
					countItems: (routingHash, start, end, limit) => store.countItems(routingHash, start, end, limit),
				},
				databaseSize: () => 2 * MB,
				promotionFraction: () => 0.5,
				rangeSplitN: () => 4,
				logName: "test",
				logParams: () => ({}),
			});
			const data = "x".repeat(300 * 1024);
			for (const [first, second] of collidingPairs) {
				const routingHash = routingHashFor(first, group);
				const put = (hk: KeyBytes) => store.upsertItem({ routingHash, hk, sk: kb("s"), data, kind: "text", ttlAt: null, txOrderTs: 1 });
				const one = put(first).keyEstBytes;
				const both = put(second).keyEstBytes;
				expect(one).toBeLessThan(threshold);
				expect(both).toBeGreaterThanOrEqual(threshold);
				// The write path signals a promotion for the total of the routingHash, not for the hash key.
				expect(planner.promotionCandidates(identity, policy, [{ hashKey: first, keyEstBytes: one }])).toEqual([]);
				expect(planner.promotionCandidates(identity, policy, [{ hashKey: second, keyEstBytes: both }])).toEqual([{ hashKey: second }]);
			}
			const answer = planner.evaluateSplit({ identity, policy });
			// Each routingHash is promoted by the hash key of its first row in key order.
			const firstInKeyOrder = collidingPairs.map(([x, y]) => (KeyCodec.compare(x, y) < 0 ? x : y));
			expect(answer && "promote" in answer ? answer.promote : answer).toEqual(expect.arrayContaining(firstInKeyOrder));
			expect(answer && "promote" in answer ? answer.promote.length : 0).toBe(2);
		});
	});
});
