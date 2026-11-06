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
import type { StorageModel } from "./item-size.js";
import type { ItemStats } from "./item-stats.js";
import { planRangeBoundaries, promotionFloor, SplitPlanner, type ItemSizeFacts } from "./split-planner.js";

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
