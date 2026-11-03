import { runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { FokosDBPartitionDO } from "../../server/do-partition.js";
import { testPartitionStub } from "../../../test/stub-helpers.js";
import { collidingHashKeys } from "../../../test/hash-digits.js";
import { prefixOf } from "../../../test/storage-prefix.js";
import { PartitionStore } from "./partition-store.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import type { FokosPartitionIdentity } from "../../sharding/route-context.js";
import type { FokosDBPolicy } from "../partition-context.js";
import type { KeySizeStats } from "./key-size-stats.js";
import { planRangeBoundaries, SplitPlanner } from "./split-planner.js";

const kb = (s: string) => KeyCodec.encode(s);
const rows = (...entries: Array<[string, number]>) => entries.map(([sk, bytes]) => ({ hk: kb("h"), sk: kb(sk), bytes }));

describe("planRangeBoundaries", () => {
	it("gives the floor reason when the rows cannot make N children", () => {
		expect(planRangeBoundaries(0, 2, null, [])).toEqual({ floor: "fewer_items" });
		expect(planRangeBoundaries(10, 3, null, rows(["a", 5], ["b", 5]))).toEqual({ floor: "fewer_items" });
		// The large row crosses the first threshold, and the next threshold is above the bytes that remain.
		expect(planRangeBoundaries(5002, 3, null, rows(["a", 1], ["m", 5000], ["z", 1]))).toEqual({ floor: "skewed_bytes" });
	});

	it("stops reading the rows at the last boundary", () => {
		let read = 0;
		function* source() {
			for (const sk of ["a", "b", "c", "d", "e", "f", "g", "h"]) {
				read++;
				yield { hk: kb("h"), sk: kb(sk), bytes: 10 };
			}
		}
		expect(planRangeBoundaries(80, 2, null, source())).toEqual({ boundaries: [{ hk: kb("h"), sk: kb("d") }] });
		expect(read).toBe(4);
	});
});

describe("planRangeBoundaries over two hash keys of one hashPrefixBucket", () => {
	it("puts a boundary between the hash keys, with the empty sort key", () => {
		const two = [
			{ hk: kb("alpha"), sk: kb("1"), bytes: 20 },
			{ hk: kb("beta"), sk: kb("1"), bytes: 10 },
			{ hk: kb("beta"), sk: kb("2"), bytes: 10 },
		];
		expect(planRangeBoundaries(40, 2, null, two)).toEqual({ boundaries: [{ hk: kb("b"), sk: KeyCodec.encodeOptional(undefined) }] });
	});

	it("separates the sort keys inside one hash key", () => {
		const one = [
			{ hk: kb("alpha"), sk: kb("aa"), bytes: 10 },
			{ hk: kb("alpha"), sk: kb("ab"), bytes: 10 },
		];
		expect(planRangeBoundaries(20, 2, null, one)).toEqual({ boundaries: [{ hk: kb("alpha"), sk: kb("ab") }] });
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
			hashPrefixBucketSeed: "",
		}) as FokosPartitionIdentity;
	/**
	 * A planner over the limit. `keys` holds one hash key for each hashPrefixBucket, and `large` the
	 * representatives of the hashPrefixBuckets whose total is above the threshold.
	 */
	const planner = (keys: KeyBytes[], large: KeyBytes[]) =>
		new SplitPlanner({
			keySizes: { largestAtLeast: () => large, bytesOf: () => 0 } as unknown as KeySizeStats,
			source: {
				probeHashPrefixBuckets: () => (keys.length === 0 ? null : { first: keys[0], more: keys.length > 1 }),
				compositeKeySizes: () => [],
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

	// Each hash key of a hashPrefixBucket can be below the threshold while the total is above it.
	it("promotes the large hashPrefixBuckets of a leaf at the limit, and the one hashPrefixBucket at any estimate", () => {
		expect(planner([kb("a"), kb("b")], [kb("b")]).evaluateSplit({ identity: leafAt(6), policy })).toEqual({ promote: [kb("b")] });
		expect(planner([kb("a")], []).evaluateSplit({ identity: leafAt(6), policy })).toEqual({ promote: [kb("a")] });
	});

	it("splits a leaf above the limit", () => {
		expect(planner([kb("a"), kb("b")], []).evaluateSplit({ identity: leafAt(5), policy })).toEqual({});
	});
});

describe("SplitPlanner over a PartitionStore at the hash depth limit", () => {
	// Each hash key is below the promotion threshold, but the total of each hashPrefixBucket is above it.
	it("promotes two hashPrefixBuckets of small hash keys, and the write path reports the same totals", async () => {
		const group = "fokos.p.limit";
		const MB = 1024 * 1024;
		const policy = { hashSplitConditions: { maxSizeMb: 1 }, rangeSplitConditions: { maxSizeMb: 1 } } as FokosDBPolicy;
		const identity = {
			schema: 1,
			ref: { partitionId: "00", doName: "leaf" },
			kind: "hash",
			hash: { rootIndex: 0, path: [0, 0, 0, 0, 0, 0], depth: 6, start: new Uint8Array(8), end: null },
			topology: { shardGroup: group, rootTreesN: 65_000, hashSplitN: 255 },
			hashPrefixBucketSeed: "",
		} as FokosPartitionIdentity;
		const threshold = MB * 0.5;
		const buckets = [collidingHashKeys(group), collidingHashKeys(group)].map((pair) => pair.map((raw) => KeyCodec.encode(raw)));

		const stub = testPartitionStub(`split-planner.${crypto.randomUUID()}`);
		await runInDurableObject(stub, (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
			const store = new PartitionStore(state.storage);
			const planner = new SplitPlanner({
				keySizes: store.keySizes,
				source: {
					probeHashPrefixBuckets: () => store.probeHashPrefixBuckets("items"),
					compositeKeySizes: (bucket, start, end) => store.compositeKeySizes(bucket, start, end),
				},
				databaseSize: () => 2 * MB,
				promotionFraction: () => 0.5,
				rangeSplitN: () => 4,
				logName: "test",
				logParams: () => ({}),
			});
			const data = "x".repeat(300 * 1024);
			for (const [first, second] of buckets) {
				const prefix = prefixOf(first, group);
				const put = (hk: KeyBytes) => store.upsertItem({ prefix, hk, sk: kb("s"), data, kind: "text", ttlAt: null, txOrderTs: 1 });
				const one = put(first).keyEstBytes;
				const both = put(second).keyEstBytes;
				expect(one).toBeLessThan(threshold);
				expect(both).toBeGreaterThanOrEqual(threshold);
				// The write path signals a promotion for the total of the hashPrefixBucket, not for the hash key.
				expect(planner.promotionCandidates(identity, policy, [{ hashKey: first, keyEstBytes: one }])).toEqual([]);
				expect(planner.promotionCandidates(identity, policy, [{ hashKey: second, keyEstBytes: both }])).toEqual([{ hashKey: second }]);
			}
			const answer = planner.evaluateSplit({ identity, policy });
			// Each hashPrefixBucket is promoted by the hash key of its first row.
			expect(answer && "promote" in answer ? answer.promote : answer).toEqual(expect.arrayContaining([buckets[0][0], buckets[1][0]]));
			expect(answer && "promote" in answer ? answer.promote.length : 0).toBe(2);
		});
	});
});
