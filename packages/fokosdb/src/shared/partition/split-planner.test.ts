import { afterEach, describe, expect, it, vi } from "vitest";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import type { FokosPartitionIdentity } from "../../sharding/route-context.js";
import type { FokosDBPolicy } from "../partition-context.js";
import type { KeySizeStats } from "./key-size-stats.js";
import { planRangeBoundaries, SplitPlanner } from "./split-planner.js";

const kb = (s: string) => KeyCodec.encode(s);
const rows = (...entries: Array<[string, number]>) => entries.map(([sk, bytes]) => ({ sk: kb(sk), bytes }));

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
				yield { sk: kb(sk), bytes: 10 };
			}
		}
		expect(planRangeBoundaries(80, 2, null, source())).toEqual({ boundaries: [kb("d")] });
		expect(read).toBe(4);
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
	/** A planner over the limit, with the hash keys of `keys` and the large keys of `large`. */
	const planner = (keys: KeyBytes[], large: KeyBytes[]) =>
		new SplitPlanner({
			keySizes: { largestAtLeast: () => large, bytesOf: () => 0 } as unknown as KeySizeStats,
			source: { probeHashKeys: () => (keys.length === 0 ? null : { first: keys[0], more: keys.length > 1 }), sortKeySizes: () => [] },
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

	it("promotes the large hash keys of a leaf at the limit, and the one hash key at any estimate", () => {
		expect(planner([kb("a"), kb("b")], [kb("b")]).evaluateSplit({ identity: leafAt(6), policy })).toEqual({ promote: [kb("b")] });
		expect(planner([kb("a")], []).evaluateSplit({ identity: leafAt(6), policy })).toEqual({ promote: [kb("a")] });
	});

	it("splits a leaf above the limit", () => {
		expect(planner([kb("a"), kb("b")], []).evaluateSplit({ identity: leafAt(5), policy })).toEqual({});
	});
});
