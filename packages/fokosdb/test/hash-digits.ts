import { hashPrefixBucketDigits, hashPrefixBucketOf } from "../src/sharding/hash-primitives.js";
import type { KeyBytes } from "../src/sharding/key-codec.js";
import type { FokosTopology } from "../src/sharding/route-context.js";

export type DigitTopology = Pick<FokosTopology, "shardGroup" | "rootTreesN" | "hashSplitN">;

/** The digits of a hash key in a topology: the root index, then the child index of each level, `depth` levels. */
export function hashDigits(topology: DigitTopology, hashKey: KeyBytes, depth: number): { root: number; path: number[] } {
	return hashPrefixBucketDigits(hashPrefixBucketOf(hashKey, topology.shardGroup), topology.rootTreesN, topology.hashSplitN, depth);
}

/** The child index of a hash key below a hash partition at `parentDepth`. */
export function childDigit(topology: DigitTopology, hashKey: KeyBytes, parentDepth: number): number {
	return hashDigits(topology, hashKey, parentDepth + 1).path[parentDepth];
}

/** The root index of a hash key. */
export function rootDigit(topology: DigitTopology, hashKey: KeyBytes): number {
	return hashDigits(topology, hashKey, 0).root;
}
