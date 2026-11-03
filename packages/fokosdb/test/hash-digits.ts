import { hash64, hashPrefixBucketDigits, hashPrefixBucketOf } from "../src/sharding/hash-primitives.js";
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

const U64 = (1n << 64n) - 1n;
// The odd primes of xxHash64. Odd is what makes a multiply invertible modulo 2^64.
const XXH_P1 = 0x9e3779b185ebca87n;
const XXH_P2 = 0xc2b2ae3d27d4eb4fn;
const XXH_P4 = 0x85ebca77c2b2ae63n;
const XXH_P5 = 0x27d4eb2f165667c5n;
const rotl64 = (x: bigint, r: bigint) => ((x << r) | (x >> (64n - r))) & U64;
const mul64 = (a: bigint, b: bigint) => (a * b) & U64;

/**
 * The inverse of an odd number modulo 2^64, by Newton steps. Each step `x * (2 - a*x)` doubles the
 * bits that are correct, so six steps give all 64 bits and seven is enough.
 */
function inverse64(a: bigint): bigint {
	let x = 1n;
	for (let i = 0; i < 7; i++) {
		x = mul64(x, (2n - mul64(a, x)) & U64);
	}
	return x;
}

/** One 8-byte lane of xxHash64, in its little-endian order (not the big-endian of a hashPrefixBucket). */
const lane64 = (b: Uint8Array, at: number) => new DataView(b.buffer, b.byteOffset + at, 8).getBigUint64(0, true);

/** The keyed step `rotl64(lane * P2, 31) * P1`. A rotate and an odd multiply are both invertible. */
const xxhRound = (lane: bigint) => mul64(rotl64(mul64(lane, XXH_P2), 31n), XXH_P1);

/** The inverse of `xxhRound`: `rotl64(k * inv(P1), 64 - 31) * inv(P2)`. */
const xxhUnround = (k: bigint) => mul64(rotl64(mul64(k, inverse64(XXH_P1)), 33n), inverse64(XXH_P2));

/**
 * The state after an 8-byte lane of an input shorter than 32 bytes: `rotl64(state ^ round(lane), 27) * P1 + P4`.
 * It is `F(state ^ round(lane))` with `F` invertible, so `state ^ round(lane)` is the only part the
 * lane chooses.
 */
const xxhLane = (state: bigint, lane: bigint) => (mul64(rotl64(state ^ xxhRound(lane), 27n), XXH_P1) + XXH_P4) & U64;

/**
 * Two binary hash keys of 16 bytes whose encoded keys have one hashPrefixBucket in `shardGroup`.
 *
 * An encoded binary key is the tag 0xFF and the raw bytes: 17 bytes. xxHash64 reads an input of 17
 * bytes as two 8-byte lanes and one last byte, and each step is invertible. The two keys differ in
 * their first lane, and the second lane of the second key brings the state after both lanes back to
 * the state of the first key. The last byte and the end steps are then the same.
 */
export function collidingHashKeys(shardGroup: string): [Uint8Array, Uint8Array] {
	const seed = hash64(shardGroup);
	const a = new Uint8Array(17);
	a[0] = 0xff;
	crypto.getRandomValues(a.subarray(1));
	const b = a.slice();
	b[1] ^= 1;
	// The xxHash64 state before the lanes of an input shorter than 32 bytes: `seed + P5 + length`.
	const start = (seed + XXH_P5 + 17n) & U64;
	// Let stateA and stateB be the states of the two keys after lane 0 (they differ in `b[1]`). A lane
	// step is `F(state ^ round(lane))` with `F` invertible, so the states after lane 1 are equal when
	//   stateA ^ round(laneA1) = stateB ^ round(laneB1)
	// This line is that equation solved for `round(laneB1)`, and `xxhUnround` gives its bytes.
	const second = xxhLane(start, lane64(a, 0)) ^ xxhRound(lane64(a, 8)) ^ xxhLane(start, lane64(b, 0));
	new DataView(b.buffer, 8, 8).setBigUint64(0, xxhUnround(second), true);
	// The callers give the raw keys to KeyCodec, which adds the tag again. Strip it here.
	return [a.subarray(1), b.subarray(1)];
}
