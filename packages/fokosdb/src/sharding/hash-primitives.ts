// Doesn't have a xxhash64 unfortunately in plain JS implementation.
// import { xxHash32 } from "js-xxhash";
import xxhash from "xxhash-wasm";
// Creates the WebAssembly instance.
const hashers = await xxhash();

import type { KeyBytes } from "./key-codec.js";

// Golden Ratio constant for better hash scattering.
// See https://softwareengineering.stackexchange.com/a/402543
export const GOLDEN_RATIO = 0x9e3779b1;
// The default base seed of `hashPrefixBucketSeedOf`, so a change moves every stored hashPrefixBucket. It must never change.
export const GOLDEN_RATIO_BIGINT = BigInt(0x9e3779b1);

// Odd 32-bit constant used to derive a distinct hash seed per tree depth. Oddness makes
// `d ↦ d·PRIME mod 2^32` a bijection, so no two depths share a seed (no per-depth collisions).
const SEED_DEPTH_PRIME = 0x85ebca77;

/**
 * Per-depth hash seed (see docs/ideas/key-ordering-utf8-convergence.md). Every tree level hashes the
 * SAME key bytes and varies the xxHash seed by depth, so each level is an independent hash stream.
 * This decorrelates the levels and allocates nothing per level (no input concat, no temp buffer).
 *
 * `seedForDepth(0) === GOLDEN_RATIO`, so root selection (depth 0) and child selection share one rule.
 * Computed in defined 32-bit space (`Math.imul` = true 32-bit multiply, `>>> 0` = unsigned) to avoid
 * JS float/bitwise footguns; depths are tiny so overflow is irrelevant to correctness.
 */
function seedForDepth(d: number): number {
	return (GOLDEN_RATIO ^ Math.imul(d, SEED_DEPTH_PRIME)) >>> 0;
}

/**
 * Given an encoded hash key and a parent's absolute depth, compute which child slot (0..K-1) the key
 * routes to. Single source of truth for hash-based child selection.
 *
 * The seed is `seedForDepth(parentAbsDepth + 1)` — the child's own depth — so each tree level uses a
 * distinct seed and siblings never cluster regardless of the key distribution.
 */
export function hashChildIndex(hashKey: KeyBytes, parentAbsDepth: number, K: number): number {
	return hashers.h32Raw(hashKey, seedForDepth(parentAbsDepth + 1)) % K;
}

/**
 * Compute the root partition index for an encoded hash key across rootTreesN root partitions.
 * Separate from hashChildIndex because root selection is depth 0 (`seedForDepth(0) === GOLDEN_RATIO`).
 */
export function hashRootIndex(hashKey: KeyBytes, rootTreesN: number): number {
	return hashers.h32Raw(hashKey, seedForDepth(0)) % rootTreesN;
}

export function hash64(data: string | Uint8Array, seed?: bigint): bigint {
	if (typeof data === "string") {
		return hashers.h64(data, seed ?? GOLDEN_RATIO_BIGINT);
	}
	return hashers.h64Raw(data, seed ?? GOLDEN_RATIO_BIGINT);
}

/**
 * The xxHash64 seed of the hashPrefixBuckets of a shard group, as 16 lowercase hex characters: xxh64
 * of its UTF-8 name with `baseSeed`. A platform can give its own `baseSeed`, and must then keep it for
 * the life of its data.
 */
export function hashPrefixBucketSeedOf(shardGroup: string, baseSeed: bigint = GOLDEN_RATIO_BIGINT): string {
	return hash64(shardGroup, baseSeed).toString(16).padStart(16, "0");
}

/** The hashPrefixBucket of an encoded hash key: xxh64(hashKey, seed), as 8 bytes, most significant byte first. */
export function hashPrefixBucket(hashKey: KeyBytes, seed: bigint): Uint8Array {
	const out = new Uint8Array(8);
	new DataView(out.buffer).setBigUint64(0, hashers.h64Raw(hashKey, seed), false);
	return out;
}

export function hash32(data: string | Uint8Array, seed?: number): number {
	if (typeof data === "string") {
		return hashers.h32(data, seed ?? GOLDEN_RATIO);
	}
	return hashers.h32Raw(data, seed ?? GOLDEN_RATIO);
}
