// Doesn't have a xxhash64 unfortunately in plain JS implementation.
// import { xxHash32 } from "js-xxhash";
import xxhash from "xxhash-wasm";
// Creates the WebAssembly instance.
const hashers = await xxhash();

import type { KeyBytes } from "./key-codec.js";
import invariant from "../shared/invariant.js";
import { CORE_INTERNAL_CODES, FokosInternalError } from "../shared/errors.js";

// Golden Ratio constant for better hash scattering.
// See https://softwareengineering.stackexchange.com/a/402543
export const GOLDEN_RATIO = 0x9e3779b1;
// The default base seed of `routingHashSeedOf`, so a change moves every stored routingHash. It must never change.
export const GOLDEN_RATIO_BIGINT = BigInt(0x9e3779b1);

declare const ROUTING_HASH_BRAND: unique symbol;

/**
 * A routingHash: the xxHash64 of an encoded hash key, seeded per shard group, as 8 bytes,
 * most significant byte first. The brand separates it from `KeyBytes` and from the interval bounds
 * of `FokosStorageKey`, which are positions in the storage order and can be shorter.
 */
export type RoutingHash = Uint8Array & { readonly [ROUTING_HASH_BRAND]: true };

/** The byte length of a routingHash. */
export const ROUTING_HASH_BYTES = 8;

/**
 * Brands `bytes` as a routingHash after the length check, so a wrong-length value fails where it
 * enters, not later in a routing or a storage read.
 */
export function asRoutingHash(bytes: Uint8Array): RoutingHash {
	if (bytes.length !== ROUTING_HASH_BYTES) {
		throw new FokosInternalError(CORE_INTERNAL_CODES.invariant_failed, {
			message: "an internal invariant failed",
			attributes: { detail: `fokos/hash-primitives: a routingHash is ${ROUTING_HASH_BYTES} bytes, got ${bytes.length}` },
		});
	}
	return bytes as RoutingHash;
}

export function hash64(data: string | Uint8Array, seed?: bigint): bigint {
	if (typeof data === "string") {
		return hashers.h64(data, seed ?? GOLDEN_RATIO_BIGINT);
	}
	return hashers.h64Raw(data, seed ?? GOLDEN_RATIO_BIGINT);
}

/**
 * The xxHash64 seed of the routingHashes of a shard group, as 16 lowercase hex characters: xxh64
 * of its UTF-8 name with `baseSeed`. A platform can give its own `baseSeed`, and must then keep it for
 * the life of its data.
 */
export function routingHashSeedOf(shardGroup: string, baseSeed: bigint = GOLDEN_RATIO_BIGINT): string {
	return hash64(shardGroup, baseSeed).toString(16).padStart(16, "0");
}

/** The routingHash of an encoded hash key: xxh64(hashKey, seed), as 8 bytes, most significant byte first. */
export function routingHash(hashKey: KeyBytes, seed: bigint): RoutingHash {
	return routingHashBytes(hashers.h64Raw(hashKey, seed));
}

/** The seed of each shard group that this isolate hashed for. A shard group is a table, a GSI, or a derived group. */
const shardGroupSeeds = new Map<string, bigint>();
const SHARD_GROUP_SEEDS_MAX = 1024;

/** The routingHash of an encoded hash key in `shardGroup`, with the seed `routingHashSeedOf(shardGroup)`. */
export function routingHashOf(hashKey: KeyBytes, shardGroup: string): RoutingHash {
	let seed = shardGroupSeeds.get(shardGroup);
	if (seed === undefined) {
		// The oldest seed goes. A hit stays one lookup, and a seed is cheap to compute again.
		if (shardGroupSeeds.size >= SHARD_GROUP_SEEDS_MAX) {
			shardGroupSeeds.delete(shardGroupSeeds.keys().next().value!);
		}
		seed = hash64(shardGroup);
		shardGroupSeeds.set(shardGroup, seed);
	}
	return routingHash(hashKey, seed);
}

/** The deepest hash partition of any topology. It bounds a lopsided branch, the hash arena, and the depth byte of a partition ID. */
export const MAX_HASH_DEPTH = 64;
const maxDepths = new Map<number, number>();

const TWO_64 = 1n << 64n;
const TWO_32 = 0x1_0000_0000;

/**
 * A cursor over the digits of a routingHash `x`, read as the fraction `x / 2^64`: the root digit
 * in base `rootTreesN`, then one digit for each `next(base)`. The value is two 32-bit limbs
 * `hi * 2^32 + lo`. One step multiplies it by `base` (at most 65,000), so each product is below 2^53
 * and exact in a number. The digit is the part above 2^64, and the rest stays in the limbs.
 */
export class RoutingHashDigitCursor {
	#hi: number;
	#lo: number;
	readonly root: number;

	constructor(routingHash: RoutingHash, rootTreesN: number) {
		// Read the bytes directly: a `DataView` for each cursor costs more than the whole walk.
		const b = routingHash;
		this.#hi = ((b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3]) >>> 0;
		this.#lo = ((b[4] << 24) | (b[5] << 16) | (b[6] << 8) | b[7]) >>> 0;
		this.root = this.next(rootTreesN);
	}

	next(base: number): number {
		const a = this.#lo * base;
		const carry = Math.floor(a / TWO_32);
		const b = this.#hi * base + carry;
		const digit = Math.floor(b / TWO_32);
		this.#hi = b - digit * TWO_32;
		this.#lo = a - carry * TWO_32;
		return digit;
	}
}

/** The digits of a routingHash: the root digit, then one digit for each level, `depth` levels. */
export function routingHashDigits(
	routingHash: RoutingHash,
	rootTreesN: number,
	hashSplitN: number,
	depth: number,
): { root: number; path: number[] } {
	const cursor = new RoutingHashDigitCursor(routingHash, rootTreesN);
	const path: number[] = [];
	for (let d = 0; d < depth; d++) {
		path.push(cursor.next(hashSplitN));
	}
	return { root: cursor.root, path };
}

/**
 * The deepest hash partition of a topology: at most `MAX_HASH_DEPTH`, and the largest `d` with
 * `rootTreesN * hashSplitN^d <= 2^64`. Down to that depth each node interval holds at least one
 * routingHash, so the digits of a routingHash are exact.
 */
export function maxHashDepth(rootTreesN: number, hashSplitN: number): number {
	const memoKey = rootTreesN * 256 + hashSplitN;
	let depth = maxDepths.get(memoKey);
	if (depth === undefined) {
		depth = 0;
		let nodes = BigInt(rootTreesN);
		while (depth < MAX_HASH_DEPTH && nodes * BigInt(hashSplitN) <= TWO_64) {
			nodes *= BigInt(hashSplitN);
			depth += 1;
		}
		maxDepths.set(memoKey, depth);
	}
	return depth;
}

/**
 * The routingHash interval `[start, end)` of the hash node at `root` and `path`. A null `end` has
 * no upper bound: the last node of a level ends at 2^64, which has no 8-byte form. A routingHash
 * is in the interval exactly when its digits give `root` and `path`, because both are
 * `floor(x * D / 2^64)` with `D = rootTreesN * hashSplitN^depth`.
 */
export function routingHashInterval(
	rootTreesN: number,
	hashSplitN: number,
	root: number,
	path: ArrayLike<number>,
): { start: RoutingHash; end: RoutingHash | null } {
	invariant(root < rootTreesN, "fokos/hash-primitives: the hash node is outside the topology");
	const k = BigInt(hashSplitN);
	let numer = BigInt(root);
	let nodes = BigInt(rootTreesN);
	for (let d = 0; d < path.length; d++) {
		invariant(path[d] < hashSplitN, "fokos/hash-primitives: the hash node is outside the topology");
		numer = numer * k + BigInt(path[d]);
		nodes *= k;
	}
	const start = routingHashBytes(ceilDiv(numer << 64n, nodes));
	return { start, end: numer + 1n === nodes ? null : routingHashBytes(ceilDiv((numer + 1n) << 64n, nodes)) };
}

function ceilDiv(a: bigint, b: bigint): bigint {
	return (a + b - 1n) / b;
}

// One view for every write. A `DataView` for each routingHash made a route resolve several times
// slower. The write and the copy have no `await` between them, so no other call can use the view.
const scratchView = new DataView(new ArrayBuffer(8));
const scratchBytes = new Uint8Array(scratchView.buffer);

/** The 8 bytes of `value`, most significant byte first, in a new array. */
function routingHashBytes(value: bigint): RoutingHash {
	scratchView.setBigUint64(0, value, false);
	return scratchBytes.slice() as RoutingHash;
}

/** The unsigned byte order of two routingHashes, which is their numeric order. */
export function compareRoutingHashes(a: RoutingHash, b: RoutingHash): number {
	for (let i = 0; i < ROUTING_HASH_BYTES; i++) {
		if (a[i] !== b[i]) {
			return a[i] - b[i];
		}
	}
	return 0;
}

/** True when `routingHash` is in `[start, end)`. A null `end` has no upper bound. */
export function inRoutingHashInterval(routingHash: RoutingHash, start: RoutingHash, end: RoutingHash | null): boolean {
	return compareRoutingHashes(routingHash, start) >= 0 && (end === null || compareRoutingHashes(routingHash, end) < 0);
}

export function hash32(data: string | Uint8Array, seed?: number): number {
	if (typeof data === "string") {
		return hashers.h32(data, seed ?? GOLDEN_RATIO);
	}
	return hashers.h32Raw(data, seed ?? GOLDEN_RATIO);
}
