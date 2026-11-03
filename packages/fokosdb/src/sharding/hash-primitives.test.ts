import fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
	asHashPrefixBucket,
	GOLDEN_RATIO_BIGINT,
	HashPrefixBucketDigitCursor,
	hash64,
	hashPrefixBucket,
	hashPrefixBucketDigits,
	hashPrefixBucketInterval,
	hashPrefixBucketOf,
	hashPrefixBucketSeedOf,
	inHashPrefixBucketInterval,
	MAX_HASH_DEPTH,
	maxHashDepth,
	type HashPrefixBucket,
} from "./hash-primitives.js";
import { KeyCodec } from "./key-codec.js";

const kb = (s: string) => KeyCodec.encode(s);

// A spread of fixture keys (BMP + astral + ASCII) to exercise the byte hashers.
const KEYS = Array.from({ length: 500 }, (_, i) => kb(`key-${i}-${i % 7 === 0 ? "😀" : "x"}-${"a".repeat(i % 11)}`));

const TWO_64 = 1n << 64n;
const toBigInt = (bucket: Uint8Array) => BigInt(`0x${bucket.toHex()}`);
function bucketOf(value: bigint): HashPrefixBucket {
	const out = new Uint8Array(8);
	new DataView(out.buffer).setBigUint64(0, value, false);
	return asHashPrefixBucket(out);
}

/** The reference digits: each step is `floor(x * M / 2^64)`, and the rest is `x * M mod 2^64`. */
function referenceDigits(value: bigint, bases: number[]): number[] {
	let x = value;
	return bases.map((base) => {
		const product = x * BigInt(base);
		x = product % TWO_64;
		return Number(product / TWO_64);
	});
}

describe("the digits of a hashPrefixBucket", () => {
	it("each limb step equals the bigint reference, for every base and the edge values", () => {
		const values = [0n, 1n, TWO_64 - 1n, TWO_64 / 2n, TWO_64 / 2n - 1n, 0xc000000000000000n, 0x123456789abcdef0n];
		for (let base = 1; base <= 65_000; base++) {
			for (const value of values) {
				const cursor = new HashPrefixBucketDigitCursor(bucketOf(value), base);
				const bases = [base, 255, 2, base];
				expect([cursor.root, cursor.next(255), cursor.next(2), cursor.next(base)]).toEqual(referenceDigits(value, bases));
			}
		}
	});

	it("each limb step equals the bigint reference for random hashPrefixBuckets and bases", () => {
		fc.assert(
			fc.property(
				fc.bigInt({ min: 0n, max: TWO_64 - 1n }),
				fc.array(fc.integer({ min: 1, max: 65_000 }), { minLength: 1, maxLength: 70 }),
				(value, bases) => {
					const cursor = new HashPrefixBucketDigitCursor(bucketOf(value), bases[0]);
					const digits = [cursor.root, ...bases.slice(1).map((base) => cursor.next(base))];
					expect(digits).toEqual(referenceDigits(value, bases));
				},
			),
			{ numRuns: 500 },
		);
	});

	it("gives the digits of the worked example: R 3, K 2, and the fraction 0.75", () => {
		expect(hashPrefixBucketDigits(bucketOf(0xc000000000000000n), 3, 2, 2)).toEqual({ root: 2, path: [0, 1] });
		const { start, end } = hashPrefixBucketInterval(3, 2, 2, [0, 1]);
		expect(toBigInt(start)).toBe(0xc000000000000000n);
		// ceil(10 * 2^64 / 12)
		expect(toBigInt(end!)).toBe((10n * TWO_64 + 11n) / 12n);
	});

	it("puts a hashPrefixBucket in the interval of a node exactly when its digits give the path of the node", () => {
		fc.assert(
			fc.property(
				fc.integer({ min: 1, max: 65_000 }),
				fc.integer({ min: 2, max: 255 }),
				fc.bigInt({ min: 0n, max: TWO_64 - 1n }),
				fc.nat(),
				(R, K, value, depthPick) => {
					const depth = depthPick % (maxHashDepth(R, K) + 1);
					const bucket = bucketOf(value);
					const { root, path } = hashPrefixBucketDigits(bucket, R, K, depth);
					const { start, end } = hashPrefixBucketInterval(R, K, root, path);
					expect(inHashPrefixBucketInterval(bucket, start, end)).toBe(true);
					// The edges of the interval have the same digits, and the values next to them do not.
					const digitsOf = (v: bigint) => hashPrefixBucketDigits(bucketOf(v), R, K, depth);
					expect(digitsOf(toBigInt(start))).toEqual({ root, path });
					const last = end === null ? TWO_64 - 1n : toBigInt(end) - 1n;
					expect(digitsOf(last)).toEqual({ root, path });
					if (toBigInt(start) > 0n) {
						expect(digitsOf(toBigInt(start) - 1n)).not.toEqual({ root, path });
					}
					if (end !== null) {
						expect(digitsOf(toBigInt(end))).not.toEqual({ root, path });
					}
				},
			),
			{ numRuns: 500 },
		);
	});

	it("divides the interval of a node into the intervals of its children, with no gap and no overlap", () => {
		fc.assert(
			fc.property(
				fc.integer({ min: 1, max: 65_000 }),
				fc.integer({ min: 2, max: 255 }),
				fc.array(fc.nat(), { maxLength: 64 }),
				fc.nat(),
				(R, K, picks, rootPick) => {
					const depth = Math.min(picks.length, maxHashDepth(R, K) - 1);
					const root = rootPick % R;
					const path = picks.slice(0, depth).map((p) => p % K);
					const parent = hashPrefixBucketInterval(R, K, root, path);
					const children = Array.from({ length: K }, (_, c) => hashPrefixBucketInterval(R, K, root, [...path, c]));
					expect(children[0].start).toEqual(parent.start);
					for (let c = 0; c + 1 < K; c++) {
						expect(children[c].end).toEqual(children[c + 1].start);
					}
					expect(children[K - 1].end).toEqual(parent.end);
				},
			),
			{ numRuns: 300 },
		);
	});

	it("spreads a fixture key set across the roots", () => {
		const R = 8;
		const counts = Array.from({ length: R }, () => 0);
		for (const k of KEYS) {
			counts[new HashPrefixBucketDigitCursor(hashPrefixBucketOf(k, "fokos.p.spread"), R).root]++;
		}
		const expected = KEYS.length / R;
		// Loose chi-square-free bound: no root should be wildly off (within 2.5x of expected).
		for (const c of counts) {
			expect(c).toBeGreaterThan(expected * 0.4);
			expect(c).toBeLessThan(expected * 2.5);
		}
	});
});

describe("maxHashDepth", () => {
	it("is the largest depth with rootTreesN * hashSplitN^depth <= 2^64, at most MAX_HASH_DEPTH", () => {
		expect(MAX_HASH_DEPTH).toBe(64);
		expect(maxHashDepth(1, 2)).toBe(64);
		expect(maxHashDepth(16, 2)).toBe(60);
		expect(maxHashDepth(16, 4)).toBe(30);
		expect(maxHashDepth(65_000, 255)).toBe(6);
		expect(maxHashDepth(1, 255)).toBe(8);
	});
});

describe("hashPrefixBucket", () => {
	const hex = (bytes: Uint8Array) => bytes.toHex();

	it("hashPrefixBucketOf uses the seed of the shard group", () => {
		const seed = BigInt(`0x${hashPrefixBucketSeedOf("fokos.p.orders")}`);
		expect(hashPrefixBucketOf(kb("user#42"), "fokos.p.orders")).toEqual(hashPrefixBucket(kb("user#42"), seed));
	});

	it("gives 8 bytes, most significant byte first, equal to xxh64 of the key bytes", () => {
		const seed = BigInt(`0x${hashPrefixBucketSeedOf("fokos.p.orders")}`);
		for (const k of KEYS.slice(0, 50)) {
			const bucket = hashPrefixBucket(k, seed);
			expect(bucket).toHaveLength(8);
			expect(BigInt(`0x${hex(bucket)}`)).toBe(hash64(k, seed));
		}
	});

	it("pins the seed of a shard group and the hashPrefixBucket of a key", () => {
		const seed = hashPrefixBucketSeedOf("fokos.p.orders");
		expect(seed).toMatch(/^[0-9a-f]{16}$/);
		expect(seed).toBe("3291cf945f451beb");
		expect(hex(hashPrefixBucket(kb("user#42"), BigInt(`0x${seed}`)))).toBe("79ea4b6db710a9a9");
	});

	it("derives the seed of a shard group from a base seed, which defaults to GOLDEN_RATIO_BIGINT", () => {
		expect(hashPrefixBucketSeedOf("fokos.p.orders", GOLDEN_RATIO_BIGINT)).toBe("3291cf945f451beb");
		const custom = hashPrefixBucketSeedOf("fokos.p.orders", 42n);
		expect(custom).toMatch(/^[0-9a-f]{16}$/);
		expect(custom).not.toBe("3291cf945f451beb");
		expect(BigInt(`0x${custom}`)).toBe(hash64("fokos.p.orders", 42n));
	});
});
