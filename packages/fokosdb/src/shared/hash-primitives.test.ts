import { describe, expect, it } from "vitest";
import { GOLDEN_RATIO, hashChildIndex, hashRootIndex } from "./hash-primitives.js";
import { KeyCodec } from "./partition-topology/key-codec.js";

const kb = (s: string) => KeyCodec.encode(s);

// A spread of fixture keys (BMP + astral + ASCII) to exercise the byte hashers.
const KEYS = Array.from({ length: 500 }, (_, i) => kb(`key-${i}-${i % 7 === 0 ? "😀" : "x"}-${"a".repeat(i % 11)}`));

describe("hash-primitives determinism", () => {
	it("hashChildIndex / hashRootIndex are deterministic for the same bytes", () => {
		for (const k of KEYS.slice(0, 20)) {
			expect(hashRootIndex(k, 8)).toBe(hashRootIndex(KeyCodec.asKeyBytes(k.slice()), 8));
			expect(hashChildIndex(k, 3, 4)).toBe(hashChildIndex(KeyCodec.asKeyBytes(k.slice()), 3, 4));
		}
	});

	it("indices are always within [0, K)", () => {
		for (const k of KEYS) {
			expect(hashRootIndex(k, 8)).toBeGreaterThanOrEqual(0);
			expect(hashRootIndex(k, 8)).toBeLessThan(8);
			for (const depth of [0, 1, 5, 17]) {
				const idx = hashChildIndex(k, depth, 4);
				expect(idx).toBeGreaterThanOrEqual(0);
				expect(idx).toBeLessThan(4);
			}
		}
	});

	it("hashRootIndex equals hashChildIndex at the root seed (seedForDepth(0) === GOLDEN_RATIO)", () => {
		// hashRootIndex uses seedForDepth(0); hashChildIndex at parentAbsDepth=-1 uses seedForDepth(0) too.
		// Both reduce to xxHash32(bytes, GOLDEN_RATIO) % n.
		for (const k of KEYS.slice(0, 20)) {
			expect(hashRootIndex(k, 5)).toBe(hashChildIndex(k, -1, 5));
		}
		// sanity: the constant is what we think it is
		expect(GOLDEN_RATIO).toBe(0x9e3779b1);
	});
});

describe("depth seeding decorrelates levels", () => {
	it("a key's child slot varies across depths (no single fixed slot for all levels)", () => {
		// For a healthy seed schedule, the per-depth slot of a key should not be constant across depths.
		const k = kb("decorrelation-probe-😀");
		const slots = new Set(Array.from({ length: 16 }, (_, d) => hashChildIndex(k, d, 4)));
		expect(slots.size).toBeGreaterThan(1);
	});

	it("seedForDepth is collision-free across small depths (distinct per-depth hash streams)", () => {
		// Indirect bijection check: across many keys, the joint (depth-d slot) vectors differ between
		// depths — if two depths shared a seed, their slot assignment would be identical for every key.
		const sampleKeys = KEYS.slice(0, 200);
		const fingerprint = (d: number) => sampleKeys.map((k) => hashChildIndex(k, d, 251)).join(",");
		const fps = new Set<string>();
		for (let d = 0; d < 32; d++) fps.add(fingerprint(d));
		expect(fps.size).toBe(32); // all 32 depths produce a distinct assignment ⇒ distinct seeds
	});
});

describe("distribution is roughly uniform", () => {
	it("spreads a fixture key set across K root slots within tolerance", () => {
		const K = 8;
		const counts = new Array(K).fill(0);
		for (const k of KEYS) counts[hashRootIndex(k, K)]++;
		const expected = KEYS.length / K;
		// Loose chi-square-free bound: no bucket should be wildly off (within 2.5x of expected).
		for (const c of counts) {
			expect(c).toBeGreaterThan(expected * 0.4);
			expect(c).toBeLessThan(expected * 2.5);
		}
	});
});
