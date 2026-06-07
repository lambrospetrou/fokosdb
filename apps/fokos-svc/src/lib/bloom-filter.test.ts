import { describe, expect, it } from "vitest";
import { BloomFilter } from "./bloom-filter.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function addedKeys(n: number): string[] {
	return Array.from({ length: n }, (_, i) => `added-${i}`);
}

// Keys that are never passed to add() in any test — used to probe for false positives.
// A fresh empty filter is guaranteed to return false for these; after population the
// 1% error rate means the probability of a specific key being a false positive is ~1%,
// so we only call has() on an empty filter for the "not added" assertion below.
function absentKeys(n: number): string[] {
	return Array.from({ length: n }, (_, i) => `absent-${i}`);
}

// ---------------------------------------------------------------------------
// Configurations exercised by the shared core suite
// ---------------------------------------------------------------------------

// "single layer" — initialCapacityN is large enough that the keys added never
//   fill the first layer, so the filter stays at exactly one layer.
// "two layers" — initialCapacityN=64, add 80 keys: layer 0 holds 64, layer 1 holds 16.
// "many layers (tiny capacity)" — initialCapacityN=4 forces a new layer every 4/8/16/…
//   keys, so adding 50 keys creates 4 layers.
// "defaults" — neither errorRate nor initialCapacityN is specified.

const CONFIGS = [
	{
		name: "single layer",
		options: { maxSizeBytes: 500_000, initialCapacityN: 10_000, errorRate: 0.01 },
		keysToAdd: 50,
		expectedMinLayers: 1,
		expectedMaxLayers: 1,
	},
	{
		name: "two layers",
		options: { maxSizeBytes: 500_000, initialCapacityN: 64, errorRate: 0.01 },
		keysToAdd: 80,
		expectedMinLayers: 2,
		expectedMaxLayers: 2,
	},
	{
		name: "many layers (tiny capacity)",
		options: { maxSizeBytes: 500_000, initialCapacityN: 4, errorRate: 0.01 },
		keysToAdd: 50,
		expectedMinLayers: 3,
		expectedMaxLayers: 10,
	},
	{
		name: "defaults",
		options: { maxSizeBytes: 500_000, errorRate: undefined, initialCapacityN: undefined },
		keysToAdd: 200,
		expectedMinLayers: 1,
		expectedMaxLayers: 5,
	},
] as const;

// ---------------------------------------------------------------------------
// Core suite — runs for every configuration above
// ---------------------------------------------------------------------------

describe.each(CONFIGS)("BloomFilter — $name", ({ options, keysToAdd, expectedMinLayers, expectedMaxLayers }) => {
	function make() {
		return BloomFilter.create(options);
	}

	it("has() returns false for every key on a fresh empty filter", () => {
		const f = make();
		for (const k of absentKeys(20)) {
			expect(f.has(k)).toBe(false);
		}
	});

	it("add() returns true for every key within capacity", () => {
		const f = make();
		for (const k of addedKeys(keysToAdd)) {
			expect(f.add(k)).toBe(true);
		}
	});

	it("has() returns true for all added keys (no false negatives)", () => {
		const f = make();
		const keys = addedKeys(keysToAdd);
		for (const k of keys) f.add(k);
		for (const k of keys) {
			expect(f.has(k)).toBe(true);
		}
	});

	it("keyCount() reflects insertions accurately", () => {
		const f = make();
		expect(f.keyCount()).toBe(0);
		const keys = addedKeys(keysToAdd);
		for (let i = 0; i < keys.length; i++) {
			f.add(keys[i]);
			expect(f.keyCount()).toBe(i + 1);
		}
	});

	it("adding the same key twice increments keyCount twice (no deduplication)", () => {
		const f = make();
		f.add("dup");
		f.add("dup");
		expect(f.keyCount()).toBe(2);
	});

	it("creates the expected number of layers for the given key count", () => {
		const f = make();
		for (const k of addedKeys(keysToAdd)) f.add(k);
		const layerCount = f.toSnapshot().layers.length;
		expect(layerCount).toBeGreaterThanOrEqual(expectedMinLayers);
		expect(layerCount).toBeLessThanOrEqual(expectedMaxLayers);
	});

	it("toSnapshot/fromSnapshot: restored filter finds all previously added keys", () => {
		const f = make();
		const keys = addedKeys(keysToAdd);
		for (const k of keys) f.add(k);

		const restored = BloomFilter.fromSnapshot(f.toSnapshot());

		for (const k of keys) {
			expect(restored.has(k)).toBe(true);
		}
	});

	it("toSnapshot/fromSnapshot: keyCount is preserved", () => {
		const f = make();
		for (const k of addedKeys(keysToAdd)) f.add(k);

		const restored = BloomFilter.fromSnapshot(f.toSnapshot());
		expect(restored.keyCount()).toBe(keysToAdd);
	});

	it("toSnapshot/fromSnapshot: snapshot fields match the creation options", () => {
		const f = make();
		const snap = f.toSnapshot();

		expect(snap.version).toBe(1);
		expect(snap.errorRate).toBe(options?.errorRate ?? 0.01);
		expect(snap.maxSizeBytes).toBe(options.maxSizeBytes);
		expect(snap.initialCapacityN).toBe(options?.initialCapacityN ?? 1024);
		expect(snap.layers.length).toBeGreaterThanOrEqual(1);
	});

	it("toSnapshot/fromSnapshot: restored filter correctly accepts new keys after restore", () => {
		const f = make();
		const before = addedKeys(keysToAdd);
		for (const k of before) f.add(k);

		const restored = BloomFilter.fromSnapshot(f.toSnapshot());
		const after = Array.from({ length: 10 }, (_, i) => `post-restore-${i}`);
		for (const k of after) restored.add(k);

		for (const k of before) expect(restored.has(k)).toBe(true);
		for (const k of after) expect(restored.has(k)).toBe(true);
		expect(restored.keyCount()).toBe(keysToAdd + 10);
	});
});

// ---------------------------------------------------------------------------
// maxSizeBytes enforcement
// ---------------------------------------------------------------------------

describe("BloomFilter — maxSizeBytes enforcement", () => {
	it("add() eventually returns false when maxSizeBytes is exhausted", () => {
		// Tiny maxSizeBytes + tiny initialCapacityN to hit the ceiling quickly.
		const f = BloomFilter.create({ maxSizeBytes: 300, initialCapacityN: 4, errorRate: 0.01 });
		let rejected = false;
		for (let i = 0; i < 100_000; i++) {
			if (!f.add(`key-${i}`)) {
				rejected = true;
				break;
			}
		}
		expect(rejected).toBe(true);
	});

	it("keyCount() does not increase after add() returns false", () => {
		const f = BloomFilter.create({ maxSizeBytes: 300, initialCapacityN: 4, errorRate: 0.01 });
		let countBeforeRejection = 0;
		for (let i = 0; i < 100_000; i++) {
			if (!f.add(`key-${i}`)) {
				expect(f.keyCount()).toBe(countBeforeRejection);
				break;
			}
			countBeforeRejection = f.keyCount();
		}
	});

	it("has() returns false for a key that was rejected due to maxSizeBytes", () => {
		const f = BloomFilter.create({ maxSizeBytes: 300, initialCapacityN: 4, errorRate: 0.01 });
		let rejectedKey: string | null = null;
		for (let i = 0; i < 100_000; i++) {
			const k = `key-${i}`;
			if (!f.add(k)) {
				rejectedKey = k;
				break;
			}
		}
		// A rejected key was never inserted, so it must not be reported as present.
		// (It could be a false positive, but with 1% FPR the probability is negligible.)
		expect(rejectedKey).not.toBeNull();
		expect(f.has(rejectedKey!)).toBe(false);
	});

	it("add() continues to return false on every subsequent call once saturated", () => {
		const f = BloomFilter.create({ maxSizeBytes: 300, initialCapacityN: 4, errorRate: 0.01 });
		// Fill to saturation.
		let saturated = false;
		for (let i = 0; i < 100_000; i++) {
			if (!f.add(`key-${i}`)) {
				saturated = true;
				break;
			}
		}
		expect(saturated).toBe(true);
		// All subsequent attempts must also fail.
		for (let i = 0; i < 5; i++) {
			expect(f.add(`extra-${i}`)).toBe(false);
		}
	});

	it("throws when maxSizeBytes is too small for the very first layer", () => {
		expect(() => BloomFilter.create({ maxSizeBytes: 1, errorRate: 0.01 })).toThrow(/too small/);
	});
});
