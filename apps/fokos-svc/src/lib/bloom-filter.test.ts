import { describe, expect, it } from "vitest";
import { AddResult, BloomFilter } from "./bloom-filter.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sequentialKeys(prefix: string, n: number): string[] {
	return Array.from({ length: n }, (_, i) => `${prefix}-${i}`);
}

// Keys that are never passed to add() in any test — used to probe for false positives.
// A fresh empty filter is guaranteed to return false for these; after population the
// 1% error rate means the probability of a specific key being a false positive is ~1%,
// so we only call has() on an empty filter for the "not added" assertion below.
function absentKeys(n: number): string[] {
	return sequentialKeys("absent", n);
}

// 55 keys verified to produce zero false positives against each other when
// inserted sequentially into a filter with {initialCapacityN: 4, errorRate: 0.01}.
// With tiny layers the cross-layer FPR is high enough that naive sequential keys
// ("k-0" … "k-58") include 4 false positives (k-16, k-36, k-43, k-54). These 55
// were found by inserting candidates and skipping any that already matched.
// xxHash32 is deterministic so this list is stable across runs.
const TINY_CAPACITY_KEYS = [
	"k-0",
	"k-1",
	"k-2",
	"k-3",
	"k-4",
	"k-5",
	"k-6",
	"k-7",
	"k-8",
	"k-9",
	"k-10",
	"k-11",
	"k-12",
	"k-13",
	"k-14",
	"k-15",
	"k-17",
	"k-18",
	"k-19",
	"k-20",
	"k-21",
	"k-22",
	"k-23",
	"k-24",
	"k-25",
	"k-26",
	"k-27",
	"k-28",
	"k-29",
	"k-30",
	"k-31",
	"k-32",
	"k-33",
	"k-34",
	"k-35",
	"k-37",
	"k-38",
	"k-39",
	"k-40",
	"k-41",
	"k-42",
	"k-44",
	"k-45",
	"k-46",
	"k-47",
	"k-48",
	"k-49",
	"k-50",
	"k-51",
	"k-52",
	"k-53",
	"k-55",
	"k-56",
	"k-57",
	"k-58",
];

// 10 keys verified to not false-positive after inserting TINY_CAPACITY_KEYS[0..50]
// into a filter with {initialCapacityN: 4, errorRate: 0.01}. "post-restore-9" is
// a false positive and is excluded.
const TINY_CAPACITY_POST_RESTORE_KEYS = [
	"post-restore-0",
	"post-restore-1",
	"post-restore-2",
	"post-restore-3",
	"post-restore-4",
	"post-restore-5",
	"post-restore-6",
	"post-restore-7",
	"post-restore-8",
	"post-restore-10",
];

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
		keys: sequentialKeys("added", 50),
		postRestoreKeys: sequentialKeys("post-restore", 10),
		expectedMinLayers: 1,
		expectedMaxLayers: 1,
	},
	{
		name: "two layers",
		options: { maxSizeBytes: 500_000, initialCapacityN: 64, errorRate: 0.01 },
		keys: sequentialKeys("added", 80),
		postRestoreKeys: sequentialKeys("post-restore", 10),
		expectedMinLayers: 2,
		expectedMaxLayers: 2,
	},
	{
		name: "many layers (tiny capacity)",
		options: { maxSizeBytes: 500_000, initialCapacityN: 4, errorRate: 0.01 },
		keys: TINY_CAPACITY_KEYS.slice(0, 50),
		postRestoreKeys: TINY_CAPACITY_POST_RESTORE_KEYS,
		expectedMinLayers: 3,
		expectedMaxLayers: 10,
	},
	{
		name: "defaults",
		options: { maxSizeBytes: 500_000, errorRate: undefined, initialCapacityN: undefined },
		keys: sequentialKeys("added", 200),
		postRestoreKeys: sequentialKeys("post-restore", 10),
		expectedMinLayers: 1,
		expectedMaxLayers: 5,
	},
];

// ---------------------------------------------------------------------------
// Core suite — runs for every configuration above
// ---------------------------------------------------------------------------

describe.each(CONFIGS)("BloomFilter — $name", ({ options, keys, postRestoreKeys, expectedMinLayers, expectedMaxLayers }) => {
	function make() {
		return BloomFilter.create(options);
	}

	it("has() returns false for every key on a fresh empty filter", () => {
		const f = make();
		for (const k of absentKeys(20)) {
			expect(f.has(k)).toBe(false);
		}
	});

	it("add() returns Added for every new key within capacity", () => {
		const f = make();
		for (const k of keys) {
			expect(f.add(k)).toBe(AddResult.Added);
		}
	});

	it("has() returns true for all added keys (no false negatives)", () => {
		const f = make();
		for (const k of keys) f.add(k);
		for (const k of keys) {
			expect(f.has(k)).toBe(true);
		}
	});

	it("additionsCount() equals number of distinct keys added", () => {
		const f = make();
		expect(f.additionsCount()).toBe(0);
		for (let i = 0; i < keys.length; i++) {
			f.add(keys[i]);
			expect(f.additionsCount()).toBe(i + 1);
		}
	});

	it("add() does not increment count for duplicates", () => {
		const f = make();
		expect(f.add("dup")).toBe(AddResult.Added);
		expect(f.add("dup")).toBe(AddResult.AlreadyPresent);
		expect(f.additionsCount()).toBe(1);
	});

	it("creates the expected number of layers for the given key count", () => {
		const f = make();
		for (const k of keys) f.add(k);
		const layerCount = f.toSnapshot().layers.length;
		expect(layerCount).toBeGreaterThanOrEqual(expectedMinLayers);
		expect(layerCount).toBeLessThanOrEqual(expectedMaxLayers);
	});

	it("toSnapshot/fromSnapshot: restored filter finds all previously added keys", () => {
		const f = make();
		for (const k of keys) f.add(k);

		const restored = BloomFilter.fromSnapshot(f.toSnapshot());

		for (const k of keys) {
			expect(restored.has(k)).toBe(true);
		}
	});

	it("toSnapshot/fromSnapshot: keyCount is preserved", () => {
		const f = make();
		for (const k of keys) f.add(k);

		const restored = BloomFilter.fromSnapshot(f.toSnapshot());
		expect(restored.additionsCount()).toBe(keys.length);
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
		for (const k of keys) f.add(k);

		const restored = BloomFilter.fromSnapshot(f.toSnapshot());
		for (const k of postRestoreKeys) restored.add(k);

		for (const k of keys) expect(restored.has(k)).toBe(true);
		for (const k of postRestoreKeys) expect(restored.has(k)).toBe(true);
		expect(restored.additionsCount()).toBe(keys.length + postRestoreKeys.length);
	});
});

// ---------------------------------------------------------------------------
// maxSizeBytes enforcement
// ---------------------------------------------------------------------------

describe("BloomFilter — maxSizeBytes enforcement", () => {
	it("add() eventually returns Full when maxSizeBytes is exhausted", () => {
		// Tiny maxSizeBytes + tiny initialCapacityN to hit the ceiling quickly.
		const f = BloomFilter.create({ maxSizeBytes: 300, initialCapacityN: 4, errorRate: 0.01 });
		let rejected = false;
		for (let i = 0; i < 100_000; i++) {
			if (f.add(`key-${i}`) === AddResult.Full) {
				rejected = true;
				break;
			}
		}
		expect(rejected).toBe(true);
	});

	it("additionsCount() does not increase after add() returns Full", () => {
		const f = BloomFilter.create({ maxSizeBytes: 300, initialCapacityN: 4, errorRate: 0.01 });
		let countBeforeRejection = 0;
		for (let i = 0; i < 100_000; i++) {
			if (f.add(`key-${i}`) === AddResult.Full) {
				expect(f.additionsCount()).toBe(countBeforeRejection);
				break;
			}
			countBeforeRejection = f.additionsCount();
		}
	});

	it("has() returns false for a key that was rejected due to maxSizeBytes", () => {
		const f = BloomFilter.create({ maxSizeBytes: 300, initialCapacityN: 4, errorRate: 0.01 });
		let rejectedKey: string | null = null;
		for (let i = 0; i < 100_000; i++) {
			const k = `key-${i}`;
			if (f.add(k) === AddResult.Full) {
				rejectedKey = k;
				break;
			}
		}
		// A rejected key was never inserted, so it must not be reported as present.
		// (It could be a false positive, but with 1% FPR the probability is negligible.)
		expect(rejectedKey).not.toBeNull();
		expect(f.has(rejectedKey!)).toBe(false);
	});

	it("add() continues to return Full for genuinely new keys once saturated", () => {
		const f = BloomFilter.create({ maxSizeBytes: 300, initialCapacityN: 4, errorRate: 0.01 });
		let saturated = false;
		for (let i = 0; i < 100_000; i++) {
			if (f.add(`key-${i}`) === AddResult.Full) {
				saturated = true;
				break;
			}
		}
		expect(saturated).toBe(true);
		// Keys that don't false-positive against the saturated filter must be rejected.
		// extra-3..6 are false positives (verified via xxHash32); extra-0..2 are not.
		expect(f.add("extra-0")).toBe(AddResult.Full);
		expect(f.add("extra-1")).toBe(AddResult.Full);
		expect(f.add("extra-2")).toBe(AddResult.Full);
	});

	it("add() returns AlreadyPresent for false-positive keys even when saturated", () => {
		const f = BloomFilter.create({ maxSizeBytes: 300, initialCapacityN: 4, errorRate: 0.01 });
		for (let i = 0; i < 100_000; i++) {
			if (f.add(`key-${i}`) === AddResult.Full) break;
		}
		// extra-3 false-positives against existing bits — the filter correctly
		// reports it as "already present".
		expect(f.has("extra-3")).toBe(true);
		expect(f.add("extra-3")).toBe(AddResult.AlreadyPresent);
	});

	it("throws when maxSizeBytes is too small for the very first layer", () => {
		expect(() => BloomFilter.create({ maxSizeBytes: 1, errorRate: 0.01 })).toThrow(/too small/);
	});
});
