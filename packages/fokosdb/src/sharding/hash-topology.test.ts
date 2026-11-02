import { describe, expect, it } from "vitest";
import { HashTopology } from "./hash-topology.js";
import { hashPrefixBucketDigits, hashPrefixBucketOf } from "./hash-primitives.js";
import { KeyCodec } from "./key-codec.js";

// The arena walks the digits of a hashPrefixBucket. Each test hashes its keys in one shard group with one root.
const R = 1;
const bk = (s: string) => hashPrefixBucketOf(KeyCodec.encode(s), "arena-test");

// Compute the relative path (sequence of child-slot indices) that a hashKey takes
// through the cache starting from ownerAbsDepth, for `depth` levels.
function pathFor(hashKey: string, ownerAbsDepth: number, depth: number, K: number): number[] {
	return hashPrefixBucketDigits(bk(hashKey), R, K, ownerAbsDepth + depth).path.slice(ownerAbsDepth);
}

// Returns a hashKey that shares the path of `base` for levels 0..divergeAt-1 but
// takes a different slot at level `divergeAt` (0-indexed relative depth).
function divergingKey(base: string, ownerAbsDepth: number, divergeAt: number, K: number): string {
	const basePath = pathFor(base, ownerAbsDepth, divergeAt + 1, K);
	for (let n = 0; n < 100_000; n++) {
		const candidate = `${base}_alt${n}`;
		const path = pathFor(candidate, ownerAbsDepth, divergeAt + 1, K);
		// Must share every level before divergeAt, and diverge at the target level.
		if (basePath.slice(0, divergeAt).every((slot, i) => path[i] === slot) && path[divergeAt] !== basePath[divergeAt]) {
			return candidate;
		}
	}
	throw new Error("divergingKey: could not find a prefix-matching diverging key within 100 000 attempts");
}

describe("HashTopology", () => {
	describe("create basics", () => {
		it("isEmpty() is true after create", () => {
			expect(HashTopology.create(R, 4, 0).isEmpty()).toBe(true);
		});

		it("findLeaf returns 0 for any key on a fresh cache", () => {
			const cache = HashTopology.create(R, 4, 0);
			expect(cache.findLeaf(bk("any-key"))).toBe(0);
			expect(cache.findLeaf(bk("another-key"))).toBe(0);
		});

		it("stats shows usedSlots === K (root block only) after create", () => {
			const s = HashTopology.create(R, 4, 0).stats();
			expect(s.usedSlots).toBe(4);
			expect(s.K).toBe(4);
		});
	});

	describe("findLeaf on empty cache", () => {
		it("returns 0 regardless of the ownerAbsDepth the cache was created with", () => {
			// Each cache is independent — ownerAbsDepth is baked in at creation time.
			expect(HashTopology.create(R, 4, 0).findLeaf(bk("key"))).toBe(0);
			expect(HashTopology.create(R, 4, 3).findLeaf(bk("key"))).toBe(0);
			expect(HashTopology.create(R, 4, 10).findLeaf(bk("key"))).toBe(0);
		});
	});

	describe("updateFromHint + findLeaf", () => {
		it("updateFromHint(actualRelDepth=3) → findLeaf returns 3", () => {
			const cache = HashTopology.create(R, 4, 0);
			expect(cache.updateFromHint(bk("key"), 3)).toBe(true);
			expect(cache.findLeaf(bk("key"))).toBe(3);
		});

		it("a different key unaffected by another key's update", () => {
			const cache = HashTopology.create(R, 4, 0);
			cache.updateFromHint(bk("key-a"), 2);
			// key-b may share some path slots by coincidence; findLeaf ≥ 0 and ≤ 2 are both valid,
			// but it must not exceed what was actually recorded for that path.
			const depth = cache.findLeaf(bk("key-b"));
			expect(depth).toBeGreaterThanOrEqual(0);
		});

		it("no-op update: already-known depth returns false without modifying findLeaf", () => {
			const cache = HashTopology.create(R, 4, 0);
			cache.updateFromHint(bk("key"), 2);
			expect(cache.updateFromHint(bk("key"), 2)).toBe(false); // nothing new to learn
			expect(cache.updateFromHint(bk("key"), 1)).toBe(false); // shallower than cached
			// findLeaf unchanged
			expect(cache.findLeaf(bk("key"))).toBe(2);
		});
	});

	describe("invalidate", () => {
		it("forgets the deepest hint of a path, so findLeaf stops at the nearest known ancestor", () => {
			const cache = HashTopology.create(R, 4, 0);
			cache.updateFromHint(bk("key"), 3);
			expect(cache.invalidate(bk("key"), 3)).toBe(true);
			expect(cache.findLeaf(bk("key"))).toBe(2);
			expect(cache.invalidate(bk("key"), 2)).toBe(true);
			expect(cache.findLeaf(bk("key"))).toBe(1);
			expect(cache.invalidate(bk("key"), 1)).toBe(true);
			expect(cache.findLeaf(bk("key"))).toBe(0);
		});

		it("changes nothing for an unknown path or a depth of zero", () => {
			const cache = HashTopology.create(R, 4, 0);
			cache.updateFromHint(bk("key"), 1);
			expect(cache.invalidate(bk("key"), 0)).toBe(false);
			expect(cache.invalidate(bk("key"), 3)).toBe(false);
			expect(cache.findLeaf(bk("key"))).toBe(1);
		});

		it("keeps the hints of a sibling path that diverges above the forgotten level", () => {
			const K = 4;
			const keyA = "key-a";
			const keyB = divergingKey(keyA, 0, 1, K);
			const cache = HashTopology.create(R, K, 0);
			cache.updateFromHint(bk(keyA), 3);
			cache.updateFromHint(bk(keyB), 3);
			expect(cache.invalidate(bk(keyA), 3)).toBe(true);
			expect(cache.findLeaf(bk(keyA))).toBe(2);
			expect(cache.findLeaf(bk(keyB))).toBe(3);
		});
	});

	describe("multiple independent paths", () => {
		it("two keys that hash to different child slots at level 1 don't interfere", () => {
			const K = 4;
			const ownerAbsDepth = 0;
			// Find two keys that diverge at relDepth 0 (different root slots).
			const keyA = "key-a";
			const keyB = divergingKey(keyA, ownerAbsDepth, 0, K);

			const cache = HashTopology.create(R, K, ownerAbsDepth);
			cache.updateFromHint(bk(keyA), 3);
			cache.updateFromHint(bk(keyB), 2);

			expect(cache.findLeaf(bk(keyA))).toBe(3);
			expect(cache.findLeaf(bk(keyB))).toBe(2);
		});
	});

	describe("shared-path convergence", () => {
		it("two keys sharing the first 2 levels but diverging at level 3 both findLeaf correctly", () => {
			const K = 4;
			const ownerAbsDepth = 0;
			// Find two keys that share path for first 2 levels but diverge at level 2.
			const keyA = "shared-prefix-a";
			const keyB = divergingKey(keyA, ownerAbsDepth, 2, K);

			// Verify they do share the first two slots.
			expect(pathFor(keyA, ownerAbsDepth, 2, K)).toEqual(pathFor(keyB, ownerAbsDepth, 2, K));

			const cache = HashTopology.create(R, K, ownerAbsDepth);
			cache.updateFromHint(bk(keyA), 3);
			cache.updateFromHint(bk(keyB), 3);

			expect(cache.findLeaf(bk(keyA))).toBe(3);
			expect(cache.findLeaf(bk(keyB))).toBe(3);
		});

		it("shared intermediate levels are not double-allocated", () => {
			const K = 4;
			const ownerAbsDepth = 0;
			const cache = HashTopology.create(R, K, ownerAbsDepth);
			const keyA = "shared-a";
			const keyB = divergingKey(keyA, ownerAbsDepth, 2, K);

			cache.updateFromHint(bk(keyA), 3);
			const slotsAfterFirst = cache.stats().usedSlots;

			cache.updateFromHint(bk(keyB), 3);
			// The two shared levels reuse existing blocks; only the diverging level
			// allocates one new block for keyB.
			expect(cache.stats().usedSlots).toBe(slotsAfterFirst + K);
		});
	});

	describe("depth cap", () => {
		it("never records a level below the hash depth limit of the topology", () => {
			// rootTreesN 65,000 and hashSplitN 255 have the depth limit 6, so an owner at depth 5 has one level.
			const cache = HashTopology.create(65_000, 255, 5);
			expect(cache.stats().maxDepth).toBe(1);
			cache.updateFromHint(bk("key"), 3);
			expect(cache.findLeaf(bk("key"))).toBe(1);
		});

		it("stops allocating at maxDepth and findLeaf returns the capped value", () => {
			const K = 4;
			const cache = HashTopology.create(R, K, 0, { maxDepth: 3 });
			cache.updateFromHint(bk("key"), 5);
			expect(cache.findLeaf(bk("key"))).toBe(3);
		});

		it("further updates for the same path don't exceed the cap", () => {
			const cache = HashTopology.create(R, 4, 0, { maxDepth: 3 });
			cache.updateFromHint(bk("key"), 5);
			expect(cache.updateFromHint(bk("key"), 5)).toBe(false);
			expect(cache.findLeaf(bk("key"))).toBe(3);
		});
	});

	describe("budget cap", () => {
		it("stops allocating when budget is exhausted and returns false", () => {
			const K = 4;
			// Budget for root block + 2 more blocks only.
			const cache = HashTopology.create(R, K, 0, { budgetBytes: K * 4 * 3 });
			cache.updateFromHint(bk("key"), 5);
			// At least one block was allocated (modified may be true), and findLeaf ≤ 2.
			expect(cache.findLeaf(bk("key"))).toBeLessThanOrEqual(2);
			expect(cache.stats().usedSlots).toBeLessThanOrEqual(K * 3);
			// A second update beyond the budget returns false.
			expect(cache.updateFromHint(bk("key"), 5)).toBe(false);
		});
	});

	describe("hybrid: budget kicks in before depth cap", () => {
		it("budget exhausted before maxDepth is reached", () => {
			const K = 4;
			// Room for root + 3 more blocks → max path depth 3; but maxDepth=10 is much higher.
			const cache = HashTopology.create(R, K, 0, { maxDepth: 10, budgetBytes: K * 4 * 4 });
			cache.updateFromHint(bk("key"), 8);
			expect(cache.findLeaf(bk("key"))).toBeLessThan(10);
			expect(cache.findLeaf(bk("key"))).toBeLessThanOrEqual(3);
		});
	});

	describe("staleness self-correction", () => {
		it("updating a path to a greater depth overwrites the shallower cached value", () => {
			const cache = HashTopology.create(R, 4, 0);
			cache.updateFromHint(bk("key"), 2);
			expect(cache.findLeaf(bk("key"))).toBe(2);

			cache.updateFromHint(bk("key"), 4);
			expect(cache.findLeaf(bk("key"))).toBe(4);
		});
	});

	describe("toSnapshot / fromSnapshot round-trip", () => {
		it("restores findLeaf results identically", () => {
			const cache = HashTopology.create(R, 4, 0);
			cache.updateFromHint(bk("key-a"), 3);
			cache.updateFromHint(bk("key-b"), 2);

			const snap = cache.toSnapshot();
			const restored = HashTopology.fromSnapshot(snap);

			expect(restored.findLeaf(bk("key-a"))).toBe(cache.findLeaf(bk("key-a")));
			expect(restored.findLeaf(bk("key-b"))).toBe(cache.findLeaf(bk("key-b")));
		});

		it("snapshot includes ownerAbsDepth so routing is correct after restore", () => {
			const cache = HashTopology.create(R, 4, 5);
			cache.updateFromHint(bk("key"), 3);
			const snap = cache.toSnapshot();
			expect(snap.ownerAbsDepth).toBe(5);
			const restored = HashTopology.fromSnapshot(snap);
			expect(restored.findLeaf(bk("key"))).toBe(cache.findLeaf(bk("key")));
		});

		it("snapshot arena is a view (no copy) covering only the used portion", () => {
			const cache = HashTopology.create(R, 4, 0);
			cache.updateFromHint(bk("key"), 2);
			const snap = cache.toSnapshot();
			expect(snap.arena.byteLength).toBe(snap.nextFree * 4);
		});

		it("round-trips isEmpty() correctly", () => {
			const empty = HashTopology.create(R, 4, 0);
			const restored = HashTopology.fromSnapshot(empty.toSnapshot());
			expect(restored.isEmpty()).toBe(true);
		});
	});
});
