import { describe, expect, it } from "vitest";
import { AddResult } from "../bloom-filter.js";
import { KeyCodec } from "./key-codec.js";
import { PartialRangeTopology } from "./partial-range-topology.js";

const kb = (s: string) => KeyCodec.encode(s);

function createSmall() {
	return PartialRangeTopology.create({ maxSizeBytes: 1024 * 1024, errorRate: 0.01, initialCapacityN: 100 });
}

describe("PartialRangeTopology", () => {
	describe("create and isEmpty", () => {
		it("isEmpty() is true after create", () => {
			expect(createSmall().isEmpty()).toBe(true);
		});

		it("isEmpty() is false after adding a key", () => {
			const prt = createSmall();
			prt.learnPromotedKey(kb("user-123"));
			expect(prt.isEmpty()).toBe(false);
		});
	});

	describe("maybePromoted", () => {
		it("returns false for unknown keys", () => {
			const prt = createSmall();
			expect(prt.maybePromoted(kb("unknown"))).toBe(false);
		});

		it("returns true for learned keys", () => {
			const prt = createSmall();
			prt.learnPromotedKey(kb("user-123"));
			expect(prt.maybePromoted(kb("user-123"))).toBe(true);
		});

		it("returns true for all learned keys in a batch", () => {
			const prt = createSmall();
			const keys = ["a", "b", "c", "d"];
			prt.learnPromotedKeys(keys.map(kb));
			for (const key of keys) {
				expect(prt.maybePromoted(kb(key))).toBe(true);
			}
		});
	});

	describe("learnPromotedKey", () => {
		it("returns Added when key is new", () => {
			const prt = createSmall();
			expect(prt.learnPromotedKey(kb("key1"))).toBe(AddResult.Added);
		});

		it("returns AlreadyPresent when key was already added", () => {
			const prt = createSmall();
			prt.learnPromotedKey(kb("key1"));
			expect(prt.learnPromotedKey(kb("key1"))).toBe(AddResult.AlreadyPresent);
		});
	});

	describe("learnPromotedKeys", () => {
		it("returns true when at least one key is new", () => {
			const prt = createSmall();
			expect(prt.learnPromotedKeys(["a", "b"].map(kb))).toBe(true);
		});

		it("returns false when all keys are already present", () => {
			const prt = createSmall();
			prt.learnPromotedKey(kb("a"));
			expect(prt.learnPromotedKeys(["a"].map(kb))).toBe(false);
		});

		it("returns true when mix of new and existing keys", () => {
			const prt = createSmall();
			prt.learnPromotedKey(kb("a"));
			expect(prt.learnPromotedKeys(["a", "b"].map(kb))).toBe(true);
		});

		it("handles empty iterable", () => {
			const prt = createSmall();
			expect(prt.learnPromotedKeys([])).toBe(false);
		});
	});

	describe("snapshot round-trip", () => {
		it("preserves learned keys across snapshot/restore", () => {
			const prt = createSmall();
			const keys = ["user-1", "user-2", "user-3"];
			prt.learnPromotedKeys(keys.map(kb));

			const snapshot = prt.toSnapshot();
			expect(snapshot.version).toBe(1);

			const restored = PartialRangeTopology.fromSnapshot(snapshot);
			for (const key of keys) {
				expect(restored.maybePromoted(kb(key))).toBe(true);
			}
			expect(restored.maybePromoted(kb("never-added"))).toBe(false);
		});

		it("preserves isEmpty state", () => {
			const empty = createSmall();
			const restoredEmpty = PartialRangeTopology.fromSnapshot(empty.toSnapshot());
			expect(restoredEmpty.isEmpty()).toBe(true);

			empty.learnPromotedKey(kb("k"));
			const restoredNonEmpty = PartialRangeTopology.fromSnapshot(empty.toSnapshot());
			expect(restoredNonEmpty.isEmpty()).toBe(false);
		});
	});

	describe("stats", () => {
		it("reports zero keys initially", () => {
			const prt = createSmall();
			const s = prt.stats();
			expect(s.bloomAdditionsCount).toBe(0);
			expect(s.bloomMaxSizeBytes).toBe(1024 * 1024);
		});

		it("reports correct key count after adds", () => {
			const prt = createSmall();
			prt.learnPromotedKey(kb("a"));
			prt.learnPromotedKey(kb("b"));
			expect(prt.stats().bloomAdditionsCount).toBe(2);
		});
	});

	describe("full bloom filter", () => {
		it("learnPromotedKey returns Full when bloom filter is full", () => {
			const prt = PartialRangeTopology.create({
				maxSizeBytes: 256,
				errorRate: 0.01,
				initialCapacityN: 10,
			});
			let added = 0;
			let lastResult: AddResult = AddResult.Added;
			for (let i = 0; i < 10_000 && lastResult !== AddResult.Full; i++) {
				lastResult = prt.learnPromotedKey(kb(`key-${i}`));
				if (lastResult !== AddResult.Full) added++;
			}
			expect(lastResult).toBe(AddResult.Full);
			expect(added).toBeGreaterThan(0);

			// Already-learned keys should still be found.
			expect(prt.maybePromoted(kb("key-0"))).toBe(true);
		});

		it("learnPromotedKeys returns false when bloom filter is full", () => {
			const prt = PartialRangeTopology.create({
				maxSizeBytes: 256,
				errorRate: 0.01,
				initialCapacityN: 10,
			});
			// Fill it up.
			for (let i = 0; i < 10_000; i++) {
				if (prt.learnPromotedKey(kb(`fill-${i}`)) === AddResult.Full) break;
			}
			// Attempting to learn more returns false (no modification).
			expect(prt.learnPromotedKeys(["new-a", "new-b"].map(kb))).toBe(false);
		});
	});
});
