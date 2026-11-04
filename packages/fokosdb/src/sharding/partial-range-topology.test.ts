import { describe, expect, it } from "vitest";
import { AddResult } from "../shared/bloom-filter.js";
import { routingHashOf } from "./hash-primitives.js";
import { KeyCodec } from "./key-codec.js";
import { PartialRangeTopology } from "./partial-range-topology.js";

/** The routingHash of a hash key in one test shard group. */
const kb = (s: string) => routingHashOf(KeyCodec.encode(s), "fokos.p.bloom-test");

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
			prt.learnPromotedRoutingHash(kb("user-123"));
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
			prt.learnPromotedRoutingHash(kb("user-123"));
			expect(prt.maybePromoted(kb("user-123"))).toBe(true);
		});

		it("returns true for all learned keys in a batch", () => {
			const prt = createSmall();
			const keys = ["a", "b", "c", "d"];
			for (const key of keys) {
				prt.learnPromotedRoutingHash(kb(key));
			}
			for (const key of keys) {
				expect(prt.maybePromoted(kb(key))).toBe(true);
			}
		});
	});

	describe("learnPromotedRoutingHash", () => {
		it("returns Added when key is new", () => {
			const prt = createSmall();
			expect(prt.learnPromotedRoutingHash(kb("key1"))).toBe(AddResult.Added);
		});

		it("returns AlreadyPresent when key was already added", () => {
			const prt = createSmall();
			prt.learnPromotedRoutingHash(kb("key1"));
			expect(prt.learnPromotedRoutingHash(kb("key1"))).toBe(AddResult.AlreadyPresent);
		});
	});

	describe("snapshot round-trip", () => {
		it("preserves learned keys across snapshot/restore", () => {
			const prt = createSmall();
			const keys = ["user-1", "user-2", "user-3"];
			for (const key of keys) {
				prt.learnPromotedRoutingHash(kb(key));
			}

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

			empty.learnPromotedRoutingHash(kb("k"));
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
			prt.learnPromotedRoutingHash(kb("a"));
			prt.learnPromotedRoutingHash(kb("b"));
			expect(prt.stats().bloomAdditionsCount).toBe(2);
		});
	});

	describe("full bloom filter", () => {
		it("learnPromotedRoutingHash returns Full when bloom filter is full", () => {
			const prt = PartialRangeTopology.create({
				maxSizeBytes: 256,
				errorRate: 0.01,
				initialCapacityN: 10,
			});
			let added = 0;
			let lastResult: AddResult = AddResult.Added;
			for (let i = 0; i < 10_000 && lastResult !== AddResult.Full; i++) {
				lastResult = prt.learnPromotedRoutingHash(kb(`key-${i}`));
				if (lastResult !== AddResult.Full) {
					added++;
				}
			}
			expect(lastResult).toBe(AddResult.Full);
			expect(added).toBeGreaterThan(0);

			// Already-learned keys should still be found.
			expect(prt.maybePromoted(kb("key-0"))).toBe(true);
		});
	});
});
