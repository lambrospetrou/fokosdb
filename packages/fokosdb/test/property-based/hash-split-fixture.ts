// The seeded table that the transaction split suites share. Its split threshold is small and the
// seed writes far more than it, so every root partition splits, most children split again, and the
// large puts of a property keep new splits in flight while the commands run.
//
// One table serves every run, because a seeded and split table is too slow to rebuild per run. Each
// run works on its own key pool (a unique prefix) and an empty model, and the seeded items are read
// back before and after a property, so a split that drops or duplicates a row is caught.
import fc from "fast-check";
import { expect } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import { arbItemData, type ItemData } from "./arbitraries.js";
import { untilAvailable } from "./model.js";

export const HASH_SPLIT_MAX_SIZE_MB = 0.5;
const SEED_ITEMS = 1_000;
const SEED_CONCURRENCY = 20;
const SEED_ITEM_BYTES = 4 * 1024;

const seedKey = (i: number) => ({ hashKey: `seed:${String(i).padStart(4, "0")}` });
const seedData = (i: number) => `${i}:`.padEnd(SEED_ITEM_BYTES, "x");

// Half of the puts of a property carry a large payload, so the table keeps growing past the
// threshold of its children and splits keep happening during the commands.
export const arbLargeOrSmallData: fc.Arbitrary<ItemData> = fc.oneof(arbItemData, fc.constant("y".repeat(2 * SEED_ITEM_BYTES)));

/** Writes the seed. The caller then asserts with `expectSeedIntact` that it split the table. */
export async function seedSplitTable(db: FokosDB): Promise<void> {
	for (let start = 0; start < SEED_ITEMS; start += SEED_CONCURRENCY) {
		const batch = Array.from({ length: Math.min(SEED_CONCURRENCY, SEED_ITEMS - start) }, (_, j) => start + j);
		await Promise.all(batch.map((i) => untilAvailable(() => db.putItem({ ...seedKey(i), data: seedData(i) }))));
	}
}

/** Reads every seeded item back and returns the deepest hash tree level a read went through. */
export async function expectSeedIntact(db: FokosDB): Promise<number> {
	let maxHashDepth = 0;
	for (let start = 0; start < SEED_ITEMS; start += SEED_CONCURRENCY) {
		const batch = Array.from({ length: Math.min(SEED_CONCURRENCY, SEED_ITEMS - start) }, (_, j) => start + j);
		const reads = await Promise.all(batch.map((i) => untilAvailable(() => db.getItem(seedKey(i)))));
		reads.forEach((res, j) => {
			expect(res).toMatchObject({ found: true, item: { data: seedData(batch[j]), version: 1 } });
			maxHashDepth = Math.max(maxHashDepth, res.meta.hashDepth);
		});
	}
	return maxHashDepth;
}
