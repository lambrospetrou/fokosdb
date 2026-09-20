// The transaction model over a table whose partitions split. The split threshold is small and the
// seed writes far more than it, so every root partition splits, most children split again, and the
// large puts of the property keep new splits in flight while the commands run.
//
// One table serves every run, because a seeded and split table is too slow to rebuild per run.
// Each run works on its own key pool (a unique prefix) and an empty model, and the seeded items are
// verified before and after the property, so a split that drops or duplicates a row is caught.
//
// A partition that splits, or a child that still imports its share, answers 503. The commands
// retry those through `untilAvailable`; every other error fails the run.
import fc from "fast-check";
import { beforeAll, describe, expect, it } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import { arbItemData, makeTestDB, poolKeys, propertyRuns, type ItemData } from "./arbitraries.js";
import { commandArbitraries, expectModelMatches, untilAvailable, type Model } from "./model.js";

const SUITE_TIMEOUT_MS = 600_000;
const HASH_SPLIT_MAX_SIZE_MB = 0.5;
const SEED_ITEMS = 1_000;
const SEED_CONCURRENCY = 20;
const SEED_ITEM_BYTES = 4 * 1024;

const seedKey = (i: number) => ({ hashKey: `seed:${String(i).padStart(4, "0")}` });
const seedData = (i: number) => `${i}:`.padEnd(SEED_ITEM_BYTES, "x");

// Half of the puts of the property carry a large payload, so the table keeps growing past the
// threshold of its children and splits keep happening during the transactions.
const arbLargeOrSmallData: fc.Arbitrary<ItemData> = fc.oneof(arbItemData, fc.constant("y".repeat(2 * SEED_ITEM_BYTES)));

async function seed(db: FokosDB): Promise<void> {
	for (let start = 0; start < SEED_ITEMS; start += SEED_CONCURRENCY) {
		const batch = Array.from({ length: Math.min(SEED_CONCURRENCY, SEED_ITEMS - start) }, (_, j) => start + j);
		await Promise.all(batch.map((i) => untilAvailable(() => db.putItem({ ...seedKey(i), data: seedData(i) }))));
	}
}

/** Reads every seeded item back and returns the deepest hash tree level a read went through. */
async function expectSeedIntact(db: FokosDB): Promise<number> {
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

describe("FokosDB transactions over splitting partitions — model-based property", () => {
	const db = makeTestDB({ hashSplitMaxSizeMb: HASH_SPLIT_MAX_SIZE_MB });

	beforeAll(async () => {
		await seed(db);
		// The seed is several times the threshold of every root partition, so at least one split has
		// started by the time the last write lands: a write that would overflow the partition further
		// is refused until the split makes room.
		expect(await expectSeedIntact(db), "the seed must have split at least one root partition").toBeGreaterThanOrEqual(1);
	}, SUITE_TIMEOUT_MS);

	it(
		"any sequence of writes, transactions and reads agrees with an in-memory map while partitions split",
		{ timeout: SUITE_TIMEOUT_MS },
		async () => {
			// The prefix is drawn as part of the run, so a replay with the same seed uses the same keys.
			const arbRun = fc.uuid().chain((prefix) =>
				fc.record({
					keys: fc.constant(poolKeys(`${prefix}:`)),
					cmds: fc.commands(commandArbitraries(fc.constantFrom(...poolKeys(`${prefix}:`)), arbLargeOrSmallData), { maxCommands: 20 }),
				}),
			);

			await fc.assert(
				fc.asyncProperty(arbRun, async ({ keys, cmds }) => {
					const model: Model = { items: new Map() };
					await fc.asyncModelRun(() => ({ model, real: db }), cmds);
					await expectModelMatches(db, model, keys);
				}),
				{ numRuns: propertyRuns(25) },
			);

			expect(await expectSeedIntact(db)).toBeGreaterThanOrEqual(1);
		},
	);
});
