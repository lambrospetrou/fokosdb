// The transaction model over a table whose partitions split. Every command of a run sends one
// operation and waits for it, so the table meets one transaction at a time and the model stays
// exact. transactions-concurrent.test.ts sends a batch of transactions at once.
//
// The seeded and split table, and the payload arbitrary that keeps new splits in flight, are in
// hash-split-fixture.ts.
//
// A partition that splits, or a child that still imports its share, answers 503. The commands
// retry those through `untilAvailable`; every other error fails the run.
import fc from "fast-check";
import { beforeAll, describe, expect, it } from "vitest";
import { makeTestDB, poolKeys, propertyRuns } from "./arbitraries.js";
import { arbLargeOrSmallData, expectSeedIntact, HASH_SPLIT_MAX_SIZE_MB, seedSplitTable } from "./hash-split-fixture.js";
import { commandArbitraries, expectModelMatches, seedPool, type Model } from "./model.js";

const SUITE_TIMEOUT_MS = 600_000;

describe("FokosDB transactions over splitting partitions — model-based property", () => {
	const db = makeTestDB({ hashSplitMaxSizeMb: HASH_SPLIT_MAX_SIZE_MB });

	beforeAll(async () => {
		await seedSplitTable(db);
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
					await seedPool(db, model, keys);
					await fc.asyncModelRun(() => ({ model, real: db }), cmds);
					await expectModelMatches(db, model, keys);
				}),
				{ numRuns: propertyRuns(25) },
			);

			expect(await expectSeedIntact(db)).toBeGreaterThanOrEqual(1);
		},
	);
});
