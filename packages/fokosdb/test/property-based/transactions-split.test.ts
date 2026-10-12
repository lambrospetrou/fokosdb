// The transaction model over a table whose partitions split. Each command of a run sends one
// operation and waits for it, so the table meets one transaction at a time and the model stays
// exact. transactions-concurrent.test.ts sends batches of transactions instead.
//
// A partition that splits, and a child that still imports its share, answer 503. The commands retry
// those; every other error fails the run.
//
// The seeded table, the payload arbitrary that keeps new splits in flight, the model and the run all
// live in harness.ts, which also says how to replay a failure.
import fc from "fast-check";
import { beforeAll, describe, expect, it } from "vitest";
import {
	arbLargeOrSmallData,
	arbRun,
	commandArbitraries,
	expectSeedIntact,
	HASH_SPLIT_MAX_SIZE_MB,
	makeTestDB,
	propertyRuns,
	runCommands,
	seedSplitTable,
} from "./harness.js";

// The seed of the table does not depend on the run count.
const SEED_TIMEOUT_MS = 600_000;
const PROPERTY_RUNS = propertyRuns(25);
// A run takes about 0.2 s alone. This allows for the load of the full suite and for a run that
// waits for a partition that splits.
const PROPERTY_TIMEOUT_MS = PROPERTY_RUNS * 5_000;

describe("FokosDB transactions over splitting partitions — model-based property", () => {
	const db = makeTestDB({ hashSplitMaxSizeMb: HASH_SPLIT_MAX_SIZE_MB });

	beforeAll(async () => {
		await seedSplitTable(db);
		// The seed is several times the threshold of every root partition, so at least one split has
		// started by the time the last write lands: the partition refuses a write that would overflow
		// it further until the split makes room.
		expect(await expectSeedIntact(db), "the seed must have split at least one root partition").toBeGreaterThanOrEqual(1);
	}, SEED_TIMEOUT_MS);

	it(
		"any sequence of writes, transactions and reads agrees with an in-memory map while partitions split",
		{ timeout: PROPERTY_TIMEOUT_MS },
		async () => {
			const arbRunOverTheTable = arbRun((keys) => commandArbitraries(fc.constantFrom(...keys), arbLargeOrSmallData), { maxCommands: 20 });

			await fc.assert(
				fc.asyncProperty(arbRunOverTheTable, (run) => runCommands(db, run)),
				{ numRuns: PROPERTY_RUNS },
			);

			expect(await expectSeedIntact(db)).toBeGreaterThanOrEqual(1);
		},
	);
});
