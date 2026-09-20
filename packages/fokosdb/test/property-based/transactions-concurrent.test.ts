// Several transactions in flight at one time, over a table whose partitions split. Two properties
// run here and share the seeded table.
//
// transactions-split.test.ts sends one operation per command and waits for it, so the table never
// meets two transactions at once and nothing that 2PC exists for is under load. Here a command
// sends a batch of 2 to 4 transactions together.
//
//   - The first property keeps the key sets of a batch disjoint, so the model predicts every
//     outcome exactly. The batch still makes several coordinators drive one table at once, puts
//     several transactions on one partition, and makes many coordinators race the same split.
//   - The second property sends every transaction of a batch at the same key, so the transactions
//     contend. No model predicts which one wins, and the oracle is an order of the transactions
//     that committed.
//
// Each batch also probes the keys it touched: once the batch has drained, a write that no
// transaction drives must land on each of them. The database refuses a write to a locked item, so
// the probe fails a run over a lock that a prepare took and no commit or cancel gave back.
//
// Both properties assert on the counts of the run at the end. Under contention a suite where every
// transaction cancels agrees with the model and proves nothing.
//
// harness.ts holds the batches, the oracle, the seeded table and the run, and it says how to replay
// a failure.
import fc from "fast-check";
import { beforeAll, describe, expect, it } from "vitest";
import {
	arbConcurrentBatch,
	arbContendingBatch,
	arbLargeOrSmallData,
	arbRun,
	commandArbitraries,
	describeTransactionStats,
	expectSeedIntact,
	HASH_SPLIT_MAX_SIZE_MB,
	makeTestDB,
	newTransactionStats,
	propertyRuns,
	runCommands,
	seedSplitTable,
} from "./harness.js";

const SUITE_TIMEOUT_MS = 600_000;
// A batch is heavier than a single command, so a run holds fewer of them.
const MAX_COMMANDS = 12;
// A contending batch does more per command: it reads the pool back, replays one transaction, and
// probes every key it touched.
const MAX_CONTENDING_COMMANDS = 8;
// A batch is what the suite is for, so it must be the common command and not one of six.
const BATCH_WEIGHT = 5;
// Most transactions of a run carry no condition, so most of them must commit. The bound is far
// under that and still fails a run whose transactions nearly all cancel.
const MIN_COMMITTED_FRACTION = 0.25;
// Every transaction of a contending batch operates on one shared key, so the lock of that key
// serializes them and a contender that meets the lock cancels. Far fewer of them commit than above,
// and the bound still fails a run whose transactions nearly all cancel.
const MIN_CONTENDED_COMMITTED_FRACTION = 0.1;

describe("FokosDB concurrent transactions over splitting partitions — model-based properties", () => {
	const db = makeTestDB({ hashSplitMaxSizeMb: HASH_SPLIT_MAX_SIZE_MB });

	beforeAll(async () => {
		await seedSplitTable(db);
		expect(await expectSeedIntact(db), "the seed must have split at least one root partition").toBeGreaterThanOrEqual(1);
	}, SUITE_TIMEOUT_MS);

	it("batches of concurrent transactions agree with an in-memory map and leave no lock behind", { timeout: SUITE_TIMEOUT_MS }, async () => {
		const stats = newTransactionStats();
		// `size: "max"` keeps a run at its full length. A shorter run reaches fewer of the item states
		// that a batch must meet, and a lock that one batch leaks shows up in the next one.
		const arbRunOverTheTable = arbRun(
			(keys) => [
				fc.oneof(
					{ arbitrary: arbConcurrentBatch(keys, arbLargeOrSmallData, stats), weight: BATCH_WEIGHT },
					// The single-operation commands run between the batches. They move the items to states
					// a batch alone reaches rarely, such as an absent key, or a text item that no update applies to.
					...commandArbitraries(fc.constantFrom(...keys), arbLargeOrSmallData).map((arbitrary) => ({ arbitrary, weight: 1 })),
				),
			],
			{ maxCommands: MAX_COMMANDS, size: "max" },
		);

		await fc.assert(
			fc.asyncProperty(arbRunOverTheTable, (run) => runCommands(db, run)),
			{ numRuns: propertyRuns(15) },
		);

		const summary = describeTransactionStats(stats);
		expect(stats.peakInFlight, `the transactions never overlapped — ${summary}`).toBeGreaterThanOrEqual(2);
		expect(stats.committed, `too few transactions committed — ${summary}`).toBeGreaterThanOrEqual(stats.started * MIN_COMMITTED_FRACTION);
		expect(stats.lockProbes, `no batch probed the locks it released — ${summary}`).toBeGreaterThan(0);
		expect(await expectSeedIntact(db)).toBeGreaterThanOrEqual(1);
	});

	it("batches of contending transactions leave a state that some order of them explains", { timeout: SUITE_TIMEOUT_MS }, async () => {
		const stats = newTransactionStats();
		const arbRunOverTheTable = arbRun(
			(keys) => [
				fc.oneof(
					{ arbitrary: arbContendingBatch(keys, arbLargeOrSmallData, stats), weight: BATCH_WEIGHT },
					...commandArbitraries(fc.constantFrom(...keys), arbLargeOrSmallData).map((arbitrary) => ({ arbitrary, weight: 1 })),
				),
			],
			{ maxCommands: MAX_CONTENDING_COMMANDS, size: "max" },
		);

		await fc.assert(
			fc.asyncProperty(arbRunOverTheTable, (run) => runCommands(db, run)),
			{ numRuns: propertyRuns(10) },
		);

		const summary = describeTransactionStats(stats);
		expect(stats.peakInFlight, `the transactions never overlapped — ${summary}`).toBeGreaterThanOrEqual(2);
		// The property exists for the transactions that meet on a locked item. A run that never met
		// one tested the disjoint case again under another name.
		expect(stats.rejections.get("pending_conflict") ?? 0, `no transaction met the lock of another — ${summary}`).toBeGreaterThan(0);
		expect(stats.committed, `too few transactions committed — ${summary}`).toBeGreaterThanOrEqual(
			stats.started * MIN_CONTENDED_COMMITTED_FRACTION,
		);
		expect(stats.lockProbes, `no batch probed the locks it released — ${summary}`).toBeGreaterThan(0);
		expect(await expectSeedIntact(db)).toBeGreaterThanOrEqual(1);
	});
});
