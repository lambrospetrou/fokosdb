// The transaction model with several transactions in flight at one time, over a table whose
// partitions split. Two properties run here and share the seeded table: one over disjoint key sets,
// where the model predicts every outcome, and one over the same keys, where the oracle is an order
// of the transactions that committed. contention-model.ts holds the second one.
//
// transactions-split.test.ts sends one operation per command and waits for it, so the table never
// meets two transactions at once and nothing that 2PC exists for is under load. Here a command
// sends a batch of 2 to 4 transactions together. Their key sets are disjoint, so the model stays
// exact, and the batch still makes several coordinators drive one table at once, puts several
// transactions on one partition, and makes many coordinators race the same split.
//
// Each batch also probes the keys it touched: once the batch has drained, a non-transactional write
// to each of them must land. A write to a locked item is refused, so a lock that a prepare took and
// no commit or cancel gave back fails the run instead of staying invisible.
//
// The counts of the run are asserted at the end. Under contention a suite whose every transaction
// cancels agrees with the model and proves nothing.
//
// A failure prints `seed`, `path` and `replayPath`. See item-crud.test.ts for how to replay them.
import fc from "fast-check";
import { beforeAll, describe, expect, it } from "vitest";
import { makeTestDB, poolKeys, propertyRuns } from "./arbitraries.js";
import { arbContendingBatch } from "./contention-model.js";
import { arbLargeOrSmallData, expectSeedIntact, HASH_SPLIT_MAX_SIZE_MB, seedSplitTable } from "./hash-split-fixture.js";
import {
	arbConcurrentBatch,
	commandArbitraries,
	describeTransactionStats,
	expectModelMatches,
	newTransactionStats,
	seedPool,
	type Model,
} from "./model.js";

const SUITE_TIMEOUT_MS = 600_000;
// A batch is heavier than a single command, so a run holds fewer of them.
const MAX_COMMANDS = 12;
// A batch is what the suite is for, so it must be the common command and not one of six.
const BATCH_WEIGHT = 5;
// Most transactions of a run carry no condition, so most of them must commit. The bound is far
// under that and still fails a run whose transactions nearly all cancel.
const MIN_COMMITTED_FRACTION = 0.25;
// Every transaction of a contending batch operates on one shared key, so the lock of that key
// serializes them and a contender that meets it cancels. Far fewer of them commit than above, and
// the bound still fails a run whose transactions nearly all cancel.
const MIN_CONTENDED_COMMITTED_FRACTION = 0.1;
// A contending batch does more per command: it reads the pool back, replays one transaction and
// probes every key it touched.
const MAX_CONTENDING_COMMANDS = 8;

describe("FokosDB concurrent transactions over splitting partitions — model-based property", () => {
	const db = makeTestDB({ hashSplitMaxSizeMb: HASH_SPLIT_MAX_SIZE_MB });

	beforeAll(async () => {
		await seedSplitTable(db);
		expect(await expectSeedIntact(db), "the seed must have split at least one root partition").toBeGreaterThanOrEqual(1);
	}, SUITE_TIMEOUT_MS);

	it("batches of concurrent transactions agree with an in-memory map and leave no lock behind", { timeout: SUITE_TIMEOUT_MS }, async () => {
		const stats = newTransactionStats();
		// The prefix is drawn as part of the run, so a replay with the same seed uses the same keys.
		const arbRun = fc.uuid().chain((prefix) => {
			const keys = poolKeys(`${prefix}:`);
			const arbCommand = fc.oneof(
				{ arbitrary: arbConcurrentBatch(keys, arbLargeOrSmallData, stats), weight: BATCH_WEIGHT },
				// The single-operation commands run between the batches. They move the items to states
				// a batch alone reaches rarely, such as an absent key or a text item that no update applies to.
				...commandArbitraries(fc.constantFrom(...keys), arbLargeOrSmallData).map((arbitrary) => ({ arbitrary, weight: 1 })),
			);
			// `size: "max"` keeps a run at its full length. A shorter run reaches fewer of the item
			// states that a batch must meet, and a lock that one batch leaks shows up in the next one.
			return fc.record({ keys: fc.constant(keys), cmds: fc.commands([arbCommand], { maxCommands: MAX_COMMANDS, size: "max" }) });
		});

		await fc.assert(
			fc.asyncProperty(arbRun, async ({ keys, cmds }) => {
				const model: Model = { items: new Map() };
				await seedPool(db, model, keys);
				await fc.asyncModelRun(() => ({ model, real: db }), cmds);
				await expectModelMatches(db, model, keys);
			}),
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
		const arbRun = fc.uuid().chain((prefix) => {
			const keys = poolKeys(`${prefix}:`);
			const arbCommand = fc.oneof(
				{ arbitrary: arbContendingBatch(keys, arbLargeOrSmallData, stats), weight: BATCH_WEIGHT },
				// The single-operation commands run between the batches and move the items to states a
				// batch alone reaches rarely, such as an absent key or a text item that no update applies to.
				...commandArbitraries(fc.constantFrom(...keys), arbLargeOrSmallData).map((arbitrary) => ({ arbitrary, weight: 1 })),
			);
			return fc.record({ keys: fc.constant(keys), cmds: fc.commands([arbCommand], { maxCommands: MAX_CONTENDING_COMMANDS, size: "max" }) });
		});

		await fc.assert(
			fc.asyncProperty(arbRun, async ({ keys, cmds }) => {
				const model: Model = { items: new Map() };
				await seedPool(db, model, keys);
				await fc.asyncModelRun(() => ({ model, real: db }), cmds);
				await expectModelMatches(db, model, keys);
			}),
			{ numRuns: propertyRuns(10) },
		);

		const summary = describeTransactionStats(stats);
		expect(stats.peakInFlight, `the transactions never overlapped — ${summary}`).toBeGreaterThanOrEqual(2);
		// The suite exists for the transactions that meet on a locked item. A run that never met one
		// tested the disjoint case again under another name.
		expect(stats.rejections.get("pending_conflict") ?? 0, `no transaction met the lock of another — ${summary}`).toBeGreaterThan(0);
		expect(stats.committed, `too few transactions committed — ${summary}`).toBeGreaterThanOrEqual(
			stats.started * MIN_CONTENDED_COMMITTED_FRACTION,
		);
		expect(stats.lockProbes, `no batch probed the locks it released — ${summary}`).toBeGreaterThan(0);
		expect(await expectSeedIntact(db)).toBeGreaterThanOrEqual(1);
	});
});
