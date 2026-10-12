// Model-based property for transactWriteItems and transactGetItems, mixed with the single-item
// operations. A write transaction must either commit every operation or apply none of them, and the
// model advances only on a commit, so a partial write shows up as a mismatch on a later read.
//
// The key pool of a run has two keys on each of the three root partitions, and the table does not
// split, so the run knows the participants of each transaction. A transaction draws how many
// partitions it touches, and a write transaction draws whether it carries a token, which decides
// between the single round trip to one partition and the coordinator. The suite asserts at the end
// that transactions committed and cancelled on each of those paths.
//
// Two properties run. The first one mixes text, bytes and json items, so many of its updates meet an
// item they do not apply to, and the transaction cancels. The second one holds only json objects, so
// every update applies and a transaction cancels only on a condition.
//
// The model, the commands and the run live in harness.ts, which also says how to replay a failure.
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
	arbItemData,
	arbJsonObject,
	arbRun,
	describeTransactionStats,
	makeTestDB,
	newTransactionStats,
	placedCommandArbitraries,
	placedPoolKeys,
	propertyRuns,
	runCommands,
	TRANSACTION_PATHS,
	type ItemData,
	type TransactionStats,
} from "./harness.js";

const PROPERTY_RUNS = propertyRuns(30);
// 4s roughly per run should be more than enough.
const PROPERTY_TIMEOUT_MS = PROPERTY_RUNS * 4_000;

// The start state of the pool in the json-only property: an object for every seeded key.
const JSON_SEED_DATA: readonly ItemData[] = [{ alpha: 1, beta: "two" }, { gamma: null }, {}];

/**
 * Runs the property with `data` as the payload of every put, and returns the counters of its
 * transactions. `seedData` is the start state of the pool, and the default mixes the item kinds.
 */
async function runProperty(data: fc.Arbitrary<ItemData>, seedData?: readonly ItemData[]): Promise<TransactionStats> {
	// One table serves every run: `arbRun` gives each run a key pool of its own, and `seedPool`
	// re-establishes the state the model expects the pool to start from.
	const db = makeTestDB();
	const stats = newTransactionStats();
	const arbCommands = arbRun(
		(keys) => placedCommandArbitraries(db, keys, data, stats),
		// `size: "max"` keeps the runs long. The default size gives runs of a few commands, and the 30
		// runs together then hold too few transactions for the path counters below.
		{ maxCommands: 40, size: "max" },
		(prefix) => placedPoolKeys(db, prefix),
	);

	await fc.assert(
		fc.asyncProperty(arbCommands, (run) => runCommands(db, run, seedData)),
		{ numRuns: PROPERTY_RUNS },
	);

	const summary = describeTransactionStats(stats);
	for (const path of TRANSACTION_PATHS) {
		expect(stats.paths.get(path)?.committed ?? 0, `no transaction committed on the path "${path}" — ${summary}`).toBeGreaterThan(0);
		expect(stats.paths.get(path)?.cancelled ?? 0, `no transaction cancelled on the path "${path}" — ${summary}`).toBeGreaterThan(0);
	}
	return stats;
}

describe("FokosDB transactions — model-based property", () => {
	it("any sequence of writes, transactions and reads agrees with an in-memory map", { timeout: PROPERTY_TIMEOUT_MS }, async () => {
		await runProperty(arbItemData);
	});
});

describe("FokosDB transactions over json objects — model-based property", () => {
	it("any sequence of writes, transactions and reads agrees with an in-memory map", { timeout: PROPERTY_TIMEOUT_MS }, async () => {
		const stats = await runProperty(arbJsonObject, JSON_SEED_DATA);
		// The pool never holds an item that an update does not apply to.
		expect(stats.rejections.get("update_not_applicable"), describeTransactionStats(stats)).toBeUndefined();
	});
});
