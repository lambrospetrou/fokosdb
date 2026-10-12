// The transaction model of transactions.test.ts, with faults. One command at a time runs on a table
// that does not split, and most write transactions meet a fault on one of their participants: the
// call of a single round trip, a prepare, a commit or a cancel fails. The request is lost or the
// answer is lost, one time or until the command removes the fault.
//
// The run is serial, so the model predicts the state exactly. A fault must not change what a
// transaction does: it commits every operation or applies none. After each transaction, the keys it
// touched must agree with the model and must hold no lock, and a call with the same token must give
// the same outcome. The stale-transaction job of a partition, the `tx_recovery` job of the
// coordinator, or that call finish what the fault stopped.
//
// The pool holds only json objects, so every update applies and most transactions reach the commit.
// The suite asserts at the end that each kind of fault met the outcome it exists for.
//
// TODO: Add a fault that stops the coordinator between two steps of a transaction: after it stores
// the transaction, after a part of the prepare answers, and after the commit decision. The test
// controls of the coordinator cannot do this now.
//
// harness.ts holds the faults, the command, the model and the run, and it says how to replay a failure.
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
	arbJsonObject,
	arbRun,
	describeFaultStats,
	faultyCommandArbitraries,
	makeTestDB,
	newTransactionStats,
	placedPoolKeys,
	propertyRuns,
	runCommands,
	useShortRecoveryTimes,
	type Fault,
	type FaultOutcome,
	type ItemData,
} from "./harness.js";

const PROPERTY_RUNS = propertyRuns(40);
// A run takes 1 to 2 s alone, because a fault that stays costs the request budget and the recovery
// after it. This allows for the load of the full suite.
const PROPERTY_TIMEOUT_MS = PROPERTY_RUNS * 8_000;
// The start state of the pool: an object for every seeded key.
const JSON_SEED_DATA: readonly ItemData[] = [{ alpha: 1, beta: "two" }, { gamma: null }, {}];

describe("FokosDB transactions with a fault on one participant — model-based property", () => {
	it("any sequence of writes, faulty transactions and reads agrees with an in-memory map", { timeout: PROPERTY_TIMEOUT_MS }, async () => {
		const db = makeTestDB({ controlled: true });
		await useShortRecoveryTimes(db);
		const stats = newTransactionStats();
		const arbCommands = arbRun(
			(keys) => faultyCommandArbitraries(db, keys, arbJsonObject, stats),
			{ maxCommands: 40, size: "max" },
			(prefix) => placedPoolKeys(db, prefix),
		);

		await fc.assert(
			fc.asyncProperty(arbCommands, (run) => runCommands(db, run, JSON_SEED_DATA)),
			{ numRuns: PROPERTY_RUNS },
		);

		// A property where no fault met the transaction it is for agrees with the model and proves
		// nothing. Each row is a fault and the outcome that shows it did its work, and each row must
		// occur with a lost request and with a lost answer.
		const expected: [op: Fault["op"], untilCleared: boolean, outcome: FaultOutcome][] = [
			["txExecuteSingleShot", false, "failed"],
			["txPrepare", false, "committed"],
			["txPrepare", true, "cancelled"],
			["txCommit", false, "committed"],
			["txCommit", true, "pending"],
			["txCancel", false, "cancelled"],
			["txCancel", true, "cancelled"],
		];
		const summary = describeFaultStats(stats);
		for (const replyLost of [false, true]) {
			for (const [op, untilCleared, outcome] of expected) {
				const seen = stats.faults.filter(
					({ fault, outcome: got }) =>
						fault?.op === op && fault.replyLost === replyLost && fault.untilCleared === untilCleared && got === outcome,
				);
				expect(
					seen.length,
					`no ${op} fault (replyLost=${replyLost}, untilCleared=${untilCleared}) ended ${outcome}\n${summary}`,
				).toBeGreaterThan(0);
			}
		}
	});
});
