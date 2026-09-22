// Model-based property for transactWriteItems and transactGetItems, mixed with the single-item
// operations. A write transaction must either commit every operation or apply none of them, and the
// model advances only on a commit, so a partial write shows up as a mismatch on a later read.
//
// The model, the commands and the run live in harness.ts, which also says how to replay a failure.
import fc from "fast-check";
import { describe, it } from "vitest";
import { arbItemData, arbRun, commandArbitraries, makeTestDB, propertyRuns, runCommands } from "./harness.js";

const PROPERTY_RUNS = propertyRuns(30);
// 4s roughly per run should be more than enough.
const PROPERTY_TIMEOUT_MS = PROPERTY_RUNS * 4_000;

describe("FokosDB transactions — model-based property", () => {
	it("any sequence of writes, transactions and reads agrees with an in-memory map", { timeout: PROPERTY_TIMEOUT_MS }, async () => {
		// One table serves every run: `arbRun` gives each run a key pool of its own, and `seedPool`
		// re-establishes the state the model expects the pool to start from.
		const db = makeTestDB();
		const arbCommands = arbRun((keys) => commandArbitraries(fc.constantFrom(...keys), arbItemData), { maxCommands: 20 });

		await fc.assert(
			fc.asyncProperty(arbCommands, (run) => runCommands(db, run)),
			{ numRuns: PROPERTY_RUNS },
		);
	});
});
