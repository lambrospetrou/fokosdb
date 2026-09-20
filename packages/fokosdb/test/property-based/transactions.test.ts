// Model-based property for transactWriteItems and transactGetItems, mixed with the single-item
// operations. A write transaction must either commit every operation or apply none of them, and the
// model advances only on a commit, so a partial write shows up as a mismatch on a later read.
//
// The model, the commands and the run live in harness.ts, which also says how to replay a failure.
import fc from "fast-check";
import { describe, it } from "vitest";
import { arbItemData, arbPoolKey, commandArbitraries, makeTestDB, POOL_KEYS, propertyRuns, runCommands } from "./harness.js";

const PROPERTY_TIMEOUT_MS = 180_000;

describe("FokosDB transactions — model-based property", () => {
	it("any sequence of writes, transactions and reads agrees with an in-memory map", { timeout: PROPERTY_TIMEOUT_MS }, async () => {
		const arbCommands = fc.commands(commandArbitraries(arbPoolKey, arbItemData), { maxCommands: 20 });

		await fc.assert(
			fc.asyncProperty(arbCommands, async (cmds) => {
				// A fresh table per run, so the runs share the one key pool and never meet.
				await runCommands(makeTestDB(), { keys: POOL_KEYS, cmds });
			}),
			{ numRuns: propertyRuns(25) },
		);
	});
});
