// Model-based property for transactWriteItems and transactGetItems, mixed with the single-item
// operations. The model and the commands live in model.ts. A write transaction must either commit
// every operation or apply none of them, and the model advances only on a commit, so any partial
// write shows up as a mismatch on a later read.
//
// A failure prints `seed`, `path` and `replayPath`. See item-crud.test.ts for how to replay them.
import fc from "fast-check";
import { describe, it } from "vitest";
import { arbItemData, arbPoolKey, makeTestDB, POOL_KEYS, propertyRuns } from "./arbitraries.js";
import { commandArbitraries, expectModelMatches, seedPool, type Model } from "./model.js";

const PROPERTY_TIMEOUT_MS = 180_000;

describe("FokosDB transactions — model-based property", () => {
	it("any sequence of writes, transactions and reads agrees with an in-memory map", { timeout: PROPERTY_TIMEOUT_MS }, async () => {
		const arbCommands = fc.commands(commandArbitraries(arbPoolKey, arbItemData), { maxCommands: 20 });

		await fc.assert(
			fc.asyncProperty(arbCommands, async (cmds) => {
				const model: Model = { items: new Map() };
				const db = makeTestDB();
				await seedPool(db, model, POOL_KEYS);
				await fc.asyncModelRun(() => ({ model, real: db }), cmds);
				// The final state must agree on every key the run could have touched, so a divergence
				// that no read command observed still fails the run.
				await expectModelMatches(db, model, POOL_KEYS);
			}),
			{ numRuns: propertyRuns(25) },
		);
	});
});
