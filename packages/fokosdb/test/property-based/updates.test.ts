// Model-based property for the `update` operation of transactWriteItems. The mixed transaction suite
// runs an update next to a put, a delete and a check, which is the interaction coverage. This suite
// runs updates and reads alone, so one run applies about ten updates instead of one, and a
// counterexample is a sequence of updates that is short enough to read.
//
// The model, the commands and the update arbitraries live in model.ts. An update is applied to the
// model only on a commit, and the model predicts every rejection it can evaluate: a text or bytes
// pre-image, and a `set` over an array pre-image.
//
// A failure prints `seed`, `path` and `replayPath`. See item-crud.test.ts for how to replay them.
import fc from "fast-check";
import { describe, it } from "vitest";
import { arbJsonData, arbItemData, arbPoolKey, keyId, makeTestDB, POOL_KEYS, propertyRuns, type ItemData } from "./arbitraries.js";
import {
	arbOptionalCondition,
	arbUpdateActions,
	expectModelMatches,
	GetItem,
	PutItem,
	seedPool,
	TransactGet,
	TransactWrite,
	type Model,
	type TxOp,
} from "./model.js";

const PROPERTY_TIMEOUT_MS = 180_000;

// Two keys hold a document the updates change, one holds text and one an array, so an inapplicable
// update is as common as an applied one. The last pool key stays absent, which is the pre-image an
// update creates the item from.
const SEED_DATA: readonly ItemData[] = [{ alpha: 1, beta: "two" }, { gamma: null }, "seed-text", [1, "two", true]];

const arbUpdateOp: fc.Arbitrary<TxOp> = fc
	.tuple(arbPoolKey, arbUpdateActions, arbOptionalCondition)
	.map(([key, actions, expectExists]) => ({ key, operation: "update" as const, actions, expectExists }));

// Most transactions carry one operation, so an applicable update commits and the run checks the
// document it produced. A set of two or three is the atomicity case: one inapplicable operation must
// stop every other one. A transaction rejects two operations on one key, so the keys of a set are unique.
const arbUpdateOps = fc.oneof(
	{ arbitrary: arbUpdateOp.map((op) => [op]), weight: 3 },
	{ arbitrary: fc.uniqueArray(arbUpdateOp, { minLength: 2, maxLength: 3, selector: (op) => keyId(op.key) }), weight: 1 },
);
const arbReadKeys = fc.uniqueArray(arbPoolKey, { minLength: 1, maxLength: 3, selector: keyId });

// A put is rare and favours a document, because its job here is to change the KIND of a pre-image
// under a later update — an item that a put turns into text stops accepting updates.
const arbPutData = fc.oneof({ arbitrary: arbJsonData, weight: 3 }, { arbitrary: arbItemData, weight: 1 });

// One arbitrary draws every command, so the weights decide how dense the updates are.
const arbCommand = fc.oneof(
	{ arbitrary: arbUpdateOps.map((ops) => new TransactWrite(ops)), weight: 6 },
	{ arbitrary: arbPoolKey.map((key) => new GetItem(key)), weight: 2 },
	{ arbitrary: arbReadKeys.map((keys) => new TransactGet(keys)), weight: 1 },
	{ arbitrary: fc.tuple(arbPoolKey, arbPutData).map(([key, data]) => new PutItem(key, data)), weight: 1 },
);

describe("FokosDB update operations — model-based property", () => {
	it("any sequence of updates agrees with an in-memory document", { timeout: PROPERTY_TIMEOUT_MS }, async () => {
		const arbCommands = fc.commands([arbCommand], { maxCommands: 24 });

		await fc.assert(
			fc.asyncProperty(arbCommands, async (cmds) => {
				const model: Model = { items: new Map() };
				const db = makeTestDB();
				await seedPool(db, model, POOL_KEYS, SEED_DATA);
				await fc.asyncModelRun(() => ({ model, real: db }), cmds);
				// The final state must agree on every key the run could have touched, so a divergence that
				// no read command observed still fails the run.
				await expectModelMatches(db, model, POOL_KEYS);
			}),
			{ numRuns: propertyRuns(25) },
		);
	});
});
