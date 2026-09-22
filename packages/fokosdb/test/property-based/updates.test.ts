// Model-based property for the `update` operation of transactWriteItems. transactions.test.ts runs
// an update next to a put, a delete and a check, which is the interaction coverage. This suite runs
// updates and reads alone, so one run applies about ten updates instead of one, and a counterexample
// is a sequence of updates short enough to read.
//
// The model applies an update only on a commit, and it predicts every rejection it can evaluate: a
// text or bytes pre-image, and a `set` over an array pre-image.
//
// The model, the commands and the update arbitraries live in harness.ts, which also says how to
// replay a failure.
import fc from "fast-check";
import { describe, it } from "vitest";
import {
	arbItemData,
	arbJsonData,
	arbOptionalCondition,
	arbRun,
	arbUpdateActions,
	GetItem,
	keyId,
	makeTestDB,
	propertyRuns,
	PutItem,
	runCommands,
	TransactGet,
	TransactWrite,
	type ItemData,
	type ItemKey,
	type TxOp,
} from "./harness.js";

const PROPERTY_RUNS = propertyRuns(30);
// 4s roughly per run should be more than enough.
const PROPERTY_TIMEOUT_MS = PROPERTY_RUNS * 4_000;

// Two keys hold a document the updates change, one holds text and one an array, so an inapplicable
// update is as common as an applied one. The last pool key stays absent, which is the pre-image an
// update creates the item from.
const SEED_DATA: readonly ItemData[] = [{ alpha: 1, beta: "two" }, { gamma: null }, "seed-text", [1, "two", true]];

const arbUpdateOp = (keys: fc.Arbitrary<ItemKey>): fc.Arbitrary<TxOp> =>
	fc
		.tuple(keys, arbUpdateActions, arbOptionalCondition)
		.map(([key, actions, expectExists]) => ({ key, operation: "update" as const, actions, expectExists }));

// Most transactions carry one operation, so an applicable update commits and the run checks the
// document it produced. A set of two or three is the atomicity case: one inapplicable operation must
// stop every other one. A transaction rejects two operations on one key, so the keys of a set are unique.
const arbUpdateOps = (keys: fc.Arbitrary<ItemKey>) => {
	const op = arbUpdateOp(keys);
	return fc.oneof(
		{ arbitrary: op.map((one) => [one]), weight: 3 },
		{ arbitrary: fc.uniqueArray(op, { minLength: 2, maxLength: 3, selector: (o) => keyId(o.key) }), weight: 1 },
	);
};
const arbReadKeys = (keys: fc.Arbitrary<ItemKey>) => fc.uniqueArray(keys, { minLength: 1, maxLength: 3, selector: keyId });

// A put is rare and favours a document, because its job here is to change the KIND of a pre-image
// under a later update: an item that a put turns into text stops taking updates.
const arbPutData = fc.oneof({ arbitrary: arbJsonData, weight: 3 }, { arbitrary: arbItemData, weight: 1 });

// One arbitrary draws every command, so the weights decide how dense the updates are.
const arbCommand = (keys: fc.Arbitrary<ItemKey>) =>
	fc.oneof(
		{ arbitrary: arbUpdateOps(keys).map((ops) => new TransactWrite(ops)), weight: 6 },
		{ arbitrary: keys.map((key) => new GetItem(key)), weight: 2 },
		{ arbitrary: arbReadKeys(keys).map((ks) => new TransactGet(ks)), weight: 1 },
		{ arbitrary: fc.tuple(keys, arbPutData).map(([key, data]) => new PutItem(key, data)), weight: 1 },
	);

describe("FokosDB update operations — model-based property", () => {
	it("any sequence of updates agrees with an in-memory document", { timeout: PROPERTY_TIMEOUT_MS }, async () => {
		// One table serves every run: `arbRun` gives each run a key pool of its own, and `seedPool`
		// re-establishes the state the model expects the pool to start from.
		const db = makeTestDB();
		const arbCommands = arbRun((keys) => [arbCommand(fc.constantFrom(...keys))], { maxCommands: 24 });

		await fc.assert(
			fc.asyncProperty(arbCommands, (run) => runCommands(db, run, SEED_DATA)),
			{ numRuns: PROPERTY_RUNS },
		);
	});
});
