// Property-based tests for putItem, getItem and deleteItem. They show the two styles that this
// directory uses:
//
// 1. A stateless property: fast-check draws random input, the test runs a short fixed scenario, and
//    it asserts a rule that holds for every input.
// 2. A model-based property: fast-check draws a random sequence of commands and runs it against the
//    real database and an in-memory model at the same time. Each command compares the real answer
//    with the model and then advances the model.
//
// harness.ts holds the model, the commands and the arbitraries, and it says how to replay a failure.
import fc from "fast-check";
import { assert, describe, expect, it } from "vitest";
import {
	arbItemData,
	arbItemKey,
	arbRun,
	DeleteItem,
	emptyModel,
	expectedDataKind,
	GetItem,
	makeTestDB,
	prefixHashKey,
	propertyRuns,
	PutItem,
} from "./harness.js";

// Every property runs the scenario many times against real Durable Objects, and a shrink runs it
// many more times. The default 5 s vitest timeout would hide the counterexample.
const PROPERTY_RUNS = propertyRuns(30);
// 4s roughly per run should be more than enough.
const PROPERTY_TIMEOUT_MS = PROPERTY_RUNS * 4_000;

describe("FokosDB item CRUD — stateless properties", () => {
	it("put then get returns the same key, data, kind and version for any key and data", { timeout: PROPERTY_TIMEOUT_MS }, async () => {
		// One table serves every run. The prefix is drawn outside the arbitrary, so a shrink replay
		// gets a fresh hash key too. The runs delete items, so a new item starts above version 1.
		const db = makeTestDB();
		await fc.assert(
			fc.asyncProperty(arbItemKey, arbItemData, async (rawKey, data) => {
				const key = { ...rawKey, hashKey: prefixHashKey(crypto.randomUUID(), rawKey.hashKey) };

				const put = await db.putItem({ ...key, data });

				const get = await db.getItem(key);
				assert(get.found);
				expect(get.item).toMatchObject({ ...key, kind: expectedDataKind(data), version: put.version });
				// `toMatchObject` matches a SUBSET of an object value, so the data is compared exactly.
				expect(get.item.data).toEqual(data);

				const del = await db.deleteItem(key);
				expect(del.deleted).toBe(true);
				expect(await db.getItem(key)).toMatchObject({ found: false, item: key });
			}),
			{ numRuns: PROPERTY_RUNS },
		);
	});
});

describe("FokosDB item CRUD — model-based property", () => {
	it("any sequence of put, get and delete agrees with an in-memory map", { timeout: PROPERTY_TIMEOUT_MS }, async () => {
		// One table serves every run: `arbRun` gives each run a key pool of its own. A shrunk run
		// replays the same pool, so the deletes below return the pool to the empty start the model
		// assumes — a command over an absent key stays a common case.
		const db = makeTestDB();
		const arbCommands = arbRun(
			(keys) => [
				fc.tuple(fc.constantFrom(...keys), arbItemData).map(([key, data]) => new PutItem(key, data)),
				fc.constantFrom(...keys).map((key) => new GetItem(key)),
				fc.constantFrom(...keys).map((key) => new DeleteItem(key)),
			],
			{ maxCommands: 30 },
		);

		await fc.assert(
			fc.asyncProperty(arbCommands, async (run) => {
				for (const key of run.keys) {
					await db.deleteItem(key);
				}
				const setup = () => ({ model: emptyModel(), real: db });
				await fc.asyncModelRun(setup, run.cmds);
			}),
			{ numRuns: PROPERTY_RUNS },
		);
	});
});
