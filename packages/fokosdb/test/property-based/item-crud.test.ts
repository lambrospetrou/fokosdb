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
	arbPoolKey,
	DeleteItem,
	expectedDataKind,
	GetItem,
	makeTestDB,
	propertyRuns,
	PutItem,
	type Model,
} from "./harness.js";

// Every property runs the scenario many times against real Durable Objects, and a shrink runs it
// many more times. The default 5 s vitest timeout would hide the counterexample.
const PROPERTY_TIMEOUT_MS = 120_000;

describe("FokosDB item CRUD — stateless properties", () => {
	it("put then get returns the same key, data, kind and version for any key and data", { timeout: PROPERTY_TIMEOUT_MS }, async () => {
		await fc.assert(
			fc.asyncProperty(arbItemKey, arbItemData, async (key, data) => {
				const db = makeTestDB();

				const put = await db.putItem({ ...key, data });
				expect(put.version).toBe(1);

				const get = await db.getItem(key);
				assert(get.found);
				expect(get.item).toMatchObject({ ...key, kind: expectedDataKind(data), version: 1 });
				// `toMatchObject` matches a SUBSET of an object value, so the data is compared exactly.
				expect(get.item.data).toEqual(data);

				const del = await db.deleteItem(key);
				expect(del.deleted).toBe(true);
				expect(await db.getItem(key)).toMatchObject({ found: false, item: key });
			}),
			{ numRuns: propertyRuns(30) },
		);
	});
});

describe("FokosDB item CRUD — model-based property", () => {
	it("any sequence of put, get and delete agrees with an in-memory map", { timeout: PROPERTY_TIMEOUT_MS }, async () => {
		const arbCommands = fc.commands(
			[
				fc.tuple(arbPoolKey, arbItemData).map(([key, data]) => new PutItem(key, data)),
				arbPoolKey.map((key) => new GetItem(key)),
				arbPoolKey.map((key) => new DeleteItem(key)),
			],
			{ maxCommands: 30 },
		);

		await fc.assert(
			fc.asyncProperty(arbCommands, async (cmds) => {
				// A fresh table and a fresh model per run, so the runs never see the items of each other.
				// The pool starts empty here, so a command over an absent key is a common case.
				const setup = () => ({ model: { items: new Map() } as Model, real: makeTestDB() });
				await fc.asyncModelRun(setup, cmds);
			}),
			{ numRuns: propertyRuns(30) },
		);
	});
});
