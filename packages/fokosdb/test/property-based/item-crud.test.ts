// Property-based tests for putItem, getItem and deleteItem. Two styles are shown:
//
// 1. A stateless property: fast-check draws random inputs, the test runs a short fixed scenario
//    and asserts a rule that must hold for every input.
// 2. A model-based property: fast-check draws a random sequence of commands and runs it against
//    the real database and against a simple in-memory model at the same time. Each command
//    compares the real answer with the model and then advances the model.
//
// A failure prints `seed` and `path`. Put them in the `fc.assert` parameters, for example
// `{ seed: 42, path: "3:1:0" }`, to replay the shrunk counterexample. A command sequence also
// prints `replayPath`; pass it to `fc.commands` as `{ replayPath: "..." }` next to the seed.
import fc from "fast-check";
import { assert, describe, expect, it } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import {
	arbItemData,
	arbItemKey,
	arbPoolKey,
	expectedDataKind,
	keyId,
	makeTestDB,
	propertyRuns,
	type DataKind,
	type ItemData,
	type ItemKey,
} from "./arbitraries.js";

// Every property runs the scenario many times against real Durable Objects, and shrinking a
// failure runs it many more. The default 5 s vitest timeout would hide the counterexample.
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

// The model: what the database must hold after every command so far.
type ModelItem = { data: ItemData; kind: DataKind; version: number };
type Model = { items: Map<string, ModelItem> };

// `check` says whether a command may run in the current model state. Every command here is valid
// in every state, so a command on a missing key is a real test case and not a skipped one.
abstract class ItemCommand implements fc.AsyncCommand<Model, FokosDB> {
	constructor(readonly key: ItemKey) {}
	check(): boolean {
		return true;
	}
	abstract run(m: Model, db: FokosDB): Promise<void>;
	// Printed in the failure report, so the command sequence is readable.
	toString(): string {
		return `${this.constructor.name}(${keyId(this.key)})`;
	}
}

class PutItem extends ItemCommand {
	constructor(
		key: ItemKey,
		readonly data: ItemData,
	) {
		super(key);
	}
	async run(m: Model, db: FokosDB): Promise<void> {
		const res = await db.putItem({ ...this.key, data: this.data });
		const id = keyId(this.key);
		// A new item starts at version 1. An overwrite adds one. A put after a delete starts again at 1.
		const version = (m.items.get(id)?.version ?? 0) + 1;
		expect(res).toMatchObject({ item: this.key, version });
		m.items.set(id, { data: this.data, kind: expectedDataKind(this.data), version });
	}
}

class GetItem extends ItemCommand {
	async run(m: Model, db: FokosDB): Promise<void> {
		const res = await db.getItem(this.key);
		const expected = m.items.get(keyId(this.key));
		if (expected === undefined) {
			expect(res).toMatchObject({ found: false, item: this.key });
			return;
		}
		assert(res.found);
		expect(res.item).toMatchObject({ ...this.key, kind: expected.kind, version: expected.version });
		// `toMatchObject` matches a SUBSET of an object value, so the data is compared exactly.
		expect(res.item.data).toEqual(expected.data);
	}
}

class DeleteItem extends ItemCommand {
	async run(m: Model, db: FokosDB): Promise<void> {
		const res = await db.deleteItem(this.key);
		const id = keyId(this.key);
		expect(res).toMatchObject({ item: this.key, deleted: m.items.has(id) });
		m.items.delete(id);
	}
}

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
				// A fresh table and a fresh model per run, so runs do not see each other's items.
				const setup = () => ({ model: { items: new Map() } as Model, real: makeTestDB() });
				await fc.asyncModelRun(setup, cmds);
			}),
			{ numRuns: propertyRuns(30) },
		);
	});
});
