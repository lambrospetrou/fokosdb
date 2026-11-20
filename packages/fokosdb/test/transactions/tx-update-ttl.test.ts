import { runInDurableObject } from "cloudflare:test";
import { describe, it, expect, vi } from "vitest";
import invariant from "../../src/shared/invariant.js";
import type { UpdateExpression } from "../../src/shared/types.js";
import type { ExpressionValue } from "../../src/shared/expression/types.js";
import { compileUpdateExpression } from "../../src/shared/expression/compiler.js";
import { EXPRESSION_LIMITS } from "../../src/shared/expression/limits.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { makeDB, owningPartition, writeOutcomeWithClockRetry } from "./tx-helpers.js";

const TTL: { ref: "ttlAt" } = { ref: "ttlAt" };
const add = (left: ExpressionValue, right: number): ExpressionValue => ({ fn: "+", args: [left, { val: right }] });
const setTtl = (value: ExpressionValue): UpdateExpression => [{ action: "set", target: TTL, value }];
const nowSec = () => Math.floor(Date.now() / 1000);

/**
 * An update action on `ttlAt` on both write paths. `singlePartitionFastPath: false` sends a
 * transaction of one item through prepare, the lock row, and commit.
 */
describe.each([true, false])("transactions - an update action on ttlAt (singlePartitionFastPath=%s)", (singlePartitionFastPath) => {
	const db = makeDB({ singlePartitionFastPath, rootTreesN: 8 });
	const newKey = (name: string) => ({ hashKey: `${name}-${crypto.randomUUID()}` });
	const update = (key: { hashKey: string }, actions: UpdateExpression) =>
		writeOutcomeWithClockRetry(db, { items: [{ ...key, operation: "update", update: actions }] });
	const rejected = (key: { hashKey: string }, code: string) => ({
		outcome: "cancelled",
		results: [{ outcome: "rejected", reason: { code, hashKey: key.hashKey } }],
	});

	// Workers SQLite binds every JS number as a real, so this also covers a whole number that is a real.
	it("stores a literal TTL", async () => {
		const key = newKey("literal");
		const ttlAt = nowSec() + 3600;
		await db.putItem({ ...key, data: { n: 1 } });

		expect(await update(key, setTtl({ val: ttlAt }))).toMatchObject({ outcome: "committed" });
		expect(await db.getItem(key)).toMatchObject({ found: true, item: { version: 2, ttlAt, data: { n: 1 } } });
	});

	it("extends the stored TTL and changes the data in the same write", async () => {
		const key = newKey("extend");
		const ttlAt = nowSec() + 3600;
		await db.putItem({ ...key, data: { renewals: 0 }, ttlAt });

		const res = await update(key, [
			{ action: "set", target: { ref: "data", path: "$.renewals" }, value: add({ ref: "data", path: "$.renewals" }, 1) },
			{ action: "set", target: TTL, value: add(TTL, 3600) },
		]);
		expect(res).toMatchObject({ outcome: "committed" });
		expect(await db.getItem(key)).toMatchObject({ found: true, item: { ttlAt: ttlAt + 3600, data: { renewals: 1 } } });
	});

	it("rejects with update_not_applicable when the item has no TTL to extend", async () => {
		const key = newKey("missing-ttl");
		await db.putItem({ ...key, data: {} });

		expect(await update(key, setTtl(add(TTL, 3600)))).toMatchObject(rejected(key, "update_not_applicable"));
		expect(await db.getItem(key)).toMatchObject({ found: true, item: { version: 1 } });
	});

	it("sets a TTL with if_not_exists on an item with no TTL and on an item with a TTL", async () => {
		const start = nowSec() + 3600;
		const actions = setTtl(add({ fn: "if_not_exists", args: [TTL, { val: start }] }, 60));
		const without = newKey("ine-without");
		const withTtl = newKey("ine-with");
		await db.putItem({ ...without, data: {} });
		await db.putItem({ ...withTtl, data: {}, ttlAt: start + 1000 });

		expect(await update(without, actions)).toMatchObject({ outcome: "committed" });
		expect(await update(withTtl, actions)).toMatchObject({ outcome: "committed" });
		expect(await db.getItem(without)).toMatchObject({ found: true, item: { ttlAt: start + 60 } });
		expect(await db.getItem(withTtl)).toMatchObject({ found: true, item: { ttlAt: start + 1060 } });
	});

	it("copies a TTL from a document attribute", async () => {
		const key = newKey("copy");
		const expiresAt = nowSec() + 3600;
		await db.putItem({ ...key, data: { expiresAt } });

		expect(await update(key, setTtl({ ref: "data", path: "$.expiresAt" }))).toMatchObject({ outcome: "committed" });
		expect(await db.getItem(key)).toMatchObject({ found: true, item: { ttlAt: expiresAt } });
	});

	it.each([
		["a number with a fraction", 1700000000.5],
		["a number in text", "1700000000"],
		["zero", 0],
		["a negative number", -1],
		["null", null],
		["a boolean", true],
		["a number above the integer range", 1e300],
	])("rejects with update_ttl_invalid for %s and keeps the stored TTL", async (_name, expiresAt) => {
		const key = newKey("invalid");
		const ttlAt = nowSec() + 3600;
		await db.putItem({ ...key, data: { expiresAt }, ttlAt });

		expect(await update(key, setTtl({ ref: "data", path: "$.expiresAt" }))).toMatchObject(rejected(key, "update_ttl_invalid"));
		expect(await db.getItem(key)).toMatchObject({ found: true, item: { version: 1, ttlAt } });
	});

	it("rejects with update_value_is_bytes when the value is bytes for this item", async () => {
		const key = newKey("bytes");
		const ttlAt = nowSec() + 3600;
		await db.putItem({ ...key, data: {}, ttlAt });
		// coalesce can give a number or bytes, so validation accepts it, and the item decides.
		const value: ExpressionValue = { fn: "sqlite.coalesce", args: [{ fn: "sqlite.unhex", args: [{ val: "01" }] }, TTL] };

		expect(await update(key, setTtl(value))).toMatchObject(rejected(key, "update_value_is_bytes"));
		expect(await db.getItem(key)).toMatchObject({ found: true, item: { version: 1, ttlAt } });
	});

	it("removes the TTL, and a remove on an item with no TTL changes only the version", async () => {
		const withTtl = newKey("remove-with");
		const without = newKey("remove-without");
		await db.putItem({ ...withTtl, data: { a: 1 }, ttlAt: nowSec() + 3600 });
		await db.putItem({ ...without, data: { a: 1 } });
		const actions: UpdateExpression = [{ action: "remove", target: TTL }];

		expect(await update(withTtl, actions)).toMatchObject({ outcome: "committed" });
		expect(await update(without, actions)).toMatchObject({ outcome: "committed" });
		for (const key of [withTtl, without]) {
			const got = await db.getItem(key);
			invariant(got.found, "expected the item");
			expect(got.item).toMatchObject({ version: 2, data: { a: 1 } });
			expect(got.item.ttlAt).toBeUndefined();
		}
	});

	it("creates an absent item with a literal TTL, and rejects a value that reads ttlAt", async () => {
		const created = newKey("absent-literal");
		const ttlAt = nowSec() + 3600;
		expect(await update(created, setTtl({ val: ttlAt }))).toMatchObject({ outcome: "committed" });
		expect(await db.getItem(created)).toMatchObject({ found: true, item: { version: 1, ttlAt, data: {} } });

		const absent = newKey("absent-read");
		expect(await update(absent, setTtl(add(TTL, 60)))).toMatchObject(rejected(absent, "update_not_applicable"));
		expect(await db.getItem(absent)).toMatchObject({ found: false });
	});

	it("refuses the ttlAt field and an action on ttlAt in one operation before any I/O", async () => {
		const key = newKey("two-sources");
		await expect(
			db.transactWriteItems({ items: [{ ...key, operation: "update", ttlAt: nowSec() + 60, update: setTtl({ val: nowSec() + 3600 }) }] }),
		).rejects.toThrow(fokosErrorWith("transact_operation_fields_invalid"));
		expect(await db.getItem(key)).toMatchObject({ found: false });
	});

	it("reads ttlAt after a put of the same item in an ordered_per_item transaction", async () => {
		const key = newKey("ordered");
		const ttlAt = nowSec() + 3600;
		const res = await writeOutcomeWithClockRetry(db, {
			executionMode: "ordered_per_item",
			items: [
				{ ...key, operation: "put", data: { a: 1 }, ttlAt },
				{ ...key, operation: "update", update: setTtl(add(TTL, 10)) },
			],
		});
		expect(res).toMatchObject({ outcome: "committed" });
		expect(await db.getItem(key)).toMatchObject({ found: true, item: { ttlAt: ttlAt + 10, data: { a: 1 } } });
	});

	it("the TTL sweep deletes an item after the TTL that an update computed", async () => {
		const key = newKey("sweep");
		await db.putItem({ ...key, data: {}, ttlAt: nowSec() + 3600 });

		expect(await update(key, setTtl(add(TTL, -7200)))).toMatchObject({ outcome: "committed" });
		const { stub, pCtx } = owningPartition(db, { ...key, sortKey: "" });
		await vi.waitFor(
			async () => {
				// A request arms the sweep timer of the partition, and the sweep runs after its delay.
				await db.getItem(key);
				// Other tests of this table write to the same partition, so the count reads this key only.
				const count = await runInDurableObject(stub, (_instance, state: DurableObjectState) => {
					return state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM items WHERE hk = ?`, KeyCodec.encode(key.hashKey)).one()
						.n;
				});
				expect(count, pCtx.doName).toBe(0);
			},
			{ timeout: 10_000, interval: 300 },
		);
	});
});

describe("transactions - the SQL limit of an update with an action on ttlAt", () => {
	// Each data action is `$.cI = if_not_exists($.cI, 0) + 1 + 1 ...`. One action stays free for ttlAt.
	const ACTIONS = EXPRESSION_LIMITS.updateActions - 1;
	const counters = (additions: number): UpdateExpression =>
		Array.from({ length: ACTIONS }, (_, i) => {
			const path = { ref: "data" as const, path: `$.c${i}` };
			let value: ExpressionValue = { fn: "if_not_exists", args: [path, { val: 0 }] };
			for (let n = 0; n < additions; n++) {
				value = add(value, 1);
			}
			return { action: "set" as const, target: path, value };
		});
	const compiles = (update: UpdateExpression): boolean => {
		try {
			compileUpdateExpression(update);
			return true;
		} catch (error) {
			expect(error).toMatchObject({ code: "sql_limit" });
			return false;
		}
	};

	it("counts the SQL of the ttlAt value in the limit of the statements", () => {
		let largest = 0;
		while (largest < 20 && compiles(counters(largest + 1))) {
			largest++;
		}
		expect(largest).toBeGreaterThan(0);
		expect(largest).toBeLessThan(20);
		// The largest data actions that compile, plus a ttlAt value that uses the rest of the operator budget.
		const rest = EXPRESSION_LIMITS.operatorsAndFunctions - ACTIONS * (largest + 1) - 1;
		expect(rest).toBeGreaterThan(largest);
		// A balanced tree of `rest` additions stays inside the depth limit.
		const sum = (n: number): ExpressionValue => {
			if (n === 0) {
				return TTL;
			}
			const left = Math.floor((n - 1) / 2);
			return { fn: "+", args: [sum(left), sum(n - 1 - left)] };
		};
		const ttlValue = sum(rest);
		expect(compiles([...counters(largest), ...setTtl(ttlValue)])).toBe(false);
	});
});
