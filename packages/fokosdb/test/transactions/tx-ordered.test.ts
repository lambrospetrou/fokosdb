// transactWriteItems with executionMode "ordered_per_item", end to end through db.ts: the mode
// validation, the client checks, the idempotency fingerprint, and the same result on the
// single-partition path and on the two-phase path. The reference is the same operations sent one by
// one, each as a transaction of its own, from the same start state.
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import type { TransactionCoordinatorDO } from "../../src/server/do-transaction-coordinator.js";
import { FokosError, TRANSACTION_PENDING_CODES } from "../../src/shared/errors.js";
import { FokosTransactionCancelledError } from "../../src/shared/errors-operations.js";
import type { TransactWriteItem } from "../../src/shared/transaction-api-types.js";
import { fokosErrorWith } from "../errors-matchers.js";
import {
	controlledCoordinator,
	controlledPartition,
	type Key,
	keysAcrossPartitions,
	keysInOnePartition,
	makeDB,
	txCalls,
} from "./tx-helpers.js";

const TTL_AT = Math.floor(Date.now() / 1000) + 365 * 24 * 3600;
const readsV = { op: "eq", args: [{ ref: "v" }, { val: 1 }] } as const;
const exists = { op: "exists", args: [{ ref: "hashKey" }] } as const;
const notExists = { op: "not_exists", args: [{ ref: "hashKey" }] } as const;

type PutData = Extract<TransactWriteItem, { operation: "put" }>["data"];

const put = (key: Key, data: PutData | number, ttlAt?: number): TransactWriteItem => ({
	...key,
	operation: "put",
	data: (typeof data === "number" ? { value: data } : data) as PutData,
	...(ttlAt === undefined ? {} : { ttlAt }),
});
const update = (key: Key): TransactWriteItem => ({
	...key,
	operation: "update",
	update: [
		{
			action: "set",
			target: { ref: "data", path: "$.n" },
			value: { fn: "+", args: [{ fn: "if_not_exists", args: [{ ref: "data", path: "$.n" }, { val: 0 }] }, { val: 1 }] },
		},
	],
});
const del = (key: Key): TransactWriteItem => ({ ...key, operation: "delete" });
const check = (key: Key, present: boolean): TransactWriteItem => ({ ...key, operation: "check", condition: present ? exists : notExists });

const ORDERED = { executionMode: "ordered_per_item" } as const;

/** The number of write transactions that reached a partition of `keys` or the coordinator. */
async function writeRpcs(db: FokosDB, keys: Key[]): Promise<number> {
	return (await txCalls(db, keys, "txExecuteSingleShot")).length + (await controlledCoordinator(db).testInitiateWriteCalls());
}

describe("transactWriteItems - executionMode", () => {
	const db = makeDB({ rootTreesN: 4, controlled: true });

	it.each([undefined, "mode-token"])("refuses an executionMode that is not valid (token %s), and sends no RPC", async (token) => {
		const keys = keysInOnePartition(db, 1, `bad-mode-${crypto.randomUUID()}`);
		const before = await writeRpcs(db, keys);
		await expect(
			db.transactWriteItems({
				items: [put(keys[0], "v")],
				clientRequestToken: token === undefined ? undefined : `${token}-${crypto.randomUUID()}`,
				executionMode: "fast" as never,
			}),
		).rejects.toThrow(fokosErrorWith("transact_execution_mode_invalid", { value: "fast" }));
		expect(await writeRpcs(db, keys)).toBe(before);
	});

	it("refuses a repeated item in standard mode, and accepts it in ordered_per_item mode", async () => {
		const [key] = keysInOnePartition(db, 1, `repeat-${crypto.randomUUID()}`);
		await expect(db.transactWriteItems({ items: [put(key, { n: 1 }), update(key)] })).rejects.toThrow(
			fokosErrorWith("transact_duplicate_key", { opIndex: 1 }),
		);
		await db.transactWriteItems({ items: [put(key, { n: 1 }), update(key)], ...ORDERED });
		expect(await db.getItem(key)).toMatchObject({ found: true, item: { data: { n: 2 }, version: 2 } });
	});

	it.each([
		["put", (k: Key) => put(k, { n: 1 })],
		["update", (k: Key) => update(k)],
		["delete", (k: Key) => del(k)],
	])("refuses a version reference after a %s of the same item, and sends no RPC", async (_name, earlier) => {
		const keys = keysInOnePartition(db, 1, `version-ref-${crypto.randomUUID()}`);
		const before = await writeRpcs(db, keys);
		const setPrev: TransactWriteItem = {
			...keys[0],
			operation: "update",
			update: [{ action: "set", target: { ref: "data", path: "$.prev" }, value: { ref: "v" } }],
		};
		for (const later of [{ ...check(keys[0], true), condition: readsV }, setPrev]) {
			await expect(db.transactWriteItems({ items: [earlier(keys[0]), later], ...ORDERED })).rejects.toThrow(
				fokosErrorWith("transact_version_after_write", { opIndex: 1, earlierOpIndex: 0, hashKey: keys[0].hashKey }),
			);
		}
		expect(await writeRpcs(db, keys)).toBe(before);
	});

	it("accepts a version reference on the first operation of an item and after a check", async () => {
		const [key] = keysInOnePartition(db, 1, `version-first-${crypto.randomUUID()}`);
		await db.putItem({ ...key, data: { n: 1 } });
		await db.transactWriteItems({
			items: [{ ...check(key, true), condition: readsV }, check(key, true), { ...check(key, true), condition: readsV }, update(key)],
			...ORDERED,
		});
		expect(await db.getItem(key)).toMatchObject({ item: { data: { n: 2 }, version: 2 } });
	});

	it("replays a token with the same mode, and refuses the token with another mode", async () => {
		const keys = keysInOnePartition(db, 2, `mode-replay-${crypto.randomUUID()}`);
		const items = [put(keys[0], "a"), put(keys[1], "b")];
		const token = `mode-replay-${crypto.randomUUID()}`;
		const first = await db.transactWriteItems({ items, clientRequestToken: token, ...ORDERED });
		expect(await db.transactWriteItems({ items, clientRequestToken: token, ...ORDERED })).toEqual(first);
		await expect(db.transactWriteItems({ items, clientRequestToken: token })).rejects.toThrow(
			fokosErrorWith("idempotent_parameter_mismatch"),
		);

		// An absent mode is "standard".
		const standardToken = `mode-standard-${crypto.randomUUID()}`;
		const standard = await db.transactWriteItems({ items, clientRequestToken: standardToken });
		expect(await db.transactWriteItems({ items, clientRequestToken: standardToken, executionMode: "standard" })).toEqual(standard);
	});
});

type Snapshot = Record<string, unknown>;

/** What a caller can read of each key: found, data, version, and TTL. */
async function readAll(db: FokosDB, keys: Key[]): Promise<Snapshot> {
	const out: Snapshot = {};
	for (const key of keys) {
		const res = await db.getItem(key);
		out[`${key.hashKey}/${key.sortKey}`] = res.found ? { data: res.item.data, version: res.item.version, ttlAt: res.item.ttlAt } : null;
	}
	return out;
}

/** The start state of each run: A exists with version 2, B exists, C was deleted. */
async function seed(db: FokosDB, [a, b, c]: Key[]): Promise<void> {
	await db.putItem({ ...a, data: { n: 1 } });
	await db.putItem({ ...a, data: { n: 1, a: true }, ttlAt: TTL_AT });
	await db.putItem({ ...b, data: { b: 1 } });
	await db.putItem({ ...c, data: "gone" });
	await db.putItem({ ...c, data: "gone" });
	await db.deleteItem(c);
}

/**
 * Runs `build(keys)` on the single-partition path, on the two-phase path, and as the reference, each on
 * a table of its own, and compares what a caller reads. `spread` picks keys on two partitions.
 */
async function expectSameOnBothPaths(build: (keys: Key[]) => TransactWriteItem[], spread = false): Promise<void> {
	const tables = [makeDB({ rootTreesN: 4 }), makeDB({ rootTreesN: 4, singlePartitionFastPath: false }), makeDB({ rootTreesN: 4 })];
	const prefix = `paths-${crypto.randomUUID()}`;
	const keys = spread
		? [...keysAcrossPartitions(tables[0], 2, prefix), ...keysInOnePartition(tables[0], 2, `${prefix}-x`)]
		: keysInOnePartition(tables[0], 4, prefix);
	const items = build(keys);
	const results: Snapshot[] = [];
	for (const [i, db] of tables.entries()) {
		await seed(db, keys);
		if (i < 2) {
			await db.transactWriteItems({ items, ...ORDERED });
		} else {
			for (const item of items) {
				await db.transactWriteItems({ items: [item] });
			}
		}
		results.push(await readAll(db, keys));
	}
	expect(results[0]).toEqual(results[2]);
	expect(results[1]).toEqual(results[2]);
}

describe("transactWriteItems - ordered_per_item gives the same result on both paths", () => {
	it.each<[string, (k: Key[]) => TransactWriteItem[]]>([
		["put → update", ([a]) => [put(a, { n: 5 }), update(a)]],
		["put → check", ([a]) => [put(a, { n: 5 }), check(a, true)]],
		["delete → put", ([a]) => [del(a), put(a, { n: 7 }, TTL_AT)]],
		["put → delete", ([a]) => [put(a, { n: 5 }), del(a)]],
		["put → delete → put", ([a]) => [put(a, { big: "x".repeat(1000) }, TTL_AT), del(a), put(a, { n: 9 })]],
		["check → check", ([a]) => [check(a, true), check(a, true)]],
		["a sequence on an absent item", ([, , , d]) => [update(d), del(d), check(d, false), put(d, { n: 2 }), update(d)]],
		[
			"a recreate of a deleted item next to a delete of another",
			([a, b, c]) => [put(c, { c: 1 }), del(b), update(c), del(a), put(a, "back")],
		],
		// The keys of a partition apply in opIndex order, not in key order.
		["a put of a new item after a delete of another", ([a, , , d]) => [del(a), put(d, { d: 1 })]],
		["a put of a new item before a delete of another", ([a, , , d]) => [put(d, { d: 1 }), del(a)]],
	])("%s", async (_name, build) => {
		await expectSameOnBothPaths(build);
	});

	it("gives the same result when the repeated items are on two partitions", async () => {
		await expectSameOnBothPaths(
			([a, b, c, d]) => [put(a, { n: 3 }), update(b), del(c), update(a), put(c, "again"), del(b), put(d, 1)],
			true,
		);
	});
});

describe("transactWriteItems - ordered_per_item cancellation", () => {
	it.each([
		["the single-partition path", false],
		["the two-phase path", true],
	])("reports the first failure of an item and applies nothing on %s", async (_name, spread) => {
		const db = makeDB({ rootTreesN: 4 });
		const prefix = `cancel-${crypto.randomUUID()}`;
		const [a, b] = spread ? keysAcrossPartitions(db, 2, prefix) : keysInOnePartition(db, 2, prefix);
		await db.putItem({ ...b, data: "b" });
		const before = await readAll(db, [a, b]);

		const error = await db.transactWriteItems({ items: [put(a, "a"), check(a, false), put(a, "a2"), del(b)], ...ORDERED }).then(
			() => null,
			(e: unknown) => e,
		);
		expect(error).toBeInstanceOf(FokosTransactionCancelledError);
		expect((error as FokosTransactionCancelledError).results).toMatchObject([
			{ outcome: "passed" },
			{ outcome: "rejected", reason: { code: "condition_failed" } },
			{ outcome: "not_evaluated" },
			{ outcome: "passed" },
		]);
		expect(await readAll(db, [a, b])).toEqual(before);
	});
});

describe("transactWriteItems - ordered_per_item recovery", () => {
	it("finishes the commit of an ordered transaction in the tx_recovery job, and a replay applies nothing", async () => {
		const db = makeDB({ controlled: true });
		const [a, b] = keysAcrossPartitions(db, 2, `ordered-recovery-${crypto.randomUUID()}`);
		await db.putItem({ ...b, data: "b" });
		const coordinator = controlledCoordinator(db);
		await coordinator.testConfig({
			fanoutRequestBudgetMs: 250,
			staleTransactionMs: 250,
			participantRetry: { baseDelayMs: 10, maxDelayMs: 50 },
		});
		const partition = controlledPartition(db, b);
		await partition.testTxResponse("txCommit", { error: "simulated participant outage" });

		const items = [put(a, { n: 1 }), update(a), del(b), put(b, { m: 1 }), update(b)];
		const token = `ordered-recovery-${crypto.randomUUID()}`;
		const error = await db.transactWriteItems({ items, clientRequestToken: token, ...ORDERED }).then(
			() => null,
			(e: unknown) => e,
		);
		expect(FokosError.isCode(error, TRANSACTION_PENDING_CODES.transaction_commit_pending)).toBe(true);

		await partition.testClearTxResponse("txCommit");
		await coordinator.testConfig({});
		await runInDurableObject(coordinator, async (instance: TransactionCoordinatorDO, state: DurableObjectState) => {
			state.storage.sql.exec(`UPDATE tc_state SET next_recovery_at = 0 WHERE completed_at IS NULL`);
			await (instance as unknown as { recoverStaleTransactions(): Promise<void> }).recoverStaleTransactions();
			expect(state.storage.sql.exec<{ state: string }>(`SELECT state FROM tc_state WHERE idempotency_token = ?`, token).one().state).toBe(
				"COMMITTED",
			);
		});

		const after = await readAll(db, [a, b]);
		expect(after).toEqual({
			[`${a.hashKey}/${a.sortKey}`]: { data: { n: 2 }, version: 2, ttlAt: undefined },
			[`${b.hashKey}/${b.sortKey}`]: { data: { m: 1, n: 1 }, version: 3, ttlAt: undefined },
		});
		await db.transactWriteItems({ items, clientRequestToken: token, ...ORDERED });
		expect(await readAll(db, [a, b])).toEqual(after);
	});
});
