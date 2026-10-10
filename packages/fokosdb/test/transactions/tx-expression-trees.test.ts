// A coordinator accepts a request whose conditions and updates are expression trees. It stores the
// trees, sends them to the participants, and each participant compiles its own.
import { runInDurableObject } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import type { TransactionCoordinatorDO } from "../../src/server/do-transaction-coordinator.js";
import type { ConditionExpression } from "../../src/shared/expression/types.js";
import { createTableConfig } from "../../src/shared/partition-context.js";
import { DEFAULT_LIMITS, encodeHashKey, encodeSortKey } from "../../src/shared/transaction-limits.js";
import type { InitiateWriteResponseEncoded, TCWriteOperation } from "../../src/shared/transaction-wire-types.js";
import { fokosErrorWith } from "../errors-matchers.js";
import {
	controlledCoordinator,
	coordinatorRouter,
	keysAcrossPartitions,
	keysInOnePartition,
	makeDB,
	txCalls,
	type Key,
} from "./tx-helpers.js";

/** A condition that is valid and that compiles to more SQL than `compiledSqlBytes`. */
const ABOVE_SQL_LIMIT: ConditionExpression = {
	op: "or",
	args: Array.from({ length: 200 }, () => ({ op: "eq", args: [{ ref: "data", path: "$.state" }, { val: "open" }] })),
};

/** Sends `items` to the one root coordinator of a `controlled` table, as `db.ts` does. */
async function initiateWrite(
	db: FokosDB,
	items: (Key & Omit<TCWriteOperation, "opIndex" | "hashKey" | "sortKey">)[],
): Promise<InitiateWriteResponseEncoded> {
	const root = coordinatorRouter(db).allRoots()[0];
	const stub = env.CONTROLLED_TRANSACTION_COORDINATOR_DO.getByName(root.doName);
	return await runInDurableObject(stub, async (instance: TransactionCoordinatorDO) => {
		const response = await instance.initiateWrite(root, {
			clientRequestToken: `tree-${crypto.randomUUID()}`,
			table: createTableConfig(db.options()),
			items: items.map((item, opIndex) => ({
				...item,
				opIndex,
				hashKey: encodeHashKey(item.hashKey, DEFAULT_LIMITS),
				sortKey: encodeSortKey(item.sortKey, DEFAULT_LIMITS),
			})),
			executionMode: "standard",
		});
		return response.value;
	});
}

describe("FokosDB — the client sends expression trees", () => {
	it("sends the condition, the update, and the projection of a transaction as the caller gave them", async () => {
		const db = makeDB({ controlled: true });
		const [key] = keysInOnePartition(db, 1, "tree-client");
		await db.putItem({ ...key, data: { state: "open" } });
		const condition: ConditionExpression = { op: "eq", args: [{ ref: "data", path: "$.state" }, { val: "open" }] };
		const update = [{ action: "set", target: { ref: "data", path: "$.count" }, value: { val: 1 } }] as const;
		const projection = [{ expr: { ref: "data", path: "$.count" } }] as const;

		await db.transactWriteItems({ items: [{ ...key, operation: "update", condition, update }] });
		const read = await db.transactGetItems({ items: [{ ...key, projection }] });

		expect(read.items[0]).toMatchObject({ found: true, data: { "$.count": 1 } });
		const [write] = await txCalls(db, [key], "txExecuteSingleShot");
		expect(write.items[0]).toMatchObject({ condition, update });
		expect(write.items[0].condition).toEqual(condition);
		expect(write.items[0].update).toEqual(update);
		const [snapshot] = await txCalls(db, [key], "txReadSnapshot");
		expect(snapshot.items[0].projection).toEqual(projection);
	});
});

describe("FokosDB — the client validates and does not compile", () => {
	it("fails a tree that is not valid before any I/O", async () => {
		const db = makeDB({ controlled: true });
		const keys = keysAcrossPartitions(db, 2, "tree-invalid");
		const notValid = { op: "eq", args: [{ ref: "v" }] } as unknown as ConditionExpression;

		await expect(
			db.transactWriteItems({
				items: [
					{ ...keys[0], operation: "put", data: "never" },
					{ ...keys[1], operation: "check", condition: notValid },
				],
			}),
		).rejects.toThrow(fokosErrorWith("expression_invalid", { expressionCode: "invalid_arity" }));

		expect(await controlledCoordinator(db).testInitiateWriteCalls()).toBe(0);
		expect(await txCalls(db, keys, "txExecuteSingleShot")).toHaveLength(0);
	});

	it("gets the SQL limit error of a valid tree from the partition, and putItem writes nothing", async () => {
		const db = makeDB();
		const key = { hashKey: "sql-limit", sortKey: "sk" };
		await db.putItem({ ...key, data: { state: "open" } });

		await expect(db.putItem({ ...key, data: { state: "closed" }, condition: ABOVE_SQL_LIMIT })).rejects.toThrow(
			fokosErrorWith("expression_invalid", { expressionCode: "sql_limit" }),
		);

		expect(await db.getItem(key)).toMatchObject({ found: true, item: { data: { state: "open" }, version: 1 } });
	});
});

describe("transactWriteItems — a broken expression from a caller that does not use the client", () => {
	it("refuses a condition that is not an object before the coordinator stores the transaction", async () => {
		const db = makeDB({ controlled: true });
		const [a, b] = keysAcrossPartitions(db, 2, "tree-broken-shape");

		await expect(
			initiateWrite(db, [
				{ ...a, operation: "put", data: "never", kind: "text" },
				{ ...b, operation: "check", condition: "v = 1" as unknown as ConditionExpression },
			]),
		).rejects.toThrow(fokosErrorWith("expression_invalid", { expressionCode: "invalid_ast" }));

		expect(await db.getItem(a)).toMatchObject({ found: false });
		await db.putItem({ ...a, data: "after" });
	});

	it("cancels a transaction whose condition has an operator that does not exist, and releases the locks", async () => {
		const db = makeDB({ controlled: true });
		const [a, b] = keysAcrossPartitions(db, 2, "tree-broken-op");

		const response = await initiateWrite(db, [
			{ ...a, operation: "put", data: "never", kind: "text" },
			{ ...b, operation: "check", condition: { op: "bogus", args: [{ ref: "v" }] } as unknown as ConditionExpression },
		]);

		expect(response).toMatchObject({ outcome: "cancelled" });
		expect(response.outcome === "cancelled" && response.results[1]).toMatchObject({
			outcome: "rejected",
			reason: { code: "expression_invalid" },
		});
		expect(await db.getItem(a)).toMatchObject({ found: false });
		await db.putItem({ ...a, data: "after" });
		expect(await db.getItem(a)).toMatchObject({ found: true, item: { data: "after", version: 1 } });
	});
});

describe("transactWriteItems — expression trees in the coordinator request", () => {
	it("commits an update and a check that carry expression trees, across two partitions", async () => {
		const db = makeDB({ controlled: true });
		const [a, b] = keysAcrossPartitions(db, 2, "tree-commit");
		await db.putItem({ ...a, data: { state: "open", count: 1 } });
		await db.putItem({ ...b, data: { state: "open" } });
		const stateIsOpen: ConditionExpression = { op: "eq", args: [{ ref: "data", path: "$.state" }, { val: "open" }] };

		const response = await initiateWrite(db, [
			{
				...a,
				operation: "update",
				condition: stateIsOpen,
				update: [
					{
						action: "set",
						target: { ref: "data", path: "$.count" },
						value: { fn: "+", args: [{ ref: "data", path: "$.count" }, { val: 1 }] },
					},
				],
			},
			{ ...b, operation: "check", condition: stateIsOpen },
		]);

		expect(response).toMatchObject({ outcome: "committed" });
		expect(await db.getItem(a)).toMatchObject({ found: true, item: { data: { state: "open", count: 2 }, version: 2 } });
	});

	it("cancels a transaction whose tree does not compile, with the expression error as the reason, and releases the locks", async () => {
		const db = makeDB({ controlled: true });
		const [a, b] = keysAcrossPartitions(db, 2, "tree-cancel");

		const response = await initiateWrite(db, [
			{ ...a, operation: "put", data: "never", kind: "text" },
			{ ...b, operation: "check", condition: ABOVE_SQL_LIMIT },
		]);

		expect(response).toMatchObject({ outcome: "cancelled" });
		expect(response.outcome === "cancelled" && response.results[1]).toMatchObject({
			outcome: "rejected",
			reason: { code: "expression_invalid" },
		});
		// No lock stays on the other participant: a write with no transaction is accepted at once.
		expect(await db.getItem(a)).toMatchObject({ found: false });
		await db.putItem({ ...a, data: "after" });
		expect(await db.getItem(a)).toMatchObject({ found: true, item: { data: "after", version: 1 } });
	});
});
