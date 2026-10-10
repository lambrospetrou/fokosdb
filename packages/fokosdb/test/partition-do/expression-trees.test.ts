import { describe, expect, it } from "vitest";
import {
	compileConditionExpression,
	compileProjectionExpression,
	compileQueryExpression,
	compileUpdateExpression,
} from "../../src/shared/expression/compiler.js";
import type { ConditionExpression, ProjectionExpression, QueryExpressions, UpdateExpression } from "../../src/shared/expression/types.js";
import type { QueryItemsRpcRequest } from "../../src/server/do-partition.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { testCoordinatorRef } from "../stub-helpers.js";
import { kb, lockKeys, makeStub, withOpIndex } from "./helpers.js";

const stateIs = (state: string): ConditionExpression => ({ op: "eq", args: [{ ref: "data", path: "$.state" }, { val: state }] });
const SET_COUNT: UpdateExpression = [{ action: "set", target: { ref: "data", path: "$.count" }, value: { val: 1 } }];
const STATE: readonly ProjectionExpression[] = [{ expr: { ref: "data", path: "$.state" } }];

/**
 * A condition that is valid and that compiles to more SQL than `compiledSqlBytes`. Only a compile
 * refuses it, so a request that carries it and gets no error did not compile it.
 */
const ABOVE_SQL_LIMIT: ConditionExpression = {
	op: "or",
	args: Array.from({ length: 200 }, () => ({ op: "eq", args: [{ ref: "data", path: "$.state" }, { val: "open" }] })),
};

const sqlLimitError = fokosErrorWith("expression_invalid", { expressionCode: "sql_limit" });
const planRefused = fokosErrorWith("compiled_plan_refused");

const json = (value: unknown) => ({ data: JSON.stringify(value), kind: "json" as const });

const queryRequest = (plan: QueryExpressions): QueryItemsRpcRequest => ({
	hashKey: kb("hk"),
	interval: {},
	direction: "asc",
	remainingEvaluatedItems: 100,
	remainingEvaluatedBytes: 1024 * 1024,
	remainingResponseBytes: 1024 * 1024,
	remainingPartitionVisits: 10,
	allowOversizedFirstItem: true,
	cursor: null,
	select: "projection",
	plan,
});

describe("PartitionDO — a request that carries a compiled plan", () => {
	// A client that still compiles sends the plan in the field of the expression tree. The cast gives
	// the plan the type of the field, as the wire does.
	const condition = compileConditionExpression(stateIs("open")) as unknown as ConditionExpression;
	const update = compileUpdateExpression(SET_COUNT) as unknown as UpdateExpression;
	const projection = compileProjectionExpression(STATE) as unknown as readonly ProjectionExpression[];
	const query = compileQueryExpression({ filter: stateIs("open"), projection: STATE }) as unknown as QueryExpressions;
	const key = { hashKey: kb("hk"), sortKey: kb("sk") };

	it("is refused by the item RPCs and by the query RPC, and changes nothing", async () => {
		const { ctx, rpc } = makeStub();
		await rpc.apiPutItem(ctx, { ...key, ...json({ state: "open" }) });

		await expect(rpc.apiPutItem(ctx, { ...key, ...json({ state: "closed" }), condition })).rejects.toThrow(planRefused);
		await expect(rpc.apiDeleteItem(ctx, { ...key, condition })).rejects.toThrow(planRefused);
		await expect(rpc.apiGetItem(ctx, { ...key, projection })).rejects.toThrow(planRefused);
		await expect(rpc.apiQueryItems(ctx, queryRequest(query))).rejects.toThrow(planRefused);

		expect(await rpc.apiGetItem(ctx, key)).toMatchObject({ found: true, item: { data: JSON.stringify({ state: "open" }), version: 1 } });
	});

	it("is refused by the transaction RPCs, and writes no lock", async () => {
		const { ctx, rpc, stub } = makeStub();
		await rpc.apiPutItem(ctx, { ...key, ...json({ state: "open" }) });
		const prepare = (item: { condition?: ConditionExpression; update?: UpdateExpression; operation: "check" | "update" }) =>
			rpc.txPrepare(ctx, {
				transactionId: "plan-refused",
				transactionTimestamp: Date.now() * 1000,
				coordinator: testCoordinatorRef(),
				items: withOpIndex([{ ...key, ...item }]),
			});

		await expect(prepare({ operation: "check", condition })).rejects.toThrow(planRefused);
		await expect(prepare({ operation: "update", update })).rejects.toThrow(planRefused);
		await expect(rpc.txExecuteSingleShot(ctx, { items: withOpIndex([{ ...key, operation: "update", update }]) })).rejects.toThrow(
			planRefused,
		);
		await expect(rpc.txReadSnapshot(ctx, { items: [{ ...key, projection }] })).rejects.toThrow(planRefused);
		await expect(rpc.txReadForTransaction(ctx, { transactionId: "plan-refused-read", items: [{ ...key, projection }] })).rejects.toThrow(
			planRefused,
		);

		expect(await lockKeys(stub, "plan-refused")).toEqual([]);
		expect(await rpc.apiGetItem(ctx, key)).toMatchObject({ found: true, item: { version: 1 } });
	});

	it("is refused by the version check of an item with two operations, before any SQL statement", async () => {
		const { ctx, rpc } = makeStub();
		await expect(
			rpc.txExecuteSingleShot(ctx, {
				items: withOpIndex([
					{ ...key, operation: "put", data: "v", kind: "text" },
					{ ...key, operation: "check", condition },
				]),
			}),
		).rejects.toThrow(planRefused);
		expect(await rpc.apiGetItem(ctx, key)).toMatchObject({ found: false });
	});
});

describe("PartitionDO — an expression tree that does not compile", () => {
	it("fails apiPutItem and apiDeleteItem with sql_limit, and writes nothing", async () => {
		const { ctx, rpc } = makeStub();
		const key = { hashKey: kb("hk"), sortKey: kb("sk") };
		await rpc.apiPutItem(ctx, { ...key, ...json({ tags: [1] }) });

		await expect(rpc.apiPutItem(ctx, { ...key, ...json({ tags: [2] }), condition: ABOVE_SQL_LIMIT })).rejects.toThrow(sqlLimitError);
		await expect(rpc.apiDeleteItem(ctx, { ...key, condition: ABOVE_SQL_LIMIT })).rejects.toThrow(sqlLimitError);

		expect(await rpc.apiGetItem(ctx, key)).toMatchObject({ found: true, item: { data: JSON.stringify({ tags: [1] }), version: 1 } });
	});

	it("fails a prepare with an expression error, and writes no lock", async () => {
		const { ctx, rpc, stub } = makeStub();
		const transactionId = crypto.randomUUID();

		await expect(
			rpc.txPrepare(ctx, {
				transactionId,
				transactionTimestamp: Date.now() * 1000,
				coordinator: testCoordinatorRef(),
				items: withOpIndex([
					{ hashKey: kb("hk"), sortKey: kb("a"), operation: "put", data: "v", kind: "text" },
					{ hashKey: kb("hk"), sortKey: kb("b"), operation: "check", condition: ABOVE_SQL_LIMIT },
				]),
			}),
		).rejects.toThrow(sqlLimitError);

		expect(await lockKeys(stub, transactionId)).toEqual([]);
	});

	it("is not compiled by a repeated prepare of an item that the transaction already locks", async () => {
		const { ctx, rpc, stub } = makeStub();
		const prepare = (condition: ConditionExpression) =>
			rpc.txPrepare(ctx, {
				transactionId: "repeated-prepare",
				transactionTimestamp: 1_000_000,
				coordinator: testCoordinatorRef(),
				items: withOpIndex([{ hashKey: kb("hk"), sortKey: kb("sk"), operation: "check", condition }]),
			});

		expect(await prepare({ op: "not_exists", args: [{ ref: "hashKey" }] })).toEqual({ outcome: "accepted" });
		// The lock row holds the answer of the first prepare, so the second prepare evaluates nothing.
		expect(await prepare(ABOVE_SQL_LIMIT)).toEqual({ outcome: "accepted" });
		expect(await lockKeys(stub, "repeated-prepare")).toEqual(["hk/sk"]);
	});
});
