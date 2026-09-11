import { describe, it, expect } from "vitest";
import invariant from "../../src/shared/invariant.js";
import type { UpdateExpression } from "../../src/shared/types.js";
import { MAX_ITEM_BYTES } from "../../src/shared/transaction-limits.js";
import { EXPRESSION_LIMITS } from "../../src/shared/expression/limits.js";
import { UPDATE_FIXED_BINDING_COUNT, UPDATE_MAX_TRAILING_BINDING_COUNT } from "../../src/shared/expression/plan.js";
import { makeDB, writeOutcome } from "./tx-helpers.js";

/**
 * A compiled plan is a fragment, not a statement: the keys are bound before it and each statement
 * appends its own tail after it. The compiler charges the widest of those tails to every plan, so a
 * plan that compiles runs on EVERY path. Without that, a wide plan committed through the single-shot
 * path and failed in the lock pass of the two-phase path, reported as a retryable transient error.
 */
describe("transactions - an update plan at the binding limit", () => {
	// The plan fills the budget the compiler leaves it, and the item carries a ttlAt so that the widest
	// statement, insertPendingUpdateLock, binds its sixth and last tail parameter. The two together are
	// exactly completeStatementBindings, which is the case that must run rather than merely compile.
	const widestUpdate = (): UpdateExpression => {
		const planCap = EXPRESSION_LIMITS.completeStatementBindings - UPDATE_FIXED_BINDING_COUNT - UPDATE_MAX_TRAILING_BINDING_COUNT;
		const literals = planCap - EXPRESSION_LIMITS.updateActions - 1;
		const paired = literals - EXPRESSION_LIMITS.updateActions;
		let next = 0;
		return Array.from({ length: EXPRESSION_LIMITS.updateActions }, (_, i) => ({
			action: "set" as const,
			target: { ref: "data" as const, path: `$.f${i}` },
			value: i < paired ? { fn: "+" as const, args: [{ val: next++ }, { val: next++ }] } : { val: next++ },
		}));
	};

	it.each([true, false])("commits on both write paths (singlePartitionFastPath=%s)", async (singlePartitionFastPath) => {
		const db = makeDB({ singlePartitionFastPath });
		const key = { hashKey: `wide-${crypto.randomUUID()}` };
		await db.putItem({ ...key, data: {} });

		const ttlAt = Math.floor(Date.now() / 1000) + 3600;
		const res = await writeOutcome(db.transactWriteItems({ items: [{ ...key, operation: "update", update: widestUpdate(), ttlAt }] }));

		expect(res.outcome).toBe("committed");
		const got = await db.getItem(key);
		invariant(got.found, "expected the updated item");
		expect(Object.keys(got.item.data as Record<string, unknown>)).toHaveLength(EXPRESSION_LIMITS.updateActions);
		expect(got.item.ttlAt).toBe(ttlAt);
	});
});

/**
 * Every case runs on both write paths. The router chooses between them, so a caller cannot, and an
 * update that behaves differently on one of them is a defect a single-path suite cannot see. The
 * two-phase path is the one with the materialized pending row, the lock, and the commit apply.
 */
describe.each([true, false])("transactions - update expressions (singlePartitionFastPath=%s)", (singlePartitionFastPath) => {
	it("evaluates pre-image: REMOVE a SET b = a, c = b gives {b: 1, c: 2}", async () => {
		const db = makeDB({ singlePartitionFastPath });
		const key = { hashKey: `user-${crypto.randomUUID()}` };
		await db.putItem({ ...key, data: { a: 1, b: 2, c: 3 } });

		const update: UpdateExpression = [
			{ action: "remove", target: { ref: "data", path: "$.a" } },
			{ action: "set", target: { ref: "data", path: "$.b" }, value: { ref: "data", path: "$.a" } },
			{ action: "set", target: { ref: "data", path: "$.c" }, value: { ref: "data", path: "$.b" } },
		];

		const res = await writeOutcome(
			db.transactWriteItems({
				items: [{ ...key, operation: "update", update }],
			}),
		);
		expect(res.outcome).toBe("committed");

		await expect(db.getItem(key)).resolves.toMatchObject({
			found: true,
			item: {
				version: 2,
				data: { b: 1, c: 2 },
			},
		});
	});

	it("removes plain array indexes under one parent in descending index order", async () => {
		const db = makeDB({ singlePartitionFastPath });
		const key = { hashKey: `list-${crypto.randomUUID()}` };
		await db.putItem({ ...key, data: { r: ["c", "h", "n", "s", "x"] } });

		const update: UpdateExpression = [
			{ action: "remove", target: { ref: "data", path: "$.r[1]" } },
			{ action: "remove", target: { ref: "data", path: "$.r[2]" } },
		];

		const res = await writeOutcome(
			db.transactWriteItems({
				items: [{ ...key, operation: "update", update }],
			}),
		);
		expect(res.outcome).toBe("committed");

		await expect(db.getItem(key)).resolves.toMatchObject({
			found: true,
			item: {
				version: 2,
				data: { r: ["c", "s", "x"] },
			},
		});
	});

	it("rejects update with update_not_applicable when item is missing, text, or bytes", async () => {
		const db = makeDB({ singlePartitionFastPath });
		const missingKey = { hashKey: `missing-${crypto.randomUUID()}` };
		const textKey = { hashKey: `text-${crypto.randomUUID()}` };
		const bytesKey = { hashKey: `bytes-${crypto.randomUUID()}` };

		await db.putItem({ ...textKey, data: "raw text" });
		await db.putItem({ ...bytesKey, data: new Uint8Array([1, 2, 3]) });

		const update: UpdateExpression = [{ action: "set", target: { ref: "data", path: "$.x" }, value: { val: 1 } }];

		const missingRes = await writeOutcome(
			db.transactWriteItems({
				items: [{ ...missingKey, operation: "update", update }],
			}),
		);
		expect(missingRes).toMatchObject({
			outcome: "cancelled",
			results: [{ outcome: "rejected", reason: { code: "update_not_applicable", hashKey: missingKey.hashKey } }],
		});

		const textRes = await writeOutcome(
			db.transactWriteItems({
				items: [{ ...textKey, operation: "update", update }],
			}),
		);
		expect(textRes).toMatchObject({
			outcome: "cancelled",
			results: [{ outcome: "rejected", reason: { code: "update_not_applicable", hashKey: textKey.hashKey } }],
		});

		const bytesRes = await writeOutcome(
			db.transactWriteItems({
				items: [{ ...bytesKey, operation: "update", update }],
			}),
		);
		expect(bytesRes).toMatchObject({
			outcome: "cancelled",
			results: [{ outcome: "rejected", reason: { code: "update_not_applicable", hashKey: bytesKey.hashKey } }],
		});
	});

	it("rejects update with update_not_applicable on missing parent or index past end", async () => {
		const db = makeDB({ singlePartitionFastPath });
		const key = { hashKey: `guard-${crypto.randomUUID()}` };
		await db.putItem({ ...key, data: { a: 1, list: [1, 2] } });

		// set on a missing parent
		const res1 = await writeOutcome(
			db.transactWriteItems({
				items: [
					{
						...key,
						operation: "update",
						update: [{ action: "set", target: { ref: "data", path: "$.missing.child" }, value: { val: 10 } }],
					},
				],
			}),
		);
		expect(res1).toMatchObject({
			outcome: "cancelled",
			results: [{ outcome: "rejected", reason: { code: "update_not_applicable", hashKey: key.hashKey } }],
		});

		// set on an index past end
		const res2 = await writeOutcome(
			db.transactWriteItems({
				items: [
					{
						...key,
						operation: "update",
						update: [{ action: "set", target: { ref: "data", path: "$.list[5]" }, value: { val: 10 } }],
					},
				],
			}),
		);
		expect(res2).toMatchObject({
			outcome: "cancelled",
			results: [{ outcome: "rejected", reason: { code: "update_not_applicable", hashKey: key.hashKey } }],
		});

		// set on scalar parent
		const res3 = await writeOutcome(
			db.transactWriteItems({
				items: [
					{
						...key,
						operation: "update",
						update: [{ action: "set", target: { ref: "data", path: "$.a.child" }, value: { val: 10 } }],
					},
				],
			}),
		);
		expect(res3).toMatchObject({
			outcome: "cancelled",
			results: [{ outcome: "rejected", reason: { code: "update_not_applicable", hashKey: key.hashKey } }],
		});
	});

	it("rejects update with update_not_applicable when an operand is missing or the arithmetic is not finite", async () => {
		const db = makeDB({ singlePartitionFastPath });
		const key = { hashKey: `operand-${crypto.randomUUID()}` };
		await db.putItem({ ...key, data: { big: 1e308 } });

		// An update never invents a value: a value that reads an absent path rejects the operation
		// instead of storing a null.
		const missingOperand = await writeOutcome(
			db.transactWriteItems({
				items: [
					{
						...key,
						operation: "update",
						update: [{ action: "set", target: { ref: "data", path: "$.copy" }, value: { ref: "data", path: "$.absent" } }],
					},
				],
			}),
		);
		expect(missingOperand).toMatchObject({
			outcome: "cancelled",
			results: [{ outcome: "rejected", reason: { code: "update_not_applicable", hashKey: key.hashKey } }],
		});

		// JSON holds no Infinity and no NaN, so an arithmetic result that is not finite rejects too.
		const overflow = await writeOutcome(
			db.transactWriteItems({
				items: [
					{
						...key,
						operation: "update",
						update: [
							{
								action: "set",
								target: { ref: "data", path: "$.big" },
								value: {
									fn: "*",
									args: [
										{ ref: "data", path: "$.big" },
										{ ref: "data", path: "$.big" },
									],
								},
							},
						],
					},
				],
			}),
		);
		expect(overflow).toMatchObject({
			outcome: "cancelled",
			results: [{ outcome: "rejected", reason: { code: "update_not_applicable", hashKey: key.hashKey } }],
		});

		// Neither rejection wrote anything.
		await expect(db.getItem(key)).resolves.toMatchObject({ found: true, item: { version: 1, data: { big: 1e308 } } });
	});

	it("remove on missing path is a no-op that still increments version", async () => {
		const db = makeDB({ singlePartitionFastPath });
		const key = { hashKey: `noop-${crypto.randomUUID()}` };
		await db.putItem({ ...key, data: { existing: "value" } });

		const update: UpdateExpression = [{ action: "remove", target: { ref: "data", path: "$.absent" } }];

		const res = await writeOutcome(
			db.transactWriteItems({
				items: [{ ...key, operation: "update", update }],
			}),
		);
		expect(res.outcome).toBe("committed");

		await expect(db.getItem(key)).resolves.toMatchObject({
			found: true,
			item: {
				version: 2,
				data: { existing: "value" },
			},
		});
	});

	it("preserves pre-image TTL when ttlAt is omitted, and replaces TTL when ttlAt is provided", async () => {
		const db = makeDB({ singlePartitionFastPath });
		const key1 = { hashKey: `ttl-preserve-${crypto.randomUUID()}` };
		const key2 = { hashKey: `ttl-replace-${crypto.randomUUID()}` };

		await db.putItem({ ...key1, data: { count: 1 }, ttlAt: 12345 });
		await db.putItem({ ...key2, data: { count: 1 }, ttlAt: 12345 });

		// Omit ttlAt: TTL preserved
		await writeOutcome(
			db.transactWriteItems({
				items: [
					{
						...key1,
						operation: "update",
						update: [{ action: "set", target: { ref: "data", path: "$.count" }, value: { val: 2 } }],
					},
				],
			}),
		);
		await expect(db.getItem(key1)).resolves.toMatchObject({
			found: true,
			item: { ttlAt: 12345 },
		});

		// Provide ttlAt: TTL replaced
		await writeOutcome(
			db.transactWriteItems({
				items: [
					{
						...key2,
						operation: "update",
						ttlAt: 99999,
						update: [{ action: "set", target: { ref: "data", path: "$.count" }, value: { val: 2 } }],
					},
				],
			}),
		);
		await expect(db.getItem(key2)).resolves.toMatchObject({
			found: true,
			item: { ttlAt: 99999 },
		});
	});

	it("rejects transaction with item_too_large when update result exceeds MAX_ITEM_BYTES", async () => {
		const db = makeDB({ singlePartitionFastPath });
		const key = { hashKey: `large-${crypto.randomUUID()}` };
		await db.putItem({ ...key, data: { str: "small" } });

		const update: UpdateExpression = [
			{ action: "set", target: { ref: "data", path: "$.str" }, value: { val: "x".repeat(MAX_ITEM_BYTES + 10) } },
		];

		const res = await writeOutcome(
			db.transactWriteItems({
				items: [{ ...key, operation: "update", update }],
			}),
		);
		expect(res).toMatchObject({
			outcome: "cancelled",
			results: [{ outcome: "rejected", reason: { code: "item_too_large", hashKey: key.hashKey } }],
		});
	});

	it("supports idempotent retries and rejects reusing token with a different update", async () => {
		const db = makeDB({ singlePartitionFastPath });
		const key = { hashKey: `idemp-${crypto.randomUUID()}` };
		await db.putItem({ ...key, data: { counter: 0 } });

		const token = `tok-${crypto.randomUUID()}`;
		const update1: UpdateExpression = [{ action: "set", target: { ref: "data", path: "$.counter" }, value: { val: 1 } }];
		const update2: UpdateExpression = [{ action: "set", target: { ref: "data", path: "$.counter" }, value: { val: 2 } }];

		// First execution
		const res1 = await writeOutcome(
			db.transactWriteItems({
				clientRequestToken: token,
				items: [{ ...key, operation: "update", update: update1 }],
			}),
		);
		expect(res1.outcome).toBe("committed");

		// Idempotent retry with identical token and operation returns same outcome
		const res2 = await writeOutcome(
			db.transactWriteItems({
				clientRequestToken: token,
				items: [{ ...key, operation: "update", update: update1 }],
			}),
		);
		expect(res2).toEqual(res1);

		// Reusing token with different update is rejected
		await expect(
			writeOutcome(
				db.transactWriteItems({
					clientRequestToken: token,
					items: [{ ...key, operation: "update", update: update2 }],
				}),
			),
		).rejects.toThrow();
	});

	it("executes mixed atomic transaction with put, update, delete, and check across partitions", async () => {
		const db = makeDB({ singlePartitionFastPath });
		const kPut = { hashKey: `mixed-put-${crypto.randomUUID()}` };
		const kUpdate = { hashKey: `mixed-upd-${crypto.randomUUID()}` };
		const kDel = { hashKey: `mixed-del-${crypto.randomUUID()}` };
		const kCheck = { hashKey: `mixed-chk-${crypto.randomUUID()}` };

		await db.putItem({ ...kUpdate, data: { num: 10 } });
		await db.putItem({ ...kDel, data: { dead: true } });
		await db.putItem({ ...kCheck, data: { verified: true } });

		const update: UpdateExpression = [
			{
				action: "set",
				target: { ref: "data", path: "$.num" },
				value: { fn: "+", args: [{ ref: "data", path: "$.num" }, { val: 5 }] },
			},
		];

		const res = await writeOutcome(
			db.transactWriteItems({
				items: [
					{ ...kPut, operation: "put", data: { created: true } },
					{ ...kUpdate, operation: "update", update },
					{ ...kDel, operation: "delete" },
					{ ...kCheck, operation: "check", condition: { op: "eq", args: [{ ref: "data", path: "$.verified" }, { val: true }] } },
				],
			}),
		);
		expect(res.outcome).toBe("committed");

		await expect(db.getItem(kPut)).resolves.toMatchObject({ found: true, item: { data: { created: true } } });
		await expect(db.getItem(kUpdate)).resolves.toMatchObject({ found: true, item: { data: { num: 15 } } });
		await expect(db.getItem(kDel)).resolves.toMatchObject({ found: false });
		await expect(db.getItem(kCheck)).resolves.toMatchObject({ found: true });
	});
});
