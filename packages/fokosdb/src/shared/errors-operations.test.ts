import { describe, expect, it } from "vitest";
import { FokosError, FOKOS_ERROR_CATEGORIES } from "./errors.js";
import {
	CONDITION_CHECK_CODES,
	FOKOS_LIBRARY_CODE_TABLES,
	FokosConditionCheckError,
	FokosTransactionCancelledError,
	TRANSACTION_CANCELLED_CODES,
	isFokosAnyError,
} from "./errors-operations.js";
import type { RejectionReason, TransactWriteOperationResult } from "./transaction-types.js";

const DEFS = FOKOS_LIBRARY_CODE_TABLES.flatMap((table) => Object.values(table));

function cancelled(results: TransactWriteOperationResult[], options: { origin?: "service"; httpStatusHint?: number } = {}) {
	return new FokosTransactionCancelledError(TRANSACTION_CANCELLED_CODES.transaction_cancelled, {
		message: "transaction cancelled",
		results,
		...options,
	});
}

function rejected(reason: RejectionReason): TransactWriteOperationResult {
	return { outcome: "rejected", reason };
}

const conditionFailed = rejected({ code: "condition_failed", hashKey: "hk" });
const migrating = rejected({ code: "partition_migrating", hashKey: "hk", error_id: "e_4rpgyu_1" });

describe("the code tables of the library", () => {
	it("give every code a unique name and a unique segment, across every category", () => {
		expect(new Set(DEFS.map((def) => def.code)).size).toBe(DEFS.length);
		expect(new Set(DEFS.map((def) => def.segment)).size).toBe(DEFS.length);
	});

	it("hold one table for each category of the library", () => {
		const tags = FOKOS_LIBRARY_CODE_TABLES.map((table) => Object.values(table)[0].tag).sort();
		const categories = [...FOKOS_ERROR_CATEGORIES.keys(), FokosConditionCheckError.tag, FokosTransactionCancelledError.tag];
		expect(tags).toEqual(categories.sort());
	});
});

describe("the categories of errors-operations", () => {
	const condition = new FokosConditionCheckError(CONDITION_CHECK_CODES.condition_failed, {
		message: "condition failed",
		reason: { code: "condition_failed", hashKey: "hk" },
		meta: { rowsRead: 1, rowsWritten: 0, servedByActorId: "a", servedByActorName: "n", forwardCount: 0 } as never,
	});
	const transaction = cancelled([conditionFailed]);

	it("declare no prototype member, and use the category as name, _tag and type", () => {
		for (const [e, Category, type] of [
			[condition, FokosConditionCheckError, "condition_check_error"],
			[transaction, FokosTransactionCancelledError, "transaction_cancelled_error"],
		] as const) {
			expect(Object.getOwnPropertyNames(Category.prototype)).toEqual(["constructor"]);
			expect([e.name, e._tag, e.type]).toEqual([Category.tag, Category.tag, type]);
			expect(isFokosAnyError(e)).toBe(true);
		}
	});

	it("carry their fields as own data properties", () => {
		for (const [e, fields] of [
			[condition, ["reason", "meta"]],
			[transaction, ["results"]],
		] as const) {
			for (const field of fields) expect(Object.hasOwn(e, field), field).toBe(true);
		}
	});
});

describe("FokosTransactionCancelledError", () => {
	it("is a service condition only when every rejected entry has a service code", () => {
		const clockSkew = rejected({ code: "clock_skew", hashKey: "hk", serverTimestampMs: 1, transactionTimestampMs: 2 });
		expect(cancelled([migrating, clockSkew, { outcome: "passed" }])).toMatchObject({ origin: "service", httpStatusHint: 503 });
	});

	it("puts a premise that must change first, then a defect, then a service condition", () => {
		const foreign = rejected({ code: "foreign_error", hashKey: "hk", error_id: "e_jvufz5_1" });
		expect(cancelled([migrating, conditionFailed])).toMatchObject({ origin: "caller", httpStatusHint: 409 });
		expect(cancelled([migrating, foreign])).toMatchObject({ origin: "internal", httpStatusHint: 500 });
		expect(cancelled([foreign, conditionFailed])).toMatchObject({ origin: "caller", httpStatusHint: 409 });
	});

	it("takes the hint of the first rejected entry of the deciding origin, in request order", () => {
		const tooLarge = rejected({ code: "item_too_large", hashKey: "hk" });
		expect(cancelled([tooLarge, conditionFailed])).toMatchObject({ origin: "caller", httpStatusHint: 400 });
		expect(cancelled([conditionFailed, tooLarge])).toMatchObject({ origin: "caller", httpStatusHint: 409 });
	});

	it("falls back to the defaults of transaction_cancelled, and keeps what the call site passes", () => {
		expect(cancelled([{ outcome: "not_evaluated" }])).toMatchObject({ origin: "caller", httpStatusHint: 409 });
		expect(cancelled([conditionFailed], { origin: "service", httpStatusHint: 503 })).toMatchObject({
			origin: "service",
			httpStatusHint: 503,
		});
	});

	it("keeps its results in fromWire from an error that crossed a hop, as the generic class of its category", () => {
		const e = cancelled([conditionFailed, migrating]);
		const copy = Object.assign(new Error(e.message), { ...e });
		const back = FokosError.fromWire(copy);
		expect(FokosTransactionCancelledError.is(back)).toBe(true);
		if (!FokosTransactionCancelledError.is(back)) throw new Error("unreachable");
		expect([back.error_id, back.results]).toEqual([e.error_id, e.results]);
	});
});
