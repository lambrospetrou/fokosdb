import { describe, expect, it } from "vitest";
import {
	FokosConditionCheckError,
	FokosError,
	FokosExpressionError,
	type FokosAnyError,
	type FokosErrorCode,
} from "../src/shared/errors.js";
import { CURSOR_VERSION, encodeCursor } from "../src/shared/query/cursor.js";
import { MAX_ITEM_BYTES, MAX_ITEMS_PER_TX } from "../src/shared/transaction-limits.js";
import type { ConditionExpression } from "../src/shared/types.js";
import { makeDB } from "./transactions/tx-helpers.js";

/**
 * The public API raises a FokosError with a contractual category and code for every caller fault it
 * detects before it reaches a partition.
 */

async function errorOf(call: () => unknown): Promise<FokosAnyError> {
	try {
		await call();
	} catch (e) {
		expect(FokosError.is(e), String(e)).toBe(true);
		return e as FokosAnyError;
	}
	throw new Error("the call did not throw");
}

const circular: Record<string, unknown> = {};
circular.self = circular;

// A condition over a function that does not exist.
const invalidCondition = { op: "fn", name: "no_such_function", args: [] } as unknown as ConditionExpression;

describe("the validation codes of the item operations", () => {
	const db = makeDB();

	const cases: Array<[string, () => unknown, FokosErrorCode]> = [
		["an empty hash key", () => db.putItem({ hashKey: "", data: "x" }), "hash_key_empty"],
		["an empty sort key", () => db.putItem({ hashKey: "h", sortKey: "", data: "x" }), "sort_key_empty"],
		["a NUL in a key", () => db.getItem({ hashKey: "a\0b" }), "key_contains_nul"],
		["a lone surrogate in a key", () => db.deleteItem({ hashKey: "\uD800" }), "key_not_well_formed_utf16"],
		["a hash key over its cap", () => db.getItem({ hashKey: "x".repeat(1025) }), "hash_key_too_large"],
		["a sort key over its cap", () => db.getItem({ hashKey: "h", sortKey: "x".repeat(513) }), "sort_key_too_large"],
		["data of the wrong type", () => db.putItem({ hashKey: "h", data: 42 as unknown as string }), "item_data_wrong_type"],
		["data that is not JSON-serializable", () => db.putItem({ hashKey: "h", data: circular as never }), "item_data_not_json_serializable"],
		["data over the item cap", () => db.putItem({ hashKey: "h", data: "x".repeat(MAX_ITEM_BYTES + 1) }), "item_data_too_large"],
		["a ttlAt that is not an integer", () => db.putItem({ hashKey: "h", data: "x", ttlAt: 1.5 }), "ttl_at_invalid"],
		["a ttlAt that is not above zero", () => db.putItem({ hashKey: "h", data: "x", ttlAt: 0 }), "ttl_at_invalid"],
		[
			"an unknown returnValuesOnConditionCheckFailure",
			() => db.deleteItem({ hashKey: "h", returnValuesOnConditionCheckFailure: "all" as "all_old" }),
			"return_values_option_invalid",
		],
		["no queries", () => db.queryItems({ queries: [] }), "query_queries_empty"],
		["a query limit of zero", () => db.queryItems({ queries: [{ hashKey: "h" }], limit: 0 }), "query_limit_invalid"],
		["a negative maxPageBytes", () => db.queryItems({ queries: [{ hashKey: "h" }], maxPageBytes: -1 }), "query_max_page_bytes_invalid"],
		[
			"an empty sort-key bound",
			() => db.queryItems({ queries: [{ hashKey: "h", sortKeyCondition: { op: "eq", value: "" } }] }),
			"key_encode_empty",
		],
		["a cursor that is not base64url JSON", () => db.queryItems({ queries: [{ hashKey: "h" }], cursor: "!!!" }), "cursor_malformed"],
		["a cursor that decodes to null", () => db.queryItems({ queries: [{ hashKey: "h" }], cursor: "bnVsbA" }), "cursor_malformed"],
		[
			"a cursor of an unknown version",
			() =>
				db.queryItems({ queries: [{ hashKey: "h" }], cursor: new TextEncoder().encode('{"v":999}').toBase64({ alphabet: "base64url" }) }),
			"cursor_version_unknown",
		],
		[
			"a cursor whose query index is out of range",
			() =>
				db.queryItems({
					queries: [{ hashKey: "h" }],
					cursor: encodeCursor({ version: CURSOR_VERSION, direction: "fwd", fingerprint: 0n, queryIdx: 3, inner: null }),
				}),
			"cursor_query_index_out_of_range",
		],
		["no transaction items", () => db.transactWriteItems({ items: [] }), "transact_items_empty"],
		[
			"too many transaction items",
			() => db.transactGetItems({ items: Array.from({ length: MAX_ITEMS_PER_TX + 1 }, (_, i) => ({ hashKey: `h${i}` })) }),
			"transact_items_too_many",
		],
		[
			"a duplicate transaction key",
			() =>
				db.transactWriteItems({
					items: [
						{ operation: "put", hashKey: "h", data: "x" },
						{ operation: "delete", hashKey: "h" },
					],
				}),
			"transact_duplicate_key",
		],
		[
			"a delete that carries data",
			() => db.transactWriteItems({ items: [{ operation: "delete", hashKey: "h", data: "x" } as never] }),
			"transact_operation_fields_invalid",
		],
		[
			"an empty clientRequestToken",
			() => db.transactWriteItems({ clientRequestToken: " ", items: [{ operation: "put", hashKey: "h", data: "x" }] }),
			"client_request_token_invalid",
		],
		["a numTxCoordinators of zero", () => makeDB({ numTxCoordinators: 0 }), "num_tx_coordinators_invalid"],
	];

	it.each(cases)("reports %s as a validation error", async (_name, call, code) => {
		const err = await errorOf(call);
		expect([err._tag, err.code, err.origin, err.httpStatusHint]).toEqual(["FokosValidationError", code, "caller", 400]);
		expect(err.message.startsWith(`fokos/${code}: `)).toBe(true);
	});

	it("reports a cursor of another request as a validation error", async () => {
		const hashKey = `cursor-${crypto.randomUUID()}`;
		await db.putItem({ hashKey, sortKey: "a", data: "x" });
		await db.putItem({ hashKey, sortKey: "b", data: "x" });
		const { cursor } = await db.queryItems({ queries: [{ hashKey }], limit: 1 });
		expect(cursor).toBeDefined();

		const reversed = await errorOf(() => db.queryItems({ queries: [{ hashKey, scanIndexForward: false }], cursor }));
		expect(reversed.code).toBe("cursor_direction_mismatch");
		const other = await errorOf(() => db.queryItems({ queries: [{ hashKey: `${hashKey}-other` }], cursor }));
		expect(other.code).toBe("cursor_fingerprint_mismatch");
	});

	it("puts the dynamic detail in the attributes, not in the message", async () => {
		const err = await errorOf(() =>
			db.transactWriteItems({
				items: [
					{ operation: "put", hashKey: "a", data: "x" },
					{ operation: "put", hashKey: "h", sortKey: "s", data: "x" },
					{ operation: "delete", hashKey: "h", sortKey: "s" },
				],
			}),
		);
		expect(err.code).toBe("transact_duplicate_key");
		expect(err.attributes).toEqual({ opIndex: 2, hashKey: "h", sortKey: "s" });
		expect(err.message).toBe("fokos/transact_duplicate_key: transactWriteItems duplicate key");
	});
});

describe("the expression codes of the item operations", () => {
	const db = makeDB();

	it.each([
		["putItem", () => db.putItem({ hashKey: "h", data: "x", condition: invalidCondition })],
		["deleteItem", () => db.deleteItem({ hashKey: "h", condition: invalidCondition })],
		["transactWriteItems", () => db.transactWriteItems({ items: [{ operation: "check", hashKey: "h", condition: invalidCondition }] })],
	])("wraps an expression that does not compile in %s, and keeps the original as cause", async (_name, call) => {
		const err = await errorOf(call);
		expect(FokosExpressionError.is(err)).toBe(true);
		expect([err.code, err.origin, err.httpStatusHint]).toEqual(["expression_invalid", "caller", 400]);
		expect((err.cause as Error).name).toBe("ExpressionError");
		expect(err.attributes.expressionCode).toBe((err.cause as { code: string }).code);
	});
});

describe("a failed condition of putItem and deleteItem", () => {
	it.each(["putItem", "deleteItem"] as const)("raises FokosConditionCheckError from %s with the reason and the meta", async (api) => {
		const db = makeDB();
		const key = { hashKey: `cond-${crypto.randomUUID()}`, sortKey: "s" };
		await db.putItem({ ...key, data: "stored" });
		const condition: ConditionExpression = { op: "eq", args: [{ ref: "v" }, { val: 999 }] };

		const err = await errorOf(() =>
			api === "putItem"
				? db.putItem({ ...key, data: "new", condition, returnValuesOnConditionCheckFailure: "all_old" })
				: db.deleteItem({ ...key, condition, returnValuesOnConditionCheckFailure: "all_old" }),
		);

		expect(FokosConditionCheckError.is(err)).toBe(true);
		const conditionErr = err as FokosConditionCheckError;
		expect([conditionErr.code, conditionErr.origin, conditionErr.httpStatusHint]).toEqual(["condition_failed", "caller", 409]);
		expect(conditionErr.attributes).toEqual(key);
		expect(conditionErr.reason).toEqual({ type: "condition_failed", ...key, item: { ...key, data: "stored", kind: "text", version: 1 } });
		expect(conditionErr.meta.rowsRead).toBeGreaterThan(0);
		expect(conditionErr.meta).not.toHaveProperty("_internal");
		for (const field of ["reason", "meta"]) expect(Object.hasOwn(conditionErr, field)).toBe(true);
	});

	it("keeps the reason and the meta in fromWire, as own data", async () => {
		const db = makeDB();
		const key = { hashKey: `cond-wire-${crypto.randomUUID()}` };
		const err = (await errorOf(() =>
			db.deleteItem({ ...key, condition: { op: "exists", args: [{ ref: "hashKey" }] } }),
		)) as FokosConditionCheckError;
		const copy = Object.assign(new Error(err.message), { ...err });

		const back = FokosError.fromWire(copy as FokosAnyError) as FokosConditionCheckError;
		expect(back).toBeInstanceOf(FokosConditionCheckError);
		expect([back.error_id, back.reason, back.meta]).toEqual([err.error_id, err.reason, err.meta]);
	});
});
