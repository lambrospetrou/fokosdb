import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { partitionStubByName } from "../../src/shared/do-stubs.js";
import { FokosError } from "../../src/shared/errors.js";
import { FokosTransactionCancelledError } from "../../src/shared/errors-operations.js";
import { KeyCodec } from "../../src/shared/partition-topology/key-codec.js";
import type { ConditionExpression } from "../../src/shared/types.js";
import { keysInOnePartition, makeDB, type Key } from "./tx-helpers.js";

/**
 * A transaction raises the same exception, category, and code for one failure on the single-partition
 * fast path and on the two-phase path. Placement is invisible to the caller, and a split changes it, so
 * the error must not depend on it.
 */

const PATHS = [
	["the fast path", {}],
	["the two-phase path", { singlePartitionFastPath: false }],
] as const;

async function errorOf(call: () => Promise<unknown>): Promise<FokosError> {
	try {
		await call();
	} catch (e) {
		expect(FokosError.is(e), String(e)).toBe(true);
		return e as FokosError;
	}
	throw new Error("the call did not throw");
}

const itemAbsent: ConditionExpression = { op: "exists", args: [{ ref: "hashKey" }] };

describe.each(PATHS)("transactWriteItems on %s", (_path, pathOptions) => {
	it("reports invalid input as a validation error", async () => {
		const db = makeDB(pathOptions);
		const err = await errorOf(() =>
			db.transactWriteItems({
				items: [
					{ operation: "put", hashKey: "dup", data: "a" },
					{ operation: "delete", hashKey: "dup" },
				],
			}),
		);
		expect([err._tag, err.code]).toEqual(["FokosValidationError", "transact_duplicate_key"]);
	});

	it("reports an expression that does not compile as an expression error", async () => {
		const db = makeDB(pathOptions);
		const condition = { op: "fn", name: "no_such_function", args: [] } as unknown as ConditionExpression;
		const err = await errorOf(() => db.transactWriteItems({ items: [{ operation: "check", hashKey: "expr", condition }] }));
		expect([err._tag, err.code]).toEqual(["FokosExpressionError", "expression_invalid"]);
	});

	it("reports a failed premise as a cancel, with the reason of the operation in its results entry", async () => {
		const db = makeDB(pathOptions);
		const keys = keysInOnePartition(db, 2, `parity-premise-${crypto.randomUUID()}`);

		const err = await errorOf(() =>
			db.transactWriteItems({
				items: [
					{ operation: "put", ...keys[0], data: "a" },
					{ operation: "check", ...keys[1], condition: itemAbsent },
				],
			}),
		);

		expect(FokosTransactionCancelledError.is(err)).toBe(true);
		expect(err).toMatchObject({
			code: "transaction_cancelled",
			origin: "caller",
			httpStatusHint: 409,
			results: [{ outcome: "passed" }, { outcome: "rejected", reason: { code: "condition_failed", ...keys[1] } }],
			attributes: { transactionId: expect.any(String), idempotencyToken: expect.any(String) },
		});
		expect(Object.hasOwn(err, "results")).toBe(true);
		// A cancelled transaction applied nothing.
		expect((await db.getItem(keys[0])).found).toBe(false);
	});
});

describe.each(PATHS)("transactGetItems on %s", (_path, pathOptions) => {
	it("reports invalid input as a validation error", async () => {
		const err = await errorOf(() => makeDB(pathOptions).transactGetItems({ items: [] }));
		expect([err._tag, err.code]).toEqual(["FokosValidationError", "transact_items_empty"]);
	});

	it("reports an item with a pending write as pending_write", async () => {
		const db = makeDB(pathOptions);
		const keys = keysInOnePartition(db, 2, `parity-pending-${crypto.randomUUID()}`);
		const release = await lockItem(db, keys[0]);

		try {
			const err = await errorOf(() => db.transactGetItems({ items: keys }));
			expect([err._tag, err.code, err.origin, err.httpStatusHint]).toEqual(["FokosConflictError", "pending_write", "caller", 409]);
		} finally {
			await release();
		}
		await expect(db.transactGetItems({ items: keys })).resolves.toMatchObject({ outcome: "committed" });
	});
});

/** Holds a real transaction lock on `key` through the prepare RPC of its partition, and returns its release. */
async function lockItem(db: ReturnType<typeof makeDB>, key: Key): Promise<() => Promise<void>> {
	const hashKey = KeyCodec.encode(key.hashKey);
	const sortKey = KeyCodec.encode(key.sortKey);
	const { partitionContext } = db.options().topology.pickPartition(hashKey, sortKey);
	const stub = partitionStubByName(env.PARTITION_DO, partitionContext.doName);
	const transactionId = crypto.randomUUID().replaceAll("-", "");
	await stub.txPrepare(partitionContext, {
		transactionId,
		coordinatorDoId: env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString(),
		transactionTimestamp: Date.now(),
		items: [{ opIndex: 0, hashKey, sortKey, operation: "put", data: "held", kind: "text" }],
	});
	return async () => {
		await stub.txCancel(partitionContext, { transactionId, items: [{ hashKey, sortKey }] });
	};
}
