import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { FokosDB } from "../src/client/db.js";
import { partitionStubByName } from "../src/shared/do-stubs.js";
import { FokosConflictError, FokosError, FokosInternalError, FokosValidationError } from "../src/shared/errors.js";
import { isFokosAnyError } from "../src/shared/errors-operations.js";
import { KeyCodec } from "../src/shared/partition-topology/key-codec.js";
import { PartitionContextCreator, type PartitionNamespaceKey } from "../src/shared/partition-topology/partition-context.js";
import { PartitionTopologyRouterImpl } from "../src/shared/partition-topology/router.js";
import { MAX_ITEM_BYTES } from "../src/shared/transaction-limits.js";
import { makeDB } from "./transactions/tx-helpers.js";

/**
 * The errors a partition raises reach the caller of FokosDB as FokosErrors with their codes, and every
 * error that leaves FokosDB is a FokosError.
 */

async function errorOf(call: () => Promise<unknown>): Promise<FokosError> {
	try {
		await call();
	} catch (e) {
		expect(FokosError.is(e), String(e)).toBe(true);
		return e as FokosError;
	}
	throw new Error("the call did not throw");
}

describe("the errors of a partition, through the public API", () => {
	it("reports a write to an item that a transaction holds as item_locked_by_transaction", async () => {
		const db = makeDB();
		const key = { hashKey: `locked-${crypto.randomUUID()}`, sortKey: "s" };
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

		for (const call of [() => db.putItem({ ...key, data: "blocked" }), () => db.deleteItem(key)]) {
			const err = await errorOf(call);
			expect(FokosConflictError.is(err)).toBe(true);
			expect(err).toMatchObject({
				code: "item_locked_by_transaction",
				origin: "caller",
				httpStatusHint: 409,
				attributes: { transactionId, ...key },
			});
			// The transaction id is an internal identifier, so it stays out of the message.
			expect(err.message).not.toContain(transactionId);
		}

		await stub.txCancel(partitionContext, { transactionId, items: [{ hashKey, sortKey }] });
		await expect(db.putItem({ ...key, data: "free" })).resolves.toMatchObject({ version: 1 });
	});

	it("reports an item that the store measures over the cap as item_too_large, a caller fault", async () => {
		const db = makeDB();
		const hashKey = `over-row-${crypto.randomUUID()}`;

		const err = await errorOf(() => db.putItem({ hashKey, data: new Uint8Array(MAX_ITEM_BYTES) }));

		expect(FokosValidationError.is(err)).toBe(true);
		expect(err).toMatchObject({ code: "item_too_large", origin: "caller", httpStatusHint: 400, attributes: { hashKey } });
	});

	it("wraps an error that a partition does not classify as foreign_error, and keeps it as the cause", async () => {
		const db = makeDB();
		const hashKey = KeyCodec.encode("malformed");
		const { partitionContext } = db.options().topology.pickPartition(hashKey, KeyCodec.encodeOptional(undefined));
		const stub = partitionStubByName(env.PARTITION_DO, partitionContext.doName);

		// A request without a partition context fails inside the partition with a plain TypeError.
		const err = await errorOf(() => stub.apiGetItem(null as never, { hashKey, sortKey: KeyCodec.encodeOptional(undefined) }));

		expect(err).toMatchObject({ _tag: "FokosInternalError", code: "foreign_error" });
		expect(err.cause).toBeInstanceOf(Error);
	});
});

describe("the public methods of FokosDB", () => {
	it("wrap an error from outside the library as foreign_error", async () => {
		const topology = new PartitionTopologyRouterImpl(
			PartitionContextCreator.create({
				ns: "NO_SUCH_BINDING" as PartitionNamespaceKey,
				nsTx: "TRANSACTION_COORDINATOR_DO",
				tableName: `unbound.${crypto.randomUUID()}`,
				rootTreesN: 1,
				hashSplitN: 2,
				hashSplitConditions: { maxSizeMb: 100 },
			}),
		);
		const db = new FokosDB({ topology, transactionCoordinatorNs: env.TRANSACTION_COORDINATOR_DO });

		const err = await errorOf(() => db.getItem({ hashKey: "k" }));

		expect(FokosInternalError.is(err)).toBe(true);
		expect(isFokosAnyError(err)).toBe(true);
		expect(err).toMatchObject({ code: "foreign_error", origin: "internal", httpStatusHint: 500 });
		expect(err.cause).toBeInstanceOf(TypeError);
	});
});
