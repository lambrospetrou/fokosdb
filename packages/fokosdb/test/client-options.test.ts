/**
 * The client settings of `FokosDBOptions` (`retry` and `partitionMigratingRetryDeadlineMs`), and the key
 * size limits that a table sets in `FokosDBPolicy.limits`.
 */
import { describe, expect, it } from "vitest";
import { MAX_HASH_KEY_BYTES } from "../src/shared/transaction-limits.js";
import { fokosErrorWith } from "./errors-matchers.js";
import { controlledPartition, keysAcrossPartitions, makeDB, txCalls } from "./transactions/tx-helpers.js";

describe("FokosDB — client options", () => {
	it("defaults the retry settings", () => {
		expect(makeDB().options()).toMatchObject({
			retry: { baseDelayMs: 100, maxDelayMs: 2_000, maxAttempts: 5 },
			partitionMigratingRetryDeadlineMs: 15_000,
		});
		expect(makeDB({ retry: { maxAttempts: 2, baseDelayMs: undefined } }).options().retry).toEqual({
			baseDelayMs: 100,
			maxDelayMs: 2_000,
			maxAttempts: 2,
		});
	});

	it.each([
		[{ retry: { maxAttempts: 0 } }, "retry.maxAttempts"],
		[{ retry: { baseDelayMs: 1.5 } }, "retry.baseDelayMs"],
		[{ retry: { maxDelayMs: 0 } }, "retry.maxDelayMs"],
		[{ retry: { baseDelayMs: 2_000 } }, "retry.baseDelayMs"],
		[{ partitionMigratingRetryDeadlineMs: 0 }, "partitionMigratingRetryDeadlineMs"],
	])("rejects %o", (options, option) => {
		expect(() => makeDB(options)).toThrow(fokosErrorWith("fokosdb_options_invalid", { option }));
	});

	it("stops each read of a read transaction after retry.maxAttempts", async () => {
		const db = makeDB({ controlled: true, singlePartitionFastPath: false, retry: { maxAttempts: 2, baseDelayMs: 1, maxDelayMs: 2 } });
		const keys = keysAcrossPartitions(db, 2, "read-attempts");
		await controlledPartition(db, keys[0]).testTxResponse("txReadForTransaction", { error: "Network connection lost." });

		await expect(db.transactGetItems({ items: keys })).rejects.toThrow(
			expect.objectContaining({ cause: expect.objectContaining({ message: "Network connection lost." }) }),
		);
		expect(await txCalls(db, [keys[0]], "txReadForTransaction")).toHaveLength(2);
	});
});

describe("FokosDB — key size limits of a table", () => {
	it("accepts a hash key above the default limit when the table raises it, and a client with the defaults refuses it", async () => {
		const tableName = `limits.${crypto.randomUUID()}`;
		const db = makeDB({ tableName, limits: { maxHashKeyBytes: 2 * MAX_HASH_KEY_BYTES } });
		const hashKey = "h".repeat(MAX_HASH_KEY_BYTES + 100);

		await db.putItem({ hashKey, sortKey: "sk", data: "value" });
		await expect(db.getItem({ hashKey, sortKey: "sk" })).resolves.toMatchObject({ found: true, item: { data: "value" } });

		// The same table with the default limits: the client refuses the key before any RPC.
		const defaults = makeDB({ tableName });
		await expect(defaults.getItem({ hashKey, sortKey: "sk" })).rejects.toThrow(
			fokosErrorWith("hash_key_too_large", { limitBytes: MAX_HASH_KEY_BYTES }),
		);
	});
});
