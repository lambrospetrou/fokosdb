import { describe, it, expect } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import { MAX_ITEM_BYTES } from "../../src/shared/transaction-limits.js";
import {
	controlledCoordinator,
	controlledPartition,
	countDistinctPartitions,
	type Key,
	keysAcrossPartitions,
	keysInOnePartition,
	makeDB,
	txCalls,
	writeOutcome,
} from "./tx-helpers.js";
import { FokosTransactionCancelledError } from "../../src/shared/errors-operations.js";

/** The answer a partition gives when it cannot execute the whole item set alone, in its envelope. */
const self = {
	ref: { partitionId: "00", doName: "stand-in" },
	actorId: "stand-in",
	hashDepth: 0,
	rangeDepth: 0,
	role: "executed" as const,
};
const fastPathNotApplicable = {
	value: { outcome: "not_applicable" as const },
	routing: { servedBy: [self], forwardCount: 0, servedByTruncated: false },
};

/**
 * The single-partition fast path answers a transaction from the owning partition in one round trip,
 * with no coordinator. These tests pin WHICH path a key set takes — routing is a pure hash of the
 * key bytes, so it is fixed, never flaky — and assert that both paths give the same answer.
 */
describe("transactions - single-partition fast path", () => {
	// One table per write path serves the whole file: every test writes keys of its own, so the
	// partition DOs stay warm instead of cold-starting a fresh set per test. The root count stays
	// small so the keys keep landing on those warm roots; a test that needs a different size cap
	// creates a database of its own below.
	const sharedDb = makeDB({ rootTreesN: 8, controlled: true });
	const sharedSlowDb = makeDB({ rootTreesN: 8, singlePartitionFastPath: false, controlled: true });

	/**
	 * Counts the RPCs that each path makes from now on, so a test can assert which one ran. It counts
	 * on the partitions of `keys` and on the coordinator of each client. The tests of this file share
	 * warm partitions, thus each count is the change since this call.
	 */
	async function countCalls(dbs: FokosDB[], keys: Key[]) {
		const read = async () => {
			const counts = { txReadSnapshot: 0, txReadForTransaction: 0, txExecuteSingleShot: 0, initiateWrite: 0 };
			for (const db of dbs) {
				counts.txReadSnapshot += (await txCalls(db, keys, "txReadSnapshot")).length;
				counts.txReadForTransaction += (await txCalls(db, keys, "txReadForTransaction")).length;
				counts.txExecuteSingleShot += (await txCalls(db, keys, "txExecuteSingleShot")).length;
				counts.initiateWrite += await controlledCoordinator(db).testInitiateWriteCalls();
			}
			return counts;
		};
		const start = await read();
		return async () => {
			const now = await read();
			return {
				txReadSnapshot: now.txReadSnapshot - start.txReadSnapshot,
				txReadForTransaction: now.txReadForTransaction - start.txReadForTransaction,
				txExecuteSingleShot: now.txExecuteSingleShot - start.txExecuteSingleShot,
				initiateWrite: now.initiateWrite - start.initiateWrite,
			};
		};
	}

	it("reads a single-partition key set in one round trip, and answers exactly as the two-phase driver does", async () => {
		const db = sharedDb;
		const slowDb = sharedSlowDb;

		const keys = keysInOnePartition(db, 3, "fast-read");
		expect(countDistinctPartitions(db, keys)).toBe(1);
		// Only two of the three are written, so an absent key is checked on both paths as well.
		for (const key of keys.slice(0, 2)) {
			await db.putItem({ ...key, data: `data-${key.hashKey}` });
			await slowDb.putItem({ ...key, data: `data-${key.hashKey}` });
		}

		const calls = await countCalls([db, slowDb], keys);
		const fast = await db.transactGetItems({ items: keys });
		expect(await calls()).toMatchObject({ txReadSnapshot: 1, txReadForTransaction: 0 });

		// The option off pins the other path for the same key set, and both answers must agree
		// item by item, in request order.
		const slow = await slowDb.transactGetItems({ items: keys });
		expect(await calls()).toMatchObject({ txReadSnapshot: 1, txReadForTransaction: 2 });
		expect(fast).toEqual(slow);
		expect(fast.items.map((i) => i.found)).toEqual([true, true, false]);
	});

	it("reads projected items in one round trip, and answers exactly as the two-phase driver does", async () => {
		const db = sharedDb;
		const slowDb = sharedSlowDb;

		const keys = keysInOnePartition(db, 2, "fast-read-proj");
		expect(countDistinctPartitions(db, keys)).toBe(1);
		for (const key of keys) {
			await db.putItem({ ...key, data: { n: 1, tag: key.hashKey } });
			await slowDb.putItem({ ...key, data: { n: 1, tag: key.hashKey } });
		}

		// A projected item and a complete item in one request: each returns its own shape, in request order.
		const items = [{ ...keys[0], projection: [{ expr: { ref: "data", path: "$.tag" }, as: "tag" }] as const }, { ...keys[1] }];
		const calls = await countCalls([db, slowDb], keys);
		const fast = await db.transactGetItems({ items });
		expect(await calls()).toMatchObject({ txReadSnapshot: 1, txReadForTransaction: 0 });

		const slow = await slowDb.transactGetItems({ items });
		expect(await calls()).toMatchObject({ txReadSnapshot: 1, txReadForTransaction: 2 });
		expect(fast).toEqual(slow);
		expect(fast.items[0]).toMatchObject({
			found: true,
			hashKey: keys[0].hashKey,
			data: { tag: keys[0].hashKey },
			kind: "projected",
			version: 1,
		});
		expect(fast.items[1]).toMatchObject({ found: true, data: { n: 1, tag: keys[1].hashKey }, kind: "json", version: 1 });
		// The envelope is uniform: `data` and `kind` are reachable on every found element, projected or not.
		expect(fast.items.filter((i) => i.found).map((i) => (i.found ? i.kind : null))).toEqual(["projected", "json"]);
	});

	it("rejects a request in which two items name the same key", async () => {
		const db = sharedDb;
		const key = keysInOnePartition(db, 1, "dup-read")[0];
		await db.putItem({ ...key, data: "v" });

		const calls = await countCalls([db], [key]);
		await expect(db.transactGetItems({ items: [key, { ...key, projection: [{ expr: { ref: "data" } }] }] })).rejects.toThrow(
			expect.objectContaining({ code: "transact_duplicate_key" }),
		);
		expect(await calls()).toMatchObject({ txReadSnapshot: 0, txReadForTransaction: 0 });
	});

	it("drives a multi-partition read from the Worker in two phases", async () => {
		const db = sharedDb;
		const keys = keysAcrossPartitions(db, 2, "span");
		expect(countDistinctPartitions(db, keys)).toBe(2);
		for (const key of keys) await db.putItem({ ...key, data: "v" });

		const calls = await countCalls([db], keys);
		const result = await db.transactGetItems({ items: keys });

		expect(result.items).toHaveLength(keys.length);
		expect(await calls()).toMatchObject({ txReadSnapshot: 0, txReadForTransaction: 4 });
	});

	it("runs the Worker two-phase path when the partition cannot execute the whole set", async () => {
		const db = sharedDb;
		const keys = keysInOnePartition(db, 2, "fast-fallback");
		for (const key of keys) await db.putItem({ ...key, data: `data-${key.hashKey}` });

		// A partition answers this when the items straddle a split or a promotion below it. Standing in
		// for that setup here keeps the test on what db.ts owns: recognising the answer and finishing the
		// read through the two-phase path. The answer itself is covered in test/partition-do/.
		const calls = await countCalls([db], keys);
		await controlledPartition(db, keys[0]).testTxResponse("txReadSnapshot", { value: fastPathNotApplicable, times: 1 });

		const result = await db.transactGetItems({ items: keys });

		expect(await calls()).toMatchObject({ txReadSnapshot: 1, txReadForTransaction: 2 });
		expect(result.items.map((i) => i.found)).toEqual([true, true]);
		expect(result.items.map((i) => (i.found ? i.data : null))).toEqual(keys.map((k) => `data-${k.hashKey}`));
	});

	it("surfaces a fast-path transport failure instead of starting the two-phase path", async () => {
		const db = sharedDb;
		const keys = keysInOnePartition(db, 2, "fast-transport");
		for (const key of keys) await db.putItem({ ...key, data: "v" });

		const calls = await countCalls([db], keys);
		await controlledPartition(db, keys[0]).testTxResponse("txReadSnapshot", { error: "Network connection lost.", times: 1 });

		// The failure crosses the RPC hop, and db.ts wraps it with the original as the cause.
		await expect(db.transactGetItems({ items: keys })).rejects.toThrow(
			expect.objectContaining({ code: "foreign_error", cause: expect.objectContaining({ message: "Network connection lost." }) }),
		);
		expect(await calls()).toMatchObject({ txReadForTransaction: 0 });
	});

	it("writes a single-partition transaction in one round trip, with no coordinator", async () => {
		const db = sharedDb;
		const keys = keysInOnePartition(db, 3, "fast-write");
		await db.putItem({ ...keys[1], data: "to-delete" });
		await db.putItem({ ...keys[2], data: "to-check" });

		const calls = await countCalls([db], keys);
		const result = await writeOutcome(
			db.transactWriteItems({
				items: [
					{ ...keys[0], operation: "put", data: "written" },
					{ ...keys[1], operation: "delete" },
					{ ...keys[2], operation: "check", condition: { op: "exists", args: [{ ref: "hashKey" }] } },
				],
			}),
		);

		expect(await calls()).toMatchObject({ txExecuteSingleShot: 1, initiateWrite: 0 });
		// The public shape is the same on both paths, so a caller cannot tell which one ran.
		expect(result).toMatchObject({ transactionId: expect.any(String), idempotencyToken: expect.any(String) });

		await expect(db.getItem(keys[0])).resolves.toMatchObject({ found: true, item: { data: "written" } });
		await expect(db.getItem(keys[1])).resolves.toMatchObject({ found: false });
		await expect(db.getItem(keys[2])).resolves.toMatchObject({ found: true, item: { data: "to-check" } });
	});

	it("reports a failed condition as a cancelled transaction, writing nothing", async () => {
		const db = sharedDb;
		const keys = keysInOnePartition(db, 2, "fast-condition");

		const calls = await countCalls([db], keys);
		const result = await writeOutcome(
			db.transactWriteItems({
				items: [
					{ ...keys[0], operation: "put", data: "never" },
					{ ...keys[1], operation: "put", data: "never", condition: { op: "exists", args: [{ ref: "hashKey" }] } },
				],
			}),
		);

		expect(await calls()).toMatchObject({ txExecuteSingleShot: 1 });
		expect(result).toMatchObject({
			outcome: "cancelled",
			results: [{ outcome: "passed" }, { outcome: "rejected", reason: { code: "condition_failed", hashKey: keys[1].hashKey } }],
		});
		await expect(db.getItem(keys[0])).resolves.toMatchObject({ found: false });
	});

	it("keeps a transaction that carries a clientRequestToken on the coordinator path", async () => {
		// The coordinator stamps the transaction with its own clock, and a partition refuses a stamp that
		// is not above its last delete or read. A shared table has the deletes and reads of other tests,
		// and the clock can go back. Thus this test uses a table of its own.
		const db = makeDB({ rootTreesN: 8, controlled: true });
		const keys = keysInOnePartition(db, 2, "fast-token");
		const items = keys.map((key) => ({ ...key, operation: "put" as const, data: "tokened" }));
		const clientRequestToken = `fast-token-${crypto.randomUUID()}`;

		const calls = await countCalls([db], keys);
		const first = await writeOutcome(db.transactWriteItems({ items, clientRequestToken }));
		const replay = await writeOutcome(db.transactWriteItems({ items, clientRequestToken }));

		// A partition keeps no record of finished transactions, so only the coordinator's ledger can
		// answer the replay — which is why a token holds a transaction on that path.
		expect(await calls()).toMatchObject({ txExecuteSingleShot: 0, initiateWrite: 2 });
		expect(first.outcome).toBe("committed");
		expect(replay).toEqual(first);
	});

	it("reports a write past the size cap as a cancel with partition_over_size on its operation", async () => {
		// An empty SQLite database is already several KB, so this cap is exceeded before anything is
		// written and every write is refused for size.
		const db = makeDB({ maxSizeMb: 0.000_001 });
		const items = [{ hashKey: "over-size", operation: "put" as const, data: "d" }];

		const err = await db.transactWriteItems({ items }).catch((e: unknown) => e);
		expect(FokosTransactionCancelledError.is(err)).toBe(true);
		// The only failure is a full partition, which clears on its own, so the cancel is a service condition.
		expect(err).toMatchObject({
			code: "transaction_cancelled",
			origin: "service",
			httpStatusHint: 503,
			results: [
				{
					outcome: "rejected",
					reason: { code: "partition_over_size", hashKey: "over-size", error_id: expect.stringMatching(/^e_49j6ez_/) },
				},
			],
		});
	});

	it("refuses a write on the coordinator path when the coordinator is past the size cap", async () => {
		// A coordinator uses the hash split threshold of its table, so this cap refuses a new transaction
		// at the coordinator, before any partition sees it.
		const db = makeDB({ maxSizeMb: 0.000_001, singlePartitionFastPath: false });
		const items = [{ hashKey: "over-size", operation: "put" as const, data: "d" }];

		const err = await db.transactWriteItems({ items }).catch((e: unknown) => e);
		expect(err).toMatchObject({ code: "coordinator_over_size", origin: "service" });
	});

	it("runs the coordinator path for a write when the partition cannot execute the whole set", async () => {
		// The coordinator stamps the transaction with its own clock, and a partition refuses a stamp that
		// is not above its last delete or read. A shared table has the deletes and reads of other tests,
		// and the clock can go back. Thus this test uses a table of its own.
		const db = makeDB({ rootTreesN: 8, controlled: true });
		const keys = keysInOnePartition(db, 2, "fast-write-fallback");

		// A partition answers this when the items straddle a split or a promotion below it. The answer
		// itself is covered in test/partition-do/; what matters here is that db.ts recognises it and
		// finishes the write on the coordinator path.
		const calls = await countCalls([db], keys);
		await controlledPartition(db, keys[0]).testTxResponse("txExecuteSingleShot", { value: fastPathNotApplicable, times: 1 });

		const result = await writeOutcome(
			db.transactWriteItems({
				items: keys.map((key) => ({ ...key, operation: "put" as const, data: "via-coordinator" })),
			}),
		);

		expect(await calls()).toMatchObject({ txExecuteSingleShot: 1, initiateWrite: 1 });
		// The whole result, so that a cancel shows the reason of each operation.
		expect(result).toMatchObject({ outcome: "committed" });
		for (const key of keys) {
			await expect(db.getItem(key)).resolves.toMatchObject({ found: true, item: { data: "via-coordinator" } });
		}
	});
});

/**
 * Every write is measured before anything is written, so no apply pass and no commit can fail on a
 * size. The two paths reject at different moments and each moment has its own hazard: the two-phase
 * path must reject at prepare, because after "accepted" the coordinator is entitled to commit and
 * commit has no way to refuse; the single-shot path must reject before its apply loop, because
 * transactionSync keeps what a returning callback wrote.
 */
describe("transactions - the item size limit is enforced before any write", () => {
	// One table per write path serves the whole describe: every test writes keys of its own, so the
	// partition DOs stay warm instead of cold-starting a fresh set per test. The root count stays
	// small so the keys keep landing on those warm roots; a test that needs a different size cap
	// creates a database of its own below.
	const sharedDb = makeDB({ rootTreesN: 8 });
	const sharedSlowDb = makeDB({ rootTreesN: 8, singlePartitionFastPath: false });

	// The client counts data bytes; the store measures the data plus the keys plus the fixed per-row
	// overhead. This value sits between the two, so validation passes it to the partition.
	const overRow = () => new Uint8Array(MAX_ITEM_BYTES);

	it("rejects an oversized put at prepare and writes nothing on the two-phase path", async () => {
		const db = sharedSlowDb;
		const fits = { hashKey: `fits-${crypto.randomUUID()}` };
		const over = { hashKey: `over-${crypto.randomUUID()}` };

		const res = await writeOutcome(
			db.transactWriteItems({
				items: [
					{ ...fits, operation: "put", data: "written-first" },
					{ ...over, operation: "put", data: overRow() },
				],
			}),
		);

		expect(res).toMatchObject({
			outcome: "cancelled",
			results: [{ outcome: "passed" }, { outcome: "rejected", reason: { code: "item_too_large", hashKey: over.hashKey } }],
		});
		await expect(db.getItem(fits)).resolves.toMatchObject({ found: false });
		await expect(db.getItem(over)).resolves.toMatchObject({ found: false });
	});

	it("rejects an oversized put in the check pass and writes nothing on the single-shot path", async () => {
		const db = sharedDb;
		// One hash key, so the whole set lands in one partition and takes the single-shot path.
		const hashKey = `single-${crypto.randomUUID()}`;
		const fits = { hashKey, sortKey: "fits" };
		const over = { hashKey, sortKey: "over" };

		const res = await writeOutcome(
			db.transactWriteItems({
				items: [
					{ ...fits, operation: "put", data: "written-first" },
					{ ...over, operation: "put", data: overRow() },
				],
			}),
		);

		expect(res).toMatchObject({
			outcome: "cancelled",
			results: [{ outcome: "passed" }, { outcome: "rejected", reason: { code: "item_too_large", hashKey, sortKey: "over" } }],
		});
		await expect(db.getItem(fits)).resolves.toMatchObject({ found: false });
		await expect(db.getItem(over)).resolves.toMatchObject({ found: false });
	});

	// jsonb_set keeps a string element unescaped, while rendering it to text and re-parsing bakes the
	// escapes into the blob, so a JSONB-to-text-to-JSONB round trip grows the document. The pending row
	// therefore holds JSONB, and the bytes prepare measured are the bytes commit writes. Without that,
	// this update passes prepare on its unescaped size and then cannot be stored at commit.
	it("commits an update that fits unescaped but not escaped, on both write paths", async () => {
		// Fits the item limit as stored (one byte per backslash), and doubles if the escapes are baked in.
		const value = "\\".repeat(230_000);

		for (const singlePartitionFastPath of [true, false]) {
			const db = singlePartitionFastPath ? sharedDb : sharedSlowDb;
			const key = { hashKey: `escape-${crypto.randomUUID()}` };
			await db.putItem({ ...key, data: { k: "small" } });

			const res = await writeOutcome(
				db.transactWriteItems({
					items: [
						{ ...key, operation: "update", update: [{ action: "set", target: { ref: "data", path: "$.k" }, value: { val: value } }] },
					],
				}),
			);

			expect(res.outcome).toBe("committed");
			await expect(db.getItem(key)).resolves.toMatchObject({ found: true, item: { version: 2, data: { k: value } } });
		}
	});
});
