import { afterEach, describe, it, expect, vi } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import { TransactionCoordinatorDO } from "../../src/server/do-transaction-coordinator.js";
import invariant from "../../src/shared/invariant.js";
import { MAX_ITEM_BYTES } from "../../src/shared/transaction-limits.js";
import { countDistinctPartitions, keysInOnePartition, makeDB } from "./tx-helpers.js";

/**
 * The single-partition fast path answers a transaction from the owning partition in one round trip,
 * with no coordinator. These tests pin WHICH path a key set takes — routing is a pure hash of the
 * key bytes, so it is fixed, never flaky — and assert that both paths give the same answer.
 */
describe("transactions - single-partition fast path", () => {
	// The DO classes run in this same isolate, so a spy on their prototype counts the real RPC
	// dispatches — and stays installed until it is restored.
	afterEach(() => {
		vi.restoreAllMocks();
	});

	/** Counts the RPCs each read path makes, so a test can assert which one ran. */
	function countReadPathCalls() {
		const snapshotCalls = vi.spyOn(PartitionDO.prototype, "txReadSnapshot");
		const transactionCalls = vi.spyOn(PartitionDO.prototype, "txReadForTransaction");
		return { snapshotCalls, transactionCalls };
	}

	/** The same, for the write paths. */
	function countWritePathCalls() {
		const partitionCalls = vi.spyOn(PartitionDO.prototype, "txExecuteSingleShot");
		const coordinatorCalls = vi.spyOn(TransactionCoordinatorDO.prototype, "initiateWrite");
		return { partitionCalls, coordinatorCalls };
	}

	it("reads a single-partition key set in one round trip, and answers exactly as the two-phase driver does", async () => {
		const db = makeDB();
		const slowDb = makeDB({ singlePartitionFastPath: false });

		const keys = keysInOnePartition(db, 3, "fast-read");
		expect(countDistinctPartitions(db, keys)).toBe(1);
		// Only two of the three are written, so an absent key is checked on both paths as well.
		for (const key of keys.slice(0, 2)) {
			await db.putItem({ ...key, data: `data-${key.hashKey}` });
			await slowDb.putItem({ ...key, data: `data-${key.hashKey}` });
		}

		const { snapshotCalls, transactionCalls } = countReadPathCalls();
		const fast = await db.transactGetItems({ items: keys });
		expect(snapshotCalls).toHaveBeenCalledTimes(1);
		expect(transactionCalls).not.toHaveBeenCalled();

		// The option off pins the other path for the same key set, and both answers must agree
		// item by item, in request order.
		const slow = await slowDb.transactGetItems({ items: keys });
		expect(transactionCalls).toHaveBeenCalledTimes(2);
		expect(fast).toEqual(slow);
		invariant(fast.outcome === "committed");
		expect(fast.items.map((i) => i.found)).toEqual([true, true, false]);
	});

	it("drives a multi-partition read from the Worker in two phases", async () => {
		const db = makeDB();
		const keys = [
			{ hashKey: "span-a", sortKey: "sk" },
			{ hashKey: "span-b", sortKey: "sk" },
		];
		expect(countDistinctPartitions(db, keys)).toBe(2);
		for (const key of keys) await db.putItem({ ...key, data: "v" });

		const { snapshotCalls, transactionCalls } = countReadPathCalls();
		const result = await db.transactGetItems({ items: keys });

		expect(result.outcome).toBe("committed");
		expect(snapshotCalls).not.toHaveBeenCalled();
		expect(transactionCalls).toHaveBeenCalledTimes(4);
	});

	it("runs the Worker two-phase path when the partition cannot execute the whole set", async () => {
		const db = makeDB();
		const keys = keysInOnePartition(db, 2, "fast-fallback");
		for (const key of keys) await db.putItem({ ...key, data: `data-${key.hashKey}` });

		// A partition raises this when the items straddle a split or a promotion below it. Standing in
		// for that setup here keeps the test on what db.ts owns: recognising the sentinel and finishing
		// the read through the two-phase path. The raise itself is covered in test/partition-do/.
		const { snapshotCalls, transactionCalls } = countReadPathCalls();
		snapshotCalls.mockRejectedValue(new Error("fokos/partition: single-partition fast path not applicable (readSnapshot)."));

		const result = await db.transactGetItems({ items: keys });

		expect(snapshotCalls).toHaveBeenCalledTimes(1);
		expect(transactionCalls).toHaveBeenCalledTimes(2);
		invariant(result.outcome === "committed");
		expect(result.items.map((i) => i.found)).toEqual([true, true]);
		expect(result.items.map((i) => (i.found ? i.data : null))).toEqual(keys.map((k) => `data-${k.hashKey}`));
	});

	it("surfaces a fast-path transport failure instead of starting the two-phase path", async () => {
		const db = makeDB();
		const keys = keysInOnePartition(db, 2, "fast-transport");
		for (const key of keys) await db.putItem({ ...key, data: "v" });

		const { snapshotCalls, transactionCalls } = countReadPathCalls();
		snapshotCalls.mockRejectedValue(new Error("Network connection lost."));

		await expect(db.transactGetItems({ items: keys })).rejects.toThrow(/Network connection lost/);
		expect(transactionCalls).not.toHaveBeenCalled();
	});

	it("writes a single-partition transaction in one round trip, with no coordinator", async () => {
		const db = makeDB();
		const keys = keysInOnePartition(db, 3, "fast-write");
		await db.putItem({ ...keys[1], data: "to-delete" });
		await db.putItem({ ...keys[2], data: "to-check" });

		const { partitionCalls, coordinatorCalls } = countWritePathCalls();
		const result = await db.transactWriteItems({
			items: [
				{ ...keys[0], operation: "put", data: "written" },
				{ ...keys[1], operation: "delete" },
				{ ...keys[2], operation: "check", condition: { op: "exists", args: [{ ref: "hashKey" }] } },
			],
		});

		expect(partitionCalls).toHaveBeenCalledTimes(1);
		expect(coordinatorCalls).not.toHaveBeenCalled();
		// The public shape is the same on both paths, so a caller cannot tell which one ran.
		expect(result).toMatchObject({ outcome: "committed", transactionId: expect.any(String), idempotencyToken: expect.any(String) });

		await expect(db.getItem(keys[0])).resolves.toMatchObject({ found: true, item: { data: "written" } });
		await expect(db.getItem(keys[1])).resolves.toMatchObject({ found: false });
		await expect(db.getItem(keys[2])).resolves.toMatchObject({ found: true, item: { data: "to-check" } });
	});

	it("reports a failed condition as a cancelled transaction, writing nothing", async () => {
		const db = makeDB();
		const keys = keysInOnePartition(db, 2, "fast-condition");

		const { partitionCalls } = countWritePathCalls();
		const result = await db.transactWriteItems({
			items: [
				{ ...keys[0], operation: "put", data: "never" },
				{ ...keys[1], operation: "put", data: "never", condition: { op: "exists", args: [{ ref: "hashKey" }] } },
			],
		});

		expect(partitionCalls).toHaveBeenCalledTimes(1);
		expect(result).toMatchObject({ outcome: "cancelled", reason: { type: "condition_failed", hashKey: keys[1].hashKey } });
		await expect(db.getItem(keys[0])).resolves.toMatchObject({ found: false });
	});

	it("keeps a transaction that carries a clientRequestToken on the coordinator path", async () => {
		const db = makeDB();
		const keys = keysInOnePartition(db, 2, "fast-token");
		const items = keys.map((key) => ({ ...key, operation: "put" as const, data: "tokened" }));
		const clientRequestToken = `fast-token-${crypto.randomUUID()}`;

		const { partitionCalls, coordinatorCalls } = countWritePathCalls();
		const first = await db.transactWriteItems({ items, clientRequestToken });
		const replay = await db.transactWriteItems({ items, clientRequestToken });

		// A partition keeps no record of finished transactions, so only the coordinator's ledger can
		// answer the replay — which is why a token holds a transaction on that path.
		expect(partitionCalls).not.toHaveBeenCalled();
		expect(coordinatorCalls).toHaveBeenCalledTimes(2);
		expect(first.outcome).toBe("committed");
		expect(replay).toEqual(first);
	});

	it("reports a write past the size cap as cancelled with transient_error, on both paths", async () => {
		// An empty SQLite database is already several KB, so this cap is exceeded before anything is
		// written and every write is refused for size.
		const overSize = { maxSizeMb: 0.000_001 };
		const items = [{ hashKey: "over-size", operation: "put" as const, data: "d" }];

		const fast = await makeDB(overSize).transactWriteItems({ items });
		const slow = await makeDB({ ...overSize, singlePartitionFastPath: false }).transactWriteItems({ items });

		expect(fast).toMatchObject({ outcome: "cancelled", reason: { type: "transient_error" } });
		expect(slow).toMatchObject({ outcome: "cancelled", reason: { type: "transient_error" } });
	});

	it("runs the coordinator path for a write when the partition cannot execute the whole set", async () => {
		const db = makeDB();
		const keys = keysInOnePartition(db, 2, "fast-write-fallback");

		// A partition raises this when the items straddle a split or a promotion below it. The raise
		// itself is covered in test/partition-do/; what matters here is that db.ts recognises it and
		// finishes the write on the coordinator path.
		const { partitionCalls, coordinatorCalls } = countWritePathCalls();
		partitionCalls.mockRejectedValue(new Error("fokos/partition: single-partition fast path not applicable (executeSingleShot)."));

		const result = await db.transactWriteItems({
			items: keys.map((key) => ({ ...key, operation: "put" as const, data: "via-coordinator" })),
		});

		expect(partitionCalls).toHaveBeenCalledTimes(1);
		expect(coordinatorCalls).toHaveBeenCalledTimes(1);
		expect(result.outcome).toBe("committed");
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
	// The client counts data bytes; the store measures the data plus the keys plus the fixed per-row
	// overhead. This value sits between the two, so validation passes it to the partition.
	const overRow = () => new Uint8Array(MAX_ITEM_BYTES);

	it("rejects an oversized put at prepare and writes nothing on the two-phase path", async () => {
		const db = makeDB({ singlePartitionFastPath: false });
		const fits = { hashKey: `fits-${crypto.randomUUID()}` };
		const over = { hashKey: `over-${crypto.randomUUID()}` };

		const res = await db.transactWriteItems({
			items: [
				{ ...fits, operation: "put", data: "written-first" },
				{ ...over, operation: "put", data: overRow() },
			],
		});

		expect(res).toMatchObject({ outcome: "cancelled", reason: { type: "item_too_large", hashKey: over.hashKey } });
		await expect(db.getItem(fits)).resolves.toMatchObject({ found: false });
		await expect(db.getItem(over)).resolves.toMatchObject({ found: false });
	});

	it("rejects an oversized put in the check pass and writes nothing on the single-shot path", async () => {
		const db = makeDB();
		// One hash key, so the whole set lands in one partition and takes the single-shot path.
		const hashKey = `single-${crypto.randomUUID()}`;
		const fits = { hashKey, sortKey: "fits" };
		const over = { hashKey, sortKey: "over" };

		const res = await db.transactWriteItems({
			items: [
				{ ...fits, operation: "put", data: "written-first" },
				{ ...over, operation: "put", data: overRow() },
			],
		});

		expect(res).toMatchObject({ outcome: "cancelled", reason: { type: "item_too_large", hashKey, sortKey: "over" } });
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
			const db = makeDB({ singlePartitionFastPath });
			const key = { hashKey: `escape-${crypto.randomUUID()}` };
			await db.putItem({ ...key, data: { k: "small" } });

			const res = await db.transactWriteItems({
				items: [{ ...key, operation: "update", update: [{ action: "set", target: { ref: "data", path: "$.k" }, value: { val: value } }] }],
			});

			expect(res.outcome).toBe("committed");
			await expect(db.getItem(key)).resolves.toMatchObject({ found: true, item: { version: 2, data: { k: value } } });
		}
	});
});
