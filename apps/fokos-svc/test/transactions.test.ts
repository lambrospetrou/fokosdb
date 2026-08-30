import { env } from "cloudflare:workers";
import { afterEach, beforeEach, describe, it, expect, vi } from "vitest";
import { tryWhile } from "durable-utils/retries";
import { FokosDB } from "../src/lib/db.js";
import { PartitionDO } from "../src/lib/do-partition.js";
import { TransactionCoordinatorDO } from "../src/lib/do-transaction-coordinator.js";
import { PartitionContextCreator } from "../src/lib/partition-topology/partition-context.js";
import { PartitionTopologyRouterImpl } from "../src/lib/partition-topology/router.js";
import invariant from "../src/lib/invariant.js";
import { KeyCodec } from "../src/lib/partition-topology/key-codec.js";
import type { ConditionExpression } from "../src/lib/types.js";

function makeDB(opts?: { singlePartitionFastPath?: boolean; maxSizeMb?: number }) {
	const { maxSizeMb, ...dbOptions } = opts ?? {};
	const prefix = `txtest.${crypto.randomUUID()}`;
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName: prefix,
		rootTreesN: 100,
		hashSplitN: 2,
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: maxSizeMb ?? 100 },
		rangeSplitConditions: { maxSizeMb: 500 },
	});
	const topology = new PartitionTopologyRouterImpl(base);
	return new FokosDB({
		transactionCoordinatorNs: env.TRANSACTION_COORDINATOR_DO,
		topology,
		...dbOptions,
	});
}

function partitionNameOf(db: FokosDB, key: { hashKey: string; sortKey?: string }): string {
	const topology = db.options().topology as PartitionTopologyRouterImpl;
	return topology.pickPartition(KeyCodec.encode(key.hashKey), KeyCodec.encodeOptional(key.sortKey)).partitionContext.doName;
}

function countDistinctPartitions(db: FokosDB, keys: Array<{ hashKey: string; sortKey?: string }>): number {
	const names = new Set<string>();
	for (const k of keys) {
		names.add(partitionNameOf(db, k));
	}
	return names.size;
}

const passingConditions: readonly ConditionExpression[] = [
	{ op: "eq", args: [{ ref: "data", path: "$.score" }, { val: 5 }] },
	{ op: "ne", args: [{ ref: "data", path: "$.score" }, { val: 6 }] },
	{ op: "lt", args: [{ ref: "data", path: "$.score" }, { val: 6 }] },
	{ op: "lte", args: [{ ref: "data", path: "$.score" }, { val: 5 }] },
	{ op: "gt", args: [{ ref: "data", path: "$.score" }, { val: 4 }] },
	{ op: "gte", args: [{ ref: "data", path: "$.score" }, { val: 5 }] },
	{ op: "between", args: [{ ref: "data", path: "$.score" }, { val: 4 }, { val: 6 }] },
	{ op: "in", args: [{ ref: "data", path: "$.score" }, { val: 4 }, { val: 5 }] },
	{
		op: "and",
		args: [
			{ op: "exists", args: [{ ref: "data", path: "$.status" }] },
			{ op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "active" }] },
		],
	},
	{
		op: "or",
		args: [
			{ op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "missing" }] },
			{ op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "active" }] },
		],
	},
	{ op: "not", args: [{ op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "missing" }] }] },
	{ op: "exists", args: [{ ref: "data", path: "$.status" }] },
	{ op: "not_exists", args: [{ ref: "data", path: "$.missing" }] },
	{ op: "begins_with", args: [{ ref: "data", path: "$.status" }, { val: "act" }] },
	{ op: "contains", args: [{ ref: "data", path: "$.tags" }, { val: "blue" }] },
];

describe("write conditions", () => {
	it.each(passingConditions)("applies $op conditions to putItem and deleteItem", async (condition) => {
		const db = makeDB();
		const key = { hashKey: `condition-${condition.op}-${crypto.randomUUID()}` };
		const data = { status: "active", score: 5, tags: ["blue", "green"] };

		await db.putItem({ ...key, data });
		await expect(db.putItem({ ...key, data, condition })).resolves.toMatchObject({ version: 2 });
		await expect(db.deleteItem({ ...key, condition })).resolves.toMatchObject({ deleted: true });
	});

	it("does not write when a JSON condition fails", async () => {
		const db = makeDB();
		const key = { hashKey: `condition-failure-${crypto.randomUUID()}` };
		await db.putItem({ ...key, data: { status: "active" } });
		const condition = {
			op: "eq",
			args: [{ ref: "data", path: "$.status" }, { val: "disabled" }],
		} as const satisfies ConditionExpression;

		await expect(db.putItem({ ...key, data: { status: "overwritten" }, condition })).rejects.toThrow(/condition failed/);
		await expect(db.deleteItem({ ...key, condition })).rejects.toThrow(/condition failed/);
		await expect(db.getItem(key)).resolves.toMatchObject({ found: true, item: { data: { status: "active" }, version: 1 } });
	});
});

describe("transactions - end-to-end", () => {
	beforeEach(async () => {
		vi.useFakeTimers({ shouldAdvanceTime: true });
	});

	it("commits 100 puts across many partitions, including 10 pre-existing items", async () => {
		const db = makeDB();

		const preExistingKeys = Array.from({ length: 10 }, (_, i) => ({
			hashKey: `pre-hk-${i}`,
			sortKey: `pre-sk-${i}`,
		}));

		for (const key of preExistingKeys) {
			await db.putItem({ ...key, data: `original-${key.hashKey}` });
		}

		vi.advanceTimersByTime(1);

		for (const key of preExistingKeys) {
			const result = await db.getItem(key);
			expect(result.found).toBe(true);
			if (result.found) {
				expect(result.item.data).toBe(`original-${key.hashKey}`);
				expect(result.item.version).toBe(1);
			}
		}

		const operations = Array.from({ length: 100 }, (_, i) => {
			if (i < 10) {
				return {
					hashKey: preExistingKeys[i].hashKey,
					sortKey: preExistingKeys[i].sortKey,
					operation: "put" as const,
					data: `tx-updated-${i}`,
				};
			}
			return {
				hashKey: `tx-hk-${i}`,
				sortKey: `tx-sk-${i}`,
				operation: "put" as const,
				data: `tx-data-${i}`,
			};
		});

		const txResult = await db.transactWriteItems({ items: operations });

		expect(txResult.outcome).toBe("committed");
		expect(txResult).toMatchObject({
			outcome: "committed",
			transactionId: expect.any(String),
			idempotencyToken: expect.any(String),
		});

		for (let i = 0; i < 10; i++) {
			const result = await db.getItem(preExistingKeys[i]);
			expect(result.found).toBe(true);
			invariant(result.found);
			expect(result.item.data).toBe(`tx-updated-${i}`);
			expect(result.item.version).toBe(2);
		}

		for (let i = 10; i < 100; i++) {
			const result = await db.getItem({ hashKey: `tx-hk-${i}`, sortKey: `tx-sk-${i}` });
			expect(result.found).toBe(true);
			invariant(result.found);
			expect(result.item.data).toBe(`tx-data-${i}`);
			expect(result.item.version).toBe(1);
		}

		expect(countDistinctPartitions(db, operations)).toBeGreaterThan(1);
	}, 20_000);

	it("atomicity: condition failure on one item rolls back the entire transaction", async () => {
		const db = makeDB();

		// Seed 5 items across different partitions.
		for (let i = 0; i < 5; i++) {
			await db.putItem({ hashKey: `atom-${i}`, data: `v1-${i}` });
		}

		// Advance time to avoid timestamp-based conflicts with the transaction's prepare phase.
		vi.advanceTimersByTime(1);

		// Transaction: update all 5 items + a 6th "check" on a non-existent item
		// with item_exists condition — this MUST fail and roll back everything.
		const txResult = await db.transactWriteItems({
			items: [
				...Array.from({ length: 5 }, (_, i) => ({
					hashKey: `atom-${i}`,
					operation: "put" as const,
					data: `should-not-appear-${i}`,
				})),
				{
					hashKey: "atom-nonexistent",
					operation: "check" as const,
					condition: { op: "exists" as const, args: [{ ref: "hashKey" as const }] },
				},
			],
		});

		expect(txResult.outcome).toBe("cancelled");
		invariant(txResult.outcome === "cancelled");
		// condition_failed is the expected reason, but timestamp_conflict is also
		// valid if another partition rejects before the condition-failing partition.
		expect(txResult.reason).toMatchObject({
			type: expect.stringMatching(/condition_failed/),
		});

		// All 5 original items must be untouched — still version 1, original data.
		for (let i = 0; i < 5; i++) {
			const result = await db.getItem({ hashKey: `atom-${i}` });
			invariant(result.found);
			expect(result.item.data).toBe(`v1-${i}`);
			expect(result.item.version).toBe(1);
		}

		// The non-existent item must still not exist.
		const missing = await db.getItem({ hashKey: "atom-nonexistent" });
		expect(missing.found).toBe(false);
	});

	it("atomicity: condition failure across partitions — no partial writes", async () => {
		const db = makeDB();

		// Create 10 items that span multiple partitions.
		const keys = Array.from({ length: 10 }, (_, i) => ({ hashKey: `cross-${i}` }));
		for (const k of keys) {
			await db.putItem({ ...k, data: `original` });
		}
		expect(countDistinctPartitions(db, keys)).toBeGreaterThan(1);

		vi.advanceTimersByTime(1);

		// Transaction: put all 10 items, but with item_not_exists condition on the
		// first one (which already exists). The condition check will fail, so none
		// of the 10 puts should be applied.
		const txResult = await db.transactWriteItems({
			items: keys.map((k, i) => ({
				...k,
				operation: "put" as const,
				data: `should-not-appear`,
				condition: i === 0 ? ({ op: "not_exists", args: [{ ref: "hashKey" }] } as const) : undefined,
			})),
		});

		expect(txResult.outcome).toBe("cancelled");

		for (const k of keys) {
			const result = await db.getItem(k);
			expect(result.found).toBe(true);
			if (result.found) {
				expect(result.item.data).toBe("original");
				expect(result.item.version).toBe(1);
			}
		}
	});

	it("isolation: concurrent non-tx putItem and transaction on the same item", async () => {
		const db = makeDB();

		// Seed the shared key so both operations can conflict on an existing item.
		await db.putItem({ hashKey: "iso-shared", data: "original" });
		vi.advanceTimersByTime(1);

		const [putResult, txResult] = await Promise.allSettled([
			db.putItem({ hashKey: "iso-shared", data: "non-tx-write" }),
			db.transactWriteItems({
				items: [
					{ hashKey: "iso-shared", operation: "put", data: "tx-shared" },
					// A second key only the transaction writes — must not appear if the transaction is cancelled.
					{ hashKey: "iso-tx-only", operation: "put", data: "tx-only-data" },
				],
			}),
		]);

		// The transaction coordinator never throws — it returns a result.
		expect(txResult.status).toBe("fulfilled");
		invariant(txResult.status === "fulfilled");
		const tx = txResult.value;

		if (putResult.status === "rejected") {
			// prepare ran before putItem arrived → putItem was blocked by the pending lock.
			// The transaction must have committed cleanly.
			expect(tx.outcome).toBe("committed");
			const shared = await db.getItem({ hashKey: "iso-shared" });
			expect(shared.found).toBe(true);
			invariant(shared.found);
			expect(shared.item.data).toBe("tx-shared");
			const txOnly = await db.getItem({ hashKey: "iso-tx-only" });
			expect(txOnly.found).toBe(true);
			invariant(txOnly.found);
			expect(txOnly.item.data).toBe("tx-only-data");
		} else {
			// putItem landed before prepare → transaction detects timestamp_conflict and is cancelled.
			expect(tx.outcome).toBe("cancelled");
			invariant(tx.outcome === "cancelled");
			expect(tx.reason.type).toBe("timestamp_conflict");
			// Atomicity: the transaction's private write must not have landed.
			const txOnly = await db.getItem({ hashKey: "iso-tx-only" });
			expect(txOnly.found).toBe(false);
			// The shared key reflects only the non-tx write.
			const shared = await db.getItem({ hashKey: "iso-shared" });
			expect(shared.found).toBe(true);
			invariant(shared.found);
			expect(shared.item.data).toBe("non-tx-write");
		}
	});

	it("conflict: concurrent transactions on overlapping keys — loser's writes are fully rolled back", async () => {
		const db = makeDB();

		// No seeds — items are created by the transactions. This isolates the test
		// to pure pending-lock contention without timestamp races from prior writes.
		const [r1, r2] = await Promise.allSettled([
			db.transactWriteItems({
				items: [
					{ hashKey: "c-shared", operation: "put", data: "tx1-shared" },
					{ hashKey: "c-only-a", operation: "put", data: "tx1-a" },
				],
			}),
			db.transactWriteItems({
				items: [
					{ hashKey: "c-shared", operation: "put", data: "tx2-shared" },
					{ hashKey: "c-only-b", operation: "put", data: "tx2-b" },
				],
			}),
		]);

		expect(r1.status).toBe("fulfilled");
		expect(r2.status).toBe("fulfilled");
		const tx1 = r1.status === "fulfilled" ? r1.value : null;
		const tx2 = r2.status === "fulfilled" ? r2.value : null;

		const outcomes = [tx1?.outcome, tx2?.outcome];
		expect(outcomes).toContain("committed");

		for (const tx of [tx1, tx2]) {
			if (tx?.outcome === "cancelled") {
				expect(["pending_conflict", "timestamp_conflict"]).toContain(tx.reason.type);
			}
		}

		// The cancelled transaction's unique item must not exist (atomicity).
		if (tx1?.outcome === "cancelled") {
			expect((await db.getItem({ hashKey: "c-only-a" })).found).toBe(false);
		}
		if (tx2?.outcome === "cancelled") {
			expect((await db.getItem({ hashKey: "c-only-b" })).found).toBe(false);
		}

		// The winner's unique item must carry the transaction data.
		if (tx1?.outcome === "committed") {
			const a = await db.getItem({ hashKey: "c-only-a" });
			expect(a.found).toBe(true);
			if (a.found) expect(a.item.data).toBe("tx1-a");
		}
		if (tx2?.outcome === "committed") {
			const b = await db.getItem({ hashKey: "c-only-b" });
			expect(b.found).toBe(true);
			if (b.found) expect(b.item.data).toBe("tx2-b");
		}

		// The shared key must reflect the committed transaction(s).
		const shared = await db.getItem({ hashKey: "c-shared" });
		expect(shared.found).toBe(true);
		invariant(shared.found);
		if (outcomes.filter((o) => o === "committed").length === 1) {
			const expectedData = tx1?.outcome === "committed" ? "tx1-shared" : "tx2-shared";
			expect(shared.item.data).toBe(expectedData);
			expect(shared.item.version).toBe(1);
		} else {
			expect(shared.item.version).toBe(2);
			expect(["tx1-shared", "tx2-shared"]).toContain(shared.item.data);
		}
	});

	it("serializability: concurrent transactions on the same key — loser retries and eventually commits", async () => {
		// The coordinator path, pinned: this key set is single-partition and untokened, so the fast path
		// would take it, and there the premise below stops existing — the fast path holds no lock, so
		// both transactions serialize inside the partition and neither loses. The fast-path counterpart
		// is the next test.
		const db = makeDB({ singlePartitionFastPath: false });

		let firstRetries = 0,
			secondRetries = 0;
		const [r1, r2] = await Promise.allSettled([
			tryWhile(
				async () => {
					const result = await db.transactWriteItems({
						items: [{ hashKey: "ser-key", operation: "put", data: "tx1" }],
					});
					if (result.outcome !== "committed") throw result;
					return result;
				},
				(_err, nextAttempt) => {
					firstRetries++;
					return nextAttempt <= 5;
				},
				{ baseDelayMs: 50, maxDelayMs: 500 },
			),
			tryWhile(
				async () => {
					const result = await db.transactWriteItems({
						items: [{ hashKey: "ser-key", operation: "put", data: "tx2" }],
					});
					if (result.outcome !== "committed") throw result;
					return result;
				},
				(_err, nextAttempt) => {
					secondRetries++;
					return nextAttempt <= 5;
				},
				{ baseDelayMs: 50, maxDelayMs: 500 },
			),
		]);

		expect(r1.status).toBe("fulfilled");
		invariant(r1.status === "fulfilled");
		expect(r2.status).toBe("fulfilled");
		invariant(r2.status === "fulfilled");
		const tx1 = r1.value;
		const tx2 = r2.value;

		expect(tx1.outcome).toBe("committed");
		expect(tx2.outcome).toBe("committed");

		let value = undefined;
		expect(firstRetries + secondRetries).toBeGreaterThan(0);
		if (firstRetries > 0) {
			value = tx1;
			expect(tx1.outcome).toBe("committed");
			expect(tx2.outcome).toBe("committed");
		} else if (secondRetries > 0) {
			value = tx2;
			expect(tx1.outcome).toBe("committed");
			expect(tx2.outcome).toBe("committed");
		}

		// Both applied serially: tx1(v1) → tx2(v2).
		const result = await db.getItem({ hashKey: "ser-key" });
		expect(result.found).toBe(true);
		if (result.found) {
			expect(result.item.data).toBe(value === tx1 ? "tx1" : "tx2");
			expect(result.item.version).toBe(2);
		}
	});

	it("serializability: concurrent single-partition transactions both commit with no retry", async () => {
		const db = makeDB();

		const write = async (data: string) => await db.transactWriteItems({ items: [{ hashKey: "ser-fast-key", operation: "put", data }] });
		const [tx1, tx2] = await Promise.all([write("tx1"), write("tx2")]);

		// The partition takes no lock for either, so neither can conflict with the other: they serialize
		// inside the single-threaded DO and both commit on their first attempt.
		expect(tx1.outcome).toBe("committed");
		expect(tx2.outcome).toBe("committed");

		// Applied one after the other, so the surviving value is one of the two and the item saw two writes.
		const result = await db.getItem({ hashKey: "ser-fast-key" });
		invariant(result.found);
		expect(["tx1", "tx2"]).toContain(result.item.data);
		expect(result.item.version).toBe(2);
	});

	it("transactGetItems returns consistent snapshot across partitions", async () => {
		const db = makeDB();

		const keys = Array.from({ length: 10 }, (_, i) => ({
			hashKey: `read-${i}`,
			sortKey: `sk-${i}`,
		}));
		for (const k of keys) {
			await db.putItem({ ...k, data: `data-${k.hashKey}` });
		}
		expect(countDistinctPartitions(db, keys)).toBeGreaterThan(1);

		const readResult = await db.transactGetItems({ items: keys });

		expect(readResult.outcome).toBe("committed");
		invariant(readResult.outcome === "committed");
		expect(readResult.items).toHaveLength(10);
		for (const item of readResult.items) {
			expect(item.found).toBe(true);
			invariant(item.found);
			expect(item.data).toBe(`data-${item.hashKey}`);
			// Each key was written exactly once above, so v=1 — the same version getItem reports, and
			// the value a caller feeds back into an attribute_equals condition.
			expect(item.version).toBe(1);
			// The 2PC bookkeeping is stripped at the public boundary.
			expect(item).not.toHaveProperty("lastCommittedTs");
			expect(item).not.toHaveProperty("hasPendingWrite");
		}
	});

	it("transactGetItems returns items positionally matched to the request", async () => {
		/**
		 * Keys from `partitions` distinct partitions, `perPartition` each, ordered so that consecutive keys
		 * sit in DIFFERENT partitions (round-robin over the buckets). The TC groups items by partition, so a
		 * response returned in group order cannot come back in this order — which is what makes it a test of
		 * the positional guarantee rather than of luck. Both counts must be >= 2 for that to hold.
		 */
		function interleavedKeysAcrossPartitions(
			db: FokosDB,
			partitions: number,
			perPartition: number,
		): Array<{ hashKey: string; sortKey: string }> {
			type Key = { hashKey: string; sortKey: string };
			// Candidate keys are routed one at a time until `partitions` buckets have filled — routing is a
			// pure hash, so how many that takes is deterministic, not a source of flakiness. The cap only
			// stops a misconfigured topology (too few root trees to ever fill them) from looping forever.
			const MAX_CANDIDATES = 2_000;
			const buckets = new Map<string, Key[]>();
			const filled: Key[][] = [];
			for (let i = 0; filled.length < partitions; i++) {
				expect(i, `no ${partitions} partitions held ${perPartition} keys within ${MAX_CANDIDATES} candidates`).toBeLessThan(MAX_CANDIDATES);
				const key = { hashKey: `ord-${i}`, sortKey: `sk-${i}` };
				const name = partitionNameOf(db, key);
				let bucket = buckets.get(name);
				if (!bucket) buckets.set(name, (bucket = []));
				if (bucket.length === perPartition) continue; // already filled and taken
				bucket.push(key);
				if (bucket.length === perPartition) filled.push(bucket);
			}
			const out: Key[] = [];
			for (let i = 0; i < perPartition; i++) {
				for (const bucket of filled) out.push(bucket[i]);
			}
			return out;
		}

		const db = makeDB();

		// 3 partitions x 4 keys, asked for in interleaved order: the answer can only come back in this
		// order if the TC restores the request order after its per-partition grouping.
		const keys = interleavedKeysAcrossPartitions(db, 3, 4);
		expect(countDistinctPartitions(db, keys)).toBe(3);
		// Only every other key is written. An ABSENT key still occupies its own position. The gaps are
		// spread over all three partitions, so no partition returns a "clean" all-found reply.
		const isWritten = (i: number) => i % 2 === 0;
		for (const [i, k] of keys.entries()) {
			if (isWritten(i)) await db.putItem({ ...k, data: `data-${k.hashKey}` });
		}

		const readResult = await db.transactGetItems({ items: keys });
		invariant(readResult.outcome === "committed");
		expect(readResult.items).toHaveLength(keys.length);
		readResult.items.forEach((item, i) => {
			expect(item).toMatchObject({
				hashKey: keys[i].hashKey,
				sortKey: keys[i].sortKey,
				found: isWritten(i),
			});
		});
		for (const [i, item] of readResult.items.entries()) {
			if (item.found) expect(item.data).toBe(`data-${keys[i].hashKey}`);
		}

		// A key asked for twice is answered twice, at both positions — one entry per requested position.
		const withDuplicate = [keys[0], keys[1], keys[0]];
		const dupResult = await db.transactGetItems({ items: withDuplicate });
		invariant(dupResult.outcome === "committed");
		expect(dupResult.items.map((item) => item.hashKey)).toEqual(withDuplicate.map((k) => k.hashKey));
	});

	it("idempotency: retrying transactWriteItems with same clientRequestToken returns same result", async () => {
		const db = makeDB();

		const token = `idemp-token-${crypto.randomUUID()}`;
		const operations = [
			{ hashKey: "idemp-1", operation: "put" as const, data: "tx-data" },
			{ hashKey: "idemp-2", operation: "put" as const, data: "tx-data" },
		];

		const result1 = await db.transactWriteItems({ items: operations, clientRequestToken: token });
		expect(result1.outcome).toBe("committed");

		const result2 = await db.transactWriteItems({ items: operations, clientRequestToken: token });
		expect(result2.outcome).toBe("committed");
		invariant(result1.outcome === "committed" && result2.outcome === "committed");
		expect(result2.transactionId).toBe(result1.transactionId);
		expect(result2.idempotencyToken).toBe(result1.idempotencyToken);

		// Item was created once by the transaction — version 1, not 2.
		const item = await db.getItem({ hashKey: "idemp-1" });
		expect(item.found).toBe(true);
		invariant(item.found);
		expect(item.item.version).toBe(1);
	});

	it("persists a JSON condition plan through two-phase commit and replays it idempotently", async () => {
		const db = makeDB();
		const key = { hashKey: `condition-plan-${crypto.randomUUID()}` };
		const token = `condition-plan-token-${crypto.randomUUID()}`;
		await db.putItem({ ...key, data: { status: "active" } });
		vi.advanceTimersByTime(1);
		const operation = {
			...key,
			operation: "put" as const,
			data: { status: "updated" },
			condition: { op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "active" }] } as const,
		};

		const first = await db.transactWriteItems({ items: [operation], clientRequestToken: token });
		const replay = await db.transactWriteItems({ items: [operation], clientRequestToken: token });

		expect(replay).toEqual(first);
		await expect(db.getItem(key)).resolves.toMatchObject({ found: true, item: { data: { status: "updated" }, version: 2 } });
	});

	// A token identifies one request, not one caller. Answering a different request with the stored
	// outcome would acknowledge writes that never execute, so the coordinator compares an
	// operation-set fingerprint and refuses.
	it("idempotency: reusing a clientRequestToken for different operations is rejected", async () => {
		const db = makeDB();

		const token = `idemp-mismatch-${crypto.randomUUID()}`;
		const operations = [{ hashKey: "mismatch-1", operation: "put" as const, data: "original" }];
		const first = await db.transactWriteItems({ items: operations, clientRequestToken: token });
		expect(first.outcome).toBe("committed");

		// Same key, different payload — the case that silently lost the write.
		await expect(
			db.transactWriteItems({
				items: [{ hashKey: "mismatch-1", operation: "put" as const, data: "different" }],
				clientRequestToken: token,
			}),
		).rejects.toThrow(/was already used for a different set of operations/);

		// A different operation SET is rejected too, not just a different payload.
		await expect(
			db.transactWriteItems({
				items: [...operations, { hashKey: "mismatch-2", operation: "put" as const, data: "original" }],
				clientRequestToken: token,
			}),
		).rejects.toThrow(/was already used for a different set of operations/);

		// The stored transaction is untouched: still the original value, still version 1.
		const item = await db.getItem({ hashKey: "mismatch-1" });
		invariant(item.found);
		expect(item.item.data).toBe("original");
		expect(item.item.version).toBe(1);

		// The legitimate replay still works — the guard rejects different work, not retries.
		const replay = await db.transactWriteItems({ items: operations, clientRequestToken: token });
		expect(replay.outcome).toBe("committed");
	});

	it("delete operations in a transaction remove items atomically", async () => {
		const db = makeDB();

		for (let i = 0; i < 5; i++) {
			await db.putItem({ hashKey: `del-${i}`, data: `data-${i}` });
		}

		vi.advanceTimersByTime(1);

		const txResult = await db.transactWriteItems({
			items: [
				{ hashKey: "del-0", operation: "delete" },
				{ hashKey: "del-1", operation: "delete" },
				{ hashKey: "del-2", operation: "put", data: "updated" },
				{ hashKey: "del-3", operation: "put", data: "updated" },
				{ hashKey: "del-4", operation: "delete" },
			],
		});

		expect(txResult.outcome).toBe("committed");

		expect((await db.getItem({ hashKey: "del-0" })).found).toBe(false);
		expect((await db.getItem({ hashKey: "del-1" })).found).toBe(false);
		expect((await db.getItem({ hashKey: "del-4" })).found).toBe(false);

		const item2 = await db.getItem({ hashKey: "del-2" });
		expect(item2.found).toBe(true);
		invariant(item2.found);
		expect(item2.item.data).toBe("updated");
		expect(item2.item.version).toBe(2);

		const item3 = await db.getItem({ hashKey: "del-3" });
		expect(item3.found).toBe(true);
		invariant(item3.found);
		expect(item3.item.data).toBe("updated");
		expect(item3.item.version).toBe(2);
	});

	it("atomicity: failed condition on a delete rolls back puts in the same transaction", async () => {
		const db = makeDB();

		await db.putItem({ hashKey: "rollback-put", data: "original" });

		vi.advanceTimersByTime(1);

		// Transaction: put on one item + delete on a non-existent item with item_exists condition.
		const txResult = await db.transactWriteItems({
			items: [
				{ hashKey: "rollback-put", operation: "put", data: "should-not-appear" },
				{
					hashKey: "rollback-missing",
					operation: "delete",
					condition: { op: "exists", args: [{ ref: "hashKey" }] },
				},
			],
		});

		expect(txResult.outcome).toBe("cancelled");

		const result = await db.getItem({ hashKey: "rollback-put" });
		expect(result.found).toBe(true);
		invariant(result.found);
		expect(result.item.data).toBe("original");
		expect(result.item.version).toBe(1);
	});

	it("coordinator distribution: 10 transactions across 3 coordinators land on multiple TCs", async () => {
		// Intercept idFromName on the TC namespace to record which shard name each transaction
		// is routed to. StaticShardedDO calls idFromName exactly once per transactWriteItems.
		const calledTCNames: string[] = [];
		const spyTCNs = new Proxy(env.TRANSACTION_COORDINATOR_DO, {
			get(target, prop) {
				if (prop === "idFromName") {
					return (name: string) => {
						calledTCNames.push(name);
						return target.idFromName(name);
					};
				}
				const value = (target as any)[prop];
				return typeof value === "function" ? value.bind(target) : value;
			},
		}) as typeof env.TRANSACTION_COORDINATOR_DO;

		const dbName = `tcdist.${crypto.randomUUID()}`;
		const base = PartitionContextCreator.create({
			ns: "PARTITION_DO",
			nsTx: "TRANSACTION_COORDINATOR_DO",
			tableName: dbName,
			rootTreesN: 100,
			hashSplitN: 2,
			rangeSplitN: 2,
			hashSplitConditions: { maxSizeMb: 100 },
			rangeSplitConditions: { maxSizeMb: 500 },
		});
		const topology = new PartitionTopologyRouterImpl(base);
		const db = new FokosDB({
			topology,
			transactionCoordinatorNs: spyTCNs,
			numTransactionCoordinators: 3,
		});

		for (let i = 0; i < 10; i++) {
			const result = await db.transactWriteItems({
				items: [
					{
						hashKey: `dist-hk-${i}`,
						sortKey: `dist-sk-${i}`,
						operation: "put",
						data: `dist-data-${i}`,
					},
				],
				clientRequestToken: `tcdist-token-${i}`,
			});
			expect(result.outcome).toBe("committed");
		}

		// One idFromName call per transactWriteItems.
		expect(calledTCNames).toHaveLength(10);

		// StaticShardedDO names shards as `${shardGroupName}-${index}`, and shardGroupName is `${tableName}.tc.`
		const expectedTCNames = new Set([`${dbName}.tc-0`, `${dbName}.tc-1`, `${dbName}.tc-2`]);
		for (const name of calledTCNames) {
			expect(expectedTCNames.has(name)).toBe(true);
		}

		// With 10 transactions across 3 shards, we are asserting >= 2 distinct TCs per coordinator.
		const uniqueTCNames = new Set(calledTCNames);
		expect(uniqueTCNames.size).toBeGreaterThanOrEqual(2);
	});
});

/**
 * The single-partition fast path answers a transaction from the owning partition in one round trip,
 * with no coordinator. These tests pin WHICH path a key set takes — routing is a pure hash of the
 * key bytes, so it is fixed, never flaky — and assert that both paths give the same answer.
 */
describe("transactions - single-partition fast path", () => {
	/** `count` keys that all route to the SAME partition, so the fast path applies to the whole set. */
	function keysInOnePartition(db: FokosDB, count: number, prefix: string): Array<{ hashKey: string; sortKey: string }> {
		type Key = { hashKey: string; sortKey: string };
		const MAX_CANDIDATES = 2_000;
		const buckets = new Map<string, Key[]>();
		for (let i = 0; ; i++) {
			expect(i, `no partition held ${count} keys within ${MAX_CANDIDATES} candidates`).toBeLessThan(MAX_CANDIDATES);
			const key = { hashKey: `${prefix}-${i}`, sortKey: "sk" };
			const bucket = buckets.get(partitionNameOf(db, key)) ?? [];
			bucket.push(key);
			buckets.set(partitionNameOf(db, key), bucket);
			if (bucket.length === count) return bucket;
		}
	}

	// The DO classes run in this same isolate, so a spy on their prototype counts the real RPC
	// dispatches — and stays installed until it is restored.
	afterEach(() => {
		vi.restoreAllMocks();
	});

	/** Counts the RPCs each read path makes, so a test can assert which one ran. */
	function countReadPathCalls() {
		const partitionCalls = vi.spyOn(PartitionDO.prototype, "txReadSnapshot");
		const coordinatorCalls = vi.spyOn(TransactionCoordinatorDO.prototype, "initiateRead");
		return { partitionCalls, coordinatorCalls };
	}

	/** The same, for the write paths. */
	function countWritePathCalls() {
		const partitionCalls = vi.spyOn(PartitionDO.prototype, "txExecuteSingleShot");
		const coordinatorCalls = vi.spyOn(TransactionCoordinatorDO.prototype, "initiateWrite");
		return { partitionCalls, coordinatorCalls };
	}

	it("reads a single-partition key set in one round trip, and answers exactly as the coordinator does", async () => {
		const db = makeDB();
		const slowDb = makeDB({ singlePartitionFastPath: false });

		const keys = keysInOnePartition(db, 3, "fast-read");
		expect(countDistinctPartitions(db, keys)).toBe(1);
		// Only two of the three are written, so an absent key is checked on both paths as well.
		for (const key of keys.slice(0, 2)) {
			await db.putItem({ ...key, data: `data-${key.hashKey}` });
			await slowDb.putItem({ ...key, data: `data-${key.hashKey}` });
		}

		const { partitionCalls, coordinatorCalls } = countReadPathCalls();
		const fast = await db.transactGetItems({ items: keys });
		expect(partitionCalls).toHaveBeenCalledTimes(1);
		expect(coordinatorCalls).not.toHaveBeenCalled();

		// The option off pins the other path for the same key set, and both answers must agree
		// item by item, in request order.
		const slow = await slowDb.transactGetItems({ items: keys });
		expect(coordinatorCalls).toHaveBeenCalledTimes(1);
		expect(fast).toEqual(slow);
		invariant(fast.outcome === "committed");
		expect(fast.items.map((i) => i.found)).toEqual([true, true, false]);
	});

	it("keeps a key set that spans partitions on the coordinator path", async () => {
		const db = makeDB();
		const keys = [
			{ hashKey: "span-a", sortKey: "sk" },
			{ hashKey: "span-b", sortKey: "sk" },
		];
		expect(countDistinctPartitions(db, keys)).toBe(2);
		for (const key of keys) await db.putItem({ ...key, data: "v" });

		const { partitionCalls, coordinatorCalls } = countReadPathCalls();
		const result = await db.transactGetItems({ items: keys });

		expect(result.outcome).toBe("committed");
		expect(partitionCalls).not.toHaveBeenCalled();
		expect(coordinatorCalls).toHaveBeenCalledTimes(1);
	});

	it("runs the coordinator path when the partition answers that it cannot execute the whole set", async () => {
		const db = makeDB();
		const keys = keysInOnePartition(db, 2, "fast-fallback");
		for (const key of keys) await db.putItem({ ...key, data: `data-${key.hashKey}` });

		// A partition raises this when the items straddle a split or a promotion below it. Standing in
		// for that setup here keeps the test on what db.ts owns: recognising the sentinel and finishing
		// the read on the coordinator path. The raise itself is covered in do-partition.test.ts.
		const { partitionCalls, coordinatorCalls } = countReadPathCalls();
		partitionCalls.mockRejectedValue(new Error("fokos/partition: single-partition fast path not applicable (readSnapshot)."));

		const result = await db.transactGetItems({ items: keys });

		expect(partitionCalls).toHaveBeenCalledTimes(1);
		expect(coordinatorCalls).toHaveBeenCalledTimes(1);
		invariant(result.outcome === "committed");
		expect(result.items.map((i) => i.found)).toEqual([true, true]);
		expect(result.items.map((i) => (i.found ? i.data : null))).toEqual(keys.map((k) => `data-${k.hashKey}`));
	});

	it("surfaces a transport failure instead of retrying it on the coordinator path", async () => {
		const db = makeDB();
		const keys = keysInOnePartition(db, 2, "fast-transport");
		for (const key of keys) await db.putItem({ ...key, data: "v" });

		const { partitionCalls, coordinatorCalls } = countReadPathCalls();
		partitionCalls.mockRejectedValue(new Error("Network connection lost."));

		await expect(db.transactGetItems({ items: keys })).rejects.toThrow(/Network connection lost/);
		expect(coordinatorCalls).not.toHaveBeenCalled();
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
		// itself is covered in do-partition.test.ts; what matters here is that db.ts recognises it and
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
