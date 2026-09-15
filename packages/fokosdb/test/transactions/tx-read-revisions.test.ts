import { env } from "cloudflare:workers";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import { PartitionDO } from "../../src/server/do-partition.js";
import { compileConditionExpression } from "../../src/shared/expression/compiler.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { KeyCodec } from "../../src/shared/partition-topology/key-codec.js";
import type { PartitionContextResolved } from "../../src/shared/partition-topology/partition-context.js";
import { PartitionTopologyRouterImpl } from "../../src/shared/partition-topology/router.js";
import { txOrderTimestampNow } from "../../src/shared/transaction-limits.js";
import type { TransactionItem } from "../../src/shared/transaction-types.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { type Key, keysAcrossPartitions, keysInOnePartition, makeDB, partitionNameOf } from "./tx-helpers.js";

const kb = (s: string) => KeyCodec.encode(s);

const itemExists = () => compileConditionExpression({ op: "exists", args: [{ ref: "hashKey" }] });

/** The stub and resolved context of the partition that owns `key`. */
function owningPartition(db: FokosDB, key: Key) {
	const topology = db.options().topology as PartitionTopologyRouterImpl;
	const { partitionContext } = topology.pickPartition(kb(key.hashKey), kb(key.sortKey));
	return { stub: PartitionDO.getByName(env.PARTITION_DO, partitionContext.doName), pCtx: partitionContext };
}

/**
 * Holds a two-phase transaction lock on `key` and returns the release. The prepare stamps the
 * partition's own clock, so it always orders above the writes that seeded the item.
 */
async function holdPendingLock(
	db: FokosDB,
	key: Key,
	item: Omit<TransactionItem, "opIndex" | "hashKey" | "sortKey">,
): Promise<() => Promise<unknown>> {
	const { stub, pCtx } = owningPartition(db, key);
	const transactionId = crypto.randomUUID();
	const keys = { hashKey: kb(key.hashKey), sortKey: kb(key.sortKey) };
	const res = await stub.txPrepare(pCtx, {
		transactionId,
		coordinatorDoId: env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString(),
		transactionTimestamp: txOrderTimestampNow(),
		items: [{ opIndex: 0, ...keys, ...item }],
	});
	expect(res.outcome).toBe("accepted");
	return () => stub.txCancel(pCtx, { transactionId, items: [keys] });
}

/**
 * Runs `between` inside the target partition after its phase-1 read and before its phase-2 read of
 * a two-phase `transactGetItems`. The callback calls the DO instance directly — no RPC, no mocked
 * response — so the second phase observes a real committed mutation.
 */
function betweenPhases(doName: string, between: (this: PartitionDO, pCtx: PartitionContextResolved) => Promise<void>) {
	const original = PartitionDO.prototype.txReadForTransaction;
	const calls = new Map<string, number>();
	return vi.spyOn(PartitionDO.prototype, "txReadForTransaction").mockImplementation(async function (this: PartitionDO, pCtx, request) {
		const call = (calls.get(pCtx.doName) ?? 0) + 1;
		calls.set(pCtx.doName, call);
		const response = await original.call(this, pCtx, request);
		if (pCtx.doName === doName && call === 1) await between.call(this, pCtx);
		return response;
	});
}

describe("transactGetItems — read revisions and pending checks", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	it("a committed check between the phases does not abort the read", async () => {
		const db = makeDB({ singlePartitionFastPath: false });
		const key = keysInOnePartition(db, 1, "check-between")[0];
		await db.putItem({ ...key, data: "value" });
		const doName = partitionNameOf(db, key);

		betweenPhases(doName, async function (this: PartitionDO, pCtx) {
			await this.txExecuteSingleShot(pCtx, {
				items: [{ opIndex: 0, hashKey: kb(key.hashKey), sortKey: kb(key.sortKey), operation: "check", condition: itemExists() }],
			});
		});

		// A check advances only the read watermark: `version` and the delete revision stand still.
		const result = await db.transactGetItems({ items: [key] });
		expect(result).toMatchObject({ outcome: "committed", items: [{ found: true, data: "value", version: 1 }] });
	});

	it("a pending check on the item does not abort the read", async () => {
		const db = makeDB({ singlePartitionFastPath: false });
		const key = keysInOnePartition(db, 1, "pending-check")[0];
		await db.putItem({ ...key, data: "value" });

		const release = await holdPendingLock(db, key, { operation: "check", condition: itemExists() });
		try {
			const result = await db.transactGetItems({ items: [key] });
			expect(result).toMatchObject({ outcome: "committed", items: [{ found: true, data: "value", version: 1 }] });
		} finally {
			await release();
		}
	});

	it("a pending content mutation on the item aborts the read", async () => {
		const db = makeDB({ singlePartitionFastPath: false });
		const key = keysInOnePartition(db, 1, "pending-put")[0];
		await db.putItem({ ...key, data: "value" });

		const release = await holdPendingLock(db, key, { operation: "put", data: "pending", kind: "text" });
		try {
			await expect(db.transactGetItems({ items: [key] })).rejects.toThrow(fokosErrorWith("pending_write"));
		} finally {
			await release();
		}
	});

	it("a committed put between the phases aborts the read", async () => {
		const db = makeDB({ singlePartitionFastPath: false });
		const key = keysInOnePartition(db, 1, "put-between")[0];
		await db.putItem({ ...key, data: "value" });
		const doName = partitionNameOf(db, key);

		betweenPhases(doName, async function (this: PartitionDO, pCtx) {
			await this.apiPutItem(pCtx, { hashKey: kb(key.hashKey), sortKey: kb(key.sortKey), data: "changed", kind: "text" });
		});

		await expect(db.transactGetItems({ items: [key] })).rejects.toThrow(fokosErrorWith("read_conflict", { hashKey: key.hashKey }));
	});

	it("a delete and recreate that lands back on the same version still aborts the read", async () => {
		const db = makeDB({ singlePartitionFastPath: false });
		const key = keysInOnePartition(db, 1, "recreate")[0];
		await db.putItem({ ...key, data: "value" });
		const doName = partitionNameOf(db, key);

		// The recreated item reads back at version 1 — the same `v` phase 1 saw — so `found` and
		// `version` agree across the phases. Only the delete revision moved.
		betweenPhases(doName, async function (this: PartitionDO, pCtx) {
			await this.apiDeleteItem(pCtx, { hashKey: kb(key.hashKey), sortKey: kb(key.sortKey) });
			await this.apiPutItem(pCtx, { hashKey: kb(key.hashKey), sortKey: kb(key.sortKey), data: "recreated", kind: "text" });
		});

		await expect(db.transactGetItems({ items: [key] })).rejects.toThrow(fokosErrorWith("read_conflict", { hashKey: key.hashKey }));
	});

	it("a create and delete of an absent item between the phases aborts the read", async () => {
		const db = makeDB({ singlePartitionFastPath: false });
		const key = keysInOnePartition(db, 1, "absent-flip")[0];
		const doName = partitionNameOf(db, key);

		// The item is absent in both phases, so `found` agrees and there is no `version` to compare.
		// Only the delete revision moved.
		betweenPhases(doName, async function (this: PartitionDO, pCtx) {
			await this.apiPutItem(pCtx, { hashKey: kb(key.hashKey), sortKey: kb(key.sortKey), data: "flicker", kind: "text" });
			await this.apiDeleteItem(pCtx, { hashKey: kb(key.hashKey), sortKey: kb(key.sortKey) });
		});

		await expect(db.transactGetItems({ items: [key] })).rejects.toThrow(fokosErrorWith("read_conflict", { hashKey: key.hashKey }));
	});

	it("an unrelated user delete in the same partition aborts the read", async () => {
		const db = makeDB({ singlePartitionFastPath: false });
		const [key, sibling] = keysInOnePartition(db, 2, "unrelated-delete");
		await db.putItem({ ...key, data: "value" });
		await db.putItem({ ...sibling, data: "sibling" });
		const doName = partitionNameOf(db, key);

		// The delete revision is partition-wide, so a delete of an item the read never asked about
		// still moves it. That makes this abort conservative: the read could have been answered.
		betweenPhases(doName, async function (this: PartitionDO, pCtx) {
			await this.apiDeleteItem(pCtx, { hashKey: kb(sibling.hashKey), sortKey: kb(sibling.sortKey) });
		});

		await expect(db.transactGetItems({ items: [key] })).rejects.toThrow(fokosErrorWith("read_conflict", { hashKey: key.hashKey }));
	});

	it("a TTL sweep between the phases does not abort the read", async () => {
		const db = makeDB({ singlePartitionFastPath: false });
		const [key, sibling] = keysInOnePartition(db, 2, "ttl-sweep");
		await db.putItem({ ...key, data: "value" });
		const ttlAt = Math.floor(Date.now() / 1000) + 3600;
		await db.putItem({ ...sibling, data: "expiring", ttlAt });
		const doName = partitionNameOf(db, key);

		// The sweep removes the expired sibling. It advances the transaction order watermark but
		// counts no user delete, so the delete revision — and the read — is undisturbed.
		betweenPhases(doName, async function (this: PartitionDO) {
			const store = new PartitionStore((this as unknown as { ctx: DurableObjectState }).ctx.storage);
			expect(store.deleteExpiredItems(ttlAt + 1, 100)).toMatchObject({ deletedRows: 1 });
		});

		const result = await db.transactGetItems({ items: [key] });
		expect(result).toMatchObject({ outcome: "committed", items: [{ found: true, data: "value", version: 1 }] });
	});

	it("a user delete in a different partition does not change this partition's revision", async () => {
		const db = makeDB({ singlePartitionFastPath: false });
		const [key, other] = keysAcrossPartitions(db, 2, "other-partition");
		await db.putItem({ ...key, data: "value" });
		await db.putItem({ ...other, data: "other" });
		const doName = partitionNameOf(db, key);
		expect(partitionNameOf(db, other)).not.toBe(doName);

		betweenPhases(doName, async function () {
			await db.deleteItem(other);
		});

		const result = await db.transactGetItems({ items: [key] });
		expect(result).toMatchObject({ outcome: "committed", items: [{ found: true, data: "value", version: 1 }] });
	});

	it("the single-partition read fast path applies the same pending-lock classification", async () => {
		const db = makeDB();
		const [key, sibling] = keysInOnePartition(db, 2, "fast-path");
		await db.putItem({ ...key, data: "value" });
		await db.putItem({ ...sibling, data: "sibling" });

		const snapshotSpy = vi.spyOn(PartitionDO.prototype, "txReadSnapshot");
		const twoPhaseSpy = vi.spyOn(PartitionDO.prototype, "txReadForTransaction");

		// A pending check cannot change the item, so the snapshot may serialize on either side of it.
		const releaseCheck = await holdPendingLock(db, key, { operation: "check", condition: itemExists() });
		try {
			const result = await db.transactGetItems({ items: [key, sibling] });
			expect(result).toMatchObject({ outcome: "committed" });
		} finally {
			await releaseCheck();
		}

		// A pending content mutation still aborts the read.
		const releasePut = await holdPendingLock(db, key, { operation: "put", data: "pending", kind: "text" });
		try {
			await expect(db.transactGetItems({ items: [key, sibling] })).rejects.toThrow(fokosErrorWith("pending_write"));
		} finally {
			await releasePut();
		}

		expect(snapshotSpy).toHaveBeenCalledTimes(2);
		expect(twoPhaseSpy).not.toHaveBeenCalled();
	});

	it("a committed put between the phases aborts a projected read", async () => {
		const db = makeDB({ singlePartitionFastPath: false });
		const key = keysInOnePartition(db, 1, "put-between-proj")[0];
		await db.putItem({ ...key, data: { n: 1 } });
		const doName = partitionNameOf(db, key);

		betweenPhases(doName, async function (this: PartitionDO, pCtx) {
			await this.apiPutItem(pCtx, { hashKey: kb(key.hashKey), sortKey: kb(key.sortKey), data: "changed", kind: "text" });
		});

		await expect(db.transactGetItems({ items: [{ ...key, projection: [{ expr: { ref: "data", path: "$.n" } }] }] })).rejects.toThrow(
			fokosErrorWith("read_conflict", { hashKey: key.hashKey }),
		);
	});

	it("a pending content mutation on the item aborts a projected read", async () => {
		const db = makeDB({ singlePartitionFastPath: false });
		const key = keysInOnePartition(db, 1, "pending-put-proj")[0];
		await db.putItem({ ...key, data: { n: 1 } });

		const release = await holdPendingLock(db, key, { operation: "put", data: "pending", kind: "text" });
		try {
			await expect(db.transactGetItems({ items: [{ ...key, projection: [{ expr: { ref: "data", path: "$.n" } }] }] })).rejects.toThrow(
				fokosErrorWith("pending_write"),
			);
		} finally {
			await release();
		}
	});
});
