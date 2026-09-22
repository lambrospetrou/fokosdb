import { describe, expect, it } from "vitest";
import { compileConditionExpression } from "../../src/shared/expression/compiler.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { fokosErrorWith } from "../errors-matchers.js";
import {
	betweenPhases,
	controlledPartition,
	holdPendingLock,
	keysAcrossPartitions,
	keysInOnePartition,
	makeDB,
	partitionNameOf,
} from "./tx-helpers.js";

const kb = (s: string) => KeyCodec.encode(s);

const itemExists = () => compileConditionExpression({ op: "exists", args: [{ ref: "hashKey" }] });

describe("transactGetItems — read revisions and pending checks", () => {
	it("a committed check between the phases does not abort the read", async () => {
		const db = makeDB({ singlePartitionFastPath: false, controlled: true });
		const key = keysInOnePartition(db, 1, "check-between")[0];
		await db.putItem({ ...key, data: "value" });

		// A check advances only the read watermark: `version` and the delete revision stand still.
		const result = await betweenPhases(
			db,
			key,
			async (instance, _state, pCtx) => {
				await instance.txExecuteSingleShot(pCtx, {
					items: [{ opIndex: 0, hashKey: kb(key.hashKey), sortKey: kb(key.sortKey), operation: "check", condition: itemExists() }],
				});
			},
			() => db.transactGetItems({ items: [key] }),
		);
		expect(result).toMatchObject({ items: [{ found: true, data: "value", version: 1 }] });
	});

	it("a pending check on the item does not abort the read", async () => {
		const db = makeDB({ singlePartitionFastPath: false });
		const key = keysInOnePartition(db, 1, "pending-check")[0];
		await db.putItem({ ...key, data: "value" });

		const release = await holdPendingLock(db, key, { operation: "check", condition: itemExists() });
		try {
			const result = await db.transactGetItems({ items: [key] });
			expect(result).toMatchObject({ items: [{ found: true, data: "value", version: 1 }] });
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
		const db = makeDB({ singlePartitionFastPath: false, controlled: true });
		const key = keysInOnePartition(db, 1, "put-between")[0];
		await db.putItem({ ...key, data: "value" });

		const read = betweenPhases(
			db,
			key,
			async (instance, _state, pCtx) => {
				await instance.apiPutItem(pCtx, { hashKey: kb(key.hashKey), sortKey: kb(key.sortKey), data: "changed", kind: "text" });
			},
			() => db.transactGetItems({ items: [key] }),
		);
		await expect(read).rejects.toThrow(fokosErrorWith("read_conflict", { hashKey: key.hashKey }));
	});

	it("a delete and recreate that lands back on the same version still aborts the read", async () => {
		const db = makeDB({ singlePartitionFastPath: false, controlled: true });
		const key = keysInOnePartition(db, 1, "recreate")[0];
		await db.putItem({ ...key, data: "value" });

		// The recreated item reads back at version 1 — the same `v` phase 1 saw — so `found` and
		// `version` agree across the phases. Only the delete revision moved.
		const read = betweenPhases(
			db,
			key,
			async (instance, _state, pCtx) => {
				await instance.apiDeleteItem(pCtx, { hashKey: kb(key.hashKey), sortKey: kb(key.sortKey) });
				await instance.apiPutItem(pCtx, { hashKey: kb(key.hashKey), sortKey: kb(key.sortKey), data: "recreated", kind: "text" });
			},
			() => db.transactGetItems({ items: [key] }),
		);
		await expect(read).rejects.toThrow(fokosErrorWith("read_conflict", { hashKey: key.hashKey }));
	});

	it("a create and delete of an absent item between the phases aborts the read", async () => {
		const db = makeDB({ singlePartitionFastPath: false, controlled: true });
		const key = keysInOnePartition(db, 1, "absent-flip")[0];

		// The item is absent in both phases, so `found` agrees and there is no `version` to compare.
		// Only the delete revision moved.
		const read = betweenPhases(
			db,
			key,
			async (instance, _state, pCtx) => {
				await instance.apiPutItem(pCtx, { hashKey: kb(key.hashKey), sortKey: kb(key.sortKey), data: "flicker", kind: "text" });
				await instance.apiDeleteItem(pCtx, { hashKey: kb(key.hashKey), sortKey: kb(key.sortKey) });
			},
			() => db.transactGetItems({ items: [key] }),
		);
		await expect(read).rejects.toThrow(fokosErrorWith("read_conflict", { hashKey: key.hashKey }));
	});

	it("an unrelated user delete in the same partition aborts the read", async () => {
		const db = makeDB({ singlePartitionFastPath: false, controlled: true });
		const [key, sibling] = keysInOnePartition(db, 2, "unrelated-delete");
		await db.putItem({ ...key, data: "value" });
		await db.putItem({ ...sibling, data: "sibling" });

		// The delete revision is partition-wide, so a delete of an item the read never asked about
		// still moves it. That makes this abort conservative: the read could have been answered.
		const read = betweenPhases(
			db,
			key,
			async (instance, _state, pCtx) => {
				await instance.apiDeleteItem(pCtx, { hashKey: kb(sibling.hashKey), sortKey: kb(sibling.sortKey) });
			},
			() => db.transactGetItems({ items: [key] }),
		);
		await expect(read).rejects.toThrow(fokosErrorWith("read_conflict", { hashKey: key.hashKey }));
	});

	it("a TTL sweep between the phases does not abort the read", async () => {
		const db = makeDB({ singlePartitionFastPath: false, controlled: true });
		const [key, sibling] = keysInOnePartition(db, 2, "ttl-sweep");
		await db.putItem({ ...key, data: "value" });
		const ttlAt = Math.floor(Date.now() / 1000) + 3600;
		await db.putItem({ ...sibling, data: "expiring", ttlAt });

		// The sweep removes the expired sibling. It advances the transaction order watermark but
		// counts no user delete, so the delete revision — and the read — is undisturbed.
		const result = await betweenPhases(
			db,
			key,
			async (_instance, state) => {
				expect(new PartitionStore(state.storage).deleteExpiredItems(ttlAt + 1, 100)).toMatchObject({ deletedRows: 1 });
			},
			() => db.transactGetItems({ items: [key] }),
		);
		expect(result).toMatchObject({ items: [{ found: true, data: "value", version: 1 }] });
	});

	it("a user delete in a different partition does not change this partition's revision", async () => {
		const db = makeDB({ singlePartitionFastPath: false, controlled: true });
		const [key, other] = keysAcrossPartitions(db, 2, "other-partition");
		await db.putItem({ ...key, data: "value" });
		await db.putItem({ ...other, data: "other" });
		expect(partitionNameOf(db, other)).not.toBe(partitionNameOf(db, key));

		const result = await betweenPhases(
			db,
			key,
			async () => {
				await db.deleteItem(other);
			},
			() => db.transactGetItems({ items: [key] }),
		);
		expect(result).toMatchObject({ items: [{ found: true, data: "value", version: 1 }] });
	});

	it("the single-partition read fast path applies the same pending-lock classification", async () => {
		const db = makeDB({ controlled: true });
		const [key, sibling] = keysInOnePartition(db, 2, "fast-path");
		await db.putItem({ ...key, data: "value" });
		await db.putItem({ ...sibling, data: "sibling" });

		const partition = controlledPartition(db, key);

		// A pending check cannot change the item, so the snapshot may serialize on either side of it.
		const releaseCheck = await holdPendingLock(db, key, { operation: "check", condition: itemExists() });
		try {
			const result = await db.transactGetItems({ items: [key, sibling] });
			expect(result.items).toMatchObject([
				{ found: true, data: "value" },
				{ found: true, data: "sibling" },
			]);
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

		expect(await partition.testTxCalls("txReadSnapshot")).toHaveLength(2);
		expect(await partition.testTxCalls("txReadForTransaction")).toHaveLength(0);
	});

	it("a committed put between the phases aborts a projected read", async () => {
		const db = makeDB({ singlePartitionFastPath: false, controlled: true });
		const key = keysInOnePartition(db, 1, "put-between-proj")[0];
		await db.putItem({ ...key, data: { n: 1 } });

		const read = betweenPhases(
			db,
			key,
			async (instance, _state, pCtx) => {
				await instance.apiPutItem(pCtx, { hashKey: kb(key.hashKey), sortKey: kb(key.sortKey), data: "changed", kind: "text" });
			},
			() => db.transactGetItems({ items: [{ ...key, projection: [{ expr: { ref: "data", path: "$.n" } }] }] }),
		);
		await expect(read).rejects.toThrow(fokosErrorWith("read_conflict", { hashKey: key.hashKey }));
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
