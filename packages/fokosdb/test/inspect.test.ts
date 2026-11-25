/**
 * `fokosInspect` on a real `FokosDBPartitionDO` and a real `FokosDBTransactionCoordinatorDO`: the
 * committed items, the locks, the transactions, the pages, and the violations of `selfCheck`.
 */
import { runInDurableObject } from "cloudflare:test";
import { describe, it } from "vitest";
import { KeyCodec } from "../src/sharding/key-codec.js";
import { fokosErrorWith } from "./errors-matchers.js";
import { testPartitionStub } from "./stub-helpers.js";
import { controlledCoordinator, holdPendingLock, makeDB, owningPartition } from "./transactions/tx-helpers.js";
import { CONTROLLED_NS, makePartition, PROMOTION_TEST_MAX_SIZE_MB, type TestPartition } from "./partition-do/partition-harness.js";
import { kb } from "./partition-do/helpers.js";

async function allItems(partition: TestPartition): Promise<string[]> {
	const keys: string[] = [];
	let cursor: string | undefined;
	do {
		const page = await partition.stub.fokosInspect({ what: ["items"], ...(cursor ? { cursor } : {}) });
		keys.push(...page.items!.map((item) => `${String(KeyCodec.decode(item.hashKey))}/${String(KeyCodec.decode(item.sortKey))}`));
		cursor = page.cursor;
	} while (cursor !== undefined);
	return keys;
}

describe.concurrent("fokosInspect", () => {
	it("answers an uninitialized partition with no state and does not bring it to life", async ({ expect }) => {
		const stub = testPartitionStub(`test.inspect-uninitialized.${crypto.randomUUID()}`);

		const res = await stub.fokosInspect({ what: ["items", "locks", "repartition", "jobs", "selfCheck"] });

		expect(res).toEqual({ items: [], locks: [], repartition: { role: "leaf" }, jobs: [], selfCheck: { violations: [] } });
		expect(await stub.fokosStatus({})).toMatchObject({ initialized: false });
	});

	it("pages the committed items, shows a lock, and finds no violation", async ({ expect }) => {
		const db = makeDB({ rootTreesN: 1 });
		for (const hashKey of ["a", "b", "c"]) {
			await db.putItem({ hashKey, data: `data-${hashKey}` });
		}
		const { stub } = owningPartition(db, { hashKey: "a", sortKey: "" });
		await holdPendingLock(db, { hashKey: "a", sortKey: "locked" }, { operation: "put", data: "locked", kind: "text" });

		const hashKeys: string[] = [];
		let cursor: string | undefined;
		do {
			const page = await stub.fokosInspect({ what: ["items"], limit: 2, ...(cursor ? { cursor } : {}) });
			expect(page.items!.length).toBeLessThanOrEqual(2);
			hashKeys.push(...page.items!.map((item) => String(KeyCodec.decode(item.hashKey))));
			cursor = page.cursor;
		} while (cursor !== undefined);
		expect(hashKeys.toSorted()).toEqual(["a", "b", "c"]);

		const one = await stub.fokosInspect({ what: ["items", "locks"], hashKey: KeyCodec.encode("b") });
		expect(one.items).toMatchObject([{ version: 1, data: "data-b" }]);
		expect(one.locks).toEqual([]);

		const res = await stub.fokosInspect({ what: ["locks", "repartition", "selfCheck"] });
		expect(res.locks).toMatchObject([{ hashKey: KeyCodec.encode("a"), sortKey: KeyCodec.encode("locked"), op: "put" }]);
		expect(res.repartition).toEqual({ role: "leaf" });
		expect(res.selfCheck).toEqual({ violations: [] });
	});

	it("refuses a bad limit, a bad cursor, and a cursor for more than one paged kind", async ({ expect }) => {
		const db = makeDB({ rootTreesN: 1 });
		await db.putItem({ hashKey: "a", data: "x" });
		const { stub } = owningPartition(db, { hashKey: "a", sortKey: "" });

		await runInDurableObject(stub, async (instance) => {
			const invalid = fokosErrorWith("inspect_request_invalid");
			await expect(instance.fokosInspect({ what: ["items", "locks"], limit: 1 })).rejects.toThrow(invalid);
			await expect(instance.fokosInspect({ what: ["items"], limit: 0 })).rejects.toThrow(invalid);
			await expect(instance.fokosInspect({ what: ["items"], limit: 1.5 })).rejects.toThrow(invalid);
			await expect(instance.fokosInspect({ what: ["items"], cursor: "not json" })).rejects.toThrow(invalid);
			const items = await instance.fokosInspect({ what: ["items"], limit: 1 });
			await expect(
				instance.fokosInspect({ what: ["locks"], cursor: items.cursor ?? JSON.stringify({ kind: "items", after: [] }) }),
			).rejects.toThrow(invalid);
			for (const after of [
				[],
				["00", "00"],
				["00", "00", "00", "00"],
				["zz", "00", "00"],
				["0", "00", "00"],
				["00".repeat(8), "00", 1],
				["00", "00", "00"],
			]) {
				await expect(instance.fokosInspect({ what: ["items"], cursor: JSON.stringify({ kind: "items", after }) })).rejects.toThrow(invalid);
			}
			await expect(
				instance.fokosInspect({ what: ["locks"], cursor: JSON.stringify({ kind: "locks", after: ["00", "00", "00"] }) }),
			).rejects.toThrow(invalid);
			await expect(instance.fokosInspect({ what: ["nope" as never] })).rejects.toThrow(invalid);
			await expect(instance.fokosInspect({ what: ["items"], hashKey: "a" as never })).rejects.toThrow(invalid);
			await expect(instance.fokosInspect({ what: ["items"], range: { start: { hk: kb("a") } as never } })).rejects.toThrow(invalid);
			await expect(instance.fokosInspect({ what: ["items"], range: { from: {} } as never })).rejects.toThrow(invalid);
			await expect(instance.fokosInspect({ what: ["locks"], transactionId: 5 as never })).rejects.toThrow(invalid);
			await expect(instance.fokosInspect({ what: ["transactions"] })).rejects.toThrow(invalid);
		});
	});

	it("pages the items of a range and skips the items outside it", async ({ expect }) => {
		const db = makeDB({ rootTreesN: 1 });
		for (const hashKey of ["a", "b", "c"]) {
			for (const sortKey of ["s1", "s2"]) {
				await db.putItem({ hashKey, sortKey, data: `${hashKey}/${sortKey}` });
			}
		}
		const { stub } = owningPartition(db, { hashKey: "a", sortKey: "" });
		// The range holds b/s2 and c/s1 only. The items in key order of the partition are by routingHash
		// first, so the scan reads items outside the range between the two.
		const range = { start: { hk: kb("b"), sk: kb("s2") }, end: { hk: kb("c"), sk: kb("s2") } };

		const keys: string[] = [];
		let cursor: string | undefined;
		do {
			const page = await stub.fokosInspect({ what: ["items"], range, limit: 1, ...(cursor ? { cursor } : {}) });
			keys.push(...page.items!.map((item) => String(item.data)));
			cursor = page.cursor;
		} while (cursor !== undefined);
		expect(keys.toSorted()).toEqual(["b/s2", "c/s1"]);
	});

	it("pages the locks and refuses two paged kinds when one has more than one page", async ({ expect }) => {
		const db = makeDB({ rootTreesN: 1 });
		for (const sortKey of ["l1", "l2", "l3"]) {
			await holdPendingLock(db, { hashKey: "locks", sortKey }, { operation: "put", data: sortKey, kind: "text" });
		}
		const { stub } = owningPartition(db, { hashKey: "locks", sortKey: "" });

		const sortKeys: string[] = [];
		let cursor: string | undefined;
		do {
			const page = await stub.fokosInspect({ what: ["locks"], limit: 2, ...(cursor ? { cursor } : {}) });
			sortKeys.push(...page.locks!.map((lock) => String(KeyCodec.decode(lock.sortKey))));
			cursor = page.cursor;
		} while (cursor !== undefined);
		expect(sortKeys).toEqual(["l1", "l2", "l3"]);
		const [first] = (await stub.fokosInspect({ what: ["locks"], limit: 1 })).locks!;
		expect((await stub.fokosInspect({ what: ["locks"], transactionId: first.transactionId })).locks).toEqual([first]);

		await runInDurableObject(stub, async (instance) => {
			await expect(instance.fokosInspect({ what: ["items", "locks"], hashKey: KeyCodec.encode("locks") })).resolves.toMatchObject({
				locks: expect.any(Array),
			});
		});
	});

	it("reports a bucket that disagrees with its items, a lock with no info row, and a missing alarm", async ({ expect }) => {
		const db = makeDB({ rootTreesN: 1 });
		const key = { hashKey: "bucket", sortKey: "" };
		for (let i = 0; i < 20; i++) {
			await db.putItem({ hashKey: key.hashKey, sortKey: `s${i}`, data: "x" });
		}
		const { stub } = owningPartition(db, key);
		await holdPendingLock(db, { hashKey: key.hashKey, sortKey: "locked" }, { operation: "put", data: "locked", kind: "text" });
		expect((await stub.fokosInspect({ what: ["selfCheck"] })).selfCheck).toEqual({ violations: [] });
		expect((await stub.fokosInspect({ what: ["jobs"] })).jobs!.length).toBeGreaterThan(0);

		await runInDurableObject(stub, async (_instance, state) => {
			await state.storage.deleteAlarm();
		});
		expect((await stub.fokosInspect({ what: ["selfCheck"] })).selfCheck!.violations).toEqual([
			expect.stringContaining("the alarm (null) is not armed at or before the job stale_tx_recovery"),
		]);

		// The scheduled run of the recovery job stays, so the alarm check still finds the missing alarm.
		await runInDurableObject(stub, (_instance, state) => {
			state.storage.sql.exec("UPDATE item_stats SET item_count = item_count + 1");
			state.storage.sql.exec("DELETE FROM pending_tx_info");
		});
		expect((await stub.fokosInspect({ what: ["selfCheck"] })).selfCheck!.violations).toEqual([
			expect.stringContaining("the alarm (null) is not armed at or before the job stale_tx_recovery"),
			expect.stringContaining("has a lock row and no pending_tx_info row"),
			expect.stringContaining("disagree with its items"),
		]);
	});

	it("reports a routingHash with more than 16 items and no bucket", async ({ expect }) => {
		const db = makeDB({ rootTreesN: 1 });
		for (let i = 0; i < 17; i++) {
			await db.putItem({ hashKey: "many", sortKey: `s${i}`, data: "x" });
		}
		for (let i = 0; i < 3; i++) {
			await db.putItem({ hashKey: "few", sortKey: `s${i}`, data: "x" });
		}
		const { stub } = owningPartition(db, { hashKey: "many", sortKey: "" });
		expect((await stub.fokosInspect({ what: ["selfCheck"] })).selfCheck).toEqual({ violations: [] });

		await runInDurableObject(stub, (_instance, state) => {
			state.storage.sql.exec("DELETE FROM item_stats");
		});
		expect((await stub.fokosInspect({ what: ["selfCheck"] })).selfCheck!.violations).toEqual([
			expect.stringMatching(/^the routingHash [0-9a-f]+ has more than 16 items and no bucket$/),
		]);
		const resumed = await stub.fokosInspect({
			what: ["selfCheck"],
			cursor: JSON.stringify({ kind: "selfCheck", after: ["ff".repeat(8)] }),
		});
		expect(resumed).toEqual({ selfCheck: { violations: [] } });
	});

	it("shows the transactions of a coordinator and a transaction with no recovery", async ({ expect }) => {
		const db = makeDB({ controlled: true });
		await db.transactWriteItems({
			items: [
				{ hashKey: "tx-a", operation: "put", data: "a" },
				{ hashKey: "tx-b", operation: "put", data: "b" },
			],
		});
		const stub = controlledCoordinator(db);

		const res = await stub.fokosInspect({ what: ["transactions", "repartition", "selfCheck"] });
		expect(res.transactions).toHaveLength(1);
		expect(res.transactions![0]).toMatchObject({ state: "COMMITTED" });
		expect(res.selfCheck).toEqual({ violations: [] });

		const transactionId = res.transactions![0].transactionId;
		await runInDurableObject(stub, async (instance) => {
			await expect(instance.fokosInspect({ what: ["transactions"], hashKey: KeyCodec.encode("tx-a") })).rejects.toThrow(
				fokosErrorWith("inspect_request_invalid"),
			);
			await expect(instance.fokosInspect({ what: ["items"] })).rejects.toThrow(fokosErrorWith("inspect_request_invalid"));
		});
		expect((await stub.fokosInspect({ what: ["transactions"], transactionId })).transactions).toHaveLength(1);
		await runInDurableObject(stub, (_instance, state) => {
			state.storage.sql.exec("UPDATE tc_state SET completed_at = NULL, next_recovery_at = NULL");
		});
		expect((await stub.fokosInspect({ what: ["selfCheck"] })).selfCheck!.violations).toEqual([
			expect.stringContaining(`the transaction ${transactionId} in COMMITTED is not complete`),
		]);
	});

	it("shows no item of a router, and only the items that each child owns", async ({ expect }) => {
		const root = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		const written = await root.triggerHashSplit();
		await root.awaitSplitCompleted();
		const children = await root.children();

		const res = await root.stub.fokosInspect({ what: ["items", "locks", "repartition"] });
		expect(res).toMatchObject({ items: [], locks: [], repartition: { role: "router" } });
		expect(res.repartition!.children!.toSorted()).toEqual(children.map((child) => child.doName).toSorted());
		const childItems = (await Promise.all(children.map(allItems))).flat();
		expect(childItems.toSorted()).toEqual(written.map((item) => `${String(KeyCodec.decode(item.hashKey))}/sk`).toSorted());
	}, 60_000);

	it("finds no violation on a promotion source while its cleanup deletes the rows of the promoted key", async ({ expect }) => {
		const partition = makePartition({ ns: CONTROLLED_NS });
		await partition.controlled.testConfig({ cleanupPromotedKeyRows: 1 });
		// One cleanup step, then a long pause: the source keeps most rows of the promoted key.
		await partition.controlled.testRuntimeConfig({ cleanupRetryMs: 60_000 });
		for (let i = 0; i < 40; i++) {
			await partition.put({ hashKey: kb("large"), sortKey: kb(`s${i}`), data: "x", kind: "text" });
		}
		await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("large") });
		await partition.awaitPromotedKeyStatus("large", ["promoted"], { drive: [partition, partition.rangeRoot("large")] });
		expect(await partition.localItemCount("large")).toBeGreaterThan(16);

		expect(await partition.stub.fokosInspect({ what: ["items", "selfCheck"] })).toEqual({ items: [], selfCheck: { violations: [] } });
	}, 60_000);

	it("shows no item of a promoted hash key on the promotion source", async ({ expect }) => {
		const partition = makePartition({ hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		await partition.put({ hashKey: kb("stay"), sortKey: kb("s"), data: "x", kind: "text" });
		await partition.put({ hashKey: kb("alice"), sortKey: kb("s"), data: "x", kind: "text" });
		await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("alice") });
		const rangeRoot = await partition.awaitPromoted("alice");

		expect(await allItems(partition)).toEqual(["stay/s"]);
		expect(await allItems(rangeRoot)).toEqual(["alice/s"]);
	}, 60_000);
});
