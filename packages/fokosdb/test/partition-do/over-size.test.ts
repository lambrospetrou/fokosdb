/**
 * A leaf above 1.1 times its cap refuses every write, so no write applies and signals a split. The
 * refusal itself starts the repartition decision. Each test brings a leaf above its admission limit,
 * sends one refused request, and checks the repartition that the refusal queued, or checks that a
 * leaf at its floor queues none.
 *
 * A test fills a partition under a large cap, then sends its requests with a context that carries a
 * lower cap. The partition stores the policy of the last request, so a test uses the low context
 * for every later request.
 */
import { runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { PartitionDO } from "../../src/server/do-partition.js";
import { DEFAULT_PARTITION_CONFIG } from "../../src/server/host-config.js";
import type { FokosDBRouteContext } from "../../src/shared/partition-context.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { hashChildIndex } from "../../src/sharding/partition-id.js";
import { fokosErrorWith } from "../errors-matchers.js";
import { testCoordinatorRef } from "../stub-helpers.js";
import { captureConsoleError, kb, lockKeys, withOpIndex } from "./helpers.js";
import { CONTROLLED_NS, drainUntil, keepTestLocks, makePartition, makeRangeRoot, TestPartition } from "./partition-harness.js";

const MB = 1024 * 1024;
// The low cap is the file size divided by this factor. The file is then above 1.1 times the cap,
// and a child that holds about half of the data stays below the cap.
const CAP_DIVISOR = 1.3;
const FLOOR_LOG = "fokos/partition: over the split threshold, but no repartition can make this partition smaller";

/** The same partition, with a request context that carries `caps` in MB. */
function withCaps(partition: TestPartition, caps: { hash?: number; range?: number }): TestPartition {
	const { policy } = partition.ctx;
	const ctx: FokosDBRouteContext = {
		...partition.ctx,
		policy: {
			...policy,
			hashSplitConditions: caps.hash === undefined ? policy.hashSplitConditions : { ...policy.hashSplitConditions, maxSizeMb: caps.hash },
			rangeSplitConditions:
				caps.range === undefined ? policy.rangeSplitConditions : { ...policy.rangeSplitConditions, maxSizeMb: caps.range },
		},
	};
	return TestPartition.at(ctx, partition.stub);
}

async function databaseSize(partition: TestPartition): Promise<number> {
	return await runInDurableObject(partition.stub, (_instance: PartitionDO, state: DurableObjectState) => state.storage.sql.databaseSize);
}

/** The same partition with a hash cap that the current file exceeds by `CAP_DIVISOR`. */
async function lowHashCap(partition: TestPartition): Promise<TestPartition> {
	return withCaps(partition, { hash: (await databaseSize(partition)) / CAP_DIVISOR / MB });
}

/** `count` hash keys that the root sends to `childIndex`, balanced over the children of that child. */
function keysForChild(prefix: string, childIndex: number, count: number): string[] {
	const keys: string[] = [];
	for (let i = 0; keys.length < count; i++) {
		const key = `${prefix}-${i}`;
		if (hashChildIndex(kb(key), 0, 2) === childIndex && hashChildIndex(kb(key), 1, 2) === keys.length % 2) {
			keys.push(key);
		}
	}
	return keys;
}

async function putRows(partition: TestPartition, hashKey: string, rows: number, bytes: number): Promise<void> {
	for (let i = 0; i < rows; i++) {
		await partition.put({ hashKey: kb(hashKey), sortKey: kb(`sk${i}`), data: "x".repeat(bytes), kind: "text" });
	}
}

/** Prepares one transaction that puts `sortKeys` of `hashKey`, and returns its id. */
async function prepare(partition: TestPartition, hashKey: string, sortKeys: string[], bytes = 64 * 1024): Promise<string> {
	const transactionId = crypto.randomUUID();
	const res = await partition.rpc.txPrepare(partition.ctx, {
		transactionId,
		transactionTimestamp: Date.now(),
		coordinator: testCoordinatorRef(),
		items: withOpIndex(
			sortKeys.map((sk) => ({
				hashKey: kb(hashKey),
				sortKey: kb(sk),
				operation: "put" as const,
				data: "p".repeat(bytes),
				kind: "text" as const,
			})),
		),
	});
	expect(res.outcome).toBe("accepted");
	return transactionId;
}

/** Quarantines every lock row of `transactionId`, as the stale job does for an over-age lock. */
async function quarantine(partition: TestPartition, transactionId: string, guardedAt: number): Promise<void> {
	await runInDurableObject(partition.stub, (_instance: PartitionDO, state: DurableObjectState) => {
		const store = new PartitionStore(state.storage);
		expect(store.guardPendingTx(transactionId, guardedAt)).toBe(true);
	});
}

async function guardsOf(partition: TestPartition, transactionId: string): Promise<(number | null)[]> {
	return await runInDurableObject(partition.stub, (_instance: PartitionDO, state: DurableObjectState) =>
		new PartitionStore(state.storage).listPendingTxItems(transactionId).map((row) => row.guarded_at),
	);
}

async function expectRefused(partition: TestPartition, hashKey: string): Promise<void> {
	await expect(partition.put({ hashKey: kb(hashKey), sortKey: kb("new"), data: "v", kind: "text" })).rejects.toThrow(
		fokosErrorWith("partition_over_size"),
	);
}

async function awaitSplitQueued(partition: TestPartition): Promise<void> {
	await vi.waitFor(async () => expect((await partition.status()).splitStatus).toBeDefined(), { timeout: 5_000, interval: 10 });
}

async function awaitPromotionQueued(partition: TestPartition, hashKey: string): Promise<void> {
	await vi.waitFor(async () => expect(await partition.promotedKeyStatus(hashKey)).toBeDefined(), { timeout: 5_000, interval: 10 });
}

describe("PartitionDO - a hash leaf that refuses a write for size", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	it.each(["apiPutItem", "txPrepare"] as const)("queues a hash split when it refuses %s, and a later request succeeds", async (op) => {
		const partition = makePartition({ hashSplitN: 2 });
		const keys = [...keysForChild("refused", 0, 8), ...keysForChild("refused", 1, 8)];
		for (const key of keys) {
			await putRows(partition, key, 2, 16 * 1024);
		}
		const low = await lowHashCap(partition);

		const send = (hashKey: string) =>
			op === "apiPutItem"
				? low.put({ hashKey: kb(hashKey), sortKey: kb("new"), data: "v", kind: "text" })
				: prepare(low, hashKey, ["new"], 16);
		await expect(send("after-cap")).rejects.toThrow(fokosErrorWith("partition_over_size"));
		await awaitSplitQueued(low);
		await low.awaitSplitCompleted();

		await send("after-split");
		for (const key of keys) {
			expect(await low.get({ hashKey: kb(key), sortKey: kb("sk1") })).toMatchObject({ found: true });
		}
	});

	it("splits a child that its import leaves over the cap, at the first write that the child refuses", async () => {
		const partition = makePartition({ hashSplitN: 2 });
		const heavy = keysForChild("born-over", 0, 8);
		for (const key of heavy) {
			await putRows(partition, key, 4, 16 * 1024);
		}
		const light = keysForChild("born-over", 1, 1)[0];
		await putRows(partition, light, 1, 1024);
		const low = await lowHashCap(partition);

		await expectRefused(low, light);
		await awaitSplitQueued(low);
		await low.awaitSplitCompleted();
		const child = await low.childOwning(heavy[0]);
		expect((await child.status()).splitStatus).toBeUndefined();

		// The import gave the child almost all of the data, above its own admission limit.
		await expectRefused(low, heavy[0]);
		await awaitSplitQueued(child);
		await child.awaitSplitCompleted();
		await low.put({ hashKey: kb(heavy[0]), sortKey: kb("new"), data: "v", kind: "text" });
	});

	it("promotes the one hash key of a leaf whose estimate is below the promotion threshold", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS });
		const rangeRoot = partition.rangeRoot("solo");
		await keepTestLocks(partition, rangeRoot);
		// The pending payloads make the file large, and the estimate counts the committed items only.
		await putRows(partition, "solo", 4, 1024);
		const transactionId = await prepare(partition, "solo", ["p0", "p1", "p2", "p3"]);
		const low = await lowHashCap(partition);
		const threshold = (low.ctx.policy.hashSplitConditions.maxSizeMb ?? 0) * MB * DEFAULT_PARTITION_CONFIG.promotionFraction;
		const estimate = await runInDurableObject(
			partition.stub,
			(_instance: PartitionDO, state: DurableObjectState) =>
				state.storage.sql.exec<{ est_bytes: number }>(`SELECT est_bytes FROM key_size_estimates WHERE hk = ?`, kb("solo")).one().est_bytes,
		);
		expect(estimate).toBeLessThan(threshold);

		await expectRefused(low, "solo");
		await awaitPromotionQueued(low, "solo");
		expect((await low.status()).splitStatus).toBeUndefined();

		const root = await low.awaitPromoted("solo");
		expect(await root.get({ hashKey: kb("solo"), sortKey: kb("sk3") })).toMatchObject({ found: true });
		expect(await lockKeys(root.stub, transactionId)).toEqual(["solo/p0", "solo/p1", "solo/p2", "solo/p3"]);
	});

	it("queues a hash split while a promotion cleans up, and no child receives a row of the promoted key", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitN: 2 });
		await partition.controlled.testConfig({ promotedKeyCleanupRows: 1 });
		// One cleanup step, then a long pause: the promotion stays `completed` for the test.
		await partition.controlled.testRuntimeConfig({ cleanupRetryMs: 60_000 });
		await putRows(partition, "large", 40, 12 * 1024);
		const small = [...keysForChild("small", 0, 4), ...keysForChild("small", 1, 4)];
		for (const key of small) {
			await putRows(partition, key, 2, 12 * 1024);
		}
		await partition.rpc.debugForcePromoteKey(partition.ctx, { hashKey: kb("large") });
		const rangeRoot = partition.rangeRoot("large");
		await partition.awaitPromotedKeyStatus("large", ["promoted"], { drive: [partition, rangeRoot] });
		expect(await partition.localItemCount("large")).toBeGreaterThan(0);
		// The completion deleted the estimate, so the decision does not name the key again.
		await runInDurableObject(partition.stub, (_instance: PartitionDO, state: DurableObjectState) => {
			expect(state.storage.sql.exec(`SELECT 1 FROM key_size_estimates WHERE hk = ?`, kb("large")).toArray()).toEqual([]);
		});
		const low = await lowHashCap(partition);

		await expectRefused(low, small[0]);
		await awaitSplitQueued(low);
		await runInDurableObject(low.stub, (_instance: PartitionDO, state: DurableObjectState) => {
			const promotion = state.storage.sql
				.exec<{ state: string }>(`SELECT state FROM fokos_repartitions WHERE kind = 'key_promotion'`)
				.one();
			expect(promotion.state).toBe("completed");
		});
		await low.awaitSplitCompleted();

		for (const child of await low.children()) {
			expect(await child.localItemCount("large")).toBe(0);
		}
		for (const key of small) {
			expect(await low.get({ hashKey: kb(key), sortKey: kb("sk1") })).toMatchObject({ found: true });
		}
		expect(await low.get({ hashKey: kb("large"), sortKey: kb("sk39") })).toMatchObject({ found: true });
	});
});

describe("PartitionDO - a hash leaf with more than one large key", () => {
	it("promotes the second large key while the promotion of the first cannot finish", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS });
		// Both keys stay below the promotion threshold of the large cap, so no write promotes them.
		await putRows(partition, "first", 24, 16 * 1024);
		await putRows(partition, "second", 12, 16 * 1024);
		const low = await lowHashCap(partition);
		const threshold = (low.ctx.policy.hashSplitConditions.maxSizeMb ?? 0) * MB * DEFAULT_PARTITION_CONFIG.promotionFraction;
		const estimates = await runInDurableObject(partition.stub, (_instance: PartitionDO, state: DurableObjectState) =>
			state.storage.sql.exec<{ est_bytes: number }>(`SELECT est_bytes FROM key_size_estimates ORDER BY est_bytes DESC`).toArray(),
		);
		expect(estimates.map((row) => row.est_bytes >= threshold)).toEqual([true, true]);

		const firstRoot = low.rangeRoot("first");
		await low.controlled.testHoldPulls({ stream: "items", target: firstRoot.doName });
		try {
			await low.rpc.debugForcePromoteKey(low.ctx, { hashKey: kb("first") });
			await low.awaitPromotedKeyStatus("first", ["promoting"]);
			await vi.waitFor(async () => expect((await low.controlled.testPullStats()).heldTargets).toContain(firstRoot.doName), {
				timeout: 10_000,
				interval: 10,
			});
			expect((await firstRoot.status()).migrationStatus).not.toBe("migration_completed");

			await expectRefused(low, "second");
			await awaitPromotionQueued(low, "second");
			const secondRoot = await low.awaitPromoted("second");

			expect((await low.status()).splitStatus).toBeUndefined();
			expect(await secondRoot.get({ hashKey: kb("second"), sortKey: kb("sk11") })).toMatchObject({
				found: true,
				item: { data: "x".repeat(16 * 1024) },
			});
			expect(await low.promotedKeyStatus("first")).toBe("promoting");
		} finally {
			await low.controlled.testReleasePulls();
		}
		await low.awaitPromoted("first");
	});
});

describe("PartitionDO - a hash leaf that holds pending payloads only", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	it.each([false, true])("splits between two pending keys, and each child imports its locks (quarantined: %s)", async (quarantined) => {
		const partition = makePartition({ ns: CONTROLLED_NS, hashSplitN: 2 });
		await keepTestLocks(partition, ...partition.hashChildren());
		const [first] = keysForChild("pending", 0, 1);
		const [second] = keysForChild("pending", 1, 1);
		const transactions = [
			await prepare(partition, first, ["p0", "p1", "p2", "p3"]),
			await prepare(partition, second, ["p0", "p1", "p2", "p3"]),
		];
		if (quarantined) {
			for (const transactionId of transactions) {
				await quarantine(partition, transactionId, 77);
			}
		}
		const low = await lowHashCap(partition);

		await expectRefused(low, "unrelated");
		await awaitSplitQueued(low);
		await low.awaitSplitCompleted();

		for (const [i, hashKey] of [first, second].entries()) {
			const child = await low.childOwning(hashKey);
			expect(await lockKeys(child.stub, transactions[i])).toEqual(["p0", "p1", "p2", "p3"].map((sk) => `${hashKey}/${sk}`));
			expect(await guardsOf(child, transactions[i])).toEqual(Array(4).fill(quarantined ? 77 : null));
		}
		await low.put({ hashKey: kb("unrelated"), sortKey: kb("new"), data: "v", kind: "text" });
	});

	it("promotes the one pending key, and the range root imports every payload and guard", async () => {
		const partition = makePartition({ ns: CONTROLLED_NS });
		const rangeRoot = partition.rangeRoot("solo");
		await keepTestLocks(partition, rangeRoot);
		const open = await prepare(partition, "solo", ["p0", "p1"]);
		const guarded = await prepare(partition, "solo", ["p2", "p3"]);
		await quarantine(partition, guarded, 77);
		const low = await lowHashCap(partition);

		await expectRefused(low, "other");
		await awaitPromotionQueued(low, "solo");
		expect((await low.status()).splitStatus).toBeUndefined();

		const root = await low.awaitPromoted("solo");
		expect(await lockKeys(root.stub, open)).toEqual(["solo/p0", "solo/p1"]);
		expect(await guardsOf(root, open)).toEqual([null, null]);
		expect(await lockKeys(root.stub, guarded)).toEqual(["solo/p2", "solo/p3"]);
		expect(await guardsOf(root, guarded)).toEqual([77, 77]);
	});

	it.each([false, true])(
		"promotes the committed key first, then the pending key after the cleanup (pending rows under the committed key: %s)",
		async (pendingUnderA) => {
			const partition = makePartition({ ns: CONTROLLED_NS });
			const rootA = partition.rangeRoot("alpha");
			const rootB = partition.rangeRoot("beta");
			await keepTestLocks(partition, rootA, rootB);
			await putRows(partition, "alpha", 4, 1024);
			const txA = pendingUnderA ? await prepare(partition, "alpha", ["p0"], 1024) : undefined;
			const txB = await prepare(partition, "beta", ["p0", "p1", "p2", "p3"]);
			await quarantine(partition, txB, 77);
			const low = await lowHashCap(partition);

			await expectRefused(low, "other");
			await awaitPromotionQueued(low, "alpha");
			expect(await low.promotedKeyStatus("beta")).toBeUndefined();
			await drainUntil([low, rootA], async () => (await low.localItemCount("alpha")) === 0, "the cleanup of alpha");
			if (txA) {
				expect(await lockKeys(rootA.stub, txA)).toEqual(["alpha/p0"]);
				expect(await lockKeys(low.stub, txA)).toEqual([]);
			}
			expect(await lockKeys(low.stub, txB)).toEqual(["beta/p0", "beta/p1", "beta/p2", "beta/p3"]);

			await expectRefused(low, "other");
			await awaitPromotionQueued(low, "beta");
			const root = await low.awaitPromoted("beta");
			expect(await lockKeys(root.stub, txB)).toEqual(["beta/p0", "beta/p1", "beta/p2", "beta/p3"]);
			expect(await guardsOf(root, txB)).toEqual([77, 77, 77, 77]);
			expect((await low.status()).splitStatus).toBeUndefined();
		},
	);
});

describe("PartitionDO - a range leaf at its floor", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	it.each([
		{ reason: "fewer_items", rows: [16, 16, 16] },
		{ reason: "skewed_bytes", rows: [150, 1, 1, 1] },
	])("queues no split and logs the floor once: $reason", async ({ reason, rows }) => {
		const { root } = await makeRangeRoot(4, { rangeSplitConditions: { maxSizeMb: 500 } });
		for (const [i, kib] of rows.entries()) {
			await root.put({ hashKey: kb("alice"), sortKey: kb(`sk${i}`), data: "x".repeat(kib * 1024), kind: "text" });
		}
		const low = withCaps(root, { range: (await databaseSize(root)) / CAP_DIVISOR / MB });
		const logged = captureConsoleError();

		for (let i = 0; i < 3; i++) {
			await expectRefused(low, "alice");
		}

		expect((await low.status()).splitStatus).toBeUndefined();
		expect(logged.withMessage(FLOOR_LOG)).toEqual([expect.objectContaining({ reason, doName: root.doName })]);
		await runInDurableObject(low.stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			expect(await state.storage.getAlarm()).toBeNull();
		});
	});
});
