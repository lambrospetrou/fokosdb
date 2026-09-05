/**
 * Shared setup for the PartitionDO suites in this folder.
 *
 * Everything here builds or drives a real PartitionDO: it creates a partition with an isolated
 * table name, grows one past a split threshold, and drains the alarms that carry a split and its
 * child migrations to completion.
 */
import { env } from "cloudflare:workers";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { expect, vi } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import { isPartitionExceededDatabaseSizeError } from "../../src/shared/partition-errors.js";
import { PartitionContextCreator } from "../../src/shared/partition-topology/partition-context.js";
import type { PartitionContextResolved } from "../../src/shared/partition-topology/partition-context.js";
import { resolveRangePartitionContext } from "../../src/shared/partition-topology/partition-id.js";
import { PartitionTopologyRouterImpl } from "../../src/shared/partition-topology/router.js";
import { RANGE_PROMOTION_FRACTION } from "../../src/shared/partition-topology/split-policy.js";
import type { SplitStatusKVItem } from "../../src/shared/partition-topology/split-state.js";
import { KeyCodec } from "../../src/shared/partition-topology/key-codec.js";
import invariant from "../../src/shared/invariant.js";
import { MAX_ITEM_BYTES } from "../../src/shared/transaction-limits.js";
import type { PromotedKeyStatus } from "../../src/shared/partition/partition-store.js";
import { compileConditionExpression } from "../../src/shared/expression/compiler.js";
import type { ConditionExpression } from "../../src/shared/expression/types.js";

export const kb = (s?: string) => KeyCodec.encodeOptional(s);
export const compiledCondition = (condition: ConditionExpression) => compileConditionExpression(condition);

export type SplitStartedOrCompleted = Extract<SplitStatusKVItem, { status: "split_started" | "split_completed" }>;

export async function waitForAlarm(stub: DurableObjectStub<PartitionDO>) {
	// runDurableObjectAlarm drains any pending alarm. The auto-fired alarm (from Miniflare
	// detecting an immediate schedule) may still be in progress when putItem returns.
	// We also need to wait for any background work scheduled via setTimeout (scheduleBackgroundWork),
	// which bypasses the alarm path entirely — those are tracked by __testing__backgroundWorkRunning.
	await runDurableObjectAlarm(stub);
	await runInDurableObject(stub, async (instance: PartitionDO) => {
		await vi.waitUntil(() => !instance.__testing__alarm_running && !instance.__testing__backgroundWorkRunning, {
			timeout: 5000,
			interval: 100,
		});
	});
}

// Chunk size for filler writes, as a fraction of the promotion threshold. The ONLY property this
// factor must have is being below 1, so no filler key crosses the promotion threshold and promotes
// itself instead of contributing to a split. 0.7 is margin, not a derived number.
//
// It is NOT small enough to keep a crossing write inside the 10% reject grace band. That would need
// <= 0.4: the chunk is RANGE_PROMOTION_FRACTION (0.25) * this * maxSizeMb, against a band of
// 0.1 * maxSizeMb. Both fillers below are fine with overshooting, for different reasons — see each.
const FILLER_CHUNK_FRACTION = 0.7;

/** Upper bound on filler writes, so a helper that never reaches its goal fails instead of hanging. */
const MAX_FILLER_WRITES = 50;

/**
 * One filler item's payload for a partition whose limit is `maxSizeMb`, sized so that no single key
 * can reach the promotion threshold on its own.
 *
 * `apiPutItem` does NOT enforce the per-item ceiling — that lives in db.ts and the coordinator — so a
 * large `maxSizeMb` would silently write items no real client could send, and the tests would stop
 * resembling the system they stand in for. The assertion keeps that honest.
 */
function fillerChunk(maxSizeMb: number): string {
	const chunkBytes = Math.floor(RANGE_PROMOTION_FRACTION * maxSizeMb * 1024 * 1024 * FILLER_CHUNK_FRACTION);
	expect(chunkBytes, "filler chunk exceeds MAX_ITEM_BYTES; lower maxSizeMb or FILLER_CHUNK_FRACTION").toBeLessThanOrEqual(MAX_ITEM_BYTES);
	return "x".repeat(chunkBytes);
}

/**
 * Writes enough data spread across multiple hash keys to push the DB over maxSizeMb
 * without any single hash key accumulating enough data to trigger range-key promotion
 * (which would block the hash split via mutual exclusion in shouldSplit).
 */
export async function triggerHashSplitThreshold(
	stub: DurableObjectStub<PartitionDO>,
	ctx: PartitionContextResolved,
	maxSizeMb: number = 1,
): Promise<void> {
	// Overshooting the grace band is expected here: the loop below treats the over-size rejection as a
	// normal exit, because it means a prior write already crossed the split threshold.
	const data = fillerChunk(maxSizeMb);
	for (let i = 0; i < MAX_FILLER_WRITES; i++) {
		try {
			await stub.apiPutItem(ctx, { hashKey: kb(`_split_trig_${i}`), sortKey: kb("sk"), data, kind: "bytes" });
		} catch (e) {
			// An over-size rejection means a prior write already crossed the split threshold.
			if (!isPartitionExceededDatabaseSizeError(e)) throw e;
			return;
		}
		const { splitStatus } = await stub.status();
		if (splitStatus?.status === "split_queued") return;
	}
	throw new Error(`triggerHashSplitThreshold: no split queued after ${MAX_FILLER_WRITES} writes`);
}

/**
 * Recursively drains all pending alarms in the split tree rooted at `stub`.
 * For each node: runs any pending alarm (startSplit or nothing if already done),
 * then for each child still awaiting migration triggers its alarm via a dummy request
 * (which sets the alarm and throws "split in progress" — the error is swallowed),
 * and finally recurses into each child to drain migration and any further splits.
 */
export async function drainSplitTree(stub: DurableObjectStub<PartitionDO>): Promise<void> {
	await waitForAlarm(stub);
	const state = await stub.status();

	if (!state.splitStatus || state.splitStatus.status === "split_queued") return;

	const splitStatus = expectSplitStatus(state.splitStatus, state.partitionContext?.doName);
	for (const childCtx of splitStatus.childPartitionContexts) {
		const childStub = PartitionDO.getByName(env.PARTITION_DO, childCtx.doName);

		const childState = await childStub.status();
		if (childState.migrationStatus === "migration_initialized" || childState.migrationStatus === "migration_migrating") {
			await childStub.internalTriggerMigration();
		}

		await drainSplitTree(childStub);
	}
}

/**
 * Narrows an already-read split status to a started or completed split. Tests that reach for
 * `childPartitionContexts` need that narrowing; failing here reports the status the partition was
 * actually in, instead of surfacing an `undefined` several lines later.
 */
export function expectSplitStatus(status: SplitStatusKVItem | undefined, doName?: string): SplitStartedOrCompleted {
	invariant(
		status?.status === "split_started" || status?.status === "split_completed",
		`${doName ?? "partition"}: expected a started or completed split, got ${status?.status ?? "none"}`,
	);
	return status;
}

/** Reads a partition's split status and narrows it with `expectSplitStatus`. */
export async function splitStatusOf(stub: DurableObjectStub<PartitionDO>): Promise<SplitStartedOrCompleted> {
	const state = await stub.status();
	return expectSplitStatus(state.splitStatus, state.partitionContext?.doName);
}

/** The promotion status this partition holds for `hashKey`, or undefined if it holds no entry. */
export async function promotedKeyStatus(
	stub: DurableObjectStub<PartitionDO>,
	hashKey: string,
	ctx?: PartitionContextResolved,
): Promise<PromotedKeyStatus | undefined> {
	const { promotedKeys } = await stub.status(ctx);
	return promotedKeys.find((e) => KeyCodec.compare(e.hashKey, kb(hashKey)) === 0)?.status;
}

/**
 * Drives the given partitions' alarms and background work until `check` passes.
 *
 * Promotion and migration only advance on a background cycle, so a test cannot simply poll: each
 * attempt has to run the alarms again. `label` completes the sentence "timed out waiting for ...".
 */
export async function drainUntil(
	drain: DurableObjectStub<PartitionDO>[],
	check: () => Promise<boolean>,
	label: string,
	timeoutMs = 5000,
): Promise<void> {
	await vi.waitFor(
		async () => {
			for (const stub of drain) await waitForAlarm(stub);
			if (!(await check())) throw new Error(`timed out waiting for ${label}`);
		},
		{ timeout: timeoutMs, interval: 100 },
	);
}

/**
 * Drains alarms until `stub` reports one of `statuses` for `hashKey`. By default it drains the same
 * partition it reads; pass `drain` when another partition owns the work that moves the status, such
 * as a range root finishing its migration.
 */
export async function waitForPromotedKeyStatus(
	stub: DurableObjectStub<PartitionDO>,
	hashKey: string,
	statuses: readonly PromotedKeyStatus[],
	opts?: { drain?: DurableObjectStub<PartitionDO>[]; timeoutMs?: number },
): Promise<void> {
	await drainUntil(
		opts?.drain ?? [stub],
		async () => {
			const status = await promotedKeyStatus(stub, hashKey);
			return status !== undefined && statuses.includes(status);
		},
		`"${hashKey}" to reach ${statuses.join(" or ")}`,
		opts?.timeoutMs,
	);
}

/**
 * Recursively walks the split tree rooted at `nodeStub` and asserts every node that has split
 * has reached split_completed. Returns the count of split nodes (non-leaf nodes).
 */
export async function assertSplitTreeComplete(nodeStub: DurableObjectStub<PartitionDO>): Promise<number> {
	const state = await nodeStub.status();
	if (!state.splitStatus) return 0;
	expect(state.splitStatus.status, `DO ${state.partitionContext?.doName} should be split_completed`).toBe("split_completed");
	const split = expectSplitStatus(state.splitStatus, state.partitionContext?.doName);
	let count = 1;
	for (const childCtx of split.childPartitionContexts) {
		count += await assertSplitTreeComplete(PartitionDO.getByName(env.PARTITION_DO, childCtx.doName));
	}
	return count;
}

/** A structured log entry as the partition DO writes it to console.error. */
export type LoggedEntry = { message?: string; transactionId?: string; [key: string]: unknown };

/**
 * Silences console.error for the test and collects what was written, so a test can assert on the
 * DO's structured logs without the entries reaching the run output.
 */
export function captureConsoleError() {
	const spy = vi.spyOn(console, "error").mockImplementation(() => {});
	return {
		spy,
		/** Every entry logged with this `message`, in the order it was logged. */
		withMessage(message: string): LoggedEntry[] {
			return spy.mock.calls.map(([entry]) => entry as LoggedEntry).filter((log) => log.message === message);
		},
	};
}

export function makeStub(opts?: Partial<Parameters<typeof PartitionContextCreator.create>[0]>) {
	const prefix = `test.${crypto.randomUUID()}`;
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName: prefix,
		// For testing determinism only one root partition.
		rootTreesN: 1,
		hashSplitN: 2,
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: 100 },
		rangeSplitConditions: { maxSizeMb: 500 },
		...opts,
	});
	const pCtxResolved = new PartitionTopologyRouterImpl(base).pickPartition(kb("dummyHashKey"));
	const stub = PartitionDO.get(env.PARTITION_DO, pCtxResolved.doId);
	return { ctx: pCtxResolved.partitionContext, stub: stub };
}

// ─── Promotion lifecycle ──────────────────────────────────────────────────────

// Promotion and the write-reject guard read two DIFFERENT numbers, and these tests stay stable by
// keeping them far apart:
//
//   - promotion fires when ONE KEY's estimate passes maxSizeMb * RANGE_PROMOTION_FRACTION;
//   - writes are rejected when the WHOLE DATABASE passes maxSizeMb * 1.1.
//
// One key holding ~30% of the budget therefore promotes while the database sits at ~35% of it, three
// times under the reject edge. An earlier maxSizeMb=0.1 left a 10 KB margin, and one
// index page broke two tests.
//
// Only one test needs the database ABOVE maxSizeMb, and it gets there through growPastSplitThreshold,
// which writes until the DO reports that size. Nothing here infers a size from an item count.
export const PROMOTION_TEST_MAX_SIZE_MB = 1;
export const PROMOTION_BIG_DATA = "x".repeat(300 * 1024); // > 25% of 1 MB, and under MAX_ITEM_BYTES (400 KB)

/**
 * Grows the partition past its promotion size threshold with filler keys, and returns the database
 * size reached. Splits are only ever evaluated on a write (`checkSplits`, called from the putItem
 * path), so a test that wants "a split is warranted but must not happen" has to write its way there.
 *
 * Overshooting the reject grace band is harmless here, which is why no rejection is caught: this
 * returns the moment the size passes `target` and never writes again. A rejection needs
 * `dbSize > maxSizeMb * 1.1`, which implies `dbSize > maxSizeMb`, so the previous write would already
 * have returned. `shouldAllow` also runs BEFORE the write, so the write that carries the database
 * across `maxSizeMb` is itself still accepted.
 */
export async function growPastSplitThreshold(
	stub: DurableObjectStub<PartitionDO>,
	ctx: PartitionContextResolved,
	maxSizeMb: number,
): Promise<number> {
	const target = maxSizeMb * 1024 * 1024;
	const chunk = fillerChunk(maxSizeMb);
	for (let i = 0; i < MAX_FILLER_WRITES; i++) {
		const r = await stub.apiPutItem(ctx, { hashKey: kb(`_filler_${i}`), sortKey: kb("sk"), data: chunk, kind: "bytes" });
		if (r.meta.databaseSize > target) return r.meta.databaseSize;
	}
	throw new Error(`growPastSplitThreshold: database never passed ${target} bytes`);
}

// ─── Range split ──────────────────────────────────────────────────────────────

// A 1 MB threshold keeps SQLite page overhead negligible vs. item data, so the post-split children
// (≈ total/N each) stay well under the limit and remain leaves (no surprise re-split or size reject).
export const RANGE_SPLIT_MAX_SIZE_MB = 1;
export const RANGE_ITEM_DATA = "x".repeat(50 * 1024); // ~50 KB/item → ~21 items cross the 1 MB threshold

// Builds a range-structure leaf owning [−∞, +∞) (parent = a hash DO), migration-complete so it serves
// locally, then writes distinct-sk items until a range split is queued. No retain-leftmost: on split it
// becomes a pure router over N fresh children.
export async function makeQueuedRangeRoot(
	rangeSplitN: number,
	overrides?: Partial<Parameters<typeof PartitionContextCreator.create>[0]>,
): Promise<{
	rootCtx: PartitionContextResolved;
	rootStub: DurableObjectStub<PartitionDO>;
	sks: string[];
}> {
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName: `rangesplit.${crypto.randomUUID()}`,
		rootTreesN: 1,
		hashSplitN: 2,
		rangeSplitN,
		hashSplitConditions: { maxSizeMb: 100 },
		rangeSplitConditions: { maxSizeMb: RANGE_SPLIT_MAX_SIZE_MB },
		...overrides,
	});
	const hashParentCtx = new PartitionTopologyRouterImpl(base).pickPartition(kb("alice")).partitionContext;
	const { partitionContext: rootCtx } = resolveRangePartitionContext(hashParentCtx, kb("alice"), null, null);
	const rootStub = PartitionDO.getByName(env.PARTITION_DO, rootCtx.doName);

	// Initialize as a ready leaf (migration complete → serves locally rather than 503).
	await rootStub.internalInitFromSplit(
		{
			parentPartitionContext: hashParentCtx,
			newPartitionContext: rootCtx,
			newPartitionRangeDepth: 0,
			splitType: "range",
			rangeAncestors: [],
		},
		true, // __testing__completeMigration
	);

	const sks: string[] = [];
	for (let i = 0; i < 100; i++) {
		// The random part at the end is to check the short boundaries computation if we want.
		const sk = `sk${String(i).padStart(3, "0")}-${crypto.randomUUID()}`;
		await rootStub.apiPutItem(rootCtx, { hashKey: kb("alice"), sortKey: kb(sk), data: RANGE_ITEM_DATA, kind: "text" as const });
		sks.push(sk);
		if ((await rootStub.status()).splitStatus?.status === "split_queued") break;
	}
	return { rootCtx, rootStub, sks };
}

// Waits for a partition's own split to finish, driving the whole subtree's migration each poll.
export async function waitForSplitCompleted(stub: DurableObjectStub<PartitionDO>): Promise<void> {
	await vi.waitFor(
		async () => {
			await drainSplitTree(stub);
			const s = await stub.status();
			if (s.splitStatus?.status !== "split_completed") throw new Error("split not completed yet");
		},
		{ timeout: 5000, interval: 100 },
	);
}

// Writes distinct-sk items (keyed by `keyPrefix` so they land in the target's own range) into a range
// partition until it queues a split, then drives that split to completion.
export async function splitRangePartition(
	stub: DurableObjectStub<PartitionDO>,
	ctx: PartitionContextResolved,
	keyPrefix: string,
): Promise<void> {
	for (let i = 0; ; i++) {
		await stub.apiPutItem(ctx, {
			hashKey: kb("alice"),
			sortKey: kb(`${keyPrefix}${String(i).padStart(4, "0")}`),
			data: RANGE_ITEM_DATA,
			kind: "text" as const,
		});
		if ((await stub.status()).splitStatus?.status === "split_queued") break;
		invariant(i < 200, `partition (${keyPrefix}) did not reach split_queued in time`);
	}
	await waitForSplitCompleted(stub);
}
