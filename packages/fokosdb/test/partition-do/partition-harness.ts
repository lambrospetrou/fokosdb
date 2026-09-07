/** PartitionDO lifecycle helpers for splits, migration, and promotion. */
import { env } from "cloudflare:workers";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { expect, vi } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import type { GetItemRpcRequest, PutItemRpcRequest } from "../../src/server/do-partition.js";
import invariant from "../../src/shared/invariant.js";
import { isPartitionExceededDatabaseSizeError } from "../../src/shared/partition-errors.js";
import type { PromotedKeyStatus } from "../../src/shared/partition/partition-store.js";
import { isHashPartition, isRangePartition } from "../../src/shared/partition-topology/partition-context.js";
import type { PartitionContextResolved } from "../../src/shared/partition-topology/partition-context.js";
import { KeyCodec } from "../../src/shared/partition-topology/key-codec.js";
import {
	PartitionIdHelper,
	hashChildIndex,
	resolveHashChildPartitionContexts,
	resolveDescendantHashPartitionContext,
	resolveRangePartitionContext,
} from "../../src/shared/partition-topology/partition-id.js";
import { PartitionTopologyRouterImpl } from "../../src/shared/partition-topology/router.js";
import { RANGE_PROMOTION_FRACTION } from "../../src/shared/partition-topology/split-policy.js";
import type { SplitStatusKVItem } from "../../src/shared/partition-topology/split-state.js";
import { MAX_ITEM_BYTES, validateItemKeys } from "../../src/shared/transaction-limits.js";
import { type PartitionOptions, type SplitStartedOrCompleted, expectSplitStatus, kb, makeStub } from "./helpers.js";

type PartitionWriter = {
	apiPutItem(ctx: PartitionContextResolved, req: PutItemRpcRequest): Promise<{ meta: { databaseSize: number } }>;
	status(ctx?: PartitionContextResolved): Promise<{ splitStatus?: SplitStatusKVItem }>;
};

// Each hash filler is below the promotion threshold and the write-reject grace band.
const FILLER_CHUNK_FRACTION = 0.3;
// ~0.075 of the split budget per filler write, so ~14 writes cross the threshold; 200 is headroom.
const MAX_FILLER_WRITES = 200;
const MAX_RANGE_FILLER_WRITES = 200;

function fillerChunk(maxSizeMb: number): string {
	const chunkBytes = Math.min(
		MAX_ITEM_BYTES - 4096,
		Math.floor(RANGE_PROMOTION_FRACTION * maxSizeMb * 1024 * 1024 * FILLER_CHUNK_FRACTION),
	);
	invariant(chunkBytes > 0, "split threshold is too small for filler items");
	expect(chunkBytes, "filler chunk exceeds MAX_ITEM_BYTES; lower maxSizeMb or FILLER_CHUNK_FRACTION").toBeLessThanOrEqual(MAX_ITEM_BYTES);
	return "x".repeat(chunkBytes);
}

export const PROMOTION_TEST_MAX_SIZE_MB = 1;
export const PROMOTION_BIG_DATA = "x".repeat(300 * 1024);
const RANGE_SPLIT_MAX_SIZE_MB = 1;
const RANGE_ITEM_DATA = "x".repeat(50 * 1024);

/** Creates a root hash partition over a table name no other suite uses. */
export function makePartition(opts?: PartitionOptions): TestPartition {
	const { ctx, stub } = makeStub(opts);
	return TestPartition.at(ctx, stub);
}

export class TestPartition {
	readonly ctx: PartitionContextResolved;
	readonly stub: DurableObjectStub<PartitionDO>;

	private constructor(ctx: PartitionContextResolved, stub?: DurableObjectStub<PartitionDO>) {
		this.ctx = ctx;
		this.stub = stub ?? PartitionDO.getByName(env.PARTITION_DO, ctx.doName);
	}

	/** Wraps a context that another partition (or a pure resolver) produced. */
	static at(ctx: PartitionContextResolved, stub?: DurableObjectStub<PartitionDO>): TestPartition {
		return new TestPartition(ctx, stub);
	}

	get doName(): string {
		return this.ctx.doName;
	}

	put(req: PutItemRpcRequest) {
		return this.stub.apiPutItem(this.ctx, req);
	}

	get(req: GetItemRpcRequest) {
		return this.stub.apiGetItem(this.ctx, req);
	}

	status() {
		return this.stub.status(this.ctx);
	}

	/** This partition's split status, narrowed to a started or completed split. */
	async splitStatus(): Promise<SplitStartedOrCompleted> {
		const state = await this.status();
		return expectSplitStatus(state.splitStatus, this.doName);
	}

	/** The promotion status this partition holds for `hashKey`, or undefined if it holds no entry. */
	async promotedKeyStatus(hashKey: string): Promise<PromotedKeyStatus | undefined> {
		const { promotedKeys } = await this.status();
		return promotedKeys.find((e) => KeyCodec.compare(e.hashKey, kb(hashKey)) === 0)?.status;
	}

	/** Returns deterministic hash children, including before the split starts. */
	hashChildren(): TestPartition[] {
		invariant(isHashPartition(this.ctx), `${this.doName}: not a hash partition`);
		return resolveHashChildPartitionContexts(this.ctx).map((ctx) => TestPartition.at(ctx));
	}

	/** The children this partition reports after splitting, hash or range. */
	async children(): Promise<TestPartition[]> {
		return (await this.splitStatus()).childPartitionContexts.map((ctx) => TestPartition.at(ctx));
	}

	/** The hash child that owns `hashKey`, one level down. */
	async childOwning(hashKey: string): Promise<TestPartition> {
		const children = await this.children();
		const idBytes = Uint8Array.fromHex(this.ctx.partitionId);
		const idx = hashChildIndex(kb(hashKey), PartitionIdHelper.depth(idBytes), this.ctx.hashSplitN);
		const expected = this.hashChildren()[idx];
		const owner = children.find((c) => c.doName === expected?.doName);
		invariant(owner, `${this.doName}: no child owns "${hashKey}"`);
		return owner;
	}

	/** Walks down the hash tree to the leaf that owns `hashKey` — this partition when it has not split. */
	async leafOwning(hashKey: string): Promise<TestPartition> {
		let node: TestPartition = this;
		while ((await node.status()).splitStatus !== undefined) {
			node = await node.childOwning(hashKey);
		}
		return node;
	}

	/** The range root of `hashKey`: the partition a promotion of that key creates. */
	rangeRoot(hashKey: string): TestPartition {
		return TestPartition.at(resolveRangePartitionContext(this.ctx, kb(hashKey), null, null).partitionContext);
	}

	/** Runs the partition's scheduled alarm once, through the runtime test API. A no-op if none is set. */
	async runAlarm(): Promise<void> {
		await runDurableObjectAlarm(this.stub);
	}

	/** Runs one alarm pass through the current split tree. */
	private async runTreeAlarms(): Promise<void> {
		await this.runAlarm();
		const state = await this.status();
		if (!state.splitStatus || state.splitStatus.status === "split_queued") return;

		for (const child of await this.children()) await child.runTreeAlarms();
	}

	/** Writes distributed filler items until this partition starts a hash split. */
	async triggerHashSplit(writer: PartitionWriter = this.stub): Promise<PutItemRpcRequest[]> {
		invariant(isHashPartition(this.ctx), `${this.doName}: not a hash partition`);
		invariant(!(await writer.status(this.ctx)).splitStatus, `${this.doName}: already splitting`);
		const data = fillerChunk(this.maxSizeMb("hash"));
		const keys = this.fillerHashKeys();
		const items: PutItemRpcRequest[] = [];
		for (let i = 0; i < MAX_FILLER_WRITES; i++) {
			const item: PutItemRpcRequest = { hashKey: kb(keys.next().value!), sortKey: kb("sk"), data, kind: "text" };
			try {
				await writer.apiPutItem(this.ctx, item);
				items.push(item);
			} catch (e) {
				// A size rejection is normal once a split is already queued — a prior write crossed the
				// threshold. Any other error, or a rejection with no split queued (e.g. mutual exclusion
				// with a queued promotion), is a real failure.
				if (!isPartitionExceededDatabaseSizeError(e)) throw e;
				if (!(await writer.status(this.ctx)).splitStatus) throw e;
			}
			const state = await writer.status(this.ctx);
			if (state.splitStatus) {
				invariant(state.splitStatus.splitType === "hash", `${this.doName}: expected a hash split`);
				return items;
			}
		}
		throw new Error(`${this.doName}: no hash split after ${MAX_FILLER_WRITES} writes; ${JSON.stringify(await writer.status(this.ctx))}`);
	}

	/**
	 * Yields hash keys that this partition owns, spread evenly across its future children.
	 *
	 * Both properties are required for a realistic split. Ownership: a leaf DO accepts any key for
	 * its own context — routing is the caller's contract, not enforced on write — so filler keys
	 * must be owned or the test stores items that production routing could never deliver here.
	 * Reads routed through the tree would then look in a different leaf and miss them. Each
	 * candidate is resolved through the full path (root → this node) by replaying the child index
	 * at every depth, and kept only when it lands here. Spread: the `emitted % hashSplitN` check
	 * admits keys in round-robin order over this partition's children, so the split leaves every
	 * child with a roughly equal share and none of them immediately crosses the threshold again.
	 */
	private *fillerHashKeys(): Generator<string> {
		const prefix = `_split_${crypto.randomUUID()}`;
		const depth = PartitionIdHelper.depth(Uint8Array.fromHex(this.ctx.partitionId));
		const router = new PartitionTopologyRouterImpl(this.ctx);
		let emitted = 0;
		for (let i = 0; i < 1_000_000; i++) {
			const key = `${prefix}_${i}`;
			const root = router.pickPartition(kb(key)).partitionContext;
			const indices = Array.from({ length: depth }, (_, d) => hashChildIndex(kb(key), d, this.ctx.hashSplitN));
			const owner = resolveDescendantHashPartitionContext(root, root, Uint8Array.fromHex(root.partitionId), indices).partitionContext;
			if (
				owner.partitionId !== this.ctx.partitionId ||
				hashChildIndex(kb(key), depth, this.ctx.hashSplitN) !== emitted % this.ctx.hashSplitN
			) {
				continue;
			}
			emitted++;
			yield key;
		}
		throw new Error(`${this.doName}: could not generate enough owned hash keys`);
	}

	/** Queues a hash split, then drains the tree until this partition reports it complete. */
	async splitHash(): Promise<TestPartition[]> {
		await this.triggerHashSplit();
		await this.awaitSplitCompleted();
		return await this.children();
	}

	/** Writes items until this range partition starts a split. */
	async triggerRangeSplit(sortKey: (i: number) => string): Promise<string[]> {
		invariant(isRangePartition(this.ctx), `${this.doName}: not a range partition`);
		invariant(!(await this.status()).splitStatus, `${this.doName}: already splitting`);
		const { hashKey, startBoundary, endBoundary } = this.ctx.rangePartition;
		const data = "x".repeat(Math.min(RANGE_ITEM_DATA.length, Math.floor((this.maxSizeMb("range") * 1024 * 1024) / 20)));
		const sks: string[] = [];
		for (let i = 0; i < MAX_RANGE_FILLER_WRITES; i++) {
			const sk = sortKey(i);
			validateItemKeys(KeyCodec.decode(hashKey), sk);
			invariant(!sks.includes(sk), `${this.doName}: duplicate filler sort key`);
			invariant(
				(startBoundary === null || KeyCodec.compare(kb(sk), startBoundary) >= 0) &&
					(endBoundary === null || KeyCodec.compare(kb(sk), endBoundary) < 0),
				`${this.doName}: filler sort key is outside the range`,
			);
			await this.put({ hashKey, sortKey: kb(sk), data, kind: "text" });
			sks.push(sk);
			if ((await this.status()).splitStatus) return sks;
		}
		throw new Error(`${this.doName}: no range split after ${MAX_RANGE_FILLER_WRITES} writes; ${JSON.stringify(await this.status())}`);
	}

	/** Splits a range partition with sort keys inside its current range. */
	async splitRange(keyPrefix: string): Promise<TestPartition[]> {
		await this.triggerRangeSplit((i) => `${keyPrefix}${String(i).padStart(4, "0")}`);
		await this.awaitSplitCompleted();
		return await this.children();
	}

	/** Drives this partition's own split to completion, driving the whole subtree on each attempt. */
	async awaitSplitCompleted(): Promise<void> {
		await vi.waitFor(
			async () => {
				await this.runTreeAlarms();
				const state = await this.status();
				if (state.splitStatus?.status !== "split_completed")
					throw new Error(`${this.doName}: split not completed; ${JSON.stringify(state)}`);
				await assertSplitTreeComplete(this);
			},
			{ timeout: 5000, interval: 100 },
		);
	}

	/** Grows the database past its configured hash-split size. */
	async growPastSplitThreshold(): Promise<number> {
		const maxSizeMb = this.maxSizeMb("hash");
		const target = maxSizeMb * 1024 * 1024;
		const chunk = fillerChunk(maxSizeMb);
		const keys = this.fillerHashKeys();
		for (let i = 0; i < MAX_FILLER_WRITES; i++) {
			const r = await this.put({ hashKey: kb(keys.next().value!), sortKey: kb("sk"), data: chunk, kind: "text" });
			if (r.meta.databaseSize > target) return r.meta.databaseSize;
		}
		throw new Error(`${this.doName}: database never passed ${target} bytes`);
	}

	/** Waits for a promotion status and drives the partitions that own the next step. */
	async awaitPromotedKeyStatus(
		hashKey: string,
		statuses: readonly PromotedKeyStatus[],
		opts?: { drive?: TestPartition[]; timeoutMs?: number },
	): Promise<void> {
		await drainUntil(
			opts?.drive ?? [this],
			async () => {
				const status = await this.promotedKeyStatus(hashKey);
				return status !== undefined && statuses.includes(status);
			},
			`"${hashKey}" to reach ${statuses.join(" or ")}`,
			opts?.timeoutMs,
		);
	}

	/** Waits for promotion and returns the serving range root. */
	async awaitPromoted(hashKey: string): Promise<TestPartition> {
		const root = this.rangeRoot(hashKey);
		await this.awaitPromotedKeyStatus(hashKey, ["promoting", "promoted"]);
		await this.awaitPromotedKeyStatus(hashKey, ["promoted"], { drive: [root] });
		await root.awaitMigrationCompleted();
		return root;
	}

	async triggerPromotion(hashKey: string, sortKeyAt?: (i: number) => string): Promise<PutItemRpcRequest[]> {
		invariant(isHashPartition(this.ctx), `${this.doName}: not a hash partition`);
		invariant(!(await this.status()).splitStatus, `${this.doName}: already splitting`);
		invariant(!(await this.promotedKeyStatus(hashKey)), `${this.doName}: key already queued for promotion`);
		const prefix = `_promotion_${crypto.randomUUID()}`;
		const data = "x".repeat(Math.min(RANGE_ITEM_DATA.length, fillerChunk(this.maxSizeMb("hash")).length));
		const items: PutItemRpcRequest[] = [];
		for (let i = 0; i < MAX_FILLER_WRITES; i++) {
			const sortKey = sortKeyAt?.(i) ?? `${prefix}_${i}`;
			validateItemKeys(hashKey, sortKey);
			invariant(!items.some((item) => KeyCodec.compare(item.sortKey!, kb(sortKey)) === 0), `${this.doName}: duplicate promotion sort key`);
			const item: PutItemRpcRequest = { hashKey: kb(hashKey), sortKey: kb(sortKey), data, kind: "text" };
			await this.put(item);
			items.push(item);
			if (await this.promotedKeyStatus(hashKey)) return items;
		}
		throw new Error(`${this.doName}: no promotion after ${MAX_FILLER_WRITES} writes; ${JSON.stringify(await this.status())}`);
	}

	async awaitMigrationCompleted(): Promise<void> {
		await drainUntil([this], async () => (await this.status()).migrationStatus === "migration_completed", `${this.doName} migration`);
	}

	/** Waits until no split or migration is in flight anywhere in the tree rooted here. */
	async awaitTreeSettled(): Promise<void> {
		await vi.waitFor(
			async () => {
				await this.runTreeAlarms();
				await assertSplitTreeComplete(this);
			},
			{ timeout: 5000, interval: 100 },
		);
	}

	async awaitSplitStarted(): Promise<void> {
		await drainUntil(
			[this],
			async () => {
				const status = (await this.status()).splitStatus?.status;
				return status === "split_started" || status === "split_completed";
			},
			`${this.doName} split start`,
		);
	}

	private maxSizeMb(kind: "hash" | "range"): number {
		const maxSizeMb = kind === "hash" ? this.ctx.hashSplitConditions?.maxSizeMb : this.ctx.rangeSplitConditions?.maxSizeMb;
		invariant(maxSizeMb, `${this.doName}: no ${kind} maxSizeMb configured`);
		return maxSizeMb;
	}
}

/**
 * Polls durable state until `check` passes. When progress stalls, it runs the scheduled alarm of
 * each partition in `drive` — the runtime normally fires them on its own, so the alarm pass is a
 * fallback nudge, not the primary driver.
 */
export async function drainUntil(drive: TestPartition[], check: () => Promise<boolean>, label: string, timeoutMs = 5000): Promise<void> {
	let nextDrive = Date.now() + 1000;
	await vi.waitFor(
		async () => {
			if (await check()) return;
			if (Date.now() >= nextDrive) {
				for (const p of drive) await p.runAlarm();
				nextDrive = Date.now() + 1000;
			}
			if (!(await check()))
				throw new Error(`timed out waiting for ${label}: ${JSON.stringify(await Promise.all(drive.map((p) => p.status())))}`);
		},
		{ timeout: timeoutMs, interval: 100 },
	);
}

/** Asserts that a split tree is complete and returns its number of split nodes. */
export async function assertSplitTreeComplete(node: TestPartition): Promise<number> {
	const state = await node.status();
	if (state.parentPartitionContext) expect(state.migrationStatus, `${node.doName}: migration incomplete`).toBe("migration_completed");
	if (!state.splitStatus) return 0;
	expect(state.splitStatus.status, `DO ${node.doName} should be split_completed`).toBe("split_completed");
	let count = 1;
	for (const child of await node.children()) {
		count += await assertSplitTreeComplete(child);
	}
	return count;
}

/** Holds every child transaction-metadata request, then releases and completes the split. */
export async function withMigrationHeld<T>(
	parent: TestPartition,
	run: (waitForAllRequests: () => Promise<void>) => Promise<T>,
): Promise<T> {
	let release!: () => void;
	const requestedBy = new Set<string>();
	const held = new Promise<void>((resolve) => {
		release = resolve;
	});
	const restore = await runInDurableObject(parent.stub, (instance: PartitionDO) => {
		// The spy MUST go on the prototype: the RPC dispatcher rejects methods installed as own
		// properties on the DO instance ("receiver does not implement the method"). The guard keeps
		// the shared-prototype mock scoped to this one instance — including stray background RPCs
		// from other partitions. Never call this from `it.concurrent`: two installs would compose
		// spies on the same prototype and each would capture the other's mock as its "original".
		const prototype: PartitionDO = Object.getPrototypeOf(instance);
		const original = prototype.migrationGetPartitionTransactionMetadata;
		const spy = vi.spyOn(prototype, "migrationGetPartitionTransactionMetadata").mockImplementation(async function (
			this: PartitionDO,
			request,
		) {
			if (this === instance) {
				requestedBy.add(request.childPartitionContext.doName);
				await held;
			}
			return original.call(this, request);
		});
		return () => spy.mockRestore();
	});
	try {
		return await run(async () => {
			await vi.waitFor(
				async () => {
					const missing = (await parent.children()).filter((child) => !requestedBy.has(child.doName));
					if (missing.length > 0) throw new Error(`migration RPC not received from ${missing.map((child) => child.doName).join(", ")}`);
				},
				{ timeout: 5000, interval: 10 },
			);
		});
	} finally {
		release();
		restore();
		if ((await parent.status()).splitStatus) await parent.awaitSplitCompleted();
	}
}

/**
 * Caps every migration batch response the parent serves at `maxRows` rows, forcing each stream
 * (items, pending transactions, promoted keys) through multiple cursor-paginated round trips.
 * A truncated response points its cursor at the last row returned; resume continues strictly after
 * it, so no row is lost or duplicated — the same path the real byte budget takes when it stops a
 * scan. `run` receives counters so the test can assert pagination actually happened.
 */
export async function withMigrationBatchCap<T>(
	parent: TestPartition,
	maxRows: number,
	run: (stats: { calls: () => number; truncated: () => number }) => Promise<T>,
): Promise<T> {
	invariant(maxRows >= 1, "withMigrationBatchCap: maxRows must be >= 1");
	let calls = 0;
	let truncated = 0;
	const restore = await runInDurableObject(parent.stub, (instance: PartitionDO) => {
		// The spies MUST go on the prototype: the RPC dispatcher rejects methods installed as own
		// properties on the DO instance ("receiver does not implement the method"). The guard keeps
		// the shared-prototype mocks scoped to this one instance — including stray background RPCs
		// from other partitions. Never call this from `it.concurrent`: two installs would compose
		// spies on the same prototype and each would capture the other's mock as its "original".
		const prototype: PartitionDO = Object.getPrototypeOf(instance);

		const origItems = prototype.migrationGetItemsBatch;
		const itemsSpy = vi.spyOn(prototype, "migrationGetItemsBatch").mockImplementation(async function (this: PartitionDO, opts) {
			const result = await origItems.call(this, opts);
			if (this !== instance) return result;
			calls++;
			if (result.items.length <= maxRows) return result;
			truncated++;
			const last = result.items[maxRows - 1];
			return { items: result.items.slice(0, maxRows), nextCursor: { hk: last.hk, sk: last.sk } };
		});

		const origTx = prototype.migrationGetPartitionTransactionMetadata;
		const txSpy = vi.spyOn(prototype, "migrationGetPartitionTransactionMetadata").mockImplementation(async function (
			this: PartitionDO,
			opts,
		) {
			const result = await origTx.call(this, opts);
			if (this !== instance) return result;
			calls++;
			if (result.pendingTransactions.length <= maxRows) return result;
			truncated++;
			const last = result.pendingTransactions[maxRows - 1];
			return {
				...result,
				pendingTransactions: result.pendingTransactions.slice(0, maxRows),
				nextCursor: { hk: last.hk, sk: last.sk, transaction_id: last.transaction_id },
			};
		});

		const origPk = prototype.migrationGetPromotedKeysBatch;
		const pkSpy = vi.spyOn(prototype, "migrationGetPromotedKeysBatch").mockImplementation(async function (this: PartitionDO, opts) {
			const result = await origPk.call(this, opts);
			if (this !== instance) return result;
			calls++;
			if (result.rows.length <= maxRows) return result;
			truncated++;
			return { rows: result.rows.slice(0, maxRows), nextCursor: { hashKey: result.rows[maxRows - 1].hash_key } };
		});

		return () => {
			itemsSpy.mockRestore();
			txSpy.mockRestore();
			pkSpy.mockRestore();
		};
	});
	try {
		return await run({ calls: () => calls, truncated: () => truncated });
	} finally {
		await restore();
		if ((await parent.status()).splitStatus) await parent.awaitSplitCompleted();
	}
}

/** Creates an empty range root so range tests do not also test promotion detection. */
export async function makeRangeRoot(rangeSplitN: number, overrides?: PartitionOptions): Promise<{ root: TestPartition; sks: string[] }> {
	const hashPartition = makePartition({
		tableName: `rangesplit.${crypto.randomUUID()}`,
		rangeSplitN,
		rangeSplitConditions: { maxSizeMb: RANGE_SPLIT_MAX_SIZE_MB },
		...overrides,
	});
	await hashPartition.stub.debugForcePromoteKey(hashPartition.ctx, kb("alice"));
	return { root: await hashPartition.awaitPromoted("alice"), sks: [] };
}

export async function makeTriggeredRangeRoot(
	rangeSplitN: number,
	overrides?: PartitionOptions,
): Promise<{ root: TestPartition; sks: string[] }> {
	const { root, sks } = await makeRangeRoot(rangeSplitN, overrides);
	const start = sks.length;
	sks.push(...(await root.triggerRangeSplit((i) => `sk${String(i + start).padStart(3, "0")}-${crypto.randomUUID()}`)));
	return { root, sks };
}
