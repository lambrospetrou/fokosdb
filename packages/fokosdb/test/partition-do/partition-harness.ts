/** PartitionDO lifecycle helpers for splits, migration, and promotion. */
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { expect, vi } from "vitest";
import type { PartitionDO } from "../../src/server/do-partition.js";
import { testControlledPartitionStub, testPartitionStub } from "../stub-helpers.js";
import type { ControlledPartitionDO } from "../controlled-partition-do.js";
import { RANGE_PROMOTION_FRACTION, type GetItemRpcRequest, type PutItemRpcRequest } from "../../src/server/do-partition.js";
import invariant from "../../src/shared/invariant.js";
import { FokosError, UNAVAILABLE_CODES } from "../../src/shared/errors.js";
import type { PromotedKeyStatus } from "../../src/shared/partition/partition-store.js";
import { isHashPartition, isRangePartition } from "../../src/sharding/route-context.js";
import type { FokosDbRouteContext } from "../../src/shared/partition-context.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import {
	PartitionIdHelper,
	hashChildIndex,
	resolveHashChildPartitionContexts,
	resolveDescendantHashPartitionContext,
	resolveRangePartitionContext,
} from "../../src/sharding/partition-id.js";
import { FokosRouter } from "../../src/sharding/router.js";
import { FOKOS_KV_KEYS } from "../../src/sharding/sharding-store.js";
import type { SplitStatusView } from "../../src/server/do-partition.js";
import { MAX_ITEM_BYTES, validateItemKeys } from "../../src/shared/transaction-limits.js";
import {
	type OpenedPartitionRpc,
	type PartitionOptions,
	type SplitStartedOrCompleted,
	expectSplitStatus,
	kb,
	makeStub,
	openedRpc,
} from "./helpers.js";

type PartitionWriter = {
	apiPutItem(ctx: FokosDbRouteContext, req: PutItemRpcRequest): Promise<{ meta: { databaseSize: number } }>;
	status(ctx: FokosDbRouteContext): Promise<{ splitStatus?: SplitStatusView }>;
};

// Each hash filler is below the promotion threshold (a quarter of the cap), and the write that
// crosses the cap leaves the stored size below the 1.1 write-reject band, so a test can still land
// a write while the split waits for its alarm.
const FILLER_CHUNK_FRACTION = 0.35;
// ~0.09 of the split budget per filler write, so ~12 writes cross the threshold; 200 is headroom.
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

/**
 * The namespace of `ControlledPartitionDO`. A test that holds, caps, or fails a call creates its
 * partitions here. Each child of a split inherits the namespace from its parent.
 */
export const CONTROLLED_NS = "CONTROLLED_PARTITION_DO";

/** Creates a root hash partition over a table name no other suite uses. */
export function makePartition(opts?: PartitionOptions): TestPartition {
	const { ctx, stub } = makeStub(opts);
	return TestPartition.at(ctx, stub);
}

export class TestPartition {
	readonly ctx: FokosDbRouteContext;
	/** The raw stub: every call answers an envelope. `runInDurableObject` needs it. */
	readonly stub: DurableObjectStub<PartitionDO>;
	/** The same stub with every envelope opened, as a test reads a response. */
	readonly rpc: OpenedPartitionRpc;

	private constructor(ctx: FokosDbRouteContext, stub?: DurableObjectStub<PartitionDO>) {
		this.ctx = ctx;
		this.stub = stub ?? testPartitionStub(ctx.doName, ctx.policy.ns);
		this.rpc = openedRpc(this.stub);
	}

	/** Wraps a context that another partition (or a pure resolver) produced. */
	static at(ctx: FokosDbRouteContext, stub?: DurableObjectStub<PartitionDO>): TestPartition {
		return new TestPartition(ctx, stub);
	}

	get doName(): string {
		return this.ctx.doName;
	}

	/** The seams of this partition. The partition must be in the `CONTROLLED_NS` namespace. */
	get controlled(): DurableObjectStub<ControlledPartitionDO> {
		if (this.ctx.policy.ns !== CONTROLLED_NS) {
			throw new Error(`${this.doName}: a seam needs a partition in ${CONTROLLED_NS}; create the partition with { ns: CONTROLLED_NS }`);
		}
		return testControlledPartitionStub(this.doName);
	}

	put(req: PutItemRpcRequest) {
		return this.rpc.apiPutItem(this.ctx, req);
	}

	get(req: GetItemRpcRequest) {
		return this.rpc.apiGetItem(this.ctx, req);
	}

	status() {
		return this.rpc.status(this.ctx);
	}

	/** This partition's split status, narrowed to a started or completed split. */
	async splitStatus(): Promise<SplitStartedOrCompleted> {
		const state = await this.status();
		return expectSplitStatus(state.splitStatus, this.doName);
	}

	/** The id of the split repartition of this partition, which a read-through caller must name. */
	async splitRepartitionId(): Promise<string> {
		return await runInDurableObject(this.stub, (_instance: PartitionDO, state: DurableObjectState) => {
			const rows = state.storage.sql
				.exec<{ id: string }>(`SELECT id FROM fokos_repartitions WHERE kind IN ('hash_split', 'range_split') LIMIT 1`)
				.toArray();
			invariant(rows[0], `${this.doName}: no split repartition`);
			return rows[0].id;
		});
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
		const idx = hashChildIndex(kb(hashKey), PartitionIdHelper.depth(idBytes), this.ctx.topology.hashSplitN);
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

	/**
	 * Counts this partition's OWN item rows for `hashKey`, straight from its SQLite storage.
	 *
	 * Reads through the public API answer for whoever owns the key now — a split child, or a range
	 * tree after a promotion. A test that asks whether the rows are still on THIS partition (promotion
	 * GC, or hash-child migration excluding a promoted key) has to look at the storage instead.
	 */
	async localItemCount(hashKey: string): Promise<number> {
		return await runInDurableObject(this.stub, (_instance: PartitionDO, state: DurableObjectState) => {
			const rows = state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM items WHERE hk = ?`, kb(hashKey)).toArray();
			return rows[0].n;
		});
	}

	/** The range root of `hashKey`: the partition a promotion of that key creates. */
	rangeRoot(hashKey: string): TestPartition {
		return TestPartition.at(resolveRangePartitionContext(this.ctx, kb(hashKey), null, null));
	}

	/** Runs the partition's scheduled alarm once, through the runtime test API. A no-op if none is set. */
	async runAlarm(): Promise<void> {
		await runDurableObjectAlarm(this.stub);
	}

	/**
	 * Runs one scheduler pass on this partition. The pass is the same work that the alarm runs. The
	 * result tells if the pass changed the lifecycle or the job record, and when the next work is due.
	 */
	async runDueWork(): Promise<DrivePass> {
		return await runInDurableObject(this.stub, async (instance: PartitionDO, state: DurableObjectState) => {
			const snapshot = () =>
				JSON.stringify({ lifecycle: instance.fokos.lifecycle(), jobs: state.storage.kv.get(FOKOS_KV_KEYS.JOBS) ?? null });
			const before = snapshot();
			await instance.fokos.runDueWork();
			const after = snapshot();
			return { doName: this.doName, changed: after !== before, alarmAt: await state.storage.getAlarm(), snapshot: after };
		});
	}

	/** This partition and each node below it that the split created. */
	private async splitTree(): Promise<TestPartition[]> {
		const state = await this.status();
		if (!state.splitStatus || state.splitStatus.status === "split_queued") return [this];
		const nodes: TestPartition[] = [this];
		for (const child of await this.children()) nodes.push(...(await child.splitTree()));
		return nodes;
	}

	/** Writes distributed filler items until this partition starts a hash split. */
	async triggerHashSplit(writer: PartitionWriter = this.rpc): Promise<PutItemRpcRequest[]> {
		invariant(isHashPartition(this.ctx), `${this.doName}: not a hash partition`);
		invariant(!(await writer.status(this.ctx)).splitStatus, `${this.doName}: already splitting`);
		const data = fillerChunk(this.maxSizeMb("hash"));
		const capBytes = this.maxSizeMb("hash") * 1024 * 1024;
		const keys = this.fillerHashKeys();
		const items: PutItemRpcRequest[] = [];
		for (let i = 0; i < MAX_FILLER_WRITES; i++) {
			const item: PutItemRpcRequest = { hashKey: kb(keys.next().value!), sortKey: kb("sk"), data, kind: "text" };
			try {
				const res = await writer.apiPutItem(this.ctx, item);
				items.push(item);
				// The split queues inside the write that carries the stored size past the cap, so only
				// that write needs the status read below.
				if (res.meta.databaseSize <= capBytes) {
					continue;
				}
			} catch (e) {
				// A size rejection is usual when a split is already in the queue: a write before this one went
				// past the threshold. A different error is a failure. A rejection with no split in the queue
				// is also a failure, for example when a promotion in the queue prevents the split. The
				// writes after it get the same rejection. Thus the loop reports the cause now, and not
				// after all of its attempts.
				if (!FokosError.isCode(e, UNAVAILABLE_CODES.partition_over_size)) throw e;
				if (!(await writer.status(this.ctx)).splitStatus) throw e;
			}
			// Stop on the exact write that queues the split: the stored size at that moment decides
			// which further writes a test can still land under the overage band.
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
		const { hashSplitN } = this.ctx.topology;
		const router = new FokosRouter(this.ctx.topology, this.ctx.rangeConfig, this.ctx.policy);
		let emitted = 0;
		for (let i = 0; i < 1_000_000; i++) {
			const key = `${prefix}_${i}`;
			const root = router.rootContext(kb(key));
			const indices = Array.from({ length: depth }, (_, d) => hashChildIndex(kb(key), d, hashSplitN));
			const owner = resolveDescendantHashPartitionContext(root, Uint8Array.fromHex(root.partitionId), indices);
			if (owner.partitionId !== this.ctx.partitionId || hashChildIndex(kb(key), depth, hashSplitN) !== emitted % hashSplitN) {
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
		const { hashKey, startBoundary, endBoundary } = rangeOf(this.ctx);
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
			// Stop on the exact write that queues the split: the byte-quantile boundaries depend on
			// the stored rows, so extra writes would move them.
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
		await driveUntil(
			() => this.splitTree(),
			async () => (await this.status()).splitStatus?.status === "split_completed" && (await isSplitTreeComplete(this)),
			`${this.doName} split completion`,
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
	async awaitPromotedKeyStatus(hashKey: string, statuses: readonly PromotedKeyStatus[], opts?: { drive?: TestPartition[] }): Promise<void> {
		await drainUntil(
			opts?.drive ?? [this],
			async () => {
				const status = await this.promotedKeyStatus(hashKey);
				return status !== undefined && statuses.includes(status);
			},
			`"${hashKey}" to reach ${statuses.join(" or ")}`,
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
		await driveUntil(
			() => this.splitTree(),
			async () => await isSplitTreeComplete(this),
			`${this.doName} tree to settle`,
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
		const maxSizeMb = kind === "hash" ? this.ctx.policy.hashSplitConditions.maxSizeMb : this.ctx.policy.rangeSplitConditions.maxSizeMb;
		invariant(maxSizeMb, `${this.doName}: no ${kind} maxSizeMb configured`);
		return maxSizeMb;
	}
}

/** The immutable range a range partition owns, decoded from its partition ID. */
export function rangeOf(ctx: FokosDbRouteContext) {
	const decoded = PartitionIdHelper.decode(Uint8Array.fromHex(ctx.partitionId));
	invariant(decoded.schema === PartitionIdHelper.SCHEMA_RANGE_V1, `${ctx.doName}: not a range partition`);
	return decoded;
}

/** What one scheduler pass on one partition left behind. */
type DrivePass = { doName: string; changed: boolean; alarmAt: number | null; snapshot: string };

// A round is idle when no pass changed a node and no node has an alarm. The runtime keeps an alarm
// at the earliest deadline of its jobs, thus a node with no alarm has no work that the loop can wait
// for. The idle rounds give an in-memory timer, such as the TTL sweep, the time to operate.
const IDLE_ROUNDS_BEFORE_FAILURE = 20;
const IDLE_PAUSE_MS = 100;
// A retry or a cleanup can be due some seconds later. The loop waits for it in steps, and it
// examines the condition after each step.
const MAX_PENDING_PAUSE_MS = 1000;

/**
 * Drives the scheduler of each node until `check` passes. The loop fails when the nodes make no
 * progress, and not when a period of time ends: a slow machine needs more time for a split, but it
 * does not need more passes. The test timeout stops a run that hangs.
 *
 * The runtime also drives the same nodes with its alarm and its fast path. The scheduler runs one
 * pass at a time, thus the two drivers cannot interleave.
 */
async function driveUntil(nodes: () => Promise<TestPartition[]>, check: () => Promise<boolean>, label: string): Promise<void> {
	let last: DrivePass[] = [];
	for (let idle = 0; idle < IDLE_ROUNDS_BEFORE_FAILURE; ) {
		if (await check()) return;
		last = [];
		for (const node of await nodes()) {
			last.push(await node.runDueWork());
		}
		const now = Date.now();
		const alarms = last.flatMap((pass) => (pass.alarmAt === null ? [] : [pass.alarmAt]));
		if (last.some((pass) => pass.changed) || alarms.some((at) => at <= now)) {
			idle = 0;
		} else if (alarms.length > 0) {
			idle = 0;
			await scheduler.wait(Math.min(Math.min(...alarms) - now, MAX_PENDING_PAUSE_MS));
		} else {
			idle++;
			await scheduler.wait(IDLE_PAUSE_MS);
		}
	}
	if (await check()) return;
	const report = last.map((pass) => `${pass.doName}: ${pass.snapshot}`).join("; ");
	throw new Error(`${label}: no progress in ${IDLE_ROUNDS_BEFORE_FAILURE} rounds; ${report}`);
}

/** Drives each partition in `drive` until `check` passes. */
export async function drainUntil(drive: TestPartition[], check: () => Promise<boolean>, label: string): Promise<void> {
	await driveUntil(async () => drive, check, label);
}

/** True when each node of the split tree completed its split and its migration. */
async function isSplitTreeComplete(node: TestPartition): Promise<boolean> {
	const state = await node.status();
	if (state.parentPartitionContext && state.migrationStatus !== "migration_completed") return false;
	if (!state.splitStatus) return true;
	if (state.splitStatus.status !== "split_completed") return false;
	for (const child of await node.children()) if (!(await isSplitTreeComplete(child))) return false;
	return true;
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

/**
 * Gives the reason that a child did not ask for its migration data.
 *
 * A child starts its import when its alarm occurs. There are two possible causes: the child did not
 * change its state, or the alarm did not occur. The deadline of `withMigrationHeld` must show which
 * cause applies.
 *
 * This probe can stop before it gets an answer. A child that does its import now waits in the held
 * RPC, and it does not reply. The probe reports this condition also.
 */
async function migrationProbe(child: TestPartition): Promise<string> {
	const probe = async () => {
		const migration = (await child.status()).migrationStatus ?? "none";
		const inner = await runInDurableObject(child.stub, async (instance: PartitionDO, state: DurableObjectState) => {
			const alarmAt = await state.storage.getAlarm();
			return { import: instance.fokos.lifecycle().import, alarmInMs: alarmAt === null ? null : alarmAt - Date.now() };
		});
		return `${child.doName}: migration=${migration} import=${JSON.stringify(inner.import)} alarmInMs=${inner.alarmInMs}`;
	};
	const blocked = new Promise<string>((resolve) => setTimeout(() => resolve(`${child.doName}: probe blocked, the child is busy`), 2000));
	return await Promise.race([probe(), blocked]);
}

/** Holds every child transaction-metadata request, then releases and completes the split. */
export async function withMigrationHeld<T>(
	parent: TestPartition,
	run: (waitForAllRequests: () => Promise<void>) => Promise<T>,
): Promise<T> {
	const source = parent.controlled;
	// Held in the pending-transaction stream, which is the last stream an import runs. Every target
	// has pulled its items by then, so the whole tree migrates when the wait returns.
	await source.testHoldPulls({ stream: "pending_tx" });
	try {
		return await run(async () => {
			// The probe reads the children. A read can also start a child that waits for its alarm. This
			// changes the condition that the probe must measure. Thus the probe operates only near the end
			// of the deadline. At that time its result is a failure report, and not a poll.
			const probeAfter = Date.now() + 25_000;
			await vi.waitFor(
				async () => {
					const { heldTargets } = await source.testPullStats();
					const missing = (await parent.children()).filter((child) => !heldTargets.includes(child.doName));
					if (missing.length === 0) return;
					const names = missing.map((child) => child.doName).join(", ");
					if (Date.now() < probeAfter) {
						throw new Error(`migration RPC not received from ${names}`);
					}
					throw new Error(`migration RPC not received from ${(await Promise.all(missing.map(migrationProbe))).join("; ")}`);
				},
				{ timeout: 30_000, interval: 10 },
			);
		});
	} finally {
		await source.testReleasePulls();
		if ((await parent.status()).splitStatus) await parent.awaitSplitCompleted();
	}
}

/**
 * Caps every migration page the source serves at `maxRows` rows. Each phase and each stream of the
 * host then needs more than one round trip of the cursor.
 *
 * A truncated page points its cursor at the last row it returned, and the resume continues after that
 * row. No row is lost and none is duplicated, which is the path the real byte budget takes when it
 * stops a scan. `run` receives counters, so a test can assert that the pagination happened.
 */
export async function withMigrationBatchCap<T>(
	parent: TestPartition,
	maxRows: number,
	run: (stats: { calls: () => Promise<number>; truncated: () => Promise<number> }) => Promise<T>,
): Promise<T> {
	invariant(maxRows >= 1, "withMigrationBatchCap: maxRows must be >= 1");
	const source = parent.controlled;
	await source.testCapPulls(maxRows);
	try {
		return await run({
			calls: async () => (await source.testPullStats()).calls,
			truncated: async () => (await source.testPullStats()).truncated,
		});
	} finally {
		await source.testReleasePulls();
		if ((await parent.status()).splitStatus) await parent.awaitSplitCompleted();
	}
}

/** Creates an empty range root so range tests do not also test promotion detection. */
/**
 * A promoted range root. `hashPartition` is the hash partition that promoted the key, and it is the
 * partition a client reaches first: a test that must route into the range tree from outside sends
 * its request there rather than to the root.
 */
export async function makeRangeRoot(
	rangeSplitN: number,
	overrides?: PartitionOptions,
): Promise<{ root: TestPartition; sks: string[]; hashPartition: TestPartition }> {
	const hashPartition = makePartition({
		tableName: `rangesplit.${crypto.randomUUID()}`,
		rangeSplitN,
		rangeSplitConditions: { maxSizeMb: RANGE_SPLIT_MAX_SIZE_MB },
		...overrides,
	});
	await hashPartition.rpc.debugForcePromoteKey(hashPartition.ctx, { hashKey: kb("alice") });
	return { root: await hashPartition.awaitPromoted("alice"), sks: [], hashPartition };
}

export async function makeTriggeredRangeRoot(
	rangeSplitN: number,
	overrides?: PartitionOptions,
): Promise<{ root: TestPartition; sks: string[]; hashPartition: TestPartition }> {
	const { root, sks, hashPartition } = await makeRangeRoot(rangeSplitN, overrides);
	const start = sks.length;
	sks.push(...(await root.triggerRangeSplit((i) => `sk${String(i + start).padStart(3, "0")}-${crypto.randomUUID()}`)));
	return { root, sks, hashPartition };
}
