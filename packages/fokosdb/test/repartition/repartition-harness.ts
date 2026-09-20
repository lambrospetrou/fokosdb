/**
 * The test harness for the repartition flow: several partitions, each with its own real
 * `PartitionStore` and its own source and target halves, driven step by step from one test.
 *
 * Each node is a real Durable Object, so every store call runs against real SQLite and real KV. A
 * node is entered through `runInDurableObject`, which is also what makes a peer call work: the peer
 * adapter enters the destination node and invokes the method on a half built over ITS storage, so a
 * control call lands on the real receiving flow with no Durable Object RPC and no serialization in
 * between. The Workers runtime forbids touching one DO's storage from another's context, so a
 * callback only ever touches the node it is inside.
 *
 * Both halves are rebuilt on every entry. That costs nothing — the only in-memory state either holds
 * is the source's split cache — and it means every test runs against a cold cache, which is what the
 * RFC requires of it: eviction must not change behaviour.
 */
import { runInDurableObject } from "cloudflare:test";
import type { PartitionDO } from "../../src/server/do-partition.js";
import { testPartitionStub } from "../stub-helpers.js";
import { KeyCodec, type KeyBytes } from "../../src/sharding/key-codec.js";
import {
	PartitionContextCreator,
	type PartitionContext,
	type PartitionContextLivePartition,
	type PartitionContextResolved,
} from "../../src/sharding/partition-context.js";
import { PartitionIdHelper, resolveRangePartitionContext } from "../../src/sharding/partition-id.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { FokosMigrationHost } from "../../src/shared/partition/fokos-migration-host.js";
import {
	RepartitionSource,
	RepartitionTarget,
	REPARTITION_KV_KEYS,
	type RepartitionSourceDeps,
	type RepartitionTargetDeps,
} from "../../src/sharding/repartition-flow.js";
import type {
	FokosInitRequest,
	FokosMigrationPage,
	FokosPartitionRef,
	FokosRepartitionPeer,
} from "../../src/sharding/repartition-types.js";

export const kb = (s: string) => KeyCodec.encode(s);

/**
 * The clock every test drives from. Steps take `now` explicitly, so a test asserts exact deadlines
 * rather than tolerances. It sits far in the future on purpose: anything a test writes without
 * naming a time carries the real clock, and a row whose deadline is in 2026 must still be due at T0.
 */
export const T0 = 4_000_000_000_000;

const CTX_KEY = "__partition_context";
const DEPTH_KEY = "__partition_depth";

/**
 * What a test sees once it is inside a node. One partition is both halves: a hash child is a target
 * first and a source later, and the two share the store but never call each other.
 */
export type NodeEnv = {
	source: RepartitionSource;
	target: RepartitionTarget;
	store: PartitionStore;
	storage: DurableObjectStorage;
	ctx: PartitionContextLivePartition;
};

export type Node = {
	ctx: PartitionContextResolved;
	ref: FokosPartitionRef;
	doName: string;
	/** Runs `fn` inside this node, against its own real storage. */
	enter<T>(fn: (e: NodeEnv) => T | Promise<T>): Promise<T>;
	/** Calls this node's peer surface, exactly as a remote source or target would. */
	peer: FokosRepartitionPeer;
};

export type Cluster = {
	base: PartitionContext;
	/** The node for a context, created on first use so a target exists before it is initialized. */
	node(ctx: PartitionContextResolved): Node;
	hashNode(idxs: number[]): Node;
	rangeNode(from: PartitionContextResolved, hashKey: KeyBytes, start: KeyBytes | null, end: KeyBytes | null): Node;
	/** Per-node counts of the side effects the flow asks its DO for. */
	scheduled(doName: string): number;
	alarms(doName: string): number[];
	/** Makes the next `fokosInit` this node serves fail, once. */
	failNextInit(doName: string, message?: string): void;
	/** Makes the next `fokosMigrationPull` this node serves answer with `page`, once. */
	nextPullPage(doName: string, page: FokosMigrationPage): void;
};

export type ClusterOptions = {
	hashSplitN?: number;
	rangeSplitN?: number;
	/** Fixes the table name, so two clusters can be built over one topology. */
	tableName?: string;
};

export function makeCluster(opts: ClusterOptions = {}): Cluster {
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName: opts.tableName ?? `repartition-${crypto.randomUUID()}`,
		rootTreesN: 1,
		hashSplitN: opts.hashSplitN ?? 2,
		rangeSplitN: opts.rangeSplitN ?? 2,
		hashSplitConditions: { maxSizeMb: 100 },
		rangeSplitConditions: { maxSizeMb: 500 },
	});

	const nodes = new Map<string, Node>();
	const scheduled = new Map<string, number>();
	const alarms = new Map<string, number[]>();
	const initFailures = new Map<string, string>();
	const cannedPages = new Map<string, FokosMigrationPage>();
	const suffix = crypto.randomUUID();

	const cluster: Cluster = {
		base,
		node(ctx) {
			const existing = nodes.get(ctx.doName);
			if (existing) return existing;
			const node = makeNode(ctx);
			nodes.set(ctx.doName, node);
			return node;
		},
		hashNode(idxs) {
			const { opaque, doName } = PartitionIdHelper.fromHashIdxs(base, idxs).encode(true);
			return cluster.node({ ...base, doName: doName!, primaryDoIdStr: "", partitionId: opaque });
		},
		rangeNode(from, hashKey, start, end) {
			return cluster.node(resolveRangePartitionContext(from, hashKey, start, end).partitionContext);
		},
		scheduled: (doName) => scheduled.get(doName) ?? 0,
		alarms: (doName) => alarms.get(doName) ?? [],
		failNextInit: (doName, message = "simulated init failure") => initFailures.set(doName, message),
		nextPullPage: (doName, page) => cannedPages.set(doName, page),
	};

	function makeNode(ctx: PartitionContextResolved): Node {
		// The DO name carries a per-cluster suffix so two clusters in one test file never share storage.
		const stubName = `rf.${suffix}.${ctx.doName}`;

		// The stub is resolved on every entry, never cached. A Durable Object stub is an I/O object
		// bound to the context that made it, and a peer call is made from INSIDE another node's context
		// — a cached stub would fail there with "cannot perform I/O on behalf of a different Durable
		// Object", which is exactly the hop this harness exists to exercise.
		const enter = async <T>(fn: (e: NodeEnv) => T | Promise<T>): Promise<T> =>
			await runInDurableObject(testPartitionStub(stubName), async (_i: PartitionDO, state: DurableObjectState) => {
				const storage = state.storage;
				const store = new PartitionStore(storage);
				store.runMigrations();
				// The stored context is what the flow reads back, so a target has none until fokosInit.
				const stored = storage.kv.get<PartitionContextLivePartition>(CTX_KEY);
				const live: PartitionContextLivePartition = stored ?? { ...ctx };
				live._partitionIdBytes = Uint8Array.fromHex(live.partitionId);

				const deps = makeDeps(ctx.doName, storage, store, live);
				const source = new RepartitionSource(store, storage, deps);
				const target = new RepartitionTarget(store, storage, deps);
				return await fn({ source, target, store, storage, ctx: live });
			});

		return {
			ctx,
			ref: { partitionId: ctx.partitionId, doName: ctx.doName },
			doName: ctx.doName,
			enter,
			// The peer surface of a real flow over a real store — the same four methods a remote
			// participant would reach over Workers RPC, with the transport taken out.
			peer: {
				async fokosInit(req) {
					const failure = initFailures.get(ctx.doName);
					if (failure !== undefined) {
						initFailures.delete(ctx.doName);
						throw new Error(failure);
					}
					await enter((e) => e.target.initAsTarget(req));
				},
				async fokosStartImport(req) {
					await enter((e) => e.target.startImport(req));
				},
				async fokosMigrationPull(req) {
					const canned = cannedPages.get(ctx.doName);
					if (canned !== undefined) {
						cannedPages.delete(ctx.doName);
						return canned;
					}
					return await enter((e) => e.source.servePage(req));
				},
				async fokosMigrationAck(req) {
					await enter((e) => e.source.acceptAck(req));
				},
			},
		};
	}

	function makeDeps(
		doName: string,
		storage: DurableObjectStorage,
		store: PartitionStore,
		live: PartitionContextLivePartition,
	): RepartitionSourceDeps & RepartitionTargetDeps {
		const depthOf = (): number => {
			if (live.rangePartition) return storage.kv.get<number>(DEPTH_KEY) ?? 0;
			return PartitionIdHelper.depth(Uint8Array.fromHex(live.partitionId));
		};
		return {
			// A target exists as soon as its source names it, exactly as a real DO does: a Durable Object
			// is created by the first call that reaches it, not registered in advance.
			getPeer: (ref) => cluster.node({ ...base, doName: ref.doName, primaryDoIdStr: "", partitionId: ref.partitionId }).peer,
			host: new FokosMigrationHost({ store, hashSplitN: () => live.hashSplitN }),
			identity: () => ({
				pCtx: live,
				depth: depthOf(),
				rangeAncestors: live.rangePartition ? store.getRangeAncestors(live.rangePartition.hashKey, depthOf()) : [],
			}),
			hasIdentity: () => storage.kv.get<PartitionContextLivePartition>(CTX_KEY) !== undefined,
			applyTargetIdentity: (req: FokosInitRequest) => {
				const next: PartitionContextLivePartition = { ...req.target };
				delete next._partitionIdBytes;
				storage.kv.put<PartitionContextLivePartition>(CTX_KEY, next);
				if (req.rangeDepth !== undefined) storage.kv.put<number>(DEPTH_KEY, req.rangeDepth);
				if (req.rangeAncestors?.length && req.target.rangePartition) {
					store.setRangeAncestors(req.target.rangePartition.hashKey, req.rangeAncestors);
				}
			},
			computeRangeBoundaries: (hashKey, start, end, n) => store.computeRangeSplitBoundaries(hashKey, start, end, n),
			lockCountForKey: (hashKey) => store.pendingLockCountForHashKey(hashKey),
			cleanupStep: (hashKey) => {
				store.deleteItemsBatchForHashKey(hashKey, CLEANUP_BATCH);
				store.deletePendingTxForHashKey(hashKey);
				if (store.hasItemsForHashKey(hashKey)) return false;
				store.deleteKeySizeEstimate(hashKey);
				return true;
			},
			onSplitCompleted: () => store.deleteAllPendingTx(),
			scheduleWork: () => scheduled.set(doName, (scheduled.get(doName) ?? 0) + 1),
			ensureAlarmSet: async (targetMs) => {
				alarms.set(doName, [...(alarms.get(doName) ?? []), targetMs]);
			},
			logParams: () => ({ test: "repartition-flow", doName }),
		};
	}

	return cluster;
}

/** Small enough that a promotion with a handful of rows needs more than one cleanup step. */
export const CLEANUP_BATCH = 2;

/** Writes one committed item straight into a node's store, as a user write would leave it. */
export function putItem(store: PartitionStore, hk: string, sk: string, data = `d-${hk}-${sk}`): void {
	store.upsertItem({ hk: kb(hk), sk: kb(sk), data, kind: "text", ttlAt: null, txOrderTs: 1 });
}

/** Writes one pending lock, which is what blocks a promotion and what a split target must inherit. */
export function putLock(store: PartitionStore, hk: string, sk: string, transactionId = "tx-1"): void {
	store.insertPendingLock({
		hk: kb(hk),
		sk: kb(sk),
		transaction_id: transactionId,
		transaction_ts: 1,
		operation: "put",
		data: "pending",
		kind: "text",
		conditions_json: null,
		ttl_epoch_utc_seconds: null,
		coordinator_do_id: "tc-1",
		created_at: 1,
		guarded_at: null,
	});
}

/** The running size estimate of one hash key, which the import maintains page by page. */
export function keySizeEstimate(storage: DurableObjectStorage, hk: string): number | undefined {
	return storage.sql.exec<{ est_bytes: number }>(`SELECT est_bytes FROM key_size_estimates WHERE hk = ?`, kb(hk)).toArray()[0]?.est_bytes;
}

/** The bytes SQLite actually stored for one hash key, which the estimate must match exactly. */
export function storedBytes(storage: DurableObjectStorage, hk: string): number {
	return storage.sql.exec<{ n: number }>(`SELECT COALESCE(SUM(est_row_bytes), 0) AS n FROM items WHERE hk = ?`, kb(hk)).one().n;
}

export { REPARTITION_KV_KEYS };
