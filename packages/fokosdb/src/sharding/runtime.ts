/**
 * `FokosShardingRuntime`: the sharding layer of one Durable Object, as an owned object.
 *
 * A host class creates one runtime in its constructor, gives it hooks, registers its operations, and
 * delegates a fixed set of `fokos`-prefixed RPC methods to it. Every public RPC method of the host is
 * one `dispatch` call. The runtime validates the identity, applies the lifecycle gate, resolves the
 * owner of each key, runs the host's local handler or forwards to another partition, learns its route
 * caches, and returns an envelope around the host's result. It owns every topology transition, every
 * forwarding decision, and the Durable Object alarm. The host keeps its storage, its RPC surface, and
 * its data semantics.
 */
import { FokosError, FokosInternalError, FokosRoutingError, FokosUnavailableError, UNAVAILABLE_CODES } from "../shared/errors.js";
import invariant from "../shared/invariant.js";
import { DESTROY_ABORT_SENTINEL } from "../shared/cf-utils.js";
import { AddResult } from "./bloom-filter.js";
import { attachRouting, envelope, RouteCollector, routedError } from "./envelope.js";
import { SHARDING_INTERNAL_CODES, SHARDING_ROUTING_CODES, SHARDING_UNAVAILABLE_CODES } from "./errors.js";
import { hashChildIndex, hashRootIndex } from "./hash-primitives.js";
import { HashTopology } from "./hash-topology.js";
import { KeyCodec, type KeyBytes } from "./key-codec.js";
import { PartialRangeTopology } from "./partial-range-topology.js";
import {
	identityDepth,
	partitionIdentityFrom,
	PartitionIdHelper,
	resolveDescendantHashPartitionContext,
	resolveRangePartitionContext,
} from "./partition-id.js";
import { isStrictSubSlice, planRangeFrontier, startCmp, type FrontierBase, type PlannedVisit } from "./range-frontier.js";
import { RepartitionSource, RepartitionTarget, type RepartitionCommonDeps } from "./repartition-flow.js";
import { sliceIncludesHashKey, sliceIncludesItem } from "./repartition-slice.js";
import type {
	FokosExecuteLocalRequest,
	FokosInitRequest,
	FokosMigrationAckRequest,
	FokosMigrationPage,
	FokosMigrationPullRequest,
	FokosPrepareDestroyRequest,
	FokosRequestPromotionRequest,
	FokosShardingRpc,
	FokosStartImportRequest,
	FokosStatusPage,
	FokosStatusRequest,
	RouteKey,
} from "./repartition-types.js";
import {
	isRangePartition,
	refOf,
	structurallyEqual,
	topologiesEqual,
	validateRangeConfig,
	validateTopology,
	type FokosPartitionIdentity,
	type FokosPartitionRef,
	type FokosRouteContext,
	type FokosStoredPolicy,
} from "./route-context.js";
import type {
	FokosChild,
	FokosEnvelope,
	FokosGroupPart,
	FokosJob,
	FokosLifecycle,
	FokosLocalCall,
	FokosOperation,
	FokosOperationSpec,
	FokosOperations,
	FokosOwner,
	FokosRangeInput,
	FokosRangeVisit,
	FokosRequestPromotionResult,
	FokosRouteNode,
	FokosRuntimeOptions,
	FokosServedRole,
	FokosShardingHooks,
	FokosSignals,
} from "./runtime-types.js";
import { FokosScheduler } from "./scheduler.js";
import { FokosShardingStore, type LearnedRangeSlice, type RepartitionState } from "./sharding-store.js";
import { cursorFallsInChild, rangeIntersects, type SkInterval } from "./sk-interval.js";
import type { RangeAncestorInfo } from "./types.js";

const DEFAULT_FALLBACK_ALARM_MS = 5_000;
const DEFAULT_FAST_PATH_DELAY_MS = 50;
const DEFAULT_IMPORT_PAGES_PER_PASS = 16;
/** Both bounds of one `fokosStatus` page: the entry count, and the estimated serialized size. */
const STATUS_PAGE_ENTRIES = 1_000;
const STATUS_PAGE_BYTES = 20 * 1024 * 1024;
/** A speculative or cached forward that misses resolves again; this bounds the retries of one call. */
const MAX_FORWARD_RETRIES = 8;

const NO_SORT_KEY = KeyCodec.encodeOptional(undefined);
const NOOP_CALL: FokosLocalCall = { signal: () => {} };

/** The names of the built-in jobs. They run first, in this order. */
const BUILTIN_JOBS = ["target_import", "target_ack", "source_repartition", "source_cleanup"] as const;

/**
 * Where a point key lives, with the facts the retry paths need: which step of the resolution chose
 * the target, the arena jump it took, and the learned range slice it used. `resolveOwner` reports the
 * public part only.
 */
type Resolution =
	| { kind: "local" }
	| {
			kind: "remote";
			target: FokosPartitionRef;
			speculative: boolean;
			via: "override" | "bloom" | "hash" | "range";
			/** The levels a hash jump skipped, when `via` is "hash". 1 is the immediate child. */
			relDepth?: number;
			/** The learned slice a range target came from, or null for the durable child or root. */
			learned: LearnedRangeSlice | null;
	  }
	| { kind: "out_of_range" };

type ResolveOptions = {
	/** Consult the promotion Bloom cache. Point and range shapes do; group and single-owner shapes do not. */
	bloom: boolean;
	/** Jump to the deepest learned range slice. Off for a fan-out, which enters a range tree at its root. */
	learnedRange: boolean;
};

const EXACT: ResolveOptions = { bloom: false, learnedRange: false };

type AnyOperation = FokosOperation<unknown, unknown>;

export type FokosRuntimeConstructorOptions<TPolicy, Ops extends FokosOperationSpec> = FokosRuntimeOptions<TPolicy> & {
	ctx: DurableObjectState;
	hooks: FokosShardingHooks<TPolicy>;
	operations: FokosOperations<Ops>;
};

export class FokosShardingRuntime<TPolicy, Ops extends FokosOperationSpec> implements FokosShardingRpc {
	readonly #ctx: DurableObjectState;
	readonly #stub: FokosRuntimeOptions<TPolicy>["stub"];
	readonly #hooks: FokosShardingHooks<TPolicy>;
	readonly #ops: Record<string, AnyOperation>;
	readonly #store: FokosShardingStore;
	readonly #source: RepartitionSource;
	readonly #target: RepartitionTarget;
	readonly #scheduler: FokosScheduler;
	readonly #fallbackAlarmMs: number;
	readonly #hashArenaBytes: number | undefined;
	readonly #bloomOptions: { expectedKeys: number; falsePositiveRate: number };

	/** The immutable identity, from `__fokos/identity`. Absent until the first request or `fokosInit`. */
	#identity?: FokosPartitionIdentity;
	/** The mutable part of the last route context this partition received, from `__fokos/policy`. */
	#stored?: FokosStoredPolicy<TPolicy>;
	#routeCtx?: FokosRouteContext<TPolicy>;
	/** The bounded ancestor boundaries of a range partition with its own appended last. Empty otherwise. */
	#rangeAncestorsWithSelf: RangeAncestorInfo[] = [];
	#hashArena: HashTopology | null = null;
	#bloom: PartialRangeTopology | null = null;
	/** The plan of every visit `rangeVisits` returned, by the visit object, so `forwardRangeVisit` can fall back. */
	readonly #plans = new WeakMap<FokosRangeVisit, PlannedVisit>();

	constructor(opts: FokosRuntimeConstructorOptions<TPolicy, Ops>) {
		this.#ctx = opts.ctx;
		this.#stub = opts.stub;
		this.#hooks = opts.hooks;
		this.#ops = opts.operations as Record<string, AnyOperation>;
		this.#validateOperations();
		this.#fallbackAlarmMs = opts.scheduler?.fallbackAlarmMs ?? DEFAULT_FALLBACK_ALARM_MS;
		this.#hashArenaBytes = opts.caches?.hashArenaBytes;
		this.#bloomOptions = opts.caches?.promotionBloom ?? { expectedKeys: 300_000, falsePositiveRate: 0.01 };
		this.#store = new FokosShardingStore(opts.ctx.storage, { rangeHierarchyMaxRows: opts.caches?.rangeHierarchyMaxRows });

		const deps: RepartitionCommonDeps = {
			getPeer: (ref) => this.#peer(ref),
			hooks: this.#hooks as FokosShardingHooks<unknown>,
			identity: () => ({ ctx: this.routeContext(), identity: this.identity() }),
			scheduleWork: () => this.#scheduler.wake(),
			logParams: () => this.#logParams(),
		};
		this.#source = new RepartitionSource(this.#store, deps);
		this.#target = new RepartitionTarget(this.#store, {
			...deps,
			hasIdentity: () => this.#identity !== undefined,
			applyTargetIdentity: (req) => this.#applyTargetIdentity(req),
			ensureAlarmSet: async (targetMs) => await this.#scheduler.ensureAlarmAtMost(targetMs),
		});
		this.#scheduler = new FokosScheduler({
			storage: opts.ctx.storage,
			store: this.#store,
			fallbackAlarmMs: this.#fallbackAlarmMs,
			fastPathDelayMs: opts.scheduler?.fastPathDelayMs ?? DEFAULT_FAST_PATH_DELAY_MS,
			isFenced: () => this.#store.isDestroying(),
			jobs: () => [...this.#builtinJobs(), ...(this.#hooks.jobs ?? [])],
			logParams: () => this.#logParams(),
		});

		void opts.ctx.blockConcurrencyWhile(async () => {
			// The sharding store migrates first, before the host runs its own migrations.
			this.#store.runMigrations();
			const identity = this.#store.getIdentity();
			const stored = this.#store.getPolicy<TPolicy>();
			if (identity && stored) this.#setIdentity(identity, stored);
			const bloom = this.#store.getPromotionBloom();
			if (bloom) this.#bloom = PartialRangeTopology.fromSnapshot(bloom);
		});
	}

	// ═══ dispatch ═══════════════════════════════════════════════════════════

	async dispatch<K extends keyof Ops & string>(
		op: K,
		routeCtx: FokosRouteContext<TPolicy>,
		req: Ops[K]["req"],
	): Promise<FokosEnvelope<Ops[K]["res"]>> {
		const descriptor = this.#ops[op];
		invariant(descriptor, () => `fokos/runtime: operation ${op} is not registered`);
		let collector: RouteCollector | undefined;
		return await this.#guard(
			op,
			async () => {
				this.#ensureIdentity(routeCtx);
				collector = new RouteCollector();
				return (await this.#dispatch(op, descriptor, req, collector)) as FokosEnvelope<Ops[K]["res"]>;
			},
			() => collector,
		);
	}

	async #dispatch(op: string, descriptor: AnyOperation, req: unknown, collector: RouteCollector): Promise<FokosEnvelope<unknown>> {
		if (descriptor.shape === "local") {
			if (descriptor.whileMigrating === "retry" && this.#target.isImporting()) {
				// The same gate the other shapes take, under the name of this operation: the request nudges
				// the import on before it is refused.
				this.#scheduler.wake();
				await this.#scheduler.ensureAlarmAtMost(Date.now() + this.#fallbackAlarmMs);
				throw new FokosUnavailableError(UNAVAILABLE_CODES.partition_migrating, {
					message: "partition split in progress, please retry later",
					attributes: { operation: op },
				});
			}
			const { call, signals } = this.#localCall();
			const value = await descriptor.local(req, call);
			await this.#applySignals(signals);
			return envelope(value, collector.build());
		}

		if (this.#target.isImporting()) return await this.#whileImporting(op, descriptor, req, collector);

		switch (descriptor.shape) {
			case "point":
				return await this.#dispatchPoint(op, descriptor, req, collector);
			case "group":
				return await this.#dispatchGroup(op, descriptor, req, collector);
			case "single_owner":
				return await this.#dispatchSingleOwner(op, descriptor, req, collector);
			case "range":
				return await this.#dispatchRange(op, descriptor, req, collector);
		}
	}

	/**
	 * The lifecycle gate. An incomplete target holds only some of the rows of its slice, so a write
	 * cannot apply and a read cannot be answered locally. The request also asks for one more import
	 * step and restores the fallback alarm, so the partition makes progress even when no start
	 * notification arrived.
	 */
	async #whileImporting(op: string, descriptor: AnyOperation, req: unknown, collector: RouteCollector): Promise<FokosEnvelope<unknown>> {
		this.#scheduler.wake();
		await this.#scheduler.ensureAlarmAtMost(Date.now() + this.#fallbackAlarmMs);
		// `dispatch` gates a `local` operation with `whileMigrating: "retry"` before this point and runs
		// every other `local` operation without the gate, so this gate never sees one.
		invariant(descriptor.shape !== "local", "fokos/runtime: a local operation cannot reach the import gate");
		if (descriptor.whileMigrating === "retry") {
			throw new FokosUnavailableError(UNAVAILABLE_CODES.partition_migrating, {
				message: "partition split in progress, please retry later",
				attributes: { operation: op },
			});
		}

		// Ownership only, never a forward: a key this partition cannot own is a routing defect, and a
		// key it owns is answered by the source for as long as the import runs.
		if (descriptor.shape === "range") {
			this.#assertCanOwnRange(op, descriptor.range(req));
		} else {
			for (const key of this.#scopeRouteKeys(descriptor, req)) {
				if (!this.#ownsByTopology(key)) throw misrouted(op, "key outside this partition");
			}
		}
		const record = this.#target.importRecord();
		invariant(record, "fokos/runtime: importing without an import record");
		// This partition owns the scope and its source executes for it: both facts reach the caller, and
		// the source's list travels unchanged. A hash target writes its own depth on the range nodes the
		// source reached through a promotion: the target owns the promoted key, so its depth is the one a
		// router above must learn, not the shallower depth of the source that read on its behalf.
		collector.add(this.#selfNode("read_through"));
		const result = (await this.#peer(record.source).fokosExecuteLocal({
			op,
			repartitionId: record.repartitionId,
			caller: this.identity().ref,
			request: req,
		})) as FokosEnvelope<unknown>;
		this.#learn(result.routing.servedBy, this.#scopeKeys(descriptor, req));
		collector.mergeForwarded(result.routing, this.#rangeDepthStamp());
		return envelope(result.value, collector.build());
	}

	async #dispatchPoint(
		op: string,
		descriptor: Extract<AnyOperation, { shape: "point" }>,
		req: unknown,
		collector: RouteCollector,
	): Promise<FokosEnvelope<unknown>> {
		const key = descriptor.key(req);
		const resolution = this.#resolve(key, { bloom: true, learnedRange: true });
		if (resolution.kind === "out_of_range") throw misrouted(op, "key outside this partition");
		if (resolution.kind === "local") {
			this.#admit(op, descriptor, [key]);
			const before = this.#beforeForward(descriptor, req);
			const { value, signals } = await this.#runLocalScope(descriptor, req, collector);
			await this.#applySignals([...before, ...signals]);
			return envelope(value, collector.build());
		}
		const before = this.#beforeForward(descriptor, req);
		const forwarded = this.#forwardPoint(op, descriptor, req, key, resolution, collector, 0);
		forwarded.catch(() => {});
		await this.#applySignals(before);
		return envelope(await forwarded, collector.build());
	}

	/**
	 * Runs the local handler and lists this partition as its executor. The node is added before the
	 * handler runs: a handler that throws still served the scope, and the error carries the node so a
	 * forwarding partition learns from the error as it learns from a result. The signals come back to
	 * the caller, because the caller decides when they apply: after the handler settled, and only when
	 * it did not throw.
	 */
	async #runLocalScope(
		descriptor: Exclude<AnyOperation, { shape: "local" }>,
		req: unknown,
		collector: RouteCollector,
	): Promise<{ value: unknown; signals: FokosSignals[] }> {
		const { call, signals } = this.#localCall();
		collector.add(this.#selfNode("executed"));
		const value = await this.#runLocal(descriptor, req, call);
		return { value, signals };
	}

	/**
	 * Forwards one point request and applies the two fallbacks of a cache miss: a speculative range
	 * forward that finds no range partition, or no cutover on a read, resolves again with the Bloom
	 * step off; a cached hash jump that finds no partition forgets the hint and resolves again from
	 * the nearest known ancestor. A learned range slice that no longer exists is forgotten the same way.
	 */
	async #forwardPoint(
		op: string,
		descriptor: Extract<AnyOperation, { shape: "point" }>,
		req: unknown,
		key: RouteKey,
		resolution: Extract<Resolution, { kind: "remote" }>,
		collector: RouteCollector,
		retries: number,
	): Promise<unknown> {
		try {
			return await this.#forwardTo(collector, resolution.target, op, req, [key.hashKey]);
		} catch (e) {
			const next = this.#fallbackAfterMiss(key, resolution, e, descriptor.readOnly === true);
			if (!next || retries >= MAX_FORWARD_RETRIES) throw e;
			if (next.kind === "out_of_range") throw misrouted(op, "key outside this partition");
			if (next.kind === "local") {
				this.#admit(op, descriptor, [key]);
				const { value, signals } = await this.#runLocalScope(descriptor, req, collector);
				await this.#applySignals(signals);
				return value;
			}
			return await this.#forwardPoint(op, descriptor, req, key, next, collector, retries + 1);
		}
	}

	/**
	 * The resolution to try after a forward failed, or null when the failure is not a cache miss. It
	 * forgets the hint that caused the miss, so the next resolution cannot repeat it.
	 */
	#fallbackAfterMiss(key: RouteKey, resolution: Extract<Resolution, { kind: "remote" }>, e: unknown, readOnly: boolean): Resolution | null {
		const notInitialized = FokosError.isCode(e, SHARDING_ROUTING_CODES.range_partition_not_initialized);
		const notCutOver = FokosError.isCode(e, SHARDING_UNAVAILABLE_CODES.repartition_not_cut_over);
		if (resolution.learned && notInitialized) {
			this.#store.deleteLearnedRangeSlice(key.hashKey, resolution.learned.startBoundary, resolution.learned.endBoundary);
			return this.#resolve(key, { bloom: resolution.via === "bloom", learnedRange: true });
		}
		if (resolution.via === "bloom" && (notInitialized || (readOnly && notCutOver))) {
			return this.#resolve(key, { bloom: false, learnedRange: true });
		}
		if (
			resolution.via === "hash" &&
			(resolution.relDepth ?? 1) > 1 &&
			FokosError.isCode(e, SHARDING_ROUTING_CODES.hash_partition_not_initialized)
		) {
			const arena = this.#arena();
			if (arena?.invalidate(key.hashKey, resolution.relDepth!)) this.#store.putHashArena(arena.toSnapshot());
			return this.#resolve(key, { bloom: true, learnedRange: true });
		}
		return null;
	}

	async #dispatchGroup(
		op: string,
		descriptor: Extract<AnyOperation, { shape: "group" }>,
		req: unknown,
		collector: RouteCollector,
	): Promise<FokosEnvelope<unknown>> {
		const items = descriptor.items(req);
		if (items.length === 0) {
			this.#admit(op, descriptor, []);
			const before = this.#beforeForward(descriptor, req);
			const { call, signals } = this.#localCall();
			const value = await this.#runLocal(descriptor, req, call);
			await this.#applySignals([...before, ...signals]);
			return envelope(value, collector.build());
		}

		const local: Array<{ key: RouteKey; item: unknown }> = [];
		const remote = new Map<string, { target: FokosPartitionRef; items: unknown[]; keys: KeyBytes[] }>();
		for (const entry of items) {
			const resolution = this.#resolve(entry.key, EXACT);
			if (resolution.kind === "out_of_range") throw misrouted(op, "item outside this partition");
			if (resolution.kind === "local") {
				local.push(entry);
				continue;
			}
			const group = remote.get(resolution.target.partitionId);
			if (group) {
				group.items.push(entry.item);
				group.keys.push(entry.key.hashKey);
			} else remote.set(resolution.target.partitionId, { target: resolution.target, items: [entry.item], keys: [entry.key.hashKey] });
		}

		// Admission, `beforeForward`, and the local work run in the same synchronous block as the
		// resolution above, before the first await, so a cutover cannot interleave between the
		// ownership decision and the write. The remote calls start right after the local one.
		if (local.length > 0) {
			this.#admit(
				op,
				descriptor,
				local.map((entry) => entry.key),
			);
		}
		const signals: FokosSignals[] = this.#beforeForward(descriptor, req);
		let localCall: { request: unknown; value: unknown; signals: FokosSignals[] } | undefined;
		if (local.length > 0) {
			const { call, signals: own } = this.#localCall();
			const request = descriptor.subRequest(
				req,
				local.map((entry) => entry.item),
			);
			collector.add(this.#selfNode("executed"));
			localCall = { request, value: this.#runLocal(descriptor, request, call), signals: own };
		}
		const remoteCalls = [...remote.values()].map((group) => {
			const request = descriptor.subRequest(req, group.items);
			const promise = this.#forwardTo(collector, group.target, op, request, group.keys);
			// A rejection that lands while the signals below run must not count as unhandled; it is
			// awaited right after.
			promise.catch(() => {});
			return { target: group.target, request, promise };
		});

		const parts: Array<FokosGroupPart<unknown, unknown>> = [];
		if (localCall) {
			localCall.value = await localCall.value;
			signals.push(...localCall.signals);
		}
		await this.#applySignals(signals);

		// The merge over remote parts makes this partition a router of the request. A partition that also
		// ran the handler keeps its `executed` role.
		if (remoteCalls.length > 0) collector.add(this.#selfNode("merged"));
		if (descriptor.failurePolicy === "fail_fast") {
			const results = await Promise.all(remoteCalls.map((c) => c.promise));
			results.forEach((result, i) => parts.push({ target: remoteCalls[i].target, request: remoteCalls[i].request, result }));
		} else {
			const settled = await Promise.allSettled(remoteCalls.map((c) => c.promise));
			const failures = settled.filter((r): r is PromiseRejectedResult => r.status === "rejected");
			if (failures.length > 0) {
				throw new FokosInternalError(SHARDING_INTERNAL_CODES.partition_fanout_failed, {
					message: "a remote group of the operation failed",
					cause: failures[0].reason,
					attributes: { operation: op, failureCount: failures.length, groupCount: remoteCalls.length },
				});
			}
			settled.forEach((result, i) => {
				if (result.status === "fulfilled")
					parts.push({ target: remoteCalls[i].target, request: remoteCalls[i].request, result: result.value });
			});
		}
		if (localCall) parts.push({ target: "local", request: localCall.request, result: localCall.value });
		return envelope(descriptor.merge(parts), collector.build());
	}

	async #dispatchSingleOwner(
		op: string,
		descriptor: Extract<AnyOperation, { shape: "single_owner" }>,
		req: unknown,
		collector: RouteCollector,
	): Promise<FokosEnvelope<unknown>> {
		const keys = descriptor.items(req).map((item) => item.key);
		let localCount = 0;
		let remote: FokosPartitionRef | null = null;
		let spansPartitions = false;
		for (const key of keys) {
			const resolution = this.#resolve(key, EXACT);
			if (resolution.kind === "out_of_range") throw misrouted(op, "item outside this partition");
			if (resolution.kind === "local") localCount++;
			else if (remote === null) remote = resolution.target;
			else if (remote.partitionId !== resolution.target.partitionId) spansPartitions = true;
		}
		if (remote === null) {
			this.#admit(op, descriptor, keys);
			const before = this.#beforeForward(descriptor, req);
			const { value, signals } = await this.#runLocalScope(descriptor, req, collector);
			await this.#applySignals([...before, ...signals]);
			return envelope(value, collector.build());
		}
		const before = this.#beforeForward(descriptor, req);
		if (spansPartitions || localCount > 0) {
			// A value and not an error: on a split shard group this is the ordinary answer for such a
			// set, and it carries no side effects at any depth of a forwarding chain.
			await this.#applySignals(before);
			return envelope(descriptor.notApplicable, collector.build());
		}
		const forwarded = this.#forwardTo(
			collector,
			remote,
			op,
			req,
			keys.map((key) => key.hashKey),
		);
		forwarded.catch(() => {});
		await this.#applySignals(before);
		return envelope(await forwarded, collector.build());
	}

	async #dispatchRange(
		op: string,
		descriptor: Extract<AnyOperation, { shape: "range" }>,
		req: unknown,
		collector: RouteCollector,
	): Promise<FokosEnvelope<unknown>> {
		const input = descriptor.range(req);
		const planned = this.#planRange(op, input, true);
		const visits = planned.map((p) => p.visit);
		if (visits.some((visit) => visit.target === "local")) this.#admit(op, descriptor, [{ hashKey: input.hashKey, sortKey: NO_SORT_KEY }]);
		const signals = this.#beforeForward(descriptor, req);
		const value = await descriptor.walk({
			request: req,
			visits,
			local: (r) => {
				const { call, signals: own } = this.#localCall();
				collector.add(this.#selfNode("executed"));
				const out = this.#runLocal(descriptor, r, call);
				signals.push(...own);
				return out;
			},
			forward: async (visit, r) => {
				// A walk that forwards makes this partition a router of the request. A leaf that also ran
				// the handler keeps its `executed` role, because `executed` outranks `merged`.
				collector.add(this.#selfNode("merged"));
				return await this.#forwardRangeVisit(op, descriptor, visit, r, collector);
			},
		});
		await this.#applySignals(signals);
		return envelope(value, collector.build());
	}

	// ═══ the primitive API ══════════════════════════════════════════════════

	identity(): FokosPartitionIdentity {
		if (!this.#identity) throw this.#notInitialized();
		return this.#identity;
	}

	/** The stored host policy: the live value that the last request updated. */
	policy(): TPolicy {
		if (!this.#stored) throw this.#notInitialized();
		return this.#stored.policy;
	}

	/** The stored route context of this partition: its identity plus the mutable part the last request left. */
	routeContext(): FokosRouteContext<TPolicy> {
		if (!this.#routeCtx) throw this.#notInitialized();
		return this.#routeCtx;
	}

	/**
	 * True once `fokosInit` or a first routed request gave this partition its identity, which the
	 * constructor then loads into memory and nothing clears. It reads no storage and allocates nothing,
	 * so a caller that must not touch storage asks it first: a partition with no identity holds no
	 * rows, owns no keys, and can have neither an import nor a repartition.
	 */
	initialized(): boolean {
		return this.#identity !== undefined;
	}

	/**
	 * The destroy fence on its own. `lifecycle()` reads the import record and the repartition rows to
	 * answer, which is too much for a host that only asks on every request whether it is fenced.
	 */
	isFenced(): boolean {
		return this.#store.isDestroying();
	}

	/** The mutable lifecycle facts, read from storage. Callers that can run cold check `initialized()` first. */
	lifecycle(): FokosLifecycle {
		const record = this.#target.importRecord();
		const active = this.#store.firstActiveRepartition();
		return {
			role: this.#source.routerRole() ? "router" : "owner",
			import: record ? { state: record.state, source: record.source, slice: record.slice } : null,
			activeRepartition:
				active && (active.state === "queued" || active.state === "planned" || active.state === "cutover")
					? { id: active.id, kind: active.kind, state: active.state }
					: null,
			destroying: this.#store.isDestroying(),
		};
	}

	/**
	 * True when this partition owns the key now. It reads the topology and the route overrides only,
	 * never a cache, so a Bloom false positive cannot make a host sweep skip a key it owns.
	 */
	owns(key: RouteKey): boolean {
		return this.#resolve(key, EXACT).kind === "local";
	}

	/** The point-routing answer, caches included. A speculative remote owner is a hint, not a fact. */
	resolveOwner(key: RouteKey): FokosOwner {
		const resolution = this.#resolve(key, { bloom: true, learnedRange: true });
		return resolution.kind === "remote" ? { kind: "remote", target: resolution.target, speculative: resolution.speculative } : resolution;
	}

	/** This router's direct targets in `target_index` order. Empty on an owner. */
	children(): FokosChild[] {
		if (!this.#source.routerRole()) return [];
		return this.#source.splitTargets().map((t) => ({
			ref: { partitionId: t.partitionId, doName: t.doName },
			start: t.slice.kind === "range" ? t.slice.start : null,
			end: t.slice.kind === "range" ? t.slice.end : null,
		}));
	}

	/** A disjoint, ordered cover of one range request. The host walks it with `forwardRangeVisit`. */
	rangeVisits(input: FokosRangeInput): FokosRangeVisit[] {
		return this.#planRange("rangeVisits", input, true).map((p) => p.visit);
	}

	/** Forwards one registered operation to one target and learns its routes. The envelope is that call's own. */
	async forward<K extends keyof Ops & string>(target: FokosPartitionRef, op: K, req: Ops[K]["req"]): Promise<FokosEnvelope<Ops[K]["res"]>> {
		const descriptor = this.#ops[op];
		invariant(descriptor && descriptor.shape !== "local", () => `fokos/runtime: ${op} cannot be forwarded`);
		const collector = new RouteCollector();
		const value = await this.#forwardTo(collector, target, op, req, this.#scopeKeys(descriptor, req));
		return envelope(value as Ops[K]["res"], collector.build());
	}

	/** Forwards one planned range visit, including the speculative and learned-slice fallbacks. */
	async forwardRangeVisit<K extends keyof Ops & string>(
		visit: FokosRangeVisit,
		op: K,
		req: Ops[K]["req"],
	): Promise<FokosEnvelope<Ops[K]["res"]>> {
		const descriptor = this.#ops[op];
		invariant(descriptor?.shape === "range", () => `fokos/runtime: ${op} is not a range operation`);
		const collector = new RouteCollector();
		const value = await this.#forwardRangeVisit(op, descriptor, visit, req, collector);
		return envelope(value as Ops[K]["res"], collector.build());
	}

	// ═══ signals and jobs ═══════════════════════════════════════════════════

	/** Asks `hooks.evaluateSplit` now, as a local success that signals `evaluateSplit` does. */
	requestSplitEvaluation(): void {
		void this.#applySignals([{ evaluateSplit: true }]);
	}

	/** Routes to the current owner of the key first, then queues the promotion there. */
	async requestPromotion(hashKey: KeyBytes, data?: unknown): Promise<FokosRequestPromotionResult> {
		return await this.#requestPromotion(hashKey, data);
	}

	/** Moves the next run of a host job earlier and arms the alarm for it when necessary. */
	async scheduleJob(name: string, runAt: number): Promise<void> {
		await this.#scheduler.scheduleJob(name, runAt);
	}

	/** One background pass. `alarm(info)` calls this and nothing else. */
	async runDueWork(): Promise<void> {
		await this.#scheduler.runDueWork();
	}

	// ═══ the control-plane RPCs ═════════════════════════════════════════════

	/**
	 * Creates this partition as the target of a repartition, or confirms an identical earlier call.
	 * Only this call creates a range partition, and only this call tells a hash child that it exists.
	 */
	async fokosInit(req: FokosInitRequest): Promise<void> {
		return await this.#guard("fokosInit", async () => await this.#target.initAsTarget(req));
	}

	/** Asks this target to start its import now, instead of at its own fallback alarm. */
	async fokosStartImport(req: FokosStartImportRequest): Promise<void> {
		return await this.#guard("fokosStartImport", async () => await this.#target.startImport(req));
	}

	/** Serves one bounded migration page to a target that is still catching up. */
	async fokosMigrationPull(req: FokosMigrationPullRequest): Promise<FokosMigrationPage> {
		return await this.#guard("fokosMigrationPull", async () => this.#source.servePage(req));
	}

	/** Records that one target holds a complete copy of its slice. */
	async fokosMigrationAck(req: FokosMigrationAckRequest): Promise<void> {
		return await this.#guard("fokosMigrationAck", async () => this.#source.acceptAck(req));
	}

	/**
	 * Serves one read for a repartition target that is still importing from this partition.
	 *
	 * A target cannot serve its own reads until its copy is complete, and it cannot ask this source
	 * through the ordinary API either: the source would route the request straight back to the target
	 * that sent it. This runs the local handler only, with no owner resolution and no lifecycle gate,
	 * for the slice the caller owns. A caller this partition cannot place is rejected outright.
	 */
	async fokosExecuteLocal(req: FokosExecuteLocalRequest): Promise<FokosEnvelope<unknown>> {
		let collector: RouteCollector | undefined;
		return await this.#guard(
			"fokosExecuteLocal",
			async () => {
				collector = new RouteCollector();
				return await this.#executeLocal(req, collector);
			},
			() => collector,
		);
	}

	async #executeLocal(req: FokosExecuteLocalRequest, collector: RouteCollector): Promise<FokosEnvelope<unknown>> {
		const slice = this.#source.resolveCallerSlice(req.repartitionId, req.caller);
		const descriptor = this.#ops[req.op];
		if (!descriptor || descriptor.shape === "local" || descriptor.readOnly !== true) {
			throw new FokosInternalError(SHARDING_INTERNAL_CODES.sharding_operation_invalid, {
				message: "the operation is not registered as a read-only operation",
				attributes: { operation: req.op },
			});
		}
		const hashSplitN = this.identity().topology.hashSplitN;
		const routeCtx = this.routeContext();

		if (descriptor.shape === "point") {
			const key = descriptor.key(req.request);
			if (!sliceIncludesItem(slice, key.hashKey, key.sortKey, hashSplitN)) throw misrouted(req.op, "key outside the caller slice");
			// A promoted key's rows live in the range tree; the local copies are stale or already
			// collected. Only a hash-child caller can reach one: a range or promoted-key slice is itself
			// inside a range tree.
			if (slice.kind === "hash_child" && this.#store.hasTerminalRouteOverride(key.hashKey)) {
				const owner = this.#rangeOwner(key.hashKey, key.sortKey, "override", false, true);
				const value = await this.#forwardTo(collector, owner.target, req.op, req.request, [key.hashKey]);
				return envelope(value, collector.build());
			}
			collector.add(this.#selfNode("executed"));
			const value = await this.#runLocal(descriptor, req.request, NOOP_CALL);
			return envelope(value, collector.build());
		}

		invariant(descriptor.shape === "range", "fokos/runtime: read_source is allowed for point and range operations only");
		const input = descriptor.range(req.request);
		if (!sliceIncludesHashKey(slice, input.hashKey, hashSplitN)) throw misrouted(req.op, "hash key outside the caller slice");
		let request = req.request;
		if (slice.kind === "range") {
			const start = slice.start ?? NO_SORT_KEY;
			if (!rangeIntersects(start, slice.end, input.interval)) throw misrouted(req.op, "interval disjoint from the caller slice");
			// The caller proves its slice with the cursor too: one that lies outside it asks for rows of
			// another caller, which is a routing defect, not a rescan.
			if (input.cursor && !cursorFallsInChild(start, slice.end, input.cursor)) throw misrouted(req.op, "cursor outside the caller slice");
			request = descriptor.clip(req.request, { target: "local", start: slice.start, end: slice.end, speculative: false });
		}
		if (slice.kind === "hash_child" && this.#store.hasTerminalRouteOverride(input.hashKey)) {
			// A range request spans sort keys, so it carries no one key that could select a deeper range
			// slice; it enters at the range root and the routers below it fan out.
			const root = refOf(resolveRangePartitionContext(routeCtx, input.hashKey, null, null));
			const value = await this.#forwardTo(collector, root, req.op, request, [input.hashKey]);
			return envelope(value, collector.build());
		}
		collector.add(this.#selfNode("executed"));
		const value = await this.#runLocal(descriptor, request, NOOP_CALL);
		return envelope(value, collector.build());
	}

	/**
	 * Queues a key promotion on the partition that owns the key now. A router forwards to the owner,
	 * because a promotion queued on a router would migrate a stale snapshot and then shadow the live
	 * rows in the child.
	 */
	async fokosRequestPromotion(req: FokosRequestPromotionRequest): Promise<FokosRequestPromotionResult> {
		return await this.#guard("fokosRequestPromotion", async () => {
			if (!this.#identity) throw this.#notInitialized(req.target);
			if (this.#identity.ref.partitionId !== req.target.partitionId || this.#identity.ref.doName !== req.target.doName) {
				throw contextMismatch({ doName: req.target.doName, expected: this.#identity.ref.doName });
			}
			return await this.#requestPromotion(req.hashKey, req.data);
		});
	}

	async #requestPromotion(hashKey: KeyBytes, data: unknown): Promise<FokosRequestPromotionResult> {
		const identity = this.identity();
		const owner = identity.ref;
		if (this.#target.isImporting()) {
			throw new FokosUnavailableError(UNAVAILABLE_CODES.partition_migrating, {
				message: "the owner of the key is still importing, please retry later",
				attributes: { operation: "fokosRequestPromotion" },
			});
		}
		// A range partition serves the key from its own tree: the key is promoted for good.
		if (identity.kind === "range") return { owner, queued: false, reason: "already_promoted", state: "cleaned" };

		const override = this.#store.routeOverrideFor(hashKey);
		if (override) {
			// A promotion in flight gets its fallback alarm back and one pass now: a request that asks
			// about it is the one signal a partition gets when an eviction lost its alarm.
			if (override.state === "queued" || override.state === "planned" || override.state === "cutover") {
				await this.#scheduler.ensureAlarmAtMost(Date.now() + this.#fallbackAlarmMs);
				this.#scheduler.wake();
			}
			return { owner, queued: false, reason: "already_promoted", state: override.state };
		}
		const resolution = this.#resolve({ hashKey, sortKey: NO_SORT_KEY }, EXACT);
		if (resolution.kind === "out_of_range") throw misrouted("fokosRequestPromotion", "key outside this partition");
		if (resolution.kind === "remote") {
			return await this.#peer(resolution.target).fokosRequestPromotion({ target: resolution.target, hashKey, data });
		}
		if (!this.#source.canQueue({ kind: "key_promotion", hashKey })) return { owner, queued: false, reason: "split_in_progress" };
		// The fallback alarm moves earlier BEFORE the queue transaction, so a crash between the two
		// leaves an alarm that reads the new row. A failed alarm write creates no row.
		await this.#scheduler.ensureAlarmAtMost(Date.now() + this.#fallbackAlarmMs);
		const row = this.#source.queue({ kind: "key_promotion", hashKey, data });
		if (!row) return { owner, queued: false, reason: "split_in_progress" };
		console.log({
			...this.#logParams(),
			message: "fokos/runtime: key queued for promotion.",
			hashKey: KeyCodec.keyForLog(hashKey),
			repartitionId: row.id,
		});
		this.#scheduler.wake();
		return { owner, queued: true, state: row.state };
	}

	/**
	 * One bounded page of every repartition this partition holds, with the target links inside it. A
	 * root request carries its context and bootstraps an empty root. A target request omits the
	 * context and never creates an empty partition.
	 */
	async fokosStatus(req: FokosStatusRequest): Promise<FokosStatusPage> {
		return await this.#guard("fokosStatus", async () => {
			if (req.rootContext) this.#ensureIdentity(req.rootContext as FokosRouteContext<TPolicy>);
			const destroying = this.#store.isDestroying();
			if (!this.#identity) return { initialized: false, destroying, ref: null, importState: null, entries: [], nextCursor: null };
			const { entries, nextCursor } = this.#source.statusEntries(req.cursor, STATUS_PAGE_ENTRIES, STATUS_PAGE_BYTES);
			return { initialized: true, destroying, ref: this.#identity.ref, importState: this.#target.importState(), entries, nextCursor };
		});
	}

	/**
	 * Fences this partition for destroy. Every background transition stops, and no new target appears.
	 * One transaction writes the fence and the optional root bootstrap. The call then waits for the
	 * pass in flight, which re-reads the fence before each remaining step, and deletes the alarm after
	 * it. A repeated call succeeds.
	 */
	async fokosPrepareDestroy(req: FokosPrepareDestroyRequest): Promise<void> {
		return await this.#guard("fokosPrepareDestroy", async () => {
			this.#store.transactionSync(() => {
				if (req.rootContext) this.#ensureIdentity(req.rootContext as FokosRouteContext<TPolicy>);
				this.#store.setDestroying();
			});
			await this.#scheduler.inFlight();
			this.#scheduler.stop();
			await this.#ctx.storage.deleteAlarm();
		});
	}

	/** Cancels the schedule, deletes all storage, and aborts the instance so the next caller gets a fresh one. */
	async fokosDestroy(): Promise<void> {
		return await this.#guard("fokosDestroy", async () => {
			this.#scheduler.stop();
			console.warn({ ...this.#logParams(), message: "fokos/runtime: destroying the partition, deleting all storage." });
			await this.#ctx.blockConcurrencyWhile(async () => {
				// Clears every timer of the instance: setTimeout returns a numeric ID that increments on
				// each call, so the newest ID gives the upper bound to clear from.
				const highestId = setTimeout(() => {
					for (let i = Number(highestId); i >= 0; i--) clearTimeout(i);
				}, 0);
				// The alarm goes before the storage, so the platform does not fire it on the evicted instance.
				await this.#ctx.storage.deleteAlarm();
				await this.#ctx.storage.deleteAll();
			});
			// Evicts the instance. The caller sees a throw with the sentinel message and ignores it.
			this.#ctx.abort(DESTROY_ABORT_SENTINEL);
		});
	}

	async alarm(info: AlarmInvocationInfo): Promise<void> {
		console.log({ ...this.#logParams(), message: "fokos/runtime: alarm triggered.", alarmInfo: info });
		await this.runDueWork();
	}

	// ═══ identity ═══════════════════════════════════════════════════════════

	/**
	 * Validates the route context of a request against the stored identity, and stores a changed
	 * policy. A root hash partition without an identity takes it from its first request. Every other
	 * partition is created by `fokosInit` only. Synchronous: `fokosPrepareDestroy` calls it inside the
	 * transaction that writes the fence.
	 */
	#ensureIdentity(routeCtx: FokosRouteContext<TPolicy>): void {
		if (this.#ctx.id.jurisdiction !== routeCtx.topology.jurisdiction) {
			throw contextMismatch({
				doName: routeCtx.doName,
				jurisdictionReq: routeCtx.topology.jurisdiction,
				jurisdictionActual: this.#ctx.id.jurisdiction,
			});
		}
		if (!this.#identity) {
			if (isRangePartition(routeCtx)) throw this.#notInitialized(routeCtx);
			invariant(routeCtx.partitionId.length > 0, "fokos/runtime: partitionId must not be empty");
			if (PartitionIdHelper.depth(Uint8Array.fromHex(routeCtx.partitionId)) > 0) throw this.#notInitialized(routeCtx);
			this.#writeIdentity(routeCtx);
			return;
		}
		const identity = this.#identity;
		if (
			identity.ref.partitionId !== routeCtx.partitionId ||
			identity.ref.doName !== routeCtx.doName ||
			!topologiesEqual(identity.topology, routeCtx.topology)
		) {
			throw contextMismatch({ doName: routeCtx.doName, expected: identity.ref.doName });
		}
		const stored = this.#stored!;
		if (structurallyEqual(stored.rangeConfig, routeCtx.rangeConfig) && structurallyEqual(stored.policy, routeCtx.policy)) return;
		validateRangeConfig(routeCtx.rangeConfig);
		const next: FokosStoredPolicy<TPolicy> = { rangeConfig: routeCtx.rangeConfig, policy: routeCtx.policy };
		this.#store.transactionSync(() => this.#store.putPolicy(next));
		this.#setIdentity(identity, next);
	}

	/** Writes the identity of a target from its `fokosInit`. It runs inside the transaction that writes the import record. */
	#applyTargetIdentity(req: FokosInitRequest): void {
		const target = req.target as FokosRouteContext<TPolicy>;
		if (this.#identity) {
			this.#ensureIdentity(target);
			return;
		}
		let range: { depth: number; ancestors: RangeAncestorInfo[] } | undefined;
		if (isRangePartition(target)) {
			invariant(req.rangeDepth !== undefined, "fokos/runtime.fokosInit: a range target needs its depth");
			const ancestors = req.rangeAncestors ?? [];
			invariant(ancestors.length === 0 || req.rangeDepth > 0, "fokos/runtime.fokosInit: only a non-root range partition has ancestors");
			range = { depth: req.rangeDepth, ancestors };
		}
		this.#writeIdentity(target, range);
	}

	#writeIdentity(routeCtx: FokosRouteContext<TPolicy>, range?: { depth: number; ancestors: RangeAncestorInfo[] }): void {
		validateTopology(routeCtx.topology);
		validateRangeConfig(routeCtx.rangeConfig);
		const identity = partitionIdentityFrom(routeCtx, range);
		const stored: FokosStoredPolicy<TPolicy> = { rangeConfig: routeCtx.rangeConfig, policy: routeCtx.policy };
		this.#store.transactionSync(() => {
			this.#store.putIdentity(identity);
			this.#store.putPolicy(stored);
		});
		this.#setIdentity(identity, stored);
	}

	/** Replaces the in-memory identity, policy and derived state together, so no reader sees one without the other. */
	#setIdentity(identity: FokosPartitionIdentity, stored: FokosStoredPolicy<TPolicy>): void {
		this.#identity = identity;
		this.#stored = stored;
		this.#routeCtx = { schema: 2, ...identity.ref, topology: identity.topology, ...stored };
		const range = identity.range;
		this.#rangeAncestorsWithSelf =
			range && range.ancestors.length > 0
				? range.ancestors.concat({ depth: range.depth, startBoundary: range.start ?? NO_SORT_KEY, endBoundary: range.end ?? NO_SORT_KEY })
				: [];
	}

	#selfNode(role: FokosServedRole): FokosRouteNode {
		const identity = this.identity();
		const depth = identityDepth(identity);
		return {
			ref: identity.ref,
			actorId: this.#ctx.id.toString(),
			hashDepth: identity.kind === "hash" ? depth : 0,
			rangeDepth: identity.kind === "range" ? depth : 0,
			role,
			...(this.#rangeAncestorsWithSelf.length > 0 ? { _rangeAncestors: this.#rangeAncestorsWithSelf } : {}),
		};
	}

	/** The initialization error of the partition kind `ref` names, or of a hash partition when nothing names it. */
	#notInitialized(ref?: FokosPartitionRef): FokosRoutingError {
		const doName = ref?.doName ?? this.#ctx.id.name;
		if (ref && isRangePartition(ref)) {
			return new FokosRoutingError(SHARDING_ROUTING_CODES.range_partition_not_initialized, {
				message: "range partition is not initialized; route via the range root and traverse",
				attributes: { doName },
			});
		}
		return new FokosRoutingError(SHARDING_ROUTING_CODES.hash_partition_not_initialized, {
			message: "hash partition is not initialized; only fokosInit creates a non-root hash partition",
			attributes: { doName },
		});
	}

	// ═══ owner resolution ═══════════════════════════════════════════════════

	/** The range form of the ownership check: the hash key, and the whole interval inside a range partition. */
	#assertCanOwnRange(op: string, input: FokosRangeInput): void {
		const identity = this.identity();
		if (identity.kind === "hash") {
			if (!this.#hashesHere(input.hashKey, identity)) throw misrouted(op, "hash key outside this partition");
			return;
		}
		const range = identity.range!;
		if (KeyCodec.compare(input.hashKey, range.hashKey) !== 0) throw misrouted(op, "hash key outside this range partition");
		if (!intervalInside(input.interval, range.start, range.end)) throw misrouted(op, "interval outside this range partition");
	}

	/** Hash step 1 and range step 1: can this partition own the key at all, by its identity alone. */
	#ownsByTopology(key: RouteKey): boolean {
		const identity = this.identity();
		if (identity.kind === "hash") return this.#hashesHere(key.hashKey, identity);
		const range = identity.range!;
		return (
			KeyCodec.compare(key.hashKey, range.hashKey) === 0 &&
			KeyCodec.compare(key.sortKey, range.start ?? NO_SORT_KEY) >= 0 &&
			(range.end === null || KeyCodec.compare(key.sortKey, range.end) < 0)
		);
	}

	#hashesHere(hashKey: KeyBytes, identity: FokosPartitionIdentity): boolean {
		const hash = identity.hash!;
		const { rootTreesN, hashSplitN } = identity.topology;
		if (hashRootIndex(hashKey, rootTreesN) !== hash.rootIndex) return false;
		return hash.path.every((idx, depth) => hashChildIndex(hashKey, depth, hashSplitN) === idx);
	}

	#resolve(key: RouteKey, opts: ResolveOptions): Resolution {
		const identity = this.identity();
		if (!this.#ownsByTopology(key)) return { kind: "out_of_range" };

		if (identity.kind === "hash") {
			const override = this.#store.routeOverrideFor(key.hashKey);
			if (override && cutOver(override.state)) return this.#rangeOwner(key.hashKey, key.sortKey, "override", false, opts.learnedRange);
			if (opts.bloom && this.#bloom?.maybePromoted(key.hashKey))
				return this.#rangeOwner(key.hashKey, key.sortKey, "bloom", true, opts.learnedRange);
			if (!this.#source.routerRole()) return { kind: "local" };
			const relDepth = Math.max(1, this.#arena()?.findLeaf(key.hashKey) ?? 0);
			return {
				kind: "remote",
				target: this.#hashDescendant(key.hashKey, relDepth),
				speculative: false,
				via: "hash",
				relDepth,
				learned: null,
			};
		}

		if (!this.#source.routerRole()) return { kind: "local" };
		const range = identity.range!;
		const child = this.#rangeChildFor(key.sortKey);
		const learned = opts.learnedRange ? this.#store.findDeepestKnownRangeSlice(range.hashKey, key.sortKey) : null;
		const jump = learned && isStrictSubSlice(learned, child.start, child.end) ? learned : null;
		const target = jump
			? refOf(resolveRangePartitionContext(this.routeContext(), range.hashKey, jump.startBoundary, jump.endBoundary))
			: child.ref;
		return { kind: "remote", target, speculative: false, via: "range", learned: jump };
	}

	/** The range partition that serves a promoted key: the deepest learned slice that contains the sort key, or the root. */
	#rangeOwner(
		hashKey: KeyBytes,
		sortKey: KeyBytes,
		via: "override" | "bloom",
		speculative: boolean,
		learnedRange: boolean,
	): Extract<Resolution, { kind: "remote" }> {
		const routeCtx = this.routeContext();
		const learned = learnedRange ? this.#store.findDeepestKnownRangeSlice(hashKey, sortKey) : null;
		const jump = learned && (learned.startBoundary !== null || learned.endBoundary !== null) ? learned : null;
		const target = jump
			? refOf(resolveRangePartitionContext(routeCtx, hashKey, jump.startBoundary, jump.endBoundary))
			: refOf(resolveRangePartitionContext(routeCtx, hashKey, null, null));
		return { kind: "remote", target, speculative, via, learned: jump };
	}

	/** The hash descendant `relDepth` levels below this partition that owns `hashKey`. */
	#hashDescendant(hashKey: KeyBytes, relDepth: number): FokosPartitionRef {
		const identity = this.identity();
		const depth = identityDepth(identity);
		const idxs = Array.from({ length: relDepth }, (_, i) => hashChildIndex(hashKey, depth + i, identity.topology.hashSplitN));
		return refOf(resolveDescendantHashPartitionContext(this.routeContext(), Uint8Array.fromHex(identity.ref.partitionId), idxs));
	}

	/** The direct range child whose interval contains `sortKey`. The children tile the whole interval, so one always does. */
	#rangeChildFor(sortKey: KeyBytes): FokosChild {
		let best: FokosChild | null = null;
		for (const child of this.children()) {
			if (KeyCodec.compare(child.start ?? NO_SORT_KEY, sortKey) <= 0 && (best === null || startCmp(child.start, best.start) > 0))
				best = child;
		}
		invariant(best !== null, () => `fokos/runtime: no range child owns ${KeyCodec.keyForLog(sortKey)}`);
		return best;
	}

	/** The hash arena of a router, created the first time this partition forwards. Null on an owner. */
	#arena(): HashTopology | null {
		if (this.#hashArena) return this.#hashArena;
		const identity = this.identity();
		if (identity.kind !== "hash" || !this.#source.routerRole()) return null;
		const snapshot = this.#store.getHashArena();
		const opts = this.#hashArenaBytes === undefined ? undefined : { budgetBytes: this.#hashArenaBytes };
		this.#hashArena = snapshot
			? HashTopology.fromSnapshot(snapshot, opts)
			: HashTopology.create(identity.topology.hashSplitN, identityDepth(identity), opts);
		return this.#hashArena;
	}

	// ═══ the range frontier ═════════════════════════════════════════════════

	#planRange(op: string, input: FokosRangeInput, bloom: boolean): PlannedVisit[] {
		const identity = this.identity();
		const routeCtx = this.routeContext();
		let bases: FrontierBase[];
		let learned: LearnedRangeSlice[] = [];
		this.#assertCanOwnRange(op, input);

		if (identity.kind === "hash") {
			const override = this.#store.routeOverrideFor(input.hashKey);
			const root = refOf(resolveRangePartitionContext(routeCtx, input.hashKey, null, null));
			if (override && cutOver(override.state)) {
				bases = [{ target: root, start: null, end: null, speculative: false }];
				learned = this.#store.listLearnedRangeSlices(input.hashKey);
			} else if (bloom && this.#bloom?.maybePromoted(input.hashKey)) {
				bases = [{ target: root, start: null, end: null, speculative: true }];
				learned = this.#store.listLearnedRangeSlices(input.hashKey);
			} else if (this.#source.routerRole()) {
				const relDepth = Math.max(1, this.#arena()?.findLeaf(input.hashKey) ?? 0);
				bases = [{ target: this.#hashDescendant(input.hashKey, relDepth), start: null, end: null, speculative: false }];
			} else {
				bases = [{ target: "local", start: null, end: null, speculative: false }];
			}
		} else {
			const range = identity.range!;
			if (this.#source.routerRole()) {
				bases = this.children().map((child) => ({ target: child.ref, start: child.start, end: child.end, speculative: false }));
				learned = this.#store.listLearnedRangeSlices(range.hashKey);
			} else {
				bases = [{ target: "local", start: range.start, end: range.end, speculative: false }];
			}
		}

		const planned = planRangeFrontier(bases, learned, input.interval, input.descending, (start, end) =>
			refOf(resolveRangePartitionContext(routeCtx, input.hashKey, start, end)),
		);
		for (const p of planned) this.#plans.set(p.visit, p);
		return planned;
	}

	/**
	 * Forwards one planned visit. A learned slice that does not exist is forgotten and the segment
	 * goes to its base target. A speculative visit that finds no range partition, or no cutover,
	 * resolves the hash tree again with the Bloom step off: on a router the target is the hash child,
	 * on a leaf it is this partition and the local handler answers for the same visit. Every retry of a
	 * cache miss goes through the same ladder, so a miss on a retry cannot skip a fallback.
	 * `MAX_FORWARD_RETRIES` bounds the misses one call can absorb.
	 */
	async #forwardRangeVisit(
		op: string,
		descriptor: Extract<AnyOperation, { shape: "range" }>,
		visit: FokosRangeVisit,
		req: unknown,
		collector: RouteCollector,
	): Promise<unknown> {
		let plan = this.#plans.get(visit);
		invariant(plan, "fokos/runtime.forwardRangeVisit: the visit was not planned by this runtime");
		invariant(visit.target !== "local", "fokos/runtime.forwardRangeVisit: a local visit is served by the tracked local function");
		const input = descriptor.range(req);
		let target = visit.target;
		for (let attempt = 0; ; attempt++) {
			try {
				return await this.#forwardTo(collector, target, op, req, [input.hashKey]);
			} catch (e) {
				if (attempt >= MAX_FORWARD_RETRIES) {
					throw e;
				}

				const notInitialized = FokosError.isCode(e, SHARDING_ROUTING_CODES.range_partition_not_initialized);
				const notCutOver = FokosError.isCode(e, SHARDING_UNAVAILABLE_CODES.repartition_not_cut_over);
				if (plan.learned && notInitialized) {
					this.#store.deleteLearnedRangeSlice(input.hashKey, plan.learned.startBoundary, plan.learned.endBoundary);
					invariant(plan.base.target !== "local", "fokos/runtime.forwardRangeVisit: a learned slice never overlays a local base");
					target = plan.base.target;
					plan = { visit, base: plan.base, learned: null };
					continue;
				}
				if (plan.base.speculative && (notInitialized || notCutOver)) {
					const again = this.#planRange(op, input, false);
					invariant(again.length === 1, "fokos/runtime.forwardRangeVisit: a hash partition plans one base visit");
					const fallback = again[0];
					if (fallback.visit.target === "local") {
						collector.add(this.#selfNode("executed"));
						return await this.#runLocal(descriptor, req, NOOP_CALL);
					}
					target = fallback.visit.target;
					plan = fallback;
					continue;
				}
				throw e;
			}
		}
	}

	// ═══ forwarding and learning ════════════════════════════════════════════

	/**
	 * One outbound RPC to a partition of the host's own class. The default calls the method named
	 * after the operation with the derived route context. The list of the answer, or of the error, is
	 * learned against `hashKeys`, the keys of this request that the RPC carried, and merged into
	 * `collector` here, so every path that forwards pays for it once.
	 */
	async #forwardTo(
		collector: RouteCollector,
		target: FokosPartitionRef,
		op: string,
		req: unknown,
		hashKeys: readonly KeyBytes[],
	): Promise<unknown> {
		const descriptor = this.#ops[op];
		invariant(descriptor && descriptor.shape !== "local", () => `fokos/runtime: ${op} cannot be forwarded`);
		const targetCtx: FokosRouteContext<TPolicy> = { ...this.routeContext(), partitionId: target.partitionId, doName: target.doName };
		const stub = this.#stub(targetCtx, target.doName);
		const call = descriptor.forward
			? descriptor.forward(stub, targetCtx, req)
			: ((stub as unknown as Record<string, (ctx: unknown, req: unknown) => Promise<FokosEnvelope<unknown>>>)[op](
					targetCtx,
					req,
				) as Promise<FokosEnvelope<unknown>>);
		// Only the partition that enters the range tree stamps its depth. A hash-to-hash forward must
		// leave the nodes alone, or a router above would overwrite the depth of the partition that owns
		// the promoted key with its own shallower one.
		const stamp = isRangePartition(target) ? this.#rangeDepthStamp() : undefined;
		try {
			const result = await call;
			this.#learn(result.routing.servedBy, hashKeys);
			collector.mergeForwarded(result.routing, stamp);
			return result.value;
		} catch (e) {
			// The error path learns what the success path learns, and the same error object travels on
			// with this hop added, so an error and a result that take one route carry one routing.
			const routed = routedError(e);
			if (routed) {
				try {
					this.#learn(routed.routing.servedBy, hashKeys);
				} catch {}
				collector.mergeForwarded(routed.routing, stamp);
				routed.routing = collector.build();
			}
			throw e;
		}
	}

	/**
	 * A range partition has no hash depth of its own. The hash partition that enters the range tree
	 * writes its own depth on every range node it merges, so a hash router above learns where the
	 * promoted key left the hash tree. A range partition changes nothing.
	 */
	#rangeDepthStamp(): ((node: FokosRouteNode) => FokosRouteNode) | undefined {
		const identity = this.identity();
		if (identity.kind !== "hash") return undefined;
		const hashDepth = identityDepth(identity);
		return (node) => (isRangePartition(node.ref) ? { ...node, hashDepth } : node);
	}

	/**
	 * Learns from the nodes of a child envelope for the hash keys of this request: the range
	 * boundaries and the promotion of every range node, and the depth of the deepest hash node that
	 * owns each key. The identity of a node is its scope, so a hash node owns a key when the key hashes
	 * along its path, and a range node names its hash key itself.
	 */
	#learn(nodes: readonly FokosRouteNode[], hashKeys: readonly KeyBytes[]): void {
		const identity = this.identity();
		const myDepth = identityDepth(identity);
		const arena = identity.kind === "hash" ? this.#arena() : null;
		let arenaChanged = false;
		let bloomChanged = false;
		const learnDepth = (hashKey: KeyBytes, hashDepth: number) => {
			const relDepth = hashDepth - myDepth;
			if (arena && relDepth > 0) arenaChanged = arena.updateFromHint(hashKey, relDepth) || arenaChanged;
		};
		for (const node of nodes) {
			const bytes = Uint8Array.fromHex(node.ref.partitionId);
			if (isRangePartition(node.ref)) {
				const decoded = PartitionIdHelper.decode(bytes);
				invariant(decoded.schema === PartitionIdHelper.SCHEMA_RANGE_V1, "fokos/runtime.learn: a range node decodes to a range id");
				const hashKey = decoded.hashKey;
				for (const ancestor of node._rangeAncestors ?? []) {
					this.#store.learnRangeBoundary(hashKey, ancestor.startBoundary, ancestor.endBoundary, ancestor.depth);
				}
				if (identity.kind !== "hash") continue;
				const added = this.#bloomForLearning().learnPromotedKey(hashKey);
				if (added === AddResult.Added) bloomChanged = true;
				else if (added === AddResult.Full) {
					console.info({
						...this.#logParams(),
						message: "fokos/runtime: the promotion Bloom filter is full, cannot learn a promoted key.",
					});
				}
				learnDepth(hashKey, node.hashDepth);
				continue;
			}
			for (const hashKey of hashKeys) {
				if (hashPathOwns(bytes, hashKey, identity.topology)) learnDepth(hashKey, node.hashDepth);
			}
		}
		if (arenaChanged && arena) this.#store.putHashArena(arena.toSnapshot());
		if (bloomChanged) this.#store.putPromotionBloom(this.#bloom!.toSnapshot());
	}

	#bloomForLearning(): PartialRangeTopology {
		this.#bloom ??= PartialRangeTopology.create({
			errorRate: this.#bloomOptions.falsePositiveRate,
			initialCapacityN: this.#bloomOptions.expectedKeys,
			// The serialized filter is one KV value, which must stay below the 2 MB limit.
			maxSizeBytes: 1.5 * 1024 * 1024,
		});
		return this.#bloom;
	}

	// ═══ local execution, admission, signals ════════════════════════════════

	/** A `local` or `beforeForward` call collects its signals; the caller applies them after a success. */
	#localCall(): { call: FokosLocalCall; signals: FokosSignals[] } {
		const signals: FokosSignals[] = [];
		return { call: { signal: (s) => signals.push(s) }, signals };
	}

	#runLocal(descriptor: Exclude<AnyOperation, { shape: "local" }>, req: unknown, call: FokosLocalCall): unknown {
		const out = descriptor.local(req, call);
		if ((descriptor.localMode ?? "sync") === "sync" && isThenable(out)) {
			throw new FokosInternalError(SHARDING_INTERNAL_CODES.sharding_local_must_be_sync, {
				message: 'a synchronous local handler returned a thenable; declare localMode: "async" and close the cutover race in the handler',
			});
		}
		return out;
	}

	/** Runs `beforeForward` when the descriptor has it, and returns the signals it reported. */
	#beforeForward(descriptor: Exclude<AnyOperation, { shape: "local" }>, req: unknown): FokosSignals[] {
		if (!descriptor.beforeForward) return [];
		const { call, signals } = this.#localCall();
		descriptor.beforeForward(req, call);
		return signals;
	}

	#admit(op: string, descriptor: Exclude<AnyOperation, { shape: "local" }>, keys: RouteKey[]): void {
		if (!this.#hooks.admit) return;
		const runtime = this;
		const decision = this.#hooks.admit({
			op,
			admissionTag: descriptor.admissionTag,
			keys,
			// Read on demand: every dispatch admits, and `lifecycle()` reads the import record and queries
			// the repartition rows to answer. A host that admits on its own size or tag alone never asks.
			get lifecycle() {
				return runtime.lifecycle();
			},
			policy: this.policy(),
		});
		if (decision !== "allow") throw decision.reject;
	}

	/** The hash keys a request names, which a forward learns against. */
	#scopeKeys(descriptor: Exclude<AnyOperation, { shape: "local" }>, req: unknown): KeyBytes[] {
		return this.#scopeRouteKeys(descriptor, req).map((key) => key.hashKey);
	}

	/** The keys a request names, for the ownership check of the read-through gate. */
	#scopeRouteKeys(descriptor: Exclude<AnyOperation, { shape: "local" }>, req: unknown): RouteKey[] {
		switch (descriptor.shape) {
			case "point":
				return [descriptor.key(req)];
			case "group":
			case "single_owner":
				return descriptor.items(req).map((item) => item.key);
			case "range":
				return [{ hashKey: descriptor.range(req).hashKey, sortKey: NO_SORT_KEY }];
		}
	}

	/**
	 * Applies the signals of a completed local phase. The result is already committed, so a failure
	 * here is logged and does not change it: the next request repeats the checks.
	 */
	async #applySignals(signals: readonly FokosSignals[]): Promise<void> {
		try {
			for (const s of signals) {
				if (s.evaluateSplit) await this.#evaluateSplit();
				for (const candidate of s.promotionCandidates ?? []) await this.#requestPromotion(candidate.hashKey, candidate.data);
				if (s.repartitionUnblocked && this.#source.onRepartitionUnblocked()) this.#scheduler.wake();
				for (const job of s.jobs ?? []) await this.#scheduler.scheduleJob(job.name, job.runAt);
			}
		} catch (error) {
			console.error({
				...this.#logParams(),
				message: "fokos/runtime: applying the signals of a completed operation failed.",
				error: String(error),
				errorProps: error,
			});
		}
	}

	async #evaluateSplit(): Promise<void> {
		const identity = this.identity();
		// A router has nothing to split: its targets own the keys.
		if (this.#source.routerRole()) return;
		const decision = this.#hooks.evaluateSplit({ identity, policy: this.policy() });
		if (decision === false) return;
		const kind = identity.kind === "hash" ? "hash_split" : "range_split";
		// The synchronous precheck rejects an ineligible attempt without an alarm write.
		if (!this.#source.canQueue({ kind })) return;
		await this.#scheduler.ensureAlarmAtMost(Date.now() + this.#fallbackAlarmMs);
		const row = this.#source.queue({ kind, data: decision.data });
		if (!row) return;
		console.log({ ...this.#logParams(), message: "fokos/runtime: split conditions met.", repartitionId: row.id, kind: row.kind });
		this.#scheduler.wake();
	}

	// ═══ jobs ═══════════════════════════════════════════════════════════════

	#importPagesPerPass(): number {
		const value = this.#hooks.runtimeConfig?.().importPagesPerPass ?? DEFAULT_IMPORT_PAGES_PER_PASS;
		invariant(
			Number.isInteger(value) && value >= 1,
			() => `fokos/runtime: importPagesPerPass must be an integer of at least 1, got ${value}`,
		);
		return value;
	}

	#builtinJobs(): FokosJob[] {
		const canRun = () => this.#identity !== undefined;
		const importRecord = () => this.#target.importRecord();
		const jobs: Record<(typeof BUILTIN_JOBS)[number], FokosJob> = {
			target_import: {
				name: "target_import",
				canRun,
				deadline: () => {
					const rec = importRecord();
					return rec && (rec.state === "awaiting_data" || rec.state === "importing") ? rec.nextAttemptAt : null;
				},
				runStep: async () => {
					for (let i = 0; i < this.#importPagesPerPass(); i++) {
						if (this.#store.isDestroying()) break;
						if ((await this.#target.importOnePage()) !== "progressed") break;
					}
					return { nextRunAt: null };
				},
			},
			target_ack: {
				name: "target_ack",
				canRun,
				deadline: () => {
					const rec = importRecord();
					return rec?.state === "imported" ? rec.nextAttemptAt : null;
				},
				runStep: async () => {
					await this.#target.sendAck();
					return { nextRunAt: null };
				},
			},
			source_repartition: {
				name: "source_repartition",
				canRun,
				deadline: () => this.#store.earliestRepartitionDeadline(),
				runStep: async () => {
					await this.#source.sourceStep();
					return { nextRunAt: null };
				},
			},
			source_cleanup: {
				name: "source_cleanup",
				canRun,
				deadline: () => this.#store.earliestCleanupDeadline(),
				runStep: () => {
					this.#source.sourceCleanupStep();
					return { nextRunAt: null };
				},
			},
		};
		return BUILTIN_JOBS.map((name) => jobs[name]);
	}

	// ═══ plumbing ═══════════════════════════════════════════════════════════

	/**
	 * Runs one entry point: the destroy fence first, then `fn`, and every error that leaves is a
	 * `FokosError` that carries the routing of this partition. `fokosStatus`, `fokosPrepareDestroy`,
	 * and `fokosDestroy` stay available behind the fence.
	 */
	async #guard<T>(name: string, fn: () => Promise<T>, collector?: () => RouteCollector | undefined): Promise<T> {
		try {
			const descriptor = this.#ops[name];
			const allowedWhileDestroying =
				name === "fokosStatus" ||
				name === "fokosPrepareDestroy" ||
				name === "fokosDestroy" ||
				(descriptor?.shape === "local" && descriptor.allowedWhileDestroying === true);
			if (this.#store.isDestroying() && !allowedWhileDestroying) {
				throw new FokosUnavailableError(UNAVAILABLE_CODES.partition_migrating, {
					message: "partition destroy in progress, please retry later",
					attributes: { operation: name },
				});
			}
			return await fn();
		} catch (e) {
			const err = FokosError.wrap(e);
			// Best effort: a failed attachment must never replace the error. A partition without an
			// identity attaches nothing, because its routing would name nothing.
			if (!routedError(err) && this.#identity) {
				try {
					const routes = collector?.() ?? new RouteCollector();
					// Admission, the lifecycle gate, and owner resolution all throw before any handler
					// runs, so the list can still be empty here. The caller must learn which partition
					// refused the request either way, and a partition that had already forwarded is the
					// router of the parts it merged.
					routes.addRaiser(this.#selfNode(routes.isEmpty ? "executed" : "merged"));
					attachRouting(err, routes.build());
				} catch {}
			}
			throw err;
		}
	}

	#peer(ref: FokosPartitionRef): FokosShardingRpc {
		return this.#stub(this.routeContext(), ref.doName) as unknown as FokosShardingRpc;
	}

	#validateOperations(): void {
		for (const [name, descriptor] of Object.entries(this.#ops)) {
			if (descriptor.shape === "local") continue;
			if (descriptor.whileMigrating !== "read_source") continue;
			if (descriptor.readOnly !== true || (descriptor.shape !== "point" && descriptor.shape !== "range")) {
				throw new FokosInternalError(SHARDING_INTERNAL_CODES.sharding_operation_invalid, {
					message: "a read_source operation must be readOnly and of the point or range shape",
					attributes: { operation: name, shape: descriptor.shape, readOnly: descriptor.readOnly },
				});
			}
		}
	}

	#logParams(): Record<string, unknown> {
		const identity = this.#identity;
		const importSource = this.#target.importRecord()?.source;
		return {
			actorId: this.#ctx.id.toString(),
			actorName: this.#ctx.id.name,
			partitionId: identity?.ref.partitionId,
			doName: identity?.ref.doName,
			kind: identity?.kind,
			depth: identity ? identityDepth(identity) : undefined,
			range: rangeForLog(identity),
			...(importSource ? { importSource: importSource.doName } : {}),
		};
	}
}

function cutOver(state: RepartitionState): boolean {
	return state === "cutover" || state === "completed" || state === "cleaned";
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
	return typeof value === "object" && value !== null && typeof (value as { then?: unknown }).then === "function";
}

/** True when `hashKey` hashes along the whole path a hash partition id encodes: the root index, then one child index per level. */
function hashPathOwns(idBytes: Uint8Array, hashKey: KeyBytes, topology: { rootTreesN: number; hashSplitN: number }): boolean {
	const decoded = PartitionIdHelper.decode(idBytes);
	if (decoded.schema !== PartitionIdHelper.SCHEMA_HASH_V1) return false;
	if (hashRootIndex(hashKey, topology.rootTreesN) !== decoded.rootIdx) return false;
	for (let d = 0; d < decoded.depth; d++) {
		if (hashChildIndex(hashKey, d, topology.hashSplitN) !== idBytes[4 + d]) return false;
	}
	return true;
}

/** True when the request interval lies inside the half-open `[start, end)` of a range partition. */
function intervalInside(interval: SkInterval, start: KeyBytes | null, end: KeyBytes | null): boolean {
	if (start !== null && (interval.lower === undefined || KeyCodec.compare(interval.lower.value, start) < 0)) return false;
	if (end !== null) {
		if (interval.upper === undefined) return false;
		const cmp = KeyCodec.compare(interval.upper.value, end);
		if (cmp > 0 || (cmp === 0 && interval.upper.inclusive)) return false;
	}
	return true;
}

/** Never transient: the key reached a partition that can neither own nor route it. */
function misrouted(operation: string, reason: string): FokosRoutingError {
	return new FokosRoutingError(SHARDING_ROUTING_CODES.partition_misrouted, {
		message: "mis-routed key this partition can neither own nor route",
		attributes: { operation, reason },
	});
}

function contextMismatch(attributes: Record<string, unknown>): FokosInternalError {
	return new FokosInternalError(SHARDING_INTERNAL_CODES.partition_context_mismatch, { message: "partition context mismatch", attributes });
}

/** The range a partition owns, rendered for a log line. KeyBytes never appear as bare Uint8Array. */
function rangeForLog(identity: FokosPartitionIdentity | undefined): Record<string, unknown> | undefined {
	const range = identity?.range;
	if (!range) return undefined;
	return {
		hashKey: KeyCodec.keyForLog(range.hashKey),
		start: range.start === null ? null : KeyCodec.keyForLog(range.start),
		end: range.end === null ? null : KeyCodec.keyForLog(range.end),
		depth: range.depth,
	};
}
