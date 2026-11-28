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
import { FokosError, FokosInternalError, FokosRoutingError, FokosUnavailableError } from "../shared/errors.js";
import invariant from "../shared/invariant.js";
import { DESTROY_ABORT_SENTINEL } from "../shared/cf-utils.js";
import { throttleTrailing } from "../shared/tsutils.js";
import { AddResult } from "../shared/bloom-filter.js";
import { attachRouting, envelope, RouteCollector, routedError } from "./envelope.js";
import { SHARDING_INTERNAL_CODES, SHARDING_ROUTING_CODES, SHARDING_UNAVAILABLE_CODES } from "./errors.js";
import {
	asRoutingHash,
	compareRoutingHashes,
	RoutingHashDigitCursor,
	routingHashInterval,
	routingHashDigits,
	routingHashOf,
	defaultRoutingHashSeedResolver,
	inRoutingHashInterval,
	type RoutingHash,
	type RoutingHashSeedResolver,
} from "./hash-primitives.js";
import { HashTopology } from "./hash-topology.js";
import { KeyCodec, type FokosCompositeKey, type KeyBytes } from "./key-codec.js";
import { PartialRangeTopology } from "./partial-range-topology.js";
import {
	identityDepth,
	isRangePartition,
	partitionIdentityFrom,
	PartitionIdHelper,
	resolveDescendantHashPartitionContext,
	resolveRangePartitionContext,
} from "./partition-id.js";
import { isStrictSubSlice, planRangeFrontier, projectOnHashKey, startCmp, type FrontierBase, type PlannedVisit } from "./range-frontier.js";
import { RepartitionSource, RepartitionTarget, type RepartitionCommonDeps } from "./repartition-flow.js";
import { storagePositionOf } from "./storage-position.js";
import {
	compositeInInterval,
	hashStorageInterval,
	rangeStorageInterval,
	sliceIncludesHashKey,
	sliceIncludesPosition,
	type FokosSlice,
} from "./repartition-slice.js";
import type {
	FokosExecuteLocalRequest,
	FokosInitRequest,
	FokosPrepareDestroyRequest,
	FokosPromotionsPage,
	FokosPromotionsRequest,
	FokosRequestPromotionRequest,
	FokosShardingAction,
	FokosShardingActions,
	FokosShardingRpc,
	FokosShardingRpcRequest,
	FokosStatusPage,
	FokosStatusRequest,
	RouteKey,
} from "./repartition-types.js";
import {
	refOf,
	structurallyEqual,
	topologiesEqual,
	validatePolicyVersion,
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
	FokosGroupFailure,
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
	FokosRepartitionPlan,
	FokosRequestPromotionResult,
	FokosRouteNode,
	FokosRuntimeOptions,
	FokosServedRole,
	FokosShardingHooks,
	FokosSignals,
	FokosStorageInterval,
	FokosStorageKey,
	FokosStoragePosition,
} from "./runtime-types.js";
import { FokosScheduler, INIT_RETRY_ALARM_MS } from "./scheduler.js";
import { systemPlatform, type FokosPlatform } from "../shared/platform.js";
import { jitterBackoff } from "durable-utils/retries";
import { PROMOTION_BLOOM_MAX_BYTES, resolveRuntimeConfig, type FokosRuntimeConfig } from "./runtime-config.js";
import { FokosShardingStore, type LearnedRangeSlice, type RepartitionState } from "./sharding-store.js";
import { cursorFallsInChild, rangeIntersects, type SkInterval } from "./sk-interval.js";
import { cachedHashTopology, cutTopologyHint, type TopologyEntry } from "./topology-cache.js";
import { findLeaf, TOPOLOGY_EVENT_TYPE, type FokosStoredTopologyEvent, type HashTopologyView } from "./topology-codec.js";
import { topologyEventToJson } from "./topology-encode.js";
import type { RangeAncestorInfo } from "./types.js";

const NO_SORT_KEY = KeyCodec.encodeOptional(undefined);
const LOCAL: OwnedResolution = { kind: "local" };
/** The in-memory step of an identity check that changed nothing. */
const NO_COMMIT = (): void => {};

/**
 * The operation name of each control-plane action, in `#guard` and in error attributes. The names are
 * constants, because `#guard` compares them on every request. A host operation cannot have one of
 * them, because the runtime refuses a host operation name with the `fokos` prefix.
 */
const RPC_OPERATION_NAMES: { readonly [A in FokosShardingAction]: `fokosRpc.${A}` } = {
	init: "fokosRpc.init",
	startImport: "fokosRpc.startImport",
	migrationPull: "fokosRpc.migrationPull",
	migrationAck: "fokosRpc.migrationAck",
	executeLocal: "fokosRpc.executeLocal",
	requestPromotion: "fokosRpc.requestPromotion",
	status: "fokosRpc.status",
	promotions: "fokosRpc.promotions",
	prepareDestroy: "fokosRpc.prepareDestroy",
	destroy: "fokosRpc.destroy",
};

/** The names of the built-in jobs. They run first, in this order. */
const BUILTIN_JOBS = ["target_import", "target_ack", "source_repartition", "source_cleanup", "admin_outbox"] as const;

/** The most outbox rows that one step of the `admin_outbox` job sends. */
const ADMIN_OUTBOX_BATCH = 16;
const ADMIN_OUTBOX_TIMEOUT_MS = 5_000;
const ADMIN_OUTBOX_RETRY_BASE_MS = 1_000;
const ADMIN_OUTBOX_RETRY_MAX_MS = 5 * 60_000;

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
			/** The topology cache entry and the bitmap position of the hash target, when the topology chose it. */
			topologyHint?: { entry: TopologyEntry<HashTopologyView>; pos: number };
			/** The learned slice a range target came from, or null for the durable child or root. */
			learned: LearnedRangeSlice | null;
	  }
	| { kind: "out_of_range" };

type RemoteResolution = Extract<Resolution, { kind: "remote" }>;
type OwnedResolution = Exclude<Resolution, { kind: "out_of_range" }>;

/** The caches a resolution reads. The hash arena has no switch: each resolution reads it. */
type ResolveOptions = {
	/** Read the promotion Bloom cache. Each dispatch shape does on its first attempt. */
	bloom: boolean;
	/** Jump to the deepest learned range slice. Each dispatch shape does. */
	learnedRange: boolean;
};

/** The durable facts and the hash arena only: `owns()` and the forward of a promotion request. */
const EXACT: ResolveOptions = { bloom: false, learnedRange: false };
/** The first attempt of each dispatch shape, and `resolveOwner`. */
const HINTED: ResolveOptions = { bloom: true, learnedRange: true };
/**
 * Each retry of a group or single-owner request. The hash arena and the learned range slices give the
 * same target to two keys of one owner, so one owner gets its keys in one sub-request. A Bloom hit
 * answers for one hash key only, and it can send the keys of one owner to two targets.
 */
const HINTED_NO_BLOOM: ResolveOptions = { bloom: false, learnedRange: true };

/**
 * The router facts of this partition, read once for one synchronous pass over many keys. No other
 * request runs until the pass yields, so one read gives the same answer as one read for each key. Do
 * not keep a view across an `await`.
 */
type RouterView = {
	router: boolean;
	/** The direct range children. Empty on a hash partition, which routes with the hash arena. */
	rangeChildren: FokosChild[];
};

const NOT_ROUTER: RouterView = { router: false, rangeChildren: [] };

/**
 * One element of the footprint of a request, after normalization. Every shape gives its footprint in
 * this form, so the import gate, the admission, the planner, and the learning have one input.
 */
type NormalizedSpan = PointSpan | IntervalSpan;
/** A span with one owner: an item that a request names, or a position that names no item. */
type PointSpan =
	| { kind: "item"; key: RouteKey; data: unknown; position: FokosStoragePosition }
	| { kind: "position"; position: FokosStoragePosition };
/** The interval of a `range` request. `position` is the start of its hash key, `(routingHash, hk)`. */
type IntervalSpan = { kind: "interval"; range: FokosRangeInput; position: FokosStoragePosition };

/** The point spans of a request that resolved to one remote target. They share one resolution. */
type RemoteVisit = { resolution: RemoteResolution; spans: PointSpan[] };
/** The point spans of a request by target, as `#planPoints` builds them. `remote` is keyed by partition ID. */
type PointPlan = {
	local: PointSpan[];
	remote: Map<string, RemoteVisit>;
	/** The ownership epoch when the plan was made. A local run checks it (`#assertEpoch`). */
	epoch: number;
};
/** The spans of one routingHash that have a Bloom hit, each with its own resolution. */
type BloomRoutingHash = { routingHash: RoutingHash; spans: Array<{ span: PointSpan; resolution: RemoteResolution }> };
/** Where the spans of a `one_owner` request go. `many` is a set that is local and remote, or on two remote targets. */
type OneOwnerResolution = { kind: "local"; epoch: number } | { kind: "remote"; resolution: RemoteResolution } | { kind: "many" };
type FanOutParts = Array<FokosGroupPart<unknown, unknown> | FokosGroupFailure<unknown>>;
/** The parts of a `fan_out` run, and the failures that the failure policy reports. */
type FanOutRun = { parts: FanOutParts; localFailure?: { request: unknown; error: unknown }; remoteFailures: unknown[] };

/**
 * The facts of a remote target that the miss ladder reads: which step of the resolution chose it, and
 * the cache hint that it came from. A point resolution and a planned range visit both give one.
 */
type MissHint = Pick<RemoteResolution, "via" | "learned" | "relDepth" | "topologyHint">;

type AnyOperation = FokosOperation<unknown, unknown>;

/** One planned range visit, and the hint of its target for the miss ladder. */
type RangePlan = { planned: PlannedVisit; hint: MissHint };

/** The execution kinds. Each shape maps to one of them, in `#compile`. */
type ExecutionKind = "one_owner" | "fan_out" | "walk" | "local";

/** The rules of each execution kind, checked once for each operation in the constructor. */
const EXECUTION_RULES: { readonly [K in ExecutionKind]: { readonly readSource: boolean } } = {
	one_owner: { readSource: true },
	fan_out: { readSource: true },
	walk: { readSource: true },
	local: { readSource: false },
};

/** One registered operation, compiled once in the constructor. */
type CompiledOperation = {
	descriptor: AnyOperation;
	execution: ExecutionKind;
	/** `one_owner` only: the answer when the spans have more than one owner. Absent: the spans cannot have more than one. */
	onManyOwners?: { answer: unknown };
	/**
	 * A retry after a cache miss can use the Bloom step again. Only a request of one point keeps it: a
	 * Bloom hit answers for one routingHash, so on a retry it can split the spans of one owner.
	 */
	bloomOnRetry: boolean;
};

/**
 * Where an execution runs. `owned`: on this partition, for its own scope. `readThrough`: on an import
 * source, for the caller slice of its target. A read-through admits nothing, runs no `beforeForward`,
 * applies no signal, and plans each span inside the caller slice.
 */
type ExecutionScope = { kind: "owned" } | { kind: "readThrough"; slice: FokosSlice };
const OWNED: ExecutionScope = { kind: "owned" };

export type FokosRuntimeConstructorOptions<TPolicy, Ops extends FokosOperationSpec> = FokosRuntimeOptions<TPolicy> & {
	ctx: DurableObjectState;
	hooks: FokosShardingHooks<TPolicy>;
	operations: FokosOperations<Ops>;
};

export class FokosShardingRuntime<TPolicy, Ops extends FokosOperationSpec> implements FokosShardingRpc {
	readonly #ctx: DurableObjectState;
	/** The id of this instance as a string. The id never changes, so it is made one time. */
	readonly #actorId: string;
	readonly #stub: FokosRuntimeOptions<TPolicy>["stub"];
	readonly #hooks: FokosShardingHooks<TPolicy>;
	readonly #ops: Record<string, AnyOperation>;
	readonly #compiled: Record<string, CompiledOperation>;
	readonly #store: FokosShardingStore;
	readonly #source: RepartitionSource;
	readonly #target: RepartitionTarget;
	readonly #scheduler: FokosScheduler;
	readonly #configOverrides: FokosRuntimeOptions<TPolicy>["config"];
	readonly #adminOutbox: FokosRuntimeOptions<TPolicy>["adminOutbox"];
	readonly #topologyCache: FokosRuntimeOptions<TPolicy>["topologyCache"];
	readonly #hostInitError: FokosRuntimeOptions<TPolicy>["hostInitError"];
	readonly #routingHashSeedResolver: RoutingHashSeedResolver;
	readonly #platform: FokosPlatform;

	/**
	 * The error of the storage step of the constructor, or of a config read. While it stands,
	 * `#guard` answers `partition_init_failed` and `runDueWork` re-arms at `INIT_RETRY_ALARM_MS`.
	 */
	#initError?: unknown;
	/** The immutable identity, from `__fokos/identity`. Absent until the first request or the `init` action. */
	#identity?: FokosPartitionIdentity;
	/** The mutable part of the last route context this partition received, from `__fokos/policy`. */
	#stored?: FokosStoredPolicy<TPolicy>;
	#routeCtx?: FokosRouteContext<TPolicy>;
	/** The bounded ancestor boundaries of a range partition, without its own slice. Empty otherwise. */
	#rangeAncestors: RangeAncestorInfo[] = [];
	#hashArena: HashTopology | null = null;
	#bloom: PartialRangeTopology | null = null;

	/** Writes the Bloom filter at most once per `promotionBloomFlushMs`, in a timer task of its own. */
	readonly #bloomFlushThrottled: ReturnType<typeof throttleTrailing>;
	/** The plan of every visit `rangeVisits` returned, by the visit object, so `forwardRangeVisit` can fall back. */
	readonly #plans = new WeakMap<FokosRangeVisit, RangePlan>();
	/**
	 * Counts the cutovers of this partition in this isolate. A cutover changes the owner of some keys, so
	 * a local run checks that the count did not change after its plan (`#assertEpoch`).
	 */
	#ownershipEpoch = 0;

	constructor(opts: FokosRuntimeConstructorOptions<TPolicy, Ops>) {
		this.#ctx = opts.ctx;
		this.#actorId = opts.ctx.id.toString();
		this.#stub = opts.stub;
		this.#hooks = opts.hooks;
		this.#ops = opts.operations as Record<string, AnyOperation>;
		this.#compiled = this.#compileOperations();
		this.#configOverrides = opts.config;
		this.#adminOutbox = opts.adminOutbox;
		this.#topologyCache = opts.topologyCache;
		this.#hostInitError = opts.hostInitError;
		this.#routingHashSeedResolver = opts.routingHashSeedResolver ?? defaultRoutingHashSeedResolver;
		this.#platform = opts.platform ?? systemPlatform;
		this.#bloomFlushThrottled = throttleTrailing(this.#platform, () => this.#flushBloom());
		// The config is validated at its first read, not here: a throw degrades the object instead of
		// bricking the constructor.
		this.#store = new FokosShardingStore(opts.ctx.storage, { rangeHierarchyMaxRows: () => this.#config().rangeHierarchyMaxRows });

		const deps: RepartitionCommonDeps = {
			getPeer: (ref) => this.#peer(ref),
			hooks: this.#hooks as FokosShardingHooks<unknown>,
			identity: () => ({ ctx: this.routeContext(), identity: this.identity() }),
			scheduleWork: () => this.#scheduler.wake(),
			logParams: () => this.#logParams(),
			config: () => this.#config(),
			routingHashSeedResolver: this.#routingHashSeedResolver,
			platform: this.#platform,
			onCompleted: (plan, completedAt) => this.#recordCompletion(plan, completedAt),
			onCutover: () => {
				this.#ownershipEpoch++;
			},
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
			platform: this.#platform,
			store: this.#store,
			fallbackAlarmMs: () => this.#config().fallbackAlarmMs,
			fastPathDelayMs: () => this.#config().fastPathDelayMs,
			isFenced: () => this.#store.isDestroying(),
			lifecycle: () => this.lifecycle(),
			jobs: () => [...this.#builtinJobs(), ...(this.#hooks.jobs ?? [])],
			logParams: () => this.#logParams(),
		});

		void opts.ctx.blockConcurrencyWhile(async () => {
			// The sharding store migrates first, before the host runs its own migrations.
			try {
				this.#store.runMigrations();
				const identity = this.#store.getIdentity();
				const stored = this.#store.getPolicy<TPolicy>();
				if (identity && stored) {
					this.#setIdentity(identity, stored);
					// The identity stays in memory, so the `status` and destroy actions still work.
					const seed = this.#routingHashSeedResolver(identity.topology.shardGroup);
					if (identity.routingHashSeed !== seed) {
						throw contextMismatch({ doName: identity.ref.doName, routingHashSeed: identity.routingHashSeed, expected: seed });
					}
				}
				const bloom = this.#store.getPromotionBloom();
				if (bloom) {
					this.#bloom = PartialRangeTopology.fromSnapshot(bloom);
				}
			} catch (error) {
				this.#initError = error;
				console.error({
					actorId: this.#actorId,
					actorName: this.#ctx.id.name,
					message: "fokos/runtime: the initialization of the partition failed.",
					error: String(error),
					errorProps: error,
				});
			}
		});
	}

	// ═══ dispatch ═══════════════════════════════════════════════════════════

	async dispatch<K extends keyof Ops & string>(
		op: K,
		routeCtx: FokosRouteContext<TPolicy>,
		req: Ops[K]["req"],
	): Promise<FokosEnvelope<Ops[K]["res"]>> {
		const compiled = this.#compiled[op];
		invariant(compiled, () => `fokos/runtime: operation ${op} is not registered`);
		let collector: RouteCollector | undefined;
		return await this.#guard(
			op,
			async () => {
				this.#ensureIdentity(routeCtx)();
				collector = new RouteCollector();
				return (await this.#dispatch(op, compiled, req, collector)) as FokosEnvelope<Ops[K]["res"]>;
			},
			() => collector,
		);
	}

	async #dispatch(op: string, compiled: CompiledOperation, req: unknown, collector: RouteCollector): Promise<FokosEnvelope<unknown>> {
		const { descriptor } = compiled;
		if (descriptor.shape === "local") {
			if (descriptor.whileMigrating === "throw" && this.#target.isImporting()) {
				// The same gate the other shapes take, under the name of this operation: the request nudges
				// the import on before it is refused.
				await this.#nudgeImport();
				throw new FokosUnavailableError(SHARDING_UNAVAILABLE_CODES.partition_migrating, {
					message: "partition split in progress, please retry later",
					attributes: { operation: op },
				});
			}
			const { call, signals } = this.#localCall();
			const value = await descriptor.local(req, call);
			await this.#applySignals(signals);
			return envelope(value, collector.build());
		}

		const spans = this.#spansOf(descriptor, req);
		if (this.#target.isImporting()) {
			return await this.#whileImporting(op, compiled, spans, req, collector);
		}
		const value = await this.#execute(op, compiled, spans, req, collector, OWNED);
		return envelope(value, collector.build());
	}

	/**
	 * Runs the executor of the operation. This is the only place that selects code by the execution
	 * kind: the import gate, the planner, the forward, the miss ladder, and the learning are the same
	 * for each kind.
	 */
	async #execute(
		op: string,
		compiled: CompiledOperation,
		spans: readonly NormalizedSpan[],
		req: unknown,
		collector: RouteCollector,
		scope: ExecutionScope,
	): Promise<unknown> {
		switch (compiled.execution) {
			case "one_owner":
				return await this.#runOneOwner(op, compiled, pointSpans(spans), req, collector, scope);
			case "fan_out":
				return await this.#runFanOut(op, compiled, pointSpans(spans), req, collector, scope);
			case "walk":
				invariant(scope.kind === "owned", "fokos/runtime: a walk reads through with its own gate");
				return await this.#runWalk(op, compiled, intervalSpan(spans), req, collector);
			case "local":
				return invariant(false, "fokos/runtime: a local operation has no executor");
		}
	}

	/**
	 * The footprint of a request as spans. A `RouteKey` and a position each become one point span, and
	 * a range request becomes one interval span.
	 */
	#spansOf(descriptor: AnyOperation, req: unknown): NormalizedSpan[] {
		switch (descriptor.shape) {
			case "point": {
				const key = descriptor.key(req);
				const position = this.#positionOf(key);
				return ["routingHash" in key ? { kind: "position", position } : { kind: "item", key, data: undefined, position }];
			}
			case "group":
				return descriptor.items(req).map(({ key, item }) => ({ kind: "item", key, data: item, position: this.#positionOf(key) }));
			case "single_owner":
				return descriptor.items(req).map(({ key }) => ({ kind: "item", key, data: undefined, position: this.#positionOf(key) }));
			case "range": {
				const range = descriptor.range(req);
				return [{ kind: "interval", range, position: this.#positionOf({ hashKey: range.hashKey, sortKey: NO_SORT_KEY }) }];
			}
			case "local":
				return [];
		}
	}

	/**
	 * Starts a pass soon when the import is due, and restores the fallback alarm. While the import waits
	 * for a retry, a pass can do no import work: the pass that deferred the import armed the alarm at
	 * the retry time, and the fallback covers a lost alarm. So a request in that time starts no pass.
	 */
	async #nudgeImport(): Promise<void> {
		const now = this.#platform.now();
		if ((this.#target.importRecord()?.nextAttemptAt ?? now) <= now) {
			this.#scheduler.wake();
		}
		await this.#scheduler.ensureAlarmAtMost(now + this.#config().fallbackAlarmMs);
	}

	/**
	 * The lifecycle gate. An incomplete target holds only some of the rows of its slice, so a write
	 * cannot apply and a read cannot be answered locally. The request also asks for one more import
	 * step and restores the fallback alarm, so the partition makes progress even when no start
	 * notification arrived.
	 */
	async #whileImporting(
		op: string,
		compiled: CompiledOperation,
		spans: readonly NormalizedSpan[],
		req: unknown,
		collector: RouteCollector,
	): Promise<FokosEnvelope<unknown>> {
		await this.#nudgeImport();
		const { descriptor } = compiled;
		// `dispatch` gates a `local` operation with `whileMigrating: "throw"` before this point and runs
		// every other `local` operation without the gate, so this gate never sees one.
		invariant(descriptor.shape !== "local", "fokos/runtime: a local operation cannot reach the import gate");
		const record = this.#target.importRecord();
		invariant(record, "fokos/runtime: importing without an import record");
		if (descriptor.whileMigrating === "throw") {
			// `importState` tells a sender that followed its Bloom filter here that this partition has no
			// page yet. That sender resolves the key again without the filter (see `#missLadder`).
			throw new FokosUnavailableError(SHARDING_UNAVAILABLE_CODES.partition_migrating, {
				message: "partition split in progress, please retry later",
				attributes: { operation: op, importState: record.state },
			});
		}

		// Ownership only, never a forward: a span this partition cannot own is a routing defect, and a
		// span it owns is answered by the source for as long as the import runs.
		for (const span of spans) {
			if (span.kind === "interval") {
				this.#assertCanOwnRange(op, span.range);
			} else if (!this.#ownsByTopology(span.position)) {
				throw misrouted(op, "key outside this partition");
			}
		}
		// This partition owns the scope and its source executes for it: both facts reach the caller, and
		// the source's list travels unchanged. A hash target writes its own depth on the range nodes the
		// source reached through a promotion: the target owns the promoted key, so its depth is the one a
		// router above must learn, not the shallower depth of the source that read on its behalf.
		collector.add(this.#selfNode("read_through"));
		collector.countForward();
		let result: FokosEnvelope<unknown>;
		try {
			result = await this.#peer(record.source).fokosRpc("executeLocal", {
				op,
				repartitionId: record.repartitionId,
				caller: this.identity().ref,
				request: req,
			});
		} catch (e) {
			// As in `#forwardTo`: the error carries the routing of the source and of this hop, and the
			// partition that raised the error stays at the head of the list.
			const routed = routedError(e);
			if (routed) {
				const stamp = this.#hashDepthStampForRangeNodes();
				const raiser = routed.routing.servedBy[0];
				collector.mergeForwarded(routed.routing, stamp);
				if (raiser) {
					collector.addRaiser(stamp ? stamp(raiser) : raiser);
				}
				routed.routing = collector.build();
			}
			throw e;
		}
		this.#learn(result.routing.servedBy, routingHashesOf(spans));
		collector.mergeForwarded(result.routing, this.#hashDepthStampForRangeNodes());
		return envelope(result.value, collector.build());
	}

	// ═══ the one_owner executor ═════════════════════════════════════════════

	/**
	 * Runs the whole request on the one owner of its spans: the `point` and the `single_owner` shapes.
	 * When the spans have more than one owner, the answer is `onManyOwners`.
	 */
	async #runOneOwner(
		op: string,
		compiled: CompiledOperation,
		spans: readonly PointSpan[],
		req: unknown,
		collector: RouteCollector,
		scope: ExecutionScope,
	): Promise<unknown> {
		const descriptor = routedDescriptor(compiled);
		const owner = this.#oneOwner(op, spans, HINTED, scope);
		if (owner.kind === "local") {
			this.#admitIn(scope, op, descriptor, spans);
			const before = this.#beforeForwardIn(scope, descriptor, req);
			const { value, signals } = await this.#runLocalScope(descriptor, req, collector, scope, owner.epoch);
			await this.#applySignalsIn(scope, [...before, ...signals]);
			return value;
		}
		const before = this.#beforeForwardIn(scope, descriptor, req);
		if (owner.kind === "many") {
			// A value and not an error: on a split shard group this is the ordinary answer for such a
			// set, and it carries no side effects at any depth of a forwarding chain.
			await this.#applySignalsIn(scope, before);
			return manyOwnersAnswer(op, compiled);
		}
		const forwarded = this.#forwardOneOwner(op, compiled, spans, req, owner.resolution, collector, scope, 0);
		forwarded.catch(() => {});
		await this.#applySignalsIn(scope, before);
		return await forwarded;
	}

	/**
	 * Where the spans of a `one_owner` request go. A Bloom hit answers for one routingHash, so it can make
	 * the spans of one owner look like the spans of two partitions. Such a set resolves again with the
	 * Bloom step off before the answer is `many`.
	 */
	#oneOwner(op: string, spans: readonly PointSpan[], opts: ResolveOptions, scope: ExecutionScope): OneOwnerResolution {
		const plan = this.#plan(op, spans, opts, scope);
		if (plan.remote.size === 0) {
			return { kind: "local", epoch: plan.epoch };
		}
		const [first] = plan.remote.values();
		if (plan.remote.size === 1 && plan.local.length === 0) {
			return { kind: "remote", resolution: first.resolution };
		}
		if (opts.bloom && [...plan.remote.values()].some((visit) => isBloomHit(visit.resolution))) {
			return this.#oneOwner(op, spans, HINTED_NO_BLOOM, scope);
		}
		return { kind: "many" };
	}

	/**
	 * Forwards a `one_owner` request and falls back after a cache miss. After the fallback the spans can
	 * be local, on one remote target, or on more than one partition. A `one_owner` request never fans
	 * out, and every error that starts a fallback comes from a check before the handler, so no handler
	 * ran on the chain.
	 */
	async #forwardOneOwner(
		op: string,
		compiled: CompiledOperation,
		spans: readonly PointSpan[],
		req: unknown,
		resolution: RemoteResolution,
		collector: RouteCollector,
		scope: ExecutionScope,
		retries: number,
	): Promise<unknown> {
		const answer = await this.#forwardVisit(op, compiled, { resolution, spans: [...spans] }, req, collector, retries);
		if (answer.kind === "answered") {
			return answer.value;
		}
		const owner = this.#oneOwner(op, spans, answer.opts, scope);
		if (owner.kind === "many") {
			return manyOwnersAnswer(op, compiled);
		}
		if (owner.kind === "local") {
			const descriptor = routedDescriptor(compiled);
			this.#admitIn(scope, op, descriptor, spans);
			const { value, signals } = await this.#runLocalScope(descriptor, req, collector, scope, owner.epoch);
			await this.#applySignalsIn(scope, signals);
			return value;
		}
		return await this.#forwardOneOwner(op, compiled, spans, req, owner.resolution, collector, scope, retries + 1);
	}

	// ═══ the point planner, the forward, and the miss ladder ════════════════

	/** The plan of the point spans of a request, in the scope of the execution. */
	#plan(op: string, spans: readonly PointSpan[], opts: ResolveOptions, scope: ExecutionScope): PointPlan {
		return scope.kind === "owned" ? this.#planPoints(op, spans, opts) : this.#planInCallerSlice(op, spans, scope.slice);
	}

	/**
	 * Resolves each point span, and groups the remote spans by their target. When a span has a Bloom
	 * hit, the speculation guard runs before the spans are grouped.
	 */
	#planPoints(op: string, spans: readonly PointSpan[], opts: ResolveOptions): PointPlan {
		const plan: PointPlan = { local: [], remote: new Map(), epoch: this.#ownershipEpoch };
		// The spans with a Bloom hit wait for the guard, by routingHash. The filter answers for a whole
		// routingHash, so the spans of one routingHash always go to one place.
		let bloomHits: Map<string, BloomRoutingHash> | undefined;
		const view = this.#routerView();
		for (const span of spans) {
			const resolution = this.#resolvePosition(span.position, opts, view);
			if (resolution.kind === "out_of_range") {
				throw misrouted(op, span.kind === "item" ? "key outside this partition" : "position outside this partition");
			}
			if (!isBloomHit(resolution)) {
				addToPlan(plan, span, resolution);
				continue;
			}
			bloomHits ??= new Map();
			const id = span.position.routingHash.toHex();
			let hit = bloomHits.get(id);
			if (hit === undefined) {
				hit = { routingHash: span.position.routingHash, spans: [] };
				bloomHits.set(id, hit);
			}
			hit.spans.push({ span, resolution });
		}
		if (bloomHits) {
			this.#guardBloomHits(bloomHits, plan, view.router);
		}
		return plan;
	}

	/**
	 * The plan of a read-through on the source. Each span must be inside the caller slice. A promoted
	 * routingHash of a `hash_child` caller slice goes to its range tree, because the rows of this
	 * partition are stale copies or already collected. A range or promoted-routingHash caller slice is
	 * itself inside a range tree, so only a hash-child caller can reach one. Every other span runs here.
	 */
	#planInCallerSlice(op: string, spans: readonly PointSpan[], slice: FokosSlice): PointPlan {
		const topology = this.identity().topology;
		const plan: PointPlan = { local: [], remote: new Map(), epoch: this.#ownershipEpoch };
		for (const span of spans) {
			const { routingHash } = span.position;
			if (!sliceIncludesPosition(slice, routingHash, span.position, topology)) {
				throw misrouted(op, "key outside the caller slice");
			}
			const promoted = slice.kind === "hash_child" && this.#store.hasTerminalRouteOverride(routingHash);
			addToPlan(plan, span, promoted ? this.#rangeOwner(routingHash, span.position, "override", false, true) : LOCAL);
		}
		return plan;
	}

	/**
	 * The speculation guard of a point plan. It keeps a Bloom hit only when the hit cannot split the
	 * spans of one owner over two sub-requests.
	 *
	 * Why. The owner of a key must receive all its keys of this request in one sub-request: `txCommit`
	 * compares its keys with the lock rows of the transaction that it owns, and throws
	 * `commit_keyset_mismatch` when the two sets differ. A Bloom hit is a guess for one routingHash R,
	 * and it sends the spans of R to a range partition of R. When the guess is wrong, the range partition
	 * refuses, and the spans of R fall back to the exact target of R: the partition that owns R without
	 * the filter. When another routingHash also goes to that exact target, the owner then receives two
	 * sub-requests.
	 *
	 * How. The input is `plan`, the spans without a Bloom hit, and `bloomHits`, the spans of each
	 * routingHash R with a Bloom hit.
	 * 1. For each R, find its exact target with `#hashTopologyOwner`. R has no cut-over route override
	 *    here, because a Bloom hit comes after the override check. So the exact target is the topology
	 *    owner, `local` or a hash descendant, and the guard reads no route override again.
	 * 2. Count the Bloom routingHashes at each exact target.
	 * 3. The exact target of R is shared when another Bloom routingHash has the same exact target, or
	 *    when `plan` already has a span there. A span of `plan` belongs to another routingHash, because
	 *    the filter answers for the whole routingHash, so each span of R has a Bloom hit.
	 * 4. When the target is shared, each span of R goes to the exact target, and the owner gets all its
	 *    spans in one sub-request. Otherwise each span of R keeps its Bloom resolution. A later fallback
	 *    then reaches a partition that gets no other span of this request from this partition.
	 *
	 * The guard compares the exact target of R only with the targets that `plan` already holds. The
	 * resolution of a span without a Bloom hit is exact, or it names a range partition of its own
	 * routingHash. A range partition of another routingHash is never the exact target of R.
	 *
	 * Example. This hash leaf owns `alice` and `carol`. A commit has the keys (alice, a) and
	 * (carol, b). The filter gives a false positive for `alice`.
	 * - Without the guard, (alice, a) goes to the range root of `alice`, which does not exist, and
	 *   (carol, b) runs here. The fallback then runs (alice, a) here in a second call. The first call
	 *   has {b}, the lock rows have {a, b}, and the commit throws `commit_keyset_mismatch`.
	 * - With the guard, the exact target of `alice` is `local`, and `plan.local` holds (carol, b). The
	 *   target is shared, so (alice, a) runs here with (carol, b) in one call.
	 * - When the commit has only `alice` keys, no other routingHash shares `local`. The Bloom hit stays,
	 *   and a true positive saves the forwards to the range owner.
	 */
	#guardBloomHits(bloomHits: Map<string, BloomRoutingHash>, plan: PointPlan, router: boolean): void {
		const owners: Array<{ hit: BloomRoutingHash; owner: OwnedResolution; id: string }> = [];
		const routingHashesAt = new Map<string, number>();
		for (const hit of bloomHits.values()) {
			const owner = this.#hashTopologyOwner(hit.routingHash, router);
			const id = owner.kind === "local" ? "local" : owner.target.partitionId;
			owners.push({ hit, owner, id });
			routingHashesAt.set(id, (routingHashesAt.get(id) ?? 0) + 1);
		}
		for (const { hit, owner, id } of owners) {
			// An earlier routingHash of this loop adds spans at `id` only when two Bloom routingHashes
			// share `id`, and then the count already says that the target is shared.
			const shared = routingHashesAt.get(id)! > 1 || (id === "local" ? plan.local.length > 0 : plan.remote.has(id));
			for (const { span, resolution } of hit.spans) {
				addToPlan(plan, span, shared ? owner : resolution);
			}
		}
	}

	/**
	 * One forward of the point spans of one visit. On a cache miss it forgets the hint and the node of
	 * the target, and gives the executor the options to plan the spans of the visit again. The executor
	 * decides what the new plan means. Every error that starts a fallback comes before any handler ran
	 * on the target.
	 */
	async #forwardVisit(
		op: string,
		compiled: CompiledOperation,
		visit: RemoteVisit,
		req: unknown,
		collector: RouteCollector,
		retries: number,
	): Promise<{ kind: "answered"; value: unknown } | { kind: "missed"; opts: ResolveOptions }> {
		const { target } = visit.resolution;
		const routingHashes = routingHashesOf(visit.spans);
		try {
			return { kind: "answered", value: await this.#forwardTo(collector, target, op, req, routingHashes) };
		} catch (e) {
			const opts =
				retries < this.#config().maxForwardRetries
					? this.#missLadder(routingHashes[0], visit.resolution, e, routedDescriptor(compiled).readOnly === true, compiled.bloomOnRetry)
					: null;
			if (!opts) {
				throw e;
			}
			collector.forget(target.partitionId);
			return { kind: "missed", opts };
		}
	}

	/**
	 * The options to plan the spans of a failed forward again with, or null when the failure is not a
	 * cache miss. It forgets the hint that caused the miss, so the next plan cannot repeat it. The spans
	 * share the target of `hint`, thus they share its hint, and one forget covers all of them.
	 * `routingHash` is the routingHash of the first span. With `bloomOnRetry`, a retry can use the Bloom
	 * step again. Otherwise every retry turns the Bloom step off and keeps the learned range slices.
	 */
	#missLadder(routingHash: RoutingHash, hint: MissHint, e: unknown, readOnly: boolean, bloomOnRetry: boolean): ResolveOptions | null {
		const retry = (opts: ResolveOptions) => (bloomOnRetry ? opts : HINTED_NO_BLOOM);
		const notInitialized = FokosError.isCode(e, SHARDING_ROUTING_CODES.range_partition_not_initialized);
		const notCutOver = FokosError.isCode(e, SHARDING_UNAVAILABLE_CODES.repartition_not_cut_over);
		const awaitingData = isAwaitingData(e);
		if (hint.learned && notInitialized) {
			this.#store.deleteLearnedRangeSlice(routingHash, hint.learned);
			return retry({ bloom: hint.via === "bloom", learnedRange: true });
		}
		// A Bloom hit can name the range root of a key whose promotion has not cut over. The exact
		// resolution reads the route overrides, so it names the partition that owns the key now.
		if (hint.via === "bloom" && (notInitialized || awaitingData || (readOnly && notCutOver))) {
			return retry({ bloom: false, learnedRange: true });
		}
		if (hint.via === "hash" && (hint.relDepth ?? 1) > 1 && FokosError.isCode(e, SHARDING_ROUTING_CODES.hash_partition_not_initialized)) {
			// The hint that missed is the one at `relDepth`: the topology, the arena, or both.
			if (hint.topologyHint) {
				cutTopologyHint(hint.topologyHint.entry, hint.topologyHint.pos);
			}
			const arena = this.#arena();
			if (arena?.invalidate(routingHash, hint.relDepth!)) {
				this.#store.putHashArena(arena.toSnapshot());
			}
			return retry(HINTED);
		}
		return null;
	}

	// ═══ the fan_out executor ═══════════════════════════════════════════════

	/** Runs the `group` shape: one sub-request for each owner, then `merge`. */
	async #runFanOut(
		op: string,
		compiled: CompiledOperation,
		spans: readonly PointSpan[],
		req: unknown,
		collector: RouteCollector,
		scope: ExecutionScope,
	): Promise<unknown> {
		const descriptor = groupDescriptor(compiled);
		if (spans.length === 0) {
			this.#admitIn(scope, op, descriptor, []);
			const before = this.#beforeForwardIn(scope, descriptor, req);
			const { call, signals } = this.#localCall(sliceOf(scope));
			const value = await this.#runLocal(descriptor, req, call);
			await this.#applySignalsIn(scope, [...before, ...signals]);
			return value;
		}

		const plan = this.#plan(op, spans, HINTED, scope);
		const remote = [...plan.remote.values()];
		const collect = descriptor.failurePolicy === "collect";

		// Admission, `beforeForward`, the local work, and the start of every remote call run in the same
		// synchronous block as the plan above: an outbound RPC yields only at the first await of
		// `#runParts`, so a cutover cannot interleave between the ownership decision and the write.
		// Under `collect`, the local part admits its own spans, so a refusal fails only that part.
		if (plan.local.length > 0 && !collect) {
			this.#admitIn(scope, op, descriptor, plan.local);
		}
		const signals: FokosSignals[] = this.#beforeForwardIn(scope, descriptor, req);
		const run = await this.#runParts(op, compiled, req, plan, remote, collector, 0, signals, collect, scope);
		return this.#settleFanOut(op, descriptor, run, remote.length);
	}

	/**
	 * The answer of a `fan_out` operation after each part settled. Under `collect`, `merge` gets every
	 * part, the failed parts included. Otherwise a failure throws: the local part is the more specific
	 * answer, and every remote part has been attempted by now.
	 */
	#settleFanOut(op: string, descriptor: GroupDescriptor, run: FanOutRun, partCount: number): unknown {
		if (descriptor.failurePolicy === "collect") {
			return descriptor.merge(run.parts);
		}
		const { localFailure, remoteFailures } = run;
		if (localFailure) {
			throw localFailure.error;
		}
		if (remoteFailures.length > 0) {
			throw new FokosInternalError(SHARDING_INTERNAL_CODES.partition_fanout_failed, {
				message: "a remote group of the operation failed",
				cause: remoteFailures[0],
				attributes: {
					operation: op,
					failureCount: remoteFailures.length,
					groupCount: partCount,
					// `cause` is a non-enumerable own property, so it does not cross an RPC hop. The
					// caller still needs the failure it stands for, and `attributes` does cross.
					...causeAttributes(remoteFailures[0]),
				},
			});
		}
		// Only `collect` adds a failed part.
		return descriptor.merge(run.parts as Array<FokosGroupPart<unknown, unknown>>);
	}

	/**
	 * Runs the local part of a `fan_out` operation and starts each remote part, in the synchronous
	 * block of the caller, then settles them by the failure policy. With `admitLocal`, the local part
	 * admits its spans itself; otherwise the caller admitted them. `signals` holds the signals of the
	 * caller, which apply with the local part. A `fail_fast` failure throws at once. An `attempt_all`
	 * failure comes back after each part settled. A `collect` failure comes back as a failed part, after
	 * each part settled.
	 */
	async #runParts(
		op: string,
		compiled: CompiledOperation,
		req: unknown,
		plan: PointPlan,
		remote: RemoteVisit[],
		collector: RouteCollector,
		retries: number,
		signals: FokosSignals[],
		admitLocal: boolean,
		scope: ExecutionScope,
	): Promise<FanOutRun> {
		const descriptor = groupDescriptor(compiled);
		const { local } = plan;
		const collect = descriptor.failurePolicy === "collect";
		const settleAll = descriptor.failurePolicy !== "fail_fast";
		let localCall: { request: unknown; value: unknown; signals: FokosSignals[] } | undefined;
		// An `attempt_all` or `collect` operation runs every part before it reports a failure, and that
		// includes the case where the local part is the part that failed. The report waits until the
		// remote parts have settled, so a decided transaction still reaches every partition that holds
		// a piece of it.
		let localFailure: { request: unknown; error: unknown } | undefined;
		if (local.length > 0) {
			const { call, signals: own } = this.#localCall(sliceOf(scope));
			const request = descriptor.subRequest(req, local.map(dataOf));
			collector.add(this.#selfNode("executed"));
			try {
				if (admitLocal) {
					this.#admitIn(scope, op, descriptor, local);
				}
				this.#assertEpoch(plan.epoch);
				localCall = { request, value: this.#runLocal(descriptor, request, call), signals: own };
			} catch (error) {
				if (!settleAll) {
					throw error;
				}
				localFailure = { request, error };
			}
		}
		const remoteCalls = remote.map((visit) => {
			const promise = this.#forwardFanOutVisit(op, compiled, req, visit, collector, retries, scope);
			// A rejection that lands while the signals below run must not count as unhandled; it is
			// awaited right after.
			promise.catch(() => {});
			return promise;
		});

		if (localCall) {
			try {
				localCall.value = await localCall.value;
				signals.push(...localCall.signals);
			} catch (error) {
				if (!settleAll) {
					throw error;
				}
				localFailure = { request: localCall.request, error };
				localCall = undefined;
			}
		}
		await this.#applySignalsIn(scope, signals);

		// The merge over remote parts makes this partition a router of the request. A partition that also
		// ran the handler keeps its `executed` role.
		if (remoteCalls.length > 0) {
			collector.add(this.#selfNode("merged"));
		}
		const parts: FanOutParts = [];
		const remoteFailures: unknown[] = [];
		if (settleAll) {
			for (const [i, result] of (await Promise.allSettled(remoteCalls)).entries()) {
				if (result.status === "fulfilled") {
					parts.push(...result.value);
				} else if (collect) {
					// The part failed after its fallbacks. It is one failed part, with the sub-request that
					// its first forward sent.
					const visit = remote[i];
					const request = descriptor.subRequest(req, visit.spans.map(dataOf));
					parts.push({ target: visit.resolution.target, request, error: result.reason });
				} else {
					remoteFailures.push(result.reason);
				}
			}
		} else {
			parts.push(...(await Promise.all(remoteCalls)).flat());
		}
		if (localCall) {
			parts.push({ target: "local", request: localCall.request, result: localCall.value });
		} else if (collect && localFailure) {
			parts.push({ target: "local", request: localFailure.request, error: localFailure.error });
		}
		return { parts, localFailure, remoteFailures };
	}

	/**
	 * Forwards one remote part of a `fan_out` operation, and falls back after a cache miss: it plans the
	 * spans of the part again, and runs them where they resolve now. The answer holds one part for each
	 * forward that answered and one for a local run.
	 *
	 * A router below the target can apply one part before another part fails, so the fallback can send
	 * a key again to an owner that applied it. Each `group` operation is safe to repeat for each key.
	 */
	async #forwardFanOutVisit(
		op: string,
		compiled: CompiledOperation,
		req: unknown,
		visit: RemoteVisit,
		collector: RouteCollector,
		retries: number,
		scope: ExecutionScope,
	): Promise<FanOutParts> {
		const descriptor = groupDescriptor(compiled);
		const { target } = visit.resolution;
		const request = descriptor.subRequest(req, visit.spans.map(dataOf));
		const answer = await this.#forwardVisit(op, compiled, visit, request, collector, retries);
		if (answer.kind === "answered") {
			return [{ target, request, result: answer.value }];
		}
		// The plan, the admission, the local run, and the start of each remote part run in one synchronous
		// block, as in `#runFanOut`.
		const collect = descriptor.failurePolicy === "collect";
		const plan = this.#plan(op, visit.spans, answer.opts, scope);
		if (plan.local.length > 0 && !collect) {
			this.#admitIn(scope, op, descriptor, plan.local);
		}
		const { parts, localFailure, remoteFailures } = await this.#runParts(
			op,
			compiled,
			req,
			plan,
			[...plan.remote.values()],
			collector,
			retries + 1,
			[],
			collect,
			scope,
		);
		if (collect) {
			return parts;
		}
		if (localFailure) {
			throw localFailure.error;
		}
		if (remoteFailures.length > 0) {
			throw remoteFailures[0];
		}
		return parts;
	}

	// ═══ the walk executor ══════════════════════════════════════════════════

	/** Runs the `range` shape: the host walks the planned visits of the interval. */
	async #runWalk(op: string, compiled: CompiledOperation, span: IntervalSpan, req: unknown, collector: RouteCollector): Promise<unknown> {
		const descriptor = rangeDescriptor(compiled);
		const planned = this.#planRange(op, span.range, true);
		const visits = planned.map((p) => p.planned.visit);
		// The walk admits one time, before the first local run: at the start when the plan has a local
		// visit, or at a fallback that runs a visit here.
		let admitted = false;
		const admitOnce = () => {
			if (!admitted) {
				admitted = true;
				this.#admit(op, descriptor, [span]);
			}
		};
		if (visits.some((visit) => visit.target === "local")) {
			admitOnce();
		}
		const signals = this.#beforeForward(descriptor, req);
		const local = (r: unknown) => {
			const { call, signals: own } = this.#localCall();
			collector.add(this.#selfNode("executed"));
			const out = this.#runLocal(descriptor, r, call);
			signals.push(...own);
			return out;
		};
		const value = await descriptor.walk({
			request: req,
			visits,
			local,
			forward: async (visit, r) => {
				// A walk that forwards makes this partition a router of the request. A leaf that also ran
				// the handler keeps its `executed` role, because `executed` outranks `merged`.
				collector.add(this.#selfNode("merged"));
				return await this.#forwardRangeVisit(op, compiled, visit, r, collector, (fallback) => {
					admitOnce();
					return local(fallback);
				});
			},
		});
		await this.#applySignals(signals);
		return value;
	}

	// ═══ the primitive API ══════════════════════════════════════════════════

	identity(): FokosPartitionIdentity {
		if (!this.#identity) {
			throw this.#notInitialized();
		}
		return this.#identity;
	}

	/** The stored host policy: the live value that the last request updated. */
	policy(): TPolicy {
		if (!this.#stored) {
			throw this.#notInitialized();
		}
		return this.#stored.policy;
	}

	/** The stored route context of this partition: its identity plus the mutable part the last request left. */
	routeContext(): FokosRouteContext<TPolicy> {
		if (!this.#routeCtx) {
			throw this.#notInitialized();
		}
		return this.#routeCtx;
	}

	/**
	 * True once the `init` action or a first routed request gave this partition its identity, which the
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

	/**
	 * The mutable lifecycle facts, read from storage. Each field reads its storage only when a caller
	 * first reads it, and then keeps the answer for this result. So a check that reads 2 fields does 2
	 * reads, and checks that share one result read each fact one time. Callers that can run cold check
	 * `initialized()` first.
	 *
	 * Do not keep a result across an `await`: a field read after the `await` can come from a different
	 * state than a field read before it.
	 */
	lifecycle(): FokosLifecycle {
		const source = this.#source;
		const target = this.#target;
		const store = this.#store;
		// `undefined` means "not read yet". A read never gives `undefined`.
		let role: FokosLifecycle["role"] | undefined;
		let imported: FokosLifecycle["import"] | undefined;
		let activeRepartition: FokosLifecycle["activeRepartition"] | undefined;
		let destroying: boolean | undefined;
		return {
			get role() {
				role ??= source.routerRole() ? "router" : "owner";
				return role;
			},
			get import() {
				if (imported === undefined) {
					const record = target.importRecord();
					imported = record ? { state: record.state, source: record.source, slice: record.slice } : null;
				}
				return imported;
			},
			get activeRepartition() {
				if (activeRepartition === undefined) {
					const active = store.firstActiveRepartition();
					activeRepartition =
						active && (active.state === "queued" || active.state === "planned" || active.state === "cutover")
							? { id: active.id, kind: active.kind, state: active.state }
							: null;
				}
				return activeRepartition;
			},
			get destroying() {
				destroying ??= store.isDestroying();
				return destroying;
			},
		};
	}

	/**
	 * True when this partition owns the key now. It reads the topology and the route overrides only,
	 * never a cache, so a Bloom false positive cannot make a host sweep skip a key it owns.
	 */
	owns(key: RouteKey): boolean {
		return this.#resolve(key, EXACT).kind === "local";
	}

	/**
	 * `owns()` for many keys in one synchronous pass. It reads the router facts once. A router owns no
	 * key, and a range partition that is not a router owns every key of its interval, so neither reads
	 * storage for each key. A hash partition that is not a router reads the route override of each key.
	 * Do not keep a checker across an `await`.
	 */
	ownerCheck(): (key: RouteKey) => boolean {
		if (this.#source.routerRole()) {
			return () => false;
		}
		return (key) => this.#resolve(key, EXACT, NOT_ROUTER).kind === "local";
	}

	/**
	 * The routingHash of a hash key in this partition: the first primary-key column of its rows. The
	 * host treats it as opaque bytes, and must not keep it for a key of another call.
	 */
	routingHashForKey(hashKey: KeyBytes): RoutingHash {
		return this.#routingHash(hashKey);
	}

	/**
	 * The immutable scope of this partition in the storage order `(routingHash, hk, sk)`. A router keeps this
	 * interval, but it owns no rows in it.
	 */
	storageInterval(): FokosStorageInterval {
		const identity = this.identity();
		if (identity.kind === "hash") {
			return hashStorageInterval(identity.hash!.start, identity.hash!.end);
		}
		const { routingHash, start, end } = identity.range!;
		return rangeStorageInterval(routingHash, start, end);
	}

	/**
	 * The bounds of the rows that this partition owns, or null on a router. A moved routingHash inside the
	 * bounds stays in them, so a host must also use `owns` or `ownerCheck` before it changes a row.
	 */
	ownedStorageInterval(): FokosStorageInterval | null {
		return this.#source.routerRole() ? null : this.storageInterval();
	}

	/** The point-routing answer, caches included. A speculative remote owner is a hint, not a fact. */
	resolveOwner(key: RouteKey): FokosOwner {
		const resolution = this.#resolve(key, HINTED);
		return resolution.kind === "remote" ? { kind: "remote", target: resolution.target, speculative: resolution.speculative } : resolution;
	}

	/** This router's direct targets in `target_index` order. Empty on an owner. */
	children(): FokosChild[] {
		if (!this.#source.routerRole()) {
			return [];
		}
		return this.#splitChildren();
	}

	#splitChildren(): FokosChild[] {
		return this.#source.splitTargets().map((t) => ({
			ref: { partitionId: t.partitionId, doName: t.doName },
			start: t.slice.kind === "range" ? t.slice.start : null,
			end: t.slice.kind === "range" ? t.slice.end : null,
		}));
	}

	/** Reads the router facts once. A caller that resolves many keys in one synchronous pass passes the view to each `#resolve`. */
	#routerView(): RouterView {
		if (!this.#source.routerRole()) {
			return NOT_ROUTER;
		}
		return { router: true, rangeChildren: this.identity().kind === "range" ? this.#splitChildren() : [] };
	}

	/** A disjoint, ordered cover of one range request. The host walks it with `forwardRangeVisit`. */
	rangeVisits(input: FokosRangeInput): FokosRangeVisit[] {
		return this.#planRange("rangeVisits", input, true).map((p) => p.planned.visit);
	}

	/** Forwards one registered operation to one target and learns its routes. The envelope is that call's own. */
	async forward<K extends keyof Ops & string>(target: FokosPartitionRef, op: K, req: Ops[K]["req"]): Promise<FokosEnvelope<Ops[K]["res"]>> {
		const descriptor = this.#ops[op];
		invariant(descriptor && descriptor.shape !== "local", () => `fokos/runtime: ${op} cannot be forwarded`);
		const collector = new RouteCollector();
		const value = await this.#forwardTo(collector, target, op, req, routingHashesOf(this.#spansOf(descriptor, req)));
		return envelope(value as Ops[K]["res"], collector.build());
	}

	/** Forwards one planned range visit, including the speculative and learned-slice fallbacks. */
	async forwardRangeVisit<K extends keyof Ops & string>(
		visit: FokosRangeVisit,
		op: K,
		req: Ops[K]["req"],
	): Promise<FokosEnvelope<Ops[K]["res"]>> {
		const compiled = this.#compiled[op];
		invariant(compiled?.execution === "walk", () => `fokos/runtime: ${op} is not a range operation`);
		const descriptor = rangeDescriptor(compiled);
		const collector = new RouteCollector();
		const signals: FokosSignals[] = [];
		// A fallback that runs the visit here admits and signals as a planned local visit does.
		const value = await this.#forwardRangeVisit(op, compiled, visit, req, collector, (r) => {
			this.#admit(op, descriptor, this.#spansOf(descriptor, r));
			const { call, signals: own } = this.#localCall();
			collector.add(this.#selfNode("executed"));
			const out = this.#runLocal(descriptor, r, call);
			signals.push(...own);
			return out;
		});
		await this.#applySignals(signals);
		return envelope(value as Ops[K]["res"], collector.build());
	}

	// ═══ signals and jobs ═══════════════════════════════════════════════════

	/** Asks `hooks.evaluateSplit` now, as a local success that signals `evaluateSplit` does. */
	requestSplitEvaluation(): void {
		void this.#applySignals([{ evaluateSplit: true }]);
	}

	/**
	 * The state of the promotion that this partition holds for `hashKey`, or undefined if it holds none.
	 * A hash child holds the promotions it inherited from its parent too. One primary-key read.
	 */
	promotionState(hashKey: KeyBytes): RepartitionState | undefined {
		return this.#store.routeOverrideFor(this.#routingHash(hashKey))?.state;
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
		if (this.#initError !== undefined) {
			// The interval is the constant: the read of the config that can be the failure does not run.
			try {
				await this.#scheduler.ensureAlarmAtMost(this.#platform.now() + INIT_RETRY_ALARM_MS);
			} catch {}
			try {
				console.error({
					...this.#logParams(),
					message: "fokos/runtime: the initialization failed; the alarm retries it at the init interval.",
					error: String(this.#initError as Error),
					errorProps: this.#initError,
				});
			} catch {}
			return;
		}
		await this.#scheduler.runDueWork();
	}

	// ═══ the control-plane RPCs ═════════════════════════════════════════════

	/**
	 * The one control-plane RPC. The host delegates its `fokosRpc` method to it. The action selects
	 * the handler, and every action goes through the lifecycle gate of `#guard`:
	 *
	 * - `init`: creates this partition as the target of a repartition, or confirms an identical
	 *   earlier call. Only this action creates a range partition, and only this action tells a hash
	 *   child that it exists.
	 * - `startImport`: asks this target to start its import now, instead of at its own fallback alarm.
	 * - `migrationPull`: serves one bounded migration page to a target that is still catching up.
	 * - `migrationAck`: records that one target holds a complete copy of its slice.
	 * - `executeLocal`: serves one read for a repartition target that is still importing from this
	 *   partition (see `#executeLocal`).
	 * - `requestPromotion`: queues a key promotion on the partition that owns the key now (see
	 *   `#requestPromotionRpc`).
	 * - `status`: the identity of this partition and its split with the target links (see `#status`).
	 * - `promotions`: one bounded page of the key promotions of this partition.
	 * - `prepareDestroy`: fences this partition for destroy (see `#prepareDestroy`).
	 * - `destroy`: cancels the schedule, deletes all storage, and aborts the instance (see `#destroy`).
	 */
	async fokosRpc<A extends FokosShardingAction>(action: A, req: FokosShardingActions[A]["req"]): Promise<FokosShardingActions[A]["res"]> {
		const call = { action, req } as FokosShardingRpcRequest;
		let collector: RouteCollector | undefined;
		const result = await this.#guard(
			// A caller of another version can send an action that this version does not know.
			Object.hasOwn(RPC_OPERATION_NAMES, action) ? RPC_OPERATION_NAMES[action] : `fokosRpc.${String(action)}`,
			async (): Promise<FokosShardingActions[FokosShardingAction]["res"]> => {
				switch (call.action) {
					case "init":
						return await this.#target.initAsTarget(call.req);
					case "startImport":
						return await this.#target.startImport(call.req);
					case "migrationPull":
						return this.#source.servePage(call.req);
					case "migrationAck":
						return this.#source.acceptAck(call.req);
					case "executeLocal":
						collector = new RouteCollector();
						return await this.#executeLocal(call.req, collector);
					case "requestPromotion":
						return await this.#requestPromotionRpc(call.req);
					case "status":
						return this.#status(call.req);
					case "promotions":
						return this.#promotions(call.req);
					case "prepareDestroy":
						return await this.#prepareDestroy(call.req);
					case "destroy":
						return await this.#destroy();
					default: {
						// The type is `never`, so an action without a case does not compile. At runtime, a caller of
						// another version can send an action that this version does not know.
						const unknownCall: never = call;
						throw new FokosInternalError(SHARDING_INTERNAL_CODES.sharding_operation_invalid, {
							message: "the control-plane action is not known",
							attributes: { action: String((unknownCall as { action: unknown }).action) },
						});
					}
				}
			},
			() => collector,
		);
		return result as FokosShardingActions[A]["res"];
	}

	/**
	 * Serves one read for a repartition target that is still importing from this partition.
	 *
	 * A target cannot serve its own reads until its copy is complete, and it cannot ask this source
	 * through the ordinary API either: the source would route the request straight back to the target
	 * that sent it. This runs the local handler only, with no owner resolution and no lifecycle gate,
	 * for the slice the caller owns. A caller this partition cannot place is rejected outright.
	 */
	async #executeLocal(req: FokosExecuteLocalRequest, collector: RouteCollector): Promise<FokosEnvelope<unknown>> {
		const slice = this.#source.resolveCallerSlice(req.repartitionId, req.caller);
		const compiled = this.#compiled[req.op];
		const descriptor = compiled?.descriptor;
		if (!descriptor || descriptor.shape === "local" || descriptor.readOnly !== true) {
			throw new FokosInternalError(SHARDING_INTERNAL_CODES.sharding_operation_invalid, {
				message: "the operation is not registered as a read-only operation",
				attributes: { operation: req.op },
			});
		}

		// The same executor as on the owner, with a plan inside the caller slice: each span must be inside
		// the slice, a promoted routingHash goes to its range tree, and every other span runs here.
		if (compiled.execution !== "walk") {
			const spans = this.#spansOf(descriptor, req.request);
			const value = await this.#execute(req.op, compiled, spans, req.request, collector, { kind: "readThrough", slice });
			return envelope(value, collector.build());
		}

		// The point planner plans positions only. A walk reads an interval of one hash key, so it reads
		// through with its own gate.
		const topology = this.identity().topology;
		const routeCtx = this.routeContext();
		invariant(descriptor.shape === "range", "fokos/runtime: a walk is the range shape");
		const input = descriptor.range(req.request);
		if (!sliceIncludesHashKey(slice, input.hashKey, topology, this.#routingHashSeedResolver)) {
			throw misrouted(req.op, "hash key outside the caller slice");
		}
		let request = req.request;
		if (slice.kind === "range") {
			// The sort keys of the requested hash key inside the composite slice.
			const segment = projectOnHashKey(input.hashKey, slice.start, slice.end);
			if (segment === null || !rangeIntersects(segment.start ?? NO_SORT_KEY, segment.end, input.interval)) {
				throw misrouted(req.op, "interval disjoint from the caller slice");
			}
			// The caller proves its slice with the cursor too: one that lies outside it asks for rows of
			// another caller, which is a routing defect, not a rescan.
			if (input.cursor && !cursorFallsInChild(segment.start ?? NO_SORT_KEY, segment.end, input.cursor)) {
				throw misrouted(req.op, "cursor outside the caller slice");
			}
			request = descriptor.clip(req.request, { target: "local", start: segment.start, end: segment.end, speculative: false });
		}
		const routingHash = this.#routingHash(input.hashKey);
		if (slice.kind === "hash_child" && this.#store.hasTerminalRouteOverride(routingHash)) {
			// A range request spans sort keys, so it carries no one key that could select a deeper range
			// slice; it enters at the range root and the routers below it fan out.
			const root = refOf(resolveRangePartitionContext(routeCtx, routingHash, null, null));
			const value = await this.#forwardTo(collector, root, req.op, request, [routingHash]);
			return envelope(value, collector.build());
		}
		collector.add(this.#selfNode("executed"));
		const value = await this.#runLocal(descriptor, request, this.#localCall(slice).call);
		return envelope(value, collector.build());
	}

	/**
	 * Queues a key promotion on the partition that owns the key now. A router forwards to the owner,
	 * because a promotion queued on a router would migrate a stale snapshot and then shadow the live
	 * rows in the child.
	 */
	async #requestPromotionRpc(req: FokosRequestPromotionRequest): Promise<FokosRequestPromotionResult> {
		if (!this.#identity) {
			throw this.#notInitialized(req.target);
		}
		if (this.#identity.ref.partitionId !== req.target.partitionId || this.#identity.ref.doName !== req.target.doName) {
			throw contextMismatch({ doName: req.target.doName, expected: this.#identity.ref.doName });
		}
		return await this.#requestPromotion(req.hashKey, req.data);
	}

	async #requestPromotion(hashKey: KeyBytes, data: unknown): Promise<FokosRequestPromotionResult> {
		const identity = this.identity();
		const owner = identity.ref;
		if (this.#target.isImporting()) {
			throw new FokosUnavailableError(SHARDING_UNAVAILABLE_CODES.partition_migrating, {
				message: "the owner of the key is still importing, please retry later",
				attributes: { operation: RPC_OPERATION_NAMES.requestPromotion },
			});
		}
		// A range partition serves the key from its own tree: the key is promoted for good.
		if (identity.kind === "range") {
			return { owner, queued: false, reason: "already_promoted", state: "cleaned" };
		}

		// A promotion moves the whole routingHash of the key, with every hash key of it.
		const routingHash = this.#routingHash(hashKey);
		const override = this.#store.routeOverrideFor(routingHash);
		if (override) {
			// A promotion in flight gets its fallback alarm back and one pass now: a request that asks
			// about it is the one signal a partition gets when an eviction lost its alarm.
			if (override.state === "queued" || override.state === "planned" || override.state === "cutover") {
				await this.#scheduler.ensureAlarmAtMost(this.#platform.now() + this.#config().fallbackAlarmMs);
				this.#scheduler.wake();
			}
			return { owner, queued: false, reason: "already_promoted", state: override.state };
		}
		const resolution = this.#resolve({ hashKey, sortKey: NO_SORT_KEY }, EXACT);
		if (resolution.kind === "out_of_range") {
			throw misrouted(RPC_OPERATION_NAMES.requestPromotion, "key outside this partition");
		}
		if (resolution.kind === "remote") {
			return await this.#peer(resolution.target).fokosRpc("requestPromotion", { target: resolution.target, hashKey, data });
		}
		if (!this.#source.canQueue({ kind: "key_promotion", routingHash })) {
			return { owner, queued: false, reason: "split_in_progress" };
		}
		// The fallback alarm moves earlier BEFORE the queue transaction, so a crash between the two
		// leaves an alarm that reads the new row. A failed alarm write creates no row.
		await this.#scheduler.ensureAlarmAtMost(this.#platform.now() + this.#config().fallbackAlarmMs);
		const row = this.#source.queue({ kind: "key_promotion", routingHash, data });
		if (!row) {
			return { owner, queued: false, reason: "split_in_progress" };
		}
		console.log({
			...this.#logParams(),
			message: "fokos/runtime: routingHash queued for promotion.",
			hashKey: KeyCodec.keyForLog(hashKey),
			routingHash: routingHash.toHex(),
			repartitionId: row.id,
		});
		this.#scheduler.wake();
		return { owner, queued: true, state: row.state };
	}

	/**
	 * The identity of this partition and its split with the target links. The key promotions are in
	 * the `promotions` action. A root request carries its context and bootstraps an empty root. A
	 * target request omits the context and never creates an empty partition.
	 */
	#status(req: FokosStatusRequest): FokosStatusPage {
		if (req.rootContext) {
			this.#ensureIdentity(req.rootContext as FokosRouteContext<TPolicy>)();
		}
		const destroying = this.#store.isDestroying();
		if (!this.#identity) {
			return { initialized: false, destroying, ref: null, role: null, importState: null, split: [] };
		}
		return {
			initialized: true,
			destroying,
			ref: this.#identity.ref,
			role: this.#source.routerRole() ? "router" : "owner",
			importState: this.#target.importState(),
			split: this.#source.splitEntries(),
		};
	}

	/** One bounded page of the key promotions of this partition. A partition with no identity has none. */
	#promotions(req: FokosPromotionsRequest): FokosPromotionsPage {
		if (!this.#identity) {
			return { entries: [], nextCursor: null };
		}
		const config = this.#config();
		return this.#source.promotionEntries(req.cursor, config.promotionsPageEntries, config.promotionsPageBytes);
	}

	/**
	 * Fences this partition for destroy. Every background transition stops, and no new target appears.
	 * One transaction writes the fence and the optional root bootstrap. The call then waits for the
	 * pass in flight, which re-reads the fence before each remaining step, and deletes the alarm after
	 * it. A repeated call succeeds.
	 */
	async #prepareDestroy(req: FokosPrepareDestroyRequest): Promise<void> {
		let commitIdentity = NO_COMMIT;
		this.#store.transactionSync(() => {
			if (req.rootContext) {
				commitIdentity = this.#ensureIdentity(req.rootContext as FokosRouteContext<TPolicy>);
			}
			this.#store.setDestroying();
		});
		commitIdentity();
		await this.#scheduler.inFlight();
		this.#scheduler.stop();
		await this.#ctx.storage.deleteAlarm();
	}

	/** Cancels the schedule, deletes all storage, and aborts the instance so the next caller gets a fresh one. */
	async #destroy(): Promise<void> {
		this.#scheduler.stop();
		console.warn({ ...this.#logParams(), message: "fokos/runtime: destroying the partition, deleting all storage." });
		await this.#ctx.blockConcurrencyWhile(async () => {
			// Clears every timer of the instance: setTimeout returns a numeric ID that increments on
			// each call, so the newest ID gives the upper bound to clear from.
			const highestId = setTimeout(() => {
				for (let i = Number(highestId); i >= 0; i--) {
					clearTimeout(i);
				}
			}, 0);
			// The alarm goes before the storage, so the platform does not fire it on the evicted instance.
			await this.#ctx.storage.deleteAlarm();
			await this.#ctx.storage.deleteAll();
		});
		// Evicts the instance. The caller sees a throw with the sentinel message and ignores it.
		this.#ctx.abort(DESTROY_ABORT_SENTINEL);
	}

	async alarm(info?: AlarmInvocationInfo): Promise<void> {
		console.log({ ...this.#logParams(), message: "fokos/runtime: alarm triggered.", alarmInfo: info });
		// The handler can start before the scheduled time on the clock of this object. A pass then
		// finds no work due and arms the alarm at the same time again. The platform does not deliver
		// an alarm that a handler arms at its own scheduled time, so no pass runs after that. The
		// wait moves the clock to the scheduled time, and the pass then finds the work due. The wait
		// is at most the fallback interval, so a wrong scheduled time cannot hold the handler.
		//
		// NOTE: This shouldn't happen in production, but it happens in workerd/miniflare/vitest environments
		// due to clock discrepancies and going back in time occasionally.
		try {
			const earlyMs = Math.min((info?.scheduledTime ?? 0) - this.#platform.now(), this.#config().fallbackAlarmMs);
			if (earlyMs > 0) {
				await this.#platform.wait(earlyMs);
			}
		} catch {
			// A config throw landed in `#initError`, which `runDueWork` answers. Any other read failure
			// meets the pass-level catch of the scheduler and its fallback re-arm.
		}
		await this.runDueWork();
	}

	// ═══ identity ═══════════════════════════════════════════════════════════

	/**
	 * Validates the route context of a request against the stored identity, and stores a changed
	 * policy with the same or a higher `policyVersion`. A root hash partition without an identity takes it from its first request. Every other
	 * partition is created by the `init` action only. Synchronous: the `prepareDestroy` and `init` actions call
	 * it inside the transaction that writes the fence, or the import record.
	 *
	 * It returns the step that puts the new state in memory, and never takes that step itself. A caller
	 * inside a transaction takes it after that transaction commits, so a rollback cannot leave the
	 * memory of this instance ahead of its storage.
	 */
	#ensureIdentity(routeCtx: FokosRouteContext<TPolicy>): () => void {
		if (this.#ctx.id.jurisdiction !== routeCtx.topology.jurisdiction) {
			throw contextMismatch({
				doName: routeCtx.doName,
				jurisdictionReq: routeCtx.topology.jurisdiction,
				jurisdictionActual: this.#ctx.id.jurisdiction,
			});
		}
		if (!this.#identity) {
			if (isRangePartition(routeCtx)) {
				throw this.#notInitialized(routeCtx);
			}
			invariant(routeCtx.partitionId.length > 0, "fokos/runtime: partitionId must not be empty");
			if (PartitionIdHelper.depth(routeCtx.partitionId) > 0) {
				throw this.#notInitialized(routeCtx);
			}
			return this.#writeIdentity(routeCtx);
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
		// A lower version comes from an older client, for example during a deploy. The request then
		// runs with the stored values.
		if (routeCtx.policyVersion < stored.policyVersion) {
			return NO_COMMIT;
		}
		if (
			routeCtx.policyVersion === stored.policyVersion &&
			structurallyEqual(stored.rangeConfig, routeCtx.rangeConfig) &&
			structurallyEqual(stored.policy, routeCtx.policy)
		) {
			return NO_COMMIT;
		}
		const next = storedPolicyOf(routeCtx);
		this.#store.transactionSync(() => this.#store.putPolicy(next));
		return () => this.#setIdentity(identity, next);
	}

	/**
	 * Writes the identity of a target from its `init` action. It runs inside the transaction that writes
	 * the import record, so it returns the in-memory step for `initAsTarget` to take after the commit.
	 */
	#applyTargetIdentity(req: FokosInitRequest): () => void {
		const target = req.target as FokosRouteContext<TPolicy>;
		if (this.#identity) {
			return this.#ensureIdentity(target);
		}
		let range: { depth: number; ancestors: RangeAncestorInfo[] } | undefined;
		if (isRangePartition(target)) {
			invariant(req.rangeDepth !== undefined, "fokos/runtime.init: a range target needs its depth");
			const ancestors = req.rangeAncestors ?? [];
			invariant(ancestors.length === 0 || req.rangeDepth > 0, "fokos/runtime.init: only a non-root range partition has ancestors");
			range = { depth: req.rangeDepth, ancestors };
		}
		return this.#writeIdentity(target, range);
	}

	#writeIdentity(routeCtx: FokosRouteContext<TPolicy>, range?: { depth: number; ancestors: RangeAncestorInfo[] }): () => void {
		validateTopology(routeCtx.topology);
		const identity = partitionIdentityFrom(routeCtx, this.#routingHashSeedResolver, range);
		const stored = storedPolicyOf(routeCtx);
		this.#store.transactionSync(() => {
			this.#store.putIdentity(identity);
			this.#store.putPolicy(stored);
		});
		return () => this.#setIdentity(identity, stored);
	}

	/** Replaces the in-memory identity, policy and derived state together, so no reader sees one without the other. */
	#setIdentity(identity: FokosPartitionIdentity, stored: FokosStoredPolicy<TPolicy>): void {
		this.#identity = identity;
		this.#stored = stored;
		this.#routeCtx = { schema: 2, ...identity.ref, topology: identity.topology, ...stored };
		// The own slice is not in the list: a learner decodes it from the partition ID of the node.
		this.#rangeAncestors = identity.range?.ancestors ?? [];
	}

	#selfNode(role: FokosServedRole): FokosRouteNode {
		const identity = this.identity();
		const depth = identityDepth(identity);
		return {
			ref: identity.ref,
			actorId: this.#actorId,
			hashDepth: identity.kind === "hash" ? depth : 0,
			rangeDepth: identity.kind === "range" ? depth : 0,
			role,
			...(this.#rangeAncestors.length > 0 ? { _rangeAncestors: this.#rangeAncestors } : {}),
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
			message: "hash partition is not initialized; only the init action creates a non-root hash partition",
			attributes: { doName },
		});
	}

	// ═══ owner resolution ═══════════════════════════════════════════════════

	/** The range form of the ownership check: the hash key, and the whole interval inside a range partition. */
	#assertCanOwnRange(op: string, input: FokosRangeInput): void {
		const identity = this.identity();
		const routingHash = this.#routingHash(input.hashKey);
		if (identity.kind === "hash") {
			if (!this.#hashesHere(routingHash, identity)) {
				throw misrouted(op, "hash key outside this partition");
			}
			return;
		}
		const range = identity.range!;
		if (compareRoutingHashes(routingHash, range.routingHash) !== 0) {
			throw misrouted(op, "hash key outside this range partition");
		}
		const segment = projectOnHashKey(input.hashKey, range.start, range.end);
		if (segment === null || !intervalInside(input.interval, segment.start, segment.end)) {
			throw misrouted(op, "interval outside this range partition");
		}
	}

	/** Hash step 1 and range step 1: can this partition own the position at all, by its identity alone. */
	#ownsByTopology(pos: FokosStoragePosition): boolean {
		const identity = this.identity();
		if (identity.kind === "hash") {
			return this.#hashesHere(pos.routingHash, identity);
		}
		const range = identity.range!;
		return compareRoutingHashes(pos.routingHash, range.routingHash) === 0 && compositeInInterval(pos, range.start, range.end);
	}

	#hashesHere(routingHash: RoutingHash, identity: FokosPartitionIdentity): boolean {
		const { start, end } = identity.hash!;
		return inRoutingHashInterval(routingHash, start, end);
	}

	/** The routingHash of a hash key in the shard group of this partition. It is not kept after the call. */
	#routingHash(hashKey: KeyBytes): RoutingHash {
		return routingHashOf(hashKey, this.identity().topology.shardGroup, this.#routingHashSeedResolver);
	}

	/** The resolution of a key: the resolution of its position. */
	#resolve(key: RouteKey, opts: ResolveOptions, view?: RouterView): Resolution {
		return this.#resolvePosition(this.#positionOf(key), opts, view);
	}

	/**
	 * The position of a route key or of a storage key. A key costs one hash, which the ownership check,
	 * the override, the Bloom filter, and the topology all use. A storage key with no `hk` or `sk` gets the
	 * empty key, which is the minimum.
	 */
	#positionOf(key: RouteKey | FokosStorageKey): FokosStoragePosition {
		return storagePositionOf(key, this.identity().topology.shardGroup, this.#routingHashSeedResolver);
	}

	/** `view` is read here when the caller does not give one. A position with a cut-over override does not need it. */
	#resolvePosition(pos: FokosStoragePosition, opts: ResolveOptions, view?: RouterView): Resolution {
		const identity = this.identity();
		const { routingHash } = pos;
		if (!this.#ownsByTopology(pos)) {
			return { kind: "out_of_range" };
		}

		if (identity.kind === "hash") {
			const override = this.#store.routeOverrideFor(routingHash);
			if (override && cutOver(override.state)) {
				return this.#rangeOwner(routingHash, pos, "override", false, opts.learnedRange);
			}
			if (opts.bloom && this.#bloom?.maybePromoted(routingHash)) {
				return this.#rangeOwner(routingHash, pos, "bloom", true, opts.learnedRange);
			}
			return this.#hashTopologyOwner(routingHash, (view ?? this.#routerView()).router);
		}

		const { router, rangeChildren } = view ?? this.#routerView();
		if (!router) {
			return { kind: "local" };
		}
		const range = identity.range!;
		const child = this.#rangeChildFor(pos, rangeChildren);
		const learned = opts.learnedRange ? this.#store.findDeepestKnownRangeSlice(range.routingHash, pos) : null;
		const jump = learned && isStrictSubSlice(learned, child.start, child.end) ? learned : null;
		const target = jump
			? refOf(resolveRangePartitionContext(this.routeContext(), range.routingHash, jump.startBoundary, jump.endBoundary))
			: child.ref;
		return { kind: "remote", target, speculative: false, via: "range", learned: jump };
	}

	/**
	 * The owner of a routingHash that has no cut-over route override on this hash partition: local on an
	 * owner, else the deeper of the hash descendants that the hash arena and the topology cache name.
	 * `router` is `routerRole()`, which reads SQLite, so a caller that asks for many keys reads it once.
	 *
	 * The key belongs to this partition, so the topology walk of the key goes through this partition,
	 * and its depth below this partition is the depth of the walk minus the depth of this partition. An
	 * older topology can stop above this partition, and then it gives no hint.
	 */
	#hashTopologyOwner(routingHash: RoutingHash, router: boolean): OwnedResolution {
		if (!router) {
			return { kind: "local" };
		}
		let relDepth = Math.max(1, this.#arena()?.findLeaf(routingHash) ?? 0);
		let topologyHint: RemoteResolution["topologyHint"];
		const cacheOptions = this.#topologyCache?.();
		const entry = cacheOptions && cachedHashTopology(cacheOptions, this.identity().topology);
		if (entry) {
			const { rootTreesN, hashSplitN } = this.identity().topology;
			const leaf = findLeaf(entry.view, rootTreesN, hashSplitN, routingHash, entry.cut);
			const topologyDepth = leaf.path.length - identityDepth(this.identity());
			if (topologyDepth >= relDepth) {
				relDepth = topologyDepth;
				topologyHint = { entry, pos: leaf.pos };
			}
		}
		return {
			kind: "remote",
			target: this.#hashDescendant(routingHash, relDepth),
			speculative: false,
			via: "hash",
			relDepth,
			...(topologyHint ? { topologyHint } : {}),
			learned: null,
		};
	}

	/** The range partition that serves a promoted routingHash: the deepest learned slice that holds the key, or the root. */
	#rangeOwner(
		routingHash: RoutingHash,
		key: FokosCompositeKey,
		via: "override" | "bloom",
		speculative: boolean,
		learnedRange: boolean,
	): RemoteResolution {
		const routeCtx = this.routeContext();
		const learned = learnedRange ? this.#store.findDeepestKnownRangeSlice(routingHash, key) : null;
		const jump = learned && (learned.startBoundary !== null || learned.endBoundary !== null) ? learned : null;
		const target = jump
			? refOf(resolveRangePartitionContext(routeCtx, routingHash, jump.startBoundary, jump.endBoundary))
			: refOf(resolveRangePartitionContext(routeCtx, routingHash, null, null));
		return { kind: "remote", target, speculative, via, learned: jump };
	}

	/** The hash descendant `relDepth` levels below this partition that owns a routingHash. */
	#hashDescendant(routingHash: RoutingHash, relDepth: number): FokosPartitionRef {
		const identity = this.identity();
		const { rootTreesN, hashSplitN } = identity.topology;
		const depth = identityDepth(identity);
		const idxs = routingHashDigits(routingHash, rootTreesN, hashSplitN, depth + relDepth).path.slice(depth);
		return refOf(resolveDescendantHashPartitionContext(this.routeContext(), idxs));
	}

	/** The direct range child whose interval contains `key`. The children tile the whole interval, so one always does. */
	#rangeChildFor(key: FokosCompositeKey, children: readonly FokosChild[]): FokosChild {
		let best: FokosChild | null = null;
		for (const child of children) {
			if (
				(child.start === null || KeyCodec.compareComposite(child.start, key) <= 0) &&
				(best === null || startCmp(child.start, best.start) > 0)
			) {
				best = child;
			}
		}
		invariant(best !== null, () => `fokos/runtime: no range child owns ${KeyCodec.compositeForLog(key)}`);
		return best;
	}

	/** The hash arena of a router, created the first time this partition forwards. Null on an owner. */
	#arena(): HashTopology | null {
		if (this.#hashArena) {
			return this.#hashArena;
		}
		const identity = this.identity();
		if (identity.kind !== "hash" || !this.#source.routerRole()) {
			return null;
		}
		const snapshot = this.#store.getHashArena();
		const opts = { budgetBytes: this.#config().hashArenaBytes };
		this.#hashArena = snapshot
			? HashTopology.fromSnapshot(snapshot, opts)
			: HashTopology.create(identity.topology.rootTreesN, identity.topology.hashSplitN, identityDepth(identity), opts);
		return this.#hashArena;
	}

	// ═══ the range frontier ═════════════════════════════════════════════════

	#planRange(op: string, input: FokosRangeInput, bloom: boolean): RangePlan[] {
		const identity = this.identity();
		const routeCtx = this.routeContext();
		let bases: FrontierBase[];
		let learned: LearnedRangeSlice[] = [];
		this.#assertCanOwnRange(op, input);
		// The miss hint of each remote base, for the miss ladder.
		const hints = new Map<FrontierBase, MissHint>();
		const base = (b: FrontierBase, hint: MissHint): FrontierBase => {
			hints.set(b, hint);
			return b;
		};

		// The range tree of the key, if it has one, is the tree of this routingHash.
		const routingHash = this.#routingHash(input.hashKey);
		if (identity.kind === "hash") {
			const override = this.#store.routeOverrideFor(routingHash);
			const root = refOf(resolveRangePartitionContext(routeCtx, routingHash, null, null));
			if (override && cutOver(override.state)) {
				bases = [base({ target: root, start: null, end: null, speculative: false }, { via: "override", learned: null })];
				learned = this.#store.listLearnedRangeSlices(routingHash);
			} else if (bloom && this.#bloom?.maybePromoted(routingHash)) {
				bases = [base({ target: root, start: null, end: null, speculative: true }, { via: "bloom", learned: null })];
				learned = this.#store.listLearnedRangeSlices(routingHash);
			} else if (this.#source.routerRole()) {
				const relDepth = Math.max(1, this.#arena()?.findLeaf(routingHash) ?? 0);
				const target = this.#hashDescendant(routingHash, relDepth);
				bases = [base({ target, start: null, end: null, speculative: false }, { via: "hash", relDepth, learned: null })];
			} else {
				bases = [{ target: "local", start: null, end: null, speculative: false }];
			}
		} else {
			const range = identity.range!;
			const { router, rangeChildren } = this.#routerView();
			if (router) {
				bases = rangeChildren.map((child) =>
					base({ target: child.ref, start: child.start, end: child.end, speculative: false }, { via: "range", learned: null }),
				);
				learned = this.#store.listLearnedRangeSlices(range.routingHash);
			} else {
				bases = [{ target: "local", start: range.start, end: range.end, speculative: false }];
			}
		}

		const planned = planRangeFrontier(input.hashKey, bases, learned, input.interval, input.descending, (slice) =>
			refOf(resolveRangePartitionContext(routeCtx, routingHash, slice.startBoundary, slice.endBoundary)),
		);
		return planned.map((p) => {
			const baseHint = hints.get(p.base);
			const plan: RangePlan = {
				planned: p,
				// A visit to a learned slice misses as that slice does. The base gives the Bloom fact.
				hint: p.learned ? { via: baseHint?.via ?? "range", learned: p.learned } : (baseHint ?? { via: "range", learned: null }),
			};
			this.#plans.set(p.visit, plan);
			return plan;
		});
	}

	/**
	 * Forwards one planned visit, with the miss ladder of every forward. A learned slice that does not
	 * exist is forgotten, and the segment goes to its base target. A hash jump that finds no partition
	 * forgets its arena hint and plans again. A speculative visit that finds no range partition, or no
	 * cutover, plans the hash tree again with the Bloom step off: on a router the target is the hash
	 * child, on a leaf it is this partition, and `runLocal` answers for the same visit. Every retry of a
	 * cache miss goes through the same ladder, so a miss on a retry cannot skip a fallback.
	 * `maxForwardRetries` bounds the misses one call can absorb.
	 */
	async #forwardRangeVisit(
		op: string,
		compiled: CompiledOperation,
		visit: FokosRangeVisit,
		req: unknown,
		collector: RouteCollector,
		runLocal: (req: unknown) => unknown,
	): Promise<unknown> {
		let plan: RangePlan | undefined = this.#plans.get(visit);
		invariant(plan, "fokos/runtime.forwardRangeVisit: the visit was not planned by this runtime");
		invariant(visit.target !== "local", "fokos/runtime.forwardRangeVisit: a local visit is served by the tracked local function");
		const input = rangeDescriptor(compiled).range(req);
		const routingHashes = [this.#routingHash(input.hashKey)];
		let target = visit.target;
		for (let attempt = 0; ; attempt++) {
			try {
				return await this.#forwardTo(collector, target, op, req, routingHashes);
			} catch (e) {
				const base: FrontierBase = plan.planned.base;
				const learned: LearnedRangeSlice | null = plan.planned.learned;
				// The ladder deletes a learned slice that does not exist. Only then does the segment go to its base.
				const learnedMissed = learned !== null && FokosError.isCode(e, SHARDING_ROUTING_CODES.range_partition_not_initialized);
				const opts = attempt < this.#config().maxForwardRetries ? this.#missLadder(routingHashes[0], plan.hint, e, true, true) : null;
				if (!opts) {
					throw e;
				}
				// Every error that starts a fallback comes before any handler ran on the target.
				collector.forget(target.partitionId);
				if (learnedMissed) {
					invariant(base.target !== "local", "fokos/runtime.forwardRangeVisit: a learned slice never overlays a local base");
					target = base.target;
					plan = { planned: { visit, base, learned: null }, hint: { via: plan.hint.via, learned: null } };
					continue;
				}
				const again = this.#planRange(op, input, opts.bloom);
				invariant(again.length === 1, "fokos/runtime.forwardRangeVisit: a hash partition plans one base visit");
				const fallback = again[0];
				if (fallback.planned.visit.target === "local") {
					return await runLocal(req);
				}
				target = fallback.planned.visit.target;
				plan = fallback;
			}
		}
	}

	// ═══ forwarding and learning ════════════════════════════════════════════

	/**
	 * One outbound RPC to a partition of the host's own class. The default calls the method named
	 * after the operation with the derived route context. The list of the answer, or of the error, is
	 * learned against `routingHashes`, of the keys or the position that the RPC carried, and merged into
	 * `collector` here, so every path that forwards pays for it once.
	 */
	async #forwardTo(
		collector: RouteCollector,
		target: FokosPartitionRef,
		op: string,
		req: unknown,
		routingHashes: readonly RoutingHash[],
	): Promise<unknown> {
		const descriptor = this.#ops[op];
		invariant(descriptor && descriptor.shape !== "local", () => `fokos/runtime: ${op} cannot be forwarded`);
		const targetCtx: FokosRouteContext<TPolicy> = { ...this.routeContext(), partitionId: target.partitionId, doName: target.doName };
		const stub = this.#stub(targetCtx, target.doName);
		collector.countForward();
		const call = descriptor.forward
			? descriptor.forward(stub, targetCtx, req)
			: ((stub as unknown as Record<string, (ctx: unknown, req: unknown) => Promise<FokosEnvelope<unknown>>>)[op](
					targetCtx,
					req,
				) as Promise<FokosEnvelope<unknown>>);
		// Only the partition that enters the range tree stamps its depth. A hash-to-hash forward must
		// leave the nodes alone, or a router above would overwrite the depth of the partition that owns
		// the promoted key with its own shallower one.
		const stamp = isRangePartition(target) ? this.#hashDepthStampForRangeNodes() : undefined;
		try {
			const result = await call;
			this.#learn(result.routing.servedBy, routingHashes);
			collector.mergeForwarded(result.routing, stamp);
			return result.value;
		} catch (e) {
			// The error path learns what the success path learns, and the same error object travels on
			// with this hop added, so an error and a result that take one route carry one routing.
			const routed = routedError(e);
			if (routed) {
				// The head of the child's list is the partition that raised the error. It keeps the head
				// here too, so the byte cap can never drop it and a caller reads it from one place,
				// whatever this partition had already collected.
				const raiser = routed.routing.servedBy[0];
				try {
					this.#learn(routed.routing.servedBy, routingHashes);
				} catch {}
				collector.mergeForwarded(routed.routing, stamp);
				// Under `collect`, the error becomes a failed part, and this partition answers with a value.
				// The envelope of that value is not an error, so it gets no raiser, and the error keeps the
				// routing of the child: `merge` reads the raiser at the head of that list.
				if (!(descriptor.shape === "group" && descriptor.failurePolicy === "collect")) {
					if (raiser) {
						collector.addRaiser(stamp ? stamp(raiser) : raiser);
					}
					routed.routing = collector.build();
				}
			}
			throw e;
		}
	}

	/**
	 * A range partition has no hash depth of its own. The hash partition that enters the range tree
	 * writes its own depth on every range node it merges, so a hash router above learns where the
	 * promoted key left the hash tree. A range partition changes nothing.
	 */
	#hashDepthStampForRangeNodes(): ((node: FokosRouteNode) => FokosRouteNode) | undefined {
		const identity = this.identity();
		if (identity.kind !== "hash") {
			return undefined;
		}
		const hashDepth = identityDepth(identity);
		return (node) => (isRangePartition(node.ref) ? { ...node, hashDepth } : node);
	}

	/**
	 * Learns from the nodes of a child envelope for the routingHashes of this request: the range
	 * boundaries and the promotion of every range node, and the depth of the deepest hash node that
	 * owns each routingHash. The identity of a node is its scope, so a hash node owns a routingHash on
	 * its path, and a range node names its hash key and its own slice itself.
	 *
	 * A slice is stored only when this partition can use it. A hash partition uses each slice below
	 * the range root. A range partition at depth d uses only a slice strictly inside one of its
	 * children, which is at depth d + 2 or deeper.
	 */
	#learn(nodes: readonly FokosRouteNode[], routingHashes: readonly RoutingHash[]): void {
		const identity = this.identity();
		const myDepth = identityDepth(identity);
		const minRangeDepth = identity.kind === "hash" ? 1 : myDepth + 2;
		const refreshMs = this.#config().rangeHierarchyRefreshMs;
		const now = this.#platform.now();
		const arena = identity.kind === "hash" ? this.#arena() : null;
		let arenaChanged = false;
		let bloomChanged = false;
		const learnDepth = (routingHash: RoutingHash, hashDepth: number) => {
			const relDepth = hashDepth - myDepth;
			if (arena && relDepth > 0) {
				arenaChanged = arena.updateFromHint(routingHash, relDepth) || arenaChanged;
			}
		};
		for (const node of nodes) {
			if (isRangePartition(node.ref)) {
				const shouldLearnNode = node.rangeDepth >= minRangeDepth;
				// A range partition uses the routingHash only to store a slice, so it skips the decode when it stores none.
				if (identity.kind !== "hash" && !shouldLearnNode && !node._rangeAncestors?.some((a) => a.depth >= minRangeDepth)) {
					continue;
				}
				let routingHash: RoutingHash;
				if (shouldLearnNode) {
					const decoded = PartitionIdHelper.decode(PartitionIdHelper.partitionIdToBytes(node.ref.partitionId));
					invariant(decoded.schema === PartitionIdHelper.SCHEMA_RANGE_V1, "fokos/runtime.learn: a range node has a hash partition ID");
					routingHash = decoded.routingHash;
					this.#store.learnRangeBoundary(routingHash, decoded.startBoundary, decoded.endBoundary, node.rangeDepth, refreshMs, now);
				} else {
					// Only the first part of the ID, because the own slice of the node is not stored.
					routingHash = PartitionIdHelper.rangeRoutingHash(node.ref.partitionId);
				}
				for (const ancestor of node._rangeAncestors ?? []) {
					if (ancestor.depth >= minRangeDepth) {
						this.#store.learnRangeBoundary(routingHash, ancestor.startBoundary, ancestor.endBoundary, ancestor.depth, refreshMs, now);
					}
				}
				if (identity.kind !== "hash") {
					continue;
				}
				// Only hash partitions are relevant for the promotions and the hash splits.
				const added = this.#bloomForLearning().learnPromotedRoutingHash(routingHash);
				if (added === AddResult.Added) {
					bloomChanged = true;
				} else if (added === AddResult.Full) {
					console.info({
						...this.#logParams(),
						message: "fokos/runtime: the promotion Bloom filter is full, cannot learn a promoted key.",
					});
				}
				learnDepth(routingHash, node.hashDepth);
				continue;
			}
			const bytes = Uint8Array.fromHex(node.ref.partitionId);
			for (const routingHash of routingHashes) {
				if (hashPathOwns(bytes, routingHash, identity.topology)) {
					learnDepth(routingHash, node.hashDepth);
				}
			}
		}
		if (arenaChanged && arena) {
			this.#store.putHashArena(arena.toSnapshot());
		}
		if (bloomChanged) {
			const bloom = this.#bloom;
			invariant(bloom, "fokos/runtime: expected a promotion Bloom filter to exist before flushing");
			const flushMs = this.#config().promotionBloomFlushMs;
			if (flushMs === 0) {
				this.#bloomFlushThrottled.forceRun();
			} else {
				this.#bloomFlushThrottled.schedule(flushMs);
			}
		}
	}

	#flushBloom(): void {
		if (!this.#bloom) {
			return;
		}
		// In a timer task of its own, this write is best-effort: `allowUnconfirmed` lets the outgoing
		// messages of other requests go without waiting for it.
		void this.#store.putPromotionBloomUnconfirmed(this.#bloom.toSnapshot()).catch((error: unknown) => {
			console.error({
				...this.#logParams(),
				message: "fokos/runtime: writing the promotion Bloom filter failed.",
				error: String(error),
				errorProps: error,
			});
		});
	}

	#bloomForLearning(): PartialRangeTopology {
		if (this.#bloom) {
			return this.#bloom;
		}
		const config = this.#config();
		this.#bloom = PartialRangeTopology.create({
			errorRate: config.promotionBloomFalsePositiveRate,
			initialCapacityN: config.promotionBloomExpectedKeys,
			maxSizeBytes: PROMOTION_BLOOM_MAX_BYTES,
		});
		return this.#bloom;
	}

	// ═══ local execution, admission, signals ════════════════════════════════

	/**
	 * A `local` or `beforeForward` call collects its signals; the caller applies them after a success.
	 * `slice` is the caller slice of a read-through, which is the scope of the call. Else the scope is the
	 * owned interval of this partition.
	 */
	#localCall(slice: FokosSlice | null = null): { call: FokosLocalCall; signals: FokosSignals[] } {
		const signals: FokosSignals[] = [];
		return { call: { signal: (s) => signals.push(s), ownedSliceEnd: (pos) => this.#ownedSliceEnd(pos, slice) }, signals };
	}

	/**
	 * The end of the slice of the scope that holds `pos`, which is also the start of the next slice. The kind
	 * of the scope decides the end, not the kind of this partition: a hash source can
	 * serve a range caller slice. Only a hash scope ends at a promoted routingHash, because a range scope is
	 * inside the range tree of its promoted routingHash.
	 */
	#ownedSliceEnd(pos: FokosStorageKey, slice: FokosSlice | null): FokosStorageKey | null {
		const identity = this.identity();
		invariant(slice !== null || !this.#source.routerRole(), "fokos/runtime.ownedSliceEnd: a router owns no slice");
		if (slice?.kind === "range" || slice?.kind === "promoted_routing_hash" || (slice === null && identity.kind === "range")) {
			const range = slice === null ? identity.range! : slice.kind === "range" ? slice : { routingHash: slice.routingHash, end: null };
			return rangeStorageInterval(range.routingHash, null, range.end).end;
		}
		const hash = identity.hash;
		invariant(hash, "fokos/runtime.ownedSliceEnd: hash identity must exist for hash scope");
		const { rootTreesN, hashSplitN } = identity.topology;
		// A hash child of a split is one level below its source.
		invariant(
			slice === null || slice.depth === hash.path.length + 1,
			"fokos/runtime.ownedSliceEnd: the caller slice is not a child of this partition",
		);
		const hashEnd =
			slice === null ? hash.end : routingHashInterval(rootTreesN, hashSplitN, hash.rootIndex, [...hash.path, slice.childIndex]).end;
		const routingHash = asRoutingHash(pos.routingHash);
		const promoted = this.#store.firstCutOverRouteOverride(routingHash, hashEnd);
		// The routing sends each position of a promoted routingHash to its range tree.
		invariant(
			promoted === null || compareRoutingHashes(promoted, routingHash) !== 0,
			"fokos/runtime.ownedSliceEnd: a position of a promoted routingHash reached its hash scope",
		);
		const endHash = promoted ?? hashEnd;
		return endHash === null ? null : { routingHash: endHash };
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
		if (!descriptor.beforeForward) {
			return [];
		}
		const { call, signals } = this.#localCall();
		descriptor.beforeForward(req, call);
		return signals;
	}

	/**
	 * Runs the local handler and lists this partition as its executor. The node is added before the
	 * handler runs: a handler that throws still served the scope, and the error carries the node so a
	 * forwarding partition learns from the error as it learns from a result. The signals come back to
	 * the caller, because the caller decides when they apply: after the handler settled, and only when
	 * it did not throw. `epoch` is the ownership epoch of the plan that chose this partition.
	 */
	async #runLocalScope(
		descriptor: Exclude<AnyOperation, { shape: "local" }>,
		req: unknown,
		collector: RouteCollector,
		scope: ExecutionScope,
		epoch: number,
	): Promise<{ value: unknown; signals: FokosSignals[] }> {
		const { call, signals } = this.#localCall(sliceOf(scope));
		collector.add(this.#selfNode("executed"));
		this.#assertEpoch(epoch);
		const value = await this.#runLocal(descriptor, req, call);
		return { value, signals };
	}

	/**
	 * The plan that chose this partition and the local run must be in one synchronous block, so that no
	 * cutover comes between the ownership decision and the write. A cutover increments the epoch, so a
	 * changed epoch means that an executor awaited between its plan and its local run.
	 */
	#assertEpoch(epoch: number): void {
		invariant(epoch === this.#ownershipEpoch, "fokos/runtime: a cutover came between the plan and the local run");
	}

	#admit(op: string, descriptor: Exclude<AnyOperation, { shape: "local" }>, spans: readonly NormalizedSpan[]): void {
		if (!this.#hooks.admit) {
			return;
		}
		const decision = this.#hooks.admit({
			op,
			admissionTag: descriptor.admissionTag,
			keys: admittedKeys(spans),
			// Every dispatch admits. A `lifecycle()` result reads its storage only for the fields that the
			// hook reads, so a host that admits on its own size or tag alone reads nothing.
			lifecycle: this.lifecycle(),
			policy: this.policy(),
		});
		if (decision !== "allow") {
			// A write that applies is the other trigger of the split decision, and a partition over its
			// admission limit applies none. The host hook runs before the throw, and the queue work runs
			// after the caller has its answer.
			this.requestSplitEvaluation();
			throw decision.reject;
		}
	}

	/** A read-through admits nothing: the target admitted the request, and a read changes nothing here. */
	#admitIn(
		scope: ExecutionScope,
		op: string,
		descriptor: Exclude<AnyOperation, { shape: "local" }>,
		spans: readonly NormalizedSpan[],
	): void {
		if (scope.kind === "owned") {
			this.#admit(op, descriptor, spans);
		}
	}

	/** A read-through runs no `beforeForward`: the target ran it. */
	#beforeForwardIn(scope: ExecutionScope, descriptor: Exclude<AnyOperation, { shape: "local" }>, req: unknown): FokosSignals[] {
		return scope.kind === "owned" ? this.#beforeForward(descriptor, req) : [];
	}

	/** A read-through applies no signal: its handler serves the slice of another partition. */
	async #applySignalsIn(scope: ExecutionScope, signals: readonly FokosSignals[]): Promise<void> {
		if (scope.kind === "owned") {
			await this.#applySignals(signals);
		}
	}

	/**
	 * Applies the signals of a completed local phase. The result is already committed, so a failure
	 * here is logged and does not change it: the next request repeats the checks.
	 */
	async #applySignals(signals: readonly FokosSignals[]): Promise<void> {
		try {
			for (const s of signals) {
				if (s.evaluateSplit) {
					await this.#evaluateSplit();
				}
				for (const candidate of s.promotionCandidates ?? []) {
					await this.#requestPromotion(candidate.hashKey, candidate.data);
				}
				for (const job of s.jobs ?? []) {
					await this.#scheduler.scheduleJob(job.name, job.runAt);
				}
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
		// A split row in any state blocks every repartition here, a router included. The check runs
		// before the host decision, because on a range leaf that decision scans rows.
		if (this.#store.getSplitRepartition() !== undefined) {
			return;
		}
		const identity = this.identity();
		const decision = this.#hooks.evaluateSplit({ identity, policy: this.policy() });
		if (decision === false) {
			return;
		}
		if ("promote" in decision) {
			for (const hashKey of decision.promote) {
				try {
					await this.#requestPromotion(hashKey, decision.data);
				} catch (error) {
					// The other keys still get their try. The next decision tries this key again.
					console.error({
						...this.#logParams(),
						message: "fokos/runtime: the promotion of a key that the split decision named failed.",
						hashKey: KeyCodec.keyForLog(hashKey),
						error: String(error),
						errorProps: error,
					});
				}
			}
			return;
		}
		const kind = identity.kind === "hash" ? "hash_split" : "range_split";
		// The synchronous precheck rejects an ineligible attempt without an alarm write.
		if (!this.#source.canQueue({ kind })) {
			return;
		}
		await this.#scheduler.ensureAlarmAtMost(this.#platform.now() + this.#config().fallbackAlarmMs);
		const row = this.#source.queue({ kind, data: decision.data });
		if (!row) {
			return;
		}
		console.log({ ...this.#logParams(), message: "fokos/runtime: split conditions met.", repartitionId: row.id, kind: row.kind });
		this.#scheduler.wake();
	}

	// ═══ jobs ═══════════════════════════════════════════════════════════════

	/** The active settings: the host overrides over the defaults, validated. Read at each use. */
	#config(): FokosRuntimeConfig {
		try {
			return resolveRuntimeConfig(this.#configOverrides?.());
		} catch (error) {
			// A config that cannot resolve behaves as a failed initialization: the gate answers it and
			// the alarm retries it, so a fixed deploy heals the object.
			this.#initError = error;
			throw error;
		}
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
					const pages = this.#config().importPagesPerPass;
					for (let i = 0; i < pages; i++) {
						if (this.#store.isDestroying()) {
							break;
						}
						if ((await this.#target.importOnePage()) !== "progressed") {
							break;
						}
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
			admin_outbox: {
				name: "admin_outbox",
				canRun,
				deadline: () => this.#store.earliestAdminOutboxDeadline(),
				runStep: async () => {
					await this.#adminOutboxStep();
					return { nextRunAt: null };
				},
			},
		};
		return BUILTIN_JOBS.map((name) => jobs[name]);
	}

	/**
	 * Writes the report of a completed repartition to the admin outbox. It runs inside the completion
	 * transaction, so the report exists exactly when the state is `completed`. The outbox keeps its
	 * own copy of the event, because the cleanup deletes the plan before it writes `cleaned`. The copy
	 * holds no name of the table: the send adds the shard group of this partition at that time.
	 */
	#recordCompletion(plan: FokosRepartitionPlan, completedAt: number): void {
		if (!this.#adminOutbox?.()) {
			return;
		}
		const { topology } = this.routeContext();
		// The local repartition id repeats on other partitions, and the source partition ID makes it unique.
		const id = `${plan.source.partitionId}/${plan.id}`;
		const event: FokosStoredTopologyEvent = {
			schema: 1,
			id,
			kind: plan.kind,
			rootTreesN: topology.rootTreesN,
			hashSplitN: topology.hashSplitN,
			sourcePartitionId: plan.source.partitionId,
			targets: plan.targets.map((t) => ({ partitionId: t.ref.partitionId, slice: t.slice })),
			completedAt,
		};
		this.#store.insertAdminOutbox(TOPOLOGY_EVENT_TYPE, id, topologyEventToJson(event), completedAt);
	}

	/**
	 * Sends the due outbox rows, at most `ADMIN_OUTBOX_BATCH`, one at a time. The rows are read in
	 * full first, because a cursor does not stay valid across the `await` of a send. A row goes after its
	 * send resolves. A send that throws keeps the row, and its next attempt waits an exponential
	 * backoff. Without a hook, the step deletes every row.
	 */
	async #adminOutboxStep(): Promise<void> {
		if (this.#store.isDestroying()) {
			return;
		}
		const outbox = this.#adminOutbox?.();
		if (!outbox) {
			this.#store.transactionSync(() => this.#store.deleteAllAdminOutbox());
			return;
		}
		let expired = false;
		let timer: number | undefined;
		const timeout = new Promise<never>((_, reject) => {
			timer = this.#platform.setTimeout(() => {
				expired = true;
				reject(new Error("fokos/runtime: admin outbox step timed out"));
			}, ADMIN_OUTBOX_TIMEOUT_MS);
		});
		const { shardGroup } = this.routeContext().topology;
		try {
			for (const row of this.#store.selectDueAdminOutbox(this.#platform.now(), ADMIN_OUTBOX_BATCH)) {
				if (expired || this.#store.isDestroying()) {
					return;
				}
				try {
					const event = { ...(JSON.parse(row.eventJson) as FokosStoredTopologyEvent), shardGroup };
					const delivery = outbox.send({ eventType: row.eventType, id: row.id, event }).then(() => {
						if (!this.#store.isDestroying()) {
							this.#store.transactionSync(() => this.#store.deleteAdminOutbox(row.eventType, row.id, row.eventJson));
						}
					});
					this.#ctx.waitUntil(delivery.catch(() => {}));
					await Promise.race([delivery, timeout]);
				} catch (error) {
					if (this.#store.isDestroying()) {
						return;
					}
					const attempt = row.attempt + 1;
					// The base delay plus a jittered backoff, so each retry waits from 1 s to 5 min.
					const delay =
						ADMIN_OUTBOX_RETRY_BASE_MS +
						jitterBackoff(attempt, ADMIN_OUTBOX_RETRY_BASE_MS, ADMIN_OUTBOX_RETRY_MAX_MS - ADMIN_OUTBOX_RETRY_BASE_MS, this.#platform);
					const updated = this.#store.transactionSync(() =>
						this.#store.setAdminOutboxAttempt(row.eventType, row.id, attempt, this.#platform.now() + delay, row.eventJson),
					);
					if (!updated) {
						continue;
					}
					console.warn({
						...this.#logParams(),
						message: "fokos/runtime: an admin outbox send failed. The runtime sends it again later.",
						eventType: row.eventType,
						id: row.id,
						attempt,
						error: String(error),
					});
				}
			}
		} finally {
			this.#platform.clearTimeout(timer);
		}
	}

	// ═══ plumbing ═══════════════════════════════════════════════════════════

	/**
	 * Runs one entry point: the destroy fence first, then `fn`, and every error that leaves is a
	 * `FokosError` that carries the routing of this partition. The `status`, `promotions`,
	 * `prepareDestroy`, and `destroy` actions stay available behind the fence.
	 */
	async #guard<T>(name: string, fn: () => Promise<T>, collector?: () => RouteCollector | undefined): Promise<T> {
		try {
			const descriptor = this.#ops[name];
			const allowedWhileDestroying =
				name === RPC_OPERATION_NAMES.status ||
				name === RPC_OPERATION_NAMES.promotions ||
				name === RPC_OPERATION_NAMES.prepareDestroy ||
				name === RPC_OPERATION_NAMES.destroy ||
				(descriptor?.shape === "local" && descriptor.allowedWhileDestroying === true);
			if (this.#store.isDestroying() && !allowedWhileDestroying) {
				throw new FokosUnavailableError(SHARDING_UNAVAILABLE_CODES.partition_migrating, {
					message: "partition destroy in progress, please retry later",
					attributes: { operation: name },
				});
			}
			// A failed initialization degrades every storage path. The `destroy` and `prepareDestroy`
			// actions stay live: they are the way out of a poisoned object. The `status` action stays live
			// too: it is the diagnosis path, and it reads KV before it reads any table. A failed host
			// initialization degrades the same paths, because the runtime RPCs read the host tables.
			const initError = this.#initError ?? this.#hostInitError?.();
			if (
				initError !== undefined &&
				name !== RPC_OPERATION_NAMES.status &&
				name !== RPC_OPERATION_NAMES.prepareDestroy &&
				name !== RPC_OPERATION_NAMES.destroy
			) {
				throw new FokosInternalError(SHARDING_INTERNAL_CODES.partition_init_failed, {
					message: "the initialization of the partition failed",
					attributes: { operation: name, ...causeAttributes(initError) },
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

	/**
	 * Compiles each operation once: its execution kind, and the facts the executor reads. It refuses a
	 * descriptor that breaks a rule of its execution kind (`EXECUTION_RULES`).
	 */
	#compileOperations(): Record<string, CompiledOperation> {
		const compiled: Record<string, CompiledOperation> = {};
		for (const [name, descriptor] of Object.entries(this.#ops)) {
			// The runtime reserves the prefix for the operation names of its control-plane actions, and
			// `#guard` answers some of them behind the destroy fence. A host operation that took one of
			// those names would answer there too.
			if (name.startsWith("fokos")) {
				throw new FokosInternalError(SHARDING_INTERNAL_CODES.sharding_operation_invalid, {
					message: "a host operation must not use the fokos prefix, which the runtime reserves for its control RPCs",
					attributes: { operation: name },
				});
			}
			const entry = compileOperation(descriptor);
			if (descriptor.shape !== "local" && descriptor.whileMigrating === "read_source") {
				if (descriptor.readOnly !== true || !EXECUTION_RULES[entry.execution].readSource) {
					throw new FokosInternalError(SHARDING_INTERNAL_CODES.sharding_operation_invalid, {
						message: "a read_source operation must be readOnly, and its execution must allow a read-through",
						attributes: { operation: name, shape: descriptor.shape, execution: entry.execution, readOnly: descriptor.readOnly },
					});
				}
			}
			compiled[name] = entry;
		}
		return compiled;
	}

	#logParams(): Record<string, unknown> {
		const identity = this.#identity;
		const importSource = this.#target.importRecord()?.source;
		return {
			actorId: this.#actorId,
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

/** The mutable part of a route context, validated, as a partition stores it under `__fokos/policy`. */
function storedPolicyOf<TPolicy>(routeCtx: FokosRouteContext<TPolicy>): FokosStoredPolicy<TPolicy> {
	validateRangeConfig(routeCtx.rangeConfig);
	validatePolicyVersion(routeCtx.policyVersion);
	return { rangeConfig: routeCtx.rangeConfig, policy: routeCtx.policy, policyVersion: routeCtx.policyVersion };
}

function cutOver(state: RepartitionState): boolean {
	return state === "cutover" || state === "completed" || state === "cleaned";
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
	return typeof value === "object" && value !== null && typeof (value as { then?: unknown }).then === "function";
}

/** True when the digits of a routingHash follow the whole path a hash partition id encodes: the root index, then one child index per level. */
function hashPathOwns(idBytes: Uint8Array, routingHash: RoutingHash, topology: { rootTreesN: number; hashSplitN: number }): boolean {
	const decoded = PartitionIdHelper.decode(idBytes);
	if (decoded.schema !== PartitionIdHelper.SCHEMA_HASH_V1) {
		return false;
	}
	const digits = new RoutingHashDigitCursor(routingHash, topology.rootTreesN);
	if (digits.root !== decoded.rootIdx) {
		return false;
	}
	for (let d = 0; d < decoded.depth; d++) {
		if (digits.next(topology.hashSplitN) !== idBytes[4 + d]) {
			return false;
		}
	}
	return true;
}

/** True when the request interval lies inside the half-open `[start, end)` of a range partition. */
function intervalInside(interval: SkInterval, start: KeyBytes | null, end: KeyBytes | null): boolean {
	if (start !== null && (interval.lower === undefined || KeyCodec.compare(interval.lower.value, start) < 0)) {
		return false;
	}
	if (end !== null) {
		if (interval.upper === undefined) {
			return false;
		}
		const cmp = KeyCodec.compare(interval.upper.value, end);
		if (cmp > 0 || (cmp === 0 && interval.upper.inclusive)) {
			return false;
		}
	}
	return true;
}

/**
 * True when the lifecycle gate of a target in `awaiting_data` refused the request. The gate refuses
 * before any handler runs, so nothing applied.
 *
 * The error can come from a partition below the one that the sender called. For example, a range root
 * cuts over and splits, and its range child stays in `awaiting_data` until its first page arrives. The
 * fallback is still safe: the exact resolution names the same range root again, and the error of the
 * second forward goes to the caller. The node of the child stays in the routing of the request.
 */
function isAwaitingData(e: unknown): boolean {
	return FokosError.isCode(e, SHARDING_UNAVAILABLE_CODES.partition_migrating) && e.attributes.importState === "awaiting_data";
}

function isBloomHit(resolution: Resolution): resolution is RemoteResolution {
	return resolution.kind === "remote" && resolution.via === "bloom";
}

function addToPlan(plan: PointPlan, span: PointSpan, resolution: OwnedResolution): void {
	if (resolution.kind === "local") {
		plan.local.push(span);
		return;
	}
	const visit = plan.remote.get(resolution.target.partitionId);
	if (visit) {
		visit.spans.push(span);
	} else {
		plan.remote.set(resolution.target.partitionId, { resolution, spans: [span] });
	}
}

/** The execution kind of a shape, and the facts of the shape that its executor reads. */
function compileOperation(descriptor: AnyOperation): CompiledOperation {
	switch (descriptor.shape) {
		case "point":
			return { descriptor, execution: "one_owner", bloomOnRetry: true };
		case "single_owner":
			return { descriptor, execution: "one_owner", onManyOwners: { answer: descriptor.notApplicable }, bloomOnRetry: false };
		case "group":
			return { descriptor, execution: "fan_out", bloomOnRetry: false };
		case "range":
			return { descriptor, execution: "walk", bloomOnRetry: true };
		case "local":
			return { descriptor, execution: "local", bloomOnRetry: false };
	}
}

type RoutedDescriptor = Exclude<AnyOperation, { shape: "local" }>;
type GroupDescriptor = Extract<AnyOperation, { shape: "group" }>;
type RangeDescriptor = Extract<AnyOperation, { shape: "range" }>;

function routedDescriptor(compiled: CompiledOperation): RoutedDescriptor {
	invariant(compiled.descriptor.shape !== "local", "fokos/runtime: a local operation has no executor");
	return compiled.descriptor;
}

function groupDescriptor(compiled: CompiledOperation): GroupDescriptor {
	invariant(compiled.descriptor.shape === "group", "fokos/runtime: a fan_out operation is the group shape");
	return compiled.descriptor;
}

function rangeDescriptor(compiled: CompiledOperation): RangeDescriptor {
	invariant(compiled.descriptor.shape === "range", "fokos/runtime: a walk operation is the range shape");
	return compiled.descriptor;
}

/** The answer of a `one_owner` request whose spans have more than one owner. */
function manyOwnersAnswer(op: string, compiled: CompiledOperation): unknown {
	invariant(compiled.onManyOwners, () => `fokos/runtime: the spans of ${op} cannot have more than one owner`);
	return compiled.onManyOwners.answer;
}

function pointSpans(spans: readonly NormalizedSpan[]): PointSpan[] {
	return spans.map((span) => {
		invariant(span.kind !== "interval", "fokos/runtime: a point executor got an interval span");
		return span;
	});
}

function intervalSpan(spans: readonly NormalizedSpan[]): IntervalSpan {
	const [span] = spans;
	invariant(spans.length === 1 && span.kind === "interval", "fokos/runtime: a walk has one interval span");
	return span;
}

/** The host data of an item span: the `item` that `items(req)` gave for its key. */
function dataOf(span: PointSpan): unknown {
	invariant(span.kind === "item", "fokos/runtime: a fan_out span names an item");
	return span.data;
}

/** The routingHashes that a forward learns against: one for each span. */
function routingHashesOf(spans: readonly NormalizedSpan[]): RoutingHash[] {
	return spans.map((span) => span.position.routingHash);
}

/**
 * The keys that `hooks.admit` gets. An item gives its key. A position names no item, so it gives none.
 * An interval gives its hash key with the empty sort key.
 */
function admittedKeys(spans: readonly NormalizedSpan[]): RouteKey[] {
	const keys: RouteKey[] = [];
	for (const span of spans) {
		if (span.kind === "item") {
			keys.push(span.key);
		} else if (span.kind === "interval") {
			keys.push({ hashKey: span.range.hashKey, sortKey: NO_SORT_KEY });
		}
	}
	return keys;
}

/** The slice that is the scope of a local call: the caller slice of a read-through, or null for the owned scope. */
function sliceOf(scope: ExecutionScope): FokosSlice | null {
	return scope.kind === "readThrough" ? scope.slice : null;
}

/** Never transient: the key reached a partition that can neither own nor route it. */
function misrouted(operation: string, reason: string): FokosRoutingError {
	return new FokosRoutingError(SHARDING_ROUTING_CODES.partition_misrouted, {
		message: "mis-routed key this partition can neither own nor route",
		attributes: { operation, reason },
	});
}

/**
 * The identity of a failure, as attributes of the error that reports it. `cause` holds the object
 * itself for a log line in this isolate, but it is a non-enumerable own property of `Error` and an
 * RPC hop drops it, so the code and the error id of the original travel here instead.
 */
function causeAttributes(reason: unknown): Record<string, unknown> {
	if (!FokosError.is(reason)) {
		return { causeMessage: String(reason) };
	}
	return { causeCode: reason.code, causeErrorId: reason.error_id, causeMessage: reason.message };
}

function contextMismatch(attributes: Record<string, unknown>): FokosInternalError {
	return new FokosInternalError(SHARDING_INTERNAL_CODES.partition_context_mismatch, { message: "partition context mismatch", attributes });
}

/** The range a partition owns, rendered for a log line. KeyBytes never appear as bare Uint8Array. */
function rangeForLog(identity: FokosPartitionIdentity | undefined): Record<string, unknown> | undefined {
	const range = identity?.range;
	if (!range) {
		return undefined;
	}
	return {
		routingHash: range.routingHash.toHex(),
		start: KeyCodec.compositeForLog(range.start),
		end: KeyCodec.compositeForLog(range.end),
		depth: range.depth,
	};
}
