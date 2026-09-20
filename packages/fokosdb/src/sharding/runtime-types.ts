/**
 * The public types of `FokosShardingRuntime`: the operation descriptors a host registers, the hooks it
 * implements, the envelope every operation returns, and the facts the primitive API reports.
 *
 * Nothing here is FokosDB code. A host declares its operations once as a spec type, and every other
 * signature of the runtime derives from it, so a call that names one operation and passes the request
 * of another does not compile.
 */
import type { KeyBytes } from "./key-codec.js";
import type { FokosPartitionIdentity, FokosPartitionRef, FokosRouteContext } from "./route-context.js";
import type { FokosSlice } from "./repartition-slice.js";
import type { FokosImportState, MigrationHost, RouteKey } from "./repartition-types.js";
import type { RepartitionKind, RepartitionState } from "./sharding-store.js";
import type { SkInterval } from "./sk-interval.js";
import type { RangeAncestorInfo } from "./types.js";

export type { RouteKey };

// ─── runtime options ─────────────────────────────────────────────────────────

export type FokosRuntimeOptions<TPolicy> = {
	/**
	 * A stub for one partition of the host's own class. Host code: it applies the host binding, the
	 * topology jurisdiction, and the policy location hint. The runtime never creates a stub itself. When
	 * the runtime needs a stub outside a request, it calls this with its own stored route context.
	 */
	stub(ctx: FokosRouteContext<TPolicy>, doName: string): DurableObjectStub;
	caches?: {
		/** The byte budget of the hash arena cache. Default: 1 MiB. */
		hashArenaBytes?: number;
		/** The row bound of the learned range hierarchy. Default: 10,000. */
		rangeHierarchyMaxRows?: number;
		/** The Bloom filter of promoted keys a hash partition learns. Default: 300,000 keys at 1%. */
		promotionBloom?: { expectedKeys: number; falsePositiveRate: number };
	};
	scheduler?: {
		/** How far ahead a pass arms its fallback before its first transition. Default: 5,000 ms. */
		fallbackAlarmMs?: number;
		/** The delay of the in-memory fast path that runs a pass without an alarm. Default: 50 ms. */
		fastPathDelayMs?: number;
	};
};

// ─── the primitive API ───────────────────────────────────────────────────────

export type FokosLifecycle = {
	/** A router forwards every key after a split cutover; an owner serves its keys locally. */
	role: "owner" | "router";
	/** The import this partition was created by, or null for a root. `source` is the partition it imports from. */
	import: null | { state: FokosImportState; source: FokosPartitionRef; slice: FokosSlice };
	activeRepartition: null | { id: string; kind: RepartitionKind; state: "queued" | "planned" | "cutover" };
	/** True after `fokosPrepareDestroy`. Every operation is refused, and a host job or timer must make no transition. */
	destroying: boolean;
};

export type FokosOwner =
	| { kind: "local" }
	/** A speculative owner comes from a cache hint and is not a fact: the caller falls back on a miss. */
	| { kind: "remote"; target: FokosPartitionRef; speculative: boolean }
	| { kind: "out_of_range" };

/** One direct target of a router, in `target_index` order. Range children carry their interval. */
export type FokosChild = { ref: FokosPartitionRef; start: KeyBytes | null; end: KeyBytes | null };

/**
 * One range request. The interval keeps its inclusive and exclusive bounds: an inclusive upper bound
 * that equals a child's start boundary must still visit that child, which a half-open pair cannot say.
 */
export type FokosRangeInput = { hashKey: KeyBytes; interval: SkInterval; descending: boolean };

/**
 * One planned visit of a range frontier.
 *
 * `start` and `end` are the half-open sort-key segment this visit covers, inside the immutable
 * interval of the target partition. A visit that fills a gap of the learned hierarchy covers only that
 * gap and not the whole interval of its target, so two visits never serve one sort key twice. The host
 * clips its request to the segment.
 */
export type FokosRangeVisit = {
	target: FokosPartitionRef | "local";
	start: KeyBytes | null;
	end: KeyBytes | null;
	speculative: boolean;
};

// ─── the envelope ────────────────────────────────────────────────────────────

/** What a partition did for the request. A partition that only forwarded is not in the list. */
export type FokosServedRole =
	/** Ran the local handler for its scope. */
	| "executed"
	/** Ran `merge` or `walk` over parts from other partitions. A router. */
	| "merged"
	/** Owns the scope but still imports it: its source ran the handler on its behalf. */
	| "read_through";

/**
 * One partition that served a scope of the request. Its identity is its scope: a hash `partitionId`
 * decodes to the root index and the child path, and a range `partitionId` to the hash key and the
 * interval it owns, so a router tests the keys of its own request against the node and needs no scope list.
 */
export type FokosRouteNode = {
	ref: FokosPartitionRef;
	actorId: string;
	/**
	 * The hash depth of this partition, or, for a range partition, of the hash partition that entered
	 * its tree. A range partition has no hash depth of its own, so the hash partition that forwards
	 * into its tree supplies the value, and a hash router above learns the depth of that partition.
	 */
	hashDepth: number;
	rangeDepth: number;
	role: FokosServedRole;
	/** Internal. The bounded ancestor boundaries of a range partition. A consumer must drop it. */
	_rangeAncestors?: RangeAncestorInfo[];
};

export type FokosRouting = {
	/** Every partition that served a scope of this request, once each, keyed by `ref.partitionId`. */
	servedBy: FokosRouteNode[];
	/** Total outbound partition RPCs in this response tree, forwards and read-throughs alike. */
	forwardCount: number;
	/** True when the byte cap dropped one or more nodes from `servedBy`. */
	servedByTruncated: boolean;
};

export type FokosEnvelope<T> = { value: T; routing: FokosRouting };

/** The routing without its internal hints, as `FokosRouter.unwrap` hands it to a Worker. */
export type FokosPublicRoute = Omit<FokosRouteNode, "_rangeAncestors">;
export type FokosPublicRouting = { servedBy: FokosPublicRoute[]; forwardCount: number };

// ─── signals and operations ──────────────────────────────────────────────────

export type FokosSignals = {
	/** Ask `hooks.evaluateSplit` whether this partition must split now. */
	evaluateSplit?: boolean;
	/** Keys that have grown past the host's own threshold. Each becomes a promotion request to its owner. */
	promotionCandidates?: Array<{ hashKey: KeyBytes; data?: unknown }>;
	/**
	 * A condition that `beforeCutover` tests has changed, for example a lock was released. Every
	 * repartition that the hook held back becomes due now, instead of at its flat retry interval.
	 */
	repartitionUnblocked?: boolean;
	/** Host jobs that must run by a deadline because of this result. */
	jobs?: Array<{ name: string; runAt: number }>;
};

/**
 * Handed to every `local` and `beforeForward` call. A handler reports signals through it, because it
 * knows facts its response does not carry. The runtime applies the signals after the handler returns
 * without throwing. `fokosExecuteLocal` passes one whose `signal` is a no-op.
 */
export type FokosLocalCall = { signal(signals: FokosSignals): void };

export type FokosOperationBase<Req, Res> = {
	/**
	 * "retry": while this partition imports, throw `partition_migrating`.
	 * "read_source": while this partition imports, run the same operation on the source partition.
	 */
	whileMigrating: "retry" | "read_source";
	/** The operation never writes partitioned data. Required for `whileMigrating: "read_source"`. */
	readOnly?: boolean;
	/** Opaque to the runtime. Passed to `hooks.admit`. */
	admissionTag?: string;
	/**
	 * "sync" (default): `local` must return a value; a thenable throws `sharding_local_must_be_sync`.
	 * "async": `local` can await, and the host closes the cutover race itself with `dispatch` or `owns`.
	 */
	localMode?: "sync" | "async";
	local(req: Req, call: FokosLocalCall): Res | Promise<Res>;
	/**
	 * Runs on every partition the request passes through, owner or router, after admission and before
	 * the first remote call. For work that is keyed by something other than a route key, for example a
	 * lock release by transaction id. It must not write partitioned data by key. Its result is discarded.
	 */
	beforeForward?(req: Req, call: FokosLocalCall): void;
	/**
	 * Default: the runtime calls the method named after the operation on the target stub with the
	 * derived route context and the request. A host overrides it only when the remote method has another name.
	 */
	forward?(stub: DurableObjectStub, target: FokosRouteContext<unknown>, req: Req): Promise<FokosEnvelope<Res>>;
};

/** One remote or local part of a `group` operation, as `merge` receives it. */
export type FokosGroupPart<Req, Res> = { target: FokosPartitionRef | "local"; request: Req; result: Res };

export type FokosOperation<Req, Res> =
	| (FokosOperationBase<Req, Res> & { shape: "point"; key(req: Req): RouteKey })
	| (FokosOperationBase<Req, Res> & {
			shape: "group";
			items(req: Req): Array<{ key: RouteKey; item: unknown }>;
			subRequest(req: Req, items: unknown[]): Req;
			/** Each part carries the sub-request it was given, so a merge can answer for every item. */
			merge(parts: Array<FokosGroupPart<Req, Res>>): Res;
			/** "fail_fast": stop at the first failure. "attempt_all": run every group, then throw if any failed. */
			failurePolicy: "fail_fast" | "attempt_all";
	  })
	| (FokosOperationBase<Req, Res> & {
			shape: "single_owner";
			items(req: Req): Array<{ key: RouteKey }>;
			/** The answer when the items span more than one partition. Returned, never thrown. */
			notApplicable: Res;
	  })
	| (FokosOperationBase<Req, Res> & {
			shape: "range";
			whileMigrating: "read_source";
			readOnly: true;
			range(req: Req): FokosRangeInput;
			/** Restricts the request to one planned visit. */
			clip(req: Req, visit: FokosRangeVisit): Req;
			/**
			 * Owns budgets, cursors, visit order, early exit, and result folding. It reaches partitions only
			 * through the tracked functions in `input`.
			 */
			walk(input: {
				request: Req;
				visits: readonly FokosRangeVisit[];
				local(req: Req): Res | Promise<Res>;
				/**
				 * The part this visit answered. The runtime collects the route evidence of the visit into
				 * the routing of the whole walk, which `dispatch` returns, so the host folds a forwarded
				 * part exactly as it folds a local one.
				 */
				forward(visit: FokosRangeVisit, req: Req): Promise<Res>;
			}): Promise<Res>;
	  })
	| {
			shape: "local";
			/** Can be async: a `local` shape has no owner resolution to race against. */
			local(req: Req, call: FokosLocalCall): Res | Promise<Res>;
	  };

/** The host's own declaration of its operations. Every other signature derives from it. */
export type FokosOperationSpec = Record<string, { req: unknown; res: unknown }>;
export type FokosOperations<Ops extends FokosOperationSpec> = { [K in keyof Ops]: FokosOperation<Ops[K]["req"], Ops[K]["res"]> };

// ─── hooks ───────────────────────────────────────────────────────────────────

export type FokosRepartitionPlan<TPolicy = unknown> = {
	id: string;
	kind: RepartitionKind;
	source: FokosPartitionRef;
	targets: Array<{ ref: FokosPartitionRef; slice: FokosSlice }>;
	/** "router": the source owns nothing after cutover. "retains_others": it owns all non-selected keys. */
	sourceAfterCutover: "router" | "retains_others";
	/** The host policy at queue time. Every hook that receives the plan reads this copy. */
	policy: TPolicy;
	/** Opaque host data from `evaluateSplit` or `requestPromotion`. */
	data?: unknown;
};

export type FokosJob = {
	name: string;
	/** False skips the job in this pass, and keeps its `deadline()` out of the alarm. Synchronous. */
	canRun(): boolean;
	/** One bounded, idempotent step. Can be async: it runs outside any transaction. */
	runStep(): { nextRunAt: number | null } | Promise<{ nextRunAt: number | null }>;
	/** The earliest time this job has durable work, read from the host's own storage, or null. Synchronous. */
	deadline?(): number | null;
};

export type FokosRuntimeConfigOverrides = {
	/** How many import pages one pass applies. Default: 16. Minimum: 1. */
	importPagesPerPass?: number;
};

/**
 * Every hook is synchronous. Four of them run inside a `transactionSync`, and an `await` there is a
 * defect. A hook returns its policy result and does not throw to express one: a thrown error is a
 * defect that the runtime logs, and the durable state stays unchanged until the next pass.
 */
export interface FokosShardingHooks<TPolicy> {
	/**
	 * Called after a local success that signals `evaluateSplit`, and by `requestSplitEvaluation`.
	 * Returns `false`, or `{ data }` when the host wants this partition to split now. `data` is opaque
	 * and travels in the plan.
	 */
	evaluateSplit(input: { identity: FokosPartitionIdentity; policy: TPolicy }): false | { data?: unknown };
	/**
	 * Range partitions only. Returns `childCount - 1` strictly increasing boundaries inside (start, end),
	 * or null when the host cannot produce valid boundaries yet.
	 */
	computeRangeBoundaries?(input: {
		hashKey: KeyBytes;
		start: KeyBytes | null;
		end: KeyBytes | null;
		childCount: number;
		policy: TPolicy;
	}): KeyBytes[] | null;
	/** The host phase of the migration. */
	migration: MigrationHost;
	/**
	 * Source side. Consulted before the runtime initializes the first target of the plan, and inside the
	 * cutover transaction. Return false to hold the plan at its current state.
	 */
	beforeCutover?(plan: FokosRepartitionPlan<TPolicy>): boolean;
	/** Source side. Runs inside the completion transaction, after the last acknowledgement. */
	beforeComplete?(plan: FokosRepartitionPlan<TPolicy>): void;
	/**
	 * Source side. One bounded step of source cleanup after completion. Returns whether the source rows
	 * of the plan are all gone. Undefined means the source keeps its data.
	 */
	cleanupSourceStep?(plan: FokosRepartitionPlan<TPolicy>): boolean;
	/** Called by the local admission step. Default: allow. */
	admit?(input: {
		op: string;
		admissionTag?: string;
		keys: RouteKey[];
		/** Read on demand: it costs several storage reads, and a hook that ignores it pays nothing. */
		lifecycle: FokosLifecycle;
		policy: TPolicy;
	}): "allow" | { reject: Error };
	/** Live runtime configuration overrides. The runtime validates each returned value. */
	runtimeConfig?(): FokosRuntimeConfigOverrides;
	/** Host background jobs. They run after the built-in jobs, in registration order. */
	jobs?: FokosJob[];
}

export type FokosRequestPromotionResult = { owner: FokosPartitionRef } & (
	| { queued: true; state: RepartitionState }
	/** The owner already tracks the key. `state` is the state of that promotion. */
	| { queued: false; reason: "already_promoted"; state: RepartitionState }
	/** A split row exists on the owner, so the key moves soon. */
	| { queued: false; reason: "split_in_progress" }
);
