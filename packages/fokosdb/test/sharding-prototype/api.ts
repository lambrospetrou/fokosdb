/**
 * PROTOTYPE. Type-only surface of `fokosdb/sharding` as `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`
 * describes it. Nothing here runs: the two classes are `declare`d and have no implementation. The other files
 * in this directory are consumers that compile against this surface, and `pnpm check` (tsc) is the test.
 *
 * Where this file departs from the RFC text, the departure is what a consumer needed to compile. Each such
 * place has a comment that starts with "Departure:".
 */
import type { KeyBytes } from "../../src/shared/partition-topology/key-codec.js";
import type { RangeAncestorInfo } from "../../src/shared/partition-topology/types.js";
import type { RepartitionKind, RepartitionState } from "../../src/shared/partition/partition-store.js";
import type { FokosSlice } from "../../src/shared/partition/repartition/repartition-slice.js";
import type { SkInterval } from "../../src/shared/query/sk-interval.js";

export type { FokosSlice, KeyBytes, RangeAncestorInfo, RepartitionKind, RepartitionState, SkInterval };

// ─── identity, topology, policy ──────────────────────────────────────────────

export type RouteKey = { hashKey: KeyBytes; sortKey: KeyBytes };

export type FokosTopology = {
	shardGroup: string;
	rootTreesN: number;
	hashSplitN: number;
	jurisdiction?: DurableObjectJurisdiction;
};

export type FokosRangeConfig = {
	rangeSplitN: number;
	rangeAncestors: { fromRoot: number; fromLeaf: number };
};

export type FokosRouteContext<TPolicy> = {
	schema: 2;
	partitionId: string;
	doName: string;
	topology: FokosTopology;
	rangeConfig: FokosRangeConfig;
	policy: TPolicy;
};

export type FokosPartitionRef = Pick<FokosRouteContext<unknown>, "partitionId" | "doName">;

export type FokosPartitionIdentity = {
	schema: 1;
	ref: FokosPartitionRef;
	kind: "hash" | "range";
	hash?: { rootIndex: number; path: number[] };
	range?: { hashKey: KeyBytes; start: KeyBytes | null; end: KeyBytes | null; depth: number; ancestors: RangeAncestorInfo[] };
	topology: FokosTopology;
};

export type FokosRuntimeOptions<TPolicy> = {
	/** A stub for one partition of the host's own class. */
	stub(ctx: FokosRouteContext<TPolicy>, doName: string): DurableObjectStub;
	caches?: {
		hashArenaBytes?: number;
		rangeHierarchyMaxRows?: number;
		promotionBloom?: { expectedKeys: number; falsePositiveRate: number };
	};
	scheduler?: { fallbackAlarmMs?: number; fastPathDelayMs?: number };
};

// ─── primitives ──────────────────────────────────────────────────────────────

export type FokosLifecycle = {
	role: "owner" | "router";
	import: null | { state: "awaiting_data" | "importing" | "imported" | "active" };
	activeRepartition: null | { id: string; kind: RepartitionKind; state: "queued" | "planned" | "cutover" };
};

export type FokosOwner = { kind: "local" } | { kind: "remote"; target: FokosPartitionRef; speculative: boolean } | { kind: "out_of_range" };

export type FokosChild = { ref: FokosPartitionRef; start: KeyBytes | null; end: KeyBytes | null };

/**
 * Departure: the RFC gives `start`/`end` as bare `KeyBytes | null`. A query interval has an inclusive or an
 * exclusive bound at each end (`SkInterval`), and an inclusive upper bound that equals a child's start
 * boundary must still visit that child. A half-open pair cannot say that, so the planner takes the interval
 * the host already has. `sk-interval.ts` moves into `src/sharding/` in M1, so the type is available there.
 */
export type FokosRangeInput = { hashKey: KeyBytes; interval: SkInterval; descending: boolean };

export type FokosRangeVisit = {
	target: FokosPartitionRef | "local";
	/** The immutable `[start, end)` interval of the visited partition, clipped to nothing. The host clips its request itself. */
	start: KeyBytes | null;
	end: KeyBytes | null;
	speculative: boolean;
};

// ─── envelope ────────────────────────────────────────────────────────────────

export type FokosRouteScope =
	| { kind: "point"; key: RouteKey }
	| { kind: "range"; hashKey: KeyBytes; start: KeyBytes | null; end: KeyBytes | null };

export type FokosRouteNode = {
	servedBy: FokosPartitionRef;
	servedByActorId: string;
	hashDepth: number;
	rangeDepth: number;
	_hint?: { rangeAncestors: RangeAncestorInfo[] };
};

export type FokosRouteEvidence = FokosRouteNode & { scopes: FokosRouteScope[] };

export type FokosRouting = {
	summary: FokosRouteNode;
	routes: FokosRouteEvidence[];
	forwardCount: number;
	routesTruncated: boolean;
};

export type FokosEnvelope<T> = { value: T; routing: FokosRouting };

/** An error that a partition raised carries its routing as a serializable own property. */
export type FokosRoutedError = Error & { routing?: FokosRouting };

export type FokosPublicRoute = Omit<FokosRouteNode, "_hint">;
export type FokosPublicRouting = { summary: FokosPublicRoute; routes: FokosPublicRoute[]; forwardCount: number };

// ─── signals and operations ──────────────────────────────────────────────────

export type FokosSignals = {
	evaluateSplit?: boolean;
	promotionCandidates?: Array<{ hashKey: KeyBytes; data?: unknown }>;
	repartitionUnblocked?: boolean;
	jobs?: Array<{ name: string; runAt: number }>;
};

/**
 * Departure: the RFC collects signals from `afterLocalSuccess(req, res)`. A local handler knows facts that
 * its response does not carry (the estimated bytes of the key it wrote, the promotion candidates of a
 * commit), and a router that runs only `beforeForward` has no local success at all. The runtime therefore
 * hands every local call one `FokosLocalCall`, and the handler reports signals through it. The runtime
 * applies them after the handler returns, and only when it returned without throwing. `fokosExecuteLocal`
 * passes a call whose `signal` is a no-op.
 */
export type FokosLocalCall = { signal(signals: FokosSignals): void };

export type FokosOperationBase<Req, Res> = {
	whileMigrating: "retry" | "read_source";
	readOnly?: boolean;
	admissionTag?: string;
	localMode?: "sync" | "async";
	local(req: Req, call: FokosLocalCall): Res | Promise<Res>;
	beforeForward?(req: Req, call: FokosLocalCall): void;
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
			/**
			 * Departure: the RFC passes `{ target, result }` only. A merge that answers for every item it was
			 * given (a prepare that fills "passed" for an accepted child) needs the sub-request of each part.
			 */
			merge(parts: Array<FokosGroupPart<Req, Res>>): Res;
			failurePolicy: "fail_fast" | "attempt_all";
	  })
	| (FokosOperationBase<Req, Res> & { shape: "single_owner"; items(req: Req): Array<{ key: RouteKey }>; notApplicable: Res })
	| (FokosOperationBase<Req, Res> & {
			shape: "range";
			whileMigrating: "read_source";
			readOnly: true;
			range(req: Req): FokosRangeInput;
			clip(req: Req, visit: FokosRangeVisit): Req;
			walk(input: {
				request: Req;
				visits: readonly FokosRangeVisit[];
				local(req: Req): Res | Promise<Res>;
				forward(visit: FokosRangeVisit, req: Req): Promise<FokosEnvelope<Res>>;
			}): Promise<Res>;
	  })
	| { shape: "local"; local(req: Req, call: FokosLocalCall): Res | Promise<Res> };

/**
 * Departure: the RFC types the registry as `Record<string, FokosOperation<any, any>>` and `dispatch` as
 * `dispatch<Req, Res>(op: string, ...)`. That pair lets a caller name one operation and pass the request of
 * another. The host instead declares one spec type, `{ [name]: { req; res } }`, and the registry, `dispatch`,
 * and `forward` are all typed from it.
 */
export type FokosOperationSpec = Record<string, { req: unknown; res: unknown }>;
export type FokosOperations<Ops extends FokosOperationSpec> = { [K in keyof Ops]: FokosOperation<Ops[K]["req"], Ops[K]["res"]> };

// ─── hooks ───────────────────────────────────────────────────────────────────

export type FokosRepartitionPlan<TPolicy = unknown> = {
	id: string;
	kind: RepartitionKind;
	source: FokosPartitionRef;
	targets: Array<{ ref: FokosPartitionRef; slice: FokosSlice }>;
	sourceAfterCutover: "router" | "retains_others";
	policy: TPolicy;
	data?: unknown;
};

export interface MigrationHost {
	buildPage(cursor: unknown, slice: FokosSlice, belongsToTarget: (key: RouteKey) => boolean): { page: unknown; nextCursor: unknown | null };
	applyPage(page: unknown, slice: FokosSlice): void;
	validatePage(cursor: unknown, page: unknown, nextCursor: unknown | null): void;
}

export type FokosJob = {
	name: string;
	canRun(): boolean;
	runStep(): { nextRunAt: number | null } | Promise<{ nextRunAt: number | null }>;
	deadline?(): number | null;
};

export type FokosRuntimeConfigOverrides = { importPagesPerPass?: number };

export interface FokosShardingHooks<TPolicy> {
	evaluateSplit(input: { identity: FokosPartitionIdentity; policy: TPolicy }): false | { data?: unknown };
	computeRangeBoundaries?(input: {
		hashKey: KeyBytes;
		start: KeyBytes | null;
		end: KeyBytes | null;
		childCount: number;
		policy: TPolicy;
	}): KeyBytes[] | null;
	migration: MigrationHost;
	beforeCutover?(plan: FokosRepartitionPlan<TPolicy>): boolean;
	beforeComplete?(plan: FokosRepartitionPlan<TPolicy>): void;
	cleanupSourceStep?(plan: FokosRepartitionPlan<TPolicy>): boolean;
	admit?(input: {
		op: string;
		admissionTag?: string;
		keys: RouteKey[];
		lifecycle: FokosLifecycle;
		policy: TPolicy;
	}): "allow" | { reject: Error };
	runtimeConfig?(): FokosRuntimeConfigOverrides;
	jobs?: FokosJob[];
}

export type FokosRequestPromotionResult = { owner: FokosPartitionRef } & (
	| { queued: true; state: RepartitionState }
	| { queued: false; reason: "already_promoted"; state: RepartitionState }
	| { queued: false; reason: "split_in_progress" }
);

// ─── control-plane RPC ───────────────────────────────────────────────────────

export type FokosMigrationCursor = { phase: "overrides"; inner: unknown } | { phase: "host"; inner: unknown };

export type FokosInitRequest = {
	repartitionId: string;
	source: FokosPartitionRef;
	target: FokosRouteContext<unknown>;
	slice: FokosSlice;
	rangeDepth?: number;
	rangeAncestors?: RangeAncestorInfo[];
};
export type FokosStartImportRequest = { repartitionId: string; source: FokosPartitionRef };
export type FokosMigrationPullRequest = { repartitionId: string; target: FokosPartitionRef; cursor: FokosMigrationCursor | null };
export type FokosMigrationAckRequest = { repartitionId: string; target: FokosPartitionRef };
export type FokosMigrationPage =
	| { phase: "overrides"; overrides: { hashKey: KeyBytes }[]; nextCursor: FokosMigrationCursor | null }
	| { phase: "host"; page: unknown; nextCursor: FokosMigrationCursor | null };
export type FokosExecuteLocalRequest = { op: string; repartitionId: string; caller: FokosPartitionRef; request: unknown };
export type FokosRequestPromotionRequest = { target: FokosPartitionRef; hashKey: KeyBytes; data?: unknown };
export type FokosStatusRequest = { cursor?: unknown };
export type FokosStatusPage = { entries: unknown[]; nextCursor: unknown | null };
export type FokosPrepareDestroyRequest = { context?: FokosRouteContext<unknown> };

export interface FokosShardingRpc {
	fokosInit(req: FokosInitRequest): Promise<void>;
	fokosStartImport(req: FokosStartImportRequest): Promise<void>;
	fokosMigrationPull(req: FokosMigrationPullRequest): Promise<FokosMigrationPage>;
	fokosMigrationAck(req: FokosMigrationAckRequest): Promise<void>;
	fokosExecuteLocal(req: FokosExecuteLocalRequest): Promise<FokosEnvelope<unknown>>;
	fokosRequestPromotion(req: FokosRequestPromotionRequest): Promise<FokosRequestPromotionResult>;
	fokosStatus(req: FokosStatusRequest): Promise<FokosStatusPage>;
	fokosPrepareDestroy(req: FokosPrepareDestroyRequest): Promise<void>;
	fokosDestroy(): Promise<void>;
	alarm(info: AlarmInvocationInfo): Promise<void>;
}

// ─── the runtime ─────────────────────────────────────────────────────────────

export declare class FokosShardingRuntime<TPolicy, Ops extends FokosOperationSpec> implements FokosShardingRpc {
	constructor(
		opts: FokosRuntimeOptions<TPolicy> & { ctx: DurableObjectState; hooks: FokosShardingHooks<TPolicy>; operations: FokosOperations<Ops> },
	);

	dispatch<K extends keyof Ops & string>(
		op: K,
		routeCtx: FokosRouteContext<TPolicy>,
		req: Ops[K]["req"],
	): Promise<FokosEnvelope<Ops[K]["res"]>>;

	identity(): FokosPartitionIdentity;
	policy(): TPolicy;
	routeContext(): FokosRouteContext<TPolicy>;
	lifecycle(): FokosLifecycle;
	owns(key: RouteKey): boolean;
	resolveOwner(key: RouteKey): FokosOwner;
	children(): FokosChild[];
	rangeVisits(input: FokosRangeInput): FokosRangeVisit[];
	forward<K extends keyof Ops & string>(target: FokosPartitionRef, op: K, req: Ops[K]["req"]): Promise<FokosEnvelope<Ops[K]["res"]>>;
	forwardRangeVisit<K extends keyof Ops & string>(visit: FokosRangeVisit, op: K, req: Ops[K]["req"]): Promise<FokosEnvelope<Ops[K]["res"]>>;

	requestSplitEvaluation(): void;
	requestPromotion(hashKey: KeyBytes, data?: unknown): Promise<FokosRequestPromotionResult>;
	scheduleJob(name: string, runAt: number): void;
	runDueWork(info?: AlarmInvocationInfo): Promise<void>;

	fokosInit(req: FokosInitRequest): Promise<void>;
	fokosStartImport(req: FokosStartImportRequest): Promise<void>;
	fokosMigrationPull(req: FokosMigrationPullRequest): Promise<FokosMigrationPage>;
	fokosMigrationAck(req: FokosMigrationAckRequest): Promise<void>;
	fokosExecuteLocal(req: FokosExecuteLocalRequest): Promise<FokosEnvelope<unknown>>;
	fokosRequestPromotion(req: FokosRequestPromotionRequest): Promise<FokosRequestPromotionResult>;
	fokosStatus(req: FokosStatusRequest): Promise<FokosStatusPage>;
	fokosPrepareDestroy(req: FokosPrepareDestroyRequest): Promise<void>;
	fokosDestroy(): Promise<void>;
	alarm(info: AlarmInvocationInfo): Promise<void>;
}

// ─── the Worker-side router ──────────────────────────────────────────────────

export declare class FokosRouter<TPolicy> {
	constructor(topology: FokosTopology, rangeConfig: FokosRangeConfig, policy: TPolicy);
	rootContext(hashKey: KeyBytes): FokosRouteContext<TPolicy>;
	allRoots(): FokosRouteContext<TPolicy>[];
	unwrap<T>(envelope: FokosEnvelope<T>): { value: T; routing: FokosPublicRouting };
	walk(
		stub: (ctx: FokosRouteContext<TPolicy>, doName: string) => DurableObjectStub,
		visit: (ctx: FokosRouteContext<TPolicy>, stub: DurableObjectStub) => Promise<void>,
	): Promise<void>;
}

/** Ordinary code that is not part of the sharding surface. Prototype helper for a body that is not written. */
export function todo<T = never>(what: string): T {
	throw new Error(`prototype: ${what}`);
}
