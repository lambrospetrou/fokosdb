import { tryWhile } from "durable-utils/retries";
import { isDestroyAbortError } from "../shared/cf-utils.js";
import { FokosError, type FokosCodeDef } from "../shared/errors.js";
import invariant from "../shared/invariant.js";
import { routedError } from "./envelope.js";
import { SHARDING_ROUTING_CODES, SHARDING_UNAVAILABLE_CODES } from "./errors.js";
import type { KeyBytes } from "./key-codec.js";
import type { FokosImportState, FokosPromotionsCursor, FokosShardingRpc, FokosStatusEntry, RouteKey } from "./repartition-types.js";
import { isRangePartition, resolveDescendantHashPartitionContext, resolveRangePartitionContext } from "./partition-id.js";
import { refOf, type FokosPartitionRef, type FokosRangeConfig, type FokosRouteContext, type FokosTopology } from "./route-context.js";
import { FokosRouter, publicRouting } from "./router.js";
import type { FokosEnvelope, FokosOperationSpec, FokosPublicRouting, FokosRangeInput } from "./runtime-types.js";
import type { RepartitionKind } from "./sharding-store.js";
import { cachedHashTopology, cachedRangeTopology, cutTopologyHint, type FokosTopologyCacheOptions } from "./topology-cache.js";
import {
	findLeaf,
	maybePromoted,
	rangeBoundaryAt,
	rangeCutKey,
	rangeIntervalLookup,
	rangePointLookup,
	type RangeTarget,
	type RangeTopologyView,
} from "./topology-codec.js";

export type FokosRetryPolicy = {
	/** Called after each failed attempt. `nextAttempt` is 2 for the first retry. */
	shouldRetry(this: void, err: unknown, nextAttempt: number): boolean;
	/** Default 100. Must be less than `maxDelayMs`, as `tryWhile` requires. */
	baseDelayMs?: number;
	/** Default 2_000. */
	maxDelayMs?: number;
};

export type FokosCallOptions = {
	/** Replaces the retry policy of the client for this call. */
	retry?: FokosRetryPolicy;
};

export type FokosShardingClientOptions<TPolicy> = {
	topology: FokosTopology;
	rangeConfig: FokosRangeConfig;
	policy: TPolicy;
	/** The version of `rangeConfig` and `policy`. See `FokosRouteContext.policyVersion`. Default: 0. */
	policyVersion?: number;
	/** The contract of `FokosRuntimeOptions.stub`. The stub must also have the `FokosShardingRpc` methods. */
	stub(this: void, ctx: FokosRouteContext<TPolicy>, doName: string): DurableObjectStub;
	/** Absent: the client sends each request once. */
	retry?: FokosRetryPolicy;
	/**
	 * The encoded topology of the shard group. With it, a request enters at the deepest known
	 * partition of its key. Absent: every request enters at a root.
	 */
	topologyCache?: FokosTopologyCacheOptions;
};

/** The cost of one client call over every attempt. */
export type FokosCallCost = {
	/** The RPCs that the client sent: the first send and each retry. */
	clientRpcs: number;
	/** The sum of `forwardCount` over every attempt, the failed attempts included. */
	totalForwardCount: number;
};

export type FokosCallResult<T> = FokosCallCost & { value: T; routing: FokosPublicRouting };

/** The keys of one entry. `indexes` are the positions of the keys in the input of `resolveAll`. */
export type FokosResolvedGroup<TPolicy> = { ctx: FokosRouteContext<TPolicy>; indexes: number[] };

export type FokosWalkNode<TPolicy> = {
	ctx: FokosRouteContext<TPolicy>;
	kind: "hash" | "range";
	role: "owner" | "router";
	/** How the walk reached this partition. Null for a root. */
	parent: { ref: FokosPartitionRef; via: RepartitionKind } | null;
	importState: FokosImportState | null;
};

type OperationStub = Record<string, (ctx: unknown, req: unknown) => Promise<FokosEnvelope<unknown>>>;

/**
 * Removes the fields of `FokosCallCost` that a failed call put on `e`. A caller that stores an error, or
 * passes it on as its own, calls it first: `FokosError.wrap` copies the own properties of a foreign
 * error into `attributes`.
 */
export function dropCallCost(e: unknown): void {
	if (typeof e === "object" && e !== null) {
		delete (e as Partial<FokosCallCost>).clientRpcs;
		delete (e as Partial<FokosCallCost>).totalForwardCount;
	}
}

/** A status entry with its target. */
type LinkedEntry = FokosStatusEntry & { target: NonNullable<FokosStatusEntry["target"]> };

/** The range lookup of one request: a point lookup of a sort key, or a lookup of a whole interval. */
type RangeLookup = (view: RangeTopologyView, cut: ReadonlySet<number>) => RangeTarget | null;

export type FokosResolveOptions = {
	/** False: give only a partition that exists. Default: true. */
	speculative?: boolean;
};

/** The entry to send to after a miss at an entry that came from the topology, or null when the error is not that miss. */
type MissFallback<TPolicy> = (e: unknown) => FokosRouteContext<TPolicy> | null;

/**
 * The most topology fallbacks of one call. Each fallback goes to a shallower partition of the same
 * topology version, so a real tree needs fewer. A refresh between two fallbacks can give a deeper
 * entry again, and the bound stops that loop.
 */
const MAX_TOPOLOGY_FALLBACKS = 16;

/** True when the partition `doName` answered that it has no identity. Nothing ran on it. */
function notInitializedAt(e: unknown, code: FokosCodeDef, doName: string): boolean {
	return FokosError.isCode(e, code) && e.attributes.doName === doName;
}

/** True when a target in `awaiting_data` refused the request before any handler ran. */
function awaitingData(e: unknown): boolean {
	return FokosError.isCode(e, SHARDING_UNAVAILABLE_CODES.partition_migrating) && e.attributes.importState === "awaiting_data";
}

/**
 * The caller side of one shard group. It resolves the entry partition of a request, gets the stub,
 * sends the operation, retries by the policy of the caller, and removes the internal route hints.
 *
 * Without a topology cache every entry is a root, and the partitions forward from there. With one, a
 * request enters at the deepest known partition of its key, and a miss there sends it again to the
 * nearest known ancestor. A topology hint changes the latency only, never the result.
 *
 * The client never retries by itself: a refusal does not prove that nothing applied, and only the
 * caller knows if its operation is idempotent. A client is cheap to make, so a caller can make one
 * for each request.
 */
export class FokosShardingClient<TPolicy, Ops extends FokosOperationSpec> {
	#router: FokosRouter<TPolicy>;
	#stub: FokosShardingClientOptions<TPolicy>["stub"];
	#retry: FokosRetryPolicy | undefined;
	#firstRoot: FokosRouteContext<TPolicy> | undefined;
	#topologyCache: FokosTopologyCacheOptions | undefined;
	/** The fallback of each entry that came from the topology, by the context object that `resolve` returned. */
	#fallbacks = new WeakMap<FokosRouteContext<TPolicy>, MissFallback<TPolicy>>();

	constructor(opts: FokosShardingClientOptions<TPolicy>) {
		this.#router = new FokosRouter(opts.topology, opts.rangeConfig, opts.policy, opts.policyVersion);
		this.#stub = opts.stub;
		this.#retry = opts.retry;
		this.#topologyCache = opts.topologyCache;
	}

	// ─── resolve: no I/O, local only ──────────────────────────────────────────────────

	/**
	 * The deepest known partition of the key. It reads the topology cache, and does no I/O.
	 *
	 * `speculative` false gives only a partition that exists: it does not send a key with a Bloom hit
	 * and no cached range entry to the range root of the key. Use it for an address that another
	 * client instance sends to later, because only this instance can fall back after a miss.
	 */
	resolve(key: RouteKey, opts?: FokosResolveOptions): FokosRouteContext<TPolicy> {
		return this.#resolveKey(key.hashKey, (view, cut) => rangePointLookup(view, key.sortKey, cut), true, opts?.speculative ?? true);
	}

	/** The deepest known partition that holds the whole interval. That partition plans the range frontier. */
	resolveRange(input: FokosRangeInput): FokosRouteContext<TPolicy> {
		return this.#resolveKey(input.hashKey, (view, cut) => rangeIntervalLookup(view, input.interval, cut), true, true);
	}

	/**
	 * A key with a Bloom hit enters its range tree: at the deepest known range partition, or at the
	 * range root when the range entry is not cached yet. Every other key enters at the deepest known
	 * hash partition. `bloom` false skips the range tree, after the range root answered that it does
	 * not exist or has no data yet. `speculative` false skips the range root of a key with no range
	 * entry, which can be a Bloom false positive. Every other entry names a partition that exists,
	 * because the keeper records only completed repartitions. The hash leaf of a promoted key forwards
	 * to its range tree.
	 */
	#resolveKey(hashKey: KeyBytes, rangeLookup: RangeLookup, bloom: boolean, speculative: boolean): FokosRouteContext<TPolicy> {
		const root = this.#router.rootContext(hashKey);
		const opts = this.#topologyCache;
		if (!opts) {
			return root;
		}
		const topology = this.#router.topology;
		const hash = cachedHashTopology(opts, topology);
		if (!hash) {
			return root;
		}
		if (bloom && maybePromoted(hash.view, hashKey)) {
			const range = cachedRangeTopology(opts, topology.shardGroup, hashKey);
			// A range entry with no segment says that the Bloom hit is a false positive.
			if (range ? range.view.depths.length > 0 : speculative) {
				const target = range ? rangeLookup(range.view, range.cut) : { start: 0, end: 1, rank: 0 };
				if (target) {
					const start = range ? rangeBoundaryAt(range.view, target.start) : null;
					const end = range ? rangeBoundaryAt(range.view, target.end) : null;
					const ctx = resolveRangePartitionContext(root, hashKey, start, end);
					this.#fallbacks.set(ctx, (e) => {
						const missing = notInitializedAt(e, SHARDING_ROUTING_CODES.range_partition_not_initialized, ctx.doName);
						if (target.rank === 0 && (missing || awaitingData(e))) {
							return this.#resolveKey(hashKey, rangeLookup, false, speculative);
						}
						if (!missing || !range) {
							return null;
						}
						cutTopologyHint(range, rangeCutKey(range.view, target));
						return this.#resolveKey(hashKey, rangeLookup, bloom, speculative);
					});
					return ctx;
				}
			}
		}
		const leaf = findLeaf(hash.view, topology.rootTreesN, topology.hashSplitN, hashKey, hash.cut);
		if (leaf.path.length === 0) {
			return root;
		}
		const ctx = resolveDescendantHashPartitionContext(root, leaf.path);
		this.#fallbacks.set(ctx, (e) => {
			if (!notInitializedAt(e, SHARDING_ROUTING_CODES.hash_partition_not_initialized, ctx.doName)) {
				return null;
			}
			cutTopologyHint(hash, leaf.pos);
			return this.#resolveKey(hashKey, rangeLookup, bloom, speculative);
		});
		return ctx;
	}

	/**
	 * Groups the keys by the entry that `resolve` gives, in the order of the first key of each group.
	 * The keys of one group keep their input order. A group can hold keys of different owners, so the
	 * operation must fan out below the entry.
	 */
	resolveAll(keys: readonly RouteKey[], opts?: FokosResolveOptions): FokosResolvedGroup<TPolicy>[] {
		const groups = new Map<string, FokosResolvedGroup<TPolicy>>();
		keys.forEach((key, index) => {
			const ctx = this.resolve(key, opts);
			const group = groups.get(ctx.doName);
			if (group) {
				group.indexes.push(index);
			} else {
				groups.set(ctx.doName, { ctx, indexes: [index] });
			}
		});
		return [...groups.values()];
	}

	// ─── resolve, send, retry, unwrap ─────────────────────────────────────────

	async point<K extends keyof Ops & string>(
		op: K,
		key: RouteKey,
		req: Ops[K]["req"],
		opts?: FokosCallOptions,
	): Promise<FokosCallResult<Ops[K]["res"]>> {
		return await this.send(op, this.resolve(key), req, [key], opts);
	}

	async range<K extends keyof Ops & string>(
		op: K,
		input: FokosRangeInput,
		req: Ops[K]["req"],
		opts?: FokosCallOptions,
	): Promise<FokosCallResult<Ops[K]["res"]>> {
		return await this.send(op, this.resolveRange(input), req, [], opts);
	}

	/**
	 * Sends to an entry that the caller selected. `keys` are the route keys of the request. The client
	 * does not read them now. An entry that `resolve`, `resolveRange`, or `resolveAll` gave falls back
	 * to a shallower partition after a miss of its topology hint.
	 *
	 * On an error, the client replaces the `routing` of the error with the public form, adds the
	 * fields of `FokosCallCost` to the error object, and throws the same error.
	 */
	async send<K extends keyof Ops & string>(
		op: K,
		entry: FokosRouteContext<TPolicy>,
		req: Ops[K]["req"],
		_keys: readonly RouteKey[],
		opts?: FokosCallOptions,
	): Promise<FokosCallResult<Ops[K]["res"]>> {
		const cost: FokosCallCost = { clientRpcs: 0, totalForwardCount: 0 };
		// A miss at an entry from the topology moves the entry to a shallower partition. A retry of the
		// policy starts at the entry that the last miss gave.
		let target = entry;
		let fallbacks = 0;
		const attempt = async (): Promise<FokosEnvelope<Ops[K]["res"]>> => {
			for (;;) {
				cost.clientRpcs += 1;
				const stub = this.#stub(target, target.doName) as unknown as OperationStub;
				try {
					const result = (await stub[op](target, req)) as FokosEnvelope<Ops[K]["res"]>;
					cost.totalForwardCount += result.routing.forwardCount;
					return result;
				} catch (e) {
					cost.totalForwardCount += routedError(e)?.routing.forwardCount ?? 0;
					// Each miss is refused before any handler runs. A fallback that names the same partition
					// again (the cut set is full) or that goes past the bound ends the fallback.
					const next = fallbacks < MAX_TOPOLOGY_FALLBACKS ? (this.#fallbacks.get(target)?.(e) ?? null) : null;
					if (!next || next.doName === target.doName) {
						throw e;
					}
					fallbacks += 1;
					target = next;
				}
			}
		};
		const policy = opts?.retry ?? this.#retry;
		try {
			const result = policy
				? await tryWhile(attempt, policy.shouldRetry, { baseDelayMs: policy.baseDelayMs ?? 100, maxDelayMs: policy.maxDelayMs ?? 2_000 })
				: await attempt();
			return { ...this.#router.unwrap(result), ...cost };
		} catch (e) {
			const routed = routedError(e);
			if (routed) {
				Object.assign(routed, { routing: publicRouting(routed.routing) });
			}
			if (typeof e === "object" && e !== null) {
				Object.assign(e, cost);
			}
			throw e;
		}
	}

	// ─── traversal ────────────────────────────────────────────────────────────

	/**
	 * Reads the partition tree and changes nothing. It yields a parent before its children, and the
	 * caller can stop early. The result is a best-effort snapshot: a split during the walk can add a
	 * target that the walk does not see.
	 *
	 * The walk calls `fokosStatus` without a root context, so it never creates an identity, and it
	 * skips a partition that has none. The status call still starts a cold Durable Object.
	 *
	 * `scope: "owners"` yields only the partitions that own a slice now: no router, and no target
	 * whose repartition has not cut over. A walk is never a source of row totals, because the stored
	 * rows do not match ownership while a repartition runs.
	 */
	async *walk(opts: { scope?: "all" | "owners" } = {}): AsyncGenerator<FokosWalkNode<TPolicy>> {
		const seen = new Set<string>();
		for (const root of this.#router.allRoots()) {
			yield* this.#walkFrom(root, null, true, opts.scope === "owners", seen);
		}
	}

	async *#walkFrom(
		ctx: FokosRouteContext<TPolicy>,
		parent: FokosWalkNode<TPolicy>["parent"],
		cutOver: boolean,
		ownersOnly: boolean,
		seen: Set<string>,
	): AsyncGenerator<FokosWalkNode<TPolicy>> {
		// One range root can be the promotion target of more than one hash partition.
		if (seen.has(ctx.doName)) {
			return;
		}
		seen.add(ctx.doName);
		const stub = this.#rpc(ctx);
		const status = await stub.fokosStatus({});
		if (!status.initialized) {
			return;
		}
		const role = status.role;
		invariant(role, "fokos/client.walk: a partition with an identity has a role");
		if (!ownersOnly || (role === "owner" && cutOver)) {
			yield { ctx, kind: isRangePartition(ctx) ? "range" : "hash", role, parent, importState: status.importState };
		}
		for await (const { repartition, target } of this.#targets(stub, status.split)) {
			const via = { ref: refOf(ctx), via: repartition.kind };
			const targetCutOver = repartition.state !== "queued" && repartition.state !== "planned";
			yield* this.#walkFrom(this.#targetContext(target.ref), via, targetCutOver, ownersOnly, seen);
		}
	}

	/**
	 * Fences and deletes every partition: routers, owners, and targets before their cutover. For every
	 * root, and then post-order for every target: fence the partition with `fokosPrepareDestroy` (with
	 * the root context on a root only), read `fokosStatus` and every `fokosPromotions` page after the
	 * fence is set, visit each target, then call `fokosDestroy`.
	 *
	 * The fence comes first, so nothing adds a target after the traversal reads the last page. A
	 * destroy that stops halfway can run again, because each parent that remains still knows its
	 * children.
	 */
	async destroy(opts: { onDestroyed?(ref: FokosPartitionRef): void } = {}): Promise<void> {
		const visited = new Set<string>();

		const destroyPartition = async (ctx: FokosRouteContext<TPolicy>, rootContext?: FokosRouteContext<TPolicy>): Promise<void> => {
			// One range root can be the promotion target of more than one hash partition.
			if (visited.has(ctx.doName)) {
				return;
			}
			visited.add(ctx.doName);
			const stub = this.#rpc(ctx);
			await stub.fokosPrepareDestroy({ rootContext });
			const status = await stub.fokosStatus({ rootContext });
			for await (const { target } of this.#targets(stub, status.split)) {
				await destroyPartition(this.#targetContext(target.ref));
			}
			try {
				await stub.fokosDestroy();
			} catch (e) {
				if (!isDestroyAbortError(e)) {
					throw e;
				}
			}
			opts.onDestroyed?.(refOf(ctx));
		};

		for (const root of this.#router.allRoots()) {
			await destroyPartition(root, root);
		}
	}

	/**
	 * The entries with a target: the split targets first, then each promotion target. The promotions
	 * pages are read one at a time, so a caller that stops early reads no more pages.
	 */
	async *#targets(stub: FokosShardingRpc, split: FokosStatusEntry[]): AsyncGenerator<LinkedEntry> {
		const withTarget = (e: FokosStatusEntry): e is LinkedEntry => e.target !== null;
		yield* split.filter(withTarget);
		let cursor: FokosPromotionsCursor | null = null;
		do {
			const page = await stub.fokosPromotions({ cursor });
			yield* page.entries.filter(withTarget);
			cursor = page.nextCursor;
		} while (cursor !== null);
	}

	#rpc(ctx: FokosRouteContext<TPolicy>): FokosShardingRpc {
		return this.#stub(ctx, ctx.doName) as unknown as FokosShardingRpc;
	}

	/** The context of a partition below a root. The shard group config is the same for every partition. */
	#targetContext(ref: FokosPartitionRef): FokosRouteContext<TPolicy> {
		this.#firstRoot ??= this.#router.allRoots()[0];
		return { ...this.#firstRoot, ...refOf(ref) };
	}
}
