import { tryWhile } from "durable-utils/retries";
import { isDestroyAbortError } from "../shared/cf-utils.js";
import invariant from "../shared/invariant.js";
import { routedError } from "./envelope.js";
import type { FokosImportState, FokosShardingRpc, FokosStatusCursor, RouteKey } from "./repartition-types.js";
import { isRangePartition } from "./partition-id.js";
import {
	refOf,
	type FokosPartitionRef,
	type FokosRangeConfig,
	type FokosRouteContext,
	type FokosTopology,
} from "./route-context.js";
import { FokosRouter, publicRouting } from "./router.js";
import type { FokosEnvelope, FokosOperationSpec, FokosPublicRouting, FokosRangeInput } from "./runtime-types.js";
import type { RepartitionKind } from "./sharding-store.js";

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
	/** The contract of `FokosRuntimeOptions.stub`. The stub must also have the `FokosShardingRpc` methods. */
	stub(this: void, ctx: FokosRouteContext<TPolicy>, doName: string): DurableObjectStub;
	/** Absent: the client sends each request once. */
	retry?: FokosRetryPolicy;
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

/**
 * The caller side of one shard group. It resolves the entry partition of a request, gets the stub,
 * sends the operation, retries by the policy of the caller, and removes the internal route hints.
 *
 * Every entry is a root now. The partitions forward from there.
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

	constructor(opts: FokosShardingClientOptions<TPolicy>) {
		this.#router = new FokosRouter(opts.topology, opts.rangeConfig, opts.policy);
		this.#stub = opts.stub;
		this.#retry = opts.retry;
	}

	// ─── resolve: no I/O, local only ──────────────────────────────────────────────────

	resolve(key: RouteKey): FokosRouteContext<TPolicy> {
		// TODO Leapfrog to deep levels based on cached topology.
		return this.#router.rootContext(key.hashKey);
	}

	/** The entry partition plans the range frontier. */
	resolveRange(input: FokosRangeInput): FokosRouteContext<TPolicy> {
		// TODO Leapfrog to deep levels based on cached topology.
		return this.#router.rootContext(input.hashKey);
	}

	/**
	 * Groups the keys by the entry that `resolve` gives, in the order of the first key of each group.
	 * The keys of one group keep their input order. A group can hold keys of different owners, so the
	 * operation must fan out below the entry.
	 */
	resolveAll(keys: readonly RouteKey[]): FokosResolvedGroup<TPolicy>[] {
		// TODO Leapfrog to deep levels based on cached topology.
		const groups = new Map<string, FokosResolvedGroup<TPolicy>>();
		keys.forEach((key, index) => {
			const ctx = this.resolve(key);
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
	 * does not read them now.
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
		const attempt = async (): Promise<FokosEnvelope<Ops[K]["res"]>> => {
			cost.clientRpcs += 1;
			const stub = this.#stub(entry, entry.doName) as unknown as OperationStub;
			try {
				const result = (await stub[op](entry, req)) as FokosEnvelope<Ops[K]["res"]>;
				cost.totalForwardCount += result.routing.forwardCount;
				return result;
			} catch (e) {
				cost.totalForwardCount += routedError(e)?.routing.forwardCount ?? 0;
				throw e;
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
		let page = await stub.fokosStatus({ cursor: null });
		if (!page.initialized) {
			return;
		}
		const role = page.role;
		invariant(role, "fokos/client.walk: a partition with an identity has a role");
		if (!ownersOnly || (role === "owner" && cutOver)) {
			yield { ctx, kind: isRangePartition(ctx) ? "range" : "hash", role, parent, importState: page.importState };
		}
		for (;;) {
			for (const { repartition, target } of page.entries) {
				if (target) {
					const via = { ref: refOf(ctx), via: repartition.kind };
					const targetCutOver = repartition.state !== "queued" && repartition.state !== "planned";
					yield* this.#walkFrom(this.#targetContext(target.ref), via, targetCutOver, ownersOnly, seen);
				}
			}
			if (page.nextCursor === null) {
				return;
			}
			page = await stub.fokosStatus({ cursor: page.nextCursor });
		}
	}

	/**
	 * Fences and deletes every partition: routers, owners, and targets before their cutover. For every
	 * root, and then post-order for every target: fence the partition with `fokosPrepareDestroy` (with
	 * the root context on a root only), read every `fokosStatus` page after the fence is set, visit
	 * each target, then call `fokosDestroy`.
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
			let cursor: FokosStatusCursor | null = null;
			do {
				const page = await stub.fokosStatus({ cursor, rootContext });
				for (const entry of page.entries) {
					if (entry.target) {
						await destroyPartition(this.#targetContext(entry.target.ref));
					}
				}
				cursor = page.nextCursor;
			} while (cursor !== null);
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

	#rpc(ctx: FokosRouteContext<TPolicy>): FokosShardingRpc {
		return this.#stub(ctx, ctx.doName) as unknown as FokosShardingRpc;
	}

	/** The context of a partition below a root. The shard group config is the same for every partition. */
	#targetContext(ref: FokosPartitionRef): FokosRouteContext<TPolicy> {
		this.#firstRoot ??= this.#router.allRoots()[0];
		return { ...this.#firstRoot, ...refOf(ref) };
	}
}
