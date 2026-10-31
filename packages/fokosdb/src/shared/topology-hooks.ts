/**
 * The hooks that connect a table to its topology: the admin outbox of a partition or a coordinator,
 * and the topology cache options of a client or a runtime. A table with no `nsAdmin` gets no outbox,
 * and a table with no `topologyEntrypoint` gets no topology cache.
 */
import { env, exports } from "cloudflare:workers";
import type { FokosAdminOutbox } from "../sharding/runtime-types.js";
import type { FokosTopologyCacheOptions } from "../sharding/topology-cache.js";
import { topologyAdminAddress, topologyAdminStub, type DataNamespaceKey, type TopologyAdminAddress } from "./do-stubs.js";
import type { FokosDBStubContext } from "./partition-context.js";

/** The refresh settings of a topology cache. Absent values take the defaults of the cache. */
export type TopologyRefreshSettings = { refreshMs?: number; refreshTimeoutMs?: number };

type TopologyEntrypointFactory = (opts: { props: TopologyAdminAddress }) => Fetcher;

/**
 * The admin outbox of a partition or a coordinator of `ctx`, or undefined when the table has no `nsAdmin`.
 * `binding` is the namespace that holds the shard group of `ctx`.
 */
export function adminOutboxFor(ctx: FokosDBStubContext, binding: DataNamespaceKey): FokosAdminOutbox | undefined {
	const address = topologyAdminAddress(env, ctx, binding);
	if (address === undefined) {
		return undefined;
	}
	// The outbox holds one event type now: the report of a completed repartition of this shard group.
	return { send: async (entry) => await topologyAdminStub(env, address).registerRepartition(entry.event) };
}

/**
 * The topology cache options of the shard group of `ctx`, or undefined when the table has no
 * `topologyEntrypoint`. `binding` is the namespace that holds the shard group. The fetch calls the
 * entrypoint through the `exports` of the Worker.
 */
export function topologyCacheFor(
	ctx: FokosDBStubContext,
	binding: DataNamespaceKey,
	waitUntil: (promise: Promise<unknown>) => void,
	settings: TopologyRefreshSettings = {},
): FokosTopologyCacheOptions | undefined {
	const { topologyEntrypoint } = ctx.policy;
	const address = topologyEntrypoint === undefined ? undefined : topologyAdminAddress(env, ctx, binding);
	if (topologyEntrypoint === undefined || address === undefined) {
		return undefined;
	}
	return {
		fetch: async (path, signal) => {
			const entrypoint = (exports as unknown as Record<string, TopologyEntrypointFactory | undefined>)[topologyEntrypoint];
			if (entrypoint === undefined) {
				throw new Error(`the Worker does not export the topology entrypoint ${topologyEntrypoint}`);
			}
			return await entrypoint({ props: address }).fetch(`http://fokos-topology${path}`, { signal });
		},
		waitUntil,
		// The entrypoint answers by the admin address, so the isolate cache keys by it too.
		scope: JSON.stringify([address.nsAdmin, address.jurisdiction ?? null, address.name]),
		...(settings.refreshMs === undefined ? {} : { refreshMs: settings.refreshMs }),
		...(settings.refreshTimeoutMs === undefined ? {} : { refreshTimeoutMs: settings.refreshTimeoutMs }),
	};
}

/**
 * The topology cache options of a Durable Object, made again only when its policy or its settings
 * change. The runtime asks for them at each forward, and the same object keeps the cost low.
 */
export class TopologyCacheMemo {
	#policy: object | undefined;
	#settings: TopologyRefreshSettings = {};
	#options: FokosTopologyCacheOptions | undefined;

	/** `binding` selects the namespace that holds the shard group of the Durable Object from its policy. */
	constructor(
		private readonly waitUntil: (promise: Promise<unknown>) => void,
		private readonly binding: (policy: FokosDBStubContext["policy"]) => DataNamespaceKey,
	) {}

	get(ctx: FokosDBStubContext, settings: TopologyRefreshSettings): FokosTopologyCacheOptions | undefined {
		if (
			ctx.policy !== this.#policy ||
			settings.refreshMs !== this.#settings.refreshMs ||
			settings.refreshTimeoutMs !== this.#settings.refreshTimeoutMs
		) {
			this.#policy = ctx.policy;
			this.#settings = settings;
			this.#options = topologyCacheFor(ctx, this.binding(ctx.policy), this.waitUntil, settings);
		}
		return this.#options;
	}
}
