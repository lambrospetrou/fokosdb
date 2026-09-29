import { PartitionIdHelper, hashRootIndex } from "./partition-id.js";
import type { KeyBytes } from "./key-codec.js";
import {
	validateRangeConfig,
	validateTopology,
	type FokosRangeConfig,
	type FokosRouteContext,
	type FokosTopology,
} from "./route-context.js";
import type { FokosEnvelope, FokosPublicRoute, FokosPublicRouting, FokosRouteNode, FokosRouting } from "./runtime-types.js";

/**
 * The Worker-side router of one shard group. It hashes a hash key to a root partition and builds
 * the route context of that root; the partitions forward from there. It caches nothing but the
 * root contexts, so a Worker can build one per request.
 */
export class FokosRouter<TPolicy> {
	#roots: Map<number, FokosRouteContext<TPolicy>> = new Map();

	constructor(
		readonly topology: FokosTopology,
		readonly rangeConfig: FokosRangeConfig,
		readonly policy: TPolicy,
	) {
		validateTopology(topology);
		validateRangeConfig(rangeConfig);
	}

	/** The root partition that owns `hashKey`. Keys arrive already encoded. */
	rootContext(hashKey: KeyBytes): FokosRouteContext<TPolicy> {
		return this.#root(hashRootIndex(hashKey, this.topology.rootTreesN));
	}

	/** Every root partition. A whole-tree traversal starts from these. */
	allRoots(): FokosRouteContext<TPolicy>[] {
		return Array.from({ length: this.topology.rootTreesN }, (_, i) => this.#root(i));
	}

	#root(idx: number): FokosRouteContext<TPolicy> {
		const cached = this.#roots.get(idx);
		if (cached) {
			return cached;
		}
		const { doName, opaque } = PartitionIdHelper.hashId(this.topology.shardGroup, idx);
		const ctx: FokosRouteContext<TPolicy> = {
			schema: 2,
			partitionId: opaque,
			doName,
			topology: this.topology,
			rangeConfig: this.rangeConfig,
			policy: this.policy,
		};
		this.#roots.set(idx, ctx);
		return ctx;
	}

	/**
	 * Opens an envelope at the Worker boundary. The internal hints stop here: they are
	 * partition-to-partition routing state, and a client has no use for them.
	 */
	unwrap<T>(envelope: FokosEnvelope<T>): { value: T; routing: FokosPublicRouting } {
		return { value: envelope.value, routing: publicRouting(envelope.routing) };
	}
}

/** The routing without the internal hints. */
export function publicRouting(routing: FokosRouting): FokosPublicRouting {
	return { servedBy: routing.servedBy.map(publicRoute), forwardCount: routing.forwardCount };
}

function publicRoute(node: FokosRouteNode): FokosPublicRoute {
	const { ref, actorId, hashDepth, rangeDepth, role } = node;
	return { ref, actorId, hashDepth, rangeDepth, role };
}
