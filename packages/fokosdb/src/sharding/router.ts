import { PartitionIdHelper, hashRootIndex } from "./partition-id.js";
import type { KeyBytes } from "./key-codec.js";
import {
	validateRangeConfig,
	validateTopology,
	type FokosPartitionRef,
	type FokosRangeConfig,
	type FokosRouteContext,
	type FokosTopology,
} from "./route-context.js";
import type { FokosPrepareDestroyRequest, FokosStatusCursor, FokosStatusPage, FokosStatusRequest } from "./repartition-types.js";
import { assertExists } from "../shared/tsutils.js";

/** What `walk` calls on every partition it reaches. A host stub has these two methods and its own. */
export type FokosWalkStub = {
	fokosPrepareDestroy(req: FokosPrepareDestroyRequest): Promise<void>;
	fokosStatus(req: FokosStatusRequest): Promise<FokosStatusPage>;
};

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
		if (cached) return cached;
		const { doName, opaque } = PartitionIdHelper.fromHashIdxs(this.topology.shardGroup, [idx]).encode(true);
		assertExists(doName);
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
	 * The destroy traversal. For every root, and then post-order for every target: fence the partition
	 * with `fokosPrepareDestroy` (with the root context on a root only), read every `fokosStatus` page
	 * after the fence is set, visit each target, then call `visit` on the partition.
	 *
	 * The fence comes first, so nothing adds a target after the traversal reads the last page. One
	 * range root is the target of a promotion on every hash child that inherited the key, so the
	 * traversal reaches one partition from more than one place; it dedupes by `doName`.
	 */
	async walk<S extends FokosWalkStub>(
		stub: (ctx: FokosRouteContext<TPolicy>, doName: string) => S,
		visit: (ctx: FokosRouteContext<TPolicy>, stub: S) => Promise<void>,
	): Promise<void> {
		const visited = new Set<string>();

		const walkPartition = async (ref: FokosPartitionRef, rootContext?: FokosRouteContext<TPolicy>): Promise<void> => {
			if (visited.has(ref.doName)) return;
			visited.add(ref.doName);
			const ctx = rootContext ?? { ...this.#root(0), ...ref };
			const s = stub(ctx, ref.doName);
			await s.fokosPrepareDestroy({ rootContext });
			let cursor: FokosStatusCursor | null = null;
			do {
				const page = await s.fokosStatus({ cursor, rootContext });
				for (const entry of page.entries) {
					if (entry.target) await walkPartition(entry.target.ref);
				}
				cursor = page.nextCursor;
			} while (cursor !== null);
			await visit(ctx, s);
		};

		for (const root of this.allRoots()) {
			await walkPartition(root, root);
		}
	}
}
