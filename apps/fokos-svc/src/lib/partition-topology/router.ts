import { env } from "cloudflare:workers";
import type { PartitionContext, PartitionContextResolved } from "./partition-context.js";
import { PartitionIdHelper, hashRootIndex, resolveRangePartitionContext } from "./partition-id.js";
import { KeyCodec } from "./key-codec.js";
import type { SplitStatusKVItem } from "./split-state.js";
import { assertExists } from "../tsutils.js";

export interface PartitionTopologyRouter {
	partitionContext(): PartitionContext;

	/**
	 * Used by the FokosDB clients and anyone that wants to route a hashKey/sortKey to the appropriate partition.
	 * @param hashKey
	 * @param sortKey
	 */
	pickPartition(hashKey: string, sortKey?: string): { doId: DurableObjectId; partitionContext: PartitionContextResolved };

	/**
	 * Returns a PartitionContextResolved for every root partition in the topology.
	 * Used as the starting points for full-tree traversal (e.g. destroy).
	 */
	rootPartitionContexts(): PartitionContextResolved[];

	/**
	 * Full-tree traversal for destroy: the router owns child-discovery order (children before
	 * their parent, linked range structures before the hash partition that links them) and the
	 * dedup of shared range roots; the caller supplies the two RPC-performing callbacks.
	 */
	traverseForDestroy(
		getStatus: (ctx: PartitionContextResolved) => Promise<{
			splitStatus?: SplitStatusKVItem;
			promotedKeys?: { hashKey: string; status: string }[];
		}>,
		visit: (ctx: PartitionContextResolved) => Promise<void>,
	): Promise<void>;
}

/**
 * Used by the FokosDB to route requests to the right partition DO based on the provided partition context and keys.
 */
export class PartitionTopologyRouterImpl implements PartitionTopologyRouter {
	#_rootContextsCache: Map<number, PartitionContextResolved> = new Map();

	constructor(private readonly basePartitionContext: PartitionContext) {
		// FIXME: This is a placeholder implementation. The actual implementation will depend on the encoding scheme used for the partition topology.
		// this.#topology = ...
	}

	partitionContext(): PartitionContext {
		return this.basePartitionContext;
	}

	/**
	 * Used by the FokosDB clients and anyone that wants to route a hashKey/sortKey to the appropriate partition.
	 */
	pickPartition(hashKey: string, sortKey?: string): { doId: DurableObjectId; partitionContext: PartitionContextResolved } {
		const { doName, partitionIdOpaque } = this.findPartition({ hashKey, sortKey });
		const { ns } = this.basePartitionContext;
		// Use idFromName to ensure the DO itself will have the `.name` populated within itself.
		const doId = env[ns].idFromName(doName);
		// Merge with any partition-specific context if needed.
		const partitionContext: PartitionContextResolved = {
			...this.basePartitionContext,
			doName: doName,
			primaryDoIdStr: doId.toString(),
			partitionId: partitionIdOpaque,
		};

		return {
			doId,
			partitionContext,
		};
	}

	private findPartition({ hashKey, sortKey }: { hashKey: string; sortKey?: string }): {
		doName: string;
		partitionIdOpaque: string;
	} {
		// First find the hash partition!
		// Root tree index first.
		// TEMP (M1 adapter, removed in M5): the router still speaks `string` keys; encode at this
		// boundary so the hash primitives operate on canonical KeyBytes. M5 encodes once at db.ts entry.
		let hIdxs: number[] = [hashRootIndex(KeyCodec.encode(hashKey), this.basePartitionContext.rootTreesN)];

		// TODO: Based on the topology encoding and the topology cache find the right partition.
		// {
		// 	// 1 for the root, then one for each level of the tree until we reach a leaf.
		// 	// The level is used as additional entropy to ensure better distribution of the partitions across the children.
		// 	let level = 1;
		// 	// This should start from the root node and traverse down the tree until it reaches a leaf node,
		// 	// which will be the partition that should handle the request.
		// 	let hNode = this.resolveRootPartitionContext(hIdxs[0]);
		// 	while (hNode.children.length > 0) {
		// 		level++;
		// 		const hChild = hashChildIndex(hashKey, level - 1, hNode.children.length);
		// 		hIdxs.push(hChild);
		// 		hNode = hNode.children[hChild];
		// 	}
		// }

		// TODO: Find the range partition if it exists.

		const { doName, opaque } = new PartitionIdHelper(this.basePartitionContext).appendHashIdx(hIdxs).encode(true);
		assertExists(doName);
		return {
			doName: doName,
			partitionIdOpaque: opaque,
		};
	}

	rootPartitionContexts(): PartitionContextResolved[] {
		const contexts: PartitionContextResolved[] = [];
		for (let i = 0; i < this.basePartitionContext.rootTreesN; i++) {
			contexts.push(this.resolveRootPartitionContext(i));
		}
		return contexts;
	}

	resolveRootPartitionContext(idx: number): PartitionContextResolved {
		if (this.#_rootContextsCache.has(idx)) {
			return this.#_rootContextsCache.get(idx)!;
		}
		const { doName, opaque } = PartitionIdHelper.fromHashIdxs(this.basePartitionContext, [idx]).encode(true);
		assertExists(doName);
		const { ns } = this.basePartitionContext;
		const doId = env[ns].idFromName(doName);
		const resolvedContext = {
			...this.basePartitionContext,
			doName,
			primaryDoIdStr: doId.toString(),
			partitionId: opaque,
		};
		this.#_rootContextsCache.set(idx, resolvedContext);
		return resolvedContext;
	}

	async traverseForDestroy(
		getStatus: (ctx: PartitionContextResolved) => Promise<{
			splitStatus?: SplitStatusKVItem;
			promotedKeys?: { hashKey: string; status: string }[];
		}>,
		visit: (ctx: PartitionContextResolved) => Promise<void>,
	): Promise<void> {
		// Dedupe range structures: a 'promoted' entry is inherited by every hash child that took ownership,
		// so the same global rangeRoot(hashKey) may be enumerated from multiple hash partitions.
		const destroyedRangeRoots = new Set<string>();

		const destroyPartition = async (ctx: PartitionContextResolved): Promise<void> => {
			// Discover children dynamically: the in-memory topology only knows root nodes,
			// but split children are recorded in the DO's own split status.
			const { splitStatus, promotedKeys } = await getStatus(ctx);
			if (splitStatus?.status === "split_started" || splitStatus?.status === "split_completed") {
				for (const childCtx of splitStatus.childPartitionContexts) {
					await destroyPartition(childCtx);
				}
			}
			// Destroy each linked range structure BEFORE the hash partition that links it. Each range root
			// recurses its own split children via the same path. Deduped by hashKey across the whole tree.
			// Skip 'queued' keys — their range root is not created yet (nothing to destroy).
			for (const { hashKey, status } of promotedKeys ?? []) {
				if (status === "queued") continue;
				const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(ctx, hashKey, null, null);
				if (destroyedRangeRoots.has(rangeRootCtx.doName)) continue;
				destroyedRangeRoots.add(rangeRootCtx.doName);
				await destroyPartition(rangeRootCtx);
			}
			await visit(ctx);
		};

		for (const rootCtx of this.rootPartitionContexts()) {
			await destroyPartition(rootCtx);
		}
	}
}
