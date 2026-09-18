import { env } from "cloudflare:workers";
import type { PartitionContext, PartitionContextResolved } from "./partition-context.js";
import { PartitionIdHelper, hashRootIndex } from "./partition-id.js";
import type { KeyBytes } from "./key-codec.js";
import type { FokosPartitionRef } from "../partition/repartition/repartition-types.js";
import { assertExists } from "../tsutils.js";

export interface PartitionTopologyRouter {
	partitionContext(): PartitionContext;

	/** Routes a hashKey/sortKey pair to the partition that owns it. */
	pickPartition(hashKey: KeyBytes, sortKey?: KeyBytes): { doId: DurableObjectId; partitionContext: PartitionContextResolved };

	/**
	 * Returns a PartitionContextResolved for every root partition. A full-tree traversal, such as
	 * destroy, starts from these.
	 */
	rootPartitionContexts(): PartitionContextResolved[];

	/**
	 * The whole-tree traversal for destroy. The router owns the order, which visits every target
	 * before the partition that links it, and the dedup of shared range roots. The caller supplies the
	 * two callbacks that make the RPCs.
	 *
	 * `discoverTargets` must fence the partition before it reads the target links, so nothing adds one
	 * after the read. It receives a root context only for a root partition, which can need it to
	 * bootstrap.
	 */
	traverseForDestroy(
		discoverTargets: (partition: FokosPartitionRef, rootContext?: PartitionContextResolved) => Promise<FokosPartitionRef[]>,
		visit: (partition: FokosPartitionRef) => Promise<void>,
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
	pickPartition(hashKey: KeyBytes, sortKey?: KeyBytes): { doId: DurableObjectId; partitionContext: PartitionContextResolved } {
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

	private findPartition({ hashKey, sortKey }: { hashKey: KeyBytes; sortKey?: KeyBytes }): {
		doName: string;
		partitionIdOpaque: string;
	} {
		// The hash partition comes first.
		// Root tree index first. Keys arrive already-encoded (db.ts encodes at entry).
		let hIdxs: number[] = [hashRootIndex(hashKey, this.basePartitionContext.rootTreesN)];

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
		discoverTargets: (partition: FokosPartitionRef, rootContext?: PartitionContextResolved) => Promise<FokosPartitionRef[]>,
		visit: (partition: FokosPartitionRef) => Promise<void>,
	): Promise<void> {
		// One range root is the target of a promotion on every hash child that inherited the key, so the
		// traversal reaches the same partition from more than one place. One set over the whole
		// traversal, keyed by the name a destroy call needs, stops a second visit.
		const visited = new Set<string>();

		const destroyPartition = async (partition: FokosPartitionRef, rootContext?: PartitionContextResolved): Promise<void> => {
			if (visited.has(partition.doName)) return;
			visited.add(partition.doName);
			// The in-memory topology knows only the roots. Everything below a root is a durable target
			// link that the partition reports: the split children, and the range trees of its promotions.
			for (const target of await discoverTargets(partition, rootContext)) {
				await destroyPartition(target);
			}
			await visit(partition);
		};

		for (const rootCtx of this.rootPartitionContexts()) {
			await destroyPartition({ partitionId: rootCtx.partitionId, doName: rootCtx.doName }, rootCtx);
		}
	}
}
