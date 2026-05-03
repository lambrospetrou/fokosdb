import { PartitionDO } from "./do-partition.js";

/**
 * PartitionContext includes any information that the DOs need at runtime to perform their operations and is sent in every request.
 * If there was a way for the user to specify parameters for the partition DOs at initialization time, we wouldn't need this, but here we are.
 *
 * FIXME: Type this properly.
 */
export type PartitionContext = {
	rootTreesN: number;
	hashSplitConditions: SplitConditions;
	rangeSplitConditions?: SplitConditions;
};

export type SplitConditions = {
	/**
	 * WARNING:: This should NOT CHANGE after initialization, otherwise it may lead to data loss.
	 *
	 * The number of splits to perform when the split conditions are met. For example, if `splitN` is 4, the partition will be split into 4 new partitions.
	 */
	splitN: number;

	/**
	 * The maximum size of the partition in megabytes before it should be split. This is an optional condition that can be used in conjunction with `splitN` or on its own.
	 */
	maxSizeMb?: number;
	/**
	 * The maximum number of items in the partition before it should be split. This is an optional condition that can be used in conjunction with `splitN` or on its own.
	 */
	maxItems?: number;
};

export class PartitionContextCreator {
	static create(opts: {
		rootTreesN: number;
		hashSplitConditions: SplitConditions;
		rangeSplitConditions?: SplitConditions;
	}): PartitionContext {
		// Assert the input options and default to reasonable values if not provided.
		if (!opts.rangeSplitConditions) {
			opts.rangeSplitConditions = { splitN: 4, maxSizeMb: 500 };
		}
		if (!opts.hashSplitConditions) {
			opts.hashSplitConditions = { splitN: 16, maxSizeMb: 100 };
		}
		if (opts.rootTreesN < 1) {
			throw new Error("fokos: rootTreesN must be at least 1");
		}
		if (opts.hashSplitConditions.splitN < 2) {
			throw new Error("fokos: hashSplitConditions.splitN must be at least 2");
		}
		if (opts.hashSplitConditions.maxSizeMb && opts.hashSplitConditions.maxSizeMb < 1) {
			throw new Error("fokos: hashSplitConditions.maxSizeMb must be at least 1");
		}
		if (opts.hashSplitConditions.maxItems && opts.hashSplitConditions.maxItems < 1) {
			throw new Error("fokos: hashSplitConditions.maxItems must be at least 1");
		}
		if (opts.rangeSplitConditions.splitN < 2) {
			throw new Error("fokos: rangeSplitConditions.splitN must be at least 2");
		}
		if (opts.rangeSplitConditions.maxSizeMb && opts.rangeSplitConditions.maxSizeMb < 1) {
			throw new Error("fokos: rangeSplitConditions.maxSizeMb must be at least 1");
		}
		if (opts.rangeSplitConditions.maxItems && opts.rangeSplitConditions.maxItems < 1) {
			throw new Error("fokos: rangeSplitConditions.maxItems must be at least 1");
		}

		return {
			rootTreesN: opts.rootTreesN,
			hashSplitConditions: opts.hashSplitConditions,
			rangeSplitConditions: opts.rangeSplitConditions,
		};
	}
}

export interface PartitionTopologyRouter {
	pickPartition(hashKey: string, sortKey?: string): { doId: DurableObjectId; partitionContext: PartitionContext };
}

export class PartitionTopologyRouterImpl implements PartitionTopologyRouter {
	constructor(
		private readonly encoded: PartitionTopologyEncoded,
		private readonly ns: DurableObjectNamespace<PartitionDO>,
		private readonly nsPrefix: string,
		private readonly basePartitionContext: PartitionContext,
	) {}

	pickPartition(hashKey: string, sortKey?: string): { doId: DurableObjectId; partitionContext: PartitionContext } {
		// FIXME This is a placeholder implementation. The actual implementation will depend on the encoding scheme used for the partition topology.
		const partitionId = hashKey; // TODO Replace with actual partitioning logic.
		const doId = this.ns.idFromName(`${this.nsPrefix}.${partitionId}`);
		return {
			doId,
			// TODO Merge with any partition-specific context if needed.
			partitionContext: this.basePartitionContext,
		};
	}
}

export type PartitionTopologyEncoded = string;
