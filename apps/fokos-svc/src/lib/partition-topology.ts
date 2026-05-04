import { env } from "cloudflare:workers";
import { PartitionDO } from "./do-partition.js";

type PartitionNamespaceKey = {
	[K in keyof Env]: Env[K] extends DurableObjectNamespace<PartitionDO> ? K : never;
}[keyof Env];

/**
 * PartitionContext includes any information that the DOs need at runtime to perform their operations and is sent in every request.
 * If there was a way for the user to specify parameters for the partition DOs at initialization time, we wouldn't need this, but here we are.
 *
 * FIXME: Type this properly.
 */
export type PartitionContext = {
	schema: 1; // For future compatibility, in case we need to change the structure of the context.

	ns: PartitionNamespaceKey;
	nsPrefix: string;

	rootTreesN: number;
	hashSplitConditions: SplitConditions;
	rangeSplitConditions?: SplitConditions;

	partitionId?: PartitionNodeId;

	/**
	 * This is used to detect changes in the topology configuration and trigger any necessary actions in the DOs,
	 * such as rebalancing or splitting.
	 * The actual value can be a hash of the configuration or a version number that increments on every change.
	 */
	signature: string;
};
export type PartitionContextResolved = PartitionContext & {
	partitionId?: PartitionNodeId;
};

// This can be used to identify the node in the topology and can be useful for routing and debugging.
// This should be internal to the topology logic and opaque to outsiders.
// External code should just pass it around.
// Base64-encoded JSON string with the necessary information to identify the partition, such as the partition ID and any other relevant metadata.
export type PartitionNodeId = string;

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
		ns: PartitionNamespaceKey;
		nsPrefix: string;
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

		const context: PartitionContext = {
			schema: 1,
			ns: opts.ns,
			nsPrefix: opts.nsPrefix,
			rootTreesN: opts.rootTreesN,
			hashSplitConditions: opts.hashSplitConditions,
			rangeSplitConditions: opts.rangeSplitConditions,

			// Filled properly below.
			signature: "",
		};
		// FIXME: Use a proper hash function.
		context.signature = btoa(JSON.stringify(context));
		return context;
	}
}

export interface PartitionTopologyRouter {
	pickPartition(hashKey: string, sortKey?: string): { doId: DurableObjectId; partitionContext: PartitionContextResolved };
}

/**
 * Used by the FokosDB to route requests to the right partition DO based on the provided partition context and keys.
 */
export class PartitionTopologyRouterImpl implements PartitionTopologyRouter {
	constructor(
		private readonly encoded: PartitionTopologyEncoded,
		private readonly basePartitionContext: PartitionContext,
	) {}

	pickPartition(hashKey: string, sortKey?: string): { doId: DurableObjectId; partitionContext: PartitionContextResolved } {
		const { ns, nsPrefix } = this.basePartitionContext;
		// FIXME This is a placeholder implementation. The actual implementation will depend on the encoding scheme used for the partition topology.
		const partitionId = hashKey;
		const doId = env[ns].idFromName(`${nsPrefix}.${partitionId}`);
		const partitionIdOpaque = btoa(
			JSON.stringify({
				partitionId,
			}),
		);
		// Merge with any partition-specific context if needed.
		const partitionContext: PartitionContextResolved = {
			...this.basePartitionContext,
			partitionId: partitionIdOpaque,
		};
		return {
			doId,
			partitionContext,
		};
	}
}

export interface PartitionTopologySplitter {
	splitStatus(): SplitStatusKVItem | undefined;

	/**
	 * Called before every operation to check if the partition can accept the request based on the provided context, storage, and keys.
	 * This can be used to implement backpressure or to prevent writes to certain partitions based on custom logic.
	 *
	 * This should be extremely fast since it's called in every request!
	 */
	shouldAllow(hashKey: string, sortKey?: string): boolean;

	/**
	 * Determines whether a partition should be split based on the provided context, storage, and keys.
	 * This method is called after every write operation to check if the partition needs to be split.
	 *
	 * This should be extremely fast since it's called in every request!
	 *
	 * Basic checks according to the conditions and potentially do more expensive things in a periodic check.
	 */
	shouldSplit(hashKey: string, sortKey?: string): boolean;

	/**
	 * Queues a split operation for the partition. This can be called when the `shouldSplit` method returns true to initiate the split process.
	 * The implementation of this method should ensure that the split process is performed in a way that minimizes disruption to ongoing operations and ensures data consistency.
	 */
	queueSplit(): Promise<SplitStatusKVItem>;
}

/**
 * Used by the Partition Durable Objects.
 */
export class PartitionTopologyImpl implements PartitionTopologySplitter {
	private static readonly KV_KEYS = {
		SPLIT_STATUS: "__split_status",
	};

	#storage: DurableObjectStorage;

	constructor(
		private readonly encoded: PartitionTopologyEncoded,
		private readonly partitionContext: PartitionContextResolved,
		private readonly ctx: DurableObjectState,
	) {
		this.#storage = ctx.storage;
	}

	shouldAllow(hashKey: string, sortKey?: string): boolean {
		// TODO If the split has started but not completed, we should reject requests to the partition to avoid data loss or returning wrong data.
		// We can track the split status in the DO's storage and check it here.

		const splitStatus = this.#storage.kv.get<SplitStatusKVItem>(PartitionTopologyImpl.KV_KEYS.SPLIT_STATUS);
		if (splitStatus && splitStatus.status !== "split_pending") {
			// Reject all requests unless the split is pending, hence not started yet.
			// Once the split is started, we reject all requests to avoid data loss or returning wrong data.
			return false;
		}

		const dbSize = this.#storage.sql.databaseSize;
		// We allow up to 10% over the max size before we start rejecting requests to avoid flapping around the threshold,
		// and to allow the requests to complete and trigger the split.
		if (
			this.partitionContext.hashSplitConditions.maxSizeMb &&
			dbSize > this.partitionContext.hashSplitConditions.maxSizeMb * 1.1 * 1024 * 1024
		) {
			return false;
		}
		return true;
	}

	shouldSplit(_hashKey: string, _sortKey?: string): boolean {
		const dbSize = this.#storage.sql.databaseSize;
		if (this.partitionContext.hashSplitConditions.maxSizeMb && dbSize > this.partitionContext.hashSplitConditions.maxSizeMb * 1024 * 1024) {
			return true;
		}
		// TODO Track some statistics per hashKey/sortKey in memory to track heavy hitter items.

		// TODO Add more conditions based on the partitionContext.
		return false;
	}

	splitStatus(): SplitStatusKVItem | undefined {
		return this.#storage.kv.get<SplitStatusKVItem>(PartitionTopologyImpl.KV_KEYS.SPLIT_STATUS);
	}

	async queueSplit(): Promise<SplitStatusKVItem> {
		const nowStatus = this.splitStatus();
		if (!nowStatus) {
			this.#storage.kv.put<SplitStatusKVItem>(PartitionTopologyImpl.KV_KEYS.SPLIT_STATUS, {
				status: "split_pending",
				createdAt: Date.now(),
				partitionContext: this.partitionContext,
				history: [],
			});
		}
		if (!(await this.#storage.getAlarm())) {
			// Set an alarm to trigger the split process. The alarm handler will check the split status and perform the split if needed.
			await this.#storage.setAlarm(Date.now());
		}
		return this.splitStatus()!;
	}
}

export type SplitStatus = "split_pending" | "split_in_progress" | "split_completed";
export type SplitStatusKVItem = {
	status: SplitStatus;
	createdAt: number;
	partitionContext: PartitionContext;
	history: {
		status: SplitStatus;
		timestamp: number;
	}[];
};

export type PartitionTopologyEncoded = string;
