import { env } from "cloudflare:workers";
import { tryWhile } from "durable-utils/retries";
import { InitFromSplitOptions, PartitionDO } from "./do-partition.js";

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
	doName: string;
	// Future proofing if we want to use DO read replication.
	primaryDoIdStr: string;

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
			doName: doId.name!,
			primaryDoIdStr: doId.toString(),
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
	 *
	 * Automatically queues a split if the conditions are met, so the caller doesn't need to worry about it.
	 */
	maybeQueueSplit(_hashKey: string, _sortKey?: string): Promise<SplitStatusKVItem | undefined>;

	/**
	 * Starts the split process for the partition.
	 * Throws if not all child partitions have been initialized.
	 * In that case, the caller should retry later by calling `queueSplit()` again at some point,
	 * which will set a new alarm to trigger this method again.
	 */
	startSplit(): Promise<void>;
}

/**
 * Used by the Partition Durable Objects.
 */
export class PartitionTopologyImpl implements PartitionTopologySplitter {
	private static readonly KV_KEYS = {
		SPLIT_STATUS: "__split_status",
	};

	#storage: DurableObjectStorage;
	#topologyRouter: PartitionTopologyRouter;

	constructor(
		private readonly encoded: PartitionTopologyEncoded,
		private readonly partitionContext: PartitionContextResolved,
		private readonly ctx: DurableObjectState,
	) {
		this.#storage = ctx.storage;
		// FIXME: Parse the topology only once!
		this.#topologyRouter = new PartitionTopologyRouterImpl(encoded, partitionContext);
	}

	shouldAllow(hashKey: string, sortKey?: string): boolean {
		// TODO If the split has started but not completed, we should reject requests to the partition to avoid data loss or returning wrong data.
		// We can track the split status in the DO's storage and check it here.

		const splitStatus = this.#storage.kv.get<SplitStatusKVItem>(PartitionTopologyImpl.KV_KEYS.SPLIT_STATUS);
		if (splitStatus && splitStatus.status !== "split_pending") {
			// Reject all requests unless the split is pending, hence not started yet.
			// Once the split is started, we reject all requests to avoid data loss or returning wrong data.
			//
			// FIXME: Instead of rejecting here we should return some information to the caller to trigger a retry with an updated topology, so we can minimize downtime during splits.
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

	splitStatus(): SplitStatusKVItem | undefined {
		return this.#storage.kv.get<SplitStatusKVItem>(PartitionTopologyImpl.KV_KEYS.SPLIT_STATUS);
	}

	async maybeQueueSplit(_hashKey: string, _sortKey?: string): Promise<SplitStatusKVItem | undefined> {
		const splitType = this.shouldSplit(_hashKey, _sortKey);
		if (splitType) {
			return await this.queueSplit(splitType);
		}
	}

	shouldSplit(_hashKey: string, _sortKey?: string): SplitType | null {
		const dbSize = this.#storage.sql.databaseSize;
		if (this.partitionContext.hashSplitConditions.maxSizeMb && dbSize > this.partitionContext.hashSplitConditions.maxSizeMb * 1024 * 1024) {
			return "hash";
		}
		// TODO Track some statistics per hashKey/sortKey in memory to track heavy hitter items.

		// TODO Add more conditions based on the partitionContext.
		return null;
	}

	async queueSplit(splitType: SplitType): Promise<SplitStatusKVItem> {
		const nowStatus = this.splitStatus();
		if (!nowStatus) {
			this.#storage.kv.put<SplitStatusKVItem>(PartitionTopologyImpl.KV_KEYS.SPLIT_STATUS, {
				status: "split_pending",
				splitType,
				createdAt: Date.now(),
				partitionContext: this.partitionContext,
			});
		}
		if (!(await this.#storage.getAlarm())) {
			// Set an alarm to trigger the split process. The alarm handler will check the split status and perform the split if needed.
			await this.#storage.setAlarm(Date.now());
		}
		return this.splitStatus()!;
	}

	async startSplit() {
		// 1. Calculate if it's a hash split, or a range split.
		// 2. Calculate the new DO IDs/names for the new partitions and the initialize contexts.
		// 3. Call the new DOs at `initFromSplit()` to initialize them with the right context and their parent partition that will use to get data during migration.
		// Final. Mark the split status as `split_in_progress` to indicate that now there are new partitions handling requests,
		// and the current partition is just a proxy that forwards requests to the new partitions until the split is completed and the data is migrated,
		// then mark the status as `split_completed` and stop forwarding requests.

		const splitStatus = this.splitStatus();
		if (!splitStatus || splitStatus.status !== "split_pending") {
			throw new Error("fokos/topology: Cannot start split process, split is not pending.");
		}

		const childPartitionContexts: InitFromSplitOptions[] = [];
		const splitType = splitStatus.splitType;
		switch (splitType) {
			case "hash":
				// Perform a hash split by calculating the new IDs for the children hash partitions.
				const anyHashKey = this.#storage.sql.exec<{ hk: string }>("SELECT hk FROM items LIMIT 1").one().hk;

				// FIXME: Do the right thing, for now just append the new child partition index to the parent ID.
				const parentName = this.#topologyRouter.pickPartition(anyHashKey).doId.name!;
				for (let i = 0; i < this.partitionContext.hashSplitConditions.splitN; i++) {
					const childPartitionId = `${parentName}.${i}`;
					const childDoId = env[this.partitionContext.ns].idFromName(childPartitionId);
					childPartitionContexts.push({
						parentPartitionContext: this.partitionContext,
						newPartitionContext: {
							...this.partitionContext,
							doName: childDoId.name!,
							primaryDoIdStr: childDoId.toString(),
							partitionId: btoa(JSON.stringify({ partitionId: childPartitionId })),
						},
						splitType: "hash",
					});
				}

				break;
			case "range":
				// FIXME Perform a range split by calculating the new sort key ranges for each child partition.
				break;
		}

		// Call the new DOs at `initFromSplit()` to initialize them with the right context and their parent partition that will use to get data during migration.
		const promises = childPartitionContexts.map(async (childContext) => {
			const doId = env[childContext.newPartitionContext.ns].idFromName(childContext.newPartitionContext.doName!);
			try {
				return await tryWhile(
					async () => {
						const childDo = env[childContext.newPartitionContext.ns].get(doId);
						return await childDo.initFromSplit(childContext);
					},
					(_error, nextAttempt) => {
						return nextAttempt <= 5; // Retry up to 5 times
					},
				);
			} catch (error) {
				// Handle initialization errors
				console.error({
					message: "fokos/topology: Split initialization failed, aborting split process. Will retry later.",
					error: String(error),
					errorProps: error,
					doId: doId.toString(),
					childContext,
				});
				throw error; // Rethrow to be caught by the outer try-catch and trigger a retry of the split process.
			}
		});

		// If any of the initializations fail we abort for now, and retry later.
		// Ideally even partial initializations should be handled gracefully, but for now we can just rely on retries to get to a consistent state.
		// The partition DOs should be the source of truth for everything so until the split initialization succeeds, this parent DO is the owner.
		// FIXME Improve this by allowing some child partitions to not be initialized, which will need a topology router functionality to ask the parent for the context again, which is doable!
		try {
			await Promise.all(promises);
		} catch (error) {
			// Handle initialization errors
			console.error({
				message: "fokos/topology: Some split initialization failed, aborting split process. Will retry later.",
				error: String(error),
				errorProps: error,
				parentPartitionContext: this.partitionContext,
			});

			// By throwing here we stop the split process. The next request will call `queueSplit()` again
			// setting a new alarm, which will retry the split process and hopefully succeed if the errors were transient.
			throw error;
		}

		// Final. Mark the split status as `split_in_progress` to indicate that now there are new partitions handling requests,
		// and the current partition is just a proxy that forwards requests to the new partitions until the split is completed and the data is migrated,
		// then mark the status as `split_completed` and stop forwarding requests.
		this.#storage.kv.put<SplitStatusKVItem>(PartitionTopologyImpl.KV_KEYS.SPLIT_STATUS, {
			status: "split_in_progress",
			splitType,
			createdAt: Date.now(),
			partitionContext: this.partitionContext,
			childPartitionContexts: childPartitionContexts.map((child) => child.newPartitionContext),
			history: [
				{
					status: splitStatus.status,
					splitType: splitStatus.splitType,
					createdAt: splitStatus.createdAt,
					partitionContext: splitStatus.partitionContext,
				},
			],
		});

		console.log({
			message: "fokos/partition: Split process completed successfully.",
			childPartitionContexts,
		});
	}
}

export type SplitType = "hash" | "range";
export type SplitStatus = "split_pending" | "split_in_progress" | "split_completed";
export type SplitStatusKVItem =
	| {
			status: "split_pending";
			splitType: SplitType;
			createdAt: number;
			partitionContext: PartitionContextResolved;
	  }
	| {
			status: "split_in_progress" | "split_completed";
			splitType: SplitType;
			createdAt: number;
			partitionContext: PartitionContextResolved;
			childPartitionContexts: PartitionContextResolved[];
			history: Pick<SplitStatusKVItem, "status" | "splitType" | "createdAt" | "partitionContext">[];
	  };

export type PartitionTopologyEncoded = string;
