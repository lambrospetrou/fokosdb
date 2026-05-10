import { env } from "cloudflare:workers";
import { tryWhile } from "durable-utils/retries";
import { xxHash32 } from "js-xxhash";
import { InitFromSplitOptions, PartitionDO } from "../do-partition.js";
import type { PartitionNodeId, PartitionTopologyEncoded, SplitStatus, SplitType, TopologyNode } from "./types.js";
import { assertExists } from "../tsutils.js";

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

	// Opaque ID used internally to identify the partition.
	// Hex-encoded bytes: [schemaVersion u8, depth u8, hashIdx_1 u8, ..., hashIdx_depth u8].
	// schemaVersion=0 is the current format.
	// TODO: Future optimization would be convert this into a bits array as well, but for now it's OK.
	partitionId: PartitionNodeId;

	// Cached parsed bytes of partitionId. Populated inside the DO for fast routing; survives structured clone.
	_partitionIdBytes?: Uint8Array;
};

// PartitionNodeId is re-exported from ./types.js above.
// Hex-encoded bytes: [schemaVersion u8, depth u8, hashIdx_1 u8, ..., hashIdx_depth u8]. schemaVersion=0 is the current format.

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
		if (opts.rootTreesN < 1 || opts.rootTreesN > 255) {
			throw new Error("fokos: rootTreesN must be between 1 and 255");
		}
		if (opts.hashSplitConditions.splitN < 2 || opts.hashSplitConditions.splitN > 255) {
			throw new Error("fokos: hashSplitConditions.splitN must be between 2 and 255");
		}
		if (opts.hashSplitConditions.maxSizeMb && opts.hashSplitConditions.maxSizeMb < 0.1) {
			throw new Error("fokos: hashSplitConditions.maxSizeMb must be at least 0.1");
		}
		if (opts.hashSplitConditions.maxItems && opts.hashSplitConditions.maxItems < 1) {
			throw new Error("fokos: hashSplitConditions.maxItems must be at least 1");
		}
		if (opts.rangeSplitConditions.splitN < 2 || opts.rangeSplitConditions.splitN > 255) {
			throw new Error("fokos: rangeSplitConditions.splitN must be between 2 and 255");
		}
		if (opts.rangeSplitConditions.maxSizeMb && opts.rangeSplitConditions.maxSizeMb < 0.1) {
			throw new Error("fokos: rangeSplitConditions.maxSizeMb must be at least 0.1");
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
	/**
	 * Used by the FokosDB clients and anyone that wants to route a hashKey/sortKey to the appropriate partition.
	 * @param hashKey
	 * @param sortKey
	 */
	pickPartition(hashKey: string, sortKey?: string): { doId: DurableObjectId; partitionContext: PartitionContextResolved };

	/**
	 * Used only internally by the Partition DOs to determine which of their children should received a request based on the provided context and keys.
	 * Used during the lazy split migration of data to avoid blocking wholesale migration of the data before requests can be handled.
	 * @param partitionContext
	 * @param hashKey
	 * @param sortKey
	 */
	pickChildPartition(
		partitionContext: PartitionContextResolved,
		hashKey: string,
		sortKey?: string,
	): { doId: DurableObjectId; partitionContext: PartitionContextResolved };

	calculateChildPartitionIds(
		parentPartitionIdOpaque: string,
		N: number,
	): {
		doName: string;
		partitionIdOpaque: string;
	}[];

	makeIsCorrectChildHashPartition(
		parentContext: PartitionContextResolved,
		childContext: PartitionContextResolved,
	): (hashKey: string, sortKey?: string) => boolean;
}

// PartitionTopologyEncoded and TopologyNode are re-exported from ./types.js above.

// Golden Ratio constant used for better hash scattering
// See https://softwareengineering.stackexchange.com/a/402543
const GOLDEN_RATIO = 0x9e3779b1;

// function __encodePartitionIdOpaque(hashIdxs: number[]): { bytes: Uint8Array; hex: string } {
// 	const bytes = new Uint8Array(1 + hashIdxs.length);
// 	bytes[0] = hashIdxs.length;
// 	for (let i = 0; i < hashIdxs.length; i++) bytes[i + 1] = hashIdxs[i];
// 	return { bytes, hex: bytes.toHex() };
// }

// function __encodeChildPartitionIdOpaque(parentPartitionId: Uint8Array, childIdxs: number | number[]): { bytes: Uint8Array; hex: string } {
// 	if (Array.isArray(childIdxs)) {
// 		const bytes = new Uint8Array(parentPartitionId.length + childIdxs.length);
// 		bytes.set(parentPartitionId, 0);
// 		bytes[0] += childIdxs.length;
// 		for (let i = 0; i < childIdxs.length; i++) bytes[parentPartitionId.length + i] = childIdxs[i];
// 		return { bytes, hex: bytes.toHex() };
// 	} else {
// 		const bytes = new Uint8Array(parentPartitionId.length + 1);
// 		bytes.set(parentPartitionId, 0);
// 		bytes[0] += 1;
// 		bytes[parentPartitionId.length] = childIdxs;
// 		return { bytes, hex: bytes.toHex() };
// 	}
// }

class PartitionIdHelper {
	static doName(basePartitionContext: PartitionContext, hashIdxs: number[] | Uint8Array<ArrayBuffer>): string {
		return `${basePartitionContext.nsPrefix}.h.${hashIdxs.join(".")}`;
	}

	static fromHashIdxs(basePartitionContext: PartitionContext, hashIdxs: number[]): PartitionIdHelper {
		const bytes = new Uint8Array(2 + hashIdxs.length);
		bytes[0] = 0; // schema version
		bytes[1] = hashIdxs.length;
		for (let i = 0; i < hashIdxs.length; i++) bytes[i + 2] = hashIdxs[i];
		return new PartitionIdHelper(basePartitionContext, bytes);
	}

	#bytes: Uint8Array | undefined;
	#appendedHashIdxs: number[];

	constructor(
		private readonly basePartitionContext: PartitionContext,
		// Either the opaque representation as encoded, or the bytes before encoding.
		partitionIdOpaque?: string | Uint8Array,
	) {
		if (partitionIdOpaque) {
			this.#bytes = partitionIdOpaque instanceof Uint8Array ? partitionIdOpaque : Uint8Array.fromHex(partitionIdOpaque);
		}
		this.#appendedHashIdxs = [];
	}

	appendHashIdx(hashIdx: number | number[]): this {
		if (Array.isArray(hashIdx)) {
			this.#appendedHashIdxs.push(...hashIdx);
		} else {
			this.#appendedHashIdxs.push(hashIdx);
		}
		return this;
	}

	encode(includeDoName: boolean): { bytes: Uint8Array; opaque: string; doName?: string } {
		if (!this.#bytes && this.#appendedHashIdxs.length === 0) {
			throw new Error("No bytes or appended hash indexes to encode");
		}
		const bytes = new Uint8Array((this.#bytes?.length ?? 2) + this.#appendedHashIdxs.length);
		if (this.#bytes) bytes.set(this.#bytes, 0);
		else bytes[0] = 0; // schema version
		const bsz = this.#bytes?.length ?? 2;
		// bytes[0] is the schema version — leave it unchanged.
		bytes[1] = bsz - 2 + this.#appendedHashIdxs.length;
		for (let i = 0; i < this.#appendedHashIdxs.length; i++) bytes[bsz + i] = this.#appendedHashIdxs[i];
		let doName: string | undefined;
		if (includeDoName) {
			// bytes[0]=version, bytes[1]=depth; extract hash indexes that follow.
			const hIdxs = bytes.subarray(2, 2 + bytes[1]);
			doName = PartitionIdHelper.doName(this.basePartitionContext, hIdxs);
		}
		return { bytes, opaque: bytes.toHex(), doName };
	}
}

/**
 * Used by the FokosDB to route requests to the right partition DO based on the provided partition context and keys.
 */
export class PartitionTopologyRouterImpl implements PartitionTopologyRouter {
	#topology: TopologyNode[];

	constructor(
		private readonly encoded: PartitionTopologyEncoded,
		private readonly basePartitionContext: PartitionContext,
	) {
		// FIXME: This is a placeholder implementation. The actual implementation will depend on the encoding scheme used for the partition topology.
		this.#topology = Array.from({ length: basePartitionContext.rootTreesN }, (_, i) => {
			const { doName, opaque } = PartitionIdHelper.fromHashIdxs(basePartitionContext, [i]).encode(true);
			assertExists(doName);

			if (doName.length > 1024) {
				console.warn({
					message:
						"fokos: DO name length exceeds 1024 bytes, which may cause issues with DO name truncation in Cloudflare Workers. Should have used higher rootTreesN and higher hashSplitConditions.splitN to reduce the depth of the tree.",
					doName,
				});
			}
			const doId = env[basePartitionContext.ns].idFromName(doName);
			return {
				partitionId: opaque,
				partitionContext: {
					// We don't take the name from doId because it could be truncated after 1024 bytes.
					doName: doName,
					primaryDoIdStr: doId.toString(),
				},
				children: [],
			};
		});
	}

	calculateChildPartitionIds(
		parentPartitionIdOpaque: string,
		N: number,
	): {
		doName: string;
		partitionIdOpaque: string;
	}[] {
		const parentBytes = Uint8Array.fromHex(parentPartitionIdOpaque);
		return Array.from({ length: N }, (_, i) => {
			const { doName, opaque } = new PartitionIdHelper(this.basePartitionContext, parentBytes).appendHashIdx(i).encode(true);
			return {
				doName: doName!,
				partitionIdOpaque: opaque,
			};
		});
	}

	makeIsCorrectChildHashPartition(
		parentContext: PartitionContextResolved,
		childContext: PartitionContextResolved,
	): (hashKey: string, sortKey?: string) => boolean {
		const childPartitionIdBytes = childContext._partitionIdBytes ?? Uint8Array.fromHex(childContext.partitionId);
		// bytes[0]=version, bytes[1]=depth; hash indexes start at bytes[2].
		const childLevel = childPartitionIdBytes[1];
		const childIdx = childPartitionIdBytes[1 + childLevel];
		return (hashKey: string, sortKey?: string) => {
			const hashedIdx = this.hash(hashKey + childLevel, childContext.hashSplitConditions.splitN);
			return hashedIdx === childIdx;
		};
	}

	// Used by the Partition DOs to route requests to their children during the lazy split migration of data.
	// This routes to the child partition directly without necessary having the most up-to-date topology in-memory.
	pickChildPartition(
		partitionContext: PartitionContextResolved,
		hashKey: string,
		sortKey?: string,
	): { doId: DurableObjectId; partitionContext: PartitionContextResolved } {
		const partitionIdBytes = partitionContext._partitionIdBytes ?? Uint8Array.fromHex(partitionContext.partitionId);
		const hChildIdx = this.hash(hashKey + (partitionIdBytes[1] + 1), partitionContext.hashSplitConditions.splitN);
		const { doName, opaque } = new PartitionIdHelper(this.basePartitionContext, partitionIdBytes).appendHashIdx(hChildIdx).encode(true);
		assertExists(doName);

		const { ns } = this.basePartitionContext;
		const doId = env[ns].idFromName(doName);
		const childPartitionContext: PartitionContextResolved = {
			...partitionContext,
			doName: doName,
			primaryDoIdStr: doId.toString(),
			partitionId: opaque,
		};
		return {
			doId,
			partitionContext: childPartitionContext,
		};
	}

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
		let hIdxs: number[] = [this.hash(hashKey, this.#topology.length)];
		{
			// 1 for the root, then one for each level of the tree until we reach a leaf.
			// The level is used as additional entropy to ensure better distribution of the partitions across the children.
			let level = 1;
			// This should start from the root node and traverse down the tree until it reaches a leaf node,
			// which will be the partition that should handle the request.
			let hNode = this.#topology[hIdxs[0]];
			while (hNode.children.length > 0) {
				level++;
				const hChild = this.hash(hashKey + level, hNode.children.length);
				hIdxs.push(hChild);
				hNode = hNode.children[hChild];
			}
		}

		// TODO: Find the range partition if it exists.

		const { doName, opaque } = new PartitionIdHelper(this.basePartitionContext).appendHashIdx(hIdxs).encode(true);
		assertExists(doName);
		return {
			doName: doName,
			partitionIdOpaque: opaque,
		};
	}

	private hash(hashKey: string, N: number): number {
		return xxHash32(hashKey, GOLDEN_RATIO) % N;
	}
}

export type SplitStatusKVItem =
	| {
			status: Extract<SplitStatus, "split_queued">;
			splitType: SplitType;
			createdAt: number;
			partitionContext: PartitionContextResolved;
	  }
	| {
			status: Extract<SplitStatus, "split_started" | "split_completed">;
			splitType: SplitType;
			createdAt: number;
			partitionContext: PartitionContextResolved;
			childPartitionContexts: PartitionContextResolved[];
			migratedChildDoNames: string[];
			history: Pick<SplitStatusKVItem, "status" | "splitType" | "createdAt" | "partitionContext">[];
	  };

export interface PartitionTopologySplitter {
	splitStatus(): SplitStatusKVItem | undefined;

	/**
	 * Called before every operation to check if the partition can accept the request based on the provided context, storage, and keys.
	 * This can be used to implement backpressure or to prevent writes to certain partitions based on custom logic.
	 *
	 * This should be extremely fast since it's called in every request!
	 */
	shouldAllow(hashKey: string, sortKey?: string): "forward" | "reject" | "ok";

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

	/**
	 * Used only internally by the Partition DOs to determine which of their children should received a request based on the provided context and keys.
	 * Used during the lazy split migration of data to avoid blocking wholesale migration of the data before requests can be handled.
	 */
	pickChildPartition(
		partitionContext: PartitionContextResolved,
		hashKey: string,
		sortKey?: string,
	): { doId: DurableObjectId; partitionContext: PartitionContextResolved };

	makeIsCorrectChildHashPartition(
		parentContext: PartitionContextResolved,
		childContext: PartitionContextResolved,
	): (hashKey: string, sortKey?: string) => boolean;

	/**
	 * Called by a child partition after it has fully migrated its share of data from the parent.
	 * Idempotent. Transitions the parent to split_completed once all children have acknowledged.
	 */
	acknowledgeChildMigration(childDoName: string): void;
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

	shouldAllow(hashKey: string, sortKey?: string): "forward" | "reject" | "ok" {
		// TODO If the split has started but not completed, we should reject requests to the partition to avoid data loss or returning wrong data.
		// We can track the split status in the DO's storage and check it here.

		// TODO - Keep this in memory to avoid reading it all the time from storage.
		const splitStatus = this.#storage.kv.get<SplitStatusKVItem>(PartitionTopologyImpl.KV_KEYS.SPLIT_STATUS);
		if (splitStatus && splitStatus.status !== "split_queued") {
			return "forward";
		}

		const dbSize = this.#storage.sql.databaseSize;
		// We allow up to 10% over the max size before we start rejecting requests to avoid flapping around the threshold,
		// and to allow the requests to complete and trigger the split.
		if (
			this.partitionContext.hashSplitConditions.maxSizeMb &&
			dbSize > this.partitionContext.hashSplitConditions.maxSizeMb * 1.1 * 1024 * 1024
		) {
			return "reject";
		}

		// All good!
		return "ok";
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
				status: "split_queued",
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
		// Final. Mark the split status as `split_started` to indicate that now there are new partitions handling requests,
		// and the current partition is just a proxy that forwards requests to the new partitions until the split is completed and the data is migrated,
		// then mark the status as `split_completed` and stop forwarding requests.

		const splitStatus = this.splitStatus();
		if (!splitStatus || splitStatus.status !== "split_queued") {
			throw new Error("fokos/topology: Cannot start split process, split is not queued.");
		}

		const childPartitionContexts: InitFromSplitOptions[] = [];
		const splitType = splitStatus.splitType;
		switch (splitType) {
			case "hash":
				const childIds = this.#topologyRouter.calculateChildPartitionIds(
					this.partitionContext.partitionId,
					this.partitionContext.hashSplitConditions.splitN,
				);

				for (let i = 0; i < this.partitionContext.hashSplitConditions.splitN; i++) {
					const childDoId = env[this.partitionContext.ns].idFromName(childIds[i].doName);
					childPartitionContexts.push({
						parentPartitionContext: this.partitionContext,
						newPartitionContext: {
							...this.partitionContext,
							doName: childIds[i].doName,
							primaryDoIdStr: childDoId.toString(),
							partitionId: childIds[i].partitionIdOpaque,
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

		// Final. Mark the split status as `split_started` to indicate that now there are new partitions handling requests,
		// and the current partition is just a proxy that forwards requests to the new partitions until the split is completed and the data is migrated,
		// then mark the status as `split_completed` and stop forwarding requests.
		this.#storage.kv.put<SplitStatusKVItem>(PartitionTopologyImpl.KV_KEYS.SPLIT_STATUS, {
			status: "split_started",
			splitType,
			createdAt: Date.now(),
			partitionContext: this.partitionContext,
			childPartitionContexts: childPartitionContexts.map((child) => child.newPartitionContext),
			migratedChildDoNames: [],
			history: [
				{
					status: splitStatus.status,
					splitType: splitStatus.splitType,
					createdAt: splitStatus.createdAt,
					partitionContext: splitStatus.partitionContext,
				},
			],
		});

		// TODO Notify the TopologyKeeperDO that the split has started.
		// We don't want this to be in the hot path of the split to allow as many partitions as necessary to do their splits.
		// The TopologyKeeperDO can be updated asynchronously since the partitions know their topology (if they have children)
		// and can route requests to them.
		// try {} catch (error) {}

		console.log({
			message: "fokos/partition: Split process completed successfully.",
			childPartitionContexts,
		});
	}

	acknowledgeChildMigration(childDoName: string): void {
		const splitStatus = this.splitStatus();
		if (!splitStatus) {
			throw new Error(`fokos/topology: acknowledgeChildMigration called when not splitting.`);
		}
		// Already fully completed — idempotent no-op.
		if (splitStatus.status === "split_completed") return;
		if (splitStatus.status !== "split_started") {
			throw new Error(`fokos/topology: acknowledgeChildMigration called in unexpected status: ${splitStatus.status}.`);
		}
		if (splitStatus.migratedChildDoNames.includes(childDoName)) return;

		const migratedChildDoNames = [...splitStatus.migratedChildDoNames, childDoName];
		const allMigrated = splitStatus.childPartitionContexts.every((c) => migratedChildDoNames.includes(c.doName));

		const newStatus: SplitStatusKVItem = allMigrated
			? {
					status: "split_completed",
					splitType: splitStatus.splitType,
					createdAt: Date.now(),
					partitionContext: splitStatus.partitionContext,
					childPartitionContexts: splitStatus.childPartitionContexts,
					migratedChildDoNames,
					history: [
						...splitStatus.history,
						{
							status: splitStatus.status,
							splitType: splitStatus.splitType,
							createdAt: splitStatus.createdAt,
							partitionContext: splitStatus.partitionContext,
						},
					],
				}
			: { ...splitStatus, migratedChildDoNames };

		this.#storage.kv.put<SplitStatusKVItem>(PartitionTopologyImpl.KV_KEYS.SPLIT_STATUS, newStatus);
	}

	/**
	 * Used only internally by the Partition DOs to determine which of their children should received a request based on the provided context and keys.
	 * Used during the lazy split migration of data to avoid blocking wholesale migration of the data before requests can be handled.
	 */
	pickChildPartition(
		partitionContext: PartitionContextResolved,
		hashKey: string,
		sortKey?: string,
	): { doId: DurableObjectId; partitionContext: PartitionContextResolved } {
		return this.#topologyRouter.pickChildPartition(partitionContext, hashKey, sortKey);
	}

	makeIsCorrectChildHashPartition(
		parentContext: PartitionContextResolved,
		childContext: PartitionContextResolved,
	): (hashKey: string, sortKey?: string) => boolean {
		return this.#topologyRouter.makeIsCorrectChildHashPartition(parentContext, childContext);
	}
}
