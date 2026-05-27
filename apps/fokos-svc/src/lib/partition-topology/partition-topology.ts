import { env } from "cloudflare:workers";
import { tryWhile } from "durable-utils/retries";
import { xxHash32 } from "js-xxhash";
import { InitFromSplitOptions, PartitionDO } from "../do-partition.js";
import type { PartitionNodeId, PartitionTopologyEncoded, SplitStatus, SplitType, TopologyNode } from "./types.js";
import { assertExists } from "../tsutils.js";
import invariant from "../invariant.js";

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
	databaseName: string;

	/**
	 * WARNING:: This should NOT CHANGE after initialization, otherwise it may lead to data loss.
	 */
	rootTreesN: number;
	hashSplitN: number;
	rangeSplitN?: number;

	hashSplitConditions: SplitConditions;
	rangeSplitConditions?: SplitConditions;
};
export type PartitionContextResolved = PartitionContext & {
	doName: string;
	// Future proofing if we want to use DO read replication.
	primaryDoIdStr: string;

	// Opaque ID used internally to identify the partition.
	// Hex-encoded bytes: [schemaVersion u8, rootIdx u16 (2 bytes big-endian), depth u8, hashIdx_1 u8, ..., hashIdx_depth u8].
	// schemaVersion=0 is the current format. depth counts only sub-tree levels (root partitions have depth=0).
	// TODO: Future optimization would be convert this into a bits array as well, but for now it's OK.
	partitionId: PartitionNodeId;

	// Cached parsed bytes of partitionId. Populated inside the DO for fast routing; survives structured clone.
	_partitionIdBytes?: Uint8Array;
};

// PartitionNodeId is re-exported from ./types.js above.
// Hex-encoded bytes: [schemaVersion u8, rootIdx u16 (2 bytes big-endian), depth u8, hashIdx_1 u8, ..., hashIdx_depth u8]. schemaVersion=0 is the current format.

export type SplitConditions = {
	/**
	 * The maximum size of the partition in megabytes before it should be split. This is an optional condition that can be used in conjunction with `splitN` or on its own.
	 */
	maxSizeMb?: number;
	/**
	 * The maximum number of items in the partition before it should be split. This is an optional condition that can be used in conjunction with `splitN` or on its own.
	 */
	maxItems?: number;
};

export function areImmutableOptionsEqual(opts1: PartitionContext, opts2: PartitionContext): boolean {
	return (
		opts1.schema === opts2.schema &&
		opts1.databaseName === opts2.databaseName &&
		opts1.rootTreesN === opts2.rootTreesN &&
		opts1.hashSplitN === opts2.hashSplitN
	);
}

export function areMutableOptionsEqual(opts1: PartitionContext, opts2: PartitionContext): boolean {
	return (
		opts1.hashSplitConditions.maxSizeMb === opts2.hashSplitConditions.maxSizeMb &&
		opts1.hashSplitConditions.maxItems === opts2.hashSplitConditions.maxItems &&
		opts1.rangeSplitConditions?.maxSizeMb === opts2.rangeSplitConditions?.maxSizeMb &&
		opts1.rangeSplitConditions?.maxItems === opts2.rangeSplitConditions?.maxItems
	);
}

export class PartitionContextCreator {
	static create(opts: {
		ns: PartitionNamespaceKey;
		databaseName: string;
		rootTreesN: number;
		hashSplitN: number;
		hashSplitConditions: SplitConditions;
		rangeSplitN?: number;
		rangeSplitConditions?: SplitConditions;
	}): PartitionContext {
		// Assert the input options and default to reasonable values if not provided.
		if (!opts.rangeSplitConditions) {
			opts.rangeSplitN = 4;
			opts.rangeSplitConditions = { maxSizeMb: 500 };
		}
		if (!opts.hashSplitConditions) {
			opts.hashSplitN = 4;
			opts.hashSplitConditions = { maxSizeMb: 100 };
		}
		if (opts.rootTreesN < 1 || opts.rootTreesN > 65000) {
			throw new Error("fokos: rootTreesN must be between 1 and 65000");
		}

		invariant(opts.hashSplitN, "fokos: hashSplitN must be provided if hashSplitConditions is provided");
		if (opts.hashSplitN < 2 || opts.hashSplitN > 255) {
			throw new Error("fokos: hashSplitN must be between 2 and 255");
		}
		if (opts.hashSplitConditions.maxSizeMb && opts.hashSplitConditions.maxSizeMb < 0.1) {
			throw new Error("fokos: hashSplitConditions.maxSizeMb must be at least 0.1");
		}
		if (opts.hashSplitConditions.maxItems && opts.hashSplitConditions.maxItems < 1) {
			throw new Error("fokos: hashSplitConditions.maxItems must be at least 1");
		}

		invariant(opts.rangeSplitN, "fokos: rangeSplitN must be provided if rangeSplitConditions is provided");
		if (opts.rangeSplitN < 2 || opts.rangeSplitN > 255) {
			throw new Error("fokos: rangeSplitN must be between 2 and 255");
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
			databaseName: opts.databaseName,
			rootTreesN: opts.rootTreesN,
			hashSplitN: opts.hashSplitN,
			rangeSplitN: opts.rangeSplitN,
			hashSplitConditions: opts.hashSplitConditions,
			rangeSplitConditions: opts.rangeSplitConditions,
		};
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

	/**
	 * Returns a PartitionContextResolved for every root partition in the topology.
	 * Used as the starting points for full-tree traversal (e.g. destroy).
	 */
	rootPartitionContexts(): PartitionContextResolved[];
}

// PartitionTopologyEncoded and TopologyNode are re-exported from ./types.js above.

// Golden Ratio constant used for better hash scattering
// See https://softwareengineering.stackexchange.com/a/402543
const GOLDEN_RATIO = 0x9e3779b1;

export class PartitionIdHelper {
	static doName(basePartitionContext: PartitionContext, bytes: Uint8Array): string {
		invariant(bytes[0] === 0, `fokos/topology: unsupported partition ID schema version: ${bytes[0]}`);
		const root = (bytes[1] << 8) | bytes[2];
		const depth = bytes[3];
		const suffix = depth > 0 ? "." + bytes.subarray(4, 4 + depth).join(".") : "";
		return `${basePartitionContext.databaseName}.h.${root}${suffix}`;
	}

	static fromHashIdxs(basePartitionContext: PartitionContext, hashIdxs: number[]): PartitionIdHelper {
		invariant(hashIdxs.length >= 1, "fokos/topology.fromHashIdxs: hashIdxs must not be empty");
		// hashIdxs[0] is the root index (u16), hashIdxs[1..] are sub-tree child indexes (u8 each).
		const depth = hashIdxs.length - 1;
		const bytes = new Uint8Array(4 + depth); // [version, rootHi, rootLo, depth, child1..child_depth]
		bytes[0] = 0; // schema version
		bytes[1] = (hashIdxs[0] >> 8) & 0xff; // root index high byte
		bytes[2] = hashIdxs[0] & 0xff; // root index low byte
		bytes[3] = depth; // sub-tree depth (u8)
		for (let i = 0; i < depth; i++) bytes[4 + i] = hashIdxs[i + 1];
		return new PartitionIdHelper(basePartitionContext, bytes);
	}

	// Readers for the encoded partition ID bytes.
	// Format: [schemaVersion u8, rootIdx u16, depth u8, hashIdx_1 u8, ..., hashIdx_depth u8]
	static rootIdx(bytes: Uint8Array): number {
		invariant(bytes[0] === 0, `fokos/topology: unsupported partition ID schema version: ${bytes[0]}`);
		return (bytes[1] << 8) | bytes[2];
	}
	static depth(bytes: Uint8Array): number {
		invariant(bytes[0] === 0, `fokos/topology: unsupported partition ID schema version: ${bytes[0]}`);
		return bytes[3];
	}
	// The last child index is this partition's slot among its siblings (only valid when depth >= 1).
	static lastChildIdx(bytes: Uint8Array): number {
		invariant(bytes[0] === 0, `fokos/topology: unsupported partition ID schema version: ${bytes[0]}`);
		return bytes[3 + bytes[3]];
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
		invariant(this.#bytes || this.#appendedHashIdxs.length > 0, "fokos/topology.encode: no bytes or appended hash indexes to encode");
		invariant(!this.#bytes || this.#bytes[0] === 0, `fokos/topology.encode: unexpected schema version byte: ${this.#bytes?.[0]}`);
		let bytes: Uint8Array;
		if (this.#bytes) {
			invariant(this.#bytes.length >= 4, "fokos/topology.encode: existing bytes too short to be valid");
			// Extending an existing encoded partition: append child indexes (u8 each).
			bytes = new Uint8Array(this.#bytes.length + this.#appendedHashIdxs.length);
			bytes.set(this.#bytes, 0);
			const bsz = this.#bytes.length;
			// bytes[0..2] = version + rootIdx — leave unchanged.
			bytes[3] = bsz - 4 + this.#appendedHashIdxs.length; // new depth (u8)
			for (let i = 0; i < this.#appendedHashIdxs.length; i++) bytes[bsz + i] = this.#appendedHashIdxs[i];
		} else {
			// Fresh instance: appendedHashIdxs[0] is the root index (u16), rest are child indexes (u8 each).
			const depth = this.#appendedHashIdxs.length - 1;
			bytes = new Uint8Array(4 + depth);
			bytes[0] = 0; // schema version
			bytes[1] = (this.#appendedHashIdxs[0] >> 8) & 0xff; // root high byte
			bytes[2] = this.#appendedHashIdxs[0] & 0xff; // root low byte
			bytes[3] = depth;
			for (let i = 0; i < depth; i++) bytes[4 + i] = this.#appendedHashIdxs[i + 1];
		}
		let doName: string | undefined;
		if (includeDoName) {
			doName = PartitionIdHelper.doName(this.basePartitionContext, bytes);
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
		const result = Array.from({ length: N }, (_, i) => {
			const { doName, opaque } = new PartitionIdHelper(this.basePartitionContext, parentBytes).appendHashIdx(i).encode(true);
			return {
				doName: doName!,
				partitionIdOpaque: opaque,
			};
		});
		invariant(result.length === N, `fokos/topology.calculateChildPartitionIds: expected ${N} children, got ${result.length}`);
		return result;
	}

	makeIsCorrectChildHashPartition(
		parentContext: PartitionContextResolved,
		childContext: PartitionContextResolved,
	): (hashKey: string, sortKey?: string) => boolean {
		const childPartitionIdBytes = childContext._partitionIdBytes ?? Uint8Array.fromHex(childContext.partitionId);
		const childLevel = PartitionIdHelper.depth(childPartitionIdBytes);
		invariant(childLevel >= 1, `fokos/topology.makeIsCorrectChildHashPartition: childLevel must be >= 1, got ${childLevel}`);
		const childIdx = PartitionIdHelper.lastChildIdx(childPartitionIdBytes);
		invariant(
			childIdx < childContext.hashSplitN,
			`fokos/topology.makeIsCorrectChildHashPartition: childIdx ${childIdx} out of range for splitN ${childContext.hashSplitN}`,
		);
		return (hashKey: string, sortKey?: string) => {
			const hashedIdx = this.hash(hashKey + childLevel, childContext.hashSplitN);
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
		const depth = PartitionIdHelper.depth(partitionIdBytes);
		// depth+1 as entropy: root (depth=0) → 1, child (depth=1) → 2, etc.
		// Ensures each tree level uses a distinct hash seed so siblings don't cluster.
		const hChildIdx = this.hash(hashKey + (depth + 1), partitionContext.hashSplitN);
		invariant(
			hChildIdx >= 0 && hChildIdx < partitionContext.hashSplitN,
			`fokos/topology/pickChildPartition: hChildIdx ${hChildIdx} out of range for splitN ${partitionContext.hashSplitN}`,
		);
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

	rootPartitionContexts(): PartitionContextResolved[] {
		return this.#topology.map((node) => ({
			...this.basePartitionContext,
			doName: node.partitionContext.doName,
			primaryDoIdStr: node.partitionContext.primaryDoIdStr,
			partitionId: node.partitionId,
		}));
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
		// If the split has started but not completed, we should reject requests to the partition to avoid data loss or returning wrong data.
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
			return this.queueSplit(splitType);
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

	queueSplit(splitType: SplitType): SplitStatusKVItem {
		const nowStatus = this.splitStatus();
		if (!nowStatus) {
			this.#storage.kv.put<SplitStatusKVItem>(PartitionTopologyImpl.KV_KEYS.SPLIT_STATUS, {
				status: "split_queued",
				splitType,
				createdAt: Date.now(),
				partitionContext: this.partitionContext,
			});
		}
		// Alarm scheduling is the caller's responsibility (PartitionDO.checkSplits).
		const written = this.splitStatus();
		invariant(written != null, "fokos/topology.queueSplit: KV write succeeded but splitStatus() returned null");
		return written;
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
			// Already started or completed — idempotent no-op.
			return;
		}

		const childPartitionContexts: InitFromSplitOptions[] = [];
		const splitType = splitStatus.splitType;
		switch (splitType) {
			case "hash":
				const childIds = this.#topologyRouter.calculateChildPartitionIds(
					this.partitionContext.partitionId,
					this.partitionContext.hashSplitN,
				);
				invariant(
					childIds.length === this.partitionContext.hashSplitN,
					`fokos/topology/startSplit: expected ${this.partitionContext.hashSplitN} children, got ${childIds.length}`,
				);
				const uniqueChildNames = new Set(childIds.map((c) => c.doName));
				invariant(uniqueChildNames.size === childIds.length, "fokos/topology.startSplit: duplicate child doNames detected");

				for (let i = 0; i < this.partitionContext.hashSplitN; i++) {
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
		// then mark the status as `split_completed`.
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

		// Kick off migration on each child immediately so it doesn't wait for the first user request.
		// Fire-and-forget: failures are logged but do not fail startSplit — the child will
		// start migrating on its first incoming request if this doesn't reach it.
		// We do not use this.ctx.waitUntil(...) since it causes vitest errors with tangling log messages.
		await Promise.allSettled(
			childPartitionContexts.map(async (childContext) => {
				try {
					const doId = env[childContext.newPartitionContext.ns].idFromName(childContext.newPartitionContext.doName);
					const childDo = env[childContext.newPartitionContext.ns].get(doId);
					await childDo.triggerMigration();
				} catch (error) {
					console.error({
						message: "fokos/topology: Failed to trigger migration on child partition; will start on the next request.",
						error: String(error),
						errorProps: error,
						childDoName: childContext.newPartitionContext.doName,
					});
				}
			}),
		);

		// TODO Notify the TopologyKeeperDO that the split has started.
		// We don't want this to be in the hot path of the split to allow as many partitions as necessary to do their splits.
		// The TopologyKeeperDO can be updated asynchronously since the partitions know their topology (if they have children)
		// and can route requests to them.
		// try {} catch (error) {}

		console.log({
			message: "fokos/topology: Split process completed successfully.",
			childPartitionContexts,
		});
	}

	acknowledgeChildMigration(childDoName: string): void {
		const splitStatus = this.splitStatus();
		invariant(splitStatus, "fokos/topology.acknowledgeChildMigration: splitStatus must exist to acknowledge child migration");
		// Already fully completed — idempotent no-op.
		if (splitStatus.status === "split_completed") return;
		invariant(
			splitStatus.status === "split_started",
			`fokos/topology.acknowledgeChildMigration: cannot acknowledge child migration in status ${splitStatus.status}`,
		);
		if (splitStatus.migratedChildDoNames.includes(childDoName)) return;

		const migratedChildDoNames = [...splitStatus.migratedChildDoNames, childDoName];
		invariant(
			migratedChildDoNames.length <= splitStatus.childPartitionContexts.length,
			`fokos/topology.acknowledgeChildMigration: more acks (${migratedChildDoNames.length}) than expected children (${splitStatus.childPartitionContexts.length})`,
		);
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
