import type { PartitionNodeId, SplitType } from "./types.js";
// Type-only import: erased at emit, so it creates NO runtime module cycle with do-partition.ts
// (the value-level cycle is what the layering refactor eliminated). It exists solely so the
// namespace-key filter below can match by class identity — structural alternatives collapse to
// `any` when TypeScript resolves them mid-cycle from do-partition.ts itself.
import type { PartitionDO } from "../do-partition.js";
import invariant from "../invariant.js";

export type PartitionNamespaceKey = {
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
	// Hex-encoded bytes: schema byte determines format.
	//   SCHEMA_HASH_V1 (0x00): [schemaVersion u8, rootIdx u16 (2 bytes big-endian), depth u8, hashIdx_1 u8, ..., hashIdx_depth u8].
	//   SCHEMA_RANGE_V1 (0x01): see PartitionIdHelper wire format.
	// schemaVersion=0 is the hash format. depth counts only sub-tree levels (root partitions have depth=0).
	// TODO: Future optimization would be convert this into a bits array as well, but for now it's OK.
	partitionId: PartitionNodeId;

	// Cached parsed bytes of partitionId. Populated inside the DO for fast routing; survives structured clone.
	_partitionIdBytes?: Uint8Array;

	// Present only on range-structure DOs. Immutable identity.
	// Redundant with the decoded partitionId, but kept denormalized for cheap routing/filters.
	// Both boundaries are immutable: a range DO owns [startBoundary, endBoundary) for life; on split it
	// becomes a pure router and its children own the sub-ranges.
	rangePartition?: {
		hashKey: string;
		startBoundary: string | null; // null = unbounded lower edge (−∞)
		endBoundary: string | null; // null = unbounded upper edge (+∞)
	};
};

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

/**
 * Sent by a splitting parent to each new child DO's initFromSplit. A context-level type: it pairs
 * the parent's resolved context (the child pulls data from it during migration) with the child's
 * own new identity.
 */
export type InitFromSplitOptions = {
	parentPartitionContext: PartitionContextResolved;
	newPartitionContext: PartitionContextResolved;
	splitType: SplitType;
};

export function isHashPartition(ctx: PartitionContextResolved): ctx is PartitionContextResolved & { rangePartition: null | undefined } {
	return !ctx.rangePartition;
}

export function isRangePartition(ctx: PartitionContextResolved): ctx is PartitionContextResolved & {
	rangePartition: NonNullable<PartitionContextResolved["rangePartition"]>;
} {
	return Boolean(ctx.rangePartition);
}

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
		opts1.rangeSplitN === opts2.rangeSplitN &&
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
		if (opts.hashSplitConditions.maxSizeMb && opts.hashSplitConditions.maxSizeMb <= 0) {
			throw new Error("fokos: hashSplitConditions.maxSizeMb must be greater than 0");
		}
		if (opts.hashSplitConditions.maxItems && opts.hashSplitConditions.maxItems < 1) {
			throw new Error("fokos: hashSplitConditions.maxItems must be at least 1");
		}

		invariant(opts.rangeSplitN, "fokos: rangeSplitN must be provided if rangeSplitConditions is provided");
		if (opts.rangeSplitN < 2 || opts.rangeSplitN > 255) {
			throw new Error("fokos: rangeSplitN must be between 2 and 255");
		}
		if (opts.rangeSplitConditions.maxSizeMb && opts.rangeSplitConditions.maxSizeMb <= 0) {
			throw new Error("fokos: rangeSplitConditions.maxSizeMb must be greater than 0");
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
