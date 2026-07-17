import type { PartitionNodeId, SplitType } from "./types.js";
import type { RangeAncestorInfo } from "../types.js";
import { KeyCodec, type KeyBytes } from "./key-codec.js";
// Type-only import: erased at emit, so it creates NO runtime module cycle with do-partition.ts
// (the value-level cycle is what the layering refactor eliminated). It exists solely so the
// namespace-key filter below can match by class identity — structural alternatives collapse to
// `any` when TypeScript resolves them mid-cycle from do-partition.ts itself.
import type { PartitionDO } from "../do-partition.js";
import type { TransactionCoordinatorDO } from "../do-transaction-coordinator.js";
import invariant from "../invariant.js";

export type PartitionNamespaceKey = {
	[K in keyof Env]: Env[K] extends DurableObjectNamespace<PartitionDO> ? K : never;
}[keyof Env];

export type TransactionCoordinatorNamespaceKey = {
	[K in keyof Env]: Env[K] extends DurableObjectNamespace<TransactionCoordinatorDO> ? K : never;
}[keyof Env];

/**
 * PartitionContext includes any information that the DOs need at runtime to perform their operations and is sent in every request.
 * If there was a way for the user to specify parameters for the partition DOs at initialization time, we wouldn't need this, but here we are.
 *
 * FIXME: Type this properly.
 */
export type PartitionContext = {
	schema: 1; // For future compatibility, in case we need to change the structure of the context.

	tableName: string;

	// TODO: Think where to put these since they are needed by the PartitionDO and the client as well.
	// Wrangler ENV vars work too, but I prefer it in the code somewhere.
	// Having them in the PartitionContext makes it easier to pass them around and use them in the PartitionDO and the client,
	// but it's adding extra bytes to every single request.
	ns: PartitionNamespaceKey;
	nsTx: TransactionCoordinatorNamespaceKey;

	/**
	 * WARNING:: This should NOT CHANGE after initialization, otherwise it may lead to data loss.
	 */
	rootTreesN: number;
	hashSplitN: number;
	rangeSplitN?: number;

	hashSplitConditions: SplitConditions;
	rangeSplitConditions?: SplitConditions;

	/**
	 * Bounded ancestor-set selection for range splits: shallowest `fromRoot` + deepest `fromLeaf`
	 * ancestors, deduped. Table-level (not per-request) because splits are driven by background
	 * jobs, not a user request.
	 */
	rangeAncestorsConfig: { fromRoot: number; fromLeaf: number };
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

	// Present only on range-structure DOs. Immutable identity.
	// Redundant with the decoded partitionId, but kept denormalized for cheap routing/filters.
	// Both boundaries are immutable: a range DO owns [startBoundary, endBoundary) for life; on split it
	// becomes a pure router and its children own the sub-ranges.
	// Canonical KeyBytes in memory (derived from the opaque partitionId on load; the partitionId is
	// their serialized carrier — survives structured clone / KV / RPC). Compared only via KeyCodec.
	rangePartition?: {
		hashKey: KeyBytes;
		startBoundary: KeyBytes | null; // null = unbounded lower edge (−∞)
		endBoundary: KeyBytes | null; // null = unbounded upper edge (+∞)
	};
};

export type PartitionContextLivePartition = PartitionContextResolved & {
	// Cached parsed bytes of partitionId. Populated inside the DO for fast routing; survives structured clone.
	_partitionIdBytes?: Uint8Array;
};

export function assertCtxHasIdBytes(
	pCtx: PartitionContextLivePartition,
): asserts pCtx is PartitionContextLivePartition & { _partitionIdBytes: Uint8Array } {
	invariant(pCtx._partitionIdBytes != null, `fokos: partition context _partitionIdBytes not initialized`);
}

export type SplitConditions = {
	/**
	 * The maximum size of the partition in megabytes before it should be split. This is an optional condition that can be used in conjunction with `splitN` or on its own.
	 */
	maxSizeMb?: number;
	/**
	 * The maximum number of items in the partition before it should be split. This is an optional condition that can be used in conjunction with `splitN` or on its own.
	 * FIXME: Not fully implemented, either remove or implement.
	 */
	maxItems?: number;
};

/**
 * Sent by a splitting parent to each new child DO's initFromSplit. A context-level type: it pairs
 * the parent's resolved context (the child pulls data from it during migration) with the child's
 * own new identity.
 */
export type InitFromSplitOptions = {
	splitType: SplitType;
	parentPartitionContext: PartitionContextLivePartition;

	newPartitionContext: PartitionContextResolved;
	newPartitionRangeDepth?: number; // undefined for hash partitions, 0 for range-root, 1 for first-level child, etc.

	/**
	 * Range splits (and range-root promotion) only. One-time init payload — NOT persisted as part
	 * of routing context. The child writes this to its local `range_hierarchy` table in
	 * `initFromSplit`.
	 */
	rangeAncestors?: RangeAncestorInfo[];
};

export function isHashPartition(
	ctx: PartitionContextResolved | PartitionContextLivePartition,
): ctx is (PartitionContextResolved | PartitionContextLivePartition) & { rangePartition: null | undefined } {
	return !ctx.rangePartition;
}

// TODO: Can I make this a type guard that narrows to PartitionContextLivePartition?
export function isRangePartition(ctx: PartitionContextResolved | PartitionContextLivePartition): ctx is (
	| PartitionContextResolved
	| PartitionContextLivePartition
) & {
	rangePartition: NonNullable<PartitionContextResolved["rangePartition"]>;
} {
	return Boolean(ctx.rangePartition);
}

/**
 * Log-safe rendering of a PartitionContextResolved. Converts KeyBytes fields in rangePartition to
 * human-readable strings via keyForLog so they never appear as bare Uint8Array in operational logs.
 */
export function pCtxForLog(ctx: PartitionContextResolved | PartitionContextLivePartition | null | undefined): Record<string, unknown> {
	if (!ctx) return { partitionContext: null };
	const { rangePartition, ...rest } = ctx;
	if ("_partitionIdBytes" in rest) delete (rest as any)._partitionIdBytes;

	return {
		...rest,
		rangePartition: rangePartition
			? {
					hashKey: KeyCodec.keyForLog(rangePartition.hashKey),
					startBoundary: rangePartition.startBoundary !== null ? KeyCodec.keyForLog(rangePartition.startBoundary) : null,
					endBoundary: rangePartition.endBoundary !== null ? KeyCodec.keyForLog(rangePartition.endBoundary) : null,
				}
			: rangePartition,
	};
}

export function areImmutableOptionsEqual(opts1: PartitionContext, opts2: PartitionContext): boolean {
	return (
		opts1.schema === opts2.schema &&
		opts1.tableName === opts2.tableName &&
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
		opts1.rangeSplitConditions?.maxItems === opts2.rangeSplitConditions?.maxItems &&
		opts1.rangeAncestorsConfig?.fromRoot === opts2.rangeAncestorsConfig?.fromRoot &&
		opts1.rangeAncestorsConfig?.fromLeaf === opts2.rangeAncestorsConfig?.fromLeaf
	);
}

export class PartitionContextCreator {
	static create(opts: {
		ns: PartitionNamespaceKey;
		nsTx: TransactionCoordinatorNamespaceKey;
		tableName: string;
		rootTreesN: number;
		hashSplitN: number;
		hashSplitConditions: SplitConditions;
		rangeSplitN?: number;
		rangeSplitConditions?: SplitConditions;
		rangeAncestorsConfig?: { fromRoot: number; fromLeaf: number };
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
		if (!opts.rangeAncestorsConfig) {
			opts.rangeAncestorsConfig = { fromRoot: 0, fromLeaf: 3 };
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

		if (opts.rangeAncestorsConfig.fromRoot < 0 || opts.rangeAncestorsConfig.fromRoot > 10) {
			throw new Error("fokos: rangeAncestorsConfig.fromRoot must be between 0 and 10");
		}
		if (opts.rangeAncestorsConfig.fromLeaf < 0 || opts.rangeAncestorsConfig.fromLeaf > 10) {
			throw new Error("fokos: rangeAncestorsConfig.fromLeaf must be between 0 and 10");
		}

		const context: PartitionContext = {
			schema: 1,
			ns: opts.ns,
			nsTx: opts.nsTx,
			tableName: opts.tableName,
			rootTreesN: opts.rootTreesN,
			hashSplitN: opts.hashSplitN,
			rangeSplitN: opts.rangeSplitN,
			hashSplitConditions: opts.hashSplitConditions,
			rangeSplitConditions: opts.rangeSplitConditions,
			rangeAncestorsConfig: opts.rangeAncestorsConfig,
		};
		return context;
	}
}
