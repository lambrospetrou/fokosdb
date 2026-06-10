import { env } from "cloudflare:workers";
import { tryWhile } from "durable-utils/retries";
import { InitFromSplitOptions, PartitionDO } from "../do-partition.js";
import { HashTopology, HashTopologySnapshot } from "./hash-topology.js";
import { GOLDEN_RATIO as _GOLDEN_RATIO, hashChildIndex as _hashChildIndex, hashRootIndex as _hashRootIndex } from "./hash-primitives.js";
import type { PartitionNodeId, SplitStatus, SplitType } from "./types.js";
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

// PartitionTopologyEncoded and TopologyNode are re-exported from ./types.js above.

// Reserved sentinel tokens for the unbounded edges of a range, used ONLY in DO names (never in
// routing comparisons — there boundaries stay `string | null` with null = unbounded). Collision-proof
// by construction: encodeRangeComponent escapes a literal "~" to "%7E", so a "~"-prefixed token can
// never equal an encoded real boundary. No "exclude from valid sk" validation is required.
export const RANGE_MIN = "~min";
export const RANGE_MAX = "~max";

// Percent-encodes any char that is not [A-Za-z0-9_-] so the literal "." delimiters in range DO names are unambiguous.
function encodeRangeComponent(s: string): string {
	return s.replace(/[^A-Za-z0-9_-]/g, (c) => "%" + c.charCodeAt(0).toString(16).padStart(2, "0").toUpperCase());
}

// Range DO name. null start/end render to the ~min/~max sentinels so every DO has the identical
// three-component shape (the range root is db.r.<hk>.~min.~max, addressable from hashKey alone).
// The ".r." namespace marker keeps range and hash DO names disjoint (hash = "db.h.…", range = "db.r.…").
export function rangePartitionDoName(
	databaseName: string,
	hashKey: string,
	startBoundary: string | null,
	endBoundary: string | null,
): string {
	const hk = encodeRangeComponent(hashKey);
	const start = startBoundary === null ? RANGE_MIN : encodeRangeComponent(startBoundary);
	const end = endBoundary === null ? RANGE_MAX : encodeRangeComponent(endBoundary);
	return `${databaseName}.r.${hk}.${start}.${end}`;
}

// Resolves a PartitionContextResolved for a range-structure DO (root or child).
// Same return shape as pickPartition / pickChildPartition so callers can use the result uniformly.
export function resolveRangePartitionContext(
	base: PartitionContextResolved,
	hashKey: string,
	startBoundary: string | null,
	endBoundary: string | null,
): { doId: DurableObjectId; partitionContext: PartitionContextResolved } {
	const { opaque, doName } = PartitionIdHelper.fromRangePartition(base, hashKey, startBoundary, endBoundary).encode(true);
	const doId = env[base.ns].idFromName(doName!);
	return {
		doId,
		partitionContext: {
			...base,
			doName: doName!,
			primaryDoIdStr: doId.toString(),
			partitionId: opaque,
			_partitionIdBytes: undefined,
			rangePartition: { hashKey, startBoundary, endBoundary },
		},
	};
}

// Re-exported from hash-primitives.ts (lives there to break the circular dependency with hash-topology.ts).
export const GOLDEN_RATIO = _GOLDEN_RATIO;
export const hashChildIndex = _hashChildIndex;
export const hashRootIndex = _hashRootIndex;

export class PartitionIdHelper {
	static readonly SCHEMA_HASH_V1 = 0x00 as const;
	static readonly SCHEMA_RANGE_V1 = 0x01 as const;

	static partitionIdToBytes(partitionId: PartitionNodeId): Uint8Array {
		return Uint8Array.fromHex(partitionId);
	}

	static isHashPartition(partitionId: PartitionNodeId): boolean {
		// PartitionID are hex-encoded bytes with a schema version byte prefix,
		// so we can peek the first byte to determine the type without full decoding.
		// This is important for efficient routing in the DOs.
		const bytes = Number.parseInt(partitionId.substring(0, 2), 16);
		return bytes === PartitionIdHelper.SCHEMA_HASH_V1;
	}

	static isRangePartition(partitionId: PartitionNodeId): boolean {
		// PartitionID are hex-encoded bytes with a schema version byte prefix,
		// so we can peek the first byte to determine the type without full decoding.
		// This is important for efficient routing in the DOs.
		const bytes = Number.parseInt(partitionId.substring(0, 2), 16);
		return bytes === PartitionIdHelper.SCHEMA_RANGE_V1;
	}

	static doName(basePartitionContext: PartitionContext, bytes: Uint8Array): string {
		if (bytes[0] === PartitionIdHelper.SCHEMA_HASH_V1) {
			const root = (bytes[1] << 8) | bytes[2];
			const depth = bytes[3];
			const suffix = depth > 0 ? "." + bytes.subarray(4, 4 + depth).join(".") : "";
			return `${basePartitionContext.databaseName}.h.${root}${suffix}`;
		}
		invariant(bytes[0] === PartitionIdHelper.SCHEMA_RANGE_V1, `fokos/topology: unsupported partition ID schema version: ${bytes[0]}`);
		const decoded = PartitionIdHelper.decode(bytes);
		invariant(decoded.schema === PartitionIdHelper.SCHEMA_RANGE_V1, "fokos/topology.doName: unreachable");
		return rangePartitionDoName(basePartitionContext.databaseName, decoded.hashKey, decoded.startBoundary, decoded.endBoundary);
	}

	// Decode a partition ID bytes to a schema-specific representation.
	static decode(
		bytes: Uint8Array,
	):
		| { schema: 0; rootIdx: number; depth: number }
		| { schema: 1; hashKey: string; startBoundary: string | null; endBoundary: string | null } {
		if (bytes[0] === PartitionIdHelper.SCHEMA_HASH_V1) {
			return { schema: 0, rootIdx: (bytes[1] << 8) | bytes[2], depth: bytes[3] };
		}
		invariant(bytes[0] === PartitionIdHelper.SCHEMA_RANGE_V1, `fokos/topology.decode: unsupported schema version: ${bytes[0]}`);
		// SCHEMA_RANGE_V1 wire format (both boundaries are immutable identity; null = unbounded edge):
		//   byte[0]     = 0x01
		//   byte[1]     = flags: bit0 = hasStartBoundary, bit1 = hasEndBoundary (absent bit ⇒ unbounded)
		//   byte[2..5]  = uint32 LE length of hashKey UTF-8 bytes (hkLen)
		//   byte[6..9]  = uint32 LE length of startBoundary UTF-8 bytes (startLen; 0 if !hasStart)
		//   byte[10..]  = hashKey bytes, then startBoundary bytes (startLen), then endBoundary bytes (rest, if hasEnd)
		const hasStart = (bytes[1] & 0x01) !== 0;
		const hasEnd = (bytes[1] & 0x02) !== 0;
		const hkLen = bytes[2] | (bytes[3] << 8) | (bytes[4] << 16) | (bytes[5] << 24);
		const startLen = bytes[6] | (bytes[7] << 8) | (bytes[8] << 16) | (bytes[9] << 24);
		const hkStart = 10;
		const skStart = hkStart + hkLen;
		const endStart = skStart + startLen;
		const hashKey = new TextDecoder().decode(bytes.subarray(hkStart, skStart));
		const startBoundary = hasStart ? new TextDecoder().decode(bytes.subarray(skStart, endStart)) : null;
		const endBoundary = hasEnd ? new TextDecoder().decode(bytes.subarray(endStart)) : null;
		return { schema: 1, hashKey, startBoundary, endBoundary };
	}

	// Creates a PartitionIdHelper for a range-structure DO. null start/end = unbounded edge.
	static fromRangePartition(
		base: PartitionContext,
		hashKey: string,
		startBoundary: string | null,
		endBoundary: string | null,
	): PartitionIdHelper {
		const hkBytes = new TextEncoder().encode(hashKey);
		const hasStart = startBoundary !== null;
		const hasEnd = endBoundary !== null;
		const skBytes = hasStart ? new TextEncoder().encode(startBoundary) : new Uint8Array(0);
		const endBytes = hasEnd ? new TextEncoder().encode(endBoundary) : new Uint8Array(0);
		const bytes = new Uint8Array(10 + hkBytes.length + skBytes.length + endBytes.length);
		bytes[0] = PartitionIdHelper.SCHEMA_RANGE_V1;
		bytes[1] = (hasStart ? 0x01 : 0x00) | (hasEnd ? 0x02 : 0x00);
		const hkLen = hkBytes.length;
		bytes[2] = hkLen & 0xff;
		bytes[3] = (hkLen >> 8) & 0xff;
		bytes[4] = (hkLen >> 16) & 0xff;
		bytes[5] = (hkLen >> 24) & 0xff;
		const startLen = skBytes.length;
		bytes[6] = startLen & 0xff;
		bytes[7] = (startLen >> 8) & 0xff;
		bytes[8] = (startLen >> 16) & 0xff;
		bytes[9] = (startLen >> 24) & 0xff;
		bytes.set(hkBytes, 10);
		bytes.set(skBytes, 10 + hkLen);
		bytes.set(endBytes, 10 + hkLen + startLen);
		return new PartitionIdHelper(base, bytes);
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

	// Readers for the encoded partition ID bytes — SCHEMA_HASH_V1 only.
	// Format: [schemaVersion u8, rootIdx u16, depth u8, hashIdx_1 u8, ..., hashIdx_depth u8]
	static rootIdx(bytes: Uint8Array): number {
		invariant(bytes[0] === PartitionIdHelper.SCHEMA_HASH_V1, `fokos/topology: expected hash schema, got: ${bytes[0]}`);
		return (bytes[1] << 8) | bytes[2];
	}
	static depth(bytes: Uint8Array): number {
		invariant(bytes[0] === PartitionIdHelper.SCHEMA_HASH_V1, `fokos/topology: expected hash schema, got: ${bytes[0]}`);
		return bytes[3];
	}
	// The last child index is this partition's slot among its siblings (only valid when depth >= 1).
	static lastChildIdx(bytes: Uint8Array): number {
		invariant(bytes[0] === PartitionIdHelper.SCHEMA_HASH_V1, `fokos/topology: expected hash schema, got: ${bytes[0]}`);
		return bytes[3 + bytes[3]];
	}

	/**
	 * Refactor maybe to have different classes for hash and range partition IDs, to avoid the need for schema checks in these helper methods.
	 */
	static calculateHashChildPartitionIds(parentContext: PartitionContextResolved): {
		doName: string;
		partitionIdOpaque: string;
	}[] {
		const parentBytes = Uint8Array.fromHex(parentContext.partitionId);
		invariant(parentBytes[0] === PartitionIdHelper.SCHEMA_HASH_V1, `fokos/topology: expected hash schema, got: ${parentBytes[0]}`);
		const result = Array.from({ length: parentContext.hashSplitN }, (_, i) => {
			const { doName, opaque } = new PartitionIdHelper(parentContext, parentBytes).appendHashIdx(i).encode(true);
			return {
				doName: doName!,
				partitionIdOpaque: opaque,
			};
		});
		invariant(
			result.length === parentContext.hashSplitN,
			`fokos/topology.calculateChildPartitionIds: expected ${parentContext.hashSplitN} children, got ${result.length}`,
		);
		return result;
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
		let bytes: Uint8Array;
		if (this.#bytes && this.#bytes[0] === PartitionIdHelper.SCHEMA_RANGE_V1) {
			// Range partition: bytes are self-contained; hash-index appending is not valid.
			invariant(this.#appendedHashIdxs.length === 0, "fokos/topology.encode: cannot append hash indexes to a range partition ID");
			bytes = this.#bytes;
		} else if (this.#bytes) {
			invariant(
				this.#bytes[0] === PartitionIdHelper.SCHEMA_HASH_V1,
				`fokos/topology.encode: unexpected schema version byte: ${this.#bytes[0]}`,
			);
			invariant(this.#bytes.length >= 4, "fokos/topology.encode: existing bytes too short to be valid");
			// Extending an existing hash partition: append child indexes (u8 each).
			bytes = new Uint8Array(this.#bytes.length + this.#appendedHashIdxs.length);
			bytes.set(this.#bytes, 0);
			const bsz = this.#bytes.length;
			// bytes[0..2] = version + rootIdx — leave unchanged.
			bytes[3] = bsz - 4 + this.#appendedHashIdxs.length; // new depth (u8)
			for (let i = 0; i < this.#appendedHashIdxs.length; i++) bytes[bsz + i] = this.#appendedHashIdxs[i];
		} else {
			// Fresh hash instance: appendedHashIdxs[0] is the root index (u16), rest are child indexes (u8 each).
			const depth = this.#appendedHashIdxs.length - 1;
			bytes = new Uint8Array(4 + depth);
			bytes[0] = PartitionIdHelper.SCHEMA_HASH_V1;
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
	childPartitionContexts(): PartitionContextResolved[] | undefined;

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

	/**
	 * Called after a forwarded request returns. Updates the topology cache from the response.
	 */
	recordForwardResult(hashKey: string, fromCtx: PartitionContextResolved, toCtx: PartitionContextResolved, responseHashDepth: number): void;

	/**
	 * Called by a child partition after it has fully migrated its share of data from the parent.
	 * Idempotent. Transitions the parent to split_completed once all children have acknowledged.
	 */
	acknowledgeChildMigration(childDoName: string): void;
}

// Fraction of rangeSplitConditions.maxSizeMb a single key must reach before it is a promotion candidate.
export const RANGE_PROMOTION_FRACTION = 0.5;

/**
 * Used by the Partition Durable Objects.
 */
export class HashPartitionTopologyImpl implements PartitionTopologySplitter {
	private static readonly KV_KEYS = {
		SPLIT_STATUS: "__split_status",
	};

	#storage: DurableObjectStorage;
	#_hashTopology: HashTopology | null = null;

	constructor(
		private readonly partitionContext: PartitionContextResolved,
		private readonly doCtx: DurableObjectState,
	) {
		this.#storage = doCtx.storage;
		// Load the topology cache eagerly. The constructor is called from ensureTopology() on the
		// first request, after blockConcurrencyWhile has completed, so synchronous KV reads are safe.
		const ownerAbsDepth = PartitionIdHelper.depth(partitionContext._partitionIdBytes ?? Uint8Array.fromHex(partitionContext.partitionId));
		const snapshot = doCtx.storage.kv.get<HashTopologySnapshot>("__topo_cache");
		if (snapshot) {
			this.#_hashTopology = HashTopology.fromSnapshot(snapshot);
		} else {
			const splitStatus = doCtx.storage.kv.get<SplitStatusKVItem>(HashPartitionTopologyImpl.KV_KEYS.SPLIT_STATUS);
			if (splitStatus?.status === "split_started" || splitStatus?.status === "split_completed") {
				this.#_hashTopology = HashTopology.create(partitionContext.hashSplitN, ownerAbsDepth);
			}
		}
	}

	shouldAllow(hashKey: string, sortKey?: string): "forward" | "reject" | "ok" {
		// If the split has started but not completed, we should reject requests to the partition to avoid data loss or returning wrong data.
		// TODO - Keep this in memory to avoid reading it all the time from storage.
		const splitStatus = this.#storage.kv.get<SplitStatusKVItem>(HashPartitionTopologyImpl.KV_KEYS.SPLIT_STATUS);
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

	childPartitionContexts(): PartitionContextResolved[] | undefined {
		const splitStatus = this.splitStatus();
		if (splitStatus?.status === "split_started" || splitStatus?.status === "split_completed") {
			return splitStatus.childPartitionContexts;
		}
	}

	splitStatus(): SplitStatusKVItem | undefined {
		return this.#storage.kv.get<SplitStatusKVItem>(HashPartitionTopologyImpl.KV_KEYS.SPLIT_STATUS);
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
			// Mutual exclusion: no hash split while any promoted key is queued or promoting.
			// A split while promotion is in-flight would leave the range root acking a parent
			// that has become a router, stranding the key's status.
			const inflightCount =
				this.#storage.sql
					.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM promoted_keys WHERE status IN ('queued', 'promoting')`)
					.toArray()[0]?.n ?? 0;
			if (inflightCount > 0) return null;
			return "hash";
		}
		// TODO Track some statistics per hashKey/sortKey in memory to track heavy hitter items.

		// TODO Add more conditions based on the partitionContext.
		return null;
	}

	queueSplit(splitType: SplitType): SplitStatusKVItem {
		const nowStatus = this.splitStatus();
		if (!nowStatus) {
			this.#storage.kv.put<SplitStatusKVItem>(HashPartitionTopologyImpl.KV_KEYS.SPLIT_STATUS, {
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
				const childIds = PartitionIdHelper.calculateHashChildPartitionIds(this.partitionContext);
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
				// Hash-partition DOs never queue a range split; range splits are handled by RangePartitionTopologyImpl.
				invariant(false, "fokos/topology.startSplit: unexpected splitType 'range' on a hash-partition topology");
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
		this.#storage.kv.put<SplitStatusKVItem>(HashPartitionTopologyImpl.KV_KEYS.SPLIT_STATUS, {
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

		// Initialize the topology cache so forwarded requests can learn and skip hops.
		if (!this.#_hashTopology) {
			const ownerAbsDepth = PartitionIdHelper.depth(
				this.partitionContext._partitionIdBytes ?? Uint8Array.fromHex(this.partitionContext.partitionId),
			);
			this.#_hashTopology = HashTopology.create(this.partitionContext.hashSplitN, ownerAbsDepth);
		}

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

		this.#storage.kv.put<SplitStatusKVItem>(HashPartitionTopologyImpl.KV_KEYS.SPLIT_STATUS, newStatus);
	}

	/**
	 * Internally used by the Partition DOs to route requests to their children after a split happened.
	 * This routes to a descendant partition directly according to the specified relative depth.
	 *
	 * Skips `relativeDepthToLeaf` levels in one shot, computing the descendant partition ID deterministically from the hash key and the owner's depth.
	 * Used by the topology cache to skip known intermediate router hops.
	 */
	pickDescendantHashPartition(
		partitionContext: PartitionContextResolved,
		hashKey: string,
		relativeDepthToLeaf: number,
	): { doId: DurableObjectId; partitionContext: PartitionContextResolved } {
		const partitionIdBytes = partitionContext._partitionIdBytes ?? Uint8Array.fromHex(partitionContext.partitionId);
		const parentDepth = PartitionIdHelper.depth(partitionIdBytes);

		const hashIdxs: number[] = [];
		for (let i = 0; i < relativeDepthToLeaf; i++) {
			hashIdxs.push(hashChildIndex(hashKey, parentDepth + i, partitionContext.hashSplitN));
		}

		const { doName, opaque } = new PartitionIdHelper(this.partitionContext, partitionIdBytes).appendHashIdx(hashIdxs).encode(true);
		assertExists(doName);

		const { ns } = this.partitionContext;
		const doId = env[ns].idFromName(doName);
		return {
			doId,
			partitionContext: {
				...partitionContext,
				doName,
				primaryDoIdStr: doId.toString(),
				partitionId: opaque,
			},
		};
	}

	pickChildPartition(
		partitionContext: PartitionContextResolved,
		hashKey: string,
		_sortKey?: string,
	): { doId: DurableObjectId; partitionContext: PartitionContextResolved } {
		if (this.#_hashTopology) {
			// Returns the relative depth of the descendant partition that is non-split according to our cached topology,
			// or 0 if the cache is not populated at all yet.
			const cachedDepth = this.#_hashTopology.findLeaf(hashKey);
			if (cachedDepth > 0) {
				return this.pickDescendantHashPartition(partitionContext, hashKey, cachedDepth);
			}
		}
		// Default to immediate child partitions.
		return this.pickDescendantHashPartition(partitionContext, hashKey, 1);
	}

	makeIsCorrectChildHashPartition(
		_parentContext: PartitionContextResolved,
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
			const hashedIdx = hashChildIndex(hashKey, childLevel - 1, childContext.hashSplitN);
			return hashedIdx === childIdx;
		};
	}

	recordForwardResult(
		hashKey: string,
		fromCtx: PartitionContextResolved,
		toCtx: PartitionContextResolved,
		responseHashDepth: number,
	): void {
		// This logic only makes sense for both being hash partitions.
		// FIXME Support learning during when a hash partition forwards to a range partition,
		// which can happen with promoted hash keys.
		if (!isHashPartition(fromCtx) || !isHashPartition(toCtx)) return;

		// targetRelDepth: how many hash-tree levels this single RPC hop crossed.
		// pickChildPartition may have skipped the cache (e.g. depth-2 skip goes straight to the
		// grandchild), so we derive the actual skip from the partition IDs rather than assuming 1.
		const fromAbsDepth = PartitionIdHelper.depth(fromCtx._partitionIdBytes ?? Uint8Array.fromHex(fromCtx.partitionId));
		const toAbsDepth = PartitionIdHelper.depth(toCtx._partitionIdBytes ?? Uint8Array.fromHex(toCtx.partitionId));
		invariant(
			toAbsDepth > fromAbsDepth,
			`fokos/topology.recordForwardResult: toCtx must be a descendant of fromCtx, got fromAbsDepth ${fromAbsDepth} and toAbsDepth ${toAbsDepth}`,
		);
		// The actual response hash depth may be larger than the targetRelDepth
		// if the target partition is itself a router that forwarded further.
		// It could also be the case that the target hash partition forwarded to a range partition,
		// and in that case the responseHashDepth would be equal to the target partition depth.
		invariant(
			responseHashDepth >= toAbsDepth,
			`fokos/topology.recordForwardResult: responseHashDepth must be >= toAbsDepth, got responseHashDepth ${responseHashDepth} and toAbsDepth ${toAbsDepth}`,
		);

		const targetRelDepth = responseHashDepth - fromAbsDepth;
		if (this.#_hashTopology && targetRelDepth > 0) {
			if (this.#_hashTopology.updateFromHint(hashKey, targetRelDepth)) {
				this.#storage.kv.put<HashTopologySnapshot>("__topo_cache", this.#_hashTopology.toSnapshot());
			}
		}
	}
}

/**
 * Topology splitter for range-structure DOs. A range DO owns exactly one hashKey and a fixed,
 * immutable [startBoundary, endBoundary) slice of the sortKey axis. On split it becomes a pure
 * router (owns nothing locally) and creates N children that tile [start, end) — including a new
 * leftmost child — then forwards every sort key to the owning child. A leaf is never also a router.
 */
export class RangePartitionTopologyImpl implements PartitionTopologySplitter {
	private static readonly KV_KEYS = {
		SPLIT_STATUS: "__split_status",
	};

	#storage: DurableObjectStorage;

	constructor(
		private readonly partitionContext: PartitionContextResolved,
		private readonly ctx: DurableObjectState,
	) {
		this.#storage = ctx.storage;
	}

	splitStatus(): SplitStatusKVItem | undefined {
		return this.#storage.kv.get<SplitStatusKVItem>(RangePartitionTopologyImpl.KV_KEYS.SPLIT_STATUS);
	}

	childPartitionContexts(): PartitionContextResolved[] | undefined {
		const splitStatus = this.splitStatus();
		if (splitStatus?.status === "split_started" || splitStatus?.status === "split_completed") {
			return splitStatus.childPartitionContexts;
		}
		return undefined;
	}

	shouldAllow(hashKey: string, sortKey?: string): "forward" | "reject" | "ok" {
		const sk = sortKey ?? "";

		const splitStatus = this.splitStatus();
		if (splitStatus?.status === "split_started" || splitStatus?.status === "split_completed") {
			// Once split, this DO is a pure router (owns nothing locally) — forward everything to a child.
			return "forward";
		}

		// Boundaries are immutable identity. null = unbounded edge.
		const start = this.partitionContext.rangePartition!.startBoundary ?? "";
		const end = this.partitionContext.rangePartition!.endBoundary;
		const inRange = sk >= start && (end === null || sk < end);
		if (!inRange) {
			// Out of owned range — routing bug; should not happen via correct routing.
			return "reject";
		}

		// Size-based backpressure (10% overage allowed, consistent with hash partition).
		if (
			this.partitionContext.rangeSplitConditions?.maxSizeMb &&
			this.#storage.sql.databaseSize > this.partitionContext.rangeSplitConditions.maxSizeMb * 1.1 * 1024 * 1024
		) {
			return "reject";
		}

		return "ok";
	}

	async maybeQueueSplit(_hashKey: string, _sortKey?: string): Promise<SplitStatusKVItem | undefined> {
		const splitType = this.shouldSplit();
		if (splitType) {
			return this.queueSplit(splitType);
		}
	}

	private shouldSplit(): SplitType | null {
		if (!this.partitionContext.rangeSplitConditions) return null;
		const dbSize = this.#storage.sql.databaseSize;
		if (
			this.partitionContext.rangeSplitConditions.maxSizeMb &&
			dbSize > this.partitionContext.rangeSplitConditions.maxSizeMb * 1024 * 1024
		) {
			return "range";
		}
		return null;
	}

	private queueSplit(splitType: SplitType): SplitStatusKVItem {
		const nowStatus = this.splitStatus();
		if (!nowStatus) {
			this.#storage.kv.put<SplitStatusKVItem>(RangePartitionTopologyImpl.KV_KEYS.SPLIT_STATUS, {
				status: "split_queued",
				splitType,
				createdAt: Date.now(),
				partitionContext: this.partitionContext,
			});
		}
		const written = this.splitStatus();
		invariant(written != null, "fokos/range: queueSplit: KV write returned null");
		return written;
	}

	async startSplit(): Promise<void> {
		const splitStatus = this.splitStatus();
		if (!splitStatus || splitStatus.status !== "split_queued") {
			// Already started or completed — idempotent no-op.
			return;
		}

		const rp = this.partitionContext.rangePartition;
		invariant(rp, "fokos/range.startSplit: missing rangePartition identity");
		const N = this.partitionContext.rangeSplitN;
		invariant(N != null && N >= 2, "fokos/range.startSplit: rangeSplitN must be >= 2");

		const hashKey = rp.hashKey;
		const start = rp.startBoundary; // null = −∞
		const end = rp.endBoundary; // null = +∞

		// Compute N-1 split boundaries within the owned slice [start, end) in one snapshot.
		const boundaries = this.computeRangeSplitBoundaries(hashKey, start, end, N);
		if (!boundaries) {
			// Not enough distinct items to split into N non-empty children — retry on a later cycle.
			console.log({
				message: "fokos/range.startSplit: insufficient items to split into N children; will retry.",
				doName: this.partitionContext.doName,
			});
			return;
		}

		// The N children tile [start, end): child i owns [starts[i], ends[i]). The leftmost child
		// (start, B1) is a brand-new DO — this node retains no slice and becomes a pure router.
		const starts: (string | null)[] = [start, ...boundaries];
		const ends: (string | null)[] = [...boundaries, end];
		const childInits: InitFromSplitOptions[] = [];
		for (let i = 0; i < N; i++) {
			const { partitionContext: childCtx } = resolveRangePartitionContext(this.partitionContext, hashKey, starts[i], ends[i]);
			childInits.push({
				parentPartitionContext: this.partitionContext,
				newPartitionContext: childCtx,
				splitType: "range",
			});
		}
		const uniqueNames = new Set(childInits.map((c) => c.newPartitionContext.doName));
		invariant(uniqueNames.size === childInits.length, "fokos/range.startSplit: duplicate child doNames detected");

		// Initialize all children (retry ≤5). Abort on failure; the split retries next cycle.
		try {
			await Promise.all(
				childInits.map((childContext) =>
					tryWhile(
						async () => {
							const doId = env[childContext.newPartitionContext.ns].idFromName(childContext.newPartitionContext.doName);
							return await env[childContext.newPartitionContext.ns].get(doId).initFromSplit(childContext);
						},
						(_error, nextAttempt) => nextAttempt <= 5,
					),
				),
			);
		} catch (error) {
			console.error({
				message: "fokos/range.startSplit: child initialization failed, aborting; will retry later.",
				error: String(error),
			});
			throw error;
		}

		// Become a pure router: persist split_started with exactly the N children (set once, never accumulates).
		this.#storage.kv.put<SplitStatusKVItem>(RangePartitionTopologyImpl.KV_KEYS.SPLIT_STATUS, {
			status: "split_started",
			splitType: "range",
			createdAt: Date.now(),
			partitionContext: this.partitionContext,
			childPartitionContexts: childInits.map((c) => c.newPartitionContext),
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

		// Kick off migration on all children immediately (fire-and-forget; each also starts on its first request).
		await Promise.allSettled(
			childInits.map(async (childContext) => {
				try {
					const doId = env[childContext.newPartitionContext.ns].idFromName(childContext.newPartitionContext.doName);
					await env[childContext.newPartitionContext.ns].get(doId).triggerMigration();
				} catch (error) {
					console.error({
						message: "fokos/range.startSplit: failed to trigger child migration; will start on first request.",
						error: String(error),
					});
				}
			}),
		);
	}

	// Computes N-1 strictly-increasing split boundaries (count-quantiles) within [start, end) in one
	// transactionSync snapshot. Returns null if there are fewer than N items (so every child stays non-empty).
	private computeRangeSplitBoundaries(hashKey: string, start: string | null, end: string | null, N: number): string[] | null {
		return this.ctx.storage.transactionSync(() => {
			const lower = start ?? ""; // −∞ ⇒ sk >= ''
			const cntRow =
				end === null
					? this.#storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM items WHERE hk = ? AND sk >= ?`, hashKey, lower).toArray()[0]
					: this.#storage.sql
							.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM items WHERE hk = ? AND sk >= ? AND sk < ?`, hashKey, lower, end)
							.toArray()[0];
			const cnt = cntRow?.n ?? 0;
			if (cnt < N) return null; // need ≥ N items so each of the N children gets ≥ 1

			const boundaries: string[] = [];
			for (let i = 1; i < N; i++) {
				const offset = Math.floor((cnt * i) / N);
				const row =
					end === null
						? this.#storage.sql
								.exec<{ sk: string }>(`SELECT sk FROM items WHERE hk = ? AND sk >= ? ORDER BY sk LIMIT 1 OFFSET ?`, hashKey, lower, offset)
								.toArray()[0]
						: this.#storage.sql
								.exec<{
									sk: string;
								}>(`SELECT sk FROM items WHERE hk = ? AND sk >= ? AND sk < ? ORDER BY sk LIMIT 1 OFFSET ?`, hashKey, lower, end, offset)
								.toArray()[0];
				invariant(row, "fokos/range.computeRangeSplitBoundaries: expected a row at the computed offset");
				boundaries.push(row.sk);
			}
			// Boundaries must be strictly above the lower bound and strictly increasing (distinct, non-empty children).
			for (let i = 0; i < boundaries.length; i++) {
				if (boundaries[i] <= lower) return null;
				if (i > 0 && boundaries[i] <= boundaries[i - 1]) return null;
			}
			return boundaries;
		});
	}

	pickChildPartition(
		partitionContext: PartitionContextResolved,
		hashKey: string,
		sortKey?: string,
	): { doId: DurableObjectId; partitionContext: PartitionContextResolved } {
		const sk = sortKey ?? "";
		const splitStatus = this.splitStatus();
		invariant(
			splitStatus?.status === "split_started" || splitStatus?.status === "split_completed",
			"fokos/range: pickChildPartition called without an active split",
		);

		// TODO: Keep childPartitionContexts sorted by startBoundary so we can break early once
		// childStart > sk, or use binary search for arrays larger than ~10 entries.
		// The N children tile the whole owned range; route to the one with the largest startBoundary <= sk.
		let best: PartitionContextResolved | null = null;
		for (const childCtx of splitStatus.childPartitionContexts) {
			const childStart = childCtx.rangePartition!.startBoundary ?? "";
			if (childStart <= sk) {
				if (best === null || childStart > (best.rangePartition!.startBoundary ?? "")) {
					best = childCtx;
				}
			}
		}
		invariant(best !== null, `fokos/range: no child found for sortKey "${sk}"`);

		const childId = env[partitionContext.ns].idFromName(best.doName);
		return { doId: childId, partitionContext: best };
	}

	acknowledgeChildMigration(childDoName: string): void {
		const splitStatus = this.splitStatus();
		invariant(splitStatus, "fokos/range: acknowledgeChildMigration: splitStatus must exist");
		if (splitStatus.status === "split_completed") return; // idempotent
		invariant(splitStatus.status === "split_started", `fokos/range: acknowledgeChildMigration: unexpected status ${splitStatus.status}`);
		if (splitStatus.migratedChildDoNames.includes(childDoName)) return;

		const migratedChildDoNames = [...splitStatus.migratedChildDoNames, childDoName];
		invariant(
			migratedChildDoNames.length <= splitStatus.childPartitionContexts.length,
			`fokos/range: acknowledgeChildMigration: more acks (${migratedChildDoNames.length}) than children (${splitStatus.childPartitionContexts.length})`,
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

		this.#storage.kv.put<SplitStatusKVItem>(RangePartitionTopologyImpl.KV_KEYS.SPLIT_STATUS, newStatus);
	}

	recordForwardResult(
		_hashKey: string,
		_fromCtx: PartitionContextResolved,
		_toCtx: PartitionContextResolved,
		_responseHashDepth: number,
	): void {
		return;
	}
}
