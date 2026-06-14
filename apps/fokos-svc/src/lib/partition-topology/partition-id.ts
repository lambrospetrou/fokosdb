import { env } from "cloudflare:workers";
import type { PartitionContext, PartitionContextResolved, PartitionNamespaceKey } from "./partition-context.js";
import type { PartitionNodeId } from "./types.js";
import { GOLDEN_RATIO as _GOLDEN_RATIO, hashChildIndex as _hashChildIndex, hashRootIndex as _hashRootIndex } from "./hash-primitives.js";
import { assertExists } from "../tsutils.js";
import invariant from "../invariant.js";

/**
 * Pure partition-identity codec: the opaque partition ID wire formats, DO naming, and
 * deterministic ID resolution. Boundary rule: `idFromName` resolution is allowed here (it is
 * deterministic and performs no I/O); stub acquisition (`.get`) is NOT — only DO classes and
 * FokosDB hold stubs.
 */

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
export function rangePartitionDoName(tableName: string, hashKey: string, startBoundary: string | null, endBoundary: string | null): string {
	const hk = encodeRangeComponent(hashKey);
	const start = startBoundary === null ? RANGE_MIN : encodeRangeComponent(startBoundary);
	const end = endBoundary === null ? RANGE_MAX : encodeRangeComponent(endBoundary);
	return `${tableName}.r.${hk}.${start}.${end}`;
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

/** Deterministic DO ID resolution (no I/O). The split policies use this so `env` access stays in this file. */
export function resolveDoId(ns: PartitionNamespaceKey, doName: string): DurableObjectId {
	return env[ns].idFromName(doName);
}

// Resolves the N hash child partition contexts of a splitting hash parent.
export function resolveHashChildPartitionContexts(parentContext: PartitionContextResolved): PartitionContextResolved[] {
	const childIds = PartitionIdHelper.calculateHashChildPartitionIds(parentContext);
	return childIds.map(({ doName, partitionIdOpaque }) => {
		const childDoId = env[parentContext.ns].idFromName(doName);
		return {
			...parentContext,
			doName,
			primaryDoIdStr: childDoId.toString(),
			partitionId: partitionIdOpaque,
		};
	});
}

// Resolves a descendant hash partition context by appending child indexes to the owner's encoded ID.
export function resolveDescendantHashPartitionContext(
	basePartitionContext: PartitionContext,
	partitionContext: PartitionContextResolved,
	partitionIdBytes: Uint8Array,
	hashIdxs: number[],
): { doId: DurableObjectId; partitionContext: PartitionContextResolved } {
	const { doName, opaque } = new PartitionIdHelper(basePartitionContext, partitionIdBytes).appendHashIdx(hashIdxs).encode(true);
	assertExists(doName);
	const doId = env[basePartitionContext.ns].idFromName(doName);
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

// Re-exported from hash-primitives.ts (lives there to break the circular dependency with hash-topology.ts).
export const GOLDEN_RATIO = _GOLDEN_RATIO;
export const hashChildIndex = _hashChildIndex;
export const hashRootIndex = _hashRootIndex;

export class PartitionIdHelper {
	static readonly SCHEMA_HASH_V1 = 0x00 as const;
	static readonly SCHEMA_HASH_V1_STR = "00" as const;

	static readonly SCHEMA_RANGE_V1 = 0x01 as const;
	static readonly SCHEMA_RANGE_V1_STR = "01" as const;

	static partitionIdToBytes(partitionId: PartitionNodeId): Uint8Array {
		return Uint8Array.fromHex(partitionId);
	}

	static isHashPartition(partitionId: PartitionNodeId): boolean {
		// PartitionID are hex-encoded bytes with a schema version byte prefix,
		// so we can peek the first byte to determine the type without full decoding.
		// This is important for efficient routing in the DOs.
		// const bytes = Number.parseInt(partitionId.substring(0, 2), 16);
		// return bytes === PartitionIdHelper.SCHEMA_HASH_V1;
		return partitionId.startsWith(PartitionIdHelper.SCHEMA_HASH_V1_STR);
	}

	static isRangePartition(partitionId: PartitionNodeId): boolean {
		// PartitionID are hex-encoded bytes with a schema version byte prefix,
		// so we can peek the first byte to determine the type without full decoding.
		// This is important for efficient routing in the DOs.
		return partitionId.startsWith(PartitionIdHelper.SCHEMA_RANGE_V1_STR);
	}

	static doName(basePartitionContext: PartitionContext, bytes: Uint8Array): string {
		if (bytes[0] === PartitionIdHelper.SCHEMA_HASH_V1) {
			const root = (bytes[1] << 8) | bytes[2];
			const depth = bytes[3];
			const suffix = depth > 0 ? "." + bytes.subarray(4, 4 + depth).join(".") : "";
			return `${basePartitionContext.tableName}.h.${root}${suffix}`;
		}
		invariant(bytes[0] === PartitionIdHelper.SCHEMA_RANGE_V1, `fokos/topology: unsupported partition ID schema version: ${bytes[0]}`);
		const decoded = PartitionIdHelper.decode(bytes);
		invariant(decoded.schema === PartitionIdHelper.SCHEMA_RANGE_V1, "fokos/topology.doName: unreachable");
		return rangePartitionDoName(basePartitionContext.tableName, decoded.hashKey, decoded.startBoundary, decoded.endBoundary);
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
