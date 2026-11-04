import type { PartitionNodeId } from "./types.js";
import type { RangeAncestorInfo } from "./types.js";
import type { FokosPartitionIdentity, FokosPartitionRef, FokosRouteContext, FokosTopology } from "./route-context.js";
import {
	asRoutingHash,
	ROUTING_HASH_BYTES,
	routingHashInterval,
	routingHashSeedOf,
	maxHashDepth,
	type RoutingHash,
} from "./hash-primitives.js";
import { KeyCodec, type FokosCompositeKey, type KeyBytes } from "./key-codec.js";
import invariant from "../shared/invariant.js";
import { FokosValidationError } from "../shared/errors.js";
import { SHARDING_VALIDATION_CODES } from "./errors.js";

/**
 * Pure partition-identity codec: the opaque partition ID wire formats and DO naming. A route context
 * for another partition is derived here from the caller's own context, so the immutable identity is
 * computed and the mutable parts travel unchanged. Nothing here resolves a Durable Object ID or a
 * stub: `idFromName` recreates the deterministic ID wherever a stub is made.
 */

/**
 * The separator between the shard group and the rest of a DO name. A shard group must not contain
 * it (`validateTopology`), so the first "~" of a name always ends the shard group.
 */
export const SHARD_GROUP_SEPARATOR = "~";

// Reserved sentinel tokens for the unbounded edges of a range, used ONLY in DO names (never in
// routing comparisons — there boundaries stay `FokosCompositeKey | null` with null = unbounded). The
// component encoding escapes "~", and base64url has no "~", so an encoded component never starts with "~m".
export const RANGE_MIN = "~min";
export const RANGE_MAX = "~max";

// The marker of a component in the base64url form.
const BINARY_COMPONENT_MARKER = "~b";

// The "%XX" escape of each byte, with uppercase hex digits.
const PERCENT_HEX: string[] = [];
for (let b = 0; b < 256; b++) {
	PERCENT_HEX.push("%" + b.toString(16).padStart(2, "0").toUpperCase());
}

// The two lowercase hex digits of each byte, as `Uint8Array.prototype.toHex` gives them.
const BYTE_HEX: string[] = [];
for (let b = 0; b < 256; b++) {
	BYTE_HEX.push(b.toString(16).padStart(2, "0"));
}
const NO_CHILD_IDXS: readonly number[] = Object.freeze([]);

// The escape set of the text form, as fixed code point ranges. A change to this set, or to
// `isEscapedCodeUnit`, changes DO names and makes the data of the partitions unreachable. Do not use
// Unicode properties here: their content changes with the Unicode version of the runtime.
// U+0000–U+0020 controls and space, `"`, `%`, `.`, `\`, U+007E–U+009F (`~`, DEL, C1 controls),
// zero-width and direction marks, line and paragraph separators, bidirectional controls, and U+FEFF.
// oxlint-disable-next-line no-control-regex
const ESCAPE_SET = /[\u0000-\u0020\u0022\u0025\u002e\u005c\u007e-\u009f\u200b-\u200f\u2028\u2029\u202a-\u202e\u2066-\u2069\ufeff]/;

/** True when the code unit is in the escape set. It must agree with `ESCAPE_SET`. */
function isEscapedCodeUnit(cu: number): boolean {
	if (cu < 0xa0) {
		return cu <= 0x20 || cu >= 0x7e || cu === 0x22 || cu === 0x25 || cu === 0x2e || cu === 0x5c;
	}
	return (
		(cu >= 0x200b && cu <= 0x200f) ||
		cu === 0x2028 ||
		cu === 0x2029 ||
		(cu >= 0x202a && cu <= 0x202e) ||
		(cu >= 0x2066 && cu <= 0x2069) ||
		cu === 0xfeff
	);
}

/**
 * The "%XX" escapes of the UTF-8 bytes of a code unit of the escape set. It writes the UTF-8 bytes
 * directly, with no `TextEncoder` call:
 *
 *   U+0000–U+007F: 1 byte   0xxxxxxx
 *   U+0080–U+07FF: 2 bytes  110xxxxx 10xxxxxx
 *   U+0800–U+FFFF: 3 bytes  1110xxxx 10xxxxxx 10xxxxxx
 *
 * The lead byte holds the high bits of the code unit after its length marker (0xC0 or 0xE0). Each
 * continuation byte holds the next 6 bits after the marker 0x80. Each code point of the escape set is
 * below U+10000 and is not a surrogate, so one code unit is the whole character, and the 4-byte form
 * of UTF-8 does not occur.
 */
function escapeCodeUnit(cu: number): string {
	if (cu < 0x80) {
		return PERCENT_HEX[cu];
	}
	if (cu < 0x800) {
		// The high 5 bits, then the low 6 bits.
		return PERCENT_HEX[0xc0 | (cu >> 6)] + PERCENT_HEX[0x80 | (cu & 0x3f)];
	}
	// The high 4 bits, then the middle 6 bits, then the low 6 bits.
	return PERCENT_HEX[0xe0 | (cu >> 12)] + PERCENT_HEX[0x80 | ((cu >> 6) & 0x3f)] + PERCENT_HEX[0x80 | (cu & 0x3f)];
}

/** Escapes each code unit of the escape set, and appends the text between two escapes as one slice. */
function escapeText(text: string): string {
	let out = "";
	let last = 0;
	for (let i = 0; i < text.length; i++) {
		const cu = text.charCodeAt(i);
		if (isEscapedCodeUnit(cu)) {
			out += text.slice(last, i) + escapeCodeUnit(cu);
			last = i + 1;
		}
	}
	return out + text.slice(last);
}

/**
 * The start of the incomplete UTF-8 character at the end of `bytes`, or `bytes.length` when the last
 * character is complete.
 *
 * A range boundary is a byte prefix of a string key, so the cut can stop inside a multi-byte
 * character. For example, "録" is E9 8C B2, and a boundary can end with E9 or E9 8C. The encoder
 * decodes the bytes before the tail as text, and writes each tail byte as "%XX".
 *
 * How it works: a UTF-8 character is one lead byte and then 0 to 3 continuation bytes (10xxxxxx). The
 * loop goes back over the continuation bytes at the end, at most 3, to the last lead byte. The lead
 * byte gives the length of its character: 110xxxxx is 2 bytes, 1110xxxx is 3, and 11110xxx is 4. When
 * fewer bytes follow the lead byte than that length, the character is incomplete and the tail starts
 * at the lead byte.
 *
 * Invariant: the tail has 0 to 3 bytes, it is only at the end, and it is the start of a character
 * that the cut stopped. Thus for a valid UTF-8 string cut at any byte, the bytes before the tail are
 * valid UTF-8, and the decode succeeds. For any other bytes the result is still a correct index, but
 * the decode can fail, and then the encoder uses the "~b" form. Examples:
 *
 *   61 E9 8C        lead E9 needs 3 bytes, 2 are present  --> tail starts at index 1
 *   61 F0 9F 8E 89  lead F0 needs 4 bytes, 4 are present  --> no tail
 *   61 62           the last byte is ASCII                --> no tail
 *   61 F8           F8 is not a lead byte                 --> no tail, and the decode fails
 */
function incompleteTailStart(bytes: Uint8Array): number {
	for (let i = bytes.length - 1; i >= Math.max(0, bytes.length - 3); i--) {
		const b = bytes[i];
		// A continuation byte (10xxxxxx): continue back to the lead byte.
		if ((b & 0xc0) === 0x80) {
			continue;
		}
		// The length of the character that starts here. 1 for ASCII and for a byte that starts no character.
		const length = b >= 0xc0 && b <= 0xdf ? 2 : b >= 0xe0 && b <= 0xef ? 3 : b >= 0xf0 && b <= 0xf7 ? 4 : 1;
		return bytes.length - i < length ? i : bytes.length;
	}
	// The last 3 bytes are continuation bytes. They end a complete 4-byte character, or the bytes are
	// not UTF-8 and the decode fails.
	return bytes.length;
}

// `ignoreBOM` keeps a leading U+FEFF, so that the escape step writes it into the name.
const UTF8_DECODER = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

/**
 * The DO name form of one range component. It is deterministic and injective, and it never contains ".":
 * - A binary key (first byte 0xFF) gives "~b" and the unpadded base64url of all its bytes.
 * - A string key gives its text. Each code unit of the escape set becomes "%XX" for each of its UTF-8
 *   bytes, and each byte of an incomplete UTF-8 sequence at the end becomes "%XX". "%" is in the
 *   escape set, so each "%XX" is exactly one byte.
 * - Bytes that are not valid UTF-8 before the tail give the "~b" form. No string key gives them.
 * The DO name is the address of the partition data, so this encoding must never change.
 */
export function encodeRangeComponent(bytes: KeyBytes): string {
	if (bytes[0] === 0xff) {
		return BINARY_COMPONENT_MARKER + toBase64Url(bytes);
	}
	const tail = incompleteTailStart(bytes);
	let text: string;
	try {
		text = UTF8_DECODER.decode(tail === bytes.length ? bytes : bytes.subarray(0, tail));
	} catch {
		return BINARY_COMPONENT_MARKER + toBase64Url(bytes);
	}
	if (ESCAPE_SET.test(text)) {
		text = escapeText(text);
	}
	for (let i = tail; i < bytes.length; i++) {
		text += PERCENT_HEX[bytes[i]];
	}
	return text;
}

/** The DO name form of one boundary: `~min` or `~max` for an unbounded edge, else its two components. */
function rangeBoundaryName(boundary: FokosCompositeKey | null, unbounded: string): string {
	return boundary === null ? unbounded : `${encodeRangeComponent(boundary.hk)}.${encodeRangeComponent(boundary.sk)}`;
}

/**
 * The DO name of a range partition: `<shardGroup>~r.<routingHash hex>.<start>.<end>`. A bounded
 * edge has two components, the hash key and the sort key, and an unbounded edge is `~min` or `~max`.
 * No component contains "." and no component starts with "~m", so a reader knows the count of
 * components of each edge, and the name is injective. A range root is `<shardGroup>~r.<hex>.~min.~max`.
 * The "r." and "h." markers after the separator keep range and hash DO names disjoint.
 */
function rangePartitionDoName(
	shardGroup: string,
	routingHash: RoutingHash,
	startBoundary: FokosCompositeKey | null,
	endBoundary: FokosCompositeKey | null,
): string {
	// FIXME: when the two boundaries share leading bytes, the DO name stores those bytes two times.
	const start = rangeBoundaryName(startBoundary, RANGE_MIN);
	const end = rangeBoundaryName(endBoundary, RANGE_MAX);
	return `${shardGroup}${SHARD_GROUP_SEPARATOR}r.${routingHash.toHex()}.${start}.${end}`;
}

/** The route context of a range partition (root or child) of the same shard group as `base`. */
export function resolveRangePartitionContext<P>(
	base: FokosRouteContext<P>,
	routingHash: RoutingHash,
	startBoundary: FokosCompositeKey | null,
	endBoundary: FokosCompositeKey | null,
): FokosRouteContext<P> {
	// The ID and the DO name come from the same keys, so no ID decode is necessary.
	const partitionId = rangeBytesToPartitionId(encodeRangeBytes(routingHash, startBoundary, endBoundary));
	const doName = rangePartitionDoName(base.topology.shardGroup, routingHash, startBoundary, endBoundary);
	return { ...base, doName, partitionId };
}

/** The route contexts of the N hash children of a splitting hash parent. */
export function resolveHashChildPartitionContexts<P>(parent: FokosRouteContext<P>): FokosRouteContext<P>[] {
	return PartitionIdHelper.calculateHashChildPartitionIds(parent).map(({ doName, partitionIdOpaque }) => ({
		...parent,
		doName,
		partitionId: partitionIdOpaque,
	}));
}

/** The route context of the hash partition `hashIdxs` levels below the hash partition `parent`. */
export function resolveDescendantHashPartitionContext<P>(parent: FokosRouteContext<P>, hashIdxs: readonly number[]): FokosRouteContext<P> {
	const { doName, opaque } = PartitionIdHelper.hashDescendantId(parent, hashIdxs);
	return { ...parent, doName, partitionId: opaque };
}

/**
 * Decodes the stored identity of a partition from its route context. A range partition also needs
 * the depth and the ancestors that only its `fokosInit` carries.
 */
export function partitionIdentityFrom(
	ctx: FokosRouteContext<unknown>,
	range?: { depth: number; ancestors: RangeAncestorInfo[] },
): FokosPartitionIdentity {
	const bytes = PartitionIdHelper.partitionIdToBytes(ctx.partitionId);
	const decoded = PartitionIdHelper.decode(bytes);
	const ref = { partitionId: ctx.partitionId, doName: ctx.doName };
	if (decoded.schema === PartitionIdHelper.SCHEMA_HASH_V1) {
		// `hashDescendantId` builds the DO name of a descendant from the DO name of this partition.
		invariant(
			ctx.doName === PartitionIdHelper.doName(ctx.topology.shardGroup, bytes),
			"fokos/topology.partitionIdentityFrom: the DO name does not match the partition ID",
		);
		checkHashDepth(decoded.depth, ctx.topology);
		const path = Array.from(bytes.subarray(4, 4 + decoded.depth));
		const { rootTreesN, hashSplitN } = ctx.topology;
		const interval = routingHashInterval(rootTreesN, hashSplitN, decoded.rootIdx, path);
		return {
			schema: 1,
			ref,
			kind: "hash",
			hash: { rootIndex: decoded.rootIdx, path, depth: decoded.depth, ...interval },
			topology: ctx.topology,
			routingHashSeed: routingHashSeedOf(ctx.topology.shardGroup),
		};
	}
	invariant(range, "fokos/topology.partitionIdentityFrom: a range partition needs its depth and ancestors");
	return {
		schema: 1,
		ref,
		kind: "range",
		range: { routingHash: decoded.routingHash, start: decoded.startBoundary, end: decoded.endBoundary, ...range },
		topology: ctx.topology,
		routingHashSeed: routingHashSeedOf(ctx.topology.shardGroup),
	};
}

/** The depth of a partition in its tree: the hash child path length, or the range depth its `fokosInit` gave it. */
export function identityDepth(identity: FokosPartitionIdentity): number {
	return identity.hash ? identity.hash.path.length : identity.range!.depth;
}

// The range ID is "01" + base64url(first) + "." + base64url(second), both parts unpadded. The dot is
// a separator, not base64url data. The flat bytes are 0x01 + first + second:
//   first:  flags u8 (bit0 = has start, bit1 = has end), then the 8 bytes of the routingHash
//   second: each present boundary, the start first: hkLen u32 LE, skLen u32 LE, the hash-key bytes,
//           then the sort-key bytes
// A range root has an empty second part, so its ID ends with the dot. The first part holds the
// routingHash, so a reader can get it without the boundary bytes.
const RANGE_FIRST_LEN = 9;
const RANGE_FLAG_START = 0x01;
const RANGE_FLAG_END = 0x02;
const RANGE_BOUNDARY_HEADER_LEN = 8;

function writeU32LE(bytes: Uint8Array, offset: number, value: number): void {
	bytes[offset] = value & 0xff;
	bytes[offset + 1] = (value >>> 8) & 0xff;
	bytes[offset + 2] = (value >>> 16) & 0xff;
	bytes[offset + 3] = (value >>> 24) & 0xff;
}

function readU32LE(bytes: Uint8Array, offset: number): number {
	return (bytes[offset] | (bytes[offset + 1] << 8) | (bytes[offset + 2] << 16) | (bytes[offset + 3] << 24)) >>> 0;
}

function toBase64Url(bytes: Uint8Array): string {
	return bytes.toBase64({ alphabet: "base64url", omitPadding: true });
}

/** The byte count of `length` characters of unpadded base64url. */
function base64UrlByteLength(length: number): number {
	return Math.floor((length * 3) / 4);
}

/**
 * Decodes canonical, unpadded base64url text into all of `target`, which has the byte count of the
 * text. Strict mode rejects non-zero trailing bits. Whitespace and "=" decode to fewer bytes than
 * the text length gives.
 */
function decodeBase64UrlInto(text: string, target: Uint8Array): void {
	const padded = text.padEnd(Math.ceil(text.length / 4) * 4, "=");
	let canonical = false;
	try {
		const { read, written } = target.setFromBase64(padded, { alphabet: "base64url", lastChunkHandling: "strict" });
		canonical = read === padded.length && written === target.length;
	} catch {
		// Text that is not base64url stays not canonical.
	}
	invariant(canonical, "fokos/topology: a range partition ID part is not canonical base64url");
}

/** The byte count of one boundary in the second part of a range ID. */
function boundaryByteLength(boundary: FokosCompositeKey | null): number {
	return boundary === null ? 0 : RANGE_BOUNDARY_HEADER_LEN + boundary.hk.length + boundary.sk.length;
}

/** Writes one boundary at `offset`, and returns the offset after it. */
function writeBoundary(bytes: Uint8Array, offset: number, boundary: FokosCompositeKey | null): number {
	if (boundary === null) {
		return offset;
	}
	writeU32LE(bytes, offset, boundary.hk.length);
	writeU32LE(bytes, offset + 4, boundary.sk.length);
	bytes.set(boundary.hk, offset + RANGE_BOUNDARY_HEADER_LEN);
	bytes.set(boundary.sk, offset + RANGE_BOUNDARY_HEADER_LEN + boundary.hk.length);
	return offset + boundaryByteLength(boundary);
}

/** Reads one boundary at `offset` of the second part, and returns it with the offset after it. */
function readBoundary(second: Uint8Array, offset: number): { boundary: FokosCompositeKey; next: number } {
	invariant(second.length - offset >= RANGE_BOUNDARY_HEADER_LEN, "fokos/topology.decode: a range partition ID boundary is too short");
	const hkLen = readU32LE(second, offset);
	const skLen = readU32LE(second, offset + 4);
	const hkStart = offset + RANGE_BOUNDARY_HEADER_LEN;
	const next = hkStart + hkLen + skLen;
	invariant(next <= second.length, "fokos/topology.decode: a range partition ID boundary length is inconsistent");
	return {
		boundary: {
			hk: KeyCodec.asKeyBytes(second.subarray(hkStart, hkStart + hkLen)),
			sk: KeyCodec.asKeyBytes(second.subarray(hkStart + hkLen, next)),
		},
		next,
	};
}

/** The flat bytes of a range ID. null is an unbounded edge. */
function encodeRangeBytes(
	routingHash: RoutingHash,
	startBoundary: FokosCompositeKey | null,
	endBoundary: FokosCompositeKey | null,
): Uint8Array {
	invariant(routingHash.length === ROUTING_HASH_BYTES, "fokos/topology: a routingHash must be 8 bytes");
	const split = 1 + RANGE_FIRST_LEN;
	const bytes = new Uint8Array(split + boundaryByteLength(startBoundary) + boundaryByteLength(endBoundary));
	bytes[0] = PartitionIdHelper.SCHEMA_RANGE_V1;
	bytes[1] = (startBoundary ? RANGE_FLAG_START : 0) | (endBoundary ? RANGE_FLAG_END : 0);
	bytes.set(routingHash, 2);
	writeBoundary(bytes, writeBoundary(bytes, split, startBoundary), endBoundary);
	return bytes;
}

/** Checks a first part and returns its flags and its routingHash. */
function readRangeFirst(first: Uint8Array): { flags: number; routingHash: RoutingHash } {
	invariant(first.length === RANGE_FIRST_LEN, "fokos/topology: a range partition ID first part has the wrong length");
	const flags = first[0];
	invariant((flags & ~(RANGE_FLAG_START | RANGE_FLAG_END)) === 0, `fokos/topology: invalid range partition ID flags: ${flags}`);
	return { flags, routingHash: asRoutingHash(first.subarray(1)) };
}

/**
 * Splits a range ID at its first dot. The scan stops at the dot, so it does not read the boundary
 * part. A second dot is not base64url, so a decode of the second part rejects it.
 */
function splitRangePartitionId(partitionId: string): [string, string] {
	const dot = partitionId.indexOf(".");
	invariant(dot >= 0, "fokos/topology: a range partition ID has no separator");
	return [partitionId.slice(PartitionIdHelper.SCHEMA_RANGE_V1_STR.length, dot), partitionId.slice(dot + 1)];
}

/** The range ID of the flat range bytes. */
function rangeBytesToPartitionId(bytes: Uint8Array): string {
	const split = 1 + RANGE_FIRST_LEN;
	return PartitionIdHelper.SCHEMA_RANGE_V1_STR + toBase64Url(bytes.subarray(1, split)) + "." + toBase64Url(bytes.subarray(split));
}

/** Refuses a hash partition below the depth limit of its topology, where the digits of a routingHash are not exact. */
function checkHashDepth(depth: number, topology: Pick<FokosTopology, "rootTreesN" | "hashSplitN">): void {
	const limit = maxHashDepth(topology.rootTreesN, topology.hashSplitN);
	if (depth > limit) {
		throw new FokosValidationError(SHARDING_VALIDATION_CODES.partition_context_options_invalid, {
			message: `a hash partition must not be deeper than the depth limit ${limit} of its topology`,
			attributes: { option: "partitionId", value: depth, limit },
		});
	}
}

/**
 * The first byte of a partition ID names its schema. Both wire formats start with the schema byte
 * as two hex digits, so a check of the prefix is sufficient and decodes nothing.
 */
export function isHashPartition(ref: Pick<FokosPartitionRef, "partitionId">): boolean {
	return ref.partitionId.startsWith(PartitionIdHelper.SCHEMA_HASH_V1_STR);
}

export function isRangePartition(ref: Pick<FokosPartitionRef, "partitionId">): boolean {
	return ref.partitionId.startsWith(PartitionIdHelper.SCHEMA_RANGE_V1_STR);
}

/**
 * Exactly two hex digits. `parseInt` alone stops at the first character that is not a hex digit, and
 * accepts a sign or a leading space, so it reads "1g" as 1. The test runs first and rejects them. The
 * expression has no `g` flag, so `test` keeps no state between calls and one shared instance is safe.
 */
const HEX_BYTE = /^[0-9a-fA-F]{2}$/;

export class PartitionIdHelper {
	static readonly SCHEMA_HASH_V1 = 0x00 as const;
	static readonly SCHEMA_HASH_V1_STR = "00" as const;

	static readonly SCHEMA_RANGE_V1 = 0x01 as const;
	static readonly SCHEMA_RANGE_V1_STR = "01" as const;

	/** The flat bytes of an opaque partition ID. `decode` checks the layout of the bytes. */
	static partitionIdToBytes(partitionId: PartitionNodeId): Uint8Array {
		if (!partitionId.startsWith(PartitionIdHelper.SCHEMA_RANGE_V1_STR)) {
			return Uint8Array.fromHex(partitionId);
		}
		const [firstText, secondText] = splitRangePartitionId(partitionId);
		const split = 1 + base64UrlByteLength(firstText.length);
		const bytes = new Uint8Array(split + base64UrlByteLength(secondText.length));
		bytes[0] = PartitionIdHelper.SCHEMA_RANGE_V1;
		decodeBase64UrlInto(firstText, bytes.subarray(1, split));
		decodeBase64UrlInto(secondText, bytes.subarray(split));
		return bytes;
	}

	/**
	 * The routingHash of a range ID, from its first part only. The boundary part is not read, so
	 * the cost does not grow with the boundaries.
	 */
	static rangeRoutingHash(partitionId: PartitionNodeId): RoutingHash {
		const [firstText] = splitRangePartitionId(partitionId);
		const first = new Uint8Array(base64UrlByteLength(firstText.length));
		decodeBase64UrlInto(firstText, first);
		return readRangeFirst(first).routingHash;
	}

	static doName(shardGroup: string, bytes: Uint8Array): string {
		if (bytes[0] === PartitionIdHelper.SCHEMA_HASH_V1) {
			const root = (bytes[1] << 8) | bytes[2];
			const depth = bytes[3];
			const suffix = depth > 0 ? "." + bytes.subarray(4, 4 + depth).join(".") : "";
			return `${shardGroup}${SHARD_GROUP_SEPARATOR}h.${root}${suffix}`;
		}
		invariant(bytes[0] === PartitionIdHelper.SCHEMA_RANGE_V1, `fokos/topology: unsupported partition ID schema version: ${bytes[0]}`);
		const decoded = PartitionIdHelper.decode(bytes);
		invariant(decoded.schema === PartitionIdHelper.SCHEMA_RANGE_V1, "fokos/topology.doName: unreachable");
		return rangePartitionDoName(shardGroup, decoded.routingHash, decoded.startBoundary, decoded.endBoundary);
	}

	// Decode a partition ID bytes to a schema-specific representation.
	static decode(
		bytes: Uint8Array,
	):
		| { schema: 0; rootIdx: number; depth: number }
		| { schema: 1; routingHash: RoutingHash; startBoundary: FokosCompositeKey | null; endBoundary: FokosCompositeKey | null } {
		if (bytes[0] === PartitionIdHelper.SCHEMA_HASH_V1) {
			return { schema: 0, rootIdx: (bytes[1] << 8) | bytes[2], depth: bytes[3] };
		}
		invariant(bytes[0] === PartitionIdHelper.SCHEMA_RANGE_V1, `fokos/topology.decode: unsupported schema version: ${bytes[0]}`);
		// SCHEMA_RANGE_V1: 0x01, then the first part and the second part of the range ID, see
		// `encodeRangeBytes`. The keys of the boundaries are raw canonical KeyBytes.
		invariant(bytes.length >= 1 + RANGE_FIRST_LEN, "fokos/topology.decode: a range partition ID is too short");
		const split = 1 + RANGE_FIRST_LEN;
		const { flags, routingHash } = readRangeFirst(bytes.subarray(1, split));
		const second = bytes.subarray(split);
		let offset = 0;
		let startBoundary: FokosCompositeKey | null = null;
		let endBoundary: FokosCompositeKey | null = null;
		if ((flags & RANGE_FLAG_START) !== 0) {
			({ boundary: startBoundary, next: offset } = readBoundary(second, offset));
		}
		if ((flags & RANGE_FLAG_END) !== 0) {
			({ boundary: endBoundary, next: offset } = readBoundary(second, offset));
		}
		invariant(offset === second.length, "fokos/topology.decode: a range partition ID has bytes after its boundaries");
		return { schema: 1, routingHash, startBoundary, endBoundary };
	}

	// Creates a PartitionIdHelper for a range-structure DO. null start/end = unbounded edge.
	static fromRangePartition(
		shardGroup: string,
		routingHash: RoutingHash,
		startBoundary: FokosCompositeKey | null,
		endBoundary: FokosCompositeKey | null,
	): PartitionIdHelper {
		// Boundaries are already canonical KeyBytes — store them raw (no TextEncoder).
		return new PartitionIdHelper(shardGroup, encodeRangeBytes(routingHash, startBoundary, endBoundary));
	}

	/**
	 * The ID and the DO name of the hash partition at root `rootIdx` and then `childIdxs`, from the root
	 * down. They are equal to the result of `fromHashIdxs(shardGroup, [rootIdx, ...childIdxs]).encode(true)`,
	 * but this function makes only the two strings, because a Worker resolves a partition for each request.
	 * The bytes of the ID are `[0, rootHi, rootLo, depth, ...childIdxs]`.
	 */
	static hashId(shardGroup: string, rootIdx: number, childIdxs: readonly number[] = NO_CHILD_IDXS): { doName: string; opaque: string } {
		invariant(Number.isInteger(rootIdx) && rootIdx >= 0 && rootIdx <= 0xffff, "fokos/topology.hashId: rootIdx must be a u16");
		invariant(childIdxs.length <= 0xff, "fokos/topology.hashId: the depth must be a u8");
		let doName = `${shardGroup}${SHARD_GROUP_SEPARATOR}h.${rootIdx}`;
		let opaque = PartitionIdHelper.SCHEMA_HASH_V1_STR + BYTE_HEX[rootIdx >> 8] + BYTE_HEX[rootIdx & 0xff] + BYTE_HEX[childIdxs.length];
		for (const idx of childIdxs) {
			invariant(Number.isInteger(idx) && idx >= 0 && idx <= 0xff, "fokos/topology.hashId: a child index must be a u8");
			doName += "." + idx;
			opaque += BYTE_HEX[idx];
		}
		return { doName, opaque };
	}

	/**
	 * The ID and the DO name of the hash partition `childIdxs` levels below the hash partition `parent`.
	 * They are equal to the result of `new PartitionIdHelper(shardGroup, parentBytes).appendHashIdx(childIdxs).encode(true)`,
	 * but this function decodes nothing: it adds to the DO name of the parent, and it replaces the depth
	 * byte of the parent ID and adds to it. Thus `parent.doName` must be the DO name of `parent.partitionId`,
	 * as `partitionIdentityFrom` makes sure for the identity of a partition. A descendant below the depth
	 * limit of the topology is refused.
	 */
	static hashDescendantId(
		parent: FokosPartitionRef & { topology: Pick<FokosTopology, "rootTreesN" | "hashSplitN"> },
		childIdxs: readonly number[],
	): { doName: string; opaque: string } {
		const parentId = parent.partitionId;
		const parentDepth = (parentId.length - 8) / 2;
		const depth = parentDepth + childIdxs.length;
		// A hash ID is "00", the root index in 4 hex digits, the depth in 2, and 2 for each child index.
		invariant(
			parentId.startsWith(PartitionIdHelper.SCHEMA_HASH_V1_STR) &&
				Number.isInteger(parentDepth) &&
				parentDepth >= 0 &&
				parentId.charCodeAt(6) === BYTE_HEX[parentDepth].charCodeAt(0) &&
				parentId.charCodeAt(7) === BYTE_HEX[parentDepth].charCodeAt(1),
			"fokos/topology.hashDescendantId: the parent must be a hash partition ID",
		);
		checkHashDepth(depth, parent.topology);
		let doName = parent.doName;
		let opaque = parentId.slice(0, 6) + BYTE_HEX[depth] + parentId.slice(8);
		for (const idx of childIdxs) {
			invariant(Number.isInteger(idx) && idx >= 0 && idx <= 0xff, "fokos/topology.hashDescendantId: a child index must be a u8");
			doName += "." + idx;
			opaque += BYTE_HEX[idx];
		}
		return { doName, opaque };
	}

	static fromHashIdxs(shardGroup: string, hashIdxs: number[]): PartitionIdHelper {
		invariant(hashIdxs.length >= 1, "fokos/topology.fromHashIdxs: hashIdxs must not be empty");
		// hashIdxs[0] is the root index (u16), hashIdxs[1..] are sub-tree child indexes (u8 each).
		const depth = hashIdxs.length - 1;
		const bytes = new Uint8Array(4 + depth); // [version, rootHi, rootLo, depth, child1..child_depth]
		bytes[0] = 0; // schema version
		bytes[1] = (hashIdxs[0] >> 8) & 0xff; // root index high byte
		bytes[2] = hashIdxs[0] & 0xff; // root index low byte
		bytes[3] = depth; // sub-tree depth (u8)
		for (let i = 0; i < depth; i++) {
			bytes[4 + i] = hashIdxs[i + 1];
		}
		return new PartitionIdHelper(shardGroup, bytes);
	}

	// Readers for the encoded partition ID bytes — SCHEMA_HASH_V1 only.
	// Format: [schemaVersion u8, rootIdx u16, depth u8, hashIdx_1 u8, ..., hashIdx_depth u8]
	static rootIdx(bytes: Uint8Array): number {
		invariant(bytes[0] === PartitionIdHelper.SCHEMA_HASH_V1, `fokos/topology: expected hash schema, got: ${bytes[0]}`);
		return (bytes[1] << 8) | bytes[2];
	}
	/**
	 * The depth of a hash partition, from its bytes or from its hex ID. The hex form reads the two
	 * digits of byte 3 and decodes nothing else.
	 */
	static depth(id: Uint8Array | PartitionNodeId): number {
		if (typeof id === "string") {
			invariant(id.startsWith(PartitionIdHelper.SCHEMA_HASH_V1_STR), () => `fokos/topology: expected hash schema, got: ${id.slice(0, 2)}`);
			const digits = id.slice(6, 8);
			invariant(HEX_BYTE.test(digits), () => `fokos/topology: the hash partition ID ${id} has no valid depth`);
			return Number.parseInt(digits, 16);
		}
		invariant(id[0] === PartitionIdHelper.SCHEMA_HASH_V1, `fokos/topology: expected hash schema, got: ${id[0]}`);
		return id[3];
	}
	/** The routingHash interval `[start, end)` of a hash partition. A null `end` has no upper bound. */
	static routingHashInterval(
		partitionId: PartitionNodeId,
		topology: Pick<FokosTopology, "rootTreesN" | "hashSplitN">,
	): { start: RoutingHash; end: RoutingHash | null } {
		const bytes = PartitionIdHelper.partitionIdToBytes(partitionId);
		const depth = PartitionIdHelper.depth(bytes);
		return routingHashInterval(topology.rootTreesN, topology.hashSplitN, PartitionIdHelper.rootIdx(bytes), bytes.subarray(4, 4 + depth));
	}

	// The last child index is this partition's slot among its siblings (only valid when depth >= 1).
	static lastChildIdx(bytes: Uint8Array): number {
		invariant(bytes[0] === PartitionIdHelper.SCHEMA_HASH_V1, `fokos/topology: expected hash schema, got: ${bytes[0]}`);
		return bytes[3 + bytes[3]];
	}

	/**
	 * TODO: Split the hash and the range partition IDs into two classes, so that these helpers need no
	 * schema check.
	 */
	static calculateHashChildPartitionIds(parent: FokosRouteContext<unknown>): {
		doName: string;
		partitionIdOpaque: string;
	}[] {
		return Array.from({ length: parent.topology.hashSplitN }, (_, i) => {
			const { doName, opaque } = PartitionIdHelper.hashDescendantId(parent, [i]);
			return { doName, partitionIdOpaque: opaque };
		});
	}

	#bytes: Uint8Array | undefined;
	#appendedHashIdxs: number[];

	constructor(
		private readonly shardGroup: string,
		// Either the opaque representation as encoded, or the bytes before encoding.
		partitionIdOpaque?: string | Uint8Array,
	) {
		if (partitionIdOpaque) {
			this.#bytes = partitionIdOpaque instanceof Uint8Array ? partitionIdOpaque : PartitionIdHelper.partitionIdToBytes(partitionIdOpaque);
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
			for (let i = 0; i < this.#appendedHashIdxs.length; i++) {
				bytes[bsz + i] = this.#appendedHashIdxs[i];
			}
		} else {
			// Fresh hash instance: appendedHashIdxs[0] is the root index (u16), rest are child indexes (u8 each).
			const depth = this.#appendedHashIdxs.length - 1;
			bytes = new Uint8Array(4 + depth);
			bytes[0] = PartitionIdHelper.SCHEMA_HASH_V1;
			bytes[1] = (this.#appendedHashIdxs[0] >> 8) & 0xff; // root high byte
			bytes[2] = this.#appendedHashIdxs[0] & 0xff; // root low byte
			bytes[3] = depth;
			for (let i = 0; i < depth; i++) {
				bytes[4 + i] = this.#appendedHashIdxs[i + 1];
			}
		}
		let doName: string | undefined;
		if (includeDoName) {
			doName = PartitionIdHelper.doName(this.shardGroup, bytes);
		}
		const opaque = bytes[0] === PartitionIdHelper.SCHEMA_RANGE_V1 ? rangeBytesToPartitionId(bytes) : bytes.toHex();
		return { bytes, opaque, doName };
	}
}
