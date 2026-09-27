import type { PartitionNodeId } from "./types.js";
import type { RangeAncestorInfo } from "./types.js";
import { SHARD_GROUP_SEPARATOR, type FokosPartitionIdentity, type FokosRouteContext } from "./route-context.js";
import { GOLDEN_RATIO as _GOLDEN_RATIO, hashChildIndex as _hashChildIndex, hashRootIndex as _hashRootIndex } from "./hash-primitives.js";
import { KeyCodec, type KeyBytes } from "./key-codec.js";
import { assertExists } from "../shared/tsutils.js";
import invariant from "../shared/invariant.js";

/**
 * Pure partition-identity codec: the opaque partition ID wire formats and DO naming. A route context
 * for another partition is derived here from the caller's own context, so the immutable identity is
 * computed and the mutable parts travel unchanged. Nothing here resolves a Durable Object ID or a
 * stub: `idFromName` recreates the deterministic ID wherever a stub is made.
 */

// Reserved sentinel tokens for the unbounded edges of a range, used ONLY in DO names (never in
// routing comparisons — there boundaries stay `KeyBytes | null` with null = unbounded). The component
// encoding escapes "~", and base64url has no "~", so an encoded boundary never starts with "~m".
export const RANGE_MIN = "~min";
export const RANGE_MAX = "~max";

// The marker of a component in the base64url form.
const BINARY_COMPONENT_MARKER = "~b";

// The "%XX" escape of each byte, with uppercase hex digits.
const PERCENT_HEX: string[] = [];
for (let b = 0; b < 256; b++) {
	PERCENT_HEX.push("%" + b.toString(16).padStart(2, "0").toUpperCase());
}

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

// Range DO name. null start/end render to the ~min/~max sentinels so every DO has the identical
// three-component shape (the range root is <shardGroup>~r.<hk>.~min.~max, addressable from hashKey
// alone). The "r." and "h." markers after the separator keep range and hash DO names disjoint.
function rangePartitionDoName(shardGroup: string, hashKey: KeyBytes, startBoundary: KeyBytes | null, endBoundary: KeyBytes | null): string {
	const hk = encodeRangeComponent(hashKey);
	const start = startBoundary === null ? RANGE_MIN : encodeRangeComponent(startBoundary);
	const end = endBoundary === null ? RANGE_MAX : encodeRangeComponent(endBoundary);
	return `${shardGroup}${SHARD_GROUP_SEPARATOR}r.${hk}.${start}.${end}`;
}

/** The route context of a range partition (root or child) of the same shard group as `base`. */
export function resolveRangePartitionContext<P>(
	base: FokosRouteContext<P>,
	hashKey: KeyBytes,
	startBoundary: KeyBytes | null,
	endBoundary: KeyBytes | null,
): FokosRouteContext<P> {
	// The ID and the DO name come from the same keys, so no ID decode is necessary.
	const partitionId = rangeBytesToPartitionId(encodeRangeBytes(hashKey, startBoundary, endBoundary));
	const doName = rangePartitionDoName(base.topology.shardGroup, hashKey, startBoundary, endBoundary);
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

/** The route context of a descendant hash partition: the owner's encoded ID plus the appended child indexes. */
export function resolveDescendantHashPartitionContext<P>(
	base: FokosRouteContext<P>,
	partitionIdBytes: Uint8Array,
	hashIdxs: number[],
): FokosRouteContext<P> {
	const { doName, opaque } = new PartitionIdHelper(base.topology.shardGroup, partitionIdBytes).appendHashIdx(hashIdxs).encode(true);
	assertExists(doName);
	return { ...base, doName, partitionId: opaque };
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
		return {
			schema: 1,
			ref,
			kind: "hash",
			hash: { rootIndex: decoded.rootIdx, path: Array.from(bytes.subarray(4, 4 + decoded.depth)) },
			topology: ctx.topology,
		};
	}
	invariant(range, "fokos/topology.partitionIdentityFrom: a range partition needs its depth and ancestors");
	return {
		schema: 1,
		ref,
		kind: "range",
		range: { hashKey: decoded.hashKey, start: decoded.startBoundary, end: decoded.endBoundary, ...range },
		topology: ctx.topology,
	};
}

/** The depth of a partition in its tree: the hash child path length, or the range depth its `fokosInit` gave it. */
export function identityDepth(identity: FokosPartitionIdentity): number {
	return identity.hash ? identity.hash.path.length : identity.range!.depth;
}

// The range ID is "01" + base64url(first) + "." + base64url(second), both parts unpadded. The dot is
// a separator, not base64url data. The flat bytes are 0x01 + first + second:
//   first:  flags u8 (bit0 = has start, bit1 = has end), hkLen u32 LE, startLen u32 LE (0 when no
//           start), then hkLen hash-key bytes
//   second: startLen start bytes when the start flag is set, then the end bytes when the end flag is set
// A range root has an empty second part, so its ID ends with the dot. The first part holds all of the
// hash key, so a reader can get the hash key without the boundary bytes.
const RANGE_HEADER_LEN = 9;
const RANGE_FLAG_START = 0x01;
const RANGE_FLAG_END = 0x02;

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

const NO_BYTES = Object.freeze(new Uint8Array(0));

/** The flat bytes of a range ID. null is an unbounded edge. */
function encodeRangeBytes(hashKey: KeyBytes, startBoundary: KeyBytes | null, endBoundary: KeyBytes | null): Uint8Array {
	const start = startBoundary ?? NO_BYTES;
	const end = endBoundary ?? NO_BYTES;
	const split = 1 + RANGE_HEADER_LEN + hashKey.length;
	const bytes = new Uint8Array(split + start.length + end.length);
	bytes[0] = PartitionIdHelper.SCHEMA_RANGE_V1;
	bytes[1] = (startBoundary ? RANGE_FLAG_START : 0) | (endBoundary ? RANGE_FLAG_END : 0);
	writeU32LE(bytes, 2, hashKey.length);
	writeU32LE(bytes, 6, start.length);
	bytes.set(hashKey, 1 + RANGE_HEADER_LEN);
	bytes.set(start, split);
	bytes.set(end, split + start.length);
	return bytes;
}

/** Checks the header of a first part and returns its hash key and the start length. */
function readRangeFirst(first: Uint8Array): { flags: number; hashKey: KeyBytes; startLen: number } {
	invariant(first.length >= RANGE_HEADER_LEN, "fokos/topology: a range partition ID header is too short");
	const flags = first[0];
	invariant((flags & ~(RANGE_FLAG_START | RANGE_FLAG_END)) === 0, `fokos/topology: invalid range partition ID flags: ${flags}`);
	const hkLen = readU32LE(first, 1);
	const startLen = readU32LE(first, 5);
	invariant(first.length === RANGE_HEADER_LEN + hkLen, "fokos/topology: a range partition ID hash-key length is inconsistent");
	invariant((flags & RANGE_FLAG_START) !== 0 || startLen === 0, "fokos/topology: a range partition ID without a start has a start length");
	return { flags, hashKey: KeyCodec.asKeyBytes(first.subarray(RANGE_HEADER_LEN)), startLen };
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
	const hkLen = readU32LE(bytes, 2);
	const split = 1 + RANGE_HEADER_LEN + hkLen;
	return PartitionIdHelper.SCHEMA_RANGE_V1_STR + toBase64Url(bytes.subarray(1, split)) + "." + toBase64Url(bytes.subarray(split));
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
	 * The hash key of a range ID, from its first part only. The boundary part is not read, so the
	 * cost does not grow with the boundaries.
	 */
	static rangeHashKey(partitionId: PartitionNodeId): KeyBytes {
		const [firstText] = splitRangePartitionId(partitionId);
		const first = new Uint8Array(base64UrlByteLength(firstText.length));
		decodeBase64UrlInto(firstText, first);
		return readRangeFirst(first).hashKey;
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
		return rangePartitionDoName(shardGroup, decoded.hashKey, decoded.startBoundary, decoded.endBoundary);
	}

	// Decode a partition ID bytes to a schema-specific representation.
	static decode(
		bytes: Uint8Array,
	):
		| { schema: 0; rootIdx: number; depth: number }
		| { schema: 1; hashKey: KeyBytes; startBoundary: KeyBytes | null; endBoundary: KeyBytes | null } {
		if (bytes[0] === PartitionIdHelper.SCHEMA_HASH_V1) {
			return { schema: 0, rootIdx: (bytes[1] << 8) | bytes[2], depth: bytes[3] };
		}
		invariant(bytes[0] === PartitionIdHelper.SCHEMA_RANGE_V1, `fokos/topology.decode: unsupported schema version: ${bytes[0]}`);
		// SCHEMA_RANGE_V1: 0x01, then the first part and the second part of the range ID, see
		// `encodeRangeBytes`. The keys and the boundaries are raw canonical KeyBytes.
		const split = 1 + RANGE_HEADER_LEN + readU32LE(bytes, 2);
		const { flags, hashKey, startLen } = readRangeFirst(bytes.subarray(1, split));
		const second = bytes.subarray(split);
		invariant(second.length >= startLen, "fokos/topology.decode: a range partition ID start length is inconsistent");
		const hasEnd = (flags & RANGE_FLAG_END) !== 0;
		invariant(hasEnd || second.length === startLen, "fokos/topology.decode: a range partition ID without an end has end bytes");
		const startBoundary = (flags & RANGE_FLAG_START) !== 0 ? KeyCodec.asKeyBytes(second.subarray(0, startLen)) : null;
		const endBoundary = hasEnd ? KeyCodec.asKeyBytes(second.subarray(startLen)) : null;
		return { schema: 1, hashKey, startBoundary, endBoundary };
	}

	// Creates a PartitionIdHelper for a range-structure DO. null start/end = unbounded edge.
	static fromRangePartition(
		shardGroup: string,
		hashKey: KeyBytes,
		startBoundary: KeyBytes | null,
		endBoundary: KeyBytes | null,
	): PartitionIdHelper {
		// Boundaries are already canonical KeyBytes — store them raw (no TextEncoder).
		return new PartitionIdHelper(shardGroup, encodeRangeBytes(hashKey, startBoundary, endBoundary));
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
	 * TODO: Split the hash and the range partition IDs into two classes, so that these helpers need no
	 * schema check.
	 */
	static calculateHashChildPartitionIds(parent: FokosRouteContext<unknown>): {
		doName: string;
		partitionIdOpaque: string;
	}[] {
		const parentBytes = Uint8Array.fromHex(parent.partitionId);
		invariant(parentBytes[0] === PartitionIdHelper.SCHEMA_HASH_V1, `fokos/topology: expected hash schema, got: ${parentBytes[0]}`);
		const { shardGroup, hashSplitN } = parent.topology;
		const result = Array.from({ length: hashSplitN }, (_, i) => {
			const { doName, opaque } = new PartitionIdHelper(shardGroup, parentBytes).appendHashIdx(i).encode(true);
			return {
				doName: doName!,
				partitionIdOpaque: opaque,
			};
		});
		invariant(
			result.length === hashSplitN,
			`fokos/topology.calculateChildPartitionIds: expected ${hashSplitN} children, got ${result.length}`,
		);
		return result;
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
