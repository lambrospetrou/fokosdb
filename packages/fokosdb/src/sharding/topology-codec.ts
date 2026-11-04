/**
 * The encoded topology of one shard group: the report event that a partition sends to the admin
 * Durable Object, the framing of the two bodies that the admin Durable Object serves, and the readers
 * that a caller uses on the fetched bytes.
 *
 * - The hash body holds the hash split bitmap and the promoted routingHashes, as a sorted list
 *   below the keeper's list limit and as a Bloom filter above it.
 * - The range body holds the known range intervals of one promoted routingHash.
 *
 * A reader makes typed-array views over the fetched bytes and does not decode them into objects.
 * `docs/agent-plans/2026-10-23-encoded-topology-auto-admin.md` gives the formats. The writer side is
 * in `topology-encode.ts`, so a caller does not ship it.
 */
import { layerHas } from "../shared/bloom-filter.js";
import { ROUTING_HASH_BYTES, RoutingHashDigitCursor, type RoutingHash } from "./hash-primitives.js";
import { KeyCodec, type FokosCompositeKey, type KeyBytes } from "./key-codec.js";
import type { FokosSlice } from "./repartition-slice.js";
import type { FokosPartitionRef } from "./route-context.js";
import type { RepartitionKind } from "./sharding-store.js";
import type { SkInterval } from "./sk-interval.js";

// ─── the report event ────────────────────────────────────────────────────────

/** The event type of a completed repartition in the admin outbox. */
export const TOPOLOGY_EVENT_TYPE = "topology.repartition_completed";

export type FokosTopologyEvent = {
	schema: 1;
	/** The repartition id. The keeper is idempotent by (shardGroup, id). */
	id: string;
	kind: RepartitionKind;
	shardGroup: string;
	/**
	 * The immutable topology values of the shard group. The keeper cannot get `rootTreesN` from a
	 * partition ID, and a target count gives `hashSplitN` only for a hash split.
	 */
	rootTreesN: number;
	hashSplitN: number;
	/** `source.doName` is the name of the source object. */
	source: FokosPartitionRef;
	/** `ctx.id.toString()` of the source. */
	actorId: string;
	/** `actorId` is `idFromName(ref.doName).toString()`, which the source computes with no RPC. */
	targets: Array<{ ref: FokosPartitionRef; slice: FokosSlice; actorId: string }>;
	/** `Date.now()` of the source when it wrote `completed`. */
	completedAt: number;
};

// ─── the body framing ────────────────────────────────────────────────────────

export const HASH_BODY_MAGIC = "FKT1";
export const RANGE_BODY_MAGIC = "FKR1";
export const BODY_SCHEMA = 1;

const utf8Decoder = new TextDecoder();

type BodyHeader = {
	schema: number;
	epoch: number;
	version: string;
	sections: Record<string, Record<string, unknown> | undefined>;
	[field: string]: unknown;
};

/** The header of a body. Refuses another magic, another schema, or a header outside the body. */
function readHeader(body: Uint8Array, magic: string): BodyHeader {
	if (body.length < 8 || utf8Decoder.decode(body.subarray(0, 4)) !== magic) {
		throw new Error(`fokos/topology: the body does not start with ${magic}`);
	}
	const headerLen = new DataView(body.buffer, body.byteOffset, 8).getUint32(4, true);
	if (8 + headerLen > body.length) {
		throw new Error("fokos/topology: the body header is outside the body");
	}
	const header = JSON.parse(utf8Decoder.decode(body.subarray(8, 8 + headerLen))) as BodyHeader;
	if (header.schema !== BODY_SCHEMA) {
		throw new Error(`fokos/topology: unsupported body schema ${String(header.schema)}`);
	}
	if (!Number.isSafeInteger(header.epoch) || typeof header.version !== "string" || typeof header.sections !== "object") {
		throw new Error("fokos/topology: the body header needs an epoch, a version, and sections");
	}
	return header;
}

/** The topology version of a body, from its header only. */
export function readBodyVersion(body: Uint8Array, magic: string): TopologyVersion {
	const header = readHeader(body, magic);
	return { epoch: header.epoch, version: BigInt(header.version) };
}

/** The offset of a section, after a check that it is a multiple of 8 and that `byteLength` bytes fit in the body. */
function sectionOffset(body: Uint8Array, section: Record<string, unknown> | undefined, byteLength: number, name: string): number {
	const offset = section?.offset;
	if (typeof offset !== "number" || !Number.isInteger(offset) || offset % 8 !== 0 || offset < 8 || offset + byteLength > body.length) {
		throw new Error(`fokos/topology: the ${name} section is not valid`);
	}
	return offset;
}

function intField(section: Record<string, unknown> | undefined, field: string): number {
	const value = section?.[field];
	if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
		throw new Error(`fokos/topology: the section field ${field} is not valid`);
	}
	return value;
}

/** The views need the body at an offset that is a multiple of 8 in its buffer. A body that is not gets a copy. */
function aligned(body: Uint8Array): Uint8Array {
	return body.byteOffset % 8 === 0 ? body : body.slice();
}

/** The topology version `(epoch, version)`, compared in that order. */
export type TopologyVersion = { epoch: number; version: bigint };

export function compareTopologyVersions(a: TopologyVersion, b: TopologyVersion): number {
	if (a.epoch !== b.epoch) {
		return a.epoch < b.epoch ? -1 : 1;
	}
	return a.version === b.version ? 0 : a.version < b.version ? -1 : 1;
}

// ─── the hash body ───────────────────────────────────────────────────────────

// The hash body implements a LOUDS data structure encoding for the hash splits as explained in https://www.lambrospetrou.com/articles/fokosdb/#fokosdb-encoded-topology.

/** The views over one hash body. `bitCount` 0 is a topology with no split: the keeper knows nothing yet. */
export type HashTopologyView = TopologyVersion & {
	rootTreesN: number;
	hashSplitN: number;
	bitCount: number;
	words: Uint32Array;
	/** The count of 1 bits before each block of 512 bits, and the total at the end. A view over the body. */
	rank: Uint32Array;
	/**
	 * The sorted routingHashes of the `promoted` section, packed 8 bytes each, or null when the
	 * body has a `bloom` section for the promoted set (the keeper uses a list up to its
	 * `promotedListMax` routingHashes).
	 */
	promoted: { bytes: Uint8Array; count: number } | null;
	bloom: { bits: Uint8Array; k: number; m: number; layerIndex: 0 } | null;
	/** The bytes that the view holds: the body. */
	byteLength: number;
};

/** The bits of one block of the rank index. */
export const TOPOLOGY_RANK_BLOCK_BITS = 512;
export const TOPOLOGY_RANK_BLOCK_WORDS = TOPOLOGY_RANK_BLOCK_BITS / 32;

export function popcount32(v: number): number {
	v = v - ((v >>> 1) & 0x55555555);
	v = (v & 0x33333333) + ((v >>> 2) & 0x33333333);
	return (Math.imul((v + (v >>> 4)) & 0x0f0f0f0f, 0x01010101) >>> 24) & 0xff;
}

export function parseHashBody(input: Uint8Array): HashTopologyView {
	const body = aligned(input);
	const header = readHeader(body, HASH_BODY_MAGIC);
	const bitmap = header.sections.bitmap;
	const bitCount = intField(bitmap, "bitCount");
	const wordCount = Math.ceil(bitCount / 32);
	const bitmapOffset = sectionOffset(body, bitmap, wordCount * 4, "bitmap");
	const words = new Uint32Array(body.buffer, body.byteOffset + bitmapOffset, wordCount);
	// The keeper builds the rank index, so a reader only makes a view over it.
	const rankCount = Math.ceil(wordCount / TOPOLOGY_RANK_BLOCK_WORDS) + 1;
	if (intField(header.sections.rank, "count") !== rankCount) {
		throw new Error("fokos/topology: the rank section is not valid");
	}
	const rankOffset = sectionOffset(body, header.sections.rank, rankCount * 4, "rank");
	const rank = new Uint32Array(body.buffer, body.byteOffset + rankOffset, rankCount);
	const rootTreesN = header.rootTreesN as unknown;
	const hashSplitN = header.hashSplitN as unknown;
	if (bitCount > 0 && (!Number.isInteger(rootTreesN) || !Number.isInteger(hashSplitN) || bitCount < (rootTreesN as number))) {
		throw new Error("fokos/topology: the hash body needs rootTreesN and hashSplitN");
	}
	// The last count is the number of splits, so it gives the bit count again.
	if (bitCount > 0 && bitCount !== (rootTreesN as number) + (hashSplitN as number) * rank[rankCount - 1]) {
		throw new Error("fokos/topology: the rank section does not agree with the bitmap");
	}
	let promoted: HashTopologyView["promoted"] = null;
	const promotedSection = header.sections.promoted;
	if (promotedSection !== undefined) {
		const count = intField(promotedSection, "count");
		const width = intField(promotedSection, "width");
		if (width !== ROUTING_HASH_BYTES) {
			throw new Error("fokos/topology: the promoted section is not valid");
		}
		const offset = sectionOffset(body, promotedSection, count * width, "promoted");
		const bytes = body.subarray(offset, offset + count * width);
		// The binary search of isPromotedRoutingHash is wrong on an unordered section, so a
		// reader must refuse one.
		if (!promotedRoutingHashesInOrder(bytes)) {
			throw new Error("fokos/topology: the promoted section is not in order");
		}
		promoted = { bytes, count };
	}
	let bloom: HashTopologyView["bloom"] = null;
	const bloomSection = header.sections.bloom;
	if (bloomSection !== undefined) {
		// A reader uses only the list when a body has both sections, so it must refuse such a body.
		if (promoted !== null) {
			throw new Error("fokos/topology: the hash body has a promoted section and a bloom section");
		}
		const length = intField(bloomSection, "length");
		const m = intField(bloomSection, "m");
		const k = intField(bloomSection, "k");
		if (m === 0 || m > length * 8) {
			throw new Error("fokos/topology: the bloom section is not valid");
		}
		const offset = sectionOffset(body, bloomSection, length, "bloom");
		bloom = { bits: body.subarray(offset, offset + length), k, m, layerIndex: 0 };
	}
	return {
		epoch: header.epoch,
		version: BigInt(header.version),
		rootTreesN: rootTreesN as number,
		hashSplitN: hashSplitN as number,
		bitCount,
		words,
		rank,
		promoted,
		bloom,
		byteLength: body.byteLength,
	};
}

/** True when the hash partition at `pos` has split. */
export function hashBit(view: HashTopologyView, pos: number): boolean {
	return pos < view.bitCount && ((view.words[pos >>> 5] >>> (pos & 31)) & 1) === 1;
}

/** The number of 1 bits before position `pos`. */
export function hashRank1(view: HashTopologyView, pos: number): number {
	const word = pos >>> 5;
	let count = view.rank[pos >>> 9];
	for (let w = (pos >>> 9) * TOPOLOGY_RANK_BLOCK_WORDS; w < word; w++) {
		count += popcount32(view.words[w]);
	}
	const bitInWord = pos & 31;
	return bitInWord === 0 ? count : count + popcount32(view.words[word] & (0xffffffff >>> (32 - bitInWord)));
}

/**
 * The deepest known hash partition of a routingHash: its bitmap position, its root index, and
 * its child path from the root. The walk stops before a position in `cut`.
 */
export function findLeaf(
	view: HashTopologyView,
	rootTreesN: number,
	hashSplitN: number,
	routingHash: RoutingHash,
	cut: ReadonlySet<number>,
): { pos: number; root: number; path: number[] } {
	const digits = new RoutingHashDigitCursor(routingHash, rootTreesN);
	const root = digits.root;
	const path: number[] = [];
	let pos = root;
	while (hashBit(view, pos)) {
		const child = digits.next(hashSplitN);
		const next = rootTreesN + hashSplitN * hashRank1(view, pos) + child;
		if (cut.has(next)) {
			break;
		}
		pos = next;
		path.push(child);
	}
	return { pos, root, path };
}

/**
 * True when the packed routingHashes of a `promoted` section are in strict ascending byte order,
 * with no repeated value. It compares the bytes in place: it makes no views and no copies.
 */
export function promotedRoutingHashesInOrder(bytes: Uint8Array): boolean {
	const width = ROUTING_HASH_BYTES;
	for (let cur = width; cur < bytes.length; cur += width) {
		// The same compare as the search of isPromotedRoutingHash, so the two agree on the order.
		if (KeyCodec.compareSlices(bytes, cur - width, cur, bytes, cur, cur + width) >= 0) {
			return false;
		}
	}
	return true;
}

/**
 * Whether the promoted set of the body holds `routingHash`:
 * - "exact": the `promoted` section holds it. This answer is always correct.
 * - "maybe": the `bloom` section holds it. This answer can be a false positive.
 * - false: the body does not hold it. This answer is always correct.
 *
 * The search compares each candidate in place: nothing is copied out of the section.
 */
export function isPromotedRoutingHash(view: HashTopologyView, routingHash: RoutingHash): "exact" | "maybe" | false {
	const promoted = view.promoted;
	if (promoted === null) {
		return view.bloom !== null && layerHas(view.bloom, routingHash) ? "maybe" : false;
	}
	const bytes = promoted.bytes;
	const key = KeyCodec.asKeyBytes(routingHash);
	let a = 0;
	let b = promoted.count;
	while (a < b) {
		// `a + (b - a) / 2` stays below `b`, so it does not overflow the 32 bits of `>>>`.
		const mid = a + ((b - a) >>> 1);
		if (KeyCodec.compareSlice(bytes, mid * ROUTING_HASH_BYTES, (mid + 1) * ROUTING_HASH_BYTES, key) < 0) {
			a = mid + 1;
		} else {
			b = mid;
		}
	}
	const found = a < promoted.count && KeyCodec.compareSlice(bytes, a * ROUTING_HASH_BYTES, (a + 1) * ROUTING_HASH_BYTES, key) === 0;
	return found ? "exact" : false;
}

// ─── the range body ──────────────────────────────────────────────────────────

const NO_KEY = KeyCodec.encodeOptional(undefined);

/** `hashKey || 0x00`: no hash key is between `hashKey` and it. */
function nextHashKey(hashKey: KeyBytes): KeyBytes {
	const next = new Uint8Array(hashKey.length + 1);
	next.set(hashKey);
	return KeyCodec.asKeyBytes(next);
}

export type RangeTopologyView = TopologyVersion & {
	/** The number of inner boundaries. */
	n: number;
	/**
	 * `2n + 1` offsets into `bytes`. Boundary `j` is the hash key `offsets[2j] .. offsets[2j + 1]` and
	 * the sort key `offsets[2j + 1] .. offsets[2j + 2]`.
	 */
	offsets: Uint32Array;
	bytes: Uint8Array;
	levels: Uint16Array | Uint32Array;
	/** `n + 1` values, or none when the routingHash is not promoted. */
	depths: Uint16Array | Uint32Array;
	truncatedAtRank: number | null;
	byteLength: number;
};

export function parseRangeBody(input: Uint8Array): RangeTopologyView {
	const body = aligned(input);
	const header = readHeader(body, RANGE_BODY_MAGIC);
	const rankBytes = header.rankBytes;
	if (rankBytes !== 2 && rankBytes !== 4) {
		throw new Error("fokos/topology: rankBytes must be 2 or 4");
	}
	const s = header.sections;
	const offsetCount = intField(s.boundaryOffsets, "count");
	if (offsetCount % 2 !== 1) {
		throw new Error("fokos/topology: the range body has an even count of boundary offsets");
	}
	const n = (offsetCount - 1) / 2;
	const byteCount = intField(s.boundaryBytes, "length");
	const levelCount = intField(s.boundaryLevel, "count");
	const depthCount = intField(s.segmentDepth, "count");
	if (n < 0 || levelCount !== n || (depthCount !== n + 1 && !(n === 0 && depthCount === 0))) {
		throw new Error("fokos/topology: the range body counts do not agree");
	}
	const at = (name: string, length: number) => body.byteOffset + sectionOffset(body, s[name], length, name);
	const offsets = new Uint32Array(body.buffer, at("boundaryOffsets", offsetCount * 4), offsetCount);
	if (offsets[0] !== 0 || offsets[2 * n] !== byteCount) {
		throw new Error("fokos/topology: the boundary offsets are not valid");
	}
	for (let j = 0; j < 2 * n; j++) {
		if (offsets[j + 1] < offsets[j]) {
			throw new Error("fokos/topology: the boundary offsets are not valid");
		}
	}
	const buffer = body.buffer as ArrayBuffer;
	const ranks = (name: string, count: number) =>
		rankBytes === 2 ? new Uint16Array(buffer, at(name, count * 2), count) : new Uint32Array(buffer, at(name, count * 4), count);
	const truncatedAtRank = header.truncatedAtRank;
	return {
		epoch: header.epoch,
		version: BigInt(header.version),
		n,
		offsets,
		bytes: new Uint8Array(body.buffer, at("boundaryBytes", byteCount), byteCount),
		levels: ranks("boundaryLevel", levelCount),
		depths: ranks("segmentDepth", depthCount),
		truncatedAtRank: typeof truncatedAtRank === "number" ? truncatedAtRank : null,
		byteLength: body.byteLength,
	};
}

/** The level of a position: -1 for `~min` (0) and `~max` (n + 1). */
function levelAt(view: RangeTopologyView, p: number): number {
	return p === 0 || p === view.n + 1 ? -1 : view.levels[p - 1];
}

/** The number of boundaries that are less than `key`, or less than or equal to it with `orEqual`. */
function countBoundaries(view: RangeTopologyView, key: FokosCompositeKey, orEqual: boolean): number {
	let lo = 0;
	let hi = view.n;
	while (lo < hi) {
		const mid = (lo + hi) >>> 1;
		const o = view.offsets;
		const cmp =
			KeyCodec.compareSlice(view.bytes, o[2 * mid], o[2 * mid + 1], key.hk) ||
			KeyCodec.compareSlice(view.bytes, o[2 * mid + 1], o[2 * mid + 2], key.sk);
		if (cmp < 0 || (orEqual && cmp === 0)) {
			lo = mid + 1;
		} else {
			hi = mid;
		}
	}
	return lo;
}

/** The boundary at position `p` (1 .. n), or null for `~min` and `~max`. */
export function rangeBoundaryAt(view: RangeTopologyView, p: number): FokosCompositeKey | null {
	if (p === 0 || p === view.n + 1) {
		return null;
	}
	const o = view.offsets;
	const j = p - 1;
	return {
		hk: KeyCodec.asKeyBytes(view.bytes.subarray(o[2 * j], o[2 * j + 1])),
		sk: KeyCodec.asKeyBytes(view.bytes.subarray(o[2 * j + 1], o[2 * j + 2])),
	};
}

/** One known interval: its start and end positions and its rank. Rank 0 is the range root. */
export type RangeTarget = { start: number; end: number; rank: number };

/** The `cut` key of an interval. */
export function rangeCutKey(view: RangeTopologyView, target: RangeTarget): number {
	return target.start * (view.n + 2) + target.end;
}

/**
 * The deepest known interval that holds the segments `i .. j`, with `threshold` as the largest rank.
 * An interval in `cut` takes the next lower threshold. Null when the cut removes the range root too.
 */
function intervalRule(view: RangeTopologyView, i: number, j: number, threshold: number, cut: ReadonlySet<number>): RangeTarget | null {
	for (let t = threshold; t >= 0; t--) {
		let start = i;
		while (levelAt(view, start) >= t) {
			start--;
		}
		let end = j + 1;
		while (levelAt(view, end) >= t) {
			end++;
		}
		const target = { start, end, rank: t };
		if (!cut.has(rangeCutKey(view, target))) {
			return target;
		}
	}
	return null;
}

/** The deepest known interval of `key`. Null for a body with no segment, or when the cut removes every candidate. */
export function rangePointLookup(view: RangeTopologyView, key: FokosCompositeKey, cut: ReadonlySet<number>): RangeTarget | null {
	if (view.depths.length === 0) {
		return null;
	}
	const i = countBoundaries(view, key, true);
	return intervalRule(view, i, i, view.depths[i], cut);
}

/**
 * The deepest known interval that holds the whole sort-key interval of a range request of `hashKey`.
 * An absent lower bound is the first key of `hashKey`, `(hashKey, x'')`. An absent upper bound is
 * below `(hashKey || 0x00, x'')`, the first key above every key of `hashKey`.
 */
export function rangeIntervalLookup(
	view: RangeTopologyView,
	hashKey: KeyBytes,
	interval: SkInterval,
	cut: ReadonlySet<number>,
): RangeTarget | null {
	if (view.depths.length === 0) {
		return null;
	}
	const i = countBoundaries(view, { hk: hashKey, sk: interval.lower?.value ?? NO_KEY }, true);
	const j = interval.upper
		? countBoundaries(view, { hk: hashKey, sk: interval.upper.value }, interval.upper.inclusive)
		: countBoundaries(view, { hk: nextHashKey(hashKey), sk: NO_KEY }, false);
	if (j < i) {
		return intervalRule(view, i, i, view.depths[i], cut);
	}
	let threshold = view.depths[i];
	for (let p = i + 1; p <= j; p++) {
		threshold = Math.min(threshold, view.depths[p], levelAt(view, p));
	}
	return intervalRule(view, i, j, threshold, cut);
}
