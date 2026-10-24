/**
 * The writer side of the encoded topology: the JSON form of the report event, and the encoders of the
 * hash body and the range body. Only a partition and the keeper use it. `topology-codec.ts` has the
 * formats and the readers.
 */
import { BloomFilter, bloomInitialLayerBytes } from "../shared/bloom-filter.js";
import { SHARDING_VALIDATION_CODES } from "./errors.js";
import { KeyCodec, type KeyBytes } from "./key-codec.js";
import type { FokosSlice } from "./repartition-slice.js";
import type { FokosPartitionRef } from "./route-context.js";
import type { RepartitionKind } from "./sharding-store.js";
import {
	BODY_SCHEMA,
	HASH_BODY_MAGIC,
	hashBit,
	hashRank1,
	popcount32,
	RANGE_BODY_MAGIC,
	TOPOLOGY_RANK_BLOCK_WORDS,
	type FokosTopologyEvent,
	type HashTopologyView,
} from "./topology-codec.js";
import invariant from "../shared/invariant.js";
import { FokosValidationError } from "../shared/errors.js";

// ─── the report event ────────────────────────────────────────────────────────

function toBase64Url(bytes: Uint8Array): string {
	return bytes.toBase64({ alphabet: "base64url", omitPadding: true });
}

function fromBase64Url(text: string): Uint8Array {
	return Uint8Array.fromBase64(text, { alphabet: "base64url" });
}

/** The JSON text of an event. Each key byte array is a base64url string. */
export function topologyEventToJson(event: FokosTopologyEvent): string {
	return JSON.stringify(event, (_key, value: unknown) => (value instanceof Uint8Array ? toBase64Url(value) : value));
}

function invalidEvent(message: string, attributes: Record<string, unknown> = {}): FokosValidationError {
	return new FokosValidationError(SHARDING_VALIDATION_CODES.topology_event_invalid, { message, attributes });
}

function isRef(value: unknown): value is FokosPartitionRef {
	const ref = value as FokosPartitionRef | null;
	return typeof ref === "object" && ref !== null && typeof ref.partitionId === "string" && typeof ref.doName === "string";
}

const KINDS: ReadonlySet<string> = new Set<RepartitionKind>(["hash_split", "range_split", "key_promotion"]);

function keyFromWire(value: unknown, field: string): KeyBytes {
	if (typeof value !== "string") {
		throw invalidEvent(`${field} must be a base64url string`);
	}
	try {
		return KeyCodec.asKeyBytes(fromBase64Url(value));
	} catch {
		throw invalidEvent(`${field} is not base64url`);
	}
}

function sliceFromWire(value: unknown): FokosSlice {
	const s = value as Record<string, unknown> | null;
	if (typeof s !== "object" || s === null) {
		throw invalidEvent("a target slice must be an object");
	}
	switch (s.kind) {
		case "hash_child":
			if (!Number.isInteger(s.childIndex) || !Number.isInteger(s.depth)) {
				throw invalidEvent("a hash_child slice needs an integer childIndex and depth");
			}
			return { kind: "hash_child", childIndex: s.childIndex as number, depth: s.depth as number };
		case "range":
			return {
				kind: "range",
				hashKey: keyFromWire(s.hashKey, "slice.hashKey"),
				start: s.start === null ? null : keyFromWire(s.start, "slice.start"),
				end: s.end === null ? null : keyFromWire(s.end, "slice.end"),
			};
		case "promoted_key":
			return { kind: "promoted_key", hashKey: keyFromWire(s.hashKey, "slice.hashKey") };
		default:
			throw invalidEvent("unknown slice kind", { kind: s.kind });
	}
}

/** Reads and checks an event in its JSON form, as the admin outbox sends it. */
export function topologyEventFromWire(value: unknown): FokosTopologyEvent {
	const e = value as Record<string, unknown> | null;
	if (typeof e !== "object" || e === null || e.schema !== 1) {
		throw invalidEvent("a topology event must be an object with schema 1");
	}
	if (typeof e.id !== "string" || typeof e.kind !== "string" || !KINDS.has(e.kind) || typeof e.shardGroup !== "string") {
		throw invalidEvent("a topology event needs an id, a known kind, and a shard group", { id: e.id, kind: e.kind });
	}
	if (!Number.isInteger(e.rootTreesN) || !Number.isInteger(e.hashSplitN) || typeof e.completedAt !== "number") {
		throw invalidEvent("a topology event needs rootTreesN, hashSplitN, and completedAt", { id: e.id });
	}
	if (!isRef(e.source) || typeof e.actorId !== "string" || !Array.isArray(e.targets) || e.targets.length === 0) {
		throw invalidEvent("a topology event needs a source, an actorId, and targets", { id: e.id });
	}
	const targets = e.targets.map((t: Record<string, unknown> | null) => {
		if (typeof t !== "object" || t === null || !isRef(t.ref) || typeof t.actorId !== "string") {
			throw invalidEvent("a target needs a ref and an actorId", { id: e.id });
		}
		return { ref: { partitionId: t.ref.partitionId, doName: t.ref.doName }, slice: sliceFromWire(t.slice), actorId: t.actorId };
	});
	return {
		schema: 1,
		id: e.id,
		kind: e.kind as RepartitionKind,
		shardGroup: e.shardGroup,
		rootTreesN: e.rootTreesN as number,
		hashSplitN: e.hashSplitN as number,
		source: { partitionId: e.source.partitionId, doName: e.source.doName },
		actorId: e.actorId,
		targets,
		completedAt: e.completedAt,
	};
}

// ─── the body framing ────────────────────────────────────────────────────────

const utf8Encoder = new TextEncoder();

type SectionMeta = Record<string, number | string | null>;
type BodySection = { name: string; bytes: Uint8Array; meta: SectionMeta };

function align8(n: number): number {
	return (n + 7) & ~7;
}

/**
 * `magic | u32 headerLen | header JSON | padding to 8 | sections`. Each section starts at a multiple
 * of 8. The offsets are in the header, and the header length depends on the offsets, so the encoder
 * repeats until the header length does not change.
 */
function encodeBody(magic: string, fields: Record<string, unknown>, sections: BodySection[]): Uint8Array {
	let headerBytes: Uint8Array = new Uint8Array(0);
	let offsets: number[] = [];
	for (;;) {
		let at = align8(8 + headerBytes.length);
		offsets = sections.map((s) => {
			const offset = at;
			at = align8(at + s.bytes.length);
			return offset;
		});
		const header = {
			schema: BODY_SCHEMA,
			...fields,
			sections: Object.fromEntries(sections.map((s, i) => [s.name, { offset: offsets[i], ...s.meta }])),
		};
		const next = utf8Encoder.encode(JSON.stringify(header));
		if (align8(8 + next.length) === align8(8 + headerBytes.length)) {
			headerBytes = next;
			break;
		}
		headerBytes = next;
	}
	const last =
		sections.length === 0 ? align8(8 + headerBytes.length) : align8(offsets[sections.length - 1] + sections.at(-1)!.bytes.length);
	const body = new Uint8Array(last);
	body.set(utf8Encoder.encode(magic), 0);
	new DataView(body.buffer).setUint32(4, headerBytes.length, true);
	body.set(headerBytes, 8);
	sections.forEach((s, i) => body.set(s.bytes, offsets[i]));
	return body;
}

// ─── the hash body ───────────────────────────────────────────────────────────

/** The false positive rate of the promoted-key Bloom filter of a hash body. */
export const TOPOLOGY_BLOOM_ERROR_RATE = 0.005;
/** The smallest capacity of the promoted-key Bloom filter of a hash body. */
export const TOPOLOGY_BLOOM_MIN_CAPACITY = 16_384;

/**
 * The node key of a split hash partition: `[depth, rootHi, rootLo, ...path]`, from its partition ID
 * bytes `[schema, rootHi, rootLo, depth, ...path]`. The byte order of node keys is the level order of
 * the bitmap: the depth, then the root index, then the child path. The key of the ancestor at depth `d`
 * is `[d, rootHi, rootLo, ...path.subarray(0, d)]`.
 */
export function hashNodeKey(partitionIdBytes: Uint8Array): Uint8Array {
	const depth = partitionIdBytes[3];
	const key = new Uint8Array(3 + depth);
	key[0] = depth;
	key[1] = partitionIdBytes[1];
	key[2] = partitionIdBytes[2];
	key.set(partitionIdBytes.subarray(4, 4 + depth), 3);
	return key;
}

/** True when `parent` is the node key of the parent of the node key `child`. */
function isParentKey(parent: Uint8Array, child: Uint8Array): boolean {
	if (parent.length !== child.length - 1 || parent[0] !== child[0] - 1) {
		return false;
	}
	for (let i = 1; i < parent.length; i++) {
		if (parent[i] !== child[i]) {
			return false;
		}
	}
	return true;
}

/**
 * The hash split bitmap, in level order: positions `0 .. R-1` are the roots, and the children of a
 * split partition at `i` are at `R + K * rank1(i) + c`. The result is padded to a multiple of 8 bytes.
 *
 * `scan()` gives the node keys (`hashNodeKey`) of the `nodeCount` split partitions in byte order, and
 * each ancestor of a split partition is also in it. Each root index is below R and each child index is
 * below K. In level order, `rank1` of a split partition is its index in the scan, and the parents of
 * the scan come in the order of their children. So the encoder reads the scan twice at the same time:
 * one pass for the nodes, and one pass that moves forward to the parent of each node. It holds no node
 * after it moves past it.
 */
export function encodeHashBitmap(
	rootTreesN: number,
	hashSplitN: number,
	nodeCount: number,
	scan: () => Iterable<Uint8Array>,
): { bits: Uint8Array; bitCount: number } {
	const bitCount = rootTreesN + hashSplitN * nodeCount;
	const bits = new Uint8Array(align8(Math.ceil(bitCount / 8)));
	const parents = scan()[Symbol.iterator]();
	let parent: Uint8Array | undefined;
	let parentIdx = -1;
	let count = 0;
	for (const node of scan()) {
		count += 1;
		let pos: number;
		if (node[0] === 0) {
			pos = (node[1] << 8) | node[2];
		} else {
			while (parent === undefined || !isParentKey(parent, node)) {
				const next = parents.next();
				invariant(!next.done, "fokos/topology-encode: a split hash partition has no split parent");
				parent = next.value;
				parentIdx += 1;
			}
			pos = rootTreesN + hashSplitN * parentIdx + node[node.length - 1];
		}
		invariant(count <= nodeCount && pos < bitCount, "fokos/topology-encode: the hash nodes do not agree with nodeCount");
		bits[pos >> 3] |= 1 << (pos & 7);
	}
	invariant(count === nodeCount, "fokos/topology-encode: the hash nodes do not agree with nodeCount");
	return { bits, bitCount };
}

/**
 * The bitmap position of the pending split `node` in `old`, or -1 when `node` is under a leaf of `old`.
 * A pending split is never a split of `old`.
 */
function oldPosition(old: HashTopologyView, node: Uint8Array): number {
	const depth = node[0];
	let pos = (node[1] << 8) | node[2];
	invariant(node.length === 3 + depth && pos < old.rootTreesN, "fokos/topology-encode: a pending hash node is not valid");
	for (let d = 0; d < depth; d++) {
		if (!hashBit(old, pos)) {
			return -1;
		}
		pos = old.rootTreesN + old.hashSplitN * hashRank1(old, pos) + node[3 + d];
	}
	invariant(!hashBit(old, pos), "fokos/topology-encode: a pending hash split is already in the old bitmap");
	return pos;
}

/**
 * Copies `len` bits from `src` at `srcPos` to `dst` at `dstPos`, 32 bits at a time, and returns the
 * number of 1 bits. `dst` is 0 from `dstPos` on, so the copy can OR the words.
 */
function copyBits(src: Uint32Array, srcPos: number, dst: Uint32Array, dstPos: number, len: number): number {
	let ones = 0;
	while (len > 0) {
		const n = len < 32 ? len : 32;
		const i = srcPos >>> 5;
		const s = srcPos & 31;
		let v = s === 0 ? src[i] : (src[i] >>> s) | ((i + 1 < src.length ? src[i + 1] : 0) << (32 - s));
		if (n < 32) {
			v &= 0xffffffff >>> (32 - n);
		}
		if (v !== 0) {
			ones += popcount32(v);
			const j = dstPos >>> 5;
			const t = dstPos & 31;
			dst[j] |= v << t;
			if (t !== 0 && t + n > 32) {
				dst[j + 1] |= v >>> (32 - t);
			}
		}
		srcPos += n;
		dstPos += n;
		len -= n;
	}
	return ones;
}

/** A run of old bitmap positions, or one new node by its node key. */
type MergeEntry = { start: number; len: number; key: Uint8Array | null };

/**
 * The hash split bitmap of the splits of `old` and the `pending` splits, without the rows of the
 * splits of `old`. `pending` gives the node keys (`hashNodeKey`) in byte order, which is the level
 * order. The parent of each pending node is a split of `old` or a pending node, and `nodeCount` is the
 * number of splits of the result.
 *
 * The merge walks the result in level order with a queue. An entry is a run of old positions or one
 * new node under a leaf of `old`. The old nodes keep their order, and the children of the old splits
 * come in the order of their parents, so the children of a run are the next run of old positions. A
 * run therefore moves to the result with a shifted word copy, and its children are one entry. The
 * merge visits one by one only a pending node and each node under it. It stops a run at the old
 * position of the next pending node, which a walk of `old` finds.
 */
export function mergeHashBitmap(
	old: HashTopologyView,
	nodeCount: number,
	pending: Iterable<Uint8Array>,
): { bits: Uint8Array; bitCount: number } {
	const R = old.rootTreesN;
	const K = old.hashSplitN;
	invariant(R > 0 && old.bitCount >= R, "fokos/topology-encode: the old bitmap is empty");
	const bitCount = R + K * nodeCount;
	const bits = new Uint8Array(align8(Math.ceil(bitCount / 8)));
	const out = new Uint32Array(bits.buffer, 0, bits.length / 4);
	const queue: MergeEntry[] = [{ start: 0, len: R, key: null }];
	let front = 0;
	let w = 0;
	let splits = 0;
	// The old position of the children of the next old split.
	let oldChild = R;
	const cursor = pending[Symbol.iterator]();
	let head: Uint8Array | undefined;
	let headPos = -1;
	const nextHead = () => {
		const next = cursor.next();
		head = next.done ? undefined : next.value;
		headPos = head === undefined ? -1 : oldPosition(old, head);
	};
	const appendOld = (start: number, len: number) => {
		const tail = queue[queue.length - 1];
		if (tail.key === null && tail.start + tail.len === start) {
			tail.len += len;
		} else {
			queue.push({ start, len, key: null });
		}
	};
	// Marks the split at `w` and queues its children, which are new nodes.
	const splitNew = (parent: Uint8Array) => {
		out[w >>> 5] |= 1 << (w & 31);
		splits += 1;
		for (let c = 0; c < K; c++) {
			const key = new Uint8Array(parent.length + 1);
			key.set(parent);
			key[0] = parent[0] + 1;
			key[parent.length] = c;
			queue.push({ start: 0, len: 1, key });
		}
		nextHead();
	};
	nextHead();
	while (front < queue.length) {
		const e = queue[front];
		if (e.key === null) {
			const len = headPos >= e.start && headPos < e.start + e.len ? headPos - e.start : e.len;
			invariant(w + len <= bitCount, "fokos/topology-encode: the hash nodes do not agree with nodeCount");
			const ones = copyBits(old.words, e.start, out, w, len);
			w += len;
			e.start += len;
			e.len -= len;
			splits += ones;
			if (ones > 0) {
				appendOld(oldChild, K * ones);
				oldChild += K * ones;
			}
			if (e.len > 0 && headPos === e.start) {
				// An old leaf that is a pending split.
				invariant(w < bitCount, "fokos/topology-encode: the hash nodes do not agree with nodeCount");
				splitNew(head!);
				w += 1;
				e.start += 1;
				e.len -= 1;
			}
		} else {
			invariant(w < bitCount, "fokos/topology-encode: the hash nodes do not agree with nodeCount");
			if (head !== undefined && headPos === -1 && sameBytes(head, e.key)) {
				splitNew(e.key);
			}
			w += 1;
			e.len = 0;
		}
		if (e.len === 0) {
			front += 1;
		}
	}
	invariant(head === undefined, "fokos/topology-encode: a pending hash split has no split parent");
	invariant(
		w === bitCount && splits === nodeCount && oldChild === old.bitCount,
		"fokos/topology-encode: the hash nodes do not agree with nodeCount",
	);
	return { bits, bitCount };
}

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
	if (a.length !== b.length) {
		return false;
	}
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) {
			return false;
		}
	}
	return true;
}

/** The count of 1 bits before each block of `TOPOLOGY_RANK_BLOCK_BITS` bits, and the total at the end. */
function rankIndex(bits: Uint8Array, bitCount: number): Uint32Array {
	const wordCount = Math.ceil(bitCount / 32);
	const words = new Uint32Array(bits.buffer, bits.byteOffset, wordCount);
	const rank = new Uint32Array(Math.ceil(wordCount / TOPOLOGY_RANK_BLOCK_WORDS) + 1);
	let ones = 0;
	for (let w = 0; w < wordCount; w++) {
		if (w % TOPOLOGY_RANK_BLOCK_WORDS === 0) {
			rank[w / TOPOLOGY_RANK_BLOCK_WORDS] = ones;
		}
		ones += popcount32(words[w]);
	}
	rank[rank.length - 1] = ones;
	return rank;
}

export type TopologyBloomSection = { bits: Uint8Array; k: number; m: number; capacity: number; count: number; errorRate: number };

export function encodeHashBody(input: {
	epoch: number;
	version: bigint | number;
	rootTreesN: number;
	hashSplitN: number;
	bitmap: { bits: Uint8Array; bitCount: number };
	bloom: TopologyBloomSection | null;
}): Uint8Array {
	const rank = rankIndex(input.bitmap.bits, input.bitmap.bitCount);
	const sections: BodySection[] = [
		{ name: "bitmap", bytes: input.bitmap.bits, meta: { bitCount: input.bitmap.bitCount } },
		{ name: "rank", bytes: new Uint8Array(rank.buffer), meta: { count: rank.length } },
	];
	if (input.bloom) {
		const { bits, ...meta } = input.bloom;
		sections.push({ name: "bloom", bytes: bits, meta: { length: bits.length, ...meta } });
	}
	return encodeBody(
		HASH_BODY_MAGIC,
		{ epoch: input.epoch, version: String(input.version), rootTreesN: input.rootTreesN, hashSplitN: input.hashSplitN },
		sections,
	);
}

/**
 * One layer with the exact set of `count` promoted keys. The capacity is the smallest power of two of
 * at least `TOPOLOGY_BLOOM_MIN_CAPACITY` and at least `count`, so the filter never adds a layer.
 *
 * When `extend` is given and its base layer has the size of this capacity, the filter starts from a
 * copy of the base bits and adds only `extend.added()`. Otherwise it adds `all()`. The bits are the
 * same in both cases, because a Bloom filter is the OR of the bits of its keys.
 */
export function promotedKeysBloom(
	count: number,
	all: () => Iterable<Uint8Array>,
	extend?: { base: { bits: Uint8Array; k: number; m: number }; added: () => Iterable<Uint8Array> },
): TopologyBloomSection {
	let capacity = TOPOLOGY_BLOOM_MIN_CAPACITY;
	while (capacity < count) {
		capacity *= 2;
	}
	const maxSizeBytes = bloomInitialLayerBytes(TOPOLOGY_BLOOM_ERROR_RATE, capacity);
	let filter: BloomFilter;
	let keys: Iterable<Uint8Array>;
	if (extend !== undefined && extend.base.bits.length === maxSizeBytes) {
		const { bits, k, m } = extend.base;
		filter = BloomFilter.fromSnapshot({
			version: 1,
			errorRate: TOPOLOGY_BLOOM_ERROR_RATE,
			maxSizeBytes,
			initialCapacityN: capacity,
			layers: [{ bits, capacity, count: 0, k, m, layerIndex: 0 }],
		});
		keys = extend.added();
	} else {
		filter = BloomFilter.create({ errorRate: TOPOLOGY_BLOOM_ERROR_RATE, initialCapacityN: capacity, maxSizeBytes });
		keys = all();
	}
	for (const key of keys) {
		filter.add(key);
	}
	const [layer] = filter.toSnapshot().layers;
	return { bits: layer.bits, k: layer.k, m: layer.m, capacity, count, errorRate: TOPOLOGY_BLOOM_ERROR_RATE };
}

// ─── the range body ──────────────────────────────────────────────────────────

/**
 * The known intervals of one promoted hash key. `boundaries` are the inner boundaries in key order,
 * `levels[j]` is the rank of the deepest known interval that has boundary j strictly inside it, and
 * `depths[i]` is the rank of the deepest known interval that holds segment i. No segment: the key is
 * not promoted.
 */
export function encodeRangeBody(input: {
	epoch: number;
	version: bigint | number;
	boundaries: readonly Uint8Array[];
	levels: readonly number[];
	depths: readonly number[];
	truncatedAtRank: number | null;
}): Uint8Array {
	const n = input.boundaries.length;
	let maxRank = 0;
	for (const r of input.depths) {
		maxRank = Math.max(maxRank, r);
	}
	const rankBytes = maxRank < 65_535 ? 2 : 4;
	const offsets = new Uint32Array(n + 1);
	for (let j = 0; j < n; j++) {
		offsets[j + 1] = offsets[j] + input.boundaries[j].length;
	}
	const bytes = new Uint8Array(offsets[n]);
	input.boundaries.forEach((b, j) => bytes.set(b, offsets[j]));
	const ranks = (values: readonly number[]) => {
		const out = rankBytes === 2 ? new Uint16Array(values) : new Uint32Array(values);
		return new Uint8Array(out.buffer);
	};
	return encodeBody(
		RANGE_BODY_MAGIC,
		{ epoch: input.epoch, version: String(input.version), rankBytes, maxRank, truncatedAtRank: input.truncatedAtRank },
		[
			{ name: "boundaryOffsets", bytes: new Uint8Array(offsets.buffer), meta: { count: n + 1 } },
			{ name: "boundaryBytes", bytes, meta: { length: bytes.length } },
			{ name: "boundaryLevel", bytes: ranks(input.levels), meta: { count: n } },
			{ name: "segmentDepth", bytes: ranks(input.depths), meta: { count: input.depths.length } },
		],
	);
}
