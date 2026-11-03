import fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
	asHashPrefixBucket,
	compareHashPrefixBuckets,
	hashPrefixBucketDigits,
	hashPrefixBucketOf,
	type HashPrefixBucket,
} from "./hash-primitives.js";
import { KeyCodec } from "./key-codec.js";
import { findLeaf, isPromotedHashPrefixBucket, parseHashBody, parseRangeBody, HASH_BODY_MAGIC } from "./topology-codec.js";
import { PartitionIdHelper } from "./partition-id.js";
import {
	encodeHashBitmap,
	encodeHashBody,
	encodeRangeBody,
	hashNodeKey,
	mergeHashBitmap,
	mergePromotedBuckets,
	promotedKeysBloom,
	promotedSectionBuckets,
	TOPOLOGY_BLOOM_MIN_CAPACITY,
} from "./topology-encode.js";
import { invariantFailure } from "../../test/errors-matchers.js";

/** A hash forest as a test builds it: each node by its path key, with its children when it split. */
type Forest = { R: number; K: number; children: Map<string, string[]> };

function buildForest(R: number, K: number, picks: number[]): Forest {
	const children = new Map<string, string[]>();
	const leaves = Array.from({ length: R }, (_, r) => String(r));
	for (const pick of picks) {
		const [leaf] = leaves.splice(pick % leaves.length, 1);
		const kids = Array.from({ length: K }, (_, c) => `${leaf}.${c}`);
		children.set(leaf, kids);
		leaves.push(...kids);
	}
	return { R, K, children };
}

/** The level-order position of each node: the roots first, then the children of each split node in order. */
function positions(forest: Forest): Map<string, number> {
	const out = new Map<string, number>();
	let queue = Array.from({ length: forest.R }, (_, r) => String(r));
	while (queue.length > 0) {
		const next: string[] = [];
		for (const node of queue) {
			out.set(node, out.size);
			next.push(...(forest.children.get(node) ?? []));
		}
		queue = next;
	}
	return out;
}

/** The hashPrefixBucket of a test key. */
const bk = (key: string) => hashPrefixBucketOf(KeyCodec.encode(key), "codec-test");

/** The reference walk: follow the digits of the hashPrefixBucket through the split nodes, and stop before a cut node. */
function referenceLeaf(forest: Forest, bucket: HashPrefixBucket, cut: ReadonlySet<string>): { node: string; path: number[] } {
	let node = String(hashPrefixBucketDigits(bucket, forest.R, forest.K, 0).root);
	const path: number[] = [];
	while (forest.children.has(node)) {
		const child = hashPrefixBucketDigits(bucket, forest.R, forest.K, path.length + 1).path[path.length];
		const next = `${node}.${child}`;
		if (cut.has(next)) {
			break;
		}
		node = next;
		path.push(child);
	}
	return { node, path };
}

/** The hash partition ID bytes of a forest node such as "3.0.2". */
function idOf(node: string): Uint8Array {
	return PartitionIdHelper.fromHashIdxs("sg", node.split(".").map(Number)).encode(false).bytes;
}

/** The node keys of the split nodes in byte order, as the keeper reads them from SQLite. */
function sortedNodeKeys(nodes: Iterable<string>): Uint8Array[] {
	return [...nodes].map((node) => hashNodeKey(idOf(node))).sort((a, b) => KeyCodec.compare(KeyCodec.asKeyBytes(a), KeyCodec.asKeyBytes(b)));
}

function encodeForest(forest: Forest): Uint8Array {
	const keys = sortedNodeKeys(forest.children.keys());
	return encodeHashBody({
		epoch: 1,
		version: 7,
		rootTreesN: forest.R,
		hashSplitN: forest.K,
		bitmap: encodeHashBitmap(forest.R, forest.K, keys.length, () => keys),
		promoted: null,
		bloom: null,
	});
}

describe("topology codec — hash body", () => {
	it("findLeaf agrees with a walk of the reference forest, with and without cuts", () => {
		fc.assert(
			fc.property(
				fc.integer({ min: 1, max: 5 }),
				fc.integer({ min: 2, max: 4 }),
				fc.array(fc.nat(), { maxLength: 60 }),
				fc.array(fc.uint8Array({ minLength: 8, maxLength: 8 }).map(asHashPrefixBucket), { minLength: 1, maxLength: 30 }),
				fc.array(fc.nat(), { maxLength: 4 }),
				(R, K, picks, keys, cutPicks) => {
					const forest = buildForest(R, K, picks);
					const pos = positions(forest);
					const view = parseHashBody(encodeForest(forest));
					const nonRoots = [...pos.keys()].filter((node) => node.includes("."));
					const cutNodes = new Set(nonRoots.length === 0 ? [] : cutPicks.map((p) => nonRoots[p % nonRoots.length]));
					const cutPositions = new Set([...cutNodes].map((node) => pos.get(node)!));
					for (const bucket of keys) {
						for (const [cut, cutPos] of [
							[new Set<string>(), new Set<number>()],
							[cutNodes, cutPositions],
						] as const) {
							const expected = referenceLeaf(forest, bucket, cut);
							const leaf = findLeaf(view, R, K, bucket, cutPos);
							expect(leaf.path).toEqual(expected.path);
							expect(leaf.pos).toBe(pos.get(expected.node));
						}
					}
				},
			),
			{ numRuns: 200 },
		);
	});

	it("gives the root for a body with no split", () => {
		const view = parseHashBody(
			encodeHashBody({
				epoch: 0,
				version: 0,
				rootTreesN: 0,
				hashSplitN: 0,
				bitmap: { bits: new Uint8Array(0), bitCount: 0 },
				promoted: null,
				bloom: null,
			}),
		);
		expect(view.bitCount).toBe(0);
		const bucket = bk("k");
		const root = hashPrefixBucketDigits(bucket, 8, 4, 0).root;
		expect(findLeaf(view, 8, 4, bucket, new Set())).toEqual({ pos: root, root, path: [] });
	});

	it("reads the splits of a deep chain across the 512-bit blocks of the rank index", () => {
		// 3 roots and 300 splits, so the bitmap is 3 + 2 * 300 bits, more than one block of the rank index.
		const forest = buildForest(
			3,
			2,
			Array.from({ length: 300 }, (_, i) => (i === 0 ? 0 : 1_000_000)),
		);
		const view = parseHashBody(encodeForest(forest));
		expect(view.bitCount).toBe(3 + 2 * 300);
		const pos = positions(forest);
		for (let i = 0; i < 200; i++) {
			const bucket = bk(`key-${i}`);
			const expected = referenceLeaf(forest, bucket, new Set());
			expect(findLeaf(view, 3, 2, bucket, new Set()).pos).toBe(pos.get(expected.node));
		}
	});

	it("refuses split nodes with a missing parent or a count that does not agree", () => {
		const forest = buildForest(2, 3, [0, 3, 5, 1, 9]);
		const keys = sortedNodeKeys(forest.children.keys());
		expect(() => encodeHashBitmap(2, 3, keys.length - 1, () => keys.slice(1))).toThrow(invariantFailure(/no split parent/));
		expect(() => encodeHashBitmap(2, 3, keys.length + 1, () => keys)).toThrow(invariantFailure(/nodeCount/));
		expect(() => encodeHashBitmap(2, 3, keys.length - 1, () => keys)).toThrow(invariantFailure(/nodeCount/));
	});

	it("refuses a rank section that does not agree with the bitmap", () => {
		const forest = buildForest(2, 3, [0, 3]);
		const body = encodeForest(forest);
		const view = parseHashBody(body);
		view.rank[view.rank.length - 1] += 1;
		expect(() => parseHashBody(body)).toThrow(/rank section/);
	});
});

describe("topology codec — the promoted list", () => {
	const emptyBody = (promoted: Uint8Array | null) =>
		encodeHashBody({
			epoch: 1,
			version: 1,
			rootTreesN: 0,
			hashSplitN: 0,
			bitmap: { bits: new Uint8Array(0), bitCount: 0 },
			promoted,
			bloom: null,
		});

	const bucketArb = fc.uint8Array({ minLength: 8, maxLength: 8 }).map(asHashPrefixBucket);

	it("finds each bucket of the list and no other, at every size up to the limit", () => {
		fc.assert(
			fc.property(fc.uniqueArray(bucketArb, { maxLength: 4_096 }), fc.array(bucketArb, { maxLength: 20 }), (members, probes) => {
				const sorted = [...members].sort(compareHashPrefixBuckets);
				const view = parseHashBody(emptyBody(mergePromotedBuckets([], sorted)));
				for (const bucket of members) {
					expect(isPromotedHashPrefixBucket(view, bucket)).toBe("exact");
				}
				const held = new Set(members.map((b) => b.toHex()));
				for (const probe of probes) {
					expect(isPromotedHashPrefixBucket(view, probe)).toBe(held.has(probe.toHex()) ? "exact" : false);
				}
			}),
			{ numRuns: 30 },
		);
	});

	it("finds each bucket of the largest list", () => {
		const members = Array.from({ length: 4_096 }, (_, i) => {
			const bucket = new Uint8Array(8);
			new DataView(bucket.buffer).setUint32(4, i);
			return asHashPrefixBucket(bucket);
		});
		const view = parseHashBody(emptyBody(mergePromotedBuckets([], members)));
		expect(view.promoted?.count).toBe(4_096);
		for (const bucket of members) {
			expect(isPromotedHashPrefixBucket(view, bucket)).toBe("exact");
		}
		const absent = new Uint8Array(8);
		new DataView(absent.buffer).setUint32(4, 4_096);
		expect(isPromotedHashPrefixBucket(view, asHashPrefixBucket(absent))).toBe(false);
	});

	it("refuses a section with another width, a count outside the body, or values out of order", () => {
		const b1 = asHashPrefixBucket(new Uint8Array([0, 0, 0, 0, 0, 0, 0, 1]));
		const b2 = asHashPrefixBucket(new Uint8Array([0, 0, 0, 0, 0, 0, 0, 2]));
		const ordered = mergePromotedBuckets([], [b1, b2]);
		// The header is JSON, so a one-digit field changes in place, at the same length.
		const patched = (from: string, to: string) => {
			const body = emptyBody(ordered);
			const i = new TextDecoder().decode(body).indexOf(from);
			expect(i).toBeGreaterThanOrEqual(0);
			body.set(new TextEncoder().encode(to), i);
			return body;
		};
		expect(() => parseHashBody(patched('"width":8', '"width":9'))).toThrow(/promoted section/);
		// `count` 9 needs 72 bytes, more than the 16 of the section.
		expect(() => parseHashBody(patched('"count":2', '"count":9'))).toThrow(/promoted section/);
		// The encoder refuses an unordered list, so the test changes the last byte of b1 in the body.
		// The section is the last one, so b1 ends 9 bytes before the end of the body.
		const withFirstLastByte = (value: number) => {
			const body = emptyBody(ordered);
			body[body.length - 9] = value;
			return body;
		};
		// Values out of order: 3 before 2.
		expect(() => parseHashBody(withFirstLastByte(3))).toThrow(/not in order/);
		// A repeated value is not in order either.
		expect(() => parseHashBody(withFirstLastByte(2))).toThrow(/not in order/);
	});

	it("refuses a body with a promoted section and a bloom section", () => {
		const b1 = asHashPrefixBucket(new Uint8Array([0, 0, 0, 0, 0, 0, 0, 1]));
		const body = encodeHashBody({
			epoch: 1,
			version: 1,
			rootTreesN: 0,
			hashSplitN: 0,
			bitmap: { bits: new Uint8Array(0), bitCount: 0 },
			promoted: null,
			bloom: promotedKeysBloom(1, () => [b1]),
		});
		// The encoder refuses both sections, so the test writes a new header with an empty promoted
		// section. The sections move 256 bytes to the back, which gives space for the longer header.
		const shift = 256;
		const headerLength = new DataView(body.buffer, body.byteOffset).getUint32(4, true);
		const header = JSON.parse(new TextDecoder().decode(body.subarray(8, 8 + headerLength)));
		const sectionsStart = (8 + headerLength + 7) & ~7;
		for (const section of Object.values<{ offset: number }>(header.sections)) {
			section.offset += shift;
		}
		header.sections.promoted = { offset: header.sections.bloom.offset, count: 0, width: 8 };
		const headerBytes = new TextEncoder().encode(JSON.stringify(header));
		expect(8 + headerBytes.length).toBeLessThanOrEqual(sectionsStart + shift);
		const both = new Uint8Array(body.length + shift);
		both.set(body.subarray(0, 4), 0);
		new DataView(both.buffer).setUint32(4, headerBytes.length, true);
		both.set(headerBytes, 8);
		both.set(body.subarray(sectionsStart), sectionsStart + shift);
		expect(() => parseHashBody(both)).toThrow(/promoted section and a bloom section/);
	});

	it("does not encode a list out of order, or a body with both sections", () => {
		const b1 = asHashPrefixBucket(new Uint8Array([0, 0, 0, 0, 0, 0, 0, 1]));
		const b2 = asHashPrefixBucket(new Uint8Array([0, 0, 0, 0, 0, 0, 0, 2]));
		expect(() => emptyBody(new Uint8Array([...b2, ...b1]))).toThrow(invariantFailure(/not in strict order/));
		expect(() => emptyBody(new Uint8Array([...b1, ...b1]))).toThrow(invariantFailure(/not in strict order/));
		expect(() =>
			encodeHashBody({
				epoch: 1,
				version: 1,
				rootTreesN: 0,
				hashSplitN: 0,
				bitmap: { bits: new Uint8Array(0), bitCount: 0 },
				promoted: new Uint8Array(b1),
				bloom: promotedKeysBloom(1, () => [b1]),
			}),
		).toThrow(invariantFailure(/not both/));
	});

	it("writes a repeated value once, from one stream or from both", () => {
		const b1 = asHashPrefixBucket(new Uint8Array([0, 0, 0, 0, 0, 0, 0, 1]));
		const b2 = asHashPrefixBucket(new Uint8Array([0, 0, 0, 0, 0, 0, 0, 2]));
		const once = new Uint8Array([...b1, ...b2]);
		expect(mergePromotedBuckets([], [b1, b1, b2])).toEqual(once);
		expect(mergePromotedBuckets([b1, b1], [b2, b2])).toEqual(once);
		expect(mergePromotedBuckets([b1, b2], [b1, b2])).toEqual(once);
	});

	it("merges a stored list and new roots in one pass, equal to a build of every root", () => {
		fc.assert(
			fc.property(fc.uniqueArray(bucketArb, { maxLength: 200 }), fc.uniqueArray(bucketArb, { maxLength: 200 }), (base, added) => {
				const stored = mergePromotedBuckets([], [...base].sort(compareHashPrefixBuckets));
				const view = parseHashBody(emptyBody(stored));
				// The keeper sorts the reported roots before the merge; the index gives them in seq order.
				const merged = mergePromotedBuckets(promotedSectionBuckets(view.promoted!), [...added].sort(compareHashPrefixBuckets));
				const all = mergePromotedBuckets([], [...base, ...added].sort(compareHashPrefixBuckets));
				expect(merged).toEqual(all);
			}),
			{ numRuns: 50 },
		);
	});
});

describe("topology codec — merge of new hash splits", () => {
	function merged(before: Forest, after: Forest): Uint8Array {
		const added = sortedNodeKeys([...after.children.keys()].filter((node) => !before.children.has(node)));
		return encodeHashBody({
			epoch: 1,
			version: 7,
			rootTreesN: after.R,
			hashSplitN: after.K,
			bitmap: mergeHashBitmap(parseHashBody(encodeForest(before)), after.children.size, added),
			promoted: null,
			bloom: null,
		});
	}

	it("gives the body of a full build, from the bitmap of any earlier forest", () => {
		fc.assert(
			fc.property(
				fc.integer({ min: 1, max: 5 }),
				fc.integer({ min: 2, max: 4 }),
				fc.array(fc.nat(), { maxLength: 120 }),
				fc.nat(),
				(R, K, picks, at) => {
					const before = buildForest(R, K, picks.slice(0, at % (picks.length + 1)));
					const after = buildForest(R, K, picks);
					expect(merged(before, after)).toEqual(encodeForest(after));
				},
			),
			{ numRuns: 300 },
		);
	});

	it("gives the body of a full build when the new splits are deep under one old leaf and spread over many words", () => {
		// 2,000 old splits, then a chain of 200 splits under the last leaf and 300 splits spread over the leaves.
		const picks = Array.from({ length: 2_000 }, (_, i) => (i * 7_919) % 4_001);
		const chain = Array.from({ length: 200 }, () => 1_000_000_000);
		const spread = Array.from({ length: 300 }, (_, i) => i * 13);
		const before = buildForest(16, 2, picks);
		expect(merged(before, buildForest(16, 2, [...picks, ...chain, ...spread]))).toEqual(
			encodeForest(buildForest(16, 2, [...picks, ...chain, ...spread])),
		);
	});

	it("refuses a pending split that the old bitmap has, that has no split parent, or a count that does not agree", () => {
		const before = buildForest(2, 3, [0, 3]);
		const after = buildForest(2, 3, [0, 3, 5, 6]);
		const old = parseHashBody(encodeForest(before));
		const added = sortedNodeKeys([...after.children.keys()].filter((node) => !before.children.has(node)));
		expect(() => mergeHashBitmap(old, 3, sortedNodeKeys(before.children.keys()).slice(0, 1))).toThrow(
			invariantFailure(/already in the old bitmap/),
		);
		const orphan = sortedNodeKeys(["1.2.0"]);
		expect(() => mergeHashBitmap(old, 3, orphan)).toThrow(invariantFailure(/no split parent/));
		expect(() => mergeHashBitmap(old, after.children.size + 1, added)).toThrow(invariantFailure(/nodeCount/));
		expect(() => mergeHashBitmap(old, after.children.size - 1, added)).toThrow(invariantFailure(/nodeCount/));
	});
});

describe("topology codec — promoted-key Bloom filter", () => {
	const keys = (from: number, to: number) => Array.from({ length: to - from }, (_, i) => KeyCodec.encode(`user-${from + i}`));

	it("extends the old filter with the new keys only, and gives the bits of a full build", () => {
		const base = promotedKeysBloom(100, () => keys(0, 100));
		const extended = promotedKeysBloom(150, () => [], { base, added: () => keys(100, 150) });
		expect(extended).toEqual(promotedKeysBloom(150, () => keys(0, 150)));
	});

	it("builds the filter again from all the keys when the count needs a larger capacity", () => {
		const base = promotedKeysBloom(TOPOLOGY_BLOOM_MIN_CAPACITY, () => keys(0, 10));
		const all = keys(0, TOPOLOGY_BLOOM_MIN_CAPACITY + 1);
		const grown = promotedKeysBloom(all.length, () => all, { base, added: () => [] });
		expect(grown.capacity).toBe(2 * TOPOLOGY_BLOOM_MIN_CAPACITY);
		expect(grown).toEqual(promotedKeysBloom(all.length, () => all));
	});
});

/**
 * A body with the sections at fixed offsets from 4096, so a test can change the header without moving
 * the sections. `sections` gives the bytes, and `header.sections` the fields of each section.
 */
function frame(
	magic: string,
	header: Record<string, unknown>,
	sections: Record<string, Uint8Array>,
	offsets: Record<string, number> = {},
): Uint8Array {
	let at = 4096;
	const placed: Record<string, number> = {};
	for (const [name, bytes] of Object.entries(sections)) {
		placed[name] = offsets[name] ?? at;
		at = (at + bytes.length + 7) & ~7;
	}
	const headerSections = header.sections as Record<string, Record<string, unknown>>;
	const withOffsets = {
		...header,
		sections: Object.fromEntries(Object.entries(headerSections).map(([name, meta]) => [name, { offset: placed[name], ...meta }])),
	};
	const json = new TextEncoder().encode(JSON.stringify(withOffsets));
	const body = new Uint8Array(Math.max(at, ...Object.entries(sections).map(([name, b]) => placed[name] + b.length)));
	body.set(new TextEncoder().encode(magic), 0);
	new DataView(body.buffer).setUint32(4, json.length, true);
	body.set(json, 8);
	for (const [name, bytes] of Object.entries(sections)) {
		if (placed[name] + bytes.length <= body.length) {
			body.set(bytes, placed[name]);
		}
	}
	return body;
}

describe("topology codec — body framing", () => {
	const header = (extra: Record<string, unknown> = {}) => ({
		schema: 1,
		epoch: 2,
		version: "9",
		rootTreesN: 1,
		hashSplitN: 2,
		sections: { bitmap: { bitCount: 3 }, rank: { count: 2 } },
		...extra,
	});
	const bitmap = new Uint8Array([0b001, 0, 0, 0, 0, 0, 0, 0]);
	// One block of the rank index, and the total of 1 bits.
	const rank = new Uint8Array(new Uint32Array([0, 1]).buffer);

	it("ignores a header key and a section that it does not know", () => {
		const body = frame(
			HASH_BODY_MAGIC,
			header({ future: { a: 1 }, sections: { bitmap: { bitCount: 3 }, rank: { count: 2 }, future: { count: 2 } } }),
			{
				bitmap,
				rank,
				future: new Uint8Array(16),
			},
		);
		const view = parseHashBody(body);
		expect(view.version).toBe(9n);
		expect(findLeaf(view, 1, 2, bk("x"), new Set()).path).toHaveLength(1);
	});

	it("refuses another magic, another schema, a section outside the body, and an offset that is not a multiple of 8", () => {
		expect(() => parseHashBody(frame("FKR1", header(), { bitmap, rank }))).toThrow(/FKT1/);
		expect(() => parseHashBody(frame(HASH_BODY_MAGIC, header({ schema: 2 }), { bitmap, rank }))).toThrow(/schema/);
		expect(() =>
			parseHashBody(frame(HASH_BODY_MAGIC, header({ sections: { bitmap: { bitCount: 1_000 }, rank: { count: 3 } } }), { bitmap, rank })),
		).toThrow(/bitmap/);
		expect(() => parseHashBody(frame(HASH_BODY_MAGIC, header(), { bitmap, rank }, { bitmap: 4100 }))).toThrow(/bitmap/);
	});

	it("reads a range body that is a view inside a larger buffer", () => {
		const encoded = encodeRangeBody({
			epoch: 1,
			version: 4,
			boundaries: [
				{ hk: KeyCodec.encode("h"), sk: KeyCodec.encode("g") },
				{ hk: KeyCodec.encode("h"), sk: KeyCodec.encode("k") },
			],
			levels: [0, 1],
			depths: [1, 2, 2],
			truncatedAtRank: null,
		});
		const outer = new Uint8Array(encoded.length + 3);
		outer.set(encoded, 3);
		const view = parseRangeBody(outer.subarray(3));
		expect(view.n).toBe(2);
		expect([...view.levels]).toEqual([0, 1]);
		expect([...view.depths]).toEqual([1, 2, 2]);
		expect(view.version).toBe(4n);
	});
});
