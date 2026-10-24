import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { hashChildIndex, hashRootIndex } from "./hash-primitives.js";
import { KeyCodec, type KeyBytes } from "./key-codec.js";
import { findLeaf, parseHashBody, parseRangeBody, HASH_BODY_MAGIC } from "./topology-codec.js";
import { PartitionIdHelper } from "./partition-id.js";
import { encodeHashBitmap, encodeHashBody, encodeRangeBody, hashNodeKey } from "./topology-encode.js";
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

/** The reference walk: follow the hash of the key through the split nodes, and stop before a cut node. */
function referenceLeaf(forest: Forest, hashKey: KeyBytes, cut: ReadonlySet<string>): { node: string; path: number[] } {
	let node = String(hashRootIndex(hashKey, forest.R));
	const path: number[] = [];
	while (forest.children.has(node)) {
		const child = hashChildIndex(hashKey, path.length, forest.K);
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
				fc.array(fc.uint8Array({ minLength: 1, maxLength: 12 }), { minLength: 1, maxLength: 30 }),
				fc.array(fc.nat(), { maxLength: 4 }),
				(R, K, picks, keys, cutPicks) => {
					const forest = buildForest(R, K, picks);
					const pos = positions(forest);
					const view = parseHashBody(encodeForest(forest));
					const nonRoots = [...pos.keys()].filter((node) => node.includes("."));
					const cutNodes = new Set(nonRoots.length === 0 ? [] : cutPicks.map((p) => nonRoots[p % nonRoots.length]));
					const cutPositions = new Set([...cutNodes].map((node) => pos.get(node)!));
					for (const raw of keys) {
						const hashKey = KeyCodec.asKeyBytes(raw);
						for (const [cut, cutPos] of [
							[new Set<string>(), new Set<number>()],
							[cutNodes, cutPositions],
						] as const) {
							const expected = referenceLeaf(forest, hashKey, cut);
							const leaf = findLeaf(view, R, K, hashKey, cutPos);
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
			encodeHashBody({ epoch: 0, version: 0, rootTreesN: 0, hashSplitN: 0, bitmap: { bits: new Uint8Array(0), bitCount: 0 }, bloom: null }),
		);
		expect(view.bitCount).toBe(0);
		const hashKey = KeyCodec.encode("k");
		expect(findLeaf(view, 8, 4, hashKey, new Set())).toEqual({ pos: hashRootIndex(hashKey, 8), root: hashRootIndex(hashKey, 8), path: [] });
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
			const hashKey = KeyCodec.encode(`key-${i}`);
			const expected = referenceLeaf(forest, hashKey, new Set());
			expect(findLeaf(view, 3, 2, hashKey, new Set()).pos).toBe(pos.get(expected.node));
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
		expect(findLeaf(view, 1, 2, KeyCodec.encode("x"), new Set()).path).toHaveLength(1);
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
			boundaries: [KeyCodec.encode("g"), KeyCodec.encode("k")],
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
