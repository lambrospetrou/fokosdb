import { describe, expect, it } from "vitest";
import { KeyCodec, type FokosCompositeKey } from "./key-codec.js";
import {
	encodeRangeComponent,
	identityDepth,
	isHashPartition,
	isRangePartition,
	partitionIdentityFrom,
	PartitionIdHelper,
	resolveDescendantHashPartitionContext,
	resolveHashChildPartitionContexts,
	resolveRangePartitionContext,
} from "./partition-id.js";
import { FokosRouter } from "./router.js";
import { asRoutingHash, routingHashSeedOf, MAX_HASH_DEPTH } from "./hash-primitives.js";
import { validateTopology, type FokosRouteContext } from "./route-context.js";
import { fokosErrorWith, invariantFailure } from "../../test/errors-matchers.js";

const kb = (s: string) => KeyCodec.encode(s);
const ROUTING_HASH = asRoutingHash(Uint8Array.fromHex("00112233445566ff"));
const ROUTING_HASH_HEX = "00112233445566ff";
/** A composite boundary. The empty string is the empty sort key. */
const c = (hk: string, sk: string): FokosCompositeKey => ({ hk: kb(hk), sk: KeyCodec.encodeOptional(sk === "" ? undefined : sk) });

const base = "iddb";
const HASH_SPLIT_N = 4;
// The topology with the deepest depth limit, MAX_HASH_DEPTH.
const DEEPEST = { rootTreesN: 1, hashSplitN: 2 };

function makeRouter(): FokosRouter<{ tier: string }> {
	return new FokosRouter(
		{ shardGroup: base, rootTreesN: 4, hashSplitN: HASH_SPLIT_N },
		{ rangeSplitN: 4, rangeAncestors: { fromRoot: 0, fromLeaf: 3 } },
		{ tier: "t" },
	);
}

describe("PartitionIdHelper — hash codec round-trips", () => {
	it("encodes a root (depth 0) and reads it back", () => {
		const { bytes, opaque, doName } = PartitionIdHelper.fromHashIdxs(base, [3]).encode(true);
		expect(doName).toBe("iddb~h.3");
		expect(PartitionIdHelper.rootIdx(bytes)).toBe(3);
		expect(PartitionIdHelper.depth(bytes)).toBe(0);
		expect(isHashPartition({ partitionId: opaque })).toBe(true);
		expect(isRangePartition({ partitionId: opaque })).toBe(false);
		const decoded = PartitionIdHelper.decode(Uint8Array.fromHex(opaque));
		expect(decoded).toEqual({ schema: 0, rootIdx: 3, depth: 0 });
	});

	it("encodes a u16 root index (> 255) correctly", () => {
		const { bytes, doName } = PartitionIdHelper.fromHashIdxs(base, [4097]).encode(true);
		expect(doName).toBe("iddb~h.4097");
		expect(PartitionIdHelper.rootIdx(bytes)).toBe(4097);
	});

	it("hashId gives the ID and the DO name of fromHashIdxs", () => {
		const paths: [number, number[]][] = [
			[0, []],
			[1, []],
			[15, []],
			[16, []],
			[255, []],
			[256, []],
			[4097, []],
			[0xffff, []],
			[3, [0]],
			[3, [15, 16, 255]],
			[64_999, [1, 2, 0, 3]],
			[7, Array.from({ length: 255 }, (_, i) => i)],
		];
		for (const [rootIdx, childIdxs] of paths) {
			const { opaque, doName } = PartitionIdHelper.fromHashIdxs(base, [rootIdx, ...childIdxs]).encode(true);
			expect(PartitionIdHelper.hashId(base, rootIdx, childIdxs)).toEqual({ opaque, doName });
		}
		expect(PartitionIdHelper.hashId(base, 3)).toEqual(PartitionIdHelper.hashId(base, 3, []));
	});

	it("hashId rejects a root index, a child index or a depth out of range", () => {
		for (const rootIdx of [0x10000, -1, 1.5]) {
			expect(() => PartitionIdHelper.hashId(base, rootIdx)).toThrow(invariantFailure(/rootIdx must be a u16/));
		}
		for (const childIdx of [256, -1, 1.5]) {
			expect(() => PartitionIdHelper.hashId(base, 0, [1, childIdx])).toThrow(invariantFailure(/a child index must be a u8/));
		}
		expect(() =>
			PartitionIdHelper.hashId(
				base,
				0,
				Array.from({ length: 256 }, () => 0),
			),
		).toThrow(invariantFailure(/the depth must be a u8/));
	});

	it("fromHashIdxs with child indexes sets depth and lastChildIdx", () => {
		const { bytes, doName } = PartitionIdHelper.fromHashIdxs(base, [1, 2, 0]).encode(true);
		expect(doName).toBe("iddb~h.1.2.0");
		expect(PartitionIdHelper.rootIdx(bytes)).toBe(1);
		expect(PartitionIdHelper.depth(bytes)).toBe(2);
		expect(PartitionIdHelper.lastChildIdx(bytes)).toBe(0);
	});

	it("appendHashIdx on existing bytes extends the depth (single and array forms)", () => {
		const root = PartitionIdHelper.fromHashIdxs(base, [0]).encode(false);

		const single = new PartitionIdHelper(base, root.bytes).appendHashIdx(1).encode(true);
		expect(single.doName).toBe("iddb~h.0.1");
		expect(PartitionIdHelper.depth(single.bytes)).toBe(1);
		expect(PartitionIdHelper.lastChildIdx(single.bytes)).toBe(1);

		const multi = new PartitionIdHelper(base, root.bytes).appendHashIdx([1, 3]).encode(true);
		expect(multi.doName).toBe("iddb~h.0.1.3");
		expect(PartitionIdHelper.depth(multi.bytes)).toBe(2);
		expect(PartitionIdHelper.lastChildIdx(multi.bytes)).toBe(3);
	});

	it("accepts the opaque hex string as constructor input (same result as bytes)", () => {
		const root = PartitionIdHelper.fromHashIdxs(base, [2]).encode(false);
		const fromHex = new PartitionIdHelper(base, root.opaque).appendHashIdx(1).encode(true);
		const fromBytes = new PartitionIdHelper(base, root.bytes).appendHashIdx(1).encode(true);
		expect(fromHex.opaque).toBe(fromBytes.opaque);
		expect(fromHex.doName).toBe(fromBytes.doName);
	});

	it("encode throws with nothing to encode and when appending to a range ID", () => {
		expect(() => new PartitionIdHelper(base).encode(false)).toThrow(invariantFailure(/no bytes or appended hash indexes/));
		const range = PartitionIdHelper.fromRangePartition(base, ROUTING_HASH, null, null).encode(false);
		expect(() => new PartitionIdHelper(base, range.bytes).appendHashIdx(1).encode(false)).toThrow(
			invariantFailure(/cannot append hash indexes/),
		);
	});

	it("hashDescendantId gives the ID and the DO name of appendHashIdx", () => {
		const parents = [[0], [7], [4097], [0xffff], [3, 0], [3, 15, 16, 255], [1, ...Array.from({ length: 61 }, (_, i) => i % 256)]];
		const childPaths = [[], [0], [3], [255], [1, 2], [16, 15, 0]];
		for (const parentIdxs of parents) {
			const parent = PartitionIdHelper.fromHashIdxs(base, parentIdxs).encode(true);
			const ref = { partitionId: parent.opaque, doName: parent.doName!, topology: DEEPEST };
			for (const childIdxs of childPaths) {
				if (parentIdxs.length - 1 + childIdxs.length > MAX_HASH_DEPTH) {
					continue;
				}
				const { opaque, doName } = new PartitionIdHelper(base, parent.bytes).appendHashIdx(childIdxs).encode(true);
				expect(PartitionIdHelper.hashDescendantId(ref, childIdxs)).toEqual({ opaque, doName });
			}
		}
	});

	it("hashDescendantId rejects a parent that is not a hash ID, a child index or a depth out of range", () => {
		const root = PartitionIdHelper.fromHashIdxs(base, [1]).encode(true);
		const ref = { partitionId: root.opaque, doName: root.doName!, topology: DEEPEST };
		const range = PartitionIdHelper.fromRangePartition(base, ROUTING_HASH, null, null).encode(true);
		const notHash = [
			{ partitionId: range.opaque, doName: range.doName!, topology: DEEPEST },
			// The depth byte says 1, but the ID has no child index.
			{ partitionId: "00000101", doName: "iddb~h.1", topology: DEEPEST },
			{ partitionId: "000001", doName: "iddb~h.1", topology: DEEPEST },
			{ partitionId: "000001000", doName: "iddb~h.1", topology: DEEPEST },
		];
		for (const parent of notHash) {
			expect(() => PartitionIdHelper.hashDescendantId(parent, [0])).toThrow(invariantFailure(/the parent must be a hash partition ID/));
		}
		for (const childIdx of [256, -1, 1.5]) {
			expect(() => PartitionIdHelper.hashDescendantId(ref, [0, childIdx])).toThrow(invariantFailure(/a child index must be a u8/));
		}
		const deep = PartitionIdHelper.fromHashIdxs(base, [1, ...Array.from({ length: MAX_HASH_DEPTH }, () => 0)]).encode(true);
		expect(() => PartitionIdHelper.hashDescendantId({ partitionId: deep.opaque, doName: deep.doName!, topology: DEEPEST }, [0])).toThrow(
			fokosErrorWith("partition_context_options_invalid", { option: "partitionId", value: MAX_HASH_DEPTH + 1, limit: MAX_HASH_DEPTH }),
		);
	});

	it("refuses a hash partition deeper than the depth limit of its topology", () => {
		// rootTreesN 65,000 and hashSplitN 255 have the depth limit 6.
		const topology = { ...makeRouter().topology, rootTreesN: 65_000, hashSplitN: 255 };
		const at = (depth: number) => {
			const { opaque, doName } = PartitionIdHelper.hashId(
				base,
				0,
				Array.from({ length: depth }, () => 0),
			);
			return { ...makeRouter().allRoots()[0], topology, partitionId: opaque, doName };
		};
		expect(partitionIdentityFrom(at(6)).hash!.depth).toBe(6);
		expect(() => partitionIdentityFrom(at(7))).toThrow(
			fokosErrorWith("partition_context_options_invalid", { option: "partitionId", value: 7, limit: 6 }),
		);
		expect(PartitionIdHelper.hashDescendantId(at(5), [254]).doName).toBe(`${base}~h.0.0.0.0.0.0.254`);
		expect(() => PartitionIdHelper.hashDescendantId(at(6), [0])).toThrow(fokosErrorWith("partition_context_options_invalid", { limit: 6 }));
	});

	it("calculateHashChildPartitionIds produces hashSplitN distinct children one level deeper", () => {
		const parent = PartitionIdHelper.fromHashIdxs(base, [1]).encode(true);
		const children = PartitionIdHelper.calculateHashChildPartitionIds({
			...makeRouter().allRoots()[1],
			doName: parent.doName!,
			partitionId: parent.opaque,
		});
		expect(children).toHaveLength(HASH_SPLIT_N);
		expect(new Set(children.map((c) => c.doName)).size).toBe(HASH_SPLIT_N);
		for (let i = 0; i < children.length; i++) {
			expect(children[i].doName).toBe(`iddb~h.1.${i}`);
			const bytes = Uint8Array.fromHex(children[i].partitionIdOpaque);
			expect(PartitionIdHelper.depth(bytes)).toBe(1);
			expect(PartitionIdHelper.lastChildIdx(bytes)).toBe(i);
		}
	});
});

describe("PartitionIdHelper — range codec round-trips", () => {
	it("round-trips all boundary combinations", () => {
		for (const [start, end] of [
			[null, null],
			[null, c("alice", "m")],
			[c("alice", "m"), null],
			[c("alice", "b1"), c("alice", "b2")],
			[c("alice", ""), c("bob", "")],
		] as const) {
			const { bytes, opaque } = PartitionIdHelper.fromRangePartition(base, ROUTING_HASH, start, end).encode(false);
			expect(isRangePartition({ partitionId: opaque })).toBe(true);
			const decoded = PartitionIdHelper.decode(bytes);
			expect(decoded).toEqual({ schema: 1, routingHash: ROUTING_HASH, startBoundary: start, endBoundary: end });
		}
	});

	it("doName formats range IDs via rangePartitionDoName", () => {
		const { bytes, doName } = PartitionIdHelper.fromRangePartition(base, ROUTING_HASH, c("alice", "b1"), null).encode(true);
		expect(doName).toBe(`iddb~r.${ROUTING_HASH_HEX}.alice.b1.~max`);
		expect(PartitionIdHelper.doName(base, bytes)).toBe(doName);
	});

	it("depth reads the hex ID with the same result as the bytes, up to the largest depth", () => {
		for (const idxs of [[3], [1, 2], [0, 1, 2, 3], [0, ...Array.from({ length: 255 }, (_, i) => i % 7)]]) {
			const { bytes, opaque } = PartitionIdHelper.fromHashIdxs(base, idxs).encode(true);
			expect(PartitionIdHelper.depth(opaque)).toBe(PartitionIdHelper.depth(bytes));
			expect(PartitionIdHelper.depth(opaque.toUpperCase())).toBe(idxs.length - 1);
		}
	});

	it("depth rejects a hex ID that is not a hash ID or that has no valid depth", () => {
		const { opaque } = PartitionIdHelper.fromRangePartition(base, ROUTING_HASH, null, null).encode(true);
		expect(() => PartitionIdHelper.depth(opaque)).toThrow(invariantFailure(/expected hash schema/));
		for (const malformed of ["000001", "000001g0", "0000010"]) {
			expect(() => PartitionIdHelper.depth(malformed)).toThrow(invariantFailure(/has no valid depth/));
		}
	});

	it("hash-only readers reject range IDs", () => {
		const { bytes } = PartitionIdHelper.fromRangePartition(base, ROUTING_HASH, null, null).encode(false);
		expect(() => PartitionIdHelper.rootIdx(bytes)).toThrow(invariantFailure(/expected hash schema/));
		expect(() => PartitionIdHelper.depth(bytes)).toThrow(invariantFailure(/expected hash schema/));
		expect(() => PartitionIdHelper.lastChildIdx(bytes)).toThrow(invariantFailure(/expected hash schema/));
	});
});

describe("PartitionIdHelper — range schema (SCHEMA_RANGE_V1)", () => {
	it("fromRangePartition root: encode then decode round-trips (both boundaries null)", () => {
		const { bytes, opaque, doName } = PartitionIdHelper.fromRangePartition(base, ROUTING_HASH, null, null).encode(true);
		expect(bytes[0]).toBe(PartitionIdHelper.SCHEMA_RANGE_V1);
		// A range root has a fixed length for each shard group.
		expect(doName).toBe(`iddb~r.${ROUTING_HASH_HEX}.~min.~max`);
		const decoded = PartitionIdHelper.decode(bytes);
		expect(decoded).toEqual({ schema: 1, routingHash: ROUTING_HASH, startBoundary: null, endBoundary: null });
		expect(opaque).toMatch(/^01[A-Za-z0-9_-]+\.$/);
		expect(PartitionIdHelper.decode(PartitionIdHelper.partitionIdToBytes(opaque))).toEqual(decoded);
	});

	it("names each bounded edge with its hash key and its sort key, and an empty sort key with an empty component", () => {
		for (const [start, end, tail] of [
			[c("alice", "b1"), c("alice", "b2"), "alice.b1.alice.b2"],
			[null, c("alice", "m"), "~min.alice.m"],
			[c("alice", "m"), null, "alice.m.~max"],
			[c("alice", ""), c("bob", ""), "alice..bob."],
		] as const) {
			const { bytes, doName } = PartitionIdHelper.fromRangePartition(base, ROUTING_HASH, start, end).encode(true);
			expect(doName).toBe(`iddb~r.${ROUTING_HASH_HEX}.${tail}`);
			expect(PartitionIdHelper.decode(bytes)).toEqual({ schema: 1, routingHash: ROUTING_HASH, startBoundary: start, endBoundary: end });
		}
	});

	it("handles unicode in the boundaries", () => {
		const { bytes } = PartitionIdHelper.fromRangePartition(base, ROUTING_HASH, c("café☕", "töst"), c("café☕", "zünd")).encode(false);
		expect(PartitionIdHelper.decode(bytes)).toEqual({
			schema: 1,
			routingHash: ROUTING_HASH,
			startBoundary: c("café☕", "töst"),
			endBoundary: c("café☕", "zünd"),
		});
	});

	it("doName dispatches correctly for range ID loaded from its opaque ID", () => {
		const { opaque } = PartitionIdHelper.fromRangePartition(base, ROUTING_HASH, c("mykey", "start1"), c("mykey", "end1")).encode(false);
		const bytes = PartitionIdHelper.partitionIdToBytes(opaque);
		expect(PartitionIdHelper.doName(base, bytes)).toBe(`iddb~r.${ROUTING_HASH_HEX}.mykey.start1.mykey.end1`);
	});
});

describe("PartitionIdHelper — range ID base64url parts", () => {
	const bin = (...values: number[]) => KeyCodec.asKeyBytes(new Uint8Array(values));
	const cases: [string, FokosCompositeKey | null, FokosCompositeKey | null][] = [
		["root", null, null],
		["null start", null, c("alice", "m")],
		["null end", c("alice", "m"), null],
		["two bounds", c("alice", "b1"), c("alice", "b2")],
		["unicode", c("café☕", "töst"), c("café☕", "zünd")],
		["reserved DO-name bytes", { hk: kb("a.b%c~d"), sk: kb('x"y\\z') }, { hk: kb("a.b%c~d"), sk: kb("~max") }],
		["binary", { hk: KeyCodec.encode(new Uint8Array([0, 0xff, 0x2e])), sk: bin(0xff, 0x00, 0x7e) }, { hk: bin(0xff, 0xfe), sk: bin() }],
		["empty sort keys", c("k", ""), c("l", "")],
	];

	it.each(cases)("round-trips byte for byte: %s", (_, start, end) => {
		const ctx = resolveRangePartitionContext(makeRouter().allRoots()[0], ROUTING_HASH, start, end);
		const { bytes, opaque, doName } = PartitionIdHelper.fromRangePartition(base, ROUTING_HASH, start, end).encode(true);
		expect(ctx.partitionId).toBe(opaque);
		expect(ctx.doName).toBe(doName);
		expect(opaque).toMatch(/^01[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*$/);
		expect(PartitionIdHelper.partitionIdToBytes(opaque)).toEqual(bytes);
		expect(PartitionIdHelper.decode(bytes)).toEqual({ schema: 1, routingHash: ROUTING_HASH, startBoundary: start, endBoundary: end });
		expect(PartitionIdHelper.rangeRoutingHash(opaque)).toEqual(ROUTING_HASH);
		// The name from the keys equals the name from a full ID decode.
		expect(PartitionIdHelper.doName(base, PartitionIdHelper.partitionIdToBytes(opaque))).toBe(ctx.doName);
		expect(new PartitionIdHelper(base, opaque).encode(true)).toEqual({ bytes, opaque, doName });
	});

	it("keeps the flat byte layout and puts the boundaries in the second part only", () => {
		const start = c("hk", "s");
		const end = c("hk", "e");
		const { bytes, opaque } = PartitionIdHelper.fromRangePartition(base, ROUTING_HASH, start, end).encode(false);
		const first = new Uint8Array([0x03, ...ROUTING_HASH]);
		const boundary = (b: FokosCompositeKey) => [b.hk.length, 0, 0, 0, b.sk.length, 0, 0, 0, ...b.hk, ...b.sk];
		const second = new Uint8Array([...boundary(start), ...boundary(end)]);
		expect(bytes).toEqual(new Uint8Array([0x01, ...first, ...second]));
		expect(opaque).toBe(
			"01" +
				first.toBase64({ alphabet: "base64url", omitPadding: true }) +
				"." +
				second.toBase64({ alphabet: "base64url", omitPadding: true }),
		);
	});

	it("reads the routingHash without the boundary part", () => {
		const { opaque } = PartitionIdHelper.fromRangePartition(base, ROUTING_HASH, c("alice", "b1"), c("alice", "b2")).encode(false);
		const firstOnly = opaque.slice(0, opaque.indexOf(".") + 1) + "!!!";
		expect(PartitionIdHelper.rangeRoutingHash(firstOnly)).toEqual(ROUTING_HASH);
		expect(() => PartitionIdHelper.partitionIdToBytes(firstOnly)).toThrow(invariantFailure(/not canonical base64url/));
	});

	it("rejects malformed separators and non-canonical base64url", () => {
		const { opaque } = PartitionIdHelper.fromRangePartition(base, ROUTING_HASH, c("alice", "b1"), c("alice", "b2")).encode(false);
		const [firstText, secondText] = opaque.slice(2).split(".");
		const decode = (id: string) => () => PartitionIdHelper.partitionIdToBytes(id);
		expect(decode("01" + firstText + secondText)).toThrow(invariantFailure(/no separator/));
		expect(decode(opaque + ".")).toThrow(invariantFailure(/canonical/));
		expect(decode("01" + firstText + "." + secondText + "==")).toThrow(invariantFailure(/base64url/));
		expect(decode("01" + firstText + "." + "+/AA")).toThrow(invariantFailure(/base64url/));
		expect(decode("01" + firstText + "." + " " + secondText)).toThrow(invariantFailure(/base64url/));
		expect(decode("01" + firstText + ".A")).toThrow(invariantFailure(/canonical/));
		expect(decode("01" + firstText + ".AA==")).toThrow(invariantFailure(/canonical/));
		// "AB" and "AA" both decode to one zero byte; only "AA" is canonical.
		expect(decode("01" + firstText + ".AB")).toThrow(invariantFailure(/canonical/));
	});

	it("rejects invalid flags and inconsistent lengths", () => {
		const b64 = (...values: number[]) => new Uint8Array(values).toBase64({ alphabet: "base64url", omitPadding: true });
		const decode = (first: number[], second: number[]) => () =>
			PartitionIdHelper.decode(PartitionIdHelper.partitionIdToBytes("01" + b64(...first) + "." + b64(...second)));
		// flags, then the routingHash; each boundary: hkLen u32 LE, skLen u32 LE, hk, sk
		const one = [1, 0, 0, 0, 1, 0, 0, 0, 0x6b, 0x61];
		expect(decode([0x01, ...ROUTING_HASH], one)).not.toThrow();
		expect(decode([0x03, ...ROUTING_HASH], [...one, ...one])).not.toThrow();
		expect(decode([0x04, ...ROUTING_HASH], [])).toThrow(invariantFailure(/invalid range partition ID flags/));
		expect(decode([0x00, ...ROUTING_HASH.subarray(1)], [])).toThrow(invariantFailure(/too short|wrong length/));
		expect(decode([0x00, ...ROUTING_HASH], [0x61])).toThrow(invariantFailure(/bytes after its boundaries/));
		expect(decode([0x01, ...ROUTING_HASH], [1, 0, 0, 0, 2, 0, 0, 0, 0x6b, 0x61])).toThrow(
			invariantFailure(/boundary length is inconsistent/),
		);
		expect(decode([0x01, ...ROUTING_HASH], [1, 0, 0])).toThrow(invariantFailure(/boundary is too short/));
		expect(decode([0x01, ...ROUTING_HASH], [...one, 0x62])).toThrow(invariantFailure(/bytes after its boundaries/));
		expect(() => PartitionIdHelper.rangeRoutingHash("01" + b64(0x08, ...ROUTING_HASH) + ".")).toThrow(
			invariantFailure(/invalid range partition ID flags/),
		);
	});

	it("partitionIdentityFrom decodes both parts of a range ID", () => {
		const ctx = resolveRangePartitionContext(makeRouter().allRoots()[0], ROUTING_HASH, c("alice", "b1"), null);
		const identity = partitionIdentityFrom(ctx, { depth: 1, ancestors: [] });
		expect(identity.range).toEqual({ routingHash: ROUTING_HASH, start: c("alice", "b1"), end: null, depth: 1, ancestors: [] });
	});

	it("gives one DO name for each routingHash and pair of composite boundaries", () => {
		// Short random keys from a small alphabet that holds the separator, the sentinel bytes, and the
		// escape byte, so near collisions are frequent. The empty sort key is common.
		const alphabet = [0x2e, 0x7e, 0x6d, 0x25, 0x61, 0xff];
		const randomKey = (min: number) =>
			KeyCodec.asKeyBytes(
				Uint8Array.from({ length: min + Math.floor(Math.random() * 3) }, () => alphabet[Math.floor(Math.random() * alphabet.length)]),
			);
		const randomBoundary = (): FokosCompositeKey | null => (Math.random() < 0.2 ? null : { hk: randomKey(1), sk: randomKey(0) });
		const routingHashes = [ROUTING_HASH, asRoutingHash(Uint8Array.fromHex("00112233445566fe"))];
		const seen = new Map<string, string>();
		for (let i = 0; i < 5_000; i++) {
			const routingHash = routingHashes[i % 2];
			const start = randomBoundary();
			const end = randomBoundary();
			const { doName, opaque } = PartitionIdHelper.fromRangePartition(base, routingHash, start, end).encode(true);
			const previous = seen.get(doName!);
			expect(previous === undefined || previous === opaque, doName).toBe(true);
			seen.set(doName!, opaque);
		}
	});
});

describe("PartitionIdHelper — hash schema (SCHEMA_HASH_V1)", () => {
	it("fromHashIdxs root: encode then decode", () => {
		const { bytes, opaque, doName } = PartitionIdHelper.fromHashIdxs(base, [0]).encode(true);

		expect(bytes[0]).toBe(PartitionIdHelper.SCHEMA_HASH_V1);
		expect(doName).toBe("iddb~h.0");

		const decoded = PartitionIdHelper.decode(bytes);
		expect(decoded.schema).toBe(0);
		if (decoded.schema === 0) {
			expect(decoded.rootIdx).toBe(0);
			expect(decoded.depth).toBe(0);
		}

		const decoded2 = PartitionIdHelper.decode(Uint8Array.fromHex(opaque));
		expect(decoded2).toEqual(decoded);
	});

	it("fromHashIdxs child: appendHashIdx extends depth", () => {
		const { bytes, doName } = PartitionIdHelper.fromHashIdxs(base, [2]).appendHashIdx(1).encode(true);

		expect(bytes[0]).toBe(PartitionIdHelper.SCHEMA_HASH_V1);
		expect(doName).toBe("iddb~h.2.1");
		expect(PartitionIdHelper.depth(bytes)).toBe(1);
		expect(PartitionIdHelper.lastChildIdx(bytes)).toBe(1);
	});

	it("rootIdx, depth, lastChildIdx assert SCHEMA_HASH_V1", () => {
		const { bytes } = PartitionIdHelper.fromRangePartition(base, ROUTING_HASH, null, null).encode(false);
		expect(() => PartitionIdHelper.rootIdx(bytes)).toThrow();
		expect(() => PartitionIdHelper.depth(bytes)).toThrow();
		expect(() => PartitionIdHelper.lastChildIdx(bytes)).toThrow();
	});

	// The readers are asserted here against hand-written bytes, not against the encoder's output. A
	// round-trip test passes even if the encoder and the readers change the layout together; these
	// literals pin the layout itself: [schema, rootIdx hi, rootIdx lo, depth, ...childIdx].
	it("readers decode hand-written wire bytes", () => {
		expect(PartitionIdHelper.rootIdx(new Uint8Array([0, 0, 0, 0]))).toBe(0);
		expect(PartitionIdHelper.rootIdx(new Uint8Array([0, 0, 42, 0]))).toBe(42);
		expect(PartitionIdHelper.rootIdx(new Uint8Array([0, 1, 0, 0]))).toBe(256);
		// 65000 = 0xFDE8
		expect(PartitionIdHelper.rootIdx(new Uint8Array([0, 0xfd, 0xe8, 0]))).toBe(65000);

		expect(PartitionIdHelper.depth(new Uint8Array([0, 0, 0, 0]))).toBe(0);
		expect(PartitionIdHelper.depth(new Uint8Array([0, 0, 0, 1, 5]))).toBe(1);
		expect(PartitionIdHelper.depth(new Uint8Array([0, 0, 0, 3, 0, 1, 2]))).toBe(3);

		expect(PartitionIdHelper.lastChildIdx(new Uint8Array([0, 0, 0, 1, 5]))).toBe(5);
		expect(PartitionIdHelper.lastChildIdx(new Uint8Array([0, 0, 0, 2, 3, 7]))).toBe(7);
		expect(PartitionIdHelper.lastChildIdx(new Uint8Array([0, 0, 0, 3, 0, 1, 2]))).toBe(2);
	});

	it("doName builds the correct DO name from hand-written wire bytes", () => {
		// Root-only (rootIdx=5, depth=0)
		expect(PartitionIdHelper.doName(base, new Uint8Array([0, 0, 5, 0]))).toBe("iddb~h.5");
		// rootIdx > 255 (rootIdx=256, depth=0) — validates u16 encoding
		expect(PartitionIdHelper.doName(base, new Uint8Array([0, 1, 0, 0]))).toBe("iddb~h.256");
		// With children (rootIdx=5, depth=2, children=[3, 7])
		expect(PartitionIdHelper.doName(base, new Uint8Array([0, 0, 5, 2, 3, 7]))).toBe("iddb~h.5.3.7");
	});

	it("doName and decode throw for unknown schema bytes (>1)", () => {
		const unknownSchema = new Uint8Array([2, 0, 0, 0]);
		expect(() => PartitionIdHelper.doName(base, unknownSchema)).toThrow();
		expect(() => PartitionIdHelper.decode(unknownSchema)).toThrow();
	});
});

describe("rangePartitionDoName", () => {
	function makeName(hashKey: string, start: string | null, end: string | null) {
		return PartitionIdHelper.fromRangePartition(
			base,
			ROUTING_HASH,
			start === null ? null : c(hashKey, start),
			end === null ? null : c(hashKey, end),
		).encode(true).doName!;
	}

	it("produces root name (null start/end → ~min/~max sentinels)", () => {
		expect(makeName("alice", null, null)).toBe(`iddb~r.${ROUTING_HASH_HEX}.~min.~max`);
	});

	it("produces child name with explicit start and end boundaries", () => {
		expect(makeName("alice", "b1", "b2")).toBe(`iddb~r.${ROUTING_HASH_HEX}.alice.b1.alice.b2`);
	});

	it("renders half-bounded edges with one sentinel (leftmost / rightmost child)", () => {
		expect(makeName("alice", null, "m")).toBe(`iddb~r.${ROUTING_HASH_HEX}.~min.alice.m`);
		expect(makeName("alice", "m", null)).toBe(`iddb~r.${ROUTING_HASH_HEX}.alice.m.~max`);
	});

	it("escapes a real boundary that looks like a sentinel (collision-proofness)", () => {
		// A literal "~min" value is escaped (~ → %7E), so it can never collide with the sentinel.
		expect(makeName("~min", "~min", null)).toBe(`iddb~r.${ROUTING_HASH_HEX}.%7Emin.%7Emin.~max`);
	});

	it("percent-encodes dots in the boundaries", () => {
		expect(makeName("a.b", "c.d", "e.f")).toBe(`iddb~r.${ROUTING_HASH_HEX}.a%2Eb.c%2Ed.a%2Eb.e%2Ef`);
	});

	it("leaves slashes literal (0x2F is a safe name byte, not reserved)", () => {
		expect(makeName("a/b", "c/d", "e/f")).toBe(`iddb~r.${ROUTING_HASH_HEX}.a/b.c/d.a/b.e/f`);
	});

	it("keeps range names disjoint from hash names (~r. vs ~h.)", () => {
		const rangeName = makeName("0", null, null);
		expect(rangeName.startsWith("iddb~r.")).toBe(true);
		expect(rangeName).not.toBe("iddb~h.0");
	});
});

describe("encodeRangeComponent", () => {
	const bytes = (...values: number[]) => KeyCodec.asKeyBytes(new Uint8Array(values));
	const enc = (key: string | Uint8Array) => encodeRangeComponent(KeyCodec.encode(key));
	const utf8 = new TextEncoder();

	it("keeps the text of non-English and emoji keys", () => {
		expect(enc("Präsentation März 2024.pptx")).toBe("Präsentation%20März%202024%2Epptx");
		expect(enc("議事録_2024年3月.docx")).toBe("議事録_2024年3月%2Edocx");
		expect(enc("party\u{1F389}")).toBe("party\u{1F389}");
		// The last 3 bytes of a complete 4-byte character are continuation bytes: the tail is empty.
		expect(enc("a\u{1F389}")).toBe("a\u{1F389}");
	});

	it("escapes the bytes of an incomplete character at the end", () => {
		// "é" (C3 A9), "録" (E9 8C B2), "🎉" (F0 9F 8E 89), each cut after each of its bytes.
		const cases: [string, string[]][] = [
			["aé", ["a%C3", "aé"]],
			["議事録", ["議事%E9", "議事%E9%8C", "議事録"]],
			["a🎉", ["a%F0", "a%F0%9F", "a%F0%9F%8E", "a🎉"]],
		];
		for (const [text, names] of cases) {
			const full = utf8.encode(text);
			const cut = full.length - names.length;
			names.forEach((name, i) => expect(encodeRangeComponent(KeyCodec.asKeyBytes(full.subarray(0, cut + i + 1)))).toBe(name));
		}
		expect(encodeRangeComponent(KeyCodec.asKeyBytes(utf8.encode("🎉").subarray(0, 2)))).toBe("%F0%9F");
	});

	it("gives the ~b form for a byte that starts no sequence, and for invalid UTF-8 before the tail", () => {
		expect(encodeRangeComponent(bytes(0x61, 0xf8))).toBe("~bYfg");
		expect(encodeRangeComponent(bytes(0x61, 0x80, 0x62))).toBe("~bYYBi");
		expect(encodeRangeComponent(bytes(0x80, 0x61, 0xe8))).toBe("~bgGHo");
	});

	it("gives the ~b form for all the bytes of a binary key", () => {
		expect(enc(Uint8Array.fromHex("f47ac10b58cc4372a5670e02b2c3d479"))).toBe("~b__R6wQtYzENypWcOArLD1Hk");
		// The tag makes the key binary, also when the other bytes are ASCII.
		expect(enc(utf8.encode("abc"))).toBe("~b_2FiYw");
		expect(encodeRangeComponent(bytes(0xff))).toBe("~b_w");
	});

	it("keeps a leading U+FEFF and escapes it", () => {
		expect(enc("\ufeffa")).toBe("%EF%BB%BFa");
		expect(enc("a\u200bb")).toBe("a%E2%80%8Bb");
	});

	it("escapes the first and the last code point of each range of the escape set", () => {
		const cases: [number, string][] = [
			[0x00, "%00"],
			[0x20, "%20"],
			[0x22, "%22"],
			[0x25, "%25"],
			[0x2e, "%2E"],
			[0x5c, "%5C"],
			[0x7e, "%7E"],
			[0x7f, "%7F"],
			[0x80, "%C2%80"],
			[0x9f, "%C2%9F"],
			[0x200b, "%E2%80%8B"],
			[0x200f, "%E2%80%8F"],
			[0x2028, "%E2%80%A8"],
			[0x2029, "%E2%80%A9"],
			[0x202a, "%E2%80%AA"],
			[0x202e, "%E2%80%AE"],
			[0x2066, "%E2%81%A6"],
			[0x2069, "%E2%81%A9"],
			[0xfeff, "%EF%BB%BF"],
		];
		for (const [cp, name] of cases) {
			expect(enc(`x${String.fromCodePoint(cp)}y`)).toBe(`x${name}y`);
		}
		for (const cp of [0x21, 0x7d, 0xa0, 0x200a, 0x2010, 0x2027, 0x202f, 0x2065, 0x206a, 0xfefe, 0xff00]) {
			expect(enc(`x${String.fromCodePoint(cp)}y`)).toBe(`x${String.fromCodePoint(cp)}y`);
		}
	});

	it("keeps the regular expression and the code unit check of the escape set equal", () => {
		// The regular expression selects the scan, and the code unit check of the scan escapes. A code unit escapes alone only when
		// both forms hold it. A code unit after " " always goes through the scan.
		const inSet = (cu: number) =>
			cu <= 0x20 ||
			cu === 0x22 ||
			cu === 0x25 ||
			cu === 0x2e ||
			cu === 0x5c ||
			(cu >= 0x7e && cu <= 0x9f) ||
			(cu >= 0x200b && cu <= 0x200f) ||
			(cu >= 0x2028 && cu <= 0x202e) ||
			(cu >= 0x2066 && cu <= 0x2069) ||
			cu === 0xfeff;
		const escaped = (text: string) => Array.from(utf8.encode(text), (b) => "%" + b.toString(16).padStart(2, "0").toUpperCase()).join("");
		for (let cu = 0; cu <= 0xffff; cu++) {
			// A decoded key holds a surrogate only in a pair, and no pair is in the escape set.
			const text =
				cu >= 0xd800 && cu <= 0xdbff
					? String.fromCharCode(cu, 0xdc00)
					: cu >= 0xdc00 && cu <= 0xdfff
						? String.fromCharCode(0xd800, cu)
						: String.fromCharCode(cu);
			const expected = inSet(cu) ? escaped(text) : text;
			const alone = enc(text);
			const scanned = enc(" " + text);
			if (alone !== expected || scanned !== "%20" + expected) {
				expect({ cu, alone, scanned }).toEqual({ cu, alone: expected, scanned: "%20" + expected });
			}
		}
	});

	it("keeps each ASCII component of today", () => {
		expect(enc("photos/2024/03/15/IMG_20240315_143")).toBe("photos/2024/03/15/IMG_20240315_143");
		expect(enc("john.doe+")).toBe("john%2Edoe+");
		expect(enc("Q1 2024 F")).toBe("Q1%202024%20F");
		expect(enc('a"b\\c%d~e\x7ff\x01')).toBe("a%22b%5Cc%25d%7Ee%7Ff%01");
	});

	it("never gives a reserved form for a string key", () => {
		expect(enc("~min")).toBe("%7Emin");
		expect(enc("~max")).toBe("%7Emax");
		expect(enc("~bYWJj")).toBe("%7EbYWJj");
	});
});

describe("the shard group separator", () => {
	it("gives different names to a hash partition and a range partition of two shard groups", () => {
		const hash = PartitionIdHelper.fromHashIdxs("x.r.k", [0]).encode(true).doName;
		const range = PartitionIdHelper.fromRangePartition("x", ROUTING_HASH, c("k", "h"), c("k", "0")).encode(true).doName;
		expect(hash).toBe("x.r.k~h.0");
		expect(range).toBe(`x~r.${ROUTING_HASH_HEX}.k.h.k.0`);
	});

	it("validateTopology rejects a shard group with ~ and accepts the internal prefixes", () => {
		const topology = { rootTreesN: 1, hashSplitN: 4 };
		expect(() => validateTopology({ ...topology, shardGroup: "a~b" })).toThrow(
			expect.objectContaining({ code: "partition_context_options_invalid", attributes: { option: "shardGroup", value: "a~b" } }),
		);
		expect(() => validateTopology({ ...topology, shardGroup: "fokos.tc.orders" })).not.toThrow();
		expect(() => validateTopology({ ...topology, shardGroup: "fokos.p.orders" })).not.toThrow();
	});
});

describe("the contexts derived from a route context", () => {
	it("keep the topology, range config, policy and policy version of the source and change only the identity", () => {
		const router = makeRouter();
		const root: FokosRouteContext<{ tier: string }> = router.rootContext(kb("hk"));

		const built = [
			...resolveHashChildPartitionContexts(root),
			resolveDescendantHashPartitionContext(root, [1, 2]),
			resolveRangePartitionContext(root, ROUTING_HASH, null, null),
		];

		for (const ctx of built) {
			expect(ctx.partitionId).not.toBe(root.partitionId);
			expect(ctx.doName).not.toBe(root.doName);
			expect(ctx.topology).toBe(root.topology);
			expect(ctx.rangeConfig).toBe(root.rangeConfig);
			expect(ctx.policy).toBe(root.policy);
			expect(ctx.policyVersion).toBe(root.policyVersion);
			expect(Object.keys(ctx).sort()).toEqual(["doName", "partitionId", "policy", "policyVersion", "rangeConfig", "schema", "topology"]);
		}
		expect(built.slice(0, HASH_SPLIT_N).map((c) => c.doName)).toEqual(
			Array.from({ length: HASH_SPLIT_N }, (_, i) => `${root.doName}.${i}`),
		);
		expect(built[HASH_SPLIT_N].doName).toBe(`${root.doName}.1.2`);
		expect(built[HASH_SPLIT_N + 1].doName).toBe(`${base}~r.${ROUTING_HASH_HEX}.~min.~max`);
	});
});

describe("partitionIdentityFrom", () => {
	it("decodes a hash identity with its root index and child path", () => {
		const root = makeRouter().allRoots()[2];
		const child = resolveDescendantHashPartitionContext(root, [3, 1]);

		const identity = partitionIdentityFrom(child);
		expect(identity).toEqual({
			schema: 1,
			ref: { partitionId: child.partitionId, doName: `${base}~h.2.3.1` },
			kind: "hash",
			// R 4 and K 4: the node 2.3.1 is (2 * 4 + 3) * 4 + 1 = 45 of 64, so its interval is [45, 46) * 2^58.
			hash: {
				rootIndex: 2,
				path: [3, 1],
				depth: 2,
				start: Uint8Array.fromHex("b400000000000000"),
				end: Uint8Array.fromHex("b800000000000000"),
			},
			topology: root.topology,
			routingHashSeed: routingHashSeedOf(base),
		});
		expect(identityDepth(identity)).toBe(2);
		expect(identityDepth(partitionIdentityFrom(root))).toBe(0);
	});

	it("rejects a hash identity whose DO name does not match its partition ID", () => {
		const root = makeRouter().allRoots()[2];
		expect(() => partitionIdentityFrom({ ...root, doName: `${base}~h.3` })).toThrow(
			invariantFailure(/the DO name does not match the partition ID/),
		);
	});

	it("decodes a range identity and takes the depth and ancestors from fokosInit", () => {
		const root = makeRouter().allRoots()[0];
		const range = resolveRangePartitionContext(root, ROUTING_HASH, c("alice", "b1"), null);
		const ancestors = [{ depth: 0, startBoundary: c("alice", "a"), endBoundary: c("alice", "z") }];

		const identity = partitionIdentityFrom(range, { depth: 1, ancestors });
		expect(identity.kind).toBe("range");
		expect(identity.hash).toBeUndefined();
		expect(identity.range).toEqual({ routingHash: ROUTING_HASH, start: c("alice", "b1"), end: null, depth: 1, ancestors });
		expect(identity.routingHashSeed).toBe(routingHashSeedOf(base));
		expect(identityDepth(identity)).toBe(1);
		expect(() => partitionIdentityFrom(range)).toThrow(invariantFailure(/needs its depth and ancestors/));
	});
});
