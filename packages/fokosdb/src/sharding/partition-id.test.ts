import { describe, expect, it } from "vitest";
import { KeyCodec, type KeyBytes } from "./key-codec.js";
import {
	identityDepth,
	partitionIdentityFrom,
	PartitionIdHelper,
	resolveDescendantHashPartitionContext,
	resolveHashChildPartitionContexts,
	resolveRangePartitionContext,
} from "./partition-id.js";
import { FokosRouter } from "./router.js";
import type { FokosRouteContext } from "./route-context.js";
import { invariantFailure } from "../../test/errors-matchers.js";

const kb = (s: string) => KeyCodec.encode(s);

const base = "iddb";
const HASH_SPLIT_N = 4;

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
		expect(doName).toBe("iddb.h.3");
		expect(PartitionIdHelper.rootIdx(bytes)).toBe(3);
		expect(PartitionIdHelper.depth(bytes)).toBe(0);
		expect(PartitionIdHelper.isHashPartition(opaque)).toBe(true);
		expect(PartitionIdHelper.isRangePartition(opaque)).toBe(false);
		const decoded = PartitionIdHelper.decode(Uint8Array.fromHex(opaque));
		expect(decoded).toEqual({ schema: 0, rootIdx: 3, depth: 0 });
	});

	it("encodes a u16 root index (> 255) correctly", () => {
		const { bytes, doName } = PartitionIdHelper.fromHashIdxs(base, [4097]).encode(true);
		expect(doName).toBe("iddb.h.4097");
		expect(PartitionIdHelper.rootIdx(bytes)).toBe(4097);
	});

	it("fromHashIdxs with child indexes sets depth and lastChildIdx", () => {
		const { bytes, doName } = PartitionIdHelper.fromHashIdxs(base, [1, 2, 0]).encode(true);
		expect(doName).toBe("iddb.h.1.2.0");
		expect(PartitionIdHelper.rootIdx(bytes)).toBe(1);
		expect(PartitionIdHelper.depth(bytes)).toBe(2);
		expect(PartitionIdHelper.lastChildIdx(bytes)).toBe(0);
	});

	it("appendHashIdx on existing bytes extends the depth (single and array forms)", () => {
		const root = PartitionIdHelper.fromHashIdxs(base, [0]).encode(false);

		const single = new PartitionIdHelper(base, root.bytes).appendHashIdx(1).encode(true);
		expect(single.doName).toBe("iddb.h.0.1");
		expect(PartitionIdHelper.depth(single.bytes)).toBe(1);
		expect(PartitionIdHelper.lastChildIdx(single.bytes)).toBe(1);

		const multi = new PartitionIdHelper(base, root.bytes).appendHashIdx([1, 3]).encode(true);
		expect(multi.doName).toBe("iddb.h.0.1.3");
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
		const range = PartitionIdHelper.fromRangePartition(base, kb("k"), null, null).encode(false);
		expect(() => new PartitionIdHelper(base, range.bytes).appendHashIdx(1).encode(false)).toThrow(
			invariantFailure(/cannot append hash indexes/),
		);
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
			expect(children[i].doName).toBe(`iddb.h.1.${i}`);
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
			[null, "m"],
			["m", null],
			["b1", "b2"],
		] as const) {
			const startKb = start === null ? null : kb(start);
			const endKb = end === null ? null : kb(end);
			const { bytes, opaque } = PartitionIdHelper.fromRangePartition(base, kb("alice"), startKb, endKb).encode(false);
			expect(PartitionIdHelper.isRangePartition(opaque)).toBe(true);
			const decoded = PartitionIdHelper.decode(bytes);
			expect(decoded).toEqual({ schema: 1, hashKey: kb("alice"), startBoundary: startKb, endBoundary: endKb });
		}
	});

	it("doName formats range IDs via rangePartitionDoName", () => {
		const { bytes, doName } = PartitionIdHelper.fromRangePartition(base, kb("alice"), kb("b1"), null).encode(true);
		expect(doName).toBe("iddb.r.alice.b1.~max");
		expect(PartitionIdHelper.doName(base, bytes)).toBe("iddb.r.alice.b1.~max");
	});

	it("hash-only readers reject range IDs", () => {
		const { bytes } = PartitionIdHelper.fromRangePartition(base, kb("k"), null, null).encode(false);
		expect(() => PartitionIdHelper.rootIdx(bytes)).toThrow(invariantFailure(/expected hash schema/));
		expect(() => PartitionIdHelper.depth(bytes)).toThrow(invariantFailure(/expected hash schema/));
		expect(() => PartitionIdHelper.lastChildIdx(bytes)).toThrow(invariantFailure(/expected hash schema/));
	});
});

describe("PartitionIdHelper — range schema (SCHEMA_RANGE_V1)", () => {
	it("fromRangePartition root: encode then decode round-trips (both boundaries null)", () => {
		const helper = PartitionIdHelper.fromRangePartition(base, kb("alice"), null, null);
		const { bytes, opaque, doName } = helper.encode(true);

		expect(bytes[0]).toBe(PartitionIdHelper.SCHEMA_RANGE_V1);
		expect(doName).toBe("iddb.r.alice.~min.~max");

		const decoded = PartitionIdHelper.decode(bytes);
		expect(decoded.schema).toBe(1);
		if (decoded.schema === 1) {
			expect(decoded.hashKey).toEqual(kb("alice"));
			expect(decoded.startBoundary).toBeNull();
			expect(decoded.endBoundary).toBeNull();
		}

		// Opaque round-trip.
		expect(opaque).toMatch(/^01[A-Za-z0-9_-]+\.$/);
		const decoded2 = PartitionIdHelper.decode(PartitionIdHelper.partitionIdToBytes(opaque));
		expect(decoded2).toEqual(decoded);
	});

	it("fromRangePartition child: encode then decode round-trips with both boundaries", () => {
		const helper = PartitionIdHelper.fromRangePartition(base, kb("alice"), kb("b1"), kb("b2"));
		const { bytes, doName } = helper.encode(true);

		expect(bytes[0]).toBe(PartitionIdHelper.SCHEMA_RANGE_V1);
		expect(doName).toBe("iddb.r.alice.b1.b2");

		const decoded = PartitionIdHelper.decode(bytes);
		expect(decoded.schema).toBe(1);
		if (decoded.schema === 1) {
			expect(decoded.hashKey).toEqual(kb("alice"));
			expect(decoded.startBoundary).toEqual(kb("b1"));
			expect(decoded.endBoundary).toEqual(kb("b2"));
		}
	});

	it("round-trips half-bounded edges (leftmost: null start; rightmost: null end)", () => {
		for (const [start, end, name] of [
			[null, kb("m"), "iddb.r.x.~min.m"],
			[kb("m"), null, "iddb.r.x.m.~max"],
		] as const) {
			const { bytes, doName } = PartitionIdHelper.fromRangePartition(base, kb("x"), start, end).encode(true);
			expect(doName).toBe(name);
			const decoded = PartitionIdHelper.decode(bytes);
			expect(decoded.schema).toBe(1);
			if (decoded.schema === 1) {
				expect(decoded.startBoundary).toEqual(start);
				expect(decoded.endBoundary).toEqual(end);
			}
		}
	});

	it("handles unicode in hashKey and boundaries", () => {
		const { bytes } = PartitionIdHelper.fromRangePartition(base, kb("café☕"), kb("töst"), kb("zünd")).encode(false);
		const decoded = PartitionIdHelper.decode(bytes);
		expect(decoded.schema).toBe(1);
		if (decoded.schema === 1) {
			expect(decoded.hashKey).toEqual(kb("café☕"));
			expect(decoded.startBoundary).toEqual(kb("töst"));
			expect(decoded.endBoundary).toEqual(kb("zünd"));
		}
	});

	it("doName dispatches correctly for range ID loaded from its opaque ID", () => {
		const { opaque } = PartitionIdHelper.fromRangePartition(base, kb("mykey"), kb("start1"), kb("end1")).encode(false);
		const bytes = PartitionIdHelper.partitionIdToBytes(opaque);
		expect(PartitionIdHelper.doName(base, bytes)).toBe("iddb.r.mykey.start1.end1");
	});
});

describe("PartitionIdHelper — range ID base64url parts", () => {
	const bin = (...values: number[]) => KeyCodec.asKeyBytes(new Uint8Array(values));
	const cases: [string, KeyBytes, KeyBytes | null, KeyBytes | null][] = [
		["root", kb("alice"), null, null],
		["null start", kb("alice"), null, kb("m")],
		["null end", kb("alice"), kb("m"), null],
		["two bounds", kb("alice"), kb("b1"), kb("b2")],
		["unicode", kb("café☕"), kb("töst"), kb("zünd")],
		["reserved DO-name bytes", kb("a.b%c~d"), kb('x"y\\z'), kb("~max")],
		["binary", KeyCodec.encode(new Uint8Array([0, 0xff, 0x2e])), bin(0xff, 0x00, 0x7e), bin(0xff, 0xfe)],
		["empty boundaries", kb("k"), bin(), bin()],
	];

	it.each(cases)("round-trips byte for byte: %s", (_, hashKey, start, end) => {
		const ctx = resolveRangePartitionContext(makeRouter().allRoots()[0], hashKey, start, end);
		const { bytes, opaque, doName } = PartitionIdHelper.fromRangePartition(base, hashKey, start, end).encode(true);
		expect(ctx.partitionId).toBe(opaque);
		expect(ctx.doName).toBe(doName);
		expect(opaque).toMatch(/^01[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*$/);
		expect(PartitionIdHelper.partitionIdToBytes(opaque)).toEqual(bytes);
		expect(PartitionIdHelper.decode(bytes)).toEqual({ schema: 1, hashKey, startBoundary: start, endBoundary: end });
		expect(PartitionIdHelper.rangeHashKey(opaque)).toEqual(hashKey);
		// The name from the keys equals the name from a full ID decode.
		expect(PartitionIdHelper.doName(base, PartitionIdHelper.partitionIdToBytes(opaque))).toBe(ctx.doName);
		expect(new PartitionIdHelper(base, opaque).encode(true)).toEqual({ bytes, opaque, doName });
	});

	it("keeps the flat byte layout and puts the boundaries in the second part only", () => {
		const { bytes, opaque } = PartitionIdHelper.fromRangePartition(base, kb("hk"), kb("s"), kb("e")).encode(false);
		const hk = kb("hk");
		const s = kb("s");
		const e = kb("e");
		const first = new Uint8Array([0x03, hk.length, 0, 0, 0, s.length, 0, 0, 0, ...hk]);
		const second = new Uint8Array([...s, ...e]);
		expect(bytes).toEqual(new Uint8Array([0x01, ...first, ...second]));
		expect(opaque).toBe(
			"01" +
				first.toBase64({ alphabet: "base64url", omitPadding: true }) +
				"." +
				second.toBase64({ alphabet: "base64url", omitPadding: true }),
		);
	});

	it("reads the hash key without the boundary part", () => {
		const { opaque } = PartitionIdHelper.fromRangePartition(base, kb("alice"), kb("b1"), kb("b2")).encode(false);
		const firstOnly = opaque.slice(0, opaque.indexOf(".") + 1) + "!!!";
		expect(PartitionIdHelper.rangeHashKey(firstOnly)).toEqual(kb("alice"));
		expect(() => PartitionIdHelper.partitionIdToBytes(firstOnly)).toThrow(invariantFailure(/not canonical base64url/));
	});

	it("rejects malformed separators and non-canonical base64url", () => {
		const { opaque } = PartitionIdHelper.fromRangePartition(base, kb("alice"), kb("b1"), kb("b2")).encode(false);
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
		// flags, hkLen u32 LE, startLen u32 LE, hash key
		expect(decode([0x03, 1, 0, 0, 0, 1, 0, 0, 0, 0x6b], [0x61, 0x62])).not.toThrow();
		expect(decode([0x04, 1, 0, 0, 0, 0, 0, 0, 0, 0x6b], [])).toThrow(invariantFailure(/invalid range partition ID flags/));
		expect(decode([0x00, 2, 0, 0, 0, 0, 0, 0, 0, 0x6b], [])).toThrow(invariantFailure(/hash-key length/));
		expect(decode([0x00, 1, 0, 0, 0, 0, 0, 0], [])).toThrow(invariantFailure(/too short/));
		expect(decode([0x00, 1, 0, 0, 0, 1, 0, 0, 0, 0x6b], [0x61])).toThrow(invariantFailure(/without a start/));
		expect(decode([0x01, 1, 0, 0, 0, 2, 0, 0, 0, 0x6b], [0x61])).toThrow(invariantFailure(/start length/));
		expect(decode([0x01, 1, 0, 0, 0, 1, 0, 0, 0, 0x6b], [0x61, 0x62])).toThrow(invariantFailure(/without an end/));
		expect(decode([0x00, 1, 0, 0, 0, 0, 0, 0, 0, 0x6b], [0x61])).toThrow(invariantFailure(/without an end/));
		expect(() => PartitionIdHelper.rangeHashKey("01" + b64(0x08, 1, 0, 0, 0, 0, 0, 0, 0, 0x6b) + ".")).toThrow(
			invariantFailure(/invalid range partition ID flags/),
		);
	});

	it("partitionIdentityFrom decodes both parts of a range ID", () => {
		const ctx = resolveRangePartitionContext(makeRouter().allRoots()[0], kb("alice"), kb("b1"), null);
		const identity = partitionIdentityFrom(ctx, { depth: 1, ancestors: [] });
		expect(identity.range).toEqual({ hashKey: kb("alice"), start: kb("b1"), end: null, depth: 1, ancestors: [] });
	});
});

describe("PartitionIdHelper — hash schema (SCHEMA_HASH_V1)", () => {
	it("fromHashIdxs root: encode then decode", () => {
		const { bytes, opaque, doName } = PartitionIdHelper.fromHashIdxs(base, [0]).encode(true);

		expect(bytes[0]).toBe(PartitionIdHelper.SCHEMA_HASH_V1);
		expect(doName).toBe("iddb.h.0");

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
		expect(doName).toBe("iddb.h.2.1");
		expect(PartitionIdHelper.depth(bytes)).toBe(1);
		expect(PartitionIdHelper.lastChildIdx(bytes)).toBe(1);
	});

	it("rootIdx, depth, lastChildIdx assert SCHEMA_HASH_V1", () => {
		const { bytes } = PartitionIdHelper.fromRangePartition(base, kb("k"), null, null).encode(false);
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
		expect(PartitionIdHelper.doName(base, new Uint8Array([0, 0, 5, 0]))).toBe("iddb.h.5");
		// rootIdx > 255 (rootIdx=256, depth=0) — validates u16 encoding
		expect(PartitionIdHelper.doName(base, new Uint8Array([0, 1, 0, 0]))).toBe("iddb.h.256");
		// With children (rootIdx=5, depth=2, children=[3, 7])
		expect(PartitionIdHelper.doName(base, new Uint8Array([0, 0, 5, 2, 3, 7]))).toBe("iddb.h.5.3.7");
	});

	it("doName and decode throw for unknown schema bytes (>1)", () => {
		const unknownSchema = new Uint8Array([2, 0, 0, 0]);
		expect(() => PartitionIdHelper.doName(base, unknownSchema)).toThrow();
		expect(() => PartitionIdHelper.decode(unknownSchema)).toThrow();
	});
});

describe("rangePartitionDoName", () => {
	function makeName(hashKey: string, start: string | null, end: string | null) {
		return PartitionIdHelper.fromRangePartition(base, kb(hashKey), start === null ? null : kb(start), end === null ? null : kb(end)).encode(
			true,
		).doName!;
	}

	it("produces root name (null start/end → ~min/~max sentinels)", () => {
		expect(makeName("alice", null, null)).toBe("iddb.r.alice.~min.~max");
	});

	it("produces child name with explicit start and end boundaries", () => {
		expect(makeName("alice", "b1", "b2")).toBe("iddb.r.alice.b1.b2");
	});

	it("renders half-bounded edges with one sentinel (leftmost / rightmost child)", () => {
		expect(makeName("alice", null, "m")).toBe("iddb.r.alice.~min.m");
		expect(makeName("alice", "m", null)).toBe("iddb.r.alice.m.~max");
	});

	it("escapes a real boundary that looks like a sentinel (collision-proofness)", () => {
		// A literal "~min" boundary value is escaped (~ → %7E), so it can never collide with the sentinel.
		expect(makeName("k", "~min", null)).toBe("iddb.r.k.%7Emin.~max");
	});

	it("percent-encodes dots in hashKey and boundaries", () => {
		expect(makeName("a.b", "c.d", "e.f")).toBe("iddb.r.a%2Eb.c%2Ed.e%2Ef");
	});

	it("leaves slashes literal (0x2F is a safe name byte, not reserved)", () => {
		expect(makeName("a/b", "c/d", "e/f")).toBe("iddb.r.a/b.c/d.e/f");
	});

	it("leaves [A-Za-z0-9_-] unchanged", () => {
		expect(makeName("Hello_World-123", "sk_value-99", "sk_value-zz")).toBe("iddb.r.Hello_World-123.sk_value-99.sk_value-zz");
	});

	it("keeps range names disjoint from hash names (.r. vs .h.)", () => {
		const rangeName = makeName("0", null, null);
		expect(rangeName).toBe("iddb.r.0.~min.~max");
		// Hash root 0 is "iddb.h.0" — no collision.
		expect(rangeName).not.toBe("iddb.h.0");
	});
});

describe("the contexts derived from a route context", () => {
	it("keep the topology, range config and policy of the source and change only the identity", () => {
		const router = makeRouter();
		const root: FokosRouteContext<{ tier: string }> = router.rootContext(kb("hk"));
		const bytes = Uint8Array.fromHex(root.partitionId);

		const built = [
			...resolveHashChildPartitionContexts(root),
			resolveDescendantHashPartitionContext(root, bytes, [1, 2]),
			resolveRangePartitionContext(root, kb("hk"), null, null),
		];

		for (const ctx of built) {
			expect(ctx.partitionId).not.toBe(root.partitionId);
			expect(ctx.doName).not.toBe(root.doName);
			expect(ctx.topology).toBe(root.topology);
			expect(ctx.rangeConfig).toBe(root.rangeConfig);
			expect(ctx.policy).toBe(root.policy);
			expect(Object.keys(ctx).sort()).toEqual(["doName", "partitionId", "policy", "rangeConfig", "schema", "topology"]);
		}
		expect(built.slice(0, HASH_SPLIT_N).map((c) => c.doName)).toEqual(
			Array.from({ length: HASH_SPLIT_N }, (_, i) => `${root.doName}.${i}`),
		);
		expect(built[HASH_SPLIT_N].doName).toBe(`${root.doName}.1.2`);
		expect(built[HASH_SPLIT_N + 1].doName).toBe(`${base}.r.hk.~min.~max`);
	});
});

describe("partitionIdentityFrom", () => {
	it("decodes a hash identity with its root index and child path", () => {
		const root = makeRouter().allRoots()[2];
		const child = resolveDescendantHashPartitionContext(root, Uint8Array.fromHex(root.partitionId), [3, 1]);

		const identity = partitionIdentityFrom(child);
		expect(identity).toEqual({
			schema: 1,
			ref: { partitionId: child.partitionId, doName: `${base}.h.2.3.1` },
			kind: "hash",
			hash: { rootIndex: 2, path: [3, 1] },
			topology: root.topology,
		});
		expect(identityDepth(identity)).toBe(2);
		expect(identityDepth(partitionIdentityFrom(root))).toBe(0);
	});

	it("decodes a range identity and takes the depth and ancestors from fokosInit", () => {
		const root = makeRouter().allRoots()[0];
		const range = resolveRangePartitionContext(root, kb("alice"), kb("b1"), null);
		const ancestors = [{ depth: 0, startBoundary: kb("a"), endBoundary: kb("z") }];

		const identity = partitionIdentityFrom(range, { depth: 1, ancestors });
		expect(identity.kind).toBe("range");
		expect(identity.hash).toBeUndefined();
		expect(identity.range).toEqual({ hashKey: kb("alice"), start: kb("b1"), end: null, depth: 1, ancestors });
		expect(identityDepth(identity)).toBe(1);
		expect(() => partitionIdentityFrom(range)).toThrow(invariantFailure(/needs its depth and ancestors/));
	});
});
