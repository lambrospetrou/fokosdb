import { describe, expect, it } from "vitest";
import { PartitionContextCreator, type PartitionContext } from "./partition-context.js";
import { PartitionIdHelper, rangePartitionDoName } from "./partition-id.js";

function makeBase(): PartitionContext {
	return PartitionContextCreator.create({
		ns: "PARTITION_DO",
		databaseName: "iddb",
		rootTreesN: 4,
		hashSplitN: 4,
		hashSplitConditions: { maxSizeMb: 100 },
	});
}

describe("PartitionIdHelper — hash codec round-trips", () => {
	it("encodes a root (depth 0) and reads it back", () => {
		const base = makeBase();
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
		const base = makeBase();
		const { bytes, doName } = PartitionIdHelper.fromHashIdxs(base, [4097]).encode(true);
		expect(doName).toBe("iddb.h.4097");
		expect(PartitionIdHelper.rootIdx(bytes)).toBe(4097);
	});

	it("fromHashIdxs with child indexes sets depth and lastChildIdx", () => {
		const base = makeBase();
		const { bytes, doName } = PartitionIdHelper.fromHashIdxs(base, [1, 2, 0]).encode(true);
		expect(doName).toBe("iddb.h.1.2.0");
		expect(PartitionIdHelper.rootIdx(bytes)).toBe(1);
		expect(PartitionIdHelper.depth(bytes)).toBe(2);
		expect(PartitionIdHelper.lastChildIdx(bytes)).toBe(0);
	});

	it("appendHashIdx on existing bytes extends the depth (single and array forms)", () => {
		const base = makeBase();
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
		const base = makeBase();
		const root = PartitionIdHelper.fromHashIdxs(base, [2]).encode(false);
		const fromHex = new PartitionIdHelper(base, root.opaque).appendHashIdx(1).encode(true);
		const fromBytes = new PartitionIdHelper(base, root.bytes).appendHashIdx(1).encode(true);
		expect(fromHex.opaque).toBe(fromBytes.opaque);
		expect(fromHex.doName).toBe(fromBytes.doName);
	});

	it("encode throws with nothing to encode and when appending to a range ID", () => {
		const base = makeBase();
		expect(() => new PartitionIdHelper(base).encode(false)).toThrow(/no bytes or appended hash indexes/);
		const range = PartitionIdHelper.fromRangePartition(base, "k", null, null).encode(false);
		expect(() => new PartitionIdHelper(base, range.bytes).appendHashIdx(1).encode(false)).toThrow(/cannot append hash indexes/);
	});

	it("calculateHashChildPartitionIds produces hashSplitN distinct children one level deeper", () => {
		const base = makeBase();
		const parent = PartitionIdHelper.fromHashIdxs(base, [1]).encode(true);
		const children = PartitionIdHelper.calculateHashChildPartitionIds({
			...base,
			doName: parent.doName!,
			primaryDoIdStr: "",
			partitionId: parent.opaque,
		});
		expect(children).toHaveLength(base.hashSplitN);
		expect(new Set(children.map((c) => c.doName)).size).toBe(base.hashSplitN);
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
		const base = makeBase();
		for (const [start, end] of [
			[null, null],
			[null, "m"],
			["m", null],
			["b1", "b2"],
		] as const) {
			const { bytes, opaque } = PartitionIdHelper.fromRangePartition(base, "alice", start, end).encode(false);
			expect(PartitionIdHelper.isRangePartition(opaque)).toBe(true);
			const decoded = PartitionIdHelper.decode(bytes);
			expect(decoded).toEqual({ schema: 1, hashKey: "alice", startBoundary: start, endBoundary: end });
		}
	});

	it("doName formats range IDs via rangePartitionDoName", () => {
		const base = makeBase();
		const { bytes } = PartitionIdHelper.fromRangePartition(base, "alice", "b1", null).encode(false);
		expect(PartitionIdHelper.doName(base, bytes)).toBe(rangePartitionDoName("iddb", "alice", "b1", null));
		expect(PartitionIdHelper.doName(base, bytes)).toBe("iddb.r.alice.b1.~max");
	});

	it("hash-only readers reject range IDs", () => {
		const base = makeBase();
		const { bytes } = PartitionIdHelper.fromRangePartition(base, "k", null, null).encode(false);
		expect(() => PartitionIdHelper.rootIdx(bytes)).toThrow(/expected hash schema/);
		expect(() => PartitionIdHelper.depth(bytes)).toThrow(/expected hash schema/);
		expect(() => PartitionIdHelper.lastChildIdx(bytes)).toThrow(/expected hash schema/);
	});
});
