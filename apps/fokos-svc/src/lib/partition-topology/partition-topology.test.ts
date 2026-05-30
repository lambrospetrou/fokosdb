import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionContextCreator, PartitionIdHelper, rangePartitionDoName } from "./partition-topology.js";
import type { PartitionContext } from "./partition-topology.js";
import type { PartitionDO } from "../do-partition.js";

function makeBase(): PartitionContext {
	return PartitionContextCreator.create({
		ns: "PARTITION_DO",
		databaseName: "testdb",
		rootTreesN: 1,
		hashSplitN: 4,
		hashSplitConditions: { maxSizeMb: 100 },
	});
}

describe("rangePartitionDoName", () => {
	it("produces root name (null startBoundary)", () => {
		expect(rangePartitionDoName("mydb", "alice", null)).toBe("mydb.r.alice.");
	});

	it("produces child name with sortKey boundary", () => {
		expect(rangePartitionDoName("mydb", "alice", "b1")).toBe("mydb.r.alice.b1");
	});

	it("percent-encodes dots in hashKey and boundary", () => {
		expect(rangePartitionDoName("mydb", "a.b", "c.d")).toBe("mydb.r.a%2Eb.c%2Ed");
	});

	it("percent-encodes slashes", () => {
		expect(rangePartitionDoName("mydb", "a/b", "c/d")).toBe("mydb.r.a%2Fb.c%2Fd");
	});

	it("leaves [A-Za-z0-9_-] unchanged", () => {
		expect(rangePartitionDoName("db", "Hello_World-123", "sk_value-99")).toBe("db.r.Hello_World-123.sk_value-99");
	});

	it("keeps range names disjoint from hash names (.r. vs .h.)", () => {
		const rangeName = rangePartitionDoName("db", "0", null);
		expect(rangeName).toBe("db.r.0.");
		// Hash root 0 is "db.h.0" — no collision.
		expect(rangeName).not.toBe("db.h.0");
	});
});

describe("PartitionIdHelper — range schema (SCHEMA_RANGE_V1)", () => {
	it("fromRangePartition root: encode then decode round-trips", () => {
		const base = makeBase();
		const helper = PartitionIdHelper.fromRangePartition(base, "alice", null);
		const { bytes, opaque, doName } = helper.encode(true);

		expect(bytes[0]).toBe(PartitionIdHelper.SCHEMA_RANGE_V1);
		expect(doName).toBe("testdb.r.alice.");

		const decoded = PartitionIdHelper.decode(bytes);
		expect(decoded.schema).toBe(1);
		if (decoded.schema === 1) {
			expect(decoded.hashKey).toBe("alice");
			expect(decoded.startBoundary).toBeNull();
		}

		// Opaque round-trip.
		const decoded2 = PartitionIdHelper.decode(Uint8Array.fromHex(opaque));
		expect(decoded2).toEqual(decoded);
	});

	it("fromRangePartition child: encode then decode round-trips with boundary", () => {
		const base = makeBase();
		const helper = PartitionIdHelper.fromRangePartition(base, "alice", "b1");
		const { bytes, doName } = helper.encode(true);

		expect(bytes[0]).toBe(PartitionIdHelper.SCHEMA_RANGE_V1);
		expect(doName).toBe("testdb.r.alice.b1");

		const decoded = PartitionIdHelper.decode(bytes);
		expect(decoded.schema).toBe(1);
		if (decoded.schema === 1) {
			expect(decoded.hashKey).toBe("alice");
			expect(decoded.startBoundary).toBe("b1");
		}
	});

	it("handles unicode in hashKey and startBoundary", () => {
		const base = makeBase();
		const { bytes } = PartitionIdHelper.fromRangePartition(base, "café☕", "töst").encode(false);
		const decoded = PartitionIdHelper.decode(bytes);
		expect(decoded.schema).toBe(1);
		if (decoded.schema === 1) {
			expect(decoded.hashKey).toBe("café☕");
			expect(decoded.startBoundary).toBe("töst");
		}
	});

	it("doName dispatches correctly for range ID loaded from opaque hex", () => {
		const base = makeBase();
		const { opaque } = PartitionIdHelper.fromRangePartition(base, "mykey", "start1").encode(false);
		const bytes = Uint8Array.fromHex(opaque);
		expect(PartitionIdHelper.doName(base, bytes)).toBe("testdb.r.mykey.start1");
	});
});

describe("PartitionIdHelper — hash schema (SCHEMA_HASH_V1) unchanged", () => {
	it("fromHashIdxs root: encode then decode", () => {
		const base = makeBase();
		const { bytes, opaque, doName } = PartitionIdHelper.fromHashIdxs(base, [0]).encode(true);

		expect(bytes[0]).toBe(PartitionIdHelper.SCHEMA_HASH_V1);
		expect(doName).toBe("testdb.h.0");

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
		const base = makeBase();
		const { bytes, doName } = PartitionIdHelper.fromHashIdxs(base, [2]).appendHashIdx(1).encode(true);

		expect(bytes[0]).toBe(PartitionIdHelper.SCHEMA_HASH_V1);
		expect(doName).toBe("testdb.h.2.1");
		expect(PartitionIdHelper.depth(bytes)).toBe(1);
		expect(PartitionIdHelper.lastChildIdx(bytes)).toBe(1);
	});

	it("rootIdx, depth, lastChildIdx assert SCHEMA_HASH_V1", () => {
		const base = makeBase();
		const { bytes } = PartitionIdHelper.fromRangePartition(base, "k", null).encode(false);
		expect(() => PartitionIdHelper.rootIdx(bytes)).toThrow();
		expect(() => PartitionIdHelper.depth(bytes)).toThrow();
		expect(() => PartitionIdHelper.lastChildIdx(bytes)).toThrow();
	});
});
