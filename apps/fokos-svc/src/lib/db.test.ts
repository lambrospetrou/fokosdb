import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { FokosDB } from "./db.js";
import { PartitionContextCreator, type PartitionNamespaceKey } from "./partition-topology/partition-context.js";
import { PartitionTopologyRouterImpl } from "./partition-topology/router.js";

// Run the whole suite against every partition DO namespace so a divergence in a customer-provided
// class (e.g. CUSTOM_PARTITION_DO) is caught as a regression. makeDB is the only namespace-coupled
// point, so binding it once per case via closure keeps every test body untouched.
describe.each(["PARTITION_DO", "CUSTOM_PARTITION_DO"] as const)("FokosDB over %s", (ns) => {
	const makeDB = () => makeDBFor(ns);

	describe("FokosDB.queryItems — multi sub-query fan-out", () => {
		it("groups results per sub-query in request order, sk-ordered within each group", async () => {
			const db = makeDB();
			for (const sk of ["a3", "a1", "a2"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
			for (const sk of ["b2", "b1"]) await db.putItem({ hashKey: "bob", sortKey: sk, data: "x" });

			const res = await db.queryItems({ queries: [{ hashKey: "alice" }, { hashKey: "bob" }] });

			// alice's group (sorted) precedes bob's group (sorted) — list order across groups, sk order within.
			expect(sksOf(res)).toEqual(["a1", "a2", "a3", "b1", "b2"]);
			expect(res.count).toBe(5);
			expect(res.cursor).toBeUndefined();
			// One leaf scan per sub-query (both route to the same single root DO, listed once per RPC).
			expect(res.partitionMetas).toHaveLength(2);
			expect(res.meta.rowsReturned).toBe(5);
		});

		it("reverses both the group contents and applies sk DESC within each group", async () => {
			const db = makeDB();
			for (const sk of ["a1", "a2"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
			for (const sk of ["b1", "b2"]) await db.putItem({ hashKey: "bob", sortKey: sk, data: "x" });

			const res = await db.queryItems({
				queries: [
					{ hashKey: "alice", scanIndexForward: false },
					{ hashKey: "bob", scanIndexForward: false },
				],
			});

			// Groups stay in request order; only sk order within each group flips.
			expect(sksOf(res)).toEqual(["a2", "a1", "b2", "b1"]);
		});

		it("supports mixed directions: one sub-query ascending, another descending", async () => {
			const db = makeDB();
			for (const sk of ["a1", "a2", "a3"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
			for (const sk of ["b1", "b2", "b3"]) await db.putItem({ hashKey: "bob", sortKey: sk, data: "x" });

			const res = await db.queryItems({
				queries: [
					{ hashKey: "alice", scanIndexForward: true },
					{ hashKey: "bob", scanIndexForward: false },
				],
			});

			expect(sksOf(res)).toEqual(["a1", "a2", "a3", "b3", "b2", "b1"]);
		});

		it("allows duplicate hash keys → two consecutive groups (union of disjoint ranges)", async () => {
			const db = makeDB();
			for (const sk of ["s1", "s2", "s3", "s4"]) await db.putItem({ hashKey: "k", sortKey: sk, data: "x" });

			const res = await db.queryItems({
				queries: [
					{ hashKey: "k", sort: { op: "lte", value: "s2" } },
					{ hashKey: "k", sort: { op: "gte", value: "s3" } },
				],
			});

			expect(sksOf(res)).toEqual(["s1", "s2", "s3", "s4"]);
		});

		it("skips an empty-interval sub-query but keeps the others in list order", async () => {
			const db = makeDB();
			for (const sk of ["a1", "a2"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
			for (const sk of ["b1"]) await db.putItem({ hashKey: "bob", sortKey: sk, data: "x" });

			const res = await db.queryItems({
				queries: [
					{ hashKey: "alice" },
					{ hashKey: "zzz", sort: { op: "between", lower: "z9", upper: "z1" } }, // lower > upper → empty
					{ hashKey: "bob" },
				],
			});

			expect(sksOf(res)).toEqual(["a1", "a2", "b1"]);
			expect(res.cursor).toBeUndefined();
		});

		it("paginates across sub-queries with a global limit, resuming without gaps or duplicates", async () => {
			const db = makeDB();
			const aliceSks = ["a1", "a2", "a3"];
			const bobSks = ["b1", "b2", "b3"];
			for (const sk of aliceSks) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
			for (const sk of bobSks) await db.putItem({ hashKey: "bob", sortKey: sk, data: "x" });

			const queries = [{ hashKey: "alice" }, { hashKey: "bob" }];
			const got: Array<string | Uint8Array | undefined> = [];
			let cursor: string | undefined;
			let pages = 0;
			for (;;) {
				const res = await db.queryItems({ queries, limit: 2, cursor });
				got.push(...sksOf(res));
				pages++;
				if (res.cursor === undefined) break;
				cursor = res.cursor;
				expect(pages).toBeLessThan(50);
			}

			expect(got).toEqual([...aliceSks, ...bobSks]);
			expect(pages).toBeGreaterThan(1); // genuinely multi-page across the sub-query boundary
			expect(new Set(got.map(String)).size).toBe(got.length); // no duplicates
		});

		it("paginates across the sub-query boundary under a tight byte budget", async () => {
			const db = makeDB();
			const big = "x".repeat(20 * 1024);
			const aliceSks = ["a1", "a2", "a3"];
			const bobSks = ["b1", "b2"];
			for (const sk of aliceSks) await db.putItem({ hashKey: "alice", sortKey: sk, data: big });
			for (const sk of bobSks) await db.putItem({ hashKey: "bob", sortKey: sk, data: big });

			const queries = [{ hashKey: "alice" }, { hashKey: "bob" }];
			const got: Array<string | Uint8Array | undefined> = [];
			let cursor: string | undefined;
			let pages = 0;
			for (;;) {
				const res = await db.queryItems({ queries, maxPageBytes: 25 * 1024, cursor });
				got.push(...sksOf(res));
				pages++;
				if (res.cursor === undefined) break;
				cursor = res.cursor;
				expect(pages).toBeLessThan(50);
			}

			expect(got).toEqual([...aliceSks, ...bobSks]);
			expect(pages).toBeGreaterThan(1);
			expect(new Set(got.map(String)).size).toBe(got.length);
		});

		it("rejects a cursor whose request fingerprint differs from the resumed request", async () => {
			const db = makeDB();
			for (const sk of ["a1", "a2", "a3"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
			await db.putItem({ hashKey: "bob", sortKey: "b1", data: "x" });

			const first = await db.queryItems({ queries: [{ hashKey: "alice" }], limit: 2 });
			expect(first.cursor).toBeDefined();

			// Same cursor, different queries[] → fingerprint mismatch.
			await expect(db.queryItems({ queries: [{ hashKey: "bob" }], cursor: first.cursor })).rejects.toThrow(/fingerprint mismatch/);
		});

		it("rejects a cursor whose direction differs from the resumed request", async () => {
			const db = makeDB();
			for (const sk of ["a1", "a2", "a3"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });

			const first = await db.queryItems({ queries: [{ hashKey: "alice" }], limit: 2 });
			expect(first.cursor).toBeDefined();

			await expect(db.queryItems({ queries: [{ hashKey: "alice", scanIndexForward: false }], cursor: first.cursor })).rejects.toThrow(
				/direction mismatch/,
			);
		});

		it("rejects a malformed cursor", async () => {
			const db = makeDB();
			await expect(db.queryItems({ queries: [{ hashKey: "alice" }], cursor: "not-a-real-cursor!!" })).rejects.toThrow(/cursor/);
		});

		it("errors on an empty queries list", async () => {
			const db = makeDB();
			await expect(db.queryItems({ queries: [] })).rejects.toThrow(/must not be empty/);
		});
	});

	describe("FokosDB.queryItems — sort-key condition operators", () => {
		const ALL_SKS = ["a", "ab", "abc", "b", "ba", "c", "d"];

		async function populateAndQuery(sort: Parameters<FokosDB["queryItems"]>[0]["queries"][0]["sort"]) {
			const db = makeDB();
			for (const sk of ALL_SKS) await db.putItem({ hashKey: "k", sortKey: sk, data: "x" });
			return db.queryItems({ queries: [{ hashKey: "k", sort }] });
		}

		it("eq: returns only the exact match", async () => {
			const res = await populateAndQuery({ op: "eq", value: "b" });
			expect(sksOf(res)).toEqual(["b"]);
		});

		it("gt: returns items strictly greater", async () => {
			const res = await populateAndQuery({ op: "gt", value: "b" });
			expect(sksOf(res)).toEqual(["ba", "c", "d"]);
		});

		it("gte: returns items greater or equal", async () => {
			const res = await populateAndQuery({ op: "gte", value: "b" });
			expect(sksOf(res)).toEqual(["b", "ba", "c", "d"]);
		});

		it("lt: returns items strictly less", async () => {
			const res = await populateAndQuery({ op: "lt", value: "b" });
			expect(sksOf(res)).toEqual(["a", "ab", "abc"]);
		});

		it("lte: returns items less or equal", async () => {
			const res = await populateAndQuery({ op: "lte", value: "b" });
			expect(sksOf(res)).toEqual(["a", "ab", "abc", "b"]);
		});

		it("between: returns items in the inclusive range", async () => {
			const res = await populateAndQuery({ op: "between", lower: "ab", upper: "c" });
			expect(sksOf(res)).toEqual(["ab", "abc", "b", "ba", "c"]);
		});

		it("between: empty when lower > upper", async () => {
			const res = await populateAndQuery({ op: "between", lower: "z", upper: "a" });
			expect(sksOf(res)).toEqual([]);
		});

		it("begins_with: matches the prefix", async () => {
			const res = await populateAndQuery({ op: "begins_with", prefix: "a" });
			expect(sksOf(res)).toEqual(["a", "ab", "abc"]);
		});

		it("begins_with: single-character prefix that is also an exact key", async () => {
			const res = await populateAndQuery({ op: "begins_with", prefix: "b" });
			expect(sksOf(res)).toEqual(["b", "ba"]);
		});

		it("begins_with: multi-character prefix", async () => {
			const res = await populateAndQuery({ op: "begins_with", prefix: "ab" });
			expect(sksOf(res)).toEqual(["ab", "abc"]);
		});

		it("begins_with: empty prefix matches all", async () => {
			const res = await populateAndQuery({ op: "begins_with", prefix: "" });
			expect(sksOf(res)).toEqual(ALL_SKS);
		});

		it("begins_with: no matching prefix returns empty", async () => {
			const res = await populateAndQuery({ op: "begins_with", prefix: "zzz" });
			expect(sksOf(res)).toEqual([]);
		});

		it("range: exclusive lower, inclusive upper", async () => {
			const res = await populateAndQuery({
				op: "range",
				lower: { value: "a", inclusive: false },
				upper: { value: "b", inclusive: true },
			});
			expect(sksOf(res)).toEqual(["ab", "abc", "b"]);
		});

		it("range: open-ended (lower only)", async () => {
			const res = await populateAndQuery({ op: "range", lower: { value: "c", inclusive: true } });
			expect(sksOf(res)).toEqual(["c", "d"]);
		});

		it("range: open-ended (upper only)", async () => {
			const res = await populateAndQuery({ op: "range", upper: { value: "b", inclusive: false } });
			expect(sksOf(res)).toEqual(["a", "ab", "abc"]);
		});

		it("no sort condition: returns all items for the hash key", async () => {
			const res = await populateAndQuery(undefined);
			expect(sksOf(res)).toEqual(ALL_SKS);
		});

		it("begins_with works correctly with scanIndexForward=false", async () => {
			const db = makeDB();
			for (const sk of ALL_SKS) await db.putItem({ hashKey: "k", sortKey: sk, data: "x" });
			const res = await db.queryItems({
				queries: [{ hashKey: "k", sort: { op: "begins_with", prefix: "a" }, scanIndexForward: false }],
			});
			expect(sksOf(res)).toEqual(["abc", "ab", "a"]);
		});
	});

	describe("FokosDB — item data kinds (bytes / text / json)", () => {
		it("round-trips each kind through put→get, exposing the reconstructed value and its kind", async () => {
			const db = makeDB();
			const bytes = new Uint8Array([0, 1, 2, 255]);
			const obj = { a: 1, nested: { b: [true, "x", null] }, list: [1, 2, 3] };

			await db.putItem({ hashKey: "k", sortKey: "bytes", data: bytes });
			await db.putItem({ hashKey: "k", sortKey: "text", data: "hello" });
			await db.putItem({ hashKey: "k", sortKey: "json", data: obj });

			const gotBytes = await db.getItem({ hashKey: "k", sortKey: "bytes" });
			const gotText = await db.getItem({ hashKey: "k", sortKey: "text" });
			const gotJson = await db.getItem({ hashKey: "k", sortKey: "json" });

			expect(gotBytes).toMatchObject({ found: true, item: { kind: "bytes", data: bytes } });
			expect(gotText).toMatchObject({ found: true, item: { kind: "text", data: "hello" } });
			expect(gotJson).toMatchObject({ found: true, item: { kind: "json" } });
			if (gotJson.found) expect(gotJson.item.data).toEqual(obj); // deep structural equality after JSONB round-trip
		});

		it("keeps a bare string as opaque text (not JSON-wrapped), byte-identical on read", async () => {
			const db = makeDB();
			const jsonText = '{"a":1}'; // legitimate JSON *text* stored as a string stays a string
			await db.putItem({ hashKey: "k", sortKey: "s", data: jsonText });
			const got = await db.getItem({ hashKey: "k", sortKey: "s" });
			expect(got).toMatchObject({ found: true, item: { kind: "text", data: jsonText } });
		});

		it("exposes kind on queryItems results and parses json rows", async () => {
			const db = makeDB();
			await db.putItem({ hashKey: "q", sortKey: "1", data: "plain" });
			await db.putItem({ hashKey: "q", sortKey: "2", data: { n: 42 } });

			const res = await db.queryItems({ queries: [{ hashKey: "q" }] });
			expect(res.items).toMatchObject([
				{ sortKey: "1", kind: "text", data: "plain" },
				{ sortKey: "2", kind: "json", data: { n: 42 } },
			]);
		});

		it("round-trips a json value written and read through a transaction", async () => {
			const db = makeDB();
			const obj = { status: "active", tags: ["a", "b"] };
			const write = await db.transactWriteItems({
				operations: [{ hashKey: "t", sortKey: "j", operation: "put", data: obj }],
			});
			expect(write.outcome).toBe("committed");

			const read = await db.transactGetItems({ items: [{ hashKey: "t", sortKey: "j" }] });
			expect(read.outcome).toBe("committed");
			if (read.outcome === "committed") {
				expect(read.items[0]).toMatchObject({ found: true, kind: "json" });
				const item = read.items[0];
				if (item.found) expect(item.data).toEqual(obj);
			}
		});

		it("rejects data that is not JSON-serializable", async () => {
			const db = makeDB();
			const circular: Record<string, unknown> = {};
			circular.self = circular;
			// Intentionally passing a non-serializable value; cast past the JsonComposite type to reach the runtime guard.
			await expect(db.putItem({ hashKey: "k", sortKey: "bad", data: circular as never })).rejects.toThrow(/not JSON-serializable/);
		});
	});
});

// Builds a FokosDB over a fresh, isolated table for the given partition DO namespace. Generous split
// thresholds keep every key on a single root partition so these tests exercise FokosDB.queryItems'
// cross-sub-query fan-out and pagination, not the DO-level range-tree walk (covered in do-partition.test.ts).
function makeDBFor(ns: PartitionNamespaceKey) {
	const tableName = `test.${crypto.randomUUID()}`;
	const base = PartitionContextCreator.create({
		ns,
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName,
		rootTreesN: 1,
		hashSplitN: 2,
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: 500 },
		rangeSplitConditions: { maxSizeMb: 500 },
	});
	return new FokosDB({
		topology: new PartitionTopologyRouterImpl(base),
		transactionCoordinatorNs: env.TRANSACTION_COORDINATOR_DO,
	});
}

function sksOf(res: { items: Array<{ sortKey?: string | Uint8Array }> }) {
	return res.items.map((i) => i.sortKey);
}
