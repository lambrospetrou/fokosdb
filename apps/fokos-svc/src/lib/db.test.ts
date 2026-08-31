import { env } from "cloudflare:workers";
import { StaticShardedDO } from "durable-utils/do-sharding";
import { describe, expect, it, vi } from "vitest";
import { FokosDB } from "./db.js";
import { TransactionCoordinatorDO } from "./do-transaction-coordinator.js";
import { PartitionContextCreator, type PartitionNamespaceKey } from "./partition-topology/partition-context.js";
import { PartitionTopologyRouterImpl } from "./partition-topology/router.js";
import { MAX_ITEM_BYTES, MAX_ITEMS_PER_TX } from "./transaction-limits.js";

// Run the whole suite against every partition DO namespace so a divergence in a customer-provided
// class (e.g. CUSTOM_PARTITION_DO) is caught as a regression. makeDB is the only namespace-coupled
// point, so binding it once per case via closure keeps every test body untouched.
describe.each(["PARTITION_DO", "CUSTOM_PARTITION_DO"] as const)("FokosDB over %s", (ns) => {
	const makeDB = () => makeDBFor(ns);

	describe("FokosDB — public results carry no internal routing state", () => {
		// `_internal.rangeAncestors` is partition-to-partition routing state whose boundaries are
		// KeyBytes, so leaking it also serialized them as {"0":97,"1":98} over HTTP. db.ts is the
		// boundary where it stops. Asserted on every method that returns a meta, and on the
		// per-partition metas, since each is a separate exit that has to strip it.
		it("strips _internal from every meta a public method returns", async () => {
			const db = makeDB();

			const put = await db.putItem({ hashKey: "alice", sortKey: "sk1", data: "x" });
			const get = await db.getItem({ hashKey: "alice", sortKey: "sk1" });
			const missing = await db.getItem({ hashKey: "alice", sortKey: "nope" });
			const query = await db.queryItems({ queries: [{ hashKey: "alice" }] });
			const del = await db.deleteItem({ hashKey: "alice", sortKey: "sk1" });

			for (const meta of [put.meta, get.meta, missing.meta, del.meta, ...query.partitionMetas]) {
				expect(meta).not.toHaveProperty("_internal");
				// The rest of the meta must survive the strip.
				expect(meta.servedByActorName).toBeTypeOf("string");
			}
			expect(query.partitionMetas).not.toHaveLength(0);
		});
	});

	describe("FokosDB — transaction coordinator pool", () => {
		it("derives two coordinators per root partition", () => {
			expect(makeDBFor(ns, { rootTreesN: 1 }).options().numTxCoordinators).toBe(2);
			expect(makeDBFor(ns, { rootTreesN: 3 }).options().numTxCoordinators).toBe(6);
		});

		it("uses and validates an explicit numTxCoordinators value", () => {
			expect(makeDBFor(ns, { rootTreesN: 3, numTxCoordinators: 5 }).options().numTxCoordinators).toBe(5);
			for (const numTxCoordinators of [0, -1, 1.5]) {
				expect(() => makeDBFor(ns, { numTxCoordinators })).toThrow(/numTxCoordinators must be an integer greater or equal to 1/);
			}
		});

		it("destroys coordinator pools larger than 1,000 shards in filtered batches", async () => {
			const db = makeDBFor(ns, { rootTreesN: 501 });
			const some = vi.spyOn(StaticShardedDO.prototype, "some").mockResolvedValue([]);
			const all = vi.spyOn(StaticShardedDO.prototype, "all");
			const traverse = vi.spyOn(db.options().topology, "traverseForDestroy").mockResolvedValue();
			try {
				await expect(db.destroy()).resolves.toEqual({ ok: true });
				expect(all).not.toHaveBeenCalled();
				expect(some).toHaveBeenCalledTimes(2);
				const calls = (some as unknown as { mock: { calls: Array<[unknown, { filterFn: (shard: number) => boolean }]> } }).mock.calls;
				const firstFilter = calls[0][1].filterFn;
				const secondFilter = calls[1][1].filterFn;
				expect([firstFilter(0), firstFilter(999), firstFilter(1000)]).toEqual([true, true, false]);
				expect([secondFilter(999), secondFilter(1000), secondFilter(1001)]).toEqual([false, true, true]);
				expect(traverse).toHaveBeenCalledTimes(1);
			} finally {
				some.mockRestore();
				all.mockRestore();
				traverse.mockRestore();
			}
		});
	});

	describe("FokosDB — TTL", () => {
		it("stores, returns, and clears ttlAt", async () => {
			const db = makeDB();
			const ttlAt = Math.floor(Date.now() / 1000) + 3600;

			await db.putItem({ hashKey: "ttl", sortKey: "item", data: "v1", ttlAt });
			expect(await db.getItem({ hashKey: "ttl", sortKey: "item" })).toMatchObject({ found: true, item: { ttlAt } });
			expect((await db.queryItems({ queries: [{ hashKey: "ttl" }] })).items[0].ttlAt).toBe(ttlAt);

			await db.putItem({ hashKey: "ttl", sortKey: "item", data: "v2" });
			const cleared = await db.getItem({ hashKey: "ttl", sortKey: "item" });
			expect(cleared).toMatchObject({ found: true, item: { data: "v2" } });
			if (cleared.found) expect(cleared.item.ttlAt).toBeUndefined();
		});

		it.each([0, -1, 1.5])("rejects invalid ttlAt %s for direct and transactional puts", async (ttlAt) => {
			const db = makeDB();
			await expect(db.putItem({ hashKey: "invalid-ttl", data: "v", ttlAt })).rejects.toThrow(/ttlAt/);
			await expect(db.transactWriteItems({ items: [{ hashKey: "invalid-ttl-tx", operation: "put", data: "v", ttlAt }] })).rejects.toThrow(
				/ttlAt/,
			);
		});

		it("accepts a past ttlAt and deletes the item in a later cycle", async () => {
			const db = makeDB();
			const ttlAt = Math.max(1, Math.floor(Date.now() / 1000) - 1);
			await db.putItem({ hashKey: "past-ttl", data: "v", ttlAt });
			expect(await db.getItem({ hashKey: "past-ttl" })).toMatchObject({ found: true, item: { ttlAt } });

			await vi.waitFor(async () => expect((await db.getItem({ hashKey: "past-ttl" })).found).toBe(false), {
				timeout: 3_000,
				interval: 100,
			});
		});
	});

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
					{ hashKey: "k", sortKeyCondition: { op: "lte", value: "s2" } },
					{ hashKey: "k", sortKeyCondition: { op: "gte", value: "s3" } },
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
					{ hashKey: "zzz", sortKeyCondition: { op: "between", lower: "z9", upper: "z1" } }, // lower > upper → empty
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

		async function populateAndQuery(sortKeyCondition: Parameters<FokosDB["queryItems"]>[0]["queries"][0]["sortKeyCondition"]) {
			const db = makeDB();
			for (const sk of ALL_SKS) await db.putItem({ hashKey: "k", sortKey: sk, data: "x" });
			return db.queryItems({ queries: [{ hashKey: "k", sortKeyCondition }] });
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
				queries: [{ hashKey: "k", sortKeyCondition: { op: "begins_with", prefix: "a" }, scanIndexForward: false }],
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
				items: [{ hashKey: "t", sortKey: "j", operation: "put", data: obj }],
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

		// `JsonComposite` accepts arrays and objects only. TypeScript says so; these pin that the
		// runtime agrees, which is what a JS caller actually meets — a primitive used to be stored
		// silently as json, making the declared type a lie.
		it.each([
			["a number", 5],
			["a boolean", true],
			["null", null],
			["a function", () => {}],
		])("rejects %s as top-level data, in putItem and transactWriteItems alike", async (_name, data) => {
			const db = makeDB();
			const expected = /data must be an object, array, string or Uint8Array/;

			await expect(db.putItem({ hashKey: "k", sortKey: "prim", data: data as never })).rejects.toThrow(expected);
			await expect(
				db.transactWriteItems({ items: [{ hashKey: "k", sortKey: "prim", operation: "put", data: data as never }] }),
			).rejects.toThrow(expected);
		});

		// The one value the guard above lets through that JSON.stringify still drops: a toJSON that
		// returns undefined makes the WHOLE document undefined, not just that field.
		it("rejects an object whose toJSON() returns undefined, and says so", async () => {
			const db = makeDB();
			const data = { toJSON: () => undefined };
			await expect(db.putItem({ hashKey: "k", sortKey: "tojson", data: data as never })).rejects.toThrow(
				/not JSON-serializable \(its toJSON\(\) returned undefined\)/,
			);
		});
	});

	// The DO returns no keys — db.ts answers with the caller's own. These pin that mapping, which no
	// DO-level test can cover.
	describe("FokosDB — results carry the caller's own keys", () => {
		it("reports an absent sortKey as undefined on put, get and delete", async () => {
			const db = makeDB();

			expect((await db.putItem({ hashKey: "no-sk", data: "v" })).item).toEqual({ hashKey: "no-sk", sortKey: undefined });

			const got = await db.getItem({ hashKey: "no-sk" });
			expect(got.found).toBe(true);
			expect(got.item.hashKey).toBe("no-sk");
			expect(got.item.sortKey).toBeUndefined();

			expect((await db.deleteItem({ hashKey: "no-sk" })).item).toEqual({ hashKey: "no-sk", sortKey: undefined });
		});

		it("returns a binary key as the bytes the caller passed, not the encoded form", async () => {
			const db = makeDB();
			// KeyCodec 0xFF-tags a binary key, so the stored form differs from this one.
			const hashKey = new Uint8Array([1, 2, 3]);
			const sortKey = new Uint8Array([9]);

			expect((await db.putItem({ hashKey, sortKey, data: "v" })).item).toEqual({ hashKey, sortKey });

			const got = await db.getItem({ hashKey, sortKey });
			expect(got.found).toBe(true);
			expect(got.item.hashKey).toEqual(hashKey);
			expect(got.item.sortKey).toEqual(sortKey);
		});
	});

	// Every write path caps one item's data at the same value, and every key-taking path runs the same
	// key rules. A limit or a rule that applies through one API and not another is a bug in itself:
	// the caller cannot know which of two equivalent calls will be accepted.
	describe("FokosDB — limits and key validation are uniform across the APIs", () => {
		it("rejects an over-size clientRequestToken before the coordinator RPC", async () => {
			const db = makeDB();
			const initiateWrite = vi.spyOn(TransactionCoordinatorDO.prototype, "initiateWrite");
			try {
				await expect(
					db.transactWriteItems({
						items: [{ hashKey: "token-limit", operation: "put", data: "value" }],
						clientRequestToken: `${"é".repeat(32)}x`,
					}),
				).rejects.toThrow(/clientRequestToken exceeds 64 bytes/);
				expect(initiateWrite).not.toHaveBeenCalled();
			} finally {
				initiateWrite.mockRestore();
			}
		});

		it("caps putItem data at the same per-item limit as a transactional put", async () => {
			const db = makeDB();
			const tooBig = new Uint8Array(MAX_ITEM_BYTES + 1);

			await expect(db.putItem({ hashKey: "big", data: tooBig })).rejects.toThrow(/item data exceeds 400 KB/);
			await expect(db.transactWriteItems({ items: [{ hashKey: "big", operation: "put", data: tooBig }] })).rejects.toThrow(
				/item data exceeds 400 KB/,
			);

			// Exactly at the limit is accepted by both.
			const atLimit = new Uint8Array(MAX_ITEM_BYTES);
			await expect(db.putItem({ hashKey: "at-limit", data: atLimit })).resolves.toMatchObject({ version: 1 });
			await expect(db.transactWriteItems({ items: [{ hashKey: "at-limit-tx", operation: "put", data: atLimit }] })).resolves.toMatchObject({
				outcome: "committed",
			});
		});

		it("caps the transactGetItems item count like the write path", async () => {
			const db = makeDB();
			const items = Array.from({ length: MAX_ITEMS_PER_TX + 1 }, (_, i) => ({ hashKey: `k-${i}` }));
			await expect(db.transactGetItems({ items })).rejects.toThrow(/at most 100 items/);
			await expect(db.transactGetItems({ items: [] })).rejects.toThrow(/at least 1 item/);
		});

		it("rejects in queryItems the hash keys that putItem rejects", async () => {
			const db = makeDB();
			for (const hashKey of ["", "h\0k"]) {
				await expect(db.putItem({ hashKey, data: "v" })).rejects.toThrow();
				await expect(db.queryItems({ queries: [{ hashKey }] })).rejects.toThrow();
			}
		});

		it("rejects a NUL in every sort-key bound a query can carry", async () => {
			const db = makeDB();
			const bad = "s\0k";
			for (const sortKeyCondition of [
				{ op: "eq", value: bad },
				{ op: "gt", value: bad },
				{ op: "begins_with", prefix: bad },
				{ op: "between", lower: "a", upper: bad },
				{ op: "range", lower: { value: bad, inclusive: true } },
			] as const) {
				await expect(db.queryItems({ queries: [{ hashKey: "hk", sortKeyCondition }] })).rejects.toThrow(/sortKey must not contain the NUL/);
			}
		});

		// An empty prefix is not an empty key — it means "every sort key" — so the emptiness rule that
		// applies to item keys must not reach query bounds.
		it("still accepts begins_with with an empty prefix", async () => {
			const db = makeDB();
			await db.putItem({ hashKey: "prefix-hk", sortKey: "s1", data: "v" });
			const res = await db.queryItems({ queries: [{ hashKey: "prefix-hk", sortKeyCondition: { op: "begins_with", prefix: "" } }] });
			expect(res.items.map((i) => i.sortKey)).toEqual(["s1"]);
		});
	});
});

// Builds a FokosDB over a fresh, isolated table for the given partition DO namespace. Generous split
// thresholds keep every key on a single root partition so these tests exercise FokosDB.queryItems'
// cross-sub-query fan-out and pagination, not the DO-level range-tree walk (covered in do-partition.test.ts).
function makeDBFor(ns: PartitionNamespaceKey, options?: { rootTreesN?: number; numTxCoordinators?: number }) {
	const tableName = `test.${crypto.randomUUID()}`;
	const base = PartitionContextCreator.create({
		ns,
		nsTx: "TRANSACTION_COORDINATOR_DO",
		tableName,
		rootTreesN: options?.rootTreesN ?? 1,
		hashSplitN: 2,
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: 500 },
		rangeSplitConditions: { maxSizeMb: 500 },
	});
	return new FokosDB({
		topology: new PartitionTopologyRouterImpl(base),
		transactionCoordinatorNs: env.TRANSACTION_COORDINATOR_DO,
		numTxCoordinators: options?.numTxCoordinators,
	});
}

function sksOf(res: { items: Array<{ sortKey?: string | Uint8Array }> }) {
	return res.items.map((i) => i.sortKey);
}
