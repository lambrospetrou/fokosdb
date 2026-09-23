import { describe, expect, it, vi } from "vitest";
import { FokosDB } from "./db.js";
import { FokosStd } from "./fokos-std.js";
import invariant from "../shared/invariant.js";
import { TransactionCoordinatorDO } from "../server/do-transaction-coordinator.js";
import { PartitionDO } from "../server/do-partition.js";
import {
	DEFAULT_EVALUATED_ITEMS_PER_PAGE,
	DEFAULT_RESPONSE_BYTES_PER_PAGE,
	MAX_EVALUATED_BYTES_PER_PAGE,
	MAX_EVALUATED_ITEMS_PER_PAGE,
	MAX_PARTITION_VISITS_PER_PAGE,
	MAX_RESPONSE_BYTES_PER_PAGE,
} from "../shared/query/page-budget.js";
import { PartitionContextCreator, type PartitionNamespaceKey } from "../shared/partition-context.js";
import { FokosRouter } from "../sharding/router.js";
import { MAX_ITEM_BYTES, MAX_ITEMS_PER_TX } from "../shared/transaction-limits.js";
import { KeyCodec } from "../sharding/key-codec.js";
import type { ConditionExpression, ProjectionExpression } from "../shared/expression/types.js";
import type { JsonValue } from "../shared/json-types.js";
import { EST_ROW_BYTES_K } from "../shared/partition/item-size.js";
import { fokosErrorWith } from "../../test/errors-matchers.js";
import { attachRouting } from "../sharding/envelope.js";
import { FokosUnavailableError, type FokosError } from "../shared/errors.js";
import { SHARDING_UNAVAILABLE_CODES } from "../sharding/errors.js";

// Run the whole suite against every partition DO namespace so a divergence in a customer-provided
// class (e.g. CUSTOM_PARTITION_DO) is caught as a regression. makeDB is the only namespace-coupled
// point, so binding it once per case via closure keeps every test body untouched.
// Every case under "isolated-fixture cases" builds its own table, so they all run concurrently.
// A describe that installs a prototype or module spy stays outside the block: a sequential
// describe is a barrier the runner never overlaps with the concurrent tests.
describe.each(["PARTITION_DO", "CUSTOM_PARTITION_DO"] as const)("FokosDB over %s", (ns) => {
	const makeDB = () => makeDBFor(ns);

	// Every case here builds its own table, so all of them run concurrently.
	describe.concurrent("isolated-fixture cases", () => {
		describe("FokosDB — public results carry no internal routing state", () => {
			// The envelope's route evidence and its `_hint.rangeAncestors` are partition-to-partition routing
			// state whose boundaries are KeyBytes, so leaking them would also serialize as {"0":97,"1":98}
			// over HTTP. db.ts is the boundary where they stop. Asserted on every method that returns a
			// meta, and on the per-partition metas, since each is a separate exit that has to build it.
			it("returns a public meta on every method, and no routing evidence", async () => {
				const db = makeDB();

				const put = await db.putItem({ hashKey: "alice", sortKey: "sk1", data: "x" });
				const get = await db.getItem({ hashKey: "alice", sortKey: "sk1" });
				const missing = await db.getItem({ hashKey: "alice", sortKey: "nope" });
				const query = await db.queryItems({ queries: [{ hashKey: "alice" }] });
				const del = await db.deleteItem({ hashKey: "alice", sortKey: "sk1" });

				for (const meta of [put.meta, get.meta, missing.meta, del.meta, ...query.partitionMetas]) {
					expect(meta).not.toHaveProperty("_rangeAncestors");
					expect(meta).not.toHaveProperty("servedBy");
					expect(meta.servedByActorName).toBeTypeOf("string");
					expect(meta.servedByPartitionId).toBeTypeOf("string");
				}
				expect(query.partitionMetas).not.toHaveLength(0);
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
				await expect(db.putItem({ hashKey: "invalid-ttl", data: "v", ttlAt })).rejects.toThrow(fokosErrorWith("ttl_at_invalid"));
				await expect(db.transactWriteItems({ items: [{ hashKey: "invalid-ttl-tx", operation: "put", data: "v", ttlAt }] })).rejects.toThrow(
					fokosErrorWith("ttl_at_invalid"),
				);
			});

			it("accepts a past ttlAt and deletes the item in a later cycle", async () => {
				const db = makeDB();
				const ttlAt = Math.max(1, Math.floor(Date.now() / 1000) - 1);
				await db.putItem({ hashKey: "past-ttl", data: "v", ttlAt });
				// The sweep can legally run before this read, so only a row that survived is checked.
				const first = await db.getItem({ hashKey: "past-ttl" });
				if (first.found) {
					expect(first.item.ttlAt).toBe(ttlAt);
				}

				await vi.waitFor(async () => expect((await db.getItem({ hashKey: "past-ttl" })).found).toBe(false), {
					timeout: 3_000,
					interval: 100,
				});
			});
		});

		describe("FokosDB.queryItems — projections", () => {
			it("returns flat projected records by resolved name", async () => {
				const db = makeDB();
				await db.putItem({ hashKey: "alice", sortKey: "a1", data: { n: 1, s: "x" } });
				await db.putItem({ hashKey: "alice", sortKey: "a2", data: { s: "y" } });
				await db.putItem({ hashKey: "alice", sortKey: "a3", data: { n: 3, s: "z", k: [1, 2] } });
				await db.putItem({ hashKey: "alice", sortKey: "a4", data: "text-item" });

				const res = await db.queryItems({
					queries: [{ hashKey: "alice" }],
					projection: [
						{ expr: { ref: "sortKey" }, as: "id" },
						{ expr: { ref: "data", path: "$.n" } },
						{ expr: { ref: "data", path: "$.k" }, as: "k" },
						{ expr: { fn: "sqlite.upper", args: [{ ref: "data", path: "$.s" }] }, as: "S" },
						{ expr: { ref: "data" } },
					],
				});

				expect(res.items).toEqual([
					{ id: "a1", "$.n": 1, S: "X", data: { n: 1, s: "x" } },
					{ id: "a2", S: "Y", data: { s: "y" } },
					{ id: "a3", "$.n": 3, k: [1, 2], S: "Z", data: { n: 3, s: "z", k: [1, 2] } },
					// A path over a text item is missing. A function over a missing argument sees NULL.
					{ id: "a4", S: null, data: "text-item" },
				]);
				// A missing cell leaves the key absent, not undefined.
				expect(Object.keys(res.items[1])).not.toContain("$.n");
				expect(res.count).toBe(4);
				expect(res.scannedCount).toBe(4);
			});

			it("rejects a projection with count selection", async () => {
				const db = makeDB();
				await expect(
					db.queryItems({ queries: [{ hashKey: "alice" }], select: "count", projection: [{ expr: { ref: "sortKey" } }] }),
				).rejects.toThrow(fokosErrorWith("query_projection_with_count"));
			});

			it("rejects a cursor resumed with another projection or none", async () => {
				const db = makeDB();
				for (const sk of ["a1", "a2", "a3", "a4"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
				const projectionA: ProjectionExpression[] = [{ expr: { ref: "sortKey" }, as: "id" }];
				const projectionB: ProjectionExpression[] = [{ expr: { ref: "sortKey" }, as: "sk" }];

				const first = await db.queryItems({ queries: [{ hashKey: "alice" }], projection: projectionA, limit: 2 });
				expect(first.items).toEqual([{ id: "a1" }, { id: "a2" }]);
				expect(first.cursor).toBeDefined();

				await expect(db.queryItems({ queries: [{ hashKey: "alice" }], projection: projectionB, cursor: first.cursor })).rejects.toThrow(
					fokosErrorWith("cursor_fingerprint_mismatch"),
				);
				await expect(db.queryItems({ queries: [{ hashKey: "alice" }], cursor: first.cursor })).rejects.toThrow(
					fokosErrorWith("cursor_fingerprint_mismatch"),
				);

				// The same projection resumes without a gap or a duplicate.
				const got: unknown[] = first.items.map((item) => item.id);
				let cursor = first.cursor;
				for (;;) {
					const res = await db.queryItems({ queries: [{ hashKey: "alice" }], projection: projectionA, cursor });
					got.push(...res.items.map((item) => item.id));
					if (res.cursor === undefined) break;
					cursor = res.cursor;
				}
				expect(got).toEqual(["a1", "a2", "a3", "a4"]);
			});

			it("paginates projected rows across sub-queries without gaps or duplicates", async () => {
				const db = makeDB();
				for (const sk of ["a1", "a2", "a3"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
				for (const sk of ["b1", "b2", "b3"]) await db.putItem({ hashKey: "bob", sortKey: sk, data: "x" });

				const queries = [{ hashKey: "alice" }, { hashKey: "bob" }];
				const projection: ProjectionExpression[] = [{ expr: { ref: "sortKey" }, as: "id" }];
				const got: unknown[] = [];
				let cursor: string | undefined;
				let pages = 0;
				for (;;) {
					const res = await db.queryItems({ queries, projection, limit: 2, cursor });
					got.push(...res.items.map((item) => item.id));
					pages++;
					if (res.cursor === undefined) break;
					cursor = res.cursor;
					expect(pages).toBeLessThan(50);
				}

				expect(got).toEqual(["a1", "a2", "a3", "b1", "b2", "b3"]);
				expect(pages).toBeGreaterThan(1);
			});
		});

		describe("FokosDB.getItem — projections", () => {
			it("returns a flat projected record for a found item, across cell kinds", async () => {
				const db = makeDB();
				await db.putItem({
					hashKey: "alice",
					sortKey: "j1",
					data: { n: 1, s: "alpha", none: null, k: [1, 2] },
				});

				const res = await db.getItem({
					hashKey: "alice",
					sortKey: "j1",
					projection: [
						{ expr: { ref: "sortKey" }, as: "id" },
						{ expr: { ref: "data", path: "$.n" } },
						{ expr: { ref: "data", path: "$.s" }, as: "name" },
						{ expr: { ref: "data", path: "$.none" } },
						{ expr: { ref: "data", path: "$.absent" }, as: "missing" },
						{ expr: { ref: "data", path: "$.k" }, as: "k" },
						{ expr: { ref: "v" }, as: "ver" },
					],
				});

				expect(res).toEqual({
					found: true,
					item: {
						hashKey: "alice",
						sortKey: "j1",
						data: { id: "j1", "$.n": 1, name: "alpha", "$.none": null, k: [1, 2], ver: 1 },
						kind: "projected",
						version: 1,
					},
					meta: expect.anything(),
				});
				// The projected record sits in `data` inside the normal envelope, tagged `kind: "projected"`.
				invariant(res.found && res.item.kind === "projected");
				expect(Object.keys(res)).toEqual(["found", "item", "meta"]);
				expect(Object.keys(res.item)).toEqual(["hashKey", "sortKey", "data", "kind", "version"]);
				expect(Object.keys(res.item.data)).not.toContain("missing");
			});

			it("projects a bytes cell as a Uint8Array", async () => {
				const db = makeDB();
				await db.putItem({ hashKey: "alice", sortKey: "b1", data: new Uint8Array([1, 2, 3]) });

				const res = await db.getItem({ hashKey: "alice", sortKey: "b1", projection: [{ expr: { ref: "data" }, as: "bytes" }] });
				invariant(res.found && res.item.kind === "projected");
				expect(res.item.data.bytes).toEqual(new Uint8Array([1, 2, 3]));
			});

			it("returns found:false with the caller's keys for a missing item", async () => {
				const db = makeDB();
				const res = await db.getItem({ hashKey: "alice", sortKey: "nope", projection: [{ expr: { ref: "data" } }] });
				expect(res).toMatchObject({ found: false, item: { hashKey: "alice", sortKey: "nope" } });
			});
		});

		describe("FokosDB reads — the caller's own type", () => {
			type Player = { name: string; score: number };

			it("types the json value and the projected record of every read", async () => {
				const db = makeDB();
				await db.putItem({ hashKey: "alice", sortKey: "p1", data: { name: "alpha", score: 3 } });

				const whole = await db.getItem<Player>({ hashKey: "alice", sortKey: "p1" });
				invariant(whole.found && whole.item.kind === "json");
				expect(whole.item.data.name).toBe("alpha");

				const projected = await db.getItem<Pick<Player, "name">>({
					hashKey: "alice",
					sortKey: "p1",
					projection: [{ expr: { ref: "data", path: "$.name" }, as: "name" }],
				});
				invariant(projected.found && projected.item.kind === "projected");
				expect(projected.item.data.name).toBe("alpha");

				const page = await db.queryItems<Pick<Player, "score">>({
					queries: [{ hashKey: "alice", sortKeyCondition: { op: "eq", value: "p1" } }],
					projection: [{ expr: { ref: "data", path: "$.score" }, as: "score" }],
				});
				expect(page.items[0].score).toBe(3);

				// One request, two unrelated items: each position is typed by its own member of the tuple.
				await db.putItem({ hashKey: "alice", sortKey: "p2", data: { label: "beta" } });
				const read = await db.transactGetItems<[Player, { label: string }]>({
					items: [
						{ hashKey: "alice", sortKey: "p1" },
						{ hashKey: "alice", sortKey: "p2" },
					],
				});
				const [first, second] = read.items;
				invariant(first.found && first.kind === "json");
				expect(first.data.score).toBe(3);
				invariant(second.found && second.kind === "json");
				expect(second.data.label).toBe("beta");
			});

			// Type-level contract, never executed: the tuple fixes the item count in both directions, and a
			// call that names no type keeps the widest types.
			async function _transactGetItemsArity(db: FokosDB) {
				// @ts-expect-error two types named, one item passed
				await db.transactGetItems<[Player, Player]>({ items: [{ hashKey: "a" }] });
				// @ts-expect-error one type named, two items passed
				await db.transactGetItems<[Player]>({ items: [{ hashKey: "a" }, { hashKey: "b" }] });
				const wide = await db.transactGetItems({ items: [{ hashKey: "a" }] });
				if (wide.items[0].found && wide.items[0].kind === "json") {
					const value: JsonValue = wide.items[0].data;
					void value;
				}
			}
			void _transactGetItemsArity;
		});

		describe("FokosDB.queryItems — filters", () => {
			// Mixed data kinds under one hash key: the filter must evaluate per candidate and must not
			// change candidate selection, so every case scans all four items. A missing operand makes a
			// comparison false, which is what keeps the text and bytes items out of the numeric cases.
			it.each<{ name: string; filter: ConditionExpression; expected: string[] }>([
				{ name: "eq", filter: { op: "eq", args: [{ ref: "data", path: "$.n" }, { val: 1 }] }, expected: ["j1"] },
				{ name: "ne", filter: { op: "ne", args: [{ ref: "data", path: "$.n" }, { val: 1 }] }, expected: ["j2"] },
				{ name: "eq on a boolean", filter: { op: "eq", args: [{ ref: "data", path: "$.flag" }, { val: true }] }, expected: ["j1"] },
				// A stored JSON null equals a null literal, and a path that is absent does not.
				{ name: "eq on a JSON null", filter: { op: "eq", args: [{ ref: "data", path: "$.none" }, { val: null }] }, expected: ["j1"] },
				{ name: "lt", filter: { op: "lt", args: [{ ref: "data", path: "$.n" }, { val: 2 }] }, expected: ["j1"] },
				{ name: "lte", filter: { op: "lte", args: [{ ref: "data", path: "$.n" }, { val: 2 }] }, expected: ["j1", "j2"] },
				{ name: "gt", filter: { op: "gt", args: [{ ref: "data", path: "$.n" }, { val: 1 }] }, expected: ["j2"] },
				{ name: "gte", filter: { op: "gte", args: [{ ref: "data", path: "$.n" }, { val: 1 }] }, expected: ["j1", "j2"] },
				{
					name: "between",
					filter: { op: "between", args: [{ ref: "data", path: "$.n" }, { val: 2 }, { val: 5 }] },
					expected: ["j2"],
				},
				{
					name: "in",
					filter: { op: "in", args: [{ ref: "data", path: "$.s" }, { val: "alpha" }, { val: "gamma" }] },
					expected: ["j1"],
				},
				{
					name: "and",
					filter: {
						op: "and",
						args: [
							{ op: "gte", args: [{ ref: "data", path: "$.n" }, { val: 1 }] },
							{ op: "eq", args: [{ ref: "data", path: "$.s" }, { val: "beta" }] },
						],
					},
					expected: ["j2"],
				},
				{
					name: "or",
					filter: {
						op: "or",
						args: [
							{ op: "eq", args: [{ ref: "data", path: "$.n" }, { val: 1 }] },
							{ op: "eq", args: [{ ref: "data", path: "$.s" }, { val: "beta" }] },
						],
					},
					expected: ["j1", "j2"],
				},
				{
					name: "not",
					filter: { op: "not", args: [{ op: "exists", args: [{ ref: "data", path: "$.n" }] }] },
					expected: ["b1", "t1"],
				},
				{ name: "exists", filter: { op: "exists", args: [{ ref: "data", path: "$.tags" }] }, expected: ["j1", "j2"] },
				{ name: "not_exists", filter: { op: "not_exists", args: [{ ref: "data", path: "$.s" }] }, expected: ["b1", "t1"] },
				{
					name: "begins_with on a path",
					filter: { op: "begins_with", args: [{ ref: "data", path: "$.s" }, { val: "al" }] },
					expected: ["j1"],
				},
				{
					name: "begins_with on whole text data",
					filter: { op: "begins_with", args: [{ ref: "data" }, { val: "text-" }] },
					expected: ["t1"],
				},
				{
					name: "contains on a JSON array",
					filter: { op: "contains", args: [{ ref: "data", path: "$.tags" }, { val: "x" }] },
					expected: ["j1"],
				},
				{
					name: "contains on byte data",
					filter: { op: "contains", args: [{ ref: "data" }, { b64: "Ag==" }] },
					expected: ["b1"],
				},
				{
					name: "sort-key reference",
					filter: { op: "gte", args: [{ ref: "sortKey" }, { val: "j2" }] },
					expected: ["j2", "t1"],
				},
			])("every condition operator as a filter, on mixed data kinds: $name", async ({ filter, expected }) => {
				const db = makeDB();
				await db.putItem({ hashKey: "alice", sortKey: "j1", data: { n: 1, s: "alpha", tags: ["x", "y"], flag: true, none: null } });
				await db.putItem({ hashKey: "alice", sortKey: "j2", data: { n: 2, s: "beta", tags: ["y"] } });
				await db.putItem({ hashKey: "alice", sortKey: "t1", data: "text-value" });
				await db.putItem({ hashKey: "alice", sortKey: "b1", data: new Uint8Array([1, 2, 3]) });

				const res = await db.queryItems({ queries: [{ hashKey: "alice" }], filter });

				expect(sksOf(res)).toEqual(expected);
				expect(res.count).toBe(expected.length);
				expect(res.scannedCount).toBe(4);
			});

			it("a filter that rejects every candidate pages the whole interval", async () => {
				const db = makeDB();
				for (let i = 0; i < 5; i++) await db.putItem({ hashKey: "alice", sortKey: `s${i}`, data: { n: i } });
				const filter: ConditionExpression = { op: "eq", args: [{ ref: "data", path: "$.n" }, { val: 999 }] };

				const first = await db.queryItems({ queries: [{ hashKey: "alice" }], filter, limit: 2 });
				expect(first.items).toEqual([]);
				expect(first.count).toBe(0);
				expect(first.scannedCount).toBe(2);
				expect(first.cursor).toBeDefined();

				let count = first.count;
				let scannedCount = first.scannedCount;
				let cursor = first.cursor;
				let pages = 1;
				for (;;) {
					const res = await db.queryItems({ queries: [{ hashKey: "alice" }], filter, limit: 2, cursor });
					expect(res.items).toEqual([]);
					count += res.count;
					scannedCount += res.scannedCount;
					pages++;
					if (res.cursor === undefined) break;
					cursor = res.cursor;
					expect(pages).toBeLessThan(50);
				}
				expect(count).toBe(0);
				expect(scannedCount).toBe(5);
			});

			it("count mode with a filter returns the matched count of the page", async () => {
				const db = makeDB();
				for (let i = 0; i < 5; i++) await db.putItem({ hashKey: "alice", sortKey: `s${i}`, data: { n: i } });
				const filter: ConditionExpression = { op: "gte", args: [{ ref: "data", path: "$.n" }, { val: 3 }] };

				const res = await db.queryItems({ queries: [{ hashKey: "alice" }], filter, select: "count" });
				expect(res.items).toEqual([]);
				expect(res.count).toBe(2);
				expect(res.scannedCount).toBe(5);
				expect(res.count).toBeLessThan(res.scannedCount);
			});

			it("rejects a filtered cursor resumed with another filter or none", async () => {
				const db = makeDB();
				for (const sk of ["a1", "a2", "a3", "a4"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
				const filterA: ConditionExpression = { op: "gte", args: [{ ref: "sortKey" }, { val: "a2" }] };
				const filterB: ConditionExpression = { op: "gte", args: [{ ref: "sortKey" }, { val: "a1" }] };

				// limit 2 evaluates two candidates: a1 is rejected and a2 is matched, so one item returns.
				const first = await db.queryItems({ queries: [{ hashKey: "alice" }], filter: filterA, limit: 2 });
				expect(sksOf(first)).toEqual(["a2"]);
				expect(first.cursor).toBeDefined();

				await expect(db.queryItems({ queries: [{ hashKey: "alice" }], filter: filterB, cursor: first.cursor })).rejects.toThrow(
					fokosErrorWith("cursor_fingerprint_mismatch"),
				);
				await expect(db.queryItems({ queries: [{ hashKey: "alice" }], cursor: first.cursor })).rejects.toThrow(
					fokosErrorWith("cursor_fingerprint_mismatch"),
				);

				// The same filter resumes without a gap or a duplicate.
				const got: Array<string | Uint8Array | undefined> = sksOf(first);
				let cursor = first.cursor;
				let pages = 1;
				for (;;) {
					const res = await db.queryItems({ queries: [{ hashKey: "alice" }], filter: filterA, cursor });
					got.push(...sksOf(res));
					pages++;
					if (res.cursor === undefined) break;
					cursor = res.cursor;
					expect(pages).toBeLessThan(50);
				}
				expect(got).toEqual(["a2", "a3", "a4"]);
			});

			it("a filter and a projection compose in one request", async () => {
				const db = makeDB();
				for (let i = 0; i < 5; i++) await db.putItem({ hashKey: "alice", sortKey: `s${i}`, data: { n: i, s: `v${i}` } });

				const res = await db.queryItems({
					queries: [{ hashKey: "alice" }],
					filter: { op: "gte", args: [{ ref: "data", path: "$.n" }, { val: 3 }] },
					projection: [{ expr: { ref: "sortKey" }, as: "id" }, { expr: { ref: "data", path: "$.n" } }],
				});

				expect(res.items).toEqual([
					{ id: "s3", "$.n": 3 },
					{ id: "s4", "$.n": 4 },
				]);
				expect(res.count).toBe(2);
				expect(res.scannedCount).toBe(5);
			});
		});

		describe("FokosDB.queryItems — sort-key condition operators", () => {
			const ALL_SKS = ["a", "ab", "abc", "b", "ba", "c", "d"];

			async function populateAndQuery(sortKeyCondition: Parameters<FokosDB["queryItems"]>[0]["queries"][0]["sortKeyCondition"]) {
				const db = makeDB();
				for (const sk of ALL_SKS) await db.putItem({ hashKey: "k", sortKey: sk, data: "x" });
				return await db.queryItems({ queries: [{ hashKey: "k", sortKeyCondition }] });
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

		describe("FokosDB.queryItems — negative prefix match with FokosStd.notBeginsWith", () => {
			// The complement of a prefix is two ranges, one sub-query each. The fixture covers every
			// boundary: an item with no sort key (sorts first), the bare prefix, keys under the prefix, the
			// successor key itself, a later string key, and a binary key (sorts after every string).
			const BIN = new Uint8Array([1]);
			const EXPECTED_ASC = [undefined, "a", "order$", "p", BIN];

			async function populate() {
				const db = makeDB();
				await db.putItem({ hashKey: "k", data: "x" });
				for (const sk of ["a", "order#", "order#1", "order$", "p"]) await db.putItem({ hashKey: "k", sortKey: sk, data: "x" });
				await db.putItem({ hashKey: "k", sortKey: BIN, data: "x" });
				return db;
			}

			it("returns every item whose sort key does not begin with the prefix, in sort order", async () => {
				const db = await populate();
				const res = await db.queryItems({ queries: FokosStd.notBeginsWith("k", "order#") });
				expect(sksOf(res)).toEqual(EXPECTED_ASC);
				expect(res.count).toBe(EXPECTED_ASC.length);
			});

			it("keeps the order reversed with scanIndexForward=false", async () => {
				const db = await populate();
				const res = await db.queryItems({ queries: FokosStd.notBeginsWith("k", "order#", { scanIndexForward: false }) });
				expect(sksOf(res)).toEqual([...EXPECTED_ASC].reverse());
			});

			it("pages across the boundary between the two ranges without a gap or a duplicate", async () => {
				const db = await populate();
				const queries = FokosStd.notBeginsWith("k", "order#");
				const seen: Array<string | Uint8Array | undefined> = [];
				let cursor: string | undefined;
				let pages = 0;
				for (;;) {
					const res = await db.queryItems({ queries, limit: 2, cursor });
					seen.push(...sksOf(res));
					pages++;
					if (res.cursor === undefined) break;
					cursor = res.cursor;
					expect(pages).toBeLessThan(50);
				}
				expect(seen).toEqual(EXPECTED_ASC);
				expect(pages).toBeGreaterThan(1);
			});

			it("mixes with other sub-queries and count mode", async () => {
				const db = await populate();
				await db.putItem({ hashKey: "other", sortKey: "z", data: "x" });
				const res = await db.queryItems({
					queries: [...FokosStd.notBeginsWith("k", "order#"), { hashKey: "other" }],
					select: "count",
				});
				expect(res.items).toEqual([]);
				expect(res.count).toBe(EXPECTED_ASC.length + 1);
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
				// A cancelled write raises, so the returned token is the commit.
				expect(write.idempotencyToken).toEqual(expect.any(String));

				const read = await db.transactGetItems({ items: [{ hashKey: "t", sortKey: "j" }] });
				expect(read.items[0]).toMatchObject({ found: true, kind: "json" });
				const item = read.items[0];
				if (item.found) expect(item.data).toEqual(obj);
			});

			it("rejects data that is not JSON-serializable", async () => {
				const db = makeDB();
				const circular: Record<string, unknown> = {};
				circular.self = circular;
				// Intentionally passing a non-serializable value; cast past the JsonComposite type to reach the runtime guard.
				await expect(db.putItem({ hashKey: "k", sortKey: "bad", data: circular as never })).rejects.toThrow(
					fokosErrorWith("item_data_not_json_serializable"),
				);
			});

			// `JsonComposite` accepts arrays and objects only. TypeScript says so, and these pin that the
			// runtime agrees, which is what a JS caller meets. A primitive stored silently as json would make
			// the declared type a lie.
			it.each([
				["a number", 5],
				["a boolean", true],
				["null", null],
				["a function", () => {}],
			])("rejects %s as top-level data, in putItem and transactWriteItems alike", async (_name, data) => {
				const db = makeDB();
				const expected = fokosErrorWith("item_data_wrong_type");

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
					fokosErrorWith("item_data_not_json_serializable"),
				);
			});
		});

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
	});

	describe("FokosDB — internal codes at the public root", () => {
		it("maps repartition_not_cut_over to partition_migrating and keeps the internal code and error id", async () => {
			const db = makeDB();
			// The repartition protocol tells a target that its source still owns the slice. A client has
			// no repartitions in its vocabulary, and the condition means "retry shortly" to it.
			const leaf = { ref: { partitionId: "01", doName: "leaf" }, actorId: "actor", hashDepth: 1, rangeDepth: 0, role: "executed" as const };
			const internal = attachRouting(
				new FokosUnavailableError(SHARDING_UNAVAILABLE_CODES.repartition_not_cut_over, {
					message: "the source still owns this slice",
					attributes: { repartitionId: "r1" },
				}),
				{ servedBy: [{ ...leaf, _rangeAncestors: [] }], forwardCount: 1, servedByTruncated: false },
			);
			const spy = vi.spyOn(PartitionDO.prototype, "apiGetItem").mockRejectedValue(internal);
			try {
				await expect(db.getItem({ hashKey: "alice" })).rejects.toThrow(fokosErrorWith("partition_migrating"));
				const raised: FokosError = await db.getItem({ hashKey: "alice" }).then(
					() => {
						throw new Error("getItem resolved; it must raise the mapped error");
					},
					(e: FokosError) => e,
				);
				expect(raised.error_id).toBe(internal.error_id);
				expect(raised.attributes.runtimeCode).toBe(SHARDING_UNAVAILABLE_CODES.repartition_not_cut_over.code);
				// The mapping builds a new error object, so the routing must move with it. The public
				// boundary then turns it into the meta a result would carry, and drops the routing itself.
				expect((raised as { meta?: unknown }).meta).toMatchObject({ servedByActorName: "leaf", forwardCount: 1, hashDepth: 1 });
				expect(raised).not.toHaveProperty("routing");
			} finally {
				spy.mockRestore();
			}
		});
	});

	describe("FokosDB — transaction coordinator pool", () => {
		it("derives two coordinator roots per root partition", () => {
			expect(makeDBFor(ns, { rootTreesN: 1 }).options().coordinatorRootsN).toBe(2);
			expect(makeDBFor(ns, { rootTreesN: 3 }).options().coordinatorRootsN).toBe(6);
			expect(makeDBFor(ns, { rootTreesN: 32_501 }).options().coordinatorRootsN).toBe(65_000);
			expect(makeDBFor(ns, { rootTreesN: 65_000 }).options().coordinatorRootsN).toBe(65_000);
		});

		it("uses and validates an explicit coordinatorRootsN value", () => {
			expect(makeDBFor(ns, { rootTreesN: 3, coordinatorRootsN: 5 }).options().coordinatorRootsN).toBe(5);
			for (const coordinatorRootsN of [0, -1, 1.5, 65001]) {
				expect(() => makeDBFor(ns, { coordinatorRootsN })).toThrow(fokosErrorWith("num_tx_coordinators_invalid"));
			}
		});

		it("destroys the coordinator group of the table first, and then the partitions", async () => {
			const db = makeDBFor(ns, { rootTreesN: 2 });
			const walked: Array<{ shardGroup: string; rootTreesN: number }> = [];
			const walk = vi.spyOn(FokosRouter.prototype, "walk").mockImplementation(async function (this: FokosRouter<unknown>) {
				walked.push({ shardGroup: this.topology.shardGroup, rootTreesN: this.topology.rootTreesN });
			});
			try {
				await expect(db.destroy()).resolves.toEqual({ ok: true });
				const table = db.options().topology.topology.shardGroup;
				expect(walked).toEqual([
					{ shardGroup: `fokos.tc.${table}`, rootTreesN: 4 },
					{ shardGroup: table, rootTreesN: 2 },
				]);
			} finally {
				walk.mockRestore();
			}
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
			expect(res.scannedCount).toBe(5);
			expect(res.cursor).toBeUndefined();
			// One leaf scan per sub-query (both route to the same single root DO, listed once per RPC).
			expect(res.partitionMetas).toHaveLength(2);
			expect(res.meta.rowsReturned).toBe(5);
		});

		it("count selection returns the page count with no items", async () => {
			const db = makeDB();
			for (const sk of ["a1", "a2", "a3"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
			for (const sk of ["b1", "b2"]) await db.putItem({ hashKey: "bob", sortKey: sk, data: "x" });

			const res = await db.queryItems({ queries: [{ hashKey: "alice" }, { hashKey: "bob" }], select: "count" });

			expect(res.items).toEqual([]);
			expect(res.count).toBe(5);
			expect(res.scannedCount).toBe(5);
			expect(res.meta.rowsReturned).toBe(5);
			expect(res.cursor).toBeUndefined();
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
				const res = await db.queryItems({ queries, maxResponseBytes: 25 * 1024, cursor });
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
			await expect(db.queryItems({ queries: [{ hashKey: "bob" }], cursor: first.cursor })).rejects.toThrow(
				fokosErrorWith("cursor_fingerprint_mismatch"),
			);
		});

		it("rejects a cursor whose direction differs from the resumed request", async () => {
			const db = makeDB();
			for (const sk of ["a1", "a2", "a3"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });

			const first = await db.queryItems({ queries: [{ hashKey: "alice" }], limit: 2 });
			expect(first.cursor).toBeDefined();

			await expect(db.queryItems({ queries: [{ hashKey: "alice", scanIndexForward: false }], cursor: first.cursor })).rejects.toThrow(
				fokosErrorWith("cursor_direction_mismatch"),
			);
		});

		it("rejects a malformed cursor", async () => {
			const db = makeDB();
			await expect(db.queryItems({ queries: [{ hashKey: "alice" }], cursor: "not-a-real-cursor!!" })).rejects.toThrow(
				fokosErrorWith("cursor_malformed"),
			);
		});

		it("errors on an empty queries list", async () => {
			const db = makeDB();
			await expect(db.queryItems({ queries: [] })).rejects.toThrow(fokosErrorWith("query_queries_empty"));
		});

		it("resolves the page budgets from the constants", async () => {
			const db = makeDB();
			await db.putItem({ hashKey: "alice", sortKey: "a1", data: "x" });

			const spy = vi.spyOn(PartitionDO.prototype, "apiQueryItems");
			try {
				await db.queryItems({ queries: [{ hashKey: "alice" }] });
				const defaults = spy.mock.calls.at(-1)![1];
				expect(defaults.remainingEvaluatedItems).toBe(DEFAULT_EVALUATED_ITEMS_PER_PAGE);
				expect(defaults.remainingEvaluatedBytes).toBe(MAX_EVALUATED_BYTES_PER_PAGE);
				expect(defaults.remainingResponseBytes).toBe(DEFAULT_RESPONSE_BYTES_PER_PAGE);
				expect(defaults.remainingPartitionVisits).toBe(MAX_PARTITION_VISITS_PER_PAGE);
				expect(defaults.allowOversizedFirstItem).toBe(true);
				expect(defaults.select).toBe("projection");

				await db.queryItems({ queries: [{ hashKey: "alice" }], limit: 10 ** 9, maxResponseBytes: 10 ** 12, select: "count" });
				const clamped = spy.mock.calls.at(-1)![1];
				expect(clamped.remainingEvaluatedItems).toBe(MAX_EVALUATED_ITEMS_PER_PAGE);
				expect(clamped.remainingResponseBytes).toBe(MAX_RESPONSE_BYTES_PER_PAGE);
				expect(clamped.select).toBe("count");
			} finally {
				spy.mockRestore();
			}
		});

		it("explicit projection selection returns the same page as the default", async () => {
			const db = makeDB();
			for (const sk of ["a1", "a2", "a3"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });

			const res = await db.queryItems({ queries: [{ hashKey: "alice" }], select: "projection" });
			expect(sksOf(res)).toEqual(["a1", "a2", "a3"]);
			expect(res.count).toBe(3);
			expect(res.scannedCount).toBe(3);
		});

		it("count selection spans multiple, duplicate, and empty sub-queries", async () => {
			const db = makeDB();
			for (const sk of ["a1", "a2", "a3"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
			for (const sk of ["b1", "b2"]) await db.putItem({ hashKey: "bob", sortKey: sk, data: "x" });
			for (const sk of ["s1", "s2", "s3", "s4"]) await db.putItem({ hashKey: "k", sortKey: sk, data: "x" });

			const res = await db.queryItems({
				queries: [
					{ hashKey: "alice" },
					{ hashKey: "zzz", sortKeyCondition: { op: "between", lower: "z9", upper: "z1" } }, // empty interval
					{ hashKey: "k", sortKeyCondition: { op: "lte", value: "s2" } },
					{ hashKey: "k", sortKeyCondition: { op: "gte", value: "s2" } },
					{ hashKey: "bob" },
				],
				select: "count",
			});

			expect(res.items).toEqual([]);
			// 3 + 0 + 2 + 3 + 2: the duplicate "s2" counts once per sub-query.
			expect(res.count).toBe(10);
			expect(res.scannedCount).toBe(10);
			expect(res.cursor).toBeUndefined();
			// The empty interval makes no RPC.
			expect(res.partitionMetas).toHaveLength(4);
		});

		it("the first-item exception applies once per page across sub-queries", async () => {
			const db = makeDB();
			const big = "x".repeat(20 * 1024);
			await db.putItem({ hashKey: "alice", sortKey: "a1", data: big });
			await db.putItem({ hashKey: "bob", sortKey: "b1", data: big });

			const queries = [{ hashKey: "alice" }, { hashKey: "bob" }];
			const p1 = await db.queryItems({ queries, maxResponseBytes: 1 });
			expect(sksOf(p1)).toEqual(["a1"]);
			expect(p1.cursor).toBeDefined();

			const p2 = await db.queryItems({ queries, maxResponseBytes: 1, cursor: p1.cursor });
			expect(sksOf(p2)).toEqual(["b1"]);
			expect(p2.cursor).toBeUndefined();
		});

		it("count mode follows the same cursor as projection mode", async () => {
			const db = makeDB();
			for (const sk of ["a1", "a2", "a3"]) await db.putItem({ hashKey: "alice", sortKey: sk, data: "x" });
			for (const sk of ["b1", "b2", "b3"]) await db.putItem({ hashKey: "bob", sortKey: sk, data: "x" });

			const queries = [{ hashKey: "alice" }, { hashKey: "bob" }];
			let count = 0;
			let cursor: string | undefined;
			let pages = 0;
			for (;;) {
				const res = await db.queryItems({ queries, limit: 2, select: "count", cursor });
				count += res.count;
				pages++;
				if (res.cursor === undefined) break;
				cursor = res.cursor;
				expect(pages).toBeLessThan(50);
			}
			expect(count).toBe(6);
			expect(pages).toBeGreaterThan(1);
		});
	});

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
				).rejects.toThrow(fokosErrorWith("client_request_token_invalid", { limitBytes: 64 }));
				expect(initiateWrite).not.toHaveBeenCalled();
			} finally {
				initiateWrite.mockRestore();
			}
		});

		it("caps putItem data at the same per-item limit as a transactional put", async () => {
			const db = makeDB();
			const tooBig = new Uint8Array(MAX_ITEM_BYTES + 1);

			await expect(db.putItem({ hashKey: "big", data: tooBig })).rejects.toThrow(fokosErrorWith("item_data_too_large"));
			await expect(db.transactWriteItems({ items: [{ hashKey: "big", operation: "put", data: tooBig }] })).rejects.toThrow(
				fokosErrorWith("item_data_too_large"),
			);

			// Between the two ceilings: under the client's data check, over the store's row measure. The
			// non-transactional putItem is the only caller with no earlier pass, so the store's guard is
			// its answer, and it must leave the item absent.
			const overRow = new Uint8Array(MAX_ITEM_BYTES);
			await expect(db.putItem({ hashKey: "over-row", data: overRow })).rejects.toThrow(
				fokosErrorWith("item_too_large", { hashKey: "over-row" }),
			);
			await expect(db.getItem({ hashKey: "over-row" })).resolves.toMatchObject({ found: false });

			// Exactly at the limit is accepted by both.
			const atLimitPut = new Uint8Array(MAX_ITEM_BYTES - KeyCodec.encode("at-limit").byteLength - EST_ROW_BYTES_K);
			await expect(db.putItem({ hashKey: "at-limit", data: atLimitPut })).resolves.toMatchObject({ version: 1 });
			const atLimitTx = new Uint8Array(MAX_ITEM_BYTES - KeyCodec.encode("at-limit-tx").byteLength - EST_ROW_BYTES_K);
			await expect(
				db.transactWriteItems({ items: [{ hashKey: "at-limit-tx", operation: "put", data: atLimitTx }] }),
			).resolves.toMatchObject({ transactionId: expect.any(String) });
		});

		it("caps the transactGetItems item count like the write path", async () => {
			const db = makeDB();
			const items = Array.from({ length: MAX_ITEMS_PER_TX + 1 }, (_, i) => ({ hashKey: `k-${i}` }));
			await expect(db.transactGetItems({ items })).rejects.toThrow(fokosErrorWith("transact_items_too_many", { limit: 100 }));
			await expect(db.transactGetItems({ items: [] })).rejects.toThrow(fokosErrorWith("transact_items_empty"));
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
				await expect(db.queryItems({ queries: [{ hashKey: "hk", sortKeyCondition }] })).rejects.toThrow(
					fokosErrorWith("key_contains_nul", { key: "sortKey" }),
				);
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
// cross-sub-query fan-out and pagination, not the DO-level range-tree walk (covered in test/partition-do/query-items.test.ts).
function makeDBFor(ns: PartitionNamespaceKey, options?: { rootTreesN?: number; coordinatorRootsN?: number }) {
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
		topology: new FokosRouter(base.topology, base.rangeConfig, base.policy),
		coordinatorRootsN: options?.coordinatorRootsN,
	});
}

function sksOf(res: { items: Array<{ sortKey?: string | Uint8Array }> }) {
	return res.items.map((i) => i.sortKey);
}
