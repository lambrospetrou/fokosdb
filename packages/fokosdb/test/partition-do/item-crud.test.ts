import { runInDurableObject } from "cloudflare:test";
import { describe, it } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import { kb, makeStub } from "./helpers.js";

describe("PartitionDO - putItem / getItem", () => {
	it("returns found:false for a missing key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const result = await stub.apiGetItem(ctx, { hashKey: kb("missing"), sortKey: kb("sk") });
		expect(result).toEqual({
			found: false,
			meta: {
				rowsRead: 0,
				rowsWritten: 0,
				databaseSize: expect.any(Number),
				servedByActorId: expect.any(String),
				servedByActorName: expect.stringMatching(/^test\..+/),
				servedByPartitionId: expect.any(String),
				forwardCount: 0,
				hashDepth: 0,
				rangeDepth: 0,
				_internal: { rangeAncestors: [] },
			},
		});
	});

	it("stores and retrieves a string value", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "hello", kind: "text" as const });
		const result = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });

		expect(result).toMatchObject({
			found: true,
			item: { data: "hello", kind: "text" as const },
		});
	});

	it("stores and retrieves binary data", async ({ expect }) => {
		const { ctx, stub } = makeStub();
		const data = new Uint8Array([1, 2, 3, 4, 5]);

		await stub.apiPutItem(ctx, { hashKey: kb("hk-bin"), sortKey: kb("sk-bin"), data, kind: "bytes" });
		const result = await stub.apiGetItem(ctx, { hashKey: kb("hk-bin"), sortKey: kb("sk-bin") });

		expect(result).toMatchObject({ found: true, item: { data } });
	});

	it("overwrites an existing item on repeated put", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "first", kind: "text" as const });
		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "second", kind: "text" as const });
		const result = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });

		expect(result).toMatchObject({ found: true, item: { data: "second" } });
	});

	it("isolates items by (hashKey, sortKey) composite key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.apiPutItem(ctx, { hashKey: kb("hk1"), sortKey: kb("sk1"), data: "a", kind: "text" as const });
		await stub.apiPutItem(ctx, { hashKey: kb("hk1"), sortKey: kb("sk2"), data: "b", kind: "text" as const });
		await stub.apiPutItem(ctx, { hashKey: kb("hk2"), sortKey: kb("sk1"), data: "c", kind: "text" as const });

		const r1 = await stub.apiGetItem(ctx, { hashKey: kb("hk1"), sortKey: kb("sk1") });
		const r2 = await stub.apiGetItem(ctx, { hashKey: kb("hk1"), sortKey: kb("sk2") });
		const r3 = await stub.apiGetItem(ctx, { hashKey: kb("hk2"), sortKey: kb("sk1") });

		expect(r1).toMatchObject({ found: true, item: { data: "a" } });
		expect(r2).toMatchObject({ found: true, item: { data: "b" } });
		expect(r3).toMatchObject({ found: true, item: { data: "c" } });
	});

	it("returns version 1 on first write", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const result = await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "hello", kind: "text" as const });

		expect(result).toMatchObject({ outcome: "ok", version: 1 });
	});

	it("increments version on each subsequent write to the same key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const r1 = await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v1", kind: "text" as const });
		const r2 = await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v2", kind: "text" as const });
		const r3 = await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v3", kind: "text" as const });

		expect(r1).toMatchObject({ outcome: "ok", version: 1 });
		expect(r2).toMatchObject({ outcome: "ok", version: 2 });
		expect(r3).toMatchObject({ outcome: "ok", version: 3 });
	});

	it("getItem returns the current version", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v1", kind: "text" as const });
		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v2", kind: "text" as const });
		const result = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });

		expect(result).toMatchObject({ found: true, item: { version: 2 } });
	});

	it("versions are independent per (hashKey, sortKey) key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk1"), data: "a", kind: "text" as const });
		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk1"), data: "a2", kind: "text" as const });
		const r1 = await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk2"), data: "b", kind: "text" as const });

		const get1 = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk1") });
		const get2 = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk2") });

		expect(r1).toMatchObject({ outcome: "ok", version: 1 });
		expect(get1).toMatchObject({ found: true, item: { version: 2 } });
		expect(get2).toMatchObject({ found: true, item: { version: 1 } });
	});

	it("includes operation metrics in putItem result", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const result = await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "data", kind: "text" as const });

		expect(result.meta).toMatchObject({
			rowsRead: expect.any(Number),
			rowsWritten: expect.any(Number),
			databaseSize: expect.any(Number),
			servedByActorId: expect.any(String),
			servedByActorName: expect.stringMatching(/^test\..+/),
		});
	});

	it("includes operation metrics in getItem result", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "data", kind: "text" as const });
		const result = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });

		expect(result).toMatchObject({
			found: true,
			meta: {
				rowsRead: expect.any(Number),
				rowsWritten: expect.any(Number),
				databaseSize: expect.any(Number),
				servedByActorId: expect.any(String),
				servedByActorName: expect.stringMatching(/^test\..+/),
			},
		});
	});

	describe("TTL", () => {
		it("stores and returns ttlAt when set on put", async ({ expect }) => {
			const { ctx, stub } = makeStub();
			const ttl = Math.floor(Date.now() / 1000) + 3600;

			await stub.apiPutItem(ctx, {
				hashKey: kb("hk"),
				sortKey: kb("sk"),
				data: "val",
				ttlAt: ttl,
				kind: "text",
			});
			const result = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });

			expect(result).toMatchObject({ found: true, item: { ttlAt: ttl } });
		});

		it("ttlAt is absent when not set on put", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "val", kind: "text" as const });
			const result = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });

			expect(result).toMatchObject({ found: true });
			if (result.found) expect(result.item.ttlAt).toBeUndefined();
		});

		it("clears ttlAt when an item is overwritten without TTL", async ({ expect }) => {
			const { ctx, stub } = makeStub();
			const ttl = Math.floor(Date.now() / 1000) + 3600;

			await stub.apiPutItem(ctx, {
				hashKey: kb("hk"),
				sortKey: kb("sk"),
				data: "v1",
				ttlAt: ttl,
				kind: "text",
			});
			await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v2", kind: "text" as const });
			const result = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });

			expect(result).toMatchObject({ found: true, item: { data: "v2" } });
			if (result.found) expect(result.item.ttlAt).toBeUndefined();
		});

		it("arms a new sweep from an RPC after the previous cycle stops", async ({ expect }) => {
			const { ctx, stub } = makeStub();
			await stub.apiPutItem(ctx, {
				hashKey: kb("rpc-arm"),
				sortKey: kb("sk"),
				data: "value",
				kind: "text",
				ttlAt: Math.floor(Date.now() / 1000) + 3600,
			});
			await scheduler.wait(600);

			await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
				state.storage.sql.exec(`UPDATE items SET ttl_epoch_utc_seconds = 1`);
			});
			await scheduler.wait(600);
			await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
				expect(state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM items`).toArray()[0].n).toBe(1);
			});

			await stub.status();
			await scheduler.wait(600);
			await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
				expect(state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM items`).toArray()[0].n).toBe(0);
			});
		});
	});

	it("stores and retrieves an item with no sortKey", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb(), data: "no-sort", kind: "text" as const });
		const result = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb() });

		// The RPC response carries no keys; db.ts answers with the caller's own. The public round-trip
		// of an absent sortKey is asserted in db.test.ts.
		expect(result).toMatchObject({ found: true, item: { data: "no-sort" } });
	});

	it("isolates null-sortKey items from same-hashKey items that have a sortKey", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb(), data: "no-sort", kind: "text" as const });
		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "with-sort", kind: "text" as const });

		const r1 = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb() });
		const r2 = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });
		const rMiss = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("other") });

		expect(r1).toMatchObject({ found: true, item: { data: "no-sort" } });
		expect(r2).toMatchObject({ found: true, item: { data: "with-sort" } });
		expect(rMiss.found).toBe(false);
	});
});

describe("PartitionDO - deleteItem", () => {
	it("returns deleted:false for a missing key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const result = await stub.apiDeleteItem(ctx, { hashKey: kb("missing"), sortKey: kb("sk") });
		expect(result).toEqual({
			outcome: "ok",
			deleted: false,
			meta: {
				rowsRead: 0,
				rowsWritten: 0,
				databaseSize: expect.any(Number),
				servedByActorId: expect.any(String),
				servedByActorName: expect.stringMatching(/^test\..+/),
				servedByPartitionId: expect.any(String),
				forwardCount: 0,
				hashDepth: 0,
				rangeDepth: 0,
				_internal: {
					rangeAncestors: [],
				},
			},
		});
	});

	it("returns deleted:true and removes the item when it exists", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "hello", kind: "text" as const });
		const result = await stub.apiDeleteItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });

		expect(result).toMatchObject({ outcome: "ok", deleted: true });
		const get = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });
		expect(get.found).toBe(false);
	});

	it("is idempotent — second delete returns deleted:false", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "hello", kind: "text" as const });
		await stub.apiDeleteItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });
		const result = await stub.apiDeleteItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });

		expect(result).toMatchObject({ outcome: "ok", deleted: false });
	});

	it("only deletes the exact (hashKey, sortKey) pair, leaving siblings untouched", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk1"), data: "a", kind: "text" as const });
		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk2"), data: "b", kind: "text" as const });
		await stub.apiPutItem(ctx, { hashKey: kb("hk2"), sortKey: kb("sk1"), data: "c", kind: "text" as const });

		await stub.apiDeleteItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk1") });

		expect((await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk1") })).found).toBe(false);
		expect(await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk2") })).toMatchObject({
			found: true,
			item: { data: "b" },
		});
		expect(await stub.apiGetItem(ctx, { hashKey: kb("hk2"), sortKey: kb("sk1") })).toMatchObject({
			found: true,
			item: { data: "c" },
		});
	});

	it("works when sortKey is absent — deletes only the no-sortKey row", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb(), data: "no-sort", kind: "text" as const });
		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "with-sort", kind: "text" as const });

		const result = await stub.apiDeleteItem(ctx, { hashKey: kb("hk"), sortKey: kb() });
		expect(result).toMatchObject({ outcome: "ok", deleted: true });

		expect((await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb() })).found).toBe(false);
		expect(await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") })).toMatchObject({
			found: true,
			item: { data: "with-sort" },
		});
	});

	it("item can be re-created after deletion (version resets to 1)", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v1", kind: "text" as const });
		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v2", kind: "text" as const });
		await stub.apiDeleteItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });
		const result = await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "fresh", kind: "text" as const });

		expect(result).toMatchObject({ outcome: "ok", version: 1 });
		expect(await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") })).toMatchObject({
			found: true,
			item: { data: "fresh", version: 1 },
		});
	});

	it("includes operation metrics in deleteItem result", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "data", kind: "text" as const });
		const result = await stub.apiDeleteItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });

		expect(result.meta).toMatchObject({
			rowsRead: expect.any(Number),
			rowsWritten: expect.any(Number),
			databaseSize: expect.any(Number),
			servedByActorId: expect.any(String),
			servedByActorName: expect.stringMatching(/^test\..+/),
			servedByPartitionId: expect.any(String),
		});
	});
});
