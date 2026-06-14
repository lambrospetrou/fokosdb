import { env } from "cloudflare:workers";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import { InitFromSplitOptions, PartitionDO } from "./do-partition.js";
import { PartitionContextCreator } from "./partition-topology/partition-context.js";
import type { PartitionContextResolved } from "./partition-topology/partition-context.js";
import { PartitionIdHelper, resolveRangePartitionContext } from "./partition-topology/partition-id.js";
import { PartitionTopologyRouterImpl } from "./partition-topology/router.js";
import { HashPartitionTopologyImpl, RANGE_PROMOTION_FRACTION } from "./partition-topology/split-policy.js";
import type { SplitStatusKVItem } from "./partition-topology/split-state.js";
import invariant from "./invariant.js";

type SplitStartedOrCompleted = Extract<SplitStatusKVItem, { status: "split_started" | "split_completed" }>;

describe("PartitionDO - putItem / getItem", () => {
	it("returns found:false for a missing key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const result = await stub.getItem(ctx, { hashKey: "missing", sortKey: "sk" });
		expect(result).toEqual({
			found: false,
			item: { hashKey: "missing", sortKey: "sk" },
			meta: {
				rowsRead: 0,
				rowsWritten: 0,
				databaseSize: expect.any(Number),
				servedByActorId: expect.any(String),
				servedByActorName: expect.stringMatching(/^test\..+/),
				servedByPartitionId: expect.any(String),
				forwardCount: 0,
				hashDepth: 0,
			},
		});
	});

	it("stores and retrieves a string value", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "hello" });
		const result = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });

		expect(result).toMatchObject({
			found: true,
			item: { hashKey: "hk", sortKey: "sk", data: "hello" },
		});
	});

	it("stores and retrieves binary data", async ({ expect }) => {
		const { ctx, stub } = makeStub();
		const data = new Uint8Array([1, 2, 3, 4, 5]);

		await stub.putItem(ctx, { hashKey: "hk-bin", sortKey: "sk-bin", data });
		const result = await stub.getItem(ctx, { hashKey: "hk-bin", sortKey: "sk-bin" });

		expect(result).toMatchObject({ found: true, item: { data } });
	});

	it("overwrites an existing item on repeated put", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "first" });
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "second" });
		const result = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });

		expect(result).toMatchObject({ found: true, item: { data: "second" } });
	});

	it("isolates items by (hashKey, sortKey) composite key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk1", sortKey: "sk1", data: "a" });
		await stub.putItem(ctx, { hashKey: "hk1", sortKey: "sk2", data: "b" });
		await stub.putItem(ctx, { hashKey: "hk2", sortKey: "sk1", data: "c" });

		const r1 = await stub.getItem(ctx, { hashKey: "hk1", sortKey: "sk1" });
		const r2 = await stub.getItem(ctx, { hashKey: "hk1", sortKey: "sk2" });
		const r3 = await stub.getItem(ctx, { hashKey: "hk2", sortKey: "sk1" });

		expect(r1).toMatchObject({ found: true, item: { data: "a" } });
		expect(r2).toMatchObject({ found: true, item: { data: "b" } });
		expect(r3).toMatchObject({ found: true, item: { data: "c" } });
	});

	it("returns version 1 on first write", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const result = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "hello" });

		expect(result.version).toBe(1);
	});

	it("increments version on each subsequent write to the same key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const r1 = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v1" });
		const r2 = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v2" });
		const r3 = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v3" });

		expect(r1.version).toBe(1);
		expect(r2.version).toBe(2);
		expect(r3.version).toBe(3);
	});

	it("getItem returns the current version", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v1" });
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v2" });
		const result = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });

		expect(result).toMatchObject({ found: true, item: { version: 2 } });
	});

	it("versions are independent per (hashKey, sortKey) key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk1", data: "a" });
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk1", data: "a2" });
		const r1 = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk2", data: "b" });

		const get1 = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk1" });
		const get2 = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk2" });

		expect(r1.version).toBe(1);
		expect(get1).toMatchObject({ found: true, item: { version: 2 } });
		expect(get2).toMatchObject({ found: true, item: { version: 1 } });
	});

	it("includes operation metrics in putItem result", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const result = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "data" });

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

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "data" });
		const result = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });

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
		it("stores and returns ttlEpochUTCSeconds when set on put", async ({ expect }) => {
			const { ctx, stub } = makeStub();
			const ttl = Math.floor(Date.now() / 1000) + 3600;

			await stub.putItem(ctx, {
				hashKey: "hk",
				sortKey: "sk",
				data: "val",
				ttlEpochUTCSeconds: ttl,
			});
			const result = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });

			expect(result).toMatchObject({ found: true, item: { ttlEpochUTCSeconds: ttl } });
		});

		it("ttlEpochUTCSeconds is absent when not set on put", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "val" });
			const result = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });

			expect(result).toMatchObject({ found: true });
			if (result.found) expect(result.item.ttlEpochUTCSeconds).toBeUndefined();
		});

		it("clears ttlEpochUTCSeconds when an item is overwritten without TTL", async ({ expect }) => {
			const { ctx, stub } = makeStub();
			const ttl = Math.floor(Date.now() / 1000) + 3600;

			await stub.putItem(ctx, {
				hashKey: "hk",
				sortKey: "sk",
				data: "v1",
				ttlEpochUTCSeconds: ttl,
			});
			await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v2" });
			const result = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });

			expect(result).toMatchObject({ found: true, item: { data: "v2" } });
			if (result.found) expect(result.item.ttlEpochUTCSeconds).toBeUndefined();
		});
	});

	it("stores and retrieves an item with no sortKey", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", data: "no-sort" });
		const result = await stub.getItem(ctx, { hashKey: "hk" });

		expect(result).toMatchObject({ found: true, item: { hashKey: "hk", data: "no-sort" } });
		if (result.found) expect(result.item.sortKey).toBeUndefined();
	});

	it("isolates null-sortKey items from same-hashKey items that have a sortKey", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", data: "no-sort" });
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "with-sort" });

		const r1 = await stub.getItem(ctx, { hashKey: "hk" });
		const r2 = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });
		const rMiss = await stub.getItem(ctx, { hashKey: "hk", sortKey: "other" });

		expect(r1).toMatchObject({ found: true, item: { data: "no-sort" } });
		expect(r2).toMatchObject({ found: true, item: { data: "with-sort" } });
		expect(rMiss.found).toBe(false);
	});
});

describe("PartitionDO - conditional putItem", () => {
	describe("item_not_exists", () => {
		it("succeeds and creates the item when it does not exist", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			const result = await stub.putItem(ctx, {
				hashKey: "hk",
				sortKey: "sk",
				data: "value",
				conditions: [{ type: "item_not_exists" }],
			});

			expect(result.version).toBe(1);
			const get = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });
			expect(get).toMatchObject({ found: true, item: { data: "value" } });
		});

		it("throws when item already exists, leaving it unchanged", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await runInDurableObject(stub, async (instance: PartitionDO) => {
				await instance.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "original" });

				await expect(
					instance.putItem(ctx, {
						hashKey: "hk",
						sortKey: "sk",
						data: "overwrite",
						conditions: [{ type: "item_not_exists" }],
					}),
				).rejects.toThrow(/item_not_exists.*v=1.*hk=hk.*sk=sk/);
			});

			const get = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });
			expect(get).toMatchObject({ found: true, item: { data: "original", version: 1 } });
		});

		it("works when sortKey is absent", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.putItem(ctx, { hashKey: "hk", data: "original" });

			await runInDurableObject(stub, async (instance: PartitionDO) => {
				await expect(
					instance.putItem(ctx, {
						hashKey: "hk",
						data: "overwrite",
						conditions: [{ type: "item_not_exists" }],
					}),
				).rejects.toThrow("item_not_exists");
			});

			const get = await stub.getItem(ctx, { hashKey: "hk" });
			expect(get).toMatchObject({ found: true, item: { data: "original" } });
		});
	});

	describe("attribute_equals", () => {
		it("succeeds when v matches the expected value", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "first" });
			const result = await stub.putItem(ctx, {
				hashKey: "hk",
				sortKey: "sk",
				data: "second",
				conditions: [{ type: "attribute_equals", attribute: "v", value: 1 }],
			});

			expect(result.version).toBe(2);
		});

		it("throws when v does not match, leaving the item unchanged", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v1" });
			await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v2" }); // v is now 2

			await runInDurableObject(stub, async (instance: PartitionDO) => {
				await expect(
					instance.putItem(ctx, {
						hashKey: "hk",
						sortKey: "sk",
						data: "stale",
						conditions: [{ type: "attribute_equals", attribute: "v", value: 1 }],
					}),
				).rejects.toThrow(/attribute_equals.*"v".*expected 1.*found 2/);
			});

			const get = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });
			expect(get).toMatchObject({ found: true, item: { data: "v2", version: 2 } });
		});

		it("throws when the item does not exist (actual v is null)", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await runInDurableObject(stub, async (instance: PartitionDO) => {
				await expect(
					instance.putItem(ctx, {
						hashKey: "hk",
						sortKey: "sk",
						data: "value",
						conditions: [{ type: "attribute_equals", attribute: "v", value: 1 }],
					}),
				).rejects.toThrow(/attribute_equals.*"v".*expected 1.*found null/);
			});
		});

		it("allows sequential optimistic-concurrency updates at the correct version", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			const r1 = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v1" });
			expect(r1.version).toBe(1);

			const r2 = await stub.putItem(ctx, {
				hashKey: "hk",
				sortKey: "sk",
				data: "v2",
				conditions: [{ type: "attribute_equals", attribute: "v", value: 1 }],
			});
			expect(r2.version).toBe(2);

			const r3 = await stub.putItem(ctx, {
				hashKey: "hk",
				sortKey: "sk",
				data: "v3",
				conditions: [{ type: "attribute_equals", attribute: "v", value: 2 }],
			});
			expect(r3.version).toBe(3);
		});
	});

	describe("multiple conditions", () => {
		it("succeeds when all conditions pass", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "first" });
			const result = await stub.putItem(ctx, {
				hashKey: "hk",
				sortKey: "sk",
				data: "second",
				conditions: [
					{ type: "attribute_equals", attribute: "v", value: 1 },
					{ type: "attribute_equals", attribute: "v", value: 1 },
				],
			});

			expect(result.version).toBe(2);
		});

		it("fails on the first failing condition and does not evaluate the rest", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			// item_not_exists is listed first and will fail since the item exists.
			// attribute_equals with value=1 would pass — but we never reach it.
			await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "original" });

			await runInDurableObject(stub, async (instance: PartitionDO) => {
				await expect(
					instance.putItem(ctx, {
						hashKey: "hk",
						sortKey: "sk",
						data: "overwrite",
						conditions: [{ type: "item_not_exists" }, { type: "attribute_equals", attribute: "v", value: 1 }],
					}),
				).rejects.toThrow("item_not_exists");
			});

			const get = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });
			expect(get).toMatchObject({ found: true, item: { data: "original", version: 1 } });
		});

		it("fails on the second condition when the first passes", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v1" });
			await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v2" }); // v is now 2

			// attribute_equals v=2 passes, then attribute_equals v=1 fails.
			await runInDurableObject(stub, async (instance: PartitionDO) => {
				await expect(
					instance.putItem(ctx, {
						hashKey: "hk",
						sortKey: "sk",
						data: "overwrite",
						conditions: [
							{ type: "attribute_equals", attribute: "v", value: 2 },
							{ type: "attribute_equals", attribute: "v", value: 1 },
						],
					}),
				).rejects.toThrow(/attribute_equals.*expected 1.*found 2/);
			});

			const get = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });
			expect(get).toMatchObject({ found: true, item: { data: "v2", version: 2 } });
		});

		it("succeeds with empty conditions array (no conditions)", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			const result = await stub.putItem(ctx, {
				hashKey: "hk",
				sortKey: "sk",
				data: "value",
				conditions: [],
			});

			expect(result.version).toBe(1);
		});
	});
});

describe("PartitionDO - deleteItem", () => {
	it("returns deleted:false for a missing key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const result = await stub.deleteItem(ctx, { hashKey: "missing", sortKey: "sk" });
		expect(result).toEqual({
			item: { hashKey: "missing", sortKey: "sk" },
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
			},
		});
	});

	it("returns deleted:true and removes the item when it exists", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "hello" });
		const result = await stub.deleteItem(ctx, { hashKey: "hk", sortKey: "sk" });

		expect(result.deleted).toBe(true);
		const get = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });
		expect(get.found).toBe(false);
	});

	it("is idempotent — second delete returns deleted:false", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "hello" });
		await stub.deleteItem(ctx, { hashKey: "hk", sortKey: "sk" });
		const result = await stub.deleteItem(ctx, { hashKey: "hk", sortKey: "sk" });

		expect(result.deleted).toBe(false);
	});

	it("only deletes the exact (hashKey, sortKey) pair, leaving siblings untouched", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk1", data: "a" });
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk2", data: "b" });
		await stub.putItem(ctx, { hashKey: "hk2", sortKey: "sk1", data: "c" });

		await stub.deleteItem(ctx, { hashKey: "hk", sortKey: "sk1" });

		expect((await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk1" })).found).toBe(false);
		expect(await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk2" })).toMatchObject({
			found: true,
			item: { data: "b" },
		});
		expect(await stub.getItem(ctx, { hashKey: "hk2", sortKey: "sk1" })).toMatchObject({
			found: true,
			item: { data: "c" },
		});
	});

	it("works when sortKey is absent — deletes only the no-sortKey row", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", data: "no-sort" });
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "with-sort" });

		const result = await stub.deleteItem(ctx, { hashKey: "hk" });
		expect(result.deleted).toBe(true);

		expect((await stub.getItem(ctx, { hashKey: "hk" })).found).toBe(false);
		expect(await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" })).toMatchObject({
			found: true,
			item: { data: "with-sort" },
		});
	});

	it("item can be re-created after deletion (version resets to 1)", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v1" });
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v2" });
		await stub.deleteItem(ctx, { hashKey: "hk", sortKey: "sk" });
		const result = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "fresh" });

		expect(result.version).toBe(1);
		expect(await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" })).toMatchObject({
			found: true,
			item: { data: "fresh", version: 1 },
		});
	});

	it("includes operation metrics in deleteItem result", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "data" });
		const result = await stub.deleteItem(ctx, { hashKey: "hk", sortKey: "sk" });

		expect(result.meta).toMatchObject({
			rowsRead: expect.any(Number),
			rowsWritten: expect.any(Number),
			databaseSize: expect.any(Number),
			servedByActorId: expect.any(String),
			servedByActorName: expect.stringMatching(/^test\..+/),
			servedByPartitionId: expect.any(String),
		});
	});

	describe("conditional deleteItem", () => {
		describe("item_exists", () => {
			it("succeeds and deletes the item when it exists", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "value" });
				const result = await stub.deleteItem(ctx, {
					hashKey: "hk",
					sortKey: "sk",
					conditions: [{ type: "item_exists" }],
				});

				expect(result.deleted).toBe(true);
				expect((await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" })).found).toBe(false);
			});

			it("throws when item does not exist, making the operation a no-op", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await runInDurableObject(stub, async (instance: PartitionDO) => {
					await expect(
						instance.deleteItem(ctx, {
							hashKey: "hk",
							sortKey: "sk",
							conditions: [{ type: "item_exists" }],
						}),
					).rejects.toThrow(/item_exists.*hk=hk.*sk=sk/);
				});

				const get = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });
				expect(get.found).toBe(false);
			});

			it("works when sortKey is absent", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await runInDurableObject(stub, async (instance: PartitionDO) => {
					await expect(instance.deleteItem(ctx, { hashKey: "hk", conditions: [{ type: "item_exists" }] })).rejects.toThrow("item_exists");
				});
			});
		});

		describe("attribute_equals", () => {
			it("succeeds when v matches the expected value", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "value" });
				const result = await stub.deleteItem(ctx, {
					hashKey: "hk",
					sortKey: "sk",
					conditions: [{ type: "attribute_equals", attribute: "v", value: 1 }],
				});

				expect(result.deleted).toBe(true);
			});

			it("throws when v does not match, leaving the item untouched", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v1" });
				await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v2" }); // v is now 2

				await runInDurableObject(stub, async (instance: PartitionDO) => {
					await expect(
						instance.deleteItem(ctx, {
							hashKey: "hk",
							sortKey: "sk",
							conditions: [{ type: "attribute_equals", attribute: "v", value: 1 }],
						}),
					).rejects.toThrow(/attribute_equals.*"v".*expected 1.*found 2/);
				});

				expect(await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" })).toMatchObject({
					found: true,
					item: { data: "v2", version: 2 },
				});
			});

			it("throws when the item does not exist (actual v is null)", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await runInDurableObject(stub, async (instance: PartitionDO) => {
					await expect(
						instance.deleteItem(ctx, {
							hashKey: "hk",
							sortKey: "sk",
							conditions: [{ type: "attribute_equals", attribute: "v", value: 1 }],
						}),
					).rejects.toThrow(/attribute_equals.*"v".*expected 1.*found null/);
				});
			});
		});

		describe("multiple conditions", () => {
			it("succeeds when all conditions pass", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "value" });
				const result = await stub.deleteItem(ctx, {
					hashKey: "hk",
					sortKey: "sk",
					conditions: [{ type: "item_exists" }, { type: "attribute_equals", attribute: "v", value: 1 }],
				});

				expect(result.deleted).toBe(true);
			});

			it("fails on the first failing condition and does not evaluate the rest", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				// item_exists is listed first and will fail since no item exists.
				// attribute_equals would never be reached.
				await runInDurableObject(stub, async (instance: PartitionDO) => {
					await expect(
						instance.deleteItem(ctx, {
							hashKey: "hk",
							sortKey: "sk",
							conditions: [{ type: "item_exists" }, { type: "attribute_equals", attribute: "v", value: 1 }],
						}),
					).rejects.toThrow("item_exists");
				});
			});

			it("fails on the second condition when the first passes", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v1" });
				await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v2" }); // v is now 2

				// item_exists passes, then attribute_equals v=1 fails.
				await runInDurableObject(stub, async (instance: PartitionDO) => {
					await expect(
						instance.deleteItem(ctx, {
							hashKey: "hk",
							sortKey: "sk",
							conditions: [{ type: "item_exists" }, { type: "attribute_equals", attribute: "v", value: 1 }],
						}),
					).rejects.toThrow(/attribute_equals.*expected 1.*found 2/);
				});

				expect(await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" })).toMatchObject({
					found: true,
					item: { data: "v2", version: 2 },
				});
			});

			it("succeeds with empty conditions array (no conditions)", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "value" });
				const result = await stub.deleteItem(ctx, { hashKey: "hk", sortKey: "sk", conditions: [] });

				expect(result.deleted).toBe(true);
			});
		});
	});
});

describe("PartitionDO - splitting", () => {
	it("reports no split status before any threshold is crossed", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 100 } });

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "small" });

		const { splitStatus } = await stub.status();
		expect(splitStatus).toBeUndefined();
	});

	it("sets split_pending status when data exceeds maxSizeMb", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });

		await triggerHashSplitThreshold(stub, ctx, 1);

		const { splitStatus } = await stub.status();
		expect(splitStatus).toBeDefined();
		// By the time of the assertion the split could be in any of these states.
		expect(["split_queued", "split_started", "split_completed"]).toContain(splitStatus?.status);
	});

	it("preserves split_queued status across subsequent writes before the alarm fires", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });

		// All writes run inside the DO's execution context so the background alarm
		// cannot fire between them, letting us assert the pre-alarm queued state.
		await runInDurableObject(stub, async (instance: PartitionDO) => {
			// Spread data across multiple keys so no single key hits the promotion threshold.
			// Stop as soon as the split is queued to stay below the 10% reject band.
			const chunkBytes = Math.floor(RANGE_PROMOTION_FRACTION * 1 * 1024 * 1024 * 0.7);
			const tData = "x".repeat(chunkBytes);
			for (let i = 0; ; i++) {
				try {
					await instance.putItem(ctx, { hashKey: `split-trig-${i}`, sortKey: "sk", data: tData });
				} catch (e) {
					if (!String(e).includes("partition exceeded")) throw e;
					break;
				}
				const { splitStatus: s } = await instance.status();
				if (s?.status === "split_queued") break;
			}

			const { splitStatus: after1 } = await instance.status();
			expect(after1?.status).toBe("split_queued");

			await instance.putItem(ctx, { hashKey: "extra", sortKey: "sk2", data: "small" });

			const { splitStatus: after2 } = await instance.status();
			expect(after2?.status).toBe("split_queued");
		});
	});

	it("alarm triggers startSplit and initializes child partitions", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		const topologyRouter = new PartitionTopologyRouterImpl(ctx);

		await triggerHashSplitThreshold(stub, ctx, 1);
		await waitForAlarm(stub);

		const parentState = await stub.status();
		expect(["split_started", "split_completed"]).toContain(parentState.splitStatus?.status);
		expect(parentState.partitionContext).toMatchObject({
			ns: "PARTITION_DO",
			tableName: ctx.tableName,
		});

		const childNames = PartitionIdHelper.calculateHashChildPartitionIds(parentState.partitionContext!).map((c) => c.doName);

		// Each child should have been initialized with the parent's context and a child-specific partition context.
		for (const name of childNames) {
			const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(name));
			const childState = await childStub.status();

			expect(childState.partitionContext).toMatchObject({
				ns: "PARTITION_DO",
				tableName: ctx.tableName,
				doName: name,
			});
			expect(childState.parentPartitionContext).toMatchObject({
				doName: ctx.doName,
				primaryDoIdStr: ctx.primaryDoIdStr,
			});
			expect(childState.parentSplitType).toBe("hash");
			// Children haven't crossed any split threshold of their own.
			expect(childState.splitStatus).toBeUndefined();
		}
	});

	it("initFromSplit is idempotent when called with identical options", async ({ expect }) => {
		const { ctx: parentCtx } = makeStub();
		const childName = `test.initfromsplit-idempotent.${crypto.randomUUID()}`;
		const childId = env.PARTITION_DO.idFromName(childName);
		const childCtx: PartitionContextResolved = {
			...parentCtx,
			doName: childName,
			primaryDoIdStr: childId.toString(),
		};
		const childStub = env.PARTITION_DO.get(childId);

		const opts: InitFromSplitOptions = {
			parentPartitionContext: parentCtx,
			newPartitionContext: childCtx,
			splitType: "hash",
		};

		await childStub.initFromSplit(opts);
		// Calling again with identical opts must not throw.
		await expect(childStub.initFromSplit(opts)).resolves.not.toThrow();

		// State must reflect the first (and only) initialization.
		const state = await childStub.status();
		expect(state.partitionContext?.primaryDoIdStr).toBe(childCtx.primaryDoIdStr);
		expect(state.parentPartitionContext?.primaryDoIdStr).toBe(parentCtx.primaryDoIdStr);
		expect(state.parentSplitType).toBe("hash");
	});

	it("initFromSplit throws when called with a conflicting child context", async ({ expect }) => {
		const { ctx: parentCtx } = makeStub();
		const childName = `test.initfromsplit-conflict.${crypto.randomUUID()}`;
		const childId = env.PARTITION_DO.idFromName(childName);
		const childCtx: PartitionContextResolved = {
			...parentCtx,
			doName: childName,
			primaryDoIdStr: childId.toString(),
		};
		const childStub = env.PARTITION_DO.get(childId);

		await childStub.initFromSplit({
			parentPartitionContext: parentCtx,
			newPartitionContext: childCtx,
			splitType: "hash",
		});

		// Use runInDurableObject so the caught rejection stays inside the DO's execution context
		// and doesn't leak as an unhandled rejection at the worker level.
		const { ctx: otherCtx } = makeStub();
		await runInDurableObject(childStub, async (instance: PartitionDO) => {
			await expect(
				instance.initFromSplit({
					parentPartitionContext: parentCtx,
					newPartitionContext: otherCtx,
					splitType: "hash",
				}),
			).rejects.toThrow("conflicting options");
		});
	});

	it("initFromSplit throws when called with a conflicting parent context", async ({ expect }) => {
		const { ctx: parentCtx } = makeStub();
		const childName = `test.initfromsplit-conflict-parent.${crypto.randomUUID()}`;
		const childId = env.PARTITION_DO.idFromName(childName);
		const childCtx: PartitionContextResolved = {
			...parentCtx,
			doName: childName,
			primaryDoIdStr: childId.toString(),
		};
		const childStub = env.PARTITION_DO.get(childId);

		await childStub.initFromSplit({
			parentPartitionContext: parentCtx,
			newPartitionContext: childCtx,
			splitType: "hash",
		});

		const { ctx: differentParentCtx } = makeStub();
		await runInDurableObject(childStub, async (instance: PartitionDO) => {
			await expect(
				instance.initFromSplit({
					parentPartitionContext: differentParentCtx,
					newPartitionContext: childCtx,
					splitType: "hash",
				}),
			).rejects.toThrow("conflicting options");
		});
	});

	it("initFromSplit throws when called with a conflicting splitType", async ({ expect }) => {
		const { ctx: parentCtx } = makeStub();
		const childName = `test.initfromsplit-conflict-splittype.${crypto.randomUUID()}`;
		const childId = env.PARTITION_DO.idFromName(childName);
		const childCtx: PartitionContextResolved = {
			...parentCtx,
			doName: childName,
			primaryDoIdStr: childId.toString(),
		};
		const childStub = env.PARTITION_DO.get(childId);

		await childStub.initFromSplit({
			parentPartitionContext: parentCtx,
			newPartitionContext: childCtx,
			splitType: "hash",
		});

		await runInDurableObject(childStub, async (instance: PartitionDO) => {
			await expect(
				instance.initFromSplit({
					parentPartitionContext: parentCtx,
					newPartitionContext: childCtx,
					splitType: "range",
				}),
			).rejects.toThrow("conflicting options");
		});
	});

	it("exposes split status via status()", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });

		await triggerHashSplitThreshold(stub, ctx, 1);

		const { splitStatus } = await stub.status();
		expect(splitStatus).toBeDefined();
		// Background work may advance split past split_queued before status() is called.
		expect(["split_queued", "split_started", "split_completed"]).toContain(splitStatus?.status);
		expect(splitStatus?.createdAt).toBeTypeOf("number");
	});

	it("alarm with no split queued and no migration in progress does nothing", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 100 } });

		// Write something small — well below the split threshold — to initialize the partition context.
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "small" });

		// No split should have been queued.
		const { splitStatus: before } = await stub.status();
		expect(before).toBeUndefined();

		// Manually schedule an alarm to simulate a stale alarm (e.g. after a crash with no pending work).
		await runInDurableObject(stub, async (instance: PartitionDO, ctx: DurableObjectState) => {
			await ctx.storage.setAlarm(Date.now());
		});

		// The alarm must complete without throwing, and leave the partition unchanged.
		await expect(waitForAlarm(stub)).resolves.not.toThrow();

		const { splitStatus: after } = await stub.status();
		expect(after).toBeUndefined();
	});

	describe("forwarding during splits", async () => {
		it("forwards putItem and getItem to a child after split, reporting forwardCount=1 and consistent servedByActorName", async ({
			expect,
		}) => {
			const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });

			// Trigger the split condition and drain the tree so all migrations complete.
			await triggerHashSplitThreshold(stub, ctx, 1);
			await drainSplitTree(stub);

			const childNames = PartitionIdHelper.calculateHashChildPartitionIds(ctx).map((c) => c.doName);

			const hashKey = "forwarded-key";
			const putResult = await stub.putItem(ctx, { hashKey, sortKey: "sk", data: "val" });
			expect(putResult.meta.forwardCount).toBe(1);
			expect(putResult.meta.servedByActorName).not.toBe(ctx.doName);
			expect(childNames).toContain(putResult.meta.servedByActorName);

			const getResult = await stub.getItem(ctx, { hashKey, sortKey: "sk" });
			expect(getResult.found).toBe(true);
			expect(getResult.meta.forwardCount).toBe(1);
			// Same child serves both the write and the subsequent read.
			expect(getResult.meta.servedByActorName).toBe(putResult.meta.servedByActorName);
		});

		it("returns found:false with forwardCount=1 for a missing key looked up through root after split", async ({ expect }) => {
			const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });

			await triggerHashSplitThreshold(stub, ctx, 1);
			await drainSplitTree(stub);

			const result = await stub.getItem(ctx, { hashKey: "definitely-missing", sortKey: "sk" });
			expect(result.found).toBe(false);
			expect(result.meta.forwardCount).toBe(1);
			expect(result.meta.servedByActorName).not.toBe(ctx.doName);
		});
	});

	describe("multi-level splits", async () => {
		it("keeps all items accessible after splits at multiple tree depths (~10 items per partition trigger threshold)", async ({
			expect,
		}) => {
			// Items are ~10 KB each so ~10 fill 0.1 MB and trigger a split at any given partition.
			// With splitN=3 and 100 total items, the root and multiple generations of children
			// all split, creating a deep tree that exercises the full forwarding chain.
			const ITEM_SIZE_BYTES = 10 * 1024;
			const dummyData = "x".repeat(ITEM_SIZE_BYTES);
			const TOTAL_ITEMS = 50;
			const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 0.1 } });

			const allItems: Array<{ hashKey: string; sortKey: string; data: string }> = [];

			for (let i = 0; i < TOTAL_ITEMS; i++) {
				const hashKey = `item-${String(i).padStart(4, "0")}`;
				const sortKey = "sk";
				allItems.push({ hashKey, sortKey, data: dummyData });

				// Writes transiently fail while a split migration is in progress.
				// Drain the full split tree and retry until the write lands.
				for (let attempt = 0; attempt < 20; attempt++) {
					try {
						await stub.putItem(ctx, { hashKey, sortKey, data: dummyData });
						break;
					} catch (e: unknown) {
						expect(String(e)).toMatch(/split in progress|partition exceeded its limits/);
					}
				}
				await drainSplitTree(stub);
				// console.log("BOOM 1 - end", { item: hashKey });
			}

			// Flush any in-flight splits triggered by the last few writes.
			await drainSplitTree(stub);

			// console.log("BOOM 2");

			// Verify every item is reachable through the root (which forwards through the tree)
			// and record the actor name that actually served each read.
			const servedByActorNames = new Set<string>();
			for (const item of allItems) {
				const result = await stub.getItem(ctx, { hashKey: item.hashKey, sortKey: item.sortKey });
				expect(result).toMatchObject({
					found: true,
					item: { hashKey: item.hashKey, sortKey: item.sortKey, data: dummyData },
				});
				if (result.found) {
					servedByActorNames.add(result.meta.servedByActorName);
					// hashDepth is constant (total tree levels) whether the cache skips hops or not.
					expect(result.meta.hashDepth, "root reads should span at least 2 hash tree levels").toBeGreaterThanOrEqual(2);
					// forwardCount starts at hashDepth (cold cache, one RPC per level) and
					// converges to 1 (warm cache, single RPC directly to the leaf). The invariant
					// that proves the cache is reducing latency is forwardCount ≤ hashDepth.
					expect(result.meta.forwardCount, "topology cache should reduce RPC hops to at most one per tree level").toBeLessThanOrEqual(
						result.meta.hashDepth,
					);
					expect(result.meta.forwardCount, "always at least one RPC hop from the root").toBeGreaterThanOrEqual(1);
				}
			}

			// With 50 items and splitN=2, the tree reaches ~3 levels deep (~8 leaf DOs).
			// Even with hash skew, at least 3 distinct instances must serve reads.
			expect(servedByActorNames.size, "many distinct partition instances should have served requests").toBeGreaterThan(3);

			const totalSplitNodes = await assertSplitTreeComplete(stub);
			expect(totalSplitNodes, "multiple levels of splits should have occurred").toBeGreaterThan(2);
		});
	}, 30_000);

	describe("hash topology cache", async () => {
		// A fixed probe key used to trace a deterministic path through the tree.
		const hashKey = "probe-key";

		it("propagates hashDepth=1 after one hash split and hashDepth=2 after two", async ({ expect }) => {
			const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
			let topology: HashPartitionTopologyImpl;
			await runInDurableObject(stub, async (instance: PartitionDO, doCtx: DurableObjectState) => {
				topology = new HashPartitionTopologyImpl(ctx, doCtx);
			});
			invariant(topology!, "topology should be initialized in the DO instance");

			// Root splits into two children.
			await triggerHashSplitThreshold(stub, ctx, 1);
			await drainSplitTree(stub);

			// root → child (leaf): hashDepth=1, forwardCount=1. Cache stays cold (child returns hashDepth=0).
			const r1 = await stub.getItem(ctx, { hashKey, sortKey: "sk" });
			expect(r1.meta.hashDepth).toBe(1);
			expect(r1.meta.forwardCount).toBe(1);

			// Split the child that owns hashKey.
			const { partitionContext: childCtx } = topology.pickChildPartition(ctx, hashKey);
			const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));
			await triggerHashSplitThreshold(childStub, childCtx, 1);
			await drainSplitTree(childStub);

			// root → child → grandchild: hashDepth=2. Cache is cold so forwardCount=2 (two RPC hops).
			const r2 = await stub.getItem(ctx, { hashKey, sortKey: "sk" });
			expect(r2.meta.hashDepth).toBe(2);
			expect(r2.meta.forwardCount).toBe(2);
		});

		it("reduces forwardCount to 1 after learning a depth-2 path from the first response", async ({ expect }) => {
			const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
			let topology: HashPartitionTopologyImpl;
			await runInDurableObject(stub, async (instance: PartitionDO, doCtx: DurableObjectState) => {
				topology = new HashPartitionTopologyImpl(ctx, doCtx);
			});
			invariant(topology!, "topology should be initialized in the DO instance");

			await triggerHashSplitThreshold(stub, ctx, 1);
			await drainSplitTree(stub);
			const { partitionContext: childCtx } = topology.pickChildPartition(ctx, hashKey);
			const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));
			await triggerHashSplitThreshold(childStub, childCtx, 1);
			await drainSplitTree(childStub);

			// First request: cold cache — root→child→grandchild (two hops). Root learns depth=2.
			const r1 = await stub.getItem(ctx, { hashKey, sortKey: "sk" });
			expect(r1.meta.hashDepth).toBe(2);
			expect(r1.meta.forwardCount).toBe(2);

			// Second request: warm cache — root skips directly to grandchild (one hop).
			const r2 = await stub.getItem(ctx, { hashKey, sortKey: "sk" });
			expect(r2.meta.hashDepth).toBe(2);
			expect(r2.meta.forwardCount).toBe(1);
		});

		it("recovers from stale cache when grandchild splits: updates to depth=3 then skips directly", async ({ expect }) => {
			const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
			let topology: HashPartitionTopologyImpl;
			await runInDurableObject(stub, async (instance: PartitionDO, doCtx: DurableObjectState) => {
				topology = new HashPartitionTopologyImpl(ctx, doCtx);
			});
			invariant(topology!, "topology should be initialized in the DO instance");

			// Build a two-level tree: root → child → grandchild.
			await triggerHashSplitThreshold(stub, ctx, 1);
			await drainSplitTree(stub);
			const { partitionContext: childCtx } = topology.pickChildPartition(ctx, hashKey);
			const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));
			await triggerHashSplitThreshold(childStub, childCtx, 1);
			await drainSplitTree(childStub);

			// Warm root's cache to depth=2 with one request (root→child→grandchild).
			const r1 = await stub.getItem(ctx, { hashKey, sortKey: "sk" });
			expect(r1.meta.hashDepth).toBe(2);

			// Now split the grandchild, making it a router for great-grandchildren.
			const { partitionContext: grandchildCtx } = topology.pickDescendantHashPartition(ctx, hashKey, 2);
			const grandchildStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(grandchildCtx.doName));
			await triggerHashSplitThreshold(grandchildStub, grandchildCtx, 1);
			await drainSplitTree(grandchildStub);

			// Stale-cache request: root targets grandchild (cached depth=2) but it is now a router.
			// Grandchild forwards one more level → root receives hashDepth=1, updates cache to depth=3,
			// and returns hashDepth=3. forwardCount=2: one RPC root→grandchild + grandchild→great-grandchild.
			const r2 = await stub.getItem(ctx, { hashKey, sortKey: "sk" });
			expect(r2.meta.hashDepth).toBe(3);
			expect(r2.meta.forwardCount).toBe(2);

			// Subsequent request: root skips directly to great-grandchild (one RPC hop).
			const r3 = await stub.getItem(ctx, { hashKey, sortKey: "sk" });
			expect(r3.meta.hashDepth).toBe(3);
			expect(r3.meta.forwardCount).toBe(1);
		});
	}, 30_000);

	describe("migration", () => {
		it("reads during migration are served from the parent; writes are rejected; all data migrated correctly", async ({ expect }) => {
			const { ctx, stub } = makeStub({ hashSplitN: 10, hashSplitConditions: { maxSizeMb: 1 } });

			// Seed items with varied hash keys so they spread across children.
			const seedItems = [
				{ hashKey: "alpha", sortKey: "s1", data: "data-alpha-1" },
				{ hashKey: "alpha", sortKey: "s2", data: "data-alpha-2" },
				{ hashKey: "banana", sortKey: "s1", data: "data-banana-1" },
				{ hashKey: "cherry", sortKey: "s1", data: "data-cherry-1" },
				{ hashKey: "delta", sortKey: "s1", data: "data-delta-1" },
				{ hashKey: "echo", sortKey: "s1", data: "data-echo-1" },
			];
			for (const item of seedItems) {
				await stub.putItem(ctx, item);
			}

			// Trigger the split condition.
			await triggerHashSplitThreshold(stub, ctx, 1);
			await waitForAlarm(stub);

			const parentState = await stub.status();
			expect(parentState.splitStatus?.status).toBe("split_started");
			const childContexts = (parentState.splitStatus as SplitStartedOrCompleted).childPartitionContexts;
			expect(childContexts).toHaveLength(10);

			// Run each child's migration alarm.
			// startSplit fire-and-forget already triggered migration on each child, so their alarms
			// may already be running or complete by the time we reach here. waitForAlarm handles both.
			for (const childCtx of childContexts) {
				const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));
				await waitForAlarm(childStub);
				const state = await childStub.status();
				expect(state.migrationStatus).toBe("migration_completed");
			}

			// Parent acknowledges all children and transitions to split_completed.
			const finalParent = await stub.status();
			expect(finalParent.splitStatus?.status).toBe("split_completed");
			const finalSplit = finalParent.splitStatus as SplitStartedOrCompleted;
			expect(finalSplit.migratedChildDoNames).toHaveLength(10);

			// All migrations complete: root successfully forwards each item to the correct child.
			for (const item of seedItems) {
				const result = await stub.getItem(ctx, { hashKey: item.hashKey, sortKey: item.sortKey });
				expect(result).toMatchObject({
					found: true,
					item: { data: item.data },
					meta: { forwardCount: 1 },
				});
			}

			// Every seed item is found in exactly one child with the correct data.
			const foundIds = new Set<string>();
			for (const item of seedItems) {
				let foundInDoName: string | undefined;
				for (const childCtx of childContexts) {
					const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));
					const result = await childStub.getItem(childCtx, {
						hashKey: item.hashKey,
						sortKey: item.sortKey,
					});
					if (result.found) {
						expect(foundInDoName, `"${item.hashKey}/${item.sortKey}" found in multiple children`).toBeUndefined();
						expect(result).toMatchObject({ item: { data: item.data } });
						foundInDoName = childCtx.doName;
						foundIds.add(foundInDoName);
					}
				}
				expect(foundInDoName, `"${item.hashKey}/${item.sortKey}" not found in any child`).toBeDefined();
			}
			// This might be flaky - but ideally we should have items across more than 1 children.
			expect(foundIds.size).toBeGreaterThan(1);
		});

		it("putItem is rejected while migration is in progress", async ({ expect }) => {
			const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });

			await stub.putItem(ctx, { hashKey: "key1", sortKey: "sk", data: "value1" });
			await triggerHashSplitThreshold(stub, ctx, 1);

			// Pre-install the gate on all child partitions before the parent alarm fires.
			// The child DO names are deterministic, and miniflare keeps the same instance when
			// initFromSplit + triggerMigration are called during the parent alarm — so the hook
			// is already in place when runMigration runs, blocking it before migration_completed.
			let releaseMigration!: () => void;
			const migrationGate = new Promise<void>((resolve) => {
				releaseMigration = resolve;
			});
			for (const { doName } of PartitionIdHelper.calculateHashChildPartitionIds(ctx)) {
				await runInDurableObject(env.PARTITION_DO.get(env.PARTITION_DO.idFromName(doName)), async (instance: PartitionDO) => {
					instance.__testing__beforeMigrationComplete = () => migrationGate;
				});
			}

			await waitForAlarm(stub);

			const parentState = await stub.status();
			const childContexts = (parentState.splitStatus as SplitStartedOrCompleted).childPartitionContexts;
			const childCtx = childContexts[0];
			const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));

			// Call putItem directly on the instance (not via RPC stub) so the error stays local.
			// Going through the stub would cause workerd to log the remote throw as an uncaught
			// exception, which Vitest surfaces as an unhandled rejection even though we catch it.
			await expect(
				runInDurableObject(childStub, (instance: PartitionDO) =>
					instance.putItem(childCtx, { hashKey: "key1", sortKey: "sk", data: "new-value" }),
				),
			).rejects.toThrow("split in progress");

			releaseMigration();
			for (const { doName } of childContexts) {
				await waitForAlarm(env.PARTITION_DO.get(env.PARTITION_DO.idFromName(doName)));
			}
			expect((await childStub.status()).migrationStatus).toBe("migration_completed");
		});

		it("getItem on a child reads through to the parent while migration is in progress", async ({ expect }) => {
			const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });

			const seedItems = [
				{ hashKey: "alpha", sortKey: "s1", data: "data-alpha-1" },
				{ hashKey: "banana", sortKey: "s1", data: "data-banana-1" },
			];
			for (const item of seedItems) {
				await stub.putItem(ctx, item);
			}
			await triggerHashSplitThreshold(stub, ctx, 1);

			// Pre-install gate on all children before the parent alarm fires.
			let releaseMigration!: () => void;
			const migrationGate = new Promise<void>((resolve) => {
				releaseMigration = resolve;
			});
			for (const { doName } of PartitionIdHelper.calculateHashChildPartitionIds(ctx)) {
				await runInDurableObject(env.PARTITION_DO.get(env.PARTITION_DO.idFromName(doName)), async (instance: PartitionDO) => {
					instance.__testing__beforeMigrationComplete = () => migrationGate;
				});
			}

			await waitForAlarm(stub);

			const parentState = await stub.status();
			const childContexts = (parentState.splitStatus as SplitStartedOrCompleted).childPartitionContexts;
			const childCtx = childContexts[0];
			const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));

			// Child migration is blocked at the gate — verify it is still migrating.
			expect((await childStub.status()).migrationStatus).toBe("migration_migrating");

			// While migration is in progress, getItem on the child must read through to the parent
			// so callers can read data that has not yet been copied to the child.
			for (const item of seedItems) {
				const result = await childStub.getItem(childCtx, {
					hashKey: item.hashKey,
					sortKey: item.sortKey,
				});
				expect(result).toMatchObject({ found: true, item: { data: item.data } });
			}

			releaseMigration();
			for (const { doName } of childContexts) {
				await waitForAlarm(env.PARTITION_DO.get(env.PARTITION_DO.idFromName(doName)));
			}
			expect((await childStub.status()).migrationStatus).toBe("migration_completed");
		});

		it("migrates all items correctly when the parent sends data in multiple cursor-paginated batches", async ({ expect }) => {
			const { ctx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });

			// Items with a mix of null and non-null sort keys to exercise the null-sk cursor boundary.
			const seedItems = [
				{ hashKey: "alpha", sortKey: undefined, data: "data-alpha-nosort" },
				{ hashKey: "alpha", sortKey: "s1", data: "data-alpha-s1" },
				{ hashKey: "banana", sortKey: "s1", data: "data-banana-1" },
				{ hashKey: "cherry", sortKey: "s1", data: "data-cherry-1" },
				{ hashKey: "delta", sortKey: "s1", data: "data-delta-1" },
			];
			for (const item of seedItems) {
				await stub.putItem(ctx, item);
			}

			// Force 1-item batches on the parent so every item requires its own cursor-paginated round trip.
			// estimateItemBytes always returns >> 1 byte, so the batch fills after the first item each time.
			// This must be set before the parent alarm fires so children use the small limit when they
			// call getItemsBatch during their migration (triggered via fire-and-forget from startSplit).
			await runInDurableObject(stub, async (instance: PartitionDO) => {
				instance.__testing__migrationBatchLimitBytes = 1;
			});

			// Trigger split and run parent alarm.
			await triggerHashSplitThreshold(stub, ctx, 1);
			await waitForAlarm(stub);

			// Run all children's migrations. startSplit already triggered their alarms via fire-and-forget.
			const parentState = await stub.status();
			const childContexts = (parentState.splitStatus as SplitStartedOrCompleted).childPartitionContexts;
			for (const childCtx of childContexts) {
				const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));
				await waitForAlarm(childStub);
				const state = await childStub.status();
				expect(state.migrationStatus).toBe("migration_completed");
			}

			// Every item is reachable through root via forwarding.
			for (const item of seedItems) {
				const result = await stub.getItem(ctx, { hashKey: item.hashKey, sortKey: item.sortKey });
				expect(result).toMatchObject({
					found: true,
					meta: { forwardCount: 1 },
					item: { hashKey: item.hashKey, sortKey: item.sortKey, data: item.data },
				});
			}

			await assertSplitTreeComplete(stub);
		});

		it("getItemDirect bypasses split forwarding and reads from local storage", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "direct-value" });

			const result = await stub.getItemDirect({ hashKey: "hk", sortKey: "sk" });
			expect(result).toMatchObject({
				found: true,
				item: { hashKey: "hk", sortKey: "sk", data: "direct-value" },
			});

			const miss = await stub.getItemDirect({ hashKey: "missing", sortKey: "sk" });
			expect(miss.found).toBe(false);
		});
	});
});

describe("PartitionDO - partitionId encoding", () => {
	it("encodes root and child partition IDs with correct byte layout and text doNames", ({ expect }) => {
		// makeStub uses rootTreesN=1, so pickPartition always lands on rootIdx=0.
		const { ctx } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		const topologyRouter = new PartitionTopologyRouterImpl(ctx);

		// Root: [version=0, rootIdx=0 (2 bytes big-endian), depth=0]
		expect(Uint8Array.fromHex(ctx.partitionId)).toEqual(new Uint8Array([0, 0, 0, 0]));

		// Children: [version=0, rootIdx=0 (2 bytes), depth=1, childIdx=i]
		const children = PartitionIdHelper.calculateHashChildPartitionIds(ctx);
		for (let i = 0; i < children.length; i++) {
			expect(Uint8Array.fromHex(children[i].partitionIdOpaque)).toEqual(new Uint8Array([0, 0, 0, 1, i]));
			expect(children[i].doName).toBe(`${ctx.tableName}.h.0.${i}`);
		}
	});

	describe("PartitionIdHelper static readers", () => {
		const baseCtx = PartitionContextCreator.create({
			ns: "PARTITION_DO",
			tableName: "test.readers",
			rootTreesN: 1,
			hashSplitN: 2,
			hashSplitConditions: { maxSizeMb: 100 },
		});

		it("rootIdx reads the 2-byte big-endian root index", () => {
			expect(PartitionIdHelper.rootIdx(new Uint8Array([0, 0, 0, 0]))).toBe(0);
			expect(PartitionIdHelper.rootIdx(new Uint8Array([0, 0, 42, 0]))).toBe(42);
			expect(PartitionIdHelper.rootIdx(new Uint8Array([0, 1, 0, 0]))).toBe(256);
			// 65000 = 0xFDE8
			expect(PartitionIdHelper.rootIdx(new Uint8Array([0, 0xfd, 0xe8, 0]))).toBe(65000);
		});

		it("depth reads the sub-tree level count", () => {
			expect(PartitionIdHelper.depth(new Uint8Array([0, 0, 0, 0]))).toBe(0);
			expect(PartitionIdHelper.depth(new Uint8Array([0, 0, 0, 1, 5]))).toBe(1);
			expect(PartitionIdHelper.depth(new Uint8Array([0, 0, 0, 3, 0, 1, 2]))).toBe(3);
		});

		it("lastChildIdx reads the partition's own slot among its siblings", () => {
			expect(PartitionIdHelper.lastChildIdx(new Uint8Array([0, 0, 0, 1, 5]))).toBe(5);
			expect(PartitionIdHelper.lastChildIdx(new Uint8Array([0, 0, 0, 2, 3, 7]))).toBe(7);
			expect(PartitionIdHelper.lastChildIdx(new Uint8Array([0, 0, 0, 3, 0, 1, 2]))).toBe(2);
		});

		it("hash-only readers (rootIdx/depth/lastChildIdx) throw for non-hash schema bytes", () => {
			// Schema byte 1 = SCHEMA_RANGE_V1: valid for doName/decode but not for hash-only readers.
			const rangeBytes = new Uint8Array([1, 0, 0, 0, 0, 0]);
			expect(() => PartitionIdHelper.rootIdx(rangeBytes)).toThrow();
			expect(() => PartitionIdHelper.depth(rangeBytes)).toThrow();
			expect(() => PartitionIdHelper.lastChildIdx(rangeBytes)).toThrow();
		});

		it("doName and decode throw for unknown schema bytes (>1)", () => {
			const unknownSchema = new Uint8Array([2, 0, 0, 0]);
			expect(() => PartitionIdHelper.doName(baseCtx, unknownSchema)).toThrow();
			expect(() => PartitionIdHelper.decode(unknownSchema)).toThrow();
		});

		it("doName builds the correct DO name from partition ID bytes", () => {
			// Root-only (rootIdx=5, depth=0)
			expect(PartitionIdHelper.doName(baseCtx, new Uint8Array([0, 0, 5, 0]))).toBe("test.readers.h.5");
			// rootIdx > 255 (rootIdx=256, depth=0) — validates u16 encoding
			expect(PartitionIdHelper.doName(baseCtx, new Uint8Array([0, 1, 0, 0]))).toBe("test.readers.h.256");
			// With children (rootIdx=5, depth=2, children=[3, 7])
			expect(PartitionIdHelper.doName(baseCtx, new Uint8Array([0, 0, 5, 2, 3, 7]))).toBe("test.readers.h.5.3.7");
		});
	});

	it("pickChildPartition and makeIsCorrectChildHashPartition agree at every tree level", async ({ expect }) => {
		// This test guards the entropy consistency between the two methods.
		// If the depth offset used in one changes without the other, routing will silently
		// assign keys to different partitions than the migration check expects.
		const { ctx: pCtx, stub } = makeStub({
			hashSplitN: 4,
			hashSplitConditions: { maxSizeMb: 100 },
		});
		let topology: HashPartitionTopologyImpl;
		await runInDurableObject(stub, async (instance: PartitionDO, ctx: DurableObjectState) => {
			topology = new HashPartitionTopologyImpl(pCtx, ctx);
		});
		invariant(topology!, "topology should be initialized in the DO instance");
		const hashKey = "routing-consistency-key";

		// Depth 0 → 1: pickChildPartition must select exactly the sibling that makeIsCorrectChildHashPartition identifies.
		const { partitionContext: child } = topology.pickChildPartition(pCtx, hashKey);
		const level1Siblings = PartitionIdHelper.calculateHashChildPartitionIds(pCtx);
		for (const sib of level1Siblings) {
			const sibCtx: PartitionContextResolved = {
				...pCtx,
				doName: sib.doName,
				partitionId: sib.partitionIdOpaque,
				primaryDoIdStr: "",
			};
			expect(topology.makeIsCorrectChildHashPartition(pCtx, sibCtx)(hashKey)).toBe(sib.doName === child.doName);
		}

		// Depth 1 → 2: same invariant one level deeper.
		const { partitionContext: grandchild } = topology.pickChildPartition(child, hashKey);
		const level2Siblings = PartitionIdHelper.calculateHashChildPartitionIds(child);
		for (const sib of level2Siblings) {
			const sibCtx: PartitionContextResolved = {
				...child,
				doName: sib.doName,
				partitionId: sib.partitionIdOpaque,
				primaryDoIdStr: "",
			};
			expect(topology.makeIsCorrectChildHashPartition(child, sibCtx)(hashKey)).toBe(sib.doName === grandchild.doName);
		}
	});

	it("caches _partitionIdBytes in the DO's stored partition context for root and children", async ({ expect }) => {
		const { ctx: pCtx, stub } = makeStub({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		let topology: HashPartitionTopologyImpl;
		await runInDurableObject(stub, async (instance: PartitionDO, ctx: DurableObjectState) => {
			topology = new HashPartitionTopologyImpl(pCtx, ctx);
		});
		invariant(topology!, "topology should be initialized in the DO instance");
		// After the first request, ensurePartitionContext stores the context with _partitionIdBytes populated.
		await stub.putItem(pCtx, { hashKey: "hk", sortKey: "sk", data: "v" });
		const rootState = await stub.status();
		expect(rootState.partitionContext?._partitionIdBytes).toBeInstanceOf(Uint8Array);
		expect(rootState.partitionContext?._partitionIdBytes).toEqual(Uint8Array.fromHex(pCtx.partitionId));

		// Trigger a split so children are initialized with their own cached bytes.
		await triggerHashSplitThreshold(stub, pCtx, 1);
		await waitForAlarm(stub);

		await runInDurableObject(stub, async (instance: PartitionDO, ctx: DurableObjectState) => {
			const children = topology.childPartitionContexts();
			for (const child of children!) {
				const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(child.doName));
				const childState = await childStub.status();
				expect(childState.partitionContext?._partitionIdBytes).toBeInstanceOf(Uint8Array);
				expect(childState.partitionContext?._partitionIdBytes).toEqual(Uint8Array.fromHex(child.partitionId));
			}
		});
	});
});

async function waitForAlarm(stub: DurableObjectStub<PartitionDO>) {
	// runDurableObjectAlarm drains any pending alarm. The auto-fired alarm (from Miniflare
	// detecting an immediate schedule) may still be in progress when putItem returns.
	// We also need to wait for any background work scheduled via setTimeout (scheduleBackgroundWork),
	// which bypasses the alarm path entirely — those are tracked by __testing__backgroundWorkRunning.
	await runDurableObjectAlarm(stub);
	await runInDurableObject(stub, async (instance: PartitionDO) => {
		await vi.waitUntil(() => !instance.__testing__alarm_running && !instance.__testing__backgroundWorkRunning, {
			timeout: 5000,
			interval: 100,
		});
	});
}

/**
 * Writes enough data spread across multiple hash keys to push the DB over maxSizeMb
 * without any single hash key accumulating enough data to trigger range-key promotion
 * (which would block the hash split via mutual exclusion in shouldSplit).
 */
async function triggerHashSplitThreshold(
	stub: DurableObjectStub<PartitionDO>,
	ctx: PartitionContextResolved,
	maxSizeMb: number = 1,
): Promise<void> {
	// Use 70% of promotion threshold per key so no single key triggers promotion,
	// and the chunk fits within the 10% reject grace band for the first write past threshold.
	const chunkBytes = Math.floor(RANGE_PROMOTION_FRACTION * maxSizeMb * 1024 * 1024 * 0.7);
	const data = "x".repeat(chunkBytes);
	for (let i = 0; ; i++) {
		try {
			await stub.putItem(ctx, { hashKey: `_split_trig_${i}`, sortKey: "sk", data });
		} catch (e) {
			// "partition exceeded" means we crossed the reject band — split was queued by a prior write.
			if (!String(e).includes("partition exceeded")) throw e;
			break;
		}
		const { splitStatus } = await stub.status();
		if (splitStatus?.status === "split_queued") break;
	}
}

/**
 * Recursively drains all pending alarms in the split tree rooted at `stub`.
 * For each node: runs any pending alarm (startSplit or nothing if already done),
 * then for each child still awaiting migration triggers its alarm via a dummy request
 * (which sets the alarm and throws "split in progress" — the error is swallowed),
 * and finally recurses into each child to drain migration and any further splits.
 */
async function drainSplitTree(stub: DurableObjectStub<PartitionDO>): Promise<void> {
	await waitForAlarm(stub);
	const state = await stub.status();

	if (!state.splitStatus || state.splitStatus.status === "split_queued") return;

	const splitStatus = state.splitStatus as SplitStartedOrCompleted;
	for (const childCtx of splitStatus.childPartitionContexts) {
		const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));

		const childState = await childStub.status();
		if (childState.migrationStatus === "migration_initialized" || childState.migrationStatus === "migration_migrating") {
			await childStub.triggerMigration();
		}

		await drainSplitTree(childStub);
	}
}

/**
 * Recursively walks the split tree rooted at `nodeStub` and asserts every node that has split
 * has reached split_completed. Returns the count of split nodes (non-leaf nodes).
 */
async function assertSplitTreeComplete(nodeStub: DurableObjectStub<PartitionDO>): Promise<number> {
	const state = await nodeStub.status();
	if (!state.splitStatus) return 0;
	expect(state.splitStatus.status, `DO ${state.partitionContext?.doName} should be split_completed`).toBe("split_completed");
	const split = state.splitStatus as SplitStartedOrCompleted;
	let count = 1;
	for (const childCtx of split.childPartitionContexts) {
		count += await assertSplitTreeComplete(env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName)));
	}
	return count;
}

function makeStub(opts?: Partial<Parameters<typeof PartitionContextCreator.create>[0]>) {
	const prefix = `test.${crypto.randomUUID()}`;
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		tableName: prefix,
		// For testing determinism only one root partition.
		rootTreesN: 1,
		hashSplitN: 2,
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: 100 },
		rangeSplitConditions: { maxSizeMb: 500 },
		...opts,
	});
	const pCtxResolved = new PartitionTopologyRouterImpl(base).pickPartition("dummyHashKey");
	const stub = env.PARTITION_DO.get(pCtxResolved.doId);
	return { ctx: pCtxResolved.partitionContext, stub: stub as DurableObjectStub<PartitionDO> };
}

// ─── Promotion lifecycle ──────────────────────────────────────────────────────

// hashSplitConditions.maxSizeMb=0.1 → promotion threshold = 0.05 MB ≈ 52 KB.
// Writing 55 KB for a single key exceeds it.
const PROMOTION_TEST_MAX_SIZE_MB = 0.1;
const PROMOTION_BIG_DATA = "x".repeat(55 * 1024);

describe("PartitionDO — promotion detection and queuing", () => {
	it("detects a heavy key and immediately cuts over to 'promoting' when no locks are held", async () => {
		const { ctx, stub } = makeStub({
			hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB },
		});
		await stub.putItem(ctx, { hashKey: "alice", sortKey: "sk1", data: PROMOTION_BIG_DATA });

		await vi.waitFor(
			async () => {
				await waitForAlarm(stub);
				const s = await stub.status();
				// Migration may complete in the same background cycle as cutover, so accept 'promoted' too.
				if (!["promoting", "promoted"].includes(s.promotedKeys.find((e) => e.hashKey === "alice")?.status ?? ""))
					throw new Error("not yet promoting");
			},
			{ timeout: 5000, interval: 100 },
		);
	});

	it("does not detect any key when the DB is well below the promotion threshold", async () => {
		const { ctx, stub } = makeStub(); // default hashSplitConditions.maxSizeMb=100 → threshold 50 MB
		await stub.putItem(ctx, { hashKey: "alice", sortKey: "sk1", data: "tiny" });

		await waitForAlarm(stub); // alarm may not be set at all; waitForAlarm is a no-op if so
		const s = await stub.status();
		expect(s.promotedKeys).toHaveLength(0);
	});

	it("blocks hash split while a key is being promoted (mutual exclusion, inverse direction)", async () => {
		// After detection cuts alice over to 'promoting', shouldSplit must return null even
		// if DB is above hashSplitConditions.maxSizeMb, because alice is in-flight.
		const { ctx, stub } = makeStub({
			hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB },
		});
		await stub.putItem(ctx, { hashKey: "alice", sortKey: "sk1", data: PROMOTION_BIG_DATA });

		// Wait for alice to reach 'promoting' or 'promoted' (the fire-and-forget range-root migration
		// may race to completion within the same background cycle).
		await vi.waitFor(
			async () => {
				await waitForAlarm(stub);
				const aliceStatus = (await stub.status()).promotedKeys.find((e) => e.hashKey === "alice")?.status;
				if (!["promoting", "promoted"].includes(aliceStatus ?? "")) throw new Error("not yet");
			},
			{ timeout: 5000, interval: 100 },
		);

		// Write more data, then assert no hash split was queued. This holds in both cases:
		//  - 'promoting': mutual exclusion blocks the split even though DB exceeds maxSizeMb.
		//  - 'promoted':  alice's data is GC'd, so the DB is back under maxSizeMb and no split is warranted.
		await stub.putItem(ctx, { hashKey: "bob", sortKey: "sk1", data: "small-data" });

		const s = await stub.status();
		expect(s.splitStatus).toBeUndefined();
	});
});

describe("PartitionDO — promotion cutover deferral and routing", () => {
	it("defers cutover to 'promoting' while the key has a pending transaction lock", async () => {
		const { ctx, stub } = makeStub({
			hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB },
		});
		await stub.putItem(ctx, { hashKey: "alice", sortKey: "sk1", data: PROMOTION_BIG_DATA });

		// Lock alice/sk1 with a prepare so the lock-free check in startPromotion defers.
		const txId = crypto.randomUUID();
		const coordId = env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString();
		const lockResult = await stub.prepare(ctx, {
			transactionId: txId,
			transactionTimestamp: Date.now(),
			coordinatorDoId: coordId,
			items: [{ hashKey: "alice", sortKey: "sk1", operation: "put", data: "pending" }],
		});
		expect(lockResult.outcome).toBe("accepted");

		// Detection queued alice but cutover was deferred — key must still be 'queued'.
		await vi.waitFor(
			async () => {
				await waitForAlarm(stub);
				const s = await stub.status();
				if (s.promotedKeys.find((e) => e.hashKey === "alice")?.status !== "queued") throw new Error("not yet queued");
			},
			{ timeout: 5000, interval: 100 },
		);

		// A write to alice while 'queued' is still served locally.
		const r = await stub.putItem(ctx, { hashKey: "alice", sortKey: "sk2", data: "still-local" });
		expect(r.meta.forwardCount).toBe(0);

		// Release the lock; next background cycle should complete the cutover.
		await stub.cancel(ctx, { transactionId: txId });
		await vi.waitFor(
			async () => {
				await waitForAlarm(stub);
				const s2 = await stub.status();
				if (s2.promotedKeys.find((e) => e.hashKey === "alice")?.status !== "promoting") throw new Error("not yet promoting");
			},
			{ timeout: 5000, interval: 100 },
		);
	});

	it("forwards reads and writes to the range root after cutover ('promoting')", async () => {
		const { ctx, stub } = makeStub({
			hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB },
		});
		await stub.putItem(ctx, { hashKey: "alice", sortKey: "sk1", data: PROMOTION_BIG_DATA });

		// Wait for detection + cutover to 'promoting'.
		const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(ctx, "alice", null, null);
		const rangeRootStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rangeRootCtx.doName));
		await vi.waitFor(
			async () => {
				await waitForAlarm(stub);
				const s = await stub.status();
				// Migration may complete in the same background cycle as cutover, so accept 'promoted' too.
				if (!["promoting", "promoted"].includes(s.promotedKeys.find((e) => e.hashKey === "alice")?.status ?? ""))
					throw new Error("not yet promoting");
			},
			{ timeout: 5000, interval: 100 },
		);

		// Drain the range root migration; hash DO transitions to 'promoted'.
		await vi.waitFor(
			async () => {
				await waitForAlarm(rangeRootStub);
				const s2 = await stub.status();
				if (s2.promotedKeys.find((e) => e.hashKey === "alice")?.status !== "promoted") throw new Error("not yet promoted");
			},
			{ timeout: 5000, interval: 100 },
		);

		// Writes via the hash partition are forwarded to the range root.
		const w = await stub.putItem(ctx, { hashKey: "alice", sortKey: "sk2", data: "in-range" });
		expect(w.meta.forwardCount).toBe(1);

		// Item is in the range root.
		const g = await rangeRootStub.getItem(rangeRootCtx, { hashKey: "alice", sortKey: "sk2" });
		expect(g).toMatchObject({ found: true, item: { data: "in-range" } });
	});
});

describe("PartitionDO — transactions spanning local and promoted keys", () => {
	it("prepare+commit spanning a local key and a promoted key both commit", async () => {
		// Promote alice, leave bob local.
		const { ctx, stub } = makeStub({
			hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB },
		});
		await stub.putItem(ctx, { hashKey: "alice", sortKey: "sk1", data: PROMOTION_BIG_DATA });
		const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(ctx, "alice", null, null);
		const rangeRootStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rangeRootCtx.doName));
		await vi.waitFor(
			async () => {
				await waitForAlarm(stub);
				await waitForAlarm(rangeRootStub);
				if ((await stub.status()).promotedKeys.find((e) => e.hashKey === "alice")?.status !== "promoted")
					throw new Error("not yet promoted");
			},
			{ timeout: 5000, interval: 100 },
		);

		// Transaction touches alice/sk2 (forwarded to range root) and bob/sk1 (local).
		const txId = crypto.randomUUID();
		const coordId = env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString();
		const prepareResp = await stub.prepare(ctx, {
			transactionId: txId,
			transactionTimestamp: Date.now(),
			coordinatorDoId: coordId,
			items: [
				{ hashKey: "alice", sortKey: "sk2", operation: "put", data: "from-txn" },
				{ hashKey: "bob", sortKey: "sk1", operation: "put", data: "bob-data" },
			],
		});
		expect(prepareResp.outcome).toBe("accepted");

		await stub.commit(ctx, {
			transactionId: txId,
			transactionTimestamp: Date.now(),
			items: [
				{ hashKey: "alice", sortKey: "sk2", operation: "put", data: "from-txn" },
				{ hashKey: "bob", sortKey: "sk1", operation: "put", data: "bob-data" },
			],
		});

		// alice/sk2 must be in the range root; bob/sk1 must be local on the hash DO.
		const aliceResult = await rangeRootStub.getItem(rangeRootCtx, {
			hashKey: "alice",
			sortKey: "sk2",
		});
		expect(aliceResult).toMatchObject({ found: true, item: { data: "from-txn" } });

		const bobResult = await stub.getItem(ctx, { hashKey: "bob", sortKey: "sk1" });
		expect(bobResult).toMatchObject({ found: true, item: { data: "bob-data" } });
	});

	it("cancel via hash DO releases both local and promoted-key locks", async () => {
		const { ctx, stub } = makeStub({
			hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB },
		});
		await stub.putItem(ctx, { hashKey: "alice", sortKey: "sk1", data: PROMOTION_BIG_DATA });
		const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(ctx, "alice", null, null);
		const rangeRootStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rangeRootCtx.doName));
		await vi.waitFor(
			async () => {
				await waitForAlarm(stub);
				await waitForAlarm(rangeRootStub);
				if ((await stub.status()).promotedKeys.find((e) => e.hashKey === "alice")?.status !== "promoted")
					throw new Error("not yet promoted");
			},
			{ timeout: 5000, interval: 100 },
		);

		const txId = crypto.randomUUID();
		const coordId = env.TRANSACTION_COORDINATOR_DO.newUniqueId().toString();
		const prepareResp = await stub.prepare(ctx, {
			transactionId: txId,
			transactionTimestamp: Date.now(),
			coordinatorDoId: coordId,
			items: [
				{ hashKey: "alice", sortKey: "sk2", operation: "put", data: "alice-data" },
				{ hashKey: "bob", sortKey: "sk1", operation: "put", data: "bob-data" },
			],
		});
		expect(prepareResp.outcome).toBe("accepted");

		// Cancel via hash DO — must fan out to range root.
		await stub.cancel(ctx, { transactionId: txId });

		// Both locks must be gone — a new prepare for the same keys must succeed.
		const txId2 = crypto.randomUUID();
		const prepareResp2 = await stub.prepare(ctx, {
			transactionId: txId2,
			transactionTimestamp: Date.now() + 1,
			coordinatorDoId: coordId,
			items: [
				{ hashKey: "alice", sortKey: "sk2", operation: "put", data: "retried" },
				{ hashKey: "bob", sortKey: "sk1", operation: "put", data: "retried" },
			],
		});
		expect(prepareResp2.outcome).toBe("accepted");
		await stub.cancel(ctx, { transactionId: txId2 });
	});
});

describe("PartitionDO — hash-child migration excludes promoted keys", () => {
	it("items belonging to a promoted key are not migrated to hash split children", async () => {
		// Use maxSizeMb=1 so the per-child reject threshold (1.1MB) stays well above what any
		// child receives after migration, avoiding fragile databaseSize comparisons.
		const { ctx, stub } = makeStub({ hashSplitConditions: { maxSizeMb: 1 } });

		// Alice data exceeds the 512KB promotion threshold for maxSizeMb=1.
		const aliceData = "x".repeat(600 * 1024);
		await stub.putItem(ctx, { hashKey: "alice", sortKey: "sk1", data: aliceData });

		// Wait for: detect → 'promoting' → range-root migration → 'promoted' → GC clears local alice items.
		const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(ctx, "alice", null, null);
		const rangeRootStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rangeRootCtx.doName));
		await vi.waitFor(
			async () => {
				await waitForAlarm(stub);
				await waitForAlarm(rangeRootStub);
				const local = await stub.getItemDirect({ hashKey: "alice", sortKey: "sk1" });
				if (local.found) throw new Error("alice not GC'd from hash DO yet");
			},
			{ timeout: 8000, interval: 100 },
		);

		// Trigger hash split with spread data; none exceeds the per-key promotion threshold.
		await triggerHashSplitThreshold(stub, ctx, 1);

		// Background: alice='promoted' so mutual exclusion allows the hash split to proceed.
		await drainSplitTree(stub);
		await assertSplitTreeComplete(stub);

		// alice's DATA must not be migrated into any hash child (the child inherits only the forward-pointer
		// entry, never the data — which lives in the range structure). The child that owns alice must:
		//   (a) hold no local copy of alice's data (strictly-local getItemDirect → not found), and
		//   (b) inherit alice's promoted-key entry, so a normal read forwards to the range structure.
		const splitStatus = (await stub.status()).splitStatus as SplitStartedOrCompleted;
		expect(splitStatus, "hash split should have completed").toBeDefined();
		for (const childCtx of splitStatus.childPartitionContexts) {
			const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));
			const local = await childStub.getItemDirect({ hashKey: "alice", sortKey: "sk1" });
			expect(local.found, `alice's data must not be migrated locally into hash child ${childCtx.doName}`).toBe(false);
		}

		// Exactly one hash child inherited alice's promoted-key entry; reading alice through THAT child
		// forwards to the range structure (the range root serves it), proving the inherited forward-pointer
		// works. Reading via the parent would forward through the parent's own cache and bypass the child,
		// so we read on the child directly to actually exercise inheritance.
		const aliceChildCtxs = [] as typeof splitStatus.childPartitionContexts;
		for (const childCtx of splitStatus.childPartitionContexts) {
			const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));
			const pk = (await childStub.status(childCtx)).promotedKeys;
			if (pk.some((e: { hashKey: string; status: string }) => e.hashKey === "alice" && e.status === "promoted"))
				aliceChildCtxs.push(childCtx);
		}
		expect(aliceChildCtxs, "exactly one hash child should inherit alice's promoted entry").toHaveLength(1);

		const aliceChildCtx = aliceChildCtxs[0];
		const aliceChildStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(aliceChildCtx.doName));
		const aliceRead = await aliceChildStub.getItem(aliceChildCtx, {
			hashKey: "alice",
			sortKey: "sk1",
		});
		expect(aliceRead.found, "alice must be reachable through its hash child via the inherited forward-pointer").toBe(true);
		expect(aliceRead.meta.forwardCount).toBeGreaterThanOrEqual(1);
		expect(aliceRead.meta.servedByActorName, "alice must be served by the range structure, not the hash child").toBe(rangeRootCtx.doName);

		// A split-trigger key must be reachable via the hash DO (forwarded to the owning child).
		const trigResult = await stub.getItem(ctx, { hashKey: "_split_trig_0", sortKey: "sk" });
		expect(trigResult).toMatchObject({
			found: true,
			item: { hashKey: "_split_trig_0", sortKey: "sk" },
			meta: { forwardCount: 1 },
		});
	});
});

// ─── Range split ──────────────────────────────────────────────────────────────

// A 1 MB threshold keeps SQLite page overhead negligible vs. item data, so the post-split children
// (≈ total/N each) stay well under the limit and remain leaves (no surprise re-split or size reject).
const RANGE_SPLIT_MAX_SIZE_MB = 1;
const RANGE_ITEM_DATA = "x".repeat(50 * 1024); // ~50 KB/item → ~21 items cross the 1 MB threshold

// Builds a range-structure leaf owning [−∞, +∞) (parent = a hash DO), migration-complete so it serves
// locally, then writes distinct-sk items until a range split is queued. No retain-leftmost: on split it
// becomes a pure router over N fresh children.
async function makeQueuedRangeRoot(rangeSplitN: number): Promise<{
	rootCtx: PartitionContextResolved;
	rootStub: DurableObjectStub<PartitionDO>;
	sks: string[];
}> {
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		tableName: `rangesplit.${crypto.randomUUID()}`,
		rootTreesN: 1,
		hashSplitN: 2,
		rangeSplitN,
		hashSplitConditions: { maxSizeMb: 100 },
		rangeSplitConditions: { maxSizeMb: RANGE_SPLIT_MAX_SIZE_MB },
	});
	const hashParentCtx = new PartitionTopologyRouterImpl(base).pickPartition("alice").partitionContext;
	const { partitionContext: rootCtx } = resolveRangePartitionContext(hashParentCtx, "alice", null, null);
	const rootStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rootCtx.doName));

	// Initialize as a ready leaf (migration complete → serves locally rather than 503).
	await rootStub.initFromSplit(
		{ parentPartitionContext: hashParentCtx, newPartitionContext: rootCtx, splitType: "range" },
		true, // __testing__completeMigration
	);

	const sks: string[] = [];
	for (let i = 0; i < 100; i++) {
		const sk = `sk${String(i).padStart(3, "0")}`;
		await rootStub.putItem(rootCtx, { hashKey: "alice", sortKey: sk, data: RANGE_ITEM_DATA });
		sks.push(sk);
		if ((await rootStub.status()).splitStatus?.status === "split_queued") break;
	}
	return { rootCtx, rootStub, sks };
}

describe("PartitionDO — range split", () => {
	it("splits a populated leaf into N contiguous children covering [−∞, +∞); the node becomes a pure router", async () => {
		const N = 4;
		const { rootCtx, rootStub, sks } = await makeQueuedRangeRoot(N);
		expect(sks.length).toBeGreaterThanOrEqual(N);

		await vi.waitFor(
			async () => {
				await drainSplitTree(rootStub);
				const s = await rootStub.status(rootCtx);
				return s.splitStatus?.status === "split_completed" ? Promise.resolve() : Promise.reject(new Error("Split not completed yet"));
			},
			{ timeout: 5000, interval: 100 },
		);

		const status = (await rootStub.status()).splitStatus as SplitStartedOrCompleted;
		expect(status.status).toBe("split_completed");
		expect(status.childPartitionContexts).toHaveLength(N);

		// Children tile [−∞, +∞): sorted by start, first.start = null, last.end = null, end[i] === start[i+1].
		const children = [...status.childPartitionContexts].sort((a, b) =>
			(a.rangePartition!.startBoundary ?? "") < (b.rangePartition!.startBoundary ?? "") ? -1 : 1,
		);
		expect(children[0].rangePartition!.startBoundary).toBeNull();
		expect(children[N - 1].rangePartition!.endBoundary).toBeNull();
		for (let i = 0; i < N - 1; i++) {
			expect(children[i].rangePartition!.endBoundary).not.toBeNull();
			expect(children[i].rangePartition!.endBoundary).toBe(children[i + 1].rangePartition!.startBoundary);
		}

		// Every item is still readable through the router, and each read forwards exactly once.
		for (const sk of sks) {
			const g = await rootStub.getItem(rootCtx, { hashKey: "alice", sortKey: sk });
			expect(g.found, `sk ${sk} readable through router`).toBe(true);
			expect(g.meta.forwardCount).toBe(1);
		}
	});

	it("partitions every sort key into exactly one child and the router serves each via that child", async () => {
		const N = 4;
		const { rootCtx, rootStub, sks } = await makeQueuedRangeRoot(N);
		await vi.waitFor(
			async () => {
				await drainSplitTree(rootStub);
				const s = await rootStub.status();
				if (s.splitStatus?.status !== "split_completed") throw new Error("split not completed yet");
			},
			{ timeout: 5000, interval: 100 },
		);
		const status = (await rootStub.status()).splitStatus as SplitStartedOrCompleted;

		for (const sk of sks) {
			// The N children form a total partition of the sort-key axis: each written sk is owned by exactly one.
			const owners = status.childPartitionContexts.filter((c) => {
				const start = c.rangePartition!.startBoundary ?? "";
				const end = c.rangePartition!.endBoundary;
				return sk >= start && (end === null || sk < end);
			});
			expect(owners, `sk ${sk} must be owned by exactly one child`).toHaveLength(1);

			// Reading through the router resolves to that exact child (servedByActorName = owning child's doName).
			const g = await rootStub.getItem(rootCtx, { hashKey: "alice", sortKey: sk });
			expect(g.found).toBe(true);
			expect(g.meta.servedByActorName, `sk ${sk} should be served by its owning child`).toBe(owners[0].doName);
		}
	});

	it("creates a brand-new leftmost child distinct from the router (no retain-leftmost)", async () => {
		const { rootCtx, rootStub } = await makeQueuedRangeRoot(4);
		await vi.waitFor(
			async () => {
				await drainSplitTree(rootStub);
				const s = await rootStub.status();
				if (s.splitStatus?.status !== "split_completed") throw new Error("split not completed yet");
			},
			{ timeout: 5000, interval: 100 },
		);
		const status = (await rootStub.status()).splitStatus as SplitStartedOrCompleted;

		const leftmost = status.childPartitionContexts.find((c) => c.rangePartition!.startBoundary === null);
		expect(leftmost, "a leftmost child [−∞, B1) must exist").toBeDefined();
		// The router keeps no slice: the leftmost child is a different DO than the splitting node.
		expect(leftmost!.doName).not.toBe(rootCtx.doName);
	});

	describe("PartialRangeTopology", () => {
		it("reduces getItem forwardCount from 2 to 1 on second access when bloom filter has learned a key promoted from a hash-depth-2 leaf", async () => {
			const hashKey = "probe-key";
			const { ctx, stub } = makeStub({
				hashSplitN: 2,
				hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB },
			});
			let topology: HashPartitionTopologyImpl;
			await runInDurableObject(stub, async (instance: PartitionDO, doCtx: DurableObjectState) => {
				topology = new HashPartitionTopologyImpl(ctx, doCtx);
			});
			invariant(topology!, "topology should be initialized in the DO instance");

			// Build a two-level hash tree: root → child → grandchild (leaf at depth=2).
			await triggerHashSplitThreshold(stub, ctx, PROMOTION_TEST_MAX_SIZE_MB);
			await drainSplitTree(stub);
			const { partitionContext: childCtx } = topology.pickChildPartition(ctx, hashKey);
			const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));
			await triggerHashSplitThreshold(childStub, childCtx, PROMOTION_TEST_MAX_SIZE_MB);
			await drainSplitTree(childStub);

			// Warm the root's hash topology cache so it can reach the depth-2 leaf in a single hop.
			// Cold: root→child→leaf (forwardCount=2). Warm: root→leaf directly (forwardCount=1).
			const rCold = await stub.getItem(ctx, { hashKey, sortKey: "sk1" });
			expect(rCold.meta.forwardCount).toBe(2);
			const rWarm = await stub.getItem(ctx, { hashKey, sortKey: "sk1" });
			expect(rWarm.meta.forwardCount).toBe(1);

			// Promote hashKey on the leaf (depth=2) that owns it.
			const { partitionContext: leafCtx } = topology.pickDescendantHashPartition(ctx, hashKey, 2);
			const leafStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(leafCtx.doName));
			await leafStub.putItem(leafCtx, { hashKey, sortKey: "sk1", data: PROMOTION_BIG_DATA });

			// Wait for promotion to complete: leaf detects heavy key → cutover ("promoting") →
			// range root migrates data → leaf acknowledges ("promoted").
			const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(leafCtx, hashKey, null, null);
			const rangeRootStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(rangeRootCtx.doName));
			await vi.waitFor(
				async () => {
					await waitForAlarm(leafStub);
					const s = await leafStub.status();
					if (
						!["promoting", "promoted"].includes(
							s.promotedKeys.find((e: { hashKey: string; status: string }) => e.hashKey === hashKey)?.status ?? "",
						)
					)
						throw new Error("not yet promoting");
				},
				{ timeout: 5000, interval: 100 },
			);
			await vi.waitFor(
				async () => {
					await waitForAlarm(rangeRootStub);
					if (
						(await leafStub.status()).promotedKeys.find((e: { hashKey: string; status: string }) => e.hashKey === hashKey)?.status !==
						"promoted"
					)
						throw new Error("not yet promoted");
				},
				{ timeout: 5000, interval: 100 },
			);

			// First getItem through root after promotion:
			// Topology cache is warm → root goes directly to the leaf (1 hash hop).
			// Leaf's PromotionManager says "promoted" → forwards to range root (1 more hop).
			// Total forwardCount=2. Root also learns hashKey in its PartialRangeTopology bloom filter.
			const r1 = await stub.getItem(ctx, { hashKey, sortKey: "sk1" });
			expect(r1.found).toBe(true);
			expect(r1.meta.forwardCount).toBe(2);

			// Second getItem through root:
			// Bloom filter now has hashKey → root bypasses the hash tree and goes directly to
			// the range root (1 hop) instead of the usual 2 hops (leaf → range root).
			const r2 = await stub.getItem(ctx, { hashKey, sortKey: "sk1" });
			expect(r2.found).toBe(true);
			expect(r2.meta.forwardCount).toBe(1);
		}, 30_000);
	});
});
