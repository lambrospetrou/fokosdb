import { env } from "cloudflare:workers";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import type { InitFromSplitOptions, PartitionDO } from "./do-partition.js";
import { PartitionContextCreator, PartitionTopologyRouterImpl } from "./partition-topology/partition-topology.js";
import type { PartitionContextResolved, SplitStatusKVItem } from "./partition-topology/partition-topology.js";

type SplitStartedOrCompleted = Extract<SplitStatusKVItem, { status: "split_started" | "split_completed" }>;

describe("PartitionDO - putItem / getItem", () => {
	it("returns found:false for a missing key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const result = await stub.getItem(ctx, { hashKey: "missing", sortKey: "sk" });
		expect(result).toEqual({
			found: false,
			meta: {
				rowsRead: 0,
				rowsWritten: 0,
				databaseSize: expect.any(Number),
				servedByActorId: expect.any(String),
				servedByActorName: expect.stringMatching(/^test\..+/),
				forwardCount: 0,
			},
		});
	});

	it("stores and retrieves a string value", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "hello" });
		const result = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });

		expect(result).toMatchObject({ found: true, hashKey: "hk", sortKey: "sk", data: "hello" });
	});

	it("stores and retrieves binary data", async ({ expect }) => {
		const { ctx, stub } = makeStub();
		const data = new Uint8Array([1, 2, 3, 4, 5]);

		await stub.putItem(ctx, { hashKey: "hk-bin", sortKey: "sk-bin", data });
		const result = await stub.getItem(ctx, { hashKey: "hk-bin", sortKey: "sk-bin" });

		expect(result).toMatchObject({ found: true, data });
	});

	it("overwrites an existing item on repeated put", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "first" });
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "second" });
		const result = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });

		expect(result).toMatchObject({ found: true, data: "second" });
	});

	it("isolates items by (hashKey, sortKey) composite key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk1", sortKey: "sk1", data: "a" });
		await stub.putItem(ctx, { hashKey: "hk1", sortKey: "sk2", data: "b" });
		await stub.putItem(ctx, { hashKey: "hk2", sortKey: "sk1", data: "c" });

		const r1 = await stub.getItem(ctx, { hashKey: "hk1", sortKey: "sk1" });
		const r2 = await stub.getItem(ctx, { hashKey: "hk1", sortKey: "sk2" });
		const r3 = await stub.getItem(ctx, { hashKey: "hk2", sortKey: "sk1" });

		expect(r1).toMatchObject({ found: true, data: "a" });
		expect(r2).toMatchObject({ found: true, data: "b" });
		expect(r3).toMatchObject({ found: true, data: "c" });
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

		expect(result).toMatchObject({ found: true, version: 2 });
	});

	it("versions are independent per (hashKey, sortKey) key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk1", data: "a" });
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk1", data: "a2" });
		const r1 = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk2", data: "b" });

		const get1 = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk1" });
		const get2 = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk2" });

		expect(r1.version).toBe(1);
		expect(get1).toMatchObject({ found: true, version: 2 });
		expect(get2).toMatchObject({ found: true, version: 1 });
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

			await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "val", ttlEpochUTCSeconds: ttl });
			const result = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });

			expect(result).toMatchObject({ found: true, ttlEpochUTCSeconds: ttl });
		});

		it("ttlEpochUTCSeconds is absent when not set on put", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "val" });
			const result = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });

			expect(result).toMatchObject({ found: true });
			if (result.found) expect(result.ttlEpochUTCSeconds).toBeUndefined();
		});

		it("clears ttlEpochUTCSeconds when an item is overwritten without TTL", async ({ expect }) => {
			const { ctx, stub } = makeStub();
			const ttl = Math.floor(Date.now() / 1000) + 3600;

			await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v1", ttlEpochUTCSeconds: ttl });
			await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v2" });
			const result = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });

			expect(result).toMatchObject({ found: true, data: "v2" });
			if (result.found) expect(result.ttlEpochUTCSeconds).toBeUndefined();
		});
	});

	it("stores and retrieves an item with no sortKey", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", data: "no-sort" });
		const result = await stub.getItem(ctx, { hashKey: "hk" });

		expect(result).toMatchObject({ found: true, hashKey: "hk", data: "no-sort" });
		if (result.found) expect(result.sortKey).toBeUndefined();
	});

	it("isolates null-sortKey items from same-hashKey items that have a sortKey", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", data: "no-sort" });
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "with-sort" });

		const r1 = await stub.getItem(ctx, { hashKey: "hk" });
		const r2 = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });
		const rMiss = await stub.getItem(ctx, { hashKey: "hk", sortKey: "other" });

		expect(r1).toMatchObject({ found: true, data: "no-sort" });
		expect(r2).toMatchObject({ found: true, data: "with-sort" });
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
			expect(get).toMatchObject({ found: true, data: "value" });
		});

		it("throws when item already exists, leaving it unchanged", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "original" });

			await runInDurableObject(stub, async (instance: PartitionDO) => {
				await expect(
					instance.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "overwrite", conditions: [{ type: "item_not_exists" }] }),
				).rejects.toThrow(/item_not_exists.*v=1.*hk=hk.*sk=sk/);
			});

			const get = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });
			expect(get).toMatchObject({ found: true, data: "original", version: 1 });
		});

		it("works when sortKey is absent", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.putItem(ctx, { hashKey: "hk", data: "original" });

			await runInDurableObject(stub, async (instance: PartitionDO) => {
				await expect(
					instance.putItem(ctx, { hashKey: "hk", data: "overwrite", conditions: [{ type: "item_not_exists" }] }),
				).rejects.toThrow("item_not_exists");
			});

			const get = await stub.getItem(ctx, { hashKey: "hk" });
			expect(get).toMatchObject({ found: true, data: "original" });
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
			expect(get).toMatchObject({ found: true, data: "v2", version: 2 });
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
						conditions: [
							{ type: "item_not_exists" },
							{ type: "attribute_equals", attribute: "v", value: 1 },
						],
					}),
				).rejects.toThrow("item_not_exists");
			});

			const get = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });
			expect(get).toMatchObject({ found: true, data: "original", version: 1 });
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
			expect(get).toMatchObject({ found: true, data: "v2", version: 2 });
		});

		it("succeeds with empty conditions array (no conditions)", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			const result = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "value", conditions: [] });

			expect(result.version).toBe(1);
		});
	});
});

describe("PartitionDO - deleteItem", () => {
	it("returns deleted:false for a missing key", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		const result = await stub.deleteItem(ctx, { hashKey: "missing", sortKey: "sk" });
		expect(result).toEqual({
			deleted: false,
			meta: {
				rowsRead: 0,
				rowsWritten: 0,
				databaseSize: expect.any(Number),
				servedByActorId: expect.any(String),
				servedByActorName: expect.stringMatching(/^test\..+/),
				forwardCount: 0,
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
		expect(await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk2" })).toMatchObject({ found: true, data: "b" });
		expect(await stub.getItem(ctx, { hashKey: "hk2", sortKey: "sk1" })).toMatchObject({ found: true, data: "c" });
	});

	it("works when sortKey is absent — deletes only the no-sortKey row", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", data: "no-sort" });
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "with-sort" });

		const result = await stub.deleteItem(ctx, { hashKey: "hk" });
		expect(result.deleted).toBe(true);

		expect((await stub.getItem(ctx, { hashKey: "hk" })).found).toBe(false);
		expect(await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" })).toMatchObject({ found: true, data: "with-sort" });
	});

	it("item can be re-created after deletion (version resets to 1)", async ({ expect }) => {
		const { ctx, stub } = makeStub();

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v1" });
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v2" });
		await stub.deleteItem(ctx, { hashKey: "hk", sortKey: "sk" });
		const result = await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "fresh" });

		expect(result.version).toBe(1);
		expect(await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" })).toMatchObject({ found: true, data: "fresh", version: 1 });
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
						instance.deleteItem(ctx, { hashKey: "hk", sortKey: "sk", conditions: [{ type: "item_exists" }] }),
					).rejects.toThrow(/item_exists.*hk=hk.*sk=sk/);
				});

				const get = await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" });
				expect(get.found).toBe(false);
			});

			it("works when sortKey is absent", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await runInDurableObject(stub, async (instance: PartitionDO) => {
					await expect(
						instance.deleteItem(ctx, { hashKey: "hk", conditions: [{ type: "item_exists" }] }),
					).rejects.toThrow("item_exists");
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

				expect(await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" })).toMatchObject({ found: true, data: "v2", version: 2 });
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
					conditions: [
						{ type: "item_exists" },
						{ type: "attribute_equals", attribute: "v", value: 1 },
					],
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
							conditions: [
								{ type: "item_exists" },
								{ type: "attribute_equals", attribute: "v", value: 1 },
							],
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
							conditions: [
								{ type: "item_exists" },
								{ type: "attribute_equals", attribute: "v", value: 1 },
							],
						}),
					).rejects.toThrow(/attribute_equals.*expected 1.*found 2/);
				});

				expect(await stub.getItem(ctx, { hashKey: "hk", sortKey: "sk" })).toMatchObject({ found: true, data: "v2", version: 2 });
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
		const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 100 } });

		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "small" });

		const { splitStatus } = await stub.status();
		expect(splitStatus).toBeUndefined();
	});

	it("sets split_pending status when data exceeds maxSizeMb", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });

		await stub.putItem(ctx, {
			hashKey: `hk.${stub.id.name!}`,
			sortKey: "sk",
			// Slightly over 1 MB to trigger the split condition.
			data: "x".repeat(1 * 1024 * 1024 + 10),
		});

		const { splitStatus } = await stub.status();
		expect(splitStatus).toBeDefined();
		expect(splitStatus?.status).toBe("split_queued");
	});

	it("preserves split_queued status across subsequent writes before the alarm fires", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });

		// Both writes run inside the DO's execution context so the background alarm
		// cannot fire between them, letting us assert the pre-alarm queued state.
		await runInDurableObject(stub, async (instance: PartitionDO) => {
			await instance.putItem(ctx, {
				hashKey: "hk",
				sortKey: "sk1",
				data: "x".repeat(1 * 1024 * 1024 + 10),
			});

			const { splitStatus: after1 } = await instance.status();
			expect(after1?.status).toBe("split_queued");

			await instance.putItem(ctx, { hashKey: "hk", sortKey: "sk2", data: "small" });

			const { splitStatus: after2 } = await instance.status();
			expect(after2?.status).toBe("split_queued");
		});
	});

	it("alarm triggers startSplit and initializes child partitions", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });
		const topologyRouter = new PartitionTopologyRouterImpl("", ctx);
		const hashKey = `hk.1`;

		await stub.putItem(ctx, {
			hashKey,
			sortKey: "sk",
			data: "x".repeat(1 * 1024 * 1024 + 10),
		});
		await waitForAlarm(stub);

		const parentState = await stub.status();
		expect(parentState.splitStatus?.status).toBe("split_started");
		expect(parentState.partitionContext).toMatchObject({ ns: "PARTITION_DO", nsPrefix: ctx.nsPrefix });

		const childNames = topologyRouter.calculateChildPartitionIds(parentState.partitionContext.partitionId, 2).map((c) => c.doName);

		// Each child should have been initialized with the parent's context and a child-specific partition context.
		for (const name of childNames) {
			const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(name));
			const childState = await childStub.status();

			expect(childState.partitionContext).toMatchObject({
				ns: "PARTITION_DO",
				nsPrefix: ctx.nsPrefix,
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
		const childCtx: PartitionContextResolved = { ...parentCtx, doName: childName, primaryDoIdStr: childId.toString() };
		const childStub = env.PARTITION_DO.get(childId);

		const opts: InitFromSplitOptions = { parentPartitionContext: parentCtx, newPartitionContext: childCtx, splitType: "hash" };

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
		const childCtx: PartitionContextResolved = { ...parentCtx, doName: childName, primaryDoIdStr: childId.toString() };
		const childStub = env.PARTITION_DO.get(childId);

		await childStub.initFromSplit({ parentPartitionContext: parentCtx, newPartitionContext: childCtx, splitType: "hash" });

		// Use runInDurableObject so the caught rejection stays inside the DO's execution context
		// and doesn't leak as an unhandled rejection at the worker level.
		const { ctx: otherCtx } = makeStub();
		await runInDurableObject(childStub, async (instance: PartitionDO) => {
			await expect(
				instance.initFromSplit({ parentPartitionContext: parentCtx, newPartitionContext: otherCtx, splitType: "hash" }),
			).rejects.toThrow("conflicting options");
		});
	});

	it("initFromSplit throws when called with a conflicting parent context", async ({ expect }) => {
		const { ctx: parentCtx } = makeStub();
		const childName = `test.initfromsplit-conflict-parent.${crypto.randomUUID()}`;
		const childId = env.PARTITION_DO.idFromName(childName);
		const childCtx: PartitionContextResolved = { ...parentCtx, doName: childName, primaryDoIdStr: childId.toString() };
		const childStub = env.PARTITION_DO.get(childId);

		await childStub.initFromSplit({ parentPartitionContext: parentCtx, newPartitionContext: childCtx, splitType: "hash" });

		const { ctx: differentParentCtx } = makeStub();
		await runInDurableObject(childStub, async (instance: PartitionDO) => {
			await expect(
				instance.initFromSplit({ parentPartitionContext: differentParentCtx, newPartitionContext: childCtx, splitType: "hash" }),
			).rejects.toThrow("conflicting options");
		});
	});

	it("initFromSplit throws when called with a conflicting splitType", async ({ expect }) => {
		const { ctx: parentCtx } = makeStub();
		const childName = `test.initfromsplit-conflict-splittype.${crypto.randomUUID()}`;
		const childId = env.PARTITION_DO.idFromName(childName);
		const childCtx: PartitionContextResolved = { ...parentCtx, doName: childName, primaryDoIdStr: childId.toString() };
		const childStub = env.PARTITION_DO.get(childId);

		await childStub.initFromSplit({ parentPartitionContext: parentCtx, newPartitionContext: childCtx, splitType: "hash" });

		await runInDurableObject(childStub, async (instance: PartitionDO) => {
			await expect(
				instance.initFromSplit({ parentPartitionContext: parentCtx, newPartitionContext: childCtx, splitType: "range" }),
			).rejects.toThrow("conflicting options");
		});
	});

	it("exposes split status via status()", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });

		await stub.putItem(ctx, {
			hashKey: `hk.${stub.id.name!}`,
			sortKey: "sk",
			data: "x".repeat(1 * 1024 * 1024 + 10),
		});

		const { splitStatus } = await stub.status();
		expect(splitStatus).toBeDefined();
		expect(splitStatus?.status).toBe("split_queued");
		expect(splitStatus?.createdAt).toBeTypeOf("number");
	});

	it("alarm with no split queued and no migration in progress does nothing", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 100 } });

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
			const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });
			const topologyRouter = new PartitionTopologyRouterImpl("", ctx);

			// Trigger the split condition and drain the tree so all migrations complete.
			await stub.putItem(ctx, { hashKey: "trigger", sortKey: "sk", data: "x".repeat(1 * 1024 * 1024 + 10) });
			await drainSplitTree(stub);

			const childNames = topologyRouter.calculateChildPartitionIds(ctx.partitionId, 2).map((c) => c.doName);

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
			const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });

			await stub.putItem(ctx, { hashKey: "trigger", sortKey: "sk", data: "x".repeat(1 * 1024 * 1024 + 10) });
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
			// Items are ~1 KB each so ~10 fill 0.2 MB and trigger a split at any given partition.
			// With splitN=3 and 100 total items, the root and multiple generations of children
			// all split, creating a deep tree that exercises the full forwarding chain.
			const ITEM_SIZE_BYTES = 10 * 1024;
			const dummyData = "x".repeat(ITEM_SIZE_BYTES);
			const TOTAL_ITEMS = 100;
			const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 3, maxSizeMb: 0.1 } });

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
						const msg = String(e);
						if (msg.includes("split in progress") || msg.includes("exceeded its limits")) {
							await drainSplitTree(stub);
						} else {
							throw e;
						}
					}
				}
			}

			// Flush any in-flight splits triggered by the last few writes.
			await drainSplitTree(stub);

			// Verify every item is reachable through the root (which forwards through the tree)
			// and record the actor name that actually served each read.
			const servedByActorNames = new Set<string>();
			for (const item of allItems) {
				const result = await stub.getItem(ctx, { hashKey: item.hashKey, sortKey: item.sortKey });
				expect(result).toMatchObject({ found: true, hashKey: item.hashKey, sortKey: item.sortKey, data: dummyData });
				if (result.found) {
					servedByActorNames.add(result.meta.servedByActorName);
					expect(result.meta.forwardCount, "root reads should have been forwarded at least once").toBeGreaterThan(2);
				}
			}

			// With 100 items and splitN=3, the tree reaches ~4 levels deep (~16 leaf DOs).
			// Even with hash skew, at least 4 distinct instances must serve reads.
			expect(servedByActorNames.size, "many distinct partition instances should have served requests").toBeGreaterThan(4);

			const totalSplitNodes = await assertSplitTreeComplete(stub);
			expect(totalSplitNodes, "multiple levels of splits should have occurred").toBeGreaterThan(2);
		});
	}, 30_000);

	describe("migration", () => {
		it("reads during migration are served from the parent; writes are rejected; all data migrated correctly", async ({ expect }) => {
			const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 10, maxSizeMb: 1 } });

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
			await stub.putItem(ctx, { hashKey: "trigger", sortKey: "sk", data: "x".repeat(1 * 1024 * 1024 + 10) });
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
				expect(result).toMatchObject({ found: true, data: item.data, meta: { forwardCount: 1 } });
			}

			// Every seed item is found in exactly one child with the correct data.
			const foundIds = new Set<string>();
			for (const item of seedItems) {
				let foundInDoName: string | undefined;
				for (const childCtx of childContexts) {
					const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(childCtx.doName));
					const result = await childStub.getItem(childCtx, { hashKey: item.hashKey, sortKey: item.sortKey });
					if (result.found) {
						expect(foundInDoName, `"${item.hashKey}/${item.sortKey}" found in multiple children`).toBeUndefined();
						expect(result).toMatchObject({ data: item.data });
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
			const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });

			await stub.putItem(ctx, { hashKey: "key1", sortKey: "sk", data: "value1" });
			await stub.putItem(ctx, { hashKey: "trigger", sortKey: "sk", data: "x".repeat(1 * 1024 * 1024 + 10) });

			// Pre-install the gate on all child partitions before the parent alarm fires.
			// The child DO names are deterministic, and miniflare keeps the same instance when
			// initFromSplit + triggerMigration are called during the parent alarm — so the hook
			// is already in place when runMigration runs, blocking it before migration_completed.
			const topologyRouter = new PartitionTopologyRouterImpl("", ctx);
			let releaseMigration!: () => void;
			const migrationGate = new Promise<void>((resolve) => {
				releaseMigration = resolve;
			});
			for (const { doName } of topologyRouter.calculateChildPartitionIds(ctx.partitionId, ctx.hashSplitConditions.splitN)) {
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
			const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });

			const seedItems = [
				{ hashKey: "alpha", sortKey: "s1", data: "data-alpha-1" },
				{ hashKey: "banana", sortKey: "s1", data: "data-banana-1" },
			];
			for (const item of seedItems) {
				await stub.putItem(ctx, item);
			}
			await stub.putItem(ctx, { hashKey: "trigger", sortKey: "sk", data: "x".repeat(1 * 1024 * 1024 + 10) });

			// Pre-install gate on all children before the parent alarm fires.
			const topologyRouter = new PartitionTopologyRouterImpl("", ctx);
			let releaseMigration!: () => void;
			const migrationGate = new Promise<void>((resolve) => {
				releaseMigration = resolve;
			});
			for (const { doName } of topologyRouter.calculateChildPartitionIds(ctx.partitionId, ctx.hashSplitConditions.splitN)) {
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
				const result = await childStub.getItem(childCtx, { hashKey: item.hashKey, sortKey: item.sortKey });
				expect(result).toMatchObject({ found: true, data: item.data });
			}

			releaseMigration();
			for (const { doName } of childContexts) {
				await waitForAlarm(env.PARTITION_DO.get(env.PARTITION_DO.idFromName(doName)));
			}
			expect((await childStub.status()).migrationStatus).toBe("migration_completed");
		});

		it("migrates all items correctly when the parent sends data in multiple cursor-paginated batches", async ({ expect }) => {
			const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });

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
			await stub.putItem(ctx, { hashKey: "trigger", sortKey: "sk", data: "x".repeat(1 * 1024 * 1024 + 10) });
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
				expect(result).toMatchObject({ found: true, meta: { forwardCount: 1 }, data: item.data });
			}

			await assertSplitTreeComplete(stub);
		});

		it("getItemDirect bypasses split forwarding and reads from local storage", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "direct-value" });

			const result = await stub.getItemDirect({ hashKey: "hk", sortKey: "sk" });
			expect(result).toMatchObject({ found: true, data: "direct-value" });

			const miss = await stub.getItemDirect({ hashKey: "missing", sortKey: "sk" });
			expect(miss.found).toBe(false);
		});
	});
});

describe("PartitionDO - partitionId encoding", () => {
	it("encodes root and child partition IDs as depth-prefixed byte arrays with text doNames", ({ expect }) => {
		const { ctx } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });
		const topologyRouter = new PartitionTopologyRouterImpl("", ctx);

		// Root: [version=0, depth=1, hashIdx=0]
		expect(Uint8Array.fromHex(ctx.partitionId)).toEqual(new Uint8Array([0, 1, 0]));

		// Children: [version=0, depth=2, hashIdx[0]=0 (root), hashIdx[1]=i]
		const children = topologyRouter.calculateChildPartitionIds(ctx.partitionId, 2);
		for (let i = 0; i < children.length; i++) {
			expect(Uint8Array.fromHex(children[i].partitionIdOpaque)).toEqual(new Uint8Array([0, 2, 0, i]));
			expect(children[i].doName).toBe(`${ctx.nsPrefix}.h.0.${i}`);
		}
	});

	it("caches _partitionIdBytes in the DO's stored partition context for root and children", async ({ expect }) => {
		const { ctx, stub } = makeStub({ hashSplitConditions: { splitN: 2, maxSizeMb: 1 } });
		const topologyRouter = new PartitionTopologyRouterImpl("", ctx);

		// After the first request, ensurePartitionContext stores the context with _partitionIdBytes populated.
		await stub.putItem(ctx, { hashKey: "hk", sortKey: "sk", data: "v" });
		const rootState = await stub.status();
		expect(rootState.partitionContext?._partitionIdBytes).toBeInstanceOf(Uint8Array);
		expect(rootState.partitionContext?._partitionIdBytes).toEqual(Uint8Array.fromHex(ctx.partitionId));

		// Trigger a split so children are initialized with their own cached bytes.
		await stub.putItem(ctx, { hashKey: "hk2", sortKey: "sk2", data: "x".repeat(1 * 1024 * 1024 + 10) });
		await waitForAlarm(stub);

		const children = topologyRouter.calculateChildPartitionIds(ctx.partitionId, 2);
		for (const child of children) {
			const childStub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(child.doName));
			const childState = await childStub.status();
			expect(childState.partitionContext?._partitionIdBytes).toBeInstanceOf(Uint8Array);
			expect(childState.partitionContext?._partitionIdBytes).toEqual(Uint8Array.fromHex(child.partitionIdOpaque));
		}
	});
});

async function waitForAlarm(stub: DurableObjectStub<PartitionDO>) {
	// The alarm is set to Date.now() in queueSplit(), so miniflare fires it automatically
	// in the background after putItem returns. runDurableObjectAlarm drains any remaining
	// scheduled alarm, but the auto-fired one may still be in progress.
	// We do this hack to ensure that the alarm is complete before we check the state, without relying on arbitrary timers.
	await runDurableObjectAlarm(stub);
	await runInDurableObject(stub, async (instance: PartitionDO) => {
		await vi.waitUntil(() => !instance.__testing__alarm_running, { timeout: 5000, interval: 100 });
	});
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

		// A child in migration_initialized has no alarm yet; any request to it transitions
		// it to migration_migrating and schedules the alarm. The error is expected.
		const childState = await childStub.status();
		if (childState.migrationStatus === "migration_initialized" || childState.migrationStatus === "migration_migrating") {
			await childStub.getItem(childCtx, { hashKey: "_", sortKey: "_" }).catch(() => {});
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
		nsPrefix: prefix,
		// For testing determinism only one root partition.
		rootTreesN: 1,
		hashSplitConditions: { splitN: 2, maxSizeMb: 100 },
		rangeSplitConditions: { splitN: 2, maxSizeMb: 500 },
		...opts,
	});

	const pCtx = new PartitionTopologyRouterImpl("", base).pickPartition("dummyHashKey");
	return { ctx: pCtx.partitionContext, stub: env.PARTITION_DO.get(pCtx.doId) };
}
