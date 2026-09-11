import { runInDurableObject } from "cloudflare:test";
import { describe, it } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import { compiledCondition, kb, makeStub } from "./helpers.js";

describe("PartitionDO - conditional putItem", () => {
	describe("item_not_exists", () => {
		it("succeeds and creates the item when it does not exist", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			const result = await stub.apiPutItem(ctx, {
				hashKey: kb("hk"),
				sortKey: kb("sk"),
				data: "value",
				condition: compiledCondition({ op: "not_exists", args: [{ ref: "hashKey" }] }),
				kind: "text",
			});

			expect(result).toMatchObject({ outcome: "ok", version: 1 });
			const get = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });
			expect(get).toMatchObject({ found: true, item: { data: "value" } });
		});

		it("rejects when item already exists, leaving it unchanged", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await runInDurableObject(stub, async (instance: PartitionDO) => {
				await instance.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "original", kind: "text" as const });

				const res = await instance.apiPutItem(ctx, {
					hashKey: kb("hk"),
					sortKey: kb("sk"),
					data: "overwrite",
					condition: compiledCondition({ op: "not_exists", args: [{ ref: "hashKey" }] }),
					kind: "text",
				});
				expect(res).toMatchObject({ outcome: "rejected", reason: { code: "condition_failed" } });
			});

			const get = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });
			expect(get).toMatchObject({ found: true, item: { data: "original", version: 1 } });
		});

		it("works when sortKey is absent", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb(), data: "original", kind: "text" as const });

			await runInDurableObject(stub, async (instance: PartitionDO) => {
				const res = await instance.apiPutItem(ctx, {
					hashKey: kb("hk"),
					sortKey: kb(),
					data: "overwrite",
					condition: compiledCondition({ op: "not_exists", args: [{ ref: "hashKey" }] }),
					kind: "text",
				});
				expect(res).toMatchObject({ outcome: "rejected", reason: { code: "condition_failed" } });
			});

			const get = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb() });
			expect(get).toMatchObject({ found: true, item: { data: "original" } });
		});
	});

	describe("attribute_equals", () => {
		it("succeeds when v matches the expected value", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "first", kind: "text" as const });
			const result = await stub.apiPutItem(ctx, {
				hashKey: kb("hk"),
				sortKey: kb("sk"),
				data: "second",
				condition: compiledCondition({ op: "eq", args: [{ ref: "v" }, { val: 1 }] }),
				kind: "text",
			});

			expect(result).toMatchObject({ outcome: "ok", version: 2 });
		});

		it("rejects when v does not match, leaving the item unchanged", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v1", kind: "text" as const });
			await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v2", kind: "text" as const }); // v is now 2

			await runInDurableObject(stub, async (instance: PartitionDO) => {
				const res = await instance.apiPutItem(ctx, {
					hashKey: kb("hk"),
					sortKey: kb("sk"),
					data: "stale",
					condition: compiledCondition({ op: "eq", args: [{ ref: "v" }, { val: 1 }] }),
					kind: "text",
				});
				expect(res).toMatchObject({ outcome: "rejected", reason: { code: "condition_failed" } });
			});

			const get = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });
			expect(get).toMatchObject({ found: true, item: { data: "v2", version: 2 } });
		});

		it("rejects when the item does not exist (actual v is null)", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await runInDurableObject(stub, async (instance: PartitionDO) => {
				const res = await instance.apiPutItem(ctx, {
					hashKey: kb("hk"),
					sortKey: kb("sk"),
					data: "value",
					condition: compiledCondition({ op: "eq", args: [{ ref: "v" }, { val: 1 }] }),
					kind: "text",
				});
				expect(res).toMatchObject({ outcome: "rejected", reason: { code: "condition_failed" } });
			});
		});

		it("allows sequential optimistic-concurrency updates at the correct version", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			const r1 = await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v1", kind: "text" as const });
			expect(r1).toMatchObject({ outcome: "ok", version: 1 });

			const r2 = await stub.apiPutItem(ctx, {
				hashKey: kb("hk"),
				sortKey: kb("sk"),
				data: "v2",
				condition: compiledCondition({ op: "eq", args: [{ ref: "v" }, { val: 1 }] }),
				kind: "text",
			});
			expect(r2).toMatchObject({ outcome: "ok", version: 2 });

			const r3 = await stub.apiPutItem(ctx, {
				hashKey: kb("hk"),
				sortKey: kb("sk"),
				data: "v3",
				condition: compiledCondition({ op: "eq", args: [{ ref: "v" }, { val: 2 }] }),
				kind: "text",
			});
			expect(r3).toMatchObject({ outcome: "ok", version: 3 });
		});
	});

	describe("multiple conditions", () => {
		it("succeeds when all conditions pass", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "first", kind: "text" as const });
			const result = await stub.apiPutItem(ctx, {
				hashKey: kb("hk"),
				sortKey: kb("sk"),
				data: "second",
				condition: compiledCondition({
					op: "and",
					args: [
						{ op: "eq", args: [{ ref: "v" }, { val: 1 }] },
						{ op: "eq", args: [{ ref: "v" }, { val: 1 }] },
					],
				}),
				kind: "text",
			});

			expect(result).toMatchObject({ outcome: "ok", version: 2 });
		});

		it("fails on the first failing condition and does not evaluate the rest", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			// item_not_exists is listed first and will fail since the item exists.
			// attribute_equals with value=1 would pass — but we never reach it.
			await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "original", kind: "text" as const });

			await runInDurableObject(stub, async (instance: PartitionDO) => {
				const res = await instance.apiPutItem(ctx, {
					hashKey: kb("hk"),
					sortKey: kb("sk"),
					data: "overwrite",
					condition: compiledCondition({
						op: "and",
						args: [
							{ op: "not_exists", args: [{ ref: "hashKey" }] },
							{ op: "eq", args: [{ ref: "v" }, { val: 1 }] },
						],
					}),
					kind: "text",
				});
				expect(res).toMatchObject({ outcome: "rejected", reason: { code: "condition_failed" } });
			});

			const get = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });
			expect(get).toMatchObject({ found: true, item: { data: "original", version: 1 } });
		});

		it("fails on the second condition when the first passes", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v1", kind: "text" as const });
			await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v2", kind: "text" as const }); // v is now 2

			// attribute_equals v=2 passes, then attribute_equals v=1 fails.
			await runInDurableObject(stub, async (instance: PartitionDO) => {
				const res = await instance.apiPutItem(ctx, {
					hashKey: kb("hk"),
					sortKey: kb("sk"),
					data: "overwrite",
					condition: compiledCondition({
						op: "and",
						args: [
							{ op: "eq", args: [{ ref: "v" }, { val: 2 }] },
							{ op: "eq", args: [{ ref: "v" }, { val: 1 }] },
						],
					}),
					kind: "text",
				});
				expect(res).toMatchObject({ outcome: "rejected", reason: { code: "condition_failed" } });
			});

			const get = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });
			expect(get).toMatchObject({ found: true, item: { data: "v2", version: 2 } });
		});

		it("succeeds with empty conditions array (no conditions)", async ({ expect }) => {
			const { ctx, stub } = makeStub();

			const result = await stub.apiPutItem(ctx, {
				hashKey: kb("hk"),
				sortKey: kb("sk"),
				data: "value",
				kind: "text",
			});

			expect(result).toMatchObject({ outcome: "ok", version: 1 });
		});
	});
});

describe("PartitionDO - deleteItem", () => {
	describe("conditional deleteItem", () => {
		describe("item_exists", () => {
			it("succeeds and deletes the item when it exists", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "value", kind: "text" as const });
				const result = await stub.apiDeleteItem(ctx, {
					hashKey: kb("hk"),
					sortKey: kb("sk"),
					condition: compiledCondition({ op: "exists", args: [{ ref: "hashKey" }] }),
				});

				expect(result).toMatchObject({ outcome: "ok", deleted: true });
				expect((await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") })).found).toBe(false);
			});

			it("rejects when item does not exist, making the operation a no-op", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await runInDurableObject(stub, async (instance: PartitionDO) => {
					const res = await instance.apiDeleteItem(ctx, {
						hashKey: kb("hk"),
						sortKey: kb("sk"),
						condition: compiledCondition({ op: "exists", args: [{ ref: "hashKey" }] }),
					});
					expect(res).toMatchObject({ outcome: "rejected", reason: { code: "condition_failed" } });
				});

				const get = await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });
				expect(get.found).toBe(false);
			});

			it("works when sortKey is absent", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await runInDurableObject(stub, async (instance: PartitionDO) => {
					const res = await instance.apiDeleteItem(ctx, {
						hashKey: kb("hk"),
						sortKey: kb(),
						condition: compiledCondition({ op: "exists", args: [{ ref: "hashKey" }] }),
					});
					expect(res).toMatchObject({ outcome: "rejected", reason: { code: "condition_failed" } });
				});
			});
		});

		describe("attribute_equals", () => {
			it("succeeds when v matches the expected value", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "value", kind: "text" as const });
				const result = await stub.apiDeleteItem(ctx, {
					hashKey: kb("hk"),
					sortKey: kb("sk"),
					condition: compiledCondition({ op: "eq", args: [{ ref: "v" }, { val: 1 }] }),
				});

				expect(result).toMatchObject({ outcome: "ok", deleted: true });
			});

			it("rejects when v does not match, leaving the item untouched", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v1", kind: "text" as const });
				await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v2", kind: "text" as const }); // v is now 2

				await runInDurableObject(stub, async (instance: PartitionDO) => {
					const res = await instance.apiDeleteItem(ctx, {
						hashKey: kb("hk"),
						sortKey: kb("sk"),
						condition: compiledCondition({ op: "eq", args: [{ ref: "v" }, { val: 1 }] }),
					});
					expect(res).toMatchObject({ outcome: "rejected", reason: { code: "condition_failed" } });
				});

				expect(await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") })).toMatchObject({
					found: true,
					item: { data: "v2", version: 2 },
				});
			});

			it("rejects when the item does not exist (actual v is null)", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await runInDurableObject(stub, async (instance: PartitionDO) => {
					const res = await instance.apiDeleteItem(ctx, {
						hashKey: kb("hk"),
						sortKey: kb("sk"),
						condition: compiledCondition({ op: "eq", args: [{ ref: "v" }, { val: 1 }] }),
					});
					expect(res).toMatchObject({ outcome: "rejected", reason: { code: "condition_failed" } });
				});
			});
		});

		describe("multiple conditions", () => {
			it("succeeds when all conditions pass", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "value", kind: "text" as const });
				const result = await stub.apiDeleteItem(ctx, {
					hashKey: kb("hk"),
					sortKey: kb("sk"),
					condition: compiledCondition({
						op: "and",
						args: [
							{ op: "exists", args: [{ ref: "hashKey" }] },
							{ op: "eq", args: [{ ref: "v" }, { val: 1 }] },
						],
					}),
				});

				expect(result).toMatchObject({ outcome: "ok", deleted: true });
			});

			it("fails on the first failing condition and does not evaluate the rest", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				// item_exists is listed first and will fail since no item exists.
				// attribute_equals would never be reached.
				await runInDurableObject(stub, async (instance: PartitionDO) => {
					const res = await instance.apiDeleteItem(ctx, {
						hashKey: kb("hk"),
						sortKey: kb("sk"),
						condition: compiledCondition({
							op: "and",
							args: [
								{ op: "exists", args: [{ ref: "hashKey" }] },
								{ op: "eq", args: [{ ref: "v" }, { val: 1 }] },
							],
						}),
					});
					expect(res).toMatchObject({ outcome: "rejected", reason: { code: "condition_failed" } });
				});
			});

			it("fails on the second condition when the first passes", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v1", kind: "text" as const });
				await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "v2", kind: "text" as const }); // v is now 2

				// item_exists passes, then attribute_equals v=1 fails.
				await runInDurableObject(stub, async (instance: PartitionDO) => {
					const res = await instance.apiDeleteItem(ctx, {
						hashKey: kb("hk"),
						sortKey: kb("sk"),
						condition: compiledCondition({
							op: "and",
							args: [
								{ op: "exists", args: [{ ref: "hashKey" }] },
								{ op: "eq", args: [{ ref: "v" }, { val: 1 }] },
							],
						}),
					});
					expect(res).toMatchObject({ outcome: "rejected", reason: { code: "condition_failed" } });
				});

				expect(await stub.apiGetItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") })).toMatchObject({
					found: true,
					item: { data: "v2", version: 2 },
				});
			});

			it("succeeds with empty conditions array (no conditions)", async ({ expect }) => {
				const { ctx, stub } = makeStub();

				await stub.apiPutItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk"), data: "value", kind: "text" as const });
				const result = await stub.apiDeleteItem(ctx, { hashKey: kb("hk"), sortKey: kb("sk") });

				expect(result).toMatchObject({ outcome: "ok", deleted: true });
			});
		});
	});
});
