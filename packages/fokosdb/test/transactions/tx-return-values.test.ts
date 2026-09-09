import { describe, expect, it } from "vitest";
import { countDistinctPartitions, keysAcrossPartitions, keysInOnePartition, makeDB, type Key } from "./tx-helpers.js";
import invariant from "../../src/shared/invariant.js";
import { applyImageCap, MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX, MAX_ITEM_BYTES } from "../../src/shared/transaction-limits.js";
import type { ParticipantOperationResultEncoded, TransactWriteItemsOptions } from "../../src/shared/transaction-types.js";

describe("transactWriteItems returnValuesOnConditionCheckFailure and per-operation results", () => {
	it("validates returnValuesOnConditionCheckFailure at the boundary", async () => {
		const db = makeDB();
		await expect(
			db.transactWriteItems({
				items: [
					{
						operation: "put",
						hashKey: "val-test-1",
						data: "hello",
						// @ts-expect-error runtime validation
						returnValuesOnConditionCheckFailure: "invalid",
					},
				],
			}),
		).rejects.toThrow(/returnValuesOnConditionCheckFailure must be 'none' or 'all_old'/);
	});

	it("committed transaction does not return a results array", async () => {
		const db = makeDB();
		const res = await db.transactWriteItems({
			items: [
				{
					operation: "put",
					hashKey: "tx-commit-1",
					data: "v1",
				},
				{
					operation: "put",
					hashKey: "tx-commit-2",
					data: "v2",
				},
			],
		});
		expect(res.outcome).toBe("committed");
		expect("results" in res).toBe(false);
	});

	describe("single-partition fast path", () => {
		it("returns positional results with passed and rejected outcomes", async () => {
			const db = makeDB();
			const hk = `sp-${crypto.randomUUID()}`;

			await db.putItem({ hashKey: hk, sortKey: "item-1", data: "initial-1" });
			await db.putItem({ hashKey: hk, sortKey: "item-2", data: "initial-2" });

			const res = await db.transactWriteItems({
				items: [
					{
						operation: "put",
						hashKey: hk,
						sortKey: "item-1",
						data: "updated-1",
					},
					{
						operation: "put",
						hashKey: hk,
						sortKey: "item-2",
						data: "updated-2",
						condition: { op: "eq", args: [{ ref: "v" }, { val: 999 }] },
						returnValuesOnConditionCheckFailure: "all_old",
					},
				],
			});

			expect(res.outcome).toBe("cancelled");
			if (res.outcome !== "cancelled") return;

			expect(res.reason).toMatchObject({
				type: "condition_failed",
				hashKey: hk,
				sortKey: "item-2",
			});
			// Top-level reason has no item
			expect((res.reason as { item?: unknown }).item).toBeUndefined();

			expect(res.results).toHaveLength(2);
			expect(res.results[0]).toEqual({ outcome: "passed" });
			expect(res.results[1]).toMatchObject({
				outcome: "rejected",
				reason: {
					type: "condition_failed",
					hashKey: hk,
					sortKey: "item-2",
					item: {
						hashKey: hk,
						sortKey: "item-2",
						data: "initial-2",
						kind: "text",
						version: 1,
					},
				},
			});

			// Verify public results do not leak internal fields
			expect("opIndex" in res.results[0]).toBe(false);
			expect("opIndex" in res.results[1]).toBe(false);
			expect("imageBytes" in res.results[1]).toBe(false);
		});

		it("returns no image when returnValuesOnConditionCheckFailure is none or omitted", async () => {
			const db = makeDB();
			const hk = `sp-none-${crypto.randomUUID()}`;

			await db.putItem({ hashKey: hk, sortKey: "s1", data: "data-1" });

			const res = await db.transactWriteItems({
				items: [
					{
						operation: "put",
						hashKey: hk,
						sortKey: "s1",
						data: "data-2",
						condition: { op: "eq", args: [{ ref: "v" }, { val: 999 }] },
						returnValuesOnConditionCheckFailure: "none",
					},
				],
			});

			expect(res.outcome).toBe("cancelled");
			if (res.outcome !== "cancelled") return;
			expect(res.results).toHaveLength(1);
			expect(res.results[0]).toMatchObject({
				outcome: "rejected",
				reason: {
					type: "condition_failed",
					hashKey: hk,
					sortKey: "s1",
				},
			});
			expect((res.results[0] as { reason: { item?: unknown } }).reason.item).toBeUndefined();
		});
	});

	describe("two-phase coordinator path", () => {
		it("evaluates all operations across partitions and preserves request order", async () => {
			const db = makeDB({ rootTreesN: 100 });
			const token = `token-${crypto.randomUUID()}`;

			const k1 = { hashKey: `partA-${crypto.randomUUID()}` };
			const k2 = { hashKey: `partB-${crypto.randomUUID()}` };
			const k3 = { hashKey: `partC-${crypto.randomUUID()}` };

			const ttlAt = Math.floor(Date.now() / 1000) + 3600;
			await db.putItem({ ...k1, data: "init-1" });
			await db.putItem({ ...k2, data: { count: 42 }, ttlAt });
			await db.putItem({ ...k3, data: "init-3" });

			const res = await db.transactWriteItems({
				clientRequestToken: token,
				items: [
					{
						operation: "put",
						...k1,
						data: "new-1",
						condition: { op: "eq", args: [{ ref: "v" }, { val: 999 }] },
						returnValuesOnConditionCheckFailure: "all_old",
					},
					{
						operation: "put",
						...k2,
						data: { count: 43 },
						condition: { op: "eq", args: [{ ref: "v" }, { val: 999 }] },
						returnValuesOnConditionCheckFailure: "all_old",
					},
					{
						operation: "put",
						...k3,
						data: "new-3",
					},
				],
			});

			expect(res.outcome).toBe("cancelled");
			if (res.outcome !== "cancelled") return;

			// Top-level reason is the first rejected operation in request order (op 0)
			expect(res.reason).toMatchObject({
				type: "condition_failed",
				hashKey: k1.hashKey,
			});
			expect((res.reason as { item?: unknown }).item).toBeUndefined();

			// 3 results in request order
			expect(res.results).toHaveLength(3);

			// Op 0: rejected with image
			expect(res.results[0]).toMatchObject({
				outcome: "rejected",
				reason: {
					type: "condition_failed",
					hashKey: k1.hashKey,
					item: {
						hashKey: k1.hashKey,
						data: "init-1",
						kind: "text",
						version: 1,
					},
				},
			});

			// Op 1: rejected with image (JSON data parsed)
			expect(res.results[1]).toMatchObject({
				outcome: "rejected",
				reason: {
					type: "condition_failed",
					hashKey: k2.hashKey,
					item: {
						hashKey: k2.hashKey,
						data: { count: 42 },
						kind: "json",
						version: 1,
						ttlAt,
					},
				},
			});

			// Op 2: passed
			expect(res.results[2]).toEqual({ outcome: "passed" });

			// Verify public fields do not carry opIndex or imageBytes
			for (const r of res.results) {
				expect("opIndex" in r).toBe(false);
				expect("imageBytes" in r).toBe(false);
			}

			// Idempotent replay under the same token returns identical results
			const replay = await db.transactWriteItems({
				clientRequestToken: token,
				items: [
					{
						operation: "put",
						...k1,
						data: "new-1",
						condition: { op: "eq", args: [{ ref: "v" }, { val: 999 }] },
						returnValuesOnConditionCheckFailure: "all_old",
					},
					{
						operation: "put",
						...k2,
						data: { count: 43 },
						condition: { op: "eq", args: [{ ref: "v" }, { val: 999 }] },
						returnValuesOnConditionCheckFailure: "all_old",
					},
					{
						operation: "put",
						...k3,
						data: "new-3",
					},
				],
			});
			expect(replay).toEqual(res);
		});

		it("rejects token reuse when operations are in different order", async () => {
			const db = makeDB({ rootTreesN: 100 });
			const token = `order-test-${crypto.randomUUID()}`;

			const k1 = { hashKey: `ord-1-${crypto.randomUUID()}` };
			const k2 = { hashKey: `ord-2-${crypto.randomUUID()}` };

			const op1 = { operation: "put" as const, ...k1, data: "v1" };
			const op2 = { operation: "put" as const, ...k2, data: "v2" };

			await db.transactWriteItems({
				clientRequestToken: token,
				items: [op1, op2],
			});

			// Same operations reversed under the same token must be rejected
			await expect(
				db.transactWriteItems({
					clientRequestToken: token,
					items: [op2, op1],
				}),
			).rejects.toThrow(/clientRequestToken was already used for a different set of operations/);
		});

		it("rejects token reuse when returnValuesOnConditionCheckFailure changes", async () => {
			const db = makeDB({ rootTreesN: 100 });
			const token = `flag-test-${crypto.randomUUID()}`;

			const k1 = { hashKey: `flag-1-${crypto.randomUUID()}` };
			const k2 = { hashKey: `flag-2-${crypto.randomUUID()}` };

			await db.putItem({ ...k1, data: "initial-1" });

			const op1 = {
				operation: "put" as const,
				...k1,
				data: "new-1",
				condition: { op: "eq" as const, args: [{ ref: "v" as const }, { val: 999 }] as const },
			};
			const op2 = { operation: "put" as const, ...k2, data: "v2" };

			await db.transactWriteItems({
				clientRequestToken: token,
				items: [op1, op2],
			});

			// Reusing token with returnValuesOnConditionCheckFailure added must be rejected
			await expect(
				db.transactWriteItems({
					clientRequestToken: token,
					items: [{ ...op1, returnValuesOnConditionCheckFailure: "all_old" }, op2],
				}),
			).rejects.toThrow(/clientRequestToken was already used for a different set of operations/);
		});

		it("round-trips binary keys and binary data through coordinator results", async () => {
			const db = makeDB({ rootTreesN: 100 });
			const token = `bin-test-${crypto.randomUUID()}`;

			const binHk = new Uint8Array([0x01, 0x02, 0x03, 0xff]);
			const binSk = new Uint8Array([0x04, 0x05]);
			const binData = new Uint8Array([0xca, 0xfe, 0xba, 0xbe]);

			await db.putItem({ hashKey: binHk, sortKey: binSk, data: binData });

			const res = await db.transactWriteItems({
				clientRequestToken: token,
				items: [
					{
						operation: "put",
						hashKey: binHk,
						sortKey: binSk,
						data: new Uint8Array([0x00]),
						condition: { op: "eq" as const, args: [{ ref: "v" as const }, { val: 999 }] as const },
						returnValuesOnConditionCheckFailure: "all_old",
					},
					{
						operation: "put",
						hashKey: `other-${crypto.randomUUID()}`,
						data: "hello",
					},
				],
			});

			expect(res.outcome).toBe("cancelled");
			if (res.outcome !== "cancelled") return;

			expect(res.results).toHaveLength(2);
			const op0 = res.results[0];
			expect(op0.outcome).toBe("rejected");
			if (op0.outcome === "rejected" && op0.reason.type === "condition_failed") {
				expect(op0.reason.type).toBe("condition_failed");
				expect(op0.reason.hashKey).toEqual(binHk);
				expect(op0.reason.sortKey).toEqual(binSk);
				expect(op0.reason.item).toBeDefined();
				expect(op0.reason.item?.data).toEqual(binData);
				expect(op0.reason.item?.kind).toBe("bytes");
			}
		});
		// Each participant answers under its own cap, so only the coordinator can see that the whole
		// transaction is over it. It caps the merged array before it stores one, which is what makes a
		// replay answer with the images the first call returned and not with the ones it dropped.
		it("caps the merged images, and a replay returns the same array including itemOmitted", async () => {
			const db = makeDB();
			const token = `cap-replay-${crypto.randomUUID()}`;

			// Just under the per-item ceiling: the store measures the whole row, so the data must leave
			// room for both keys and the fixed per-row overhead.
			const imageBytes = MAX_ITEM_BYTES - 2048;
			const data = "x".repeat(imageBytes);
			const fitting = Math.floor(MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX / imageBytes);
			const keys = keysAcrossPartitions(db, fitting + 2, `cap-${crypto.randomUUID()}`);

			for (const k of keys) {
				await db.putItem({ ...k, data });
			}

			const request: TransactWriteItemsOptions = {
				clientRequestToken: token,
				items: keys.map((k) => ({
					operation: "delete",
					...k,
					condition: { op: "not_exists", args: [{ ref: "hashKey" }] },
					returnValuesOnConditionCheckFailure: "all_old",
				})),
			};

			const res = await db.transactWriteItems(request);
			invariant(res.outcome === "cancelled", "expected the transaction to cancel");
			expect(res.results).toHaveLength(keys.length);

			for (const [i, r] of res.results.entries()) {
				invariant(r.outcome === "rejected" && r.reason.type === "condition_failed", `op ${i} should have failed its condition`);
				if (i < fitting) {
					expect(r.itemOmitted).toBeUndefined();
					expect(r.reason.item?.data).toBe(data);
				} else {
					expect(r.itemOmitted).toBe("response_too_large");
					expect(r.reason.item).toBeUndefined();
				}
			}

			// The replay reads the stored array and the surviving images back, so it must be identical —
			// the images the cap dropped are gone from storage and cannot reappear.
			const replay = await db.transactWriteItems(request);
			expect(replay).toEqual(res);
		});
	});

	// The outcome codes a caller reads must not depend on which partition owns each operation. Only
	// the image set may, because every node caps what it sends on its own.
	describe("partition layout", () => {
		it("returns the same outcome codes and images whether one partition owns every operation or several do", async () => {
			const db = makeDB();

			// The same four-operation shape twice: operations 0 and 2 fail their condition and ask for the
			// old image, operations 1 and 3 pass. Only the keys differ, so only the layout differs.
			const runOver = async (keys: Key[], token?: string) => {
				for (const [i, k] of keys.entries()) {
					await db.putItem({ ...k, data: `stored-${i}` });
				}
				const res = await db.transactWriteItems({
					...(token ? { clientRequestToken: token } : {}),
					items: keys.map((k, i) => ({
						operation: "put" as const,
						...k,
						data: `new-${i}`,
						...(i % 2 === 0
							? {
									condition: { op: "eq" as const, args: [{ ref: "v" as const }, { val: 999 }] },
									returnValuesOnConditionCheckFailure: "all_old" as const,
								}
							: {}),
					})),
				});
				invariant(res.outcome === "cancelled", "expected the transaction to cancel");
				return res;
			};

			const oneKeys = keysInOnePartition(db, 4, `layout-one-${crypto.randomUUID()}`);
			const manyKeys = keysAcrossPartitions(db, 4, `layout-many-${crypto.randomUUID()}`);
			expect(countDistinctPartitions(db, oneKeys)).toBe(1);
			expect(countDistinctPartitions(db, manyKeys)).toBe(4);

			// The spread set carries a token, which is what puts it on the two-phase path; the single
			// partition set runs the single-shot fast path.
			const one = await runOver(oneKeys);
			const many = await runOver(manyKeys, `layout-${crypto.randomUUID()}`);

			const codesOf = (res: Awaited<ReturnType<typeof runOver>>) => res.results.map((r) => r.outcome);
			expect(codesOf(one)).toEqual(["rejected", "passed", "rejected", "passed"]);
			expect(codesOf(many)).toEqual(codesOf(one));

			// The same images come back on both layouts, and the reason is the lowest rejected index.
			const imagesOf = (res: Awaited<ReturnType<typeof runOver>>) =>
				res.results.map((r) => (r.outcome === "rejected" && r.reason.type === "condition_failed" ? r.reason.item?.data : undefined));
			expect(imagesOf(one)).toEqual(["stored-0", undefined, "stored-2", undefined]);
			expect(imagesOf(many)).toEqual(imagesOf(one));
			expect(one.reason.type).toBe("condition_failed");
			expect(many.reason.type).toBe(one.reason.type);
			for (const res of [one, many]) {
				expect(res.results.every((r) => r.outcome !== "rejected" || r.itemOmitted === undefined)).toBe(true);
			}
		});
	});

	describe("image cap logic", () => {
		it("drops images exceeding the cap and sets itemOmitted: response_too_large", () => {
			const items: ParticipantOperationResultEncoded[] = [
				{
					opIndex: 0,
					outcome: "rejected",
					reason: {
						type: "condition_failed",
						hashKey: "k0",
						item: { hashKey: "k0", data: "data-0", kind: "text", version: 1 },
					},
					imageBytes: 50,
				},
				{
					opIndex: 1,
					outcome: "rejected",
					reason: {
						type: "condition_failed",
						hashKey: "k1",
						item: { hashKey: "k1", data: "data-1", kind: "text", version: 1 },
					},
					imageBytes: 60,
				},
				{
					opIndex: 2,
					outcome: "passed",
				},
				{
					opIndex: 3,
					outcome: "rejected",
					reason: {
						type: "condition_failed",
						hashKey: "k3",
						item: { hashKey: "k3", data: "data-3", kind: "text", version: 1 },
					},
					imageBytes: 30,
				},
			];

			// Cap at 100 bytes: opIndex 0 (50 bytes) fits, opIndex 1 (60 bytes) takes total to 110 > 100 so dropped
			applyImageCap(items, 100);

			expect(items[0].outcome).toBe("rejected");
			if (items[0].outcome === "rejected" && items[0].reason.type === "condition_failed") {
				expect(items[0].reason.item).toBeDefined();
				expect(items[0].itemOmitted).toBeUndefined();
			}

			expect(items[1].outcome).toBe("rejected");
			if (items[1].outcome === "rejected" && items[1].reason.type === "condition_failed") {
				expect(items[1].reason.item).toBeUndefined();
				expect(items[1].itemOmitted).toBe("response_too_large");
			}

			expect(items[2].outcome).toBe("passed");

			expect(items[3].outcome).toBe("rejected");
			if (items[3].outcome === "rejected" && items[3].reason.type === "condition_failed") {
				expect(items[3].reason.item).toBeUndefined();
				expect(items[3].itemOmitted).toBe("response_too_large");
			}
		});

		it("leaves existing itemOmitted untouched and does not count its bytes", () => {
			const items: ParticipantOperationResultEncoded[] = [
				{
					opIndex: 0,
					outcome: "rejected",
					reason: {
						type: "condition_failed",
						hashKey: "k0",
					},
					itemOmitted: "response_too_large",
					imageBytes: 100,
				},
				{
					opIndex: 1,
					outcome: "rejected",
					reason: {
						type: "condition_failed",
						hashKey: "k1",
						item: { hashKey: "k1", data: "data-1", kind: "text", version: 1 },
					},
					imageBytes: 50,
				},
			];

			applyImageCap(items, 80);

			// Item 0 was already omitted, so it stays omitted and its bytes do not count against running total
			if (items[0].outcome === "rejected") {
				expect(items[0].itemOmitted).toBe("response_too_large");
			}
			// Item 1 has 50 bytes, fits under 80, so it keeps its image
			if (items[1].outcome === "rejected" && items[1].reason.type === "condition_failed") {
				expect(items[1].reason.item).toBeDefined();
				expect(items[1].itemOmitted).toBeUndefined();
			}
		});
	});
});
