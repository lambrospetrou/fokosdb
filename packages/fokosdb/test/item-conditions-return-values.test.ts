import { describe, expect, it } from "vitest";
import { ConditionCheckFailedError } from "../src/shared/partition-errors.js";
import { makeDB } from "./transactions/tx-helpers.js";

describe("ReturnValuesOnConditionCheckFailure for putItem and deleteItem", () => {
	it("rejects invalid returnValuesOnConditionCheckFailure value at client boundary", async () => {
		const db = makeDB();
		const key = { hashKey: "k1" };

		await expect(
			db.putItem({
				...key,
				data: "v",
				// @ts-expect-error test runtime validation of invalid value
				returnValuesOnConditionCheckFailure: "invalid_option",
			}),
		).rejects.toThrow(/returnValuesOnConditionCheckFailure must be 'none' or 'all_old'/);

		await expect(
			db.deleteItem({
				...key,
				// @ts-expect-error test runtime validation of invalid value
				returnValuesOnConditionCheckFailure: "invalid_option",
			}),
		).rejects.toThrow(/returnValuesOnConditionCheckFailure must be 'none' or 'all_old'/);
	});

	// Test 1: A conditional putItem that fails returns the stored item, its kind, its version, and its TTL, for each of the three data kinds.
	describe("Test 1: conditional putItem failure returns stored item across all three data kinds", () => {
		it("returns text image on condition failure", async () => {
			const db = makeDB();
			const key = { hashKey: `text-${crypto.randomUUID()}` };
			const ttlAt = Math.floor(Date.now() / 1000) + 3600;

			await db.putItem({ ...key, data: "original text", ttlAt });

			let caughtError: unknown;
			try {
				await db.putItem({
					...key,
					data: "new text",
					condition: { op: "eq", args: [{ ref: "v" }, { val: 999 }] },
					returnValuesOnConditionCheckFailure: "all_old",
				});
			} catch (e) {
				caughtError = e;
			}

			expect(caughtError).toBeInstanceOf(ConditionCheckFailedError);
			const err = caughtError as ConditionCheckFailedError;
			expect(err.message).toContain("condition failed");
			expect(err.reason).toMatchObject({
				type: "condition_failed",
				hashKey: key.hashKey,
				item: {
					hashKey: key.hashKey,
					data: "original text",
					kind: "text",
					version: 1,
					ttlAt,
				},
			});
			expect(err.item).toEqual(err.reason.type === "condition_failed" ? err.reason.item : undefined);
			expect(err.meta).toBeDefined();
			expect(err.meta.rowsRead).toBeGreaterThanOrEqual(2);
		});

		it("returns bytes image on condition failure", async () => {
			const db = makeDB();
			const key = { hashKey: `bytes-${crypto.randomUUID()}` };
			const originalBytes = new Uint8Array([10, 20, 30, 40]);
			const ttlAt = Math.floor(Date.now() / 1000) + 7200;

			await db.putItem({ ...key, data: originalBytes, ttlAt });

			let caughtError: unknown;
			try {
				await db.putItem({
					...key,
					data: new Uint8Array([1]),
					condition: { op: "eq", args: [{ ref: "v" }, { val: 999 }] },
					returnValuesOnConditionCheckFailure: "all_old",
				});
			} catch (e) {
				caughtError = e;
			}

			expect(caughtError).toBeInstanceOf(ConditionCheckFailedError);
			const err = caughtError as ConditionCheckFailedError;
			expect(err.item).toEqual({
				hashKey: key.hashKey,
				data: originalBytes,
				kind: "bytes",
				version: 1,
				ttlAt,
			});
		});

		it("returns parsed json image on condition failure", async () => {
			const db = makeDB();
			const key = { hashKey: `json-${crypto.randomUUID()}`, sortKey: "sk-1" };
			const originalJson = { name: "Alice", count: 42, tags: ["admin", "staff"] };

			await db.putItem({ ...key, data: originalJson });

			let caughtError: unknown;
			try {
				await db.putItem({
					...key,
					data: { name: "Bob" },
					condition: { op: "eq", args: [{ ref: "data", path: "$.count" }, { val: 0 }] },
					returnValuesOnConditionCheckFailure: "all_old",
				});
			} catch (e) {
				caughtError = e;
			}

			expect(caughtError).toBeInstanceOf(ConditionCheckFailedError);
			const err = caughtError as ConditionCheckFailedError;
			expect(err.item).toEqual({
				hashKey: key.hashKey,
				sortKey: key.sortKey,
				data: originalJson,
				kind: "json",
				version: 1,
			});
		});
	});

	// Test 2: A conditional putItem that fails on an absent item returns no image.
	it("Test 2: conditional putItem on absent item returns no image", async () => {
		const db = makeDB();
		const key = { hashKey: `absent-${crypto.randomUUID()}` };

		let caughtError: unknown;
		try {
			await db.putItem({
				...key,
				data: "value",
				condition: { op: "exists", args: [{ ref: "hashKey" }] },
				returnValuesOnConditionCheckFailure: "all_old",
			});
		} catch (e) {
			caughtError = e;
		}

		expect(caughtError).toBeInstanceOf(ConditionCheckFailedError);
		const err = caughtError as ConditionCheckFailedError;
		expect(err.item).toBeUndefined();
		expect(err.reason).toMatchObject({ type: "condition_failed", hashKey: key.hashKey });
	});

	// Test 3: A putItem with returnValuesOnConditionCheckFailure: "none" that fails returns no image, and its meta.rowsRead equals the value it reports today. The same call with "all_old" reports one more row read.
	it("Test 3: putItem with none returns no image, and all_old reports one more rowRead", async () => {
		const db = makeDB();
		const key = { hashKey: `rows-${crypto.randomUUID()}` };
		await db.putItem({ ...key, data: "stored" });

		let errorNone: ConditionCheckFailedError | undefined;
		try {
			await db.putItem({
				...key,
				data: "stale",
				condition: { op: "eq", args: [{ ref: "v" }, { val: 999 }] },
				returnValuesOnConditionCheckFailure: "none",
			});
		} catch (e) {
			errorNone = e as ConditionCheckFailedError;
		}

		expect(errorNone).toBeInstanceOf(ConditionCheckFailedError);
		expect(errorNone!.item).toBeUndefined();
		const rowsReadWithoutImage = errorNone!.meta.rowsRead;

		let errorAllOld: ConditionCheckFailedError | undefined;
		try {
			await db.putItem({
				...key,
				data: "stale",
				condition: { op: "eq", args: [{ ref: "v" }, { val: 999 }] },
				returnValuesOnConditionCheckFailure: "all_old",
			});
		} catch (e) {
			errorAllOld = e as ConditionCheckFailedError;
		}

		expect(errorAllOld).toBeInstanceOf(ConditionCheckFailedError);
		expect(errorAllOld!.item).toBeDefined();
		expect(errorAllOld!.meta.rowsRead).toBe(rowsReadWithoutImage + 1);
	});

	// Test 4: A conditional putItem that succeeds reads the same number of rows whether or not the caller asks for an image, and getItemImage is not called.
	it("Test 4: successful conditional putItem reads same rows regardless of image option", async () => {
		const db = makeDB();
		const key1 = { hashKey: `succ1-${crypto.randomUUID()}` };
		const key2 = { hashKey: `succ2-${crypto.randomUUID()}` };

		await db.putItem({ ...key1, data: "initial" });
		await db.putItem({ ...key2, data: "initial" });

		const resNone = await db.putItem({
			...key1,
			data: "updated",
			condition: { op: "eq", args: [{ ref: "v" }, { val: 1 }] },
			returnValuesOnConditionCheckFailure: "none",
		});

		const resAllOld = await db.putItem({
			...key2,
			data: "updated",
			condition: { op: "eq", args: [{ ref: "v" }, { val: 1 }] },
			returnValuesOnConditionCheckFailure: "all_old",
		});

		expect(resNone.meta.rowsRead).toBe(resAllOld.meta.rowsRead);
	});

	// Test 5: deleteItem repeats tests 1 to 3.
	describe("Test 5: deleteItem repeats tests 1 to 3", () => {
		it("deleteItem failure returns stored image for each data kind", async () => {
			const db = makeDB();
			const keyText = { hashKey: `del-text-${crypto.randomUUID()}` };
			const ttl = 12345;
			await db.putItem({ ...keyText, data: "text item", ttlAt: ttl });

			let errText: ConditionCheckFailedError | undefined;
			try {
				await db.deleteItem({
					...keyText,
					condition: { op: "eq", args: [{ ref: "v" }, { val: 999 }] },
					returnValuesOnConditionCheckFailure: "all_old",
				});
			} catch (e) {
				errText = e as ConditionCheckFailedError;
			}
			expect(errText).toBeInstanceOf(ConditionCheckFailedError);
			expect(errText!.item).toEqual({
				hashKey: keyText.hashKey,
				data: "text item",
				kind: "text",
				version: 1,
				ttlAt: ttl,
			});

			const keyBytes = { hashKey: `del-bytes-${crypto.randomUUID()}` };
			const bytesData = new Uint8Array([5, 6, 7]);
			await db.putItem({ ...keyBytes, data: bytesData });

			let errBytes: ConditionCheckFailedError | undefined;
			try {
				await db.deleteItem({
					...keyBytes,
					condition: { op: "eq", args: [{ ref: "v" }, { val: 999 }] },
					returnValuesOnConditionCheckFailure: "all_old",
				});
			} catch (e) {
				errBytes = e as ConditionCheckFailedError;
			}
			expect(errBytes).toBeInstanceOf(ConditionCheckFailedError);
			expect(errBytes!.item).toEqual({
				hashKey: keyBytes.hashKey,
				data: bytesData,
				kind: "bytes",
				version: 1,
			});

			const keyJson = { hashKey: `del-json-${crypto.randomUUID()}`, sortKey: "sk-del" };
			const jsonData = { key: "v", num: 100 };
			await db.putItem({ ...keyJson, data: jsonData });

			let errJson: ConditionCheckFailedError | undefined;
			try {
				await db.deleteItem({
					...keyJson,
					condition: { op: "eq", args: [{ ref: "v" }, { val: 999 }] },
					returnValuesOnConditionCheckFailure: "all_old",
				});
			} catch (e) {
				errJson = e as ConditionCheckFailedError;
			}
			expect(errJson).toBeInstanceOf(ConditionCheckFailedError);
			expect(errJson!.item).toEqual({
				hashKey: keyJson.hashKey,
				sortKey: keyJson.sortKey,
				data: jsonData,
				kind: "json",
				version: 1,
			});
		});

		it("deleteItem on absent item returns no image", async () => {
			const db = makeDB();
			const key = { hashKey: `del-absent-${crypto.randomUUID()}` };

			let caughtError: unknown;
			try {
				await db.deleteItem({
					...key,
					condition: { op: "exists", args: [{ ref: "hashKey" }] },
					returnValuesOnConditionCheckFailure: "all_old",
				});
			} catch (e) {
				caughtError = e;
			}

			expect(caughtError).toBeInstanceOf(ConditionCheckFailedError);
			const err = caughtError as ConditionCheckFailedError;
			expect(err.item).toBeUndefined();
		});

		it("deleteItem with none returns no image, and all_old reports one more rowRead", async () => {
			const db = makeDB();
			const key = { hashKey: `del-rows-${crypto.randomUUID()}` };
			await db.putItem({ ...key, data: "stored" });

			let errorNone: ConditionCheckFailedError | undefined;
			try {
				await db.deleteItem({
					...key,
					condition: { op: "eq", args: [{ ref: "v" }, { val: 999 }] },
					returnValuesOnConditionCheckFailure: "none",
				});
			} catch (e) {
				errorNone = e as ConditionCheckFailedError;
			}
			expect(errorNone).toBeInstanceOf(ConditionCheckFailedError);
			expect(errorNone!.item).toBeUndefined();
			const rowsWithoutImage = errorNone!.meta.rowsRead;

			let errorAllOld: ConditionCheckFailedError | undefined;
			try {
				await db.deleteItem({
					...key,
					condition: { op: "eq", args: [{ ref: "v" }, { val: 999 }] },
					returnValuesOnConditionCheckFailure: "all_old",
				});
			} catch (e) {
				errorAllOld = e as ConditionCheckFailedError;
			}
			expect(errorAllOld).toBeInstanceOf(ConditionCheckFailedError);
			expect(errorAllOld!.item).toBeDefined();
			expect(errorAllOld!.meta.rowsRead).toBe(rowsWithoutImage + 1);
		});
	});
});
