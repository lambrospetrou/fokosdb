import { describe, expect, expectTypeOf, it } from "vitest";
import type { FokosDB } from "../src/client/db.js";
import type { DataKind, JsonComposite, ReadItem } from "../src/shared/types.js";
import { makeDB } from "./transactions/tx-helpers.js";
import { fokosErrorWith } from "./errors-matchers.js";

/**
 * `returnValues: "all_old"` on a `putItem` or `deleteItem` that succeeds returns the old item in
 * `item`, with the caller's own keys. Without it, the result has no `item`. No write result has keys.
 */
describe("returnValues for putItem and deleteItem", () => {
	const newKey = (name: string) => ({ hashKey: `${name}-${crypto.randomUUID()}`, sortKey: "s" });

	it("returns no item and no keys without returnValues, and with none", async () => {
		const db = makeDB();
		const key = newKey("none");
		await db.putItem({ ...key, data: "v1" });

		const put = await db.putItem({ ...key, data: "v2" });
		expect(put).toEqual({ version: 2, meta: expect.anything() });
		const del = await db.deleteItem({ ...key, returnValues: "none" });
		expect(del).toEqual({ deleted: true, meta: expect.anything() });
	});

	it("returns the old item of a put and of a delete across the three data kinds, with its TTL", async () => {
		const db = makeDB();
		const ttlAt = Math.floor(Date.now() / 1000) + 3600;
		const cases: Array<{ data: string | Uint8Array | JsonComposite; next: string | Uint8Array | JsonComposite; kind: DataKind }> = [
			{ data: { n: 1, tags: ["a"] }, next: { n: 2 }, kind: "json" },
			{ data: "old text", next: "new text", kind: "text" },
			{ data: new Uint8Array([1, 2, 3]), next: new Uint8Array([4]), kind: "bytes" },
		];
		for (const { data, next, kind } of cases) {
			const key = newKey(kind);
			await db.putItem({ ...key, data, ttlAt });

			const put = await db.putItem({ ...key, data: next, returnValues: "all_old" });
			expect(put).toEqual({ item: { ...key, kind, data, ttlAt, version: 1 }, version: 2, meta: expect.anything() });

			const del = await db.deleteItem({ ...key, returnValues: "all_old" });
			expect(del).toEqual({ item: { ...key, kind, data: next, version: 2 }, deleted: true, meta: expect.anything() });
		}
	});

	it("returns no item when no old item existed", async () => {
		const db = makeDB();
		const key = newKey("absent");

		await expect(db.putItem({ ...key, data: "first", returnValues: "all_old" })).resolves.toEqual({
			version: 1,
			meta: expect.anything(),
		});
		await db.deleteItem(key);
		await expect(db.deleteItem({ ...key, returnValues: "all_old" })).resolves.toEqual({ deleted: false, meta: expect.anything() });
	});

	it("returns the old item of a write whose condition passes, and counts its read", async () => {
		const db = makeDB();
		const key = newKey("condition");
		await db.putItem({ ...key, data: "v1" });
		const condition = { op: "eq", args: [{ ref: "v" }, { val: 1 }] } as const;

		const plain = await db.putItem({ ...key, data: "v2", condition });
		const put = await db.putItem({
			...key,
			data: "v3",
			condition: { op: "eq", args: [{ ref: "v" }, { val: 2 }] },
			returnValues: "all_old",
		});

		expect(put.item).toEqual({ ...key, kind: "text", data: "v2", version: 2 });
		// The old item costs one more row read than the same write without it.
		expect(put.meta.partitions[0].rowsRead).toBe(plain.meta.partitions[0].rowsRead + 1);
	});

	it("rejects a returnValues value that is not valid", async () => {
		const db = makeDB();
		const key = newKey("invalid");
		// @ts-expect-error test runtime validation of an invalid value
		await expect(db.putItem({ ...key, data: "v", returnValues: "all_new" })).rejects.toThrow(
			fokosErrorWith("return_values_option_invalid"),
		);
		// @ts-expect-error test runtime validation of an invalid value
		await expect(db.deleteItem({ ...key, returnValues: "all_new" })).rejects.toThrow(fokosErrorWith("return_values_option_invalid"));
	});

	// Type-level contract, never executed: `item` is the complete old item, typed by `T`.
	async function _types(db: FokosDB) {
		const put = await db.putItem<{ n: number }>({ hashKey: "a", data: { n: 1 }, returnValues: "all_old" });
		expectTypeOf(put.item).toEqualTypeOf<ReadItem<{ n: number }> | undefined>();
		if (put.item?.kind === "json") {
			expectTypeOf(put.item.data.n).toEqualTypeOf<number>();
		}
		const del = await db.deleteItem({ hashKey: "a" });
		expectTypeOf(del.item).toEqualTypeOf<ReadItem | undefined>();
		// @ts-expect-error a write result has no keys
		void put.hashKey;
	}
	void _types;
});
