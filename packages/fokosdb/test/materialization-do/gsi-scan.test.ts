import { describe, expect, it, vi } from "vitest";
import { FokosDB } from "../../src/client/db.js";
import type { GsiScanItemsOptions } from "../../src/shared/types.js";
import { TARGET_WAIT } from "../partition-do/helpers.js";

function makeDB() {
	return new FokosDB({
		table: {
			name: `gsiscan.${crypto.randomUUID()}`,
			ns: "PARTITION_DO",
			nsTx: "TRANSACTION_COORDINATOR_DO",
			nsMat: "GSI_MATERIALIZATION_DO",
			rootTreesN: 2,
			hashSplitN: 2,
		},
		captureChanges: true,
	});
}

type Entry = { hashKey: unknown; sortKey?: unknown; baseHashKey?: unknown; baseSortKey?: unknown };
const keysOf = (items: readonly Entry[]) => items.map((i) => `${String(i.hashKey)}|${String(i.sortKey)}|${String(i.baseHashKey)}`);

describe("db.gsi(indexName).scanItems", () => {
	it("scans the entries of a GSI, stops between two entries of one GSI key, and starts at GSI keys with no base keys", async () => {
		const db = makeDB();
		const orders = [
			["o1", "OPEN", "t1"],
			["o2", "OPEN", "t2"],
			["o3", "OPEN", "t2"],
			["o4", "DONE", "t3"],
		] as const;
		for (const [hashKey, status, at] of orders) {
			await db.putItem({ hashKey, sortKey: "s", data: { status, at, owner: "u" } });
		}
		const gsi = db.gsi("byStatus");
		const drain = async (opts: GsiScanItemsOptions) => {
			const items: Entry[] = [];
			let cursor: string | undefined;
			do {
				const page = await gsi.scanItems({ ...opts, ...(cursor === undefined ? {} : { cursor, exclusiveStartKey: undefined }) });
				items.push(...page.items);
				cursor = page.cursor;
			} while (cursor !== undefined);
			return keysOf(items);
		};
		await vi.waitFor(async () => expect(await drain({})).toHaveLength(4), TARGET_WAIT);

		const all = await drain({});
		const first = await gsi.scanItems({});
		expect(first.items.find((item) => item.baseHashKey === "o1")).toMatchObject({
			hashKey: "OPEN",
			sortKey: "t1",
			baseSortKey: "s",
			data: { at: "t1" },
		});
		// One entry per page: the cursor keeps the stored GSI sort key, so the two entries of `t2` each come once.
		expect(await drain({ limit: 1 })).toEqual(all);
		const t2 = all.filter((key) => key.startsWith("OPEN|t2|"));
		expect(t2).toEqual(["OPEN|t2|o2", "OPEN|t2|o3"]);

		// A start key with the GSI keys and no base keys starts before both entries of `t2`.
		const fromT2 = await drain({ exclusiveStartKey: { hashKey: "OPEN", sortKey: "t2" } });
		expect(fromT2).toEqual(all.slice(all.indexOf(t2[0])));
		// A start key with the full key of an entry starts after that entry.
		const afterO2 = await drain({ exclusiveStartKey: { hashKey: "OPEN", sortKey: "t2", baseHashKey: "o2", baseSortKey: "s" } });
		expect(afterO2).toEqual(all.slice(all.indexOf(t2[1])));

		await expect(gsi.scanItems({ exclusiveStartKey: { hashKey: "OPEN", baseSortKey: "s" } })).rejects.toMatchObject({
			code: "scan_start_key_invalid",
		});
		// A cursor of the GSI fails on the base table.
		const page = await gsi.scanItems({ limit: 1 });
		await expect(db.scanItems({ cursor: page.cursor })).rejects.toMatchObject({ code: "cursor_fingerprint_mismatch" });
	});
});
