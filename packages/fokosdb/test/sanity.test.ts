import { describe, it } from "vitest";
import { FokosDB } from "../src/client/db.js";

describe("fokosdb", async () => {
	const testSplitOptions = {
		table: { name: "fokos", ns: "PARTITION_DO", nsTx: "TRANSACTION_COORDINATOR_DO", rootTreesN: 10, hashSplitN: 2 },
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: 1 },
		rangeSplitConditions: { maxSizeMb: 1 },
	} as const;

	it("should route to the right partition DO", async ({ expect }) => {
		const db = new FokosDB(testSplitOptions);

		await expect(
			db.putItem({
				hashKey: "test-hash-key",
				sortKey: "test-sort-key",
				data: new Uint8Array([1, 2, 3]),
			}),
		).resolves.not.toThrow();

		await expect(
			db.getItem({
				hashKey: "test-hash-key",
				sortKey: "test-sort-key",
			}),
		).resolves.toMatchObject({
			found: true,
			item: {
				hashKey: "test-hash-key",
				sortKey: "test-sort-key",
				data: new Uint8Array([1, 2, 3]),
				version: 1,
			},
			meta: {
				rowsRead: 1,
				rowsWritten: 0,
				databaseSize: expect.any(Number),
				servedByActorId: expect.any(String),
			},
		});
	});
});
