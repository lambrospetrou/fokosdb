import { env, exports } from "cloudflare:workers";
import { describe, it } from "vitest";
import { FokosDB } from "../src/lib/db.js";
import { PartitionContextCreator, PartitionTopologyRouterImpl } from "../src/lib/partition-topology.js";

it("routes to the correct partition DO", async ({ expect }) => {
	const response = await exports.default.fetch("https://example.com/sql");
	expect(await response.text()).toBe("Hello, world!");
});

describe("fokosdb", async () => {
	const testSplitOptions = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsPrefix: "fokos",
		rootTreesN: 10,
		hashSplitConditions: { splitN: 2, maxSizeMb: 1 },
		rangeSplitConditions: { splitN: 2, maxSizeMb: 1 },
	});

	it("should route to the right partition DO", async ({ expect }) => {
		const db = new FokosDB({
			ns: env.PARTITION_DO,
			topology: new PartitionTopologyRouterImpl("encoded-topology", {
				...testSplitOptions,
			}),
		});

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
		).resolves.toEqual({
			found: true,
			data: new Uint8Array([1, 2, 3]),
			hashKey: "test-hash-key",
			sortKey: "test-sort-key",
			meta: {
				rowsRead: 1,
				rowsWritten: 0,
				databaseSize: expect.any(Number),
			},
		});
	});

	it("should route to the right partition DO", async ({ expect }) => {
		const db = new FokosDB({
			ns: env.PARTITION_DO,
			topology: new PartitionTopologyRouterImpl("encoded-topology", {
				...testSplitOptions,
			}),
		});

		const res = await db.putItem({
			hashKey: "hk1",
			sortKey: "sk1",
			// >1MB to trigger the condition for the split.
			data: "x".repeat(1 * 1024 * 1024 + 10),
		});
		expect(res.__debug?.splitStatus).toBeDefined();
	});
});
