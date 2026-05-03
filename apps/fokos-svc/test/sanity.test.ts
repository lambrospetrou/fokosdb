import { env, exports } from "cloudflare:workers";
import { describe, it } from "vitest";
import { FokosDB } from "../src/lib/db.js";
import { PartitionTopologyRouterImpl } from "../src/lib/partition-topology.js";

it("routes to the correct partition DO", async ({ expect }) => {
	const response = await exports.default.fetch("https://example.com/sql");
	expect(await response.text()).toBe("Hello, world!");
});

describe("fokosdb", async () => {
	it("should route to the right partition DO", async ({ expect }) => {
		const db = new FokosDB({
			ns: env.PARTITION_DO,
			topology: new PartitionTopologyRouterImpl("encoded-topology", env.PARTITION_DO, "fokos", {
				rootTreesN: 10,
				hashSplitConditions: { splitN: 2, maxSizeMb: 1 },
				rangeSplitConditions: { splitN: 2, maxSizeMb: 1 },
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
		});
	});
});
