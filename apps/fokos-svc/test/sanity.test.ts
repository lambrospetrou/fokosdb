import { env, exports } from "cloudflare:workers";
import { describe, it } from "vitest";
import { FokosDB } from "../src/lib/db.js";
import { PartitionContextCreator, PartitionTopologyRouterImpl } from "../src/lib/partition-topology/partition-topology.js";

describe("fokosdb", async () => {
	const testSplitOptions = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		databaseName: "fokos",
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
			transactionCoordinatorNs: env.TRANSACTION_COORDINATOR_DO,
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
		).resolves.toMatchObject({
			found: true,
			item: { hashKey: "test-hash-key", sortKey: "test-sort-key", data: new Uint8Array([1, 2, 3]), version: 1 },
			meta: {
				rowsRead: 1,
				rowsWritten: 0,
				databaseSize: expect.any(Number),
				servedByActorId: expect.any(String),
			},
		});
	});
});
