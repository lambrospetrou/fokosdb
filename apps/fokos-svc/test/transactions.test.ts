import { env } from "cloudflare:workers";
import { describe, it, expect } from "vitest";
import { FokosDB } from "../src/lib/db.js";
import { PartitionContextCreator, PartitionTopologyRouterImpl } from "../src/lib/partition-topology/partition-topology.js";

function makeDB() {
	const prefix = `txtest.${crypto.randomUUID()}`;
	const base = PartitionContextCreator.create({
		ns: "PARTITION_DO",
		nsPrefix: prefix,
		rootTreesN: 100,
		hashSplitConditions: { splitN: 2, maxSizeMb: 100 },
		rangeSplitConditions: { splitN: 2, maxSizeMb: 500 },
	});
	const topology = new PartitionTopologyRouterImpl("", base);
	return new FokosDB({
		ns: env.PARTITION_DO,
		topology,
		transactionCoordinatorNs: env.TRANSACTION_COORDINATOR_DO,
	});
}

describe("transactions - end-to-end", () => {
	it("transactWriteItems across 100 items spanning many partitions, including 10 pre-existing items", async () => {
		const db = makeDB();

		const preExistingKeys = Array.from({ length: 10 }, (_, i) => ({
			hashKey: `pre-hk-${i}`,
			sortKey: `pre-sk-${i}`,
		}));

		for (const key of preExistingKeys) {
			await db.putItem({ ...key, data: `original-${key.hashKey}` });
		}

		for (const key of preExistingKeys) {
			const result = await db.getItem(key);
			expect(result.found).toBe(true);
			if (result.found) {
				expect(result.data).toBe(`original-${key.hashKey}`);
				expect(result.version).toBe(1);
			}
		}

		const operations = Array.from({ length: 100 }, (_, i) => {
			if (i < 10) {
				return {
					hashKey: preExistingKeys[i].hashKey,
					sortKey: preExistingKeys[i].sortKey,
					operation: "put" as const,
					data: `tx-updated-${i}`,
				};
			}
			return {
				hashKey: `tx-hk-${i}`,
				sortKey: `tx-sk-${i}`,
				operation: "put" as const,
				data: `tx-data-${i}`,
			};
		});

		const txResult = await db.transactWriteItems({
			operations,
			clientRequestToken: `test-token-${crypto.randomUUID()}`,
		});

		expect(txResult.outcome).toBe("committed");
		expect(txResult).toMatchObject({
			outcome: "committed",
			transactionId: expect.any(String),
			idempotencyToken: expect.any(String),
		});

		for (let i = 0; i < 10; i++) {
			const result = await db.getItem(preExistingKeys[i]);
			expect(result.found).toBe(true);
			if (result.found) {
				expect(result.data).toBe(`tx-updated-${i}`);
				expect(result.version).toBe(2);
			}
		}

		for (let i = 10; i < 100; i++) {
			const result = await db.getItem({ hashKey: `tx-hk-${i}`, sortKey: `tx-sk-${i}` });
			expect(result.found).toBe(true);
			if (result.found) {
				expect(result.data).toBe(`tx-data-${i}`);
				expect(result.version).toBe(1);
			}
		}

		const partitionDoNames = new Set<string>();
		const topology = (db.options.topology as PartitionTopologyRouterImpl);
		for (const op of operations) {
			const { partitionContext } = topology.pickPartition(op.hashKey, op.sortKey);
			partitionDoNames.add(partitionContext.doName);
		}
		expect(partitionDoNames.size).toBeGreaterThan(1);
	});
});
