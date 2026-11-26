/**
 * `routingHashBaseSeed`: a client and a partition class with the same value, and a client with
 * another value than the partition class.
 */
import { describe, expect, it } from "vitest";
import { FokosDB } from "../src/client/db.js";
import { fokosErrorWith } from "./errors-matchers.js";
import { TEST_ROUTING_HASH_BASE_SEED } from "./routing-hash.js";

/** A client of a table on `BaseSeedPartitionDO`, with eight roots, so most keys change root with the seed. */
function makeClient(tableName: string, routingHashBaseSeed?: string): FokosDB {
	return new FokosDB({
		table: { name: tableName, ns: "BASE_SEED_PARTITION_DO", nsTx: "TRANSACTION_COORDINATOR_DO", rootTreesN: 8, hashSplitN: 2 },
		rangeSplitN: 2,
		hashSplitConditions: { maxSizeMb: 100 },
		rangeSplitConditions: { maxSizeMb: 500 },
		routingHashBaseSeed,
	});
}

const KEYS = Array.from({ length: 16 }, (_, i) => `key-${i}`);

describe("FokosDB — routingHashBaseSeed", () => {
	it("serves every key when the client and the partition class have the same value", async () => {
		const db = makeClient(`baseseed.${crypto.randomUUID()}`, TEST_ROUTING_HASH_BASE_SEED);
		for (const hashKey of KEYS) {
			await db.putItem({ hashKey, data: hashKey });
		}
		for (const hashKey of KEYS) {
			expect((await db.getItem({ hashKey })).item).toBeDefined();
		}
	});

	it("refuses each key that a client with another value sends to a root that does not own it, and changes no item", async () => {
		const tableName = `baseseed.${crypto.randomUUID()}`;
		const other = makeClient(tableName);
		const refused = new Set<string>();
		for (const hashKey of KEYS) {
			try {
				await other.putItem({ hashKey, data: hashKey });
			} catch (error) {
				expect(error).toEqual(fokosErrorWith("partition_misrouted"));
				refused.add(hashKey);
			}
		}
		expect(refused.size).toBeGreaterThan(0);

		// A key that reached its owner root by chance is stored with the seed of the partition class.
		const db = makeClient(tableName, TEST_ROUTING_HASH_BASE_SEED);
		for (const hashKey of KEYS) {
			expect((await db.getItem({ hashKey })).item !== undefined).toBe(!refused.has(hashKey));
		}
	});
});
