// Property-based test of `db.scanItems` against a model: the sorted list of the stored keys.
//
// Each run writes a random set of items into a new table with a random number of roots, and reads it
// back with random page limits and a random segment count. The pages of all segments together must
// return each item exactly once, and in the storage order inside each segment. A gap, a duplicate, or a
// cursor that does not resume shows up as a mismatch with the model. scan-items.test.ts covers the
// splits and the promotions between two pages.
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import type { FokosDB } from "../../src/client/db.js";
import { createTableConfig } from "../../src/shared/partition-context.js";
import { routingHashOf } from "../../src/sharding/hash-primitives.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { compareStorageKeys } from "../../src/sharding/repartition-slice.js";
import { testRoutingHashSeedResolver } from "../routing-hash.js";
import { makeDB } from "../transactions/tx-helpers.js";
import { propertyRuns } from "./harness.js";

const RUNS = propertyRuns(8);

/** A string key, or a binary key, so that both kinds share one byte order. */
const keyPart = fc.oneof(
	fc.string({ unit: fc.constantFrom("a", "b", "z", "0", "#"), minLength: 1, maxLength: 3 }),
	fc.uint8Array({ minLength: 1, maxLength: 3 }).map((bytes) => bytes.map((b) => b || 1)),
);

function sortedKeys(db: FokosDB, keys: ReadonlyArray<{ hashKey: string | Uint8Array; sortKey: string | Uint8Array }>): string[] {
	const { shardGroup } = createTableConfig(db.options()).topology;
	const position = (key: (typeof keys)[number]) => {
		const hk = KeyCodec.encode(key.hashKey);
		return { routingHash: routingHashOf(hk, shardGroup, testRoutingHashSeedResolver), hk, sk: KeyCodec.encode(key.sortKey) };
	};
	return [...keys].sort((a, b) => compareStorageKeys(position(a), position(b))).map((key) => text(key));
}

const text = (key: { hashKey: unknown; sortKey?: unknown }) => {
	const show = (k: unknown) => (k instanceof Uint8Array ? `0x${k.toHex()}` : String(k));
	return `${show(key.hashKey)}/${show(key.sortKey)}`;
};

describe("FokosDB scanItems — model-based properties", () => {
	it(
		"returns each item exactly once across the pages of every segment, in the storage order",
		async () => {
			await fc.assert(
				fc.asyncProperty(
					fc.uniqueArray(fc.record({ hashKey: keyPart, sortKey: keyPart }), { maxLength: 30, selector: text }),
					fc.integer({ min: 1, max: 6 }),
					fc.integer({ min: 1, max: 8 }),
					fc.integer({ min: 1, max: 4 }),
					async (keys, rootTreesN, limit, totalSegments) => {
						const db = makeDB({ rootTreesN });
						for (const key of keys) {
							await db.putItem({ ...key, data: "v" });
						}
						const scanned: string[] = [];
						for (let segment = 0; segment < totalSegments; segment++) {
							let cursor: string | undefined;
							do {
								const page = await db.scanItems({ limit, segment, totalSegments, ...(cursor === undefined ? {} : { cursor }) });
								expect(page.scannedCount).toBeLessThanOrEqual(limit);
								scanned.push(...page.items.map(text));
								cursor = page.cursor;
							} while (cursor !== undefined);
						}
						expect(scanned).toEqual(sortedKeys(db, keys));
					},
				),
				{ numRuns: RUNS },
			);
		},
		RUNS * 30_000,
	);
});
