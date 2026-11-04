/**
 * Measures the storage of one `items` row, to calibrate `EST_ROW_BYTES_K`, and of one `latest_items`
 * row, to calibrate `LATEST_ROW_BYTES_K`. Each case inserts ROWS rows of one hash key, in sort-key
 * order, into a new partition. The value is the increase of `databaseSize` divided by the row count.
 * The bench prints the K that makes the size estimate equal to the measured value for each case. Run
 * it with `pnpm --filter fokosdb bench:workerd test/storage-bench/row-bytes.workerd-bench.ts`.
 */
import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { it } from "vitest";
import type { FokosDBPartitionDO } from "../../src/server/do-partition.js";
import type { FokosDBMaterializationDO } from "../../src/server/materialization/do-materialization.js";
import { MaterializationStore } from "../../src/server/materialization/materialization-store.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { testPartitionStub } from "../stub-helpers.js";
import { routingHashFor } from "../routing-hash.js";

const ROWS = 20_000;
/** [hash key bytes, sort key bytes, data bytes, TTL] */
const CASES: Array<[number, number, number, boolean]> = [
	[8, 8, 0, false],
	[8, 8, 100, false],
	[8, 32, 0, false],
	[8, 32, 100, false],
	[8, 32, 100, true],
	[8, 100, 0, false],
	[8, 100, 100, false],
];

it("measures the storage of one items row", async () => {
	const lines = ["| hk | sk | data | TTL | bytes for each row | K |", "| --- | --- | --- | --- | --- | --- |"];
	for (const [hkBytes, skBytes, dataBytes, ttl] of CASES) {
		const stub = testPartitionStub(`row-bytes.${crypto.randomUUID()}`);
		const perRow = await runInDurableObject(stub, (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
			const store = new PartitionStore(state.storage);
			// Encoded as a binary key, so each key has exactly the length of the case.
			const hk = KeyCodec.encode(new Uint8Array(hkBytes - 1).fill(0x61));
			const routingHash = routingHashFor(hk);
			const before = store.databaseSize;
			store.transactionSync(() => {
				for (let i = 0; i < ROWS; i++) {
					const sk = KeyCodec.encode(String(i).padStart(skBytes, "0"));
					const data = "d".repeat(dataBytes);
					store.upsertItem({ routingHash, hk, sk, data, kind: "text", ttlAt: ttl ? 2_000_000_000 : null, txOrderTs: 1 });
				}
			});
			return (store.databaseSize - before) / ROWS;
		});
		const k = perRow - dataBytes - hkBytes - skBytes;
		lines.push(`| ${hkBytes} | ${skBytes} | ${dataBytes} | ${ttl ? "yes" : "no"} | ${perRow.toFixed(0)} | ${k.toFixed(0)} |`);
	}
	console.log(lines.join("\n"));
});

/** [hash key bytes, sort key bytes, data bytes, tombstone] */
const LATEST_CASES: Array<[number, number, number, boolean]> = [
	[8, 8, 0, false],
	[8, 8, 100, false],
	[8, 32, 100, false],
	[8, 100, 100, false],
	[8, 8, 0, true],
	[8, 32, 0, true],
	[8, 100, 0, true],
];

it("measures the storage of one latest_items row", async () => {
	const lines = ["| hk | sk | data | tombstone | bytes for each row | K |", "| --- | --- | --- | --- | --- | --- |"];
	for (const [hkBytes, skBytes, dataBytes, deleted] of LATEST_CASES) {
		const stub = env.MATERIALIZATION_DO.getByName(`latest-row-bytes.${crypto.randomUUID()}`);
		const perRow = await runInDurableObject(stub, (_instance: FokosDBMaterializationDO, state: DurableObjectState) => {
			const store = new MaterializationStore(state.storage);
			const hashKey = KeyCodec.encode(new Uint8Array(hkBytes - 1).fill(0x61));
			const routingHash = routingHashFor(hashKey);
			const before = store.databaseSize;
			store.transactionSync(() => {
				for (let i = 0; i < ROWS; i++) {
					const sortKey = KeyCodec.encode(String(i).padStart(skBytes, "0"));
					const image = deleted
						? { opIndex: 0, hashKey, sortKey, revision: 1, deleted: true as const }
						: {
								opIndex: 0,
								hashKey,
								sortKey,
								revision: 1,
								deleted: false as const,
								data: "d".repeat(dataBytes),
								kind: "text" as const,
								ttlAt: null,
							};
					store.putLatestImage(routingHash, image, 1_000, undefined, []);
				}
			});
			return (store.databaseSize - before) / ROWS;
		});
		// The estimate counts the 8-byte routingHash and the keys two times (the row and the item-id index),
		// and three times for a tombstone (also the tombstone index).
		const k = perRow - dataBytes - (deleted ? 3 : 2) * (8 + hkBytes + skBytes);
		lines.push(`| ${hkBytes} | ${skBytes} | ${dataBytes} | ${deleted ? "yes" : "no"} | ${perRow.toFixed(0)} | ${k.toFixed(0)} |`);
	}
	console.log(lines.join("\n"));
});
