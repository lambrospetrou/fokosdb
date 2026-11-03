/**
 * Measures the storage of one `items` row, to calibrate `EST_ROW_BYTES_K`. Each case inserts ROWS rows
 * of one hash key, in sort-key order, into a new partition. The value is the increase of
 * `databaseSize` divided by the row count. The bench prints the K that makes `est_row_bytes`
 * (`data + hk + sk + K`) equal to the measured value for each case. Run it with
 * `pnpm --filter fokosdb bench:workerd test/storage-bench/row-bytes.workerd-bench.ts`.
 */
import { runInDurableObject } from "cloudflare:test";
import { it } from "vitest";
import type { FokosDBPartitionDO } from "../../src/server/do-partition.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { testPartitionStub } from "../stub-helpers.js";
import { prefixOf } from "../storage-prefix.js";

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
			const prefix = prefixOf(hk);
			const before = store.databaseSize;
			store.transactionSync(() => {
				for (let i = 0; i < ROWS; i++) {
					const sk = KeyCodec.encode(String(i).padStart(skBytes, "0"));
					const data = "d".repeat(dataBytes);
					store.upsertItem({ prefix, hk, sk, data, kind: "text", ttlAt: ttl ? 2_000_000_000 : null, txOrderTs: 1 });
				}
			});
			return (store.databaseSize - before) / ROWS;
		});
		const k = perRow - dataBytes - hkBytes - skBytes;
		lines.push(`| ${hkBytes} | ${skBytes} | ${dataBytes} | ${ttl ? "yes" : "no"} | ${perRow.toFixed(0)} | ${k.toFixed(0)} |`);
	}
	console.log(lines.join("\n"));
});
