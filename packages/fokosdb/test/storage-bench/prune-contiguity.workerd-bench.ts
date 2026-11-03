/**
 * Measures the storage that a contiguous delete in the storage order of `items` gives back. Each case
 * fills a new partition to about 8 MiB with items in random key order, then deletes the rows of one
 * half, and then of three quarters, of the hashPrefixBucket space, as a hash child keeps 1/2 and then
 * 1/4 of its parent. Each delete is one key range of the primary key.
 *
 * The ratio is the `databaseSize` after the delete against the `databaseSize` before it. A contiguous
 * delete frees whole pages, so the ratio is near the part that stays. The case fails above that part
 * plus `TOLERANCE`. Run it with
 * `pnpm --filter fokosdb bench:workerd test/storage-bench/prune-contiguity.workerd-bench.ts`.
 */
import { runInDurableObject } from "cloudflare:test";
import { expect, it } from "vitest";
import type { FokosDBPartitionDO } from "../../src/server/do-partition.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { testPartitionStub } from "../stub-helpers.js";
import { prefixOf } from "../storage-prefix.js";

const FILL_BYTES = 8 * 1024 * 1024;
const DATA_BYTES = [100, 200, 500, 1_000, 1_500, 2_000];
/** The ratio can be this much above the part that stays, because the B-trees keep some part-full pages. */
const TOLERANCE = 0.1;
const ROWS_PER_TRANSACTION = 500;

/** The 8-byte hashPrefixBucket whose first byte is `first`, and the other bytes are 0. */
const bound = (first: number) => Uint8Array.from([first, 0, 0, 0, 0, 0, 0, 0]);

it.each(DATA_BYTES)("a contiguous delete of %i-byte items frees a proportional part of the storage", async (dataBytes) => {
	const stub = testPartitionStub(`prune-contiguity.${crypto.randomUUID()}`);
	const { half, quarter } = await runInDurableObject(stub, (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
		const store = new PartitionStore(state.storage);
		const sql = state.storage.sql;
		let rows = 0;
		while (store.databaseSize < FILL_BYTES) {
			store.transactionSync(() => {
				for (let i = 0; i < ROWS_PER_TRANSACTION; i++) {
					const hk = KeyCodec.encode(crypto.randomUUID());
					const data = crypto.getRandomValues(new Uint8Array(dataBytes));
					store.upsertItem({ prefix: prefixOf(hk), hk, sk: KeyCodec.encode("s"), data, kind: "bytes", ttlAt: null, txOrderTs: 1 });
				}
			});
			rows += ROWS_PER_TRANSACTION;
		}
		const full = store.databaseSize;
		// Keep [0, 2^63): delete each row from the middle of the space to its end.
		sql.exec(`DELETE FROM items WHERE hash_prefix_bucket >= ?`, bound(0x80));
		sql.exec(`DELETE FROM key_size_estimates WHERE hash_prefix_bucket >= ?`, bound(0x80));
		const half = store.databaseSize / full;
		// Keep [0, 2^62).
		sql.exec(`DELETE FROM items WHERE hash_prefix_bucket >= ?`, bound(0x40));
		sql.exec(`DELETE FROM key_size_estimates WHERE hash_prefix_bucket >= ?`, bound(0x40));
		const quarter = store.databaseSize / full;
		console.log(
			`| ${dataBytes} | ${rows} | ${(full / 1024 / 1024).toFixed(2)} MiB | ${(half * 100).toFixed(1)}% | ${(quarter * 100).toFixed(1)}% |`,
		);
		return { half, quarter };
	});
	expect(half).toBeLessThanOrEqual(0.5 + TOLERANCE);
	expect(quarter).toBeLessThanOrEqual(0.25 + TOLERANCE);
});
