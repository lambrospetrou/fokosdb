import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { PartitionDO } from "../../server/do-partition.js";
import { testPartitionStub } from "../../../test/stub-helpers.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import { KeySizeStats } from "./key-size-stats.js";

const kb = (s: string) => KeyCodec.encode(s);
const keyLabel = (key: KeyBytes) => KeyCodec.decode(key) as string;

/** Runs `fn` with a `KeySizeStats` over the storage of a new partition, which already ran the migrations. */
async function withStats(fn: (stats: KeySizeStats, state: DurableObjectState) => void): Promise<void> {
	const stub = testPartitionStub(`key-sizes.${crypto.randomUUID()}`);
	await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) => {
		fn(new KeySizeStats(state.storage), state);
	});
}

describe("KeySizeStats", () => {
	it("adds, changes, and subtracts row sizes, and keeps no row for a key with no bytes", async () => {
		await withStats((stats, state) => {
			const rows = () => state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM key_size_estimates`).one().n;

			expect(stats.change(kb("k"), 0, 100)).toBe(100);
			expect(stats.change(kb("k"), 0, 50)).toBe(150);
			expect(stats.change(kb("k"), 100, 30)).toBe(80);
			stats.add(kb("k"), 20);
			stats.add(kb("k"), 0);
			expect(stats.bytesOf(kb("k"))).toBe(100);

			stats.subtract(kb("k"), 60);
			expect(stats.bytesOf(kb("k"))).toBe(40);
			stats.subtract(kb("k"), 40);
			expect(rows()).toBe(0);
			expect(stats.bytesOf(kb("k"))).toBe(0);

			stats.add(kb("other"), 10);
			stats.remove(kb("other"));
			expect(rows()).toBe(0);
		});
	});

	it("writes no row when the size of a row does not change", async () => {
		await withStats((stats, state) => {
			stats.change(kb("k"), 0, 100);
			// The rows that the connection inserted, updated, or deleted until now.
			const changes = () => state.storage.sql.exec<{ n: number }>(`SELECT total_changes() AS n`).one().n;
			const before = changes();
			expect(stats.change(kb("k"), 100, 100)).toBe(100);
			expect(changes()).toBe(before);
			stats.change(kb("k"), 100, 90);
			expect(changes()).toBeGreaterThan(before);
		});
	});

	// Every refused write on a hash leaf with two or more keys runs this query. Without its index, the
	// query reads one row for each hash key of the partition.
	it("largestAtLeast returns the large keys, largest first, from one seek of its index", async () => {
		await withStats((stats, state) => {
			for (let i = 1; i <= 12; i++) {
				stats.add(kb(`k${i}`), i * 100);
			}
			expect(stats.largestAtLeast(500, 8).map(keyLabel)).toEqual(["k12", "k11", "k10", "k9", "k8", "k7", "k6", "k5"]);
			expect(stats.largestAtLeast(1_100, 8).map(keyLabel)).toEqual(["k12", "k11"]);
			expect(stats.largestAtLeast(1_300, 8)).toEqual([]);

			const plan = state.storage.sql
				.exec<{ detail: string }>(
					`EXPLAIN QUERY PLAN SELECT hk FROM key_size_estimates WHERE est_bytes >= ? ORDER BY est_bytes DESC LIMIT ?`,
					500,
					8,
				)
				.toArray()
				.map((r) => r.detail)
				.join(" | ");
			expect(plan).toContain("key_size_estimates_by_bytes (est_bytes>?)");
			expect(plan).not.toContain("TEMP B-TREE");
		});
	});
});
