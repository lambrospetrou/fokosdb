import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { FokosDBPartitionDO } from "../../server/do-partition.js";
import { testPartitionStub } from "../../../test/stub-helpers.js";
import { KeyCodec, type KeyBytes } from "../../sharding/key-codec.js";
import { asRoutingHash } from "../../sharding/hash-primitives.js";
import { KeySizeStats } from "./key-size-stats.js";
import { routingHashFor } from "../../../test/routing-hash.js";

/** The routingHash of a hash key, as the partition stores it. */
const P = (hk: KeyBytes) => routingHashFor(hk);

const kb = (s: string) => KeyCodec.encode(s);
const keyLabel = (key: KeyBytes) => KeyCodec.decode(key) as string;

/** Runs `fn` with a `KeySizeStats` over the storage of a new partition, which already ran the migrations. */
async function withStats(fn: (stats: KeySizeStats, state: DurableObjectState) => void): Promise<void> {
	const stub = testPartitionStub(`key-sizes.${crypto.randomUUID()}`);
	await runInDurableObject(stub, (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
		fn(new KeySizeStats(state.storage), state);
	});
}

describe("KeySizeStats", () => {
	it("adds, changes, and subtracts row sizes, and keeps no row for a routingHash with no bytes", async () => {
		await withStats((stats, state) => {
			const rows = () => state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM key_size_estimates`).one().n;

			expect(stats.change(P(kb("k")), kb("k"), 0, 100)).toBe(100);
			expect(stats.change(P(kb("k")), kb("k"), 0, 50)).toBe(150);
			expect(stats.change(P(kb("k")), kb("k"), 100, 30)).toBe(80);
			stats.add(P(kb("k")), kb("k"), 20);
			stats.add(P(kb("k")), kb("k"), 0);
			expect(stats.bytesOf(P(kb("k")))).toBe(100);

			stats.subtract(P(kb("k")), 60);
			expect(stats.bytesOf(P(kb("k")))).toBe(40);
			stats.subtract(P(kb("k")), 40);
			expect(rows()).toBe(0);
			expect(stats.bytesOf(P(kb("k")))).toBe(0);

			stats.add(P(kb("other")), kb("other"), 10);
			stats.remove(P(kb("other")));
			expect(rows()).toBe(0);
		});
	});

	it("writes no row when the size of a row does not change", async () => {
		await withStats((stats, state) => {
			stats.change(P(kb("k")), kb("k"), 0, 100);
			// The rows that the connection inserted, updated, or deleted until now.
			const changes = () => state.storage.sql.exec<{ n: number }>(`SELECT total_changes() AS n`).one().n;
			const before = changes();
			expect(stats.change(P(kb("k")), kb("k"), 100, 100)).toBe(100);
			expect(changes()).toBe(before);
			stats.change(P(kb("k")), kb("k"), 100, 90);
			expect(changes()).toBeGreaterThan(before);
		});
	});

	// Every refused write on a hash leaf with two or more keys runs this query. Without its index, the
	// query reads one row for each hash key of the partition.
	it("largestAtLeast returns the large keys, largest first, from one seek of its index", async () => {
		await withStats((stats, state) => {
			for (let i = 1; i <= 12; i++) {
				stats.add(P(kb(`k${i}`)), kb(`k${i}`), i * 100);
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

	it("keeps one total for the hash keys of one routingHash, and a rollback restores it", async () => {
		await withStats((stats, state) => {
			// Two hash keys with one routingHash: the test gives the routingHash, as a host does.
			const routingHash = asRoutingHash(Uint8Array.fromHex("0102030405060708"));
			expect(stats.change(routingHash, kb("a"), 0, 100)).toBe(100);
			expect(stats.change(routingHash, kb("b"), 0, 50)).toBe(150);
			expect(stats.bytesOf(routingHash)).toBe(150);
			// The first hash key stays the representative, also after its own bytes go.
			stats.subtract(routingHash, 100);
			expect(stats.largestAtLeast(0, 5).map(keyLabel)).toEqual(["a"]);
			expect(() =>
				state.storage.transactionSync(() => {
					stats.change(routingHash, kb("b"), 50, 500);
					throw new Error("roll back");
				}),
			).toThrow("roll back");
			expect(stats.bytesOf(routingHash)).toBe(50);
		});
	});

	it("adds the rows of a page once for each run of one routingHash", async () => {
		await withStats((stats) => {
			const p1 = asRoutingHash(Uint8Array.fromHex("0000000000000001"));
			const p2 = asRoutingHash(Uint8Array.fromHex("0000000000000002"));
			stats.addRows([
				{ routingHash: p1, hk: kb("a"), bytes: 10 },
				{ routingHash: p1, hk: kb("b"), bytes: 20 },
				{ routingHash: p2, hk: kb("c"), bytes: 5 },
			]);
			expect(stats.bytesOf(p1)).toBe(30);
			expect(stats.bytesOf(p2)).toBe(5);
		});
	});
});
