import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import invariant from "../../src/shared/invariant.js";
import { KeyCodec, type KeyBytes } from "../../src/sharding/key-codec.js";
import { isRangePartition } from "../../src/sharding/partition-context.js";
import { kb } from "./helpers.js";
import { makePartition, makeRangeRoot, PROMOTION_TEST_MAX_SIZE_MB, type TestPartition } from "./partition-harness.js";

// The seeded state migration has to carry: a marker item whose read watermark is above its write
// watermark (so the two columns are visibly different), and one real user delete so both deletion
// metadata fields are non-zero.
const SEED = { readTs: 999_000_000, writeTs: 500_000, deleteWatermarkTs: 777_000_000 } as const;

/**
 * Seeds the marker and a victim row directly in the store (the item timestamps are far below the
 * wall clock, so they must be written with explicit stamps), bumps the marker's read watermark,
 * then deletes the victim so `getDeletionMetadata()` is
 * `{ maxDeleteTxOrderTs: 777_000_000, deleteRevision: 1 }`.
 */
async function seedForMigration(node: TestPartition, marker: { hk: KeyBytes; sk: KeyBytes }, victim: { hk: KeyBytes; sk: KeyBytes }) {
	return await runInDurableObject(node.stub, (_instance: PartitionDO, state: DurableObjectState) => {
		const store = new PartitionStore(state.storage);
		store.upsertItem({ hk: marker.hk, sk: marker.sk, data: "marker-data", kind: "text", ttlAt: null, txOrderTs: SEED.writeTs });
		store.upsertItem({ hk: victim.hk, sk: victim.sk, data: "victim-data", kind: "text", ttlAt: null, txOrderTs: SEED.writeTs });
		store.bumpItemReadTs(marker.hk, marker.sk, SEED.readTs);
		store.deleteItem({ hk: victim.hk, sk: victim.sk, txOrderTs: SEED.deleteWatermarkTs });
		return { row: store.getItem(marker.hk, marker.sk).row, meta: store.getDeletionMetadata() };
	});
}

/** Reads the marker row and the deletion metadata of `node` straight from its storage. */
async function storeStateOf(node: TestPartition, marker: { hk: KeyBytes; sk: KeyBytes }) {
	return await runInDurableObject(node.stub, (_instance: PartitionDO, state: DurableObjectState) => {
		const store = new PartitionStore(state.storage);
		return { row: store.getItem(marker.hk, marker.sk).row, meta: store.getDeletionMetadata() };
	});
}

describe("PartitionDO — migration carries item timestamps and deletion metadata", () => {
	it("a hash split copies both item timestamps and both deletion-metadata values", async () => {
		const root = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		const marker = { hk: kb("marker"), sk: kb("sk") };
		const seeded = await seedForMigration(root, marker, { hk: kb("victim"), sk: kb("sk") });
		expect(seeded.row).toMatchObject({ last_read_ts: SEED.readTs, last_write_ts: SEED.writeTs });
		expect(seeded.meta).toEqual({ maxDeleteTxOrderTs: SEED.deleteWatermarkTs, deleteRevision: 1 });

		const children = await root.splitHash();

		const owner = await root.childOwning("marker");
		expect((await storeStateOf(owner, marker)).row).toMatchObject({
			v: 1,
			data: "marker-data",
			last_read_ts: SEED.readTs,
			last_write_ts: SEED.writeTs,
		});

		// The deletion metadata is partition-wide, so every child inherits it.
		for (const child of children) {
			expect((await storeStateOf(child, marker)).meta).toEqual(seeded.meta);
		}
	});

	it("a child keeps the inherited timestamps and metadata after it starts serving traffic", async () => {
		const root = makePartition({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		const marker = { hk: kb("marker"), sk: kb("sk") };
		const seeded = await seedForMigration(root, marker, { hk: kb("victim"), sk: kb("sk") });

		await root.splitHash();
		const child = await root.childOwning("marker");

		// Ordinary traffic on the child.
		expect(await child.get({ hashKey: marker.hk, sortKey: marker.sk })).toMatchObject({ found: true });
		await child.put({ hashKey: kb("new-key"), sortKey: kb("sk"), data: "new", kind: "text" });

		const afterTraffic = await storeStateOf(child, marker);
		expect(afterTraffic.row).toMatchObject({ last_read_ts: SEED.readTs, last_write_ts: SEED.writeTs });
		expect(afterTraffic.meta).toEqual(seeded.meta);

		// A put on the marker lands at the child's own clock, above both inherited watermarks.
		await child.put({ hashKey: marker.hk, sortKey: marker.sk, data: "rewritten", kind: "text" });
		const afterPut = await storeStateOf(child, marker);
		expect(afterPut.row?.last_read_ts).toBe(afterPut.row?.last_write_ts);
		expect(afterPut.row?.last_read_ts).toBeGreaterThan(SEED.readTs);
	});

	it("a range promotion copies both item timestamps and both deletion-metadata values", async () => {
		const hash = makePartition({
			hashSplitN: 2,
			hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB },
			rangeSplitN: 2,
			rangeSplitConditions: { maxSizeMb: 500 },
		});
		// The marker sits under the hash key that will be promoted, so the range root inherits it.
		const marker = { hk: kb("hot"), sk: kb("marker") };
		const seeded = await seedForMigration(hash, marker, { hk: kb("victim"), sk: kb("sk") });

		await hash.triggerPromotion("hot");
		const rangeRoot = await hash.awaitPromoted("hot");

		const onRoot = await storeStateOf(rangeRoot, marker);
		expect(onRoot.row).toMatchObject({ v: 1, last_read_ts: SEED.readTs, last_write_ts: SEED.writeTs });
		expect(onRoot.meta).toEqual(seeded.meta);
	});

	it("a range split copies both item timestamps and both deletion-metadata values", async () => {
		const { root } = await makeRangeRoot(2);
		const marker = { hk: kb("alice"), sk: kb("zz-marker") };
		const seeded = await seedForMigration(root, marker, { hk: kb("alice"), sk: kb("zz-victim") });

		const children = await root.splitRange("aa");

		// The marker lands in exactly one child, by the sort-key boundaries of the split.
		const owners = children.filter((child) => {
			invariant(isRangePartition(child.ctx), `${child.doName}: not a range partition`);
			const { startBoundary: start, endBoundary: end } = child.ctx.rangePartition;
			return (start === null || KeyCodec.compare(marker.sk, start) >= 0) && (end === null || KeyCodec.compare(marker.sk, end) < 0);
		});
		expect(owners).toHaveLength(1);
		expect((await storeStateOf(owners[0], marker)).row).toMatchObject({
			v: 1,
			last_read_ts: SEED.readTs,
			last_write_ts: SEED.writeTs,
		});

		for (const child of children) {
			expect((await storeStateOf(child, marker)).meta).toEqual(seeded.meta);
		}
	});
});
