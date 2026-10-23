import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { MaterializationDO } from "./do-materialization.js";
import { KeyCodec } from "../../sharding/key-codec.js";
import { MaterializationStore, type LatestImage, type TargetPositions } from "./materialization-store.js";

const kb = (value: string) => KeyCodec.encode(value);
const NO_SK = KeyCodec.encodeOptional(undefined);

type Harness = {
	store: MaterializationStore;
	/** Applies revision `revision` of item `hk` as the ingest does, with the positions of the targets. */
	ingest: (hk: string, revision: number, targets: TargetPositions[], deleted?: boolean) => void;
	/** The positions of the partition, in `seq` order. */
	positions: () => Array<{ seq: number; item_id: number; prev_seq: number | null }>;
	newestSeq: (hk: string) => number | null | undefined;
};

async function withStore(fn: (h: Harness) => void): Promise<void> {
	const stub = env.MATERIALIZATION_DO.getByName(`mat-store.${crypto.randomUUID()}`);
	await runInDurableObject(stub, (_instance: MaterializationDO, state: DurableObjectState) => {
		const store = new MaterializationStore(state.storage);
		const image = (hk: string, revision: number, deleted: boolean): LatestImage =>
			deleted
				? { opIndex: 0, hashKey: kb(hk), sortKey: NO_SK, revision, deleted: true }
				: { opIndex: 0, hashKey: kb(hk), sortKey: NO_SK, revision, deleted: false, data: `v${revision}`, kind: "text", ttlAt: null };
		fn({
			store,
			ingest: (hk, revision, targets, deleted = false) =>
				store.putLatestImage(image(hk, revision, deleted), 1_000, store.storedImage(kb(hk), NO_SK), targets),
			positions: () =>
				state.storage.sql
					.exec<{
						seq: number;
						item_id: number;
						prev_seq: number | null;
					}>(`SELECT seq, item_id, prev_seq FROM change_positions ORDER BY seq`)
					.toArray(),
			newestSeq: (hk) => store.storedImage(kb(hk), NO_SK)?.newestSeq,
		});
	});
}

/** A target that read up to `cursor` and has no batch in flight. */
const at = (cursor: number): TargetPositions => ({ cursor, read: cursor });

describe("MaterializationStore — change positions", () => {
	it("writes no position when no target exists", async () => {
		await withStore((h) => {
			h.ingest("x", 1, []);
			expect(h.positions()).toEqual([]);
			expect(h.newestSeq("x")).toBeNull();
		});
	});

	it("keeps one position for each group of cursors while a target lags (steps 1, 4, and 3)", async () => {
		await withStore((h) => {
			const lake = at(10);
			// Step 1: X has no position. The new position is above each cursor.
			h.ingest("x", 1, [at(10), lake]);
			const itemId = h.positions()[0].item_id;
			expect(h.positions()).toEqual([{ seq: 11, item_id: itemId, prev_seq: null }]);

			// The GSI read 11. Step 4: lake needs 11, and 11 has no older position.
			h.ingest("x", 2, [at(11), lake]);
			expect(h.positions()).toEqual([
				{ seq: 11, item_id: itemId, prev_seq: null },
				{ seq: 12, item_id: itemId, prev_seq: 11 },
			]);
			expect(h.newestSeq("x")).toBe(12);

			// The GSI read 12. Step 3: lake reaches 11 first, so 12 moves.
			h.ingest("x", 3, [at(12), lake]);
			expect(h.positions()).toEqual([
				{ seq: 11, item_id: itemId, prev_seq: null },
				{ seq: 13, item_id: itemId, prev_seq: 11 },
			]);
			h.ingest("x", 4, [at(13), lake]);
			expect(h.positions().map((p) => p.seq)).toEqual([11, 14]);
			expect(h.newestSeq("x")).toBe(14);
		});
	});

	it("writes no position when no target read the newest position (step 2)", async () => {
		await withStore((h) => {
			h.ingest("x", 1, [at(0)]);
			h.ingest("x", 2, [at(0)]);
			h.ingest("x", 3, [at(0)], true);
			expect(h.positions().map((p) => p.seq)).toEqual([1]);
			expect(h.newestSeq("x")).toBe(1);
		});
	});

	it("gives a new position when a batch in flight read the newest position", async () => {
		await withStore((h) => {
			h.ingest("x", 1, [at(0)]);
			// The batch in flight read position 1, and its cursor is still 0. If it fails, it reads 1
			// again; if it applies, the cursor moves past 1, so the new image needs a new position.
			h.ingest("x", 2, [{ cursor: 0, read: 1 }]);
			expect(h.positions().map((p) => [p.seq, p.prev_seq])).toEqual([
				[1, null],
				[2, 1],
			]);
		});
	});

	it("counts a position at or below the lowest cursor as absent, and puts a new position above each cursor", async () => {
		await withStore((h) => {
			h.ingest("x", 1, [at(0)]);
			h.store.deletePositionsUpTo(1, 10);
			// The cursor is above each stored position, so the next position is above the cursor.
			h.ingest("x", 2, [at(20)]);
			expect(h.positions().map((p) => [p.seq, p.prev_seq])).toEqual([[21, null]]);
		});
	});
});
