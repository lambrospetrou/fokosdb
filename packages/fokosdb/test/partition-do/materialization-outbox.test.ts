import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { PartitionDO } from "../../src/server/do-partition.js";
import { FokosMigrationHost } from "../../src/shared/partition/fokos-migration-host.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { KeyCodec, type KeyBytes } from "../../src/sharding/key-codec.js";
import { kb, makeStub } from "./helpers.js";
import { PROMOTION_TEST_MAX_SIZE_MB, TestPartition, drainUntil, makePartition } from "./partition-harness.js";

type OutboxRow = { hk: string; revision: number; is_deleted: number; is_local_only: number };

async function outboxRows(stub: DurableObjectStub<PartitionDO>, hashKey?: string): Promise<OutboxRow[]> {
	return await runInDurableObject(stub, (_instance: PartitionDO, state: DurableObjectState) =>
		state.storage.sql
			.exec<{ hk: ArrayBuffer; revision: number; is_deleted: number; is_local_only: number }>(
				`SELECT hk, revision, is_deleted, is_local_only FROM materialization_outbox ${hashKey === undefined ? "" : "WHERE hk = ?"} ORDER BY hk, sk`,
				...(hashKey === undefined ? [] : [kb(hashKey)]),
			)
			.toArray()
			.map((row) => ({ ...row, hk: KeyCodec.decode(new Uint8Array(row.hk) as KeyBytes) as string })),
	);
}

describe("PartitionDO — outbox capture", () => {
	it("a table without capture writes no outbox row", async () => {
		const { ctx, stub, rpc } = makeStub();
		await rpc.apiPutItem(ctx, { hashKey: kb("a"), sortKey: kb("s"), data: "d", kind: "text" });
		await rpc.apiDeleteItem(ctx, { hashKey: kb("a"), sortKey: kb("s") });
		expect(await outboxRows(stub)).toEqual([]);
	});
});

// The export runs during these splits, so an outbox row can leave before or after the copy. The
// copy itself is checked without the export by the FokosMigrationHost test below.
describe("PartitionDO — outbox migration", () => {
	it("after a hash split no row stays on the source, and each row of a child is not local only", async () => {
		const partition = makePartition({ captureChanges: true, hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		await partition.triggerHashSplit();
		await partition.awaitSplitCompleted();
		const children = await partition.children();

		await drainUntil([partition], async () => (await outboxRows(partition.stub)).length === 0, "source cleanup of the outbox copies");
		const copied = (await Promise.all(children.map(async (child) => await outboxRows(child.stub)))).flat();
		expect(copied.every((row) => row.is_local_only === 0 && row.is_deleted === 0)).toBe(true);
	});

	it("after a promotion no row of the key stays on the source, and each row of the range root is not local only", async () => {
		const partition = makePartition({ captureChanges: true, hashSplitConditions: { maxSizeMb: PROMOTION_TEST_MAX_SIZE_MB } });
		await partition.triggerPromotion("alice");
		const root: TestPartition = await partition.awaitPromoted("alice");

		await drainUntil([partition], async () => (await outboxRows(partition.stub, "alice")).length === 0, "source cleanup of alice");
		expect((await outboxRows(root.stub)).every((row) => row.hk === "alice" && row.is_local_only === 0)).toBe(true);
	});
});

describe("FokosMigrationHost — the export fences of the outbox stream", () => {
	type Page = { page: unknown; nextCursor: unknown };
	const budget = { pageBytes: 1024 * 1024, pageRows: 1, scanRows: 100 };
	const slice = { kind: "promoted_key" as const, hashKey: kb("moved") };
	const inSlice = (key: { hashKey: KeyBytes }) => KeyCodec.compare(key.hashKey, kb("moved")) === 0;

	function hostOf(store: PartitionStore, selfId: string): FokosMigrationHost {
		return new FokosMigrationHost({ store, logParams: () => ({}), captureChanges: () => true, selfId: () => selfId });
	}

	it("the last page gives each target the current epoch of the source and its fences, and the pages raise no epoch", async () => {
		const source = makeStub({ captureChanges: true });
		const target = makeStub({ captureChanges: true });
		const pages = await runInDurableObject(source.stub, (_instance: PartitionDO, state: DurableObjectState) => {
			const store = new PartitionStore(state.storage, { captureChanges: () => true });
			for (const sk of ["1", "2"]) {
				store.upsertItem({ hk: kb("moved"), sk: kb(sk), data: "d", kind: "text", ttlAt: null, txOrderTs: 1 });
			}
			store.upsertItem({ hk: kb("stays"), sk: kb("s"), data: "d", kind: "text", ttlAt: null, txOrderTs: 1 });
			// The source is a split child too: its own inherited row keeps a fence of its parent.
			store.insertMigratedOutboxRow({ hk: kb("old"), sk: kb("s"), revision: 1, is_deleted: 1 });
			store.importExportFences("grandparent", 3, []);
			const before = store.exportState().epoch;

			const host = hostOf(store, "source-id");
			const out: Page[] = [];
			let cursor: unknown = { stream: "outbox", cursor: null };
			while (cursor !== null) {
				const page = host.buildPage(cursor, slice, inSlice, budget);
				out.push(page);
				cursor = page.nextCursor;
			}
			const after = store.exportState().epoch;
			// A second target of the same split gets the same epoch.
			const again = host.buildPage({ stream: "outbox", cursor: null }, slice, inSlice, { ...budget, pageRows: 10 });
			return { pages: out, again, before, after };
		});

		expect(pages.after).toBe(pages.before);
		const fences = { sourceId: "source-id", epoch: pages.after, inherited: [{ ancestorId: "grandparent", minEpoch: 3 }] };
		expect(pages.pages.map((p) => (p.page as { fences?: unknown }).fences)).toEqual([undefined, undefined, fences]);
		expect((pages.again.page as { fences?: unknown }).fences).toEqual(fences);

		const stored = await runInDurableObject(target.stub, (_instance: PartitionDO, state: DurableObjectState) => {
			const store = new PartitionStore(state.storage, { captureChanges: () => true });
			const host = hostOf(store, "target-id");
			for (const { page } of pages.pages) {
				host.applyPage(page, slice);
			}
			const untilId = state.storage.sql.exec<{ id: number }>(`SELECT MAX(outbox_id) AS id FROM materialization_outbox`).one().id;
			const rows = state.storage.sql
				.exec<{
					ancestor_id: string;
					min_epoch: number;
					until_id: number;
				}>(`SELECT ancestor_id, min_epoch, until_id FROM ancestor_fences ORDER BY ancestor_id`)
				.toArray();
			return { untilId, rows, outbox: store.dueOutboxRows(0, 10) };
		});
		expect(stored.outbox.map((row) => [KeyCodec.decode(row.sk), row.isLocalOnly])).toEqual([
			["1", false],
			["2", false],
		]);
		expect(stored.rows).toEqual([
			{ ancestor_id: "grandparent", min_epoch: 3, until_id: stored.untilId },
			{ ancestor_id: "source-id", min_epoch: pages.after, until_id: stored.untilId },
		]);
	});
});
