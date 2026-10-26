/**
 * `gsiWriteEntries`: the internal partition operation that a `FokosDBMaterializationDO` writes a GSI table with.
 */
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { GsiWriteEntriesRequest, GsiWriteEntriesResponse, GsiWriteEntry, FokosDBPartitionDO } from "../../src/server/do-partition.js";
import { FokosMigrationHost } from "../../src/shared/partition/fokos-migration-host.js";
import { PartitionStore } from "../../src/shared/partition/partition-store.js";
import { gsiItemSortKey } from "../../src/shared/gsi/sort-key.js";
import type { FokosEnvelope } from "../../src/sharding/runtime-types.js";
import { kb, makeStub } from "./helpers.js";

const PRODUCER = "a".repeat(64);
const OTHER = "b".repeat(64);

const sk = (gsiSK: string | undefined, baseHK: string, baseSK?: string) => gsiItemSortKey(kb(gsiSK), kb(baseHK), kb(baseSK));
const put = (opIndex: number, gsiHK: string, gsiSK: string, baseHK: string, data = "d"): GsiWriteEntry => ({
	opIndex,
	hashKey: kb(gsiHK),
	sortKey: sk(gsiSK, baseHK),
	op: "put",
	data,
	kind: "text",
	baseRevision: 1,
});
const del = (opIndex: number, gsiHK: string, gsiSK: string, baseHK: string): GsiWriteEntry => ({
	opIndex,
	hashKey: kb(gsiHK),
	sortKey: sk(gsiSK, baseHK),
	op: "delete",
});

function gsiTable() {
	const t = makeStub();
	return {
		...t,
		async write(req: Partial<GsiWriteEntriesRequest> & { items: GsiWriteEntry[] }): Promise<GsiWriteEntriesResponse> {
			const env = (await t.stub.gsiWriteEntries(t.ctx, {
				producerId: PRODUCER,
				epoch: 1,
				ancestorFences: [],
				...req,
			})) as unknown as FokosEnvelope<GsiWriteEntriesResponse>;
			return env.value;
		},
		async epochs(): Promise<Record<string, number>> {
			return await runInDurableObject(t.stub, (_i: FokosDBPartitionDO, state: DurableObjectState) =>
				Object.fromEntries(
					state.storage.sql
						.exec<{ producer_id: string; epoch: number }>(`SELECT producer_id, epoch FROM producer_fences`)
						.toArray()
						.map((r) => [r.producer_id, r.epoch]),
				),
			);
		},
		async count(): Promise<number> {
			return await runInDurableObject(
				t.stub,
				(_i: FokosDBPartitionDO, state: DurableObjectState) =>
					state.storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM items`).one().n,
			);
		},
	};
}

describe("FokosDBPartitionDO — gsiWriteEntries", () => {
	it("applies puts and deletes, and stores the epoch of the producer", async () => {
		const t = gsiTable();
		const res = await t.write({ epoch: 2, items: [put(0, "g", "a", "u1"), put(1, "g", "a", "u2")] });
		expect(res).toMatchObject({ applied: [{ opIndex: 0 }, { opIndex: 1 }], failed: [] });
		expect(await t.count()).toBe(2);
		expect(await t.epochs()).toEqual({ [PRODUCER]: 2 });

		expect(await t.write({ epoch: 2, items: [del(0, "g", "a", "u1")] })).toMatchObject({ applied: [{ opIndex: 0 }], failed: [] });
		expect(await t.count()).toBe(1);
	});

	it("refuses a call below the stored epoch with producer_epoch_fenced and writes nothing", async () => {
		const t = gsiTable();
		await t.write({ epoch: 3, items: [put(0, "g", "a", "u1")] });
		const res = await t.write({ epoch: 2, items: [del(0, "g", "a", "u1"), put(1, "g", "b", "u2")] });
		expect(res).toMatchObject({
			applied: [],
			failed: [
				{ outcome: "unprocessed", code: "producer_epoch_fenced", opIndexes: [0] },
				{ outcome: "unprocessed", code: "producer_epoch_fenced", opIndexes: [1] },
			],
		});
		expect(await t.count()).toBe(1);
		// The epochs of one producer do not fence another producer.
		expect(await t.write({ producerId: OTHER, epoch: 1, items: [put(0, "g", "b", "u2")] })).toMatchObject({ failed: [] });
		expect(await t.epochs()).toEqual({ [PRODUCER]: 3, [OTHER]: 1 });
	});

	it("raises the epoch of each ancestor fence before it checks the producer, and never lowers it", async () => {
		const t = gsiTable();
		await t.write({
			producerId: OTHER,
			epoch: 1,
			ancestorFences: [{ ancestorId: PRODUCER, minEpoch: 5 }],
			items: [put(0, "g", "a", "u1")],
		});
		expect(await t.epochs()).toEqual({ [PRODUCER]: 5, [OTHER]: 1 });
		expect(await t.write({ epoch: 4, items: [del(0, "g", "a", "u1")] })).toMatchObject({ failed: [{ code: "producer_epoch_fenced" }] });
		await t.write({
			producerId: OTHER,
			epoch: 1,
			ancestorFences: [{ ancestorId: PRODUCER, minEpoch: 3 }],
			items: [put(0, "g", "b", "u1")],
		});
		expect(await t.epochs()).toEqual({ [PRODUCER]: 5, [OTHER]: 1 });
	});

	it("the migration copies the whole producer_fences table to a target and merges it with MAX", async () => {
		const source = gsiTable();
		await source.write({ epoch: 7, items: [put(0, "g", "a", "u1")] });
		await source.write({ producerId: OTHER, epoch: 2, items: [put(0, "h", "a", "u1")] });
		const slice = { kind: "promoted_key" as const, hashKey: kb("none") };
		const inSlice = () => false;
		const budget = { pageBytes: 1 << 20, pageRows: 1, scanRows: 100 };
		const host = (store: PartitionStore) =>
			new FokosMigrationHost({ store, logParams: () => ({}), captureChanges: () => false, selfId: () => "self" });
		const pages = await runInDurableObject(source.stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => {
			const h = host(new PartitionStore(state.storage));
			const out: unknown[] = [];
			let cursor: unknown = { stream: "pending_tx", cursor: null };
			while (cursor !== null) {
				const page = h.buildPage(cursor, slice, inSlice, budget);
				h.validatePage(cursor, page.page, page.nextCursor, inSlice);
				out.push(page.page);
				cursor = page.nextCursor;
			}
			return out;
		});
		expect(pages.filter((p) => (p as { stream: string }).stream === "producer_fences")).toEqual([
			{ stream: "producer_fences", fences: [{ producerId: PRODUCER, epoch: 7 }] },
			{ stream: "producer_fences", fences: [{ producerId: OTHER, epoch: 2 }] },
			{ stream: "producer_fences", fences: [] },
		]);

		const target = gsiTable();
		await target.write({ epoch: 9, items: [put(0, "x", "a", "u1")] });
		await runInDurableObject(target.stub, (_i: FokosDBPartitionDO, state: DurableObjectState) => {
			const h = host(new PartitionStore(state.storage));
			for (const page of pages) {
				h.applyPage(page, slice);
			}
		});
		expect(await target.epochs()).toEqual({ [PRODUCER]: 9, [OTHER]: 2 });
	});
});
