import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { ApplyLatestImagesRequest, LatestImage, MaterializationDO } from "../../src/server/materialization/do-materialization.js";
import { materializationStubByName } from "../../src/shared/do-stubs.js";
import { createTableConfig, materializationTableConfig, type FokosDBRouteContext } from "../../src/shared/partition-context.js";
import { KeyCodec } from "../../src/sharding/key-codec.js";
import { FokosRouter } from "../../src/sharding/router.js";
import { DATA_KINDS } from "../../src/shared/types.js";
import { MaterializationStore } from "../../src/server/materialization/materialization-store.js";
import { kb } from "../partition-do/helpers.js";

type StoredImage = {
	revision: number;
	deleted: boolean;
	data: SqlStorageValue;
	data_kind: number | null;
	ttl_at: number | null;
	est_row_bytes: number;
};

/**
 * A root of the materialization group of a new table, and the router of that group. A partition with
 * no target deletes a tombstone at once, so a test that reads a tombstone uses `keepTombstones`: the
 * partition then has a push target that refuses each item, and the cursor of the target stays.
 */
function makeMaterialization(opts: { rootTreesN?: number; maxSizeMb?: number; keepTombstones?: boolean } = {}) {
	const table = createTableConfig({
		table: {
			name: `mat.${crypto.randomUUID()}`,
			ns: "PARTITION_DO",
			nsTx: "TRANSACTION_COORDINATOR_DO",
			nsMat: opts.keepTombstones ? "PUSH_MATERIALIZATION_DO" : "MATERIALIZATION_DO",
			rootTreesN: opts.rootTreesN ?? 1,
			hashSplitN: 2,
		},
		captureChanges: true,
		hashSplitConditions: { maxSizeMb: opts.maxSizeMb ?? 100 },
	});
	const mat = materializationTableConfig(table);
	const router = new FokosRouter(mat.topology, mat.rangeConfig, mat.policy, mat.policyVersion);
	const at = (hashKey: string) => {
		const ctx = router.rootContext(kb(hashKey));
		return { ctx, stub: materializationStubByName(env, ctx, ctx.doName) };
	};
	return { at, refuse: async (hashKey: string) => await env.PUSH_SINK_DO.getByName("sink").refuse(at(hashKey).stub.id.toString()) };
}

const PRODUCER = "aa".repeat(32);
const OTHER_PRODUCER = "bb".repeat(32);

function live(opIndex: number, hashKey: string, revision: number, data = `v${revision}`): Extract<LatestImage, { deleted: false }> {
	return { opIndex, hashKey: kb(hashKey), sortKey: kb("s"), revision, deleted: false, data, kind: "text", ttlAt: null };
}
function deleted(opIndex: number, hashKey: string, revision: number): LatestImage {
	return { opIndex, hashKey: kb(hashKey), sortKey: kb("s"), revision, deleted: true };
}

async function apply(
	target: { ctx: FokosDBRouteContext; stub: DurableObjectStub<MaterializationDO> },
	req: Partial<ApplyLatestImagesRequest>,
) {
	const envelope = await target.stub.applyLatestImages(target.ctx, {
		producerId: PRODUCER,
		epoch: 1,
		ancestorFences: [],
		items: [],
		...req,
	});
	return envelope.value;
}

async function storedImage(stub: DurableObjectStub<MaterializationDO>, hashKey: string): Promise<StoredImage | undefined> {
	return await runInDurableObject(stub, (_instance: MaterializationDO, state: DurableObjectState) => {
		const row = state.storage.sql
			.exec<
				Omit<StoredImage, "deleted"> & { deleted_at_ms: number | null }
			>(`SELECT revision, deleted_at_ms, data, data_kind, ttl_at, est_row_bytes FROM latest_items WHERE base_hk = ? AND base_sk = ?`, kb(hashKey), kb("s"))
			.toArray()[0];
		if (!row) {
			return undefined;
		}
		const { deleted_at_ms, ...rest } = row;
		return { ...rest, deleted: deleted_at_ms !== null };
	});
}

async function keyEstimate(stub: DurableObjectStub<MaterializationDO>, hashKey: string): Promise<number | undefined> {
	return await runInDurableObject(
		stub,
		(_instance: MaterializationDO, state: DurableObjectState) =>
			state.storage.sql.exec<{ est_bytes: number }>(`SELECT est_bytes FROM key_size_estimates WHERE hk = ?`, kb(hashKey)).toArray()[0]
				?.est_bytes,
	);
}

async function producerEpoch(stub: DurableObjectStub<MaterializationDO>, producerId: string): Promise<number | undefined> {
	return await runInDurableObject(
		stub,
		(_instance: MaterializationDO, state: DurableObjectState) =>
			state.storage.sql.exec<{ epoch: number }>(`SELECT epoch FROM producer_fences WHERE producer_id = ?`, producerId).toArray()[0]?.epoch,
	);
}

const ackedOpIndexes = (res: { acknowledged: Array<{ opIndex: number }> }) => res.acknowledged.map((a) => a.opIndex).sort((a, b) => a - b);

describe("MaterializationDO — applyLatestImages revisions", () => {
	it("applies a newer revision, and acknowledges an equal or older revision with no write", async () => {
		const m = makeMaterialization().at("x");
		expect(await apply(m, { items: [live(0, "x", 2)] })).toMatchObject({ acknowledged: [{ opIndex: 0 }], failed: [] });
		expect(await storedImage(m.stub, "x")).toMatchObject({ revision: 2, deleted: false, data: "v2" });

		// The same revision again, and an older revision: both acknowledged, and the stored image stays.
		expect(await apply(m, { items: [live(0, "x", 2, "again"), live(1, "x", 1, "old")] })).toMatchObject({ failed: [] });
		expect(await storedImage(m.stub, "x")).toMatchObject({ revision: 2, data: "v2" });

		expect(await apply(m, { items: [live(0, "x", 5)] })).toMatchObject({ acknowledged: [{ opIndex: 0 }] });
		expect(await storedImage(m.stub, "x")).toMatchObject({ revision: 5, data: "v5" });
		// The update has the same size, so the estimate of the hash key stays the size of its one row.
		expect(await keyEstimate(m.stub, "x")).toBe((await storedImage(m.stub, "x"))!.est_row_bytes);
	});

	it("a delete keeps a tombstone with no data, and a late live image below it does not create the item again", async () => {
		const mat = makeMaterialization({ keepTombstones: true });
		await mat.refuse("x");
		const m = mat.at("x");
		await apply(m, { items: [live(0, "x", 1, "x".repeat(1000))] });
		const liveBytes = (await storedImage(m.stub, "x"))!.est_row_bytes;
		expect(await keyEstimate(m.stub, "x")).toBe(liveBytes);

		expect(await apply(m, { items: [deleted(0, "x", 2)] })).toMatchObject({ acknowledged: [{ opIndex: 0 }] });
		const tombstone = (await storedImage(m.stub, "x"))!;
		expect(tombstone).toMatchObject({ revision: 2, deleted: true, data: null, data_kind: null, ttl_at: null });
		expect(tombstone.est_row_bytes).toBeLessThan(liveBytes);
		expect(await keyEstimate(m.stub, "x")).toBe(tombstone.est_row_bytes);

		expect(await apply(m, { items: [live(0, "x", 1)] })).toMatchObject({ acknowledged: [{ opIndex: 0 }] });
		expect(await storedImage(m.stub, "x")).toMatchObject({ revision: 2, deleted: true });
	});

	it("stores a delete of an item that it never saw as a tombstone", async () => {
		const mat = makeMaterialization({ keepTombstones: true });
		await mat.refuse("x");
		const m = mat.at("x");
		await apply(m, { items: [deleted(0, "x", 4)] });
		expect(await storedImage(m.stub, "x")).toMatchObject({ revision: 4, deleted: true, data: null });
	});

	it("stores the data, the kind and the TTL of a live image unchanged", async () => {
		const m = makeMaterialization().at("x");
		const bytes = new Uint8Array([1, 2, 3]);
		await apply(m, { items: [{ ...live(0, "x", 1), data: bytes, kind: "bytes", ttlAt: 1_900_000_000 }] });
		const stored = (await storedImage(m.stub, "x"))!;
		expect(new Uint8Array(stored.data as ArrayBuffer)).toEqual(bytes);
		expect(stored).toMatchObject({ data_kind: DATA_KINDS.indexOf("bytes"), ttl_at: 1_900_000_000 });
	});
});

describe("MaterializationDO — applyLatestImages producer epochs", () => {
	it("stores a higher epoch, and refuses a call below the stored epoch with producer_epoch_fenced", async () => {
		const m = makeMaterialization().at("x");
		await apply(m, { epoch: 2, items: [live(0, "x", 1)] });
		expect(await producerEpoch(m.stub, PRODUCER)).toBe(2);

		const fenced = await apply(m, { epoch: 1, items: [live(0, "x", 9)] });
		expect(fenced).toMatchObject({
			acknowledged: [],
			failed: [{ outcome: "unprocessed", code: "producer_epoch_fenced", opIndexes: [0] }],
		});
		expect(await storedImage(m.stub, "x")).toMatchObject({ revision: 1 });

		// The epochs of one producer do not fence another producer.
		expect(await apply(m, { producerId: OTHER_PRODUCER, epoch: 1, items: [live(0, "x", 3)] })).toMatchObject({ failed: [] });
		expect(await storedImage(m.stub, "x")).toMatchObject({ revision: 3 });
	});

	it("writes a fence row only when an epoch goes up", async () => {
		const m = makeMaterialization().at("x");
		await apply(m, { epoch: 3, items: [live(0, "x", 1)] });
		const changes = await runInDurableObject(m.stub, (_instance: MaterializationDO, state: DurableObjectState) => {
			const store = new MaterializationStore(state.storage);
			const changed = () => state.storage.sql.exec<{ n: number }>(`SELECT changes() AS n`).one().n;
			const out: number[] = [];
			for (const epoch of [3, 2, 4]) {
				store.raiseProducerEpoch(PRODUCER, epoch);
				out.push(changed());
			}
			return out;
		});
		expect(changes).toEqual([0, 0, 1]);
		expect(await producerEpoch(m.stub, PRODUCER)).toBe(4);
	});

	it("an ancestor fence of a child call fences the late calls of the ancestor", async () => {
		const m = makeMaterialization().at("x");
		const child = OTHER_PRODUCER;
		await apply(m, { producerId: child, epoch: 1, ancestorFences: [{ ancestorId: PRODUCER, minEpoch: 5 }], items: [live(0, "x", 2)] });
		expect(await producerEpoch(m.stub, PRODUCER)).toBe(5);

		expect(await apply(m, { producerId: PRODUCER, epoch: 4, items: [live(0, "x", 9)] })).toMatchObject({
			failed: [{ code: "producer_epoch_fenced" }],
		});
		// A fence never lowers an epoch.
		await apply(m, { producerId: child, epoch: 1, ancestorFences: [{ ancestorId: PRODUCER, minEpoch: 3 }], items: [live(0, "x", 3)] });
		expect(await producerEpoch(m.stub, PRODUCER)).toBe(5);
		expect(await storedImage(m.stub, "x")).toMatchObject({ revision: 3 });
	});
});

describe("MaterializationDO — applyLatestImages size and routing", () => {
	it("above its size limit refuses each newer live image with partition_over_size, and still applies each delete and each fence", async () => {
		const mat = makeMaterialization({ maxSizeMb: 0.0001, keepTombstones: true });
		await mat.refuse("x");
		const m = mat.at("x");
		const res = await apply(m, {
			epoch: 2,
			ancestorFences: [{ ancestorId: OTHER_PRODUCER, minEpoch: 4 }],
			items: [live(0, "x", 1), deleted(1, "y", 2)],
		});
		expect(res).toMatchObject({
			acknowledged: [{ opIndex: 1 }],
			failed: [{ outcome: "unprocessed", code: "partition_over_size", opIndexes: [0] }],
		});
		expect(await storedImage(m.stub, "x")).toBeUndefined();
		// The receiver rules ran for the refused image too.
		expect(await producerEpoch(m.stub, PRODUCER)).toBe(2);
		expect(await producerEpoch(m.stub, OTHER_PRODUCER)).toBe(4);
		// A live image at or below the stored revision needs no write, so it is acknowledged.
		expect(await apply(m, { epoch: 2, items: [live(0, "y", 1)] })).toMatchObject({ acknowledged: [{ opIndex: 0 }], failed: [] });
	});

	it("answers each item of a call by its opIndex, at the root that owns its key", async () => {
		const mat = makeMaterialization({ rootTreesN: 4 });
		const keys = Array.from({ length: 12 }, (_, i) => `k${i}`);
		// A caller groups the items by root, as FokosShardingClient.resolveAll does, and sends one call to each root.
		const byRoot = Map.groupBy(
			keys.map((k, i) => live(i, k, 1)),
			(image) => mat.at(KeyCodec.decode(image.hashKey) as string).ctx.doName,
		);
		expect(byRoot.size).toBeGreaterThan(1);
		for (const images of byRoot.values()) {
			const res = await apply(mat.at(KeyCodec.decode(images[0].hashKey) as string), { items: images });
			expect(res.failed).toEqual([]);
			expect(ackedOpIndexes(res)).toEqual(images.map((image) => image.opIndex).sort((a, b) => a - b));
		}
		for (const k of keys) {
			expect(await storedImage(mat.at(k).stub, k)).toMatchObject({ revision: 1 });
		}
	});
});
