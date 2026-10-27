import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { ApplyLatestImagesRequest, FokosDBMaterializationDO } from "../../src/server/materialization/do-materialization.js";
import type { LatestImage } from "../../src/server/materialization/materialization-store.js";
import { materializationTableConfig, type FokosDBRouteContext } from "../../src/shared/partition-context.js";
import { materializationStubByName } from "../../src/shared/do-stubs.js";
import { FokosRouter } from "../../src/sharding/router.js";
import type { FokosPartitionRef } from "../../src/sharding/route-context.js";
import { kb, makeStub, type PartitionOptions } from "../partition-do/helpers.js";
import type { ReceivedPush } from "./push-target-host.js";

/** The producer of the test exports. */
const PRODUCER_ID = "dd".repeat(32);
const DONE_STATES = ["completed", "cleaned"];

type Mat = { ctx: FokosDBRouteContext; stub: DurableObjectStub<FokosDBMaterializationDO>; ref: FokosPartitionRef };

/** The materialization root of a new table whose materialization class has the push target `sink`. */
function materializationRoot(opts: PartitionOptions): Mat {
	const { ctx: base } = makeStub({ captureChanges: true, nsMat: "PUSH_MATERIALIZATION_DO", ...opts });
	const mat = materializationTableConfig({
		topology: base.topology,
		rangeConfig: base.rangeConfig,
		policy: base.policy,
		policyVersion: base.policyVersion,
	});
	const ctx = new FokosRouter(mat.topology, mat.rangeConfig, mat.policy, mat.policyVersion).rootContext(kb("any"));
	return { ctx, stub: materializationStubByName(env, ctx, ctx.doName), ref: { partitionId: ctx.partitionId, doName: ctx.doName } };
}

function stubOf(root: Mat, ref: FokosPartitionRef): DurableObjectStub<FokosDBMaterializationDO> {
	return materializationStubByName(env, root.ctx, ref.doName);
}

async function apply(root: Mat, items: Array<Omit<Extract<LatestImage, { deleted: false }>, "opIndex">>) {
	const req: ApplyLatestImagesRequest = {
		producerId: PRODUCER_ID,
		epoch: 1,
		ancestorFences: [],
		items: items.map((item, opIndex) => ({ ...item, opIndex })),
	};
	return (await root.stub.applyLatestImages(root.ctx, req)).value;
}

/** Runs the due work of each partition until `check` passes. */
async function driveUntil(
	stubs: () => Promise<DurableObjectStub<FokosDBMaterializationDO>[]>,
	check: () => Promise<boolean>,
): Promise<void> {
	for (let round = 0; round < 400; round++) {
		if (await check()) {
			return;
		}
		for (const stub of await stubs()) {
			await runInDurableObject(stub, async (instance: FokosDBMaterializationDO) => {
				if (instance.fokos.initialized()) {
					await instance.fokos.runDueWork();
				}
			});
		}
		await scheduler.wait(25);
	}
	throw new Error("the partitions made no progress");
}

/** The split state of a materialization partition. */
async function rows(stub: DurableObjectStub<FokosDBMaterializationDO>) {
	return await runInDurableObject(stub, (_instance: FokosDBMaterializationDO, state: DurableObjectState) => {
		const sql = state.storage.sql;
		return {
			items: sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM latest_items`).one().n,
			targets: sql.exec(`SELECT target_id, cursor_seq, epoch FROM materialization_targets`).toArray(),
			fences: sql.exec(`SELECT target_id, ancestor_id, min_epoch FROM ancestor_fences`).toArray(),
			producers: sql.exec(`SELECT producer_id, epoch FROM producer_fences`).toArray(),
			keyBytes: sql.exec(`SELECT hk FROM key_size_estimates`).toArray().length,
			snapshots: state.storage.kv.get("__fokosdb/mat/cursor_snapshots"),
		};
	});
}

const sink = () => env.PUSH_SINK_DO.getByName("sink");

describe("FokosDBMaterializationDO — splits", () => {
	it("a hash split moves the items, the cursor, and the producer fences, and the children send with the split fence", async () => {
		const root = materializationRoot({ hashSplitN: 2, hashSplitConditions: { maxSizeMb: 1 } });
		const rootId = root.stub.id.toString();
		// The target refuses each call of the root, so each item of the root is inherited work of a child.
		await sink().refuse(rootId);
		const data = "x".repeat(90 * 1024);
		let written = 0;
		for (; written < 30; written++) {
			const res = await apply(root, [
				{ hashKey: kb(`k${written}`), sortKey: kb("s"), revision: 1, deleted: false, data, kind: "text", ttlAt: null },
			]);
			if ((res.partitions[0].databaseSize ?? 0) > 1024 * 1024) {
				written++;
				break;
			}
		}

		const children = async () => {
			const { split } = await root.stub.fokosStatus({});
			return split.flatMap((entry) => (entry.target ? [stubOf(root, entry.target.ref)] : []));
		};
		await driveUntil(
			async () => [root.stub, ...(await children())],
			async () => {
				const { split } = await root.stub.fokosStatus({});
				return split.length > 0 && split.every((entry) => DONE_STATES.includes(entry.repartition.state));
			},
		);

		// The cutover raised the epoch of the target on the root.
		expect((await rows(root.stub)).targets).toEqual([{ target_id: "sink", cursor_seq: 0, epoch: 2 }]);
		const stubs = await children();
		expect(stubs).toHaveLength(2);
		let moved = 0;
		for (const child of stubs) {
			const childId = child.id.toString();
			const state = await rows(child);
			moved += state.items;
			// The child runs as soon as it imported, so its cursor can have moved.
			expect(state.targets).toMatchObject([{ target_id: "sink", epoch: 1 }]);
			expect(state.producers).toEqual([{ producer_id: PRODUCER_ID, epoch: 1 }]);

			// The child sends its items with the fence of the root, and then removes the fence.
			await driveUntil(
				async () => [child],
				async () => Object.keys(await sinkItems(childId)).length === state.items && (await rows(child)).fences.length === 0,
			);
			expect((await sink().fences(childId))[0]).toEqual([{ ancestorId: rootId, minEpoch: 2 }]);
		}
		expect(moved).toBe(written);
	});

	it("a promotion moves the items of the key from the cursor snapshot, and the source deletes them after", async () => {
		const root = materializationRoot({ hashSplitConditions: { maxSizeMb: 100 } });
		const rootId = root.stub.id.toString();
		await sink().refuse(rootId);
		const item = (hashKey: string, sortKey: string) =>
			({
				hashKey: kb(hashKey),
				sortKey: kb(sortKey),
				revision: 1,
				deleted: false,
				data: `${hashKey}/${sortKey}`,
				kind: "text",
				ttlAt: null,
			}) as const;
		await apply(root, [item("hot", "s0"), item("hot", "s1"), item("hot", "s2"), item("cold", "s0")]);
		await root.stub.fokosRequestPromotion({ target: root.ref, hashKey: kb("hot") });

		const promotion = async () => (await root.stub.fokosPromotions({ cursor: null })).entries[0];
		const child = async () => {
			const ref = (await promotion())?.target?.ref;
			return ref ? [stubOf(root, ref)] : [];
		};
		await driveUntil(
			async () => [root.stub, ...(await child())],
			async () => (await promotion())?.repartition.state === "cleaned",
		);

		// The source keeps its other key, and no row or snapshot of the promoted key.
		expect(await rows(root.stub)).toMatchObject({
			items: 1,
			keyBytes: 1,
			snapshots: undefined,
			targets: [{ target_id: "sink", epoch: 2 }],
		});
		const [range] = await child();
		const rangeId = range.id.toString();
		expect(await rows(range)).toMatchObject({ items: 3, targets: [{ target_id: "sink", epoch: 1 }] });
		await driveUntil(
			async () => [range],
			async () => (await sink().received(rangeId)).length === 3 && (await rows(range)).fences.length === 0,
		);
		expect((await sink().fences(rangeId))[0]).toEqual([{ ancestorId: rootId, minEpoch: 2 }]);
	});
});

/** The items that the push target received from one producer, by base key. */
async function sinkItems(producerId: string): Promise<Record<string, unknown>> {
	const received: ReceivedPush[] = await sink().received(producerId);
	return Object.fromEntries(received.map((row) => [row.hashKey, row]));
}
