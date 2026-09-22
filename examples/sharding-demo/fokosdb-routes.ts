import { Hono } from "hono";
import { FokosDB, FokosRouter, PartitionContextCreator } from "fokosdb/client";
import { KeyCodec, type FokosStatusCursor, type FokosStatusEntry, type FokosStatusPage } from "fokosdb/sharding";
import { jsonBody, summaryOf, type ActionTrace, type TopologyItem } from "./shared.js";

/** Demo 3 routes: item writes and reads on a real FokosDB table, and the topology of its partitions. */

const table = PartitionContextCreator.create({
	ns: "PARTITION_DO",
	nsTx: "TRANSACTION_COORDINATOR_DO",
	tableName: "demo3_table",
	rootTreesN: 2,
	hashSplitN: 2,
	rangeSplitN: 2,
	hashSplitConditions: { maxSizeMb: 100 },
	rangeSplitConditions: { maxSizeMb: 100 },
});
const router = new FokosRouter(table.topology, table.rangeConfig, table.policy);

function db(): FokosDB {
	return new FokosDB({ topology: router, numTxCoordinators: 1 });
}

/** The trace of one item request, from the partition information that FokosDB returns. */
function traceOfMeta(meta: { servedByActorName: string; forwardCount: number }): ActionTrace {
	return { servedBy: [{ doName: meta.servedByActorName, role: "executed" }], forwardCount: meta.forwardCount };
}

/** Reads every status page of one root partition. */
async function statusEntries(env: Env, ctx: ReturnType<typeof router.allRoots>[number]): Promise<FokosStatusEntry[]> {
	const stub = env.PARTITION_DO.get(env.PARTITION_DO.idFromName(ctx.doName));
	const entries: FokosStatusEntry[] = [];
	let cursor: FokosStatusCursor | null = null;
	do {
		const page: FokosStatusPage = await stub.fokosStatus({ cursor, rootContext: ctx });
		entries.push(...page.entries);
		cursor = page.nextCursor;
	} while (cursor);
	return entries;
}

export const fokosdbRoutes = new Hono<{ Bindings: Env }>();

/** The root partitions and their direct targets. A deeper level needs a status read on each target. */
fokosdbRoutes.get("/topology", async (c) => {
	const roots: TopologyItem[] = [];
	for (const ctx of router.allRoots()) {
		const entries = await statusEntries(c.env, ctx);
		const children: TopologyItem[] = entries.flatMap((e) =>
			e.target
				? [
						{
							id: e.target.ref.partitionId,
							doName: e.target.ref.doName,
							role: "leaf" as const,
							kind: e.repartition.kind === "key_promotion" ? ("range" as const) : ("hash" as const),
							status: e.repartition.state,
							importState: null,
							label: e.repartition.hashKey ? (KeyCodec.decode(e.repartition.hashKey) as string) : null,
							itemCount: 0,
							children: [],
						},
					]
				: [],
		);
		const isRouter = entries.some((e) => e.repartition.kind === "hash_split");
		roots.push({
			id: ctx.partitionId,
			doName: ctx.doName,
			role: isRouter ? "router" : "leaf",
			kind: "hash",
			status: "active",
			importState: null,
			label: null,
			itemCount: 0,
			children,
		});
	}
	return c.json({ tile: "demo3", roots, summary: summaryOf(roots) });
});

fokosdbRoutes.post("/put-item", async (c) => {
	const {
		hashKey = "user#1",
		sortKey = "profile",
		data = "live demo",
	} = await jsonBody<{ hashKey: string; sortKey: string; data: string }>(c);
	const res = await db().putItem({ hashKey, sortKey, data });
	return c.json({ success: true, result: { hashKey, sortKey }, trace: traceOfMeta(res.meta) });
});

fokosdbRoutes.post("/get-item", async (c) => {
	const { hashKey = "user#1", sortKey = "profile" } = await jsonBody<{ hashKey: string; sortKey: string }>(c);
	const res = await db().getItem({ hashKey, sortKey });
	return c.json({ success: true, result: { found: res.found, item: res.item }, trace: traceOfMeta(res.meta) });
});

fokosdbRoutes.post("/seed", async (c) => {
	const { count = 8 } = await jsonBody<{ count: number }>(c);
	const fokos = db();
	for (let i = 0; i < count; i++) await fokos.putItem({ hashKey: `seed#${i}`, sortKey: "entry", data: `Seed record ${i}` });
	return c.json({ success: true, result: { seeded: count } });
});

fokosdbRoutes.post("/reset", async (c) => {
	await db().destroy();
	return c.json({ success: true });
});
