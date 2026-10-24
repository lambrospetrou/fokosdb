/**
 * Measures the build of the hash body of the topology keeper inside workerd, from 100 to 10,000,000
 * partitions. Run it with `pnpm --filter fokosdb bench:workerd test/topology-bench`.
 *
 * The bench fills the tables of the keeper with SQL: a balanced tree with R = 16 and K = 2, and one
 * promoted key for each 100 partitions. Then it measures:
 * - the first build of the hash body,
 * - a build after 1 new hash split and 1 new promotion,
 * - a build after 100 new hash splits and 1 new promotion.
 *
 * Timers in workerd advance in whole milliseconds.
 */
import { runInDurableObject } from "cloudflare:test";
import { env } from "cloudflare:workers";
import { expect, it } from "vitest";
import { FokosTopologyKeeper, KeyCodec } from "../../src/sharding/index-server.js";
import { parseHashBody } from "../../src/sharding/topology-codec.js";
import { hashSplitReport, promotionReport } from "../sharding/topology-events.js";

const R = 16;
const K = 2;
const SIZES = [100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000];
const LATER = Number.MAX_SAFE_INTEGER;
const INSERT_BATCH = 50_000;

/** The root index and the child path of the node at index `i` of the level order of a complete tree. */
function nodeAt(i: number): { root: number; path: number[] } {
	let depth = 0;
	let before = 0;
	while (i >= before + R * K ** depth) {
		before += R * K ** depth;
		depth += 1;
	}
	const offset = i - before;
	const width = K ** depth;
	const root = Math.floor(offset / width);
	let p = offset % width;
	const path = new Array<number>(depth);
	for (let d = depth - 1; d >= 0; d--) {
		path[d] = p % K;
		p = Math.floor(p / K);
	}
	return { root, path };
}

function nodeKey(root: number, path: number[]): Uint8Array {
	return new Uint8Array([path.length, root >> 8, root & 0xff, ...path]);
}

function timed<T>(fn: () => T): { ms: number; value: T } {
	const t0 = performance.now();
	const value = fn();
	return { ms: performance.now() - t0, value };
}

it("hash body build", async () => {
	const rows: string[] = [];
	for (const n of SIZES) {
		const stub = env.TOPOLOGY_ADMIN_DO.getByName(`bench-${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance, state) => {
			const storage = state.storage;
			const sql = storage.sql;
			const keeper = new FokosTopologyKeeper(storage, { buildDelayMs: 0, buildStepRows: Number.MAX_SAFE_INTEGER, scheduleBuild: () => {} });
			keeper.runMigrations();
			const sg = "sg";
			const splits = Math.floor((n - R) / K);
			const promoted = Math.max(1, Math.floor(n / 100));
			const fill = timed(() => {
				for (let start = 0; start < splits; start += INSERT_BATCH) {
					storage.transactionSync(() => {
						for (let i = start; i < Math.min(splits, start + INSERT_BATCH); i++) {
							const { root, path } = nodeAt(i);
							sql.exec(`INSERT INTO fokos_topo_hash_nodes (shard_group, node, seq) VALUES (?1, ?2, 1)`, sg, nodeKey(root, path));
						}
					});
				}
				storage.transactionSync(() => {
					for (let i = 0; i < promoted; i++) {
						sql.exec(
							`INSERT INTO fokos_topo_range_intervals (shard_group, hash_key, start, end_is_max, end, seq) VALUES (?1, ?2, x'', 1, x'', 1)`,
							sg,
							KeyCodec.encode(`user-${i}`),
						);
					}
					sql.exec(
						`INSERT INTO fokos_topo_groups (shard_group, root_trees_n, hash_split_n, version, hash_version, build_due_at, promoted_n, hash_nodes_n)
						 VALUES (?1, ?2, ?3, 1, 1, 0, ?4, ?5)`,
						sg,
						R,
						K,
						promoted,
						splits,
					);
				});
			});
			const first = timed(() => keeper.runDueBuilds(LATER));
			const read = keeper.getTopology(sg);
			expect(read.status).toBe(200);
			const bodyBytes = (read as { body: Uint8Array }).body.length;
			expect(parseHashBody((read as { body: Uint8Array }).body).bitCount).toBe(R + K * splits);

			// The first leaves in level order, spread over the tree.
			const leafStep = Math.max(1, Math.floor((n - splits) / 101));
			const step = async (newSplits: number, offset: number) => {
				for (let j = 0; j < newSplits; j++) {
					const { root, path } = nodeAt(splits + offset + j * leafStep);
					await keeper.registerRepartition(hashSplitReport(sg, { rootTreesN: R, hashSplitN: K }, root, path, 1), 1);
				}
				await keeper.registerRepartition(promotionReport(sg, KeyCodec.encode(`new-${offset}-${newSplits}`), 1, R, K), 1);
				return timed(() => keeper.runDueBuilds(LATER)).ms;
			};
			const one = await step(1, 0);
			const hundred = await step(100, 1);
			rows.push(
				`| ${n.toLocaleString("en")} | ${splits.toLocaleString("en")} | ${promoted.toLocaleString("en")} | ${(bodyBytes / 1024).toFixed(1)} KiB | ${fill.ms.toFixed(0)} | ${first.ms.toFixed(1)} | ${one.toFixed(1)} | ${hundred.toFixed(1)} |`,
			);
			// The storage of a large object is not needed after the measure.
			await storage.deleteAll();
		});
	}
	console.log(
		[
			"| partitions | hash nodes | promoted keys | hash body | fill ms | first build ms | +1 split build ms | +100 splits build ms |",
			"|---:|---:|---:|---:|---:|---:|---:|---:|",
			...rows,
		].join("\n"),
	);
});
