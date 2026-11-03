import { allOk, maxDbSize, sumForwards } from "../metrics.js";
import type { Workload } from "./index.js";

const PAD = "x".repeat(200);

/**
 * The single-item cycle: `putItem` → `getItem` → `deleteItem` → `getItem` (not_found).
 * `iter` doubles: put_ms, get_ms, delete_ms, get_miss_ms, database_size, forwards.
 */
export const crud: Workload = {
	defaults: { iterations: 100, concurrency: 5, budgetMs: 35_000 },
	async tick(s) {
		const iterationsDone = await s.runIterations(async (i) => {
			const hk = `crud:${s.names.instance}:${i}`;
			const put = await s.op(() => s.db.putItem({ hashKey: hk, data: { i, at: Date.now(), pad: PAD } }));
			const get = await s.op(() => s.db.getItem({ hashKey: hk }));
			const del = await s.op(() => s.db.deleteItem({ hashKey: hk }));
			const miss = await s.op(() => s.db.getItem({ hashKey: hk }));
			const calls = [put, get, del, miss];
			s.iter([put.ms, get.ms, del.ms, miss.ms, maxDbSize(calls), sumForwards(calls)], allOk(calls));
		});
		return { iterationsDone };
	},
};
