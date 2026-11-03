import type { Workload } from "./index.js";

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const TTL_SECONDS = 5;
const POLL_MS = 1_000;

/**
 * Writes one item that expires in 5 seconds, then polls `getItem` every second until the row is
 * gone: an expired row still answers `found` until the TTL sweep deletes it, and the polls keep the
 * partition's sweep armed. The point reports the observed lag from `ttlAt` to deletion.
 * `iter` doubles: total_ms, lag_ms (-1 when the budget ends first), polls_n.
 */
export const ttl: Workload = {
	defaults: { iterations: 1, concurrency: 1, budgetMs: 40_000 },
	async tick(s) {
		const start = Date.now();
		const deadline = start + s.params.budgetMs;
		const hk = `ttl:${s.names.instance}:${Date.now()}`;
		const ttlAtSec = Math.floor(Date.now() / 1000) + TTL_SECONDS;
		const put = await s.op(() => s.db.putItem({ hashKey: hk, data: { expires: ttlAtSec }, ttlAt: ttlAtSec }));
		await sleep(TTL_SECONDS * 1_000);
		let polls = 0;
		let lagMs = -1;
		let outcome = put.outcome;
		while (Date.now() < deadline) {
			const get = await s.op(() => s.db.getItem({ hashKey: hk }));
			polls += 1;
			if (get.outcome !== "ok") {
				outcome = get.outcome;
				break;
			}
			if (!get.result!.found) {
				lagMs = Date.now() - ttlAtSec * 1_000;
				outcome = "ok";
				break;
			}
			await sleep(POLL_MS);
		}
		s.iter([Date.now() - start, lagMs, polls], outcome === "ok" && lagMs < 0 ? "ttl_pending" : outcome);
		return { iterationsDone: 1 };
	},
};
