import type { Workload } from "./index.js";

const ITEMS_PER_BATCH = 100;

/**
 * `batchWriteItems` of 100 puts, then `batchGetItems` of the same 100 keys: read-your-writes across
 * the partitions the keys spread over.
 * `iter` doubles: total_ms, write_ms, read_ms, write_ok_n, write_unprocessed_n, read_found_n,
 * read_not_found_n, read_unprocessed_n.
 */
export const batch: Workload = {
	defaults: { iterations: 15, concurrency: 3, budgetMs: 35_000 },
	async tick(s) {
		const iterationsDone = await s.runIterations(async (i) => {
			const start = Date.now();
			const keys = Array.from({ length: ITEMS_PER_BATCH }, (_, j) => `b:${s.names.instance}:${i}:${j}`);
			const write = await s.op(() =>
				s.db.batchWriteItems({
					items: keys.map((hashKey) => ({ operation: "put" as const, hashKey, data: { i, at: Date.now(), pad: "y".repeat(120) } })),
				}),
			);
			const read = await s.op(() => s.db.batchGetItems({ items: keys.map((hashKey) => ({ hashKey })) }));
			const count = (items: readonly { outcome: string }[], outcome: string): number => items.filter((r) => r.outcome === outcome).length;
			const writeRes = write.result?.results ?? [];
			const readRes = read.result?.items ?? [];
			s.iter(
				[
					Date.now() - start,
					write.ms,
					read.ms,
					count(writeRes, "ok"),
					count(writeRes, "unprocessed") + count(writeRes, "unknown"),
					count(readRes, "found"),
					count(readRes, "not_found"),
					count(readRes, "unprocessed"),
				],
				write.outcome === "ok" ? read.outcome : write.outcome,
			);
		});
		return { iterationsDone };
	},
};
