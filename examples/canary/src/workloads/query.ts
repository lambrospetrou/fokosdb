import type { QueryItemsOptions } from "fokosdb/client";
import type { RunScope, Workload } from "./index.js";

const HASH_KEYS = 8;
const SORT_KEYS = 250;

const hashKey = (i: number): string => `qhk:${i}`;
const sortKey = (i: number): string => `sk:${String(i).padStart(4, "0")}`;

/** Reads up to `maxPages` pages of `opts`, metering each page. Returns the totals. */
async function paged(s: RunScope, opts: QueryItemsOptions, maxPages: number) {
	let pages = 0;
	let items = 0;
	let scanned = 0;
	let ms = 0;
	let forwards = 0;
	let partitions = 0;
	let outcome = "ok";
	let cursor: string | undefined;
	do {
		const page = await s.op(() => s.db.queryItems({ ...opts, cursor }));
		ms += page.ms;
		forwards += page.stats.forwards;
		partitions += page.stats.partitions;
		if (page.outcome !== "ok") {
			outcome = page.outcome;
			break;
		}
		pages += 1;
		items += page.result!.items.length;
		scanned += page.result!.scannedCount;
		cursor = page.result!.cursor;
	} while (cursor !== undefined && pages < maxPages);
	return { pages, items, scanned, ms, forwards, partitions, outcome };
}

/**
 * Paginated `queryItems` over a seeded dataset: 8 hash keys x 250 sort keys, so a request that names
 * several hash keys crosses partitions. Iterations cycle through five patterns in blob6.
 * `iter` doubles: pages, items, ms_total, forwards, partition_visits.
 */
export const query: Workload = {
	defaults: { iterations: 30, concurrency: 3, budgetMs: 35_000 },
	async setup(s) {
		let seededItems = 0;
		for (let h = 0; h < HASH_KEYS; h++) {
			for (let start = 0; start < SORT_KEYS; start += 100) {
				const items = Array.from({ length: Math.min(100, SORT_KEYS - start) }, (_, j) => {
					const n = start + j;
					return {
						operation: "put" as const,
						hashKey: hashKey(h),
						sortKey: sortKey(n),
						data: { n, parity: n % 2 === 0 ? "even" : "odd", pad: "x".repeat(120) },
					};
				});
				await s.op(() => s.db.batchWriteItems({ items }));
				seededItems += items.length;
			}
		}
		return { seededItems };
	},
	async tick(s) {
		const iterationsDone = await s.runIterations(async (i) => {
			const hk = hashKey(i % HASH_KEYS);
			let totals;
			let pattern: string;
			switch (i % 5) {
				case 0:
					pattern = "between";
					totals = await paged(
						s,
						{ queries: [{ hashKey: hk, sortKeyCondition: { op: "between", lower: sortKey(50), upper: sortKey(199) } }], limit: 50 },
						4,
					);
					break;
				case 1:
					pattern = "begins_with";
					totals = await paged(s, { queries: [{ hashKey: hk, sortKeyCondition: { op: "begins_with", prefix: "sk:01" } }], limit: 50 }, 3);
					break;
				case 2:
					pattern = "multi_hk";
					totals = await paged(
						s,
						{
							queries: [0, 1, 2].map((k) => ({
								hashKey: hashKey((i + k) % HASH_KEYS),
								sortKeyCondition: { op: "begins_with", prefix: "sk:00" } as const,
							})),
							limit: 60,
						},
						3,
					);
					break;
				case 3:
					pattern = "count";
					totals = await paged(s, { queries: [{ hashKey: hk }], select: "count" }, 1);
					break;
				default:
					pattern = "filtered";
					totals = await paged(
						s,
						{
							queries: [{ hashKey: hk, sortKeyCondition: { op: "lte", value: sortKey(120) } }],
							limit: 40,
							filter: { op: "eq", args: [{ ref: "data", path: "$.parity" }, { val: "even" }] },
						},
						3,
					);
			}
			s.iter([totals.pages, totals.items, totals.ms, totals.forwards, totals.partitions], totals.outcome, [pattern]);
		});
		return { iterationsDone };
	},
};
