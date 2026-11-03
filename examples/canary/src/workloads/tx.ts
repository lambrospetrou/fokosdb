import type { TransactWriteItem } from "fokosdb/client";
import type { Workload } from "./index.js";

const ACCOUNT_COUNT = 24;

const accountKey = (i: number): string => `acct:${i}`;

const pickAccounts = (n: number): string[] => {
	const picked = new Set<number>();
	while (picked.size < n) {
		picked.add(Math.floor(Math.random() * ACCOUNT_COUNT));
	}
	return [...picked].map(accountKey);
};

const BALANCE_PLUS_ONE = [
	{
		action: "set",
		target: { ref: "data", path: "$.balance" },
		value: { fn: "+", args: [{ fn: "if_not_exists", args: [{ ref: "data", path: "$.balance" }, { val: 0 }] }, { val: 1 }] },
	},
] as const;

/**
 * Transactions in three parallel lanes:
 * - `write`: a multi-partition `transactWriteItems` — put, update, check over three accounts.
 * - `read`: a `transactGetItems` over four accounts.
 * - `ordered`: an `ordered_per_item` chain on one account.
 * `iter` doubles: ms, partitions, forwards. blob6 is the lane. `transaction_cancelled` is a normal outcome.
 */
export const tx: Workload = {
	defaults: { iterations: 10, concurrency: 1, budgetMs: 35_000 },
	async setup(s) {
		const items = Array.from({ length: ACCOUNT_COUNT }, (_, i) => ({
			operation: "put" as const,
			hashKey: accountKey(i),
			data: { balance: 1000 },
		}));
		await s.op(() => s.db.batchWriteItems({ items }));
		return { seededItems: ACCOUNT_COUNT };
	},
	async tick(s) {
		const writeLane = s.runIterations(async (i) => {
			const [a, b, c] = pickAccounts(3);
			const items: TransactWriteItem[] = [
				{ operation: "put", hashKey: a, data: { i, at: Date.now() } },
				{ operation: "update", hashKey: b, update: BALANCE_PLUS_ONE },
				{ operation: "check", hashKey: c, condition: { op: "exists", args: [{ ref: "data" }] } },
			];
			const call = await s.op(() => s.db.transactWriteItems({ items }));
			s.iter([call.ms, call.stats.partitions, call.stats.forwards], call.outcome, ["write"]);
		});
		const readLane = s.runIterations(async () => {
			const keys = pickAccounts(4).map((hashKey) => ({ hashKey }));
			const call = await s.op(() => s.db.transactGetItems({ items: keys }));
			s.iter([call.ms, call.stats.partitions, call.stats.forwards], call.outcome, ["read"]);
		});
		const orderedLane = s.runIterations(async () => {
			const [a] = pickAccounts(1);
			const items: TransactWriteItem[] = [
				{ operation: "check", hashKey: a, condition: { op: "exists", args: [{ ref: "data" }] } },
				{ operation: "update", hashKey: a, update: BALANCE_PLUS_ONE },
				{ operation: "update", hashKey: a, update: BALANCE_PLUS_ONE },
			];
			const call = await s.op(() => s.db.transactWriteItems({ items, executionMode: "ordered_per_item" }));
			s.iter([call.ms, call.stats.partitions, call.stats.forwards], call.outcome, ["ordered"]);
		});
		const done = await Promise.all([writeLane, readLane, orderedLane]);
		return { iterationsDone: Math.min(...done) };
	},
};
