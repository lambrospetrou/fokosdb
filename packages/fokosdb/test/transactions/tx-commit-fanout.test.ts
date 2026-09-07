import { env } from "cloudflare:workers";
import { runDurableObjectAlarm } from "cloudflare:test";
import { afterEach, beforeEach, describe, it, expect, vi } from "vitest";
import { PartitionDO } from "../../src/server/do-partition.js";
import {
	TX_FANOUT_REQUEST_BUDGET_MS,
	TransactionCoordinatorDO,
	isTransactionCommitPendingError,
	isTransactionUndecidedError,
} from "../../src/server/do-transaction-coordinator.js";
import { keysAcrossPartitions, makeDB, partitionNameOf } from "./tx-helpers.js";

/**
 * The commit fan-out carries keys only, and the `committed` answer waits for every participant: a
 * client that receives it can read what it wrote everywhere. A commit that cannot finish inside the
 * request budget answers the retryable commit-pending error — never "committed" — and the alarm
 * finishes the work.
 */
describe("transactions - commit fan-out: keys only, and the gated committed answer", () => {
	beforeEach(() => {
		// Real clocks: these tests measure wall-clock budgets, and the retry backoffs must sleep.
		vi.useRealTimers();
	});

	afterEach(() => {
		vi.restoreAllMocks();
	});

	// The shipped budget and stale threshold are both 5s, and a test that must watch one expire
	// would spend that long doing nothing. These shorten the wait to the phase under test. Restore
	// the spy before any step that has to FINISH inside the budget, so that step runs on the shipped
	// value — see the test below that runs the whole path on it.
	const SHORT_BUDGET_MS = 250;

	function shortenFanoutBudget() {
		return vi.spyOn(TransactionCoordinatorDO.prototype, "fokosFanoutRequestBudgetMs").mockReturnValue(SHORT_BUDGET_MS);
	}

	function shortenStaleTransactions() {
		return vi.spyOn(PartitionDO.prototype, "fokosStaleTransactionMs").mockReturnValue(SHORT_BUDGET_MS);
	}

	it("commits a multi-megabyte transaction with commit RPCs that carry keys only", { timeout: 60_000 }, async () => {
		const db = makeDB();
		// 10 items x 350 KB ≈ 3.4 MB: well over a megabyte on the wire if the payload were re-sent.
		const keys = keysAcrossPartitions(db, 10, "keys-only");
		const data = "x".repeat(350 * 1024);
		const items = keys.map((key) => ({ ...key, operation: "put" as const, data }));

		const commitSpy = vi.spyOn(PartitionDO.prototype, "txCommit");
		const result = await db.transactWriteItems({ items });

		expect(result.outcome).toBe("committed");
		// One commit RPC per participant, and every wire item is a bare key: the payload each
		// participant applies comes from its own pending_transactions rows, not from the wire.
		expect(commitSpy.mock.calls.length).toBeGreaterThanOrEqual(2);
		for (const [, request] of commitSpy.mock.calls) {
			for (const item of request.items) {
				expect(Object.keys(item).sort()).toEqual(["hashKey", "sortKey"]);
			}
		}
		for (const key of keys) {
			await expect(db.getItem(key)).resolves.toMatchObject({ found: true, item: { data } });
		}
	});

	it("answers the retryable commit-pending error when one participant is unreachable, and a token replay commits after it returns", async () => {
		const db = makeDB();
		const keys = keysAcrossPartitions(db, 2, "commit-pending");
		const unreachable = partitionNameOf(db, keys[1]);
		const items = keys.map((key) => ({ ...key, operation: "put" as const, data: `data-${key.hashKey}` }));
		const token = `commit-pending-${crypto.randomUUID()}`;
		const budget = shortenFanoutBudget();

		const orig = PartitionDO.prototype.txCommit;
		const spy = vi.spyOn(PartitionDO.prototype, "txCommit").mockImplementation(function (this: PartitionDO, pCtx, request) {
			if (pCtx.doName === unreachable) throw new Error("simulated participant outage");
			return orig.call(this, pCtx, request);
		});

		const start = Date.now();
		const err = await db.transactWriteItems({ items, clientRequestToken: token }).then(
			() => null,
			(e: unknown) => e,
		);
		// PREPARED is final, so this transaction commits — but the unreachable participant has not
		// applied it yet, and the caller must not be told "committed".
		expect(isTransactionCommitPendingError(err)).toBe(true);
		expect(isTransactionUndecidedError(err)).toBe(false);
		// Within the request budget plus at most one in-flight attempt — the deadline is checked
		// after each backoff, so a sleep already running when it passes still finishes. Never the
		// alarm's full retry budget (10 attempts, 13 s of backoff ceiling).
		expect(Date.now() - start).toBeLessThan(SHORT_BUDGET_MS + 3_000);

		// The replay has to finish its fan-out inside the budget, so give it the shipped one.
		budget.mockRestore();
		spy.mockRestore();
		const replay = await db.transactWriteItems({ items, clientRequestToken: token });
		expect(replay.outcome).toBe("committed");
		// Read-your-writes: every written key, on every participant, returns the new value.
		for (const key of keys) {
			await expect(db.getItem(key)).resolves.toMatchObject({ found: true, item: { data: `data-${key.hashKey}` } });
		}
	});

	it("answers commit-pending while a split participant's children migrate, and a token replay commits once they finish", async () => {
		const db = makeDB();
		const keys = keysAcrossPartitions(db, 2, "split-window");
		const splitting = partitionNameOf(db, keys[0]);
		const items = keys.map((key) => ({ ...key, operation: "put" as const, data: `data-${key.hashKey}` }));
		const token = `split-window-${crypto.randomUUID()}`;
		const budget = shortenFanoutBudget();

		// A partition that started a split forwards the commit to its children, and a child rejects
		// commit while it still migrates. Hold that window open deterministically instead of racing
		// a real migration against the request budget.
		let childrenMigrating = true;
		const orig = PartitionDO.prototype.txCommit;
		const spy = vi.spyOn(PartitionDO.prototype, "txCommit").mockImplementation(function (this: PartitionDO, pCtx, request) {
			if (pCtx.doName === splitting && childrenMigrating) throw new Error("simulated split window: children migrating");
			return orig.call(this, pCtx, request);
		});

		const err = await db.transactWriteItems({ items, clientRequestToken: token }).then(
			() => null,
			(e: unknown) => e,
		);
		expect(isTransactionCommitPendingError(err)).toBe(true);

		childrenMigrating = false;
		budget.mockRestore();
		spy.mockRestore();
		const replay = await db.transactWriteItems({ items, clientRequestToken: token });
		expect(replay.outcome).toBe("committed");
		for (const key of keys) {
			await expect(db.getItem(key)).resolves.toMatchObject({ found: true, item: { data: `data-${key.hashKey}` } });
		}
	});

	it("answers cancelled while an unreachable participant still holds its locks, and the stale-transaction alarm releases them", async () => {
		const db = makeDB();
		const [checkKey, lockedKey] = keysAcrossPartitions(db, 2, "cancel-locked");
		const unreachable = partitionNameOf(db, lockedKey);
		// A failing check on the reachable participant cancels the transaction during prepare, after
		// the unreachable participant already locked its key.
		await db.putItem({ ...checkKey, data: { x: 1 } });
		const items = [
			{ ...lockedKey, operation: "put" as const, data: "locked-data" },
			{
				...checkKey,
				operation: "check" as const,
				condition: { op: "eq", args: [{ ref: "data", path: "$.x" }, { val: 5 }] } as const,
			},
		];
		const token = `cancel-locked-${crypto.randomUUID()}`;
		shortenFanoutBudget();
		shortenStaleTransactions();

		const orig = PartitionDO.prototype.txCancel;
		const spy = vi.spyOn(PartitionDO.prototype, "txCancel").mockImplementation(function (this: PartitionDO, pCtx, request) {
			if (pCtx.doName === unreachable) throw new Error("simulated participant outage");
			return orig.call(this, pCtx, request);
		});

		const start = Date.now();
		const result = await db.transactWriteItems({ items, clientRequestToken: token });
		// The answer follows the decision: a cancelled transaction applied nothing anywhere, so it
		// is final even though the unreachable participant still holds its lock.
		expect(result).toMatchObject({ outcome: "cancelled", reason: { type: "condition_failed" } });
		// The cancel fan-out is bounded exactly as the commit fan-out is: the unreachable
		// participant costs the caller the budget, not the whole retry ladder.
		expect(Date.now() - start).toBeLessThan(SHORT_BUDGET_MS + 3_000);
		// The lock is still held — the cancel could not reach that participant — and a held lock
		// makes every non-transactional write to the key throw. Reads still answer committed state,
		// which proves the cancelled put applied nothing.
		await expect(db.putItem({ ...lockedKey, data: "blocked" })).rejects.toThrow(/item is locked by an in-progress transaction/);
		await expect(db.getItem(lockedKey)).resolves.toMatchObject({ found: false });

		spy.mockRestore();
		// The stale-transaction alarm releases the lock once it ages past the stale threshold,
		// whose clock does not depend on how long the cancel retries happened to take. Force the
		// partition's alarm on every attempt so the release does not depend on the scheduler,
		// and retry the write that the lock was blocking until it goes through.
		const unreachableStub = PartitionDO.getByName(env.PARTITION_DO, unreachable);
		await vi.waitFor(
			async () => {
				await runDurableObjectAlarm(unreachableStub);
				await db.putItem({ ...lockedKey, data: "after-release" });
			},
			{ timeout: 10_000, interval: 100 },
		);
		await expect(db.getItem(lockedKey)).resolves.toMatchObject({ found: true, item: { data: "after-release" } });
	});

	/**
	 * Every other test here shortens the budget to keep the suite quick, which means none of them
	 * would notice if the shipped value stopped reaching the request path — a hook that returned 0,
	 * or a call site that lost its budget argument, would still pass them. This one runs the whole
	 * path on the shipped constant and pays the wall-clock cost to prove it.
	 */
	it("waits out the shipped fan-out budget, and stops there", { timeout: 60_000 }, async () => {
		const db = makeDB();
		const keys = keysAcrossPartitions(db, 2, "shipped-budget");
		const unreachable = partitionNameOf(db, keys[1]);
		const items = keys.map((key) => ({ ...key, operation: "put" as const, data: "value" }));

		const orig = PartitionDO.prototype.txCommit;
		vi.spyOn(PartitionDO.prototype, "txCommit").mockImplementation(function (this: PartitionDO, pCtx, request) {
			if (pCtx.doName === unreachable) throw new Error("simulated participant outage");
			return orig.call(this, pCtx, request);
		});

		const start = Date.now();
		const err = await db.transactWriteItems({ items }).then(
			() => null,
			(e: unknown) => e,
		);
		const elapsed = Date.now() - start;

		expect(isTransactionCommitPendingError(err)).toBe(true);
		// The caller waited the budget out — the deadline is only reached by the clock running.
		expect(elapsed).toBeGreaterThanOrEqual(TX_FANOUT_REQUEST_BUDGET_MS);
		// And stopped there, plus at most the one backoff already in flight.
		expect(elapsed).toBeLessThan(TX_FANOUT_REQUEST_BUDGET_MS + 3_000);
	});
});
