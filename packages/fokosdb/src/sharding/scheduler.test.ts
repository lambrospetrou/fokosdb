import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { FokosDBPartitionDO } from "../server/do-partition.js";
import { testPartitionStub } from "../../test/stub-helpers.js";
import { FokosScheduler, type FokosSchedulerDeps } from "./scheduler.js";
import { FokosShardingStore } from "./sharding-store.js";
import type { FokosJob, FokosLifecycle } from "./runtime-types.js";

const FALLBACK_MS = 60_000;

/** A new lifecycle result of a root owner, one object for each call. */
function ownerLifecycle(): FokosLifecycle {
	return { role: "owner", import: null, activeRepartition: null, destroying: false };
}

/**
 * Runs `fn` with a scheduler over REAL Durable Object storage and one host job. The job counts the
 * reads of its deadline, because a deadline can cost a storage query.
 */
async function withScheduler(
	job: Omit<FokosJob, "deadline" | "canRun"> & { deadline: () => number | null },
	fn: (scheduler: FokosScheduler, state: DurableObjectState, deadlineReads: () => number) => Promise<void>,
	deps: Partial<FokosSchedulerDeps> = {},
): Promise<void> {
	const stub = testPartitionStub(`scheduler-test.${crypto.randomUUID()}`);
	await runInDurableObject(stub, async (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
		const store = new FokosShardingStore(state.storage);
		store.runMigrations();
		let reads = 0;
		const counted: FokosJob = {
			...job,
			canRun: () => true,
			deadline: () => {
				reads++;
				return job.deadline();
			},
		};
		const scheduler = new FokosScheduler({
			storage: state.storage,
			store,
			fallbackAlarmMs: () => FALLBACK_MS,
			fastPathDelayMs: () => 0,
			isFenced: () => false,
			lifecycle: ownerLifecycle,
			jobs: () => [counted],
			logParams: () => ({}),
			...deps,
		});
		try {
			await fn(scheduler, state, () => reads);
		} finally {
			await state.storage.deleteAlarm();
		}
	});
}

describe("FokosScheduler - deadline reads", () => {
	it("reads each deadline one time before the steps and one time after them", async () => {
		let pending = true;
		await withScheduler(
			{
				name: "work",
				deadline: () => (pending ? Date.now() - 1 : null),
				runStep: () => {
					pending = false;
					return { nextRunAt: null };
				},
			},
			async (scheduler, state, deadlineReads) => {
				await scheduler.runDueWork();

				expect(pending).toBe(false);
				expect(deadlineReads()).toBe(2);
				// The read after the step sees the new deadline, so no work is left and no alarm stays.
				expect(await state.storage.getAlarm()).toBeNull();
			},
		);
	});

	it("reads each deadline one time when no job is due, and arms the alarm at the earliest", async () => {
		const at = Date.now() + 30_000;
		await withScheduler(
			{
				name: "later",
				deadline: () => at,
				runStep: () => {
					throw new Error("the job is not due");
				},
			},
			async (scheduler, state, deadlineReads) => {
				await scheduler.runDueWork();

				expect(deadlineReads()).toBe(1);
				expect(await state.storage.getAlarm()).toBe(at);
			},
		);
	});
});

describe("FokosScheduler - failure backoff", () => {
	it("runs a failed step again at the fallback, not at its stale durable deadline", async () => {
		let runs = 0;
		await withScheduler(
			{
				name: "work",
				// The step throws before it moves its durable deadline, so the deadline stays due.
				deadline: () => 0,
				runStep: () => {
					runs++;
					throw new Error("the step failed");
				},
			},
			async (scheduler, state, deadlineReads) => {
				await scheduler.runDueWork();

				const jobs = new FokosShardingStore(state.storage).getJobs();
				expect(jobs["work"]?.failed).toBe(true);
				expect(jobs["work"]?.nextRunAt).toBeGreaterThan(Date.now() + FALLBACK_MS - 5_000);
				expect(await state.storage.getAlarm()).toBe(jobs["work"]?.nextRunAt);
				// The deadline read after the step skipped the durable deadline the flag suppresses.
				expect(deadlineReads()).toBe(1);

				// A pass before the scheduled run does not rerun the step or read its deadline.
				await scheduler.runDueWork();
				expect(runs).toBe(1);
				expect(deadlineReads()).toBe(1);
			},
		);
	});

	it("quarantines a job whose deadline read throws, and still runs the others", async () => {
		const stub = testPartitionStub(`scheduler-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
			const store = new FokosShardingStore(state.storage);
			store.runMigrations();
			let badReads = 0;
			let badRuns = 0;
			let goodRuns = 0;
			let goodPending = true;
			const jobs: FokosJob[] = [
				{
					name: "bad",
					canRun: () => true,
					deadline: () => {
						badReads++;
						throw new Error("the deadline read failed");
					},
					runStep: () => {
						badRuns++;
						return { nextRunAt: null };
					},
				},
				{
					name: "good",
					canRun: () => true,
					deadline: () => (goodPending ? 0 : null),
					runStep: () => {
						goodRuns++;
						goodPending = false;
						return { nextRunAt: null };
					},
				},
			];
			const scheduler = new FokosScheduler({
				storage: state.storage,
				store,
				fallbackAlarmMs: () => FALLBACK_MS,
				fastPathDelayMs: () => 0,
				isFenced: () => false,
				lifecycle: ownerLifecycle,
				jobs: () => jobs,
				logParams: () => ({}),
			});
			try {
				await scheduler.runDueWork();

				const record = store.getJobs();
				expect(record["bad"]?.failed).toBe(true);
				expect(record["bad"]?.nextRunAt).toBeGreaterThan(Date.now() + FALLBACK_MS - 5_000);
				// The quarantined job never ran, the other job did, and the flag stopped the second
				// deadline read of the pass.
				expect(badRuns).toBe(0);
				expect(goodRuns).toBe(1);
				expect(badReads).toBe(1);

				// A pass before the scheduled run does not read the throwing deadline again.
				await scheduler.runDueWork();
				expect(badReads).toBe(1);
				expect(badRuns).toBe(0);
			} finally {
				await state.storage.deleteAlarm();
			}
		});
	});

	it("excludes a job whose canRun check throws, and still runs the others", async () => {
		const stub = testPartitionStub(`scheduler-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
			const store = new FokosShardingStore(state.storage);
			store.runMigrations();
			let badRuns = 0;
			let goodRuns = 0;
			const jobs: FokosJob[] = [
				{
					name: "bad",
					canRun: () => {
						throw new Error("the canRun check failed");
					},
					deadline: () => 0,
					runStep: () => {
						badRuns++;
						return { nextRunAt: null };
					},
				},
				{
					name: "good",
					canRun: () => true,
					deadline: () => null,
					runStep: () => {
						goodRuns++;
						return { nextRunAt: null };
					},
				},
			];
			store.putJobs({ good: { nextRunAt: 0 } });
			const scheduler = new FokosScheduler({
				storage: state.storage,
				store,
				fallbackAlarmMs: () => FALLBACK_MS,
				fastPathDelayMs: () => 0,
				isFenced: () => false,
				lifecycle: ownerLifecycle,
				jobs: () => jobs,
				logParams: () => ({}),
			});
			try {
				await scheduler.runDueWork();

				expect(badRuns).toBe(0);
				expect(goodRuns).toBe(1);
			} finally {
				await state.storage.deleteAlarm();
			}
		});
	});

	it("resolves with the fallback alarm armed when the pass itself throws", async () => {
		await withScheduler(
			{
				name: "work",
				deadline: () => 0,
				runStep: () => ({ nextRunAt: null }),
			},
			async (scheduler, state) => {
				await scheduler.runDueWork();

				const alarm = await state.storage.getAlarm();
				expect(alarm).toBeGreaterThanOrEqual(Date.now() + FALLBACK_MS - 5_000);
				expect(alarm).toBeLessThanOrEqual(Date.now() + FALLBACK_MS + 5_000);
			},
			{
				jobs: () => {
					throw new Error("the jobs list failed");
				},
			},
		);
	});

	it("clears the failure entry after a step succeeds, and the durable deadline drives again", async () => {
		let pending = true;
		let failOnce = true;
		await withScheduler(
			{
				name: "work",
				deadline: () => (pending ? 0 : null),
				runStep: () => {
					if (failOnce) {
						failOnce = false;
						throw new Error("the first step failed");
					}
					pending = false;
					return { nextRunAt: null };
				},
			},
			async (scheduler, state) => {
				await scheduler.runDueWork();
				expect(new FokosShardingStore(state.storage).getJobs()["work"]?.failed).toBe(true);

				// An earlier scheduled run drops the flag, so the durable deadline counts again and the
				// step runs now. Its success deletes the entry.
				await scheduler.scheduleJob("work", Date.now());
				await scheduler.runDueWork();

				expect(pending).toBe(false);
				expect("work" in new FokosShardingStore(state.storage).getJobs()).toBe(false);
				expect(await state.storage.getAlarm()).toBeNull();
			},
		);
	});
});

describe("FokosScheduler - lifecycle reads", () => {
	it("gives all jobs one lifecycle result before the steps, and a new one after them", async () => {
		const stub = testPartitionStub(`scheduler-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: FokosDBPartitionDO, state: DurableObjectState) => {
			const store = new FokosShardingStore(state.storage);
			store.runMigrations();
			const made: FokosLifecycle[] = [];
			const seen: Array<{ name: string; lifecycle: FokosLifecycle }> = [];
			let pending = true;
			const job = (name: string): FokosJob => ({
				name,
				canRun: (lifecycle) => {
					seen.push({ name, lifecycle });
					return true;
				},
				deadline: () => (pending ? Date.now() - 1 : null),
				runStep: () => {
					pending = false;
					return { nextRunAt: null };
				},
			});
			const jobs = [job("first"), job("second")];
			const scheduler = new FokosScheduler({
				storage: state.storage,
				store,
				fallbackAlarmMs: () => FALLBACK_MS,
				fastPathDelayMs: () => 0,
				isFenced: () => false,
				lifecycle: () => {
					const lifecycle = ownerLifecycle();
					made.push(lifecycle);
					return lifecycle;
				},
				jobs: () => jobs,
				logParams: () => ({}),
			});
			try {
				await scheduler.runDueWork();
			} finally {
				await state.storage.deleteAlarm();
			}

			// One result for the check before the steps, and a new one for the check after them, because a
			// step can change the facts.
			expect(made).toHaveLength(2);
			expect(seen.map(({ name, lifecycle }) => [name, made.indexOf(lifecycle)])).toEqual([
				["first", 0],
				["second", 0],
				["first", 1],
				["second", 1],
			]);
		});
	});

	it("keeps each lifecycle field of one result at its first read, and a new result reads again", async () => {
		const stub = testPartitionStub(`scheduler-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (instance: FokosDBPartitionDO, state: DurableObjectState) => {
			const before = instance.fokos.lifecycle();
			expect(before.destroying).toBe(false);

			new FokosShardingStore(state.storage).setDestroying();

			expect(before.destroying).toBe(false);
			expect(instance.fokos.lifecycle().destroying).toBe(true);
		});
	});
});
