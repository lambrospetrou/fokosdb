import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { PartitionDO } from "../server/do-partition.js";
import { testPartitionStub } from "../../test/stub-helpers.js";
import { FokosScheduler } from "./scheduler.js";
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
): Promise<void> {
	const stub = testPartitionStub(`scheduler-test.${crypto.randomUUID()}`);
	await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
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

describe("FokosScheduler - lifecycle reads", () => {
	it("gives all jobs one lifecycle result before the steps, and a new one after them", async () => {
		const stub = testPartitionStub(`scheduler-test.${crypto.randomUUID()}`);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
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
		await runInDurableObject(stub, async (instance: PartitionDO, state: DurableObjectState) => {
			const before = instance.fokos.lifecycle();
			expect(before.destroying).toBe(false);

			new FokosShardingStore(state.storage).setDestroying();

			expect(before.destroying).toBe(false);
			expect(instance.fokos.lifecycle().destroying).toBe(true);
		});
	});
});
