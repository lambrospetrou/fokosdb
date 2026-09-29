import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { PartitionDO } from "../server/do-partition.js";
import { testPartitionStub } from "../../test/stub-helpers.js";
import { FokosScheduler } from "./scheduler.js";
import { FokosShardingStore } from "./sharding-store.js";
import type { FokosJob } from "./runtime-types.js";

const FALLBACK_MS = 60_000;

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
