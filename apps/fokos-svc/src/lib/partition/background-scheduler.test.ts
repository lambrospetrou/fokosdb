import { env } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it, vi } from "vitest";
import { PartitionDO } from "../do-partition.js";
import { AlarmScheduler, BackgroundJobRunner, BackgroundScheduler, type BackgroundJob } from "./background-scheduler.js";

function job(overrides: Partial<BackgroundJob>): BackgroundJob {
	return { name: "test", run: async () => {}, nextAlarmMs: () => null, ...overrides };
}

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

describe("BackgroundJobRunner", () => {
	it("runs jobs sequentially in registration order", async () => {
		const order: string[] = [];
		const runner = new BackgroundJobRunner({ logParams: () => ({}) });
		runner.register(job({ name: "a", run: async () => void order.push("a") }));
		runner.register(job({ name: "b", run: async () => void order.push("b") }));
		runner.register(job({ name: "c", run: async () => void order.push("c") }));
		await runner.runOnce();
		expect(order).toEqual(["a", "b", "c"]);
	});

	it("a failing job is logged and never blocks the rest", async () => {
		const order: string[] = [];
		const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
		try {
			const runner = new BackgroundJobRunner({ logParams: () => ({ actor: "x" }) });
			runner.register(
				job({
					name: "boom",
					run: async () => {
						throw new Error("boom");
					},
				}),
			);
			runner.register(job({ name: "b", run: async () => void order.push("b") }));
			await runner.runOnce();
			expect(order).toEqual(["b"]);
			expect(errorSpy).toHaveBeenCalledWith(
				expect.objectContaining({ actor: "x", message: "fokos/partition: boom job failed.", error: "Error: boom" }),
			);
		} finally {
			errorSpy.mockRestore();
		}
	});

	it("resolves the minimum nextAlarmMs across jobs, and null when all jobs are idle", async () => {
		const runner = new BackgroundJobRunner({ logParams: () => ({}) });
		let wantedA: number | null = null;
		let wantedB: number | null = null;
		runner.register(job({ name: "a", nextAlarmMs: () => wantedA }));
		runner.register(job({ name: "b", nextAlarmMs: () => wantedB }));

		expect(await runner.runOnce()).toBeNull();

		wantedA = 2_000;
		wantedB = 1_000;
		expect(await runner.runOnce()).toBe(1_000);

		wantedB = null;
		expect(await runner.runOnce()).toBe(2_000);
	});

	it("beforeRun throwing aborts before any job runs", async () => {
		const order: string[] = [];
		const runner = new BackgroundJobRunner({
			logParams: () => ({}),
			beforeRun: () => {
				throw new Error("not initialized");
			},
		});
		runner.register(job({ name: "a", run: async () => void order.push("a") }));
		await expect(runner.runOnce()).rejects.toThrow("not initialized");
		expect(order).toEqual([]);
	});

	it("sweeps nextAlarmMs inside the provided transactionSync", async () => {
		let sweptInsideTransaction = false;
		let inTransaction = false;
		const runner = new BackgroundJobRunner({
			logParams: () => ({}),
			transactionSync: (fn) => {
				inTransaction = true;
				try {
					return fn();
				} finally {
					inTransaction = false;
				}
			},
		});
		runner.register(
			job({
				name: "a",
				nextAlarmMs: () => {
					sweptInsideTransaction = inTransaction;
					return null;
				},
			}),
		);
		await runner.runOnce();
		expect(sweptInsideTransaction).toBe(true);
	});
});

describe("AlarmScheduler", () => {
	// Runs `fn` against REAL Durable Object storage (vitest-pool-workers), with far-future alarm
	// times so the alarm never actually fires, and the alarm deleted before the DO is released.
	async function withStorage(fn: (storage: DurableObjectStorage) => Promise<void>): Promise<void> {
		const id = env.PARTITION_DO.idFromName(`alarm-scheduler-test.${crypto.randomUUID()}`);
		const stub = env.PARTITION_DO.get(id);
		await runInDurableObject(stub, async (_instance: PartitionDO, state: DurableObjectState) => {
			try {
				await fn(state.storage);
			} finally {
				await state.storage.deleteAlarm();
			}
		});
	}

	it("sets the alarm when none is pending and only ever moves it earlier", async () => {
		await withStorage(async (storage) => {
			const alarm = new AlarmScheduler(storage);
			const base = Date.now() + 120_000;

			await alarm.ensureAlarmSet(base);
			expect(await storage.getAlarm()).toBe(base);

			// A later target never postpones the pending alarm.
			await alarm.ensureAlarmSet(base + 60_000);
			expect(await storage.getAlarm()).toBe(base);

			// An earlier target moves it forward.
			await alarm.ensureAlarmSet(base - 60_000);
			expect(await storage.getAlarm()).toBe(base - 60_000);
		});
	});
});

describe("BackgroundScheduler", () => {
	// In-memory fake of BackgroundJobRunner: counts runs, tracks concurrency, and can block runs
	// on gates to simulate slow/stuck background work.
	class FakeRunner {
		runs = 0;
		active = 0;
		peakConcurrency = 0;
		nextAlarm: (runIndex: number) => number | null = () => null;
		blockNextRuns = 0;
		#gates: Array<() => void> = [];

		async runOnce(): Promise<number | null> {
			const idx = this.runs++;
			this.active++;
			this.peakConcurrency = Math.max(this.peakConcurrency, this.active);
			try {
				if (this.blockNextRuns > 0) {
					this.blockNextRuns--;
					await new Promise<void>((resolve) => this.#gates.push(resolve));
				}
			} finally {
				this.active--;
			}
			return this.nextAlarm(idx);
		}

		releaseAll(): void {
			for (const release of this.#gates.splice(0)) release();
		}
	}

	function makeScheduler(opts?: { maxConcurrentRuns?: number; allowOverlapAfterMs?: number }) {
		const runner = new FakeRunner();
		const alarmCalls: number[] = [];
		const scheduler = new BackgroundScheduler({
			runner,
			alarm: { ensureAlarmSet: async (ms: number) => void alarmCalls.push(ms) },
			logParams: () => ({}),
			...opts,
		});
		return { runner, alarmCalls, scheduler };
	}

	it("coalesces concurrent schedule calls into one run", async () => {
		const { runner, scheduler } = makeScheduler();
		for (let i = 0; i < 5; i++) {
			scheduler.schedule({ delayMs: 5 });
		}
		await vi.waitUntil(() => runner.runs >= 1, { timeout: 2_000, interval: 10 });
		await sleep(50);
		expect(runner.runs).toBe(1);
		scheduler.dispose();
	});

	it("an earlier schedule replaces the pending timer; a later one is ignored", async () => {
		const { runner, scheduler } = makeScheduler();

		// Later target is ignored — the pending trigger fires sooner anyway.
		scheduler.schedule({ delayMs: 5 });
		scheduler.schedule({ delayMs: 30 });
		await vi.waitUntil(() => runner.runs >= 1, { timeout: 2_000, interval: 10 });
		await sleep(60);
		expect(runner.runs).toBe(1);

		// Earlier target cancels and replaces the pending timer — the replaced one never fires.
		scheduler.schedule({ delayMs: 30 });
		scheduler.schedule({ delayMs: 5 });
		await vi.waitUntil(() => runner.runs >= 2, { timeout: 2_000, interval: 10 });
		await sleep(60);
		expect(runner.runs).toBe(2);
		scheduler.dispose();
	});

	it("triggers during a young run are dropped, and a timely completion clears its own racer", async () => {
		const { runner, scheduler } = makeScheduler({ allowOverlapAfterMs: 100 });
		runner.blockNextRuns = 1;
		scheduler.schedule({ delayMs: 0 });
		await vi.waitUntil(() => runner.active === 1, { timeout: 2_000, interval: 10 });

		// The run armed its racer at start, so triggers while it is young are dropped.
		scheduler.schedule({ delayMs: 0 });
		await sleep(30);
		void scheduler.runJobs();
		expect(runner.runs).toBe(1);

		// Completing within allowOverlapAfterMs clears the racer; with no pending job state
		// (nextAlarm null) there is nothing to follow up on — no spurious racing run later.
		runner.releaseAll();
		await vi.waitUntil(() => runner.active === 0, { timeout: 2_000, interval: 10 });
		await sleep(150); // past the racer deadline
		expect(runner.runs).toBe(1);
		expect(runner.peakConcurrency).toBe(1);
		scheduler.dispose();
	});

	it("a stuck run is raced automatically after allowOverlapAfterMs, with no extra trigger", async () => {
		const { runner, scheduler } = makeScheduler({ allowOverlapAfterMs: 50 });
		runner.blockNextRuns = 1;
		scheduler.schedule({ delayMs: 0 });
		await vi.waitUntil(() => runner.active === 1, { timeout: 2_000, interval: 10 });

		// The watchdog armed at run start fires on its own and races the stuck run.
		await vi.waitUntil(() => runner.runs === 2, { timeout: 2_000, interval: 10 });
		expect(runner.peakConcurrency).toBe(2);

		runner.releaseAll();
		await vi.waitUntil(() => runner.active === 0, { timeout: 2_000, interval: 10 });
		expect(runner.runs).toBe(2);
		scheduler.dispose();
	});

	it("a trigger races a surviving stuck run itself when the racer slot is empty", async () => {
		const { runner, scheduler } = makeScheduler({ allowOverlapAfterMs: 50, maxConcurrentRuns: 2 });
		// Only the first run gets stuck; its racing run completes immediately and clears the racer
		// it armed for itself — leaving the stuck survivor with an empty racer slot.
		runner.blockNextRuns = 1;
		scheduler.schedule({ delayMs: 0 });
		await vi.waitUntil(() => runner.runs === 2 && runner.active === 1, { timeout: 2_000, interval: 10 });

		// A trigger now finds runs active but no racer armed ⇒ the survivor is ≥50ms old → races it.
		await sleep(20);
		await scheduler.runJobs();
		expect(runner.runs).toBe(3);

		runner.releaseAll();
		await vi.waitUntil(() => runner.active === 0, { timeout: 2_000, interval: 10 });
		scheduler.dispose();
	});

	it("at maxConcurrentRuns capacity further triggers are dropped", async () => {
		const { runner, scheduler } = makeScheduler({ allowOverlapAfterMs: 40, maxConcurrentRuns: 2 });
		runner.blockNextRuns = 2;
		scheduler.schedule({ delayMs: 0 });

		// The stuck run's watchdog starts the racing run, which gets stuck too.
		await vi.waitUntil(() => runner.active === 2, { timeout: 2_000, interval: 10 });

		// At capacity, triggers (and the second run's own racer firing at ~80ms) are no-ops.
		void scheduler.runJobs();
		await sleep(120);
		expect(runner.runs).toBe(2);

		// Completions with no pending job state don't re-run either (the trigger was dropped;
		// pending work would resurface via the end-of-run sweep or the fallback alarm).
		runner.releaseAll();
		await vi.waitUntil(() => runner.active === 0, { timeout: 2_000, interval: 10 });
		await sleep(100);
		expect(runner.runs).toBe(2);
		scheduler.dispose();
	});

	it("re-arms the fallback alarm from the run result and keeps going until the jobs are idle", async () => {
		const { runner, alarmCalls, scheduler } = makeScheduler();
		const wantedAlarm = Date.now() + 60_000;
		runner.nextAlarm = (runIndex) => (runIndex === 0 ? wantedAlarm : null);

		scheduler.schedule({ delayMs: 0 });
		// Run 1 reports pending work → alarm armed + immediate follow-up run; run 2 reports idle → stop.
		await vi.waitUntil(() => runner.runs >= 2, { timeout: 2_000, interval: 10 });
		await sleep(50);
		expect(runner.runs).toBe(2);
		expect(alarmCalls).toEqual([wantedAlarm]);
		scheduler.dispose();
	});

	it("dispose cancels pending timers and refuses new triggers", async () => {
		const { runner, scheduler } = makeScheduler();
		scheduler.schedule({ delayMs: 5 });
		scheduler.dispose();
		await sleep(50);
		expect(runner.runs).toBe(0);
		scheduler.schedule({ delayMs: 0 });
		await scheduler.runJobs();
		await sleep(30);
		expect(runner.runs).toBe(0);
	});
});
