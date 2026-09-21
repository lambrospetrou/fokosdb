/**
 * The one scheduler of a partition. It owns the Durable Object alarm and drives every background job
 * through it: the built-in import, acknowledgement, repartition and cleanup steps first, then the host
 * jobs in registration order.
 *
 * A job runs one bounded, idempotent step and reports when it wants to run next. The scheduler writes
 * that deadline to `__fokos/jobs` right after the step, arms the alarm at the earliest durable deadline
 * it knows, and runs at most one pass at a time: a request or an alarm that arrives during a pass waits
 * for it and then gets one more pass.
 */
import type { FokosJob } from "./runtime-types.js";
import type { FokosShardingStore } from "./sharding-store.js";

export type FokosSchedulerDeps = {
	storage: DurableObjectStorage;
	store: FokosShardingStore;
	/** How far ahead a pass arms its fallback before its first transition. */
	fallbackAlarmMs: number;
	/** The delay of the in-memory fast path. */
	fastPathDelayMs: number;
	/** True after the destroy fence. A fenced pass runs nothing and arms nothing. */
	isFenced: () => boolean;
	/** Built-in jobs first, then host jobs. Read at the start of every pass, and again at its end. */
	jobs: () => readonly FokosJob[];
	logParams: () => Record<string, unknown>;
};

export class FokosScheduler {
	readonly #deps: FokosSchedulerDeps;
	#inFlight: Promise<void> | null = null;
	#queued: Promise<void> | null = null;
	#fastPath: ReturnType<typeof setTimeout> | null = null;

	constructor(deps: FokosSchedulerDeps) {
		this.#deps = deps;
	}

	/** The pass in flight, so a destroy can wait for it before it deletes the alarm. */
	inFlight(): Promise<void> {
		return this.#inFlight?.catch(() => {}) ?? Promise.resolve();
	}

	/**
	 * Runs one pass, or joins the pass in flight and runs one more after it. Two passes never
	 * interleave: two steps over one import would each hold a page the other has moved past.
	 */
	runDueWork(): Promise<void> {
		if (!this.#inFlight) {
			this.#inFlight = this.#pass().finally(() => {
				this.#inFlight = null;
			});
			return this.#inFlight;
		}
		this.#queued ??= this.#inFlight
			.catch(() => {})
			.then(() => {
				this.#queued = null;
				return this.runDueWork();
			});
		return this.#queued;
	}

	/** Runs a pass in this isolate soon, without an alarm. A pending timer absorbs a second call. */
	wake(): void {
		if (this.#fastPath !== null) return;
		this.#fastPath = setTimeout(() => {
			this.#fastPath = null;
			this.runDueWork().catch((error: unknown) => {
				console.error({ ...this.#deps.logParams(), message: "fokos/scheduler: the fast-path pass failed.", error: String(error) });
			});
		}, this.#deps.fastPathDelayMs);
	}

	/** Stops the fast path. The alarm is the caller's to delete. */
	stop(): void {
		if (this.#fastPath !== null) clearTimeout(this.#fastPath);
		this.#fastPath = null;
	}

	/**
	 * Moves the next run of one job earlier, never later, and arms the alarm for it when the current
	 * alarm is missing or later. One transaction reads the record first, so a write from a pass that
	 * runs at the same time cannot be lost.
	 */
	async scheduleJob(name: string, runAt: number): Promise<void> {
		const moved = this.#deps.store.transactionSync(() => {
			const record = this.#deps.store.getJobs();
			const current = record[name]?.nextRunAt;
			if (current !== undefined && current <= runAt) return false;
			record[name] = { nextRunAt: runAt };
			this.#deps.store.putJobs(record);
			return true;
		});
		if (!moved) return;
		await this.ensureAlarmAtMost(runAt);
		if (runAt <= Date.now()) this.wake();
	}

	/** Sets the alarm to `at` when no alarm exists or the existing one is later. */
	async ensureAlarmAtMost(at: number): Promise<void> {
		const existing = await this.#deps.storage.getAlarm();
		if (existing === null || at < existing) await this.#deps.storage.setAlarm(at);
	}

	/**
	 * One pass. Job errors are caught and logged, so one failing job never stops another. An
	 * alarm-storage error escapes, so the platform retries the alarm.
	 */
	async #pass(): Promise<void> {
		if (this.#deps.isFenced()) return;
		const now = Date.now();
		const runnable = this.#runnable();
		const due = this.#deadlines(runnable).filter(({ at }) => at <= now);

		const earliest = this.#earliest(runnable);
		if (earliest === null) return;
		if (due.length === 0) {
			await this.ensureAlarmAtMost(earliest);
			return;
		}

		// Armed BEFORE the pass changes state or calls an RPC. A crash inside the pass then leaves an
		// alarm that can read the new durable state. The end of the pass replaces it with the earliest
		// real deadline.
		await this.ensureAlarmAtMost(now + this.#deps.fallbackAlarmMs);

		for (const { job } of due) {
			if (this.#deps.isFenced()) return;
			let nextRunAt: number | null;
			try {
				nextRunAt = (await job.runStep()).nextRunAt;
			} catch (error) {
				console.error({
					...this.#deps.logParams(),
					message: `fokos/scheduler: the ${job.name} job failed.`,
					error: String(error),
					errorProps: error,
				});
				nextRunAt = Date.now() + this.#deps.fallbackAlarmMs;
			}
			// Written right after the step, and from a fresh read: a request can call `scheduleJob` for
			// another job while the step awaits, and a batch write at the end would put stale values over it.
			this.#deps.store.transactionSync(() => {
				const record = this.#deps.store.getJobs();
				if (nextRunAt === null) {
					if (!(job.name in record)) return;
					delete record[job.name];
				} else {
					if (record[job.name]?.nextRunAt === nextRunAt) return;
					record[job.name] = { nextRunAt };
				}
				this.#deps.store.putJobs(record);
			});
		}

		if (this.#deps.isFenced()) return;
		// This write REPLACES the fallback the pass armed, and it can move the alarm later. The pass is
		// over here, so the earlier fallback protects nothing.
		//
		// `canRun` is asked again, and never read from the list this pass started with: a step can make
		// another job runnable, and a job left out here loses its deadline and the alarm with it.
		const next = this.#earliest(this.#runnable());
		if (next === null) {
			await this.#deps.storage.deleteAlarm();
			return;
		}
		await this.#deps.storage.setAlarm(next);
		if (next <= Date.now()) this.wake();
	}

	/** The jobs that can run now, built-ins first. Asked again whenever the answer can have changed. */
	#runnable(): FokosJob[] {
		return this.#deps.jobs().filter((job) => job.canRun());
	}

	/** The earliest durable deadline of each runnable job: its scheduled run, or its own durable work. */
	#deadlines(runnable: readonly FokosJob[]): Array<{ job: FokosJob; at: number }> {
		const record = this.#deps.store.getJobs();
		const out: Array<{ job: FokosJob; at: number }> = [];
		for (const job of runnable) {
			const scheduled = record[job.name]?.nextRunAt ?? null;
			const own = job.deadline?.() ?? null;
			const at = scheduled === null ? own : own === null ? scheduled : Math.min(scheduled, own);
			if (at !== null) out.push({ job, at });
		}
		return out;
	}

	#earliest(runnable: readonly FokosJob[]): number | null {
		// FIXME: Index the jobs in a smarter way to avoid scanning all deadlines every time.
		let earliest: number | null = null;
		for (const { at } of this.#deadlines(runnable)) {
			if (earliest === null || at < earliest) earliest = at;
		}
		return earliest;
	}
}
