/**
 * The DO's background-work machinery, split into three independent concerns:
 *
 * 1. BackgroundJobRunner — the jobs themselves: run them in order, isolate failures, and resolve
 *    the next time the work needs a wake-up. Trigger-agnostic.
 * 2. AlarmScheduler — the DO alarm: the RELIABLE progress mechanism. Even if the isolate is
 *    evicted and every pending setTimeout dies with it, the alarm fires, the DO's alarm() runs
 *    the jobs, and the run re-arms the alarm from the runner's result. Parts 1+2 alone keep the
 *    partition making progress.
 * 3. BackgroundScheduler — the in-memory setTimeout fast path that triggers runs sooner than the
 *    alarm would, plus the run-concurrency policy. Pure acceleration: deleting it would only make
 *    progress slower (alarm cadence), never wrong.
 */

/**
 * INVARIANTS FOR ALL BACKGROUND JOBS:
 * - They should be idempotent and safe to run concurrently (e.g. if the alarm fires again while a previous run is still ongoing) to avoid issues with retries and overlapping runs.
 * - They should be crash-safe, meaning that if they crash they should not cause the rest jobs to not run and they should be able to resume or retry their work without causing inconsistencies or data loss.
 * - If they encounter an error, they should log it and reschedule the next run for some time in the future ensuring progress is made eventually.
 */
export type BackgroundJob = {
	/** Used in failure logs: `fokos/partition: ${name} job failed.` */
	name: string;
	run(): Promise<void>;
	/**
	 * The wall-clock ms at which this job wants a fallback alarm, or null when it has no pending
	 * work. Called after every run (inside `transactionSync` when provided); the runner returns
	 * the minimum across jobs.
	 */
	nextAlarmMs(): number | null;
};

export type BackgroundJobRunnerDeps = {
	/** Structured-log context (the DO's logParams), so extracted logs keep their shape. */
	logParams: () => Record<string, unknown>;
	/** Invoked at the start of every run; throwing aborts the run (e.g. context-initialized invariant). */
	beforeRun?: () => void;
	/** Wraps the post-run `nextAlarmMs()` sweep (the DO passes store.transactionSync for a consistent snapshot). */
	transactionSync?: <T>(fn: () => T) => T;
};

/** Runs the registered jobs; knows nothing about alarms or timers. */
export class BackgroundJobRunner {
	#jobs: BackgroundJob[] = [];

	constructor(private readonly deps: BackgroundJobRunnerDeps) {}

	register(job: BackgroundJob): void {
		this.#jobs.push(job);
	}

	/**
	 * Runs every job sequentially — one job failing never blocks the rest — then resolves the
	 * earliest wall-clock ms at which any job still has pending work, or null when all are idle.
	 */
	async runOnce(): Promise<number | null> {
		this.deps.beforeRun?.();

		for (const job of this.#jobs) {
			try {
				await job.run();
			} catch (error) {
				console.error({
					...this.deps.logParams(),
					message: `fokos/partition: ${job.name} job failed.`,
					error: String(error),
					errorProps: error,
				});
			}
		}

		let nextAlarmMs: number | null = null;
		const sweepNextAlarms = () => {
			for (const job of this.#jobs) {
				const ms = job.nextAlarmMs();
				if (ms !== null && (nextAlarmMs === null || ms < nextAlarmMs)) {
					nextAlarmMs = ms;
				}
			}
		};
		if (this.deps.transactionSync) {
			this.deps.transactionSync(sweepNextAlarms);
		} else {
			sweepNextAlarms();
		}
		return nextAlarmMs;
	}
}

/**
 * Simple DO-alarm scheduler: the reliable, eviction-proof progress guarantee. Request paths arm
 * it directly (e.g. a prepare arms the stale-tx deadline); every background run re-arms it from
 * the runner's resolved next-alarm time.
 */
export class AlarmScheduler {
	constructor(private readonly storage: DurableObjectStorage) {}

	/** Sets the DO alarm to `targetMs` unless an earlier alarm is already pending (only ever moves it earlier). */
	async ensureAlarmSet(targetMs: number): Promise<void> {
		const existing = await this.storage.getAlarm();
		if (existing === null || targetMs < existing) {
			await this.storage.setAlarm(targetMs);
		}
	}
}

export type BackgroundSchedulerDeps = {
	runner: Pick<BackgroundJobRunner, "runOnce">;
	alarm: Pick<AlarmScheduler, "ensureAlarmSet">;
	/** Structured-log context (the DO's logParams), so extracted logs keep their shape. */
	logParams: () => Record<string, unknown>;
	/**
	 * Max runs in flight at once. Default 2: one normal run, plus one that may start when the
	 * first is slow or stuck (e.g. on a long remote RPC call) so background work is never blocked
	 * behind it.
	 */
	maxConcurrentRuns?: number;
	/**
	 * How old the newest in-flight run must be before a pending trigger may start an additional
	 * concurrent run instead of waiting for it to complete.
	 */
	allowOverlapAfterMs?: number;
};

const DEFAULT_MAX_CONCURRENT_RUNS = 2;
const DEFAULT_ALLOW_OVERLAP_AFTER_MS = 5_000;

type ActiveRun = { startedAt: number };

/**
 * Triggers job runs in-memory. The setTimeout path exists to speed up the alarm path: after every
 * run with pending work the alarm is re-armed (correctness) AND a near-immediate trigger is
 * scheduled (speed). All triggers — the pending timer, the racer, and the DO alarm handler —
 * enter through runJobs(), so the run-concurrency policy is enforced uniformly.
 *
 * Concurrency policy — race stuck runs instead of blocking behind them:
 * - ONE pending timer holds future demand: an earlier schedule() replaces it, a later one is
 *   ignored (the pending trigger fires sooner anyway).
 * - Every run arms the ONE racer timer at start, for `allowOverlapAfterMs` later: its watchdog.
 *   If the run completes in time it clears the racer — but only if the slot still holds the
 *   handle it armed; a different handle belongs to a racing run watching itself. If the racer
 *   fires first, the run is presumed slow/stuck (e.g. on a long remote RPC) and a racing run
 *   starts alongside it — escalating one race per `allowOverlapAfterMs`, up to
 *   `maxConcurrentRuns`.
 * - A trigger while runs are active is dropped when the racer is armed or capacity is reached —
 *   safely, because triggers persist job state BEFORE scheduling and every completing run
 *   re-schedules itself from its end-of-run sweep of that state, with the fallback alarm
 *   backstopping everything. With runs active but the racer slot EMPTY, the trigger starts a
 *   racing run itself: a young run always has its racer armed (it leaves the slot only by firing
 *   or by its owner completing), so an empty slot means the newest run is already
 *   `allowOverlapAfterMs` old.
 */
export class BackgroundScheduler {
	#pendingTimer: { timer: ReturnType<typeof setTimeout>; atMs: number } | null = null;
	#racerTimer: ReturnType<typeof setTimeout> | null = null;
	#activeRuns = new Set<ActiveRun>();
	#disposed = false;

	readonly #maxConcurrentRuns: number;
	readonly #allowOverlapAfterMs: number;

	constructor(private readonly deps: BackgroundSchedulerDeps) {
		this.#maxConcurrentRuns = deps.maxConcurrentRuns ?? DEFAULT_MAX_CONCURRENT_RUNS;
		this.#allowOverlapAfterMs = deps.allowOverlapAfterMs ?? DEFAULT_ALLOW_OVERLAP_AFTER_MS;
	}

	/** Whether a run is currently in flight (exposed for the DO's __testing__ hook only). */
	get isRunning(): boolean {
		return this.#activeRuns.size > 0;
	}

	schedule(ops: { delayMs: number }): void {
		if (this.#disposed) {
			return;
		}
		const delayMs = ops.delayMs ?? 10;
		const atMs = Date.now() + delayMs;
		if (this.#pendingTimer !== null) {
			if (this.#pendingTimer.atMs <= atMs) {
				// The pending trigger fires sooner (or at the same time) anyway.
				return;
			}
			clearTimeout(this.#pendingTimer.timer);
			this.#pendingTimer = null;
		}
		const timer = setTimeout(() => {
			if (this.#pendingTimer?.timer === timer) {
				this.#pendingTimer = null;
			}
			void this.runJobs();
		}, delayMs);
		this.#pendingTimer = { timer, atMs };
	}

	/** Cancels both timers and refuses future schedule()/runJobs() calls; called by destroyPartition. */
	dispose(): void {
		this.#disposed = true;
		if (this.#pendingTimer !== null) {
			clearTimeout(this.#pendingTimer.timer);
			this.#pendingTimer = null;
		}
		if (this.#racerTimer !== null) {
			clearTimeout(this.#racerTimer);
			this.#racerTimer = null;
		}
	}

	/** The single run entry point for all triggers (the pending timer, the racer, and the DO alarm handler). */
	async runJobs(): Promise<void> {
		if (this.#disposed) {
			return;
		}
		if (this.#activeRuns.size > 0) {
			if (this.#racerTimer !== null || this.#activeRuns.size >= this.#maxConcurrentRuns) {
				// Drop: the racer covers a stuck run, the end-of-run sweep covers a completing
				// one, and the fallback alarm backstops both.
				return;
			}
			// Racer slot empty while runs are active ⇒ the newest run is already
			// allowOverlapAfterMs old (see the class doc) — fall through and race it. Defensive
			// guard in case the invariant is ever broken: re-arm the watchdog instead of racing
			// a young run early.
			const raceAtMs = this.#newestRunStartedAt() + this.#allowOverlapAfterMs;
			if (Date.now() < raceAtMs) {
				this.#racerTimer = this.#armRacerTimer(raceAtMs);
				return;
			}
		}
		await this.#run();
	}

	async #run(): Promise<void> {
		const run: ActiveRun = { startedAt: Date.now() };
		this.#activeRuns.add(run);
		// Watchdog: if this run is still in flight in allowOverlapAfterMs, race it.
		const racer = this.#armRacerTimer(run.startedAt + this.#allowOverlapAfterMs);
		this.#racerTimer = racer;
		try {
			const nextAlarmMs = await this.deps.runner.runOnce();
			if (nextAlarmMs !== null) {
				// Correctness: the alarm guarantees progress even if this isolate is evicted.
				await this.deps.alarm.ensureAlarmSet(nextAlarmMs);
				// Speed: keep making progress without waiting for the alarm.
				this.schedule({ delayMs: 10 });
			} else {
				console.log({
					...this.deps.logParams(),
					message: "fokos/partition: Background work ran, nothing to schedule forward.",
				});
			}
		} finally {
			this.#activeRuns.delete(run);
			// Clear the watchdog this run armed — and only that one: a different handle in the
			// slot belongs to a racing run watching itself.
			if (this.#racerTimer === racer) {
				clearTimeout(racer);
				this.#racerTimer = null;
			}
		}
	}

	#newestRunStartedAt(): number {
		let newest = 0;
		for (const run of this.#activeRuns) {
			if (run.startedAt > newest) {
				newest = run.startedAt;
			}
		}
		return newest;
	}

	/** Creates a racer timer for `atMs`, replacing whatever is in the slot (callers own the slot update). */
	#armRacerTimer(atMs: number): ReturnType<typeof setTimeout> {
		if (this.#racerTimer !== null) {
			// Only reachable when a run starts while a stale unowned racer (defensive-guard arm)
			// lingers; the new run's watchdog supersedes it.
			clearTimeout(this.#racerTimer);
			this.#racerTimer = null;
		}
		const timer = setTimeout(
			() => {
				if (this.#racerTimer === timer) {
					this.#racerTimer = null;
				}
				// Re-evaluates the gate: races the still-in-flight run, or no-ops at capacity.
				void this.runJobs();
			},
			Math.max(0, atMs - Date.now()),
		);
		return timer;
	}
}
