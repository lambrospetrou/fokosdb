# RFC — Bounded failure behavior for alarms, background jobs, and Durable Object initialization

**State:** Draft
**Date:** 2026-10-31
**Author:** Lambros Petrou

**Status:** Nothing is built. Section 3 lists the milestones.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
  - [4.3 Open questions](#43-open-questions)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

A Durable Object alarm has at-least-once delivery. When `alarm()` returns with an uncaught error, the platform
runs it again, up to 6 retries with a backoff that starts at 2 seconds. After the retries the alarm does not run
again until the code calls `setAlarm` ([Alarms](https://developers.cloudflare.com/durable-objects/api/alarms/)).

`FokosScheduler` (`src/sharding/scheduler.ts`) owns the alarm of each partition Durable Object and runs each
background job through it. `FokosShardingAutoAdminDO` (`src/sharding/topology-keeper.ts`) owns a second alarm
that runs the topology builds. The Durable Object classes run their schema migrations inside
`ctx.blockConcurrencyWhile`, so a request never reaches storage before the schema is ready.

The `README.md` task list carries an open item: check for background alarms that run forever because of errors.
The example there is an `initFromSplit` conflict that made a Durable Object throw on every start. That code path
is gone: `fokosInit` is now an idempotent RPC, and a conflict throws back to the caller. An audit of the current
code found four failure shapes that remain.

### 1.1 A job step error before its durable deadline loops at `fastPathDelayMs`

`FokosScheduler.#pass` catches a `runStep` error and writes `record[job].nextRunAt = now + fallbackAlarmMs` to
`__fokos/jobs`. The end of the pass then computes the next run of each job as
`min(scheduled, own)`, where `own` is the deadline that the job reads from its own durable state
(`FokosScheduler.#deadlines`). A step that throws before it moves its durable deadline leaves `own` in the past.
The minimum picks the past time, and the pass ends with `setAlarm` at a past time plus `wake()`. The loop runs
one pass each `fastPathDelayMs` (50 ms by default, 0 is a valid override) inside the isolate, logs
`the <job> job failed` each pass, and runs again at once after an eviction, because the past alarm fires
immediately.

Steps that can throw before they move their durable deadline:

- `source_repartition`: the `invariant` checks of `RepartitionSource.#plan`, a missing `planned` head in
  `RepartitionSource.#initializeTargets`, a `beforeCutover` or `onRepartitionCutover` hook that throws
  (`src/sharding/repartition-flow.ts`).
- `source_cleanup`: a `cleanupSourceStep` hook that throws. `earliestCleanupDeadline` stays in the past
  (`src/sharding/repartition-flow.ts`).
- `target_import` and `target_ack`: `deps.getPeer(rec.source)` runs outside the `try` in
  `RepartitionTarget.importOnePage` and `RepartitionTarget.sendAck`. The stub factory reads `env[ctx.policy.ns]`,
  and a missing binding throws (`src/sharding/repartition-flow.ts`, `src/shared/do-stubs.ts`).
- `admin_outbox`: a store error before the per-row `setAdminOutboxAttempt`
  (`FokosShardingRuntime.#adminOutboxStep` in `src/sharding/runtime.ts`).
- Every host job (`stale_tx_recovery`, `materialization_export`, `tx_recovery`, `idempotency_sweep`,
  `target_push`): a `fokosConfig()` or `fokosRuntimeConfig()` override that throws makes `runStep` throw on each
  pass, while the durable deadline (`pending_tx_info.next_recovery_at`, the outbox `next_at`,
  `tc_state.next_recovery_at`, the target `retry` rows) stays in the past.

The codebase already knows this failure class for the idle case: `REPARTITION_HAS_STEP`
(`src/sharding/sharding-store.ts`) excludes a `cutover` row whose targets are all notified, because selecting it
would "reschedule the pass forever". The same mechanism on the error path is not covered.

### 1.2 A `fokosInit` conflict retries forever

`RepartitionTarget.initAsTarget` throws `partition_context_mismatch` when the stored import conflicts with the
request, or when the partition holds an identity and no import. `retryDelay` gives `nonRetryableRetryMs` only to
`repartition_unknown` and `repartition_target_unknown`. A context conflict compares durable immutable facts: a
retry sends identical bytes and gets the identical answer. The repartition stays `planned`, the source retries
`fokosInit` with `jitterBackoff` up to `sourceRetryMaxMs` forever, and each attempt logs an error. The conflict
also blocks later repartitions on that source: an unfinished promotion blocks a hash split, and a split row
blocks every later promotion.

### 1.3 A pass-level error consumes the 6 platform retries, then the work stops

The errors that `FokosScheduler.#pass` does not catch leave `alarm()`: the deadline reads of `#runnable` and
`#deadlines` (`job.canRun`, `job.deadline`, `getJobs`, the `lifecycle` facts), the fallback `ensureAlarmAtMost`,
and the final `setAlarm` or `deleteAlarm`. The platform retries at most 6 times, then the alarm does not run
again until a request arms one. A partition that serves no request after that keeps its durable deadlines but
never runs a pass: imports, acknowledgements, repartition steps, and host jobs all stop.

### 1.4 The admin alarm has no error handling

`FokosShardingAutoAdminDO.alarm` calls `FokosTopologyKeeper.runDueBuilds` without a `try`. A build that throws
leaves `alarm()` before `setAlarm`, consumes the 6 platform retries, and then stops. `build_due_at` stays in the
past, so each new report of any shard group arms the alarm and fails again at the same group. The failed group
also blocks the groups behind it: `runDueBuilds` iterates `ORDER BY build_due_at` and the throw ends the loop.

### 1.5 A deterministic initialization error bricks the Durable Object

`ctx.blockConcurrencyWhile` runs the sharding migrations, the host migrations, and the identity reads in the
constructor of each Durable Object (`FokosShardingRuntime`, `FokosDBPartitionDO`,
`FokosDBTransactionCoordinatorDO`, `FokosDBMaterializationDO`, `FokosShardingAutoAdminDO`). A deterministic
failure — a schema version ahead of the code after a downgrade, corrupt storage, or an override that throws —
rejects the callback. Every RPC then fails, the alarm retries 6 times and stops, and `fokosDestroy` cannot run:
the destroy flow cannot wipe the poisoned partition.

### Glossary

- **Pass** — one run of `FokosScheduler.#pass`: the due job steps plus the alarm re-arm.
- **Durable deadline** — the next-run time that a job reads from its own tables (`job.deadline()`), as opposed
  to the scheduled time that the scheduler stores in `__fokos/jobs`.
- **Init gate** — the check that turns a failed initialization into a typed error on each request.

## 2. Goals and requirements

### 2.1 In scope

1. A `runStep` error must not run the job again before `fallbackAlarmMs`. A durable deadline of that job that
   stays in the past must not keep the pass awake.
2. `alarm()` must not return an uncaught error. A pass error logs and re-arms the fallback alarm.
3. `partition_context_mismatch` must get `nonRetryableRetryMs`, the same wait that `repartition_unknown` and
   `repartition_target_unknown` get.
4. A failed topology build must log, defer only its own shard group, and not block other groups. The admin
   `alarm()` must always leave an armed alarm.
5. A deterministic initialization error must not brick a Durable Object. Requests must get a typed error, and
   `fokosDestroy`, `fokosPrepareDestroy`, and `fokosStatus` must stay available. `fokosPromotions` reads the
   repartition tables, so it stays behind the gate.
6. The suppression state of goal 1 must survive an eviction, so it must live in `__fokos/jobs`.

### 2.2 Out of scope

- A terminal `failed` state for a repartition or an import. The decision is to keep the rows and retry at
  `nonRetryableRetryMs`; an operator reads `fokosStatus` for the diagnosis. A dead-letter state needs its own
  design.
- The retry policy of a `fokosMigrationPull` that gets `partition_migrating` from a completed source. The
  protocol makes that case unreachable.
- The client retry bounds. `partition_migrating` retries are bounded by `partitionMigratingRetryDeadlineMs`,
  and the forward retries by `maxForwardRetries`.
- A Worker-side circuit breaker for overloaded Durable Objects. That is a separate `README.md` item.

## 3. Milestones

- **M1 — Job failure backoff suppresses the stale deadline, and the alarm never throws.** Changes in
  `src/sharding/scheduler.ts`. A failed job waits `fallbackAlarmMs` independent of its durable deadline, and a
  pass error logs and re-arms the fallback alarm. Delivers the end of the ~50 ms error loop and of the
  six-retries-then-stop alarm loss.
- **M2 — Protocol-defect errors take the non-retryable wait.** A change in `retryDelay`
  (`src/sharding/repartition-flow.ts`). Delivers one log per `nonRetryableRetryMs` for a deterministic
  `fokosInit` or `fokosStartImport` conflict.
- **M3 — The topology keeper quarantines a failed group and the admin alarm always re-arms.** Changes in
  `src/sharding/topology-keeper.ts`. Delivers: a failed group delays itself, other groups proceed, and the admin
  alarm never dies on a build error.
- **M4 — Degraded initialization instead of a bricked Durable Object.** Changes in `src/sharding/runtime.ts`,
  `src/server/do-partition.ts`, `src/server/do-transaction-coordinator.ts`,
  `src/server/materialization/do-materialization.ts`, and `src/sharding/topology-keeper.ts`. Delivers: typed
  errors on requests, the destroy and status RPCs stay live, and the alarm stays armed at a long interval.

After M4 ships, the `README.md` item "Check for background alarms runaway errors due to errors" is removed. This
document is the record of the check.

## 4. Proposed solution

### 4.1 High-level overview

Three rules cover the four failure shapes:

1. **A failed job waits.** The scheduler marks the job entry in `__fokos/jobs` on a `runStep` error. While the
   mark stands, the durable deadline of that job does not count for scheduling. The job runs again at
   `fallbackAlarmMs`, and a successful run clears the mark.
2. **The alarm never throws.** The scheduler catches a pass error, logs it, and re-arms the fallback alarm as a
   best effort. The admin alarm does the same and quarantines a failed build group.
3. **A Durable Object that cannot initialize degrades.** `blockConcurrencyWhile` keeps its ordering guarantee —
   no event reaches storage before the schema step ends — but a failure is captured, not propagated. Requests
   get a typed error, `fokosDestroy` and the status RPCs stay available, and the alarm re-arms at a long
   interval.

### 4.2 Technical details

#### 4.2.1 M1 — the failure mark in `__fokos/jobs` and the pass catch

`FokosJobsRecord` entries get an optional `failed` flag:

```ts
{ nextRunAt: number, failed?: true }
```

Rules:

- On a `runStep` error, the catch in `#pass` writes `record[name] = { nextRunAt: now + fallbackAlarmMs, failed:
  true }`.
- `FokosScheduler.#deadlines` computes `at = scheduled` when `record[name]?.failed === true`. The durable
  deadline `own` does not count while the mark stands.
- Every other write of the entry drops the flag: `scheduleJob`, `scheduleJobSyncNoAlarm`, and the post-step
  write. A step that runs again and returns clears the mark, because the post-step write replaces the entry
  (`{ nextRunAt: t }`) or deletes it (`nextRunAt: null`). The durable deadline is authoritative again after a
  success.
- An explicit earlier `scheduleJob` clears the flag and runs the step earlier. A failure then sets the flag
  again. No state makes `min` pick a stale `own` while `failed` stands, so the loop of section 1.1 cannot form.
- The flag lives in `__fokos/jobs`, so it survives an eviction.

For the pass-level errors of section 1.3, `FokosScheduler.runDueWork` wraps the pass: on a rejection it logs the
error, calls `ensureAlarmAtMost(now + fallbackAlarmMs)` inside a nested `try`, and resolves. `alarm()` then
never returns an error to the platform, and the fallback alarm keeps the work alive at `fallbackAlarmMs`
cadence while the pass itself is broken. A `setAlarm` or storage error inside the nested `try` is swallowed:
nothing can re-arm while storage is down, and the platform retries are already consumed by then. The `wake()`
catch stays; it is harmless.

Testing:

- A job whose `runStep` always throws and whose `deadline()` stays in the past: assert one run each
  `fallbackAlarmMs`, one log each run, the alarm at `now + fallbackAlarmMs`, and no `wake()` chain.
- A job whose `deadline()` throws: assert the alarm is armed at the fallback and `alarm()` resolves.
- A job that fails once and succeeds after: assert the flag clears and the durable deadline drives again.
- `pnpm test` and `pnpm check`.

#### 4.2.2 M2 — `partition_context_mismatch` takes the non-retryable wait

`retryDelay` (`src/sharding/repartition-flow.ts`) returns `nonRetryableRetryMs` for
`SHARDING_INTERNAL_CODES.partition_context_mismatch`, in the same branch as `repartition_unknown` and
`repartition_target_unknown`. The code comment there already describes the class: "a protocol defect no retry
can fix keeps its state and its identifiers" — a context conflict is exactly that.

The repartition row and the target rows stay, so `fokosStatus` and `fokosPromotions` report the wedged state to
an operator. The attempts counter keeps counting, so the log line shows how long the conflict stands.

Testing: a target that holds the import of a different repartition answers `fokosInit` with
`partition_context_mismatch`; assert `next_attempt_at` moves by about `nonRetryableRetryMs` and the repartition
stays `planned`.

#### 4.2.3 M3 — per-group quarantine and an unconditional re-arm in the admin DO

`FokosTopologyKeeperOptions` gets `buildErrorRetryMs?: number`, default `60_000`, minimum `1`.

`FokosTopologyKeeper.runDueBuilds` wraps each `#buildStep` call in a `try`. On an error it logs the error, sets
`build_due_at = now + buildErrorRetryMs` for that shard group only, and continues with the next due group. A
failed group retries at `buildErrorRetryMs` instead of blocking the `ORDER BY build_due_at` loop.

`FokosShardingAutoAdminDO.alarm` calls `runDueBuilds` inside a `try` that logs a failure, and then always arms
an alarm: `setAlarm(Math.max(next, now + 1))` with `next` from `keeper.nextBuildAt()`. When `nextBuildAt()` itself
throws or returns `null` after a failed pass, the handler arms `now + buildErrorRetryMs`. The alarm therefore
never leaves the object without an armed alarm while builds are due or broken.

Testing:

- A `#buildStep` that throws for one group: the group gets `build_due_at = now + buildErrorRetryMs`, a second
  group still builds, the alarm is armed, and the error is logged.
- `pnpm test` and `pnpm check`.

#### 4.2.4 M4 — captured initialization errors degrade the Durable Object

`blockConcurrencyWhile` keeps running initialization before any event is delivered. Each `blockConcurrencyWhile`
callback wraps its work in a `try`. On an error it stores the error in an `#initError` field, logs it, and
resolves. The object then serves events normally, with the gate below on every storage path.

New error code: `partition_init_failed` in `SHARDING_INTERNAL_CODES` (`src/sharding/errors.ts`), carried by a
`FokosInternalError`.

The gate:

- `FokosShardingRuntime.#guard` throws `partition_init_failed` when `#initError` is set, after the
  destroy-fence check. `fokosDestroy`, `fokosPrepareDestroy`, and `fokosStatus` skip the gate: destroy is the
  recovery path and status is the diagnosis path. `fokosPromotions` stays behind the gate, because it reads
  the repartition tables that a broken schema cannot serve. `fokosStatus` works on KV reads but can still fail
  on a broken schema read; it stays exempt because it is the diagnosis path.
- Each host class keeps its own `#initError` from its own `blockConcurrencyWhile`. The host entry (`#api` in
  `FokosDBPartitionDO`, `fokos.dispatch` callers in the coordinator, `applyLatestImages` in the materialization
  DO, and the RPC methods of `FokosShardingAutoAdminDO`) checks it and throws the same typed error.
- The `FokosShardingRuntime` constructor must not call `this.#config()` eagerly (`src/sharding/runtime.ts`).
  The first read validates. A `fokosRuntimeConfig()` or `fokosConfig()` override that throws then surfaces
  through `#config()` at each use; `#config()` stores the throw into `#initError` before it propagates, so the
  gate covers it.
- `FokosDBPartitionDO` must not call `this.config()` in its constructor (`#ttl.arm` reads
  `ttlSweepDelayMs`). The arm moves to the first use or reads the default; a throwing `fokosConfig()` then
  degrades instead of bricking the constructor.

The alarm path:

- `FokosShardingRuntime.runDueWork` checks `#initError` first: it logs once, calls
  `ensureAlarmAtMost(now + INIT_RETRY_ALARM_MS)` as a best effort, and returns. `INIT_RETRY_ALARM_MS` is a fixed
  `300_000` constant, so the re-arm does not depend on the config that can be the failure.
- Each host `alarm()` checks the host `#initError` first, re-arms at `INIT_RETRY_ALARM_MS`, and returns. The
  runtime pass then does not run host hooks that a broken host init would fail.
- `FokosShardingAutoAdminDO` captures the keeper `runMigrations` error the same way: RPC methods throw
  `partition_init_failed`, and `alarm()` re-arms at `INIT_RETRY_ALARM_MS`.

What the degrade gives each failure case:

- A schema version ahead of the code (downgrade): requests get `partition_init_failed` instead of a reset
  loop; the alarm stays armed so a fixed deploy heals the object without an operator.
- Corrupt storage or a corrupt stored identity: same typed error; `fokosDestroy` can still wipe the object.
- A `fokosConfig()` or `fokosRuntimeConfig()` override that throws: same typed error; a fixed deploy heals it.

Testing:

- A migration that throws deterministically: requests get `partition_init_failed` with routing;
  `fokosStatus`, `fokosDestroy`, and `fokosPrepareDestroy` still answer; the alarm is armed at
  `INIT_RETRY_ALARM_MS`.
- A `fokosConfig()` override that throws: same gate behavior; no constructor rejection.
- `pnpm test` and `pnpm check`.

## 5. Alternative options

### 5.1 Lazy initialization inside each RPC handler

Initialize inside `#guard` on the first request instead of inside `blockConcurrencyWhile`. Rejected:
`blockConcurrencyWhile` is the guarantee that no event — an RPC, an alarm, or a `setTimeout` task — touches
storage before the schema step ends. A lazy gate must cover every entry point, including the TTL timer, the
Bloom-flush throttle, the `wake()` fast path, and the exporter and runner workers. A missed gate reads an
unmigrated schema. The eager-init-plus-degrade of M4 gives the same benefits (typed errors, open destroy path,
alarm control) without the missed-gate risk.

### 5.2 Let `blockConcurrencyWhile` keep rejecting

The status quo. Rejected: a deterministic failure bricks the object — every RPC fails, the alarm dies after 6
retries, and `fokosDestroy` cannot run, so the destroy flow cannot wipe the object.

### 5.3 Count attempts and stop retrying a failed job

A job that fails `n` times stops until an operator clears it. Rejected for now: a hard stop needs the same
operator tooling as a terminal repartition state, and a transient fault that outlasts the count wedges the job
silently. The failure mark of M1 already bounds the rate at `fallbackAlarmMs`; a dead-letter state is open
question 4.3.1's sibling and ships with it if at all.

## 6. Frequently asked questions

**Why keep the failed job retrying at `fallbackAlarmMs` instead of an exponential backoff?**
`fallbackAlarmMs` is the safety interval the runtime already arms before any background work. A deterministic
failure at that cadence costs one bounded pass and one log line each 5 seconds, which matches the cost model of
the other waits (`sourceRetryMaxMs`, `nonRetryableRetryMs`). A job-level backoff table can come later with the
dead-letter state.

**Does the `failed` flag hide a job that has real earlier work?**
While `failed` stands, `own` does not count — that is the point. The flag lasts only until the next step run,
which is at most `fallbackAlarmMs` away, and a success clears it. A job that has more due work after a
successful step is selected again on its durable deadline in the same pass.

**Why does `runDueWork` resolve instead of propagating the pass error to `alarm()`?**
`alarm()` is the only caller that must not throw, and `wake()` already swallows. Resolving in `runDueWork`
makes every caller safe and keeps the rule in one place.

**Can `fokosDestroy` run when the sharding migrations never ran?**
Yes. It calls `deleteAlarm` and `deleteAll`, which need no schema. That is why the recovery path is exempt from
the init gate.

**Why not capture the host override error inside the host `blockConcurrencyWhile` only?**
The runtime constructor validates `fokosRuntimeConfig()` before the `blockConcurrencyWhile` opens, and
`FokosDBPartitionDO` calls `this.config()` in its constructor. Both throws happen outside any `try`, so M4
moves the first validation to the first use and routes it into `#initError` through `#config()`.

## 7. References

- `README.md` — the open item in the feature list: "Check for background alarms runaway errors due to errors".
- [Durable Objects Alarms](https://developers.cloudflare.com/durable-objects/api/alarms/) — the retry contract:
  at-least-once delivery, up to 6 retries, no re-run until the next `setAlarm`.
- `src/sharding/scheduler.ts` — `FokosScheduler`, `#pass`, `#deadlines`, `runDueWork`, `wake`.
- `src/sharding/repartition-flow.ts` — `RepartitionSource`, `RepartitionTarget`, `retryDelay`.
- `src/sharding/sharding-store.ts` — `REPARTITION_HAS_STEP`, `FokosJobsRecord`, `__fokos/jobs`.
- `src/sharding/runtime.ts` — `FokosShardingRuntime`, `#builtinJobs`, `#guard`, `#adminOutboxStep`, `alarm`.
- `src/sharding/topology-keeper.ts` — `FokosTopologyKeeper.runDueBuilds`, `FokosShardingAutoAdminDO.alarm`.
- `src/server/do-partition.ts`, `src/server/do-transaction-coordinator.ts`,
  `src/server/materialization/do-materialization.ts` — the host constructors, jobs, and alarms.
- `src/shared/do-stubs.ts` — the namespace and stub helpers that read `env`.
