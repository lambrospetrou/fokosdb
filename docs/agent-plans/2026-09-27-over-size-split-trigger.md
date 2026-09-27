# RFC — Queue the repartition of an over-size partition from every path that finds it

**State:** Draft
**Date:** 2026-09-27
**Author:** Lambros Petrou

This RFC replaces `docs/agent-plans/2026-09-20-over-size-partition-split-deadlock.md`. That document describes the
code before the sharding runtime refactor, and its functions no longer exist.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
  - [1.1 How a split is queued today](#11-how-a-split-is-queued-today)
  - [1.2 Every event that changes the size or the cap](#12-every-event-that-changes-the-size-or-the-cap)
  - [1.3 A failure, step by step](#13-a-failure-step-by-step)
  - [1.4 Evidence](#14-evidence)
  - [1.5 What the failure costs](#15-what-the-failure-costs)
- [2. Goals and requirements](#2-goals-and-requirements)
  - [2.1 In scope](#21-in-scope)
  - [2.2 Out of scope](#22-out-of-scope)
  - [2.3 Requirements](#23-requirements)
  - [2.4 The floor](#24-the-floor)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

A partition has a size cap. Two decisions read it, and the host hooks in `PartitionDO` make both.

| Decision                   | Condition                        | Hook                     |
| -------------------------- | -------------------------------- | ------------------------ |
| Queue a split              | `databaseSize > maxSizeMb`       | `hooks().evaluateSplit`  |
| Refuse a `write`-tag request | `databaseSize > maxSizeMb * 1.1` | `hooks().admit`          |

The cap is `hashSplitConditions.maxSizeMb` for a hash partition and `rangeSplitConditions.maxSizeMb` for a range
partition. The 10% margin is intentional. It stops the decision from flapping at the cap, and it lets the write that
crosses the cap complete.

`TransactionCoordinatorDO` has the same two hooks. Its cap is the smaller of `hashSplitConditions.maxSizeMb` and
`MAX_TC_DATABASE_BYTES / 1.1`.

### 1.1 How a split is queued today

A split is queued in one way only:

1. A local handler of `apiPutItem`, `txCommit` or `txExecuteSingleShot` applies a write.
2. The handler calls `signalGrowth`, which signals `promotionCandidates` and then `evaluateSplit`.
3. `FokosShardingRuntime.#applySignals` runs `#evaluateSplit` after the handler returns.

`#admit` in `runtime.ts` throws the refusal before any handler runs, so a refused request applies no signal. The
coordinator calls `requestSplitEvaluation()` in its begin handler, which also runs only after admission.

So a split is checked only when a write succeeds. Above 1.1 times the cap no write succeeds. The partition never
checks again, and it refuses every write for ever.

### 1.2 Every event that changes the size or the cap

| Event                                                           | Where it runs                            | Checked |
| --------------------------------------------------------------- | ---------------------------------------- | ------- |
| `apiPutItem`, `txCommit` or `txExecuteSingleShot` applies       | local handler, then `signalGrowth`       | Yes     |
| A `write`-tag request is refused for size, `txPrepare` included | `#admit`, six call sites in `runtime.ts` | No      |
| The caller sends a lower cap                                    | `#ensureIdentity` stores the new policy  | No      |
| A split child finishes its import                               | `target_import` job, in a pass           | No      |
| A promotion delivers a key into a range root                    | `target_import` job, in a pass           | No      |
| A promotion finishes, so the hash split behind it can queue     | `source_repartition`, `source_cleanup`   | No      |
| A coordinator refuses a new transaction for size                | coordinator `admit`, through `#admit`    | No      |

The six call sites of `#admit` are: `#dispatchPoint`, the local fallback of `#forwardPoint`, the two branches of
`#dispatchGroup`, `#dispatchSingleOwner`, and `#dispatchRange`.

Every "No" row passes through one of two places: `#admit`, or the end of a scheduler pass.

### 1.3 A failure, step by step

1. A hash leaf holds 3 MB. Its cap is 10 MB.
2. The operator lowers `hashSplitConditions.maxSizeMb` to 1. The next request stores the new policy.
3. A put arrives. `admit` sees 3 MB > 1.1 MB and refuses it.
4. No handler runs, so no signal applies. No split is queued.
5. Every later put, and every `txPrepare`, is refused the same way.

### 1.4 Evidence

Both of these were measured on 2026-09-20, before the sharding runtime refactor. The current code has the same
trigger, as section 1.1 shows.

- **A lower cap.** A partition was filled under a generous cap, then received requests that carried a lower one. All
  30 writes were refused, and `splitStatus` stayed undefined.
- **A promotion that overshoots.** A key is promoted at `hashSplitConditions.maxSizeMb * RANGE_PROMOTION_FRACTION`
  and lands in a range root that `rangeSplitConditions.maxSizeMb` caps. A root received 844 KB against a cap of
  0.2 MB and refused every write for the 35 s of the test.

A third path is possible but is not measured: a split child that is born above 1.1 times its own cap, because the
key distribution or the row sizes are skewed.

`test/property-based/query-harness.ts` seeds its range tree in two phases only because of this defect. The comment
above `TREE_HASH_SPLIT_MAX_SIZE_MB` explains why.

### 1.5 What the failure costs

- **Only a leaf is affected.** `#admit` runs only when the resolution is local, so a router forwards before it reads
  a size.
- **Only `write`-tag requests are affected.** Reads, queries, deletes, `txCommit` and `txCancel` continue.
- **The error looks temporary.** The partition answers `partition_over_size`, a 503-class `FokosUnavailableError`.
  A correct client retries for ever and never recovers.
- **The coordinator is affected the same way.** It refuses every new transaction with `coordinator_over_size`.
- **One manual recovery exists.** Raise the cap above the current size, let one write apply, and that write queues
  the split. The cap can go back down after the split completes.

## 2. Goals and requirements

Restore this invariant: **when a leaf is above its cap and a repartition can make it smaller, a repartition is
queued.** A repartition here is a hash split, a range split, or a key promotion.

### 2.1 In scope

- A refused `write`-tag request must cause a repartition decision within one scheduler pass.
- A stored policy change must cause a repartition decision within one scheduler pass.
- The end of every scheduler pass must re-check the decision. This covers the end of an import, the end of a
  promotion, and the end of a cleanup.
- The fix must live in the runtime, so `PartitionDO` and `TransactionCoordinatorDO` both get it.
- For a hash leaf, the decision must prefer a promotion over a hash split. A split row blocks every later promotion
  on its source, so the wrong order wedges a large key for ever.
- The decision must not queue a repartition that cannot make the partition smaller (section 2.4).
- Regression tests for each row of the table in section 1.2.

### 2.2 Out of scope

- The 1.1 margin, the admission tags, and the backpressure contract. A refused write stays refused.
- The error that the caller receives. It stays the same 503-class error.
- Validation between `hashSplitConditions.maxSizeMb` and `rangeSplitConditions.maxSizeMb`. It closes one path of
  three, and it is a separate change.
- A `debugForceSplit` RPC.
- The range planner. When `computeRangeBoundaries` returns `null`, the source still defers with a backoff of up to
  `SOURCE_RETRY_MAX_MS`. After this change that case needs a race: a delete between the queue and the plan.
- A size measure that falls at once after a delete. See section 4.2.9.
- A single path for the write-time promotion. `signalGrowth` keeps its `promotionCandidates` signal, because it
  carries the exact size of each written key.

### 2.3 Requirements

- **No timer on an idle partition.** A partition with no due work must keep no alarm, as it does today.
- **No busy loop.** When the decision is "nothing to do", the check must not schedule another pass.
- **No cost on the common path.** A partition under its cap must pay one read of `sql.databaseSize` per deadline
  read, and nothing more.
- **No new RPC, and no new host hook.** The change widens the result type of `evaluateSplit` only.
- **Never on incomplete data.** A target in `awaiting_data` or `importing` holds only part of its slice. It must not
  decide.

### 2.4 The floor

A repartition cannot make a partition smaller than one item. The floor for each kind is:

- **Range leaf.** `computeRangeSplitBoundaries` needs at least `rangeSplitN` items, because each child needs one.
  The default `rangeSplitN` is 4. A range leaf with fewer items cannot split.
- **Hash leaf.** A hash split cannot divide one hash key, but a promotion can move that key into a range tree, and
  the range tree splits it by sort key. `RANGE_PROMOTION_FRACTION` is 0.25, so a single key above 1.1 times the cap
  is always above the promotion threshold. A hash leaf reaches its floor only when its size is not item data, for
  example lock rows or free pages.

A real deployment sets a cap far above the size of one item, so the floor belongs to tests. It matters for one
reason: the decision must not queue a repartition below the floor. A range split below the floor stays `queued` for
ever and wakes the alarm every `SOURCE_RETRY_MAX_MS` (5 minutes).

## 3. Milestones

Each milestone must leave the test suite green. Review each one before the next starts.

1. **The host decision.** `evaluateSplit` of `PartitionDO` returns a promotion, a split, or `false`, and respects
   the floor. Add the `est_bytes` index. The write path uses the new decision at once.
2. **The `split_check` job.** A built-in runtime job whose deadline is "a repartition is due now". It covers the end
   of every pass.
3. **The wakes.** `#admit` wakes the scheduler before it throws, and `#ensureIdentity` wakes it after it stores a
   changed policy.
4. **The tests.** One regression test per row of section 1.2. Seed the fixture in
   `test/property-based/query-harness.ts` in one phase, and remove its two-phase comment. Record the run time of
   `query-items-split.test.ts` and `query-items-active-split.test.ts` before and after, for the milestone review.

## 4. Proposed solution

### 4.1 High-level overview

Today the partition checks for a split when something happens: a write applies. The fix makes the check depend on
the state: "this leaf is over its cap, and a repartition can help". The scheduler already reads a deadline from each
job at the start and at the end of every pass. A new runtime job, `split_check`, answers "due now" exactly while that
state holds and a repartition can be queued. At all other times it answers "no deadline", so it costs nothing.

Two things start a pass today, and two are added:

- The alarm and the wake timer, for background work. The end of that pass now also checks the split.
- A refused write. It now calls `wake()`, which starts a pass in 50 ms (`DEFAULT_FAST_PATH_DELAY_MS`).
- A stored policy change. It also calls `wake()`.
- The existing write-time signal stays as the fast path.

`wake()` is one shared `setTimeout` per instance. It writes nothing. The pass that it starts touches the alarm only
when a job is due.

The host decides what to do. For a hash leaf it chooses a promotion first, then a split. For a range leaf it
chooses a split only when the leaf has enough items. When the host answers `false`, the deadline is `null`, so no
pass repeats the check.

```
  refused write ──wake()──┐
  policy change ──wake()──┤
  alarm / wake timer ─────┤
                          ▼
                 ┌──────────────────┐   start and end of the pass: read every job deadline
                 │  scheduler pass  │──────────────────────────────────────────────────┐
                 └──────────────────┘                                                  │
                          │ runs due jobs: target_import, target_ack, source_*, ...    │
                          ▼                                                            ▼
                 split_check.deadline()  ── #repartitionDecision() ── hooks.evaluateSplit()
                   │           │                     │                      │
                   │ null      │ now                 └─ canQueue()          ├─ false          → nothing
                   ▼           ▼                                            ├─ { promote: K } → promotion
               no alarm    split_check.runStep() ── #evaluateSplit()        └─ { data }       → hash or range split
```

### 4.2 Technical details

#### 4.2.1 The `evaluateSplit` result type

The result type in `runtime-types.ts` becomes:

```ts
evaluateSplit(input: { identity: FokosPartitionIdentity; policy: TPolicy }):
	| false
	| { data?: unknown }
	| { promote: KeyBytes; data?: unknown };
```

The change is additive. The existing hosts return `false` or `{ data }` and need no change: the coordinator, and
the test hosts in `test/sharding/` and `test/sharding-prototype/`.

The hook stays synchronous. Its documentation changes from "called after a local success" to "called when the
runtime decides whether a repartition is due, which includes every scheduler pass".

#### 4.2.2 The decision of `PartitionDO`

`hooks().evaluateSplit` runs these steps in order, and stops at the first answer:

1. Read the cap for the identity kind. When `sql.databaseSize` is at or below it, return `false`. This is one
   property read, and it is the whole cost for a partition under its cap.
2. **Hash leaf, promotion.** Read the largest key:
   `SELECT hk, est_bytes FROM key_size_estimates ORDER BY est_bytes DESC LIMIT 1`. When `est_bytes` is at or above
   `hashSplitConditions.maxSizeMb * RANGE_PROMOTION_FRACTION * 1024 * 1024`, return `{ promote: hk }`.
3. **Hash leaf, split.** When the leaf holds two or more hash keys, return `{}`. The query must stop at two rows:
   `SELECT COUNT(*) FROM (SELECT DISTINCT hk FROM items LIMIT 2)`.
4. **Range leaf, split.** When the leaf holds `rangeSplitN` or more items, return `{}`. `rangeSplitN` comes from
   `this.fokos.routeContext().rangeConfig`. The query must stop at `rangeSplitN` rows:
   `SELECT COUNT(*) FROM (SELECT 1 FROM items LIMIT ?)`. This is the same count that
   `computeRangeSplitBoundaries` checks.
5. Otherwise the leaf is at its floor (section 2.4). Return `false`. Log the floor once per instance: keep an
   in-memory flag, set it when this step logs, and clear it when the hook returns anything else. The runtime reads
   the deadline three times per pass, so a log per call floods the logs.

Step 2 returns the largest key even when that key already has a route override. The runtime then refuses it in
`canQueue` (section 4.2.3), and no repartition is due until the promotion of that key finishes. This keeps the host
out of the `fokos_` tables. The cleanup of a promotion calls `deleteKeySizeEstimate`, so the key leaves step 2 when
its promotion is `cleaned`. The next largest key is then considered in the next pass.

`signalGrowth` keeps its order: `promotionCandidates` first, then `evaluateSplit`. On the write path, step 2 then
finds the key that the signal already queued, `canQueue` refuses it, and the result is the same as today.

#### 4.2.3 The decision in the runtime

A new synchronous method holds the first half of today's `#evaluateSplit`:

```ts
/** The repartition that `#evaluateSplit` would queue now, or null. Synchronous; writes nothing. */
#repartitionDecision():
	| { kind: "hash_split" | "range_split"; data?: unknown }
	| { kind: "key_promotion"; hashKey: KeyBytes; data?: unknown }
	| null {
	if (this.#source.routerRole()) {
		return null;
	}
	const identity = this.identity();
	const decision = this.#hooks.evaluateSplit({ identity, policy: this.policy() });
	if (decision === false) {
		return null;
	}
	if ("promote" in decision) {
		const request = { kind: "key_promotion" as const, hashKey: decision.promote };
		return this.#source.canQueue(request) ? { ...request, data: decision.data } : null;
	}
	const kind = identity.kind === "hash" ? "hash_split" : "range_split";
	return this.#source.canQueue({ kind }) ? { kind, data: decision.data } : null;
}
```

`#evaluateSplit` calls `#repartitionDecision()`. For a split it keeps today's steps: `ensureAlarmAtMost`, then
`queue`, then the log line and `wake()`. For a promotion it calls `#requestPromotion(hashKey, data)`, which already
arms the alarm before its queue transaction.

`queue` repeats every check of `canQueue` inside its transaction. So a request that passes `#repartitionDecision()`
and then loses a race is refused there, and it writes nothing.

#### 4.2.4 The `split_check` job

`split_check` is the fifth entry of `BUILTIN_JOBS`, after `source_cleanup`. It runs last in a pass, so it sees the
state that the other built-in jobs left.

```ts
split_check: {
	name: "split_check",
	canRun: () =>
		this.#identity !== undefined &&
		!this.#store.isDestroying() &&
		!this.#target.isImporting() &&
		!this.#source.routerRole(),
	deadline: () => {
		if (Date.now() < this.#splitCheckRetryAt) {
			return this.#splitCheckRetryAt;
		}
		try {
			return this.#repartitionDecision() ? Date.now() : null;
		} catch (error) {
			// Logged, and retried after the backoff.
			this.#splitCheckRetryAt = Date.now() + this.#fallbackAlarmMs;
			return this.#splitCheckRetryAt;
		}
	},
	runStep: async () => {
		try {
			await this.#evaluateSplit();
		} catch (error) {
			// Logged. The scheduler would retry at `now` otherwise, because `deadline()` still answers now.
			this.#splitCheckRetryAt = Date.now() + this.#fallbackAlarmMs;
		}
		return { nextRunAt: null };
	},
},
```

The rules that the sketch holds:

- **`canRun` excludes an incomplete target.** `#evaluateSplit` does not check `isImporting()` today. That is safe
  only because the import gate refuses writes before `#admit`. The job must check it itself.
- **The deadline is `null` when nothing can be queued.** This is the only defense against a busy loop. A deadline
  of "now" that the step cannot clear makes the scheduler wake every `DEFAULT_FAST_PATH_DELAY_MS`.
- **The backoff lives in memory.** `#deadlines` in `scheduler.ts` takes the minimum of the scheduled run and the
  own deadline. An own deadline of "now" overrides the retry delay that the scheduler writes after a failed step, so
  the job must keep its own `#splitCheckRetryAt`. An eviction clears it, and that is correct: the next pass tries
  again once.
- **`deadline()` must not throw.** `#deadlines` does not catch, so a throw from a hook stops the pass. The sketch
  catches and backs off.
- **The target in `imported` can decide.** It holds its complete slice. Its split does not conflict with
  `sendAck` or with `servePage`, which accept a source at `cutover`.

#### 4.2.5 The wake points

Two calls to `this.#scheduler.wake()` start a check after the request:

1. **Refusal.** In `#admit`, before `throw decision.reject`. The same line covers all six call sites and the
   coordinator, because both hosts reach their `admit` hook only through `#admit`.
2. **Policy change.** In the commit step that `#ensureIdentity` returns when the stored policy changed, after
   `#setIdentity`. The call runs after the transaction commits, so a rollback starts no pass.

The properties of `wake()`:

- It is synchronous and writes nothing. It sets one timer, and a pending timer absorbs every later call. A storm
  of refused writes starts one pass.
- The pass reads the `split_check` deadline and runs `#evaluateSplit` when it is due. When no job is due, the pass
  writes nothing.
- When `split_check` is due, the pass arms the fallback alarm before the step, as for every job. A crash inside the
  step then leaves an alarm that runs the check again.
- The caller receives its answer at once. It does not wait for the pass.

`ctx.waitUntil` is not an option here. The Durable Object state API page says it has no effect in a Durable
Object. The existing fast path already runs a pass from a timer after the request.

#### 4.2.6 Coverage

| Event (section 1.2)                                         | Mechanism after this change                         |
| ----------------------------------------------------------- | --------------------------------------------------- |
| `apiPutItem`, `txCommit`, `txExecuteSingleShot` applies     | `signalGrowth`, unchanged                           |
| A `write`-tag request is refused, `txPrepare` included      | `wake()` in `#admit`, then the pass                 |
| The caller sends a lower cap                                | `wake()` in `#ensureIdentity`, then the pass        |
| A split child finishes its import                           | End of the `target_import` pass                     |
| A promotion delivers a key into a range root                | End of the `target_import` pass on the range root   |
| A promotion finishes, so the hash split behind it can queue | End of the `source_repartition` or cleanup pass     |
| A coordinator refuses a new transaction                     | `wake()` in `#admit`, then the pass                 |

The end of a pass re-reads every deadline, and it calls `wake()` when one is due (`FokosScheduler.#pass`). No
extra call is needed at the end of an import.

#### 4.2.7 Failure and recovery

- **Crash after `imported` and before the check.** The import pass that wrote `imported` has not re-read the
  deadlines. The alarm that the pass armed first (`fallbackAlarmMs`, 5 s) runs a new pass, and that pass checks.
- **Eviction with no alarm.** A leaf over its cap with no due work keeps no alarm. The next refused write wakes it.
  A leaf that nobody writes to needs nothing, because the only symptom is a refused write.
- **`ensureAlarmAtMost` or `queue` fails.** `runStep` logs, and `#splitCheckRetryAt` holds the next try for
  `fallbackAlarmMs`.
- **Two passes at once.** The scheduler runs one pass at a time. A request that runs `#evaluateSplit` from a
  signal during a pass is arbitrated by `queue`, which runs in one synchronous transaction.
- **A floor leaf.** The decision is `false`, the deadline is `null`, and no alarm stays armed. Writes stay refused.
  An operator must raise the cap or delete data.

#### 4.2.8 Cost

- **Partition under its cap.** Three reads of `sql.databaseSize` per pass: two at the start, one at the end. No
  query.
- **Refused write, or policy change.** One `setTimeout`, or none when a timer is pending.
- **Over-size leaf.** Per pass: one indexed seek on `est_bytes` (hash), and one query that stops at 2 or at
  `rangeSplitN` rows. This runs until a repartition is queued, then `canQueue` answers from one partial-index seek.
- **Write path.** `evaluateSplit` now runs the step 2 seek on a hash leaf above its cap. Today it reads the size
  only.

#### 4.2.9 The size after a delete

`sql.databaseSize` falls only when complete SQLite pages become free and the storage collects them. So a delete
can leave the size above the cap for a time. The design handles both effects:

- **A leaf that clients delete from.** It refuses writes until its size falls under 1.1 times the cap. When its
  remaining items are below the floor, the decision is `false`, so no pass repeats.
- **A promotion source after its cleanup.** It can stay above its cap. With two or more hash keys left, the check
  queues a hash split of the remaining keys. A write on the same source does the same today.

A fall in size needs no trigger, because a partition under its cap needs no repartition.

#### 4.2.10 Schema change

Add an index to the migration that creates `key_size_estimates` in `partition-store.ts`. Edit the migration in
place, because the project is before its first release:

```sql
CREATE INDEX IF NOT EXISTS key_size_estimates_by_bytes ON key_size_estimates (est_bytes);
```

The decision uses an index and not a scan:

- **With the index**, step 2 of section 4.2.2 is one seek.
- **Without the index**, step 2 reads up to one row per hash key on every pass of an over-size leaf.

The cost is one index write for each update of `key_size_estimates`, and every item upsert makes one such update.

#### 4.2.11 Deployment and rollback

The change adds no RPC and no durable record. `split_check` keeps no row in `__fokos/jobs`, because its
`runStep` returns `nextRunAt: null`. A rollback removes the job and the wake, and restores today's behavior. The
index can stay after a rollback.

#### 4.2.12 Testing

Each test uses real timers and the scheduled-alarm test APIs. None mocks a global clock.

1. **Refusal path.** Fill a hash leaf, lower the cap, and send one put. A hash split runs, and a later put succeeds.
   Suite: a new over-size file in `test/partition-do/`.
2. **Transaction-only workload.** The same as test 1, with `txPrepare` only. Suite: the same file.
3. **Promotion first.** Lower the cap on a leaf whose data is one hash key. A promotion is queued, and no split row
   exists. Suite: the same file.
4. **Import path.** Promote a key into a small range cap. The range root splits, and no write is sent. Suite:
   `test/repartition/`.
5. **Floor, no busy loop.** A range leaf holds 3 items above 1.1 times its cap. After one pass there is no split row,
   and `getAlarm()` is `null`. Suite: `test/partition-do/`.
6. **Coordinator.** Lower the cap of a coordinator. A new transaction is refused once, then the coordinator splits.
   Suite: `test/transactions/tx-coordinator-split.test.ts`.
7. **Failure path.** A hook throws in `deadline()`. The pass continues, and the next try waits `fallbackAlarmMs`.
   Suite: `test/sharding/`.

The property fixture in `test/property-based/query-harness.ts` then seeds in one phase. Both
`query-items-split.test.ts` and `query-items-active-split.test.ts` use it. The active suite keeps writing to the
tree, so it can meet a range root that refuses writes until its first split completes. Milestone 4 records the run
time of both suites before and after.

## 5. Alternative options

- **A periodic job every 0.5 to 1 s.** Each partition runs a pass on a fixed timer and checks the size. It is
  rejected for three reasons:
  - Cost. At 1 s, one partition gets 86,400 alarm invocations per day, about 2.6 million per month. Alarm
    invocations are billed as requests at $0.15 per million, so about $0.39 per partition per month, or about
    $3,900 per month for 10,000 partitions. Each `setAlarm()` is also billed as one row written. Source: the
    Durable Objects pricing page.
  - It removes the rule in `FokosScheduler.#pass` that an idle partition keeps no alarm.
  - It fixes neither hard part. It still needs the floor and the promotion-first order, and without them it queues
    useless repartitions faster. It is also up to 1 s late, where `wake()` fires after 50 ms.
- **Check the size on the alarm only.** The pass deletes the alarm when no durable work remains, so an idle wedged
  leaf has no alarm to run the check. The `split_check` job keeps the part of this option that works: every pass
  checks.
- **Accept the write while no split is queued.** Refuse a write only while a split runs. This removes the wedge by
  construction, and it gives up the guarantee that a partition stops growing at 1.1 times its cap. It changes the
  backpressure contract, so it needs a separate decision.
- **Validate the caps against each other.** Reject a context whose range cap is far below its hash cap. It is a
  useful guard, and it closes one path of three. The wedge stays.
- **Put the refusal trigger in the `admit` hook of `PartitionDO`.** The host calls `requestSplitEvaluation()` before
  it returns the refusal. It fixes the partition only, and the coordinator stays wedged. It also covers no import
  path.

## 6. Frequently asked questions

**Does a caller see a different error?** No. The refusal still throws the same 503-class error, and the caller
still retries.

**How many refused writes does recovery need?** One. It wakes the scheduler, and the pass 50 ms later queues the
repartition. The refused caller retries and succeeds once the repartition completes.

**Why keep the write-time signal?** It runs in the same request, so a normal split starts without a timer. It also
carries the exact size of each written key for promotion.

**What happens to a leaf that is over its cap and that nobody writes to?** Nothing, and nothing is needed. The only
symptom is a refused write, and the first one wakes the check.

**Why a promotion before a hash split?** `queue` refuses every promotion on a source that has a split row in any
state. When a hash split is queued first, the large key moves to a child, the child reaches one hash key, and no
repartition can help it again.

**Why does the range floor use `rangeSplitN` and not 2?** The planner needs one item per child. With fewer items
`computeRangeBoundaries` returns `null`, and the split stays `queued` for ever.

**Why keep the 1.1 margin?** It stops the split decision from flapping, and it lets the write that crossed the cap
finish. The defect is the missing trigger, not the margin.

**How does an operator recover a wedged leaf before this change?** Raise the cap above the current size of the
leaf. One write then applies and queues the split. The cap can go back down after the split completes.

## 7. References

- `docs/agent-plans/2026-09-20-over-size-partition-split-deadlock.md` — the superseded RFC for the code before the
  sharding runtime refactor.
- `docs/agent-plans/2026-09-17-unified-repartition-flow.md` — `queue`, `canQueue`, and the arbitration between a
  split and a promotion.
- `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md` — the runtime, its hooks, its signals, and its scheduler.
- `packages/fokosdb/src/server/do-partition.ts` — `hooks()` (`evaluateSplit`, `admit`), `signalGrowth`,
  `promotionCandidates`.
- `packages/fokosdb/src/server/do-transaction-coordinator.ts` — the coordinator `evaluateSplit` and `admit`.
- `packages/fokosdb/src/sharding/runtime.ts` — `#admit`, `#applySignals`, `#evaluateSplit`, `#requestPromotion`,
  `#ensureIdentity`, `#builtinJobs`, `BUILTIN_JOBS`.
- `packages/fokosdb/src/sharding/scheduler.ts` — `FokosScheduler.wake`, `#pass`, `#deadlines`.
- `packages/fokosdb/src/sharding/runtime-types.ts` — `FokosShardingHooks.evaluateSplit`, `FokosJob`.
- `packages/fokosdb/src/sharding/repartition-flow.ts` — `RepartitionSource.canQueue`, `queue`, the range planner.
- `packages/fokosdb/src/shared/partition/partition-store.ts` — `computeRangeSplitBoundaries`, the
  `key_size_estimates` migration.
- `packages/fokosdb/test/property-based/query-harness.ts` — the two-phase seed.
- [Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)
- [Durable Objects SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Durable Object state API](https://developers.cloudflare.com/durable-objects/api/state/) — `ctx.waitUntil`.
