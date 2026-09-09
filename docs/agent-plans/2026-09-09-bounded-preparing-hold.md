# RFC — Bound how long a transaction stays in PREPARING

**State:** Draft
**Date:** 2026-09-09
**Author:** Lambros

---

## Table of Contents

1. [Overview and Context](#1-overview-and-context)
2. [Goals and Requirements](#2-goals-and-requirements)
3. [Milestones](#3-milestones)
4. [Proposed Solution](#4-proposed-solution)
5. [Alternative Options](#5-alternative-options)
6. [Frequently Asked Questions](#6-frequently-asked-questions)
7. [References](#7-references)

---

## 1. Overview and Context

### 1.1 The problem

A transaction whose participant never answers stays in `PREPARING` for the life of the coordinator. It
holds the locks of every participant that accepted, and nothing releases them.

The first prepare pass does not have this problem. `drivePrepare` in
`packages/fokosdb/src/server/do-transaction-coordinator.ts` treats a prepare that throws after its
retries as a failure to run. Its `allAccepted` test needs a fulfilled promise and an `accepted`
outcome, so a throw makes the test false and the transaction cancels.

The recovery pass behaves differently. `runPrepareRecovery` re-prepares only the participants whose
`prepare_outcome` is NULL. When one of them throws again, and no participant rejected, two tests
decide nothing:

- `allAccepted` is false, because one row is still NULL.
- `anyRejected` is false, because no row says `rejected`.

The function then returns and writes no transition. The transaction stays in `PREPARING`.

Four consequences follow.

1. **The transaction never expires.** The sweep in `alarm()` selects `completed_at < ?`. A `PREPARING`
   row has a NULL `completed_at`, and `NULL < x` is NULL in SQLite, so the sweep never selects the row.
2. **The alarm repeats every 5 seconds.** `alarm()` selects each row whose state is not `COMMITTED` and
   not `CANCELLED`. While such a row exists, the alarm re-arms after `STALE_THRESHOLD_MS`, which is
   5 seconds. Each pass calls `runPrepareRecovery`, and each pass holds again.
3. **The locks stay held.** Every participant that accepted holds rows in `pending_transactions`. A held
   lock makes every non-transactional write to that key throw. The stale transaction job in
   `PartitionDO.alarm` calls `TransactionCoordinatorDO.recoverTransaction`, which answers `driving` for a
   `PREPARING` transaction. That job acts on `COMMITTED`, `CANCELLED`, and `not_found`. It has no branch
   for `driving`, so it does nothing. Its lock-age guard runs only for `not_found`, so the guard never
   applies here.
4. **The caller has no terminal answer.** A retry with the same token reaches `resumeTransaction`, which
   calls `runPrepareRecovery` and then throws the undecided error again.

The transaction resolves only when the participant becomes reachable. A participant that is permanently
gone leaves the transaction and its locks permanently.

### 1.2 Why the hold exists

The hold is deliberate. A prepare that throws is a retryable failure, and the transaction can still
commit when the participant answers. The alarm drives the recovery with no caller waiting on it, so it
can afford more attempts than the request-driven first pass.

The hold is correct. Its bound is missing.

### 1.3 What the reader must know

- A transaction that has not reached `PREPARED` applied nothing on any participant. `PREPARED` is the
  point of no return, and both writers of `CANCELLING` guard their `UPDATE` on `state = 'PREPARING'`.
- `runCancel` sends a cancel to every participant that has neither committed nor cancelled, including a
  participant whose `prepare_outcome` is NULL. `PartitionDO.txCancel` is a DELETE that does nothing when
  no lock exists, so a cancel is safe for a participant that never prepared.
- `cancelTransactionInStore` reports a NULL `prepare_outcome` as `transient_error`, and marks every
  operation `not_evaluated`.
- `tc_state.created_at` records when the coordinator accepted the transaction.

---

## 2. Goals and Requirements

### 2.1 In scope

1. A transaction that stays in `PREPARING` reaches a terminal state within a bounded time, whether or
   not its participants answer.
2. The locks of a transaction that cannot prepare are released without an operator action.
3. A participant that answers before the bound still commits the transaction. The bound must not make
   the recovery pass more destructive than it is today for transient trouble.

### 2.2 Out of scope

| Item | Reason |
| --- | --- |
| The first prepare pass | `drivePrepare` already cancels when a prepare throws. |
| A `driving` branch in the partition | The coordinator owns the decision. Section 5 gives the reason. |
| Retention of `PREPARING` rows | The bound makes every row terminal, so the sweep reaches it as it does today. |

### 2.3 Requirements that constrain the solution

- The bound must never cancel a transaction that reached `PREPARED`. The decision at `PREPARED` is
  durable, and the transaction commits.
- The bound must be a multiple of `STALE_THRESHOLD_MS`, so the alarm makes more than one recovery pass
  before it ends the hold.
- The bound must not exceed `IDEMPOTENCY_WINDOW_MS`. A hold that outlasts the replay window leaves a
  caller that retries its token starting a second transaction against locked keys.
- The bound must not need a schema change. `tc_state` already records `created_at`.
- The answer a caller receives must be one it already receives today. This RFC adds no reason code and
  no response field.

---

## 3. Milestones

**M0 — The bound.** `runPrepareRecovery` cancels a transaction that is older than `MAX_PREPARING_HOLD_MS`
when participants are still NULL and no participant rejected. One constant, one test in
`runPrepareRecovery`, and the tests of section 4.2.5. It ships on its own.

---

## 4. Proposed Solution

### 4.1 High-level overview

The recovery pass gets a deadline. It holds while the failure can be transient, and it decides when the
hold has gone on too long.

```
age of the transaction        0s ───────────── 25 s ─────────────▶

participant answers           hold, retry every 5s      commit or cancel
participant never answers     hold, retry every 5s      cancel, release the locks
```

Cancelling is safe at every point in this window, because the transaction has not reached `PREPARED` and
applied nothing anywhere. The hold buys the chance to commit. The bound stops the hold from costing the
locks forever.

### 4.2 Technical details

#### 4.2.1 The constant

```ts
const MAX_PREPARING_HOLD_MS = Math.min(5 * STALE_THRESHOLD_MS, IDEMPOTENCY_WINDOW_MS); // 25 seconds
```

It lives in `packages/fokosdb/src/server/do-transaction-coordinator.ts`, beside `STALE_THRESHOLD_MS`.
That file already imports `IDEMPOTENCY_WINDOW_MS`, so the constant needs no new export.

Each term states one rule.

1. **Five stale windows set the hold.** The alarm re-arms one `STALE_THRESHOLD_MS` after each pass, so
   the bound is a count of recovery passes and not a wall-clock guess. It reads in the same units as the
   retry budgets beside it: the first prepare pass retries a participant 3 times, and the recovery pass
   retries an unanswered participant 5 times.
2. **`IDEMPOTENCY_WINDOW_MS` is the ceiling.** The hold must never outlast the window in which a caller
   can replay its token, because a caller that replays after the window starts a second transaction
   against keys the first one still locks. The `Math.min` holds that rule when `STALE_THRESHOLD_MS`
   changes, so a larger stale window cannot push the hold past the replay window.

The hold is 25 seconds with the current `STALE_THRESHOLD_MS` of 5 seconds. A recovery pass itself takes
time, because it retries each unanswered participant with a backoff of up to 2 seconds, so the
transaction gets several passes inside the hold and not exactly five.

#### 4.2.2 The decision

`runPrepareRecovery` keeps its two tests and gains a third. The order is `allAccepted`, then
`anyRejected`, then the bound:

```ts
const heldTooLong = Date.now() - stateRow.created_at > MAX_PREPARING_HOLD_MS;
if (allAccepted) {
    // PREPARED, then commit. Unchanged.
} else if (anyRejected || heldTooLong) {
    this.cancelTransactionInStore(transactionId);
    await this.runCancel(transactionId, idempotencyToken, requestBudgetMs);
}
// Under the bound with participants still NULL: stay in PREPARING, and the alarm retries.
```

`allAccepted` stays first, so a transaction whose last participant answers on the same pass that crosses
the bound commits. The bound decides only a transaction that has no answer.

`stateRow` is already loaded at the top of `runPrepareRecovery`, so the test costs no read.

#### 4.2.3 What the caller receives

`cancelTransactionInStore` merges the stored answers as it does today. A NULL `prepare_outcome` is a
failure to run, so:

- the transaction `reason` is `transient_error`;
- every operation is `not_evaluated`;
- the images of any participant that rejected are deleted.

This is the same answer the caller receives today when a prepare throws on the first pass. The caller
sees no new shape and no new code.

#### 4.2.4 Invariants

| Invariant | Mechanism |
| --- | --- |
| A transaction that reached `PREPARED` never cancels. | `cancelTransactionInStore` guards its `UPDATE` on `state = 'PREPARING'`. A `PREPARED` row does not match, so it writes no row. |
| The bound never cancels a transaction that every participant accepted. | `allAccepted` is tested first, and it wins the branch. |
| A cancelled transaction releases every lock. | `runCancel` reaches every participant that has neither committed nor cancelled. `PartitionDO.txCancel` is safe for one that holds no lock. |
| A bounded transaction reaches the sweep. | The cancel path sets `completed_at`, the column the sweep selects on. |

#### 4.2.5 Testing

1. A transaction whose participant always throws, and that is older than `MAX_PREPARING_HOLD_MS`, cancels
   with `transient_error` and marks every operation `not_evaluated`.
2. The same transaction, younger than `MAX_PREPARING_HOLD_MS`, stays in `PREPARING` and writes no
   transition.
3. A participant that answers `accepted` on a pass that crosses the bound commits the transaction. The
   bound does not cancel it.
4. A transaction in `PREPARED` that crosses the bound stays `PREPARED`.
5. The locks of a participant that accepted are released after the bound cancels the transaction.
6. The sweep deletes the cancelled transaction one `IDEMPOTENCY_WINDOW_MS` after the bound cancelled it.

#### 4.2.6 Deployment and rollback

The change adds one constant and one test in one branch. It needs no schema change and no migration. A
rollback restores the unbounded hold, which is the behaviour before this RFC.

---

## 5. Alternative Options

**Cancel on the first recovery pass that finds a NULL participant.** This ends the hold at once and
needs no constant. It was rejected because one transient failure during recovery then cancels a
transaction that would commit, which removes the reason the recovery pass exists. It also makes the
recovery pass more destructive than the first pass, which retries three times before it decides.

**Add a `driving` branch to the stale transaction job of `PartitionDO`, and release the lock there.**
The partition cannot know the outcome of the transaction. A lock released while the coordinator can
still commit breaks atomicity: the coordinator commits, and another writer has already changed the item.
The decision belongs to the coordinator alone.

**Sweep `PREPARING` rows by `created_at`.** The sweep deletes the row instead of deciding the
transaction. The participants then read `not_found` from `recoverTransaction`, and their lock-age guard
releases the locks one `IDEMPOTENCY_WINDOW_MS` later. It was rejected for two reasons. The record a
replay needs is gone, so a caller that retries its token starts a second transaction against keys the
first one still locks. The release also depends on each partition's own guard instead of one decision by
the coordinator.

**Bound the hold with an attempt count instead of an age.** The coordinator would count the recovery
passes for each transaction. That needs a new column and a write on every pass. The age needs neither,
because `tc_state.created_at` already records it.

---

## 6. Frequently Asked Questions

**Is it safe to cancel a transaction whose participants have not all answered?**
Yes. The transaction has not reached `PREPARED`, so no participant applied anything. `runCancel` reaches
every participant that has neither committed nor cancelled, and `PartitionDO.txCancel` is a DELETE that
does nothing when the participant holds no lock. A participant that never prepared is unaffected.

**Can a participant prepare after the coordinator cancels?**
Yes, and it is the case `runCancel` already handles today. A prepare RPC that lands after its cancel
leaves a lock that the stale transaction job of the partition then resolves: `recoverTransaction` answers
`CANCELLED`, and the job sends the cancel. This RFC adds no new case.

**Why does the first prepare pass cancel after 3 retries, and the recovery pass hold for 25 seconds?**
A caller waits on the first pass, so it must answer quickly, and a cancelled transaction costs that
caller one retry. No caller waits on the recovery pass, so it can spend more attempts on a failure that
can still clear. The transaction it drives already holds locks, so the hold stays short: 25 seconds buys
several more passes, and it does not make a held lock an operational problem.

**Does the bound change what an operator sees?**
A transaction that would previously spin in `PREPARING` now appears as `CANCELLED` with
`transient_error`. The repeated alarm for that transaction stops.

---

## 7. References

- `docs/agent-plans/2026-08-30-bounded-stateful-transaction-coordination.md`
- `docs/agent-plans/2026-09-08-return-values-on-condition-check-failure.md`
- `packages/fokosdb/src/server/do-transaction-coordinator.ts`
- `packages/fokosdb/src/server/do-partition.ts`
