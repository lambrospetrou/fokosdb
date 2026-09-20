# RFC — Recovery of an over-size partition that cannot queue its split

**State:** Proposed
**Date:** 2026-09-20
**Author:** Lambros Petrou

## Table of contents

- [1. Overview and context](#1-overview-and-context)
  - [1.1 Evidence](#11-evidence)
  - [1.2 How a partition reaches this state](#12-how-a-partition-reaches-this-state)
  - [1.3 What the failure costs](#13-what-the-failure-costs)
- [2. Goals and requirements](#2-goals-and-requirements)
  - [2.1 In scope](#21-in-scope)
  - [2.2 Out of scope](#22-out-of-scope)
  - [2.3 The irreducible floor](#23-the-irreducible-floor)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

Two thresholds govern the size of one partition, and different code reads each one.

| Decision            | Threshold                       | Function                    |
| ------------------- | ------------------------------- | --------------------------- |
| Queue a split       | `databaseSize > maxSizeMb`      | `shouldSplit()`             |
| Refuse a write      | `databaseSize > maxSizeMb * 1.1`| `shouldAllow(…, "write")`   |

The 10% margin is intentional. It stops the decision from flapping at the threshold, and it lets the
write that triggers the split complete. The margin is correct only while the size crosses it slowly,
through local writes.

The defect is the trigger. `shouldSplit()` has one caller, `checkSplits()`. `checkSplits()` has three
call sites, and each one runs AFTER a write applied: the `putItem` and `deleteItem` path, `commit`,
and `executeSingleShot`. The repartition flow reads no size at all. The alarm runs the background
pass, and that pass only RUNS a split that is already queued.

Therefore only a write that applies can queue a split. Above 1.1 times the cap no write applies. The
partition cannot recover, and it stays in that state for ever.

The split trigger is an edge on a local write. The size of a partition is a level that other
operations also change. The two do not agree.

### 1.1 Evidence

Both of these were measured on 2026-09-20.

- An operator lowers a cap. The thresholds travel in `PartitionContext` with every request, so this
  is an ordinary configuration change. A partition was filled under a generous cap, then received
  requests that carried a lower one. All 30 writes were refused, and `splitStatus` stayed undefined.
- A promotion overshoots. A key is promoted at `hashSplitConditions.maxSizeMb * RANGE_PROMOTION_FRACTION`
  and lands in a range root that `rangeSplitConditions.maxSizeMb` caps. A root received 844 KB against
  a cap of 0.2 MB and refused every write for the 35 s of the test.

A third path is possible but is not measured: a split child that is born above 1.1 times its own cap,
because the key distribution or the row sizes are skewed.

### 1.2 How a partition reaches this state

The size of a partition changes in three ways, and only the first one can queue a split today.

- A local write applies.
- A repartition import delivers rows, through a promotion or a split.
- The cap itself changes, because the operator sends a different `PartitionContext`.

### 1.3 What the failure costs

- Only a leaf is affected. Both `shouldAllow` implementations return `"forward"` for a router before
  they read the size, so a partition that has split is never refused.
- Only writes are affected. The check reads `intent === "write"`, so reads, queries and transaction
  reads continue to work.
- The error looks temporary. It is a 503-class `FokosUnavailableError`, so a correct client retries
  for ever and never recovers.
- One recovery exists today, and it is not documented: raise the cap above the current size, let one
  write apply, and that write queues the split.

## 2. Goals and requirements

Restore this invariant: **if a partition is above its cap, a split is queued.**

### 2.1 In scope

- Queue the split when a write is refused because the partition is over size.
- Queue the split when a repartition import leaves the target above its cap.
- Refuse to queue a split that cannot reduce the size, so the two changes above cannot create
  partitions without end.
- Regression tests for both paths.

### 2.2 Out of scope

- The 1.1 margin, the intents, and the backpressure contract. A refused write stays refused.
- The error that the caller receives. It stays the same 503-class error.
- Validation between `hashSplitConditions.maxSizeMb` and `rangeSplitConditions.maxSizeMb`. It guards
  one path only, and it is a separate change.
- A `debugForceSplit` RPC.

### 2.3 The irreducible floor

A split cannot make a partition smaller than the data that one item needs. A range tree cannot divide
one sort key, and a hash tree cannot divide one hash key. If the cap is small enough that a single
item is above 1.1 times the cap, no split can recover the partition, and every write stays refused.

A real deployment sets a cap far above the size of one item, so this shape belongs to tests. This
document does not try to serve it. It matters for one reason only: **the solution must not queue a
split that cannot reduce the size.** Without that guard, each refused write would add one more level
of children, each child would hold the same single item, and the tree would grow without end.

## 3. Milestones

Each milestone must leave the test suite green. Review each one before the next starts.

1. **The guard.** A partition answers whether a split can divide what it holds. Queue no split when
   the answer is no.
2. **The refusal path.** Queue the split when `shouldAllow` refuses a write for size, then throw as
   before.
3. **The import path.** Queue the split when a repartition target finishes its import above its cap.
4. **The tests.** One regression test per path, and the removal of the two-phase seed described in
   section 7.

## 4. Proposed solution

### 4.1 High-level overview

Keep one queueing function, `checkSplits()`, and call it at every point where the code discovers that
a partition is over its cap. Today there is one such point, and it is the only point a refused write
cannot reach. Two more are added: the refusal itself, and the end of an import.

The refusal is the better of the two. It is the exact moment at which the partition knows that it is
over size and that it cannot heal itself. The import is the faster of the two, because a fresh
partition splits at once instead of waiting for a caller to be refused first.

### 4.2 Technical details

#### 4.2.1 The guard against a split that cannot divide

`checkSplits()` must not queue a split when the partition holds fewer than two routable keys: fewer
than two distinct hash keys in a hash partition, or fewer than two distinct sort keys in a range
partition. One bounded query answers this, and the statement must stop at two rows.

The guard belongs in `checkSplits()` and not at the new call sites, so the existing write path gets it
too. Log the refusal once, because a partition in this state needs an operator.

#### 4.2.2 The refusal path

`withSplitForwarding` handles `reject_over_size` and throws `errExceededDatabaseSize`. Call
`checkSplits()` before the throw.

- Wrap the call in `try`/`catch` and log a failure, exactly as the three existing call sites do. A
  failure to queue must never change the error that the caller receives.
- `queue()` is already idempotent. It opens one synchronous transaction and returns `undefined` when a
  split repartition exists, so a storm of refused writes queues one split.
- The caller still receives the 503. Its next retry meets a partition whose split is running.

#### 4.2.3 The import path

A repartition target persists `imported` before it acknowledges its source. Call `checkSplits()` after
that point, on the target.

This covers a promotion that delivers a key above the range cap, and a split child that is born above
its own cap. The target is a leaf at this moment, so the call reads its own size and needs no routing.

#### 4.2.4 What does not change

- A write above 1.1 times the cap is still refused.
- A router still forwards before it reads any size.
- No new public API, no new RPC, and no new production hook.

## 5. Alternative options

- **Check the size on the alarm.** The background pass would queue a split for any partition above its
  cap. `scheduleNextPass` deletes the alarm when no durable work remains, so an idle partition that is
  wedged has no alarm to run the check. It helps only beside the refusal path, and it costs a periodic
  read of the size.
- **Accept the write while no split is queued.** This removes the deadlock by construction: refuse a
  write only while a split is already running. It is the most principled option, and it gives up the
  guarantee that a partition stops growing at 1.1 times its cap. It changes the backpressure contract,
  so it needs a separate decision.
- **Validate the caps against each other.** Reject a context whose range cap is far below its hash
  cap. It is a useful guard rail, and it closes one path of three. It leaves the deadlock in place.

## 6. Frequently asked questions

**Does a caller see a different error?** No. The refusal path still throws the same 503-class error,
and the caller still retries.

**How many writes does recovery need?** One refused write queues the split. The caller that was
refused retries and succeeds once the split completes.

**What happens to a partition that is over size and that nobody writes to?** Nothing, and nothing is
needed. The only symptom of the state is a refused write.

**Why keep the 1.1 margin?** It does its job. It stops the split decision from flapping, and it lets
the write that crossed the threshold finish. The defect is the missing trigger, not the margin.

**How does an operator recover a wedged partition before this change?** Raise the cap above the
current size of the partition. One write then applies and queues the split. The cap can go back down
after the split completes.

## 7. References

- `packages/fokosdb/src/shared/partition-topology/split-policy.ts` — `shouldAllow` and `shouldSplit`
  for both partition kinds, and `RANGE_PROMOTION_FRACTION`.
- `packages/fokosdb/src/server/do-partition.ts` — `checkSplits`, its three call sites,
  `withSplitForwarding`, and `scheduleNextPass`.
- `packages/fokosdb/src/shared/partition/repartition/repartition-flow.ts` — `queue()` and the import
  lifecycle. No file in this folder reads a size today.
- `packages/fokosdb/test/property-based/query-items-split.test.ts` — its fixture seeds in two phases
  only because of this defect. The comment above `HASH_SPLIT_MAX_SIZE_MB` explains why. Remove the
  second phase when this RFC is implemented, and the suite then covers the promotion path directly.
