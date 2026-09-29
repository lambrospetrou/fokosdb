# RFC — Queue the repartition of an over-size partition when it refuses a write

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
  - [4.3 Open questions](#43-open-questions)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

A partition has a size cap. Two decisions read it, and the host hooks in `PartitionDO` make both.

| Decision                     | Condition                        | Hook                    |
| ---------------------------- | -------------------------------- | ----------------------- |
| Queue a split                | `databaseSize > maxSizeMb`       | `hooks().evaluateSplit` |
| Refuse a `write`-tag request | `databaseSize > maxSizeMb * 1.1` | `hooks().admit`         |

The cap is `hashSplitConditions.maxSizeMb` for a hash partition and `rangeSplitConditions.maxSizeMb` for a range
partition. The 10% margin is intentional. It stops the decision from flapping at the cap, and it lets the write that
crosses the cap complete.

`TransactionCoordinatorDO` has the same two hooks. Its cap is the smaller of `hashSplitConditions.maxSizeMb` and
`maxDatabaseBytes / 1.1`. `maxDatabaseBytes` is a setting of `TransactionCoordinatorDO.fokosConfig()`, and its default
is 5 GiB. This change gives the coordinator a floor (section 4.2.6).

### 1.1 How a split is queued today

A split is queued in one way only:

1. A local handler of `apiPutItem`, `txCommit` or `txExecuteSingleShot` applies a write.
2. The handler calls `signalGrowth`, which signals `promotionCandidates` and then `evaluateSplit`.
3. `FokosShardingRuntime.#applySignals` runs `#evaluateSplit` after the handler returns.

`#admit` in `runtime.ts` throws the refusal before any handler runs, so a refused request applies no signal. The
coordinator calls `requestSplitEvaluation()` in `initiateWriteLocal`, which also runs only after admission.

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
| A promotion finishes, so the hash split behind it can queue     | `acceptAck`, then `source_cleanup`       | No      |
| A coordinator refuses a new transaction for size                | coordinator `admit`, through `#admit`    | No      |

The six call sites of `#admit` are: `#dispatchPoint`, the local fallback of `#forwardPoint`, the two branches of
`#dispatchGroup`, `#dispatchSingleOwner`, and `#dispatchRange`.

Each "No" row becomes visible only as a refused `write`-tag request, and every refusal passes through `#admit`. A
partition between 1.0 and 1.1 times its cap still accepts writes, and the write path already checks it.

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
- **A promotion that overshoots.** A key is promoted at `hashSplitConditions.maxSizeMb * promotionFraction` and lands
  in a range root that `rangeSplitConditions.maxSizeMb` caps. A root received 844 KB against a cap of 0.2 MB and
  refused every write for the 35 s of the test.

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

Restore this invariant: **when a leaf refuses a write because of its size, and a repartition can make it smaller, a
repartition is queued.** A repartition here is a hash split, a range split, or a key promotion. The invariant has one
known exception until milestone 5: a second large key behind a promotion that cannot finish (section 4.2.8).

### 2.1 In scope

- A refused `write`-tag request must start a repartition decision. The decision must not delay the refusal.
- The fix must live in the runtime, so `PartitionDO` and `TransactionCoordinatorDO` both get it.
- A hash leaf with one hash key must promote that key, whatever the size estimate of the key (section 2.4).
- The decision must not queue a repartition that cannot make the partition smaller (section 2.4).
- The coordinator must split only when it holds two or more idempotency tokens (section 4.2.6).
- Regression tests for each row of the table in section 1.2.
- Milestone 5: a hash leaf with more than one large key must promote them, and a promotion that cannot finish must
  not stop the promotion of another key (section 4.2.8).

### 2.2 Out of scope

- The 1.1 margin, the admission tags, and the backpressure contract. A refused write stays refused.
- The error that the caller receives. It stays the same 503-class error.
- A check without a request. A leaf that nobody writes to keeps its size, because its only symptom is a refused
  write.
- Validation between `hashSplitConditions.maxSizeMb` and `rangeSplitConditions.maxSizeMb`. It closes one path of
  three, and it is a separate change.
- A `debugForceSplit` RPC.
- The range planner. When `computeRangeBoundaries` returns `null`, the source still defers with a backoff of up to
  `sourceRetryMaxMs`. After this change that case needs a race: a write or a delete between the queue and the plan
  changes the rows so that the planner finds no boundaries.
- A size measure that falls at once after a delete. See section 4.2.7.
- A single path for the write-time promotion. `signalGrowth` keeps its `promotionCandidates` signal, because it
  carries the exact size of each written key.

### 2.3 Requirements

- **No timer.** The change adds no timer, no alarm and no scheduler job. `#evaluateSplit` keeps its existing alarm,
  which it arms before it queues a repartition.
- **No loop.** Only a client request starts a decision. A decision that finds nothing to do schedules nothing.
- **No cost under the cap.** A partition under its cap pays one read of `sql.databaseSize` on the write path, as
  today. A refusal happens only above 1.1 times the cap.
- **Bounded queries.** A decision on a hash leaf runs a fixed number of index seeks. The one scan is the range
  boundary scan, and section 4.2.2 bounds it at the floor.
- **No new RPC, and no new host hook.** The change widens the result type of `evaluateSplit` only.
- **Never on incomplete data.** A target in `awaiting_data` or `importing` holds only part of its slice, so it must
  not decide. The import gate of `#dispatch` refuses the request before `#admit` runs, so a refusal on such a target
  never starts a decision.

### 2.4 The floor

A repartition cannot make a partition smaller than one item. The floor for each kind is:

- **Range leaf.** A range leaf is at its floor when `computeRangeSplitBoundaries` returns `null`. It returns `null`
  in two cases:
  - The leaf holds fewer than `rangeSplitN` items, because each child needs one. The default `rangeSplitN` is 4.
  - The bytes are skewed, so the byte-balanced scan finds fewer than `rangeSplitN - 1` boundaries. For example,
    with `rangeSplitN` 4, a leaf that holds one 300 KB row and three 1 KB rows gets one boundary, not three. One
    item can be up to `MAX_ITEM_BYTES` (400 KB), so this case is real for a range cap of a few MB or less.
- **Hash leaf.** A hash split cannot divide one hash key, but a promotion can move that key into a range tree, and
  the range tree splits it by sort key. A leaf with one hash key therefore promotes that key whatever its estimated
  size. The promotion threshold alone is not enough, for these reasons:
  - The threshold compares the logical `est_bytes` of the key with the cap times `promotionFraction`.
  - Admission compares the physical file size with the cap.
  - The file also holds the keys again in both indexes, overflow pages for large keys, and free space in pages.
    With a 1 KB hash key and 100-byte rows, the file is about 2.5 times `est_bytes`.
  - With a `promotionFraction` of 0.5, a one-key leaf at 1.15 times the cap can then have `est_bytes` of only 0.45
    times the cap.

  A hash leaf reaches its floor only when it holds no item, and its size is lock rows or runtime tables.

A real deployment usually sets a cap far above the size of one item, so the floor is rare outside tests. It matters
for one reason: the decision must not queue a repartition below the floor. A range split below the floor stays
`queued` for ever and wakes the alarm every `sourceRetryMaxMs` (5 minutes).

## 3. Milestones

Each milestone must leave the test suite green. Review each one before the next starts.

1. **The coordinator floor.** The coordinator decision splits only with two or more tokens (section 4.2.6). This
   milestone lands first, because the refusal trigger of milestone 3 makes an over-size coordinator child split.
2. **The host decision.** Widen the result type of `evaluateSplit` (section 4.2.3). `evaluateSplit` of `PartitionDO`
   returns a split, a promotion of the one key of a leaf, or `false`, and it respects the floor (section 4.2.4). The
   write path uses the new decision at once.
3. **The refusal trigger.** `#admit` starts a decision before it throws the refusal, and `#evaluateSplit` skips the
   hook when a split row exists (section 4.2.2).
4. **The tests.** One regression test per row of section 1.2. Seed the fixture in
   `test/property-based/query-harness.ts` in one phase, and remove its two-phase comment. Record the run time of
   `query-items-split.test.ts` and `query-items-active-split.test.ts` before and after, for the milestone review.
5. **More than one large key.** A hash leaf with two or more hash keys promotes its large keys before it splits, up
   to 8 of them in one decision (section 4.2.8).

## 4. Proposed solution

### 4.1 High-level overview

Today the partition checks for a split only when a write applies. A partition above 1.1 times its cap applies no
write, so it never checks.

The fix adds one trigger: when admission refuses a request, the runtime runs the same split decision that the write
path runs. It does this before it throws the refusal, and it does not wait for the result. The first refused write
therefore queues the repartition, and the caller that retries succeeds after the repartition completes.

Every event of section 1.2 ends in a refused write, so this one trigger covers all of them. No timer and no
background job is needed. A leaf that nobody writes to needs nothing, because its only symptom is a refused write.

The host decides what to do, and it respects the floor:

- A hash leaf with two or more keys splits.
- A hash leaf with one key promotes that key.
- A range leaf splits when the planner can find its boundaries.
- At the floor, the decision is `false`, and the host logs an error once per instance.

The coordinator gets the same floor as a hash leaf: it splits only when it holds two or more idempotency tokens.
Without it, a coordinator child that holds one large transaction would split again at each refused transaction.

```
  write-tag request ──► #admit ──► hooks.admit
                                     │
                     ┌── "allow" ────┴──── { reject } ──┐
                     ▼                                  ▼
             local handler                    requestSplitEvaluation()   (new, not awaited)
                     │                                  │
             signalGrowth (today)                       │         then: throw the refusal
                     │                                  │
                     └──────────────┬───────────────────┘
                                    ▼
                             #evaluateSplit
                                    │
                         split row exists? ── yes ──► stop
                                    │ no
                         hooks.evaluateSplit()
                            ├─ false                 ──► stop (floor: error log, once per instance)
                            ├─ { data }              ──► canQueue ──► queue a hash or range split
                            └─ { promote: [K, ...] } ──► #requestPromotion(K) for each key
```

### 4.2 Technical details

#### 4.2.1 The refusal trigger

`#admit` in `runtime.ts` calls `this.requestSplitEvaluation()` before `throw decision.reject`. The same line covers
the six call sites of `#admit` and the coordinator, because both hosts reach their `admit` hook only through
`#admit`.

The properties of the call:

- **It does not delay the refusal.** `requestSplitEvaluation` starts `#applySignals` and does not await it. The host
  hook is synchronous, so it runs before the throw. The alarm write and the queue transaction run after the caller
  receives its answer. The coordinator already uses the same call in `initiateWriteLocal`.
- **It cannot fail the request.** `#applySignals` catches and logs every error.
- **It runs on every refusal.** The runtime does not know why a host refused. Both hosts refuse only for size today,
  and the host decision starts with its own size check, so a refusal for another reason costs one size read.

`ctx.waitUntil` is not needed. The Durable Object state API page says it has no effect in a Durable Object, and the
instance keeps running the promise after the response.

#### 4.2.2 The decision in the runtime

`#evaluateSplit` changes in two places:

```ts
async #evaluateSplit(): Promise<void> {
	// A split row in any state blocks every repartition here, a router included.
	if (this.#store.getSplitRepartition() !== undefined) {
		return;
	}
	const identity = this.identity();
	const decision = this.#hooks.evaluateSplit({ identity, policy: this.policy() });
	if (decision === false) {
		return;
	}
	if ("promote" in decision) {
		for (const hashKey of decision.promote) {
			try {
				await this.#requestPromotion(hashKey, decision.data);
			} catch (error) {
				// Logged. The other keys still get their try.
			}
		}
		return;
	}
	// Unchanged from today: canQueue, ensureAlarmAtMost, queue, the log line, and wake().
}
```

- **The split row check.** Today the method returns early on `routerRole()`, which reads the split row and its
  targets. The new check reads the split row only, and it also stops a leaf whose split is `queued` or `planned`. So
  the refused writes during a split do not run the host decision. `canQueue` refuses the same cases, but it runs
  after the host decision, and on a range leaf that decision scans rows.
- **A promotion.** `#requestPromotion` already checks `canQueue`, arms the alarm before its queue transaction, and
  answers without a throw when a promotion or a split row already exists. A failure of one key does not stop the
  next key.
- **A race.** `queue` repeats every check of `canQueue` inside its transaction. A request that passes the checks and
  then loses a race is refused there, and it writes nothing.
- **A failure.** When the alarm write or the queue fails, `#applySignals` logs the error. The next refused write
  tries again. No timer retries it, so a failure that repeats causes no loop.

#### 4.2.3 The `evaluateSplit` result type

The hook in `runtime-types.ts` becomes:

```ts
evaluateSplit(input: { identity: FokosPartitionIdentity; policy: TPolicy }):
	| false
	| { data?: unknown }
	| { promote: KeyBytes[]; data?: unknown };
```

- **`{ promote: [...] }`** lists the keys that the host wants promoted, largest first. It is never empty. The runtime
  never queues a split from this answer. Until milestone 5, the list holds one key.
- **The existing hosts need no change to the shape of their answer.** They return `false` or `{ data }`: the
  coordinator, and the test hosts in `test/sharding/` and `test/sharding-prototype/`. The counter host of
  `test/sharding/` splits only with two or more keys, so it respects the floor. The hosts in
  `test/sharding-prototype/` are sketches that no test runs.

The hook stays synchronous. Its documentation changes from "called after a local success that signals
`evaluateSplit`, and by `requestSplitEvaluation`" to "called after a local success that signals `evaluateSplit`, by
`requestSplitEvaluation`, and when admission refuses a request". The documentation also states the floor rule, which
the current text does not say: "Return `false` when no repartition can make this partition smaller. A host that
returns a split for a partition at its floor makes each child split again at its first refused write." Only the host
can apply this rule, because only the host can read its own rows.

#### 4.2.4 The decision of `PartitionDO`

`hooks().evaluateSplit` runs these steps in order, and stops at the first answer:

1. **Size.** Read the cap for the identity kind. When `sql.databaseSize` is at or below it, return `false`. This is
   one property read, and it is the whole cost for a partition under its cap.
2. **Hash leaf.** Count the hash keys up to two, with two seeks of the `(hk, sk)` index:

   ```sql
   SELECT hk FROM items ORDER BY hk LIMIT 1;       -- the first key
   SELECT 1 FROM items WHERE hk > ? LIMIT 1;       -- any key after it
   ```

   - Two or more keys: return `{}`.
   - One key: return `{ promote: [thatKey] }`, whatever the size estimate of the key (section 2.4).
   - No key: go to step 4.

   A `SELECT DISTINCT hk ... LIMIT 2` is not bounded: it reads every row of the first key before it finds the second
   key.
3. **Range leaf.** Call `this.#store.computeRangeSplitBoundaries(hashKey, start, end, rangeSplitN)` with the range
   of the identity and `rangeSplitN` from `this.fokos.routeContext().rangeConfig`. These are the same arguments that
   the planner passes. When the result is not `null`, return `{}`. The planner then finds the same boundaries,
   unless a write or a delete changes the rows first (section 2.2).
4. **Floor.** The leaf is at its floor (section 2.4). Return `false`, and log an error once per instance. Keep an
   in-memory flag: set it when this step logs, and clear it when the hook returns anything else. Every refused write
   runs the decision, so a log per call floods the logs. The log is an error, because the leaf refuses every write
   until an operator raises the cap or deletes data. It names the size, the cap, and the reason: no hash key, fewer
   than `rangeSplitN` items, or bytes that are too skewed for `rangeSplitN` parts.

`computeRangeSplitBoundaries` logs a `console.warn` today on each call that finds fewer than `rangeSplitN` items.
That log moves out of the store, because step 4 logs the same fact once per instance.

**The cost of step 3 at the floor.** Step 3 is the only step that scans rows. When it finds boundaries, the split is
queued, and the split row check of section 4.2.2 stops every later call. At the floor, every refused write scans
again:

- With fewer than `rangeSplitN` items, the store stops after a count of at most `rangeSplitN` rows.
- With skewed bytes, the scan reads every row of the leaf. A scan can miss a boundary only when one row holds more
  than about `1 / (rangeSplitN × (rangeSplitN - 1))` of the bytes of the leaf. The item bytes of such a leaf are below
  `rangeSplitN × (rangeSplitN - 1) × MAX_ITEM_BYTES`: 4.8 MB when `rangeSplitN` is 4. The scan reads the covering
  index `idx_items_scan` only. TODO: measure the scan time at that size.

When the measure shows that this cost matters, section 4.3 has the option to remove it.

**The rows of a promoted key after `completed`.** Step 2 counts a promoted key while its cleanup still deletes its
rows. A leaf that holds the leftover rows of one promoted key and one other key therefore splits as a leaf with two
keys. The split copies no row of the promoted key: `belongsToTarget` leaves out every key with a terminal route
override. This costs one split level, and the child that gets the other key promotes it when it refuses a write. The
split can queue at `completed`, because `hasUnfinishedPromotion` counts only `queued`, `planned` and `cutover`.

`signalGrowth` keeps its order: `promotionCandidates` first, then `evaluateSplit`. On the write path, a one-key leaf
then finds the key that the signal already queued, and `#requestPromotion` answers `already_promoted`.

#### 4.2.5 Coverage

| Event (section 1.2)                                         | After this change                              |
| ----------------------------------------------------------- | ---------------------------------------------- |
| `apiPutItem`, `txCommit`, `txExecuteSingleShot` applies     | `signalGrowth`, unchanged                      |
| A `write`-tag request is refused, `txPrepare` included      | `requestSplitEvaluation()` in `#admit`         |
| The caller sends a lower cap                                | The next write is refused, as above            |
| A split child finishes its import                           | First write: refused, or applied and signalled |
| A promotion delivers a key into a range root                | First write: refused, or applied and signalled |
| A promotion finishes, so the hash split behind it can queue | The next refused write can queue the split     |
| A coordinator refuses a new transaction                     | `requestSplitEvaluation()` in `#admit`         |

#### 4.2.6 The coordinator floor

Today the coordinator decision is only `sql.databaseSize > maxBytes(policy)`. The coordinator has no floor, so the
refusal trigger can split it again and again:

1. A coordinator splits. One child imports a token whose ledger rows are above the cap. The ledger of one
   transaction can hold up to 4 MB of payload and 10 MiB of condition images, and it stays for the idempotency
   window of 10 minutes.
2. The child refuses a new transaction, and the refusal queues its split.
3. The grandchild that gets the token is also above the cap. Its next refused transaction splits it too. This
   repeats until the idempotency sweep deletes the token.
4. When the cap is below the size of an empty coordinator file, every child is above it. Each refused transaction
   then adds a level. `test/transactions/tx-paths.test.ts` sets a cap of 1 byte, so the suite reaches this case.

The fix gives the coordinator the floor of a hash leaf. A hash split moves whole tokens, so it cannot divide one
token, the same as it cannot divide one hash key. The coordinator `evaluateSplit` runs these steps in order:

1. **Size.** When `sql.databaseSize` is at or below `maxBytes(policy)`, return `false`. This is unchanged.
2. **Tokens.** Count the idempotency tokens up to two, with two seeks of the unique index
   `tc_state_idempotency_token`:

   ```sql
   SELECT idempotency_token FROM tc_state ORDER BY idempotency_token LIMIT 1;  -- the first token
   SELECT 1 FROM tc_state WHERE idempotency_token > ? LIMIT 1;                 -- any token after it
   ```

   - Two or more tokens: return `{}`.
   - One token or none: return `false`, and log an error once per instance, as step 4 of section 4.2.4 does.

The floor stops both cases above:

- **An empty coordinator** holds no token, so it never splits. The 1-byte cap of `tx-paths.test.ts` then refuses the
  transaction and splits nothing, and the test needs no change.
- **A coordinator with one large token** does not split. It stays above its cap, and it refuses every new token that
  routes to it until the idempotency sweep deletes the old token. The idempotency window is 10 minutes, so the wait
  is about 10 minutes plus the interval of the sweep.
- **A coordinator with two or more tokens** splits. Each split separates tokens, and a child that is left with one
  token stops.

The floor needs no new option and no validation, and the cap of the coordinator stays `hashSplitConditions`. A
production cap is far above the ledger of one transaction: the default cap is 500 MB, and one ledger holds at most
4 MB of payload and 10 MiB of condition images. So the one-token case happens only in tests or with a wrong cap.

#### 4.2.7 The size after a delete

These facts about Durable Object SQLite storage hold for this design:

- SQLite stores rows in pages. A delete frees a page only when it removes every row of that page.
- The storage runs with `auto_vacuum = FULL`, so SQLite gives a free page back at the commit of each write
  transaction. There is no free list that waits for a `VACUUM`.
- So a few deletes can leave `sql.databaseSize` unchanged: their pages still hold other rows.
- A delete of tens or hundreds of MB frees many complete pages, so the size is expected to fall. This is expected,
  not guaranteed: rows that are spread thinly over many pages can keep every page in use.

So a delete can leave the size above the cap. The design handles each case:

- **A leaf that clients delete from.** It refuses writes until its size falls under 1.1 times the cap. When its
  remaining items are below the floor, each refused write gets the answer `false`.
- **A promotion source after its promotion completes.** It can stay above its cap. With two or more hash keys left,
  the next refused write queues a hash split, while the cleanup still runs (section 4.2.4).
- **A hash leaf with one key and much partly empty space.** For example: the cap is 10 MB, one key L with 2 MB of
  live rows remains, and the file stays at 11.5 MB because the rows of L are spread thinly over many pages. Step 2
  of section 4.2.4 finds one key and promotes it. The range root copies only the live rows into a compact file, and
  the cleanup empties the leaf.

A fall in size needs no trigger, because a partition under its cap needs no repartition.

#### 4.2.8 Milestone 5: more than one large key

Until milestone 5, a hash leaf with two or more keys splits on the refusal path, even when one key is large. This has
two costs:

- **One extra copy.** The split copies the large key into a child, and the child then promotes it with a second
  copy. The write path still promotes first, through `signalGrowth`, so the extra copy happens only after a cap
  change or a skewed split.
- **A key behind a stuck promotion.** A promotion that cannot finish, for example because of a quarantined lock,
  blocks the hash split: `canQueue` refuses a hash split while a promotion is `queued`, `planned` or `cutover`. A
  second large key on the same leaf then cannot move, because the refused writes carry no promotion signal. A
  quarantined lock needs an operator in any case.

Milestone 5 removes both costs. Step 2 of section 4.2.4 changes for a leaf with two or more keys:

1. Read the largest keys at or above the promotion threshold:

   ```sql
   SELECT hk FROM key_size_estimates WHERE est_bytes >= ? ORDER BY est_bytes DESC LIMIT ?
   ```

   The threshold is `hashSplitConditions.maxSizeMb * promotionFraction * 1024 * 1024`, as in
   `promotionCandidates`. The limit is `PROMOTION_CANDIDATES_MAX`, a constant of `PartitionDO` with the value 5.
2. When the query returns a row, return `{ promote: [hk, ...] }`. The runtime tries each key, and
   `#requestPromotion` refuses a key that already has a promotion.
3. Otherwise return `{}`.

Three more changes come with it:

- **The index.** Add `CREATE INDEX IF NOT EXISTS key_size_estimates_by_bytes ON key_size_estimates (est_bytes);` to
  the migration that creates `key_size_estimates` in `partition-store.ts`. Edit the migration in place, because the
  project is before its first release. With the index, the query is one seek that reads at most 8 rows. Without it,
  the query reads one row per hash key. The cost is one index write for each update of `key_size_estimates`, and
  every item upsert makes one such update.
- **A completed promotion leaves the list.** The `beforeComplete` hook of `PartitionDO` deletes the size estimate of
  the promoted key, in the transaction that moves the promotion to `completed`. Without it, the key stays in the list
  until the cleanup deletes its last row, and the split behind it waits. The cleanup deletes
  `promotedKeyCleanupRows` (1,000) rows per step, every `cleanupRetryMs` (5 s). A key of 250,000 rows then holds the
  split, and every refused write, for about 21 minutes. No other reader needs the estimate after `completed`: every
  new write of the key goes to the range tree. The cleanup keeps its own call of `deleteKeySizeEstimate` after the
  last row, as a guard.
- **The limit of 5.** When the 5 largest keys all have promotions that cannot finish, a sixth key over the threshold
  waits until one of them finishes.

#### 4.2.9 Deployment and rollback

The change adds no RPC, no option and no durable record. A rollback removes the refusal trigger and restores today's
behavior. The index of milestone 5 can stay after a rollback.

#### 4.2.10 Testing

Each test uses real timers and the scheduled-alarm test APIs. None mocks a global clock.

1. **Refusal path.** Fill a hash leaf with two or more keys, lower the cap, and send one put. The put is refused, a
   hash split runs, and a later put succeeds. Suite: a new over-size file in `test/partition-do/`.
2. **Transaction-only workload.** The same as test 1, with `txPrepare` only. Suite: the same file.
3. **One key.** Lower the cap on a leaf whose data is one hash key, below the promotion threshold of the key. A
   refused put queues a promotion, and no split row exists. Suite: the same file.
4. **Import path.** Promote a key into a small range cap. The first write to the range root is refused, and the
   range root splits. Suite: `test/repartition/`.
5. **Range floor, few items.** A range leaf holds 3 items above 1.1 times its cap, with `rangeSplitN` 4. After some
   refused writes there is no split row, `getAlarm()` is `null`, and the floor error is logged once. Suite:
   `test/partition-do/`.
6. **Range floor, skewed bytes.** A range leaf holds one row of about 300 KB and three rows of 1 KB, above 1.1 times
   its cap, with `rangeSplitN` 4. The result is the same as test 5. Suite: the file of test 5.
7. **Split during a cleanup.** A hash leaf holds one large key and some small keys, and stays above its cap after
   the large key is promoted. Set `promotedKeyCleanupRows` low, so the cleanup takes more than one step. A refused write
   queues a hash split while the promotion is still `completed`, and no child receives a row of the promoted key.
   Suite: the file of test 1.
8. **Coordinator split.** A coordinator holds two or more tokens, and a controlled coordinator reports a size above
   its cap, as the existing suite does. A new transaction is refused once, then the coordinator splits. Suite:
   `test/transactions/tx-coordinator-split.test.ts`.
9. **Coordinator floor.** A coordinator holds one token, or none, above its cap. A refused transaction queues no
   split, and the floor error is logged once. The existing 1-byte test in `tx-paths.test.ts` covers the case with no
   token. Suite: `test/transactions/tx-coordinator-split.test.ts`.
10. **Milestone 5: two large keys, one blocked.** A hash leaf holds two keys above the promotion threshold. The first
    key holds a quarantined lock, so its promotion stays `planned`. A refused write promotes the second key. Suite:
    the file of test 1.

The property fixture in `test/property-based/query-harness.ts` then seeds in one phase. Both
`query-items-split.test.ts` and `query-items-active-split.test.ts` use it. The active suite keeps writing to the
tree, so it can meet a range root that refuses writes until its first split completes. Milestone 4 records the run
time of both suites before and after.

### 4.3 Open questions

#### 4.3.1 A memo for the range floor

At the floor, each refused write runs the boundary scan again (section 4.2.4). When the measure shows that this cost
matters, the host can keep an exact in-memory memo:

- `PartitionStore` keeps a counter, `itemsVersion`. Each method that inserts, replaces or deletes a row of `items`
  adds 1 to it before its statement runs: `upsertItem`, `updateItemSingleShot`, `deleteItem`, `deleteExpiredItems`,
  `insertItemIfAbsent`, and `deleteItemsBatchForHashKey`.
- When step 3 returns `null`, the host stores the pair (`itemsVersion`, `rangeSplitN`). The next decision skips the
  scan when the pair is equal.
- A refused write changes no row, so it scans no more. A new method that writes `items` must add 1 to the counter.

## 5. Alternative options

- **A `split_check` job in the scheduler.** A built-in job answers "due now" while a leaf is over its cap and a
  repartition can queue. The scheduler reads it at the start and the end of every pass, and a refused write or a
  policy change calls `wake()`. It is rejected for these reasons:
  - Every event that it covers ends in a refused write, which the refusal trigger covers.
  - It needs a guard against a busy loop, a retry pause after a failure, and a change to `FokosScheduler.#pass`.
  - At the end of each import, it splits a coordinator child that is above its cap, with no request, so a cap below
    the size of an empty file splits children with no end.
- **A periodic job every 0.5 to 1 s.** Each partition runs a pass on a fixed timer and checks the size. It is
  rejected for three reasons:
  - Cost. At 1 s, one partition gets 86,400 alarm invocations per day, about 2.6 million per month. Alarm invocations
    are billed as requests at $0.15 per million, so about $0.39 per partition per month, or about $3,900 per month
    for 10,000 partitions. Each `setAlarm()` is also billed as one row written. Source: the Durable Objects pricing
    page.
  - It removes the rule in `FokosScheduler.#pass` that an idle partition keeps no alarm.
  - It does not solve the floor, and without the floor it queues useless repartitions faster. It is also up to 1 s
    late, where the refusal trigger runs in the refused request.
- **Check the size on the alarm only.** The pass deletes the alarm when no durable work remains, so an idle wedged
  leaf has no alarm to run the check.
- **Accept the write while no split is queued.** Refuse a write only while a split runs. This removes the wedge by
  construction, and it gives up the guarantee that a partition stops growing at 1.1 times its cap. It changes the
  backpressure contract, so it needs a separate decision.
- **A separate coordinator cap with a validated minimum.** A new option, `txCoordinatorSplitConditions`, with a
  minimum of 10 times the largest ledger of one transaction, about 142 MiB with the default key limits. It keeps a
  coordinator with one large token under its cap. It is rejected, because it adds a public option, a validation,
  and the rewrite of two coordinator tests. The floor of section 4.2.6 stops the same splits with two seeks.
- **Validate the caps against each other.** Reject a context whose range cap is far below its hash cap. It is a
  useful guard, and it closes one path of three. The wedge stays.
- **Put the refusal trigger in the `admit` hook of `PartitionDO`.** The host calls `requestSplitEvaluation()` before
  it returns the refusal. It fixes the partition only, and the coordinator stays wedged. The trigger in `#admit`
  covers both hosts with one line.
- **A runtime hook that tells the host a split was skipped.** Only the host can see that a repartition cannot make a
  partition smaller, because only the host reads its rows. The host already knows when it returns `false`, so the
  hook carries no new fact. The host logs the floor itself (section 4.2.4).

## 6. Frequently asked questions

**Does a caller see a different error?** No. The refusal still throws the same 503-class error, and the caller
still retries.

**How many refused writes does recovery need?** One. The refused request starts the decision, and the decision
queues the repartition after the caller receives its answer. The caller retries and succeeds once the repartition
completes.

**Why keep the write-time signal?** It promotes a key by the exact size of each written key. It also queues a split
between 1.0 and 1.1 times the cap, before any write is refused.

**What happens to a leaf that is over its cap and that nobody writes to?** Nothing, and nothing is needed. The only
symptom is a refused write, and the first one starts the decision.

**Why does a one-key leaf promote its key whatever its size?** The size cap reads the physical file, and the
promotion threshold reads the logical estimate. The file can be about 2.5 times the estimate (section 2.4). Without
this rule, a one-key leaf above its cap can stay below the promotion threshold and refuse writes for ever.

**Why does a leaf with two or more keys split before it promotes its large key?** It keeps the decision to two index
seeks. The cost is one extra copy of the large key, only on the refusal path. Milestone 5 adds promotion first.

**Why does the range floor call the boundary scan, and not count the items?** The planner needs one item per child,
and it also needs the bytes to fall into `rangeSplitN` parts. A count of `rangeSplitN` items is not enough: a leaf
with one large row and a few small rows has enough items and still gets no plan. When the decision queues a
split that the planner cannot plan, the split stays `queued` for ever. The decision runs the same function as the
planner, so the two cannot disagree on the same rows.

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
- `packages/fokosdb/src/server/do-partition.ts` — `hooks()` (`evaluateSplit`, `admit`, `beforeComplete`,
  `cleanupSourceStep`), `signalGrowth`, `promotionCandidates`.
- `packages/fokosdb/src/server/do-transaction-coordinator.ts` — the coordinator `evaluateSplit`, `admit`, and
  `initiateWriteLocal`.
- `packages/fokosdb/src/sharding/runtime.ts` — `#admit`, `#applySignals`, `#evaluateSplit`, `#requestPromotion`,
  `requestSplitEvaluation`.
- `packages/fokosdb/src/sharding/runtime-types.ts` — `FokosShardingHooks.evaluateSplit`.
- `packages/fokosdb/src/sharding/repartition-flow.ts` — `RepartitionSource.canQueue`, `queue`, `belongsToTarget`,
  the range planner.
- `packages/fokosdb/src/sharding/sharding-store.ts` — `getSplitRepartition`, `hasUnfinishedPromotion`.
- `packages/fokosdb/src/shared/partition/partition-store.ts` — `computeRangeSplitBoundaries`, the
  `key_size_estimates` migration.
- `packages/fokosdb/test/property-based/query-harness.ts` — the two-phase seed.
- [Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)
- [Durable Objects SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Durable Object state API](https://developers.cloudflare.com/durable-objects/api/state/) — `ctx.waitUntil`.
