# RFC — Bounded stateful transaction coordination

**State:** Draft
**Date:** 2026-08-30
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
8. [Appendix: deferred designs](#8-appendix-deferred-designs)

---

## 1. Overview and Context

### 1.1 The problem

FokosDB creates a `StaticShardedDO` pool of `DEFAULT_NUM_TRANSACTION_COORDINATORS` (100) coordinator
shards (`src/lib/db.ts`). Each shard stores its transactions in `tc_state`, `tc_participants` and
`tc_items`. Four problems follow.

1. **Storage grows without bound.** Nothing deletes a transaction. `tc_items` holds the full payload,
   up to `MAX_PAYLOAD_BYTES_PER_TX` (4 MB) per transaction. A Durable Object holds at most 10 GB. That
   is 2,500 maximum-size transactions per shard, permanently.
2. **A down shard blocks its transactions.** `pending_transactions.coordinator_do_id` names one Durable
   Object. The recovery alarm can reach that object and no other. While the object is unavailable, its
   locks stay held, and a held lock makes every non-transactional write to that key throw.
3. **The pool size is one global constant.** 100 shards are too many for a small database and too few
   for a large one. FokosDB can serve millions of instances, and most of them are small. A small
   instance spreads its rare transactions over 100 shards, so it pays a Durable Object cold start on
   almost every transaction.
4. **Placement is arbitrary.** Each shard lives where it was first created. That location has no
   relation to the caller and no relation to the partitions.

### 1.2 Why the coordinator stays stateful

The predecessor plan (`docs/agent-plans/2026-08-29-stateless-transaction-coordination.md`) removed the
coordinator state and moved the decision into a ledger table. It was rejected for a round-trip
regression. The reason for that regression fixes the shape of any replacement.

- The decision must be durable before any participant applies it, and the client gets its answer only
  after the commit applies everywhere. The chain `prepare → decide → commit → answer` is sequential.
- On Cloudflare, a durable write costs about 1 ms only inside a Durable Object, which writes its own
  SQLite behind an output gate. A Worker has no durable storage, so a Worker-side driver pays a full
  network round trip to decide.

The component that decides must therefore be a Durable Object that the request already visits. That is
the coordinator. DynamoDB has the same shape: its coordinator is stateful, and its ledger write is a
local in-region write next to the coordinator (USENIX ATC 2023). Its speed comes from a decider that
owns a local log, not from fewer phases.

This plan keeps the coordinator and its write-ahead protocol as they are. It fixes problem 1 with
garbage collection and problem 3 with a pool size that derives from `rootTreesN`. Problems 2 and 4 stay
open as documented residuals (section 2).

### 1.3 What the reader must know

- A write transaction runs two-phase commit. The Worker calls one coordinator shard. The shard writes
  its state to SQLite before every outbound RPC (write-ahead), runs the prepare fan-out, runs the
  commit fan-out, and answers after the commit.
- Each participant (`PartitionDO`) stores the payload for its own items in `pending_transactions`. Each
  lock row stores `transaction_id`, `transaction_ts` and `coordinator_do_id`.
- A participant's stale-transaction alarm (`PartitionDO.STALE_TX_MS`, 5,000 ms) pokes the coordinator
  named in the lock row (`recoverTransaction`). A `not_found` answer means the coordinator has no
  record, and the participant cancels the lock. Write-ahead makes this safe: no record proves the
  transaction can never commit.
- A multi-partition write costs one hop from the Worker to the coordinator, one prepare round trip, and
  one commit round trip. The coordinator's own state writes are local. Total: hop + 2 round trips.

---

## 2. Goals and Requirements

In scope. Each statement must be true when the work is finished.

1. Coordinator storage per shard is bounded: the in-flight transactions, plus one idempotency window of
   small rows. A decided transaction keeps only a small `tc_state` row, and the sweep deletes that row
   one window after the transaction completed. One residual stays: a transaction whose participant is
   permanently unreachable never completes, so its keys and its participant rows stay until the
   participant returns. Its payload does not (4.3.1).
2. A multi-partition write costs hop + 2 round trips, the same as today. No path gets slower.
3. A multi-partition read transaction costs 2 round trips, one less than today.
4. The pool size derives from `rootTreesN` when the caller gives no value, so it is proportional to
   the database and a small database gets a small, warm pool. The `numTxCoordinators` option stays
   optional and overrides the derived value, and each `transactWriteItems` call can override it again
   for new transactions. A pool size change never affects an in-flight transaction.
5. The commit fan-out carries keys, not payload.
6. The idempotency window for `clientRequestToken` is finite, documented, set at creation, and
   immutable afterwards.
7. An over-age lock raises an operational error instead of a silent cancel.
8. Read-your-writes is a guarantee: a client that receives `committed` can read what it wrote on every
   participant (4.3.6). The partition answers reads from its own state, and the read path does not
   resolve locks.

Out of scope, with the reason.

- **Survival of a down coordinator shard.** State lives on the shard, so only that shard can decide its
  in-flight transactions. An outage stalls them, and the participant alarms retry until the Durable
  Object returns. This is the accepted behavior, the same as today. The Appendix (8.1) records the
  settled mitigation, the async decision mirror.
- **Automatic pool scaling.** The pool grows only when the caller passes a larger `numTxCoordinators`
  (4.3.7). The library does not observe load and does not scale the pool itself. Open Question 4.4.2
  tracks the long-term concern.
- **Placement.** This plan adds no location hints. The pool is placed where it is first touched.
- **Changes to the two-phase commit semantics, the conflict rules, the timestamp rules, or the
  single-partition fast paths.**
- **Cross-table transactions.**

---

## 3. Milestones

Each step ships on its own.

1. **Done.** `runCommit` sends keys, not the payload, and the `committed` answer waits for every
   participant (4.3.6). This step also adds the two retryable errors and the commit fan-out request
   budget.
2. **Done.** Move `initiateRead` into the Worker (4.3.4).
3. **Done.** Skip the stale-transaction recovery job on a split parent and on a still-migrating child
   (4.3.2), with an independent `txPendingCanSweep` guard that applies the same ownership rules as the
   TTL sweep. This step needs nothing from the other steps and pays for itself alone. Recovery from
   either role cannot make progress today. Each cycle spends a coordinator RPC and a child fan-out for
   no result, and holds a slot in `listStalePendingTx`. It must ship before step 4, because it keeps a
   split parent's copies away from the `not_found` that the sweep creates.
4. **Done.** Add the garbage collection lifecycle: payload stripping, row deletion at the terminal
   transition, the sweep alarm, the alarm recovery budget, and the `clientRequestToken` length limit
   (4.3.1 to 4.3.3).
5. Derive the default pool size from `rootTreesN`, rename the option to `numTxCoordinators`, and rename
   the shard group to `fokos_tc.<tableName>` (4.3.7). This is a breaking change: existing databases are
   deleted, not migrated (4.3.9).
6. Add the lock-age guard with its operator tools (4.3.5): the three poke outcomes, the quarantine that
   keeps a guarded transaction out of the stale scan, the guard log line with its full field set, and
   the `debugForceResolveTransaction` RPC on `PartitionDO`. The guard compares against
   `IDEMPOTENCY_WINDOW_MS`, so it cannot ship before step 4. Its error state has no resolution path
   without the RPC, so it cannot ship without the RPC.
7. **Optional.** Add the per-call `numTxCoordinators` parameter to `transactWriteItems` (4.3.7). Ship
   it only if a deployment needs it.

---

## 4. Proposed Solution

### 4.1 High-level overview

The coordinator pool stays, stateful, with today's protocol. Four changes.

1. **Garbage collection.** The coordinator strips the payload from `tc_items` when the prepare phase
   ends, deletes the per-transaction rows at the terminal transition, and keeps only one small
   `tc_state` row per transaction for the idempotency window. The existing alarm sweeps expired rows.
   Storage per shard drops from "everything, for ever" to "in-flight, plus one window".
2. **Pool size from `rootTreesN`, growable per call.** The default `numTxCoordinators` becomes a
   multiple of `rootTreesN` instead of the constant 100. A ten-partition database gets a pool of about
   ten warm shards. A large table provisions more with the same knob it already uses for capacity, and
   each `transactWriteItems` call can pass a larger value for new transactions. In-flight transactions
   are unaffected: recovery resolves the coordinator by the Durable Object id stored in each lock row,
   never by the pool size.
3. **Read transactions move into the Worker.** `initiateRead` persists nothing, so it needs no
   coordinator. The Worker runs the two-phase double read itself. This removes one round trip from
   every multi-partition read transaction, and removes all read load from the pool.
4. **The commit fan-out carries keys only.** Participants apply the payload from their own
   `pending_transactions` rows, so today the commit RPC re-sends up to 4 MB for nothing.

The write flow keeps its shape:

```
 Worker            TC shard              P1        P2
   |--initiateWrite-->|                                   hop
   |                  | write-ahead (local SQLite, ~1 ms)
   |                  |--prepare--------->|         |     RTT 1
   |                  |--prepare------------------->|
   |                  |<--accepted--------|<--------|
   |                  | decide (local SQLite, ~1 ms)
   |                  |--commit(KEYS)---->|         |     RTT 2
   |                  |--commit(KEYS)-------------->|
   |                  |<--ok--------------|<--------|
   |                  | strip payload, delete rows,
   |                  | keep one small tc_state row
   |<--committed------|
```

The storage lifecycle of one transaction on the coordinator:

```
 full rows (state + participants + items with payload)
   → payload stripped         at PREPARED or CANCELLING
   → items + participants deleted, completed_at set
                              at the COMMITTED or CANCELLED transition
   → tc_state row deleted     by the sweep alarm, one idempotency
                              window after completed_at
```

### 4.2 Requirements held

- Goal 2: the flow above is hop + 2 round trips. The decision is a local write.
- Goal 8: the `committed` answer comes only after every participant confirmed. This is stricter than
  today (4.3.6).
- Goal 1: section 4.3.8 gives the storage bound.

### 4.3 Technical details

#### 4.3.1 Payload stripping

`tc_items` keeps the payload only while a prepare can still need it.

- The coordinator writes `tc_state`, `tc_participants` and `tc_items` before the first prepare RPC.
  This is unchanged. It is the write-ahead that makes `not_found` safe (4.3.2).
- A transaction in `CREATED` or `PREPARING` keeps its payload. The coordinator alarm re-drives a
  stalled transaction in these states through `drivePrepare`, which loads the payload with `loadItems`.
  This re-drive can still end in a commit. The rejected predecessor plan lost this property. This plan
  keeps it.
- When the transaction reaches `PREPARED` or `CANCELLING`, the coordinator sets `data`, `kind` and
  `conditions_json` to NULL in `tc_items`, inside the same SQLite transaction as the state change. The
  keys and the partition names stay. From this point every code path needs keys only: `runCommit`
  (after milestone 1) and `runCancel` load keys with `loadItemKeys`, and each participant applies from
  its own `pending_transactions` rows (`#applyCommitItems` reads `pendingRow.data`).
- A schema migration relaxes any NOT NULL constraint on the payload columns.

#### 4.3.2 Row deletion, and why `not_found` stays safe

The coordinator deletes the `tc_items` and `tc_participants` rows and sets `tc_state.completed_at` in
the same `UPDATE` that moves the transaction to `COMMITTED` or `CANCELLED`.

One `UPDATE` is enough because a terminal state already proves that every participant confirmed. It is
the only proof the coordinator has, and it is the guard on both terminal transitions: `runCommit`
writes `COMMITTED` only when `COUNT(*) FROM tc_participants WHERE commit_outcome IS NULL` is 0, and
`runCancel` writes `CANCELLED` only when no row still has both `commit_outcome` and `cancel_outcome`
NULL. A transaction with an unconfirmed participant therefore never becomes terminal. It stays in
`COMMITTING` or `CANCELLING`, and the coordinator alarm retries it, as today. A separate confirmation
check before the delete would re-read `tc_participants` to re-derive a fact the transition already
established.

The alarm path keeps today's rule: `recoverTransaction` returns `not_found` when no `tc_state` row
exists, and the poking participant releases its lock. Three invariants keep this sound after garbage
collection.

1. **Write-ahead.** The `tc_state` row exists before any lock exists. "No row" therefore still proves
   "this transaction never got a decision and never will".
2. **Confirmation-gated deletion.** The coordinator deletes the rows only at the terminal transition,
   and that transition needs every participant to confirm.
3. **Only a key owner pokes.** A split parent and a still-migrating child skip the stale-transaction
   recovery job, so the partition that observes a `not_found` owns the keys of its lock and its lock is
   authoritative.

Together, `not_found` means "never prepared" or "fully applied and released". Both make a release
either a no-op or the correct outcome. The lock-age guard (4.3.5) turns a violation of these invariants
into a loud error instead of a silent cancel.

**A split parent's redundant copies.** Invariant 3 exists because a split parent can hold a redundant
copy of a lock whose ownership moved to a child. A parent in `split_started` keeps its
`pending_transactions` rows, and a commit through it routes every item to the children
(`groupItemsByRouting`), so `commitLocal` never runs on the parent and its copy survives the commit.
The coordinator confirms the parent because the forwarded commit succeeded, so the copy is invisible to
the confirmation proof.

Two mechanisms already in the code make the copies harmless. This plan adds one rule, so that a copy
never reaches the `not_found` path.

- **The migration copies the locks to the children.** `SplitMigration` fetches the parent's
  `pending_transactions` rows for the slice of each child and inserts them there
  (`src/lib/partition/migration.ts`). After its migration, the child holds the authoritative lock.
- **`split_completed` deletes the parent's copies.** When the last child acknowledges,
  `migrationAcknowledgeChildComplete` runs `topology.acknowledgeChildMigration` and
  `deleteAllPendingTx` in one `transactionSync` (`src/lib/do-partition.ts`). This is the collection
  point for every copy.
- **New: a split parent and a still-migrating child skip the stale-transaction recovery job.** The
  independent `txPendingCanSweep` guard applies the same ownership rules as the TTL sweep: skip while
  the migration status is `migration_initialized` or `migration_migrating`, and skip while the split
  status is `split_started` or `split_completed`.

The new rule gives up nothing, because recovery from either role cannot make progress today. A parent
pokes the coordinator, gets `COMMITTED`, and calls its own `txCommit`, which routes every key to the
children. `local.length` is 0, so `commitLocal` never runs and the parent's copy stays whatever the
children answer. A still-migrating child rejects its own `txCommit` on `ensureMigration`. Each role
therefore spends one coordinator RPC and one child fan-out per cycle for no progress, and each such
transaction holds one of the ten slots of `listStalePendingTx` while it does so.

**Accepted residual: a stalled split keeps the parent's copies.** A split that never reaches
`split_completed` never runs `deleteAllPendingTx`, and the parent no longer pokes, so it never learns
`not_found` either. The copies stay, with their payloads. The set is bounded by the transactions in
flight at `startSplit`, because a prepare that arrives after `split_started` goes to the child. The key
range of the parent is already unavailable in this state, because its children cannot serve, so the
stalled split is the larger fault.

The Appendix (8.2) records the deferred optimization that deletes the parent's copies per acknowledged
child, to narrow each later fan-out.

#### 4.3.3 The sweep and the idempotency window

- New column: `tc_state.completed_at` (epoch ms, NULL while the transaction runs), with a schema
  migration and a partial index, so the sweep never scans the full table.

  ```sql
  CREATE INDEX IF NOT EXISTS idx_tc_state_completed_at
      ON tc_state (completed_at) WHERE completed_at IS NOT NULL;
  ```

- The existing coordinator alarm gains one job: delete the `tc_state` rows where
  `completed_at < now - IDEMPOTENCY_WINDOW_MS`.
- **The alarm re-arms while swept work remains.** Today `alarm()` re-arms only while a non-terminal
  transaction exists, so an idle shard would never sweep its last rows. The new rule: after each run,
  set the next alarm to the earlier of the stale-transaction need and the earliest
  `completed_at + IDEMPOTENCY_WINDOW_MS`. Arm nothing only when the shard holds no non-terminal row and
  no completed row.
- **The recovery loop gets a deadline, so the sweep always runs.** `alarm()` drives up to 100
  non-terminal transactions one at a time, and each one awaits a fan-out that can retry for tens of
  seconds. A busy shard can therefore spend the whole alarm inside recovery and never reach the sweep,
  which loses the storage bound of goal 1. The loop stops at `ALARM_RECOVERY_BUDGET_MS` (30 s) of wall
  clock, finishes the transaction in hand, and leaves the rest to the next alarm. The sweep then runs on
  every alarm, not only on a quiet one. **FIXME:** the loop drives the transactions one at a time. It
  must drive them concurrently with a bounded fan-out.
- **The sweep is batched.** At the throughput ceiling, one window holds about 600,000 rows (4.3.8), so
  a single unbounded `DELETE` is not safe inside one alarm. The sweep deletes at most
  `SWEEP_BATCH_ROWS` (1,000) rows per run, and re-arms immediately while more expired rows remain.
  `tc_state` is `WITHOUT ROWID`, so the statement selects the primary key instead of
  `DELETE ... LIMIT`:
  `DELETE FROM tc_state WHERE idempotency_token IN (SELECT idempotency_token FROM tc_state WHERE
  completed_at < ? LIMIT ?)`.
- `IDEMPOTENCY_WINDOW_MS` is 10 minutes. DynamoDB documents the same window for `ClientRequestToken`.
  It is **one exported constant in one module**, imported by `TransactionCoordinatorDO` for the sweep
  and by `PartitionDO` for the lock-age guard (4.3.5). It is not a per-instance option and it does not
  travel in `PartitionContext`. Both readers need it inside an alarm, where no request is in flight,
  and a single constant is the only form that cannot drift between them. Drift in one direction fires
  false guard errors. Drift in the other re-executes replays that were still inside the promised
  window. A change to the constant is a deploy that re-scopes every row already written under the old
  value, so treat it as a breaking change, not as a tunable.
- The window is the new, documented contract for `clientRequestToken`. A replay inside the window
  returns the recorded outcome. A replay after the window is a new execution. Today the window is
  unbounded, because nothing is deleted. A replay with the same token and different operations throws
  inside the window, as today.
- **`clientRequestToken` gains a length limit:** 1 to 64 bytes when UTF-8 encoded, enforced at the
  client boundary in `transaction-limits.ts` (`MAX_CLIENT_REQUEST_TOKEN_BYTES`). Today the only check
  is "non-empty after trim" (`db.ts`). The token is the `tc_state` primary key and lives for the whole
  window, so an unbounded token makes the retained row size unbounded. DynamoDB caps the same parameter
  at 36 characters. 64 bytes leaves room for prefixed UUIDs.

#### 4.3.4 Read transactions move into the Worker

`initiateRead` persists nothing and holds no locks. The logic moves from `TransactionCoordinatorDO`
into the Worker-side library (`src/lib/db.ts`, or a small driver module). The two-phase double read is
unchanged: read every item, check that no item has a pending write, read again, compare
`lastCommittedTs`, and abort on any change.

- The cost drops from hop + 2 round trips to 2 round trips.
- The pool serves writes and recovery only.
- When the Worker dies in the middle of a read, nothing leaks. The client retries.
- **Accepted residual: the fan-out now runs under the Worker's connection limit.** A Worker can hold 6
  connections at a time while it waits for response headers. The
  [Workers limits](https://developers.cloudflare.com/workers/platform/limits/) page lists fetch, KV,
  Cache, R2, Queues and TCP for that limit, and does not say whether a Durable Object stub call counts.
  If it counts, a read over N partitions costs `ceil(N / 6)` waves per phase instead of one, so the 2
  round trips above are a ceiling and not a measurement. The read stays in the Worker either way,
  because it removes the coordinator hop and all read load from the pool. Measure only if
  `transactGetItems` latency over many partitions becomes a complaint.

#### 4.3.5 The lock-age guard

**The guard is a defence against a broken invariant. It is not a part of garbage collection.** Garbage
collection is correct without it. A healthy system gives case 1 below, or a `not_found` on a lock
younger than the window, and today's unconditional release resolves both correctly. The guard makes
case 3 loud, because a healthy system
cannot reach case 3, and a silent release there resolves the transaction the wrong way with no later
signal.

**The guard decides per transaction, not per lock row.** The release primitive is already per
transaction (`deletePendingTx`), and the alarm already iterates distinct transaction ids. A per-key
decision would need bookkeeping that buys nothing, because a transaction cannot hold both an owned lock
and a routed-away lock on one partition: a split parent forwards every key, and a promotion cutover is
deferred while any lock exists on that hash key. The alarm loads the local pending rows of the
transaction once, routes them, and takes the oldest `created_at` among them as the lock age.

The stale-transaction alarm handles the three poke outcomes differently.

1. **The poke fails as an RPC error** (the coordinator shard is unreachable): retry on the next alarm
   cycle. The row waits at the coordinator, and nothing is lost.
2. **`not_found`, and the keys route away from this partition**: delete the local rows silently, with
   a direct `deletePendingTx` on the store, and **not** through `txCancel`. The public path exists so
   that recovery which applies an outcome carries the migration guard and the split routing with it.
   This case applies no outcome, because the keys belong to another partition and the rows are
   garbage. The branch is defensive. A split parent is the only known producer of such a row, and the
   guard of 4.3.2 keeps a split parent out of this path, so a healthy system never reaches case 2.
   Keep the branch: it costs a few lines, and without it a stray routed-away row falls through to the
   unconditional release.
3. **`not_found`, this partition owns the keys, and the oldest lock's `created_at` is older than
   `IDEMPOTENCY_WINDOW_MS`**: raise an operational error (log and alert), and do not release. An outage
   alone cannot reach this state, because the sweep waits for this participant's confirmation, so a
   down participant delays garbage collection instead of losing its row (section 6). Arrival here means
   that one of the three invariants of 4.3.2 broke, and a silent release would hide a possible atomicity
   violation. The lock stays held until an operator resolves it with the debug RPC below.

A `not_found` for an owned lock younger than the window releases the lock, as today.

**A guarded transaction leaves the stale scan.** Case 3 creates the first lock this system never
releases on its own, and the scan that finds it is
`SELECT DISTINCT transaction_id, coordinator_do_id FROM pending_transactions WHERE created_at < ?
LIMIT 10`, served by the `created_at` index. A guarded transaction is permanently the oldest, so ten of
them fill every cycle for ever and no other stale transaction on the partition is ever recovered. The
keys of those other transactions then stay locked, and a held lock makes every non-transactional write
to its key throw. One broken invariant would therefore grow into a partition-wide write outage, and
case 3 fires for many transactions at once when it fires at all, because its cause is systemic.

The quarantine is one nullable column:

```sql
-- pending_transactions: set by the lock-age guard, cleared by debugForceResolveTransaction.
-- NULL for every healthy lock.
guarded_at INTEGER
```

- The guard sets `guarded_at` in the same statement that decides not to release.
- `listStalePendingTx` adds `AND guarded_at IS NULL`. The query plan does not change: that scan already
  reads rows instead of covering from `pending_transactions_created_at`, because `coordinator_do_id` is
  not in that index.
- `debugForceResolveTransaction` clears `guarded_at` as a part of the resolution of the transaction, so
  the operator tool and the quarantine are one mechanism.
- The guard logs on the transition into quarantine, and not once per alarm cycle.

The column is durable on purpose. An in-memory set would cost no schema, but eviction loses it: the
guard would re-fire, re-alert and re-starve the scan for one cycle after every eviction, and an
operator could not answer "which locks are stuck?" from storage. The cost is near zero, because
`pending_transactions` rows are transient and a NULL integer adds nothing to a row.

The tradeoff to accept: a quarantined transaction is never poked again, so it cannot heal itself if its
coordinator later returns. That is the intended behavior, because case 3 means that an invariant broke,
and the resolution is an operator who has looked at the two possible outcomes, not a retry that
guesses.

**The debug RPC repairs. It does not diagnose.** `debugForceResolveTransaction` takes the outcome as an
argument. The coordinator has no record, because that is what `not_found` means. The operator therefore
finds the outcome from the other partitions: look at the other keys of the transaction and see whether
they carry its writes.

The operator tools ship with the guard, in milestone 5.

- **The guard log line** carries everything the operator needs:
  `"fokos/partition: lock-age guard: over-age lock with not_found"`, plus `transactionId`,
  `coordinatorDoId`, the encoded keys of every lock this transaction holds here, `lockCreatedAt` and
  `lockAgeMs` of the oldest of them, `windowMs`, and the `doName` and `partitionId` of the partition.
- **The debug RPC:** `debugForceResolveTransaction(pCtx, { transactionId, outcome: "commit" |
  "cancel" })` on `PartitionDO`. It resolves through the public `txCommit` or `txCancel`, with keys
  derived from the partition's own `pending_transactions` rows, so the migration guard and the split
  routing hold. It takes one call per stuck partition.

The guard and the sweep read the one `IDEMPOTENCY_WINDOW_MS` constant of 4.3.3.

#### 4.3.6 The commit fan-out: keys only, and the answer waits for it

**Keys only.** `runCommit` loads the full payload with `loadItems` and ships it on every commit RPC.
The participant ignores it: `#applyCommitItems` reads `pendingRow.data`, never touches `item.data`, and
branches on `pendingRow.operation` and not on the wire value. A 4 MB transaction therefore re-sends
4 MB for nothing. `runCancel` already loads keys only with `loadItemKeys`, and `runCommit` must do the
same.

The interface change: `CommitRequest.items` changes from `TransactionItem[]` to
`TransactionItemKey[]`, the type `CancelRequest.items` already uses, and `commitLocal` and
`#applyCommitItems` take keys. No participant behavior changes, because every per-item fact they use
(`operation`, `data`, `kind`, `conditions`) already comes from the participant's own
`pending_transactions` row. `loadItemKeys` needs no new columns. The stale-transaction alarm on a
participant builds its recovery `txCommit` request from keys the same way.

The key-set cross-check in `commitLocal` is unchanged. Keys stay in the RPC because keys are the
routing information: `groupItemsByRouting` derives the owning child from the key itself, so a commit
reaches forwarded locks on children without a broadcast.

**The `committed` answer waits for every participant.** The coordinator answers `committed` only from
the `COMMITTED` state, which it enters only when every participant confirmed. Today `drivePrepare`
catches a `runCommit` failure and answers `committed` anyway, so a client can read a stale value from
the one participant that did not apply. This plan changes the answer: when the commit fan-out cannot
complete inside the retry budget of the request, the coordinator answers a retryable
**commit-pending** error, and the alarm finishes the commit. `PREPARED` stays final, so the
commit-pending error never means cancelled.

**The two retryable errors.** The coordinator throws both. `InitiateWriteResponse` carries decisions —
`committed` and `cancelled` — and a third outcome member would invite a caller to treat a non-answer as
terminal. Only one of the two errors is new: the undecided throw exists today, and this plan names it.

The error is called commit-pending, and not in-doubt. Classic two-phase commit uses "in doubt" for a
participant that does not know the outcome. Here the coordinator has no doubt: the decision is durable
and `PREPARED` is final, so the transaction commits, and the only unknown is when every participant has
applied it. The two errors stay separate because they promise different things to a client with no
token: undecided can still cancel, commit-pending commits.

| Error | Thrown from | Meaning | Predicate |
| --- | --- | --- | --- |
| undecided | `CREATED`, `PREPARING` | The outcome can still be either. | `isTransactionUndecidedError` |
| commit-pending | `PREPARED`, `COMMITTING` | The transaction commits. Not every participant applied it yet. | `isTransactionCommitPendingError` |

Both predicates are exported next to `isPartitionExceededDatabaseSizeError`, which is how this library
already exposes a testable error class. `FokosDB.transactWriteItems` retries neither one. A write retry
is the decision of the caller, because a tokenless retry is a second transaction.

**The request budget.** `runCommit` retries a participant up to 10 times with up to 2 s of backoff, so
the fan-out can hold a request for about 20 s before it gives up. A gated answer makes that the wait of
the client. The fan-out inside a request therefore stops at `COMMIT_FANOUT_REQUEST_BUDGET_MS` (5 s) of
wall clock and answers commit-pending. The alarm keeps the full budget, because nothing waits on it.

**The split window.** A partition that starts a split after a transaction prepared on it fails every
forwarded commit until its children finish the migration: the parent passes `ensureMigration`, and each
migrating child rejects `txCommit`. Today that answers `committed` and the alarm finishes the work.
Under this rule it answers commit-pending for the length of the migration, which is minutes for a large
partition. The blast radius is the transactions that prepared before `startSplit` and no others,
because a prepare that arrives during the split is rejected and the transaction cancels. This is a
small, bounded set, and it is the accepted cost of goal 8.

What the client does with the commit-pending error:

- With a `clientRequestToken`: retry. The replay reaches the same shard, `runCommit` retries the
  missing participants, and the answer is `committed` once every one confirms.
- Without a token: a retry is a new transaction while the first one still commits through the alarm.
  This ambiguity is not new. It is the lost-response case that exists today, and a tokenless client
  that must not double-apply already needs a token.

This gating is what makes goal 8 a guarantee instead of a common case.

**Accepted residual: the cancel arm still answers early.** `loadFinalResponse` answers `cancelled` from
`CANCELLING`, before every participant confirmed the cancel. That state is reachable under the same
conditions as commit-pending: a participant that stays unreachable past the 10 retries of `runCancel`,
or a participant that rejects `ensureMigration("cancel")` while it migrates. The answer stays truthful
about the outcome, because a cancelled transaction applied nothing anywhere, so goal 8 is not
weakened. What lags is the lock release. The participant keeps its `pending_transactions` rows until
its stale-transaction alarm or the coordinator alarm releases them, and `apiPutItem` and
`apiDeleteItem` throw "item is locked by an in-progress transaction" for any key that holds a pending
row. A client that retries at once can therefore fail on the locks of its own cancelled transaction.
This plan keeps the early answer and adds no third error, because the outcome is complete and only the
cleanup is outstanding.

#### 4.3.7 Pool size: default from `rootTreesN`, growable per call

- **The effective pool size resolves in this order**, and the first value that is present wins:

  1. The optional `numTxCoordinators` argument of `transactWriteItems` (milestone 7, optional work).
  2. The optional `numTxCoordinators` option of `FokosDBOptions`.
  3. The derived default, `2 * rootTreesN`.

  The option in `FokosDBOptions` stays optional and keeps its meaning: when the caller gives a value,
  that value is the pool size, and the derived default is not used. Only the fallback changes, from
  the constant 100 to `2 * rootTreesN`, so an instance that already passes an explicit size behaves
  the same. The option is renamed from `numTransactionCoordinators` to `numTxCoordinators`, so the
  instance option and the per-call parameter carry one name. The multiplier of the derived default is
  a placeholder, and Open Question 4.4.1 tracks it.
- The coordinator shard group is renamed from `<tableName>.tc` to `fokos_tc.<tableName>`, for
  consistency with the naming convention across the library. The Durable Object name of a shard is
  `<shardGroupName>-<shard>`, so this moves every coordinator to a new object. That is safe only
  because the release already deletes existing databases (4.3.9). Shards under the old name are
  unreachable afterwards, and drain themselves through their own sweep.
- **Optional (milestone 7).** `transactWriteItems` accepts a new optional `numTxCoordinators` parameter
  that overrides the instance value for that call. This grows the pool for new transactions without a
  redeploy. Ship it only if a deployment needs it. The instance option gives almost the same
  capability, because a Worker builds a `FokosDB` for each request and can read the value from its own
  configuration. The parameter adds two costs: the idempotency contract below, which the library cannot
  enforce, and shards that `destroy()` does not sweep (4.3.9). `transactGetItems` needs no parameter,
  because read transactions run in the Worker and use no coordinator (4.3.4).
- Any integer of 1 or more is accepted. Growth is the intended use, and a smaller value is not
  rejected: the library keeps no record of past per-call values to validate against, so the per-token
  contract below is the only rule.
- **In-flight transactions do not depend on the pool size.** `initiateWrite` stamps every lock with the
  Durable Object id string of the coordinator (`this.ctx.id.toString()` → `coordinator_do_id`), and the
  participant alarm resolves it directly with `idFromString`. The pool size routes only the initial
  claim, and recovery never hashes the token over the pool. A pool size change never re-routes an
  in-flight transaction.
- One assumption is a part of the API contract: **idempotency holds only while the same
  `clientRequestToken` is retried under the same pool size.** A replay with a different value routes by
  `hash(token) % newSize`, which usually lands on a different shard, but a same-shard result is
  possible. Neither outcome is guaranteed. A replay that lands on a shard without the record executes a
  second time. The library cannot detect the violation, because the record lives on a shard the replay
  does not visit, so the contract is documented on the parameter and not enforced. Tokenless calls can
  use any value at any time.
- Rationale for the proportional default: a coordinator shard sustains about 1,000 requests per second
  (the Durable Objects soft limit), and the write-transaction volume a database can accept scales with
  its partition count, which scales from `rootTreesN`. A database with `rootTreesN: 1` gets 2 shards
  that stay warm. A database with `rootTreesN: 100` gets 200.

#### 4.3.8 Performance and storage bounds

Round trips for the common paths. A round trip between Durable Objects is about 1 to 10 ms in one
location, and 100 to 300 ms across continents. These are estimates and not measurements, because no
location hints exist yet.

| Path | Today | This plan |
| --- | --- | --- |
| Multi-partition write, with or without token | hop + 2 | hop + 2 |
| Multi-partition read transaction | hop + 2 | **2** |
| Single-partition write or read (fast path) | 1 | 1 |
| Token replay of a decided transaction, inside the window | 1 | 1 |

Bytes on the wire drop on every commit by the payload size, up to 4 MB.

Storage per shard splits into two parts.

- **In-flight**: the payloads in `tc_items` and the `tc_participants` rows. A participant row carries
  the serialized partition context, a few hundred bytes each. The coordinator deletes both on
  confirmation, so they last seconds in the normal case.
- **One window of `tc_state` rows.** A committed row is about 200 bytes with the token capped at 64
  bytes (4.3.3): token, transaction id, operations hash, state, and three timestamps. A cancelled row
  also keeps `rejection_reason_json`, which persists the rejecting key. The JSON tagging of a binary
  key costs about 4 bytes per key byte, so a maximum-size binary key (1,024 + 512 bytes) makes a
  cancelled row of about 7 KB.

At the ceiling of 1,000 requests per second and a 10-minute window: 600,000 rows, about 120 MB when
every transaction commits, and about 4 GB in the pathological case where every transaction cancels on a
maximum-size binary key. The pathological case stays under the 10 GB limit, and it needs ten straight
minutes of ceiling-rate cancellations on maximum keys. A typical shard holds far less.

#### 4.3.9 Deployment, migration, rollback

- Two schema changes ship through `SQLSchemaMigrations`: `tc_state.completed_at` with its partial index
  (4.3.3), and `pending_transactions.guarded_at` (4.3.5). Payload stripping needs no migration, because
  `tc_items.data`, `data_kind` and `conditions_json` are already nullable. Both tables are pre-release,
  so the migrations are edited in place instead of added as `ALTER TABLE` steps.
- **This is a breaking release. Existing databases are deleted and recreated, not migrated.** The
  change of the default pool size can route a token to a different shard, so a replay against a
  pre-upgrade database could execute twice. No compatibility path is kept, and `destroy()` removes the
  old data.
- Keys-only commit changes `CommitRequest.items` to `TransactionItemKey[]` (4.3.6). Participant
  behavior is unchanged, and the release is breaking anyway.
- `destroy()` sweeps the pool up to the effective instance pool size (4.3.7), so a deployment
  that passed a larger per-call value leaves shards outside that range untouched. **This is bounded,
  not permanent.** Every shard arms its own alarm and deletes its own `tc_state` rows one idempotency
  window after `completed_at` (4.3.3), so an unswept shard drains itself with no help from `destroy()`.
  The residual is one window: for `IDEMPOTENCY_WINDOW_MS` after `destroy()`, an unswept shard can still
  answer a replayed `clientRequestToken` with the outcome of the destroyed table. After that the shard
  is empty and the replay is a new execution. To reach every shard is therefore a storage-reclamation
  nicety, and not a correctness requirement.
- `destroy()` must iterate the coordinator shards in filtered batches. The platform is not the
  constraint: a Worker invocation allows 1,000 subrequests on Free and 10,000 on Paid
  ([Workers limits](https://developers.cloudflare.com/workers/platform/limits/)), raisable to 10
  million on request. The constraint is `StaticShardedDO`, whose `all()` and `tryAll()` throw outright
  above 1,000 shards, while `some()` and `trySome()` take a `filterFn` and have no such guard. The pool
  can hold up to `2 * 65,000` shards, so `destroy()` uses `some()` over successive shard ranges.
- **TODO(cleanup), independent of this plan:** the partition traversal in `destroy()`
  (`traverseForDestroy`) has the same exposure for a table with more than about 1,000 partitions. It
  predates this plan. Batch it the same way.
- Rollback is not uniform across the release. Garbage collection is additive and can be disabled.
  Stripped or deleted rows are not restored, which only widens the window in which a replay
  re-executes. The other two changes cannot be rolled back with a flag. `CommitRequest.items` becomes
  `TransactionItemKey[]`, so the coordinator and the partitions must deploy together, because a
  half-rolled-back pair disagrees on the wire shape. The gated `committed` answer changes what a client
  observes, so a deploy in either direction has transactions in flight that answer under the old rule
  and transactions that answer under the new one.

#### 4.3.10 Testing

Tests run in the real Workers runtime (`@cloudflare/vitest-pool-workers`). The existing
`test/transactions.test.ts` suite must pass unchanged after every milestone.

Garbage collection:

- The payload is NULL in `tc_items` after `PREPARED`, and the commit still applies the correct data on
  every participant.
- The alarm re-drives a stalled `PREPARING` transaction and it commits, with its payload intact.
- The coordinator deletes rows at the terminal transition and not before, including for a participant
  that forwarded locks to split children: while one participant is unconfirmed the transaction stays in
  `COMMITTING`, and its `tc_items` and `tc_participants` rows survive.
- A token replay inside the window returns the recorded outcome after the per-transaction rows are
  deleted.
- A token replay after the window executes a new transaction.
- A token replay with different operations throws inside the window.
- An idle shard sweeps its last completed rows with no further traffic: the alarm re-arms until the
  window passes (4.3.3).
- A shard with more expired rows than `SWEEP_BATCH_ROWS` deletes them across several alarm runs, and
  re-arms until none remain (4.3.3).
- A shard whose recovery loop exceeds `ALARM_RECOVERY_BUDGET_MS` still runs the sweep in that same
  alarm (4.3.3).
- A `clientRequestToken` longer than `MAX_CLIENT_REQUEST_TOKEN_BYTES` throws at the client boundary,
  before any coordinator RPC (4.3.3).

Lock-age guard and split-parent copies:

- A split parent and a still-migrating child skip the stale-transaction recovery job: neither pokes the
  coordinator while the split runs (4.3.2).
- A commit through a split parent applies on every child, and the copies of the parent are gone after
  the last child acknowledges, through the `split_completed` transition (4.3.2).
- A split stalled past the window leaves the copies of the parent in place, with no poking and no
  operational error (4.3.2).
- The operational error fires only for a `not_found` on a lock whose key the partition owns and that is
  older than the window, and the lock is not released.
- A quarantined transaction leaves the stale scan: with ten guarded transactions held, an eleventh,
  younger stale transaction on the same partition still recovers on the next alarm cycle, and the guard
  logs once per transaction instead of once per cycle (4.3.5).
- `debugForceResolveTransaction` clears `guarded_at`, and the locks of the resolved transaction are
  gone afterwards (4.3.5).
- A failed poke (coordinator unreachable) retries and never raises the error.
- The guard log line carries every field of 4.3.5, and `debugForceResolveTransaction` resolves the lock
  through the public paths.

Reads in the Worker:

- The double read aborts on a pending write and on a `lastCommittedTs` change, as it does on the
  coordinator today.

Commit with keys, and the gated answer:

- A multi-megabyte transaction commits with commit RPCs that carry no payload.
- A commit with one unreachable participant answers the retryable commit-pending error, and never
  `committed`. After the participant returns, a token replay answers `committed`, and a `getItem` on
  every written key returns the new value (4.3.6).
- `isTransactionCommitPendingError` matches that error and `isTransactionUndecidedError` does not, and
  the reverse holds for a transaction still in `PREPARING` (4.3.6).
- The commit-pending answer arrives within `COMMIT_FANOUT_REQUEST_BUDGET_MS`, and not after the full
  retry budget of the alarm (4.3.6).
- A transaction that prepared before a split answers commit-pending while the children migrate, and
  answers `committed` on a token replay once the migration completes (4.3.6).
- A cancel with one unreachable participant answers `cancelled` while that participant still holds its
  locks, and a non-transactional write to one of those keys throws until the stale-transaction alarm
  releases them (4.3.6).

Pool size:

- An in-flight transaction recovers after the pool size changes: the stale-transaction alarm reaches
  the coordinator through the stored Durable Object id, with any pool size in effect.
- A token replay with the same pool size returns the recorded outcome. A replay with a different pool
  size that re-routes the token executes a new transaction, because the contract of 4.3.7 holds only
  under the same value. The test picks a token and value pair that re-routes.

### 4.4 Open Questions

#### 4.4.1 The pool multiplier

`2 * rootTreesN` is a placeholder. The options are 1x, 2x, 3x, or a measured formula from coordinator
throughput. The answer changes only the default, and the mechanism is the same. TODO: measure the
throughput of a coordinator shard under transaction load.

#### 4.4.2 Pool growth after partition splits

The pool is sized from `rootTreesN` at creation, but the partition count grows through splits. A
long-lived database can hold many more partitions than root trees, so its write throughput scales past
a pool that stays at the initial size. The options are to keep the per-call `numTxCoordinators`
override as the manual lever, or to scale the pool from measured database throughput and the post-split
partition count. The answer decides whether automatic pool scaling (out of scope, section 2) becomes
its own plan.

---

## 5. Alternative Options

**Stateless coordinator with a ledger table.** The rejected predecessor,
`docs/agent-plans/2026-08-29-stateless-transaction-coordination.md`. The driver moves into the Worker
and the decision moves into a second FokosDB instance. Rejected: a Worker has no durable storage, so
the decision costs one network round trip, and a token write grows from hop + 2 to claim + 3 round
trips. The predecessor document keeps the full correctness analysis.

**Answer at the decision point, resolve locks on touch.** Percolator-style lazy commit: answer
`committed` when the decision is durable, run the commit fan-out after the answer, and make every read
or write that touches a still-locked item resolve the lock through a ledger read. It reaches 2 round
trips for a tokenless write. Rejected: the read path gains a network dependency, with its tail latency
and its availability coupled to the ledger. The partition must answer reads from its own state alone.

**One fixed large pool (for example 4,096 shards).** Overprovisioning makes a resize unnecessary.
Rejected: FokosDB serves millions of instances and most are small. A small database spreads its rare
transactions over thousands of cold shards, and pays a cold start on almost every transaction. The pool
must be proportional to the database.

**Time-windowed coordinators.** Name shards by (shard, time window) and drop whole Durable Objects when
a window expires. Rejected: a `clientRequestToken` does not carry its window, so a replay must probe
the current and the previous window, and old Durable Objects need a sweeper to destroy them. Row-level
garbage collection gives the same bound with none of that.

**One coordinator Durable Object per transaction.** It gives perfect isolation and trivially bounded
storage. Rejected: every transaction pays a Durable Object cold start on the happy path, and every
recovery pays one too.

**Async decision mirror.** Not rejected. Settled and deferred to the Appendix (8.1).

---

## 6. Frequently Asked Questions

**Why must the coordinator keep state? The predecessor removed it.**
The decision must be durable before the commit fan-out, and the client gets its answer after the
commit. Only a Durable Object writes durably in about 1 ms, and a Worker pays a network round trip. A
stateless design therefore costs at least one extra round trip per write (1.2).

**Is the storage bound real, with a 4 MB payload limit?**
The payload lives on the coordinator only between the write-ahead and the end of the prepare phase,
which is normally well under one second. After that the shard holds keys, and then one `tc_state` row
for the window: about 200 bytes committed, and up to about 7 KB cancelled (4.3.8). The bound depends on
the token length limit of 4.3.3, because without it the retained row is unbounded. The worst cases in
4.3.8 assume that the shard runs at its throughput ceiling for the whole window.

**What happens when a coordinator shard is down?**
Its in-flight transactions stall, and their locks block non-transactional writes to those keys. The
participant alarms retry every `STALE_TX_MS` and wait for the Durable Object to return. This is today's
behavior, unchanged and accepted. The async decision mirror in the Appendix (8.1) is the mitigation if
this becomes a problem in practice.

**Can a participant outage outlast the idempotency window and strand a lock?**
No. The sweep waits for that participant, and not the other way around. The timeline: the transaction
decides, the commit RPC to the down participant fails, so its `tc_participants` row stays unconfirmed,
`completed_at` is never set, and the sweep never deletes the row. When the participant returns, after
any duration, the retry of the coordinator or the poke of the participant finds the row, gets the
decision, applies it, and releases the lock. `not_found` cannot appear in this timeline at any lock
age, which is why the error state of the guard (4.3.5, case 3) signals a broken invariant and not a
slow outage.

**What does a client see when a participant is down during the commit?**
A retryable commit-pending error, and never `committed` (4.3.6). The decision is durable and the alarm
finishes the commit. A replay with the same `clientRequestToken` answers `committed` once every
participant confirmed. A tokenless retry is a new transaction, which is the same ambiguity a lost
response gives today.

**Why an error at all, instead of a retry until every participant applies?**
The coordinator does retry, and `COMMIT_FANOUT_REQUEST_BUDGET_MS` is what buys that retry. The error is
what happens when the retries run out, and they must run out: a participant can be unreachable for
hours, and a request that waits for it never returns and holds the shard. The choice is not "error or
retry". It is which answer to give when the retry budget ends. `committed` would be the lie this plan
removes, and `cancelled` would be false because `PREPARED` is final. A retryable error is the only
truthful answer left.

**Does the commit-pending answer weaken read-your-writes?**
No. It is what makes goal 8 a guarantee. Goal 8 is conditional: a client that receives `committed` can
read what it wrote everywhere. A client that receives commit-pending received no `committed`, so it has
nothing to read back yet. It retries with its token and gets `committed` once the last participant
confirms. This change does not create the window in which one participant has applied the transaction
and another has not. That window exists between the first and the last commit RPC today, because
two-phase commit is atomic in its outcome, not in the instant that outcome becomes visible. What
changes is only whether the client is told `committed` before that window closes.

**Does `destroy()` have to reach every coordinator shard?**
No, and it cannot when a per-call `numTxCoordinators` grew the pool past the instance value. Every
shard sweeps its own `tc_state` one idempotency window after each transaction completes, so an unswept
shard empties itself. The exposure is one window (4.3.9).

**How does the pool grow, and what does a change affect?**
Pass a larger `numTxCoordinators` on `transactWriteItems`. The value routes only the initial claim of a
new transaction, and in-flight transactions are unaffected. Idempotency holds only while the same
`clientRequestToken` is retried under the same pool size (4.3.7).

**What breaks for existing databases?**
This is a breaking release with no migration path: existing databases are deleted and recreated. The
change of the default pool size is the reason, because a different pool can route a token to a
different shard (4.3.9).

**Does garbage collection change what a client observes?**
One thing: the `clientRequestToken` idempotency window becomes finite (10 minutes) instead of
accidentally infinite. A replay after the window is a new execution. DynamoDB documents the same
contract.

---

## 7. References

- `docs/agent-plans/2026-08-29-stateless-transaction-coordination.md` (rejected predecessor)
- `docs/agent-plans/dynamodb-distributed-transactions-plan.md`
- `docs/agent-plans/2026-08-23-single-partition-transaction-fast-path.md`
- [Distributed Transactions at Scale in Amazon DynamoDB, USENIX ATC 2023](https://www.usenix.org/system/files/atc23-idziorek.pdf)
- [Amazon DynamoDB transactions: `ClientRequestToken`](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html)
- [Cloudflare Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Cloudflare Workers limits](https://developers.cloudflare.com/workers/platform/limits/)

---

## 8. Appendix: deferred designs

Neither design is a part of this plan. Both are recorded so that they are settled when the need
appears.

### 8.1 The async decision mirror

The mirror shrinks the stall from a down shard (section 2, out of scope) to the transactions whose
mirror write had not landed. A second FokosDB instance holds one decision row per transaction, keyed by
`txId`.

- The coordinator writes the row asynchronously, **after** its local decision is durable, best effort.
  A lost mirror write degrades recovery back to "wait for the coordinator", and can never cause a wrong
  outcome.
- The mirror is a cache, and only a cache. **Absence never decides.** A recovery miss means "ask the
  coordinator, or wait", and never "cancel". A hit applies only when `row.txId` matches the
  `transaction_id` of the lock. Because absence proves nothing, eviction is free: any retention, for
  example 24 hours, is an operational choice with no correctness impact.
- Recovery only. The write path never reads the mirror, and the `clientRequestToken` idempotency window
  stays equal to the retention of the coordinator (4.3.3). An extension of the window through the
  mirror would cost one mirror read on every token claim, because a new token and a swept token both
  miss locally.
- A partition reaches the mirror by convention: it derives the `PartitionContext` of the mirror from
  its own, with the same namespace and topology values and a different table name under a reserved
  prefix. `PartitionContextImpl.create` rejects user table names with that prefix, and this validation
  ships with the mirror.
- Cost: one extra asynchronous Durable Object write per transaction, and a second instance to operate.
  The mirror grows and splits like any table. It is off the critical path, so a splitting mirror
  partition only delays recovery reads.

The rejected predecessor plan (`docs/agent-plans/2026-08-29-stateless-transaction-coordination.md`)
holds the full correctness analysis for ledger-style decision rows.

### 8.2 Per-acknowledged-child cleanup of a split parent's copies

A split parent keeps its `pending_transactions` rows for the whole split (4.3.2), so a commit or a
recovery through the parent fans out to every child the transaction touched, including the children
that already applied and acknowledged. A delete of the parent's rows per acknowledged child narrows
each later fan-out to the children that still hold keys.

Two changes are needed.

- **The child fan-out in `txCommit` becomes `Promise.allSettled`.** `Promise.all` rejects on the first
  failure and discards the other results, which hides the set of children that succeeded. The parent
  still collects the failures and rethrows, so a failed child keeps the coordinator from confirming.
  The cost is fail-fast: the parent then waits for the slowest child before it rethrows, which spends
  more of `COMMIT_FANOUT_REQUEST_BUDGET_MS` (4.3.6) and makes a commit-pending answer more likely.
- **A delete over the item set of one child.** When every child succeeded, the existing
  `deletePendingTx(transactionId)` is enough. A per-child delete needs a new `PartitionStore` method
  keyed by `(transaction_id, hk, sk)`, because the two deletes today are both too wide:
  `deletePendingTx` drops every key of the transaction, and `deletePendingTxForHashKey` drops every
  transaction on a hash key.

The gain is a narrower fan-out, and not fewer recovery cycles. `listStalePendingTx` selects
`DISTINCT transaction_id`, so a transaction with any remaining row still holds one of the ten slots and
still pokes the coordinator.

One rule constrains the optimization: **a copy owned by a child that has not acknowledged must never be
deleted.** Migration fetches are hash-sliced, so a still-migrating child can hold the parent's copy as
its only source of the committed data, and a deletion loses the write on that child. Cancel is exempt,
which is why `cancelLocal`-first is already correct: the copy of a cancelled transaction carries
nothing to apply, so the pre-transaction items a child fetched are already the correct final state.
