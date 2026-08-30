# RFC — Bounded stateful transaction coordination

**State:** Draft
**Date:** 2026-08-30
**Author:** Lambros

References:

- `docs/agent-plans/2026-08-29-stateless-transaction-coordination.md` (rejected predecessor)
- `docs/agent-plans/dynamodb-distributed-transactions-plan.md`
- `docs/agent-plans/2026-08-23-single-partition-transaction-fast-path.md`
- [Distributed Transactions at Scale in Amazon DynamoDB, USENIX ATC 2023](https://www.usenix.org/system/files/atc23-idziorek.pdf)
- [Amazon DynamoDB transactions: `ClientRequestToken`](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html)
- [Cloudflare Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Cloudflare Workers limits](https://developers.cloudflare.com/workers/platform/limits/)

---

## Table of Contents

1. [Overview and Context](#1-overview-and-context)
2. [Goals and Requirements](#2-goals-and-requirements)
3. [Milestones](#3-milestones)
4. [Proposed Solution](#4-proposed-solution)
5. [Alternative Options](#5-alternative-options)
6. [Future extensions](#6-future-extensions)
7. [Frequently Asked Questions](#7-frequently-asked-questions)

---

## 1. Overview and Context

### 1.1 The problem

`FokosDB` creates a `StaticShardedDO` pool of `DEFAULT_NUM_TRANSACTION_COORDINATORS`
(100) coordinator shards (`src/lib/db.ts`). Each shard stores its transactions in
`tc_state`, `tc_participants` and `tc_items`. Four problems follow:

1. **Storage grows without bound.** There is no garbage collection. `tc_items` holds
   the full payload, up to `MAX_PAYLOAD_BYTES_PER_TX` (4 MB) per transaction. A Durable
   Object holds at most 10 GB. That is 2,500 maximum-size transactions per shard,
   permanently.
2. **A down shard blocks its transactions.** `pending_transactions.coordinator_do_id`
   names one DO. The recovery alarm can reach that one object and no other. While the
   object is unavailable, its locks stay held, and a held lock makes every
   non-transactional write to that key throw.
3. **The pool size is one global constant.** 100 shards is too many for a small
   database and too few for a large one. FokosDB can serve millions of instances, and
   most of them are small. A small instance routes its rare transactions across 100
   shards, so it pays a DO cold start on almost every transaction.
4. **Placement is arbitrary.** Each shard lives where it was first created. That
   location is unrelated to the caller and unrelated to the partitions.

### 1.2 Why the coordinator stays stateful

The predecessor plan (`2026-08-29-stateless-transaction-coordination.md`) removed the
coordinator state and moved the decision into a ledger table. It was rejected for a
round-trip regression. The reason for that regression fixes the shape of any
replacement:

- The decision must be durable before any participant applies it, and the client is
  answered only after the commit applies everywhere. The chain
  `prepare → decide → commit → answer` is sequential.
- On Cloudflare, a durable write costs about 1 ms only inside a Durable Object, which
  writes its own SQLite behind an output gate. A Worker has no durable storage, so a
  Worker-side driver pays a full network round trip to decide.

Therefore the component that decides must be a Durable Object that the request already
visits. That is the coordinator. DynamoDB has the same shape: its coordinator is
stateful, and its ledger write is a local in-region write next to the coordinator
(USENIX ATC 2023 paper). Its speed comes from the decider that owns a local log, not
from fewer phases.

This plan keeps the coordinator and its write-ahead protocol exactly as they are, and
fixes the four problems one by one: garbage collection bounds the storage, the pool
size derives from `rootTreesN`, and the two smaller problems are documented residuals.

### 1.3 What the reader must know

- A write transaction runs two-phase commit: the Worker calls one coordinator shard,
  the shard writes its state to SQLite before every outbound RPC (write-ahead), then
  runs the prepare fan-out and the commit fan-out, and answers after the commit.
- Each participant (`PartitionDO`) stores the payload for its own items in
  `pending_transactions`. Each lock row stores `transaction_id`, `transaction_ts` and
  `coordinator_do_id`.
- A participant's stale-transaction alarm (`PartitionDO.STALE_TX_MS`, 5,000 ms) pokes
  the coordinator named in the lock row (`recoverTransaction`). A `not_found` answer
  means the coordinator has no record, and the participant cancels the lock. This is
  safe because of write-ahead: no record proves the transaction can never commit.
- Round trips for a multi-partition write today: one hop from the Worker to the
  coordinator, one prepare round trip, one commit round trip. The coordinator's own
  state writes are local. Total: hop + 2 round trips.

---

## 2. Goals and Requirements

In scope. Each statement must be true when the work is finished:

1. Coordinator storage per shard is bounded: the in-flight transactions plus one
   idempotency window of small rows. Nothing is kept forever.
2. A multi-partition write costs hop + 2 round trips, the same as today. No path gets
   slower.
3. A multi-partition read transaction costs 2 round trips, one less than today.
4. The default pool size derives from `rootTreesN`, so it is proportional to the
   database. A small database gets a small, warm pool. Each `transactWriteItems` call
   can override the pool size for new transactions, and a pool size change never
   affects an in-flight transaction.
5. The commit fan-out carries keys, not payload.
6. The idempotency window for `clientRequestToken` is finite, documented, set at
   creation, and immutable afterwards.
7. An over-age lock raises an operational error instead of a silent cancel.
8. Read-your-writes is a guarantee: a client that receives `committed` can read what it
   wrote on every participant (4.3.6). The partition answers reads from its own state;
   the read path does not resolve locks.

Out of scope, with the reason:

- **Surviving a down coordinator shard.** State lives on the shard, so only that shard
  can decide its in-flight transactions. An outage stalls them, and the participant
  alarms retry until the DO returns. This is the accepted behavior, the same as today.
  Future extensions (section 6) records the settled mitigation (async decision mirror).
- **Automatic pool scaling.** The pool grows only when the caller passes a larger
  `numTxCoordinators` (4.3.7). The library does not observe load and does not scale the
  pool itself. Open Question 4.4.2 tracks the long-term concern.
- **Placement.** No location hints in this plan. The pool is placed where it is first
  touched.
- **Changes to the 2PC semantics, the conflict rules, the timestamp rules, or the
  single-partition fast paths.**
- **Cross-table transactions.**

---

## 3. Milestones

Each step is shippable on its own.

1. `runCommit` sends keys, not the payload, and the `committed` answer waits for every
   participant (4.3.6).
2. Add the lock-age guard (4.3.5).
3. Move `initiateRead` into the Worker (4.3.4).
4. Add the garbage-collection lifecycle: payload stripping, row deletion on
   confirmation, the sweep alarm, and the `clientRequestToken` length limit (4.3.1 to
   4.3.3). The sweep is what exposes the split-parent copies to `not_found`, so this
   step also ships the per-confirmed-child copy cleanup in `txCommit` (4.3.2) and the
   guard scoping of 4.3.5.
5. Derive the default pool size from `rootTreesN`, and add the per-call
   `numTxCoordinators` parameter to `transactWriteItems` (4.3.7). Breaking change:
   existing databases are deleted, not migrated (4.3.9).
6. Add the guard's operator tools (4.3.5): the `debugForceResolveTransaction` RPC on
   `PartitionDO`, and the guard log line with its full field set.

---

## 4. Proposed Solution

### 4.1 High-level overview

The coordinator pool stays, stateful, with today's protocol. Four changes:

1. **Garbage collection.** The coordinator strips the payload from `tc_items` when the
   prepare phase ends, deletes the per-transaction rows when every participant
   confirms, and keeps only one small `tc_state` row per transaction for the
   idempotency window. The existing alarm sweeps expired rows. Storage per shard drops
   from "everything, forever" to "in-flight plus one window".
2. **Pool size from `rootTreesN`, growable per call.** The default `numTxCoordinators`
   becomes a multiple of `rootTreesN` instead of the constant 100. A ten-partition
   database gets a pool of about ten warm shards. A large table provisions more with
   the same knob it already uses for capacity, and each `transactWriteItems` call can
   pass a larger value for new transactions. In-flight transactions are unaffected:
   recovery resolves the coordinator by the DO id stored in each lock row, never by the
   pool size.
3. **Read transactions move into the Worker.** `initiateRead` persists nothing, so it
   needs no coordinator. The Worker runs the two-phase double read itself. This removes
   one round trip from every multi-partition read transaction and removes all read load
   from the pool.
4. **The commit fan-out carries keys only.** Participants apply the payload from their
   own `pending_transactions` rows, so the commit RPC re-sends up to 4 MB for nothing
   today.

Write flow, unchanged in shape:

```
 Worker            TC shard              P1        P2
   |--initiateWrite-->|                                      hop
   |                  | write-ahead (local SQLite, ~1 ms)
   |                  |--prepare--------->|         |        RTT 1
   |                  |--prepare------------------->|
   |                  |<--accepted--------|<--------|
   |                  | decide (local SQLite, ~1 ms)
   |                  |--commit(KEYS)---->|         |        RTT 2
   |                  |--commit(KEYS)--------------->|
   |                  |<--ok--------------|<--------|
   |                  | strip payload, delete rows,
   |                  | keep one small tc_state row
   |<--committed------|
```

Storage lifecycle of one transaction on the coordinator:

```
 full rows (state + participants + items with payload)
   → payload stripped         at PREPARED or CANCELLING
   → items + participants deleted, completed_at set
                              when every participant confirmed
   → tc_state row deleted     by the sweep alarm, one idempotency
                              window after completed_at
```

### 4.2 Requirements held

- Goal 2: the flow above is hop + 2 round trips. The decision is a local write.
- Goal 8: the `committed` answer comes only after every participant confirmed. This is
  stricter than today (4.3.6).
- Goal 1: section 4.3.8 gives the storage bound.

### 4.3 Technical details

#### 4.3.1 Payload stripping

`tc_items` keeps the payload only while a prepare can still need it.

- The coordinator writes `tc_state`, `tc_participants` and `tc_items` before the first
  prepare RPC. Unchanged: this is the write-ahead that makes `not_found` safe (4.3.2).
- A transaction in `CREATED` or `PREPARING` keeps its payload. The coordinator alarm
  re-drives a stalled transaction in these states through `drivePrepare`, which loads
  the payload with `loadItems`. This re-drive can still end in a commit. The rejected
  predecessor plan lost this property; this plan keeps it.
- When the transaction reaches `PREPARED` or `CANCELLING`, the coordinator sets `data`,
  `kind` and `conditions_json` to NULL in `tc_items`, inside the same SQLite
  transaction as the state change. The keys and partition names stay. From this point
  every code path needs keys only: `runCommit` (after milestone 1) and `runCancel` load
  keys with `loadItemKeys`, and each participant applies from its own
  `pending_transactions` rows (`#applyCommitItems` reads `pendingRow.data`).
- A schema migration relaxes any NOT NULL constraint on the payload columns.

#### 4.3.2 Row deletion, and why `not_found` stays safe

When the transaction is terminal (`COMMITTED` or `CANCELLED`) and every participant
confirmed (`commit_outcome` or `cancel_outcome` set on every `tc_participants` row),
the coordinator deletes the `tc_items` and `tc_participants` rows and sets
`tc_state.completed_at`. A terminal transaction with an unconfirmed participant is not
deleted; the coordinator alarm retries it until it confirms, as today.

The alarm path keeps today's rule: `recoverTransaction` returns `not_found` when no
`tc_state` row exists, and the poking participant releases its lock. Two invariants
keep this sound after garbage collection:

1. **Write-ahead**: the `tc_state` row exists before any lock exists. So "no row" still
   proves "this transaction never got a decision and never will".
2. **Confirmation-gated deletion**: the sweep deletes only rows whose every participant
   confirmed, and a confirmed participant holds no **authoritative** lock. A forwarded
   commit or cancel confirms only after every owning child acknowledges.

Together: `not_found` means "never prepared" or "fully applied and released". Both make
a release a no-op or the correct outcome. The lock-age guard (4.3.5) turns any
violation of these invariants into a loud error instead of a silent cancel.

**The split-parent copy.** Invariant 2 says "authoritative" because a split parent can
hold a redundant copy of a lock whose ownership moved to a child. A parent in
`split_started` deletes its `pending_transactions` rows only at the `split_completed`
transition. A commit through a split parent routes every item to the children
(`groupItemsByRouting`), so `commitLocal` never runs on the parent, and the parent's
copy survives. The coordinator confirms the parent because the forwarded commit
succeeded, so the copy is invisible to the confirmation proof — and when a sibling
child stalls the split, the copy grows older than the window. Two rules make the copies
harmless:

- **The parent deletes its copies per confirmed child.** When `txCommit` on a split
  parent forwards to the children, it deletes its local rows for each child that
  succeeded, inside the call and before it responds. The Durable Object output gate
  holds the response until the deletion is durable, so a crash before durability also
  withholds the success. The coordinator then does not confirm, retries, and the retry
  cleans up, because the children are idempotent. The stale-transaction alarm resolves
  through the public `txCommit`, so it inherits this cleanup.
- **A commit copy owned by an unconfirmed child is never deleted.** Migration fetches
  are hash-sliced: a child fetches only the pending rows inside its own slice. A
  still-migrating child can therefore depend on the parent's copy as its only source of
  the committed data, and a deletion loses the write on that child. A different child's
  confirmed copies are safe to delete for the same reason: nothing outside that slice
  ever fetches them. Cancel is exempt from this rule, which is why `cancelLocal`-first
  is already correct today: a cancelled transaction's copy carries nothing to apply, so
  the pre-transaction items a child fetched are already the correct final state.

#### 4.3.3 The sweep and the idempotency window

- New column: `tc_state.completed_at` (epoch ms, NULL while the transaction runs), with
  a schema migration, and a partial index so the sweep never scans the full table:

  ```sql
  CREATE INDEX IF NOT EXISTS idx_tc_state_completed_at
      ON tc_state (completed_at) WHERE completed_at IS NOT NULL;
  ```

- The existing coordinator alarm gains one job: delete `tc_state` rows where
  `completed_at < now - IDEMPOTENCY_WINDOW_MS`.
- **The alarm re-arms while swept work remains.** Today `alarm()` re-arms only while a
  non-terminal transaction exists, so an idle shard would never sweep its last rows.
  The rule becomes: after each run, set the next alarm to the earlier of the
  stale-transaction need and the earliest `completed_at + IDEMPOTENCY_WINDOW_MS`. Arm
  nothing only when the shard holds no non-terminal row and no completed row.
- `IDEMPOTENCY_WINDOW_MS` defaults to 10 minutes. DynamoDB documents the same window
  for `ClientRequestToken`. The value is set when the `FokosDB` instance is created and
  **must not change afterwards**. The sweep and the lock-age guard (4.3.5) both compare
  against it, and a changed value re-scopes rows that completed under the old one:
  drift in one direction fires false guard errors, drift in the other re-executes
  replays that were still inside the promised window.
- The window is the new, documented contract for `clientRequestToken`: a replay inside
  the window returns the recorded outcome; a replay after the window is a new
  execution. Today the window is unbounded because nothing is deleted. A replay with
  the same token and different operations throws inside the window, as today.
- **`clientRequestToken` gains a length limit**: 1 to 64 bytes when UTF-8 encoded,
  enforced at the client boundary in `transaction-limits.ts`
  (`MAX_CLIENT_REQUEST_TOKEN_BYTES`). Today the only check is "non-empty after trim"
  (`db.ts`). The token is the `tc_state` primary key and lives for the whole window, so
  an unbounded token makes the retained row size unbounded. DynamoDB caps the same
  parameter at 36 characters; 64 bytes leaves room for prefixed UUIDs.

#### 4.3.4 Read transactions move into the Worker

`initiateRead` persists nothing and holds no locks. The logic moves from
`TransactionCoordinatorDO` into the Worker-side library (`src/lib/db.ts` or a small
driver module). The two-phase double read is unchanged: read every item, check that no
item has a pending write, read again, compare `lastCommittedTs`, abort on any change.

- Cost drops from hop + 2 round trips to 2 round trips.
- The pool serves writes and recovery only.
- When the Worker dies mid-read, nothing leaks. The client retries.

#### 4.3.5 The lock-age guard

The stale-transaction alarm handles the three poke outcomes differently:

1. **The poke fails as an RPC error** (the coordinator shard is unreachable): retry on
   the next alarm cycle. The row waits at the coordinator; nothing is lost.
2. **`not_found`, and the key routes away from this partition** (a split parent's
   leftover copy, 4.3.2): delete the local copy silently. The sweep implies every
   owning child confirmed, so the copy is redundant.
3. **`not_found`, this partition owns the key, and the lock's `created_at` is older
   than `IDEMPOTENCY_WINDOW_MS`**: raise an operational error (log and alert) and do
   not release. This state is unreachable by an outage alone — the sweep waits for this
   participant's confirmation, so a down participant delays garbage collection instead
   of losing its row (see the FAQ). Reaching it means invariant 1 or 2 of 4.3.2 broke,
   and a silent release would hide a possible atomicity violation. The lock stays held
   until an operator resolves it with the debug RPC below.

The operator tools ship in milestone 6:

- **The guard log line** carries everything the operator needs:
  `"fokos/partition: lock-age guard: over-age lock with not_found"`, plus
  `transactionId`, `coordinatorDoId`, the encoded `hashKey` and `sortKey`,
  `lockCreatedAt`, `lockAgeMs`, `windowMs`, and the partition's `doName` and
  `partitionId`.
- **The debug RPC**:
  `debugForceResolveTransaction(pCtx, { transactionId, outcome: "commit" | "cancel" })`
  on `PartitionDO`. It resolves through the public `txCommit` / `txCancel` with keys
  derived from the partition's own `pending_transactions` rows, so the migration guard
  and the split routing hold. One call per stuck partition.

A `not_found` for an owned lock younger than the window releases it, as today.

The guard and the sweep must read the same configured value. Two constants drift, and
drift in one direction raises false alarms while drift in the other restores the silent
cancel.

#### 4.3.6 The commit fan-out: keys only, and the answer waits for it

**Keys only.** `runCommit` loads the full payload with `loadItems` and ships it on
every commit RPC. The participant ignores it: `#applyCommitItems` reads
`pendingRow.data` and never touches `item.data`, and it branches on
`pendingRow.operation`, not on the wire value. A 4 MB transaction re-sends 4 MB for
nothing. `runCancel` already loads keys only with `loadItemKeys` and documents why.
`runCommit` must do the same.

The concrete interface change: `CommitRequest.items` changes from `TransactionItem[]`
to `TransactionItemKey[]`, the type `CancelRequest.items` already uses. `commitLocal`
and `#applyCommitItems` take keys. No participant behavior changes, because every
per-item fact they use (`operation`, `data`, `kind`, `conditions`) already comes from
the participant's own `pending_transactions` row. `loadItemKeys` needs no new columns.
The stale-transaction alarm on a participant builds its recovery `txCommit` request
from keys the same way.

The key-set cross-check in `commitLocal` is unchanged. Keys stay in the RPC because
keys are the routing information: `groupItemsByRouting` derives the owning child from
the key itself, so a commit reaches forwarded locks on children without a broadcast.

**The `committed` answer waits for every participant.** The coordinator answers
`committed` only from the `COMMITTED` state, which it enters only when every
participant confirmed. Today `drivePrepare` catches a `runCommit` failure and answers
`committed` anyway, so a client can read a stale value from the one participant that
did not apply. This plan changes the answer: when the commit fan-out cannot complete
inside the request's retry budget, the coordinator answers a retryable **in-doubt**
error, and the alarm finishes the commit. `PREPARED` stays final — the in-doubt error
never means cancelled.

What the client does with the in-doubt error:

- With a `clientRequestToken`: retry. The replay reaches the same shard, `runCommit`
  retries the missing participants, and the answer is `committed` once every one
  confirms.
- Without a token: a retry is a new transaction while the first one still commits
  through the alarm. This ambiguity is not new — it is the lost-response case that
  exists today — and a tokenless client that must not double-apply already needs a
  token.

This gating is what makes goal 8 a guarantee instead of a common case: a client that
receives `committed` can read what it wrote on every participant.

#### 4.3.7 Pool size: default from `rootTreesN`, growable per call

- The default becomes `numTxCoordinators = 2 * rootTreesN`, replacing the constant
  100. The multiplier is a placeholder; Open Question 4.4.1 tracks it. The option in
  `FokosDBOptions` stays for explicit sizing.
- `transactWriteItems` accepts a new optional `numTxCoordinators` parameter that
  overrides the instance value for that call. This grows the pool for new transactions
  without a redeploy. `transactGetItems` needs no parameter: read transactions run in
  the Worker and use no coordinator (4.3.4).
- Any integer of 1 or more is accepted. Growth is the intended use, and a smaller
  value is not rejected: the library keeps no record of past per-call values to
  validate against, so the per-token contract below is the only rule.
- **In-flight transactions do not depend on the pool size.** `initiateWrite` stamps
  every lock with the coordinator's DO id string (`this.ctx.id.toString()` →
  `coordinator_do_id`), and the participant alarm resolves it directly with
  `idFromString`. The pool size routes only the initial claim; recovery never hashes
  the token over the pool. A pool size change never re-routes an in-flight
  transaction.
- One assumption is part of the API contract: **idempotency is guaranteed only while
  the same `clientRequestToken` is retried under the same pool size.** A replay with a
  different value routes by `hash(token) % newSize`. That usually lands on a different
  shard, but a same-shard result is possible; neither outcome is guaranteed. When the
  replay lands on a shard without the record, it executes a second time. The library
  cannot detect the violation, because the record lives on a shard the replay does not
  visit — the contract is documented on the parameter, not enforced. Tokenless calls
  can use any value at any time.
- Rationale for the proportional default: a coordinator shard sustains about 1,000
  requests per second (the Durable Objects soft limit), and the write-transaction
  volume a database can accept scales with its partition count, which scales from
  `rootTreesN`. A database with `rootTreesN: 1` gets 2 shards that stay warm. A
  database with `rootTreesN: 100` gets 200.

#### 4.3.8 Performance and storage bounds

Round trips for the common paths. A round trip between Durable Objects is about 1 to
10 ms in one location and 100 to 300 ms across continents (estimates, not
measurements; no location hints exist yet):

| Path | Today | This plan |
| --- | --- | --- |
| Multi-partition write, with or without token | hop + 2 | hop + 2 |
| Multi-partition read transaction | hop + 2 | **2** |
| Single-partition write or read (fast path) | 1 | 1 |
| Token replay of a decided transaction, inside the window | 1 | 1 |

Bytes on the wire drop on every commit by the payload size (up to 4 MB).

Storage per shard splits into two parts:

- **In-flight**: the payloads in `tc_items` and the `tc_participants` rows. A
  participant row carries the serialized partition context, a few hundred bytes each.
  Both are deleted on confirmation, so they last seconds in the normal case.
- **One window of `tc_state` rows.** A committed row is about 200 bytes with the token
  capped at 64 bytes (4.3.3): token, transaction id, operations hash, state, and three
  timestamps. A cancelled row also keeps `rejection_reason_json`, which persists the
  rejecting key; the JSON tagging of a binary key costs about 4 bytes per key byte, so
  a maximum-size binary key (1,024 + 512 bytes) makes a cancelled row of about 7 KB.

At the 1,000 requests-per-second ceiling and a 10-minute window: 600,000 rows, about
120 MB when every transaction commits, and about 4 GB in the pathological case where
every transaction cancels on a maximum-size binary key. The pathological case stays
under the 10 GB limit, and it needs ten straight minutes of ceiling-rate cancellations
on maximum keys. A typical shard holds far less.

#### 4.3.9 Deployment, migration, rollback

- Schema changes ship through `SQLSchemaMigrations`: `tc_state.completed_at`, and
  nullable payload columns on `tc_items`.
- **This is a breaking release. Existing databases are deleted and recreated, not
  migrated.** The default pool size change can route a token to a different shard, so
  a replay against a pre-upgrade database could execute twice. No compatibility path
  is kept; `destroy()` removes the old data.
- Keys-only commit changes `CommitRequest.items` to `TransactionItemKey[]` (4.3.6).
  Participant behavior is unchanged, and the release is breaking anyway.
- `destroy()` sweeps the pool up to the instance's configured `numTxCoordinators`. A
  deployment that passed a larger per-call value must set the instance option to the
  largest value it ever used before it calls `destroy()`, or the extra shards keep
  their idempotency rows.
- `destroy()` must iterate the coordinator shards in batches of fewer than 1,000. A
  single invocation is capped at 1,000 subrequests ([Workers
  limits](https://developers.cloudflare.com/workers/platform/limits/)), and the pool
  can hold up to `2 * 65,000` shards. Today's `staticShardedTCs.all()` fans out to
  every shard at once and breaks past the cap.
- **TODO(cleanup), independent of this plan:** the partition traversal in `destroy()`
  (`traverseForDestroy`) has the same exposure for a table with more than about 1,000
  partitions. It predates this plan; batch it the same way.
- Rollback: garbage collection is additive and can be disabled. Stripped or deleted
  rows are not restored, which only widens the window in which a replay re-executes.

#### 4.3.10 Testing

Tests run in the real Workers runtime (`@cloudflare/vitest-pool-workers`). The existing
`test/transactions.test.ts` suite must pass unchanged after every milestone.

Garbage collection:

- The payload is NULL in `tc_items` after `PREPARED`, and the commit still applies the
  correct data on every participant.
- A stalled `PREPARING` transaction is re-driven by the alarm and commits, with its
  payload intact.
- Rows are deleted only after every participant confirmed, including a participant
  that forwarded locks to split children.
- A token replay inside the window returns the recorded outcome after the
  per-transaction rows are deleted.
- A token replay after the window executes a new transaction.
- A token replay with different operations throws inside the window.
- An idle shard sweeps its last completed rows with no further traffic: the alarm
  re-arms until the window passes (4.3.3).
- A `clientRequestToken` longer than `MAX_CLIENT_REQUEST_TOKEN_BYTES` throws at the
  client boundary, before any coordinator RPC (4.3.3).

Lock-age guard and split-parent copies:

- A commit through a split parent deletes the parent's local copies for each child
  that confirmed, and keeps the copies owned by a still-migrating child. The committed
  data applies on that child after its migration completes.
- A split stalled past the window: the parent's leftover copy resolves through the
  silent `not_found` cleanup (4.3.5 case 2) — no operational error, and the alarm
  stops poking for it afterwards.
- The operational error fires only for a `not_found` on a lock whose key the partition
  owns, older than the window, and the lock is not released.
- A failed poke (coordinator unreachable) retries and never raises the error.
- The guard log line carries every field of 4.3.5, and `debugForceResolveTransaction`
  resolves the lock through the public paths.

Reads in the Worker:

- The double read aborts on a pending write and on a `lastCommittedTs` change, as on
  the coordinator today.

Commit with keys, and the gated answer:

- A multi-megabyte transaction commits with commit RPCs that carry no payload.
- A commit with one unreachable participant answers the retryable in-doubt error,
  never `committed`. After the participant returns, a token replay answers
  `committed`, and a `getItem` on every written key returns the new value (4.3.6).

Pool size:

- An in-flight transaction recovers after the pool size changes: the stale-transaction
  alarm reaches the coordinator through the stored DO id, with any pool size in
  effect.
- A token replay with the same pool size returns the recorded outcome. A replay with a
  different pool size that re-routes the token executes a new transaction — the
  contract of 4.3.7 holds only under the same value. The test picks a token and value
  pair that re-routes.

### 4.4 Open Questions

#### 4.4.1 The pool multiplier

`2 * rootTreesN` is a placeholder. Options: 1x, 2x, 3x, or a measured formula from
coordinator throughput. The answer changes only the default; the mechanism is the
same. TODO: measure coordinator shard throughput under transaction load.

#### 4.4.2 Pool growth after partition splits

The pool is sized from `rootTreesN` at creation, but the partition count grows through
splits. A long-lived database can hold many more partitions than root trees, so its
write throughput scales past a pool that stays at the initial size. Options: keep the
per-call `numTxCoordinators` override as the manual lever, or scale the pool from
measured database throughput and the post-split partition count. The answer decides
whether automatic pool scaling (out of scope, section 2) becomes its own plan.

---

## 5. Alternative Options

**Stateless coordinator with a ledger table.** The rejected predecessor,
`docs/agent-plans/2026-08-29-stateless-transaction-coordination.md`. The driver moves
into the Worker and the decision moves into a second FokosDB instance. Rejected: a
Worker has no durable storage, so the decision costs one network round trip, and a
token write grows from hop + 2 to claim + 3 round trips. The predecessor document
keeps the full correctness analysis.

**Answer at the decision point, resolve locks on touch.** Percolator-style lazy
commit: answer `committed` when the decision is durable, run the commit fan-out after
the answer, and make every read or write that touches a still-locked item resolve the
lock through a ledger read. Reaches 2 round trips for a tokenless write. Rejected: the
read path gains a network dependency, with its tail latency and its availability
coupled to the ledger. The partition must answer reads from its own state alone.

**One fixed large pool (for example 4,096 shards).** Makes resize unnecessary through
overprovisioning. Rejected: FokosDB serves millions of instances and most are small. A
small database spreads its rare transactions across thousands of cold shards and pays
a cold start on almost every transaction. The pool must be proportional to the
database.

**Time-windowed coordinators.** Name shards by (shard, time window) and drop whole DOs
when a window expires. Rejected: a `clientRequestToken` does not carry its window, so
a replay must probe the current and previous windows, and old DOs need a sweeper to
destroy them. Row-level garbage collection gives the same bound with none of that.

**One coordinator Durable Object per transaction.** Perfect isolation and trivially
bounded storage. Rejected: every transaction pays a DO cold start on the happy path,
and every recovery pays one too.

**Async decision mirror.** Not rejected — settled and deferred. See Future extensions.

---

## 6. Future extensions

Not in this plan. Recorded so the design is settled when the need appears.

**Async decision mirror.** Shrinks the down-shard stall (section 2, out of scope) to
the transactions whose mirror write had not landed. A second FokosDB instance holds
one decision row per transaction, keyed by `txId`:

- The coordinator writes the row asynchronously, **after** its local decision is
  durable, best effort. A lost mirror write degrades recovery back to "wait for the
  coordinator" and can never cause a wrong outcome.
- The mirror is a cache, and only a cache. **Absence never decides**: a recovery miss
  means "ask the coordinator, or wait" — never cancel. A hit applies only when
  `row.txId` matches the lock's `transaction_id`. Because absence proves nothing,
  eviction is free: any retention (for example 24 hours) is an operational choice with
  no correctness impact.
- Recovery only. The write path never reads the mirror, and the `clientRequestToken`
  idempotency window stays equal to the coordinator's retention (4.3.3). An extension
  of the window through the mirror would cost one mirror read on every token claim,
  because a new token and a swept token both miss locally.
- A partition reaches the mirror by convention: it derives the mirror's
  `PartitionContext` from its own — same namespace and topology values, a different
  table name under a reserved prefix. `PartitionContextImpl.create` rejects user table
  names with that prefix; the validation ships with the mirror.
- Cost: one extra asynchronous Durable Object write per transaction, and a second
  instance to operate. The mirror grows and splits like any table. It is off the
  critical path, so a splitting mirror partition only delays recovery reads.

The rejected predecessor plan
(`docs/agent-plans/2026-08-29-stateless-transaction-coordination.md`) holds the full
correctness analysis for ledger-style decision rows.

---

## 7. Frequently Asked Questions

**Why must the coordinator keep state? The predecessor removed it.**
The decision must be durable before the commit fan-out, and the client is answered
after the commit. Only a Durable Object writes durably in about 1 ms; a Worker pays a
network round trip. A stateless design therefore costs at least one extra round trip
per write. The 2023 DynamoDB paper's coordinator is stateful for the same reason: its
ledger write is local to the coordinator.

**Is the storage bound real, with a 4 MB payload limit?**
The payload lives on the coordinator only between write-ahead and the end of the
prepare phase — normally well under one second. After that the shard holds keys, then
one `tc_state` row for the window: about 200 bytes committed, up to about 7 KB
cancelled with a maximum binary key in the persisted reason (4.3.8). The bound depends
on the token length limit of 4.3.3; without it, the retained row is unbounded. The
worst cases in 4.3.8 assume the shard runs at its throughput ceiling for the whole
window.

**What happens when a coordinator shard is down?**
Its in-flight transactions stall, and their locks block non-transactional writes to
those keys. The participant alarms retry every `STALE_TX_MS` and wait for the DO to
return. This is today's behavior, unchanged and accepted. The async decision mirror in
Future extensions (section 6) is the mitigation if this becomes a problem in practice.

**What does a client see when a participant is down during the commit?**
A retryable in-doubt error, never `committed` (4.3.6). The decision is durable and the
alarm finishes the commit. A replay with the same `clientRequestToken` answers
`committed` once every participant confirmed. A tokenless retry is a new transaction —
the same ambiguity a lost response gives today.

**Can a participant outage outlast the idempotency window and strand a lock?**
No. The sweep waits for that participant, not the other way around. The timeline: the
transaction decides, the commit RPC to the down participant fails, so its
`tc_participants` row stays unconfirmed, `completed_at` is never set, and the row is
never swept. When the participant returns — after any duration — the coordinator's
retry or the participant's own poke finds the row, gets the decision, applies, and
releases. `not_found` cannot appear in this timeline at any lock age, which is why the
guard's error state (4.3.5 case 3) signals a broken invariant and not a slow outage.

**How does the pool grow, and what does a change affect?**
Pass a larger `numTxCoordinators` on `transactWriteItems` (4.3.7). The value routes
only the initial claim of a new transaction. In-flight transactions are unaffected:
their locks store the coordinator's DO id, and recovery resolves it directly. The API
contract is that idempotency holds only while the same `clientRequestToken` is retried
under the same pool size. A different value can route the token to a shard without the
record — not guaranteed, but never excluded — and the replay then executes a second
time (4.3.7).

**What breaks for existing databases?**
This is a breaking release with no migration path. Existing databases are deleted and
recreated (4.3.9). The pool-size default change is the reason: a different pool can
route a token to a different shard, so a replay against pre-upgrade state could
execute twice.

**Why do read transactions move to the Worker?**
`initiateRead` holds no state and takes no locks, so the coordinator hop buys nothing.
The move saves one round trip on every multi-partition read transaction and removes
the read load from the pool.

**Does garbage collection change what a client observes?**
One thing: the `clientRequestToken` idempotency window becomes finite (10 minutes by
default) instead of accidentally infinite. A replay after the window is a new
execution. DynamoDB documents the same contract.
