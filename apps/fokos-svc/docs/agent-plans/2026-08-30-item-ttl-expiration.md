# RFC — Item TTL expiration in partition Durable Objects

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

---

## 1. Overview and Context

### 1.1 The problem

The `items` table already has a `ttl_epoch_utc_seconds` column. `PartitionStore.upsertItem`,
`PartitionStore.getItem`, `PartitionStore.queryItemsPage` and `PartitionStore.insertItemIfAbsent` all
carry the column. A split copies it to the child partition. Nothing ever expires an item.

Four gaps follow:

1. **The public API rejects TTL.** `FokosDB.putItem` throws `"fokosdb: TTL expiration not yet implemented"`
   when the caller gives `ttlSeconds` or `ttlEpochUTCSeconds`.
2. **A transaction cannot carry a TTL.** `TransactWriteItem` has no TTL field. `TCWriteOperation` has
   none, and the coordinator's `tc_items` table — from which the coordinator rebuilds every prepare —
   has no TTL column. `TransactionItem` has no TTL field. `pending_transactions` has no TTL column.
   `TransactionParticipant` writes `ttlEpochUtcSeconds: null` on both the commit path and the
   single-shot path.
3. **No index finds expired rows.** A sweep would scan the whole table.
4. **No job deletes expired rows.** Storage grows without limit. A Durable Object holds at most 10 GB.

A user who stores session data, a cache entry, or an audit record has no way to bound its lifetime. The
user must delete each item, which costs one write per item.

### 1.2 What the reader must know about the current system

**Two state machines run beside every request.** A child partition in `migration_migrating` does not yet
hold all of its rows or its inherited locks. A parent partition in `split_started` or `split_completed`
owns no key range any more, because the children own it. `AGENTS.md` states the rules that every
operation must follow.

**Background work is one serial chain.** `PartitionDO.runBackgroundWork` runs four jobs in order:
partition migration, partition split, stale transaction recovery, and key promotion with its garbage
collection. An alarm drives it. `PartitionDO.scheduleBackgroundWork` also drives it from a `setTimeout`.

**Two columns decide a transaction conflict.** `items.last_transaction_ts` holds the timestamp of the
last transaction that touched the item. `deletion_metadata.max_deleted_ts` holds the deletion
high-water mark for items that no longer exist. `TransactionParticipant.prepare` rejects a transaction
when the item is absent and `transactionTimestamp <= max_deleted_ts`. `PartitionStore.deleteItem` bumps
the mark on every delete.

**`pending_transactions` holds the 2PC locks.** A row in that table means a transaction owns the item.

**`key_size_estimates` feeds the split and promotion decisions.** Every delete decrements the estimate
for the hash key of the item.

### 1.3 Glossary

- **`ttlAt`** — the one public and wire field that carries the expiry instant: epoch UTC, in seconds.
  Every TypeScript type uses this name, except the store row types that mirror SQL rows
  (`MigratedItem`, `PendingTransactionRow`), which keep the column name `ttl_epoch_utc_seconds` in the
  same way they keep `last_transaction_ts`.
- **Expiry instant** — the value of `items.ttl_epoch_utc_seconds` for one row, in seconds.
- **Expired row** — a row whose expiry instant is at or before the current time.
- **Sweep** — one pass that deletes expired rows.
- **Chunk** — the rows that one sweep deletes in one storage transaction.
- **Cycle** — one run of the sweeper, made of one or more chunks.

---

## 2. Goals and Requirements

### 2.1 In scope

1. `FokosDB.putItem` accepts one field, `ttlAt`, and stores it as the expiry instant.
2. `FokosDB.transactWriteItems` accepts `ttlAt` on a `put` operation, and a committed transactional
   put stores the expiry instant — on the coordinator path and on the single-shot path alike.
3. Stale transaction recovery replays a transactional put with the same expiry instant as the first
   try.
4. A background sweeper deletes expired rows from the partition that owns them.
5. The sweeper deletes at most `chunkSize` rows per storage transaction. It yields between two chunks,
   and it waits `sleepMs` after it deletes `maxRowsBeforeSleep` rows or `maxBytesBeforeSleep` bytes.
6. The sweeper never deletes a row that a pending transaction locks.
7. The sweeper never runs while the partition migrates or after the partition splits.
8. A TTL delete moves `deletion_metadata.max_deleted_ts` forward to the expiry instant of the row.
9. Every public RPC of `PartitionDO` passes through one wrapper method.

### 2.2 Out of scope

- **Read-time filtering.** A read returns an expired row until the sweeper deletes it. Section 4.2.3
  states the contract. Section 5.1 states why.
- **Load gating.** The sweeper does not measure the load of the partition. Section 5.3 states why.
- **An alarm for the sweeper.** Section 4.2.9 states how the timer starts instead.
- **Storage reclaim.** Deleting a row frees a SQLite page for reuse. Whether the page returns to the
  operating system is Open Question 4.3.1.
- **A per-table switch for TTL.** The partition always honours the column, and the configuration of
  section 4.2.11 has no off switch.

### 2.3 Requirements

1. The sweeper must not block a user request. Each chunk runs in one synchronous storage transaction,
   and the sweeper yields to the event loop between two chunks.
2. The sweeper must not exceed the CPU or the memory limit of a Durable Object. It must never read the
   `data` column of an expired row.
3. The sweeper must be crash-safe. A cycle that stops halfway leaves the table correct, and the next
   cycle continues.
4. The sweeper must be idempotent. Two concurrent cycles must not corrupt `key_size_estimates` or
   `deletion_metadata`.
5. The change must not weaken the conflict detection of the transaction protocol.

---

## 3. Milestones

Each milestone ships on its own and leaves the system correct.

**M1 — Schema and the write path.** The schema changes of section 4.2.1, the validation of section
4.2.2, the `ttlAt` field on `TransactWriteItem`, `TCWriteOperation` and `TransactionItem`, the TTL
column on `pending_transactions` and on `tc_items`, and the operations hash. After M1 a user can store
an expiry instant and read it back. Nothing expires yet.

**M2 — The sweeper.** The `TtlExpiry` class of section 4.2.4, its guard conditions, its watermark rule,
the timer that the constructor of `PartitionDO` arms, and the arm on migration completion. After M2 a
partition that wakes deletes its expired rows.

**M3 — The RPC wrapper.** The wrapper of section 4.2.10, and the call to `TtlExpiry.arm` inside it.
After M3 a request arms the timer, so a partition that already runs does not wait for the next wake.

---

## 4. Proposed Solution

### 4.1 High-level overview

An item can carry an expiry instant. The partition that owns the item deletes it after that instant
passes. The partition deletes in small chunks and it yields between two chunks, so a partition with
100,000 expired rows does not stop serving requests.

A timer inside the Durable Object drives the sweeper. The timer has four sources:

1. The constructor of `PartitionDO` arms it 500 ms after the object wakes.
2. The sweeper re-arms it after a cycle that still finds expired rows.
3. Every public RPC arms it, if no timer is pending.
4. The background work arms it after a child migration completes.

The sweeper does not use an alarm. A partition that gets no request does not wake, so it does not
sweep. The item stays on disk until the next request or the next wake. This is the cost of the design,
and section 5.2 states why the design accepts it.

One cycle works like this:

```
 arm(500ms)
     │
     ▼
 ┌─────────────────────────────────────────────────────┐
 │ canSweep()?                                         │
 │   partition context loaded, and                     │──no──▶ stop, do not re-arm
 │   not migrating, and                                │
 │   not split_started / split_completed               │
 └───────────────────────┬─────────────────────────────┘
                         │ yes
                         ▼
 ┌─────────────────────────────────────────────────────┐
 │ chunk, in one transactionSync:                      │
 │   DELETE <= 100 expired rows, oldest first          │
 │     skipping a row a pending transaction locks      │
 │     skipping a row whose hash key is promoted       │
 │   RETURNING hk, est_row_bytes, ttl                  │
 │   decrement key_size_estimates per key              │
 │   bump max_deleted_ts to the largest expiry         │
 └───────────────────────┬─────────────────────────────┘
                         │
       deleted < 100     │      deleted == 100
        ┌────────────────┴────────────────┐
        ▼                                 ▼
   stop, do not re-arm        ┌──────────────────────────────┐
   (a request or the next     │ 10,000 rows or 50 MB since   │
    wake arms the timer)      │ the last sleep?              │
                              └───────┬──────────────┬───────┘
                                  no  │              │ yes
                                      ▼              ▼
                              yield to the      wait 500 ms
                              event loop        reset the budget
                                      │              │
                                      └──────┬───────┘
                                             │
                     rows in cycle < 100,000 ─┴─ yes ──▶ next chunk
                                             │
                                            no
                                             ▼
                                       re-arm(500ms)
```

A cycle deletes at most 100,000 rows, then it re-arms. The cap bounds one continuous run.

The sweep rate is not one chunk per 500 ms. The loop sleeps only after it deletes 10,000 rows or 50 MB
since the last sleep, whichever comes first. Small items therefore drain about 100 times faster than a
sleep per chunk, and large items still hold the rate down to about 50 MB per 500 ms. The chunk itself
stays at 100 rows, so one storage transaction stays short whatever the budget is.

### 4.2 Technical details

#### 4.2.1 Schema changes

The project is before its first release, so the change edits the existing migration entries in
`PartitionStore` in place. It does not add a new entry. An existing partition loses its data.

**Migration 1, the `items` table.** Add a partial index:

```sql
CREATE INDEX IF NOT EXISTS idx_items_ttl ON items (ttl_epoch_utc_seconds, hk, sk)
    WHERE ttl_epoch_utc_seconds IS NOT NULL;
```

The index is partial, so a row with no TTL costs nothing. The index answers two questions with one
seek: "is any row expired", and "which rows expire first". It carries `hk` and `sk`, so the sweeper
reads no table page to find its chunk.

The existing `idx_items_scan` index stays as it is. The sweeper does not use it, and adding
`ttl_epoch_utc_seconds` to it would make every scan of `est_row_bytes` read a wider index.

**Migration 2, the `pending_transactions` table.** Add one column:

```sql
ttl_epoch_utc_seconds INTEGER,
```

The column holds the expiry instant of a prepared put until the transaction commits. It is `NULL` for a
`delete` operation and for a `check` operation, in the same way `data_kind` is.

**Migration 1 of the transaction coordinator, the `tc_items` table.** Add the same column:

```sql
ttl_epoch_utc_seconds INTEGER,
```

The coordinator writes its state ahead of every outbound RPC and rebuilds every prepare — the first
attempt, a retry, and a crash recovery — from `tc_items`. A value that is not in this table does not
survive the coordinator, so the column is as load-bearing as the one on `pending_transactions`.

#### 4.2.2 TTL on the write path

**One field, passed through.** The public API takes one optional field, `ttlAt`: the epoch instant in
seconds at which the item expires. The caller computes the instant; no layer reads a clock on its
behalf. DynamoDB works the same way — the TTL attribute is a number the caller wrote, and the service
only ever compares it. The value of a request is therefore a pure function of what the caller wrote:
a retry carries the same bytes as the first attempt. Sections 5.6 and 5.7 state what the two dropped
conveniences would have cost.

**Validation.** `db.ts` checks the field once, at the public boundary, for `putItem` and for a `put`
inside `transactWriteItems`:

1. `ttlAt` must be an integer. A non-integer throws.
2. `ttlAt` must be greater than zero. Zero or a negative value throws.

A `ttlAt` at or before the current time is valid. The item is expired the moment it lands, a read
returns it until the sweeper deletes it (section 4.2.3), and the next cycle deletes it. DynamoDB
accepts a past timestamp too, and section 5.6 states why rejecting one is worse.

**A put with no TTL clears the TTL.** `PartitionStore.upsertItem` already writes
`ttl_epoch_utc_seconds = excluded.ttl_epoch_utc_seconds`. A put replaces the whole item, so it also
replaces the expiry instant. A caller that wants to keep the TTL must send it again.

**The single put path.** `db.ts` sends `ttlAt` on the `apiPutItem` request, and `PartitionDO` passes
it to `upsertItem`. The wire request and the store already carry the value under the name
`ttlEpochUTCSeconds`; the field renames to `ttlAt` (glossary, section 1.3) and the throw at the
boundary goes away.

**The transaction path carries the value through the coordinator:**

1. `TransactWriteItem`, the `put` variant, gets `ttlAt`. `db.ts` validates it and copies it onto the
   `TCWriteOperation`.
2. `TransactionCoordinatorDO.initiateWrite` persists the value into the new `tc_items` column of
   section 4.2.1.
3. `hashTransactionOperations` folds `ttlAt` into the operations hash. Two requests that differ only
   in `ttlAt` are different transactions, and an idempotent replay still hashes the same because the
   value is deterministic.
4. `toTransactionItems` copies the column onto `TransactionItem.ttlAt`, so the coordinator's prepare
   and the single-shot path carry the same shape.
5. `PartitionStore.insertPendingLock` writes the value into the new column of `pending_transactions`.
   `PendingTransactionRow` gets the field.
6. `TransactionParticipant` passes the value to `upsertItem` on commit. The commit path reads it from
   the pending row — `getPendingTxOp` gets the column — and never from the request items, which only
   name the keys. The single-shot path reads `item.ttlAt`, because it holds no pending row.

**Recovery and split inheritance carry the value for free, and each needs a test.** Stale transaction
recovery rebuilds a `TransactionItem[]` and calls `txCommit`, and commit reads the TTL from the
pending row, so recovery needs no copy of its own. A split child inherits pending locks through
`queryPendingTxPage`, whose rows are `PendingTransactionRow` — the compiler enforces that column. But
`getPendingTxOp` and `listPendingTxItems` return their own inline row types, which the compiler does
not connect to the schema, so tests 17 and 19 of section 4.2.15 hold both paths.

#### 4.2.3 Read behaviour

A read returns an expired row as an ordinary item, until the sweeper deletes it. This holds for
`getItem`, `queryItems`, `transactGetItems`, a condition of type `item_exists` or `item_not_exists`, and
the timestamp check inside `prepare`.

The caller filters. Every read result already carries the value — `GetItemResult`,
`QueryItemsResult`, and the read result of a transaction — and the field renames to `ttlAt` there
too. The public documentation of `ttlAt` must state the contract.

#### 4.2.4 The `TtlExpiry` class

A new file `src/lib/partition/ttl-expiry.ts` holds the class. The class owns the timer and the sweep
loop. It holds no Durable Object state and no stub. `PartitionDO` builds it in the constructor and
wires the dependencies, in the same way it builds `PromotionManager`.

```ts
export type TtlExpiryDeps = {
    store: PartitionStore;
    /** False while the partition migrates, after it splits, or before it loads its context. */
    canSweep: () => boolean;
    logParams: () => Record<string, unknown>;
    /** Read late, so a subclass of PartitionDO can override the values. */
    config: () => TtlSweepConfig;
    now?: () => number;
    /** The yield and the sleep of section 4.2.6. Defaults to scheduler.wait; a test injects a counter. */
    wait?: (ms: number) => Promise<void>;
};

export class TtlExpiry {
    /** Arms the timer when no timer is pending. Cheap enough for every RPC. */
    arm(delayMs?: number): void;
    /** Clears a pending timer. */
    disarm(): void;
    /** True while a timer is pending. */
    get armed(): boolean;
    /** One cycle, without a timer. The timer callback and the tests both call it. */
    runCycle(): Promise<{ deletedRows: number; deletedBytes: number; more: boolean }>;
}
```

The class keeps a private `#running` flag. `runCycle` returns at once when a cycle already runs, so two
timers cannot delete the same chunk twice.

**One new method on `PartitionStore`:**

```ts
/**
 * Deletes at most `limit` expired rows, oldest expiry first, and keeps key_size_estimates and
 * deletion_metadata consistent. One storage transaction. Never reads the `data` column.
 */
deleteExpiredItems(nowSeconds: number, limit: number): { deletedRows: number; deletedBytes: number };
```

`TtlExpiry` passes a row count and gets back a summary. It never sees a key. Both filters live in SQL,
so no key crosses the boundary in either direction.

The method runs inside `PartitionStore.transactionSync`. It starts with one statement:

```sql
DELETE FROM items
 WHERE rowid IN (
     SELECT i.rowid FROM items i INDEXED BY idx_items_ttl
      WHERE i.ttl_epoch_utc_seconds IS NOT NULL
        AND i.ttl_epoch_utc_seconds <= ?1
        AND NOT EXISTS (SELECT 1 FROM pending_transactions p WHERE p.hk = i.hk AND p.sk = i.sk)
        AND NOT EXISTS (SELECT 1 FROM promoted_keys pk WHERE pk.hash_key = i.hk)
      ORDER BY i.ttl_epoch_utc_seconds, i.hk, i.sk
      LIMIT ?2
 )
RETURNING hk, est_row_bytes, ttl_epoch_utc_seconds
```

Five parts of the statement matter.

1. **The two `NOT EXISTS` clauses are the filters.** The first seeks the primary key autoindex of
   `pending_transactions`, which is `(hk, sk, transaction_id)`. The second seeks the primary key of
   `promoted_keys`, which is `hash_key` on a `WITHOUT ROWID` table. Both sit in the `WHERE` clause, so
   `LIMIT` counts only rows the sweeper can delete. A locked row or a promoted key never blocks the
   chunk, and it never starves the rows behind it.
2. **The `ORDER BY` is a total order.** `ORDER BY i.ttl_epoch_utc_seconds` alone breaks a tie
   arbitrarily. Adding `i.hk, i.sk` makes the chunk deterministic, and `idx_items_ttl` serves the order
   with no sort step.
3. **`INDEXED BY` pins the plan**, in the same way `#storedEstRowBytes` pins `idx_items_scan`. Section
   4.2.5 gives the measured plan. Without the pin, a later change that stops the planner from matching
   the partial index turns this statement into a full scan of `items`, which is silent and costs the
   whole table on every chunk. With the pin, the same change fails the statement loudly.
4. **`RETURNING` replaces a second pass.** `PartitionStore` already uses `RETURNING` in `upsertItem`,
   and the note above `#storedEstRowBytes` records the measurement for the delete form: SQLite reports
   the returned row as one extra read. One statement that returns the rows costs less than a `SELECT`
   pass followed by a `DELETE` pass, which reads every row twice.
5. **It never reads `data`.** `est_row_bytes` is a plain column, and the returned columns are two
   integers and one key per row.

#### 4.2.5 The measured query plan

`EXPLAIN QUERY PLAN` over the real schema in workerd, with 500 rows in `items`, gives:

```
SEARCH items USING INTEGER PRIMARY KEY (rowid=?)
LIST SUBQUERY 3
  SEARCH i USING COVERING INDEX idx_items_ttl (ttl_epoch_utc_seconds>? AND ttl_epoch_utc_seconds<?)
  CORRELATED SCALAR SUBQUERY 1
    SEARCH p USING COVERING INDEX sqlite_autoindex_pending_transactions_1 (hk=? AND sk=?)
  CORRELATED SCALAR SUBQUERY 2
    SEARCH pk USING PRIMARY KEY (hash_key=?)
CREATE BLOOM FILTER
```

Five facts follow, and each one is a requirement the query-plan test of section 4.2.15 holds:

1. **The victim scan is covering.** `idx_items_ttl` carries `ttl_epoch_utc_seconds`, `hk`, `sk` and the
   implicit rowid, which is every column the inner query needs. Picking a chunk reads no table page.
2. **No sort step appears.** There is no `USE TEMP B-TREE FOR ORDER BY` line, so the index serves the
   `ORDER BY`, and `LIMIT` stops the scan at the first `chunkSize` rows that pass the filters. The scan
   does not visit every expired row in the partition.
3. **Both filters are seeks**, one on each of the two indexes named above. Neither is a scan.
4. **`LIST SUBQUERY` means the victim set is materialized** before the delete starts. The statement
   does not delete from a B-tree that it still reads.
5. **The plan does not depend on statistics.** The same plan appears with and without `ANALYZE`, which
   matters because a production Durable Object never runs `ANALYZE`.

The planner also matches the partial index when the query omits `ttl_epoch_utc_seconds IS NOT NULL`,
because it proves that `x <= ?` excludes NULL. The statement keeps the predicate anyway. It states the
`WHERE` clause of the partial index word for word, so the match does not rest on the prover, and the
extra lower bound costs one comparison per seek.

The method then folds the returned rows, inside the same transaction:

1. Sum `est_row_bytes` per distinct `hk`, then run
   `UPDATE key_size_estimates SET est_bytes = MAX(0, est_bytes - ?) WHERE hk = ?` once per key. This
   mirrors `PartitionStore.deleteItem`.
2. `bumpMaxDeletedTs(maxExpirySeconds * 1000)`, where `maxExpirySeconds` is the largest
   `ttl_epoch_utc_seconds` of the chunk. Section 4.2.8 states why.
3. Return the row count and the sum of `est_row_bytes`, which is the byte budget of section 4.2.6.

The fold is inside `PartitionStore`, over at most `chunkSize` rows. It is not a round trip: the rows
arrive as the output of the statement that deleted them, and no key returns to SQLite except the
distinct keys that the estimate update needs.

The sweeper deletes the oldest expiry first. A large backlog drains in the order the rows expired.

#### 4.2.6 The chunk loop and the sleep budget

A chunk is small so that one storage transaction stays short. Sleeping after every chunk is too slow
when the items are small: 100 rows per 500 ms drains 12,000 rows per minute, whatever the size of a
row.

The loop separates the two concerns. It runs a chunk, then it yields. It sleeps only after the work
since the last sleep reaches a budget:

```
runCycle():
  rowsInCycle = 0, rowsSinceSleep = 0, bytesSinceSleep = 0
  loop:
    if not canSweep():                 return { more: false }
    if rowsInCycle >= maxRowsPerCycle: return { more: true }

    res = store.deleteExpiredItems(nowSeconds(), chunkSize)
    if res.deletedRows == 0:           return { more: false }

    rowsInCycle    += res.deletedRows
    rowsSinceSleep += res.deletedRows
    bytesSinceSleep += res.deletedBytes

    if res.deletedRows < chunkSize:    return { more: false }

    if rowsSinceSleep >= maxRowsBeforeSleep or bytesSinceSleep >= maxBytesBeforeSleep:
        await wait(sleepMs)
        rowsSinceSleep = 0, bytesSinceSleep = 0
    else:
        await wait(0)
```

Five rules follow from the loop.

**`canSweep` runs before every chunk, not once per cycle.** A split can start while a cycle runs. The
check reads two storage KV keys, which the Durable Object holds in memory, so the cost per chunk is
small.

**The loop yields between two chunks even when it does not sleep.** A chunk is synchronous, so a run of
100 chunks with no `await` would hold the isolate for the whole run. The yield lets a queued request
run. The sweeper never holds the input gate across a chunk.

**Both the yield and the sleep are `scheduler.wait`.** The yield must be a macrotask, because a
queued request is an event, not a microtask: `await Promise.resolve()` drains only the microtask
queue and lets no request run. `scheduler.wait(0)` is the platform's macrotask yield, and
`scheduler.wait(sleepMs)` is the sleep, so the loop has one waiting primitive. The loop reaches it
through the `wait` dependency of section 4.2.4, which defaults to `scheduler.wait`, so a test counts
the sleeps without a fake timer.

**A partial chunk ends the cycle.** `deletedRows < chunkSize` means no more row matches the filters
now. The rows that stay are locked or promoted, and the sweeper leaves them to the next cycle. A commit
releases a lock and a commit is a write, so the RPC wrapper of section 4.2.10 arms the timer again.

**The cycle returns `more: true` only at the cap.** The timer callback re-arms in that case, and it
stops otherwise. The cap bounds one continuous run and gives the other work of the partition a turn.

#### 4.2.7 Guard conditions

`canSweep` returns false in three cases. `PartitionDO` supplies the function.

1. **No partition context.** The constructor loads the context from storage inside
   `blockConcurrencyWhile`, and a hash partition can also load it lazily on the first request. A
   partition with rows always has a stored context, so a partition with no context has nothing to
   sweep. The sweeper stops and does not re-arm.
2. **The partition migrates.** `SPLIT_MIGRATION_STATUS` is `migration_initialized` or
   `migration_migrating`. The child does not hold all of its rows, and it does not hold its inherited
   locks, so the lock guard of section 4.2.4 cannot see a lock that has not arrived. The migration also
   ends with `PartitionStore.rebuildKeySizeEstimates`, which would lose the decrements of a concurrent
   sweep. The sweeper stops and does not re-arm itself — `runCycle` returns the same `more: false` for
   every guard, so the timer callback cannot tell this case apart. The re-arm comes from outside:
   `runBackgroundWork` arms the timer after the migration job writes `migration_completed` (section
   4.2.9, source 4), so the child sweeps the expired rows it inherited without waiting for a request.
3. **The partition split.** The split status is `split_started` or `split_completed`. Section 4.2.12
   states why.

`TtlExpiry` calls `canSweep` before every chunk, and not once per cycle, because a split can start while
a cycle runs.

A promoted hash key is not a case here. The `NOT EXISTS` clause on `promoted_keys` in section 4.2.4
excludes those rows per row, so one promoted key does not stop the sweep of the whole partition.

#### 4.2.8 The deletion watermark

A TTL delete bumps `deletion_metadata.max_deleted_ts` to `expiry_instant * 1000`, and not to
`Date.now()`. The choice has two reasons.

**It does not abort a live transaction.** `TransactionParticipant.prepare` rejects a transaction when
the item is absent and `transactionTimestamp <= max_deleted_ts`. A bump to `Date.now()` on every chunk
would reject every transaction that started a few milliseconds earlier. A bump to the expiry instant
rejects only a transaction that is older than the death of the item, which is correct.

**It converges across partitions.** The contribution of one row to the watermark is a function of the
row alone. A parent and a child that both hold a copy of the same expired row compute the same value.
A bump to `Date.now()` would make the two watermarks differ forever.

#### 4.2.9 The timer lifecycle

The timer is a private field of the `TtlExpiry` instance, and that instance is a private field of the
`PartitionDO` instance. It must not be a `static` field. Several Durable Object instances of one class
can share one isolate, so a static handle would let one partition clear the timer of another.

Four sources arm the timer:

1. **The constructor.** `PartitionDO` calls `arm(500)` after it builds `TtlExpiry`. The call is outside
   `blockConcurrencyWhile`, and it follows the pattern the constructor already uses for the colo
   lookup.
2. **The sweeper.** `runCycle` reports `more: true`, so the timer callback re-arms.
3. **A request.** The RPC wrapper of section 4.2.10 calls `arm()`.
4. **Migration completion.** `runBackgroundWork` calls `arm()` after the migration job writes
   `migration_completed`. A child inherits the expired rows of its parent, migration is background
   work and not a wrapped RPC, and nothing else wakes a child that gets no traffic — without this
   source the guard of section 4.2.7 case 2 would hold those rows on disk until the first request.

`arm` returns at once when a timer is already pending, so the cost per RPC is one null check.

**The timer callback never throws.** It wraps `runCycle` in a try/catch, logs the error with
`logParams`, and does not re-arm. An uncaught throw in a `setTimeout` callback is an unhandled
rejection that carries no partition context into the log. `runCycle` itself throws to its caller, so
a direct call in a test sees the failure — only the timer callback swallows it. A transient error
heals on the next arm, from any of the four sources. A persistent error, such as a schema change that
breaks the pinned plan of section 4.2.4 or an invalid configuration of section 4.2.11, costs one
failed chunk per arm instead of a hot loop, and every failure is one log line. Test 22 forces this
path, so a broken chunk fails a local run and not production.

**The sweeper does not arm a timer for a future expiry.** A cycle that finds no expired row stops. A
partition whose next expiry is one hour away holds no timer for that hour. The next request arms the
timer, or the constructor arms it at the next wake. The design accepts a delay for a partition that
gets no traffic, and it does not hold an idle Durable Object alive for an hour.

#### 4.2.10 The RPC wrapper

Every public method of `PartitionDO` passes through one private helper:

```ts
async #rpc<T>(name: string, fn: () => Promise<T>): Promise<T> {
    this.#ttl.arm();
    return await fn();
}
```

The wrapper covers the `api*` methods, the `internal*` methods, the `tx*` methods, the `migration*`
methods, `status` and `destroyPartition`. It does not cover `alarm`, which is a handler and not an RPC.

Three rules hold for the wrapper:

1. **It must rethrow the error unchanged.** Callers match on the identity of an error: the
   migration error that makes a client retry, `errSinglePartitionFastPathFallback`,
   `DESTROY_ABORT_SENTINEL`, and the retry predicate of `tryWhile` inside `runBackgroundWork`. The
   wrapper can log, and it must not wrap or normalise.
2. **It must be an explicit call, not a decorator and not a `Proxy`.** `tsconfig.json` does not set
   `experimentalDecorators`, and the emit of a standard decorator through the build is not proven here.
   A `Proxy` around the instance would sit between Workers RPC and its own property access. An explicit
   helper is greppable and changes no signature, so the structural type `PartitionDOStub` stays as it
   is.
3. **It carries the `name` argument from the start.** The wrapper is the seam for the canonical log
   line, the operation metrics, and the count of requests in flight that section 5.3 defers. Those come
   later, and the argument keeps the call sites correct now.

Arming the timer inside a read RPC is deliberate. A partition that serves only reads arms the timer,
the sweeper finds nothing, and it stops without a re-arm. The cost is one index seek per 500 ms while
the traffic runs.

#### 4.2.11 Configuration

One `protected` method on `PartitionDO` returns the whole configuration. A subclass overrides that one
method:

```ts
export type TtlSweepConfig = {
    /** Rows per DELETE statement, and per storage transaction. */
    chunkSize: number;
    /** The wait after the loop reaches a budget below. */
    sleepMs: number;
    /** Rows deleted since the last sleep that trigger a sleep. */
    maxRowsBeforeSleep: number;
    /** Bytes deleted since the last sleep that trigger a sleep, measured as est_row_bytes. */
    maxBytesBeforeSleep: number;
    /** Rows in one cycle, after which the cycle re-arms the timer and returns. */
    maxRowsPerCycle: number;
    /** The delay the constructor of PartitionDO passes to arm. */
    initialDelayMs: number;
};

protected ttlConfig(): TtlSweepConfig {
    return {
        chunkSize: 100,
        sleepMs: 500,
        maxRowsBeforeSleep: 10_000,
        maxBytesBeforeSleep: 50 * 1024 * 1024,
        maxRowsPerCycle: 100_000,
        initialDelayMs: 500,
    };
}
```

It is a method, and not a field with an initializer. A subclass assigns a field after `super()` returns,
so the base constructor would read the value of the base class. A method lives on the prototype, so an
override works from the constructor onward. `TtlExpiry` calls it through the `config` callback on every
cycle, which also keeps an override correct after construction.

**`runCycle` validates the configuration before its first chunk.** `chunkSize`, `maxRowsPerCycle`,
`maxRowsBeforeSleep` and `maxBytesBeforeSleep` must be integers greater than zero. `sleepMs` and
`initialDelayMs` must be integers of at least zero, so a test can pass `sleepMs: 0`. An invalid
configuration throws. Without the check, `maxRowsPerCycle: 0` would make every cycle return
`more: true` before its first chunk, and the timer would re-arm itself every 500 ms forever on a
sweeper that never sweeps — a busy loop that holds the Durable Object awake for nothing. There is no
off switch: the partition always honours the column (section 2.2).

#### 4.2.12 Concurrency with split and migration

**A split parent must not sweep.** This is the rule that keeps the transaction protocol correct.

`SplitMigration.runHashChildMigration` copies the rows first, and it pulls `maxDeletedTs` from the
parent after that. Take an expired row that belongs to the child. When the parent deletes the row
before the cursor of the child reaches it, the child never receives the row. The child never bumps its
own watermark for that row. The parent moved its watermark forward and the child did not. The child
then serves with a watermark that is too small, and `TransactionParticipant.prepare` stops rejecting a
transaction that it must reject.

The rule removes the case: a parent at `split_started` or `split_completed` owns no key range, so the
children sweep their own rows. The rule also matches the routing rule of `AGENTS.md`. It applies to a
range partition that became a router in the same way.

**A sweep before the split and a sweep after it land the same value.** The parent may sweep at
`split_queued`, before any child pulls a row — a premature pull cannot happen, because the parent's
batch RPCs assert `split_started` or later. A row the parent sweeps then never reaches the child, but
its bump does: the child pulls `maxDeletedTs` from the parent, and the parent's watermark already
carries `row.ttl`. A row the parent does not sweep reaches the child, and the child bumps its own
watermark to the same `row.ttl` when it sweeps its copy later. Either order lands the same value,
because section 4.2.8 makes the bump a function of the row.

**Cursor paging is safe under a concurrent delete.** `PartitionStore.queryItemsPage` and
`PartitionStore.queryRangeItemsPage` resume with a row-value comparison on `(hk, sk)`, and the child
checkpoints the cursor in storage. A delete behind the cursor changes nothing. A delete ahead of the
cursor means the child never copies the row, which is the wanted result. No offset arithmetic exists to
corrupt.

**Migration copies an expired row.** The migration read does not filter on the TTL. The child receives
the expired row and sweeps it with its own cycle. The behaviour is self-healing, and it keeps the byte
budget of `collectBatch` unchanged.

**A promotion is not a split.** A hash partition that promotes one key stays outside both split states,
and the range root pulls the rows of that key through `migrationGetItemsBatchForRange`. The `NOT EXISTS`
clause on `promoted_keys` in section 4.2.4 covers it. `migrationGetItemsBatch` excludes the same rows
for the same reason, through the in-memory `PromotionManager.hasStatus`. The sweeper reads the table
instead, because it runs in the background and does not pay for the hot routing path.

**A split parent never reclaims its rows.** `migrationAcknowledgeChildComplete` deletes only
`pending_transactions`. The parent keeps its `items` rows as a fallback for a child with a late
migration job. With this rule the parent does not sweep those rows either. Nothing reclaims them today,
so the behaviour is unchanged, and TTL does not bound the storage of a split parent.

#### 4.2.13 Performance

**The chunk.** `idx_items_ttl` gives the rows in expiry order and carries `hk` and `sk`, so the victim
scan reads no table page. The two `NOT EXISTS` clauses cost two index seeks per candidate row, so about
200 seeks per chunk of 100. `RETURNING` bills one extra read per returned row, as the note above
`#storedEstRowBytes` records. The statement never reads the `data` column, so one chunk holds about 100
rows of one key and two integers in memory.

**The bookkeeping.** One `UPDATE` per distinct hash key in the chunk, plus one `UPDATE` on
`deletion_metadata`. A chunk of 100 rows under one hash key costs one `UPDATE`; a chunk of 100 rows
under 100 keys costs 100. All of it runs in the same synchronous storage transaction as the `DELETE`.

**The rate.** The loop sleeps 500 ms per 10,000 rows or per 50 MB, whichever comes first. That is about
20,000 rows per second for small items, and about 100 MB per second for large items. A cycle of 100,000
rows therefore takes about 5 seconds of wall time for small items. The Durable Object serves requests
during each sleep and each yield, because the sweeper never holds the input gate across a chunk.

`TODO: measure` the CPU cost of one chunk, and the wall time of a cycle over 100,000 expired rows.

#### 4.2.14 Deployment and rollback

The schema change edits migration 1 and migration 2 in place, so an existing partition loses its data.
The project is before its first release and accepts the break. A deployment must destroy the existing
Durable Object namespaces.

To roll back, revert the change and destroy the namespaces again.

#### 4.2.15 Testing

The tests use no mock and no fake timer. `TtlExpiry` takes a `PartitionStore` and plain functions, so
a test builds the real class over real storage and drives it.

`partition-store.test.ts` already has the harness: `withStore` opens a real `PartitionStore` over real
Durable Object storage through `runInDurableObject`. The tests for `TtlExpiry` use the same harness,
pass `sleepMs: 0` through the `config` callback, and call `runCycle` directly. No test waits on a
timer, except tests 22 and 23, which exercise the timer path and arm with a zero delay.

The cases:

0. A query-plan test runs `EXPLAIN QUERY PLAN` over the delete statement and asserts the five facts of
   section 4.2.5: the victim scan uses `idx_items_ttl` as a covering index, no `USE TEMP B-TREE FOR
   ORDER BY` line appears, each `NOT EXISTS` is a `SEARCH` and not a `SCAN`, the victim set is a
   `LIST SUBQUERY`, and the plan holds with no `ANALYZE`. The test also drops the `INDEXED BY` pin and
   asserts the plan is still the same, so the pin can be removed when it stops earning its place. This
   follows the query-plan test that already guards `idx_items_scan`.
1. `deleteExpiredItems` deletes the oldest expiry first, and it keeps a row with no TTL.
2. `deleteExpiredItems` keeps a row that a pending transaction locks, and it still fills the chunk from
   the rows behind it.
3. `deleteExpiredItems` keeps a row whose hash key has a `promoted_keys` entry, at each of the three
   statuses, and it still fills the chunk from the rows behind it.
4. `deleteExpiredItems` decrements `key_size_estimates` by the sum of `est_row_bytes` per hash key, over
   a chunk that holds rows of several keys.
5. `deleteExpiredItems` bumps `max_deleted_ts` to the largest expiry of the chunk, in milliseconds, and
   it never lowers the value.
6. `deleteExpiredItems` reports `deletedBytes` as the sum of `est_row_bytes` of the deleted rows.
7. Two calls to `deleteExpiredItems` with the same arguments delete two different sets of rows, which
   proves the total order of the `ORDER BY` and that no row is deleted twice.
8. `runCycle` drains a backlog larger than one chunk, and it reports `more: true` at `maxRowsPerCycle`.
9. `runCycle` reports `more: false` when the last chunk is partial, and when no row is expired.
10. `runCycle` sleeps once per `maxRowsBeforeSleep` rows, and once per `maxBytesBeforeSleep` bytes for
    large items. The test counts the sleeps through the `wait` dependency.
11. `runCycle` returns at once when a cycle already runs.
12. `canSweep` returning false stops the cycle before the first chunk, and it also stops a cycle that
    already runs, before the next chunk.
13. `arm` sets one timer, and a second call while the timer is pending does nothing.
14. `putItem` with `ttlAt` stores the instant, and a put with no TTL clears it.
15. `putItem` throws for a `ttlAt` that is not an integer, and for zero or a negative value. A `ttlAt`
    in the past stores, and the next cycle deletes the row.
16. `transactWriteItems` with a `ttlAt` stores the instant after the commit, on the single-shot path
    and on the coordinator path.
17. A recovered transaction stores the same instant as the first try.
18. An end-to-end test writes an item with a `ttlAt` one second ahead, waits, and finds the row
    deleted.
19. A transactional put with a `ttlAt` prepared before a hash split commits after the split, and the
    child stores the instant. This holds the `queryPendingTxPage` lock-inheritance path.
20. A `transactWriteItems` replay with the same `clientRequestToken` and the same `ttlAt` returns the
    stored outcome. A replay with a different `ttlAt` is rejected as a different operation set.
21. `runCycle` throws for a configuration whose `maxRowsPerCycle` or `chunkSize` is zero, negative, or
    not an integer, before it deletes anything.
22. A chunk that throws — forced through a `config` callback that throws — is caught by the timer
    callback, the error is logged, and the timer stays disarmed. The same failure through a direct
    `runCycle` call rethrows.
23. A child that completes migration arms the timer, and the expired rows it inherited are deleted
    without a request.

### 4.3 Open Questions

#### 4.3.1 Does Durable Object SQLite return the freed pages?

A large delete frees SQLite pages. Whether the pages leave the file, or stay in the free list, decides
two things: whether TTL bounds the billed storage of a partition, and whether a sweep can lower
`sql.databaseSize` enough to hold off a split. `PartitionStore.databaseSize` reads `sql.databaseSize`,
and `maybeQueueSplit` decides on that value.

A unit test against workerd answers it: write rows, read `databaseSize`, delete the rows, read
`databaseSize` again. The answer does not block M1 or M2.

---

## 5. Alternative Options

### 5.1 Filter the expired rows at read time

Every read path treats a row as absent when its expiry instant has passed, whatever is on disk. The
sweeper becomes a storage job with no effect on the answer of a read.

It is rejected for now. It touches `getItem`, `getItemStamp`, `queryItemsForHashKey`, the transaction
read paths and `hasItemsForHashKey`, and it puts a predicate on every hot read. The predicate also
sends the query scan back to the table, because `idx_items_scan` does not carry the TTL column. The
sweeper delivers the value without the read cost, and DynamoDB has the weaker contract too.

The cost of the choice is in section 4.2.3: an expired item is alive until the sweeper deletes it.

### 5.2 Drive the sweeper from the alarm

`runBackgroundWork` already computes the next alarm in its `finally` block, and one more reason in that
computation would wake a partition with no traffic.

It is rejected. It also puts the sweep inside the serial chain of `runBackgroundWork`, where the waits
of 500 ms per chunk would hold back partition migration, partition split and stale transaction
recovery for about 24 seconds. Keeping an alarm for a partition with no traffic is the only gain, and
the value of that gain depends on Open Question 4.3.1. The design can add it later without a change to
`TtlExpiry`.

### 5.3 Gate the sweeper on the load of the partition

The sweeper counts the requests in flight and skips a chunk while the count is above a threshold, with
a maximum deferral so it always makes progress.

It is deferred. No counter for requests in flight exists in the code today, and no measurement says
what the threshold must be. The wrapper of section 4.2.10 is the seam that a counter needs, so the work
adds the counter and the gate without a change to the call sites.

### 5.4 Bump the watermark with the current time

Rejected. Section 4.2.8 states the two failures: it aborts a live transaction on every chunk, and it
makes the watermark of a parent and a child differ forever.

### 5.5 Add a new migration entry for the schema change

Rejected for now. The project is before its first release, so editing migration 1 and migration 2 in
place keeps the schema readable. A new entry is right after the first release.

### 5.6 Reject a `ttlAt` in the past

`putItem` and `transactWriteItems` throw when `ttlAt` is at or before the current time, so a caller
bug surfaces at once.

Rejected. The check reads a wall clock, so the same request is valid or invalid depending on when it
arrives. A `transactWriteItems` replay with a `clientRequestToken` must reach the coordinator ledger
to learn the stored outcome; a client that retries after the instant passed would throw at the
boundary instead, and never learn that its transaction committed. Accepting the value has no such
case and needs no clock: the item is expired on arrival and the sweeper deletes it. DynamoDB accepts
a past timestamp for the same reason.

### 5.7 A relative `ttlSeconds` convenience field

The API also accepts a relative `ttlSeconds`, and `db.ts` resolves `now + ttlSeconds` into the
absolute instant at the boundary.

Rejected. The resolution makes the wire value non-deterministic: two sends of the same request carry
two different instants. A `clientRequestToken` replay then fails the operations-hash check as a
different set of operations — which is exactly the mistake the hash exists to catch — and the only
way out is to exclude the TTL from the hash and accept the drift silently. One absolute field keeps
the request a pure function of what the caller wrote. A caller that wants a relative TTL adds two
numbers.

---

## 6. Frequently Asked Questions

**Why does a read return an expired item?** Because the sweeper is the only mechanism that removes it,
and a read does not check the expiry instant. Section 5.1 states the tradeoff. Every read result
carries `ttlAt`, so a caller that needs the stronger contract filters on it.

**What happens when a caller writes milliseconds into `ttlAt`?** A millisecond epoch is a far-future
instant — around the year 57,000 — so the item never expires and storage holds it. No guard rejects
it: a bound tight enough to catch the mistake would also reject a legitimate far expiry, and DynamoDB
draws the same line. Its only guard sits on the other side — it ignores an expiry more than five
years in the past — which this design does not need, because a past `ttlAt` is simply swept.

**What happens to an expired item in a partition that gets no request?** It stays on disk. The
partition does not wake, so it does not sweep. The next request, or the next wake for another reason,
arms the timer and the sweeper deletes the item.

**Why 100 rows and 500 ms?** The 100 rows bound one storage transaction. The 500 ms bound the rate, and
the loop pays it once per 10,000 rows or per 50 MB, not once per chunk, so a small item does not drain
at the rate of a large one. The values are a starting point, and `ttlConfig()` in section 4.2.11 makes
them overridable. `TODO: measure` whether a larger chunk is safe.

**Why does the sweeper read `promoted_keys` instead of the in-memory set?** Because the filter belongs
in the statement that picks the chunk. A JavaScript filter would need the keys of every candidate row
in memory, and it would return them to SQLite to delete them. The table has `hash_key` as its primary
key, so the `NOT EXISTS` clause is one index seek per candidate row on a background path.

**What happens when a transaction locks an expired item?** The sweeper skips the row. The transaction
commits or cancels, the lock goes away, and the next chunk deletes the row.

**Does a TTL delete increment the version of the item?** No. The row goes away. A later put on the same
key starts at version 1, in the same way it does after `deleteItem`.

**Can the sweeper and the promotion garbage collection delete the same row?** No. The sweeper excludes
every hash key that `PromotionManager.hasStatus` reports, so the promotion jobs own those rows.

**Does the sweeper stop a split from happening?** Open Question 4.3.1 decides it. `maybeQueueSplit`
reads `sql.databaseSize`, so the answer depends on whether SQLite returns the freed pages.

---

## 7. References

References:

- `AGENTS.md` (the rules for PartitionDO operations)
- `docs/agent-plans/promoted-keys-bloom-filter-cache.md`
- `docs/agent-plans/range-partition-splits-v2.md`
- [Amazon DynamoDB: Time to Live](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html)
- [Cloudflare Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Cloudflare Workers limits](https://developers.cloudflare.com/workers/platform/limits/)
