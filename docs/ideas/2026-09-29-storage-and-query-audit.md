# Audit — storage schemas and queries of the sharding runtime and FokosDB

**State:** Findings. Nothing is decided or implemented.
**Date:** 2026-09-29
**Updated:** 2026-09-30, after commit `1b4d8e3` (a promotion moves the locks of its key). Section 8 lists what
that commit changed. R2 and X3 no longer apply.

## Table of contents

- [1. Scope and method](#1-scope-and-method)
- [2. Summary](#2-summary)
- [3. Sharding runtime](#3-sharding-runtime)
- [4. FokosDB](#4-fokosdb)
- [5. Issues that cross both layers](#5-issues-that-cross-both-layers)
- [6. Schema changes to decide before the freeze](#6-schema-changes-to-decide-before-the-freeze)
- [7. What was checked and is fine](#7-what-was-checked-and-is-fine)
- [8. Changes after commit 1b4d8e3](#8-changes-after-commit-1b4d8e3)

## 1. Scope and method

The audit covers every table, index and KV key, and every statement on the request paths and in the background
jobs (alarm, scheduler, TTL timer, migration, cleanup). The target scale is thousands of transactions in flight and
partitions of 1 GB.

The query plans of the suspect statements were checked with SQLite 3.45 on the real schemas, with 10k–200k rows
and no `ANALYZE`. Paths are under `packages/fokosdb/src/`. No finding was run as a runtime test. Each finding
says how it was checked:

- **Plan:** confirmed with `EXPLAIN QUERY PLAN`.
- **Code:** confirmed by following the code path.
- **Code, not run:** follows from the code path, but a test must confirm it.

## 2. Summary

| # | Layer | Finding | Severity | Schema change? |
|---|---|---|---|---|
| F1 | FokosDB | A promotion or a range split reads the whole source table, including `data`, for each target. A promoted key is not available for writes during that time | High (higher after `1b4d8e3`) | No |
| X1 | Cross | A hash split reads the whole source once for each child, with a per-row hash and a per-row JOIN | High | Optional (now or never) |
| X2 | Cross | The stale-lock job can run again every ~50 ms and blocks the split and import jobs | High | Yes, if fixed with F4 |
| ~~R2~~ | Runtime | Removed by `1b4d8e3`: the `repartitionUnblocked` signal and `markPromotionsDueNow` are gone | — | — |
| C1 | FokosDB (TC) | `tx_recovery` does a full scan and sort of `tc_state` every 5 s | High | Additive index |
| F5 | FokosDB | One partition-wide delete counter makes read transactions abort on unrelated deletes | High | Additive table |
| F3 | FokosDB | Split sources keep all item rows for life: depth d keeps d+1 copies of the data | High (cost) | No |
| F4 | FokosDB | `pending_transactions` repeats per-transaction data on each key; `conditions_json` is never read; the stale queries step past lock copies | Medium | Yes |
| R3 | Runtime | `routerRole()` reads the split row and all target rows, several times per request, per key | Medium | No |
| R5 | Runtime | `learnRangeBoundary` counts the whole table on each insert and each refresh | Medium | Optional index |
| C3 | FokosDB (TC) | Rows of up to 400 KB sit in WITHOUT ROWID tables (2–5x storage, as measured for `items`) | Medium | Yes |
| R7 | Runtime | The Bloom filter (~360 KB) is written whole each time the partition learns one promoted key | Medium | Optional |

## 3. Sharding runtime

### 3.1 What it stores

- **`fokos_repartitions`**. Rows are permanent. The indexes are the PK, `seq`, `due (state, next_attempt_at,
  seq)`, and a partial index on `split`. Without statistics, the due, deadline and cleanup queries use
  `idx_fokos_repartitions_due` (**Plan:** OK). With `ANALYZE` statistics, SQLite chose a full scan for
  `earliestRepartitionDeadline` and `firstActiveRepartition` on 20k rows. A query-plan test would lock the good
  plan in.
- **`fokos_repartition_targets`**, **`fokos_route_overrides`**, **`fokos_range_hierarchy`**.
- **KV keys:** `identity`, `policy`, `import`, `destroying`, `jobs`, `hash_arena` (≤ 1 MB), `promotion_bloom`
  (≤ 1.5 MB, ~360 KB at the defaults), and the plan heads.

### 3.2 Request path

**R2 — each commit and cancel rewrites every waiting promotion (High).** **No longer applies:** commit `1b4d8e3`
removed the `repartitionUnblocked` signal, `markPromotionsDueNow`, and the lock hold of `beforeCutover`. The text
below describes the code before that commit. **Code.**

- **What happens:** `txCommit` and `txCancel` always signal `repartitionUnblocked`
  (`server/do-partition.ts:561`, `:587`). While any promotion is unfinished, `markPromotionsDueNow`
  (`sharding/sharding-store.ts:609`) runs two UPDATEs. They rewrite every queued or planned promotion row and its
  `idx_due` entry, and every target that is not initialized. Then a pass runs, and `#deferTargets` writes those
  targets again when the key is still locked.
- **Example:** Key K grows past the promotion size, so K is hot and almost always holds a lock. Each commit on the
  partition writes about 2P + T rows (P = waiting promotions, T = their targets) and starts a pass.
  `beforeCutover` sees a lock again and defers again. The promotion can also never cut over (see X3).
- **Fix:** Signal the hash keys that the commit or cancel released. Update only the promotion of such a key (one
  `routeOverrideFor` seek), and only when `next_attempt_at > now`.

**R3 — router facts are read again for each key, including all targets (Medium).** **Code.**

- **What happens:** `routerRole()` calls `#split()` (`sharding/repartition-flow.ts:141`, `:175`). That call reads
  the split row and `listRepartitionTargets`: K rows with the wide `do_name` and `partition_id` columns.
- **Where it runs:**
  - For each key in `#resolve` on a hash partition (`#hashTopologyOwner(key, routerRole())`).
  - After each write, in `#evaluateSplit`.
  - In `lifecycle()`.
  - On a range router, `#rangeChildFor` calls `children()`, which is `routerRole()` + `splitTargets()`: two reads
    of 1 + K rows for each key.
- **Example:** A `txPrepare` of 100 keys reaches a range router with `rangeSplitN = 4`. It does about 1,000 row
  reads and 100 slice searches only to route.
- **Fix, step 1:** Make `routerRole()` read only the split row.
- **Fix, step 2:** Keep the split row and its targets in memory when the state is `cutover` or later. At that
  point the targets never change and the role never goes back. The "not a router" answer stays a read, so the
  risk that the comment at `sharding/repartition-flow.ts:166` describes does not return.

**R4 — facts that seldom change are read on each request (Low–Medium).** **Code.**

- **What happens:**
  - `#guard` reads the KV key `destroying` on each RPC.
  - `#dispatch` reads the KV import record on each RPC.
  - A hash partition runs the `routeOverrideFor` JOIN for each key of each request, also when it has no override.
- **Fix:** Keep the destroy fence (it only goes to true) and the import state in memory. Only this runtime writes
  them, so it can update memory after each commit. Keep an in-memory "has any override" flag to skip the JOIN.

**R5 — `learnRangeBoundary` counts the whole table (Medium).** **Code + Plan.**

- **What happens:**
  - Any `rowsWritten > 0` runs `SELECT COUNT(*)` over `fokos_range_hierarchy` (`sharding/sharding-store.ts:957`,
    marked FIXME). This includes the 60-second refresh of a known row, where the table does not grow.
  - The eviction query scans and sorts the whole table, because `learned_at` has no index.
- **Example:** A router serves traffic to 5,000 learned slices, so about 83 refreshes per second. Each refresh
  counts 5–10k entries: about 0.5M row visits per second on a single-threaded DO.
- **Fix:**
  1. Count once at load, and keep the count in memory.
  2. Split the statement into `INSERT … ON CONFLICT DO NOTHING` (on a write, add 1) and a conditional `UPDATE`.
  3. Evict in batches, for example the oldest 10 % when the count goes above the maximum.

**R6 — `findDeepestKnownRangeSlice` reads all slices of the key (Low–Medium).** **Plan.**

- **What happens:** The plan is `SEARCH PK (hk=? AND start<?)` + `TEMP B-TREE`. The query reads every learned slice
  of the hash key that starts at or before the sort key, and then sorts them. It runs for each key of each request
  to a promoted key.
- **Fix:** Slices of one depth do not overlap, so do one seek per depth:
  `WHERE hk=? AND depth=? AND sk_start_boundary<=? ORDER BY sk_start_boundary DESC LIMIT 1`. The existing
  `idx_fokos_range_hierarchy_depth` index supports it.

**R7 — the Bloom filter is written as one KV value (Medium).** **Code.**

- **What happens:** The runtime writes about 360 KB (300k keys at 1 %) each time a hash partition learns one new
  promoted key (`sharding/runtime.ts:1792`). The output gate of that request waits for the write. The runtime also
  reads the whole value at each start of the DO.
- **Example:** A router learns 2,000 promoted keys: about 720 MB of writes over its life.
- **Fix without a schema change:** The Bloom filter is a cache, so write it with a delay: mark it dirty and write
  it at most once per interval. A crash loses only hints, which cost one more hop.
- **Fix with a schema change:** Store the bits as 4 KB pages in a table, and write only the ≤ k pages that changed
  (k = 7 at 1 %).
- **Hash arena:** It uses the same pattern, but it is small and changes seldom (Low).

**R11 — policy "last writer wins" can write on every request (Low–Medium).** **Code.**

- **What happens:** `#ensureIdentity` writes `__fokos/policy` each time the request policy is different from the
  stored one.
- **Example:** During a rolling deploy, two Worker versions with different table options send requests in turn.
  Each request writes the KV key and changes the split thresholds back and forth, on every partition they reach.
- **Fix:** Add a monotonic policy version to the route context, and store only a newer policy. This is a wire
  change, so do it before the freeze.

### 3.3 Background path

**R10 — the scheduler does more reads than it needs (Low).** **Code.**

- **What happens:**
  - Each pass reads the deadline of every job 2 times (3 times before `1b4d8e3`), and calls `canRun` 2 times.
  - `canSweepLocally` and `canDriveLocally` call `lifecycle()`, which does 4 reads, one of them the R3 target read.
  - Each request on an importing target starts a pass.
- **Fix:** Memoize `lifecycle()` for one pass.

**R8 — each status page sorts the whole union (Low).** **Plan.**

- **What happens:** Each `fokosStatus` page (destroy traversal and admin) builds the whole UNION ALL after the
  cursor, and then sorts it. Rows are permanent, so a full walk costs O(rows² / page size).
- **Fix:** Walk `fokos_repartitions` by `seq` with a LIMIT. Then read the targets of each row with an ordered seek.

**R9 — inherited promotions store 3 rows and about 6 copies of the hash key (Low).** **Code.**

- **What happens:** `#applyOverrides` writes a repartition row, a target row and an override row for each key. The
  target row holds `slice_hash_key`, `do_name` and `partition_id`, and the last two also encode the hash key. Each
  later hash split copies the rows again. Only the override is needed to route.
- **Fix:** Store a finished promotion as the override row only, if the smaller schema is worth the change.

## 4. FokosDB

### 4.1 PartitionDO

**F1 — migration ignores the slice (High).** **Code.**

- **What happens:** `#buildItemsPage` (`shared/partition/fokos-migration-host.ts:90`) never uses `_slice`. It pages
  `queryItemsPage` from the first row of `items` to the last row, and it reads `data` for each row.
  `queryRangeItemsPage` exists, but nothing in `src/` calls it.
- **Example (promotion):** A 1 GB hash partition with 5M rows promotes one key. The target pulls about 500 pages,
  and most pages have no items. The source reads about 1 GB for one key.
- **Example (range split):** In a range split with N children, each child reads from row 0 to the end of the table.
  The source reads the table N times.
- **Fix:** Seek to `(slice.hashKey, slice.start)`, and end the stream when the scan goes past the slice. There is
  no schema change.

**F3 — split sources keep all their item rows for life (High, cost).** **Code.**

- **What happens:** `cleanupSourceStep` returns `true` at once for a split (`server/do-partition.ts:704`). A router
  therefore keeps its full copy. A key space at hash depth 3 stores 4 copies of the data. After `completed`, no
  target reads through, so the rows are dead. The coordinator deletes its migrated rows
  (`deleteMigratedRowsStep`), so the two hosts do not agree.
- **Fix:** Delete the rows in bounded batches, as promotions and the TC ledger already do.
- **Trade-off:** Row deletes are billed as rows written. For 1 GB of 1 KB rows, the delete costs about the same as
  five months of storage; for 100-byte rows, about fifty months. Measure whether a `DELETE FROM items` with no
  `WHERE` clause is cheap on Durable Objects before choosing the method.

**F4 — `pending_transactions` shape (Medium).** **Code + Plan.**

- **Problems:**
  - `conditions_json` is written for each lock, but only the migration copy reads it. It is dead.
  - `coordinator_json`, `created_at`, `transaction_ts` and `guarded_at` are the same for every key of one
    transaction, but each key stores them. Each key also adds an entry to the `created_at` index.
  - `guardPendingTx` and `clearPendingTxGuard` update N rows.
  - `listStalePendingTx` must walk every row of a transaction. The documented worst case is 9,000 rows for 10
    results.
  - Guarded rows stay at the head of the `created_at` index. Each pass walks past them in
    `earliestUnguardedPendingTxCreatedAt`.
- **Fix (schema):** Add a per-transaction table
  `pending_tx(transaction_id PK, coordinator_json, transaction_ts, created_at, guarded_at, next_recovery_at)`, with
  a partial index on `next_recovery_at WHERE guarded_at IS NULL`. The lock rows keep `(hk, sk, transaction_id,
  operation, data_kind, ttl, data)`. Drop `conditions_json`.
- **Result:** Today a prepare of N keys does 4N B-tree writes. After the fix it does 3N + 2. The stale scan reads
  one row per transaction. A guard writes one row. `next_recovery_at` also gives the stale job a backoff (X2).
- **Smallest fix:** Drop `conditions_json`, and make the `created_at` index partial `WHERE guarded_at IS NULL`.

**F5 — one partition-wide delete counter (High).** **Code.**

- **What happens:**
  - `readForTransactionLocal` reports one `delete_revision` for the whole partition. `client/db.ts:867` aborts a
    read transaction when it changes between the two phases.
  - The same applies to `max_delete_tx_order_ts` at prepare
    (`shared/partition/transaction-participant.ts:289`). A transactional insert of an absent item fails with
    `timestamp_conflict` when any delete in the partition has a later timestamp, including deletes stamped by
    clocks that run up to 5 s ahead.
- **Example:** A partition gets 200 deletes/s, and the two phases of a read are 20 ms apart. On average 4 deletes
  land between the phases, so about 98 % of multi-partition reads on that partition abort.
- **Fix:** Use buckets, as the comment in `readForTransactionLocal` says: a table
  `deletion_buckets(bucket PK, revision, max_delete_ts)` with about 1,024 rows, where a hash of (hk, sk) selects the
  bucket.
  - The wire already carries `deleteRevision` for each item, so the client does not change.
  - Migration must copy the buckets and merge them with MAX.
  - The table is additive, but the change is smallest now.
- **Why a per-item counter is not enough:** A read that sees an item absent in both phases must still detect a
  create and a delete between the phases. Only a counter that is shared by more keys than one item can record it.

**F6 — two indexes on (hk, sk) (decision to confirm).** **Code.**

- **What happens:** `items` has `UNIQUE(hk, sk)` and also `idx_items_scan (hk, sk, est_row_bytes)`.
  - Each insert and each delete writes both indexes.
  - Each update that changes the size moves the `idx_items_scan` entry.
  - The keys are stored 3 times. With 1 KB hash keys, that is most of a small row.
- **Why it exists:** Count queries and the range split scan read only the index. This is a real trade-off, not a
  defect, but confirm it before the freeze.
- **Related:** The `key_size_estimates` write adds one write to each item write. That cost is fixed per request,
  so it is acceptable.

**F7 — the range split boundary scan blocks the DO (Medium).** **Code.**

- **What happens:** `computeRangeSplitBoundaries` (`shared/partition/partition-store.ts:981`) scans up to
  (N−1)/N of the partition index in one synchronous `transactionSync` inside `#plan`. On a 1 GB range partition
  with 10M small rows, that blocks every request for seconds.
- **Fix:** Scan in chunks over several steps, and keep the running totals in the plan head. Or sample the index.

**F8 — commit reads each lock row 2 times (Low).** **Code.** `listPendingTxKeys` and `getPendingTxOp` for each
key read the same rows (`1b4d8e3` removed the third read, `pendingTxCountFor`). One query that returns the keys and
the payload can replace both. The release is now one `DELETE` by primary key for each key
(`deletePendingTxKeys`). The rows written are the same as before, so this is acceptable.

**F9 — TTL sweep (Low).** **Code + Plan.** The plan is good: a covering index scan on `idx_items_ttl` and an
anti-join on the PK of the locks. Expired rows that are locked stay at the head of the index and are read again
each cycle, but locks are few.

### 4.2 TransactionCoordinatorDO

**C1 — `tx_recovery` does a full scan of `tc_state` (High).** **Plan:** `SCAN tc_state` + `TEMP B-TREE FOR ORDER BY`.

- **What happens:**
  - `tc_state` keeps every transaction for `IDEMPOTENCY_WINDOW_MS` (10 minutes).
  - Under steady traffic, some transaction is always in flight, so the job runs every `staleTransactionMs` (5 s)
    for ever (`server/do-transaction-coordinator.ts:1255`).
  - Each run scans and sorts the whole 10-minute ledger: about 300k rows at 500 tx/s.
  - `hasNonTerminalRows` (`:1293`) also scans the whole table when no transaction is in flight.
- **Fix:** Add a partial index `ON tc_state (created_at, transaction_id) WHERE completed_at IS NULL`, and write both
  queries with `completed_at IS NULL`. `completeTransaction` sets the state and `completed_at` together, so
  "non-terminal" and "`completed_at IS NULL`" mean the same thing.

**C2 — writes on the happy path that it does not need (Medium).** **Code.**

- **What happens:** One transaction with N items and P participants does about 3N + 4P + 10 row writes. The extra
  writes are:
  - `stripPayload` rewrites N `tc_items` rows at PREPARED (`:1002`) and at CANCELLING (`:939`), a few
    milliseconds before `completeTransaction` deletes them.
  - `prepare_outcome = 'accepted'` and `commit_outcome = 'committed'` add 2P updates. Only the recovery path needs
    them, and a new prepare or commit is idempotent.
  - The move from PREPARED to COMMITTING is a separate write.
  - `partition_context_json` stores a full route context for each participant of each transaction, although one
    coordinator group serves one table.
- **Fix:** Write these values only when the request path cannot finish. That gives about 2N + 2P + 8 writes.

**C3 — wide rows in WITHOUT ROWID tables (Medium).** **Code.**

- **What happens:** `tc_items.data` (≤ 400 KB), `tc_results.image_data`, the JSON columns of `tc_participants`,
  and `tc_state.results_json` are all in WITHOUT ROWID tables. The migration comment of `items` measured 2.3–4.7x
  storage for this layout. A coordinator splits on size, so the extra storage also makes it split early.
- **Fix:** Use rowid tables with an explicit unique index, as `items` and `pending_transactions` already do.

**C4 — recovery drives one transaction at a time (Low).** **Code.** Recovery drives transactions one at a time
(FIXME at `:1273`) in a step of up to `alarmRecoveryBudgetMs` (30 s). The split and import jobs of the coordinator
wait behind it (see X2).

## 5. Issues that cross both layers

**X1 — a hash split reads the whole source once for each child (High).** **Code.**

- **What happens:**
  - Only a JS hash of `hk` at the partition depth gives the child index. `items` is ordered by `(hk, sk)`, so a
    child finds its rows only when the source reads every row, with `data`.
  - `belongsToTarget` (`sharding/repartition-flow.ts:849`) runs `hasTerminalRouteOverride`, a JOIN, for each row.
    It also does this in the coordinator, where no promotion can exist.
- **Example:** A 1 GB partition has 10M rows and `hashSplitN = 4`. The source reads about 4 GB, and runs 40M hashes
  and 40M JOINs. It builds each page synchronously, with up to 10k scanned rows.
- **Fix without a schema change:**
  1. Keep the answer for the last hash key. Rows arrive in `hk` order, so there is one hash and one lookup per key.
  2. Skip the JOIN when the partition has no finished override.
  3. Read the keys from `idx_items_scan` first, and read `data` only for the rows that match.
- **Fix with a schema change (now or never):** Add a column `split_bucket = hashChildIndex(hk, depth, hashSplitN)`,
  computed on insert, and put it first in both key indexes: `UNIQUE(split_bucket, hk, sk)`. Each child's rows are
  then one index range. A child computes the column again for its own depth when it imports a row. A range
  partition stores 0. The cost is one hash for each write and one byte for each index entry, and each point and
  range query adds `split_bucket = ?`.

**X2 — a job's own deadline can make it run without pause (High).** **Code, not run.**

- **Cause:** In the scheduler, `#deadlines` uses `min(scheduled, own)`. When the `deadline()` of a job stays in the
  past, the job runs again about 50 ms after each pass, and the step's `nextRunAt` has no effect.
- **Where it happens:** The `stale_tx_recovery` deadline is "oldest unguarded lock + 5 s"
  (`server/do-partition.ts:739`). The lock stays in these cases:
  - The coordinator answers `driving`.
  - The call fails fast. For example, the coordinator answers `COMMITTED`, and the `txCommit` dispatch reaches a
    child that is still importing and gets `partition_migrating`.
  - In both cases the job runs about 20 times per second and sends up to `staleLockScanRows` (10) RPCs each time.
- **Precedent:** The coordinator hit the same problem and worked around it with a KV key (comment at
  `server/do-transaction-coordinator.ts:471`).
- **Second problem:** Passes run one at a time, and so do the jobs of one pass. One slow stale-lock step (10 locks
  × a 10 s coordinator budget) holds `target_import` and `source_repartition` for up to 100 s.
- **Fix:**
  1. Give each job a durable backoff. The `next_recovery_at` column of F4 does this for the stale-lock job.
  2. Let `nextRunAt` limit how often `deadline()` can make a job due.
  3. Bound the time of host jobs, or run them in a separate pass.
- **First step:** Write a test that holds a lock whose coordinator answers `driving`, and count the job runs.

**X3 — a hot key can block its own promotion (liveness).** **No longer applies:** after `1b4d8e3`, a lock does not
hold a promotion. The lock moves with the key. The text below describes the code before that commit. **Code.**

- **What happens:** `beforeCutover` needs zero locks on the key. The key that grows past the promotion size is the
  hot key, so under steady transactions it can always hold a lock. R2 adds write load on each commit. The
  partition can go past 1.1 times its cap and then stop accepting writes, which is the known over-size stop.
- **Fix to consider:** A short admission hold on new prepares for a key while its promotion waits for cutover.

## 6. Schema changes to decide before the freeze

The SQL migrations can still be edited in place.

- **Breaking:**
  - F4: the per-transaction lock table, the removal of `conditions_json`, and the partial index.
  - F10: the index `(created_at, hk) WHERE guarded_at IS NULL` on `pending_transactions`.
  - C3: rowid tables in the coordinator.
  - X1: `split_bucket`, if it is wanted.
  - R11: a policy version in the route context.
  - R7: paged Bloom storage, if it is wanted.
  - R9: finished promotions as override rows only, if it is wanted.
- **Additive, but cheapest now:**
  - C1: the partial index on `tc_state`.
  - F5: the delete buckets.
  - R5: an index on `learned_at`, if it is wanted.
- **No schema change:** R3, R4, R6, R8, R10, F1, F3, F7, F8, C2, C4, and the scheduler part of X2.

## 7. What was checked and is fine

- The point read and write statements: one seek each.
- The TTL sweep plan (F9).
- The due, cleanup and deadline queries of `fokos_repartitions`, without statistics.
- The idempotency sweep of the coordinator (covering partial index on `completed_at`).
- The token lookups of the coordinator.
- Lock release by transaction id (index `pending_transactions_transaction_id`).
- The cleanup batches of promotions and of the coordinator ledger.
- The hash arena snapshot: it writes only the used part, and only when the tree grows.

## 8. Changes after commit 1b4d8e3

Commit `1b4d8e3` makes a promotion cut over while its key holds locks. The `pending_tx` stream copies the locks to
the range root. The source keeps its rows of the key as copies until the completion transaction deletes them. Each
local decision (commit, cancel, quarantine) now changes owned rows only, one statement per key. The two stale
queries exclude the copies with `hk NOT IN (<fokos.sql.movedHashKeys()>)`.

### 8.1 Findings that the commit removed

- **R2:** The `repartitionUnblocked` signal and `markPromotionsDueNow` are gone. A commit or a cancel writes no
  repartition row and starts no pass.
- **X3:** PartitionDO has no `beforeCutover` hook now, so a lock cannot hold a promotion.
- **One cause of the X2 loop:** Before the change, the oldest copy of a moved key would keep the stale-lock
  deadline in the past for the whole import. The `NOT IN` clause removes this case.

### 8.2 Findings that the commit made more important

**F1 — the promotion import now decides how long a hot key is unavailable.** **Code.**

- **What happens:** After cutover, the range root is `awaiting_data` or `importing`. Each write and each
  transaction step on the key answers `partition_migrating` until the import ends. The root also cannot sweep its
  stale locks during the import (`canSweepLocally` is false).
- **Why F1 matters more now:** Before the change, a promotion waited for zero locks. Now it cuts over while
  transactions are in flight, so these transactions wait for the whole import. A commit fan-out that waits longer
  than `fanoutRequestBudgetMs` (5 s) goes to `tx_recovery`.
- **Example:** A 1 GB hash partition promotes a 250 MB key. Because of F1, the import reads the full 1 GB and
  sends about 500 mostly empty pages. The unavailable time follows the size of the source partition, not the size
  of the key.
- **Fix:** Unchanged. Seek to the slice, and stop the stream after it.

**X2 — the stale-lock loop can still start.** **Code, not run.**

- **What happens:** The deadline still reads "oldest unguarded owned lock + 5 s". A lock that stays after a
  recovery step keeps the deadline in the past. Two cases remain:
  - The coordinator answers `driving`, or answers a state that the job does not act on.
  - The call to the coordinator fails fast. For example, the lock names a coordinator that split, and its child
    still imports, so the forward answers `partition_migrating`.
- **New cost in the loop:** While a promotion is in `cutover`, each deadline read and each stale scan steps past
  the copies. The comment on `earliestUnguardedPendingTxCreatedAt` measured 20,003 rows read for 10,000 copies,
  and a pass reads the deadline 2 times. If the loop runs during a promotion import, each loop turn reads about
  60,000 rows.
- **Fix:** Unchanged: a durable backoff for each job (see F4, `next_recovery_at`).

### 8.3 New item

**F10 — the stale queries read 2 rows for each copy (Low–Medium).** **Code + Plan.**

- **What happens:** The plans are good: the `NOT IN` subquery reads `idx_fokos_repartitions_due` one time for each
  statement. But `pending_transactions_created_at` holds only `created_at`. Thus each copy that the scan steps past
  costs one index entry and one table row, for `guarded_at` and `hk`. This lasts for the whole promotion import
  (see F1 for its length).
- **Fix (schema):** Make the index `(created_at, hk) WHERE guarded_at IS NULL`. The deadline query then reads only
  the index, and guarded rows cost nothing. This also covers the smallest fix of F4.

### 8.4 Effect on the F4 fix

The per-transaction table of F4 still works, with these rules:

- A transaction row must stay while any of its lock rows stay, including copies. Delete it in the same statement
  group as the last lock row, for example `DELETE FROM pending_tx WHERE transaction_id = ? AND NOT EXISTS (SELECT 1
  FROM pending_transactions WHERE transaction_id = ?)`. This is one seek on `pending_transactions_transaction_id`.
- A transaction row cannot tell owned rows from copies. A transaction with only copies must not keep the deadline
  in the past. Each recovery attempt must therefore move `next_recovery_at` forward, also when it finds no owned
  row. This is the same backoff that fixes X2.
- The guard is now per key (`guardPendingTx` takes the owned keys). With one `guarded_at` for each transaction,
  the migration would copy a guard that the source set after cutover to the copies of the moved key. This is
  harmless: `not_found` from the coordinator is true for every owner of the transaction, so the new owner would
  set the same guard at its next check.

### 8.5 Findings that the commit did not change

C1, C2, C3, C4 (the coordinator source did not change), F3, F5, F6, F7, R3 to R11, and X1. R3 is used more now:
`owns()` runs for each hash key of a copy in `commitLocal`, and for each hash key in stale recovery. Each call
reads the route override and the router role.
