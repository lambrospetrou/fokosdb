# Audit — storage schemas and queries of the sharding runtime and FokosDB

**State:** Findings. Done: K1, K5, F4, F8, F10, C1 and X2. Skipped: C3. The other findings are not decided or
implemented.
**Date:** 2026-09-29
**Updated:** 2026-09-30.

## Table of contents

- [1. Scope and method](#1-scope-and-method)
- [2. Summary](#2-summary)
- [3. Sharding runtime](#3-sharding-runtime)
- [4. FokosDB](#4-fokosdb)
- [5. Issues that cross both layers](#5-issues-that-cross-both-layers)
- [6. Schema changes to decide before the freeze](#6-schema-changes-to-decide-before-the-freeze)
- [7. What was checked and is fine](#7-what-was-checked-and-is-fine)

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
- **Collector:** the production `collectBatch` ran with logical row sizes, without payload allocation.
- **SQL replay:** the current statements ran against the matching schema in SQLite 3.45.1.

## 2. Summary

| # | Layer | Finding | Severity | Schema change? |
|---|---|---|---|---|
| F1 | FokosDB | A promotion or a range split reads the whole source table, including `data`, for each target. A promoted key is not available for writes during that time | High | No |
| F11 | FokosDB | Migration fetches payload arrays before the byte budget; a default fetch can hold about 390 MiB | High | No |
| C5 | FokosDB (TC) | One PREPARING transaction can exceed the 32 MiB migration RPC limit and stop an import | High | No |
| X1 | Cross | A hash split reads the whole source once for each child, with a per-row hash and a per-row JOIN | High | Optional (now or never) |
| X2 | Cross | Done. The stale-lock job could run again every ~50 ms, and a slow step blocked the split and import jobs | High | Done with F4 |
| C1 | FokosDB (TC) | Done. `tx_recovery` did a full scan and sort of `tc_state` every 5 s | High | Additive index |
| F5 | FokosDB | One partition-wide delete counter makes read transactions abort on unrelated deletes | High | Additive table |
| F3 | FokosDB | Split sources keep all item rows for life: depth d keeps d+1 copies of the data | High (cost) | No |
| F4 | FokosDB | Done. `pending_transactions` repeated per-transaction data on each key; `conditions_json` was never read; the stale queries stepped past lock copies | Medium | Yes |
| F7 | FokosDB | The range-boundary scan blocks the request path and runs again during planning | Medium | No |
| F12 | FokosDB | Empty hash keys retain their size-estimate rows and index entries | Medium | No |
| F13 | FokosDB | The last migration acknowledgement deletes all lock copies in one synchronous transaction | Medium | No |
| R3 | Runtime | `routerRole()` reads the split row and all target rows, several times per request, per key | Medium | No |
| R5 | Runtime | `learnRangeBoundary` counts the whole table on each insert and each refresh | Medium | Optional index |
| C3 | FokosDB (TC) | Skipped. WITHOUT ROWID tables use 3x storage only for rows of about 1–2.5 KB, and most of those rows are short-lived | Low | Yes |
| R7 | Runtime | The Bloom filter (~360 KB) is written whole each time the partition learns one promoted key | Medium | Optional |

----

The next most important items are the ones that can stop an import. A stuck import keeps keys unavailable. After those come the schema changes that you must decide before the freeze. I checked the code: F1, F11 and F5 are still open. _slice is not used in fokos-migration-host.ts:63, collectBatch checks the byte budget only after it fetches the rows, and deletion_metadata has only one counter row.

Priority 1: stuck imports (availability)

1. F11: migration memory bound. One page fetch can hold about 390 MiB of payload, and the isolate limit is 128 MB. Each retry fails on the same batch. The target then never finishes its import, and its keys stay unavailable.
2. F1: use the slice during migration. A promotion reads the whole source to move one key. So the time that a hot key is unavailable depends on the size of the source partition, not on the size of the key.
   - Do F1 and F11 together. Both change #buildItemsPage and the store queries under it.
   - Add the X1 fixes that need no schema change in the same pass: keep the hash answer for the last hk, skip the JOIN when no override exists, and read the keys before data. They change the same code path.
3. C5: page the rows of one coordinator transaction. One transaction in PREPARING state can go above the 32 MiB RPC limit and stop the import of the coordinator. The fix changes the internal migration cursor and the page format, so do it before the freeze.

Priority 2: correctness under load (and cheapest now)

4. F5: delete buckets. With one delete counter for the whole partition, about 98 % of multi-partition reads abort at 200 deletes per second. Transactional inserts also fail on deletes of other keys. The change adds one table, and the wire format does not change. It is cheapest to do now.

Priority 3: schema decisions before the freeze

You must decide these now, also when the answer is "no":

5. X1 split_bucket: you can add it now or never.
6. C3: skipped. The measured gain is small (see C3).
7. R11: a policy version in the route context. This changes the wire format.
8. R7 (paged Bloom filter), K3 and R9 are optional. Their value is lower.

Priority 4: cost and background work (no schema change)

9. F3: delete the item rows of split sources. At depth d you keep d+1 copies of the data. First measure the cost of a DELETE FROM items with no WHERE clause on Durable Objects.
10. F13, F12 and F7.

Priority 5: small request-path fixes

11. R3, R4/K2, R5, R6 and K6. Each fix is small and local, and you can do them at any time after the freeze.

My recommendation: start with F11 and F1 as one change, then C5, then F5. Before you edit the migrations in place, make the decisions on X1 and R11.


## 3. Sharding runtime

### 3.1 What it stores

- **`fokos_repartitions`**. Rows are permanent. The indexes are the PK, `seq`, `due (state, next_attempt_at,
  seq)`, and a partial index on `split`. Without statistics, the due, deadline and cleanup queries use
  `idx_fokos_repartitions_due` (**Plan:** OK). With `ANALYZE` statistics, SQLite chose a full scan for
  `earliestRepartitionDeadline` and `firstActiveRepartition` on 20k rows. A query-plan test would lock the good
  plan in.
- **`fokos_repartition_targets`**, **`fokos_route_overrides`**, **`fokos_range_hierarchy`**.
- **KV keys:** see section 3.4.

### 3.2 Request path

**R3 — router facts are read again for each key, including all targets (Medium).** **Code.**

- **What happens:** `routerRole()` calls `#split()` (`sharding/repartition-flow.ts:141`, `:175`). That call reads
  the split row and `listRepartitionTargets`: K rows with the wide `do_name` and `partition_id` columns.
- **Where it runs:**
  - For each key in `#resolve` on a hash partition (`#hashTopologyOwner(key, routerRole())`).
  - In `lifecycle()`.
  - On a range router, `#rangeChildFor` calls `children()`, which is `routerRole()` + `splitTargets()`: two reads
    of 1 + K rows for each key.
  - For each hash key in `owns()`: `commitLocal` calls it for each hash key of a lock copy, and stale recovery
    calls it for each hash key of a stale transaction.
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
  - Each pass reads the deadline of every job 2 times, and calls `canRun` 2 times.
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

### 3.4 KV keys

**How KV storage works.** On a SQLite-backed Durable Object, `ctx.storage.kv` stores its data in a hidden SQLite
table. A local `workerd` database shows this table as `_cf_KV (key TEXT PRIMARY KEY, value BLOB) WITHOUT ROWID`.
The Durable Objects pricing page bills each `get`, `put`, `delete` and `list` as rows read or rows written. The
size of a value does not change the bill. The limit for a key and its value together is 2 MB.

**Inventory.** "Each start" means each time the object starts, in `blockConcurrencyWhile`.

| Key | Owner | Value | Written | Read |
|---|---|---|---|---|
| `__fokos/schema_version` | runtime | a number | once per schema version | each start |
| `__fokosdb/partition/schema_version` | `PartitionDO` | a number | once per schema version | each start |
| `__fokosdb/tc/schema_version` | coordinator | a number | once per schema version | each start |
| `__fokos/identity` | runtime | ~200 B; KBs for a range partition with large keys | once | each start |
| `__fokos/policy` | runtime | ~225 B | when the request policy changes (R11) | each start |
| `__fokos/destroying` | runtime | `true` | once, at the destroy fence | 2 times per `PartitionDO` request (`#api`, `#guard`), 1 time per coordinator request, and several times per pass |
| `__fokos/import` | runtime | a few hundred B; up to ~3 KB with a cursor of large keys | each migration page, retry, start and acknowledgement | 1 time per request (`#dispatch`), 2 times per pass (two job deadlines), `lifecycle()`, and each log line (`#logParams`) |
| `__fokos/jobs` | runtime | a small record | after a job step that changes it; `scheduleJob` when the new time is earlier | 2 times per pass, and in each `scheduleJob`: each accepted `txPrepare`, each coordinator `initiateWrite`, and each completed coordinator transaction |
| `__fokos/cache/hash_arena` | runtime | ≤ 1 MB | when the tree it learns grows | once, at the first forward |
| `__fokos/cache/promotion_bloom` | runtime | ~360 KB at the defaults | each new promoted key it learns (R7) | each start, whole |
| `__fokos/repartition/<id>/plan/00000001` | runtime | policy and host data of queue time, and the planned depth and ancestors | at queue and at plan | each hook call through `#hookPlan`, and `#head()` in `#plan` and in each target initialization step |

**K1 — done: the host keys have one prefix. A new naming scheme does not make a read faster (no change needed for speed).**

- Each `get` is one seek on the primary key of `_cf_KV`. The table holds about 5 to 10 keys, so the seek reads one
  or two pages. A prefix, the order of the keys, and the length of a key have no measurable effect on this seek.
- A prefix helps only `list({ prefix })`. No code calls `list()`.
- The large values do not slow the small keys. A value above the inline limit of a WITHOUT ROWID page (~1 KB) keeps
  a local part on the leaf page, and the rest goes to overflow pages. A `get` of another key never reads those
  overflow pages.
- **Keep `__fokos/`.** It marks what only the runtime can touch, and a full delete of the runtime state can use it.
- **Done: the host keys have the `__fokosdb/` prefix (clarity, not speed).** `PartitionStore` tracks its migrations
  under `__fokosdb/partition/schema_version`, and the coordinator under `__fokosdb/tc/schema_version`. The
  coordinator key `tc/recovery_due_at` is removed (K5). Each class has its own storage, so
  the old default name `__sql_migrations_lastID` did not collide. A class-specific name keeps the key clear if a
  second host store ever shares one storage.

**K2 — each `PartitionDO` request reads 3 KV keys before it does any work (Medium).** **Code.**

- `#api` (`server/do-partition.ts:430`) calls `isFenced()`, and `#guard` reads `destroying` again. Then `#dispatch`
  reads `import` through `isImporting()`. A coordinator request reads 2 keys. Each read is a SQLite seek and a V8
  deserialization of the value.
- After a target finishes its import, its record stays in `active` state for life. So every later request still
  reads and deserializes it, only to learn "not importing".
- **Fix:** This is R4. Only this runtime writes these two keys, so keep both in memory and update memory after each
  commit. A combined key does not help: with the memory copy there is no read left to combine, and without it a
  combined key still costs one seek and one deserialization per request.
- **Smallest fix:** `#api` reads the fence once, and passes it on, so `#guard` does not read it again.

**K3 — the plan head can be a column of its repartition row (Low, schema).** **Code.**

- The plan head has the same life as its `fokos_repartitions` row. The queue transaction writes both, and the
  transaction that writes `cleaned` deletes the head. `#hookPlan` reads the row, the targets and the head in each
  hook call, so the head costs one more read each time.
- The chain (`nextKey`) exists to allow a plan above one value. A KV value and a SQL row have the same 2 MB limit,
  so the chain gives no extra room. `deletePlanChain` also does one `get` and one `delete` for each link.
- **Fix:** Store the plan as a last column of `fokos_repartitions` (wide columns go last, as the migration comment
  says). The row read then includes it, and the `cleaned` transaction sets it to NULL. This removes one KV key per
  repartition, and the chain code.

**K4 — `identity` and `policy` stay two keys (no change).**

- Both are read only at each start, so one combined key saves one seek per start.
- `identity` is immutable and can be KBs (range ancestors with large keys), and `policy` changes. A combined key
  makes each policy change rewrite the identity.
- `__fokos/schema_version` and the host schema-version key also stay apart, because two different owners run their own
  migrations.

**K5 — done: the coordinator stores its recovery time in `__fokos/jobs` (Low).** **Code.**

- The coordinator stored a job deadline in its own key, `tc/recovery_due_at`, and each pass read it 2 times through
  `deadline()`.
- Now `applyMigrationPage` calls `scheduleJobInTransaction`, which writes the `tx_recovery` time into
  `__fokos/jobs` in the transaction of the page. It does not arm the alarm. Only a job step applies a page, and the
  end of the pass arms the alarm at the new deadline. The `tx_recovery` job has no `deadline()` now.

**K6 — `jobs` is read on the transaction path (Low).** **Code.**

- `scheduleJob` opens a transaction and reads the record on each accepted `txPrepare`, each coordinator
  `initiateWrite`, and each completed coordinator transaction. It writes only when the new time is earlier, so the
  write is rare. The read is on each call.
- One record for all jobs is the right shape: the jobs are few, and the scheduler reads all of them together.
- **Fix:** Only the scheduler of this instance writes the record, so keep a copy in memory, and read storage only
  at the start.

**K7 — the Bloom filter is read whole at each start (Low–Medium).** **Code.**

- The constructor reads and deserializes the whole value (~360 KB at the defaults) inside `blockConcurrencyWhile`.
  So the first request of each hash partition that ever learned a promoted key waits for it.
- The paged storage of R7 also fixes this: a lookup tests k bits (k = 7 at 1 %), so it needs at most k pages of
  4 KB, and the object can read a page when a lookup first needs it.
- A lazy read of the whole value helps less. Each request on a hash partition asks the filter, so the first request
  pays the same cost. Only control calls, for example `fokosStatus`, would skip it.

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
- **Why it matters for availability:** A promotion cuts over while its key holds locks. After cutover, the range
  root is `awaiting_data` or `importing`. Each write and each transaction step on the key answers
  `partition_migrating` until the import ends, and the root cannot sweep its stale locks (`canSweepLocally` is
  false). A commit fan-out that waits longer than `fanoutRequestBudgetMs` (5 s) goes to `tx_recovery`. Because of
  F1, the import of a 250 MB key from a 1 GB partition reads the full 1 GB. The time that the hot key is not
  available follows the size of the source partition, not the size of the key.
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

**F4 — done: `pending_transactions` shape (Medium).** **Code + Plan.**

- **The problems were:**
  - `conditions_json` was written for each lock, but only the migration copy read it.
  - `coordinator_json`, `created_at`, `transaction_ts` and `guarded_at` are the same for every key of one
    transaction, but each key stored them, and each key added an entry to the `created_at` index.
  - `guardPendingTx` ran one UPDATE for each owned key of the transaction.
  - `listStalePendingTx` walked every row of a transaction: 9,000 rows for 10 results in the worst case.
  - The stale queries walked past guarded rows and past the lock copies of a moved key (F10).
- **What changed:**
  - The table `pending_tx_info(transaction_id PK, transaction_ts, created_at, coordinator_json, guarded_at,
    next_recovery_at)` holds the facts of each transaction. The partial index `pending_tx_info_due (next_recovery_at)
    WHERE guarded_at IS NULL` gives the deadline and the batch of the stale job.
  - The lock rows keep `(hk, sk, transaction_id, operation, data_kind, ttl_epoch_utc_seconds, data)`.
    `conditions_json` and `pending_transactions_created_at` are removed.
  - The statement that deletes the last lock row of a transaction also deletes its `pending_tx_info` row.
  - The guard is per transaction. It covers the copies of a moved key, and the migration carries it to the new
    owner.
  - Each lock row of a migration page carries the `pending_tx_info` fields of its transaction. The target merges a
    repeated row: it keeps a guard and the earlier `next_recovery_at`. The page format did not change.
  - The stale step claims the due transactions and moves `next_recovery_at` forward before it calls the
    coordinator (X2). A transaction with only copies gets no coordinator call.
- **Result:** A prepare of N keys writes 3N + 3 B-tree entries, where it wrote 4N. The stale scan and the deadline
  read one index entry and one table row for each transaction. A guard writes one row.

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
- **Related:** Each item size change updates `key_size_estimates` and its `key_size_estimates_by_bytes` index.
  The index also stores the hash key, because the table is WITHOUT ROWID. Count both B-tree updates and the extra
  key storage. `largestKeysAtLeast` uses a bounded covering seek, confirmed with `EXPLAIN QUERY PLAN` in SQLite
  3.45.1. Empty keys retain both entries (F12).

**F7 — the range split boundary scan blocks the request path and runs again during planning (Medium).** **Code.**

- **What happens:** `planRangeSplit` (`shared/partition/partition-store.ts`) scans the covering partition index
  inside a synchronous `transactionSync`. `splitDecision` (`server/do-partition.ts`) runs it before the runtime
  queues the split. The planner calls it again through `computeRangeSplitBoundaries`
  (`sharding/repartition-flow.ts`, `#plan`).
- **Example:** With uniform rows, each scan visits about (N−1)/N of the index. At `rangeSplitN = 4`, the two scans
  visit about 1.5 times the row count. A 1 GB range partition with 10M small rows therefore needs about 15M row
  visits. Both scans block other requests. TODO: measure their duration at that size.
- **Refusal cost:** The decision runs synchronously before an over-size write receives its refusal. More requests
  can repeat the scan while the alarm write is pending, before the split row exists. At the floor, every refused
  write repeats the decision. The implemented over-size RFC already bounds the floor cases; it does not remove
  this request-path cost.
- **Fix:** Scan in chunks over several background steps, and keep the running totals in the plan head. Or sample
  the index. Reuse a completed decision scan only when the item state and split arguments still match.

**F8 — done: commit read each lock row 2 times (Low).** **Code.** `listPendingTxKeys` and `getPendingTxOp` for
each key read the same rows. Now `commitLocal` reads the rows of the transaction one time with `listPendingTxItems`. The release is one
`DELETE` by primary key for each key (`deletePendingTxKeys`), which is one write for each item, so it is acceptable.

**F9 — TTL sweep (Low).** **Code + Plan.** The plan is good: a covering index scan on `idx_items_ttl` and an
anti-join on the PK of the locks. Expired rows that are locked stay at the head of the index and are read again
each cycle, but locks are few.

**F10 — done: the stale queries read 2 rows for each lock copy (Low–Medium).** **Code + Plan.**

- **What happened:** While a promotion was in `cutover`, the stale queries excluded the lock copies of the moved key
  with `hk NOT IN (<fokos.sql.movedHashKeys()>)`, and each copy that the scan stepped past cost one index entry
  and one table row. The code comment measured 20,003 rows read for 10,000 copies.
- **What changed:** The stale queries read `pending_tx_info_due`, one entry for each transaction, with no filter on
  `hk`. The step reads the keys of a claimed transaction and skips it when no key is owned. The runtime call
  `fokos.sql.movedHashKeys()` had no other user, so it is removed.

**F11 — migration reads payload arrays before it applies the byte budget (High).** **Code + Collector.**

- **What happens:** `collectBatch` (`sharding/batch-scan.ts`) fetches a complete array before it checks
  `budgetBytes`. `#buildItemsPage` and `#buildPendingTxPage` (`shared/partition/fokos-migration-host.ts`) both use
  this collector. Their store queries materialize every requested row, including `data`.
- **Example:** The default `migrationPageRows` is 1,000, and `migrationPageBytes` is 20 MiB. With near-maximum
  400 KiB items, one fetch can hold about 390 MiB of payload before the collector applies its byte budget.
  [Workers limits](https://developers.cloudflare.com/workers/platform/limits/#memory) allow 128 MB per isolate.
  The budget bounds the response, not the memory needed to build it.
- **Read cost:** A collector check with logical row sizes of `400 * 1024 - 128` fetched 1,000 rows and returned 51.
  The returned cursor resumes after row 51, so the next fetch reads most of the unused payloads again.
- **Failure:** A split or promotion cuts over, then its source exceeds the memory limit while it builds a page.
  Repeated attempts can fail on the same batch. The target cannot finish its import, so writes and transaction
  steps on its keys remain unavailable. This failure was not run in Workers.
- **Fix:** Stream rows through the byte budget before a payload array can grow beyond it. A slice-aware query
  (F1) does not fix this memory bound. The item and lock streams both need the bound. No schema change is needed.

**F12 — empty hash keys retain their size-estimate rows and index entries (Medium).** **Code + SQL replay.**

- **What happens:** `deleteItem` and `deleteExpiredItems` (`shared/partition/partition-store.ts`) reduce
  `key_size_estimates.est_bytes` to zero, but keep the row. Only promotion completion and cleanup call
  `deleteKeySizeEstimate`. The `key_size_estimates_by_bytes` index also keeps an entry for each empty key.
- **Example:** Create one item under each new hash key, then delete it or let its TTL expire. The estimate table
  grows with all keys ever written, not with live keys. A replay of the create/delete statements in SQLite 3.45.1
  retained 10,000 zero-byte estimate rows after 10,000 cycles.
- **Failure:** A small live dataset accumulates estimate rows and duplicated keys in the index. This increases
  storage and can trigger size-based repartition even when the live items fit below the cap.
- **Fix:** Remove an estimate when its key has no committed item. Keep the removal in the same transaction as the
  item deletion and estimate update. Pending-only keys already use the lock-table fallback in `splitDecision`.
  No schema change is needed.

**F13 — completion deletes all lock copies in one synchronous transaction (Medium).** **Code.**

- **What happens:** `acceptAck` (`sharding/repartition-flow.ts`) runs `beforeComplete` inside the transaction of
  the last acknowledgement. `PartitionDO` then calls `deletePendingTxForHashKey` for a promotion, or
  `deleteAllPendingTx` for a split (`shared/partition/partition-store.ts`). Neither deletion has a batch limit.
- **Example:** A source retains 1,000 transactions with 100 lock rows each. One split completion deletes all
  100,000 rows and their index entries before the acknowledgement returns. The item cleanup job has a row budget,
  but it does not bound this lock deletion.
- **Failure:** Other requests wait for the synchronous deletion and its commit. The completion cost grows with
  the whole copied lock set. TODO: measure the duration at the target transaction count and payload sizes.
- **Fix direction:** Measure this path before selecting a cleanup method. If the work needs stages, start only
  after the targets hold the locks. Preserve the per-transaction key-set rule: remove no partial copy that a
  routed commit or forced resolution can mistake for the complete set. No schema change is required by the finding.

### 4.2 TransactionCoordinatorDO

**C1 — done: `tx_recovery` did a full scan of `tc_state` (High).** **Plan:** `SCAN tc_state` + `TEMP B-TREE FOR ORDER BY`.

- **What changed:** `tc_state` has the column `next_recovery_at` and the partial index `idx_tc_state_recovery
  (next_recovery_at) WHERE completed_at IS NULL`. The job reads its batch and its `deadline()` from this index,
  and `hasNonTerminalRows` is removed. The job is also fair now: each iteration of the step claims one due
  transaction, moves its `next_recovery_at` forward, and drives it. Each drive gets at most
  `fanoutRequestBudgetMs`. Before this change, the oldest transaction with a participant that was down came first
  in each step and used the whole budget of 30 s, and the newer transactions got no drive.

The finding was:

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
  - Done: `stripPayload` rewrote N `tc_items` rows at PREPARED and at CANCELLING, a few milliseconds before
    `completeTransaction` deleted them. Now the request path does not call it. The `tx_recovery` claim calls it
    for a PREPARED, COMMITTING or CANCELLING transaction, in the storage transaction of the claim, and it writes
    no row when the payload is already removed. A transaction that does not complete therefore keeps its payload
    only until its first claim, about `staleTransactionMs` after its creation. `buildMigrationPage` does not
    carry the payload of a transaction in these states. No step after the prepare reads the payload: `runCommit`
    and `runCancel` read only the keys.
  - `prepare_outcome = 'accepted'` and `commit_outcome = 'committed'` add 2P updates. Only the recovery path needs
    them, and a new prepare or commit is idempotent.
  - The move from PREPARED to COMMITTING is a separate write.
  - `partition_context_json` stores a full route context for each participant of each transaction, although one
    coordinator group serves one table.
- **Fix:** Write these values only when the request path cannot finish. That gives about 2N + 2P + 8 writes.
  The `stripPayload` part is done, so a transaction now does about 2N + 4P + 10 row writes.

**C3 — skipped: wide rows in WITHOUT ROWID tables (Low).** **Code + SQL replay.**

- **What happens:** `tc_items.data` (≤ 400 KB), `tc_results.image_data`, the JSON columns of `tc_participants`,
  and `tc_state.results_json` are all in WITHOUT ROWID tables.
- **Measurement:** SQLite with 4 KiB pages, each table built as WITHOUT ROWID and as a rowid table with a UNIQUE
  index:
  - `tc_state`: WITHOUT ROWID uses 1.15–1.18x the storage of a rowid table. For transactions of about 50 ops
    (`results_json` of about 1 KB), it uses 3.26x.
  - Payload rows: 3.13x against 1.40x for 1,500-byte values. For values below 1 KB or above 3 KB, the difference
    is 0–30 %.
  - The large loss occurs only for rows of about 1–2.5 KB. Each such row goes above the ~1,002-byte inline limit
    and gets a private overflow page that is mostly empty.
- **Why the effect is small:**
  - `completeTransaction` deletes the `tc_items` and `tc_participants` rows, and the first `tx_recovery` claim
    removes the payload of a transaction that does not complete (C2). These rows exist only while the
    transaction is in flight.
  - Only `tc_state` (every transaction) and `tc_results` (only condition failures with `all_old`) stay for the
    10-minute idempotency window. At 500 tx/s, `tc_state` holds about 300k rows: about 90 MB as WITHOUT ROWID
    and 78 MB as rowid. This is far below the split threshold, so the layout does not make the coordinator
    split early.
  - In a rowid table, the partial indexes on `tc_state` hold the rowid and not `transaction_id`. The recovery
    job and the idempotency sweep then need one more table read for each row, unless the indexes include
    `transaction_id`.
- **Decision:** Skipped. Convert only `tc_state` if its migration is edited for another reason, for example C2.

**C4 — recovery drives one transaction at a time (Low).** **Code.** Recovery drives transactions one at a time
(FIXME at `:1273`) in a step of up to `alarmRecoveryBudgetMs` (30 s). The split and import jobs of the coordinator
wait behind it (see X2).

**C5 — one PREPARING transaction can exceed the migration RPC limit (High).** **Code, not run.**

- **What happens:** `storePrepareAnswer` (`server/do-transaction-coordinator.ts`) stores every condition-failure
  image that a participant returns. Each participant caps its own answer at 10 MiB. The coordinator applies the
  combined cap only in `cancelTransactionInStore`, when it records `CANCELLING`. Before that transition, one
  transaction can hold more images than the combined cap.
- **Failure, step by step:**
  1. A transaction sends 100 condition checks to ten participants, with `returnValuesOnConditionCheckFailure`
     set to `all_old`. Each participant receives ten operations.
  2. Nine participants each return ten failed-condition images of 390 KiB. The last participant remains pending.
     Each returned answer holds about 3.8 MiB, below its 10 MiB cap. The stored images total about 34.3 MiB.
  3. The coordinator splits while the transaction stays `PREPARING`. After cutover, the source cannot make its
     transition to `CANCELLING`, and its recovery job cannot run locally.
  4. `buildMigrationPage` loads the complete transaction, including all rows of `tc_results`. It includes the
     first transaction even when `migratedTransactionBytes` exceeds the default 20 MiB page budget.
  5. The images alone exceed the
     [32 MiB RPC limit](https://developers.cloudflare.com/workers/runtime-apis/rpc/#limitations).
     The response fails, and the child retries the same transaction. It cannot finish its import or resume the
     transaction to apply the combined cap. Other tokens routed to that importing child also remain unavailable.
- **Verification:** The byte calculation and first-transaction exception were checked outside Workers.
  The split and failed RPC sequence needs a runtime test. Use byte images so string serialization is not a factor.
- **Fix:** Page the rows within a transaction, with a cursor that identifies its table and row position. A complete
  transaction cannot be the smallest migration unit. Apply the import gate until every row arrives. No SQL schema
  change is needed, but the internal migration cursor and page format must change.

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

**X2 — done: a job's own deadline can make it run without pause (High).**

- **Done:** The stale-lock job now has a durable backoff in `next_recovery_at` (F4). The test "asks a coordinator
  that answers driving one time, and arms the alarm at the next attempt" failed before the change and passes
  after it. No other job has a deadline that stays in the past, so the scheduler change (fix 2) is not
  necessary.
- **Done, second problem:** `recoverTransaction` no longer drives the transaction in the call. It makes the
  `tx_recovery` job of the coordinator due and answers `driving`, so a call is one round trip. The setting
  `recoverTransactionBudgetMs` is removed. One step of the stale-lock job starts coordinator calls for at most
  10 s.
- **Found on the way, done:** More than one drive of one transaction can run at the same time. `drivePrepare` and
  `runPrepareRecovery` sent the fan-out of their own decision also when the decision of another drive had won. A
  cancel after a commit decision released a lock, and the participant then answered the commit with the
  idempotent success: the transaction was COMMITTED, but one participant did not apply its write. `runCommit` and
  `runCancel` now send only when the stored state is their decision. Two tests make the other drive win during
  a prepare call. Both tests failed before the change.


- **Cause:** In the scheduler, `#deadlines` uses `min(scheduled, own)`. When the `deadline()` of a job stays in the
  past, the job runs again about 50 ms after each pass, and the step's `nextRunAt` has no effect.
- **Where it happens:** The `stale_tx_recovery` deadline is "oldest unguarded owned lock + 5 s". The lock stays
  in these cases:
  - The coordinator answers `driving`, or a state that the job does not act on.
  - The call to the coordinator fails fast. For example, the lock names a coordinator that split, and its child
    still imports, so the forward answers `partition_migrating`.
  - In both cases the job runs about 20 times per second and sends up to `staleLockScanRows` (10) RPCs each time.
    During a promotion import, each turn also reads about 60,000 rows for 10,000 lock copies (F10).
- **Precedent:** The coordinator hit the same problem and worked around it with a KV key (comment at
  `server/do-transaction-coordinator.ts:471`).
- **Second problem:** Passes run one at a time, and so do the jobs of one pass. One slow stale-lock step (10 locks
  × a 10 s coordinator budget) holds `target_import` and `source_repartition` for up to 100 s.
- **Fix:**
  1. Give each job a durable backoff. The `next_recovery_at` column of F4 does this for the stale-lock job.
  2. Let `nextRunAt` limit how often `deadline()` can make a job due.
  3. Bound the time of host jobs, or run them in a separate pass.
- **First step:** Write a test that holds a lock whose coordinator answers `driving`, and count the job runs.

## 6. Schema changes to decide before the freeze

The SQL migrations can still be edited in place.

- **Breaking:**
  - X1: `split_bucket`, if it is wanted.
  - R11: a policy version in the route context.
  - R7: paged Bloom storage, if it is wanted.
  - R9: finished promotions as override rows only, if it is wanted.
  - K3: the plan head as a column of `fokos_repartitions`, if it is wanted.
- **Additive, but cheapest now:**
  - F5: the delete buckets.
  - R5: an index on `learned_at`, if it is wanted.
- **No schema change:** R3, R4 (and K2), R6, R8, R10, F1, F3, F7, F11, F12, F13, C2, C4, C5 and K6. C5 needs an internal migration cursor and page-format change.

## 7. What was checked and is fine

- The point read and write statements: one seek each.
- The TTL sweep plan (F9).
- The due, cleanup and deadline queries of `fokos_repartitions`, without statistics.
- The idempotency sweep of the coordinator (covering partial index on `completed_at`).
- The token lookups of the coordinator.
- Lock release: one `DELETE` by primary key for each owned key.
- The item cleanup batches of promotions and the cleanup batches of the coordinator ledger. Lock-copy deletion
  at completion is separate (F13).
- The hash arena snapshot: it writes only the used part, and only when the tree grows.
