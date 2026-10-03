# Audit — storage schemas and queries of the sharding runtime and FokosDB

**State:** Closed. No more work is planned from this audit.

- **Done:** K1, K5, F1, F4, F8, F10, F11, F12, F13, C1, C2, C4, C5, X1, X2, R3, R5, R6, R7, R8 and R11.
- **Partly done:** R4 (the override flag and the destroy fence), K2 (the destroy fence) and R10 (the lazy lifecycle
  and the request gate).
- **Spec written, not built:** F5 (`docs/agent-plans/2026-10-03-max-deleted-version.md`).
- **Fix in another RFC:** F7 (`docs/agent-plans/2026-09-30-item-size-facts-and-range-split.md`).
- **Decided, no change:** F6, K3, K4, K7, and the `split_bucket` column of X1.
- **Skipped:** C3.
- **Postponed:** R9.
- **Open, not planned:** F3, K6, the import record of R4 and K2, and the deadline reads of R10. Each section says
  what a later fix must do.

**Date:** 2026-09-29
**Updated:** 2026-10-03.

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
- **Local benchmark:** a test Worker ran in local workerd (`wrangler dev`, SQLite-backed DO, state on disk). It
  does not include the replication of a production commit.

## 2. Summary

| # | Layer | Finding | Severity | Schema change? |
|---|---|---|---|---|
| F1 | FokosDB | Done. A promotion or a range split read the whole source table, including `data`, for each target. A promoted key was not available for writes during that time | High | No |
| F11 | FokosDB | Done. Migration fetched payload arrays before the byte budget; a default fetch could hold about 390 MiB | High | No |
| C5 | FokosDB (TC) | Done. One PREPARING transaction could exceed the 32 MiB migration RPC limit and stop an import | High | No |
| X1 | Cross | Done. A hash split read the whole source once for each child, with a per-row hash and a per-row JOIN | High | No (decided) |
| X2 | Cross | Done. The stale-lock job could run again every ~50 ms, and a slow step blocked the split and import jobs | High | Done with F4 |
| C1 | FokosDB (TC) | Done. `tx_recovery` did a full scan and sort of `tc_state` every 5 s | High | Additive index |
| F5 | FokosDB | Spec written, not built (`docs/agent-plans/2026-10-03-max-deleted-version.md`). One partition-wide delete counter makes read transactions abort on unrelated deletes | High | One column replaced |
| F3 | FokosDB | Split sources keep all item rows for life: depth d keeps d+1 copies of the data | High (cost) | No |
| F4 | FokosDB | Done. `pending_transactions` repeated per-transaction data on each key; `conditions_json` was never read; the stale queries stepped past lock copies | Medium | Yes |
| F7 | FokosDB | The range-boundary scan blocks the request path and runs again during planning. The fix is part of the item-size RFC | Medium | No |
| F12 | FokosDB | Done. Empty hash keys kept their size-estimate rows and index entries | Medium | No |
| F13 | FokosDB | Done. The last migration acknowledgement deleted all lock copies in one synchronous transaction. Now the source cleanup deletes them in bounded steps | Medium | No |
| R3 | Runtime | Done. The router facts are read once for each request or owner check, not for each key | Medium | No |
| R5 | Runtime | Done. `learnRangeBoundary` counted the whole table on each insert and each refresh | Medium | No |
| C2 | FokosDB (TC) | Done. The commit path wrote P `commit_outcome` rows and a separate PREPARED state that `completeTransaction` deleted or replaced a few milliseconds later. Now about 2N + 3P + 9 row writes | Medium | No |
| C3 | FokosDB (TC) | Skipped. WITHOUT ROWID tables use 3x storage only for rows of about 1–2.5 KB, and most of those rows are short-lived | Low | Yes |
| R7 | Runtime | Done. The Bloom filter stays one KV value with a 128K first layer (~172 KB). The runtime writes it at most once per `promotionBloomFlushMs` (5 s), with `allowUnconfirmed`, in place of once for each new promoted key | Medium | No |

----

The next most important items are the ones that can stop an import. A stuck import keeps keys unavailable. After those come the schema changes that you must decide before the freeze. F1, F11 and C5 are done. F5 is still open: deletion_metadata has only one counter row. The spec `docs/agent-plans/2026-10-03-max-deleted-version.md` addresses it.

Priority 1: stuck imports (availability)

1. F11: done. The store queries give the migration rows one at a time, and the byte budget stops the SQL read.
2. F1: done. A promotion and a range split read only the key range of their slice, with the same generators as F11. X1 is done: a hash child walks the source one hash key at a time.
3. C5: done. The coordinator applies the combined image cap when it stores each prepare answer, so one transaction stays below the RPC limit.

Priority 2: correctness under load (and cheapest now)

4. F5: the highest deleted item version. Spec written: `docs/agent-plans/2026-10-03-max-deleted-version.md`. With one delete counter for the whole partition, about 98 % of multi-partition reads abort at 200 deletes per second. The change replaces `delete_revision` with `max_deleted_v`, so a read of a found item compares only its version. It replaces one column and one field of the migration page. It is cheapest to do now.

Priority 3: schema decisions before the freeze

You must decide these now, also when the answer is "no":

5. X1 split_bucket: decided, no column. The hash-key walk gives most of the gain with no schema change (see X1).
6. C3: skipped. The measured gain is small (see C3).
7. R11: done. The route context carries a policy version.
8. C2 keeps one route context for each participant, for transactions across tables. K3 is decided: no change. R9 is postponed, and it does
   not depend on the freeze (see R9).
9. R7: done. The filter stays one KV value, because each row written has a cost. The runtime writes it at most
   once per `promotionBloomFlushMs`. The filter is a cache, so its storage can change at any time.

Priority 4: cost and background work (no schema change)

9. F3: delete the item rows of split sources. At depth d you keep d+1 copies of the data. First measure the cost of a DELETE FROM items with no WHERE clause on Durable Objects.
10. F7. F12 and F13 are done.

Priority 5: small request-path fixes

11. R4/K2 (the import state) and K6: not planned. R6 and C4 are done. Each fix is small and local, and you can do them at any time after the freeze.

The audit is closed. The next step is to build F5 from `docs/agent-plans/2026-10-03-max-deleted-version.md`.


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

**R3 — done: router facts are read again for each key (Medium).** **Code.**

- **What happened:** `routerRole()` called `#split()`, which read the split row and `listRepartitionTargets`: K
  rows with the wide `do_name` and `partition_id` columns. A hash partition read 1 + K rows for each key only to
  learn if it is a router.
- **Step 1, done:** `routerRole()` reads only the split row, and does an exhaustive `switch` on its state.
  `splitTargets()` reads the split row and its target rows. `#split()` is removed.
- **Step 2, done:** `#routerView()` in `sharding/runtime.ts` reads `routerRole()` once, and on a range router
  also the targets as `FokosChild` values. `#groupByOwner`, `#singleOwner` and the range branch of `#planRange`
  read the view once, and pass it to each `#resolve` and to `#guardBloomHits`. These loops are synchronous, so no
  other request can change the split row while they run, and one read gives the same answer as one read for each
  key. A view is never kept across an `await`. A caller that resolves one key reads the view in `#resolve`.
- **Result:** A `txPrepare` of 100 keys on a range router with `rangeSplitN = 4` reads 6 rows only to route,
  where it read 700 after step 1 and about 1,500 before it. On a hash partition, it reads 1 row, where it read
  100.
- **Step 3, done:** `ownerCheck()` in `sharding/runtime.ts` reads `routerRole()` once, and returns a check for
  many keys. A router owns no key, so the check answers false and reads nothing. A range partition that is not a
  router owns every key of its interval, so the check reads nothing either. A hash partition that is not a router
  reads the route override of each hash key, and the override flag of R4 skips that read when the partition has
  no override. `#ownedRows` (`server/do-partition.ts`) and `TransactionParticipant.commitLocal` get a new check for
  each synchronous block, and `ownsByHashKey` still calls it once for each hash key. The callers of the check:
  - `commitLocal`, on each commit. It calls the check only for a lock row outside the request, so on the usual
    path it does not call it.
  - Stale recovery, once for each stale transaction, before and after the call to the coordinator.
  - `debugForceResolveTransaction`.
  - `owns()` stays for one key, for example the token check of the coordinator.
- **Where the reads ran before step 2:**
  - For each key in `#resolve` on a hash partition (`#hashTopologyOwner(key, routerRole())`).
  - For each key in `#resolve` on a range router (`#rangeChildFor`), and for each range request in `#planRange`.
  - In `lifecycle()`.
  - For each hash key in `owns()`.
- **Not done: no cache in memory for now.** A cache of the split row and its targets from `cutover` onwards was
  tried and removed. The set of targets and their slices does not change after cutover, but a target row also
  holds fields that change after cutover (`acknowledged`, `startNotified`, `attempts`, `nextAttemptAt`). A cached
  row returns old values for those fields to a caller that reads them. The state can also change in storage while
  a cache in memory keeps the old answer.
- **Possible later fix:** A cache with a time limit. It keeps the last result of the store read and reads storage
  again after X ms, so the reads have a bound per interval and not per key. Before it is added, decide how long
  an old answer can be correct. An old "not a router" answer is the dangerous direction: the partition then
  serves its own rows for keys that its targets already own.

**R4 — facts that seldom change are read on each request (Low–Medium).** **Code.**

- **What happens:**
  - Done: `#guard` read the KV key `destroying` on each RPC.
  - `#dispatch` reads the KV import record on each RPC.
  - Done: a hash partition ran the `routeOverrideFor` JOIN for each key of each request, also when it had no
    override.
- **Done, the destroy fence:** `FokosShardingStore` keeps the fence in memory for each storage. The first read reads
  storage, and `setDestroying` sets the memory value. `transactionSync` deletes the value when its function throws:
  a fence that a rolled-back transaction wrote would otherwise stay true, and the partition would refuse every
  request. Only `fokosPrepareDestroy` writes the fence, and no host code can call it inside a host transaction.
- **Not done, on purpose: the import record and the jobs record (K6).** A copy in memory of these keys can go out
  of date in two cases that the store cannot detect:
  - A host transaction (`ctx.storage.transactionSync`) that rolls back a runtime write inside it. The store sees only
    its own `transactionSync`, and not the outer transaction. No runtime method that a host can call synchronously
    writes these keys today, but a later method could.
  - A host `storage.deleteAll()` without `ctx.abort()`.
  - A SQLite trigger cannot detect these writes: workerd refuses every statement on `_cf_KV`, also
    `CREATE TRIGGER` (`SQLITE_AUTH`). A trigger can also not call JavaScript to clear the memory copy.
  - **Possible later fix:** a write marks the copy "not confirmed", and reads go to storage until a microtask reads
    storage again. A `transactionSync` function cannot `await`, so the microtask runs after the outermost
    transaction ends. First check how the async `storage.transaction()` behaves on a SQLite-backed object.
- **Done, the override flag:** `FokosShardingStore` keeps a flag for each storage that says if the partition may
  hold a route override. The first lookup reads it with one `SELECT 1 … LIMIT 1`, and `insertRouteOverride` sets
  it to true. No statement deletes an override, so the flag goes only from false to true. An insert that rolls
  back leaves it true, which only costs the usual read. While the flag is false, `routeOverrideFor`,
  `hasRouteOverride` and `hasTerminalRouteOverride` return at once with no read. The flag is kept for the storage
  object, not for one store instance, so every store on the same storage sees an insert.

**R5 — done: `learnRangeBoundary` counted the whole table (Medium).** **Code + Plan.**

- **What happened:**
  - Any `rowsWritten > 0` ran `SELECT COUNT(*)` over `fokos_range_hierarchy`. This included the 60-second refresh
    of a known row, where the table does not grow. SQLite stores no row count, so each count read the whole table,
    and Durable Objects bill each row it reads.
  - Above the bound, each insert scanned and sorted the whole table to evict only 1 row, because `learned_at` has
    no index.
- **Example:** A router serves traffic to 5,000 learned slices, so about 83 refreshes per second. Each refresh
  counted 5–10k entries: about 0.5M row visits per second on a single-threaded DO.
- **What changed** (`sharding/sharding-store.ts`):
  1. The upsert is split into `INSERT … ON CONFLICT DO NOTHING` and, when it inserts nothing, a conditional
     `UPDATE` of `learned_at`. A refresh does not count the rows. A hot row costs two seeks and no write.
  2. A module `WeakMap` keeps the row count for each storage. The first insert of an isolate counts the rows, and
     each later insert adds 1. `deleteLearnedRangeSlice` subtracts the rows it deletes. The count only decides when
     an eviction runs, and the eviction counts the rows exactly again. A write that rolls back can make the count
     wrong, and the next eviction corrects it.
  3. An eviction deletes the oldest rows, deepest first, down to 90 % of the bound (`maxRows - floor(maxRows /
     10)`). One count and one sort then serve the next tenth of the bound in inserts. Below 10 rows the bound is
     exact, as before.
- **Not done, on purpose:** No index on `learned_at`. Each refresh would move its index entry, and the batch
  eviction already makes the sort rare. A counter row was also not used: it costs one more billed read and write on
  each insert, and the eviction counts exactly anyway.

**R6 — done: `findDeepestKnownRangeSlice` read all slices of the key (Low–Medium).** **Plan + test.**

- **What happened:** The plan was `SEARCH PK (hk=? AND start<?)` + `TEMP B-TREE`. The end test was not part of
  the seek, so the query read every learned slice of the hash key that starts at or before the sort key, at all
  depths, and then sorted them. It runs for each key of each request to a promoted key.
- **Example:** One hash key holds 10k learned slices of a binary range tree (depth 14). A lookup read about half of
  them, about 5,000 rows.
- **What changed** (`sharding/sharding-store.ts`): Slices of one depth do not overlap. Thus at one depth, only the
  slice with the largest start at or before the sort key can contain it.
  1. One seek finds the deepest learned depth of the hash key: `ORDER BY depth DESC LIMIT 1`.
  2. From that depth down to depth 1, one statement for each depth. An inner query takes
     `WHERE hk=? AND depth=? AND sk_start_boundary<=? ORDER BY sk_start_boundary DESC LIMIT 1`. An outer filter
     checks the end. The first depth whose slice contains the sort key gives the result. Learned depths start at
     1, because the range root is never stored.
  3. Both statements use `idx_fokos_range_hierarchy_depth` as a covering index, with no temp B-tree.
- **Why the end test is outside the seek:** In the inner `WHERE`, it would make SQLite walk back over the slices
  of that depth until one covers the key. Outside, each seek reads at most one row.
- **Cost:** At most D+1 seeks, and each seek reads at most one row. Eviction gaps do not change this. If rows of
  one depth overlap, the result can only be a shallower slice, which costs one more forward.
- **Measured** (10k rows of one hash key, Python sqlite3, about 2 µs of driver cost for each statement):

  | Case | Old query | Per-depth loop |
  |---|---|---|
  | Full tree learned, random keys | 340 µs, ~5,000 rows | 7.7 µs, ≤ 3 seeks |
  | Path to the key evicted below depth 2 | 579 µs, ~5,000 rows | 42 µs, 14 seeks |

- **Not done, on purpose:** One statement that walks the slices in `sk_start_boundary DESC` order and stops at the
  first slice that covers the key. With all slices learned, it reads 1 row (4.8 µs), but it needs a `depth DESC`
  tie-break, because a left child has the same start as its parent. Its worst case has no bound: when eviction
  removed the path to the key, it reads every learned slice between the covering slice and the key (536 µs, ~5,000
  rows in the gap case above).

**R7 — done: the Bloom filter stays one KV value with a 128K first layer, and its writes are throttled (Medium).**
**Code + Local benchmark + test.**

- **What happened:** `#learn` wrote the whole filter each time a hash partition learned at least one new
  promoted key. One `#learn` call wrote once, also when it learned many keys. A key that the filter already
  held caused no write. The output gate of that request waited for the write. The runtime also reads the whole
  value at each start of the DO (K7).
- **Size:** the default first layer holds 128,000 keys (`PROMOTION_BLOOM_DEFAULT_EXPECTED_KEYS`). It held
  300,000 keys before. Layer i uses the error rate `1 % × 0.5^(i+1)`, so the first layer uses 0.5 %: 172.3 KB
  and k = 8.
- **Example, before the throttle:** A router learns 2,000 promoted keys: 2,000 writes of 172.3 KB, about
  340 MB over its life. With the earlier 300K first layer, it was 2,000 writes of 403.8 KB, about 790 MB. With
  the throttle, the number of writes is at most one per 5 s while the router learns new keys.
- **Layers for each first-layer size** (`tools/bloom-filter-sizing.js`, the 1.5 MB limit of
  `PROMOTION_BLOOM_MAX_BYTES`). `BloomFilter.add` does not create a layer that goes above the limit; it returns
  `Full`.

  | First layer | Layers | First layer size | Size when full | Most keys |
  |---|---|---|---|---|
  | 1K | 9 | 1.3 KB | 1.29 MB | 511,000 |
  | 2K | 8 | 2.7 KB | 1.20 MB | 510,000 |
  | 4K | 7 | 5.4 KB | 1.11 MB | 508,000 |
  | 8K | 6 | 10.8 KB | 1.02 MB | 504,000 |
  | 16K | 5 | 21.5 KB | 943.8 KB | 496,000 |
  | 32K | 4 | 43.1 KB | 837.8 KB | 480,000 |
  | 64K | 3 | 86.2 KB | 715.8 KB | 448,000 |
  | **128K (default)** | 3 | 172.3 KB | 1.40 MB | 896,000 |
  | 256K | 2 | 344.6 KB | 1.10 MB | 768,000 |
  | 300K | 2 | 403.8 KB | 1.29 MB | 900,000 |
  | 512K | 1 | 689.2 KB | 689.2 KB | 512,000 |

- **Cost of more layers (measured, Node 24, `BloomFilter.has`):** a key that is not in the filter checks every
  layer. Each layer costs about 130 ns, most of it for the two `xxhash-wasm` calls. With 9 layers, one lookup
  costs about 1.25 µs. This is less than 1 % of a request.
- **Cost of the size (Local benchmark):** the stored value has the shape of `BloomFilterSnapshot` with random
  bits. Each put changes one byte and writes the whole value again. Values are p50 round trips, from 300
  requests for each row. The cost of the operation is the time above the empty request.

  | Size | Empty request | 1 get | 1 put |
  |---|---|---|---|
  | 1.3 KB | 10.9 ms | 11.2 ms | 14.4 ms |
  | 5.4 KB | 10.9 ms | 11.3 ms | 14.3 ms |
  | 21.5 KB | 10.9 ms | 11.4 ms | 14.7 ms |
  | 86.2 KB | 10.5 ms | 11.4 ms | 15.1 ms |
  | 178.9 KB | 10.7 ms | 11.7 ms | 14.5 ms |
  | 236.0 KB | 10.9 ms | 11.0 ms | 14.8 ms |
  | 284.0 KB | 10.4 ms | 11.8 ms | 14.3 ms |
  | 403.8 KB | 11.1 ms | 11.8 ms | 15.1 ms |
  | 1.32 MB | 10.4 ms | 12.1 ms | 14.9 ms |

  - One put adds about 3.5–4 ms at all sizes. The differences between sizes are smaller than the noise
    (about ±0.5 ms). The commit of the request costs most of the time, not the bytes.
  - One get adds about 0.3–1.7 ms, and it grows a little with the size.
  - 50 operations in one request show the part that grows with the size, because SQLite commits once. Each put
    costs about 0.25 ms at 1.3 KB, 0.37 ms at 403.8 KB and 0.61 ms at 1.32 MB. Each get costs about 0.19 ms,
    0.26 ms and 0.58 ms.
  - The timer in the DO does not move during a request, so the benchmark measures from the client.
- **Not measured:** the replication of a production commit, which can take longer for a larger value. Also not
  measured: a get from a cold start (K7). All gets in the benchmark found the pages in memory.
- **Decision:** use a 128K first layer. It holds 896,000 keys, almost as many as 300K (900,000), and its first
  layer is less than half the size. Locally the size saves less than 1 ms on each write, but a smaller write can
  help when production replication is slow. A filter that exists keeps its stored sizes, because `fromSnapshot`
  reads them.
- **Rejected: the bits in 4 KB rows.** Each row written has a cost, so one learned key would write up to k rows
  in place of one.
- **What changed (throttled write):**
  - `promotionBloomFlushMs` in `FokosRuntimeConfig` (default 5,000) sets the longest time that a new key stays
    only in memory. `#learn` calls `throttleTrailing(...).schedule(flushMs)` (`shared/tsutils.ts`). The first
    call starts a timer, and the calls before it fires do nothing. When the timer fires, `#flushBloom` writes
    the current filter once, so the write includes all keys of the interval.
  - The throttle reads no clock. The Workers runtime does not advance `Date.now()` while code runs, so a check
    of timestamps does not work in a request.
  - `#flushBloom` uses the async `storage.put` with `allowUnconfirmed: true`
    (`putPromotionBloomUnconfirmed`), because the sync KV API has no options. `getPromotionBloom` reads the same
    key with the sync API (Local benchmark: a timer-task write read back with `kv.get`, also after a restart).
  - The write runs in a timer task of its own. If a sync write is in the same implicit transaction, the
    filter write becomes part of a confirmed commit, and `allowUnconfirmed` has no effect. Thus the write does
    not run in the alarm pass or in a request.
  - With `promotionBloomFlushMs = 0`, the runtime writes the filter at once, in the request that learns the
    key. The tests that read the stored filter (`useSmallBloom`) use 0.
  - `fokosDestroy` clears all timers before `deleteAll`, so a write that waits does not run after a destroy.
  - A lost write costs only hints: when the instance stops before the timer fires, each key of that interval
    costs one more forward until the partition learns it again.
- **Local benchmark of the write types** (p50 / p90 round trip in ms, 300 requests per row):

  | Size | No write | sync `kv.put` | async `put` | async `put`, `allowUnconfirmed` | write in a timer task |
  |---|---|---|---|---|---|
  | 5.3 KB | 11.6 / 14.2 | 14.8 / 17.1 | 14.7 / 16.7 | 14.4 / 16.5 | 15.6 / 17.4 |
  | 172.3 KB | 11.7 / 15.0 | 14.3 / 16.4 | 15.0 / 17.1 | 14.9 / 17.3 | 16.0 / 18.0 |
  | 1.32 MB | 10.8 / 12.8 | 15.4 / 17.5 | 15.6 / 18.1 | 15.5 / 17.5 | 16.5 / 18.9 |

  - Locally, `allowUnconfirmed` does not reduce the wait. The probable cause is that local workerd runs the
    SQLite commit on the DO thread; this was not verified. In production the output gate also waits for
    replication, so only a production test can show the gain of `allowUnconfirmed`.
  - The sure gain of the throttle is fewer commits and fewer rows written: at most one per interval.
- **Hash arena:** It uses the same pattern, but it is small and changes seldom (Low).

**R11 — done: policy "last writer wins" could write on every request (Low–Medium).** **Code + test.**

- **What happened:** `#ensureIdentity` wrote `__fokos/policy` each time the request policy was different from the
  stored one.
- **Example:** During a rolling deploy, two Worker versions with different table options send requests in turn.
  Each request wrote the KV key and changed the split thresholds back and forth, on every partition they reached.
- **What changed:**
  - `FokosRouteContext` and `FokosStoredPolicy` have `policyVersion`, a non-negative integer. It covers
    `rangeConfig` and `policy`. `FokosTableOptions.policyVersion` sets it, and the default is 0.
  - `#ensureIdentity` ignores a request with a lower version, and the request runs with the stored values. A
    higher version replaces the stored values. An equal version keeps "last writer wins", so a user who never sets
    the version, or forgets to increase it, gets the earlier behavior.
  - The coordinator stores the route context of each participant, with its version. A late commit or recovery
    that sends an old context therefore does not replace a newer stored policy.
- **Trap:** A rollback to a Worker version with a lower version has no effect on partitions that saw the higher
  one. To go back to earlier options, deploy them with a higher version. The README states this.

### 3.3 Background path

**R10 — the scheduler does more reads than it needs (Low).** **Code.**

- **What happens:**
  - Done: `canSweepLocally` and `canDriveLocally` called `lifecycle()`, which did 4 reads, once for each job and
    each check.
  - The deadlines of `target_import` and `target_ack` each read the import record.
  - Done: each request on an importing target started a pass, also while the import waited for a retry.
- **Why the deadlines are read 2 times:** The read after the steps is necessary, because a step can change a
  deadline. When no job is due, the pass reads the deadlines one time.
- **Done, the lazy lifecycle:**
  - Each field of a `lifecycle()` result reads its storage only when a caller first reads it, and the result keeps
    the answer. `firstActiveRepartition()` runs only for a caller that reads `activeRepartition`.
  - `canRun(lifecycle)` gets one result. `#runnable()` makes a new result for each check, and gives it to all jobs.
    The check is synchronous. The check after the steps gets a new result, because a step can change the facts.
  - The runtime does not know the rules of the host jobs. Each host keeps its own condition.
  - Do not keep a result across an `await`: a field read after the `await` can come from a different state than a
    field read before it.
- **Done, the request gate:** A request on an importing target starts a pass only when `nextAttemptAt` is due.
  While the import waits for a retry, a pass can do no import work: the pass that deferred the import armed the
  alarm at the retry time. The request still restores the fallback alarm, for a lost alarm.
- **Not done:** the import record reads of the deadlines. They need the memory copy of the import record (R4).

**R8 — done: each status page sorted the whole union (Low).** **Plan + test.**

- **What happened:** Each `fokosStatus` page (destroy traversal, walk and admin) built the whole UNION ALL of
  repartitions and targets after the cursor, and then sorted it. Rows are permanent, so a full walk cost
  O(rows² / page size). Only promotions made the list long: a partition splits at most one time, and a split has
  few targets.
- **What changed:**
  - `fokosStatus` is not paged. It returns the identity, the role, the import state, and `split`: the split and its
    targets in `target_index` order. The read uses `idx_fokos_repartitions_split`, and it sorts only the few target
    rows.
  - `fokosPromotions({ cursor })` is a new paged RPC. It walks the `key_promotion` rows by `seq` with a `LIMIT`, and
    a `LEFT JOIN` gives the one target of each promotion. The plan is a seek on `idx_fokos_repartitions_seq` with no
    temp B-tree, so a page reads about `limit` rows. A full walk is O(rows). The config keys are
    `promotionsPageEntries` and `promotionsPageBytes`.
  - `walk` and `destroy` read `fokosStatus`, and then every `fokosPromotions` page. `fokosPromotions` stays
    available behind the destroy fence.
  - The partition suites read the promotion of one key with the `promotedKeyStatus` test op. It calls
    `runtime.promotionState(hashKey)`, which is `routeOverrideFor`: one primary-key read. It also sees the keys
    that a hash child inherited. `PartitionDO.status()` has no `promotedKeys` list now.
- **No promoted-key count in the status:** no index covers `kind = 'key_promotion'`, so a count reads every
  repartition row. Walk reads the status of every partition, so the count would multiply. No caller needs it.

**R9 — postponed: inherited promotions store 3 rows and about 6 copies of the hash key (Low).** **Code.**

- **What happens:** `#applyOverrides` writes a repartition row, a target row and an override row for each key. The
  target row holds `slice_hash_key`, `do_name` and `partition_id`, and the last two also encode the hash key. Each
  later hash split copies the rows again. Only the override is needed to route.
- **Cost:** With a 100 B hash key, one inherited key uses about 880 B and 8 B-tree entries. The override row alone
  is about 110 B. Example: 10,000 promoted keys, each with 3 later hash splits on its path, use about 23 MB in
  place of about 3 MB. Each inherited row is also one more entry in the `fokosPromotions` listing, so walk and
  destroy read more.
- **What reads the inherited rows:**
  - Routing and the override export of the next split need only "a finished override exists". No caller reads the
    `repartitionId` that `routeOverrideFor` returns.
  - `walk` and `destroy` do not need them. The partition that did the promotion keeps its own row for life, so the
    walk reaches the range root through it. `destroy` is post-order, so that partition stays until the range root
    is deleted.
  - The `promotedKeyStatus` test op reads them: a child answers for the keys it inherited
    (`read-through.test.ts` checks this). It reads `routeOverrideFor`, so step 3 keeps that answer.
- **Fix:**
  1. `fokos_route_overrides.repartition_id` can be NULL. NULL means "the promotion finished at an ancestor".
  2. `#applyOverrides` writes only the override row.
  3. `routeOverrideFor` uses a `LEFT JOIN` and answers `cleaned` for NULL. `hasTerminalRouteOverride` and
     `queryTerminalRouteOverridesPage` add `o.repartition_id IS NULL OR …`.
- **Why it does not depend on the freeze:** The `LEFT JOIN` reads the old and the new form, so no second code path
  is needed. A later version can use the marker `''` in place of NULL, because no repartition has that id, and
  then it needs no schema change. The NULL form needs a table rebuild, which is cheap for the small override table.
- **Decision:** Postponed. The change saves storage only on partitions with many promoted keys. Do it when the
  storage or the promotions listing cost becomes real.

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
| `__fokos/policy` | runtime | ~225 B | when a request carries a higher `policyVersion`, or the same version and other values (R11) | each start |
| `__fokos/destroying` | runtime | `true` | once, at the destroy fence | 2 times per `PartitionDO` request (`#api`, `#guard`), 1 time per coordinator request, and several times per pass |
| `__fokos/import` | runtime | a few hundred B; up to ~3 KB with a cursor of large keys | each migration page, retry, start and acknowledgement | 1 time per request (`#dispatch`), 2 times per pass (two job deadlines), `lifecycle()`, and each log line (`#logParams`) |
| `__fokos/jobs` | runtime | a small record | after a job step that changes it; `scheduleJob` when the new time is earlier | 2 times per pass, and in each `scheduleJob`: each accepted `txPrepare`, each coordinator `initiateWrite`, and each completed coordinator transaction |
| `__fokos/cache/hash_arena` | runtime | ≤ 1 MB | when the tree it learns grows | once, at the first forward |
| `__fokos/cache/promotion_bloom` | runtime | ~172 KB at the defaults, up to 1.40 MB | at most once per `promotionBloomFlushMs` (5 s) while it learns new promoted keys (R7) | each start, whole |
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

- `#api` (`server/do-partition.ts`) calls `isFenced()`, and `#guard` reads `destroying` again. Then `#dispatch`
  reads `import` through `isImporting()`. A coordinator request reads 2 keys.
- After a target finishes its import, its record stays in `active` state for life. So every later request still
  reads and deserializes it, only to learn "not importing".
- **Done, the fence:** the memory copy of R4 removes both fence reads.
- **Not done:** the import read. See R4 for why the import record has no memory copy.

**K3 — decided, no change: the plan head can be a column of its repartition row (Low, schema).** **Code.**

- The plan head has the same life as its `fokos_repartitions` row. The queue transaction writes both, and the
  transaction that writes `cleaned` deletes the head. `#hookPlan` reads the row, the targets and the head in each
  hook call, so the head costs one more read each time.
- The chain (`nextKey`) exists to allow a plan above one value. A KV value and a SQL row have the same 2 MB limit,
  so the chain gives no extra room. `deletePlanChain` also does one `get` and one `delete` for each link.
- **Fix:** Store the plan as a last column of `fokos_repartitions` (wide columns go last, as the migration comment
  says). The row read then includes it, and the `cleaned` transaction sets it to NULL. This removes one KV key per
  repartition, and the chain code.
- **Decision:** Do not do this. The KV value stores the plan as an object with structured clone, so the plan can
  hold `Uint8Array` keys and other objects without an encoding step. A column needs an encoding for each value,
  and a schema change for each new field it must query. The KV key keeps the plan format open for future changes.
  The cost stays: one more read in each hook call, and the chain code.

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
- **Not done:** a copy in memory can go out of date when a host transaction rolls back a write. See R4.

**K7 — the Bloom filter is read whole at each start (Low–Medium).** **Code + Local benchmark.**

- The constructor reads and deserializes the whole value (~172 KB at the defaults, up to 1.40 MB) inside
  `blockConcurrencyWhile`. So the first request of each hash partition that ever learned a promoted key waits for
  it.
- In the local benchmark of R7, a warm get costs about 1.0 ms more than an empty request at 178.9 KB, and about
  1.7 ms more at 1.32 MB. A get from a cold start was not measured.
- No change: R7 keeps the filter as one KV value, so the read stays whole.
- A lazy read of the whole value helps less. Each request on a hash partition asks the filter, so the first request
  pays the same cost. Only control calls, for example `fokosStatus`, would skip it.

## 4. FokosDB

### 4.1 PartitionDO

**F1 — done: migration ignored the slice (High).** **Code + Plan + test.**

- **What happened:** `#buildItemsPage` (`shared/partition/fokos-migration-host.ts`) did not use the slice. It paged
  `queryItemsPage` from the first row of `items` to the last row, and it read `data` for each row. The lock stream
  did the same. `queryRangeItemsPage` existed, but nothing in `src/` called it.
- **Example (promotion):** A 1 GB hash partition with 5M rows promotes one key. The target pulled about 500 pages,
  and most pages had no items. The source read about 1 GB for one key.
- **Example (range split):** In a range split with N children, each child read from row 0 to the end of the table.
  The source read the table N times.
- **Why it mattered for availability:** A promotion cuts over while its key holds locks. After cutover, the range
  root is `awaiting_data` or `importing`. Each write and each transaction step on the key answers
  `partition_migrating` until the import ends, and the root cannot sweep its stale locks (`canSweepLocally` is
  false). A commit fan-out that waits longer than `fanoutRequestBudgetMs` (5 s) goes to `tx_recovery`. The import
  of a 250 MB key from a 1 GB partition read the full 1 GB. The time that the hot key was not available followed
  the size of the source partition, not the size of the key.
- **What changed:**
  - `partition-store.ts` has the type `KeyRange` (`hk`, `start` inclusive, `end` exclusive or null).
    `queryItemsPage` and `queryPendingTxPage` take `range: KeyRange | null`. A null range reads the whole table,
    as before. `queryRangeItemsPage` is deleted.
  - The two builders `itemsPageStatement` and `pendingTxPageStatement` make the SQL, and the generators run
    exactly that SQL. The WHERE clause:

    | Case | Items | Locks |
    |---|---|---|
    | no range, no cursor | none | none |
    | no range, cursor | `(hk, sk) > (?, ?)` | `(p.hk, p.sk, p.transaction_id) > (?, ?, ?)` |
    | range, no cursor | `hk = ? AND sk >= ?` | `p.hk = ? AND p.sk >= ?` |
    | range, cursor | `hk = ? AND sk > ?` | `p.hk = ? AND (p.sk, p.transaction_id) > (?, ?)` |
    | range with end | add `AND sk < ?` | add `AND p.sk < ?` |

  - With a range, the cursor replaces `start`. The statement never holds both bounds: SQLite could then seek on
    `start` and check each row after it, and each page would start again at the first row of the slice.
  - The lock cursor stays a row value on `(sk, transaction_id)` after `hk = ?`, because two locks on one key can
    be on the two sides of a page boundary.
  - `buildPage` calls `sliceKeyRange(slice)` once and gives the range to both streams. A promoted key reads
    `[empty sort key, last row of the key]`. A range slice reads `[start, end)`. A hash child gets null, and X1
    gives it the hash-key walk. `belongsToTarget` filters each row of a promoted key and of a range slice.
  - Old cursors get no special handling. An import that is in progress during the deploy can skip rows. This is
    accepted before the release.
- **Plans** (`EXPLAIN QUERY PLAN` in workerd, exact text; SQLite shows `sk>?` also for `sk >= ?`):
  - Items with a range: `SEARCH items USING INDEX sqlite_autoindex_items_1 (hk=? AND sk>?)`, and with an end
    `(hk=? AND sk>? AND sk<?)`. The cursor gives the same text.
  - Locks with a range and a cursor: `SEARCH p USING INDEX sqlite_autoindex_pending_transactions_1 (hk=? AND
    (sk,transaction_id)>(?,?) AND sk<?)`, then `SEARCH t USING INDEX sqlite_autoindex_pending_tx_info_1
    (transaction_id=?)`.
  - No plan has a `TEMP B-TREE`.
- **Tests:**
  - `partition-store.test.ts`, "each page statement seeks on all its bounds": the exact plan of all 12 forms (items
    and locks; no range, a range, and a range with an end; with and without a cursor).
  - "a range page reads only the rows that it returns": 500 rows on each key before and after the slice key, and
    500 rows of the slice key above `end`. An items page reads at most the rows it returns + 1. A lock page reads
    at most 2 × returned + 1, and + 2 with a cursor: the JOIN reads one `pending_tx_info` row for each lock, and
    the seek reads the cursor row and steps past it.
  - The range results for items and locks: the edges, the empty sort key, `end = null`, a cursor, and two locks
    on one key that a page of one row separates.
  - `repartition-flow.test.ts`, with `migrationScanRows: 5`: a promotion next to keys of 50 rows, and a range
    split. Each stream of each target finishes in one pull, and each child receives exactly its interval. Before
    the change, the promotion took 21 item pulls.

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

- **Spec:** `docs/agent-plans/2026-10-03-max-deleted-version.md` addresses this finding. It is not built yet. `docs/ideas/2026-10-03-delete-buckets.md` keeps the
  bucket design below as an idea for later.

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

**F6 — decided, keep both: two indexes on (hk, sk) (Low).** **Code + SQL replay.**

- **What happens:** `items` has `UNIQUE(hk, sk)` and also `idx_items_scan (hk, sk, est_row_bytes)`.
  - Each insert and each delete writes both indexes.
  - Each update that changes the size moves the `idx_items_scan` entry.
  - The keys are stored 3 times. With 1 KB hash keys, that is most of a small row.
- **Why both exist:**
  - The upsert uses `ON CONFLICT (hk, sk)`, so it needs a unique index on exactly `(hk, sk)`.
  - The count queries, the size lookups and the range split boundary scan read only `idx_items_scan`.
- **Why one index cannot do both:**
  - A unique index is unique on all its columns. `UNIQUE (hk, sk, est_row_bytes)` accepts two rows with the same
    `(hk, sk)` and different sizes.
  - SQLite has no `INCLUDE` columns, so an index cannot be unique on `(hk, sk)` and also hold `est_row_bytes`.
- **Measurement:** SQLite 3.45.1, 4 KiB pages, 50k rows with the real columns, random hash keys.
  - The size of each B-tree in the current layout:

    | Item size | `items` | `sqlite_autoindex_items_1` | `idx_items_scan` |
    |---|---|---|---|
    | 100 B | 7.3 MB | 2.1 MB | 2.1 MB |
    | 1,500 B | 97.9 MB | 2.0 MB | 2.2 MB |

  - The total size of the current layout and of a `WITHOUT ROWID` table with `PRIMARY KEY (hk, sk)` and
    `idx_items_scan`:

    | Item size | rowid (current) | WITHOUT ROWID |
    |---|---|---|
    | 100 B | 11.4 MB | 9.8 MB (0.86x) |
    | 1,500 B | 102.1 MB | 225.6 MB (2.2x) |
    | 10,000 B | 492.7 MB | 616.2 MB (1.25x) |

- **Alternatives, and why they are rejected:**
  - Remove `idx_items_scan`, and read `est_row_bytes` from the table. At 1,500 B items, a boundary scan then reads
    the 97.9 MB table and not the 2.2 MB index, about 45 times more pages. The boundary scan already blocks the
    request path (F7), so this makes F7 worse.
  - Change `items` to `WITHOUT ROWID` with `PRIMARY KEY (hk, sk)`. This removes `sqlite_autoindex_items_1` and
    keeps the uniqueness. It is smaller only for very small items, and 2.2x larger at 1,500 B items, for the
    reason that C3 measured. It also removes `item_id`, which links an item to its rows in other tables, and new
    rows no longer go to the end of the table B-tree.
- **Decision:** Keep both indexes. The second index costs about 22 % of the storage at 100 B items and about 2 % at
  1,500 B items, and one more index write for each insert and delete. Examine `WITHOUT ROWID` again only if small
  items become the main workload, and then together with C3.
- **Related:** Each item size change updates `key_size_estimates` and its `key_size_estimates_by_bytes` index.
  The index also stores the hash key, because the table is WITHOUT ROWID. Count both B-tree updates and the extra
  key storage. `largestKeysAtLeast` uses a bounded covering seek, confirmed with `EXPLAIN QUERY PLAN` in SQLite
  3.45.1. A key with no items keeps no entry (F12).

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
- **Fix:** The decision does not need the boundaries. It uses only the fact that boundaries exist, and the scan finds
  only the `skewed_bytes` floor. That floor is a planner defect. The RFC
  `docs/agent-plans/2026-09-30-item-size-facts-and-range-split.md` (sections 1.5 and 4.2.9) removes the floor. Then
  the decision checks only the item count, which reads at most N rows, and only `#plan` scans, one time for each
  split. The planner scan still blocks requests. Measure it before you scan in chunks over several alarm steps.

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

**F11 — done: migration read payload arrays before it applied the byte budget (High).** **Code + Collector + test.**

- **What happened:** `collectBatch` (`sharding/batch-scan.ts`) received a complete array from each fetch before it
  checked `budgetBytes`. `queryItemsPage` and `queryPendingTxPage` (`shared/partition/partition-store.ts`) put every
  row of the `LIMIT`, with `data`, into that array. `#buildItemsPage` and `#buildPendingTxPage` use these queries.
- **Example:** The default `migrationPageRows` is 1,000, and `migrationPageBytes` is 20 MiB. With items of about
  400 KiB, one fetch held about 390 MiB of payload, and the page kept only about 51 rows.
  [Workers limits](https://developers.cloudflare.com/workers/platform/limits/#memory) allow 128 MB per isolate.
- **Failure:** After cutover, the source ran out of memory while it built a page, and each retry read the same
  rows again. The target did not finish its import, so writes and transaction steps on its keys stayed
  unavailable. Also when memory was sufficient, the next page read again the rows that the budget had refused:
  with rows of 100 KiB, each payload was read about 5 times.
- **What changed:**
  - `fetchPage` returns an `Iterable`. `collectBatch` counts the rows of each fetch to find the end of the table.
    A stop at a limit leaves the loop with `break`, which closes the fetch.
  - `queryItemsPage` and `queryPendingTxPage` are generators. Each one reads and decodes a row only when the
    collector asks for the next row. The caller reads the generator in one synchronous block, and does not write
    to the table while it is open.
  - The overrides stream still gives an array. Its rows hold only a hash key.
- **Result:** A page reads at most one row past the byte budget, the item cap or the scan cap. The rows that the
  filter refuses (for example in a hash split) are decoded and released, and they are not kept. Only the row
  that the budget refuses is read again on the next page.
- **Tests:** `batch-scan.test.ts` counts the rows that each fetch gives, and checks that each limit stops the read
  and closes the fetch. `repartition-flow.test.ts` drains a hash split of rows of about 200 KB with a page budget
  of 1 MiB, and checks the number of pages and that each row arrives once. Miniflare does not apply the isolate
  memory limit, so no local test shows the original failure.
- **Not done:**
  - Done with X1: a hash child reads no `data` for the rows of a key that it does not own.
  - The RPC copies the page when it sends it. The peak memory of the source is therefore a multiple of
    `migrationPageBytes`. TODO: measure it in Workers before you increase the default.

**F12 — done: empty hash keys kept their size-estimate rows and index entries (Medium).** **Code + SQL replay + test.**

- **What happened:** `deleteItem` and `deleteExpiredItems` (`shared/partition/partition-store.ts`) reduced
  `key_size_estimates.est_bytes` to zero, but kept the row. Only promotion completion and cleanup called
  `deleteKeySizeEstimate`. The `key_size_estimates_by_bytes` index also kept an entry for each empty key.
- **Example:** Create one item under each new hash key, then delete it or let its TTL expire. The estimate table
  grew with all keys ever written, not with live keys. A replay of the create/delete statements in SQLite 3.45.1
  retained 10,000 zero-byte estimate rows after 10,000 cycles.
- **Failure:** A small live dataset accumulated estimate rows and duplicated keys in the index. This increased
  storage. The split decision reads `databaseSize`, so the rows could start a size-based repartition only
  through the file size, after millions of dead keys at a cap of 1 GB.
- **What changed:** Both delete paths call `#subtractKeySizeEstimate`. It first runs
  `DELETE FROM key_size_estimates WHERE hk = ? AND est_bytes <= ?` with the deleted bytes. When that deletes no
  row, it runs the old `UPDATE`. The estimate is exact and each item row has more than 0 bytes, so a total at or
  below the deleted bytes means that the key has no committed item. Both statements run in the transaction of
  the item deletion. A key that is written again gets a new row from the upsert. The TTL sweep calls the helper
  once for each key of its chunk.
- **Cost:** No extra write. The delete of the last item of a key does one write, as before. The delete of
  another item reads one more row.
- **Tests:** `partition-store.test.ts` checks that the delete of the last item removes the row, that a new write
  starts a new estimate, that the TTL sweep removes the rows of the keys it empties, and that deletes through
  the transactional form leave no rows. This includes a delete of a row that is already gone.

**F13 — done: completion deleted all lock copies in one synchronous transaction (Medium).** **Code + test.**

- **What changed:**
  - `beforeComplete` deletes no lock row. For a promotion it deletes only the size estimate of the key.
  - `cleanupSourceStep` calls `deletePendingTxCopiesBatch` (`shared/partition/partition-store.ts`) for a split
    and for a promotion. For a promotion, the step deletes the lock copies of the key first, and the items after
    them. Each step deletes at most `cleanupTxLockCopyRows` (1,000) lock rows, every `cleanupRetryMs` (5 s).
  - The method deletes all copies of one transaction in one statement, so a transaction never keeps only a part
    of its copies. It can delete up to 99 rows more than the budget, because one transaction has at most
    `MAX_ITEMS_PER_TX` rows.
- **Why not measure first:** A local run does not show the real cost. In production, the WAL frames of a large
  delete go to the durability followers before the output gate opens, and every request of the partition waits.
- **Why the copies can stay after `completed`:** A split source is a router, and the stale-lock job does not run
  on a router. A promotion source skips a transaction with only copies, and a later hash split does not copy
  the promoted key, because `belongsToTarget` refuses a key with a terminal route override.
- **Tests:** `partition-store.test.ts` checks that a step deletes whole transactions and keeps the rows of
  another hash key. `repartition-flow.test.ts` checks that the acknowledgement deletes no copy, and that the
  cleanup deletes the copies before the items of a promoted key.

The finding was:

- **What happened:** `acceptAck` (`sharding/repartition-flow.ts`) runs `beforeComplete` inside the transaction of
  the last acknowledgement. `PartitionDO` then calls `deletePendingTxForHashKey` for a promotion, or
  `deleteAllPendingTx` for a split (`shared/partition/partition-store.ts`). Neither deletion has a batch limit.
- **Example:** A source retains 1,000 transactions with 100 lock rows each. One split completion deletes all
  100,000 rows and their index entries before the acknowledgement returns. The item cleanup job has a row budget,
  but it does not bound this lock deletion.
- **Failure:** Other requests wait for the synchronous deletion and its commit. The completion cost grows with
  the whole copied lock set. TODO: measure the duration at the target transaction count and payload sizes.
- **Fix direction:** If the work needs stages, start only after the targets hold the locks. Preserve the
  per-transaction key-set rule: remove no partial copy that a routed commit or forced resolution can mistake for
  the complete set. No schema change is required by the finding.

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

**C2 — done: writes on the happy path that it does not need (Medium).** **Code + test.**

- **What happens:** One transaction with N items and P participants does about 3N + 4P + 10 row writes. The extra
  writes are:
  - Done: `stripPayload` rewrote N `tc_items` rows at PREPARED and at CANCELLING, a few milliseconds before
    `completeTransaction` deleted them. Now the request path does not call it. The `tx_recovery` claim calls it
    for a PREPARED, COMMITTING or CANCELLING transaction, in the storage transaction of the claim, and it writes
    no row when the payload is already removed. A transaction that does not complete therefore keeps its payload
    only until its first claim, about `staleTransactionMs` after its creation. `buildMigrationPage` does not
    carry the payload of a transaction in these states. No step after the prepare reads the payload: `runCommit`
    and `runCancel` read only the keys.
  - Done: `commit_outcome = 'committed'` added P updates. Now `runCommit` writes them only when some commit does
    not confirm, and only for the participants that confirmed. Then it counts the stored outcomes, because a
    parallel drive can store the rest. When every commit confirms, `completeTransaction` deletes the rows and no
    outcome is written. A missing outcome costs one more `txCommit`, and the participant answers it with the
    idempotent success.
  - Done: the move from PREPARED to COMMITTING was a separate write. Now `markCommitting` moves PREPARING directly
    to COMMITTING, and `runCommit` reads the state and writes no row. An UPDATE that writes the same value again
    also counts as one row written. `runCommit` moves a PREPARED row from older code to COMMITTING.
  - `partition_context_json` stores a full route context for each participant of each transaction, although one
    coordinator group serves one table. This part is decided: keep it (see "Schema" below).
- **Measurement:** `cursor.rowsWritten` in a Durable Object test (miniflare, the SQLite of workerd):
  - Two UPDATEs of one row in one `transactionSync` count 2 rows. The storage does not combine them.
  - An UPDATE that writes the same value again counts 1 row. An UPDATE that matches no row counts 0.
  - An UPDATE of a column in an index counts 2 rows. An UPDATE that only removes the row from a partial index
    counts 1. The counter seems to count inserts into the table and index B-trees, and not deletes.
  - Thus a merge of two states saves a row only if no later step writes the same state again.
- **Result:** A transaction now does about 2N + 3P + 9 row writes on the commit path.
- **Tests:** `do-transaction-coordinator.test.ts`, block "commit outcomes": no outcome when every participant
  confirms; only the confirmed outcomes when one participant fails, and the next drive sends only the missing
  commit; a PREPARED row from older code completes; a participant with no items fails an invariant and gets no
  commit.
- **Keep `prepare_outcome = 'accepted'`:** `markCommitting` decides from the stored answers, not from the answers in
  memory. Two drives of one transaction can run at the same time. A partition can reject the prepare of one drive,
  and then accept the prepare of the other drive. The stored first answer is the rejection, so the transaction must
  cancel, although the second drive saw only accepted answers in memory.
- **Schema, decided: keep one context for each participant.** The coordinator calls each participant, also in
  recovery, with its stored route context: `policy.ns`, `topology.jurisdiction` and `doName` select the stub, and
  the partition checks `partitionId`, `topology` and `policyVersion`. A later feature can add transactions across
  tables. The participants of one transaction then have different topologies and policies, and one context for the
  coordinator cannot reach all of them. A table config for each pair of transaction and table saves more bytes, but
  adds a table, a join on the commit and recovery paths, and code to build the context again.
  - **Measurement:** A root context of a table with default options is 385 bytes as JSON. `topology` (73),
    `rangeConfig` (76), `policy` (147) and `policyVersion` (17) are about 80 % of it, and they are the same for each
    participant of one table. Only the root index is different for each participant.
  - **Possible later fix:** Do not store `schema` (10), `partitionId` (24) and `doName` (30). Build them again from
    the `partition_do_name` column, which already holds the DO name. This saves about 17 % for each participant,
    needs no new table, and works for transactions across tables.
  - **Why it can wait:** A transaction has at most 100 participants, so about 38 KB at most, and
    `completeTransaction` deletes the rows. The stored `policyVersion` also stops a late commit or recovery from
    storing old options on a partition.
  - The other parts of the fix change no schema.

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

**C4 — done: recovery drove one transaction at a time (Low).** **Code + test.**

- **What happened:** `recoverStaleTransactions` drove the due transactions one at a time, in a step of up to
  `alarmRecoveryBudgetMs` (30 s). Each drive to a participant that does not answer waits `fanoutRequestBudgetMs`
  (5 s). Three problems followed:
  - About 6 due transactions with one down participant used a whole step. A healthy transaction that was due
    after them waited.
  - A healthy drive takes about 20–50 ms, so recovery finished about 20–50 transactions per second. For example,
    a restart that left 2,000 transactions unfinished kept their keys locked for about 40–100 s.
  - Passes never overlap. The import and repartition jobs of the next pass waited for the step, up to 30 s.
- **Done:** The step runs `recoveryConcurrentDrives` (default 6) workers. Each worker claims one transaction and
  drives it, and then claims the next one. `claimDueTransactions` is synchronous, so two workers never claim the
  same transaction. Concurrent drives of one transaction were already safe (X2). The default is 6, because a
  Worker has at most 6 outgoing calls that wait for an answer, and the platform queues the other calls.
- **Done:** `alarmRecoveryBudgetMs` is 10 s, not 30 s, so the other jobs wait less.
- **Test:** "drives up to recoveryConcurrentDrives transactions at the same time, so a drive that waits does not
  stop the others". It times out with the old loop.

**C5 — done: one PREPARING transaction could exceed the migration RPC limit (High).** **Code + test.**

- **What happened:** `storePrepareAnswer` (`server/do-transaction-coordinator.ts`) stored every condition-failure
  image that a participant returned. Each participant caps its own answer at 10 MiB. The coordinator applied the
  combined cap only in `cancelTransactionInStore`, when it records `CANCELLING`. Before that transition, one
  transaction could hold up to about 39 MiB of images (100 operations of 400 KiB).
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
     The response fails, and the child retries the same transaction. It cannot finish its import.
- **What changed:**
  - The first stored answer of a participant stays, and a later answer is ignored. After an accepted answer, the
    participant holds its locks until the decision. After a rejection, the transaction can only be cancelled, and
    the cancel releases a lock that a later prepare took. So the stored images of a transaction only grow until
    the decision.
  - `applyImageCap` keeps the images in `opIndex` order up to the cap. An image that is above the cap with the
    stored answers therefore stays above it in the final merge. `storePrepareAnswer` runs the cap over the stored
    answers each time an answer with images arrives. It deletes the images above the cap, and does not write them.
  - The client response does not change, and `answer_json` does not change.
- **Result:** A transaction stores at most 10 MiB of images. With the 4 MiB payload limit
  (`MAX_PAYLOAD_BYTES_PER_TX`), one transaction is about 15 MiB or less with the default key limits. A page holds
  at most `migrationPageBytes`, which is at most the RPC limit, or one transaction.
- **Not done, on purpose:** Paging the rows of one transaction. It changes the internal migration cursor and the
  page format, and the image bound removes the case that the limits allow. Only a table that sets very large key
  limits can still make one transaction large: the keys are stored in `tc_items`, in the answers and in the
  results.

## 5. Issues that cross both layers

**X1 — done: a hash split read the whole source once for each child (High).** **Code + Plan + test.**

- **What happened:**
  - Only a JS hash of `hk` at the partition depth gives the child index. `items` is ordered by `(hk, sk)`, so a
    child found its rows only when the source read every row, with `data`.
  - `belongsToTarget` (`sharding/repartition-flow.ts`) ran a hash and `hasTerminalRouteOverride`, a JOIN, for each
    row.
  - After F1, a hash child still had no key range, so `buildPage` gave the store a null range, and the child read
    the whole table.
- **Example:** A 1 GB partition has 10M rows and `hashSplitN = 4`. The source read about 4 GB, and ran 40M hashes
  and 40M JOINs.
- **Why it matters for availability:** A hash child that imports answers each write on its keys with
  `partition_migrating` until the import ends. With `hashSplitN = 4`, that is 1/4 of the keys of the source.
- **What changed: the hash-key walk.**
  - `PartitionStore.walkItemsByHashKey` and `walkPendingTxByHashKey` (`shared/partition/partition-store.ts`) walk
    `items` and `pending_transactions` one hash key at a time. Both use one generic walk. `nextHashKeyStatement`
    finds the first row of the next key with one seek:
    `SELECT hk, sk FROM <table> WHERE hk > ? ORDER BY hk, sk LIMIT 1`.
  - The walk asks the ownership function one time for each hash key, with the first row of the key. A hash-child
    slice owns whole hash keys, so that answer is correct for each row of the key. The hash and the override
    JOIN therefore run one time for each key, not for each row.
  - For an owned key, the walk reads the rows with the range statements of F1 (`hk = ? AND sk >= ?`). For a key
    that it does not own, it gives one `skipped` entry, and reads no `data` and no `pending_tx_info` row.
  - The cursor has two kinds. `{ kind: "row", row }` holds the row cursor of the stream: `(hk, sk)` for items,
    `(hk, sk, transaction_id)` for locks. The next page continues after that row, also inside an owned key.
    `{ kind: "after_key", hk }` continues after every row of `hk`, so a page can stop after a skipped key.
  - `asHostCursor` checks each cursor that the host reads or receives: the keys must be bytes, and a lock cursor
    must have a transaction id. A key that is not bytes makes the seek `hk > ?` match no row, so the stream would
    end with no error and the target would miss its other rows. With the check, the page fails and applies
    nothing.
  - A skipped key counts as one scanned entry, so `migrationScanRows` also stops a long run of keys that the child
    does not own.
  - `FokosMigrationHost` (`shared/partition/fokos-migration-host.ts`) uses the walk in both streams when the slice
    has no key range, which is only a hash child. A promoted key and a range slice keep the F1 read. One function,
    `collectStream`, collects the pages of both streams.
  - Old cursors get no special handling, as in F1.
- **Rejected: `SELECT DISTINCT hk … LIMIT 100`.** SQLite reads each index entry of each key for DISTINCT, and does
  not skip. Replay in SQLite 3.45.1 with 200 keys of 10k rows: 100 keys with DISTINCT took 33 ms, and 100 seeks
  took 0.3 ms. The two plans have the same text, `SEARCH … COVERING INDEX … (hk>?)`.
- **Rejected: `key_size_estimates` as the list of hash keys.** It has one row for each key, but no schema rule
  keeps it equal to `items`. Promotion completion deletes the estimate of the promoted key before the cleanup
  deletes its rows. A future write path that forgets the estimate would make a split lose rows with no error. The
  seeks on the items index read the real rows, at the same cost.
- **Rejected: a `split_bucket` column.** It puts `hashChildIndex(hk, depth, hashSplitN)` first in the key indexes,
  so the rows of each child are one index range. It repeats one value for each row, but the walk needs it one time
  for each key. Each write and each query must compute it. `UNIQUE(split_bucket, hk, sk)` also stops the schema
  from enforcing a unique `(hk, sk)`: a write with a wrong bucket makes a second row for one key. After the walk,
  the bucket only saves one seek for each key of a sibling.
- **Result:** Each child reads the `data` of its own rows and locks one time. For each key of a sibling, it does one
  seek in each table. A lock row can hold a payload of up to 400 KB, so the lock stream gains as much as the items
  stream.
  The number of pages does not change, because `migrationPageRows` limits each page.
- **Tests:**
  - `partition-store.test.ts`, "the hash-key walk of items …" and "the hash-key walk of locks …": one ownership
    call for each key, the limit inside an owned key, and a cursor after a skipped key. The lock test also stops
    a page between two locks of one key.
  - "the next-key seek reads one index entry, also after a key with many rows": for both tables, the plan is a
    covering seek on `hk>?`, and `rowsRead` is 1 after a key of 500 rows.
  - `repartition-flow.test.ts`, "lets a hash child step past each hash key of a sibling with one scanned entry, in
    both streams": with `migrationScanRows: 5`, a child with 6 rows and 6 locks next to 10 sibling keys of 100 rows
    and 100 locks imports in 4 item pulls and 4 lock pulls. Before the change, the import did not finish in 50
    pulls.
  - "rejects a page whose next cursor has no key, and applies nothing": three bad cursors, and the child keeps its
    cursor.
- **Not done:**
  - `migrationScanRows` stays 10,000. A skipped key now costs one seek, so a higher limit can be correct. Change it
    one time for all streams and for the coordinator, after a measurement in workerd of how long one page blocks
    the source.

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
  - X1: decided, no `split_bucket`. The hash-key walk needs no schema change.
  - R11: done, a policy version in the route context.
- **Additive, but cheapest now:**
  - F5: `max_deleted_v` replaces `delete_revision` (spec `docs/agent-plans/2026-10-03-max-deleted-version.md`). The migration page carries the new value, so a
    later change must accept pages of the old format during a deploy.
- **Breaking, not tied to the freeze:**
  - R9: postponed. Finished promotions as override rows only. A `LEFT JOIN` reads both forms, and the marker `''`
    needs no schema change.
- **Decided, no change:** C2 keeps `tc_participants.partition_context_json` for each participant. The fields
  that repeat the DO name can go later, with no schema change. R7 keeps the Bloom filter as one KV value, because
  a filter in pages writes more rows.
- **No schema change:** R3, R4 (and K2), R6, R7 (and K7), R8, R10, X1, F1, F3, F7, F11, F12, F13, C2, C4, C5 and
  K6.

## 7. What was checked and is fine

- The point read and write statements: one seek each.
- The TTL sweep plan (F9).
- The due, cleanup and deadline queries of `fokos_repartitions`, without statistics.
- The idempotency sweep of the coordinator (covering partial index on `completed_at`).
- The token lookups of the coordinator.
- Lock release: one `DELETE` by primary key for each owned key.
- The item and lock-copy cleanup batches of repartitions, and the cleanup batches of the coordinator ledger.
- The hash arena snapshot: it writes only the used part, and only when the tree grows.
