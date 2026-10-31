# RFC — Exact item-size facts, item statistics in buckets, and correct range splits

**State:** Draft
**Date:** 2026-10-31

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
  - [4.3 Open questions](#43-open-questions)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)
- [Appendix A. Measurements](#appendix-a-measurements)
- [Appendix B. Walkthrough: the two planner defects](#appendix-b-walkthrough-the-two-planner-defects)
- [Appendix C. Walkthrough: the stop rule of the promotion lookup][appendix-c]
- [Appendix D. Walkthrough: why the bucket size counts items](#appendix-d-walkthrough-why-the-bucket-size-counts-items)
- [Appendix E. Walkthrough: the cost bound of the planner](#appendix-e-walkthrough-the-cost-bound-of-the-planner)
- [Appendix F. Walkthrough: the choice of K](#appendix-f-walkthrough-the-choice-of-k)

[appendix-c]: #appendix-c-walkthrough-the-stop-rule-of-the-promotion-lookup

## 1. Overview and context

### 1.1 Glossary

| Term | Meaning in this document |
| --- | --- |
| item | One committed row of `items` (partition) or `latest_items` (materialization) |
| key bytes | `octet_length(hk) + octet_length(sk)` of one item |
| `item_bytes` | The exact encoded size of one item: stored data plus key bytes |
| estimate | The storage bytes of one item or of a group of items, from the storage model |
| coefficient | One of the four constants of the storage model (section 4.2.3) |
| facts | The exact sums of a group of items: count, item bytes, key bytes, and the TTL parts |
| bucket | One row of `item_stats`: the facts of the items of one hash key in one sort-key range |
| hash leaf, range leaf | A partition that owns hash keys, or one slice `[start, end)` of one promoted hash key |
| `K` | The target item count of a bucket. The value is 2,048 |
| `C`, `N`, `B` | The item count of a slice, the child count of a range split, the estimate of a slice |

### 1.2 Current size accounting

`shared/partition/item-size.ts` defines one formula, and `items.est_row_bytes` stores its result:

```text
est_row_bytes = octet_length(stored data) + octet_length(hk) + octet_length(sk) + 108
```

Five consumers use this one value:

| Consumer | Code |
| --- | --- |
| Item limit: put, update, prepare | `PartitionStore.upsertItem`, `expression/plan.ts`, `measureItemBytes` |
| The evaluated-byte budget of `queryItems` | `query/query-collector.ts` |
| The byte budget of an export call | `partition/materialization-export.ts` |
| The per-key total for promotion | `KeySizeStats`, `signalGrowth`, `largestAtLeast` |
| The weights of range boundaries | `split-planner.ts` `planRangeBoundaries`, `PartitionStore.sortKeySizes` |

`latest_items` of the materialization DO uses a second formula, `data + 2 · key bytes + 60`, and the same
`SplitPlanner`.
`key_size_estimates(hk, est_bytes)` holds one total for each hash key, with an index on `est_bytes`.

### 1.3 Problems

1. **One value has five meanings.** A change of the constant 108 changes which items are valid. An item that
   prepare accepted can then fail at commit, and a commit must not fail. Query pages and export batches also change.
2. **The estimate is too low for long keys.** SQLite stores each key in the table, in the UNIQUE index, and in
   `idx_items_scan`, and in the TTL index when the item has a TTL. The formula counts the key one time.
   For a 408-byte key and 100 bytes of data, the formula gives 616 bytes, and the file grows by 1,610 bytes
   (appendix A.1). GSI rows have long physical sort keys and little data, so they get this error the most.
3. **Each row stores the constant.** A change of the constant leaves old rows at the old value.
4. **`latest_items` has a different formula**, and its range scan reads table rows, because no index covers it.
5. **The planner has two defects** (appendix B). It counts the bytes of the crossing item in the lower child.
   It also reports a false `skewed_bytes` floor, for example for weights `[111, 5111, 111]` and `N = 3`.
   At that floor, the leaf refuses each write until an operator acts, although a valid split exists.
6. **The split decision scans the slice on the request path.** `signalGrowth` signals `evaluateSplit` after each
   applied write, and admission requests it for each refused write. On a range leaf above its cap,
   `SplitPlanner.evaluateSplit` then scans the slice with `planRangeBoundaries` for each write, until the split row
   exists. At the false floor, each write scans again. `#plan` then scans again.
7. **The planner scan has no bound.** It costs about 1.2 µs for each row (appendix A.4). A leaf near the 10 GB limit
   of a Durable Object holds about 70M small rows. The scan then takes more than the 30 s CPU limit, and each try of
   the plan fails (appendix E.3).
8. **The summary hides drift.** `KeySizeStats` clamps with `MAX(0, …)`. It deletes the row when the bytes reach 0,
   because it has no item count. A transactional delete of an absent item also writes the summary row with no change.
9. **Promotion comes late.** The threshold is absolute, and the estimate is too low. A large key then stays through
   more hash splits, and each split copies the key again.
10. **Change capture depends on the size.** `#captureWrite(…, oldEst === 0)` reads "the old size is 0" as
    "the write created the row".

### 1.4 Why now

The first release is close. After the release, a change of `idx_items_scan` rebuilds an index over each item of each
partition, and a change of the primary key of the summary table rewrites each summary row. On a leaf of 9 GB, such a
rebuild blocks every request of the partition. Before the release, the change edits the schema migrations in place,
and the deployment uses new namespaces.

## 2. Goals and requirements

### 2.1 In scope

- Separate the exact item size from the storage estimate and from the RPC response estimate.
- Store exact facts for each item and for each bucket. Store no coefficient.
- Price items and buckets with one storage model, with measured coefficients for each table.
- Keep the facts of a range leaf in buckets of `K` to `2K` items, with an inline split and merge.
- Find promotion candidates with an index that does not depend on a coefficient.
- Correct the planner, and bound its work with the buckets.
- Remove the scan from the split decision of a range leaf.
- Apply the same design to `latest_items`.
- Optimize the costs for range leaves of 500 MB to 2 GB. Keep leaves up to the 10 GB limit of a Durable Object within
  the costs of section 4.2.13.

### 2.2 Out of scope

- An exact page attribution for each item. The model does not include the page waste of items of 1 KB to 1.5 KB
  (appendix A.1).
- The storage of `item_stats` rows in the estimate of a key. One summary row adds 50 to 264 bytes to a key, which is
  less than 0.001% of a key at the promotion threshold (appendix A.3).
- A storage model for `pending_transactions`, the outbox, or the coordinator.
- A generic cost plug-in framework, and placeholder tables for unspecified features.
- A public configuration of the coefficients or of `K`.
- An upper limit on `rangeSplitConditions.maxSizeMb`. The buckets bound the planner at each cap (appendix E).
- Changes to routing names, key encoding, ownership rules, or the repartition protocol.
- The deletion of the item rows that a split source keeps.

### 2.3 Requirements

1. The item limit must compare `item_bytes` with `MAX_ITEM_BYTES`. No coefficient can change that result.
2. Prepare and commit must apply the same item-validity rule.
3. The query evaluated-byte budget and the export byte budgets must use `item_bytes`. Response budgets keep their
   RPC estimates.
4. After each commit, the facts of each bucket must equal the SQL sums over the items of its sort-key range.
   The one exception is a defect, which the repair of section 4.2.7 corrects.
5. Each path that changes an item must change its facts in the same storage transaction.
6. A migration retry must add facts only for the rows that it inserts.
7. A change of a coefficient must reprice each item and each bucket with no rewrite of a row.
8. Size scans, count checks, bucket splits, and recounts must read only covering indexes. They must not read item data.
9. A hash leaf must keep exactly one bucket for each hash key that has items.
10. After each commit on a range leaf: each bucket must hold at most `2K` items, each two neighbour buckets of a key
    must hold at least `K` items together, and the first bucket of a key must have `lower_sk = x''`.
11. The promotion lookup must return every key whose estimate is at or above the threshold, up to
    `PROMOTION_CANDIDATES_MAX` (5) keys, largest first.
12. The write-path promotion check and the promotion lookup must compare the same estimate with the same threshold.
13. With at least `N` items, the planner must return `N - 1` strictly increasing boundaries and `N` non-empty children.
14. Each boundary must be `KeyCodec.shortestSeparator` of two adjacent keys in canonical byte order.
15. The planner must read its bucket rows and its scanned rows in one `transactionSync`.
16. The split decision of a range leaf must read at most `N` index entries.
17. The physical split trigger and the admission guard must continue to use `sql.databaseSize`.
18. No test can need a new production hook.

## 3. Milestones

The change ships as two milestones. Both milestones must ship before the first release. No delivery dates are set.

1. **M1 — Exact facts, storage model, promotion, and the corrected planner.** Sections 4.2.1 to 4.2.5 and
   4.2.7 to 4.2.12. Each leaf keeps one bucket for each key. The planner uses the streaming scan of section 4.2.9.
   The planner change and the split decision of section 4.2.10 ship together.
   M1 alone must not be released: its planner scan has no bound, and `maxSizeMb` has no upper limit.
2. **M2 — Buckets on range leaves.** Section 4.2.6, the bucket groups of the import and of the TTL sweep, and the
   bucket path of the planner. M2 changes no schema. No range leaf from M1 reaches a release, so M2 converts no data.
   A development namespace from M1 must be destroyed: the first split of its one large bucket reads the whole slice.

## 4. Proposed solution

### 4.1 High-level overview

Each item stores one exact fact: `item_bytes`, the stored data plus the key bytes. The item limit, the query budget,
and the export budgets use only this fact.

A new table `item_stats` holds the facts of groups of items: the item count, the item bytes, the key bytes, and the
same two values for the items with a TTL. A row of `item_stats` is a bucket: the items of one hash key from its
`lower_sk` up to the `lower_sk` of the next bucket.

- On a hash leaf, each key has one bucket. That bucket is the summary of the key.
- On a range leaf, the one key has many buckets of `K` to `2K` items. The write that adds an item splits a bucket
  above `2K` items in its own transaction, and a delete merges two small neighbours.

One storage model prices a row or a bucket from its facts. It counts each key copy in each index and the cost of a TTL.
The coefficients are constants in the code, so a new coefficient reprices every row and bucket at once.

The facts feed two decisions:

- **Promotion.** The lookup walks an index on the exact `item_bytes` of each hash-leaf bucket, from the largest.
  It prices each key with the model, and it stops at a proven bound.
- **Range split.** The planner reads the bucket rows of the slice and calculates the byte target of each boundary.
  It then scans only the bucket that holds each target, and it selects the exact gap between two items.

The split decision of a range leaf counts at most `N` items. Only the planner, in the alarm, reads more.

```text
items.item_bytes (exact) ───────────────► item limit, query evaluated bytes, export budgets
        │ same transaction as the write
        ▼
item_stats buckets (exact facts)
        │
        ├── hash leaf: one bucket per key ──► storage model ──► promotion lookup and write-path check
        │
        └── range leaf: K..2K items per bucket ──► storage model ──► planner:
                                                                     bucket rows + one scan per target bucket

sql.databaseSize ───────────────────────► split trigger and admission (unchanged)
```

### 4.2 Technical details

#### 4.2.1 Size definitions

| Value | Definition | Consumers |
| --- | --- | --- |
| `item_bytes` | `octet_length(stored data) + key bytes` | Item limit, query evaluated bytes, export budgets |
| estimate | The facts priced by the storage model of the table | Promotion, range boundaries |
| response estimate | `estimateItemBytes()` and `estimateProjectedRowBytes()` | Response and migration page budgets |

`item_bytes` keeps the current measurements: UTF-8 bytes for text, BLOB bytes for binary data, JSONB bytes for JSON,
and the one-byte tag of a binary key. An absent sort key has zero bytes. `MAX_ITEM_BYTES` stays 400 KB. The fixed
overhead of 108 bytes leaves the item limit, so an item can hold 108 more bytes than today.

This rule is the encoded-item rule of FokosDB. It does not claim exact DynamoDB size compatibility.
The response estimators must not supply row weights or facts.

#### 4.2.2 Item schema and covering indexes

**`items`** (the partition):

- Replace `est_row_bytes` with an ordinary `item_bytes INTEGER NOT NULL` column. Each writer calculates it in SQLite,
  over the same expression that produces the stored data.
- Replace `idx_items_scan` with `(hk, sk, item_bytes, ttl_epoch_utc_seconds)`.
  `ttl_epoch_utc_seconds` is a plain column and the last column. Do not index `ttl_epoch_utc_seconds IS NOT NULL`:
  an indexed expression can stop SQLite from using the index as a covering index.
- Keep the rowid table, `item_id`, the UNIQUE key constraint, and the partial TTL index.
- Do not use a generated column for `item_bytes`. SQLite does not use an index with a generated column as a covering
  index.

**`latest_items`** (the materialization DO):

- Replace `est_row_bytes` with `item_bytes`, with the same definition. A tombstone has no data, so its `item_bytes`
  is its key bytes.
- Add the covering index `(base_hk, base_sk, item_bytes, ttl_at)`.

**The old-facts read.** A write reads the old facts of its item with one covering seek:
`SELECT item_bytes, ttl_epoch_utc_seconds FROM items INDEXED BY idx_items_scan WHERE hk = ? AND sk = ?`.
The presence of the row is the "created" signal of `#captureWrite`, in place of `oldEst === 0`.

#### 4.2.3 Storage model

The estimate of one item is:

```text
estimate = item_bytes
         + key_index_factor * key_bytes
         + base_overhead
         + (TTL set ? ttl_key_factor * key_bytes + ttl_overhead : 0)
```

The estimate of a group of items uses the facts of the group. It equals the sum of the item estimates:

```text
estimate = item_bytes + key_index_factor * key_bytes + base_overhead * item_count
         + ttl_key_factor * ttl_key_bytes + ttl_overhead * ttl_item_count
```

| Coefficient | `items` | `latest_items` | What it covers |
| --- | --- | --- | --- |
| `key_index_factor` | 2.5 | 2.5 | The key copies in the UNIQUE index and in the covering index |
| `base_overhead` | 50 | 40 | The integer columns, record headers, rowid, and index entry headers |
| `ttl_key_factor` | 1.25 | 0 | The key copy in the partial TTL index |
| `ttl_overhead` | 20 | 4 | The fixed part of the TTL index entry, or of the `ttl_at` value |

- Each coefficient is a multiple of 1/4. Thus a double holds each sum of integer facts exactly, up to 2^51.
- Each index stores 2 copies of a key, but its pages are not full, so `key_index_factor` is 2.5 and not 2.
- The model is within 5% of the measured file growth for data up to 100 bytes, with or without a TTL (appendix A.1).
- For data of about 1,000 bytes, the model is 5% to 23% too low, because such rows fill a 4 KB page badly.
  A boundary uses the weights of one slice relative to each other, so a constant error moves no boundary.
- `shared/partition/item-size.ts` defines the `item_bytes` expression, the model type, and the two coefficient sets.
  No other module holds a coefficient.

#### 4.2.4 The `item_stats` table

```sql
CREATE TABLE item_stats (
  hk             BLOB    NOT NULL,
  lower_sk       BLOB    NOT NULL,   -- inclusive lower bound of the bucket; x'' for the first bucket
  item_count     INTEGER NOT NULL,
  item_bytes     INTEGER NOT NULL,
  key_bytes      INTEGER NOT NULL,
  ttl_item_count INTEGER NOT NULL,
  ttl_key_bytes  INTEGER NOT NULL,
  PRIMARY KEY (hk, lower_sk)
) WITHOUT ROWID, STRICT;

CREATE INDEX item_stats_promotion ON item_stats (item_bytes) WHERE lower_sk = x'';
```

- **Bucket rule.** The bucket of an item is the row of its `hk` with the greatest `lower_sk <= sk`.
- **First bucket.** The first bucket of a key has `lower_sk = x''`, the smallest encoded sort key. It stays while the
  key has an item. A merge never deletes it, because a merge deletes the upper row.
- **Hash leaf.** Each key has only its first bucket. A hash leaf never splits a bucket.
- **Range leaf.** The one key has buckets that cover its slice. The import builds them, and each write keeps them
  inside the band of section 4.2.6.
- **The promotion index** holds only first buckets. A range leaf never reads it.
- One class, `ItemStats` in `shared/partition/item-stats.ts`, owns the table, its migrations under its own KV key,
  and the split, merge, and recount statements. It replaces `KeySizeStats`. The host gives it the item table, the
  covering index, the key and TTL column names, the leaf kind, and the coefficient set. These are fixed SQL fragments
  from the code, never user input.

#### 4.2.5 Fact updates

Each write path changes the facts with the bucket helper of `ItemStats`, in the transaction of the item change.

| Operation | Fact change |
| --- | --- |
| Insert | Add the new facts and 1 to `item_count`. On a range leaf, then apply the split check |
| Overwrite or update | Add the new facts minus the old facts. `item_count` stays |
| Present-item delete | Subtract the old facts and 1 from `item_count`; then merge on a range leaf |
| Delete of an absent item | No statement |
| TTL sweep | Subtract the facts of each deleted row from its bucket. On a range leaf, then apply the merge check |
| Migration insert | Add inserted-row facts once per bucket and page; then split on a range leaf |
| Migration retry | Nothing for a row that is already present |
| Promotion `beforeComplete` | Delete the `item_stats` rows of the promoted key |
| Source copy cleanup | No change. The copies have no bucket after `beforeComplete` |
| Prepare and cancel | No change. Pending rows are not committed items |
| Commit apply | The same as the insert, overwrite, update, or delete that it applies |
| TTL change from null to a value | Add the item to `ttl_item_count` and `ttl_key_bytes`. The reverse subtracts it |
| Read-stamp change (`check`) | No change |

**Bucket lookup.** On a hash leaf, the helper writes the row `(hk, x'')`. On a range leaf, it updates the bucket with
one seek:

```sql
UPDATE item_stats SET item_count = item_count + ?, item_bytes = item_bytes + ?, …
 WHERE hk = ?1 AND lower_sk = (SELECT MAX(lower_sk) FROM item_stats WHERE hk = ?1 AND lower_sk <= ?2)
 RETURNING …
```

When no bucket exists for the key, the helper inserts the first bucket. When the facts of a write do not change, the
helper writes no row. An update writes the row and its index entry, also when the values stay the same.

**Delete of an unrepresented row.** When a delete removes a row whose key has no bucket, the delete changes no facts.
The bucket did not represent the row, so a subtraction makes the facts wrong.

**Empty buckets.** A bucket that reaches 0 items is below `K`, so on a range leaf the merge check of section 4.2.6
runs on it before its row is removed. When no merge applies — each pair with the empty bucket already holds at least
`K` items — the helper deletes the row if it is not the first bucket. Each neighbour then holds at least `K` items on
its own, so the pair that the removal makes adjacent also holds at least `K` together. When the empty bucket is the
only bucket of the key, the helper deletes it: the key then has no items. An empty first bucket that is not alone
stays, and the merge check applies to it like to every other bucket.

**Migration.** The import keeps its page grouping. It must not write one bucket update for each imported item.
Do not replace the bulk paths with item-table triggers: a trigger adds one row write for each row of those paths.

#### 4.2.6 Bucket split and merge (range leaf only)

The split and the merge run in the transaction of the write that makes them necessary. No job and no signal exist.

**Split.** After an insert, while a bucket holds more than `2K` items:

1. Find the first sort key of the upper half with one covering seek, `ORDER BY sk LIMIT 1 OFFSET floor(count / 2)`.
2. Calculate the facts of the lower half and of the upper half with two aggregates over the covering index.
3. Write the lower facts to the old row. Insert a row at `lower_sk = <first sort key of the upper half>`.

- The new `lower_sk` is a real sort key, not a shortest separator. A bucket bound is not a DO name.
- Step 2 counts both halves from the items. Thus each split also repairs drift in that bucket.
- A split reads about `3K` index entries in SQLite and reads no row into JavaScript. At `K = 2,048` it takes 0.70 to
  0.86 ms (appendix A.4). A bucket splits at most once for each `K` inserts into it.
- An import page can add more than `K` items to one bucket, so the split repeats while a half holds more than `2K`.

**Merge.** After a delete, while the changed bucket holds fewer than `K` items — also at 0 items, before the
empty-bucket rule of section 4.2.5 removes the row:

1. Read the previous and the next bucket rows with one seek each.
2. When the bucket and one neighbour hold fewer than `K` items together, add the upper facts to the lower row,
   and delete the upper row. Select the smaller neighbour.
3. Repeat with the merged bucket until each pair with it holds at least `K` items.

A merge reads no item. The band prevents thrash: a split gives two halves of about `K` items each, so the pair holds
about `2K`. A merged bucket holds fewer than `K` items, so it needs more than `K` inserts before it splits again.

**Failure mode.** The split and the merge only read and write small rows, so they add no failure cause to a write.
A commit apply can still get `SQLITE_FULL`, as it can today.

#### 4.2.7 Drift repair

When a write would make a fact of a bucket negative, the facts and the items disagree. The write path must not throw:
a throw rolls back a commit apply, the coordinator in `COMMITTING` then retries forever, and the lock stays. The write
path must not clamp with `MAX(0, …)`, because a clamp keeps the wrong facts. In the same storage transaction:

1. Log the defect one time, with the key, the bucket, the stored facts, and the delta.
2. After the item change, count the facts of the bucket again from the covering index.
3. Write the counted facts, or delete the bucket under the rule for empty buckets of section 4.2.5.

On a range leaf, the recount reads at most `2K` entries. On a hash leaf, the bucket is the whole key. The recount runs
only after a defect.

#### 4.2.8 Promotion

The threshold is `T = maxSizeBytes * promotionFraction` of the hash split policy. Both checks below use one shared
estimate function over the facts of the first bucket of the key.

**Write-path check.** The bucket helper returns the facts of the bucket with `RETURNING`. On a hash leaf, the host
calculates the estimate of the key from them and reports a candidate when the estimate is at or above `T`.

**Lookup of the split decision.** It replaces `largestAtLeast`:

```sql
SELECT hk, item_count, item_bytes, key_bytes, ttl_item_count, ttl_key_bytes
  FROM item_stats WHERE lower_sk = x'' AND item_bytes >= ?1 ORDER BY item_bytes DESC
```

1. Bind `?1` to `T / r`, where `r` is the largest possible ratio of estimate to `item_bytes`:
   `r = 1 + key_index_factor + ttl_key_factor + (base_overhead + ttl_overhead) / 1`.
   The smallest item has 1 byte, because a key cannot be empty. `r` is 74.75 for `items` and 47.5 for `latest_items`.
2. Calculate the estimate of each returned key. Keep only the largest `PROMOTION_CANDIDATES_MAX` keys at or above `T`.
3. Return at most `PROMOTION_CANDIDATES_MAX` keys, largest estimate first.

- Each key at or above `T` has `item_bytes >= T / r`, so the walk finds each candidate (appendix C).
- Once the walk holds `PROMOTION_CANDIDATES_MAX` keys at or above `T`, it can stop before `T / r`: a key that is not
  read yet has `item_bytes` below the cursor, and when `r` times that value is below the fifth estimate, the key
  cannot enter the top five.
- The walk reads at most `r * (sum of item_bytes) / T` entries, and never more than the first-bucket count of the
  leaf. In a hash leaf at 1.1 times its cap with `promotionFraction = 0.25`, that is at most about 330 entries for
  `items` and 209 for `latest_items`. 285 entries took 0.585 ms (appendix A.4).
- A cap reduction can make `T / r` smaller than the `item_bytes` of every summary. One pass then reads each first
  bucket of the leaf. Section 4.3.3 covers the cost of that pass.
- The walk runs only while the leaf is over its cap.
- The index is on an exact fact, so a coefficient change rewrites nothing.

**Cache.** `ItemStats` owns one in-memory cache for a hash leaf. The pass and the cache each retain at most
`PROMOTION_CANDIDATES_MAX` keys and their estimates. They must not collect all qualifying keys.

- The cache identifies its threshold, policy, and coefficient values. It contains the current top five, in estimate
  order, or a valid empty result.
- Before each SQL write to `item_stats`, `ItemStats` must invalidate the cache. This covers inserts, overwrites,
  deletes, TTL expiry, tombstone GC, imports, recounts, and the removal at promotion completion.
- A rollback leaves the cache invalid. The next decision can repeat the pass, but must not reuse facts from the
  rolled-back write. The lookup must build and publish the cache from committed facts, outside item-write transactions.
- A change of the threshold, policy, or coefficient values, or the end of a local import, also invalidates the cache.
  Eviction loses the cache and requires a new pass.
- With a valid cache, the decision returns its ranked keys with no summary read. A valid empty result stays valid.
  After invalidation, the next decision over the cap runs a new pass. No cache state needs a SQLite or KV write.

An in-flight promotion keeps its ranked place and answers `already_promoted`. The runtime tries each other key of
the returned set independently. An unfinished promotion alone must not suppress discovery of the other top-five keys.
When `beforeComplete` removes its bucket, that write invalidates the cache and permits discovery of the next keys.

**Retry after a write.** The write-path signal can fail before it queues a promotion. Cache reuse must not depend on
that signal succeeding. The committed fact change already invalidates the cache, so a later decision discovers the
eligible key again. Each decision retries its returned candidates, including after an earlier promotion request failed.

The rest of the decision stays: a hash leaf with one key promotes that key whatever its estimate, and a leaf with two
or more keys promotes its candidates before it splits.

#### 4.2.9 Range planner

The planner keeps one `transactionSync`. Its inputs are the hash key, the owned slice `[start, end)`, and `N`.
On a range leaf, the buckets of the key cover exactly that slice.

**Bucket path (M2).**

1. Apply the count check of section 4.2.10. Return `fewer_items` when the slice holds fewer than `N` items.
2. Read the bucket rows in sort-key order. Price each bucket with the model, and keep the running total before each
   bucket. The total is `B`.
3. Calculate the targets `i * B / N` for `i = 1 … N - 1`, and find the bucket that holds each target.
4. Scan each of those buckets one time, in sort-key order, through the covering index. Start from the running total
   before the bucket, add each item estimate, and select the gap nearest each target. The candidate gaps are the gaps
   inside the bucket, the start of the bucket, and the start of the next bucket. When two gaps have the same distance,
   select the earlier gap.
5. Assign the boundaries in target order, with the same rule as the streaming path: the gap nearest the target after
   the previous boundary. When the gap that step 4 selected for a target is after the previous boundary, it is the
   boundary. Otherwise the boundary is the first gap after the previous boundary, and the planner reads the item
   that ends it: one row for each such step. This collision occurs only when one item weighs at least `B / N`.
6. When a boundary is the last gap and later targets have no boundary, apply the repair of the streaming path: read
   the last `N` items of the slice in reverse order, keep the boundaries before their first gap, and fill the missing
   boundaries from their gaps, latest gap first.
7. Make each boundary with `KeyCodec.shortestSeparator` of the two items at its gap. At the start of a bucket, one seek
   reads the item before `lower_sk`.

The planner reads at most `2C / K + 1` bucket rows. Its scans read at most `min(N - 1, buckets) * 2K` rows for the
buckets of the targets, at most `N - 1` rows for the collision steps, and at most `N` rows for the repair:
at most `min(N - 1, buckets) * 2K + 2N` rows in total (appendix E).

**Streaming path (M1).**

1. Apply the count check.
2. Read the rows of the slice through a streaming covering-index cursor, and price each row with the model.
3. For each target in order, select the gap nearest the target after the previous boundary. Ties select the earlier gap.
4. Keep the last `N` rows of the scan in a buffer. These rows have `N - 1` gaps.
5. Stop the scan when it has `N - 1` boundaries.
6. When the cursor ends with fewer boundaries, repair: keep the boundaries before the first gap of the buffer, and fill
   the missing boundaries from the gaps of the buffer, latest gap first.
7. Make each boundary with `KeyCodec.shortestSeparator`.

**Both paths.**

- For a cut after `k` items, the lower prefix holds only those `k` items.
- A gap is the position between two adjacent items. No gap exists before the first item or after the last item, so each
  child holds at least one item.
- `B` and the facts control only the balance. Wrong facts give a worse balance, but the boundaries stay valid.
- The planner must preserve canonical byte order, strict boundary order, and the slice bounds. It must not use repeated
  `OFFSET` walks in JavaScript or materialize the slice.
- Remove `skewed_bytes`. `RangeSplitPlan` keeps only `fewer_items` as a floor. `SplitPlanner.rangeBoundaries`, the
  `computeRangeBoundaries` hook, returns the boundaries, or `null` for that floor.
- This planner does not give the optimal balance. A child that is still over its cap splits again.

This section replaces the threshold loop of `range-split-boundary-byte-seek.md` and the range-floor rule of
`2026-09-27-over-size-split-trigger.md`.

#### 4.2.10 Split decision of a range leaf

With the planner of section 4.2.9, `fewer_items` is the only range floor. For a range leaf over its cap,
`SplitPlanner.evaluateSplit`:

1. Counts the items of the slice through the covering index with `LIMIT N`. The planner uses the same function.
2. Answers a split when the count is `N` or more, and returns the `fewer_items` floor when it is less.
3. Does not call the planner.

Only `#plan` reads more, one time for each queued split, in the alarm. When deletes reduce the count below `N` after
the decision, the planner returns `null`, and `#plan` waits with backoff, as it does today. The in-memory cache of the
floor result of `2026-09-27-over-size-split-trigger.md` is not necessary.

The planner change and this decision ship together. With the current planner, weights `[111, 5111, 111]` and `N = 3`
pass the count check, and `#plan` returns `null` at each try. The split then never happens.

#### 4.2.11 Item limit, query budget, and export budgets

- `measureItemBytes`, the put guard of `upsertItem`, the update guard of `updateItemSingleShot`, the compiled update
  probe of `expression/runtime.ts`, and the prepare and single-shot checks of `TransactionParticipant` compare
  `item_bytes` with `MAX_ITEM_BYTES`.
- The early client check `validateItemDataSize` stays a lower bound on the data only.
- The query scan gives `item_bytes` to its candidate consumer. The collector charges it for each evaluated item,
  also for an item that the filter rejects. The cursor stays a key position.
- `MaterializationExportConfig.maxBytes` and `TargetRunnerConfig.maxBytesPerStep` count `item_bytes`.

These changes move some page and batch boundaries.

#### 4.2.12 Materialization DO

`MaterializationStore` uses the same machinery as `PartitionStore`: `item_bytes`, the covering index of section 4.2.2,
`ItemStats` with the `latest_items` coefficients, the same fact updates, the same split and merge, the same promotion
lookup, and the same planner. The tombstone GC delete is a delete of a present item.

#### 4.2.13 Performance

All values are from local workerd on an AMD Ryzen 9 7940HS (appendix A.4), with `K = 2,048` and small items of about
150 bytes. Production CPUs can be slower.

| Path | Cost |
| --- | --- |
| Write on a hash leaf | Bucket update: 5.3 µs, 2 rows read, 2 rows written (the row and its promotion index entry) |
| Write on a range leaf | One bucket seek update: 8.7 to 9.0 µs, 3 rows read, 1 row written (2 for the first bucket) |
| Split | 0.70 to 0.86 ms and about 6,150 rows read, at most once for each `K` inserts into a bucket |
| Merge | 2 more seeks for each delete while the bucket holds fewer than `K` items; 2 row writes for a merge |
| Promotion lookup | One pass after cache invalidation; no summary read while the cache stays valid |
| Split decision of a range leaf | At most `N` index entries |
| Planner, `N = 4`, caps of 500 MiB to 2 GiB | About 20 to 33 ms |
| Planner, `N = 4`, each cap up to 10 GB | At most about 100 ms |
| Planner, `N = 255`, each cap up to 10 GB | At most about 1.4 s |
| Bucket table of a leaf at 2 GiB | At most about 14,000 rows, 1 MB to 2 MB |
| Storage of a bucket row | 11 to 16 bytes more than a `key_size_estimates` row today, plus the bytes of `lower_sk` |

The write that splits a bucket blocks the partition for less than 1 ms. The planner blocks the partition for its
duration, one time for each split.

At 1.1 times the cap and the default promotion fraction, a promotion pass reads at most about 330 entries in about
0.7 ms. A cap reduction can make it read each first bucket of the leaf. The cache retains at most five candidates.
Repeated decisions reuse the result only while the facts and the pricing inputs stay unchanged. Each statistics
write invalidates the cache, so a delete, TTL sweep, or promotion completion can require another pass.

The timings use sort keys of 16 and 100 bytes. A longer key makes each row slower to read: from 16 to 100 bytes, a
JavaScript row read costs 11% more, and the split costs 23% more. A longer key also makes each item larger, so a leaf
holds fewer items, and the planner reads fewer bucket rows. Thus small items give the worst case.

#### 4.2.14 Deployment and migration

- Edit the existing schema migrations of `PartitionStore` and `MaterializationStore` in place. Do not add migration
  entries.
- Remove `key_size_estimates`, its index, and the KV key `__fokosdb/key_sizes/sql_schema_version`.
  `ItemStats` tracks its migrations under `__fokosdb/item_stats/sql_schema_version`.
- A deployment must use new namespaces. Existing Durable Objects are destroyed, not migrated, so the change needs no
  backfill and no rollback procedure.
- Key encoding, DO names, and committed item contents stay the same.
- After the release, a coefficient change is an edit of the constants. It must not change item validity or commit
  eligibility.
- Update `AGENTS.md` for the range floor, the promotion lookup, and the bucket rules.

#### 4.2.15 Verification

Tests must check definitions, not only the agreement between two uses of the same value.

**Exact facts**

- Check multibyte text, binary data, JSONB, binary key tags, and an absent sort key.
- Check inserts, overwrites, compiled updates, deletes, and absent deletes, and the "created" signal of the capture.
- Check null-to-TTL, TTL-to-null, and non-null TTL changes, TTL expiry, and the subtraction from the bucket.
- Check migration retries, page grouping, restart, and source copy cleanup.
- Check that prepare and cancel add no facts.
- After `beforeComplete` of a promotion, check that a delete of a copy creates no bucket and changes no other bucket.
- Write a wrong bucket directly with SQL in the test. Then commit a delete in that bucket. Check that the commit
  succeeds, the defect log appears one time, and the facts equal the SQL sums again.
- Every other test treats the defect log as a failure.

**Buckets (property-based, `fast-check`)**

- Run random sequences of inserts, overwrites, deletes, TTL expiries, and import pages. After each operation, check:
  - The buckets cover the slice with no gap and no overlap.
  - Each bucket equals the SQL sums of its range.
  - The band rules of requirement 10 hold, and a hash leaf has one bucket for each key.
- Check an append-only sort-key sequence and a "delete most items" sequence.
- A bulk TTL expiry that empties a bucket between two small neighbours must merge the neighbours before the
  transaction commits.
- After a decrease of `K`, check deletes that leave ten buckets of 1,024 items at `K = 2,048`, from `K = 4,096`.
  Check the new-`K` bucket-row bound and the old-`2K` occupancy bound for buckets that receive no inserts.

**Storage model and consumers**

- Check that item estimates sum to the bucket estimate.
- Change the coefficients in a test build, and check that item validity, query evaluated bytes, and the facts stay the
  same, and that a prepared transaction still commits.
- Check the item limit below, at, and above `MAX_ITEM_BYTES`.
- Check that response estimates stay separate from evaluated-byte charges.

**Promotion**

- Check a key with many small items and long keys that ranks below a key with large items by `item_bytes`, but above
  it by estimate. The lookup must return it.
- Check that the write-path estimate equals the lookup estimate for a key with long sort keys and TTLs.
- Lower the cap below the size of a leaf with many keys. Compare each pass with the current top-five SQL result.
  With unchanged facts and pricing inputs, repeated decisions must reuse the cache, including a valid empty result.
- While one promotion is unfinished, check that the decision still returns the other eligible top-five keys.
- Seed six keys with estimates `[100, 90, 80, 70, 60, 50]` KiB at `T = 10 KiB`, then build the cache. Reduce the
  first five to 11 KiB through deletes or TTL expiry before cutover. The next decision must include the sixth key first.
- Check invalidation after each statistics mutation of section 4.2.8, and after each pricing-input change and eviction.
  A rolled-back write can invalidate the cache, but a later pass must use committed facts only.
- Make an uncached key cross `T` and enter the top five through a committed write. Fail its promotion request before
  the queue write. A later refused write must discover the key again and retry while another promotion is unfinished.
- With more than five eligible keys, check that the pass and the cache each retain at most five candidates.
  Completion must invalidate the cache so that the next pass discovers the remaining eligible keys.

**Planner and decision**

- Four equal weights and `N = 2` give two items for each child.
- `[111, 5111, 111]` and `N = 3` give three non-empty children. `[111, 111, 5111]` and `N = 3` use the repair and give
  `[111]`, `[111]`, `[5111]`. Check dominant items at the start, the middle, and the end.
- Check exactly `N` items, fewer than `N`, an empty slice, variable sort-key lengths, TTL costs, variable data sizes,
  explicit slice bounds, shortest separators, string and binary keys, and children that cover the slice.
- Check that the bucket path and the streaming path give the same boundaries on the same items, also when adjacent
  items above `B / N` force a collision and the end repair.
- Check that the scans of the bucket path stay inside the bound of appendix E when collisions occur.
- Check that wrong facts change the balance and not the validity.
- A range leaf over its cap with `C >= N` answers a split, and with `C < N` returns `fewer_items`. A refused write on
  a range leaf does not call the planner.
- Replace the tests that expect `null` or the `skewed_bytes` floor for the dominant-item case.

**Query plans and flows**

- Use `EXPLAIN QUERY PLAN` to assert covering scans for the size scans, the count checks, the split statements, the
  recount, and the old-facts read. Assert that the promotion lookup uses `item_stats_promotion`.
- Check range split, migration, promotion, and transaction flows in the Workers runtime with the existing partition
  harness. Add no production test hook.

### 4.3 Open questions

#### 4.3.1 Costs on a deployed Durable Object

All measurements of appendix A come from local workerd. The split statements, the bucket seek, and the refine scan
must be measured again on a deployed Durable Object before the release. If the split costs much more than 1 ms, the
answer can change `K`.

#### 4.3.2 Tombstones of `latest_items`

A tombstone also has an entry in `idx_latest_items_deleted_gc`. The `latest_items` model does not include that entry.
TODO: measure the cost of a tombstone row, and decide if the model needs a term for it.

#### 4.3.3 First-pass cost after a cap reduction

This question concerns the promotion-candidate cache of section 4.2.8, not the range-planner buckets or their `K`.
The cache lives in the memory of one hash-leaf Durable Object. It holds at most five candidate keys and their
estimates. It holds no item data and writes no SQLite or KV row.

The lookup must find those candidates before it can cache them. Its index orders keys by `item_bytes`, but promotion
orders them by the storage estimate. These orders can differ, so the lookup cannot always stop after five entries.

When an operator lowers the cap sharply:

1. The leaf keeps the same data, but its promotion threshold `T` falls.
2. More keys pass the index cutoff `T / r`.
3. The lookup can read every first bucket before it can select the top five.

For example, a leaf with 100,000 hash keys can require a pass over all 100,000 summaries. This is one summary per hash
key, not one read per item. The pass runs synchronously and blocks the Durable Object while it runs.

Repeated refused writes change no facts, so they can reuse the cached result. Statistics writes invalidate it;
a delete, TTL expiry, or promotion completion can require another pass. Eviction also loses the cache.
The cache bounds retained memory and avoids repeated work with unchanged facts. It does not bound one pass's CPU time.

TODO: measure this pass at the largest supported key count. The open question is whether it fits the CPU and latency
budgets after a large cap reduction. Decide later whether to accept that latency exception or divide the lookup across
invocations if it exceeds the CPU budget.

## 5. Alternative options

### 5.1 A larger fixed constant

It reduces part of the error. It cannot represent variable key copies, it keeps item validity tied to storage, and old
rows keep their old price.

### 5.2 A priced weight in each row

A scan then reads a ready weight. A coefficient change then needs a rewrite of each item, or leaves mixed model
versions. Exact facts avoid this.

### 5.3 A stored estimate in each bucket with a plain index

The promotion lookup then reads at most 5 rows. Each write path must calculate the estimate in its upsert, and a
coefficient change must rewrite each bucket row. A stale estimate can hide a candidate until the rewrite ends. The index
on `item_bytes` of section 4.2.8 has no derived state, and its walk has a bound.

### 5.4 An expression index on the estimate

The query must repeat the index expression exactly. A difference removes the index from the plan with no error, and
a coefficient change rebuilds the index. Rejected.

### 5.5 No index for the promotion lookup

The lookup then reads every key on each write while the leaf is over its cap. A scan of 1M index entries takes about
0.05 s. Rejected.

### 5.6 Promotion by the share of the leaf, or calibration with `sql.databaseSize`

Both compare the estimate of a key with the total of the leaf. A maintained total costs one more row write on each
write, and a summed total costs a scan of each key. Copies that wait for cleanup also change the total. The measured
model is within 5% for most rows, so the absolute threshold is sufficient.

### 5.7 A byte target for the bucket size

A byte rule, for example `S = cap / 256`, does not bound the work. A 16 MiB bucket holds about 115,000 small items or
40 large items, and the scan cost depends on the item count (appendix D). Rejected.

### 5.8 A background job for bucket splits

A job keeps the band only when it keeps up with the writes. Until it runs, a bucket can grow past `2K`, and the planner
bound fails. The job also needs a signal, an alarm, and a time budget. The inline split costs less than 1 ms once for
each `K` inserts. Rejected.

### 5.9 A cheaper split with one aggregate

The split can count only the lower half and calculate the upper half as the total minus the lower half. It then reads
about `K` entries in place of `3K`. It loses the drift repair of each split. Not chosen.

### 5.10 The scan in chunks over some alarm steps

It bounds each step, but not the total work, and it needs resumable plan state in the repartition flow. The buckets
bound the total work. Not chosen.

### 5.11 An upper limit on `rangeSplitConditions.maxSizeMb`

A limit of about 2 GiB keeps the streaming scan of M1 below about 13 s at `N = 4`. With buckets, the planner cost does
not grow with the cap, so no limit is necessary. Not chosen.

### 5.12 Skip the refine scan for a large `N`

Without the refine scan, a boundary can move by one bucket. The error of a boundary is then at most the estimate of
that bucket: at most `N * largest_bucket_estimate / B` of a child. Only with items of equal weight is that
`2K * N / C` — 1.5% for `N = 255` and 70M items. A bucket of `2K` near-maximum items next to buckets of small items
can hold several child targets at once. This would remove most of the 1.4 s worst case at `N = 255`. Not chosen,
because the default `N` is 4, and one rule for every `N` is simpler.

### 5.13 A generated item-size column

A generated column removes some write expressions. SQLite then does not use the index as a covering index. Rejected.

### 5.14 RPC estimates as storage weights

RPC estimators measure a different representation: public JSON reads use decoded text, and stored JSON uses JSONB.
They also do not represent SQLite indexes. Rejected.

### 5.15 Whole-slice materialization or an order-statistics structure

Materialization breaks the streaming memory rule. An order-statistics structure adds work to each mutation. Buckets
give bounded planner work with one row update for each write. Rejected.

## 6. Frequently asked questions

### Does the estimate equal `sql.databaseSize`?

No. It estimates the storage of committed items from exact facts. Pages, shared partition state, `item_stats`, and
transaction storage stay outside that equality. The physical size continues to control the cap.

### Does a coefficient change need a schema migration?

No. Rows and buckets store exact facts only. The new coefficient applies to each estimate at once. A new fact for a new
table can need a migration and a bounded backfill.

### How does a future table add its storage to the model?

The feature adds the facts that its storage shape needs: a count for a variable number of rows, a byte sum for variable
data, or a sum over the items that hold a conditional row. It updates them with its rows in the same transaction, and
its migration and delete paths move or remove them with the item. A new fact can need a schema migration and a bounded
backfill. A change of a coefficient never needs a backfill.

### Why are the buckets bounded by item count and not by bytes?

Each cost of a bucket grows with its item count: the split, the refine scan, the recount, and the number of bucket rows.
The bytes of a bucket affect only the precision of a boundary, and the refine scan makes each boundary exact to one
item. Appendix D has the details.

### Why does the write split the bucket, and not a job?

The rule "at most `2K` items" then holds after each commit, and the planner bound needs that rule. The split costs less
than 1 ms once for each `K` inserts into a bucket. A job needs a signal and an alarm, and it can fall behind.

### Can `K` change after the release?

Yes. Correctness does not depend on `K`: each bucket layout gives valid boundaries. For one change from a layout in
the old band, use two separate bounds until the layout reaches the new band:

- Bucket rows: at most `2C / min(old K, new K) + 1`.
- Bucket occupancy: at most `2 * max(old K, new K)` items.

- **A larger `K`.** Each existing bucket is already below the new `2K`, but two neighbours can hold fewer than the
  new `K` together. The bucket-row bound then stays at `2C / K + 1` rows of the old `K` until deletes merge the
  small pairs, while a bucket that keeps growing already reaches the new `2K` items. A mixed layout can thus cost
  the old bucket-row bound and the new refine-scan bound at the same time. A one-time job can merge the pairs
  earlier. A merge reads bucket rows only, no item.
- **A smaller `K`.** The old pair rule holds at deployment, but later deletes can leave pairs below the old `K`.
  The new merge rule permits those pairs, so the bucket-row bound uses the new `K`. An unchanged bucket can still
  hold up to the old `2K` items. Its next insert splits it in the write. Until then, its refine scan can read the
  old `2K` rows. A mixed layout can cost the new bucket-row bound and the old refine-scan bound together.
  A one-time job is necessary only for oversized buckets that get no inserts.

Each Durable Object uses the `K` of its code version, and each child builds its buckets during its import. Thus a
rollout needs no coordination between Durable Objects.

### Does a hash leaf ever have more than one bucket for a key?

No. The promotion lookup and the write-path check read one row for each key. A large key gets promoted, and the range
root builds its buckets during its import.

### What happens with sort keys that only increase?

Each insert goes to the last bucket. It splits after about `K` inserts, so the cost is about 3 index entries read for
each insert. The planner is not affected.

### Can a split stay unequal, or a child stay over its cap?

Yes. An item cannot cross a boundary, so a dominant item makes equal children impossible. With at least `N` items, the
planner still gives `N` non-empty children. A child that is still over its cap splits again.

### Why does the split decision not calculate the boundaries?

The decision needs one fact: is a split possible. With the planner of section 4.2.9, the item count gives that fact.
The decision runs before a refused write gets its error, so a scan there blocks the partition for each write.

### Why use JSONB bytes for the exact item size?

It keeps the existing measurement of the stored representation, and it needs no separate JSON text measurement. The
rule stays separate from the RPC response estimate and from exact DynamoDB compatibility.

## 7. References

Code paths are relative to `packages/fokosdb/src`:

- `shared/partition/item-size.ts` — the stored-size expression and the data expression.
- `shared/partition/key-size-stats.ts` — the current per-key summary, which `ItemStats` replaces.
- `shared/partition/split-planner.ts` — `SplitPlanner` and `planRangeBoundaries`.
- `shared/partition/partition-store.ts` — the schema, the mutations, the query scans, and `sortKeySizes`.
- `shared/partition/fokos-migration-host.ts` — the per-page migration accounting.
- `shared/partition/transaction-participant.ts` — the prepare checks and the committed mutations.
- `shared/partition/materialization-export.ts` — the export byte budget.
- `shared/expression/plan.ts` and `shared/expression/runtime.ts` — the compiled update statement and its size probe.
- `shared/query/query-collector.ts` — the evaluated-byte and response-byte charges.
- `server/do-partition.ts` — admission, `signalGrowth`, the split hooks, and `beforeComplete`.
- `server/materialization/materialization-store.ts` and `server/materialization/target-runner.ts` — `latest_items`
  and its batch budget.
- `sharding/runtime.ts` — `#admit`, `requestSplitEvaluation`, and `#evaluateSplit`.
- `sharding/repartition-flow.ts` — `#plan` and its backoff.

Repository documents:

- `AGENTS.md` — invariants, test harnesses, and verification commands.
- `docs/agent-plans/range-split-boundary-byte-seek.md` — the streaming boundary scan that section 4.2.9 replaces.
- `docs/agent-plans/2026-09-27-over-size-split-trigger.md` — the physical cap, promotion decisions, and range floors.
- `docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md` — `latest_items` and the GSI physical sort key.
- `docs/research/claude-hashing_and_splits_across_distributed_databases.md` — split triggers and split points in
  other systems.

Platform references:

- [SQLite file format: SQL indexes](https://www.sqlite.org/fileformat2.html#representation_of_sql_indices)
- [Durable Object SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Durable Object limits](https://developers.cloudflare.com/durable-objects/platform/limits/) — 10 GB of storage for
  each Durable Object, and 30 s of CPU for each invocation by default, up to 5 minutes with `limits.cpu_ms`.

## Appendix A. Measurements

### A.1 `items` under the proposed schema

Date: 2026-10-30. Each case inserts 20,000 rows of one 8-byte hash key, in sort-key order, into a new Durable Object.
The value is the increase of `sql.databaseSize` divided by the row count. The runtime is local workerd under
`@cloudflare/vitest-plugin`, and the page size is 4,096 bytes. The schema is the one of section 4.2.2. `key` is the key
bytes. The model column uses the `items` coefficients of section 4.2.3.

| key | data | Measured, no TTL | Model, no TTL | Measured, TTL | Model, TTL |
| --- | --- | --- | --- | --- | --- |
| 16 | 0 | 110 | 106 | 153 | 146 |
| 16 | 100 | 214 | 206 | 258 | 246 |
| 16 | 1,000 | 1,430 | 1,106 | 1,470 | 1,146 |
| 58 | 0 | 250 | 253 | 339 | 346 |
| 58 | 100 | 355 | 353 | 450 | 446 |
| 58 | 1,000 | 1,526 | 1,253 | 1,613 | 1,346 |
| 108 | 0 | 425 | 428 | 576 | 583 |
| 108 | 100 | 535 | 528 | 681 | 683 |
| 108 | 1,000 | 1,646 | 1,428 | 1,793 | 1,583 |
| 208 | 0 | 769 | 778 | 1,026 | 1,058 |
| 208 | 100 | 886 | 878 | 1,143 | 1,158 |
| 208 | 1,000 | 1,881 | 1,778 | 2,138 | 2,058 |
| 408 | 0 | 1,480 | 1,478 | 1,992 | 2,008 |
| 408 | 100 | 1,610 | 1,578 | 2,122 | 2,108 |
| 408 | 1,000 | 3,076 | 2,478 | 3,588 | 3,008 |

- Least-squares fits of `measured - item_bytes` against `key`, with no TTL: `2.50 * key + 48` for no data,
  `2.57 * key + 51` for 100 bytes of data, and `3.20 * key + 242` for 1,000 bytes of data.
- The TTL adds `1.19 * key + 17` to `1.20 * key + 21`.
- Random insert order changes the result by at most 1.7% (353 in place of 355, and 1,162 in place of 1,143).
- Rows of 1 KB to 1.5 KB fill a 4,096-byte page badly, so the model is too low for them.

The measurement of the current schema (2026-10-04) gave about the same file growth. The current formula `item + 108`
gives 616 bytes for `key = 408, data = 100`, where the file grows by 1,610 bytes.

### A.2 `latest_items` with the covering index

The same method, with the schema of section 4.2.2 and no TTL. The model uses the `latest_items` coefficients.

| key | data | Measured | Model |
| --- | --- | --- | --- |
| 16 | 0 / 100 / 1,000 | 96 / 202 / 1,430 | 96 / 196 / 1,096 |
| 58 | 0 / 100 / 1,000 | 234 / 345 / 1,526 | 243 / 343 / 1,243 |
| 108 | 0 / 100 / 1,000 | 411 / 520 / 1,646 | 418 / 518 / 1,418 |
| 208 | 0 / 100 / 1,000 | 754 / 855 / 1,881 | 768 / 868 / 1,768 |
| 408 | 0 / 100 / 1,000 | 1,480 / 1,610 / 3,076 | 1,468 / 1,568 / 2,468 |

A `ttl_at` value adds 4 bytes (524 in place of 520 for `key = 108, data = 100`).

### A.3 `item_stats` rows

The same method, with one first bucket for each of 20,000 hash keys.

| Hash key | `item_stats` row | `key_size_estimates` row today |
| --- | --- | --- |
| 8 bytes | 50 | 39 |
| 50 bytes | 145 | 134 |
| 100 bytes | 264 | 248 |

### A.4 Statement costs

Date: 2026-10-30. Local workerd, AMD Ryzen 9 7940HS. One range-leaf slice of 300,000 items: an 8-byte hash key, a
16-byte or a 100-byte sort key, 20 bytes of data, and a TTL on one item in three. Each value is the wall time of a
`runInDurableObject` call minus an empty call (about 2 ms), the median of 3 calls, divided by the repetitions in the
call. The clock of a Worker does not advance during synchronous code, so the test side measures the time.

| Operation | sk 16 bytes | sk 100 bytes |
| --- | --- | --- |
| JavaScript iteration of covering-index rows, with the weight | 1.17 µs / row | 1.30 µs / row |
| SQL `COUNT(*)` over the covering index | 0.043 µs / entry | 0.080 µs / entry |
| Split, `K = 2,048` (6,147 rows read) | 0.70 ms | 0.86 ms |
| Split, `K = 4,096` (12,291 rows read) | 1.32 ms | 1.90 ms |
| Split, `K = 8,192` (24,579 rows read) | 2.52 ms | 3.66 ms |
| Split, `K = 16,384` (49,155 rows read) | 5.06 ms | 7.34 ms |
| Refine (`2K` rows), `K = 2,048 / 4,096 / 8,192 / 16,384` | 5.0 / 9.5 / 19.0 / 36.8 ms | 5.3 / 10.5 / 21.1 / 42.0 ms |
| Bucket update of the first bucket (2 rows read, 2 written) | 5.30 µs | 5.35 µs |
| Bucket seek update, 1,000 buckets (3 rows read, 1 written) | 8.70 µs | 8.95 µs |

- The split is the `OFFSET` seek, the two aggregates, and the two bucket writes of section 4.2.6.
- The promotion lookup over 100,000 first buckets read 285 entries in 0.585 ms.
- `EXPLAIN QUERY PLAN` shows a covering-index search for the seek and for the aggregates, a primary-key search for the
  bucket seek update, and a search of the partial promotion index for the lookup.

Measurement of the current `planRangeSplit` (2026-10-04, same machine, 8-byte hash key, 16-byte sort key, 20 bytes of
data, 146 MB of database for 1M rows):

| Rows | N | Planner time | Full JavaScript iteration | Full `COUNT(*)` in SQL |
| --- | --- | --- | --- | --- |
| 1M | 2 | 0.49 s | 0.89 s | 0.05 s |
| 1M | 4 | 0.73 s | | |
| 1M | 16 | 0.92 s | | |
| 2M | 2 | 0.96 s | 1.78 s | 0.07 s |
| 2M | 4 | 1.43 s | | |
| 2M | 16 | 1.80 s | | |

## Appendix B. Walkthrough: the two planner defects

**Crossing item in the lower prefix.** The current planner adds the bytes of an item before it makes a boundary in
front of that item. The total then holds bytes that the lower child does not get.

```text
Weights:        [111, 111, 111, 111], N = 2, target 222
Current result: [111] [333]   (the total reaches 222 at the second item, and the boundary goes before it)
Correct result: [222] [222]
```

**False `skewed_bytes` floor.** The current planner sets the next threshold to `acc + step`. A dominant item then
pushes the last threshold past the total.

```text
Weights: [111, 5111, 111], N = 3, B = 5333, targets 1777 and 3555
Current: one boundary, then no threshold is reached -> skewed_bytes, and the leaf refuses writes
Correct: gaps at 111 and 5222. The nearest gap to 1777 is 111 (1666 away; 5222 is 3445 away).
         The only gap left for 3555 is 5222. Children: [111] [5111] [111]
```

**Repair at the end.** Weights `[111, 111, 5111]` and `N = 3` have the targets 1,777 and 3,555, and the gaps 111 and
222. Both targets select the gap 222, so the forward pass gives one boundary. The repair takes the gaps of the buffer,
latest first, and gives `[111]`, `[111]`, `[5111]`.

## Appendix C. Walkthrough: the stop rule of the promotion lookup

The index orders keys by `item_bytes`, and the decision needs the order by estimate. The two orders can differ:

```text
Key X: 100 items of 1 MB         -> item_bytes 100 MB, estimate about 100 MB
Key Y: 500,000 items, 100-byte keys and 20 bytes of data
                                 -> item_bytes  60 MB, estimate 60 + 2.5 * 50 + 50 * 0.5 = 210 MB
```

X comes first in the index, and Y is larger. Thus the walk cannot stop after 5 rows.

**Bound on the ratio.** For any key, `key_bytes <= item_bytes`, `ttl_key_bytes <= key_bytes`,
`ttl_item_count <= item_count`, and `item_count <= item_bytes / 1`, because each item has at least one byte. So:

```text
estimate <= item_bytes * (1 + key_index_factor + ttl_key_factor + base_overhead + ttl_overhead) = r * item_bytes
```

A key with `estimate >= T` therefore has `item_bytes >= T / r`. The walk that reads each key with
`item_bytes >= T / r` finds every candidate.

**Bound on the rows.** The number of keys with `item_bytes >= T / r` is at most `sum(item_bytes) * r / T`. For
`items`, `r = 74.75`. In a hash leaf at 1.1 times its cap with `T = 0.25 * cap`, that is
`1.1 * 74.75 / 0.25`, about 330 keys. Most leaves have few keys above `T / r`, which is about 1.7 MB at the default
500 MB cap, so a walk usually reads a few rows.

## Appendix D. Walkthrough: why the bucket size counts items

| What a bucket affects | Grows with | Reason |
| --- | --- | --- |
| The split | items | It reads index entries. The covering index does not read item data |
| The refine scan | items | JavaScript reads each row, at about 1.2 µs for each row |
| The recount after a defect | items | The same scan as the split |
| The number of bucket rows | items | It is the item count of the slice divided by the bucket size |
| The precision of a boundary | bytes, only without a refine scan | The refine scan selects the exact gap |

A byte target does not bound these costs. A 16 MiB bucket holds about 115,000 items of 146 bytes, a refine scan of
about 140 ms, or 40 items of 400 KB, a scan of less than 1 ms. A long key makes each index entry larger, but it changes
the cost of a row by less than 2 times (1.17 µs and 1.30 µs in appendix A.4).

With a count rule, `K` does not depend on the cap or on the policy. A change of `maxSizeMb` or `rangeSplitN` changes no
bucket.

## Appendix E. Walkthrough: the cost bound of the planner

### E.1 Bucket rows: at most `2C / K + 1`

The merge rule says that each two neighbour buckets hold at least `K` items together. Group the buckets in pairs
`(b1, b2)`, `(b3, b4)`, and so on. The pairs do not overlap, and each pair holds at least `K` items:

```text
(buckets / 2) * K <= C   ->   buckets <= 2C / K   (+1 when the count is odd)
```

The split rule gives the lower limit: each bucket holds at most `2K` items, so `buckets >= C / (2K)`.

### E.2 Scanned rows: at most `min(N - 1, buckets) * 2K + 2N`

```text
buckets:   [ b1 ][ b2 ][ b3 ][ b4 ][ b5 ][ b6 ][ b7 ][ b8 ]
bytes:      ----------------------------------------------- B
targets:            ^ B/4         ^ 2B/4          ^ 3B/4
                    in b2         in b4           in b7     -> scan b2, b4, and b7 only
```

- `N - 1` targets exist, and each target falls in one bucket.
- Two or more targets can fall in one bucket. The planner scans that bucket one time.
- Thus the planner scans at most `N - 1` buckets, and never more than all buckets.
- Each scanned bucket holds at most `2K` items, and the scanned buckets do not overlap.
- A collision step reads one item for each boundary it makes: at most `N - 1` rows.
- The end repair reads the last `N` items one time: at most `N` rows.

Example, `K = 2,048`, `C = 1,000,000`, `N = 4`: at most 977 bucket rows, and at most 3 * 4,096 + 8 = 12,296 scanned
rows. That is about 17 ms. The streaming scan reads about 750,000 rows, about 0.9 s.

Example, `K = 2,048`, `C = 300,000`, `N = 255`: at most 293 buckets, and 254 * 4,096 + 510 is more than `C`. Thus the
scan reads at most all 300,000 rows plus at most `2N` repair rows. The slice is small, so the planner reads it all,
in about 0.36 s.

### E.3 The planner at each cap

Small items of about 150 bytes, 1.2 µs for each row read, and 5.3 ms for each refine scan at `K = 2,048`.

| Cap | Items | Streaming scan, `N = 4` | Streaming scan, `N = 255` | Buckets, `N = 4` | Buckets, `N = 255` |
| --- | --- | --- | --- | --- | --- |
| 500 MiB | 3.5M | 2.6M rows, 3.1 s | 3.5M rows, 4.2 s | 3,400 + 12,300 rows, 20 ms | 1.04M rows, 1.35 s |
| 2 GiB | 14.3M | 10.7M rows, 12.9 s | 14.3M rows, 17.2 s | 14,000 + 12,300 rows, 33 ms | 1.05M rows, 1.37 s |
| 5 GiB | 35.8M | 26.8M rows, 32.2 s | 35.8M rows, 43.0 s | 35,000 + 12,300 rows, 58 ms | 1.08M rows, 1.39 s |
| 8 GiB | 57.3M | 43.0M rows, 51.6 s | 57.3M rows, 68.7 s | 56,000 + 12,300 rows, 83 ms | 1.10M rows, 1.42 s |

The streaming scan passes the 30 s CPU limit at about 4.7 GiB at `N = 4`. With buckets, the planner cost does not grow
with the cap. The collision repair of section 4.2.9 adds at most `2N` rows, below the precision of the table.

## Appendix F. Walkthrough: the choice of K

The worst case is a leaf at the 10 GB limit with about 70M small items. The refine scans use the measured costs of
appendix A.4 for a 100-byte sort key.

| K | Split | Rows / split | Refine | Buckets at 10 GB | Planner, `N = 4` | Planner, `N = 255` | Splits / 1M inserts |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **2,048** | 0.70–0.86 ms | 6,147 | 5.3 ms | ≤ 68,000 (82 ms) | about 98 ms | about 1.4 s | 488 |
| 4,096 | 1.32–1.90 ms | 12,291 | 10.5 ms | ≤ 34,000 (41 ms) | about 73 ms | about 2.7 s | 244 |
| 8,192 | 2.52–3.66 ms | 24,579 | 21.1 ms | ≤ 17,000 (21 ms) | about 84 ms | about 5.4 s | 122 |
| 16,384 | 5.06–7.34 ms | 49,155 | 42.0 ms | ≤ 8,500 (10 ms) | about 136 ms | about 10.7 s | 61 |

The target range of leaves is 500 MB to 2 GB (section 2.1). The two closest values at each cap:

| Cap | `K = 2,048`, `N = 4` | `K = 4,096`, `N = 4` | `K = 2,048`, `N = 255` | `K = 4,096`, `N = 255` |
| --- | --- | --- | --- | --- |
| 500 MiB | **about 20 ms** | about 34 ms | **about 1.35 s** | about 2.7 s |
| 2 GiB | **about 33 ms** | about 40 ms | **about 1.37 s** | about 2.7 s |
| 5 GiB | about 58 ms | **about 52 ms** | **about 1.39 s** | about 2.7 s |
| 10 GB | about 98 ms | **about 73 ms** | **about 1.43 s** | about 2.7 s |

- The planner reads all bucket rows and up to `2K` rows for each target. Below about 3.4 GiB, the shorter refine scans
  of `K = 2,048` save more than its extra bucket rows cost. Above it, `K = 4,096` is faster at `N = 4` by at most
  about 25 ms.
- The total split work is about 3 index entries read for each insert, for each `K`. A smaller `K` gives more, shorter
  splits.
- At `K = 2,048`, each split blocks the partition for less than 1 ms, and the planner stays below 0.1 s at `N = 4` at
  each cap. The bucket table of a 2 GiB leaf holds at most about 14,000 rows (1 MB to 2 MB), and the bucket table of a
  10 GB leaf at most about 68,000 rows (4.5 MB to 10 MB), for sort keys of 16 to 100 bytes.
- `K = 1,024` is not measured. A linear extrapolation gives about 42 ms at 2 GiB and about 170 ms at 10 GB at `N = 4`,
  both worse than `K = 2,048`.
- Larger items make every value in this appendix smaller, because a leaf then holds fewer items (section 4.2.13).
