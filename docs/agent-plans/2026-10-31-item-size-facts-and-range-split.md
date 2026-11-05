# RFC — Exact item-size facts, item statistics in buckets, and correct range splits

**State:** Draft
**Date:** 2026-10-31, updated 2026-11-05

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
- [Appendix C. Walkthrough: the promotion lookup][appendix-c]
- [Appendix D. Walkthrough: why the bucket size counts items](#appendix-d-walkthrough-why-the-bucket-size-counts-items)
- [Appendix E. Walkthrough: the cost bound of the planner](#appendix-e-walkthrough-the-cost-bound-of-the-planner)
- [Appendix F. Walkthrough: the choice of K](#appendix-f-walkthrough-the-choice-of-k)
- [Appendix G. Walkthrough: buckets in storage order](#appendix-g-walkthrough-buckets-in-storage-order)

[appendix-c]: #appendix-c-walkthrough-the-promotion-lookup

## 1. Overview and context

### 1.1 Glossary

The terms of the current system that this document uses. Section 4 defines the terms of the solution where it first
uses them.

| Term | Meaning in this document |
| --- | --- |
| item | One committed row of `items` (partition DO) or `latest_items` (materialization DO) |
| composite key | The pair `(hk, sk)` of an item, compared by `hk` first and then by `sk` |
| routingHash | The 8-byte hash of the hash key (`docs/agent-plans/2026-11-01-routing-hash.md`). Two hash keys can have one routingHash |
| storage order | The primary-key order `(routing_hash, hk, sk)` of `items`, and `(routing_hash, base_hk, base_sk)` of `latest_items` |
| hash leaf | A partition that owns an interval of routingHashes |
| range leaf | A partition that owns one composite-key slice `[start, end)` of one promoted routingHash |
| cap | The `maxSizeMb` of the split policy of a leaf. A leaf above its cap splits or promotes |
| promotion | The move of one large routingHash from a hash leaf to its own range tree. The threshold is a fraction of the cap |
| hash split, range split | The split of a leaf into children. A range split makes `rangeSplitN` children, 4 by default |

### 1.2 Current size accounting

**Storage.** `items` is a `WITHOUT ROWID` table with `PRIMARY KEY (routing_hash, hk, sk)`. Its rows are in storage
order. It has three secondary indexes:

- `idx_items_item_id (item_id)`, unique. Each entry also holds the primary key.
- `idx_items_scan (routing_hash, hk, sk, est_row_bytes)`, for the covering size scans and count queries.
- `idx_items_ttl (ttl_epoch_utc_seconds, routing_hash, hk, sk)`, partial, for the items with a TTL.

Thus an insert writes 3 rows, or 4 with a TTL. Each item stores its routingHash and its key 3 times, or 4 times with a
TTL.

**Formula.** `shared/partition/item-size.ts` defines one formula, and `items.est_row_bytes` stores its result:

```text
est_row_bytes = octet_length(stored data) + octet_length(hk) + octet_length(sk) + 150
```

The constant 150 (`EST_ROW_BYTES_K`) covers the routingHash, the integer columns, the record header, and the entries
of `idx_items_scan` and `idx_items_item_id`.

Five consumers use this one value:

| Consumer | Code |
| --- | --- |
| Item limit: put, update, prepare | `PartitionStore.upsertItem`, `expression/plan.ts`, `measureItemBytes` |
| The evaluated-byte budget of `queryItems` | `query/query-collector.ts` |
| The byte budget of an export call | `partition/materialization-export.ts` |
| The total of each routingHash for promotion | `KeySizeStats`, `signalGrowth`, `largestAtLeast` |
| The weights of range boundaries | `split-planner.ts` `planRangeBoundaries`, `PartitionStore.compositeKeySizes` |

`latest_items` of the materialization DO is also `WITHOUT ROWID`, on `(routing_hash, base_hk, base_sk)`, with
`UNIQUE (item_id)`. Its formula is `data + 2 · (routingHash + key bytes) + 70` for a live row. For a tombstone it is
`3 · (routingHash + key bytes) + 70`, because the tombstone index holds a third copy. It uses the same `SplitPlanner`.

`key_size_estimates (routing_hash PRIMARY KEY, hk, est_bytes)` holds one total for each routingHash, with an index on
`est_bytes`. `hk` is a representative hash key of the routingHash, for the hash-key promotion API of the host.

### 1.3 Problems

1. **One value has five meanings.** A change of the constant 150 changes which items are valid. An item that
   prepare accepted can then fail at commit, and a commit must not fail. Query pages and export batches also change.
2. **The estimate is too low for long keys.** SQLite stores each key in the table row, in `idx_items_item_id`, in
   `idx_items_scan`, and in `idx_items_ttl` when the item has a TTL. The formula counts the key one time.
   On the earlier rowid layout, which also stored 3 key copies, a 408-byte key with 100 bytes of data gave 616 bytes,
   and the file grew by 1,610 bytes (appendix A.2). GSI rows have long physical sort keys and little data, so they
   get this error the most.
3. **Each row stores the constant.** A change of the constant leaves old rows at the old value.
4. **`latest_items` has a different formula** and a different constant.
5. **The planner has two defects** (appendix B). It counts the bytes of the crossing item in the lower child.
   It also reports a false `skewed_bytes` floor, for example for item sizes `[111, 5111, 111]` and 3 children.
   At that floor, the leaf refuses each write until an operator acts, although a valid split exists.
6. **The split decision scans the slice on the request path.** `signalGrowth` signals `evaluateSplit` after each
   applied write, and admission requests it for each refused write. On a range leaf above its cap,
   `SplitPlanner.evaluateSplit` then scans the slice with `planRangeBoundaries` for each write, until the split row
   exists. At the false floor, each write scans again. `#plan` then scans again.
7. **The planner scan has no bound.** It costs about 1.2 µs for each row (appendix A.4). A leaf near the 10 GB limit
   of a Durable Object holds about 70M small rows. The scan then takes more than the 30 s CPU limit, and each try of
   the plan fails (appendix E.3).
8. **The summary hides drift.** `KeySizeStats` clamps with `MAX(0, …)`. It deletes the row when the bytes reach 0,
   because it has no item count. A transactional delete of an absent item with `bumpTxOrderTsAlways` calls
   `subtract` with 0 bytes, which writes the summary row with no change. The representative `hk` can outlive its own
   items.
9. **Promotion comes late.** The threshold is absolute, and the estimate is too low. A large routingHash then stays
   through more hash splits, and each split copies it again.
10. **Change capture depends on the size.** `#captureWrite(…, oldEst === 0)` reads "the old size is 0" as
    "the write created the row". The item-id assignment of `upsertItem` uses the same test.
11. **`idx_items_scan` repeats the table order.** The primary-key B-tree is already in storage order, and its record
    holds each column before `data`. The index adds one row written for each insert and one more key copy for each
    item. Its only gain is narrower entries for the size scans.

### 1.4 Why now

The first release is close. After the release, a drop or a change of `idx_items_scan` changes an index over each item
of each partition. A change of the primary key of the summary table rewrites each summary row. On a leaf of 9 GB, such
a change blocks every request of the partition. Before the release, the change edits the schema migrations in place,
and the deployment uses new namespaces.

## 2. Goals and requirements

### 2.1 In scope

- One exact size for each item, for the item limit and the byte budgets.
- A storage estimate for each group of items that follows the real storage: each key copy in each index, the index
  entry of an item with a TTL or of a tombstone, and the overflow pages of large rows.
- Promotion and range splits that use this estimate.
- The same behaviour in the partition DO and in the materialization DO.
- Costs optimized for range leaves of 500 MB to 2 GB.

### 2.2 Out of scope

- An exact page attribution for each item. A page holds a whole number of rows, and the model does not count the
  space that this wastes. Rows with 400 to 700 bytes of data are up to 18% too low for that reason (appendix A.1). A
  range split compares the sizes of one slice with each other, so an error that is the same for each item moves no
  boundary.
- The exact payload size of each row. The model estimates the overflow pages from a fixed payload estimate.
- The overflow pages of index entries. Only keys of more than about 990 bytes give such entries.
- The storage of the size summaries themselves. One summary row uses about 66 bytes. At a cap of 32 MB or more, that
  is less than 0.001% of a routingHash at the promotion threshold (appendix A.3).
- A storage model for `pending_transactions`, the outbox, or the coordinator.
- A generic cost plug-in framework, and placeholder tables for unspecified features.
- A public configuration of the storage model.
- An upper limit on `rangeSplitConditions.maxSizeMb`. The solution bounds the planner at each cap (appendix E).
- Changes to the routingHash, the clustered primary keys, `idx_items_item_id`, `idx_items_ttl`, routing names, key
  encoding, ownership rules, or the repartition protocol.
- The deletion of the item rows that a split source keeps.

### 2.3 Requirements

**Functional**

1. The validity of an item must depend only on its encoded size and the item limit of 400 KB. Prepare and commit must
   apply the same rule, so an item that prepare accepted cannot fail at commit.
2. The byte budgets of query pages and of export batches must count the encoded size of each item. The budgets of RPC
   responses keep their own estimates.
3. The size summaries must stay exact: after each commit, they must match the committed items. A write must not fail
   because of a summary defect. The partition must detect the defect, log it, and repair it in the same transaction.
4. The storage model must be able to change after the release, for new constants, columns, indexes, and tables. A
   change of a constant must apply at once to item validity, the byte budgets, and the range planner, with no rewrite
   of a stored row. The stored estimate of the promotion lookup can keep the old constant until the next write or
   import of its bucket (section 4.2.8). Only a new kind of fact can need a backfill.
5. The promotion decision must find each routingHash whose estimate is at or above the promotion threshold, up to
   five, largest first, each with a hash key that has items. The check after a write and the decision must agree.
6. A range split must succeed whenever the slice holds at least one item for each child. Each child must hold at least
   one item, and each boundary must be the shortest key that separates two adjacent items. The children must be close
   to equal in estimated size.
7. Migration retries and promotion cleanups must leave the size summaries exact.
8. The physical size of the database must continue to control the split trigger and the admission of writes.

**Non-functional**

9. **Rows written.** Each row written is billed. An item write must write fewer rows than today. An import or a TTL
   sweep must not write one summary row for each item.
10. **Request path.** The size accounting of one write must read and write a fixed number of rows. The split decision
    on the request path must read a number of rows that does not grow with the leaf size.
11. **Planner.** The range split planner must not read each item of the slice. The rows that it reads must be a small,
    bounded fraction of the item count, so that it stays far below the 30 s CPU limit at each cap, up to the 10 GB
    limit of a Durable Object.
12. **Large writes.** Work that changes many rows must run in bounded steps. One large write blocks each request of
    the partition until the write is durable.
13. **Tests.** No test can need a new production hook.

## 3. Milestones

The change ships as two milestones. Both milestones must ship before the first release. No delivery dates are set.

1. **M1 — Exact item sizes, the storage model, promotion, and the corrected planner.** Sections 4.2.1 to 4.2.5 and
   4.2.7 to 4.2.12. M1 drops `idx_items_scan`. Each leaf keeps one bucket for each routingHash. The planner scans the
   slice (the streaming path of section 4.2.9). The planner change and the split decision of section 4.2.10 ship
   together. M1 alone must not be released: its planner scan has no bound, and `maxSizeMb` has no upper limit.
2. **M2 — Buckets on range leaves.** Section 4.2.6, the import buckets, the bucket groups of the TTL sweep, and the
   bucket path of the planner. M2 changes no schema. No range leaf from M1 reaches a release, so M2 converts no data.
   A development namespace from M1 must be destroyed: the first split of its one large bucket reads the whole slice.

## 4. Proposed solution

### 4.1 High-level overview

**Exact item size.** Each item stores one exact value: `item_bytes`, the stored data plus the key bytes. The item
limit, the query budget, and the export budgets use only this value.

**Facts and buckets.** The facts of a group of items are exact sums: the item count, the item bytes, the key bytes, the
same two sums for the items with an index entry (a TTL or a tombstone), and the overflow bytes. A new table
`item_stats` holds the facts in buckets. A bucket is one row: the items of one routingHash from its lower bound up to
the lower bound of the next bucket, in composite-key order.

- On a hash leaf, each routingHash has one bucket. That bucket is the summary of the routingHash, and it includes
  each hash key that has that routingHash.
- On a range leaf, the one routingHash has many buckets of `K` to `2K` items, with `K = 2,048`. The write that adds an
  item splits a bucket above `2K` items in its own transaction, and a delete merges two small neighbours.

**Storage order.** `items` and `item_stats` are both in storage order. Thus:

- The items of a bucket are one key range of the primary key. A bucket split, a recount, and a planner scan read
  that range and no other row.
- The facts of a routingHash are one key range of `item_stats`. A promotion cleanup deletes one key range.
- The import receives its items in storage order, so it builds its buckets with no item read.
- The table already holds the size columns in key order, so `idx_items_scan` goes.

**Storage model.** The estimate of an item or a bucket is the storage bytes that the storage model calculates from its
facts. The model multiplies each fact by a coefficient, a constant in the code that is measured for each table. It
counts each key copy in each index, the index entry of a TTL or a tombstone, and the overflow pages of large rows. A
new coefficient changes every estimate that the planner calculates at once, with no row rewrite.

The facts feed two decisions:

- **Promotion.** Each bucket also stores `est_x4`, 4 times its estimate. Each statement that writes facts calculates
  it again from the new facts. A partial index on `est_x4` holds the first bucket of each routingHash. The lookup reads
  at most 5 rows of this index from the largest value. After a coefficient change, a bucket gets the new value at its
  next write or import.
- **Range split.** The planner reads the bucket rows of the slice in key order and calculates the byte target of each
  boundary. It then scans only the bucket that holds each target, and it selects the exact gap between two items.

The split decision of a range leaf counts at most `N` items, where `N` is `rangeSplitN`. Only the planner, in the
alarm, reads more.

```text
items.item_bytes (exact) ───────────────► item limit, query evaluated bytes, export budgets
        │ same transaction as the write
        ▼
item_stats buckets (exact facts), key (routing_hash, lower_hk, lower_sk)
        │
        ├── hash leaf: one bucket per routingHash ──► est_x4 ──► top 5 on item_stats_promotion ──► promotion
        │
        └── range leaf: K..2K items per bucket ──► storage model ──► planner:
                                                                     bucket rows + one scan per target bucket

sql.databaseSize ───────────────────────► split trigger and admission (unchanged)
```

#### 4.1.1 End-to-end flows

**A write on a hash leaf** (put, update, delete, or commit apply), in one storage transaction:

1. The old-facts read gets the old `item_bytes` and the indexed state of the item, or finds no row (section 4.2.2).
2. The item write calculates the new `item_bytes` in SQLite and returns it.
3. The bucket helper adds the new facts minus the old facts to the first bucket of the routingHash, and it sets
   `est_x4` from the new facts in the same statement (section 4.2.5). The statement returns the new `est_x4`.
4. After the commit, the write-path check compares the returned `est_x4` with the promotion threshold. It reports a
   promotion candidate when the estimate is at or above the threshold. The write then signals the split decision.

**A hash leaf over its cap.** The split decision runs after the write. It reads at most 5 rows of `item_stats_promotion`
from the largest `est_x4`, and it returns up to five routingHashes at or above the promotion threshold (section 4.2.8). The
runtime requests a promotion for each one. When no routingHash is at or above the threshold, the leaf queues a hash
split.

**A promotion.** The new range root imports the items of the routingHash in storage order, and it builds its buckets
with no item read (section 4.2.6). At `beforeComplete`, the source deletes the buckets of the routingHash. The source
cleanup then deletes the item copies in steps. Those deletes change no facts, because the routingHash has no bucket.

**A hash split.** Each hash child imports its routingHashes, and it adds the facts of each run of one routingHash to
the first bucket of that routingHash. The source becomes a router and does not read its `item_stats` again.

**A write on a range leaf.** The same steps as on a hash leaf, but the bucket helper finds the bucket of the
composite key with one seek. After an insert, the bucket splits when it holds more than `2K` items. After a delete, it
merges with a neighbour when the pair holds fewer than `K` items (section 4.2.6). A range leaf has no promotion.

**A range leaf over its cap.** The split decision counts at most `N` items of the slice (section 4.2.10). In the
alarm, the planner reads the bucket rows, scans the buckets that hold the targets, and returns `N - 1` boundaries
(section 4.2.9). Each range child imports its slice and builds its buckets. The source becomes a router.

**A TTL sweep.** It deletes a chunk of expired items, groups their facts by bucket, and writes each changed bucket
one time (section 4.2.5).

### 4.2 Technical details

#### 4.2.1 Size definitions

| Value | Definition | Consumers |
| --- | --- | --- |
| key bytes | The encoded bytes of the hash key plus the encoded bytes of the sort key | `item_bytes`, the key facts |
| `item_bytes` | `octet_length(stored data) + key bytes` | Item limit, query evaluated bytes, export budgets |
| estimate | The facts priced by the storage model of the table | Promotion, range boundaries |
| response estimate | `estimateItemBytes()` and `estimateProjectedRowBytes()` | Response and migration page budgets |

- `item_bytes` keeps the current measurements: UTF-8 bytes for text, BLOB bytes for binary data, JSONB bytes for JSON,
  and the one-byte tag of a binary key. JSONB bytes measure the stored representation, so no separate JSON text
  measurement is necessary.
- An absent sort key has zero bytes. The routingHash is not in `item_bytes`, because it is not part of the item.
- `MAX_ITEM_BYTES` stays 400 KB. The fixed overhead of 150 bytes leaves the item limit, so an item can hold 150 more
  bytes than today.
- The rule does not match the DynamoDB item size exactly.
- The response estimators must not supply row weights or facts.

#### 4.2.2 Item schema

**`items`** (the partition):

- Replace `est_row_bytes` with an ordinary `item_bytes INTEGER NOT NULL` column. Keep it before `data`, so `data`
  stays the last column. Each writer calculates it in SQLite, over the same expression that produces the stored data.
- Drop `idx_items_scan`. The primary-key B-tree is in storage order. Its record stores `routing_hash`, `hk`, and `sk`
  first, then the other columns in declared order. A read of `item_bytes` and `ttl_epoch_utc_seconds` stops before
  `data`, so it reads no overflow page.
- Keep `WITHOUT ROWID`, the primary key, `idx_items_item_id`, `idx_items_ttl`, and `item_id_counter`.
- Do not use a generated column for `item_bytes` (section 5.13).

**`latest_items`** (the materialization DO):

- Replace `est_row_bytes` with `item_bytes`, with the same definition, before `data`. A tombstone has no data, so its
  `item_bytes` is its key bytes.
- Add no index. The table is in storage order, and `item_bytes`, `deleted_at_ms`, and `ttl_at` are before `data`.

**The old-facts read.** A write reads the old facts of its item with one primary-key seek:
`SELECT item_bytes, ttl_epoch_utc_seconds FROM items WHERE routing_hash = ? AND hk = ? AND sk = ?`.
On `latest_items`, the seek reads `item_bytes` and `deleted_at_ms` on `(routing_hash, base_hk, base_sk)`.

- It replaces `#storedEstRowBytes` and its `INDEXED BY idx_items_scan` pin.
- The seek reads the leaf page of the row, which can hold the inline part of `data`, but no overflow page.
- The presence of the row is the "created" signal of `#captureWrite` and of the item-id assignment, in place of
  `oldEst === 0`.
- The key bytes of an item come from its key. The old and the new `overflow_bytes` come from the old and the new
  `item_bytes` (section 4.2.3).

**The new facts.** Each statement that writes an item returns the `item_bytes` that SQLite calculated, with
`RETURNING`, as the writes return `est_row_bytes` today. This covers the put, the update, the compiled update, the
commit apply, and the migration insert. A delete that already returns its row with `RETURNING`, such as the TTL sweep
and the tombstone collection, returns `item_bytes` and the full key. The tombstone collection of `latest_items`
deletes by `item_id`, so it must also return `base_sk` for the bucket lookup of a range leaf.

**Rows written.** An insert into `items` writes 2 rows (the row and its `idx_items_item_id` entry), or 3 with a TTL.
Today it writes 3, or 4.

#### 4.2.3 Storage model

An indexed item is an item with an entry in the partial index of its table: an item with a TTL in `items`, and a
tombstone in `latest_items`. The estimate of one item is:

```text
estimate = item_bytes
         + key_index_factor * key_bytes
         + base_overhead
         + (indexed item ? indexed_key_factor * key_bytes + indexed_overhead : 0)
         + overflow_coefficient * overflow_bytes(item_bytes)
```

The estimate of a group of items uses the facts of the group. It equals the sum of the item estimates:

```text
estimate = item_bytes + key_index_factor * key_bytes + base_overhead * item_count
         + indexed_key_factor * indexed_key_bytes + indexed_overhead * indexed_item_count
         + overflow_coefficient * overflow_bytes
```

| Coefficient | `items` | `latest_items` | What it covers |
| --- | --- | --- | --- |
| `key_index_factor` | 1.25 | 1.25 | The key copy in `idx_items_item_id` or `UNIQUE (item_id)`, and the part-full pages of the table |
| `base_overhead` | 76 | 67.25 | The two routingHash copies, the item id, the integer columns, the record and cell headers |
| `indexed_key_factor` | 1.25 | 1.25 | The key copy in `idx_items_ttl` or in `idx_latest_items_deleted_gc` |
| `indexed_overhead` | 25 | 24.75 | The routingHash copy and the fixed columns of that index entry |
| `overflow_coefficient` | 1 | 1 | The overflow pages. A measured constant error of `overflow_bytes` changes this value, not the stored sums |

- The partial index of `items` is `idx_items_ttl`: its entry is `(ttl_epoch_utc_seconds, routing_hash, hk, sk)`.
  The partial index of `latest_items` is `idx_latest_items_deleted_gc`: its entry is
  `(deleted_at_ms, item_id, routing_hash, base_hk, base_sk)`. The `ttl_at` column of `latest_items` has no index, and
  the model does not count its few bytes.
- Each coefficient is 0 or more, and a multiple of 1/4. Thus a double holds each sum of integer facts exactly, up to
  2^51.
- The coefficients are the multiples of 1/4 that make the largest relative error of the model smallest, for rows with
  20 to 200 bytes of data, with and without an indexed item (appendix A.1). The fit prefers rows with some data to rows
  with no data. The errors, as (measured − model) / measured:

  | Rows | `items` | `latest_items` |
  | --- | --- | --- |
  | 20 to 200 bytes of data | −7.4% to +7.5% | −8.4% to +8.5% |
  | No data | −10.5% to +3.8% | −13.5% to +3.7% |
  | 400 to 700 bytes of data | 0% to +18% | −0.2% to +13.7% |
  | Rows with overflow pages | −0.4% to +0.4% | −0.2% to +0.2% |

  A negative error is an estimate that is too high, so a routingHash of such rows is promoted earlier. A line in the
  key bytes cannot be closer, because a page holds a whole number of rows (section 2.2).
- `shared/partition/item-size.ts` defines the `item_bytes` expression, the model type, the two coefficient sets, the
  SQL fragment that sets `est_x4`, and both forms of `overflow_bytes`. No other module holds a coefficient.

**Overflow pages.** A `WITHOUT ROWID` table keeps its rows in an index B-tree. On a 4 KiB page, SQLite keeps a row
payload of at most 1,002 bytes on the tree page. For a larger payload, it keeps 489 bytes or more on the tree page and
puts the remainder on overflow pages. Each overflow page holds 4,092 bytes of one row only, and its unused space stays
empty. Thus a row of 1,100 bytes uses about 4,600 bytes, and a row of 2,000 bytes also uses about 4,600 bytes. The
other terms count each payload byte one time, so `overflow_bytes` adds the remainder of the overflow pages:

```text
P = item_bytes + row_payload_overhead
if P <= 1002:          overflow_bytes = 0
local = 489 + (P - 489) mod 4092
if local > 1002:       local = 489
moved = P - local
overflow_bytes = ceil(moved / 4092) * 4096 - moved + overflow_row_overhead
```

- `row_payload_overhead` is a constant for each table: the payload bytes of the columns that are not in `item_bytes`
  (the routingHash, the item id, the integer columns, and the record header). It is 40 for `items` and 29 for
  `latest_items`. The measured payload overhead of `items` is 37 to 39 bytes with no TTL, by the size of the item id,
  and 4 bytes more with a TTL. On `latest_items` it is 28 bytes for small item ids, and up to 2 bytes more for a 4-byte
  item id. Thus the constant is within 3 bytes of each row, and only a row whose payload is within 3 bytes of 1,002 can
  get the wrong overflow estimate (appendix A.5).
- `overflow_row_overhead` is a constant for each table: the bytes that a row with overflow pages uses on the tree page
  more than the other terms count. It is 61 for `items` and 69 for `latest_items`, from the rows with overflow pages of
  appendix A.1. It is a fixed value for each such row, not a multiple of the unused overflow space, so it is in the
  function and not in `overflow_coefficient`.
- 1,002, 489, 4,092, and 4,096 are the SQLite values for a 4 KiB page, a usable size of 4,096, and an index B-tree.
- The function reads only `item_bytes`. The old-facts read returns `item_bytes`, so a delete or an update subtracts
  the same value that the write of the old item added (invariant I2). The real payload can differ from `P` by a few
  bytes, because an integer column has a variable size. Such a difference changes the estimate, never the symmetry.
- `row_payload_overhead`, `overflow_row_overhead`, and the page constants are not coefficients. They decide which items
  have overflow pages and what each bucket stores in `overflow_bytes`. Thus a change of one of these constants needs a
  recount of each bucket (section 4.2.7). A change of `overflow_coefficient` needs no recount, like each other
  coefficient.
- `item-size.ts` gives the function in JavaScript, for the write paths and the import, and as an SQL expression over
  `item_bytes`, for the split aggregates and the recount. Both forms must give the same value for each `item_bytes`.

#### 4.2.4 The `item_stats` table

```sql
CREATE TABLE item_stats (
  routing_hash       BLOB    NOT NULL,
  lower_hk           BLOB    NOT NULL,   -- inclusive lower bound (lower_hk, lower_sk) of the bucket;
  lower_sk           BLOB    NOT NULL,   -- (x'', x'') for the first bucket
  item_count         INTEGER NOT NULL,
  item_bytes         INTEGER NOT NULL,
  key_bytes          INTEGER NOT NULL,
  indexed_item_count INTEGER NOT NULL,
  indexed_key_bytes  INTEGER NOT NULL,
  overflow_bytes     INTEGER NOT NULL,
  est_x4             INTEGER NOT NULL,   -- 4 times the estimate of the facts of this row (section 4.2.8)
  PRIMARY KEY (routing_hash, lower_hk, lower_sk)
) WITHOUT ROWID, STRICT;

CREATE INDEX item_stats_promotion ON item_stats (est_x4) WHERE lower_hk = x'' AND lower_sk = x'';
```

- **Bucket rule.** The bucket of an item is the row of its `routing_hash` with the greatest
  `(lower_hk, lower_sk) <= (hk, sk)`.
- **First bucket.** The first bucket of a routingHash has the bound `(x'', x'')`, the smallest composite key. It stays
  while the routingHash has an item. A merge never deletes it, because a merge deletes the upper row.
- **Hash leaf.** Each routingHash has only its first bucket. Two hash keys with one routingHash share it, and their
  combined estimate decides the promotion. A hash leaf never splits a bucket.
- **Range leaf.** The one routingHash has buckets that cover its slice. The import builds them, and each write keeps
  them inside the band of section 4.2.6. The bounds are composite, so a bucket can hold the end of one hash key and
  the start of the next. Appendix G gives an example.
- **`est_x4`** is an ordinary column. Each statement that writes facts also sets it (section 4.2.5). It is not a
  generated column (section 5.23). Only the promotion checks read it. A range leaf also stores it, because both leaf
  kinds use one schema. It costs about 6 bytes in each bucket row and no row written.
- **The promotion index** holds only first buckets. A range leaf never reads it. The query must repeat both terms of
  the index condition.
- **Storage order.** The table leads with `routing_hash`, like `items`. Thus the buckets of one routingHash are one
  key range, and the buckets of a hash child are one key range of the parent (I6 of the routingHash RFC).
- One class, `ItemStats` in `shared/partition/item-stats.ts`, owns the table, its migrations under its own KV key,
  and the split, merge, and recount statements. It replaces `KeySizeStats`. The host gives it the item table, the key
  column names, the indexed-item condition (`ttl_epoch_utc_seconds IS NOT NULL` or `deleted_at_ms IS NOT NULL`),
  the leaf kind, and the coefficient set. These are fixed SQL fragments from the code, never user
  input. The host gives each routingHash as opaque bytes, as it does to `KeySizeStats` today.

#### 4.2.5 Fact updates

Each write path changes the facts with the bucket helper of `ItemStats`, in the transaction of the item change.

| Operation | Fact change |
| --- | --- |
| Insert | Add the new facts and 1 to `item_count`. On a range leaf, then apply the split check |
| Overwrite or update | Add the new facts minus the old facts. `item_count` stays |
| Present-item delete | Subtract the old facts and 1 from `item_count`; then merge on a range leaf |
| Delete of an absent item | No statement |
| TTL sweep | Subtract the facts of the deleted rows from their buckets, one statement for each bucket. On a range leaf, then apply the merge check |
| Migration insert | Add inserted-row facts once for each bucket and page (section 4.2.6) |
| Migration retry | Nothing for a row that is already present |
| Promotion `beforeComplete` | `DELETE FROM item_stats WHERE routing_hash = ?`, one contiguous delete |
| Source copy cleanup | No fact change: the copies have no bucket after `beforeComplete`. After its last item delete, the cleanup runs `DELETE FROM item_stats WHERE routing_hash = ?` again, as a guard, as it does today |
| Prepare and cancel | No change. Pending rows are not committed items |
| Commit apply | The same as the insert, overwrite, update, or delete that it applies |
| TTL change from null to a value | Add the item to `indexed_item_count` and `indexed_key_bytes`. The reverse subtracts it |
| `latest_items` live row to tombstone | Add the item to the indexed facts, and change `item_bytes` to the key bytes. The reverse subtracts it |
| Tombstone collection of `latest_items` | A delete of a present item |
| Read-stamp change (`check`) | No change |

The facts of an item include its `overflow_bytes`. An overwrite or an update that changes `item_bytes` adds the new
`overflow_bytes` and subtracts the old one, which the function gives from the old `item_bytes`.

**Statements that write `item_stats`.** Only `ItemStats` writes the table. Each statement that writes a fact column
must set `est_x4` in the same statement, from the new facts of that row. `item-size.ts` gives one function,
`estSql(itemCount, itemBytes, keyBytes, indexedItemCount, indexedKeyBytes, overflowBytes)`. It takes six SQL
expressions for the new facts and returns 4 times the estimate over them, with the coefficients of the code. In an
SQLite `UPDATE`, each `SET` expression reads the old values of the row, so an update passes the old value plus its
delta:

```sql
est_x4 = estSql(item_count + ?dc, item_bytes + ?db, key_bytes + ?dk, …, overflow_bytes + ?do)
```

A statement must never set `est_x4` from the old `est_x4` plus a delta. A recalculation from the facts gives each
written row the coefficients of the code, so a coefficient change reaches each bucket at its next write (section
4.2.8).

| Statement | Facts that it writes | Arguments of `estSql` |
| --- | --- | --- |
| Hash-leaf upsert, insert branch (first write of a routingHash) | The new facts | The bound new facts |
| Hash-leaf upsert, `ON CONFLICT DO UPDATE` branch | Old facts plus the delta | Each old column plus its delta |
| Range-leaf bucket seek update | Old facts plus the delta | Each old column plus its delta |
| Range-leaf insert of the first bucket, when the routingHash has none | The new facts | The bound new facts |
| Split: update of the lower row | The counted lower facts | The bound counted facts |
| Split: insert of the upper row | The counted upper facts | The bound counted facts |
| Merge: update of the lower row | Lower facts plus upper facts | Each old column plus the upper fact |
| Import: update of the last bucket, and the hash-child upsert | Old facts plus the page sums | Each old column plus its page sum |
| Import: insert of a new bucket | The page sums of the bucket | The bound sums |
| TTL sweep: update of one bucket | Old facts minus the group sums | Each old column minus its group sum |
| Recount of the drift repair | The counted facts | The bound counted facts |
| Coefficient backfill, optional (section 4.2.8) | No fact | The columns of the row |
| Deletes: empty bucket, merge upper row, `beforeComplete`, cleanup guard | The row goes | Nothing |

- No second statement is necessary. The column adds no row write: the statement already writes the bucket row and its
  index entry.
- A new statement that writes `item_stats` must add a row to this table.

**Bucket lookup.** On a hash leaf, the helper upserts the row `(routing_hash, x'', x'')`. On a range leaf, it updates
the bucket with one seek:

```sql
UPDATE item_stats SET item_count = item_count + ?dc, item_bytes = item_bytes + ?db, …, est_x4 = <fragment>
 WHERE routing_hash = ?1 AND (lower_hk, lower_sk) = (
       SELECT lower_hk, lower_sk FROM item_stats
        WHERE routing_hash = ?1 AND (lower_hk, lower_sk) <= (?2, ?3)
        ORDER BY lower_hk DESC, lower_sk DESC LIMIT 1)
 RETURNING …
```

When no bucket exists for the routingHash, the helper inserts the first bucket. When the facts of a write do not
change, the helper writes no row. An update writes the row and its index entry, also when the values stay the same.

**Delete of an unrepresented row.** When a delete removes a row whose routingHash has no bucket, the delete changes no
facts. The bucket did not represent the row, so a subtraction makes the facts wrong.

**Empty buckets.** A bucket that reaches 0 items is below `K`, so on a range leaf the merge check of section 4.2.6
runs on it before the helper removes its row.

- When no merge applies, each pair with the empty bucket already holds at least `K` items. The helper then deletes the
  row if it is not the first bucket. Each neighbour holds at least `K` items on its own, so the pair that the removal
  makes adjacent also holds at least `K` together.
- When the empty bucket is the only bucket of the routingHash, the helper deletes it: the routingHash then has no
  items.
- An empty first bucket that is not alone stays, and the merge check applies to it like to every other bucket.

**TTL sweep.** The sweep deletes a chunk of expired items in TTL order, and each delete returns the facts and the key
of its row. The sweep groups the deleted rows by bucket in JavaScript:

- On a hash leaf, the group is the routingHash.
- On a range leaf, the bucket of a row comes from the select part of the bucket seek. The rows of a chunk are in TTL
  order, not in key order, so the sweep looks up the bucket of each row (M2).

The sweep then writes one update for each changed bucket, and applies the empty-bucket rule and the merge check to it.

**Migration.** The import keeps its page grouping: one bucket write for each page, not for each item. Do not replace
the bulk paths with item-table triggers: a trigger adds one row write for each row of those paths.

#### 4.2.6 Bucket split, merge, and import (range leaf only)

The split and the merge run in the transaction of the write that makes them necessary. No job and no signal exist.

**Split.** After an insert, while a bucket holds more than `2K` items:

1. Find the first item of the upper half with one seek in the primary key:

   ```sql
   SELECT hk, sk FROM items
    WHERE routing_hash = ?1 AND (hk, sk) >= (?2, ?3) AND (hk, sk) < (?4, ?5)
    ORDER BY hk, sk LIMIT 1 OFFSET ?6
   ```

   `(?2, ?3)` is the bound of the bucket, `(?4, ?5)` is the bound of the next bucket, and `?6` is
   `floor(count / 2)`. The last bucket has no upper term.
2. Calculate the facts of the lower half and of the upper half with two aggregates over the same key ranges.
3. Write the lower facts to the old row. Insert a row at the composite key of the first item of the upper half.

- The new bound is a real composite key, not a shortest separator. A bucket bound is not a DO name.
- Step 2 counts both halves from the items. Thus each split also repairs drift in that bucket.
- A split reads about `3K` items in SQLite and reads no row into JavaScript. It selects no `data`. A bucket splits
  at most once for each `K` inserts into it. On the primary key, it costs 1.45 to 1.65 ms locally (appendix A.4).
- `OFFSET` steps over the rows. The primary key has no order-statistic tree, so a seek to the middle costs `K` steps.

**Merge.** After a delete, while the changed bucket holds fewer than `K` items, also at 0 items:

1. Read the previous and the next bucket rows with one seek each.
2. When the bucket and one neighbour hold fewer than `K` items together, add the upper facts to the lower row,
   and delete the upper row. Select the smaller neighbour.
3. Repeat with the merged bucket until each pair with it holds at least `K` items.

A merge reads no item. The band prevents thrash: a split gives two halves of about `K` items each, so the pair holds
about `2K`. A merged bucket holds fewer than `K` items, so it needs more than `K` inserts before it splits again.

**Import.** A target takes no local write while it imports: the runtime answers `partition_migrating`. Its pages arrive
in storage order after the cursor. Thus each item that a page inserts is above each item that the target already
holds. `FokosMigrationHost.validatePage` and `MaterializationMigrationHost.validatePage` refuse an items page whose
rows are not strictly increasing in storage order above the page cursor (`assertItemFollows`). The check costs one key
comparison for each row. The import uses this order and builds the buckets with no item read:

1. Read the last bucket of the routingHash with one seek, or start the first bucket `(x'', x'')`.
2. Add each inserted item to the last bucket in JavaScript.
3. When the last bucket holds `K` items, the next inserted item starts a new bucket at its own composite key.
4. Write each changed or new bucket one time for the page, in the page transaction.

- Each import bucket holds `K` items, except the last, which holds 1 to `K`. Thus invariant I5 holds.
- A retried page inserts no row, so it changes no bucket.
- A hash child is a hash leaf. Its import adds the facts of each run of one routingHash to the first bucket, as
  `KeySizeStats.addRows` does today.
- After the import, the writes use the split and merge rules.

**Failure mode.** The split and the merge read only keys and small columns, and they write small rows. Thus they add
no failure cause to a write. A commit apply can still get `SQLITE_FULL`, as it can today.

#### 4.2.7 Drift repair

When a write would make a fact of a bucket negative, the facts and the items disagree. The bucket helper finds this
from the facts that its statement returns with `RETURNING`.

- The write path must not throw. A throw rolls back a commit apply, the coordinator in `COMMITTING` then retries
  forever, and the lock stays.
- The write path must not clamp with `MAX(0, …)`, because a clamp keeps the wrong facts.

In the same storage transaction:

1. Log the defect one time, with the routingHash, the bucket, the stored facts, and the delta.
2. After the item change, count the facts of the bucket again from its key range of the primary key.
3. Write the counted facts and their `est_x4` with one statement, or delete the bucket under the rule for empty
   buckets of section 4.2.5.

On a range leaf, the recount reads at most `2K` items. On a hash leaf, the bucket is the whole routingHash:
`WHERE routing_hash = ?`. The recount runs only after a defect.

#### 4.2.8 Promotion

`T` is the promotion threshold: the cap of the leaf in bytes times `promotionFraction`, 0.25 by default. Both checks
below compare the `est_x4` of the first bucket of the routingHash with `ceil(4 * T)`. `est_x4` is an integer, so
`est_x4 >= ceil(4 * T)` is the same test as `estimate >= T`.

**`est_x4`.**

```text
est_x4 = 4 * estimate(facts of the row, coefficients of the code that last wrote the row)
```

- Each coefficient is a multiple of 1/4 (section 4.2.3), so `est_x4` is an exact integer.
- It includes each fact of section 4.2.3, also the indexed facts. Thus a TTL change, or a change from a live row to a
  tombstone, changes it. That statement already writes the bucket row and its index entry, so this adds no row written.
- Each statement that writes facts calculates it again from the new facts (section 4.2.5).
- The range planner does not read it. The planner prices the facts with the live coefficients (section 4.2.9).

**Write-path check.** The bucket helper returns the `est_x4` of the bucket with `RETURNING`. On a hash leaf, the host
reports the changed hash key as a candidate when `est_x4 >= ceil(4 * T)`. The runtime maps the hash key to its
routingHash, as today.

**Lookup of the split decision.** It replaces `largestAtLeast`.

```sql
SELECT routing_hash, est_x4
  FROM item_stats
 WHERE lower_hk = x'' AND lower_sk = x'' AND est_x4 >= ?1
 ORDER BY est_x4 DESC
 LIMIT 5
```

1. Bind `?1` to `ceil(4 * T)`. The `LIMIT` is `PROMOTION_CANDIDATES_MAX` (5).
2. SQLite seeks to the largest entry of `item_stats_promotion` and reads the index backwards. It stops after 5 rows, or
   at the first row below `?1`.
3. For each row, read one hash key with one primary-key seek:
   `SELECT hk FROM items WHERE routing_hash = ? ORDER BY hk, sk LIMIT 1`. The host promotes by hash key, and the
   runtime maps it back to the routingHash.
4. Return at most 5 hash keys, largest estimate first.

- When the seek of step 3 finds no row, the bucket has no items. Log the defect one time and skip that routingHash. Do
  not read more index rows to replace it. The lookup then returns fewer candidates, and its bound stays.
- **Bound.** The lookup reads at most 6 index entries (5 rows, and the row that stops the read) and makes at most 5
  primary-key seeks. The bound does not depend on the leaf size, the number of routingHashes, or the cap. Thus a cap
  reduction does not make the lookup longer. A read of 6 index rows takes about 0.15 ms locally (appendix A.4).
- The query must repeat both terms of the index condition. Otherwise SQLite can scan the table with no error. A test
  asserts the plan with `EXPLAIN QUERY PLAN` (section 4.2.16).
- The lookup runs only while the leaf is over its cap. It runs after the write commits, so it reads committed facts.
- The lookup keeps no result between two decisions, and it writes no row. Each decision reads again.

**Coefficient change.** A release can change a coefficient. A bucket row then keeps the `est_x4` of the old
coefficients until a statement writes it again. Correctness does not need a job or a version, because these paths give
each row the new value:

- Each write of a routingHash calculates `est_x4` again from the facts. A routingHash that grows gets the new value at
  its next write.
- Each split, merge, and recount of a bucket does the same.
- Each import builds its buckets with the coefficients of the code. Thus each hash child, each range child, and each new
  range root starts with the new values. The source becomes a router and does not read its `item_stats` again.

Only the first bucket of a routingHash with no write since the deployment, on a leaf that did not split, keeps the old
value. The effects:

| Coefficient change | Effect on such a routingHash | Cost |
| --- | --- | --- |
| Larger | The lookup can miss it when its new estimate is at or above `T`. The leaf then makes a hash split. The import of the child gives it the new value, and the write-path check or the next decision of the child promotes it | One more copy of the routingHash |
| Smaller | The lookup can return it when its new estimate is below `T` | An early promotion |
| Either | The order in the top five can change | None |

- Item validity, the byte budgets, the facts, and the range boundaries do not read `est_x4`. A stale value cannot
  change them.
- Such a routingHash gets no writes, so it does not make the leaf grow.
- Each Durable Object uses the coefficients of its code version. A rollout needs no coordination between Durable
  Objects.

**Optional backfill.** Neither milestone builds it. When a coefficient change must reach each bucket before the next
writes and imports:

1. `ItemStats` stores the version of the coefficients under its KV key. A new `item_stats` table stores the version of
   the code.
2. When the class starts and the stored version is older than the code version, a background job sets `est_x4` from
   the facts of each bucket row, in primary-key order, in bounded batches in the alarm, for example 500 rows. A first
   bucket writes the row and its index entry. Another bucket writes the row only.
3. When the job has updated each row, it stores the code version in KV.

The lookup does not change during the job.

**Promotions in progress.** An in-flight promotion keeps its place in the top five and answers `already_promoted`. The runtime
tries each other candidate of the returned set independently. An unfinished promotion must not prevent the discovery
of the other top-five routingHashes. When `beforeComplete` removes its buckets, the next decision finds the next ones.

**Retry after a write.** The write-path signal can fail before it queues a promotion. Each decision reads the committed
facts again, so a later decision finds the eligible routingHash again. Each decision retries its returned candidates,
also after an earlier promotion request failed.

The rest of the decision stays: a hash leaf with one routingHash promotes it whatever its estimate, and a leaf with
two or more routingHashes promotes its candidates before it splits. `probeRoutingHashes` does not change.

#### 4.2.9 Range planner

The planner keeps one `transactionSync`. Its inputs are the routingHash, the owned composite slice `[start, end)`, and
`N`. On a range leaf, the buckets of the routingHash cover exactly that slice. All reads use the key range
`routing_hash = ? AND (hk, sk) >= start AND (hk, sk) < end` of the primary key, or a part of it.

The planner reads all bucket rows of the slice in storage order and prices them with the live coefficients. It does
not read the buckets in size order, so it does not read `est_x4`.

**Bucket path (M2).**

1. Apply the count check of section 4.2.10. Return `fewer_items` when the slice holds fewer than `N` items.
2. Read the bucket rows in storage order. Price each bucket with the model, and keep the running total before each
   bucket. The total is `B`.
3. Calculate the targets `i * B / N` for `i = 1 … N - 1`, and find the bucket that holds each target.
4. Scan each of those buckets one time, in storage order. Start from the running total before the bucket, add each
   item estimate, and select the gap nearest each target. The candidate gaps are the gaps inside the bucket, the start
   of the bucket, and the start of the next bucket. When two gaps have the same distance, select the earlier gap.
5. Assign the boundaries in target order, with the same rule as the streaming path: the gap nearest the target after
   the previous boundary.
   - When the gap that step 4 selected for a target is after the previous boundary, it is the boundary.
   - Otherwise the boundary is the first gap after the previous boundary, and the planner reads the item that ends
     it: one row for each such step. This collision occurs only when one item weighs at least `B / N`.
6. When a boundary is the last gap and later targets have no boundary, apply the repair of the streaming path: read
   the last `N` items of the slice in reverse order, keep the boundaries before their first gap, and fill the missing
   boundaries from their gaps, latest gap first.
7. Make each boundary with `KeyCodec.shortestCompositeSeparator` of the two items at its gap. At the start of a
   bucket, one seek reads the item before the bucket bound: `ORDER BY hk DESC, sk DESC LIMIT 1`.

`C` is the item count of the slice. The planner reads at most `2C / K + 1` bucket rows. Its scans read at most
`min(N - 1, buckets) * 2K` rows for the buckets of the targets, at most `N - 1` rows for the collision steps, and at
most `N` rows for the repair: at most `min(N - 1, buckets) * 2K + 2N` rows in total (appendix E).

**Streaming path (M1).**

1. Apply the count check.
2. Read the items of the slice in storage order through a streaming cursor, and price each item with the model.
3. For each target in order, select the gap nearest the target after the previous boundary. Ties select the earlier gap.
4. Keep the last `N` items of the scan in a buffer. These items have `N - 1` gaps.
5. Stop the scan when it has `N - 1` boundaries.
6. When the cursor ends with fewer boundaries, repair: keep the boundaries before the first gap of the buffer, and fill
   the missing boundaries from the gaps of the buffer, latest gap first.
7. Make each boundary with `KeyCodec.shortestCompositeSeparator`.

**Both paths.**

- For a cut after `k` items, the lower prefix holds only those `k` items.
- A gap is the position between two adjacent items. No gap exists before the first item or after the last item, so each
  child holds at least one item.
- A gap can be between two hash keys of one routingHash. The separator is then `(s, x'')`, as
  `shortestCompositeSeparator` defines.
- `B` and the facts control only the balance. Wrong facts give a worse balance, but the boundaries stay valid.
- The planner must preserve storage order, strict boundary order, and the slice bounds. It must not use repeated
  `OFFSET` walks in JavaScript or materialize the slice. It selects `hk`, `sk`, `item_bytes`, and
  `ttl_epoch_utc_seconds`, never `data`.
- Remove `skewed_bytes`. `RangeSplitPlan` keeps only `fewer_items` as a floor. `SplitPlanner.rangeBoundaries`, the
  `computeRangeBoundaries` hook, returns the boundaries, or `null` for that floor.
- `SplitSource.compositeKeySizes` gives the facts of each item in place of one size.
- This planner does not give the optimal balance. An item cannot cross a boundary, so a dominant item makes equal
  children impossible. A child that is still over its cap splits again.

This section replaces the threshold loop of `range-split-boundary-byte-seek.md` and the range-floor rule of
`2026-09-27-over-size-split-trigger.md`.

#### 4.2.10 Split decision of a range leaf

With the planner of section 4.2.9, `fewer_items` is the only range floor. For a range leaf over its cap,
`SplitPlanner.evaluateSplit`:

1. Counts the items of the slice in its primary-key range with `LIMIT N`. The planner uses the same function.
2. Answers a split when the count is `N` or more, and returns the `fewer_items` floor when it is less.
3. Does not call the planner.

The decision needs one fact: is a split possible. The item count gives that fact. The decision runs before a refused
write gets its error, so a scan there blocks the partition for each write.

Only `#plan` reads more, one time for each queued split, in the alarm. When deletes reduce the count below `N` after
the decision, the planner returns `null`, and `#plan` waits with backoff, as it does today. The in-memory cache of the
floor result of `2026-09-27-over-size-split-trigger.md` is not necessary.

The planner change and this decision ship together. With the current planner, weights `[111, 5111, 111]` and `N = 3`
pass the count check, but `#plan` returns `null` at each try, so the split never happens.

#### 4.2.11 Item limit, query budget, and export budgets

- `measureItemBytes`, the put guard of `upsertItem`, the update guard of `updateItemSingleShot`, the compiled update
  probe of `expression/runtime.ts`, and the prepare and single-shot checks of `TransactionParticipant` compare
  `item_bytes` with `MAX_ITEM_BYTES`.
- The early client check `validateItemDataSize` stays a lower bound on the data only.
- The query scan gives `item_bytes` to its candidate consumer. The collector charges it for each evaluated item,
  also for an item that the filter rejects. The cursor stays a key position.
- The count selection of `queryScanStatement` reads `sk` and `item_bytes` from the primary key, with no `INDEXED BY`
  pin.
- `MaterializationExportConfig.maxBytes` and `TargetRunnerConfig.maxBytesPerStep` count `item_bytes`.

These changes move some page and batch boundaries.

#### 4.2.12 Materialization DO

`MaterializationStore` uses the same machinery as `PartitionStore`:

- `item_bytes` and the primary-key reads of section 4.2.2.
- `ItemStats` with the `latest_items` coefficients.
- The same fact updates, split, merge, import, promotion lookup, and planner.

Its routingHash comes from its own runtime, with the seed of the materialization shard group. Its indexed items are
its tombstones. The tombstone GC delete is a delete of a present item, and it returns `base_sk` (section 4.2.2). Its
`beforeComplete` and its cleanup guard delete the buckets of the promoted routingHash, as in the partition.

#### 4.2.13 Invariants

Each invariant must hold after each commit. The verification of section 4.2.16 checks each one.

| # | Invariant | Mechanism |
| --- | --- | --- |
| I1 | The facts of each bucket equal the sums over the items in its key range | Each write changes the item and its facts in one storage transaction (section 4.2.5). The drift repair corrects a defect in the same transaction (section 4.2.7) |
| I2 | The facts that a write adds for an item equal the facts that a later update or delete of that item subtracts | The old-facts read returns `item_bytes`, and `overflow_bytes` is a function of `item_bytes` only. Its JavaScript and SQL forms give the same value (sections 4.2.2 and 4.2.3) |
| I3 | The `est_x4` of each bucket equals `estSql` over its facts, with the coefficients of the code that last wrote the row. After a coefficient change, a row with no write since the deployment can keep the older value | Each statement that writes facts sets `est_x4` from the new facts in the same statement, never from the old `est_x4` (section 4.2.5) |
| I4 | A hash leaf holds exactly one bucket for each routingHash that has items: its first bucket `(x'', x'')` | The helper upserts the first bucket and deletes it with the last item (section 4.2.5). A hash leaf never splits a bucket |
| I5 | On a range leaf, each bucket holds at most `2K` items, and each two neighbour buckets hold at least `K` items together. The first bucket stays while the routingHash has an item | The split and the merge run in the transaction of the write (section 4.2.6). The import makes buckets of `K` items. A merge deletes the upper row |
| I6 | The buckets of a routingHash are one key range of `item_stats`, and the items of a bucket are one key range of the primary key of the item table | Both tables lead with `routing_hash` and are in storage order (section 4.2.4). Thus a promotion cleanup is one range delete |
| I7 | The rows of an imported items page are strictly increasing in storage order above the page cursor | `assertItemFollows` in `validatePage` of both migration hosts (section 4.2.6) |
| I8 | A retried migration page adds facts only for the items that it inserts | The import adds the facts of a row only when its insert reports a new row (section 4.2.6) |
| I9 | The promotion lookup reads at most 6 index entries and makes at most 5 primary-key seeks, for each leaf size and each cap | The partial index `item_stats_promotion`, `LIMIT 5`, and a query that repeats the index condition (section 4.2.8) |
| I10 | The check after a write and the promotion lookup use the same estimate and the same `T` | Both compare the stored `est_x4` of the first bucket with `ceil(4 * T)` (section 4.2.8) |
| I11 | When a slice holds at least `N` items, the planner returns `N - 1` boundaries in increasing order. Each child holds at least one item, and each boundary is the shortest key that separates two adjacent items | One `transactionSync` for all reads, gaps only between items, and `KeyCodec.shortestCompositeSeparator` (section 4.2.9) |
| I12 | A read of sizes, counts, or facts reads no item data | `item_bytes` and the TTL columns are before `data`, and no size read selects `data` (sections 4.2.2 and 4.2.9) |
| I13 | The split decision of a range leaf reads at most `N` items | The count check with `LIMIT N` (section 4.2.10) |

#### 4.2.14 Performance

The costs below come from local workerd on an AMD Ryzen 9 7940HS, on the clustered layout of section 4.2.2
(appendix A.4), with `K = 2,048` and small items of about 150 bytes. Production CPUs can be slower. TODO: measure on a
deployed Durable Object (section 4.3.1).

| Path | Cost |
| --- | --- |
| Old-facts read | 11 to 15 µs, 1 row read |
| Write on a hash leaf | First-bucket upsert: 24 µs on one hot routingHash, 52 to 74 µs spread over 100,000 routingHashes. 1 row read, 2 rows written (the row and its promotion index entry). With the item row and its `idx_items_item_id` entry, an insert writes 4 rows. Today it writes 5: `items`, `idx_items_scan`, `idx_items_item_id`, and the `key_size_estimates` row and its index entry |
| Write on a range leaf | One bucket seek update: 8 to 13 µs, 3 rows read, 1 row written (2 for the first bucket) |
| Split | 1.45 to 1.65 ms, 6,150 rows read, 2 rows written, at most once for each `K` inserts into a bucket |
| Merge | 2 more seeks for each delete while the bucket holds fewer than `K` items; 2 row writes for a merge |
| Import | No item read for the buckets; one bucket write for each `K` imported items, and one for each page |
| Promotion lookup | At most 6 index rows and 5 primary-key seeks, at each cap. About 0.15 ms for 6 rows. No row written. It runs for each split decision of a hash leaf over its cap |
| Split decision of a range leaf | At most `N` items |
| Planner, `N = 4`, caps of 500 MiB to 2 GiB | About 30 to 50 ms. Measured: 12 ms for a slice of 300,000 items |
| Planner, `N = 4`, each cap up to 10 GB | At most about 150 ms |
| Planner, `N = 255`, each cap up to 10 GB | At most about 2 s |
| Bucket table of a leaf at 2 GiB | At most about 14,000 rows, about 1 MB |
| Storage of a bucket row | About 66 bytes for a first bucket, 69 bytes for a range bucket (appendix A.3) |

The write that splits a bucket blocks the partition for about 1.5 ms. The planner blocks the partition for its
duration, one time for each split.

The timings use sort keys of 16 and 100 bytes. From 16 to 100 bytes, a JavaScript row read and a split at `K = 2,048`
each cost about 7% more. A longer key also makes each item larger, so a leaf holds fewer items, and the planner reads
fewer bucket rows. Thus small items give the worst case.

#### 4.2.15 Deployment and migration

- Edit the existing schema migrations of `PartitionStore` and `MaterializationStore` in place. Do not add migration
  entries.
- In the `items` migration, drop `idx_items_scan`, replace `est_row_bytes` with `item_bytes`, and update the schema
  comment: the size reads use the primary key, and `EST_ROW_BYTES_K` goes.
- Remove `key_size_estimates`, its index, `KeySizeStats`, and the KV key `__fokosdb/key_sizes/sql_schema_version`.
  `ItemStats` tracks its migrations under `__fokosdb/item_stats/sql_schema_version`.
- A deployment must use new namespaces. Existing Durable Objects are destroyed, not migrated, so the change needs no
  backfill and no rollback procedure.
- Key encoding, the routingHash, DO names, and committed item contents stay the same.
- After the release, a coefficient change is an edit of the constants. It must not change item validity or commit
  eligibility. The stored `est_x4` follows at the next write or import, or with the optional backfill of section 4.2.8.
- Update `AGENTS.md` for the range floor, the promotion lookup, and the bucket rules.

#### 4.2.16 Verification

Tests must check definitions, not only the agreement between two uses of the same value.

**Exact facts**

- Check multibyte text, binary data, JSONB, binary key tags, and an absent sort key.
- Check inserts, overwrites, compiled updates, deletes, and absent deletes, and the "created" signal of the capture
  and of the item-id assignment.
- Check null-to-TTL, TTL-to-null, and non-null TTL changes, TTL expiry, and the subtraction from the bucket.
- Check live-to-tombstone, tombstone-to-live, and tombstone collection in `latest_items`.
- Check `overflow_bytes` at `P` of 1,002, 1,003, 5,094, 5,095, and `MAX_ITEM_BYTES`. Check that the JavaScript and the
  SQL forms give the same value. Insert, update across the limit, and delete items of these sizes, and check that the
  bucket returns to 0.
- Check migration retries, page grouping, restart, and source copy cleanup, with its guard delete.
- On a range leaf, check a TTL sweep chunk whose rows are in several buckets, and a tombstone collection of
  `latest_items`. Each bucket must equal the SQL sums of its range after the operation.
- Check that the page validation refuses an items page with a row out of storage order, or at or below its cursor.
- Check that prepare and cancel add no facts.
- After `beforeComplete` of a promotion, check that a delete of a copy creates no bucket and changes no other bucket.
- Write a wrong bucket directly with SQL in the test. Then commit a delete in that bucket. Check that the commit
  succeeds, the defect log appears one time, and the facts equal the SQL sums again.
- Every other test treats the defect log as a failure.

**Buckets (property-based, `fast-check`)**

- Run random sequences of inserts, overwrites, deletes, TTL expiries, and import pages. After each operation, check:
  - The buckets cover the slice with no gap and no overlap.
  - Each bucket equals the SQL sums of its composite range.
  - Invariants I4 and I5 hold.
- Include two hash keys with one routingHash (`collidingHashKeys` in `test/hash-digits.ts`). On a hash leaf, both share
  one bucket. On a range leaf, a bucket can hold items of both, and a boundary between them is valid.
- Check an append-only sort-key sequence and a "delete most items" sequence.
- Check that an import gives buckets of `K` items, except the last, and reads no item for them.
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

- Compare each lookup result with a reference: a full scan of the first buckets that calculates each estimate.
- Check a routingHash with many small items and long keys that ranks below a routingHash with large items by
  `item_bytes`, but above it by estimate. The lookup must return it.
- Check a routingHash of 1-byte items with no data. The lookup must return it when its estimate is at or above `T`.
- Check that `est_x4` equals 4 times the estimate of its facts after inserts, updates, deletes, TTL changes,
  tombstone changes, splits, merges, imports, and recounts.
- Write a bucket with an old `est_x4` directly with SQL in the test, as after a coefficient change. Check that the
  next write of that routingHash sets `est_x4` from the facts, and that an import of the routingHash gives the new
  value.
- Check `T` values for which `4 * T` is not an integer.
- Check that the write-path estimate equals the lookup estimate for a routingHash with long sort keys and TTLs.
- Check that two hash keys with one routingHash give one candidate, with a hash key that has items.
- Lower the cap below the size of a leaf with many routingHashes. The lookup must give the same result as the
  reference, and `rowsRead` must stay within the bound of invariant I9.
- While one promotion is unfinished, check that the decision still returns the other eligible top-five routingHashes.
- Seed six routingHashes with estimates `[100, 90, 80, 70, 60, 50]` KiB at `T = 10 KiB`. Reduce the first five to
  11 KiB through deletes or TTL expiry before cutover. The next decision must return the sixth first.
- Make a routingHash cross `T` through a committed write. Fail its promotion request before the queue write. A later
  decision must find it again and retry while another promotion is unfinished.

**Planner and decision**

- Four equal weights and `N = 2` give two items for each child.
- `[111, 5111, 111]` and `N = 3` give three non-empty children. `[111, 111, 5111]` and `N = 3` use the repair and give
  `[111]`, `[111]`, `[5111]`. Check dominant items at the start, the middle, and the end.
- Check exactly `N` items, fewer than `N`, an empty slice, variable sort-key lengths, TTL costs, variable data sizes,
  explicit composite slice bounds, shortest composite separators, string and binary keys, and children that cover the
  slice.
- Check that the bucket path and the streaming path give the same boundaries on the same items, also when adjacent
  items above `B / N` force a collision and the end repair.
- Check that the scans of the bucket path stay inside the bound of appendix E when collisions occur.
- Check that wrong facts change the balance and not the validity.
- A range leaf over its cap with `C >= N` answers a split, and with `C < N` returns `fewer_items`. A refused write on
  a range leaf does not call the planner.
- Replace the tests that expect `null` or the `skewed_bytes` floor for the dominant-item case.

**Query plans and flows**

- Use `EXPLAIN QUERY PLAN` to assert a primary-key search on a key range for the size scans, the count checks, the
  split statements, the recount, the planner scans, and the old-facts read. Assert that the promotion lookup uses
  `item_stats_promotion`, and that the promotion cleanup of `item_stats` is a primary-key range delete.
- Check range split, migration, promotion, and transaction flows in the Workers runtime with the existing partition
  harness. Add no production test hook.

### 4.3 Open questions

#### 4.3.1 Costs on the clustered layout and on a deployed Durable Object

The local costs on the clustered tables are measured (appendix A.4). A JavaScript row read of the primary key costs
about 1.8 µs, against 1.2 µs over `idx_items_scan`, and a split at `K = 2,048` costs about 1.5 ms, against 0.8 ms.
The planner stays within the bounds of section 4.2.14, so the fallback of section 5.16 is not necessary locally.

TODO: measure the same statements on a deployed Durable Object before the release. If the split costs much more than
1.5 ms there, the answer can change `K` (appendix F).

## 5. Alternative options

### 5.1 A larger fixed constant

It reduces part of the error. It cannot represent variable key copies, it keeps item validity tied to storage, and old
rows keep their old price.

### 5.2 A priced weight in each row

A scan then reads a ready weight. A coefficient change then needs a rewrite of each item, or leaves mixed model
versions. Exact facts avoid this.

### 5.3 A rank column with fixed weights and a ratio bound

Each bucket stores `rank_bytes = item_bytes + RANK_KEY * key_bytes + RANK_ITEM * item_count + RANK_OVERFLOW *
overflow_bytes`, with integer weights that are not the coefficients. The lookup walks the index on `rank_bytes` from
`T / r`, where `r` bounds `estimate / rank_bytes` from the live coefficients and the weights. It calculates the estimate
of each row that it reads, and it stops when `r * rank_bytes` is below the fifth estimate. Thus it misses no candidate
after a coefficient change, with no row rewrite.

Not chosen. It needs the bound `r` and its proof, a bench step that selects the weights, and a versioned job for a
weight change. The walk reads about 7 rows at 1.1 times the cap, but about `4 * X * 1.65` rows when the leaf is `X`
times its cap, for example about 66,000 rows after a cap change from 10 GB to 1 MB. Its guarantee is exact against a
heuristic threshold: the model can be up to 18% below the measured storage (section 4.2.3). The stored estimate of
section 4.2.8 can miss a routingHash only after a coefficient change, and only until its next write or import. The cost
of such a miss is one more copy of that routingHash.

### 5.4 An expression index on the estimate

The query must repeat the index expression exactly. A difference removes the index from the plan with no error, and
a coefficient change rebuilds the index. Rejected.

### 5.5 No index for the promotion lookup

The lookup then reads every routingHash on each write while the leaf is over its cap. A scan of 1M index entries takes
about 0.05 s. Rejected.

### 5.6 Promotion by the share of the leaf, or calibration with `sql.databaseSize`

Both compare the estimate of a routingHash with the total of the leaf. A maintained total costs one more row write on
each write, and a summed total costs a scan of each routingHash. Copies that wait for cleanup also change the total.
The measured model is accurate for most rows, so the absolute threshold is sufficient. Rejected.

### 5.7 A byte target for the bucket size

A byte rule, for example `S = cap / 256`, does not bound the work, because the scan cost depends on the item count
(appendix D). Rejected.

### 5.8 A background job for bucket splits

A job keeps the band only when it keeps up with the writes. Until it runs, a bucket can grow past `2K`, and the planner
bound fails. The job also needs a signal, an alarm, and a time budget. The inline split costs about 1.5 ms once for
each `K` inserts. Rejected.

### 5.9 A cheaper split with one aggregate

The split can count only the lower half and calculate the upper half as the total minus the lower half. It then reads
about `K` items in place of `3K`. It loses the drift repair of each split. Not chosen.

### 5.10 The scan in chunks over some alarm steps

It bounds each step, but not the total work, and it needs resumable plan state in the repartition flow. The buckets
bound the total work. Not chosen.

### 5.11 An upper limit on `rangeSplitConditions.maxSizeMb`

A limit of about 2 GiB keeps the streaming scan of M1 below about 19 s at `N = 4`. With buckets, the planner reads
about `2C / K` bucket rows plus a bounded scan, at most about 0.15 s at `N = 4` up to 10 GB (appendix F). Thus no
limit is necessary. Not chosen.

### 5.12 Skip the refine scan for a large `N`

Without the refine scan, a boundary can move by one bucket. The error of a boundary is then at most the estimate of
that bucket: at most `N * largest_bucket_estimate / B` of a child. Only with items of equal weight is that
`2K * N / C`: 1.5% for `N = 255` and 70M items. A bucket of `2K` near-maximum items next to buckets of small items
can hold several child targets at once. This option removes most of the 2 s worst case at `N = 255`. Not chosen,
because the default `N` is 4, and one rule for every `N` is simpler.

### 5.13 A generated item-size column

With no covering index, a `STORED` generated column works for the reads of the primary key. Not chosen: the item
limit check runs before the write, in the prepare check and in the compiled update probe, so it needs the same SQL
expression anyway. A change of the expression of a generated column also needs a table rebuild.

### 5.14 RPC estimates as storage weights

RPC estimators measure a different representation: public JSON reads use decoded text, and stored JSON uses JSONB.
They also do not represent SQLite indexes. Rejected.

### 5.15 Whole-slice materialization or an order-statistics structure

Materialization breaks the streaming memory rule. An order-statistics structure adds work to each mutation. Buckets
give bounded planner work with one row update for each write. Rejected.

### 5.16 Keep a covering index for the size reads

An index `(routing_hash, hk, sk, item_bytes, ttl_epoch_utc_seconds)` gives the size reads narrower entries than the
primary key. It costs one more row written for each insert and one more key copy for each item, on each partition.
The buckets bound each size read to at most `2K` items for each target, so the narrower entries save little. Not
chosen, unless the measurements of section 4.3.1 show that the primary-key scans break the planner bounds.

### 5.17 A representative hash key in each bucket

`KeySizeStats` stores one hash key in each row today. It costs the bytes of a hash key in each first bucket, and the
key can outlive its own items. The primary-key seek of section 4.2.8 gives a hash key with items, for at most five
seeks for each lookup. Rejected.

### 5.18 Two facts for the items above a size limit

Each bucket counts the items above a fixed size and their `item_bytes`, and the model gives them their own
coefficients. The overflow cost of a row is not linear in its size: a row of 1,100 bytes and a row of 2,000 bytes both
use about 4,600 bytes. Such sums cannot give that cost. `overflow_bytes` gives it for each item. Rejected.

### 5.19 An overflow column in each item row

Each row stores its own `overflow_bytes`, and a delete subtracts the stored value. This adds bytes to each row. The
function of `item_bytes` gives the same value again at the delete, so the column adds nothing. Rejected.

### 5.20 A promotion index on `item_bytes`

A walk on this index then needs a bound on `estimate / item_bytes`, as in section 5.3. The 1-byte item sets that bound, because the fixed bytes of
each item divide by its size: `r = 1 + key_index_factor + indexed_key_factor + base_overhead + indexed_overhead`,
about 105 with the coefficients of `items`. A typical routingHash has a ratio of about 2. The walk then reads about
`4.4 * 105`, or 460, rows at 1.1 times the cap. Most of them cannot be candidates. The stored estimate of section
4.2.8 orders the rows exactly. Rejected.

### 5.21 A minimum `item_bytes` for each partition

The term `(base_overhead + indexed_overhead) / 1` of section 5.20 then divides by the minimum. An insert can lower the
minimum with no read, but a delete cannot raise it without a scan. One small item thus keeps `r` high until a split
or an import resets the minimum. An average or a maximum cannot bound the ratio. Rejected.

### 5.22 An in-memory cache of the top five

The leaf keeps the result of the last lookup until a fact or a pricing input changes. Each write to `item_stats`, each
rollback, and each eviction must clear it. The lookup reads at most 6 index rows, so the cache saves almost nothing.
Rejected.

### 5.23 `est_x4` as a virtual generated column

`est_x4 INTEGER GENERATED ALWAYS AS (<estimate formula>) VIRTUAL`, with the same partial index. SQLite calculates the
value from the facts of the row. No statement sets it, it cannot drift from the facts, and the table row does not store
it. The index stores it. Durable Object SQLite accepts it in a `STRICT, WITHOUT ROWID` table, and the lookup uses the
index. Rejected, because each later change of the formula is a schema change:

- A change of a coefficient must drop the index, drop the column, add it with the new formula, and create the index
  again. `CREATE INDEX` writes one row for each routingHash in one statement, so the rate cannot be bounded.
- SQLite refuses to drop a column that the formula uses. A later release that removes such a fact must change the
  formula first.
- `SELECT *` returns the column, but no statement can write it. A table copy, for example in a table rebuild, must
  name its columns.
- The migration writes the coefficients into the schema, and the planner reads them from the code. A code change with
  no schema change gives two models in one partition.

The ordinary column costs about 6 bytes in each bucket row and one SQL fragment in the fact statements. A coefficient
change then reaches the rows at their next write, or with a bounded backfill.

## 6. Frequently asked questions

### Does the estimate equal `sql.databaseSize`?

No. It estimates the storage of committed items from exact facts. Part-full pages, the overflow pages of index
entries, shared partition state, `item_stats`, and transaction storage stay outside that equality. The physical size
continues to control the cap.

### How does a change of the schema reach the model?

- **A new column, index, or key copy for each item.** Measure again, and change the coefficients. Rows and buckets
  store exact facts only, so the new coefficient applies to each estimate of the planner at once, with no migration.
  The stored `est_x4` of a bucket gets the new coefficient at its next write or import (section 4.2.8).
- **A new table with its own storage shape.** The feature adds the facts that its shape needs: a count for a variable
  number of rows, a byte sum for variable data, or a sum over the items that hold a conditional row. It updates them
  with its rows in the same transaction, and its migration and delete paths move or remove them with the item. A new
  fact needs a schema migration, a bounded backfill, and a term in `estSql`.

### Can `K` change after the release?

Yes. Each bucket layout gives valid boundaries, so correctness does not depend on `K`. Until the layout reaches the new
band, two bounds apply:

- Bucket rows: at most `2C / min(old K, new K) + 1`.
- Bucket occupancy: at most `2 * max(old K, new K)` items.

**A larger `K`.** Each existing bucket is already below the new `2K`, but two neighbours can hold fewer than the new
`K` together. The bucket-row bound then stays at `2C / K + 1` rows of the old `K` until deletes merge the small pairs.
A bucket that keeps growing already reaches the new `2K` items. A mixed layout can thus cost the old bucket-row bound
and the new refine-scan bound at the same time. A one-time job can merge the pairs earlier. A merge reads bucket rows
only, no item.

**A smaller `K`.** The old pair rule holds at deployment, but later deletes can leave pairs below the old `K`. The new
merge rule permits those pairs, so the bucket-row bound uses the new `K`. An unchanged bucket can still hold up to the
old `2K` items. Its next insert splits it in the write. Until then, its refine scan can read the old `2K` rows. A mixed
layout can cost the new bucket-row bound and the old refine-scan bound together. A one-time job is necessary only for
oversized buckets that get no inserts.

Each Durable Object uses the `K` of its code version, and each child builds its buckets during its import. A rollout
needs no coordination between Durable Objects.

### What happens with sort keys that only increase?

Each insert goes to the last bucket. It splits after about `K` inserts, so the cost is about 3 item reads for each
insert. The planner is not affected.

## 7. References

Code paths are relative to `packages/fokosdb/src`:

- `shared/partition/item-size.ts` — the stored-size expression and the data expression.
- `shared/partition/key-size-stats.ts` — the current summary for each routingHash, which `ItemStats` replaces.
- `shared/partition/split-planner.ts` — `SplitPlanner`, `SplitSource`, and `planRangeBoundaries`.
- `shared/partition/partition-store.ts` — the schema, the mutations, the query scans, `#storedEstRowBytes`,
  `probeRoutingHashes`, and `compositeKeySizes`.
- `shared/partition/fokos-migration-host.ts` — the per-page migration accounting, the page validation, and
  `assertItemFollows`.
- `shared/partition/transaction-participant.ts` — the prepare checks and the committed mutations.
- `shared/partition/materialization-export.ts` — the export byte budget.
- `shared/partition-context.ts` — the validation of `maxSizeMb`.
- `shared/expression/plan.ts` and `shared/expression/runtime.ts` — the compiled update statement and its size probe.
- `shared/query/query-collector.ts` — the evaluated-byte and response-byte charges.
- `server/do-partition.ts` — admission, `signalGrowth`, the split hooks, and `beforeComplete`.
- `server/materialization/materialization-store.ts`, `materialization-migration-host.ts`, and `target-runner.ts` —
  `latest_items`, its migration accounting, and its batch budget.
- `sharding/key-codec.ts` — `compareComposite` and `shortestCompositeSeparator`.
- `sharding/runtime.ts` — `#admit`, `requestSplitEvaluation`, and `#evaluateSplit`.
- `sharding/repartition-flow.ts` — `#plan` and its backoff.

Tests and benches, relative to `packages/fokosdb`:

- `test/storage-bench/item-size-model.workerd-bench.ts` — the coefficients, `row_payload_overhead`, the overflow
  pages, the `item_stats` rows, and the statement costs of appendix A.
- `test/hash-digits.ts` — `collidingHashKeys`, two hash keys with one routingHash.

Repository documents:

- `AGENTS.md` — invariants, test harnesses, and verification commands.
- `docs/agent-plans/2026-11-01-routing-hash.md` — the routingHash, the clustered layout, promotions by routingHash,
  and composite keys.
- `docs/agent-plans/range-split-boundary-byte-seek.md` — the streaming boundary scan that section 4.2.9 replaces.
- `docs/agent-plans/2026-09-27-over-size-split-trigger.md` — the physical cap, promotion decisions, and range floors.
- `docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md` — `latest_items` and the GSI physical sort key.
- `docs/research/claude-hashing_and_splits_across_distributed_databases.md` — split triggers and split points in
  other systems.

Platform references:

- [SQLite file format: SQL indexes](https://www.sqlite.org/fileformat2.html#representation_of_sql_indices)
- [SQLite: clustered indexes and the WITHOUT ROWID optimization](https://www.sqlite.org/withoutrowid.html)
- [SQLite: generated columns](https://www.sqlite.org/gencol.html)
- [Durable Object SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Durable Object limits](https://developers.cloudflare.com/durable-objects/platform/limits/) — 10 GB of storage for
  each Durable Object, and 30 s of CPU for each invocation by default, up to 5 minutes with `limits.cpu_ms`.

## Appendix A. Measurements

### A.1 Coefficients on the clustered layout

Date: 2026-11-05. Local workerd under `@cloudflare/vitest-plugin`, AMD Ryzen 9 7940HS, page size 4,096 bytes.
`test/storage-bench/item-size-model.workerd-bench.ts` creates the tables of section 4.2.2 in a new Durable Object for
each case: `items` with `item_bytes` and no `idx_items_scan`, and `latest_items` with `item_bytes`. Each case inserts
20,000 rows of one 8-byte hash key in sort-key order. The value is the increase of `sql.databaseSize` divided by the
row count. An indexed `items` row has a TTL. An indexed `latest_items` row is a tombstone, which has no data.

The cases are keys of 16, 58, 108, 208, and 408 bytes, and data of 0, 20, 50, 100, 200, 400, 700, and 1,000 bytes. The
bench prints each measured value and its model value. It calculates:

1. The coefficients: the multiples of 1/4 that make the largest relative error smallest, for rows with 20 to 200
   bytes of data and no overflow pages. A least-squares fit gives up to 12% error for the smallest rows, because their
   fixed bytes dominate.
2. `overflow_row_overhead`: the integer that makes the largest relative error smallest for the rows with overflow
   pages.

| Result | `items` | `latest_items` |
| --- | --- | --- |
| Model, not indexed | `1.25 * key + 76` | `1.25 * key + 67.25` |
| Model, indexed, more | `1.25 * key + 25` | `1.25 * key + 24.75` |
| Measured payload overhead | 37 to 39 bytes, 41 with a TTL | 28 bytes |
| `row_payload_overhead` of the model | 40 | 29 |
| `overflow_row_overhead` | 61 | 69 |

Error of the model, as (measured − model) / measured, smallest to largest:

| data | `items` | `items`, indexed | `latest_items` | `latest_items`, tombstone |
| --- | --- | --- | --- | --- |
| 0 | −10.5% to +3.8% | −6.9% to +1.1% | −13.5% to +3.7% | −2.2% to +2.2% |
| 20 | −7.4% to +7.5% | −4.3% to +3.7% | −8.4% to +8.3% | |
| 50 | −3.7% to +4.8% | −1.9% to +1.8% | −4.2% to +5.6% | |
| 100 | +0.3% to +5.7% | −1.2% to +3.8% | +1.1% to +6.9% | |
| 200 | +0.1% to +6.6% | −1.3% to +4.3% | +0.8% to +8.5% | |
| 400 | +1.3% to +18.0% | −0.1% to +16.3% | +2.2% to +11.1% | |
| 700 | 0% to +13.0% | −0.8% to +10.5% | −0.2% to +13.7% | |
| 1,000 | 0% to +0.4% | −0.4% to +0.3% | −0.2% to +0.2% | |

- A row with 1,000 bytes of data has overflow pages at each key. A row with 700 bytes of data has them only with a
  key of 408 bytes.
- The item id has 2 bytes for ids up to 2,000. The measured payload overhead of `items` is 38 bytes for ids from 40,000
  and 39 bytes for ids from 10,000,000.
- Random insert order: key 58 with 100 bytes of data gave 319 to 322 bytes (sorted 315), and key 208 gave 705 to 710
  (sorted 683): at most 3.9% more.
- The measured values of two runs were the same.
- Rows written for one insert, from the `rowsWritten` of SQLite: `items` 2, or 3 with a TTL; with a scan index 3, or 4.
  `latest_items` 2 for a live row and 3 for a tombstone.

### A.2 The rowid layout

Date: 2026-10-30. These values come from the earlier rowid layout: a rowid `items` table, the UNIQUE key index, and a
covering index `(hk, sk, item_bytes, ttl_epoch_utc_seconds)`. That layout stored the key 3 times, like the current
layout. The values show the size of the key-copy error. They do not give the coefficients of section 4.2.3.

Each case inserts 20,000 rows of one 8-byte hash key, in sort-key order, into a new Durable Object. The value is the
increase of `sql.databaseSize` divided by the row count. The runtime is local workerd under
`@cloudflare/vitest-plugin`, and the page size is 4,096 bytes. `key` is the key bytes. The model column uses the
coefficients that were fitted for that layout: `2.5 * key + 50`, and `1.25 * key + 20` more with a TTL.

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

- Random insert order changed the result by at most 1.7%.
- Rows of 1 KB to 1.5 KB fill a 4,096-byte page badly, so the model is too low for them.
- The formula of that time, `item + 108`, gave 616 bytes for `key = 408, data = 100`, where the file grew by 1,610
  bytes.

### A.3 `item_stats` rows

Date: 2026-11-05, with the method of appendix A.1, on the schema of section 4.2.4, 20,000 rows for each case.

| Row kind | Bounds | Bytes for each row |
| --- | --- | --- |
| First bucket, with its promotion index entry | `(x'', x'')`, 20,000 routingHashes | 65.7 |
| Range bucket, not in the promotion index | an 8-byte hash key and a 16-byte sort key, one routingHash | 69.4 |

A first bucket stores an 8-byte routingHash and two empty bounds, so its size does not depend on the hash key.

### A.4 Statement costs

Date: 2026-11-05. Local workerd, AMD Ryzen 9 7940HS, `test/storage-bench/item-size-model.workerd-bench.ts`. The
clustered tables of section 4.2.2. One range-leaf slice of 300,000 items: an 8-byte hash key, a 16-byte or a 100-byte
sort key, 20 bytes of data, and a TTL on one item in three. Its buckets hold `K = 2,048` items each, as an import
builds them: 147 buckets.

Each value is the wall time of a `runInDurableObject` call minus an empty call, the median of 3 calls, divided by the
repetitions in the call. The clock of a Worker does not advance during synchronous code, so the test side measures
the time. The rows read and written come from `rowsRead` and `rowsWritten` of SQLite. A JavaScript row read includes
the estimate of the item, with `overflow_bytes`.

| Operation | sk 16 bytes | sk 100 bytes | Rows read | Rows written |
| --- | --- | --- | --- | --- |
| JavaScript iteration of primary-key rows | 1.77 µs / row | 1.89 µs / row | 1 / row | 0 |
| SQL `COUNT(*)` over the primary key | 0.14 µs / entry | 0.19 µs / entry | 1 / entry | 0 |
| Split, `K = 2,048` | 1.45 ms | 1.55 ms | 6,150 | 2 |
| Split, `K = 4,096` | 2.75 ms | 3.40 ms | 12,294 | 2 |
| Split, `K = 8,192` | 5.50 ms | 7.00 ms | 24,582 | 2 |
| Split, `K = 16,384` | 10.4 ms | 18.4 ms | 49,158 | 2 |
| Refine scan of `2K` rows, `K = 2,048 / 4,096 / 8,192 / 16,384` | 7.45 / 14.7 / 32.4 / 59.5 ms | 7.0 / 15.1 / 28.6 / 57.0 ms | `2K + 2` | 0 |
| Bucket seek update, 147 buckets | 8.2 µs | 13.4 µs | 3 | 1 |
| Old-facts read | 11.2 µs | 14.8 µs | 1 | 0 |
| Planner bucket path, `N = 4`, 147 buckets | 12 ms | 12 ms | 6,297 | 0 |

On 100,000 first buckets of hash leaves:

| Operation | Time | Rows read | Rows written |
| --- | --- | --- | --- |
| Promotion index read of 6 rows | 0.14 to 0.16 ms | 7 | 0 |
| First-bucket upsert, one hot routingHash | 24 to 29 µs | 1 | 2 |
| First-bucket upsert, spread over all routingHashes | 52 to 74 µs | 1 | 2 |

- The split is the `OFFSET` seek, the two aggregates with `overflow_bytes`, and the two bucket writes of section 4.2.6.
- The time ranges come from two runs. The index-read times vary between runs more than the other values.
- `EXPLAIN QUERY PLAN` shows a primary-key search for the range scan, the count, the split seek, the old-facts read, the
  seek of the last item before a bound, and the `item_stats` cleanup. The promotion read is
  `SEARCH m_item_stats USING INDEX m_item_stats_promotion (rank_bytes>?)`.
- The bench measured the index read on an earlier column, `rank_bytes`, with the same partial index and the same plan
  shape. TODO: measure the read on `est_x4` with `LIMIT 5`.

The current layout reads its sizes from the covering index `idx_items_scan`. On 2026-10-30, a JavaScript iteration of
that index cost 1.17 µs for each row with a 16-byte sort key, and 1.30 µs with a 100-byte sort key.

Measurement of the `planRangeSplit` of that time (2026-10-04, same machine, 8-byte hash key, 16-byte sort key, 20 bytes
of data, 146 MB of database for 1M rows):

| Rows | N | Planner time | Full JavaScript iteration | Full `COUNT(*)` in SQL |
| --- | --- | --- | --- | --- |
| 1M | 2 | 0.49 s | 0.89 s | 0.05 s |
| 1M | 4 | 0.73 s | | |
| 1M | 16 | 0.92 s | | |
| 2M | 2 | 0.96 s | 1.78 s | 0.07 s |
| 2M | 4 | 1.43 s | | |
| 2M | 16 | 1.80 s | | |

### A.5 Overflow pages

Date: 2026-11-05, with the method of appendix A.1, on `items`, an 8-byte hash key and a 16-byte sort key (24 key
bytes), no TTL, 2,000 rows for each case. The column `overflow pages` is `overflow_bytes` with
`row_payload_overhead = 37`, the value measured for this case, and with no `overflow_row_overhead`.

| data | `item_bytes` | Measured | overflow pages | Measured − `item_bytes` − overflow pages |
| --- | --- | --- | --- | --- |
| 800 | 824 | 1,077 | 0 | 253 |
| 900 | 924 | 1,077 | 0 | 153 |
| 950 | 974 | 4,727 | 3,574 | 179 |
| 1,000 | 1,024 | 4,727 | 3,524 | 179 |
| 1,500 | 1,524 | 4,727 | 3,024 | 179 |
| 2,000 | 2,024 | 4,727 | 2,524 | 179 |
| 3,000 | 3,024 | 4,727 | 1,524 | 179 |
| 4,100 | 4,124 | 4,727 | 424 | 179 |
| 4,600 | 4,624 | 4,731 | 4 | 103 |
| 5,000 | 5,024 | 5,173 | 4 | 145 |
| 8,000 | 8,024 | 8,823 | 620 | 179 |

- The largest data with no overflow page is 941 bytes: a payload of 941 + 24 + 37 = 1,002 bytes.
- From 950 to 8,000 bytes, the last column is 179 bytes at each size, so the unused overflow space of the function is
  exact. The other terms of the model give 1.25 * 24 + 76 = 106 bytes for this row. The 73 bytes more are space on the
  tree page. Over all the keys of appendix A.1, the best fixed value is 61 for `items`.
- Rows of 800 to 900 bytes fit 3 to a page, so they waste page space that the model does not count (section 2.2).
- At 4,600 and 5,000 bytes, the part on the tree page is 569 and 969 bytes, so fewer rows fit on a tree page. The
  model does not count that space.

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

## Appendix C. Walkthrough: the promotion lookup

The examples use the coefficients of `items` (section 4.2.3): `key_index_factor` 1.25 and `base_overhead` 76. The
routingHashes have no TTL. Only X has overflow pages, and they add about 0.4% to it. The cap is 1 GB, so `T` is 250 MB.

**The order by `est_x4` is the estimate order.**

```text
routingHash X: 100 items of 1 MB
  item_bytes 100 MB, estimate about 100 MB,                          est_x4 about 400 M
routingHash Y: 500,000 items, 100-byte keys and 20 bytes of data
  item_bytes  60 MB, estimate 60 + 1.25 * 50 + 76 * 0.5 = 160.5 MB,  est_x4 642 M
routingHash Z: 1,000,000 items of 1-byte keys and no data
  item_bytes   1 MB, estimate  1 + 1.25 * 1 + 76 * 1 = 78.25 MB,     est_x4 313 M
```

By `item_bytes`, Z is 100 times smaller than X. By `est_x4`, Z is in its estimate place: the read order is Y, X, Z.
The fixed bytes of each item and the overflow pages are terms of the estimate, so small items and large items get the
same treatment. None of the three is at or above `T`, so the lookup reads one index row and returns no candidate.

**A coefficient change.** A later release changes `base_overhead` from 76 to 152.

```text
Z after the release, with no write:     est_x4 313 M  (old coefficients)
Z new estimate:                          1 + 1.25 + 152 = 154.25 MB, est_x4 617 M
```

Both values are below `4 * T` = 1,000 M, so the result is the same. Suppose instead that Z has 2,000,000 items. Its
old value is 626 M and its new estimate is 308.5 MB, at or above `T`. Then:

1. If Z gets a write, the statement sets `est_x4` to 1,234 M. The write-path check reports Z, and the lookup returns
   it.
2. If Z gets no write and the leaf goes over its cap, the lookup does not return Z. The leaf makes a hash split. The
   import of the child that owns Z writes `est_x4` 1,234 M. The write-path check of a later write, or the next decision
   of the child over its cap, promotes Z. Z is copied one more time.

## Appendix D. Walkthrough: why the bucket size counts items

| What a bucket affects | Grows with | Reason |
| --- | --- | --- |
| The split | items | It reads keys and small columns. It does not select item data |
| The refine scan | items | JavaScript reads each row, at about 1.8 µs for each row |
| The recount after a defect | items | The same scan as the split |
| The number of bucket rows | items | It is the item count of the slice divided by the bucket size |
| The precision of a boundary | bytes, only without a refine scan | The refine scan selects the exact gap |

A byte target does not bound these costs. A 16 MiB bucket holds about 115,000 items of 146 bytes, a refine scan of
about 0.2 s, or 40 items of 400 KB, a scan of less than 1 ms. A long key makes each entry larger, but it changes
the cost of a row by less than 2 times (1.77 µs and 1.89 µs in appendix A.4).

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
rows. That is about 24 ms. The streaming scan reads about 750,000 rows, about 1.3 s.

Example, `K = 2,048`, `C = 300,000`, `N = 255`: at most 293 buckets, and 254 * 4,096 + 510 is more than `C`. Thus the
scan reads at most all 300,000 rows plus at most `2N` repair rows. The slice is small, so the planner reads it all,
in about 0.53 s.

### E.3 The planner at each cap

Small items of about 150 bytes, 1.77 µs for each row read, and 7.45 ms for each refine scan at `K = 2,048`. These
costs come from appendix A.4, with a 16-byte sort key.

| Cap | Items | Streaming scan, `N = 4` | Streaming scan, `N = 255` | Buckets, `N = 4` | Buckets, `N = 255` |
| --- | --- | --- | --- | --- | --- |
| 500 MiB | 3.5M | 2.6M rows, 4.6 s | 3.5M rows, 6.2 s | 3,400 + 12,300 rows, 28 ms | 1.04M rows, 1.90 s |
| 2 GiB | 14.3M | 10.7M rows, 19.0 s | 14.3M rows, 25.3 s | 14,000 + 12,300 rows, 47 ms | 1.05M rows, 1.92 s |
| 5 GiB | 35.8M | 26.8M rows, 47.5 s | 35.8M rows, 63.4 s | 35,000 + 12,300 rows, 84 ms | 1.08M rows, 1.95 s |
| 8 GiB | 57.3M | 42.9M rows, 76.0 s | 57.3M rows, 101 s | 56,000 + 12,300 rows, 121 ms | 1.10M rows, 1.99 s |

The streaming scan passes the 30 s CPU limit at about 3.2 GiB at `N = 4`. With buckets, only the bucket rows grow
with the cap, at one row for each `K` to `2K` items. The collision repair of section 4.2.9 adds at most `2N` rows,
below the precision of the table.

## Appendix F. Walkthrough: the choice of K

The worst case is a leaf at the 10 GB limit with about 70M small items. The values use the measured costs of
appendix A.4 for a 100-byte sort key: 1.89 µs for each row read.

| K | Split | Rows / split | Refine | Buckets at 10 GB | Planner, `N = 4` | Planner, `N = 255` | Splits / 1M inserts |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **2,048** | 1.45–1.55 ms | 6,150 | 7.0 ms | ≤ 68,000 (129 ms) | about 150 ms | about 1.9 s | 488 |
| 4,096 | 2.75–3.40 ms | 12,294 | 15.1 ms | ≤ 34,000 (65 ms) | about 110 ms | about 3.9 s | 244 |
| 8,192 | 5.50–7.00 ms | 24,582 | 28.6 ms | ≤ 17,000 (32 ms) | about 118 ms | about 7.3 s | 122 |
| 16,384 | 10.4–18.4 ms | 49,158 | 57.0 ms | ≤ 8,500 (16 ms) | about 187 ms | about 14.5 s | 61 |

The target range of leaves is 500 MB to 2 GB (section 2.1). The two closest values at each cap:

| Cap | `K = 2,048`, `N = 4` | `K = 4,096`, `N = 4` | `K = 2,048`, `N = 255` | `K = 4,096`, `N = 255` |
| --- | --- | --- | --- | --- |
| 500 MiB | **about 27 ms** | about 49 ms | **about 1.78 s** | about 3.84 s |
| 2 GiB | **about 47 ms** | about 59 ms | **about 1.80 s** | about 3.85 s |
| 5 GiB | about 87 ms | **about 78 ms** | **about 1.84 s** | about 3.87 s |
| 10 GB | about 150 ms | **about 110 ms** | **about 1.91 s** | about 3.90 s |

- The planner reads all bucket rows and up to `2K` rows for each target. Below about 3.7 GiB, the shorter refine scans
  of `K = 2,048` save more than its extra bucket rows cost. Above it, `K = 4,096` is faster at `N = 4` by at most
  about 40 ms.
- The total split work is about 3 item reads for each insert, for each `K`. A smaller `K` gives more, shorter
  splits.
- At `K = 2,048`, each split blocks the partition for about 1.5 ms, and the planner stays below about 0.15 s at
  `N = 4` at each cap. The bucket table of a 10 GB leaf holds at most about 68,000 rows, about 4.7 MB.
- `K = 1,024` is not measured. A linear extrapolation gives about 63 ms at 2 GiB and about 270 ms at 10 GB at `N = 4`,
  both worse than `K = 2,048`.
- An import builds its buckets with no item read, so `K` does not change the import cost.
- Larger items make every value in this appendix smaller, because a leaf then holds fewer items (section 4.2.14).

## Appendix G. Walkthrough: buckets in storage order

A range leaf owns the slice `[(A, x''), null)` of the routingHash `b7`. Two hash keys, `A` and `B`, have the
routingHash `b7`, and `A < B`. Use `K = 2` for the example, so a bucket splits above 4 items.

**Import.** The pages arrive in storage order: `(A, 1) (A, 2) (A, 3) (B, 1) (B, 2)`.

```text
item_stats (routing_hash, lower_hk, lower_sk)      items in the primary key
(b7, x'', x'')  item_count 2  ───────────────────► (b7, A, 1) (b7, A, 2)
(b7, A,   3)    item_count 2  ───────────────────► (b7, A, 3) (b7, B, 1)
(b7, B,   2)    item_count 1  ───────────────────► (b7, B, 2)
```

The import reads no item. When a bucket holds `K = 2` items, the next item starts a new bucket at its own key. The
bucket `(b7, A, 3)` holds items of both hash keys.

**Insert.** A write inserts `(B, 0)`. The bucket rule selects the greatest bound at or below `(B, 0)`: that is
`(b7, A, 3)`. Its count becomes 3. It is not above `2K = 4`, so no split happens.

**Delete.** A write deletes `(B, 2)`. The last bucket reaches 0 items. Its pair with `(b7, A, 3)` holds 3 items, which
is not fewer than `K`, so no merge happens. The helper deletes the empty row, which is not the first bucket.

**Promotion cleanup.** On the hash leaf that held `b7` before the promotion, `b7` had one bucket `(b7, x'', x'')`. The
cleanup deletes it with `DELETE FROM item_stats WHERE routing_hash = b7`, one key range.
