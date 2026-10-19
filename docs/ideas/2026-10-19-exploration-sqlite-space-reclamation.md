# Exploration — SQLite space reclamation after random deletes in a partition

**State:** Exploration
**Date:** 2026-10-19
**Author:** Lambros Petrou

## Table of Contents

1. [Questions and Method](#1-questions-and-method)
2. [How `workerd` SQLite Reports `databaseSize`](#2-how-workerd-sqlite-reports-databasesize)
3. [Why the `items` Table and the Key Indexes Behave Differently](#3-why-the-items-table-and-the-key-indexes-behave-differently)
4. [Measurements Across Five Storage Sizes](#4-measurements-across-five-storage-sizes)
5. [Measurements Across Item Payload Sizes](#5-measurements-across-item-payload-sizes)
6. [Decomposition: Table B-tree Against Key-Ordered B-tree](#6-decomposition-table-b-tree-against-key-ordered-b-tree)
7. [Summary of Findings](#7-summary-of-findings)

## 1. Questions and Method

This exploration answers two questions for a `PartitionDO` that holds items with uniformly random hash keys:

1. When the partition deletes 3/4 (75%) of its items at random, what happens to `state.storage.sql.databaseSize`?
2. When later writes insert new items with uniformly random hash keys, do those writes fill the holes inside the pages that stayed allocated?

The experiment ran in `vitest` on `workerd` (`@cloudflare/vitest-plugin`) against `PartitionStore` on real Durable Object SQLite storage. For each target size (`10 MiB`, `25 MiB`, `50 MiB`, `75 MiB`, and `100 MiB`) and each payload size (`100 B` to `16 KiB`), the test:

1. Inserted items with uniformly random 35-byte hash keys (`hk-<32 hex chars>`) and sort key `sk0` through `PartitionStore.upsertItem` until `databaseSize` reached the target size.
2. Shuffled the inserted keys uniformly at random and deleted 75% of them through `PartitionStore.deleteItem`.
3. Inserted new items with new uniformly random hash keys in three steps (+25%, +50%, and +75% of the initial row count) until the partition held 100% of its initial row count again.
4. Repeated the 75% random delete and 75% random refill for two more cycles (Cycles 2 and 3) to check whether repeated churn grows the database without bound.

## 2. How `workerd` SQLite Reports `databaseSize`

In `workerd` Durable Object storage:

- `PRAGMA page_size` is `4096` bytes (`4 KiB`).
- `PRAGMA page_count`, `PRAGMA freelist_count`, and `PRAGMA auto_vacuum` are blocked with `SQLITE_AUTH`.
- `state.storage.sql.databaseSize` reports the active bytes only: `(page_count - freelist_count) * 4096`.

A probe that inserted 1,000 rows of `4,000 B` (`databaseSize = 4,296,704 B`, `1,049` pages) and then deleted all 1,000 rows saw `databaseSize` drop immediately to `114,688 B` (`28` pages, the empty schema baseline).

Therefore:

- When a `DELETE` frees an entire `4 KiB` page, `databaseSize` decreases by `4,096 B` right away.
- When a `DELETE` leaves at least one live entry on a `4 KiB` page, that page stays allocated and counts in full (`4,096 B`) toward `databaseSize`.

## 3. Why the `items` Table and the Key Indexes Behave Differently

Each committed item in `PartitionStore` writes to two kinds of B-trees:

1. **The `items` table B-tree (`item_id INTEGER PRIMARY KEY`)**:
   - `items` is a rowid table where `item_id` is the `INTEGER PRIMARY KEY`.
   - `PartitionStore.upsertItem` inserts new rows without an explicit `item_id`, so SQLite assigns `MAX(item_id) + 1`.
   - Every new row goes to the **right-most leaf page** of the `items` table B-tree.
   - Because new rows always have a higher `item_id` than all existing rows, **later inserts of new keys never land inside older partially-full `items` table pages**. Every batch of new inserts allocates new pages (or reuses completely empty freelist pages) at the right edge of the `items` B-tree.
   - However, during a `DELETE`, SQLite runs its standard B-tree balance algorithm (`balance_nonroot`). When adjacent sibling leaf pages in `items` drop below the underflow threshold, SQLite merges their remaining cells into fewer pages and frees the empty sibling pages immediately.

2. **The `hk`-ordered B-trees (`sqlite_autoindex_items_1`, `idx_items_scan`, `key_size_estimates`, and `key_size_estimates_by_bytes`)**:
   - Entries in these four B-trees are ordered by `hk` (or `est_bytes`).
   - When `PartitionStore.deleteItem` removes 75% of the keys at random, SQLite merges underflowing sibling index pages immediately. Notice also that `deleteItem` deletes the `key_size_estimates` row when a hash key has no items left.
   - When later writes insert new uniformly random hash keys, those entries land across the existing index leaf pages and reuse the free space inside those pages before splitting them.

## 4. Measurements Across Five Storage Sizes

The behavior is scale-invariant across `10 MiB`, `25 MiB`, `50 MiB`, `75 MiB`, and `100 MiB`.

### 4.1 Small items (`200 B` payload, ~13 rows per `4 KiB` table leaf page)

| Target Size | Initial Rows | Peak Size | After 75% Delete (25% rows left) | +25% New (50% rows) | +50% New (75% rows) | +75% New (Cycle 1: 100% rows) | Cycle 2 (Del 75% -> Refill) | Cycle 3 (Del 75% -> Refill) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **10 MiB** | 22,000 | 10.16 MiB | **4.73 MiB (46.5%)** | 6.32 MiB | 8.79 MiB | **11.25 MiB (110.7%)** | 4.73 -> 11.32 MiB (111.4%) | 4.73 -> 11.31 MiB (111.3%) |
| **25 MiB** | 55,000 | 25.27 MiB | **11.75 MiB (46.5%)** | 15.72 MiB | 21.93 MiB | **28.21 MiB (111.7%)** | 11.74 -> 28.20 MiB (111.6%) | 11.73 -> 28.19 MiB (111.6%) |
| **50 MiB** | 109,000 | 50.13 MiB | **23.13 MiB (46.1%)** | 31.11 MiB | 43.32 MiB | **55.68 MiB (111.1%)** | 22.98 -> 55.52 MiB (110.7%) | 23.28 -> 55.89 MiB (111.5%) |
| **75 MiB** | 164,000 | 75.36 MiB | **34.70 MiB (46.0%)** | 46.60 MiB | 65.10 MiB | **83.80 MiB (111.2%)** | 34.80 -> 83.78 MiB (111.2%) | 34.68 -> 83.90 MiB (111.3%) |
| **100 MiB** | 219,000 | 100.45 MiB | **46.36 MiB (46.2%)** | 62.20 MiB | 86.78 MiB | **111.73 MiB (111.2%)** | 46.45 -> 111.86 MiB (111.4%) | 46.55 -> 111.91 MiB (111.4%) |

At `100 MiB` (`200 B` payload):

- The initial sequential `item_id` inserts pack the `items` table leaf pages to ~95% occupancy.
- Deleting 75% of the rows at random triggers SQLite sibling page merges, but the merged pages settle at ~50%–55% occupancy instead of ~95%. Thus `databaseSize` drops to **46.2%** of peak (`46.36 MiB` for 25% of the rows) instead of 25%.
- During the refill back to 219,000 rows, `databaseSize` grows linearly from `46.36 MiB` to `111.73 MiB` (**111.2%** of the initial peak), because the 75% new rows append to the right edge of `items` at ~95% density while the surviving 25% rows from the first generation stay in their ~50%-full pages.
- In Cycles 2 and 3, `databaseSize` does **not** keep growing: it alternates between **46.5 MiB** after the 75% delete and **111.9 MiB (111.4%)** after the refill, even as `MAX(item_id)` reaches `711,747`. Each new delete wave removes 75% of the older surviving rows, which causes those older `items` pages to underflow, merge, and return to the freelist.

### 4.2 Medium and large items (`2,000 B` payload, 1–2 rows per `4 KiB` table leaf page)

| Target Size | Initial Rows | Peak Size | After 75% Delete (25% rows left) | +25% New (50% rows) | +50% New (75% rows) | +75% New (Cycle 1: 100% rows) | Cycle 2 (Del 75% -> Refill) | Cycle 3 (Del 75% -> Refill) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **10 MiB** | 3,000 | 12.44 MiB | **3.30 MiB (26.5%)** | 6.26 MiB | 9.35 MiB | **12.44 MiB (100.0%)** | 3.33 -> 12.43 MiB (99.9%) | 3.34 -> 12.44 MiB (100.0%) |
| **25 MiB** | 7,000 | 28.88 MiB | **7.62 MiB (26.4%)** | 14.50 MiB | 21.68 MiB | **28.92 MiB (100.1%)** | 7.61 -> 28.89 MiB (100.0%) | 7.59 -> 28.87 MiB (100.0%) |
| **50 MiB** | 13,000 | 53.55 MiB | **14.06 MiB (26.3%)** | 26.88 MiB | 40.26 MiB | **53.60 MiB (100.1%)** | 14.08 -> 53.61 MiB (100.1%) | 14.07 -> 53.57 MiB (100.0%) |
| **75 MiB** | 19,000 | 78.19 MiB | **20.47 MiB (26.2%)** | 39.23 MiB | 58.74 MiB | **78.25 MiB (100.1%)** | 20.51 -> 78.32 MiB (100.2%) | 20.48 -> 78.30 MiB (100.1%) |
| **100 MiB** | 25,000 | 102.92 MiB | **26.97 MiB (26.2%)** | 51.60 MiB | 77.23 MiB | **102.98 MiB (100.1%)** | 26.97 -> 102.96 MiB (100.0%) | 26.94 -> 102.96 MiB (100.0%) |

When each row is `2,000 B`, each row takes its own `4 KiB` table page. Deleting 75% of the rows frees 75% of the table pages immediately, dropping `databaseSize` to **26.2%** of peak. Refilling 75% new rows brings `databaseSize` back to **100.0%** of peak.

## 5. Measurements Across Item Payload Sizes

Measured at `~20 MiB` initial database size across 6 payload sizes:

| Payload Size | Initial Rows | Peak Size | After 75% Delete (25% rows left) | +25% New (50% rows) | +50% New (75% rows) | +75% New (100% rows) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **100 B** | 55,000 | 20.04 MiB (5,130p) | **9.71 MiB (48.4%)** | 14.61 MiB | 19.54 MiB | **24.48 MiB (122.2%)** |
| **500 B** | 27,000 | 20.79 MiB (5,322p) | **9.25 MiB (44.5%)** | 14.32 MiB | 19.48 MiB | **24.66 MiB (118.6%)** |
| **1,000 B** | 14,000 | 21.27 MiB (5,446p) | **7.14 MiB (33.5%)** | 12.37 MiB | 17.64 MiB | **22.91 MiB (107.7%)** |
| **2,000 B** | 5,000 | 20.69 MiB (5,297p) | **5.49 MiB (26.5%)** | 10.61 MiB | 15.75 MiB | **20.89 MiB (100.9%)** |
| **4,000 B** | 5,240 | 21.71 MiB (5,558p) | **5.75 MiB (26.5%)** | 11.11 MiB | 16.50 MiB | **21.91 MiB (100.9%)** |
| **16,000 B** | 1,310 | 20.86 MiB (5,340p) | **5.36 MiB (25.7%)** | 10.52 MiB | 15.69 MiB | **20.89 MiB (100.1%)** |

Fine-grained refill trace at `~10 MiB` (adding 1/12 of the initial row count per step after a 75% delete):

- **`200 B` payload (`10.02 MiB` peak)**:
  `4.65 MiB (0%) -> 5.43 MiB (+8%) -> 6.27 MiB (+17%) -> 7.08 MiB (+25%) -> 7.92 MiB (+33%) -> 8.76 MiB (+42%) -> 9.57 MiB (+50%) -> 10.38 MiB (+58%) -> 11.24 MiB (+67%) -> 12.11 MiB (+75%)`
- **`2,000 B` payload (`10.79 MiB` peak)**:
  `2.89 MiB (0%) -> 3.77 MiB (+8%) -> 4.65 MiB (+17%) -> 5.54 MiB (+25%) -> 6.43 MiB (+33%) -> 7.31 MiB (+42%) -> 8.20 MiB (+50%) -> 9.08 MiB (+58%) -> 9.97 MiB (+66%) -> 10.90 MiB (+75%)`

In both cases, `databaseSize` increases on the very first refill batch (`+8%`). There is no flat plateau during refill because `databaseSize` already subtracted the freed pages at `DELETE` time, and the new `items` rows append to new pages at `MAX(item_id) + 1`.

## 6. Decomposition: Table B-tree Against Key-Ordered B-tree

To isolate the `items` table B-tree from the `hk`-ordered index B-trees, two micro-benchmarks ran on `40,000` rows in `workerd`:

1. **Rowid table only (`CREATE TABLE raw_items (item_id INTEGER PRIMARY KEY, data TEXT NOT NULL) STRICT`, `250 B` payload, no secondary index)**:
   - Peak (40,000 rows): `10.55 MiB` (`0.11 MiB` baseline).
   - After deleting 75% at random: **`4.87 MiB` (45.6% of net peak)**.
   - After inserting 30,000 new rows (back to 40,000 rows): **`12.70 MiB` (120.5% of net peak)**.
   - Because new rows append at `MAX(item_id) + 1`, none of the holes in the surviving `4.87 MiB` of old pages are reused by the 30,000 new rows. Those 30,000 new rows require `0.75 * 10.44 MiB = 7.83 MiB` of fresh right-edge pages (`4.87 + 7.83 = 12.70 MiB`).

2. **Random-keyed B-tree only (`CREATE TABLE idx_only (hk BLOB NOT NULL PRIMARY KEY, v INTEGER NOT NULL) WITHOUT ROWID, STRICT`)**:
   - Peak (40,000 random keys): `1.86 MiB` (`0.11 MiB` baseline).
   - After deleting 75% at random: **`0.95 MiB` (47.7% of net peak)**.
   - After inserting 30,000 new random keys (back to 40,000 keys): **`2.29 MiB` (124.8% of net peak)**.
   - When a key-ordered B-tree drops to 25% of its keys and SQLite merges underflowing sibling pages, the merged pages sit near 50% occupancy. When random inserts start again, those ~50%-full pages accept some new keys, and the pages that reach 100% split into two 50%-full pages before the rest fill up, shifting the average page occupancy slightly downward until steady state.

## 7. Summary of Findings

1. **Deleting 75% of uniformly random keys reclaims between 52% and 74% of `databaseSize` immediately**:
   - For items `>= 2 KiB`, `databaseSize` drops to **~26%** of peak (74% of space reclaimed).
   - For small items (`100 B`–`500 B`), SQLite merges underflowing sibling pages during the `DELETE`, and the surviving pages hold ~50%–55% occupancy instead of their initial ~95% append occupancy. Thus `databaseSize` drops to **~45%–48%** of peak (52%–55% of space reclaimed).
2. **Subsequent inserts of new keys do not fill holes in the `items` table pages**:
   - New items get `item_id = MAX(item_id) + 1` and append to the right edge of the `items` B-tree. Only the `hk` indexes receive inserts into existing interior/leaf pages.
   - As a result, `databaseSize` grows linearly from the first refill write onward.
3. **Repeated delete-and-insert churn reaches a bounded steady state (~111%–122% of the initial fresh-append size)**:
   - Repeating the 75% random delete and 75% random refill across multiple cycles does **not** leak storage. Every delete wave removes older surviving rows from earlier cycles, allowing SQLite to merge and free those older `item_id` pages.
