# RFC — Integer link id for the `items` table

**State:** Implemented
**Date:** 2026-09-13

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)
- [8. Appendix: measurements](#8-appendix-measurements)

## 1. Overview and context

Each partition stores its items in the SQLite table `items`. The item key is `(hk, sk)`. The hash key can be up to
1024 bytes (`MAX_HASH_KEY_BYTES`), and the sort key can be up to 512 bytes (`MAX_SORT_KEY_BYTES`).

Future features can need more tables that hold rows for an item. Examples are metadata that changes often, or a
separate table for large `data` values. Each such table needs a key that links its rows to the item row.

The item key is a bad link key. A row in another table must repeat up to 1536 key bytes, and a one-to-many table
repeats them in each row. An integer is 1 to 9 bytes.

The change must happen before public testing starts:

- SQLite cannot add an `INTEGER PRIMARY KEY` to an existing table. `ALTER TABLE ADD COLUMN` does not accept
  `PRIMARY KEY`, and it adds the column after `data`.
- After public testing starts, the only way to add the column is a table rebuild in each Durable Object.
- Before public testing, the project edits SQL migrations in place.

### 1.1 Terms

| Term | Meaning |
| --- | --- |
| Link id | The value of the `items.item_id` column. |
| Parent partition | The partition that serves items to a child after a hash split, a range split, or a promotion. |
| Child partition | The partition that copies its items from one parent partition. |
| Lineage | A parent partition and all the partitions that copy items from it, directly or through other children. |

## 2. Goals and requirements

### 2.1 In scope

- The `items` table has the column `item_id INTEGER PRIMARY KEY`.
- In one partition, each item has a different link id.
- Migration copies the link id from the parent to the child without change.
- Migration does not remap ids, and the copy order does not matter.
- The link id does not leave `PartitionStore`. The only exception is the migration copy, which moves the value
  and does not read it.

### 2.2 Requirements

- The link id must cost at most a few bytes for each row.
- The link id must not make inserts slower or table pages less full.
- When migration ingests an item whose link id another item already holds, the insert must fail with an error.
  It must not drop the item.
- The link id must not appear in the public API, in cursors, in `pending_transactions`, in `tc_items`, or in
  transaction RPCs.
- The `queryItems` RPC must not carry the link id.

### 2.3 Out of scope

- A table that uses the link id. This change only adds the key.
- A link id that is unique across all partitions. No operation merges partitions today.
- A guarantee that SQLite never gives a deleted link id to a new item. Section 4.2.5 gives the delete rule that
  makes reuse safe.
- A separate table for `data`. For items of a few KB, a separate table makes projection queries 1.7x to 1.8x slower
  and upserts 1.5x to 1.6x slower (Appendix 8.2). A join also reads 2 billed rows for each item.

## 3. Milestones

### 3.1 Link id (implemented)

- The migration that creates `items` adds `item_id INTEGER PRIMARY KEY` and replaces `PRIMARY KEY (hk, sk)` with
  `UNIQUE (hk, sk)`.
- The migration readers return the link id, and `insertItemIfAbsent` writes it.
- The types `StoredItem`, `MigratedItem`, and `ItemLinkId` keep the link id inside the store.
- The tests cover the copy, the retry, the id conflict, and the query boundary.

## 4. Proposed solution

### 4.1 High-level overview

`items.item_id` is the rowid of the item row. SQLite gives a new item the value `MAX(item_id) + 1`.

When a child partition migrates, it copies each item row with its link id. A child copies only from its parent,
and the ids in the parent are unique. Thus, the ids in the child are also unique. After the copy, a new item in
the child gets an id that is higher than all copied ids.

A future table keys its rows by the link id. Migration copies those rows with the same link id. The child then has
the item rows and the linked rows with the same ids, and no step remaps them.

The link id is internal to `PartitionStore`. Queries, transactions, and the API do not see it. Migration moves it
as an opaque branded value.

```
Parent partition                                   Child partition
items                                              items
  item_id=7  (a, 1)  ── migrationGetItemsBatch ──▶  item_id=7  (a, 1)  copied without change
  item_id=9  (a, 2)  ────────────────────────────▶  item_id=9  (a, 2)
                                                    item_id=10 (a, 3)  new item: MAX(item_id) + 1
future_table                                       future_table
  item_id=7          ────────────────────────────▶  item_id=7          same link, no remap
```

### 4.2 Technical details

#### 4.2.1 Schema

```sql
CREATE TABLE IF NOT EXISTS items (
    item_id               INTEGER PRIMARY KEY,

    hk                    BLOB    NOT NULL,
    sk                    BLOB    NOT NULL DEFAULT x'',
    data_kind             INTEGER NOT NULL DEFAULT 0,
    v                     INTEGER NOT NULL,
    last_read_ts          INTEGER NOT NULL DEFAULT 0,
    last_write_ts         INTEGER NOT NULL DEFAULT 0,
    ttl_epoch_utc_seconds INTEGER,
    est_row_bytes         INTEGER NOT NULL,
    data                  ANY     NOT NULL,

    UNIQUE (hk, sk)
) STRICT;
```

- `item_id` is an alias of the rowid. SQLite stores the rowid in the cell, and stores NULL in the record for
  `item_id`. The cost is 1 byte in the record header.
- `UNIQUE (hk, sk)` creates the index `sqlite_autoindex_items_1`. The primary key created the same index name, so
  the `INDEXED BY` pin in `#storedEstRowBytes` and its query-plan test do not change.
- `hk` and `sk` keep an explicit `NOT NULL`, because a `UNIQUE` constraint does not make a column `NOT NULL`.
- `ON CONFLICT (hk, sk)` in `upsertItem` and `updateItemSingleShot` works with the `UNIQUE` constraint.
- An upsert that overwrites an item keeps its link id.
- A delete followed by a put creates a new item with a new link id.
- `deleteExpiredItems` selects its victims with `item_id IN (...)`. The value is the same as the rowid, so the query
  plan does not change.
- A table that links to an item must name its link column `item_id`. Thus, a join can use `USING (item_id)`, and a
  search for `item_id` finds each link.

#### 4.2.2 Why the link id is sequential

A sequential rowid adds each new row at the end of the table B-tree. A random or hashed rowid adds rows at random
positions. The B-tree then splits more leaf pages, and the pages contain more empty space.

With 40000 rows, a random rowid used 22% more space for 100-byte items and 26% more space for 1000-byte items.
Inserts were 2.9x slower (Appendix 8.1).

#### 4.2.3 Uniqueness in the lineage

The link id is unique in a partition because of these facts:

1. A child partition copies items from exactly one parent partition (`SplitMigration`).
2. The items of a child are a subset of the items of its parent. The ids in the parent are unique, so the copied
   ids are unique.
3. A migrating child refuses writes with 503 while its status is `migration_migrating`. No local insert takes an
   id while the copy runs.
4. After the copy, SQLite gives a new item `MAX(item_id) + 1`. This value is higher than all copied ids.

Two sibling partitions can give the same link id to different items. This is correct, because each link id is
used only in its own partition.

#### 4.2.4 Migration ingest

`insertItemIfAbsent` writes the copied link id:

```sql
INSERT INTO items (item_id, hk, sk, data_kind, ttl_epoch_utc_seconds, v, last_read_ts, last_write_ts, est_row_bytes, data)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, <estRowBytesExpr>, ?9)
ON CONFLICT (hk, sk) DO NOTHING
```

- When a retried batch serves an item that the child already has, the `(hk, sk)` conflict skips the row. The
  stored row stays.
- When a different item has a link id that the child already holds, the statement fails with
  `UNIQUE constraint failed: items.item_id`. The storage transaction rolls back.
- The statement must not use `INSERT OR IGNORE`. `OR IGNORE` also ignores a conflict on `item_id`, and the item then
  disappears without an error.

#### 4.2.5 Link id reuse and the delete rule

When SQLite deletes the row with the highest id, it can give that id to the next new row. The table does not use
`AUTOINCREMENT` (Section 5.3).

Thus, each operation that deletes an item must also delete the rows linked to its link id, in the same storage
transaction. This applies to `deleteItem`, `deleteExpiredItems`, and `deleteItemsBatchForHashKey`. A future table
that uses the link id must add its delete to these operations.

#### 4.2.6 VACUUM

The SQLite documentation says that VACUUM can change the rowids of a table without an explicit
`INTEGER PRIMARY KEY`. An explicit `INTEGER PRIMARY KEY` keeps its values. Thus, the link id stays stable if a
VACUUM runs.

#### 4.2.7 Type boundary

`partition-store.ts` defines three types:

| Type | Fields | Users |
| --- | --- | --- |
| `StoredItem` | The item fields without the link id. | `QueryScanRow`, `collectQueryPage`, `QueryItemsRpcResponse`, `estimateItemBytes` |
| `MigratedItem` | `StoredItem` and `item_id: ItemLinkId`. | `queryItemsPage`, `queryRangeItemsPage`, `insertItemIfAbsent`, `GetItemsBatchResult` |
| `ItemLinkId` | A branded `number`. | `MigratedItem` |

- `scanQueryPage` does not select `item_id`, and it builds each item without it.
- `readForTransactionLocal` uses `getItem`, which does not select `item_id`.
- `client/db.ts` maps each query item field by name.

`ItemLinkId` is `number & { readonly [ITEM_LINK_ID_BRAND]: true }`. `ITEM_LINK_ID_BRAND` is a `declare const` of
type `unique symbol`. It exists only for the type checker, and the file does not export it. `KeyBytes` uses the
same method.

- At run time, the link id is a plain number. Workers RPC sends the number, and the `MigratedItem` type at the
  receiver adds the brand again.
- TypeScript does not accept a plain `number` where the code needs an `ItemLinkId`.
- Only `PartitionStore` creates an `ItemLinkId`. The row type of a migration read gives the value the brand.
- Code outside `PartitionStore` must not cast a number to `ItemLinkId`. A test that takes the place of a parent
  store is the only exception.
- Code outside `PartitionStore` must not use the link id to identify an item. The brand cannot stop code that reads
  the value as a number, so code review must enforce this rule.

#### 4.2.8 Performance

- Storage: 1 byte for each row. With 20000 rows of 100-byte data, the table used 592 pages with and without the
  column (Appendix 8.1).
- Insert time: the same as an implicit rowid.
- Query plans: the point lookup still uses `sqlite_autoindex_items_1`. The metadata scans still use
  `idx_items_scan` as a covering index.

#### 4.2.9 Deployment and rollback

- The change edits the migration that creates `items` (`idMonotonicInc: 1`).
- A partition that ran the old migration keeps the old schema. Its migration ingest fails, because `items` has no
  `item_id` column. Delete the development databases that use the old schema.
- To roll back, revert the migration and the store code, and delete the databases again.

#### 4.2.10 Testing

`partition-store.test.ts`:

- `insertItemIfAbsent` keeps the copied link id.
- A retried key keeps the stored row.
- A different item with a held link id fails with `UNIQUE constraint failed: items.item_id`, and no row disappears.
- A local write after the copy gets an id higher than all copied ids.
- An overwrite keeps the link id.
- A `scanQueryPage` projection item has no `item_id` property.

`migration.test.ts`:

- The fake parent (`makeFakePeer`) gives each item the link id `position + 1`. Different items get different ids.
  An item gets the same id in each page, each retry, and each fake peer made from the same items.
- Tests get the link ids from `parentItems`. No test creates a link id.
- The hash-child migration test checks that each ingested item has the link id that the parent served.
- The idempotency test writes the local row from `parentItems`, so the local row and the served row have the same
  link id, as in production.

## 5. Alternative options

### 5.1 Hash of `(hk, sk)` with probing on a collision

Rejected:

- After one probe, the id depends on the insert history. The code must read the id from the table. Code that
  calculates the id from the keys is then wrong in the rare collision case.
- The probe path almost never runs, so tests do not cover it.
- A delete followed by a put of the same keys gets the same id. Old linked rows can then attach to the new item.
- A hashed rowid has the B-tree cost of a random rowid (Section 4.2.2).

### 5.2 Random 64-bit link id

Rejected. It copies without change and gives a new id to a new item. But it uses 22% to 26% more space for small
items, and inserts are 2.9x slower (Appendix 8.1).

### 5.3 `INTEGER PRIMARY KEY AUTOINCREMENT`

Not chosen now. `AUTOINCREMENT` prevents the reuse of a deleted id in one partition. Each insert then also writes
to `sqlite_sequence`, and the SQLite documentation recommends against it unless the application needs it. The
delete rule in Section 4.2.5 makes reuse safe.

For ids that are never reused in a lineage, a child must also copy the `sqlite_sequence` value of its parent.
Local insert time was 2.6 µs against 2.4 µs (Appendix 8.1). `TODO: measure` whether the `sqlite_sequence` update
counts as a billed row write on Durable Objects.

### 5.4 Implicit rowid without an `item_id` column

Rejected. VACUUM can change an implicit rowid (Section 4.2.6). A future table must not depend on a value that
SQLite does not promise to keep.

### 5.5 Remap the link id in the child

Rejected. The child must insert the items first, read the new ids, and then insert the linked rows with the new
ids. The copy then depends on the order of the rows, and each linked table needs a key lookup. A copy without
change has neither cost.

### 5.6 Key future tables by `(hk, sk)`

Possible, and it needs no change now. Rejected for this change because each linked row repeats up to 1536 key
bytes. The cost grows for long keys and for one-to-many tables. A delete followed by a put of the same keys also
reuses the key, so old linked rows can attach to the new item.

### 5.7 An `item_extra` JSONB column before `data`

Not part of this change. A JSONB column fixes a position before `data` for future metadata. But each change of its
size rewrites the full row, including the overflow pages of `data`. It suits metadata that the item writes
together with `data`. It does not suit metadata that changes often.

## 6. Frequently asked questions

### 6.1 Does the link id identify an item across partitions?

No. The link id is unique only in one partition. Two sibling partitions can use the same link id for different
items. The item key `(hk, sk)` identifies an item across partitions.

### 6.2 What happens if a future operation merges two partitions?

The link ids of the two partitions can collide. `insertItemIfAbsent` then fails with
`UNIQUE constraint failed: items.item_id`, and the merge stops. A merge operation must remap link ids, or it must use
ids that are unique across partitions.

### 6.3 Why does the query path use `StoredItem` and not `MigratedItem`?

TypeScript accepts an object with extra fields where it expects a smaller type. A separate `StoredItem` type
and a `scanQueryPage` that does not select `item_id` keep the link id out of the query RPC at run time, not only in the
types.

### 6.4 Why must `insertItemIfAbsent` name the conflict target?

`INSERT OR IGNORE` ignores all constraint conflicts, including a conflict on `item_id`. A copied item with a held
link id then disappears without an error. `ON CONFLICT (hk, sk) DO NOTHING` skips only a retried key.

## 7. References

References:

- `packages/fokosdb/src/shared/partition/partition-store.ts` — the `items` migration, `ItemLinkId`,
  `insertItemIfAbsent`, `queryItemsPage`, `queryRangeItemsPage`, `scanQueryPage`
- `packages/fokosdb/src/shared/partition/migration.ts` — `SplitMigration`
- `packages/fokosdb/src/shared/partition/partition-peer.ts` — `GetItemsBatchResult`
- `packages/fokosdb/src/shared/partition/partition-store.test.ts`
- `packages/fokosdb/src/shared/partition/migration.test.ts`
- `packages/fokosdb/src/shared/partition-topology/key-codec.ts` — `KeyBytes`, the same brand method
- [SQLite: VACUUM](https://www.sqlite.org/lang_vacuum.html)
- [SQLite: Autoincrement](https://www.sqlite.org/autoinc.html)
- [SQLite: UPSERT](https://www.sqlite.org/lang_upsert.html)
- [SQLite: ON CONFLICT clause](https://www.sqlite.org/lang_conflict.html)
- [SQLite: Database file format](https://www.sqlite.org/fileformat2.html)

## 8. Appendix: measurements

All measurements used SQLite 3.45.1 through Python `sqlite3` on Linux, with 4 KiB pages and WAL mode. The
absolute numbers on Durable Objects are different. The ratios are the useful part.

### 8.1 Link id choice

40000 inserts in batches of 1000, with 8 random sort-key bytes:

| Data size | Implicit rowid | Sequential `INTEGER PRIMARY KEY` | Random `INTEGER PRIMARY KEY` |
| --- | --- | --- | --- |
| 100 B | 6.8 MB, 98% leaf fill, 4.1 µs | 6.7 MB, 99% leaf fill, 4.0 µs | 8.3 MB, 87% leaf fill, 11.6 µs |
| 1000 B | 50.9 MB, 84% leaf fill, 8.5 µs | 56.4 MB, 75% leaf fill, 9.1 µs | 64.0 MB, 67% leaf fill, 30.6 µs |
| 3000 B | 166.0 MB, 74% leaf fill, 20.4 µs | 166.0 MB, 74% leaf fill, 20.7 µs | 166.8 MB, 74% leaf fill, 49.2 µs |

At 1000 B, the sequential `INTEGER PRIMARY KEY` used more space than the implicit rowid. A second in-memory run
checked the data sizes near 1000 B. At 900 B and 1100 B, both schemas used the same number of pages. Thus, the
difference comes from the page boundary at 1000 B, and not from the column:

| Data size | Implicit rowid | Sequential `INTEGER PRIMARY KEY` | `AUTOINCREMENT` |
| --- | --- | --- | --- |
| 100 B | 2.4 µs, 1429 pages | 2.4 µs, 1426 pages | 2.6 µs, 1429 pages |
| 900 B | 4.8 µs, 10237 pages | 5.1 µs, 10232 pages | 5.4 µs, 10238 pages |
| 1000 B | 5.5 µs, 12208 pages | 5.7 µs, 13570 pages | 6.0 µs, 13567 pages |
| 1100 B | 6.0 µs, 13574 pages | 6.0 µs, 13575 pages | 6.1 µs, 13578 pages |

With 20000 rows of 100-byte data, the table used 592 pages in both schemas. The record bytes were 2260008 without
the column and 2280009 with the column, which is 1 byte for each row.

### 8.2 Separate `data` table

Microseconds for each operation. The first value is the current schema, and the second value is a schema with
`item_data(item_id INTEGER PRIMARY KEY, data)`:

| Operation | 200 B | 1.5 KB | 20 KB | 200 KB | 900 KB |
| --- | --- | --- | --- | --- | --- |
| `getItem` | 5.8 / 7.4 | 6.5 / 7.4 | 9.9 / 11.9 | 55 / 52 | 213 / 234 |
| Stamp read | 6.1 / 4.5 | 7.1 / 5.3 | 5.3 / 4.2 | 6.4 / 4.2 | 5.3 / 4.0 |
| `bumpItemReadTs` | 14 / 13 | 16 / 12 | 22 / 12 | 111 / 12 | 395 / 11 |
| Upsert, same size | 14 / 23 | 17 / 25 | 95 / 81 | 786 / 604 | 3358 / 2933 |
| Query 50 rows with data | 63 / 109 | 82 / 151 | 662 / 722 | 6914 / 6577 | 18415 / 23314 |
