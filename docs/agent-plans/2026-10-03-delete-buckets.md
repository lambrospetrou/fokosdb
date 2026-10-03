# RFC — Delete buckets: one delete counter for each key bucket, not one for each partition

**State:** Draft
**Date:** 2026-10-03
**Author:** Lambros Petrou
**Status:** Not built. The partition keeps one `deletion_metadata` row.

## Table of Contents

1. [Overview and Context](#1-overview-and-context)
2. [Goals and Requirements](#2-goals-and-requirements)
3. [Milestones](#3-milestones)
4. [Proposed Solution](#4-proposed-solution)
5. [Alternative Options](#5-alternative-options)
6. [Frequently Asked Questions](#6-frequently-asked-questions)
7. [References](#7-references)

## 1. Overview and Context

### 1.1 The current system

Each partition has the table `deletion_metadata` with exactly one row (`id = 1`). The row has two columns:

- `delete_revision`: a counter. It goes up by 1 each time a delete removes an item row.
- `max_delete_tx_order_ts`: the highest transaction order timestamp of a delete in the partition.

`PartitionStore.deleteItem` updates the row in the same statement sequence as the `DELETE`. The TTL sweep
(`PartitionStore.deleteExpiredItems`) moves `max_delete_tx_order_ts` forward. The sweep does not change
`delete_revision`, because the logical delete of an expired row occurs at its expiry time.

Two features read the row. Both read the same value for every key in the partition.

**The read transaction.** A multi-partition read transaction reads its items two times, in phase 1 and in phase 2.
`TransactionParticipant.readForTransactionLocal` reports `deleteRevision` for each item. Today
`PartitionStore.deleteRevisionFor(hk)` ignores `hk` and returns the one partition counter. The function reads the
counter one time for each RPC and gives the same value to every item. In `client/db.ts`, the read driver compares
the two phases item by item (`sameCommittedState`). When `found`, `version` or `deleteRevision` is different, the
read fails with `read_conflict`.

**The prepare of an absent item.** In `TransactionParticipant` prepare, an item with no stamp has no live row.
Thus `max_delete_tx_order_ts` is the only ordering signal. When `request.transactionTimestamp <=
getMaxDeleteTxOrderTs()`, the participant rejects the operation with `timestamp_conflict`. This applies to every
operation on an absent item: a `check`, and an update that creates the item.

### 1.2 The problem

A delete of one item changes the counter and the watermark for all items in the partition.

**Problem 1: a read transaction fails on a delete of an unrelated item.**

1. A client starts a read transaction for item `user#1` in partition P.
2. Phase 1 reads `user#1`. `deleteRevision = 500`.
3. 10 ms later, another client deletes `order#77`, also in partition P. The counter goes to 501.
4. Phase 2 reads `user#1`. `user#1` did not change, but `deleteRevision = 501`.
5. 500 ≠ 501. The read fails with `read_conflict`.

Example load: a partition gets 200 deletes/s, and the two phases of a read are 20 ms apart. On average, 4 deletes
occur between the phases. The chance of 0 deletes is e⁻⁴ ≈ 1.8 %. Thus about 98 % of multi-partition reads on that
partition fail. A retry fails at the same rate.

**Problem 2: a transactional insert fails on a delete of an unrelated item.**

1. A client whose clock is 3 s ahead deletes `order#77`. The partition watermark is now "now + 3 s".
2. For the next 3 s, every transactional write to an absent item in the partition fails with
   `timestamp_conflict`. This includes `user#9`, which has no relation to `order#77`.

Clocks can be up to 5 s ahead, so the bad interval can be up to 5 s long. The TTL sweep also moves the watermark
forward, so expired rows cause the same failure for unrelated keys.

### 1.3 Why the shared value is necessary

The read check must detect this sequence for an item `X`:

1. Phase 1: `X` is absent.
2. A transaction creates `X`.
3. A transaction deletes `X`.
4. Phase 2: `X` is absent.

Both phases see "absent", but two writes occurred between them. After the delete, `items` has no row for `X`. Thus
no row can hold a counter for `X` only. The counter must be in a row that stays after the delete, and more keys
than one must share that row. The same applies to the delete watermark of an absent item at prepare.

### 1.4 Why now

The SQL migrations can still be edited in place before the schema freeze. The change adds one table and changes
the migration page. After the freeze, the target of a migration must also accept pages of the old format during a
deploy. Now, the change needs no compatibility code.

## 2. Goals and Requirements

### 2.1 In scope

- A delete changes the revision and the watermark of one bucket only: the bucket of the deleted key.
- A read transaction reports, for each item, the revision of the bucket of that item.
- The prepare of an absent item compares the transaction timestamp with the watermark of the bucket of that item.
- The TTL sweep moves forward the watermark of the bucket of each swept row. It does not change a revision.
- A migration copies all buckets to the target. The target merges each bucket with `MAX`.
- The create-then-delete sequence of section 1.3 still fails the read with `read_conflict`.

### 2.2 Requirements

- The wire format of the read transaction must not change. Each item result already carries `deleteRevision`
  (`shared/transaction-wire-types.ts`), and `client/db.ts` already compares it for each item.
- A delete or an expiry must write at most one bucket row. A user delete writes one bucket row in place of the one
  `deletion_metadata` row, the same as today. Durable Object storage bills each row written.
- The bucket of a key must depend only on the key. It must not depend on the partition. This makes a merge by
  bucket number correct after a split or a promotion.

### 2.3 Out of scope

- A counter for each item. Section 1.3 gives the reason.
- Compatibility with migration pages of the old format. Section 1.4 gives the reason.

## 3. Milestones

1. **Schema and store.** Add `deletion_buckets` with its comment (section 4.2.10), and remove `deletion_metadata`, in
   the SQL migration. Change `deleteItem`, `bumpMaxDeleteTxOrderTs`, `getMaxDeleteTxOrderTs` and `deleteRevisionFor`
   to take the key and use its bucket. Change the TTL sweep to update each bucket.
2. **Read and prepare.** `readForTransactionLocal` reads the bucket of each item. The prepare of an absent item reads
   the watermark of the bucket of that item. Update the text that describes one counter for each partition: the
   comment of `sameCommittedState` in `client/db.ts`, the `deleteRevision` comment in
   `shared/transaction-wire-types.ts`, the comments in `readForTransactionLocal` and `deleteRevisionFor`, and the
   Transactions section of `AGENTS.md`.
3. **Migration.** The `pending_tx` stream carries all buckets. The target merges them with `MAX`.

Each milestone ships with its tests (section 4.2.7).

## 4. Proposed Solution

### 4.1 High-level overview

The partition keeps a fixed set of 1,024 counter rows, the buckets, in place of one row. A hash of the key selects
the bucket of the key. A delete updates only the bucket of the deleted key. A read transaction and a prepare read
only the bucket of their item.

```
today                              with buckets
-----                              ------------
delete order#77 ─┐                 delete order#77 ──► bucket 803  (revision +1)
delete user#4  ──┼─► deletion_     delete user#4   ──► bucket 140  (revision +1)
read user#1  ◄───┘   metadata      read user#1     ◄── bucket 12   (unchanged)
                     (id = 1)
```

The example of Problem 1 with buckets:

1. `user#1` is in bucket 12. `order#77` is in bucket 803.
2. The delete of `order#77` changes only bucket 803.
3. Phase 2 for `user#1` reads bucket 12. The value is the same as in phase 1. The read succeeds.

A read now fails on an unrelated delete only when the two keys share a bucket. With 4 deletes between the phases,
the chance for one item is about 4 / 1,024 ≈ 0.4 %, not 98 %. A read of more items in the partition fails more
often, because each item has its own bucket. A read of 100 items touches about 95 buckets, so it fails about
1 − (1 − 95 / 1,024)⁴ ≈ 32 % of the time. A create and a delete of the same item still change the bucket of that
item, so the check of section 1.3 still works.

### 4.2 Technical details

#### 4.2.1 Data model

```sql
CREATE TABLE IF NOT EXISTS deletion_buckets (
    bucket        INTEGER PRIMARY KEY,
    revision      INTEGER NOT NULL DEFAULT 0,
    max_delete_ts INTEGER NOT NULL DEFAULT 0
) STRICT;
```

- The table replaces `deletion_metadata` in the same SQL migration, edited in place.
- `bucket` is in the range `0 .. 1023`.
- The SQL migration creates all 1,024 rows. Thus a delete, a read and a merge always find their row, and each one is
  a plain `UPDATE` or `SELECT` by primary key:

  ```sql
  WITH RECURSIVE b(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM b WHERE n < 1023)
  INSERT OR IGNORE INTO deletion_buckets (bucket) SELECT n FROM b;
  ```

- Size: 1,024 rows of 3 integers, about 30 KB.

#### 4.2.2 Bucket function

`bucketOf(hk, sk) = hash32(<bytes of (hk, sk)>) % 1024`. `hash32` is in `sharding/hash-primitives.ts`. The function
must be the same on every partition and on every deploy, because a migration merges buckets by number. The encoding
of `(hk, sk)` into one byte string must keep the pairs distinct: `("ab", "c")` and `("a", "bc")` must give different
bytes.

`bucketOf` must use a fixed seed that no routing function uses. The router selects a root with
`hashRootIndex(hk) = h32(hk, GOLDEN_RATIO) % rootTreesN`, and `GOLDEN_RATIO` is also the default seed of `hash32`.
When the bucket hash has the same seed and the same input bytes, the two hashes are not independent. Example: with
`rootTreesN = 4`, root 1 holds only keys where `h32(hk) % 4 == 1`. If `(hk, "")` encodes to the bytes of `hk`, every
key of root 1 goes to a bucket where `bucket % 4 == 1`. That root then uses 256 buckets, not 1,024, and gets 4 times
more false conflicts.

The hash takes both `hk` and `sk`, not `hk` only. Thus the items of one hash key go to different buckets. When one
hash key has many deletes, for example a queue, the other sort keys of that hash key get no false conflicts.

#### 4.2.3 Delete

`PartitionStore.deleteItem` keeps its current rules, and applies them to one bucket:

- When the `DELETE` removed a row, the store adds 1 to `revision` and sets `max_delete_ts = MAX(max_delete_ts, ?)`
  for the bucket of `(hk, sk)`.
- When the row was absent and `bumpTxOrderTsAlways` is set, the store sets only `max_delete_ts` for the bucket.

The callers are `PartitionDO` (the single-item delete) and `TransactionParticipant` (the commit of a transactional
delete). They already pass `hk` and `sk`.

#### 4.2.4 TTL sweep

`PartitionStore.deleteExpiredItems` must return `sk` in its `RETURNING` clause. The store groups the deleted rows by
bucket, and moves `max_delete_ts` of each bucket forward with `MAX(max_delete_ts, ?)` to the highest expiry of its
rows, in transaction order units. The
store must not apply one maximum for all rows to every bucket, because that moves the watermark of unrelated
buckets. The sweep does not change `revision`. The update stays in the same `transactionSync` as the `DELETE`.

#### 4.2.5 Read transaction and prepare

- `readForTransactionLocal` reads the revision of the bucket of each item. It must not share one value across the
  items of the RPC. Items of one RPC that share a bucket can share one read.
- `readForTransactionLocal` takes a flag that tells whether the caller needs the revisions. `txReadForTransaction`
  sets it, because the two-phase read compares the revisions. `txReadSnapshot` does not set it, because one partition
  reads the whole set in one step, and `client/db.ts` removes `deleteRevision` from the snapshot result. Without the
  flag, the function reads no bucket and reports `deleteRevision: 0`, because the wire type needs the field.
- Prepare: when an item has no stamp, the participant rejects the operation with `timestamp_conflict` when
  `request.transactionTimestamp <= max_delete_ts` of the bucket of that item.
- `client/db.ts` does not change.

#### 4.2.6 Migration

Today each `pending_tx` page carries `deletionMetadata` (`FokosMigrationHost`), and `#applyPendingTx` merges it with
`mergeDeletionMetadata`. Each slice gets at least one `pending_tx` page, also a slice with no lock.

With buckets:

- Each `pending_tx` page carries the `(bucket, revision, max_delete_ts)` of all buckets of the source. 1,024 entries
  are about 30 KB on a page.
- The target merges each bucket: `revision = MAX(revision, ?)` and `max_delete_ts = MAX(max_delete_ts, ?)`. The merge
  is idempotent, so a retried page gives the same result.
- The merge must write only the buckets whose values go up. SQLite counts each row that the `WHERE` clause matches as
  written, also when the values do not change. Without a guard, each page writes 1,024 billed rows. The guard:

  ```sql
  UPDATE deletion_buckets
     SET revision = MAX(revision, ?2), max_delete_ts = MAX(max_delete_ts, ?3)
   WHERE bucket = ?1 AND (revision < ?2 OR max_delete_ts < ?3);
  ```

- The target gets all buckets, also the buckets of keys that it does not own. This is correct: a higher value can
  only cause an extra conflict, never a missed conflict.

#### 4.2.7 Testing

- Problem 1: a delete of an item in another bucket between the two phases does not fail the read.
- Section 1.3: a create and a delete of the same absent item between the two phases fail the read with
  `read_conflict`.
- Problem 2: a delete with a timestamp in the future does not fail a transactional insert of an absent item in
  another bucket. It fails an insert of an absent item in the same bucket.
- The TTL sweep moves forward only the buckets of the swept rows, and changes no revision.
- A migration copies the buckets. A retried page gives the same values. A target that already has higher values
  keeps them.
- The current tests that read `deletion_metadata` move to the new store functions. In `packages/fokosdb/`:
  - `src/shared/partition/partition-store.test.ts`
  - `src/shared/partition/transaction-participant.test.ts`
  - `test/partition-do/migration-timestamps.test.ts`
  - `test/repartition/repartition-flow.test.ts`
  - `test/transactions/tx-end-to-end.test.ts`

#### 4.2.8 Cost

- A delete: one bucket row written, the same as today.
- A read transaction: one bucket read for each distinct bucket in the RPC, in place of one read for each RPC. With
  `(hk, sk)` buckets, this is about one primary-key read for each item.
- A single-partition snapshot read: no bucket read. Today it reads the `deletion_metadata` row one time.
- A TTL sweep: at most one bucket row written for each expired item. A chunk of `chunkSize: 100` random keys touches
  about 1,024 × (1 − e^(−100/1024)) ≈ 95 buckets, so it writes about 95 bucket rows. Today a chunk writes one
  `deletion_metadata` row. The `DELETE` of the expired rows counts as rows written, the same as today. Thus the
  sweep writes up to one more row for each expired item than today, and an expiry now writes one bucket row, the
  same as a user delete.
- A new partition: 1,024 rows written one time, when the SQL migration creates the buckets.
- A migration page: one row written for each bucket whose value goes up on the target. At most 1,024 rows. Each page
  also reads 1,024 bucket rows on the source, and 1,024 bucket rows on the target for the guarded `UPDATE`s.
- Storage: about 30 KB for each partition.

#### 4.2.9 Deployment and rollback

`PartitionStore` runs its SQL migrations with `SQLSchemaMigrations` from `durable-utils`. The library records the ID
of each applied migration, and it does not run an applied ID again. Thus an edit in place of an applied migration has
no effect on an existing partition. That partition never gets `deletion_buckets`, and each delete fails.

The deploy is a breaking change:

1. Delete the project.
2. Deploy the new version.

No old code runs next to the new code, so no partition has the old schema and no migration page has the old
format.

The rules for the deploy:

- **Schema.** The change edits the SQL migration with `idMonotonicInc: 2` in place. It removes `deletion_metadata`
  and adds `deletion_buckets`. No new migration ID and no data upgrade are necessary, because no partition with data
  exists before the deploy.
- **Rollback.** There is no rollback. A fault in the new version gets a fix forward. The old code fails on a
  partition without `deletion_metadata`.

#### 4.2.10 Schema comment

The SQL migration has no table for the single-row design. A comment on `deletion_buckets` documents why the buckets
exist and what the single-row design was. The comment text:

```sql
-- deletion_buckets: delete counters, one row for each key bucket. bucketOf(hk, sk) selects one of 1,024 buckets.
--
-- After a delete, items has no row for the key, so no row can keep a counter for that key only.
-- Thus keys share counters, and a counter must stay after the delete of its keys.
--
-- revision      : goes up by 1 when a delete removes an item row. A read transaction compares the revision of
--                 each item between its two phases. This detects a create and then a delete of an absent item.
-- max_delete_ts : the highest transaction order timestamp of a delete or of a TTL expiry in the bucket. The prepare
--                 of an absent item rejects a transaction timestamp that is not greater than this value.
--
-- One row for the whole partition also gives correct results:
--     deletion_metadata(id = 1, delete_revision, max_delete_tx_order_ts)
-- But then a delete of one key changes the counter of all keys. At 200 deletes/s and 20 ms between the two
-- phases of a read, about 98 % of multi-partition reads fail. With 1,024 buckets, a delete gives a false conflict
-- only to the keys in its bucket. At the same load, a read of 1 item fails about 0.4 % of the time, and a read
-- of 100 items in the partition about 32 % of the time.
```

## 5. Alternative Options

- **A counter for each item.** Rejected. After a delete, no row is left to hold the counter (section 1.3).
- **Keep one counter for each partition.** Rejected. About 98 % of reads fail at 200 deletes/s (section 1.2).
- **A bucket hash of `hk` only.** Rejected. All items of one hash key share one bucket, so a hash key with many
  deletes gives false conflicts to all its sort keys. A promoted hash key has a range tree of its own, so every item
  of those partitions is in one bucket, the same as one counter for each partition. These are the largest keys. The
  gain is for a read of many sort keys of one hash key in a hash partition: it reads one bucket, and it fails about
  0.4 % of the time, not about 32 % (section 4.1). The read RPC also reads fewer buckets, but each read is one
  primary-key read in local SQLite. A counter for each hash key also does not prepare for a transactional query over
  a hash key, because an insert does not change the revision.
- **A separate watermark for the whole partition, for the TTL sweep only.** Rejected. Only the sweep updates it. A
  sweep sets it to an expiry time that has already passed, so it gives false conflicts only to clients whose clocks
  are behind. It saves about one row written for each expired item. But the prepare of an absent item must then
  compare with `MAX(bucket.max_delete_ts, <sweep watermark>)`, the migration page must carry and merge one more
  value, and the schema comment and the tests must cover a second watermark. One mechanism for all deletes is
  simpler.
- **Create each bucket row on its first write.** Rejected. Each write then needs an upsert, and each read must handle
  a missing row. The SQL migration creates the 1,024 rows one time.
- **Carry the buckets on only one page of each slice.** Rejected. The source must know which page is the last one,
  and the target must not finish the slice before it applies that page. The guarded merge (section 4.2.6) keeps the
  cost of a repeated set of buckets to the rows that go up.

## 6. Frequently Asked Questions

**Does the client change?** No. Each item result already has its own `deleteRevision`, and the client compares it
for each item.

**Does a delete write more rows?** A user delete does not. It writes one bucket row in place of one
`deletion_metadata` row. The TTL sweep does. It writes at most one bucket row for each expired item, where today it
writes one row for each chunk (section 4.2.8).

**Can a read still fail on an unrelated delete?** Yes, when the two keys share a bucket. The chance for one item is
about the number of deletes between the phases divided by 1,024. A read of more items in the partition fails more
often (section 4.1).

**Is a merge of all source buckets into a target correct?** Yes. The bucket of a key does not depend on the
partition. A value that is too high causes only a conservative conflict.

## 7. References

- `docs/ideas/2026-09-29-storage-and-query-audit.md` (finding F5)
- `packages/fokosdb/src/shared/partition/partition-store.ts`
- `packages/fokosdb/src/shared/partition/transaction-participant.ts`
- `packages/fokosdb/src/shared/partition/fokos-migration-host.ts`
- `packages/fokosdb/src/client/db.ts`
- `packages/fokosdb/src/shared/transaction-wire-types.ts`
- `packages/fokosdb/src/sharding/hash-primitives.ts`
