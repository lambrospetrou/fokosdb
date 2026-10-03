# RFC — Replace the delete revision with the highest deleted item version, and keep the key watermark across deletes

**State:** Draft
**Date:** 2026-10-03
**Author:** Lambros Petrou
**Status:** Not built. The partition keeps `delete_revision` in its one `deletion_metadata` row. A late prepare can
apply a committed transaction a second time.

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

Each item row has a version `v`. A new item row starts at `v = 1`. Each later write to the row sets `v = v + 1`.

Each partition has the table `deletion_metadata` with exactly one row (`id = 1`). The row has two columns:

- `max_delete_tx_order_ts`: the highest transaction order timestamp of a delete or of a TTL expiry. The prepare of
  an absent item rejects a transaction timestamp that is not greater than this value.
- `delete_revision`: a counter. It goes up by 1 each time a user delete removes an item row. The TTL sweep does not
  change it.

A multi-partition read transaction reads its items two times, in phase 1 and in phase 2.
`TransactionParticipant.readForTransactionLocal` reports `version` for each found item, and the partition
`delete_revision` for every item. In `client/db.ts`, `sameCommittedState` compares the two phases item by item. The
read fails with `read_conflict` when `found` is different, when `version` is different for a found item, or when
`deleteRevision` is different for any item.

The read needs `delete_revision` for two sequences that `version` cannot detect:

```text
found(v=1)  -> delete -> recreate(v=1) -> found(v=1)     the version repeats
absent      -> create -> delete        -> absent         no row holds a version
```

### 1.2 Problem 1: a read fails on an unrelated delete

Every user delete in the partition changes `delete_revision`, and every item of a read compares it. Thus a read fails
on a delete of an unrelated item.

1. A client starts a read transaction for item `user#1` in partition P.
2. Phase 1 reads `user#1` with `version = 7`, and `deleteRevision = 500`.
3. 10 ms later, another client deletes `order#77`, also in partition P. The counter goes to 501.
4. Phase 2 reads `user#1` with `version = 7`, and `deleteRevision = 501`.
5. 500 ≠ 501. The read fails with `read_conflict`, although `user#1` did not change.

The two phases of a read are about 20 ms apart. With `d` deletes per second in the partition, the chance of 0 deletes
between the phases is `e^(−d × 0.02)`. The chance that a read on that partition fails:

| Deletes/s in the partition | Read fails |
| --- | --- |
| 1 | about 2 % |
| 10 | about 18 % |
| 200 | about 98 % |

A retry fails at the same rate.

### 1.3 Problem 2: a committed transaction can apply a second time

After a commit, the lock row of the transaction is gone. Only the timestamp watermark of the key then rejects a late
copy of the same prepare. The watermark of a key is the `last_read_ts` of its row when the row exists, and
`max_delete_tx_order_ts` when it does not.

A copy arrives late when the partition applied the first prepare attempt, the answer was lost, and a second drive of
the transaction committed before the retry of the first drive arrived. A write that stamps the partition clock can
lower the watermark, because a prepare accepts a coordinator clock up to `maxClockSkewMs` ahead. The single-item put
and delete, the single-shot transaction, and the TTL sweep stamp the partition clock.

Two sequences break the watermark today. In both, transaction T has the timestamp 200, the partition clock is at 160,
and T has committed on the partition:

```text
T puts X      (last_read_ts = 200) -> single-shot delete at 160 (max_delete_tx_order_ts = 160) -> X absent
T deletes X   (max_delete_tx_order_ts = 200) -> single-shot put at 160 (new row, last_read_ts = 160)
```

In both sequences the late prepare of T compares 200 with 160 and passes. It writes a new lock row. The stale
transaction job asks the coordinator, the coordinator answers `COMMITTED`, and the partition applies T a second time.
The first sequence brings back the deleted item. The second sequence deletes the new item.

### 1.4 How DynamoDB avoids Problem 1

The read transaction of DynamoDB compares the log sequence number (LSN) of each item between the two phases
([Idziorek et al., ATC 2023](https://www.usenix.org/system/files/atc23-idziorek.pdf), section 3.4). The LSN of an
item is "the sequence number of the last write that the storage node performed". The paper does not describe the
log. If the log has one sequence for the node, each write gets a new number, and a recreated item gets an LSN that
no earlier version of the item had. A delete of another item does not change the stored LSN of the read item.

The paper has no partition-wide delete counter. Its only partition-wide delete value is the max delete timestamp at
prepare (section 3.3). FokosDB has the same value as `max_delete_tx_order_ts`. The paper does not say how DynamoDB
detects the `absent -> create -> delete -> absent` sequence.

FokosDB needs `delete_revision` for found items only because `v` starts again at 1 after a recreate.

### 1.5 Why now

The SQL migrations can still be edited in place before the schema freeze. The change replaces one column and changes
one field of the migration page. Now, the change needs no compatibility code.

## 2. Goals and Requirements

### 2.1 In scope

- The `v` of a key never repeats in a partition, also after a delete and a recreate of the key.
- A read transaction compares only `found` and `version` for an item that it finds in both phases.
- A delete of another item never fails a read of an item that is found in both phases.
- A read transaction compares `max_deleted_v` for an item that is absent in both phases.
- Both sequences of section 1.1 still fail the read with `read_conflict`.
- `max_deleted_v` replaces `delete_revision`. The change adds no concept to the transaction model.
- A late prepare of a committed put, update or delete gets `timestamp_conflict`, also after a delete or a create of
  the key by a write that stamps the partition clock (section 1.3).

### 2.2 Requirements

- The change must not add a row written to any operation. Durable Object storage bills each row written.
- `max_deleted_v` must only move up. This includes a migration to a child partition or to a range root.
- The timestamp watermark of a key (section 1.3) must never go down. Section 4.2.12 gives the rules.

### 2.3 Out of scope

- Buckets for the prepare of an absent item. The prepare keeps the partition-wide `max_delete_tx_order_ts`, the same
  as the max delete timestamp of DynamoDB. A conflict needs a transaction timestamp that is not greater than the
  timestamp of an earlier delete in the partition. The create rule of section 4.2.12 keeps this conflict after the key
  gets a new row, until the clocks pass the value.
- False conflicts for an item that is absent in both phases. Section 4.2.10 gives their rate.
  `docs/ideas/2026-10-03-delete-buckets.md` describes buckets that reduce both this rate and the prepare conflicts.
- Compatibility with data or migration pages of the old format. Section 1.5 gives the reason.

## 3. Milestones

1. **Store.** In the SQL migration, replace `delete_revision` with `max_deleted_v` and add the schema comment
   (section 4.2.1). Apply the create rule (section 4.2.2) in `upsertItem` and `updateItemSingleShot`. Make
   `deleteItem` and `deleteExpiredItems` record the `v` of the removed rows (sections 4.2.4 and 4.2.5). In the same
   statements, apply the timestamp watermark rules (section 4.2.12).
2. **Read transaction.** `readForTransactionLocal` reports `maxDeletedV` only for an absent item. Change the wire type
   and `sameCommittedState` (section 4.2.7). Update the text that describes the old counter: the comment of
   `sameCommittedState` in `client/db.ts`, the comment of `ReadForTransactionItemResultEncoded` in
   `shared/transaction-wire-types.ts`, the comments of `readForTransactionLocal` and of the store functions that read
   the deletion metadata, and the Transactions section of `AGENTS.md`.
3. **Migration.** The `pending_tx` page carries `maxDeletedV` in place of `deleteRevision`, and the target merges it
   with `MAX` (section 4.2.8).

Each milestone ships with its tests (section 4.2.14).

## 4. Proposed Solution

### 4.1 High-level overview

The `deletion_metadata` row keeps `max_deleted_v` in place of `delete_revision`. It is the highest `v` of an item row
that a delete or a TTL expiry removed in the partition.

- **Create.** A new item row starts at `v = max_deleted_v + 1`, not at `v = 1`.
- **Update.** A write to an existing row sets `v = v + 1`, the same as today.
- **Delete.** A delete that removes a row sets `max_deleted_v = MAX(max_deleted_v, <v of the row>)`, in the same
  `UPDATE` that today increases `delete_revision`.

Each rule prevents one cause of a repeated `v`. `v + 1` prevents a repeat inside one lifetime of the row. The create
rule prevents a repeat across lifetimes, because the delete of the earlier lifetime left its last `v` in the row.

The read transaction then compares one value for each item:

```text
phase 1    phase 2    compare
-------    -------    -------
found      found      version
absent     absent     maxDeletedV
found      absent     conflict
absent     found      conflict
```

The example of section 1.2 with the change:

1. Phase 1 reads `user#1` with `version = 7`.
2. The delete of `order#77` raises `max_deleted_v`. `user#1` does not change.
3. Phase 2 reads `user#1` with `version = 7`. The values are equal, so the read succeeds.

The two sequences of section 1.1 with the change, when `max_deleted_v = 40` at phase 1:

```text
found(v=1)  -> delete (max_deleted_v = 40) -> recreate(v=41) -> found(v=41)          1 ≠ 41, conflict
absent      -> create(v=41) -> delete (max_deleted_v = 41)   -> absent               40 ≠ 41, conflict
```

The same statements also keep the timestamp watermark of a key from going down (Problem 2):

- **Delete.** A delete that removes a row raises `max_delete_tx_order_ts` to at least the `last_read_ts` of the row.
- **Create.** A new row starts with `last_read_ts` and `last_write_ts` at least `max_delete_tx_order_ts`.

The two sequences of section 1.3 with the change:

```text
T puts X    (last_read_ts = 200) -> delete at 160 (max_delete_tx_order_ts = 200) -> late prepare 200: conflict
T deletes X (max_delete_tx_order_ts = 200) -> put at 160 (new row, last_read_ts = 200) -> late prepare 200: conflict
```

### 4.2 Technical details

#### 4.2.1 Data model

The change edits the SQL migration with `idMonotonicInc: 2` in place:

```sql
-- deletion_metadata: one row (id = 1) with the delete values of the whole partition. The values stay after the
-- delete of an item row, so they describe keys that have no row.
--
-- max_delete_tx_order_ts : the highest transaction order timestamp of a delete or of a TTL expiry, and at least
--                          the last_read_ts of each removed row. The prepare of an absent item rejects a
--                          transaction timestamp that is not greater than this value. A new item row starts with
--                          last_read_ts and last_write_ts at least this value. Thus the timestamp watermark of a
--                          key never goes down, and a late prepare of a committed transaction gets a conflict.
-- max_deleted_v          : the highest v of an item row that a delete or a TTL expiry removed. A new item row
--                          starts at max_deleted_v + 1, so the v of a key never repeats, also after a delete and a
--                          recreate. A read transaction compares this value for an item that is absent in both
--                          phases. A read compares v for an item that is found in both phases.
CREATE TABLE IF NOT EXISTS deletion_metadata (
    id                     INTEGER PRIMARY KEY CHECK (id = 1),
    max_delete_tx_order_ts INTEGER NOT NULL DEFAULT 0,
    max_deleted_v          INTEGER NOT NULL DEFAULT 0
) STRICT;
INSERT OR IGNORE INTO deletion_metadata (id, max_delete_tx_order_ts, max_deleted_v) VALUES (1, 0, 0);
```

On a new partition `max_deleted_v = 0`, so the first item row starts at `v = 1`, the same as today.

#### 4.2.2 Create

Two statements create an item row: `PartitionStore.upsertItem` and `PartitionStore.updateItemSingleShot`. Each one
is an `INSERT ... ON CONFLICT(hk, sk) DO UPDATE`. The insert branch writes the literal `1` into `v` today. The change
replaces the literal with a subquery:

```sql
(SELECT max_deleted_v + 1 FROM deletion_metadata WHERE id = 1)
```

The conflict branch keeps `v = v + 1`. Every path that creates an item goes through these two functions: the
non-transactional put, the commit of a transaction, and the single-shot transaction.

A migration copies each item row with its `v` (`PartitionStore.insertItemIfAbsent`). This is not a create, and it
does not apply the rule.

An expired row that the sweep has not removed yet is still a row. A read returns it, and a write to it takes the
conflict branch. Thus `v` stays continuous until the sweep removes the row.

#### 4.2.3 Update

An update keeps `v = v + 1`. It does not read `max_deleted_v`.

- `v = max_deleted_v + 1` on an update is wrong. `max_deleted_v` changes only on a delete, so two updates in a row
  get the same `v`, and a read misses the second update. When `v` is above `max_deleted_v`, the update also moves
  `v` down.
- `v = MAX(v, max_deleted_v) + 1` is correct, but it gains nothing. `v + 1` already gives a new value inside one
  lifetime. It also makes the public `version` jump on an unrelated delete.

#### 4.2.4 Delete

`PartitionStore.deleteItem` keeps its current rules:

- When the `DELETE` removed a row, the store sets `max_deleted_v = MAX(max_deleted_v, ?)` with the `v` of the removed
  row, and `max_delete_tx_order_ts = MAX(max_delete_tx_order_ts, ?)`. This is one `UPDATE` of the one row, the same
  as today. The `DELETE` gets `v` with `RETURNING v`.
- When the row was absent and `bumpTxOrderTsAlways` is set, the store sets only `max_delete_tx_order_ts`. A delete
  that removes no row leaves no `v` to record.

The callers are `PartitionDO` (the single-item delete) and `TransactionParticipant` (the commit of a transactional
delete, and the single-shot transaction).

#### 4.2.5 TTL sweep

`PartitionStore.deleteExpiredItems` adds `v` to its `RETURNING` clause. In the same `transactionSync` as the
`DELETE`, one `UPDATE` sets `max_deleted_v` to `MAX(max_deleted_v, <highest v of the chunk>)`, and moves
`max_delete_tx_order_ts` forward as today. The sweep still writes one metadata row for each chunk.

Today the sweep does not change `delete_revision`. The sweep must change `max_deleted_v`, because a later create must
start above the `v` of the expired row. Without the update, this read passes and it must fail:

1. `X` is at `v = 3` and expired, and `max_deleted_v = 2`. Phase 1 finds `X` with `version = 3`.
2. The sweep removes `X`.
3. A put recreates `X` with `v = max_deleted_v + 1 = 3`.
4. Phase 2 finds `X` with `version = 3`. The values are equal, but `X` changed.

The cost of the update is more false conflicts for a read of an absent item. Section 4.2.10 gives the rate.

#### 4.2.6 Promotion cleanup

`PartitionStore.deleteItemsBatchForHashKey` removes the copies of a promoted hash key from the source partition. It
does not record `v`. After the promotion, the source routes the key to the range root, so the source never creates
a row for that key again.

#### 4.2.7 Read transaction

The wire type `ReadForTransactionItemResultEncoded` in `shared/transaction-wire-types.ts` changes:

- The shared field `deleteRevision` goes away.
- The member `{ found: false }` gets the field `maxDeletedV: number`.

`readForTransactionLocal` reads `max_deleted_v` one time for each RPC, and only when an item of the RPC is absent. It
gives the value to each absent item. Both `txReadForTransaction` and `txReadSnapshot` use the function. The snapshot
result does not need the value, but one primary-key read for each RPC is the cost of today, and one code path is
simpler than a flag.

`sameCommittedState` in `client/db.ts`:

```ts
if (a.found !== b.found) {
	return false;
}
if (a.found && b.found) {
	return a.version === b.version;
}
return a.maxDeletedV === b.maxDeletedV;
```

The public result does not carry `maxDeletedV`. `db.ts` removes it at the public boundary, as it removes
`deleteRevision` today.

#### 4.2.8 Migration

Each `pending_tx` page carries `deletionMetadata: { maxDeleteTxOrderTs, maxDeletedV }` (`FokosMigrationHost`). The
target merges both values with `MAX` in `mergeDeletionMetadata`, as it merges `deleteRevision` today. The merge is
idempotent, so a retried page gives the same result. Each slice gets at least one `pending_tx` page, also a slice
with no lock.

The source cuts routing over before the target pulls pages. Thus each delete of a moved key on the source occurs
before the source builds the page, and the page carries its `v`. The target accepts no write until its import ends,
so it creates no row before the merge.

#### 4.2.9 Invariant

**The `v` of a key never repeats in a partition lineage.** A partition lineage is a partition and the partitions
that import its keys.

1. Inside one lifetime of a row, each write sets `v = v + 1`, so `v` only goes up.
2. When a row is removed, its last `v` is the highest `v` of its lifetime. The delete or the sweep raises
   `max_deleted_v` to at least that value.
3. `max_deleted_v` only goes up: every update uses `MAX`, and the migration merge uses `MAX`.
4. A new row of the key starts at `max_deleted_v + 1`. This is above every `v` of every earlier lifetime of the key.

Steps 2 and 4 need the delete and the create to run on partitions of one lineage. A migration carries
`max_deleted_v` to the new owner before the new owner creates a row (section 4.2.8).

**The read finds each change of a found item.** When the item is found in both phases, a write between the phases
gives a `v` that the item did not have in phase 1. A delete and a recreate between the phases also give a new `v`.

**The read finds `absent -> create -> delete -> absent`.** Phase 1 reads `max_deleted_v = M1`. The create sets
`v ≥ M1 + 1`, because `max_deleted_v` only goes up. The delete raises `max_deleted_v` to at least that `v`. Thus
phase 2 reads a value above `M1`.

#### 4.2.10 False conflicts for absent items

A read of an item that is absent in both phases fails when `max_deleted_v` changes between the phases. A delete
changes `max_deleted_v` only when the `v` of the removed row is above the current value. A delete of an old row
with a low `v` does not change it. Thus the rate is at most the rate of section 1.2, with the expired rows of the TTL
sweep counted as deletes.

Two sources raise the rate:

- A partition where items are created and soon deleted, for example a queue. Each new row starts above
  `max_deleted_v`, so most deletes raise it.
- The TTL sweep. Today it does not change `delete_revision`. With the change, a sweep chunk raises `max_deleted_v`
  when it removes a row with a `v` above the current value.

A read of a found item has no false conflict from a delete.

#### 4.2.11 Public version

`version` is part of the public results (`PutItemResult` and the read results), and a condition can compare it
(`{ ref: "v" }`). After the change, a new item row starts at `max_deleted_v + 1`, not at 1. A caller that expects
`version = 1` for a new item gets a larger value in a partition that had deletes.

The value grows by at most 1 for each write in the partition lineage. At 1,000 writes per second, it reaches
`Number.MAX_SAFE_INTEGER` (about 9 × 10¹⁵) after about 285,000 years.

#### 4.2.12 Timestamp watermark

Section 1.3 gives the problem.

**The rules.** Each rule changes a statement that this RFC already changes. No rule adds a row written.

- **Delete.** `deleteItem` returns `last_read_ts` with `v` (`RETURNING v, last_read_ts`). The one metadata `UPDATE`
  sets `max_delete_tx_order_ts = MAX(max_delete_tx_order_ts, <tx order ts>, <last_read_ts of the row>)`. A delete of an
  absent row with `bumpTxOrderTsAlways` keeps its current rule.
- **TTL sweep.** `deleteExpiredItems` returns `last_read_ts` with `v`. The one metadata `UPDATE` for each chunk moves
  `max_delete_tx_order_ts` to the highest of the expiry time and the `last_read_ts` of each row of the chunk.
- **Create.** The insert branch stamps `last_read_ts` and `last_write_ts` with
  `MAX(<tx order ts>, max_delete_tx_order_ts)`. The statement reads `deletion_metadata` one time, with
  `INSERT … SELECT … FROM deletion_metadata d WHERE d.id = 1 AND <size test>`, and takes both `d.max_deleted_v + 1`
  and `d.max_delete_tx_order_ts` from that row. This join replaces the scalar subquery of section 4.2.2, because two
  subqueries read the row two times. `updateItemSingleShot` keeps its pre-image join: its source becomes
  `FROM deletion_metadata d LEFT JOIN items AS i ON …` in place of `FROM (VALUES (1)) LEFT JOIN items AS i ON …`.
- **Update.** The conflict branch keeps `MAX(last_read_ts, <tx order ts>)` and `MAX(last_write_ts, <tx order ts>)`
  with the bound parameter, not with `excluded.last_read_ts`. With `excluded`, each update of an existing row would
  also take the partition delete timestamp.

**The invariant.** A delete moves the watermark of the key from the row to `max_delete_tx_order_ts`, and the delete
rule makes that value at least the `last_read_ts` of the row. A create moves the watermark back to the row, and the
create rule makes the row stamps at least `max_delete_tx_order_ts`. Every other write uses `MAX`, and the migration
copies the row stamps and merges `max_delete_tx_order_ts` with `MAX`. Thus the watermark of a key never goes down, and
a late prepare of a committed put, update or delete always gets `timestamp_conflict`.

A late prepare of a committed `check` still passes, because a check compares `last_write_ts` and its commit moves only
`last_read_ts`. The data stays correct: a second commit of a check changes no item. The new lock refuses writes to the
key until the stale transaction job releases it.

**The cost.** `max_delete_tx_order_ts` is one value for the partition. After a delete by a coordinator whose clock is
ahead, each new row of the partition starts with stamps up to `maxClockSkewMs` ahead. A transaction with a lower
timestamp on that key gets `timestamp_conflict` until the clocks pass the value. The same transaction gets the same
conflict while the key is absent, so the rule adds no conflict. It only keeps the conflict after the create.

The delete rule and the sweep rule add a conflict. They raise the partition value to the `last_read_ts` of a removed
row, and that value can be up to `maxClockSkewMs` ahead of the partition clock. Example:

1. T commits a put of `X` at 200. The partition clock is at 160.
2. A single-item delete of `X` sets `max_delete_tx_order_ts = 200`. Today it sets 160.
3. Until the clocks pass 200, each prepare of an absent item in the partition with a timestamp in (160, 200] gets
   `timestamp_conflict`, also for a key other than `X`.

The conflict lasts at most `maxClockSkewMs`. `docs/ideas/2026-10-03-delete-buckets.md` makes both effects smaller.

#### 4.2.13 Cost

| Operation | Today | With the change |
| --- | --- | --- |
| Put, or commit of a transactional put or update | 0 metadata reads | 1 metadata read (sections 4.2.2, 4.2.12) |
| User delete | 1 metadata row written | the same |
| TTL sweep chunk | 1 metadata row written | the same |
| Read transaction RPC | 1 metadata read | 1 metadata read when an item is absent, else 0 |
| Migration page | 1 metadata read, at most 1 row written | the same |
| Storage | 1 row | the same |

SQLite reads the metadata row of the create statement before it finds a conflict. Thus an update of an existing row also
reads the metadata row. No operation writes more rows.

#### 4.2.14 Testing

- A create, a delete and a recreate of one key: the recreate starts above the last `v` of the first lifetime.
- A first item row in a new partition starts at `v = 1`.
- Section 1.2: a delete of another item between the two phases does not fail a read of a found item.
- Section 1.1, first sequence: a delete and a recreate of a found item between the phases fail the read with
  `read_conflict`.
- Section 1.1, second sequence: a create and a delete of an absent item between the phases fail the read with
  `read_conflict`.
- A delete of an absent item with `bumpTxOrderTsAlways` does not change `max_deleted_v`.
- The TTL sweep raises `max_deleted_v` to the highest `v` of its chunk. A recreate after the sweep starts above it.
- A migration copies `max_deleted_v`. A retried page gives the same value. A target with a higher value keeps it.
- After a hash split and after a promotion, a recreate on the new owner starts above the last `v` on the source.
- Section 1.3, first sequence: a transaction with a timestamp ahead of the partition clock puts X and commits. A
  single-shot delete of X follows. The same prepare again gets `timestamp_conflict`, and X stays absent. Repeat with
  the single-item delete in place of the single-shot delete.
- Section 1.3, second sequence: a transaction with a timestamp ahead of the partition clock deletes X and commits.
  A single-shot put of X follows. The same prepare again gets `timestamp_conflict`, and X stays. Repeat with the
  single-item put in place of the single-shot put.
- The TTL sweep removes a row whose `last_read_ts` is above its expiry time. `max_delete_tx_order_ts` becomes at least
  that `last_read_ts`.
- An update of an existing row does not take `max_delete_tx_order_ts` into its stamps.
- The current tests that read `delete_revision` move to `max_deleted_v`. In `packages/fokosdb/`:
  - `src/shared/partition/partition-store.test.ts`
  - `src/shared/partition/transaction-participant.test.ts`
  - `test/partition-do/migration-timestamps.test.ts`
  - `test/repartition/repartition-flow.test.ts`
  - `test/transactions/tx-end-to-end.test.ts`
- A test that expects `version = 1` after a delete and a recreate in the same partition changes its expected value.

#### 4.2.15 Deployment and rollback

The deploy is a breaking change:

1. Delete the project.
2. Deploy the new version.

No old code runs next to the new code, so no partition has the old schema and no migration page has the old format.

- **Schema.** The change edits the SQL migration with `idMonotonicInc: 2` in place. No new migration ID and no data
  upgrade are necessary, because no partition with data exists before the deploy.
- **Rollback.** There is no rollback. A fault in the new version gets a fix forward.

## 5. Alternative Options

- **A log sequence number for the partition, as in DynamoDB.** Each put, update and delete sets `v` to the next value
  of one partition counter. `v` then never repeats, and a found item needs no delete value. Rejected for three
  reasons:
  - Each put and update also writes the counter row: one more billed row for each write.
  - A read of an absent item still needs a value that only deletes change, for example the LSN of the last delete.
    The LSN alone changes on every write, so every write in the partition would fail a read of an absent item.
  - The public `version` jumps on every write by the number of writes to other items.
- **Keep `delete_revision` for every item.** Rejected. Section 1.2 gives the failure rate.
- **Delete buckets.** 1,024 rows, and a hash of `(hk, sk)` selects the row of a key. Not chosen now. It writes up to
  one metadata row for each expired item in place of one for each chunk, and it adds a table. After this change, it
  helps only a read of an absent item and the prepare of an absent item. `docs/ideas/2026-10-03-delete-buckets.md`
  keeps the design.
- **Compare `last_write_ts` in place of `v`.** Rejected. Two writes can get the same timestamp, so the read can miss
  the second write. `v` is a counter and cannot miss a write.
- **Use `items.item_id` as the never-repeating value.** Rejected. `item_id` is an `INTEGER PRIMARY KEY` without
  `AUTOINCREMENT`, so SQLite gives a new row `MAX(item_id) + 1`. A delete and a recreate of the row with the highest
  `item_id` gives the same value again. `AUTOINCREMENT` updates `sqlite_sequence` on each insert, which is one more
  row written. A migration also copies `item_id`, and the code keeps it out of every interface.
- **Keep the TTL sweep out of `max_deleted_v`.** Rejected. A recreate after a sweep can then repeat the `v` of the
  expired row. Section 4.2.5 gives the sequence.
- **A second column that only user deletes raise, for the absent-item read.** It removes the false conflicts from the
  sweep in section 4.2.10. Not chosen. It adds a concept, and it helps only a read of an absent item.

## 6. Frequently Asked Questions

**Does the client change?** Yes. `sameCommittedState` compares `version` for a found item and `maxDeletedV` for an
absent item. The public API keeps the same fields.

**Does any operation write more rows?** No. A put or an update reads one more row. Section 4.2.13 gives the costs.

**Can a read still fail on an unrelated delete?** Only a read of an item that is absent in both phases. Section
4.2.10 gives the rate.

**Why does a delete not set `v` on a row?** After the delete there is no row. `max_deleted_v` is the value that stays.

**Does DynamoDB have `max_deleted_v`?** The paper does not describe one. Its LSN gives the never-repeating version
(section 1.4). The paper does not say how DynamoDB handles an item that is absent in both phases.

## 7. References

- [Distributed Transactions at Scale in Amazon DynamoDB](https://www.usenix.org/system/files/atc23-idziorek.pdf)
  (ATC 2023), and the local copy `docs/research/atc23-idziorek-dynamodb-transactions.md`
- `docs/agent-plans/2026-09-05-item-order-timestamps-and-read-revisions.md` (adds `delete_revision`)
- `docs/ideas/2026-10-03-delete-buckets.md`
- `docs/ideas/2026-09-29-storage-and-query-audit.md` (finding F5)
- `packages/fokosdb/src/shared/partition/partition-store.ts`
- `packages/fokosdb/src/shared/partition/transaction-participant.ts`
- `packages/fokosdb/src/shared/partition/fokos-migration-host.ts`
- `packages/fokosdb/src/shared/transaction-wire-types.ts`
- `packages/fokosdb/src/client/db.ts`
