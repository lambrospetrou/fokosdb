# RFC — Read and write timestamps on the items table

**State:** Deferred and Incomplete
**Date:** 2026-09-05
**Author:** Lambros

---

## Why this RFC is deferred

The review of 2026-09-05 found that neither milestone pays for itself today.

- **Serializability does not depend on the timestamps.** Prepare takes a pending lock on every item of the
  transaction, `check` items included, and commit releases them. A non-transactional write is rejected on a
  locked item, the TTL sweep skips a locked row, and `transactGetItems` rejects on a pending lock. The
  real-time commit order is therefore always a valid serial order. The timestamp test in `prepareLocal` only
  rejects on top of that. M0 closes a gap in the timestamp order that no client can observe, and it adds one
  more partition-level watermark that rejects transactions.
- **The win is narrow.** The Thomas Write Rule fires only when the older transaction carries an unconditional
  put or delete on the item, the newer write was a full overwrite, and the two arrived out of timestamp
  order. That window is the coordinator clock skew plus the RPC spread. The rejection that hurts under
  contention is `pending_conflict`, and this RFC does not touch it.
- **The cost is not zero.** One more column on every row, one more watermark, one more billed read on every
  delete, and a coarse read watermark that rejects creates from every coordinator clock behind this partition
  for the skew window.

The RFC reopens when one of two things happens.

1. A measured rate of `timestamp_conflict` rejections under a contended workload justifies the columns.
   Section 6 carries the measurement as a TODO.
2. The first release is near. After the release the schema freezes, and a column after `data` costs a table
   rebuild in every Durable Object. If the optimizations of section 1.4 are ever wanted after the release, the
   columns must exist at the release, correct from the first row. Until then the migrations are edited in
   place and the columns can land on the day a rule needs them.

The document is kept as the design record. Section 1.2 states the three rules a later implementation must
follow, and the review corrections are folded into the text below.

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

An `items` row carries one timestamp, `last_transaction_ts`. Every committed operation writes it: a put, an
update, a delete, and a `check`. `TransactionParticipant.prepareLocal` rejects a transaction when its timestamp
is not above that value. It is the test of Listing 3 of the ATC 2023 paper:

```
AND item.timestamp < input.timestamp
```

Each rejection returns to the client, and the client retries the whole transaction. Under contention on one
item, this costs one extra round trip for every rejected transaction.
`docs/agent-plans/dynamodb-distributed-transactions-plan.md` records this cost as a FIXME.

Section 4 of the paper removes some of these rejections. It states its base: "The classic timestamp ordering
concurrency control scheme [4, 13] can be extended with novel optimizations when applied to a key-value store
where reads and writes of individual items are mixed with multi-item transactions."

Classic timestamp ordering keeps two timestamps for each item. One records the last read, and one records the
last write. This partition keeps one number for both. That single fact blocks every write optimization of
section 4.

### 1.2 The two rules, and why one number blocks them

Classic timestamp ordering guards an item with three rules. This document uses these names for them, and it
names the two timestamps by the columns of section 4.2.1.

**The read rule.** A partition rejects a transaction `T` that writes an item when the timestamp of `T` is not
above `last_read_ts`. A committed operation with a later timestamp already read the item. The write of `T`
belongs before that operation in the serial order, so it would change what that operation read. `T` must abort,
and no other outcome is correct.

**The reader rule.** A partition rejects a transaction `T` that reads an item when the timestamp of `T` is not
above `last_write_ts`. Prepare evaluates a condition and an `update` against the current row, so `T` would read
a value that a later write stored. Section 1.3 lists the operations that read. This rule is why the Thomas
Write Rule below applies only to an unconditional put and an unconditional delete: those two are the only
operations that read nothing.

**The write rule.** A partition accepts a transaction `T` whose operation on an item is an unconditional put or
an unconditional delete when the read rule passed and the timestamp of `T` is not above `last_write_ts`. The
commit then discards the write of `T` instead of applying it, because a later full overwrite already replaced
it. This is the Thomas Write Rule. Without the reader rule it is unsafe: a conditional put at t=15 whose
condition passed on the value that a put at t=30 stored has no valid serial order. Before t=30 its condition
must fail, and after t=30 its write must apply.

The paper states the rule for "a write operation that is part of a transaction" and restricts only the newer
write, which must be "either an individual put or transactional put operation" and not a modify. It does not
say what happens to a precondition on the older transaction. Its Listing 3 evaluates the conditions against the
current item before the timestamp test, so a relaxation of the timestamp test alone opens the hole above. The
reader rule closes it, and it matches how the paper treats a checked condition on the singleton write path,
where only "a put or delete operation that has no precondition" may take a later timestamp.

Today `prepareLocal` runs none of the three rules. It rejects a transaction when its timestamp is not above
`last_transaction_ts`, which is one number for both. That is correct, and it is the conservative merge of the
rules: it rejects every case that the read rule and the reader rule reject, and it also rejects every case
that the write rule would accept and discard.

The main optimization of section 4 is the Thomas Write Rule. It accepts a transaction with an old timestamp and
then discards its write:

> Write transactions can be accepted even with an old timestamp. If a write operation that is part of a
> transaction arrives at a storage node that has already performed a write (either an individual put or
> transactional put operation) with a later write timestamp, this transaction can still be accepted and enter
> the prepared state. If this transaction is committed, its write operation is ignored with the observation
> that, even if performed earlier, it would have been completely overwritten by the later put operation.

The rule has a premise and a safety condition.

- The premise is that a newer write exists. The timestamp of `T` is below the last write timestamp.
- The safety condition is that nothing read the item after `T`. The timestamp of `T` is above the last read
  timestamp.

One number makes the safety condition the opposite of the premise. The rule runs only when the premise holds,
and the safety condition then always fails. The store must always abort, so the optimization delivers nothing.

The three rules therefore ship together, and this document calls that change "the read rule ships".
`prepareLocal` tests `last_read_ts` and `last_write_ts` separately, and the commit gains the branch that
discards a write. Section 2.2 keeps that change out of scope. This RFC adds the timestamps the rules read.

The rules apply to the two-phase path only. The single-shot path stamps the partition's own clock and runs no
timestamp test, and section 1.5 gives the reason. A non-transactional write also stamps the partition's own
clock, so it never arrives with an old timestamp. Both are the "later write" side of the write rule, never the
older side.

### 1.3 Every operation is a read, a write, or both

The whole design follows from one classification.

| Operation | Reads the item | Writes the item |
| --- | --- | --- |
| A put with no condition | no | yes |
| A put with a condition | yes | yes |
| An `update` | yes | yes |
| A delete with no condition | no | yes |
| A delete with a condition | yes | yes |
| A `check` | yes | no |

An `update` reads the item because it computes its result from the pre-image. A put or a delete with a
condition reads the item because the condition decides if its operation commits. A `check` reads the item and
writes nothing.

A delete leaves no row, so its read has no row to land on. Section 4.2.3 states how the delete watermark
carries it.

This classification answers the caveat that the paper attaches to the rule:

> This argument does not hold if the last write was a modify operation that partially updated the item's
> contents.

The caveat needs no separate flag. A partial write is always also a read, so it raises the read timestamp. The
read rule then rejects the old transaction before the store reaches the Thomas Write Rule. The paper states the
same for the second optimization, and the same mechanism covers it:

> Transactions with modify operations that perform partial updates must execute in their assigned timestamp
> order since the final value of the item depends on the sequence of execution.

Five histories show the three rules together. Each history ends at t=30, and a transaction arrives with t=15.

| History | Read ts | Write ts | Operation at t=15 | Outcome for t=15 |
| --- | --- | --- | --- | --- |
| A put with no condition at t=30 | 0 | 30 | A put with no condition | Accept, then discard the write |
| A put with no condition at t=30 | 0 | 30 | A put with a condition, or an `update` | Reject, by the reader rule |
| A put with a condition at t=30 | 30 | 30 | Any | Reject |
| An `update` at t=30 | 30 | 30 | Any | Reject |
| A put at t=10, then a `check` at t=30 | 30 | 10 | Any write | Reject |

### 1.4 The optimizations and the columns each one needs

Section 4 of the paper holds seven optimizations. This section maps each one to the state it needs. The table
is the reason this RFC adds a column that no code reads yet.

| Optimization from section 4 | State it needs | Status |
| --- | --- | --- |
| A singleton read succeeds against a prepared write | none | Built |
| A transaction that writes one partition runs in one round | none | Built, in `executeSingleShot` |
| A singleton write is serialized before prepared transactions | The conditions and timestamps of each prepared row | In `pending_transactions` |
| A singleton write waits for a prepared transaction | A queue of deferred writes | A new table |
| A prepare is accepted with an old timestamp | `last_write_ts`, `last_read_ts`, `max_absent_read_ts` | **This RFC** |
| More than one transaction is prepared on one item | `last_write_ts`, `last_read_ts` | **This RFC** |
| A read transaction runs in one round | `last_read_ts`, written by every read | Out of scope, section 5.3 |

Three facts follow.

1. Two optimizations need the same three values, and no other optimization needs a column on `items`. The union
   of the columns is therefore small and fixed: `last_write_ts` and `last_read_ts` on `items`, and
   `max_absent_read_ts` on `partition_metadata`.
2. The optimizations this RFC does not build need no column that this RFC does not add. The buffered write
   needs a table, and a table can be added at any time. The single-round read transaction needs `last_read_ts`,
   which this RFC adds. Only its write rate is out of scope, and section 5.3 gives the reason.
3. The remaining optimizations are already built, or their state already lives in `pending_transactions`.

The columns therefore cover every optimization of section 4, and section 1.6 states why they must land before
the rules that read them.

### 1.5 What the reader must know about the current system

- `PartitionStore` owns all SQL on `items`. The schema lives in the migration list of
  `packages/fokosdb/src/shared/partition/partition-store.ts`.
- `items` is a `STRICT` rowid table. `data` is the last column, and `idx_items_scan (hk, sk, est_row_bytes)`
  covers the size scans.
- `last_transaction_ts` is monotonic per item. Every writer applies `MAX(last_transaction_ts, ?)`.
- `PartitionStore.bumpItemLastTransactionTs` is the `check`. It moves the timestamp and writes no contents. On
  an absent item it matches no row and records nothing.
- A deleted item leaves no row. The partition keeps one watermark, `deletion_metadata.max_deleted_ts`, and
  `prepareLocal` compares against it when the item is absent.
- Four paths remove a row. The two-phase delete passes `prepareLocal`, so its timestamp is above the timestamp
  of the row. The single-shot delete in `executeSingleShot` and the non-transactional delete in
  `PartitionDO.apiDeleteItem` run no timestamp test and stamp the watermark with the partition's own clock.
  The TTL sweep in `PartitionStore.deleteExpiredItems` stamps the watermark with the expiry instant of the rows
  it removes. All of them discard the timestamps of the row. The fourth path is the promotion GC in
  `PartitionStore.deleteItemsBatchForHashKey`, which removes the rows of a promoted hash key from the parent.
  It touches no watermark, and section 4.2.3 states why it must not.
- Reads do not filter expired rows. An expired row stays readable until the sweep removes it, and the sweep
  skips a row that holds a pending lock.
- A write transaction runs on one of two paths. The two-phase path locks each item in `prepareLocal` and
  applies the stored payload in `commitLocal`. The single-shot path runs the whole set in `executeSingleShot`.
- The single-shot path stamps the partition's own clock and runs no `timestamp_conflict`, `clock_skew`, or
  watermark test. That is by design, and `docs/agent-plans/2026-08-23-single-partition-transaction-fast-path.md`
  records the reason: one Durable Object validates and applies the whole set inside one storage transaction,
  so its serial position is its execution order. The store applies `MAX`, so a stamp below the timestamps of
  the row is absorbed. Adding the prepare tests to this path would reject every single-shot transaction on an
  item that a coordinator clock ahead of this partition wrote, for the whole skew window, and would protect
  nothing. The paper's single-round path keeps the coordinator timestamp and runs the prepare tests; this
  design differs on purpose.
- Serializability today comes from the locks. Every item of a two-phase transaction is locked from prepare
  to commit, `check` items included. The timestamp test in `prepareLocal` adds rejections on top of the locks
  and is not what makes the commit order serializable. The section "Why this RFC is deferred" draws the
  consequence.
- `FokosDB.transactGetItems` runs a two-phase writeless protocol. It writes nothing to any partition. Between
  its two phases it compares the version counter `v` and `lastCommittedTs`, which
  `TransactionParticipant.readForTransactionLocal` reads from `last_transaction_ts`.
- A split copies rows to a child through `MigratedItem` and the `migrationGetItemsBatch` RPC. The child writes
  each row with `PartitionStore.insertItemIfAbsent`. The child then inherits `max_deleted_ts` through the
  `migrationGetPartitionTransactionMetadata` RPC.
- The project is before its first release. A schema change edits the existing migration in place.

### 1.6 Why the columns must land before the rules

This RFC adds the timestamps. Beyond the one comparison of M0, it does not change `prepareLocal` or the
commit. Two reasons make the columns worth adding first.

1. **The position of a column.** `data` must stay the last column of `items`. A `SELECT` of a metadata column
   reads the record until it satisfies that column, so a column after `data` must traverse the `data` payload
   and its overflow pages. `ALTER TABLE items ADD COLUMN` appends after `data`. To place a column correctly
   later, a migration must build a new table, copy every row, drop the old table, and rename. That runs once
   per Durable Object.
2. **The value must be correct from the first row.** A rule that reads the read timestamp trusts every row the
   partition holds, and every watermark. A column that starts to record reads at some later date gives no
   answer for the rows written before that date.

## 2. Goals and Requirements

### 2.1 In scope

- `items` holds `last_read_ts`, which records the last committed operation that read the contents of the row.
- `items` renames `last_transaction_ts` to `last_write_ts`. A write sets it. The `check` also sets it until the
  read rule ships, and section 4.2.2 gives the reason.
- A put with a condition, a delete with a condition, an `update`, and a `check` are reads. Each one records its
  read on the row when a row remains, and on a watermark when no row remains. This includes the
  non-transactional delete with a condition that observes an absent item: it removes nothing today and
  raises no watermark, and it must raise `max_absent_read_ts`.
- `deletion_metadata` becomes `partition_metadata` and holds `max_absent_read_ts` beside `max_deleted_ts`.
- A `check` on an absent item raises `max_absent_read_ts`, and `prepareLocal` compares an absent item against
  both watermarks. This is the hotfix of M0.
- Every path that removes a row raises both watermarks to at least the timestamps of the row it removed.
- `readForTransactionLocal` reads `lastCommittedTs` from `last_write_ts`.
- `MigratedItem` carries `last_read_ts`, the `migrationGetPartitionTransactionMetadata` RPC carries
  `max_absent_read_ts`, and a split preserves both timestamps of every row and both watermarks.

### 2.2 Out of scope

- **The relaxed prepare and the commit that discards a write.** `prepareLocal` must run the three rules of
  section 1.2, and the commit apply of `TransactionParticipant` must discard the write of a superseded
  transaction. Three sources give `prepareLocal` the item stamp, and each must carry both values:
  `PartitionStore.getItemStamp`, `ConditionEvaluationResult`, and `UpdateProbeResult`. None of that is a
  schema change, so none of it constrains this RFC. The rename of M1 does reach two of those sources, and
  section 3 lists the files.
- **More than one prepared transaction on one item.** `pending_transactions` can hold more than one row for one
  key, because its primary key is `(hk, sk, transaction_id)`. `PartitionStore.pendingLockFor` and
  `prepareLocal` both assume at most one row. That change rewrites the lock model, and it needs no new column.
- **The buffered write.** Section 4 also describes a storage node that holds a single-item write until the
  prepared transaction completes. That needs a queue, which is a new table.
- **The single-round read transaction.** Section 5.3 gives the reason. `transactGetItems` stays writeless and
  writes no read timestamp.
- **Any change to the public API.** Both timestamps are internal. `FokosDB` projects each query row field by
  field, and `apiGetItem` builds its own response shape.

### 2.3 Requirements that constrain the solution

- `data` must stay the last column of `items`. Both timestamps must come before it.
- Neither timestamp must join `idx_items_scan`. That index must stay covering for the `est_row_bytes` scans,
  and a query-plan test asserts the plan.
- Both timestamps and both watermarks must be monotonic. Every writer applies `MAX`.
- A read of a present row must not add a statement to any path. Each such operation already writes the row,
  so the column joins a statement that runs today. The `check` on an absent item is the one exception, and it
  adds one `UPDATE` on `partition_metadata`.
- A path that removes a row must read the timestamps of that row in the `DELETE` it already runs, with
  `RETURNING`, and must not add a `SELECT`. `RETURNING` reports the returned row as one more billed read, as
  the comment on `PartitionStore.#storedEstRowBytes` records. `deleteItem` must therefore return
  `est_row_bytes` in the same `RETURNING` and drop its covering-index pre-read, so a delete that finds a row
  costs what it costs today, and a delete that finds no row costs one more read. The TTL sweep already uses
  `RETURNING` and does not change.
- The default of each timestamp must be `0`, which is the value that admits every transaction.

## 3. Milestones

### M0 — The hotfix for the `check` on an absent item

`deletion_metadata` gains `max_absent_read_ts`. `PartitionStore.bumpItemLastTransactionTs` raises it with the
transaction timestamp when its `UPDATE` on `items` matches no row. The absent branch of `prepareLocal` rejects
a transaction whose timestamp is not above either watermark. `GetPartitionTransactionMetadataResult` carries
`maxAbsentReadTs` beside `maxDeletedTs`, and both child migration loops in `SplitMigration` raise both
watermarks.

M0 runs on the current schema and ships on its own. Section 4.2.3 states the gap it closes: a `check` on a
present item orders every later write behind itself, and a `check` on an absent item does not.

M0 is deferred with the rest of this RFC. It changes no outcome that a client can observe, because the locks
already make the commit order serializable, and it adds a watermark that only rejects. It ships on the day
the read rule ships, and not before. It does not add the prepare tests to `executeSingleShot`; section 1.5
gives the reason.

### M1 — The columns, the writers, and the split

`items` gains `last_read_ts` and renames `last_transaction_ts` to `last_write_ts`. `deletion_metadata` becomes
`partition_metadata`, and `recordCheck` takes over the writer of M0. Every writer of `PartitionStore` sets the
timestamps of section 4.2.2. Every path that removes a row raises both watermarks from the row it removed.
`readForTransactionLocal` reads `lastCommittedTs` from `last_write_ts`, and the comment on the `check` in
`TransactionParticipant` states the reason of section 4.2.2.

`MigratedItem` carries `last_read_ts`, `PartitionStore.insertItemIfAbsent` writes it, and the
`migrationGetItemsBatch` RPC carries it. The watermark already travels since M0.

The rename reaches beyond the store. `composeConditionStatement` in `src/shared/expression/plan.ts` and
`composeUpdateProbeStatement` in `src/shared/expression/runtime.ts` select `i.last_transaction_ts` by name,
and `ConditionEvaluationResult` and `UpdateProbeResult` carry it as `lastTransactionTs`. An M1 that renames
the column and leaves those statements fails every conditional prepare with a SQL error. The compiled plan
that the client ships does not name the column, so the plan version does not change.

The split is part of M1 and not a milestone of its own, because a build that writes `last_read_ts` and does
not copy it writes a zero read timestamp into every child.

M1 delivers correct timestamps and watermarks for every row a partition writes, removes, or receives from a
parent.

## 4. Proposed Solution

### 4.1 High-level overview

An `items` row splits its one timestamp into two.

```
items row
  hk, sk                 the key
  data, data_kind        the value
  v                      the version counter
  last_write_ts          WHEN a write last changed the contents      (was last_transaction_ts)
  last_read_ts           WHEN an operation last read the contents    (new)
  ttl_epoch_utc_seconds  the expiry
  est_row_bytes          the stored size
```

Each operation sets the timestamps that section 1.3 assigns to it. A put with no condition sets the write
timestamp. A `check` sets the read timestamp. A put with a condition and an `update` set both.

An absent item has no row, so the partition keeps the same two values for every absent item together, as two
watermarks. `max_deleted_ts` is the write timestamp of the absent items, and `max_absent_read_ts` is their read
timestamp. A delete raises them from the row it removes, and a `check` on an absent item raises the read
watermark.

Only the absent branch of `prepareLocal` reads `max_absent_read_ts`, from M0 on. Nothing reads `last_read_ts`
in this RFC. Section 1.4 lists the optimizations that will, and section 1.6 gives the reason to write it now.

### 4.2 Technical details

#### 4.2.1 The schema change

The change edits the `items` migration and the `deletion_metadata` migration in place:

```sql
CREATE TABLE IF NOT EXISTS items (
    hk                    BLOB    NOT NULL,
    sk                    BLOB    NOT NULL DEFAULT x'',
    data_kind             INTEGER NOT NULL DEFAULT 0,
    v                     INTEGER NOT NULL,
    last_write_ts         INTEGER NOT NULL DEFAULT 0,
    last_read_ts          INTEGER NOT NULL DEFAULT 0,
    ttl_epoch_utc_seconds INTEGER,
    est_row_bytes         INTEGER NOT NULL,
    data                  ANY     NOT NULL,

    PRIMARY KEY (hk, sk)
) STRICT;

CREATE TABLE IF NOT EXISTS partition_metadata (
    id                 INTEGER PRIMARY KEY CHECK (id = 1),
    max_deleted_ts     INTEGER NOT NULL DEFAULT 0,
    max_absent_read_ts INTEGER NOT NULL DEFAULT 0
) STRICT;
```

The index list of `items` does not change.

The rename of `last_transaction_ts` is mechanical, and it pays for itself. `last_write_ts` and `last_read_ts`
name the two halves of the same rule, and a symmetric pair is harder to confuse than a name that says
"transaction" for a value that only a write sets.

The rename of `deletion_metadata` follows the same reason. The table holds two watermarks after this change,
and only one of them is about a delete.

#### 4.2.2 The writers

| Store method | Operation it applies | Sets `last_write_ts` | Sets `last_read_ts` | Raises a watermark |
| --- | --- | --- | --- | --- |
| `upsertItem` | a put with no condition | yes | no | — |
| `upsertItem` | a put with a condition | yes | yes | — |
| `upsertItem` | the commit apply of an `update` | yes | yes | — |
| `updateItemSingleShot` | an `update` | yes | yes | — |
| `recordCheck` | a `check` on a present item | yes, until the read rule ships | yes | — |
| `recordCheck` | a `check` on an absent item | — | — | `max_absent_read_ts` from the `check` |
| `deleteItem` | a delete that removes a row | — | — | both, section 4.2.3 |
| `deleteItem` | a transactional delete on an absent item | — | — | `max_deleted_ts` from the delete, as today |
| `deleteItem` | a non-transactional delete with a condition on an absent item | — | — | `max_absent_read_ts` from the delete |
| `deleteExpiredItems` | the TTL sweep | — | — | both, section 4.2.3 |
| `deleteItemsBatchForHashKey` | the promotion GC | — | — | none, section 4.2.3 |
| `insertItemIfAbsent` | the migration ingest | the value of the source row | the value of the source row | — |

`recordCheck` replaces `bumpItemLastTransactionTs`. It runs the same `UPDATE` on `items` with both columns.
When that statement writes no row, the item is absent, and the method raises `max_absent_read_ts` with the
same timestamp. The pending lock keeps the presence of the item stable between prepare and commit: a
non-transactional put or delete is rejected on a locked item, and the TTL sweep skips a locked row. The
presence that the commit apply observes is therefore the presence that prepare evaluated.

`upsertItem` cannot tell a conditional put from an unconditional put, so it takes a flag from its caller.
`PartitionDO.apiPutItem` knows if the request carries a condition. The commit apply of `TransactionParticipant`
reads `pendingRow.operation` and `pendingRow.conditions_json`, and `PartitionStore.getPendingTxOp` must add
`conditions_json` to its select, because it does not return that column today.
`TransactionParticipant.executeSingleShot` reads `item.operation` and `item.condition`.

The `check` on a present item sets both timestamps. Section 3.3 of the paper does the same with its one
timestamp: "Items for which a precondition was checked but that are not being written also have their
timestamps updated", and Listing 4 sets `item.timestamp` on every commit. The write bump is what orders a
`check` against a later write while `prepareLocal` reads one timestamp. Without it, this sequence puts a write
before a read that already observed the item:

1. An item holds `last_write_ts = 10`, and it holds the value V.
2. A transaction commits at t=30. It runs a `check` that reads V, and it writes a second item.
3. A transaction arrives at t=20 and writes the first item. Prepare compares 20 against `last_write_ts` of 10,
   and it accepts.

The timestamp order holds t=20 before t=30, so in that order the `check` must read the value that t=20 wrote.
It read V. The bump makes prepare reject t=20, which is what the read rule will do from `last_read_ts`.

The bump comes off when `prepareLocal` starts to test both timestamps. Until then the `check` writes a value to
`last_write_ts` that no write produced. That is conservative and it is safe: the Thomas Write Rule discards a
write only when the timestamp of `T` is also above `last_read_ts`, and a `check` raises both together.

The bump does not serve `transactGetItems`, and the comment on the `check` in `TransactionParticipant` must
stop saying that it does. A `check` between the two read phases changes no contents. If the same transaction
also wrote an item in the read set, the version counter `v` of that item changes and the second phase
rejects. If it wrote only items outside the read set, the read transaction serializes before it. The paper's
read protocol in section 3.4 compares a log sequence number of the last write and never reads the timestamp.
`readForTransactionLocal` therefore reads `lastCommittedTs` from `last_write_ts`, and nothing changes for read
transactions when the bump comes off.

Every write applies `MAX`, so neither timestamp moves backwards. The reason is the reason `last_transaction_ts`
has today: the two writers read different clocks, and prepare accepts a coordinator clock up to
`MAX_CLOCK_SKEW_MS` ahead of this partition's clock.

#### 4.2.3 The absent item and the watermarks

A read of an absent item has no row to record on, and a removed row takes its timestamps with it. The partition
keeps two watermarks for both cases. This mirrors `max_deleted_ts`, and the paper gives the reason for that
shape:

> Rather than maintaining tombstones for deleted items, which would incur both a high storage cost and garbage
> collection cost if items are frequently created and deleted, DynamoDB stores a partition-level max delete
> timestamp.

**A `check` on an absent item.** A `check` with `attribute_not_exists` observes an absent item and commits.
`recordCheck` writes no row and raises `max_absent_read_ts` with the transaction timestamp. A put with a
condition that observes an absent item creates the row in the same commit, and the new row carries the read in
`last_read_ts`. A transactional delete with a condition that observes an absent item removes nothing, and the
delete watermark orders every later write behind the delete itself, because both transactional paths pass
`bumpWatermarkAlways`. The non-transactional delete does not: `deleteItem` skips both watermarks when it
removes no row and the flag is not set. A non-transactional delete with `attribute_not_exists` at local t=45
then records nothing, and a transaction at t=40 from a coordinator clock behind this partition creates the
item under it. That path must raise `max_absent_read_ts` with its own timestamp when it carries a condition
and removes no row.

**A removed row.** Every path that removes a row reads `last_write_ts` and `last_read_ts` of that row with
`RETURNING` in the `DELETE` it already runs. One `UPDATE` on `partition_metadata` then raises `max_deleted_ts`
to at least the delete timestamp and the `last_write_ts` of the row, and `max_absent_read_ts` to at least the
`last_read_ts` of the row. The TTL sweep applies the `MAX` over its batch and runs the `UPDATE` once per batch,
as it does today.

Only the two-phase delete carries a timestamp above the timestamps of the row, because only it passes
`prepareLocal`. The other paths do not:

- The single-shot delete and the non-transactional delete stamp the partition's own clock, and a coordinator
  clock can run ahead of it by up to `MAX_CLOCK_SKEW_MS`. A `check` at t=50 sets `last_read_ts = 50`. A
  non-transactional delete at a local t=45 removes the row. Without the row's value, `max_deleted_ts` holds
  45, and a transaction at t=48 that creates the item is accepted. In timestamp order t=48 comes before the
  read at t=50, and it changes what the `check` observed.
- The TTL sweep stamps the expiry instant, and reads do not filter expired rows. A `check` at t=5000 reads a row
  that expired at t=1000. The sweep removes the row and raises `max_deleted_ts` to at most 1000. A transaction
  at t=3000 that creates the item is accepted, with the same result.

Raising both watermarks from the removed row closes both cases. The same gap exists for `last_write_ts` today,
and the same rule closes it. A watermark that rises above the delete timestamp is one behaviour change of this
RFC, and it only rejects more transactions than the code rejects today.

**The promotion GC must not raise the watermarks.** `deleteItemsBatchForHashKey` removes the rows of a
promoted hash key from the parent, in batches of 1000, after the promoted partition owns the key. The parent
never serves a prepare for that key again. If the GC raised the parent's watermarks from those rows, it would
raise them to the maximum timestamps of thousands of rows the parent no longer owns, and every later create
under a different key in the parent with a timestamp not above that maximum would be rejected. The promoted
partition inherits `max_deleted_ts`, and after M0 `max_absent_read_ts`, through
`migrationGetPartitionTransactionMetadata`.

**The comparison lands in M0.** `prepareLocal` compares an absent item against `max_deleted_ts` only today,
and Listing 3 of the paper does the same. After M0 the absent branch rejects a transaction whose timestamp is
not above either watermark, with the same `timestamp_conflict` reason.

M0 is a hotfix against the intent of the current code, not against serializability. The `check` on a present
item bumps the timestamp so that, as its comment states, it "still orders this transaction against later
ones". The `check` on an absent item records nothing, so a later transaction with a lower timestamp creates the
item under it. The code orders one case and not the other, and M0 makes the two cases equal.

The sequences above and in section 4.2.2 violate timestamp order, but they do not violate serializability
today: every conflicting pair is ordered by the pending lock, which is held from prepare to commit, so the
real-time commit order is always a valid serial order. Timestamp order becomes the serial order only when the
Thomas Write Rule discards a write, because a discarded write forces its transaction before the write that
superseded it. M0 therefore rejects transactions that the code accepts today, and each one is a transaction
that the read rule will reject on the day it ships.

A partition-level watermark is coarse, so it rejects some transactions that a per-item value would accept. Only
a removal and a `check` that observed an absent item raise it, which keeps the rate low. The paper reports the
same tradeoff for the delete watermark: "in practice, an insignificant percentage of transactions are cancelled
due to the transaction's timestamp being lower than the partition's maximum delete timestamp."

#### 4.2.4 The split and the migration path

`MigratedItem` carries `last_read_ts` beside the write timestamp, `insertItemIfAbsent` writes both, and the
`migrationGetItemsBatch` RPC carries both. `GetPartitionTransactionMetadataResult` carries `maxAbsentReadTs`
beside `maxDeletedTs`, and both child migration loops in `SplitMigration` raise both watermarks with `MAX`.
This changes the shape of two RPC payloads. Both sides of each RPC ship in one build, so no compatibility
window exists.

A split that dropped either value would write `0` into every child. A child would then accept a transaction
that the parent rejected, which loses serializability once the rules read the values. Each value is one integer
the parent already reads, so the copy costs nothing.

#### 4.2.5 Performance

The change adds one integer per row and one integer to a single metadata row. SQLite stores the value `0` with
no payload byte and one header byte, so a row that never leaves the default grows by 1 byte.

`EST_ROW_BYTES_K` stays at 100. The migration comment describes K as the fixed per-row remainder, and it names
the wide integer columns as part of that remainder. One more small integer stays inside the value.

Two paths gain a statement: the `check` on an absent item, which runs one `UPDATE` on `partition_metadata`
after the `UPDATE` on `items` matched no row, and the non-transactional delete with a condition on an absent
item, which runs the same `UPDATE`. Every other operation that sets `last_read_ts` already writes the row, so
the column joins a statement that runs today. The removal paths read the timestamps with `RETURNING` in their
existing `DELETE`, and they already run the watermark `UPDATE`. `RETURNING` costs one billed read, so
`deleteItem` folds its `est_row_bytes` pre-read into it, as section 2.3 requires: a delete that finds a row
costs the same as today, and a delete that finds no row costs one more read. `transactGetItems` writes
nothing, before and after this change.

#### 4.2.6 Deployment and rollback

The project is before its first release, so the change edits the existing migrations in place. A Durable Object
that already holds data runs `CREATE TABLE IF NOT EXISTS`, which does not change an existing table. Any local
state under `.wrangler/` must be deleted before the change is tested against it.

To roll back, revert the commit and delete the local state again. No data conversion runs in either direction.

#### 4.2.7 Testing

- Each write path of section 4.2.2 sets the timestamps that the table assigns to it.
- A `check` on a present item moves both timestamps, so every test that asserts the current `check` behaviour
  still passes.
- A `check` on an absent item raises `max_absent_read_ts` and leaves `max_deleted_ts` alone, on both the
  two-phase path and the single-shot path.
- A transaction that writes an absent item with a timestamp not above `max_absent_read_ts` is rejected with
  `timestamp_conflict`, and one with a timestamp above both watermarks is accepted.
- A put with a condition moves both timestamps. A put with no condition moves only `last_write_ts`. A put with
  a condition on an absent item creates a row that carries the read.
- The non-transactional delete, the single-shot delete, the two-phase delete, and the TTL sweep each raise
  `max_deleted_ts` to the `last_write_ts` of a removed row and `max_absent_read_ts` to its `last_read_ts` when
  those values are above the delete timestamp.
- A non-transactional delete with a condition on an absent item raises `max_absent_read_ts` and leaves
  `max_deleted_ts` alone. One without a condition on an absent item raises neither.
- The promotion GC leaves both watermarks of the parent unchanged.
- A single-shot transaction on an item that a coordinator clock ahead of the partition wrote is accepted.
- `readForTransactionLocal` returns `last_write_ts` as `lastCommittedTs`.
- `insertItemIfAbsent` stores both timestamps of the source row.
- A split copies both timestamps and both watermarks, and a read on the child returns the values the parent
  held.
- The query-plan test for `idx_items_scan` still reports the covering index.

### 4.3 Open Questions

- The rate of `timestamp_conflict` rejections under a contended workload is not measured. The RFC stays
  deferred until it is. Section 6 states what the measurement decides.

## 5. Alternative Options

### 5.1 A `last_op` column that records the write class

Store a code on each row that says if the last write replaced the whole item or changed part of it. The Thomas
Write Rule then discards a write when the code says the last write was a full overwrite.

Rejected. The code answers only the caveat of section 1.3, and it answers it in a place that cannot hold the
whole answer. A code records the last operation, so a `check` or a conditional put disappears behind any later
put. The read timestamp holds the same fact for every operation and for every order of operations, and it makes
the code redundant: a partial write is also a read, so the read rule rejects the transaction first.

### 5.2 Keep one conflated timestamp

Reject a transaction when its timestamp is not above `last_transaction_ts`, as the partition does today.

Rejected. Section 1.2 shows that one number makes the safety condition of the Thomas Write Rule the opposite of
its premise. This is the correct behaviour, and it forecloses every write optimization of section 4.

### 5.3 Write the read timestamp from every read

Section 4 also describes a read transaction that completes in one round instead of two. That protocol needs
every read to write the read timestamp.

Rejected. Section 3.4 of the same paper records that DynamoDB refused this design: "Updating this timestamp for
operations in a read transaction would have turned every read into a more costly write operation on persistent,
replicated data. To avoid this latency and cost, DynamoDB devised a two-phase writeless protocol for executing
read transactions." Section 4 opens by stating that some of its techniques are not built: "We have implemented
some of these techniques in DynamoDB and others we plan to integrate as we hear more feedback from our
customers."

`FokosDB` runs the two-phase writeless protocol that DynamoDB ships. A read transaction over N partitions would
turn from 2N reads into N reads plus N writes. This RFC writes `last_read_ts` only from operations that already
write the row, so it takes the column and not the cost.

### 5.4 A tombstone for each deleted item

Keep a row for a deleted item so that it can hold both timestamps.

Rejected. Section 4.2.3 gives the watermark design and quotes the storage cost that the paper names.

### 5.5 Land the comparison against `max_absent_read_ts` with the read rule

Write the watermark in M1 and add the comparison to the absent branch of `prepareLocal` only when the read rule
ships. Section 4.2.3 shows that the sequences it rejects are serializable today, so nothing breaks in the
meantime.

Rejected. The current code already orders a `check` on a present item against later writes, and the absent
case is the one place where that intent fails. The comparison is one line, it needs no new column on `items`,
and it ships on its own as M0. Landing it early also means that every rejection it adds is measured before
the read rule changes the rejection rate again.

## 6. Frequently Asked Questions

**Does a non-transactional put set the same timestamps as a transactional put?**

Yes. The paper puts "an individual put or transactional put operation" in one class. What matters is whether
the operation carries a condition, not which path delivered it.

**Does a `check` still set the write timestamp?**

Yes, until the read rule ships. A `check` writes no contents, so the write timestamp is not the value that
belongs to it. That bump is the only thing that orders a `check` against a later write while `prepareLocal`
reads one timestamp, and the paper's Listing 4 does the same. Section 4.2.2 gives the sequence that the bump
rejects, and it states why read transactions do not depend on the bump.

**Does this RFC change the behaviour of the partition?**

Two changes, and both only reject more transactions than the code rejects today.

1. M0 rejects a transaction that writes an absent item with a timestamp not above `max_absent_read_ts`.
2. A removal raises `max_deleted_ts` to at least the `last_write_ts` of the removed row, so a
   non-transactional delete or a TTL sweep can move the watermark above its own timestamp.

Section 4.2.3 gives the sequences each one rejects. Every other write path sets one more value that no rule
reads.

**Why is `0` the default of each timestamp?**

`0` admits every transaction, because every transaction timestamp is above it. A row that misses a value
therefore behaves as a row that nothing has read, which matches an item that no operation has touched.

**How much does this save?**

It unlocks the two write optimizations of section 4. Each removes a class of prepare rejection, and each
rejection costs the client one retry of the whole transaction. The Thomas Write Rule removes only the
rejections where the older transaction carries an unconditional put or delete on the item and the newer write
was a full overwrite. `TODO: measure` the rate of `timestamp_conflict` rejections under a contended workload,
against the rate of `pending_conflict`, so the saving has a number. If `timestamp_conflict` is a small fraction
of `pending_conflict`, this RFC stays deferred and the lock-model change of section 2.2 is the one worth a
design.

**Should `executeSingleShot` run the prepare tests?**

No, not while it stamps the partition's own clock. Section 1.5 gives the reason. The tests order
coordinator-stamped transactions that arrive out of timestamp order. The single-shot path has no coordinator
timestamp, and its serial position is its execution on one Durable Object. If the fast path ever adopts the
coordinator timestamp, it must run every prepare test in its check pass, `clock_skew` included, and that is a
separate RFC.

**Does the current code need M0 if this RFC never ships?**

No. The locks make the commit order serializable, and M0 only makes the timestamp order complete. A `check`
at t=30 that observes K absent, followed by a transaction at t=20 that creates K, commits in the real-time
order and that order is a valid serial order. No client can observe the timestamps.

## 7. References

- `docs/agent-plans/dynamodb-distributed-transactions-plan.md`
- `docs/agent-plans/2026-08-23-single-partition-transaction-fast-path.md`
- `docs/agent-plans/2026-09-02-update-expressions.md`
- `docs/research/atc23-idziorek-dynamodb-transactions.md`, the text of the paper
- [Distributed Transactions at Scale in Amazon DynamoDB (USENIX ATC 2023)](https://www.usenix.org/system/files/atc23-idziorek.pdf)
- [SQLite ALTER TABLE](https://sqlite.org/lang_altertable.html)
