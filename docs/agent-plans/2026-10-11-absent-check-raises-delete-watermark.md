# RFC — A committed check on an absent item raises the delete watermark of its partition

**State:** Draft
**Date:** 2026-10-11
**Author:** Lambros Petrou
**Status:** Not built. Today the commit of a `check` on an absent item records no timestamp.

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

Each two-phase write transaction has one transaction order timestamp. Its coordinator assigns the timestamp one
time, when it creates the transaction. Each later drive of the transaction uses the stored value.

A partition keeps three order watermarks:

- `last_read_ts` of an item row: the highest timestamp of a committed `check`, put or update of the item.
- `last_write_ts` of an item row: the highest timestamp of a committed put or update of the item.
- `max_delete_tx_order_ts` of the partition: one value in `deletion_metadata` for all keys that have no row. This
  document names it the delete watermark.

`TransactionParticipant.#evaluate` rejects a two-phase prepare with `timestamp_conflict` when the transaction
timestamp is not above the watermark of the item:

| The item at prepare | Operation | Watermark |
| --- | --- | --- |
| Has a row | put, update, delete | `last_read_ts` |
| Has a row | `check` | `last_write_ts` |
| Has no row | all operations | `max_delete_tx_order_ts` |

At commit, each operation records the transaction timestamp `T`:

| Operation | What the commit records |
| --- | --- |
| put, update | `last_read_ts` and `last_write_ts` of the row, with `MAX` |
| delete | `max_delete_tx_order_ts`, with `MAX`, also when the item has no row |
| `check`, the item has a row | `last_read_ts` of the row, with `MAX` |
| `check`, the item has no row | nothing |

The last row of the table is the subject of this document. `PartitionStore.bumpItemReadTs` is an `UPDATE` on
`items`. When the item has no row, the statement changes nothing.

### 1.2 The problem: the timestamp order is not a serial order

1. Item `X` is absent in partition P1. The delete watermark of P1 is 1.
2. Transaction A has timestamp 5. It has a `check` that `X` does not exist, on P1, and a put of `Y`, on P2.
3. A prepares on P1 and P2, and commits. P1 records nothing for `X`.
4. Transaction B has timestamp 3. Its coordinator has a late clock, or a recovery drive sends its prepare late.
5. B has a put of `X`. P1 compares 3 with the delete watermark 1, and accepts. B commits.

The result is serializable in the order "A, then B". The locks give that order: a `check` lock is exclusive for a
two-phase prepare, for a single-partition transaction and for a non-transactional write. A coordinator sends a
commit only after each participant accepted.

The result is not serializable in the timestamp order "B, then A". In that order `X` exists when A reads it, and
the `check` of A fails.

Thus the serial order of the system is the lock order, and the timestamps agree with it only for an item that has
a row. This has three effects:

- The property `WriteSerializable` in `docs/ideas/2026-10-09-p-model-of-fokosdb-transactions.md` says that the
  timestamp order is a serial order of the two-phase transactions. The example disproves it.
- A late prepare of a committed `check` on an absent item passes the timestamp test. It leaves a lock that stays
  until the stale-transaction job resolves it.
- The optimizations of section 4 of the DynamoDB paper need a timestamp order that is a serial order. Examples
  are more than one prepared transaction on one item, and a write with an old timestamp that the commit ignores.
  `docs/agent-plans/2026-09-05-item-order-timestamps-and-read-revisions.md` keeps them out of scope, and this
  document does not add them.

### 1.3 What the DynamoDB paper says

The paper ([Idziorek et al., USENIX ATC 2023](https://www.usenix.org/system/files/atc23-idziorek.pdf), local copy
`docs/research/atc23-idziorek-dynamodb-transactions.md`) does not state what a committed `check` on an absent item
records. It gives these facts:

- **Prepare, Listing 3.** For an item that does not exist, the storage node compares the transaction timestamp
  with `partition.maxDeleteTimestamp`. It then makes an item record that holds `ongoingTransaction`. That record
  is the lock of the absent key.
- **Commit, section 3.3.** "Items for which a precondition was checked but that are not being written also have
  their timestamps updated."
- **Commit, Listing 4.** `processCommit` sets `item.timestamp` to the transaction timestamp on the item record. It
  has no branch that removes a record that the prepare made.
- **Cancel, Listing 4.** `processCancel` calls `deleteItem` for an item that "was created during prepare".
- **Deletes, section 3.3.** DynamoDB keeps no tombstone. A delete raises the partition-level max delete timestamp
  when the timestamp of its transaction is higher.

Read word for word, `processCommit` leaves a record with the timestamp of the `check` for the absent key. That
record is a tombstone, and section 3.3 says that DynamoDB keeps none. Thus the listing is a simplification. The
paper does not say that the removal of the record raises the max delete timestamp, at commit or at cancel.

The intent of the paper is clear: an item that a transaction only checked gets the transaction timestamp. For a
key with no row, the partition-level max delete timestamp is the only value that can hold it. This document
applies that reading at commit. Section 6 gives the reason for no change at cancel.

## 2. Goals and Requirements

### 2.1 In scope

- When a two-phase commit applies a `check` and the item has no row, the partition must raise
  `max_delete_tx_order_ts` to at least the transaction timestamp.
- After the change, for each key, the watermark that a prepare compares must be at least the timestamp of each
  committed two-phase operation that the watermark orders against.
- A late prepare of a committed `check` on an absent item must get `timestamp_conflict`.
- A statement that does not raise `max_delete_tx_order_ts` must write no row.
- `AGENTS.md`, `docs/agent-plans/2026-09-05-item-order-timestamps-and-read-revisions.md` and
  `docs/ideas/2026-10-09-p-model-of-fokosdb-transactions.md` must state the new rule.

### 2.2 Out of scope

- A cancel must not raise `max_delete_tx_order_ts`. A cancelled transaction has no place in the serial order.
- A `check` in a single-partition transaction (`executeSingleShot`) must not raise `max_delete_tx_order_ts`.
  Section 6 gives the reason.
- The change must not add a second watermark for absent reads, and must not add a row for an absent key.
- The change must not touch `max_deleted_v`. A read transaction must see no new `read_conflict`.
- The change must not add the optimizations of section 4 of the DynamoDB paper.

## 3. Milestones

One milestone. It delivers the store change, the participant change, the tests and the document updates together.

## 4. Proposed Solution

### 4.1 High-level overview

The commit of a `check` records its timestamp in one of two places. When the item has a row, the row holds it, as
today. When the item has no row, the delete watermark of the partition holds it.

```text
commit of a check at timestamp T
        |
        v
  item has a row? ---- yes ---> last_read_ts = MAX(last_read_ts, T)                (today)
        |
        no
        |
        v
  max_delete_tx_order_ts = MAX(max_delete_tx_order_ts, T)                          (new)
```

The same example then gives a different result:

1. Transaction A (timestamp 5) commits its `check` on the absent item `X`. The delete watermark of P1 goes to 5.
2. Transaction B (timestamp 3) prepares its put of `X`. P1 compares 3 with 5, and rejects with
   `timestamp_conflict`.
3. The caller sends B again with a new token. The new transaction gets a new timestamp above 5.

The timestamp order of the two-phase transactions is then a serial order. The cost is false conflicts: the delete
watermark is one value for the partition, so it also rejects an old prepare on each other absent key of P1.
Section 4.2.5 gives the cases.

### 4.2 Technical details

#### 4.2.1 Schema

No schema change. `deletion_metadata.max_delete_tx_order_ts` gets one more writer.

#### 4.2.2 Store

- `PartitionStore.bumpItemReadTs` must return whether the statement updated a row.
- `PartitionStore.bumpMaxDeleteTxOrderTs` must write only when the new value is higher:

```sql
UPDATE deletion_metadata SET max_delete_tx_order_ts = ?1 WHERE id = 1 AND max_delete_tx_order_ts < ?1
```

The guard also applies to the transactional delete of an absent item, which calls the same function.

#### 4.2.3 Participant

In `TransactionParticipant`, the `check` branch of `#write` calls `bumpItemReadTs`. When the call updated no row
and the caller is the two-phase commit (`commitLocal`, through `#apply`), the branch calls
`bumpMaxDeleteTxOrderTs` with the transaction timestamp.

The single-partition path (`executeSingleShot`, through `#writeRequest`) does not make the second call.

The comment on the watermark in `#evaluate` and the comment on `max_delete_tx_order_ts` in the schema of
`partition-store.ts` must state the new rule.

#### 4.2.4 Invariants

| Invariant | Mechanism |
| --- | --- |
| A put, update or delete of a key has a timestamp above each earlier committed two-phase read or write of the key. | Row: `last_read_ts`. No row: `max_delete_tx_order_ts`, which a committed `check` now also raises. |
| The watermark of a key does not go down when its row goes away. | A delete and a TTL expiry raise `max_delete_tx_order_ts` to at least the `last_read_ts` of the removed row. No change. |
| The watermark of a key does not go down when its row comes back. | A new row starts its timestamps at least at `max_delete_tx_order_ts`. No change. |
| The watermark moves with the key. | A migration merges `max_delete_tx_order_ts` with `MAX`. No change. |

Operations on one item in an `ordered_per_item` transaction need no new rule. `#evaluate` compares each operation
with the stamps from before the transaction, and all operations of a transaction use one timestamp.

A `check` on an absent item that a later put of the same transaction creates raises the delete watermark, and the
put then starts the row at the same timestamp. The first write is redundant. It stays, because the case is rare
and a test for it adds a read.

#### 4.2.5 False conflicts

After a `check` on an absent item commits at timestamp `T`, the partition rejects a prepare when all of these are
true:

- It is a two-phase prepare. The single-partition path and the non-transactional writes do no timestamp test.
- One of its items has no row in that partition. This is the false part: the item can be any absent key.
- Its timestamp is at or below `T`.
- It arrives after the commit.

A `check` on an absent key is also rejected, although two reads of one key do not conflict.

The conflicts have the same form as the ones that a transactional delete causes today. Three causes make an older
transaction arrive after the commit of a newer one:

| Cause | Window |
| --- | --- |
| Tail latency: a slow prepare, a forward through a router, a retry after `partition_migrating`. | About the duration of the two-phase run of the committed transaction. TODO: measure. |
| A recovery drive. `tx_recovery` drives a transaction that stayed in `PREPARING` with its stored timestamp. | The age of the transaction. It fails on an absent key in each partition with a newer commit of this type. |
| Clock skew. A partition accepts a timestamp up to `maxClockSkewMs` ahead of its clock. | At most `maxClockSkewMs`, 2 s by default (`server/host-config.ts`). A commit from a coordinator that is 500 ms ahead rejects each correct-clock prepare on an absent key for 500 ms. |

The effect depends on the partition:

- A partition with transactional deletes, non-transactional deletes of present rows, or TTL expiry already keeps
  its delete watermark near the current time. The change adds few conflicts there. TODO: measure.
- A partition with inserts and checks and no deletes has a delete watermark of 0 today. The change brings
  `timestamp_conflict` on absent keys to that partition for the first time.

A read transaction is not affected. It compares `max_deleted_v`, which the change does not touch.

#### 4.2.6 Cost

Each committed `check` on an absent item writes one row of `deletion_metadata` when it raises the value. The guard
of section 4.2.2 removes the write when the value stays.

#### 4.2.7 Failure and recovery

The raise runs in the storage transaction of `commitLocal`, with the release of the lock. A commit that throws
rolls both back. A repeated commit finds no owned lock row and writes nothing.

A late prepare of the committed transaction compares its timestamp `T` with a delete watermark of at least `T`,
and gets `timestamp_conflict`. It writes no lock.

#### 4.2.8 Deployment

No migration. The system is before its first release.

#### 4.2.9 Testing

In `packages/fokosdb/src/shared/partition/transaction-participant.test.ts`:

- A committed two-phase `check` on an absent item raises `max_delete_tx_order_ts` to its timestamp.
- After that commit, a prepare of a put on the same key at a lower timestamp gets `timestamp_conflict`.
- After that commit, a prepare on a different absent key at a lower timestamp gets `timestamp_conflict`.
- After that commit, a repeated prepare of the same transaction gets `timestamp_conflict` and writes no lock.
- A committed two-phase `check` on an item that has a row does not change `max_delete_tx_order_ts`.
- A cancelled `check` on an absent item does not change `max_delete_tx_order_ts`.
- A single-partition `check` on an absent item does not change `max_delete_tx_order_ts`.
- A committed `check` on an absent item does not change `max_deleted_v`.

In `packages/fokosdb/src/shared/partition/partition-store.test.ts`:

- `bumpMaxDeleteTxOrderTs` with a value that is not higher writes no row.

## 5. Alternative Options

### 5.1 Keep the code and change the property

Limit the timestamp rule of `WriteSerializable` to keys that have a row, and prove serializable writes with a
monitor that searches serial orders. This option adds no conflict and no row write. It leaves the lock order as
the only serial order, so the timestamps stay a partial signal, and a late prepare of a committed `check` on an
absent item still leaves a lock.

### 5.2 A row for each checked absent key

Keep a row with the read timestamp for each absent key that a `check` read. This option gives no false conflict
on other keys. It adds one stored row for each such key and a job that removes the rows. The DynamoDB paper
rejects tombstones for the same storage and cleanup cost.

### 5.3 A second partition watermark for absent reads

Keep `max_absent_read_ts` next to `max_delete_tx_order_ts`. A write on an absent key compares both values, and a
`check` on an absent key compares only the delete watermark. This option removes the false conflict between two
checks on absent keys. It adds a column and a second comparison, and a write on an absent key gets the same false
conflicts as in the proposed solution.

## 6. Frequently Asked Questions

### Is the current behavior a serializability defect?

No. The locks make each result serializable in the lock order. The change makes the timestamp order agree with
that order for an absent key.

### Why does a cancel not raise the delete watermark?

A cancelled transaction applied nothing and its `check` result reached no committed state. No later write must
order after it. A raise at cancel lets each rejected transaction cause conflicts for other transactions.

### Why does the single-partition path not raise the delete watermark?

The single-partition path stamps with the clock of the partition at the time of the request. That value is above
the timestamp of each two-phase transaction that is in flight, so each such `check` rejects all of them on absent
keys. The single-partition path takes no timestamp test itself, and `WriteSerializable` excludes it.

A single-partition delete of an absent item raises the delete watermark today. This document does not change it.

### Does the change affect a `check` on an item that has a row?

No. That commit raises `last_read_ts` of the row, as today.

### Which rule of an earlier plan does this change replace?

Sections 2.2 and 4.2.4 of `docs/agent-plans/2026-09-05-item-order-timestamps-and-read-revisions.md` say that a
`check` on an absent item must not add another watermark. This document adds no watermark, and it makes that
`check` raise the delete watermark at commit.

## 7. References

- `docs/research/atc23-idziorek-dynamodb-transactions.md`
- [Distributed Transactions at Scale in Amazon DynamoDB](https://www.usenix.org/system/files/atc23-idziorek.pdf)
- `docs/agent-plans/2026-09-05-item-order-timestamps-and-read-revisions.md`
- `docs/agent-plans/2026-10-03-max-deleted-version.md`
- `docs/ideas/2026-10-09-p-model-of-fokosdb-transactions.md`
- `packages/fokosdb/src/shared/partition/transaction-participant.ts`
- `packages/fokosdb/src/shared/partition/partition-store.ts`
