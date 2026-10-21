# RFC — Idempotent sink writes: batch conditions, external versions, and versioned tombstones

**State:** Draft
**Date:** 2026-10-21
**Author:** Lambros Petrou
**Status:** Nothing is built. Section 1 and the word "today" describe the code before this spec.

## Table of Contents

- [1. Overview and Context](#1-overview-and-context)
  - [1.1 The current system](#11-the-current-system)
  - [1.2 The problem](#12-the-problem)
  - [1.3 Facts that constrain the design](#13-facts-that-constrain-the-design)
  - [1.4 Glossary](#14-glossary)
- [2. Goals and Requirements](#2-goals-and-requirements)
  - [2.1 In scope](#21-in-scope)
  - [2.2 Requirements](#22-requirements)
  - [2.3 Out of scope](#23-out-of-scope)
- [3. Milestones](#3-milestones)
- [4. Proposed Solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
    - [4.2.1 Milestone 1: conditions in `batchWriteItems`](#421-milestone-1-conditions-in-batchwriteitems)
    - [4.2.2 Milestone 2: the external version](#422-milestone-2-the-external-version)
    - [4.2.3 Milestone 3: the versioned tombstone delete](#423-milestone-3-the-versioned-tombstone-delete)
    - [4.2.4 Cost](#424-cost)
    - [4.2.5 Trade-offs](#425-trade-offs)
    - [4.2.6 Deployment](#426-deployment)
    - [4.2.7 Testing](#427-testing)
  - [4.3 Open Questions](#43-open-questions)
- [5. Alternative Options](#5-alternative-options)
- [6. Frequently Asked Questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and Context

### 1.1 The current system

- `putItem` and `deleteItem` take an optional `condition?: ConditionExpression` (`PutItemOptions` and
  `DeleteItemOptions` in `src/shared/types.ts`). `putItemLocal` and `deleteItemLocal` in `src/server/do-partition.ts`
  evaluate the condition with `PartitionStore.evaluateCondition` in the same `transactionSync` as the write. A failed
  condition raises `FokosConditionCheckError` with the code `condition_failed` (`CONDITION_CHECK_CODES` in
  `src/shared/errors-operations.ts`).
- `batchWriteItems` puts or deletes 1 to 100 items. `BatchWriteItem` in `src/shared/batch-api-types.ts` has no
  condition. `validateBatchWriteItems` in `src/shared/transaction-limits.ts` refuses an item with `condition`,
  `update`, or `returnValuesOnConditionCheckFailure` (`batch_operation_fields_invalid`). Each item gets one of the
  outcomes `ok`, `rejected`, `unprocessed`, or `unknown`. `batchWriteLocal` runs each item in its own
  `transactionSync`.
- Each item row has an internal revision `v`. Every write increments it. A delete or a TTL expiry of a row at `v` has
  the delete revision `v + 1`. `max_deleted_v` in the one-row table `deletion_metadata` is the highest delete
  revision of the partition, and a new row starts at `max_deleted_v + 1` (`PartitionStore.upsertItem`,
  `PartitionStore.deleteItem`, `PartitionStore.deleteExpiredItems`).
- A delete removes the `items` row. Nothing keeps the version of a deleted key.
- The TTL sweep `PartitionStore.deleteExpiredItems` deletes expired, unlocked rows by `idx_items_ttl`. It does not
  test the owner of each row.

### 1.2 The problem

A writer that retries, or that delivers updates out of order, cannot use FokosDB as an idempotent sink. The sink must
apply an update only when it is newer than the stored state, and it must remember the version of a deleted key for
some time. Three users need this:

1. **A push target of the coalesced materialization RFC.** It receives `PushTargetItem` records with a `revision`,
   at-least-once and out of order. It must drop each push with `revision <= appliedRevision`, and it must keep the
   revision of a deleted key for at least `deletedRevisionHorizonMs`
   (`docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md`, Section 4.2.5).
2. **Replication or change data capture (CDC) from an external system into FokosDB.** The source system owns the
   version, as with the Elasticsearch option `version_type=external`.
3. **A later option for the GSIs of the coalesced materialization RFC.** Each GSI entry stores the base revision, and
   a GSI delete writes a versioned tombstone with a TTL. This option trades the producer epoch fences of RFC Section
   5.6 for tombstones with a time bound. It also gives a GSI query the base `version` (RFC Open Question 4.3.3). This
   spec does not choose the option. It only lists it as a user.

Today a caller can store a version in a JSON attribute and check it with a condition on `putItem`. That fails in three
ways: it works only for `json` items, `batchWriteItems` has no condition, and a late write after a delete creates the
item again.

### 1.3 Facts that constrain the design

- **`last_write_ts` cannot order last-writer-wins updates across retries** (RFC Section 1.3 of the coalesced
  materialization spec). A sink needs a version from the writer.
- **A failed condition in a batch is not a thrown error.** `classifyBatchFailure` (`src/shared/batch-failure.ts`)
  gives an error of the local part the outcome `unprocessed` (rule 3). A `FokosConditionCheckError` is not a
  validation error, so the handler must give a condition failure its outcome directly.
- **`conditionPlanOf` compiles on each call** (`src/shared/expression/request-plans.ts`). `projectionPlanOf` and
  `updatePlanOf` keep the plan of a tree object in a `WeakMap`.
- **`batchWriteItems` has no payload cap in the client.** A root RPC above 32 MiB gives each item of that root
  `rejected` with `batch_request_too_large`. `transactWriteItems` adds the JSON size of each condition to
  `MAX_PAYLOAD_BYTES_PER_TX` (4 MB) in `validateTransactWriteOperations`.
- **`items.data` is `ANY NOT NULL`** (migration 1 of `PartitionStore`). A row with no payload needs a schema change or
  a sentinel value.
- **Partitions never merge.** `RepartitionKind` (`src/sharding/sharding-store.ts`) is `hash_split`, `range_split`, or
  `key_promotion`. A partition that splits stays split.
- **A delete passes the size admission.** A partition refuses a write above 1.1 times its cap, and the `admit` hook
  lets a delete through, because a delete is how a client brings a partition back under its cap.
- **A read transaction compares `max_deleted_v` for an item that is absent in both phases**, and `version` for an item
  that is found in both phases (`docs/agent-plans/2026-10-03-max-deleted-version.md`).
- **There is no public `updateItem`.** An update is the `update` operation of `transactWriteItems`. On an absent item
  it uses the empty document as the pre-image (`TransactWriteItem` in `src/shared/transaction-api-types.ts`). The
  single-shot path runs `PartitionStore.updateItemSingleShot`.

### 1.4 Glossary

- **Sink** — a FokosDB table that receives writes from a writer that retries or reorders them.
- **External version** — a non-negative integer that the writer gives with a write. FokosDB stores it and compares
  it. It is not `v`.
- **Stale write** — a write whose external version does not pass the rule of Section 4.2.2.
- **Tombstone** — a key-only `items` row that a versioned delete keeps in place of the item. Every read treats it as
  an absent item.

## 2. Goals and Requirements

### 2.1 In scope

1. `batchWriteItems` accepts an optional `condition` on each `put` and `delete` item (Milestone 1).
2. `putItem`, `deleteItem`, and the `put` and `delete` items of `batchWriteItems` accept `externalVersion`. A stale
   write changes nothing (Milestone 2).
3. A read returns the stored external version of an item (Milestone 2).
4. A delete with `externalVersion` and `tombstoneTtlSeconds` keeps a tombstone. A stale write over a tombstone changes
   nothing. The TTL sweep removes the tombstone (Milestone 3).

### 2.2 Requirements

- Each milestone must ship on its own, in the order of Section 3.
- A condition and an external version check must run in the same `transactionSync` as the write of the item.
- `externalVersion` must work for each data kind (`json`, `text`, `bytes`), and it must need no expression compile.
- `v` must still increase on each write. `externalVersion` does not replace `v`.
- The revisions of a key (each `v` and each delete revision) must always increase, also across a tombstone.
- No read path returns a tombstone as an item.
- A milestone must add no row write to a write that does not use its feature.

### 2.3 Out of scope

- Update expressions and `returnValuesOnConditionCheckFailure` in `batchWriteItems`. The batch spec keeps them out,
  and `transactWriteItems` covers them. Milestone 1 keeps this boundary.
- The choice between producer epoch fences and versioned tombstones for the GSIs of the coalesced materialization RFC.
- A tombstone without a TTL. Each tombstone must expire (Section 4.2.5).
- A merge of partitions after a burst of tombstones.

## 3. Milestones

1. **Conditions in `batchWriteItems`** (Section 4.2.1). The public type, the validation, the wire type, the handler,
   and the outcome. The HTTP example test that expects 400 for a batch condition changes.
2. **The external version** (Section 4.2.2). The `items` column, the write option on the three write paths, the
   stale rule, and the read field. It works without Milestone 3, with one limit: a late write after a delete creates
   the item again.
3. **The versioned tombstone delete** (Section 4.2.3). It needs Milestone 2, and Milestone 1 of the coalesced
   materialization RFC ("The TTL sweep deletes only owned rows") must ship first.

## 4. Proposed Solution

### 4.1 High-level overview

The three features are independent options of the existing write paths. Each one runs inside the `transactionSync`
of its item, so a check and its write are atomic.

1. **A batch condition.** Each `put` or `delete` of a batch can carry a condition. The partition evaluates it as
   `putItem` does. A failed condition gives that item `rejected` with `condition_failed`, and the other items of the
   batch continue.
2. **An external version.** A write carries `externalVersion`. The partition stores it in a new `items` column. The
   write applies only when the stored external version is absent or lower. A stale write changes nothing, and the
   caller treats it as done.
3. **A versioned tombstone.** A delete with `externalVersion` and `tombstoneTtlSeconds` keeps a key-only row with the
   external version and a TTL. Reads and queries skip the row. A late write with a lower version meets the tombstone
   and is stale. The TTL sweep removes the tombstone when it expires.

```text
 writer (retries, out of order)               PartitionDO, one transactionSync per item
 ──────────────────────────────               ─────────────────────────────────────────
 put   k  ext=5  ───────────────────────────▶ stored ext absent      -> apply, ext=5
 put   k  ext=7  ───────────────────────────▶ stored ext 5 < 7       -> apply, ext=7
 put   k  ext=6  (late retry) ──────────────▶ stored ext 7 >= 6      -> stale, no write
 delete k ext=8 tombstoneTtlSeconds=3600 ───▶ stored ext 7 < 8       -> tombstone, ext=8, ttl=now+3600
 put   k  ext=7  (late retry) ──────────────▶ tombstone ext 8 >= 7   -> stale, no write
 ... 3600 s later, deleteExpiredItems removes the tombstone
 put   k  ext=7  (a call that is late by 1 h) ▶ no row               -> apply (the time-bound risk)
```

### 4.2 Technical details

#### 4.2.1 Milestone 1: conditions in `batchWriteItems`

**API.** The `put` and `delete` variants of `BatchWriteItem` get `condition?: ConditionExpression`, the type of
`PutItemOptions.condition`. `validateBatchWriteItems` accepts `condition` and still refuses `update` and
`returnValuesOnConditionCheckFailure` with `batch_operation_fields_invalid`. `db.ts` validates each condition tree with
`expression/semantic.ts` before any I/O, as for `putItem`. `BatchWriteRpcItem` carries the tree.

**Handler.** In `batchWriteLocal`, the block of each item runs these steps:

1. `assertNotLocked`.
2. When the item has a condition, `PartitionStore.evaluateCondition`. When the condition fails, the block writes
   nothing and returns a condition failure.
3. The upsert or the delete.

The handler gives a condition failure the outcome `rejected` with the code `condition_failed` and a new `error_id`. It
does not throw, because `classifyBatchFailure` gives a thrown local error `unprocessed` (Section 1.3). The next item
runs. `putItemLocal`, `deleteItemLocal`, and `batchWriteLocal` share one function for the steps 1 to 3.

**Outcome.** `rejected` means "nothing of the operation applied, and the same operation fails the same way again"
(`BatchWriteItemResult`). A condition failure fits for the current state of the item: the same item against the same
state fails again, and `FokosStd.batchWriteItemsWithRetry` does not send a `rejected` item again. The state can change
later and make the condition pass, so the fit is not exact. Open Question 4.3.1 lists a separate outcome.

**Compile cost.** Many items of one batch can share one condition shape, for example "the stored version is lower than
X" with a different X for each item. `conditionPlanOf` compiles each tree on each call. A batch of 100 conditions
compiles 100 times. TODO: decide the caching (Open Question 4.3.2). The expression compile cost plan
(`docs/agent-plans/2026-10-10-expression-compile-cost.md`, Section 5.4) did not choose a plan cache in the partition.

**Size.** A condition counts toward the size of the request. `batchWriteItems` has no client payload cap, so the
condition counts toward the 32 MiB RPC limit, and an oversize root gives `batch_request_too_large`. TODO: decide
whether the batch counts the JSON size of each condition, as `validateTransactWriteOperations` does.

#### 4.2.2 Milestone 2: the external version

**API.** `PutItemOptions`, `DeleteItemOptions`, and the `put` and `delete` variants of `BatchWriteItem` get
`externalVersion?: number`. It must be a non-negative safe integer, or the call throws a validation error before any
I/O. TODO: decide the range (Open Question 4.3.3) and the name of the validation code.

**Schema.** The `items` table gets one column, for example `ext_version INTEGER`, NULL when no write gave a version.
TODO: name the column. The column works for each data kind and needs no expression compile.

**Rule.** A write with `externalVersion = E` applies only when the stored external version is absent or lower than
`E`. The partition checks the rule in the `transactionSync` of the item, after the lock check and with the condition:

- When the item has both a `condition` and an `externalVersion`, both checks must pass.
- When the stored version is equal to `E`, the result is open (Open Question 4.3.4).
- A stale write writes no row. Its result is open (Open Question 4.3.5).
- A write without `externalVersion` on a row with a stored version is open (Open Question 4.3.6).

`PartitionStore.upsertItem` reads `est_row_bytes` with `#storedEstRowBytes` before the upsert. The same read can return
the stored external version, so the check adds no row read. That read uses the covering index `idx_items_scan`. TODO:
decide whether the index gets the column, or the read reads the table. `PartitionStore.deleteItem` already reads the
row before the delete.

**Reads.** `getItem`, `batchGetItems`, `queryItems`, and `transactGetItems` return the stored external version with
the item metadata, next to `version` in `ReadItem`. TODO: name the field. A projection can read it: TODO decide
whether the expression engine gets a reference to the column, as `{ ref: "v" }`.

**Interactions.**

- `v` still increases on each applied write. A stale write does not change `v`.
- `transactWriteItems`: allowed or not is open (Open Question 4.3.7).
- The `update` operation of `transactWriteItems` and `updateItemSingleShot`: an update without a version keeps or
  clears the stored version (Open Question 4.3.6).
- The migration pages of `FokosMigrationHost` and the ingest must carry the column.

#### 4.2.3 Milestone 3: the versioned tombstone delete

**API.** `DeleteItemOptions` and the `delete` variant of `BatchWriteItem` get `tombstoneTtlSeconds?: number`. It needs
`externalVersion`, or the call throws a validation error. TODO: the validation code, and a policy maximum or a
default (Open Question 4.3.9).

**The tombstone row.** A delete with `externalVersion = E` that passes the rule of Section 4.2.2 keeps one `items` row
for the key:

| Column | Value |
| --- | --- |
| `data` | removed: NULL, or a sentinel. `data` is `ANY NOT NULL` today. TODO: decide. |
| a deleted flag | set. TODO: a new column, or a code in `data_kind` (an on-disk index of `DATA_KINDS`). |
| `ext_version` | `E` |
| `ttl_epoch_utc_seconds` | `now + tombstoneTtlSeconds` |
| `v` | open (Open Question 4.3.8) |
| `est_row_bytes` | the key bytes plus `EST_ROW_BYTES_K` (108), with no payload |

TODO: decide whether the delete writes a tombstone when no row exists for the key. A sink needs it when a late put
with a lower version can arrive after the delete. The `deleted` field of the result then stays true only when an item
was present.

**Reads.** Every read path treats a tombstone as an absent item:

- `getItem` and `batchGetItems` return "not found".
- `queryItems` skips a tombstone as a filter rejects a candidate. The tombstone counts toward the rows read and the
  evaluated budgets, and it can make a page short. A page can hold no items and still carry a cursor
  (`docs/agent-plans/2026-09-14-read-projections-and-query-filters.md`, requirement 10). `select: "count"` does not
  count it.
- A condition evaluates a tombstone as an absent item. The `update` operation uses the empty document as the
  pre-image.
- A read transaction treats a tombstone as an absent item. Open Question 4.3.8 covers the revision that it compares.
- TODO: list every statement that reads `items` (the point read, the image read, the query statements, the
  transaction reads, the range split boundaries) and add the tombstone test to each one.

**Writes over a tombstone.** A put or a delete with an external version at or below the tombstone version is stale,
with the rule of Section 4.2.2. A higher version replaces the tombstone: a put writes the item, and a delete writes a
new tombstone. A write without a version over a tombstone is open (Open Question 4.3.6).

**The TTL sweep.** `deleteExpiredItems` removes an expired tombstone, as it removes an expired item. It updates
`key_size_estimates` and `deletion_metadata` in the same `transactionSync`. Milestone 1 of the coalesced
materialization RFC must ship first. Today the sweep does not test the owner of each row, so a promotion source can
delete the copy of a tombstone before the range root pulls it. Then the range root has no tombstone, and a late write
with a lower version applies.

**Revisions.** The tombstone row must fit the delete revision rule, and the next put over a tombstone must get a `v`
above every earlier revision. Open Question 4.3.8 has the options.

**`captureChanges`.** With `captureChanges = true` (coalesced materialization RFC), a tombstone delete is a delete for
the outbox. TODO: decide what the outbox gets when the sweep removes a tombstone. The item was already deleted.

**Size.** A tombstone counts in `key_size_estimates` and in the partition size until the sweep removes it. A tombstone
over an absent key adds a row. A delete passes the size admission (Section 1.3), so tombstones can grow a partition
above 1.1 times its cap. TODO: decide whether a tombstone over an absent key goes through the size admission.

#### 4.2.4 Cost

| Milestone | Extra rows read | Extra rows written |
| --- | --- | --- |
| 1: batch condition | the rows that the condition statement reads (1 for a point condition). TODO: measure. | 0 |
| 2: external version | 0, with the read before the write (Section 4.2.2) | 0. A column of the `items` row. |
| 3: tombstone delete | 0 | about 1 to 2 for each delete (below) |

A plain delete of a present item writes 3 rows: `items`, `key_size_estimates`, and `deletion_metadata`. A tombstone
delete writes the tombstone row and `key_size_estimates`, plus `deletion_metadata` when the delete raises
`max_deleted_v` at once. Later the sweep deletes the tombstone and writes `key_size_estimates` and
`deletion_metadata`. The sweep writes these two once for each hash key and once for each chunk. Thus the extra cost is
about 1 to 2 rows for each delete. A tombstone over an absent key costs about 3 rows, where a plain delete of an absent
key writes 0. The index entries of `idx_items_scan` and `idx_items_ttl` can add rows written. TODO: measure each case.

**Storage.** At 100 deletes per second and a TTL of 1 hour, a table keeps 360,000 tombstones. With a key of 50 bytes,
one tombstone is about 160 bytes in `est_row_bytes`, so 360,000 tombstones are about 58 MB across the table. TODO:
measure the size on disk.

#### 4.2.5 Trade-offs

- **The tombstone bound is time-based.** A tombstone protects against a late write with a lower version only until
  the sweep removes it. A call that stays in flight for 1 hour is rare, so a TTL of 1 hour or more makes the risk
  small. The risk is not zero: a write that arrives after the sweep creates the item again. Elasticsearch has the same
  bound for deleted versions (`index.gc_deletes`, 60 s by default).
- **Tombstones can split partitions that stay split.** A burst of deletes adds tombstone rows, which count toward the
  partition size. A partition that splits because of tombstones does not merge after the sweep.
- **A query page can be short.** Tombstones cost rows read and evaluated budget, and give no item.
- **`externalVersion` is a column, not an attribute.** It works for each data kind and needs no compile. A caller who
  wants a version inside the JSON data still uses a condition.

#### 4.2.6 Deployment

- Milestones 2 and 3 change the `items` schema. FokosDB is pre-release, so the change edits the existing migration of
  `PartitionStore` in place.
- Deploy the Worker that defines `PartitionDO` before a client that sends the new fields.

#### 4.2.7 Testing

- **Milestone 1.** `test/partition-do/batch-write-items.test.ts`: a passing and a failing condition on a put and a
  delete, a failed item next to applied items, a condition on an absent item, and a locked item with a condition. The
  HTTP example test "rejects a batchWriteItems operation with a condition as 400" changes to accept the condition.
- **Milestone 2.** A higher version applies, a lower version is stale, an equal version follows Open Question 4.3.4,
  each data kind, a version with a condition, `v` after a stale write, the read field, and a migration that keeps the
  column.
- **Milestone 3.** A tombstone is not found by `getItem`, `batchGetItems`, and `queryItems`. A condition sees an absent
  item. A lower version over a tombstone is stale, a higher version replaces it. The sweep removes it. A promotion
  keeps the tombstone (after Milestone 1 of the coalesced materialization RFC). A read transaction across a tombstone
  delete gets `read_conflict`. Each `v` and each delete revision of a key increases.
- **Property-based.** The item model of `test/property-based/harness.ts` gets external versions and tombstones.
  TODO: decide the commands.

### 4.3 Open Questions

#### 4.3.1 A separate outcome for a batch condition failure

`rejected` fits for the current state, but a later state can make the condition pass (Section 4.2.1). Options: keep
`rejected` with `condition_failed`, or add an outcome such as `condition_failed`. A new outcome changes
`BatchWriteItemResult`, the `FokosStd` helper, and the property-based model.

#### 4.3.2 The compile of many conditions in one batch

Options: compile each tree (today), keep the plan of a tree object in a `WeakMap` as `projectionPlanOf` does, or key a
cache by the canonical identity of the tree. A `WeakMap` helps only when items share one tree object. TODO: verify
whether a tree that the client shares between items stays one object after the RPC. Values that differ for each item
make different trees, so only a cache by shape with separate bindings helps that case.

#### 4.3.3 The range of `externalVersion`

Options: a non-negative safe integer (up to 2^53 - 1), or a `bigint` up to the SQLite `INTEGER` maximum (2^63 - 1).
A `bigint` fits a source system with 64-bit versions. It changes the type of each read field and the JSON surface of
the HTTP example.

#### 4.3.4 A write with an equal version

Options: an equal version applies again (an idempotent overwrite), or it is stale. An overwrite is necessary when a
projection definition changes and the producer sends the same revision with a new value. Elasticsearch has both:
`version_type=external` needs a higher version, and `external_gte` accepts an equal one.

#### 4.3.5 The result of a stale write

Options: a success with a flag (for example `applied: false`), or `rejected` with a new code such as
`external_version_stale`. The caller of a sink treats a stale write as done, so a success flag can fit better. For
`putItem` and `deleteItem`, `rejected` means a thrown error.

#### 4.3.6 A write without `externalVersion` on a versioned row

This covers a put, a delete, and the `update` operation of `transactWriteItems`. Options: the write applies and keeps
the stored version, the write applies and clears it, or the write is refused. Over a tombstone, the same choice
decides whether a write without a version can create the item again.

#### 4.3.7 `externalVersion` in `transactWriteItems`

Options: allowed on `put`, `delete`, and `update`, or refused. When allowed, the rule runs in the prepare and in the
commit, and a stale operation cancels the transaction or gives a flag.

#### 4.3.8 The revisions of a tombstone

Options:

1. The tombstone row takes `v + 1`, and the sweep raises `max_deleted_v` when it removes the row. The sweep already
   raises `max_deleted_v` to `row.v + 1` for each removed row.
2. The delete raises `max_deleted_v` to the delete revision at once, as a plain delete does.

A read transaction compares `max_deleted_v` for an item that is absent in both phases. Under option 1, the sequence
"tombstone, put, tombstone" between the two phases does not change `max_deleted_v`, so the read transaction must also
compare the `v` of the tombstone. Option 2 costs one more row written (`deletion_metadata`) for each delete.

#### 4.3.9 The limit of `tombstoneTtlSeconds`

Options: no limit, a policy maximum, a default, or a minimum such as 1 hour. A long TTL keeps more rows (Section 4.2.4).
A short TTL makes the time-bound risk larger (Section 4.2.5).

## 5. Alternative Options

### 5.1 User marker items with a TTL

The caller writes a marker item with a TTL in place of a delete, and checks it with a condition. Not chosen: reads and
queries return the markers, and every caller must filter them. It works only for `json` items.

### 5.2 Conditions only, with no tombstone

The caller checks a version attribute with a condition on each write. Not chosen as the complete solution: a late write
after a delete finds no row and creates the item again. It stays as Milestone 1.

### 5.3 Producer epoch fences

Section 5.6.6 of the coalesced materialization RFC fences each producer with an epoch. It has no time bound and no
tombstone rows. Not chosen as a user API: each receiver keeps one row for each producer that never goes away, and the
writer must keep an epoch. It fits the internal hops of FokosDB better than a general sink.

## 6. Frequently Asked Questions

### 6.1 Why not use `v` as the external version?

FokosDB owns `v`, and every write increments it. An external writer cannot set it, and `v` starts at
`max_deleted_v + 1` after a delete. The external version comes from the source of the writes.

### 6.2 Can a sink use Milestone 2 without Milestone 3?

Yes, when the source never deletes, or when a late write after a delete can create the item again.

### 6.3 Does a tombstone protect forever?

No. It protects until the sweep removes it, at the earliest `tombstoneTtlSeconds` after the delete (Section 4.2.5).

## 7. References

- `docs/agent-plans/2026-10-17-batch-get-and-write-items.md`
- `docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md`
- `docs/agent-plans/2026-10-03-max-deleted-version.md`
- `docs/agent-plans/2026-08-30-item-ttl-expiration.md`
- `docs/agent-plans/2026-10-10-expression-compile-cost.md`
- `docs/agent-plans/2026-09-14-read-projections-and-query-filters.md`
- [Elasticsearch: Index API, versioning](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#index-versioning)
- [Elasticsearch: Delete API, versioning](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete.html)
