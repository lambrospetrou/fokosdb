# RFC — Ordered per-item execution for transactWriteItems

**State:** Draft
**Date:** 2026-10-03
**Author:** Lambros

## Table of contents

- [1. Overview and context](#1-overview-and-context)
  - [1.1 The current write transaction](#11-the-current-write-transaction)
  - [1.2 Versions and deletion metadata](#12-versions-and-deletion-metadata)
  - [1.3 The problem](#13-the-problem)
  - [1.4 Glossary](#14-glossary)
- [2. Goals and requirements](#2-goals-and-requirements)
  - [2.1 In scope](#21-in-scope)
  - [2.2 Requirements](#22-requirements)
  - [2.3 Out of scope](#23-out-of-scope)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
    - [4.2.1 Public API](#421-public-api)
    - [4.2.2 Validation and limits](#422-validation-and-limits)
    - [4.2.3 Evaluate](#423-evaluate)
    - [4.2.4 Apply](#424-apply)
    - [4.2.5 The paths](#425-the-paths)
    - [4.2.6 Item identity](#426-item-identity)
    - [4.2.7 Lock row and commit](#427-lock-row-and-commit)
    - [4.2.8 Versions at prepare and at commit](#428-versions-at-prepare-and-at-commit)
    - [4.2.9 Coordinator](#429-coordinator)
    - [4.2.10 Result rules](#4210-result-rules)
    - [4.2.11 Condition failure images](#4211-condition-failure-images)
    - [4.2.12 Routing, retries, recovery, and migration](#4212-routing-retries-recovery-and-migration)
    - [4.2.13 Invariants](#4213-invariants)
    - [4.2.14 Performance](#4214-performance)
    - [4.2.15 Deployment](#4215-deployment)
    - [4.2.16 Testing](#4216-testing)
    - [4.2.17 Fatal prepare errors](#4217-fatal-prepare-errors)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

### 1.1 The current write transaction

`FokosDB.transactWriteItems` applies up to `MAX_ITEMS_PER_TX` (100) operations atomically. Each operation is a
`put`, a `delete`, a `check`, or an `update`. Each operation has an optional condition.

The current code has these properties:

- `validateTransactWriteOperations` rejects two operations on the same `(hashKey, sortKey)` pair with
  `transact_duplicate_key`.
- `db.ts` gives each operation its request position as `opIndex`. Every result carries `opIndex` back, so each node
  merges the results in request order.
- A cancelled transaction raises `FokosTransactionCancelledError`. Its `results[i]` answers request operation `i`,
  with the outcome `passed`, `rejected`, or `not_evaluated`.
- Two execution paths exist. The single-partition path sends the whole set to one partition through
  `txExecuteSingleShot`. The two-phase path sends the set to a `TransactionCoordinatorDO`, which drives 2PC.
- The coordinator stores one `tc_items` row for each operation, with `PRIMARY KEY (transaction_id, hk, sk)`.
- A participant stores one `pending_transactions` row for each key. The row holds the effect that commit applies:
  the operation, the data, the kind, and the TTL. Prepare materializes an update into its new document.
- SQLite evaluates each condition and each update against the committed `items` row.
- The single-partition path applies the operations in request order. The two-phase commit applies the keys in the
  order that the coordinator sends them. `loadItemKeys` has no `ORDER BY`, so that order is not the request order.

### 1.2 Versions and deletion metadata

The design depends on these rules of `PartitionStore`. The RFC `2026-10-03-max-deleted-version.md` defines them.

- Each partition has one `deletion_metadata` row with `max_deleted_v` and `max_delete_tx_order_ts`.
- A write to an existing row sets `v = v + 1`. A new row starts at `v = max_deleted_v + 1`. Thus the `v` of a key
  never repeats, also after a delete and a recreate.
- A delete that removes a row raises `max_deleted_v` to the `v` of the row. It also raises
  `max_delete_tx_order_ts` to at least the `last_read_ts` of the row.
- A new row starts with `last_read_ts` and `last_write_ts` at least `max_delete_tx_order_ts`. Thus the timestamp
  watermark of a key never goes down, and a late prepare of a committed transaction gets `timestamp_conflict`.
- A read transaction compares `version` for an item that it finds in both phases. It compares `max_deleted_v` for an
  item that is absent in both phases.

The store writes `upsertItem`, `updateItemSingleShot`, `deleteItem`, and `bumpItemReadTs` apply these rules. This
design changes none of them.

### 1.3 The problem

A caller cannot send two operations for the same item in one transaction. For example, a caller cannot check an item,
then update it, then check the result, all in one atomic request.

### 1.4 Glossary

- **Item** — one `(hashKey, sortKey)` pair. A missing `sortKey` is the empty sort key. Two keys are the same item
  only when their encoded bytes are equal (section 4.2.6).
- **Operation** — one entry of the request `items` array. Its `opIndex` is its position in that array.
- **Sequence** — the operations of one item, in `opIndex` order.
- **Repeated item** — an item with two or more operations in the request. A request with a repeated item is a
  **repeated request**.
- **Write** — a `put` or an `update`. A `delete` and a `check` are not writes.
- **Store write** — one call of `upsertItem`, `updateItemSingleShot`, `deleteItem`, or `bumpItemReadTs`.
- **Standalone operations** — the operations of a request, applied one by one in `opIndex` order as one-operation
  transactions with the same transaction timestamp `T`.
- **Committed state** — the `items` row of an item before the transaction starts.
- **Temporary write** — a store write that the evaluate step makes, so that a later operation sees its effect.
- **Temporary state** — the state that the temporary writes of the earlier operations left. Only the evaluate step of
  the same transaction can see it.
- **Last-write data** — the stored data, kind, and TTL of an item immediately after the last write of its sequence.
- **Operation list** — the `(opIndex, operation type)` pairs of one item, in a lock row.
- **Version reference** — a condition plan or an update plan whose `requiredColumns` contains `"v"`. The expression
  reads `{ ref: "v" }`.
- **Evaluate** and **apply** — the two steps of the engine (sections 4.2.3 and 4.2.4).

## 2. Goals and requirements

### 2.1 In scope

1. `transactWriteItems` accepts `executionMode: "standard" | "ordered_per_item"`. The default is `"standard"`.
2. Standard mode keeps its current behavior. It continues to reject repeated items with `transact_duplicate_key`.
3. In `"ordered_per_item"` mode, a request can contain more than one operation for the same item, on both paths.
4. Every request applies its operations in `opIndex` order, in both modes and on both paths.
5. Prepare evaluates, and apply writes:
   - Each operation sees the effects of the earlier operations of the request.
   - Its condition, its update check, its size check, and its condition failure image use the temporary state at
     prepare.
   - Apply then makes the store writes of the operations in `opIndex` order, at the moment the transaction applies.
   - On the two-phase path, other requests run between the two steps. Thus a row that the transaction creates can
     get a higher `v` at apply than at prepare (section 4.2.8).
6. For the same start state, the single-partition path and the two-phase path give the same result. Only the
   timestamps can differ, because the two paths take `T` from different clocks.
7. All operations of one transaction use the same transaction timestamp `T`.
8. The first failure of an item is the only failure that the item reports. The later operations of that item get
   `not_evaluated`. The other items continue, so that the result reports their outcomes. One failure cancels the whole
   transaction, and the continued evaluation does not permit a partial commit.
9. The result type stays the same: one result for each `opIndex` (section 4.2.10).
10. The coordinator uses `PRIMARY KEY (transaction_id, op_index)` for `tc_items` in both modes.

### 2.2 Requirements

- The temporary state of a transaction must be invisible to every other request.
- Prepare must persist everything that commit applies. Commit must not evaluate a condition or an update plan.
- A condition or an update value must not read a `v` that apply can change (section 4.2.8).
- The rules of section 1.2 must hold. The design must reach them only through the current store writes.
- A participant must hold one lock row for each item.
- Every node must agree which operations belong to one item. Each node compares the key bytes, and no hash value
  decides that two keys are the same item.
- Commit and cancel must send each item key one time.
- The original `opIndex` of each operation must stay the same through routing, retries, recovery, and migration.
- The idempotency fingerprint must include the execution mode.
- The current request validation and limits must stay. `MAX_ITEMS_PER_TX` (100) and `MAX_PAYLOAD_BYTES_PER_TX` (4 MB)
  count operations, not unique items.
- A request with no repeated item must cost the same as now: no temporary write and one storage transaction.

### 2.3 Out of scope

- **A global order across partitions.** Each partition applies its operations in `opIndex` order. Different
  partitions apply in parallel.
- **The same `v` at prepare and at commit for a new row.** A standard `put` of an absent item has the same behavior
  now (section 4.2.8).
- **The `clientRequestToken` rule of the single-partition path.** A request with a token skips that path in both
  modes. A later change will look at that rule again.
- **Compatibility with existing data.** The schema changes edit the current migrations in place. A deployment must
  destroy the existing Durable Object namespaces (section 4.2.15).
- **A separate public method.** Section 5 gives the reason.

## 3. Milestones

Each milestone ships alone. Milestones 1 to 4 keep the results of standard mode, with two exceptions:

- After milestone 2, two different keys with the same hash are two items. The client no longer rejects them as a
  duplicate.
- After milestone 3, the two-phase commit applies in `opIndex` order. A new row can then get a different `v` when the
  same transaction also deletes another item of the partition.

Milestone 5 makes ordered mode available. Milestone 7 comes after the ordered-mode work. It changes how the
coordinator handles a prepare error that a retry cannot clear.

1. **Coordinator schema.** Edit the `tc_items` migration in place to `PRIMARY KEY (transaction_id, op_index)`. Send
   each key one time in commit and cancel. Section 4.2.9.
2. **Item identity.** Add `KeyPairMap`, and use it at each identity site in place of `KeyCodec.pairKey`. Remove
   `KeyCodec.pairKey`. Section 4.2.6.
3. **Operation list and apply.** Edit the `pending_transactions` migration in place to add `op_list`. Write one entry
   for each lock row. Make commit apply all owned lock rows in `opIndex` order, refuse a duplicate key, and refuse a
   duplicate `opIndex`. Make the migration stream carry the new column. Sections 4.2.4 and 4.2.7.
4. **Evaluate.** Make `prepareLocal` and `executeSingleShot` run the evaluate step in `opIndex` order, with temporary
   writes for a repeated request. Add the two blocks of a prepare and the version-reference check of the partition.
   Sections 4.2.3, 4.2.5, and 4.2.8.
5. **Public API.** Add `executionMode` and validate its value. Skip the duplicate check in ordered mode, and add the
   version-reference check of the client. Add the mode to the fingerprint, expose the mode in the HTTP example, and
   update the public documentation. Sections 4.2.1, 4.2.2, and 4.2.9.
6. **Cross-path tests.** Add the tests of section 4.2.16.
7. **Fatal prepare errors.** Make `prepareRetry` stop on a `FokosValidationError`. Make `runPrepareRecovery` cancel a
   transaction when a participant stored such an error. Section 4.2.17.

Until milestone 7 ships, the coordinator retries a prepare that fails the version-reference check of the
partition. A recovery drive then keeps the transaction in `PREPARING` until `maxPreparingHoldMs`. The transaction
still cancels and applies nothing. Only a caller that does not use `db.ts` can reach this case, because the client
check refuses the request first.

## 4. Proposed solution

### 4.1 High-level overview

An ordered transaction gives the result of its standalone operations. The Durable Object runs one event at a time.
Thus the engine can run the operations of a request one by one, with the current store writes, inside one storage
transaction.

The client accepts repeated items only when the caller selects `"ordered_per_item"`. Nothing below the client reads
the mode. The coordinator and the partition run the same code for every request. A request with no repeated item
takes the same steps as now.

The engine has two steps:

1. **Evaluate.** For each operation in `opIndex` order, it checks the lock, the condition, the update and the size,
   and the timestamp. In a repeated request, it also applies each passed operation as a temporary write, so that the
   next operation sees it. It reads the last-write data of each repeated item.
2. **Apply.** For each operation in `opIndex` order, it makes one store write: `upsertItem` for a write, `deleteItem`
   for a `delete`, and `bumpItemReadTs` for a `check`.

The paths use the steps in this way:

- **Single-partition path.** One `transactionSync`. In a repeated request, the temporary writes of evaluate are the
  real writes, so the block keeps them when every operation passed. In a request with no repeated item, evaluate
  writes nothing, and apply runs after it, as now. When one operation failed, the block throws and rolls back.
- **Prepare on the two-phase path.** In a repeated request, evaluate runs in its own `transactionSync` and then
  throws, so SQLite rolls back every temporary write. A second `transactionSync`, the lock block, writes one lock row
  for each item: its last-write data and its operation list. In a request with no repeated item, evaluate writes
  nothing, so the lock rows follow in the same block, as now.
- **Commit on the two-phase path.** Apply reads the operation lists of the owned lock rows and sorts the entries by
  `opIndex`. It makes one store write for each entry. Each write of an item uses its last-write data.

The data in the middle of a sequence is invisible to other requests. The visible values depend only on the order and
the types of the operations: `v`, `max_deleted_v`, the timestamps, and `key_size_estimates`. Thus apply with the
last-write data gives the same result as the standalone operations (section 4.2.4). A condition or an update value
that reads `v` after a write or a delete of the same item reads a `v` that apply can change. The client and the
partition refuse such a request (section 4.2.8).

```text
db.ts ── validate (repeated items only in ordered mode, version references), fingerprint includes the mode
  │
  ├── single-partition path ──► partition: one transactionSync
  │                               evaluate in opIndex order
  │                               repeated request: each passed operation is a real write
  │                               no repeated item: apply after evaluate, as now
  │                               all passed → commit │ one failed → throw, roll back
  │
  └── two-phase path ──► coordinator: tc_items (transaction_id, op_index)
                           │ prepare: all operations of the partition
                           ▼
                         partition prepare
                           evaluate block: temporary writes, last-write data, throw → roll back
                           lock block: one lock row per item: last-write data + operation list
                           │
                           ▼ commit / cancel: unique keys
                         partition commit
                           apply: entries of all owned lock rows, sorted by opIndex, one store write each
                           release the lock rows, in the same transactionSync
```

### 4.2 Technical details

#### 4.2.1 Public API

`TransactWriteItemsOptions` in `shared/transaction-api-types.ts` gets one field:

```ts
export type TransactWriteItemsOptions = {
	items: TransactWriteItem[];
	clientRequestToken?: string;
	/** Defaults to "standard". "ordered_per_item" accepts more than one operation for the same item. */
	executionMode?: "standard" | "ordered_per_item";
};
```

`TransactWriteItemsResult`, `TransactWriteOperationResult`, and `FokosTransactionCancelledError` do not change.
`TransactWriteItemsResult` carries no `v`, so no response depends on the values that apply writes.

The HTTP example exposes the mode. `TransactWriteItemsBodySchema` in `examples/http-api/src/rpc/schemas.ts` gets
`executionMode` as an optional field with the two values. The `transactWriteItems` route in
`examples/http-api/src/rpc/routes.ts` already passes the parsed body to `FokosDB.transactWriteItems`.

These documentation comments change:

- `TransactWriteOperationResult` — `not_evaluated` also means that an earlier operation of the same item failed.
- `returnValuesOnConditionCheckFailure` — in ordered mode, the image can show a temporary state. The transaction
  cancelled, so that state never committed.
- The public `version` — in ordered mode, a condition or an update value that reads `v` must come before every
  `put`, `update`, and `delete` of the same item. An earlier `check` is permitted. Otherwise the request fails with
  `transact_version_after_write` (section 4.2.2).
- `TransactWriteItemsOptions.executionMode` — the same rule in one sentence, and the reason: the `v` of a row that
  the transaction creates can be higher at commit than at prepare (section 4.2.8).

#### 4.2.2 Validation and limits

`db.ts` validates `executionMode` before any other check of the request. A value other than `"standard"` or
`"ordered_per_item"` fails with a `FokosValidationError` with the new code `transact_execution_mode_invalid` in
`VALIDATION_CODES`. An absent value is `"standard"`. The check runs on both paths, before the request leaves the
client. Thus no node below the client sees a mode that is not valid.

`validateTransactWriteOperations` in `shared/transaction-limits.ts` gets the execution mode. In ordered mode, it skips
the `transact_duplicate_key` check. In standard mode, the check finds a duplicate with `KeyPairMap`. Thus it rejects
two operations only when their key bytes are equal (section 4.2.6). Every other check stays the same for both modes:

- The key validation and the canonical key bytes.
- `MAX_ITEMS_PER_TX` (100), counted over operations.
- `MAX_PAYLOAD_BYTES_PER_TX` (4 MB), summed over the data of all operations.
- `MAX_ITEM_BYTES` (400 KB) for the data of each put.
- The rules for each operation type: data, condition, and update plan.

**The version-reference check.** `validateTransactWriteOperations` refuses an operation when both are true:

- Its condition plan or its update plan is a version reference.
- An earlier operation of the same item in the request is a `put`, an `update`, or a `delete`.

The refusal is a `FokosValidationError` with the new code `transact_version_after_write` in `VALIDATION_CODES`. Its
attributes are the `opIndex` of the operation, the `opIndex` of the earlier operation, and the keys. An earlier
`check` does not count, because a `check` does not change `v`. The check groups the operations by item with
`KeyPairMap`, and it reads only the `requiredColumns` of the compiled plans. It runs in both modes, but it never
refuses in standard mode, because standard mode has no repeated item. Section 4.2.8 gives the reason for the rule.

The check is conservative:

- On a row that exists before the transaction and that the sequence does not delete, `v` is the same at prepare and at
  apply. A version reference after a write is then safe.
- The client cannot know that the row exists, so it also refuses that request.
- A caller has three options. It can put the condition on `v` on the first operation of the item, where it reads the
  committed `v`. It can use a literal in place of an update value that reads `v`. It can send two transactions.

The client is the validation boundary. The partition runs the same check as a guard (section 4.2.3, step 2). Thus a
caller that sends the RPC without `db.ts` cannot store a `v` that apply changes.

`db.ts` keeps `opIndex = i` for request position `i`. `InitiateWriteRequest` gets `executionMode`, and the
coordinator uses it only for the fingerprint. `SingleShotRequest` and `PrepareRequest` do not carry the mode.

#### 4.2.3 Evaluate

One function in `TransactionParticipant` serves `prepareLocal` and `executeSingleShot`. It replaces the check pass of
both methods and the duplicate-key invariant at the start of `prepareLocal`. It does these steps:

1. Sort the operations by `opIndex`. The order of the request array does not matter.
2. Group the operations by item with `KeyPairMap` (section 4.2.6). The grouping finds the repeated items and keeps
   the state of each item: failed or not, the lock result, and the committed stamps. Then run the version-reference
   check of section 4.2.2 on the groups. When an operation fails it, throw the same `FokosValidationError`. The check
   runs before the first SQL statement, so the throw writes nothing. Section 4.2.17 gives what the coordinator does
   with the error.
3. On the two-phase path of a repeated request, read the committed `max_delete_tx_order_ts` one time, before the
   first temporary write (section 4.2.3.1).
4. For each operation, in `opIndex` order:
   1. When an earlier operation of the item failed, give it `not_evaluated`.
   2. At the first operation of the item, check the lock against the committed state. A lock of another transaction
      gives `pending_conflict`. A lock of this transaction makes every operation of the item `passed`, with no
      evaluation and no temporary write (section 4.2.12).
   3. Evaluate the condition against `items`. On a failure, read the image with `#imageForFailedCondition`.
   4. Run `#precheckWrite` against `items`.
   5. On the two-phase path, check the timestamp against the committed stamps (section 4.2.3.1).
   6. When a check fails, record the failure, and mark the item as failed.
   7. When the operation passed and the request is a repeated request, apply the operation as a temporary write.
   8. When the operation is the last write of a repeated item, read the last-write data from the row that it wrote.
5. Return one result for each operation, the last-write data of each repeated item, and whether a temporary write
   occurred.

Step 4.7 uses the current store writes with `T`: `upsertItem` for a `put`, `updateItemSingleShot` for an `update`,
`deleteItem` with `bumpTxOrderTsAlways` for a `delete`, and `bumpItemReadTs` for a `check`. In a repeated request,
each item gets temporary writes, also an item with one operation. Thus the temporary state at each operation is the
state that apply gives at the same position, for every item.

At steps 4.3 and 4.4 of an operation, `items` holds the state that the earlier operations left. In a request with no
repeated item, evaluate makes no temporary write, so each check reads the committed state, as now.

When an operation fails, evaluate drops the last-write data that it read, because no lock block and no commit follow.

The temporary writes report promotion candidates, and evaluate discards them. On the single-partition path, the block
reports the candidate of the last write of each item whose final state is present, when the block commits.

##### 4.2.3.1 The timestamp rule

The timestamp check uses only the committed state from before the transaction. Conditions, update checks, and size
checks use the temporary state.

The reason: all operations of a transaction use one timestamp `T`. A check against the temporary state rejects valid
sequences:

- `put → check`: the put sets `last_read_ts = last_write_ts = T`. The check then finds `T <= T`.
- `delete → put`: the delete sets `max_delete_tx_order_ts` to at least `T`. The put of the absent item then finds
  `T <= T`.

The committed values come from two places. Evaluate reads each of them before a temporary write can change it:

- **`max_delete_tx_order_ts`** — `deletion_metadata` holds one value for the whole partition (`id = 1`). A temporary
  delete of any item raises it. Thus evaluate reads it one time, before the first operation, and uses that value for
  every item.
- **The stamps of an item** — `last_read_ts` and `last_write_ts` belong to the row of the item. Only the operations of
  that item write the row. The first operation of the item reads them, as now, and evaluate keeps them for the later
  operations of the item.

Without the first rule, a temporary delete of one item makes a later absent item fail. An example: item A exists and
has `delete → put`, and item B is absent and has `put`. The temporary delete of A raises the watermark to `T`, and the
put of B then finds `T <= T`.

Each operation applies its own watermark rule, as now: a `check` uses `last_write_ts`, and a mutation uses
`last_read_ts`. The failure goes to the first operation that does not pass.

This changes one detail of `prepareLocal`. The current code takes the stamps from the condition read or the update
probe (`conditionResult ?? probe`). For operation 2 and later of an item, that read sees the temporary row. Thus
evaluate must use the kept stamps for those operations.

`clock_skew` and `pending_conflict` also use the committed state. The single-partition path has no timestamp check,
as now.

#### 4.2.4 Apply

One function in `TransactionParticipant` applies a list of entries. Each entry is an `opIndex`, an operation type,
an item key, and the source of the data of the item. The function does these steps:

1. Check with `invariant()` that no two entries have the same `opIndex`.
2. Sort the entries by `opIndex`.
3. For each entry, make one store write with `T`:
   - **`put` and `update`** — `upsertItem` with the data, kind, and TTL of the source.
   - **`delete`** — `deleteItem` with `bumpTxOrderTsAlways`.
   - **`check`** — `bumpItemReadTs`.
4. Return the promotion candidate of the last write of each item whose final state is present.

The callers give these sources:

| Caller | Item | Source of the data |
| --- | --- | --- |
| Commit on the two-phase path | every item | the `data`, `data_kind`, and TTL of the lock row |
| Single-partition path, no repeated item | the item of a `put` | the request data, as now |
| Single-partition path, no repeated item | the item of an `update` | the update plan: the entry uses `updateItemSingleShot`, as now |

The single-partition path of a repeated request does not call apply. Its temporary writes are already the writes of
apply in `opIndex` order (section 4.2.5).

**Why the last-write data gives the same result.** Every write of an item in apply uses the last-write data, also
the writes in the middle of the sequence. The data of those writes is invisible: they run in the same
`transactionSync`, and a later write or delete replaces them. The visible values do not depend on that data:

- `v` goes up by 1 for each write of a row. A new row starts at `max_deleted_v + 1`.
- A delete raises `max_deleted_v` to the `v` of the row, and `max_delete_tx_order_ts` to at least its `last_read_ts`.
- Each write moves the stamps to `MAX(current, T)`, or to at least `max_delete_tx_order_ts` for a new row.
- Each write and delete changes `key_size_estimates` by the difference between the old row and the new row. Thus the
  sum ends at the size of the final row.
- Each insert of a new row gets a new `item_id`, in the same order.

The last-write data itself is the same at prepare and at apply. An update value can read only the keys, the TTL, the
data, and `v` of its own item. The first three do not depend on `max_deleted_v`, and the version-reference check
keeps `v` stable (section 4.2.8).

An example on item A with `v = 4`, in a partition with `max_deleted_v = 2`. The sequence is
`put(1 KB) → delete → put(5 KB)`. The lock row holds the 5 KB data and the operation list `[put, delete, put]`:

| Step | Standalone operations | Apply with the last-write data |
| --- | --- | --- |
| `put` | `v = 5`, 1 KB | `v = 5`, 5 KB |
| `delete` | `max_deleted_v = 5` | `max_deleted_v = 5` |
| `put` | new row, `v = 6`, 5 KB | new row, `v = 6`, 5 KB |

When the final state of an item is absent, apply uses its last-write data only for the writes before the last delete.
When the sequence has no write, the item has no data, and apply makes no `upsertItem` call for it.

The last write passed `#precheckWrite`, so the last-write data fits in `MAX_ITEM_BYTES`. Thus each write of apply
passes the size guard of `upsertItem`.

#### 4.2.5 The paths

**The single-partition path.** `executeSingleShot` runs one `transactionSync`:

1. Run evaluate.
2. When one operation failed, throw a sentinel that carries the results. SQLite rolls back every write.
3. In a repeated request, the temporary writes are the real writes of every operation, in `opIndex` order. The block
   returns, and all writes commit.
4. In a request with no repeated item, evaluate wrote nothing. Run apply with the request as the source, and return.

**The prepare.** `prepareLocal` runs one or two `transactionSync` calls:

1. **The evaluate block.** It runs evaluate. It copies every condition failure image and the last-write data into
   JavaScript.
2. When evaluate made a temporary write, the block throws a sentinel that carries the results and the last-write
   data. SQLite rolls back every write of the block.
3. When evaluate made no temporary write and every operation passed, the same block continues with the lock writes,
   as now.
4. **The lock block.** After a rollback, it runs only when every operation passed. It writes the `pending_tx_info`
   row and one `pending_transactions` row for each item. It writes no lock row for an item that this transaction
   already locks (section 4.2.12).

The two blocks are safe for these reasons:

- No `await` runs between them, and the Durable Object runs one JavaScript event at a time. No other request can read
  `items` between the blocks.
- The evaluate block commits nothing. When the object stops between the two blocks, no lock exists, and the
  coordinator sends the prepare again.
- The `dispatch` of the sharding runtime does not wrap a local handler in a `transactionSync`. The blocks are top-level
  transactions, so they need no nested transaction.
- `PartitionStore` keeps no in-memory state. Its fields are `#storage` and `#migrations`, and it does not use
  `ctx.storage.kv`. Thus a rollback leaves no stale value in memory.

A rollback must not change any in-memory state. This applies to each value that a block changes: the Bloom filter,
the size estimates, the TTL timer, and every cache. Promotion candidates and job signals go to `call.signal(...)` only
after the block committed.

**The commit.** `commitLocal` runs one `transactionSync`, as now:

1. Read the owned lock rows, and compare their keys with the request (section 4.2.7).
2. Build one apply entry for each pair of the operation list of each owned row. The source of each entry is its lock
   row.
3. Run apply.
4. Release the lock rows of the request keys.

Apply and the release run in one `transactionSync`. Thus a commit applies all operations of the partition and
releases its locks, or it changes nothing.

#### 4.2.6 Item identity

**The problem.** In the current code, each identity site uses `KeyCodec.pairKey`. It is a 128-bit value made of two
xxhash64 values with a fixed public seed. It is a hash, so two different keys can have the same value. A caller can
find two such sort keys with a birthday search of about 2^32 hashes.

The current client rejects such a pair as a false `transact_duplicate_key`, so the pair never reaches a partition.
Ordered mode skips that check. The coordinator and SQLite compare the key bytes, but the participant compares
`pairKey`. The two sides then disagree about the number of items:

- When evaluate groups by `pairKey`, two items become one sequence. One item gets no lock, but its operations report
  `passed`. Commit writes only the other item, so the transaction is not atomic.
- When evaluate groups by bytes, commit collapses the two keys into one entry. Then one of two failures occurs:
  - The duplicate check throws on each try. The transaction stays in `COMMITTING` and keeps both locks.
  - Commit writes the lock row of one item into the other item.

**The rule.** Two keys are the same item only when the bytes of their hash keys and of their sort keys are equal. A
hash value can select where to look for a key, but only a byte comparison decides that two keys are equal.

**The primitive.** `KeyPairMap<V>` in `sharding/key-codec.ts` is a map from a `(hashKey, sortKey)` pair to a value:

- Up to 8 entries, it keeps the entries in an array. It finds a key with a byte comparison of each entry.
- Above 8 entries, it also keeps a `Map` from `keyPairHash(hashKey, sortKey)` to the entries with that hash. It finds
  a key with one hash and a byte comparison of the entries in that bucket.
- `keyPairHash` in `sharding/key-codec.ts` is `hash32(sortKey, hash32(hashKey, KEY_PAIR_SEED))`. `KEY_PAIR_SEED` is a
  random 32-bit value. The module makes it one time, with `crypto.getRandomValues`, when the isolate loads it. The map
  lives only in the memory of one call, so the seed does not have to be stable. A caller cannot know the seed, so a
  caller cannot choose keys that go into one bucket.
- The byte comparison checks the two lengths first. Then it compares the bytes from the last byte to the first. The
  sort key goes first, because two keys of one transaction usually have the same hash key and differ in the sort key.
- It allocates no string and no `BigInt` for a key.

A map with a hash key only, such as the owner cache of `ownsByHashKey`, uses the same class with the empty sort key.

**The sites.** Each of these uses `KeyPairMap`:

- The duplicate check and the version-reference check of `validateTransactWriteOperations` (section 4.2.2).
- The duplicate-key invariant of `prepareLocal`, until milestone 4 replaces it with the grouping of evaluate.
- The grouping of evaluate (section 4.2.3, step 2).
- `commitLocal`: the key set of the request, the map of owned lock rows, the duplicate-key check (section 4.2.7), and
  the owner cache of `ownsByHashKey`.
- `validateTransactGetItemKeys`, and the pairing of the two phases of a read transaction in `db.ts`. These sites are
  safe now, because the client rejects a colliding pair. They change so that the codebase has one identity rule.

After the change, `KeyCodec.pairKey` has no caller, and the change removes it. The coordinator needs no change. SQLite
compares `BLOB` values by bytes, so `SELECT DISTINCT` in `loadItemKeys` and the primary keys of `tc_items` and
`pending_transactions` already use the rule.

**The measured cost.** A workerd test inside `@cloudflare/vitest-plugin` measured the work to find the distinct items
of one request. Each value is the median of 7 batches of at least 200 ms. The 2 KiB keys are the worst case. A long
shared prefix makes a comparison from the first byte slow. A long shared suffix makes a comparison from the last byte
slow.

| Option | 2 ops, 1 item | 10 ops, one hash key | 100 ops, one hash key | 100 ops, 100 hash keys | 100 ops, 2 KiB keys, shared prefix | 8 ops, 2 KiB keys, shared suffix | 100 ops, 2 KiB keys, shared suffix |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `pairKey` `Set` (current code, not exact) | 0.28 µs | 1.6 µs | 16 µs | 47 µs | 51 µs | 3.9 µs | 75 µs |
| `pairKey` `Map` and a byte check | 0.35 µs | 1.9 µs | 19 µs | 80 µs | 58 µs | 4.2 µs | 110 µs |
| Byte comparison of each pair | 0.03 µs | 0.16 µs | 18 µs | 49 µs | 19 µs | 55 µs | 10 ms |
| Sort, then compare neighbors | 0.13 µs | 1.3 µs | 6.8 µs | 4.3 µs | 250 µs | 18 µs | 246 µs |
| base64 text in a `Set` | 0.55 µs | 2.9 µs | 29 µs | 30 µs | 173 µs | 13 µs | 178 µs |
| hex text in a `Set` | 0.33 µs | 1.7 µs | 17 µs | 15 µs | 590 µs | 44 µs | 568 µs |
| `hash32` `Map` and a byte check | 0.24 µs | 1.2 µs | 12 µs | 12 µs | 69 µs | 5.2 µs | 68 µs |
| **`KeyPairMap`** (array up to 8, then `hash32`) | **0.03 µs** | **1.3 µs** | **13 µs** | **13 µs** | **71 µs** | **57 µs** | **71 µs** |

The results:

- For a small request with short keys, `KeyPairMap` is the fastest option: 0.03 µs for 2 operations, against
  0.28 µs for the current code. Above 8 entries, it costs the same as the `hash32` map.
- Its worst case is about 71 µs, at 100 operations with 2 KiB keys. With the hash, the worst case grows with the bytes
  of the keys, and not with the square of the number of operations. The byte comparison of each pair takes 10 ms in
  its worst case, and the sort takes 250 µs.
- The 71 µs bound needs keys that spread over the buckets. With a fixed public seed, a caller can make 100 keys with
  one hash value. The map then costs the same as the byte comparison of each pair: 10 ms. The random seed of
  `keyPairHash` prevents this.
- The array part has its own worst case, 57 µs at 8 operations with 2 KiB keys. That is less than the worst case of
  the hash part, so the limit of 8 does not raise the worst case of the map.
- With short keys, it costs less than the `pairKey` `Set` of the current code. With 2 KiB keys, it costs up to 20 µs
  more, because the exact answer needs a byte comparison.
- `hash32` returns a number, so the map needs no `BigInt`. A 32-bit hash has more collisions than `pairKey`. A
  collision costs one more byte comparison and does not change the answer. With 100 keys, a collision occurs in about
  one request of a million.
- Each value is small compared with one RPC of the transaction. The choice keeps the exact answer and a bounded worst
  case at no cost in the usual path.

#### 4.2.7 Lock row and commit

The change edits the `pending_transactions` migration of `PartitionStore` in place and adds one column:

```sql
op_list               TEXT    NOT NULL,
```

`op_list` holds the operation list of the item as JSON: an array of `[opIndex, operation]` pairs in `opIndex` order,
for example `[[0,"delete"],[1,"put"],[4,"update"]]`. A lock row of an item with one operation holds one pair. Each
store method that reads `op_list` checks its form with `invariant()`: an array of pairs, each `opIndex` an integer,
and each operation one of `put`, `update`, `delete`, or `check`.

The other columns of a lock row:

- **`operation`** — the last operation of the sequence that is not a `check`, or `check` when every operation is a
  `check`. `readForTransactionLocal` reads it for `hasPendingWrite`, as now. Apply does not read it.
- **`data`, `data_kind`, `ttl_epoch_utc_seconds`** — the last-write data of the item, or none when the sequence has no
  write.

The data of each lock row comes from one of these places:

| Item | Where the lock row data comes from | Copied into JavaScript? |
| --- | --- | --- |
| One `put` | the request data, as now | no, it is already in memory |
| One `update` | `INSERT … SELECT` on the committed row, inside SQLite, as now | no |
| One `delete` or `check` | no data | no |
| Repeated item with a write | the row after its last temporary write (section 4.2.3, step 4.8) | **yes** |
| Repeated item with no write | no data | no |

The temporary row exists only inside the evaluate block, and the lock block runs after the rollback. That is why a
repeated item must copy its data.

The read at step 4.8 is a new store method. It returns the stored bytes, the kind, and the TTL of the row.
`getItemImage` is not correct for it, because it decodes JSONB to JSON text, and a JSONB to text to JSONB round trip is
not size-stable. The comment of `insertPendingUpdateLock` gives the reason: the bytes that the checks measured must be
the bytes that commit writes. `itemDataExpr` binds a JSONB `Uint8Array` verbatim, so `upsertItem` writes the stored
bytes without a change.

**The size of a lock row.** A lock row holds one copy of the data and the operation list:

| Part | Largest size |
| --- | --- |
| `hk` and `sk` | 1024 B + 512 B (`MAX_HASH_KEY_BYTES`, `MAX_SORT_KEY_BYTES`) |
| Last-write data | 400 KB (`MAX_ITEM_BYTES`) |
| `op_list` | 100 pairs of about 15 B: about 1.5 KB |

The largest row is about 403 KB. A lock row of the current code holds up to 400 KB, so the change adds only the
operation list. The list holds no data, no condition, and no update plan.

**Commit.** `commitLocal` keeps its rules for the key set, the copies, and the release of each key. It adds these
rules:

- It fails with an `invariant()` error when the request contains one key two times. The check, the key set of the
  request, and the map of owned lock rows use `KeyPairMap` (section 4.2.6).
  - The check is necessary because `commitLocal` compares the size of the request key set with the number of owned
    lock rows. A duplicate key passes that comparison, and apply then applies the entries of one lock row two times.
  - The check is an invariant because the coordinator sends unique keys (section 4.2.9). A duplicate key is thus a
    defect in the code, not an input of a caller. The commit fails, and the coordinator stays non-terminal and
    retries.
- It applies the entries of all owned lock rows in one apply call, in `opIndex` order. The order of the keys in the
  request does not matter.
- It takes everything that it applies from the lock rows. The commit request carries keys only, as now, and
  `stripPayload` can remove the payload of the coordinator in `COMMITTING`.

A cancel with a duplicate key releases the same lock two times, which changes nothing. Thus cancel keeps its current
behavior.

The migration stream must carry the new column. `pendingTxPageStatement` selects it, and `insertPendingLock` writes it
on the target.

#### 4.2.8 Versions at prepare and at commit

The single-partition path evaluates and applies in one block, so the temporary state and the committed state are the
same.

On the two-phase path, other requests run between prepare and commit. The locks keep each locked row unchanged:

- A non-transactional write to a locked item fails.
- Another transaction gets `pending_conflict`.
- The TTL sweep skips a locked item.

The partition-wide `deletion_metadata` row has no lock. A delete of another item can raise `max_deleted_v` between
prepare and commit. A row that the transaction creates then gets a higher `v` at commit than in the temporary state.
An example on an absent item X with `put → update → check`, in a partition with `max_deleted_v = 10`:

1. Evaluate creates the temporary row with `v = 11`. The `check` sees `v = 12`.
2. Before the commit, a delete of another item with `v = 50` raises `max_deleted_v` to 50.
3. Apply creates X with `v = 51`, then updates it to `v = 52`.

Commit must not write the `v` of the temporary state. A new row must start above the `max_deleted_v` of the moment
it is written. A read transaction depends on that rule to find an item that is created and deleted between its two
phases. A standard `put` of an absent item has the same behavior now: its `v` comes from the `max_deleted_v` of the
commit.

When the row of the item exists before the transaction and the sequence has no `delete`, the committed `v` is the
temporary `v`, because apply adds 1 for each write.

**The difference must not reach a decision or the data.** In the example, a condition of the `check` on `v` passes on
`v = 12` at prepare, and that row never exists. An update value that reads `v` stores a `v` in the data that the
committed row does not have. For example, `put X → update X SET $.prevVersion = v` stores `11`, and X commits with
`v = 52`. The version-reference check refuses both requests: the client check of section 4.2.2, and the partition
check of section 4.2.3, step 2. Thus the higher `v` at commit is visible only as the `v` of the row.

#### 4.2.9 Coordinator

**Schema.** The change edits the `tc_items` migration in place. The primary key becomes:

```sql
PRIMARY KEY (transaction_id, op_index)
```

The other columns do not change. `db.ts` gives each operation of a transaction a different `op_index`, so the new key
holds every operation.

**Keys and payload.**

- `initiateWrite` writes one `tc_items` row for each operation, as now.
- `loadItems` reads the rows `ORDER BY op_index`, as now. Thus a prepare and a prepare from recovery send the
  operations in request order. The participant sorts them again (section 4.2.3).
- `loadItemKeys` must return each `(hk, sk, partition_do_name)` one time, for example with `SELECT DISTINCT`.
  `runCommit` and `runCancel` use it, so commit and cancel send each key one time. All operations of an item have the
  same `partition_do_name`, because the root partition depends on the hash key only. The order of the keys does not
  matter, because apply sorts by `opIndex`.
- `cancelTransactionInStore` builds the positional results from `loadItems`, as now.
- `applyMigrationPage` uses `INSERT OR REPLACE INTO tc_items`. The new primary key keeps the insert idempotent.

**Fingerprint.** `hashTransactionOperations` gets the execution mode. In standard mode, the hash must stay the same as
now. In ordered mode, the function chains the mode into the hash. `db.ts` sends `"standard"` when the caller gives no
mode, so a missing mode and `"standard"` give the same hash. `tc_state` gets no column for the mode, because the
fingerprint is the only use of the mode, and `operations_hash` holds it.

#### 4.2.10 Result rules

Every operation gets one result, at the position of its `opIndex`:

- **Every operation of the item passed.** Each operation gets `passed`.
- **Operation *k* failed a check of its own.** The operations before *k* get `passed`. Operation *k* gets `rejected`
  with its reason. The operations after *k* get `not_evaluated`.
- **Another transaction holds the lock of the item.** The first operation gets `rejected` with `pending_conflict`.
  The later operations get `not_evaluated`.
- **The partition refused the whole request**, for example with `clock_skew`. Every operation of that partition gets
  the code, as now.
- **The partition failed with an execution error.** Every operation of that partition gets the code and the
  `error_id`, as now.
- **This transaction already holds the lock of the item** (a repeated prepare). Each operation gets `passed`.

The checks of one operation run in the order of section 4.2.3, step 4: the lock, the condition, `#precheckWrite`, and
then the timestamp. The first check that fails gives the reason.

`applyImageCap` already sorts by `opIndex` before it caps the image bytes. Thus the grouping by item does not change
which images it drops.

#### 4.2.11 Condition failure images

`#imageForFailedCondition` reads the row from `items` in the same block as the condition. For operation *k*, `items`
holds the temporary state that the earlier operations left. Thus the image shows the state immediately before
operation *k*, with its temporary data, `v`, and TTL.

- When an earlier operation deleted the item, the condition sees no item, and the result has no image.
- When the first operation of an item fails, the image shows the committed state, as now.
- The evaluate block copies the image into JavaScript before it rolls back.
- `MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX` (10 MiB) and `applyImageCap` apply as now.

#### 4.2.12 Routing, retries, recovery, and migration

- **Routing.** `txPrepare` is a `group` operation. All operations of an item must go to the same owner in one
  sub-request. The runtime must resolve equal keys of one dispatch to the same owner, also when the Bloom filter
  takes part. A test must prove this rule.
- **Order.** The participant sorts by `opIndex` in evaluate and in apply. Thus a change of the array order by routing,
  forwarding, or migration has no effect.
- **Repeated prepare.** When this transaction already holds the lock of an item, every operation of the item gets
  `passed`. Evaluate does not evaluate the item again, and the lock block writes no lock row for it, because its lock
  row already holds the result of the first prepare. When this transaction locks every item of the request, the
  prepare writes nothing and answers `accepted`. `insertPendingLock` keeps `INSERT OR IGNORE`, because the migration
  stream can send one lock row two times.
- **Late prepare after a commit.** The commit removed the lock rows. The timestamp check then rejects the prepare of
  each item that has a `put`, an `update`, or a `delete`:
  - A write stamps `last_read_ts` and `last_write_ts` with at least `T`.
  - A `delete` raises the watermark to at least `T`.
  - An item with only `check`s can pass, present or absent. A `check` compares `last_write_ts`, and its commit raises
    only `last_read_ts`.

  When every item of the late prepare has only `check`s, the prepare passes and takes a lock again. The
  stale-transaction job then asks the coordinator, which answers `COMMITTED`, or `not_found` after the idempotency
  window. The partition then commits the lock, which applies the `check`s again, or it cancels the new lock. Neither
  changes an item. Until then, the lock refuses non-transactional writes to its keys. Standard mode has the same
  behavior now (`2026-10-03-max-deleted-version.md`). A fix needs a record of finished transactions in the partition,
  and this design does not add one.
- **Repeated commit.** The first commit released the lock rows in the same `transactionSync` as apply. A repeated
  commit finds no owned row and answers the idempotent `committed`. Apply never runs two times.
- **Commit with part of the keys.** The current key-set check fails it with `commit_keyset_mismatch`, and the
  coordinator retries. Thus a partition applies all of its entries in one call.
- **Cancel.** It deletes the lock rows by key, as now.
- **Recovery.** The stale-transaction job and `debugForceResolveTransaction` send the outcome through `dispatch`. The
  owner applies its lock rows with the same apply.
- **Partition migration.** A lock row moves with its last-write data and its `op_list` (section 4.2.7). The target
  merges `max_deleted_v` with `MAX` before it accepts a write. When a split puts the items of one transaction on two
  partitions, each partition applies its own entries in `opIndex` order.
- **Coordinator migration.** A migration page carries the `tc_items` rows with their `op_index`.

#### 4.2.13 Invariants

Each invariant names the mechanism that holds it:

- **No request sees a temporary state.** The evaluate block of a prepare always rolls back when it made a temporary
  write. No `await` runs between the two blocks (section 4.2.5).
- **A transaction commits all items or none.** Prepare writes locks only when every operation passed. The
  single-partition path rolls back on one failure.
- **Commit evaluates nothing.** The lock row holds the last-write data and the operation list.
- **No plan reads a `v` that apply can change.** The client and evaluate both run the version-reference check
  (section 4.2.8).
- **Each partition applies in `opIndex` order.** Evaluate and apply sort by `opIndex`. Apply refuses a duplicate
  `opIndex`.
- **The rules of section 1.2 hold.** Every change to `items` and `deletion_metadata` goes through the current store
  writes. No path writes `v` or `deletion_metadata` with a value that it computed.
- **One lock for each item.** `pending_transactions` keeps `PRIMARY KEY (hk, sk, transaction_id)`. The lock block
  writes one row for each item.
- **Each `opIndex` keeps its value.** `db.ts` sets it. `tc_items` uses it as its key. Every wire type and `op_list`
  carry it.
- **A rollback leaves memory unchanged.** `PartitionStore` has no in-memory state, and signals go out only after a
  commit (section 4.2.5).

#### 4.2.14 Performance

| Request | Evaluate | Lock block | Apply at commit or in the single-partition block |
| --- | --- | --- | --- |
| No repeated item, both paths | reads only, as now | one row for each item, as now | one write for each operation, as now |
| Repeated request, two-phase path | one temporary write for each passed operation, rolled back | one row for each item | one write for each operation |
| Repeated request, single-partition path | one write for each passed operation, kept | none | none: the evaluate writes are the apply writes |

- A repeated request on the two-phase path writes each operation two times: one time in evaluate, which rolls back,
  and one time at commit. A rejected repeated request pays the temporary writes up to the operation that failed.
- Commit writes each operation, not each item. This is the same number of writes as the standalone operations.
- Each write of apply in the middle of a sequence writes the last-write data, up to 400 KB. The writes run in one
  `transactionSync`.

##### 4.2.14.1 Memory of the last-write data

A prepare holds the last-write data of its repeated items in JavaScript between the evaluate block and the lock block.
The data must leave SQLite, because the rollback discards every row that the block wrote.

The worst case of an accepted prepare is about 20 MB. A request has at most `MAX_ITEMS_PER_TX` (100) operations, so it
has at most 50 repeated items. Each last-write data is at most `MAX_ITEM_BYTES` (400 KB). An example:

1. A request has 50 items, each with `update → update`. Each committed item holds 400 KB. An update carries no data,
   so the request payload is small, and `MAX_PAYLOAD_BYTES_PER_TX` does not limit it.
2. Evaluate makes 100 temporary writes and reads the row after the second update of each item: 50 copies of 400 KB,
   20 MB in all.
3. The block throws, and SQLite rolls back the temporary writes. The 50 copies stay in JavaScript.
4. The lock block writes 50 lock rows from the copies. The copies are then garbage.

A rejected prepare can hold copies and condition failure images at the same time, until it drops the copies. Each
operation adds at most one copy or one image of at most 400 KB, so the peak is at most about 40 MB. The memory of a
Durable Object is 128 MB. Standard mode already has the same worst case: `prepareLocal` collects every condition
failure image before `applyImageCap` drops the images above 10 MiB. Thus a rejected request with 100 operations on
items of 400 KB holds about 40 MB of images.

##### 4.2.14.2 Measured cost

A scratch benchmark measured each step on Durable Object storage in the local Workers runtime of
`@cloudflare/vitest-plugin`, on the current `PartitionStore`:

- **A repeated request** runs the steps of this design with the current store methods:
  - Evaluate: `pendingLockFor` and `getItemStamp` at the first operation of an item, and `measureItemBytes` or
    `probeUpdate` for each operation.
  - The temporary writes: `upsertItem`, `updateItemSingleShot`, and `deleteItem`.
  - The read of the last-write data of each item.
  - The lock block: `insertPendingLock` with the copied data, and `insertPendingUpdateLock` for an item with one
    `update`.
  - The commit: `listPendingTxItems`, one store write for each operation in `opIndex` order with the last-write data,
    and `deletePendingTxKeys`.
- **A request with no repeated item** runs the current `prepareLocal`, `commitLocal`, and `executeSingleShot`, because
  the design keeps that code.
- The operations have no condition. The benchmark keeps the operation lists in JavaScript, so the numbers do not
  include the `op_list` column, which is at most about 1.5 KB for each item.
- Each item holds a committed JSON item of the given size before each run. A `put` writes a document of the same
  size. An `update` sets `$.n` to `if_not_exists($.n, 0) + 1`.

The columns:

- **reads only** — the checks of evaluate with no temporary write.
- **evaluate block** — the checks, the temporary writes, the reads of the last-write data, and the rollback.
- **prepare** — the evaluate block and then the lock block. For a request with no repeated item, it is the one block
  of `prepareLocal`.
- **commit** — the apply and the release of the locks, in one `transactionSync`.
- **single-partition path** — evaluate with the temporary writes kept, in one `transactionSync` that commits.

Each value is the median of 15 runs, less the median of an empty `runInDurableObject` call (2 ms). The timer has a
resolution of 1 ms. The benchmark ran two times, and the table gives both values.

| Transaction | reads only | evaluate block | prepare | commit | single-partition path |
| --- | --- | --- | --- | --- | --- |
| 2 items × 2 operations, 100 B | 0 / 0 ms | 4 / 4 ms | 4 / 5 ms | 4 / 5 ms | 4 / 5 ms |
| 10 items × 10 operations, 1 KB, 1 delete for each item | 1 / 2 ms | 7 / 8 ms | 8 / 8 ms | 6 / 6 ms | 7 / 7 ms |
| 1 item × 100 operations, 40 KB | 3 / 3 ms | 16 / 17 ms | 17 / 17 ms | 6 / 6 ms | 17 / 16 ms |
| 10 items × 2 operations, 380 KB | 6 / 6 ms | 63 / 63 ms | 80 / 80 ms | 31 / 113 ms | 44 / 46 ms |
| 50 items × 2 operations, 1 KB | 1 / 2 ms | 8 / 9 ms | 9 / 9 ms | 7 / 8 ms | 8 / 9 ms |
| 1 item × 100 updates, 390 KB | 7 / 7 ms | 27 / 29 ms | 29 / 29 ms | 15 / 15 ms | 29 / 27 ms |
| 50 items × 2 updates, 390 KB | 13 / 16 ms | 216 / 226 ms | 291 / 299 ms | 163 / 168 ms | 184 / 191 ms |
| 1 item × 2 updates and 98 items × 1 update, 390 KB | 14 / 14 ms | 386 / 376 ms | 550 / 552 ms | 301 / 304 ms | 305 / 309 ms |
| 100 items × 1 update, 390 KB, no repeated item | 17 / 16 ms | — | 168 / 167 ms | 290 / 275 ms | 291 / 292 ms |

The results:

- The cost grows with the bytes that a step writes to different rows. 100 updates of one 390 KB item take about
  28 ms in the evaluate block. 100 updates over 50 items of 390 KB take about 220 ms.
- With small data, the evaluate block costs the same as the single-partition path, which makes the same writes and
  commits. With large data, the rollback costs 20 % to 40 % more: 63 ms against 45 ms, 221 ms against 188 ms, and
  381 ms against 307 ms.
- The worst case of section 4.2.14.3 is the row with 1 repeated item and 98 single updates. Its prepare takes about
  550 ms, against about 168 ms for the prepare of 100 single updates with no repeated item. The evaluate block takes
  about 380 ms of the 550 ms.
- The commit of a repeated request costs less than its single-partition path, because the commit makes no check:
  15 ms against 28 ms for 1 item with 100 updates. For many large items, the commit costs the same as the commit of a
  request with no repeated item: about 300 ms for 99 items and about 280 ms for 100 items.
- The single-partition path of a repeated request costs about the same as a request with no repeated item: about
  307 ms for 99 items and about 291 ms for 100 items.
- The commit of "10 items × 2 operations, 380 KB" gave 31 ms in one run and 113 ms in the other. The other cells
  changed by at most 15 ms between the two runs.
- The pricing page of Durable Objects does not say if a rolled-back row counts as a row written. In the worst case,
  each temporary write counts. One temporary write writes about 2 rows, the item and its `key_size_estimates` row,
  so one evaluate block writes at most about 200 rows. They cost $0.0002 at $1.00 for each million rows.

The local runtime is not the production runtime. These numbers compare the steps with each other, and they do not
predict the latency in production. Section 4.2.14.3 gives the cost of a rollback in production.

##### 4.2.14.3 Replication of the temporary writes

A rolled-back evaluate block can still write WAL frames. SQLite can write the pages of a large transaction to the WAL
before the end of the transaction, and the rollback then discards them. A Durable Object sends its WAL frames to the
durability followers before the output gate opens. Thus the temporary writes can delay the other requests of the
partition, also when they roll back.

The request limits bound this cost:

- One evaluate block makes at most `MAX_ITEMS_PER_TX` (100) temporary writes. Each one writes at most `MAX_ITEM_BYTES`
  (400 KB), so one block writes at most about 40 MB.
- The lock block writes one row for each item: in a repeated request, at most 99 rows of up to 400 KB, about 40 MB.

Thus one prepare event of a repeated request writes at most about 80 MB: the evaluate block, which rolls back, and
the lock block, which commits. A standard prepare writes at most about 40 MB, in its lock block only. An example of
the worst case: one item with `update → update` and 98 single `update` operations on other items, each item of
400 KB, all in one partition. Every item gets temporary writes (section 4.2.3), so evaluate writes about 40 MB. The
lock block then writes 99 lock rows, also about 40 MB.

The design adds no limit for this cost. The upper bound is two times the bound of a standard prepare, and a standard
commit already writes up to 40 MB in one event.

The local runtime has no replication, so the measurements of section 4.2.14.2 do not show this cost.

#### 4.2.15 Deployment

- The change edits the migrations of `tc_items` and `pending_transactions` in place. It adds no migration entry.
- A deployment must destroy the existing Durable Object namespaces of the partitions and the coordinators.
- The standard-mode fingerprint does not change.

#### 4.2.16 Testing

The tests use the current suites in `test/partition-do/`, `test/transactions/`, `test/repartition/`, and
`test/property-based/`.

The **reference run** is the oracle of the sequence tests. It applies the standalone operations on a
`TransactionParticipant` with the same start state:

- Each operation is a one-operation call of `executeSingleShot`. Its `txOrderTimestamp` returns the `T` of the run
  under test.
- A one-operation transaction uses the transactional store writes: `deleteItem` with `bumpTxOrderTsAlways`, and
  `bumpItemReadTs` for a `check`.
- The single-partition path has no timestamp check, so operations with the same `T` do not reject each other.
- Non-transactional requests are not a reference. Each one takes its own `T`. A non-transactional delete of an absent
  item does not raise `max_delete_tx_order_ts`. No non-transactional `check` exists.

The sequence tests:

- **Repeated-item sequences.** `put → update`, `put → check`, `delete → put`, `put → delete`, `put → delete → put`,
  `check → check`, and long sequences, on an existing item and on an absent item. Each test runs on a
  `TransactionParticipant`: `executeSingleShot` with `txOrderTimestamp` pinned to `T`, and `prepareLocal` with
  `commitLocal` at the transaction timestamp `T`. It compares the data, `v`, the TTL, `max_deleted_v`,
  `max_delete_tx_order_ts`, `key_size_estimates`, and the timestamps with the reference run.
- **Both paths, end to end.** Each sequence also runs through `db.ts` on the single-partition path and on the two-phase
  path, from the same start state. Both paths give the same result. Only the values that come from `T` can differ.
- **Order across items.** A request `[put B, delete A]` and a request `[delete A, put B]` on one partition give B the
  `v` of the standalone operations, on both paths. The test chooses keys whose byte order is the reverse of the request
  order.
- **Property-based.** A `fast-check` suite runs random ordered transactions on a `TransactionParticipant` with one
  pinned `T`: on the single-partition path, on the two-phase path, and as the reference run. All three give the same
  items, `deletion_metadata`, and `key_size_estimates`, the stamps included. The generator makes no version reference
  after a write, because the version-reference tests cover that case.

The evaluate tests:

- **First failure.** A failure at each position of a sequence. Check the `passed`, `rejected`, and `not_evaluated`
  results, and the results of the other items.
- **Full rollback.** A failure on one item leaves every item and `deletion_metadata` unchanged, on both paths.
- **Timestamp rule.** `put → check` and `delete → put` pass. A sequence fails with `timestamp_conflict` against a newer
  committed stamp, at the first operation whose rule fails. A transaction with an existing item A (`delete → put`) and
  an absent item B (`put`) in one partition commits, in both orders of A and B in the request.
- **Images.** An `all_old` image of operation *k* shows the temporary state before *k*. A temporary delete gives no
  image.
- **Item size.** An intermediate state above `MAX_ITEM_BYTES` fails the operation that makes it.
- **No repeated item.** A request with no repeated item makes no temporary write and runs one `transactionSync` in
  prepare.
- **Memory.** A rolled-back block leaves no change in memory.

The version tests:

- **Version after prepare.** A delete of another item between prepare and commit gives a new row of the transaction a
  `v` above the new `max_deleted_v`.
- **Version-reference check, client.** In ordered mode, these requests fail with `transact_version_after_write` and
  send no RPC: `put → check` with a condition on `v`, and `put → update` with an update value that reads `v`. The same
  operations after a `delete` or an `update` fail in the same way. A condition on `v` on the first operation of the
  item passes, and so does one after a `check` only. The error names the `opIndex` of the refused operation and of
  the earlier write.
- **Version-reference check, partition.** A `txPrepare` request and a `txExecuteSingleShot` request that skip `db.ts`
  and carry `put X → update X SET $.prevVersion = v` throw `transact_version_after_write`. Neither writes a row, a
  lock row, or `deletion_metadata`.

The lock row and commit tests:

- **Last-write data.** For a sequence that ends in an `update`, the lock row holds the stored JSONB bytes, and commit
  writes the same bytes. For a sequence that ends absent after a write, apply raises `max_deleted_v` as the standalone
  operations do.
- **Duplicate commit key and duplicate `opIndex`.** A `txCommit` request with one key two times fails and changes no
  row. A lock row set with one `opIndex` two times fails apply and changes no row.
- **Item identity.** A unit test of `KeyPairMap` finds two different sort keys with the same `keyPairHash` for the
  seed of the test isolate. On average, a search finds such a pair after about 80 000 random keys. The map keeps the
  two keys as two entries in one bucket. The unit test also covers equal keys, a length difference, a difference in
  the first byte, a difference in the last byte, and the change from the array to the hash at the ninth entry.
- **Identity sites.** A Durable Object can load the module in another isolate with another seed, so these tests do
  not depend on a collision. They send more than 8 keys that differ only in their last byte, so the hash part of
  `KeyPairMap` answers. Each site of section 4.2.6 treats the keys as different items: standard mode accepts them,
  evaluate makes one sequence for each key, prepare writes one lock row for each key, and commit applies each lock row
  to its own item.

The protocol tests:

- **Mode validation.** An `executionMode` other than the two values fails with `transact_execution_mode_invalid`, with
  and without a `clientRequestToken`, and sends no RPC. An absent mode gives the same result and the same fingerprint
  as `"standard"`.
- **Idempotent retries.** A retry with the same token and the same mode gets the stored outcome. A retry with a
  different mode gets the token mismatch error. A standard-mode fingerprint keeps its current value.
- **Repeated prepare of a repeated item.** A second prepare of an accepted ordered transaction answers `accepted`. It
  changes no lock row and no `pending_tx_info` row. The commit after it gives the same result as a commit after one
  prepare.
- **Late prepare.** After the commit of each sequence that has a `put`, an `update`, or a `delete`, a late prepare of
  the same transaction gets `timestamp_conflict`. After the commit of `check → check`, on a present item and on an
  absent item, a late prepare passes. The commit that the stale-transaction job then sends changes no item.
- **Recovery.** A repeated prepare, a repeated commit, the coordinator `tx_recovery` job, and the partition
  stale-transaction job resolve an ordered transaction. A repeated commit applies nothing.
- **Routing.** Equal keys of one `txPrepare` dispatch go to one owner in one sub-request, with the Bloom filter on.
- **Partition migration.** A hash split and a promotion move a lock row with its last-write data and its `op_list`.
  Commit on the target applies the entries.
- **Coordinator migration.** A coordinator split moves the `tc_items` rows with their `op_index`.

Section 4.2.17 gives the tests of milestone 7.

#### 4.2.17 Fatal prepare errors

Milestone 7 ships this section after the ordered-mode work. It applies to every prepare, in both modes.

**The problem.** The coordinator treats every prepare error as transient:

- `prepareRetry` retries every error except `partition_over_size`. It tries up to `prepareMaxAttempts` times in
  `drivePrepare` and up to `prepareRecoveryMaxAttempts` times in `runPrepareRecovery`.
- `runPrepareRecovery` treats a participant whose prepare threw as undecided. The transaction stays in `PREPARING`
  until `maxPreparingHoldMs`. Each pass of the `tx_recovery` job sends the prepare again, and the other participants
  keep their locks for that time.

Some errors cannot clear on a retry, because the same request gets the same error each time. The version-reference
check of the partition (section 4.2.3, step 2) is one of them. A later check that only the partition can make can be
another one.

**The rule.** A `FokosValidationError` from a prepare is fatal. A validation error says that the request is not
valid, so the same request gets it again. The category survives each RPC hop, because `FokosError.is` reads `_tag`.
The other categories stay retryable:

- `FokosExpressionError`, because an expression error can depend on the item data, and the data can change before
  the next try.
- `FokosUnavailableError`, `FokosRoutingError`, and `FokosInternalError`. They include the errors of a split, a
  migration, and the runtime.

In the current code, a prepare can raise these validation errors: `item_too_large` from the size guard of the store,
`partition_context_options_invalid`, and the key-encoding errors of `KeyCodec`. Each one gives the same answer for the
same request.

**The changes.**

1. `prepareRetry` returns `false` from `shouldRetry` when `FokosValidationError.is(err)` is true, as it does now for
   `partition_over_size`. `drivePrepare` and `runPrepareRecovery` both use `prepareRetry`, so one change covers both
   drives.
2. `runPrepareRecovery` cancels when a participant has a fatal error. After its fan-out, it reads each participant
   whose `prepare_outcome` is NULL, and it builds the stored error from `error_json` with `FokosError.fromWire`. When
   `FokosValidationError.is` is true for one of them, the transaction goes to `cancelTransactionInStore` and
   `runCancel`, the same as for `anyRejected`.
3. `drivePrepare` does not change. It already cancels when a participant did not accept.

**Why the cancel is safe.**

- The transaction is in `PREPARING`, so no participant has committed. `markCommitting` needs an accepted answer from
  every participant, so a participant with a fatal error already blocks the commit decision.
- `storePrepareError` writes `error_json` only while the state is `PREPARING` and the participant has no answer.
- A router can lock the keys of one child and then throw the error of another child. `runCancel` sends a cancel to
  every participant without a cancel outcome, a participant with no answer included. Thus the cancel releases those
  locks.

**The result for the caller.** `cancelTransactionInStore` gives each operation of the participant the stored code and
`error_id` (`participantFailure`). The caller gets `FokosTransactionCancelledError`, and the results carry the code,
for example `transact_version_after_write`. The single-partition path does not change. `db.ts` does not retry it, and
it reports the error as a cancelled transaction.

**Tests.**

- A prepare that throws a `FokosValidationError` gets one try in `drivePrepare`, and the transaction cancels. The
  results carry the code and the `error_id`.
- A recovery drive that gets a `FokosValidationError` cancels at once, before `maxPreparingHoldMs`. The cancel
  releases the locks of the participants that accepted.
- A `FokosExpressionError` and a `partition_migrating` error are still retried.

## 5. Alternative options

**The execution model.**

- **Collapse each sequence into one net write with a version delta.** The lock row holds the net operation and the
  number of writes, and commit makes one write for each item. Commit then skips the deletes in the middle of a
  sequence. The `v` of the item and of other new rows, and `max_deleted_v`, then differ from the standalone operations
  and between the two paths. Each difference needs its own rule, and some need new store methods.
- **A `replace` lock operation that deletes and inserts the row at commit.** It records one delete inside a sequence.
  It needs a fifth lock operation and a new store method, and it still skips the other deletes of the sequence.
- **Evaluate the temporary state in JavaScript.** SQLite evaluates every condition and every update now. A second
  evaluator in JavaScript can give a different answer than SQLite.
- **A scratch table for the temporary state.** The compiled condition and update plans read `items`. A scratch table
  needs a second form of each plan and more code.
- **Temporary writes for every request.** This removes the condition "repeated request" from evaluate. It adds writes
  and a rollback to every standard prepare.
- **Temporary writes only for repeated items, in a repeated request.** It saves the temporary writes of the other
  items. A temporary delete of a repeated item then raises `max_deleted_v` before an earlier delete of another item
  runs. Thus a condition on `v` sees a state that apply does not give.
- **No temporary write for the last operation of an item.** It saves one temporary write for each repeated item. The
  last-write data then needs a separate read for each case: a document `SELECT` of the last update, or the request
  data of the last put. Each case needs its own code and test.
- **Roll back the single-partition block and then run apply.** This gives one apply path for both paths. The
  temporary writes of the single-partition path are already the writes of apply in `opIndex` order, so the rollback
  only adds writes.
- **Check the timestamp against the temporary state.** All operations use one timestamp, so valid sequences fail
  (section 4.2.3.1).

**The prepare.**

- **Nested `transactionSync` (savepoints).** The Durable Object documentation does not describe nested transactions.
  Two top-level blocks need no nesting.
- **Undo the temporary writes by hand, so that the lock block can read the data in SQLite.** The block copies the
  committed rows aside, writes the lock rows from the temporary rows, and then restores `items`,
  `key_size_estimates`, and `deletion_metadata`. One missed restore corrupts the committed state.
- **A second pass for each repeated item, to hold one copy at a time.** After the evaluate block, the prepare runs the
  temporary writes of each repeated item again, reads its data, rolls back, and writes its lock row. The peak memory
  goes from about 20 MB to 400 KB, but the temporary writes run two times. The 20 MB worst case is less than the 40 MB
  of condition failure images that standard mode can hold now (section 4.2.14.1).

**The lock row and commit.**

- **Store each operation with its data in the lock rows, and replay the data at commit.** One lock row for each item
  can then exceed the row limit of 2 MB: 10 puts of 400 KB on one item hold 4 MB. One lock row for each operation
  changes the rule of one lock row for each key at every site that reads locks. The last-write data gives the same
  visible result (section 4.2.4).
- **Send the operations with the commit request.** Commit then depends on the payload of the coordinator, which
  `stripPayload` can remove, and each commit retry carries up to 4 MB.
- **Store the absolute final `v` in the lock row.** For a new row, this value can be below the `max_deleted_v` of the
  commit. A read transaction then misses a create and a delete between its phases (section 4.2.8).
- **Write `deletion_metadata` one time at the end of the single-partition block, with a net effect for each item.**
  The write replaces values that the store writes raised with `MAX`, so it can lower `max_deleted_v` and
  `max_delete_tx_order_ts`. A lower watermark lets a late prepare of a committed transaction apply again, and a lower
  `max_deleted_v` lets a `v` repeat.

**Item identity.**

- **Keep `pairKey`, and reject a hash collision at the client in ordered mode.** This keeps the false
  `transact_duplicate_key` of standard mode, and every site still depends on one client check. A caller that sends
  the RPC without `db.ts` reaches the participant with the collision.
- **Other forms of exact identity.** A byte comparison of each pair takes 10 ms in its worst case. A sort takes
  250 µs. A base64 or hex text key allocates a string for each key and costs up to 590 µs (section 4.2.6).
- **A fixed public seed for `keyPairHash`.** A caller can then make keys that all go into one bucket. The map then
  costs the same as the byte comparison of each pair: 10 ms.

**The API and the validation.**

- **A separate method `transactWriteOrderedItems()`.** It is a wrapper of one line, but it doubles the public types,
  the documentation, and the HTTP surface. The `executionMode` option gives the same function.
- **Send the mode to the partition.** The engine gives the same result for both modes. The client is the validation
  boundary, so the partition needs no mode.
- **A precise version-reference rule in evaluate.** Evaluate refuses a version reference only when an earlier
  operation of the same transaction created the current temporary row. This removes the conservative refusals of
  section 4.2.2. It needs a new public rejection code and new result rules. The single-partition path must apply it
  too, although `v` is exact there, or the two paths give different results. The caller learns of the refusal only at
  run time, and the answer depends on whether the row existed.
- **The version-reference check at the client only.** A caller that sends the RPC without `db.ts` can then store a
  `v` in the data that the committed row does not have.
- **A list of fatal codes in `prepareRetry`.** Each new check that only the partition can make then needs a change to
  the coordinator. The category rule of section 4.2.17 needs none.

## 6. Frequently asked questions

**Does ordered mode change a request with no repeated item?**
No. The request makes no temporary write and takes the same steps as standard mode.

**Do standard mode and ordered mode use different code?**
No. Below the client, the code reads the request, not the mode. A standard request is a request with no repeated
item.

**Can an ordered transaction commit in part?**
No. One failure cancels the transaction. The other items continue only to report their results.

**Is there an order across items?**
Yes, inside one partition: each partition applies all of its operations in `opIndex` order. There is no order across
partitions.

**Do the two paths leave the same state?**
Yes, for the same start state. Both paths make the same store writes in `opIndex` order. Only the values that come
from `T` can differ.

**Which image does `all_old` return?**
The state immediately before the failed operation, with the temporary changes of the earlier operations
(section 4.2.11).

**Why is the two-block prepare safe without nested transactions?**
No `await` runs between the blocks, and the first block commits nothing (section 4.2.5).

**Does commit write the data of each operation?**
No. Each write of an item uses its last-write data. The data in the middle of a sequence is invisible, and the visible
values depend only on the order and the types of the operations (section 4.2.4).

**Why can a condition on `v` not follow a write of the same item?**
On the two-phase path, the `v` of a row that the transaction creates can be higher at commit than at prepare. The
condition then decides on a `v` that the committed row does not have (section 4.2.8). A condition on `v` on the first
operation of the item reads the committed `v` and is permitted.

**What happens when a prepare fails a check that the client did not make?**
The partition throws a `FokosValidationError`. After milestone 7, the coordinator does not retry it and cancels the
transaction at once (section 4.2.17).

**Can a lock row exceed the row limit?**
No. A lock row holds one copy of the data, up to 400 KB, and an operation list of about 1.5 KB (section 4.2.7).

**Can a prepare keep the last-write data in SQLite and out of JavaScript?**
Not with a rollback, because the rollback discards every row that the evaluate block wrote. Section 5 gives the options
that avoid the copy or reduce it, and their cost.

**Why does the coordinator not store the mode in a column?**
Nothing below the client reads the mode. The fingerprint is its only use, and `operations_hash` holds it.

**Does the single-partition path change its rule for `clientRequestToken`?**
No. A request with a token uses the two-phase path in both modes.

## 7. References

References:

- `AGENTS.md`
- `docs/agent-plans/2026-10-03-max-deleted-version.md`
- `docs/agent-plans/2026-08-23-single-partition-transaction-fast-path.md`
- `docs/agent-plans/2026-09-02-update-expressions.md`
- `docs/agent-plans/2026-09-05-item-order-timestamps-and-read-revisions.md`
- `docs/agent-plans/2026-09-08-return-values-on-condition-check-failure.md`
- `docs/agent-plans/2026-09-27-learned-routes-for-every-dispatch-shape.md`
- `docs/agent-plans/2026-09-29-promotion-moves-its-locks.md`
- [SQLite-backed Durable Object Storage](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)
- [ATC 2023, Idziorek et al.](https://www.usenix.org/system/files/atc23-idziorek.pdf)
