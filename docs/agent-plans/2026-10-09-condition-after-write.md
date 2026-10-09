# RFC — A condition on the state after the write

**State:** Abandoned - with ordered_per_item transaction execution mode this becomes unnecessary for now.
**Date:** 2026-10-09
**Author:** Lambros

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

A write condition reads the item **before** the write. Some rules apply to the item **after** the write. An
example is an `update` that decrements `$.balance` and must not leave it below zero.

Today the caller must restate the rule for the old value. "The balance after `- 5` is at least 0" becomes
"the balance before is at least 5". This works for a simple update. It is hard to get right for an update
with many actions, for `if_not_exists`, or for an update that creates the item. The rule then lives in two
forms, and the two forms can drift.

### 1.2 What the reader must know about the current system

- A condition is a SQL statement that reads the row of the item from `items`
  (`PartitionStore.evaluateCondition`). It sees the current state of the row in the storage transaction.
  Thus a statement that runs after a write in the same transaction sees the new row.
- `PartitionDO.putItemLocal` and `PartitionDO.deleteItemLocal` run the lock check, the condition and the
  write in one `transactionSync` block. `transactionSync` rolls back when the callback throws. It commits
  when the callback returns, also when the callback returns a rejection.
- `TransactionParticipant.#evaluate` already makes temporary writes when an item has two or more operations
  (`executionMode: "ordered_per_item"`). The single-shot path keeps them as the writes of the transaction.
  The prepare throws `EvaluationRollback`, SQLite rolls them back, and a second block writes the locks.
- On the two-phase path, `COMMITTING` is the point of no return. A check after the commit cannot cancel the
  transaction.
- Between prepare and commit, the lock of the transaction holds the item. A non-transactional write to a
  locked item is refused, and the TTL sweep deletes only unlocked items.
- The `v` of a row that a transaction creates can be higher at commit than at prepare, because a delete of
  another item can raise `max_deleted_v` between the two steps (`validateVersionReferences`).

This RFC builds on `docs/agent-plans/2026-10-09-condition-arrays.md`. It uses the `WriteCondition` type,
the `evaluateConditions` store method, and the `conditions` outcomes of that RFC.

---

## 2. Goals and Requirements

### 2.1 In scope

- `putItem`, `deleteItem`, and the `put`, `delete` and `update` operations of `transactWriteItems` accept
  `conditionAfter`. It is a `WriteCondition`: one expression or an array of expressions.
- `conditionAfter` evaluates against the row after the write of its operation.
- When `conditionAfter` fails, nothing is written. A transaction cancels.
- The error of a failed `conditionAfter` says that the after condition failed, and it gives the outcome of
  each condition of `conditionAfter`.
- With `returnValuesOnConditionCheckFailure: "all_old"`, the image is the row **before** the write, as for a
  failed `condition`.
- Both transaction paths (single-shot and two-phase) give the same outcomes for the same request.

### 2.2 Out of scope

- `conditionAfter` on a `check` operation. A check does not write, so its after state is its before state.
- An image of the row after the write (an `all_new` option).

### 2.3 Requirements

- In `transactWriteItems`, a `conditionAfter` that reads `v` must be refused, on both paths. Section 4.2.5
  gives the reason. `putItem` and `deleteItem` allow it, because there the value is exact.
- The prepare of the two-phase path must decide `conditionAfter`. The commit must not evaluate it again.
- The after state that the prepare evaluates must be the state that the commit writes, for every column
  that a `conditionAfter` can read.
- An expression that is not valid fails in `db.ts`, before any request.

---

## 3. Milestones

1. **Item operations.** The public types, the wire types, `db.ts`, `putItemLocal` and `deleteItemLocal`.
   Tests for `putItem` and `deleteItem`.
2. **Transactions.** `TransactionParticipant.#evaluate`, the validation, the fingerprint, and the
   coordinator column. Tests for both paths, for `ordered_per_item`, and for a replay with the same token.
3. **The example.** The valibot schemas and the serializer of `examples/http-api`.

Milestone 1 of this RFC needs milestone 1 of `2026-10-09-condition-arrays.md`.

---

## 4. Proposed Solution

### 4.1 High-level overview

The partition writes the item, then runs `conditionAfter` against the new row, in the same storage
transaction. When `conditionAfter` fails, the partition throws inside the transaction. SQLite then rolls
the write back, and the caller gets a `condition_failed` reason with `phase: "after"`.

For `putItem` and `deleteItem`, this is the whole mechanism.

A two-phase transaction cannot check after its commit. Thus its prepare makes the write as a temporary
write, runs `conditionAfter`, and rolls the write back. The item lock then holds the item until the commit.
The commit applies the same operation to the same old row, so it writes the same new row. The one
exception is `v`, so a transaction refuses a `conditionAfter` that reads `v`.

```
putItem / deleteItem / single-shot transaction        two-phase prepare
──────────────────────────────────────────────        ─────────────────────────────────────
transactionSync {                                     transactionSync {
  lock check                                            lock check
  condition          (before)                           condition          (before)
  write                                                 timestamp check
  conditionAfter     (after)                            temporary write
  failed → throw → rollback                             conditionAfter     (after)
}                                                       throw EvaluationRollback → rollback
                                                      }
                                                      accepted → transactionSync { write locks }
                                                      ... commit applies the same write
```

### 4.2 Technical details

#### 4.2.1 Public API

```ts
type PutItemOptions = { /* ... */ condition?: WriteCondition; conditionAfter?: WriteCondition };
type DeleteItemOptions = { /* ... */ condition?: WriteCondition; conditionAfter?: WriteCondition };
// TransactWriteItem: put, delete and update take `conditionAfter?: WriteCondition`. check does not.
```

The `condition_failed` reason gets the phase:

```ts
| {
    code: "condition_failed";
    hashKey: HashKey;
    sortKey?: SortKey;
    /** "before" for `condition`, "after" for `conditionAfter`. */
    phase: "before" | "after";
    /** One outcome for each condition of the phase, in the order of the request. */
    conditions: ConditionOutcome[];
    item?: I;
  }
```

When `condition` fails, the write does not run, and `conditionAfter` does not run. The reason then has
`phase: "before"`.

#### 4.2.2 Validation in `db.ts`

1. `db.ts` compiles `conditionAfter` as it compiles `condition` (`2026-10-09-condition-arrays.md`, 4.2.2).
   The same codes apply: `conditions_empty` and `conditions_too_many`. `MAX_CONDITIONS_PER_WRITE` (10)
   applies to `condition` and to `conditionAfter` separately, so one operation runs at most 20 condition
   statements.
2. `validateTransactWriteOperations` changes in three places:
   - A `check` operation with `conditionAfter` fails with the existing `transact_operation_fields_invalid`.
     This is the code for a field that the operation type does not take, as for `data` on a `delete`.
   - The byte count adds the JSON size of each plan of `conditionAfter`.
   - A plan of `conditionAfter` that reads `v` fails with the existing `transact_version_after_write`. The
     code means "a plan reads `v` after a write of its item", and a `conditionAfter` always reads after
     the write of its own operation. The `attributes` carry `opIndex` and the keys, as today. The
     `earlierOpIndex` attribute is the `opIndex` of the operation itself.
3. `sequencePlanOf` in the participant runs the same `v` check, so a request that skips `db.ts` gets the
   same refusal before the first SQL statement.

#### 4.2.3 Wire types

A new field `conditionAfter?: CompiledConditionPlan[]` in:

- `PutItemRpcRequest` and `DeleteItemRpcRequest` (`packages/fokosdb/src/server/do-partition.ts`).
- `TransactionItem` and `TCWriteOperation` (`packages/fokosdb/src/shared/transaction-wire-types.ts`).

#### 4.2.4 `putItem` and `deleteItem`

`putItemLocal` and `deleteItemLocal` change as follows:

1. In the `transactionSync` block, after `upsertItem` or `deleteItem`, run
   `evaluateConditions(req.conditionAfter, hashKey, sortKey)`.
2. When it fails, throw a sentinel error that holds the result. The block must throw, because a returned
   rejection commits the write.
3. Outside the block, catch the sentinel. When `all_old` is set, read the image with `getItemImage`. The
   rollback restored the old row, so the image is the row before the write. No `await` runs between the
   rollback and the read.
4. Build the reason with `conditionFailedReason`, with `phase: "after"` and the outcomes.
5. The `meta` of the error is the sum of the metrics of every statement that ran: the `condition`
   statements, the write, the `conditionAfter` statements, and the image read.
6. `signalGrowth` runs only after a write that committed.

An error that the write raises (for example `item_too_large`) rolls back as today, and `conditionAfter`
does not run.

#### 4.2.5 Transactions

`TransactionParticipant.#evaluate` changes as follows.

**Which operations write in the evaluate step.** A new plan flag `writesInEvaluate` is true when the request
has an item with two or more operations (`plan.repeated`), or when one or more operations have
`conditionAfter`.

- On the single-shot path, when `writesInEvaluate` is true, the evaluate step writes every passed operation.
  The block then keeps these writes as the writes of the transaction, as it does now for `plan.repeated`.
  The flag must cover the whole request. If only the operations with `conditionAfter` wrote, the block
  would commit those writes and skip the other operations.
- On the prepare, the evaluate step writes what it writes now, and also each operation with
  `conditionAfter`. The block then throws `EvaluationRollback`, and the second block writes the locks.
- The read of `max_delete_tx_order_ts` before the first temporary write uses `writesInEvaluate` in place
  of `plan.repeated`. Otherwise a temporary delete raises the value before the read, and a later
  operation gets a false `timestamp_conflict`.

**The order inside one operation.**

1. The lock check (first operation of the item).
2. `condition`.
3. `#precheckWrite`.
4. The timestamp check (prepare only).
5. The temporary write.
6. `conditionAfter`. When it fails, the operation is rejected with `phase: "after"`.
7. `pass(i)`.

`pass(i)` must run after `conditionAfter`, because `pass` adds a `passed` result once the request has a
rejection.

**The item stamps.** A prepare reads the item stamps from the `condition` statement or from
`#precheckWrite`. It never reads them from a `conditionAfter` statement, because that statement reads the
temporary row.

**`ordered_per_item`.** `conditionAfter` of an operation sees the state after that operation, before the
later operations of the same item.

**A rejected operation keeps its temporary write until the block ends.** The rollback then removes it. Both
paths make the same temporary writes, so a later image shows the same `max_deleted_v` on both paths.

**Why the prepare result holds at commit.** The commit applies the same operation to the same old row:

- The lock holds the item from prepare to commit. A non-transactional write is refused, and the TTL sweep
  skips a locked item.
- A put writes the data of the request. A delete removes the row. An update evaluates its plan against the
  old row. The allowed SQLite functions (`packages/fokosdb/src/shared/expression/sqlite-functions.ts`)
  have no time or random input. A function with such an input must not join that list.
- `hk`, `sk`, `data_kind`, `ttl_epoch_utc_seconds` and `data` are thus the same at prepare and at commit.
- `v` is the exception. A row that the transaction creates can get a higher `v` at commit than at prepare.
  Thus a `conditionAfter` that reads `v` is refused (2.3). The rule is conservative: an existing row keeps
  the same `v`, but the client cannot know whether the row exists.

The commit does not change. It applies the lock rows as today.

#### 4.2.6 Transaction coordinator

- A new column `tc_items.conditions_after_json` holds the JSON array of plans. Edit the existing migration in
  place.
- Every place that reads or writes `conditions_json` handles the new column too: the insert of the items,
  the copy of the items on a coordinator split, the statement that clears the payload of a finished
  transaction, the size estimate of a row, the cancel path of `loadItems` that selects `NULL AS
  conditions_json`, and the conversion from a row to a `TransactionItem`.
- `hashOperation` adds a presence flag for `conditionAfter` to its token, and then hashes the number of plans
  and the `identity` of each plan, in order.
- The `phase` field goes into `results_json` with no other change. A replay with the same token returns the
  same reason.

#### 4.2.7 Cost

Durable Object SQLite bills the rows that a statement writes, also when the storage transaction rolls back.
The table counts them. N is the number of conditions in `conditionAfter`, at most 10.

| Case | Extra rows read | Extra billed rows written |
| --- | --- | --- |
| `putItem` / `deleteItem`, after condition passes | N | 0 |
| `putItem` / `deleteItem`, after condition fails | N | the rows of the write, which rolls back |
| Single-shot transaction, passes | N for each operation | 0, because the evaluate writes are the writes |
| Single-shot transaction, fails | N for each operation | the rows of every evaluate write, which roll back |
| Two-phase prepare, accepted or rejected | N for each operation | the rows of each temporary write |

The two-phase path is the most expensive case. Each operation with `conditionAfter` pays for its write two
times: one time at prepare, which rolls back, and one time at commit. A caller that does not need
`conditionAfter` pays nothing extra.

#### 4.2.8 Testing

- `test/partition-do/item-conditions.test.ts`:
  - `putItem` with a `conditionAfter` that passes, and one that fails. After the failure, the stored item
    has the old data and the old `v`.
  - `deleteItem` with `conditionAfter` `not_exists`.
  - The `all_old` image after an after failure is the row before the write.
  - A failed `condition` gives `phase: "before"` and runs no `conditionAfter`.
  - `conditionAfter` that reads `v` works in `putItem`.
- `test/transactions/tx-update-expressions.test.ts`: an update that decrements a balance below zero fails
  `conditionAfter`, and the item does not change.
- `test/transactions/tx-error-parity.test.ts`: both paths give the same results for the same request,
  including a request where only one operation has `conditionAfter`.
- `test/transactions/tx-ordered.test.ts`: `conditionAfter` of a middle operation sees the state after that
  operation and before the later operations of the same item.
- `test/transactions/tx-paths.test.ts`: a two-phase transaction whose prepare passes `conditionAfter`
  commits the same row that the prepare evaluated.
- Validation: `conditionAfter` on `check`, and a `conditionAfter` that reads `v` in `transactWriteItems`.
- `packages/fokosdb/src/shared/transaction-idempotency.test.ts`: a replay that adds or changes
  `conditionAfter` gets a different fingerprint.
- Property-based coverage, in milestone 2. Either the model of
  `test/property-based/transactions-ordered.test.ts` adds `conditionAfter`, or a new suite
  `test/property-based/transactions-condition-after.test.ts` covers it. Choose the new suite when the
  change to the existing model is large. The model computes the expected after state of each operation, and
  the suite checks that the actual outcome, the committed items, and the outcomes of both paths match it.

---

## 5. Alternative Options

### 5.1 Evaluate `conditionAfter` at commit

The commit runs `conditionAfter` and cancels on a failure. Not chosen: `COMMITTING` is the point of no
return, and a commit must not cancel.

### 5.2 Only one expression for `conditionAfter`

Not chosen: `condition` accepts an array (`2026-10-09-condition-arrays.md`). The same type for both fields
lets them share the compile code, the evaluation, the limits and the reason shape.

### 5.3 Compile the expressions in the partition

Not chosen, for the reasons in `2026-10-09-condition-arrays.md`, section 5.2.

---

## 6. Frequently Asked Questions

**Why can a `putItem` read `v` in `conditionAfter`, and a transaction cannot?** `putItem` evaluates and
commits in one storage transaction, so the `v` it reads is the `v` it stores. A two-phase transaction
evaluates at prepare and writes at commit, and `v` of a created row can change between the two steps. The
single-shot path is exact too, but the client cannot choose the path, and both paths must give the same
answer.

**What does `conditionAfter` see after a delete?** An absent row. `exists` is false, and every reference to
the data is missing.

**Does `all_old` show the new row?** No. It shows the row before the write. The new row was never
committed.

---

## 7. References

- `docs/agent-plans/2026-10-09-condition-arrays.md`
- `docs/agent-plans/2026-09-08-return-values-on-condition-check-failure.md`
- `docs/agent-plans/2026-10-03-ordered-per-item-transact-write.md`
- `docs/agent-plans/2026-10-03-max-deleted-version.md`
- `docs/agent-plans/2026-09-02-update-expressions.md`
- `packages/fokosdb/src/shared/partition/transaction-participant.ts`
- `packages/fokosdb/src/server/do-partition.ts`
