# RFC — A write condition as an array of condition expressions

**State:** Draft
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

A write takes one condition expression. Two limits follow from this:

1. **The statement limits apply to all checks together.** One condition compiles to one SQL statement. The
   statement must stay below `EXPRESSION_LIMITS.completeStatementBindings` (100 bindings) and
   `EXPRESSION_LIMITS.compiledSqlBytes` (100,000 bytes). When a caller joins many checks with `and`, the
   combined expression reaches these limits, but each check alone is small.
2. **A failure has no detail.** The `condition_failed` reason says that the condition failed. It does not
   say which part failed. A caller with five checks in one `and` cannot tell which check stopped the write.

### 1.2 What the reader must know about the current system

- `FokosDB` in `packages/fokosdb/src/client/db.ts` compiles each condition with
  `compileConditionExpression` into a `CompiledConditionPlan`. It does this before it sends a request, so an
  expression that is not valid fails with no I/O.
- `PartitionStore.evaluateCondition` runs one plan as one statement (`composeConditionStatement`). The
  statement reads the row of the item from `items`, and returns `item_present`, `condition_ok`,
  `last_read_ts` and `last_write_ts`.
- `PartitionDO.putItemLocal` and `PartitionDO.deleteItemLocal` run the lock check, the condition and the
  write in one `transactionSync` block.
- `TransactionParticipant.#evaluate` runs the condition of each transaction operation, on the single-shot
  path and on the two-phase prepare. A prepare reads the item stamps from the condition statement when a
  condition ran.
- The transaction coordinator stores the plan of each operation in `tc_items.conditions_json`, and it
  hashes `plan.identity` into the idempotency fingerprint (`hashOperation`).
- A condition plan that reads `v` after a write of the same item is refused (`validateVersionReferences`).

---

## 2. Goals and Requirements

### 2.1 In scope

- `putItem`, `deleteItem`, and every `transactWriteItems` operation that takes a condition (`put`,
  `delete`, `check`, `update`) accept an array of condition expressions.
- Each condition expression of the array compiles to its own plan and runs as its own SQL statement. The
  statement limits apply to each condition alone.
- The partition evaluates every condition of the array, also after a failure. The write applies only when
  all conditions pass.
- When a condition fails, the `condition_failed` reason holds one outcome for each condition, in array
  order.
- Both transaction paths (single-shot and two-phase) give the same outcomes for the same request.

### 2.2 Out of scope

- The `filter` of `queryItems`. It is a `ConditionExpression`, but it is not a write condition.
- A combined statement for all conditions. Section 5.1 gives the reason.
- Moving compilation into the partition. Section 5.2 gives the reason.

### 2.3 Requirements

- An array must hold at least one condition. An empty array fails validation in `db.ts`.
- An array must hold at most `MAX_CONDITIONS_PER_WRITE` conditions. The value is 10. Each condition reads
  the row again, so the limit bounds the rows read of one operation.
- A condition that reads `v` after a write of the same item is refused, as today. The rule applies to each
  condition of the array.
- An expression that is not valid fails in `db.ts`, before any request, as today.

---

## 3. Milestones

1. **Item operations.** The public types, the wire types, `db.ts`, and `putItemLocal` / `deleteItemLocal`
   take an array. The `condition_failed` reason carries the outcomes. Tests for `putItem` and `deleteItem`.
2. **Transactions.** `TransactionItem`, `TCWriteOperation`, `TransactionParticipant.#evaluate`,
   `validateTransactWriteOperations`, `validateVersionReferences`, `hashOperation`, and the coordinator
   column. Tests for both paths and for a replay with the same token.
3. **The example.** The valibot schemas and the serializer of `examples/http-api`.

Each milestone builds and passes its tests alone. Milestone 2 needs milestone 1, because both use the same
reason shape.

---

## 4. Proposed Solution

### 4.1 High-level overview

The caller gives a list of conditions. The client compiles each condition into its own plan. The partition
runs the plans one after the other, in one storage transaction, and keeps the outcome of each. When all
plans pass, the write applies. When one or more plans fail, nothing is written, and the error lists the
outcome of each condition.

```
caller: condition: [c0, c1, c2]
  │
  ▼ db.ts: compileConditionExpression(c0), (c1), (c2)    ← each plan has its own limits
  │
  ▼ partition, one transactionSync block:
      lock check
      evaluate c0 → passed
      evaluate c1 → failed
      evaluate c2 → passed        ← evaluated also after the failure of c1
      one or more failed → no write
  │
  ▼ FokosConditionCheckError
      reason: { code: "condition_failed", conditions: ["passed", "failed", "passed"], item?: <old image> }
```

### 4.2 Technical details

#### 4.2.1 Public API

`condition` accepts one expression or an array of expressions. `db.ts` turns one expression into an array of
one condition, so every layer below `db.ts` sees an array.

```ts
type WriteCondition = ConditionExpression | readonly ConditionExpression[];

type PutItemOptions = { /* ... */ condition?: WriteCondition };
type DeleteItemOptions = { /* ... */ condition?: WriteCondition };
// TransactWriteItem: put, delete and update take `condition?: WriteCondition`.
// check takes `condition: WriteCondition`.
```

The `condition_failed` reason gets the outcome of each condition:

```ts
type ConditionOutcome = "passed" | "failed";

| {
    code: "condition_failed";
    hashKey: HashKey;
    sortKey?: SortKey;
    /** One outcome for each condition, in the order of the request. */
    conditions: ConditionOutcome[];
    item?: I;
  }
```

A caller that sent one expression gets an array of one outcome.

#### 4.2.2 Validation in `db.ts`

1. A new constant `MAX_CONDITIONS_PER_WRITE = 10` goes in `packages/fokosdb/src/shared/transaction-limits.ts`,
   with the other request limits. When `condition` is an array, `db.ts` checks its length and raises a
   `FokosValidationError` with one of two new codes. No existing code matches, because the existing
   `transact_items_*` codes are for the operations of a transaction, and these codes also apply to
   `putItem` and `deleteItem`. The two codes follow the pattern of `transact_items_empty` and
   `transact_items_too_many`:
   - `conditions_empty`: the array holds no condition.
   - `conditions_too_many`: the array holds more than `MAX_CONDITIONS_PER_WRITE` conditions. The error
     carries the length and the limit in `attributes`.
2. `db.ts` compiles each condition with `withExpressionErrors(() => compileConditionExpression(c))`. The
   `FokosExpressionError` of a condition that is not valid carries the index of that condition in
   `attributes.conditionIndex`.
3. `validateTransactWriteOperations` changes in three places:
   - A `check` operation with no `condition` fails with the existing `transact_operation_fields_invalid`, as
     today. A `check` with an empty array fails with `conditions_empty`.
   - The byte count adds the JSON size of each plan.
   - `validateVersionReferences` refuses the operation with the existing `transact_version_after_write` when
     any plan of the array reads `v` after a write of the same item. `readsVersion` examines each plan.

#### 4.2.3 Wire types

The `condition?: CompiledConditionPlan` field becomes `condition?: CompiledConditionPlan[]` in:

- `PutItemRpcRequest` and `DeleteItemRpcRequest` (`packages/fokosdb/src/server/do-partition.ts`).
- `TransactionItem` and `TCWriteOperation` (`packages/fokosdb/src/shared/transaction-wire-types.ts`).

`CompiledConditionPlan`, `composeConditionStatement`, `validateConditionPlan` and `evaluateConditionPlan`
do not change.

#### 4.2.4 Evaluation in the partition

A new store method runs the array:

```ts
evaluateConditions(plans: CompiledConditionPlan[], hk: KeyBytes, sk: KeyBytes): ConditionsEvaluationResult;
// { conditionOk: boolean; outcomes: ConditionOutcome[]; itemPresent; lastReadTs; lastWriteTs; rowsRead; rowsWritten }
```

1. The method runs `evaluateCondition` for each plan, in array order.
2. It runs every plan, also after a failure.
3. `conditionOk` is true when every outcome is `passed`.
4. `itemPresent`, `lastReadTs` and `lastWriteTs` come from the first statement. All statements run in one
   `transactionSync` block with no `await` between them, so all statements read the same row.
5. `rowsRead` and `rowsWritten` are the sums over all statements.

The three callers use the new method in place of `evaluateCondition`:

- `PartitionDO.putItemLocal` and `PartitionDO.deleteItemLocal`. On a failure, the reason gets `outcomes`.
  The image read (`getItemImage`) runs one time, when `all_old` is set and the item is present.
- `TransactionParticipant.#evaluate`. On a failure, `conditionFailedReason` gets `outcomes`. The prepare
  reads the item stamps from the result, as it does now from the one statement.

`conditionFailedReason` in `packages/fokosdb/src/shared/transaction-limits.ts` takes the outcomes, so every
path builds the same reason.

#### 4.2.5 Transaction coordinator

- `tc_items.conditions_json` holds the JSON array of plans. Edit the existing migration in place. The column
  name already fits.
- `loadItems` parses the array into `TransactionItem.condition`.
- The coordinator stores the cancel results as JSON (`results_json`). The `conditions` field of a reason
  goes into that JSON with no other change. A replay with the same token returns the same outcomes.
- The image rows (`tc_results`) stay keyed on `condition_failed`, with no change.
- `hashOperation` hashes the number of plans and then the `identity` of each plan, in order. Thus a replay
  that changes, adds, removes or reorders a condition gets a different fingerprint.

#### 4.2.6 Cost

| Path | Statements for one operation | Rows read |
| --- | --- | --- |
| One condition (today) | 1 | 1 |
| Array of N conditions | N | N |

Each statement reads the row of one item through the primary key. The cost grows linearly with N.
`MAX_CONDITIONS_PER_WRITE` bounds it at 10 rows read for each operation.

#### 4.2.7 Testing

- `test/partition-do/item-conditions.test.ts`: an array where all conditions pass; an array where one
  condition fails and the later conditions still run; the outcomes are in array order; the item image is
  present one time with `all_old`; an empty array fails with `conditions_empty`; an array of 11
  conditions fails with `conditions_too_many`, and an array of 10 passes; a condition that is not valid
  reports its `conditionIndex`.
- `test/transactions/tx-error-parity.test.ts`: the single-shot path and the two-phase path give the same
  outcomes for the same request.
- `test/transactions/tx-ordered.test.ts`: a condition that reads `v` after a write of the same item is
  refused when it is the second item of an array.
- `test/transactions/tx-return-values.test.ts`: a replay with the same token returns the same outcomes.
- `packages/fokosdb/src/shared/transaction-idempotency.test.ts`: a reordered array gets a different fingerprint.
- A test with an array whose conditions each stay inside the statement limits, but whose `and` would not.

---

## 5. Alternative Options

### 5.1 One statement with one column for each condition

The plans compile into one statement that returns `condition_ok_0` to `condition_ok_n`. It reads the row
one time, so it costs 1 row read in place of N.

Not chosen: the one statement keeps the limits of 100 bindings and 100,000 bytes for all conditions
together. Removing that limit is the main goal of this change (1.1).

### 5.2 Compile the expressions in the partition

The caller sends the expression trees, and the partition compiles them.

Not chosen, for four reasons:

1. An expression that is not valid fails only after I/O. In `transactWriteItems`, it makes a coordinator
   row, a prepare fan-out, and a cancel, and the caller gets `FokosTransactionCancelledError` in place of a
   validation error.
2. A retry, a recovery drive, and a forward after a split each compile the same expression again.
3. The coordinator still needs the identity of each expression for the fingerprint, and the version check
   still needs the columns of each plan.
4. The changes this RFC needs are in the types, the wire fields, the coordinator column and the
   fingerprint. They are the same for a tree and for a plan.

### 5.3 Stop at the first failure

The partition stops at the first failed condition, and reports `not_evaluated` for the later ones. This
saves rows read on a failure. Not chosen: the caller wants the outcome of each condition.

---

## 6. Frequently Asked Questions

**Is an array the same as `and`?** For the decision, yes: the write applies only when all conditions pass.
An array adds an outcome for each condition, and it gives each condition its own statement limits.

**Do all statements see the same row?** Yes. They run in one `transactionSync` block with no `await`
between them, and no condition writes.

**Why does the first statement give the item stamps?** All statements read the same row, so the stamps are
the same in each.

---

## 7. References

- `docs/agent-plans/2026-09-08-return-values-on-condition-check-failure.md`
- `docs/agent-plans/2026-08-29-typed-expression-engine-spec.md`
- `docs/agent-plans/2026-10-03-ordered-per-item-transact-write.md`
- `packages/fokosdb/src/shared/expression/plan.ts`
- `packages/fokosdb/src/shared/partition/transaction-participant.ts`
