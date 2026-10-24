# RFC — Conditions and condition images in `batchWriteItems`

**State:** Implemented
**Date:** 2026-10-24
**Author:** Lambros Petrou
**Status:** All milestones are built. Section 1 and the word "today" describe the code before this spec.

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
    - [4.2.1 Public API](#421-public-api)
    - [4.2.2 Validation and the size cap](#422-validation-and-the-size-cap)
    - [4.2.3 The handler](#423-the-handler)
    - [4.2.4 The outcome of a failed condition](#424-the-outcome-of-a-failed-condition)
    - [4.2.5 A resend after `unknown`](#425-a-resend-after-unknown)
    - [4.2.6 Milestone 2: the image of a failed condition](#426-milestone-2-the-image-of-a-failed-condition)
    - [4.2.7 Cost](#427-cost)
    - [4.2.8 Deployment](#428-deployment)
    - [4.2.9 Testing](#429-testing)
- [5. Alternative Options](#5-alternative-options)
- [6. Frequently Asked Questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and Context

### 1.1 The current system

- `putItem` and `deleteItem` take an optional `condition?: ConditionExpression` and an optional
  `returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure` (`"none"` or `"all_old"`), in
  `PutItemOptions` and `DeleteItemOptions` (`src/shared/types.ts`).
- `putItemLocal` and `deleteItemLocal` (`src/server/do-partition.ts`) run these steps in one `transactionSync`:
  1. `assertNotLocked`.
  2. `PartitionStore.evaluateCondition`, when the request has a condition.
  3. When the condition fails and the request asks for `all_old`, `PartitionStore.getItemImage` reads the old item.
  4. When the condition passes, the upsert or the delete.
- A failed condition of `putItem` or `deleteItem` raises `FokosConditionCheckError` with the code
  `condition_failed` (`CONDITION_CHECK_CODES` in `src/shared/errors-operations.ts`). Its `ConditionFailedReason`
  carries the old image.
- `batchWriteItems` puts or deletes 1 to 100 items (`MAX_ITEMS_PER_BATCH`). `BatchWriteItem`
  (`src/shared/batch-api-types.ts`) has no condition. `validateBatchWriteItems` (`src/shared/transaction-limits.ts`)
  refuses an item with `condition`, `update`, or `returnValuesOnConditionCheckFailure`
  (`batch_operation_fields_invalid`). The HTTP example schema `BatchWriteItemsBodySchema`
  (`examples/http-api/src/rpc/schemas.ts`) also refuses them.
- The batch is not atomic. `batchWriteLocal` runs each item in its own `transactionSync`, with `assertNotLocked` and
  then the upsert or the delete. Each item gets one of the outcomes `ok`, `rejected`, `unprocessed`, or `unknown`
  (`BatchWriteItemResult`). Only a request that is not valid throws.
- `classifyBatchFailure` (`src/shared/batch-failure.ts`) gives a validation error or an expression error `rejected`
  (rule 2). It gives an error of the local part `unprocessed` (rule 3), and a write with an unknown result `unknown`.
- `FokosStd.batchWriteItemsWithRetry` (`src/client/fokos-std.ts`) sends `unprocessed` and `unknown` items again, and
  never sends a `rejected` item again. When an item got `unknown` and a later try is not `ok`, it keeps `unknown`.
- `validateBatchWriteItems` refuses two items with the same key (`batch_duplicate_key`).
- When the partition is above 1.1 times its cap, `batchWriteLocal` refuses each put of the batch. A delete still runs,
  because a delete is how a client brings a partition back under its cap.
- There is no public `updateItem`. An update is the `update` operation of `transactWriteItems`.

### 1.2 The problem

The single-item writes and the batch write do not have the same abilities. `putItem` and `deleteItem` accept a
condition and can return the old image of a failed condition. `batchWriteItems` accepts neither.

An idempotent sink needs both. A sink receives writes that a writer retries or delivers out of order, and it must
apply a write only when it is newer than the stored state (`docs/ideas/2026-10-21-idempotent-sink-writes.md`). Today
a caller can store a version in a JSON attribute and check it with a condition on `putItem`. With `batchWriteItems`,
the caller cannot, so a sink must write one item for each call.

### 1.3 Facts that constrain the design

- **A failed condition in a batch must not be a thrown error.** `classifyBatchFailure` gives a thrown error of the
  local part the outcome `unprocessed` (rule 3). A `FokosConditionCheckError` is not a validation error. Thus the
  handler must give a condition failure its outcome directly.
- **`conditionPlanOf` compiles on each call** (`src/shared/expression/request-plans.ts`). `projectionPlanOf` and
  `updatePlanOf` keep the plan of a tree object in a `WeakMap`.
- **workerd keeps prepared statements.** A condition binds every caller value, so two conditions with the same shape
  give the same SQL text. The second `sql.exec` of the same text is 4 to 44 times faster than the first one
  (`docs/agent-plans/2026-10-10-expression-compile-cost.md`, Section 6).
- **`transactWriteItems` already caps the images of failed conditions.** `applyImageCap`
  (`src/shared/transaction-limits.ts`) keeps images in `opIndex` order up to `MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX`
  (10 MiB). It marks each image that does not fit with `itemOmitted: "response_too_large"`. Each node of the answer
  applies the cap.
- **One Workers RPC carries at most 32 MiB** (`WORKERS_RPC_MAX_BYTES` in `src/sharding/rpc-limits.ts`). A root RPC of
  `batchWriteItems` above that limit gives each item of that root `rejected` with `batch_request_too_large`.
- **One item is at most 400 KB** (`MAX_ITEM_BYTES`). Thus 100 images can hold about 40 MB, above the RPC limit.
- **`batchWriteItems` has no payload cap in the client.** `transactWriteItems` adds the JSON size of each condition to
  `MAX_PAYLOAD_BYTES_PER_TX` (4 MB) in `validateTransactWriteOperations`.

### 1.4 Glossary

- **Image** — the old item that a failed condition returns when the item asks for `all_old`.
- **Sink** — a FokosDB table that receives writes from a writer that retries or reorders them.

## 2. Goals and Requirements

### 2.1 In scope

1. `batchWriteItems` accepts an optional `condition` on each `put` and `delete` item (Milestone 1).
2. `batchWriteItems` accepts `returnValuesOnConditionCheckFailure` on each `put` and `delete` item, as `putItem` and
   `deleteItem` do (Milestone 2).

### 2.2 Requirements

- A condition must run in the same `transactionSync` as the write of its item.
- A batch item with a condition must behave as `putItem` or `deleteItem` with the same condition, on the same state.
- A failed condition must change only the result of its own item. The other items of the batch continue.
- A tree that is not valid must fail the whole call before any I/O.
- An item without a condition must cost no extra row read and no extra row write.
- The answer of a batch must stay below the RPC limit when many items return an image.

### 2.3 Out of scope

- Update expressions in `batchWriteItems`. FokosDB has no public `updateItem`, so a single-item write has no update
  either. `transactWriteItems` covers updates.
- The external version and the versioned tombstone delete. `docs/ideas/2026-10-21-idempotent-sink-writes.md` has
  them.

## 3. Milestones

1. **Conditions in `batchWriteItems`** (Sections 4.2.1 to 4.2.5). The public type, the validation, the wire type,
   the handler, the outcome, and the size cap of the conditions. The HTTP example schema `BatchWriteItemsBodySchema`
   accepts a condition, and its test that expects 400 for a batch condition changes. The batch model of
   `test/property-based/harness.ts` gets conditions.
2. **The image of a failed condition** (Section 4.2.6). `returnValuesOnConditionCheckFailure` on each batch item, and
   the image cap. It needs Milestone 1.

## 4. Proposed Solution

### 4.1 High-level overview

Each `put` or `delete` of a batch can carry a condition. The partition evaluates the condition in the
`transactionSync` of its item, as `putItem` does, so the check and the write are atomic. A failed condition gives
that item `rejected` with the code `condition_failed`, and the other items of the batch continue. When the item asks
for `all_old` and the item exists, the result carries the old image. The images of one answer share a cap of
10 MiB, and an image that does not fit is replaced by the marker `itemOmitted: "response_too_large"`.

```text
 batchWriteItems                          PartitionDO, one transactionSync per item
 ───────────────                          ─────────────────────────────────────────
 0: put    a  cond: v = 3   ────────────▶ stored v 3   -> condition passes -> put    -> ok
 1: put    b  cond: v = 3   ────────────▶ stored v 4   -> condition fails  -> no write -> rejected condition_failed
                 all_old                                                               reason.item = image of b
 2: delete c  cond: exists  ────────────▶ c exists     -> condition passes -> delete -> ok
```

### 4.2 Technical details

#### 4.2.1 Public API

- The `put` and `delete` variants of `BatchWriteItem` get `condition?: ConditionExpression`, the type of
  `PutItemOptions.condition`.
- In Milestone 2, the same variants get
  `returnValuesOnConditionCheckFailure?: ReturnValuesOnConditionCheckFailure`.
- `BatchWriteRpcItem` carries the condition tree and, in Milestone 2, `returnValuesOnConditionCheckFailure`.
- `BatchWriteItemsBodySchema` of the HTTP example accepts the same fields.

#### 4.2.2 Validation and the size cap

- `validateBatchWriteItems` accepts `condition`. It still refuses `update` with `batch_operation_fields_invalid`.
  Until Milestone 2, it also refuses `returnValuesOnConditionCheckFailure`.
- In Milestone 2, `validateBatchWriteItems` checks `returnValuesOnConditionCheckFailure` with
  `validateReturnValuesOnConditionCheckFailure`, as `putItem` does.
- `db.ts` validates each condition tree with `expression/semantic.ts` before any I/O, as for `putItem`. A tree that is
  not valid fails the whole call before the first RPC, as every other validation error of `batchWriteItems` does.
- `validateBatchWriteItems` adds the JSON size of each condition, as `validateTransactWriteOperations` does. When the
  sum is above `MAX_PAYLOAD_BYTES_PER_TX` (4 MB), the call fails before any I/O with the new validation code
  `batch_conditions_too_large` (`VALIDATION_CODES`).
- The data of the items does not count toward this sum, because `batchWriteItems` has no payload cap for data. The
  conditions and the data together still count toward the RPC limit, and an oversize root gives
  `batch_request_too_large`.

#### 4.2.3 The handler

In `batchWriteLocal`, the block of each item runs these steps in its `transactionSync`:

1. `assertNotLocked`.
2. When the item has a condition, `PartitionStore.evaluateCondition`. When the condition fails, the block writes
   nothing and returns a condition failure.
3. The upsert or the delete.

The handler gives a condition failure the outcome `rejected` with the code `condition_failed` and a new `error_id`. It
does not throw (Section 1.3). The next item runs. `putItemLocal`, `deleteItemLocal`, and `batchWriteLocal` share one
function for the steps 1 to 3. In Milestone 2, the function also reads the image (Section 4.2.6).

These cases need no new rule:

- When the condition statement raises a `FokosExpressionError` (for example `sql_limit`), the block rolls back.
  `classifyBatchFailure` rule 2 gives the item `rejected` with the code of the error.
- When the partition is above 1.1 times its cap, the handler refuses each put before its condition runs. A delete
  with a condition still runs.
- `batch_duplicate_key` keeps each key of a batch unique. Thus a condition never reads the write of another item of
  the same batch.
- A condition reads the same row as the condition of `putItem`. An item that has expired and that the sweep has not
  deleted yet is present for both.

`conditionPlanOf` compiles each tree on each call, and the batch keeps that. A sink sends one condition shape with a
different value for each item, so each item gives the same SQL text, and workerd reuses the prepared statement
(Section 1.3). Section 4.2.7 has the measured cost, and Section 5.2 has the reason for no plan cache.

#### 4.2.4 The outcome of a failed condition

A condition failure gets `rejected` with the code `condition_failed`. The definition of `rejected` in
`BatchWriteItemResult` changes to: "nothing of the operation applied, and the same operation against the same state
of the item fails the same way again".

A caller tells a condition failure from the other rejections by `code`. `FokosStd.batchWriteItemsWithRetry` does not
send a `rejected` item again. Section 5.1 has the reason for no separate outcome.

#### 4.2.5 A resend after `unknown`

A condition can fail because the first try of the same item applied:

1. The caller sends a put of `k` with the condition `not_exists(hashKey)`. No item `k` exists.
2. The partition applies the put. The answer does not get back to the caller.
3. The item gets `unknown`, and `unprocessedItems` holds it.
4. The caller sends the item again. The condition reads the item of step 2 and fails.
5. The item gets `rejected` with `condition_failed`.

Thus `condition_failed` after `unknown` does not mean that another writer changed the item. The rules:

- `FokosStd.batchWriteItemsWithRetry` keeps the earlier `unknown` when a later result is not `ok`. Thus the helper
  reports `unknown` for this case, and needs no change.
- A caller that sends `unprocessedItems` again by itself must apply the same rule.
- For a sink condition such as "the stored version is lower than X", either answer means that the stored version is
  at least X.

#### 4.2.6 Milestone 2: the image of a failed condition

**Result.** The `rejected` entry of `BatchWriteItemResult` gets two optional fields:

- `reason?: ConditionFailedReason`. It is present when `code` is `condition_failed`. `reason.item` is the image when
  the item asked for `all_old` and the item exists. It is the same record that `putItem` and `deleteItem` carry in
  `FokosConditionCheckError`.
- `itemOmitted?: "response_too_large"`. It is present when the image did not fit in the cap below. Then `reason` has
  no `item`.

A result with no `item` and no `itemOmitted` means that the item does not exist.

**Handler.** When the condition fails, the item asks for `all_old`, and the item exists, the block reads the image
with `PartitionStore.getItemImage` in the same `transactionSync`, as `putItemLocal` does.

**The image cap.** 100 images can be above the RPC limit (Section 1.3). Thus the batch applies the cap of
`transactWriteItems` at each level of the answer:

1. `batchWriteLocal` applies the cap of `MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX` (10 MiB) to its own answer, in
   `opIndex` order.
2. `mergeBatch` applies the same cap to the joined answer of a router.
3. `db.ts` applies the same cap to the joined answers of all roots.

Each level uses the image bytes that the partition measured, and skips a result that already has `itemOmitted`, as
`applyImageCap` does. The batch uses `applyImageCap`, or one function that both callers share. A dropped image does
not change the outcome or the code of the item. The caller can read the item with `getItem`.

#### 4.2.7 Cost

| Milestone | Extra rows read | Extra rows written |
| --- | --- | --- |
| 1: a condition | the rows that the condition statement reads: 1 when the item exists, 0 when it is absent | 0 |
| 2: the image of a failed condition | 1, only when the condition fails, the item asks for `all_old`, and the item exists | 0 |

**Compile and evaluation.** A benchmark measured 100 conditions in one `transactionSync` of a `PartitionDO`, with
local workerd. Each measurement includes the compile and the `sql.exec` of each condition, and reads 100 different
items. The value is the median of 7 batches. The timers of workerd advance in whole milliseconds.

| 100 conditions | Tree bytes of one condition | Batch |
| --- | ---: | ---: |
| "optimistic lock", one shape | about 120 | 2 to 4 ms |
| an `or` of 49 `eq` terms, one shape | 3,332 | 12 ms |
| an `or` of 49 `eq` terms, a different SQL text for each condition | 3,332 | 12 ms |
| the same, with each path 700 bytes longer | 37,632 | 10 ms |
| an `or` of 49 `eq` terms, with each path 4,000 bytes longer | 199,577 | 80 to 90 ms |

The results:

- 49 terms is the limit. Each `eq` term binds its path and its value, and a condition statement can bind 98 values
  of the caller (`completeStatementBindings` is 100, and the keys use 2). Thus the compiled SQL stays near 10 KB, far
  below `compiledSqlBytes`.
- A different SQL text for each condition costs no more than one text. The prepare of a condition statement is not
  the cost. The cost is the evaluation of the JSON paths, and it increases with the path bytes.
- The last row is not possible in one batch: its 100 conditions hold 20 MB, above the size cap of Section 4.2.2. The
  cap accepts about 20 such conditions, and those cost about 17 ms (one fifth of the last row, not measured).
- Thus the size cap limits the worst case of one batch to about 20 ms of synchronous work. `transactWriteItems`
  already accepts the same 4 MB of conditions.

#### 4.2.8 Deployment

- No schema change.
- Deploy the Worker that defines `PartitionDO` before a client that sends the new fields.

#### 4.2.9 Testing

- **Milestone 1.**
  - `test/partition-do/batch-write-items.test.ts`: a passing and a failing condition on a put and a delete, a failed
    item next to applied items, a condition on an absent item, and a locked item with a condition.
  - A condition that raises `sql_limit` gives `rejected` for its item only.
  - A delete with a condition applies above 1.1 times the cap.
  - The sum of the condition sizes above 4 MB fails with `batch_conditions_too_large` before any I/O.
  - The HTTP example test "rejects a batchWriteItems operation with a condition as 400"
    (`examples/http-api/test/index.test.ts`) changes to accept the condition.
  - `src/client/fokos-std.test.ts`: an item that gets `unknown`, then `rejected` with `condition_failed` on the
    resend, keeps `unknown` (Section 4.2.5).
  - The batch model of `test/property-based/harness.ts` gets conditions. A batch item with a condition gives the same
    result as `putItem` or `deleteItem` with the same condition on the same model state.
- **Milestone 2.**
  - A failed condition with `all_old` returns the image, and with `none` returns no image.
  - A failed condition on an absent item returns no image and no `itemOmitted`.
  - Images above 10 MiB in one partition answer, in one router answer, and across roots get `itemOmitted` after the
    cap, in `opIndex` order.

## 5. Alternative Options

### 5.1 A separate outcome for a failed condition

Add an outcome such as `condition_failed` to `BatchWriteItemResult`. Not chosen: it changes `BatchWriteItemResult`,
the `FokosStd` helper, and the property-based model, and `code` already gives the same information. A caller that
handles `rejected` already handles a condition failure correctly: it does not send the item again.

### 5.2 A plan cache for conditions in the partition

Keep the plan of a tree object in a `WeakMap`, as `projectionPlanOf` does, or key a cache by the canonical identity of
the tree. Not chosen:

- The compile of one point condition costs 0.4 to 10.3 µs in the cases of the compile cost plan
  (`docs/agent-plans/2026-10-10-expression-compile-cost.md`, Section 8.1).
- In the benchmark of Section 4.2.7, 100 different SQL texts cost the same as one text. A cache of plans or of
  statements removes no measured cost.
- workerd already keeps prepared statements by text, and a sink gives one text for many items (Section 1.3).
- A `WeakMap` helps only when items share one tree object, and a sink gives a different tree for each item.

## 6. Frequently Asked Questions

### 6.1 Why does the DynamoDB `BatchWriteItem` have no conditions?

The response of `BatchWriteItem` has only `UnprocessedItems`. A put or a delete goes there when the provisioned
throughput is exceeded or an internal processing failure occurs, and the caller sends it again in a loop. The response
has no field for one item that fails for a reason that a resend does not clear, such as a failed condition. A blind
put or delete also gives the same final state when the loop sends it again, and a write with a condition does not.

DynamoDB has conditions in a batch on a newer API. In the PartiQL `BatchExecuteStatement`, each statement can have a
`WHERE` condition, each statement gets its own error (`BatchStatementError`, with the code `ConditionalCheckFailed`),
and each `BatchStatementRequest` has its own `ReturnValuesOnConditionCheckFailure`.

`batchWriteItems` of FokosDB already gives one result for each item (`ok`, `rejected`, `unprocessed`, or `unknown`),
as `BatchExecuteStatement` does. Section 4.2.5 has the resend rule for a write with a condition.

## 7. References

- `docs/ideas/2026-10-21-idempotent-sink-writes.md`
- `docs/agent-plans/2026-10-17-batch-get-and-write-items.md`
- `docs/agent-plans/2026-10-10-expression-compile-cost.md`
- [Amazon DynamoDB API Reference: BatchWriteItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html)
- [Amazon DynamoDB API Reference: BatchExecuteStatement](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchExecuteStatement.html)
- [Amazon DynamoDB API Reference: BatchStatementError](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchStatementError.html)
- [Amazon DynamoDB API Reference: BatchStatementRequest](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchStatementRequest.html)
