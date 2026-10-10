# RFC — Send expression trees to the partitions, and compile them in the partition

**State:** Draft
**Date:** 2026-10-10
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
8. [Appendix](#8-appendix)

---

## 1. Overview and Context

### 1.1 The problem

`FokosDB` compiles each condition, update, projection, and query filter into a SQL plan in the Worker. It sends
the plan to the partition, and the partition runs the SQL text of the plan. This causes four problems.

1. **The partition runs SQL text that the request carries.** `validateConditionPlan`, `validateUpdatePlan`,
   `validateProjectionPlan` and `validateQueryPlan` check only the plan version, the SQL size, and the binding
   counts. Thus any caller of the partition namespace can make a partition run SQL fragments of its choice.
   Section 6.1 of the typed expression engine spec accepts this risk only while every caller is controlled code.
2. **The plan is 3.4 to 17.8 times larger than the expression tree.** Most of the plan is SQL text. A typical
   condition with four terms is 320 bytes as a tree and 2,738 bytes as a plan. Appendix 8.1 has the data.
3. **The compiled SQL is a stored format.** The transaction coordinator writes each plan into
   `tc_items.conditions_json` and `tc_items.update_json`, and it sends the stored plan again on recovery. Thus
   every change to how a partition runs an expression must also run each plan that an older version stored.
   Section 6.4 of the typed expression engine spec needs the old plan decoders for that reason.
4. **The package version of the client decides the SQL that a partition runs.** The SQL holds schema facts:
   the `data_kind` codes, the binding layout, and the type of each bound literal. A client on an older package
   version sends SQL that matches the older schema. For example, an integer literal stored as a JSON float
   until a compiler change bound it with `CAST(? AS INTEGER)`, and only a new client gets the fix.

### 1.2 What the reader must know about the current system

- **Compilation in the client.** `packages/fokosdb/src/client/db.ts` calls `compileConditionExpression`,
  `compileUpdateExpression`, `compileProjectionExpression` and `compileQueryExpression`
  (`src/shared/expression/compiler.ts`). The compiler validates the tree, computes the canonical identity,
  renders the SQL, and checks the SQL limits. A tree that is not valid fails before any I/O.
- **Plan fields that other code reads:**
  - `identity` — `hashOperation` (`src/shared/transaction-idempotency.ts`) hashes it into the idempotency
    fingerprint.
  - `filterIdentity` and `projectionIdentity` — `computeCursorFingerprint` (`src/shared/query/cursor.ts`) uses
    them.
  - `requiredColumns` — `validateVersionReferences` (`src/shared/transaction-limits.ts`) reads it. The client
    calls it through `validateTransactWriteOperations`, and the partition calls it again in `sequencePlanOf`
    (`src/shared/partition/transaction-participant.ts`).
  - `names` — `projectedItemFromWireRow` builds the projected record from it.
- **Execution in the partition.** `PartitionStore.evaluateCondition`, `PartitionStore.probeUpdate`,
  `PartitionStore.insertPendingUpdateLock`, `PartitionStore.updateItemSingleShot`,
  `PartitionStore.getItemProjected` and `PartitionStore.scanQueryPage` take a plan, compose the statement, bind
  the parameters, and run it.
- **Commit does not use a plan.** A prepare evaluates the condition and the update, and the lock row stores the
  result in `op_list` and the data of the last write. `commitLocal` applies that stored result.
- **A repeated prepare evaluates nothing again.** When the lock row of an item already belongs to the same
  transaction, `#evaluate` marks the sequence `lockedBefore`, and the operation passes with the first answer.
- **The payload limit counts plan bytes.** `validateTransactWriteOperations` adds the JSON length of each
  condition plan and each update plan to the transaction payload.
- **An expression error is not a fatal prepare error.** `withExpressionErrors` raises `FokosExpressionError`.
  `isFatalPrepareError` in `src/server/do-transaction-coordinator.ts` accepts only `FokosValidationError`.

### 1.3 Glossary

- **Expression tree** — the public value that the caller gives: `ConditionExpression`, `UpdateExpression`,
  `ProjectionExpression[]`, or a query `filter` with a `projection` (`src/shared/expression/types.ts`).
- **Plan** — the output of the compiler: `CompiledConditionPlan`, `CompiledUpdatePlan`,
  `CompiledProjectionPlan`, or `CompiledQueryPlan` (`src/shared/expression/plan.ts`).
- **Client check** — an optional compile of the expression tree in the client, to fail before any I/O.

---

## 2. Goals and Requirements

### 2.1 In scope

1. Every RPC to a partition carries expression trees. No RPC to a partition carries a plan.
2. The partition compiles each expression tree before it runs the expression. The partition compile is the
   authoritative check.
3. The coordinator stores expression trees in `tc_items`. It stores no plan.
4. The coordinator computes the idempotency fingerprint from the canonical identity of each expression tree.
5. The client check is optional. Correctness does not depend on it.
6. The transaction payload limit counts the bytes of the expression trees.
7. A compile error in a prepare is a fatal prepare error.
8. After the rollout, a partition refuses a request that carries a plan.

### 2.2 Out of scope

- **A plan cache in the partition.** The value depends on how often one partition gets the same expression,
  and that number is not known. Section 4.3.4 records it as an investigation.
- **The pool layout for condition and update plans.** A partition can choose its layout without a wire change
  after this RFC. That change needs its own RFC.
- **A write condition as an array of conditions.** `docs/agent-plans/2026-10-09-condition-arrays.md` covers it.
  Section 4.3.6 records how the two RFCs interact.

### 2.3 Requirements

- **No durable state before a compile error.** For `putItem` and `deleteItem`, the partition compiles before
  its first write. For a transaction, a compile error cancels the transaction through the fatal-error path.
- **A commit never compiles.** A change of the compiler between prepare and commit must not change the result
  of the commit.
- **The client check can be stricter than the partition, and never more permissive.** When the client and the
  partition do not agree, the answer of the partition applies. Section 4.2.6 gives the rule.
- **The meaning of an expression must stay the same across compiler versions.** During a deploy, two
  participants of one transaction can run different compiler versions. Section 4.3.3 records the open question
  for a change of meaning.
- **The extra heap allocation in the partition must stay measured.** Section 4.2.8 gives the numbers and the
  ways to lower them.

---

## 3. Milestones

1. **The partition accepts both forms.** Each partition RPC accepts an expression tree or a plan. The partition
   compiles a tree, and runs a plan as it does now. A compile error in a prepare is fatal. This milestone ships
   alone: no client sends a tree yet.
2. **The client sends trees, and the coordinator stores trees.** `db.ts` sends trees. The coordinator stores
   trees and computes the fingerprint from the tree. The payload limit counts tree bytes. This milestone needs
   milestone 1 on every partition first.
3. **The partition refuses plans.** The partition removes the plan path. This milestone needs the end of the
   drain period of section 4.2.9.
4. **The client check becomes optional.** The client check gets its switch. Section 4.3.1 decides the default.
5. **Allocation investigation (optional).** Measure the levers of section 4.2.8, and the plan cache of section
   6.3.4.

---

## 4. Proposed Solution

### 4.1 High-level overview

The client sends the expression tree that the caller gave. Each partition compiles the tree when it needs the
SQL, and it uses the plan for that request only. The coordinator stores the tree and sends the tree to each
participant. The client can still compile the tree first, to fail before any I/O, but the partition decides.

```
caller ──tree──▶ FokosDB (db.ts)
                   │ validate the tree (names, requiredColumns, identity for the cursor)
                   │ optional client check: compile, then drop the plan
                   ├──tree──▶ PartitionDO ──compile──▶ plan ──▶ SQLite      (item RPCs, queries)
                   └──tree──▶ TransactionCoordinatorDO
                                │ store the tree in tc_items, fingerprint = hash(identity(tree))
                                └──tree──▶ PartitionDO (prepare) ──compile──▶ plan ──▶ SQLite
                                           PartitionDO (commit)  ──▶ applies the stored lock rows, no compile
```

The result:

- A partition runs only SQL that it generated.
- A request carries 3.4 to 17.8 times fewer expression bytes.
- A partition can change how it runs an expression without a change in the coordinator or the client.
- The cost moves to the partition: 2 to 500 µs of CPU, and 2 KiB to 1 MiB of short-lived heap, for each
  compile. Section 4.2.7 and section 4.2.8 give the numbers.

### 4.2 Technical details

#### 4.2.1 Wire types

Each field below changes from a plan type to a tree type:

| Type | Field | Today | After |
| --- | --- | --- | --- |
| `PutItemRpcRequest` (`src/server/do-partition.ts`) | `condition` | `CompiledConditionPlan` | `ConditionExpression` |
| `DeleteItemRpcRequest` | `condition` | `CompiledConditionPlan` | `ConditionExpression` |
| `GetItemRpcRequest` | `projection` | `CompiledProjectionPlan` | `ProjectionExpression[]` |
| The query RPC request | `plan` | `CompiledQueryPlan \| null` | `{ filter?, projection? } \| null` |
| `TransactionItem`, `TCWriteOperation` (`src/shared/transaction-wire-types.ts`) | `condition`, `update` | plans | trees |
| `TransactionReadItem` | `projection` | `CompiledProjectionPlan` | `ProjectionExpression[]` |
| `TransactWriteOperationLike` (`src/shared/transaction-limits.ts`) | `condition`, `update` | plans | trees |

During milestone 1 and milestone 2, each field accepts the union of both types. The partition tells them apart
by the `kind` and `version` fields, which only a plan has. Milestone 3 removes the plan type from the union.

#### 4.2.2 The client

`db.ts` keeps these steps for each expression tree:

1. Validate the tree with `validateConditionExpression`, `validateUpdateExpression` or
   `validateProjectionExpression` (`src/shared/expression/semantic.ts`). Validation gives `requiredColumns` and
   the projection `names`. It costs 0.4 to 18 KiB of heap (appendix 8.1).
2. For a query, compute `filterIdentity` and `projectionIdentity` with `canonicalConditionIdentity` and
   `canonicalProjectionIdentity` (`src/shared/expression/identity.ts`). `computeCursorFingerprint` needs them.
3. When the client check is on, compile the tree, then drop the plan. A compile error throws
   `FokosExpressionError` before any I/O, as it does today.
4. Send the tree.

`validateVersionReferences` reads `requiredColumns` from the validation result, not from a plan. The payload
count in `validateTransactWriteOperations` adds `JSON.stringify` of each tree.

The client check keeps the compiler in the client bundle. Without the client check, the client imports only the
validator and the identity module: about 23 KB minified in place of about 48 KB. Section 4.3.1 asks if the
bundle must make the compiler optional.

#### 4.2.3 Compilation in the partition

The partition compiles a tree at the first point where it needs the SQL:

| Operation | Compile point |
| --- | --- |
| `apiPutItem`, `apiDeleteItem` | In the handler, before the condition runs and before the first write. |
| `apiGetItem` with a projection | Before `PartitionStore.getItemProjected`. |
| `apiQueryItems` | Before the first `PartitionStore.scanQueryPage` of the request. One plan serves all visits of one request. |
| `txPrepare`, `txExecuteSingleShot` | In `TransactionParticipant.#evaluate`, for each operation that the partition evaluates. |
| `txReadForTransaction`, `txReadSnapshot` | Before `PartitionStore.getItemProjected` for the item. |

Rules:

- **Compile only what runs.** A prepare compiles no operation of a sequence that is `lockedBefore`. Thus a
  repeated prepare compiles nothing.
- **`validateVersionReferences` runs on the compiled analysis.** `sequencePlanOf` needs `requiredColumns`
  before `#evaluate`. It gets them from `validateConditionExpression` and `validateUpdateExpression`, which
  cost 0.4 to 18 KiB of heap, and not from a full compile.
- **The plan lives for one request.** No field and no table keeps it. `materializedPlanBindings` keeps its
  `WeakMap` cache by the descriptor array, so the statements of one request share one set of bound values.
- **A forward compiles again.** A router that forwards a request sends the tree. The owner compiles it. A
  read-through to the source compiles on the source.

#### 4.2.4 Transactions

- **Storage.** The coordinator writes `JSON.stringify` of each tree into `tc_items.conditions_json` and
  `tc_items.update_json`. The column names stay. `stripPayload` clears them on completion, as it does today.
  `migratedTransactionBytes` reads the size of the two columns, so it counts the tree bytes with no change.
- **Fingerprint.** `hashOperation` hashes the canonical identity of each tree. The coordinator computes it with
  `canonicalConditionIdentity` and `canonicalUpdateIdentity`. It does not take an identity from the request.
  The function and its input are the same as today, so a retry with the same request gets the same fingerprint.
- **Recovery.** A recovery drive sends the stored tree. The participant compiles it with its current compiler.
- **Commit.** No change. A commit carries no expression, and `commitLocal` applies the stored lock rows.

#### 4.2.5 Errors

| Case | Error | Caller retries |
| --- | --- | --- |
| The client check fails | `FokosExpressionError`, before any I/O | No |
| A compile fails in `apiPutItem`, `apiDeleteItem`, `apiGetItem`, `apiQueryItems` | `FokosExpressionError`, from the partition | No |
| A compile fails in `txPrepare` | A fatal prepare error. The coordinator cancels. The caller gets the transaction cancellation with the expression error as the reason of the operation. | No |
| A partition gets a plan after milestone 3 | `FokosValidationError` | No |

A compile error must be fatal in prepare. Today `isFatalPrepareError` accepts only `FokosValidationError`, so the
coordinator retries an expression error until the transaction is stale. Section 4.3.2 asks which of the two
mechanisms makes it fatal.

#### 4.2.6 Limits

The partition compile is the authoritative check of `EXPRESSION_LIMITS`, also of `compiledSqlBytes` and
`completeStatementBindings`. The client check uses the limits of the client package version.

- When the client check rejects a tree, the request stops before any I/O.
- When the client check accepts a tree and the partition rejects it, the error of section 4.2.5 applies.
- When a partition removes or raises a limit, an older client check still rejects at the older limit. A caller
  must upgrade the client to use the new limit. The client check stays a stricter filter, and it cannot make a
  partition run an expression that the partition rejects.

#### 4.2.7 Performance

CPU for one received request, in Node v24.20 with default flags. "Today" is the deserialize of the plan, the plan
checks, the statement composition, and the binding materialization. "Tree" is the deserialize of the tree, the
compile, and the same composition and materialization.

| Case | Today µs | Tree µs | Extra µs |
| --- | ---: | ---: | ---: |
| Condition: optimistic lock | 3.3 | 6.7 | 3.4 |
| Condition: four terms on paths | 7.2 | 16.4 | 9.2 |
| Condition: 40 distinct path `eq` | 28.1 | 137.0 | 108.9 |
| Condition: 149 `eq` on one path (about 80 KB SQL) | 46.1 | 495.4 | 449.3 |
| Update: counter and timestamp | 4.5 | 17.5 | 13.0 |
| Update: 20 actions with arithmetic | 17.8 | 165.2 | 147.4 |
| Projection: 3 paths and `v` | 4.2 | 11.4 | 7.2 |
| Projection: 48 paths | 33.9 | 175.4 | 141.5 |
| Query: four-term filter and 5 projections | 10.9 | 40.6 | 29.7 |
| Query: 40-term filter and 48 projections | 90.7 | 352.0 | 261.3 |

For scale, the SQLite statement of the same expression runs in 4 to 111 µs in workerd, with the direct layout.
One RPC hop takes about 1 ms (napkin estimate, `TODO: measure`).

How the cost grows:

- **Once per request for each partition.** A query compiles once on each partition that it visits, for each
  page.
- **More compiles in a transaction.** A transaction compiles once for each participant, for each prepare drive
  that evaluates. A read transaction compiles twice: once for each phase.
- **A forward adds a compile on the owner.** A read-through to the source adds one on the source.

#### 4.2.8 Memory and garbage collection

Heap allocated for each compile, from appendix 8.1:

| Expression size | Examples | Allocated by the compile |
| --- | --- | ---: |
| Small | `not_exists`, one path `eq`, one update literal, one projection path | 2 to 11 KiB |
| Typical | four-term condition, counter update, workflow query with 5 projections | 20 to 51 KiB |
| Large | 40 path terms, 20 update actions, 48 projections | 126 to 260 KiB |
| At the SQL limit | 149 `eq` terms, 50 `contains`, 40-term filter with 48 projections | 538 to 896 KiB |

A plan that a request keeps alive holds 0.3 to 155 KiB. The memory limit of an isolate is 128 MB.

The GC cost depends on the objects that survive a scavenge, not on the garbage. The compile garbage dies before
the request ends. Measured GC time for each call, from appendix 8.2:

| Young generation | Extra GC µs per call | GC share of the call | Old-generation collections |
| --- | ---: | ---: | --- |
| Node default | up to 28 | 4.5% to 10.2% (today: 14.4% to 26.8%) | none in 20,000 calls, both forms |
| 1 MiB (worst case) | 0.5 to 282 | 16.7% to 42.4% (today: 28.2% to 64.1%) | 0 to 6, at most 1 apart between the forms |

`TODO: measure` the young generation size of workerd. The default of Node can differ from it.

Where the allocation goes:

- **Validation:** 0.4 to 18 KiB. The partition needs it.
- **Canonical identity:** 0.1 to 76 KiB, 5% to 21% of a compile. Only the coordinator and the client need it. A
  partition compile can skip it.
- **SQL rendering, compaction, and the size check:** the rest, 67% to 91% of a compile.

Levers to lower the allocation, for milestone 5:

1. Skip the canonical identity in the partition compile.
2. Reuse the statement that the compiler composes for the size check, in place of a second composition.
   `TODO: measure`.
3. Remove the second copy of the SQL that `compactPlanParameters` makes with `replace` when it renumbers.
   `TODO: measure`.
4. Cache the plan in the partition (section 4.3.4).

#### 4.2.9 Deployment, migration, and rollback

1. Deploy milestone 1 to every partition and coordinator class. Nothing changes for a client.
2. Deploy milestone 2 to the clients. A new client sends trees. An old client still sends plans, and the
   partition still runs them. The coordinator stores what it gets.
3. **Drain period.** The partition accepts plans while a `tc_items` row can still hold a plan. `stripPayload`
   clears the payload when a transaction completes. Thus the drain ends when every transaction that started
   before step 2 is complete, and when no old client remains. Section 4.3.5 asks how to detect that.
4. Deploy milestone 3. The partition refuses a plan.

Rollback:

- **Rollback of a client** to a version before milestone 2 is safe, because the partition still accepts plans.
- **Rollback of a partition** to a version before milestone 1 is not safe after step 2: an old partition cannot
  compile a tree. Roll back the clients first.

#### 4.2.10 Testing

- Tests that build a plan and send it to a partition or a store send a tree. These include
  `transaction-participant.test.ts`, `transaction-participant-ordered.test.ts`, `partition-store.test.ts`,
  `runtime.test.ts`, `projection-runtime.test.ts` and `do-transaction-coordinator.test.ts`.
- A partition refuses a request that carries SQL text after milestone 3.
- A compile error in a prepare cancels the transaction with no retry. The coordinator tests of fatal prepare
  errors cover the new error.
- A repeated prepare of a locked sequence compiles nothing.
- `putItem` with a tree above `compiledSqlBytes` writes nothing and fails with `sql_limit`, with the client
  check off.
- The fingerprint of a request is the same before and after milestone 2.
- During milestone 1 and milestone 2, each partition RPC accepts both a tree and a plan, and gives the same
  result for both.

### 4.3 Open Questions

#### 4.3.1 The default of the client check

Options: on by default, or off by default. On keeps the fail-before-I/O behavior of today and keeps the compiler
in the client bundle. Off saves about 25 KB minified in the client and its compile CPU, and moves every
expression error to the partition. The answer also decides if the build must keep the compiler out of the
client graph when the check is off.

#### 4.3.2 How a compile error becomes fatal in prepare

Options:

1. The partition raises a compile error in prepare as a `FokosValidationError`.
2. `isFatalPrepareError` also accepts `FokosExpressionError`.

Option 1 changes the error class that a caller sees for one path. Option 2 changes only the coordinator.

#### 4.3.3 A change of meaning in the expression language

Two participants of one transaction can run different compiler versions during a deploy. A compiler change that
keeps the meaning, such as the layout or the shape of the SQL, needs nothing. A change of meaning needs a
version in the request. The question: does the tree carry an expression-language version that the partition
must know, or does each change of meaning get its own rollout?

#### 4.3.4 A plan cache in the partition

A cache keyed by the plan kind, the plan version and the canonical identity costs 0.2 to 11 µs for each lookup.
A compile costs 2 to 500 µs. The value depends on the share of requests that repeat an expression on one
partition. `TODO: measure` that share for the expected workloads before a cache is built.

#### 4.3.5 The end of the drain period

What tells the operator that no `tc_items` row and no client sends a plan? Options include a count of the plans
that partitions receive, or a fixed period longer than the stale-transaction window.

#### 4.3.6 Interaction with condition arrays

`docs/agent-plans/2026-10-09-condition-arrays.md` makes `condition` an array of plans. Its section 5.2 rejects
compilation in the partition. Section 6 of this RFC answers its four reasons. The two RFCs must agree on one
wire type for the condition before either ships.

---

## 5. Alternative Options

### 5.1 Keep the plans on the wire

This is the current design. Not chosen: it keeps the four problems of section 1.1.

### 5.2 Authenticate the plans

The client signs each plan, and the partition checks the signature. This removes problem 1 only. The plan stays
large, stays a stored format, and still depends on the client version.

### 5.3 The coordinator compiles at intake

The coordinator compiles each tree before it writes `CREATED`, to reject a transaction before the prepare
fan-out. Not chosen: it ties the coordinator to the compiler version and to its limits, which this RFC removes.
The client check covers the early failure.

### 5.4 Compile in the client only, send the tree, and trust the client

The client compiles and sends the tree, and the partition runs the tree without a second check. Not possible:
the partition needs SQL to run, so it must compile.

---

## 6. Frequently Asked Questions

**Can a partition upgrade break a transaction that a partition already accepted?**
No. A commit uses the stored lock rows and no expression. A repeated prepare passes a locked sequence without a
compile. A partition that rejects a tree during prepare does so while the coordinator is in `PREPARING`, and the
coordinator cancels.

**`2026-10-09-condition-arrays.md` rejects compilation in the partition for four reasons. What changes?**

1. *An expression that is not valid fails only after I/O.* The client check keeps the failure before I/O.
   Without the client check, a transaction with a bad expression makes a coordinator row, a prepare, and a
   cancel.
2. *A retry, a recovery drive, and a forward compile again.* Yes. Section 4.2.7 gives the cost, and section
   4.3.4 the cache.
3. *The coordinator needs the identity, and the version check needs the columns.* The coordinator computes the
   identity from the tree. The version check gets `requiredColumns` from validation.
4. *The changes are the same for a tree and for a plan.* The changes are the same. The stored format and the
   trust model are not.

**How much GC work does the compile in the partition add?**
In Node, with the default young generation, the compile adds up to 28 µs of GC time for each call, and no
old-generation collection runs. The GC time grows less than the CPU time, because the compile garbage dies before
the next scavenge. Section 4.2.8 lists the young generation of workerd as `TODO: measure`.

**Why does the partition not trust the client check?**
The partition must compile to get SQL. The client check runs on another package version, and a caller can
bypass it.

---

## 7. References

- `docs/agent-plans/2026-08-29-typed-expression-engine-spec.md` — sections 6.1, 6.3, 6.4 and 6.5.
- `docs/agent-plans/2026-09-02-update-expressions.md`
- `docs/agent-plans/2026-09-14-read-projections-and-query-filters.md`
- `docs/agent-plans/2026-10-03-ordered-per-item-transact-write.md`
- `docs/agent-plans/2026-10-09-condition-arrays.md`
- `packages/fokosdb/src/shared/expression/compiler.ts`, `plan.ts`, `runtime.ts`, `semantic.ts`, `identity.ts`
- [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Workers limits](https://developers.cloudflare.com/workers/platform/limits/) — memory per isolate.

---

## 8. Appendix

### 8.1 Allocation for each case

Node v24.20.0, `--expose-gc` and a young generation of 256 MiB, so that no GC runs while the script counts. The
value is the mean heap growth for one call after a warm-up. "Partition today" is the deserialize of the plan, the
plan checks, the statement composition and the binding materialization. "Partition with tree" is the deserialize
of the tree, the compile, and the same composition and materialization. All values except the byte columns are
KiB.

| Case | Tree B | Plan B | Validate | Identity | Compile | Partition today | Partition with tree | Extra | Plan kept |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| cond: `true` | 13 | 263 | 0.4 | 0.1 | 1.9 | 1.2 | 2.6 | 1.4 | 0.3 |
| cond: `not_exists(hashKey)` | 46 | 327 | 0.5 | 0.3 | 2.1 | 1.2 | 3.2 | 1.9 | 0.7 |
| cond: optimistic lock | 108 | 567 | 0.7 | 0.9 | 4.8 | 1.6 | 6.4 | 4.7 | 1.6 |
| cond: `sortKey` `begins_with` | 68 | 598 | 0.5 | 0.5 | 4.4 | 1.9 | 5.6 | 3.7 | 1.3 |
| cond: one path `eq` | 71 | 966 | 0.4 | 0.5 | 6.5 | 2.2 | 7.7 | 5.5 | 1.7 |
| cond: `between` on a path | 80 | 1,573 | 0.5 | 0.7 | 11.1 | 3.0 | 12.2 | 9.2 | 2.5 |
| cond: `contains` on an array path | 72 | 1,791 | 0.4 | 0.5 | 9.7 | 3.1 | 10.9 | 7.8 | 2.5 |
| cond: four terms | 320 | 2,738 | 0.4 | 2.4 | 20.4 | 4.9 | 23.6 | 18.7 | 4.9 |
| cond: nested access policy | 390 | 3,808 | 0.4 | 2.8 | 28.0 | 5.8 | 32.0 | 26.2 | 6.3 |
| cond: nested SQLite text functions | 310 | 1,383 | 1.6 | 2.1 | 19.5 | 3.2 | 22.6 | 19.4 | 3.4 |
| cond: `concat` of 32 paths | 1,309 | 9,335 | 0.9 | 8.2 | 77.3 | 16.0 | 85.3 | 69.3 | 16.0 |
| cond: `in` with 90 choices | 1,843 | 6,643 | 0.4 | 14.5 | 68.8 | 23.7 | 86.9 | 63.2 | 20.2 |
| cond: 40 distinct path `eq` | 2,481 | 28,660 | 0.4 | 21.2 | 227.7 | 43.6 | 255.6 | 212.0 | 46.6 |
| cond: 149 `eq` on one path | 8,812 | 95,406 | 0.4 | 75.9 | 895.9 | 255.7 | 1,025.5 | 769.8 | 154.8 |
| cond: 50 `contains` on one path | 3,371 | 73,346 | 0.4 | 27.1 | 538.2 | 170.5 | 640.7 | 470.2 | 94.8 |
| upd: set 1 literal | 85 | 717 | 1.3 | 0.7 | 7.0 | 2.6 | 8.9 | 6.4 | 1.4 |
| upd: remove 1 path | 66 | 574 | 1.0 | 0.5 | 5.6 | 2.2 | 7.1 | 5.0 | 1.1 |
| upd: counter and timestamp | 260 | 2,589 | 2.1 | 2.1 | 23.2 | 4.8 | 26.5 | 21.7 | 4.0 |
| upd: 10 literal sets | 731 | 3,353 | 3.8 | 6.1 | 40.4 | 8.2 | 46.7 | 38.5 | 6.5 |
| upd: 20 actions with arithmetic | 2,785 | 28,277 | 18.2 | 22.1 | 259.2 | 36.5 | 284.8 | 248.3 | 41.0 |
| upd: 32 literal sets | 2,381 | 9,909 | 10.9 | 20.5 | 126.1 | 21.9 | 143.8 | 122.0 | 19.6 |
| proj: 1 path | 41 | 972 | 0.8 | 0.4 | 7.6 | 3.4 | 9.9 | 6.5 | 1.7 |
| proj: whole `data` | 25 | 946 | 0.8 | 0.3 | 6.2 | 3.2 | 8.2 | 5.0 | 1.5 |
| proj: 3 paths and `v` | 103 | 1,854 | 0.6 | 0.9 | 13.0 | 5.7 | 17.1 | 11.4 | 3.2 |
| proj: 48 paths | 2,669 | 36,173 | 3.5 | 18.8 | 255.4 | 93.1 | 307.2 | 214.1 | 55.7 |
| query: one path `eq` filter | 82 | 1,071 | 0.6 | 0.6 | 10.3 | 6.8 | 16.1 | 9.3 | 1.7 |
| query: four-term filter | 331 | 3,044 | 0.6 | 2.4 | 26.1 | 9.8 | 34.2 | 24.5 | 5.1 |
| query: four-term filter and 5 projections | 552 | 6,564 | 1.4 | 4.0 | 50.5 | 23.2 | 68.8 | 45.5 | 10.4 |
| query: 40-term filter and 48 projections | 4,369 | 66,099 | 4.1 | 35.6 | 548.9 | 256.0 | 768.3 | 512.3 | 98.8 |

### 8.2 GC time for each call

Node v24.20.0, `--expose-gc`, 20,000 calls for each row after 5,000 warm-up calls. Each call keeps only its own
result alive, as a request does. `GCProfiler` gives the GC pauses.

| Case | Form | Young generation | µs per call | GC µs per call | GC share | Scavenges | Old-generation GCs |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| cond: optimistic lock | today | default | 3.3 | 0.53 | 16.0% | 4 | 0 |
| cond: optimistic lock | tree | default | 6.7 | 0.68 | 10.2% | 5 | 0 |
| cond: four terms | today | default | 7.2 | 1.06 | 14.7% | 2 | 0 |
| cond: four terms | tree | default | 16.4 | 1.02 | 6.2% | 7 | 0 |
| cond: 40 distinct path `eq` | today | default | 28.1 | 4.87 | 17.3% | 24 | 0 |
| cond: 40 distinct path `eq` | tree | default | 137.0 | 10.61 | 7.7% | 82 | 0 |
| cond: 149 `eq` on one path | today | default | 46.1 | 10.62 | 23.1% | 113 | 0 |
| cond: 149 `eq` on one path | tree | default | 495.4 | 38.81 | 7.8% | 327 | 0 |
| upd: counter and timestamp | today | default | 4.5 | 0.98 | 21.7% | 2 | 0 |
| upd: counter and timestamp | tree | default | 17.5 | 1.05 | 6.0% | 8 | 0 |
| upd: 20 actions with arithmetic | today | default | 17.8 | 4.77 | 26.8% | 22 | 0 |
| upd: 20 actions with arithmetic | tree | default | 165.2 | 10.52 | 6.4% | 89 | 0 |
| proj: 3 paths and `v` | today | default | 4.2 | 0.64 | 15.1% | 2 | 0 |
| proj: 3 paths and `v` | tree | default | 11.4 | 0.69 | 6.1% | 5 | 0 |
| proj: 48 paths | today | default | 33.9 | 5.99 | 17.7% | 41 | 0 |
| proj: 48 paths | tree | default | 175.4 | 8.16 | 4.7% | 97 | 0 |
| query: four-term filter and 5 projections | today | default | 10.9 | 1.60 | 14.7% | 9 | 0 |
| query: four-term filter and 5 projections | tree | default | 40.6 | 1.82 | 4.5% | 21 | 0 |
| query: 40-term filter and 48 projections | today | default | 90.7 | 13.07 | 14.4% | 105 | 0 |
| query: 40-term filter and 48 projections | tree | default | 352.0 | 20.19 | 5.7% | 240 | 0 |
| cond: optimistic lock | today | 1 MiB | 3.3 | 0.93 | 28.2% | 52 | 0 |
| cond: optimistic lock | tree | 1 MiB | 6.6 | 1.39 | 21.0% | 140 | 0 |
| cond: four terms | today | 1 MiB | 7.1 | 2.61 | 36.9% | 169 | 1 |
| cond: four terms | tree | 1 MiB | 20.7 | 4.05 | 19.6% | 534 | 1 |
| cond: 40 distinct path `eq` | today | 1 MiB | 43.2 | 16.79 | 38.9% | 1,563 | 4 |
| cond: 40 distinct path `eq` | tree | 1 MiB | 187.9 | 45.96 | 24.5% | 5,814 | 4 |
| cond: 149 `eq` on one path | today | 1 MiB | 139.9 | 89.67 | 64.1% | 9,182 | 6 |
| cond: 149 `eq` on one path | tree | 1 MiB | 877.6 | 372.13 | 42.4% | 26,253 | 5 |
| upd: counter and timestamp | today | 1 MiB | 6.1 | 2.44 | 39.9% | 170 | 0 |
| upd: counter and timestamp | tree | 1 MiB | 19.6 | 3.28 | 16.7% | 527 | 0 |
| upd: 20 actions with arithmetic | today | 1 MiB | 28.0 | 13.50 | 48.2% | 1,322 | 4 |
| upd: 20 actions with arithmetic | tree | 1 MiB | 218.9 | 55.11 | 25.2% | 6,651 | 4 |
| proj: 3 paths and `v` | today | 1 MiB | 5.8 | 1.88 | 32.4% | 190 | 0 |
| proj: 3 paths and `v` | tree | 1 MiB | 14.1 | 2.86 | 20.4% | 365 | 1 |
| proj: 48 paths | today | 1 MiB | 65.3 | 30.41 | 46.5% | 3,290 | 5 |
| proj: 48 paths | tree | 1 MiB | 224.7 | 53.09 | 23.6% | 6,662 | 4 |
| query: four-term filter and 5 projections | today | 1 MiB | 18.0 | 7.12 | 39.6% | 686 | 1 |
| query: four-term filter and 5 projections | tree | 1 MiB | 53.1 | 11.66 | 21.9% | 1,516 | 2 |
| query: 40-term filter and 48 projections | today | 1 MiB | 169.3 | 77.50 | 45.8% | 7,422 | 6 |
| query: 40-term filter and 48 projections | tree | 1 MiB | 547.3 | 191.80 | 35.0% | 19,013 | 5 |
