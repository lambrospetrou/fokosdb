# RFC — Send expression trees to the partitions, and compile them in the partition

**State:** Implemented
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
- **Client validation** — the check of the expression tree in the client with the validator
  (`src/shared/expression/semantic.ts`), to fail before any I/O. The client never compiles.

---

## 2. Goals and Requirements

### 2.1 In scope

1. Every RPC to a partition carries expression trees. No RPC to a partition carries a plan.
2. The partition compiles each expression tree before it runs the expression. The partition compile is the
   authoritative check.
3. The coordinator stores expression trees in `tc_items`. It stores no plan.
4. The coordinator computes the idempotency fingerprint from the canonical identity of each expression tree.
5. The client validates each tree and never compiles. Correctness does not depend on the client.
6. The transaction payload limit counts the bytes of the expression trees.
7. A compile error in a prepare is a fatal prepare error.
8. After the rollout, a partition refuses a request that carries a plan.

### 2.2 Out of scope

- **A plan cache in the partition.** Section 4.3.4 records the decision: no cache.
- **The pool layout for condition and update plans.** A partition can choose its layout without a wire change
  after this RFC. That change needs its own RFC.
- **A write condition as an array of conditions.** `docs/agent-plans/2026-10-09-condition-arrays.md` covers it.
  Section 4.3.6 records how the two RFCs interact.

### 2.3 Requirements

- **No durable state before a compile error.** For `putItem` and `deleteItem`, the partition compiles before
  its first write. For a transaction, a compile error cancels the transaction through the fatal-error path.
- **A commit never compiles.** A change of the compiler between prepare and commit must not change the result
  of the commit.
- **The client validation can be stricter than the partition, and never more permissive.** When the client
  and the partition do not agree, the answer of the partition applies. Section 4.2.6 gives the rule.
- **The meaning of an expression never changes.** During a deploy, two participants of one transaction can run
  different compiler versions. A compiler change can change the layout or the shape of the SQL, and it must
  keep the meaning. Thus a tree carries no version of the expression language.
- **The extra heap allocation in the partition must stay measured.** Section 4.2.8 gives the numbers and the
  ways to lower them.

---

## 3. Milestones

All milestones are implemented. No deploy occurred between them.

1. **The partition and the coordinator accept both forms.** Each partition RPC accepted an expression tree or a
   plan. The partition compiled a tree, and ran a plan as before. The coordinator stored the form that it got,
   and computed the fingerprint from the identity of a tree or from the identity field of a plan. Each
   `FokosExpressionError` from a prepare became fatal.
2. **The client sends trees.** `db.ts` sends trees. The payload limit counts tree bytes.
3. **The partition refuses plans.** The partition removed the plan path. Each request field has a tree type
   only, and a request that carries a plan gets `compiled_plan_refused`.
4. **The client does not compile.** The client validates each tree and computes the query identities. The
   build fails when the client entry reaches the compiler, and `fokosdb/client` does not export the compile
   functions.
5. **Allocation investigation.** Section 4.2.8 has the measurements. Lever 1 and lever 2 are implemented.

---

## 4. Proposed Solution

### 4.1 High-level overview

The client sends the expression tree that the caller gave. Each partition compiles the tree when it needs the
SQL, and it uses the plan for that request only. The coordinator stores the tree and sends the tree to each
participant. The client validates the tree first, to fail before any I/O, but the partition decides.

```
caller ──tree──▶ FokosDB (db.ts)
                   │ validate the tree (names, requiredColumns, identity for the cursor)
                   │ no compile
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
- The client bundle has no compiler.
- The cost moves to the partition: CPU and short-lived heap for each compile. Section 4.2.7 and section 4.2.8
  give the numbers.

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

The query type is `QueryExpressions` (`src/shared/expression/types.ts`). No field accepts a plan. A plan has a
`kind` field and a `version` field, which no tree has. `refuseCompiledPlan` (`src/shared/expression/plan.ts`)
throws `compiled_plan_refused` for a value that has both.

#### 4.2.2 The client

`db.ts` does these steps for each expression tree:

1. Validate the tree with `validateConditionExpression`, `validateUpdateExpression` or
   `validateProjectionExpression` (`src/shared/expression/semantic.ts`). Validation gives `requiredColumns` and
   the projection `names`. It costs 0.4 to 22 KiB of heap (section 4.2.8). A tree that is not valid throws
   `FokosExpressionError` before any I/O.
2. For a query, compute `filterIdentity` and `projectionIdentity` with `canonicalConditionIdentity` and
   `canonicalProjectionIdentity` (`src/shared/expression/identity.ts`). `computeCursorFingerprint` needs them.
3. Send the tree.

The client never compiles. `pnpm build` fails when the client entry reaches
`src/shared/expression/compiler.ts` or `src/shared/expression/request-plans.ts`, and `fokosdb/client` does not
export a compile function. Thus only a partition checks the limits of the compiled SQL.

`validateVersionReferences` gets `requiredColumns` from validation. It validates a tree only for an operation
that follows a write of the same item. The payload count in `validateTransactWriteOperations` adds
`JSON.stringify` of each tree.

#### 4.2.3 Compilation in the partition

The compile is in `PartitionStore`. Each store method that takes an expression compiles the tree before its
statement runs (`src/shared/expression/request-plans.ts`). The handlers and the participant pass the tree of
the request through with no change. Thus the partition compiles at the first point where it needs the SQL:

| Operation | Compile point |
| --- | --- |
| `apiPutItem`, `apiDeleteItem` | In `PartitionStore.evaluateCondition`, after the lock check and before the first write. |
| `apiGetItem` with a projection | In `PartitionStore.getItemProjected`. |
| `apiQueryItems` | In `PartitionStore.scanQueryPage`, which one request calls one time on each partition. |
| `txPrepare`, `txExecuteSingleShot` | In the store calls of `TransactionParticipant.#evaluate`, for each operation that the partition evaluates. The storage transaction rolls back on a compile error. |
| `txReadForTransaction`, `txReadSnapshot` | In `PartitionStore.getItemProjected` for the item. |

Rules:

- **Compile only what runs.** A prepare compiles no operation of a sequence that is `lockedBefore`. Thus a
  repeated prepare compiles nothing.
- **`validateVersionReferences` does not compile.** `sequencePlanOf` needs `requiredColumns` before
  `#evaluate`. It gets them from `validateConditionExpression` and `validateUpdateExpression`, and only for an
  operation that follows a write of the same item.
- **The plan lives for one request.** No table keeps it, and it carries no identity. One request runs the
  plan of an update in more than one statement: the probe, then a write or a lock row. `updatePlanOf` keeps
  that plan in a `WeakMap` by the tree object of the request, so the plan is released with the request.
  `materializedPlanBindings` keeps its `WeakMap` cache by the descriptor array, so the statements of one
  request share one set of bound values.
- **No second check of the plan.** The partition runs the plan that it compiled. `runtime.ts` has no plan
  validator.
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
| The client validation fails | `FokosExpressionError`, before any I/O | No |
| A compile fails in `apiPutItem`, `apiDeleteItem`, `apiGetItem`, `apiQueryItems` | `FokosExpressionError`, from the partition. Nothing is written. | No |
| A compile fails in `txPrepare` | A fatal prepare error. The coordinator cancels. The caller gets the transaction cancellation with `expression_invalid` as the reason of the operation. | No |
| A partition gets a plan | `FokosValidationError` with the code `compiled_plan_refused`. In a prepare it is fatal. | No |

Each `FokosExpressionError` from a prepare is fatal: `prepareRetry` does not retry it, and
`isFatalPrepareError` accepts it next to `FokosValidationError`. This includes the `runtime_capability` code,
which says that SQLite did not run the compiled statement.

#### 4.2.6 Limits

`EXPRESSION_LIMITS` (`src/shared/expression/limits.ts`) has a comment for each limit that gives its origin.

- **The validator checks the limits of the tree**: the counts, the depth, the path sizes, and the total UTF-8
  bytes of the text literals and the base64 literals (`canonicalPayloadBytes`, 512 KiB). The client and the partition
  both run the validator, so these errors occur before any I/O for a caller that uses the client.
- **Only the partition compile checks the limits of the compiled SQL**: `compiledSqlBytes`,
  `completeStatementBindings`, and the size of the pooled bindings of a projection or a query. A tree that
  passes validation and fails one of these gets `sql_limit` from the partition, after I/O. For example, an `in`
  with 100 distinct choices is valid and needs more than 100 parameters.
- **The coordinator checks the identity size.** The fingerprint computes the canonical identity of each
  condition and each update, which refuses an identity above `canonicalPayloadBytes`.
- When a partition removes or raises a limit of the validator, an older client still rejects at the older
  limit. A caller must upgrade the client to use the new limit. The client stays a stricter filter, and it
  cannot make a partition run an expression that the partition rejects.

#### 4.2.7 Performance

The numbers of this section are from before the changes of section 4.2.8, so the cost of a compile is now
lower.

CPU for one received request, in Node v24.20 with default flags. "Today" is the path before this RFC: the
deserialize of the plan, the plan checks, the statement composition, and the binding materialization. "Tree" is
the deserialize of the tree, the compile, and the same composition and materialization.

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

The numbers above are from before the two changes below.

Changes that are implemented:

1. **The partition compile computes no identity.** The plan types have no identity field. Only the coordinator
   and the client call the identity module. The identity step also enforced `canonicalPayloadBytes`, so the
   validator now counts the UTF-8 bytes of the literals (section 4.2.6).
2. **The partition does not check its own plan.** `validateConditionPlan`, `validateUpdatePlan`,
   `validateProjectionPlan` and `validateQueryPlan` are gone. Each one composed the statement a second time to
   repeat a check of the compiler.

Heap KiB for one expression on the partition path, `pnpm bench:alloc:expression`, Node v24.20.0:

| Case | Before | After | Saved |
| --- | ---: | ---: | ---: |
| cond: optimistic lock | 6.6 | 5.3 | 20% |
| cond: four terms | 24.0 | 19.1 | 20% |
| cond: 40 distinct path `eq` | 256.1 | 209.5 | 18% |
| cond: 80 `eq` on one path | 617.3 | 490.9 | 20% |
| upd: counter and timestamp | 26.1 | 22.4 | 14% |
| upd: 20 actions with arithmetic | 384.3 | 307.2 | 20% |
| proj: 3 paths and `v` | 21.7 | 20.3 | 6% |
| proj: 48 paths | 313.1 | 264.5 | 16% |
| query: four-term filter and 5 projections | 59.8 | 49.9 | 17% |
| query: 40-term filter and 48 projections | 901.5 | 633.8 | 30% |

Not implemented:

- **The parameter scan of `compactPlanParameters`.** The function scans each SQL fragment with `matchAll` for
  each compile, which costs 8% to 26% of the allocation of one expression. Its `replace` copy runs only when a
  bound parameter is not used, and no benchmark case causes that. A compiler that records the used parameters
  while it renders removes the scan.
- **A plan cache.** Section 4.3.4 records the decision.

#### 4.2.9 Deployment, migration, and rollback

The package is not released, so the milestones shipped together with no drain period. The client, the
coordinator and the partition must deploy at the same version:

- A client from before this RFC sends a plan. The partition refuses it with `compiled_plan_refused`.
- A `tc_items` row from before this RFC holds a plan. A recovery drive sends it, the participant refuses it,
  and the coordinator cancels the transaction, because the error is fatal.
- A rollback of a partition to a version from before this RFC is not safe: that partition cannot compile a
  tree.

#### 4.2.10 Testing

- Tests that sent a plan to a partition or a store send a tree. The helpers `conditionTree`, `updateTree`,
  `projectionTree` and `queryTree` (`src/shared/expression/test-fixtures.ts`) keep the literal types of a tree.
- Each partition RPC refuses a request that carries a plan, and writes nothing
  (`test/partition-do/expression-trees.test.ts`).
- A compile error in a prepare cancels the transaction with no retry. The coordinator tests of fatal prepare
  errors cover `FokosExpressionError`.
- A repeated prepare of a locked sequence compiles nothing: it accepts a tree that no compile accepts.
- `putItem` with a valid tree above `compiledSqlBytes` writes nothing and fails with `sql_limit` from the
  partition.
- A tree that is not valid fails in the client before any I/O.
- The partition receives the condition, the update and the projection as the caller gave them
  (`test/transactions/tx-expression-trees.test.ts`).
- The payload limit counts the JSON bytes of a tree.

### 4.3 Decisions and open items

#### 4.3.1 The client check

Decision: the client validates and never compiles, with no option. The compiler is not in the client bundle,
and `fokosdb/client` exports no compile function. The cost: a `sql_limit` error comes from the partition, after
I/O (section 4.2.6).

#### 4.3.2 How a compile error becomes fatal in prepare

Decision: `isFatalPrepareError` and `prepareRetry` accept each `FokosExpressionError`, with each expression
code. The partition raises the same error class for a prepare as for `putItem`.

#### 4.3.3 A change of meaning in the expression language

Decision: the meaning of an expression never changes. A tree carries no version of the expression language.

#### 4.3.4 A plan cache in the partition

Decision: no cache for now.

#### 4.3.5 The end of the drain period

Not applicable: no drain period occurred (section 4.2.9).

#### 4.3.6 Interaction with condition arrays

Open. `docs/agent-plans/2026-10-09-condition-arrays.md` makes `condition` an array of plans, and its section 5.2
rejects compilation in the partition. That RFC must change to an array of expression trees.

#### 4.3.7 Known gaps

- **The coordinator does not validate a tree.** The fingerprint computes the identity of each condition and
  each update. The identity step refuses a tree of a wrong shape with a `FokosExpressionError`, before the
  coordinator stores the transaction. Each other broken tree, for example one with an operator that does not
  exist, gets a coordinator row and a prepare. The partition refuses it, and the coordinator cancels with no
  retry. Only a caller that bypasses the client can cause this, because the client validates.
- **The coordinator does not refuse a plan.** It stores the request, and the first prepare cancels the
  transaction with `compiled_plan_refused`.
- **The SQL expression depth has no compile check.** A long `and` or `or` compiles, and SQLite refuses it
  ("Expression tree is too large (maximum depth 100)"). `docs/agent-plans/2026-10-10-expression-compile-cost.md`
  covers it.
- **The young generation size of workerd is not measured**, so the GC numbers of section 4.2.8 are for Node.

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
The client validation covers the early failure for each error except `sql_limit`.

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

1. *An expression that is not valid fails only after I/O.* The client validation keeps the failure before
   I/O. Only a valid tree above a limit of the compiled SQL fails after I/O: a transaction with such a tree
   makes a coordinator row, a prepare, and a cancel.
2. *A retry, a recovery drive, and a forward compile again.* Yes. Section 4.2.7 gives the cost.
3. *The coordinator needs the identity, and the version check needs the columns.* The coordinator computes the
   identity from the tree. The version check gets `requiredColumns` from validation.
4. *The changes are the same for a tree and for a plan.* The changes are the same. The stored format and the
   trust model are not.

**How much GC work does the compile in the partition add?**
In Node, with the default young generation, the compile adds up to 28 µs of GC time for each call, and no
old-generation collection runs. The GC time grows less than the CPU time, because the compile garbage dies before
the next scavenge. Section 4.2.8 lists the young generation of workerd as `TODO: measure`.

**Why does the partition not trust the client validation?**
The partition must compile to get SQL, and the compile validates. The client runs on another package version,
and a caller can bypass it.

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
