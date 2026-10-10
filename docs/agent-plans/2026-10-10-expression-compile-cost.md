# RFC — Lower the memory cost and the CPU cost of the expression compile

**State:** Draft
**Date:** 2026-10-10
**Author:** Lambros
**Status:** The benchmark suite of section 4.2.1 is built. No change to the compiler is built.

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

`docs/agent-plans/2026-10-10-expression-trees-over-rpc.md` moves the expression compile from the Worker into
the partition. After that change, each partition compiles each condition, update, projection, and query filter
that it runs. A partition is one isolate with a memory limit of 128 MB, and it serves all its requests on one
thread. Thus the cost of one compile becomes a cost of each partition request.

The benchmark suite of section 4.2.1 gives the cost of the compiler today:

1. **One compile allocates 50 to 230 times the bytes of its expression tree.** A condition with four terms is
   320 bytes as a tree. Its partition path allocates 21.5 KiB. The partition path of a filter with 40 terms and
   48 projections allocates 883 KiB.
2. **A part of the allocation produces no SQL.** A profile of the compiler finds four causes that produce no part
   of the SQL: a regular expression scan, a closure set for each function node, a key string for each binding
   lookup, and a canonical identity that the partition does not read. Section 1.2 has the shares.
3. **The remaining allocation is in proportion to the SQL size, and the SQL is longer than necessary.** One
   comparison of a data path with a literal is 539 characters of SQL. 198 characters give the same result.
4. **SQLite pays for the SQL size a second time.** In a `PartitionDO`, the first `sql.exec` of a statement text
   that reads one item costs 5 to 18 times the JavaScript work of the partition path. A query pays for the SQL
   size a third time, for each item that it scans.

Appendix 8.1 has the baseline for each case.

### 1.2 Where the allocation goes

A sampling heap profile of `compileConditionExpression`, `compileUpdateExpression` and `compileQueryExpression`
in Node v24.20.0 gives these shares of the allocated bytes:

| Cause | Function | Share |
| --- | --- | ---: |
| `matchAll` makes one match object for each `?N` in the SQL | `compactPlanParameters`, `compactPoolParameters` | 21% to 29% |
| `join` copies the text of each term, at each level of the tree | `compileComparison`, `compileCondition` | 7% to 20% |
| The canonical identity text | `canonicalConditionIdentity`, `canonicalUpdateIdentity` | 5% to 11% |
| Four closures and one object for each function node | `makeRenderers` | 10% (updates with functions) |
| One descriptor object and one `JSON.stringify` key for each binding lookup | `bindDescriptor`, `bindPath` | 2% to 6% |
| A flat copy of the type SQL for each constant test | `constTypeName` (`startsWith`) | 5% (conditions) |

The rest is the SQL text and its parts.

### 1.3 What the reader must know about the current system

- **The compiler.** `packages/fokosdb/src/shared/expression/compiler.ts` has four entry points:
  `compileConditionExpression`, `compileUpdateExpression`, `compileProjectionExpression` and
  `compileQueryExpression`. Each one validates the tree, renders SQL fragments, removes the bindings that the SQL
  does not use, checks the SQL limits, and computes the canonical identity.
- **The renderers.** `renderValue`, `renderType` and `renderPresent` each return a SQL string for one value. An
  operation of `operation-registry.ts` renders its arguments through the `OperationRenderers` callbacks.
- **Constant folding by text.** A renderer returns `'text'`, `1` or `0` when the answer is a constant. The
  caller reads the returned string to drop a term or a whole branch.
- **Binding layouts.** A condition and an update bind each descriptor as the numbered parameter `?N` (the
  "direct" layout). A projection and a query bind one JSON array as `?1`, and each fragment reads its element
  with `json_extract(?1, '$[i]')` (the "pool" layout).
- **Parameter renumbering.** Workers SQLite needs the bound value count to equal the parameter count of the
  statement. Constant folding can drop a fragment after its binding was registered. Thus
  `compactPlanParameters` finds the bindings that the SQL uses, and it renumbers them when one is not used.
- **The runtime.** `runtime.ts` checks a plan again (`validateConditionPlan`, `validateUpdatePlan`,
  `validateProjectionPlan`, `validateQueryPlan`), because a plan arrives over RPC today. Each check composes the
  full statement a second time and measures it a second time.

### 1.4 Glossary

- **Partition path** — all the JavaScript work for one expression before `sql.exec`: the compile, the plan
  check, the statement composition, and the bound values. `prepareCase` in
  `packages/fokosdb/test/expression-bench/expression-cases.ts` is its definition for the benchmarks.
- **SQLite first** — one `sql.exec` of a statement text that SQLite did not see before. It includes the prepare.
- **SQLite again** — one `sql.exec` of a statement text that SQLite saw before.
- **Step** — one group of changes in this RFC: step A, step B, or step C.

---

## 2. Goals and Requirements

### 2.1 In scope

1. A benchmark suite measures the compile, the partition path, and the SQLite statement inside a `PartitionDO`,
   and the heap bytes of the same cases in Node.
2. Step A: the compiler makes the same plans with less allocation and less CPU time.
3. Step B: the compiler makes shorter SQL that gives the same result for each item.
4. Step C: the partition path has no canonical identity, no second plan check, and no encode buffer.

### 2.2 Out of scope

- **A plan cache in the partition.** Section 4.3.4 of `2026-10-10-expression-trees-over-rpc.md` records it as an
  investigation.
- **Shorter SQL for updates.** Section 4.3.2 records what the update SQL repeats. It needs its own design.
- **A different way to build the SQL text.** Section 5.1 gives the reason.
- **A fix for the expression depth limit of Workers SQLite.** Section 4.3.1 records the defect. The fix does not
  lower the compile cost, and it needs its own change.

### 2.3 Requirements

- **Step A and step C must not change the SQL.** Each plan must be equal to the plan before the step, byte for
  byte, apart from the fields that step C removes.
- **Step B must not change a result.** For each expression and each item, the statement must return the rows
  that the statement of today returns.
- **A change must not make an expression invalid.** An expression that compiles today must compile after each
  step. Step B can make an expression valid that fails the SQL size limit today.
- **The `OperationDefinition` contract must stay the same.** An operation in `operation-registry.ts` must need
  no change, and a new operation must need no knowledge of a step.
- **The benchmark suite must run with no change to a production module.** It must not add a test hook.

---

## 3. Milestones

1. **The benchmark suite.** Built. It gives the baseline of appendix 8.1.
2. **Step A.** It changes `compiler.ts` only. It ships alone, and it does not depend on another RFC.
3. **Step B.** It changes `compiler.ts` and the tests that compare SQL text. It does not depend on another RFC.
4. **Step C.** It needs milestone 3 of `2026-10-10-expression-trees-over-rpc.md`, after which no plan arrives
   over RPC and no stored row holds a plan.

Step B comes before step C because step B has no dependency and gives the largest gain.

---

## 4. Proposed Solution

### 4.1 High-level overview

The compiler keeps its design: it renders SQL strings from the expression tree. Three steps lower its cost.

```
expression tree
   │ validate
   │ render SQL fragments ── step A: one renderer set for each compile, binding lookup with no key string
   │                         step B: one json_type test for a data path against a literal
   │ find the bindings that the SQL uses ── step A: a scan with no regular expression
   │ check the SQL limits ── step C: no encode buffer for ASCII text
   │ canonical identity ── step C: removed from the plan
   ▼
plan ──▶ plan check and second composition ── step C: removed ──▶ sql.exec
```

- **Step A** removes allocation that produces no SQL. The plans stay equal, byte for byte.
- **Step B** makes the SQL shorter. Less SQL text lowers the compile allocation, the SQLite prepare time, and
  the SQLite time for each scanned item.
- **Step C** removes the work that exists only because a plan is a wire format today.

The result for the partition path, with all three steps. The heap column is Node v24.20.0, and the time column is
a `PartitionDO` in workerd. Appendix 8.2 has the method.

| Case | Heap KiB today | A | A+B | A+B+C | Change | Path µs today | A | A+B | A+B+C | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| cond: not_exists(hashKey) | 3.4 | 3.6 | 3.6 | 3.3 | -3% | 1.5 | 1.8 | 1.7 | 1.3 | -13% |
| cond: optimistic lock | 6.6 | 6.7 | 6.8 | 5.9 | -11% | 3.5 | 3.2 | 3.1 | 2.6 | -26% |
| cond: one path eq | 8.4 | 8.0 | 6.2 | 5.4 | -36% | 4.3 | 4.0 | 3.4 | 2.7 | -37% |
| cond: contains on an array path | 10.7 | 9.3 | 9.3 | 8.8 | -18% | 5.8 | 4.2 | 4.1 | 3.9 | -33% |
| cond: four terms | 22.0 | 18.3 | 12.9 | 10.7 | -51% | 10.6 | 8.2 | 7.5 | 5.9 | -44% |
| cond: nested access policy | 28.7 | 24.5 | 20.8 | 18.5 | -36% | 12.4 | 9.9 | 8.8 | 7.0 | -44% |
| cond: 40 distinct path eq | 233 | 180 | 104 | 82.3 | -65% | 113 | 77.1 | 53.7 | 39.1 | -65% |
| cond: 80 eq on one path | 611 | 513 | 187 | 142 | -77% | 219 | 142 | 91.3 | 77.6 | -65% |
| upd: set 1 literal | 8.9 | 7.9 | 7.9 | 7.5 | -16% | 4.4 | 3.4 | 3.4 | 2.7 | -39% |
| upd: remove 1 path | 7.3 | 7.1 | 7.1 | 6.5 | -11% | 3.5 | 3.0 | 2.9 | 2.2 | -37% |
| upd: counter and timestamp | 25.8 | 17.7 | 17.7 | 15.7 | -39% | 11.9 | 8.1 | 8.4 | 6.8 | -43% |
| upd: 20 actions with arithmetic | 333 | 196 | 196 | 171 | -49% | 191 | 114 | 112 | 106 | -44% |
| upd: 32 literal sets | 126 | 81.4 | 81.3 | 60.7 | -52% | 75.2 | 55.2 | 52.7 | 47.6 | -37% |
| proj: 1 path | 9.9 | 9.1 | 8.2 | 7.9 | -20% | 5.1 | 4.0 | 3.5 | 3.1 | -39% |
| proj: 3 paths and v | 21.8 | 18.0 | 15.4 | 14.3 | -34% | 9.8 | 6.8 | 6.0 | 5.4 | -45% |
| proj: 48 paths | 283 | 217 | 173 | 159 | -44% | 124 | 93.3 | 69.3 | 59.1 | -52% |
| query: one path eq filter | 15.1 | 14.4 | 12.8 | 9.9 | -34% | 7.3 | 6.2 | 5.6 | 4.4 | -40% |
| query: four-term filter and 5 projections | 59.6 | 49.8 | 39.4 | 30.3 | -49% | 25.4 | 18.7 | 15.7 | 11.5 | -55% |
| query: 40-term filter and 48 projections | 883 | 755 | 509 | 318 | -64% | 258 | 211 | 163 | 122 | -53% |

A typical expression (four terms, a counter update, a query with five projections) needs 39% to 51% less heap
and 43% to 55% less CPU time. The smallest conditions change by 3% to 11% in heap.

### 4.2 Technical details

#### 4.2.1 The benchmark suite

The suite is in `packages/fokosdb/test/expression-bench/`:

| File | What it does | Command |
| --- | --- | --- |
| `expression-cases.ts` | The 19 cases, the benchmark items, and `prepareCase` (the partition path). | — |
| `expression.workerd-bench.ts` | Times each case inside a `PartitionDO` in workerd. | `pnpm --filter fokosdb bench:workerd expression` |
| `expression-alloc.mjs` | Counts the heap bytes of each case in Node. | `pnpm --filter fokosdb bench:alloc:expression` |

Rules:

- **The timed work runs inside a `PartitionDO`.** The benchmark makes a partition with `makeStub`, writes 200
  JSON items through `apiPutItem`, and then runs all the timed work in `runInDurableObject`. The statements run
  on the `items` table of that partition, through `state.storage.sql`.
- **The columns.** Tree bytes, statement characters, binding count, compile µs, partition path µs, SQLite first
  µs, and SQLite again µs. The first three do not depend on the machine, so a change in them is always a change
  in the compiler.
- **SQLite first.** Each call adds a different SQL comment to the statement, so that SQLite prepares the
  statement each time.
- **A query case scans 200 items.** A condition, an update probe, and a projection read one item. The update
  case runs `composeUpdateProbeStatement`, which writes nothing.
- **A case name is a stable identifier.** The tables of this RFC refer to the names. A new case goes at the end
  of its group, and the tree of an existing case does not change.
- **The heap count runs in Node.** workerd gives no heap statistics, and both use V8. The script accepts a count
  only when no garbage collection ran in it. It waits for the sweep of the buffers outside the heap before each
  count, because a sweep in the middle of a count makes the count too low.
- **`pnpm test` does not run the suite.** The file name `*.workerd-bench.ts` keeps it out of the test run.

The timer of workerd advances in whole milliseconds. Each sample is a batch of at least 100 ms, and the result
is the median of 5 samples. Two runs of one case differ by about 10%.

#### 4.2.2 Step A: scan for the used bindings with no regular expression

**Today.** `compactPlanParameters` and `compactPoolParameters` call `matchAll` on each SQL fragment. `matchAll`
makes one match array and one substring for each `?N` or `?1, '$[N]'`. When a binding is not used, a `replace`
with the same pattern renumbers the rest.

**Change.** One function replaces the two. It finds each marker with `indexOf`, reads the digits with
`charCodeAt`, and sets a flag in a `Uint8Array` that has one entry for each binding. The marker is `?` for the
direct layout and `?P, '$[` for the pool layout. When the count of set flags equals the binding count, the
function returns the fragments as they are. The renumbering keeps the `replace` call.

**Why the scan must stay.** In 17,194 random expressions that compile, no expression reached the renumbering.
The scan is still necessary: an operation that folds a rendered fragment away leaves a binding that no SQL uses,
and the statement then fails in Workers SQLite.

**Evidence.** Partition path, Node v24.20.0, today against a prototype with this change only:

| Case | Heap KiB today | With the scan | Change | µs today | With the scan | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| cond: not_exists(hashKey) | 3.4 | 3.2 | -6% | 3.9 | 3.2 | -18% |
| cond: optimistic lock | 6.6 | 6.3 | -5% | 6.9 | 5.9 | -14% |
| cond: one path eq | 8.4 | 7.6 | -10% | 4.3 | 3.7 | -14% |
| cond: contains on an array path | 10.7 | 9.1 | -15% | 5.8 | 4.2 | -28% |
| cond: four terms | 22.0 | 19.2 | -13% | 10.5 | 8.9 | -15% |
| cond: nested access policy | 28.7 | 25.6 | -11% | 12.2 | 10.1 | -17% |
| cond: 40 distinct path eq | 233 | 197 | -15% | 105 | 92.7 | -11% |
| cond: 80 eq on one path | 611 | 555 | -9% | 252 | 223 | -12% |
| upd: set 1 literal | 8.9 | 7.4 | -17% | 3.9 | 3.0 | -23% |
| upd: remove 1 path | 7.3 | 6.4 | -12% | 2.9 | 2.3 | -21% |
| upd: counter and timestamp | 25.8 | 21.3 | -17% | 11.1 | 9.5 | -14% |
| upd: 20 actions with arithmetic | 333 | 265 | -20% | 148 | 112 | -25% |
| upd: 32 literal sets | 126 | 90.0 | -29% | 58.4 | 44.8 | -23% |
| proj: 1 path | 9.9 | 9.0 | -9% | 5.6 | 4.0 | -29% |
| proj: 3 paths and v | 21.8 | 18.8 | -14% | 9.6 | 7.6 | -21% |
| proj: 48 paths | 283 | 256 | -10% | 127 | 102 | -19% |
| query: one path eq filter | 15.1 | 14.2 | -6% | 6.9 | 6.3 | -9% |
| query: four-term filter and 5 projections | 59.6 | 52.5 | -12% | 26.5 | 22.4 | -15% |
| query: 40-term filter and 48 projections | 883 | 755 | -14% | 300 | 264 | -12% |

#### 4.2.3 Step A: one renderer set for each compile, and a binding lookup with no key string

**Today.**

- `makeRenderers` makes four closures and one object each time `renderValue`, `renderType` or `renderPresent`
  reaches a function node.
- `bindDescriptor` gets a new descriptor object from its caller, and it makes the key
  `${kind}:${JSON.stringify(value)}` to find the binding. Both happen also when the value is already bound. One
  comparison of a data path binds the same path three times.

**Change.**

- The compile context holds one `OperationRenderers` set. One `createContext` function makes the context for the
  four entry points, in place of four object literals. The context makes the set at the first function node,
  so that an expression with no function pays nothing.
- `bindDescriptor` takes the kind and the value. The context holds one `Map` for each descriptor kind, from the
  value to the binding index. `bindDescriptor` makes a descriptor only for a value that is not bound.

**Why the lookup is the same.** A `Map` compares keys with SameValueZero. It keeps `1` and `"1"` apart, and
`null`, `true` and `false` apart. `bindLiteral` already changes `-0` to `0`, and `validateScalarLiteral` refuses
`NaN`. Thus two values share a binding in the new lookup exactly when they share one today.

**Evidence.** Partition path, Node v24.20.0, the prototype of section 4.2.2 against a prototype that adds both
changes:

| Case | Heap KiB before | After | Change | µs before | After | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| cond: not_exists(hashKey) | 3.2 | 3.6 | +13% | 3.2 | 3.1 | -3% |
| cond: optimistic lock | 6.3 | 6.7 | +6% | 5.9 | 5.7 | -3% |
| cond: one path eq | 7.6 | 8.0 | +5% | 3.7 | 4.0 | +8% |
| cond: contains on an array path | 9.1 | 9.3 | +2% | 4.2 | 4.1 | -2% |
| cond: four terms | 19.2 | 18.3 | -5% | 8.9 | 6.7 | -25% |
| cond: nested access policy | 25.6 | 24.5 | -4% | 10.1 | 8.3 | -18% |
| cond: 40 distinct path eq | 197 | 180 | -9% | 92.7 | 62.7 | -32% |
| cond: 80 eq on one path | 555 | 513 | -8% | 223 | 168 | -25% |
| upd: set 1 literal | 7.4 | 7.9 | +7% | 3.0 | 3.2 | +7% |
| upd: remove 1 path | 6.4 | 7.1 | +11% | 2.3 | 2.5 | +9% |
| upd: counter and timestamp | 21.3 | 17.7 | -17% | 9.5 | 7.0 | -26% |
| upd: 20 actions with arithmetic | 265 | 196 | -26% | 112 | 84.9 | -24% |
| upd: 32 literal sets | 90.0 | 81.4 | -10% | 44.8 | 35.3 | -21% |
| proj: 1 path | 9.0 | 9.1 | +1% | 4.0 | 5.8 | +45% |
| proj: 3 paths and v | 18.8 | 18.0 | -4% | 7.6 | 6.5 | -14% |
| proj: 48 paths | 256 | 217 | -15% | 102 | 75.4 | -26% |
| query: one path eq filter | 14.2 | 14.4 | +1% | 6.3 | 5.6 | -11% |
| query: four-term filter and 5 projections | 52.5 | 49.8 | -5% | 22.4 | 17.9 | -20% |
| query: 40-term filter and 48 projections | 755 | 755 | 0% | 264 | 204 | -23% |

The prototype makes the renderer set when it makes the context. That costs 0.2 to 0.7 KiB for an expression with
no function, which is the increase in the smallest cases. The set at the first function node removes that part
of the increase. `TODO: measure` the smallest cases after that change.

All of step A, today against the prototype with the three changes. The time column is a `PartitionDO` in workerd:

| Case | Heap KiB today | Step A | Change | Path µs today | Step A | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| cond: not_exists(hashKey) | 3.4 | 3.6 | +6% | 1.5 | 1.8 | +20% |
| cond: optimistic lock | 6.6 | 6.7 | +2% | 3.5 | 3.2 | -9% |
| cond: one path eq | 8.4 | 8.0 | -5% | 4.3 | 4.0 | -7% |
| cond: contains on an array path | 10.7 | 9.3 | -13% | 5.8 | 4.2 | -28% |
| cond: four terms | 22.0 | 18.3 | -17% | 10.6 | 8.2 | -23% |
| cond: nested access policy | 28.7 | 24.5 | -15% | 12.4 | 9.9 | -20% |
| cond: 40 distinct path eq | 233 | 180 | -23% | 113 | 77.1 | -32% |
| cond: 80 eq on one path | 611 | 513 | -16% | 219 | 142 | -35% |
| upd: set 1 literal | 8.9 | 7.9 | -11% | 4.4 | 3.4 | -23% |
| upd: remove 1 path | 7.3 | 7.1 | -3% | 3.5 | 3.0 | -14% |
| upd: counter and timestamp | 25.8 | 17.7 | -31% | 11.9 | 8.1 | -32% |
| upd: 20 actions with arithmetic | 333 | 196 | -41% | 191 | 114 | -40% |
| upd: 32 literal sets | 126 | 81.4 | -36% | 75.2 | 55.2 | -27% |
| proj: 1 path | 9.9 | 9.1 | -8% | 5.1 | 4.0 | -22% |
| proj: 3 paths and v | 21.8 | 18.0 | -17% | 9.8 | 6.8 | -31% |
| proj: 48 paths | 283 | 217 | -23% | 124 | 93.3 | -25% |
| query: one path eq filter | 15.1 | 14.4 | -5% | 7.3 | 6.2 | -15% |
| query: four-term filter and 5 projections | 59.6 | 49.8 | -16% | 25.4 | 18.7 | -26% |
| query: 40-term filter and 48 projections | 883 | 755 | -14% | 258 | 211 | -18% |

#### 4.2.4 Step B: shorter SQL

**1. A data path against a literal.** Today `compileComparison` renders three terms for
`{ op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "pending" }] }`: a presence test, a type test
through a `CASE` with nine arms, and the value test. The three terms read the path three times:

```sql
(CASE WHEN (i.hk IS NOT NULL AND i.data_kind = 2) THEN json_type(i.data, ?3) IS NOT NULL ELSE 0 END
 AND CASE WHEN (i.hk IS NOT NULL AND i.data_kind = 2) THEN CASE json_type(i.data, ?3) WHEN 'null' THEN 'null'
   WHEN 'true' THEN 'boolean' WHEN 'false' THEN 'boolean' WHEN 'integer' THEN 'number' WHEN 'real' THEN 'number'
   WHEN 'text' THEN 'text' WHEN 'array' THEN 'array' WHEN 'object' THEN 'object' ELSE 'missing' END
   ELSE 'missing' END = 'text'
 AND (CASE WHEN (i.hk IS NOT NULL AND i.data_kind = 2) THEN json_extract(i.data, ?3) END IS ?4))
```

When one side is a data path and the other side is a literal, the type of the literal is known at compile time.
One `json_type` test then covers the presence and the type, and the statement reads the path two times:

```sql
(CASE WHEN (i.hk IS NOT NULL AND i.data_kind = 2) AND json_type(i.data, ?3) = 'text' THEN 1 ELSE 0 END
 AND (CASE WHEN (i.hk IS NOT NULL AND i.data_kind = 2) THEN json_extract(i.data, ?3) END IS ?4))
```

The test for each literal type:

| Literal type | Test on `json_type(data, path)` |
| --- | --- |
| `null` | `= 'null'` |
| `boolean` | `IN ('true', 'false')` |
| `number` | `IN ('integer', 'real')` |
| `text` | `= 'text'` |

Rules:

- **The guard is never NULL.** `json_type` returns NULL for an absent path. The guard is
  `CASE WHEN ... THEN 1 ELSE 0 END`, and a `CASE WHEN` reads NULL as false. Thus the guard is 0 for an absent
  path, and a `not` around the comparison stays correct.
- **Where it applies.** `compileComparison` (`eq`, `ne`, `lt`, `lte`, `gt`, `gte`, and thus `between`),
  `compileBeginsWith`, and the branch of `compileIn` where all choices have one literal type.
- **Where it does not apply.** A comparison of two paths, of a path with a function, or of a key reference keeps
  the SQL of today. `compileContains` keeps the SQL of today.
- **One function holds the rule.** A new function returns the guard for a value and a type name, or nothing when
  the value is not a data path. `pushTypeGuards` stays as the general path. No operation definition changes.

**2. The type column of a projected reference.** `renderProjectionType` wraps the type in
`CASE WHEN (present) THEN type ELSE 'missing' END`. For a reference, `referenceType` already returns `'missing'`
when the reference is absent. The change returns the type of a reference with no wrapper. That removes one path
read for each projected path, for each item.

**Evidence, the JavaScript side.** Step A against a prototype with step A and step B. The statement length does
not depend on the machine. The time column is a `PartitionDO` in workerd:

| Case | SQL B today | Step B | Change | Heap KiB step A | Step A+B | Change | Path µs step A | Step A+B | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| cond: not_exists(hashKey) | 330 | 330 | 0% | 3.6 | 3.6 | 0% | 1.8 | 1.7 | -6% |
| cond: optimistic lock | 461 | 461 | 0% | 6.7 | 6.8 | +1% | 3.2 | 3.1 | -3% |
| cond: one path eq | 845 | 504 | -40% | 8.0 | 6.2 | -22% | 4.0 | 3.4 | -15% |
| cond: contains on an array path | 1,676 | 1,676 | 0% | 9.3 | 9.3 | 0% | 4.2 | 4.1 | -2% |
| cond: four terms | 2,066 | 1,055 | -49% | 18.3 | 12.9 | -30% | 8.2 | 7.5 | -9% |
| cond: nested access policy | 3,067 | 2,385 | -22% | 24.5 | 20.8 | -15% | 9.9 | 8.8 | -11% |
| cond: 40 distinct path eq | 22,208 | 8,532 | -62% | 180 | 104 | -42% | 77.1 | 53.7 | -30% |
| cond: 80 eq on one path | 43,818 | 16,538 | -62% | 513 | 187 | -64% | 142 | 91.3 | -36% |
| upd: set 1 literal | 958 | 958 | 0% | 7.9 | 7.9 | 0% | 3.4 | 3.4 | 0% |
| upd: remove 1 path | 831 | 831 | 0% | 7.1 | 7.1 | 0% | 3.0 | 2.9 | -3% |
| upd: counter and timestamp | 3,753 | 3,753 | 0% | 17.7 | 17.7 | 0% | 8.1 | 8.4 | +4% |
| upd: 20 actions with arithmetic | 56,071 | 56,071 | 0% | 196 | 196 | 0% | 114 | 112 | -2% |
| upd: 32 literal sets | 6,804 | 6,804 | 0% | 81.4 | 81.3 | 0% | 55.2 | 52.7 | -5% |
| proj: 1 path | 718 | 561 | -22% | 9.1 | 8.2 | -10% | 4.0 | 3.5 | -12% |
| proj: 3 paths and v | 2,109 | 1,583 | -25% | 18.0 | 15.4 | -14% | 6.8 | 6.0 | -12% |
| proj: 48 paths | 30,471 | 22,897 | -25% | 217 | 173 | -21% | 93.3 | 69.3 | -26% |
| query: one path eq filter | 1,342 | 979 | -27% | 14.4 | 12.8 | -11% | 6.2 | 5.6 | -10% |
| query: four-term filter and 5 projections | 5,260 | 3,500 | -33% | 49.8 | 39.4 | -21% | 18.7 | 15.7 | -16% |
| query: 40-term filter and 48 projections | 58,796 | 36,629 | -38% | 755 | 509 | -33% | 211 | 163 | -23% |

**Evidence, the SQLite side.** The statement of today against the statement of step B, in a `PartitionDO` with
200 items, µs for each `sql.exec`:

| Case | SQLite first today | Step B | Change | SQLite again today | Step B | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| cond: not_exists(hashKey) | 15.5 | 15.5 | 0% | 3.6 | 3.8 | +6% |
| cond: optimistic lock | 19.7 | 19.0 | -4% | 3.8 | 4.0 | +5% |
| cond: one path eq | 33.4 | 24.7 | -26% | 5.0 | 4.8 | -4% |
| cond: contains on an array path | 63.0 | 57.1 | -9% | 6.5 | 6.0 | -8% |
| cond: four terms | 64.9 | 43.7 | -33% | 7.9 | 7.1 | -10% |
| cond: nested access policy | 91.3 | 73.2 | -20% | 6.8 | 6.5 | -4% |
| cond: 40 distinct path eq | 1,457 | 277 | -81% | 65.4 | 50.3 | -23% |
| cond: 80 eq on one path | 2,359 | 1,082 | -54% | 74.7 | 51.3 | -31% |
| upd: set 1 literal | 43.9 | 39.6 | -10% | 6.5 | 6.5 | 0% |
| upd: remove 1 path | 37.6 | 35.2 | -6% | 6.2 | 6.4 | +3% |
| upd: counter and timestamp | 131 | 143 | +9% | 12.0 | 11.7 | -3% |
| upd: 20 actions with arithmetic | 2,906 | 2,297 | -21% | 135 | 151 | +12% |
| upd: 32 literal sets | 367 | 365 | -1% | 88.9 | 91.3 | +3% |
| proj: 1 path | 27.1 | 22.5 | -17% | 4.9 | 4.6 | -6% |
| proj: 3 paths and v | 61.5 | 48.3 | -21% | 7.8 | 7.5 | -4% |
| proj: 48 paths | 2,063 | 1,629 | -21% | 102 | 89.8 | -12% |
| query: one path eq filter | 828 | 766 | -8% | 856 | 797 | -7% |
| query: four-term filter and 5 projections | 1,234 | 1,000 | -19% | 1,047 | 883 | -16% |
| query: 40-term filter and 48 projections | 25,500 | 20,125 | -21% | 24,000 | 18,500 | -23% |

The condition cases show the first change, and the projection cases show the second one. The query cases show
both: a page of 200 items is 7% to 23% faster when the filter compares paths with literals. The update cases
get no shorter SQL, and their differences are noise.

`cond: 80 eq on one path` gets a statement of 16,538 characters. The size check then answers from the length,
so this case also loses the encode buffer of section 4.2.5 in this step.

#### 4.2.5 Step C: a plan that stays in the partition

After milestone 3 of `2026-10-10-expression-trees-over-rpc.md`, a plan is made and used in one partition
request. Three parts of the partition path then have no reader.

**1. The canonical identity.** Each compile ends with `canonicalConditionIdentity`, `canonicalUpdateIdentity` or
`canonicalProjectionIdentity`. The readers of the identity are `hashOperation` in the coordinator and
`computeCursorFingerprint` in the client. Section 4.2.2 and section 4.2.4 of the expression trees RFC make both
call the identity functions on the tree. The plan types then lose `identity`, `filterIdentity` and
`projectionIdentity`. The Identity column of appendix 8.1 is the saving: 0.3 to 43.9 KiB. The identity does not
depend on the SQL, so step B does not change it.

**2. The second plan check.** `validateConditionPlan`, `validateProjectionPlan` and `validateQueryPlan` compose
the statement and measure it, after the compiler did the same. `validateUpdatePlan` measures three fragments
again. The change:

- `compileConditionExpression` and `compileProjectionExpression` return the statement that they composed for the
  size check. `evaluateConditionPlan` and `readProjectedItem` run that statement.
- The four `validate*Plan` functions go away with the plan path of the partition.
- A query still composes two statements: the widest one for the size check, and the one that it runs.
  Section 4.3.3 records the option to remove one.

**3. The encode buffer.** `utf8WithinLimit` answers from the length when `text.length * 3 <= limit`. For the SQL
limit of 100,000 bytes, a text of 33,334 to 100,000 characters goes to `TextEncoder.encode`, which makes a buffer
of the size of the text. The compiled SQL is ASCII, because every caller value is bound. The change adds one
test before the encode: when `/^[\x00-\x7f]*$/` matches the text, the byte count equals the length. The function
stays correct for text that is not ASCII.

**Evidence.** Partition path, step A+B against a prototype with all three steps. The time column is a
`PartitionDO` in workerd:

| Case | Heap KiB step A+B | Step A+B+C | Change | Path µs step A+B | Step A+B+C | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| cond: not_exists(hashKey) | 3.6 | 3.3 | -8% | 1.7 | 1.3 | -24% |
| cond: optimistic lock | 6.8 | 5.9 | -13% | 3.1 | 2.6 | -16% |
| cond: one path eq | 6.2 | 5.4 | -13% | 3.4 | 2.7 | -21% |
| cond: contains on an array path | 9.3 | 8.8 | -5% | 4.1 | 3.9 | -5% |
| cond: four terms | 12.9 | 10.7 | -17% | 7.5 | 5.9 | -21% |
| cond: nested access policy | 20.8 | 18.5 | -11% | 8.8 | 7.0 | -20% |
| cond: 40 distinct path eq | 104 | 82.3 | -21% | 53.7 | 39.1 | -27% |
| cond: 80 eq on one path | 187 | 142 | -24% | 91.3 | 77.6 | -15% |
| upd: set 1 literal | 7.9 | 7.5 | -5% | 3.4 | 2.7 | -21% |
| upd: remove 1 path | 7.1 | 6.5 | -8% | 2.9 | 2.2 | -24% |
| upd: counter and timestamp | 17.7 | 15.7 | -11% | 8.4 | 6.8 | -19% |
| upd: 20 actions with arithmetic | 196 | 171 | -13% | 112 | 106 | -5% |
| upd: 32 literal sets | 81.3 | 60.7 | -25% | 52.7 | 47.6 | -10% |
| proj: 1 path | 8.2 | 7.9 | -4% | 3.5 | 3.1 | -11% |
| proj: 3 paths and v | 15.4 | 14.3 | -7% | 6.0 | 5.4 | -10% |
| proj: 48 paths | 173 | 159 | -8% | 69.3 | 59.1 | -15% |
| query: one path eq filter | 12.8 | 9.9 | -23% | 5.6 | 4.4 | -21% |
| query: four-term filter and 5 projections | 39.4 | 30.3 | -23% | 15.7 | 11.5 | -27% |
| query: 40-term filter and 48 projections | 509 | 318 | -37% | 163 | 122 | -25% |

After step B, the size check of `query: 40-term filter and 48 projections` still encodes a text of more than
33,334 characters: one time in the compile and one time in `validateQueryPlan`. The case loses both buffers,
which is a part of its 37%.

#### 4.2.6 Extensibility

- No step changes `OperationDefinition`, `OperationRenderers`, or an entry of `OPERATION_REGISTRY`.
- A new operation renders through the same callbacks. It gets the general guards of `pushTypeGuards`, and it
  needs no knowledge of the guard of step B.
- A new binding kind needs one entry in `ExpressionBindingDescriptor`, as it does today. The lookup of
  section 4.2.3 makes its `Map` on first use.
- A new literal type needs one row in the table of section 4.2.4. Without the row, the comparison uses the
  general guards.

#### 4.2.7 Deployment and rollback

- **Step A** changes no output. It needs no order of deployment.
- **Step B** changes the SQL text of new plans. A plan that the coordinator stored before the change keeps the
  old SQL, and the partition runs both. The client and the partition can run different versions, because both
  SQL texts give the same result.
- **Step C** ships with milestone 3 of the expression trees RFC, and it follows the deployment order of that RFC.
- **Rollback.** Each step is a change of code only. No stored format changes.

#### 4.2.8 Testing

- **Step A and step C.** For each expression, the plan must equal the plan of the compiler before the step,
  apart from the removed fields. A run on the prototypes compared 17,194 random expressions that compile, the
  fixtures of `test-fixtures.ts`, and the benchmark cases: all plans are equal. The same run gave the same
  error for the 12,865 random expressions that do not compile.
- **The renumbering.** No random expression reaches it. A direct comparison of the old function and the new
  function on 20,000 made-up SQL fragments, of which 14,727 renumber, gave equal results. The implementation
  must keep a unit test that calls the function with a binding that no fragment uses, for both layouts.
- **Step B.** A run on the prototype executed the SQL of today and the shorter SQL on SQLite 3.53.4
  (`node:sqlite`), for 17,194 conditions, projections and queries over 17 items: absent, bytes, text, and
  JSON items with each value type at each path. All 292,298 results are equal. A defect that the run got on
  purpose (`number` tested as `'integer'` only) failed on the first affected expression. In a `PartitionDO`, the
  19 benchmark cases return equal rows for both SQL texts.
- **Before each step merges**, the implementation must repeat its comparison against the compiler of the commit
  before it.
- **SQL text tests.** The tests that compare SQL text change with step B. The tests that compare results must
  pass with no change.
- **The benchmark suite.** Each step must update the tables of appendix 8.1.

### 4.3 Open Questions

#### 4.3.1 Workers SQLite refuses an expression deeper than 100

The benchmark suite found a defect in the current system. SQLite parses `a OR b OR c` as one level of depth for
each term. Workers SQLite fails with `Expression tree is too large (maximum depth 100)`. In a `PartitionDO`, an
`and` or an `or` of 93 `eq` terms on `v` runs, and 94 terms fail. The compiler accepts both, because
`EXPRESSION_LIMITS.operatorsAndFunctions` is 300. The case "149 `eq` on one path" in the appendix of the
expression trees RFC compiles, and no partition can run it.

Options:

1. Render a long `and` or `or` as a balanced tree of parenthesized groups. 300 terms then need a depth of 9.
2. Add a compile limit on the count of terms in one `and` or `or`.

The answer decides if the limit of 300 operators stays true for a flat expression.

#### 4.3.2 Shorter SQL for updates

Step B does not change the update SQL. The update SQL repeats text in three ways:

- `applicableSql` holds the full `documentSql` inside `json_type(...)`, and the probe statement holds
  `applicableSql` two times and `documentSql` one more time.
- `renderArithmeticPresent` keeps terms that are constant, such as `1 AND 1` and `'number' = 'number'`.
- Two `set` actions with the same parent path give the same target guard two times.

`upd: 20 actions with arithmetic` is 3,312 bytes as a tree and 56,071 characters as a probe statement. Its first
`sql.exec` costs 2,750 µs. The question: which of the three can go with no change to the result?

#### 4.3.3 One statement composition for a query

`compileQueryExpression` composes the widest statement for the size check. `PartitionStore.scanQueryPage` then
composes the statement that it runs. Options: keep both, or check the size of the statement that runs. The
second option moves the size error from the compile to the first scan of the request, and the client check of
the expression trees RFC has no statement to check.

#### 4.3.4 Validation runs two times in the partition

Section 4.2.3 of the expression trees RFC makes `sequencePlanOf` call `validateConditionExpression` for
`requiredColumns`, and then `#evaluate` compiles, which validates again. A validation allocates 0.4 to 21.9 KiB.
Options: keep both, or let the compile take the analysis of the first validation.

#### 4.3.5 The young generation of workerd

The heap counts are from Node. The cost of a garbage collection depends on the size of the young generation,
which the expression trees RFC records as `TODO: measure` for workerd. This RFC has the same gap.

---

## 5. Alternative Options

### 5.1 Build the SQL in one buffer

Each emit function appends SQL parts to one array, and one `join` makes each SQL text. A prototype of the full
compiler in this form exists outside the repository. It gives the SQL text and the bindings of the prototype
with all three steps, byte for byte, for 17,194 random conditions, projections and queries, and for 32,543
random updates.

What this form changes:

- **Constant folding.** A caller cannot read a returned string. Each operation gets two more members: one
  answers if its presence is a constant, and one answers if its type is a constant. Both render nothing and bind
  nothing. Every entry of `OPERATION_REGISTRY` changes to that contract.
- **No scan.** A binding registers when its text goes into the buffer, and nothing removes text from the
  buffer. Thus every binding is used, and the scan of section 4.2.2 goes away.
- **Five fragments stay strings.** The bindings of these fragments must register before their place in the
  text, to keep the parameter numbers of today: the operands of an arithmetic presence test, the choices of
  `in`, the type of a pass-through argument, the type test of an update value, and the type of a computed
  projection.
- **One buffer for a condition statement.** The condition statement goes out whole. An update, a projection,
  and a query keep their fragments as strings, because each caller composes a different statement from them.

Evidence. Partition path, the string prototype with all three steps against the buffer prototype:

| Case | Heap KiB A+B+C | Buffer | Change | Node µs A+B+C | Buffer | Change | workerd µs A+B+C | Buffer | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| cond: not_exists(hashKey) | 3.3 | 2.1 | -36% | 2.7 | 2.4 | -11% | 1.3 | 1.4 | +8% |
| cond: optimistic lock | 5.9 | 3.8 | -36% | 5.1 | 4.1 | -20% | 2.6 | 2.5 | -4% |
| cond: one path eq | 5.4 | 3.7 | -31% | 2.3 | 2.3 | 0% | 2.7 | 2.5 | -7% |
| cond: contains on an array path | 8.8 | 6.0 | -32% | 3.5 | 3.2 | -9% | 3.9 | 3.4 | -13% |
| cond: four terms | 10.7 | 6.7 | -37% | 4.5 | 4.5 | 0% | 5.9 | 5.0 | -15% |
| cond: nested access policy | 18.5 | 8.9 | -52% | 6.0 | 5.6 | -7% | 7.0 | 5.8 | -17% |
| cond: 40 distinct path eq | 82.3 | 57.6 | -30% | 34.2 | 33.6 | -2% | 39.1 | 34.9 | -11% |
| cond: 80 eq on one path | 142 | 102 | -28% | 62.8 | 58.9 | -6% | 77.6 | 69.3 | -11% |
| upd: set 1 literal | 7.5 | 5.7 | -24% | 2.9 | 2.4 | -17% | 2.7 | 2.5 | -7% |
| upd: remove 1 path | 6.5 | 5.0 | -23% | 2.1 | 2.0 | -5% | 2.2 | 2.2 | 0% |
| upd: counter and timestamp | 15.7 | 12.5 | -20% | 5.6 | 5.5 | -2% | 6.8 | 5.9 | -13% |
| upd: 20 actions with arithmetic | 171 | 142 | -17% | 73.2 | 71.0 | -3% | 106 | 103 | -4% |
| upd: 32 literal sets | 60.7 | 51.3 | -15% | 26.9 | 25.1 | -7% | 47.6 | 44.2 | -7% |
| proj: 1 path | 7.9 | 6.2 | -22% | 3.3 | 2.8 | -15% | 3.1 | 2.8 | -10% |
| proj: 3 paths and v | 14.3 | 12.6 | -12% | 5.1 | 5.0 | -2% | 5.4 | 5.2 | -4% |
| proj: 48 paths | 159 | 146 | -8% | 51.7 | 50.2 | -3% | 59.1 | 61.0 | +3% |
| query: one path eq filter | 9.9 | 7.7 | -22% | 4.7 | 3.8 | -19% | 4.4 | 4.2 | -5% |
| query: four-term filter and 5 projections | 30.3 | 24.0 | -21% | 11.9 | 11.1 | -7% | 11.5 | 11.4 | -1% |
| query: 40-term filter and 48 projections | 318 | 261 | -18% | 111 | 114 | +3% | 122 | 118 | -3% |

- **Heap.** A condition needs 28% to 52% less. An update, a projection, and a query need 8% to 24% less.
- **CPU time.** In workerd, a condition changes by +8% to -17%, and the other cases by +3% to -13%. Most of
  these differences are inside the noise of 10%.
- **SQLite.** No change. The SQL text is the same.

Not chosen for this RFC: the heap gain needs a new contract for every operation, which the requirements of
section 2.3 forbid, and the CPU gain is not clear of the noise. The buffer form stays possible after step C. The
decision then depends on the value of 4.0 KiB for each four-term condition (10.7 KiB against 6.7 KiB).

### 5.2 Keep the rendered SQL of each value node

A map from a value node to its rendered SQL saves a second render of the same node, for example in `between` and
in `renderArithmeticPresent`. Not chosen: a comparison of a path with a literal, which is the common case,
renders no node two times in one mode, so the map only adds allocation there.

### 5.3 Remove the renumbering

No expression in the test run reaches the renumbering. Not chosen: the statement fails in Workers SQLite when a
future operation folds a bound fragment away, and the scan of section 4.2.2 is cheap.

### 5.4 A plan cache in the partition

A cache removes the compile for a repeated expression. Not chosen here: the expression trees RFC records it as
an investigation, and each step of this RFC also lowers the cost of a cache miss.

---

## 6. Frequently Asked Questions

**Why does the SQL size matter more than the JavaScript time?**
The first `sql.exec` of the four-term condition costs 60.1 µs, and its partition path costs 10.3 µs. For 40 path
terms, the numbers are 1,441 µs and 112 µs. SQLite parses and plans each character of the statement.

**Does a partition prepare the statement for each request?**
No. For a statement that reads one item, the second `sql.exec` of the same text is 4 to 40 times faster than the
first one, so workerd keeps prepared statements. A condition and an update bind every caller value, so two
requests with the same expression shape give the same text. `TODO: verify` in the workerd source how many
statements it keeps and when it drops one.

**Can step B change the result of a condition that exists today?**
The requirement is that it cannot, and section 4.2.8 has the evidence. The guard tests the same `json_type` value
that the three terms of today test.

**Why is the heap not measured in workerd?**
workerd gives no heap statistics and no way to start a garbage collection. Node v24 and workerd both use V8, and
the allocation of the compiler depends on V8, not on the host.

**Do the steps change the limits of an expression?**
No limit changes. With step B, an expression that fails `compiledSqlBytes` today can fit.

---

## 7. References

- `docs/agent-plans/2026-10-10-expression-trees-over-rpc.md` — sections 4.2.2, 4.2.3, 4.2.4, 4.2.8 and 4.3.4.
- `docs/agent-plans/2026-08-29-typed-expression-engine-spec.md`
- `docs/agent-plans/2026-09-14-read-projections-and-query-filters.md`
- `packages/fokosdb/src/shared/expression/compiler.ts`, `plan.ts`, `runtime.ts`, `bindings.ts`, `utf8.ts`,
  `operation-registry.ts`
- `packages/fokosdb/test/expression-bench/expression-cases.ts`, `expression.workerd-bench.ts`,
  `expression-alloc.mjs`
- [Limits of SQLite](https://www.sqlite.org/limits.html) — the maximum depth of an expression tree.
- [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)

---

## 8. Appendix

### 8.1 The baseline

`pnpm --filter fokosdb bench:workerd expression`. workerd, a `PartitionDO` with 200 items, µs for each call,
median of 5 batches of at least 100 ms:

| Case | Tree B | SQL B | Binds | Compile | Path | SQLite first | SQLite again |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| cond: not_exists(hashKey) | 46 | 330 | 0 | 0.6 | 1.6 | 15.4 | 4.0 |
| cond: optimistic lock | 107 | 461 | 1 | 2.1 | 3.3 | 18.8 | 4.1 |
| cond: one path eq | 71 | 845 | 2 | 2.9 | 4.2 | 31.0 | 4.9 |
| cond: contains on an array path | 80 | 1676 | 2 | 4.1 | 5.6 | 52.2 | 6.0 |
| cond: four terms | 320 | 2066 | 8 | 8.7 | 10.3 | 60.1 | 8.1 |
| cond: nested access policy | 390 | 3067 | 7 | 10.9 | 12.0 | 85.0 | 7.1 |
| cond: 40 distinct path eq | 2961 | 22208 | 80 | 119.1 | 112.3 | 1441.4 | 69.3 |
| cond: 80 eq on one path | 5850 | 43818 | 81 | 198.2 | 238.3 | 2976.6 | 74.2 |
| upd: set 1 literal | 83 | 958 | 3 | 3.4 | 4.6 | 42.5 | 6.5 |
| upd: remove 1 path | 68 | 831 | 1 | 2.1 | 3.4 | 35.2 | 6.5 |
| upd: counter and timestamp | 260 | 3753 | 6 | 10.2 | 11.4 | 127.0 | 12.1 |
| upd: 20 actions with arithmetic | 3312 | 56071 | 42 | 181.6 | 170.9 | 2750.0 | 133.8 |
| upd: 32 literal sets | 2637 | 6804 | 65 | 73.2 | 73.2 | 359.4 | 90.3 |
| proj: 1 path | 43 | 718 | 1 | 3.3 | 4.9 | 26.6 | 4.9 |
| proj: 3 paths and v | 147 | 2109 | 3 | 7.3 | 9.1 | 63.5 | 8.0 |
| proj: 48 paths | 2391 | 30471 | 48 | 125.0 | 127.0 | 2304.7 | 98.6 |
| query: one path eq filter | 82 | 1342 | 2 | 4.6 | 6.8 | 835.9 | 871.1 |
| query: four-term filter and 5 projections | 533 | 5260 | 9 | 20.1 | 25.1 | 1234.4 | 1109.4 |
| query: 40-term filter and 48 projections | 5377 | 58796 | 128 | 232.4 | 277.3 | 24500.0 | 23750.0 |

`pnpm --filter fokosdb bench:alloc:expression`. Node v24.20.0, heap KiB for each call, mean of 8 to 2,000 calls
after 3,000 warm-up calls:

| Case | Validate | Identity | Compile | Path | Kept |
| --- | ---: | ---: | ---: | ---: | ---: |
| cond: not_exists(hashKey) | 0.5 | 0.3 | 2.3 | 3.4 | 0.7 |
| cond: optimistic lock | 0.8 | 0.9 | 4.9 | 6.2 | 1.0 |
| cond: one path eq | 0.5 | 0.5 | 6.7 | 8.5 | 1.3 |
| cond: contains on an array path | 0.5 | 0.5 | 9.7 | 10.7 | 2.2 |
| cond: four terms | 0.7 | 2.3 | 20.7 | 21.5 | 2.5 |
| cond: nested access policy | 0.5 | 2.8 | 27.9 | 29.1 | 3.5 |
| cond: 40 distinct path eq | 0.5 | 22.0 | 230.4 | 234.8 | 22.9 |
| cond: 80 eq on one path | 0.4 | 43.9 | 554.8 | 611.1 | 43.8 |
| upd: set 1 literal | 1.3 | 0.7 | 7.0 | 8.5 | 1.4 |
| upd: remove 1 path | 1.2 | 0.6 | 5.9 | 7.1 | 1.3 |
| upd: counter and timestamp | 3.1 | 2.0 | 23.5 | 24.9 | 4.2 |
| upd: 20 actions with arithmetic | 21.9 | 25.9 | 330.1 | 333.1 | 55.6 |
| upd: 32 literal sets | 10.9 | 20.6 | 123.7 | 126.4 | 7.7 |
| proj: 1 path | 0.8 | 0.3 | 7.9 | 9.5 | 1.1 |
| proj: 3 paths and v | 1.0 | 1.2 | 17.5 | 21.4 | 2.6 |
| proj: 48 paths | 3.5 | 15.1 | 255.7 | 281.8 | 31.2 |
| query: one path eq filter | 0.5 | 0.6 | 10.5 | 15.1 | 1.7 |
| query: four-term filter and 5 projections | 1.3 | 3.9 | 47.3 | 59.8 | 5.5 |
| query: 40-term filter and 48 projections | 4.0 | 37.1 | 621.0 | 883.1 | 59.5 |

### 8.2 How the step tables were measured

- **The prototypes.** Each step table compares the compiler of today with a prototype of the step. A prototype
  is a copy of `compiler.ts` with the change of the step. The prototypes are not in the repository.
- **The cases.** The 19 cases of `expression-cases.ts`.
- **Heap KiB.** The partition path in Node v24.20.0, with the count method of `expression-alloc.mjs`. For the
  step A+B+C prototype, the partition path composes the statement one time and does not call a
  `validate*Plan` function.
- **Path µs in workerd.** The partition path inside a `PartitionDO`, with the timer method of
  `expression.workerd-bench.ts`. One run measured the four compilers for each case, one after the other.
- **µs in Node** (sections 4.2.2 and 4.2.3). The best of 9 batches of 1,000 calls. No workerd run measured these
  two prototypes apart from each other.
- **The buffer prototype** (section 5.1). The same two methods. For a condition, its partition path gets the
  whole statement from the compile.
- **Noise.** Two runs of one case differ by about 10% in time. A change smaller than that is not evidence.
