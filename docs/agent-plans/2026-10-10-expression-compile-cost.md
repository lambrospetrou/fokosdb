# RFC — Lower the memory cost and the CPU cost of the expression compile

**State:** Draft
**Date:** 2026-10-10
**Author:** Lambros
**Status:** The benchmark suite of section 4.2.1 is built. The plan has no canonical identity and the partition
has no second plan check (commit `a31c0e8`). Step A is built. Step B and step C are not built.

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

`docs/agent-plans/2026-10-10-expression-trees-over-rpc.md` moved the expression compile from the Worker into
the partition. Each partition compiles each condition, update, projection, and query filter that it runs. A
partition is one isolate with a memory limit of 128 MB, and it serves all its requests on one thread. Thus the
cost of one compile is a cost of each partition request.

The benchmark suite of section 4.2.1 gives the cost of the compiler today:

1. **One compile allocates 40 to 210 times the bytes of its expression tree.** A condition with four terms is
   320 bytes as a tree. Its partition path allocates 19.1 KiB. The partition path of a filter with 40 terms and
   48 projections allocates 634 KiB.
2. **A part of the allocation produces no SQL.** A profile of the compiler finds three causes that produce no
   part of the SQL: a regular expression scan, a closure set for each function node, and a key string for each
   binding lookup. Section 1.2 has the shares.
3. **The remaining allocation is in proportion to the SQL size, and the SQL is longer than necessary.** One
   comparison of a data path with a literal is 539 characters of SQL. 198 characters give the same result.
4. **SQLite pays for the SQL size a second time.** In a `PartitionDO`, the first `sql.exec` of a statement text
   that reads one item costs 5 to 19 times the JavaScript work of the partition path. A query pays for the SQL
   size a third time, for each item that it scans.
5. **SQLite refuses statements that the compiler accepts.** A chain of 94 terms, or 25 nested arithmetic
   operations, is deeper than the expression depth that Workers SQLite permits. An update of 32 actions can
   make a statement above the statement size that SQLite permits. The caller gets an error that does not name
   the cause. Section 4.2.5 has the measurements.

Appendix 8.1 has the baseline for each case.

### 1.2 Where the allocation goes

A sampling heap profile of `compileConditionExpression`, `compileUpdateExpression` and `compileQueryExpression`
in Node v24.20.0 gives these shares of the allocated bytes:

| Cause | Function | Share |
| --- | --- | ---: |
| `matchAll` makes one match object for each `?N` in the SQL | `compactPlanParameters`, `compactPoolParameters` | 21% to 29% |
| `join` copies the text of each term, at each level of the tree | `compileComparison`, `compileCondition` | 7% to 20% |
| Four closures and one object for each function node | `makeRenderers` | 10% (updates with functions) |
| One descriptor object and one `JSON.stringify` key for each binding lookup | `bindDescriptor`, `bindPath` | 2% to 6% |
| A flat copy of the type SQL for each constant test | `constTypeName` (`startsWith`) | 5% (conditions) |

The rest is the SQL text and its parts.

The profile is from the compiler before commit `a31c0e8`. That compiler also computed the canonical identity,
which was 5% to 11% of the bytes. Thus each share of today is a little higher than its row.

### 1.3 What the reader must know about the current system

- **The compiler.** `packages/fokosdb/src/shared/expression/compiler.ts` has four entry points:
  `compileConditionExpression`, `compileUpdateExpression`, `compileProjectionExpression` and
  `compileQueryExpression`. Each one validates the tree, renders SQL fragments, removes the bindings that the SQL
  does not use, and checks the SQL limits.
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
- **The plan stays in the partition.** A request carries an expression tree. `PartitionStore` compiles the tree
  when its statement runs (`request-plans.ts`), and the plan lives for one request. Commit `a31c0e8` removed two
  parts that existed only for a plan on the wire:
  - The plan has no `identity`, `filterIdentity` or `projectionIdentity`. `hashOperation` in the coordinator and
    the query cursor in the client call the identity functions on the tree.
  - `runtime.ts` has no `validate*Plan` function. It runs the plan that the partition compiled.
- **The size check.** `utf8WithinLimit` answers from the length when `text.length * 3 <= limit`. For the SQL
  limit of 100,000 bytes, a text of 33,334 to 100,000 characters goes to `TextEncoder.encode`, which makes a
  buffer of the size of the text. `utf8.ts` also has `utf8ByteLength`, which counts the bytes with `encodeInto`
  into one fixed buffer and makes no copy.
- **The update size check.** `compileUpdateExpression` checks `documentSql` and `applicableSql`, each one
  alone. It does not check a statement. The other three entry points check the composed statement.

### 1.4 Glossary

- **Partition path** — all the JavaScript work for one expression before `sql.exec`: the compile, the
  statement composition, and the bound values. `prepareCase` in
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
4. Step C: a statement that the compiler accepts runs in SQLite. A long chain and nested arithmetic stay below
   the expression depth of SQLite, and the compile refuses an update whose statement is above the statement
   size of SQLite.

### 2.2 Out of scope

- **A plan cache in the partition.** Section 4.3.4 of `2026-10-10-expression-trees-over-rpc.md` decides against
  it for now.
- **Shorter SQL for updates.** Section 4.3.2 records what the update SQL repeats. It needs its own design.
- **A different way to build the SQL text.** Section 5.1 gives the reason.
- **One statement composition for a condition and a projection.** The compile composes the statement for the
  size check, and `runtime.ts` composes it again. The second composition allocates 1.3 KiB or less for a
  condition, and it makes no second copy of the text. Thus the plan does not get a statement field.

### 2.3 Requirements

- **Step A must not change the SQL.** Each plan must be equal to the plan before the step, byte for byte.
- **Step B and step C must not change a result.** For each expression and each item, the statement must return
  the rows that the statement of today returns, when the statement of today runs.
- **A change must not refuse an expression that runs today.** An expression that compiles today and runs in
  SQLite must compile and run after each step. Step B can make an expression valid that fails the SQL size
  limit today. Step C refuses, with `sql_limit`, an update that compiles today and fails in SQLite.
- **The `OperationDefinition` contract must stay the same.** A new operation must need no knowledge of a step.
  Step A and step B change no operation in `operation-registry.ts`. Step C changes the body of
  `renderArithmeticPresent`, and no other operation.
- **The benchmark suite must run with no change to a production module.** It must not add a test hook.

---

## 3. Milestones

1. **The benchmark suite.** Built. It gives the baseline of appendix 8.1.
2. **Step A.** It changes `compiler.ts` and `utf8.ts`. It changes no SQL.
3. **Step B.** It changes `compiler.ts` and the tests that compare SQL text.
4. **Step C.** It changes `compiler.ts`, `plan.ts`, `runtime.ts`, `operation-registry.ts`,
   `partition-store.ts`, and the tests that compare SQL text.

No step depends on another RFC, and each step ships alone. Step B comes before step C because it gives the
largest gain, and because step C then measures the update statements and the chains in their final text.

---

## 4. Proposed Solution

### 4.1 High-level overview

The compiler keeps its design: it renders SQL strings from the expression tree. Step A and step B lower its
cost. Step C makes each statement that the compiler accepts run in SQLite.

```
expression tree
   │ validate
   │ render SQL fragments ── step A: one renderer set for each compile, binding lookup with no key string
   │                         step B: one json_type test for a data path against a literal
   │                         step C: a long chain of AND or OR terms as balanced groups
   │ find the bindings that the SQL uses ── step A: a scan with no regular expression
   │ check the SQL limits ── step A: a byte count with no encode buffer
   │                         step C: an update checks the statements that run, not its fragments
   ▼
plan ──▶ statement composition ──▶ sql.exec
```

- **Step A** removes allocation that produces no SQL. The plans stay equal, byte for byte.
- **Step B** makes the SQL shorter. Less SQL text lowers the compile allocation, the SQLite prepare time, and
  the SQLite time for each scanned item.
- **Step C** changes the SQL text of a chain, and adds one size check. It lowers no cost.

The result for the partition path, with step A and step B. The heap column is Node v24.20.0, and the time
column is a `PartitionDO` in workerd. Appendix 8.2 has the method.

**The "before" columns of this RFC are not the baseline of today.** Each step table of section 4.1 and
section 4.2 compares a prototype with the compiler before commit `a31c0e8`. The partition path of both sides
included the canonical identity and the second plan check, which are gone. Appendix 8.1 has the baseline of
today: its heap count is 5% to 28% lower than the "before" column, and its time is equal inside the noise. The
prototypes do not have the size check of section 4.2.3. `TODO: measure` each step again on the compiler of
today before it merges (section 4.2.8).

| Case | Heap KiB before | A | A+B | Change | Path µs before | A | A+B | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| cond: not_exists(hashKey) | 3.4 | 3.6 | 3.6 | +6% | 1.5 | 1.8 | 1.7 | +13% |
| cond: optimistic lock | 6.6 | 6.7 | 6.8 | +3% | 3.5 | 3.2 | 3.1 | -11% |
| cond: one path eq | 8.4 | 8.0 | 6.2 | -26% | 4.3 | 4.0 | 3.4 | -21% |
| cond: contains on an array path | 10.7 | 9.3 | 9.3 | -13% | 5.8 | 4.2 | 4.1 | -29% |
| cond: four terms | 22.0 | 18.3 | 12.9 | -41% | 10.6 | 8.2 | 7.5 | -29% |
| cond: nested access policy | 28.7 | 24.5 | 20.8 | -28% | 12.4 | 9.9 | 8.8 | -29% |
| cond: 40 distinct path eq | 233 | 180 | 104 | -55% | 113 | 77.1 | 53.7 | -52% |
| cond: 80 eq on one path | 611 | 513 | 187 | -69% | 219 | 142 | 91.3 | -58% |
| upd: set 1 literal | 8.9 | 7.9 | 7.9 | -11% | 4.4 | 3.4 | 3.4 | -23% |
| upd: remove 1 path | 7.3 | 7.1 | 7.1 | -3% | 3.5 | 3.0 | 2.9 | -17% |
| upd: counter and timestamp | 25.8 | 17.7 | 17.7 | -31% | 11.9 | 8.1 | 8.4 | -29% |
| upd: 20 actions with arithmetic | 333 | 196 | 196 | -41% | 191 | 114 | 112 | -41% |
| upd: 32 literal sets | 126 | 81.4 | 81.3 | -35% | 75.2 | 55.2 | 52.7 | -30% |
| proj: 1 path | 9.9 | 9.1 | 8.2 | -17% | 5.1 | 4.0 | 3.5 | -31% |
| proj: 3 paths and v | 21.8 | 18.0 | 15.4 | -29% | 9.8 | 6.8 | 6.0 | -39% |
| proj: 48 paths | 283 | 217 | 173 | -39% | 124 | 93.3 | 69.3 | -44% |
| query: one path eq filter | 15.1 | 14.4 | 12.8 | -15% | 7.3 | 6.2 | 5.6 | -23% |
| query: four-term filter and 5 projections | 59.6 | 49.8 | 39.4 | -34% | 25.4 | 18.7 | 15.7 | -38% |
| query: 40-term filter and 48 projections | 883 | 755 | 509 | -42% | 258 | 211 | 163 | -37% |

A typical expression (four terms, a counter update, a query with five projections) needs 31% to 41% less heap
and 29% to 38% less CPU time. The two smallest conditions need 3% to 6% more heap in the prototype.
Section 4.2.3 gives the cause and the change that removes it.

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

**Change.** One function replaces the two. It finds each marker with `indexOf`, reads each digit as its
position in the text `0123456789`, and sets a flag in a `Uint8Array` that has one entry for each binding. The marker is `?` for the
direct layout and `?P, '$[` for the pool layout. When the count of set flags equals the binding count, the
function returns the fragments as they are. The renumbering keeps the `replace` call. The scan does not use
`charCodeAt`: `check:keys` permits it only in `key-codec.ts` and `partition-id.ts`.

**Why the scan must stay.** An expression of the public API reaches the renumbering. The example is a
comparison of an arithmetic value with the literal `null`:

```ts
{ op: "eq", args: [{ fn: "+", args: [{ ref: "data", path: "$.a" }, { val: 1 }] }, { val: null }] }
```

1. The validator accepts it. `$.a` can be `null`, so the type of `$.a + 1` includes `null`.
2. `compileComparison` renders the presence test of `$.a + 1` first. That registers the bindings `$.a` and `1`.
3. The rendered type of an arithmetic value is the constant `'number'`, and the type of the literal is the
   constant `'null'`. `pushTypeGuards` finds that the comparison can never pass, and the result is `(0)`.
4. The two bindings stay registered, and no SQL uses them.

In `and(<the example>, eq($.b, "x"))`, the bindings of `$.b` and `"x"` are registered as `?5` and `?6`, and the
renumbering makes them `?3` and `?4`. Without it, the statement binds four values for two parameters, and
Workers SQLite refuses the statement. The 17,194 random expressions of section 4.2.8 did not include this
shape.

**Evidence.** Partition path, Node v24.20.0, the compiler before `a31c0e8` against a prototype with this
change only:

| Case | Heap KiB before | With the scan | Change | µs before | With the scan | Change |
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

#### 4.2.3 Step A: one renderer set, a binding lookup with no key string, and a size check with no buffer

**Today.**

- `utf8WithinLimit` encodes a text of 33,334 to 100,000 characters to count its bytes (section 1.3).
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
- `utf8WithinLimit` keeps its two answers from the length. For a text between them, it returns
  `utf8ByteLength(text) <= limit` in place of the `TextEncoder.encode` call. `utf8ByteLength` is in `utf8.ts`
  and has its tests. The function gives the same answer for each text, so no plan and no error changes.

**The size check, measured.** Appendix 8.1 against the compiler of today with this one change, partition path,
heap KiB, Node v24.20.0. Only a statement above 33,334 characters changes:

| Case | Statement characters | Heap KiB today | With the change | Change |
| --- | ---: | ---: | ---: | ---: |
| cond: 80 eq on one path | 43,818 | 490.9 | 448.1 | -9% |
| query: 40-term filter and 48 projections | 58,796 | 633.8 | 576.4 | -9% |

The 17 other cases do not reach the encode, and their counts are equal.

**Why the lookup is the same.** A `Map` compares keys with SameValueZero. It keeps `1` and `"1"` apart, and
`null`, `true` and `false` apart. `bindLiteral` already changes `-0` to `0`, and `validateScalarLiteral` refuses
`NaN`. Thus two values share a binding in the new lookup exactly when they share one today.

**Evidence for the renderer set and the lookup.** Partition path, Node v24.20.0, the prototype of
section 4.2.2 against a prototype that adds these two changes:

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
of the increase. Measured with step A as built (appendix 8.1): each of the 19 cases allocates less than the
baseline, and `cond: not_exists(hashKey)` goes from 3.0 KiB to 2.7 KiB.

Step A with no size check change: the compiler before `a31c0e8` against the prototype with the scan, the
renderer set and the lookup. The time column is a `PartitionDO` in workerd:

| Case | Heap KiB before | Step A | Change | Path µs before | Step A | Change |
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

`cond: 80 eq on one path` gets a statement of 16,538 characters. In the prototype, the size check then answers
from the length, so a part of the 64% of this case is the encode buffer. The size check of section 4.2.3
removes that buffer in step A, thus step B gives this case a smaller change than the table shows.

#### 4.2.5 Step C: avoid or refuse what SQLite refuses

Two kinds of statement pass the compile today and fail in SQLite. The caller then gets `runtime_capability`,
which does not name the cause. Step C makes the first kind run, and it refuses the second kind in the compile
with `sql_limit`.

**1. The expression depth.** Workers SQLite refuses a statement whose expression is deeper than 100:
`Expression tree is too large (maximum depth 100)`. SQLite parses `a OR b OR c` as `((a OR b) OR c)`, so each
term of a chain adds one level. The results of today in a `PartitionDO`:

| Expression | Result today |
| --- | --- |
| `and` or `or` of 93 `eq` terms on `v` | Runs |
| `and` or `or` of 94 `eq` terms on `v` | Fails |
| `eq` of `v + 1 + 1 ...` with 20 nested `+`, and a literal | Runs |
| The same with 25 nested `+` | Fails |
| Update: `set $.a` to `$.a + 1 + 1 ...` with 20 nested `+` | Runs |
| The same with 30 nested `+` | Fails |
| 30 nested `not`, `sqlite.abs`, `if_not_exists`, `sqlite.coalesce` or `sqlite.iif`, or 30 nested `and` and `or` of two terms | Runs |

The compiler accepts all these expressions: `EXPRESSION_LIMITS.operatorsAndFunctions` is 300 and
`EXPRESSION_LIMITS.astDepth` is 32.

The chains that cause the depth:

- `compileCondition` joins the terms of an `and` or an `or` in one chain. An expression can have 299 terms.
- `compileIn` joins one `eq` comparison for each choice with `OR`, when the choices have more than one type or
  are `null`. An `in` can have 100 choices.
- `compileUpdateExpression` joins the terms of `applicableSql` with `AND`: one term, then two terms for each
  `set` action, then two terms. 32 actions give a chain of 67 terms. It joins the terms of `valueTypeSql` in
  the same way.
- `renderArithmeticPresent` joins five terms with `AND`. The first term is the presence test of the first
  operand. When that operand is also a `+`, a `-` or a `*`, it holds the same chain of five terms. Thus each
  nested arithmetic operation adds four levels.

**Change.** One function, `joinBalanced(terms, operator)`, replaces the `join` call at these four places. It
keeps the order of the terms, and it puts them in groups of two with parentheses, as a balanced tree:

```sql
-- today: 4 terms, depth 3
a AND b AND c AND d
-- step C: depth 2
(a AND b) AND (c AND d)
```

- A list of one term or two terms gives the text of today.
- A chain of `n` terms gets a depth of `ceil(log2(n))`. 299 terms need 9 levels, and 67 terms need 7.
- The five terms of `renderArithmeticPresent` become `(p0 AND p1) AND (t0 AND (t1 AND finite))`. Each nested
  arithmetic operation then adds two levels, and 30 nested operations add 60.
- The function is in a module that `compiler.ts` and `operation-registry.ts` both import, because
  `compiler.ts` imports the registry.
- `documentSql` does not change. It nests one `jsonb_set` or `jsonb_remove` call for each action, which is 32
  levels or fewer.
- The joins of `compileComparison`, `compileBeginsWith` and `compileContains` do not change. Each has four
  terms or fewer, and a comparison is never inside a comparison, so these chains add their levels one time.

**Why the result is the same.** `AND` and `OR` are associative in SQL, also when a term is NULL. The order of
the terms does not change, so SQLite evaluates them from left to right as it does today. A guard such as
`i.data_kind = 2` stays before the `json_type` call that depends on it.

**Evidence.** In a `PartitionDO`, the predicate of an `or` of 299 `eq` terms on `v`, joined as balanced groups,
runs. The flat form fails at 94 terms. For 94 terms, the statement is 12,712 characters flat and 12,896
characters as balanced groups (+1.4%). `TODO: measure` the nested arithmetic after the change of
`renderArithmeticPresent`: the two levels for each operation are a calculation.

**The rule for completion.** Step C is complete when each of these expressions runs in a `PartitionDO`:

- An `and` and an `or` with the largest term count that the other limits permit.
- An `in` with the largest count of choices of mixed types that the binding limit permits.
- For each condition operator and each operation of `OPERATION_REGISTRY` that can hold itself: the operation
  nested to `EXPRESSION_LIMITS.astDepth`.
- An update with 32 `set` actions that each hold nested arithmetic, at the largest size that part 2 accepts.

If one shape still fails, the step must add a compile check that refuses it with `sql_limit`.

**2. The size of the update statement.** `compileUpdateExpression` checks `documentSql` and `applicableSql`
against `EXPRESSION_LIMITS.compiledSqlBytes`, each one alone. No statement runs one fragment alone:

| Statement | Fragments in its text |
| --- | --- |
| The probe (`composeUpdateProbeStatement`) | `applicableSql` two times, `valueTypeSql` one time, `documentSql` one time. `applicableSql` also holds `documentSql`. |
| The write (`PartitionStore.updateItemSingleShot`) | `documentSql` three times |
| The lock row (`PartitionStore.insertPendingUpdateLock`) | `documentSql` one time |

Thus a statement can be above the limit of SQLite (100,000 bytes) when each fragment is below it. Measured in a
`PartitionDO`, for an update of 32 `set` actions:

| Value of each action | `documentSql` | `applicableSql` | Probe statement | Result today |
| --- | ---: | ---: | ---: | --- |
| `if_not_exists(path, 0) + 1` | 9,774 | 39,426 | 89,108 | Runs |
| `(if_not_exists(path, 0) + 1) + 1` | 10,542 | 52,504 | 116,032 | `statement too long: SQLITE_TOOBIG` |

**Change.** `compileUpdateExpression` checks the size of the statements that run the plan, as the other three
entry points do:

- It composes the probe statement and checks it. `composeUpdateProbeStatement` moves to `plan.ts`, beside the
  other compose functions.
- It checks the widest form of the write statement. The text of that statement moves to a compose function in
  `plan.ts`, and `updateItemSingleShot` calls it. `QUERY_WIDEST_SCAN_CONDITIONS` is the model for "the widest
  form".
- The two checks of one fragment alone go away. The lock row statement is always shorter than the write
  statement.
- The check measures the text of the probe that runs, and the widest form of the write. The widest form is a
  few characters longer than a write with no TTL value. Apart from those characters, the check refuses no
  statement that SQLite accepts today.

The cost: the compile composes two statements more. With the size check of section 4.2.3, a check allocates no
copy of the text. `updatePlanOf` keeps the plan for the request, so the check runs one time for each update.
`TODO: measure` the update cases after the change.

Part 1 makes the update statements longer by two characters for each group. Thus part 1 and part 2 ship
together, and the size check of part 2 measures the text of part 1.

#### 4.2.6 Extensibility

- No step changes `OperationDefinition` or `OperationRenderers`. Step C changes the body of
  `renderArithmeticPresent`, which the entries `+`, `-` and `*` share. No other entry of `OPERATION_REGISTRY`
  changes.
- A new operation renders through the same callbacks. It gets the general guards of `pushTypeGuards`, and it
  needs no knowledge of the guard of step B.
- A new operation that joins three or more terms, of which one can hold the operation again, must use
  `joinBalanced`. The nesting test of section 4.2.5 runs each entry of `OPERATION_REGISTRY`, so it finds an
  operation that does not.
- A new binding kind needs one entry in `ExpressionBindingDescriptor`, as it does today. The lookup of
  section 4.2.3 makes its `Map` on first use.
- A new literal type needs one row in the table of section 4.2.4. Without the row, the comparison uses the
  general guards.

#### 4.2.7 Deployment and rollback

- **No SQL is stored.** A request and a coordinator row carry an expression tree. A partition compiles the tree
  for each request, with its current compiler, and a lock row holds the document that the update made, not SQL.
  Thus no step has an order of deployment, and two partitions with different versions give the same result for
  one expression.
- **Step A** changes no output.
- **Step B** changes the SQL text. Both SQL texts give the same result.
- **Step C** changes the SQL text of a chain, with the same result. It also changes one error: an update whose
  statement is above the SQL limit gets `sql_limit` from the compile, where it gets `runtime_capability` from
  SQLite today. Both are a `FokosExpressionError`, which is fatal in a prepare.
- **Rollback.** Each step is a change of code only. No stored format changes.

#### 4.2.8 Testing

- **Tests use the public compile functions only.** No step adds an export or a hook for a test. A branch that
  no expression reaches has no test.
- **Step A.** For each expression, the plan must equal the plan of the compiler before the step. A run on the
  prototypes compared 17,194 random expressions that compile, the fixtures of `test-fixtures.ts`, and the
  benchmark cases: all plans are equal. The same run gave the same error for the 12,865 random expressions that
  do not compile. The size check keeps the tests of `utf8.test.ts`, and gets one test of `utf8WithinLimit` for
  a text between its two length answers, with ASCII text and with text of 3 bytes for each character.
- **The renumbering.** The expression of section 4.2.2 reaches it through the public compile functions. The
  tests, for the direct layout (a condition) and for the pool layout (a query filter with a projection):
  - The dropped term first: `and(<dropped>, eq($.b, "x"))`. The plan has only the bindings of `$.b` and `"x"`,
    numbered from the first parameter.
  - The dropped term last: `or(eq($.b, "x"), <dropped>)`. The plan has the same two bindings, and no parameter
    number changes.
  - The dropped term alone. The plan has no binding.
  - Each of the three runs in a `PartitionDO` and gives the correct result, which proves that the bound value
    count equals the parameter count.
  The random run must get this shape: a comparison of an arithmetic value on a data path with `null`. A direct
  comparison of the old function and the new function on 20,000 made-up SQL fragments, of which 14,727
  renumber, gave equal results on the prototype.
- **Step B.** A run on the prototype executed the SQL of today and the shorter SQL on SQLite 3.53.4
  (`node:sqlite`), for 17,194 conditions, projections and queries over 17 items: absent, bytes, text, and
  JSON items with each value type at each path. All 292,298 results are equal. A defect that the run got on
  purpose (`number` tested as `'integer'` only) failed on the first affected expression. In a `PartitionDO`, the
  19 benchmark cases return equal rows for both SQL texts.
- **Step C, the depth.** The expressions of "The rule for completion" in section 4.2.5 run in a `PartitionDO`
  and give the correct result. For the chains that run today (93 terms or fewer), the comparison of step B
  applies: the statement of today and the statement of step C return equal rows.
- **Step C, the update size.** Two updates run through the public API in a `PartitionDO`: the largest one that
  the check accepts runs its probe and its write, and the next larger one gets `sql_limit` from the compile.
- **Before each step merges**, the implementation must repeat its comparison against the compiler of the commit
  before it.
- **SQL text tests.** The tests that compare SQL text change with step B and with step C. The tests that
  compare results must pass with no change.
- **The benchmark suite.** Each step must update the tables of appendix 8.1.

### 4.3 Open Questions

#### 4.3.1 Fold the comparison before it registers a binding

Resolved for this RFC: the scan and the renumbering stay, and section 4.2.8 has their tests.

Open: `compileComparison` can test the two constant types before it renders a presence test. The expression of
section 4.2.2 then registers no binding, and no known expression reaches the renumbering. That makes the
renumbering code that no test can run, so this RFC does not make that change.

#### 4.3.2 Shorter SQL for updates

Step B does not change the update SQL. The update SQL repeats text in three ways:

- `applicableSql` holds the full `documentSql` inside `json_type(...)`, and the probe statement holds
  `applicableSql` two times and `documentSql` one more time.
- `renderArithmeticPresent` keeps terms that are constant, such as `1 AND 1` and `'number' = 'number'`.
- Two `set` actions with the same parent path give the same target guard two times.

Part 2 of step C refuses an update whose statement is above the SQL limit. It does not make the statement
shorter. With the probe statement of today, the largest update that runs has an `applicableSql` of about one
half of the limit.

`upd: 20 actions with arithmetic` is 3,312 bytes as a tree and 56,071 characters as a probe statement. Its first
`sql.exec` costs 2,313 µs. The question: which of the three can go with no change to the result?

#### 4.3.3 One statement composition for a query

`compileQueryExpression` composes the widest statement for the size check. `PartitionStore.scanQueryPage` then
composes the statement that it runs. Options: keep both, or check the size of the statement that runs. The
second option moves the size error from the compile to the first scan of the request.

#### 4.3.4 Validation runs two times in the partition

Section 4.2.3 of the expression trees RFC makes `sequencePlanOf` call `validateConditionExpression` for
`requiredColumns`, and then `#evaluate` compiles, which validates again. This occurs only for an operation that
follows a write of the same item. A validation allocates 0.4 to 21.9 KiB.
Options: keep both, or let the compile take the analysis of the first validation.

#### 4.3.5 The young generation of workerd

The heap counts are from Node. The cost of a garbage collection depends on the size of the young generation,
which the expression trees RFC records as `TODO: measure` for workerd. This RFC has the same gap.

---

## 5. Alternative Options

### 5.1 Build the SQL in one buffer

Each emit function appends SQL parts to one array, and one `join` makes each SQL text. A prototype of the full
compiler in this form exists outside the repository. It gives the SQL text and the bindings of the A+B+C
prototype (defined below), byte for byte, for 17,194 random conditions, projections and queries, and for 32,543
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

In this section, "A+B+C" is a prototype of an earlier form of this RFC: step A and step B, with no canonical
identity, no second plan check, one statement composition, and a size check that makes no buffer for ASCII
text. It is near to the compiler of today with step A and step B. It does not have step C of section 4.2.5.

Evidence. Partition path, the A+B+C prototype against the buffer prototype:

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
section 2.3 forbid, and the CPU gain is not clear of the noise. The buffer form stays possible after step B. The
decision then depends on the value of 4.0 KiB for each four-term condition (10.7 KiB against 6.7 KiB).

### 5.2 Keep the rendered SQL of each value node

A map from a value node to its rendered SQL saves a second render of the same node, for example in `between` and
in `renderArithmeticPresent`. Not chosen: a comparison of a path with a literal, which is the common case,
renders no node two times in one mode, so the map only adds allocation there.

### 5.3 Remove the renumbering

Not chosen: an expression of the public API reaches the renumbering (section 4.2.2), and its statement fails
in Workers SQLite without it.

### 5.4 A plan cache in the partition

A cache removes the compile for a repeated expression. Not chosen here: the expression trees RFC decides against it
for now, and each step of this RFC also lowers the cost of a cache miss.

---

## 6. Frequently Asked Questions

**Why does the SQL size matter more than the JavaScript time?**
The first `sql.exec` of the four-term condition costs 63.0 µs, and its partition path costs 10.2 µs. For 40 path
terms, the numbers are 2,000 µs and 125 µs. SQLite parses and plans each character of the statement.

**Does a partition prepare the statement for each request?**
No. For a statement that reads one item, the second `sql.exec` of the same text is 4 to 44 times faster than the
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
No value of `EXPRESSION_LIMITS` changes. With step B, an expression that fails `compiledSqlBytes` today can
fit. With step C, a chain of 94 terms or more runs, and an update is measured as the statements that run it:
an update that SQLite refuses today for its statement size gets `sql_limit` from the compile.

---

## 7. References

- `docs/agent-plans/2026-10-10-expression-trees-over-rpc.md` — sections 4.2.2, 4.2.3, 4.2.4, 4.2.8 and 4.3.4.
- `docs/agent-plans/2026-08-29-typed-expression-engine-spec.md`
- `docs/agent-plans/2026-09-14-read-projections-and-query-filters.md`
- `packages/fokosdb/src/shared/expression/compiler.ts`, `plan.ts`, `runtime.ts`, `bindings.ts`, `utf8.ts`,
  `operation-registry.ts`, `request-plans.ts`
- `packages/fokosdb/src/shared/partition/partition-store.ts` — `updateItemSingleShot` and
  `insertPendingUpdateLock`.
- `packages/fokosdb/test/expression-bench/expression-cases.ts`, `expression.workerd-bench.ts`,
  `expression-alloc.mjs`
- [Limits of SQLite](https://www.sqlite.org/limits.html) — the maximum depth of an expression tree.
- [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)

---

## 8. Appendix

### 8.1 The baseline

Both tables are from commit `cf4ad33`. It has no step of this RFC. It has no canonical identity in the plan and
no second plan check (section 1.3).

`pnpm --filter fokosdb bench:workerd expression`. workerd, a `PartitionDO` with 200 items, µs for each call,
median of 5 batches of at least 100 ms:

| Case | Tree B | SQL B | Binds | Compile | Path | SQLite first | SQLite again |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| cond: not_exists(hashKey) | 46 | 330 | 0 | 0.4 | 1.4 | 16.1 | 4.0 |
| cond: optimistic lock | 107 | 461 | 1 | 1.9 | 3.0 | 19.8 | 4.0 |
| cond: one path eq | 71 | 845 | 2 | 2.9 | 4.5 | 33.2 | 5.0 |
| cond: contains on an array path | 80 | 1676 | 2 | 4.2 | 5.5 | 52.2 | 6.2 |
| cond: four terms | 320 | 2066 | 8 | 8.4 | 10.2 | 63.0 | 8.3 |
| cond: nested access policy | 390 | 3067 | 7 | 10.3 | 11.5 | 85.9 | 7.3 |
| cond: 40 distinct path eq | 2961 | 22208 | 80 | 118.7 | 125.0 | 2000.0 | 67.9 |
| cond: 80 eq on one path | 5850 | 43818 | 81 | 188.5 | 227.5 | 3257.8 | 73.7 |
| upd: set 1 literal | 83 | 958 | 3 | 3.2 | 4.2 | 41.0 | 6.7 |
| upd: remove 1 path | 68 | 831 | 1 | 1.9 | 3.1 | 37.8 | 6.6 |
| upd: counter and timestamp | 260 | 3753 | 6 | 9.5 | 10.9 | 133.8 | 12.5 |
| upd: 20 actions with arithmetic | 3312 | 56071 | 42 | 166.0 | 169.9 | 2312.5 | 141.6 |
| upd: 32 literal sets | 2637 | 6804 | 65 | 76.2 | 75.2 | 373.0 | 95.7 |
| proj: 1 path | 43 | 718 | 1 | 3.0 | 4.6 | 27.6 | 4.9 |
| proj: 3 paths and v | 147 | 2109 | 3 | 6.8 | 8.7 | 61.5 | 8.1 |
| proj: 48 paths | 2391 | 30471 | 48 | 106.4 | 119.1 | 2257.8 | 99.1 |
| query: one path eq filter | 82 | 1342 | 2 | 4.5 | 5.7 | 835.9 | 914.1 |
| query: four-term filter and 5 projections | 533 | 5260 | 9 | 18.4 | 22.1 | 1234.4 | 1093.8 |
| query: 40-term filter and 48 projections | 5377 | 58796 | 128 | 248.0 | 263.7 | 25250.0 | 24750.0 |

`pnpm --filter fokosdb bench:alloc:expression`. Node v24.20.0, heap KiB for each call, mean of 8 to 2,000 calls
after 3,000 warm-up calls. The Compile column includes one validation and no identity:

| Case | Validate | Identity | Compile | Path | Kept |
| --- | ---: | ---: | ---: | ---: | ---: |
| cond: not_exists(hashKey) | 0.5 | 0.3 | 1.8 | 3.0 | 0.7 |
| cond: optimistic lock | 0.8 | 0.9 | 4.0 | 5.3 | 1.0 |
| cond: one path eq | 0.6 | 0.5 | 6.0 | 7.8 | 1.3 |
| cond: contains on an array path | 0.5 | 0.5 | 9.1 | 10.0 | 2.2 |
| cond: four terms | 0.8 | 2.3 | 18.3 | 19.1 | 2.5 |
| cond: nested access policy | 0.5 | 2.8 | 25.0 | 26.1 | 3.5 |
| cond: 40 distinct path eq | 0.5 | 22.0 | 208.0 | 209.5 | 22.8 |
| cond: 80 eq on one path | 0.4 | 43.9 | 490.8 | 490.9 | 44.0 |
| upd: set 1 literal | 1.3 | 0.7 | 6.2 | 7.8 | 1.4 |
| upd: remove 1 path | 1.3 | 0.6 | 5.0 | 6.5 | 1.3 |
| upd: counter and timestamp | 3.1 | 2.0 | 21.5 | 22.4 | 4.2 |
| upd: 20 actions with arithmetic | 21.9 | 25.9 | 307.2 | 307.2 | 55.6 |
| upd: 32 literal sets | 10.9 | 20.6 | 101.3 | 104.3 | 7.7 |
| proj: 1 path | 0.8 | 0.3 | 7.5 | 9.0 | 1.3 |
| proj: 3 paths and v | 1.0 | 1.2 | 16.3 | 20.3 | 2.6 |
| proj: 48 paths | 3.5 | 15.1 | 219.5 | 264.5 | 31.2 |
| query: one path eq filter | 0.6 | 0.6 | 10.0 | 12.3 | 1.6 |
| query: four-term filter and 5 projections | 1.3 | 3.9 | 43.6 | 49.9 | 5.5 |
| query: 40-term filter and 48 projections | 4.0 | 37.1 | 580.5 | 633.8 | 59.5 |

**After step A.** The working tree on commit `9c7b260`, with step A. Each "before" column is the compiler of
that commit. The workerd run measured the two compilers one after the other, on one machine. Step A changes
no SQL, so the SQL size, the binding count and the two SQLite columns do not change.

| Case | Heap KiB before | Step A | Change | Compile µs before | Step A | Path µs before | Step A | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| cond: not_exists(hashKey) | 3.0 | 2.7 | -10% | 0.4 | 0.4 | 1.6 | 1.6 | 0% |
| cond: optimistic lock | 5.3 | 5.0 | -6% | 1.9 | 1.6 | 3.4 | 3.1 | -9% |
| cond: one path eq | 7.8 | 6.7 | -14% | 3.4 | 2.5 | 5.1 | 4.1 | -20% |
| cond: contains on an array path | 10.0 | 7.9 | -21% | 4.6 | 3.1 | 6.0 | 4.6 | -23% |
| cond: four terms | 19.1 | 14.6 | -24% | 9.8 | 6.5 | 11.2 | 8.3 | -26% |
| cond: nested access policy | 26.1 | 20.9 | -20% | 10.5 | 8.2 | 12.3 | 9.6 | -22% |
| cond: 40 distinct path eq | 209.5 | 153.7 | -27% | 116.2 | 82.0 | 119.1 | 83.5 | -30% |
| cond: 80 eq on one path | 490.9 | 341.2 | -30% | 201.2 | 193.4 | 207.0 | 201.2 | -3% |
| upd: set 1 literal | 7.8 | 6.3 | -19% | 3.4 | 2.4 | 5.2 | 3.5 | -33% |
| upd: remove 1 path | 6.5 | 5.6 | -14% | 2.3 | 1.4 | 3.5 | 3.0 | -14% |
| upd: counter and timestamp | 22.4 | 14.4 | -36% | 11.0 | 7.3 | 12.1 | 8.6 | -29% |
| upd: 20 actions with arithmetic | 307.2 | 176.0 | -43% | 187.5 | 136.7 | 193.4 | 131.8 | -32% |
| upd: 32 literal sets | 104.3 | 59.9 | -43% | 76.7 | 63.0 | 80.1 | 71.3 | -11% |
| proj: 1 path | 9.0 | 7.7 | -14% | 4.1 | 2.5 | 5.6 | 4.2 | -25% |
| proj: 3 paths and v | 20.3 | 16.0 | -21% | 8.2 | 4.9 | 10.1 | 7.2 | -29% |
| proj: 48 paths | 264.5 | 195.8 | -26% | 98.6 | 74.2 | 144.5 | 104.5 | -28% |
| query: one path eq filter | 12.3 | 11.0 | -11% | 5.1 | 4.6 | 7.4 | 5.9 | -20% |
| query: four-term filter and 5 projections | 49.9 | 39.1 | -22% | 19.5 | 13.3 | 24.5 | 15.7 | -36% |
| query: 40-term filter and 48 projections | 633.8 | 511.5 | -19% | 240.2 | 155.3 | 293.0 | 183.6 | -37% |

In one earlier run of step A, the partition path of `cond: 80 eq on one path` was 134.8 µs. Its change is
thus not clear of the noise.

The plan comparison of section 4.2.8, step A against the compiler of commit `9c7b260`: 22,740 expressions that
compile give equal plans, of which 1,401 renumber, and 37,409 expressions that do not compile give equal
errors. The set has random conditions, updates, projections and queries, the fixtures of `test-fixtures.ts`,
the 19 benchmark cases, and statements above 33,334 characters.

### 8.2 How the step tables were measured

- **The prototypes.** Each step table compares the compiler of today with a prototype of the step. A prototype
  is a copy of `compiler.ts` with the change of the step. The prototypes are not in the repository.
- **The cases.** The 19 cases of `expression-cases.ts`.
- **Heap KiB.** The partition path in Node v24.20.0, with the count method of `expression-alloc.mjs`. The "before" side and
  the prototypes of step A and step B computed the canonical identity and called the `validate*Plan` functions,
  which commit `a31c0e8` removed. The A+B+C prototype of section 5.1 did not.
- **Path µs in workerd.** The partition path inside a `PartitionDO`, with the timer method of
  `expression.workerd-bench.ts`. One run measured the four compilers for each case, one after the other.
- **µs in Node** (sections 4.2.2 and 4.2.3). The best of 9 batches of 1,000 calls. No workerd run measured these
  two prototypes apart from each other.
- **The buffer prototype** (section 5.1). The same two methods. For a condition, its partition path gets the
  whole statement from the compile.
- **Noise.** Two runs of one case differ by about 10% in time. A change smaller than that is not evidence.
