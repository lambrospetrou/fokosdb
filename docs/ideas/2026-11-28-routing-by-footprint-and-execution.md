# RFC — Routing by footprint and execution in `FokosShardingRuntime`

**State:** Draft
**Date:** 2026-11-28
**Author:** Lambros
**Status:** Milestone 1 is built (section 6.1). This document gives the design that section 8 of
`docs/agent-plans/2026-11-20-scan-items.md` asks for. It is a breaking change of the host API one time, by decision.
After it, a new kind of dispatch must be an additive change (section 8).

References:

- `docs/agent-plans/2026-11-20-scan-items.md`, section 8 "Appendix: dispatch by footprint and execution"
- `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`, sections 4.2.6 to 4.2.12
- `docs/agent-plans/2026-09-27-learned-routes-for-every-dispatch-shape.md`
- `docs/agent-plans/2026-11-27-graph-get-items.md`
- `docs/ideas/2026-09-20-query-entry-point-into-a-range-tree.md`
- `packages/fokosdb/src/sharding/runtime.ts`, `runtime-types.ts`, `client.ts`, `range-frontier.ts`, `sk-interval.ts`
- `packages/fokosdb/src/server/do-partition.ts`, `operations()`

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. What routing must give](#2-what-routing-must-give)
- [3. How the code gives it now](#3-how-the-code-gives-it-now)
- [4. Verification of the scanItems conclusion](#4-verification-of-the-scanitems-conclusion)
- [5. Goals and requirements](#5-goals-and-requirements)
- [6. Milestones](#6-milestones)
- [7. Proposed solution](#7-proposed-solution)
- [8. Future compatibility: adding a dispatch kind later](#8-future-compatibility-adding-a-dispatch-kind-later)
- [9. Alternative options](#9-alternative-options)
- [10. Open questions](#10-open-questions)
- [11. Frequently asked questions](#11-frequently-asked-questions)

## 1. Overview and context

### 1.1 The problem

A host declares each operation with one dispatch shape: `point`, `group`, `single_owner`, `range`, or `local`
(`FokosOperation` in `sharding/runtime-types.ts:288`). A shape answers two questions at once:

1. **Footprint.** What part of the storage order does the request read or write?
2. **Execution.** How does the runtime run the owners of that part?

Each new use case so far needed a new answer to one of the two questions, and got it as a special case:

- `scanItems` needed "the owner of a position". It overloads `point` with `RouteKey | FokosStorageKey`, and the runtime
  and the client tell the two apart with `"routingHash" in key` (`runtime.ts:487`, `client.ts:324`).
- `queryItems` needed "an ordered walk over an interval". The `range` shape carries the FokosDB query model into the
  sharding layer: a hash key, an `SkInterval` with inclusive bounds, a direction, and a `FokosRangeCursor`.
- `graphGetItems` (draft) needs gets and queries in one step to one partition. A `group` item is a `RouteKey` only, so
  the spec routes a query by its hash key, and the partition dispatches a second `range` request when the key is
  promoted (`2026-11-27-graph-get-items.md`, section 4.2.11).

The next use cases also need new answers: a scan walk inside a Durable Object, a count or a delete over an interval, a
batch of queries, and a fan-out to each owner of the table for backup or replication. With the current API, each one
adds a shape or overloads one more.

### 1.2 Why now

The shapes are the core contract between a host and the runtime. Three hosts use them today
(`FokosDBPartitionDO`, `FokosDBTransactionCoordinatorDO`, `FokosDBMaterializationDO`). Each new shape or overload
changes this contract, and each change spans about 19 `descriptor.shape` branches in `runtime.ts`. The design must
be decided before the next consumer. A breaking change of the host API is acceptable now. A breaking change for each
later kind of dispatch is not.

### 1.3 Glossary

- **Position** — a place in the storage order `(routingHash, hk, sk)`. Type `FokosStoragePosition`, which is
  `FokosStorageKey` in the code today. An absent `hk` or `sk` is the empty key, the minimum.
- **Storage interval** — a half-open interval `[start, end)` of positions. Type `FokosStorageInterval`.
- **Scope** — the storage interval that one partition can own, from its identity (`storageInterval()`).
- **Span** — one element of a footprint: an item, a hash key, a position, or a storage interval. Type `FokosSpan`.
  Each span normalizes to storage intervals.
- **Footprint** — the list of spans of one request. Type `FokosFootprint`.
- **Visit** — one target partition, or `local`, with the spans that it serves. A planner gives a list of visits.
- **Execution** — the rule that runs the visits of one request: `one_owner`, `fan_out`, or `walk`.
- **Normalized form** — the checked form of a position or a span, inside the runtime only: `NormalizedPosition` and
  `NormalizedSpan` (section 7.2.1).

## 2. What routing must give

This section lists what the routing layer must give, independent of the API. Section 7 must keep each item.

### 2.1 The questions that routing answers

| # | Question | Exact or hinted |
| --- | --- | --- |
| Q1 | Which partition owns position `p` now? | Exact |
| Q2 | Which partition do I send `p` to, with the fewest hops? | Hinted: a cache can be wrong, and a miss falls back |
| Q3 | Which disjoint segments of an interval does each owner hold, in storage order? | Exact cover, hinted targets |
| Q4 | Which spans of one request have the same owner? | Exact grouping |
| Q5 | What does this partition own: its scope, the end of its owned slice, and does it own key `k`? | Exact |
| Q6 | During an import: which source answers my reads, and inside which caller slice? | Exact |
| Q7 | Which partitions served this request, and how many forwards did it cost? | Evidence for learning and metering |

### 2.2 Safety guarantees

- **G1 One owner.** At each instant, each position has exactly one owner. The topology, the cut-over route overrides,
  and the repartition state decide it.
- **G2 No misroute.** A partition never runs a handler for a span outside its scope. It throws
  `partition_misrouted`, and does not apply the request.
- **G3 No cutover race.** The ownership decision, the admission, and the local write of a synchronous handler run in
  one synchronous block. The first `await` is a remote call.
- **G4 Exact cover.** The visits of an interval cover it with no gap and no overlap. A learned slice is used only when
  it holds the whole segment (`2026-09-20-query-entry-point-into-a-range-tree.md`).
- **G5 Stable order.** An ordered walk visits owners in storage order. A repartition does not move a position, so a
  continuation position stays valid across splits and promotions.
- **G6 One call per owner.** The spans of one request that have one owner reach it in one sub-request. `txCommit`
  needs this rule (`commit_keyset_mismatch`).
- **G7 Safe fallback.** A cache-miss fallback starts only after an error that proves that no handler ran on the
  target. A fan-out operation must be idempotent for each span, because a router can apply one part before another
  part fails.
- **G8 Correct read-through.** During an import, only a read-only operation reads through. The source serves only the
  caller slice, and sends a promoted routingHash to its range tree, so it never serves a stale copy.
- **G9 No swallowed failure.** A failed part fails the request, except when the execution gives the failed part to
  `merge` (`collect`).
- **G10 One derivation.** Each hop derives the footprint from the request with the same function. The wire carries
  `op(ctx, req)` and no plan. Two hops of different versions agree when the function is the same.
- **G11 Hints do not change answers.** The Bloom filter, the hash arena, the topology cache, and the learned slices
  change only the number of hops.

### 2.3 Performance properties

- **P1 Few hops.** A request enters at the deepest known partition that holds its whole footprint, and jumps over
  known routers. `maxForwardRetries` bounds the fallbacks.
- **P2 Learning.** Every answer and every routed error carries `servedBy`, and each forwarder learns from it.
- **P3 Bounded fan-out.** A page visits at most `MAX_PARTITION_VISITS_PER_PAGE` (100) partitions.
- **P4 Progress during a repartition.** Reads continue through the source. Writes get `partition_migrating` and retry.

### 2.4 API properties

- **A1 Key-model independence.** The sharding layer knows positions and storage intervals only. It does not know
  sort-key conditions, cursors, or directions of FokosDB.
- **A2 Additive growth.** A new operation with a new footprint or a new execution adds no shape and no branch in the
  read-through gate, the admission, the miss ladder, or the learning.
- **A3 One coordinate in the client and the runtime.** Both resolve the same footprint types.
- **A4 A stable wire and stable storage.** A change of the host API changes no RPC and no stored state.

## 3. How the code gives it now

| Item | Mechanism today | Where |
| --- | --- | --- |
| Q1, G1 | `#resolvePosition` with `EXACT`: topology, then route override, then router role | `runtime.ts:1821` |
| Q2, G11 | `#resolvePosition` with `HINTED`: Bloom, hash arena, topology cache, learned slices | `runtime.ts:1821`, `1862` |
| Q3, G4 | `#planRange` and `planRangeFrontier`, on the sort keys of one hash key | `runtime.ts:1950`, `range-frontier.ts:138` |
| Q4, G6 | `#groupByOwner` and `#guardBloomHits` | `runtime.ts:678`, `751` |
| Q5 | `owns`, `ownerCheck`, `storageInterval`, `ownedStorageInterval`, `call.ownedSliceEnd` | `runtime.ts:1174` to `1221`, `2260` |
| Q6, G8 | `#whileImporting` (2 branches) and `#executeLocal` (3 branches) | `runtime.ts:417`, `1396` |
| Q7, P2 | `#forwardTo`, `#learn`, `RouteCollector` | `runtime.ts:2060`, `2138` |
| G2 | `out_of_range`, `#assertCanOwnRange`, `#ownsByTopology` | `runtime.ts:1767`, `1787` |
| G3 | Synchronous `local`, `localMode: "async"` for the coordinator | `runtime.ts:2288` |
| G7 | `#missLadder(routingHash, resolution, e, readOnly, shape)` and three forward functions | `runtime.ts:570` |
| G9 | `#settleGroup`, `partition_fanout_failed` | `runtime.ts:648` |
| G10 | `key(req)`, `items(req)`, `range(req)` at each hop | `runtime-types.ts:288` |
| P3 | The host budget in `walk`, and the visit loop of the client | `do-partition.ts:2047`, `client/db.ts` |

The guarantees hold today. The cost is in how they hold: each shape has its own resolver, its own forward function,
its own fallback, its own read-through branch, and its own admission input.

## 4. Verification of the scanItems conclusion

Section 8.1 of the `scanItems` RFC makes four claims. The code confirms each claim, with the corrections below. The
code also shows five more problems.

### 4.1 The claims

**Claim 1: each shape answers two questions at once.** Confirmed. `#dispatch` selects one code path for each shape
(`runtime.ts:386`). The footprint and the execution are not separate anywhere in the runtime.

**Claim 2: `point` is a `group` with one item.** Partly true. A `point` differs from a one-item `group` in four ways,
and a merged design must keep each one as a parameter, not as a shape:

- A `point` retry can use the Bloom step again. A `group` retry turns it off (`HINTED_NO_BLOOM`, `runtime.ts:190`).
  With one hash key, the Bloom step cannot break the one-call-per-owner rule, so the rule is "Bloom off on a retry
  when the spans have more than one hash key".
- A `point` error reaches the caller unchanged. A remote failure of an `attempt_all` group becomes
  `partition_fanout_failed`.
- A `point` has no `subRequest` and no `merge`.
- A `point` key can be a position. A `group` item is a `RouteKey` only.

**Claim 3: `single_owner` is a `group` with a stricter policy.** Not quite. `single_owner` is not a fan-out. It needs
the whole request in one synchronous handler call on one owner, for atomicity, and it answers `notApplicable` when the
keys are on more than one partition. It is a precondition on the execution: "one owner, or decline". It also cannot read
through: `#validateOperations` refuses it (`runtime.ts:2671`), and `#executeLocal` has no branch for it.

**Claim 4: the FokosDB key model is in the sharding layer.** Confirmed. `FokosRangeInput` holds `hashKey`, an
`SkInterval` with inclusive bounds, `descending`, and a `FokosRangeCursor` (`runtime-types.ts:124`). The read-through
gate checks the cursor with `cursorFallsInChild`, and the planner projects composite slices on one hash key with
`projectOnHashKey`. The lint rule keeps FokosDB imports out of `sharding/`, but the concepts moved in as types of the
sharding layer.

### 4.2 More problems that the code shows

1. **The admission input is lossy.** `hooks.admit` gets `keys: RouteKey[]`. A position gives `[]`
   (`runtime.ts:487`). A range gives the made-up key `{ hashKey, sortKey: NO_SORT_KEY }` (`runtime.ts:1056`). A hook
   cannot tell "no key" from "a scan", and cannot see an interval.
2. **The coordinator makes up a key too.** `tokenKey` sets `sortKey: NO_SORT_KEY` (`do-transaction-coordinator.ts:404`),
   because a footprint must be a `RouteKey` even when the host routes by a token only.
3. **The walk runs in two places.** `queryItems` walks inside the Durable Object with the `walk` callback.
   `scanItems` walks in the client with a loop of `point` steps. The two use different continuation values:
   `FokosRangeCursor` and `next: FokosStorageKey`.
4. **The client has a smaller model than the runtime.** `FokosShardingClient` has `resolve`, `resolveRange`,
   `resolveAll`, `point`, `range`, and `send`. `send` ignores its `_keys` argument (`client.ts:348`). The client has no
   fan-out, so `client/db.ts` (5 places), the coordinator, `gsi-writer.ts`, and `do-partition.ts` each group with
   `resolveAll` and merge the parts themselves.
5. **An empty `group` runs locally on the partition that receives it, also on a router** (`runtime.ts:615`).
   `txPrepare` has a handler branch for an empty request. It is a hidden rule of the shape, not a declared choice.

### 4.3 What already exists

`scanItems` milestone 1 did part of the work. `#resolvePosition` uses only `(routingHash, hk, sk)`, and a `RouteKey`
becomes a position in its first line. `storageInterval()`, `ownedStorageInterval()`, and `call.ownedSliceEnd(pos)`
use the storage coordinate. The route overrides, the learned slices, and the arena are keyed by routingHash or by
composite boundaries, not by hash key. The coordinate of section 2.4 (A1) is therefore already the internal
coordinate for points. Intervals are the part that remains.

## 5. Goals and requirements

### 5.1 In scope

- An operation declares its footprint, its execution, and its gates as three separate fields.
- The five shapes go. Every operation of the three hosts moves to the new form in the same change. This is a
  breaking change of the host API of `fokosdb/sharding/server`, by decision.
- One planner gives the visits of any footprint: items, hash keys, positions, and storage intervals, mixed in one
  request.
- One forward function with one fallback ladder serves every visit.
- One read-through gate checks any footprint against the caller slice, and plans it again at the source.
- `hooks.admit` gets the full footprint.
- The client resolves the same footprint types as the runtime.
- **A later span kind, execution kind, or gate is an additive change** for the host API, the wire, and the stored
  state (section 8).

### 5.2 Out of scope

- **A change of the wire protocol.** `op(ctx, req)`, the `fokosRpc` actions, and the envelope stay the same.
- **A change of stored state.** The route overrides, the learned slices, the arena, and the Bloom filter stay the same.
- **New user-facing operations.** This RFC enables them. Each one gets its own RFC.

### 5.3 Requirements

- Every guarantee of section 2.2 holds for every footprint and every execution.
- Each moved operation gives the same targets, the same errors, and the same answers as before the change.
- The property-based suites and the routing suites pass after each milestone. A test changes only for a renamed API.
- The sharding layer imports no FokosDB module, also not as `import type`.
- An unknown span kind or execution kind fails at construction with `sharding_operation_invalid`, never at request
  time.

## 6. Milestones

Each milestone ships on its own, and the full test suite passes after each one.

1. **The executor core.** Inside the runtime: one normalization of spans to storage intervals, one planner for
   points, one `#forwardVisit`, one read-through gate, and the `FokosExecutor` interface of section 7.2.5. The shapes
   still are the input, and the runtime maps each shape to an executor. No change of the host API and no change of
   behaviour. This milestone isolates the risk of the refactor from the risk of the API change.
2. **The composite frontier.** The planner plans storage intervals across routingHashes: local slices, hash children,
   and the range trees of promoted routingHashes. `planRangeFrontier` works on composite keys. The planner of an
   interval reads the topology cache, as the planner of a point does. The walk then reads through with the gate of
   the other executors, and its own branch in `#executeLocal` goes.
3. **The new descriptor (breaking).** The shapes, `FokosRangeInput`, `FokosRangeVisit`, `SkInterval`, and
   `FokosRangeCursor` leave the sharding layer. Every operation of the three hosts moves. `hooks.admit` gets `spans`
   in place of `keys`. The coordinator uses a `hash_key` span, and `apiScanItems` uses a `position` span. The position
   types get their new names (section 7.2.1): `FokosStorageKey` becomes `FokosStoragePosition`, and the internal
   `FokosStoragePosition` becomes `NormalizedPosition`.
4. **The client (breaking).** `FokosShardingClient.plan(spans)` and `send` replace `resolve`, `resolveRange`,
   `resolveAll`, `point`, and `range`. `fanOut` replaces the five copies of the grouping code.
5. **The first new consumer.** The `graphGetItems` step, with gets and queries in one `fan_out`, proves the design.

### 6.1 What milestone 1 built

The change is in `sharding/runtime.ts`, and one callback in `sharding/repartition-flow.ts`. The host API, the wire,
and the stored state do not change. The full test suite passes with no test change: 148 files and 2809 tests, and the
26 tests of `examples/http-api`, the same as before the change.

- **Spans.** `#spansOf` gives the footprint of each shape as `NormalizedSpan`s: an `item`, a `position`, or the
  `interval` of a `range` request. Each span carries its position, so a key is hashed one time for each request. The
  import gate, the admission (`admittedKeys`), and the learning (`routingHashesOf`) read spans. The
  `"routingHash" in key` check of the runtime is now in `#spansOf` only.
- **Compiled operations.** The constructor compiles each descriptor to a `CompiledOperation` with its execution kind
  (`compileOperation`), and checks it against `EXECUTION_RULES`. `#execute` is the only `switch` on the execution kind.
  `point` and `single_owner` are one executor, `one_owner`, with `onManyOwners` for `single_owner`. `group` is
  `fan_out`, and `range` is `walk`.
- **One point planner.** `#planPoints` plans the point spans of every point executor, with the Bloom guard.
  `#planInCallerSlice` is the same plan for a read-through.
- **One forward, one miss ladder.** `#forwardVisit` forwards the spans of a point visit, and `#forwardRangeVisit`
  forwards a range visit. Both use `#missLadder` with a `MissHint`. A planned range visit carries a hint, as a point
  resolution does.
- **One read-through path for the point executors.** `#executeLocal` runs the same executor as the owner, with an
  `ExecutionScope` of kind `readThrough`. The scope turns off the admission, `beforeForward`, and the signals, and it
  gives the local call the caller slice. The walk keeps its own read-through branch until milestone 2.
- **The epoch check.** A cutover increments `#ownershipEpoch` (`onCutover` of the repartition flow). A point plan
  records the epoch, and `#runLocalScope` and the local part of a `fan_out` check it with an invariant.

The executors are private methods of the runtime, not objects behind a context interface. The primitives that they
use (the planner, the forward, the admission, the local run) are private fields of the runtime. A new executor is one
entry in `EXECUTION_RULES`, one case in `compileOperation`, one case in `#execute`, and one method. Milestone 3 moves
the executors behind the `FokosExecutionContext` of section 7.2.5, together with the public descriptor.

### 6.2 What milestone 1 found

The refactor put the code of each shape next to the code of the other shapes. These differences had no reason, and
milestone 1 removes them:

| # | Finding | Before | After |
| --- | --- | --- | --- |
| 1 | The range forward had its own miss ladder. | A stale hash-arena hint on a range visit had no fallback: the error went to the caller. A point, a group, and a single-owner forward fell back. | `#forwardRangeVisit` uses `#missLadder`. A hash miss forgets the arena hint and plans again. |
| 2 | A range fallback that ran the visit here skipped steps. | It did not admit, and it dropped the signals of the handler, in the walk and in `forwardRangeVisit`. | It runs through the local function of the walk: it admits one time and keeps the signals. |
| 3 | The read-through of `group` used the wrong scope. | `ownedSliceEnd` of its local call gave the end of the owned slice of the source, not of the caller slice. | Each read-through local call has the caller slice. |
| 4 | The read-through of `group` applied signals on the source. | The read-through of `point` applied none. | No read-through applies a signal. |
| 5 | The read-through of a promoted `point` key had no miss fallback. | A stale learned slice gave an error. The read-through of `group` fell back. | Both use `#forwardVisit`. |
| 6 | `single_owner` could not read through. | `#validateOperations` refused it, because `#executeLocal` had no branch for it. | The rule is in `EXECUTION_RULES`, and `one_owner` reads through. |
| 7 | The Bloom guard grouped spans by hash key. | The Bloom filter answers for a routingHash. Two hash keys with one routingHash were two groups. | The guard groups by routingHash. It needs no hash of the key. |
| 8 | A forward hashed each key again for the learning. | `#forwardGroup` and `#forwardSingleOwner` hashed each key a second time. | Each span carries its routingHash. |

Two findings stay open, because a fix changes behaviour (sections 10.5 and 10.6):

- A walk can run a local visit after it awaited an earlier visit, and a cutover can come between the two.
- An empty `group` runs its handler here and lists no `executed` node in `servedBy`. Each other local run lists one.

Milestone 1 also changed two things in the design of section 7.2.5:

- The epoch check is an invariant, not the error code `sharding_cutover_raced`. A changed epoch is a defect of an
  executor, so `invariant_failed` is the correct answer, and the error catalog gets no new code.
- The Bloom rule on a retry stays a fact of each shape (`bloomOnRetry`): `point` and `range` keep the Bloom step, and
  `group` and `single_owner` turn it off. Milestone 3 gives each operation the rule of section 7.2.5, by the number of
  routingHashes of the visit.

## 7. Proposed solution

### 7.1 High-level overview

An operation has three independent parts:

1. **A footprint function** gives the spans of a request.
2. **An execution** says how the runtime runs the owners of the spans.
3. **Gates** say what the runtime does around the handler: `whileMigrating`, `readOnly`, `admissionTag`,
   `localMode`, `allowedWhileDestroying`.

Inside the runtime, every request goes through one pipeline. Only the executor step depends on the execution, and the
executor reaches partitions only through the primitives of the pipeline.

```
 req ──► footprint(req) ──► normalize ──► gates ──► executor ──► envelope
                            (every span  (import,   │  uses only:
                             becomes      admit)     ├─ plan(spans)       → visits
                             intervals)              ├─ runLocal(visit)     → the handler, inside the sync window
                                                     └─ forwardVisit(visit) → forward, miss ladder, learning
```

The shapes of today become these combinations:

| Shape today | Footprint | Execution |
| --- | --- | --- |
| `point` | one item, or one position | `one_owner` |
| `single_owner` | some items | `one_owner` with `onManyOwners: { answer }` |
| `group` | some items | `fan_out` with a failure policy |
| `range` | one interval inside one hash key | `walk` |
| `local` | none | `local` |

The new form adds the combinations that have no shape today:

| Use case | Footprint | Execution |
| --- | --- | --- |
| A coordinator request | one hash key | `one_owner` |
| `scanItems` step | one position | `one_owner` |
| `scanItems` walk inside a Durable Object | one interval across routingHashes | `walk` |
| `graphGetItems` step | items and intervals | `fan_out` |
| A count or a delete over an interval | one interval | `fan_out` |
| A batch of queries | some intervals | `walk` or `fan_out` |
| Each owner of the table, for backup | the interval of the whole table | `fan_out` |

### 7.2 Technical details

#### 7.2.1 The footprint

```ts
/** One element of a footprint. `data` is opaque host data: the runtime gives it back in the visit. */
export type FokosSpan<D = unknown> =
	/** An item. The runtime hashes `key.hashKey`. */
	| { kind: "item"; key: RouteKey; data?: D }
	/** Every position of one hash key, for a host that routes by a hash key with no sort key. */
	| { kind: "hash_key"; hashKey: KeyBytes; data?: D }
	/** A position that names no item, for example the start of a slice read. */
	| { kind: "position"; position: FokosStoragePosition; data?: D }
	/** A half-open storage interval. */
	| { kind: "interval"; interval: FokosStorageInterval; data?: D };

/** The spans of one request, in the order of the request. */
export type FokosFootprint<S extends FokosSpan = FokosSpan> = readonly S[];
```

Rules:

- Each span has an explicit `kind`. No code tells two kinds apart by the fields they have.
- The footprint function must be pure and deterministic (G10). The runtime calls it at each hop.
- **Each span kind normalizes to storage intervals, in one function.** An item and a position become a one-position
  interval. A hash key becomes `[(rh, hk), (rh, hk || 0x00))`. The planner, the read-through gate, the containment
  check, and the learning work on intervals only. A one-position interval takes the `#resolvePosition` fast path.
- A forwarded request must have a footprint inside the scope of its target. The executor makes sure of this with
  `split`, so `partition_misrouted` always means a defect (G2).

**The public form and the normalized form.** Each storage type has two forms. The public form comes from a host or
from an RPC, so the runtime cannot trust it. The normalized form is checked one time, at the edge of the pipeline, and
only the runtime uses it.

| Concept | Public form (exported) | Normalized form (internal) |
| --- | --- | --- |
| A position | `FokosStoragePosition`: `{ routingHash: Uint8Array; hk?; sk? }` | `NormalizedPosition`: a checked 8-byte `RoutingHash`, `hk` and `sk` always present, and a `FokosCompositeKey` |
| An interval | `FokosStorageInterval`: bounds of `FokosStoragePosition` | bounds of `NormalizedPosition` |
| A span | `FokosSpan` | `NormalizedSpan`: the span, its normalized intervals, and its routingHash interval |
| A footprint | `FokosFootprint` | `readonly NormalizedSpan[]` |

- **The names today.** `FokosStorageKey` is the public position, and `FokosStoragePosition` is the normalized position
  (`runtime-types.ts:104` and `115`). Milestone 3 renames both. The word "key" then means an item key only, and a
  position no longer looks like a `RouteKey`. That likeness is what let `"routingHash" in key` decide the kind.
- **One check.** `storagePositionOf` becomes the normalization of a position. It checks the length of `routingHash`
  with `asRoutingHash`, and gives the empty key to an absent `hk` or `sk`. Only the normalized form reaches the
  planner, the read-through gate, and the learning.
- **Why `routingHash` stays `Uint8Array` in the public form.** The value comes from a host or over RPC, and the brand of
  `RoutingHash` is a type only, which an RPC does not keep. The comment of `FokosStorageKey` today gives another reason:
  "a bound can also be the empty byte string". No code outside the tests makes such a bound, and `asRoutingHash`
  refuses it, so the new comment drops that reason.
- `NormalizedPosition` and `NormalizedSpan` are not exported. A host gets the public form back in `split`, in
  `FokosLocalCall.spans`, and in `hooks.admit`.

**Intervals with inclusive bounds.** `KeyCodec.compare` is an unsigned byte compare (`key-codec.ts:103`). In this
order, the smallest key above `s` is `s || 0x00`. So:

- An inclusive upper bound `u` becomes the exclusive end `u || 0x00`.
- An exclusive lower bound `l` becomes the inclusive start `l || 0x00`.
- The end of all sort keys of hash key `hk` is the position `(rh, hk || 0x00)`.

A new `KeyCodec.successor(bytes)` does this, because only `KeyCodec` synthesizes keys. A cursor narrows the interval
before the footprint function returns it. A direction is a parameter of `walk`, not of the footprint. `SkInterval`,
`FokosRangeCursor`, and `projectOnHashKey` move to the FokosDB query code, or go.

A helper builds the interval of one hash key, so that a host does not build positions by hand:

```ts
/** The positions of hash key `hk` with `lower <= sk < upper`. An absent bound is open. */
function hashKeyInterval(routingHash: RoutingHash, hk: KeyBytes, lower?: KeyBytes, upper?: KeyBytes): FokosStorageInterval;
```

#### 7.2.2 The execution

```ts
export type FokosExecution<Req, Res, S extends FokosSpan> =
	/** All spans must have one owner. The owner gets the whole request. */
	| { kind: "one_owner"; onManyOwners?: "misrouted" | { answer: Res } }
	/** Each owner gets one sub-request with its spans. The answers are merged. */
	| {
			kind: "fan_out";
			split(req: Req, spans: readonly S[]): Req;
			merge(parts: Array<FokosPart<Req, Res>>): Res;
			failurePolicy: "fail_fast" | "attempt_all" | "collect";
			/** A request with no spans: "local" runs the handler here, and `{ answer }` answers with no handler. */
			onEmpty: "local" | { answer: Res };
	  }
	/** The host visits the owners in storage order, and owns the budget and the early exit. */
	| {
			kind: "walk";
			order(req: Req): "asc" | "desc";
			split(req: Req, spans: readonly S[]): Req;
			walk(input: FokosWalkInput<Req, Res, S>): Promise<Res>;
	  }
	/** No footprint: this partition runs the handler. */
	| { kind: "local" };
```

- **`one_owner`.** The default `onManyOwners: "misrouted"` fits an item, a hash key that is not promoted, and a position.
  The single-shot transaction uses `{ answer: notApplicable }`. Before the runtime answers with `onManyOwners`, it resolves
  again with the Bloom step off, as `#singleOwner` does today. An error reaches the caller unchanged.
- **`fan_out`.** `split` replaces `subRequest` and `clip`. In a sub-request, an interval span has the clipped interval
  of its visit, and an item span is unchanged. One owner gets one sub-request for all its spans, items and intervals
  together (G6). The `attempt_all` error stays `partition_fanout_failed`. Each `fan_out` operation must be idempotent
  for each span (G7). `onEmpty` has no default, so the rule that `txPrepare` uses today is now a declared choice.
- **`walk`.** `FokosWalkInput` has `local` and `forward` functions, as today. `visits` becomes an
  `Iterable<FokosVisit<S>>` and not an array, because an interval across many promoted routingHashes can have many
  visits. The host stops the iteration when its budget is used up. A walk is `readOnly: true` for now (section 10).
- **`local`.** The `local` shape of today. The destroy fence and the import gate apply as today.

`S` is the span type of the footprint function of the operation. `split`, `merge`, and `walk` get only that type
(section 8.3).

#### 7.2.3 The visit

```ts
export type FokosVisit<S extends FokosSpan = FokosSpan> = {
	target: FokosPartitionRef | "local";
	/** The spans that this visit serves. An interval is clipped to the scope of the target. */
	spans: readonly S[];
	speculative: boolean;
};
```

`FokosVisit` replaces `FokosRangeVisit`, whose `start` and `end` are sort keys of one hash key. The `rangeVisits` and
`forwardRangeVisit` primitives become `plan(spans)` and `forwardVisit(visit, op, req)`.

#### 7.2.4 The planner

One function, `#plan(intervals, scope, opts)`, gives the visits of a footprint. `scope` is the owned interval of the
partition, or the caller slice during a read-through.

- **A one-position interval** resolves with `#resolvePosition`, as today.
- **An interval on a hash owner** is cut at each cut-over route override inside it. The parts between overrides are
  `local` visits. Each promoted routingHash `P` gets the visits of its range tree for `[P, P + 1)`, planned with the
  learned slices of `P`. `firstCutOverRouteOverride` gives the next override in one seek, as `ownedSliceEnd` does.
- **An interval on a hash router** is cut by the routingHash intervals of the children. Each part goes to the deepest
  known partition that holds the whole part: the hash arena, then the topology cache, then the direct child. The
  overrides of the router apply first, as in `#resolvePosition`.
- **An interval on a range partition** uses `planRangeFrontier` on composite keys. The learned slices already have
  composite boundaries, so the projection on one hash key goes. The rule of G4 stays: a learned slice replaces its
  base only when the slice holds the whole segment.
- **Grouping.** The planner merges the spans of one target into one visit, and runs `#guardBloomHits` over all
  spans, so G6 holds for mixed footprints.

The planner is lazy for intervals. It yields visits in storage order, and it reads storage only for the visits that
the caller takes.

#### 7.2.5 The executor contract

Each execution kind is one `FokosExecutor` inside the runtime. The executor is the only part of the pipeline that
knows the kind.

```ts
/** Internal. One execution kind. */
interface FokosExecutor<E extends { kind: string }> {
	readonly kind: E["kind"];
	/** The rules of this kind, checked once at construction. */
	readonly rules: {
		spanKinds: ReadonlyArray<FokosSpan["kind"]> | "any";
		readSource: "allowed" | "refused";
		needsReadOnly: boolean;
	};
	/** Extra checks of the descriptor. Throws `sharding_operation_invalid`. */
	validate(execution: E): void;
	run(x: FokosExecutionContext, execution: E): Promise<unknown>;
}

/** What an executor can do. It cannot read storage or make a stub. */
type FokosExecutionContext = {
	op: string;
	req: unknown;
	spans: readonly NormalizedSpan[];
	plan(spans: readonly NormalizedSpan[], opts?: { bloom?: boolean }): Iterable<PlannedVisit>;
	/** Runs the handler here. Throws `sharding_cutover_raced` when the ownership epoch changed since the plan. */
	runLocal(visit: PlannedVisit, req: unknown): unknown;
	/** One forward with the miss ladder and the learning. A miss gives the visits of a new plan to the executor. */
	forwardVisit(visit: PlannedVisit, req: unknown): Promise<FokosForwardResult>;
	admit(spans: readonly NormalizedSpan[]): void;
	collector: RouteCollector;
};
```

What the contract gives:

- **Guarantees by construction.** The executor gets the import gate, the admission, the planner, the forward, the
  fallback, and the learning from the context. G1, G2, G4, G7, G8, and G11 therefore hold for each executor, the new
  ones too.
- **G3 as a check, not only a rule.** The cutover transaction increments an in-memory ownership epoch. `plan` records
  the epoch in each visit. `runLocal` fails an invariant when the epoch changed. A `walk` is the exception: its local
  visits can run after an `await` (section 10.5). Today G3 depends on each dispatch function to keep
  the plan and the local write in one synchronous block. With the check, a new executor that awaits between the two
  fails in a test, and it never writes on a partition that lost the key.
- **One miss ladder.** `forwardVisit` forgets the hint and plans the spans of the visit again. It returns the new
  visits to the executor. The executor decides what they mean: `one_owner` answers `onManyOwners` for more than one visit,
  `fan_out` runs each one as a part, and `walk` puts them in the place of the failed visit. The Bloom step stays on
  when all spans of the visit have one routingHash, and turns off otherwise.
- **Data-driven validation.** The constructor checks each descriptor against the `rules` of its executor. A new
  executor brings its own rules, and no table elsewhere changes.

#### 7.2.6 One read-through gate

On the importing target, `#whileImporting` checks each normalized interval against the scope. Then it sends
`executeLocal` to the source, as today.

On the source, `#executeLocal` derives the spans from the request, checks them against the caller slice, and runs
the executor of the operation with `plan` limited to the caller slice and the `EXACT` options. The planner already
sends a promoted routingHash of a `hash_child` caller slice to its range tree, and it clips intervals to the caller
slice. This replaces the three branches of `#executeLocal`. The gate does not know the execution kind, so a new
executor gets the read-through with no new code. The `executeLocal` request does not change.

A read-through admits nothing, runs no `beforeForward`, and applies no signal: the target did the first two, and the
handler serves the slice of another partition. Its local call gets the caller slice, so `ownedSliceEnd` ends at the
caller slice. A `one_owner` operation with `onManyOwners` reads through like any other executor.

#### 7.2.7 Admission, local calls, and learning

`hooks.admit` gets the spans of the local part in place of `keys`:

```ts
admit?(input: {
	op: string;
	admissionTag?: string;
	/** The spans of the local part. Intervals are clipped to this partition. */
	spans: readonly FokosSpan[];
	lifecycle: FokosLifecycle;
	policy: TPolicy;
}): "allow" | { reject: Error };
```

The FokosDB hooks change in two lines. The partition hook tests `spans.length === 0` in place of
`keys.length === 0`. The coordinator hook reads `spans[0].hashKey` from its `hash_key` span.

`FokosLocalCall` is the place for per-call facts. It gets `spans`, the part of the footprint that this call
serves, next to `signal` and `ownedSliceEnd`.

`#learn` takes the routingHash intervals of the spans in place of a list of routingHashes. An item or a position
gives a one-hash interval, so the learning of points does not change.

#### 7.2.8 The descriptor

```ts
export type FokosOperation<Req, Res, S extends FokosSpan = FokosSpan> = FokosOperationGates<Req, Res> & {
	/** Absent only for the `local` execution. */
	footprint?(req: Req): FokosFootprint<S>;
	execution: FokosExecution<Req, Res, S>;
};
```

`FokosOperationGates` holds `whileMigrating`, `readOnly`, `admissionTag`, `localMode`, `allowedWhileDestroying`,
`local`, `beforeForward`, and `forward`. Each gate is optional or has a default, so a later gate is a new optional
field (section 8.2).

Examples, with the FokosDB operations of today:

```ts
apiGetItem: {
	footprint: (req) => [{ kind: "item", key: { hashKey: req.hashKey, sortKey: req.sortKey } }],
	execution: { kind: "one_owner" },
	whileMigrating: "read_source", readOnly: true, admissionTag: "read",
	local: (req) => this.readItemLocally(req),
},
apiScanItems: {
	footprint: (req) => [{ kind: "position", position: req.start }],
	execution: { kind: "one_owner" },
	whileMigrating: "read_source", readOnly: true, admissionTag: "read",
	local: (req, call) => this.scanItemsLocal(req, call),
},
txExecuteSingleShot: {
	footprint: (req) => req.items.map((item) => ({ kind: "item", key: keyOf(item) })),
	execution: { kind: "one_owner", onManyOwners: { answer: { outcome: "not_applicable" } } },
	whileMigrating: "throw", admissionTag: "write",
	local: (req, call) => this.singleShotLocal(req, call),
},
txCommit: {
	footprint: (req) => req.items.map((item) => ({ kind: "item", key: keyOf(item), data: item })),
	execution: {
		kind: "fan_out",
		failurePolicy: "attempt_all",
		onEmpty: "local",
		split: (req, spans) => ({ ...req, items: spans.map((t) => t.data!) }),
		merge: (parts) => ({ outcome: "committed", partitions: parts.flatMap((p) => p.result.partitions) }),
	},
	whileMigrating: "throw", admissionTag: "ignore_size_reject",
	local: (req, call) => this.commitLocal(req, call),
},
apiQueryItems: {
	// The host builds the interval from the hash key, the sort-key condition, and the cursor.
	footprint: (req) => [{ kind: "interval", interval: queryStorageInterval(req, this.fokos) }],
	execution: {
		kind: "walk",
		order: (req) => req.direction,
		split: (req, spans) => clipQueryToInterval(req, spans[0].interval),
		walk: (input) => walkQueryVisits(input),
	},
	whileMigrating: "read_source", readOnly: true, admissionTag: "read",
	local: (req) => this.queryItemsLocal(req),
},
initiateWrite: {
	footprint: (req) => [{ kind: "hash_key", hashKey: encodeHashKey(req.clientRequestToken, DEFAULT_LIMITS) }],
	execution: { kind: "one_owner" },
	whileMigrating: "throw", localMode: "async", admissionTag: "write",
	local: async (req) => await this.initiateWriteLocal(req),
},
```

#### 7.2.9 The client

`FokosShardingClient` gets the same model, and its old resolve methods go:

- `plan(spans, opts?)` gives `Array<{ ctx; spans; indexes }>`. A one-position span goes to the deepest known
  partition of its position. An interval goes to the deepest known partition that holds the whole interval. Spans
  with the same entry form one group.
- `send(op, entry, req, opts?)` sends one request to one entry, with the topology fallbacks and the retry policy of
  today. The unused `keys` argument goes.
- `fanOut(op, spans, req, split, opts?)` sends one request for each group in parallel, and gives the parts back. It
  replaces the grouping code in `client/db.ts`, the coordinator, `gsi-writer.ts`, and `do-partition.ts`.
- The normalization of spans is one module that the client and the runtime both import, so a new span kind is one
  change for both sides.

The client does not import a descriptor. A host that wants one footprint function on both sides puts it in a shared
module, next to the request type.

#### 7.2.10 Performance

- **Points.** The pipeline adds one array of one span and one compiled-descriptor lookup. Milestone 1 must show no
  regression with the `profile-worker-local` skill on `getItem` and `putItem`.
- **Groups.** The cost is the same as today: one resolution for each span and one guard pass.
- **Intervals.** A plan costs one override seek for each promoted routingHash in the interval, plus one learned-slice
  list for each range tree that it enters. The lazy planner pays only for the visits that the walk takes.

#### 7.2.11 Testing

- A property test, for random topologies with hash splits, range splits, and promotions: for each footprint, the
  visits of `#plan` cover each span exactly once (G4), in storage order, and each visit is inside the scope of its
  target (G2).
- A property test, run once against the code before milestone 3: each moved operation gives the same targets, the
  same errors, the same answers, and the same admission decisions as its shape.
- A test that an executor that awaits between `plan` and `runLocal` across a cutover gets `sharding_cutover_raced`.
- A test for each rule of each executor (section 7.2.5).
- The suites of `queryItems`, `scanItems`, the transactions, and the routing pass after each milestone.

## 8. Future compatibility: adding a dispatch kind later

This change breaks the host API one time. After it, a new kind of dispatch must be an additive change. This section
gives the rules that make it additive, for each of the four places where a change can break something.

### 8.1 The three extension points

A "new kind of dispatch" is a change on one of three axes. Each axis has one extension point.

| Axis | Examples | Extension point | What else changes |
| --- | --- | --- | --- |
| What the request reads or writes | `hash_key` (in this RFC), a key prefix, a routingHash range | a span kind: one normalization to intervals | nothing |
| How the owners run | a local continuation for `graphGetItems`, a paged fan-out, a walk that writes | a `FokosExecutor` | nothing |
| What happens around the handler | a new `whileMigrating` value | an optional gate with a default | the gates code only |

The planner, the read-through gate, the admission, the learning, and the envelope work on normalized intervals and
on visits. They do not know the span kind or the execution kind. This is what makes each axis additive.

### 8.2 The host API (TypeScript)

1. **The unions that a host builds are open for new members.** `FokosSpan` and `FokosExecution` get new members.
   Code that builds an old member still compiles.
2. **The values that a host receives have the types that the host declared.** `split`, `merge`, `walk`, and
   `FokosLocalCall.spans` get `S`, the span type of the footprint function of the operation. A new span kind
   therefore never reaches the code of an operation that does not use it, and an exhaustive `switch` in host code
   stays exhaustive.
3. **The values that the runtime makes are open, and the documentation says so.** `FokosServedRole`, the error codes,
   and `FokosVisit.target` can get new members. A consumer must have a default branch. The doc comment of each such
   type says this.
4. **Every new input is a field of an options object**, with a default that keeps the old behaviour. No new
   positional argument.
5. **An unknown kind fails at construction** with `sharding_operation_invalid`. A host that uses a new kind with an
   old runtime fails at deploy time, never at request time.

### 8.3 The wire (two versions during a deploy)

During a gradual deploy, a router of version N can forward to a target of version N + 1. The rules:

1. **The wire carries no kind and no plan.** A forward is `stub[op](targetCtx, req)`. A read-through is
   `fokosRpc("executeLocal", { op, repartitionId, caller, request })`. Each hop derives the spans and the execution
   from its own descriptor of `op` (G10).
2. **The meaning of a request at its target does not depend on the execution kind.** The contract of every executor
   is: a target gets a request whose footprint is inside its scope, and serves exactly that footprint. A target
   therefore never needs to know which executor sent the request.
3. **The footprint function and the execution of an operation never change in place.** A router of version N and a
   router of version N + 1 must plan the same request the same way. A new dispatch for an existing operation is a new
   operation name, for example `apiScanItemsWalk`, or a field of the request that both versions read. The old name
   stays until no caller of the old version runs.
4. **A new operation must reach every Durable Object before a caller uses it.** This rule exists today for any new
   method.
5. **The envelope gets optional fields only.** A value that one executor needs, for example a continuation position,
   goes in the host value, not in `FokosRouting`.

### 8.4 Stored state

The executors store nothing. A new executor that must store a fact, for example the progress of a paged fan-out, uses
a new `fokos_` table or a new `__fokos/` key, and never changes the format of an existing one.

### 8.5 The escape hatch

The context primitives `plan`, `forwardVisit`, and `runLocal` are also public on the runtime, with the same epoch
check. A host can then build a new kind of dispatch inside a `local` operation before the runtime has an executor for
it. The guarantees of section 7.2.5 hold, because the primitives give them. When the dispatch becomes an executor, the
operation gets a new name (rule 3 of section 8.3).

### 8.6 Checklist for a new kind

1. Add the member to `FokosSpan` or to `FokosExecution`, with a doc comment.
2. For a span kind: add its normalization to intervals in the shared module, and a case in the coverage property
   test.
3. For an execution kind: add one `FokosExecutor` with its `rules`. Use only the context primitives.
4. Add the validation tests of its rules, and the property test for G4 and G2 with the new kind.
5. Do not change the planner, the read-through gate, the admission, the learning, or the envelope. A change there means
   that the kind needs a new primitive, and that primitive needs its own RFC.

### 8.7 Two worked examples

**A span kind: `hash_key`.** This RFC adds it for the coordinator. Its normalization is
`[(rh, hk), (rh, hk || 0x00))`. The planner, the gate, and the learning need no change. The coordinator stops making up
a sort key, and its admission hook reads `spans[0].hashKey`.

**An execution kind: the local continuation of `graphGetItems`** (`2026-11-27-graph-get-items.md`, section 4.2.12).
The handler reads its spans, and returns derived reads as new spans. The executor `continue` runs:

1. `plan` the spans, and run the local visit with `runLocal`.
2. Take the new spans from the answer, and `plan` them. Run the local visits in the same synchronous block, until no
   local span is ready or the answer is full.
3. Return the remaining spans as the frontier, or forward them with `forwardVisit`.

The executor uses only the context primitives. The epoch check replaces the rule "no checker is kept across an
`await`" of that RFC. No other part of the runtime changes, and the wire gets no new field.

## 9. Alternative options

- **Add a shape for each new use case.** Rejected. Each shape adds a resolver, a forward function, a fallback, a
  read-through branch, and an admission rule. The cost grows with the product of shapes and concerns.
- **Keep the shapes as presets of the new form.** Rejected. Backwards compatibility is not a requirement now, and two
  forms of one descriptor double the API that section 8 must keep stable.
- **The runtime gives only `plan` and `forwardVisit`, and the host runs every execution.** Rejected as the main API.
  The host then must keep G3, G6, G7, and G9 itself. The design keeps these primitives as the escape hatch
  (section 8.5).
- **Put the footprint or the execution kind on the wire.** Rejected. It changes the wire, it sends the same bytes twice,
  and a router then trusts a footprint that can disagree with the request. A kind on the wire also makes each new kind
  a wire change, which is what section 8 avoids.
- **A plugin API for executors that hosts register.** Rejected for now. An executor keeps G3, G6, and G7 only when it
  uses the primitives correctly, and the escape hatch already lets a host try a new dispatch. An executor moves into
  the runtime after it is proved.
- **Always walk in the client.** Rejected. A walk inside a Durable Object saves one round trip for each visit. The
  place of the walk is a choice of each operation.

## 10. Open questions

### 10.1 The fan-out width of a large interval

A `fan_out` over the whole table reaches each owner. A router fans out to at most `hashSplitN` children, plus one range
tree for each promoted routingHash inside its interval. The number of promoted routingHashes has no bound today.
Options: a cap with an error, or a paged fan-out executor. The answer decides if a backup can use `fan_out`.

### 10.2 A walk that writes

A delete over an interval is a `walk` or a `fan_out` with `readOnly: false` and `whileMigrating: "throw"`. It must be
idempotent for each span, and it must stop at a budget. Option: allow `fan_out` writes over intervals first, and keep
`walk` read-only.

### 10.3 Data on the span or an index

`data` keeps the shape of `items(req)` today. An index into the footprint needs no generic type for the data. Option:
give `split` both the spans and their indexes.

### 10.4 Admission of intervals

A host can then refuse a scan on a partition that is over its size. The host must decide which tags refuse an
interval, because a refused read on a full partition does not make it smaller.

### 10.5 A local visit of a walk after a cutover

A walk plans its visits one time, and the host awaits each forward before the next visit. When a split of this
partition cuts over during the walk, a later local visit still reads the rows of this partition. Writes go to the
children after the cutover. A child refuses writes while it imports, so the read is stale only when the import of a
child completes during the walk. Options:

- The `local` function of the walk checks the epoch. When it changed, the visit is planned again and forwarded. This
  needs a walk that can fold more than one answer for one visit.
- The `local` function throws `partition_migrating` when the epoch changed, and the caller retries the page.

The second option is small and keeps G3. It changes the answer of a rare race from a stale read to a retry.

### 10.6 The empty fan-out

An empty `group` request runs the handler on the partition that receives it, also on a router, and lists no
`executed` node. `txPrepare` has a handler branch for it. With `onEmpty` (section 7.2.2), each operation declares this
behaviour. Option: an empty request with `onEmpty: "local"` lists `executed`, as every other local run does.

## 11. Frequently asked questions

**Does every host change?**
Yes, in milestone 3. The three FokosDB hosts move in the same change. The wire and the stored state do not change, so
a deploy needs no migration.

**Is `point` gone?**
Yes, as a name. A point is a footprint of one item, one hash key, or one position, with `one_owner`.

**Why does `single_owner` not become a `fan_out`?**
Because it needs the whole request in one synchronous handler call, for atomicity. A `fan_out` gives each owner a part.
`one_owner` with `onManyOwners` says the rule directly.

**Why keep positions when intervals exist?**
A scan step needs "the owner of this position, which serves its slice from there". An interval of `one_owner` can have
more than one owner and then gives `onManyOwners`. A position has one owner, so it says the rule of the step directly.

**Why does a new dispatch for an existing operation need a new name?**
Because each hop plans the request with its own descriptor. During a deploy, two versions of one operation can be on
one route, and they must plan the same way.

**Does the design change the order of rows?**
No. The storage order `(routingHash, hk, sk)` and the tree order stay the same. The design only lets an interval cross
routingHashes in the planner, which `scanItems` already does in the client.
