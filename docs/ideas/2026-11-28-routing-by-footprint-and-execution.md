# RFC — Routing by footprint and execution in `FokosShardingRuntime`

**State:** Draft
**Date:** 2026-11-28
**Author:** Lambros
**Status:** Nothing is built. This document gives the design that section 8 of
`docs/agent-plans/2026-11-20-scan-items.md` asks for.

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
- [8. Compatibility: why this is not a breaking change](#8-compatibility-why-this-is-not-a-breaking-change)
- [9. Alternative options](#9-alternative-options)
- [10. Open questions](#10-open-questions)
- [11. Frequently asked questions](#11-frequently-asked-questions)

## 1. Overview and context

### 1.1 The problem

A host declares each operation with one dispatch shape: `point`, `group`, `single_owner`, `range`, or `local`
(`FokosOperation` in `sharding/runtime-types.ts:288`). A shape answers two questions at once:

1. **Footprint.** What part of the storage order does the request touch?
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
changes this contract, and each change touches about 19 `descriptor.shape` branches in `runtime.ts`. The design must
be decided before the next consumer, so that the change is additive and not a breaking change.

### 1.3 Glossary

- **Position** — a place in the storage order `(routingHash, hk, sk)`. Type `FokosStorageKey`. An absent `hk` or `sk`
  is the empty key, the minimum.
- **Storage interval** — a half-open interval `[start, end)` of positions. Type `FokosStorageInterval`.
- **Scope** — the storage interval that one partition can own, from its identity (`storageInterval()`).
- **Touch** — one element of a footprint: an item, a position, or a storage interval.
- **Footprint** — the list of touches of one request.
- **Visit** — one target partition, or `local`, with the touches that it serves. A planner gives a list of visits.
- **Execution** — the rule that runs the visits of one request: `one_owner`, `fan_out`, or `walk`.

## 2. What routing must give

This section lists what the routing layer must give, independent of the API. Section 7 must keep each item.

### 2.1 The questions that routing answers

| # | Question | Exact or hinted |
| --- | --- | --- |
| Q1 | Which partition owns position `p` now? | Exact |
| Q2 | Which partition do I send `p` to, with the fewest hops? | Hinted: a cache can be wrong, and a miss falls back |
| Q3 | Which disjoint segments of an interval does each owner hold, in storage order? | Exact cover, hinted targets |
| Q4 | Which touches of one request have the same owner? | Exact grouping |
| Q5 | What does this partition own: its scope, the end of its owned slice, and does it own key `k`? | Exact |
| Q6 | During an import: which source answers my reads, and inside which caller slice? | Exact |
| Q7 | Which partitions served this request, and how many forwards did it cost? | Evidence for learning and metering |

### 2.2 Safety guarantees

- **G1 One owner.** At each instant, each position has exactly one owner. The topology, the cut-over route overrides,
  and the repartition state decide it.
- **G2 No misroute.** A partition never runs a handler for a touch outside its scope. It throws
  `partition_misrouted`, and does not apply the request.
- **G3 No cutover race.** The ownership decision, the admission, and the local write of a synchronous handler run in
  one synchronous block. The first `await` is a remote call.
- **G4 Exact cover.** The visits of an interval cover it with no gap and no overlap. A learned slice is used only when
  it holds the whole segment (`2026-09-20-query-entry-point-into-a-range-tree.md`).
- **G5 Stable order.** An ordered walk visits owners in storage order. A repartition does not move a position, so a
  continuation position stays valid across splits and promotions.
- **G6 One call per owner.** The touches of one request that have one owner reach it in one sub-request. `txCommit`
  needs this rule (`commit_keyset_mismatch`).
- **G7 Safe fallback.** A cache-miss fallback starts only after an error that proves that no handler ran on the
  target. A fan-out operation must be idempotent for each touch, because a router can apply one part before another
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
  when the touches have more than one hash key".
- A `point` error reaches the caller unchanged. A remote failure of an `attempt_all` group becomes
  `partition_fanout_failed`.
- A `point` has no `subRequest` and no `merge`.
- A `point` key can be a position. A `group` item is a `RouteKey` only.

**Claim 3: `single_owner` is a `group` with a stricter policy.** Not quite. `single_owner` is not a fan-out. It needs
the whole request in one synchronous handler call on one owner, for atomicity, and it answers `notApplicable` when the
keys span more than one partition. It is a precondition on the execution: "one owner, or decline". It also cannot read
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

- An operation declares its footprint and its execution as two separate fields.
- One planner gives the visits of any footprint: items, positions, and storage intervals, mixed in one request.
- One forward function with one fallback ladder serves every visit.
- One read-through gate checks any footprint against the caller slice, and plans it again at the source.
- `hooks.admit` gets the full footprint.
- The client resolves the same footprint types as the runtime.
- The five current shapes keep working, with the same behaviour, as presets of the new form.

### 5.2 Out of scope

- **A change of the wire protocol.** `op(ctx, req)`, the `fokosRpc` actions, and the envelope stay the same.
- **A change of stored state.** The route overrides, the learned slices, the arena, and the Bloom filter stay the same.
- **New user-facing operations.** This RFC enables them. Each one gets its own RFC.
- **A removal of the shapes.** A later major version can remove them, after every host moved.

### 5.3 Requirements

- Every guarantee of section 2.2 holds for every footprint and every execution.
- The property-based suites and the routing suites pass without a change after each milestone.
- The sharding layer imports no FokosDB module, also not as `import type`.
- An unknown footprint kind or execution kind fails at construction with `sharding_operation_invalid`, never at
  request time.

## 6. Milestones

Each milestone ships on its own, and the full test suite passes after each one.

1. **One internal pipeline.** The runtime compiles each shape into a footprint and an execution at construction. One
   planner for items and positions, one `#forwardVisit`, and one read-through gate replace the per-shape functions.
   No API change and no behaviour change.
2. **The composite frontier.** The planner plans storage intervals across routingHashes: local slices, hash children,
   and the range trees of promoted routingHashes. The `range` preset converts its `SkInterval` and cursor to a storage
   interval in a deprecated adapter.
3. **Public descriptors.** Hosts can declare `footprint` and `execution`. `hooks.admit` gets `touches`.
   `apiScanItems` moves to a `position` touch, and the `"routingHash" in key` checks go.
4. **The client.** `FokosShardingClient.plan(touches)` replaces `resolve`, `resolveRange`, and `resolveAll`, which stay
   as wrappers. An optional `fanOut` helper replaces the five copies of the grouping code.
5. **The first new consumer.** The `graphGetItems` step, with gets and queries in one `fan_out`, proves the design.
6. **Host migration.** Each FokosDB operation moves to the new form. The shapes stay as deprecated presets.

## 7. Proposed solution

### 7.1 High-level overview

An operation has three independent parts:

1. **A footprint function** gives the touches of a request, in the storage coordinate.
2. **An execution** says how the runtime runs the owners of the touches.
3. **Gates** say what the runtime does around the handler: `whileMigrating`, `readOnly`, `admissionTag`, `localMode`.

Inside the runtime, every request goes through one pipeline. Only step 5 depends on the execution.

```
 req ──► footprint(req) ──► normalize ──► gate ──► plan ──► execute ──► learn ──► envelope
                            (positions,   (import, (visits:   (one_owner |     (servedBy
                             intervals)    admit)   target +   fan_out |        against the
                                                    touches)   walk)            touches)
                                                       ▲           │
                                                       └─ fallback ┘  one miss ladder for each visit
```

The five shapes become presets:

| Shape today | Footprint | Execution |
| --- | --- | --- |
| `point` | one item, or one position | `one_owner` |
| `single_owner` | some items | `one_owner` with `onSpan: { answer }` |
| `group` | some items | `fan_out` with a failure policy |
| `range` | one interval inside one hash key | `walk` |
| `local` | none | here |

The new form adds the combinations that have no shape today:

| Use case | Footprint | Execution |
| --- | --- | --- |
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
export type FokosTouch<D = unknown> =
	/** An item. The runtime hashes `key.hashKey`. Admission sees the key. */
	| { kind: "item"; key: RouteKey; data?: D }
	/** A position that names no item, for example the start of a slice read. */
	| { kind: "position"; position: FokosStorageKey; data?: D }
	/** A half-open storage interval. */
	| { kind: "interval"; interval: FokosStorageInterval; data?: D };
```

Rules:

- Each touch has an explicit `kind`. No code tells two kinds apart by the fields they have.
- The footprint function must be pure and deterministic (G10). The runtime calls it at each hop.
- An item and a position resolve to one owner. An interval can have more than one owner.
- A forwarded request must have a footprint inside the scope of its target. The planner makes sure of this with
  `split`, so `partition_misrouted` always means a defect (G2).

**Intervals with inclusive bounds.** `KeyCodec.compare` is an unsigned byte compare (`key-codec.ts:103`). In this
order, the smallest key above `s` is `s || 0x00`. So:

- An inclusive upper bound `u` becomes the exclusive end `u || 0x00`.
- An exclusive lower bound `l` becomes the inclusive start `l || 0x00`.
- The end of all sort keys of hash key `hk` is the position `(rh, hk || 0x00)`.

A new `KeyCodec.successor(bytes)` does this, because only `KeyCodec` synthesizes keys. A cursor narrows the interval
before the footprint function returns it. A direction is a parameter of `walk`, not of the footprint.
`SkInterval`, `FokosRangeCursor`, and `projectOnHashKey` then leave the public types of the sharding layer.

A helper builds the interval of one hash key, so that a host does not build positions by hand:

```ts
/** The positions of hash key `hk` with `lower <= sk < upper`. An absent bound is open. */
function hashKeyInterval(routingHash: RoutingHash, hk: KeyBytes, lower?: KeyBytes, upper?: KeyBytes): FokosStorageInterval;
```

#### 7.2.2 The execution

```ts
export type FokosExecution<Req, Res> =
	/** All touches must have one owner. The owner gets the whole request. */
	| { kind: "one_owner"; onSpan?: "misrouted" | { answer: Res } }
	/** Each owner gets one sub-request with its touches. The answers are merged. */
	| {
			kind: "fan_out";
			split(req: Req, touches: readonly FokosTouch[]): Req;
			merge(parts: Array<FokosPart<Req, Res>>): Res;
			failurePolicy: "fail_fast" | "attempt_all" | "collect";
			/** A request with no touches. "local": run here, as `group` does today. Default: "local". */
			onEmpty?: "local" | { answer: Res };
	  }
	/** The host visits the owners in storage order, and owns the budget and the early exit. */
	| {
			kind: "walk";
			order(req: Req): "asc" | "desc";
			split(req: Req, touches: readonly FokosTouch[]): Req;
			walk(input: FokosWalkInput<Req, Res>): Promise<Res>;
	  };
```

- **`one_owner`.** `point` uses the default `onSpan: "misrouted"`: an item or a position cannot span. `single_owner`
  uses `{ answer: notApplicable }`. Before the runtime answers with `onSpan`, it resolves again with the Bloom step
  off, as `#singleOwner` does today. An error reaches the caller unchanged.
- **`fan_out`.** `split` replaces `subRequest` and `clip`. In a sub-request, an interval touch has the clipped
  interval of its visit, and an item touch is unchanged. One owner gets one sub-request for all its touches, items and
  intervals together (G6). The `attempt_all` error stays `partition_fanout_failed`. Each `fan_out` operation must be
  idempotent for each touch (G7).
- **`walk`.** `FokosWalkInput` has the same `local` and `forward` functions as today. `visits` becomes an
  `Iterable<FokosVisit>` and not an array, because an interval across many promoted routingHashes can have many visits.
  The host stops the iteration when its budget is used up. A walk is `readOnly: true` for now (section 10).

The execution decides only what step 5 of the pipeline does. The other steps are the same for each execution.

#### 7.2.3 The visit

```ts
export type FokosVisit = {
	target: FokosPartitionRef | "local";
	/** The touches that this visit serves. An interval is clipped to the scope of the target. */
	touches: readonly FokosTouch[];
	speculative: boolean;
};
```

`FokosVisit` replaces `FokosRangeVisit`, whose `start` and `end` are sort keys of one hash key. The `rangeVisits` and
`forwardRangeVisit` primitives become `plan(touches)` and `forwardVisit(visit, op, req)`. The old names stay as
wrappers.

#### 7.2.4 The planner

One function, `#plan(touches, scope, opts)`, gives the visits of a footprint. `scope` is the owned interval of the
partition, or the caller slice during a read-through.

- **An item or a position** resolves with `#resolvePosition`, as today.
- **An interval on a hash owner** is cut at each cut-over route override inside it. The parts between overrides are
  `local` visits. Each promoted routingHash `P` gets the visits of its range tree for `[P, P + 1)`, planned with the
  learned slices of `P`. `firstCutOverRouteOverride` gives the next override in one seek, as `ownedSliceEnd` does.
- **An interval on a hash router** is cut by the routingHash intervals of the children. Each part goes to the deepest
  known partition that holds the whole part: the hash arena, then the topology cache, then the direct child. The
  overrides of the router apply first, as in `#resolvePosition`.
- **An interval on a range partition** uses `planRangeFrontier` on composite keys. The learned slices already have
  composite boundaries, so the projection on one hash key goes. The rule of G4 stays: a learned slice replaces its
  base only when the slice holds the whole segment.
- **Grouping.** The planner merges the touches of one target into one visit, and runs `#guardBloomHits` over all
  touches, so G6 holds for mixed footprints.

The planner is lazy for intervals. It yields visits in storage order, in the order of the walk, and it reads storage
only for the visits that the caller takes.

#### 7.2.5 One forward and one fallback

`#forwardVisit(visit, op, req)` replaces `#forwardPoint`, `#forwardGroup`, `#forwardSingleOwner`, and
`#forwardRangeVisit`. On a cache miss, it forgets the hint and plans the touches of the visit again. The new plan can
be `local`, one target, or more targets. The execution then decides:

- `one_owner`: more than one target gives `onSpan`.
- `fan_out`: each new visit runs as a part. This is what `#forwardGroup` does today.
- `walk`: the new visits replace the failed visit in the same position of the iteration.

The Bloom rule on a retry depends on the touches, not on the shape: the Bloom step stays on when all touches of the
visit have one routingHash, and turns off otherwise. This gives the `point` behaviour for one key and the `group`
behaviour for many keys.

#### 7.2.6 One read-through gate

On the importing target, `#whileImporting` checks each touch against `#ownsByTopology`: a position must be inside the
scope, and an interval must be inside the scope. Then it sends `executeLocal` to the source, as today.

On the source, `#executeLocal` calls `#plan(touches, callerSlice, EXACT)`. The planner already sends a promoted
routingHash of a `hash_child` caller slice to its range tree, and it clips intervals to the caller slice. Then the
execution runs the visits. This replaces the three branches of `#executeLocal`, and it gives `one_owner` with
`onSpan` a read-through, which `single_owner` cannot have today. The `executeLocal` request does not change.

#### 7.2.7 Admission, local calls, and learning

`hooks.admit` gets one more field. The old field stays, with the same values as today:

```ts
admit?(input: {
	op: string;
	admissionTag?: string;
	/** Unchanged: the items of the local part. */
	keys: RouteKey[];
	/** New: every touch of the local part. Intervals are clipped to this partition. */
	touches: readonly FokosTouch[];
	lifecycle: FokosLifecycle;
	policy: TPolicy;
}): "allow" | { reject: Error };
```

`FokosLocalCall` is the place for per-call facts. It gets `touches`, the part of the footprint that this call
serves, next to `signal` and `ownedSliceEnd`. A handler then does not need to compute its own part again.

`#learn` takes the routingHash intervals of the touches in place of a list of routingHashes. An item or a position
gives a one-hash interval, so the learning of points does not change.

#### 7.2.8 The descriptor

```ts
export type FokosRoutedOperation<Req, Res> = FokosOperationGates<Req, Res> & {
	footprint(req: Req): readonly FokosTouch[];
	execution: FokosExecution<Req, Res>;
};

export type FokosOperation<Req, Res> =
	| FokosRoutedOperation<Req, Res>
	/** Deprecated presets. Each one compiles to a `FokosRoutedOperation`. */
	| FokosPointOperation<Req, Res>
	| FokosGroupOperation<Req, Res>
	| FokosSingleOwnerOperation<Req, Res>
	| FokosRangeOperation<Req, Res>
	| FokosLocalOperation<Req, Res>;
```

`FokosOperationGates` holds `whileMigrating`, `readOnly`, `admissionTag`, `localMode`, `local`, `beforeForward`, and
`forward`, which are the fields of `FokosOperationBase` today. A routed operation has no `shape` field. The runtime
compiles every descriptor once, in the constructor, and rejects an invalid combination there:

| Combination | Rule |
| --- | --- |
| `whileMigrating: "read_source"` | needs `readOnly: true`, for each execution |
| `walk` | needs `readOnly: true`, and only interval touches |
| `onEmpty: "local"` | allowed for `fan_out` only |
| an unknown `kind` | `sharding_operation_invalid` |

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
	execution: { kind: "one_owner", onSpan: { answer: { outcome: "not_applicable" } } },
	whileMigrating: "throw", admissionTag: "write",
	local: (req, call) => this.singleShotLocal(req, call),
},
txCommit: {
	footprint: (req) => req.items.map((item) => ({ kind: "item", key: keyOf(item), data: item })),
	execution: {
		kind: "fan_out",
		failurePolicy: "attempt_all",
		split: (req, touches) => ({ ...req, items: touches.map((t) => t.data as TransactionItemKey) }),
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
		split: (req, touches) => clipQueryToInterval(req, touches[0]),
		walk: (input) => walkQueryVisits(input),
	},
	whileMigrating: "read_source", readOnly: true, admissionTag: "read",
	local: (req) => this.queryItemsLocal(req),
},
```

#### 7.2.9 The client

`FokosShardingClient` gets the same model:

- `plan(touches, opts?)` gives `Array<{ ctx; touches; indexes }>`. An item or a position goes to the deepest known
  partition of its position, as `resolve` does. An interval goes to the deepest known partition that holds the whole
  interval, as `resolveRange` does. Touches with the same entry form one group, as `resolveAll` does.
- `resolve`, `resolveRange`, `resolveAll`, `point`, and `range` stay. Each one becomes a wrapper of `plan` and `send`.
- `send` keeps its signature. Its `_keys` argument becomes optional, and stays unused.
- An optional `fanOut(op, touches, req, split)` sends one request for each group in parallel, and gives the parts
  back. It replaces the grouping code in `client/db.ts`, the coordinator, `gsi-writer.ts`, and `do-partition.ts`.

The client does not import a descriptor. A host that wants one footprint function on both sides puts it in a shared
module, next to the request type.

#### 7.2.10 Performance

- **Points.** The pipeline adds one array of one touch and one compiled-descriptor lookup. Milestone 1 must show no
  regression with the `profile-worker-local` skill on `getItem` and `putItem`.
- **Groups.** The cost is the same as today: one resolution for each touch and one guard pass.
- **Intervals.** A plan costs one override seek for each promoted routingHash in the interval, plus one learned-slice
  list for each range tree that it enters. The lazy planner pays only for the visits that the walk takes.

#### 7.2.11 Testing

- A property test, for random topologies with hash splits, range splits, and promotions: for each footprint, the
  visits of `#plan` cover each touch exactly once (G4), in storage order, and each visit is inside the scope of its
  target (G2).
- A property test: each compiled preset gives the same targets, the same errors, and the same admission input as the
  shape before milestone 1.
- A test for each row of the validation table of section 7.2.8.
- The suites of `queryItems`, `scanItems`, the transactions, and the routing pass without a change after each
  milestone.

## 8. Compatibility: why this is not a breaking change

Four things can break. The design changes none of them.

1. **The wire.** A forward is `stub[op](targetCtx, req)`, and a read-through is `fokosRpc("executeLocal", { op,
   repartitionId, caller, request })`. Neither carries a shape or a plan. Each hop derives the footprint from `req`
   (G10). Thus two Durable Objects of different versions agree when the footprint function gives the same touches,
   and a preset gives the same touches as its shape.
2. **Stored state.** The overrides, the learned slices, the arena, the Bloom filter, and the identity do not change.
3. **The host API.** Every change is additive:
   - The shapes stay, as deprecated presets with the same types.
   - `FokosRoutedOperation` is a new member of the `FokosOperation` union.
   - `hooks.admit` gets a new field, and the old field keeps its values, also the made-up key of a range.
   - `FokosLocalCall` gets a new field.
   - `plan`, `forwardVisit`, and `fanOut` are new methods. The old methods become wrappers.
4. **Behaviour.** Each preset keeps the rules of its shape: the `point` Bloom retry, the empty `group` that runs
   locally, the `partition_fanout_failed` error of `attempt_all`, and the `notApplicable` answer. The property test of
   section 7.2.11 checks this.

Rules that keep the next change additive too:

- Each union that a host builds has an explicit `kind`. A new kind is a new member.
- The runtime rejects an unknown kind at construction. A host that uses a new kind with an old runtime fails at
  deploy time, not at request time.
- A new per-call fact goes into `FokosLocalCall`. A new per-request input goes into the touch or the gates. No new
  positional argument.
- A new operation must reach every Durable Object before a caller uses it. This rule exists today for any new
  method.

## 9. Alternative options

- **Add a shape for each new use case.** Rejected. Each shape adds a resolver, a forward function, a fallback, a
  read-through branch, and an admission rule. The cost grows with the product of shapes and concerns.
- **The runtime gives only `plan` and `forwardVisit`, and the host runs every execution.** Rejected as the main API.
  The host then must keep G3, G6, G7, and G9 itself. The design keeps these primitives as the escape hatch for an
  execution that the runtime does not have, as `rangeVisits` and `forwardRangeVisit` are today.
- **Put the footprint on the wire, next to the request.** Rejected. It changes the wire, it sends the same bytes twice,
  and a router then trusts a footprint that can disagree with the request. The derivation at each hop has none of
  these costs.
- **Always walk in the client.** Rejected. A walk inside a Durable Object saves one round trip for each visit. The
  place of the walk is a choice of each operation: `walk` inside the Durable Object, or `one_owner` steps on positions
  from the client.
- **Replace the shapes in one breaking release.** Rejected. Three hosts and every test helper change at once, and the
  compiled presets give the same result with no break.

## 10. Open questions

### 10.1 The fan-out width of a large interval

A `fan_out` over the whole table reaches each owner. A router fans out to at most `hashSplitN` children, plus one range
tree for each promoted routingHash inside its interval. The number of promoted routingHashes has no bound today.
Options: a cap with an error, or a `fan_out` that pages its visits as `walk` does. The answer decides if a backup can
use `fan_out`.

### 10.2 A walk that writes

A delete over an interval is a `walk` or a `fan_out` with `readOnly: false` and `whileMigrating: "throw"`. It must be
idempotent for each touch, and it must stop at a budget. Option: allow `fan_out` writes over intervals first, and keep
`walk` read-only.

### 10.3 Data on the touch or an index

`data` keeps the shape of `items(req)` today. An index into the footprint is smaller on the wire of `split`, and needs
no generic type. Option: give `split` both the touches and their indexes.

### 10.4 Admission of intervals

A host can then refuse a scan on a partition that is over its size. The host must decide which tags refuse an
interval, because a refused read on a full partition does not make it smaller.

## 11. Frequently asked questions

**Does a host have to change now?**
No. Each shape keeps working. A host moves one operation at a time.

**Is `point` gone?**
No. `point` is the preset "one item or one position, `one_owner`". The position form gets an explicit `kind` in the
routed form, so the runtime and the client do not test `"routingHash" in key`.

**Why does `single_owner` not become a `fan_out`?**
Because it needs the whole request in one synchronous handler call, for atomicity. A `fan_out` gives each owner a part.
`one_owner` with `onSpan` says the rule directly.

**Why keep positions when intervals exist?**
A scan step needs "the owner of this position, which serves its slice from there". An interval of `one_owner` can span
owners and then gives `onSpan`. A position cannot span, so it says the rule of the step directly.

**Does the design change the order of rows?**
No. The storage order `(routingHash, hk, sk)` and the tree order stay the same. The design only lets an interval cross
routingHashes in the planner, which `scanItems` already does in the client.
