# A Query Must Enter a Range Tree at Its Root

Status: **fixed**. Found on 2026-09-20 in the routing that `PartitionDO.withSplitForwarding`
implemented. `FokosShardingRuntime` replaced that routing and satisfies every invariant of section 6:
`apiQueryItems` declares `shape: "range"`, which carries no entry-point key, and the interval planner
in `sharding/range-frontier.ts` selects a learned slice per segment and only when the slice contains
that segment whole. This document stays as the reasoning behind those invariants and as the record of
what breaks without them.

The guards are `src/sharding/range-frontier.test.ts` ("keeps the base cover beside a left-edge slice
that a point read taught" and its deeper companion) and
`test/partition-do/query-items.test.ts` ("queryItems after a point read taught the range hierarchy").
Both fail when the entry point becomes key-based again. Section 8.3 is ported into
`docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`, sections 4.2.10 and 4.2.20.

## 1. The symptom

`queryItems` on a promoted hash key returns a page that holds only part of the answer. The page
carries no cursor, so the caller reads it as the complete answer. Nothing fails and nothing logs.

The loss is not transient. It does not clear when the split that caused it completes. Every later
query of that hash key through the same hash partition answers the same truncated way.

`getItem` of a lost item stays correct. Only the operations that span the sort-key axis lose rows.

## 2. The measurement

A range tree of 7 leaves held one hash key `range:hot` with 340 stable sort keys `sk0000`…`sk0339`.
The child start boundaries were `sk0045`, `sk0090`, `sk0135`, `sk0180`, `sk0225` and `sk0270`. A
writer then added large items with sort keys `sk0034.w0000`… — inside the FIRST leaf, the one whose
start boundary is unbounded — until that leaf passed `rangeSplitConditions.maxSizeMb` and split.

After that split:

| Request | Answer |
| --- | --- |
| `queryItems` of the whole hash key | one leaf visited: `….r.range:hot.~min.sk0034%2Ew0001` |
| `queryItems` with `between sk0035 and sk0035` | 0 items, no cursor |
| the same request 3 seconds later | 0 items, no cursor |
| `getItem` of `sk0035` | found, served by `….r.range:hot.sk0034%2Ew003.sk0045` |

`rangeSplitN` was 2, and the two leaf names prove that the left region had split more than once: one
leaf ends at `sk0034.w0001`, and another one starts inside `sk0034.w003…` and ends at `sk0045`. The
query walked exactly one leaf of that region. Every row above `sk0034.w0001` was missing from every
query answer, and `getItem` still read those rows from the leaves that own them.

## 3. The mechanism

Four parts combine.

1. **A query carries no single sort key, but it sends one.** `#apiQueryItems` routes a hash partition
   through `withSplitForwarding` and passes the routing sentinel as the sort key,
   `KeyCodec.encodeOptional(undefined)`, which is the EMPTY byte string. The comment at that call
   site is right about its purpose: the sentinel makes the hash routing decide by hash key alone,
   because every sort key of a non-promoted key lives on one leaf.

2. **The sentinel travels further than the hash decision.** `withSplitForwarding` hands the same sort
   key to `forwardToRangeRootPartition`, both from the promoted-key override and from the bloom
   filter check.

3. **`forwardToRangeRootPartition` treats it as a real key.** With a sort key it asks
   `PartitionStore.findDeepestKnownRangeSlice(hashKey, sortKey)` for the deepest range slice it has
   learned that contains that key, and it enters the tree there instead of at the root. The intent is
   sound for a point operation: boundary identity is immutable, so a stale hint is safe, and the hop
   through the routers above is saved.

4. **The empty key selects the leftmost chain.** The lookup is
   `WHERE hk = ? AND sk_start_boundary <= ? AND (sk_end_boundary > ? OR sk_end_boundary = ?) ORDER BY
   depth DESC LIMIT 1`, where the empty byte string tags an unbounded edge. The empty sort key is the
   byte minimum, so `sk_start_boundary <= ?` holds only for a slice whose start is unbounded, and
   `sk_end_boundary > ?` holds for every real end. The answer is therefore always the deepest known
   slice on the LEFT edge of the tree.

The query then enters that leftmost slice. That node owns one interval, so it answers for one
interval. `clipQueryToSlice` narrows the request to what the node owns, which is correct on its own
terms: a range child inherits the interval of the client and relies on its router to clip. Here that
same clip turns a wrong entry point into a silent truncation.

## 4. Why nothing caught it

- **A query never teaches the cache.** `forwardToRangeRootPartition` learns from
  `meta._internal.rangeAncestors` of the answer. A query enters at the root, and the root builds no
  ancestors of its own (`#_rangeAncestors` stays empty for depth 0), so a query only ever learns the
  root, which the `startBoundary !== null || endBoundary !== null` guard then rejects. The cache is
  filled by point operations.
- **A usable poison needs a point operation on the left edge.** A learned slice can only match the
  sentinel when its start is unbounded. A write or a read to the leftmost region of the tree, AFTER
  that region has split, is what stores it.
- **The settled suites never write there twice.** `query-items-split.test.ts` writes `sk0000` upward
  while the tree is still small, and its later point operations land in other leaves, so the newest
  learned left-edge slice is the root itself and the lookup finds nothing usable. The defect appears
  as soon as a second point operation reaches the left edge of a tree that has grown.
- **`getItem` is correct by construction.** It names one sort key, so the learned slice it picks
  really does contain the key.

The property suite `query-items-active-split.test.ts` found it, because its writer targets one
region until the leaf that owns it splits, and its first region is the left edge of the tree.

## 5. The reasoning for the fix in the current routing

The defect is a category error: an operation that spans the sort-key axis has NO entry-point key, and
the sentinel is not one. The fix states that at the call site.

- `withSplitForwarding` takes a `spansSortKeys` flag. When it is set, the range entry key is
  `undefined`, so `forwardToRangeRootPartition` skips the learned-slice lookup and enters at the
  root. The hash decisions — `shouldAllow` and `pickChildPartition` — keep the sentinel, because they
  decide by hash key alone.
- `#apiQueryItems` is the one call site that sets it. `putItem`, `deleteItem` and `getItem` name a
  real sort key and keep the optimization.

**The check does not belong in a lower layer.** An item may legitimately have no sort key at all, and
its key is the same empty byte string. A `getItem` of that item passes the empty key correctly, and
its leftmost learned slice is the right entry point. Below the call site the sentinel and a real
absent sort key are the same value, so only the caller can tell them apart. A byte-length test in
`forwardToRangeRootPartition` or in `findDeepestKnownRangeSlice` would break that read instead.

Cost of the fix: a query on a promoted key always enters at the range root. That is one hop, and the
root fans out to the children anyway. The skipped hop was never valid for a query.

## 6. What the new routing must keep

These hold whatever replaces `withSplitForwarding`.

1. **An operation that spans the sort-key axis has no entry-point key.** A range-spanning operation
   enters at the root of the range tree, or at a node that covers its whole interval. A routing cache
   keyed by one point key must never be consulted with a sentinel.
2. **A routing sentinel must not be a value that a real key can take.** The empty byte string is the
   key of an item with no sort key. A sentinel that collides with a real key makes every layer below
   the caller unable to tell a routing placeholder from data.
3. **An entry point must cover the whole requested interval.** A clip to what a node owns is a safety
   net for a caller that asks for more than it owns. It must never be the thing that decides how far
   an answer reaches.
4. **A wrong entry point must not answer partially.** A page that covers less than the request asked
   for, with no cursor, is indistinguishable from a complete answer. Either the entry node re-forwards
   to a node that covers the interval, or it fails.

## 7. How to test it

A deterministic case needs three steps, and every step is available through the public API.

1. Promote one hash key into a range tree, and let the tree grow to several leaves.
2. Write into the FIRST leaf — the one with the unbounded start — until it splits, and let the split
   complete.
3. Read one item of that region with `getItem`, so the hash partition learns the new deep left-edge
   slice. Then query the whole hash key.

Before the fix, step 3 returns the items of one leaf. After it, the query returns every item. The same
recipe with a middle leaf shows nothing, which is the reason the defect survived so long.

## 8. How the fix ports to FokosShardingRuntime

`docs/agent-plans/2026-09-19-fokos-sharding-runtime.md` replaced the routing that held this defect, and the
runtime is built. This section records how the fix maps onto that API. Subsection 8.3 is ported into the RFC and
is kept here only as the reasoning for those three additions.

### 8.1 The fix becomes a shape

Today one function serves two different questions. `withSplitForwarding` takes one `{ hashKey, sortKey }` route
key and gives it to `forwardToRangeRootPartition`, so a query must invent a sort key that it does not have. The
`spansSortKeys` flag exists only to undo that invention.

The runtime separates the two questions by structure.

| Today                                                          | Runtime                                                   |
| -------------------------------------------------------------- | --------------------------------------------------------- |
| `withSplitForwarding` with the sentinel sort key               | `shape: "point"` with `key(req): RouteKey`                |
| the same call for `queryItems`                                 | `shape: "range"` with `range(req): FokosRangeInput`       |
| `forwardToRangeRootPartition` → `findDeepestKnownRangeSlice`    | owner resolution (4.2.8) for point, group and single owner |
| no separate path for an interval                               | the interval frontier planner (4.2.10) for range          |

Section 4.2.8 states the split: "The `range` shape uses the interval planner of section 4.2.10." A `range`
descriptor has no `key(req)` member. There is therefore nowhere to put a sentinel, and nothing to switch off.
`spansSortKeys: true` becomes `shape: "range"`.

Three rules of the proposed design carry the invariants of section 6 on their own.

- **Section 4.2.10, base cover step 1: "A promoted hash key selects the range root."** An operation that spans
  the axis enters at the root. The learned hierarchy is then overlaid as segments, and it does not choose the
  entry. This is rule 1 of section 6.
- **Section 4.2.10, step 4: select "the deepest known partition that fully contains" the segment.** The current
  code picks the deepest slice that contains ONE key, which is how it enters a slice that covers part of the
  interval. Full containment is rule 3 of section 6, and "their union must equal the requested interval" states it for
  the whole plan.
- **Section 4.2.10: "A partial cache cannot create a coverage gap because each uncovered segment keeps its base
  target."** A cache miss costs latency and never truncates an answer. This is rule 4 of section 6.

A promotion Bloom hit also degrades safely: it "makes the range-root base cover speculative", so it selects the
root and never a deep slice.

### 8.2 Where the new code states it

In the FokosDB host, in the descriptor of `queryItems`: `shape: "range"`, `whileMigrating: "read_source"`,
`readOnly: true`. Section 4.2.18 already prescribes those three values. Nothing changes inside the runtime.

The `spansSortKeys` flag has no successor. Delete it with the code it patches.

### 8.3 What the RFC must still add — ported

1. **The empty sort key is a real key.** `RouteKey` is `{ hashKey, sortKey }` as `KeyBytes`, and an item with no
   sort key holds the empty sort key. The RFC invents no placeholder today, but the collision in rule 2 of
   section 6 stays open for any later "point route that names no sort key". One sentence on `RouteKey` closes it: the empty
   sort key is a value, never a placeholder, and a route that names no sort key uses the `range` shape.
2. **Say which evidence feeds the range hierarchy, and why the answer does not matter.** Section 4.2.10 says the
   cache "learns `_hint.rangeAncestors` from range evidence", and `_hint` sits on every `FokosRouteNode`,
   including the point entry that a `getItem` of a promoted key returns. Point evidence is how the cache fills up
   today. If an implementation also learns from point evidence, the asymmetry that caused this defect returns to
   the INPUT of the cache. The defence then rests on the interval-based reader alone, which is sound and must be
   written down: whatever fills the range hierarchy, only an interval lookup reads it.
3. **Add the cross-feed test.** The test list of section 4.2.20 covers the frontier property, a warm cache, a
   partial cache, and a learned partition that split again. It does not cover the case that found this defect: a
   POINT operation teaches the cache, and a broad range request must still return every item. The left edge is
   the case that matters, because only a slice with an unbounded start matches a byte-minimum key. The recipe is
   in section 7.

## 9. References

- `packages/fokosdb/src/server/do-partition.ts` — `#apiQueryItems`, `withSplitForwarding`,
  `forwardToRangeRootPartition`.
- `packages/fokosdb/src/shared/partition/partition-store.ts` — `findDeepestKnownRangeSlice`,
  `setRangeAncestors`, `getRangeAncestors`.
- `packages/fokosdb/src/shared/partition/repartition/repartition-slice.ts` — `clipQueryToSlice`.
- `packages/fokosdb/test/property-based/query-items-active-split.test.ts` — the suite that found it.
