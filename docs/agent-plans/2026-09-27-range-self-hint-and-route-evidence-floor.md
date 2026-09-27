# RFC — Learn the slice of every range node, and keep one route node under the byte cap

**State:** Implemented
**Date:** 2026-09-27
**Author:** Lambros Petrou

**Status:** Implemented: M1, M2, and M3.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
  - [4.3 Future extensions](#43-future-extensions)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

### 1.1 The missing self hint

A partition that forwards a request learns routes from the `routing.servedBy` list of the response. For a range
node, `FokosShardingRuntime.#learn` in `src/sharding/runtime.ts` decodes the partition ID to get the hash key. It
learns range boundaries only from the `_rangeAncestors` field of the node.

`FokosShardingRuntime.#setIdentity` builds that field from the stored ancestors of the partition. It adds the own
slice of the partition only when the partition has at least one stored ancestor. Two cases have no stored
ancestors:

1. **A depth-1 child.** `selectRangeAncestors` in `src/sharding/range-ancestors.ts` returns `[]` when the parent is
   the range root.
2. **A deeper child when the configuration selects no ancestor.** With `rangeAncestorsConfig` set to
   `{ fromRoot: 0, fromLeaf: 0 }`, every partition at depth 2 or deeper has no stored ancestors.

In both cases the response does not name the slice of the partition. A hash partition that owns a promoted key
then finds no learned slice in `findDeepestKnownRangeSlice` or `listLearnedRangeSlices`. Each later request goes
through the range root, which adds one hop. Routing stays correct.

With the default configuration `{ fromRoot: 0, fromLeaf: 3 }`, this applies to every promoted key whose range tree
split once. That is the most frequent shape of a range tree.

The tests in `test/partition-do/range-split.test.ts` expect this behavior. The test "propagates rangeDepth and the
ancestor set across two levels of splits" expects `[]` for each depth-1 child. The test "is fully inert when
rangeAncestorsConfig={fromRoot:0,fromLeaf:0}" expects `[]` at depth 2.

Before the migration to `FokosShardingRuntime`, a partition used two conditions. The `fokosInit` path added the own
slice only when ancestors existed. The load path after a restart added the own slice for each range partition with
a depth above 0. The migration kept only the `fokosInit` condition.

### 1.2 The byte cap can drop every node

`RouteCollector.build` in `src/sharding/envelope.ts` caps the list at `ROUTE_EVIDENCE_MAX_BYTES` (10 KiB). It
measures each node with `routeNodeBytes`. It stops at the first node that crosses the cap, and that can be the first
node.

`routeNodeBytes` counts 128 bytes, plus 2 bytes for each character of `doName`, `partitionId`, and `actorId`. The
measurements in section 4.2.7 give these sizes for one range node:

| Keys | `partitionId` | `doName` | `routeNodeBytes` |
| --- | --- | --- | --- |
| 30-byte hash key and boundaries, printable | 200 characters | 136 characters | 928 bytes |
| 1,024-byte hash key, 512-byte boundaries, binary | 4,116 characters | 4,708 characters | 17,904 bytes |

The second node is above the cap by itself, so `build` returns an empty `servedBy`. The caller then learns nothing,
and an error carries no node that names the partition that raised it.

The runtime spec states that "a node at the head of the list also always survives the byte cap". The code does not
hold that rule.

### 1.3 Range routers learn slices that they never use

A range router at depth d reads a learned slice only when the slice is a strict sub-slice of one of its direct
children. `#resolve` checks this with `isStrictSubSlice`, and `planRangeFrontier` overlays only strict sub-slices.
So the range router uses only slices at depth d + 2 or deeper.

Today `#learn` on a range router stores each entry of `_rangeAncestors` that its descendants send. With
`{ fromRoot: 0, fromLeaf: 3 }`, a leaf at depth d + 3 sends the slices at depths d, d + 1, and d + 2. The range
router stores all three rows, and it never uses the first two:

- depth d, and each depth below d, is the slice of the router itself or of an ancestor, which holds all its children;
- depth d + 1 is equal to one of its direct children.

These rows use the row budget of `fokos_range_hierarchy`. When the table is full, each new row costs about 1 ms,
because of the eviction. Section 4.2.7 gives the measurement. The self hint of section 4.2.4 adds one more such row
for each direct child that answers.

## 2. Goals and requirements

### 2.1 In scope

- Each range node with `rangeDepth > 0` in a response teaches its own slice to the partition that forwarded the
  request. This is true for each value of `rangeAncestorsConfig`.
- `_rangeAncestors` holds only the ancestors that `selectRangeAncestors` selected. It does not hold the slice of the
  partition itself.
- `RouteCollector.build` always returns at least one node when the collector holds at least one node.
- On the success path, the node that `build` always keeps is the node with the highest role.
- On the error path, the node that `build` always keeps is the node that raised the error.
- A range partition at depth d learns only slices at depth d + 2 or deeper. A hash partition learns each slice at
  depth 1 or deeper.

### 2.2 Out of scope

- A change to `selectRangeAncestors` or to the meaning of `rangeAncestorsConfig`. The selection stays as it is.
- A change to the order of `servedBy`, or to the rule that the raiser leads the list on an error. Section 4.3.1
  describes that later work.
- The removal of an ancestor entry that more than one node in one response carries. Section 4.3.2 describes it.
- A route cache in `FokosShardingClient`. `docs/agent-plans/2026-09-26-fokos-sharding-client.md` owns it.

### 2.3 Requirements

- A cache hint must never change a result. A missing or old learned slice changes latency only.
- The learner must decode the full range partition ID, with both boundaries, for each range node with
  `rangeDepth > 0`.
- `build` must keep at most one node that crosses `ROUTE_EVIDENCE_MAX_BYTES`. The other nodes follow the current
  cap rule.
- The change must not add a production hook for a test.
- This spec comes after `docs/agent-plans/2026-09-26-range-partition-id-base64url.md`. That document says that the
  learner decodes only the first part of a range ID, and reads boundaries only from `_rangeAncestors`. This spec
  replaces that rule for range nodes with `rangeDepth > 0`. That document does not change.

## 3. Milestones

1. **Self hint and depth filter.** Change `#learn` and `#setIdentity`. Update the range-split tests and add the
   learning tests. This milestone fixes the extra hop, and it stops the unused rows on range routers.
2. **Byte-cap floor.** Change `RouteCollector.build` and `RouteCollector.forget`. Add the envelope tests. This
   milestone makes the spec rule about the byte cap true on its own.
3. **Documents.** Update section 4.2.9 of `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md` to match the
   byte-cap rule of this spec.

## 4. Proposed solution

### 4.1 High-level overview

A range partition ID already encodes the hash key and both boundaries of its slice. Each route node carries that ID
in `ref.partitionId`, and its depth in `rangeDepth`. So the learner can get the own slice of each node from the node
itself. The partition does not need to send its own slice a second time in `_rangeAncestors`.

```text
leaf L (depth d, ancestors A selected at split)
  response node: { ref.partitionId = id(hk, start, end), rangeDepth = d, _rangeAncestors = A }

forwarding partition, #learn:
  decode(ref.partitionId) --> (hk, start, end) --> learn (hk, start, end, d)   when d > 0
  for each a in A          --> learn (hk, a.start, a.end, a.depth)
```

The learner stores a slice only where the slice can be used. A hash partition stores each slice at depth 1 or
deeper. A range router at depth d stores only slices at depth d + 2 or deeper, which are strictly inside one of its
children.

The second change keeps at least one node in each response. `build` first chooses one node that it always keeps.
That node is the raiser on an error, and the node with the highest role on success. It then adds the other nodes
under the byte cap, as it does now.

### 4.2 Technical details

#### 4.2.1 Content of `rangeAncestors`

| Partition | Stored `identity.range.ancestors` | `_rangeAncestors` on its response node |
| --- | --- | --- |
| Range root, depth 0 | `[]` | absent |
| Depth-1 child | `[]`, because the parent is the root | absent |
| Child at depth d ≥ 2 | the `fromRoot` shallowest and `fromLeaf` deepest of depths 1 to d-1 | the same list |
| Any depth with `{ fromRoot: 0, fromLeaf: 0 }` | `[]` | absent |

Example: with `{ fromRoot: 0, fromLeaf: 3 }`, a leaf at depth 5 sends `[d2, d3, d4]`. Its own slice, d5, comes from
its `ref.partitionId`.

#### 4.2.2 How the runtime writes the ancestors

The write path does not change.

1. The range-split plan in `RepartitionFlow` (`src/sharding/repartition-flow.ts`) sets `rangeDepth` to the parent
   depth plus 1.
2. `selectRangeAncestors` builds the candidates. They are `[]` when the parent is the root. Otherwise they are the
   stored ancestors of the parent plus the slice of the parent. It keeps the `fromRoot` shallowest and the
   `fromLeaf` deepest, one per depth. The slice of the parent stays a candidate, because it is an ancestor of each
   child.
3. The key-promotion plan sets depth 0 and `[]` for a new range root.
4. `fokosInit` carries `rangeDepth` and `rangeAncestors` to each child.
5. `#applyTargetIdentity` checks that only a partition with a depth above 0 has ancestors.
6. `#writeIdentity` and `partitionIdentityFrom` build `identity.range`. `FokosShardingStore.putIdentity` stores it.
7. `#setIdentity` loads the identity into memory after init, after a restart, and after a policy update.

`#setIdentity` changes. It keeps `range.ancestors` as they are, and it does not append the own slice. The field
`#rangeAncestorsWithSelf` becomes `#rangeAncestors`.

#### 4.2.3 How the runtime sends the ancestors

1. `#selfNode(role)` builds `{ ref, actorId, hashDepth, rangeDepth, role, _rangeAncestors? }`. It adds
   `_rangeAncestors` only when the list is not empty.
2. `RouteCollector.add` keeps one node for each partition, with the highest role.
3. `#forwardTo` merges the list of the child with `RouteCollector.mergeForwarded`. When a hash partition enters a
   range tree, `#rangeDepthStamp` writes its hash depth on each range node. The stamp does not change
   `_rangeAncestors`.
4. `RouteCollector.build` applies the byte cap. Section 4.2.6 gives the new rule.
5. `FokosRouter.unwrap` and `publicRoute` remove `_rangeAncestors` at the Worker boundary. A client does not read
   it.

A non-root range node now carries one ancestor entry fewer. `routeNodeBytes` counts each entry as 24 bytes plus the
length of both boundaries.

#### 4.2.4 How the runtime learns

`#forwardTo` is the only caller of `#learn`. It calls `#learn` on a result and on a routed error.

`#learn` does these steps for each node of `servedBy`:

1. For a range node:
   1. Decode `ref.partitionId` with `PartitionIdHelper.decode` to get `hashKey`, `startBoundary`, and
      `endBoundary`.
   2. **New:** When `node.rangeDepth` passes the depth filter, call `learnRangeBoundary(hashKey,
      startBoundary ?? NO_SORT_KEY, endBoundary ?? NO_SORT_KEY, node.rangeDepth)`. This applies to each role:
      `executed`, `merged`, and `read_through`.
   3. For each entry of `node._rangeAncestors` that passes the depth filter, call `learnRangeBoundary` with the
      entry. **New:** the depth filter applies to the entries too.
   4. When this partition is a hash partition, add the hash key to the promotion Bloom filter and learn the hash
      depth from `node.hashDepth`.
2. For a hash node, learn the hash depth for each request key that the node owns.
3. Store the hash arena and the Bloom filter when either changed.

**New: the depth filter.** The learner computes the minimum useful depth once for each call of `#learn`:

| This partition | Minimum depth that the learner stores |
| --- | --- |
| Hash partition | 1 |
| Range partition at depth d | d + 2 |

The learner skips depth 0 because the range root is never a useful jump. `#rangeOwner` already rejects a slice that
is unbounded on both sides. Section 1.3 gives the reason for d + 2 on a range partition. A range partition calls
`#learn` when it forwards as a router, and when it reads through to its source during an import. The decode of the
ID still runs for each range node, because a hash partition needs the hash key for the Bloom filter. A range
partition can skip the decode when the node and all its entries are below the minimum depth.

`FokosShardingStore.learnRangeBoundary` inserts `(hk, start, end, depth)` into `fokos_range_hierarchy`. When the row
exists, it refreshes `learned_at` only if the row is older than `RANGE_HIERARCHY_REFRESH_MS` (60 seconds). It evicts
the oldest rows, deepest first, above `rangeHierarchyMaxRows` (default 10,000).

A node with role `merged` or `read_through` is a partition that exists, so its slice is a correct hint. A slice of
a partition that split later costs one more forward. The next response teaches the deeper leaf.

#### 4.2.5 How the runtime reads a learned slice

The read path does not change.

- **Point operation, hash partition.** `#resolve` calls `#rangeOwner`. `findDeepestKnownRangeSlice` returns the
  deepest learned slice that holds the sort key. The runtime jumps to it, or goes to the range root. The new
  depth-1 slices apply here.
- **Point operation, range router.** `#rangeChildFor` selects the durable child that holds the sort key. `#resolve`
  jumps to the deepest learned slice only when `isStrictSubSlice` is true for that child. Otherwise it forwards to
  the child. The depth filter of section 4.2.4 removes only rows that this check rejects, so the result does not
  change.
- **Range operation.** `#planRange` reads `listLearnedRangeSlices`. On a range router the bases are the durable
  children. `planRangeFrontier` in `src/sharding/range-frontier.ts` overlays each strict sub-slice of a base and
  selects the deepest slice for each segment. A segment with no learned slice goes to its base child.
- **Old slice.** When a learned target answers `range_partition_not_initialized`, `#fallbackAfterMiss` and
  `#forwardRangeVisit` call `deleteLearnedRangeSlice` and resolve again.

#### 4.2.6 The byte-cap floor in `RouteCollector.build`

`RouteCollector` keeps the ID of the raiser. `addRaiser` sets it. `forget` clears it when it removes that node.

`build` does these steps:

1. Choose the kept node:
   - When the collector holds a raiser, the kept node is the raiser.
   - Otherwise, the kept node is the first node, in insertion order, with the highest `ROLE_RANK`.
2. Start the byte count with `routeNodeBytes` of the kept node.
3. Go through the other nodes in insertion order. When a node makes the count cross `ROUTE_EVIDENCE_MAX_BYTES`, set
   `servedByTruncated` and stop.
4. Return the kept node and the added nodes in insertion order.

The output order does not change. `addRaiser` already puts the raiser first, so the raiser stays at the head. On the
success path the kept node stays at its insertion position.

`build` returns at most one node above the cap. The largest node is a range node with the maximum key sizes and 20
ancestor entries, because `fromRoot` and `fromLeaf` are each at most 10. Each entry counts at most 24 + 1,024 bytes.
That node measures 38,864 bytes with `routeNodeBytes`, which over-counts. The serialized node without ancestors
measures 8,986 JSON characters.

The Workers RPC message limit is 32 MiB. The runtime error for a larger argument is "Serialized RPC arguments or
return values are limited to 32MiB". The kept node uses about 0.1% of that limit. One response adds at most
`ROUTE_EVIDENCE_MAX_BYTES` plus one node, so the floor cannot make an RPC fail.

Effect for each response shape:

| Response | Kept node |
| --- | --- |
| Point operation | the executor, because a router that only forwards adds no node |
| Range operation over many leaves | the first `executed` leaf, not the `merged` router |
| Error | the raiser |

`#forwardRangeVisit` and `#forwardPoint` call `forget` for a target that refused a request before any handler ran,
and then retry with the same collector. That target is the raiser, so `forget` clears the raiser mark. When a
raiser mark stays after a successful retry, `build` keeps that node. The list is a hint, so this changes only which
node survives the cap.

#### 4.2.7 Performance

**Method.** A scratch vitest file ran in the local Workers runtime (`@cloudflare/vitest-plugin`, workerd). Each
row is the mean of 1,000 to 20,000 operations after a warm-up. The timer in workerd has a resolution of about 1 ms,
and each measurement window is 5 ms or longer, so a small row has an error of up to 20%. These are local numbers,
not production numbers.

| Operation | 30-byte keys, printable | 1,024/512/512-byte keys, binary |
| --- | --- | --- |
| `Uint8Array.fromHex` and `PartitionIdHelper.decode`, the current learner | 1.0 µs | 6.2 µs |
| `PartitionIdHelper.decode` of bytes already in memory | 0.25 µs | 0.25 µs |
| base64url decode of the boundary part (60 B and 1,024 B) | 0.7 µs | 1.0 µs |
| `learnRangeBoundary`, new row, table not full | 13.5 µs | 83.5 µs |
| `learnRangeBoundary`, known row, no refresh | 11.0 µs | 14.5 µs |
| `learnRangeBoundary`, new row, table full at 10,000 rows | 995 µs | not measured |

- **Learner decode.** The current learner already decodes the full hex ID, at 1 to 6 µs for each range node. With
  the base64url format, the decode of the boundary part adds 0.7 to 1 µs for each range node with
  `rangeDepth > 0`, once for each forwarding hop.
- **Learner writes.** Each learned slice is one SQLite upsert. The self hint adds one upsert for each range node
  that passes the depth filter. In the steady state the row is known, so it costs about 11 to 15 µs. That is more
  than 10 times the decode. An RPC hop between Durable Objects is a network round trip, which costs more than both.
- **Full table.** When `fokos_range_hierarchy` holds `rangeHierarchyMaxRows` rows, each new row costs about 1 ms. The
  `COUNT(*)` and the `DELETE ... ORDER BY learned_at, depth DESC` scan the table. The `FIXME` in
  `learnRangeBoundary` names this cost. The depth filter lowers the number of new rows on range routers.
- **Response size.** Each non-root range node drops one ancestor entry: 84 bytes of `routeNodeBytes` for 30-byte
  boundaries, and 1,048 bytes for 512-byte boundaries.

#### 4.2.8 Deployment

The stored identity does not change, so no migration is necessary. `#setIdentity` derives the list in memory, so a
partition uses the new rule at its next load.

During a rollout, old and new partitions exchange responses:

- An old partition still sends its own slice in `_rangeAncestors`. A new learner learns the same row twice, and the
  second write does nothing.
- Rows that a range router stored before the change stay in its table until eviction. The read path rejects them,
  as it does today.
- A new partition does not send its own slice. An old learner does not learn it until the rollout ends. This costs
  one hop, as it does today.

A rollback returns to the current behavior. Learned rows stay valid hints.

#### 4.2.9 Testing

- `test/partition-do/range-split.test.ts`:
  - The two-level test expects `[]` for each depth-1 child, as now. At depth 2 it expects one entry, the depth-1
    parent, in place of two.
  - The test with `{ fromRoot: 0, fromLeaf: 0 }` expects `[]`, as now.
  - A new test reads a key through a range root whose leaf is at depth 3, with `{ fromRoot: 0, fromLeaf: 3 }`.
    Then it reads `fokos_range_hierarchy` of the range root with `runInDurableObject`. The table holds only rows
    at depth 2 or deeper.
  - A new test sends a point read for a promoted key through its hash partition twice, with a depth-1 child as the
    owner. The second read has a `forwardCount` that is 1 lower than the first.
  - A new test does the same for a depth-2 owner with `{ fromRoot: 0, fromLeaf: 0 }`. The first read goes
    through the range root and the depth-1 child, so it has a `forwardCount` of 3. The second read goes directly
    to the owner, so it has a `forwardCount` of 1.
- `src/sharding/envelope.test.ts`:
  - One node above the cap is kept, and `servedByTruncated` is false.
  - An `executed` node above the cap, after a `merged` node, is kept, and the `merged` node is dropped.
  - A raiser above the cap stays at the head, and the nodes after it are dropped.
  - `forget` of the raiser clears the raiser mark.
  - The current tests for order and truncation pass without a change.

### 4.3 Future extensions

#### 4.3.1 The raiser of an error without a list position

**The problem.** On an error, the runtime marks the partition that raised it by list position: it is `servedBy[0]`.
Three places depend on that position:

- `#forwardTo` and the read-through path in `src/sharding/runtime.ts` read `servedBy[0]`. Then they call `addRaiser`
  to put it at the head of their own list.
- `partitionInfoOf` in `src/client/partition-info.ts` uses `servedBy[0]` when no node is `executed`.
- The planned client route cache needs to know if the raiser of `partition_migrating` with
  `importState: "awaiting_data"` is the entry partition.

The runtime marks the raiser for three reasons:

1. A partition can refuse a request before any handler runs: admission, the lifecycle gate, or owner resolution.
   Without its node, the error has an empty list, and the caller cannot name the partition.
2. A router that throws `partition_fanout_failed` must lead the list. Otherwise the first group that answered
   leads it.
3. The raiser must survive the byte cap. Section 4.2.6 now makes this true.

The success path has no raiser. Readers there search by role or by `partitionId`.

**The goal.** No reader depends on the order of `servedBy`. `build` can then sort the list as it wants.

**Two options:**

- **A `raisedBy` field on `FokosRouting`.** It holds the partition ID of the raiser. `mergeForwarded` copies it from
  the error routing of a child. `addRaiser` and the two `servedBy[0]` reads go away. The public routing does not
  change.
- **A `raised` role.** `ROLE_RANK` becomes `{ merged: 0, read_through: 1, executed: 2, raised: 3 }`. The merge keeps
  the highest role, so the mark travels up without extra code. `role` is part of `FokosPublicRoute`, so this adds a
  value to a public union type.

**The difficult case, for both options.** A group operation with `failurePolicy: "attempt_all"` merges the error
routing of each failed group into one collector. Each failed group brings a raiser. Then the router throws its own
`partition_fanout_failed` and becomes the raiser. There must be only one raiser. So the router must remove the mark
from the others. With the role option, the role of a node before it was marked is lost. The router must choose
another role for it, for example `executed`.

#### 4.3.2 Remove repeated ancestor entries

Sibling leaves carry the same ancestors, and the list of a range router is a prefix of the lists of its leaves. A
range query over k sibling leaves with `fromLeaf: 3` sends about 3k entries, and about 3 are unique. `build` can
drop each entry that an earlier node in the output already sent. The key is the hash key, the depth, and both
boundaries. With the base64url ID, the hash key part is the text before the dot, so no decode is necessary. The
learner reads the whole envelope, so one copy of each entry is enough.

#### 4.3.3 Rank the list before the cut

`build` can sort by role, then by depth, before it applies the cap. Then a wide fan-out keeps the deep leaves and
drops the routers first. This needs section 4.3.1 first, because it changes the list order.

#### 4.3.4 Skip a known slice before the decode

A small in-memory set of recently learned partition IDs lets `#learn` skip the decode and the upsert for a node it
already learned. This also covers the existing `TODO(perf)` about repeated inserts.

#### 4.3.5 Cheaper eviction in `fokos_range_hierarchy`

A new row in a full table costs about 1 ms, because `learnRangeBoundary` counts and sorts the whole table. A kept
row count in memory, or an eviction of a batch of rows at a time, can remove most of that cost.

## 5. Alternative options

- **Add the own slice when `range.depth > 0`.** A one-line change in `#setIdentity`, with no change to the learner.
  Rejected because each non-root range node then carries its boundaries three times: in `partitionId`, in `doName`,
  and in `_rangeAncestors`. The extra bytes cost more than the decode.
- **Sort `servedBy` by role in `build` now.** Rejected for this spec, because the error path depends on the list
  position. Section 4.3.3 keeps it as future work.
- **A `raisedBy` field or a `raised` role now.** Rejected for this spec. The byte-cap floor keeps the raiser without
  them. Section 4.3.1 describes both.

## 6. Frequently asked questions

**Does the learner decode more IDs than today?**
No. It decodes each range node once, as it does today. With the base64url format, it also decodes the second part
for nodes with `rangeDepth > 0`.

**Can a learned slice of a `merged` router send a request to the wrong partition?**
No. A router owns the slice in its ID and forwards each key in it. A learned slice changes the number of hops only.

**Why does the depth-2 test expect one entry and not two?**
The own slice of the grandchild moves from `_rangeAncestors` to its `ref.partitionId`. The one entry left is the
depth-1 parent.

**Why does a range router not store the slice of its direct child?**
It never reads it. Its durable child record already names that partition. A learned slice helps a range router only
when it is strictly inside a child, which is depth d + 2 or deeper.

**Does `rangeAncestorsConfig` with `{ fromRoot: 0, fromLeaf: 0 }` still turn off learning?**
No. It turns off only the ancestor entries. Each range node still teaches its own slice.

## 7. References

- `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`
- `docs/agent-plans/2026-09-26-range-partition-id-base64url.md`
- `docs/agent-plans/2026-09-26-fokos-sharding-client.md`
- `docs/ideas/2026-09-26-range-self-hint-bugfix.md`
- `packages/fokosdb/src/sharding/runtime.ts`
- `packages/fokosdb/src/sharding/envelope.ts`
- `packages/fokosdb/src/sharding/range-ancestors.ts`
- `packages/fokosdb/src/sharding/sharding-store.ts`
- `packages/fokosdb/src/sharding/partition-id.ts`
- `packages/fokosdb/src/client/partition-info.ts`
