# RFC — `scanItems`: a paginated scan of a table or a GSI

**State:** Draft
**Date:** 2026-11-20
**Author:** Lambros

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)
- [8. Appendix: dispatch by footprint and execution](#8-appendix-dispatch-by-footprint-and-execution)
- [9. Appendix: the inversion of `xxh64`](#9-appendix-the-inversion-of-xxh64)

## 1. Overview and context

### 1.1 The problem

FokosDB has no operation that reads all items of a table. `queryItems` reads the items of the hash keys that the caller
names. A caller who does not know the hash keys cannot read the table. DynamoDB has `Scan` for this, with the
pagination fields `ExclusiveStartKey` and `LastEvaluatedKey`, and with a parallel scan by `Segment` and
`TotalSegments`. FokosDB follows the DynamoDB API, so it needs the same operation.

### 1.2 What the reader must know about the current system

- **The storage order.** The primary key of `items` is `(routing_hash, hk, sk)`
  (`shared/partition/partition-store.ts`). The routingHash is `xxh64(hk, seed)`, with the seed of the shard group
  (`routingHashOf` in `sharding/hash-primitives.ts`). The client knows the seed, and it routes every point operation
  with it.
- **The tree keeps the storage order.** `RoutingHashDigitCursor` reads the routingHash as mixed-radix digits, the most
  significant digit first: the root digit, then one digit for each level. The hash partitions in depth-first order
  (root 0, child 0, child 1, ...) therefore cover routingHash intervals that increase and touch each other
  (`routingHashInterval`). A promoted routingHash moves into a range tree that splits by `(hk, sk)`. That tree stays at
  the same place in the storage order.
- **A repartition does not move an item in the storage order.** A hash split, a range split, and a promotion change
  the owner of a row. They do not change the position `(routingHash, hk, sk)` of the row.
- **The `queryItems` page.** `queryItems` returns one bounded page with an opaque `cursor`. `QueryPageBudget`
  (`shared/query/page-budget.ts`) carries four budgets: evaluated items, evaluated bytes, response bytes, and partition
  visits. `MAX_PARTITION_VISITS_PER_PAGE` is 100.
- **Dispatch shapes.** Each partition operation declares a shape: `point`, `group`, `single_owner`, `range`, or
  `local` (`FokosOperation` in `sharding/runtime-types.ts`). `range` covers a sort-key interval of one hash key only.

### 1.3 Glossary

- **Position** — a place in the storage order, `(routingHash, hk?, sk?)`. An absent `hk` or `sk` is the minimum. The
  type is `FokosStorageKey` (`sharding/runtime-types.ts`).
- **Slice** — the part of the storage order that one partition owns now, from a position to its end. An override
  that has cut over ends the slice of a hash partition.
- **Step** — one RPC from `db.ts` to the owner of one position. The owner reads its slice and answers.
- **`E`** — the slice of the last complete step of a page. A page that stops at a slice boundary returns a key inside
  `E`.
- **Found key** — a string or binary hash key that the client computes, so that its routingHash has an exact value.

## 2. Goals and requirements

### 2.1 In scope

- `db.scanItems(opts)` returns one bounded page of the items of the base table, in the storage order.
- `db.gsi(indexName).scanItems(opts)` returns one bounded page of the entries of a GSI, in the storage order of the
  GSI.
- A caller continues the scan with `exclusiveStartKey = lastEvaluatedKey` until `lastEvaluatedKey` is absent.
- A caller can scan one segment of `totalSegments` segments. The caller runs the segments in parallel.
- A page continues at the owner of its start position. It does not visit again a partition that an earlier page read,
  except for the one partition that holds the start position (section 4.2.7). There are two exceptions, and both
  return `key(L)`: a page that a budget stops before the first row of a step, and the fallback 1 of section 4.2.6.
  The next page then reads again the empty slices after `L`, which are fewer than the visit limit.
- A page that visits only empty partitions still makes progress, and it does not return an error.
- Each item that exists for the whole scan and is not deleted appears exactly once. A split, a promotion, or an import
  during the scan does not change this.
- `scanItems` takes `limit`, `maxResponseBytes`, `select`, `filter`, and `projection`, with the same rules as
  `queryItems`.

### 2.2 Out of scope

- **A parallel scan inside FokosDB.** The caller runs the segments. A helper in `FokosDBStd` can do it later.
- **A consistent snapshot.** A page reads each slice at a different time. An item that a caller creates or deletes
  during the scan can appear or not appear. DynamoDB `Scan` has the same behaviour.
- **A descending scan.** DynamoDB `Scan` has no direction.
- **A change of the `range` dispatch shape.** Section 8 describes this follow-up.

### 2.3 Requirements

- The public request and response contain only keys. They contain no routingHash, no position, no partition name, and
  no opaque cursor.
- The visit limit of a page is `MAX_PARTITION_VISITS_PER_PAGE` (100), the limit of `queryItems`. This keeps the latency
  of one page between 1 and 10 s (section 4.2.11). The subrequest limit of the Worker is set high enough that it does
  not limit a page.
- The client must not import a Durable Object class as a value (`AGENTS.md`).
- The host must not read a `fokos_` table. It gets each routing fact from the runtime (`AGENTS.md`).
- The GSI scan needs the total order of GSI entries (`2026-11-22-gsi-sort-key-total-order.md`). That RFC adds the
  separator `00 00` after the escaped base sort key. Section 4.2.9 depends on it.

## 3. Milestones

Each milestone ships on its own, and the full test suite passes after each one.

1. **Position routing.** A `point` descriptor can give a `FokosStorageKey` in place of a `RouteKey`. The runtime and
   the client resolve the owner of a position. Each use of the point key accepts a position (section 4.2.4). No
   operation uses it yet, so there is no change in behaviour.
2. **Owned slice.** `call.ownedSlice(pos)` in the runtime, the store method for the next override, and the scope of a
   read-through.
3. **Host operation.** `apiScanItems` in `FokosDBPartitionDO`, the scan of a storage interval in `PartitionStore`, and
   the hash key of each row in the page collector (section 4.2.3).
4. **Client.** `db.scanItems`: the walk, the segments, the key at a slice boundary, and the found key.
5. **GSI.** `db.gsi(indexName).scanItems`. This milestone starts after the RFC
   `2026-11-22-gsi-sort-key-total-order.md` is implemented.
6. **Documentation.** A `scanItems` section in `AGENTS.md`.

## 4. Proposed solution

### 4.1 High-level overview

`db.ts` walks the storage order of the table, from one owned slice to the next.

1. The client turns the start key into a start position. With no start key, the position is the minimum. With a start
   key `k`, the client computes `rh = routingHashOf(k.hashKey)`, and the position is "after `(rh, hk, sk)`".
2. The client sends a step to the owner of the position. It uses the route cache, as for `getItem`.
3. The owner reads its slice from the position, in the storage order. It stops at the end of its slice, at the end of
   the segment, or when the budget of the page is used up. It answers with the items and with `next`, the position
   where the next slice starts.
4. The client sends the next step to the owner of `next`. It repeats until the budget or the visit limit stops the
   page, or until `next` is absent.
5. The client returns `lastEvaluatedKey`. This is the key of the last evaluated item. When the page stopped at a slice
   boundary after an empty slice, it is a key inside that slice (section 4.2.7). The client takes that key from a range
   boundary, or computes a found key that hashes to an exact routingHash.

A step is a `point` request whose route key is a position. No new dispatch shape is necessary.

```
storage order (routingHash, hk, sk) ──────────────────────────────────────────────────────────►

 root 0 [00,80)                                                                 root 1 [80,∞)
 ├─ 0.0 [00,40)          ├─ 0.1.0 [40,60)                  ├─ 0.1.1 [60,80)      │
 │  bob, carol           │  dave │ rh 47 promoted │ frank  │  (empty)            │  gina, hank
 │                       │       ▼                │        │                     │
 │                       │    R.0 [min,(eve,m))   │        │                     │
 │                       │    R.1 [(eve,m),max)   │        │                     │

 steps:  (00)→0.0   (40)→0.1.0   (47)→R.0   (47,eve,m)→R.1   (48)→0.1.0   (60)→0.1.1   (80)→root 1
```

The next page starts at the owner of the last key of the page. The resolver finds the owner from the key. Partitions
before that owner get no visit.

### 4.2 Technical details

#### 4.2.1 Public API

```ts
type ScanItemsOptions = {
	/** Evaluated items per page. The same default and maximum as `queryItems`. */
	limit?: number;
	/** Materialized item bytes per page. The same default and maximum as `queryItems`. */
	maxResponseBytes?: number;
	select?: QuerySelect;
	filter?: ConditionExpression;
	projection?: readonly ProjectionExpression[];
	exclusiveStartKey?: { hashKey: HashKey; sortKey?: SortKey };
	/** From 0 to `totalSegments - 1`. Give `segment` and `totalSegments` together, or neither. */
	segment?: number;
	/** From 1 to 1,000,000. */
	totalSegments?: number;
};

/** The options of `db.gsi(indexName).scanItems`. The start key can name one entry of the GSI. */
type GsiScanItemsOptions = Omit<ScanItemsOptions, "exclusiveStartKey"> & {
	exclusiveStartKey?: { hashKey: HashKey; sortKey?: SortKey; baseHashKey?: HashKey; baseSortKey?: SortKey };
};

type ScanItemsPage<Item> = {
	items: Item[];
	count: number;
	scannedCount: number;
	/** Absent when the scan of the table, or of the segment, is complete. */
	lastEvaluatedKey?: { hashKey: HashKey; sortKey?: SortKey };
	meta: QueryItemsPage<Item>["meta"];
};
```

- `items`, `count`, `scannedCount`, and `meta` have the meanings that they have in `QueryItemsPage`.
- The projected overload follows `queryItems`: a request with a `projection` returns `ProjectedItem` records.
- `lastEvaluatedKey` can name a key that holds no item (section 4.2.7). The caller must treat it as a resume value.
  It can hold control characters (section 4.2.8), so a caller encodes it before it puts it in a URL, a header, or a
  log.
- The result has no fingerprint. A caller can send a different `filter`, `projection`, or `limit` on each page, and
  can start a scan at a key that the caller chooses.
- On a GSI, `lastEvaluatedKey` is `{ hashKey, sortKey?, baseHashKey?, baseSortKey? }`. The base keys are present when
  the key names a real entry, because two entries can have the same GSI keys. A key at a slice boundary can have no
  base keys (section 4.2.9).

**Validation.** The client validates before any I/O:

- `limit`, `maxResponseBytes`, `select`, `filter`, and `projection` get the checks of `#queryItems`, with the same
  validation codes.
- `segment` and `totalSegments` must both be present or both be absent. `totalSegments` must be an integer from 1 to
  1,000,000. `segment` must be an integer from 0 to `totalSegments - 1`.
- `exclusiveStartKey` gets `validateItemKeys`. On a GSI, `baseHashKey` and `baseSortKey` get the same key checks, and
  `baseSortKey` requires `baseHashKey`. The validation code for a `baseSortKey` without `baseHashKey` is
  `scan_start_key_invalid`.
- When the request has a segment, the routingHash of `exclusiveStartKey.hashKey` must be inside the segment.
- Each failure is a `FokosValidationError`. The new codes are `scan_segment_invalid`, `scan_start_key_invalid`, and
  `scan_start_key_outside_segment`.

#### 4.2.2 Segments

Segment `s` of `T` is the routingHash interval `[⌊s · 2^64 / T⌋, ⌊(s + 1) · 2^64 / T⌋)`, as 8 bytes with the most
significant byte first. The last segment has no upper bound. Without a segment, the interval is the whole storage
order.

Each step carries the end of the segment. A slice that crosses the end of the segment returns only the part before
the end. The walk of the segment ends when `next` is at or after the end of the segment.

The segment edges do not depend on the partition tree. A split during the scan does not move a segment.

#### 4.2.3 The partition operation `apiScanItems`

`apiScanItems` is declared in `PartitionOps` and registered in `operations()` with `shape: "point"`,
`readOnly: true`, and `whileMigrating: "read_source"`. Its `key(req)` returns `req.start`.

```ts
type ScanItemsRpcRequest = {
	start: FokosStorageKey;
	/** True for a slice boundary. False for the position after a start key. */
	startInclusive: boolean;
	/** The end of the segment. Null when the request has no upper bound. */
	end: FokosStorageKey | null;
	remainingEvaluatedItems: number;
	remainingEvaluatedBytes: number;
	remainingResponseBytes: number;
	allowOversizedFirstItem: boolean;
	select: QuerySelect;
	plan: { filter?: ConditionExpression; projection?: readonly ProjectionExpression[] } | null;
};

type ScanItemsRpcResponse = {
	items: Array<StoredItem | ProjectedWireRow>;
	count: number;
	scannedCount: number;
	/** Stored bytes of the evaluated items, charged to the evaluated-byte budget. */
	evaluatedBytes: number;
	/** Estimated RPC bytes of the materialized items, charged to the response-byte budget. */
	responseBytes: number;
	rowsReturned: number;
	/** The same entries as `QueryItemsRpcResponse["partitions"]`. */
	partitions: QueryItemsRpcResponse["partitions"];
	/** The key of the last row that this step evaluated. Null when the step evaluated no row. */
	lastEvaluated: { hk: KeyBytes; sk: KeyBytes } | null;
	/** True when the step read its slice to the end. False when the budget stopped it. */
	sliceComplete: boolean;
	/** When `sliceComplete` is true: the start of the next slice, or null at the end of the table or segment. */
	next: FokosStorageKey | null;
};
```

The local handler is synchronous, as the other point handlers are:

1. It gets `{ end: sliceEnd, next }` from `call.ownedSlice(req.start)`.
2. It reads from `req.start` to `min(sliceEnd, req.end)` in the storage order:
   `WHERE (routing_hash, hk, sk) > (?, ?, ?) AND (routing_hash, hk, sk) < (?, ?, ?) ORDER BY routing_hash, hk, sk`.
   The first bound uses `>=` when `startInclusive` is true.
3. It feeds each row to the page collector (`createQueryPageCollector`), with the compiled filter, the projection,
   and the remaining budgets. The rules for visibility and expiry are the rules of the `queryItems` leaf scan. The
   response takes `evaluatedBytes` and `responseBytes` from the state of the collector, as `QueryItemsRpcResponse`
   does. `QueryPageBudget.consume` subtracts them, so the byte budgets apply to the whole page and not to each step.
   The collector reads one fixed `hashKey` now, and `QueryCandidateConsumer` gets only `sk`. A scan step reads rows of
   many hash keys. Thus the consumer also gets the `hk` of each row, and the collector writes that `hk` into
   `lastEvaluatedCursor` and `nextCursor`. The `queryItems` leaf scan gives its one hash key for each row, so its
   behaviour does not change.
4. When `req.end` is at or before `sliceEnd`, the read ends at `req.end`, and `next` is null.

`PartitionStore` gets one method for this read. The method takes a storage interval, the direction `asc`, and the
plan. It does not change the existing query read.

#### 4.2.4 Position routing in the sharding layer

**Runtime.** `#resolve` computes the routingHash from `key.hashKey` in its first line, and then uses only
`(routingHash, hk, sk)`. The change moves the hash to the edge:

- `key(req)` of a `point` descriptor can return a `RouteKey` or a `FokosStorageKey`. The runtime turns a `RouteKey`
  into a position with `routingHashForKey`. Every other step uses the position.
- `#resolve`, `#ownsByTopology`, `#rangeOwner`, the point check of the read-through (`sliceIncludesItem`), and
  `#learn` take a position, or the routingHash of the position. The section "Each use of the point key" below gives
  the full list.
- A position with no `hk` resolves like a key with the minimum `(hk, sk)`. In a range tree it goes to the child that
  holds the minimum. A promoted routingHash goes to its range tree, with the same Bloom fallback as a key.

**Client.** `#resolveKey` in `FokosShardingClient` computes the routingHash first, and after that it uses only the
routingHash and `(hk, sk)`. The change:

- `#resolveKey` and `FokosRouter.rootContext` start from a routingHash.
- `resolve(key)` stays as a wrapper that hashes the key.
- `point(op, key, req)` also accepts a position. The fallbacks of the route cache call the resolver with the position.

**Each use of the point key.** The runtime reads `descriptor.key(req)` in more places than `#resolve`. Each place
must accept a position:

| Place in `sharding/runtime.ts` | What it uses | For a position |
|---|---|---|
| `#admit`, which calls `hooks.admit({ keys })` | the keys that the request names | `keys: []` |
| `#scopeKeys`, for `#learn` and `#forwardTo` | the routingHash of each hash key | `pos.routingHash` |
| `#missLadder([key], ...)` | the routingHash of `keys[0].hashKey` | `pos.routingHash` |
| The import gate: `#scopeRouteKeys` and `#ownsByTopology` | the position | the position |
| The read-through gate in `#executeLocal`: `sliceIncludesItem` and the promoted forward | the position | the position |

- **`hooks.admit`.** A scan step reads a slice. It names no item, so `keys` is `[]`. `#dispatchGroup` already admits
  an empty group with `[]`. The host hooks in `do-partition.ts` and `do-transaction-coordinator.ts` gate only the
  `write` tag, and they allow an empty `keys`. The type of the hook does not change. Its contract gets one sentence:
  "`keys` lists the keys that a request names. A read of a slice names none." A host that must gate a scan uses `op`
  or `admissionTag`.
- **`#learn`, `#forwardTo`, `#missLadder`.** These read a hash key only to compute its routingHash. `#learn` passes
  `#routingHash(hashKey)` to `hashPathOwns` and `learnDepth`. A range node gives its routingHash from its partition
  id, and does not read the hash keys. `#missLadder` uses the routingHash for `deleteLearnedRangeSlice` and
  `arena.invalidate`. The three functions therefore take routingHashes. One helper, `#scopeRoutingHashes`, gives
  them: `routingHashForKey(key)` for a `RouteKey`, and `pos.routingHash` for a position. The learned routes do not
  change, and a forward computes no extra `xxh64`.
- **The import gate.** `#ownsByTopology` takes the position, in place of
  `#ownsByTopology(key, this.#routingHash(key.hashKey))`.
- **The read-through gate.** `sliceIncludesItem(slice, pos)` checks the position. A position of a promoted
  routingHash that reaches a `hash_child` source goes to the range owner of the position, as a key does.

**No change for a key.** `#resolve(key, opts)` becomes the wrapper `#resolvePosition(positionOf(key), opts)`.
`positionOf` is the first line of `#resolve` now: it computes the routingHash and builds `{ hk, sk }`. After that line,
`#resolve` uses only `(routingHash, hk, sk)`, so a key runs the same steps as before. The client gets the same wrapper
around `#resolveKey`. Only a scan step makes a position with no `hk`. The range lookup takes the empty `hk` as the
minimum, because `fokos_range_hierarchy.start_hk` defaults to `x''`. The Bloom filter and the overrides use the
routingHash, so they need no change.

#### 4.2.5 The owned slice

`FokosLocalCall` gets one function: `ownedSlice(pos: FokosStorageKey): { end: FokosStorageKey | null; next:
FokosStorageKey | null }`. The runtime computes it against the scope of the current call.

The scope of the call:

- On an owner, the scope is `ownedStorageInterval()`.
- During a read-through, the scope is the caller slice that `resolveCallerSlice` gives. This rule is necessary. The
  source is a router, and its own interval covers the slices of all its children. If the source used its own interval,
  a walk could get one row from the source for two children.

The end and `next` depend on the kind of the scope, not on the kind of the partition that runs the step. During a
read-through, a hash source can serve a scope inside a range tree.

- A **hash scope** is an owner hash partition or a `hash_child` caller slice.
- A **range scope** is an owner range partition, a `range` caller slice, or a `promoted_routing_hash` caller slice
  (`sharding/repartition-slice.ts`).

| Scope | `end` | `next` |
|---|---|---|
| Hash scope, no override in `[pos, B)` | `B`, the end of its routingHash interval | `(B)`, or null at the end |
| Hash scope, an override in `[pos, B)` | `P`, the first cut-over override in `[pos, B)` | `(P)` |
| Range scope, not the last of its tree | its end boundary `(P, hk, sk)` | `(P, hk, sk)` |
| Range scope, the last of its tree | the end of the routingHash `P` | `(P + 1)`, or null when `P` is the maximum |

A `promoted_routing_hash` caller slice is the last of its tree. Only a hash scope reads `fokos_route_overrides`. A
range scope is inside the range tree of `P`, so the override of `P` does not end it. This rule is necessary during
the `cutover` state of a promotion. The range root then reads through to the hash source, and that source holds the
override of `P` in the state `cutover`. If the override ended the scope, the step would read an empty slice and miss
each item of `P`. `#executeLocal` uses the same rule: it checks the override only for a `hash_child` caller slice.

`P + 1` is the 8-byte successor of `P`. An override that is `queued` or `planned` has not moved ownership. It does not
end the slice. This is the same rule that `#resolve` uses (`cutOver`).

**Store.** `FokosShardingStore` gets one method: the first routingHash in `fokos_route_overrides` at or after `pos` and
before `B`, whose repartition has cut over. `routing_hash` is the primary key of the table, so the read is one seek:
`WHERE routing_hash >= ? AND routing_hash < ? ORDER BY routing_hash LIMIT 1`, joined with `fokos_repartitions` for the
state. The states are `cutover`, `completed`, and `cleaned`, the same as `cutOver()`. The method is therefore not
`queryTerminalRouteOverridesPage`, which accepts only `completed` and `cleaned`. With that method, a hash slice does
not end at `P` during the `cutover` state of a promotion. The scan then reads the source copy of `P` and never visits
the range tree, which receives the writes of `P` after the cutover.

A hash partition can hold stale copies of the rows of a promoted routingHash until `cleanupSourceStep` deletes them.
The slice ends at `P`, and the next slice of the hash partition starts at `P + 1`. The scan therefore never reads the
copies.

#### 4.2.6 The walk in `db.ts`

```
pos  = exclusiveStartKey ? after(position(exclusiveStartKey)) : min(segment)
L    = none                                     // the last evaluated row of this page
last = none                                     // the start position and the answer of the last complete step
for visits = 1 .. ∞:
    if visits > MAX_PARTITION_VISITS_PER_PAGE or budget.budgetExhausted:
        k = stopKey(last, pos)
        if k: return page(k)
                                                // no key: one more step, over the visit limit
    answer = partitions.point("apiScanItems", pos, request(pos, segmentEnd, budget))
    collect answer.items; budget.consume(answer)
    if answer.lastEvaluated: L = answer.lastEvaluated
    if !answer.sliceComplete:
        return page(answer.lastEvaluated ? key(L) : stopKey(last, pos))
    if answer.next == null: return page(no lastEvaluatedKey)          // the end of the table or the segment
    last = { start: pos, answer }
    pos = answer.next

stopKey(last, S) = boundaryKey(last, S) ?? (L ? key(L) : none)
```

The position of a start key is `(routingHashOf(hk), hk, sk)`, with the encoded keys. An absent `sortKey` is the empty
sort key. Section 4.2.9 gives the position of a GSI start key.

A step that stops inside its slice has `sliceComplete: false`. When it evaluated a row, `lastEvaluatedKey` is the key
of that row. When it evaluated no row, the page stops at the start of that step, and `boundaryKey` applies. This
happens when the remaining response bytes cannot hold the first row of the step. An earlier step then returned a row,
so `L` exists for the fallback.

`boundaryKey` gives no key only when the found-key search reaches its cap or a check of case 4 or 5 fails
(section 4.2.7). The fallback is then:

1. When the page has `L`, the page returns `key(L)`. The next page reads the empty slices after `L` again. It still
   makes progress, because those slices are fewer than the visit limit.
2. When the page has no `L`, the page does one more step and calls `boundaryKey` again for the new `E`. The new `E`
   ends at a different boundary, so the search has a new target and a new independent chance.

Both cases occur with a chance of about `e^-37` for each page, or with a 64-bit collision of two hash keys. A page
goes over the visit limit only in case 2.

**A step error.** `partitions.point` retries a step with the retry policy of the client. An error that stays after
those retries ends the page: `scanItems` throws it and returns no item of the page. The caller retries the page with
the same `exclusiveStartKey`. A step only reads, so a retry is safe. DynamoDB `Scan` does the same.

#### 4.2.7 The key at a slice boundary

When a page stops at a slice boundary `S`, `E` is the slice of the last complete step: `[last.start, S)`. `E` was
read. `boundaryKey` returns a key whose position is inside `E`. The next page starts after that key, in `E`. It finds
no row in the rest of `E` and continues at `S`. The cost is one RPC with one index seek that returns no row.

An empty `E` is never the first step of a page. The first step has the full budget and `allowOversizedFirstItem`, so
it stops inside its slice only after it evaluates a row. When a page stops at a boundary right after its first step,
that step evaluated a row, and case 1 applies. Thus an empty `E` starts at a slice boundary, after the start of the
page. The client checks this with an `invariant`.

The cases:

| Case | `E` | The key |
|---|---|---|
| 1 | `E` had rows | `key(L)`, the last evaluated row |
| 2 | An empty hash slice `[A, S)` | a found key with `xxh64(hk) = S − 1` |
| 3 | The only range partition of a promoted `P`, empty: `[(P), (P + 1))` | a found key with `xxh64(hk) = S − 1 = P` |
| 4 | The first range partition of `P`, empty: `[(P), (P, hk_S, sk_S))` | `{ hashKey: hk_S }` |
| 5 | Another range partition of `P`, empty: `[(P, hk_a, sk_a), S)` | `{ hashKey: hk_a, sortKey: v }` |

In case 2, `A` is the start of a hash partition or `P + 1`, and `S` is the end of the hash partition or a promoted
`P`. In case 5, `v` is `smallestValidAtOrAfter(sk_a)`. When `v` is empty, the key is `{ hashKey: hk_a }` with no
`sortKey`, because an empty sort key fails `sort_key_empty`. Its position `(P, hk_a, "")` is the start of `E`, so the
key is inside `E`.

The client tells the cases apart from the positions `last.start` and `S`:

- When `last.start` has an `hk`, the case is 5.
- When `last.start` has no `hk` and `S` has an `hk`, the case is 4.
- When neither has an `hk`, the case is 2 or 3. `E` then holds every position with the routingHash `S − 1`, for every
  `hk` and `sk`. Thus any hash key whose routingHash is `S − 1` gives a position inside `E`, whatever its bytes are.

**Cases 4 and 5 use the real hash key of the range tree.** A range tree of `P` holds the hash keys whose routingHash is
`P`. Two hash keys with one routingHash need a 64-bit collision, so in practice the tree holds one hash key.
`shortestCompositeSeparator` (`sharding/key-codec.ts`) keeps the hash key of the upper item when the two items have
the same hash key, so the boundary holds the real hash key. A found key cannot be used here: its order against the
real hash key is random, so its position can be outside `E`.

The checks of cases 4 and 5:

- `routingHashOf(hk)` must equal `P`. Between two hash keys of one routingHash, the boundary is
  `(prefix of hk, "")`, and this check rejects it.
- Case 4 needs a non-empty `sk_S`, because `(hk_S, "")` must be less than `S`. On a GSI, the position of the key is
  `(P, hk_S, gsiItemSortKey("", "", ""))`, and the client checks that this position is less than `S`. The check
  passes each time: `sk_S` is a cut of a stored GSI sort key that has a non-empty base hash key, so `sk_S` is greater
  than the stored key of three empty parts.
- Case 5 needs a key that is less than `S` when `S` is in the same tree. The key passes this check each time, because
  each range partition holds one item or more when its split creates it. The split planner puts each boundary
  between two items. Thus a valid key exists in `[a, S)`, and the smallest valid key at or after `a` is at or below it.
  On a GSI, this argument needs the total order of GSI entries (section 4.2.9).

When a check fails, `boundaryKey` gives no key, and the fallback of section 4.2.6 applies. Only a 64-bit collision of
two hash keys in one range tree can make a check fail.

**`smallestValidAtOrAfter(b)`** gives the smallest encoded sort key that is a valid public key and is at or after the
bytes `b`. A boundary is a byte prefix of an item key, so only its last character can be cut:

- **Binary** (`b[0] = 0xFF`): `b` is valid. The one exception is `b = [0xFF]`, which decodes to an empty `Uint8Array`
  and fails `sort_key_empty`. The result is then `[0xFF, 0x00]`, the binary key `Uint8Array.of(0)`. This boundary
  occurs when the lower item has a string sort key and the upper item has a binary one, because
  `shortestSeparator` does not treat the `0xFF` tag in a special way.
- **String:** when `b` is valid UTF-8, the result is `b`. Else the last character of `b` is cut, and the client
  completes it with the smallest valid continuation bytes. For example, the lead byte `0xE2` becomes `E2 80 80`. The
  smallest continuation depends on the lead byte, because UTF-8 excludes overlong forms and surrogates: after `E0` the
  next byte is at least `A0`, and after `F0` it is at least `90`.
- **GSI:** see section 4.2.9.

The result is never longer than the item key that the boundary came from, so it fits `maxSortKeyBytes`.

#### 4.2.8 The found key

A found key is a hash key of 16 encoded bytes. The client computes it so that `xxh64(bytes, seed) = T` exactly, with
the seed of the shard group. `xxh64` of a 16-byte input processes two 8-byte lanes and then the avalanche. Each step is
a bijection: a multiply by an odd constant, a rotate, a xor-shift. Thus the client fixes the first lane and computes the
second lane from `T` in O(1).
Section 9 gives the steps.

- **String found key.** The first lane is `"fokos"` and 3 base-36 characters. The client computes the
  second lane. When all its 8 bytes are in `0x01–0x7F`, the 16 bytes are an ASCII string that passes
  `validateKeyContent`. Else the client takes the next value of the 3 characters. The chance of success for each try is
  `(127/256)^8`, about `1/270`. A local workerd measurement over 2,000 targets gave a mean of 265 tries and 0.33 ms, a
  median of 185 tries, a p99 of 1,307 tries, and a maximum of 1,779 tries. One try costs about 0.83 µs: 20,000 tries
  took 16.6 ms. The cap is 10,000 tries, which is about 8.3 ms.
  The chance that the search reaches the cap is about `(1 − 1/270)^10000`, which is about `e^-37`. Then
  `boundaryKey` gives no key, and the fallback of section 4.2.6 applies.
- **Control characters.** A string found key can hold the bytes `0x01–0x1F` and `0x7F`, for example `\n` and `\r`.
  A key of a caller can hold them too, and a binary key can hold any byte. Thus FokosDB does not promise that a
  returned key is safe in a URL, a header, or a log. A caller that puts a key there must encode it, for example with
  base64url. The documentation of `lastEvaluatedKey` says this.
- **Binary found key.** The tag `0xFF`, 7 fixed bytes, and the computed lane. Any byte value is valid in a binary
  key, so the client needs no search. One binary key costs about 0.8 µs in workerd.

**The type of the found key** is the type of a hash key of the request, so that the caller gets the type that it uses:

1. When the request has an `exclusiveStartKey`, the found key has the type of its `hashKey`.
2. Else, when the page evaluated a row, the found key has the type of the hash key of `L`. The tag `0xFF` of the
   encoded bytes gives the type.
3. Else the found key is a string. This is the first page of a scan that found no row. A string survives JSON, and
   `JSON.stringify` of a `Uint8Array` does not give back a `Uint8Array`. A caller that declares `HashKey` as
   `Uint8Array` gets a string only in this case.

The found key is a normal key. The prefix `"fokos"` is not reserved, and the client does not treat the key in a
special way on the next page. When an item with the same key exists later, that item is behind the scan position.
This is the same behaviour as any other write during a scan. The found key has 16 bytes, and `maxHashKeyBytes` is at
least 64 (`resolveLimits`).

#### 4.2.9 GSI

A GSI is its own shard group with its own seed. `db.gsi(indexName).scanItems` runs the same walk with the GSI client
(`#gsiClient`), as `#queryItems` does. The client decodes each row with `gsiKeys`. It takes `GsiScanItemsOptions`
(section 4.2.1), so a caller can send the `lastEvaluatedKey` of a GSI page back with its base keys.

**The position of a GSI start key** is `(routingHashOf(hk), hk, gsiItemSortKey(sortKey, baseHashKey, baseSortKey))`
(`shared/gsi/sort-key.ts`). An absent part is the empty key. The same mapping applies to every GSI start key, also to
a found key and to a key from a boundary. In cases 2 and 3 any sort key gives a position inside `E`, so a found key
with no other part is correct.

**The order of GSI entries.** The stored GSI sort key is
`0xFF escape(gsiSK) 00 00 escape(baseHK) 00 00 escape(baseSK) 00 00 [rawGsiSK] <L><flag><D>`
(`2026-11-22-gsi-sort-key-total-order.md`). Each part ends with `00 00`, and `00 00` never occurs inside an escaped
part. Thus the stored order is the order of `(gsiSK, baseHK, baseSK)`, each part compared as `KeyBytes`, and the raw
copy and the suffix never decide the order. An absent part encodes as the empty key and its separator, so the start
key of the parts that are present sorts at or below each entry that has those parts.

**`smallestValidAtOrAfter(b)` for a GSI** follows the layout of `decodeGsiSortKey`: the escaped GSI sort key, the
separator `00 00`, the escaped base hash key, the separator, the escaped base sort key, the separator, and the suffix
with the raw copy, the length, and the flag. The client parses `b` up to the cut:

1. It completes the part that is cut with its smallest valid continuation. The parts after the cut are absent.
2. A cut after the first `00` of a pair continues with `00`, which is the separator, because `00 00` is less than the
   escaped NUL `00 FF`. The part before the pair is then complete.
3. A cut after the third separator, inside the raw copy or the suffix, gives back the full key of the item. All three
   parts are complete there, and the client computes the raw copy and the suffix from `gsiSK`.

Because the stored order is the order of the parts, the result encodes to bytes at or after `b`, and at or below each
stored GSI sort key that starts with `b`. A test compares it with a reference that tries all short keys.

#### 4.2.10 Concurrency and consistency

- One step reads one slice in one synchronous block of the local handler. The step sees one state of its slice.
- Two steps of one page read at different times. A page is not a snapshot.
- `items` holds committed state only, so a scan reads committed state, as `queryItems` does. A lock of a transaction
  that is in flight does not block the scan.
- A split between two steps sends the next step to a router. The router forwards it to the child that owns the
  position. A child that is still importing reads through its source with its caller slice.
- The client learns routes from `servedBy` of each step. A later step goes directly to the owner on a cache hit.

#### 4.2.11 Performance

- One step is one round trip from the client, plus one forward for each stale hint. A round trip takes 10 to 100 ms
  within one location hint or jurisdiction, by the location of the partition.
- A page has at most `MAX_PARTITION_VISITS_PER_PAGE` (100) steps, one after another. A page that reads 100 slices
  therefore takes 1 to 10 s. A caller who needs more throughput scans segments in parallel.
- The scan of a slice is a range read on the primary key of `items`, plus one seek on `fokos_route_overrides`.
- A page that stops at a slice boundary makes the next page do one extra step. That step returns no row.
- The found key costs 0.33 ms on average and about 8.3 ms at its cap (section 4.2.8). The key from a boundary costs
  no I/O.

#### 4.2.12 Testing

The tests use `TestPartition` (`partition-harness.ts`) to build the topology.

- **Walk.** A table with hash partitions at more than one depth, an empty partition, and a promoted routingHash with
  two range partitions. A full scan returns each item once, in the storage order.
- **Stale copies.** The scan never returns the stale copies of a promoted routingHash in its hash partition.
- **Split between pages.** A hash split and a range split between two pages give no duplicate and no missed item.
- **Read-through.** A step to a child that is still importing returns only the rows of the caller slice.
- **Promotion import.** A scan during the `cutover` state of a promotion returns each item of `P` once. The range root
  reads through to the hash source, and the scope is a range scope (section 4.2.5).
- **Page byte budgets.** A page of large items across many slices stops when the sum of `responseBytes` reaches
  `maxResponseBytes`, and when the sum of `evaluatedBytes` reaches `MAX_EVALUATED_BYTES_PER_PAGE`.
- **Start key.** A page that starts at `lastEvaluatedKey` visits no partition before the owner of that key.
- **Boundary cases.** One test for each case 1 to 5 of section 4.2.7, and a case 5 with an empty `v`. Each test
  stops a page at the visit limit after that kind of empty slice. The next page starts with one step inside `E` and
  then continues at `S`.
- **Found key.** For random targets and seeds, `xxh64` of the string found key and of the binary found key equals the
  target, and the string passes `validateItemKeys`. The found key has the type of the start key, else of `L`, else it
  is a string.
- **Collector.** A step that reads rows of two hash keys and stops inside its slice gives `lastEvaluated` with the
  hash key of the last row, and the next page starts after that row.
- **Step error.** A step that still fails after the retry policy makes `scanItems` throw. A retry of the page with the
  same `exclusiveStartKey` returns the same items.
- **Fallback.** `boundaryKey` and the found-key search are pure functions, and the cap is an argument. A unit test calls
  them with a cap of 0. The page returns `key(L)` when it has `L`, and it does one more step when it has none.
- **`smallestValidAtOrAfter`.** A cut UTF-8 character of 2, 3, and 4 bytes, the lead bytes `E0`, `ED`, `F0`, and `F4`,
  the boundary `[0xFF]`, and GSI keys cut in each part and in each separator. Include GSI base sort keys, string and
  binary, where one value is a prefix of another and the next byte is below `0x3A`, for example `"order#01"` and
  `"order#0100"`, or `[01]` and `[01 00]`. For short keys, the result equals the result of a reference that tries all
  keys.
- **GSI case 5 after a range split.** A promoted GSI hash key with entries that share the GSI sort key and the base
  hash key, and the base sort keys `"order#0099"`, `"order#0100"`, and `"order#0101"`. A range split puts
  `"order#0100"` alone in one partition. Delete that entry, and stop a page at the visit limit after that partition.
  The next page returns `"order#0101"`.
- **Segments.** The union of all segments of `T` equals the full scan, for some values of `T`. A start key outside
  its segment fails with `scan_start_key_outside_segment`.
- **GSI.** The same walk on a GSI, with two entries that have the same GSI keys and different base keys.
- **Property-based.** For a random set of items and a random sequence of splits between pages, the pages of a scan
  together return each item exactly once. Read `test/property-based/README.md` first.
- **No regression.** The `queryItems` suites and the routing suites pass without a change.
- **Position routing.** For random keys, on topologies with hash splits, range splits, and promotions,
  `#resolve(key)` and `#resolvePosition(positionOf(key))` give the same resolution. A scan step calls `hooks.admit`
  with `keys: []`, and its forward learns the same routes as a point read of a key with the same routingHash.

## 5. Alternative options

- **An opaque cursor with the routingHash.** The cursor could hold the exact position. Rejected: the public API must
  contain only keys.
- **A reserved key that holds the position**, for example `"~fokos.scan.<position>"`. It is exact and simple.
  Rejected: it puts the routingHash in the API, and it needs a reserved prefix that items cannot use.
- **A key `(rh, smallest hk, "")` at the boundary.** Rejected: the client always computes `rh` from `hk`, so the caller
  cannot choose `rh`. The key resumes at `xxh64(smallest hk)`, a random position.
- **A random search for a key whose routingHash falls in the empty region.** The client tries `"<prefix>" + counter`
  until `xxh64` falls in the region. Rejected: the expected number of tries is `2^64 / width`. For a table with
  10,000,000 leaves and a page of 100 empty leaves, 10,000 tries find a key with a chance of about 10%. The search
  cannot work for a region inside one routingHash.
- **Walk past the visit limit until the page evaluates an item.** Rejected: a long run of empty partitions, for example
  after a mass delete, gives a page with no bound on its latency. 1,000 empty partitions take 10 to 100 s.
- **Fail at the visit limit.** Rejected: a table with many empty partitions cannot be scanned.
- **Always a binary found key.** It needs no search. Rejected: it breaks a caller who declares `HashKey` as `string`,
  and it does not survive JSON.
- **Always a string found key.** Rejected: it breaks a caller who declares `HashKey` as `Uint8Array`. The type of the
  hash key of the request is the better choice (section 4.2.8).
- **A binary found key when the string search reaches its cap.** Rejected: it has the type problem of a binary key,
  and the fallback of section 4.2.6 keeps the type of the caller.
- **A new `scan` dispatch shape.** Rejected: one step goes to one owner, which is the `point` contract. A new shape
  copies the point routing and adds one more shape.
- **Change `range` to take a storage interval now.** Rejected for this RFC: `#planRange`, `planRangeFrontier`, the
  read-through gate, and the range retries all depend on one hash key. The change rewrites the planner of the query
  path. Section 8 describes it as a follow-up.
- **A walk across slices inside a Durable Object.** Rejected: no partition covers an interval across roots, and the
  host never makes a stub to a peer of its own class.

## 6. Frequently asked questions

**Why can `lastEvaluatedKey` name a key that holds no item?**
A slice boundary of a hash partition is a routingHash, and a routingHash is not a key. FokosDB does not track which
partitions are empty, so a page cannot always stop at a real item. A key inside the last empty slice is the only way to
make progress with keys only.

**Why does the next page visit a partition again?**
The next page starts at the owner of the start key. When the page stopped at a slice boundary, that owner holds `E`,
which the page already read. The first step finds no row and continues at `S`. The cost is one RPC.

**Is the order correct when string keys and binary keys are mixed?**
Yes. A string encodes to UTF-8 with no tag and a binary key to `0xFF` and its bytes. Both sort in one byte order, and
`xxh64` hashes the encoded bytes. A found key of either type has a position inside `E`.

**Are the items in key order?**
No. They are in the storage order `(routingHash, hk, sk)`. The items of one hash key come together, in sort-key order.

**Can a caller build `exclusiveStartKey`?**
Yes. Any valid key works. With a segment, the routingHash of the key must be inside the segment.

**Why is there no fingerprint as in the `queryItems` cursor?**
A scan has no list of sub-queries for the fingerprint to protect. The start key alone gives the position.

**Why does the client not cache the found key?**
The client computes a found key at most once for each page, and only when the page stops after an empty slice. The
cost is 0.33 ms on average, and a page takes 1 to 10 s. A cache gets a hit only when two pages stop at the same
boundary. The next page of a segment stops at a different boundary, and two segments have no boundary in common, so
almost only a retry of the same page gets a hit. A cache in the partition has the same low hit rate. It also needs an
in-memory entry that a split or a promotion makes stale, and a field in each step response. A partition does not know
whether the client stops after its step, so it must compute the key for each empty step, or the client must ask for it.

**What happens when a partition splits during a scan?**
The position of each item stays the same. The next step goes to a router, which forwards it to the new owner. See
section 4.2.10.

## 7. References

- `AGENTS.md`
- `docs/agent-plans/query-items-design.md`
- `docs/agent-plans/2026-09-11-query-items-selection.md`
- `docs/agent-plans/2026-09-14-read-projections-and-query-filters.md`
- `docs/agent-plans/2026-09-27-learned-routes-for-every-dispatch-shape.md`
- `docs/agent-plans/2026-11-22-gsi-sort-key-total-order.md`
- [DynamoDB API Reference: Scan](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html)
- [DynamoDB Developer Guide: Parallel scan](
  https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan)

## 8. Appendix: dispatch by footprint and execution

This appendix records a follow-up. A separate RFC must give its design.

### 8.1 The problem

Each dispatch shape answers two questions at once: what the request touches, and how the runtime runs it.

| Shape | What the request touches | How the runtime runs it |
|---|---|---|
| `point` | one key | one owner |
| `group` | a set of keys | fan out by owner, with a failure policy |
| `single_owner` | a set of keys | one owner, or `notApplicable` |
| `range` | an interval inside one hash key | an ordered walk, where the host owns the budget |
| `local` | nothing | this partition |

When the two questions are separate, `point` is a `group` with one item, and `single_owner` is a `group` with a
stricter policy. The FokosDB key model also goes into the sharding layer. `FokosRangeInput` is
`{ hashKey, interval: SkInterval }`, while the sharding layer has its own coordinate, `FokosStorageInterval`.
`storageInterval()`, `ownedStorageInterval()`, and the migration reads use it.

Each new operation with a new footprint adds a shape, and the shapes get harder to understand.

### 8.2 The rough idea

1. **One internal coordinate.** The runtime uses positions and storage intervals everywhere. `RouteKey` is a helper at
   the edge. This RFC starts the work for `point`.
2. **Two fields in the descriptor.** The footprint is `positions(req)` or `interval(req)`. The execution is
   `one_owner`, `fan_out(failurePolicy)`, or `walk`. `local` stays separate.
3. **One frontier on the composite axis.** `planRangeFrontier` plans a storage interval. A hash partition can then
   plan more than one visit: its local slices, its hash children, and the range roots of promoted routingHashes.
   `projectOnHashKey` and `SkInterval` leave the sharding layer, and `queryItems` builds its interval in the host. An
   inclusive sort-key bound `u` becomes the exclusive bound `u || 0x00`. The RFC must prove this against the lookup of
   learned slices.
4. **One read-through gate**, on positions and intervals, for each kind of execution.
5. **A walk inside a Durable Object for a scan**, if the walk in the client is too slow.

Items 2 and 3 change the transaction operations and the query path. The property-based suites are the gate for that
RFC.

## 9. Appendix: the inversion of `xxh64`

This appendix gives the steps that compute a found key (section 4.2.8). `xxh64` is a fast hash, not a cryptographic
hash. Each step of `xxh64` is a bijection. When the length of the input is 16 bytes and the first 8 bytes are fixed,
the last 8 bytes map one to one to the 64-bit result. Thus each target `T` has exactly one value of the last 8 bytes,
and the client computes it when it runs the steps in the reverse order.

### 9.1 `xxh64` of a 16-byte input

All arithmetic is modulo `2^64`. `P1` to `P5` are the constants of `xxh64`. The input is two 64-bit lanes, read in
little-endian order.

```
acc = seed + P5 + 16                       // 16 is the length of the input

for lane in [lane1, lane2]:                // the round of each 8-byte lane
    k   = rotl(lane * P2, 31) * P1
    acc = rotl(acc ^ k, 27) * P1 + P4

acc ^= acc >> 33;  acc *= P2               // the avalanche
acc ^= acc >> 29;  acc *= P3
acc ^= acc >> 32
return acc
```

A 16-byte input has no tail of 4 bytes or of 1 byte. Those steps apply only to other lengths. The fixed length keeps
the function to exactly the steps above.

### 9.2 The inverse of each operation

| Operation | Inverse | Why the inverse exists |
|---|---|---|
| `x + c` | `x − c` | addition modulo `2^64` |
| `x ^ c`, with `c` known | `x ^ c` | xor is its own inverse |
| `rotl(x, r)` | `rotl(x, 64 − r)` | a rotation keeps all bits |
| `x * P`, with `P` odd | `x * P⁻¹` | an odd number has an inverse modulo `2^64` |
| `y = x ^ (x >> s)` | repeat `x = y ^ (x >> s)` | the top `s` bits do not change, and each pass gives `s` more bits |

**The inverse of an odd constant.** The Newton step `x = x * (2 − a * x)` doubles the number of correct low bits.
For an odd `a`, the start value `x = a` has 3 correct bits, because `a² ≡ 1 (mod 8)`. Six steps give 3, 6, 12, 24,
48, and 96 bits, which is more than 64. The client computes `P1⁻¹`, `P2⁻¹`, and `P3⁻¹` one time.

**The inverse of a xor-shift.** In `y = x ^ (x >> s)`, the top `s` bits of `y` are the top `s` bits of `x`. Each pass
of `x = y ^ (x >> s)` gives the next `s` bits. A shift of 32 or 33 needs 1 pass. A shift of 29 needs 2 passes.

### 9.3 The second lane from `T`

The first lane `lane1` is fixed. The steps are:

```
// 1. Undo the avalanche. The result is the state after lane 2.
x = unxorshr(T, 32);  x *= P3⁻¹
x = unxorshr(x, 29);  x *= P2⁻¹
acc2 = unxorshr(x, 33)

// 2. Compute the state after lane 1. All its inputs are known.
acc1 = rotl((seed + P5 + 16) ^ round(lane1), 27) * P1 + P4

// 3. Solve acc2 = rotl(acc1 ^ k2, 27) * P1 + P4 for k2.
k2 = rotl((acc2 − P4) * P1⁻¹, 64 − 27) ^ acc1

// 4. Solve k2 = rotl(lane2 * P2, 31) * P1 for lane2.
lane2 = rotl(k2 * P1⁻¹, 64 − 31) * P2⁻¹
```

The client writes `lane2` in little-endian order as the last 8 bytes. The 16 bytes then have `xxh64 = T` exactly. One
solution is a small number of 64-bit operations, about 0.8 µs in workerd.

- **Byte order.** A routingHash is stored with the most significant byte first. The client reads it as a big-endian
  number to get `T`. The lanes are little-endian, as `xxh64` requires.
- **Seed.** The seed is the seed of the shard group (`routingHashOf`). The client already has it for routing. A GSI
  has its own seed.

### 9.4 A valid string key

The solved `lane2` can have any byte value. A string key must be well-formed UTF-8 with no NUL. The client accepts
only ASCII `0x01–0x7F` in `lane2`.

- `lane2` is like a random 64-bit value. The chance that all its 8 bytes are in `0x01–0x7F` is `(127/256)^8`, about
  `1/270`.
- When a byte is out of the range, the client changes `lane1`. `lane1` is `"fokos"` and a counter of 3 base-36
  characters. A different `lane1` gives a different `acc1`, and thus an independent `lane2`. The client solves again.
- The number of tries has a geometric distribution with a mean of about 270. The chance of more than `n` tries is
  `(1 − 1/270)^n`, about `e^-37` for the cap of 10,000 tries.

The result is 16 ASCII bytes: 8 fixed characters and 8 computed characters. `KeyCodec.encode` of this string gives the
same 16 bytes, so the next page hashes the same bytes and gets `T`.

**A binary key needs no search.** `lane1` starts with the tag `0xFF`, and the client uses the solved `lane2` as it is,
because a binary key accepts any byte value. The public key is the 15 bytes after the tag.

### 9.5 Why the inversion is safe

- **It is the standard `xxh64`.** A prototype gave the same result as `xxhash-wasm`, which `hash-primitives.ts` uses.
  It matched 1,000 targets in Node.js and 500 targets through `routingHash` in workerd.
- **It depends on the length.** The length is in the start value and selects the steps. The found key has 16 bytes,
  and a test compares the inversion with the production `routingHash`. A change of the hash function makes that test
  fail.
- **It removes no security property.** `xxh64` makes no claim that it is hard to invert. The routingHash spreads keys
  across partitions. It does not protect them. A caller who knows the seed can already find keys of one partition by
  trial. The inversion is a faster way to do the same thing.
