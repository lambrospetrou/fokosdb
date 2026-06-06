# Hash Topology Cache — Implementation Plan

Implement a per-partition-DO subtree topology cache that lets hash partitions skip intermediate
router hops when forwarding requests. The cache is the "Option B3: flat arena array with hybrid
depth-limit + budget-cap eviction" from `docs/ideas/topology-propagation-via-piggyback.md`.

## Background

When a hash partition splits, it becomes a pure router. A tree of depth D requires D sequential RPC
hops per request. Each hop is deterministic: `childIdx = xxHash32(hashKey + (depth + 1), GOLDEN_RATIO) % K`.
If an ancestor knew which descendants have split, it could compute the leaf partition directly and
forward in one hop.

The cache is best-effort. A stale or missing cache entry just means an extra hop (existing behavior).
Partitions never disappear from the tree — a cached path can never point to a non-existent node.
Staleness self-corrects: the stale target is a router, it forwards one more level, and the response
carries the updated depth.

## Phases

### Phase 0: Extract shared hash routing primitives

**Problem:** The hash child index computation (`xxHash32(hashKey + (depth + 1), GOLDEN_RATIO) % K`)
currently lives as a private method on `PartitionTopologyRouterImpl` and is duplicated across
`pickChildPartition`, `makeIsCorrectChildHashPartition`, and `findPartition`. The new `HashTopology`
class and `pickDescendantHashPartition` method would duplicate it further.

**Solution:** Extract two exported functions that become the single source of truth.

File: `src/lib/partition-topology/partition-topology.ts`

Export the existing `GOLDEN_RATIO` constant (currently module-private).

Add two exported free functions:

```typescript
/**
 * Given a hashKey and a parent's absolute depth, compute which child slot (0..K-1)
 * the key routes to. This is the single source of truth for hash-based child selection.
 */
export function hashChildIndex(hashKey: string, parentAbsDepth: number, K: number): number {
  return xxHash32(hashKey + (parentAbsDepth + 1), GOLDEN_RATIO) % K;
}

/**
 * Compute the root partition index for a hashKey across rootTreesN root partitions.
 * Separate from hashChildIndex because root selection has no depth suffix.
 */
export function hashRootIndex(hashKey: string, rootTreesN: number): number {
  return xxHash32(hashKey, GOLDEN_RATIO) % rootTreesN;
}
```

Then refactor all existing call sites in `PartitionTopologyRouterImpl`:

1. **`pickChildPartition`**: replace `this.hash(hashKey + (depth + 1), partitionContext.hashSplitN)`
   with `hashChildIndex(hashKey, depth, partitionContext.hashSplitN)`.

2. **`makeIsCorrectChildHashPartition`**: replace `this.hash(hashKey + childLevel, childContext.hashSplitN)`
   with `hashChildIndex(hashKey, childLevel - 1, childContext.hashSplitN)`.
   (`childLevel` is the child's absolute depth, so the parent's depth is `childLevel - 1`.)

3. **`findPartition`**: replace `this.hash(hashKey, this.#topology.length)` with
   `hashRootIndex(hashKey, this.#topology.length)`. The inner loop hash computation does not need
   changing yet (it operates on the in-memory topology tree which is currently roots-only).

4. Remove the private `hash` method from `PartitionTopologyRouterImpl` — it is no longer needed.

**Verification:** all existing `partition-topology.test.ts` tests must pass unchanged after this
refactor. Run `vitest -t` against the topology tests to confirm.

### Phase 1: `HashTopology` class

New file: `src/lib/partition-topology/hash-topology.ts`

#### Data structure

A `Uint32Array` used as an arena allocator. Each **node block** is `K` consecutive slots (one per
child). A slot value of `0` means "child is a leaf or unknown." A non-zero value is the array index
where that child's own K-slot block begins.

The root block occupies slots `[0, K)`. New blocks are appended at `nextFree` (initially `K`). No
data is ever moved or shifted.

Instance fields:
- `arena: Uint32Array` — pre-allocated to `maxSlots` entries
- `nextFree: number` — next allocation index
- `K: number` — fanout (= `hashSplitN`, range 2–255)
- `maxDepth: number` — relaxed depth cap (B3 hybrid)
- `maxSlots: number` — hard budget cap in slots (= `budgetBytes / 4`)

#### Hash computation

Import `hashChildIndex` from `partition-topology.ts` (extracted in Phase 0). All traversal and
update logic calls `hashChildIndex(hashKey, ownerAbsDepth + relDepth, K)` — no hash logic lives
inside `HashTopology` itself.

#### API

```typescript
// Snapshot for single-key KV persistence. `arena` is a Uint8Array VIEW (no copy).
export type HashTopologySnapshot = {
  arena: Uint8Array;   // view: new Uint8Array(arena.buffer, 0, nextFree * 4)
  nextFree: number;
  K: number;
};

export class HashTopology {
  // Create an empty cache. Root block is allocated (K slots, all zero).
  static create(K: number, opts?: { maxDepth?: number; budgetBytes?: number }): HashTopology;

  // Restore from a previously persisted snapshot.
  static fromSnapshot(snapshot: HashTopologySnapshot,
                      opts?: { maxDepth?: number; budgetBytes?: number }): HashTopology;

  // Serialize. The `arena` field is a zero-copy Uint8Array view over the used portion.
  // The actual copy happens inside kv.put (structured clone).
  toSnapshot(): HashTopologySnapshot;

  // Traverse the cache for `hashKey`. Returns the relative depth of the deepest known
  // descendant (0 = no splits known for this path, i.e. immediate child is the target).
  // `ownerAbsDepth`: the absolute depth of the partition that owns this cache.
  findLeaf(hashKey: string, ownerAbsDepth: number): number;

  // Learn topology from a forwarded response.
  // `ownerAbsDepth`: absolute depth of the owning partition.
  // `knownRelDepth`: what findLeaf returned before forwarding (the depth we targeted).
  // `actualRelDepth`: knownRelDepth + response.meta.hashDepth (the true leaf depth).
  // Returns true if the cache was modified (caller should persist).
  updateFromHint(hashKey: string, ownerAbsDepth: number,
                 knownRelDepth: number, actualRelDepth: number): boolean;

  isEmpty(): boolean;
  stats(): { usedSlots: number; maxSlots: number; K: number; maxDepth: number };
}
```

#### `findLeaf` pseudocode

```
findLeaf(hashKey, ownerAbsDepth):
  block = 0
  relDepth = 0
  while true:
    childIdx = hashChildIndex(hashKey, ownerAbsDepth + relDepth, K)
    ptr = arena[block + childIdx]
    if ptr == 0: break
    block = ptr
    relDepth++
  return relDepth
```

Returns 0 when the cache is empty or has no info for this path. The caller falls back to the
immediate child (existing behavior, `pickChildPartition`).

#### `updateFromHint` pseudocode

```
updateFromHint(hashKey, ownerAbsDepth, knownRelDepth, actualRelDepth):
  if actualRelDepth <= knownRelDepth: return false
  
  // Navigate to knownRelDepth
  block = 0
  for rd = 0 to knownRelDepth - 1:
    childIdx = hashChildIndex(hashKey, ownerAbsDepth + rd, K)
    block = arena[block + childIdx]  // must be non-zero (we navigated here before)

  // Allocate blocks for newly discovered splits
  updated = false
  for rd = knownRelDepth to actualRelDepth - 1:
    childIdx = hashChildIndex(hashKey, ownerAbsDepth + rd, K)
    if arena[block + childIdx] != 0:
      block = arena[block + childIdx]  // already known, descend
      continue
    // Check hybrid eviction: depth cap, then budget cap
    if rd + 1 >= maxDepth: break
    if nextFree + K > maxSlots: break
    // Allocate new block
    newBlock = nextFree
    nextFree += K
    // slots are already 0 (pre-allocated Uint32Array)
    arena[block + childIdx] = newBlock
    block = newBlock
    updated = true

  return updated
```

The level at `actualRelDepth` is the leaf — do NOT allocate a block for it.

#### Default depth caps

```typescript
function defaultMaxDepth(K: number): number {
  if (K <= 2) return 15;
  if (K <= 4) return 12;
  if (K <= 8) return 7;
  return 5;
}
```

Default `budgetBytes`: 1MB (262,144 Uint32 slots).

#### `fromSnapshot` implementation notes

- Allocate full `Uint32Array(maxSlots)`.
- Copy `snapshot.arena` bytes into the start of the new arena.
- Use `snapshot.K` as `K`, `snapshot.nextFree` as `nextFree`.
- `opts.maxDepth` and `opts.budgetBytes` override defaults if provided.

### Phase 2: Add `hashDepth` and `rangeDepth` to response metadata

File: `src/lib/types.ts`

Add two fields to `PartitionInfo`:

```typescript
export type PartitionInfo = {
  servedByActorId: string;
  servedByActorName: string;
  forwardCount: number;
  hashDepth: number;    // hash-to-hash forwards from this point to the hash leaf
  rangeDepth: number;   // range-to-range forwards (unused now; avoids future breaking change)
};
```

Update every place that constructs a `meta` object (search for `forwardCount: 0` in `do-partition.ts`
— there are several in `putItem`, `deleteItem`, `getItem`/`readItemLocally`). Add `hashDepth: 0,
rangeDepth: 0` alongside `forwardCount: 0`.

Update `withSplitForwarding` return statement — currently:

```typescript
return { ...result, meta: { ...result.meta, forwardCount: result.meta.forwardCount + 1 } } as T;
```

For now (before Phase 4 adds skip logic), just increment `hashDepth` by 1 when a hash partition
forwards to a hash child. Do NOT increment for cross-type forwards (hash→range promotion).
Specifically:

```typescript
// Inside the "forward" case of withSplitForwarding:
const isHashToHash = isHashPartition(ctx) && !isRangePartition(partitionContext);
return {
  ...result,
  meta: {
    ...result.meta,
    forwardCount: result.meta.forwardCount + 1,
    hashDepth: result.meta.hashDepth + (isHashToHash ? 1 : 0),
    // rangeDepth: similar logic when range caching is added
  },
} as T;
```

The `isHashToHash` check: the owning partition is a hash partition (`isHashPartition(ctx)`) AND the
target is also a hash partition (NOT a range partition — check via the target `partitionContext` not
having `rangePartition` set). The promotion path sets `rangePartition` on the target context via
`resolveRangePartitionContext`, so this check correctly excludes it.

Also update `RangePartitionTopologyImpl`'s `shouldAllow → "forward"` path to increment `rangeDepth`
by 1. This is in `PartitionDO.withSplitForwarding` — the range path already goes through the same
method. Check `isRangePartition(ctx) && isRangePartition(partitionContext)` for range-to-range.

### Phase 3: Generalize `pickChildPartition` → `pickDescendantHashPartition`

File: `src/lib/partition-topology/partition-topology.ts`

**Key insight:** `pickChildPartition` is just `pickDescendantHashPartition` with `relativeDepth = 1`.
Rather than adding a parallel method, generalize the existing one and keep `pickChildPartition` as a
thin wrapper.

Add a new method to `PartitionTopologyRouterImpl`:

```typescript
pickDescendantHashPartition(
  partitionContext: PartitionContextResolved,
  hashKey: string,
  relativeDepth: number,
): { doId: DurableObjectId; partitionContext: PartitionContextResolved }
```

Implementation (uses `hashChildIndex` from Phase 0):

```typescript
pickDescendantHashPartition(
  partitionContext: PartitionContextResolved,
  hashKey: string,
  relativeDepth: number,
): { doId: DurableObjectId; partitionContext: PartitionContextResolved } {
  const partitionIdBytes = partitionContext._partitionIdBytes ?? Uint8Array.fromHex(partitionContext.partitionId);
  const depth = PartitionIdHelper.depth(partitionIdBytes);
  
  const hashIdxs: number[] = [];
  for (let i = 0; i < relativeDepth; i++) {
    hashIdxs.push(hashChildIndex(hashKey, depth + i, partitionContext.hashSplitN));
  }
  
  const { doName, opaque } = new PartitionIdHelper(this.basePartitionContext, partitionIdBytes)
    .appendHashIdx(hashIdxs)
    .encode(true);
  assertExists(doName);
  
  const { ns } = this.basePartitionContext;
  const doId = env[ns].idFromName(doName);
  return {
    doId,
    partitionContext: {
      ...partitionContext,
      doName,
      primaryDoIdStr: doId.toString(),
      partitionId: opaque,
    },
  };
}
```

Then rewrite `pickChildPartition` to delegate:

```typescript
pickChildPartition(
  partitionContext: PartitionContextResolved,
  hashKey: string,
  sortKey?: string,
): { doId: DurableObjectId; partitionContext: PartitionContextResolved } {
  return this.pickDescendantHashPartition(partitionContext, hashKey, 1);
}
```

**Verification:** `pickDescendantHashPartition(ctx, hk, 1)` must produce the same `doId` and
`partitionId` as the old `pickChildPartition`. Add a test that calls both and asserts equality.

Also add `pickDescendantHashPartition` to the `PartitionTopologyRouter` interface and to the
`PartitionTopologySplitter` interface. Implement in `PartitionTopologyImpl` by delegating to the
inner `#topologyRouter`.

### Phase 4: Integration in `PartitionDO`

File: `src/lib/do-partition.ts`

#### New field

```typescript
#_hashTopology: HashTopology | null = null;
```

#### Startup — load from KV

In the `blockConcurrencyWhile` block (after loading `#_partitionContext`), if the partition is a hash
partition that has already split, load the cache:

```typescript
if (pCtx && isHashPartition(pCtx)) {
  const snapshot = ctx.storage.kv.get<HashTopologySnapshot>("__topo_cache");
  if (snapshot) {
    this.#_hashTopology = HashTopology.fromSnapshot(snapshot, {
      maxDepth: undefined,     // use default for this K
      budgetBytes: undefined,  // use default (1MB)
    });
  }
}
```

#### Create on first split

In `PartitionTopologyImpl.startSplit`, after writing `split_started` to KV, the parent partition
should initialize an empty `HashTopology` if it doesn't have one. This is done in `PartitionDO`, not
in `PartitionTopologyImpl`. After `topology.startSplit()` succeeds in `runBackgroundWork`:

```typescript
if (isHashPartition(pCtx) && !this.#_hashTopology) {
  this.#_hashTopology = HashTopology.create(pCtx.hashSplitN);
}
```

No need to persist the empty cache — it will be persisted on the first `updateFromHint`.

#### Modified `withSplitForwarding` — the `"forward"` case

Replace the existing `"forward"` case with:

```typescript
case "forward": {
  let targetRelDepth = 1;
  let target: { doId: DurableObjectId; partitionContext: PartitionContextResolved };

  // --- Promoted key forwarding (existing logic, unchanged) ---
  // This block runs before the split-forwarding check and returns early.
  // The cache only applies to hash-to-hash split forwarding below.

  // --- Skip logic (hash partitions only) ---
  if (this.#_hashTopology && isHashPartition(ctx)) {
    const absDepth = PartitionIdHelper.depth(
      ctx._partitionIdBytes ?? Uint8Array.fromHex(ctx.partitionId)
    );
    const cachedDepth = this.#_hashTopology.findLeaf(hashKey, absDepth);
    if (cachedDepth > 1) {
      targetRelDepth = cachedDepth;
      target = topology.pickDescendantHashPartition(ctx, hashKey, targetRelDepth);
    } else {
      targetRelDepth = 1;
      target = topology.pickChildPartition(ctx, hashKey, sortKey);
    }
  } else {
    target = topology.pickChildPartition(ctx, hashKey, sortKey);
  }

  const { doId, partitionContext } = target;
  const stub = this.env[this.pCtx().ns].get(doId);
  const result = await forward(stub, partitionContext);

  // --- Learn from response (hash partitions only) ---
  if (this.#_hashTopology && isHashPartition(ctx) && result.meta.hashDepth > 0) {
    const absDepth = PartitionIdHelper.depth(
      ctx._partitionIdBytes ?? Uint8Array.fromHex(ctx.partitionId)
    );
    const actualRelDepth = targetRelDepth + result.meta.hashDepth;
    if (this.#_hashTopology.updateFromHint(hashKey, absDepth, targetRelDepth, actualRelDepth)) {
      this.ctx.storage.kv.put<HashTopologySnapshot>(
        "__topo_cache",
        this.#_hashTopology.toSnapshot()
      );
    }
  }

  // --- Build return value ---
  const isHashToHash = isHashPartition(ctx) && !partitionContext.rangePartition;
  return {
    ...result,
    meta: {
      ...result.meta,
      forwardCount: result.meta.forwardCount + 1,
      hashDepth: result.meta.hashDepth + (isHashToHash ? targetRelDepth : 0),
    },
  } as T;
}
```

Key semantics:
- `forwardCount` increments by 1 (one actual RPC hop, regardless of how many levels were skipped).
- `hashDepth` increments by `targetRelDepth` (the number of tree levels accounted for). This
  preserves the invariant that `hashDepth` at any point in the chain equals the total hash tree depth
  from that partition to the leaf.

### Phase 5: Tests

#### Unit tests: `src/lib/partition-topology/hash-topology.test.ts`

1. **`create` basics**: `create(4)` returns an instance where `isEmpty()` is true, `findLeaf` returns
   0 for any key, `stats()` shows `usedSlots === 4` (root block).

2. **`toSnapshot` / `fromSnapshot` round-trip**: create, update with some hints, snapshot, restore,
   verify `findLeaf` returns the same results.

3. **`findLeaf` on empty cache**: returns 0 for any hashKey.

4. **`updateFromHint` + `findLeaf`**: update with `(knownRelDepth=0, actualRelDepth=3)`, then
   `findLeaf` returns 3 for that hashKey. A different hashKey still returns 0 (or whatever its path
   shows).

5. **Multiple paths**: two hashKeys that hash to different child slots at level 1 — updating one
   doesn't affect the other.

6. **Same-path convergence**: two hashKeys that share the same path for the first 2 levels but
   diverge at level 3 — both findLeaf correctly.

7. **Depth cap**: set `maxDepth=3`, update with `actualRelDepth=5`. `findLeaf` returns 3 (stopped at
   cap). Further calls with `actualRelDepth=5` still return 3 (no change).

8. **Budget cap**: set `budgetBytes` very small (e.g. `K * 4 * 3` = room for root + 2 blocks). Fill
   it up, then try to update — returns false, `findLeaf` stays at the capped depth.

9. **Hybrid**: maxDepth=10 but budget only fits 4 levels — budget kicks in first.

10. **Staleness self-correction**: update to depth 2, then update to depth 4 for the same key path.
    `findLeaf` returns 4.

11. **No-op update**: `updateFromHint` with `actualRelDepth <= knownRelDepth` returns false.

12. **`updateFromHint` with already-known intermediate levels**: update to depth 3, then update for a
    different key that shares the first 2 levels but diverges at level 3 — the shared levels are
    already allocated, no double allocation.

#### Integration tests

Extend existing integration tests (or add new ones in `do-partition.test.ts`) to verify:

1. **`hashDepth` propagation**: split a partition, send a request, verify response has `hashDepth: 1`
   (one hash forward). Split the child, send again, verify `hashDepth: 2`.

2. **Cache population**: after the `hashDepth: 2` response, the root's cache should have learned
   about depth 2. The next request should have `forwardCount: 1` (direct skip) but `hashDepth: 2`
   (tree depth preserved).

3. **Staleness recovery**: after cache says depth 2, split the grandchild. Next request: root
   forwards to depth 2 (stale), grandchild forwards to depth 3, root receives `hashDepth: 1` from
   the grandchild. Root updates cache to depth 3. Subsequent request: `forwardCount: 1`,
   `hashDepth: 3`.

## Files to modify

| File | Phase | Change |
|------|-------|--------|
| `src/lib/partition-topology/partition-topology.ts` | 0 | Export `GOLDEN_RATIO`; add `hashChildIndex()` and `hashRootIndex()` free functions; refactor `pickChildPartition`, `makeIsCorrectChildHashPartition`, `findPartition` to use them; remove private `hash` method |
| `src/lib/partition-topology/hash-topology.ts` | 1 | **NEW** — `HashTopology` class, imports `hashChildIndex` from above |
| `src/lib/partition-topology/hash-topology.test.ts` | 1 | **NEW** — unit tests |
| `src/lib/types.ts` | 2 | Add `hashDepth`, `rangeDepth` to `PartitionInfo` |
| `src/lib/do-partition.ts` | 2, 4 | Add `hashDepth: 0, rangeDepth: 0` to all meta objects; add `#_hashTopology` field; load/create/use/persist the cache; modify `withSplitForwarding` |
| `src/lib/partition-topology/partition-topology.ts` | 3 | Add `pickDescendantHashPartition` to router and both interfaces; rewrite `pickChildPartition` as wrapper |
