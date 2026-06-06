# Topology Propagation via Response Piggyback

## Context and Goal

FokosDB partitions form a tree via hash splits: when a partition exceeds its size threshold it splits into K children (`hashSplitN`, fixed, 2-255). After splitting, the parent becomes a pure router — it owns no data and forwards every request to the appropriate child via `withSplitForwarding`. If that child has also split, it forwards again, and so on.

For a tree of depth D, every request takes D hops. Each hop is a full Cloudflare Workers RPC call. The routing at each level is **deterministic**: `childIdx = xxHash32(hashKey + level, GOLDEN_RATIO) % K`. If any ancestor knew the tree shape below it, it could compute the leaf partition directly and skip all intermediate routers.

**Goal**: propagate subtree topology information upward so that ancestor partitions can forward requests directly to deeper descendants, reducing the number of hops per request. The optimization is best-effort — correctness never depends on cached topology. A stale or missing cache entry just means an extra hop (the existing behavior).

---

## Current System

- `PartitionTopologyRouterImpl.pickPartition()` hashes to a **root partition** only — it has no knowledge of splits below the root.
- `PartitionDO.withSplitForwarding()` checks `shouldAllow()` → "forward" → `pickChildPartition()` → RPC to immediate child. If the child has also split, the same thing repeats recursively.
- Each partition knows ONLY about its immediate children (stored in `SplitStatusKVItem.childPartitionContexts`).
- The `PartitionTopologyEncoded` parameter is currently unused (`""` placeholder).

**The cost**: for a depth-D subtree, every request incurs D sequential RPC hops through pure-router partitions that do no useful work beyond forwarding.

---

## Design: Response Piggyback (Option A)

Every forwarded response carries a **routing hint** — the depth of the actual leaf that served the request. When a partition forwards a request and receives a response, it uses the hint to update an in-memory subtree topology cache. Future requests for the same hash space route directly to a deeper descendant, skipping intermediate hops.

### Depth definition

Depth follows the existing `PartitionIdHelper` convention: **root = depth 0**. Depth counts sub-tree levels below the root, not the root itself. This matches the partition ID wire format (`depth u8` field) and the hash seed computation (`hash(hashKey + (depth + 1), K)`).

### Routing hint format

```typescript
type RoutingHint = {
  forwardCount: number;  // +1 on every forward, regardless of partition type (existing field)
  hashDepth: number;     // +1 only when a hash partition forwards to a hash child
  rangeDepth: number;    // +1 only when a range partition forwards to a range child
};
```

Three counters, each with distinct incrementing rules:

- **`forwardCount`**: total number of forwards in the chain. Incremented by every partition that forwards, regardless of type. This is the existing field already present in response metadata.
- **`hashDepth`**: number of hash-to-hash forwards. A hash partition increments this when it forwards to a hash child (split forwarding). It does NOT increment when forwarding to a range root (promotion) — that is a cross-type hop.
- **`rangeDepth`**: number of range-to-range forwards. A range partition increments this when it forwards to a range child.

Cross-type forwards (hash partition → range root via key promotion) increment `forwardCount` only. Neither `hashDepth` nor `rangeDepth` is incremented because no same-type split was traversed.

A partition serving locally contributes 0 to all counters. `hashDepth = 0` means "I am the hash leaf, no hash splits below me." `rangeDepth = 0` means "I am the range leaf, no range splits below me."

**Example trace** — hash tree of depth 2 with a promoted key leading to a range tree of depth 1:

```
root(hash,d=0) → child(hash,d=1) → grandchild(hash,d=2,promoted HK) → rangeRoot → rangeChild(leaf)

Response built bottom-up:

  rangeChild serves locally             → {forwardCount:0, hashDepth:0, rangeDepth:0}
  rangeRoot  → rangeChild (range→range) → {forwardCount:1, hashDepth:0, rangeDepth:1}
  grandchild → rangeRoot  (hash→range)  → {forwardCount:2, hashDepth:0, rangeDepth:1}  ← cross-type, no depth increment
  child      → grandchild (hash→hash)   → {forwardCount:3, hashDepth:1, rangeDepth:1}
  root       → child      (hash→hash)   → {forwardCount:4, hashDepth:2, rangeDepth:1}
```

The root learns: `hashDepth = 2` means the hash leaf is 2 levels below. It populates its subtree cache by marking depths 0 and 1 as split (using `hash(hk + 1, K)` and `hash(hk + 2, K)`). The `rangeDepth = 1` is not used by the hash subtree cache — range topology caching is future work.

The ancestor does not need the leaf's full context or identity. Since hash routing is deterministic, the ancestor can recompute the full path for any `hashKey` at each depth using `hash(hk + (depth + 1), K)`. Knowing the `hashDepth` is sufficient to infer which hash nodes along the path have split.

### Promoted keys and cross-type boundaries

When a hash partition has a promoted key, it forwards the request to the range root partition for that hashKey. This is a cross-type boundary — the request leaves the hash tree and enters the range tree. The two trees have independent depth counters:

- The hash subtree cache maintained by hash partitions uses `hashDepth` only. It tracks which hash descendants have split. The promoted key forwarding is handled by the hash leaf's own `withSplitForwarding` logic (checking `#_promotedKeys` before `shouldAllow`), not by the subtree cache.
- A future range subtree cache could use `rangeDepth` similarly, but range routing requires sortKey boundary knowledge (not just depth), so this is deferred.

The cross-type boundary means that a hash partition's subtree cache correctly stops at the hash leaf — it doesn't try to encode the range tree structure, which has different routing semantics (boundary-based, not hash-based).

### Learning flow

1. Partition P has a subtree cache. A request arrives for hashKey `hk`.
2. P traverses its cache: at each depth, compute `childIdx = hash(hk + (depth+1), K)`, check if that node is marked as split. Follow splits until reaching a node marked as leaf (or unknown).
3. P forwards to that node (the deepest known descendant along this path).
4. The response comes back with `hashDepth = 7` (but P's cache only knew about depth 4).
5. P updates its cache: mark nodes at depths 4, 5, 6 along this path as split. Depth 7 is the leaf — do not mark it.
6. Next request hashing to the same path: P routes directly to depth 7.

### Staleness and self-correction

If a cached leaf has since split further (P's cache is stale), the request reaches a partition that has itself split. That partition's `shouldAllow()` returns "forward", it routes to its child, and the response comes back with a deeper `hashDepth`. P updates its cache — the staleness self-corrects on the next request. Nodes are never removed from the tree — even after splitting, a partition remains as a pure router/forwarder. This is a core invariant of the system, which means a cached path can never point to a non-existent node. The only form of staleness is "the cached leaf has split further," which is always self-correcting.

---

## Subtree Cache Data Structures

Each partition maintains an in-memory cache of its subtree's topology (which descendants have split). The cache has a fixed memory budget (e.g. 1MB). Two representations were evaluated: LOUDS bit vector and flat arena array. Both exploit the fixed-fanout property (every split creates exactly K children).

### Option A: LOUDS Bit Vector with In-Place Updates

LOUDS (Level-Ordered Unary Degree Sequence) for fixed-fanout trees stores **1 bit per node** in BFS order: 1 = split (internal), 0 = leaf. Navigation uses `rank1` (popcount prefix) to compute child positions.

**Space**: N bits for N nodes. 1M nodes = 125 KB. Extremely compact.

**Navigation**: children of node at BFS position `p`:

```
rank1_exclusive(p) = number of 1-bits in positions [0, p)
first_child(p) = K * rank1_exclusive(p) + 1
children at positions: first_child(p), first_child(p)+1, ..., first_child(p)+K-1
```

**Traversal pseudocode**:

```
function findLeaf(louds, hashKey, K):
    p = 0  // root BFS position
    depth = 0
    while getBit(louds, p) == 1:        // node is split
        r = rank1Exclusive(louds, p)     // count 1-bits before p
        childIdx = hash(hashKey + (depth + 1), K)
        p = K * r + 1 + childIdx         // jump to child
        depth++
    return (p, depth)                    // BFS position and depth of the leaf
```

**Update when a leaf at BFS position `p` splits**:

```
function insertSplit(louds, p, K):
    // 1. Compute where the new children go in BFS order
    r = rank1Exclusive(louds, p)         // internal nodes before p
    insertionPoint = K * r + 1           // BFS position of first child

    // 2. Flip the leaf to internal
    setBit(louds, p, 1)

    // 3. Make room: shift all bits from insertionPoint onward by K positions
    //    This is a memmove on the underlying byte array.
    shiftBitsRight(louds, insertionPoint, K)

    // 4. Initialize the K new child positions as leaves (0)
    for j in 0..K-1:
        setBit(louds, insertionPoint + j, 0)

    // 5. Rebuild the rank index (superblock popcount table)
    rebuildRankIndex(louds)
```

**Full update flow when a response reveals `leafDepth`**:

```
function updateFromHint(louds, hashKey, knownDepth, leafDepth, K):
    // Navigate to the node at knownDepth (where we thought the leaf was)
    p = navigateToDepth(louds, hashKey, knownDepth)

    // Mark each newly-discovered split, shallowest first
    for d in knownDepth .. leafDepth-1:
        insertSplit(louds, p, K)
        // After insertion, the child we care about is at the insertion point
        childIdx = hash(hashKey + (d + 1), K)
        r = rank1Exclusive(louds, p)
        p = K * r + 1 + childIdx
```

Each `insertSplit` shifts `N/8` bytes (the tail of the bit vector). For N = 1M nodes (125 KB): the shift moves ~125 KB of data, which takes approximately 10-50 microseconds on modern hardware. For 2 updates per 10 seconds, total CPU cost is negligible (~100 microseconds per 10 seconds).

**Eviction**: since LOUDS stores nodes in BFS order, the deepest levels are at the tail. Truncation = reduce the bit length counter. No data movement. This naturally discards the least valuable information first (deeper levels have exponentially lower ROI per bit because they affect a `1/K^d` fraction of requests).

**Tradeoffs**:

- Extremely space-efficient: 1M nodes in 125 KB.
- Update requires O(N) bit shifting per discovered split.
- Requires rank/select implementation (moderate complexity).
- BFS-order truncation gives optimal depth-based eviction for free.

### Option B: Flat Arena Array (Uint32Array)

A `Uint32Array` used as an arena allocator. Each **node block** is K consecutive slots, one per child. A slot value of `0` means "child is a leaf or unknown." A value `> 0` is the array index where that child's own K-slot block begins.

The root block occupies slots `[0, K-1]`. New blocks are appended at `nextFree` (initially `K`). No data is ever moved or shifted.

**Example with K=3**:

```
Initial (root split, all children are leaves):
  array: [0, 0, 0]     nextFree = 3
          c0 c1 c2 ← root's children

Child 1 splits (allocate block at index 3):
  array: [0, 3, 0,  |  0, 0, 0]     nextFree = 6
          c0 c1 c2     c0 c1 c2
          └ root ┘     └ child1 ┘
              └────────────┘

Child 1's child 2 splits (allocate block at index 6):
  array: [0, 3, 0,  |  0, 0, 6,  |  0, 0, 0]     nextFree = 9
          └ root ┘     └ child1 ┘    └ c1.c2 ┘
                            └────────────┘
```

**Traversal pseudocode**:

```
function findLeaf(array, hashKey, K):
    block = 0                                // root block starts at index 0
    depth = 0
    while true:
        childIdx = hash(hashKey + (depth + 1), K)
        ptr = array[block + childIdx]
        if ptr == 0:
            break                            // child is a leaf or unknown
        block = ptr                          // descend to child's block
        depth++
    return (block, depth)
```

**Update pseudocode**:

```
function updateFromHint(array, nextFree, hashKey, knownDepth, leafDepth, K, maxSize):
    // Navigate to the node at knownDepth
    block = navigateToBlock(array, hashKey, knownDepth)

    for d in knownDepth .. leafDepth-1:
        childIdx = hash(hashKey + (d + 1), K)
        if array[block + childIdx] == 0:
            // Check budget before allocating
            if nextFree + K > maxSize:
                break                        // budget exhausted, stop learning
            // Allocate new block at the end — no shifting, no pointer fixup
            newBlock = nextFree
            nextFree += K
            for j in 0..K-1:
                array[newBlock + j] = 0      // initialize children as leaves
            array[block + childIdx] = newBlock
        block = array[block + childIdx]

    return nextFree
```

**Space**: each split node occupies `K × 4` bytes. The table below shows **worst-case** space (every node at depths 0 through D-1 is split) for different fanout K and depth cap D:

| K \ D | 5 | 10 | 20 |
|---|---|---|---|
| **2** | 248 B | 8 KB | 8 MB |
| **4** | 5.3 KB | 5.3 MB | ~5.9 TB |
| **8** | 146 KB | 4.6 GB | — |
| **16** | 4.3 MB | — | — |

Maximum depth D that stays within a 1 MB budget (worst case, fully split tree):

| K | Max D in 1 MB | Max split nodes |
|---|---|---|
| 2 | 17 | 131,071 |
| 4 | 8 | 21,845 |
| 8 | 5 | 4,681 |
| 16 | 4 | 4,369 |

**Tradeoffs**:

- O(1) append-only updates. No data movement, ever.
- Dead simple: no rank/select, no bit manipulation, just array indexing.
- Zero-copy serialization: save/load the `Uint32Array` buffer directly to/from KV.
- 30-100x less space-efficient than LOUDS (32-bit pointers vs 1-bit flags).
- Fragmentation: if eviction is needed, freed blocks leave holes that require compaction (O(N) pointer fixup) or a free list.

### Option B Eviction Variants

Three approaches to staying within the memory budget:

#### B1: Depth-Limited (cap at D levels)

During update, refuse to allocate blocks beyond depth D:

```
if depth >= MAX_DEPTH:
    break    // don't allocate, fall back to hop-by-hop for deeper levels
```

No fragmentation, no holes, no compaction. The array fills from the top of the tree. D is chosen so the worst-case fully-split space fits in the budget (e.g. D=8 for K=4 → 341 KB worst case).

The depth limit can be tuned per-K:

| K | Recommended D | Worst-case space | Hops saved |
|---|---|---|---|
| 2 | 15 | 256 KB | 15 |
| 4 | 8 | 341 KB | 8 |
| 8 | 5 | 146 KB | 5 |

**Caches compose across the chain**: if every partition in the tree caches D=8 levels and the full tree is 30 deep, a request goes: root → depth 8 → depth 16 → depth 24 → leaf. That is 4 hops instead of 30.

#### B2: Budget-Limited (fill to capacity, then stop)

Allocate blocks at any depth, first-come-first-served, until `nextFree × 4 >= BUDGET`:

```
if nextFree + K > maxSlots:
    break    // array full, stop learning new splits
```

No depth restriction — deep splits are learned if they arrive early. Discovery order is roughly traffic-proportional: hot paths are discovered first. Once full, no new topology is learned.

This naturally captures the most-traversed paths. However, a burst of deep-branch traffic early on can fill the array with deep, narrow knowledge that helps a small fraction of requests, leaving no room for broader shallow coverage later.

#### B3: Hybrid (depth limit + budget cap)

Combine a relaxed depth limit with a budget cap. Use a depth limit higher than the "safe" worst-case value (e.g. D=12 for K=4 instead of D=8), accepting that the worst case exceeds 1 MB, but enforce a hard budget cap as a backstop:

```
if depth >= RELAXED_MAX_DEPTH:
    break
if nextFree + K > maxSlots:
    break
```

This allows deeper coverage when the tree is sparse (most real trees are — not every node at every level splits). The depth limit prevents unbounded depth exploration, and the budget cap prevents memory blowout if many branches DO split deeply.

In practice the tree is rarely fully split: a fully-split K=4 tree at D=12 requires 100+ TB of data under a single root partition. The hybrid approach exploits this: set D=12 (allowing 12 hops saved when the tree is sparse), and the 1 MB budget cap handles the unlikely case where many branches fill in.

If the budget is exhausted mid-update (deep discovery that would overflow), the partition simply stops learning at that point. The remaining depth falls back to hop-by-hop — no different from the pre-optimization behavior.

---

## Persistence

Both structures serialize trivially for KV storage:

```typescript
// LOUDS
ctx.storage.kv.put("__topo_louds", loudsBytes.subarray(0, Math.ceil(bitLength / 8)));
ctx.storage.kv.put("__topo_len", bitLength);

// Flat array
ctx.storage.kv.put("__topo_arena", array.buffer.slice(0, nextFree * 4));
ctx.storage.kv.put("__topo_next", nextFree);
```

On DO startup (`blockConcurrencyWhile`), load and rebuild any derived structures (rank index for LOUDS, or just restore `nextFree` for the flat array). Both are fast: O(N/64) for rank rebuild, O(1) for flat array restore.

---

## Comparison Summary

| | LOUDS (Option A) | Flat Array (Option B) |
|---|---|---|
| **Space per split node** | ~1 bit | K × 4 bytes |
| **Nodes in 1 MB (K=4)** | ~8M | ~65K |
| **Update cost** | O(N) bit shift | O(1) append |
| **Traversal cost** | O(D) rank queries | O(D) array reads |
| **Implementation** | Moderate (rank/select) | Simple (array index) |
| **Serialization** | memcpy | memcpy |
| **Eviction** | BFS tail truncation (natural) | Depth cap or budget cap |
| **Fragmentation risk** | None | Only if blocks are freed |

### When to prefer LOUDS

The subtree is very large (>100K splits) and the partition needs maximum coverage. The O(N) shift per update is acceptable because splits are rare (~seconds apart). Space efficiency is critical.

### When to prefer Flat Array

Simplicity is paramount. The subtree is moderate (<50K splits for K=4) or the depth cap provides sufficient coverage. O(1) updates with zero data movement are preferred. The B3 hybrid variant (depth limit + budget cap) provides good coverage for typical trees while staying within budget.

---

## Recommendation

Start with **Option B3 (flat array, hybrid eviction)**:

- Set `RELAXED_MAX_DEPTH` to 12 for K=4 (or `ceil(log_K(BUDGET / (4*K))) + 2` as a formula).
- Set `BUDGET` to 1 MB.
- Implement the flat array with append-only allocation and the two-condition eviction check.
- The implementation is minimal: a `Uint32Array`, an integer `nextFree`, and ~30 lines of traversal/update logic.
- Persistence is trivial (one KV write of the raw buffer).

If profiling later shows that coverage gaps matter (the 65K split node capacity is insufficient for a real workload), consider upgrading to Option A (LOUDS) for 100x better space efficiency, accepting the moderate implementation complexity and O(N) update cost.

---

## Future Work: TopologyKeeperDO and Workers KV

This ADR covers DO-level topology caching only (each partition caches its own subtree). A future TopologyKeeperDO will maintain the authoritative full-tree topology, compiled into a LOUDS encoding and stored in Workers KV. The entry-point `PartitionTopologyRouterImpl` will fetch this on cold start for direct 1-hop routing to the leaf. The DO-level piggyback caching described here complements the TopologyKeeper by providing self-healing when the Workers KV cache is stale — the two layers are independent and compose cleanly.
