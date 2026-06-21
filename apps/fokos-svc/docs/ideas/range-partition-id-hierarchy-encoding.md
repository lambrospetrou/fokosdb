# Range Partition ID Hierarchy Encoding

## Problem

Hash partition IDs encode the full split path: `[schema, rootIdx(u16), depth(u8), childIdx_1, ..., childIdx_depth]`. From a hash partition ID alone, you can reconstruct every ancestor's identity and compute its DO name and `DurableObjectId` — no I/O required.

Range partition IDs encode only the current boundaries: `[schema, flags, hkLen, startLen, hashKey, startBoundary, endBoundary]`. There is no split history. To traverse from root to a deep leaf, you must forward level-by-level — each range partition routes to its children, but a caller with no cached tree state must start at the root every time.

### Goal

Encode hierarchy information in the range partition ID so that a caller receiving a response from a deep range partition can learn about its ancestors and skip levels on future requests. This enables:

1. **Leapfrog routing:** Jump directly to a deep ancestor instead of traversing from root.
2. **Incremental cache building:** Each response populates the caller's routing table with ancestor entries. Hot paths get cached fast; cold paths degrade gracefully to root-first traversal.
3. **Ancestor DO resolution without I/O:** Compute any encoded ancestor's DO name and `DurableObjectId` from the partition ID alone (same property hash partitions already have).

### Constraints

- **DO name stability:** The DO name format `tableName.r.hashKey.start.end` must be derivable from the partition ID. The hierarchy metadata must not affect DO name computation — it is supplementary data alongside the identity-bearing boundaries.
- **Correctness under partial information:** The caller's routing cache may be incomplete or stale. Routing must remain correct: any partition whose boundaries contain the sort key is a valid routing target (it either serves as a leaf or forwards as a router).
- **Split-only invariant:** Range partitions only split (never merge). A split parent becomes a permanent router with immutable boundaries. This guarantees that cached entries never become invalid — only stale (requiring extra forwarding hops).

### Key difference from hash partitions

For hash partitions, the path IS the identity — `[root=5, children=[1,3]]` deterministically specifies the hash space owned. For range partitions, the boundaries ARE the identity — split points are data-dependent (computed from the actual key distribution via `shortestSeparator`). The hierarchy is supplementary metadata ("how we got here"), while the boundaries are the identity ("what we own").

This means:
- Hash: path alone → can compute boundaries (deterministic hash space).
- Range: path alone → cannot compute boundaries (data-dependent). Must store actual boundary bytes.

---

## Assumed key sizes for estimates

All size estimates use these representative values for single-table design patterns:

| Component | Example | Size |
|-----------|---------|------|
| Hash key | `org#a]7b2c9d-e4f5-6789-abcd-ef0123456789` | ~50 bytes |
| Sort key / boundary | `user#invoice#2025-06-15T10:30:00Z#item#abc123` | ~100 bytes |
| Hash key (encoded for DO name) | percent-encoded, mostly passthrough for ASCII | ~50 bytes |

---

## Option 1: Full ancestor boundary stack

Store every ancestor's boundaries in the partition ID, from the shallowest (depth 1) to the partition's own boundaries (depth D). The root at depth 0 is always `[null, null)` and is implicit.

### Wire format

```
byte[0]       = 0x01 (schema)
byte[1]       = own_depth (u8)
byte[2..5]    = hkLen (u32 LE)
byte[6..+hkLen] = hashKey

// D entries (one per level, depth 1 through D), each:
//   1 byte flags:
//     bit0 = start_null
//     bit1 = end_null
//   If start not null: u32 LE startLen + start bytes
//   If end not null:   u32 LE endLen + end bytes
```

### Size estimate

Header: 6 + 50 (hashKey) = **56 bytes**

Per level, worst case (middle child, both boundaries new, ~100 bytes each):
`1 flag + 4 len + 100 start + 4 len + 100 end = 209 bytes`

Per level, best case (edge child, one boundary inherited as null):
`1 flag + 4 len + 100 = 105 bytes`

Average per level (mix of edge and middle children): **~160 bytes**

| Depth | Total size |
|-------|-----------|
| 3     | 56 + 3 × 160 = **~536 bytes** |
| 5     | 56 + 5 × 160 = **~856 bytes** |
| 10    | 56 + 10 × 160 = **~1,656 bytes** |
| 20    | 56 + 20 × 160 = **~3,256 bytes** |

### Pros

- Complete ancestry — every ancestor's DO name and ID is computable.
- Simple encoder/decoder — no delta references, each level is self-contained.
- Single response gives the caller the full routing tree for this branch.

### Cons

- **Size grows linearly with depth.** At depth 20, the partition ID exceeds 3 KB. This is carried in every RPC response and stored in every partition context.
- **Massive redundancy.** Adjacent levels often share 90%+ of their boundary bytes (e.g., `user#invoice#2025-06-15T10:30:00Z#item#abc` vs `user#invoice#2025-06-15T10:30:00Z#item#def`). Storing 100 bytes when only 3 differ is wasteful.
- **Unbounded growth.** No cap on how large the partition ID can get — it's proportional to depth × key length.

---

## Option 2: Ancestor stack with prefix sharing (delta encoding)

Same full ancestor stack as Option 1, but each entry's boundaries are delta-encoded against the previous entry's boundaries. Since child boundaries are refinements within the parent's range, adjacent levels in the stack tend to share long byte prefixes.

### Encoding scheme

Each non-inherited, non-null boundary is stored as:

```
u16 LE shared_prefix_len   (bytes shared with the same boundary of the previous entry)
u16 LE suffix_len           (new bytes after the shared prefix)
suffix bytes
```

The first entry (shallowest ancestor) has no previous entry to delta against, so `shared_prefix_len = 0` and the full boundary is stored. Subsequent entries only store the diverging suffix.

Each entry also has flag bits for `start_inherited` and `end_inherited` — when a boundary is identical to the previous level's (e.g., leftmost child inherits parent's start), it costs zero bytes beyond the flag bit.

### Wire format

```
byte[0]       = 0x01 (schema)
byte[1]       = own_depth (u8)
byte[2..5]    = hkLen (u32 LE)
byte[6..+hkLen] = hashKey

// D entries (depth 1 through D, shallowest first), each:
//   1 byte flags:
//     bit0 = start_null
//     bit1 = end_null
//     bit2 = start_inherited (identical to previous entry's start)
//     bit3 = end_inherited (identical to previous entry's end)
//   If start is new and not null:
//     u16 LE shared_prefix_len
//     u16 LE suffix_len
//     suffix bytes
//   If end is new and not null:
//     u16 LE shared_prefix_len
//     u16 LE suffix_len
//     suffix bytes
```

### Size estimate

Header: **56 bytes** (same as Option 1)

First entry (no delta reference, full boundaries): **~209 bytes**

Subsequent entries, assuming ~90% prefix sharing on 100-byte boundaries (10-byte suffix):

- New boundary (delta-encoded): `4 header + 10 suffix = 14 bytes`
- Inherited boundary: `0 bytes`
- Average per entry (mix of inherited and delta): **~20 bytes** (1 flag + ~1 inherited + ~1 delta at 14 bytes)

| Depth | Total size |
|-------|-----------|
| 3     | 56 + 209 + 2 × 20 = **~305 bytes** |
| 5     | 56 + 209 + 4 × 20 = **~345 bytes** |
| 10    | 56 + 209 + 9 × 20 = **~445 bytes** |
| 20    | 56 + 209 + 19 × 20 = **~645 bytes** |

### Pros

- **Dramatic size reduction for composite keys.** At depth 10, ~445 bytes vs ~1,656 bytes (Option 1) — roughly 3.7x smaller.
- **Never worse than Option 1.** When prefix_len = 0 (no sharing), the overhead is 4 bytes per boundary (u16+u16 headers vs u32 length) — same ballpark.
- **Complete ancestry** — same coverage as Option 1.
- Inherited boundaries are free (flag bit only).

### Cons

- **Still grows linearly with depth,** just with a smaller constant. At depth 50 (unlikely but possible), ~1,200 bytes.
- **Sequential decode required.** Each entry depends on the previous entry's decoded boundaries. Cannot jump to a specific level without decoding all preceding entries. This is a forward-only pass over at most D entries — negligible cost in practice, but worth noting.
- **More complex encoder/decoder.** Must track running boundary state and compute shared prefix lengths. Straightforward to implement but more code than Option 1.

---

## Option 3: Capped N ancestors with exponential spacing + prefix sharing (recommended)

Instead of storing every ancestor, store only up to N ancestors (e.g., N=5). The ancestors do not need to be consecutive levels — they can have gaps. Use exponential spacing to maximize skip potential, and apply the prefix-sharing delta encoding from Option 2 to keep the size small.

### Ancestor selection strategy

At depth D, store ancestors at approximately exponentially-spaced levels. When a partition at depth D splits and creates a child at depth D+1:

1. The parent's partition ID already contains N ancestor entries.
2. Add the parent itself to the list (N+1 entries total).
3. Re-select N entries from the full set using exponential spacing: prefer levels at `D, D-1, D-2, D-4, D-8, D-16, ...` (most recent + exponential backoff).
4. Encode the selected N entries (shallowest first) in the child's partition ID.

Example at depth 20, N=5: store ancestors at levels **[4, 8, 16, 19, 20]** — one hop from root reaches depth 4, then 8, then 16, then 19, then 20. Worst case from a cold cache: 6 hops (root → 4 → 8 → 16 → 19 → 20) instead of 20.

### Routing correctness with partial information

The routing rule: **find the deepest cached partition whose `[start, end)` contains the sort key; route there. If none matches, route to root.**

This is correct because:

1. **Any ancestor that covers the sort key is a valid target.** It either serves (leaf) or forwards (router). No request is ever lost or misrouted.
2. **Stale entries are safe.** A cached partition may have split since the ID was created. It is now a router and forwards correctly. Extra hops, but correct.
3. **Gaps between cached levels are safe.** If we have depths [4, 8] cached and the sort key maps to depth 4, the depth-4 partition routes through 5 → 6 → 7 → 8 as needed. A few extra hops, but correct.
4. **The cache fills over time.** Each response from a range partition contributes ~5 ancestor entries to the caller's routing table. Hot paths converge to near-direct routing quickly.

The only failure mode is **extra hops**, never **wrong routing**. This holds as long as range partitions are split-only (never merge or change boundaries).

### Wire format

```
byte[0]       = 0x01 (schema)
byte[1]       = own_depth (u8)
byte[2]       = ancestor_count (u8), 0..N
byte[3..6]    = hkLen (u32 LE)
byte[7..+hkLen] = hashKey

// (ancestor_count + 1) entries, shallowest ancestor first, own boundaries last.
// Each entry:
//   1 byte: entry_depth (u8)     [the tree depth this entry represents]
//   1 byte flags:
//     bit0 = start_null
//     bit1 = end_null
//     bit2 = start_inherited (same bytes as previous entry's decoded start)
//     bit3 = end_inherited (same bytes as previous entry's decoded end)
//   If start is new and not null:
//     u16 LE shared_prefix_len   (with previous entry's decoded start; 0 for first entry)
//     u16 LE suffix_len
//     suffix bytes
//   If end is new and not null:
//     u16 LE shared_prefix_len   (with previous entry's decoded end; 0 for first entry)
//     u16 LE suffix_len
//     suffix bytes
```

The final entry (own boundaries) always has `entry_depth = own_depth`. Its decoded start/end are the partition's actual boundaries, used for DO name computation via `rangePartitionDoName(tableName, hashKey, start, end)`.

### Size estimate

Header: 7 + 50 (hashKey) = **57 bytes**

Always 6 entries (5 ancestors + own), regardless of depth.

First entry (shallowest, full boundaries): `1 depth + 1 flags + 4+100 start + 4+100 end = 210 bytes`

Entries 2-6 (delta-encoded, ~90% prefix sharing): `1 depth + 1 flags + 4+10 + 4+10 = 30 bytes` (worst case, both new). With one inherited: `1 + 1 + 14 = 16 bytes`.

Average per delta entry: **~23 bytes**

| Depth | Entries | Total size |
|-------|---------|-----------|
| 1     | 1 (own only) | 57 + 210 = **~267 bytes** |
| 3     | 4 (3 ancestors + own) | 57 + 210 + 3 × 23 = **~336 bytes** |
| 5     | 6 (5 ancestors + own) | 57 + 210 + 5 × 23 = **~382 bytes** |
| 10    | 6 (5 ancestors + own) | 57 + 210 + 5 × 23 = **~382 bytes** |
| 20    | 6 (5 ancestors + own) | 57 + 210 + 5 × 23 = **~382 bytes** |
| 50    | 6 (5 ancestors + own) | 57 + 210 + 5 × 23 = **~382 bytes** |

### Pros

- **Bounded size.** Partition ID is ~380 bytes regardless of depth. No unbounded growth.
- **Excellent skip potential.** Exponential spacing gives O(log D) worst-case hops from root. At depth 20 with N=5, worst case is 6 hops vs 20.
- **Incremental cache building.** Each response contributes 5 ancestor data points to the caller's routing table. Hot paths converge quickly.
- **Prefix sharing keeps it compact.** Delta encoding avoids redundant storage of long composite key prefixes.
- **Graceful degradation.** Shallow trees (depth ≤ 5) store all ancestors — no loss. Deep trees (depth > 5) store a useful subset. Missing levels cause at most a few extra forwarding hops.
- **Simple correctness argument.** Route to deepest matching cached partition, fall back to root. Always correct, never misroutes.

### Cons

- **Incomplete ancestry.** At depth > N, some ancestors are missing. The caller cannot resolve every level's DO — only the N cached ones + root.
- **Sequential decode.** Prefix sharing requires decoding entries in order (each depends on the previous). Bounded at 6 entries — negligible.
- **Most complex encoder/decoder** of the three options. Must handle ancestor selection (exponential spacing), delta encoding, and the flags protocol. More code than Options 1 or 2, but still straightforward.
- **Ancestor selection at split time.** When a partition splits, the parent must decide which ancestors to propagate to the child. This is a simple computation (re-select N from the parent's list + parent itself) but adds logic to the split path.

---

## Summary comparison

| | Option 1: Full stack | Option 2: Full + prefix sharing | Option 3: Capped N + prefix sharing |
|---|---|---|---|
| **Size at depth 5** | ~856 B | ~345 B | ~382 B |
| **Size at depth 10** | ~1,656 B | ~445 B | ~382 B |
| **Size at depth 20** | ~3,256 B | ~645 B | ~382 B |
| **Size at depth 50** | ~8,056 B | ~1,245 B | ~382 B |
| **Ancestry coverage** | Complete | Complete | Last N (5) |
| **Skip potential** | O(1) to any level | O(1) to any level | O(log D) worst case |
| **Decoder complexity** | Simple | Medium | Medium |
| **Bounded size** | No | No | Yes (~382 B) |
| **Cache convergence** | Instant (one response) | Instant (one response) | Gradual (multiple responses) |

---

## Option 4: Response-level ancestor propagation (recommended)

Leave the partition ID unchanged (own boundaries only — no hierarchy encoding). Instead, propagate ancestor boundaries through two mechanisms:

1. **Split initialization:** When a parent splits, it passes ancestor context to children during `initFromSplit`. Each child stores a bounded set of ancestor boundaries in its local DO state.
2. **Response enrichment:** Each response includes ancestor boundaries in a `rangeAncestors` field on `PartitionInfo`. The caller uses these to populate its routing cache.

The key insight: the partition ID is identity (used for DO name resolution). The routing cache is operational metadata (used for skipping levels). These are separate concerns and belong in separate data paths.

### Which ancestors to include

The ancestor set is configurable with two parameters:

- **`fromRoot` (default: 2):** Number of shallowest ancestor depths to include, counting from depth 1. These provide broad coverage for cold-start and far-miss scenarios.
- **`fromLeaf` (default: 2):** Number of leaf-adjacent ancestor depths to include, counting upward from depth Leaf-1. These provide narrow coverage for the common near-miss pattern (sibling/cousin ranges).

Total ancestors per response: `min(fromRoot + fromLeaf, own_depth)`. For shallow trees where the sets overlap, they collapse naturally (no duplicates).

Examples with `fromRoot=2, fromLeaf=2`:

| Leaf depth | Ancestors included | Notes |
|------------|-------------------|-------|
| 1 | (none) | Leaf IS root's child, no ancestors needed |
| 2 | [depth 1] | fromRoot and fromLeaf overlap completely |
| 3 | [depth 1, depth 2] | Still fully overlapping |
| 4 | [depth 1, depth 2, depth 3] | fromRoot=[1,2], fromLeaf=[3], one overlap at depth 2 |
| 5 | [depth 1, depth 2, depth 3, depth 4] | fromRoot=[1,2], fromLeaf=[3,4], no overlap |
| 10 | [depth 1, depth 2, depth 8, depth 9] | fromRoot=[1,2], fromLeaf=[8,9], clear separation |
| 20 | [depth 1, depth 2, depth 18, depth 19] | fromRoot=[1,2], fromLeaf=[18,19] |

### Why this selection works

**Shallow ancestors (fromRoot)** cover broad ranges. They're learned quickly (after `splitN^fromRoot` responses, all are cached — e.g., 16 responses for 4-way splits with fromRoot=2). After warmup, they're redundant in responses but cost little space and provide a safety net for:
- Cold-start routing (first requests after restart)
- Far-miss routing (sort key in a completely different sub-tree from any cached leaf)

**Leaf-adjacent ancestors (fromLeaf)** cover the narrow ranges close to where requests actually land. These are the high-value entries in steady state because:
- When a cached leaf splits, its siblings fall in Leaf-1's range — one hop instead of traversing from depth 1.
- When a request hits a cousin (nearby but different leaf), Leaf-2 catches it — two hops instead of traversing from depth 1.
- Deeper trees have more partitions at each level, so leaf-adjacent entries are less likely to already be cached.

**The gap in the middle (depths 3 through Leaf-3)** is filled naturally by traversal. When a cache miss falls to a depth-1 or depth-2 partition, the traversal from there to the leaf passes through intermediate levels. Each intermediate can add itself to the response on the way back (the forwarding chain sees the response). Alternatively, the miss traversal itself teaches the caller about the intermediates it passes through.

### Data flow

**At split time (parent → child initialization):**

```
parent splits into N children:
  for each child:
    child.initFromSplit({
      ...existingInitPayload,
      rangeAncestors: selectAncestors(parent.rangeAncestors, parent.boundaries, parent.depth)
    })
```

The `selectAncestors` function:
1. Takes the parent's stored ancestors (which the parent received at its own init).
2. Adds the parent itself to the list.
3. Selects the final set: shallowest `fromRoot` entries + deepest `fromLeaf` entries (relative to the child's depth = parent.depth + 1).
4. If total exceeds `fromRoot + fromLeaf`, deduplicate overlapping entries.

**At response time (leaf → caller via forwarding chain):**

```typescript
// In PartitionInfo response type:
interface PartitionInfo {
  // ... existing fields ...
  rangeAncestors?: Array<{
    depth: number;
    startBoundary: KeyBytes | null;
    endBoundary: KeyBytes | null;
  }>;
}
```

The leaf populates `rangeAncestors` from its local DO state. No intermediate router needs to modify the response — the ancestors are pre-selected at init time and carried by the leaf.

Optionally, intermediate routers CAN append themselves to `rangeAncestors` as the response flows back (enriching the set beyond the pre-selected entries). This is additive — more entries means faster cache convergence — but not required for correctness.

### Routing cache behavior

The caller maintains a local routing table for each hashKey (or range structure):

```
routingCache[hashKey] = sorted list of { depth, start, end, doName, doId }
```

**On response:** extract `rangeAncestors` + leaf boundaries. Insert/update entries in the routing cache.

**On new request for sort key `sk`:**
1. Find the deepest entry in `routingCache[hashKey]` where `start <= sk < end`.
2. If found: route directly to that partition's DO (skip everything above it).
3. If not found: route to range root (implicit [null, null) at depth 0).

**Correctness guarantee:** Any partition whose boundaries contain the sort key is a valid routing target. It either serves (leaf) or forwards (router). The worst case is extra forwarding hops, never misrouting. This holds because:
- Range partitions only split (never merge). Split parents become permanent routers.
- Boundaries are immutable once assigned.
- A stale entry (partition has split since cached) simply forwards — correct, just slower.

### Size estimate

**Partition ID: unchanged.** Same as current SCHEMA_RANGE_V1:

`1 schema + 1 flags + 4 hkLen + 4 startLen + hashKey + start + end`

| Depth | Partition ID size |
|-------|------------------|
| Any | 10 + 50 + 100 + 100 = **~260 bytes** (max, both boundaries set) |
| Root | 10 + 50 = **~60 bytes** (both boundaries null) |

**Response `rangeAncestors` field** (with fromRoot=2, fromLeaf=2):

Each ancestor entry: `1 depth(u8) + 1 flags + [4 + boundary_bytes] × (0, 1, or 2)`

Per entry with both boundaries set (~100 bytes each): `1 + 1 + 4 + 100 + 4 + 100 = 210 bytes`
Per entry with one null boundary: `1 + 1 + 4 + 100 = 106 bytes`
Average (mix of edge/middle children): **~160 bytes**

| Leaf depth | Entries | Response overhead |
|------------|---------|------------------|
| 1 | 0 | **0 bytes** |
| 2 | 1 | **~160 bytes** |
| 3 | 2 | **~320 bytes** |
| 5 | 4 | **~640 bytes** |
| 10 | 4 | **~640 bytes** |
| 20 | 4 | **~640 bytes** |
| 50 | 4 | **~640 bytes** |

Note: response overhead is bounded at `(fromRoot + fromLeaf) × ~160 = 640 bytes` regardless of depth. This is per-response wire cost, not stored in the partition ID.

**Local DO state for ancestors:**

Same `~640 bytes` stored once per range partition DO. Negligible relative to the data the partition holds.

**With optional prefix-sharing in the response encoding** (delta-encode ancestor boundaries against each other, shallowest first): the shallowest entry stores full boundaries (~210 bytes), subsequent entries share prefixes with their predecessor. At ~90% prefix sharing:

| Leaf depth | Entries | Response overhead (with prefix sharing) |
|------------|---------|----------------------------------------|
| 5 | 4 | 210 + 3 × 30 = **~300 bytes** |
| 10 | 4 | 210 + 3 × 30 = **~300 bytes** |
| 20 | 4 | 210 + 3 × 30 = **~300 bytes** |

Prefix sharing is optional — it adds decoder complexity but roughly halves the wire cost.

### Pros

- **Partition ID stays small and simple.** No wire format changes. Identity and routing metadata are cleanly separated.
- **Bounded response overhead.** At most `(fromRoot + fromLeaf) × ~160` bytes regardless of tree depth. With defaults (2+2), ~640 bytes uncompressed, ~300 bytes with prefix sharing.
- **Configurable trade-off.** Increase `fromRoot` for better cold-start coverage. Increase `fromLeaf` for better steady-state near-miss routing. Tune per workload.
- **Optimal for warm caches.** Leaf-adjacent ancestors address the actual miss pattern: siblings and cousins of cached leaves. These are the entries the caller is least likely to already have.
- **Natural cache lifecycle.** Shallow entries are learned fast (few partitions at top levels). Deep entries grow organically from responses. The gap fills through traversal when misses occur.
- **No partition ID migration.** Existing range partition IDs continue to work as-is. The `rangeAncestors` field is additive.
- **Intermediate enrichment is additive.** Routers in the forwarding chain CAN append themselves to the response for bonus cache entries — strictly optional, doesn't affect correctness.

### Cons

- **Cannot resolve ancestors from partition ID alone.** Need a response (I/O) to learn ancestors. Tooling/debugging that wants to inspect the tree from an ID alone must fall back to tree traversal.
- **Init payload grows.** `initFromSplit` must pass the ancestor set to children. With 4 entries at ~160 bytes each, this adds ~640 bytes to the init RPC. Negligible in practice.
- **Local DO storage.** Each range partition stores ~640 bytes of ancestor data. Trivial relative to data stored.
- **Cache convergence is gradual.** One response gives ~4 entries. Full tree knowledge requires many responses (or cache misses that traverse and learn). For hot paths this is fast; for cold/rare paths it takes longer.
- **Shallow ancestors become redundant.** After `splitN^fromRoot` responses (~16 for defaults), the fromRoot entries repeat known information. They cost 2 entries per response but provide zero new information. This is the price of cold-start safety.

### Configuration guidance

| Workload | fromRoot | fromLeaf | Rationale |
|----------|----------|----------|-----------|
| Default | 2 | 2 | Balanced — good cold start, good steady state |
| Hot-key heavy (few deep paths) | 1 | 3 | Shallow levels learned instantly; invest in deep coverage |
| Uniform distribution (many shallow paths) | 2 | 1 | Many top-level branches; shallow entries high value |
| Very deep trees (depth 15+) | 2 | 3 | Extra leaf-adjacent entry bridges the wider gap |

---

## Summary comparison

| | Option 1: Full stack | Option 2: Full + prefix | Option 3: Capped N in ID | Option 4: Response propagation |
|---|---|---|---|---|
| **Partition ID size** | ~1,656 B (depth 10) | ~445 B (depth 10) | ~382 B (any depth) | **~260 B (unchanged)** |
| **Response overhead** | 0 (in ID) | 0 (in ID) | 0 (in ID) | ~640 B (or ~300 w/ prefix sharing) |
| **Ancestry coverage** | Complete | Complete | N=5 (exponential) | fromRoot + fromLeaf (configurable) |
| **Skip potential** | O(1) any level | O(1) any level | O(log D) worst | O(1) cached, O(D) cold gap |
| **Decoder complexity** | Simple | Medium | Medium | Simple (just boundary list) |
| **Partition ID changes** | Yes (new schema) | Yes (new schema) | Yes (new schema) | **None** |
| **Bounded size** | No | No | Yes | **Yes (both ID and response)** |
| **Cache convergence** | Instant | Instant | Gradual | Gradual |
| **Steady-state value** | Redundant | Redundant | Partially redundant | **Optimized (leaf-adjacent)** |
| **Configurable** | No | No | N only | **Yes (fromRoot, fromLeaf)** |

Option 4 is recommended. It keeps the partition ID simple and unchanged, puts routing metadata in the response where it belongs, and optimizes for the actual steady-state miss pattern (leaf-adjacent entries). The configurable `fromRoot`/`fromLeaf` parameters let the strategy adapt to different workload shapes without wire format changes.
