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

Option 3 is recommended. The bounded size (~382 bytes) makes it safe for any tree depth, the exponential spacing gives strong skip potential, and the incremental cache-building model aligns with real access patterns: hot paths get learned fast, cold paths degrade gracefully to root-first routing with a few extra hops.
