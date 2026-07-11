# Range split boundary computation: byte-seek optimization

## Problem

`PartitionStore.computeRangeSplitBoundaries` (`src/lib/partition/partition-store.ts:416`) computes
the `N-1` split boundaries a range partition uses to divide its owned `[start, end)` slice into `N`
children. It runs synchronously inside a single `transactionSync` on the splitting parent, **before
migration begins**, so its cost is on the critical path of every range split (and every hash-key
promotion's range structure, once it starts splitting).

The current implementation does:

1. One `COUNT(*)` over the `hk` slice → **1 full pass** (`cnt` rows walked).
2. `N-1` separate `SELECT sk … ORDER BY sk LIMIT 2 OFFSET (offset_i - 1)` queries. SQLite does not
   seek on `OFFSET` — it walks and discards `offset_i ≈ cnt·i/N` rows each time. Summed:
   `Σ cnt·i/N = cnt·(N-1)/2`.

Total row-walks ≈ `cnt + cnt·(N-1)/2 = cnt·(N+1)/2` — **~2.5·cnt at the default `rangeSplitN = 4`**,
all blocking, before a single item is migrated. By contrast, hash-split boundary "computation" is
pure partition-id arithmetic with zero data queries, which is a large part of why hash splits are
observably faster than range splits.

The `items` table is `WITHOUT ROWID, STRICT` with `PRIMARY KEY (hk, sk)`
(`src/lib/partition/partition-store.ts:146-155`), so each scan is an optimal clustered range scan —
but 2.5 of them is still 2.5 of them.

## Goal

Reduce the boundary computation to a **single early-stopping scan (~0.75·cnt)** with:

- **No new maintained state** — reuse the size estimates already kept current on every write.
- **No whole-partition materialization** — must stay a streaming cursor (Durable Object SQLite hits
  memory errors with CTE / window-function materialization such as `COUNT(*) OVER ()`).
- **Behavior**: equal-**byte** children instead of equal-**count** children. Because the split is
  *triggered by size*, byte-balance is the metric we actually care about, and it naturally isolates
  a heavy row into its own child instead of burying it in a count-balanced bucket.

## Reusable state we already maintain

- `key_size_estimates.est_bytes` — per-`hk` total byte estimate, maintained O(1) on every
  upsert/delete (`src/lib/partition/partition-store.ts:343-346, 377`). Gives the key's total bytes
  without a scan.
- `items.est_row_bytes` — per-row byte estimate, already stored on every row
  (`src/lib/partition/partition-store.ts:153`) and summed into `est_bytes`. Consistent with the
  total by construction (`rebuildKeySizeEstimates`, `:739-743`).

These are **estimates**, which is fine: split boundaries only need to be approximately balanced, and
the existing validation (below) guarantees correctness regardless of estimate accuracy.

## Algorithm

Replace the `COUNT(*)` + `N-1 × OFFSET` scans with:

1. **Total bytes in O(1)**: read `B = est_bytes[hk]`. If `B <= 0`, there is nothing to split →
   return `null` (mirrors today's "not enough items" contract).

2. **Cheap "≥ N items" guard, O(N) not O(cnt)**: the children must each get ≥ 1 item, so there must
   be at least `N` items in the slice. Probe with a bounded query rather than a full count:

   ```sql
   SELECT COUNT(*) FROM (
     SELECT 1 FROM items WHERE hk = ? AND sk >= ? [AND sk < ?] LIMIT ?N
   )
   ```

   If the result is `< N`, return `null`.

3. **Single streaming scan with JS byte accumulation**: open one ordered cursor over the slice
   (no window function), accumulate `est_row_bytes`, and emit a boundary each time the running total
   crosses the next byte threshold. Break as soon as the `(N-1)`th boundary is found — which happens
   at roughly `(N-1)/N` of the way through the data (~0.75·cnt at `N = 4`).

   ```
   const B = est_bytes[hk]
   if (B <= 0) return null
   if (itemCountAtLeast(hk, start, end, N) < N) return null

   const step = B / N
   const lower = start ?? KeyCodec.encodeOptional(undefined)
   const cursor = sql.exec(
     `SELECT sk, est_row_bytes FROM items
      WHERE hk = ? AND sk >= ? [AND sk < ?]
      ORDER BY sk`, hk, lower, [end])

   let acc = 0
   let threshold = step
   let prev: KeyBytes | null = null
   const boundaries: KeyBytes[] = []

   for (const row of cursor) {
     const sk = fromSqlKey(row.sk)
     acc += row.est_row_bytes
     if (prev !== null && acc >= threshold && boundaries.length < N - 1) {
       boundaries.push(KeyCodec.shortestSeparator(prev, sk))
       // Relative bump: guarantees ≥1 row per child and stops a single huge row from
       // emitting duplicate boundaries when it crosses several thresholds at once.
       threshold = acc + step
       if (boundaries.length === N - 1) break
     }
     prev = sk
   }
   ```

   Notes on the loop:
   - `prev !== null` guard: the first row can never be a boundary, so child 0 always owns ≥ 1 row.
   - `KeyCodec.shortestSeparator(prev, sk)` reproduces today's semantics exactly: today's `LIMIT 2`
     fetches the predecessor (`offset-1`) and boundary (`offset`) rows and separates them; here
     `prev` is the predecessor and `sk` is the crossing row. Child `i` owns `[b_{i-1}, b_i)`; the
     crossing row falls into the upper child.
   - `threshold = acc + step` (relative, not `threshold += step`): if one oversized row pushes `acc`
     past several thresholds at once, we still emit only one boundary and re-anchor from the current
     position, so no two boundaries can land on the same adjacent-key pair.

4. **Keep the existing validation unchanged** (`src/lib/partition/partition-store.ts:454-457`):
   every boundary must be strictly above `lower` and strictly increasing. On skewed data the scan
   may produce fewer than `N-1` boundaries; treat that the same as today's failure and return `null`
   (the split retries on a later cycle). This is the safety net that makes estimate inaccuracy
   harmless — a bad estimate can only ever yield a lopsided-but-valid split or a `null` retry, never
   an incorrect one.

## Cost

| | Row-walks (default `N=4`) |
|---|---|
| Current (`COUNT` + `N-1` OFFSET) | `cnt·(N+1)/2` ≈ **2.5·cnt** |
| Byte-seek (this plan) | O(N) guard + single scan to last boundary ≈ **0.75·cnt** |

~70% fewer row touches, no `COUNT(*)` pass, no OFFSET re-walks, single streaming cursor.

## Correctness / invariants preserved

- **Same `null` contract**: `null` = "cannot split into `N` non-empty children yet" (either `B <= 0`
  or fewer than `N` items or the validation rejected the boundary set). Callers
  (`runSplit` at `src/lib/do-partition.ts:1062-1073`, and `prepareSplit` which asserts
  `boundaries.length === N - 1`) are unaffected.
- **Same boundary semantics**: `shortestSeparator(predecessor, boundary)` in byte space, matching the
  SQL scans that later migrate the data.
- **Runs inside `transactionSync`**: the `est_bytes` read and the scan see one consistent snapshot.
- **Estimate-tolerant**: the strictly-increasing / `> lower` validation is retained, so accuracy of
  `est_row_bytes` / `est_bytes` affects only balance quality, never correctness.

## Memory safety

The scan is a plain `sql.exec(...)` cursor iterated row-by-row in JS and broken early. It never
materializes the partition — unlike `COUNT(*) OVER ()` / `ROW_NUMBER() OVER ()` CTEs, which buffer
the whole partition to resolve the total and reliably OOM in a Durable Object on large keys. This is
the load-bearing constraint that rules the CTE approaches out (see Appendix).

## Optional follow-up: covering index (only if scan is I/O-bound)

Because `items` is `WITHOUT ROWID`, its PK-b-tree leaves hold the entire row (including the `data`
blob), so even `SELECT sk, est_row_bytes` drags full rows through the page cache. If profiling shows
the boundary scan is I/O-bound on those fat leaves, add a skinny covering index:

```sql
CREATE INDEX idx_items_split ON items (hk, sk, est_row_bytes);
```

The scan is then answered from the narrow index alone. This is a constant-factor I/O win, not an
algorithmic one, and it costs write-amplification on the hot put/delete path — so add it only if
measurements justify it. Do **not** convert the table to a rowid layout to get the same effect
(see Appendix).

## Testing

Add focused tests on a synthetic large single-key partition:

- **Uniform rows**: byte-balanced boundaries produce `N` children of roughly equal `est_bytes`; each
  child non-empty; boundaries strictly increasing.
- **One heavy row**: the heavy row lands in its own (or the correct) child; still `N-1` valid,
  strictly-increasing boundaries.
- **Too few items** (`< N`): returns `null`.
- **Skewed/duplicate keys** that can't form `N` distinct boundaries: returns `null` (retry contract).
- **Parity**: for uniform data, resulting children partition the full `[start, end)` with no gaps or
  overlaps and route the same keys the old count-based version did (approximately — exact parity is
  not expected since the metric changed from count to bytes).

---

## Appendix: Alternatives considered and rejected

### A1. Window-function / CTE single pass (`COUNT(*) OVER ()` + `ROW_NUMBER()`)

Compute rank and total in one CTE and filter boundary rows arithmetically:

```sql
WITH ranked AS (
  SELECT sk,
         ROW_NUMBER() OVER (ORDER BY sk) - 1 AS idx0,
         COUNT(*)     OVER ()                AS n
  FROM items WHERE hk = ? AND sk >= ? [AND sk < ?]
)
SELECT sk, idx0, n FROM ranked
WHERE idx0 = (n * i) / N OR idx0 = (n * i) / N - 1   -- for i in 1..N-1
```

**Rejected — memory.** `COUNT(*) OVER ()` must know the total before emitting the first row, forcing
SQLite to materialize the entire partition into a temp b-tree. In a Durable Object this OOMs on large
keys (exactly the keys that reach the 500 MB range-split threshold). This is a hard constraint, not a
micro-optimization concern. Would be a true single pass (~1·cnt) if the memory model allowed it — it
does not.

### A2. Optimization 1 — chained relative-OFFSET, count-based

Keep exact count-balance; seed each boundary query from the previous boundary key so each `OFFSET`
is relative (`~cnt/N`) instead of from the slice start:

```sql
b1 = … WHERE sk >= lower ORDER BY sk LIMIT 2 OFFSET (o1 - 1)
b2 = … WHERE sk >  b1    ORDER BY sk LIMIT 2 OFFSET (o2 - o1 - 2)
b3 = … WHERE sk >  b2    ORDER BY sk LIMIT 2 OFFSET (o3 - o2 - 2)
```

Cost: `COUNT(*)` (1·cnt) + chained OFFSETs (0.75·cnt) = **1.75·cnt**. Pure SQLite, no JS accumulation,
exact count-balance, no new state.

**Rejected — still pays the `COUNT(*)` pass** (now the dominant term) and produces count-balanced
children, which is the wrong metric for a size-triggered split. Strictly worse than the byte-seek on
both axes. Retained here only as the minimal-diff fallback if we ever need exact count-balance
without touching the write path.

### A3. Optimization 1 + a maintained per-`hk` item count

Add a per-`hk` row counter (maintained like `est_bytes`) so A2 can drop its `COUNT(*)`, reaching
**~0.75·cnt** — cost-competitive with the byte-seek.

**Rejected — same cost, worse trade.** Even at cost-parity the byte-seek wins because:

- **Balance metric**: count-balance can still produce byte-lopsided children on variable row sizes;
  byte-balance is aligned with the size trigger.
- **Asymmetric failure mode**: A2's OFFSET positions are derived from the count. A count that drifts
  *high* makes the last OFFSET overshoot the end, returning `< 2` rows and tripping the
  `rows.length === 2` invariant (`src/lib/partition/partition-store.ts:446-448`). The byte-seek has
  no such cliff — it stops at end-of-cursor.
- **Exactness burden**: a count used for OFFSET indexing must be near-exact, so every write path
  (upsert-of-existing, `INSERT OR IGNORE` on conflict during migration, TTL/GC deletes, transaction
  commit/cancel) must maintain it perfectly. The byte estimate is drift-tolerant by design; a new
  exact counter is fragile new state.

Worth adding a per-`hk` count only if it pays for itself elsewhere (e.g. a future count-based split
trigger, or O(1) item-count reads) — not to win this function.

### A4. Convert `items` to a rowid table (drop `WITHOUT ROWID`)

Give `items` an integer rowid plus a secondary index on `(hk, sk)`, hoping rowid enables faster
boundary lookup.

**Rejected — no algorithmic benefit, and it penalizes the hot path.**

- rowid order reflects *insertion* order, uncorrelated with `sk` order, so it gives nothing for
  `sk`-quantile lookup.
- SQLite b-trees store no subtree counts, so order-statistics ("find the k-th `sk`") is an O(k) walk
  on any index, rowid or not.
- The only real effect would be a constant-factor I/O win (covering the scan with a skinny index
  instead of dragging full rows) — but that is achievable with the A-plan's covering index while
  keeping `WITHOUT ROWID` (see "Optional follow-up").
- Converting to rowid makes point reads/writes by `(hk, sk)` a two-hop lookup (index → rowid → row)
  instead of one, slowing the *frequent* operation to speed up a *rare* one. Wrong trade.

### A5. Maintained order-statistics / histogram state

Maintain incremental structures per `hk` so boundary lookup becomes sub-linear (O(log n) or a bounded
scan) instead of O(cnt). The fundamental difficulty is that boundaries are **quantiles over an
arbitrary byte ordering, with deletes** — most clean quantile structures violate one of those three.
Candidates:

- **Prefix-bucket histogram (equi-width)** — a companion table `(hk, sk_prefix, cnt, bytes)`, a
  natural extension of `est_bytes`: insert/delete increments/decrements the bucket for `sk`'s leading
  bytes. Query walks buckets to locate each boundary's bucket, then scans only that bucket.
  Delete-friendly, but degenerates to O(cnt) under **prefix skew** (timestamp / tenant-prefixed keys
  all land in one bucket).
- **Fenwick / order-statistics tree over buckets** — same bucketing but with cumulative counts for
  O(log #buckets) rank lookup and update. Adds the skew problem *plus* array-in-SQLite awkwardness and
  O(log) row rewrites on every write.
- **Streaming quantile sketches (t-digest / GK / Q-digest)** — ~M summary points approximating the
  distribution. Rejected by **deletes** (append-oriented, no removal) and, for t-digest, the lack of a
  meaningful mean over byte strings.
- **Bounded reservoir sample per key** — a random sample of `sk` values, quantiles from the sample.
  Deletes make it stale/biased and force periodic rebuild scans — the cost we are trying to avoid.

**Rejected — the win is capped and the tax is on the hot path.** Even a perfect O(log n) structure
only removes the boundary step, which is already ~0.75·cnt of local, background, once-per-split work
and is dwarfed by the migration that re-scans the whole partition anyway — while the structure taxes
every put/delete on the request hot path forever; maintained statistics are worth revisiting only if
the same structure independently serves hot-sub-range-aware splitting or query cardinality estimation,
amortizing that write cost across several features.
