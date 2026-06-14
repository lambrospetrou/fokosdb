# `queryItems` Design Plan

Status: **design / not yet implemented**. This plan defines a `queryItems` operation alongside
`putItem`/`getItem`/`deleteItem`, modeled on the DynamoDB `Query` operation and adapted to fokosdb's
hash-partition + promoted-range topology.

Scope note: the `items` table stores `data` as opaque bytes and exposes only key/system attributes
(`hk`, `sk`, `ttl`, `v`). This plan does **not** introduce attribute extraction, projection, or
filter expressions over `data` — those are deferred to a later SPEC. Consequently there is no
`ProjectionExpression`, no `FilterExpression`, and no PartiQL.

## 1. DynamoDB parity matrix

| DynamoDB Query feature | fokosdb decision |
|---|---|
| Partition key = equality, exactly one | **Extended**: a request is a **list of per-key sub-queries** `{hashKey, sort?}`; a hashKey may appear more than once (dedup is the caller's responsibility) |
| Sort key ops `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `begins_with` | **Adopt all** as ergonomic sugar, **plus** a low-level `range` form with explicit per-end inclusivity. Each normalizes to one contiguous sk interval before processing |
| `ScanIndexForward` | Adopt as `scanIndexForward` (default `true`) |
| `Limit` (item count) | Adopt as `limit` |
| 1MB page cap + `LastEvaluatedKey`/`ExclusiveStartKey` | Adopt; cap is a configurable **byte budget** (`maxPageBytes`, default ~3–4MB) and the continuation is an opaque `cursor` |
| `Select` ALL/COUNT/SPECIFIC | `ALL_ATTRIBUTES`, plus optional `COUNT` (`countOnly`). No SPECIFIC (no projection) |
| `FilterExpression` / `ProjectionExpression` / PartiQL | Dropped (attributes not exposed yet) |
| `ConsistentRead` | Each `(hk, sk)` slice is owned by one DO ⇒ per-slice strong consistency. A multi-leaf/multi-key scan is **not** a global snapshot |

A query is therefore: **a list of `{hashKey, sort?}` sub-queries × {direction} × {byte/limit budget}**, paginated.

## 2. Public API (types only)

The request is a **list of per-key sub-queries**, each carrying its own optional sort condition. This
subsumes the three shapes considered (single key + condition; many keys + shared condition; per-key
conditions). The "many keys + one shared condition" convenience form is **not** a core wire field — it
lives in a **utilities/SDK package** that expands the shared condition over the list, so the core has a
single code path.

```ts
// Public, ergonomic — what most callers write:
type SortKeyCondition =
  | { op: "eq"; value: string }
  | { op: "lt" | "lte" | "gt" | "gte"; value: string }
  | { op: "between"; lower: string; upper: string }          // inclusive/inclusive (DDB semantics)
  | { op: "beginsWith"; prefix: string }
  | { op: "range";                                            // low-level escape hatch
      lower?: { value: string; inclusive: boolean };
      upper?: { value: string; inclusive: boolean } };

type QueryItemsOptions = {
  queries: Array<{ hashKey: string; sort?: SortKeyCondition }>; // 1..N; grouped in this order
  scanIndexForward?: boolean;          // default true
  limit?: number;                      // max items returned
  maxPageBytes?: number;               // default ~3-4MB, clamped to server max
  cursor?: string;                     // opaque continuation token
  countOnly?: boolean;                 // Select = COUNT (phase 4)
};

type QueryItemsResult = {
  items: Array<{ hashKey: string; sortKey?: string; data: string | Uint8Array;
                 ttlEpochUTCSeconds?: number; version: number }>;
  count: number;
  cursor?: string;                     // present iff more results remain
  meta: OperationMetrics & PartitionInfo; // rowsRead aggregated across visited DOs
};
```

Notes on the request shape:

- **Per-key conditions are architecturally free** given the grouped-by-hashKey execution (§3): each
  sub-query already routes and scans independently with its own interval.
- **Duplicate hash keys are allowed** — repeating a hashKey with different conditions yields a union of
  disjoint sk ranges (returned as consecutive groups in list order). This is the escape hatch for
  OR/multi-range; first-class `OR` is deferred. Overlapping ranges may duplicate items; dedup is the
  caller's responsibility.
- **Multiple ANDed conditions are intentionally not a thing** — the intersection of bound-predicates is
  always a single contiguous interval, which the `range` form expresses directly (including the
  half-open and mixed-inclusivity cases `between` cannot).

### Sort-key condition → interval (the only primitive the store sees)

A single pure normalization function collapses every `SortKeyCondition` into one canonical interval
**before** anything else runs, so the store/traversal never learns about operators:

```ts
type SkInterval = {
  lower?: { value: string; inclusive: boolean };
  upper?: { value: string; inclusive: boolean };
};
```

- `eq v` → `lower=v incl, upper=v incl`
- `beginsWith p` → `lower=p incl, upper=successor(p) excl`; `beginsWith("")` → `lower="" incl, upper=none` (match-all)
- `between a b` → `lower=a incl, upper=b incl`; if `a > b` → empty result
- `gt`/`gte` → lower bound only (excl/incl)
- `lt`/`lte` → upper bound only (excl/incl)
- `range {lower?, upper?}` → passed through; inverted/empty interval → empty result
- (no `sort`) → whole hashKey (`lower="" incl, upper=none`)

`successor(p)` (the prefix upper bound) is the one place we **synthesize** a key, and is therefore the
one place exposed to the key-encoding/ordering subtlety covered in **Appendix A**. The other operators
take user-provided literals and are unaffected.

**Decision for this plan:** `successor(p)` is computed in **code-point space** (`codePointAt`, max
`U+10FFFF`, carry) — *not* UTF-16 code units. This yields a valid string that matches SQLite's UTF-8
`TEXT` ordering for all well-formed strings, keeps `beginsWith` as an index-friendly
`sk >= :p AND sk < :succ` range scan, and avoids `LIKE`/`GLOB`. Ill-formed input (lone surrogates) is
rejected/normalized at the boundary. The system-wide byte/BLOB migration in Appendix A is **out of
scope** for this plan; the code-point `successor` is the chosen approach here. See Appendix A, option 3.

## 3. Execution model — four cases

Single-hashKey routing matches `getItem` up to the scan, because hash partitioning is purely by
hashKey: **all sort keys of one non-promoted hashKey live on exactly one leaf hash partition**.

- **Case A — single, non-promoted hashKey.** Enter at the root hash DO → split-forward to the one
  leaf owning the hashKey → scan its `items` slice over `[skLower, skUpper)` in the requested
  direction (`queryRangeItemsPage` + `collectBatch`). No fan-out; pagination stays in that leaf.
- **Case B — single, promoted hashKey.** The hash DO's promotion check (authoritative
  `PromotionManager.statusFor`, then speculative bloom — mirroring `withSplitForwarding`) forwards to
  the **range root**. The range tree (split by sort-key boundaries) is traversed in sk order (§4).
- **Case C — multiple sub-queries.** Fan out to N independent single-key queries (each A or B), one per
  `queries[]` entry. Results are **grouped per sub-query in list order**; sk order applies *within* each
  group. The cursor records which `queryIdx` we're on plus that entry's inner position. (Decision:
  grouped-by-hashKey.) A repeated hashKey is just two entries → two consecutive groups.
- **Case D — reverse (`scanIndexForward=false`).** Within a leaf: `ORDER BY sk DESC` + cursor
  `sk < ?`. Across a range tree: visit leaves in **descending** boundary order. Direction is threaded
  into the store and the traversal.

## 4. Range-tree traversal for a promoted key — **bounded recursive walk** (decided)

A split range root is a router holding `split_status.childPartitionContexts`, each owning `[start, end)`;
children may be split further. The chosen strategy:

- New `PartitionDO.queryItems` RPC. A **leaf** range DO scans its slice ∩ requested interval and
  returns a page plus a remaining-budget signal. A **router** range DO iterates its children in
  boundary order (reverse order if `scanIndexForward=false`), calling each child's `queryItems` with
  the *remaining* byte/limit budget, short-circuiting once the budget is spent. It returns the combined
  page and a cursor = last `(hashKey, sk)` emitted.
- This fills a page across multiple leaf DOs in a single client round-trip, and keeps the
  stub-ownership boundary intact (only DO classes / FokosDB acquire stubs).
- Resumption uses the **logical** `(hashKey, sk)` token (§5), not physical DO identity, so a split or
  promotion landing between pages just changes which DO answers the same logical boundary.

## 5. Pagination / cursor design

Opaque, forward-only, base64 token storing **logical position, not physical DO identity**:

```
cursor = {
  queryIdx: number,          // index into the `queries` sub-query list
  lastHashKey: string,
  lastSortKey: string,       // resume strictly after (before, if reverse) this sk
  direction: "fwd" | "rev",  // pinned at first page; mismatch on resume is rejected
}
```

Resumption re-routes `lastHashKey`/`lastSortKey` through normal routing, so the logical boundary is
stable across splits/promotions. `collectBatch`'s contract is reused verbatim: cursor advances past
every scanned row; the first matched row is always included even if oversized; `nextCursor` is
non-null **only** when the byte budget stopped the scan. We extend its cursor to the `(hk, sk)` token
above. An empty page with a non-null cursor is valid (a run of skipped/filtered rows hit the budget).

## 6. Store-layer changes (`partition/partition-store.ts`)

- Generalize `queryRangeItemsPage` to take `direction: "asc" | "desc"` and inclusive/exclusive bounds,
  emitting `ORDER BY sk ASC|DESC` with the matching cursor comparator (`sk > ?` vs `sk < ?`, and
  `>=`/`<=` for inclusive endpoints needed by `eq`/`between`). Today it is ascending-only with
  `sk >= lower` / `sk > cursor` / `sk < end`; we add the mirror image.
- (Phase 4) `countItemsInRange(hk, lower, upper, dir)` for `countOnly`.
- No schema change: `PRIMARY KEY (hk, sk)` on `WITHOUT ROWID` already supports ordered range scans in
  both directions.

## 7. Orchestration placement

- **`FokosDB.queryItems`** (`db.ts`): validate, normalize each `sort` to an `SkInterval`, fan out across
  the `queries[]` sub-queries, enforce the *global* byte/limit budget, assemble the cross-query cursor.
  FokosDB may hold stubs.
- **`PartitionDO.queryItems`** RPC (`do-partition.ts`): single-hashKey + interval + direction + budget
  + cursor → page + nextCursor. Mirrors `withSplitForwarding`'s promotion + split-forward logic and
  adds the range-tree walk (§4).
- Reuse `collectBatch` (byte budget) and `estimateItemBytes` (sizing).

## 8. Correctness / concurrency

- **Splits / migration in flight.** Mirror `getItem`: read the relevant slice from the parent while a
  child is migrating, rather than rejecting, for availability.
- **Promotion in flight (`promoting`).** Forward to the range root (as point reads do); it is
  authoritative during migration.
- **Locked rows (pending transactions).** **Return the last committed value** and ignore pending
  locks (decided). Queries are reads outside transaction isolation in phase 1; document this.
- **TTL.** **Mirror `getItem` (decided):** `readItemLocally` returns the row and surfaces
  `ttlEpochUTCSeconds` without filtering expired items, so `queryItems` does the same — no read-time TTL
  filtering, `ttl` passed through. Revisit jointly with `getItem` if/when a reaper or read-time filter
  is introduced.
- **Consistency.** Per-slice strong consistency only. A multi-leaf / multi-key result is not a global
  point-in-time snapshot; items may change between leaf visits. Document explicitly.

## 9. Limits & config

- `maxPageBytes` default 3–4MB, clamped to a hard server max well under the 32MB RPC limit (e.g. ≤16MB).
- `limit` (item count) and the byte budget are independent; stop at whichever hits first.
- Each DO hop does bounded work (the range walk honors the remaining budget) — no unbounded hop.
- `meta.rowsRead` is aggregated across all visited DOs.

## 10. Phasing

1. **Phase 1** — single sub-query (A+B), forward direction, all sk operators incl. `beginsWith`,
   byte+limit pagination, opaque cursor, lock = return-committed. Delivers DynamoDB-Query parity for one
   key. `beginsWith` depends on the `successor()` decision in **Appendix A**.
2. **Phase 2** — `scanIndexForward=false` (D) end-to-end (store + reverse range walk).
3. **Phase 3** — multi-sub-query fan-out (C) + cross-query cursor (grouped-by-hashKey).
4. **Phase 4 (optional)** — `countOnly`, first-class `OR`/multi-range, lock-aware modes.

## 11. Decisions locked

- Request shape: **list of `{hashKey, sort?}` sub-queries**; shared-condition convenience form lives in
  a **utilities/SDK package**, not the core wire API.
- **Duplicate hash keys allowed**; dedup is the caller's responsibility (this is the OR/multi-range
  escape hatch). First-class `OR` deferred.
- Sort condition: ergonomic ops **and** a low-level `range` form, both **normalized to one `SkInterval`**
  before processing.
- Multi-key ordering: **grouped by hashKey** (request order; sk order within each group).
- Range traversal: **bounded recursive walk** (router fills the page across leaves per call).
- Locked rows: **return the last committed value**.
- TTL: **mirror `getItem`** — no read-time filtering.
- `beginsWith` upper bound: **code-point `successor`** + range scan (Appendix A, option 3). The
  system-wide byte/BLOB migration is **out of scope** for this plan.

## Appendix A — Key encoding & ordering: UTF-16 vs TEXT vs BLOB (why we should move to UTF-8 everywhere)

This appendix records the discussion behind how sort/hash keys are *ordered* and *compared*, why the
current scheme has a latent inconsistency, and the recommended direction. It is broader than
`queryItems` — `queryItems` only *surfaces* the issue via `beginsWith`'s synthesized `successor(p)`.

### The latent inconsistency

There are two orderings in play and they are **not** the same:

- **JS side** — `shortestSeparator`, `computeRangeSplitBoundaries`, the `sk >= start && sk < end`
  range-ownership checks, and a `beginsWith` `successor()` all operate on JS strings, i.e. in **UTF-16
  code-unit order** (`charCodeAt`, `<`).
- **SQLite side** — keys are stored as `TEXT`; the default `BINARY` collation compares the **UTF-8
  bytes** (the DO database is UTF-8). So `ORDER BY sk` / `sk < ?` run in **UTF-8 byte order**.

UTF-8 byte order equals Unicode code-point order. UTF-16 code-unit order equals code-point order **for
the entire BMP**, but **diverges for supplementary-plane (astral) characters**, because UTF-16 places
the surrogate range `0xD800–0xDFFF` *below* `0xE000–0xFFFF`:

```
A = "￿"     (U+FFFF)   UTF-8: EF BF BF
B = "\u{1F600}"  (😀)        UTF-8: F0 9F 98 80   (UTF-16: D83D DE00)

UTF-8 / code-point order:  A < B   (EF < F0)
UTF-16 code-unit order:    B < A   (0xD83D < 0xFFFF)
```

So for keys containing astral characters, JS-side routing/boundary/`successor` decisions can disagree
with what SQLite actually returns. This is a **pre-existing, system-wide** assumption (the range-split
machinery already relies on it); `queryItems` would merely add one more instance via `beginsWith`.

### How DynamoDB avoids it

DynamoDB never uses UTF-16. String keys are ordered by **UTF-8 byte comparison**, Binary keys by
**unsigned byte comparison**, and `begins_with`/`BETWEEN`/comparisons all evaluate in that same byte
space. Compare-space == prefix-space == storage-space ⇒ inherently self-consistent. (Consequence we
inherit: numbers embedded in string keys sort lexically, not numerically — callers zero-pad / use
sortable encodings, same as DynamoDB.)

### Key insight: SQLite already orders the "DynamoDB way"

`TEXT` + `BINARY` collation **already** compares in UTF-8 byte order — exactly DynamoDB's String
semantics. The stored data is already ordered correctly. **The bug is entirely on the JS side** (UTF-16
arithmetic). The fix is to drag the JS side onto the UTF-8 order SQLite already uses.

Also note: of all `queryItems` operators, only `beginsWith` *synthesizes* a key. `eq`/`lt`/`lte`/`gt`/
`gte`/`between`/`range` take user-provided literal bounds that we bind as params and SQLite compares in
UTF-8 — they have **no** JS-side ordering exposure. The whole issue concentrates in `successor(p)`.

### Options for `beginsWith` specifically

1. **UTF-16 `successor` (status quo).** Wrong for astral keys; rejected.
2. **`LIKE 'p%'` / `GLOB 'p*'`.** Pushes prefix matching into SQLite's own (consistent) space, so no JS
   synthesis. But: `LIKE` is **ASCII case-insensitive by default** (needs global `PRAGMA
   case_sensitive_like=ON`); both need **wildcard escaping** (`%`/`_` resp. `*`/`?`/`[`) of the prefix;
   and their **index optimization is fragile** with bound parameters — if it doesn't trigger, the query
   degrades to a per-row scan of the hashKey's slice, i.e. exactly the **promoted** keys promotion
   exists to protect. `GLOB` is the saner of the two (case-sensitive, `BINARY`-based).
3. **Code-point `successor` + range scan — ✅ SELECTED FOR THIS PLAN.** Compute `successor`
   in **code-point space** (`codePointAt`, max `U+10FFFF`, carry) instead of UTF-16 code units. The
   result is always a valid string (safe `TEXT` param) and matches UTF-8 order for all well-formed
   strings, and a plain `sk >= :p AND sk < :succ` scan stays index-friendly with none of `LIKE`'s
   case/wildcard/plan landmines. Loose end: ill-formed input (lone surrogates) must be
   rejected/normalized.
4. **Bytes/BLOB end-to-end (system-wide target, OUT OF SCOPE for this plan).** The uniformly-consistent
   end state, but a separate prerequisite SPEC — see below.

### System-wide target (out of scope for this plan): UTF-8 / byte keys everywhere

`queryItems` ships with the code-point `successor` (option 3). The following is the recommended **future
direction** for the system as a whole, tracked as a separate SPEC — not implemented here.

The only *uniformly* consistent fix is to make the **whole system** agree on one byte-wise order:

- Switch the key columns (`items.hk/sk`, `pending_transactions.hk/sk`, `promoted_keys.hash_key`,
  `key_size_estimates.hk`) to **`BLOB`**, encode string keys to UTF-8 at the boundary, and do **all**
  comparison + `successor` + separator arithmetic in **raw byte space**.
- `BLOB` comparison is `memcmp` (unsigned bytes) = our byte arithmetic = DynamoDB semantics. Boundaries
  need **not** be valid UTF-8 (a byte-space `successor` can produce non-UTF-8 bytes — e.g. prefix ending
  `0x7F` → `0x80`; `BLOB` accepts it, `TEXT` cannot). UTF-8 is prefix-preserving, so byte-space
  `beginsWith` is exactly character-space `beginsWith`.
- This also future-proofs **binary keys** (DynamoDB's Binary key type).

Cost: a real migration touching the schema, every JS key comparison site (`shortestSeparator`,
`computeRangeSplitBoundaries`, range-ownership checks, partial-range-topology), and the
range-boundary-in-DO-name encoding (`encodeRangeComponent`, already escaped, so bytes fit). Because it
is broader than `queryItems` and touches the live range-split/promotion code, it should be its **own
prerequisite SPEC**, with `queryItems` built on top of an already byte-consistent system.

### Action items

1. **Verify the ground truth empirically** (don't trust the doc-strings): `PRAGMA encoding`, then insert
   `"￿"` vs an emoji and `ORDER BY sk` to confirm astral-after-BMP UTF-8 ordering.
2. **Declare UTF-8 byte order the canonical key ordering** as a system invariant.
3. **Sequence (decided):** `queryItems` ships now with the **code-point `successor`** (option 3),
   correct for all well-formed strings. The byte/BLOB "canonical key ordering" SPEC is tracked
   separately as the eventual system-wide convergence; `queryItems` does not block on it.
