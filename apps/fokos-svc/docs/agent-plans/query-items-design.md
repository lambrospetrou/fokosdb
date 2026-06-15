# `queryItems` Design Plan

Status: **design / not yet implemented**. This plan defines a `queryItems` operation alongside
`putItem`/`getItem`/`deleteItem`, modeled on the DynamoDB `Query` operation and adapted to fokosdb's
hash-partition + promoted-range topology.

> **Update (post UTF-8/BLOB migration).** The system-wide byte/BLOB key migration that earlier
> revisions of this plan deferred as "out of scope" (Appendix A, option 4) **has shipped**. Keys are
> now stored as `BLOB` and compared by unsigned `memcmp`, and all JS-side key arithmetic runs in byte
> space via `KeyCodec`/`KeyBytes` (`src/lib/partition-topology/key-codec.ts`). Consequences threaded
> through this plan: the `beginsWith` upper bound now uses the existing byte-space
> `KeyCodec.successor()` (the code-point-`successor` stopgap is **dropped**); keys may be binary
> (`string | Uint8Array`); intervals/cursors carry `KeyBytes`, not strings; and Appendix A is now a
> historical record of a resolved issue rather than a pending concern.

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

Keys are `string | Uint8Array` everywhere a key appears — strings encode to UTF-8, `Uint8Array` to a
`0xFF`-tagged binary key (DynamoDB's Binary key type), exactly as `getItem`/`putItem` already accept.
`KeyCodec.encode()` validates at the boundary (rejects empty keys and lone surrogates, enforces the
1024/512-byte hash/sort key-length limits).

```ts
type Key = string | Uint8Array;

// Public, ergonomic — what most callers write:
type SortKeyCondition =
  | { op: "eq"; value: Key }
  | { op: "lt" | "lte" | "gt" | "gte"; value: Key }
  | { op: "between"; lower: Key; upper: Key }                // inclusive/inclusive (DDB semantics)
  | { op: "beginsWith"; prefix: Key }
  | { op: "range";                                            // low-level escape hatch
      lower?: { value: Key; inclusive: boolean };
      upper?: { value: Key; inclusive: boolean } };

type QueryItemsOptions = {
  queries: Array<{ hashKey: Key; sort?: SortKeyCondition }>; // 1..N; grouped in this order
  scanIndexForward?: boolean;          // default true
  limit?: number;                      // max items returned; ABSENT = no item-count cap (byte budget still applies)
  maxPageBytes?: number;               // default ~3-4MB, clamped to server max
  cursor?: string;                     // opaque continuation token
  countOnly?: boolean;                 // Select = COUNT (phase 4)
};

// Aggregatable scalars only — summed across every partition that responded.
type QueryItemsMeta = {
  rowsRead: number;          // total rows scanned (incl. filtered) across all visited DOs
  rowsReturned: number;      // total items returned this page (== count; differs under countOnly)
  forwardCount: number;      // total cross-partition forwards performed
  partitionsVisited: number; // number of leaf DOs that scanned rows (== partitionMetas.length; routers excluded)
};

type QueryItemsResult = {
  items: Array<{ hashKey: string | Uint8Array; sortKey?: string | Uint8Array;
                 data: string | Uint8Array;
                 ttlEpochUTCSeconds?: number; version: number }>;
  count: number;
  cursor?: string;                     // present iff more results remain
  meta: QueryItemsMeta;                // aggregatable scalars only
  partitionMetas: Array<OperationMetrics & PartitionInfo>; // leaf-only: hash leaves + non-split range partitions (no routers)
};
```

`meta` carries only the fields that are meaningful when summed across DOs. The per-DO fields that are
*not* summable — `databaseSize`, `servedByActorId/Name`, `servedByPartitionId`, `hashDepth` — live only
in `partitionMetas`, which holds the full `OperationMetrics & PartitionInfo` object from **every
data-bearing leaf** that scanned rows: a hash leaf (Case A) or a non-split range partition (the leaves of
a promoted key's range tree). A non-split range partition counts because it *holds data*; once it splits
it becomes a pure router and drops out.

**Routers are not enumerated — neither hash nor range.** Only leaves appear in `partitionMetas`, so
`partitionsVisited === partitionMetas.length` counts data-bearing DOs only. Every forwarding hop is still
captured numerically: each `QueryItemsRpcResult.meta.forwardCount` is **subtree-cumulative** — the shared
`withSplitForwarding` (also used by `getItem`/`putItem`/`deleteItem`) adds 1 per hash hop, and a range
router adds its child fan-out plus every descendant router's forwards. `FokosDB.queryItems` reads the
aggregate `forwardCount` straight off the top-level `meta` (not by summing the array). This keeps
`partitionMetas` a clean per-leaf debugging trail while `forwardCount` still reflects full routing depth.

**Budget resolution (public → internal).** `limit`/`maxPageBytes` are user-facing and optional;
`FokosDB.queryItems` resolves them into the internal RPC budget before fan-out: `maxPageBytes` →
`budgetBytes` (always present after default resolution), `limit` → `remainingLimit: number | null`
(`null` = uncapped, when the user omitted `limit`). Users never set `budgetBytes`/`remainingLimit`
directly. See §7 for the RPC types.

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

**Input validation (all reject with an error, not an empty result):**

- `queries.length === 0` → error.
- `limit` present and `<= 0` → error. (Absent `limit` = uncapped, valid.)
- `maxPageBytes` present and `<= 0` → error. The first matched row is always returned even if it alone
  exceeds the budget (so a tiny `maxPageBytes` degrades to one-row pages, never zero-row stalls).
- `cursor` that fails to decode, has an unknown version, or whose direction / request-fingerprint does
  not match the current request → error (see §5).
- A per-key condition that normalizes to an **empty interval** (e.g. `between a b` with `a > b`, or an
  inverted `range`) is **not** an error — that sub-query simply contributes no items.

### Sort-key condition → interval (the only primitive the store sees)

A single pure normalization function collapses every `SortKeyCondition` into one canonical interval
**before** anything else runs, so the store/traversal never learns about operators. The interval is in
**byte space** — bounds are `KeyBytes`, the same canonical representation the store and routing use:

```ts
type SkInterval = {
  lower?: { value: KeyBytes; inclusive: boolean };
  upper?: { value: KeyBytes; inclusive: boolean };
};
```

Literals are run through `KeyCodec.encode()` during normalization (this is where validation and the
key-length limits apply). Then:

- `eq v` → `lower=v incl, upper=v incl`
- `beginsWith p` → `lower=p incl, upper=KeyCodec.successor(p) excl`; if `successor(p)===null` (prefix is
  all `0xFF`, no byte-space successor) → `upper=none` (unbounded). `beginsWith("")` → match-all
  (`lower=absent-sentinel incl, upper=none`)
- `between a b` → `lower=a incl, upper=b incl`; if `KeyCodec.compare(a,b) > 0` → empty result
- `gt`/`gte` → lower bound only (excl/incl)
- `lt`/`lte` → upper bound only (excl/incl)
- `range {lower?, upper?}` → encoded and passed through; inverted/empty interval (`compare(lo,hi) > 0`,
  or equal with either end exclusive) → empty result
- (no `sort`) → whole hashKey (`lower=absent-sentinel incl, upper=none`)

The "absent sentinel" is the empty byte string (`KeyCodec.encodeOptional(undefined)` → `[]`), which is
the global byte minimum and the same value stored for items written without a sort key. Because
`KeyCodec.encode("")` *rejects* empty input, the match-all cases (`beginsWith("")`, no `sort`) are
special-cased to this sentinel rather than routed through `encode`.

`KeyCodec.successor(p)` (the `beginsWith` upper bound) is the one place we **synthesize** a key. It
increments in raw byte space, so its result matches SQLite's `BLOB`/`memcmp` ordering exactly, keeps
`beginsWith` as an index-friendly `sk >= :p AND sk < :succ` range scan, and avoids `LIKE`/`GLOB`. UTF-8
(and the `0xFF`-tagged binary encoding) is prefix-preserving, so byte-space `beginsWith` is exactly
character/binary-space `beginsWith` for both key types. The other operators take user-provided literals
that bind directly as `BLOB` params and are compared by `memcmp` — no JS-side synthesis. See Appendix A
for the (now-resolved) history behind this.

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

- New `PartitionDO.queryItems` RPC. A **leaf** range DO scans its slice ∩ requested interval (slice
  ownership reuses the established `KeyCodec.compare(sk, start) >= 0 && (end===null || compare(sk,end) <
  0)` check) and returns a page plus a remaining-budget signal. A **router** range DO iterates its
  children in boundary order (reverse order if `scanIndexForward=false`), calling each child's
  `queryItems` with
  the *remaining* byte/limit budget, short-circuiting once the budget is spent. It returns the combined
  page and a cursor = last `(hashKey, sk)` emitted.
- This fills a page across multiple leaf DOs in a single client round-trip, and keeps the
  stub-ownership boundary intact (only DO classes / FokosDB acquire stubs).
- Resumption uses the **logical** `(hashKey, sk)` token (§5), not physical DO identity, so a split or
  promotion landing between pages just changes which DO answers the same logical boundary.

**Router walk algorithm (explicit).** A range router serving `queryItems(interval, direction,
budgetBytes, remainingLimit, cursor)`:

1. Order its children by boundary — **ascending** for `asc`, **descending** for `desc`.
2. **Skip** any child whose `[start, end)` does not intersect the requested `interval` (compare with
   `KeyCodec.compare`). On a resumed call, also skip children that lie entirely *before* (for `asc`) /
   *after* (for `desc`) the cursor's `sk`.
3. For each remaining child, in order, call its `queryItems` with: the `interval` **clipped** to the
   child's `[start, end)`, the *current remaining* `budgetBytes` and `remainingLimit`, and the cursor
   (only the child that contains the cursor's `sk` receives a non-null cursor; later children start
   fresh). Accumulate returned `items`, subtract `bytesConsumed` from `budgetBytes` and the returned
   item count from `remainingLimit`, and concatenate `partitionMetas`.
4. **Stop conditions, in priority order:**
   - If a child returns a **non-null `nextCursor`** (its own budget/limit stopped it mid-slice) → stop;
     return the accumulated page with that `nextCursor`. (The leaf-level cursor `(hk, sk)` is already a
     valid logical resume point.)
   - Else the child fully drained its clipped slice. If `budgetBytes <= 0` **or** `remainingLimit === 0`
     after the subtraction → stop; set `nextCursor` to the **last emitted `(hk, sk)`**. On resume,
     re-routing that `sk` lands back in the just-finished child, which scans an empty tail
     (`sk > lastSk`) and the walk advances to the next child — one redundant (cheap) child visit per
     resume, in exchange for a purely logical cursor.
   - Else continue to the next child.
5. If all children drain within budget → `nextCursor = null` (this subtree is complete). A range router
   contributes **no** `partitionMetas` entry (only leaves do); it folds its child fan-out and every
   descendant router's forwards into its `meta.forwardCount`, which propagates up as a cumulative count.

A **leaf** range DO (and a Case-A hash leaf) runs the same contract over its local `items` slice via the
generalized `queryRangeItemsPage` + extended `collectBatch` (§6) — `getItemsBatchForRange`
(`do-partition.ts`) is the existing template to generalize.

## 5. Pagination / cursor design

Opaque, forward-only token storing **logical position, not physical DO identity**. The key fields
carry **`KeyBytes`** (raw bytes), not strings: binary keys aren't representable as strings, and
resumption re-routes through `KeyBytes`-native routing, so carrying bytes avoids re-encoding and any
"was this string or binary?" ambiguity:

```ts
type DecodedCursor = {
  version: number;         // cursor format version; unknown → reject (forward-compat, §11)
  fingerprint: bigint;     // hash64 of the request identity (validation, below)
  direction: "fwd" | "rev";// pinned at first page; mismatch on resume is rejected
  queryIdx: number;        // index into the `queries` sub-query list to resume at
  // resume position within queries[queryIdx], or null to start that sub-query from its first row
  inner: { hashKey: KeyBytes; sortKey: KeyBytes; inclusive: boolean } | null;
};
```

The `queryIdx` + optional `inner` split is what makes the cursor *cross-sub-query*: a page that
stops mid-slice resumes the **same** sub-query at `inner` (Stop 1); a page that exhausts the global
budget right as a sub-query drains resumes the **next** non-empty sub-query with `inner = null`
(Stop 2, "start fresh"). `inner.inclusive` is the boundary-continuation flag the range-walk's
fan-out cap emits; row-derived cursors are exclusive.

**Wire encoding.** `JSON.stringify`, then base64url — keys are base64'd inside the JSON and the
`bigint` fingerprint is a decimal string (JSON has no bigint). Chosen for simplicity over a
hand-rolled binary layout; the token is opaque and only needs to round-trip.

Decode validates: known `version`, well-formed JSON, `direction` matches the request, in-range
`queryIdx`, parseable `fingerprint`, and (if present) decodable `inner` keys — then the
`fingerprint` is compared. Any failure → error (§2 validation).

**Cursor ↔ request binding (DynamoDB-style logical cursor).** The cursor is logical-only; the caller
**must re-send the same request** on resume. We guard accidental misuse with a `fingerprint = hash64(...)`
(`hash-primitives`) over the request's *identity-determining* fields **only**: the ordered `queries[]`
(each `hashKey` bytes + its normalized `SkInterval` bounds + inclusivity flags, with **empty intervals
explicitly marked** so the list — and thus every `queryIdx` — is stable) and `direction`. It does
**not** cover `limit` / `maxPageBytes` / `countOnly` — those may legitimately change between pages (as
DynamoDB allows changing `Limit`). Build a length-delimited canonical `Uint8Array` of those fields and
hash it; on resume recompute and compare. This is best-effort — a guard against accidental misuse
(edited/swapped request, cross-query token reuse), **not** a security boundary.

Resumption re-routes the cursor's `hashKey`/`sortKey` through normal routing, so the logical boundary is
stable across splits/promotions. `collectBatch`'s contract is reused (extended with `maxItems`, §6):
the cursor advances past every scanned row; the first matched row is always included even if oversized;
`nextCursor` is non-null **only** when the byte budget *or* item cap stopped the scan. An empty page with
a non-null cursor is valid (a run of skipped rows hit the budget; or — DynamoDB-consistent — the item
cap landed exactly on the last row, so the next page is empty with a null cursor).

## 6. Store-layer changes (`partition/partition-store.ts`)

- Generalize `queryRangeItemsPage` to take `direction: "asc" | "desc"` and inclusive/exclusive bounds,
  emitting `ORDER BY sk ASC|DESC` with the matching cursor comparator. Today its signature is
  `{ hk: KeyBytes; lower: KeyBytes; end: KeyBytes | null; cursor: MigrationCursor | null; limit }`,
  hard-wired to **ascending**, **lower-inclusive** (`sk >= lower`), **upper-exclusive** (`sk < end`),
  with cursor resume `sk > ?`. We add: `desc` (`ORDER BY sk DESC`, cursor `sk < ?`), **inclusive upper**
  (`sk <= ?`, needed by `eq`/`between`), and **exclusive lower** (`sk > ?`, needed by `gt`/`range`).
  Bounds stay `KeyBytes`; the `MigrationCursor` is already byte-based, so the cursor comparator just
  flips with direction.

  **Bound-inclusivity × cursor truth table** (the SQL builder must encode exactly this — the
  start-bound flag matters only on the *first* page, because resume is always strict, mirroring
  DynamoDB's exclusive `ExclusiveStartKey` + every-page `KeyConditionExpression`):

  | direction | first page (no cursor) | resume (cursor present) | far-bound (every page) |
  |---|---|---|---|
  | `asc`  | `sk >= lower` or `sk > lower` (per lower flag) | `sk > cursor` (always strict) | `sk < end` or `sk <= end` (per upper flag) |
  | `desc` | `sk <= upper` or `sk < upper` (per upper flag) | `sk < cursor` (always strict) | `sk > start` or `sk >= start` (per lower flag) |

- **Byte budget metric.** Pagination uses the existing `estimateItemBytes` (→ `estimateRowBytes`), i.e. a
  **stored-row-size estimate** (`data + hk + sk + ~40B fixed overhead`), **not** serialized wire size.
  `maxPageBytes` is therefore an estimate that runs slightly above pure `data` bytes; this is intentional
  (cheap, and consistent with the migration/promotion budgets that already use it).
- **Item-count cap.** Extend `collectBatch` (`partition/batch-scan.ts`) with an optional
  `maxItems?: number`: after the cursor advances past an *included* row, if `rows.length >= maxItems`
  set `reachedLimit` and stop. Backward-compatible (existing callers omit it → unchanged). The check sits
  *after* `advanceCursor` (the row fit, so don't re-emit it), unlike the byte-budget break which sits
  *before* `advanceCursor` (the oversized row is re-fetched next page). The leaf passes `remainingLimit`
  as `maxItems`; the router decrements the global remaining by each child's returned count.
- (Phase 4) `countItemsInRange(hk, lower, upper, dir)` for `countOnly`.
- No schema change: keys are `BLOB` and `PRIMARY KEY (hk, sk)` on `WITHOUT ROWID` already orders by
  `memcmp` in both directions — `KeyCodec.compare()` matches this exactly, so no `COLLATE` is needed.

## 7. Orchestration placement

- **`FokosDB.queryItems`** (`db.ts`): validate (§2); encode keys via `KeyCodec.encode()` (mirroring
  `keyIn`/`optKeyIn`); normalize each `sort` to a byte-space `SkInterval` (this is where
  `KeyCodec.successor()` and the empty-interval `KeyCodec.compare()` checks run); **resolve the budget**
  (`maxPageBytes` → `budgetBytes`, `limit` → `remainingLimit: number | null`); fan out across the
  `queries[]` sub-queries enforcing the *global* byte/limit budget; assemble + fingerprint the
  cross-query cursor; aggregate `meta` and collect `partitionMetas`. FokosDB may hold stubs.
- **`PartitionDO.queryItems`** RPC (`do-partition.ts`): mirrors `withSplitForwarding`'s promotion +
  split-forward logic and adds the range-tree walk (§4). Types:

  ```ts
  type QueryItemsRpcRequest = {
    hashKey: KeyBytes;
    interval: SkInterval;            // byte-space, already normalized by FokosDB
    direction: "asc" | "desc";
    budgetBytes: number;             // remaining byte budget for THIS call (resolved; required)
    remainingLimit: number | null;   // remaining item cap; null = uncapped
    cursor: MigrationCursor | null;  // { hk, sk } logical resume position (null = first page)
  };
  type QueryItemsRpcResult = {
    items: MigratedItem[];                                   // { hk, sk, data, ttl_epoch_utc_seconds?, v }
    nextCursor: MigrationCursor | null;
    bytesConsumed: number;                                   // for the caller to decrement the byte budget
    partitionMetas: Array<OperationMetrics & PartitionInfo>; // this subtree, including self
  };
  ```

  `MigratedItem`/`MigrationCursor` already exist and cross workerd RPC; `SkInterval` is a flat struct of
  `Uint8Array` + booleans and structured-clones fine. `rowsRead`/`forwardCount` live inside each
  `partitionMetas` entry; the public `QueryItemsMeta` is the summed projection of them.
- Reuse `collectBatch` (byte budget + new `maxItems` cap) and `estimateItemBytes` (sizing).

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
  Measured as a **stored-row-size estimate** (`estimateItemBytes`), not wire size (§6).
- `limit` (item count) and the byte budget are independent; stop at whichever hits first. `limit` is
  **optional** — absent means uncapped (only the byte budget bounds the page).
- Each DO hop does bounded work (the range walk honors the remaining budget) — no unbounded hop. No
  artificial cap on leaves-per-page; the byte/item budget is the backstop.
- `meta` aggregates `rowsRead`/`rowsReturned`/`forwardCount`/`partitionsVisited`; `partitionMetas`
  carries the full `OperationMetrics & PartitionInfo` for **leaf DOs only** (routers excluded, §2, §10).

## 10. Phasing

1. **Phase 1 — ✅ DONE** — single sub-query (A+B), forward direction, all sk operators incl.
   `beginsWith`, byte+limit pagination, opaque cursor, lock = return-committed. Delivers DynamoDB-Query
   parity for one key. `beginsWith` uses the existing `KeyCodec.successor()` — no new key-synthesis code.
2. **Phase 2 — ✅ DONE** — `scanIndexForward=false` (D) end-to-end (store + reverse range walk).
3. **Phase 3 — ✅ DONE** — multi-sub-query fan-out (C) + cross-query cursor (grouped-by-hashKey).
   `FokosDB.queryItems` walks `queries[]` in list order under one global byte/limit/visit budget,
   concatenating each sub-query's group; the cursor carries `queryIdx` plus an optional in-sub-query
   resume position (absent = start the next sub-query fresh). The fingerprint covers the full ordered
   list (incl. empty intervals). The cursor is now JSON+base64url (was a hand-rolled binary layout).
4. **Phase 4 (optional)** — `countOnly`, first-class `OR`/multi-range, lock-aware modes.

## 11. Decisions locked

- Request shape: **list of `{hashKey, sort?}` sub-queries**; shared-condition convenience form lives in
  a **utilities/SDK package**, not the core wire API.
- **Duplicate hash keys allowed**; dedup is the caller's responsibility (this is the OR/multi-range
  escape hatch). First-class `OR` deferred.
- Sort condition: ergonomic ops **and** a low-level `range` form, both **normalized to one byte-space
  `SkInterval`** (`KeyBytes` bounds) before processing.
- Keys are `string | Uint8Array` (UTF-8 strings / `0xFF`-tagged binary), encoded via `KeyCodec` at the
  boundary — same as `getItem`/`putItem`.
- Multi-key ordering: **grouped by hashKey** (request order; sk order within each group).
- Range traversal: **bounded recursive walk** (router fills the page across leaves per call).
- Locked rows: **return the last committed value**.
- TTL: **mirror `getItem`** — no read-time filtering.
- `beginsWith` upper bound: **byte-space `KeyCodec.successor()`** + range scan (the system-wide
  byte/BLOB migration, Appendix A option 4, has **shipped** — the earlier code-point-`successor` stopgap
  is dropped).
- Item cap: **extend `collectBatch`** with an optional `maxItems` (not a forked function); checked after
  the cursor advances past an included row.
- Byte budget metric: reuse `estimateItemBytes` (**stored-row-size estimate**, not wire size).
- Cursor: **logical-only, DynamoDB-style** (caller re-sends the same request), validated by a 32-bit
  `hash32` **request fingerprint** (queries + direction only) plus a leading **version** byte; binary
  layout, base64url. Decode/version/direction/fingerprint failures are errors.
- `meta`: **aggregatable scalars** at top level (`rowsRead`, `rowsReturned`, `forwardCount`,
  `partitionsVisited`) **+ `partitionMetas`** array of full per-DO meta for **leaf DOs only** (hash
  leaves + non-split range partitions; routers excluded, captured numerically via cumulative
  `forwardCount`).
- Budget inputs (`limit`, `maxPageBytes`) are **user-optional with defaults**; resolved to the internal
  RPC's `remainingLimit`/`budgetBytes`. `limit` absent = uncapped.
- Range-router walk: **explicit algorithm** in §4 (boundary-ordered children, interval clip, budget
  decrement, stop on non-null child cursor or exhausted budget, last-emitted `(hk, sk)` as resume token).

## Appendix A — Key encoding & ordering: UTF-16 vs TEXT vs BLOB (RESOLVED — historical record)

> **Resolved.** This appendix originally argued for moving the whole system to UTF-8/byte keys. That
> migration has **shipped** (keys are `BLOB`/`memcmp`, all JS key arithmetic is byte-space `KeyCodec`),
> so the "latent inconsistency" below no longer exists and option 4 is now the implemented reality.
> `queryItems` is built on top of an already byte-consistent system; `beginsWith` uses
> `KeyCodec.successor()`. The text is kept for the rationale and to document what was done.

This appendix records the discussion behind how sort/hash keys are *ordered* and *compared*, why the
former scheme had a latent inconsistency, and the direction that was taken. It is broader than
`queryItems` — `queryItems` only *surfaced* the issue via `beginsWith`'s synthesized `successor(p)`.

### The latent inconsistency (now eliminated)

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
3. **Code-point `successor` + range scan — superseded.** Was the interim choice while keys were `TEXT`:
   compute `successor` in code-point space (`codePointAt`, max `U+10FFFF`, carry). Correct for all
   well-formed strings, but a stopgap. Dropped once option 4 shipped — `KeyCodec.successor()` (raw byte
   carry) is strictly more general (handles binary keys, no surrogate loose end).
4. **Bytes/BLOB end-to-end — ✅ SHIPPED (now the implementation).** The uniformly-consistent end state;
   delivered as its own migration. See below for what it entailed.

### System-wide target: UTF-8 / byte keys everywhere — SHIPPED

This was delivered as its own migration ahead of `queryItems`. The whole system now agrees on one
byte-wise order (`src/lib/partition-topology/key-codec.ts`):

- The key columns (`items.hk/sk`, `pending_transactions.hk/sk`, `promoted_keys.hash_key`,
  `key_size_estimates.hk`) are now **`BLOB`**; string keys encode to UTF-8 at the boundary, binary keys
  to a `0xFF`-tagged form, and **all** comparison + `successor` + separator arithmetic runs in **raw
  byte space** (`KeyCodec.compare`/`successor`/`shortestSeparator`).
- `BLOB` comparison is `memcmp` (unsigned bytes) = `KeyCodec.compare` = DynamoDB semantics. Boundaries
  need **not** be valid UTF-8 (a byte-space `successor` can produce non-UTF-8 bytes — e.g. prefix ending
  `0x7F` → `0x80`; `BLOB` accepts it, `TEXT` could not). UTF-8 is prefix-preserving, so byte-space
  `beginsWith` is exactly character-space `beginsWith`.
- **Binary keys** (DynamoDB's Binary key type) are now first-class via the `0xFF` tag.

What it touched: the schema, every JS key comparison site (`shortestSeparator`,
`computeRangeSplitBoundaries`, range-ownership checks, partial-range-topology), the
range-boundary-in-DO-name encoding (`encodeRangeComponent`), and hash routing (now depth-seeded over
`KeyBytes`). `queryItems` is built on top of this already byte-consistent system.

### Action items — done

1. ~~Verify the ground truth empirically~~ — moot; keys are `BLOB`/`memcmp`, ordering is byte-wise by
   construction (no reliance on `TEXT` collation).
2. **UTF-8/unsigned-byte order is the canonical key ordering** — now a system invariant enforced by
   `KeyCodec` and the `BLOB` columns.
3. **Sequence (done):** the byte/BLOB migration shipped first; `queryItems` now builds on it and uses
   `KeyCodec.successor()` for `beginsWith`.
