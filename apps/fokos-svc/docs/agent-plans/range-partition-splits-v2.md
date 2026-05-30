# Range Partition Splits — Implementation Plan v2

This is the authoritative implementation plan for splitting a single hash key's data along the sort
key axis. It is self-contained: it specifies the data model, routing, transactions, migration,
lifecycle, and an ordered set of implementation phases.

## Goal

Hash partition splits scatter items across DOs by `hashKey`. They cannot help a single `hashKey`
that accumulates many `sortKey`s — all those items share one key and always hash to the same child.
The second split axis: **range structures** that own exactly one `hashKey` and split along the
`sortKey` axis.

## Scope of this version

**In scope:** promotion of a heavy hash key into a range structure, range splits within that
structure, correct routing and transactions via router traversal, migration, mutual exclusion with
hash splits, destroy traversal.

**Deferred to a later version (separate group of DOs):** caller-side topology caching (LOUDS), the
bloom filter, and the per-key boundary directory that together enable direct caller→leaf routing.
Until then we rely on **parent-as-router traversal**: a request that lands on an ancestor of the true
owner is forwarded down to it. This adds round trips but is correct. Nothing in this version should
preclude adding those caches later (DO names are computable, boundaries derive from sibling starts).

---

## Architecture: two tiers

The hash partition tree stays **pure hash** (so it remains LOUDS-encodable later — a unified tree
would need unbounded per-node routing data). Range structures are a **separate, self-routing
structure per promoted hash key**, linked from the hash partition that owned the key.

```
Hash partition P  (owns a hash-key slice; may hash-split as today)
  │  promoted-keys set: { "alice", ... }   →  forwards alice to its range structure
  ▼
Range root  mydb.r.alice.            owns sk ∈ [∅, B1)        ← created at promotion; keeps leftmost
  ├─ child  mydb.r.alice.{enc(B1)}   owns sk ∈ [B1, B2)
  └─ child  mydb.r.alice.{enc(B2)}   owns sk ∈ [B2, ∅)
```

### Why forwarding is always correct

A stale topology view always points to an **ancestor** of the true owner (splits only _add_
descendants; a node once believed to be a leaf becomes a router above the real owner). There are
three kinds of downward link, and forwarding only ever descends:

1. **Hash-descendant** — a split hash partition routes to its hash child.
2. **Promotion cross-link** — a hash partition routes a promoted key to `rangeRoot(hashKey)` via its
   promoted-keys set. (This is why the promoted-keys set is inherited on a hash split — it keeps the
   link reachable from wherever a request lands.)
3. **Range-descendant** — a range DO routes to its child leaf by sort key.

Land anywhere at-or-above the true owner, forward down, and (for multi-item transactions) aggregate
the results on the way back up.

---

## Invariants (the contract)

1. **Start boundary = immutable identity; end boundary = mutable local state.** A range DO's lower
   bound is fixed for life (part of its name and its validated context). Its upper bound shrinks when
   it splits and is stored **only locally** (KV), never in the validated `PartitionContextResolved`.
   This makes a self-mutating identity impossible.
2. **Range children are contiguous and cover the parent's range.** Therefore `end[i] = start[i+1]`
   and `end[last] = parent.end`. End boundaries are never stored in contexts — they derive from the
   sorted sibling start boundaries plus the node's own end.
3. **A partition forwards what it does not own locally and aggregates the result; it may own some
   items and forward others in the same operation.** For `prepare`/`commit`/`cancel`/
   `readForTransaction`, a partition groups the request's items into a local set (keys it owns) and
   per-child forward sets (keys that belong to a hash child, a promoted key's range structure, or a
   range child). It executes the local set, forwards each child set, and returns the aggregate. The
   transaction coordinator therefore only ever talks to the partitions it originally contacted (the
   "tops"); fan-out below them is internal. Any item a partition can neither own nor route is a
   routing bug and is rejected.
4. **Single-key ops (`getItem`/`putItem`/`deleteItem`) forward transparently** the same way, so
   stale callers keep working without client-visible errors.
5. **Hash splits and range promotions are mutually exclusive on the same hash partition.** No hash
   split may start while any promoted key on that partition is `queued` or `promoting`, and no
   promotion may advance while a hash split is queued or started.
6. **Correctness never depends on any cache.** The hash partition's promoted-keys set is the source
   of truth for "is this key ranged"; a range DO self-identifies via the presence of
   `__partition_context`.
7. **Always route via the `doName` carried in the propagated `PartitionContextResolved`; never read
   `this.ctx.id.name`.** DO names can be arbitrarily long (the runtime hashes them for `idFromName`),
   and `ctx.id.name` may be truncated. All DO addressing uses `env[ns].idFromName(ctx.doName)` with
   the context's `doName`, as the existing code already does (`getChildStub`, `pickPartition`). This
   is why range names can embed full hash keys and sort-key boundaries with no length concern.
8. **A range root is fully initialized before any request reaches it, and a key is cut over to its
   range structure only when it holds no in-flight transaction locks.** The hash partition forwards a
   key to its range structure only once the key's status is `promoting` (or `promoted`). The
   transition `queued → promoting` (the cutover) happens in one `transactionSync` that first verifies
   the key has no `pending_transactions` rows. This prevents two failures: (a) a forwarded write
   initializing an empty range root before migration is set up, and (b) a transaction that locked the
   key on the hash partition later having its `commit` routed to the range root, where the lock does
   not exist.
9. **`partitionId` is self-describing for both tiers.** A range DO's `partitionId` is a real
   `PartitionIdHelper` value tagged with a range schema byte that encodes `(hashKey, startBoundary)`.
   It is immutable identity and is validated like a hash partition's `partitionId`.

---

## Naming

New helper in `partition-topology/partition-topology.ts` (or a small `range-naming.ts`):

```typescript
// Percent-encodes any char that is not [A-Za-z0-9_-] so the literal "." delimiters are unambiguous.
function encodeRangeComponent(s: string): string;

// Range DO name. startBoundary === null means the range root (owns from ∅).
// The ".r." namespace marker mirrors the ".h." marker hash partitions use
// (`db.h.<root>[.<child>…]`, partition-topology.ts:194) and is REQUIRED to avoid collisions:
// without it, a range leaf for hash key "h" with start boundary "0" would be `db.h.0`, colliding
// with hash root partition 0.
function rangePartitionDoName(databaseName: string, hashKey: string, startBoundary: string | null): string {
	const hk = encodeRangeComponent(hashKey);
	const sk = startBoundary == null ? "" : encodeRangeComponent(startBoundary);
	return `${databaseName}.r.${hk}.${sk}`;
}
```

- `rangeRoot(hashKey)` ≡ `rangePartitionDoName(db, hashKey, null)` → `db.r.<enc(hk)>.`.
- The `.r.` marker keeps the range and hash name spaces disjoint (hash = `db.h.…`, range = `db.r.…`).
- Start boundaries are real `sk` values; `(hk, sk)` is a PK, so they are unique within a hash key →
  no name collisions among range DOs either.
- **No name-length limit.** The runtime hashes DO names internally, so `idFromName` accepts names of
  any length. The only hazard is reading a name back from `this.ctx.id.name` (can be truncated) — see
  invariant 7.

---

## Type changes

### `partition-topology/types.ts`

`SplitType = "hash" | "range"` already exists. **No new SplitType** — promotion is tracked
separately (see Promoted keys), and range splits reuse `"range"`.

### `partition-topology/partition-topology.ts`

Extend `PartitionContextResolved` (immutable identity only — no end boundary):

```typescript
export type PartitionContextResolved = PartitionContext & {
	doName: string;
	primaryDoIdStr: string;
	partitionId: PartitionNodeId; // for range DOs, a SCHEMA_RANGE id (see PartitionIdHelper below)
	_partitionIdBytes?: Uint8Array;

	// Present only on range-structure DOs. Immutable identity.
	// Redundant with the decoded partitionId, but kept denormalized for cheap routing/filters.
	rangePartition?: {
		hashKey: string;
		startBoundary: string | null; // null = range root (owns from ∅)
	};
};
```

#### `PartitionIdHelper` — range schema

Today `partitionId` is `[version=0x00, rootHi, rootLo, depth, childIdx…]` and its `doName` is
`db.h.<root>[.<child>…]` (`partition-topology.ts:188-195`; the schema byte is currently `0x00`,
asserted in every reader). Range DOs are not in the hash tree, so a hash-path id is meaningless for
them — but we still want `partitionId` to fully describe a node and to derive its `doName`. Add a
second schema:

```typescript
static readonly SCHEMA_HASH_V1  = 0x00;  // existing format (currently the bare literal 0)
static readonly SCHEMA_RANGE_V1 = 0x01;

// Wire format for SCHEMA_RANGE_V1:
//   byte[0]      = 0x01
//   byte[1]      = flags: bit0 = hasStartBoundary (0 ⇒ range root, owns from ∅)
//   byte[2..5]   = uint32 LE length of the hashKey UTF-8 bytes
//   byte[6..]    = hashKey UTF-8 bytes, then (if hasStartBoundary) startBoundary UTF-8 bytes to end
static fromRangePartition(base: PartitionContext, hashKey: string, startBoundary: string | null): PartitionIdHelper;
```

- The existing readers (`doName`, `rootIdx`, `depth`, `lastChildIdx`, `encode`) currently assert
  `bytes[0] === 0`; generalize them to dispatch on the schema byte. Callers that walk the hash tree
  (`depth`, `lastChildIdx`, `appendHashIdx`) keep asserting `SCHEMA_HASH_V1`.
- `decode` branches on `byte[0]`: `0x00 → hash id`, `0x01 → range id` (exposing
  `{ hashKey, startBoundary }`).
- `doName` branches on the schema byte: hash → existing `db.h.<root>[.<child>…]`; range →
  `rangePartitionDoName(db, hashKey, startBoundary)` (i.e. `db.r.<enc(hk)>.<enc(sk)>`). So
  `partitionId` alone determines `doName` for both tiers; `rangePartition.*` on the context is a
  denormalized convenience.
- `startPromotion` and range `startSplit` build the new context's `partitionId` via
  `PartitionIdHelper.fromRangePartition(...)`; `ensurePartitionContext` validates it as immutable
  exactly like a hash id.

### `do-partition.ts`

Extend `InitFromSplitOptions`:

```typescript
export type InitFromSplitOptions = {
	parentPartitionContext: PartitionContextResolved;
	newPartitionContext: PartitionContextResolved; // carries rangePartition.* for range DOs
	splitType: SplitType; // "range" for both the promotion root and range-split children
	// Initial end boundary for a range child (mutable local state, NOT part of identity).
	// null = unbounded (range root). Omitted for hash children.
	rangeEndBoundary?: string | null;
};
```

### `transaction-types.ts`

`PrepareResponse` is unchanged — `{ outcome: "accepted" } | { outcome: "rejected"; reason }`. A
partition forwards-and-aggregates internally and returns a single accepted/rejected result, so there
is no new outcome and the coordinator protocol does not change.

---

## State

### Hash partition DO — promoted keys

New SQL table (migration M3): the set of promoted keys and their lifecycle status. The range-root
name is computed, not stored.

```sql
CREATE TABLE IF NOT EXISTS promoted_keys (
  hash_key   TEXT NOT NULL PRIMARY KEY,
  status     TEXT NOT NULL CHECK (status IN ('queued', 'promoting', 'promoted')),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
) STRICT;
```

Three statuses:

- `queued`: detection picked this key, but the range root is **not yet created** and the hash
  partition **still serves the key locally** (no forwarding). Writes/locks keep landing on the hash
  partition normally.
- `promoting`: range root created **and** `initFromSplit`-complete, then cut over (the
  `queued → promoting` flip verified no pending locks for the key). The hash partition now forwards
  reads to the range structure (which reads-from-parent while migrating), forwards single-key writes
  (rejected with 503 by the migrating root), and forwards transaction items.
- `promoted`: migration complete; the hash partition has GC'd the key's local items.

In-memory cache `Map<string, "queued" | "promoting" | "promoted">` on `PartitionDO`, loaded in
`blockConcurrencyWhile`, updated transactionally alongside every `promoted_keys` write. **Only
`promoting`/`promoted` cause forwarding; `queued` does not.**

### Range-structure DO — local end boundary

New KV key (range DOs only):

| KV key                 | Value            | Notes                                                                                     |
| ---------------------- | ---------------- | ----------------------------------------------------------------------------------------- |
| `__range_end_boundary` | `string or null` | Mutable. `null` = unbounded. Set at `initFromSplit`; shrunk on self-split. NOT validated. |

Range DOs reuse all existing split KV keys (`__split_status`, `__split_migration_status`,
`__split_migration_cursor`, `__parent_partition_context`, `__parent_split_type`) unchanged. Range
splits of a range DO use the existing `__split_status` machinery with `splitType: "range"`, with one
additive field on the `split_started`/`split_completed` variant of `SplitStatusKVItem`:

```typescript
// Added to the split_started | split_completed variant; present only for splitType === "range".
parentEndAtSplit?: string | null; // the splitting DO's end boundary at the start of the current
                                  // split round; null = unbounded. Used by the rightmost new
                                  // child's migration filter (the parent's own end has since shrunk).
```

`childPartitionContexts` holds **only the new children** (the retained leftmost self is never
listed), and it **accumulates across successive splits of the same node** (see Range-split
lifecycle). Routing never needs end boundaries.

### SQL migration M3

```sql
-- M3: range partition support (created in all partition DOs; unused where not applicable)
CREATE TABLE IF NOT EXISTS promoted_keys (
  hash_key   TEXT NOT NULL PRIMARY KEY,
  status     TEXT NOT NULL CHECK (status IN ('queued', 'promoting', 'promoted')),
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
) STRICT;
```

There is no `range_split_children` table — range-split children live in
`__split_status.childPartitionContexts`, the single source of truth, so `cancel`/ack/`destroy` keep
working unchanged.

---

## Topology selection: `ensureTopology`

`ensureTopology(pCtx)` selects the implementation by context:

```typescript
private ensureTopology(pCtx: PartitionContextResolved): PartitionTopologySplitter {
  if (!this.#_topology) {
    this.#_topology = pCtx.rangePartition
      ? new RangePartitionTopologyImpl("", pCtx, this.ctx)
      : new PartitionTopologyImpl("", pCtx, this.ctx);
  }
  return this.#_topology;
}
```

`RangePartitionTopologyImpl implements PartitionTopologySplitter`:

- `shouldAllow(hk, sk)`:
  - If `split_status` is `split_started`/`split_completed`: `"ok"` if `sk` ∈ own `[start, end)`
    (serve locally — the retained node IS the leftmost owner), else `"forward"`.
  - Else (not split): `"ok"` if `sk` ∈ own `[start, end)`, else `"reject"` (out of structure; should
    not happen via correct routing). Size-based `"reject"` uses `rangeSplitConditions`.
  - `start` = `pCtx.rangePartition.startBoundary`; `end` = local `__range_end_boundary`.
- `pickChildPartition(pCtx, hk, sk)`: the children tile `[smallestChildStart, parentEndAtSplit)`
  contiguously, so routing is the child in `split_status.childPartitionContexts` with the **largest
  `startBoundary ≤ sk`**. `sk` below the smallest child start is served locally via `shouldAllow` and
  never reaches `pickChildPartition`. (Because `childPartitionContexts` accumulates across re-splits,
  this single rule keeps working as the node re-splits its retained slice.)
- `maybeQueueSplit` / `shouldSplit`: use `rangeSplitConditions` and queue `splitType: "range"`.
- `startSplit`: the range-split path (below).
- `makeIsCorrectChildHashPartition`: not used for range; provide `makeIsCorrectChildRangePartition`
  returning a sort-key-range predicate (below).

---

## Routing changes

### Single-key ops (getItem / putItem / deleteItem) — `withSplitForwarding`

On a **hash partition**, before the existing `shouldAllow` logic, consult the promoted-keys cache:

```
status = promotedKeys.get(hashKey)
if status === "promoting" || status === "promoted":   // NOT "queued" — queued still serves locally
    forward to rangeRoot(hashKey)       // parent-as-router; the range structure routes by sk
else:
    existing hash shouldAllow/forward/reject logic    // includes status === "queued"
```

`queued` deliberately does not forward: the range root may not exist yet and the key is still owned
locally. Only after the `queued → promoting` cutover (invariant 8) does forwarding begin.

The range root then forwards by sort key down its own structure via `RangePartitionTopologyImpl`.
`getItem` during a range DO's migration reads-from-parent via the existing `getItemDirect` path
(parent = whatever `__parent_partition_context` says — the hash partition for the root, the parent
range DO for a split child).

`getItemDirect` must read **strictly local**. It is the parent-read primitive the range root uses
during migration; it must not consult `promotedKeys` or apply any forwarding, or the range root
reading from its parent would be bounced straight back to itself (`root → P.getItemDirect → forward
→ root → …`). Only `withSplitForwarding` gains the promoted-keys branch.

### Transactions — `groupItemsByRouting` + forward-and-aggregate

`prepare`/`commit`/`cancel`/`readForTransaction` each follow the same shape (invariant 3). The
existing `local.length === 0` guard (which forbade mixing local and forwarded work) is **replaced** by
a placeability guard, and the handler now executes local work _and_ forwards in the same call:

```
const { local, forwards, unplaceable } = groupItemsByRouting(items)
invariant(unplaceable.length === 0, "fokos: mis-routed item this node can neither own nor route")

const results = []
for (const [childContext, childItems] of forwards):
    results.push( childStub(childContext).<op>(txnId, childItems, childContext) )   // forward
if local.length > 0:
    results.push( handleLocal<Op>(txnId, local) )                                   // own slice
return aggregate(await settle(results))
```

`groupItemsByRouting` produces forward groups for: hash children (existing), a promoted key
(`status` ∈ `promoting`/`promoted` → `rangeRoot(hashKey)`), and range children (sort key outside own
`[start, end)` → the owning child from `split_status.childPartitionContexts`). An item that is
neither locally owned nor routable to any of those is `unplaceable` (a routing bug) and the operation
rejects.

Aggregation:

- `prepare`: all `accepted` → `accepted`; any `rejected` → `rejected`; any RPC/transient failure →
  propagate as failure (the coordinator retries or cancels).
- `commit` / `cancel`: aggregate the acknowledgements; idempotent, so a coordinator re-drive
  re-forwards safely.
- `readForTransaction`: aggregate the returned items.

Forwarding can be multi-level (hash partition → range root → range leaf); each level groups,
forwards, and aggregates, so the result bubbles up to the top the coordinator contacted.

---

## Transaction coordinator (`do-transaction-coordinator.ts`)

**No changes are required.** The coordinator groups items by its (possibly stale) view's `doName`,
contacts those partitions ("tops"), runs 2PC, and on commit/cancel iterates the same participant set
— exactly as it already does for hash splits, where a split parent forwards internally. Because every
partition now forwards-and-aggregates (invariant 3), all fan-out below a top is internal and the
coordinator never needs to learn the leaves.

Orphan locks from a partial-prepare failure are cleaned by the normal cancel path: when a top returns
`rejected`, the coordinator goes `CANCELLING` and sends `cancel` to that top, which fans `cancel` out
the same way it fanned `prepare` — releasing its own local slice and every child that locked. No leaf
is ever stranded and no leaf-initiated recovery is needed; the coordinator drives the tops, the tops
drive the leaves, for prepare, commit, and cancel alike.

> Cost accepted for this version: each prepare/commit/cancel traverses the forward chain (hash depth
>
> - range depth). The deferred topology cache will let the client pre-route via
>   `pickPartition(hashKey, sortKey)` so most traversals collapse to a direct call.

---

## Promotion lifecycle (hash partition → range structure)

### Detection (`shouldSplit` / a background job on hash partitions only)

Runs only on a **multi-key, non-router** hash partition: `!pCtx.rangePartition` (not a range DO) and
`splitStatus()` is not `split_started`/`split_completed` (a hash partition that has hash-split is a
router; its items belong to children and promoting them would orphan data). Run at most every 30s
(in-memory timestamp), not on every write; under real `databaseSize` pressure run promptly rather
than waiting the full interval.

```sql
SELECT hk, SUM(LENGTH(CAST(data AS BLOB)) + LENGTH(sk) + 80) AS est_bytes
FROM items GROUP BY hk
HAVING est_bytes >= ?     -- RANGE_PROMOTION_FRACTION * hashSplitConditions.maxSizeMb * 1024 * 1024
ORDER BY est_bytes DESC LIMIT 5
```

`RANGE_PROMOTION_FRACTION` is a hardcoded constant (`0.5`) — an internal heuristic, not a
`PartitionContext` field. The per-key estimate is used only to **rank** which key to promote; the
decision to act at all is gated on real `databaseSize` pressure. For each chosen key with no
`promoted_keys` row and no active/queued hash split, insert it as `queued` (the range root does not
exist yet; see invariant 8):

```sql
INSERT OR IGNORE INTO promoted_keys (hash_key, status, created_at, updated_at)
VALUES (?, 'queued', ?, ?)
```

### `startPromotion(hashKey)` on `PartitionDO`

The order is load-bearing (invariant 8): initialize the range root first, cut over only when the key
is lock-free, migrate last. Each step is idempotent and retried across background cycles.

```
Precondition: promoted_keys[hashKey] == 'queued' and no active/queued hash split.

A. Build identity:
     name  = rangeRoot(hashKey); doId = idFromName(name).
     pid   = PartitionIdHelper.fromRangePartition(base, hashKey, null).encode(true).
     newCtx = { ...pCtx, doName: name, primaryDoIdStr: doId, partitionId: pid.opaque,
                rangePartition: { hashKey, startBoundary: null } }.

B. Initialize the root (NO forwarding yet — status is still 'queued', so P serves the key locally):
     child.initFromSplit({ parentPartitionContext: pCtx, newPartitionContext: newCtx,
                           splitType: "range", rangeEndBoundary: null });   // retry ≤ 5, idempotent

C. Cutover (queued → promoting) in ONE transactionSync on P:
     if EXISTS (SELECT 1 FROM pending_transactions WHERE hk = hashKey):
         leave status 'queued' and retry the cutover next cycle (the root stays initialized & idle).
     else:
         UPDATE promoted_keys SET status='promoting' WHERE hash_key=hashKey;  (+ cache)
   From this instant P forwards the key; no new locks for it can land on P.

D. child.triggerMigration();  // fire-and-forget — now safe: the root is initialized and P forwards.
```

If forwarding began before step B, a forwarded `putItem` would hit a root with no
`__partition_context` and (via `ensurePartitionContext`) initialize it as an ordinary empty partition
that never migrates P's data — data loss. If cutover did not check for pending locks, a transaction
that already locked the key on P could later have its `commit` forwarded to the root, which holds no
such lock. The lock-free check makes promotion mutually exclusive with in-flight transactions on that
key.

The range root's `initFromSplit` writes `__partition_context` (with `rangePartition`),
`__parent_partition_context = pCtx` (the hash partition), `__parent_split_type = "range"`,
`__range_end_boundary = null`, and `__split_migration_status = "migration_initialized"`.

Starvation note: if the key is perpetually locked, the cutover (step C) defers indefinitely. Locks
are short-lived and the cutover retries each background cycle, so a gap normally appears quickly; emit
a metric/log if a key stays `queued` beyond a threshold so a drain mode can be added later.

Because of the lock-free precondition there are no pending transaction rows for the key at cutover,
and none can be created afterward (P forwards them). So promotion migration only carries committed
`items` plus the deletion watermark; it does not migrate `pending_transactions`.

### Migration + completion

Reuses `runMigration` (see Migration). On completion the range root calls a new RPC on the hash
partition:

```typescript
async acknowledgePromotionComplete(hashKey: string): Promise<void>
```

which transactionally sets `promoted_keys[hashKey] = 'promoted'` (and the cache). A background GC job
on the hash partition then deletes the promoted key's local rows:

```sql
DELETE FROM items WHERE hk = ?;                          -- paged
DELETE FROM pending_transactions WHERE hk = ?;           -- expected empty (see lock-free cutover)
```

After `promoting`, all writes for the key are forwarded away, so no new local rows for the key
appear; the delete is unconditionally safe.

### Inheritance on hash split

When a hash partition hash-splits, child hash partitions must inherit `promoted_keys` rows for the
keys they take ownership of. New parent RPC:

```typescript
async getPromotedKeysBatch(opts: {
  childPartitionContext: PartitionContextResolved;
  cursor: { hashKey: string } | null;
}): Promise<{ rows: { hash_key: string; status: string }[]; nextCursor: { hashKey: string } | null }>
```

The parent pages `promoted_keys ORDER BY hash_key`, filtering with
`makeIsCorrectChildHashPartition(parentCtx, childCtx)(row.hash_key)`. The child inserts received rows
into its own `promoted_keys` and cache. Called in `runMigration` (hash-child path) after the items
loop. Only the _set_ transfers; the range-root name is recomputable and the range structure is
autonomous post-promotion (the stale `__parent_partition_context` it holds is harmless).

**Hash-split item migration excludes promoted keys.** Mutual exclusion guarantees that at hash-split
time every promoted key is `promoted` (its data lives in the range structure; the hash partition's
copy is GC'd or being GC'd). The hash-child item filter must therefore exclude promoted keys —
`... AND hk NOT IN (SELECT hash_key FROM promoted_keys)` — so a residual row is never copied to a
hash child that would then hold both a forward-pointer and local data it must not serve. The child
inherits the _entry_, never the data.

---

## Range-split lifecycle (range DO → range DOs)

`RangePartitionTopologyImpl.startSplit()` runs inside the range DO when `split_status` is
`split_queued` with `splitType: "range"` (the existing background split job already calls
`startSplit()` — `runBackgroundWork` lines 1266–1280, unchanged):

```
1. Capture parentEndAtSplit = current __range_end_boundary (null for an unbounded node).
2. Compute N-1 boundaries B1..B_{N-1} (N = rangeSplitN) within the node's CURRENT owned slice
   [start, end) in ONE transactionSync snapshot (no inter-query drift):
     SELECT sk FROM items WHERE sk >= start AND (end IS NULL OR sk < end)
       ORDER BY sk LIMIT 1 OFFSET (cnt * i / N)   for i in 1..N-1
   Guard: if cnt < minItemsToSplit or any two boundaries coincide, abort/redo later.
   (Require minItemsToSplit >= rangeSplitN so the N-1 offsets are distinct and no new child is empty.)
3. Create the N-1 NEW right-sibling DOs (self is never a child):
     idx i (1..N-1): startBoundary = B_i, end = B_{i+1} (or parentEndAtSplit for the last).
   Each: initFromSplit({ parent = self, newCtx with rangePartition.startBoundary = B_i,
                         splitType: "range", rangeEndBoundary = B_{i+1} | parentEndAtSplit }). retry ≤ 5
4. Shrink own end: __range_end_boundary = B1.  (Mutable local state; identity/name unchanged.)
5. Persist split_status = split_started:
     - childPartitionContexts: APPEND the N-1 new children to any existing list (accumulate).
     - migratedChildDoNames: keep existing acked children; the new children are pending.
     - parentEndAtSplit: this round's value (from step 1).
6. triggerMigration() on the N-1 new children (fire-and-forget). Self keeps its left slice and does
   not migrate.
```

**Re-split accumulation.** Because the node retains its leftmost slice, that slice can grow and the
node will split again later. A later split appends its new children to `childPartitionContexts`
rather than replacing it; the previously-split children are untouched and remain in the routing set.
Routing (`pickChildPartition`, "largest `startBoundary ≤ sk`") then continues to work over the union,
and the node owns `[start, smallestChildStart)`. The split state machine must allow re-entering
`split_queued`/`split_started` from `split_completed` without dropping prior children, and
`migratedChildDoNames` accumulates alongside `childPartitionContexts`.

Completion uses the existing `acknowledgeChildMigration` over `split_status.childPartitionContexts`:
self is never in the list, so the current round completes exactly when all its new children have
acked (previously-split children are already in `migratedChildDoNames`). `cancel` iterating
`childPartitionContexts` therefore never self-RPCs.

Cleanup: after a round reaches `split_completed`, a background job deletes the migrated-away rows from
the retained node: `DELETE FROM items WHERE sk >= ?` (own `__range_end_boundary`), paged; likewise
`pending_transactions` (these were migrated to the new children, so commit/cancel forward to them).
Safe because once `split_started`, `shouldAllow` forwards all `sk >= end`.

---

## Migration changes (`do-partition.ts`)

### `getItemsBatch` — branch on the requesting child

```
if (childPartitionContext.rangePartition) {
  const hk = childPartitionContext.rangePartition.hashKey;
  if (this is a hash partition serving a PROMOTION) {
     // authorize via promoted_keys[hk] == 'promoting'; the range is unbounded
     filter: WHERE hk = ? AND <cursor>  ORDER BY hk, sk LIMIT ?
  } else {
     // this is a range DO range-splitting; derive the child's [start, end):
     //   start = childPartitionContext.rangePartition.startBoundary
     //   end   = the next sibling's startBoundary in split_status.childPartitionContexts,
     //           or split_status.parentEndAtSplit if this is the rightmost child of the round
     const [start, end] = rangeOf(childPartitionContext, this.splitStatus());
     filter: WHERE hk = ? AND sk >= start AND (end IS NULL OR sk < end) AND <cursor> ORDER BY hk, sk LIMIT ?
  }
} else {
  // hash-child migration, excluding promoted keys (their data belongs to range structures):
  filter: <existing hash predicate> AND hk NOT IN (SELECT hash_key FROM promoted_keys) AND <cursor>
}
```

Authorization: the promotion path checks `promoted_keys`; the range-split path checks the child is in
`split_status.childPartitionContexts` (the existing `isKnownChild` check, generalized).

`getPartitionTransactionMetadata` branches identically: hk-equality for promotion (expected empty),
hk + sk-range for range split, the hash predicate otherwise.

### `makeIsCorrectChildRangePartition(childCtx, [start, end])`

```typescript
(hashKey: string, sortKey?: string) =>
	hashKey === childCtx.rangePartition!.hashKey && (start == null || (sortKey ?? "") >= start) && (end == null || (sortKey ?? "") < end);
```

### `runMigration` — branch on `__parent_split_type` + own `rangePartition`

```
splitType = kv.get(PARENT_SPLIT_TYPE)
if splitType === "hash":  runHashChildMigration()    // existing + getPromotedKeysBatch step
else if splitType === "range":
   runRangeChildMigration()  // items filtered by own range (unbounded for the promotion root)
                             // + pending tx for a range-split child (none for a promotion root)
                             // completion: promotion root → acknowledgePromotionComplete(hk)
                             //             range-split child → acknowledgeChildMigrationComplete(doName)
```

The promotion-root vs range-split-child distinction at completion: if
`__parent_partition_context.rangePartition` is set, the parent is a range DO → call
`acknowledgeChildMigrationComplete`; otherwise the parent is a hash partition (promotion) → call
`acknowledgePromotionComplete`.

---

## `initFromSplit` / `ensurePartitionContext` changes

- `initFromSplit`: when `newPartitionContext.rangePartition` is set, also persist
  `__range_end_boundary = opts.rangeEndBoundary ?? null`. The idempotency check extends to compare
  `rangePartition.hashKey` and `startBoundary` (immutable). The existing conflicting-options guard
  applies.
- `ensurePartitionContext`: validate `rangePartition.hashKey`, `rangePartition.startBoundary`, and
  the range `partitionId` as immutable. Never read or validate an end boundary from the incoming
  context (there is none). All other fields keep their current immutable/mutable validation.

---

## Mutual exclusion (hash split ⇄ promotion)

On a hash partition:

- Before queuing or advancing a promotion: require `splitStatus()` is undefined or `split_completed`.
  If a hash split is `split_queued`/`split_started`, skip promotion this cycle (and a router never
  promotes — see Detection).
- Before queuing a hash split (`shouldSplit`): require **no `promoted_keys` row is `queued` or
  `promoting`** (only `promoted` may coexist with a hash split — it is a pure forward-pointer with no
  local data). If any promotion is in flight, skip the hash split this cycle; it is re-evaluated on
  the next background run.

`queued` must also block a hash split: a `queued`/`promoting` key whose owning partition split would
leave the range root acking a parent that has become a router, stranding the key's status. Forcing all
in-flight promotions to reach `promoted` before a split removes that case — by then
`acknowledgePromotionComplete` has already landed on the still-unsplit partition.

Both checks are cheap and live in the respective background jobs. Because promotion migration is lazy
but bounded, the window where hash splits are deferred is short.

---

## Background jobs (`runBackgroundWork`)

Hash partition only:

- **Detect + queue promotions** (≤ every 30s; the heavy-key query above; mutual-exclusion gated;
  skipped on a router). Inserts rows as `queued`.
- **Drive queued promotions**: for each `queued` row, call `startPromotion(hashKey)` — idempotent;
  performs init → lock-free cutover (may keep it `queued`) → migration. Re-running each cycle retries
  a deferred cutover.
- **GC promoted keys**: for `promoted` rows with residual local rows, paged delete (items + pending).

Range DO only:

- **Start queued range split**: the existing job (`splitStatus()?.status === "split_queued" →
startSplit()`) now handles `splitType: "range"`, including re-splits of the retained slice.
- **GC right-sibling rows**: after a round's `split_completed`, paged
  `DELETE … WHERE sk >= __range_end_boundary`.

Alarm scheduling extends the existing `wantAlarm` accounting for these states.

---

## New / changed RPCs

| RPC                                                    | Where             | Purpose                                                           |
| ------------------------------------------------------ | ----------------- | ----------------------------------------------------------------- |
| `acknowledgePromotionComplete(hashKey)`                | Hash partition DO | Range root signals promotion migration done.                      |
| `getPromotedKeysBatch(opts)`                           | Hash partition DO | Paginated `promoted_keys` for hash-split inheritance.             |
| `getItemsBatch` / `getPartitionTransactionMetadata`    | Partition DO      | Branch for range-structure children.                              |
| `acknowledgeChildMigrationComplete`                    | Partition DO      | Reused unchanged for range-split children.                        |
| `getItemDirect`                                        | Partition DO      | Reused unchanged for range DO reads-from-parent during migration. |
| `prepare` / `commit` / `cancel` / `readForTransaction` | Partition DO      | Now forward-and-aggregate (own local slice + forward child sets). |

---

## Destroy traversal (`db.ts`)

`destroy` already recurses `splitStatus.childPartitionContexts` (covering range-split sub-trees, which
use the same field). Extend the per-hash-partition step to also enumerate `promoted_keys` and, for
each, destroy `rangeRoot(hashKey)` (which recurses its own `childPartitionContexts`). Order: destroy
range structures before the hash partition that links them.

A `promoted` entry is inherited by hash children, so the same `hashKey` (hence the same global
`rangeRoot(hashKey)`) may be enumerated from multiple hash partitions. Dedupe range-structure
teardown by `hashKey` and tolerate an already-destroyed root (idempotent / catch the destroyed
sentinel) so a second visit is a no-op.

---

## Implementation phases

Each phase is independently testable; behavior is gated so earlier phases do not regress hash splits.

**Phase 1 — Types + migration (no behavior change)**

- `rangePartition?` on `PartitionContextResolved`; `PartitionIdHelper` range schema; `rangeEndBoundary?`
  on `InitFromSplitOptions`.
- `__range_end_boundary` KV key; SQL migration M3 (`promoted_keys`).
- `rangePartitionDoName` + `encodeRangeComponent`.
- Tests: migration runs clean; range/hash `partitionId` round-trips; no hash-split regressions.

**Phase 2 — RangePartitionTopologyImpl + `ensureTopology` selection**

- Implement `RangePartitionTopologyImpl` (`shouldAllow` by `[start,end)`, `pickChildPartition` by
  sibling boundaries over the accumulated child set, `maybeQueueSplit` on `rangeSplitConditions`).
- `ensureTopology` selects by `pCtx.rangePartition`.
- Tests: a hand-constructed range DO serves/forwards by sort key; queues a range split.

**Phase 3 — Promotion: detection, routing, migration**

- promoted-keys cache (`queued`/`promoting`/`promoted`, loaded in `blockConcurrencyWhile`); detection
  (inserts `queued`, router-guarded) + `startPromotion` (init → lock-free cutover → migrate).
- `withSplitForwarding` consults the cache; single-key forward only on `promoting`/`promoted`.
  `getItemDirect` stays strictly local.
- `getItemsBatch`/`getPartitionTransactionMetadata` promotion branch; `runRangeChildMigration`
  (items + deletion watermark, no pending_transactions); `acknowledgePromotionComplete`; GC job.
- Mutual exclusion checks.
- Tests: heavy key → `queued` → cutover deferred while a lock is held, then proceeds → migrate →
  `promoted` → GC; a write during `queued` stays local; single-key ops route correctly after cutover;
  reads during migration via parent; a write racing initialization never orphans data; prepare-lock
  then promote then commit does not corrupt.

**Phase 4 — Transactions: forward-and-aggregate**

- Replace the `local.length === 0` guard in `prepare`/`commit`/`cancel`/`readForTransaction` with the
  placeability guard; each handler executes its local slice and forwards child sets, then aggregates.
- `groupItemsByRouting` produces forward groups for hash children, promoted keys, and range children.
- Tests: a transaction spanning a promoted key + a local key on the same partition commits;
  a transaction spanning two range leaves commits; a partial-prepare rejection releases all locks via
  the coordinator's cancel fan-out; commit/cancel are idempotent under coordinator re-drive.

**Phase 5 — Range split**

- `RangePartitionTopologyImpl.startSplit` (boundary computation in one snapshot over the current
  slice, min-size guard, retain-left/migrate-right, children appended to `split_status`).
- Re-split accumulation: re-entering split from `split_completed` appends children and keeps prior
  ones; routing over the union.
- `runRangeChildMigration` range-split path; cleanup job.
- Tests: full N=4 range split; a second split of the retained node keeps the first split's children
  reachable; availability (sk below the smallest boundary always available; new children 503 while
  migrating); transactions across new leaves commit.

**Phase 6 — Hash-split inheritance + destroy**

- `getPromotedKeysBatch`; hash-child migration inherits promoted-key entries and excludes promoted
  keys' items.
- `destroy` enumerates `promoted_keys` → range roots, deduped by `hashKey`, tolerating
  already-destroyed.
- Tests: hash split inherits promoted-key entries but not their data; destroy cleans up all range DOs
  once each.

---

## Design decisions

- **Transaction routing — forward-and-aggregate, not coordinator re-routing.** A partition owns some
  items and forwards the rest in the same `prepare`/`commit`/`cancel`, aggregating the result, so the
  coordinator only ever talks to the partitions it originally contacted. This mirrors the existing
  hash-split forwarding (generalized to mixed owner/router nodes and to promotion cross-links and
  range children) and requires no coordinator changes. Orphan locks are released by the coordinator's
  normal cancel fan-out; no leaf-initiated recovery is needed.
- **Range split retains the leftmost slice.** The splitting DO keeps `[start, B1)` and creates N-1
  new right-sibling children, so the retained left data never migrates. The node is therefore a mixed
  owner/router, and it accumulates children across successive re-splits.
- **Range-split child tracking excludes self.** `split_status.childPartitionContexts` holds only the
  new children; completion fires when the current round's new children ack; `cancel` never self-RPCs;
  `parentEndAtSplit` (per round) gives the rightmost new child its upper bound.
- **Three-state promotion with a lock-free cutover.** `queued → promoting → promoted`. The range root
  is initialized while still `queued` (no forwarding); the `queued → promoting` cutover is one
  `transactionSync` that requires the key to have no pending locks. This avoids initializing an empty
  root and avoids stranding a lock when commit routing follows the promotion, and lets migration skip
  `pending_transactions` entirely.
- **Promotion threshold** is a hardcoded constant `RANGE_PROMOTION_FRACTION = 0.5`, not a
  `PartitionContext` field; the per-key estimate ranks candidates while real `databaseSize` gates the
  action.
- **Boundary computation** uses exact count-quantiles (`ORDER BY sk LIMIT 1 OFFSET (cnt*i/N)`, cheap
  on the `(hk, sk)` PK index) in one `transactionSync` snapshot, with a min-size guard.
- **DO name length** is a non-issue (the runtime hashes names); always route via the context's
  `doName`, never `this.ctx.id.name`. Range names use a `.r.` namespace marker to stay disjoint from
  hash names.
- **Range `partitionId`** is a real `SCHEMA_RANGE` `PartitionIdHelper` value encoding
  `(hashKey, startBoundary)`, validated as immutable like a hash id; `doName` derives from it.
- **Mutual exclusion** blocks a hash split while any promoted key is `queued` or `promoting` (only
  `promoted` coexists); routers never run promotion detection.
- **`getItemDirect` stays strictly local** so range-root read-from-parent cannot loop.
- **Hash-split migration excludes promoted keys**, inheriting the forward-pointer entry, never the
  data.

---

## Deferred (future version, separate DO group)

- Boundary directory + LOUDS topology cache → caller-side direct routing (removes most traversals).
- Bloom filter caller hint + false-positive bounce (a range DO returns "not ranged" when it lacks
  `__partition_context`; the caller falls back to the hash partition).
- Uniform GC for router/parent dead-weight across the hash and range tiers.
- Eager child-cancel on partial-prepare rejection (latency optimization; the coordinator's cancel
  fan-out already guarantees correctness).
