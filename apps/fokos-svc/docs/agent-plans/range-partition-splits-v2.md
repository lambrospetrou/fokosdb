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
function rangePartitionDoName(
	databaseName: string,
	hashKey: string,
	startBoundary: string | null,
): string {
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
	hashKey === childCtx.rangePartition!.hashKey &&
	(start == null || (sortKey ?? "") >= start) &&
	(end == null || (sortKey ?? "") < end);
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

---

## Revision 1 — End-boundary naming and pure-router range splits

**Status.** This revision **supersedes** the conflicting parts of: Invariants 1–2, Naming,
`__range_end_boundary` (mutable state), `RangePartitionTopologyImpl` behavior, the entire
**Range-split lifecycle** (retain-leftmost / re-split accumulation / `parentEndAtSplit`), and appendix
#5. Phases 1–4 were implemented against the original scheme and must be revised per the checklist at
the end of this section. Phases 5–6 are authored directly against this revision.

### Motivation

The original design retains the leftmost slice on a range split: the parent keeps `[start, B1)`,
spawns N−1 right siblings, and shrinks its own `end`. That forces **re-split accumulation** — the
retained slice grows and the same DO splits again, so `childPartitionContexts` /
`migratedChildDoNames` accumulate and the split state machine must re-enter `split_queued` from
`split_completed`. Appendix #5 rejected the cleaner **pure-router** split (parent becomes a pure
router with N new children, including a fresh leftmost) because the leftmost child would share the
parent's start boundary → same DO name → `idFromName` collision, claiming it "requires epoch/opaque
names." That objection is defeated by **putting the end boundary in the name too.**

### The change

A range DO's name and identity encode **both** boundaries: `db.r.<enc(hk)>.<start>.<end>`. On a split
the node becomes a **pure router** over its fixed `[start, end)` and creates **N** children that tile
the whole range — including a brand-new leftmost child `[start, B1)`, whose name differs from the
parent's because its `end` (`B1`) differs. This restores exact symmetry with hash splits: **once a
node splits it is purely a router; a leaf is never also a router** (no retain-leftmost, no re-split
accumulation, no `parentEndAtSplit`).

**Uniform ±∞ sentinels — name space only, not comparison space.** Boundaries live in two spaces with
different requirements, and conflating them is what makes sentinels seem hard:

- **Comparison space** (`shouldAllow`, `pickChildPartition`, the SQL `sk >= ? AND sk < ?` filters)
  needs _ordering_. Lexicographic order over unbounded strings has a **minimum but no maximum**: `""`
  precedes every string (so `−∞` is just `""` and `sk >= ""` is trivially true), but for any candidate
  max `s`, `s + "x"` sorts after it — **no string is greater than all strings.** So an ordered `+∞`
  sentinel is _impossible_, and the upper edge must be handled by branching regardless. The
  representation here is therefore `string | null` where **`null` = unbounded** (lower `null` ⇒ treat
  as `""`; upper `null` ⇒ omit the `< end` predicate, i.e. `end === null || sk < end`). This orders
  correctly with no magic value, and "leftmost child sorts first" falls out of `start ?? ""`.
- **Name space** (the DO name and `partitionId`) needs only _unambiguous identity_; ordering is
  irrelevant. A sentinel token is materialized **only** in `rangePartitionDoName` (to avoid the empty
  component `db.r.hk..~max`) and mapped back to `null` in `decode`. No comparison ever sees the token.

The name tokens are **reserved-char, collision-proof by construction**: `encodeRangeComponent` only
ever emits `[A-Za-z0-9_-]` and `%XX`, so a token containing `~` (e.g. `~min` / `~max`) can never equal
an encoded real boundary — `encodeRangeComponent` escapes a literal `~` to `%7E`. **No "exclude the
sentinel from valid sk" validation is needed** — that is precisely why `~min`/`~max` beat a
word-character token like `___fokos_range_infinite_minus`, which passes through the encoder unescaped
and _would_ collide with a real `sk` of that literal. The only special-case is the unavoidable
`null ↔ token` mapping at name encode/decode. In `partitionId`, `null` is encoded as a _flag bit
absent_ (`bit0 = hasStart`, `bit1 = hasEnd`), never as a token. Every DO — including the range root
`db.r.<hk>.~min.~max` — thus has the identical three-component shape and the root stays addressable
from `hashKey` alone with no cached boundaries.

### Why it is collision-free for all time

The intervals that ever exist form a laminar family (recursive subdivision, no merges, DOs persist).
If two distinct parents both produced a child `[a, c)`, laminarity forces them nested, and following
the nesting forces one to equal `[a, c)` and hold itself as a child — impossible. So
`(hashKey, start, end)` is a permanent unique identity; appendix #5's "needs epoch names" was too
pessimistic.

### Invariants 1–2, revised

- **Invariant 1 (revised).** _Both_ boundaries are immutable identity. A range DO's `[start, end)` is
  fixed for life — part of its name and validated `partitionId` / `rangePartition`. It never shrinks;
  on split it becomes a router and its children own the sub-ranges. **`__range_end_boundary` (mutable
  KV) is removed**; `end` is read from the validated context. There is no self-mutating state to
  guard, and `InitFromSplitOptions.rangeEndBoundary` is removed (end comes from
  `newPartitionContext.rangePartition.endBoundary`).
- **Invariant 2 (revised).** The **N** children tile the parent exactly: `start[0] = parent.start`,
  `end[i] = start[i+1]`, `end[N-1] = parent.end`. Each child's `end` is explicit in its own identity
  (it still equals the next sibling's start, so the leaf boundary _set_ reconstructs all leaves — see
  the routing-cache note).

### Routing-cache correctness (why this was non-trivial)

Moving `end` into the name makes addressable identity a function of **two** boundaries. A _flat,
partial_ boundary cache can then pair a real `start` with a real `end` that were **never adjacent in
the tree** (they straddle an un-cached higher split), fabricating the name of a DO that never existed
→ an uninitialized DO. The original start-only naming was robust to any stale subset because the name
was a function of the immutable `start` alone.

This version stays correct **without any cache** (root-down traversal uses each router's stored
`childPartitionContexts` and never fabricates names). For the **deferred** routing cache, end-in-name
imposes constraints that are now load-bearing:

1. **The cache must be a descendable tree, not a flat boundary list.** Route by descending from the
   root through known routers to the deepest cached node covering `sk`; never binary-search a flat
   boundary set. Then non-adjacent boundaries are never paired.
2. **Ancestor-ordered propagation.** A split is published to a cache only after all its ancestor
   splits are present (the keeper buffers out-of-order notifications). Eviction must be subtree-aware
   (never drop an interior router while keeping a descendant).
3. **Phantom-bounce guard — the correctness backstop (added now).** A `.r.` DO with no stored
   `__partition_context` **must never lazy-init from a request**; it bounces the caller, which falls
   back to the range root and traverses. This makes _any_ stale/lossy/cold cache safe, so (1) and (2)
   are pure hit-rate optimizations. No false positives: every real range DO is born through
   `initFromSplit` (promotion creates the root; a split creates children), so "has
   `__partition_context`" cleanly separates real from phantom. In this version the guard should never
   fire (traversal-only), but it is the piece the deferred soft cache rests on.
4. **Soft, non-authoritative keeper.** A split completes on the node's local `split_status`
   regardless of the keeper; the notification `(parentDoName, nodeDoName, childDoNames)` is
   fire-and-forget, idempotent, and self-healing (the keeper re-derives by enumerating the tree). A
   down/lagging keeper costs only hit-rate, never correctness or scaling availability (stays on
   appendix #3, not the rejected central authority of #2). Boundaries derive from the child names (the
   names are the single source of truth), so the cache can never record a boundary that disagrees with
   a name.

### Cost (accepted)

Every split now migrates the **leftmost slice too** (into the new `[start, B1)` DO) and leaves the old
DO as a dead router. The leftmost slice therefore loses the original zero-downtime property — it gets
the same 503-while-migrating window as every other child. This is the cost appendix #5 already called
"otherwise acceptable," and it matches the hash tier's dead-router behavior. In exchange: no re-split
accumulation, uniform leaf-xor-router semantics, and simpler migration filters (each child's
`[start, end)` is explicit; no `parentEndAtSplit`).

### Implementation revision checklist

**Phase 1 (revise — `partition-topology.ts`, `do-partition.ts`).**

- `rangePartition: { hashKey, startBoundary, endBoundary }` — all immutable, each
  `string | null` where **`null` = unbounded edge** (the comparison-space representation; see the
  ±∞-sentinels note). Routing keeps branching on `null`; it never compares against a sentinel token.
- Add reserved-char token constants `const RANGE_MIN = "~min", RANGE_MAX = "~max"` (collision-proof:
  `encodeRangeComponent` escapes a literal `~` to `%7E`, so a `~`-token can never equal an encoded
  real boundary — no "exclude from valid sk" validation needed).
- `rangePartitionDoName(db, hashKey, start, end)` → `db.r.<enc(hk)>.<startComp>.<endComp>`, where
  `startComp = start === null ? RANGE_MIN : enc(start)` and likewise `endComp` with `RANGE_MAX`.
- `PartitionIdHelper.fromRangePartition(base, hashKey, start, end)`: SCHEMA_RANGE_V1 wire format gains
  a `startLen` field and the trailing `endBoundary` bytes; **`null` is encoded as a flag bit absent**
  (`bit0 = hasStart`, `bit1 = hasEnd`), never as a token. `decode` → `{ hashKey, startBoundary,
endBoundary }` (component `=== RANGE_MIN/MAX` ⇒ `null` only when decoding from a name); `doName`
  passes both through `rangePartitionDoName`.
- `resolveRangePartitionContext(base, hashKey, start, end)`.
- **Remove** `__range_end_boundary` KV key and `InitFromSplitOptions.rangeEndBoundary`; `initFromSplit`
  reads `end` from `newPartitionContext.rangePartition.endBoundary`; `ensurePartitionContext`
  validates `start` **and** `end` as immutable.
- Update every `resolveRangePartitionContext(..., null)` root call site (`do-partition.ts` ~910, 1110,
  1160, 1495) to pass `(null, null)` for the root `[−∞, +∞)` — which renders to the `~min.~max` name.

**Phase 2 (revise — `RangePartitionTopologyImpl`).**

- `shouldAllow`: read `end` from the context, not KV. If split → **always `"forward"`** (a router owns
  nothing); delete the leftmost-local branch. Not-split + in-range → `ok` / size-`reject`;
  out-of-range → `reject`.
- `pickChildPartition`: same rule ("largest `start ≤ sk`"), but children now tile the whole range and
  there are N; drop the "accumulates across re-splits" comment.
- `acknowledgeChildMigration`: `childPartitionContexts` is set once (N children) and never accumulates;
  remove the re-entry-from-`split_completed` allowance.

**Phase 3 (revise — promotion).** Range root identity = `(null, null)` (the `~min.~max` name); drop "keeps leftmost" language;
`initFromSplit` reads `end` from context. The init → lock-free cutover → migrate flow is otherwise
unchanged.

**Phase 4 (verify — transactions).** A range node is now pure leaf **xor** pure router, so
`groupItemsByRouting` on a range node is "all local" or "all forwarded," never mixed. Confirm no path
assumes a splitting range node still owns a leftmost slice. The hash partition stays mixed (local +
promoted-key forwards + hash-child forwards).

**Phase 5 (author against this revision).** `startSplit` creates **N** children including the new
leftmost; the node becomes a pure router (no end-shrink); `childPartitionContexts` set once; migrate
all N; after `split_completed` GC **all** rows (everything migrated), paged. Each child's migration
filter derives from its explicit `[start, end)`; no `parentEndAtSplit`. Guard `minItemsToSplit ≥ N`.

**Phase 6.** `rangeRoot(hashKey)` resolves to the root `(null, null)` / `~min.~max` name in destroy enumeration; dedupe by
`hashKey`; inheritance unchanged.

**New.** Add the phantom-bounce guard in the range-DO request entry path (`ensurePartitionContext` /
`withSplitForwarding`).

---

## Appendix: architecture brainstorm — is there a fundamentally better design?

A final stress-test of the architecture, asking whether some other design achieves the same end goal
(bottomless storage, hash key as the main driver, some keys with arbitrarily many sort keys,
correctness, low client memory, fast enough) better or more simply. The conclusion: the two-tier
shape is the right call for this workload; the leverage is in the deferred range-tier routing layer.

> **Requirement confirmed: ordered sort-key range scans ARE needed.** This rules out the "drop order,
> sub-split a hot key by `hash(sortKey)`" option below as a general replacement — range structures
> must stay order-preserving. (Without this requirement we would just keep the hash-split approach.)

### The fundamental tension (a tradeoff triangle for routing metadata)

For the routing metadata you can have at most two of three cleanly:

1. **Succinct** — client holds ~O(nodes) bits, no central service.
2. **Order-preserving + data-driven balance** — boundaries chosen to split load evenly and support
   sort-key range scans.
3. **Zero extra hops** — resolve the owner without asking anyone.

The **hash tier gets all three for free**, because its boundaries are _implicit_
(`hash(hashKey, depth) % N`): nothing stored, perfectly succinct (1 bit/node = LOUDS), zero hops.
That is why it is the right primitive.

The **range tier cannot get all three**, because its boundaries are _data-dependent_ — arbitrary
sort-key values that must physically live somewhere. There are exactly three places to put them, and
the choice _is_ the architecture:

- **In the client** → memory cost (the boundary directory).
- **In a DO you ask** → hop cost (an index/router DO).
- **Nowhere; discover by walking** → latency cost (traversal — what this plan does now).

v2 chooses "traversal now, client-cache later." The real question is therefore only: _where should
range boundaries live, and how should they be encoded?_

### Alternatives that are genuinely worse (and why)

- **Single uniform range tree over `(hashKey, sortKey)`** (Bigtable tablets / Cockroach ranges):
  uniform, no promotion, but pays heavy non-succinct boundary metadata for _every_ key to serve the
  _rare_ hot key, and loses hash scatter (sequential/write-skewed hash keys hotspot). Wrong tradeoff
  for "most keys have few sort keys."
- **Single order-preserving radix trie over the whole key** (succinct and uniform): a trie is
  LOUDS-able, but it reintroduces the sequential-write hotspot that hashing solves (adjacent keys →
  same leaf) and spends order across hash keys that are never range-scanned. Net worse.
- **Central routing service** (Spanner-style placement driver): zero client memory, arbitrary
  boundaries, but a global bottleneck and an extra hop — the decentralized parent-as-router avoids
  exactly this.

These confirm the asymmetric two-tier design is well-matched to the workload skew.

### Alternatives that could be better without compromising

1. **Make the deferred boundary directory a _succinct ordered trie_ (FST / SuRF-style), not a list.**
   This gives all three triangle corners at once: data-driven balanced boundaries (kept) + succinct
   client routing (a Fast Succinct Trie of the boundary keys is O(bits)) + order-preserving range
   scans. It can also **subsume the bloom filter** — a succinct set of "ranged keys" answers "is this
   key ranged?" with no false positives. Cost: FST/SuRF are awkward to update incrementally; rebuild
   periodically with a small overlay of recent splits (topology changes slowly, so cheap).

2. **Centralize the metadata — a per-key boundary authority (a per-key index DO, or a flat index
   co-located on the range root). Rejected: single point of failure.** These two are the _same idea_
   at different granularity — a pure metadata DO, or the same boundary list co-located on the
   leftmost-owning data DO — so they are one option. What it would buy: re-split bookkeeping becomes a
   trivial flat-list insert, and _if routing is made authoritative_ (the coordinator resolves the
   exact leaf before sending) the forward-and-aggregate machinery becomes removable. What it does
   **not** change: the data plane is identical — splitting a leaf and migrating its rows (create leaf,
   lazy-pull, 503, ack, GC) is the same work, with the same races, in every topology. And it
   introduces a **single point of failure** — every split must publish its boundary to the authority,
   so if the authority is down, slow, or cannot start, splitting and scaling for that whole key stall.
   (Note: forward-and-aggregate is a consequence of _non-authoritative routing_, not of the tree — a
   cached succinct boundary set over the tree removes it just as well, with no central node.)

3. **The SPOF-free way to get those benefits: authoritative tree + a soft, rebuildable index cache.**
   Keep the self-routing tree as the source of truth — it is already SPOF-free, because each leaf
   knows its own range authoritatively (`start` is immutable identity in the name/`partitionId`; `end`
   is local and equals the next sibling's start), so the union of leaves _is_ the boundary set and no
   central node is needed to know it. Then layer an index (a per-key flat index now, the client-side
   FST later) on top as a **non-authoritative hint**, with the same contract as the bloom filter:
   - **Splits never depend on the index.** A leaf splits via its own local `split_status` and
     completes independently, then notifies the index best-effort. If that fails (index down), no
     harm — the new leaf exists and is reachable through the tree. Scaling is never blocked.
   - **The index is a hint, verified by the data plane.** Index up: ask it → leaf guess → contact it;
     if stale, the leaf forwards (tree fallback) and the response carries the correction, which
     updates the index. Index down: skip it, enter at the range root and traverse. Always correct,
     never blocked.
   - **The index rebuilds itself.** On cold start or after an outage it re-derives boundaries by
     enumerating the tree, and self-heals from forward-correction responses — pure derived state, like
     a search index over a database.
     Warm case: index-speed routing (a client holding the cache can skip the root and go straight to the
     owning leaf). Cold/down case: degrades to today's traversal. Correctness and split/scale
     availability never touch the index.

4. **(Ruled out by the confirmed requirement, recorded for completeness.)** If ordered sort-key range
   scans were _not_ needed for some table, a hot key could be sub-split by `hash(sortKey)`: balanced
   by construction, succinctly encodable (1 bit/node, no boundary directory ever), the hash tier's
   trick one level down. Not applicable here since ordered scans are required, but could be a
   per-table mode if any future table opts out of ordered scans.

5. **~~Rejected~~ → Adopted in Revision 1: pure-router range splits (uniform with hash splits).**
   _Superseded — see Revision 1. The collision objection below is real for **start-only** naming but is
   defeated by also encoding the **end** boundary in the name (`(hashKey, start, end)` is provably
   collision-free), so pure-router splits are adopted after all._ Making the splitting node a pure
   router with N new children (migrating the leftmost slice too, which is otherwise acceptable) would
   remove re-split accumulation. But it is **incompatible with start-boundary naming**: at every level
   the leftmost child shares the parent's start boundary, hence the same DO name, and `idFromName` is
   deterministic so two DOs cannot share a name. Retain-leftmost (the parent shrinks its `end` instead
   of spawning a same-named child) is therefore _forced_ by the naming. Pure-router would require
   epoch/opaque names, sacrificing names computable from `(hashKey, startBoundary)` (the directory/FST
   would have to store `boundary → name`). Not worth it; keep retain-leftmost and its mild re-split
   accumulation.

### Synthesis and the open fork

The architecture is sound, and the tree is the right substrate: distributed leaf identities make it
SPOF-free, and the start-boundary naming that forces retain-leftmost (see #5) is what keeps DO names
computable from `(hashKey, startBoundary)`. The leverage is concentrated in the deferred range-tier
_routing_ layer, and the fork is **where the authoritative boundary state lives**:

- **Recommended — authoritative tree + soft index cache (#3).** Keep the tree as the source of truth;
  add a rebuildable index (a per-key flat index now, the client-side succinct FST (#1) later) as a
  non-authoritative accelerator. Gives index-speed routing, easy eventual merges, and removable
  forward-and-aggregate (once routing is authoritative while warm) **without** a single point of
  failure; degrades gracefully to traversal when the cache is cold or down.
- **Rejected — centralized authoritative metadata (#2).** Simpler on paper, but makes the per-key
  authority a hard dependency for splitting and scaling. Not worth the availability cost.

Open decision for a later session: the encoding/placement of the soft index — a per-key flat index DO
(a cacheable hop, trivial freshness) vs a client-side succinct FST (zero hop, low client memory,
periodic rebuild). Both are no-compromise relative to the goals; pick per the latency/memory profile
you want.
