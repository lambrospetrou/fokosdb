# Range Partition Splits

## Background

Hash partition splits scatter items across DOs by `hashKey`. They cannot help when a single
`hashKey` accumulates so much data that it saturates one hash partition — all those items share
the same key and will always land in the same child after any number of hash splits.

The solution is a second split axis: **range partitions** that own exactly one `hashKey` and
split along the `sortKey` axis.

The overall flow is:

1. A hash partition detects a "heavy" hash key (≥ 50 % of `hashSplitConditions.maxSizeMb`)
2. It creates a **range partition** DO for that hash key and migrates all matching items into it
   ("range promotion")
3. Future requests for that hash key are forwarded to the range partition
4. When the range partition exceeds `rangeSplitConditions`, it splits into N child range
   partitions by sort key boundary ("range split")
5. Steps 4–5 recurse as needed

This is a two-tier tree:

```
Hash Partition  (owns a hash-key slice)
  └─ Range Partition for hk="user:alice"  (owns sk ∈ [∅, ∅))
       ├─ Range Child 0  (owns sk ∈ [∅, "2024-06"))    ← same DO as parent
       ├─ Range Child 1  (owns sk ∈ ["2024-06", "2024-09"))
       └─ Range Child 2  (owns sk ∈ ["2024-09", ∅))
```

---

## Design Decisions

### 1. Promote first, then range-split — not direct multi-child split

When a hash key is first detected as heavy it may be only 50 MB in a partition with a 100 MB
hash limit. The range split threshold is much larger (500 MB default). A single range partition
can comfortably hold the data; no boundary computation is needed upfront. The range partition
accumulates real data before splitting, so it picks a better boundary. This avoids a
double-migration (hash partition → N range children directly would require N parallel migrations
each filtered by sort key range).

If the hash key already exceeds the range split threshold at promotion time, the range partition
is created and immediately queues a range split — two sequential steps, same total work.

### 2. Hash/range split status stays in KV; only range_promotions goes into SQLite

The existing `SplitStatusKVItem` in KV works and the API is ergonomic. No reason to change it.

`range_promotions` must be a SQL table because when a hash partition performs its own hash split,
child hash partitions inherit some hash keys and must also inherit the corresponding range
promotion entries. This migration uses the same cursor-based paging as `getItemsBatch`. A SQL
table is the only way to paginate those rows efficiently by `hash_key`.

### 3. DO naming: `<dbName>.rk.<encoded_hashKey>.<encoded_startBoundary>`

The start boundary uniquely identifies a range partition within a hash key's range. The encoding
(base64url or percent-encoding) ensures no ambiguous dots or special characters. The length of
hash key + boundary is capped at ~500 chars (before encoding) to stay well under the 1024-byte
DO name limit.

### 4. Original DO becomes the leftmost child on range split

DO names are permanent — `idFromName` always returns the same DO for the same name. The
leftmost child always has an empty start boundary, which is the same as the original range
partition's name. Therefore the original DO IS child 0: it keeps its name, updates its
`rangePartition.maxSortKey` to the split boundary, and retains its left-portion data. Only the
N-1 right siblings are new DOs.

For N=4 with boundaries B1, B2, B3:

```
Before:  mydb.rk.alice.                 owns sk ∈ [∅, ∅)

After split_started:
         mydb.rk.alice.                 child 0 — same DO, keeps sk ∈ [∅, B1),  no migration
         mydb.rk.alice.{encode(B1)}     child 1 — new DO,  owns sk ∈ [B1, B2), migrates from original
         mydb.rk.alice.{encode(B2)}     child 2 — new DO,  owns sk ∈ [B2, B3), migrates from original
         mydb.rk.alice.{encode(B3)}     child 3 — new DO,  owns sk ∈ [B3, ∅),  migrates from original
```

After all N-1 new children acknowledge, the original marks `split_completed` and queues a
background cleanup to delete items with sk ≥ B1 from its own storage.

### 5. Forwarding during range split: left child served locally, right children forwarded

The original (child 0) cannot forward to itself. `shouldAllow()` returns `"ok"` for requests
whose sort key falls in its own range `[∅, B1)`, and `"forward"` for requests whose sort key
falls in a right sibling's range. There is no per-child migration-status check — the rule is
simply "my range is determined by my context; everything else is forwarded."

New children follow the same 503 rejection pattern as hash split children: `getItem` reads from
the parent range partition via `getItemDirect`, writes return 503 while `migration_migrating`.
Writes forwarded to migrating children are rejected, so they never land on the original and
cursor-based migration cannot miss them.

Availability picture for N=4:

| Sort key range | During migration                           | After children ack |
| -------------- | ------------------------------------------ | ------------------ |
| sk < B1        | Always available (original serves locally) | Always available   |
| sk ∈ [B1, B2)  | 503 while child 1 migrates                 | Available          |
| sk ∈ [B2, B3)  | 503 while child 2 migrates                 | Available          |
| sk ≥ B3        | 503 while child 3 migrates                 | Available          |

---

## Type Changes

### `partition-topology/types.ts`

```typescript
// Add "range_promote" as a new SplitType value.
// "hash"         — hash partition splits into N hash child partitions
// "range_promote" — hash partition promotes a heavy hash key to a range partition
// "range"        — range partition splits into N range child partitions by sort key boundary
export type SplitType = "hash" | "range_promote" | "range";
```

### `partition-topology/partition-topology.ts`

Extend `PartitionContextResolved` with optional range-partition fields:

```typescript
export type PartitionContextResolved = PartitionContext & {
	doName: string;
	primaryDoIdStr: string;
	partitionId: PartitionNodeId;
	_partitionIdBytes?: Uint8Array;

	// Present only on range partition DOs. Absent on hash partition DOs.
	rangePartition?: {
		hashKey: string;
		minSortKey: string | null; // null = no lower bound
		maxSortKey: string | null; // null = no upper bound
	};
};
```

This field travels in every request context and is persisted in `__partition_context`.

Extend `InitFromSplitOptions`:

```typescript
export type InitFromSplitOptions = {
	parentPartitionContext: PartitionContextResolved;
	newPartitionContext: PartitionContextResolved; // includes rangePartition.* for range DOs
	splitType: SplitType;
	// Set for range_promote and range splits. Redundant with newPartitionContext.rangePartition
	// but kept explicit to make the migration path unambiguous.
	ownerHashKey?: string;
};
```

---

## State Added

### In the hash partition DO

**New SQL table** (migration M3 or next available):

```sql
CREATE TABLE range_promotions (
  hash_key     TEXT NOT NULL PRIMARY KEY,
  do_name      TEXT NOT NULL UNIQUE,
  -- Serialized PartitionContextResolved of the range partition, for forwarding.
  context_json TEXT NOT NULL,
  status       TEXT NOT NULL CHECK (status IN ('rp_queued', 'rp_started', 'rp_completed')),
  created_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL
) STRICT;
```

**In-memory cache** in `PartitionDO`: a `Map<string, RangePromotionEntry>` loaded from the
`range_promotions` table inside `blockConcurrencyWhile` on startup. Updated transactionally
alongside every SQL write to `range_promotions`.

No new KV keys in the hash partition DO.

### In the range partition DO

Reuse existing KV keys (unchanged semantics):

| KV key                       | Value                           | Notes                                        |
| ---------------------------- | ------------------------------- | -------------------------------------------- |
| `__partition_context`        | `PartitionContextResolved`      | Now includes `rangePartition.*` fields       |
| `__parent_partition_context` | `PartitionContextResolved`      | Parent hash partition context                |
| `__parent_split_type`        | `SplitType`                     | `"range_promote"` for initial promotion      |
| `__split_migration_status`   | `PartitionSplitMigrationStatus` | Reused unchanged                             |
| `__split_migration_cursor`   | `MigrationCursor`               | Reused unchanged                             |
| `__split_status`             | `SplitStatusKVItem`             | For when range partition itself range-splits |

**New SQL table** in the range partition DO (same migration as above or separate):

```sql
-- Tracks children created when this range partition itself range-splits.
-- Only populated once a range split is initiated.
CREATE TABLE range_split_children (
  do_name         TEXT NOT NULL PRIMARY KEY,
  start_sk        TEXT,    -- NULL = no lower bound (leftmost child = self, never inserted here)
  end_sk          TEXT,    -- NULL = no upper bound
  migration_acked INTEGER NOT NULL DEFAULT 0,
  created_at      INTEGER NOT NULL
) STRICT;
```

Note: the leftmost child (the original DO itself) is NOT inserted into `range_split_children`
because it never acknowledges to itself. Completion is detected when all rows in
`range_split_children` have `migration_acked = 1`.

---

## DO Naming

### Range partition name helper

```typescript
// Encodes a hash key or sort key for use in a DO name.
// Uses percent-encoding of any character that is not alphanumeric, hyphen, or underscore.
function encodeRangePartitionComponent(s: string): string { ... }

// Returns the DO name for a range partition.
// startSortKey=null means "no lower bound" (the initial / leftmost partition for this hash key).
function rangePartitionDoName(
  databaseName: string,
  hashKey: string,
  startSortKey: string | null,
): string {
  const encodedHk  = encodeRangePartitionComponent(hashKey);
  const encodedSk  = startSortKey == null ? "" : encodeRangePartitionComponent(startSortKey);
  const name = `${databaseName}.rk.${encodedHk}.${encodedSk}`;
  if (name.length > 900) {
    // Warn — approaching 1024-byte limit.
    console.warn({ message: "fokos: range partition DO name is long", name });
  }
  return name;
}
```

### Range split child naming

When range partition `mydb.rk.alice.` splits with boundaries [B1, B2, B3]:

```
child 0  →  mydb.rk.alice.               (same DO, startSortKey=null)
child 1  →  mydb.rk.alice.{encode(B1)}   (new DO, startSortKey=B1)
child 2  →  mydb.rk.alice.{encode(B2)}   (new DO, startSortKey=B2)
child 3  →  mydb.rk.alice.{encode(B3)}   (new DO, startSortKey=B3)
```

---

## Routing Changes

### Layer 1: Hash partition → range partition (new)

`shouldAllow()` in `PartitionTopologyImpl` is called before every operation. It currently returns
`"ok" | "forward" | "reject"`. It needs to check the range promotion registry first.

New routing decision type (or inline logic in `withSplitForwarding`):

```
1. Look up hashKey in the in-memory range promotion cache.
2. If found and status == "rp_completed":
     return "forward_range" (forward to rangePartition DO)
3. If found and status == "rp_started":
     return "forward_range" (child reads from parent directly via getItemDirect if migrating)
4. If found and status == "rp_queued":
     return "ok" (range partition not ready yet, serve locally)
5. Not found: existing hash split forwarding logic unchanged.
```

`pickChildRangePartition(hashKey)` returns the `PartitionContextResolved` from the cache entry
and creates a `DurableObjectId` via `idFromName`.

`groupItemsByRouting()` (used by `prepare`, `commit`, `cancel`, `readForTransaction`) must also
check the range promotion cache per hash key before the existing hash split check, so that
transaction items for a promoted hash key route to the range partition.

### Layer 2: Range partition → range split children (new)

`shouldAllow()` within a range partition DO:

```
1. If own rangePartition.hashKey != request.hashKey: reject (should never happen in practice)
2. If split_status is split_started or split_completed:
   a. If sortKey is within own range (minSortKey ≤ sk < maxSortKey):
        return "ok" — serve locally (this is the "original is child 0" invariant)
   b. Else:
        return "forward" — find the child in range_split_children whose [start_sk, end_sk) covers sk
3. Else: return "ok"
```

`pickChildRangeSplitPartition(sortKey)` queries `range_split_children` for the row where
`start_sk <= sortKey AND (end_sk IS NULL OR sortKey < end_sk)`.

### Layer 3: Hash partition → hash split children (existing, unchanged)

---

## Migration Changes

### Migration type A: range promotion (hash partition → range partition)

The child range partition calls `getItemsBatch` on the parent hash partition with its
`childPartitionContext.rangePartition.hashKey` set. The parent branches:

```typescript
// In getItemsBatch on the hash partition:
if (opts.childPartitionContext.rangePartition) {
  // Range promotion migration — filter by hash key equality only.
  // SQL: WHERE hk = ? AND (cursor conditions) ORDER BY hk, sk LIMIT ?
  const hk = opts.childPartitionContext.rangePartition.hashKey;
  ...
} else {
  // Hash split migration — existing makeIsCorrectChildHashPartition filter.
  ...
}
```

Similarly `getPartitionTransactionMetadata` branches on the same condition.

After migration completes, the child range partition calls a new RPC on the parent:

```typescript
// New RPC on hash partition DO.
async acknowledgeRangePartitionMigrationComplete(hashKey: string, childDoName: string): Promise<void>
```

This updates `range_promotions SET status = 'rp_completed' WHERE hash_key = ?` and the
in-memory cache. The parent does NOT delete the promoted items from its own `items` table here;
a background job handles that after completion (see Background Jobs below).

### Migration type B: range split (range partition → range split children)

Only the N-1 right siblings call `getItemsBatch` on the original range partition. The leftmost
child (original DO) does not migrate — it keeps its own data and updates its context.

The original range partition's `getItemsBatch` for range split children:

```typescript
// In getItemsBatch on the range partition:
const childRange = opts.childPartitionContext.rangePartition!;
// SQL: WHERE hk = ? AND sk >= ? AND (sk < ? OR end IS NULL) AND (cursor conditions) ORDER BY hk, sk LIMIT ?
```

`makeIsCorrectChildRangePartition(parentContext, childContext)` returns a predicate:

```typescript
(hashKey: string, sortKey?: string) =>
	hashKey === childContext.rangePartition!.hashKey &&
	(childContext.rangePartition!.minSortKey == null || (sortKey ?? "") >= childContext.rangePartition!.minSortKey) &&
	(childContext.rangePartition!.maxSortKey == null || (sortKey ?? "") < childContext.rangePartition!.maxSortKey);
```

After a right-sibling child acknowledges (`acknowledgeChildMigrationComplete` on the original),
the original checks if all rows in `range_split_children` have `migration_acked = 1`. If so,
it marks `split_completed` in `__split_status` and queues a background cleanup job.

### Migrating range_promotions during hash splits

When a hash partition hash-splits, child hash partitions must inherit range promotion entries
for the hash keys they take ownership of. Add a new parent RPC:

```typescript
async getRangePromotionsBatch(opts: {
  childPartitionContext: PartitionContextResolved;
  cursor: { hashKey: string } | null;
}): Promise<{ promotions: RangePromotionRow[]; nextCursor: { hashKey: string } | null }>
```

The parent pages through `range_promotions ORDER BY hash_key` and filters rows in-code using
`makeIsCorrectChildHashPartition(parentCtx, childCtx)(row.hash_key)`. The child inserts the
received rows into its own `range_promotions` table and populates its in-memory cache.

This call happens in `runMigration()` after the main items migration loop and before the
pending-transactions migration loop.

---

## `startSplit` Changes

### Range promotion path (`splitType == "range_promote"`)

Called from the hash partition's background job (not from `PartitionTopologyImpl.startSplit`).
Implemented as a new method `startRangePromotion(hashKey)` on `PartitionDO`:

```
1. Read range_promotions entry for hashKey; assert status == "rp_queued".
2. Compute the range partition DO name: rangePartitionDoName(databaseName, hashKey, null).
3. Get or create the DO ID via idFromName.
4. Build newPartitionContext with rangePartition: { hashKey, minSortKey: null, maxSortKey: null }.
5. Call child.initFromSplit({ parentPartitionContext, newPartitionContext, splitType: "range_promote", ownerHashKey: hashKey }).
   Retry up to 5 times (same pattern as existing startSplit).
6. On success: UPDATE range_promotions SET status = 'rp_started'.
7. Call child.triggerMigration() fire-and-forget.
```

### Range split path (`splitType == "range"`)

Called from `PartitionTopologyImpl.startSplit()` when `split_status.splitType == "range"`.
This runs inside the range partition DO itself:

```
1. Compute the median sort key boundary (and B2, B3 for N=4):
   SELECT sk FROM items ORDER BY sk LIMIT 1 OFFSET (SELECT COUNT(*)/4 FROM items)   -- B1
   SELECT sk FROM items ORDER BY sk LIMIT 1 OFFSET (SELECT COUNT(*)*2/4 FROM items) -- B2
   SELECT sk FROM items ORDER BY sk LIMIT 1 OFFSET (SELECT COUNT(*)*3/4 FROM items) -- B3

2. Update own context: set rangePartition.maxSortKey = B1 in __partition_context.

3. Insert N-1 rows into range_split_children (children 1, 2, 3):
   INSERT INTO range_split_children (do_name, start_sk, end_sk, migration_acked, created_at) VALUES (...)

4. Build newPartitionContext for each new child with rangePartition.{ hashKey, minSortKey, maxSortKey }.

5. Call initFromSplit on children 1, 2, 3 (retry up to 5 times each).

6. Mark __split_status = split_started.

7. Call triggerMigration() on children 1, 2, 3 fire-and-forget.
```

---

## New Background Jobs (in `runBackgroundWork`)

### Job: Detect heavy hash keys and queue range promotions (hash partition only)

Run at most every 30 seconds (tracked by an in-memory timestamp). Not run on every write.

```sql
SELECT hk, SUM(LENGTH(CAST(data AS BLOB)) + LENGTH(sk) + 80) AS estimated_bytes
FROM items
GROUP BY hk
HAVING estimated_bytes >= ?    -- 0.5 * hashSplitConditions.maxSizeMb * 1024 * 1024
LIMIT 5
```

For each result where no `range_promotions` entry exists yet:

```sql
INSERT OR IGNORE INTO range_promotions (hash_key, do_name, context_json, status, created_at, updated_at)
VALUES (?, ?, ?, 'rp_queued', ?, ?)
```

Set alarm. The next `runBackgroundWork` invocation calls `startRangePromotion(hashKey)` for
each row with `status = 'rp_queued'`.

### Job: Start queued range promotions (hash partition only)

```
For each row in range_promotions WHERE status = 'rp_queued':
  call startRangePromotion(hashKey)
```

### Job: Start queued range splits (range partition only)

Reuses the existing split job in `runBackgroundWork`:

```typescript
const splitStatus = topology.splitStatus();
if (splitStatus?.status === "split_queued") {
	await topology.startSplit(); // now handles "range" type too
}
```

### Job: Cleanup promoted items from parent hash partition (hash partition only)

After a range promotion reaches `rp_completed`, the parent hash partition still holds the
promoted items. A background job deletes them:

```sql
DELETE FROM items WHERE hk = ?   -- per promoted hash key with rp_completed status
```

Run with a page limit to avoid blocking the DO for too long. Use the migration cursor pattern
if the hash key has many items.

After cleanup: optionally mark the range_promotions row as `rp_gc_completed` or leave
`rp_completed` and add a `gc_completed_at` column.

### Job: Cleanup right-sibling items from original range partition (range partition only)

After all `range_split_children` are `migration_acked = 1` (split_completed):

```sql
DELETE FROM items WHERE (sk >= ? OR sk IS NOT NULL) AND sk >= ?   -- items outside own range
```

Or more precisely: `DELETE FROM items WHERE sk >= maxSortKey` (using own context's
`rangePartition.maxSortKey`). Paged.

---

## New / Modified RPCs

| RPC                                                           | Where added       | Purpose                                                        |
| ------------------------------------------------------------- | ----------------- | -------------------------------------------------------------- |
| `acknowledgeRangePartitionMigrationComplete(hashKey, doName)` | Hash partition DO | Called by range partition child after promotion migration      |
| `getRangePromotionsBatch(opts)`                               | Hash partition DO | Paginated range_promotions rows for hash-split child migration |
| `getItemDirect(opts)`                                         | Already exists    | Range partition child calls this on parent during migration    |

The existing `acknowledgeChildMigrationComplete(childDoName)` is reused for range split
children calling back to the original range partition.

---

## `initFromSplit` Changes

Add handling for `splitType == "range_promote"` and `splitType == "range"`:

```typescript
async initFromSplit(opts: InitFromSplitOptions) {
  const { parentPartitionContext, newPartitionContext, splitType } = opts;

  if (this.#_partitionContext) {
    // Idempotent check (existing logic) ...
  }

  this.ensurePartitionContext(newPartitionContext);
  this.ctx.storage.kv.put(KV.PARENT_PARTITION_CONTEXT, parentPartitionContext);
  this.ctx.storage.kv.put(KV.PARENT_SPLIT_TYPE, splitType);

  if (splitType === "range_promote" || splitType === "range") {
    // rangePartition fields are already encoded in newPartitionContext.
    // No additional KV keys needed beyond what ensurePartitionContext writes.
  }

  this.ctx.storage.kv.put(KV.SPLIT_MIGRATION_STATUS, "migration_initialized");
}
```

---

## `runMigration` Changes

Branch on `__parent_split_type`:

```typescript
private async runMigration(): Promise<void> {
  const splitType = this.ctx.storage.kv.get<SplitType>(KV.PARENT_SPLIT_TYPE);

  if (splitType === "hash") {
    await this.runHashChildMigration();       // existing logic (items + pending_transactions)
  } else if (splitType === "range_promote") {
    await this.runRangePromotionMigration();  // items filtered by hk=ownerHashKey + pending_transactions
  } else if (splitType === "range") {
    await this.runRangeSplitChildMigration(); // items filtered by sk range + pending_transactions
  }
}
```

`runRangePromotionMigration()` is structurally identical to `runHashChildMigration()` except:

- `getItemsBatch` filter = hash key equality (not hash routing)
- Completion calls `acknowledgeRangePartitionMigrationComplete(ownerHashKey, doName)` instead
  of `acknowledgeChildMigrationComplete(doName)`

`runRangeSplitChildMigration()` is structurally identical except:

- `getItemsBatch` filter = sort key range
- Completion calls `acknowledgeChildMigrationComplete(doName)` on the parent range partition

---

## `ensureTopology` Changes

Currently always returns a `PartitionTopologyImpl`. After these changes:

```typescript
private ensureTopology(pCtx: PartitionContextResolved): PartitionTopologySplitter {
  if (!this.#_topology) {
    this.#_topology = new PartitionTopologyImpl("", pCtx, this.ctx);
  }
  return this.#_topology;
}
```

`PartitionTopologyImpl` must be updated to handle `SplitType = "range"` in `startSplit()`,
and its `shouldAllow()` / `pickChildPartition()` must be aware of whether this is a hash
partition or range partition (detectable from `pCtx.rangePartition`).

Alternatively, create a `RangePartitionTopologyImpl` and select based on context:

```typescript
if (pCtx.rangePartition) {
	this.#_topology = new RangePartitionTopologyImpl("", pCtx, this.ctx);
} else {
	this.#_topology = new PartitionTopologyImpl("", pCtx, this.ctx);
}
```

`RangePartitionTopologyImpl` implements the same `PartitionTopologySplitter` interface but:

- `shouldAllow()` checks sort key range (serve locally if sk in own range, forward otherwise)
- `pickChildPartition()` queries `range_split_children` by sort key
- `maybeQueueSplit()` uses `rangeSplitConditions` instead of `hashSplitConditions`
- `startSplit()` runs the range split path described above

---

## `ensurePartitionContext` Changes

Currently validates that the incoming context matches the stored context. After these changes,
also accept updates to `rangePartition.maxSortKey` (since the original range partition updates
its own boundary during a range split). All other fields remain immutable.

---

## SQL Migrations

Add as migration M3 (next after existing M2):

```sql
-- M3: range partition support

-- In hash partition DOs: tracks hash keys promoted to dedicated range partition DOs.
CREATE TABLE IF NOT EXISTS range_promotions (
  hash_key     TEXT NOT NULL PRIMARY KEY,
  do_name      TEXT NOT NULL UNIQUE,
  context_json TEXT NOT NULL,
  status       TEXT NOT NULL CHECK (status IN ('rp_queued', 'rp_started', 'rp_completed')),
  created_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL
) STRICT;

-- In range partition DOs: tracks right-sibling children created during a range split.
-- The leftmost child (the original DO itself) is never inserted here.
CREATE TABLE IF NOT EXISTS range_split_children (
  do_name         TEXT NOT NULL PRIMARY KEY,
  start_sk        TEXT,
  end_sk          TEXT,
  migration_acked INTEGER NOT NULL DEFAULT 0,
  created_at      INTEGER NOT NULL
) STRICT;
```

Both tables are created in all partition DOs by the migration; they are simply unused in DOs
that never perform the respective operation.

---

## Implementation Phases

### Phase 1 — Types and DB migration (no behavior change)

- Add `"range_promote"` to `SplitType`
- Add `rangePartition?` to `PartitionContextResolved`
- Extend `InitFromSplitOptions` with optional `ownerHashKey`
- Add SQL migration M3 (`range_promotions`, `range_split_children`)
- Add `rangePartitionDoName()` helper and encoding utilities
- Tests: verify migration runs cleanly, no behavior regressions

### Phase 2 — Range promotion: detection and routing

- Add in-memory `Map<string, RangePromotionEntry>` cache to `PartitionDO`; populate in `blockConcurrencyWhile`
- Add per-hash-key size detection query to `runBackgroundWork` (hash partition only)
- Add `startRangePromotion(hashKey)` on `PartitionDO`
- Extend `withSplitForwarding()` and `groupItemsByRouting()` to check range promotion cache
- Add `acknowledgeRangePartitionMigrationComplete()` RPC
- Tests: detect heavy hash key, queue promotion, forward requests after `rp_completed`

### Phase 3 — Range promotion: migration

- Extend `getItemsBatch()` and `getPartitionTransactionMetadata()` to branch on
  `childPartitionContext.rangePartition` for hash-key-equality filtering
- Extend `initFromSplit()` to handle `"range_promote"` split type
- Implement `runRangePromotionMigration()` in `runMigration()`
- Implement background cleanup job for promoted items in parent hash partition
- Tests: full promotion cycle (rp_queued → rp_started → migration → rp_completed → cleanup)

### Phase 4 — Range promotion: hash-split inheritance

- Add `getRangePromotionsBatch()` RPC on hash partition DO
- Extend `runHashChildMigration()` to also call `getRangePromotionsBatch` and insert received
  rows into child's `range_promotions` table and in-memory cache
- Tests: hash split inherits range promotion entries correctly

### Phase 5 — Range split

- Implement `RangePartitionTopologyImpl` (or extend `PartitionTopologyImpl`):
  - `shouldAllow()` with own-range check
  - `pickChildPartition()` querying `range_split_children`
  - `maybeQueueSplit()` using `rangeSplitConditions`
  - `startSplit()` with boundary computation, `range_split_children` inserts, child DO init
- Extend `getItemsBatch()` for sort-key-range filtering
- Implement `runRangeSplitChildMigration()` in `runMigration()`
- Implement background cleanup job for right-sibling items in original range partition
- Update `ensureTopology()` to select `RangePartitionTopologyImpl` when `pCtx.rangePartition` set
- Tests: full range split cycle for N=4, including availability properties

### Phase 6 — Destroy traversal

- Extend `FokosDB.destroy()` to also traverse range partition DOs reachable from each hash
  partition's `range_promotions` table and, transitively, each range partition's
  `range_split_children` table
- Tests: destroy cleans up all range partition DOs

---

## Open Questions / Future Work

- **Sort key boundary computation**: the quartile queries above are simple but may produce
  imbalanced splits if the sort key distribution is skewed. A reservoir-sampling approach or
  an explicit ANALYZE pass could improve split quality.
- **Range promotion threshold**: currently fixed at 50 % of `hashSplitConditions.maxSizeMb`.
  Should this be configurable in `PartitionContext`?

## Resolved Questions

### Right-sibling cleanup racing with new writes

No race exists. Once a range partition transitions to `split_started`, its `shouldAllow()` only
returns `"ok"` for sort keys within its own range `[∅, B1)`. All requests for sk ≥ B1 are
forwarded to right-sibling children (which reject writes with 503 while migrating, exactly as
hash split children do). No new writes for sk ≥ B1 ever land on the original after
`split_started`, so the background DELETE of items with sk ≥ B1 is unconditionally safe. This
is the same guarantee that hash splits rely on: forward everything outside your own range,
accept nothing you don't own.

### Transaction items spanning multiple range children for the same hash key

After a range split, a single transaction may include `(hk="alice", sk="2024-01")` and
`(hk="alice", sk="2024-08")` which now live in different range children. The current
`groupItemsByRouting()` groups by hash key (one destination per hash key). This must become
per destination DO — determined by `(hashKey, sortKey)` together. The change: for any item
whose hash key routes to a range partition that is in `split_started` or `split_completed`,
resolve the destination by also matching the sort key against `range_split_children` boundaries,
and group by the resulting DO name. Items for the same hash key but different sort key ranges
will land in different groups and be sent to different DOs independently.

---

## Appendix: Review Concerns (2026-05-29)

Concerns raised during a correctness/race review of this plan, grounded in the existing
hash-split implementation (`do-partition.ts`, `partition-topology/partition-topology.ts`).
Recorded here so they are not lost; several are addressed by a subsequent redesign.

### Blocking correctness gaps

**A. Mixed local + forwarded items in a transaction hard-fails.** The current
`prepare`/`commit`/`cancel`/`readForTransaction` all assert
`invariant(local.length === 0, "split routing must not mix local and forwarded items")`
(`do-partition.ts:534, 658, 795`). This holds for hash splits only because `shouldAllow`
is all-or-nothing once `split_started` (`partition-topology.ts:544-546` forwards *every*
key). Range promotion breaks the invariant: a hash partition serves most keys locally while
forwarding a few promoted keys to range partitions. An ordinary multi-key transaction
touching one promoted key + one local key produces `local.length > 0 && forwarded.size > 0`
and throws — permanently, for that key combination. Not a one-line fix: the transaction
methods are written `local XOR forwarded`, and would need `local AND forwarded` with correct
2PC/rollback across the local portion + N range partitions. Cleanest resolution is to make
the routing layer aware of promotions so promoted keys are routed independently and never
mixed into a hash partition's local `prepare`.

**B. Concurrent hash split + in-flight range promotion corrupts state.** Nothing prevents a
hash partition from hash-splitting while one of its hash keys is mid-promotion (`rp_started`).
The hash child inherits the `range_promotions` row (still `rp_started`) *and* inherits the
promoted key's items via the normal hash-split filter, so the key's data ends up in three
places (old parent — items are NOT deleted at hash `split_completed`; new hash child; range
root). The range root acks completion to its *stored* parent (the old hash partition), so the
hash child's inherited row stays `rp_started` forever — its cleanup never runs and it forwards
the key while holding orphaned local items. Requires explicit **mutual exclusion** between the
two split axes (no promotion while a hash split is queued/active; no hash split while any row
is `rp_started`), or defined re-parenting of the in-flight migration.

**C. Cleanup ignores `pending_transactions` and the singleton watermark.** Hash split deletes
*all* `pending_transactions` atomically at `split_completed` (`do-partition.ts:500-510`,
"Bug 3"). The plan's per-key cleanup only does `DELETE FROM items WHERE hk = ?` and never
deletes `pending_transactions` for the promoted key (or `sk >= maxSortKey` for range split),
so copied pending rows are orphaned and the stale-tx recovery job pokes the coordinator
forever. The pending-row deletion must also be atomic with the status transition. Separately,
`deletion_metadata.max_deleted_ts` is a singleton shared across all hash keys
(`do-partition.ts:737, 1450-1477`); a range partition inherits the parent's *global* watermark
(safe but conservative), and the bare cleanup `DELETE` does not bump it (fine here, but
document it).

### Races / ordering

**D. Boundary computation races with concurrent writes.** During `split_queued`, `shouldAllow`
does NOT forward (`partition-topology.ts:544` only forwards when `status !== "split_queued"`),
so writes keep flowing while `startSplit` runs. Computing B1/B2/B3 as three separate `OFFSET`
queries can yield non-monotonic boundaries if inserts shift offsets between queries. Compute
all boundaries in one `transactionSync` snapshot. (Data correctness is still safe — items are
captured by the child's full-scan migration and deleted only after acks — but inverted
boundaries break routing.) Also add a minimum-size guard so small partitions don't produce
empty middle ranges.

**E. `ensurePartitionContext` cannot validate a self-mutated `maxSortKey`.** The range root
updates its own `maxSortKey` (null → B1) on split, but the hash partition forwards using the
promotion-time `context_json` (`maxSortKey: null`, never updated). Stored vs. incoming will
permanently mismatch. Resolution: range bounds must be locally-authoritative, *excluded* from
the equality check (not merely "accept updates," which risks a stale forwarded context
clobbering the real boundary), and routing must always use the stored value. Consequence: a
promoted-and-split key is always ≥2 hops (hash partition → range root → range child); the root
must stay alive as a pure router (it does, as child 0).

**F. Two parallel child-tracking mechanisms for range split.** The plan adds the SQL table
`range_split_children` with `migration_acked` *and* reuses `acknowledgeChildMigrationComplete`,
which tracks completion in KV `split_status.migratedChildDoNames` and flips `split_completed`
when all `childPartitionContexts` ack (`partition-topology.ts:749-788`). `cancel` forwarding
also iterates `splitStatus.childPartitionContexts` (`do-partition.ts:764`). Two sources of
truth that must stay in sync. Prefer dropping `range_split_children` and reusing
`split_status.childPartitionContexts` (which already carries `rangePartition.min/maxSortKey`
for routing), so `cancel`/ack/`destroy` keep working unchanged.

**G. `getPartitionTransactionMetadata` range-split branch unspecified.** The plan branches it
for range promotion (hk filter) but `runRangeSplitChildMigration` is "structurally identical
except sk-range filter" without saying the pending-transaction metadata fetch also needs an
sk-range branch. Specify it.

### Smaller issues

- **H. Garbled cleanup SQL.** `DELETE FROM items WHERE (sk >= ? OR sk IS NOT NULL) AND sk >= ?`
  is nonsense (`sk IS NOT NULL` is always true; `sk` is `NOT NULL DEFAULT ''`). Use
  `WHERE sk >= maxSortKey`.
- **I. Heavy-key detection must be hard-guarded off on range partitions** (`!pCtx.rangePartition`),
  or a range partition self-promotes its only hash key in a loop.
- **J. Size-estimate unit mismatch.** Detection uses `SUM(LENGTH(data)+LENGTH(sk)+80)` per `hk`
  but split conditions use `sql.databaseSize` (page-allocated, incl. indexes). Different units;
  calibrate or document the heuristic.
- **K. `pickChildRangeSplitPartition` must reconstruct a byte-identical context** (merge all
  immutable fields: `ns`, `hashSplitConditions`, `rangeSplitConditions`, `partitionId`, …), or
  the child's `ensurePartitionContext` rejects.
- **L. DO-name length cap behavior undefined.** If `hk` + boundary exceeds the encoded cap,
  define refuse-to-promote vs. hash-the-name; silent warn-then-truncate risks collisions.
- **M. In-memory cache + SQL write atomicity.** The parent's `range_promotions` cache update
  must share one `transactionSync` with the row insert/update.
