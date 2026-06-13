# Promoted Keys Bloom Filter Cache — Implementation Plan

Each hash partition maintains a bloom filter of hash keys that have been promoted into their own
range partitions. This enables ancestor (router) hash partitions to skip intermediate hops through
child hash partitions and route directly to the range root DO for promoted keys.

This is analogous to how `HashTopology` (in `hash-topology.ts`) caches the depth of the hash split
tree from forwarded responses — the bloom filter caches _which_ keys live in range structures.

## Goal

When a request arrives at an ancestor hash partition for a promoted key, the current flow is:

```
Grandparent → Child hash partition → Range root (serves request)
```

With the bloom filter cache, the grandparent learns from the first forwarded response that the key
was served by a range partition. On subsequent requests:

```
Grandparent → Range root (serves request)   // skips the child hop
```

## Key properties

- **No false negatives.** If the bloom filter says a key is not present, it is definitely not
  promoted. Safe to route through the hash tree.
- **False positives possible.** The bloom filter may say a key is promoted when it is not. The
  range root DO will not be initialized and will throw a phantom-bounce error. We catch this and
  fall back to normal hash-tree routing.
- **Append-only.** Promotions are permanent (keys are never un-promoted), so the bloom filter only
  grows — which is fine since standard bloom filters cannot remove entries.
- **Range root names are deterministic.** `resolveRangePartitionContext` computes the range root DO
  name purely from `(databaseName, hashKey, startBoundary, endBoundary)`. It does NOT depend on
  the calling partition's identity. Any ancestor can compute the same range root name and route
  directly. (See `rangePartitionDoName` in `partition-id.ts`.)

## Bloom filter configuration

- Error rate: 1%
- Initial capacity N: 100K
- Max size: 1MB (grow to this if the initial bloom filter fills up)

Since the `BloomFilter` class takes a single `maxSizeBytes` and layers grow automatically, the
implementation creates the filter with `maxSizeBytes: 1MB`.

## Detecting range partition responses (no new metadata field)

We do NOT add a new `isPromotedKey` field to `PartitionInfo`. Instead, we use the existing
`servedByPartitionId` field:

```typescript
PartitionIdHelper.isRangePartition(result.meta.servedByPartitionId);
```

The partition ID encodes the partition type in its first byte (`SCHEMA_HASH_V1` vs
`SCHEMA_RANGE_V1`). The `servedByPartitionId` is set by the leaf that actually serves the request
and propagates unchanged through forwarding hops (only `forwardCount` and `hashDepth` are
overwritten by intermediaries). So at any ancestor level, `servedByPartitionId` reflects the true
serving partition's type.

---

## Architecture: `PartialRangeTopology`

### Why a dedicated class

The bloom filter is a **routing cache** — the same category as `HashTopology`. It belongs alongside
it in `src/lib/partition-topology/`, not inside `PromotionManager` (which manages lifecycle) or
inline in `PartitionDO` (which would scatter routing logic).

A thin wrapper class gives us the right seam to extend later: once we start learning sort key
boundaries from forwarded responses, we can add a per-key boundary cache to route directly to range
leaves — skipping the range root traversal too. The class provides that extension point without
needing a second refactor.

### Design

The class follows the same pattern as `HashTopology`:

- Pure in-memory data structure, no storage awareness.
- The DO owns the instance, loads it from KV on startup, and persists when modified.
- Methods return booleans to signal "was the cache modified?" so the caller decides when to persist.

```
src/lib/partition-topology/partial-range-topology.ts
```

```typescript
export type PartialRangeTopologySnapshot = {
    version: 1;
    promotedKeysBloom: BloomFilterSnapshot;
    // Future: per-key boundary caches (learned sort key split points).
};

export class PartialRangeTopology {
    private bloom: BloomFilter;

    private constructor(bloom: BloomFilter) { ... }

    static create(opts: {
        errorRate?: number;
        maxSizeBytes: number;
        initialCapacityN?: number;
    }): PartialRangeTopology;

    static fromSnapshot(snapshot: PartialRangeTopologySnapshot): PartialRangeTopology;

    /**
     * Check if a hash key might be promoted to a range structure.
     * Returns false → definitely not promoted (no false negatives).
     * Returns true → might be promoted (possible false positive).
     */
    maybePromoted(hashKey: string): boolean;

    /**
     * Learn a single promoted key (e.g. from a forwarded response).
     * Returns true if the cache was modified (caller should persist).
     * Returns false if the key could not be added (bloom filter full)
     * — caller should log but not fail.
     */
    learnPromotedKey(hashKey: string): boolean;

    /**
     * Batch-learn promoted keys (e.g. syncing from PromotionManager after
     * migration or drive()). Returns true if any key was added (caller
     * should persist once for the whole batch).
     */
    learnPromotedKeys(hashKeys: Iterable<string>): boolean;

    toSnapshot(): PartialRangeTopologySnapshot;

    isEmpty(): boolean;

    stats(): {
        bloomKeyCount: number;
        bloomFull: boolean;
        bloomMaxSizeBytes: number;
    };
}
```

### Future extension: sort key boundary cache

Once range splits are common, we will also learn which sort key boundaries the range root has split
at. This enables direct routing to the correct range leaf, skipping the range root traversal:

```
Current:   Ancestor → Range root → Range leaf (serves request)
Future:    Ancestor → Range leaf (serves request)
```

The class naturally extends with:

```typescript
// Future additions:
learnBoundaries(hashKey: string, boundaries: string[]): boolean;
pickRangeLeaf(hashKey: string, sortKey: string): {
    start: string | null;
    end: string | null;
} | null;
```

Nothing in the current implementation should preclude this.

---

## Integration: no PromotionManager callback

The bloom filter is NOT updated via a callback on `PromotionManager`. Callbacks cause per-key
persistence during migration (N snapshot writes instead of 1) and conflate routing-cache concerns
with promotion-lifecycle concerns.

Instead, the DO owns the `PartialRangeTopology` and syncs it explicitly at **two** integration
points:

### 1. Per-request learning (in `withSplitForwarding`)

When a forwarded response comes back through the hash topology `"forward"` path and
`PartitionIdHelper.isRangePartition(result.meta.servedByPartitionId)` is true, call
`learnPromotedKey(hashKey)`. If it returns true (modified), persist the snapshot immediately.
This is at most one add + one persist per request — cheap.

### 2. Batch sync (in `runBackgroundWork` finally block)

After migration and drive() complete, iterate `this.#promotion.snapshot()` and collect all keys
with status `"promoting"` or `"promoted"`. Call `learnPromotedKeys(keys)`. If it returns true
(modified), persist the snapshot once. This covers:

- Keys newly transitioned to "promoting" by drive().
- Keys inherited during migration with "promoting" or "promoted" status.

One snapshot write regardless of how many keys were processed.

---

## Changes by file

### 1. `src/lib/partition-topology/partial-range-topology.ts` (NEW)

New file, sibling to `hash-topology.ts`. Contains `PartialRangeTopology` class and
`PartialRangeTopologySnapshot` type as described in the Architecture section above.

The class wraps a `BloomFilter` and delegates to it:

- `maybePromoted(hashKey)` → `this.bloom.has(hashKey)`
- `learnPromotedKey(hashKey)` → `this.bloom.add(hashKey)`, return the result
- `learnPromotedKeys(hashKeys)` → loop `this.bloom.add()`, return whether any succeeded
- `toSnapshot()` → `{ version: 1, promotedKeysBloom: this.bloom.toSnapshot() }`
- `fromSnapshot()` → `BloomFilter.fromSnapshot(snapshot.promotedKeysBloom)`
- `isEmpty()` → `this.bloom.keyCount() === 0`
- `stats()` → derived from `this.bloom` state

When `learnPromotedKey` returns false (bloom filter full — `BloomFilter.add()` returned false),
the caller logs an info message. The class itself does not log.

### 2. `src/lib/do-partition.ts`

This is the main integration point. Changes grouped by concern:

#### 2a. New KV key and field

Add to `KV_KEYS`:

```typescript
PARTIAL_RANGE_TOPOLOGY: "__partial_range_topology";
```

Add private field:

```typescript
#_partialRangeTopology: PartialRangeTopology | null = null;
```

Import `PartialRangeTopology` and `PartialRangeTopologySnapshot` from
`../partition-topology/partial-range-topology.js`.

#### 2b. Startup loading

In the `blockConcurrencyWhile` block (constructor), after loading the partition context and before
loading promoted keys:

```typescript
const prtSnap = ctx.storage.kv.get<PartialRangeTopologySnapshot>(PartitionDO.KV_KEYS.PARTIAL_RANGE_TOPOLOGY);
if (prtSnap) {
	this.#_partialRangeTopology = PartialRangeTopology.fromSnapshot(prtSnap);
}
```

#### 2c. Helper methods

```typescript
private getOrCreatePartialRangeTopology(): PartialRangeTopology {
    if (!this.#_partialRangeTopology) {
        this.#_partialRangeTopology = PartialRangeTopology.create({
            errorRate: 0.01,
            maxSizeBytes: 1 * 1024 * 1024, // 1MB max
            initialCapacityN: 100_000,
        });
    }
    return this.#_partialRangeTopology;
}

private persistPartialRangeTopology(): void {
    if (this.#_partialRangeTopology) {
        this.ctx.storage.kv.put<PartialRangeTopologySnapshot>(
            PartitionDO.KV_KEYS.PARTIAL_RANGE_TOPOLOGY,
            this.#_partialRangeTopology.toSnapshot()
        );
    }
}
```

#### 2d. Phantom-bounce detection helper

Add a module-level helper:

```typescript
function isPhantomBounceError(e: unknown): boolean {
	return e instanceof Error && e.message.includes("phantom-bounce");
}
```

#### 2e. Two new routing methods

Extract the "forward to range root" logic from `withSplitForwarding` into two methods:

**`forwardToRangeRootPartition`** — inner primitive. Resolves the range root, forwards the
request, adjusts response meta (`hashDepth`, `forwardCount`). Throws on phantom-bounce (any error
from the range root propagates normally).

```typescript
private async forwardToRangeRootPartition<T extends { meta: PartitionInfo }>(
    ctx: PartitionContextResolved,
    hashKey: string,
    forward: (stub: PartitionDOStub, pCtx: PartitionContextResolved) => Promise<T>,
): Promise<T> {
    const { partitionContext: rangeRootCtx } = resolveRangePartitionContext(
        ctx, hashKey, null, null
    );
    const doId = this.env[ctx.ns].idFromName(rangeRootCtx.doName);
    const rangeRootStub = this.env[ctx.ns].get(doId);
    const result = await forward(rangeRootStub, rangeRootCtx);
    return {
        ...result,
        meta: {
            ...result.meta,
            hashDepth: PartitionIdHelper.depth(this.pCtx()._partitionIdBytes!),
            forwardCount: result.meta.forwardCount + 1,
        },
    } as T;
}
```

**`maybeForwardToRangeRootPartition`** — outer wrapper for speculative (bloom filter) routing.
Calls `forwardToRangeRootPartition`, catches phantom-bounce errors, and returns `null` to signal
"fall through to normal routing."

```typescript
private async maybeForwardToRangeRootPartition<T extends { meta: PartitionInfo }>(
    ctx: PartitionContextResolved,
    hashKey: string,
    forward: (stub: PartitionDOStub, pCtx: PartitionContextResolved) => Promise<T>,
): Promise<T | null> {
    try {
        return await this.forwardToRangeRootPartition(ctx, hashKey, forward);
    } catch (e) {
        if (isPhantomBounceError(e)) {
            return null;
        }
        throw e;
    }
}
```

#### 2f. Refactor `withSplitForwarding`

Replace the current promotion-check block and add the bloom filter check. New flow:

```typescript
private async withSplitForwarding<T extends { meta: PartitionInfo }>(opts: { ... }): Promise<T> {
    const { ctx, keys: { hashKey, sortKey }, operationName, forward, local } = opts;

    if (isHashPartition(ctx)) {
        // Step 1: Authoritative promotion check (unchanged logic, uses new helper).
        const promotedStatus = this.#promotion.statusFor(hashKey);
        if (promotedStatus === "promoting" || promotedStatus === "promoted") {
            return await this.forwardToRangeRootPartition(ctx, hashKey, forward);
        }

        // Step 2: Speculative bloom filter check — learned promotions from descendants.
        const prt = this.#_partialRangeTopology;
        if (prt?.maybePromoted(hashKey)) {
            const result = await this.maybeForwardToRangeRootPartition(ctx, hashKey, forward);
            if (result) return result;
            // Bloom filter false positive — fall through to topology routing.
        }
    }

    // Step 3: Topology check (unchanged).
    const topology = this.ensureTopology(ctx);
    const decision = topology.shouldAllow(hashKey, sortKey);
    switch (decision) {
        case "ok":
            return await local();
        case "forward": {
            const { doId, partitionContext } = topology.pickChildPartition(ctx, hashKey, sortKey);
            const stub = this.env[ctx.ns].get(doId);
            const result = await forward(stub, partitionContext);
            topology.recordForwardResult(hashKey, ctx, partitionContext, result.meta.hashDepth);

            // Learn promoted keys from forwarded responses.
            if (isHashPartition(ctx)
                && PartitionIdHelper.isRangePartition(result.meta.servedByPartitionId)) {
                const prt = this.getOrCreatePartialRangeTopology();
                if (prt.learnPromotedKey(hashKey)) {
                    this.persistPartialRangeTopology();
                } else {
                    console.info({
                        ...this.logParams(),
                        message: "fokos/partition: partial range topology bloom filter is full, "
                            + "cannot learn promoted key.",
                        hashKey,
                    });
                }
            }

            return {
                ...result,
                meta: { ...result.meta, forwardCount: result.meta.forwardCount + 1 },
            } as T;
        }
        case "reject":
            throw new Error(
                `fokos/partition: partition exceeded its limits, please retry later `
                + `(${operationName}).`
            );
        default: {
            const _exhaustive: never = decision;
            invariant(
                false,
                `fokos/partition.withSplitForwarding: unexpected decision value: ${_exhaustive}`
            );
        }
    }
}
```

#### 2g. Batch sync in `runBackgroundWork`

In the `finally` block of `runBackgroundWork`, after the existing alarm/schedule logic, add the
batch sync for the partial range topology. This runs after both migration (inherited keys) and
drive() (newly promoted keys) have completed:

```typescript
// Sync promoted keys into the partial range topology bloom filter.
if (isHashPartition(this.pCtx())) {
	const promotedHashKeys = this.#promotion
		.snapshot()
		.filter(({ status }) => status === "promoting" || status === "promoted")
		.map(({ hashKey }) => hashKey);
	if (promotedHashKeys.length > 0) {
		const prt = this.getOrCreatePartialRangeTopology();
		if (prt.learnPromotedKeys(promotedHashKeys)) {
			this.persistPartialRangeTopology();
		}
	}
}
```

#### 2h. FIXME in `groupItemsByRouting`

Add a FIXME comment at the top of `groupItemsByRouting` noting that the bloom filter check should
be added for transaction routing after the initial implementation is validated:

```typescript
// FIXME: Add PartialRangeTopology bloom filter check for promoted keys in transaction routing
// (prepare/commit/readForTransaction). Currently only the authoritative PromotionManager is
// checked. The bloom filter would save hops for keys promoted by descendant partitions, but
// false positives need careful handling in multi-item transaction flows.
```

### 3. No changes to `src/lib/bloom-filter.ts`

The existing `BloomFilter` API (`create`, `fromSnapshot`, `add`, `has`, `toSnapshot`) is sufficient.

### 4. No changes to `src/lib/partition/hash-key-promotion.ts`

The `PromotionManager` is not aware of the bloom filter. The DO syncs from PM's snapshot at batch
boundaries instead of using callbacks.

---

## Implementation milestones

### Milestone 1: `PartialRangeTopology` class

**File:** `src/lib/partition-topology/partial-range-topology.ts` (new)

- Create `PartialRangeTopologySnapshot` type (versioned, wrapping `BloomFilterSnapshot`).
- Create `PartialRangeTopology` class with `create`, `fromSnapshot`, `maybePromoted`,
  `learnPromotedKey`, `learnPromotedKeys`, `toSnapshot`, `isEmpty`, `stats`.
- Unit tests for the class in isolation (add keys, check membership, snapshot round-trip,
  batch learn, full filter returns false).

**Verification:** Unit tests pass. No integration with DO yet.

### Milestone 2: DO storage and lifecycle

**File:** `src/lib/do-partition.ts`

- Add KV key constant, private field.
- Add startup loading in `blockConcurrencyWhile`.
- Add `getOrCreatePartialRangeTopology()` and `persistPartialRangeTopology()` helpers.

**Verification:** Existing tests pass. The topology is loaded/saved but not yet used for routing
or learning.

### Milestone 3: Extract routing helpers

**File:** `src/lib/do-partition.ts`

- Add `isPhantomBounceError()`.
- Add `forwardToRangeRootPartition()` and `maybeForwardToRangeRootPartition()`.
- Refactor the existing promotion-check block in `withSplitForwarding` to use
  `forwardToRangeRootPartition()` — pure refactor, same behavior.

**Verification:** Existing tests pass. No new routing behavior yet.

### Milestone 4: Bloom filter routing and learning

**File:** `src/lib/do-partition.ts`

- Add the bloom filter check (step 2) in `withSplitForwarding` between the promotion check and
  the topology check, using `maybeForwardToRangeRootPartition`.
- Add learning in the `"forward"` case: check `isRangePartition` on response, call
  `learnPromotedKey`, persist if modified.
- Add batch sync in `runBackgroundWork` finally block: iterate PM snapshot, call
  `learnPromotedKeys`, persist if modified.
- Add FIXME comment in `groupItemsByRouting`.

**Verification:** Existing tests pass. New behavior:

- Ancestor routers learn promoted keys from forwarded responses (per-request path).
- Ancestor routers learn promoted keys from PM state (batch path in background work).
- Ancestor routers skip child hops for learned promoted keys.
- False positives fall back to normal routing without errors.

### Milestone 5: Integration tests

Write focused tests for:

1. **PartialRangeTopology unit tests.** Add/check keys, snapshot round-trip, batch learn,
   full-filter behavior, `isEmpty`/`stats`.
2. **Bloom filter updated on local promotion.** A hash partition promotes a key; after background
   work runs, the partial range topology contains it.
3. **Bloom filter updated from forwarded response.** A grandparent forwards to a child, child
   serves via range root, grandparent's partial range topology now contains the key.
4. **Direct range root routing on cache hit.** After learning, the grandparent routes directly to
   the range root (verify forwardCount is 1 less than without the cache).
5. **False positive fallback.** Manually populate the bloom filter with a key that is NOT
   promoted. Verify the request still succeeds (falls back to hash-tree routing via
   phantom-bounce catch).
6. **Bloom filter survives DO restart.** Promote a key, run background work (syncs to bloom
   filter), simulate restart, verify the topology is restored and routing still works.
7. **Bloom filter full.** Fill the bloom filter to capacity, verify the info log fires and
   subsequent `learnPromotedKey` calls return false (no crash).
8. **Inherited keys populate bloom filter.** A child partition inherits promoted keys during
   migration; after background work runs, the child's partial range topology contains them.
9. **Batch sync is efficient.** Verify that migration of N promoted keys results in one
   snapshot persist (not N).

---

## Non-goals / deferred

- **Transaction routing via bloom filter.** Tracked by the FIXME in `groupItemsByRouting`. Will be
  addressed separately after validating the single-key routing path.
- **Sort key boundary cache.** `PartialRangeTopology` is designed to accommodate this later
  (versioned snapshot, class extension point). Not implemented in this phase.
- **Bloom filter compaction or resizing.** The layered bloom filter grows automatically; no
  shrinking or compaction is planned.
- **Shared/distributed bloom filter.** Each partition maintains its own topology independently.
  There is no cross-partition synchronization beyond learning from forwarded responses.
