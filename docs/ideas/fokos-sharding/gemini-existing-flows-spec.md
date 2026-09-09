# RFC — FokosDB Existing Sharding, Routing, and Lifecycle Architecture

**State:** Draft
**Date:** 2026-09-09
**Author:** Lambros Petrou

---

## 1. Table of Contents

- [1. Table of Contents](#1-table-of-contents)
- [2. Overview and Context](#2-overview-and-context)
- [3. Goals and Scope](#3-goals-and-scope)
- [4. Current Architecture and Component Map](#4-current-architecture-and-component-map)
- [5. Subsystem Flows and Durable Object Interactions](#5-subsystem-flows-and-durable-object-interactions)
  - [5.1 Partition Identity, Naming, and Context Management](#51-partition-identity-naming-and-context-management)
  - [5.2 Request Admission and Split Forwarding Pipeline](#52-request-admission-and-split-forwarding-pipeline)
  - [5.3 Topology Caching Subsystems](#53-topology-caching-subsystems)
  - [5.4 Partition Splitting Subsystem](#54-partition-splitting-subsystem)
  - [5.5 Data Migration Protocol](#55-data-migration-protocol)
  - [5.6 Hash Key Promotion Subsystem](#56-hash-key-promotion-subsystem)
  - [5.7 Background Scheduling and Alarms](#57-background-scheduling-and-alarms)
- [6. Operation Interaction Matrix](#6-operation-interaction-matrix)
- [7. Layer Boundaries and Required Extension Points](#7-layer-boundaries-and-required-extension-points)
  - [7.1 Domain Responsibility Split](#71-domain-responsibility-split)
  - [7.2 Required Extension Points and Lifecycle Hooks](#72-required-extension-points-and-lifecycle-hooks)
- [8. Simplification and Generalization Opportunities](#8-simplification-and-generalization-opportunities)
  - [8.1 Single-Step Migration with Opaque Cursors](#81-single-step-migration-with-opaque-cursors)
  - [8.2 Unified Child Partition Resolution and Provisioning](#82-unified-child-partition-resolution-and-provisioning)
  - [8.3 Generalized Context and Custom Domain Payloads](#83-generalized-context-and-custom-domain-payloads)
  - [8.4 Single Migration Acknowledgment Endpoint](#84-single-migration-acknowledgment-endpoint)
  - [8.5 Automated Request Interception Pipeline](#85-automated-request-interception-pipeline)
  - [8.6 Pluggable Admission and Backpressure Policies](#86-pluggable-admission-and-backpressure-policies)
  - [8.7 Generic Read-Through Fallback](#87-generic-read-through-fallback)
- [9. References](#9-references)

---

## 2. Overview and Context

FokosDB provides a DynamoDB-compatible database on top of Cloudflare Durable Objects. It includes global strong
consistency and distributed transactions.

The `PartitionDO` class (`packages/fokosdb/src/server/do-partition.ts`) currently mixes two different concerns:
1. The DynamoDB-like application logic: item storage, JSON expressions, transaction participant locks, and TTL sweeps.
2. The distributed partition lifecycle logic: partition routing, topology caching, automatic splitting, key promotions,
and inter-partition data migrations.

This document describes the existing flows and boundaries in `PartitionDO`. It provides the technical baseline needed to
extract the generic sharding and routing subsystem into an independent library package.

---

## 3. Goals and Scope

### In Scope
- Document all interactions between `PartitionDO` and the routing, sharding, splitting, migration, and promotion layers.
- Document how each public and transactional RPC interacts with these layers.
- Enumerate the data structures, state machines, and caching layers used for routing and splitting.
- Identify the extension points and lifecycle hooks needed to extract the sharding engine into a generic package.

### Out of Scope
- Implementation of the new generic package (specified in a separate RFC).
- Modifications to the 2-phase commit protocol or coordinator implementation.
- Changes to the external public FokosDB client API.

---

## 4. Current Architecture and Component Map

The current `PartitionDO` implementation combines application logic and partition lifecycle coordination:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Application Domain (DynamoDB Compatibility Layer)                                │
│                                                                                 │
│ - KeyCodec & Expression Engine (Compiler, Runtime, Condition Evaluator)         │
│ - Item Data Encoding (JSONB, raw text, raw bytes)                               │
│ - SQLite Storage Schema (items, deletion_metadata tables)                       │
│ - 2PC Transaction Participant (pending_transactions table, locks, recovery)    │
│ - TTL Sweep Engine (TtlExpiry)                                                  │
│ - Public & Transaction RPC Handlers (apiPutItem, apiGetItem, txPrepare, etc.)  │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │
                                         │ Calls via withSplitForwarding & Hooks
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Generic Partition Sharding, Routing & Lifecycle Subsystem                      │
│                                                                                 │
│ 1. Identity & Context (PartitionContext, PartitionIdHelper, DO naming)         │
│ 2. Routing Pipeline (PartitionTopologyRouter, withSplitForwarding, groupItems)  │
│ 3. Topology Caching (HashTopology arena, Range Ancestors, PartialRange Bloom)   │
│ 4. Split State Machine (SplitStateMachine, Hash & Range Split Policies)         │
│ 5. Data Migration Protocol (SplitMigration driver, batch streaming, acks)      │
│ 6. Key Promotion Subsystem (PromotionManager, hash-to-range cutover & GC)       │
│ 7. Background Scheduling & Alarms (runBackgroundWork, fallback alarms)          │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Subsystem Flows and Durable Object Interactions

### 5.1 Partition Identity, Naming, and Context Management

Cloudflare Workers RPC does not support constructor parameters for Durable Objects. Every RPC request therefore passes
a `PartitionContext` parameter.

#### Context Validation (`ensurePartitionContext`)
- The DO loads the stored `PartitionContextLivePartition` from KV key `__partition_context` during startup.
- On each RPC invocation, `ensurePartitionContext(pCtx)` validates that incoming immutable configuration matches the
  stored configuration (`tableName`, `rootTreesN`, `hashSplitN`, `rangeSplitN`, `ns`).
- If mutable settings differ, the DO updates its stored context.

#### Identity Schemas (`PartitionIdHelper`)
- **Hash Partition ID (`SCHEMA_HASH_V1 = 0x00`)**:
  - Byte layout: `[schemaVersion u8, rootIdx u16, depth u8, hashIdx_1 u8, ..., hashIdx_depth u8]`.
  - DO name format: `<tableName>.h.<rootIdx>[.<childIdx>...]`.
- **Range Partition ID (`SCHEMA_RANGE_V1 = 0x01`)**:
  - Byte layout: `[schemaVersion u8, flags u8, hkLen u32LE, startLen u32LE, hkBytes, startBytes, endBytes]`.
  - DO name format: `<tableName>.r.<encodedHk>.<encodedStart>.<encodedEnd>`.
  - Unbounded range boundaries use reserved sentinels `~min` and `~max`.
- **Phantom-Bounce Guard**:
  - Range DOs are created only through `internalInitFromSplit`.
  - A request reaching an uninitialized Range DO throws a phantom-bounce error. This causes the caller to route through
    the range root and traverse downward.

---

### 5.2 Request Admission and Split Forwarding Pipeline

`PartitionDO` routes single-item and multi-item requests using `withSplitForwarding`, `groupItemsByRouting`, and
`routeSingleDestination`.

```
Incoming Request (hashKey, sortKey, intent)
   │
   ▼
Is Hash Partition?
   ├─► Yes ──► Check PromotionManager: Is key 'promoting' or 'promoted'?
   │             ├─► Yes ──► Forward directly to Range Root (or cached range slice)
   │             └─► No  ──► Check PartialRangeTopology Bloom Filter
   │                           └─► Match? ──► Speculative forward to Range Root
   │                                           (Fallback to Hash on phantom bounce)
   ▼
Check Partition Topology: shouldAllow(hashKey, sortKey, intent)
   │
   ├─► "ok" ────────────────► Execute request locally in this DO
   │
   ├─► "forward" ───────────► 1. pickChildPartition(ctx, hashKey, sortKey)
   │                          2. Send RPC to target child DO
   │                          3. recordForwardResult(hashKey, ctx, childCtx, meta)
   │                          4. If target was Range DO: learn key into Bloom filter
   │
   ├─► "reject_over_size" ──► Throw 503 error (storage exceeds size cap + 10%)
   │
   └─► "reject_out_of_range"► Throw invalid routing error (bug in routing)
```

#### Operation Intents
- `"read"`: Does not increase partition size. Always allowed past size caps.
- `"write"`: Increases partition size. Rejected if database size exceeds cap + 10% grace allowance.
- `"delete"`: Decreases partition size. Always allowed past size caps to permit partition recovery.
- `"ignore_size_reject"`: Transaction commit or cancel. Locks are already accounted for; size reject is forbidden to
  prevent wedging committed transactions.

---

### 5.3 Topology Caching Subsystems

`PartitionDO` uses three caches to bypass intermediate router DO hops:

1. **Hash Tree Arena Cache (`HashTopology`)**:
   - Stored in KV (`__topo_cache`) as a `Uint32Array` flat arena.
   - Learns descendant relative depths from response `meta.hashDepth`.
   - `pickChildPartition` uses `findLeaf(hashKey)` to skip router levels and route directly to the
     deepest known hash leaf.
2. **Range Ancestor Cache (`range_hierarchy` table in SQLite)**:
   - Populated by `RangeAncestorInfo` propagated in `meta._internal.rangeAncestors`.
   - `findDeepestKnownRangeSlice(hashKey, sortKey)` jumps directly to the deepest known range slice,
     skipping the range root router chain.
3. **Partial Range Bloom Filter (`PartialRangeTopology`)**:
   - Stored in KV (`__partial_range_topology`) as a scalable Bloom filter.
   - Learns promoted hash keys when a hash partition receives a forward result from a Range DO.
   - Allows hash roots to forward speculative requests straight to range roots.

---

### 5.4 Partition Splitting Subsystem

#### Split Triggers
- Evaluated after write operations (`apiPutItem`, `txCommit`, `txExecuteSingleShot`) via `checkSplits`
  and `checkSplitsNoKey`.
- A split is queued when SQLite database size exceeds `maxSizeMb`.
- Mutual exclusion: Hash splits are blocked when key promotions are in flight
  (`PromotionManager.hasInFlightPromotions()`).

#### Split State Machine (`SplitStateMachine`)
1. **`split_queued`**:
   - Write split record to KV (`__split_status`).
   - Arm fallback alarm (`Date.now() + 5000ms`) and schedule background work.
2. **`split_started`**:
   - `runSplit` executes in the background.
   - For Range splits: compute $N-1$ split boundaries using `store.computeRangeSplitBoundaries`.
   - Resolve $N$ child partition contexts.
   - Send `internalInitFromSplit` RPC to all $N$ child DOs (with up to 5 retries each).
   - If all children initialize successfully: transition KV state to `split_started`.
   - Send `internalTriggerMigration` RPC to each child DO (fire-and-forget).
3. **Parent Router Mode**:
   - After `split_started`, the parent DO holds no active key ranges. It acts as a routing proxy.
4. **`split_completed`**:
   - Each child sends `migrationAcknowledgeChildComplete` after finishing migration.
   - When all $N$ children acknowledge: transition KV state to `split_completed` and delete parent
     `pending_transactions`.

---

### 5.5 Data Migration Protocol

Child partitions pull their data slice from their parent partition asynchronously after initialization.

```
Child PartitionDO                                         Parent PartitionDO
       │                                                          │
       │ ── migrationGetItemsBatch(childCtx, cursor) ───────────► │
       │ ◄─ { items: MigratedItem[], nextCursor } ─────────────── │
       │ (Applies items locally via insertItemIfAbsent)           │
       │ (Checkpoints cursor to KV: __split_migration_cursor)     │
       │                                                          │
       │ ── migrationGetPartitionTransactionMetadata(childCtx) ─► │
       │ ◄─ { pendingTransactions, maxDeletedTs, nextCursor } ─── │
       │ (Inserts pending locks & updates maxDeletedTs watermark) │
       │                                                          │
       │ ── migrationGetPromotedKeysBatch(childCtx) ────────────► │
       │ ◄─ { rows: PromotedKeyRow[], nextCursor } ────────────── │
       │ (Syncs promoted key records; Hash splits only)           │
       │                                                          │
       │ (Sets __split_migration_status = "migration_completed")  │
       │ (Deletes __split_migration_cursor from KV)               │
       │                                                          │
       │ ── migrationAcknowledgeChildComplete(childDoName) ─────► │
       │                                                          │ (Parent records ack;
       │                                                          │  Transitions to split_completed
       │                                                          │  when all children ack)
```

#### Migration Gates
- `ensureMigration("opName", throwIfMigrating = true)`:
  - Invoked at the start of write and 2PC transaction RPCs (`putItem`, `deleteItem`, `txPrepare`,
    `txCommit`, `txCancel`, `txReadForTransaction`, `txExecuteSingleShot`).
  - Throws a retryable 503 error if the partition status is `migration_initialized` or `migration_migrating`.
- `ensureMigration("opName", throwIfMigrating = false)`:
  - Invoked by `getItem` and `queryItems`.
  - If partition is migrating, reads directly from parent DO via `internalGetItemDirect` or `internalQueryItemsDirect`.

---

### 5.6 Hash Key Promotion Subsystem

A hash key that grows beyond `hashSplitConditions.maxSizeMb * RANGE_PROMOTION_FRACTION` is promoted to its own
autonomous Range DO tree.

```
Hash PartitionDO                                           Range Root DO
       │                                                         │
1. Queue Promotion (maybeQueuePromotion / debugForcePromoteKey)  │
   (Inserts row into promoted_keys table with status='queued')   │
       │                                                         │
2. Background Drive (PromotionManager.drive)                     │
   (Resolves Range Root context: db.r.<hk>.~min.~max)            │
   │ ── internalInitFromSplit(parentCtx, rangeRootCtx) ────────► │
   │                                                             │
3. Lock-Free Cutover                                             │
   (Checks pendingLockCountForHashKey(hk) === 0)                 │
   (Updates promoted_keys status: 'queued' -> 'promoting')       │
   │                                                             │
   │ ── internalTriggerMigration() ────────────────────────────► │
   │                                                             │ (Range Root pulls items
   │                                                             │  via migrationGetItemsBatch)
   │ ◄─ migrationAcknowledgePromotionComplete(hashKey) ───────── │
   │                                                             │
4. Mark Promoted                                                 │
   (Updates promoted_keys status: 'promoting' -> 'promoted')     │
       │                                                         │
5. Garbage Collection (PromotionManager.runGC)                   │
   (Deletes local items and pending locks for promoted key)      │
   (Deletes key size estimate when all items removed)            │
```

---

### 5.7 Background Scheduling and Alarms

`PartitionDO` coordinates background work through an in-memory timer (`scheduleBackgroundWork`) and durable alarms
(`ctx.storage.setAlarm`).

#### Alarm Execution (`alarm()` and `runBackgroundWork()`)
The background execution loop runs the following tasks sequentially:
1. **Partition Migration Job**: Executes `SplitMigration.runMigration()` for migrating child partitions.
2. **Partition Split Job**: Executes `runSplit()` if `SplitStateMachine` status is `split_queued`.
3. **Stale Transaction Recovery Job**: If `txPendingCanSweep()` is true, scans stale 2PC locks and queries the
   transaction coordinator.
4. **Promotion Drive and GC Jobs**:
   - `PromotionManager.drive()`: Advances queued promotions through initialization and cutover.
   - `PromotionManager.runGC()`: Sweeps residual local items for promoted keys.
5. **Next Alarm Computation**:
   - Evaluates earliest required wake-up time across all active tasks.
   - Updates `ctx.storage.setAlarm(nextAlarmMs)`.

---

## 6. Operation Interaction Matrix

The table below describes how each `PartitionDO` operation interacts with the routing, migration, and lifecycle layers:

| Operation | Migration Gate | Forwarding Intent | Promotion Checks | Post-Op Checks |
| :--- | :--- | :--- | :--- | :--- |
| **`apiPutItem`** | 503 if migrating | `write` | Auth + Bloom; Learn Range | Queue Promotion; `checkSplits` |
| **`apiDeleteItem`** | 503 if migrating | `delete` | Auth + Bloom; Learn Range | None |
| **`apiGetItem`** | Read parent if migrating | `read` | Auth + Bloom; Learn Range | None |
| **`apiQueryItems`** | Read parent if migrating | Range / Split Forward | Forward to Range Root | None |
| **`txPrepare`** | 503 if migrating | `write` | Forward to Range Root | Set stale TX alarm |
| **`txCommit`** | 503 if migrating | `commit` | Forward to Range Root | `checkSplitsNoKey` |
| **`txCancel`** | 503 if migrating | `cancel` | Forward to Range Root | None |
| **`txReadForTx`** | 503 if migrating | `read` | Forward to Range Root | None |
| **`txReadSnapshot`** | 503 if migrating | `read` (fast-path) | Forward to Range Root | None |
| **`txSingleShot`** | 503 if migrating | `write` (fast-path) | Forward to Range Root | `checkSplitsNoKey` |
| **`forcePromote`** | 503 if migrating | Local execution | Force queue promotion | Schedule work |
| **`destroyPartition`**| None | Tree walk via router | None | None |

---

## 7. Layer Boundaries and Required Extension Points

### 7.1 Domain Responsibility Split

To extract the sharding and routing framework, responsibilities are divided as follows:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Generic Sharded Durable Object Framework                                        │
│                                                                                 │
│ - Opaque partition IDs, hierarchy encoding, and DO naming                       │
│ - Client routing engine (pickPartition, root enumeration, tree traversal)       │
│ - Request admission control and forwarding (shouldAllow, withSplitForwarding)   │
│ - Topology caches (Hash arena tree, Range hierarchy, Bloom filter)              │
│ - Split lifecycle state machine (split_queued -> split_started -> completed)    │
│ - Child DO provisioning (initFromSplit) and acknowledgment tracking             │
│ - Migration driver loop (pull streaming, cursor persistence, acknowledgments)   │
│ - Key promotion state machine (initialization, cutover, migration ack, GC)      │
│ - Background alarm scheduling and fallback timer loops                          │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │
                                         │ Uses Adapters & Hooks
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Custom Application Durable Object (e.g. FokosDB PartitionDO)                    │
│                                                                                 │
│ - Storage schema, tables, and item serialization (SQLite, KV)                   │
│ - Application CRUD operations and expressions (Put, Get, Delete, Query)         │
│ - Transaction concurrency and locking (2PC participant, single-shot)            │
│ - Application background tasks (TTL expiry, transaction recovery)               │
│ - Exporting and importing data batches during migration                         │
│ - Calculating split boundaries from stored application data                     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

### 7.2 Required Extension Points and Lifecycle Hooks

A custom Durable Object will provide the following interfaces and hooks to the generic sharding framework:

#### 1. Storage and Context Adapter
- Provides access to Durable Object storage (`ctx.storage.kv` and `ctx.storage.sql`).
- Stores and loads partition context, migration checkpoints, and topology cache snapshots.
- Lets the generic framework persist its own KV keys without knowing the application schema.

#### 2. Admission and Split Policy Hooks (`PartitionAdmissionPolicy`)
- `evaluateAdmission(key, intent)`: Evaluates whether the partition can accept the operation or should return
  `"reject_over_size"` or custom backpressure.
- `shouldSplit(inputs)`: Determines whether partition storage size, item counts, or custom metrics exceed
  split thresholds.

#### 3. Split Boundary Calculator Hook (`RangeSplitStrategy`)
- `computeSplitBoundaries(hashKey, startBoundary, endBoundary, splitCount)`: Returns $N-1$ key boundaries from
  application data to partition a range slice evenly.

#### 4. Data Migration Adapter (`PartitionMigrationAdapter<TCursor, TPayload>`)
- **Export (Parent)**:
  - `exportDataBatch(slice, cursor, budgetBytes)`: Streams an opaque page of application records and metadata for the
    child partition, together with an opaque continuation cursor.
- **Import (Child)**:
  - `importDataBatch(payload)`: Writes imported application data and metadata into local storage with idempotency.
  - `onMigrationComplete()`: Invoked when the migration loop finishes to rebuild local indexes, caches, or statistics.

#### 5. Key Promotion Adapter (`KeyPromotionAdapter`)
- `checkPromotionEligibility(hashKey, keyEstimatedBytes)`: Checks if a key qualifies for promotion to an autonomous
  Range DO.
- `canCutoverPromotion(hashKey)`: Verifies that the key has zero active transaction locks before cutover.
- `deleteLocalPromotedKeyData(hashKey, batchLimit)`: Deletes local application rows during garbage collection.

#### 6. Lifecycle and Alarm Hooks (`PartitionLifecycleHooks`)
- `onPostWrite(key, writeMetrics)`: Invoked after writes to evaluate split conditions or key promotion.
- `onCustomBackgroundWork()`: Invoked during background alarm passes to execute application background tasks
  (TTL sweeps, transaction recovery).
- `onCustomAlarmEvaluation()`: Allows the application layer to request earlier alarm execution times.

---

## 8. Simplification and Generalization Opportunities

The existing implementation in `PartitionDO` contains optimizations and multi-step protocols tailored specifically to
FokosDB. When extracting the generic sharding and routing layer, several flows can be generalized and simplified.

### 8.1 Single-Step Migration with Opaque Cursors

#### Current Implementation
The current migration flow executes three distinct RPC methods with separate while-loops in sequence:
1. `migrationGetItemsBatch`: Streams application items with `ScanCursor`.
2. `migrationGetPartitionTransactionMetadata`: Streams 2PC locks and watermarks with `PendingTransactionCursor`.
3. `migrationGetPromotedKeysBatch`: Streams promoted key forward-pointers with `PromotedKeyCursor`.

#### Generalized Design
The generic sharding framework does not need knowledge of application tables, transaction locks, or metadata.
Instead, it can use a single unified migration RPC:
```typescript
migrationPullBatch(req: {
  childCtx: PartitionContextResolved;
  cursor: TCursor | null;
  budgetBytes: number;
}): Promise<{ payload: TPayload; nextCursor: TCursor | null }>
```

The generic migration driver manages the pull loop, KV cursor checkpointing, and retry handling. The custom Durable
Object controls what data to stream. The custom DO encodes its multi-phase progression into the opaque cursor:
- Example composite cursor: `{ phase: "items", innerCursor: "..." }` transitioning to
  `{ phase: "locks", innerCursor: "..." }`.
- When all phases complete, the custom DO returns `nextCursor: null`.
- The generic driver calls `adapter.importDataBatch(payload)` on each batch, then signals completion.

### 8.2 Unified Child Partition Resolution and Provisioning

#### Current Implementation
`PartitionDO` uses separate code paths for Hash splits (`resolveHashChildPartitionContexts`) and Range splits
(`prepareSplit` with `boundaries` and `selectRangeAncestors`).

#### Generalized Design
The generic framework can provide a unified child partition resolution pipeline:
- For Hash splits: The framework deterministically computes the $N$ child partition IDs and DO names.
- For Range splits: The custom DO provides the split boundaries via `RangeSplitStrategy.computeSplitBoundaries()`. The
  framework then constructs the child partition contexts.
- The framework coordinates the `internalInitFromSplit` RPC fan-out, retry policies, and error rollbacks uniformly for
  all split types.

### 8.3 Generalized Context and Custom Domain Payloads

#### Current Implementation
`PartitionContext` contains FokosDB-specific fields: `rootTreesN`, `hashSplitN`, `rangeSplitN`, `hashSplitConditions`,
`rangeSplitConditions`, `rangeAncestorsConfig`, `tableName`, `ns`, and `nsTx`.

#### Generalized Design
The base `PartitionContext` should contain only generic topology and identity fields:
- Core fields: `partitionId`, `doName`, `primaryDoIdStr`, `ns`, `schemaVersion`, and topology routing parameters.
- Extensible custom context: `customContext: TCustomContext` for application-specific parameters.
- Context validation: The generic layer validates topology fields; the custom DO validates its custom context payload.

### 8.4 Single Migration Acknowledgment Endpoint

#### Current Implementation
Two separate acknowledgment RPCs exist on the parent PartitionDO:
- `migrationAcknowledgeChildComplete(childDoName)` for hash and range splits.
- `migrationAcknowledgePromotionComplete(hashKey)` for hash key promotions.

#### Generalized Design
Both splitting and key promotion are core features of the generic sharding framework. The custom application code
does not manage migration acknowledgments.

The framework provides a single internal acknowledgment RPC:
- `migrationAcknowledgeComplete(ackPayload: MigrationAckPayload): Promise<void>`

The generic sharding engine in the parent DO handles the acknowledgment automatically:
- **For Split Children**: The framework updates the `SplitStateMachine`. When all child partitions acknowledge,
  the parent transitions to `split_completed` and invokes the optional `onSplitCompleted()` hook on the custom DO.
- **For Promoted Keys**: The framework updates the `PromotionManager` state from `promoting` to `promoted` and schedules
  background garbage collection.
- The custom application DO does not implement acknowledgment dispatching. It only supplies optional lifecycle hooks
  if it needs to clean up local state after a split or promotion completes.

### 8.5 Automated Request Interception Pipeline

#### Current Implementation
Every public and internal RPC in `PartitionDO` manually repeats boilerplate calls:
- Context checking: `this.ensurePartitionContext(pCtx)`.
- Migration checking: `await this.ensureMigration("opName")`.
- Split forwarding: `await this.withSplitForwarding(...)` or `this.groupItemsByRouting(...)`.
- Topology learning: `topology.recordForwardResult(...)`.

#### Generalized Design
The framework can provide a unified request execution wrapper:
```typescript
await this.shardingEngine.executeRoutedRequest({
  ctx: pCtx,
  keys: { hashKey, sortKey },
  intent: "write",
  operationName: "putItem",
  local: async () => this.localPutItem(pCtx, req),
  forward: async (stub, childCtx) => stub.apiPutItem(childCtx, req),
});
```
This wrapper automatically executes:
1. Context validation.
2. Migration gating (throwing 503 or executing parent read-through).
3. Promotion checks and speculative Bloom filter routing.
4. Split forwarding and topology cache learning.
5. Post-operation split checks and background scheduling.

### 8.6 Pluggable Admission and Backpressure Policies

#### Current Implementation
Admission control hardcodes a check against SQLite database size: `dbSize > maxSizeMb * 1.1 * 1024 * 1024`.

#### Generalized Design
The custom DO provides an implementation of `PartitionAdmissionPolicy`:
- Allows admission decisions based on any metric: SQLite database size, KV storage count, memory usage, CPU limits, or
  custom rate limits.
- Supports custom rejection reasons and error types that travel back to the client.

### 8.7 Generic Read-Through Fallback

#### Current Implementation
`apiGetItem` and `apiQueryItems` manually check `ensureMigration(..., false)` and call `internalGetItemDirect` or
`internalQueryItemsDirect` on the parent DO.

#### Generalized Design
The request interception wrapper can automate read-through routing:
- When a partition is migrating and the operation intent is `"read"`, the framework automatically forwards the read
  request directly to the parent DO's direct read endpoint if read-through is enabled.
- The custom DO does not need custom branches inside its read handlers.

---

## 9. References

- [Amazon DynamoDB: A Scalable, Predictably Performant NoSQL Database (USENIX ATC 2022)](
  https://www.usenix.org/system/files/atc22-elhemali.pdf)
- [Distributed Transactions at Scale in Amazon DynamoDB (USENIX ATC 2023)](
  https://www.usenix.org/system/files/atc23-idziorek.pdf)
- Cloudflare Workers and Durable Objects Documentation:
  [https://developers.cloudflare.com/durable-objects/](https://developers.cloudflare.com/durable-objects/)
- FokosDB Partition Implementation: `packages/fokosdb/src/server/do-partition.ts`
- FokosDB Partition Topology Implementation: `packages/fokosdb/src/shared/partition-topology/`

