# RFC — Coalesced materialization for global secondary indexes and push targets

**State:** Draft
**Date:** 2026-10-17
**Author:** Lambros Petrou
**Status:** The delete revision `v + 1` (Section 4.2.2) and Milestone 1 are built. Nothing else is built. This document specifies
global secondary indexes (GSIs) and push targets.

## Table of Contents

- [1. Overview and Context](#1-overview-and-context)
  - [1.1 The current system](#11-the-current-system)
  - [1.2 The problems](#12-the-problems)
  - [1.3 Facts that constrain the design](#13-facts-that-constrain-the-design)
  - [1.4 Glossary](#14-glossary)
- [2. Goals and Requirements](#2-goals-and-requirements)
  - [2.1 In scope](#21-in-scope)
  - [2.2 Out of scope](#22-out-of-scope)
  - [2.3 Requirements](#23-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed Solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
    - [4.2.1 Shard groups, bindings, and routing](#421-shard-groups-bindings-and-routing)
    - [4.2.2 Monotonic base revisions](#422-monotonic-base-revisions)
    - [4.2.3 Base mutation capture and export](#423-base-mutation-capture-and-export)
    - [4.2.4 `MaterializationDO` storage and ingest](#424-materializationdo-storage-and-ingest)
    - [4.2.5 Change positions](#425-change-positions)
    - [4.2.6 Target runner and target step](#426-target-runner-and-target-step)
    - [4.2.7 GSI targets and key sets](#427-gsi-targets-and-key-sets)
    - [4.2.8 Push targets](#428-push-targets)
    - [4.2.9 Producer epochs and fences](#429-producer-epochs-and-fences)
    - [4.2.10 Failed items and permanent violations](#4210-failed-items-and-permanent-violations)
    - [4.2.11 Tombstones and garbage collection](#4211-tombstones-and-garbage-collection)
    - [4.2.12 Splits and promotions](#4212-splits-and-promotions)
    - [4.2.13 Target definitions and table destroy](#4213-target-definitions-and-table-destroy)
    - [4.2.14 GSI tables, composite sort key, and queries](#4214-gsi-tables-composite-sort-key-and-queries)
    - [4.2.15 Backpressure, circuit breaking, and cost](#4215-backpressure-circuit-breaking-and-cost)
    - [4.2.16 Item lifecycle](#4216-item-lifecycle)
  - [4.3 Future Follow-ups](#43-future-follow-ups)
    - [4.3.1 Online target backfill](#431-online-target-backfill)
    - [4.3.2 `FokosDBAutoAdminDO` catalog and activation status](#432-fokosdbautoadmindo-catalog-and-activation-status)
    - [4.3.3 Per-GSI topology and key-limit overrides](#433-per-gsi-topology-and-key-limit-overrides)
    - [4.3.4 Capture on a table that has data](#434-capture-on-a-table-that-has-data)
    - [4.3.5 Base version on GSI queries](#435-base-version-on-gsi-queries)
    - [4.3.6 Target changes and target versions](#436-target-changes-and-target-versions)
- [5. Alternative Options](#5-alternative-options)
  - [5.1 Base outbox, base shadow, and stateless Queue fan-out](#51-base-outbox-base-shadow-and-stateless-queue-fan-out)
  - [5.2 Separate `GsiMaintainerDO` worker tier](#52-separate-gsimaintainerdo-worker-tier)
  - [5.3 Pull cursors (`change_seq`) for push targets](#53-pull-cursors-change_seq-for-push-targets)
  - [5.4 One sidecar `MaterializationDO` for each base `PartitionDO`](#54-one-sidecar-materializationdo-for-each-base-partitiondo)
  - [5.5 A hash or a local integer ID in the GSI sort key](#55-a-hash-or-a-local-integer-id-in-the-gsi-sort-key)
  - [5.6 Per-item target state with a write deadline fence](#56-per-item-target-state-with-a-write-deadline-fence)
  - [5.7 Compact per-item target state with writes only on a GSI key change](#57-compact-per-item-target-state-with-writes-only-on-a-gsi-key-change)
  - [5.8 Versioned GSI tombstones with a TTL instead of GSI-hop epochs](#58-versioned-gsi-tombstones-with-a-ttl-instead-of-gsi-hop-epochs)
  - [5.9 A time limit for the split fences of a target](#59-a-time-limit-for-the-split-fences-of-a-target)
- [6. Frequently Asked Questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and Context

### 1.1 The current system

FokosDB has no global secondary index (GSI) and no push connector for external systems such as a data lake. Today, a
caller reads items only by the base table key `(hashKey, sortKey)`. The read operations are `getItem`,
`batchGetItems`, `transactGetItems`, and `queryItems`.

Each base partition is a `PartitionDO` in the shard group `fokos.p.<tableName>`. It hosts `FokosShardingRuntime`
(`packages/fokosdb/src/sharding/runtime.ts`). It stores committed rows in SQLite through `PartitionStore`
(`packages/fokosdb/src/shared/partition/partition-store.ts`).

Each row in `items` has a version `v`. Each partition has one `deletion_metadata` row (`id = 1`) with
`max_delete_tx_order_ts` and `max_deleted_v`:

- A delete or a TTL expiry of a row at `v` has the delete revision `v + 1`.
- `max_deleted_v` is the highest delete revision of the partition.
- A new item row starts at `v = max_deleted_v + 1`. Each update to an existing row sets `v = v + 1`.
- Thus the revisions of a key (each `v` and each delete revision) always increase, also after a delete and a
  recreate.
- A split or a key promotion merges `max_deleted_v` onto the target partition with `MAX` in `FokosMigrationHost`
  (`packages/fokosdb/src/shared/partition/fokos-migration-host.ts`).

### 1.2 The problems

1. **Secondary access patterns need up to 10 GSIs per table.** Each GSI extracts its own partition key, an optional
   sort key, and a projection from the base item.
2. **External systems need a coalesced push stream.** A push target writes to a data lake or an external service. It
   must receive the newest state of each changed item. The availability of base writes must not depend on the push
   target.
3. **A GSI needs the old GSI key to move an entry.** When an item changes its GSI key from `A` to `C`, the maintainer
   must delete `A` and write `C`. The new item image does not contain `A`.
4. **Base items move between `PartitionDO` instances.** A hash split, a key promotion, or a range split moves an item
   to a new `PartitionDO`. An old source partition can hold an unexported revision while the new owner exports a newer
   revision. Downstream targets must not depend on the delivery order between base partitions.
5. **Downstream outages must not grow base partitions without a bound.** A GSI partition or a push target can be slow
   or unavailable for hours. The base write path must stay one local SQLite transaction. Base storage must not copy
   400 KB item payloads for each mutation or for each target.

### 1.3 Facts that constrain the design

- **An item is up to 400 KB** (`MAX_ITEM_BYTES` in `packages/fokosdb/src/shared/transaction-limits.ts`). Workers RPC
  supports payloads up to 32 MiB. Cloudflare Queues caps a message at 128 KB, so a queue message cannot carry a full
  FokosDB item.
- **Keys can be multiple kilobytes.** By default, `MAX_HASH_KEY_BYTES` is 1,024 bytes and `MAX_SORT_KEY_BYTES` is 512
  bytes. A table policy can raise both limits.
- **`last_write_ts` does not strictly increase for each item.** `PartitionStore.upsertItem` sets
  `last_write_ts = MAX(last_write_ts, txOrderTs)`. A two-phase transaction stamps the coordinator clock, and
  `prepare` accepts that clock up to `maxClockSkewMs` ahead. A later non-transactional write in that skew window keeps
  the same `last_write_ts`. Thus `last_write_ts` cannot order last-writer-wins (LWW) updates across retries.
- **A `WITHOUT ROWID` table bloats large payloads in Durable Object SQLite.** On a 4 KiB SQLite page, a
  `WITHOUT ROWID` table puts each row above 1,002 bytes onto a private overflow page. The storage amplification
  measured in `PartitionStore` is 3.1x to 4.7x. Each table that stores item payloads or multiple multi-kilobyte keys
  must be a rowid table.
- **The TTL sweep deletes unowned copies today.** `PartitionStore.deleteExpiredItems` removes expired rows and does
  not check `this.fokos.ownerCheck()`. A promotion source can delete an expired copy before the range root pulls it.
  Then no partition writes a tombstone for that item.
- **Durable Object alarms are too slow and cost too much to drain one batch per alarm.** Each `setAlarm` writes
  durable storage and is billed as one row written. A drain at a high throughput needs an in-memory background loop
  that runs while due work exists. The Durable Object alarm is armed only as the fallback after a crash or an
  eviction.
- **Rows written cost 1,000 times more than rows read.** On the Durable Objects SQLite storage backend, a row written
  costs $1.00 per million rows, and a row read costs $0.001 per million rows.

### 1.4 Glossary

| Term | Meaning |
| --- | --- |
| Base item | One item in the user table, identified by `(baseHashKey, baseSortKey)`. |
| Base revision | The strictly increasing integer revision of one base item across writes, deletes, and splits. |
| Delete revision | The base revision of a delete or a TTL expiry of a row at `v`: `v + 1`. |
| Outbox row | The coalesced `materialization_outbox` row in a base `PartitionDO` for one changed base item. |
| Capture | The write of `materialization_outbox` rows and their export, enabled by `captureChanges`. |
| Materialization partition | One `MaterializationDO` in the shard group `fokos.m.<tableName>`. |
| Latest image | The newest live image or tombstone that `MaterializationDO` accepted for a base item. |
| Tombstone | A delete record with no payload: an outbox row, a `latest_items` row, or an export entry. |
| Target | A registered downstream destination in `MaterializationDO`: a GSI or a push target. |
| Push target | A target of kind `push_sink`: a `WorkerEntrypoint` or a `DurableObject` that gets batches over Workers RPC. |
| Change position | A row of `change_positions` that points to one changed item. All targets share the positions. |
| Cursor (`C_t`) | `materialization_targets.cursor_seq` of target `t`. Each position at or below `C_t` is done for `t`. |
| Read position (`R_t`) | In memory: the highest position that the batch in flight of target `t` read, or `C_t`. |
| Applied map | An in-memory map `item_id -> revision` of the revisions that a target applied in its current epoch. |
| Key set | The `target_index_keys` row of one item and one GSI: each GSI key that can have an entry of the item. |
| Producer | The sender on a hop: a base `PartitionDO` for the export, or a `MaterializationDO` for a target. |
| Receiver | The partition that applies a call: a `MaterializationDO`, a GSI `PartitionDO`, or a push target. |
| Producer epoch | An integer that a producer stores for each hop and sends with each call. It only goes up. |
| Ancestor fence | `(ancestorId, minEpoch)`: a child sends it so that each receiver rejects the late calls of a parent. |
| Inherited work | The work that a parent can have in flight at a split cutover, copied to the child with the slice. |
| Violation | A base item revision whose target write fails permanently, for example a GSI key of a type that is not valid. |
| Unowned copy | A row that a split or a promotion moved to another partition. The source keeps it until `cleanupSourceStep`. |
| `gsiSK` | The encoded GSI sort key of an item, or `x''` when the GSI has no sort key. |
| `physicalSk` | The sort key that a GSI table stores: the escaped `gsiSK`, `baseHK`, and `baseSK`, and a trailer. |

## 2. Goals and Requirements

### 2.1 In scope

- Support up to 10 GSIs per table. Each GSI defines its own partition key, optional sort key, and attribute
  projection.
- Support push targets (a `WorkerEntrypoint` or a `DurableObject` called over Workers RPC) from the same target
  runner in `MaterializationDO`.
- Define GSIs and push targets in a subclass of `MaterializationDO` that overrides `targets()`.
- When `captureChanges = true`, capture one coalesced outbox marker in the base `PartitionDO`, in the same SQLite
  transaction as each item mutation.
- Route the materialization state by `(baseHashKey, baseSortKey)` in an independent shard group
  `fokos.m.<tableName>`.
- Drain outbox exports in `PartitionDO` and target pushes in `MaterializationDO` with an in-memory background loop.
  `MaterializationDO` round-robins the targets with 2 parallel workers. The Durable Object alarm is only for recovery
  after a crash or an eviction.
- Keep a full mirror of live items in `MaterializationDO` (`latest_items.data`). The mirror supports target
  projections, retries, and future online target backfills (Section 4.3.1).
- Isolate failures between targets. One unavailable GSI or push target must not stop healthy targets or base writes.
- Write a `MaterializationDO` row for each item and each target only when a GSI key changes or when an item fails.

### 2.2 Out of scope

- A full change data capture (CDC) history of each intermediate version. Intermediate writes before consumption
  coalesce into the latest image.
- Pull-based stream cursors for external consumers. All targets use push delivery from `MaterializationDO`.
- Strongly consistent GSI reads, or atomic visibility of multiple items across a GSI.
- Prevention of temporary duplicate entries during a GSI key move across partitions. The GSI step writes the new entry
  before it deletes the old entry, as in the eventually consistent GSIs of DynamoDB.
- Uniqueness constraints on a GSI key. A caller that needs a unique secondary key must use `transactWriteItems` on the
  base table.
- Online backfill of targets that are added after data exists, cluster-wide activation tracking through
  `FokosDBAutoAdminDO`, per-GSI topology overrides, capture on a table that has data, a base version on GSI
  queries, and changes to the target list. Section 4.3 tracks them.

### 2.3 Requirements

1. A delete of a present item at `v` must have the delete revision `v + 1` and must raise `max_deleted_v` to at least
   `v + 1`.
2. When `captureChanges` is `true`, the base partition must write the item mutation and the `materialization_outbox`
   row in one `transactionSync`. When `captureChanges` is `false`, the partition must write no `materialization_outbox`
   row. A GSI shard group must always have `captureChanges = false`.
3. The `materialization_outbox` row must store only the key, the revision, and retry metadata. It must never store a
   copy of the item payload.
4. A transaction `prepare`, a failed condition, a `check`, a `cancel`, a delete of an absent item, or a migration page
   import must not write a new `materialization_outbox` revision.
5. A TTL sweep must delete only the expired rows that its partition owns. When `captureChanges = true`, it must write
   an outbox tombstone for each deleted row. It must keep unowned copies until `cleanupSourceStep` deletes them.
6. Each GSI must be a standard FokosDB table (`PartitionDO`) written with `batchWriteItems` and queried with
   `queryItems` over the composite sort key. The physical key of each valid base item must fit the derived key limits
   of each GSI table.
7. Each background scan, export batch, and target push step must enforce bounds on the item count, the byte count, and
   the duration.
8. `FokosDB.destroy()` must destroy each shard group of the table in dependency order.
9. The correctness of the delivery to `MaterializationDO` and to each target must not depend on a clock, a timeout,
   or a time horizon.

## 3. Milestones

### Milestone 1 — DONE — The TTL sweep deletes only owned rows
- Change `PartitionStore.deleteExpiredItems` to filter its expired, unlocked candidates with
  `this.fokos.ownerCheck()` and to delete only the owned rows. Unowned copies stay until `cleanupSourceStep` deletes
  them (Section 4.2.3, the TTL sweep item).
- Update the comment of `deleteExpiredItems`, which says that the sweep does not test the owner of each row. Update the
  unit tests and the promotion tests. This milestone ships on its own before any materialization code.

### Milestone 2: Base `materialization_outbox`, export epochs, and `MaterializationDO` mirror
- Add `nsMat` to `FokosTableIdentity` and `FokosDBPolicy`. Add `captureChanges` to `FokosTableOptions` and
  `FokosDBPolicy` (Sections 4.2.1 and 4.2.3).
- Add `materialization_outbox` and `materialization_export_state` to `PartitionStore`. Add the outbox writes to
  `upsertItem`, `updateItemSingleShot`, `deleteItem`, `TransactionParticipant.commitLocal`,
  `TransactionParticipant.executeSingleShot`, and the owned rows of `deleteExpiredItems` (Section 4.2.3).
- Build the in-memory export loop with its alarm fallback, the in-flight key set of the same-epoch rule, `is_call_open`,
  and the epoch raise after an `unknown` outcome (Sections 4.2.3, 4.2.6, and 4.2.9).
- Create `MaterializationDO` in the shard group `fokos.m.<tableName>` with `latest_items`, `key_size_estimates`,
  `producer_fences`, `ancestor_fences`, and `applyLatestImages` (`group` dispatch with `failurePolicy: "collect"`)
  with the receiver rules (Sections 4.2.4 and 4.2.9).
- Migrate `materialization_outbox` rows and the export fences with the `until_id` mark in `FokosMigrationHost`. Add the
  fence removal check to each delete of an outbox row. Delete unowned outbox copies in `cleanupSourceStep` (Sections
  4.2.3, 4.2.9, and 4.2.12).

### Milestone 3: Target runner and push targets
- Add the `MaterializationDO.targets()` method override and the stored target list `__fokosdb/mat/targets`
  (Section 4.2.13).
- Build these parts (Sections 4.2.5 to 4.2.12):
  - `change_positions`, the ingest steps, and the position cleanup,
  - `materialization_targets` with the cursor, the epoch, and `is_batch_open`,
  - the in-memory round-robin target runner with 2 parallel workers, the target step, and the applied map,
  - `push_sink` targets with `PushTargetBatch`,
  - `target_retry` with `idx_target_retry_inherited`, violation handling, and the circuit breaker of each target,
  - the tombstone GC,
  - the `MaterializationDO` split and promotion migration with the split fences.

### Milestone 4: Static GSIs and `db.gsi(indexName)`
- Add `producerId`, `epoch`, `ancestorFences`, and `producer_epoch_fenced` to `batchWriteItems`. Add
  `producer_fences` to the GSI `PartitionDO` and to its split migration (Sections 4.2.9 and 4.2.14).
- Build the GSI target step with `target_index_keys` and two calls in sequence (`put` and then `delete`) with
  `FokosDB.batchWriteItems` (Section 4.2.7).
- Add the composite sort key codec (`<L><flag><D>`), the `{ ref: "sortKey" }` compiler rules, the derived GSI key
  limits, `db.gsi(indexName).queryItems`, and `FokosDB.destroy()` across all shard groups of the table (Sections
  4.2.13 and 4.2.14).

### Milestone 5: Circuit breaker tuning
- Tune the circuit breaker values (the failure count that opens a breaker, and `probeIntervalMs`) with measurements.

## 4. Proposed Solution

### 4.1 High-level overview

A table with GSIs or push targets runs independent `FokosShardingRuntime` shard groups. `FokosShardingClient` connects
them:

```text
Base Shard Group: fokos.p.<tableName> (PartitionDO)
  - Stores committed items + coalesced materialization_outbox markers (no payload copy)
  - In-memory export loop sends dirty items by (baseHK, baseSK); alarm is fallback only
  - Producer: base DO ID + export epoch
          |
          | applyLatestImages(producerId, epoch, ancestorFences): batched group RPC, failurePolicy: "collect"
          v
Materialization Shard Group: fokos.m.<tableName> (MaterializationDO)
  - Routed by (baseHK, baseSK); the first targets() result is stored and fixed
  - Full mirror of live items in latest_items (1 payload copy per live item)
  - change_positions shared by all targets; 1 cursor and 1 epoch per target
  - target_index_keys (GSI keys that can hold an entry), target_retry (failed items)
  - Receiver: producer_fences; in-memory runner round-robins targets (2 workers); alarm is fallback
          |
          +-----------------------------------------------+
          | batchWriteItems(producerId, epoch,            | PushTargetBatch(producerId, epoch,
          |   ancestorFences)                             |   ancestorFences, items)
          v                                               v
GSI Shard Groups (up to 10):                       Remote Push Targets:
  fokos.gsiv1.<n>.<tableName>.<indexName>            Workers RPC to a WorkerEntrypoint or DurableObject
  - Standard FokosDB table (PartitionDO)             - Drops each revision <= its applied revision
  - Receiver: producer_fences
```

1. **Base write (`PartitionDO`).** When `captureChanges = true`, each committed write, delete, or owned TTL expiry
   updates `items` and upserts a compact marker into `materialization_outbox` in one `transactionSync`. Then it wakes
   the in-memory export loop.
2. **Export (`PartitionDO -> MaterializationDO`).** The export loop claims the owned, due outbox rows and reads the
   live payloads from `items`. It sends batched `applyLatestImages` RPCs to `fokos.m.<tableName>`. `MaterializationDO`
   checks the epoch of the sender and then the revision. It updates `latest_items`, gives the item a change position,
   and wakes its in-memory target runner. It acknowledges each item in the same call.
3. **Target push (`MaterializationDO -> GSI or push target`).** One in-memory background loop in `MaterializationDO`
   round-robins across the targets with 2 parallel workers. Each target has at most 1 batch in flight. A target reads
   the change positions above its cursor, sends the latest image of each item, and moves its cursor one time for each
   batch.
   - For a GSI, the step records each new GSI key in the key set before the call. It writes the new entry with
     `FokosDB.batchWriteItems`, and then deletes the old entry when the key moved.
   - For a push target, the step pushes a batch of `PushTargetItem` records over Workers RPC.
   - A failed item gets a `target_retry` row, and the cursor continues past it.
4. **Late calls.** Each call carries the ID and the epoch of its sender. A sender raises its epoch after an `unknown`
   outcome, at a cold start when a call can still be in flight, and at a split. A receiver rejects a call whose epoch
   is below the epoch that it stored for that sender. Thus a late call cannot overwrite newer state, and no
   correctness rule depends on a clock.

### 4.2 Technical details

#### 4.2.1 Shard groups, bindings, and routing

`FokosTableIdentity` and `FokosDBPolicy` (`packages/fokosdb/src/shared/partition-context.ts`) add the optional
`MaterializationDO` namespace binding key `nsMat?: MaterializationNamespaceKey`. `createTableConfig` needs `nsMat`
when `captureChanges` is `true`. `shared/do-stubs.ts` adds `materializationNamespace` and
`materializationStubByName`. They apply `jurisdiction` and `locationHint` as `partitionStubByName` does.

A table `orders` with 3 GSIs runs these Durable Object shard groups:

| Shard group | DO class | Binding | Route key `(hk, sk)` | Split trigger |
| --- | --- | --- | --- | --- |
| `fokos.p.orders` | `PartitionDO` | `policy.ns` | `(baseHK, baseSK)` | `items` + `pending_transactions` bytes |
| `fokos.tc.orders` | `TransactionCoordinatorDO` | `policy.nsTx` | `(clientRequestToken, "")` | Ledger bytes |
| `fokos.m.orders` | `MaterializationDO` | `policy.nsMat` | `(baseHK, baseSK)` | `latest_items` + target state bytes |
| `fokos.gsiv1.6.orders.g1` | `PartitionDO` | `policy.ns` | `(g1HK, physicalSk)` | GSI-1 entry bytes and key skew |
| `fokos.gsiv1.6.orders.g2` | `PartitionDO` | `policy.ns` | `(g2HK, physicalSk)` | GSI-2 entry bytes and key skew |
| `fokos.gsiv1.6.orders.g3` | `PartitionDO` | `policy.ns` | `(g3HK, physicalSk)` | GSI-3 entry bytes and key skew |

`fokos.m.<tableName>` and each GSI shard group inherit `rootTreesN`, `hashSplitN`, `jurisdiction`, `rangeConfig`, and
the split thresholds from the route context of the base table. Thus each client and each DO knows their topology with
no extra configuration.

The three sharded tiers split independently:

1. **Base split or promotion (`fokos.p`).** `fokos.m.<tableName>` routes by `(baseHK, baseSK)`, not by the base
   partition ID. Thus a base split or promotion does not change which `MaterializationDO` owns `(baseHK, baseSK)`:

```text
Base Table (fokos.p.orders)                          Materialization (fokos.m.orders)
---------------------------                          --------------------------------
fokos.p.orders~h.0 (old source, rev=5) ------------> fokos.m.orders~h.0
fokos.p.orders~r.dTE.. (promoted root, rev=6) ----->   owns (hk="u1", sk="o9")
                                                       accepts rev=6; fences a late rev=5 or acks it as stale
```

2. **`MaterializationDO` split or promotion (`fokos.m`).** Each base `PartitionDO` sends exports through a
   `FokosShardingClient` for `fokos.m.<tableName>`. When a `MaterializationDO` splits or promotes a hot base hash key,
   its migration host moves the rows of each item in the slice and the cursor of each target (Section 4.2.12). Each
   child continues from the copied cursors. No external cursor handoff is necessary.
3. **GSI split or promotion (`fokos.gsiv1`).** Each `MaterializationDO` holds one `FokosShardingClient` for each GSI
   shard group. A low-cardinality GSI hash key (such as `status = "OPEN"`) promotes to a range tree in that GSI shard
   group. The range tree splits by `physicalSk`, and `fokos.p` and `fokos.m` do not split.

#### 4.2.2 Monotonic base revisions

Each item mutation has a strictly increasing integer revision for each `(baseHK, baseSK)`. The revision increases
across creates, updates, deletes, TTL expiries, and repartitions. This behavior is built: a delete advances
`max_deleted_v` to `before.v + 1`.

1. In `PartitionStore.deleteItem`, when `before` exists at `before.v`, the delete has the revision
   `deleteVersion = before.v + 1`. `deleteItem` calls
   `#raiseDeletionMetadata(Math.max(opts.txOrderTs, before.last_read_ts), deleteVersion)` and returns `deleteVersion`.
   `deleteVersion` is absent when no row was removed.
2. In `PartitionStore.deleteExpiredItems`, each deleted row at `row.v` has the delete revision `row.v + 1`. The chunk
   raises `max_deleted_v` to `MAX(max_deleted_v, row.v + 1)`. Today the method returns only counts. Milestone 2 uses
   the `v` that its `RETURNING` clause already reads to write the tombstone of each row.
3. The next `upsertItem` or `composeUpdateWriteStatement` for that key starts at `d.max_deleted_v + 1`. That value is
   at least `before.v + 2`.
4. The P model (`PSrc/Partition.p`) announces the delete revision of a present row as its `v`. The monitor
   `VersionIncreases` (`PSpec/Versions.p`) checks that each revision of a key is above all earlier revisions.

```text
Create X   -> v = 1 (revision 1, deleted = 0)
Update X   -> v = 2 (revision 2, deleted = 0)
Delete X   -> max_deleted_v = 3 (revision 3, deleted = 1)
Recreate X -> v = 4 (revision 4, deleted = 0)
```

The existing transaction invariants stay true (Section 6.4).

#### 4.2.3 Base mutation capture and export

`FokosDBPolicy` and `FokosTableOptions` add `captureChanges` (default `false`), which travels in `PartitionContext`.
When `captureChanges` is `false`, a partition writes no outbox row and runs no export loop. This includes each GSI
shard group.

`captureChanges` is an ordinary option of the policy, so a request with a higher `policyVersion` can change it. Each
partition reads it from its stored policy at each write. The caller must set it only when the table is created: a
partition records only the changes after it receives the value, so a table that turns it on when it has data never
exports its older items. No partition checks this rule. Section 4.3.4 tracks capture on a table that has data.

`PartitionStore` adds the rowid table `materialization_outbox`:

```sql
CREATE TABLE IF NOT EXISTS materialization_outbox (
    outbox_id            INTEGER PRIMARY KEY,
    hk                   BLOB    NOT NULL,
    sk                   BLOB    NOT NULL DEFAULT x'',
    revision             INTEGER NOT NULL,
    is_deleted           INTEGER NOT NULL,
    is_local_only        INTEGER NOT NULL DEFAULT 0,  -- 1 while no receiver can have a revision of the key
    next_attempt_at      INTEGER NOT NULL DEFAULT 0,
    UNIQUE (hk, sk)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_materialization_outbox_due
    ON materialization_outbox (next_attempt_at, outbox_id);
```

##### Capture

When `captureChanges = true`, each committed content mutation updates `materialization_outbox` in the same
`transactionSync` as `items`:

1. **Insert of a new item (no existing row in `items`):**
   - When no row exists in `materialization_outbox`, insert
     `(hk, sk, revision = newV, is_deleted = 0, is_local_only = 1, next_attempt_at = 0)`.
   - When a row already exists in `materialization_outbox` (an unacknowledged delete), update it to
     `revision = newV, is_deleted = 0, next_attempt_at = 0`. The row keeps `is_local_only = 0` (step 3).
2. **Update of an existing item:**
   - Upsert `(hk, sk, revision = newV, is_deleted = 0, next_attempt_at = 0)`. When the row exists, keep
     `outbox_id` and `is_local_only`. Otherwise, set `is_local_only = 0`, because `MaterializationDO` can have an
     older revision of the item.
   - The upsert must be `INSERT ... ON CONFLICT (hk, sk) DO UPDATE`. It must not be `INSERT OR REPLACE`, because a
     replace gives the row a new `outbox_id`, and the split fences use `outbox_id` (Section 4.2.9).
3. **Delete of an existing item, or owned TTL expiry:**
   - When the `materialization_outbox` row has `is_local_only = 1`, no byte of this item has left the base
     partition. `PartitionStore` must delete the `materialization_outbox` row in place and must send no RPC to
     `MaterializationDO`.
   - Otherwise, `PartitionStore` upserts
     `(hk, sk, revision = deleteVersion, is_deleted = 1, is_local_only = 0, next_attempt_at = 0)`. It keeps
     `outbox_id` (step 2).

`is_local_only = 1` means that no receiver can have a revision of the key. It changes only from 1 to 0. Two cases
set it to 0:

- **An update of an exported item.** For example, the create of X at revision 1 is exported and acknowledged, so the
  outbox row is deleted. The update to revision 2 inserts a new outbox row. If that row had `is_local_only = 1`, a
  delete before the next export would remove it in place, and `MaterializationDO` would keep X at revision 1.
- **The export claim** (step 1 of "Export step" below). This prevents a lost delete when an earlier create RPC timed
  out after `MaterializationDO` committed it.

##### Export loop

`PartitionDO` drives the export of `materialization_outbox` with one in-memory background promise
(`#exportPromise`):

- Each committed base write wakes `#exportPromise`.
- In one `transactionSync`, the loop claims up to `maxExportCalls` (default 2) batches of owned, due rows from
  `idx_materialization_outbox_due`. It `await`s the `applyLatestImages` RPCs in parallel. Then it applies the
  responses in one `transactionSync`.
- It repeats until no due outbox rows remain or `canSweepLocally` becomes `false`.
- When the circuit breaker to `MaterializationDO` is open, the exporter claims no rows, and `is_local_only` does not
  change.
  TODO: define the trigger and the probe of this circuit breaker.
- Section 4.2.6 ("Lifecycle guard and alarm fallback") gives the alarm fallback.

##### Export step

Each export call has these steps:

1. **Claim.** In the claim `transactionSync`, the exporter:
   - selects up to `maxExportItems` (100) rows with `next_attempt_at <= now` from `idx_materialization_outbox_due`,
     bounded by `maxExportBytes` (1 MiB),
   - skips each row whose key is in a call in flight (the same-epoch rule, Section 4.2.9),
   - skips each unowned copy of a promoted key (`!this.fokos.owns(key)`) and sets its
     `next_attempt_at = now + exportRetryMs` (default 30 s). Thus unowned copies never block the owned keys behind
     them, and they do not spin the alarm.
   - sets `is_local_only = 0` on the claimed rows that have `is_local_only = 1`. A claimed row with
     `is_local_only = 0` gets no write,
   - sets `is_call_open = 1` when no export call is in flight (Section 4.2.9).
2. **Build.** For each claimed row with `is_deleted = 0`, the exporter reads the item from `items` in the same
   synchronous snapshot and checks `items.v == outbox.revision`. When the row is absent or `items.v != outbox.revision`
   (an invariant defect), it logs one error with the keys, deletes the outbox row, and continues. For
   `is_deleted = 1`, it builds a tombstone entry with no item payload.
3. **Send.** The exporter sends the call with its `producerId` and its export epoch. While export fence rows exist in
   `ancestor_fences`, the call also carries all of them (Section 4.2.9).
4. **Complete.** The exporter applies the response rules of Section 4.2.9. For each item that `MaterializationDO`
   acknowledged at `ackedRevision` and that this partition still owns (`this.fokos.owns(key)`), it runs this
   statement:

```sql
DELETE FROM materialization_outbox
 WHERE hk = ?1 AND sk = ?2 AND revision = ?3;
```

The base partition must delete a `materialization_outbox` row on acknowledgement only when
`stored.revision == acked.revision` and the partition still owns the key.

For example, a newer write (`revision = 12`) arrives while `revision = 11` is in flight. The `DELETE` for `11` then
matches zero rows and leaves `12` in the outbox with `next_attempt_at = 0`. For an item that returned `unprocessed`,
the exporter sets `next_attempt_at` to a backoff timestamp. TODO: give the backoff values of the export.

##### Splits, promotions, and TTL sweeps

Splits, promotions, and TTL sweeps handle outbox rows and ownership as follows:

1. **Migration (`FokosMigrationHost`).** The host adds a third migration stream, `materialization_outbox`, after
   `items` and `pending_tx`. It copies the outbox rows of the slice to the child with `is_local_only = 0` and
   `next_attempt_at = 0`, because an export call from the source can be in flight at the cutover. When the stream
   ends, the child stores the export fences with the mark `until_id = MAX(outbox_id)` (Sections 4.2.9 and 4.2.12).
2. **Cutover ownership.** After the cutover, `this.fokos.owns(key)` is `false` on the source.
   - The source exporter skips unowned outbox rows (it advances their `next_attempt_at`), and it drops the
     acknowledgements of unowned keys.
   - The child exports its migrated copy. `MaterializationDO` acknowledges an already-applied revision idempotently.
3. **Source cleanup (`cleanupSourceStep`).** After the migration reaches `completed`, `cleanupSourceStep` deletes:
   - the migrated `materialization_outbox` copies: all rows on a split, and the rows of the promoted `hashKey` on a
     promotion,
   - the `pending_transactions` copies,
   - on a promotion, the `items` rows.

   It returns `true` only when all tables have zero rows left for the moved slice.
4. **TTL sweep (`PartitionStore.deleteExpiredItems`).** In one `transactionSync`, the sweep reads a chunk of expired,
   unlocked candidates in `(ttl_epoch_utc_seconds, hk, sk)` order. It filters them with `this.fokos.ownerCheck()` and
   deletes only the owned rows.
   - Unowned copies stay on the promotion source until `cleanupSourceStep` removes them. Thus the range root always
     pulls an expired row during the migration, and its own sweep writes the tombstone after the import.
   - Each chunk returns `resumeAfter`, the last row that it read, and the next chunk of the same cycle starts after it.
     A chunk stops at `limit` owned rows or at `10 * limit` candidates. Thus the sweep reads each copy one time in a
     cycle, and it deletes the owned expired rows that sort after the copies. The next cycle starts again at the first
     expired row.

#### 4.2.4 `MaterializationDO` storage and ingest

`MaterializationDO` keeps the latest payload of each live base item in `latest_items.data` (full-mirror mode):

- Each table that holds an item payload or multi-kilobyte keys is a rowid table (Section 1.3).
- Each per-item table links to `latest_items.item_id` (an 8-byte integer). Thus `(base_hk, base_sk)` is stored one
  time for each item, not one time for each target.
- `key_size_estimates` tracks the byte totals of each `base_hk` for `promotionCandidates` and `planRangeSplit`.

```sql
CREATE TABLE IF NOT EXISTS latest_items (
    item_id       INTEGER PRIMARY KEY,
    base_hk       BLOB    NOT NULL,
    base_sk       BLOB    NOT NULL DEFAULT x'',
    revision      INTEGER NOT NULL,
    deleted_at_ms INTEGER,          -- NULL for a live item; the ingest time of the delete for a tombstone
    newest_seq    INTEGER,          -- the newest change position of the item (Section 4.2.5)
    data_kind     INTEGER,
    ttl_at        INTEGER,
    est_row_bytes INTEGER NOT NULL DEFAULT 0,
    data          ANY,
    UNIQUE (base_hk, base_sk)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_latest_items_deleted_gc
    ON latest_items (deleted_at_ms)
    WHERE deleted_at_ms IS NOT NULL;

CREATE TABLE IF NOT EXISTS key_size_estimates (
    hk        BLOB    PRIMARY KEY,
    est_bytes INTEGER NOT NULL
) WITHOUT ROWID, STRICT;
```

A `latest_items` row is a tombstone when `deleted_at_ms IS NOT NULL`. The value gives the order of the GC sweep
(Section 4.2.11) and helps debugging. No rule reads the value of the time, so correctness does not depend on the clock
(Requirement 9). A split copies the value unchanged.

The other tables of `MaterializationDO` are in the sections that use them: `change_positions` (Section 4.2.5),
`materialization_targets` (Section 4.2.6), `target_index_keys` (Section 4.2.7), `producer_fences` and
`ancestor_fences` (Section 4.2.9), and `target_retry` (Section 4.2.10).

`applyLatestImages` is a `group` operation on `MaterializationDO` with `whileMigrating: "throw"`,
`failurePolicy: "collect"`, and `admissionTag: "batch_write"`. For each item, in the `transactionSync` of that item:

1. `MaterializationDO` applies the receiver rules of Section 4.2.9 (the producer epoch and the ancestor fences).
2. It must accept an export only when `incoming.revision > stored.revision`.
3. It must idempotently acknowledge an export when `incoming.revision == stored.revision`.
4. When `MaterializationDO` is above `1.1x maxSizeMb`, its local handler must reject only live upserts
   (`deleted: false`), as `unprocessed` with `partition_over_size`, and signal `evaluateSplit`. It must still ingest
   deletions (`deleted: true`), so that deletes can reclaim space.

The ingest of an accepted image does these steps:

1. **Live item (`deleted: false` at `revision = R`).** It upserts the `latest_items` row (`revision = R`,
   `deleted_at_ms = NULL`, `data`, `est_row_bytes`) and updates `key_size_estimates`.
2. **Deleted item (`deleted: true` at `revision = R`).** It sets `latest_items.data = NULL` and
   `deleted_at_ms = now`. It updates `est_row_bytes` to the key-only tombstone size and updates `key_size_estimates`.
   Thus the item payload is free at once. When the item has no `latest_items` row, the ingest stores a tombstone row.
   A GSI with no key set for the item then sends no call (Section 4.2.7), and a push target gets the tombstone.
3. **Change position.** It writes or moves the change position of the item with the rules of Section 4.2.5.
4. **Retry rows.** When the item has `target_retry` rows of kind `'retry'`, it sets `not_before = 0` on them
   (Section 4.2.10).
5. **Wake.** After the batch commits, it wakes the target runner (Section 4.2.6).

**Link-ID delete rule.** Each path that deletes a `latest_items` row (the tombstone GC sweep, and `cleanupSourceStep`
on a promotion source) must first delete all `change_positions`, `target_index_keys`, and `target_retry` rows with that
`item_id`, in the same `transactionSync`. Thus a rowid that SQLite gives again never attaches orphaned rows to a new
base item.

#### 4.2.5 Change positions

```sql
CREATE TABLE IF NOT EXISTS change_positions (
    seq      INTEGER PRIMARY KEY,
    item_id  INTEGER NOT NULL,
    prev_seq INTEGER            -- the older position of the same item, or NULL
) STRICT;
```

`MaterializationDO` gives each accepted image a position in `change_positions`, a table that all targets share.
`latest_items.newest_seq` is the newest position of the item. The ingest writes the `latest_items` row in all cases, so
this column adds no row write. `change_positions` has no secondary index. `seq` is the rowid, and the ingest finds the
positions of an item through `newest_seq` and `prev_seq`.

Each target `t` has two positions:

- `C_t` is `cursor_seq` in SQLite (Section 4.2.6). Each position at or below `C_t` is done for `t`.
- `R_t` is in memory. While a batch of `t` is in flight, `R_t` is the highest position that the batch read. Otherwise,
  `R_t = C_t`. After a restart, `R_t = C_t`.

When `MaterializationDO` accepts an image of item X, it reads `e = latest_items.newest_seq` and the row `e` to get
`p = prev_seq`. A position at or below `MIN(C_t)` counts as absent. `N` is the next position, above each position in
the partition. Then the ingest does one of these steps:

1. **When `e` is absent,** the ingest inserts `(N, X, NULL)` and sets `newest_seq = N`.
2. **When `R_t < e` for each target,** the ingest writes no position. No target has read `e`, so each target reads the
   new image at `e`.
3. **When each target with `C_t < e` also has `C_t < p`,** the ingest moves `e` to `N`
   (`UPDATE change_positions SET seq = N WHERE seq = e`) and sets `newest_seq = N`. Each target that has not finished
   `e` reaches `p` first. Each other target finds X at `N`.
4. **Otherwise,** the ingest inserts `(N, X, e)` and sets `newest_seq = N`.

When no target is registered, the ingest writes no position. A cleanup step deletes the rows with
`seq <= MIN(C_t)` in bounded chunks.

The positions must keep these invariants:

1. **Each target reads each change.** When target `t` has not applied the latest image of X, X has a position above
   `C_t`, or X has a `retry` row for `t` (Section 4.2.10).
2. **No target starves.** The ingest moves only the newest position of an item, and only when no target depends on
   that position. Thus the oldest position of X above `C_t` stays fixed until `t` reads it. A hot item cannot move
   ahead of a lagging cursor.
3. **Step 2 uses `R_t`, and step 3 uses `C_t`.**
   - A batch in flight can hold the old image of X from position `e`. If step 2 used `C_t`, the ingest would write no
     new position, the batch would move the cursor past `e`, and the new image would be lost.
   - When that batch fails, the target reads again from `C_t`. Thus step 3 must count the target as a target that still
     needs `e`.
4. **At most `T + 1` positions for each item.** Each position of X above `MIN(C_t)` serves a different group of
   cursors. Thus the table grows with the number of changed items, not with the number of mutations.

Example: the GSI `g` reads at the end of the positions. The push target `lake` is down at `C_lake = 10`.

| Change of X | Positions of X | Ingest step |
| --- | --- | --- |
| rev 1 | 100 | Step 1: X has no position. |
| `g` reads 100. rev 2 | 100, 205 | Step 4: `lake` needs 100, and `p` is absent. |
| `g` reads 205. rev 3 | 100, 310 | Step 3: `lake` reaches 100 first, so 205 moves to 310. |
| `g` reads 310. rev 4 | 100, 415 | Step 3. |

When `lake` recovers, it reads position 100 and sends the latest image of X. At position 415, the applied map
(Section 4.2.6) skips X.

#### 4.2.6 Target runner and target step

`MaterializationDO` stores the state of each target in one row:

```sql
CREATE TABLE IF NOT EXISTS materialization_targets (
    target_id     TEXT    PRIMARY KEY,
    kind          TEXT    NOT NULL,
    cursor_seq    INTEGER NOT NULL DEFAULT 0,
    epoch         INTEGER NOT NULL DEFAULT 1,
    is_batch_open INTEGER NOT NULL DEFAULT 0  -- 1 while a batch of this target can be in flight
) WITHOUT ROWID, STRICT;
```

##### In-memory runner

A Durable Object alarm for each batch is too slow, and it writes alarm storage on each step. Thus `MaterializationDO`
drives its targets with one in-memory background promise (`#pushPromise`), as `TtlExpiry.runCycle` in
`packages/fokosdb/src/shared/partition/ttl-expiry.ts` does:

- `applyLatestImages` wakes the loop as soon as a batch commits.
- The loop starts `targetConcurrency` (default 2) parallel worker coroutines. They round-robin across the targets of
  the stored target list (Section 4.2.13).
- Each `target_id` has at most 1 batch in flight, so two workers never claim the same target at the same time. One
  slow target holds at most 1 of the 2 workers. The other worker drains all the other healthy targets.
- The workers repeat in memory until no target has due work or `canSweepLocally` becomes `false`. They yield with
  `scheduler.wait(0)` between batches.

Each target keeps an in-memory map `item_id -> revision`, the applied map:

- The map holds the revisions that the target applied in its current epoch.
- The map has at most `appliedMapMaxEntries` (default 10,000) entries for each target, with least-recently-used
  eviction.
- The map is only an optimization. When the map has no entry for an item, the target sends the item again, and the
  receiver drops the duplicate or writes the same entry again.

##### Target step

A step of target `t` has these parts:

1. **Claim.** In one `transactionSync`, the step:
   - takes the due `retry` rows of `t`, up to half of `maxItemsPerStep` (Section 4.2.10),
   - reads the positions above `C_t` in `seq` order to fill the batch, up to `maxItemsPerStep` (100, the
     `batchWriteItems` limit) items, `maxBytesPerStep` (16 MiB, below the RPC limit) of item data, and
     `maxScannedPositions` (5,000) positions,
   - sets `is_batch_open = 1` when it is `0` (Section 4.2.9).

   The claim stops at the first limit that it reaches. When the claim finds no item to send, it sets `cursor_seq` to
   the highest position that it read, in the same `transactionSync`. It sends no call and does not set `is_batch_open`.
   Thus a long run of skipped positions (step 2), for example the positions of a promoted key, moves the cursor in
   bounded steps.
2. **Filter.** For each position, the step joins `latest_items` by `item_id`. It skips the position when one of these
   conditions is true:
   - the item has a `retry` row for `t`,
   - the item is already in the batch,
   - the applied map holds `latest_items.revision` for the item,
   - the partition does not own the item (`!this.fokos.owns(key)`), because the item moved to another partition.

   A skipped position is done for `t`.
3. **Build.** The step builds the entries from the latest image of each item: GSI entries (Section 4.2.7) or a push
   record (Section 4.2.8). It sets `R_t` to the highest position that the claim read.
4. **Send.** The step sends the call with `producerId`, `epoch`, and the ancestor fences (Section 4.2.9).
5. **Complete.** In one `transactionSync`, the step applies the response with the rules of Sections 4.2.9 and 4.2.10.
   When each item of the batch is applied or has a `retry` row, it sets `cursor_seq` to the highest position that the
   claim read. When the target has no more due work (no position above the new cursor and no due `retry` row), the same
   update sets `is_batch_open = 0`. This is 1 row update in `materialization_targets` for each batch. Then the step sets
   `R_t = C_t`.

The cursor moves to the highest position that the claim read, not to the position of the last item that the step
sent. The skipped positions are done for `t`.

##### Lifecycle guard and alarm fallback

- An export job or a target step must act only on rows that its partition owns. It must write an outcome only for a
  row that its partition still owns (Section 4.2.9, response rule 2).
- Neither loop runs when `canSweepLocally` is `false`: the partition is not initialized, is a router, is
  `awaiting_data` or `importing`, or is fenced for destroy.
- A `FokosScheduler` job is only the durable fallback when the DO restarts or is evicted: `materialization_export` in
  `PartitionDO`, and `target_push` in `MaterializationDO`.
- While the in-memory loop runs, `deadline()` returns `now + fallbackAlarmMs`. Thus `FokosScheduler` does not start a
  new pass every `fastPathDelayMs` (default 50 ms).
- While the in-memory loop is idle, `deadline()` returns the earliest due time in SQLite, and `runStep()` starts the
  in-memory loop. In `PartitionDO`, the due time is the earliest `next_attempt_at`. In `MaterializationDO`, it is
  `now` when a position above a cursor exists, or else the earliest `not_before` of a `retry` row.

#### 4.2.7 GSI targets and key sets

A `gsi` target writes to `fokos.gsiv1.<n>.<tableName>.<indexName>` with `FokosDB.batchWriteItems`.

##### Desired GSI key

For each item in a batch, the step evaluates the desired GSI key `D` from the latest image:

- `D` is absent (sparse index) in these cases:
  - `latest_items.deleted_at_ms IS NOT NULL`,
  - the GSI partition key attribute is missing or `null`,
  - the GSI defines a `sortKey` whose attribute is missing or `null`.
- Otherwise, the step evaluates the GSI key and projection expressions against `latest_items` to get `D` and the
  projected entry. The evaluation aliases `base_hk AS hk, base_sk AS sk, revision AS v, ttl_at AS
  ttl_epoch_utc_seconds`, so that the compiled expression SQL reads the column names it expects.
- When the evaluation fails with a permanent violation (Section 4.2.10), the step runs the violation cleanup instead of
  `put(D)`.

##### Key sets

```sql
CREATE TABLE IF NOT EXISTS target_index_keys (
    key_set_id INTEGER PRIMARY KEY,
    item_id    INTEGER NOT NULL,
    target_id  TEXT    NOT NULL,
    index_keys BLOB    NOT NULL,  -- encoded list of (index_hk, gsiSK) pairs
    UNIQUE (item_id, target_id)
) STRICT;
```

The key set of `(item_id, target_id)` holds each GSI key that can have an entry of the item in that GSI. When no row
exists, the GSI has no entry of the item. The set normally holds 1 key. It holds 2 keys while a key moves. It holds
more keys only after failed deletes. It is a rowid table, because the keys can be multiple kilobytes (Section 1.3).

`index_keys` stores the encoded `gsiSK`, not `physicalSk` (Section 4.2.14). With 1.5 KB base keys and 10 GSIs, this
saves about 15 KB for each item at rest and 30 KB while a key moves. Before `MaterializationDO` sends a GSI call, it
reads `base_hk` and `base_sk` from `latest_items` by `item_id`. Both columns come before the trailing `data` column, so
SQLite reads no overflow page. Then it builds `physicalSk = encodeGsiSortKey(index_sk, base_hk, base_sk)`.

##### GSI step

After the step evaluates `D`, it does these steps:

1. **When `D` is present and is not in the set,** the claim `transactionSync` adds `D` to the set before the call.
2. **The first call** sends `put(D)` with `ttlAt: latest_items.ttl_at` when `D` is present. When `D` is absent, it
   sends `delete(K)` for each key `K` in the set.
3. **The second call** of the same step sends `delete(K)` for each key `K` in the set other than `D`, after `put(D)`
   applied. Thus a key move writes the new entry before it deletes the old entry.
4. **After `delete(K)` applies in the current epoch,** the completion removes `K` from the set. When the set becomes
   empty, the completion deletes the row.

When `D` is present and is the only key in the set, the step writes no row. `put(D)` overwrites the entry with the
latest projection. When `D` is absent and no row exists, the step sends no call and writes no row.

The key set prevents an orphan GSI entry in two cases:

- **The sender stops between `put(D)` and a later record of `D`.** Then the item moves to `E`. If `D` were not
  recorded, no step would delete the entry at `D`. The rule of step 1 stores each key that a call can write before
  the call.
- **A `delete(K)` has an `unknown` outcome.** The entry at `K` can still exist. Thus `K` stays in the set until a
  `delete(K)` applies, and a later step deletes it again.

The two calls of one step run in sequence and carry different GSI keys of the item. Thus they keep the same-epoch
rule of Section 4.2.9.

`MaterializationDO` validates each item before it calls `batchWriteItems`. When `batchWriteItems` still throws a
validation error for the whole call before I/O, `MaterializationDO` sends each item of that batch again alone. Thus
only the item that is not valid becomes a violation.

#### 4.2.8 Push targets

A `push_sink` target calls a `WorkerEntrypoint` or a `DurableObject` over Workers RPC in one call. A push target keys
records by `(baseHashKey, baseSortKey)`, so it has no old secondary key to delete, and it needs no key set.

For each item, the step sends one `PushTargetItem` with the latest image or the tombstone. It applies the optional
target projection to live items. The batch carries the producer fields of Section 4.2.9:

```ts
export type PushTargetItem = {
	baseHashKey: HashKey;
	/** Absent when the base item has no sort key. */
	baseSortKey?: SortKey;
	revision: number;
	deleted: boolean;
	/** The complete item or projected record. Absent when `deleted` is true. */
	value?: ReadItemValue;
	/** Epoch UTC seconds. Absent when the item has no TTL or when `deleted` is true. */
	ttlAt?: number;
};

export type PushTargetBatch = {
	/** The `DurableObjectId` of the sending `MaterializationDO`, as `DurableObjectId.toString()` gives it (hex). */
	producerId: string;
	epoch: number;
	ancestorFences: readonly { ancestorId: string; minEpoch: number }[];
	items: readonly PushTargetItem[];
};

/** One result for each item of the batch, in batch order. */
export type PushTargetResult =
	| { outcome: "applied" }
	| { outcome: "retry"; code?: string }
	| { outcome: "rejected"; code: string };
```

The step calls `method(batch)` on the configured binding (Section 4.2.13) and applies each returned
`PushTargetResult`. When the call throws, fails at the transport, or returns an array of the wrong length, each item
of the batch is `unknown`. After an `unknown` outcome, the step raises its epoch and sends again at once
(Section 4.2.9).

The push target contract:

- Delivery is at-least-once and coalesced. Intermediate revisions can be skipped.
- `revision` strictly increases for each `(baseHashKey, baseSortKey)` across creates, updates, and deletes.
- Two pushes for one key can arrive out of order when an earlier call stays in flight after a lost response. Thus the
  push target must keep the highest applied `revision` of each key and ignore each push with
  `revision <= appliedRevision`.
- A push target that checks the producer epoch with the receiver rules of Section 4.2.9 can delete the revision of a
  deleted key after it applies the delete. A push target that does not check the epoch must keep that revision. Its
  retention is the choice of the push target.

#### 4.2.9 Producer epochs and fences

##### Producers and receivers

| Hop | Producer ID | Epoch storage at the producer | Receiver checks |
| --- | --- | --- | --- |
| Base `PartitionDO` -> `MaterializationDO` (`applyLatestImages`) | `DurableObjectId` of the base partition, in hex | `materialization_export_state` | The producer epoch, then the revision (Section 4.2.4) |
| `MaterializationDO` -> GSI `PartitionDO` (`batchWriteItems`) | `DurableObjectId` of the `MaterializationDO`, in hex | `materialization_targets.epoch` of the GSI target | The producer epoch |
| `MaterializationDO` -> push target | `DurableObjectId` of the `MaterializationDO`, in hex | `materialization_targets.epoch` of the push target | The revision. The producer epoch is optional (Section 4.2.8). |

Each GSI shard group gets calls from one target only. Thus a GSI partition stores one epoch for each
`MaterializationDO`, and the producer ID needs no `targetId`. The producer ID is the `DurableObjectId` as
`DurableObjectId.toString()` gives it: 64 hex characters, the form that `idFromString` takes. A partition ID or a DO
name can hold user keys, which can be multiple kilobytes.

The base partition stores its export state in one row:

```sql
CREATE TABLE IF NOT EXISTS materialization_export_state (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    epoch        INTEGER NOT NULL DEFAULT 1,
    is_call_open INTEGER NOT NULL DEFAULT 0  -- 1 while an export call can be in flight
) STRICT;
```

Each receiver (`MaterializationDO` for the base hop, and each GSI `PartitionDO`) stores the highest epoch of each
producer:

```sql
CREATE TABLE IF NOT EXISTS producer_fences (
    producer_id TEXT    PRIMARY KEY,  -- DurableObjectId in hex
    epoch       INTEGER NOT NULL
) WITHOUT ROWID, STRICT;
```

##### Receiver rules

For each item of a call, in the `transactionSync` of that item, the receiver does these steps:

1. For each ancestor fence `(ancestorId, minEpoch)` of the call, when the stored epoch of `ancestorId` is below
   `minEpoch`, it stores `minEpoch`. An absent row has the epoch `0`.
2. When the epoch of the call is below the stored epoch of `producerId`, it writes nothing for the item and returns
   `unprocessed` with `producer_epoch_fenced`. This outcome is definite: the item did not apply.
3. When the epoch of the call is above the stored epoch, it stores the epoch of the call.
4. It applies the item. `MaterializationDO` also applies the revision rules of Section 4.2.4.

The receiver writes a `producer_fences` row only when an epoch goes up. In normal operation, the epoch stays the same,
and the check adds no row write. The receiver keeps the row of each producer in memory after the first read.

The receiver must not delete a `producer_fences` row. A deleted row has the epoch `0`, and a late call of that
producer then applies. The number of rows is the number of producers that ever sent to the receiver. A row is about
80 bytes, so 10,000 producers use about 800 KB in each receiver. TODO: measure the row size.

A receiver split copies the whole table to each child with `MAX(epoch)` (Section 4.2.12). The partition that applies
a forwarded call checks it.

A forward or a client retry must not send a second copy of a call that can have applied. `FokosDB.batchWriteItems`
does not retry, and the runtime sends a forward again only after an error that came before any handler ran on the
target. Thus each call reaches the receiver at most one time.

##### The same-epoch rule

Two calls of one producer with the same epoch must not carry the same item at the same time.

- **Why.** The receiver cannot order two calls with the same epoch. For example, the base exporter sends X at revision
  2 and, in a parallel call, the delete of X at revision 3. Revision 3 applies, and the targets read the tombstone, so
  the garbage collection of Section 4.2.11 deletes it. Then revision 2 arrives, and X is created again.
- **`MaterializationDO` -> target.** Each target has at most 1 batch in flight, and the two calls of a GSI step run in
  sequence. Thus the rule is always true.
- **Base `PartitionDO` -> `MaterializationDO`.** The exporter sends up to `maxExportCalls` (default 2) calls in
  parallel. It keeps an in-memory set of the item keys in calls in flight. The claim skips each outbox row whose key is
  in the set. A key leaves the set when its call returns. The calls stay full of other items, and a skipped item merges
  its newer writes into its outbox row until its call returns.

##### When the producer raises its epoch

The producer raises its epoch by 1 in SQLite before its next call on that hop, in these cases:

1. **After an `unknown` outcome.** An `unknown` outcome is a thrown call, a transport failure, a response of the wrong
   length, or a timeout at the producer. The old call can still apply. The producer raises the epoch in the
   `transactionSync` that prepares the next call.
2. **At a cold start, when a call from before the restart can still be in flight.** That call can still apply. The
   producer checks this before its first call after the restart:
   - **`MaterializationDO`, for each target:** `is_batch_open = 1`.
   - **Base `PartitionDO`:** `is_call_open = 1`.

   When the check finds no call that can be in flight, the epoch stays.
3. **At a split cutover** (see "Split fences" below).

The producer must store the raised epoch before it sends the next call. If the producer sent the call first and then
stopped, it could use the old epoch again after the restart.

The cold-start checks keep the epoch stable across idle restarts. If each cold start raised the epoch, the producer
would write 1 row after each wake, and each receiver that it calls next would write 1 `producer_fences` row. A
`MaterializationDO` that wakes for each base write would then write 1 row in each GSI partition that it reaches.

- **`is_batch_open`** costs at most 1 more row for each period of target work, not 1 row for each batch:
  - The claim sets `is_batch_open = 1` only when it is `0`. The runner keeps the value in memory, so the next claims
    write no row for the marker.
  - The completion of the last batch clears it in the cursor update, which it writes in all cases (Section 4.2.6,
    step 5).
- **`is_call_open`** costs at most 2 rows for each period of export work, not 1 row for each batch or each item:
  - The claim `transactionSync` sets `is_call_open = 1` only when no export call is in flight. The exporter keeps the
    value in memory, so a claim while `is_call_open = 1` writes no row for the marker.
  - The completion `transactionSync` of the last call in flight sets `is_call_open = 0`.
  - The cold-start check reads 1 row. It does not depend on the size of the outbox, so a large outbox after a long
    outage does not make each cold start slower.
- **Optional optimization (not in the first version): delayed clear.** For a partition in memory that gets one write
  and then a pause, each write is a new period of export work, so it costs 2 rows for `is_call_open`. The exporter can
  clear `is_call_open` only after no call was in flight for a short time. If the DO is evicted before the clear, the
  next cold start raises the epoch, which is correct.

After an `unknown` outcome, the producer sends again at once:

- `MaterializationDO` reads again from `C_t`, because the cursor did not move. Thus the new call carries each item of
  the old call. For a GSI item, it contacts each key in the key set, and the set holds each key that the old call
  could write. Thus each receiver that the old call could reach gets the new epoch.
- The base exporter sends the same outbox rows again, at their current revision, to the `MaterializationDO` of each
  key.
- At each receiver, the old call and the new call can arrive in two orders:
  - When the old call arrives first, the new call overwrites or deletes its write.
  - When the new call arrives first, the receiver stores the new epoch and rejects the old call.

A producer can stop its wait for a slow call after `callTimeoutMs`. The outcome is then `unknown`, and the rules above
apply. The value of `callTimeoutMs` changes cost and latency, not correctness. Each timeout costs 1 row at the
producer and 1 row at each receiver that gets the new epoch first. The default of `callTimeoutMs` is 30 s. TODO:
measure the normal call duration and tune the default.

##### Response rules

The completion handler of a call applies these rules:

1. **Old epochs.** When the epoch of the call is below the current epoch of the hop, the handler discards the response
   and writes nothing. A call of the new epoch decides the outcome. A `producer_epoch_fenced` result of an old call is
   not a circuit breaker failure.
2. **Checks before each write.** In the completion `transactionSync`, the handler checks each condition before it
   writes for an item. When a check fails, it writes nothing for that item.
   - The partition still owns the item (`this.fokos.owns(key)`).
   - For a cursor move, `cursor_seq` still has the value that the claim read.
   - For a key removal, the key is still in the key set.
   - For the delete of a `retry` row, `latest_items.revision` equals the revision that the call sent.
   - For the delete of a `materialization_outbox` row, `stored.revision == acked.revision` (Section 4.2.3).
3. **Order.** The handler applies the definite per-item results first. Then, when the call has an `unknown` part, it
   raises the epoch.

##### Split fences

The rules apply to a split or a promotion of a base `PartitionDO` (the base hop) and of a `MaterializationDO` (each
target hop). The parent is the source of the split, and the child is each target of the split.

1. **The parent raises its epoch.** Before the parent serves the first page of its export state, it raises its epoch
   by 1 in one `transactionSync`. The base partition raises `materialization_export_state.epoch`. `MaterializationDO`
   raises the epoch of each target. The parent stores the raised epoch `E` with the repartition ID, so that each child
   of the split gets the same `E`.
2. **The parent stops work on the moved slice.** After the cutover, the parent does not own the moved slice. Thus its
   claims skip those rows, and its completion handlers write nothing for them (response rule 2). The pages that the
   parent serves after the cutover hold the final export state of the slice.
3. **The child stores the fences.** The child gets `(parent DO ID, E)` and the fence rows of the parent:

```sql
CREATE TABLE IF NOT EXISTS ancestor_fences (
    hop         TEXT    NOT NULL,  -- 'export', or a target_id
    ancestor_id TEXT    NOT NULL,  -- DurableObjectId in hex
    min_epoch   INTEGER NOT NULL,
    until_id    INTEGER NOT NULL,  -- the highest copied seq (target hop) or outbox_id (export hop)
    PRIMARY KEY (hop, ancestor_id)
) WITHOUT ROWID, STRICT;
```

4. **The child sends the fences with its inherited work.** The inherited work is the work that the parent can have in
   flight at the cutover. While the fence rows of a hop exist, each call of that hop carries all of them.
   - For a `MaterializationDO` target `t`: the positions at or below `until_id`, and the `target_retry` rows that the
     child copied (`is_inherited = 1`). The fences of `t` stay while `C_t < until_id` or an inherited `retry` row of `t`
     exists.
   - For the base hop: each outbox row with `outbox_id <= until_id`. The child takes no writes while it imports (the
     lifecycle gate `#whileImporting` in `packages/fokosdb/src/sharding/runtime.ts`), so at the end of the import
     these are exactly the copied rows. A newer write keeps the `outbox_id` of its row (Section 4.2.3, capture step
     2). The acknowledgement delete removes the row.
   - The child counts each copied outbox row as inherited, also a row that had `is_local_only = 1` on the parent. A
     fence on a call that does not need it is safe. The parent sends no later call for the moved slice, and it sends its
     other keys with an epoch of `E` or higher.
   - SQLite gives a new row the ID `MAX(rowid) + 1`. When the row with the highest ID is deleted, a new row can get
     an ID at or below `until_id`. This is safe: that row gets fences that it does not need, and the fence removal
     waits for its acknowledgement.
5. **The child removes the fences.** When no inherited work remains for a hop, the child deletes all fence rows of that
   hop in one `transactionSync`.
   - For the base hop, each `transactionSync` that deletes an outbox row also runs
     `SELECT 1 FROM materialization_outbox WHERE outbox_id <= ?1 LIMIT 1` with `until_id`. This is a seek on the
     primary key that reads at most 1 row. When it returns no row, the same transaction deletes the export fence
     rows. After that, the exporter does not run the check.
   - For a target hop `t`, each `transactionSync` that moves `C_t`, or that deletes or changes an inherited `retry`
     row of `t`, also checks both conditions of step 4. The first is `C_t >= until_id`. The second is that
     `SELECT 1 FROM target_retry WHERE target_id = ?1 AND is_inherited = 1 AND kind = 'retry' LIMIT 1` returns no row.
     The query uses `idx_target_retry_inherited` (Section 4.2.10) and reads at most 1 row. When both are true, the
     same transaction deletes the fence rows of `t`. After that, the target step does not run the check.
   - The index leaves out `violation` rows, because a `violation` row has no call in flight. The completion sets
     `kind = 'violation'` only after the violation cleanup deleted each key in the key set (Section 4.2.10). When the
     parent wrote the row, no parent call for the item was in flight. When the child changes an inherited row to
     `violation`, the cleanup calls of the child carried the fences to each key that a late parent call can write.
   - A grandchild copies the fence rows of its parent and adds its own parent. For the export hop, it sets `until_id`
     of each export fence row to its own mark, because the outbox IDs of the parent have no meaning in the
     grandchild.
   - Thus the list normally holds 0 entries. It holds 1 or 2 entries after a split, until the copied work is done.

The child starts its own epoch at 1. Each receiver has no row for the new `producerId`, so it accepts the first call.
The child does not depend on a parent descendant check at the receiver, because the child names each ancestor in the
call.

The fences are enough for these reasons:

1. A late parent call can change a receiver only at an item that the parent had in flight at the cutover.
2. That item is part of the inherited work of the child:
   - A target cursor moves only after a definite outcome, so the item has a position above the copied cursor, or a
     copied `retry` row. Step 3 of Section 4.2.5 never moves a position that a cursor still needs.
   - A base outbox row of a call in flight stays in the outbox of the parent until the response arrives. Thus the
     child copies it, with an `outbox_id` at or below `until_id`.
3. For a GSI item, each key that the parent call could write is in the copied key set (Section 4.2.7, step 1).
4. Thus the child sends a call with the fences to the receiver of each such key or item. A key always maps to the same
   GSI partition, and a receiver split copies `producer_fences` with `MAX`.
5. At that receiver, the late parent call and the child call arrive in one of two orders:
   - When the late call arrives first, the child call overwrites or deletes its write.
   - When the child call arrives first, the receiver stores `E`, and it rejects the late call, which has an epoch
     below `E`.

A promotion source continues to send its other keys with an epoch of `E` or higher, and each receiver accepts them.
Its calls from before the raise can get `producer_epoch_fenced`. The source discards their responses (response rule 1)
and sends those items again with epoch `E`.

#### 4.2.10 Failed items and permanent violations

```sql
CREATE TABLE IF NOT EXISTS target_retry (
    target_id    TEXT    NOT NULL,
    item_id      INTEGER NOT NULL,
    kind         TEXT    NOT NULL, -- 'retry' | 'violation'
    not_before   INTEGER NOT NULL, -- the backoff
    attempts     INTEGER NOT NULL DEFAULT 0,
    is_inherited INTEGER NOT NULL DEFAULT 0, -- 1 when a split copied the row from the parent
    last_error   TEXT,
    PRIMARY KEY (target_id, item_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_target_retry_due
    ON target_retry (target_id, not_before)
    WHERE kind = 'retry';

CREATE INDEX IF NOT EXISTS idx_target_retry_inherited
    ON target_retry (target_id)
    WHERE is_inherited = 1 AND kind = 'retry';
```

`idx_target_retry_inherited` holds only the `retry` rows that a split copied. The split fences use it (Section 4.2.9,
"Split fences", step 5). Normal writes do not write to it.

##### Retry rows

Each target step takes the due `retry` rows (`not_before <= now`) first, up to half of `maxItemsPerStep`. Then it fills
the rest of the batch from the cursor (Section 4.2.6). When the cursor has no work, the `retry` rows can fill the whole
batch. When no `retry` row is due, the cursor fills the whole batch. Thus the `retry` rows cannot stop the cursor, and
the cursor cannot stop the `retry` rows. Each item gets its latest image. A failed item has its own backoff and no
position order, so a due index fits it better than a second cursor.

These rules apply:

1. **A `retry` row owns its item for its target.** The cursor skips an item that has a `retry` row. Thus only one path
   sends an item for a target at one time.
2. **A change of the item makes its `retry` rows due.** When the ingest accepts an image of an item that has `retry`
   rows, it sets `not_before = 0` on these rows. This costs 1 row, only for items in retry. The cursor skips the new
   position of the item (rule 1), so the `retry` row must send the new image.
3. **A `violation` row does not own its item.** When the item changes, the cursor sends the new image. When the new
   image applies, the completion deletes the `violation` row.

The `retry` backoff starts at 1 s, doubles after each failure, and stops at 10 s.

A call is a failure of the whole call when each item of the call fails with a code that can be retried (`unprocessed`,
or push `retry`). Then the step writes no `retry` row and counts one circuit breaker failure (Section 4.2.15). Thus a
target that fails each item does not create a `retry` row for each item. When at least one item applies, each failed
item gets its own `retry` row.

##### Call outcomes

| Outcome for an item | Action of the completion |
| --- | --- |
| `applied`, and `latest_items.revision` equals the sent revision | Record the revision in the applied map. Delete the `retry` or `violation` row when one exists. |
| `applied`, and the item changed during the call | For a `retry` item, keep the row with `not_before = 0`. For a cursor item, write nothing: the change has a position above the cursor (Section 4.2.5, invariant 1). |
| `unprocessed`, or push `retry` | Insert or update the `retry` row: `attempts + 1`, and an exponential backoff from 1 s to 10 s. |
| `rejected` (`batch_request_too_large`) | The RPC is above 32 MiB, and nothing applied. Split the batch and send again. No row. |
| `rejected` (any other code) | A permanent item failure. Run the violation cleanup, then set `kind = 'violation'`. |
| The whole call fails with a definite outcome | Keep the cursor. Write no `retry` row. Back off and count a circuit breaker failure. |
| `unknown` | Raise the epoch, keep the cursor, and send again at once (Section 4.2.9). |

Example: the batch limit is 100, 3 `retry` rows are due, and `C_t = 500`.

1. The step takes the 3 retry items.
2. It reads positions 501 to 610. It skips 4 positions of items with `retry` rows, 3 positions that the applied map
   skips, and 6 positions of items that are already in the batch. It sends the other 97 items.
3. The call returns: 2 retry items applied, 1 retry item is `unprocessed` with `partition_migrating`, and the 97 cursor
   items applied.
4. One `transactionSync` deletes 2 `retry` rows, updates 1 `retry` row, and sets `cursor_seq = 610`.

##### Permanent violations

When `MaterializationDO` evaluates a GSI key and projection against `latest_items`, or pushes an item, three cases are
possible:

1. **Missing or `null` GSI key attribute (a sparse omission, not a violation).** The item is absent from that GSI
   (`D` is absent).
2. **GSI key type or size that is not valid (a violation):**
   - **Type violation.** The extracted GSI key is not a valid non-empty string (`text` with no NUL `\0` and no lone
     UTF-16 surrogate) and not a non-empty `Uint8Array` (`bytes`). Examples: a number, a boolean, an array, an object,
     an empty string, a string that contains `\0` (`key_contains_nul`), an empty byte array, a lone UTF-16 surrogate,
     or a JSON path evaluated against a `bytes` or `text` base item.
   - **Size violation.** The encoded GSI hash key is larger than the `maxHashKeyBytes` of the base table, or the
     encoded GSI sort key is larger than its `maxSortKeyBytes` (Section 4.2.14).
3. **Entry that cannot be written (a violation):**
   - **Projected `'bytes'` attribute in a GSI `projection`.** A GSI `projection` builds a JSONB document
     (`kind = "json"`), which cannot store raw binary values (`Uint8Array`). Each projected cell that evaluates to
     `'bytes'` at run time is a violation, as `update_value_is_bytes` is in `UpdateExpression`. The validation of
     `GsiTargetDefinition` rejects a compile-time constant `'bytes'` expression in `projection`.
   - **Row size.** The GSI entry row (projected data + GSI hash key + `physicalSk` + fixed overhead) is larger than
     `MAX_ITEM_BYTES` (400 KB). `MaterializationDO` measures it with `estRowBytesExpr`, `itemDataExpr`, and
     `validateItemDataSize`.
   - **Target rejection.** The GSI returns `rejected` with a code other than `batch_request_too_large`, or a push
     target returns `{ outcome: "rejected", code }`.

A permanent failure of one item on a target must not fail the other items of the batch, block base outbox
acknowledgements, or trip the target circuit breaker. Thus a poison item does not retry forever. `MaterializationDO`
handles a violation in these steps:

1. **Clean up each earlier GSI entry.** For a GSI, the step sends `delete(K)` for each key `K` in the key set, with the
   rules of Section 4.2.7. Thus the GSI keeps no stale entry from an older valid revision. When a delete fails, the
   item gets a `retry` row, and a later step runs the cleanup again.
2. **Park the item.**
   - When the violating image is a tombstone (`latest_items.deleted_at_ms IS NOT NULL`), the completion logs the
     violation and deletes the `target_retry` row of the item, so that the tombstone can expire.
   - Otherwise, the completion sets `kind = 'violation'` and `last_error = code`, and writes one structured error log.
   - `idx_target_retry_due` indexes only `kind = 'retry'`, so a `violation` row costs the runner no work.
3. **Recover on a newer write.** When the item changes, the cursor sends the new image (rule 3 of "Retry rows").

#### 4.2.11 Tombstones and garbage collection

- The ingest of a delete writes the tombstone (Section 4.2.4, ingest step 2) and gives it a position.
- A background sweep on `idx_latest_items_deleted_gc` deletes a tombstone row when each condition is true:
  1. `newest_seq <= MIN(C_t)`: each target read the tombstone.
  2. No `target_retry` row exists for `item_id`.
  3. No `target_index_keys` row exists for `item_id`: each GSI deleted its entries.
- In the same `transactionSync`, the sweep subtracts the key bytes from `key_size_estimates` and deletes the positions
  of the item. Condition 1 puts these positions at or below `MIN(C_t)`. This keeps the link-ID delete rule of Section
  4.2.4.
- The sweep needs no time horizon:
  - A late export with a lower revision comes from a producer whose stored epoch at the `MaterializationDO` is above
    the epoch of the late call.
  - When the producer is the base partition that exported the delete, the same-epoch rule puts the delete in a call
    with a higher epoch than the late call, or the late call ended before the delete call started.
  - When the producer is an ancestor partition, the child exported the item with the split fences (Section 4.2.9).
- While a target is down, the tombstones stay until that target reads them. A tombstone is a key-only row.

#### 4.2.12 Splits and promotions

Section 4.2.1 gives the three tiers that split independently. Section 4.2.9 ("Split fences") gives the epoch rules of
a split.

A `MaterializationDO` split or promotion migrates these rows for the items of the slice:

1. `latest_items` with `item_id` and `newest_seq` unchanged.
2. `change_positions` with `seq` unchanged. The child gives each new image a position above each copied position.
3. `target_index_keys`.
4. `target_retry`, with `is_inherited = 1`.
5. `materialization_targets`, with `cursor_seq` unchanged. The child starts each `epoch` at 1.
6. `ancestor_fences`: the rows of the parent, plus `(parent DO ID, E, until_id)` for each target. `until_id` is the
   highest copied position.
7. `producer_fences`: the whole table, with `MAX(epoch)`. A child must reject each late call that its parent rejects.
8. The KV item `__fokosdb/mat/targets`: the stored target list of the parent (Section 4.2.13).

A base `PartitionDO` split or promotion migrates the `materialization_outbox` rows of Section 4.2.3. The child also
gets the `ancestor_fences` row `('export', parent DO ID, E, until_id)` and the export fence rows of the parent. The
child writes these rows in the `transactionSync` that ends the outbox stream, with `until_id = MAX(outbox_id)`. When
the slice has no outbox row, no work of the slice is in flight, and the child stores no export fence row.

A GSI `PartitionDO` split migrates the whole `producer_fences` table to each child with `MAX(epoch)`.

`cleanupSourceStep` deletes the moved rows of `latest_items`, `change_positions`, `target_index_keys`, and
`target_retry` on the source, with the link-ID delete rule. A target step on the source skips an unowned item, and its
position is then done (Section 4.2.6, target step 2).

The split fences need the final export state of the slice. `RepartitionSource.servePage`
(`packages/fokosdb/src/sharding/repartition-flow.ts`) rejects each page request before the cutover, for each stream of
`FokosMigrationHost`. Thus the new streams also serve pages only after the cutover.

#### 4.2.13 Target definitions and table destroy

A user declares GSIs and push targets in a subclass of `MaterializationDO` that overrides `targets()`. This is the same
pattern as `PartitionDO.fokosConfig()`:

```ts
export type GsiTargetDefinition = {
	kind: "gsi";
	targetId: string;
	hashKey: ExpressionValue;
	sortKey?: ExpressionValue;
	projection?: readonly ProjectionExpression[];
};

export type PushSinkTargetDefinition = {
	kind: "push_sink";
	targetId: string;
	/** Name of the Worker env binding: a service binding (`WorkerEntrypoint`) or a `DurableObjectNamespace`. */
	binding: string;
	/** RPC method name that receives a `PushTargetBatch`. */
	method: string;
	/** Target DO name when `binding` is a `DurableObjectNamespace`. */
	objectName?: string;
	projection?: readonly ProjectionExpression[];
};

export type FokosTargetDefinition = GsiTargetDefinition | PushSinkTargetDefinition;
```

- At startup, each `MaterializationDO` leaf validates `this.targets()`:
  - at most 10 GSIs,
  - unique, non-empty `targetId` values with no `~`,
  - valid `hashKey`, `sortKey`, and `projection` expressions.
- **The target list is fixed in the first version.** A partition stores the first valid `this.targets()` result in
  the KV item `__fokosdb/mat/targets`, and then uses only the stored list:
  - A root stores the list from its first request. A split or promotion child gets the stored list of its parent
    through the migration, not from `this.targets()`.
  - At each start, the partition reads `this.targets()` again and compares it with the stored list. When the two are
    different, it logs one error for each instance with the `targetId` values of both lists. It continues with the
    stored list.
  - Section 4.3.6 tracks how to add, remove, and change targets.
- The leaf stores each `targetId` and `kind` in `materialization_targets` (Section 4.2.6). Thus `destroy()` knows each
  GSI that was ever written.
- A helper in `packages/fokosdb/src/shared/do-stubs.ts` resolves `binding` (and `objectName` when present) from the
  Worker `env`.

##### Table destroy

`FokosDB.destroy()` destroys the shard groups of the table in this order: each writer before its receiver.

1. `fokos.tc.<tableName>`.
2. `fokos.p.<tableName>`. `fokosPrepareDestroy` fences each base partition, stops its in-memory `#exportPromise`, and
   waits for it.
3. `fokos.m.<tableName>`, when `nsMat` is configured. `fokosPrepareDestroy` fences each `MaterializationDO`, stops its
   in-memory `#pushPromise`, and waits for it. It collects the union of the GSI `targetId` values from
   `materialization_targets`.
4. `fokos.gsiv1.<n>.<tableName>.<indexName>` for each collected GSI `targetId`.

Each step is idempotent. A caller must not use `<tableName>` again until `destroy()` returns `{ ok: true }`.

Destroy is best effort for calls in flight:

- After `fokosPrepareDestroy`, no call of the current instance of a producer stays in flight.
- A call that a producer sent before a restart can still arrive after its receiver was destroyed. When the receiver is
  a root hash partition, the call creates it again, because a root takes its identity from its first request.
- A client call that is in flight during a destroy has the same effect today. A second run of `destroy()` deletes a
  root that a late call created again.
- If a later version needs a hard bound, each call can carry its send time, and a receiver can reject a call that is
  older than `callTimeoutMs`.

#### 4.2.14 GSI tables, composite sort key, and queries

Each GSI is a standard `FokosDB` table in the shard group `fokos.gsiv1.<n>.<tableName>.<indexName>`:

- `<indexName>` is `targetId` (non-empty, no `~`).
- `<n>` is `tableName.length` in UTF-16 code units. The length prefix makes each `(tableName, indexName)` pair
  unambiguous, also when both names contain `.`.
- `gsiv1` names the version of this encoding. A later naming scheme can use a new version (for example `gsiv2`) with
  no collision with the names of `gsiv1`.
- The shard group name is the address of the GSI data. Thus the `gsiv1` encoding must never change.

`MaterializationDO` and `db.gsi(indexName)` construct the internal GSI `FokosDB` client with `captureChanges = false`
and no transaction coordinator group.

##### `batchWriteItems` producer extension

`BatchWriteItemsOptions` (`packages/fokosdb/src/shared/batch-api-types.ts`) and `BatchWriteItemsRpcRequest`
(`packages/fokosdb/src/server/do-partition.ts`) add the optional fields `producerId`, `epoch`, and `ancestorFences`.
Only `MaterializationDO` sets them. When they are present, the `transactionSync` block of each item in
`apiBatchWriteItems` runs the receiver rules of Section 4.2.9. A fenced item returns `unprocessed` with
`producer_epoch_fenced`. The GSI `PartitionDO` stores `producer_fences` and migrates it on a split (Section 4.2.12).

##### Order-preserving composite sort key with `<L><flag><D>` trailer

Multiple base items can share `(gsiHK, gsiSK)`. The identity invariant of FokosDB makes item identity a byte
comparison, never a hash. Two facts of SQLite also apply:

- `replace(X, Y, Z)` is a `TEXT` function that truncates `BLOB` values at `0x00`.
- `instr(i.sk, x'0000')` scans each byte from offset 1.

To support byte-order B-tree range scans and `O(1)` `substr` extraction in SQLite,
`encodeGsiSortKey(gsiSK, baseHK, baseSK)` takes the `KeyBytes` of each component (`gsiSK` is `x''` when absent) and
builds `physicalSk`:

- Escape each `0x00` byte in `gsiSK`, `baseHK`, and `baseSK` as `0x00 0xFF`.
- Join `escape(gsiSK)`, `escape(baseHK)`, and `escape(baseSK)` with the 2-byte separator `0x00 0x00`.
- Append the trailer `<L><flag><D>`:
  - `<L>` is the decimal ASCII byte length of the unescaped `gsiSK` (`1` to `9` digits, `"0"` when absent).
  - `<flag>` at offset `-2` is `':'` (`0x3A`) when `gsiSK` contains no `0x00` byte. It is `'!'` (`0x21`) when `gsiSK`
    contains a `0x00` byte. In that case, the unescaped `rawGsiSK` comes directly before `<L>!<D>`.
  - `<D>` at offset `-1` is the ASCII digit `'1'..'9'` that gives the byte length of `<L>`.

`MaterializationDO` passes `physicalSk` to `batchWriteItems` as a `Uint8Array`. `KeyCodec.encode` prepends the binary
tag `0xFF`, so `i.sk` in SQLite is `0xFF || physicalSk`. Byte `n` of `physicalSk` is byte `n + 1` of `i.sk`:

```text
Case 1: gsiSK has no 0x00 byte (gsiSK = "2026-10-17", L = 10, D = 2):
  1 .. 10        11..12   13..14   15..16   17..18   19..22
+--------------+--------+--------+--------+--------+--------+
| "2026-10-17" | \x0000 |  "u1"  | \x0000 |  "o9"  | "10:2" |
+--------------+--------+--------+--------+--------+--------+
  ^----------^                                        ^ ^ ^
  gsiSK (10 B)                                        L : D

Case 2: gsiSK contains a 0x00 byte (gsiSK = \xff\x61\x00\x62, L = 4, D = 1):
  1 .. 5                6..7     8..9    10..11   12..13   14 .. 17          18..20
+---------------------+--------+-------+--------+-------+-----------------+-------+
| \xff\x61\x00\xff\x62| \x0000 | "u1"  | \x0000 | "o9"  | \xff\x61\x00\x62| "4!1" |
+---------------------+--------+-------+--------+-------+-----------------+-------+
  ^-----------------^                                     ^--------------^  ^ ^ ^
  escaped (for sort)                                      rawGsiSK (4 B)    L ! D
```

Properties of `0xFF || physicalSk`:

- **Uniqueness.** An escaped component never contains `0x00 0x00`, and items with an equal `gsiSK` share the same
  trailer. Thus two different base items never produce equal bytes.
- **Contiguous `gsiSK` ranges.** Each entry with `gsiSK = k` starts with `escape(k) || 0x00 0x00`. Thus entries sort
  by `gsiSK`, then by `baseHK`. In the same `(gsiSK, baseHK)`, entries sort by the stored bytes of
  `escape(baseSK) || trailer`. DynamoDB also does not specify the order of identical GSI keys.

In SQLite, `CAST(substr(i.sk, -2 - D) AS INTEGER)` stops at the non-digit `<flag>`:

```sql
CASE WHEN substr(i.sk, -2, 1) = x'3a'
  THEN substr(i.sk, 2, CAST(substr(i.sk, -2 - CAST(substr(i.sk, -1) AS INTEGER)) AS INTEGER))
  ELSE substr(
    i.sk,
    -2 - CAST(substr(i.sk, -1) AS INTEGER) - CAST(substr(i.sk, -2 - CAST(substr(i.sk, -1) AS INTEGER)) AS INTEGER),
    CAST(substr(i.sk, -2 - CAST(substr(i.sk, -1) AS INTEGER)) AS INTEGER)
  )
END
```

When the GSI has no sort key (`"0:1"`), `substr(i.sk, 2, 0)` returns `x''`.

Measured in SQLite 3.45.1 over 20,000 rows (the average of 25 runs) for a compiled `{ ref: "sortKey" } = ?` filter:

| Sort key size | Trailer | Direct `i.sk` (base table) | `instr` scan (no trailer) | Two-step `<L><flag><D>` trailer |
| --- | --- | --- | --- | --- |
| **10 B** | `4 B` (`10:2`) | `1.88 ms` | `4.71 ms` (`2.57x`) | **`4.75 ms` (`2.53x`)** |
| **64 B** | `4 B` (`64:2`) | `2.23 ms` | `7.03 ms` (`3.40x`) | **`5.76 ms` (`2.58x`)** |
| **256 B** | `5 B` (`256:3`) | `3.94 ms` | `16.60 ms` (`3.49x`) | **`8.15 ms` (`2.07x`)** |
| **1024 B** | `6 B` (`1024:4`) | `8.13 ms` | `41.44 ms` (`6.25x`) | **`12.37 ms` (`1.52x`)** |
| **5000 B** | `6 B` (`5000:4`) | `20.64 ms` | `167.65 ms` (`7.44x`) | **`30.04 ms` (`1.46x`)** |

##### Query translation and `{ ref: "sortKey" }` compilation

`db.gsi(indexName).queryItems(...)` needs no GSI schema on the server:

1. **Client translation of `sortKeyCondition`.** Let `E(k) = escape(KeyCodec.encode(k)) || 0x00 0x00` and
   `succ(E(k)) = escape(KeyCodec.encode(k)) || 0x00 0x01`. The client validates each caller bound against the
   `maxSortKeyBytes` of the base table. Then it translates `sortKeyCondition` into a binary
   `{ op: "range", lower, upper }` over `physicalSk`:

| Caller `sortKeyCondition` | `lower` bound on `physicalSk` | `upper` bound on `physicalSk` |
| --- | --- | --- |
| `{ op: "eq", value: k }` | `E(k)` (inclusive) | `succ(E(k))` (exclusive) |
| `{ op: "gte", value: k }` | `E(k)` (inclusive) | unbounded |
| `{ op: "gt", value: k }` | `succ(E(k))` (inclusive) | unbounded |
| `{ op: "lte", value: k }` | unbounded | `succ(E(k))` (exclusive) |
| `{ op: "lt", value: k }` | unbounded | `E(k)` (exclusive) |
| `{ op: "between", lower: a, upper: b }` | `E(a)` (inclusive) | `succ(E(b))` (exclusive) |
| `{ op: "begins_with", prefix: p }` | `escape(encode(p))` (inclusive)* | `escape(successor(encode(p)))` (exclusive)* |

*\*When `p` is empty (`""` or `new Uint8Array(0)`), both `lower` and `upper` are unbounded. When
`KeyCodec.successor(encode(p))` is `null`, `upper` is unbounded.*

2. **`{ ref: "sortKey" }` in GSI `filter` and `projection` expressions.** On a `fokos.gsiv1.` shard group, the
   expression compiler (`packages/fokosdb/src/shared/expression/compiler.ts`) compiles `{ ref: "sortKey" }` as follows:
   - `referencePresent("sortKey")`: `(i.hk IS NOT NULL AND substr(i.sk, 2, 2) != x'0000')`.
   - `referenceType("sortKey")`: `keyType("substr(i.sk, 2)", "i.hk IS NOT NULL AND substr(i.sk, 2, 2) != x'0000'")`.
     It is `'bytes'` when `substr(i.sk, 2, 1) = x'ff'`, `'text'` when present and not `x'ff'`, and `'missing'`
     otherwise.
   - `referenceValue("sortKey")`: the `<L><flag><D>` `substr` expression above. In `logical` mode,
     `logicalKeyValue` wraps it.
3. **Response decoding.** The pagination cursor keeps all the `physicalSk` bytes. On returned items with no
   projection, `db.gsi(indexName)` decodes `physicalSk` into `{ sortKey: gsiSK, baseHashKey: baseHK, baseSortKey:
   baseSK }`.
4. **Version.** This spec does not change `version` or `{ ref: "v" }`. On a GSI, both give `items.v` of the GSI entry,
   not the version of the base item. Section 4.3.5 tracks a base version on GSI queries.

##### Derived GSI key limits

`MaterializationDO` and `db.gsi(indexName)` configure the internal GSI `FokosDB` client with these limits. `h` and `s`
are the `maxHashKeyBytes` and `maxSortKeyBytes` of the base table.

- `maxHashKeyBytes = h`.
- `maxSortKeyBytes = 1 + 2 * (2 * s + h) + 4 + s + digits(s) + 2`:
  - `1` is the `0xFF` tag.
  - `2 * (2 * s + h)` covers the worst-case `0x00` escape of `gsiSK`, `baseHK`, and `baseSK`.
  - `4` covers the two `0x00 0x00` separators.
  - `s + digits(s) + 2` covers the `'!'` trailer.
- With the default limits (`h = 1024`, `s = 512`), `maxSortKeyBytes` is 4,618 bytes.

#### 4.2.15 Backpressure, circuit breaking, and cost

##### Circuit breaking

Each `MaterializationDO` keeps one circuit breaker for each target, in memory. A breaker does not cover one
destination GSI partition:

- When calls to a target fail as a whole (each item fails with a code that can be retried), `MaterializationDO` opens
  the circuit breaker of that target. It stops claiming batches for the target, and sends one probe batch every
  `probeIntervalMs` (default 5 s) until a probe succeeds.
- When only some items of a call fail, for example the items of one GSI partition that migrates, the breaker stays
  closed. Each failed item retries on its own (Section 4.2.10), and the healthy items continue. Thus the per-item retry
  keeps a bad GSI partition apart from the healthy partitions, and `MaterializationDO` tracks no GSI partition.

The size limits do not change: `MaterializationDO` and the base `PartitionDO` use the existing `1.0x` split threshold
and the `1.1x maxSizeMb` admission limit. A partition that rejects a write above `1.1x` queues its split.

##### Four-stage backpressure

Pressure moves upstream in four stages:

1. **Stage 1 (target down).** The target circuit breaker opens in `MaterializationDO`. Incoming base exports still
   update `latest_items` and the change positions. Base writes and healthy targets get no delay.
2. **Stage 2 (`MaterializationDO` reaches `1.0x maxSizeMb`).** `MaterializationDO` splits into `hashSplitN` children
   (or promotes a hot key). It does not wait for the down target to recover. Each split multiplies the storage
   capacity.
3. **Stage 3 (`MaterializationDO` migrating, or at the split floor and at `1.1x maxSizeMb`).** `MaterializationDO`
   rejects live upserts (`deleted: false`) in `applyLatestImages` with `partition_migrating` or
   `partition_over_size`. It still accepts deletions (`deleted: true`). Base `PartitionDO` instances keep and coalesce
   compact `materialization_outbox` rows, and base writes continue to succeed.
4. **Stage 4 (base `PartitionDO` reaches `1.1x maxSizeMb`).** The base `PartitionDO` splits. At the split floor, it
   rejects new writes with `partition_over_size`.

Example of the capacity during a 2-hour outage of 1 of 3 GSIs at 100 base writes/s. The outage has 720,000 mutations
in total: 72,000 new items and 648,000 mutations across 50,000 existing items.

- The base `materialization_outbox` stays near 0 rows, with 0 ms of extra write latency.
- `MaterializationDO` coalesces the 648,000 updates into 50,000 changed items, which is 83% less GSI work.
- An item that is created and deleted during the outage has no key set for the down GSI. Its tombstone then converges
  with 0 RPCs to that GSI.
- 122,000 items change. Each changed item has at most 2 positions, so `change_positions` holds at most 244,000 rows.
  TODO: measure the size of a `change_positions` row.
- When the GSI recovers, the in-memory runner drains the 122,000 to 172,000 `batchWriteItems` operations in 1.5 to 2.5
  minutes, at 1,000 to 2,000 items/s across 4 `MaterializationDO` partitions. The healthy targets continue to share
  the round-robin workers.

##### Cost of one base write

SQLite table rows written for each base write, with no coalescing, for a table with `T` GSIs (the worst case: 1 export
for each write):

| Base write | Today | Base partition | `MaterializationDO` | GSI partitions | Total | `T = 10` |
| --- | --- | --- | --- | --- | --- | --- |
| Put, new item | 2 | 2 + 3 | 4 + T | 2T | 9 + 3T | 39 |
| Put, every GSI key stays | 2 | 2 + 2 | 4 | 2T | 8 + 2T | 28 |
| Put, every GSI key moves | 2 | 2 + 2 | 4 + 2T | 5T | 8 + 7T | 78 |
| Delete | 3 | 3 + 2 | 6 + T | 3T | 11 + 4T | 51 |

- **Base partition.** A put writes `items` and `key_size_estimates` (2). A delete also writes `deletion_metadata` (3).
  Capture adds 3 outbox writes for a new item: the upsert, the `is_local_only = 0` claim, and the delete on the
  acknowledgement. It adds 2 for an update or a delete, because their rows have `is_local_only = 0` and the claim
  writes no row.
- **`MaterializationDO` ingest.** `latest_items` and `key_size_estimates` (2), and the position insert and its cleanup
  delete (2). Step 2 of Section 4.2.5 writes no position when no target read the newest position.
- **GSI key sets.** A new key costs 1 row. A key move costs 2 rows: the add and the removal. A delete costs 1 row: the
  delete of the key set. A key that stays costs 0 rows.
- **Delete.** The tombstone GC deletes `latest_items` and updates `key_size_estimates` (2).
- **GSI partitions.** A put writes `items` and `key_size_estimates` (2). A delete writes 3 rows.
- **Push target or sparse GSI.** A push target costs 0 rows for each item in `MaterializationDO` and 0 rows in FokosDB.
  A sparse GSI with an absent key and no key set costs 0 rows.
- **Left out of the table:**
  - The cursor update costs 1 row for each batch of up to 100 items: about 0.01 rows for each item and each target.
    `is_batch_open` costs at most 1 more row for each period of target work.
  - A `producer_fences` row costs 1 row the first time that a producer calls a receiver, and 1 row when its epoch goes
    up.
  - An epoch raise costs 1 row at the producer.
  - `is_call_open` costs at most 2 rows in the base partition for each period of export work.
  - A failed item costs about 2 rows: the insert of its `retry` row and its delete.
  - A `retry` row that a split copies adds 1 entry to `idx_target_retry_inherited` at the import, and removes 1 entry
    when the row is deleted or becomes a `violation` row.
  - The ingest of an item with `retry` rows costs 1 row for each such row.

When `N` base writes of one item coalesce before the export, the cost is `3N` base rows and one downstream export and
push cycle. Secondary index updates can add billed row writes. TODO: measure the billed `rowsWritten` in the Workers
runtime.

One live item with full-item projections across `T` GSIs stores `2 + T` payload copies: base `items`, `latest_items`,
and `T` GSI rows. Each `target_index_keys` row adds the bytes of its GSI keys. TODO: measure the size of a
`target_index_keys` row.

##### Trade-offs

- **Reads.** Each target reads the payload of each changed item, so a payload is read `T` times. A row read costs
  1,000 times less than a row written (Section 1.3).
- **No time in correctness.** No rule waits for a deadline, `maxClockSkewMs`, or a horizon. After an `unknown`
  outcome, the target sends again at once. A split needs no wait.
- **Permanent receiver rows.** Each receiver keeps one `producer_fences` row for each producer that ever sent to it.
- **Interface changes.** `batchWriteItems` needs the GSI-only options `producerId`, `epoch`, and `ancestorFences`, and
  the code `producer_epoch_fenced`. `applyLatestImages` needs the same three fields.
- **Tombstones during an outage.** A target that is down keeps the tombstones of all deleted items until it recovers.
  A tombstone is a key-only row.

#### 4.2.16 Item lifecycle

Item X has the base key `(hk = "u1", sk = "o9")`. It lives in base partition P and in `MaterializationDO` M as
`item_id = 42`. The targets are the GSI `byStatus` (hash key `status`) and the push target `lake`. The GSI partitions
are `G_open` and `G_shipped`. The numbers in parentheses are rows written.

1. **Put revision 1, `status = "OPEN"`.**
   - P writes `items`, `key_size_estimates`, and the outbox row (3). The exporter claims the row (1) and sends
     `applyLatestImages` with epoch 1.
   - M checks the epoch of P and the revision. It writes `latest_items` (`newest_seq = 100`), `key_size_estimates`, and
     position 100 (3). P deletes the outbox row on the acknowledgement (1).
   - The `byStatus` step reads position 100. `D = OPEN` is not in the key set, so the claim writes `{OPEN}` (1). It
     sends `put(OPEN)` to `G_open` with M's epoch 1. `G_open` writes the entry (2), plus one `producer_fences` row the
     first time that M calls it (1).
   - The `lake` step pushes revision 1. Each target moves its cursor at the end of its batch (1 for each batch).
   - The cleanup deletes position 100 when `MIN(C_t) >= 100` (1).
2. **Put revision 2, `status = "SHIPPED"`.**
   - P writes the item and the outbox row (3), the claim (1), and the acknowledgement delete (1).
   - M accepts revision 2 (3). The `byStatus` claim adds `SHIPPED`: `{OPEN, SHIPPED}` (1).
   - The first call sends `put(SHIPPED)` to `G_shipped` (2). After it applies, the second call sends `delete(OPEN)` to
     `G_open` (3). The completion removes `OPEN`: `{SHIPPED}` (1).
   - The `lake` step pushes revision 2.
3. **Delete, revision 3.**
   - P deletes the item, writes `key_size_estimates` and `deletion_metadata`, and writes the outbox tombstone (4). Then
     the claim (1) and the acknowledgement delete (1).
   - M writes the tombstone and position 310 (3). The `byStatus` step sends `delete(SHIPPED)` to `G_shipped` (3). After
     it applies, the completion deletes the `target_index_keys` row (1).
   - The `lake` step pushes `{revision: 3, deleted: true}`.
   - When `MIN(C_t) >= 310`, the sweep deletes the tombstone and updates `key_size_estimates` (2).

Failure: `put(SHIPPED)` in step 2 has an `unknown` outcome.

1. `{OPEN, SHIPPED}` is already in SQLite.
2. M raises the `byStatus` epoch to 2 (1). It reads again from `C_t` and sends `put(SHIPPED)` and then `delete(OPEN)`
   with epoch 2.
3. When the late `put(SHIPPED)` with epoch 1 arrives before the new put, the new put overwrites it. When it arrives
   after, `G_shipped` has stored epoch 2 and rejects it.
4. The response of the old call is discarded.

Split: M splits during step 1, while `put(OPEN)` is in flight.

1. M raises the `byStatus` epoch to `E` before it serves the first export state page.
2. The child M2 copies item 42, position 100, `{OPEN}`, `cursor_seq`, and the fence `(M, E, until_id)`.
3. M2 reads position 100 and sends `put(OPEN)` with its own epoch 1 and the fence `(M, E)`.
4. When the late `put(OPEN)` of M arrives at `G_open` after the call of M2, `G_open` has stored `E` for M and rejects
   it. When it arrives before, the call of M2 overwrites it.

### 4.3 Future Follow-ups

These features are not part of Milestones 1 to 4. Each one can ship later on its own.

#### 4.3.1 Online target backfill

A user can deploy a new GSI or push target on a table that already holds data in `latest_items`. The cleanup deletes
the positions that all earlier targets passed, so the new target cannot read the existing items from
`change_positions`. Each `MaterializationDO` leaf then waits until its `canSweepLocally` guard is true and each parent
import is complete. Then it:

1. sets `cursor_seq` of the new target to the highest position, so that each change during the scan reaches the
   target through a position,
2. records `backfill_cursor_item_id = 0` in `materialization_targets`,
3. runs a bounded background scan over `latest_items`
   (`WHERE item_id > backfill_cursor_item_id AND deleted_at_ms IS NULL`) and sends each item to the new target with
   the target step.

The follow-up must define how the scan and the cursor of the same target keep the same-epoch rule (Section 4.2.9).

`MaterializationDO` stores a full mirror in `latest_items.data`. Thus the backfill of a new target reads nothing from
the base `PartitionDO` partitions.

#### 4.3.2 `FokosDBAutoAdminDO` catalog and activation status

Some applications need a cluster-wide `BACKFILLING | ACTIVE` status check when they add a GSI online. For them, a
singleton `FokosDBAutoAdminDO` (`fokos.admin.<tableName>`) can walk the `fokos.m.<tableName>` owners. It reports the
target as `ACTIVE` when each initialized leaf has:

- finished its import (`importState === null || importState === "imported" || importState === "active"`),
- completed its `latest_items` backfill scan,
- moved the cursor of the new target past the position that it set at the start of the backfill.

#### 4.3.3 Per-GSI topology and key-limit overrides

A workload can need a GSI with a higher `rootTreesN`, or a different `maxHashKeyBytes` or `maxSortKeyBytes`, than the
base table. `FokosTableOptions` (or `db.gsi(indexName, opts)`) can then carry these overrides for each GSI. Thus
`MaterializationDO` and the caller `FokosDB` client construct identical `FokosRouteContext` topologies for that GSI
shard group.

#### 4.3.4 Capture on a table that has data

This spec allows `captureChanges` only on a new table (Section 4.2.3). A later spec can enable capture on a table that
already holds items. It must solve two problems:

- An item that gets no write after the policy change has no outbox row, so it never reaches `latest_items`.
- A partition that has not yet received the new policy writes no outbox row.

A possible design:

1. The policy change turns on capture with a new `policyVersion`. Each base partition that gets it starts to write
   outbox rows for new mutations.
2. Each base partition runs a bounded background scan over `items` with a durable cursor. The scan writes an outbox
   row for each item that has no outbox row. The export path of Section 4.2.3 then sends the items, and the revision
   rules of `MaterializationDO` drop each duplicate.
3. A split or a promotion migrates the scan cursor with the slice.
4. A catalog, for example `FokosDBAutoAdminDO` (Section 4.3.2), walks the base partitions and reports the capture as
   active when each owner has finished its scan. Then the targets can backfill from `latest_items` (Section 4.3.1).

#### 4.3.5 Base version on GSI queries

Each GSI is a standard `PartitionDO` table. Thus `items.v` in a GSI partition counts the writes to that
`(gsiHK, physicalSk)` entry, not the `version` (`revision`) of the base item. This spec keeps the current behavior:
`version` and `{ ref: "v" }` on a GSI give that local `items.v` (Section 4.2.14).

A later spec can change this:

- `db.gsi(indexName).queryItems` can omit `version`, and the compiler can reject `{ ref: "v" }` in a GSI `filter` or
  `projection`.
- `MaterializationDO` can store the base `revision` in each GSI entry. `db.gsi(indexName).queryItems` then returns it
  as `version`, and `{ ref: "v" }` reads it.
- A caller-provided `external_version` on base writes can travel with the item to the GSIs and the push targets.

#### 4.3.6 Target changes and target versions

The first version fixes the target list of each partition (Section 4.2.13). A later spec must define how to add,
remove, and change targets. During a gradual rollout, old and new Worker versions run at the same time. One
`MaterializationDO` can restart on the old version after it ran on the new version, so it can see the two lists one
after the other. Thus a list change must not depend on the code version that a partition runs. The spec must solve
these problems:

1. **Versions.** Each target list has an integer version, for example `targetsVersion`, as `policyVersion` has. A
   partition stores a list only when its version is above the stored version, and it ignores a list with a lower
   version. Thus an oscillation between two code versions cannot move a partition back.
2. **One list across the shard group.** In the first version, each root stores the list from its first request. A
   root that gets its first request after a code change can store a different list. A version scheme, or a catalog
   such as `FokosDBAutoAdminDO` (Section 4.3.2), must give all partitions the same list.
3. **Fixed definitions.** A `targetId` keeps one definition for its whole life. A changed definition is a new
   `targetId` and the removal of the old `targetId`. Thus no GSI holds entries of two definitions.
4. **Add.** A new target starts its cursor at the highest position, and then runs the backfill of Section 4.3.1. The
   GSI must not serve queries until each partition finished the backfill (Section 4.3.2).
5. **Remove.** A bounded background job deletes the rows of the target (cursor, key sets, retry rows). The target then
   no longer counts in `MIN(C_t)` and no longer blocks the tombstone GC and the position cleanup. `destroy()` still
   finds the GSI shard group through `materialization_targets`, and a separate step can destroy it.
6. **A target that is broken for good**, for example a deleted service binding, keeps all tombstones and positions
   until it recovers. The removal of item 5 is the way out, so it must not need the target to answer.

## 5. Alternative Options

### 5.1 Base outbox, base shadow, and stateless Queue fan-out

This option keeps `_fokos_change_pending (baseKey, version, newImage)` and
`_fokos_change_shadow (baseKey, lastShippedKeyAttrs)` in the base `PartitionDO`. It sends batches to a Cloudflare
Queue. Stateless queue consumers compute `oldGsiKey -> newGsiKey` and apply LWW writes and tombstones to the GSIs. It is
rejected for five reasons:

1. **Permanent orphan GSI entries under `A -> B -> C` after a crash between `queue.send()` and the shadow update.**
   `await env.QUEUE.send()` and SQLite `transactionSync` cannot commit atomically:
   - The DO sends `{ old: A, new: B, v: 2 }` and crashes before it sets `shadow = B`.
   - A client then writes `v3 (C)`. The next alarm coalesces the outbox to `v3 (C)` with `shadow = A` and sends
     `{ old: A, new: C, v: 3 }`.
   - Both messages delete `A`, and `B@v2` stays in the GSI forever.
   - To set `shadow = B` before `queue.send()` has the opposite defect when `queue.send()` fails: `A@v1` stays forever.
2. **`last_write_ts` does not strictly increase for each item.** Two writes in `maxClockSkewMs` can share the same
   `last_write_ts`, which breaks timestamp-based LWW.
3. **The 128 KB limit of Cloudflare Queues is less than the 400 KB item limit.** An item above 128 KB cannot fit in a
   Queue message. A pointer fails when the base item changes or is deleted before all consumers read it.
4. **Base partition bloat and coupling.** A `newImage` in the base outbox duplicates 400 KB payloads during downstream
   outages. `lastShippedKeyAttrs` couples the base partition to all 10 GSI key extractors.
5. **Tombstone retention depends on the age of the Queue and its DLQ.** Delayed queue messages force GSI partitions to
   keep tombstones for days.

### 5.2 Separate `GsiMaintainerDO` worker tier

A separate DO tier that claims dirty work from `MaterializationDO` is rejected. `MaterializationDO` already shards by
`(baseHK, baseSK)` and holds `latest_items` and the target state in local SQLite. A push directly from
`MaterializationDO` has no extra DO hop and no distributed work leases.

### 5.3 Pull cursors (`change_seq`) for push targets

A partition-local `change_seq` for external pull consumers is rejected. With pull cursors, each consumer must track
partition splits and join the cursor epochs of a parent and its children. The cursors of Section 4.2.6 are internal to
`MaterializationDO` and move with its migration. Thus no consumer sees a split.

### 5.4 One sidecar `MaterializationDO` for each base `PartitionDO`

A sidecar DO `<part>.mat` for each base partition `<part>` is rejected for two reasons:

- A base split or promotion forces the sidecar to split, also when the materialization state is small.
- Out-of-order exports from an old parent and a new child no longer arrive at the same `MaterializationDO`.

### 5.5 A hash or a local integer ID in the GSI sort key

1. **Fixed-size hash suffix `H128(baseHK, baseSK)`.** Rejected. Two base items can share `(gsiHK, gsiSK)` and have the
   same hash. Then the `put` of one item overwrites the other item, and a `delete` deletes the other item.
2. **Partition-local `(baseHK, baseSK) -> id` lookup tables, or reference-counted `KeyBytes -> id` lookup tables.**
   Rejected for three reasons:
   - They double or triple `rowsRead` and `rowsWritten` on puts, deletes, and queries.
   - GSI tables become different from standard `PartitionDO` tables.
   - A split migration must compute the reference counts again across the child slices.

### 5.6 Per-item target state with a write deadline fence

**Status:** Rejected.

This option replaces the change positions, the cursors, the key sets, `target_retry`, and the producer epochs of
Section 4.2 with one state row for each item and each target:

- **`target_item_state`.** One rowid row for each `(item_id, target_id)` holds the applied revision and GSI key
  (`applied_revision`, `applied_index_hk`, `applied_index_sk`), `desired_revision`, the in-flight transition
  (`inflight_revision`, `inflight_index_hk`, `inflight_index_sk`, `inflight_phase`, `inflight_attempt`,
  `inflight_apply_before_ms`), `status` (`'IDLE'`, `'DIRTY'`, `'IN_FLIGHT'`, or `'VIOLATION'`), `next_attempt_at`,
  `last_error_code`, and `violation_revision`. The partial index
  `idx_target_item_state_due (target_id, next_attempt_at, item_id) WHERE status IN ('DIRTY', 'IN_FLIGHT')` gives the
  due rows.
- **Ingest.** The ingest raises `desired_revision` on the row of each target. It moves each row that is not
  `'IN_FLIGHT'` to `'DIRTY'` with `next_attempt_at = 0`.
- **State machines.** Before each call, the runner freezes the transition in the row. A GSI runs
  `WRITE_NEW -> DELETE_OLD -> COMMIT_APPLIED`. A push target runs `PUSH_BATCH -> COMMIT_APPLIED`. A violation parks
  the row in `'VIOLATION'`, and a write with `revision > violation_revision` moves it back to `'DIRTY'`.
- **Write deadline fence.** Each export call and each GSI call carries `applyBeforeMs = now + callDeadlineMs`
  (default 30 s). The receiver rejects an item with `write_deadline_passed` when its clock is above `applyBeforeMs`.
  After an `unknown` outcome, the next call for the row waits until `applyBeforeMs + maxClockSkewMs` (default
  2,000 ms). A split child waits for the same deadline.
- **Tombstone horizon.** A `latest_items` tombstone stays for `deletedRevisionHorizonMs` (default 24 hours) after the
  last target finished the delete. Thus `MaterializationDO` rejects a late export with a lower revision only during
  that horizon. A push target must keep the revision of a deleted key for the same horizon.
- **Destroy.** `destroy()` waits until the highest `apply_before_ms` (base) or `inflight_apply_before_ms`
  (`MaterializationDO`) of the calls that survived a restart, plus `maxClockSkewMs`.

| Topic | Section 4.2 | This option |
| --- | --- | --- |
| `MaterializationDO` rows for each put, every GSI key stays | 4 | 2 + 3T |
| `MaterializationDO` rows for each put, every GSI key moves | 4 + 2T | 2 + 4T |
| `MaterializationDO` rows for each delete | 6 + T | 4 + 3T |
| Total rows, `T = 10` (key stays / key moves / delete) | 29 / 79 / 52 | 57 / 97 / 70 |
| `MaterializationDO` rows for each item and push target | 0 | 3 |
| Protection against a late call | Producer epochs | `applyBeforeMs` and `maxClockSkewMs` |
| Tombstone retention | Until each target read the tombstone | 24 hours after the last target finished |
| Wait after an `unknown` outcome | None | About 32 s |
| Tables for the target state | 6 | 1 |

Pros:

1. One row gives the full state of an item for a target. It is simple to read, to debug, and to test.
2. A retry or a violation updates the row of the item. It needs no retry table and no rule for a failure of the whole
   call.
3. It needs no change positions, no producer epochs, no split fences, and no `producer_fences` rows in the receivers.
4. It was fully specified, and `maxClockSkewMs` (default 2,000 ms) already exists in the host configuration.

Cons:

1. **About 2 times the rows written.** At 100 base writes/s with `T = 10` and no coalescing, this option writes about
   5,700 rows/s against 2,900 rows/s. At $1.00 per million rows, this is about $14,800 against $7,500 for each month.
2. **Clock skew becomes a safety rule.** Today `maxClockSkewMs` only bounds the clock of a `prepare` in
   `TransactionParticipant`. With this option, a clock offset above `maxClockSkewMs` between two Durable Objects can
   leave an orphan GSI entry.
3. **A time horizon in correctness.** A late export that arrives after `deletedRevisionHorizonMs` can create a deleted
   item again.
4. **Latency after a failure.** After an `unknown` outcome, the item waits about 32 s before its next call.

It is rejected because it writes about 2 times the rows, and because its correctness depends on clocks (Requirement 9).

### 5.7 Compact per-item target state with writes only on a GSI key change

**Status:** Rejected.

This option keeps `target_item_state` of Section 5.6 and removes seven columns: `applied_revision`,
`desired_revision`, `inflight_revision`, `inflight_phase`, `inflight_attempt`, `inflight_apply_before_ms`, and
`violation_revision`:

- It infers the desired revision from `latest_items.revision`, and the in-flight guard from the 1 batch in flight of
  each target.
- It writes the row before a call only when `WRITE_NEW` writes a GSI key that is not yet in `applied_index_*`. When the
  key stays, the key is already durable. A `delete` or a push writes no new key.
- One value `inflight_until = applyBeforeMs + maxClockSkewMs` in `materialization_targets` fences the calls of a target
  after a crash. A split child copies it with `MAX` and waits until it passes.

| Base write | `MaterializationDO` rows | Total, `T = 10` |
| --- | --- | --- |
| Put, every GSI key stays | 2 + 2T | 47 |
| Put, every GSI key moves | 2 + 3T | 87 |
| Delete | 4 + 2T | 60 |

A push target costs 2 rows in `MaterializationDO` for each item.

Pros: it keeps the per-item state of Section 5.6, with no change positions, no producer epochs, and no split fences.

It is rejected because it still writes about `2T` rows for each base write, and because its correctness still depends
on `maxClockSkewMs` and on a wait after a restart or a split (Requirement 9).

### 5.8 Versioned GSI tombstones with a TTL instead of GSI-hop epochs

**Status:** Rejected. Section 4.2 uses producer epochs on all hops. This option changes only the GSI hop. It uses the
versioned writes of `docs/ideas/2026-10-21-idempotent-sink-writes.md`.

Each GSI entry stores the base `revision`. A `put` applies only when its revision is at or above the stored revision.
A `delete` writes a key-only tombstone with the revision and a TTL, instead of removing the row. The item TTL sweep
removes the tombstone. The TTL must be longer than the life of any call, for example 1 hour.

Changes to Section 4.2:

- **Removed from the GSI hop:**
  - the producer ID, the epoch, and the ancestor fences in `batchWriteItems`,
  - `producer_fences` in GSI partitions,
  - the epoch raise after an `unknown` outcome, and the `is_batch_open` marker.

  A late call has a revision at or below the stored revision or tombstone, so the GSI partition rejects it. This is
  also true for a late call of a parent `MaterializationDO`.
- **Unchanged:** the change positions, the cursors, the applied map, `target_retry`, the GSI key sets, and the two
  calls in sequence for a key move. A tombstone still needs its GSI key, so the key set must still store the key
  before the `put`.
- **The base hop is a separate choice.** When the `latest_items` tombstones also expire after a time horizon, the
  base hop can drop its epochs, its fences, its cold-start check, and the same-epoch rule. When they do not, the base
  hop keeps the rules of Section 4.2.9.
- **Added:**
  - a revision condition in the GSI path of `batchWriteItems`,
  - tombstone rows in GSI partitions,
  - a tombstone filter in each GSI query.

Pros:

1. Fewer rules on the GSI hop: no epochs, no fences, and no response rule for old epochs. The checks before each
   write stay.
2. GSI writes do not depend on order. Copies of a write in any order give the same final state while the tombstone
   exists. A retry needs no coordination.
3. Each GSI entry holds the base revision, which the follow-up of Section 4.3.5 needs.
4. The receiver state expires. `producer_fences` rows stay forever, but a tombstone goes after its TTL.

Cons:

1. **A time bound in correctness.** When a late call arrives after its tombstone expired, the call creates an orphan
   entry. A TTL of 1 hour makes this case unlikely. Only a receiver deadline check (`applyBeforeMs`) gives a hard
   bound, and that check depends on `maxClockSkewMs`.
2. **More rows written.** A GSI delete writes the tombstone row and `key_size_estimates` (2), and the sweep later
   deletes them (2 to 3). A plain delete writes 3 rows. This is about 1 to 2 more rows for each GSI delete or key
   move. With `T = 10`, a key move costs about 89 to 99 rows instead of 79, and a delete about 62 to 72 instead of
   52 (Section 4.2.15). TODO: measure.
3. **Storage for the TTL.** A tombstone holds the GSI hash key and `physicalSk`, which holds the base keys. For
   example, 100 deletes or key moves each second with a 1-hour TTL keep 360,000 tombstones in each GSI. TODO: measure
   the size of a tombstone.
4. **Permanent splits.** Tombstones count in the partition size, and partitions never merge (`RepartitionKind`). A
   burst of deletes can split GSI partitions, and the partitions stay split after the tombstones expire.
5. **Query cost.** Each GSI query reads the tombstones in its range and skips them. A page with a `limit` can hold
   fewer items.
6. **Revision ties.** When a `targets()` change alters the sparse rules of a GSI, one revision can be present at a key
   and absent at the same key. The `put` and tombstone rules then need a rule for equal revisions.

It is rejected, because cons 1 to 4 cost more than the rules that it removes. Section 4.3.5 can use the revision of
each GSI entry without the tombstones.

### 5.9 A time limit for the split fences of a target

The child keeps the fences of target `t` while `C_t < until_id`, and for at least 24 hours after the split. This rule
replaces the check for inherited `retry` rows (Section 4.2.9, "Split fences", step 5).

- **Pro.** It needs no `idx_target_retry_inherited` and no `is_inherited` column. The index has entries only for a short
  time after a split.
- **Con.** The fences protect against a late parent call. No platform limit gives a maximum time for a call in flight,
  so the 24 hours are a guess.
- **Con.** Correctness then depends on a clock and a time horizon (Requirement 9).

It is rejected because of Requirement 9. The index costs rows only for the `retry` rows that a split copies, and a
call that fails as a whole writes no `retry` row (Section 4.2.10).

## 6. Frequently Asked Questions

### 6.1 Do GSIs and push targets (for example a data lake) share the same state machine?

Yes. Both read the same change positions with their own cursor and epoch, and both use `target_retry` for failed
items (Sections 4.2.5 to 4.2.10). A GSI also keeps a key set, and its step sends `put` and then `delete` with
`FokosDB.batchWriteItems`. A push target sends one `PushTargetBatch` over Workers RPC.

### 6.2 Why does `MaterializationDO` keep a full mirror of live items?

With `latest_items.data` for live items, `MaterializationDO` can evaluate projections again on a retry. Future online
targets (Section 4.3.1) can also backfill from local SQLite with zero reads from the base `PartitionDO` shard group.

### 6.3 What happens when an item is created, updated, and deleted while `MaterializationDO` is unreachable?

While the circuit breaker to `MaterializationDO` is open, the exporter claims no rows. Thus the outbox row of a new
item keeps `is_local_only = 1`, and the local delete removes it in place with zero RPCs (Section 4.2.3). When a claim
already set `is_local_only = 0` before the breaker opened, the delete writes an outbox tombstone. The exporter sends
it when `MaterializationDO` is reachable again.

### 6.4 Does the change of `max_deleted_v` to `before.v + 1` change the existing transaction behavior?

No. `transactGetItems` still detects each create and each delete between phase 1 and phase 2. New rows still start
at `d.max_deleted_v + 1`, strictly above each earlier `v` and each earlier delete revision of that key (Section 4.2.2).
Only the version numbers change: a delete raises `max_deleted_v` by 2 instead of 1, so a recreate after a delete starts
one higher than before.

### 6.5 Why does the export need epochs when `MaterializationDO` already checks revisions?

Because the tombstone GC has no time horizon (Section 4.2.11). The base exporter can send X at revision 2 and the
delete at revision 3 in two calls. When revision 3 applies first, the targets read the tombstone, and the GC deletes
it. Then revision 2 arrives, finds no row, and creates X again. The epochs and the same-epoch rule (Section 4.2.9)
reject such a late call. A GSI entry stores no revision, so the GSI hop needs the epochs in all cases.

## 7. References

- `docs/agent-plans/2026-10-03-max-deleted-version.md`
- `docs/agent-plans/2026-09-13-items-link-item_id.md`
- `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`
- `docs/agent-plans/2026-09-26-fokos-sharding-client.md`
- `docs/agent-plans/2026-10-17-batch-get-and-write-items.md`
- `docs/ideas/2026-10-21-idempotent-sink-writes.md`
- `packages/fokosdb/src/shared/partition/partition-store.ts`
- `packages/fokosdb/src/shared/partition/fokos-migration-host.ts`
- `packages/fokosdb/src/shared/partition/ttl-expiry.ts`
- `packages/fokosdb/src/shared/partition-context.ts`
- `packages/fokosdb/src/shared/do-stubs.ts`
- `packages/fokosdb/src/shared/batch-api-types.ts`
- `packages/fokosdb/src/shared/expression/compiler.ts`
- `packages/fokosdb/src/shared/transaction-limits.ts`
- `packages/fokosdb/src/sharding/runtime.ts`
- `packages/fokosdb/src/sharding/repartition-flow.ts`
- `packages/fokosdb/src/server/do-partition.ts`
- `packages/fokosdb/formal/p/PSrc/Partition.p`
- `packages/fokosdb/formal/p/PSpec/Versions.p`
- [Using Global Secondary Indexes in DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html)
- [Cloudflare Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Cloudflare Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)
- [Cloudflare Queues limits](https://developers.cloudflare.com/queues/platform/limits/)
