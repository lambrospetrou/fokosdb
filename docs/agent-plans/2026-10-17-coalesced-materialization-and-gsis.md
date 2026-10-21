# RFC — Coalesced materialization for global secondary indexes and push targets

**State:** Draft
**Date:** 2026-10-17
**Author:** Lambros Petrou
**Status:** The delete revision `v + 1` (Section 4.2.2) is built. Nothing else is built. This document specifies
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
    - [4.2.3 Base mutation capture and outbox](#423-base-mutation-capture-and-outbox)
    - [4.2.4 `MaterializationDO` storage and deletion lifecycle](#424-materializationdo-storage-and-deletion-lifecycle)
    - [4.2.5 Target push state machine and background runner](#425-target-push-state-machine-and-background-runner)
    - [4.2.6 Permanent violations](#426-permanent-violations)
    - [4.2.7 Target definitions and table destroy](#427-target-definitions-and-table-destroy)
    - [4.2.8 GSI tables, composite sort key, and queries](#428-gsi-tables-composite-sort-key-and-queries)
    - [4.2.9 Backpressure, circuit breaking, and cost](#429-backpressure-circuit-breaking-and-cost)
  - [4.3 Open Questions](#43-open-questions)
    - [4.3.1 Circuit breaker scope and admission thresholds](#431-circuit-breaker-scope-and-admission-thresholds)
    - [4.3.2 Capture on a table that has data](#432-capture-on-a-table-that-has-data)
    - [4.3.3 `version` and `{ ref: "v" }` on GSI queries](#433-version-and--ref-v--on-gsi-queries)
  - [4.4 Future Follow-ups](#44-future-follow-ups)
    - [4.4.1 Online target backfill](#441-online-target-backfill)
    - [4.4.2 `FokosDBAutoAdminDO` catalog and activation status](#442-fokosdbautoadmindo-catalog-and-activation-status)
    - [4.4.3 Per-GSI topology and key-limit overrides](#443-per-gsi-topology-and-key-limit-overrides)
- [5. Alternative Options](#5-alternative-options)
  - [5.1 Base outbox, base shadow, and stateless Queue fan-out](#51-base-outbox-base-shadow-and-stateless-queue-fan-out)
  - [5.2 Separate `GsiMaintainerDO` worker tier](#52-separate-gsimaintainerdo-worker-tier)
  - [5.3 Pull cursors (`change_seq`) for push targets](#53-pull-cursors-change_seq-for-push-targets)
  - [5.4 One sidecar `MaterializationDO` for each base `PartitionDO`](#54-one-sidecar-materializationdo-for-each-base-partitiondo)
  - [5.5 A hash or a local integer ID in the GSI sort key](#55-a-hash-or-a-local-integer-id-in-the-gsi-sort-key)
  - [5.6 Per-target cursors over change positions in `MaterializationDO`](#56-per-target-cursors-over-change-positions-in-materializationdo)
  - [5.7 Compact `target_item_state` with key-change-only pre-RPC writes](#57-compact-target_item_state-with-key-change-only-pre-rpc-writes)
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

### 1.4 Glossary

| Term | Meaning |
| --- | --- |
| Base item | One item in the user table, identified by `(baseHashKey, baseSortKey)`. |
| Base revision | The strictly increasing integer revision of one base item across writes, deletes, and splits. |
| Delete revision | The base revision of a delete or a TTL expiry of a row at `v`: `v + 1`. |
| Export outbox | The coalesced `materialization_outbox` row in a base `PartitionDO` for one dirty base item. |
| Capture | The write of `materialization_outbox` rows and their export, enabled by `captureChanges`. |
| Materialization partition | One `MaterializationDO` in the shard group `fokos.m.<tableName>`. |
| Latest image | The newest live image or tombstone that `MaterializationDO` accepted for a base item. |
| Tombstone | An image with `deleted = 1` and no payload: an outbox row, an export entry, or a `latest_items` row. |
| Target | A registered downstream destination in `MaterializationDO`: a GSI or a push target. |
| Push target | A target of kind `push_sink`: a `WorkerEntrypoint` or a `DurableObject` that gets batches over Workers RPC. |
| Applied state | The revision (and for a GSI, the GSI key `(applied_index_hk, applied_index_sk)`) last completed. |
| In-flight transition | The revision (and for a GSI, the GSI key) that `MaterializationDO` froze in SQLite before a call. |
| Desired state | The newest revision that a target must converge to for one base item. |
| Violation | A base item revision whose target write fails permanently, for example a GSI key of a type that is not valid. |
| Unowned copy | A row that a split or a promotion moved to another partition. The source keeps it until `cleanupSourceStep`. |
| `gsiSK` | The encoded GSI sort key of an item, or `x''` when the GSI has no sort key. |
| `physicalSk` | The sort key that a GSI table stores: the escaped `gsiSK`, `baseHK`, and `baseSK`, and a trailer. |

## 2. Goals and Requirements

### 2.1 In scope

- Support up to 10 GSIs per table. Each GSI defines its own partition key, optional sort key, and attribute
  projection.
- Support push targets (a `WorkerEntrypoint` or a `DurableObject` called over Workers RPC) from the same
  `MaterializationDO` state machine.
- Define GSIs and push targets in a subclass of `MaterializationDO` that overrides `targets()`.
- When `captureChanges = true`, capture one coalesced outbox marker in the base `PartitionDO`, in the same SQLite
  transaction as each item mutation.
- Route the materialization state by `(baseHashKey, baseSortKey)` in an independent shard group
  `fokos.m.<tableName>`.
- Drain outbox exports in `PartitionDO` and target pushes in `MaterializationDO` with an in-memory background loop.
  `MaterializationDO` round-robins the targets with 2 parallel workers. The Durable Object alarm is only for recovery
  after a crash or an eviction.
- Keep a full mirror of live items in `MaterializationDO` (`latest_items.data`). The mirror supports target
  projections, retries, and future online target backfills (Section 4.4.1).
- Isolate failures between targets. One unavailable GSI or push target must not stop healthy targets or base writes.

### 2.2 Out of scope

- A full change data capture (CDC) history of each intermediate version. Intermediate writes before consumption
  coalesce into the latest image.
- Pull-based stream cursors. All targets use push delivery from `MaterializationDO`.
- Strongly consistent GSI reads, or atomic visibility of multiple items across a GSI.
- Prevention of temporary duplicate entries during a GSI key move across partitions. `WRITE_NEW` runs before
  `DELETE_OLD`, as in the eventually consistent GSIs of DynamoDB.
- Uniqueness constraints on a GSI key. A caller that needs a unique secondary key must use `transactWriteItems` on the
  base table.
- Online backfill of targets that are added after data exists, cluster-wide activation tracking through
  `FokosDBAutoAdminDO`, and per-GSI topology overrides. Section 4.4 tracks them.

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

## 3. Milestones

### Milestone 1: The TTL sweep deletes only owned rows
- Change `PartitionStore.deleteExpiredItems` to filter its expired, unlocked candidates with
  `this.fokos.ownerCheck()` and to delete only the owned rows. Unowned copies stay until `cleanupSourceStep` deletes
  them (Section 4.2.3, the TTL sweep item).
- Update the comment of `deleteExpiredItems`, which says that the sweep does not test the owner of each row. Update the
  unit tests and the promotion tests. This milestone ships on its own before any materialization code.

### Milestone 2: Base `materialization_outbox` and `MaterializationDO` mirror
- Add `captureChanges` and `nsMat` to `FokosTableIdentity`, `FokosTableOptions`, and `FokosDBPolicy`. Add
  `materialization_outbox` to `PartitionStore` (Sections 4.2.1 and 4.2.3).
- Add the outbox writes to `upsertItem`, `updateItemSingleShot`, `deleteItem`, `TransactionParticipant.commitLocal`,
  `TransactionParticipant.executeSingleShot`, and the owned rows of `deleteExpiredItems`.
- Migrate `materialization_outbox` rows in `FokosMigrationHost`. Delete unowned outbox copies in `cleanupSourceStep`.
- Create `MaterializationDO` in the shard group `fokos.m.<tableName>` with `latest_items`, `key_size_estimates`,
  `applyLatestImages` (`group` dispatch with `failurePolicy: "collect"`), and the in-memory base export loop with its
  alarm fallback (Sections 4.2.3 to 4.2.5).

### Milestone 3: Push targets (`PUSH_BATCH -> COMMIT_APPLIED`)
- Add `target_item_state` and the `MaterializationDO.targets()` method override (Sections 4.2.4 and 4.2.7).
- Build these parts (Sections 4.2.4 to 4.2.6):
  - the in-memory round-robin target runner with 2 parallel workers,
  - the write deadline fence,
  - the single-phase `PUSH_BATCH -> COMMIT_APPLIED` state machine for `push_sink` targets,
  - violation handling and tombstone GC,
  - the `MaterializationDO` split and promotion migration.

### Milestone 4: Static GSIs (`WRITE_NEW -> DELETE_OLD -> COMMIT_APPLIED`) and `db.gsi(indexName)`
- Add `applyBeforeMs` and `write_deadline_passed` to `batchWriteItems` from
  `docs/agent-plans/2026-10-17-batch-get-and-write-items.md` (Section 4.2.8).
- Build the `WRITE_NEW -> DELETE_OLD -> COMMIT_APPLIED` state machine for `gsi` targets with `FokosDB.batchWriteItems`
  (Section 4.2.5).
- Add the composite sort key codec (`<L><flag><D>`), the `{ ref: "sortKey" }` compiler rules, the derived GSI key
  limits, `db.gsi(indexName).queryItems`, and `FokosDB.destroy()` across all shard groups of the table (Sections 4.2.7
  and 4.2.8).

### Milestone 5: Circuit breaker tuning and admission thresholds
- Resolve Open Questions 4.3.1 and 4.3.2. Tune the circuit breaker scope and the storage admission thresholds.

## 4. Proposed Solution

### 4.1 High-level overview

A table with GSIs or push targets runs independent `FokosShardingRuntime` shard groups. `FokosShardingClient` connects
them:

```text
Base Shard Group: fokos.p.<tableName> (PartitionDO)
  - Stores committed items + coalesced materialization_outbox markers (no payload copy)
  - In-memory export loop sends dirty items by (baseHK, baseSK); alarm is fallback only
          |
          | applyLatestImages (batched group RPC, failurePolicy: "collect")
          v
Materialization Shard Group: fokos.m.<tableName> (MaterializationDO)
  - Routed by (baseHK, baseSK); subclass override targets() defines GSIs and push sinks
  - Full mirror of live items in latest_items (1 payload copy per live item)
  - 1 compact target_item_state row per (item_id, target_id)
  - Single in-memory loop round-robins targets (2 batches in parallel); alarm is fallback
          |
          +----------------------------------------+
          |                                        |
          v                                        v
GSI Shard Groups (up to 10):             Remote Push Targets:
  fokos.gsiv1.<n>.<tableName>.<indexName>  Workers RPC to a WorkerEntrypoint
  - Standard FokosDB table (PartitionDO)   or DurableObject
  - Written via FokosDB.batchWriteItems    - Batched push of coalesced latest items
```

1. **Base write (`PartitionDO`).** When `captureChanges = true`, each committed write, delete, or owned TTL expiry
   updates `items` and upserts a compact marker into `materialization_outbox` in one `transactionSync`. Then it wakes
   the in-memory export loop.
2. **Export (`PartitionDO -> MaterializationDO`).** The export loop claims the owned, due outbox rows and reads the
   live payloads from `items`. It sends batched `applyLatestImages` RPCs to `fokos.m.<tableName>`.
   `MaterializationDO` updates `latest_items`, raises `desired_revision` on each registered target, and wakes its
   in-memory target push loop. It acknowledges each item in the same call.
3. **Target push (`MaterializationDO -> GSI or push target`).** One in-memory background loop in `MaterializationDO`
   round-robins across the registered targets with 2 parallel workers. Each target has at most 1 batch in flight.
   - For a GSI, the loop freezes the transition in `target_item_state` and writes entries with
     `FokosDB.batchWriteItems`. When a key moves, it deletes the old GSI key. Then it records the applied GSI key.
   - For a push target, the loop pushes a batch of `PushTargetItem` records over Workers RPC and records the applied
     revision.

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
| `fokos.m.orders` | `MaterializationDO` | `policy.nsMat` | `(baseHK, baseSK)` | `latest_items` + `target_item_state` |
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
                                                       accepts rev=6, acks late rev=5 as stale
```

2. **`MaterializationDO` split or promotion (`fokos.m`).** Each base `PartitionDO` sends exports through a
   `FokosShardingClient` for `fokos.m.<tableName>`. When a `MaterializationDO` splits or promotes a hot base hash key,
   its migration host moves the `latest_items` row and all `target_item_state` rows of each item in the same slice.
   Each child continues to push the items of its slice. No external cursor handoff is necessary.
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

All existing transaction invariants stay true:

- `transactGetItems` still detects a create and a delete between phase 1 and phase 2. `max_deleted_v` increases by 2
  instead of 1.
- A new row still starts strictly above each earlier `v` and each earlier delete revision.

#### 4.2.3 Base mutation capture and outbox

`FokosDBPolicy` and `FokosTableOptions` add `captureChanges` (default `false`), which travels in `PartitionContext`.
When `captureChanges` is `false`, a partition writes no outbox row and runs no export loop. This includes each GSI
shard group.

`PartitionStore` adds the rowid table `materialization_outbox`:

```sql
CREATE TABLE IF NOT EXISTS materialization_outbox (
    outbox_id          INTEGER PRIMARY KEY,
    hk                 BLOB    NOT NULL,
    sk                 BLOB    NOT NULL DEFAULT x'',
    revision           INTEGER NOT NULL,
    deleted            INTEGER NOT NULL,
    unexported_create  INTEGER NOT NULL DEFAULT 0,
    attempted          INTEGER NOT NULL DEFAULT 0,
    next_attempt_at    INTEGER NOT NULL DEFAULT 0,
    apply_before_ms    INTEGER,
    UNIQUE (hk, sk)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_materialization_outbox_due
    ON materialization_outbox (next_attempt_at, outbox_id);
```

When `captureChanges = true`, each committed content mutation updates `materialization_outbox` in the same
`transactionSync` as `items`:

1. **Insert of a new item (no existing row in `items`):**
   - When no row exists in `materialization_outbox`, insert
     `(hk, sk, revision = newV, deleted = 0, unexported_create = 1, attempted = 0, next_attempt_at = 0)`.
   - When a row already exists in `materialization_outbox` (an unacknowledged delete), update it to
     `revision = newV, deleted = 0, unexported_create = 0, next_attempt_at = 0`.
2. **Update of an existing item:**
   - Upsert `(hk, sk, revision = newV, deleted = 0, next_attempt_at = 0)`. When the row exists, keep
     `unexported_create` and `attempted`. Otherwise, set both to `0`.
3. **Exporter claim:**
   - Before the exporter sends an `applyLatestImages` RPC for a batch of keys, it sets `attempted = 1`,
     `apply_before_ms = now + callDeadlineMs`, and `next_attempt_at = apply_before_ms + maxClockSkewMs` on those rows
     in SQLite.
   - When the circuit breaker to `MaterializationDO` is open, the exporter claims no rows, and `attempted` stays `0`.
4. **Delete of an existing item, or owned TTL expiry:**
   - When the `materialization_outbox` row has `unexported_create = 1 AND attempted = 0`, no byte of this item has left
     the base partition. `PartitionStore` must delete the `materialization_outbox` row in place and must send no RPC
     to `MaterializationDO`.
   - Otherwise, `PartitionStore` upserts
     `(hk, sk, revision = deleteVersion, deleted = 1, unexported_create = 0, next_attempt_at = 0)`.
   - The `attempted` guard prevents a lost delete when an earlier create RPC timed out after `MaterializationDO`
     committed it.

For each claimed row in `materialization_outbox`, the base exporter does these steps:

1. In the claim `transactionSync`, it selects up to `maxItemsPerStep` (100) rows with `next_attempt_at <= now` from
   `idx_materialization_outbox_due`, bounded by `maxBytesPerStep` (1 MiB).
   - For an unowned copy of a promoted key (`!this.fokos.owns(key)`), it sets
     `next_attempt_at = now + exportRetryMs` (default 30 s) and skips the row.
   - Thus unowned copies never block the owned keys behind them, and they do not spin the alarm.
2. For each owned row with `deleted = 0`, it reads the item from `items` in the same synchronous snapshot and checks
   `items.v == outbox.revision`. When the row is absent or `items.v != outbox.revision` (an invariant defect), it logs
   one error with the keys, deletes the outbox row, and continues.
3. For `deleted = 1`, it builds a tombstone entry with no item payload.
4. When the `applyLatestImages` response arrives, it runs this statement for each item that `MaterializationDO`
   acknowledged at `ackedRevision` and that this partition still owns (`this.fokos.owns(key)`):

```sql
DELETE FROM materialization_outbox
 WHERE hk = ?1 AND sk = ?2 AND revision = ?3;
```

The base partition must delete a `materialization_outbox` row on acknowledgement only when
`stored.revision == acked.revision` and the partition still owns the key.

For example, a newer write (`revision = 12`) arrives while `revision = 11` is in flight. The `DELETE` for `11` then
matches zero rows and leaves `12` in the outbox with `next_attempt_at = 0`. For an item that returned `unprocessed`,
the exporter sets `next_attempt_at` to a backoff timestamp.

Splits, promotions, and TTL sweeps handle outbox rows and ownership as follows:

1. **Migration (`FokosMigrationHost`).** The host adds a third migration stream, `materialization_outbox`, after
   `items` and `pending_tx`. It copies the outbox rows of the slice to the child with `attempted = 1` and
   `next_attempt_at = 0`, because an export call from the source can be in flight at the cutover.
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
   - When a chunk holds unowned copies, the sweep cycle stops early. The owned expired rows that sort after those
     copies wait until `cleanupSourceStep` finishes. TODO: measure this delay for a promoted key.

#### 4.2.4 `MaterializationDO` storage and deletion lifecycle

`MaterializationDO` keeps the latest payload of each live base item in `latest_items.data` (full-mirror mode):

- Both item tables are rowid tables.
- `target_item_state` links to `latest_items.item_id` (an 8-byte integer). Thus `(base_hk, base_sk)` is stored one
  time for each item, not one time for each target.
- `key_size_estimates` tracks the byte totals of each `base_hk` for `promotionCandidates` and `planRangeSplit`.

```sql
CREATE TABLE IF NOT EXISTS latest_items (
    item_id         INTEGER PRIMARY KEY,
    base_hk         BLOB    NOT NULL,
    base_sk         BLOB    NOT NULL DEFAULT x'',
    revision        INTEGER NOT NULL,
    deleted         INTEGER NOT NULL,
    deleted_at_ms   INTEGER,
    data_kind       INTEGER,
    ttl_at          INTEGER,
    est_row_bytes   INTEGER NOT NULL DEFAULT 0,
    data            ANY,
    UNIQUE (base_hk, base_sk)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_latest_items_deleted_gc
    ON latest_items (deleted_at_ms)
    WHERE deleted = 1 AND deleted_at_ms IS NOT NULL;

CREATE TABLE IF NOT EXISTS key_size_estimates (
    hk        BLOB    PRIMARY KEY,
    est_bytes INTEGER NOT NULL
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS target_item_state (
    state_id                 INTEGER PRIMARY KEY,
    item_id                  INTEGER NOT NULL,
    target_id                TEXT    NOT NULL,

    applied_revision         INTEGER,
    applied_index_hk         BLOB,    -- encoded GSI hash key; NULL when the GSI holds no entry
    applied_index_sk         BLOB,    -- encoded GSI sort key; x'' when the GSI has no sort key

    desired_revision         INTEGER NOT NULL,

    inflight_revision        INTEGER,
    inflight_index_hk        BLOB,
    inflight_index_sk        BLOB,
    inflight_phase           TEXT,
    inflight_attempt         INTEGER NOT NULL DEFAULT 0,
    inflight_apply_before_ms INTEGER,

    status                   TEXT    NOT NULL, -- 'IDLE' | 'DIRTY' | 'IN_FLIGHT' | 'VIOLATION'
    next_attempt_at          INTEGER NOT NULL DEFAULT 0,
    last_error_code          TEXT,
    violation_revision       INTEGER,

    UNIQUE (item_id, target_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_target_item_state_due
    ON target_item_state (target_id, next_attempt_at, item_id)
    WHERE status IN ('DIRTY', 'IN_FLIGHT');
```

`applyLatestImages` is a `group` operation on `MaterializationDO` with `whileMigrating: "throw"`,
`failurePolicy: "collect"`, and `admissionTag: "batch_write"`:

- `MaterializationDO` must accept an export only when `incoming.revision > stored.revision`.
- It must idempotently acknowledge an export when `incoming.revision == stored.revision`.
- When `MaterializationDO` is above `1.1x maxSizeMb`, its local handler must reject only live upserts
  (`deleted = 0`), as `unprocessed` with `partition_over_size`, and signal `evaluateSplit`. It must still ingest
  deletions (`deleted = 1`), so that deletes can reclaim space.

Row lifecycle in `MaterializationDO`:

1. **Live item (`deleted = 0` at `revision = R`).** `latest_items` upserts the row (`deleted = 0`,
   `deleted_at_ms = NULL`, `data`, `est_row_bytes`) and updates `key_size_estimates`. For each target in
   `this.targets()`, `target_item_state` upserts `(item_id, target_id)` with `desired_revision = R`:
   - When the row does not exist or its `status` is not `'IN_FLIGHT'`, it sets `status = 'DIRTY'` and
     `next_attempt_at = 0`.
   - When its `status` is `'IN_FLIGHT'`, it keeps `status` and `inflight_*` unchanged.
2. **Deleted item (`deleted = 1` at `revision = R`):**
   - At ingestion, `MaterializationDO` sets `latest_items.data = NULL`, `deleted = 1`, and `deleted_at_ms = NULL`. It
     updates `est_row_bytes` to the key-only tombstone size and updates `key_size_estimates`. Thus the item payload is
     free at once.
   - For each target row, it sets `desired_revision = R`. It moves each row whose status is not `'IN_FLIGHT'` to
     `'DIRTY'` with `next_attempt_at = 0`.
   - When no `target_item_state` row exists for `item_id` (no target is defined), the same transaction sets
     `latest_items.deleted_at_ms = now`.
   - When a target finishes the deletion `R` (or marks a deletion violation), `MaterializationDO` deletes that
     `target_item_state` row. When it deletes the last `target_item_state` row of `item_id` while
     `latest_items.deleted = 1`, it sets `latest_items.deleted_at_ms = now`.
   - The `latest_items` tombstone row (`data = NULL`) stays until `now - deleted_at_ms >= deletedRevisionHorizonMs`
     (default 24 hours). Thus `MaterializationDO` rejects each late export with `revision < R`. Then a background
     sweep on `idx_latest_items_deleted_gc` deletes the `latest_items` row and subtracts its key bytes from
     `key_size_estimates`.
3. **Link-ID delete rule.** Each path that deletes a `latest_items` row (the tombstone GC sweep, and
   `cleanupSourceStep` on a promotion source) must first delete all `target_item_state` rows with that `item_id`, in
   the same `transactionSync`. Thus a rowid that SQLite gives again never attaches orphaned `target_item_state` rows
   to a new base item.

The row lifecycle must keep these invariants:

1. The ingest of a new latest image must update `latest_items` and raise `desired_revision`. It must not change
   `inflight_*` or the `status` of an `'IN_FLIGHT'` row. Only a row that is not `'IN_FLIGHT'` moves to `'DIRTY'` with
   `next_attempt_at = 0`.
2. A `'DIRTY'` row must have `inflight_revision IS NULL`.
3. A `latest_items` tombstone (`deleted = 1`) must set `deleted_at_ms` as soon as no `target_item_state` row remains for
   its `item_id`, also when no target is registered.
4. A `MaterializationDO` split or promotion must move the `latest_items` row and all `target_item_state` rows of a
   base item in the same slice. Each deletion of a `latest_items` row follows the link-ID delete rule (rule 3 above).

`applied_index_sk` and `inflight_index_sk` store the encoded `gsiSK`, not `physicalSk` (Section 4.2.8). With 1.5 KB
base keys and 10 GSIs, this saves about 15 KB per item at rest and 30 KB while `'IN_FLIGHT'`. Before
`MaterializationDO` sends a GSI call, it reads `base_hk` and `base_sk` from `latest_items` by `item_id`. Both columns
come before the trailing `data` column, so SQLite reads no overflow page. Then it builds
`physicalSk = encodeGsiSortKey(index_sk, base_hk, base_sk)`.

#### 4.2.5 Target push state machine and background runner

`MaterializationDO` drives two kinds of targets:

- **`gsi`.** It writes to `fokos.gsiv1.<n>.<tableName>.<indexName>` with `FokosDB.batchWriteItems` in two phases
  (`WRITE_NEW -> DELETE_OLD -> COMMIT_APPLIED`).
- **`push_sink`.** It calls a `WorkerEntrypoint` or a `DurableObject` over Workers RPC in one phase
  (`PUSH_BATCH -> COMMIT_APPLIED`). A push target keys records by `(baseHashKey, baseSortKey)`, so it has no old
  secondary key to delete.

Before `MaterializationDO` sends an outbound RPC to a target, it must write these columns to SQLite:

- `inflight_revision`, `inflight_phase`, `inflight_attempt`, `inflight_apply_before_ms`, and `next_attempt_at`,
- for a GSI, also `inflight_index_hk` and `inflight_index_sk`.

##### GSI transition phases (`WRITE_NEW -> DELETE_OLD -> COMMIT_APPLIED`)

For a GSI target, `MaterializationDO` must not freeze a new transition over a row whose `inflight_revision` is not
`NULL`. It must first finish or clean up the in-flight transition.

For each due row (`next_attempt_at <= now`) on a `gsi` target:

1. **Resume an `'IN_FLIGHT'` row (`status == 'IN_FLIGHT'`):**
   - When `inflight_phase == 'DELETE_OLD'`, freeze and send `delete` again for `(applied_index_hk, applied_index_sk)`.
     On `applied`, go to step 6 (`COMMIT_APPLIED`).
   - When `inflight_phase == 'WRITE_NEW'` and `desired_revision == inflight_revision`, project `latest_items.data`
     again, freeze, and send `put` again for `(inflight_index_hk, inflight_index_sk)` with
     `ttlAt: latest_items.ttl_at`. On `applied`, go to step 5 (`DELETE_OLD`).
   - When `inflight_phase == 'WRITE_NEW'` and `desired_revision > inflight_revision`, a newer base write replaced
     `inflight_revision` after the earlier `WRITE_NEW` call failed:
     - When `(inflight_index_hk, inflight_index_sk)` equals `(applied_index_hk, applied_index_sk)`, clear `inflight_*`
       and go directly to step 2 (`status = 'DIRTY'`) with zero RPCs. Thus the live `applied_index_*` entry stays.
     - Otherwise, freeze and send `delete` for `(inflight_index_hk, inflight_index_sk)`. On `applied`, clear
       `inflight_*`, keep `applied_*` unchanged, and set `status = 'DIRTY'` and `next_attempt_at = 0`.
2. **Evaluate the desired state of a `'DIRTY'` row (`status == 'DIRTY'`):**
   - When `inflight_revision IS NOT NULL` (an invariant defect), log one error and run step 1 as for an
     `'IN_FLIGHT'` row.
   - The desired state is `absent` (sparse index) in these cases:
     - `latest_items.deleted == 1`,
     - the GSI partition key attribute is missing or `null`,
     - the GSI defines a `sortKey` whose attribute is missing or `null`.
   - Otherwise, evaluate the GSI key and projection expressions against `latest_items` to get `target_hk` and
     `target_sk`. The evaluation aliases `base_hk AS hk, base_sk AS sk, revision AS v, ttl_at AS
     ttl_epoch_utc_seconds`, so that the compiled expression SQL reads the column names it expects.
   - When the evaluation fails with a permanent violation (Section 4.2.6), run the violation cleanup instead of
     `WRITE_NEW`.
3. **Short-circuit when the applied state and the desired state are both absent:**
   - When `applied_index_hk IS NULL`, `inflight_index_hk IS NULL`, and the desired state is `absent`, send zero RPCs:
     - When `latest_items.deleted == 1`, delete the `target_item_state` row. When no target row remains, set
       `latest_items.deleted_at_ms = now`.
     - Otherwise, set `applied_revision = desired_revision` and `status = 'IDLE'`.
4. **Phase `WRITE_NEW` (when the desired state is present):**
   - Freeze `inflight_revision = desired_revision`, `inflight_index_hk = target_hk`, `inflight_index_sk = target_sk`,
     `inflight_phase = 'WRITE_NEW'`, and `status = 'IN_FLIGHT'` in SQLite.
   - Then send the `put` entry (with `ttlAt: latest_items.ttl_at`) in `gsiDb.batchWriteItems(...)`. On `applied`, go
     to step 5.
5. **Phase `DELETE_OLD` (when the desired state is absent, or when `WRITE_NEW` completed for a different GSI key):**
   - From step 2, with `desired = absent` and `applied_index_hk IS NOT NULL`: freeze
     `inflight_revision = desired_revision`, `inflight_index_hk = NULL`, `inflight_index_sk = NULL`,
     `inflight_phase = 'DELETE_OLD'`, and `status = 'IN_FLIGHT'` in SQLite. Then send a `delete` entry for
     `(applied_index_hk, applied_index_sk)` in `gsiDb.batchWriteItems(...)`.
   - After `WRITE_NEW` (`inflight_index_hk IS NOT NULL`): when `applied_index_hk IS NOT NULL` and
     `(applied_index_hk, applied_index_sk) != (inflight_index_hk, inflight_index_sk)`, set
     `inflight_phase = 'DELETE_OLD'` and `next_attempt_at = 0`. The next batch then sends `delete` for
     `(applied_index_hk, applied_index_sk)`. Otherwise, go to step 6 at once, in the same `transactionSync`.
6. **Phase `COMMIT_APPLIED`:**
   - When `latest_items.deleted == 1` and `inflight_revision == latest_items.revision`, delete the
     `target_item_state` row. When no `target_item_state` row remains for `item_id`, set
     `latest_items.deleted_at_ms = now`.
   - Otherwise, set `applied_revision = inflight_revision`, `applied_index_hk = inflight_index_hk`, and
     `applied_index_sk = inflight_index_sk`, and clear `inflight_*`. When `desired_revision > applied_revision`, set
     `status = 'DIRTY'` and `next_attempt_at = 0`. Otherwise, set `status = 'IDLE'`.

##### Push target phase (`PUSH_BATCH -> COMMIT_APPLIED`)

For each due batch on a `push_sink` target:

1. Before the call, freeze `inflight_revision = desired_revision`, `inflight_phase = 'PUSH_BATCH'`, and
   `status = 'IN_FLIGHT'` on each row in SQLite. A resumed `'IN_FLIGHT'` row freezes again with the current
   `desired_revision` and the current image (or tombstone), because the push target keys items by
   `(baseHashKey, baseSortKey)`.
2. Build the batch of `PushTargetItem` records. Apply the optional target projection to live items:

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

/** One result for each item of the batch, in batch order. */
export type PushTargetResult =
	| { outcome: "applied" }
	| { outcome: "retry"; code?: string }
	| { outcome: "rejected"; code: string };
```

3. Call `method(batch)` on the configured binding (Section 4.2.7) and apply each returned `PushTargetResult`. When the
   call throws, fails at the transport, or returns an array of the wrong length, each item of the batch is `unknown`.

The push target contract:

- Delivery is at-least-once and coalesced. Intermediate revisions can be skipped.
- `revision` strictly increases for each `(baseHashKey, baseSortKey)` across creates, updates, and deletes.
- Two pushes for one key can arrive out of order when an earlier call stays in flight after a lost response. Thus the
  push target must keep the highest applied `revision` of each key and ignore each push with
  `revision <= appliedRevision`. It must keep the revision of a deleted key for at least `deletedRevisionHorizonMs`.

##### In-memory background runner and alarm fallback

A Durable Object alarm for each batch is too slow, and it writes alarm storage on each step. Thus `PartitionDO` (for
`materialization_outbox`) and `MaterializationDO` (for `target_item_state`) each drive their work with one in-memory
background promise, as `TtlExpiry.runCycle` in `packages/fokosdb/src/shared/partition/ttl-expiry.ts` does. A
`FokosScheduler` job is only the durable fallback when the DO restarts or is evicted.

An export or target push job must act only on rows that its partition owns. It must advance `next_attempt_at` on
skipped unowned copies, so that they never block owned keys or spin the alarm. It must write an outcome only for a row
that its partition still owns and whose `inflight_attempt` matches the call.

1. **One background loop in `MaterializationDO` (`#pushPromise`):**
   - `applyLatestImages` wakes the loop as soon as a batch commits.
   - The loop starts `targetConcurrency` (default 2) parallel worker coroutines. They round-robin across the targets
     that `this.targets()` returns.
   - Each `target_id` has at most 1 batch call in flight, so two workers never claim the same target at the same
     time. One slow target holds at most 1 of the 2 workers. The other worker drains all the other healthy targets.
   - To claim a batch for `target_id`, the worker runs one synchronous `transactionSync`:
     - It selects up to `maxItemsPerStep` (100) due rows (`next_attempt_at <= now`) from `idx_target_item_state_due`,
       joined with `latest_items` (`USING (item_id)`), bounded by `maxBytesPerStep` (1 MiB).
     - For an unowned copy of a promoted key (`!this.fokos.owns(key)`), it sets
       `next_attempt_at = now + targetRetryMs` (default 30 s) and skips the row.
     - For each owned row, it freezes the transition. It writes `inflight_attempt = inflight_attempt + 1`,
       `inflight_apply_before_ms = now + callDeadlineMs`, and
       `next_attempt_at = inflight_apply_before_ms + maxClockSkewMs`.
   - The worker `await`s the outbound `batchWriteItems` or `push_sink` RPC with a timeout of `callDeadlineMs`. Then it
     runs one `transactionSync` that applies the outcome of each row that this partition still owns and whose
     `inflight_attempt` still matches the call.
   - The workers repeat in memory until no target has due rows or `canSweepLocally` becomes `false`. They yield with
     `scheduler.wait(0)` between batches.
2. **One background loop in `PartitionDO` (`#exportPromise`):**
   - Each committed base write wakes `#exportPromise`.
   - In one `transactionSync`, the loop claims up to `maxExportCalls` (default 2) batches of owned, due rows from
     `idx_materialization_outbox_due`. It `await`s the `applyLatestImages` RPCs in parallel. Then it deletes the
     acknowledged rows in one `transactionSync`.
   - It repeats until no due outbox rows remain or `canSweepLocally` becomes `false`.
3. **Lifecycle guard and alarm fallback:**
   - Neither loop runs when `canSweepLocally` is `false`: the partition is not initialized, is a router, is
     `awaiting_data` or `importing`, or is fenced for destroy.
   - The `FokosScheduler` job (`materialization_export` in `PartitionDO`, `target_push` in `MaterializationDO`) gives
     the recovery after a crash or an eviction.
   - While the in-memory loop runs, `deadline()` returns `now + fallbackAlarmMs`. Thus `FokosScheduler` does not
     start a new pass every `fastPathDelayMs` (default 50 ms).
   - While the in-memory loop is idle, `deadline()` returns the earliest `next_attempt_at` in SQLite, and `runStep()`
     starts the in-memory loop.

##### The write deadline fence

A GSI table stores no revision or tombstone for each entry. Thus a late, forwarded `put(B)` from a timed-out call can
arrive after the `delete(B)` of the resume, and leave an orphan entry. These rules prevent it:

- `MaterializationDO` must send a call for a row only when no earlier call for that row can still apply.
- Each export call and each GSI `batchWriteItems` call must carry `applyBeforeMs`.
- The receiver must reject an item with `write_deadline_passed` when its clock is above `applyBeforeMs`.

The write deadline fence prevents an overlap between two consecutive calls for the same row:

1. **Freeze.** In the `transactionSync` before each call, the step increments `inflight_attempt` and writes
   `inflight_apply_before_ms = now + callDeadlineMs` (default 30 s) and
   `next_attempt_at = inflight_apply_before_ms + maxClockSkewMs` (default 2,000 ms).
2. **Receiver deadline check.** The call carries `applyBeforeMs = inflight_apply_before_ms`. `PartitionDO` (in the
   `transactionSync` block of each item of `apiBatchWriteItems`) and `MaterializationDO` (in `applyLatestImages`)
   compare their clock with `applyBeforeMs`. When `clock > applyBeforeMs`, the handler writes nothing and returns
   `unprocessed` with `write_deadline_passed`.
3. **Next call scheduling.**
   - When a call returns a definite outcome (`applied`, `unprocessed`, or `rejected`), no copy of that call can still
     run. Thus the completion handler updates `next_attempt_at` at once.
   - When the outcome is `unknown` or the call times out, `next_attempt_at` stays at
     `inflight_apply_before_ms + maxClockSkewMs`.
   - `maxClockSkewMs` bounds the clock offset between any two Durable Objects. Thus the receiver clock is past
     `applyBeforeMs` before the sender clock reaches `applyBeforeMs + maxClockSkewMs`. Each copy of the earlier call
     has then applied or expired before the next call starts.
   - `callDeadlineMs` must be longer than the longest normal call, forwards included. TODO: measure that duration.

##### Call outcomes

| Outcome | Meaning | Action in the completion handler |
| --- | --- | --- |
| `applied` | The write committed. | Go to the next phase. Set `next_attempt_at = 0`. |
| `unprocessed` (or push `retry`) | Nothing applied. | Keep the frozen state. Back off. Count a breaker failure. |
| `rejected` (`batch_request_too_large`) | The RPC is above 32 MiB. Nothing applied. | Split the batch and retry. No violation. |
| `rejected` (any other code) | A permanent item failure. | Move the item to `'VIOLATION'` (Section 4.2.6). |
| `unknown` | The write can have applied. | Keep the frozen state, and keep `next_attempt_at` at the deadline fence. |

`MaterializationDO` validates each item before it calls `batchWriteItems`. When `batchWriteItems` still throws a
validation error for the whole call before I/O, `MaterializationDO` retries each item of that batch alone. Thus only
the item that is not valid becomes `'VIOLATION'`.

#### 4.2.6 Permanent violations

When `MaterializationDO` evaluates a GSI key and projection against `latest_items`, or pushes an item, three cases are
possible:

1. **Missing or `null` GSI key attribute (a sparse omission, not a violation).** The item is absent from that GSI
   (`desired = absent`).
2. **GSI key type or size that is not valid (`VIOLATION`):**
   - **Type violation.** The extracted GSI key is not a valid non-empty string (`text` with no NUL `\0` and no lone
     UTF-16 surrogate) and not a non-empty `Uint8Array` (`bytes`). Examples: a number, a boolean, an array, an object,
     an empty string, a string that contains `\0` (`key_contains_nul`), an empty byte array, a lone UTF-16 surrogate,
     or a JSON path evaluated against a `bytes` or `text` base item.
   - **Size violation.** The encoded GSI hash key is larger than the `maxHashKeyBytes` of the base table, or the
     encoded GSI sort key is larger than its `maxSortKeyBytes` (Section 4.2.8).
3. **Entry that cannot be written (`VIOLATION`):**
   - **Projected `'bytes'` attribute in a GSI `projection`.** A GSI `projection` builds a JSONB document
     (`kind = "json"`), which cannot store raw binary values (`Uint8Array`). Each projected cell that evaluates to
     `'bytes'` at run time is a violation, as `update_value_is_bytes` is in `UpdateExpression`. The validation of
     `GsiTargetDefinition` rejects a compile-time constant `'bytes'` expression in `projection`.
   - **Row size.** The GSI entry row (projected data + GSI hash key + `physicalSk` + fixed overhead) is larger than
     `MAX_ITEM_BYTES` (400 KB). `MaterializationDO` measures it with `estRowBytesExpr`, `itemDataExpr`, and
     `validateItemDataSize`.
   - **Target rejection.** The GSI returns `rejected` with a code other than `batch_request_too_large`, or a push
     target returns `{ outcome: "rejected", code }`.

A permanent failure of one item on a target must mark that `(item_id, target_id)` as `'VIOLATION'` after the cleanup
of each earlier GSI entry. It must not fail the other items of the batch, block base outbox acknowledgements, or trip
the target circuit breaker. Thus a poison item does not retry forever. `MaterializationDO` handles a violation in
these steps:

1. **Clean up each earlier GSI entry.**
   - When the row has an in-flight GSI key (`inflight_index_hk IS NOT NULL`) that is different from
     `applied_index_*`, first delete `(inflight_index_hk, inflight_index_sk)`.
   - When `applied_index_hk IS NOT NULL`, run `DELETE_OLD(applied_index_hk, applied_index_sk)`. Thus the GSI keeps no
     stale entry from an older valid revision.
2. **Park in `'VIOLATION'`.**
   - When the violating revision is a deletion (`latest_items.deleted == 1`), log the violation and delete the
     `target_item_state` row, so that the tombstone can expire.
   - Otherwise, when `applied_index_hk` and `inflight_index_hk` are `NULL`, set `status = 'VIOLATION'`,
     `violation_revision = desired_revision`, `last_error_code = code`, and `next_attempt_at = 0`. Write one
     structured error log.
   - `idx_target_item_state_due` indexes only `status IN ('DIRTY', 'IN_FLIGHT')`, so a `'VIOLATION'` row costs the
     runner no work.
3. **Recover on a newer write.** When a base write or delete arrives with `revision > violation_revision`,
   `applyLatestImages` sets `desired_revision = revision` and `next_attempt_at = 0`, and moves `status` from
   `'VIOLATION'` to `'DIRTY'`.

#### 4.2.7 Target definitions and table destroy

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
	/** RPC method name that receives `PushTargetItem[]`. */
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
- The leaf stores the known `targetId` and `kind` values in a local SQLite table `materialization_targets`. Thus
  `destroy()` knows each GSI that was ever written.
- A helper in `packages/fokosdb/src/shared/do-stubs.ts` resolves `binding` (and `objectName` when present) from the
  Worker `env`.

##### Table destroy

`FokosDB.destroy()` destroys the shard groups of the table in this order: each writer before its receiver. Thus an
in-flight RPC never creates a destroyed Durable Object again.

1. `fokos.tc.<tableName>`.
2. `fokos.p.<tableName>`. `fokosPrepareDestroy` fences each base partition, stops its in-memory `#exportPromise`, and
   waits for it. It returns the maximum `apply_before_ms` of each `attempted = 1` row that had no in-memory promise
   (from an earlier restart). When that timestamp exists, `destroy()` waits until `maxApplyBeforeMs + maxClockSkewMs`
   before it deletes storage. The wait is 0 ms when all calls settled in memory or when `captureChanges = false`.
3. `fokos.m.<tableName>`, when `nsMat` is configured. `fokosPrepareDestroy` fences each `MaterializationDO`, stops its
   in-memory `#pushPromise`, and waits for it. It collects the union of the GSI `targetId` values from
   `materialization_targets`. It returns the maximum `inflight_apply_before_ms` of each unresolved `'IN_FLIGHT'` row
   that survived a restart. When that timestamp exists, `destroy()` waits until
   `maxInflightApplyBeforeMs + maxClockSkewMs` before it deletes storage.
4. `fokos.gsiv1.<n>.<tableName>.<indexName>` for each collected GSI `targetId`.

Each step is idempotent. A caller must not use `<tableName>` again until `destroy()` returns `{ ok: true }`.

#### 4.2.8 GSI tables, composite sort key, and queries

Each GSI is a standard `FokosDB` table in the shard group `fokos.gsiv1.<n>.<tableName>.<indexName>`:

- `<indexName>` is `targetId` (non-empty, no `~`).
- `<n>` is `tableName.length` in UTF-16 code units. The length prefix makes each `(tableName, indexName)` pair
  unambiguous, also when both names contain `.`.
- `gsiv1` names the version of this encoding. A later naming scheme can use a new version (for example `gsiv2`) with
  no collision with the names of `gsiv1`.
- The shard group name is the address of the GSI data. Thus the `gsiv1` encoding must never change.

`MaterializationDO` and `db.gsi(indexName)` construct the internal GSI `FokosDB` client with `captureChanges = false`
and no transaction coordinator group.

##### `batchWriteItems` deadline extension

`apiBatchWriteItems` (`docs/agent-plans/2026-10-17-batch-get-and-write-items.md`) adds the optional field
`applyBeforeMs?: number` to `BatchWriteItemsOptions` and `BatchWriteItemsRpcRequest`. Section 4.2.5 ("The write
deadline fence", step 2) gives the check that each item's `transactionSync` block runs.

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

#### 4.2.9 Backpressure, circuit breaking, and cost

##### Circuit breaking and four-stage backpressure

When calls to a target fail, `MaterializationDO` opens the circuit breaker of that target. It stops freezing new
transitions for the target, and sends one probe batch every `probeIntervalMs` (default 5 s) until a probe succeeds.
Pressure moves upstream in four stages:

1. **Stage 1 (target down).** The target circuit breaker opens in `MaterializationDO`. Incoming base exports still
   update `latest_items` and coalesce `desired_revision` in place. Base writes and healthy targets get no delay.
2. **Stage 2 (`MaterializationDO` reaches `1.0x maxSizeMb`).** `MaterializationDO` splits into `hashSplitN` children
   (or promotes a hot key). It does not wait for the down target to recover. Each split multiplies the storage
   capacity.
3. **Stage 3 (`MaterializationDO` migrating, or at the split floor and at `1.1x maxSizeMb`).** `MaterializationDO`
   rejects live upserts (`deleted = 0`) in `applyLatestImages` with `partition_migrating` or `partition_over_size`. It
   still accepts deletions (`deleted = 1`). Base `PartitionDO` instances keep and coalesce compact
   `materialization_outbox` rows, and base writes continue to succeed.
4. **Stage 4 (base `PartitionDO` reaches `1.1x maxSizeMb`).** The base `PartitionDO` splits. At the split floor, it
   rejects new writes with `partition_over_size`.

Example of the capacity during a 2-hour outage of 1 of 3 GSIs at 100 base writes/s. The outage has 720,000 mutations
in total: 72,000 new items and 648,000 mutations across 50,000 existing items.

- The base `materialization_outbox` stays near 0 rows, with 0 ms of extra write latency.
- `MaterializationDO` coalesces the 648,000 updates into 50,000 rows, which is 83% less GSI work.
- Each item that is created and deleted during the outage (`applied_revision IS NULL` and `deleted = 1`) converges
  with 0 RPCs to the down GSI.
- The 122,000 dirty `target_item_state` rows use about 15 MB across the shard group.
- When the GSI recovers, the in-memory runner drains the 122,000 to 172,000 `batchWriteItems` operations in 1.5 to 2.5
  minutes, at 1,000 to 2,000 items/s across 4 `MaterializationDO` partitions. The healthy targets continue to share
  the round-robin workers.

##### Cost of one base write

SQLite table rows written for each base write, with no coalescing, for a table with `T` GSIs (the worst case: 1 export
for each write):

| Base write | Today | Base partition | `MaterializationDO` | GSI partitions | Total | `T = 10` |
| --- | --- | --- | --- | --- | --- | --- |
| Put, every GSI key stays | 2 | 2 + 3 | 2 + 3T | 2T | 7 + 5T | 57 |
| Put, every GSI key moves | 2 | 2 + 3 | 2 + 4T | 5T | 7 + 9T | 97 |
| Delete | 3 | 3 + 3 | 4 + 3T | 3T | 10 + 6T | 70 |

- **Base partition.** A put writes `items` and `key_size_estimates` (2). A delete also writes `deletion_metadata` (3).
  Capture adds 3 outbox writes: the upsert, the `attempted = 1` claim, and the delete on the acknowledgement.
- **`MaterializationDO`.** The ingest writes `latest_items` and `key_size_estimates` (2) and `T` target rows. Each GSI
  writes its target row at each freeze and at each commit: 2 when the key stays, 3 when the key moves. For each target,
  a delete writes the `DELETE_OLD` freeze and deletes the target row. It also writes `deleted_at_ms` and, later, the
  tombstone GC delete on `latest_items` and `key_size_estimates`.
- **GSI partitions.** A put writes `items` and `key_size_estimates` (2). A delete writes 3 rows.
- **Push target or sparse GSI.** A push target costs 3 rows in `MaterializationDO` and 0 rows in FokosDB. A sparse GSI
  with an absent key costs 1 to 2 rows in `MaterializationDO` and 0 rows in the GSI.

When `N` base writes of one item coalesce before the export, the cost is `3N` base rows and one downstream export and
push cycle. Secondary index updates can add billed row writes. TODO: measure the billed `rowsWritten` in the Workers
runtime.

One live item with full-item projections across `T` GSIs stores `2 + T` payload copies: base `items`, `latest_items`,
and `T` GSI rows. Each `target_item_state` row adds about `|gsiHK| + |gsiSK| + 64` bytes.

### 4.3 Open Questions

#### 4.3.1 Circuit breaker scope and admission thresholds

Decide whether a circuit breaker covers one `target_id` or one destination GSI partition. Define the exact size
thresholds at which `MaterializationDO` stops export acknowledgements and `PartitionDO` rejects writes.

#### 4.3.2 Capture on a table that has data

Define how a table that already contains items enables `captureChanges`. Two cases are open:

- An item that gets no write after the policy change has no outbox row, and it never reaches `latest_items`.
- A partition that has not yet received the new policy writes no outbox row.

#### 4.3.3 `version` and `{ ref: "v" }` on GSI queries

Each GSI is a standard `PartitionDO` table. Thus `items.v` in a GSI partition counts the writes to that
`(gsiHK, physicalSk)` entry, not the `version` (`revision`) of the base item. Decide between two options:

- `db.gsi(indexName).queryItems` omits `version` on items with no projection and rejects `{ ref: "v" }` in a GSI
  `filter` or `projection`. A caller who needs the base version then projects `{ expr: { ref: "v" }, as: "version" }`
  in `GsiTargetDefinition.projection`.
- `db.gsi(indexName).queryItems` exposes the local `items.v` of the GSI entry.

### 4.4 Future Follow-ups

These features are not part of Milestones 1 to 4. Each one can ship later on its own.

#### 4.4.1 Online target backfill

A user can deploy a new GSI or push target in `MaterializationDO.targets()` on a table that already holds data in
`latest_items`. Each `MaterializationDO` leaf then waits until its `canSweepLocally` guard is true and each parent
import is complete. Then it:

1. records `backfill_cursor_item_id = 0` in `materialization_targets`,
2. runs a bounded background scan over `latest_items` (`WHERE item_id > backfill_cursor_item_id AND deleted = 0`),
3. inserts `'DIRTY'` rows into `target_item_state` with `ON CONFLICT (item_id, target_id) DO NOTHING`.

`MaterializationDO` stores a full mirror in `latest_items.data`. Thus the backfill of a new target reads nothing from
the base `PartitionDO` partitions.

#### 4.4.2 `FokosDBAutoAdminDO` catalog and activation status

Some applications need a cluster-wide `BACKFILLING | ACTIVE` status check when they add a GSI online. For them, a
singleton `FokosDBAutoAdminDO` (`fokos.admin.<tableName>`) can walk the `fokos.m.<tableName>` owners. It reports the
target as `ACTIVE` when each initialized leaf has:

- finished its import (`importState === null || importState === "imported" || importState === "active"`),
- completed its `latest_items` backfill scan,
- drained its initial `target_item_state` backlog for the new target.

#### 4.4.3 Per-GSI topology and key-limit overrides

A workload can need a GSI with a higher `rootTreesN`, or a different `maxHashKeyBytes` or `maxSortKeyBytes`, than the
base table. `FokosTableOptions` (or `db.gsi(indexName, opts)`) can then carry these overrides for each GSI. Thus
`MaterializationDO` and the caller `FokosDB` client construct identical `FokosRouteContext` topologies for that GSI
shard group.

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
`(baseHK, baseSK)` and holds `latest_items` and `target_item_state` in local SQLite. A push directly from
`MaterializationDO` has no extra DO hop and no distributed work leases.

### 5.3 Pull cursors (`change_seq`) for push targets

A partition-local `change_seq` for external pull consumers is rejected. With pull cursors, each consumer must track
partition splits and join the cursor epochs of a parent and its children. Push targets use `target_item_state`, which
migrates with each `MaterializationDO` split.

### 5.4 One sidecar `MaterializationDO` for each base `PartitionDO`

A sidecar DO `<part>.mat` for each base partition `<part>` is rejected for two reasons:

- A base split or promotion forces the sidecar to split, also when the materialization state is small.
- Out-of-order exports from an old parent and a new child no longer arrive at the same `MaterializationDO`.

### 5.5 A hash or a local integer ID in the GSI sort key

1. **Fixed-size hash suffix `H128(baseHK, baseSK)`.** Rejected. Two base items can share `(gsiHK, gsiSK)` and have the
   same hash. Then `WRITE_NEW` overwrites one item, and `DELETE_OLD` deletes the other.
2. **Partition-local `(baseHK, baseSK) -> id` lookup tables, or reference-counted `KeyBytes -> id` lookup tables.**
   Rejected for three reasons:
   - They double or triple `rowsRead` and `rowsWritten` on puts, deletes, and queries.
   - GSI tables become different from standard `PartitionDO` tables.
   - A split migration must compute the reference counts again across the child slices.

### 5.6 Per-target cursors over change positions in `MaterializationDO`

**Status:** Not decided. This option replaces `target_item_state`, the target state machine, and the write deadline
fence of Sections 4.2.4 and 4.2.5. It also replaces `deletedRevisionHorizonMs`. Sections 4.2.1 to 4.2.3 and 4.2.6 to
4.2.8 stay, with the changes that this section gives.

#### 5.6.1 Why

The design of Section 4.2 has two costs:

1. **Rows written for each item and each target.** Each target writes its `target_item_state` row at the ingest,
   before each outbound call, and at each commit. Section 4.2.9 gives `2 + 3T` rows in `MaterializationDO` for each
   base write.
2. **Correctness that depends on clocks.**
   - The write deadline fence (Section 4.2.5) prevents an orphan GSI entry only while the clock offset between two
     Durable Objects stays below `maxClockSkewMs`.
   - The `latest_items` tombstone rejects a late export only during `deletedRevisionHorizonMs`.
   - After an `unknown` outcome, an item waits about 32 s before its next call.

This option writes a row for each item and each target only when a GSI key changes or an item fails. No correctness
rule depends on a clock or a timeout.

#### 5.6.2 Design overview

1. **Change positions (Section 5.6.3).** `MaterializationDO` gives each accepted image a position in a table that all
   targets share. When an item changes again, the ingest keeps or moves its position. Each target reads each changed
   item, and no target starves.
2. **One cursor for each target (Section 5.6.4).** Each target reads the positions above its cursor and sends the
   latest image of each item in batches. It moves its cursor one time for each batch.
3. **GSI key sets (Section 5.6.5).** For each item and each GSI, `target_index_keys` holds the GSI keys that can have
   an entry in the index. `MaterializationDO` writes this row only when a key is added or removed.
4. **Producer epochs (Section 5.6.6).** Each call carries the ID and the epoch of its sender. A receiver rejects a call
   whose epoch is below the highest epoch that it stored for that sender. A child partition fences its parent with the
   same mechanism.
5. **Retry rows (Section 5.6.7).** An item that fails gets a `target_retry` row, and the cursor continues past it.

```text
Base PartitionDO (producer: base DO ID, export epoch)
  - materialization_outbox (Section 4.2.3) + inherited_fence
          |
          | applyLatestImages(producerId, epoch, ancestorFences)
          v
MaterializationDO (receiver: producer_fences; producer: MaterializationDO DO ID, one epoch per target)
  - latest_items (+ newest_seq), change_positions
  - materialization_targets: cursor_seq, epoch, open_batch
  - target_index_keys (GSI only), target_retry, ancestor_fences
          |
          +---------------------------------------------+
          | batchWriteItems(producerId, epoch,          | PushTargetItem[] + producerId, epoch,
          |   ancestorFences)                           |   ancestorFences
          v                                             v
GSI PartitionDO (receiver: producer_fences)         Push target (drops revision <= applied revision)
```

#### 5.6.3 Change positions

```sql
CREATE TABLE IF NOT EXISTS change_positions (
    seq      INTEGER PRIMARY KEY,
    item_id  INTEGER NOT NULL,
    prev_seq INTEGER            -- the older position of the same item, or NULL
) STRICT;
```

`latest_items` adds the column `newest_seq INTEGER`, the newest position of the item. The ingest writes the
`latest_items` row in all cases, so this column adds no row write. `change_positions` has no secondary index. `seq` is
the rowid, and the ingest finds the positions of an item through `newest_seq` and `prev_seq`.

Each target `t` has two positions:

- `C_t` is `cursor_seq` in SQLite. Each position at or below `C_t` is done for `t`.
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
   `C_t`, or X has a `retry` row for `t` (Section 5.6.7).
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
(Section 5.6.4) skips X.

#### 5.6.4 Target cursor and target step

`materialization_targets` (Section 4.2.7) adds three columns:

```sql
CREATE TABLE IF NOT EXISTS materialization_targets (
    target_id   TEXT    PRIMARY KEY,
    kind        TEXT    NOT NULL,
    cursor_seq  INTEGER NOT NULL DEFAULT 0,
    epoch       INTEGER NOT NULL DEFAULT 1,
    open_batch  INTEGER NOT NULL DEFAULT 0  -- 1 while a batch of this target can be in flight
) WITHOUT ROWID, STRICT;
```

The in-memory runner of Section 4.2.5 stays. `targetConcurrency` (default 2) workers round-robin across the targets,
and each target has at most 1 batch in flight.

Each target also keeps an in-memory map `item_id -> revision`, the applied map:

- The map holds the revisions that the target applied in its current epoch.
- The map has at most `appliedMapMaxEntries` (default 10,000) entries for each target, with least-recently-used
  eviction.
- The map is only an optimization. When the map has no entry for an item, the target sends the item again, and the
  receiver drops the duplicate or writes the same entry again.

A step of target `t` has these parts:

1. **Claim.** In one `transactionSync`, the step:
   - takes the due `retry` rows of `t`, up to half of `maxItemsPerStep` (Section 5.6.7),
   - reads the positions above `C_t` in `seq` order to fill the batch, up to `maxItemsPerStep` (100) items and
     `maxBytesPerStep` (1 MiB),
   - sets `open_batch = 1` (Section 5.6.6).
2. **Filter.** For each position, the step joins `latest_items` by `item_id`. It skips the position when one of these
   conditions is true:
   - the item has a `retry` row for `t`,
   - the item is already in the batch,
   - the applied map holds `latest_items.revision` for the item,
   - the partition does not own the item (`!this.fokos.owns(key)`), because the item moved to another partition.

   A skipped position is done for `t`.
3. **Build.** The step builds the entries from the latest image of each item: a push record (Section 5.6.8) or GSI
   entries (Section 5.6.5). It sets `R_t` to the highest position that the claim read.
4. **Send.** The step sends the call with `producerId`, `epoch`, and the ancestor fences (Section 5.6.6).
5. **Complete.** In one `transactionSync`, the step applies the response with the rules of Section 5.6.6. When each
   item of the batch is applied or has a `retry` row, it sets `cursor_seq` to the highest position that the claim read
   and sets `open_batch = 0`. This is 1 row update in `materialization_targets` for each batch. Then the step sets
   `R_t = C_t`.

The cursor moves to the highest position that the claim read, not to the position of the last item that the step
sent. The skipped positions are done for `t`.

A new target needs the backfill of Section 4.4.1, because the cleanup deletes the positions that all earlier targets
passed. Before the backfill scan starts, the new target sets `cursor_seq` to the highest position. Thus each change
during the scan reaches the target through a position.

#### 5.6.5 GSI key sets

```sql
CREATE TABLE IF NOT EXISTS target_index_keys (
    key_set_id  INTEGER PRIMARY KEY,
    item_id     INTEGER NOT NULL,
    target_id   TEXT    NOT NULL,
    index_keys  BLOB    NOT NULL,  -- encoded list of (index_hk, gsiSK) pairs
    UNIQUE (item_id, target_id)
) STRICT;
```

The key set of `(item_id, target_id)` holds each GSI key that can have an entry of the item in that GSI. When no row
exists, the GSI has no entry of the item. The set normally holds 1 key. It holds 2 keys while a key moves. It holds
more keys only after failed deletes. `index_keys` stores the encoded `gsiSK`, not `physicalSk`, for the reason of
Section 4.2.4. It is a rowid table, because the keys can be multiple kilobytes (Section 1.3).

For each item in a batch of GSI target `t`, the step evaluates the desired GSI key `D` from the latest image with the
rules of Section 4.2.5 (step 2). `D` is absent when the item is deleted or when a GSI key attribute is missing or
`null`. A permanent violation follows Section 4.2.6. Then the step does these steps:

1. **When `D` is present and is not in the set,** the claim `transactionSync` adds `D` to the set before the call.
2. **The first call** sends `put(D)` when `D` is present. When `D` is absent, it sends `delete(K)` for each key `K` in
   the set.
3. **The second call** of the same step sends `delete(K)` for each key `K` in the set other than `D`, after `put(D)`
   applied. Thus a key move writes the new entry before it deletes the old entry, as in Section 4.2.5.
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
rule of Section 5.6.6.

#### 5.6.6 Producer epochs and fences

##### Producers and receivers

| Hop | Producer ID | Epoch storage at the producer | Receiver checks |
| --- | --- | --- | --- |
| Base `PartitionDO` -> `MaterializationDO` (`applyLatestImages`) | 32-byte `DurableObjectId` of the base partition | `materialization_export_state` | The producer epoch, then the revision (Section 4.2.4) |
| `MaterializationDO` -> GSI `PartitionDO` (`batchWriteItems`) | 32-byte `DurableObjectId` of the `MaterializationDO` | `materialization_targets.epoch` of the GSI target | The producer epoch |
| `MaterializationDO` -> push target | 32-byte `DurableObjectId` of the `MaterializationDO` | `materialization_targets.epoch` of the push target | The revision. The producer epoch is optional (Section 5.6.8). |

Each GSI shard group gets calls from one target only. Thus a GSI partition stores one epoch for each
`MaterializationDO`, and the producer ID needs no `targetId`. The `DurableObjectId` has a fixed size of 32 bytes. A
partition ID or a DO name can hold user keys, which can be multiple kilobytes.

The base partition stores its export state in one row:

```sql
CREATE TABLE IF NOT EXISTS materialization_export_state (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    epoch       INTEGER NOT NULL DEFAULT 1,
    open_calls  INTEGER NOT NULL DEFAULT 0  -- 1 while an export call can be in flight
) STRICT;
```

Each receiver (`MaterializationDO` for the base hop, and each GSI `PartitionDO`) stores the highest epoch of each
producer:

```sql
CREATE TABLE IF NOT EXISTS producer_fences (
    producer_id  BLOB    PRIMARY KEY,  -- 32-byte DurableObjectId
    epoch        INTEGER NOT NULL
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
50 bytes, so 10,000 producers use about 500 KB in each receiver. TODO: measure the row size.

A receiver split copies the whole table to each child with `MAX(epoch)` (Section 5.6.10). The partition that applies
a forwarded call checks it.

##### The same-epoch rule

Two calls of one producer with the same epoch must not carry the same item at the same time.

- **Why.** The receiver cannot order two calls with the same epoch. For example, the base exporter sends X at revision
  2 and, in a parallel call, the delete of X at revision 3. Revision 3 applies, and the targets read the tombstone, so
  the garbage collection of Section 5.6.9 deletes it. Then revision 2 arrives, and X is created again.
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
2. **At a cold start, when `open_batch = 1` or `open_calls = 1`.** A call from before the restart can still apply. When
   the marker is `0`, no call was in flight, and the epoch stays.
3. **At a split cutover** (see "Split fences" below).

The producer must store the raised epoch before it sends the next call. If the producer sent the call first and then
stopped, it could use the old epoch again after the restart.

The markers `open_batch` and `open_calls` keep the epoch stable across idle restarts:

- If each cold start raised the epoch, each receiver would write 1 row after each wake. A `MaterializationDO` that
  wakes for each base write would then write 1 row in each GSI partition that it reaches.
- `open_batch` costs 1 more row for each batch: the claim sets it. The completion clears it in the cursor update.
- `open_calls` costs at most 2 rows for each period of export work. The first claim sets it when no call is in flight.
  The last completion clears it when no other call is in flight.

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
producer and 1 row at each receiver that gets the new epoch first. TODO: measure the normal call duration and define
the default of `callTimeoutMs`.

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
    hop          TEXT    NOT NULL,  -- 'export', or a target_id
    ancestor_id  BLOB    NOT NULL,  -- 32-byte DurableObjectId
    min_epoch    INTEGER NOT NULL,
    until_seq    INTEGER,           -- MaterializationDO: the highest copied position. NULL for the base hop.
    PRIMARY KEY (hop, ancestor_id)
) WITHOUT ROWID, STRICT;
```

4. **The child sends the fences with its inherited work.** The inherited work is the work that the parent can have in
   flight at the cutover:
   - For a `MaterializationDO` target `t`: the positions at or below `until_seq`, and the `target_retry` rows that the
     child copied (`inherited = 1`). Each call of `t` carries the fences while `C_t < until_seq` or an inherited
     `retry` row of `t` exists.
   - For the base hop: the outbox rows copied with `attempted = 1`. The migration sets `inherited_fence = 1` on them.
     The export of such a row carries the fences. A newer write that replaces the row keeps the flag. The flag goes
     away with the row at the acknowledgement delete.
5. **The child removes the fences.** When no inherited work remains for a hop, the child deletes the fence rows of that
   hop. A grandchild copies the fence rows of its parent and adds its own parent. Thus the list normally holds 0
   entries, and it holds 1 or 2 entries for a short time after a split.

The child starts its own epoch at 1. Each receiver has no row for the new `producerId`, so it accepts the first call.
The child does not depend on a parent descendant check at the receiver, because the child names each ancestor in the
call.

Why the fences are enough:

1. A late parent call can change a receiver only at an item that the parent had in flight at the cutover.
2. That item is part of the inherited work of the child:
   - A target cursor moves only after a definite outcome, so the item has a position above the copied cursor, or a
     copied `retry` row. Step 3 of Section 5.6.3 never moves a position that a cursor still needs.
   - A base outbox row of a call in flight has `attempted = 1`.
3. For a GSI item, each key that the parent call could write is in the copied key set (Section 5.6.5, step 1).
4. Thus the child sends a call with the fences to the receiver of each such key or item. A key always maps to the same
   GSI partition, and a receiver split copies `producer_fences` with `MAX`.
5. At that receiver, the late parent call and the child call arrive in one of two orders:
   - When the late call arrives first, the child call overwrites or deletes its write.
   - When the child call arrives first, the receiver stores `E`, and it rejects the late call, which has an epoch
     below `E`.

A promotion source continues to send its other keys with an epoch of `E` or higher, and each receiver accepts them.
Its calls from before the raise can get `producer_epoch_fenced`. The source discards their responses (response rule 1)
and sends those items again with epoch `E`.

#### 5.6.7 Failed items (`target_retry`)

```sql
CREATE TABLE IF NOT EXISTS target_retry (
    target_id   TEXT    NOT NULL,
    item_id     INTEGER NOT NULL,
    kind        TEXT    NOT NULL, -- 'retry' | 'violation'
    not_before  INTEGER NOT NULL, -- the backoff
    attempts    INTEGER NOT NULL DEFAULT 0,
    inherited   INTEGER NOT NULL DEFAULT 0, -- 1 when a split copied the row from the parent
    last_error  TEXT,
    PRIMARY KEY (target_id, item_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_target_retry_due
    ON target_retry (target_id, not_before)
    WHERE kind = 'retry';
```

Each target step takes the due `retry` rows (`not_before <= now`) first, up to half of `maxItemsPerStep`. Then it fills
the rest of the batch from the cursor (Section 5.6.4). When the cursor has no work, the `retry` rows can fill the whole
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
   image applies, the completion deletes the `violation` row. A violation of a tombstone follows Section 4.2.6: the
   cleanup runs, and then the completion deletes the row.

| Outcome for an item | Action of the completion |
| --- | --- |
| `applied`, and `latest_items.revision` equals the sent revision | Record the revision in the applied map. Delete the `retry` or `violation` row when one exists. |
| `applied`, and the item changed during the call | For a `retry` item, keep the row with `not_before = 0`. For a cursor item, write nothing: the change has a position above the cursor (Section 5.6.3, invariant 1). |
| `unprocessed`, or push `retry` | Insert or update the `retry` row: `attempts + 1`, and a capped exponential backoff. |
| `rejected` (`batch_request_too_large`) | Split the batch and send again. No row. |
| `rejected` (any other code) | Run the cleanup of Section 4.2.6, then set `kind = 'violation'`. |
| The whole call fails with a definite outcome | Keep the cursor. Write no `retry` row. Back off and count a circuit breaker failure. |
| `unknown` | Raise the epoch, keep the cursor, and send again at once (Section 5.6.6). |

TODO: define the cap of the `retry` backoff. TODO: define when a call with many failed items is a failure of the whole
call, for example "more than half of the batch failed with the same code". This threshold also decides when the
circuit breaker opens (Section 4.3.1), and it prevents a mass of `retry` rows when a target fails each item.

Example: the batch limit is 100, 3 `retry` rows are due, and `C_t = 500`.

1. The step takes the 3 retry items.
2. It reads positions 501 to 610. It skips 4 positions of items with `retry` rows, 3 positions that the applied map
   skips, and 6 positions of items that are already in the batch. It sends the other 97 items.
3. The call returns: 2 retry items applied, 1 retry item is `unprocessed` with `partition_migrating`, and the 97 cursor
   items applied.
4. One `transactionSync` deletes 2 `retry` rows, updates 1 `retry` row, and sets `cursor_seq = 610`.

#### 5.6.8 Push targets

- A push target needs no key set. The step sends one `PushTargetItem` (Section 4.2.5) for each item, with the latest
  image or the tombstone.
- The batch also carries `producerId`, `epoch`, and the ancestor fences.
- The push target contract of Section 4.2.5 stays. Delivery is at-least-once and coalesced. `revision` strictly
  increases for each `(baseHashKey, baseSortKey)`. The push target must keep the highest applied `revision` of each key
  and ignore each push with `revision <= appliedRevision`.
- A push target that checks the producer epoch with the receiver rules of Section 5.6.6 can delete the revision of a
  deleted key after it applies the delete. A push target that does not check the epoch must keep that revision. Its
  retention is the choice of the push target.
- After an `unknown` outcome, the push step raises its epoch and sends again at once. The push target drops the
  duplicates by revision.

#### 5.6.9 Tombstones and garbage collection

- The ingest of a delete sets `data = NULL`, `deleted = 1`, `deleted_at_ms = now`, and `newest_seq` on the
  `latest_items` row. It updates `key_size_estimates` to the key-only size.
- When a tombstone arrives for an item with no `latest_items` row, the ingest stores a tombstone row. The targets then
  send a delete that changes nothing.
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
  - When the producer is an ancestor partition, the child exported the item with the split fences (Section 5.6.6).
- While a target is down, the tombstones stay until that target reads them. A target that leaves `targets()` no longer
  counts in `MIN(C_t)`.

#### 5.6.10 Splits and promotions

A `MaterializationDO` split or promotion migrates these rows for the items of the slice:

1. `latest_items` with `item_id` and `newest_seq` unchanged.
2. `change_positions` with `seq` unchanged. The child gives each new image a position above each copied position.
3. `target_index_keys`.
4. `target_retry`, with `inherited = 1`.
5. `materialization_targets`, with `cursor_seq` unchanged. The child starts each `epoch` at 1.
6. `ancestor_fences`: the rows of the parent, plus `(parent DO ID, E, until_seq)` for each target. `until_seq` is the
   highest copied position.
7. `producer_fences`: the whole table, with `MAX(epoch)`. A child must reject each late call that its parent rejects.

A base `PartitionDO` split or promotion migrates the `materialization_outbox` rows of Section 4.2.3. Each row copied
with `attempted = 1` gets `inherited_fence = 1`. The child also gets the `ancestor_fences` row
`('export', parent DO ID, E, NULL)` and the export fence rows of the parent.

A GSI `PartitionDO` split migrates the whole `producer_fences` table to each child with `MAX(epoch)`.

`cleanupSourceStep` deletes the moved rows of `latest_items`, `change_positions`, `target_index_keys`, and
`target_retry` on the source, with the link-ID delete rule. The rule of Section 4.2.5 that advances `next_attempt_at`
on an unowned copy goes away: the target step skips an unowned item, and the position is done.

TODO: confirm that `FokosMigrationHost` serves the pages of the new streams only after the cutover, as it serves the
`items` stream. The split fences need the final export state of the slice.

#### 5.6.11 Item lifecycle

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

Split: M splits after step 1, while `put(OPEN)` is in flight.

1. M raises the `byStatus` epoch to `E` before it serves the first export state page.
2. The child M2 copies item 42, position 100, `{OPEN}`, `cursor_seq`, and the fence `(M, E, until_seq)`.
3. M2 reads position 100 and sends `put(OPEN)` with its own epoch 1 and the fence `(M, E)`.
4. When the late `put(OPEN)` of M arrives at `G_open` after the call of M2, `G_open` has stored `E` for M and rejects
   it. When it arrives before, the call of M2 overwrites it.

#### 5.6.12 Cost

SQLite table rows written for each base write, with no coalescing, for a table with `T` GSIs. The columns follow the
conventions of Section 4.2.9. The base partition and the GSI partitions are the same in both designs.

| Base write | Base partition | GSI partitions | `MaterializationDO` (Section 4.2) | `MaterializationDO` (Section 5.6) | Total (Section 4.2) | Total (Section 5.6) | `T = 10` (Section 4.2 / Section 5.6) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Put, new item | 2 + 3 | 2T | 2 + 3T | 4 + T | 7 + 5T | 9 + 3T | 57 / 39 |
| Put, every GSI key stays | 2 + 3 | 2T | 2 + 3T | 4 | 7 + 5T | 9 + 2T | 57 / 29 |
| Put, every GSI key moves | 2 + 3 | 5T | 2 + 4T | 4 + 2T | 7 + 9T | 9 + 7T | 97 / 79 |
| Delete | 3 + 3 | 3T | 4 + 3T | 6 + T | 10 + 6T | 12 + 4T | 70 / 52 |

- **Ingest.** `latest_items` and `key_size_estimates` (2), and the position insert and its cleanup delete (2). Step 2
  of Section 5.6.3 writes no position when no target read the newest position.
- **GSI key sets.** A new key costs 1 row. A key move costs 2 rows: the add and the removal. A delete costs 1 row: the
  delete of the key set. A key that stays costs 0 rows.
- **Delete.** The tombstone GC deletes `latest_items` and updates `key_size_estimates` (2).
- **Push target.** 0 rows for each item and each push target. Section 4.2 writes 3 rows for each item and each push
  target.
- **Left out of the table:**
  - The cursor update and the `open_batch` claim cost 2 rows for each batch of up to 100 items: about 0.02 rows for
    each item and each target.
  - A `producer_fences` row costs 1 row the first time that a producer calls a receiver, and 1 row when its epoch goes
    up.
  - An epoch raise costs 1 row at the producer.
  - A failed item costs about 2 rows: the insert of its `retry` row and its delete.
  - The ingest of an item with `retry` rows costs 1 row for each such row.

While a target is down, `change_positions` holds at most `T + 1` rows for each changed item. In the outage example of
Section 4.2.9, 122,000 items change, and one of 3 GSIs is down. Thus each changed item has at most 2 positions, and
the table holds at most 244,000 rows. TODO: measure the size of a `change_positions` row.

#### 5.6.13 Trade-offs

- **Reads.** Each target reads the payload of each changed item, so a payload is read `T` times. A row read costs
  $0.001 per million rows, and a row written costs $1.00 per million rows on the Durable Objects SQLite storage
  backend.
- **No time in correctness.** No rule waits for `applyBeforeMs`, `maxClockSkewMs`, or a horizon. After an `unknown`
  outcome, the target sends again at once. A split needs no wait.
- **Permanent receiver rows.** Each receiver keeps one `producer_fences` row for each producer that ever sent to it.
- **Interface changes.** `batchWriteItems` needs the GSI-only options `producerId`, `epoch`, and `ancestorFences`, and
  the code `producer_epoch_fenced`. They replace `applyBeforeMs` and `write_deadline_passed` of Section 4.2.8 and
  `docs/agent-plans/2026-10-17-batch-get-and-write-items.md`. `applyLatestImages` needs the same three fields.
- **Comparison with Section 5.3.** Section 5.3 rejects cursors for external consumers, because each consumer must then
  track the splits of `MaterializationDO`. Here each cursor is internal to `MaterializationDO` and moves with its
  migration. Thus no consumer sees a split.
- **Tombstones during an outage.** A target that is down keeps the tombstones of all deleted items until it recovers
  or leaves `targets()`. A tombstone is a key-only row.

### 5.7 Compact `target_item_state` with key-change-only pre-RPC writes

**Status:** Not decided. This option keeps `target_item_state` and `idx_target_item_state_due` from Sections 4.2.4 and
4.2.5, infers seven columns from `latest_items` and `materialization_targets`, and writes before an outbound RPC only
when a GSI key changes.

#### 5.7.1 Why

Section 5.6 removes `target_item_state`, which introduces hot-item starvation on a lagging cursor and requires a batch
threshold between per-item failures and target failures. Section 4.2.5 avoids both defects, because each `(item_id,
target_id)` has its own row in `idx_target_item_state_due` and `next_attempt_at` stays `0` when a `'DIRTY'` item changes
again.

However, Section 4.2.5 writes `target_item_state` before every outbound RPC (`WRITE_NEW`, `DELETE_OLD`, and
`PUSH_BATCH`):

1. A pre-RPC write on `target_item_state` is necessary to prevent an orphan GSI entry only when `WRITE_NEW` writes a GSI
   key that is not yet in `target_item_state`.
2. When the GSI key of an item does not change (`(target_hk, target_sk) == (applied_index_hk, applied_index_sk)`), the
   key is already durable in `applied_index_*`. A crash during `put` leaves no unrecorded GSI key.
3. On `DELETE_OLD`, the old key is already durable in `applied_index_*` or `inflight_index_*`. A crash during `delete`
   leaves no new GSI key.
4. On `PUSH_BATCH`, the push target keys records by `(baseHashKey, baseSortKey)` and deduplicates by `revision`. It has
   no old secondary key to delete.

#### 5.7.2 Design

1. **Compact schema.** `target_item_state` drops `applied_revision`, `desired_revision`, `inflight_revision`,
   `inflight_phase`, `inflight_attempt`, `inflight_apply_before_ms`, and `violation_revision`. `materialization_targets`
   adds `inflight_until`:

```sql
CREATE TABLE IF NOT EXISTS materialization_targets (
    target_id       TEXT    PRIMARY KEY,
    kind            TEXT    NOT NULL,
    inflight_until  INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS target_item_state (
    state_id          INTEGER PRIMARY KEY,
    item_id           INTEGER NOT NULL,
    target_id         TEXT    NOT NULL,

    applied_index_hk  BLOB,    -- NULL when the GSI holds no entry, or for a push_sink
    applied_index_sk  BLOB,    -- x'' when the GSI has no sort key

    inflight_index_hk BLOB,    -- set only before WRITE_NEW when target_key != applied_index_key
    inflight_index_sk BLOB,

    status            TEXT    NOT NULL, -- 'IDLE' | 'DIRTY' | 'VIOLATION'
    next_attempt_at   INTEGER NOT NULL DEFAULT 0,
    last_error_code   TEXT,

    UNIQUE (item_id, target_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_target_item_state_due
    ON target_item_state (target_id, next_attempt_at, item_id)
    WHERE status = 'DIRTY';
```

2. **Inferred state:**
   - **Desired revision.** Always `latest_items.revision` from the join `USING (item_id)`. The worker records the
     `stepRevision` of each item in memory when it claims the batch.
   - **In-flight guard.** Each `target_id` has at most 1 batch in flight in memory. No row moves to `'IN_FLIGHT'`. A row
     stays `'DIRTY'` until `COMMIT_APPLIED` sets `status = 'IDLE'` (when `latest_items.revision == stepRevision`) or
     deletes the row (when `latest_items.deleted == 1` and `latest_items.revision == stepRevision`). When a newer write
     arrives during the RPC (`latest_items.revision > stepRevision`), `status` stays `'DIRTY'` with
     `next_attempt_at = 0`.
   - **Violation recovery.** `applyLatestImages` accepts a write only when `incoming.revision > latest_items.revision`.
     Thus every accepted write is newer than any parked violation, and `applyLatestImages` unconditionally sets
     `status = 'DIRTY'` and `next_attempt_at = 0`.

3. **Target-level fence for crashes and splits.** Before a worker sends a GSI batch with `applyBeforeMs = now +
   callDeadlineMs`, its claim `transactionSync` writes `inflight_until = applyBeforeMs + maxClockSkewMs` on the one row
   of `target_id` in `materialization_targets`.
   - When the batch returns a definite outcome (`applied`, `unprocessed`, or `rejected`) for every item, the completion
     `transactionSync` sets `inflight_until = 0`.
   - When an item returns `unknown`, the completion `transactionSync` sets `next_attempt_at = applyBeforeMs +
     maxClockSkewMs` on that item row and sets `inflight_until = 0`.
   - After a crash, an eviction, or a cold start, the worker waits until `now >= inflight_until` before it claims a
     batch for `target_id`.
   - On a split or a promotion, `FokosMigrationHost` copies `materialization_targets` with `MAX(inflight_until)` to each
     child before cutover. Thus a child also waits for any in-flight call from the parent to expire.

4. **GSI execution steps for each due `'DIRTY'` row:**
   - **Stale `inflight_index_*` cleanup.** When `inflight_index_hk IS NOT NULL` and `(inflight_index_hk,
     inflight_index_sk)` differs from both `(applied_index_hk, applied_index_sk)` and the desired `(target_hk,
     target_sk)`, first send `delete(inflight_index_hk, inflight_index_sk)` with zero pre-RPC item writes. On `applied`,
     clear `inflight_index_* = NULL` in SQLite.
   - **Same GSI key (`(target_hk, target_sk) == (applied_index_hk, applied_index_sk)`):**
     - Write nothing to `target_item_state` before the RPC.
     - Send `put(target_hk, target_sk)` in `batchWriteItems`.
     - On `applied`, run `COMMIT_APPLIED` (`status = 'IDLE'` when `latest_items.revision == stepRevision`).
   - **New or changed GSI key (`(target_hk, target_sk) != (applied_index_hk, applied_index_sk)`):**
     - When `(inflight_index_hk, inflight_index_sk) != (target_hk, target_sk)`, write `inflight_index_hk = target_hk`
       and `inflight_index_sk = target_sk` in the claim `transactionSync` before `put(target_hk, target_sk)`.
     - Send `put(target_hk, target_sk)`.
     - When `put` returns `applied` and `applied_index_hk IS NOT NULL`, send `delete(applied_index_hk,
       applied_index_sk)` in the same worker step with no intermediate SQLite write to `target_item_state` (only the
       1-row update to `materialization_targets.inflight_until`).
     - When `delete` (or `put` when `applied_index_hk IS NULL`) returns `applied`, run `COMMIT_APPLIED`: set
       `applied_index_hk = target_hk`, `applied_index_sk = target_sk`, `inflight_index_* = NULL`, and update `status`.
   - **Absent desired key (`DELETE_OLD` for a delete or a sparse omission):**
     - Write nothing to `target_item_state` before the RPC.
     - Send `delete(applied_index_hk, applied_index_sk)` (and `delete(inflight_index_*)` when present).
     - On `applied`, run `COMMIT_APPLIED`.

5. **Push target execution steps (`push_sink`):**
   - Write nothing to `target_item_state` or `materialization_targets` before the RPC.
   - Send `method(batch)`.
   - On `applied`, run `COMMIT_APPLIED` (`status = 'IDLE'` or delete the row when `latest_items.deleted == 1`).

#### 5.7.3 Cost

SQLite table rows written for each base write, with no coalescing, for a table with `T` GSIs (leaving out the 2 writes
to `materialization_targets.inflight_until` per batch of up to 100 items, which cost 0.02 rows per item):

| Base write | `MaterializationDO` (Section 4.2.9) | `MaterializationDO` (Section 5.7) | Total (Section 5.7) | `T = 10` (Section 4.2.9 / Section 5.7) |
| --- | --- | --- | --- | --- |
| Put, every GSI key stays | 2 + 3T | 2 + 2T | 7 + 4T | 57 / 47 |
| Put, every GSI key moves | 2 + 4T | 2 + 3T | 7 + 8T | 97 / 87 |
| Delete | 4 + 3T | 4 + 2T | 10 + 5T | 70 / 60 |

A push target costs 2 rows in `MaterializationDO` instead of 3 (ingest and `COMMIT_APPLIED`).

#### 5.7.4 Trade-offs

- **No hot-item starvation.** A `'DIRTY'` row keeps `next_attempt_at = 0` and stays at its `item_id` position in
  `idx_target_item_state_due` when new updates arrive.
- **Per-item failure isolation.** A retry or a violation updates `next_attempt_at` or `status` on that item's own
  `target_item_state` row, with no extra retry table and no batch failure threshold.
- **Cold-start fence across the target.** When `MaterializationDO` restarts during an in-flight GSI batch, the target
  waits for `materialization_targets.inflight_until` before it claims its next batch. Normal in-memory batches and
  item-level `unknown` outcomes do not wait on `materialization_targets.inflight_until`.

## 6. Frequently Asked Questions

### 6.1 Do GSIs and push targets (for example a data lake) share the same state machine?

Yes. Both are rows in `target_item_state` that `MaterializationDO` drives (Section 4.2.5). A GSI target runs
`WRITE_NEW -> DELETE_OLD -> COMMIT_APPLIED` with `FokosDB.batchWriteItems`. A push target runs
`PUSH_BATCH -> COMMIT_APPLIED` over Workers RPC.

### 6.2 Why does `MaterializationDO` keep a full mirror of live items?

With `latest_items.data` for live items, `MaterializationDO` can evaluate projections again on a retry. Future online
targets (Section 4.4.1) can also backfill from local SQLite with zero reads from the base `PartitionDO` shard group.

### 6.3 What happens when an item is created, updated, and deleted while `MaterializationDO` is unreachable?

While `MaterializationDO` is unreachable, the outbox row keeps `unexported_create = 1` and `attempted = 0`. A local
delete removes the `materialization_outbox` row in place and sends zero RPCs (Section 4.2.3).

### 6.4 Does the change of `max_deleted_v` to `before.v + 1` change the existing transaction behavior?

No. `transactGetItems` still detects each delete between phase 1 and phase 2. New rows still start at
`d.max_deleted_v + 1`, above each earlier version of that key (Section 4.2.2). Only the version numbers change: a
recreate after a delete starts one higher than before.

## 7. References

- `docs/ideas/2026-09-17-coalesced-materialization-and-gsis.md`
- `docs/agent-plans/2026-10-03-max-deleted-version.md`
- `docs/agent-plans/2026-09-13-items-link-item_id.md`
- `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`
- `docs/agent-plans/2026-09-26-fokos-sharding-client.md`
- `docs/agent-plans/2026-10-17-batch-get-and-write-items.md`
- `packages/fokosdb/src/shared/partition/partition-store.ts`
- `packages/fokosdb/src/shared/partition/fokos-migration-host.ts`
- `packages/fokosdb/src/shared/partition/ttl-expiry.ts`
- `packages/fokosdb/src/shared/partition-context.ts`
- `packages/fokosdb/src/shared/do-stubs.ts`
- `packages/fokosdb/src/shared/expression/compiler.ts`
- `packages/fokosdb/src/shared/transaction-limits.ts`
- `packages/fokosdb/src/sharding/runtime.ts`
- `packages/fokosdb/src/server/do-partition.ts`
- `packages/fokosdb/formal/p/PSrc/Partition.p`
- `packages/fokosdb/formal/p/PSpec/Versions.p`
- [Using Global Secondary Indexes in DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html)
- [Cloudflare Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Cloudflare Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)
- [Cloudflare Queues limits](https://developers.cloudflare.com/queues/platform/limits/)
