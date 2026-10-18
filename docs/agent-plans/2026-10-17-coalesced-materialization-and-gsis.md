# RFC — Coalesced materialization for global secondary indexes and remote push targets

**State:** Draft
**Date:** 2026-10-17
**Author:** Lambros Petrou
**Status:** The delete revision `v + 1` (Section 4.2.2) is built. Nothing else is built. This document specifies
global secondary indexes (GSIs) and remote push targets.

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
- [6. Frequently Asked Questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and Context

### 1.1 The current system

FokosDB has no global secondary index (GSI) and no push connector for external systems such as a data lake. Today, a
caller reads items only by the base table `(hashKey, sortKey)` through `getItem`, `batchGetItems`, `transactGetItems`,
and `queryItems`.

Each base partition is a `PartitionDO` in the shard group `fokos.p.<tableName>`. It hosts `FokosShardingRuntime`
(`packages/fokosdb/src/sharding/runtime.ts`) and stores committed rows in SQLite through `PartitionStore`
(`packages/fokosdb/src/shared/partition/partition-store.ts`).

Each row in `items` has a version `v`. Each partition has one `deletion_metadata` row (`id = 1`) with
`max_delete_tx_order_ts` and `max_deleted_v`. A delete or a TTL expiry of a row at `v` has the delete revision
`v + 1`, and `max_deleted_v` is the highest delete revision of the partition. A new item row starts at
`v = max_deleted_v + 1`, and each update to an existing row sets `v = v + 1`. Thus the revisions of a key (each `v` and
each delete revision) always increase, also after a delete and a recreate. A split or key promotion merges
`max_deleted_v` onto the target partition with `MAX` in `FokosMigrationHost`
(`packages/fokosdb/src/shared/partition/fokos-migration-host.ts`).

### 1.2 The problems

1. **Secondary access patterns need up to 10 GSIs per table.** Each GSI extracts a different partition key, an optional
   sort key, and a projection from the base item.
2. **External systems need a coalesced push stream.** A remote target (a `WorkerEntrypoint` or a `DurableObject` that
   writes to a data lake or external service) must receive the newest state of each changed item without coupling base
   write availability to the remote target.
3. **A GSI needs the old GSI key to move an entry.** When an item changes its GSI key from `A` to `C`, the maintainer
   must delete `A` and write `C`. The new item image alone does not contain `A`.
4. **Base items move across `PartitionDO` instances.** A hash split, a key promotion, or a range split moves an item to
   a new `PartitionDO`. An old source partition can hold an unexported revision while the new owner exports a newer
   revision. Downstream targets must not depend on delivery order between base partitions.
5. **Downstream outages must not grow base partitions without bound.** A GSI partition or a remote push target can be
   slow or unavailable for hours. The base write path must stay a single local SQLite transaction, and base storage
   must not copy 400 KB item payloads for every mutation or every target.

### 1.3 Facts that constrain the design

- **An item is up to 400 KB** (`MAX_ITEM_BYTES` in `packages/fokosdb/src/shared/transaction-limits.ts`). Workers RPC
  supports payloads up to 32 MiB. Cloudflare Queues caps a message at 128 KB, so a queue message cannot carry a full
  FokosDB item.
- **Keys can be multiple kilobytes.** By default, `MAX_HASH_KEY_BYTES` is 1,024 bytes and `MAX_SORT_KEY_BYTES` is 512
  bytes, and a table policy can raise both limits.
- **`last_write_ts` is not strictly increasing per item.** `PartitionStore.upsertItem` sets
  `last_write_ts = MAX(last_write_ts, txOrderTs)`. A two-phase transaction stamps the coordinator clock, which
  `prepare` accepts up to `maxClockSkewMs` ahead. A later non-transactional write within that skew window keeps the
  same `last_write_ts`. Thus `last_write_ts` cannot order last-writer-wins (LWW) updates across retries.
- **A `WITHOUT ROWID` table bloats large payloads in Durable Object SQLite.** On a 4 KiB SQLite page, a `WITHOUT ROWID`
  table puts any row above 1,002 bytes onto a private overflow page (measured at 3.1x to 4.7x storage amplification in
  `PartitionStore`). Any table that stores item payloads or multiple multi-kilobyte keys must be a rowid table.
- **The TTL sweep deletes unowned copies today.** `PartitionStore.deleteExpiredItems` removes expired rows without
  checking `this.fokos.ownerCheck()`. A promotion source can delete an expired copy before the range root pulls it, so
  no partition writes a deletion marker for that item.
- **Durable Object alarms are too slow and expensive for per-batch draining.** `setAlarm` writes durable storage on
  every pass. High-throughput draining needs an in-memory background loop that runs while due work exists, with the
  Durable Object alarm armed only as the crash and eviction fallback.

### 1.4 Glossary

| Term | Meaning |
| --- | --- |
| Base item | One item in the user table, identified by `(baseHashKey, baseSortKey)`. |
| Base revision | The strictly increasing integer revision of one base item across writes, deletes, and splits. |
| Export outbox | The coalesced `materialization_outbox` row in a base `PartitionDO` for one dirty base item. |
| Capture | The write of `materialization_outbox` rows and their export, enabled by `captureChanges`. |
| Materialization partition | One `MaterializationDO` in the shard group `fokos.m.<tableName>`. |
| Latest image | The newest live image or deletion tombstone that `MaterializationDO` accepted for a base item. |
| Target | A registered downstream destination in `MaterializationDO`: either a GSI or a remote push target. |
| Applied state | The revision (and GSI key `(applied_index_hk, applied_index_sk)` for a GSI) last completed. |
| In-flight transition | The revision (and GSI key for a GSI) that `MaterializationDO` froze in SQLite before a call. |
| Desired state | The newest revision that a target must converge to for one base item. |
| Violation | A base item revision whose target write fails permanently (for example, an invalid GSI key type). |

## 2. Goals and Requirements

### 2.1 In scope

- Support up to 10 GSIs per table, where each GSI defines its own partition key, optional sort key, and attribute
  projection.
- Support remote push targets (a `WorkerEntrypoint` or a `DurableObject` called over Workers RPC) from the same
  `MaterializationDO` state machine.
- Define GSIs and remote push targets by subclassing `MaterializationDO` and overriding `targets()`.
- Capture one coalesced outbox marker in the base `PartitionDO` in the same SQLite transaction as each item mutation
  when `captureChanges = true`.
- Route materialization state by `(baseHashKey, baseSortKey)` in an independent shard group `fokos.m.<tableName>`.
- Drain outbox exports in `PartitionDO` and target pushes in `MaterializationDO` with an in-memory background loop
  (round-robining targets with 2 parallel workers in `MaterializationDO`), using the Durable Object alarm only for
  crash and eviction recovery.
- Keep a full mirror of live items in `MaterializationDO` (`latest_items.data`) to support target projections, retries,
  and future online target backfills (Section 4.4.1).
- Isolate failures across targets so one unavailable GSI or remote push target does not stall healthy targets or base
  writes.

### 2.2 Out of scope

- Full change data capture (CDC) history of every intermediate version. Intermediate writes before consumption coalesce
  into the latest image.
- Pull-based stream cursors. All targets use push delivery from `MaterializationDO`.
- Strongly consistent GSI reads or atomic multi-item visibility across a GSI.
- Preventing temporary duplicate entries during a cross-partition GSI key move (`WRITE_NEW` runs before `DELETE_OLD`,
  matching DynamoDB eventually consistent GSI semantics).
- Uniqueness constraints on a GSI key. Callers that need a unique secondary key must use `transactWriteItems` on the
  base table.
- Online backfill of targets added after data exists, cluster-wide activation tracking via `FokosDBAutoAdminDO`, and
  per-GSI topology overrides (tracked in Section 4.4).

### 2.3 Requirements

1. A delete of a present item at `v` must assign deletion revision `v + 1` and raise `max_deleted_v` to at least
   `v + 1`.
2. When `captureChanges` is `true`, the base partition must write the item mutation and the `materialization_outbox`
   row in one `transactionSync`. When `captureChanges` is `false`, the partition must write no `materialization_outbox`
   row. A GSI shard group must always have `captureChanges = false`.
3. The `materialization_outbox` row must store only key, revision, and retry metadata, never a copy of the item
   payload.
4. A transaction `prepare`, a failed condition, a `check`, a `cancel`, a delete of an absent item, or a migration page
   import must not write a new `materialization_outbox` revision.
5. A TTL sweep must delete only expired rows that its partition owns, and must write an outbox deletion marker for each
   deleted row when `captureChanges = true`. It must keep unowned copies until `cleanupSourceStep` deletes them.
6. Each GSI must be a standard FokosDB table (`PartitionDO`) written with `batchWriteItems` and queried with
   `queryItems` over the composite sort key. The physical key of each valid base item must fit the derived key limits
   of each GSI table.
7. Every background scan, export batch, and target push step must enforce item count, byte count, and duration bounds.
8. `FokosDB.destroy()` must destroy every shard group of the table in dependency order.

## 3. Milestones

### Milestone 1: The TTL sweep deletes only owned rows
- Change `PartitionStore.deleteExpiredItems` to filter its expired, unlocked candidates with
  `this.fokos.ownerCheck()` and to delete only the owned rows. Unowned copies stay until `cleanupSourceStep` deletes
  them (Section 4.2.3, the TTL sweep item).
- Update the comment of `deleteExpiredItems`, which says that the sweep does not test the owner of each row, and the
  unit tests and the promotion tests. This milestone ships on its own before any materialization code.

### Milestone 2: Base `materialization_outbox` and `MaterializationDO` mirror
- Add `captureChanges` and `nsMat` to `FokosTableIdentity`, `FokosTableOptions`, and `FokosDBPolicy`, and add
  `materialization_outbox` to `PartitionStore` (Sections 4.2.1 and 4.2.3).
- Wire outbox writes into `upsertItem`, `updateItemSingleShot`, `deleteItem`, `TransactionParticipant.commitLocal`,
  `TransactionParticipant.executeSingleShot`, and owned rows in `deleteExpiredItems`.
- Migrate `materialization_outbox` rows in `FokosMigrationHost`, and delete unowned outbox copies in
  `cleanupSourceStep`.
- Create `MaterializationDO` in shard group `fokos.m.<tableName>` with `latest_items`, `key_size_estimates`,
  `applyLatestImages` (`group` dispatch with `failurePolicy: "collect"`), and the in-memory base export loop with alarm
  fallback (Sections 4.2.3 to 4.2.5).

### Milestone 3: Remote push targets (`PUSH_BATCH -> COMMIT_APPLIED`)
- Add `target_item_state` and the `MaterializationDO.targets()` method override (Sections 4.2.4 and 4.2.7).
- Build the in-memory round-robin target runner (2 parallel workers), the write deadline fence, the single-phase
  `PUSH_BATCH -> COMMIT_APPLIED` state machine for `push_sink` targets, violation handling, tombstone GC, and
  `MaterializationDO` split/promotion migration (Sections 4.2.4 to 4.2.6).

### Milestone 4: Static GSIs (`WRITE_NEW -> DELETE_OLD -> COMMIT_APPLIED`) and `db.gsi(indexName)`
- Add `applyBeforeMs` and `write_deadline_passed` to `batchWriteItems` from
  `docs/agent-plans/2026-10-17-batch-get-and-write-items.md` (Section 4.2.8).
- Build the `WRITE_NEW -> DELETE_OLD -> COMMIT_APPLIED` state machine for `gsi` targets using `FokosDB.batchWriteItems`
  (Section 4.2.5).
- Add the composite sort key codec (`<L><flag><D>`), `{ ref: "sortKey" }` compiler rules, derived GSI key limits,
  `db.gsi(indexName).queryItems`, and `FokosDB.destroy()` across all table shard groups (Sections 4.2.7 and 4.2.8).

### Milestone 5: Circuit breaker tuning and admission thresholds
- Resolve Open Questions 4.3.1 and 4.3.2 and tune circuit breaker scope and storage admission thresholds.

## 4. Proposed Solution

### 4.1 High-level overview

A table with GSIs or remote push targets runs independent `FokosShardingRuntime` shard groups connected by
`FokosShardingClient`:

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

1. **Base write (`PartitionDO`):** When `captureChanges = true`, every committed write, delete, or owned TTL expiry
   updates `items`, upserts a compact marker into `materialization_outbox` in one `transactionSync`, and wakes the
   in-memory export loop.
2. **Export (`PartitionDO -> MaterializationDO`):** The export loop claims owned due outbox rows, reads live payloads
   from `items`, and sends batched `applyLatestImages` RPCs to `fokos.m.<tableName>`. `MaterializationDO` updates
   `latest_items`, raises `desired_revision` on each registered target, wakes its in-memory target push loop, and
   acknowledges each item at once.
3. **Target push (`MaterializationDO -> GSI or remote sink`):** A single in-memory background loop in
   `MaterializationDO` round-robins across registered targets with 2 parallel workers (at most 1 batch in flight per
   target). For a GSI, it freezes the transition in `target_item_state`, writes entries with `FokosDB.batchWriteItems`,
   deletes old GSI keys when a key moves, and records the applied GSI key. For a remote push target, it pushes a batch
   of `PushTargetItem` records over Workers RPC and records the applied revision.

### 4.2 Technical details

#### 4.2.1 Shard groups, bindings, and routing

`FokosTableIdentity` and `FokosDBPolicy` (`packages/fokosdb/src/shared/partition-context.ts`) add the optional
`MaterializationDO` namespace binding key `nsMat?: MaterializationNamespaceKey`. `createTableConfig` needs `nsMat`
when `captureChanges` is `true`, and `shared/do-stubs.ts` adds `materializationNamespace` and
`materializationStubByName` (applying `jurisdiction` and `locationHint` like `partitionStubByName`).

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
split thresholds from the base table's route context, so every client and DO knows their topology without extra
configuration.

The three sharded tiers split independently:

1. **Base split or promotion (`fokos.p`):** Because `fokos.m.<tableName>` routes by `(baseHK, baseSK)` and not by the
   base partition ID, a base split or promotion does not change which `MaterializationDO` owns `(baseHK, baseSK)`:

```text
Base Table (fokos.p.orders)                          Materialization (fokos.m.orders)
---------------------------                          --------------------------------
fokos.p.orders~h.0 (old source, rev=5) ------------> fokos.m.orders~h.0
fokos.p.orders~r.dTE.. (promoted root, rev=6) ----->   owns (hk="u1", sk="o9")
                                                       accepts rev=6, acks late rev=5 as stale
```

2. **`MaterializationDO` split or promotion (`fokos.m`):** Each base `PartitionDO` sends exports through a
   `FokosShardingClient` for `fokos.m.<tableName>`. When a `MaterializationDO` splits or promotes a hot base hash key,
   its migration host moves the `latest_items` row and all `target_item_state` rows for each item in the same slice.
   Each child continues pushing the items in its slice with no external cursor handoff.
3. **GSI split or promotion (`fokos.gsiv1`):** Each `MaterializationDO` holds one `FokosShardingClient` per GSI shard
   group. A low-cardinality GSI hash key (such as `status = "OPEN"`) promotes to a range tree in that GSI shard group
   and splits by `physicalSk` without splitting `fokos.p` or `fokos.m`.

#### 4.2.2 Monotonic base revisions

Every item mutation has a strictly increasing integer revision per `(baseHK, baseSK)` across creates, updates,
deletes, TTL expiries, and repartitions. This behavior is built: a delete advances `max_deleted_v` to `before.v + 1`.

1. In `PartitionStore.deleteItem`, when `before` exists at `before.v`, the delete has the revision
   `deleteVersion = before.v + 1`. `deleteItem` calls
   `#raiseDeletionMetadata(Math.max(opts.txOrderTs, before.last_read_ts), deleteVersion)` and returns `deleteVersion`.
   `deleteVersion` is absent when no row was removed.
2. In `PartitionStore.deleteExpiredItems`, each deleted row at `row.v` has the delete revision `row.v + 1`, and the
   chunk raises `max_deleted_v` to `MAX(max_deleted_v, row.v + 1)`. The method returns only counts today. Milestone 2
   uses the `v` that its `RETURNING` clause already reads to write the deletion marker of each row.
3. The next `upsertItem` or `composeUpdateWriteStatement` for that key starts at `d.max_deleted_v + 1`, which is at
   least `before.v + 2`.
4. The P model (`PSrc/Partition.p`) announces the delete revision of a present row as its `v`, and the monitor
   `VersionIncreases` (`PSpec/Versions.p`) checks that each revision of a key is above all earlier revisions.

```text
Create X   -> v = 1 (revision 1, deleted = 0)
Update X   -> v = 2 (revision 2, deleted = 0)
Delete X   -> max_deleted_v = 3 (revision 3, deleted = 1)
Recreate X -> v = 4 (revision 4, deleted = 0)
```

This preserves all existing transaction invariants: `transactGetItems` still detects a create and delete between phase 1
and phase 2 (`max_deleted_v` increases by 2 instead of 1), and a new row still starts strictly above every earlier `v`
and deletion revision.

#### 4.2.3 Base mutation capture and outbox

`FokosDBPolicy` and `FokosTableOptions` add `captureChanges` (default `false`), which travels in `PartitionContext`.
When `captureChanges` is `false` (including every GSI shard group), a partition writes no outbox row and runs no export
loop.

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

When `captureChanges = true`, every committed content mutation updates `materialization_outbox` in the same
`transactionSync` as `items`:

1. **Insert of a new item (no existing row in `items`):**
   - If no row exists in `materialization_outbox`, insert
     `(hk, sk, revision = newV, deleted = 0, unexported_create = 1, attempted = 0, next_attempt_at = 0)`.
   - If a row already exists in `materialization_outbox` (an unacknowledged delete), update it to
     `revision = newV, deleted = 0, unexported_create = 0, next_attempt_at = 0`.
2. **Update of an existing item:**
   - Upsert `(hk, sk, revision = newV, deleted = 0, next_attempt_at = 0)`. Keep `unexported_create` and `attempted` if
     the row exists; else set both to `0`.
3. **Exporter claim:**
   - Before sending an `applyLatestImages` RPC for a batch of keys, the exporter sets `attempted = 1`,
     `apply_before_ms = now + callDeadlineMs`, and `next_attempt_at = apply_before_ms + maxClockSkewMs` on those rows
     in SQLite. When the circuit breaker to `MaterializationDO` is open, the exporter claims no rows and `attempted`
     stays `0`.
4. **Delete of an existing item (or owned TTL expiry):**
   - If the `materialization_outbox` row has `unexported_create = 1 AND attempted = 0`, no byte for this item has left
     the base partition. `PartitionStore` must delete the `materialization_outbox` row in place and must send no RPC
     to `MaterializationDO`.
   - Otherwise, `PartitionStore` upserts
     `(hk, sk, revision = deleteVersion, deleted = 1, unexported_create = 0, next_attempt_at = 0)`. The `attempted`
     guard prevents a lost delete when an earlier create RPC timed out after `MaterializationDO` committed it.

For each claimed row in `materialization_outbox`, the base exporter acts as follows:

1. In the claim `transactionSync`, it selects up to `maxItemsPerStep` (100) rows with `next_attempt_at <= now` from
   `idx_materialization_outbox_due` (bounded by `maxBytesPerStep` = 1 MiB). For any unowned copy of a promoted key
   (`!this.fokos.owns(key)`), it advances `next_attempt_at = now + exportRetryMs` (default 30 s) and skips the row so
   unowned copies never starve owned keys behind them or spin the alarm.
2. For each owned row with `deleted = 0`, it reads the item from `items` in the same synchronous snapshot and checks
   `items.v == outbox.revision`. If the row is absent or `items.v != outbox.revision` (an invariant defect), it logs
   one error with the keys, deletes the outbox row, and continues.
3. For `deleted = 1`, it builds a tombstone entry with no item payload.
4. On receiving the `applyLatestImages` response, for each item that `MaterializationDO` acknowledged at
   `ackedRevision` and that this partition still owns (`this.fokos.owns(key)`), it runs:

```sql
DELETE FROM materialization_outbox
 WHERE hk = ?1 AND sk = ?2 AND revision = ?3;
```

The base partition must delete a `materialization_outbox` row on acknowledgement only when
`stored.revision == acked.revision` and the partition still owns the key.

If a newer write (`revision = 12`) arrived while `revision = 11` was in flight, the `DELETE` for `11` matches zero rows
and leaves `12` in the outbox with `next_attempt_at = 0`. For an item that returned `unprocessed`, the exporter sets
`next_attempt_at` to a backoff timestamp.

Splits, promotions, and TTL sweeps handle outbox rows and ownership as follows:

1. **Migration (`FokosMigrationHost`):** Adds a third migration stream `materialization_outbox` after `items` and
   `pending_tx`. It copies outbox rows in the slice to the child with `attempted = 1` and `next_attempt_at = 0`,
   because an export call from the source can be in flight at cutover.
2. **Cutover ownership:** After cutover, `this.fokos.owns(key)` is `false` on the source. The source exporter skips
   unowned outbox rows (advancing their `next_attempt_at`) and drops acknowledgements for unowned keys. The child
   exports its migrated copy, and `MaterializationDO` acknowledges an already-applied revision idempotently.
3. **Source cleanup (`cleanupSourceStep`):** After migration reaches `completed`, `cleanupSourceStep` deletes the
   migrated `materialization_outbox` copies (all rows on a split; the promoted `hashKey` rows on a promotion) along
   with `pending_transactions` copies and (on a promotion) `items` rows, returning `true` only when all tables have
   zero rows left for the moved slice.
4. **TTL sweep (`PartitionStore.deleteExpiredItems`):** In one `transactionSync`, the sweep reads a chunk of expired,
   unlocked candidates in `(ttl_epoch_utc_seconds, hk, sk)` order, filters them with `this.fokos.ownerCheck()`, and
   deletes only the owned rows. Unowned copies stay on the promotion source until `cleanupSourceStep` removes them, so
   the range root always pulls an expired row during migration and its own sweep writes the deletion marker after
   import. When a chunk holds unowned copies, the sweep cycle stops early and owned expired rows that sort after those
   copies wait until `cleanupSourceStep` finishes. TODO: measure this delay for a promoted key.

#### 4.2.4 `MaterializationDO` storage and deletion lifecycle

`MaterializationDO` keeps the latest payload of every live base item in `latest_items.data` (full-mirror mode). Both
item tables are rowid tables, `target_item_state` links to `latest_items.item_id` (an 8-byte integer) so `(base_hk,
base_sk)` is stored once per item instead of once per target, and `key_size_estimates` tracks per-`base_hk` byte
totals for `promotionCandidates` and `planRangeSplit`:

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
`failurePolicy: "collect"`, and `admissionTag: "batch_write"`. `MaterializationDO` must accept an export only when
`incoming.revision > stored.revision`, and must idempotently acknowledge an export when
`incoming.revision == stored.revision`. When `MaterializationDO` is above `1.1x maxSizeMb`, its local handler must
reject only live upserts (`deleted = 0`), as `unprocessed` with `partition_over_size` (and signal `evaluateSplit`). It
must still ingest deletions (`deleted = 1`), so deletes can reclaim space.

Row lifecycle in `MaterializationDO`:

1. **Live item (`deleted = 0` at `revision = R`):** `latest_items` upserts the row (`deleted = 0`,
   `deleted_at_ms = NULL`, `data`, `est_row_bytes`) and updates `key_size_estimates`. For each target in
   `this.targets()`, `target_item_state` upserts `(item_id, target_id)` with `desired_revision = R`: if the row does
   not exist or its `status` is not `'IN_FLIGHT'`, it sets `status = 'DIRTY'` and `next_attempt_at = 0`; if its
   `status` is `'IN_FLIGHT'`, it leaves `status` and `inflight_*` unchanged.
2. **Deleted item (`deleted = 1` at `revision = R`):**
   - On ingestion, `MaterializationDO` sets `latest_items.data = NULL`, `deleted = 1`, `deleted_at_ms = NULL`, and
     updates `est_row_bytes` (to the key-only tombstone size) and `key_size_estimates`, freeing the item payload at
     once. For each target row, it sets `desired_revision = R` and moves any row whose status is not `'IN_FLIGHT'` to
     `'DIRTY'` with `next_attempt_at = 0`. If no `target_item_state` row exists for `item_id` (no target is defined),
     the same transaction sets `latest_items.deleted_at_ms = now`.
   - When a target finishes applying the deletion `R` (or marks a deletion violation), `MaterializationDO` deletes
     that `target_item_state` row. When the last `target_item_state` row for `item_id` is deleted while
     `latest_items.deleted = 1`, `MaterializationDO` sets `latest_items.deleted_at_ms = now`.
   - The `latest_items` tombstone row (`data = NULL`) stays until `now - deleted_at_ms >= deletedRevisionHorizonMs`
     (default 24 hours) so any late export with `revision < R` is rejected. A background sweep on
     `idx_latest_items_deleted_gc` then deletes the `latest_items` row (and subtracts its key bytes from
     `key_size_estimates`).
3. **Link-ID delete rule:** Every path that deletes a `latest_items` row (the tombstone GC sweep and
   `cleanupSourceStep` on a promotion source) must first delete all `target_item_state` rows with that `item_id` in the
   same `transactionSync` so SQLite rowid reuse never attaches orphaned `target_item_state` rows to a new base item.

The row lifecycle must keep these invariants:

1. Ingesting a new latest image must update `latest_items` and raise `desired_revision`. It must not change
   `inflight_*` or the `status` of an `'IN_FLIGHT'` row. Only a row that is not `'IN_FLIGHT'` moves to `'DIRTY'` with
   `next_attempt_at = 0`.
2. A `'DIRTY'` row must have `inflight_revision IS NULL`.
3. A `latest_items` tombstone (`deleted = 1`) must set `deleted_at_ms` as soon as no `target_item_state` row remains for
   its `item_id`, including when no target is registered.
4. A `MaterializationDO` split or promotion must move the `latest_items` row and all `target_item_state` rows for a
   base item in the same slice. Any deletion of a `latest_items` row must delete its `target_item_state` rows in the
   same `transactionSync` first.

`applied_index_sk` and `inflight_index_sk` store the encoded `gsiSK` (`KeyBytes`, or `x''` when the GSI has no sort
key), not `physicalSk` (Section 4.2.8). With 1.5 KB base keys and 10 GSIs, storing `gsiSK` instead of `physicalSk`
saves about 15 KB per item at rest and 30 KB while `'IN_FLIGHT'`. When sending a GSI call, `MaterializationDO` reads
`base_hk` and `base_sk` from `latest_items` by `item_id` (before the trailing `data` column, so SQLite reads no
overflow page) and builds `physicalSk = encodeGsiSortKey(index_sk, base_hk, base_sk)`.

#### 4.2.5 Target push state machine and background runner

`MaterializationDO` drives two target kinds:

- **`gsi`:** Writes to `fokos.gsiv1.<n>.<tableName>.<indexName>` with `FokosDB.batchWriteItems` in two phases
  (`WRITE_NEW -> DELETE_OLD -> COMMIT_APPLIED`).
- **`push_sink`:** Calls a `WorkerEntrypoint` or `DurableObject` over Workers RPC in one phase
  (`PUSH_BATCH -> COMMIT_APPLIED`), because a remote sink keys records by `(baseHashKey, baseSortKey)` and has no old
  secondary key to delete.

`MaterializationDO` must write `inflight_revision`, `inflight_phase`, `inflight_attempt`, `inflight_apply_before_ms`,
`next_attempt_at`, and (for a GSI) `inflight_index_hk` and `inflight_index_sk` to SQLite before it sends an outbound
RPC to a target.

##### GSI transition phases (`WRITE_NEW -> DELETE_OLD -> COMMIT_APPLIED`)

For a GSI target, `MaterializationDO` must not freeze a new transition over a row whose `inflight_revision` is not
`NULL`. It must finish or clean up the in-flight transition first.

For each due row (`next_attempt_at <= now`) on a `gsi` target:

1. **Resume an `'IN_FLIGHT'` row (`status == 'IN_FLIGHT'`):**
   - If `inflight_phase == 'DELETE_OLD'`, freeze and resend `delete` for `(applied_index_hk, applied_index_sk)`. On
     `applied`, advance to step 6 (`COMMIT_APPLIED`).
   - If `inflight_phase == 'WRITE_NEW'` and `desired_revision == inflight_revision`, re-project `latest_items.data`,
     freeze, and resend `put` for `(inflight_index_hk, inflight_index_sk)` with `ttlAt: latest_items.ttl_at`. On
     `applied`, advance to step 5 (`DELETE_OLD`).
   - If `inflight_phase == 'WRITE_NEW'` and `desired_revision > inflight_revision`, a newer base write superseded
     `inflight_revision` after the earlier `WRITE_NEW` call failed. If `(inflight_index_hk, inflight_index_sk)` equals
     `(applied_index_hk, applied_index_sk)`, clear `inflight_*` and move directly to step 2 (`status = 'DIRTY'`) with
     zero RPCs so the live `applied_index_*` entry is not deleted. Otherwise, freeze and send `delete` for
     `(inflight_index_hk, inflight_index_sk)`; on `applied`, clear `inflight_*`, keep `applied_*` unchanged, and set
     `status = 'DIRTY'` and `next_attempt_at = 0`.
2. **Evaluate desired state for a `'DIRTY'` row (`status == 'DIRTY'`):**
   - If `inflight_revision IS NOT NULL` (an invariant defect), log one error and run step 1 as an `'IN_FLIGHT'` row.
   - If `latest_items.deleted == 1`, or the GSI partition key attribute is missing or `null`, or the GSI defines a
     `sortKey` whose attribute is missing or `null` (sparse index), the desired state is `absent`.
   - Otherwise, evaluate the GSI key and projection expressions against `latest_items` (aliasing `base_hk AS hk,
     base_sk AS sk, revision AS v, ttl_at AS ttl_epoch_utc_seconds` so compiled expression SQL reads the expected
     column names) to produce `target_hk` and `target_sk`. If evaluation fails with a permanent violation
     (Section 4.2.6), run the violation cleanup instead of `WRITE_NEW`.
3. **Short-circuit when both applied and desired are absent:**
   - If `applied_index_hk IS NULL`, `inflight_index_hk IS NULL`, and desired is `absent`, delete the
     `target_item_state` row when `latest_items.deleted == 1` (setting `latest_items.deleted_at_ms = now` when no
     target row remains), or set `applied_revision = desired_revision, status = 'IDLE'` with zero RPCs.
4. **Phase `WRITE_NEW` (when desired is present):**
   - Freeze `inflight_revision = desired_revision`, `inflight_index_hk = target_hk`, `inflight_index_sk = target_sk`,
     `inflight_phase = 'WRITE_NEW'`, and `status = 'IN_FLIGHT'` in SQLite before sending the `put` entry (with
     `ttlAt: latest_items.ttl_at`) in `gsiDb.batchWriteItems(...)`. On `applied`, advance to step 5.
5. **Phase `DELETE_OLD` (when desired is absent, or when `WRITE_NEW` completed for a different GSI key):**
   - When entering from step 2 with `desired = absent` and `applied_index_hk IS NOT NULL`, freeze
     `inflight_revision = desired_revision`, `inflight_index_hk = NULL`, `inflight_index_sk = NULL`,
     `inflight_phase = 'DELETE_OLD'`, and `status = 'IN_FLIGHT'` in SQLite and send a `delete` entry for
     `(applied_index_hk, applied_index_sk)` in `gsiDb.batchWriteItems(...)`.
   - When entering after `WRITE_NEW` (`inflight_index_hk IS NOT NULL`), if `applied_index_hk IS NOT NULL` and
     `(applied_index_hk, applied_index_sk) != (inflight_index_hk, inflight_index_sk)`, set
     `inflight_phase = 'DELETE_OLD'` and `next_attempt_at = 0` so the next batch sends `delete` for
     `(applied_index_hk, applied_index_sk)`. Otherwise, advance immediately to step 6 in the same `transactionSync`.
6. **Phase `COMMIT_APPLIED`:**
   - If `latest_items.deleted == 1` and `inflight_revision == latest_items.revision`, delete the `target_item_state`
     row (and set `latest_items.deleted_at_ms = now` if no `target_item_state` row remains for `item_id`).
   - Otherwise, set `applied_revision = inflight_revision`, `applied_index_hk = inflight_index_hk`,
     `applied_index_sk = inflight_index_sk`, and clear `inflight_*`. If `desired_revision > applied_revision`, set
     `status = 'DIRTY'` and `next_attempt_at = 0`; else set `status = 'IDLE'`.

##### Remote push target phase (`PUSH_BATCH -> COMMIT_APPLIED`)

For each due batch on a `push_sink` target:

1. Freeze `inflight_revision = desired_revision`, `inflight_phase = 'PUSH_BATCH'`, and `status = 'IN_FLIGHT'` on each
   row in SQLite before the call. A resumed `'IN_FLIGHT'` row freezes again with the current `desired_revision` and
   current image (or tombstone), because the sink keys items by `(baseHashKey, baseSortKey)`.
2. Build the batch of `PushTargetItem` records, applying the optional target projection to live items:

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
   call throws, fails at transport, or returns an array of the wrong length, treat every item in the batch as `unknown`.

Push target contract:
- Delivery is at-least-once and coalesced (intermediate revisions can be skipped).
- `revision` strictly increases per `(baseHashKey, baseSortKey)` across creates, updates, and deletes.
- Two pushes for one key can arrive out of order if an earlier call stays in flight after a lost response. The target
  must retain the highest applied `revision` per key (and retain a deleted key's revision for at least
  `deletedRevisionHorizonMs`) and ignore any push with `revision <= appliedRevision`.

##### In-memory background runner and alarm fallback

Relying on Durable Object alarms to drain every batch is too slow and writes alarm storage on every step. Both
`PartitionDO` (for `materialization_outbox`) and `MaterializationDO` (for `target_item_state`) drive work with a single
in-memory background promise (like `TtlExpiry.runCycle` in `packages/fokosdb/src/shared/partition/ttl-expiry.ts`), and
use a `FokosScheduler` job only as the durable fallback if the DO restarts or is evicted.

An export or target push job must act only on rows that its partition owns. It must advance `next_attempt_at` on
skipped unowned copies, so they never block owned keys or spin the alarm. It must write an outcome only for a row that
its partition still owns and whose `inflight_attempt` matches the call.

1. **Single background loop in `MaterializationDO` (`#pushPromise`):**
   - `applyLatestImages` wakes the loop as soon as a batch commits.
   - The loop spawns `targetConcurrency` (default 2) parallel worker coroutines that round-robin across the targets
     returned by `this.targets()`. At most 1 batch call is in flight for any given `target_id` at a time, so two
     workers never claim the same target concurrently, and one slow target holds at most 1 of the 2 workers while the
     other worker drains all remaining healthy targets.
   - To claim a batch for `target_id`, the worker runs one synchronous `transactionSync`: it selects up to
     `maxItemsPerStep` (100) due rows (`next_attempt_at <= now`, bounded by `maxBytesPerStep` = 1 MiB) from
     `idx_target_item_state_due` joined with `latest_items` (`USING (item_id)`). For any unowned copy of a promoted
     key (`!this.fokos.owns(key)`), it advances `next_attempt_at = now + targetRetryMs` (default 30 s) and skips the
     row. For each owned row, it freezes the transition (writing `inflight_attempt = inflight_attempt + 1`,
     `inflight_apply_before_ms = now + callDeadlineMs`, and
     `next_attempt_at = inflight_apply_before_ms + maxClockSkewMs`).
   - The worker `await`s the outbound `batchWriteItems` or `push_sink` RPC (with a per-call timeout of
     `callDeadlineMs`), then runs one `transactionSync` to apply the outcome of each row that this partition still owns
     and whose `inflight_attempt` still matches the call.
   - The workers repeat in memory (yielding with `scheduler.wait(0)` between batches) until no target has due rows or
     `canSweepLocally` becomes `false`.
2. **Single background loop in `PartitionDO` (`#exportPromise`):**
   - Every committed base write wakes `#exportPromise`.
   - The loop claims up to `maxExportCalls` (default 2) batches of owned due rows from `idx_materialization_outbox_due`
     in one `transactionSync`, `await`s the `applyLatestImages` RPCs in parallel, deletes acknowledged rows in one
     `transactionSync`, and repeats until no due outbox rows remain or `canSweepLocally` becomes `false`.
3. **Lifecycle guard and alarm fallback:**
   - Neither loop runs when `canSweepLocally` is `false` (uninitialized, router, `awaiting_data`, `importing`, or
     fenced for destroy).
   - The `FokosScheduler` job (`materialization_export` in `PartitionDO`, `target_push` in `MaterializationDO`) provides
     crash and eviction recovery: when the in-memory loop is running, `deadline()` returns `now + fallbackAlarmMs` so
     `FokosScheduler` never spins in a 10 ms loop; when the in-memory loop is idle, `deadline()` returns the earliest
     `next_attempt_at` in SQLite, and `runStep()` starts the in-memory loop.

##### The write deadline fence

Because a GSI table stores no per-entry revision or tombstone, a late forwarded `put(B)` from a timed-out call can
otherwise arrive after the resume `delete(B)` and leave an orphan entry. `MaterializationDO` must send a call for a row
only when no earlier call for that row can still apply. Each export call and each GSI `batchWriteItems` call must carry
`applyBeforeMs`, and the receiver must reject an item with `write_deadline_passed` when its clock is above
`applyBeforeMs`. The write deadline fence prevents any overlap between consecutive calls for the same row:

1. **Freeze:** In the `transactionSync` before each call, the step increments `inflight_attempt` and writes
   `inflight_apply_before_ms = now + callDeadlineMs` (default 30 s) and
   `next_attempt_at = inflight_apply_before_ms + maxClockSkewMs` (default 2,000 ms).
2. **Receiver deadline check:** The call carries `applyBeforeMs = inflight_apply_before_ms`. Both `PartitionDO` (in
   each item's `transactionSync` block of `apiBatchWriteItems`) and `MaterializationDO` (in `applyLatestImages`) compare
   their clock with `applyBeforeMs`. When `clock > applyBeforeMs`, the handler writes nothing and returns `unprocessed`
   with `write_deadline_passed`.
3. **Next call scheduling:** When a call returns a definite outcome (`applied`, `unprocessed`, or `rejected`), the
   completion handler updates `next_attempt_at` at once because no copy of that call remains executable. When the
   outcome is `unknown` or the call times out, `next_attempt_at` stays at
   `inflight_apply_before_ms + maxClockSkewMs`. Because `maxClockSkewMs` bounds the clock offset between any two
   Durable Objects, the receiver clock is past `applyBeforeMs` before the sender clock reaches
   `applyBeforeMs + maxClockSkewMs`. Thus every copy of the earlier call has either applied or expired before the next
   call starts. `callDeadlineMs` must exceed the longest normal call duration including forwards. TODO: measure that
   duration.

##### Call outcomes

| Outcome | Meaning | Action in completion handler |
| --- | --- | --- |
| `applied` | The write committed. | Advance phase; set `next_attempt_at = 0`. |
| `unprocessed` (or push `retry`) | Nothing applied. | Keep frozen state; backoff; count breaker failure. |
| `rejected` (`batch_request_too_large`) | RPC above 32 MiB; nothing applied. | Split batch and retry; no violation. |
| `rejected` (any other code) | Permanent item failure. | Transition item to `'VIOLATION'` (Section 4.2.6). |
| `unknown` | Write can have applied. | Keep frozen state and `next_attempt_at` at the deadline fence. |

`MaterializationDO` validates each item before calling `batchWriteItems`. If `batchWriteItems` still throws a whole-call
validation error before I/O, `MaterializationDO` retries each item of that batch individually so only the invalid item
becomes `'VIOLATION'`.

#### 4.2.6 Permanent violations

When `MaterializationDO` evaluates a GSI key and projection against `latest_items` or pushes an item, three cases can
occur:

1. **Missing or `null` GSI key attribute (sparse omission — not a violation):** The item is absent from that GSI
   (`desired = absent`).
2. **Invalid GSI key type or size (`VIOLATION`):**
   - **Type violation:** The extracted GSI key is not a valid non-empty string (`text` without NUL `\0` or lone UTF-16
     surrogates) or non-empty `Uint8Array` (`bytes`) — for example, a number, boolean, array, object, empty string,
     string containing `\0` (`key_contains_nul`), empty byte array, lone UTF-16 surrogate, or a JSON path evaluated
     against a `bytes` or `text` base item.
   - **Size violation:** The encoded GSI hash key exceeds the base table's `maxHashKeyBytes`, or the encoded GSI sort
     key exceeds its `maxSortKeyBytes` (Section 4.2.8).
3. **Unwritable entry (`VIOLATION`):**
   - **Projected `'bytes'` attribute in a GSI `projection`:** A GSI `projection` builds a JSONB document (`kind =
     "json"`), which cannot store raw binary values (`Uint8Array`). Any projected cell that evaluates to `'bytes'` at
     run time is a violation (matching `update_value_is_bytes` in `UpdateExpression`), and `GsiTargetDefinition`
     validation rejects compile-time constant `'bytes'` expressions in `projection`.
   - **Row size:** The GSI entry row (projected data + GSI hash key + `physicalSk` + fixed overhead), measured in
     `MaterializationDO` with `estRowBytesExpr`, `itemDataExpr`, and `validateItemDataSize`, exceeds `MAX_ITEM_BYTES`
     (400 KB).
   - **Target rejection:** The GSI returns `rejected` with a code other than `batch_request_too_large`, or a remote
     push target returns `{ outcome: "rejected", code }`.

A permanent failure of one item on a target must mark that `(item_id, target_id)` as `'VIOLATION'` after the cleanup
of any earlier GSI entry. It must not fail other items in the batch, block base outbox acknowledgements, or trip the
target circuit breaker.

To prevent a poison item from retrying forever or tripping the target circuit breaker, `MaterializationDO` handles a
violation as follows:

1. **Clean up any earlier GSI entry:** If the row has an in-flight GSI key (`inflight_index_hk IS NOT NULL`) that
   differs from `applied_index_*`, delete `(inflight_index_hk, inflight_index_sk)` first. If
   `applied_index_hk IS NOT NULL`, run `DELETE_OLD(applied_index_hk, applied_index_sk)` so the GSI retains no stale
   entry from an older valid revision.
2. **Park in `'VIOLATION'`:** If the violating revision is a deletion (`latest_items.deleted == 1`), log the violation
   and delete the `target_item_state` row so the tombstone can expire. Otherwise, once `applied_index_hk` and
   `inflight_index_hk` are `NULL`, set `status = 'VIOLATION'`, `violation_revision = desired_revision`,
   `last_error_code = code`, and `next_attempt_at = 0`, and emit one structured error log. Because
   `idx_target_item_state_due` indexes only `status IN ('DIRTY', 'IN_FLIGHT')`, a `'VIOLATION'` row uses zero runner
   work.
3. **Recover on a newer write:** When a base write or delete arrives with `revision > violation_revision`,
   `applyLatestImages` updates `desired_revision = revision`, sets `next_attempt_at = 0`, and moves `status` from
   `'VIOLATION'` to `'DIRTY'`.

#### 4.2.7 Target definitions and table destroy

Users declare GSIs and remote push targets by subclassing `MaterializationDO` and overriding `targets()` (the same
pattern as `PartitionDO.fokosConfig()`):

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

- Every `MaterializationDO` leaf validates `this.targets()` at startup (at most 10 GSIs, unique non-empty `targetId`
  values without `~`, valid `hashKey`, `sortKey`, and `projection` expressions) and persists the known `targetId` and
  `kind` values in a local SQLite table `materialization_targets` so `destroy()` knows every GSI ever written.
- A helper in `packages/fokosdb/src/shared/do-stubs.ts` resolves `binding` (and `objectName` when present) from the
  Worker `env`.

##### Table destroy

`FokosDB.destroy()` destroys the table's shard groups in writer-before-receiver order so an in-flight RPC never
recreates a destroyed Durable Object:

1. `fokos.tc.<tableName>`
2. `fokos.p.<tableName>` — `fokosPrepareDestroy` fences each base partition, stops and awaits its in-memory
   `#exportPromise`, and returns the maximum `apply_before_ms` of any `attempted = 1` row that had no in-memory promise
   (from an earlier restart). If that timestamp exists, `destroy()` waits until `maxApplyBeforeMs + maxClockSkewMs`
   (0 ms when all calls settled in memory or when `captureChanges = false`) before deleting storage.
3. `fokos.m.<tableName>` (when `nsMat` is configured) — `fokosPrepareDestroy` fences each `MaterializationDO`, stops
   and awaits its in-memory `#pushPromise`, collects the union of GSI `targetId` values from `materialization_targets`,
   and returns the maximum `inflight_apply_before_ms` of any unresolved `'IN_FLIGHT'` row that survived a restart. If
   that timestamp exists, `destroy()` waits until `maxInflightApplyBeforeMs + maxClockSkewMs` before deleting storage.
4. `fokos.gsiv1.<n>.<tableName>.<indexName>` for each collected GSI `targetId`.

Each step is idempotent. Callers must not reuse `<tableName>` until `destroy()` returns `{ ok: true }`.

#### 4.2.8 GSI tables, composite sort key, and queries

Each GSI is a standard `FokosDB` table in shard group `fokos.gsiv1.<n>.<tableName>.<indexName>`, where `<indexName>`
is `targetId` (non-empty, no `~`) and `<n>` is `tableName.length` in UTF-16 code units. Prefixing `<tableName>` with
its length makes every `(tableName, indexName)` pair unambiguous even when both contain `.`. `gsiv1` names the version
of this encoding, so a later naming scheme can use a new version (for example `gsiv2`) without a collision with the
names of `gsiv1`. Because the shard group name is the address of the GSI data, the `gsiv1` encoding must never change.
`MaterializationDO` and `db.gsi(indexName)` construct the internal GSI `FokosDB` client with `captureChanges = false`
and no transaction coordinator group.

##### `batchWriteItems` deadline extension

`apiBatchWriteItems` (`docs/agent-plans/2026-10-17-batch-get-and-write-items.md`) adds the optional field
`applyBeforeMs?: number` to `BatchWriteItemsOptions` and `BatchWriteItemsRpcRequest`. In each item's `transactionSync`
block, the partition checks its clock against `applyBeforeMs` and returns `unprocessed` with `write_deadline_passed`
without writing when `clock > applyBeforeMs`.

##### Order-preserving composite sort key with `<L><flag><D>` trailer

Multiple base items can share `(gsiHK, gsiSK)`. By FokosDB's identity invariant, item identity is a byte comparison and
never a hash. At the same time, SQLite's `replace(X, Y, Z)` is a `TEXT` function that truncates `BLOB` values at `0x00`,
and `instr(i.sk, x'0000')` scans every byte from offset 1.

To support both byte-order B-tree range scans and `O(1)` `substr` extraction in SQLite,
`encodeGsiSortKey(gsiSK, baseHK, baseSK)` takes the `KeyBytes` of each component (`gsiSK` is `x''` when absent) and
builds `physicalSk`:

- Escape every `0x00` byte inside `gsiSK`, `baseHK`, and `baseSK` as `0x00 0xFF`.
- Join `escape(gsiSK)`, `escape(baseHK)`, and `escape(baseSK)` with the 2-byte separator `0x00 0x00`.
- Append the trailer `<L><flag><D>`:
  - `<L>` is the decimal ASCII byte length of the unescaped `gsiSK` (`1` to `9` digits; `"0"` when absent).
  - `<flag>` at offset `-2` is `':'` (`0x3A`) when `gsiSK` contains no `0x00` byte, or `'!'` (`0x21`) when `gsiSK`
    contains a `0x00` byte (in which case the unescaped `rawGsiSK` is appended immediately before `<L>!<D>`).
  - `<D>` at offset `-1` is the ASCII digit `'1'..'9'` giving the byte length of `<L>`.

`MaterializationDO` passes `physicalSk` to `batchWriteItems` as a `Uint8Array`, and `KeyCodec.encode` prepends the
binary tag `0xFF` so `i.sk` in SQLite is `0xFF || physicalSk` (byte `n` of `physicalSk` is byte `n + 1` of `i.sk`):

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
- **Uniqueness:** An escaped component never contains `0x00 0x00`, and items with equal `gsiSK` share the same trailer,
  so two distinct base items never produce equal bytes.
- **Contiguous `gsiSK` ranges:** Every entry with `gsiSK = k` starts with `escape(k) || 0x00 0x00`, so entries sort by
  `gsiSK`, then by `baseHK`. Within the same `(gsiSK, baseHK)`, entries sort by the stored bytes of
  `escape(baseSK) || trailer` (DynamoDB likewise leaves the order of identical GSI keys unspecified).

In SQLite, `CAST(substr(i.sk, -2 - D) AS INTEGER)` stops at the non-digit `<flag>` automatically:

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

Measured in SQLite 3.45.1 over 20,000 rows (averaged over 25 runs) for a compiled `{ ref: "sortKey" } = ?` filter:

| Sort key size | Trailer | Direct `i.sk` (base table) | `instr` scan (no trailer) | Two-step `<L><flag><D>` trailer |
| --- | --- | --- | --- | --- |
| **10 B** | `4 B` (`10:2`) | `1.88 ms` | `4.71 ms` (`2.57x`) | **`4.75 ms` (`2.53x`)** |
| **64 B** | `4 B` (`64:2`) | `2.23 ms` | `7.03 ms` (`3.40x`) | **`5.76 ms` (`2.58x`)** |
| **256 B** | `5 B` (`256:3`) | `3.94 ms` | `16.60 ms` (`3.49x`) | **`8.15 ms` (`2.07x`)** |
| **1024 B** | `6 B` (`1024:4`) | `8.13 ms` | `41.44 ms` (`6.25x`) | **`12.37 ms` (`1.52x`)** |
| **5000 B** | `6 B` (`5000:4`) | `20.64 ms` | `167.65 ms` (`7.44x`) | **`30.04 ms` (`1.46x`)** |

##### Query translation and `{ ref: "sortKey" }` compilation

`db.gsi(indexName).queryItems(...)` needs no server GSI schema:

1. **`sortKeyCondition` client translation:** Let `E(k) = escape(KeyCodec.encode(k)) || 0x00 0x00` and
   `succ(E(k)) = escape(KeyCodec.encode(k)) || 0x00 0x01`. After validating each caller bound against the base table's
   `maxSortKeyBytes`, the client translates `sortKeyCondition` into a binary `{ op: "range", lower, upper }` over
   `physicalSk`:

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

2. **`{ ref: "sortKey" }` in GSI `filter` and `projection` expressions:** On a `fokos.gsiv1.` shard group, the
   expression compiler (`packages/fokosdb/src/shared/expression/compiler.ts`) compiles `{ ref: "sortKey" }` as follows:
   - `referencePresent("sortKey")`: `(i.hk IS NOT NULL AND substr(i.sk, 2, 2) != x'0000')`.
   - `referenceType("sortKey")`: `keyType("substr(i.sk, 2)", "i.hk IS NOT NULL AND substr(i.sk, 2, 2) != x'0000'")`
     (`'bytes'` when `substr(i.sk, 2, 1) = x'ff'`, `'text'` when present and not `x'ff'`, else `'missing'`).
   - `referenceValue("sortKey")`: the `<L><flag><D>` `substr` expression above, wrapped in `logicalKeyValue` when in
     `logical` mode.
3. **Response decoding:** The pagination cursor keeps the full `physicalSk` bytes. On unprojected returned items,
   `db.gsi(indexName)` decodes `physicalSk` into `{ sortKey: gsiSK, baseHashKey: baseHK, baseSortKey: baseSK }`.

##### Derived GSI key limits

`MaterializationDO` and `db.gsi(indexName)` configure the internal GSI `FokosDB` client with:
- `maxHashKeyBytes = h` (the base table's `maxHashKeyBytes`).
- `maxSortKeyBytes = 1 + 2 * (2 * s + h) + 4 + s + digits(s) + 2`, where `h` and `s` are the base table's
  `maxHashKeyBytes` and `maxSortKeyBytes`, `1` is the `0xFF` tag, `2 * (2 * s + h)` covers worst-case `0x00` escaping
  of `gsiSK`, `baseHK`, and `baseSK`, `4` covers the two `0x00 0x00` separators, and `s + digits(s) + 2` covers the
  `'!'` trailer. With the default limits (`h = 1024`, `s = 512`), `maxSortKeyBytes` is 4,618 bytes.

#### 4.2.9 Backpressure, circuit breaking, and cost

##### Circuit breaking and four-stage backpressure

When calls to a target fail, `MaterializationDO` opens the circuit breaker for that target, stops freezing new
transitions for it, and sends one probe batch every `probeIntervalMs` (default 5 s) until a probe succeeds. Pressure
propagates in four stages:

1. **Stage 1 (Target down):** The target circuit breaker opens in `MaterializationDO`. Incoming base exports still
   update `latest_items` and coalesce `desired_revision` in place; base writes and healthy targets see zero delay.
2. **Stage 2 (`MaterializationDO` reaches `1.0x maxSizeMb`):** `MaterializationDO` splits into `hashSplitN` children
   (or promotes a hot key) without waiting for the down target to recover, multiplying storage capacity.
3. **Stage 3 (`MaterializationDO` migrating or at split floor and reaches `1.1x maxSizeMb`):** `MaterializationDO`
   rejects live upserts (`deleted = 0`) in `applyLatestImages` with `partition_migrating` or `partition_over_size`,
   while still accepting deletions (`deleted = 1`). Base `PartitionDO` instances retain and coalesce compact
   `materialization_outbox` rows while base writes continue to succeed.
4. **Stage 4 (Base `PartitionDO` reaches `1.1x maxSizeMb`):** Base `PartitionDO` splits, or rejects new writes with
   `partition_size_exceeded` if at the split floor.

Example capacity during a 2-hour outage of 1 of 3 GSIs at 100 base writes/s (720,000 total mutations: 72,000 new items
and 648,000 mutations across 50,000 existing items):
- Base `materialization_outbox` stays near 0 rows with 0 ms added write latency.
- `MaterializationDO` coalesces the 648,000 updates into 50,000 rows (an 83% reduction in GSI work), and any item
  created and deleted during the outage (`applied_revision IS NULL` and `deleted = 1`) converges with 0 RPCs to the
  down GSI. The 122,000 dirty `target_item_state` rows occupy ~15 MB across the shard group.
- When the GSI recovers, the in-memory runner drains the 122,000 to 172,000 `batchWriteItems` operations in 1.5 to
  2.5 minutes at 1,000 to 2,000 items/s across 4 `MaterializationDO` partitions, while healthy targets continue to
  share the round-robin workers.

##### Cost of one base write

Un-coalesced SQLite table rows written per base write for a table with `T` GSIs (worst case, 1 export per write):

| Base write | Today | Base partition | `MaterializationDO` | GSI partitions | Total | `T = 10` |
| --- | --- | --- | --- | --- | --- | --- |
| Put, every GSI key stays | 2 | 2 + 3 | 2 + 3T | 2T | 7 + 5T | 57 |
| Put, every GSI key moves | 2 | 2 + 3 | 2 + 4T | 5T | 7 + 9T | 97 |
| Delete | 3 | 3 + 3 | 4 + 3T | 3T | 10 + 6T | 70 |

- **Base partition:** Put writes `items` and `key_size_estimates` (2); delete also writes `deletion_metadata` (3).
  Capture adds 3 outbox writes (upsert, `attempted = 1` claim, delete on ack).
- **`MaterializationDO`:** Ingestion writes `latest_items` and `key_size_estimates` (2) and `T` target rows. Each GSI
  writes its target row at each freeze and commit (2 when the key stays, 3 when the key moves). A delete writes the
  `DELETE_OLD` freeze and deletes the target row per target, plus `deleted_at_ms` and the later tombstone GC delete on
  `latest_items` and `key_size_estimates`.
- **GSI partitions:** Put writes `items` and `key_size_estimates` (2); delete writes 3 rows.
- **Push target / sparse GSI:** A push target costs 3 rows in `MaterializationDO` and 0 rows in FokosDB. A sparse GSI
  with an absent key costs 1 to 2 rows in `MaterializationDO` and 0 rows in the GSI.

Coalescing `N` base writes of one item before export reduces the cost to `3N` base rows and one downstream export and
push cycle. Secondary index updates can add billed row writes. TODO: measure billed `rowsWritten` in the Workers
runtime. One live item with full-item projections across `T` GSIs stores `2 + T` payload copies (base `items`,
`latest_items`, and `T` GSI rows), plus ~`|gsiHK| + |gsiSK| + 64` bytes per `target_item_state` row.

### 4.3 Open Questions

#### 4.3.1 Circuit breaker scope and admission thresholds
Select whether a circuit breaker scopes to one `target_id` or to one destination GSI partition, and define the exact
size thresholds where `MaterializationDO` stops export acknowledgements and where `PartitionDO` rejects writes.

#### 4.3.2 Capture on a table that has data
Define how a table that already contains items enables `captureChanges`. An item that receives no write after the
policy change has no outbox row and never reaches `latest_items`, and a partition that has not received the new policy
yet writes no outbox row.

#### 4.3.3 `version` and `{ ref: "v" }` on GSI queries
Because each GSI is a standard `PartitionDO` table, `items.v` in a GSI partition counts writes to that
`(gsiHK, physicalSk)` entry rather than the base item's `version` (`revision`). Select whether
`db.gsi(indexName).queryItems` omits `version` on unprojected items and rejects `{ ref: "v" }` in a GSI `filter` or
`projection` (requiring callers who want the base version to project `{ expr: { ref: "v" }, as: "version" }` in
`GsiTargetDefinition.projection`), or exposes the GSI entry's local `items.v`.

### 4.4 Future Follow-ups

These features are deferred out of the core specification and will ship incrementally after Milestones 1–4:

#### 4.4.1 Online target backfill
When a user deploys a new GSI or push target in `MaterializationDO.targets()` on a table that already holds data in
`latest_items`, each `MaterializationDO` leaf (once its `canSweepLocally` guard is true and any parent import is
complete) records `backfill_cursor_item_id = 0` in `materialization_targets` and runs a bounded background scan over
`latest_items` (`WHERE item_id > backfill_cursor_item_id AND deleted = 0`), inserting `'DIRTY'` rows into
`target_item_state` with `ON CONFLICT (item_id, target_id) DO NOTHING`. Because `MaterializationDO` stores a full
mirror in `latest_items.data`, backfilling a new target performs zero reads against base `PartitionDO` partitions.

#### 4.4.2 `FokosDBAutoAdminDO` catalog and activation status
For applications that need a cluster-wide `BACKFILLING | ACTIVE` status check when adding a GSI online, a singleton
`FokosDBAutoAdminDO` (`fokos.admin.<tableName>`) can walk `fokos.m.<tableName>` owners, verify that every initialized
leaf has finished its import (`importState === null || importState === "imported" || importState === "active"`),
completed its `latest_items` backfill scan, and drained its initial `target_item_state` backlog for the new target, and
then report the target as `ACTIVE`.

#### 4.4.3 Per-GSI topology and key-limit overrides
If a workload needs a GSI with a higher `rootTreesN` or different `maxHashKeyBytes` / `maxSortKeyBytes` than the base
table, those per-GSI topology and limit overrides can be passed in `FokosTableOptions` (or `db.gsi(indexName, opts)`)
so both `MaterializationDO` and the caller `FokosDB` client construct identical `FokosRouteContext` topologies for that
GSI shard group.

## 5. Alternative Options

### 5.1 Base outbox + base shadow (`_fokos_change_shadow`) + stateless Queue fan-out

An alternative keeps `_fokos_change_pending (baseKey, version, newImage)` and
`_fokos_change_shadow (baseKey, lastShippedKeyAttrs)` in the base `PartitionDO`, sends batches to a Cloudflare Queue,
and uses stateless queue consumers to compute `oldGsiKey -> newGsiKey` and apply LWW writes and tombstones to the GSIs.
Rejected for five reasons:

1. **Permanent orphan GSI entries under `A -> B -> C` across a crash between `queue.send()` and the shadow update:**
   `await env.QUEUE.send()` and SQLite `transactionSync` cannot commit atomically. If the DO sends `{ old: A, new: B,
   v: 2 }` and crashes before updating `shadow = B`, and a client then writes `v3 (C)`, the next alarm coalesces the
   outbox to `v3 (C)` with `shadow = A` and sends `{ old: A, new: C, v: 3 }`. Both messages delete `A`, and `B@v2`
   remains in the GSI forever. Updating `shadow = B` before `queue.send()` has the mirror defect when `queue.send()`
   fails (`A@v1` remains forever).
2. **`last_write_ts` is not strictly increasing per item:** Two writes within `maxClockSkewMs` can share the same
   `last_write_ts`, breaking timestamp-based LWW.
3. **Cloudflare Queues 128 KB limit vs 400 KB items:** Items above 128 KB cannot fit in a Queue message, and passing a
   pointer fails when the base item is updated or deleted before all consumers read it.
4. **Base partition bloat and coupling:** Storing `newImage` in the base outbox duplicates 400 KB payloads during
   downstream outages, and `lastShippedKeyAttrs` couples the base partition to all 10 GSI key extractors.
5. **Tombstone retention coupled to Queue/DLQ age:** Delayed queue messages force GSI partitions to retain tombstones
   for days.

### 5.2 Separate `GsiMaintainerDO` worker tier

A separate DO tier claiming dirty work from `MaterializationDO` is rejected because `MaterializationDO` already shards
by `(baseHK, baseSK)` and holds `latest_items` and `target_item_state` in local SQLite. Pushing directly from
`MaterializationDO` avoids an extra DO hop and distributed work leases.

### 5.3 Pull cursors (`change_seq`) for remote sinks

Assigning a partition-local `change_seq` for external pull consumers is rejected because pull cursors force consumers
to track partition splits and stitch parent/child cursor epochs, whereas push targets reuse `target_item_state` and
migrate automatically when `MaterializationDO` splits.

### 5.4 1:1 sidecar `MaterializationDO` per base `PartitionDO`

Pairing each base partition `<part>` with a `<part>.mat` sidecar DO is rejected because a base split or promotion
forces the sidecar to split even when materialization state is small, and out-of-order exports from an old parent and a
new child no longer land on the same `MaterializationDO`.

### 5.5 Hashing `(baseHK, baseSK)` or partition-local integer IDs in the GSI sort key

1. **Fixed-size hash suffix `H128(baseHK, baseSK)`:** Rejected because a hash collision between two base items sharing
   `(gsiHK, gsiSK)` overwrites one item on `WRITE_NEW` and deletes the survivor on `DELETE_OLD`.
2. **Partition-local `(baseHK, baseSK) -> id` or reference-counted `KeyBytes -> id` lookup tables:** Rejected because
   they double or triple `rowsRead` and `rowsWritten` on puts, deletes, and queries, make GSI tables diverge from
   standard `PartitionDO` tables, and force split migrations to recompute reference counts across child slices.

## 6. Frequently Asked Questions

### 6.1 Do GSIs and remote data-lake sinks share the same state machine?

Yes. Both are rows in `target_item_state` driven by `MaterializationDO` (Section 4.2.5). A GSI target runs
`WRITE_NEW -> DELETE_OLD -> COMMIT_APPLIED` with `FokosDB.batchWriteItems`, and a remote push target runs
`PUSH_BATCH -> COMMIT_APPLIED` over Workers RPC.

### 6.2 Why does `MaterializationDO` keep a full mirror of live items?

Keeping `latest_items.data` for live items allows `MaterializationDO` to re-evaluate projections on retry and allows
future online targets (Section 4.4.1) to backfill from local SQLite with zero reads to the base `PartitionDO` shard
group.

### 6.3 What happens when an item is created, updated, and deleted while `MaterializationDO` is unreachable?

While `MaterializationDO` is unreachable, the outbox row keeps `unexported_create = 1` and `attempted = 0`. A local
delete removes the `materialization_outbox` row in place and sends zero RPCs (Section 4.2.3).

### 6.4 Does advancing `max_deleted_v` to `before.v + 1` change existing transaction behavior?

No. `transactGetItems` still detects any delete between phase 1 and phase 2, and new rows still start at
`d.max_deleted_v + 1`, above every earlier version of that key (Section 4.2.2). Only the version numbers change: a
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
- `packages/fokosdb/src/server/do-partition.ts`
- [Using Global Secondary Indexes in DynamoDB][dynamodb-gsi]
- [Cloudflare Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Cloudflare Queues limits](https://developers.cloudflare.com/queues/platform/limits/)
- [dynamodb-gsi]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
