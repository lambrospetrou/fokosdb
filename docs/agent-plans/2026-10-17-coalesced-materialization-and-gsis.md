# RFC — Coalesced materialization for latest-image streams, remote sinks, and global secondary indexes

**State:** Draft
**Date:** 2026-10-17
**Author:** Lambros Petrou
**Status:** Nothing is built. This document specifies the architecture for latest-image streams, remote target sinks,
and global secondary indexes (GSIs).

## Table of Contents

1. [Overview and Context](#1-overview-and-context)
2. [Goals and Requirements](#2-goals-and-requirements)
3. [Milestones](#3-milestones)
4. [Proposed Solution](#4-proposed-solution)
5. [Alternative Options](#5-alternative-options)
6. [Frequently Asked Questions](#6-frequently-asked-questions)
7. [References](#7-references)

## 1. Overview and Context

### 1.1 The current system

FokosDB has no global secondary index (GSI), no public change stream, and no connector for remote sinks such as a data
lake. Today, a caller can read items only by the base table `(hashKey, sortKey)` through `getItem`, `transactGetItems`,
and `queryItems`.

Each base partition is a `PartitionDO` in the shard group `fokos.p.<tableName>`. It hosts `FokosShardingRuntime` in
`packages/fokosdb/src/sharding/runtime.ts` and stores its committed rows in SQLite through `PartitionStore` in
`packages/fokosdb/src/shared/partition/partition-store.ts`.

Each item row in `items` has a version `v`. Each partition has one `deletion_metadata` row (`id = 1`) with
`max_delete_tx_order_ts` and `max_deleted_v`. A new item row starts at `v = max_deleted_v + 1`. Each update to an
existing row sets `v = v + 1`. A delete or TTL expiry raises `max_deleted_v` to at least the removed `v`. A split or
key promotion merges `max_deleted_v` onto the target partition with `MAX` in `FokosMigrationHost`
(`packages/fokosdb/src/shared/partition/fokos-migration-host.ts`).

### 1.2 The problems

1. **Secondary access patterns need up to 10 GSIs per table.** Each GSI extracts a different partition key, an optional
   sort key, and a projection from the base item.
2. **External systems need a latest-image stream.** A remote target, such as an object-store data lake or an HTTP sink,
   must receive the newest state of each changed item without coupling base write availability to the remote target.
3. **A GSI needs the previous GSI key to move an entry.** When an item changes its GSI key from `A` to `C`, the GSI
   maintainer must delete `A` and write `C`. The new item image alone does not contain `A`.
4. **Base items move across `PartitionDO` instances.** A hash split, a key promotion, or a range split moves an item to
   a new `PartitionDO`. An old source partition can hold an unexported revision while the new owner exports a newer
   revision. Downstream consumers must not depend on delivery order between physical base partitions.
5. **Downstream outages must not grow base partitions without bound.** A GSI partition or a remote data lake can be
   slow or unavailable for hours. The base write path must stay a single local SQLite transaction, and base storage
   must not duplicate 400 KB item payloads for every mutation or every index.

### 1.3 Facts that constrain the design

- **An item is up to 400 KB** (`MAX_ITEM_BYTES` in `packages/fokosdb/src/shared/transaction-limits.ts`). Workers RPC
  supports payloads up to 32 MiB. Cloudflare Queues caps a message at 128 KB, so a queue message cannot carry a full
  FokosDB item.
- **`last_write_ts` is not strictly increasing per item.** `PartitionStore.upsertItem` sets
  `last_write_ts = MAX(last_write_ts, txOrderTs)`. A two-phase transaction stamps the coordinator clock, which
  `prepare` accepts up to `maxClockSkewMs` ahead. A later non-transactional write within that skew window keeps the
  same `last_write_ts`. Therefore, `last_write_ts` cannot order last-writer-wins (LWW) updates across retries.
- **`v` is monotonic per item across updates and recreates, except on delete.** Today, creating an item at `v = 5` and
  deleting it sets `max_deleted_v = 5`. The live row and the deletion share `5`. Raising `max_deleted_v` to
  `before.v + 1` on delete gives the deletion its own strictly higher revision `6`, and the next create starts at `7`.
- **A `WITHOUT ROWID` table bloats large payloads in Durable Object SQLite.** On a 4 KiB SQLite page, a `WITHOUT ROWID`
  table puts any row above ~1002 bytes onto a private overflow page (measured at 3.1x to 4.7x storage amplification in
  `PartitionStore`). Any table that stores item payloads must be a rowid table with the payload column last.
- **Promotion cleanup deletes source `items` rows.** In `PartitionDO.hooks().cleanupSourceStep`, a key promotion
  deletes the source `items` rows for the promoted hash key after import completes. The export path must not rely on
  reading `items` from a source partition after `cleanupSourceStep` removes them.
- **The TTL sweep deletes unowned copies.** `PartitionStore.deleteExpiredItems` removes expired rows without checking
  `this.fokos.owns(key)`. Only the current owner of a key can emit a deletion marker for a TTL expiry.

### 1.4 Glossary

| Term | Meaning |
| --- | --- |
| Base item | One item in the user table, identified by `(baseHashKey, baseSortKey)`. |
| Base revision | The strictly increasing integer revision of one base item across writes, deletes, and splits. |
| Export outbox | The coalesced `materialization_outbox` row in a base `PartitionDO` for one dirty base item. |
| Materialization partition | One `MaterializationDO` in the shard group `fokos.m.<tableName>`. |
| Latest image | The newest live image or deletion tombstone that `MaterializationDO` accepted for a base item. |
| Applied state | The GSI key `(applied_index_hk, applied_index_sk)` and revision last completed for one GSI. |
| In-flight target | The immutable GSI key and revision that `MaterializationDO` is currently writing or deleting. |
| Desired state | The newest revision that a GSI must converge to for one base item. |
| GSI tombstone | A hidden row in a GSI partition that rejects a late write with an older revision. |
| Remote sink | An external consumer that reads coalesced changes from `MaterializationDO` using `change_seq`. |

## 2. Goals and Requirements

### 2.1 In scope

- Support up to 10 GSIs per table, where each GSI defines its own partition key, optional sort key, and attribute
  projection.
- Support latest-image subscriptions and remote target sinks (such as a data lake) from the same materialization tier.
- Capture one coalesced export marker in the base `PartitionDO` in the same SQLite transaction as each item mutation.
- Coalesce `create -> update -> delete` in the base outbox into zero outbound RPCs when no export attempt has started.
- Route materialization state by `(baseHashKey, baseSortKey)` in an independent shard group `fokos.m.<tableName>`.
- Enforce monotonic base revisions across hash splits, key promotions, and range splits using `v` and `max_deleted_v`.
- Isolate failures across GSIs and remote sinks so one unavailable target never stalls healthy GSIs or base writes.
- Store the unconsumed item payload at most once in `MaterializationDO`, and reclaim it in compact mode after all GSIs
  and registered sinks advance past its revision.

### 2.2 Out of scope

- Full change data capture (CDC) history of every intermediate version. Intermediate writes before consumption coalesce
  into the latest image.
- Strongly consistent GSI reads or atomic multi-item visibility across a GSI.
- Uniqueness constraints on a GSI key. Callers that need a unique secondary key must use `transactWriteItems` on the
  base table.
- Global ordering of changes across different base items.

### 2.3 Requirements

1. The base partition must write the item change and the `materialization_outbox` row in one `transactionSync`.
2. The `materialization_outbox` row must store only key and revision metadata, never a copy of the item payload.
3. A transaction `prepare`, a failed condition, a `check`, or a `cancel` must not write a `materialization_outbox` row.
4. A delete of an absent item must not write a `materialization_outbox` row.
5. A TTL expiry must write a deletion marker only when `this.fokos.owns(key)` is true on that partition.
6. A migration page import must not emit a new mutation revision; it copies the existing revision state.
7. A delete of a present item at `v` must assign deletion revision `v + 1` and raise `max_deleted_v` to at least
   `v + 1`.
8. `MaterializationDO` must accept an export only when `incoming.revision > stored.revision` (or idempotently ack when
   equal).
9. The base partition must delete a `materialization_outbox` row on ack only when `stored.revision == acked.revision`.
10. When an item with `unexported_create = 1` and `attempted = 0` is deleted in the base partition, the base partition
    must delete the `materialization_outbox` row in place and send no RPC to `MaterializationDO`.
11. `MaterializationDO` must write `inflight_index_hk`, `inflight_index_sk`, `inflight_revision`, and `inflight_phase`
    to SQLite before it sends a GSI write or delete RPC.
12. A new latest image in `MaterializationDO` must update `latest_items` and `desired_revision` without overwriting an
    active `inflight_*` transition.
13. Each GSI operation must carry `(baseHashKey, baseSortKey, revision)`. A GSI partition must reject an operation
    whose revision is lower than the stored entry or tombstone revision.
14. A failed GSI operation or a slow remote sink must not fail `MaterializationDO` ingestion or block base outbox
    acknowledgements.
15. A `MaterializationDO` split must move the `latest_items` row and all `gsi_item_state` rows for a base item in one
    atomic slice.
16. Every background scan, export batch, GSI step, and stream read must enforce item count, byte count, and duration
    bounds.

## 3. Milestones

### Milestone 1: Monotonic deletion revision and base `materialization_outbox`
- Update `PartitionStore.deleteItem` and `PartitionStore.deleteExpiredItems` so removing a row at `v` raises
  `max_deleted_v` to `MAX(max_deleted_v, v + 1)` and returns `deleteRevision = v + 1`.
- Add `materialization_outbox` to `PartitionStore` and wire it into `upsertItem`, `updateItemSingleShot`,
  `deleteItem`, `TransactionParticipant.commitLocal`, `TransactionParticipant.executeSingleShot`, and owned rows in
  `deleteExpiredItems`.
- Migrate `materialization_outbox` rows in `FokosMigrationHost` and clean up unowned copies in `cleanupSourceStep`.

### Milestone 2: `MaterializationDO` shard group and base export job
- Create `MaterializationDO` hosting `FokosShardingRuntime` in shard group `fokos.m.<tableName>`.
- Build `latest_items` storage, `applyLatestImages` (`group` dispatch), and the base `PartitionDO` export job with
  compare-and-delete acknowledgements.
- Build `MaterializationDO` split and promotion migration for `latest_items`.

### Milestone 3: Latest-image stream and remote sink cursors
- Build `readStreamPage` on `MaterializationDO` over `latest_items_by_change (change_seq, base_hk, base_sk)`.
- Add cursor registration and compact-mode payload reclamation (`data = NULL` after all targets pass `revision`).

### Milestone 4: GSI shard groups and the GSI maintenance state machine
- Add `gsi_item_state` to `MaterializationDO` and the GSI entry/tombstone store methods on `PartitionStore`.
- Build the `WRITE_NEW -> DELETE_OLD -> COMMIT_APPLIED` state machine, per-index circuit breakers, AIMD recovery
  budgets, and tombstone TTL cleanup.
- Wire `queryItems` on a GSI shard group `fokos.gsi.<tableName>.<indexName>` into `FokosDB`.

### Milestone 5: Online GSI backfill
- Build the leaf-by-leaf base `items` scan that pushes current rows at version `v` to `MaterializationDO` and marks the
  index `ACTIVE` when all leaves complete.

## 4. Proposed Solution

### 4.1 High-level overview

A table with GSIs or remote streams uses independent `FokosShardingRuntime` shard groups connected by
`FokosShardingClient`:

```text
Base Shard Group: fokos.p.<tableName> (PartitionDO)
  - Stores committed items + 32-byte materialization_outbox markers
  - Routes exports by (baseHK, baseSK)
          |
          | 1 coalesced export RPC per dirty item
          v
Materialization Shard Group: fokos.m.<tableName> (MaterializationDO)
  - Routed by (baseHK, baseSK)
  - Stores 1 latest_items row per dirty item (single payload copy)
  - Stores 1 compact gsi_item_state metadata row per (index_id, baseHK, baseSK)
          |
          +----------------------------------------+
          |                                        |
          v                                        v
GSI Shard Groups (up to 10):             Remote Sinks (Data Lake / HTTP / R2):
  fokos.gsi.<tableName>.<indexName>        Pull or push by (partitionId, change_seq)
  - PartitionDO routed by                  - Watermark interval scans on latest_items
    (gsiHK, tuple(gsiSK, baseHK, baseSK))  - Automatic coalescing while sink is down
```

1. **Base write (`PartitionDO`):** Every write, delete, transaction commit, or owned TTL expiry updates `items` and
   upserts a 32-byte marker into `materialization_outbox` in one SQLite transaction. The live item payload stays in
   `items` and is not copied into the outbox.
2. **Export (`PartitionDO -> MaterializationDO`):** A background job in `PartitionDO` reads dirty outbox rows that this
   partition owns, reads the current item from `items`, and sends a batched `applyLatestImages` RPC to
   `fokos.m.<table>`. `MaterializationDO` stores the latest image, marks the affected GSIs dirty, and immediately
   acknowledges the base partition.
3. **GSI maintenance (`MaterializationDO -> GSI PartitionDO`):** For each GSI, `MaterializationDO` extracts the new GSI
   key and projection from `latest_items.data` and compares the new key with `applied_index_hk/sk`. It freezes the
   transition in `gsi_item_state`, writes the new GSI entry, writes a versioned tombstone at the old GSI entry when the
   key moved, and updates `applied_index_hk/sk`.
4. **Remote sinks (`MaterializationDO -> Remote Target`):** Each accepted update in `MaterializationDO` gets a local
   monotonic `change_seq`. A remote sink holds one cursor `(partitionId, change_seq)` per `MaterializationDO` and reads
   bounded intervals `cursor < change_seq <= H`.

### 4.2 How the shard group topologies interconnect

`MaterializationDO` is **not** a 1:1 sidecar attached to a single base `PartitionDO`. It is a separate shard group
(`fokos.m.<tableName>`) that routes keys by `(baseHashKey, baseSortKey)` through its own `FokosShardingRuntime` tree.

A table `orders` with 3 GSIs runs these shard groups:

| Shard group | DO class | Route key `(hk, sk)` | Split trigger |
| --- | --- | --- | --- |
| `fokos.p.orders` | `PartitionDO` | `(baseHK, baseSK)` | Base `items` + `pending_transactions` bytes |
| `fokos.tc.orders` | `TransactionCoordinatorDO` | `(clientRequestToken, "")` | Coordinator ledger bytes |
| `fokos.m.orders` | `MaterializationDO` | `(baseHK, baseSK)` | `latest_items` + `gsi_item_state` bytes |
| `fokos.gsi.orders.g1` | `PartitionDO` | `(g1HK, tuple(g1SK, baseHK, baseSK))` | GSI-1 entry bytes and GSI-1 key skew |
| `fokos.gsi.orders.g2` | `PartitionDO` | `(g2HK, tuple(g2SK, baseHK, baseSK))` | GSI-2 entry bytes and GSI-2 key skew |
| `fokos.gsi.orders.g3` | `PartitionDO` | `(g3HK, tuple(g3SK, baseHK, baseSK))` | GSI-3 entry bytes and GSI-3 key skew |

#### 4.2.1 Base partition split or key promotion

Because `fokos.m.orders` routes by `(baseHK, baseSK)` and not by the base partition ID, a split or promotion in
`fokos.p.orders` does not change which `MaterializationDO` owns `(baseHK, baseSK)`:

```text
Base Table (fokos.p.orders)                          Materialization (fokos.m.orders)
---------------------------                          --------------------------------
Before base split:
  fokos.p.orders~h.0
    (hk="u1", sk="o9", rev=5) ---------------------> fokos.m.orders~h.0
                                                       owns (hk="u1", sk="o9")
After base split and promotion:
  fokos.p.orders~h.0 (router) ---------------------> fokos.m.orders~h.0
  fokos.p.orders~r.dTE.. (promoted range root)         still owns (hk="u1", sk="o9")
    (hk="u1", sk="o9", rev=6) --------------------->   accepts rev=6, rejects late rev=5
```

If an old base partition sends `rev = 5` after the promoted range root has already sent `rev = 6`, both RPCs arrive at
the same owner in `fokos.m.orders`. `MaterializationDO` compares `5 < 6` in one SQLite transaction and acknowledges
`rev = 5` as stale without changing state.

#### 4.2.2 `MaterializationDO` split or key promotion

Each base `PartitionDO` holds a `FokosShardingClient` for `fokos.m.<tableName>`. When `fokos.m.orders~h.0` grows past
its `maxSizeMb`, its `FokosShardingRuntime` splits it into `fokos.m.orders~h.0.0` and `~h.0.1` (or promotes a hot base
hash key). Its migration host moves the `latest_items` row and all `gsi_item_state` rows for each key together.

When a base `PartitionDO` sends an export to the old parent `fokos.m.orders~h.0`, the runtime forwards the call to the
child and returns `routing` evidence in `FokosEnvelope`. The base `PartitionDO`'s `FokosShardingClient` caches the
learned route and sends subsequent exports directly to the child.

#### 4.2.3 GSI partition split or key promotion

Each `MaterializationDO` holds one `FokosShardingClient` per GSI shard group (`fokos.gsi.<tableName>.<indexName>`).
When a GSI has a low-cardinality hash key (for example, `status = "OPEN"` shared by 500,000 items), the GSI shard
group promotes `"OPEN"` to a range tree and splits it by `(gsiSK, baseHK, baseSK)`. Neither `fokos.p.<tableName>` nor
`fokos.m.<tableName>` splits for `"OPEN"`, because both are sharded by `(baseHK, baseSK)`.

### 4.3 Monotonic base revisions using `v` and `max_deleted_v`

Every item mutation needs a strictly increasing integer revision per `(baseHK, baseSK)` across creates, updates,
deletes, TTL expiries, and repartitions.

FokosDB already maintains `v` on each row in `items` and `max_deleted_v` in `deletion_metadata`:

- A new row starts at `v = d.max_deleted_v + 1`.
- An update to an existing row sets `v = v + 1`.
- `FokosMigrationHost` already copies `maxDeletedV` in `deletionMetadata` and merges it with `MAX` on the target
  partition before the target serves writes.

Only one change is needed: a delete must advance the revision above the live row it removes.

1. In `PartitionStore.deleteItem`, when `before` exists at version `before.v`, the deletion revision is
   `deleteRevision = before.v + 1`. `deleteItem` calls `#raiseDeletionMetadata(txOrderTs, before.v + 1)`.
2. In `PartitionStore.deleteExpiredItems`, each expired row at `row.v` gets `deleteRevision = row.v + 1`, and the batch
   raises `max_deleted_v` to `MAX(max_deleted_v, row.v + 1)`.
3. The next `upsertItem` for that key inserts at `d.max_deleted_v + 1`, which is at least `before.v + 2`.

```text
Create X     -> v = 1 (revision 1, deleted = 0)
Update X     -> v = 2 (revision 2, deleted = 0)
Delete X     -> max_deleted_v = 3 (revision 3, deleted = 1)
Recreate X   -> v = 4 (revision 4, deleted = 0)
```

With this rule, `v` (and `before.v + 1` on delete) is strictly increasing for every key across all partition splits and
promotions, with no separate mutation clock and no change to `FokosShardingRuntime` cutover records.

### 4.4 Base mutation capture and smart outbox coalescing

`PartitionStore` adds the `materialization_outbox` table:

```sql
CREATE TABLE IF NOT EXISTS materialization_outbox (
    hk                BLOB    NOT NULL,
    sk                BLOB    NOT NULL DEFAULT x'',
    revision          INTEGER NOT NULL,
    deleted           INTEGER NOT NULL,
    unexported_create INTEGER NOT NULL DEFAULT 0,
    attempted         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (hk, sk)
) WITHOUT ROWID, STRICT;
```

`WITHOUT ROWID` is appropriate here because every row is ~36 bytes and has no large payload column.

#### 4.4.1 Coalescing rules in the base partition

Every content mutation updates `materialization_outbox` inside the same `transactionSync` as `items`:

1. **Insert of a new item** (no row existed in `items`):
   - If no row exists in `materialization_outbox`, insert
     `(hk, sk, revision = newV, deleted = 0, unexported_create = 1, attempted = 0)`.
   - If a row already exists in `materialization_outbox` (for example, a previous delete is still unacknowledged),
     update it to `revision = newV, deleted = 0`, and keep `unexported_create = 0`.
2. **Update of an existing item:**
   - Upsert `(hk, sk, revision = newV, deleted = 0)`. Preserve `unexported_create` and `attempted` if the row already
     exists; otherwise set both to `0`.
3. **Exporter claims a batch to send to `MaterializationDO`:**
   - Before the exporter sends an outbound RPC for a batch of keys, it sets `attempted = 1` on those rows in SQLite.
   - When the circuit breaker to `MaterializationDO` is open, the exporter does not claim rows, so `attempted` stays
     `0`.
4. **Delete of an existing item (or owned TTL expiry):**
   - If the `materialization_outbox` row for `(hk, sk)` has `unexported_create = 1 AND attempted = 0`, no byte for this
     item has ever left the base partition. `PartitionStore` runs
     `DELETE FROM materialization_outbox WHERE hk = ? AND sk = ?`. Zero rows remain in the outbox and zero RPCs are
     sent to `MaterializationDO`.
   - Otherwise, `PartitionStore` upserts
     `(hk, sk, revision = deleteRevision, deleted = 1, unexported_create = 0)`.

The `attempted` guard prevents a lost delete: if the exporter sent `rev 10` (`create`) and the RPC timed out while
`MaterializationDO` committed `rev 10`, `attempted` is `1`, so a subsequent local delete at `rev 11` sends the
tombstone `rev 11` instead of dropping the outbox row.

#### 4.4.2 Exporting from the base partition

For each owned row in `materialization_outbox`:

1. For `deleted = 0`, the exporter reads the current item from `items` in the same synchronous snapshot as the outbox
   row and checks that `items.v == outbox.revision`.
2. For `deleted = 1`, the exporter sends a tombstone with no item payload.
3. Upon receiving the acknowledgement for `(hk, sk, ackedRevision)`, the exporter runs:

```sql
DELETE FROM materialization_outbox
 WHERE hk = ?1 AND sk = ?2 AND revision = ?3;
```

If a newer write (`revision = 12`) arrived while `revision = 11` was in flight, the `DELETE` for `revision = 11`
matches zero rows and leaves `revision = 12` in the outbox.

#### 4.4.3 Outbox behavior during base splits and promotions

1. `FokosMigrationHost` adds a third migration stream `materialization_outbox` after `items` and `pending_tx`. It
   copies `materialization_outbox` rows in the slice to the child with `attempted = 1`, because the source can have
   started an export before cutover.
2. The base export job checks `this.fokos.owns({ hashKey: row.hk, sortKey: row.sk })` and skips unowned copies.
3. After migration reaches `completed`, `cleanupSourceStep` deletes the migrated `materialization_outbox` copies
   before `deleteItemsBatchForHashKey` removes promoted `items` rows.

### 4.5 `MaterializationDO` storage and single-payload rule

`MaterializationDO` stores two tables in SQLite.

First, `latest_items` is a **rowid table** (never `WITHOUT ROWID`) with `data` as the last column:

```sql
CREATE TABLE IF NOT EXISTS latest_items (
    item_id         INTEGER PRIMARY KEY,
    base_hk         BLOB    NOT NULL,
    base_sk         BLOB    NOT NULL DEFAULT x'',
    revision        INTEGER NOT NULL,
    change_seq      INTEGER NOT NULL,
    deleted         INTEGER NOT NULL,
    data_kind       INTEGER,
    ttl_at          INTEGER,
    est_row_bytes   INTEGER NOT NULL DEFAULT 0,
    data            ANY,
    UNIQUE (base_hk, base_sk)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_latest_items_change
    ON latest_items (change_seq, base_hk, base_sk);
```

Second, `gsi_item_state` stores **only compact key and revision metadata** per `(index_id, base_hk, base_sk)`. It never
stores a copy of the projected item payload:

```sql
CREATE TABLE IF NOT EXISTS gsi_item_state (
    index_id           TEXT    NOT NULL,
    base_hk            BLOB    NOT NULL,
    base_sk            BLOB    NOT NULL DEFAULT x'',

    applied_revision   INTEGER,
    applied_index_hk   BLOB,
    applied_index_sk   BLOB,

    desired_revision   INTEGER NOT NULL,

    inflight_revision  INTEGER,
    inflight_index_hk  BLOB,
    inflight_index_sk  BLOB,
    inflight_phase     TEXT,

    status             TEXT    NOT NULL, -- 'IDLE' | 'DIRTY' | 'IN_FLIGHT' | 'VIOLATION'
    next_attempt_at    INTEGER,
    last_error_code    TEXT,
    violation_revision INTEGER,

    PRIMARY KEY (index_id, base_hk, base_sk)
) WITHOUT ROWID, STRICT;

CREATE INDEX IF NOT EXISTS idx_gsi_item_state_due
    ON gsi_item_state (index_id, status, next_attempt_at)
    WHERE status IN ('DIRTY', 'IN_FLIGHT');
```

Why `gsi_item_state` omits `desired_projection` and `inflight_projection`:
- With 10 GSIs, storing projections in `gsi_item_state` writes up to 10 extra payload blobs on every ingestion.
- Instead, `latest_items.data` holds the single copy of the latest item image while any GSI has
  `applied_revision < revision` or any remote sink has `cursor_seq < change_seq`.
- When the GSI maintainer runs `WRITE_NEW` for `index_id`, it always projects and writes the **newest**
  `desired_revision` from `latest_items.data`.
- Once all registered GSIs have `applied_revision == revision` and all registered remote sinks have
  `cursor_seq >= change_seq`, `MaterializationDO` sets `latest_items.data = NULL` and `est_row_bytes = 0` in compact
  mode. Steady-state storage in `MaterializationDO` drops to ~50 bytes per item plus ~40 bytes per GSI entry.

### 4.6 GSI maintenance state machine

When `MaterializationDO` accepts a newer `revision` for `(base_hk, base_sk)`:

1. It upserts `latest_items` with `revision`, a new `change_seq`, and `data` (or `deleted = 1, data = NULL`).
2. For each registered GSI `index_id`, it upserts `gsi_item_state`:
   - `desired_revision = revision`.
   - If `status` is `'IDLE'` or `'VIOLATION'`, set `status = 'DIRTY'` and `next_attempt_at = now`.
   - If `status` is `'IN_FLIGHT'`, leave `inflight_*` untouched so the active transition finishes its cleanup first.
3. It commits the SQLite transaction and returns the acknowledgement to the base `PartitionDO`.

#### 4.6.1 Transition phases per `(index_id, base_hk, base_sk)`

When the scheduler claims a row for `index_id`:

1. **Derive desired target from `latest_items`:**
   - If `latest_items.deleted == 1` or the item lacks the GSI partition key attribute (sparse index), the desired target
     is `absent` at `desired_revision`.
   - Otherwise, evaluate the GSI key and projection expressions against `latest_items.data` to get
     `(target_hk, target_sk, projected_data)` at `desired_revision`.
2. **Short-circuit if no GSI key exists and desired is absent:**
   - If `applied_index_hk IS NULL`, `inflight_index_hk IS NULL`, and the desired target is `absent`, set
     `applied_revision = desired_revision, status = 'IDLE'` in SQLite with **zero GSI RPCs**.
3. **Freeze `WRITE_NEW` (when desired target is present):**
   - Write `inflight_revision = desired_revision`, `inflight_index_hk = target_hk`, `inflight_index_sk = target_sk`,
     `inflight_phase = 'WRITE_NEW'`, `status = 'IN_FLIGHT'` to SQLite **before** sending the RPC.
   - Send `gsiPutEntry(target_hk, target_sk, base_hk, base_sk, inflight_revision, projected_data)` to the GSI shard
     group.
4. **Advance to `DELETE_OLD` (when an older key at a different coordinate exists):**
   - If `applied_index_hk IS NOT NULL` and `(applied_index_hk, applied_index_sk)` differs from
     `(inflight_index_hk, inflight_index_sk)`, set `inflight_phase = 'DELETE_OLD'` in SQLite and send
     `gsiDeleteEntry(applied_index_hk, applied_index_sk, base_hk, base_sk, inflight_revision)` to the GSI shard group.
5. **Commit applied state (`COMMIT_APPLIED`):**
   - Set `applied_revision = inflight_revision`, `applied_index_hk = inflight_index_hk`,
     `applied_index_sk = inflight_index_sk`, and clear `inflight_*`.
   - If a newer base write arrived during the RPCs (`desired_revision > applied_revision`), keep `status = 'DIRTY'` and
     `next_attempt_at = now`; otherwise set `status = 'IDLE'` and reclaim `latest_items.data` if all other targets have
     also caught up.

#### 4.6.2 Why `A -> B -> C` never leaks an orphan GSI entry

Suppose an item moves from GSI key `A` (`rev 1`) to `B` (`rev 2`) and then to `C` (`rev 3`):

- **Case 1 (`rev 3` arrives before `rev 2` starts `WRITE_NEW`):** `desired_revision` becomes `3`. `MaterializationDO`
  never sends `B` to the GSI; it transitions directly from `A` to `C` (`WRITE_NEW(C, rev 3)` then
  `DELETE_OLD(A, rev 3)`).
- **Case 2 (`rev 3` arrives while `WRITE_NEW(B, rev 2)` is in flight):** `inflight_index_hk/sk` is already frozen at
  `B` in SQLite. The maintainer finishes `WRITE_NEW(B, rev 2)` and `DELETE_OLD(A, rev 2)`, sets `applied = B (rev 2)`,
  sees `desired_revision = 3 > 2`, and immediately runs `B -> C` (`WRITE_NEW(C, rev 3)` then `DELETE_OLD(B, rev 3)`).
- In every interleaving and crash recovery, any key `B` that is sent over the network is durably recorded in
  `inflight_index_hk/sk` before the RPC starts, so `B` is guaranteed to become `applied_index_hk/sk` and be deleted by
  the next transition.

### 4.7 GSI partition storage and tombstone fences

Each GSI is a `PartitionDO` shard group `fokos.gsi.<tableName>.<indexName>`. The physical key stored in the GSI
partition is:

- `hk`: `KeyBytes` of the GSI partition key attribute.
- `sk`: `KeyCodec.encodeTuple([gsiSortKeyBytes, baseHashKey, baseSortKey])`.

Each GSI row stores `v` as the `baseRevision` and a `tombstone_until_ms` column (`NULL` for a live entry, integer epoch
milliseconds for a tombstone):

1. **`gsiPutEntry(hk, sk, baseRevision, data)`:**
   - Applies only when no row exists for `(hk, sk)` or `incoming.baseRevision >= stored.v`.
   - When `incoming.baseRevision < stored.v` (for example, a timed-out `WRITE_NEW(A, rev 10)` arriving after
     `DELETE_OLD(A, rev 20)` wrote a tombstone at `v = 20`), the GSI partition returns `{ applied: false, stale: true }`
     and changes nothing.
2. **`gsiDeleteEntry(hk, sk, baseRevision)`:**
   - Upserts a tombstone row at `(hk, sk)` with `v = MAX(stored.v, baseRevision)`, `data = x''`, and
     `tombstone_until_ms = now + gsiTombstoneTtlMs` (default 5 minutes).
   - Because `MaterializationDO` is the single sequential driver for `(index_id, base_hk, base_sk)`, the only way an
     older `gsiPutEntry` can arrive after `gsiDeleteEntry` is a single timed-out DO-to-DO RPC still on the wire. A
     5-minute tombstone horizon exceeds the maximum Workers RPC lifetime by an order of magnitude.
3. **Queries and TTL sweep:**
   - `queryItems` on a GSI filters `WHERE tombstone_until_ms IS NULL` and strips the `(baseHashKey, baseSortKey)`
     suffix from `sk` when returning the public sort key (or exposes `baseHashKey` and `baseSortKey` as item keys).
   - The existing `TtlExpiry` sweep removes expired GSI tombstones where `tombstone_until_ms <= now`.

### 4.8 Remote sinks and latest-image subscriptions

A remote sink (such as an R2 / S3 Iceberg or Parquet data-lake writer, a ClickHouse loader, or a webhook worker) reads
coalesced changes from `MaterializationDO` without per-item state rows.

Each `MaterializationDO` stores registered stream consumers in a small table:

```sql
CREATE TABLE IF NOT EXISTS stream_subscriptions (
    subscription_id TEXT    NOT NULL PRIMARY KEY,
    cursor_seq      INTEGER NOT NULL DEFAULT 0,
    updated_at      INTEGER NOT NULL,
    expires_at      INTEGER NOT NULL
) WITHOUT ROWID, STRICT;
```

To read or push a page for `subscription_id`:

1. Read the partition high watermark `H = MAX(change_seq)` from `latest_items`.
2. Scan `latest_items INDEXED BY idx_latest_items_change` where `change_seq > cursor_seq AND change_seq <= H` up to
   the page item and byte budgets (`MAX_RESPONSE_BYTES_PER_PAGE`).
3. Deliver the batch `{ baseHashKey, baseSortKey, revision, changeSeq, deleted, data }` to the remote target.
4. Once the remote target confirms durability (for example, after writing the Parquet/JSONL batch to R2), call
   `ackStreamCursor(subscriptionId, lastChangeSeq)` on `MaterializationDO`.
5. If an item changes 500 times while the remote target is down, each update moves that item's `change_seq` forward in
   `latest_items`. When the remote target recovers, its scan sees only the final state at the newest `change_seq`.

### 4.9 Backpressure and outage walkthrough (3 GSIs, 1 down for 2 hours)

Consider a table with 3 GSIs (`GSI-1` healthy, `GSI-2` healthy, `GSI-3` completely down for 2 hours = 7,200 seconds)
under this workload:

- **Base write rate:** 100 writes/s = **720,000 total mutations** over 2 hours.
- **New items (10%):** 10 writes/s = **72,000 distinct new items**.
- **Updates and deletes (90%):** 90 writes/s = **648,000 mutations** across a working set of `U = 50,000` existing
  items (~13 mutations per item).
- **Total distinct dirty items accumulated for `GSI-3`:** `D = 72,000 + 50,000 = 122,000` distinct items.

#### 4.9.1 Tier 1: Base `PartitionDO` (Zero backpressure)

1. The acknowledgement boundary is between `PartitionDO` and `MaterializationDO`, not between `MaterializationDO` and
   `GSI-3`.
2. For all 720,000 writes, `MaterializationDO` commits `latest_items` and `gsi_item_state` locally and acknowledges the
   base `PartitionDO` in a single hop.
3. `materialization_outbox` in each base `PartitionDO` stays near **0 rows** for all 2 hours. Base writes see **0 ms**
   of added latency and **0 rejections**.

#### 4.9.2 Tier 2: `MaterializationDO` (Coalescing and circuit breaking)

1. **`GSI-1` and `GSI-2` isolation:** The scheduler queries `idx_gsi_item_state_due` per `index_id`. `GSI-1` and
   `GSI-2` process their dirty rows in milliseconds for the entire 2 hours.
2. **`GSI-3` circuit breaker:** When the first RPCs to `GSI-3` fail, `MaterializationDO` opens the circuit breaker for
   `index_id = 'gsi_3'`. While the circuit is open:
   - `MaterializationDO` stops freezing new `inflight_*` targets for `GSI-3`.
   - Incoming writes update only `latest_items` and `gsi_item_state.desired_revision` (`status = 'DIRTY'`).
   - Every `probeIntervalMs` (default 5 s), `MaterializationDO` sends a single probe item to `GSI-3`.
3. **Coalescing math:**
   - The 648,000 updates and deletes on the 50,000 existing items collapse in place into **50,000 rows** (removing
     598,000 intermediate GSI transitions, an **83% reduction** in total work).
   - Any item created and deleted within the 2 hours (where `applied_revision IS NULL` and `latest_items.deleted == 1`)
     converges locally in `MaterializationDO` with **0 RPCs** to `GSI-3`.
4. **Storage math in `MaterializationDO`:**
   - Because `GSI-3` still needs `latest_items.data` for the 122,000 dirty items, `MaterializationDO` retains those
     122,000 payloads.
   - At 2 KB average item size, `122,000 * 2 KB = 244 MB` across the `fokos.m.<table>` shard group (~61 MB per root
     across 4 roots).
   - At 10 KB average item size, `122,000 * 10 KB = 1.22 GB` across the shard group (~305 MB per root across 4 roots).

#### 4.9.3 Tier 3: Multi-stage pressure propagation if the outage continues

If the outage lasts long enough for a `MaterializationDO` partition to reach `hashSplitConditions.maxSizeMb`, pressure
propagates in four stages:

```text
Stage 1: GSI-3 down
  -> GSI-3 circuit breaker opens in MaterializationDO
  -> MaterializationDO buffers 1 latest image per distinct dirty item (D)
  -> Base writes, GSI-1, and GSI-2 are 100% unaffected

Stage 2: MaterializationDO reaches 1.0x maxSizeMb
  -> MaterializationDO splits into hashSplitN children (migrating latest_items + gsi_item_state)
  -> The split does NOT wait for GSI-3 to recover
  -> Storage capacity scales out by hashSplitN (e.g., 4x) automatically

Stage 3: MaterializationDO is migrating or at split floor and reaches 1.1x maxSizeMb
  -> MaterializationDO.admit rejects applyLatestImages with partition_migrating / partition_size_exceeded
  -> Base PartitionDO retains 32-byte materialization_outbox markers (122,000 items = ~4.4 MB total)
  -> Base PartitionDO coalesces create -> update -> delete in place (section 4.4.1)
  -> Base writes STILL succeed

Stage 4: Base PartitionDO itself reaches 1.1x maxSizeMb
  -> Base PartitionDO splits, or rejects new base writes with partition_size_exceeded if at floor
```

#### 4.9.4 Tier 4: Recovery when `GSI-3` returns at `t = 2 hours`

When the periodic probe to `GSI-3` succeeds at `t = 2h`:

1. **Per-index step budgets:** Each scheduler pass on `MaterializationDO` gives `GSI-3` a bounded step
   (`maxItemsPerStep = 50`, `maxBytesPerStep = 1 MiB`, `stepBudgetMs = 250 ms`) before yielding to `GSI-1`, `GSI-2`,
   and incoming base exports. `GSI-1` and `GSI-2` do not stall behind the `GSI-3` backlog.
2. **AIMD concurrency control:** `MaterializationDO` starts `GSI-3` recovery at low concurrency (for example, 2
   in-flight batch RPCs), increases concurrency additively on success up to `maxConcurrentWrites` (for example, 8), and
   halves it on any transient error or `partition_migrating`.
3. **Drain time:** The 72,000 new items need 1 write each (`WRITE_NEW`), and the 50,000 updated items need 1 write
   (same GSI key) or 1 write + 1 delete (changed GSI key), for **122,000 to 172,000 total GSI entry operations**
   (grouped into batched RPCs per destination `GSI-3` partition). At 1,000 items/s across 4 `MaterializationDO`
   partitions, the 2-hour backlog drains in **~2.5 minutes**, and `MaterializationDO` reclaims `latest_items.data` as
   each item completes.

### 4.10 Open questions

#### 4.10.1 Default retention mode and payload mode for `latest_items.data`
- **Compact mode:** Clear `latest_items.data = NULL` as soon as all registered GSIs and active stream subscriptions pass
  `revision`. Online creation of a new GSI then scans base `PartitionDO` leaves.
- **Full-mirror mode:** Retain `latest_items.data` for all live items so online GSI creation scans only
  `MaterializationDO` (`2x` live item storage cost).
- **Pointer mode:** A future extension can send only `(baseHK, baseSK, revision)` for large items and let
  `MaterializationDO` read the current row from the base table.

#### 4.10.2 Temporary duplicate visibility during a cross-partition GSI key move
`WRITE_NEW(C)` followed by `DELETE_OLD(A)` prefers a brief duplicate over a brief omission when `A` and `C` live on
different GSI partitions. When a caller needs zero temporary duplicates, the move must use a 2PC transaction across the
old and new GSI partitions.

#### 4.10.3 GSI worker ownership
`MaterializationDO` can drive its own GSI state machines, or separate `GsiMaintainerDO` instances can claim work. The
choice affects split migration, leases, and independent worker scaling.

#### 4.10.4 Materialization stream cursors during repartition
Define how a remote sink finishes reading a parent `MaterializationDO` epoch after a split, starts reading the child
epochs, and how long the parent retains rows for active cursors.

#### 4.10.5 GSI definition, key expressions, and asynchronous key violations
Define key expressions, ordered numeric encoding, projection modes, the public GSI query API, and whether base
`PartitionDO` instances validate GSI key expressions synchronously during base writes or record `status = 'VIOLATION'`
asynchronously in `MaterializationDO`. Also decide whether violation records block a backfilling GSI from becoming
`ACTIVE`.

#### 4.10.6 First release scope
Decide whether the first release supports online GSI creation on an existing table or only static GSIs declared when the
table is created.

#### 4.10.7 Circuit breaker scope and storage admission thresholds
Select whether a circuit breaker scopes to one `index_id` or to one destination GSI partition, and define the exact
size thresholds where `MaterializationDO` stops export acknowledgements and where `PartitionDO` rejects writes.

## 5. Alternative Options

### 5.1 Base outbox + base shadow (`_fokos_change_shadow`) + stateless Queue fan-out

An alternative design keeps `_fokos_change_pending (baseKey, version, newImage)` and
`_fokos_change_shadow (baseKey, lastShippedKeyAttrs)` in the base `PartitionDO`, drains batches to a Cloudflare Queue,
and uses stateless queue consumers to compute `oldGsiKey -> newGsiKey` and apply LWW writes and tombstones to the GSIs.

This option is rejected for five reasons:

1. **Permanent orphan GSI entries under `A -> B -> C` when a crash occurs between `queue.send()` and the shadow
   update:** In a Durable Object, `await env.QUEUE.send()` is an external network call and SQLite `transactionSync` is
   local synchronous storage. They cannot commit atomically:
   - Item `X` ships at `v1` with GSI key `A` (`shadow = A`).
   - At `v2`, `X` changes to GSI key `B`. The alarm sends `{ old: A, new: B, v: 2 }` to the Queue, and the DO crashes
     or times out before it updates `shadow` to `B`.
   - Before the next alarm runs, a client updates `X` at `v3` to GSI key `C`. The base outbox coalesces the pending row
     to `v3 (C)`, while `shadow` is still `A`.
   - The next alarm sends `{ old: A, new: C, v: 3 }` and updates `shadow` to `C`.
   - The stateless consumers execute both messages: the first deletes `A@v2` and writes `B@v2`; the second deletes
     `A@v3` and writes `C@v3`. Neither message deletes `B@v2`, so `B@v2` remains in the GSI forever.
   - Updating `shadow = B` before `queue.send()` has the mirror defect: if `queue.send()` fails and `v3 (C)` arrives,
     `{ old: B, new: C, v: 3 }` is sent and `A@v1` remains in the GSI forever.
2. **TSO `last_write_ts` is not strictly increasing per item:** `PartitionStore.upsertItem` uses
   `MAX(last_write_ts, txOrderTs)` to tolerate `maxClockSkewMs` between coordinators and non-transactional writes. Two
   successive writes to one item can share the same `last_write_ts`, which breaks timestamp-based LWW.
3. **Cloudflare Queues 128 KB message limit vs 400 KB FokosDB items:** Any item above 128 KB cannot travel in a Queue
   message. Sending a pointer fails when the item is subsequently updated or deleted in the base table before all 10
   GSI consumers and remote sinks fetch the pointer.
4. **Base partition image duplication and schema coupling:** Storing `newImage` in `_fokos_change_pending` duplicates
   every dirty item inside the base `PartitionDO` when transport lags, and maintaining `lastShippedKeyAttrs` forces
   the base partition to know the key extractors of all 10 GSIs.
5. **Tombstone horizon coupled to Queue/DLQ retention:** With stateless queue fan-out, a delayed `put(A, v2)` can sit
   in a retry queue or DLQ for days, so GSI partitions must retain tombstones for days instead of minutes.

### 5.2 Lossless per-mutation CDC log in the base partition

Each base partition can append every mutation `(seq, baseKey, oldImage, newImage)` to a log table. This avoids keeping
`applied_index_hk/sk` in `MaterializationDO`, but storage grows with total mutation count (`720,000` rows in the
2-hour outage scenario instead of `122,000` distinct items) and needs strict cross-partition log ordering after a
base split or promotion.

### 5.3 1:1 sidecar `MaterializationDO` per base `PartitionDO`

Each base partition `<part>` can have a paired `<part>.mat` DO. This avoids routing by `(baseHK, baseSK)` on export,
but when the base partition splits or promotes a key, the sidecar must also split even when the materialization state is
small, and out-of-order exports from an old parent and a new child no longer land on the same `MaterializationDO`.

## 6. Frequently Asked Questions

### 6.1 Do GSIs and remote data-lake sinks share the same pipeline?

Yes. Base `PartitionDO` instances export once to `MaterializationDO`. Inside `MaterializationDO`, GSIs use the
per-item `gsi_item_state` state machine (because a GSI must delete its old key entry), while remote sinks use the
cursor-based watermark scan on `latest_items (change_seq)`.

### 6.2 Can an old base partition overwrite a newer item state after multiple splits?

No. `v` and `max_deleted_v` produce a strictly increasing integer revision for each `(baseHK, baseSK)` across splits
and promotions. Both the old base partition and the new base partition route `(baseHK, baseSK)` to the same owner in
`fokos.m.<tableName>`, which rejects any export with `revision <= stored.revision`.

### 6.3 Does a slow or dead remote data lake cause `MaterializationDO` or base partitions to reject writes?

A dead remote sink only lags its own `cursor_seq` in `stream_subscriptions`. Base outbox acknowledgements and all 10
GSIs continue without delay. Each subscription has an `expires_at` TTL; if a remote sink is abandoned past its
retention window, `MaterializationDO` expires the cursor and reclaims `latest_items.data` so storage cannot grow
forever.

### 6.4 What happens when an item is created, updated, and deleted while `MaterializationDO` is unreachable?

While `MaterializationDO` is unreachable, the base exporter has not tried to send the new item (`unexported_create = 1,
attempted = 0`). When `deleteItem` runs, `PartitionStore` deletes the `materialization_outbox` row in place. Zero
bytes remain in the base outbox and zero RPCs are sent when `MaterializationDO` recovers.

## 7. References

- `docs/ideas/2026-09-17-coalesced-materialization-and-gsis.md`
- `docs/agent-plans/2026-10-03-max-deleted-version.md`
- `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`
- `docs/agent-plans/2026-09-26-fokos-sharding-client.md`
- `packages/fokosdb/src/shared/partition/partition-store.ts`
- `packages/fokosdb/src/shared/partition/fokos-migration-host.ts`
- `packages/fokosdb/src/server/do-partition.ts`
- [Using Global Secondary Indexes in DynamoDB][dynamodb-gsi]
- [Cloudflare Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Cloudflare Queues limits](https://developers.cloudflare.com/queues/platform/limits/)

[dynamodb-gsi]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
