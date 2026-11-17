# RFC — Consistent backup and restore of a table with lazy clock-boundary cuts

**State:** Draft
**Date:** 2026-11-17
**Author:** Lambros Petrou

**Status:** Nothing is built. "Today" means the code on `main` at the date of this document.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
  - [1.1 Glossary](#11-glossary)
  - [1.2 The current system](#12-the-current-system)
  - [1.3 Platform facts](#13-platform-facts)
  - [1.4 Problems](#14-problems)
  - [1.5 Why now](#15-why-now)
- [2. Goals and requirements](#2-goals-and-requirements)
  - [2.1 In scope](#21-in-scope)
  - [2.2 Out of scope](#22-out-of-scope)
  - [2.3 Requirements](#23-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
    - [4.2.1 Invariants](#421-invariants)
    - [4.2.2 M1 — The seed scope](#422-m1--the-seed-scope)
    - [4.2.3 M2 — Position references](#423-m2--position-references)
    - [4.2.4 M3 — Peer calls](#424-m3--peer-calls)
    - [4.2.5 M4 — max_v](#425-m4--max_v)
    - [4.2.6 M5 — Tags and cuts](#426-m5--tags-and-cuts)
    - [4.2.7 M6 — The client boundary wait](#427-m6--the-client-boundary-wait)
    - [4.2.8 M7 — Restore windows and the admin client](#428-m7--restore-windows-and-the-admin-client)
    - [4.2.9 M8 — Restore into a new table](#429-m8--restore-into-a-new-table)
    - [4.2.10 M9 — Restore in place](#4210-m9--restore-in-place)
    - [4.2.11 Why a set of cuts is consistent](#4211-why-a-set-of-cuts-is-consistent)
    - [4.2.12 Performance and cost](#4212-performance-and-cost)
    - [4.2.13 Deployment and rollback](#4213-deployment-and-rollback)
    - [4.2.14 Testing](#4214-testing)
  - [4.3 Open questions](#43-open-questions)
  - [4.4 Follow-ups](#44-follow-ups)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)
- [Appendix A. Controller marker cuts](#appendix-a-controller-marker-cuts)
- [Appendix B. Worked examples](#appendix-b-worked-examples)

---

## 1. Overview and context

A FokosDB table is many Durable Objects. Today an operator cannot go back to an earlier state of a table. A bad
deploy or a bad batch job that corrupts items has no recovery path.

Durable Objects keep 30 days of point-in-time history for each object, and a new API can restore that history into the
same object or into another one. This RFC uses it to give every table a restore point every 30 minutes. A restore
point is consistent across all the Durable Objects of the table. Capture costs nothing for a Durable Object that gets
no requests.

An operator can restore a table into a new table, or in place under the same name.

### 1.1 Glossary

- **DO** — a Durable Object.
- **Bookmark** — an opaque string that names one position in the storage history of one DO. Bookmarks of one DO
  compare as strings in history order.
- **Snapshot** — the `DurableObjectSnapshot` handle that `ctx.storage.snapshot()` returns. It names the storage of
  one DO at one bookmark.
- **Backup period** — `backupPeriodMs`, the time between two restore points. The default is 1,800,000 ms (30
  minutes).
- **Boundary** — a time in ms that is a multiple of the backup period: `boundaryAt(t) = t − (t mod backupPeriodMs)`.
- **Tag** — the value `{ backupBoundary, generation? }` that a FokosDB message carries.
- **Current boundary** — of a DO or a client: `max(boundaryAt(own clock), highest tag boundary seen, cut boundary)`.
- **Cut** — the step in which a DO records one bookmark of its state before it changes that state at a new current
  boundary.
- **Cut row** — one row of `fokos_backup_cuts`.
- **Restore point** — a boundary that a table can return to.
- **Restore window** — an interval of time in which a table has restore points, with one backup period.
- **Generation** — an internal number of a table that each in-place restore and each undo increases by 1. It starts
  at 0. A DO stores it, and a message from a DO carries it.
- **Position** — the part of a DO name after `~`, for example `h.0.1` or `r.<routingHash hex>.<start>.<end>`. A shard
  group must not contain `~`, so the position is the text after the first `~`.
- **Source** — the table that a restore reads.
- **Restore set** — the DOs that a restore reads and writes: every DO of the partition group, of the coordinator
  group, of the materialization group, and of each GSI group of the source.
- **Fence** — a stored record that stops all changes of a DO until an in-place restore replaces its storage.
- **Pre-fence bookmark** — the bookmark of a DO just before its fence. An undo restores to it.
- **Absent DO** — a DO of the restore set that had no identity at the restore point.
- **Seed scope** — the part of a shard group name that gives the seed of its routingHash
  ([the seed-scope RFC](2026-11-17-routing-hash-seed-scope.md)).
- **Revision floor** — a number above every item revision that the table issued before an in-place restore.
- **Table admin** — the DO `fokos.admin.<tableName>~<fp(ns)>`. It holds the restore windows and the generation of
  the table.
- **Restore job DO** — the DO `<table admin name>.restore.<restoreId>`. It runs one restore.
- **δ** — `maxClockSkewMs`, the largest difference between two clocks that FokosDB tolerates: 1,000 ms.

This document uses "epoch" only in the names of two existing values: the topology epoch (`TopologyVersion.epoch` in
`src/sharding/topology-codec.ts`) and the export epoch (`materialization_export_state.epoch`).

### 1.2 The current system

The DOs of one table:

- **Partitions** (`FokosDBPartitionDO`, shard group `fokos.p.<tableName>`). They hold items, lock rows and the
  materialization outbox. They split into children, and a promotion moves a routingHash into a range tree.
- **GSI partitions** (`FokosDBPartitionDO`, shard group `fokos.gsiv1.<n>.<tableName>.<indexName>`, one group for each
  GSI). The materialization DOs write them. `destroy` finds the index names from the targets of the materialization
  DOs (`FokosDB.destroy` in `src/client/db.ts`).
- **Coordinators** (`FokosDBTransactionCoordinatorDO`, `fokos.tc.<tableName>.<fp(ns)>`). They drive 2PC and hold
  the transaction ledger.
- **Materialization DOs** (`FokosDBMaterializationDO`, `fokos.m.<tableName>.<fp(ns)>`). They hold the change state
  of the GSIs, and they push changes to the push targets of the user (`pushTargetStub` in `src/shared/do-stubs.ts`).
- **Topology admin DOs** (`FokosDBAutoAdminDO`, `fokos.topo.…`). They hold the topology of each shard group. A
  client uses that topology as a hint: "A topology hint changes the latency only, never the result"
  (`FokosShardingClient`, `src/sharding/client.ts`). A client cache keeps its view when a fetched version is older
  (`compareTopologyVersions` in `src/sharding/topology-cache.ts`).
- **The table admin** (`FokosDBAutoAdminDO`, `fokos.admin.<tableName>~<fp(ns)>`). It holds no state today. The class
  selects the role of an instance by its name.

The partitions, the coordinators and the materialization DOs host `FokosShardingRuntime`
(`src/sharding/runtime.ts`). A split only adds DOs. Nothing merges two DOs. A parent persists each repartition target
before it calls `fokosInit`. DO names are deterministic, so a later split of a partition uses the same child names as
an earlier split of that partition.

Every FokosDB protocol writes its state before it sends an RPC, and it retries from the stored state until it gets
an answer. 2PC, the repartition flow, the migration pulls, the materialization outbox and the admin outbox all follow
this rule. A DO restarts from any stored state without loss.

A DO calls another DO from several modules: the runtime (`#peer`), the repartition flow (`fokosInit`,
`fokosMigrationPull`, `fokosMigrationAck`), the coordinator through `FokosShardingClient`, the admin outbox, and the
materialization exports.

`destroy` is the only code path that calls `ctx.storage.deleteAll()`. It runs in `FokosShardingRuntime`, and it
calls `ctx.abort(DESTROY_ABORT_SENTINEL)` right after. When a non-root partition has no identity, it answers
`hash_partition_not_initialized`, and the client cuts the topology hint and resolves the key again from a shallower
partition.

Stored state names the table in these places today:

| Stored where | What it holds |
| --- | --- |
| `__fokos/identity` | `ref.doName`, `topology.shardGroup` and `routingHashSeed` |
| Each `routing_hash` column | `xxh64(hashKey, seed)`, with a seed from the whole shard group name today |
| `fokos_repartition_targets.do_name`, the repartition plans in KV, `__fokos/import` | The names of peer DOs |
| `pending_tx_info.coordinator_json` | The `doName` of the coordinator |
| `tc_participants.partition_do_name`, `partition_context_json` | The names and contexts of the participants |
| `ancestor_fences.ancestor_id`, `producer_fences.producer_id` | DurableObjectId hex, which depends on the name |
| The tables of the topology keeper | Rows keyed by `shard_group` |

A read transaction compares `max_deleted_v` for an item that is absent in both of its phases
(`src/shared/transaction-wire-types.ts`). Only a delete raises `max_deleted_v`, so a write of another key does not
abort the read. A new item row starts at `max_deleted_v + 1`.

### 1.3 Platform facts

From the [SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/):

- Point-in-time recovery (PITR) applies to SQLite-backed DOs. It covers the SQL data and the KV data.
- `getCurrentBookmark()` returns the bookmark of the current position.
- `getBookmarkForTime(t)` returns a bookmark for about the time `t`, inside the last 30 days.
- PITR is not available in local development.

From workerd (`src/workerd/api/actor-state.h`, `src/workerd/api/snapshot.h`):

- `ctx.storage.snapshot({ bookmark? })` returns a snapshot of the current position or of an earlier bookmark.
- `ctx.storage.onNextSessionRestore(snapshotOrBookmark, { kvData? })` schedules a restore for the next start. It
  returns the bookmark that undoes the restore. `kvData` holds at most 20 keys and 3 KB. The restore writes it as KV
  values before the constructor runs. `kvData` works only with a snapshot target.
- Both methods need the experimental compatibility flag `durable_object_snapshots`. Cloudflare enables the flag for
  each account.
- A facet cannot call either method.
- workerd has no backend for snapshots or PITR. Both throw in local development and in vitest.

From the author, about the production implementation (not in the public documentation on 2026-11-16):

- A snapshot works across Workers and across namespaces.
- A snapshot cannot be stored. It can only travel over RPC. A bookmark is the stored form of a position.
- A bookmark older than 30 days cannot be restored. A bookmark of a history that a restore abandoned stays
  restorable for 30 days.
- A restore of a DO from a snapshot of itself is as fast as `onNextSessionRestoreBookmark()`.
- A restore does not restore alarms, and it clears the alarm of the target DO.
- PITR works after `deleteAll()`.
- A bookmark from `getCurrentBookmark()` includes the writes of the same transaction and of the earlier transactions
  that its output gate covers.
- `snapshot()` takes about one remote call. A restore of a snapshot or a bookmark takes about 20 s at p90 and 50 to
  65 s at p99. A restore into another DO downloads and uploads the whole storage of the DO.
- No limit applies to the number of restores that run at the same time.

From the [Workers limits](https://developers.cloudflare.com/workers/platform/limits/) and the
[Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/) on 2026-11-17:

- An invocation on the Paid plan can make 10,000 subrequests by default, and the Wrangler configuration can raise it
  to 10 million.
- An invocation can have six connections that wait for response headers. A seventh waits in a queue.
- An alarm handler has a wall time of 15 minutes.
- The pages do not say whether a call to a Durable Object stub counts for the subrequests or for the six connections.

### 1.4 Problems

**P1. One bookmark for each DO is not a consistent backup.** Take a bookmark of each DO at about the same time.
Coordinator C is in `COMMITTING` and sends `txCommit` to P1 and P2. The bookmark of P1 is after the commit. The
bookmark of P2 is before the prepare, so P2 holds no lock. After a restore, C sends `txCommit` to P2 again. P2 has no
lock and answers with the idempotent success. Half of the transaction is lost.

**P2. A walk of all DOs in each period costs too much.** The expected scale is thousands of tables with hundreds or
thousands of partitions each. A protocol that wakes every DO in each backup period bills each idle DO for one wake
each period: 48 wakes each day for each DO with a 30-minute period.

**P3. A restore point must stay restorable.** A bookmark older than 30 days fails. An idle DO must never give an old
bookmark for a recent restore point.

**P4. Item revisions must keep increasing under one table name.** AGENTS.md states that the revisions of a key always
increase. An in-place restore moves an item back to an older revision. Without a fix, the next write issues a
revision that the abandoned history already issued with other data.

**P5. The stored data names its table.** The seed of each routingHash comes from the table name today, so the stored
rows of a table cannot serve another table name. Peer references and fences also hold full names.

**P6. A table name for each restore does not scale.** Users have thousands of tables. A restore that always needs a
new table name forces them to keep a map of names.

### 1.5 Why now

The snapshot API is available behind `durable_object_snapshots`. The repartition flow will also use snapshots for
splits and promotions. That work must keep the rules of [4.2.6](#426-m5--tags-and-cuts), so the rules must exist
first. The seed scope (M1) is a breaking change of the stored data, so it must ship before more tables store data.

---

## 2. Goals and requirements

### 2.1 In scope

- G1. A table has a restore point at each boundary inside its restore windows, in the last 30 days minus
  `RESTORE_WINDOW_MARGIN_MS`.
- G2. A restored table is a consistent cut. Each 2PC transaction is complete or absent in all DOs. Each split,
  promotion, materialization, GSI write and outbox is at a state that its own recovery completes.
- G3. Inside one request and inside one client instance, a restored table keeps causal order with no clock
  assumption.
- G4. When the client option `boundaryWait` is on, a restored table also keeps causal order across requests,
  isolates and queues of the application, if no two clocks differ by more than δ. Push targets are not covered
  ([6](#6-frequently-asked-questions)).
- G5. Capture wakes no DO. A DO that handles nothing in a backup period does no work for that period.
- G6. An operator can restore a table into a new table. The source serves reads and writes during the whole restore.
- G7. An operator can restore a table in place, under the same name, into the same DOs, with no name to choose.
- G8. During an in-place restore, each DO serves non-transactional reads until its own restart.
- G9. An operator can undo the latest in-place restore of a table.
- G10. Under one table name, item revisions increase across in-place restores and undos.
- G11. An operator can change the backup period with a deploy.
- G12. A later version can add a restore to a point of an abandoned history without a change of the stored data of
  this RFC ([4.4](#44-follow-ups)).

### 2.2 Out of scope

- A restore into another account.
- Retention longer than 30 days. This needs a copy of each DO into an archive DO.
- A restore of a destroyed table. `destroy` deletes the cut rows.
- An on-demand restore point at an exact time. The nearest restore point is at most one backup period old.
- A restore point between two boundaries.
- A restore of one partition or of some items.
- After an in-place restore to P, a restore to a point between P and that restore. Only an undo returns to that
  history in this RFC. [4.4](#44-follow-ups) shows how a later version adds it.
- An undo of an in-place restore that is not the latest one, and an undo of an undo.
- A rewind of push targets. A push target gets only the changes after a restore.
- Splits and promotions by snapshot. That work must follow [R9](#23-requirements).

### 2.3 Requirements

- R1. Every FokosDB message must carry a tag: each request, each response and each error that a FokosDB DO method
  throws. A DO that holds data sends its generation, 0 when it stored none. A FokosDB client, the table admin and a
  restore job DO send a tag without a generation.
- R2. A DO must not change its storage before it completes a cut for its current boundary.
- R3. Every call from a FokosDB DO to another FokosDB DO must go through `callPeer`. The type checker must reject a
  direct call ([4.2.4](#424-m3--peer-calls)).
- R4. A DO must refuse a request whose tag boundary `b` has `own clock < b − δ`. When a response has such a tag, the
  caller must treat the call as failed and must write nothing from the response.
- R5. A write must wait until the bookmark of its cut is stored or marked lost.
- R6. All DOs and clients of a table must use the same `backupPeriodMs`, except during a deploy that changes it. A
  change of the period follows [4.2.13](#4213-deployment-and-rollback).
- R7. A restore into a new table must not change a source DO. An in-place restore changes a DO only with its fence
  and its restore.
- R8. An in-place restore must fence every DO of the restore set before it restores any DO.
- R9. A DO that starts from the storage of another DO must delete the cut rows it copied.
- R10. Capture must use only `getCurrentBookmark()`. Only a restore needs `durable_object_snapshots`.
- R11. Every item revision after an in-place restore or an undo must be above its revision floor.
- R12. Each write that issues a live item revision must raise `max_v` in the same synchronous block. A read
  transaction must not compare `max_v`, and a new row must not start from it.
- R13. The seed of each routingHash must come from the seed scope of its shard group
  ([the seed-scope RFC](2026-11-17-routing-hash-seed-scope.md)).
- R14. A DO must address each peer by the position of the peer and by the shard group of its own identity. It must not
  address a peer by a stored full name.
- R15. A DO must apply the generation rules of [4.2.6](#426-m5--tags-and-cuts) to each message that has a
  generation.
- R16. A fenced DO must change no state and must send no call, except in the restore RPCs. It serves the
  non-transactional reads.
- R17. A restore job DO of an in-place restore and its workers must keep the walk set, the pre-fence bookmarks, the
  restore point, the generation and the removed windows for 30 days after the fence.

---

## 3. Milestones

M1 to M4 do not depend on backups. Each one is a change that FokosDB can ship and use alone. M5 to M9 build the
backups on them.

| Milestone | Delivers on its own | Needs |
| --- | --- | --- |
| M1 — The seed scope | The seed of each routingHash comes from the kind of its shard group, not from the table name. A breaking change of the stored data. | — |
| M2 — Position references | Stored references to peers hold positions. Fences key by position, and topology rows by kind. The type `StoredRef`. | — |
| M3 — Peer calls | `callPeer` and the peer handles. One place for every call from a DO to another DO. | — |
| M4 — `max_v` | Each partition tracks the highest live revision that it issued. | — |
| M5 — Tags and cuts | Every message carries a tag with a boundary and an optional generation. The generation rules are in the tag check, and they do nothing until M9 stores a generation. Each DO of the restore set cuts before its first change at each boundary. Restore points start to exist. | M3 |
| M6 — The client boundary wait | The option `boundaryWait`, off by default. | M5 |
| M7 — Restore windows and the admin client | `FokosDBAdmin` with `enableBackups`, `disableBackups` and `listRestoreWindows`. | M5 |
| M8 — Restore into a new table | `restoreToNewTable`, `getRestore`, `cancelRestore`. Needs `durable_object_snapshots`. | M1, M2, M7 |
| M9 — Restore in place | The fence, the restore of each DO into itself with a new generation, the revision floor, the topology cleanup, `restoreTable` and `undoRestore`. Needs `durable_object_snapshots`. | M4, M7 |

M8 and M9 do not depend on each other. M9 does not need M1 and M2, because an in-place restore keeps every name.

---

## 4. Proposed solution

### 4.1 High-level overview

**Capture.** Every DO and every client knows the current boundary from its clock. Every message carries the boundary
of its sender, so a DO with a slow clock learns the boundary from its peers. A DO records a cut at most once in each
backup period, and only when it is awake for another reason: a request, a response, an alarm or a timer. Before its
first change of state at a new boundary, it stores one cut row and one bookmark. A DO that is not woken records
nothing, and its state does not change.

**Resolution.** For a restore point P, the state of one DO is the bookmark of its smallest cut row at or above P. When
a DO has no cut row at or above P, it has changed nothing since P. The restore then asks the DO, the request cuts it,
and that cut gives the bookmark. No message that a DO sends after its cut can change another DO before the cut of that
DO. So these states together form a consistent cut. A transaction is complete or absent in all of them.

**Restore into a new table (M8).** The job copies each source DO at its resolved bookmark into the DO with the same
position in the new table. The source DO calls `snapshot({ bookmark })`, and the snapshot travels over RPC to the
target DO, which calls `onNextSessionRestore()`. The stored rows do not depend on the table name: the seed comes from
the seed scope (M1), and each peer reference holds a position (M2). The source keeps serving.

**Restore in place (M9).** The job fences every DO of the table, and each fenced DO serves only plain reads. Each DO
then restores itself from a snapshot of its own resolved bookmark, with a marker that holds the new generation and
the revision floor. A DO that did not exist at P deletes its storage. On its next start, each DO completes its
recovery: it re-arms its alarm, a coordinator cancels each transaction that has no decision, and a partition lifts
all later revisions above the floor. A message from a DO of the old generation cannot change a restored DO. The
topology admin DOs drop the repartitions after P and raise their topology epoch. An undo restores each DO to its
pre-fence bookmark in the same way.

```
 capture (each DO, no controller)              restore (the restore job DO)

 request / response / alarm / timer            M8: resolve P on each source DO (it keeps serving)
          │                                        source snapshot({bookmark}) ──RPC──► new-table DO
          ▼                                    M9: fence + resolve P on each DO (plain reads only)
 current boundary = max(clock, tags, cut)          each DO: snapshot({bookmark}) of itself
          │                                        ─► onNextSessionRestore(marker) ─► abort
   cut due? ──no──► handle                               │
          │ yes                                          ▼
          ▼                                    next start: download, marker, recovery
 blockConcurrencyWhile:                                  │
   insert cut row, getCurrentBookmark(),                 ▼
   store the bookmark ──► handle               topology admin DOs: drop the repartitions after P
```

### 4.2 Technical details

#### 4.2.1 Invariants

- **I1. Cut before change.** A DO changes its storage only when its cut row for its current boundary exists. The cut
  check at each event entry and at each write site holds it. The write guard in each store makes a missed site fail
  ([4.2.6](#426-m5--tags-and-cuts)). Three writes are exempt: the writes of the cut itself, the creation of
  `fokos_backup_cuts`, and the steps of a restore marker.
- **I2. Tags do not go down.** The tag of a message is the current boundary of the sender when it sends. The current
  boundary is a `max`, so it never goes down. Thus a message sent after a change at boundary `b` has a tag of `b` or
  more.
- **I3. Cuts are crash-safe.** A cut row in the state `pending` means the DO has changed nothing since the row. R5
  holds it.
- **I4. A decision implies its locks.** When the P-state of a coordinator holds `COMMITTING` for a transaction, the
  P-state of each participant holds the lock or the commit of that transaction. I1, I2 and R3 hold it
  ([4.2.11](#4211-why-a-set-of-cuts-is-consistent)).
- **I5. The cut rows of a DO describe its current history.** An in-place restore to P keeps the cut rows up to the
  resolved row, which are the history before P. The rows after it go with the abandoned history, and the restore
  windows end at P. So each DO resolves every restore point of the table from its own rows. An undo brings back the
  rows and the windows of the abandoned history.
- **I6. A restored DO handles no message of an older generation.** The generation rules of the tag check hold it
  (R15).
- **I7. Revisions increase under one name.** Each in-place restore and each undo lifts every later revision above all
  revisions of the table before it (R11).
- **I8. A table addresses only itself.** Each DO builds the name of a peer from the shard group of its own identity
  and the position of the peer (R14). A DO of one table never calls a DO of another table.

#### 4.2.2 M1 — The seed scope

The seed of each routingHash comes from the seed scope of its shard group: `fokos.p`, `fokos.tc`, `fokos.m`, and
`fokos.gsiv1.<n'>.<indexName>` for a GSI. The table name and the namespace fingerprint are not in the seed scope.
[The seed-scope RFC](2026-11-17-routing-hash-seed-scope.md) holds the problem, the reasons, the rule, the option
`routingSeedOf` of the sharding library, what the change breaks, and the tests. Its M2 adds an optional base seed for
a deployment, `routingHashBaseSeed`, which this RFC does not need.

The groups of one table keep different seeds, and two tables get the same seed for each kind of group. So the
`routing_hash` columns of a table are valid under another table name, and a client of a new table computes the same
routingHashes with no configuration.

M1 changes the seed of every existing shard group. It deploys to new namespaces
([4.2.13](#4213-deployment-and-rollback)).

#### 4.2.3 M2 — Position references

M2 makes the stored state of a DO independent of the name of its table, except the own identity of the DO:

- **Position references.** Each stored reference to a peer holds the position of the peer: the repartition targets,
  the repartition plans, `__fokos/import`, `pending_tx_info.coordinator_json`, and the participant rows of
  `tc_participants`. A DO builds the name of a peer from the shard group of its own identity and the position (R14).
  A row from before M2 holds a full name, and a reader keeps only the position after the first `~`. No migration
  rewrites those rows.
- **The type `StoredRef`.** A stored reference is the branded type `StoredRef`. Only the builder of a peer name
  accepts it, and the stub helpers of `src/shared/do-stubs.ts` accept only a built name. The type checker then
  rejects a stored full name that goes to a stub helper.
- **Fences by position.** `ancestor_fences` and `producer_fences` key their rows by the position of the ancestor or
  the producer, not by its DurableObjectId hex. A migration rewrites the existing rows. Both tables hold a few rows
  for each DO.
- **Topology rows by kind.** The tables of the topology keeper key their rows by the kind of the shard group (`p`,
  `tc`, `m`, or the GSI), not by its full name. There is one topology admin DO for each shard group, so the kind is
  enough. A migration rewrites the existing rows.

A DO of a table that no restore copied sees no change: the built names are the names of today.

#### 4.2.4 M3 — Peer calls

`src/sharding/peer-call.ts` exports `callPeer(peer, method, req)`. In M3 it only awaits the call. From M5 it also adds
the tag of the caller to the request and runs the tag check on the response or on the error. When the check fails, it
throws a retryable error, and the caller handles it as a failed call.

The stub helpers of `src/shared/do-stubs.ts` and `#peer` of the runtime return a peer handle. The handle is a branded
type with no methods, and only `callPeer` can call it. The type checker then rejects a direct call on a stub of the
four classes. A push target is not a FokosDB class, so `pushTargetStub` keeps its plain stub.

M3 changes every call site of a DO-to-DO call: the runtime, the repartition flow, the coordinator, the admin outbox
and the materialization exports. It changes no behaviour.

#### 4.2.5 M4 — max_v

A new column of `deletion_metadata` holds the highest live `v` that the partition issued:

```sql
-- max_v : the highest v of a live item row that this partition issued. A delete revision is in max_deleted_v.
--         Only the revision floor of an in-place restore reads it.
ALTER TABLE deletion_metadata ADD COLUMN max_v INTEGER NOT NULL DEFAULT 0;
```

- Each statement that issues a live `v` raises `max_v` in the same synchronous block as the item write:
  `UPDATE deletion_metadata SET max_v = ? WHERE id = 1 AND max_v < ?`. The sites are the upsert of
  `src/shared/partition/partition-store.ts` (a new row at `max_deleted_v + 1`, an update at `v + 1`) and the compiled
  plan of `src/shared/expression/plan.ts`. `commitLocal` and the import pages write through these sites. M4 lists
  each site.
- The partition keeps `max_v` in memory after its first read, and it skips the statement when the new `v` is not
  above it. Most writes thus cost one comparison in memory. Only a write that raises the highest `v` of the
  partition writes the row. A partition whose hottest key holds the highest `v`, for example a counter, writes one
  more row on each write to that key.
- A migration merges `max_v` with `MAX`, as it merges `max_deleted_v`. A split child thus starts at or above its
  source.
- `max_v` is not `max_deleted_v`. `max_v` changes on many writes, so a comparison of `max_v` would abort an
  absent-item read transaction when another key of the partition changes between the two phases. A new row starts
  at `max_deleted_v + 1`, never at `max_v + 1`, so normal writes do not jump.
- The partition tracks `max_v` always. Rows from before M4 need no backfill: only a revision that the table issued
  after the restore point can collide with a new revision. A revision before the restore point is in the restored
  state, and the next write goes above it. The restore point is in a restore window, which starts after M5, so
  `max_v` tracked each revision after it.

#### 4.2.6 M5 — Tags and cuts

**The boundary.** One module in `src/sharding/` holds the functions:

```ts
export const DEFAULT_BACKUP_PERIOD_MS = 1_800_000;
export const MAX_CLOCK_SKEW_MS = 1_000;
export function boundaryAt(ms: number, periodMs: number): number { return ms - (ms % periodMs); }
export type FokosBackupTag = { backupBoundary: number; generation?: number };
```

A boundary is a time, not an index. So two boundaries compare correctly also when they come from two backup
periods. `backupPeriodMs` is a setting of the host config (`fokosConfig()`) and of the client options.

Each DO keeps three values:

- `seenBoundary`, in memory: the highest tag boundary that this instance accepted.
- `cutBoundary`, under the KV key `__fokos/backup/cut_boundary`: the boundary of its last cut.
- `generation`, under the KV key `__fokos/backup/generation`. When the key is absent, the generation is 0. Only an
  in-place restore (M9) and the rules below store it.

`currentBoundary() = max(boundaryAt(Date.now(), backupPeriodMs), seenBoundary, cutBoundary)`.

**The wire format.**

- A request carries `backupTag` next to its route context. A method that takes no route context carries
  `backupTag` in its request object. M5 lists every RPC method of the four DO classes and adds the field to each one.
- `FokosRouting` (`src/sharding/runtime-types.ts`) gets `backupTag`. Every `FokosEnvelope` thus carries it. A
  response that is not an envelope gets a `backupTag` field.
- A `FokosRoutedError` carries `routing`, and thus the tag. An error that a FokosDB DO method throws outside
  `dispatch` gets the tag at the method boundary.
- A DO of the restore set sends `{ backupBoundary: currentBoundary(), generation }`, also when its generation is 0.
- A missing tag, from code before M5, is accepted and adopts nothing. A missing `generation` skips the generation
  rules.

**The tag check.** A DO runs these steps for each request when it arrives. `callPeer` runs them for each response
and each error. Steps 1 and 2 are the generation rules. They apply only when the tag has a generation. A client runs
steps 3 and 4 for each response.

1. **A lower generation.** For a request, refuse with `table_generation_stale` (retryable, with the own generation).
   For a response, the call failed.
2. **A higher generation.** When the DO is fenced, for a request, refuse with `table_restoring` (retryable), and for
   a response, the call failed. Else, store the generation of the tag and continue.
3. **A tag too far ahead.** When `tag.backupBoundary > currentBoundary()` and
   `Date.now() < tag.backupBoundary − δ`: for a request, refuse with `clock_skew` (retryable, with
   `retryAfterMs = tag.backupBoundary − δ − Date.now()`). For a response, the call failed. Log the two clocks.
4. Else, set `seenBoundary = max(seenBoundary, tag.backupBoundary)`.

So a new DO takes its generation from the message that creates it, for example `fokosInit`. A DO that bootstraps
from a client request, for example the root of a GSI that the table gets after a restore, takes it from its first
message from a DO. Only a DO whose state is current can adopt a higher generation in this way: an in-place restore
fences every DO before it raises the generation (R8), its walk reaches every DO, and a fenced DO refuses a higher
generation.

Before the first in-place restore, every DO has the generation 0, so the generation rules do nothing. A DO sends the
generation 0 explicitly: after the first restore, a late message of the old history then has the lower generation 0,
and a restored DO refuses it. A tag with no generation would skip the rules. M5 ships the rules anyway: every version
from M5 on refuses a late message of an old history, so the first in-place restore depends only on the RPCs of M9.

"The call failed" means: the caller writes nothing that comes from the response, and it retries the call later
through its normal retry path. A refusal happens before the handler runs, so it changes no state.

A DO does not clamp a tag. A clamp accepts the data of a message and drops its boundary, which breaks I2.

**The client.** `FokosShardingClient` keeps `seenBoundary` for each instance and sends its current boundary. A new
client starts from the boundary of its clock. One `seenBoundary` serves all the tables of a client, because a
boundary is a time. With clocks within ±250 ms of real time, two clocks differ by at most 500 ms. A tag is then above
the current boundary of the receiver only in the last 500 ms before a boundary, and the check with δ = 1,000 ms
accepts it. [Appendix B.3](#b3-a-bad-clock) shows a bad clock.

**The DOs that cut.** `FokosDBPartitionDO`, `FokosDBTransactionCoordinatorDO` and `FokosDBMaterializationDO` cut. A
GSI partition is a `FokosDBPartitionDO`, so it cuts too. The cut module lives in `src/sharding/`, and
`FokosShardingRuntime` uses it. `FokosDBAutoAdminDO` runs the tag check in all its roles and does not cut.

**The backup storage.** The cut module and the restore RPCs reach PITR and snapshots only through the type
`FokosBackupStorage`: `getCurrentBookmark()`, `snapshot()` and `onNextSessionRestore()`. The runtime takes it as an
option, the same way the scheduler takes its storage. Production passes `ctx.storage`. A test passes a fake
([4.2.14](#4214-testing)).

**The schema.** The sharding store migrations add the table.

```sql
-- One row for each cut.
-- backup_boundary : the current boundary of the DO at the cut, in ms.
-- cut_at          : Date.now() of the DO at the cut.
-- had_identity    : 1 when the DO had an identity at the cut. A row with 0 resolves to an absent DO.
-- state           : pending until the bookmark is stored, then stored. lost when the bookmark call failed.
--                   A pending row means no change since the row (I3).
CREATE TABLE IF NOT EXISTS fokos_backup_cuts (
    backup_boundary INTEGER PRIMARY KEY,
    cut_at          INTEGER NOT NULL,
    had_identity    INTEGER NOT NULL,
    state           TEXT    NOT NULL,
    bookmark        TEXT
) STRICT;
```

**The cut.** A cut is due when `currentBoundary() > cutBoundary`. `cutIfNeeded()` is the only way to start a cut.

```ts
async cutIfNeeded(): Promise<void> {
    if (!this.#enabled || this.#fenced || !this.#cutDue()) return;
    await this.#ctx.blockConcurrencyWhile(async () => {
        if (!this.#cutDue()) return;
        const boundary = this.currentBoundary();
        this.#inCut = true; // The write guard lets the cut write.
        try {
            this.#ctx.storage.transactionSync(() => {
                // INSERT INTO fokos_backup_cuts (..., state) VALUES (..., 'pending')
                // DELETE the rows with cut_at < Date.now() - CUT_RETENTION_MS, except the newest row
                // kv.put("__fokos/backup/cut_boundary", boundary)
            });
            this.#cutBoundary = boundary;
            const bookmark = await withTimeout(this.#backupStorage.getCurrentBookmark(), BOOKMARK_TIMEOUT_MS);
            // UPDATE fokos_backup_cuts SET state = 'stored', bookmark = ? WHERE backup_boundary = ?
        } catch (err) {
            // UPDATE fokos_backup_cuts SET state = 'lost' WHERE backup_boundary = ?
            // Log "fokos/backup: the bookmark of a cut is lost" with the error.
        } finally {
            this.#inCut = false;
        }
    });
}
```

- The bookmark covers the inserted row, so it names a commit of this backup period. It is never an old position
  (P3).
- `blockConcurrencyWhile` stops all other events of the DO until the bookmark is stored or lost (R5).
- The callback never throws. A thrown error in `blockConcurrencyWhile` resets the DO, so a failure of PITR would
  stop every write. A lost cut instead makes some restore points unrestorable
  ([4.2.8](#428-m7--restore-windows-and-the-admin-client)).
- `BOOKMARK_TIMEOUT_MS` is 5,000. TODO: measure the latency of `getCurrentBookmark()`.
- `CUT_RETENTION_MS` is 31 days. Pruning runs only inside a cut, so an idle DO does not wake to prune.

**Where the cut check runs.**

- At each event entry, before the handler: `dispatch` and each other RPC method, `alarm()`, and each timer callback
  (the TTL sweep timer of `FokosDBPartitionDO`). A read also cuts. The DO is already awake, and the cut costs one
  bookmark call.
- At each write site that runs after an `await`: the durable transitions of the coordinator (`markCommitting`, the
  write of `CANCELLING`, the terminal states), the steps of the repartition flow, the import page writes, the stale
  transaction recovery steps, the outbox acknowledgements, and the scheduler. M5 lists each site.

A request handler of a `point`, `group`, `single_owner` or `range` operation writes before its first `await`. So the
check at the `dispatch` entry covers it.

**The write guard.** Each store checks before each write: `#enabled && !#inCut && (#fenced || #cutDue())`. When the
check is true, the write throws the retryable `backup_cut_due`, or `table_restoring` when the DO is fenced (M9). The
next attempt goes through an event entry and cuts. M5 lists the write path of each store: the sharding store, the
partition store, the item stats, the coordinator store and the materialization store.

**The start of a DO.** The runtime starts in its own `blockConcurrencyWhile`, before the host. Its order:

1. Create `fokos_backup_cuts`. This write needs no cut. Restore points start after the rollout
   ([4.2.13](#4213-deployment-and-rollback)).
2. When the newest cut row is `pending`, call `getCurrentBookmark()` and store it. I3 makes this correct. When the
   call fails, mark the row `lost`.
3. When the KV key `__fokos/restored` exists, run the runtime steps of the marker
   ([4.2.9](#429-m8--restore-into-a-new-table), [4.2.10](#4210-m9--restore-in-place)). They need no cut.
4. Call `cutIfNeeded()`. The start is an event.
5. Run the other migrations of the runtime. The host then runs its own migrations.
6. When `__fokos/restored` exists, the host runs its steps of the marker after its migrations. A restored DO can hold
   an older schema, so the steps run on the current schema.

**Disabled cuts.** The host config `backups` turns cuts on. The value is part of the deployment and is the same in
all DOs of a version. When it is off, the DO does not cut and the guard checks only the fence. Tags still flow. Local
development and vitest have no PITR, so `backups` is off there by default.

#### 4.2.7 M6 — The client boundary wait

**The rule.** When the client option `boundaryWait` is on, `FokosDB` waits before it returns a result or an error to
the application. It reads the highest tag boundary `B` of the call. When `Date.now() < B + δ`, it waits until that
time. The default of `boundaryWait` is `false`.

**Why it works.** After the wait, the clock of the client is at least `B + δ`. Any later action by any actor happens
later in real time. Its clock is then at least `B`, because two clocks differ by at most δ. So that actor tags `B`
or more, and the receiver cuts before it changes state. This covers a new client for each request, other isolates,
and queues.

**The cost.** A response has a tag `B` only after some clock reached `B`. The clock of the client is then at least
`B − δ`. So a wait happens only in a window of `2δ` of each backup period, and one wait lasts at most `2δ`. With
δ = 1 s and a 30-minute period, at most 0.11% of the calls wait, and each waits at most 2 s. A DO-to-DO call does not
wait, because tags order it.

When `boundaryWait` is off, G4 holds only for causal chains inside one client instance (G3).

#### 4.2.8 M7 — Restore windows and the admin client

**The schema of the table admin.**

```sql
-- The restore windows of the table. valid_to is NULL for the open window.
-- period_ms : the backup period of the window. Its restore points are the boundaries of this period.
CREATE TABLE IF NOT EXISTS fokos_backup_windows (
    valid_from INTEGER PRIMARY KEY,
    valid_to   INTEGER,
    period_ms  INTEGER NOT NULL
) STRICT;

-- One row. The generation of the table, the restore that runs now, and the latest in-place restore.
CREATE TABLE IF NOT EXISTS fokos_backup_state (
    id                INTEGER PRIMARY KEY CHECK (id = 1),
    generation        INTEGER NOT NULL DEFAULT 0,
    active_restore_id TEXT,
    latest_in_place   TEXT
) STRICT;
```

**The admin client.** A new class `FokosDBAdmin`, exported from `fokosdb/client`, sends each operation to the table
admin. It takes the same table options as `FokosDB`, `backupPeriodMs` included, and it sends the route contexts of
the shard groups with each restore, as `destroy` uses them. It imports no Durable Object class as a value, as
AGENTS.md requires for the client. The data client `FokosDB` has none of these operations.

| Operation | Milestone | Effect |
| --- | --- | --- |
| `enableBackups()` | M7 | When the table has no open window, inserts one from the next boundary after now, with `backupPeriodMs`. When the open window has another period, ends it now and inserts the new one. |
| `disableBackups()` | M7 | Sets `valid_to` of the open window to now. |
| `listRestoreWindows()` | M7 | Returns `{ from, to, periodMs }` for each window: the intersection of `[valid_from, valid_to or now]` and `[now − 30 days + RESTORE_WINDOW_MARGIN_MS, now]`. |
| `restoreToNewTable({ to, tableName })` | M8 | Restores the table at `to` into the empty table `tableName` ([4.2.9](#429-m8--restore-into-a-new-table)). |
| `getRestore(restoreId)` | M8 | Returns the state of the job and its counts. |
| `cancelRestore(restoreId)` | M8 | Cancels a job before its point of no return. |
| `restoreTable({ to })` | M9 | Restores the table at `to` in place ([4.2.10](#4210-m9--restore-in-place)). |
| `undoRestore(restoreId)` | M9 | Undoes the latest in-place restore of the table. |

Each restore takes `P = boundaryAt(to, period_ms)` of the window that holds `to`. It refuses when `P` is before
`valid_from` or when no window holds `to`. A table that is the source or the target of a running restore refuses a
second restore with `restore_in_progress`.

`RESTORE_WINDOW_MARGIN_MS` is 1 day. It keeps each bookmark restorable until the last DO of a job restores.
TODO: measure the duration of a restore job.

The operator calls `enableBackups()` after a deploy that turns `backups` on reaches 100%. An older version writes
without cuts, so a restore point before that call can be wrong.

**The restore job DO.** The restore job DO is a role of `FokosDBAutoAdminDO`, selected by its name, as the other roles
are. The table admin creates it with the restore point, the kind of the restore and the route contexts. The job DO
drives its steps from its alarm, so a restart of the job DO resumes the job. Each step retries with backoff until it
succeeds, and a failure never moves the job back.

**The fan-out of a restore.** A restore of a large table holds one call open for each DO that restores, for 20 to
65 s ([1.3](#13-platform-facts)). One invocation cannot be sure to hold thousands of such calls. So the job spreads
the work over restore worker DOs, `<job name>.w.<i>`, a role of `FokosDBAutoAdminDO`:

1. The job sends each root of each shard group to a worker.
2. A worker walks its subtree. When it holds `RESTORE_WORKER_DOS` DOs, it gives each further child subtree to a new
   worker. Each worker reports its DOs and their answers to the job.
3. For an in-place restore, the job waits until every worker reports a complete walk (R8), computes the revision
   floor, and then tells each worker to restore.
4. Each worker restores its DOs, at most `RESTORE_WORKER_CONCURRENCY` at the same time, and reports each completed DO
   to the job.

Each worker stores the `restore_targets` rows of its DOs ([4.2.9](#429-m8--restore-into-a-new-table),
[4.2.10](#4210-m9--restore-in-place)) and drives its steps from its own alarm, so a restart resumes it. The job keeps
one row for each worker, and the totals. With `RESTORE_WORKER_DOS = RESTORE_WORKER_CONCURRENCY`, every DO starts its
restore when the walk completes, because no limit applies to the number of restores. TODO: measure how many calls to
Durable Object stubs one invocation can hold open, and set both values from it
([4.3.1](#431-concurrent-calls-of-one-invocation)).

**The walk.** A restore walks each shard group of the source from every root name, also the roots that never had a
request: the partition group, the coordinator group, the materialization group, and each GSI group. The job reads the
GSI index names from the targets of the materialization DOs, as `destroy` does. Each DO answers the names that its
state holds: the repartition targets in every state, and the range-tree roots of its promotions. The job adds them to
the walk. A DO that existed at P is a target of a repartition that its parent persisted before P, so the walk finds
it. The topology admin DOs are not in the walk.

**The resolution of one DO for restore point P.** The DO resolves from its own cut rows (I5):

1. When the DO is not fenced, run `cutIfNeeded()`. The request is an event, so the DO then has a cut row at or after
   P.
2. Take the cut row with the smallest `backup_boundary >= P`.
3. When the row has `had_identity = 0`, the DO did not exist at P. It is absent.
4. When the row is `lost`, the restore point is lost for the whole table. The job cancels itself.
5. Else, the bookmark of the row is the target.

A resolved bookmark comes from a cut at or after `P`. It is thus restorable while P is in a window.

**The restart of a restored DO.** M8 and M9 restart a DO in the same way:

1. `fokosRestoreRestart({ restoreId })`. When `__fokos/backup/last_restore` holds this `restoreId`, the restore
   already ran, and the DO answers `already_restored`. Else it calls `ctx.abort(RESTORE_ABORT_SENTINEL)`. The worker
   treats the sentinel error as success, the same way `destroy` treats `DESTROY_ABORT_SENTINEL`
   (`src/shared/cf-utils.ts`).
2. `fokosAfterRestore({ restoreId })`. This call starts the next session, so the DO runs the restore first. The worker
   retries the call with a long timeout. When `__fokos/backup/last_restore` does not hold this `restoreId`, the DO
   answers `not_restored`, and the worker starts the restore of that DO again.

The last host step of each marker writes `__fokos/backup/last_restore = restoreId`.

#### 4.2.9 M8 — Restore into a new table

**The target.** `restoreToNewTable({ to, tableName })` needs a table that no client used. The job checks each root of
each shard group of the target with `fokosStatus`, and refuses `restore_target_not_empty` when one has an identity.
The target must have the same `rootTreesN`, `hashSplitN`, `coordinatorRootsN` and GSIs as the source, and the job
refuses `restore_topology_mismatch` otherwise. The other options can differ, for example the jurisdiction. The table
admin of the target creates the job DO `<target table admin name>.restore.<restoreId>`.

```sql
-- One row for each source DO.
-- state : resolved, copying, restarted, or completed. absent for a DO with no target.
CREATE TABLE IF NOT EXISTS restore_targets (
    source_name TEXT PRIMARY KEY,
    kind        TEXT NOT NULL, -- partition, gsi_partition, coordinator, materialization
    position    TEXT NOT NULL,
    state       TEXT NOT NULL,
    bookmark    TEXT
) WITHOUT ROWID, STRICT;
```

**The states of the job.**

```
created ──► resolving ──► copying ──► copying_topology ──► completed
   │            │            │               │
   └────────────┴────────────┴───────────────┴──► cancelled
```

| Transition | Trigger | Effect |
| --- | --- | --- |
| `created → resolving` | the job starts | The job walks the source. |
| `resolving → copying` | every source DO is resolved | The job starts the copies. |
| `copying → copying_topology` | every copy is `completed` | The job copies the topology admin DOs. |
| `copying_topology → completed` | every topology admin DO answered | The target table admin opens a window from the next boundary after now, when the source had an open window. |
| any `→ cancelled` | `cancelRestore`, or a lost cut | The job destroys the target with `destroy`. The source never changed. |

**Step 1 — resolve.** The job walks the source ([4.2.8](#428-m7--restore-windows-and-the-admin-client)) with
`fokosRestoreResolve({ restoreId, restorePoint: P })`. Each DO answers its bookmark, `absent` or `lost`, and its
names. A split during the walk adds only DOs that did not exist at P, and those resolve to `absent`.

**Step 2 — copy each DO.** Each worker runs these calls for each of its DOs with a bookmark
([4.2.8](#428-m7--restore-windows-and-the-admin-client)).

1. `fokosRestoreSnapshot({ restoreId, bookmark })` on the source DO answers `snapshot({ bookmark })`.
2. `fokosRestoreReceive({ restoreId, snapshot, marker })` on the target DO. When `__fokos/restored` or
   `__fokos/backup/last_restore` holds this `restoreId`, the DO answers `already_received`. Else it calls
   `onNextSessionRestore(snapshot, { kvData: { "__fokos/restored": marker } })`. The marker is
   `{ restoreId, kind: "new_table", shardGroup }`.
3. The restart of the target DO ([4.2.8](#428-m7--restore-windows-and-the-admin-client)). After `not_restored`, the
   worker runs this list again from step 1.

**The steps of the marker `new_table`.** They are part of the restore and need no cut (I1).

- The runtime, in its start block before `cutIfNeeded()`:
  1. Rewrite `__fokos/identity`: `ref.doName` and `topology.shardGroup` get the names of the target. The position,
     the kind and `routingHashSeed` stay. The seed scope is the same, so the seed check passes.
  2. Delete every row of `fokos_backup_cuts`, the key `__fokos/backup/cut_boundary` and the key
     `__fokos/backup/generation` (R9). The source keeps its own.
- The host, after its migrations, in one `transactionSync`:
  1. A coordinator cancels each transaction in `CREATED` or `PREPARING`. [4.2.10](#4210-m9--restore-in-place) gives
     the statement and why it is safe.
  2. Delete `__fokos/restored` and write `__fokos/backup/last_restore`.
- Then the runtime re-arms the scheduler from `__fokos/jobs`, because the restore cleared the alarm.

A transaction in `COMMITTING` completes in the target. Its coordinator holds the positions of its participants, so it
sends the commits to the participants of the target (M2). A lock row holds the position of its coordinator, so the
stale recovery of a partition calls the coordinator of the target. A pending outbox row sends to the materialization
DOs of the target, and a materialization DO writes to the GSI partitions of the target.

**Step 3 — copy the topology admin DOs.** A topology hint changes only latency, and the topology admin DOs are not in
the cut. The job copies the current state of each topology admin DO of the source into the matching DO of the target,
with `snapshot()` at the current position. The rows are keyed by kind (M2), so they need no rewrite. The target admin
DO then deletes each repartition whose targets are all absent, with its hash nodes and its range intervals. The job
gives it the absent positions in pages. A repartition that was in progress at P and completed later can stay
`completed`. A client then sends a request to a child that answers `partition_migrating` or reads through to its
source, so the result stays correct.

**Item versions.** A new table has its own versions, so M8 sets no revision floor.

#### 4.2.10 M9 — Restore in place

An in-place restore keeps every DO name. Each DO restores its own storage from a snapshot of its own resolved
bookmark, so M9 does not need M1 and M2.

**Generations.** The generation rules of the tag check ship in M5 ([4.2.6](#426-m5--tags-and-cuts)). M9 is the
first code that raises a generation: an in-place restore gives every DO of the walk the generation `G`, also each
root and each absent DO.

**The walk set.** The walk of [4.2.8](#428-m7--restore-windows-and-the-admin-client). For an `undo`, the walk set also
holds every DO of the job that it undoes.

**The restore job DO.** The table admin creates `<table admin name>.restore.<restoreId>`, with the restore point `P`,
the new generation `G` (the generation of the table plus 1) and the kind (`in_place` or `undo`).

```sql
-- One row for each DO of the walk set.
-- state              : fenced, prepared, restarted, or completed.
-- target             : the resolved bookmark, or NULL for an absent DO.
-- pre_fence_bookmark : the bookmark of the DO just before its fence. An undo of this job restores to it.
-- max_revision       : for a partition, max(max_v, max_deleted_v, revision_floor) at its fence.
CREATE TABLE IF NOT EXISTS restore_targets (
    do_name            TEXT    PRIMARY KEY,
    kind               TEXT    NOT NULL, -- partition, gsi_partition, coordinator, materialization
    state              TEXT    NOT NULL,
    target             TEXT,
    pre_fence_bookmark TEXT    NOT NULL,
    max_revision       INTEGER
) WITHOUT ROWID, STRICT;
```

The job also stores the windows that it removes. It keeps all of this for 30 days after its fence (R17), for an undo
and for the follow-up of [4.4](#44-follow-ups).

**The states of the job.**

```
created ──► fencing ──► restoring ──► cleaning_topology ──► completed
               │
               └──► cancelled (only from fencing)
```

| Transition | Trigger | Effect |
| --- | --- | --- |
| `created → fencing` | the job starts | The table admin sets `valid_to` of the open window to now, and records that it was open. |
| `fencing → restoring` | every DO is fenced and resolved | The table admin stores `G` as the generation of the table, and removes the restore windows after P: the window that holds P ends at P. For an `undo`, it puts back the windows that the undone job removed instead. This is the point of no return. |
| `fencing → cancelled` | `cancelRestore`, or a DO resolves to a lost cut | The job removes each fence. The table admin opens a window again from the next boundary after now. |
| `restoring → cleaning_topology` | every DO is `completed` | The job starts the topology cleanup. |
| `cleaning_topology → completed` | every topology admin DO answered | When the table had an open window, the table admin inserts a window from the next boundary after now. For `in_place`, it stores this job as `latest_in_place`. For `undo`, it clears `latest_in_place`. |

**Step 1 — fence and resolve.** The job walks the table with `fokosRestoreFence({ restoreId, generation: G,
restorePoint })`. `restorePoint` is `P` for an `in_place` restore and absent for an `undo`. The DO:

1. Runs `cutIfNeeded()`. The fence is a change, so the DO then has a cut row at or after P.
2. Calls `getCurrentBookmark()`. This is the pre-fence bookmark.
3. Writes the fence under `__fokos/restore_fence`: `{ restoreId, generation: G, preFenceBookmark }`.
4. Answers the pre-fence bookmark, its resolution for P, and its names. A partition also answers
   `max(max_v, max_deleted_v, revision_floor)` from `deletion_metadata`, one row read.

When the fence of the same `restoreId` already exists, the DO answers from the stored fence. The answer is thus the
same after a lost reply. A fenced DO cannot split, so the walk is complete. `FokosShardingClient.walk` is a
best-effort walk, so the job does not use it.

For an `undo`, the target of each DO is its pre-fence bookmark from the job that it undoes. A DO with no row in that
job is absent: a split created it after that restore.

**The revision floor.** The revision floor of the job is the highest `max_revision` over all partitions and all GSI
partitions of the walk set. It is table-wide, because after P a key can move to a split child that becomes absent,
and that child issued the later revisions of the key. The fence reads absent DOs too, so their revisions count. All
DOs are fenced before any restore (R8), so the floor is final.

**Step 2 — restore each DO.** Each worker runs these calls for each of its DOs
([4.2.8](#428-m7--restore-windows-and-the-admin-client)).

1. For a present DO:
   1. `fokosRestorePrepare({ restoreId, bookmark: target, marker })`. When `__fokos/restored` or
      `__fokos/backup/last_restore` holds this `restoreId`, the DO answers `already_prepared`. Else it calls
      `snapshot({ bookmark })` on itself and `onNextSessionRestore(snapshot, { kvData: { "__fokos/restored":
      marker } })`. The marker is `{ restoreId, kind, generation: G, revisionFloor }`.
   2. The restart of the DO ([4.2.8](#428-m7--restore-windows-and-the-admin-client)). After `not_restored`, the worker
      runs this list again from `fokosRestorePrepare`.
2. For an absent DO, `fokosRestoreAbsent({ restoreId, generation: G })`. The DO calls `deleteAll()`, writes
   `__fokos/backup/generation = G` and `__fokos/backup/last_restore = restoreId`, and calls
   `ctx.abort(RESTORE_ABORT_SENTINEL)`, as `destroy` does. When `__fokos/backup/last_restore` already holds this
   `restoreId`, the DO answers `already_restored`.

The job never uses the bookmark that `onNextSessionRestore()` returns. The undo target is the pre-fence bookmark,
which the fence stores before the reply. So a lost reply cannot lose the undo target.

**The steps of the markers `in_place` and `undo`.** They are part of the restore and need no cut (I1).

- The runtime, in its start block before `cutIfNeeded()`: set `__fokos/backup/generation` to `G`. The cut rows stay:
  they describe the history before P (I5).
- The host, after its migrations, in one `transactionSync`:
  1. A partition stores `revisionFloor` in the new column `deletion_metadata.revision_floor`.
  2. For `in_place`, a coordinator cancels each transaction in `CREATED` or `PREPARING`:
     `UPDATE tc_state SET state = 'CANCELLING', results_json = ?, next_recovery_at = ? WHERE state IN ('CREATED',
     'PREPARING')`. `results_json` holds the cancel result that `cancelTransactionInStore` writes. The `tx_recovery`
     job then sends the cancels.
  3. Delete `__fokos/restored` and write `__fokos/backup/last_restore`.
- Then the runtime re-arms the scheduler from `__fokos/jobs`, because the restore cleared the alarm.

The cancel is a third writer of `CANCELLING`, next to `drivePrepare` and `runPrepareRecovery`. It is safe: it runs in
the start `blockConcurrencyWhile`, so no drive runs at the same time, and it never changes `COMMITTING`. Without it,
`driveTransaction` sends a `CREATED` transaction to `drivePrepare`, and that transaction can commit after the
restore. An `undo` restores pre-fence states, which form a consistent cut, so it cancels nothing.

The restored state has no fence. For `in_place`, the resolved cut row comes before the fence write. For `undo`, the
pre-fence bookmark comes before the fence write.

**Revisions after a restore.** Every write that issues a revision uses `max(computed revision, revision_floor + 1)`.
This covers a new row (`max_deleted_v + 1`), an update (`v + 1`), a delete revision (`v + 1`) and `commitLocal`, at
the sites that raise `max_v` ([4.2.5](#425-m4--max_v)) and at the delete paths. A migration merges `revision_floor`
with `MAX`, as it merges `max_deleted_v`, so a split child keeps the floor. The first write of an item after a restore
jumps above the floor, and each later write adds 1.

**The fenced DO.** While `__fokos/restore_fence` exists:

- The DO serves `apiGetItem`, `apiBatchGetItems`, `apiQueryItems` and `fokosStatus` from its state before the fence.
- It refuses every other operation with the retryable `table_restoring`, except the restore RPCs. This covers
  `txReadForTransaction` and `txReadSnapshot`, because a transactional read must not compare two histories.
- It does not cut, and its write guard refuses every write except the restore RPCs.
- `callPeer` refuses every outgoing call. The scheduler and the TTL sweep do not run.
- From the restart until `fokosAfterRestore` completes, the DO serves nothing.

A fenced DO changes nothing and sends nothing (R16). So each message that a DO handled before its fence came from a
sender that was not fenced yet, and the pre-fence states of one job form a consistent cut. An undo restores to them.

**Restored and fenced DOs at the same time.** A restored DO serves at once, also while other DOs are still fenced.
Its call to a fenced DO gets `table_restoring` and retries. A fenced DO and a DO that has not restored yet answer with
the old generation, so `callPeer` of a restored DO treats the answer as a failed call. A message of the old history
that is still in flight has the old generation, 0 before the first restore, so a restored DO refuses it with
`table_generation_stale`. The restore RPCs carry no generation, so the generation rules do not refuse them.

**Absent DOs and stale routes.** An absent non-root partition has no identity, so it answers
`hash_partition_not_initialized`. The client cuts the topology hint and resolves the key from a shallower partition,
as it does today. An absent root partition bootstraps on its next request, as a new root does today, and it keeps the
generation `G` that the restore stored. An old-history `fokosInit` to an absent DO has a lower generation, so the
absent DO refuses it. A `fokosInit` of the restored history has the generation `G` and initializes it. The name of an
absent DO comes back when its restored parent splits again.

**Step 3 — the topology cleanup.** A topology hint changes only latency, so the restore does not restore the topology
admin DOs. The job calls `fokosTopologyRestore({ restoreId, generation: G, absentPositions })` on the admin DO of each
shard group, with `absentPositions` in pages. The admin DO:

1. Stores `G` as its generation. From then on it refuses each report of a lower generation with
   `table_generation_stale`. A report of the old history that is still in flight cannot add a node back.
2. Deletes each repartition whose targets are all absent, with its hash nodes and its range intervals.
3. Raises the topology epoch. A client cache keeps a view when the fetched version is older. A higher topology epoch
   makes the caches take the cleaned view.

For each promoted routingHash whose range root is absent, the job also resets the routingHash admin DO, as `destroy`
does. The restored partitions then send again the admin outbox rows that they had pending at P. A repartition that was
in progress at P and completed later can stay `completed` in the admin DO. A client then sends a request to a child
that answers `partition_migrating` or reads through to its source. The result stays correct, and the admin DO learns
the real state from the next report of the source.

**Undo.** `undoRestore(restoreId)` works only for `latest_in_place`. It starts a job of the kind `undo` with the next
generation. The job fences the walk set, computes a new revision floor from the fenced state, and restores each DO to
its pre-fence bookmark from the undone job. The writes that the table accepted after the undone restore are lost. The
cut rows of each DO come back with its pre-fence state, and the table admin puts back the removed windows, so the
restore points of the undone history are restorable again. An undo is possible for 30 days minus
`RESTORE_WINDOW_MARGIN_MS` after the fence of the undone job, while its pre-fence bookmarks are restorable.

#### 4.2.11 Why a set of cuts is consistent

**Inside one DO.** Each bookmark is a state that the DO stored. FokosDB recovers from each stored state after a crash,
so each state alone is valid.

**Across DOs.** Take a message M from sender S to receiver R, and a restore point P.

- When R changes state because of M after its P-cut, the P-state of R does not hold M. The P-state of S can hold the
  send or not. Both are valid. When S holds the send, S retries from its stored state after the restore, because each
  FokosDB protocol writes before it sends ([1.2](#12-the-current-system)).
- When R changes state because of M before its P-cut, the current boundary of R was below P. Then the tag of M was
  below P, because R adopts each tag before it changes state (R3). By I2, S sent M before any change at a boundary
  of P or later. So the P-state of S holds the send.

So each effect in a set of P-states has its cause in the same set. This is the consistency condition of
[Chandy and Lamport](https://lamport.azurewebsites.net/pubs/chandy.pdf). Responses are messages too, so the rule also
covers the accepts of a prepare. `callPeer` is the mechanism that applies R3 to responses.

**The case that matters for 2PC (I4).** A coordinator writes `COMMITTING` only after it has an accept from each
participant. Take a participant that wrote its lock after its P-cut. By I2 its accept has a tag of P or more. The
coordinator adopts the tag in `callPeer`, so its write of `COMMITTING` cuts first. So `COMMITTING` is in the P-state
of the coordinator only when each lock is in the P-state of its participant.
[Appendix B.1](#b1-a-transaction-with-an-idle-participant) shows the steps.

**Clock skew and mixed periods.** No step above uses a clock bound or a common period. Skew moves the cut of a DO in
real time. A sender with a longer backup period tags an older boundary, so a receiver can cut later. Neither makes a
set of cuts inconsistent. [Appendix B.2](#b2-clocks-on-two-sides-of-a-boundary) shows the steps.

#### 4.2.12 Performance and cost

| Work | Cost |
| --- | --- |
| A tag | One time and one integer in each message, about 40 bytes of serialized data. |
| A cut | At most one for each DO in each backup period in which the DO handles an event: one insert, one `getCurrentBookmark()`, one update. All other events of the DO wait for one `getCurrentBookmark()`, at most `BOOKMARK_TIMEOUT_MS`. TODO: measure. |
| An idle DO | Nothing. |
| The cut rows | With a 30-minute period: at most 48 rows each day and 1,488 rows in 31 days. At about 100 bytes each, about 150 KB for each DO. |
| `max_v` | One comparison in memory for most writes. One more row write only when a write raises the highest `v` of the partition. |
| Position references | No cost on a request. A DO builds a peer name from a string it already has. |
| The table admin | No work for each backup period. |
| The client boundary wait | Off by default. When on: at most 0.11% of the calls with a 30-minute period, at most 2 s each ([4.2.7](#427-m6--the-client-boundary-wait)). |
| The job DO and its workers | About 200 bytes for each DO of the walk set, in the worker of the DO. |
| A restore into a new table | It wakes every DO of the source. Each copied DO downloads its storage. The target doubles the storage until the user destroys one of the two tables. |
| An in-place restore | It wakes every DO of the table. Each DO restores from its own snapshot. The storage does not grow. |

**Unavailability.** A restore into a new table refuses nothing: the source keeps serving, and the target serves when
it completes. An in-place restore refuses writes and transactional reads on a DO from its fence until its
`fokosAfterRestore` completes, and plain reads from its restart. All DOs are fenced first (R8). With the fan-out,
every DO starts its restore when the fence walk completes, so each DO refuses writes for about the fence walk and its
own restore: about 20 s at p90 and 50 to 65 s at p99 after the walk. The slowest DO of the table ends the job.

| DOs | Restores at the same time | The last DO refuses writes for about |
| --- | --- | --- |
| 10,000 | 32, from one DO | 10,000 × 20 s / 32 ≈ 1.7 h |
| 10,000 | 256 | 10,000 × 20 s / 256 ≈ 13 min |
| 10,000 | all, with the fan-out | the fence walk and the slowest restore, about 1 to 2 min |

TODO: measure the fence walk, and the restore time above p99, for a table of 10,000 DOs.

#### 4.2.13 Deployment and rollback

- **M1** changes the seed of every existing shard group. It is a breaking change of the stored data, as the
  routing-hash RFC was: it deploys to new namespaces. An existing partition with the old seed refuses all work with
  `partition_init_failed`, so no write goes to a wrong place. M1 ships first, before more tables store data.
- **M2** reads old full names as positions, and its migrations rewrite the fence rows and the topology rows. The
  built names of a table that no restore copied are the names of today.
- **M3** and **M4** change no wire format and no behaviour.
- **M5** is compatible with older code: a message with no tag is accepted. It ships with `backups` off. The operator
  turns it on in the host config, deploys, and calls `enableBackups()` (M7) when the deploy reaches 100%.
- **M9** adds RPCs. The operator runs the first in-place restore after the deploy of M9 reaches 100%. Every DO then
  has the generation rules of M5.
- Before a deploy turns `backups` off, the operator calls `disableBackups()`.
- After a release with a storage change that older states cannot migrate, the operator calls `disableBackups()` and
  then `enableBackups()`. A restore point before that release can hold a state that the new code cannot read.
- A restored DO can hold an older schema. The migrations of the current code upgrade it at its start.
- Capture needs only PITR. M8 and M9 need the flag `durable_object_snapshots` in `compatibility_flags`, and
  Cloudflare must enable the flag for the account.
- `deleteAll()` clears the alarm from the compatibility date 2026-02-24. The wrangler projects use 2026-05-23.

**A change of the backup period.** Boundaries are times, so cut rows and tags of two periods compare correctly. A
restore point is correct only when every DO that changed state near it cut at it. So:

- When the new period divides the old period, every old boundary is also a new boundary. The operator deploys and
  calls `enableBackups()` at 100%. The old window stays correct during the deploy, and the new window starts at that
  call.
- For any other change, the operator calls `disableBackups()` before the deploy and `enableBackups()` at 100%. The
  table has no restore points during the deploy.

Restore points of the earlier windows stay restorable for 30 days, with their own period.

#### 4.2.14 Testing

- **M1.** The tests of [the seed-scope RFC](2026-11-17-routing-hash-seed-scope.md): pinned seeds for each seed
  scope, the same seeds for two tables, and different seeds for the groups of one table.
- **M2.** A fast-check property: for each generated position, hash paths and range bounds with edge keys included,
  `positionOf(nameIn(groupA, position)) === position`, and the name in group B equals the name built in B. A test file
  that passes a stored full name to a stub helper must fail the type check. A reader of a row from before M2 gets the
  same peer as before.
- **M3.** A test file that calls a method on a peer handle directly must fail the type check.
- **M4.** `max_v` at each write site that issues a `v`, the merge into a split child, and an absent-item read
  transaction that does not abort when another key changes.
- **M5.** `boundaryAt` and the tag check as pure functions, the generation rules included. Runtime tests with a fake
  backup storage: the cut check at each event entry and each write site, the write guard, I3 after a restart, and a
  lost cut. The fake is a `FokosBackupStorage` under `test/`. For each bookmark it keeps a copy of the SQL tables and
  the KV keys of the DO. A snapshot of the fake can restore into the same DO or travel to another DO of the test,
  which writes the copy and `kvData` at its next start. The production code is the same, and only the dependency
  differs.
- **M7.** The windows, the resolution rule and the walk, with splits, promotions, GSIs and idle DOs.
- **M8.** The restore job, with transactions in flight. An isolation test: after a workload, the test restores table A
  into table B, and then runs the workload and the recovery on B. The test namespace records the name of each stub
  that the code makes. No name may belong to a shard group of A.
- **M9.** The fenced DO, the generation rules against a message of the old history, the absent path, the topology
  cleanup, the revision floor, and the undo.
- **P model.** Add boundaries, tags, clocks with skew, cuts and a restore to `packages/fokosdb/formal/p`. Add the
  monitor `RestoredCutIsConsistent`: after a restore to P, no transaction is in `COMMITTING` without each of its locks
  or commits. Add the seeded defect `tcBugAdoptTagAfterWrite`, which must fail with that monitor.
- **Deployed test.** On Cloudflare, with the flag: run a transfer workload over many partitions and a GSI, with splits
  and promotions, and keep some partitions idle. Restore into a new table, and restore in place. Check that the sum of
  the balances is unchanged, that each transaction is complete or absent, that each GSI matches the base items, and,
  after an in-place restore, that every revision is above every revision before it. Then undo the in-place restore,
  and check the same.

### 4.3 Open questions

#### 4.3.1 Concurrent calls of one invocation

The limits pages do not say whether a call to a Durable Object stub counts for the six connections or for the
subrequests of one invocation ([1.3](#13-platform-facts)). A restore call stays open for 20 to 65 s. The answer sets
`RESTORE_WORKER_CONCURRENCY` and `RESTORE_WORKER_DOS`. With six calls for each worker, a table of 10,000 DOs needs
about 1,700 workers to restore all its DOs at the same time. With no such limit, a few workers are enough.

### 4.4 Follow-ups

**A restore to a point of an abandoned history.** After an in-place restore R to P, a later version can restore to a
point Q between P and R. This RFC stores what that needs, so the follow-up changes no stored data:

1. R17 keeps the walk set of R, the pre-fence bookmark of each DO, P, the generation and the removed windows. The
   removed windows tell which Q is valid.
2. The cut rows of the abandoned history are in the pre-fence state of each DO. A bookmark of an abandoned history
   stays restorable for 30 days ([1.3](#13-platform-facts)). So the follow-up copies each pre-fence state into a
   scratch DO with `snapshot()`, reads its cut rows, resolves Q, and deletes the scratch DO.
3. The restore itself is an in-place restore with a new generation and the resolved bookmarks as targets. A DO with
   no row in the walk set of R is absent.

The follow-up adds the scratch copies and a new kind of job. Each scratch copy downloads the storage of one DO, so it
costs about one copy of the table.

---

## 5. Alternative options

**One bookmark for each DO at one time.** Each DO takes `getBookmarkForTime(t)`. This tears 2PC
transactions ([P1](#14-problems)). Rejected.

**Controller marker cuts.** A controller increases a counter and walks every DO with a marker. Rejected because each
backup period wakes every idle DO ([P2](#14-problems)). [Appendix A](#appendix-a-controller-marker-cuts) describes it
and compares it with this RFC.

**A transaction timestamp T in the future, sent to all partitions.** Each partition cuts before its first write with a
timestamp above T. A broadcast of T wakes every DO, as in Appendix A. A T from a time grid gives the boundary cut of
this RFC. A trigger by the timestamp of a write also needs a rule for each protocol: a participant refuses a prepare
at or below T after its cut, a coordinator decides each transaction at or below T before its cut, and a migration
page and an outbox export need their own rules. Tags give one rule for all protocols. Rejected.

**Fence and restore the DOs of an in-place restore in groups.** Each group gets its own fence and its own revision
floor, so a DO waits only for its group. Rejected: no limit applies to the number of restores, so the fan-out of
[4.2.8](#428-m7--restore-windows-and-the-admin-client) already gives each DO about its own restore time with one fence
for all DOs. Groups would also need a revision floor for each group, a new proof of the undo, and a change of the
generation rules, because a DO that is not fenced adopts a higher generation.

**A global write freeze.** Every DO delays its writes until every DO has taken a bookmark. It keeps causal order with no
clock bound. Rejected because each period pauses the writes of the whole table and wakes every DO.

**An in-place restore through a new table instance.** The job copies the table into new DOs with internal names, then
fences the old DOs and makes the new instance current. Rejected: it doubles the storage, a cold isolate pays one more
DO hop to find the current instance, and a copy into another DO is not faster than a restore of a DO into itself.

**A copy of the cut history out of each DO and back.** The job copies the cut rows of each DO out before an in-place
restore and writes them back after it, and each cut row and each resolution carries a generation. It gives a restore
to any point of an abandoned history in the first version. Rejected: the undo returns to that history with no copy,
and the follow-up of [4.4](#44-follow-ups) adds the rest with no change of the stored data.

**A seed in the table config, learned by the client.** A client of a restored table learns the seed from the first
refusal of a partition. Each cold isolate then pays one more round trip. Rejected: the seed scope gives the same seed
with no configuration.

**A boundary as an index.** A tag carries `floor(now / period)`. Rejected: a change of the period changes the numbers,
so two periods cannot compare, and the period could never change.

**Refuse every request with a lower boundary.** This makes each sender cut early. It is not necessary for consistency
([4.2.11](#4211-why-a-set-of-cuts-is-consistent)), and it costs one more round trip for each sender at each boundary.
Rejected.

**Clamp a tag that is too far ahead.** This keeps the data of a message and drops its boundary, which breaks I2. The
tag check refuses the message instead.

**A scan of `items` for the revision floor.** Rejected: FokosDB never scans a whole table, and a 10 GB partition
cannot answer a full scan in one bounded call. `max_v` gives the value in one row read.

**A floor from the generation.** Every revision after generation `G` is at least `G × 2^40`. It needs no column.
Rejected: versions jump by about 1.1 × 10^12 after each restore, and the scheme limits a key to 2^40 writes for each
generation and the table to about 8,000 restores before 2^53.

**Backup operations on `FokosDB`.** The data client would hold `restoreTable` next to `getItem`. Rejected: the admin
operations are destructive and rare, so `FokosDBAdmin` holds them.

---

## 6. Frequently asked questions

**A DO was idle for 40 days. Which bookmark does a restore use?** It has no cut row at or above P, so it has changed
nothing since P. The resolve request or the fence cuts it, and that bookmark is new.

**Does a restore lose the writes after P?** A restore into a new table loses nothing: the source keeps them. An
in-place restore discards the writes after P, and an undo brings them back. An in-place restore also loses the
idempotency records after P. A client that retries a token whose transaction committed after P runs the transaction
again.

**Why are the restore points between P and an in-place restore gone after it?** Each DO keeps only the cut rows of
its current history. An undo brings that history back with its cut rows and its windows. [4.4](#44-follow-ups) shows
how a later version restores to such a point.

**Why do item versions jump after an in-place restore?** The revision floor lifts every new revision above all
revisions before the restore. Versions always increase under one table name, but the first write of an item after a
restore can add more than 1. A new table has its own versions and no floor.

**What happens to an item whose TTL passed between P and the restore?** The restored DO holds the item. The TTL sweep
deletes it after the restore.

**Are the GSIs consistent with the base items?** Yes. The materialization DOs and the GSI partitions are in the
restore set. A GSI can lag the base items at P, and the outbox of each restored partition sends the missing changes
again.

**What does a push target see?** A push target is outside FokosDB, so a restore does not rewind it. A target that
drops older revisions keeps its images from before the restore until each item changes again. A push is also not a
result of `FokosDB`, so the boundary wait does not apply to it.

**Why is `boundaryWait` off by default?** It adds up to 2 s to some calls near each boundary. Without it, a restore
keeps the causal order inside one client instance. An application that passes results between requests, isolates or
queues, and needs that order in a restore, turns it on.

**Do reads make a DO cut?** Yes, once in each backup period in which the DO handles any event. Only a DO that is not
woken does nothing.

**What does a clock that is wrong by more than δ break?** It does not break atomicity. Near a boundary, its messages
get `clock_skew` refusals. A DO with a fast clock cuts early, so its restore points move earlier in real time.

**What happens when PITR fails?** The cut is marked `lost` and the DO keeps serving. A restore point that resolves to
a lost cut cannot be restored, and the job cancels itself before its point of no return.

**What happens to requests during an in-place restore?** A fenced DO serves plain reads from its state before the
fence, until its restart. It answers `table_restoring` to the other operations, and the client retries. A restored DO
serves at once. During a restore, a query over several partitions can mix restored and old data.

**Why does the restore also restore the coordinators?** The coordinator ledger decides each transaction. A restored
partition with a lock needs the coordinator state of the same restore point.

**Why do M1 to M4 come first?** They do not depend on backups, and each one is useful alone. M1 is a breaking change,
so it must ship before more tables store data. M2 removes the table name from the stored state. M3 gives one place
for every call between DOs. M4 gives the highest revision of a partition with no scan.

---

## 7. References

References:

- `AGENTS.md`
- `docs/agent-plans/2026-11-01-routing-hash.md`
- `docs/agent-plans/2026-11-17-routing-hash-seed-scope.md`
- `docs/agent-plans/2026-09-17-unified-repartition-flow.md`
- `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`
- `docs/agent-plans/2026-09-26-configuration-surface.md`
- `docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md`
- `docs/agent-plans/2026-10-31-alarm-job-and-init-failure-safety.md`
- `docs/research/atc22-elhemali-dynamodb.md`, Section 5.5
- [SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Durable Object state, `abort()`](https://developers.cloudflare.com/durable-objects/api/state/)
- [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [workerd `actor-state.h`](https://github.com/cloudflare/workerd/blob/main/src/workerd/api/actor-state.h)
- [workerd `snapshot.h`](https://github.com/cloudflare/workerd/blob/main/src/workerd/api/snapshot.h)
- [Chandy and Lamport, Distributed Snapshots](https://lamport.azurewebsites.net/pubs/chandy.pdf)

---

## Appendix A. Controller marker cuts

A different capture method with the same restores. It is recorded here for comparison.

**The method.**

1. The table admin runs an alarm in each backup period. It increases a counter by 1 and stores it.
2. It sends a marker with the new counter to each root of each shard group.
3. Each DO that gets a message with a higher counter cuts before it handles the message, in
   `blockConcurrencyWhile`. The marker, a request, a response and an error are all messages.
4. Each DO answers the marker with its bookmark and the children in its cut state. The table admin sends markers to
   the children.
5. When each DO of the walk has answered, the table admin stores the manifest of `doName → bookmark` for the counter.
   When a DO cannot answer, the backup fails.

Only the table admin increases the counter. Clocks have no part in it.

**The freeze variant.** Each DO that gets the marker delays all its responses until a lease ends or until the table
admin confirms that every DO has cut. The table admin checks that the slowest answer arrived before the earliest lease
ended. This keeps causal order across requests with no clock bound. It pauses the table for the time of the whole
walk.

**Comparison with lazy clock-boundary cuts.**

| Property | Lazy clock-boundary cuts (this RFC) | Controller marker cuts |
| --- | --- | --- |
| Idle DO in each backup period | No work | One wake, one write, one bookmark |
| Active DO in each backup period | One cut | One cut |
| Controller in each backup period | No work | A walk of every DO |
| Real-time precision of a cut | Clock skew, at most δ | Time of the marker walk, about 4 network round trips for a tree of depth 4 |
| Causal order across requests | With `boundaryWait` and skew at most δ | Only with the freeze variant, which pauses writes |
| Dependence on clocks | Precision and causal order across requests | None |
| A bad clock | Refused by the tag check, and restore points of that DO move | No effect |
| Proof of a complete backup | At restore time only | A complete manifest at the end of each backup period |
| Where the bookmarks live | In the cut rows of each DO | In the table admin |
| Restore point on demand | The latest boundary | Any time |
| Cut check sites | Each event entry, each write site after an `await`, and `callPeer` | Message receipt and `callPeer` |
| Capture failure | A lost bookmark loses some restore points | An unreachable DO fails the backup |

---

## Appendix B. Worked examples

The table has 3 root partitions and `hashSplitN = 2`, with two branches that split three levels deep:

```
R0 (router)                      R1 (router)                    R2 (leaf, idle)
├─ R0.0 (router)                 ├─ R1.0 (leaf, busy)
│  ├─ R0.0.0 (router)            └─ R1.1 (router)
│  │  ├─ R0.0.0.0 (leaf, idle)      ├─ R1.1.0 (leaf)
│  │  └─ R0.0.0.1 (leaf)            └─ R1.1.1 (router, split source)
│  └─ R0.0.1 (leaf)                    ├─ R1.1.1.0 (leaf)
└─ R0.1 (leaf, idle)                   └─ R1.1.1.1 (leaf, imports from R1.1.1)
```

R0.0.0 is `fokos.p.orders~h.0.0.0`, and its position is `h.0.0.0`. C0 is a busy coordinator and C1 is an idle
coordinator. The backup period is 30 minutes, and P is the boundary 12:00:00. P−1 is the boundary 11:30:00.

### B.1 A transaction with an idle participant

Transaction T touches R0.0.1 and R1.1.1.0. R1.1.1.0 was idle since 11/01.

| Time | Event | P-state |
| --- | --- | --- |
| 11:59:58 | C0 writes `PREPARING` and sends `txPrepare` with tag P−1. | C0 holds `PREPARING`. |
| 11:59:59 | R0.0.1 is at P−1. It writes lock L1 and answers with tag P−1. | R0.0.1 holds L1. |
| 11:59:59 | C0 stores the accept of R0.0.1. | C0 holds one accept. |
| 12:00:02 | The prepare reaches R1.1.1.0. Its clock is past P and its last cut is from 11/01. The `dispatch` entry cuts. Then it writes lock L2 and answers with tag P. | R1.1.1.0 holds no lock. |
| 12:00:02 | `callPeer` of C0 adopts tag P. Its write of the accept cuts first. Then it writes `COMMITTING` and sends `txCommit` with tag P. | C0 holds `PREPARING` and one accept. |
| 12:00:03 | R0.0.1 adopts tag P and cuts before the commit. | R0.0.1 holds L1, not committed. |

After a restore to P, in place or into a new table, the restored C0 moves T to `CANCELLING`, and the recovery
releases the restored L1. T is absent in all restored DOs.

When the prepare is still in flight at an in-place restore, the fence of R1.1.1.0 cuts it in its current state, with
no lock. The late prepare then gets `table_restoring`, or `table_generation_stale` after the restore.

When the clocks of C0, R0.0.1 and R1.1.1.0 are all still before P, T can commit before the cut in all three DOs.
Then the P-state of C0 holds `COMMITTING`, and the restored C0 completes T from the restored locks. The clocks decide
on which side of P the transaction is. They cannot split it.

### B.2 Clocks on two sides of a boundary

R1.0 runs 50 ms fast. R0.0.0.1 runs 50 ms slow.

- At real 11:59:59.980, a write W1 reaches R1.0. Its clock reads 12:00:00.030, so W1 is after its cut.
- At real 12:00:00.020, an unrelated write W2 reaches R0.0.0.1. Its clock reads 11:59:59.970, so W2 is before its cut.

The P-states exclude W1 and hold W2. No write depends on the other, so the state is valid. The precision loss is
within the skew.

When a client writes W1, then W2 in the same request, the answer of W1 has tag P. The client sends W2 with tag P, and
R0.0.0.1 cuts before W2. With a new client for each request, `boundaryWait` gives the same result
([4.2.7](#427-m6--the-client-boundary-wait)).

The restore compares no clocks. Each DO resolves its own cut rows by boundary.

### B.3 A bad clock

A client clock runs 3 minutes fast. At real 12:28:00 it reads 12:31:00, so it tags the boundary 12:30:00.

- R1.0 reads 12:28:00. That is before `12:30:00 − δ` = 12:29:59, so R1.0 refuses with `clock_skew`. The client fails
  for 3 minutes before each boundary, and the logs show the fault.
- Without the check, R1.0 would cut at 12:28 for the boundary 12:30 and send the tag to its peers. The restore point
  12:30 would then give their state at 12:28.
- A check that only refuses a tag more than one backup period ahead accepts this tag. It detects only a clock that is
  more than 30 minutes fast.

The idle DOs R2, R0.1, R0.0.0.0, C1 and the routers get no message in these examples. They cut nothing and do no
work. A restore resolves each of them with the cut of its resolve request or of its fence.
