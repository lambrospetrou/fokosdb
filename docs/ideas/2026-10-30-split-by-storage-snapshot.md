# Exploration — Partition splits with the Durable Object storage snapshot API

**State:** Exploration
**Date:** 2026-10-30
**Author:** Lambros Petrou
**Status:** Nothing is built. The word "today" describes the code at commit `b5ce9bd`.

## Table of Contents

- [1. Summary](#1-summary)
- [2. The snapshot API](#2-the-snapshot-api)
- [3. The split today](#3-the-split-today)
- [4. A split by clone and prune](#4-a-split-by-clone-and-prune)
  - [4.1 Source steps](#41-source-steps)
  - [4.2 The restore call](#42-the-restore-call)
  - [4.3 The child finds its role](#43-the-child-finds-its-role)
  - [4.4 The adopt transaction](#44-the-adopt-transaction)
  - [4.5 The prune phases](#45-the-prune-phases)
  - [4.6 The write gate and the early split](#46-the-write-gate-and-the-early-split)
  - [4.7 Each table of the clone](#47-each-table-of-the-clone)
  - [4.8 The other two hosts](#48-the-other-two-hosts)
- [5. Measurements: space after a prune](#5-measurements-space-after-a-prune)
- [6. What gets simpler](#6-what-gets-simpler)
- [7. What gets harder](#7-what-gets-harder)
- [8. Key promotions](#8-key-promotions)
- [9. Recommendation](#9-recommendation)
- [10. Open questions](#10-open-questions)

## 1. Summary

With `storage.snapshot()` and `storage.onNextSessionRestore()`, a split source can give each child an exact copy of
its storage at the cutover. The child then deletes the rows that it does not own. This removes the page-pull import:
the migration cursor, the four host streams, the read-through, `validatePage`, the overrides phase, and the import
gate that refuses writes for the whole copy.

The change is not free. Four facts decide the design:

1. **A random delete of half the rows frees little space.** A hash child of a 2-way split keeps 82% to 94% of the
   `databaseSize` of its parent when items are 100 B to 500 B (section 5). A copy keeps 50%. Thus the size
   trigger of the split must not read `databaseSize` alone on a pruned child, or the child splits again soon, or
   at once.
2. **The prune costs `(hashSplitN - 1)` row deletes for each row of the parent**, summed over all children. The
   import costs one row write for each row. For `hashSplitN = 2` the two are equal. For `hashSplitN = 4` the prune
   costs 3 times more.
3. **A restore takes effect only when the child restarts**, and a restart fails the call that asked for it. The
   source cannot read the result of the restore call, so the protocol needs a separate confirmation.
4. **Local `workerd` does not implement snapshots.** The storage back-end of `workerd` throws "This Durable
   Object's storage back-end does not implement snapshots." Thus `vitest` cannot run the new path.

The recommendation (section 9) is: use clone and prune for hash splits and range splits, keep the copy flow for key
promotions at first, and make the split decision of a pruned child read the logical size of its rows.

## 2. The snapshot API

Source: `cloudflare/workerd` at `0c4fe08`, `src/workerd/api/actor-state.{h,c++}`, `snapshot.{h,c++}`, and the
compatibility flag `durable_object_snapshots` (experimental) in `compatibility-date.capnp`.

- `storage.snapshot(options?: { bookmark?: string }): Promise<DurableObjectSnapshot>` captures the storage at
  `bookmark`, or at the current position when the option is absent.
- `DurableObjectSnapshot` is an opaque handle with no methods. It can only go over Workers RPC. Structured clone
  and storage refuse it: "DurableObjectSnapshots cannot be cloned or stored. They can only be transferred over RPC."
  It is an `IoOwn` capability, so it lives in the I/O context that holds it.
- `storage.onNextSessionRestore(target: DurableObjectSnapshot | string): Promise<string>` arranges that the next
  session restores the snapshot (or a bookmark string). It returns a bookmark of the state just before the
  restore, which can undo it.
- The header comment of `onNextSessionRestoreBookmark` says: "It is up to the caller to force a restart in order
  to complete the restoration, for instance by calling state.abort() or by throwing from a
  blockConcurrencyWhile() callback."
- The local back-end (`ActorCacheInterface` in `io/actor-cache.h`) throws for both `captureBookmarkSnapshot` and
  `onNextSessionRestore`. Only the production back-end can implement them.

Facts that the source does not state, and that the design must not assume (section 10): whether a restore
replaces the alarm, how long a bookmark stays valid for `snapshot({ bookmark })`, whether a restore is cheap for a
large database, and whether a pending restore survives `deleteAll()`.

## 3. The split today

`src/sharding/repartition-flow.ts` holds two state machines:

```
source: (none) -> queued -> planned -> cutover -> completed -> cleaned
target: fokosInit -> awaiting_data -> importing -> imported -> active
```

1. `queue` writes the repartition row and the plan head (`__fokos/repartition/<id>/plan/00000001`).
2. `#plan` writes one `fokos_repartition_targets` row for each child.
3. `#initializeTargets` calls `fokosInit` on each child. The child writes `__fokos/identity`, `__fokos/policy` and
   `__fokos/import` (`awaiting_data`).
4. `#cutover` moves the row to `cutover`. From this moment the source is a router (`routerRole()`), and its data
   tables do not change: the TTL sweep, the stale-transaction job and the export do not run on a router.
5. `#notifyStart` calls `fokosStartImport`. Each child pulls pages with `fokosMigrationPull`: first the route
   overrides, then the host streams `items`, `pending_tx`, `producer_fences` and `outbox`
   (`src/shared/partition/fokos-migration-host.ts`). Each page commits with its cursor.
6. While a child imports, a read goes to the source through `fokosExecuteLocal`, and every other operation answers
   `partition_migrating`.
7. The child writes `imported`, then sends `fokosMigrationAck`. The last acknowledgement moves the source to
   `completed`. `cleanupSourceStep` deletes the lock copies and the outbox copies. A split source keeps its item
   rows for life.

The source is frozen from step 4. The clone design depends on this fact: a snapshot taken at any time after the
cutover holds the same data rows as a snapshot at the cutover.

## 4. A split by clone and prune

```
source: (none) -> queued -> planned -> cutover -> completed -> cleaned          (unchanged)
target row:  pending -> restore_sent -> restored                                 (replaces initialization)
child:  (empty) -> restore scheduled -> restart -> adopt -> pruning -> pruned -> active
```

### 4.1 Source steps

1. `queue` and `#plan` stay. `#plan` also stores the actor ID of each target in a new column
   `fokos_repartition_targets.actor_id`, and the actor ID of the source on the repartition row
   (`source_actor_id`). The host gives both: `stub(ctx, doName).id.toString()`, as `#recordCompletion` does today.
   A stub is not an RPC, so this costs no I/O.
2. `fokosInit` goes. The child gets its identity from the clone (section 4.4). `beforeCutover` stays.
3. The cutover moves the row to `cutover` in one transaction, as today. The source then reads
   `await storage.getCurrentBookmark()` and stores it on the row (`snapshot_bookmark`). A crash between the two
   writes is safe: the next step reads a new bookmark, and the data rows are frozen.
4. A new step `#restoreTargets` takes up to six due targets, as `#notifyStart` does. For each target it moves the
   row to `restore_sent`, then calls `storage.snapshot({ bookmark })` and `peer.fokosRestore(...)` with the handle.
   Every child gets an image of the same bookmark, so the retries and the siblings get identical images.
5. A target becomes `restored` only when the child confirms it (section 4.2). The ack of today stays: the child
   acknowledges when its adopt transaction is durable, and the last ack moves the source to `completed`.
6. `cleanupSourceStep` stays as it is.

### 4.2 The restore call

`fokosRestore({ repartitionId, source, snapshot })` on the child:

1. When the child has an identity, it does not restore. If its stored import record names this repartition and
   this source, it answers `restored`. Otherwise it throws `partition_context_mismatch`. This rule is the guard
   against the worst failure of the design: a second restore after the child accepted a write deletes that write.
2. When the child has no identity, it calls `await storage.onNextSessionRestore(snapshot)` and then
   `ctx.abort()`. The call fails at the source with the abort error.

The source treats every result of `fokosRestore` the same way: it calls `fokosStatus` on the child. A child that
answers with its identity and an import record of this repartition is `restored`. A child with no identity gets a
new `fokosRestore` after a backoff. A child with any other identity is a protocol defect: the source logs it and
waits `nonRetryableRetryMs`, as `retryDelay` does today.

A repeated restore before the child accepted any write is harmless, because each image is the same bookmark.

### 4.3 The child finds its role

The runtime constructor reads the stored state in `blockConcurrencyWhile`, before the host migrations run. On a
clone, `__fokos/identity` holds the identity of the source. The constructor compares `ctx.id.toString()` with the
cloned rows:

| Condition | Role |
| --- | --- |
| No split row, or the split row is `queued` or `planned` | The stored identity is its own. |
| The split row is `cutover` or later, and `ctx.id` equals `source_actor_id` | The source. |
| The split row is `cutover` or later, and `ctx.id` equals the `actor_id` of target `i` | Child `i`: run the adopt transaction. |
| The split row is `cutover` or later, and `ctx.id` matches neither | A clone that nobody asked for: fail every request with an internal error. |

The identity stores no actor ID today, so the comparison needs the two new columns. The adopt transaction deletes
the inherited split row, so a grandchild that a child restores later sees the split row of the child, and never
the row of the grandparent.

### 4.4 The adopt transaction

One `transactionSync`, inside the constructor's `blockConcurrencyWhile`. It must stay small, because it blocks
the object:

1. Write `__fokos/identity` and `__fokos/policy` of target `i`. Build the context with `#targetContext` from the
   cloned identity of the source. A range child takes `rangeDepth` and `rangeAncestors` from the cloned plan head.
2. Replace `__fokos/import` with `{ state: "pruning", repartitionId, source, slice, cursor: null }`. The source can
   itself hold an `active` import record of an older repartition. `#putImport` refuses a change of
   `repartitionId`, so the adopt step writes the record directly.
3. Delete the inherited split row, its target rows and its plan head.
4. Delete `fokos_admin_outbox`: the source sends its own events.
5. Delete `__fokos/jobs`, `__fokos/cache/hash_arena` and `__fokos/cache/promotion_bloom`. They describe the
   source.
6. Run a new host hook `adoptClone(slice, parentActorId)` in the same transaction. FokosDB calls
   `importExportFences(parentActorId, epoch, inherited)` there, with the export epoch that the cutover raised. The
   clone holds that epoch, so no page has to carry it.
7. Arm the alarm, after the commit. Do not assume that the restore kept the alarm of the source.

### 4.5 The prune phases

The prune runs as a runtime job (`target_prune`) with a bounded step, as `target_import` does today. A new host
hook `pruneStep(phase, slice, belongsToTarget, budget): boolean` deletes at most `budget` rows and returns true when
the phase is complete. Ownership comes from `belongsToTarget`, the same predicate the import uses.

**Phase 1 — before the child serves any request (state `pruning_meta`).** The small tables whose foreign rows can
make a local decision wrong:

- `pending_transactions` and `pending_tx_info` of the keys outside the slice. Delete by key, never by
  transaction ID alone, because a transaction can lock keys inside and outside the slice. Then delete each
  `pending_tx_info` row with no lock row left. Without this step the stale job reads a deadline from transactions
  that this child does not own, and it runs again and again.
- `fokos_route_overrides`, and the promotion rows and their target rows, of the hash keys outside the slice. A
  hash child keeps the terminal promotions of its own keys as they are: today it builds the same rows from the
  overrides page.
- `fokos_range_hierarchy` rows of the hash keys outside the slice.
- `materialization_outbox` rows outside the slice.

While phase 1 runs, every operation answers `partition_migrating`, as the import gate does today. In the normal
case these tables are small, so the gate holds for one or two steps.

**Phase 2 — while the child serves requests (state `pruning`).** `items` and `key_size_estimates` outside the
slice, in batches of `cleanupPromotedKeyRows` rows. Each batch is one transaction, and the job yields between
batches. The local measurement (section 5) deleted about 20,000 rows of 100 B for each second of CPU. A 500 MB
parent with 100 B items holds about 1.4 million rows, so the child of a 2-way split deletes about 700,000 rows,
about 35 s of CPU in many steps. The step size must keep each step well under the CPU limit and must not hold the
input gate for long.

**Phase 3 — optional compaction (state `compacting`).** Section 5 shows that a move of each kept row to a new
`item_id` frees about one third of the space that the delete leaves. It costs about 3 rows written for each kept
row (the table and its two indexes). `item_id` is the link key that other tables can use later, so the compaction
is safe only while no table links to it.

**Done.** The child writes `active`. Only from this moment the split decision and the promotions run.

A child serves only the keys that it owns while foreign rows stay, so each local read must filter by ownership.
Today this holds: a point read and a group read resolve the owner first, `clipQueryToVisit` clips a range query to
the visit, and the TTL sweep, the stale job and the export read `ownerCheck()`. A promotion source already depends
on these filters, because it keeps the rows of the promoted key until its cleanup. A test must cover each read
path with foreign rows present.

### 4.6 The write gate and the early split

The source splits when `databaseSize` passes `maxSizeMb`, and admission refuses a write above
`maxSizeMb * ADMISSION_MARGIN` (1.1). Thus a child boots at between `1.0` and `1.1` times its cap. With no gate:

- The first write calls `evaluateSplit`, which reads `databaseSize`, and the child queues a split at once.
- A promotion candidate from `key_size_estimates` can name a hash key outside the slice.
- Admission accepts a write only while the child is below `1.1` times its cap, so the headroom is at most 10%.

The gate:

1. `evaluateSplit` returns `false`, and `promotionCandidates` returns nothing, until the child is `active`. The
   runtime already refuses a split while a split row exists. It needs one more condition: no prune in progress.
2. Admission stays on `databaseSize`. A write that admission refuses during the prune answers
   `partition_exceeded_database_size`, which is retryable, and the prune makes space. This keeps a hard bound on
   the physical size.
3. Optionally, the child refuses writes until phase 2 has deleted a fixed fraction of the foreign rows, for
   example until `databaseSize` is below `0.9 * maxSizeMb`. This trades write availability for headroom. Choose it
   only if measurements in production show refusals at the 1.1 limit.
4. After the prune, the split decision of the child must read the logical size (the sum of `key_size_estimates`)
   and not only `databaseSize`. Section 5 shows why: after a 2-way prune with small items the child stays at 82% to
   94% of its parent's size. When the parent was above `1.06` times its cap at the split, `1.06 * 0.94 = 1.0`, and
   the child splits again at once. Each extra level duplicates the whole partition once more and leaves the same
   bloat.

The answer to "how much must the child delete before it accepts writes" is therefore: nothing for correctness,
phase 1 for the transaction and routing state, and the split decision must wait for the whole prune.

### 4.7 Each table of the clone

| Table or key | Today (import) | Clone and prune |
| --- | --- | --- |
| `__fokos/identity`, `__fokos/policy` | `fokosInit` | Adopt: rewrite for target `i` |
| `__fokos/import` | `fokosInit`, page cursor | Adopt: replace, prune cursor |
| `__fokos/jobs`, hash arena, Bloom cache | Empty on the child | Adopt: delete |
| Plan head, split row, split targets | Not copied | Adopt: delete |
| Promotions, `fokos_route_overrides` | Overrides phase builds `cleaned` rows | Phase 1: delete outside the slice, keep inside |
| `fokos_range_hierarchy` | Not copied | Phase 1: delete outside the slice |
| `fokos_admin_outbox` | Not copied | Adopt: delete |
| `items` | `items` stream | Phase 2: delete outside the slice |
| `key_size_estimates` | Rebuilt from page sizes | Phase 2: delete outside the slice |
| `pending_transactions`, `pending_tx_info` | `pending_tx` stream | Phase 1: delete outside the slice, by key |
| `deletion_metadata` | Merged with `MAX` | Kept as it is: the clone holds the source values |
| `producer_fences` | Whole table, merged with `MAX` | Kept as it is |
| `materialization_outbox` | `outbox` stream | Phase 1: delete outside the slice |
| `ancestor_fences` | Last outbox page | Adopt hook: `importExportFences` |
| `materialization_export_state` | Default row | Kept. The epoch belongs to the actor ID, which is new, so the cloned value is harmless |

### 4.8 The other two hosts

`FokosDBTransactionCoordinatorDO` and `FokosMaterializationDO` also host the runtime. Each needs its own
`adoptClone` and `pruneStep`:

- The coordinator deletes the `tc_state`, `tc_participants`, `tc_items` and `tc_results` rows of the tokens it does
  not own, in phase 1. Its `tx_recovery` job claims transactions by `next_recovery_at` and must not drive a foreign
  transaction. Each transition checks `fokos.owns(token)` and throws `partition_migrating`, so a foreign drive
  writes nothing, but the job loops on it.
- The materialization host prunes its own tables (`change_positions`, `materialization_targets`, `target_retry`,
  `target_index_keys`, `ancestor_fences`) by its own ownership function.

## 5. Measurements: space after a prune

Method: `vitest` on local `workerd`, `PartitionStore.upsertItem` until `databaseSize` passed 8 MiB, then
`PartitionStore.deleteItem` for the rows outside one slice. The hash child keeps the hash keys whose hash modulo
`N` is 0, with random 35-byte hash keys. The range child keeps the lowest quarter of the sort-key space of one hash
key. The ratio is `(after - empty) / (peak - empty)`. "Compacted" moves each kept row to a new `item_id` in rowid
order. "Fresh copy" inserts the kept rows into an empty partition in hash-key order, as the import does. The
scripts were scratch files and are not in the repository.

| Case | 100 B | 200 B | 500 B | 1000 B | 2000 B |
| --- | ---: | ---: | ---: | ---: | ---: |
| Hash child, keep 1/2 | 93.5% | 89.1% | 82.3% | 62.9% | 53.5% |
| Hash child, keep 1/2, compacted | 73.0% | 68.1% | 61.0% | 55.7% | 53.4% |
| Hash child, keep 1/4 | 40.3% | 37.0% | 36.7% | 30.5% | 26.7% |
| Hash child, keep 1/4, compacted | 36.4% | 33.1% | 30.6% | 28.4% | 26.6% |
| Fresh copy of 1/2 | ~50% | ~50% | ~49% | not measured | not measured |

| Range child, keep 1/4 | 200 B | 2000 B |
| --- | ---: | ---: |
| Sort keys written in order | 24.9% | 25.0% |
| Sort keys written in random order | 30.8% | 23.4% |

Why: SQLite merges a B-tree page only when a delete leaves it less than about one third full. Keep 1/2 leaves most
`items` pages half full, so SQLite frees almost none of them. Keep 1/4 drops most pages below the threshold, so
SQLite merges them (this matches `2026-10-19-exploration-sqlite-space-reclamation.md`). Compaction repacks the
`items` table, but the indexes in hash-key order stay half full, so a compacted 2-way child still holds 61% to 73%.
A range child of keys written in sort-key order frees whole pages, because `item_id` order and sort-key order are
the same. `VACUUM` fails ("cannot VACUUM from within a transaction"), and `PRAGMA auto_vacuum` and
`PRAGMA incremental_vacuum` fail with `SQLITE_AUTH`.

Consequences:

- `hashSplitN = 2` with small items is the bad case. `hashSplitN >= 4` and range splits are close to a copy.
- `hashSplitN` must never change after initialization, so an existing table cannot move to a larger value.

## 6. What gets simpler

- **No page pull.** `fokosMigrationPull`, `servePage`, `importOnePage`, `validatePage`, the migration cursor, the
  page budgets (`migrationPageBytes`, `migrationPageRows`, `migrationScanRows`), and the stale-page checks in
  `importOnePage` go for splits.
- **No host streams.** `FokosMigrationHost.buildPage` and `applyPage`, and the four streams, go for splits. The
  per-row rules that they carry today come for free: `deletion_metadata` (no `MAX` merge), `producer_fences`, the
  lock rows with their `guarded_at`, the export fences, and the exact `est_row_bytes` of each row.
- **No overrides phase.** The terminal promotions of the slice are already in the clone.
- **No read-through.** The child holds its whole slice from its first request, so `fokosExecuteLocal`,
  `resolveCallerSlice` and the `read_source` path of `#whileImporting` go for splits. Reads and writes of the
  slice are unavailable only during the restore and phase 1, not during the whole copy.
- **No `fokosInit` and no `fokosStartImport`.** The child derives its identity and its slice from the clone.
- **The in-flight transactions need no protocol.** The lock rows, their coordinator references and their
  quarantine marks are in the image at one bookmark. The `transactions in flight` concern of the copy reduces to
  the rule that phase 1 deletes by key.
- **The source does less work.** It serves no pages. One snapshot call per child replaces many page reads.
- **The `item_id` copy rule goes.** A clone keeps each `item_id`. The duplicate check of `insertItemIfAbsent`
  is not on the split path.

## 7. What gets harder

- **Space after the prune** (section 5). The split trigger must read the logical size on a pruned child, or
  compaction must run, or both. Without one of them, a 2-way split with small items can split again at once.
- **Write cost.** The children of one split delete `(hashSplitN - 1)` times the row count of the parent, and each
  delete also writes its index entries (`rowsWritten` was about 3 for each row moved in the measurement). A split
  source still keeps its own rows, as today.
- **Storage held during the prune.** Each child holds a full image until its prune ends, so a split holds
  `hashSplitN + 1` times the size of the source for some time.
- **CPU.** A prune of 700,000 rows needs many bounded steps (section 4.5). Each step holds the input gate while
  its transaction runs.
- **The restore protocol.** The restore call fails by design, so the source confirms with `fokosStatus`. The
  child must refuse a restore once it has an identity. A missed guard deletes accepted writes.
- **The destroy fence.** A destroy that runs while a restore is pending can delete the storage, and then the next
  session restores the image again. `fokosPrepareDestroy` must refuse, or wait, while a target is
  `restore_sent`. The walk must treat a `restore_sent` target as a node that can hold data.
- **Role detection at boot.** Each constructor must compare `ctx.id` with the cloned rows before it serves a
  request. A defect here makes a child act as its parent, a router with no data.
- **Testing.** Local `workerd` throws for both methods, and the project does not add a production hook for a
  test. The flow cannot run in `vitest` until `workerd` implements a local back-end. A storage-level fake inside the
  test harness can cover the adopt and prune logic (start a DO, copy the rows of another DO in the test, restart
  it), but not the snapshot call or the restart.
- **An experimental flag.** `durable_object_snapshots` is experimental. A deploy needs it, and the copy flow must
  stay until the API is stable.

## 8. Key promotions

A promotion moves one hash key to a new range root. Its share is at least `promotionFraction` (0.25) of the cap,
and often less of the partition. A clone makes the range root delete every other hash key: at least 75% of the
rows, often more. One split decision can promote up to 8 keys, which makes 8 full images. The rows of a key that
grew over time spread across all `items` pages, so the range root also keeps the bloat of section 5.

The copy flow moves only the rows of the key. It stays the better choice for promotions. If promotions keep the
copy flow, the migration flow stays in the code, but it loses the overrides phase (a promotion has no overrides to
send) and the hash-child branch of `belongsToTarget`.

## 9. Recommendation

1. **Ask Cloudflare the open questions** of section 10 before any code. The alarm and the destroy questions
   decide whether the protocol is safe.
2. **Add the logical-size split decision first**, independent of the snapshot API. Make `SplitPlanner.evaluateSplit`
   read the sum of `key_size_estimates` together with `databaseSize`. This also helps today: a partition after
   heavy deletes keeps empty space inside its pages.
3. **Build clone and prune for hash splits and range splits** behind a host option, with the copy flow as the
   default until the flag is stable. Keep the source states. Replace the target initialization with
   `pending -> restore_sent -> restored`, and the target import with `pruning_meta -> pruning -> active`.
4. **Keep the copy flow for key promotions.**
5. **Document `hashSplitN >= 4`** for new tables that use clone and prune with small items, because
   `hashSplitN` cannot change later.
6. **Measure in production**: the time of a restore of a 500 MB image, the prune rate under load, and the
   `databaseSize` of the children after the prune.

## 10. Open questions

1. Does a restore replace the alarm of the target, keep the alarm of the target, or remove it? The design arms
   the alarm after the adopt step in each case, but an alarm from the image of the source can fire on the child
   before the adopt step if the platform restores it.
2. How long does a bookmark stay valid for `snapshot({ bookmark })`? The point-in-time recovery window is a limit
   on it. A split that stalls past the window must take a snapshot of the current state, which is safe only
   because the source is frozen after the cutover.
3. Does a pending restore survive `deleteAll()` in the same session? Can `onNextSessionRestore` with the undo
   bookmark cancel a pending restore?
4. What does a restore of a large database cost in time and in billed rows? Is the image shared copy-on-write
   until the child writes?
5. Can a snapshot handle travel through a second RPC hop, for example from the source to a router and then to
   the child? The design does not need it, but the handle is an `IoOwn` of the context that created it.
6. Does `workerd` plan a local back-end for snapshots, so that `vitest` can run the flow?
7. The Durable Objects limits and pricing pages were not reachable from the environment of this exploration.
   Read the CPU limit, the storage limit per object and the billing of deleted rows from
   `/durable-objects/platform/limits/` and `/durable-objects/platform/pricing/` before the step sizes and the
   cost figures of sections 4.5 and 7 are final.
