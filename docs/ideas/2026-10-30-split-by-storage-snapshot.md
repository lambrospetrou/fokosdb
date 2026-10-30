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
  - [4.9 Restore time](#49-restore-time)
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
2. **The prune costs `(hashSplitN - 1)` row deletes for each row of the parent**, summed over all children, and a
   delete is billed as a write. The import costs one row write for each row. For `hashSplitN = 2` the two are
   equal. For `hashSplitN = 4`, and for a range split with the default `rangeSplitN = 4`, the prune costs 3 times
   more.
3. **A restore takes effect only when the child restarts**, and a restart fails the call that asked for it. The
   child aborts with a sentinel message, and the source confirms with a separate call. That call pays the
   restore: a cold start of the source's size plus one upload of the image, 16 s to 160 s for 500 MB at the
   planned 50 to 100 Mbit/s. During that time the source serves the reads of the slice, and the child takes
   writes only after the source releases the slice to it.
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

The design uses these facts, which the source does not state:

- **A restore does not keep an alarm.** The restored child has no alarm until it arms one.
- **A bookmark is valid for 30 days after the source generates it.** Each child must boot from its image before
  that deadline.
- **The child calls `ctx.abort(<sentinel>)` immediately after `onNextSessionRestore`.** No other request can run
  between the two calls, so no `deleteAll()` or write can run between the schedule and the restart.
- **A restore costs one download and one upload of the database.** The first session of the child is a cold start
  of a database of the source's size, plus an upload of the image to the object storage of the child, under the
  child's own key. After the restore the child's storage does not depend on the source. Thus each child holds a
  full, separate copy until its prune ends: the platform does not share the pages.

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
target row:  pending -> restore_sent -> restore_scheduled -> restored -> released (replaces initialization and ack)
child:  (empty) -> restore scheduled + abort -> cold start with the image -> adopt -> pruning_meta
        -> awaiting_release -> pruning -> active
```

### 4.1 Source steps

1. `queue` and `#plan` stay. `#plan` also stores the actor ID of each target in a new column
   `fokos_repartition_targets.actor_id`, and the actor ID of the source on the repartition row
   (`source_actor_id`). The host gives both: `stub(ctx, doName).id.toString()`, as `#recordCompletion` does today.
   A stub is not an RPC, so this costs no I/O.
2. `fokosInit` goes. The child gets its identity from the clone (section 4.4). `beforeCutover` stays.
3. The cutover moves the row to `cutover` in one transaction, as today. The source then reads
   `await storage.getCurrentBookmark()` and stores it on the row (`snapshot_bookmark`, `snapshot_bookmark_at`). A
   crash between the two writes is safe: the next step reads a new bookmark, and the data rows are frozen.
   A bookmark is valid for 30 days. When a target is not `restored` 29 days after `snapshot_bookmark_at`, the
   source reads a new bookmark and stores it. This is safe before `completed` for the same reason: the data rows
   do not change after the cutover, and the cleanup deletes nothing before `completed`. The new bookmark only
   changes the bookkeeping columns of the targets, which the adopt transaction does not read. The source logs
   the refresh, because a split that waits for 29 days is a defect to look at.
4. A new step `#restoreTargets` takes up to six due targets, as `#notifyStart` does. For each target it moves the
   row to `restore_sent`, then calls `storage.snapshot({ bookmark })` and `peer.fokosRestore(...)` with the handle.
   Every child gets an image of the same bookmark, so the retries and the siblings get identical images.
5. A target becomes `restored` only when the child confirms it (section 4.2), and `released` only when the source
   accepts the release request of the child (section 4.2). The release replaces the ack of today: the last release
   moves the source to `completed`.
6. `cleanupSourceStep` stays as it is.

### 4.2 The restore call

`fokosRestore({ repartitionId, source, snapshot })` on the child:

1. When the child has an identity, it does not restore. If its stored import record names this repartition and
   this source, it answers `restored`. Otherwise it throws `partition_context_mismatch`. This rule is the guard
   against the worst failure of the design: a second restore after the child accepted a write deletes that write.
2. When the child has no identity but holds the destroy fence (section 7), it throws `partition_context_mismatch`
   and does not restore. The schema tables of an empty child do not count as state: each wake of the child runs
   the schema migrations, and the restore replaces them.
3. When the child has no identity and no destroy fence, it runs, inside `blockConcurrencyWhile`:
   `await storage.onNextSessionRestore(snapshot)` and then `ctx.abort(RESTORE_ABORT_SENTINEL)`. The
   `blockConcurrencyWhile` keeps every other request out between the two calls. The abort is the restart that
   applies the restore.

The source reads the result of `fokosRestore` as `fokosDestroy` reads its own abort today
(`DESTROY_ABORT_SENTINEL` in `shared/cf-utils.ts`):

| Result | Meaning | Next |
| --- | --- | --- |
| Error with `RESTORE_ABORT_SENTINEL` | The child scheduled the restore and restarted. | Target `restore_scheduled`. Confirm. |
| `restored` | An earlier attempt restored the child. | Target `restored`. |
| Any other error | Unknown: the call can have failed before or after the schedule. | Confirm. |

To confirm, the source calls `fokosStatus` on the child. This call starts the first session of the child, which
applies the restore: a cold start of a database of the source's size plus the upload of the image (section 2).
Thus the call needs a timeout of its own, much longer than an ordinary RPC timeout (section 4.9 gives the
numbers), and the source sends at most `REPARTITION_RPC_CONCURRENCY` of them at the same time. A child that answers
with its identity and an import record of this repartition is `restored`. A child with no identity gets a new
`fokosRestore` after a backoff. A
child with any other state is a protocol defect: the source logs it and waits `nonRetryableRetryMs`, as
`retryDelay` does today. A confirm that times out is retried: the restore runs again only if the child still has
no identity.

A repeated restore before the child accepted any write is harmless, because each image is the same bookmark.

A restore keeps no alarm, so a restored child that nobody calls stays asleep. The confirm call of the source is
what wakes it and runs the adopt transaction. The source keeps its own alarm on the restore step until each
target is `restored`, so the child always gets that call.

**The release.** The restore of a 500 MB image can take minutes (section 4.9). During that time the source
serves the reads of the slice, so reads stay available as they are with the read-through today:

1. While a target is not `released`, the source answers each read of that target's slice from its own rows, and
   it does not forward the read. Its rows are complete and correct for the slice: the source is frozen after the
   cutover, and no other partition can write the slice before the release. The source answers each write of the
   slice with `partition_migrating`.
2. After the adopt transaction and phase 1 (section 4.5), the child is `awaiting_release`. It serves reads, because
   it holds the same rows as the source. It answers each write with `partition_migrating`.
3. The child calls `fokosRelease({ repartitionId, target })` on the source, and retries it until the source
   answers, as `sendAck` does today. The source moves the target to `released` in one transaction. From that
   commit it forwards every request of the slice to the child.
4. After the answer, the child writes `pruning` and accepts writes.

The rule that makes this linearizable: the decision of the source to serve a read locally, and that read, run in
one synchronous block, with no `await` between them. A local handler is synchronous today, so this holds when the
release check runs inside the same block. Thus no local read of the source runs after the release commits, and
the child takes no write before the release commits.

A client that learned the route to the child and calls it directly gets the same answers: a child with no
identity answers `hash_partition_not_initialized` or `range_partition_not_initialized` and writes nothing (only a root takes an identity from a request),
a child in its first session waits for the restore and then answers as step 2.

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
7. Arm the alarm for the prune job after the commit. A restore keeps no alarm, so this is the only alarm of the
   child, and no alarm of the source can fire on the child before the adopt transaction.

### 4.5 The prune phases

The prune runs as a runtime job (`target_prune`) with a bounded step, as `target_import` does today. A new host
hook `pruneStep(phase, slice, belongsToTarget, budget): boolean` deletes at most `budget` rows and returns true when
the phase is complete. Ownership comes from `belongsToTarget`, the same predicate the import uses.

**Phase 1 — before the release (state `pruning_meta`).** The small tables whose foreign rows can make a local
decision wrong:

- `pending_transactions` and `pending_tx_info` of the keys outside the slice. Delete by key, never by
  transaction ID alone, because a transaction can lock keys inside and outside the slice. Then delete each
  `pending_tx_info` row with no lock row left. Without this step the stale job reads a deadline from transactions
  that this child does not own, and it runs again and again.
- `fokos_route_overrides`, and the promotion rows and their target rows, of the hash keys outside the slice. A
  hash child keeps the terminal promotions of its own keys as they are: today it builds the same rows from the
  overrides page.
- `fokos_range_hierarchy` rows of the hash keys outside the slice.
- `materialization_outbox` rows outside the slice.

While phase 1 runs, the child serves reads and answers writes with `partition_migrating`. The source does the
same for the slice, so a client sees no change. In the normal case these tables are small, so phase 1 takes one
or two steps.

**Phase 2 — while the child serves requests (state `pruning`).** `items` and `key_size_estimates` outside the
slice. The limits are 30 s of CPU for each invocation and 128 MB of memory for each isolate, and the prune must
stay far below both:

- **Step size.** The local measurement (section 5) deleted about 20,000 rows of 100 B for each second of CPU. A
  500 MB parent with 100 B items holds about 1.4 million rows, so the child of a 2-way split deletes about
  700,000 rows: about 35 s of CPU in total, more than one invocation can use. One step is one transaction of at
  most about 5,000 rows (about 250 ms at the local rate), so a request waits at most about that long behind it.
  The production rate can differ, so the step also stops at a time budget.
- **Pass size.** One alarm pass runs steps for at most a few seconds of wall time and then arms the alarm for the
  next pass, as the stale job stops at `alarmRecoveryBudgetMs` (10 s) today. A request between steps runs in
  between, because each step yields.
- **Memory.** The step reads keys, never item data. A hash child decides ownership by a hash of the hash key in
  JavaScript, so it walks the hash keys one at a time, as the hash-key walk of the import does: one seek steps
  past all rows of an owned key, and the step deletes the rows of each foreign key with one statement. A step
  holds at most its batch of hash keys (each at most 1024 bytes), so it also stops at about 1 MiB of key bytes.
  A range child decides ownership by the sort-key interval, so each step is one `DELETE` of the rows outside
  `[start, end)` by a `rowid` subquery with a `LIMIT`. It reads no row into JavaScript.

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

The answer to "how much must the child delete before it accepts writes" is therefore: nothing for the size, phase
1 for the transaction and routing state, the release for linearizability, and the split decision must wait for
the whole prune.

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

### 4.9 Restore time

A restore downloads the image and uploads it to the child's own key (section 2). The planning assumption is one
stream for the download and one for the upload, one after the other, because each stream also encrypts. The time
is `2 * size / throughput`, plus the cold start:

| Throughput | 100 MB | 250 MB | 500 MB |
| --- | ---: | ---: | ---: |
| 50 Mbit/s (6.25 MB/s), worst case | 32 s | 80 s | 160 s |
| 100 Mbit/s (12.5 MB/s), conservative | 16 s | 40 s | 80 s |
| 400 Mbit/s (50 MB/s), expected in the same region | 4 s | 10 s | 20 s |

The 400 Mbit/s row is not measured. A single stream to S3-compatible object storage in the same region usually
does tens of MB/s, and AES-GCM with hardware support encrypts faster than one stream transfers, so 50 to
100 Mbit/s is a safe lower bound. A source far from the bucket of its region is slower, because the latency limits
one stream. A download and an upload that overlap halve the times.

Consequences:

- The timeout of the confirm call must be more than the worst row for `maxSizeMb`, with a margin: about 5 minutes
  for the default of 500 MB.
- Writes of the slice are refused for this time, and reads stay available (the release, section 4.2). Today a
  write is refused for the whole import, which moves the same bytes over Workers RPC.
- The children restore in parallel, at most `REPARTITION_RPC_CONCURRENCY` at a time, so a `hashSplitN` of 6 or
  less takes about one restore time.
- A smaller `maxSizeMb` shortens the gap linearly.

The local `workerd` cannot measure any of these numbers: it has no snapshot back-end and no object storage
between two objects. Only a test in production can measure them.

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
- **No read-through RPC.** The child holds its whole slice from its first session, so `fokosExecuteLocal`,
  `resolveCallerSlice` and the `read_source` path of `#whileImporting` go for splits. Until the release, the
  source answers the reads of the slice itself, with no extra hop (section 4.2). Reads stay available for the
  whole split, and writes are refused only until the release.
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
- **Write cost.** A deleted row is billed as a written row. The children of one split delete `(hashSplitN - 1)`
  times the row count of the parent. The import of today inserts each row of the parent one time. An insert and a
  delete touch the same table and index entries, so the two cost the same for each row. For a 500 MB parent with
  100 B items (about 1.4 million rows):

  | `hashSplitN` | Import (rows inserted) | Clone and prune (rows deleted) | Ratio |
  | ---: | ---: | ---: | ---: |
  | 2 | 1.4 million | 1.4 million | 1x |
  | 4 | 1.4 million | 4.2 million | 3x |
  | 8 | 1.4 million | 9.8 million | 7x |

  The optional compaction adds about one more write for each kept row. A range split with `rangeSplitN = 4` (the
  default) costs 3x, as a hash split with 4 children does. A split source still keeps its own rows, as today.
- **Storage held during the prune.** Each child holds a full, separate image until its prune ends, so a split
  holds `hashSplitN + 1` times the size of the source for some time.
- **The write gap.** Writes of a slice are refused from the cutover until the release: one restore (16 s to
  160 s for 500 MB at 100 to 50 Mbit/s, section 4.9) plus phase 1. The source now has a second read path for the
  slices it has not released, and the release check must run in the same synchronous block as the local read.
- **CPU.** A prune of 700,000 rows needs about 35 s of CPU at the local rate, more than the 30 s of one
  invocation, so it runs as many steps of about 5,000 rows over many alarm passes (section 4.5). Each step holds
  the input gate while its transaction runs.
- **The restore protocol.** The restore call fails by design, so the source confirms with `fokosStatus`. The
  child must refuse a restore once it has an identity. A missed guard deletes accepted writes.
- **Destroy and restore.** The abort right after `onNextSessionRestore` closes the window inside the child. One
  race stays: the destroy walk can reach an empty child before a `fokosRestore` of the source arrives there. The
  child then restores after its destroy, and the data comes back. Two rules close it:
  1. The source stops the restore step once its destroy fence is written (`isDestroying()`), as the import job
     stops today.
  2. `fokosDestroy` on a child with empty storage writes the destroy fence and leaves it, instead of leaving empty
     storage. Rule 2 of section 4.2 then refuses a late restore. The cost is one stored key in a destroyed child
     that never restored.

  The walk treats a target in `restore_sent` or `restore_scheduled` as a node that can hold data, and confirms it
  with `fokosStatus` before it destroys it.
- **Role detection at boot.** Each constructor must compare `ctx.id` with the cloned rows before it serves a
  request. A defect here makes a child act as its parent, a router with no data.
- **Testing.** Local `workerd` throws for both methods, and the design cannot wait for a local back-end. Two
  ways to test, and neither measures the transfer time:
  1. **Copy the SQLite file.** Local storage keeps one SQLite file for each object under `.wrangler/state`. A
     harness that runs in Node (not inside `vitest-pool-workers`, which cannot read the file system) can stop the
     runtime, copy the file of the source to the file name of the child's ID, and start the runtime again. The
     file name for an ID must be checked against the local storage layout. This tests the role detection, the
     adopt transaction, the prune and the release with real storage.
  2. **A snapshot adapter.** The runtime calls `snapshot` and `onNextSessionRestore` through one small object that
     the host gives, as it gives `stub` today. The test host gives an adapter that copies the rows of the source
     into the child. This puts the seam in the code that production runs, so it must be weighed against the rule
     "do not add a production hook for a test".
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

1. **Measure the open items** of section 10 (the restore time and the prune rate).
2. **Add the logical-size split decision first**, independent of the snapshot API. Make `SplitPlanner.evaluateSplit`
   read the sum of `key_size_estimates` together with `databaseSize`. This also helps today: a partition after
   heavy deletes keeps empty space inside its pages.
3. **Build clone and prune for hash splits and range splits** behind a host option, with the copy flow as the
   default until the flag is stable. Keep the source states. Replace the target initialization and the ack with
   `pending -> restore_sent -> restore_scheduled -> restored -> released`, and the target import with
   `pruning_meta -> awaiting_release -> pruning -> active`.
4. **Keep the copy flow for key promotions.**
5. **Choose `hashSplitN` with both costs in view**, because it cannot change later. A larger value leaves less
   empty space after the prune, and costs more deletes:

   | `hashSplitN` | Child size after the prune, 100 B to 500 B items (section 5) | Rows written against the import |
   | ---: | ---: | ---: |
   | 2 | 82% to 94% of the parent (61% to 73% compacted) | 1x |
   | 4 | 37% to 40% of the parent (31% to 36% compacted) | 3x |

   With `hashSplitN = 2` the split decision on the logical size (step 2) is required, not optional. Without it the
   child splits again soon after the prune. With `hashSplitN = 4` the space is close to a copy, and each split
   costs 3 times the writes of the import. The same 3x applies to a range split with `rangeSplitN = 4`. A range
   split can lower its cost with a lower `rangeSplitN`, because `rangeSplitN` can change. That works well for
   sort keys written in order (for example time-ordered keys), because the prune then frees whole pages. For sort
   keys written in random order, `rangeSplitN = 2` leaves the same half-full pages as `hashSplitN = 2`. For large items (2 KB
   and more) `hashSplitN = 2` leaves almost no empty space, so it is the cheaper choice there.
6. **Measure in production**: the time of a restore of a 500 MB image, the prune rate under load, and the
   `databaseSize` of the children after the prune.

## 10. Open questions

Answered (section 2 states them as facts):

1. A restore does not keep the alarm. The adopt transaction arms the alarm of the child (section 4.4), and the
   confirm call of the source wakes the restored child (section 4.2).
2. A bookmark is valid for 30 days after it is generated. The source refreshes the bookmark at 29 days, before
   `completed` (section 4.1).
3. A pending restore does not need to survive `deleteAll()`: the child aborts immediately after
   `onNextSessionRestore`, inside `blockConcurrencyWhile`, so nothing runs between the two. The race that stays is
   on the source side, and section 7 closes it.
4. A restore costs one download and one upload of the database: a cold start of that size plus the upload of the
   image to the child's own key. The images are separate copies (sections 2 and 7).

5. A snapshot handle can probably travel through a second RPC hop. This needs a test. The design does not need
   it: the source calls the child directly.
6. A local back-end for snapshots may come, but the design does not depend on it. Section 7 gives two ways to
   test without it. The transfer and the copy times cannot be measured locally.
7. The restore time depends on the distance from the object to the bucket of its region. Section 4.9 plans with
   one stream each way at 50 to 100 Mbit/s.
8. The CPU limit is 30 s for each invocation, and the memory limit is 128 MB for each isolate. The prune stays far
   below both (section 4.5).

9. A deleted row is billed as a written row (section 7).

Still open:

10. The real restore time and prune rate in production, for a 500 MB image (section 9, step 6).
