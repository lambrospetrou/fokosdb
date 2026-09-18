# RFC — One repartition flow for hash splits, range splits, and key promotions

**State:** Implemented
**Milestone state:** M0 is complete on 2026-09-18. M1 is complete on 2026-09-18: all four stages are
delivered. The 100 MB migration benchmark of section 4.14 is not delivered, because its before-M1
half cannot be measured after the old components are removed.
**Date:** 2026-09-17
**Author:** Lambros

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

### 1.1 The problem

`PartitionDO` moves ownership in three ways:

- A hash split moves all keys to hash children.
- A range split moves one sort-key interval to range children.
- A key promotion moves one hash key to a range root.

Each flow selects ownership, creates targets, changes routing, copies state, collects acknowledgements, and
cleans the source. The current code uses separate state models:

- Hash and range splits use the KV key `__split_status`. `SplitStateMachine` owns this key.
- Promotions use the SQL table `promoted_keys`. `PromotionManager` owns this table.
- Every target uses five KV keys. `SplitMigration` owns three of these keys.
- The source serves five migration RPCs.

The separate source models cannot arbitrate atomically. A hash split checks for an in-flight promotion before
it queues, and a promotion checks for a split before it cuts over. Promotion queueing does not check the split
state, and split start does not check for a promotion queued after `split_queued`. A split record and a
promotion record can therefore appear at the same time.

The current code has two more gaps that M1 must close:

- Target import start is not durable. `internalInitFromSplit` sets no fallback alarm on the target. The parent
  triggers migration after cutover with a best-effort call. When that call fails and no request reaches the
  target, the import does not start.
- An acknowledgement does not validate target membership. The split state machine records the acknowledging
  name before it proves that the name is a configured child. An unknown name cannot complete the split, but it
  consumes an acknowledgement entry and can later fail the count invariant.

The current code already retries target acknowledgements and routes a forced promotion. M1 must preserve those
fixes.

`docs/ideas/fokos-sharding/2026-09-09-fokos-partition-runtime.md` specifies one repartition model and one import
loop. This RFC builds that durable model and protocol inside `PartitionDO`. A later change can move the code to
the runtime package.

FokosDB has no released durable data to convert. M1 therefore replaces the records in one deployment. This
option ends after the first release.

### 1.2 Current system

- One `PartitionDO` stores one partition in SQLite.
- A hash partition owns the keys that hash to its path.
- A range partition owns one hash key and one immutable `[startBoundary, endBoundary)` interval.
- Each request carries a `PartitionContextResolved` because a Durable Object has no constructor parameters.
- `ensurePartitionContext` validates the immutable identity and updates the mutable split policy.
- A split source becomes a router after cutover.
- A promotion source keeps all hash keys except the promoted key.
- An importing target rejects writes with `partition_migrating`.
- An importing target serves point and query reads through its source.
- `runBackgroundWork` and one alarm drive all background jobs.
- Durable transitions can update synchronous KV and SQL in one `ctx.storage.transactionSync` call.

## 2. Goals and requirements

### 2.1 In scope

- One SQL model must hold hash splits, range splits, and key promotions.
- One KV record, `__fokos/import`, must hold target import state.
- One arbitration transaction must decide each queue request and each cutover.
- Four control RPCs must replace the old initialization and migration RPCs.
- `fokosExecuteLocal` must replace both direct read RPCs.
- The target must persist `imported` before it acknowledges the source.
- The target must have its own fallback alarm.
- Each range split must persist one plan before it initializes a target.
- Each target must persist `pending`, `initializing`, or `initialized` on the source.
- A hash split and an unfinished key promotion must remain mutually exclusive.
- A terminal promotion must survive a later hash split.
- A promotion request after split cutover must route to the current hash child.
- An importing child must reject a promotion request until its import is complete.
- M0 must fix the three defects in section 4.13 before M1 replaces the durable records.
- The public `FokosDB` API and the transaction coordinator protocol must not change.
- Existing public error codes must not change.
- Existing integration assertions must continue to pass, except for replaced internal RPC names and for the
  four assertions that section 3 records against M0.

### 2.2 Out of scope

- The runtime dispatch pipeline, operation descriptors, response envelope, and lease modes are out of scope.
- Runtime package extraction and the example host are out of scope.
- The split of `PartitionContext` into identity, topology, and policy is out of scope.
- The KV keys `__partition_context` and `__partition_depth` must remain.
- A hash leaf ownership check is out of scope. A hash leaf does not verify that a key hashes to its path, and
  correct parent routing remains required.
- A bound for learned `range_hierarchy` rows is out of scope. The table has no size limit or cleanup.
- Split source item reclamation is out of scope. A hash or range split source keeps its item rows after
  completion. Only a promotion cleans its source.
- A promotion cutover during a hash-child import is out of scope.
- Internal error renames from the runtime RFC are out of scope.
- An in-memory copy of `fokos_route_overrides` is out of scope.

### 2.3 Requirements

- Each durable transition must use one `ctx.storage.transactionSync` call with no `await`.
- SQL and KV records must decide protocol behavior.
- A cache must update after the transaction returns and before the next `await`.
- Each serialized RPC message must stay below the 32 MiB Workers RPC limit.
- Each migration page and each `fokosStatus` page must stay at or below 20 MiB.
- Each migration pull must scan at most 10,000 source rows.
- Each migration page must return at most 1,000 data rows.
- Each target import step must apply at most one migration page. One pass runs up to
  `fokosImportPagesPerPass()` steps in sequence.
- Each source repartition step must select one repartition.
- `REPARTITION_RPC_CONCURRENCY` must be 6.
- Each source repartition step must call at most `REPARTITION_RPC_CONCURRENCY` targets.
- This bound matches the Workers limit of six simultaneous outgoing connections per request.
- Each KV key and value pair must stay below 2 MB.
- Each Durable Object must use its one alarm through `ensureAlarmSet`.
- A request path and the pre-pass fallback must never replace an earlier deadline with a later deadline.
- The end of a work pass may set the alarm to the earliest durable deadline, even when that deadline is later
  than the fallback the pass armed.
- Each background job must be idempotent, bounded, and resumable.
- `MAX_HASH_KEY_BYTES` must remain 1,024 bytes.
- `hashSplitN` and `rangeSplitN` must remain between 2 and 255.
- `splitN` values that affect deterministic routing must remain immutable after initialization.
- The partition must not retain every route override in an in-memory cache.
- `status()` must keep `splitStatus`, `migrationStatus`, and `promotedKeys` as a compatibility view for tests.
- `fokosStatus` must paginate all repartition and target rows.
- Destroy traversal must fence background work before it reads target links.
- Migration 3 of `sqlMigrations` must create the new tables in place.

## 3. Milestones

Each milestone must build, pass the existing suites, and be safe to ship on its own.

### M0 — Fix three current defects

M0 implements section 4.13. Each fix must start with a failing test.

M0 replaces `internalGetItemDirect` and `internalQueryItemsDirect` with `fokosExecuteLocal`. The M0 request is
the `FokosExecuteLocalRequest` type of section 4.8 without `repartitionId`, because the current source models
have no repartition ID. M1 adds the required ID. M0 also adds `repartition_target_unknown` for a caller that
fails membership validation.

The current source models authorize the caller and resolve its slice during M0:

- A split source matches `caller.doName` in `splitStatus.childPartitionContexts`. The matched context gives
  the hash-child slice or the range `[start, end)` slice.
- A promotion source decodes the hash key from `caller.partitionId` with `PartitionIdHelper`. It requires a
  `promoted_keys` row in `promoting` for that key. The slice is that one hash key.
- A caller that matches neither throws `repartition_target_unknown`.
- A terminal override is a `promoted_keys` row in `promoted`.

The slice validation and the read rules of section 4.8 apply to M0 with these inputs.

#### M0 result

M0 is complete. It delivers:

- `fokosExecuteLocal` on `PartitionDO`, with the caller resolution and the slice validation above.
  `internalGetItemDirect` and `internalQueryItemsDirect` are removed.
- `repartition_target_unknown` in `INTERNAL_CODES`.
- `FokosSlice` and the pure slice validation in
  `packages/fokosdb/src/shared/partition/repartition-slice.ts`. M1 moves this file beside the flow, in
  `packages/fokosdb/src/shared/partition/repartition/`.
- One in-memory import promise in `PartitionDO`, and one durable guard in each migration page transaction.
- Forwarded range-child contexts that the router rebuilds from its current context.

M0 changes two decisions of this RFC:

1. **D1 keeps its multi-page pass.** The written plan also asked for one page per pass. That rule needs a
   durable phase cursor, which M1 replaces with `FokosMigrationCursor`, so M0 would build and then delete it.
   M0 implements the three durable rules of section 4.13 instead, and drops the prefetch. The guards, not the
   loop shape, stop a stale page. Measurements show no migration slowdown from the dropped prefetch.
2. **Slice clipping arrives in M0, not M1.** The caller slice decides each read, so M0 cannot resolve a slice
   and then ignore it. Four existing integration assertions therefore change:
   - `hash-split.test.ts` read two keys through one child. One key hashed to its sibling. Each key now reads
     on the child that owns it.
   - Three `query-items.test.ts` cases asserted that a migrating range child answers for the whole hash key.
     Each case now expects the caller's slice. `forwardToRangeRootPartition` can send a broad interval to one
     leaf, so this leak was reachable.

   The M1 test "A broad query is clipped to one importing range child" moves to M0 for the same reason.

### M1 — Replace the durable flow

M1 delivers the complete unified flow in one deployment. It includes:

- The three SQL tables and the plan KV records.
- The `__fokos/import` target record.
- Both state machines and the arbitration rules.
- The `RepartitionSource` and `RepartitionTarget` classes, the `MigrationHost` interface, and the FokosDB host.
- The four control RPCs and `fokosExecuteLocal`.
- The phased migration cursor and bounded pull work.
- Single-flight background work and fair due-row selection.
- The derived `status()` view and paginated `fokosStatus`.
- A durable destroy fence and traversal over every durable target row.

M1 removes:

- `SplitStateMachine`.
- `PromotionManager`.
- `SplitMigration`.
- The old migration RPCs.
- `promoted_keys`.
- The old target migration KV keys.

#### M1 stages

M1 ships as one deployment, but it is built in four stages. Each stage builds, type checks, and keeps
the whole test suite green, so work can stop and resume at a stage boundary. The old components stay
until stage 3, which is the only way the earlier stages can compile.

**Stage 1 — the store, the schema, and the row types. Complete.**
Migration 3 creates `fokos_repartitions`, `fokos_repartition_targets`, and `fokos_route_overrides`,
and `PartitionStore` gains the API for them: sequence allocation, arbitration reads, due-row and
cleanup selection, target progress, the joined override lookup, and the paginated status view.
`insertItemIfAbsent` now reports the exact stored bytes so an import can maintain `key_size_estimates`
page by page, and `deleteExpiredItems` reads `fokos_route_overrides` instead of `promoted_keys`.
`promoted_keys` stays in the same migration until stage 3 removes its last caller.

**Stage 2 — the source and target halves, the host boundary, and the unit suite. Complete.**
`packages/fokosdb/src/shared/partition/repartition/` holds the flow, its wire types, and the slice
helpers, and `fokos-migration-host.ts` holds the FokosDB `items` and `pending_tx` streams behind the
opaque host phase. `test/repartition/repartition-flow.test.ts` drives real flows over real stores through a harness
whose peer calls land on the receiving flow directly. Nothing calls the flow outside its own suite
yet.

**Stage 3 — wire `PartitionDO` to the flow, and delete the old components. Complete.**
`PartitionDO` holds one flow and delegates to it: the five control RPCs, the request-path reads of
section 4.8, the derived `status()` view of section 4.12.1, single-flight background work, and the
alarm computation of section 4.9.5. `SplitStateMachine`, `PromotionManager`, `SplitMigration`,
`promoted_keys`, the old migration RPCs, and the old target migration KV keys all go in this stage,
and `withFokosErrors` starts mapping `repartition_not_cut_over` to `partition_migrating`.

**Stage 4 — destroy, `fokosStatus`, and the integration tests. Complete.**
The destroy fence, `fokosPrepareDestroy`, the paginated `fokosStatus`, and the traversal of section
4.12.2, together with the integration tests of section 4.14 and the harness changes to
`withMigrationHeld` and `withMigrationBatchCap`. The concurrent stale-page cases that M0 holds in
`migration.test.ts` move here rather than into the unit suite: holding a page in flight needs two
interleaved loops, which the unit harness cannot express.

#### M1 stage 3 notes

- The source keeps NO in-memory cache of its split row. Section 4.3 allows one, but the case worth caching
  is absence, which every leaf hits on every request, and a stale negative answer is the dangerous
  direction: a router that believes it is not one serves rows its targets already own. The lookup is one
  seek of a partial index, which is what the KV read it replaced cost.
- A lock release wakes the promotion waiting on it. A promotion that cannot move a locked key parks five
  seconds out; a commit or a cancel is the only event that can change that answer, so it clears the
  deadline rather than letting the poll interval decide how long the key stays put.
- `fokosInit` arms the target's fallback alarm and starts nothing. The source is still `planned` when it
  calls, so an immediate pull would earn `repartition_not_cut_over` and park the target behind a retry it
  did not need. `fokosStartImport` begins the import and clears any such deadline.
- The hash routing cache is created on demand, not in the topology constructor. The topology is built on
  the first request, long before the partition splits, so a constructor decision would leave a router
  unable to learn.
- `withMigrationHeld` now holds the `pending_tx` stream, the last one an import runs. The overrides phase
  runs FIRST, so a held import has already inherited its route overrides — which is what section 4.13's
  D2 fix requires, and it inverts the precondition one read-through test used to assert.

#### M1 stage 4 notes

- `traverseForDestroy` no longer resolves a range root from a promoted hash key. Every partition
  below a root is now a durable target link that `fokosStatus` reports, so the router walks refs and
  needs neither the split status nor the promoted-key list. It dedupes by `doName` over the whole
  traversal, which is what a range root shared by two hash children needs.
- The destroy fence also stops the TTL sweep and the stale-transaction sweep. Both read
  `canSweepLocally`, so one guard covers the two jobs section 4.9.3 lists last.
- `fokosPrepareDestroy` waits for the in-flight pass and only then deletes the alarm. The pass can
  re-arm the alarm at its own end, so deleting first would leave one behind.
- `statusEntries` takes the byte budget, so the count limit and the size limit are enforced in one
  place. The first entry of a page always goes out: a page that returned none could never drain.

#### M1 decisions taken during implementation

1. **The cleanup stage is unified, and `cleanup_started` is gone.** `completed` already means that
   every target acknowledged and cleanup is pending, so the flag named a state that `state` implied.
   Every kind now ends `completed` then `cleaned`, a split's cleanup step reclaims nothing and reports
   itself done, and one job drives all three kinds with no branch on the kind.
2. **`fokos_repartition_targets` stores no slice kind.** A repartition never mixes slice kinds, so its
   `kind` gives the kind of every target, and every reader holds the repartition row first. The write
   path asserts the slice matches, because nothing else can.
3. **Wide columns sit last in both rowid tables.** SQLite reads a record until it has the columns a
   query needs, so the keys and the derived names follow every column the due-row scan, the alarm, and
   the target counts read.
4. **The test split threshold moved from 0.1 MB to 0.25 MB.** The new tables and indexes cost about
   45 KB of empty pages, which put a fresh partition over a 0.1 MB cap before its first write. It
   affects `hash-split.test.ts` and `destroy.test.ts` only; production defaults are 100 MB.
5. **`collectBatch` gained `maxScannedRows`.** Section 2.3 bounds a pull at 10,000 scanned source rows
   and the helper had no way to express it, so a sparse slice would scan a whole table for one page.

## 4. Proposed solution

### 4.1 High-level overview

A **repartition** is one durable plan that moves ownership from one source to one or more targets.

- A `hash_split` moves all source keys to `hashSplitN` deterministic children. The source becomes a router.
- A `range_split` moves the source interval to `rangeSplitN` children. The source becomes a router.
- A `key_promotion` moves one hash key to its range root. The source keeps all other hash keys.

The source and target use separate durable state machines:

```text
source: fokos_repartitions.state

(none) -> queued -> planned -> cutover -> completed -> cleaned

Every kind ends the same way. `completed` means every target acknowledged and source cleanup is
pending. `cleaned` means that cleanup finished. A split reclaims no item rows, so its cleanup step
deletes nothing and moves the row straight to `cleaned`.

target: __fokos/import.state

fokosInit -> awaiting_data --non-final page--> importing
                  |                              |
                  +---------final page----------+-> imported -> active
```

The source persists the plan and every target before it calls `fokosInit`. It persists `initializing` before
each initialization call. It changes routing only after every target is `initialized`.

Each target pulls two migration phases: the `overrides` phase that the flow owns, and the `host` phase that
the application owns. It commits one page and its cursor atomically. The final page sets
`imported`. The target then retries the source acknowledgement until the source accepts it.

A split in `cutover` or `completed` makes its source a router. A key promotion in `cutover`, `completed`, or
`cleaned` makes its range tree the owner.

An unfinished promotion blocks a hash split. A hash split blocks each later promotion on its source. Terminal
promotions move to the owning hash child as route overrides.

#### 4.1.1 Code structure

Two classes, `RepartitionSource` and `RepartitionTarget`, both in
`packages/fokosdb/src/shared/partition/repartition/repartition-flow.ts`, own the flow. `PartitionDO` holds one
of each and delegates to them. Each follows the pattern of `TransactionParticipant` and `TtlExpiry`: it takes
the real `PartitionStore`, the `DurableObjectStorage`, and a deps object. Neither holds a stub or makes an RPC
of its own.

A partition is both halves, because a hash child is a target first and a source later. The two share no
in-memory state and never call each other; the only thing they have in common is the `PartitionStore`. The
request path joins them at the Durable Object: the request gate reads the source's `routerRole()` and the
target's `isImporting()`, and the alarm reads `sourceDeadline()` and `importDeadline()`. The receiver is what
names the role at a call site, so no method carries a role prefix.

The deps split the same way. `RepartitionCommonDeps` holds `getPeer`, `host`, `identity`, `scheduleWork`, and
`logParams`. `RepartitionSourceDeps` adds `computeRangeBoundaries`, `lockCountForKey`, `cleanupStep`, and
`onSplitCompleted`. `RepartitionTargetDeps` adds `hasIdentity`, `applyTargetIdentity`, and `ensureAlarmSet`.
The DO builds one object that satisfies both.

The file has these sections, in this order:

1. The wire types and the `MigrationHost` interface.
2. `RepartitionSource`: the reads the request path makes, arbitration, `queue`, `plan`, `init_start`,
   `init_done`, `cutover`, `start_import`, `sourceCleanupStep`, due-row selection, the `fokosStatus` pages,
   `acceptAck`, and `resolveCallerSlice`.
3. `RepartitionTarget`: `importOnePage`, the request-gate reads (`importRecord`, `importState`,
   `isImporting`, `importDeadline`), `initAsTarget`, `startImport`, and `sendAck`.
4. Shared module functions: the retry delay, the backoff, and the cursor and slice comparisons.

The migration protocol keeps its two ends side by side across the class boundary: `servePage` is the LAST
member of the source and `importOnePage` is the FIRST of the target, and the `overrides` page build and apply
sit either side of the same line. The host phase passes through to the injected host.

Four methods still name their role, because the receiver alone leaves them ambiguous. `acceptAck` on the
source and `sendAck` on the target are the two sides of one acknowledgement and must not be read for each
other. `initAsTarget` would otherwise read as "initialize this object". `sourceCleanupStep` matches the
`source_cleanup` job name of section 4.9.3, and pairs with `sourceStep`.

The DO keeps:

- Every RPC method as one delegation through `#rpc`, with `ensurePartitionContext` where a request carries a
  context.
- Every stub. The deps object gives the flow `getPeer(ref)`, which returns a `FokosPartitionControlRpc`.
- `withSplitForwarding`, `groupItemsByRouting`, and the local read of `fokosExecuteLocal`. The flow resolves the
  caller slice, and the DO reads.
- `runBackgroundWork` as the single-flight scheduler, the alarm, and the `__fokos/destroying` fence.
- `TransactionParticipant`, TTL expiry, and stale transaction recovery.

The deps object carries:

- `getPeer(ref: FokosPartitionRef): FokosPartitionControlRpc`.
- `host: MigrationHost`, with `buildPage(cursor, slice)` on the source and `applyPage(page, slice)` on the
  target. The FokosDB implementation holds the `items` and `pending_tx` streams and lives in
  `packages/fokosdb/src/shared/partition/fokos-migration-host.ts`.
- `computeRangeBoundaries(hashKey, start, end, n)`, `lockCountForKey(hashKey)`, `cleanupStep(hashKey)` that
  returns whether the source rows are gone, and `onSplitCompleted()` that deletes the source pending rows.
  These read and write application data that the flow does not own.
- `scheduleWork()`, `ensureAlarmSet(ms)`, and `logParams()`, as the current components take them.
- `identity()`: the partition context, the depth, and the range ancestors.

`HashPartitionTopologyImpl` and `RangePartitionTopologyImpl` take the flow instead of `SplitStateMachine`. They
read `routerRole()`, `splitTargets()`, and `overrideFor(hashKey)` from it. `PartitionStore` remains the only
owner of the SQL statements, and the flow calls its methods.

This structure is one step toward the runtime package. A later change lifts the class, the host interface, and
the deps object as they are. The dispatch pipeline, the route context, the response envelope, and the identity
split stay out of scope.

#### 4.1.2 A hash split, end to end

This walkthrough names every method, RPC and state change of one `hash_split` on a hash leaf `P` with
`hashSplitN = 2`. A range split is the same sequence with different targets and slices. A key promotion is the
same sequence with one target, plus the lock check of section 4.4 and the row reclaim of section 4.9.3.

```text
source P                                               targets C0, C1
────────                                               ──────────────
queue({kind:"hash_split"})            state: queued
  |
sourceStep -> #plan                   state: planned   target rows: pending
  |
sourceStep -> #initializeTargets      target: initializing
  |-- fokosInit --------------------------------------> initAsTarget
  |                                                       identity + import record
  |   <----------------- ok -------------------------     fallback alarm armed
  |                                 target: initialized   state: awaiting_data
  |
sourceStep -> #cutover                state: cutover  <== ROUTING MOVES HERE
  |
sourceStep -> #notifyStart
  |-- fokosStartImport -------------------------------> startImport (clears the deadline)
  |
  |   <--- fokosMigrationPull (overrides) ------------- importOnePage
  |-- servePage -> #buildOverridesPage ---------------> #applyOverrides + cursor
  |   <--- fokosMigrationPull (host, items) ----------- state: importing
  |-- servePage -> host.buildPage --------------------> host.applyPage + cursor
  |   <--- fokosMigrationPull (host, pending_tx) -----
  |-- servePage -> host.buildPage --------------------> final page, cursor null
  |                                                       state: imported
  |   <--- fokosMigrationAck -------------------------- sendAck
acceptAck: all targets acknowledged                       state: active
  |  state: completed, onSplitCompleted()
  |
sourceCleanupStep                     state: cleaned
```

**1. Queue.** A successful local write asks `PartitionDO` to evaluate the split policy. The DO calls
`RepartitionSource.queue`, which arbitrates and writes the row in one transaction: it refuses a second split,
and it refuses a split while a promotion is unfinished. The row starts in `queued` and is due now.

**2. Plan.** The next background pass calls `sourceStep`, which selects the one due row by
`(next_attempt_at, seq)` and dispatches on its state. `#plan` resolves the `hashSplitN` deterministic child
contexts, writes one target row per child with the slice `{ kind: "hash_child", childIndex }`, writes the plan
key, and sets `planned`. All of this is one transaction, so a crash leaves no half plan.

**3. Initialize.** `#advancePlanned` calls `#initializeTargets` until every target is `initialized`. Each pass
takes up to `REPARTITION_RPC_CONCURRENCY` due targets and marks each `initializing` BEFORE its call. A call in
flight is therefore indistinguishable from one that lost its reply, and the retry repeats the same idempotent
`fokosInit`. The target writes its identity and its import record in `awaiting_data`, arms its fallback alarm,
and starts nothing. A failed call leaves its own target behind a backoff and does not hold up its siblings.

**4. Cut over.** When every target is `initialized`, `#advancePlanned` calls `#cutover`. It re-reads the counts
inside the transaction, sets `cutover`, and deletes the now spent plan key. This is the only step that moves
ownership: from here `routerRole()` is true, and the request path forwards every key to its child instead of
serving it locally.

**5. Start.** `#notifyStart` calls `fokosStartImport` on up to `REPARTITION_RPC_CONCURRENCY` due targets. The
call is an optimisation, not a requirement: it clears the retry deadline the target may hold, and each target
also has its own alarm, so an import still starts when no call arrives.

**6. Import.** Each target runs `importOnePage` up to `fokosImportPagesPerPass()` times per pass, and one step
applies at most one page. It calls `fokosMigrationPull` with its durable cursor. The source answers through
`servePage`, which validates the caller, checks its own state, and builds one page of one phase: the
`overrides` phase first, then the `host` phase that the injected host owns. The target applies the page and the
new cursor in ONE storage transaction, after it re-reads the durable record and proves the page is not stale.
The page with a null cursor sets `imported`.

While the target is in `awaiting_data` or `importing`, a read that reaches it goes back to the source through
`fokosExecuteLocal`, and a write fails with `partition_migrating`. Section 4.8 holds those rules.

**7. Acknowledge.** The target persists `imported` before it calls, so a lost reply costs nothing. `sendAck`
calls `fokosMigrationAck` and retries until the source accepts it. `acceptAck` marks the target and, when every
target has acknowledged, sets `completed` and calls `onSplitCompleted()`, which deletes the source lock rows
that every target now owns its own copy of. The target then sets itself `active`.

**8. Clean.** `sourceCleanupStep` runs for a `completed` row. A split keeps its item rows for life, so its step
reclaims nothing and moves the row straight to `cleaned`. Only a promotion has rows to give back.

Every step of the source is one bounded transaction, and every step of the target commits one page with its
cursor. Both are driven by the same single-flight background pass, which section 4.9 describes.

### 4.2 Data model

Migration 3 in `packages/fokosdb/src/shared/partition/partition-store.ts` must create these tables. Migrations 4
and 5 must not change. `PartitionStore` must remain the only owner of their SQL statements.

```sql
CREATE TABLE IF NOT EXISTS fokos_repartitions (
    id              TEXT    NOT NULL PRIMARY KEY,
    seq             INTEGER NOT NULL,
    kind            TEXT    NOT NULL,
    state           TEXT    NOT NULL,
    hash_key        BLOB,
    queued_at       INTEGER NOT NULL,
    cutover_at      INTEGER,
    completed_at    INTEGER,
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_fokos_repartitions_seq
    ON fokos_repartitions (seq);
CREATE INDEX IF NOT EXISTS idx_fokos_repartitions_due
    ON fokos_repartitions (state, next_attempt_at, seq);
CREATE INDEX IF NOT EXISTS idx_fokos_repartitions_split
    ON fokos_repartitions (kind) WHERE kind IN ('hash_split', 'range_split');

CREATE TABLE IF NOT EXISTS fokos_repartition_targets (
    repartition_id     TEXT    NOT NULL,
    partition_id       TEXT    NOT NULL,
    do_name            TEXT    NOT NULL,
    target_index       INTEGER NOT NULL,
    slice_hash_key     BLOB,
    slice_start        BLOB,
    slice_end          BLOB,
    slice_child_idx    INTEGER,
    initialization     TEXT    NOT NULL DEFAULT 'pending',
    start_notified     INTEGER NOT NULL DEFAULT 0,
    acknowledged       INTEGER NOT NULL DEFAULT 0,
    attempts           INTEGER NOT NULL DEFAULT 0,
    next_attempt_at    INTEGER NOT NULL,
    PRIMARY KEY (repartition_id, partition_id),
    UNIQUE (repartition_id, target_index)
) STRICT;

CREATE TABLE IF NOT EXISTS fokos_route_overrides (
    hash_key       BLOB NOT NULL PRIMARY KEY,
    repartition_id TEXT NOT NULL
) WITHOUT ROWID, STRICT;
```

The valid values are:

- `kind`: `hash_split`, `range_split`, or `key_promotion`.
- `state`: `queued`, `planned`, `cutover`, `completed`, or `cleaned`.
- `initialization`: `pending`, `initializing`, or `initialized`.
- `hash_key`: the promoted key of a `key_promotion`, and null for a split. A hash split moves every key, and a
  range split moves an interval of the key that the partition identity already holds.

The source creates local IDs as `r<seq>`. It gets `seq` from `MAX(seq) + 1`. Rows are permanent, so sequence
values do not repeat. The source `doName` and local ID form a global identity.

The slice uses SQL columns because migration filters and range routing read it. `target_index` defines target
order. Range partition IDs do not sort by boundary.

The target row stores no slice kind. A repartition never mixes slice kinds, so its `kind` gives the kind of
every one of its targets: a `hash_split` hands out `hash_child` slices, a `range_split` hands out `range`
slices, and a `key_promotion` hands out one `promoted_key` slice. Every reader holds the repartition row
before it reads a target row, because `fokosMigrationPull` and `fokosMigrationAck` check that the repartition
exists first. A stored kind would also be the only thing separating a `range` slice with two unbounded edges
from a `promoted_key` slice, and the write path must reject a slice that its repartition kind does not take.

The table stores no depth for a `hash_child` slice. The depth of a hash child is the source depth plus one, and
the target `partition_id` encodes it. The source fills `FokosSlice.depth` from its own depth when it builds the
import record and the migration filter.

The key `__fokos/repartition/<id>/plan` stores the immutable plan with structured clone. The plan contains:

- The source identity.
- Computed range boundaries.
- Selected range ancestors.

The plan must not contain mutable split thresholds. Target rows already contain target references and slices,
so the plan must not repeat them.

The planning transaction must write the plan, all target rows, and `state = 'planned'`. A maximum-shape test
must use these limits:

- `rangeSplitN = 255`.
- Maximum hash and sort key sizes.
- The maximum 20 selected range ancestors.

The structured-clone value and its key must stay below 2 MB. The cutover transaction must delete the plan key.
All targets are initialized at that point, and target rows hold all routing slices.

Promotion cleanup needs no progress key. Each step deletes the lowest sort keys that remain, so the item rows
are the progress.

The boolean KV key `__fokos/destroying` is the durable destroy fence. Normal requests and background
transitions must stop when this value is true.

Each target stores one import record:

```ts
type FokosImportRecord = {
	schema: 1;
	state: "awaiting_data" | "importing" | "imported" | "active";
	repartitionId: string;
	source: PartitionContextLivePartition;
	slice: FokosSlice;
	cursor: FokosMigrationCursor | null;
	attempts: number;
	nextAttemptAt: number;
	updatedAt: number;
};

type FokosSlice =
	| { kind: "hash_child"; childIndex: number; depth: number }
	| { kind: "range"; hashKey: KeyBytes; start: KeyBytes | null; end: KeyBytes | null }
	| { kind: "promoted_key"; hashKey: KeyBytes };

type FokosMigrationCursor =
	| { phase: "overrides"; inner: PromotedKeyCursor | null }
	| { phase: "host"; inner: unknown };

/** The FokosDB host cursor. The flow does not read it. */
type FokosDbHostCursor =
	| { stream: "items"; cursor: ScanCursor | null }
	| { stream: "pending_tx"; cursor: PendingTransactionCursor | null };
```

The `overrides` phase moves the rows that the flow owns. The `host` phase moves the application data behind an
opaque cursor. The flow starts the host phase with `inner: null` and repeats it until the host returns a null
cursor. The application defines its own streams and page content inside that value. FokosDB uses two streams,
`items` and then `pending_tx`. A later runtime package keeps this boundary: the flow never reads a host cursor
or a host page.

A null import cursor means the start of the `overrides` phase. A null page cursor means that both phases are
complete.

The target stores the full source context to resolve the source namespace and name. The source validates only
the immutable `partitionId` and `doName` from control requests. It must not apply mutable policy from this
stored remote context.

The new records replace the old records as follows:

- `fokos_repartitions` and `fokos_repartition_targets` replace KV `__split_status`.
- `fokos_route_overrides` and `key_promotion` rows replace SQL `promoted_keys`.
- `__fokos/import.state` replaces KV `__split_migration_status`.
- `__fokos/import.cursor` replaces KV `__split_migration_cursor`.
- The `imported` state replaces KV `__split_migration_parent_ack_pending`.
- `__fokos/import.source` replaces KV `__parent_partition_context`.
- The source repartition kind replaces KV `__parent_split_type`.
- `completed` and `cleaned` replace `promoted_keys.gc_done`.
- The repartition states replace `promoted_keys.status`.

`PartitionStore.deleteExpiredItems` must check `fokos_route_overrides` instead of `promoted_keys`. The TTL
sweep must skip a hash key while any override row exists.

### 4.3 Source state machine

Durable SQL and KV records are authoritative. The source can cache its one split row and bounded split target
set. It must not load promotion rows or plans at startup.

A point override lookup must join `fokos_route_overrides` to its repartition row. A cache miss must read SQL.
Cache eviction must not change behavior.

The source transitions are:

- `queue`: Arbitration accepts a signal. Insert `queued`, `queued_at`, and a due deadline. Add a promotion
  override.
- `plan`: A `queued` row is due. Write the plan and targets, then set `planned`.
- `init_start`: At most six targets are due. Set `pending` targets to `initializing` before the RPCs.
- `init_done`: A `fokosInit` call succeeds. Set its target to `initialized` and reset retry fields.
- `cutover`: Every target is `initialized`. For a promotion, recheck the lock count. Set the state and
  `cutover_at`. Delete the plan.
- `start_import`: At most six targets are due. Advance retries before calls. Mark each success as `start_notified`.
- `ack`: A member target acknowledges. Mark it. Set `completed` and `completed_at` after the final acknowledgement.
- `cleanup`: A repartition is `completed`. Run one bounded cleanup step for its kind and set `cleaned`
  after the final step. A promotion deletes one batch of source rows. A split deletes nothing and
  reports itself done at once.

The queue path must arm the fallback alarm before it writes a row. An alarm with no row is a safe no-op. A
repeated signal for an unfinished row must also restore a missing alarm.

The repartition kind determines its plan:

- A `hash_split` uses `resolveHashChildPartitionContexts`.
- A `range_split` computes boundaries once with `PartitionStore.computeRangeSplitBoundaries`.
- A `range_split` selects child ancestors once with `selectRangeAncestors`.
- A `key_promotion` uses `resolveRangePartitionContext(pCtx, hashKey, null, null)`.

A range split can queue on size with fewer than `rangeSplitN` items in its interval.
`computeRangeSplitBoundaries` then returns null, because each child needs one item. The row stays `queued`
with no target rows and retries. Only a new write can change the answer, so the delay doubles from 5 seconds
to a maximum of 5 minutes.

Before promotion initialization, the source must check that the key has no pending lock. A lock keeps the
target `pending`. The source retries this guard every 5 seconds. The cutover transaction must check the lock
count again.

A target in `initializing` means that an initialization call can be in flight or can have lost its reply. A due
retry must repeat the same idempotent `fokosInit` call.

The source must call at most six targets in one step. It must use `Promise.allSettled` so one failure does not
skip another selected target. A failed target keeps durable retry state. A successful target advances even when
another target fails.

A failed `fokosStartImport` must keep `start_notified = 0`. The source must retry it. The target alarm remains
the independent progress mechanism.

The source accepts local operations in `queued` and `planned`. A range plan can become unbalanced before
cutover. This does not change ownership coverage because the plan stores fixed intervals.

After the final split acknowledgement, the completion transaction must delete all source pending transaction
rows. The targets then hold the authoritative copies. A promotion must schedule bounded cleanup instead.

### 4.4 Arbitration

One `transactionSync` call must read all relevant rows and write each arbitration decision.

| Request                  | Acceptance rule                                                                 |
| ------------------------ | ------------------------------------------------------------------------------- |
| Queue `hash_split`       | No split row exists, and no promotion is `queued`, `planned`, or `cutover`      |
| Queue `range_split`      | The source is a range partition, and no split row exists                        |
| Queue `key_promotion`    | The source is a hash partition, no split row exists, and the key has no override|
| Cut over a promotion     | No split row exists, its target is `initialized`, and the key has no lock       |
| Cut over a split         | Every target is `initialized`                                                   |

An unfinished promotion blocks a hash split. This keeps the current safety rule. It also removes the need for
a target cancellation protocol and a transaction-wide reservation for key-size bytes.

A lock can delay a promotion and its source hash split. The existing partition-level backpressure can then
reject new writes with `partition_over_size`. Reads, deletes, transaction commits, and cancels must remain
available. A `PREPARED` transaction must always be able to commit.

A guarded lock counts as a lock. A guarded row waits for `debugForceResolveTransaction`, so the promotion
and the hash split behind it wait for the operator. The stale-transaction guard logs that row once when it
quarantines it. This keeps the current behavior. The promotion must not skip a guarded row: a later forced
commit would route the key to the range root, find no pending row, and lose the write.

A hash split row in any state must block a promotion on that source. A split source in `cutover` or
`completed` is a router and owns no hash key.

Two promotions for different hash keys can progress at the same time. Fair due-row selection prevents one
failed promotion from starving another.

`debugForcePromoteKey` must use `routeSingleDestination`. After split cutover, it reaches the hash child that
owns the key. An importing child returns `partition_migrating` and creates no promotion row. A source with a
split row in `queued` or `planned` still owns the key, and arbitration rejects the promotion. The call must
then throw `partition_over_size`, as the current code does for a queued split. A key that already has an
override returns `{ queued: false }` with its mapped status.

The existing signals remain:

1. A successful local write requests split evaluation.
2. A successful local put or committed transactional put can name a promotion candidate.
3. `debugForcePromoteKey` names a promotion candidate directly.

`TransactionParticipant` must not call an asynchronous promotion hook from a storage transaction. Its local
commit and single-shot methods must collect `{ hashKey, keyEstBytes }` results. `PartitionDO` must process the
deduplicated candidates after the item transaction returns. It must process promotion signals before split
evaluation.

The request result is fixed before post-write signal work starts. The durable queue write and alarm update must
be awaited. A failure must be logged and must not change the completed write result. Commit and single-shot
paths must catch this post-write failure because their item transaction already committed.

### 4.5 Target state machine

The target transitions are:

- `awaiting_data`: `fokosInit` writes identity, depth, ancestors, and the import record.
- `importing`: The target commits its first non-final page with the state and cursor.
- `imported`: The target commits the final page and a null cursor with this state.
- `active`: The source accepts the acknowledgement, and the target resets its retry fields.

The `items` stream must maintain `key_size_estimates` as section 4.7.2 specifies. This removes the unbounded final
`rebuildKeySizeEstimates` scan.

The target must persist `imported` before it calls the source. A crash or lost reply then causes another
acknowledgement attempt.

Each `fokosInit` call must restore the target fallback alarm before it returns. This rule also applies to an
idempotent retry. The alarm must start import when `fokosStartImport` does not arrive.

The request gate must apply these rules:

- In `awaiting_data` and `importing`, a supported read must use `fokosExecuteLocal` on the source.
- In `awaiting_data` and `importing`, every write or transaction RPC must fail with `partition_migrating`.
- A request to an incomplete target must request an earlier import step and restore the fallback alarm.
- In `imported` and `active`, local operations can run because data and locks are complete.
- TTL and stale transaction sweeps can run in `imported` and `active`.

A target in `imported` can serve requests before its acknowledgement succeeds. The source remains in a routing
state, and all target data is complete.

### 4.6 Control RPCs

The partition interface must include these types:

```ts
type FokosPartitionRef = {
	partitionId: string;
	doName: string;
};

type FokosInitRequest = {
	repartitionId: string;
	source: PartitionContextLivePartition;
	target: PartitionContextResolved;
	slice: FokosSlice;
	rangeDepth?: number;
	rangeAncestors?: RangeAncestorInfo[];
};

type FokosMigrationPullRequest = {
	repartitionId: string;
	target: FokosPartitionRef;
	cursor: FokosMigrationCursor | null;
};

type FokosMigrationPage =
	| { phase: "overrides"; overrides: { hashKey: KeyBytes }[]; nextCursor: FokosMigrationCursor | null }
	| { phase: "host"; page: unknown; nextCursor: FokosMigrationCursor | null };

/** The FokosDB host page. The flow does not read it. */
type FokosDbHostPage =
	| { stream: "items"; items: MigratedItem[] }
	| {
			stream: "pending_tx";
			pendingTransactions: PendingTransactionRow[];
			deletionMetadata: { maxDeleteTxOrderTs: number; deleteRevision: number };
	  };

interface FokosPartitionControlRpc {
	fokosInit(req: FokosInitRequest): Promise<void>;
	fokosStartImport(req: { repartitionId: string; source: FokosPartitionRef }): Promise<void>;
	fokosMigrationPull(req: FokosMigrationPullRequest): Promise<FokosMigrationPage>;
	fokosMigrationAck(req: { repartitionId: string; target: FokosPartitionRef }): Promise<void>;
	fokosExecuteLocal(req: FokosExecuteLocalRequest): Promise<GetItemRpcResponse | QueryItemsRpcResponse>;
}
```

Every control request must carry the immutable `partitionId` and `doName` of its remote participant. The
receiver must compare them with its durable rows. It must not trust a `doName` alone.

`fokosInit` must be idempotent for the repartition ID, source identity, target identity, and slice. A matching
retry must restore the alarm and return success in every import state. A mutable policy change must update the
stored target policy and source context. An immutable identity or slice conflict must throw
`partition_context_mismatch`. A target that has a stored partition context and no import record is also a
conflict and must throw the same error. A hash root is never a target, and a child or range partition gets its
context only from `fokosInit`, so this state is not reachable in production. M1 has no target takeover
protocol.

A hash partition can initialize from its first ordinary request. A range partition must initialize only through
`fokosInit`. The `range_partition_not_initialized` guard must remain.

`fokosStartImport` must compare the repartition and source with the import record. A target with no import
record for that repartition ID must throw `repartition_unknown`. A record with the same ID and a different
source identity must throw `partition_context_mismatch`. A valid call schedules one import step in
`awaiting_data` or `importing`. A matching call in `imported` or `active` returns success.

`fokosMigrationPull` must check these rules in order:

1. The repartition must exist. Otherwise, throw `repartition_unknown`.
2. The target identity must match a target row. Otherwise, throw `repartition_target_unknown`.
3. A source in `queued` or `planned` must throw `repartition_not_cut_over`.
4. A source in `completed` or `cleaned` must throw `partition_migrating`. Every target has
   acknowledged at that point, so no target still needs a page, and a promotion source can already
   have deleted the rows.
5. The source must return one bounded page for the requested cursor.

`fokosMigrationAck` must apply the first two checks. It must accept a repeated acknowledgement in `cutover`,
`completed`, or `cleaned`. It must throw `repartition_not_cut_over` before cutover.

### 4.7 Migration phases

A **terminal override** has a repartition row in `completed` or `cleaned`. A hash split must export exactly the
terminal overrides in each target slice. Its `items` stream must exclude exactly those keys.

The flow runs the `overrides` phase and then the `host` phase. Inside the host phase, FokosDB runs its streams
in this order:

1. `items`
2. `pending_tx`

The host phase is the `MigrationHost` interface of section 4.1.1. `buildPage` builds a source page for a host
cursor and a target slice. `applyPage` applies a host page on the target inside the page transaction. The flow
calls both and passes the values through without reading them.

#### 4.7.1 Overrides

Only a hash split returns override rows. The source must filter them by the hash-child slice and order them by
hash key.

For each imported override, the target must allocate a new local repartition sequence. It must atomically
create:

- A `key_promotion` row in `cleaned`.
- A range-root target row in `initialized`.
- `start_notified = 1` and `acknowledged = 1` on that target row.
- A route override for the hash key.

The page transaction must use one timestamp for `queued_at`, `cutover_at`, `completed_at`, and
`next_attempt_at`. It must set both attempt counts to zero and `target_index = 0`.
Multiple rows in one page must receive consecutive local sequence values.

The inherited row needs no plan or cleanup key. The child has no source item for that key. The row exists for
routing, status, and destroy traversal.

A replay after a rolled-back page is safe because none of these records committed. A replay after a committed
page cannot apply because the import cursor advanced in the same transaction.

Other repartition kinds must return an empty override page and the host phase cursor.

#### 4.7.2 Items

The source must use the existing migration item queries and `collectBatch`. It must apply the target slice as
the filter.

A hash-child slice must exclude each terminal override key. The range tree owns those keys.

The target must insert each row with `insertItemIfAbsent`. The method must report whether it inserted the row
and its exact `est_row_bytes`. The page transaction must add inserted bytes to `key_size_estimates` by hash key.

#### 4.7.3 Pending transactions

The source must apply the same slice to `pending_transactions`. The target must use `insertPendingLock` and
`mergeDeletionMetadata` in the page transaction.

The source must include deletion metadata on every page in this stream. It must return one empty page with the
metadata when the slice has no pending row.

A promoted-key slice has no lock because promotion cutover requires a zero lock count. It still receives the
deletion metadata.

#### 4.7.4 Page bounds and cursor rules

Each page must contain one phase, and a host page must contain one stream. The source owns the page byte
budget as one constant of 20 MiB. The request carries no budget. Each page must obey these limits:

- At most 20 MiB by the existing conservative row estimators.
- At most 1,000 returned data rows.
- At most 10,000 scanned source rows.

The scan cursor must advance across excluded rows. A sparse target can therefore receive an empty page with a
non-null cursor. The target must continue from that cursor.

A phase or stream that drains within the limits must return the next one with a null inner cursor. This
transition costs one extra RPC. The final `pending_tx` page must return `nextCursor: null`.

The target must validate the response phase against its requested cursor. It must reject a cursor that moves
backward or skips a phase before it starts the page transaction. The host applies the same check to its own
streams.

One target work step must pull and commit at most one page. It must not prefetch another page. This rule keeps
memory bounded and makes the durable cursor the only progress state. A pass repeats the step up to
`fokosImportPagesPerPass()` times, and each step starts after the previous page committed.

### 4.8 Read-through and request routing

`fokosExecuteLocal` replaces `internalGetItemDirect` and `internalQueryItemsDirect`:

```ts
type FokosExecuteLocalRequest =
	| {
			op: "getItem";
			repartitionId: string;
			caller: FokosPartitionRef;
			request: GetItemRpcRequest;
	  }
	| {
			op: "queryItems";
			repartitionId: string;
			caller: FokosPartitionRef;
			request: QueryItemsRpcRequest;
	  };
```

The source must apply these rules in order:

1. Seek the repartition target by `(repartitionId, caller.partitionId)` and validate the complete caller identity.
2. Reject `queued` and `planned` with `repartition_not_cut_over`.
3. Reject a promotion in `completed` or `cleaned` with `repartition_slice_reclaimed`. The rows went back to
   the range tree, so no retry can make them readable here.
4. Validate every requested key or interval against the caller slice.
5. For a promoted key from a hash-child caller, forward to the range root.
6. Otherwise, read the validated local slice without normal forwarding or lifecycle gates.

Slice validation must use the same helpers as migration and routing:

- A `hash_child` key must select the stated child and depth.
- A `range` key must match the hash key and `[start, end)` interval.
- A `promoted_key` key must match the stated hash key.
- A query interval must be clipped to the caller range.
- A point outside the slice must throw `partition_misrouted`.
- A disjoint interval must throw `partition_misrouted`.
- A cursor outside the clipped interval must throw `partition_misrouted`.

The direct local read must bypass source forwarding. The terminal-override exception must use normal
`apiGetItem` or `apiQueryItems` forwarding to the range root.

A hash target must replace the answer hash depth with its own depth. It must preserve the serving range
partition metadata when the source followed an override.

The request path must read the new records as follows:

- `shouldAllow` reads a split in `cutover` or `completed`.
- `pickChildPartition` and `walkRangeChildren` read targets in `target_index` order.
- `withSplitForwarding` and `groupItemsByRouting` use one joined override lookup.
- `ensureMigration` reads `__fokos/import.state`.
- `txPendingCanSweep` and `ttlCanSweep` read the import state and split router role.
- `HashPartitionTopologyImpl` reads a hash split in `cutover` or `completed`.

A router must build each forwarded context from its current context and the durable target slice. It must not
forward a stored mutable context.

A speculative range-root read must fall back on `repartition_not_cut_over`. The source still owns the key. A
write that reaches the same incomplete target must keep `partition_migrating` and must not fall back.

When `__fokos/destroying` is true, each normal request and control transition must fail with
`partition_migrating`. Status, prepare-destroy, and final destroy calls must remain available.

### 4.9 Cleanup, scheduling, and recovery

#### 4.9.1 Source cleanup

Every repartition reaches `cleaned` through one job. Only a key promotion reclaims source item rows.
One promotion cleanup step must:

1. Delete at most 1,000 item rows with `deleteItemsBatchForHashKey`.
2. Delete pending rows for the hash key with `deletePendingTxForHashKey`.
3. Delete the key-size estimate after the last item row.
4. Set the repartition to `cleaned`.

Hash and range split sources keep item rows. This keeps the current behavior. Their cleanup step
reclaims nothing and sets `cleaned` at once, so one job drives every kind and no caller branches on
the kind.

#### 4.9.2 Single-flight work

`runBackgroundWork` must keep one in-memory in-flight promise. A timer, alarm, or request that arrives during a
pass must request one more pass after the current pass. Two passes must not interleave.

The in-memory promise is not durable progress. Every job must read its durable state before it writes. Each
background transition must stop when `__fokos/destroying` is true. A fenced pass must not schedule a timer or
alarm.

`fokosPrepareDestroy` must atomically validate an optional root context and set the fence. It must then wait for
the in-flight promise. A pass that resumes after a remote call must see the fence and make no transition. The
method must cancel the alarm after the pass stops. A repeated call must return success.

#### 4.9.3 Jobs

The jobs run in this order:

1. `target_import`: pull and apply one page per step, up to `fokosImportPagesPerPass()` steps.
   `flow.importOnePage()` in a loop.
2. `target_ack`: make one acknowledgement attempt. `flow.ackOnce()`.
3. `source_repartition`: advance one due repartition by one bounded step. `flow.sourceStep()`.
4. `source_cleanup`: run one bounded cleanup step for one completed repartition. `flow.cleanupStep()`.
5. Stale transaction recovery.
6. TTL expiry.

`runBackgroundWork` calls the four flow methods in that order and passes each result to the alarm
computation.

`fokosImportPagesPerPass()` is an overridable `PartitionDO` method, like `fokosStaleTransactionMs()`. The
default is 16, and the minimum is 1. The import loop stops early when a step fails, when the import reaches
`imported`, or when the destroy fence is set. Import throughput matters more than the other jobs, because
every write to the target waits for it. A request that reaches an incomplete target still requests one step,
not a full pass.

`source_repartition` must select one due row by `(next_attempt_at, seq)`. A failed row moves to a later
deadline. Another due row can then run. `source_cleanup` must use the same order for completed
repartitions and must move an incomplete cleanup to a later deadline.

A repartition row is due only when the source has a step to run for it:

- A `queued` row is due when its plan deadline has passed.
- A `planned` row is due when a `pending` or `initializing` target has a passed deadline, or when every target
  is `initialized`.
- A `cutover` row is due when a target with `start_notified = 0` has a passed deadline.

A `cutover` row whose targets are all `start_notified` waits for acknowledgements. The `fokosMigrationAck` RPC
advances it. The selection query must exclude that row, so a pass does not select it, count an empty step as a
success, and reschedule itself.

Within the selected repartition, initialization and start notifications must select at most six due targets.
A step must attempt all six selected targets and record each result.

The repartition `next_attempt_at` is the deadline of its next source step. It equals the earliest
`next_attempt_at` of the targets that still need a call. When no target needs a call and a state step remains,
for example the cutover after the last initialization, it equals now. Each target result must update this value
in its own transaction.

#### 4.9.4 Retry policy

The source uses these retry delays:

- A lock-blocked promotion: 5 seconds with no backoff.
- A range plan with no boundaries: exponential from 5 seconds to 5 minutes.
- A target initialization or start failure: exponential from 5 seconds to 5 minutes.
- An incomplete cleanup: 5 seconds.

The target uses these retry delays:

- `repartition_not_cut_over`: 10 seconds with no backoff.
- Any other retryable import or acknowledgement error: exponential from 10 seconds to 5 minutes.

A successful step must reset its attempt count. A new durable work item must start due now. A non-retryable
protocol error must keep the state, log the complete identifiers, and retry no sooner than 5 minutes.

Each failed step must log the repartition ID, target identity, phase, cursor, attempt count, and next deadline.

#### 4.9.5 Alarm

After a work pass checks the destroy fence, it must arm a future fallback before it mutates state or awaits an
RPC. A crash must leave an alarm that can read the new durable state. After the pass, the alarm must move to
the earliest durable deadline. This write replaces the fallback, and it can move the alarm later. The pass is
complete at that point, so the earlier fallback protects nothing. Without this replacement, the fallback fires
at its short interval for as long as any durable work has a later deadline. The deadlines come from:

- An import in `awaiting_data`, `importing`, or `imported`.
- A repartition row that section 4.9.3 defines as due, now or later.
- A repartition in `completed`, whose cleanup is still pending.
- An unguarded pending transaction.

A cleaned repartition needs no repartition alarm.

The alarm handler must catch job errors and set a new durable deadline. Cloudflare retries a thrown alarm only
six times, so correctness must not depend on those automatic retries.

### 4.10 Concurrency and invariants

A Durable Object has one JavaScript thread, but requests interleave at `await` points.

These rules prevent ownership races:

1. Each transition uses one synchronous storage transaction.
2. Durable state is authoritative.
3. A local mutation has no `await` between owner resolution and its write.
4. Target initialization writes `initializing` before the RPC.
5. One background pass runs at a time.
6. A page transaction checks the repartition ID, import state, and expected cursor.
7. Recovery uses public `txCommit` and `txCancel` methods.
8. Cancel attempts every destination and rethrows after any child failure.

The mechanisms hold these invariants:

- A split router never serves its local item copy. `shouldAllow` forwards in `cutover` and `completed`.
- The source owns data before cutover. Pull and read-through reject `queued` and `planned`.
- Routing and migration use one ownership function. Both derive it from the durable target slice.
- A query does not read a sibling range. Each router and source clips the interval.
- Every target acknowledges before completion. Ack checks membership and counts target rows.
- A normal write does not change an incomplete target. The import gate rejects it before `imported`.
- A promoted-key read reaches the range tree. Overrides migrate first, and read-through follows them.
- Each migration step is bounded and durable. One bounded page commits with its cursor.
- A pre-split lock moves to its owner. The `pending_tx` stream completes before `imported`.
- A promotion does not move a locked key. The source checks before init and during cutover.
- A terminal promotion survives a hash split. The child receives its override and no item copy.
- A decided transaction can always commit. Commit keeps `ignore_size_reject`.
- Failed work keeps durable progress. Each job persists a guarded step and retry deadline.

The `items` table must continue to hold committed rows only. Migration copies committed item rows and separate
pending lock rows. It must not combine them.

### 4.11 Failure recovery

The flow recovers as follows:

- The source stops after `initializing`: a due pass repeats the same `fokosInit`.
- A `fokosInit` reply is lost: the idempotent retry restores the target alarm.
- The source stops after cutover: each target starts from its own alarm.
- The target stops during import: it resumes from the committed cursor.
- The target stops after `imported`: `target_ack` retries.
- The source cannot accept an acknowledgement: the imported target continues to serve and retry.
- A range split stops after partial initialization: its durable plan and target states remain.
- A pull reaches the source before cutover: the target retries `repartition_not_cut_over`.
- A promotion key has a lock: its target stays `pending`.
- A lock appears during initialization: the promotion cutover guard fails.
- One selected target RPC fails: other calls finish, and only failed targets retry.
- One repartition keeps failing: its later deadline lets another due row run.
- An alarm job fails: it records another deadline before the alarm handler returns.
- Destroy starts during a target RPC: the durable target row exists, and the fence waits for the source pass.
- A cache misses or evicts an entry: the caller reads SQL and KV.

### 4.12 Status, destroy, errors, performance, and deployment

#### 4.12.1 Compatibility `status()`

`status()` must derive its existing fields from the new records:

- `splitStatus`: `queued` and `planned` map to `split_queued`.
- `splitStatus`: `cutover` maps to `split_started`.
- `splitStatus`: `completed` maps to `split_completed`.
- `migrationStatus`: `awaiting_data` maps to `migration_initialized`.
- `migrationStatus`: `importing` maps to `migration_migrating`.
- `migrationStatus`: `imported` and `active` map to `migration_completed`.
- `promotedKeys`: `queued` and `planned` map to `queued`.
- `promotedKeys`: `cutover` maps to `promoting`.
- `promotedKeys`: `completed` and `cleaned` map to `promoted`.
- `parentPartitionContext` comes from `__fokos/import.source`.
- The source kind derives `parentSplitType` and `splitType`.
- `createdAt` uses `queued_at`, `cutover_at`, or `completed_at` for its mapped split state.
- `migratedChildDoNames` comes from acknowledged split targets.
- `history` is the derived list of earlier mapped split states.
- `partitionContext` uses the source's current context.

Split target contexts must use `target_index` order and the source's current mutable context.

#### 4.12.2 Paginated `fokosStatus`

M1 must add this bounded administration view:

```ts
type FokosStatusCursor = {
	seq: number;
	targetIndex: number;
};

type FokosStatusEntry = {
	repartition: {
		id: string;
		seq: number;
		kind: "hash_split" | "range_split" | "key_promotion";
		state: "queued" | "planned" | "cutover" | "completed" | "cleaned";
	};
	target: null | {
		index: number;
		ref: FokosPartitionRef;
		initialization: "pending" | "initializing" | "initialized";
		acknowledged: boolean;
	};
};

type FokosStatusPage = {
	initialized: boolean;
	destroying: boolean;
	partitionContext: PartitionContextLivePartition | null;
	importState: FokosImportRecord["state"] | null;
	entries: FokosStatusEntry[];
	nextCursor: FokosStatusCursor | null;
};

type FokosStatusRequest = {
	cursor: FokosStatusCursor | null;
	rootContext?: PartitionContextResolved;
};

interface FokosPartitionStatusRpc {
	fokosPrepareDestroy(req: { rootContext?: PartitionContextResolved }): Promise<void>;
	fokosStatus(req: FokosStatusRequest): Promise<FokosStatusPage>;
	destroyPartition(): Promise<void>;
}
```

Each page must:

- Order entries by `(seq, target_index)`.
- Use `targetIndex = -1` for a repartition that has no target.
- Represent that repartition as one entry with `target: null`.
- Return at most 1,000 entries.
- Use a conservative serialized-byte estimator and stay at or below 20 MiB.
- Resume strictly after the cursor.

Diagnostic pages can reflect transitions that occur between calls. Destroy traversal sets the fence first, so
its pages read a stable link set.

A root status request can carry its root context for bootstrap. A target status request must omit it. The target
request must not initialize an empty partition. It must return `initialized: false`, an empty entry list, and a
null cursor.

`traverseForDestroy` must apply this sequence to each partition:

1. Call `fokosPrepareDestroy`. Pass a context only when it bootstraps a root.
2. Read every `fokosStatus` page after the fence is active.
3. Walk each target before it destroys the source.
4. Deduplicate target `doName` values across the full traversal.
5. Call `destroyPartition` after all target calls finish.

A `pending` target has received no initialization call and is a leaf. An `initializing` target can be empty or
initialized. Traversal must fence it, then check its status without a root context. An uninitialized target must
be destroyed as a leaf.

This traversal must include `pending`, `initializing`, and `initialized` target rows. Pagination must prevent an
unbounded promotion count from blocking destroy. The fence must prevent a source from adding a target after
traversal reads its final page.

#### 4.12.3 Errors

M0 adds `repartition_target_unknown` as a non-retryable `FokosInternalError`. The target is not a member.

M1 adds three more codes in `packages/fokosdb/src/shared/errors.ts`:

- `repartition_not_cut_over` is a retryable `FokosUnavailableError`. The source still owns the slice.
- `repartition_unknown` is a non-retryable `FokosInternalError`. The repartition does not exist.
- `repartition_slice_reclaimed` is a non-retryable `FokosInternalError`. A read-through caller asked for a
  promoted key whose rows the source already gave back.

A speculative read must fall back on `repartition_not_cut_over`. If this code reaches the public root,
`withFokosErrors` must map it to `partition_migrating`. The mapped error must keep the internal code in
`attributes.runtimeCode` and keep the original `error_id`.

#### 4.12.4 Performance

A point operation on a hash partition performs one indexed override join. This matches the current
`PromotionManager.statusFor` SQL lookup.

A source keeps only its split row and at most 255 split targets in memory. Promotion rows remain in SQL. Each
production promotion query must use an index and a bounded result. The test-only `status()` compatibility view
keeps its current unbounded shape.

Migration adds one RPC for each phase transition. It uses one data RPC for each bounded page. One pass applies
up to `fokosImportPagesPerPass()` pages, one at a time.

#### 4.12.5 Deployment and rollback

M0 uses the current durable records. A code revert can roll it back.

M1 has no compatibility with records from the current implementation. Deployment must use fresh Durable Object
namespaces or fresh table names. M1 replaces source and target records in one deployment.

An M1 rollback needs the old code and the old namespace. It cannot read M1 records.

### 4.13 Defects that M0 must fix

#### D1 — Concurrent import loops can restore deleted state

`scheduleBackgroundWork` releases its marker after 1 second while work can still run. An alarm can start a
second `SplitMigration` loop.

Failure sequence:

1. Loop A imports a page and finishes the import.
2. A user deletes an imported item on the active target.
3. Loop B commits an older page that it already holds.
4. `INSERT OR IGNORE` restores the deleted item because the row is absent.

The same sequence can restore a pending lock.

M0 must use one in-memory import promise. Each page transaction must recheck the durable migration state. An
item page must also check its expected durable cursor. M1 applies the cursor check to every phase and stream.

M0 does all three. It also removes the batch prefetch, so the durable checkpoint is the only record of
progress. One pass still applies more than one page: section 3 records why. `SplitMigration` keeps one guard
for the three streams, because the hash and the range drivers now share their page loops.

#### D2 — A promoted-key read through an importing hash child can use stale source rows

The current read-through calls the parent direct-read RPC before it resolves promotions. A hash split imports
promoted-key metadata after item rows.

Failure sequence:

1. Hash leaf L promotes key K.
2. L later splits and creates child C.
3. A cache routes a read of K to C during import.
4. C reads K from L's local item rows.
5. Promotion cleanup can make those rows stale or absent.

M0 must use `fokosExecuteLocal`. The source must validate the caller and follow a terminal promotion override.
M1 must also import overrides before item rows.

#### D3 — A range router forwards stale mutable policy

`SplitStateMachine` stores full child contexts at split time. The range router later forwards those stored
contexts. A child can then replace new split thresholds with old values.

M0 must build each forwarded child context from the router's current context and the stored immutable
boundaries.

### 4.14 Testing

The replaced unit suites move to `packages/fokosdb/test/repartition/repartition-flow.test.ts`. They must keep
their current cases. The suite and its harness live under `test/`, not beside the source: they drive several real
partitions at once and need a harness, which is not what a unit test beside its module looks like.

That suite must run several partitions' source and target halves over real `PartitionStore` instances in one
process.
The peer factory of the target returns the source instance, so a control call lands on it directly. The suite
drives each step by hand and inspects the rows between steps. It must cover the arbitration table, the due-row
rules, the cursor and phase validation, the retry deadlines, and every recovery case of section 4.11, for each
repartition kind. It needs no Durable Object.

Integration tests that seed the old durable records directly must change. `tx-stale-recovery.test.ts` writes
`__split_status` and `__split_migration_status` to set up its sweep-guard cases. M1 must seed
`fokos_repartitions`, `fokos_repartition_targets`, and `__fokos/import` instead. An integration test that only
proves a split or migration transition, and that a new unit suite proves directly on the extracted component,
can be deleted. A test that proves request behavior during a transition must stay.

The harness changes are:

- `withMigrationHeld` must hold `fokosMigrationPull` in the `pending_tx` stream.
- `withMigrationBatchCap` must cap each phase and stream of `fokosMigrationPull`.

M0 adds tests for these behaviors. Each one started as a failing test for its defect:

- A stale import step cannot restore a deleted item or pending lock (D1). Three cases in `migration.test.ts`
  cover the item state guard, the item cursor guard, and the pending-lock state guard. The suite drives a real
  `PartitionStore` and moves the durable state while a page is on the wire.
- Promoted-key point and query reads through an importing hash child reach the range tree (D2).
- An unknown target cannot read through (D2). A caller must match both `doName` and `partitionId`.
- A hash child cannot read a sibling's key through its source (D2).
- A broad query is clipped to one importing range child, and a cursor outside that slice fails (D2).
- A range router forwards its current mutable context (D3).

`test/partition-do/read-through.test.ts` holds the `fokosExecuteLocal` cases. `TestPartition.localItemCount`
replaces the removed direct-read RPC where a test must read a partition's own rows.

M1 must add integration tests for these behaviors:

- Item page retries keep exact key-size estimates without a final full-table scan.
- An empty page with a non-null scan cursor resumes correctly.
- A pull scans at most 10,000 source rows.
- A target in `imported` retries its acknowledgement after restart.
- Idempotent `fokosInit` restores a deleted alarm.
- A repeated source signal restores a deleted alarm.
- A target starts without `fokosStartImport`.
- A conflicting `fokosInit` cannot replace an import.
- An unknown target cannot pull or acknowledge.
- A lock-blocked promotion prevents a hash split.
- A terminal promotion survives a hash split.
- The owning child receives the override and no item copy.
- A promotion cannot queue after a hash split exists.
- A forced promotion routes to the current owner.
- A forced promotion during child import creates no row.
- A retry after child import queues the promotion on that child.
- A Bloom false positive before cutover falls back for reads only.
- A range split reuses its plan after partial initialization.
- A maximum-shape range plan fits the KV limit.
- `repartition_not_cut_over` maps to `partition_migrating` at the public root.
- Cache eviction does not change routing.
- One failed promotion does not starve another due promotion.
- One failed target RPC does not skip another selected target.
- Destroy reaches every target state through paginated status.
- A destroy fence waits for an in-flight target RPC and prevents another transition.
- Each `fokosStatus` page stays within both page limits.
- A 100 MB migration benchmark records the before-M1 and after-M1 results.

Existing transaction tests must continue to prove:

- `PREPARED` always commits.
- Commit and cancel bypass size rejection but still route.
- Cancel attempts all child destinations and reports a partial failure.
- Recovery uses public commit and cancel paths.

## 5. Alternative options

### 5.1 Keep the two source state models

This option cannot make split and promotion arbitration atomic. It also adds retry and membership fields to both
models. It produces most of M1 without removing duplicate code.

### 5.2 Build the runtime package first

The runtime RFC has earlier package and dispatch milestones. Its durable repartition work arrives later. The
pre-release schema window can close before that work completes.

### 5.3 Convert current records

No released deployment needs conversion. Fresh namespaces or table names make a converter unnecessary.

### 5.4 Keep one RPC for each migration record type

Three pull RPCs need three cursors and three authorization paths. One phased cursor uses one path. It costs one
small RPC at each phase or stream transition. The opaque host phase also lets a later runtime package keep the
flow unchanged while an application defines its own streams.

### 5.5 Store each target slice as one BLOB

The runtime RFC uses a BLOB because its runtime does not query inside a slice. M1 filters migration rows and
orders range children in SQL. Columns support both operations.

### 5.6 Let a hash split abandon a pending promotion

This option needs a durable cancellation fence once target initialization can start. It also needs exact
per-hash-key size reservations across concurrent prepares. Without reservations, several accepted transactions
can exceed a key cap before commit.

A copied hot key can also cause repeated hash splits before its next promotion. Keeping mutual exclusion avoids
these states and preserves current behavior.

### 5.7 Prefetch one migration page

Two 20 MiB serialized pages can have more than 40 MiB of live decoded data. A Workers isolate has 128 MB of
memory. Prefetch also adds a second cursor identity and stale-page handling. M1 uses one page at a time.

## 6. Frequently asked questions

### Does a client see a change?

No. Public methods, public error codes, and the transaction coordinator protocol do not change. A write to an
importing target still fails with `partition_migrating`.

### Why does `status()` keep its old fields?

The existing partition tests use those fields. The derived view keeps those tests useful during M1.

### Why use `fokos*` RPC names now?

M1 replaces the complete internal wire protocol. The names match the later runtime package and need no second
migration.

### Can an imported target start its own repartition before its acknowledgement succeeds?

Yes. The `imported` state has complete data and locks. Its target acknowledgement continues as a separate job.
The partition can be a target of one repartition and the source of another.

### Can a promotion cut over while its hash child imports?

No. The migration gate rejects the promotion request. The caller can retry after import completes.

### Why does a lock-blocked promotion also block a hash split?

A split cannot move or abandon an initialized promotion without a cancellation fence. The current system also
keeps these flows mutually exclusive. This RFC keeps that rule and fixes its atomicity.

### Why does scheduling use the earliest due row?

A random order can scan the eligible set and has no starvation bound. Ordering by `(next_attempt_at, seq)` uses
an index. A failed row moves behind another due row.

## 7. References

- `docs/ideas/fokos-sharding/2026-09-09-fokos-partition-runtime.md`
- `docs/agent-plans/range-partition-splits-v2.md`
- `docs/agent-plans/promoted-keys-bloom-filter-cache.md`
- `packages/fokosdb/src/server/do-partition.ts`
- `packages/fokosdb/src/shared/partition-topology/split-state.ts`
- `packages/fokosdb/src/shared/partition-topology/split-policy.ts`
- `packages/fokosdb/src/shared/partition-topology/partition-id.ts`
- `packages/fokosdb/src/shared/partition-topology/router.ts`
- `packages/fokosdb/src/shared/partition/migration.ts`
- `packages/fokosdb/src/shared/partition/hash-key-promotion.ts`
- `packages/fokosdb/src/shared/partition/partition-store.ts`
- `packages/fokosdb/src/shared/partition/partition-peer.ts`
- [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
- [Durable Objects Alarms API](https://developers.cloudflare.com/durable-objects/api/alarms/)
- [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Workers limits](https://developers.cloudflare.com/workers/platform/limits/)
- [Workers RPC](https://developers.cloudflare.com/workers/runtime-apis/rpc/#limitations)
