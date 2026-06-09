# ADR: Layering and component extraction for `src/lib/`

- **Status:** Accepted (2026-06-09)
- **Scope:** `apps/fokos-svc/src/lib/` — PartitionDO, TransactionCoordinatorDO, partition-topology
- **Deciders:** Lambros + Claude (analysis session)

## Context

The codebase has an implicit four-layer architecture that is mostly correct:

1. **Client/API** — `FokosDB` (`db.ts`): resolves a partition for a key, calls the DO.
2. **Coordination DOs** — `PartitionDO` (`do-partition.ts`, ~2,200 lines) and
   `TransactionCoordinatorDO` (`do-transaction-coordinator.ts`).
3. **Topology** — `partition-topology/`: routing math, partition identity encoding, split policy.
4. **Primitives** — `hash-primitives.ts`, `hash-topology.ts`, `bloom-filter.ts`, `invariant.ts`,
   `tsutils.ts`. Already pure, small, and tested.

`TransactionCoordinatorDO` is a clean, self-contained state machine. The problems concentrate in
`do-partition.ts` and `partition-topology.ts`, where boundaries leak:

1. **Layering inversion in topology.** `HashPartitionTopologyImpl.startSplit()` and
   `RangePartitionTopologyImpl.startSplit()` resolve child DO stubs via the global `env` and call
   `initFromSplit`/`triggerMigration` RPCs. `computeRangeSplitBoundaries()` queries the `items`
   table and `shouldSplit()` counts `promoted_keys` rows — both PartitionDO-owned tables. This is
   also the root cause of the circular import (`partition-topology.ts` ↔ `do-partition.ts`) and of
   the duplicated hand-written `PartitionDOStub` structural types.
2. **Scattered raw SQL with real duplication.** The `items` upsert with `est_row_bytes` /
   `key_size_estimates` bookkeeping exists twice (`putItem`, `applyCommitItems`) and must stay in
   lockstep or size accounting drifts. The deletion-watermark update appears three times;
   pending-lock checks four times. No single owner of the partition's storage model.
3. **The byte-budgeted cursor scan is copy-pasted ~5 times.** `getItemsBatch`,
   `getItemsBatchForRange`, two variants in `getPartitionTransactionMetadata`, and
   `getPromotedKeysBatch` are the same algorithm: page with a composite cursor, filter, accumulate
   to a byte budget, return `{rows, nextCursor}`. Cursor-resume correctness under crash/retry is
   load-bearing for migration and has no direct unit tests.
4. **Control plane and data plane share one class.** PartitionDO is simultaneously CRUD executor,
   2PC participant, split-forwarding router, migration source and sink, promotion lifecycle
   driver, GC, stale-tx recoverer, and a hand-rolled background scheduler. Each is coherent;
   none is testable in isolation.
5. **Smaller leaks:** transact-write validation (100 items / 4 MB / duplicate keys) duplicated
   between `db.ts` and the TC with separately defined constants; the
   `string | ArrayBuffer → string | Uint8Array` SQL data conversion repeated ~8 times;
   `FokosDB.destroy()` carries tree traversal its own TODO says belongs in the topology router.

## Decision

Keep `PartitionDO` as the single main data-partition Durable Object. Extract collaborators that
the DO **owns and wires**, each independently testable. Do not split the DO into multiple DOs, do
not introduce a storage abstraction/ORM, do not add interfaces speculatively.

### Target structure

```
src/lib/
  db.ts                          FokosDB client (unchanged role)
  do-partition.ts                PartitionDO: RPC surface, context guard, wiring ONLY
  do-transaction-coordinator.ts  mostly as-is; shares transaction-limits.ts
  partition/
    partition-store.ts           ALL SQL for items / pending_transactions / deletion_metadata /
                                 key_size_estimates / promoted_keys + schema migrations +
                                 estimateRowBytes; owns computeRangeSplitBoundaries
    batch-scan.ts                the shared byte-budgeted, cursor-paged scan helper
    partition-peer.ts            the single PartitionPeer gateway interface — the narrow
                                 RPC surface components need from other PartitionDOs
                                 (replaces the hand-written structural stub types)
    transaction-participant.ts   prepareLocal/commitLocal/cancelLocal/readForTransactionLocal +
                                 stale-tx listing, on top of PartitionStore
    migration.ts                 child-side migration PULL DRIVER only; parent-side batch
                                 serving stays as thin DO methods over store + batch-scan
    promotion.ts                 promoted-keys in-memory cache + queued→promoting→promoted
                                 lifecycle + GC (cache and lifecycle move as ONE unit)
    background-scheduler.ts      alarm/setTimeout dedupe machinery; jobs registered as
                                 idempotent closures (extracted LAST — independent of the rest)
  partition-topology/
    partition-context.ts         PartitionContext types, creator/validation, equality fns
    partition-id.ts              PartitionIdHelper + rangePartitionDoName (pure codec)
    router.ts                    PartitionTopologyRouterImpl (client-side) + destroy traversal
    split-state.ts               the shared KV-backed SplitStateMachine
                                 (split_queued → split_started → split_completed + ack
                                 bookkeeping) — today duplicated ~verbatim between the
                                 hash and range topology impls
    split-policy.ts              Hash/Range topology impls: shouldAllow / shouldSplit /
                                 pickChildPartition / prepareSplit — pure decisions +
                                 their own KV state via split-state.ts; NO stubs, NO env,
                                 NO SQL on partition-owned tables
    hash-topology.ts             as-is
    hash-primitives.ts           as-is
    types.ts                     as-is
  transaction-limits.ts          shared validation constants + checks for db.ts and the TC
  bloom-filter.ts                UNTOUCHED — needed for an upcoming feature (caller-side
                                 routing caches per range-partition-splits-v2 deferred scope)
```

### Post-refactor component relationships (overview)

```
                     RPC                            RPC
  FokosDB ────────► PartitionDO ◄──────────── TransactionCoordinatorDO
     │                  │    ▲                          │
     │ pickPartition    │    └─ RPC to peer PartitionDOs│
     ▼                  │       (forward / split init / │
 TopologyRouter         │       migrate) — stubs built  │
     │                  │       ONLY here, handed down  ├── transaction-limits
     │                  │       as PartitionPeer        │   (shared w/ FokosDB)
     │           wires  │ (constructor DI)
     │     ┌────────────┼────────────┬──────────────┬──────────────┐
     │     ▼            ▼            ▼              ▼              ▼
     │ SplitPolicy  Transaction  SplitMigration  Promotion    Background
     │ (hash|range) Participant  (child driver   Manager      Scheduler
     │    │    │        │         + PartitionPeer) (+ peer)   (runs DO's jobs)
     │    │    ▼        │            │              │
     │    │ SplitState- │            │              │
     │    │ Machine(KV) │            │              │
     │    ▼             └────────────┴───┬──────────┘
     │ HashTopology                      ▼
     │ (arena cache)              PartitionStore ──► batch-scan
     │                                   │
     │                                   ▼
     │                             SQL / KV storage
     │
     └──────► partition-id / partition-context   (pure codec & types — leaf
              dependencies used by everything above; no arrows back up)
```

Reading rules for the diagram: every RPC arrow originates at a DO class (or FokosDB) — the
components below `PartitionDO` reach remote DOs only through a `PartitionPeer` instance handed to
them; `PartitionStore` is the sole owner of partition SQL tables; `partition-id` /
`partition-context` are leaf dependencies with no imports back into anything above them.

### The boundary rule (the one invariant that matters most)

> **Components and topology decide; only DO classes (and FokosDB) hold stubs and make RPCs.**

Concretely:

- `split-policy.ts` returns _"create these N child contexts"_ / _"transition split status"_;
  `PartitionDO` performs the `initFromSplit` / `triggerMigration` fan-out in its background work.
- Inputs the policies need from the DO (e.g. "are promotions in flight?", "split boundaries for
  `[start, end)`") become parameters or `PartitionStore` calls — never embedded SQL in topology.
- This breaks the `partition-topology.ts` ↔ `do-partition.ts` import cycle and removes the need
  for duplicated structural stub types.

Precise interpretation of the rule:

- **Allowed everywhere:** resolving a `DurableObjectId` via `ns.idFromName(name)` — it is
  deterministic and performs no I/O. The router and `resolveRangePartitionContext` keep doing this.
- **DO/FokosDB only:** acquiring a stub (`ns.get(id)`) and invoking RPC methods on it. Outside
  DOs/FokosDB, touching the global `env` from `cloudflare:workers` is permitted solely to reach
  a namespace for `idFromName` ID resolution (`partition-id.ts` / `router.ts`) — never to call
  `.get(...)`.
- **Gateway pattern for components that inherently talk to remote DOs** (migration driver,
  promotion lifecycle): one shared `PartitionPeer` interface (`partition/partition-peer.ts`)
  lists the remote PartitionDO methods components may call — it subsumes today's hand-written
  `PartitionDOStub` / `ParentPartitionDOStub` structural types. The DO constructs the stub
  (which satisfies `PartitionPeer` structurally, no wrapper) and passes it in, or passes a
  factory `(ctx) => PartitionPeer`. Components never resolve stubs themselves; components
  needing only a subset take a `Pick<PartitionPeer, ...>`. This is what makes them testable
  with an in-memory fake gateway.

### Supporting design rules

1. **PartitionStore is the only writer of partition tables.** One class, cohesive method groups
   per table; not split per-table. Raw SQL is fine once it lives in one place. The items upsert,
   deletion watermark, and size-estimate bookkeeping each exist exactly once.
2. **One batch-scan implementation.** All five paged-streaming endpoints are expressed through
   `batch-scan.ts` (page function + filter + byte estimator + budget → `{rows, nextCursor}`).
   Crash/resume semantics get direct unit tests.
3. **Components receive dependencies via constructor from the DO** (storage / store /
   `DurableObjectState` as needed). No DI framework, no service locator — plain constructor
   arguments wired in the PartitionDO constructor.
4. **`withSplitForwarding` and `groupItemsByRouting` stay in PartitionDO.** They are the DO's core
   routing duty, need stubs by definition, and are already well-factored private methods.
5. **Promotion moves with its cache.** The `#_promotedKeys` in-memory map is read by hot-path
   routing; extracting the lifecycle without the cache (or vice versa) would split one consistency
   domain across two owners.
6. **Test against real DO storage.** Vitest workers pool provides real SQLite-in-DO semantics
   (`transactionSync`, KV, alarms). Components are tested against that, not against fakes of
   platform semantics.
7. **No platform abstraction.** DO-native primitives (alarms, `blockConcurrencyWhile`,
   `transactionSync`) ARE the architecture. Abstracting them would mean faking exactly the
   semantics that need to be true.

### Construction & testability guidelines

- **Default to plain `constructor(deps)` with explicit dependency injection.** Every component
  takes its collaborators (store, `DurableObjectStorage`, gateway interfaces, config values) as
  constructor arguments. All wiring happens in one place: the `PartitionDO` constructor (or
  lazily in `ensureX()` accessors where the partition context is needed first). Tests construct
  components directly with fakes — no module mocking, no factory interception needed.
- **Do NOT adopt a blanket `Class.create()` convention.** Static factories only aid
  mocking when a class internally creates its own collaborators; constructor injection removes
  that situation entirely. A mandatory `.create()` would be boilerplate with no testability gain.
- **Use a private constructor + named static factories only when construction is non-trivial,**
  i.e. one of: multiple construction paths with computed state (existing precedent:
  `HashTopology.create()` / `HashTopology.fromSnapshot()`), input validation/normalization that
  produces a different shape (existing precedent: `PartitionContextCreator.create()`), or
  construction requiring async work. Name factories by intent (`fromSnapshot`, `forRangeRoot`),
  not a generic `create`, when there is more than one.
- **Stub the dependency, not the construction.** If a test needs to alter a component's
  behavior, pass a fake gateway/store/clock value through the constructor. Reach for `vi.spyOn`
  on an instance method next, and module-level `vi.mock` only as a last resort.
- **Keep `__testing__*` hooks on the DO only.** Extracted components must be controllable through
  their constructor inputs (e.g. `batchLimitBytes` becomes a constructor option of the migration
  component instead of the `__testing__migrationBatchLimitBytes` field).

## Alternatives considered

### A. Role/strategy decomposition — rejected

Model the DO's modes (hash leaf, hash router, range leaf, range router, migrating child) as
strategy objects. Rejected because roles transition mid-flight (a leaf becomes a router while
requests are in the air; a migrating child serves reads from its parent), and most behavior is
shared across roles. The outcome would be duplicated logic per role or a base-class web, while
`withSplitForwarding` + `shouldAllow` already encode the transitions in ~80 lines.
State-as-data beats state-as-objects here.

### B. Coarse two-module split (data plane / control plane) — rejected, viable fallback

Just `data-plane.ts` and `control-plane.ts` over a shared store. Fewer seams and less churn, but
migration cursor-resume, promotion cutover, and the scheduler have independent failure modes and
would remain tangled in one ~1,000-line control-plane file. Since the extracted components depend
only on the store and the DO (not on each other), the finer split costs almost nothing extra.
If mid-refactor the finer split proves heavier than expected, collapsing into this shape is the
sanctioned fallback.

### C. Ports-and-adapters / platform-agnostic core — rejected

Interfaces for storage, RPC, and clock injected everywhere. Textbook overengineering for a
DO-native system; see design rule 7.

## Implementation phases

### Ordering rationale

The phases are ordered to front-load impact and to guarantee that pausing between (and at marked
checkpoints within) phases never leaves the tree in a transitional/broken state — "not broken" is
defined as the full test suite green; there are no feature flags or dual code paths anywhere in
this plan, so any green checkpoint is a shippable state.

- **Phase 0** is independent quick wins (minutes each, zero risk).
- **Phases 1+2 capture most of the total value** — phase 1 eliminates the one active bug source
  (the items-upsert/size-estimate logic duplicated between `putItem` and `applyCommitItems` that
  must stay in lockstep) and is the foundation everything else builds on; phase 2 is the biggest
  architectural fix (layering inversion + import cycle).
- **Phases 3–5 are deferrable testability dividends.** Stopping after phase 2 for weeks leaves
  nothing half-done — the remaining extractions are independent improvements, not cleanup of a
  transitional state.
- **Phase 6 (background scheduler) goes last** because it is mostly independent of the rest:
  it touches only the scheduling machinery, no other phase depends on it, and it carries the
  plan's largest sanctioned behavior change — best done once everything else is settled.
- The 2PC participant (phase 5) runs over a by-then-proven store, when refactor confidence is
  highest.

Within phases, **green checkpoints** are marked — states safe to walk away from for days. The
single exception requiring one uninterrupted sitting is called out in phase 2.

Git operations (commits, branches, PRs) are managed exclusively by Lambros — agents executing a
phase must NOT run any git commands; they only apply the described changes to the working tree
and report what changed.

### Execution protocol (applies to every phase)

- **Code motion first:** move symbols verbatim, update imports, keep behavior bit-identical, and
  verify green before applying any behavior change. Behavior changes (only where a phase
  explicitly calls for them) are applied and reported as a distinct, separately-reviewable step.
- Verify after every step: `npm test` (runs `tsc --noEmit`, prettier check, and the vitest
  suite). For tight loops while iterating, use `vitest run -t "<test name>"` — the full
  `do-partition.test.ts` is slow; do not loop it.
- The existing integration tests (`do-partition.test.ts`, `partition-topology.test.ts`,
  `hash-topology.test.ts`) are the behavioral safety net. Do not weaken or delete assertions to
  make a phase pass; if a test fails, the refactor introduced a behavior change — fix the
  refactor.
- Preserve all existing log shapes (`logParams()` spreads, `message:` strings) — they feed WOBS
  plotting (see commit `ccbeb03`).
- Do not touch `bloom-filter.ts`, the 2PC wire types (`transaction-types.ts`), or the public
  result shapes in `types.ts`.

### Phase 0 — independent quick wins

**Goal:** zero-risk cleanups with no dependency on any other phase.

1. Delete the dead `#topologyRouter` field in `HashPartitionTopologyImpl`
   (`partition-topology.ts:631` and `:640`) — assigned in the constructor, never read. After
   this, the router is exclusively a FokosDB-side class.
2. Create `transaction-limits.ts`: `MAX_ITEMS_PER_TRANSACTION = 100`,
   `MAX_PAYLOAD_BYTES = 4 MB`, `validateTransactWriteOperations(ops)` covering count, duplicate
   keys, payload bytes, and put-requires-data. Both `FokosDB.transactWriteItems` and
   `TransactionCoordinatorDO.validateWriteRequest` call it.
   - **Sanctioned behavior change (apply as a distinct step after the motion is green):** this
     adds the put-requires-data check to the TC path (today only `db.ts` has it) — an intended
     alignment; call it out explicitly when reporting the changes.

**Tests:** `transaction-limits.test.ts` for the shared validator.

**Acceptance:** one definition of the transaction limits; no `topologyRouter` references; suite
green. Each of the two items is independently a green checkpoint.

### Phase 1 — `partition/partition-store.ts` + `partition/batch-scan.ts`

**Goal:** all SQL on partition-owned tables lives in one class; the byte-budgeted cursor scan has
one implementation.

**Green checkpoints:** (a) `batch-scan.ts` + its unit tests landed with nothing else changed;
(b) each table's SQL moved into the store — migrate table-by-table (`items` first, then
`pending_transactions`, then `deletion_metadata` / `key_size_estimates` / `promoted_keys`), each
move independently green. The five scan-loop rewrites ride along with their table or come last.

1. Create `partition/batch-scan.ts` exporting a generic `collectBatch<TRow, TCursor>` taking:
   `fetchPage(cursor, pageSize): TRow[]`, `advanceCursor(row): TCursor`,
   `include?(row): boolean`, `estimateBytes(row): number`, `budgetBytes`, `pageSize`,
   `startCursor`. Returns `{ rows, nextCursor }`. Preserve the exact current semantics:
   - the cursor advances past **every scanned row**, matched or not;
   - the first matched row is always included even if it alone exceeds the budget
     (the `rows.length > 0 && total + bytes > budget` guard);
   - scanning stops when a fetched page is shorter than `pageSize`;
   - `nextCursor` is non-null only when the byte budget stopped the scan.
2. Create `partition/partition-store.ts` (`class PartitionStore`, plain constructor taking
   `DurableObjectStorage`). Move from `do-partition.ts`:
   - the `sqlMigrations` array and the `SQLSchemaMigrations` setup (expose `runMigrations()`);
   - `estimateRowBytes`, `estimateItemBytes`, `estimatePendingTxBytes`;
   - `evaluateConditionsOnItem` + the `ItemSnapshot` type — pure condition evaluation used by
     BOTH the non-transactional `putItem`/`deleteItem` (stay in the DO) and, later, the
     transaction participant; it must NOT end up inside `transaction-participant.ts`;
   - one `fromSqlData(value)` helper for the `string | ArrayBuffer → string | Uint8Array`
     conversion currently repeated ~8 times — all store row-reading methods return
     already-converted data;
   - row/cursor types: `MigratedItem`, `PendingTransactionRow`, `PendingTransactionCursor`,
     `MigrationCursor`, `PromotedKeyCursor`, `PromotedKeyStatus`;
   - methods (single-purpose, named for intent — final naming at implementer's discretion):
     `getItem`, `upsertItem` (the items upsert + `key_size_estimates` maintenance, used by BOTH
     `putItem` and `applyCommitItems` — this is the de-duplication), `deleteItem` (with
     watermark + size-estimate updates), `pendingLockFor(hk, sk)`, `insertPendingLock(s)`,
     `deletePendingTx(transactionId)`, `listStalePendingTx(olderThanMs, limit)`,
     `getMaxDeletedTs` / `bumpMaxDeletedTs(ts)`, promoted-keys CRUD (`listPromotedKeys`,
     `insertPromotedKey`, `updatePromotedKeyStatus`), `queryItemsPage`, `queryPendingTxPage`,
     `queryPromotedKeysPage`, `rebuildKeySizeEstimates()` (the post-migration
     `INSERT ... SELECT hk, SUM(...)`), `databaseSize` getter.
   - Store methods return `{ rowsRead, rowsWritten }` metrics where the DO currently surfaces
     them in `meta` (keep `sumSqlMetrics` semantics intact).
   - **Transaction composition stays at the caller.** Expose a `transactionSync<T>(fn)`
     passthrough; multi-statement atomicity (e.g. `commitLocal`,
     `acknowledgeChildMigrationComplete`) is composed by the DO around store calls, exactly
     mirroring today's `this.ctx.storage.transactionSync(...)` blocks.
3. Rewrite the five paged endpoints (`getItemsBatch` hash filter, `getItemsBatchForRange`, the
   two `getPartitionTransactionMetadata` variants, `getPromotedKeysBatch`) through
   `collectBatch` over the store's page queries. The DO keeps the authorization invariants
   (promoting-status checks, known-child checks) — only the scan loop moves.
4. `PartitionDO` constructs `#store` in its constructor and uses it everywhere.

**Tests:** new `batch-scan.test.ts` (pure unit: cursor resume, byte budget incl. single-oversized
row, filtering, page-boundary cases) and `partition-store.test.ts` (against real DO storage via
the same vitest-pool-workers harness as `do-partition.test.ts`: upsert version increments,
size-estimate bookkeeping across put/overwrite/delete, watermark monotonicity).

**Acceptance:** zero `storage.sql.exec` / `storage.kv` table-access calls remain in
`do-partition.ts` outside `#store` (KV keys for partition context / migration status may stay in
the DO for now — they move in later phases); five scan loops replaced by one helper; full suite
green with no test edits.

### Phase 2 — topology split

**Goal:** `partition-topology.ts` split by responsibility; no RPC, no `env` access, and no SQL on
partition tables anywhere in topology; import cycle with `do-partition.ts` eliminated; the
duplicated split-status state machine unified.

**Green checkpoints:** (a) steps 1–3 are pure file moves behind the re-export barrel — fully
pausable after each; (b) step 4's `prepareSplit`/`commitSplitStarted` rework is **the one step in
the whole plan to finish in a single uninterrupted sitting** (a few hours) — it is the only place
where stopping halfway leaves orchestration logic split across two homes. Steps 5–6 are again
pausable.

1. Create `partition-topology/partition-context.ts`: `PartitionContext`,
   `PartitionContextResolved`, `SplitConditions`, `isHashPartition`, `isRangePartition`,
   `areImmutableOptionsEqual`, `areMutableOptionsEqual`, `PartitionContextCreator`. Also move
   `InitFromSplitOptions` here (from `do-partition.ts`) — it is a context-level type and moving
   it breaks the cycle.
2. Create `partition-topology/partition-id.ts`: `PartitionIdHelper`, `RANGE_MIN`/`RANGE_MAX`,
   `encodeRangeComponent`, `rangePartitionDoName`, `resolveRangePartitionContext`,
   `GOLDEN_RATIO`/`hashChildIndex`/`hashRootIndex` re-exports. `idFromName` resolution is allowed
   here (see boundary-rule interpretation); stub acquisition is not.
3. Create `partition-topology/router.ts`: `PartitionTopologyRouter` interface,
   `PartitionTopologyRouterImpl`. Move the destroy-traversal _ordering and dedup_ out of
   `FokosDB.destroy()` into a router method
   `traverseForDestroy(getStatus: (ctx) => Promise<{splitStatus, promotedKeys}>, visit: (ctx) => Promise<void>)`
   — the router owns child-discovery order, range-root resolution, and the
   `destroyedRangeRoots` dedup; `FokosDB` supplies the two callbacks that perform RPCs.
4. Create `partition-topology/split-state.ts` and `partition-topology/split-policy.ts`:
   - **`split-state.ts` — one shared `SplitStateMachine`** (KV-backed, constructor takes
     storage + the KV key). Owns `splitStatus()`, `queueSplit(splitType, pCtx)`,
     `commitSplitStarted(children)` (incl. `history`), and `acknowledgeChildMigration(name)`
     with its idempotency and more-acks-than-children invariants. Today this logic is
     duplicated ~verbatim (~100 lines) between the hash and range impls — it is defined once
     here and both policies hold an instance.
   - **`split-policy.ts`** — `SplitStatusKVItem`, `PartitionTopologySplitter`,
     `HashPartitionTopologyImpl`, `RangePartitionTopologyImpl`, `RANGE_PROMOTION_FRACTION`,
     reduced to pure decisions (`shouldAllow`, `shouldSplit`, `pickChildPartition`,
     `prepareSplit`) plus delegation to their `SplitStateMachine`.
   - **`startSplit` no longer performs RPC.** Split it into `prepareSplit()` (computes and
     validates child contexts; for range, takes precomputed `boundaries` as input; returns
     `InitFromSplitOptions[] | null`) and the state machine's `commitSplitStarted(children)`.
     `PartitionDO`'s background split job becomes: `policy.prepareSplit(...)` →
     `initFromSplit` fan-out with `tryWhile(≤5)` → `commitSplitStarted(...)` →
     `triggerMigration` fan-out (`Promise.allSettled`, fire-and-forget). Failure ordering must
     match today: if any init fails, abort before the KV transition so the retry path is
     unchanged.
   - `computeRangeSplitBoundaries` moves to `PartitionStore` (it is a data query); the DO calls
     it and passes boundaries into `prepareSplit`.
   - The promotion-mutual-exclusion check in `shouldSplit` stops querying `promoted_keys` via
     SQL; the DO passes `hasInFlightPromotions: boolean` (answerable from the in-memory
     `#_promotedKeys` map — no storage read needed) into `maybeQueueSplit`/`shouldSplit`.
   - Policies + state machine keep reading/writing their own `__split_status` / `__topo_cache`
     KV keys — that is split-policy-owned state, not partition-table state.
5. Convert `partition-topology/partition-topology.ts` into a pure re-export barrel so existing
   imports keep compiling; migrate importers opportunistically. The barrel is deleted in phase 5.
6. Delete the two hand-written structural stub types (`PartitionDOStub` in both DOs stays for
   now — TC's copy is fine; the one in `do-partition.ts` survives until phase 3 removes the
   parent-stub variant) — at minimum, the `partition-topology.ts` → `do-partition.ts` import is
   gone. Verify: `grep -rn "from \"../do-partition\|from \"./do-partition\" src/lib/partition-topology/` returns nothing.

**Tests:** new `partition-id.test.ts` (hash + range codec round-trips, `doName` formatting,
`depth`/`rootIdx`/`lastChildIdx` readers, append/encode paths) and `split-state.test.ts` for the
`split_queued → split_started → split_completed` transitions incl. idempotent re-acks and the
more-acks-than-children invariant — tested once, covering both impls. Existing
`partition-topology.test.ts` keeps passing unchanged.

**Acceptance:** `grep -n "env\[" src/lib/partition-topology/*.ts` only matches `idFromName`
resolution in `partition-id.ts`/`router.ts`; no `.get(` stub acquisition and no
`sql.exec` on `items`/`promoted_keys` anywhere in `partition-topology/`; suite green.

### Phase 3 — `partition/partition-peer.ts` + `partition/migration.ts`

**Goal:** one gateway interface for all peer-DO calls; the child-side migration driver is a
component with crash/resume covered by direct tests. Parent-side batch serving is deliberately
NOT extracted — after phase 1 it is already thin DO methods (authorization invariant +
`store.queryXPage` + `collectBatch`), and a component there would be indirection without gain.

1. Create `partition/partition-peer.ts`: the `PartitionPeer` interface — the union of the remote
   PartitionDO methods components need (`getItemsBatch`, `getPartitionTransactionMetadata`,
   `getPromotedKeysBatch`, `acknowledgeChildMigrationComplete`, `acknowledgePromotionComplete`,
   `initFromSplit`, `triggerMigration`). A `DurableObjectStub<PartitionDO>` satisfies it
   structurally — no wrapper class. Delete the hand-written `ParentPartitionDOStub` type; this
   single interface also serves phase 4's promotion gateway needs. The TC's own local
   `PartitionDOStub` type (prepare/commit/cancel/readForTransaction) stays as-is deliberately —
   it is the 2PC surface, a different concern; do NOT unify it with `PartitionPeer` or couple
   the TC to `partition/` internals.
2. Create `partition/migration.ts` (`class SplitMigration`, constructor:
   `{ store, storage (for the migration KV keys), parent: PartitionPeer, batchLimitBytes?,
beforeComplete? }`):
   - `runMigration()` orchestrating `runHashChildMigration` / `runRangeChildMigration` — batch
     ingestion (`INSERT OR IGNORE` semantics preserved verbatim, including the comment's
     rationale), `SPLIT_MIGRATION_CURSOR` KV checkpointing, pending-tx + watermark sync,
     promoted-keys inheritance, `rebuildKeySizeEstimates`, the `migration_*` status
     transitions, and the final parent acknowledgement.
   - `__testing__migrationBatchLimitBytes` and `__testing__beforeMigrationComplete` become the
     `batchLimitBytes` / `beforeComplete` constructor options; the DO's `__testing__` fields
     forward to them so existing tests keep working until updated.
3. `PartitionDO` keeps: `initFromSplit` (KV writes + idempotency check), `ensureMigration`
   (request-path gate), the migrating-read fallback in `getItem` (parent `getItemDirect`), and
   the parent-side serving endpoints. These are request-path concerns, not migration-driver
   concerns.

**Tests:** new `migration.test.ts` with a fake `PartitionPeer`: resume-from-cursor after a
simulated crash mid-batch, idempotent re-ingestion of an already-written batch, the
`cursor === null` completion invariant, promoted-keys inheritance filtering, and the
ack-routing rule (hash parent → `acknowledgePromotionComplete`, range parent →
`acknowledgeChildMigrationComplete`).

**Acceptance:** `do-partition.ts` contains no migration loop bodies; one gateway type instead of
the hand-written stub types; migration is testable without spinning up two real DOs; suite
green.

### Phase 4 — `partition/promotion.ts`

**Goal:** promotion lifecycle owns its cache; promotion state has exactly one owner.

1. Create `partition/promotion.ts` (`class PromotionManager`): owns the in-memory
   `Map<string, PromotedKeyStatus>` (moved from `#_promotedKeys`), `loadFromStorage()` (called
   from the DO's `blockConcurrencyWhile`), `statusFor(hk)` (hot-path read used by
   `withSplitForwarding`/`groupItemsByRouting`), `hasInFlightPromotions()` (feeds phase 2's
   `shouldSplit` input), `maybeQueuePromotion(pCtx, hk, estBytes)`, `startPromotion(hk)`
   (init range root → cutover `transactionSync` → trigger migration, via a
   `Pick<PartitionPeer, "initFromSplit" | "triggerMigration">` provided by the DO — phase 3's
   interface, no new gateway type), `acknowledgePromotionComplete(hk)`, `runGC()` (the batched
   delete job), and `snapshot()` for `status()`. Mutual-exclusion guard (skip when a hash split
   is queued/started) takes the split status as a parameter — promotion does not read
   split-policy KV directly.
2. The DO's `runBackgroundWork` keeps its current inline structure (it changes in phase 6);
   the promotion-drive and GC job bodies simply delegate to `PromotionManager`.

**Tests:** `promotion.test.ts` (queue threshold math with `RANGE_PROMOTION_FRACTION`, cutover
deferred under pending locks, idempotent re-drive, GC residual handling) with fake gateway.

**Acceptance:** no promotion lifecycle logic or `#_promotedKeys` access outside
`PromotionManager`; suite green.

### Phase 5 — `partition/transaction-participant.ts` + cleanup

**Goal:** the 2PC participant is an isolated, heavily-tested component; transitional scaffolding
is removed.

1. Create `partition/transaction-participant.ts` (`class TransactionParticipant`, constructor:
   `{ store, now?: () => number }` — injectable clock for skew/staleness tests): move
   `prepareLocal` (clock-skew rejection, pending-conflict, condition evaluation via the shared
   `evaluateConditionsOnItem` — which lives with the store since phase 1 because the
   non-transactional `putItem`/`deleteItem` also use it, timestamp-conflict incl. the
   deletion-watermark
   check for absent items, lock insertion), `commitLocal` + `applyCommitItems` (keyset-equality
   invariants preserved verbatim), local cancel, `readForTransactionLocal`, and
   `listStaleTransactions()`. The DO keeps `prepare`/`commit`/`cancel`/`readForTransaction`
   (routing fan-out via `groupItemsByRouting` + delegation to the participant for the local
   set) and drives stale-tx recovery (TC RPC + re-entrant `this.commit`/`this.cancel`, which
   must go through the DO because they fan out to children).
2. Cleanup: delete the `partition-topology.ts` barrel and fix remaining imports; remove DO
   `__testing__` fields that became component constructor options (update tests accordingly);
   re-audit that `do-partition.ts` is down to RPC surface + context guard + routing + wiring +
   the background-work structure (which phase 6 addresses).

**Tests:** `transaction-participant.test.ts` against real DO storage: idempotent re-prepare,
each rejection reason (`pending_conflict`, `condition_failed`, `timestamp_conflict` incl.
watermark path, `clock_skew` via injected clock), commit keyset-mismatch invariants, check-op
timestamp bump.

**Acceptance:** prepare/commit semantics testable without a TC or routing; barrel gone; suite
green.

### Phase 6 — `partition/background-scheduler.ts` (last; independent of the rest)

**Goal:** the scheduling machinery is generic, isolated, and tested. Deliberately last: no other
phase depends on it, and it carries the plan's largest sanctioned behavior change.

1. Create `partition/background-scheduler.ts` (`class BackgroundScheduler`): generalizes
   `scheduleBackgroundWork` + `ensureAlarmSet`. Jobs register as
   `{ name, run(): Promise<void>, nextAlarmMs(): number | null }`; the scheduler runs them
   sequentially with per-job try/catch + logging (preserving today's "one job failing never
   blocks the rest" invariant), then computes the min next alarm from `nextAlarmMs()` exactly as
   the current `finally` block does.
2. `runBackgroundWork` in the DO becomes registration + delegation: migration job (phase 3
   component), split job (phase 2 prepare/fan-out/commit sequence), stale-tx recovery
   (participant `listStaleTransactions` + DO-driven TC calls), promotion drive + GC (phase 4
   component).
3. The scheduler exposes a `cancelAll()`/`dispose()` that `destroyPartition` calls — replacing
   the current "highest setTimeout ID" loop hack for clearing pending timers (the alarm delete
   stays in `destroyPartition`).
4. **Sanctioned behavior change (apply as a distinct step after the motion is green):** replace
   the `Promise.race` 1-second reset hack with a single-in-flight guard plus a pending-rerun
   flag (if a run is requested while one is active, run once more on completion).

**Tests:** `background-scheduler.test.ts`: coalescing of concurrent schedule calls,
`forceSchedule`, rerun-after-active, and alarm-fallback interaction.

**Acceptance:** no `setTimeout`/`Promise.race` scheduling logic left inline in
`do-partition.ts`; suite green.

## Consequences

**Positive**

- Invariant-dense logic (2PC participant checks, migration resume, promotion cutover, topology
  learning) becomes unit-testable without driving the whole DO through integration tests.
- The import cycle and duplicated stub types disappear; the dependency graph becomes
  `DOs → components → store → platform` with topology purely decision-side.
- Storage bookkeeping (size estimates, watermarks) has a single owner; lockstep-duplication bugs
  become structurally impossible.
- `do-partition.ts` shrinks to RPC surface + routing + wiring, making the eventual range-routing
  cache work (LOUDS / bloom filter, per range-partition-splits-v2) easier to land.

**Negative / accepted costs**

- More files and indirection: readers must follow constructor wiring to see the whole picture.
  Mitigated by the boundary rule being simple and uniform.
- Refactor churn risk on a subtle codebase. Mitigated by phase ordering (mechanical moves first,
  participant last), per-phase green test suite, and the existing 2,090-line DO integration test
  acting as the behavioral safety net throughout.
- Some hot-path code (e.g. promoted-key lookup) gains one method-call hop. Negligible relative to
  storage/RPC costs.

**Explicitly out of scope**

- Any change to `bloom-filter.ts` (reserved for the upcoming caller-side routing feature).
- DO topology changes, new DO classes, or changes to the 2PC protocol/wire types.
- The TC beyond adopting `transaction-limits.ts`.
- Behavioral changes hidden inside refactor phases. The only sanctioned ones are the
  put-requires-data validation alignment on the TC path (phase 0) and the scheduler rework
  (phase 6), each applied as a distinct step after the phase's code motion is verified green.
