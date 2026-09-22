# RFC — FokosShardingRuntime demo suite: three tiles for a two-minute walkthrough

**State:** Draft
**Date:** 2026-09-22
**Author:** Lambros Petrou

**Status:** Not started. This plan defines the demo suite only. It does not change
`packages/fokosdb/src/sharding/` or `packages/fokosdb/src/server/do-partition.ts`.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
  - [4.3 Open questions](#43-open-questions)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

`FokosShardingRuntime` (`packages/fokosdb/src/sharding/`) is a reusable sharding and routing layer for Durable
Objects. `PartitionDO` is its only host in production today. Two milestones from
`docs/agent-plans/2026-09-19-fokos-sharding-runtime.md` are not built yet: M5, an independent example host with
no FokosDB code, and M6, `TransactionCoordinatorDO` as a second host.

This document defines a demo suite. The suite shows the runtime works for use cases other than FokosDB, inside a
two-minute live walkthrough.

The walkthrough must show five points without extra narration:

- The runtime works with any Durable Object, not only FokosDB.
- A host chooses its own split trigger: request volume, storage size, or another rule.
- A host chooses hash-only partitioning or hash-plus-range partitioning.
- The runtime can promote one hot key out of a shared partition into its own partition.
- The runtime recovers correctly when a partition dies during a migration.

Three demo tiles carry these points. All three tiles share one topology view.

## 2. Goals and requirements

### 2.1 In scope

- The demo suite has a shared topology view. It renders one partition as one box. A split moves the parent box
  up into a router box and shows its children below it. A click on a router box animates a forward to the
  correct child box.
- Demo 1 is a hash-only example host that splits on request volume. A debug-only control kills the owning
  partition mid-split, to show that the migration resumes correctly.
- Demo 2 is a multi-tenant, full-text search example host. Tenants start co-located on shared hash partitions.
  The host promotes a tenant into its own partition once the tenant grows past a size threshold.
- Demo 3 shows the real `PartitionDO` topology, plus `TransactionCoordinatorDO` once it is a runtime host.
  `TransactionCoordinatorDO` becoming a host is M6 of `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`.
  This plan does not redo that work.
- The suite has a rehearsed script for the two-minute run, with a fixed tile order, fixed button presses, and
  one line of narration per beat.
- The suite lives in a new example package, `examples/sharding-demo/`, next to `examples/http-api/`.

### 2.2 Out of scope

- Game mechanics, scoring, and multi-user play. Every tile is presenter-driven. A button fires synthetic load;
  the demo does not wait on input from the audience.
- A spatial or canvas-based demo. Section 5 gives the reason.
- Building M6, `TransactionCoordinatorDO` as a host. Demo 3 needs M6, but this plan does not build it.
- A production deployment of any example host. The suite runs in a throwaway Workers environment, or locally
  with `wrangler dev`.

### 2.3 Requirements

- The full walkthrough, Demo 1 through Demo 3 with narration, must run in 2 minutes.
- Each split and each kill/resume must complete in TODO: measure seconds, so it fits inside a live walkthrough.
  The thresholds that produce this are demo-tuned constants. They are not a recommendation for production
  defaults.
- No tile changes `packages/fokosdb/src/sharding/` or an existing FokosDB host, except the M6 work Demo 3 needs.
  M6 is tracked in `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`, not in this document.

## 3. Milestones

### D1 — Shared topology view and control panel shell

- A web UI with one panel per tile. Each panel polls its example host's `fokosStatus` operation and renders the
  current partition tree as boxes.
- The box-tree animation: a split shrinks the parent into a router box, shows its children below it, and
  flashes a line from parent to child on a routed request.
- A control panel per tile. Each button fires synthetic load at the example host through the host's own RPC or
  HTTP entry point. No button reads or writes a real user session.
- The UI stack matches `examples/http-api/public/demo/index.html`: Preact, `@preact/signals`, and `htm/preact`,
  loaded through an import map from `esm.sh`, with PicoCSS from `cdn.jsdelivr.net`. No bundler and no npm UI
  dependency. Section 4.2 gives the theming detail.

### D2 — Demo 1: hash-only counter host, split on volume, kill and resume

- A new example Durable Object host, hash-partitioned only, with no FokosDB import. It stores a small counter or
  a few items per hash key.
- The host's `evaluateSplit` hook returns a split decision once the local request count crosses a fixed
  threshold. No runtime host splits by request volume today: `PartitionDO` splits by storage size alone (the
  `evaluateSplit` hook in `packages/fokosdb/src/server/do-partition.ts`).
- A debug-only RPC, excluded from any non-demo build, calls `this.ctx.abort()` on the host. This forces
  Cloudflare to evict the Durable Object. `ctx.abort()` already appears in the runtime, for its own destroy path
  (`packages/fokosdb/src/sharding/runtime.ts`). The control panel's "kill parent" button calls the debug RPC
  during a split that is in progress.
- The demo must show that after the kill, the next `fokosMigrationPull` call from a child reinstantiates the
  parent from its durable storage, and the migration resumes from its saved cursor with no write lost or
  duplicated.
- The control panel shows a live count of writes acknowledged against writes present after the kill, so the
  audience sees the reconciliation, not a claim that it worked.

### D3 — Demo 2: multi-tenant FTS5 host, promotion on tenant growth

- A new example Durable Object host, with no FokosDB import. It stores documents in an FTS5 virtual table per
  partition (`CREATE VIRTUAL TABLE ... USING fts5(...)`), with `tenant_id` as an `UNINDEXED` column. `tenant_id`
  is the route key's `hashKey`.
- Search is a `range`-shape operation: `FokosRangeInput { hashKey: tenantId, interval: full }` (the
  `FokosRangeInput` type in `packages/fokosdb/src/sharding/runtime-types.ts`). The query is `SELECT ... FROM
  docs_fts WHERE tenant_id = ? AND docs_fts MATCH ?`. The query does not change when a tenant is promoted. Only
  its routing changes.
- The host keeps a per-tenant byte or row counter in its own storage. The runtime does not track this count.
  `PartitionDO`'s own `keyEstBytes` bookkeeping, in `packages/fokosdb/src/server/do-partition.ts`, is the model
  to copy. Once one tenant's counter crosses a threshold, the host calls `requestPromotion(tenantHashKey, data)`
  (the `requestPromotion` method in `packages/fokosdb/src/sharding/runtime.ts`) to carve that tenant into its
  own range-partition subtree.
- When a promoted tenant keeps growing, its own subtree splits further by range, for example by document date.
  Its search stays a `range` operation scoped to its own `hashKey`, so it only visits that tenant's own
  partitions.
- Cloudflare's Durable Object SQLite storage backend supports `FTS5` virtual tables and `bm25()` ranking.

### D4 — Demo 3: the real FokosDB topology

- A topology panel reads `fokosStatus` from a running `PartitionDO` shard group, using the same box-tree view as
  Demo 1 and Demo 2.
- A second topology panel shows `TransactionCoordinatorDO`, once it is a runtime host. This depends on M6 of
  `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`, not built as of this plan's date. Demo 3 is blocked on
  that milestone; this plan does not schedule it.
- A seed script gives the shard group more than one partition before the demo starts, so the panel is not one
  empty box when the walkthrough reaches it.

### D5 — Script rehearsal

- A fixed two-minute script: tile order, the exact buttons pressed, one line of narration per beat.
- A timed dry run. When Demo 1's kill/resume beat and Demo 2's promotion beat do not both fit inside the budget,
  the script drops Demo 1's kill/resume beat first: it is additive to Demo 1, not a fourth tile.

## 4. Proposed solution

### 4.1 High-level overview

The demo suite has four parts: one shared topology view, and three demo tiles.

The topology view polls an example host's `fokosStatus` operation and draws the current partition tree as
boxes. A split shrinks the parent partition into a router box and shows its children below it. A click on a
router box animates a forward to the correct child.

Demo 1 is a hash-only example host with no FokosDB code. It splits when its local request count crosses a
threshold. A debug-only control can kill its owning partition mid-split. The topology view then shows the
migration resume from where it stopped, with no write lost.

Demo 2 is a multi-tenant, full-text search example host, also with no FokosDB code. Tenants share one partition
while they are small. Once a tenant's data crosses a size threshold, the host promotes that tenant into its own
dedicated partition. The tenant's search query does not change; only which partition answers it changes.

Demo 3 shows the real `PartitionDO` topology, plus the transaction coordinator pool once it becomes a runtime
host under M6 of `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`.

Each tile is presenter-driven. A button fires synthetic load. Nothing in the demo waits on input from the
audience.

### 4.2 Technical details

**Route key convention.** Each example host picks its own business key as the route key's `hashKey`: a counter
bucket id for Demo 1, a tenant id for Demo 2. The runtime routes on `{ hashKey, sortKey }` only and does not
require a `sortKey` when a host never enters a range tree, as in Demo 1.

**Pluggable split trigger.** `evaluateSplit` is a host hook (`packages/fokosdb/src/sharding/runtime-types.ts`),
not a runtime policy. Demo 1's host counts local requests. Demo 2's host counts bytes or rows per tenant.
`PartitionDO` counts storage bytes for the whole partition. All three read the same hook contract and return the
same shape of decision.

**Promotion for one hot key.** `requestPromotion(hashKey, data)` (`packages/fokosdb/src/sharding/runtime.ts`)
moves one hash key out of a shared partition into its own range-partition subtree, independent of how small the
rest of the partition stays. Demo 2 is the only tile that calls it. `PartitionDO` calls it too, from its own
per-key size bookkeeping (`packages/fokosdb/src/server/do-partition.ts`).

**Scoped range queries.** `FokosRangeInput` (`packages/fokosdb/src/sharding/runtime-types.ts`) always carries one
`hashKey`. A `range`-shape operation walks only the partitions under that key's subtree. Demo 2's per-tenant
search uses this directly: it never needs a fan-out across every partition in the shard group, because the
runtime has no broadcast-to-every-leaf primitive and the query never needs one.

**UI stack and theme.** The suite uses the same stack as `examples/http-api/public/demo/index.html`: Preact,
`@preact/signals` for state, and `htm/preact` for templates, all loaded from `esm.sh` through an import map, with
PicoCSS loaded from `cdn.jsdelivr.net`. There is no bundler and no build step. The suite loads
`pico.orange.min.css` and sets `data-theme="dark"` on the `<html>` element, which forces Pico's dark palette
instead of following the OS setting. The box-tree animation is hand-written DOM or SVG in each tile; no widget
library supplies it.

**Failure and recovery path.** Demo 1's kill/resume beat depends on two existing properties of the runtime, not
on new code: migration state is durable, not held in memory, and the migration protocol is pull-based and
resumable (`fokosMigrationPull`, `fokosMigrationAck`, with cursor pagination through `buildPage`, `applyPage`,
and `validatePage`). When the debug RPC calls `this.ctx.abort()` on the parent, Cloudflare evicts it. The next
`fokosMigrationPull` call from a child reinstantiates the parent from storage, and it resumes from its
persisted cursor.

### 4.3 Open questions

**Debug-only abort RPC.** Confirm that exposing a `ctx.abort()`-calling debug RPC (D2) is acceptable to ship
behind a build flag, or whether it must live only in a demo-specific package that never imports into
`packages/fokosdb/src/server/`.

**Demo-only thresholds.** Pick the volume and storage thresholds for D2 and D3, so each split completes inside
the budget in section 2.3. Record the chosen constants once picked.

**Tile order.** This plan assumes the order Demo 1, Demo 2, Demo 3. Confirm the order with the timed dry run in
D5.

## 5. Alternative options

**Spatial multiplayer canvas or territory-claim game.** Visually strong, but too slow to build and to narrate
inside a two-minute budget. It buries the specific points, the pluggable split trigger, hash-only against
hash-plus-range, and promotion, behind game rules the audience has to learn first.

**Global free-text search across every tenant or every partition.** Rejected in favor of the per-tenant scoped
search in Demo 2. A whole-corpus search needs host-built fan-out logic, because `range` and `rangeVisits` are
always scoped to one `hashKey`; the runtime has no built-in broadcast-to-every-leaf primitive. The per-tenant
version uses the `range` shape directly, with no extra fan-out code, and is a more direct demonstration of what
the runtime gives a host for free.

**One combined game app instead of separate tiles.** Rejected because it couples the demo's pacing to player
interaction instead of the presenter's own timing, which risks the two-minute budget.

**Tailwind CSS with Cloudflare's Kumo component library.** Rejected in favor of the PicoCSS-plus-Preact stack in
section 4.2. Kumo is React-only and needs either a Tailwind build with a `@source` directive or a precompiled
CSS bundle; Preact interop would need a `preact/compat` alias. Adopting it adds a bundler and npm React
dependencies where the sibling example has none. The suite's hardest UI piece, the box-tree animation, is
hand-written in both stacks, so Kumo's component set does not remove that work. Pico's `pico.orange.min.css`
theme and its `data-theme="dark"` attribute already give the orange, dark-mode look with no new tooling.

## 6. Frequently asked questions

TODO: the author must supply the questions a reviewer is expected to ask about this plan, with their answers.

## 7. References

- `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`
- `docs/agent-plans/2026-09-17-unified-repartition-flow.md`
