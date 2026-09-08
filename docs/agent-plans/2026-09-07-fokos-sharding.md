# RFC — FokosSharding: a reusable Durable Object sharding and splitting utility

**State:** Draft
**Date:** 2026-09-07
**Author:** Lambros

---

## Table of Contents

1. [Overview and Context](#1-overview-and-context)
2. [Goals and Requirements](#2-goals-and-requirements)
3. [Milestones](#3-milestones)
4. [Proposed Solution](#4-proposed-solution)
5. [Alternative Options](#5-alternative-options)
6. [Frequently Asked Questions](#6-frequently-asked-questions)
7. [References](#7-references)

---

## 1. Overview and Context

### 1.1 The problem

`PartitionDO` in `packages/fokosdb/src/server/do-partition.ts` mixes the generic Durable Object
sharding lifecycle with the FokosDB item model. It owns routing, hash and range splitting, child
migration, hash-key promotion, background scheduling, two-phase commit transactions, item TTL, and
the DynamoDB-like item API in one class.

`PartitionStore` in `packages/fokosdb/src/shared/partition/partition-store.ts` stores both generic
sharding metadata (`promoted_keys`, `key_size_estimates`, `range_hierarchy`, `deletion_metadata`)
and the FokosDB application schema (`items`, `pending_transactions`).

`PromotionManager`, `SplitMigration`, and `TransactionParticipant` are already partially
extracted, but they still depend on the FokosDB `PartitionStore` and item model. A custom Durable
Object cannot reuse the sharding machinery without also adopting the FokosDB item schema.

### 1.2 Current system

The current FokosDB architecture uses these pieces, which are already close to generic:

- `packages/fokosdb/src/shared/partition-topology/partition-context.ts` carries the topology
  configuration with every RPC.
- `packages/fokosdb/src/shared/partition-topology/partition-id.ts` encodes hash and range
  partition identities.
- `packages/fokosdb/src/shared/partition-topology/split-policy.ts` defines
  `PartitionTopologySplitter`, `HashPartitionTopologyImpl`, and `RangePartitionTopologyImpl`.
- `packages/fokosdb/src/shared/partition-topology/split-state.ts` tracks the split lifecycle.
- `packages/fokosdb/src/shared/partition-topology/router.ts` gives the client a way to pick a
  root partition.
- `packages/fokosdb/src/shared/partition-topology/hash-topology.ts` caches the hash split tree.
- `packages/fokosdb/src/shared/partition/partial-range-topology.ts` caches promoted keys in a bloom
  filter.
- `packages/fokosdb/src/shared/partition/batch-scan.ts` pages batches for migration.

The FokosDB-specific pieces are the item tables, pending transaction locks, condition and update
expressions, TTL, and two-phase commit logic in `PartitionDO` and `PartitionStore`.

### 1.3 Proposed capability

Create a new workspace package `fokos-sharding`. It exposes a `FokosSharding` composition runtime
that a custom Durable Object can create and use. The DO implements a small set of hooks for its own
SQLite schema. The runtime handles automatic hash sharding, range splits, migration, optional
hash-key promotion, and background scheduling.

FokosDB then consumes the same package by making `PartitionDO` use the runtime and implement the
hooks on top of its existing `PartitionStore`, `TransactionParticipant`, `PromotionManager`, and
`TtlExpiry`.

---

## 2. Goals and Requirements

### 2.1 In scope

1. Create a new workspace package `packages/fokos-sharding` with its own build, tests, and client
   and server entry points.
2. Extract the generic sharding code from `packages/fokosdb` into `fokos-sharding`.
3. Provide a `FokosSharding` composition runtime that a custom DO can own.
4. Provide an optional `FokosShardedDurableObject` base class for DOs that prefer inheritance.
5. Define a hook contract that lets a DO keep full control of its own SQLite schema.
6. Support hash splits, range splits, and hash-key promotion as optional features.
7. Provide a default `RangeTopologyStore` backed by a reserved SQL table.
8. Provide a generic `ShardedRouter` client that routes to partitions without knowing the DO's
   methods.
9. Keep the sharding runtime and topology layer behind the `fokosdb/client` API.
10. Define the final `FokosDBOptions` after the sharding utility API is stable.
11. Preserve the existing correctness invariants for splits, migration, promotion, and routing.
12. Leave the client router and the runtime open to a future global topology cache. The cache can
    route directly to leaf partitions without a DO code change.

### 2.2 Out of scope

- The `runCustom` command dispatcher and `putItemSinglePartition` primitive from
  `docs/ideas/2026-08-30-extensible-partition-execution.md`. Those are built on top of this
  foundation in a later RFC.
- Arbitrary custom SQL schema migration tools. Each DO owns its own schema.
- Making the transaction coordinator generic. `TransactionCoordinatorDO` stays in FokosDB.
- Dynamic code upload or runtime schema changes.

### 2.3 Requirements

1. `fokos-sharding` must not pull any Durable Object class into the client bundle. The build guard
   in `packages/fokosdb/tsdown.config.ts` must be replicated for the new package.
2. The package must define a minimal `ShardingEnv` type. The caller passes its full `Env`, and the
   runtime uses the namespace binding it needs.
3. The runtime must use reserved `__fokos_` KV keys and SQL table names so it does not conflict
   with DO-owned schema.
4. The runtime's internal RPC methods must use the `fokos` prefix.
5. Migration record payloads must be `StructuredCloneable`, because Workers RPC transports them.
6. Hash splits and promotions must remain mutually exclusive while any promotion is `queued` or
   `promoting`.
7. The `FokosSharding` runtime must report topology events to an optional `TopologyReporter`.
   Reporter failures must not change the result of a data operation or a lifecycle transition.
8. The `ShardedRouter` client must accept an optional `TopologyResolver` that can override root
   routing when a future global topology cache is available.
9. A resolver result must be the correct owner or an ancestor of the correct owner.
10. Resolver failures and timeouts must fall back to root routing.
11. The runtime must apply each lifecycle hook synchronously in the same storage transaction as its
    lifecycle transition or durable completion marker.
12. The mandatory topology decision must run before an application acceptance hook. An application
    hook must not replace a `forward` or `reject_out_of_range` decision.
13. Migration apply hooks must be idempotent for a repeated batch. Migration must finish a stream
    only when its `nextCursor` is `null`.
14. Every migration stream must use a byte budget below the Workers RPC serialized-size limit.
15. The runtime must retry the parent acknowledgement before it marks a child migration complete.
16. The FokosDB adapter must migrate existing lifecycle state and preserve rollback compatibility.
17. The runtime must support ordered range-query fan-out with byte, item, and partition-visit
    budgets.

---

## 3. Milestones

### Milestone 0 — Package skeleton

Create `packages/fokos-sharding` with `package.json`, `tsconfig.json`, `tsdown.config.ts`, a server
entry, a client entry, a Vitest/Miniflare harness, and empty API type definitions. No runtime
behavior. `PartitionDO` is unchanged.

### Milestone 1 — Move generic shared modules

Move these modules from `packages/fokosdb` to `packages/fokos-sharding/src/shared/`:

- key codec, hash primitives, and partition ID encoding,
- split state, split policy, hash topology, partial range topology,
- batch scan, page budget, sort-key interval,
- partition errors, typed DO stubs,
- the client router.

Keep compatibility re-exports in `fokosdb` through the Milestone 8 API review. After that review,
remove only exports that the selected FokosDB client API does not need.

### Milestone 2 — Decouple split policy from `PartitionStore`

Introduce `RangeTopologyStore`, `RangeSplitBoundaryProvider`, and `SubspaceStore` interfaces.
Refactor `HashPartitionTopologyImpl` and `RangePartitionTopologyImpl` to call the interfaces.
`PartitionStore` implements the interfaces for FokosDB.

### Milestone 3 — Extract the composition runtime

Create the `FokosSharding` class and move these behaviors from `PartitionDO`:

- `initFromSplit`, `ensurePartitionContext`, and `ensureMigration`,
- `withSplitForwarding`, `groupItemsByRouting`, and ordered range-query routing,
- `runSplit`, `runMigration`, `scheduleBackgroundWork`, and `runBackgroundWork`,
- `alarm` and `destroy`.

Define the public DO wrapper contract for every internal `fokos*` RPC and `alarm()`. A DO that uses
composition must delegate those methods to its runtime. The optional base class supplies the same
wrappers.

The FokosDB adapter dual-reads and dual-writes the old and new lifecycle keys. `PartitionDO` keeps
working by delegating to the runtime.

### Milestone 4 — Generic migration

Replace the hard-coded `PartitionPeer` data exchange with the migration scan and apply hooks. The
runtime handles separate record and metadata cursors, byte budgets, and status transitions. Apply
hooks are idempotent for a repeated batch.

Add `migration_ack_pending` between `migration_migrating` and `migration_completed`. The runtime
retries the idempotent parent acknowledgement while the child is in this state. FokosDB implements
the hooks with `PartitionStore`.

### Milestone 5 — Optional promotion

Turn `PromotionManager` into a generic `SubspacePromotion` engine that depends on `SubspaceStore`.
Enable it only when the DO overrides the promotion hooks. FokosDB implements the store.

### Milestone 6 — Client router

Extract `ShardedRouter` from `PartitionTopologyRouterImpl` and expose it. Add soft fallback for a
resolver error, timeout, or cache miss. The `FokosDB` client in
`packages/fokosdb/src/client/db.ts` uses the router through its internal adapter.

### Milestone 7 — Example custom DO

Add a small example DO that uses `FokosSharding` with a custom SQLite schema. It proves the hooks,
public RPC wrappers, alarm delegation, migration replay, and range-query routing are sufficient for
non-FokosDB applications.

### Milestone 8 — FokosDB integration design gate

Review the stable sharding utility API before the final FokosDB client integration. Define the
minimum `FokosDBOptions` fields that create the internal sharding context and router. A FokosDB user
must not construct or import `FokosSharding` or `ShardedRouter`.

Finalize the public `FokosDB` construction path only after this API review. The `PartitionDO` adapter
from earlier milestones remains internal. Re-export only the types and utilities that a FokosDB user
needs to operate FokosDB.

### Milestone 9 — Ownership validation

Validate that each request reaches the correct owner or one of its ancestors. Validate the hash path
for a hash partition. Validate the hash key and sort-key interval for a range partition. Reject a
sibling or unrelated partition context with a non-retryable routing error.

Add test helpers that generate a hash key for a specified partition and a sort key for a specified
range. Tests that inspect physical storage use strict local reads instead of an invalid public route.
Do not add a production validation bypass.

### Milestone 10 — Compatibility cleanup and publication

Keep the old lifecycle keys and internal RPC aliases while rollback support is active. Remove them
only in a separately approved compatibility release. Publish `fokos-sharding` after the FokosDB
integration and compatibility review.

---

## 4. Proposed Solution

### 4.1 High-level overview

The system has three layers:

1. `fokos-sharding` owns routing, topology, split lifecycle, migration, optional promotion, and
   background scheduling.
2. A custom Durable Object creates a `FokosSharding` instance, defines its own SQLite schema, and
   implements the hooks.
3. `fokosdb` consumes `fokos-sharding` through `PartitionDO`.

A request flows as follows:

1. The client uses `ShardedRouter` to pick a partition and get a `DurableObjectId`. A
   `TopologyResolver` can skip router partitions and select an owner or ancestor.
2. The client sends an RPC to the partition with a `ShardingContextResolved`.
3. The DO calls `this.sharding.route()` or `this.sharding.routeRange()`.
4. The runtime validates context, ownership, migration, promotion, and split state.
5. The topology makes the mandatory local or forward decision.
6. For local work, the optional application acceptance hook can reject the request.
7. The runtime calls the DO's local callback or forwards to one or more child stubs.
8. The DO's local callback reads or writes its own schema.

### 4.2 Package layout

`packages/fokos-sharding`:

- `src/server/sharding.ts` — `FokosSharding` runtime and `FokosShardedDurableObject`.
- `src/server/topology/` — `ShardingTopology`, `HashShardingTopology`, `RangeShardingTopology`,
  `SplitStateMachine`.
- `src/server/migration.ts` — generic migration engine.
- `src/server/promotion.ts` — optional `SubspacePromotion` engine.
- `src/server/background.ts` — alarm and background scheduler.
- `src/shared/` — key codec, partition ID, hash primitives, batch scan, page budget, sort-key
  interval, shared types, partition errors, DO stubs, and the router.
- `src/client/router.ts` — `ShardedRouter`.
- `src/server/index.ts` and `src/client/index.ts` as entry points.

`packages/fokosdb`:

- `PartitionDO`, `PartitionStore`, `TransactionParticipant`, the FokosDB promotion adapter, and
  `TtlExpiry` stay.
- `PartitionDO` creates a `FokosSharding<PartitionDO>` instance and delegates generic work.
- `PartitionStore` implements the `RangeTopologyStore`, `RangeSplitBoundaryProvider`,
  `SubspaceStore`, and migration hooks.

### 4.3 Core types

`ShardingContext` is the renamed `PartitionContext`. It contains:

- `schema`,
- `shardGroup` (replaces `tableName`),
- `ns` (the DO namespace binding key),
- `rootTreesN`, `hashSplitN`, and `rangeSplitN`,
- `hashSplitConditions` and `rangeSplitConditions`,
- `rangeAncestorsConfig`,
- optional `nsTx` for FokosDB.

`ShardingContextResolved` adds the identity of one concrete partition:

- `doName`,
- `primaryDoIdStr`,
- `partitionId`,
- `depth`,
- optional `rangePartition: { hashKey, startBoundary, endBoundary }` for range DOs.

Both range boundaries are immutable identity. The range partition ID and DO name encode both
boundaries. The runtime validates the denormalized boundaries against `partitionId` before local
work.

`RoutingKey` is:

```ts
type RoutingKey = { hashKey: KeyBytes; sortKey: KeyBytes };
```

`OperationIntent` is `"read" | "write" | "delete" | "ignore_size_reject"`. `RoutingDecision` is
`"ok" | "forward" | "reject_over_size" | "reject_out_of_range"`.

`RangeSlice` is:

```ts
type RangeSlice = {
  start: KeyBytes | null;
  end: KeyBytes | null;
  depth: number;
};
```

`PromotedStatus` is `"queued" | "promoting" | "promoted"`. `PromotedSubspace` is:

```ts
type PromotedSubspace = {
  hashKey: KeyBytes;
  status: PromotedStatus;
};
```

`RangeAncestorInfo` is:

```ts
type RangeAncestorInfo = {
  partitionId: string;
  start: KeyBytes | null;
  end: KeyBytes | null;
  depth: number;
};
```

`TopologyEvent` describes a change the runtime can report to a future global topology cache:

```ts
type TopologyEvent =
  | { type: "split_completed"; parentCtx: ShardingContextResolved; childCtxs: ShardingContextResolved[] }
  | {
      type: "forward_result";
      fromCtx: ShardingContextResolved;
      toCtx: ShardingContextResolved;
      rangeAncestors: RangeAncestorInfo[];
    }
  | { type: "promotion_completed"; hashCtx: ShardingContextResolved; rangeRootCtx: ShardingContextResolved };

interface TopologyReporter {
  report(event: TopologyEvent): Promise<void> | void;
}

interface TopologyResolver {
  resolve(
    ctx: ShardingContext,
    hashKey: KeyBytes,
    sortKey?: KeyBytes,
  ): Promise<{ doId: DurableObjectId; partitionContext: ShardingContextResolved } | undefined>;
}
```

`TopologyReporter.report` is optional and defaults to a no-op. The runtime treats the reporter as a
soft dependency. It catches synchronous errors and promise rejections. A report failure must not
change a request result or a persisted lifecycle transition. A future production reporter can batch
and deduplicate events before it sends them to the central store.

`TopologyResolver.resolve` must return the correct owner, one of its ancestors, or `undefined`. The
router treats `undefined`, an exception, and a configured timeout as a cache miss. It then routes to
the root partition. The resolver must never become a required dependency for data access.

`ShardedMeta` is the metadata every routed response must carry:

```ts
interface ShardedMeta {
  forwardCount: number;
  servedByPartitionId: string;
  hashDepth: number;
  rangeDepth: number;
  _internal: {
    rangeAncestors: RangeAncestorInfo[];
  };
}
```

The runtime owns `_internal`. The DO can add more metrics. The runtime adds or updates
`forwardCount`, `servedByPartitionId`, `hashDepth`, `rangeDepth`, and `_internal.rangeAncestors`.

`MigrationRecord` is:

```ts
type MigrationRecord = {
  routingKey: { hashKey: KeyBytes; sortKey: KeyBytes };
  payload: unknown;
};
```

The DO must put only `StructuredCloneable` values in `payload`, because Workers RPC transports them.

`ShardedRangeRequest` describes one ordered range page:

```ts
type ShardedRangeRequest = {
  hashKey: KeyBytes;
  interval: {
    lower?: { value: KeyBytes; inclusive: boolean };
    upper?: { value: KeyBytes; inclusive: boolean };
  };
  direction: "asc" | "desc";
  cursor: { hashKey: KeyBytes; sortKey: KeyBytes; inclusive?: boolean } | null;
  budgetBytes: number;
  remainingLimit: number | null;
  maxPartitionVisits: number;
};

type ShardedRangePage<TRecord> = {
  records: TRecord[];
  nextCursor: ShardedRangeRequest["cursor"];
  bytesConsumed: number;
  meta: ShardedMeta;
  partitionMetas: ShardedMeta[];
};
```

### 4.4 The `FokosSharding` composition runtime

The runtime is a class that the DO owns:

```ts
class FokosSharding<T extends DurableObject> {
  constructor(deps: {
    ctx: DurableObjectState;
    env: ShardingEnv;
    hooks: ShardingHooks;
    rangeTopologyStore?: RangeTopologyStore;
    subspaceStore?: SubspaceStore;
    topologyReporter?: TopologyReporter;
  });
  init(): Promise<void>;
  context(): ShardingContextResolved | null;
  route<TRes extends { meta: ShardedMeta }>(
    ctx: ShardingContextResolved,
    key: RoutingKey,
    intent: OperationIntent,
    handlers: {
      local: () => Promise<TRes>;
      readFromParent?: (parentStub: DurableObjectStub<T>, parentCtx: ShardingContextResolved) => Promise<TRes>;
      forward: (stub: DurableObjectStub<T>, childCtx: ShardingContextResolved) => Promise<TRes>;
    },
  ): Promise<TRes>;
  routeRange<TRecord>(
    ctx: ShardingContextResolved,
    request: ShardedRangeRequest,
    handlers: {
      local: (request: ShardedRangeRequest) => Promise<ShardedRangePage<TRecord>>;
      readFromParent?: (
        parentStub: DurableObjectStub<T>,
        parentCtx: ShardingContextResolved,
        request: ShardedRangeRequest,
      ) => Promise<ShardedRangePage<TRecord>>;
      forward: (
        stub: DurableObjectStub<T>,
        childCtx: ShardingContextResolved,
        request: ShardedRangeRequest,
      ) => Promise<ShardedRangePage<TRecord>>;
    },
  ): Promise<ShardedRangePage<TRecord>>;
  groupByRouting<TItem>(
    items: TItem[],
    getRoutingKey: (item: TItem) => RoutingKey,
    intent: OperationIntent,
    operationName: string,
  ): { local: TItem[]; forwarded: Map<string, { ctx: ShardingContextResolved; items: TItem[] }> };
  forwardToGroup<TRes, TItem>(
    group: { ctx: ShardingContextResolved; items: TItem[] },
    forward: (stub: DurableObjectStub<T>, childCtx: ShardingContextResolved, items: TItem[]) => Promise<TRes>,
  ): Promise<TRes>;
  scheduleBackgroundWork(opts: { delayMs: number; forceSchedule?: boolean }): void;
  alarm(alarmInfo?: AlarmInvocationInfo): Promise<void>;
  fokosInitFromSplit(opts: InitFromSplitOptions): Promise<void>;
  fokosTriggerMigration(): Promise<void>;
  fokosGetMigrationBatch(request: MigrationScanRequest): Promise<MigrationBatch>;
  fokosGetMigrationMetadata(request: MigrationScanRequest): Promise<MigrationMetadataBatch>;
  fokosAcknowledgeMigrationComplete(request: MigrationAcknowledgement): Promise<void>;
  fokosStatus(): Promise<ShardingStatus>;
  fokosDestroy(): Promise<void>;
  fokosDebugForcePromoteSubspace(hashKey: KeyBytes): Promise<void>;
}
```

The DO uses it like this. The `init()` call only sets up the runtime schema and loads caches. It
does not transfer migration data; that work runs in the alarm or on demand.

```ts
class MyDO extends DurableObject<Env> {
  private sharding: FokosSharding<MyDO>;
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sharding = new FokosSharding({ ctx, env, hooks: this });
    void ctx.blockConcurrencyWhile(async () => { await this.sharding.init(); });
  }
  async myOp(ctx: ShardingContextResolved, req: MyRequest): Promise<MyResult & { meta: ShardedMeta }> {
    const key = this.#extractKey(req);
    return this.sharding.route(ctx, key, "write", {
      local: () => this.#store.myOpLocal(req),
      forward: async (stub, childCtx) => stub.myOp(childCtx, req),
    });
  }
  alarm(info: AlarmInvocationInfo): Promise<void> {
    return this.sharding.alarm(info);
  }
  fokosInitFromSplit(opts: InitFromSplitOptions): Promise<void> {
    return this.sharding.fokosInitFromSplit(opts);
  }
  fokosTriggerMigration(): Promise<void> {
    return this.sharding.fokosTriggerMigration();
  }
  fokosGetMigrationBatch(request: MigrationScanRequest): Promise<MigrationBatch> {
    return this.sharding.fokosGetMigrationBatch(request);
  }
  fokosGetMigrationMetadata(request: MigrationScanRequest): Promise<MigrationMetadataBatch> {
    return this.sharding.fokosGetMigrationMetadata(request);
  }
  fokosAcknowledgeMigrationComplete(request: MigrationAcknowledgement): Promise<void> {
    return this.sharding.fokosAcknowledgeMigrationComplete(request);
  }
  fokosStatus(): Promise<ShardingStatus> {
    return this.sharding.fokosStatus();
  }
  fokosDestroy(): Promise<void> {
    return this.sharding.fokosDestroy();
  }
  fokosDebugForcePromoteSubspace(hashKey: KeyBytes): Promise<void> {
    return this.sharding.fokosDebugForcePromoteSubspace(hashKey);
  }
}
```

`DurableObjectStub<T>` is the Workers RPC stub type. The generic `T` lets the `forward` callback use
the DO's own RPC methods. `fokos-sharding` must not import a FokosDB server type.

`ShardingEnv` is:

```ts
type ShardingEnv = object;
```

A generated Workers `Env` interface is assignable to `object` without an index signature. The runtime
casts its internal indexed view to `Record<string, unknown>`. It then casts `env[ctx.ns]` to
`DurableObjectNamespace<T>` when it resolves a stub.

### 4.5 Optional `FokosShardedDurableObject` base class

The package also exports:

```ts
abstract class FokosShardedDurableObject<
  Env,
  This extends FokosShardedDurableObject<Env, This> = FokosShardedDurableObject<Env, any>,
> extends DurableObject<Env> {
  protected sharding: FokosSharding<This>;
}
```

This class wires the alarm, migration, split, and internal RPC methods to the runtime. A custom DO
can use it when it does not already extend another Durable Object class.

### 4.6 Hook contract

`ShardingHooks` gathers all callbacks that the DO can implement.

#### Routing

- `getPartitionSize(): number` returns the SQLite database size. The default uses
  `ctx.storage.sql.databaseSize`.
- `canAcceptLocalRequest?(key: RoutingKey, intent: OperationIntent): "ok" | "reject_over_size"`
  applies an optional application policy after the topology returns `ok`.

The runtime always makes the ownership decision first. The application hook cannot replace
`forward` or `reject_out_of_range`. It cannot make a split parent serve local data.

#### Splitting

- `shouldSplit(): SplitType | null` uses the configured split conditions and `getPartitionSize()` by
  default.
- `computeRangeSplitBoundaries(N: number): KeyBytes[] | null` returns `null` by default. This disables
  range splits.
- `onBeforeSplit(): void` runs before the runtime creates children.
- `onSplitStarted(children: ShardingContextResolved[]): void` runs when the parent becomes a router.
- `onSplitCompleted(): void` runs when all children acknowledge.

The runtime enforces promotion and split mutual exclusion around a custom `shouldSplit()` result. A
hook cannot start a range split without valid boundaries.

#### Durable lifecycle hooks

The lifecycle hooks are synchronous in this version. A lifecycle hook must return `void`. It must not
return a promise or start an outbound operation. The runtime rejects a thenable result.

The runtime calls each hook inside `ctx.storage.transactionSync()`. The same transaction writes the
related runtime transition or a durable hook-completion marker. If the hook throws, the transaction
rolls back both the hook writes and the runtime state.

`onBeforeSplit` uses a completion marker in the queued split record. The runtime writes the marker in
the same transaction as the hook. It calls child initialization RPCs only after this transaction
commits. The other lifecycle hooks run in the transaction that writes their state transition.

A hook can write an application-owned work record in the DO storage. The application can process the
record later. Asynchronous lifecycle hooks are outside this version and need a breaking API change.

#### Migration

```ts
type MigrationScanRequest = {
  childCtx: ShardingContextResolved;
  cursor: unknown | null;
  budgetBytes: number;
};

type MigrationBatch = {
  records: MigrationRecord[];
  nextCursor: unknown | null;
};

type MigrationMetadataBatch = {
  metadata: unknown;
  nextCursor: unknown | null;
};

type MigrationAcknowledgement =
  | { kind: "split"; childCtx: ShardingContextResolved }
  | { kind: "promotion"; childCtx: ShardingContextResolved; hashKey: KeyBytes };

type ShardingStatus = {
  context: ShardingContextResolved | null;
  splitStatus?: SplitStatusRecord;
  migrationStatus?: MigrationStatus;
  parentContext?: ShardingContextResolved;
  promotedSubspaces: PromotedSubspace[];
};
```

- `scanMigrationBatch(childCtx, cursor, budgetBytes): Promise<MigrationBatch>` scans application
  records.
- `scanMigrationMetadata?(childCtx, cursor, budgetBytes): Promise<MigrationMetadataBatch>` scans
  application metadata.
- `applyMigrationRecords(records): Promise<void>` applies one record batch.
- `applyMigrationMetadata?(metadata): Promise<void>` applies one metadata batch.
- `onMigrationComplete(): void` runs once after both streams complete and before parent
  acknowledgement.

Each cursor and metadata value must be `StructuredCloneable`. Each scan must keep the complete
serialized result within `budgetBytes`. The record and metadata streams use separate durable cursors.

Each apply hook must be idempotent for an identical replay. The runtime can stop a stream only when
its `nextCursor` is `null`. An empty result with a non-null cursor must continue.

#### Promotion (optional)

- `getSubspaceSize(hashKey: KeyBytes): number` returns `0` by default. This disables promotion.
- `shouldPromoteSubspace(hashKey, size): boolean` returns `false` by default.
- `canCutoverSubspace(hashKey): boolean` returns `false` by default.
- `deleteSubspaceRecords(hashKey: KeyBytes, limit: number): number` returns `0` by default.
- `hasSubspaceRecords(hashKey: KeyBytes): boolean` returns `false` by default.
- `onSubspacePromoted(hashKey): void` runs after cutover and cleanup.

If the DO does not override these hooks, `SubspacePromotion` is disabled.

#### Background

```ts
type BackgroundJob = {
  name: string;
  eligibility: "authoritative_owner" | "migration_complete" | "always";
  run(): Promise<void>;
};
```

`registerBackgroundJobs?(): BackgroundJob[]` lets the DO add jobs to the alarm cycle. The runtime
checks eligibility before it calls a job. `migration_complete` skips all non-terminal migration
states. `authoritative_owner` also skips parents in `split_started` or `split_completed`. `always`
runs after the partition context exists.

FokosDB stale-transaction recovery uses `authoritative_owner`. The runtime catches each job failure,
runs the remaining jobs, and schedules another alarm when durable work remains.

### 4.7 Pluggable stores

#### Range topology store

The runtime provides a default `RangeTopologyStore` that creates a `__fokos_range_routing` SQL
table. The DO can pass a custom `RangeTopologyStore` in the constructor.

```ts
interface RangeTopologyStore {
  getRangeAncestors(hashKey: KeyBytes, lessThanDepth: number): RangeAncestorInfo[];
  insertRangePartitionBoundary(
    hashKey: KeyBytes,
    start: KeyBytes | null,
    end: KeyBytes | null,
    depth: number,
  ): void;
  findDeepestKnownRangeSlice(hashKey: KeyBytes, sortKey: KeyBytes): RangeSlice | null;
}
```

The default table has columns `hash_key`, `start`, `end`, and `depth`. It is only created when a DO
passes no custom store and enables range splits or promotion.

#### Subspace store

The runtime provides a default `SubspaceStore` that creates a `__fokos_promoted_subspaces` SQL
table. The DO can pass a custom `SubspaceStore` in the constructor.

```ts
interface SubspaceStore {
  getStatus(hashKey: KeyBytes): PromotedStatus | undefined;
  setStatus(hashKey: KeyBytes, status: PromotedStatus): void;
  listInFlight(): PromotedSubspace[];
  getSize(hashKey: KeyBytes): number;
  canCutover(hashKey: KeyBytes): boolean;
  deleteRecords(hashKey: KeyBytes, limit: number): number;
  hasRecords(hashKey: KeyBytes): boolean;
  markGcDone(hashKey: KeyBytes): void;
}
```

The default store delegates `getSize`, `canCutover`, `deleteRecords`, and `hasRecords` to the DO's
promotion hooks. It is only created when a DO passes no custom store and enables promotion.

### 4.8 State machines

#### Split lifecycle

The split lifecycle is `split_queued` → `split_started` → `split_completed`.

1. A write or background check calls `shouldSplit()`. When it returns a split type, the runtime
   queues a split.
2. The runtime calls `onBeforeSplit()` in a storage transaction. The same transaction marks this hook
   complete in the queued split record.
3. The alarm calls `runSplit()`. For a range split it first calls `computeRangeSplitBoundaries(N)`.
4. The runtime prepares child contexts and calls `fokosInitFromSplit` on each child.
5. After all children initialize, one storage transaction writes `split_started` and calls
   `onSplitStarted(children)`.
6. The parent calls `fokosTriggerMigration` on each child.
7. Each child pulls and applies its migration streams.
8. Each child retries `fokosAcknowledgeMigrationComplete` until the parent records its
   acknowledgement.
9. The final acknowledgement transaction writes `split_completed` and calls `onSplitCompleted()`.
10. The runtime reports `split_completed` after the transaction commits. Reporter failure does not
    change the split state.

Child initialization and acknowledgement are idempotent. A retry uses the same child contexts.

#### Migration lifecycle

The migration lifecycle is `migration_initialized` → `migration_migrating` →
`migration_ack_pending` → `migration_completed`.

1. `fokosInitFromSplit` stores the parent context and writes `migration_initialized`.
2. A trigger, request, or alarm writes `migration_migrating` and starts the record stream.
3. The child calls `fokosGetMigrationBatch` with the durable record cursor and byte budget.
4. The child applies the records, then checkpoints `nextCursor`.
5. The child repeats steps 3 and 4 until `nextCursor` is `null`.
6. The child repeats the same process for `fokosGetMigrationMetadata` with a separate cursor.
7. One storage transaction calls `onMigrationComplete()` and writes `migration_ack_pending`.
8. The child calls `fokosAcknowledgeMigrationComplete` on the parent.
9. If the call fails or its response is lost, the alarm retries the same acknowledgement.
10. After an acknowledgement succeeds, one storage transaction writes `migration_completed` and
    deletes both cursors.

The migration guard rejects writes in all states except `migration_completed`. A read can use the
parent fallback through `migration_ack_pending`. The parent acknowledgement is idempotent. A parent
split reaches `split_completed`; it never writes a migration state for itself.

#### Promotion lifecycle (optional)

The promotion lifecycle is `queued` → `promoting` → `promoted`.

1. The runtime detects a promotable subspace with `shouldPromoteSubspace`.
2. It creates a range root with `fokosInitFromSplit`.
3. When `canCutoverSubspace` returns `true`, one storage transaction changes the status to
   `promoting`.
4. The hash parent forwards requests for that subspace to the range root.
5. The range root pulls records and metadata from the hash parent.
6. The range root uses the migration acknowledgement protocol to notify the hash parent.
7. The hash parent changes the status to `promoted` after it records the acknowledgement.
8. The hash parent deletes local records in bounded batches.
9. The final cleanup transaction calls `onSubspacePromoted(hashKey)` and marks GC complete.
10. The runtime reports `promotion_completed` after the status becomes `promoted`. Reporter failure
    does not change the promotion state.

### 4.9 Routing

`route()` runs these steps in order:

1. Resolve the `ShardingContext` to `ShardingContextResolved` and validate it against the stored
   context.
2. Validate that the partition is the correct owner or an ancestor of the correct owner.
3. If this partition is a child in a non-terminal migration state, run the migration guard.
   - A write throws a retryable migration error.
   - A read with a `readFromParent` handler calls the strict local-read method on the parent.
4. If this is a hash partition and the subspace is promoted, forward to the range root.
5. Ask the topology for the mandatory `RoutingDecision`.
   - `forward` picks a child and calls `forward`.
   - `reject_over_size` throws a retryable size error.
   - `reject_out_of_range` throws a non-retryable routing error.
   - `ok` continues to the application acceptance hook.
6. If `canAcceptLocalRequest` rejects the request, throw the selected local acceptance error.
7. Call `local` only after all mandatory checks return `ok`.
8. Record a successful forward result so ancestors can learn deeper topology.

`groupByRouting()` uses the same mandatory decisions for multiple keys. It returns `local` items and
one `forwarded` group per destination. The DO uses `forwardToGroup()` to forward each group. A fan-out
starts every destination call, collects every failure, and rethrows when one or more calls fail.

`routeRange()` uses the same context, ownership, migration, promotion, and split checks. A hash
partition forwards the complete request to one hash child or one range root. An unsplit range leaf
calls `local` with an interval clipped to its immutable boundaries.

A split range router walks all child ranges that intersect the request. It visits children in sort-key
order. It clips the interval for each child and gives each call the remaining byte, item, and
partition-visit budgets. It merges leaf records in the same order. It stops when a child returns a
cursor or a budget ends. It emits a cursor only when the current child or a later intersecting child
can return more records.

A migrating range child uses `readFromParent`. The parent handler reads strict local state and must
not enter range fan-out. This prevents a child-to-parent-to-child loop.

### 4.10 Forward result recording

When the runtime forwards to a child, it reads `meta.hashDepth` and
`meta._internal.rangeAncestors` from the response. It updates the in-memory `HashTopology` cache and
the `RangeTopologyStore`. This matches the current `recordForwardResult` behavior in
`HashPartitionTopologyImpl` and `RangePartitionTopologyImpl`.

If a `TopologyReporter` is configured, the runtime calls `report` with a `forward_result` event after
the cache update. The reporter receives the same `rangeAncestors` that the caches use. The runtime
catches and logs a reporter failure. It returns the data result unchanged.

### 4.11 Invariants

The following invariants from the current system must hold:

1. A split parent in `split_started` or `split_completed` must not write local data. It must
   forward writes to children.
2. A migrating child must reject writes until migration completes.
3. A read that tolerates stale data can read from the parent while the child is migrating.
4. `cancel` must reach children in both `split_started` and `split_completed`.
5. Child errors must not be swallowed when forwarding to multiple children.
6. Stale transaction recovery must skip split parents and migrating children.
7. Terminal recovery outcomes must call the public `commit()` or `cancel()` methods. They must not
   bypass routing or migration guards.
8. Hash splits and promotions must be mutually exclusive when any promotion is `queued` or
   `promoting`.
9. `rootTreesN` and `hashSplitN` must not change after initialization.
10. Both range boundaries must remain immutable for the life of a range partition.
11. A child must not become writable before its parent records the migration acknowledgement.
12. A lifecycle hook and its related runtime transition or completion marker must commit atomically.
13. Every internal RPC must carry or derive and validate the required `ShardingContextResolved`.
14. A resolver result must be the correct owner or one of its ancestors.
15. Range-query pagination must preserve order and must not omit or duplicate records.

### 4.12 Concurrency and background work

A Durable Object has one alarm. The `FokosSharding` runtime owns it. The alarm runs these jobs in
order:

1. Child migration and acknowledgement retry.
2. Queued splits.
3. Optional promotion drive and GC.
4. Optional stale-transaction recovery with `authoritative_owner` eligibility.
5. Custom background jobs from `registerBackgroundJobs`.

The runtime isolates a failure in one job and runs the remaining eligible jobs. Before the alarm
returns, the runtime checks all durable work states. When work remains, it sets the next alarm. This
explicit schedule continues after Cloudflare exhausts the automatic alarm retries.

The DO must not call `ctx.storage.setAlarm()` directly. It can request work through
`scheduleBackgroundWork()` and `registerBackgroundJobs()`.

### 4.13 Reserved names

The runtime stores state under the `__fokos_` KV key prefix:

- `__fokos_partition_context`
- `__fokos_parent_partition_context`
- `__fokos_partition_depth`
- `__fokos_split_status`
- `__fokos_split_type`
- `__fokos_migration_status`
- `__fokos_migration_records_cursor`
- `__fokos_migration_metadata_cursor`
- `__fokos_hash_topology`
- `__fokos_partial_range_topology`

It creates these SQL table names only when the relevant feature is enabled:

- `__fokos_range_routing`
- `__fokos_promoted_subspaces`

The following public RPC method names are reserved by the runtime:

- `fokosInitFromSplit`
- `fokosTriggerMigration`
- `fokosGetMigrationBatch`
- `fokosGetMigrationMetadata`
- `fokosAcknowledgeMigrationComplete`
- `fokosStatus`
- `fokosDestroy`
- `fokosDebugForcePromoteSubspace`

A composition DO must expose these exact methods and delegate them to its `FokosSharding` instance.
It must not replace their lifecycle behavior. `FokosShardedDurableObject` supplies the delegates.

#### Existing FokosDB lifecycle keys

The FokosDB adapter supports these old keys during the rollback window:

- `__partition_context`
- `__parent_partition_context`
- `__partition_depth`
- `__parent_split_type`
- `__split_status`
- `__split_migration_status`
- `__split_migration_cursor`
- `__topo_cache`
- `__partial_range_topology`

When a new key is absent, the adapter reads and converts the old value in one storage transaction. It
then writes the new key. During the rollback window, each lifecycle update writes both formats in one
transaction. Context conversion maps `tableName` to `shardGroup` without changing partition identity.
The adapter maps `migration_ack_pending` to old `migration_migrating`. Old code can then replay the
idempotent migration and parent acknowledgement after a rollback.

The new worker keeps these old RPC adapters during the same window:

- `internalInitFromSplit` delegates to `fokosInitFromSplit`.
- `internalTriggerMigration` delegates to `fokosTriggerMigration`.
- `migrationGetItemsBatch` delegates to `fokosGetMigrationBatch`.
- `migrationGetPartitionTransactionMetadata` and `migrationGetPromotedKeysBatch` adapt to
  `fokosGetMigrationMetadata`.
- `migrationAcknowledgeChildComplete` and `migrationAcknowledgePromotionComplete` adapt to
  `fokosAcknowledgeMigrationComplete`.
- `status` delegates to `fokosStatus`.
- `destroyPartition` delegates to `fokosDestroy`.
- `debugForcePromoteKey` delegates to `fokosDebugForcePromoteSubspace`.

A rollback can therefore read every state change that the new worker made. Milestone 10 removes old
keys and adapters only after a separate compatibility approval.

### 4.14 Client side

`ShardedRouter` accepts `env`, a base context, and optional resolver settings. Resolver settings
contain a `TopologyResolver` and a positive `timeoutMs`. The router exposes:

- `partitionContext(): ShardingContext`
- `pickPartition(hashKey, sortKey?): Promise<{ doId: DurableObjectId; partitionContext: ShardingContextResolved }>`
- `rootPartitionContexts(): ShardingContextResolved[]`
- `traverseForDestroy(getStatus, visit): Promise<void>`

When a resolver is present, `pickPartition` asks it first. The router uses a result only when it
returns before `timeoutMs`. An `undefined` result, exception, or timeout falls back to
`env[ns].idFromName(doName)` on the root partition.

The resolver contract permits only the correct owner or one of its ancestors. Milestone 9 adds data
plane validation for this contract. A stale ancestor forwards the request toward its current owner.
An invalid sibling or unrelated result gets a non-retryable routing error after that milestone.

The caller gets a stub and calls its own RPC methods. `ShardedRouter` does not know the DO's method
signatures.

### 4.15 How FokosDB consumes the runtime

`PartitionDO` creates a `FokosSharding<PartitionDO>` instance. It keeps:

- `PartitionStore` for the FokosDB item schema,
- `TransactionParticipant` for two-phase commit,
- `SubspacePromotion` for hash-key promotion,
- `TtlExpiry` for TTL cleanup.

`PartitionDO` implements the hooks by delegating to `PartitionStore`:

- `getPartitionSize` → `PartitionStore.databaseSize`.
- `computeRangeSplitBoundaries` → `PartitionStore.computeRangeSplitBoundaries`.
- `scanMigrationBatch` → `PartitionStore.queryItemsPage` with the child filter and byte budget.
- `applyMigrationRecords` → `PartitionStore.insertItemIfAbsent`.
- `scanMigrationMetadata` → byte-budgeted `PartitionStore` scans for pending transactions and
  promoted keys.
- `applyMigrationMetadata` → idempotent pending-lock insertion and promoted-key inheritance.
- `onMigrationComplete` → `PartitionStore.rebuildKeySizeEstimates` once after all streams finish.
- `getSubspaceSize` → `PartitionStore` key size estimates.
- `canCutoverSubspace` → `PartitionStore.pendingLockCountForHashKey` returns zero.
- `deleteSubspaceRecords` → `PartitionStore.deleteItemsBatchForHashKey`.
- `hasSubspaceRecords` → `PartitionStore.hasItemsForHashKey`.
- `onSubspacePromoted` → `PartitionStore.markPromotedKeyGcDone` and `PartitionStore.deletePendingTxForHashKey`.

The public `apiPutItem`, `apiGetItem`, and `apiDeleteItem` methods call `this.sharding.route()`.
Transaction RPCs call `this.sharding.groupByRouting()`. `apiQueryItems` calls
`this.sharding.routeRange()` so a query can traverse all intersecting range leaves.

Milestone 8 defines the final `FokosDBOptions` after these runtime APIs are stable. The options contain
the FokosDB configuration that the internal context and router need. A FokosDB user does not import
or construct a `fokos-sharding` runtime, router, or context.

### 4.16 Testing

The `fokos-sharding` package has its own Vitest/Miniflare tests. They must cover:

- hash split and migration with a mock DO,
- range split boundaries and routing with both immutable boundaries,
- ordered range queries across all intersecting leaves,
- range-query byte, item, and partition-visit pagination in both directions,
- optional promotion lifecycle with a mock DO,
- the composition runtime with a custom schema and all public RPC delegates,
- alarm delegation for a composition DO and the optional base class,
- a synchronous lifecycle hook that writes application storage,
- rollback when a lifecycle hook throws or returns a thenable,
- record and metadata batch replay after apply and before a cursor checkpoint,
- an empty migration page with a non-null cursor,
- metadata responses near the configured byte budget,
- parent acknowledgement failure and a lost successful response,
- the `migration_ack_pending` restart path,
- an isolated custom background job failure,
- resolver cache miss, exception, and timeout fallback,
- reporter failure after a successful data operation and lifecycle transition,
- an application acceptance hook on a split parent,
- owner-or-ancestor validation and invalid sibling rejection,
- the build guard that no Durable Object class leaks into the client bundle.

The FokosDB tests must continue to pass after each milestone. Compatibility tests must create state with
the old keys, run the new adapter, write more state, and then read all state through the old format.
They must cover `migration_ack_pending` rollback.

The ownership-validation milestone adds test-only key helpers. A hash helper finds a key whose root and
child path match a specified partition ID. A range helper finds a sort key within a specified immutable
range. A test that inspects one physical partition uses a strict local read or a `PartitionStore` test.
The production runtime has no validation bypass.

### 4.17 Performance

The common path for a request that lands on the owner uses one SQLite operation and no extra RPC. A
forwarded request adds one Workers RPC per forwarding hop. The `HashTopology` and
`RangeTopologyStore` caches reduce hops for hot paths. A future `TopologyResolver` can let the client
go directly to a leaf partition.

A range query visits at most `maxPartitionVisits` leaves in one page. Its RPC count grows linearly with
the number of intersecting leaves that the page visits. It forwards sequentially to preserve order and
remaining budgets.

Record and metadata migration use the current 20 MB batch budget from `collectBatch`. The FokosDB
adapter rebuilds key-size estimates once after both streams finish. It does not rebuild them after each
record batch.

### 4.18 Deployment and rollback

`fokos-sharding` is a new dependency of `fokosdb`. The package skeleton and shared-module moves do
not change persisted behavior.

Before `PartitionDO` delegates lifecycle work, the FokosDB adapter adds the dual-read and dual-write
rules from section 4.13. The same deployment adds aliases for every old internal RPC name. New code
can then serve an object with old state, and an old caller can reach a new object during deployment.

During the rollback window, new code keeps the old state representation current. A rollback to the
previous FokosDB version therefore sees every split, migration, promotion, and context update. A child
in `migration_ack_pending` appears as `migration_migrating` to old code and safely replays its work.

Milestone 10 requires a separate compatibility approval before it removes old keys or RPC aliases.
Rollback to a version that only understands the old format is not supported after that removal. The
release notes must identify this boundary.

### 4.19 Open question

#### Final `FokosDBOptions`

The exact FokosDB construction options remain open until the sharding utility API is stable.
Milestone 8 compares the available integration options and selects the minimum FokosDB-specific
fields. The selected API must keep the generic runtime, router, and context internal to FokosDB.

---

## 5. Alternative Options

### 5.1 Keep the generic code in `fokosdb` as a new export

This is smaller, but it keeps the reusable utility inside the FokosDB package. A separate package
makes the boundary clear and lets non-FokosDB applications depend only on `fokos-sharding`.

### 5.2 Use only an inheritance base class

This is easier to use, but it conflicts with Durable Object libraries that already define a base
class, such as the Cloudflare Agents SDK. The plan provides both composition and an optional base
class.

### 5.3 Make promotion mandatory

This would force every DO to track subspace sizes and support a range structure. The plan makes
promotion optional. DOs that only need hash sharding can leave the promotion hooks at their
defaults.

### 5.4 Expose sharding objects through `FokosDBOptions`

FokosDB could require users to construct `FokosSharding`, `ShardedRouter`, or a sharding context. This
would expose the generic topology layer through the FokosDB API. The selected design keeps these
objects internal. Milestone 8 defines FokosDB-specific options after the utility API is stable.

---

## 6. Frequently Asked Questions

### Can the custom DO keep its own SQLite schema?

Yes. The DO owns its own tables. The runtime only touches reserved `__fokos_*` KV keys and SQL
tables.

### Can the DO use its own transaction model?

Yes. The runtime does not force two-phase commit. FokosDB layers its own `TransactionParticipant`
on top.

### What if the DO does not want range splits or promotion?

It leaves `computeRangeSplitBoundaries` and the promotion hooks at their defaults. The runtime
disables those features.

### Why a separate package?

A separate package makes the boundary between the generic sharding engine and the FokosDB item
model explicit. Other projects can depend only on `fokos-sharding`.

### How does the client route requests without knowing the DO's methods?

`ShardedRouter` returns the partition context and `DurableObjectId`. The caller uses its own typed
stub and calls its own RPC.

### Can a lifecycle hook do asynchronous work?

No. A lifecycle hook is synchronous in this version. It can write an application work record in the
same storage transaction. The application processes that record later.

### Must a FokosDB user import `fokos-sharding`?

No. `FokosDBOptions` supplies the FokosDB configuration that the internal runtime and router need.
Milestone 8 defines the exact fields after the utility API is stable.

### What happens when a topology resolver is unavailable?

The router treats a cache miss, exception, or configured timeout as a soft miss. It routes the request
to the root partition.

---

## 7. References

- `packages/fokosdb/src/server/do-partition.ts`
- `packages/fokosdb/src/shared/partition/partition-store.ts`
- `packages/fokosdb/src/shared/partition-topology/split-policy.ts`
- `packages/fokosdb/src/shared/partition-topology/router.ts`
- `packages/fokosdb/src/shared/partition-topology/split-state.ts`
- `packages/fokosdb/src/shared/partition/migration.ts`
- `packages/fokosdb/src/shared/partition/hash-key-promotion.ts`
- `packages/fokosdb/src/client/db.ts`
- `docs/agent-plans/range-partition-splits-v2.md`
- `docs/ideas/2026-08-30-extensible-partition-execution.md`
- [Cloudflare Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Cloudflare Durable Objects alarms](https://developers.cloudflare.com/durable-objects/api/alarms/)
- [Cloudflare Workers RPC](https://developers.cloudflare.com/workers/runtime-apis/rpc/)
