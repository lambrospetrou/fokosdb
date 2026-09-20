# RFC — FokosShardingRuntime: extract the sharding layer of `PartitionDO` into a reusable runtime

**State:** Draft
**Date:** 2026-09-19
**Author:** Lambros Petrou

**Status:** Nothing in this document is built. The repartition flow, the migration protocol, the control RPCs, and
the read-through that this document reuses are built inside `PartitionDO` by
`docs/agent-plans/2026-09-17-unified-repartition-flow.md`.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
  - [4.3 Future extensions](#43-future-extensions)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

`PartitionDO` in `packages/fokosdb/src/server/do-partition.ts` does two jobs. It is the FokosDB data partition:
items, conditions, TTL, and the 2PC participant. It is also the sharding runtime of that partition: identity,
routing, topology caches, splits, key promotions, migration, and the alarm.

The two jobs meet in every RPC method. Each of the 28 RPC methods repeats `ensurePartitionContext`,
`ensureMigration`, and one of `withSplitForwarding`, `groupItemsByRouting`, `routeSingleDestination`, or
`walkRangeChildren`. The boundary between the two jobs does not exist in the code, so no other Durable Object
can use the sharding part.

The sharding part is needed by other Durable Objects that grow past one object and model their data as a hash
key plus a sort key: the GSI forwarders, and a sharded free-text search index. They need deterministic identity,
routing that survives splits, a durable cutover, a resumable data migration, and one alarm that drives many jobs.

This document defines `FokosShardingRuntime`. A Durable Object class creates it in its constructor, gives it a
small set of hooks, registers its operations, and delegates a fixed set of `fokos`-prefixed RPC methods to it.
The runtime owns every topology transition and every forwarding decision. The class keeps its storage, its RPC
surface, and its data semantics.

### Glossary

| Term           | Meaning in this document                                                                          |
| -------------- | ------------------------------------------------------------------------------------------------- |
| runtime        | One `FokosShardingRuntime` instance inside one Durable Object.                                    |
| host           | The Durable Object class that creates the runtime and implements its hooks.                       |
| partition      | One Durable Object that takes part in a shard group. It is a hash partition or a range partition. |
| shard group    | All partitions that share one root set and one `FokosTopology`. The FokosDB `tableName`.          |
| route key      | `{ hashKey, sortKey }` as `KeyBytes`. The runtime routes on these two values only.                |
| repartition    | One durable plan that moves ownership from a source partition to one or more target partitions.   |
| source, target | The partition that gives ownership, and a partition that receives it, inside one repartition.     |
| cutover        | The durable write on the source after which requests for the moved ownership go to targets.       |
| page           | One bounded, opaque unit of migration data that the host builds and applies.                      |
| owner, router  | A partition that serves its keys locally, or one that forwards every key after a split cutover.   |

## 2. Goals and requirements

### 2.1 In scope

- A host class creates one runtime in its constructor and implements `FokosShardingHooks`. No base class is
  needed.
- The runtime owns identity, owner resolution, forwarding, the route caches, the repartition state, the
  migration protocol, acknowledgements, and the Durable Object alarm. The host cannot bypass a transition.
- The host owns its storage schema, its RPC types, its local operation semantics, its admission policy, its
  split policy, its migration pages, and its own background jobs.
- The runtime imports no FokosDB module. A build guard enforces this.
- The runtime exposes the primitive calls that its shapes are made of, so a host can write a traversal that no
  shape offers without a stub of its own.
- The five operation shapes in use today are supported: `point`, `group`, `single_owner`, `range`, and `local`.
- The `range` shape gives the host an optimized interval frontier and tracked calls. The host owns the walk.
- The hash-leaf ownership check, the exhaustive topology compare, the route list on every operation, and a row
  bound on the learned range hierarchy are added.
- `PartitionDO` becomes a host of the runtime and the existing test suites pass. Old code is deleted.
- Public FokosDB error codes do not change.

### 2.2 Out of scope

- A base Durable Object class. Nothing in this design needs it.
- Compatibility with partitions that the current code created. A deployment starts with fresh Durable Object
  namespaces or fresh shard groups (section 4.2.19).
- A pluggable ownership rule. The hash tree and the range tree are the only two rules, and they stay internal.
- A lease around asynchronous local handlers. A local handler is synchronous by default; an `async` handler
  closes the cutover race itself with `dispatch` or `owns` (section 4.2.17).
- An in-memory copy of repartition rows or route overrides. SQL is authoritative (section 4.2.3).
- A scheduler adapter for a base class that owns the alarm. The runtime owns the alarm.
- Changes to the transaction coordinator state machine or to the 2PC protocol. The coordinator wire types
  change to carry the envelope (section 4.2.9); nothing else in the coordinator changes.
- The hooks of section 4.3. They are named with a purpose and no design.
- A Worker-side cache of the live split tree. The Worker enters through a root partition.

### 2.3 Requirements

- The runtime must use `ctx.storage.kv` and `ctx.storage.sql` of the host. Every SQL table with the prefix
  `fokos_` and every KV key under `__fokos/` belongs to the runtime. The host must not read, write, or name them
  in a migration or a statement.
- Every durable transition of the runtime must be one `ctx.storage.transactionSync` call with no `await`.
- One serialized RPC message must stay below 32 MiB
  ([Workers RPC limits](https://developers.cloudflare.com/workers/runtime-apis/rpc/#limitations)). A migration
  page and a status page must stay at or below 20 MiB.
- One KV key and value must stay below 2 MB
  ([Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)).
- Each Durable Object has one alarm ([Alarms API](https://developers.cloudflare.com/durable-objects/api/alarms/)).
  The runtime owns it and the host delegates `alarm()`.
- A host must use a compatibility date on or after `2026-04-21`, or it must enable
  `enhanced_error_serialization` on each RPC provider and consumer. Workers RPC then preserves the `name`, the
  `message`, and the serializable own properties of an error
  ([RPC error handling](https://developers.cloudflare.com/workers/runtime-apis/rpc/error-handling/)).
- Every runtime error must be a `FokosError`. Callers must use `FokosError.isCode`, not `instanceof`.
- The host cannot receive constructor parameters. Topology and host policy travel with every request. A root
  hash partition bootstraps from its first request. Every other partition is created by `fokosInit` only.
- The runtime must not read a field of the host policy. It stores, compares, and forwards it as an opaque value.
- A host must treat each policy field that selects a partition or coordinator namespace as immutable for one
  shard group. The host must validate these fields before it resolves the first stub. The runtime cannot detect
  a change that sends the request to a different namespace.
- `rootTreesN` must be 1 to 65,000. `hashSplitN` and `rangeSplitN` must be 2 to 255.
- A shard group name must be non-empty. It can contain `.` but must not start with `fokos.`.
- FokosDB reserves `fokos.` for its own Durable Object names. Its coordinator group is
  `fokos.tc.<shardGroup>`.
- `rootTreesN` and `hashSplitN` must not change for a shard group that exists.
- Each source step must call at most `REPARTITION_RPC_CONCURRENCY` (6) targets.
- The runtime bounds the pages it builds itself: an overrides page and a status page hold at most 1,000 rows,
  scan at most 10,000 source rows, and stay at or below 20 MiB. A host page is opaque to the runtime, so its
  bounds are a host obligation. The host must keep one page below the RPC limit above. The sharding entry
  exports the FokosDB values (`FOKOS_PAGE_BYTES`, `FOKOS_PAGE_ROWS`, `FOKOS_SCAN_ROWS`) for a host that wants
  the same bounds.
- One target step must pull and commit at most one page. One pass runs up to the value from
  `runtimeConfig().importPagesPerPass`. The default is 16 and the minimum is 1.
- The serialized `routes` list of one envelope must stay at or below `ROUTE_EVIDENCE_MAX_BYTES` (10 KiB).

## 3. Milestones

The work is done in place. Each stage moves existing code and adapts it, so no code is written twice. Each stage
builds, type checks, and keeps the whole test suite green.

### M1 — Move the sharding code into `src/sharding/` and add the entry

Deliverables:

- The directory `packages/fokosdb/src/sharding/` with the entry `packages/fokosdb/src/sharding/index.ts`,
  exported as `fokosdb/sharding` in `package.json` and `tsdown.config.ts`.
- These modules move into it with their tests, with `git mv` and import path updates only:
  `shared/partition-topology/*`, `shared/partition/repartition/*`, `shared/partition/batch-scan.ts`,
  `shared/query/sk-interval.ts`, `shared/bloom-filter.ts`, and `shared/hash-primitives.ts`. `shared/do-stubs.ts`
  stays whole in `shared/`: its helpers read the FokosDB binding and location hint, which are host policy, so
  they are host code (section 4.2.1). `sk-interval.ts` moves because `repartition-slice.ts` needs it, and
  `query/cursor.ts` imports it from its new home.
- The `check-client-bundle` plugin gains a rule for the `sharding/index` entry: it must not reach `src/server/`,
  `src/client/`, `src/shared/expression/`, `src/shared/query/`, or a `src/shared/transaction-*` module. It can
  reach `src/sharding/` and an allow list of generic `src/shared/` modules that the plugin holds (`errors.ts`,
  `invariant.ts`, `tsutils.ts`, `cache-lru.ts`). `src/shared/partition/` joins the forbidden list in M3, because
  `split-policy.ts` and the flow take `PartitionStore` until then.
- No behavior change. The guard passes only after the type-only imports of `do-partition.ts` in the moved
  modules are replaced by sharding-owned types (section 4.2.11).

### M2 — Split `PartitionContext` into identity, topology, and policy

Deliverables:

- `FokosRouteContext<TPolicy>`, `FokosTopology`, `FokosRangeConfig`, and `FokosPartitionRef` (section 4.2.2).
- `FokosRouter` (section 4.2.15) replaces `PartitionTopologyRouterImpl`. `PartitionContextCreator` builds a
  `FokosRouteContext<FokosDbPolicy>`. `db.ts`, the coordinator, and every RPC signature carry it.
- `ensurePartitionContext` compares the topology exhaustively and the range config and the policy structurally.
- The KV keys `__fokos/identity` and `__fokos/policy` replace `__partition_context` and `__partition_depth`.

### M3 — The sharding store

Deliverables:

- `FokosShardingStore` owns every `fokos_` table and every `__fokos/` key, and runs its own migrations in the
  `PartitionDO` constructor before `PartitionStore` runs its own.
- `PartitionStore` drops the repartition tables, `range_hierarchy`, and their statements. `range_hierarchy`
  becomes `fokos_range_hierarchy` with a row bound.
- `RepartitionSource`, `RepartitionTarget`, `HashPartitionTopologyImpl`, and `RangePartitionTopologyImpl` take
  the sharding store, not `PartitionStore`.
- The build guard adds `src/shared/partition/` to the forbidden list of the sharding entry.

### M4 — The runtime object, `dispatch`, and the shapes

Deliverables:

- `FokosShardingRuntime` with its constructor, `dispatch`, the primitive API, the hooks, the operation registry,
  the envelope, the scheduler, and the sharding error module. The runtime constructor runs the sharding store
  migrations.
- `PartitionDO` converts one operation at a time. Each conversion is one reviewable change: the public method
  becomes one `dispatch` call, the local closure becomes a synchronous `local` handler, and the post-write work
  becomes `afterLocalSuccess` signals.
- The transaction operations convert last in this stage. The coordinator and `db.ts` unwrap the envelope.
- `withSplitForwarding`, `groupItemsByRouting`, `routeSingleDestination`, `walkRangeChildren`,
  `forwardToRangeRootPartition`, `ensureMigration`, `scheduleBackgroundWork`, and `runBackgroundWork` are deleted
  from `PartitionDO`.

### M5 — The example host and the independence tests

Deliverables:

- An example host in `packages/fokosdb/test/sharding/` that stores one SQL table and imports nothing from
  FokosDB. It runs a hash split, a range split, a key promotion, and a `range` walk.
- The interval-frontier and ownership property tests of section 4.2.20.
- The FokosDB suites of section 4.2.20 pass unchanged where they go through the client.

## 4. Proposed solution

### 4.1 High-level overview

The host creates one runtime, tells it how to reach other partitions, gives it hooks, and registers its
operations. Every public RPC method of the host is one `runtime.dispatch` call. The runtime validates the
identity, applies the lifecycle gate, resolves the owner of each key, runs the host's local handler or forwards
to another partition, learns its caches, and returns an envelope around the host's result.

```
                Worker                                     Durable Object (host)
  ┌──────────────────────────────┐            ┌───────────────────────────────────────────────┐
  │ FokosRouter                  │            │ class MyDO extends DurableObject               │
  │  rootContext(hashKey) ───────┼── RPC ────►│   fokos = new FokosShardingRuntime({...})      │
  │  allRoots()                  │  carries   │                                               │
  │  walk(...)                   │  topology  │   putItem(ctx, req)  = fokos.dispatch(...)    │
  └──────────────────────────────┘  + policy  │   getItem(ctx, req)  = fokos.dispatch(...)    │
                                              │   fokosInit(...)     = fokos.fokosInit(...)   │
                                              │   fokosMigrationPull = fokos.fokosMigrat...   │
                                              │   alarm(info)        = fokos.alarm(info)      │
                                              │                                               │
                                              │   hooks: evaluateSplit, migration host,       │
                                              │          beforeCutover, admit, jobs, ...      │
                                              └───────────────────────┬───────────────────────┘
                                                                      │ forwards, init, pull, ack
                                                                      ▼
                                                      other partitions of the same class
```

The runtime has one control-plane concept, the repartition. A hash split, a range split, and a key promotion are
three kinds of one plan: select ownership on a source, create targets, cut routing over, migrate data, collect
acknowledgements, and clean the source. The host sees the plan through a few hooks. The runtime keeps every
durable stage in SQL.

Migration is one loop. The target asks the source for one page with an opaque cursor. The host builds the page
on the source and applies it on the target. The runtime commits the page and the cursor in one transaction and
repeats until the host returns no cursor.

One scheduler drives all background work through the Durable Object alarm. The runtime registers its own jobs.
The host registers its jobs, for example TTL expiry. Each job runs one bounded, idempotent step and reports when
it wants to run next.

The shapes are built on a small primitive API that the runtime also exposes: `identity`, `lifecycle`, `owns`,
`resolveOwner`, `rangeVisits`, `children`, and `forward`. The runtime plans an exact, cache-optimized frontier for
one range interval. The host walks that frontier with tracked `local` and `forward` calls. This keeps topology
and cache logic in the runtime while the host owns budgets, cursors, early exit, and result folding.

### 4.2 Technical details

#### 4.2.1 Integration model

The runtime is an owned object, not a base class and not a decorator.

- The host creates it in the constructor, before any other work, so its `blockConcurrencyWhile` runs first.
- The host implements `FokosShardingRpc` (section 4.2.14) with one-line delegations.
- The host calls `runtime.dispatch(operationName, routeCtx, request)` from each public RPC method.
- The host reads `runtime.identity()`, `runtime.lifecycle()`, and `runtime.policy()` when it needs facts.
- The host signals work with `runtime.requestSplitEvaluation()` and `runtime.requestPromotion(hashKey)`.

The host must not call a private method of another partition and must not create a stub for any call. The
runtime is the only code that creates stubs. The host must not call `setAlarm`.

```ts
type MyPolicy = { ns: keyof Env; maxSizeMb: number };

export class MyPartitionDO extends DurableObject<Env> implements FokosShardingRpc {
	readonly fokos: FokosShardingRuntime<MyPolicy>;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.fokos = new FokosShardingRuntime({
			ctx,
			// Host code. It reads the binding and the location hint from its own policy and applies
			// the topology jurisdiction. The runtime never creates a stub itself.
			stub: (routeCtx, doName) => partitionStubByName(env, routeCtx, doName),
			hooks: new MyHooks(ctx.storage, () => this.fokos.policy()),
			operations: myOperations(this),
		});
		void ctx.blockConcurrencyWhile(async () => this.runMyMigrations());
	}

	putItem(routeCtx: FokosRouteContext<MyPolicy>, req: PutReq): Promise<FokosEnvelope<PutRes>> {
		return this.fokos.dispatch("putItem", routeCtx, req);
	}

	fokosInit(req: FokosInitRequest) { return this.fokos.fokosInit(req); }
	fokosStartImport(req: FokosStartImportRequest) { return this.fokos.fokosStartImport(req); }
	fokosMigrationPull(req: FokosMigrationPullRequest) { return this.fokos.fokosMigrationPull(req); }
	fokosMigrationAck(req: FokosMigrationAckRequest) { return this.fokos.fokosMigrationAck(req); }
	fokosExecuteLocal(req: FokosExecuteLocalRequest) { return this.fokos.fokosExecuteLocal(req); }
	fokosRequestPromotion(req: FokosRequestPromotionRequest) { return this.fokos.fokosRequestPromotion(req); }
	fokosStatus(req: FokosStatusRequest) { return this.fokos.fokosStatus(req); }
	fokosPrepareDestroy(req: FokosPrepareDestroyRequest) { return this.fokos.fokosPrepareDestroy(req); }
	fokosDestroy() { return this.fokos.fokosDestroy(); }
	alarm(info: AlarmInvocationInfo) { return this.fokos.alarm(info); }
}
```

#### 4.2.2 Identity, topology, policy, and the route context

Every application request carries a route context. It has four parts with three lifetimes.

```ts
type FokosRouteContext<TPolicy> = {
	/** 2, not 1: the shape differs from `PartitionContext`, and a reader must reject the old record instead of misparsing it. */
	schema: 2;
	/** Immutable identity of the target partition. */
	partitionId: string; // hex-encoded opaque bytes, the wire format of PartitionIdHelper
	doName: string;      // `<shardGroup>.h.<root>[.<child>...]` or `<shardGroup>.r.<hk>.<start>.<end>`
	/** Immutable topology of the shard group. Persisted at creation. A later mismatch is an error. */
	topology: FokosTopology;
	/** Range split parameters the runtime reads when it plans a range split. Mutable, last writer wins. */
	rangeConfig: FokosRangeConfig;
	/** Host policy. Opaque to the runtime. Persisted and replaced when a request carries a new value. */
	policy: TPolicy;
};

type FokosTopology = {
	shardGroup: string;
	rootTreesN: number;
	hashSplitN: number;
	jurisdiction?: DurableObjectJurisdiction;
};

type FokosRangeConfig = {
	/** The child count of the next range split. Range children are named by their boundaries, so it can change. */
	rangeSplitN: number;
	/** The bounded ancestor set a new range child receives. Read once per split. */
	rangeAncestors: { fromRoot: number; fromLeaf: number };
};

type FokosPartitionRef = Pick<FokosRouteContext<unknown>, "partitionId" | "doName">;
```

| Field                   | Lifetime                  | Setter                  | Reader                   |
| ----------------------- | ------------------------- | ----------------------- | ------------------------ |
| `partitionId`, `doName` | Immutable                 | Router or source        | Runtime identity         |
| `topology`              | Immutable per shard group | First Worker request    | Runtime routing          |
| `rangeConfig`           | Last writer wins          | Each Worker request     | Range split planning     |
| `policy`                | Last writer wins          | Each Worker request     | Host through `policy()`  |

Only `topology` is frozen. A field that the runtime reads only when it plans a repartition is mutable, so an
operator can change it without an outage. The plan that reads it snapshots it.

FokosDB maps its `PartitionContext` as follows. `tableName` becomes `topology.shardGroup`. `rootTreesN`,
`hashSplitN`, and `jurisdiction` become `topology`. `rangeSplitN` and `rangeAncestorsConfig` become
`rangeConfig`. `ns`, `nsTx`, `hashSplitConditions`, `rangeSplitConditions`, and `locationHint` become the
FokosDB `policy`. `primaryDoIdStr` is dropped because `idFromName` recreates the deterministic ID.

The FokosDB host treats `ns` and `nsTx` as immutable for one shard group. It validates them before it resolves
its first partition or coordinator stub. A change selects another Durable Object namespace, so a runtime inside
the old namespace cannot detect it. Other policy fields remain last-writer-wins.

**Persisted identity**, KV `__fokos/identity`, written once at bootstrap or `fokosInit`:

```ts
type FokosPartitionIdentity = {
	schema: 1;
	ref: FokosPartitionRef;
	kind: "hash" | "range";
	/** Hash: the root index and the child path, decoded from ref.partitionId. */
	hash?: { rootIndex: number; path: number[] };
	range?: {
		hashKey: KeyBytes; start: KeyBytes | null; end: KeyBytes | null; depth: number;
		/** The bounded ancestor set from `fokosInit`. Immutable. */
		ancestors: RangeAncestorInfo[];
	};
	topology: FokosTopology;
};
```

**Persisted policy**, KV `__fokos/policy`, holds `{ rangeConfig, policy }`: the last values a request carried.
Background jobs read them when no request is in flight.

Validation on each request, in `dispatch` step 1:

1. `partitionId` and `doName` must equal the stored identity. `topology.jurisdiction` must equal
   `ctx.id.jurisdiction`. Every field of `topology` must equal the stored topology; the compare is exhaustive
   over the type. A mismatch throws `partition_context_mismatch`.
2. `rangeConfig` and `policy` are compared structurally with the stored values. When either differs, the runtime
   validates the `rangeConfig` bounds and writes both in one `transactionSync` before it runs the operation.
   Equal values cost one compare and no write.
3. A root hash partition without a stored identity writes the identity, the range config, and the policy from
   the first valid route context. An uninitialized non-root hash partition throws
   `hash_partition_not_initialized`. An uninitialized range partition throws
   `range_partition_not_initialized`. Only `fokosInit` creates either target kind.

A target created by `fokosInit` receives its full route context from the source. The source derives the child
`partitionId` and `doName`. It copies its own `topology`, stored `rangeConfig`, and stored `policy`.

The runtime validates the topology bounds of section 2.3 at bootstrap and on every `fokosInit`. It validates the
range config bounds on every write of `__fokos/policy`. It rejects an empty shard group and a shard group that
starts with `fokos.`. A violation throws `partition_context_options_invalid`.

**Runtime options**, a constructor argument:

```ts
type FokosRuntimeOptions<TPolicy> = {
	/** A stub for one partition of the host's own class. */
	stub(ctx: FokosRouteContext<TPolicy>, doName: string): DurableObjectStub;
	caches?: {
		hashArenaBytes?: number;
		rangeHierarchyMaxRows?: number;
		promotionBloom?: { expectedKeys: number; falsePositiveRate: number };
	};
	scheduler?: { fallbackAlarmMs?: number; fastPathDelayMs?: number };
};
```

The `stub` callback must apply the host binding, the topology jurisdiction, and the policy location hint. When
the runtime needs a stub outside a request, it calls `stub` with its own stored route context.

#### 4.2.3 Persisted state owned by the runtime

The runtime owns this KV state:

- `__fokos/identity`: `FokosPartitionIdentity`.
- `__fokos/policy`: `{ rangeConfig, policy }`.
- `__fokos/import`: `FokosImportRecord` from section 4.2.12.
- `__fokos/repartition/<id>/plan/00000001`: `FokosStoredRepartitionPlan`.
- `__fokos/destroying`: `true` after `fokosPrepareDestroy`.
- `__fokos/jobs`: `{ [jobName]: { nextRunAt } }`.
- `__fokos/cache/hash_arena`: the byte-bounded `HashTopologySnapshot`.
- `__fokos/cache/promotion_bloom`: the byte-bounded `PartialRangeTopologySnapshot`.
- `__fokos/schema_version`: the last sharding migration that ran.

The runtime owns these SQL tables:

- `fokos_repartitions`: one row per repartition, as shipped.
- `fokos_repartition_targets`: one row per target, as shipped.
- `fokos_route_overrides`: `hash_key` to repartition ID, as shipped.
- `fokos_range_hierarchy`: the row-bounded learned descendant boundaries.

The three repartition tables keep the schema of `docs/agent-plans/2026-09-17-unified-repartition-flow.md`
section 4.2. Target references and slices stay in SQL and do not repeat in the KV plan. `fokos_range_hierarchy`
keeps the columns of `range_hierarchy` and adds `learned_at INTEGER NOT NULL` for eviction order.

```ts
type FokosStoredRepartitionPlan<TPolicy = unknown> = {
	schema: 1;
	queue: {
		policy: TPolicy;
		data?: unknown;
	};
	planned: null | {
		rangeDepth?: number;
		rangeAncestors?: RangeAncestorInfo[];
	};
	/** The key of the next plan item. Version 1 always stores null. */
	nextKey: string | null;
};
```

The queue transaction writes the plan head and the `queued` SQL row together. The planning transaction writes
the target rows, fills `planned`, and moves the SQL row to `planned`. Cutover retains the head. The runtime
reconstructs `FokosRepartitionPlan` from the head, the repartition row, and the target rows for every later hook.
The completion and cleanup hooks therefore read the policy and data from queue time.

The final cleanup transaction deletes the plan chain before it moves the repartition to `cleaned`. Version 1 has
one item. A later version can write another object first and then set `nextKey` to its key in the same
transaction. Readers follow keys until `nextKey` is null.

`FokosShardingStore` in `src/sharding/sharding-store.ts` owns every statement over these tables and keys. It
runs its migrations in the runtime constructor inside `blockConcurrencyWhile`, before the host runs its own
migrations, and records the last migration in `__fokos/schema_version`. The host's migration runner must not
name a `fokos_` table.

The runtime loads the identity, the policy, and the import record into memory in its constructor. Only the
runtime writes those three records, so the in-memory copy cannot go stale. It keeps no copy of any repartition
row, target row, or route override: the split row and its targets are read from SQL on every resolution, as the
shipped flow reads them, and a point override lookup is one indexed seek that joins `fokos_route_overrides` to
its repartition row. The dangerous value is absence. A stale "no split row" on an in-memory copy would make a
router serve rows its targets own, so every one of these reads stays in SQL (section 4.3 names the cache as a
future optimization).

Repartition rows are permanent routing rules. A completed split makes the source a router forever. A completed
promotion sends one hash key to a range tree forever.

#### 4.2.4 Runtime construction and API

```ts
class FokosShardingRuntime<TPolicy> implements FokosShardingRpc {
	constructor(
		opts: FokosRuntimeOptions<TPolicy> & {
			ctx: DurableObjectState;
			hooks: FokosShardingHooks<TPolicy>;
			operations: Record<string, FokosOperation<any, any>>;
		},
	);

	dispatch<Req, Res>(op: string, routeCtx: FokosRouteContext<TPolicy>, req: Req): Promise<FokosEnvelope<Res>>;

	// ─── the primitive API. The shapes are built on these, and a host traversal uses them too. ───
	identity(): FokosPartitionIdentity;
	/** The stored host policy. Throws the partition-kind initialization error before bootstrap. */
	policy(): TPolicy;
	/** The stored route context of this partition. */
	routeContext(): FokosRouteContext<TPolicy>;
	lifecycle(): FokosLifecycle;
	/**
	 * True when this partition owns the key now. Reads the topology and the route overrides only, never a
	 * cache, so a Bloom false positive cannot make a host sweep skip a key it owns.
	 */
	owns(key: RouteKey): boolean;
	/** The point-routing answer, caches included. A speculative remote owner is a hint, not a fact. */
	resolveOwner(key: RouteKey): FokosOwner;
	/** This router's direct targets in `target_index` order. Empty on an owner. */
	children(): FokosChild[];
	/** A disjoint, ordered cover of one range request. Section 4.2.10 defines the cover. */
	rangeVisits(input: FokosRangeInput): FokosRangeVisit[];
	/** Forward one registered operation to one target, learn its routes, and count the RPC. */
	forward<Res>(target: FokosPartitionRef, op: string, req: unknown): Promise<FokosEnvelope<Res>>;
	/** Forward one planned range visit, including speculative fallback. */
	forwardRangeVisit<Res>(visit: FokosRangeVisit, op: string, req: unknown): Promise<FokosEnvelope<Res>>;

	// ─── signals and jobs ───
	requestSplitEvaluation(): void;
	/** Routes to the current owner of the key first (section 4.2.14), then queues there. */
	requestPromotion(hashKey: KeyBytes, data?: unknown): Promise<FokosRequestPromotionResult>;
	scheduleJob(name: string, runAt: number): void;
	/** One background pass. `alarm(info)` calls this and nothing else. */
	runDueWork(info?: AlarmInvocationInfo): Promise<void>;
}

type FokosChild = { ref: FokosPartitionRef; start: KeyBytes | null; end: KeyBytes | null };

type FokosRangeInput = {
	hashKey: KeyBytes;
	start: KeyBytes | null;
	end: KeyBytes | null;
	descending: boolean;
};

type FokosRangeVisit = {
	target: FokosPartitionRef | "local";
	start: KeyBytes | null;
	end: KeyBytes | null;
	speculative: boolean;
};

type FokosLifecycle = {
	role: "owner" | "router";
	import: null | { state: "awaiting_data" | "importing" | "imported" | "active" };
	activeRepartition: null | { id: string; kind: RepartitionKind; state: "queued" | "planned" | "cutover" };
};
```

`forward` and `forwardRangeVisit` are the only ways a host reaches another partition. A host can build a custom
recursive traversal with `children()` and `forward()`. The `range` shape uses `rangeVisits()` and
`forwardRangeVisit()` to keep range topology and speculative fallback in the runtime. Its host `walk` callback
owns visit order, budgets, cursors, early exit, and result folding.

#### 4.2.5 Hooks

```ts
interface FokosShardingHooks<TPolicy> {
	/**
	 * Called after a local success that signals `evaluateSplit`, and by `requestSplitEvaluation`. Returns
	 * `false`, or `{ data }` when the host wants this partition to split now. `data` is opaque and travels
	 * in the plan. Synchronous. The host reads its own metrics and thresholds from `policy`. A queued split
	 * is not evaluated again: the row moves to `planned` on its own.
	 */
	evaluateSplit(input: { identity: FokosPartitionIdentity; policy: TPolicy }): false | { data?: unknown };

	/**
	 * Range partitions only. Returns `childCount - 1` strictly increasing boundaries inside (start, end),
	 * or null when the host cannot produce valid boundaries yet. Synchronous.
	 */
	computeRangeBoundaries?(input: {
		hashKey: KeyBytes; start: KeyBytes | null; end: KeyBytes | null; childCount: number; policy: TPolicy;
	}): KeyBytes[] | null;

	/** The host phase of the migration. Section 4.2.12. */
	migration: MigrationHost;

	/**
	 * Source side. Consulted at two points: before the runtime initializes the first target of the plan,
	 * and inside the cutover transactionSync. Return false to hold the plan at its current state and
	 * retry at the flat lock interval. The first point stops a range root from being created for a key
	 * that cannot move yet; the second closes the window in which a lock appeared while the targets
	 * were created. FokosDB returns `pendingLockCountForHashKey(hk) === 0` for a promotion. Synchronous.
	 */
	beforeCutover?(plan: FokosRepartitionPlan): boolean;

	/** Source side. Runs inside the completion transactionSync, after the last acknowledgement. Synchronous. */
	beforeComplete?(plan: FokosRepartitionPlan): void;

	/**
	 * Source side, optional. One bounded step of source cleanup after completion. Returns whether the
	 * source rows of the plan are all gone. Undefined means the source keeps its data. Synchronous.
	 */
	cleanupSourceStep?(plan: FokosRepartitionPlan): boolean;

	/** Called by the local admission step. Default: allow. Synchronous. */
	admit?(input: {
		op: string;
		admissionTag?: string;
		keys: RouteKey[];
		lifecycle: FokosLifecycle;
		policy: TPolicy;
	}): "allow" | { reject: Error };

	/** Live runtime configuration overrides. The runtime validates each returned value. Synchronous. */
	runtimeConfig?(): FokosRuntimeConfigOverrides;

	/** Host background jobs. Section 4.2.13. */
	jobs?: FokosJob[];
}

type FokosRuntimeConfigOverrides = {
	/** Default: 16. Minimum: 1. */
	importPagesPerPass?: number;
};

interface MigrationHost {
	buildPage(cursor: unknown, slice: FokosSlice, belongsToTarget: (key: RouteKey) => boolean):
		{ page: unknown; nextCursor: unknown | null };
	/** Runs inside the page transactionSync together with the cursor checkpoint. Synchronous. */
	applyPage(page: unknown, slice: FokosSlice): void;
	validatePage(cursor: unknown, page: unknown, nextCursor: unknown | null): void;
}

type FokosSlice =
	| { kind: "hash_child"; childIndex: number; depth: number }
	| { kind: "range"; hashKey: KeyBytes; start: KeyBytes | null; end: KeyBytes | null }
	| { kind: "promoted_key"; hashKey: KeyBytes };

type FokosRepartitionPlan<TPolicy = unknown> = {
	id: string;
	kind: "hash_split" | "range_split" | "key_promotion";
	source: FokosPartitionRef;
	targets: Array<{ ref: FokosPartitionRef; slice: FokosSlice }>;
	/** "router": the source owns nothing after cutover. "retains_others": it owns all non-selected keys. */
	sourceAfterCutover: "router" | "retains_others";
	/** The host policy at queue time. Every hook that receives the plan reads this copy. */
	policy: TPolicy;
	/** Opaque host data from `evaluateSplit` or `requestPromotion`. */
	data?: unknown;
};

type FokosRequestPromotionResult = {
	/** The partition that holds the decision. */
	owner: FokosPartitionRef;
} & (
	| { queued: true; state: RepartitionState }
	/** The owner already tracks the key. `state` is the state of that promotion. */
	| { queued: false; reason: "already_promoted"; state: RepartitionState }
	/** A split row exists on the owner, so the key moves soon. FokosDB maps this to `partition_over_size`. */
	| { queued: false; reason: "split_in_progress" }
);
```

Rules for hooks:

- Every hook is synchronous. Four of them run inside a `transactionSync`, and an `await` there is a defect.
- A hook that receives a `plan` reads `plan.policy`, the snapshot from queue time. A hook without a plan calls
  `runtime.policy()`, the live value that the last request updated.
- A hook must not throw to express a policy result. It returns the result. A thrown error is a defect: the
  runtime logs it, keeps the durable state unchanged, and retries on the next background pass.
- `applyPage` must be idempotent. The runtime commits the page and the cursor in one transaction, but the host
  cursor is opaque, so two pages can overlap at a phase boundary.
- `belongsToTarget` in `buildPage` is the ownership function of section 4.2.8. The host must filter rows with
  it. For a `hash_child` slice it returns false for a hash key with a terminal route override, because a range
  tree owns that key; the child receives the override pointer and no data copy. For a `range` slice it tests the
  hash key and `[start, end)`. For a `promoted_key` slice it tests the hash key only.

#### 4.2.6 Operation descriptors

The host registers each operation once. The runtime uses the descriptor for `dispatch`, for `forward`, and for
`fokosExecuteLocal`, which finds the local handler by name.

```ts
type FokosOperationBase<Req, Res> = {
	/**
	 * "retry": while this partition imports, throw `partition_migrating`.
	 * "read_source": while this partition imports, run the same operation locally on the source partition.
	 */
	whileMigrating: "retry" | "read_source";
	/**
	 * The operation never writes partitioned data. Required for `whileMigrating: "read_source"`. The
	 * constructor throws `sharding_operation_invalid` for a `read_source` descriptor without it.
	 */
	readOnly?: boolean;
	/** Opaque to the runtime. Passed to hooks.admit. */
	admissionTag?: string;
	/**
	 * "sync" (default): `local` must return a value; a thenable throws `sharding_local_must_be_sync`.
	 * "async": `local` can await. The host follows the write rule of section 4.2.17.
	 */
	localMode?: "sync" | "async";
	local(req: Req): Res | Promise<Res>;
	/**
	 * Optional. Runs on every partition the request passes through, owner or router, after admission
	 * and before the first remote call, with the complete request. Synchronous. It is for work that is
	 * keyed by something other than a route key and must happen on every hop, for example a lock
	 * release by transaction id. It must not write partitioned data by key: owner resolution has not
	 * placed the request yet. Its result is discarded and it cannot change `value`.
	 */
	beforeForward?(req: Req): void;
	/**
	 * Optional. Default: the runtime calls the method named `op` on the target stub with the derived route
	 * context and the request. A host overrides it only when the remote method has another name.
	 */
	forward?(stub: DurableObjectStub, target: FokosRouteContext<unknown>, req: Req): Promise<FokosEnvelope<Res>>;
	/** Runs after a local success. Returns signals. Cannot change the result. Synchronous. */
	afterLocalSuccess?(req: Req, res: Res): FokosSignals | void;
};

type FokosSignals = {
	evaluateSplit?: boolean;
	promotionCandidates?: Array<{ hashKey: KeyBytes; data?: unknown }>;
	/**
	 * A condition that `beforeCutover` tests has changed, for example a lock was released. The runtime
	 * marks every repartition row that `beforeCutover` held back as due now and runs the fast path.
	 * Without it the held row waits out the flat lock interval.
	 */
	repartitionUnblocked?: boolean;
	/** Host jobs that must run by a deadline because of this result. */
	jobs?: Array<{ name: string; runAt: number }>;
};

type FokosOperation<Req, Res> =
	| (FokosOperationBase<Req, Res> & { shape: "point"; key(req: Req): RouteKey })
	| (FokosOperationBase<Req, Res> & {
			shape: "group";
			items(req: Req): Array<{ key: RouteKey; item: unknown }>;
			subRequest(req: Req, items: unknown[]): Req;
			merge(parts: Array<{ target: FokosPartitionRef | "local"; result: Res }>): Res;
			/** "fail_fast": stop at the first failure. "attempt_all": run every group, then throw if any failed. */
			failurePolicy: "fail_fast" | "attempt_all";
		})
	| (FokosOperationBase<Req, Res> & {
			shape: "single_owner";
			items(req: Req): Array<{ key: RouteKey }>;
			/** The answer when the items span more than one partition. Returned, never thrown. */
			notApplicable: Res;
		})
	| (FokosOperationBase<Req, Res> & {
			shape: "range";
			whileMigrating: "read_source";
			readOnly: true;
			range(req: Req): FokosRangeInput;
			/** Restricts the request to one planned visit. */
			clip(req: Req, visit: FokosRangeVisit): Req;
			/**
			 * Owns budgets, cursors, visit order, early exit, and result folding. It reaches partitions only
			 * through the tracked functions in `input`.
			 */
			walk(input: {
				request: Req;
				visits: readonly FokosRangeVisit[];
				local(req: Req): Res | Promise<Res>;
				forward(visit: FokosRangeVisit, req: Req): Promise<FokosEnvelope<Res>>;
			}): Promise<Res>;
		})
	| {
			shape: "local";
			/** Can be async: a `local` shape has no owner resolution to race against. */
			local(req: unknown): unknown | Promise<unknown>;
		};
```

The five shapes behave as follows:

- `point` resolves one key. It runs locally or forwards to one partition and learns exact point routes.
- `group` resolves every item. It builds one local group and one group per remote partition. The host merges.
- `single_owner` resolves every item. It selects one destination or returns the host's `notApplicable` value.
- `range` resolves one hash key and one sort-key interval. The runtime plans the frontier. The host walks it.
- `local` resolves nothing. It is not forwarded or gated. Admin calls and custom host traversals use it.

When the items of a `single_owner` operation span more than one partition, the runtime returns the value the
host names as `notApplicable` instead of an error. Today that value is `{ outcome: "not_applicable" }` on
`SingleShotResponse` and `ReadSnapshotResponse`. It has no side effects and passes unchanged through every
forwarding hop, so the caller runs its multi-partition path. On a split table it is the ordinary answer for
such a set, so it is not an error.

A `group` operation on a router has an empty local group, and the runtime does not call `local` for it. The
host `merge` receives zero or more parts. When `items(req)` returns an empty list, the runtime runs `local(req)`
on this partition, owner or router, with no remote group.

Work that must happen on every hop goes into `beforeForward`. `txCancel` is the case in FokosDB: a router
between cutover and completion still holds the pre-cutover lock rows of a transaction, and the release is by
transaction id, not by key. Its `beforeForward` calls `cancelLocal(transactionId)` on every node the cancel
passes through, and its `local` handler is then empty. `beforeForward` runs before the remote groups start, so
a child failure cannot leave the local row behind.

For a `range` operation, the runtime computes the visits before it calls `walk`. The functions in the walk input
record each local scope and forwarded envelope. The host must use only those functions to reach partitioned
data. `fokosExecuteLocal` clips the request to the caller slice and calls only the descriptor's `local` handler.
It never calls `walk`.

#### 4.2.7 The dispatch pipeline

`dispatch` runs these steps in this order.

1. **Identity.** Validate the identity and the topology, store a changed policy, or bootstrap a root
   (section 4.2.2).
2. **Lifecycle gate.** When `import.state` is `awaiting_data` or `importing`, the runtime first schedules
   `target_import` on the fast path and moves the fallback alarm to `now + fallbackAlarmMs` when that is earlier.
   Then, in both states:
   - `whileMigrating: "read_source"` → run owner resolution step 1 for the keys of the request and throw
     `partition_misrouted` on a miss. Then call `source.fokosExecuteLocal({ op, request, caller: selfRef,
     repartitionId })`, add one to the envelope-level `forwardCount`, and return (section 4.2.12). The source answers
     `repartition_not_cut_over` from its own row while it is `queued` or `planned`.
   - `whileMigrating: "retry"` → throw `partition_migrating`.
   The states `imported` and `active` pass the gate.
3. **Routing.** Resolve every point key and group items by destination. For a `range` operation, compute the
   exact interval frontier from section 4.2.10.
4. **Admission.** When local work exists, call `hooks.admit`. A rejection throws the host error unchanged.
5. **Execution.** Steps 3 and 4 are synchronous, and so is this order inside step 5: call `beforeForward` when
   the descriptor has it, then run the local work of the shape, then start the remote calls. The first `await`
   of `dispatch` after step 1 is the await on a remote call, and every local write of a `point`, `group`, or
   `single_owner` operation is already committed at that point. A cutover therefore cannot interleave between
   owner resolution and the local write, for any shape. Remote groups run in parallel. For a `range`
   operation, call the host's `walk` callback with the frontier and tracked functions.
6. **Learning.** Learn every route-evidence entry from each successful remote envelope. Add each outbound
   partition RPC to the envelope-level `forwardCount`.
7. **Signals.** When local work succeeded and the descriptor has `afterLocalSuccess`, collect the signals. Apply
   repartition signals per section 4.2.11. Apply each `jobs` entry through `scheduleJob`, which persists the
   deadline and moves the alarm earlier when necessary. An error here is logged and does not change the result.
   This step runs before the response is returned. It is a behavior change for `txCommit` and
   `txExecuteSingleShot`, which today run their split check in `ctx.waitUntil` after the response. A follow-up
   can let a descriptor mark its signals as deferred, so the runtime applies them in `waitUntil` after step 8.
8. **Envelope.** Return the value with the collected point and range route evidence.

When `__fokos/destroying` is true, step 1 throws `partition_migrating` for every operation. `fokosStatus`,
`fokosPrepareDestroy`, and `fokosDestroy` stay available.

#### 4.2.8 Owner resolution

One function resolves a point route key to an owner. The `point`, `group`, and `single_owner` shapes use it.
`runtime.resolveOwner` exposes the same function. The `range` shape uses the interval planner of section 4.2.10.

```ts
type FokosOwner =
	| { kind: "local" }
	| { kind: "remote"; target: FokosPartitionRef; speculative: boolean }
	| { kind: "out_of_range" };
```

Resolution order on a hash partition:

1. **Ownership.** The hash key must hash to `identity.hash.rootIndex` with `rootTreesN`, and at each depth `d` of
   `identity.hash.path` it must hash to `path[d]` with `hashSplitN`. Otherwise `out_of_range`. This is one hash
   per level, in memory, and it turns a routing defect into an error instead of a write on the wrong partition.
2. **Route override.** When `fokos_route_overrides` has the hash key and its repartition is `cutover` or later,
   the owner is the range root, or a deeper range slice from the range hierarchy cache. This is authoritative.
3. **Promotion Bloom cache.** When the filter reports a probable promotion by a descendant, the owner is the
   range root, `speculative: true`. Point and range shapes use this step. Group and single-owner shapes skip it.
4. **Topology.** When this partition is a router, pick the child by the hash function at this depth, then apply
   the hash arena cache to jump deeper. Otherwise the owner is local.

Resolution order on a range partition:

1. **Ownership.** The hash key must equal `identity.range.hashKey`, and the sort key must be inside
   `[start, end)`. Otherwise `out_of_range`.
2. When this partition is a router, pick the child whose interval contains the sort key, then apply the range
   hierarchy cache to jump deeper. Otherwise the owner is local.

A speculative range forward that fails with `range_partition_not_initialized` or
`repartition_not_cut_over` resolves again with the Bloom step disabled. The `repartition_not_cut_over` fallback
applies to reads only; a write throws it unchanged. A cached hash jump that receives
`hash_partition_not_initialized` invalidates that hash-arena hint and retries from the nearest known ancestor.
A non-cached call propagates the error.

`out_of_range` throws `partition_misrouted`. It is a routing defect, not backpressure.

`runtime.owns(key)` runs hash steps 1, 2, and 4, or range steps 1 and 2, and skips the two caches. It answers
`true` only for `{ kind: "local" }`. A host job that sweeps its rows relies on it, and a Bloom false positive
must not make the sweep skip a key this partition owns.

The `belongsToTarget` predicate of `buildPage` is the same function as hash step 4 for hash children and range
step 2 for range children, with one addition for hash children: a key with a terminal route override returns
`false`. One implementation, two callers.

#### 4.2.9 Response envelope

The runtime wraps every result. Application response types do not carry routing hints. A host value can still
carry application metrics.

```ts
type FokosRouteScope =
	| { kind: "point"; key: RouteKey }
	| { kind: "range"; hashKey: KeyBytes; start: KeyBytes | null; end: KeyBytes | null };

type FokosRouteNode = {
	servedBy: FokosPartitionRef;
	servedByActorId: string;
	hashDepth: number;
	rangeDepth: number;
	/** Internal. Bounded ancestor boundaries of a range leaf. Consumers must drop it. */
	_hint?: { rangeAncestors: RangeAncestorInfo[] };
};

/** One serving partition and every scope it served in this response. */
type FokosRouteEvidence = FokosRouteNode & { scopes: FokosRouteScope[] };

type FokosEnvelope<T> = {
	value: T;
	routing: {
		/** The partition that produced or merged `value`. */
		summary: FokosRouteNode;
		routes: FokosRouteEvidence[];
		/** Total outbound partition RPCs in this response tree. */
		forwardCount: number;
		/** True when the byte cap dropped one or more entries from `routes`. */
		routesTruncated: boolean;
	};
};
```

The list holds one entry per serving partition, keyed by `servedBy.partitionId`. A point result has one entry
with one point scope. A grouped result adds one point scope per served key to the entry of its partition, in
request order. A range result adds one range scope per served interval, in visit order. A forwarding partition
merges the entries of a child envelope into its own list by the same key, so a partition that served ten keys
appears once with ten scopes, and its `_hint` travels once.

The serialized list is capped at `ROUTE_EVIDENCE_MAX_BYTES` (10 KiB, section 2.3). The runtime measures the
list with a conservative estimator, as `fokosStatus` measures its page. When an entry would cross the cap, the
runtime drops that entry and every later one and sets `routesTruncated`. `summary` and `forwardCount` are never
dropped. Evidence is a cache hint (section 4.2.10), so a dropped entry costs one more hop on a later request
and nothing else. A `local` operation can return an empty route list when it serves no partitioned data.

A local result uses the serving partition as `summary`. A forwarded `point` or `single_owner` result keeps the
child `summary`. A `group` result that calls `merge` and a `range` result that calls `walk` use the current
partition as `summary`, even when the host consumes one part.

A forwarding partition learns every evidence entry from the child envelope, merges the entries into its own
list by `servedBy.partitionId`, and changes no field of an entry. It adds one to the envelope-level
`forwardCount` for each outbound partition RPC. A range walk collects only the
local and forwarded calls that the host made through its tracked functions.

An error follows the same rule. The partition that raises an error attaches its `routing` as a serializable own
data property and uses itself as `summary`. A forwarding partition learns from `error.routing`, adds its RPC
count, and rethrows the same error object. A partition without an identity attaches no routing data.

Every operation returns an envelope, including transaction operations. `TransactionCoordinatorDO` unwraps the
envelope of `txPrepare`, `txCommit`, and `txCancel` where it calls `partitionStubByName`. `db.ts` unwraps every
partition call with `FokosRouter.unwrap`. The unwrap removes `scopes` and `_hint`. FokosDB combines `summary`
with the aggregate operation metrics to build `meta`. It keeps `partitionMetas` inside `value` for per-partition
operation metrics and combines them with the public route list. The old `_internal` field is removed.

#### 4.2.10 Route caches and the range frontier

Caches are hints. A miss, a full cache, a stale entry, or a disabled cache changes latency only. Ownership is
decided by the route override table and the durable topology, never by a cache.

The runtime uses three caches:

- The hash arena uses KV `__fokos/cache/hash_arena`. It learns `hashDepth` from point and range route evidence.
  `caches.hashArenaBytes` and the depth cap bound it.
- The range hierarchy uses SQL `fokos_range_hierarchy`. It learns `_hint.rangeAncestors` from range evidence.
  `caches.rangeHierarchyMaxRows` bounds it.
- The Promotion Bloom cache uses KV `__fokos/cache/promotion_bloom`. It learns when a hash partition receives
  evidence from a range partition. Its filter size bounds it, and it does not remove entries.

The range hierarchy table holds learned rows only. A learn writes or refreshes `learned_at`. When full, the
runtime evicts the rows with the oldest `learned_at`, deepest first. A partition's own ancestors are in its
identity, so eviction cannot change its route evidence. The default of `rangeHierarchyMaxRows` is 10,000. A row
holds two boundary keys and one hash key. The cache implementations move from `hash-topology.ts` and
`partial-range-topology.ts` with their tests. They use one internal contract: `lookup`, `learn`, and `invalidate`.

**The range frontier.** `rangeVisits()` returns an ordered list of non-overlapping visits. Their union must equal
the requested interval inside the current partition. A range partition first validates the hash key and requires
the request interval to be inside its immutable interval. A violation throws `partition_misrouted`.

The runtime then builds the frontier:

1. Build a complete base cover. A hash router selects the hash destination. A promoted hash key selects the
   range root. A range router uses its durable direct children. A leaf uses `local`.
2. Intersect each base interval with the requested interval.
3. Overlay the learned range descendants. Split a base interval at each learned boundary.
4. For each segment, select the deepest known partition that fully contains it.
5. Keep the base target for each gap in the learned hierarchy.
6. Merge adjacent segments that have the same target.
7. Order the visits by `descending`.

A partial cache cannot create a coverage gap because each uncovered segment keeps its base target. A learned
partition can have split again. Its immutable interval still contains the visit, and that partition computes a
deeper frontier. When learned descendants cover the full interval, the caller bypasses the range root.

A cached range visit that gets `range_partition_not_initialized` invalidates that hierarchy entry. The runtime
computes the segment again without that entry and uses its base target.

A Promotion Bloom hit makes the range-root base cover speculative. A speculative visit that gets
`range_partition_not_initialized` or `repartition_not_cut_over` invalidates that plan. The runtime computes the
hash-tree frontier again with the Bloom step disabled, inside the same `forward(visit, req)` call. On a hash
router the new target is the hash child. On a hash leaf the new target is this partition: `forward` then calls
the descriptor's `local` handler with the same clipped request and returns an envelope whose `summary` and
evidence name this partition. The visit interval does not change, so the host's cursor and budget logic sees
one answer for one visit. This is what `withSplitForwarding` does today when `maybeForwardToRangeRootPartition`
returns null. The host does not implement either fallback.

The runtime calls the descriptor's `walk` callback with the frontier. The host calls `clip` before each visit and
uses only the tracked `local` and `forward` functions. The host can visit sequentially or with bounded
parallelism. It owns its cursor, budgets, visit limit, early exit, and result merge. The runtime records the calls
that the host made, learns their route evidence, and builds the envelope.

The FokosDB host implements its current `walkRangeChildren` behavior in `walk`. It carries `QueryPageBudget` and
the cursor in its request. It uses each visit interval to build a boundary cursor. `fokosExecuteLocal` uses
`clip` against the caller slice and calls only `local`.

A broad range response returns evidence for every served leaf interval. The caller learns all of those intervals.
A later broad request can therefore leap directly to the learned leaves.

#### 4.2.11 Repartition state machine and arbitration

The state machines, the arbitration rules, the plan persistence, the retry delays, the fair due-row selection,
and the paginated status are those of `docs/agent-plans/2026-09-17-unified-repartition-flow.md` sections 4.1
to 4.7, 4.9, and 4.12. This section records only what changes.

**Source side**, `fokos_repartitions.state`: `queued → planned → cutover → completed → cleaned`.

| Request                  | Acceptance rule                                                                 |
| ------------------------ | ------------------------------------------------------------------------------- |
| Queue `hash_split`       | No split row exists, and no promotion is `queued`, `planned`, or `cutover`      |
| Queue `range_split`      | The source is a range partition, and no split row exists                        |
| Queue `key_promotion`    | The source is a hash partition, no split row exists, and the key has no override|
| Initialize the first target | `beforeCutover` returned true or is undefined; otherwise the targets stay `pending` |
| Cut over a promotion     | No split row exists, its target is `initialized`, and `beforeCutover` returned true |
| Cut over a split         | Every target is `initialized`, and `beforeCutover` returned true or is undefined |

**Target side**, KV `__fokos/import`: `awaiting_data → importing → imported → active`.

What changes against the shipped flow:

- `RepartitionSource` and `RepartitionTarget` take `FokosShardingStore` instead of `PartitionStore`. The FokosDB
  values they read through their deps (`computeRangeBoundaries`, `lockCountForKey`, `cleanupStep`,
  `onSplitCompleted`) become the hooks `computeRangeBoundaries`, `beforeCutover`, `cleanupSourceStep`, and
  `beforeComplete`.
- The wire types in `repartition-types.ts` drop the type-only imports of `do-partition.ts`.
  `FokosExecuteLocalRequest` becomes `{ op: string; repartitionId: string; caller: FokosPartitionRef; request:
  unknown }`, and the source finds the operation in its registry.
- Queue writes the plan head and the `queued` row in one transaction. Planning fills the head and writes all
  targets. Cutover retains the plan. Completion and cleanup reconstruct the hook plan from KV and SQL. The final
  cleanup deletes the plan chain before it writes `cleaned`.
- Signals arrive from `afterLocalSuccess`, `requestSplitEvaluation`, and `requestPromotion`. For
  `evaluateSplit: true`, the runtime first calls `hooks.evaluateSplit`. A false result stops there. An accepted
  split or a promotion candidate becomes a queue attempt.
- `repartitionUnblocked: true` is the shipped `onLockReleased`: one indexed check for a row that `beforeCutover`
  held back, then `markPromotionsDueNow` in one `transactionSync` and the fast path. A partition with no held
  row pays the check and nothing else. FokosDB returns it from `txCommit` and `txCancel` after a local success.
- A synchronous arbitration precheck rejects an ineligible queue attempt without an alarm write. For a possible
  new row, or an unfinished row that still needs work, the runtime moves the fallback alarm earlier before it
  opens the queue transaction. The transaction repeats arbitration and writes the decision. A failed fallback
  write creates no row.
- After a queue succeeds, the runtime schedules `source_repartition` on the fast path. A post-write signal
  failure is logged and does not change the completed item-operation result.
- `debugForcePromoteKey` calls `runtime.requestPromotion`, which routes to the owner (section 4.2.14). It
  returns `queued` and `state` for `queued: true` and `already_promoted`, and throws `partition_over_size` for
  `split_in_progress`, as it does today.

#### 4.2.12 Migration protocol and read-through

The protocol is that of the shipped flow: one `fokosMigrationPull` RPC, an `overrides` phase that the runtime
owns and a `host` phase behind an opaque cursor, one page in flight, the page and its cursor committed in one
transaction, `imported` persisted before the acknowledgement, and the target's own fallback alarm. The request
carries no budget. The runtime bounds the overrides page with its own constants, and the host bounds every host
page in `buildPage` (section 2.3).

```ts
type FokosImportRecord = {
	/** 2, not 1: `source` is a ref here and a full context in the shipped record. */
	schema: 2;
	state: "awaiting_data" | "importing" | "imported" | "active";
	repartitionId: string;
	source: FokosPartitionRef;
	slice: FokosSlice;
	cursor: FokosMigrationCursor | null;
	attempts: number;
	nextAttemptAt: number;
	updatedAt: number;
};
```

`source` becomes a `FokosPartitionRef`: the target reaches the source with `stub(routeContext(), source.doName)`,
so it needs no stored remote context.

**Read-through.** A `read_source` operation on a target in `awaiting_data` or `importing` calls
`source.fokosExecuteLocal({ op, request, caller, repartitionId })`. The source:

1. Seeks the target row by `(repartitionId, caller.partitionId)` and validates the whole caller identity. An
   unknown caller gets `repartition_target_unknown`.
2. Rejects `queued` and `planned` with `repartition_not_cut_over`. Rejects a promotion in `completed` or
   `cleaned` with `repartition_slice_reclaimed`.
3. Finds the operation by name. It must exist and be `readOnly`; otherwise `sharding_operation_invalid`.
4. Extracts the scope with the descriptor (`key`, `items`, or `range`) and tests it with `belongsToTarget` of the
   caller slice. A key outside the slice throws `partition_misrouted`. A range request is clipped to the slice
   with `clip`; a cursor outside the clipped interval throws `partition_misrouted`.
5. For a hash key that a terminal override moved, resolves the owner (section 4.2.8 step 2), forwards with
   `forward`, and returns that envelope.
6. Otherwise calls `local` without owner resolution, the lifecycle gate, `admit`, `walk`, or
   `afterLocalSuccess`. It returns the local point or range evidence from the source.

For an ordinary source read, the target replaces `summary` with its own route node. It also replaces
`servedBy`, `hashDepth`, `rangeDepth`, and `_hint` in each evidence entry with its own values. It adds one to the
envelope-level `forwardCount`. When the source followed an override, the target keeps the served partition and
`_hint` in `summary` and in each evidence entry. It replaces only `hashDepth` and `rangeDepth` with its own
values. The caller then learns the promotion and range boundaries.

#### 4.2.13 Background scheduler

The runtime owns the Durable Object alarm. The host delegates `alarm()` and must not call `setAlarm`.

```ts
type FokosJob = {
	name: string;
	/** False skips the job in this pass. Synchronous. */
	canRun(): boolean;
	/** One bounded, idempotent step. Can be async: it runs outside any transaction. Section 4.2.17 gives its write rule. */
	runStep(): { nextRunAt: number | null } | Promise<{ nextRunAt: number | null }>;
	/**
	 * The earliest time this job has durable work, read from the host's own storage, or null. Synchronous.
	 * The pass reads it at its end so the alarm covers work that no request signalled, for example a lock
	 * that a restart left behind, or the earliest TTL. The pass ignores it while `canRun()` is false, so
	 * a job that cannot run on this partition does not keep the alarm firing.
	 */
	deadline?(): number | null;
};
```

A host job reaches the alarm in two ways. A request that creates durable work returns a `jobs` signal from
`afterLocalSuccess` (section 4.2.6), and `dispatch` step 7 persists that deadline and arms the alarm before the
request returns; FokosDB does this after `prepare` so stale-transaction recovery runs even when the coordinator
never returns. A pass reads `deadline()` of every runnable job at its end, so durable work that no request
signalled still gets an alarm.

A host job writes its own storage directly. Section 4.2.17 states when it needs `runtime.owns(key)` or
`dispatch` instead.

Built-in jobs run first, in this order: `target_import`, `target_ack`, `source_repartition`, `source_cleanup`.
Host jobs follow in registration order. FokosDB registers stale-transaction recovery here; TTL expiry stays on
its own in-memory timer (section 4.2.18). One pass runs at most `runtimeConfig().importPagesPerPass` sequential
import steps. The default is 16, and the runtime rejects a value below 1.

One pass:

1. Check the destroy fence. A fenced pass runs nothing and arms nothing.
2. Read the durable import, source, job, and host deadlines. Evaluate each `canRun()` function.
3. When no durable work exists, return without changing the alarm.
4. When work exists but none is due, move the alarm to the earliest deadline only if it is missing or later.
   Then return.
5. Before the first state change or RPC, move the alarm earlier to `now + fallbackAlarmMs`. Do not write when an
   earlier alarm already exists. An error from this fallback write escapes the alarm handler.
6. For each due job whose `canRun()` returned true, run `runStep()`. Catch its error, log it, and set its next
   run to `now + fallbackAlarmMs`. One failing job never stops another.
7. Write the result of each job in one `transactionSync` right after its step, not in one batch at the end of
   the pass: read the current `__fokos/jobs`, replace this job's `nextRunAt`, and write the record. A `runStep`
   awaits, and a request can call `scheduleJob` for another job during that await. A batch write at the end
   would put the values read at step 2 over that request's write and lose its earlier deadline. The write
   right after the step reads the record again, so it cannot lose a value that another path wrote. A step that
   returns the value the record already holds writes nothing.
8. Set the alarm to the earliest durable deadline: the earliest `nextRunAt`, the import deadline, the source
   deadline, and the `deadline()` of each job whose `canRun()` returned true in step 2. This write replaces the
   fallback and can move the alarm later. Delete the fallback when no durable deadline remains. A job with
   `canRun() === false` contributes nothing here: a router between cutover and completion holds pending lock
   rows it cannot sweep, and its stale-recovery `deadline()` is in the past; if the pass read it, the alarm would
   fire at once, run nothing, and re-arm at once until completion.

The pass is `runtime.runDueWork(info?)`. It catches job errors. An alarm-storage error can escape so Cloudflare
retries the alarm. The fast path is an in-memory timer of `fastPathDelayMs` (default 50 ms) that calls the same
pass. The runtime keeps one in-flight pass promise. A fast-path request or an alarm that arrives during a pass
waits for it, then runs one more pass. Two passes never interleave. A request path and the pre-pass fallback must
not replace an earlier deadline with a later one.

A pass with N runnable due jobs writes `__fokos/jobs` up to N times, arms the fallback once, and writes the
final alarm once. Section 4.3 names a write budget for the scheduler as future work: the design above is
correct first, and a pass that runs often should cost few storage writes.

`runtime.scheduleJob(name, runAt)` moves `nextRunAt` of one job earlier only, inside one `transactionSync` that
reads the record first. It persists the new deadline and moves the alarm earlier only when necessary.

#### 4.2.14 Control-plane RPC surface

The host implements this interface by delegation. These are the only RPC methods the runtime calls on a peer.

```ts
interface FokosShardingRpc {
	fokosInit(req: FokosInitRequest): Promise<void>;
	fokosStartImport(req: FokosStartImportRequest): Promise<void>;
	fokosMigrationPull(req: FokosMigrationPullRequest): Promise<FokosMigrationPage>;
	fokosMigrationAck(req: FokosMigrationAckRequest): Promise<void>;
	fokosExecuteLocal(req: FokosExecuteLocalRequest): Promise<FokosEnvelope<unknown>>;
	/** Queue a key promotion on the partition that owns the key now. Forwards through routers like a point operation. */
	fokosRequestPromotion(req: FokosRequestPromotionRequest): Promise<FokosRequestPromotionResult>;
	fokosStatus(req: FokosStatusRequest): Promise<FokosStatusPage>;
	fokosPrepareDestroy(req: FokosPrepareDestroyRequest): Promise<void>;
	/** Cancel the schedule, delete all storage, abort. The caller traverses the status pages first. */
	fokosDestroy(): Promise<void>;
	alarm(info: AlarmInvocationInfo): Promise<void>;
}
```

`fokosInit`, `fokosStartImport`, `fokosMigrationPull`, `fokosMigrationAck`, `fokosStatus`, and
`fokosPrepareDestroy` keep the request types and the rules of the shipped flow, with `FokosInitRequest.target`
as a `FokosRouteContext` and `FokosInitRequest.source` as a `FokosPartitionRef`. `fokosDestroy` replaces
`destroyPartition`.

`fokosRequestPromotion` is new. Its request is `{ target: FokosPartitionRef; hashKey: KeyBytes; data?: unknown }`.
The receiver validates `target` against its stored identity as dispatch step 1 validates a route context, and
throws `partition_context_mismatch` on a difference; an uninitialized receiver throws the initialization error
of its kind. The runtime then resolves the owner of `{ hashKey, sortKey: empty }` as a point operation with the
Bloom step skipped. A route override in any state, or a range owner, means the key is promoted or on its way:
the result is `{ queued: false, reason: "already_promoted", state, owner }`, where `state` is the state of that
promotion row. A remote hash owner receives `fokosRequestPromotion` with its own ref as `target` and answers for
itself. An importing owner throws `partition_migrating`. A local owner runs arbitration and answers
`queued: true`, or `reason: "split_in_progress"` when a split row refused it.

#### 4.2.15 Worker-side router

```ts
class FokosRouter<TPolicy> {
	/** Cheap to construct. A Worker can build one per request with a tenant-specific topology and policy. */
	constructor(topology: FokosTopology, rangeConfig: FokosRangeConfig, policy: TPolicy);
	rootContext(hashKey: KeyBytes): FokosRouteContext<TPolicy>;
	allRoots(): FokosRouteContext<TPolicy>[];
	unwrap<T>(envelope: FokosEnvelope<T>): { value: T; routing: FokosPublicRouting };
	/**
	 * The destroy traversal. For every root, and then post-order for every target: call `fokosPrepareDestroy`
	 * (with the root context on a root only), read every `fokosStatus` page after the fence is set, visit each
	 * target, then call `visit` on the partition. Dedupes by `doName` over the whole traversal.
	 */
	walk(
		stub: (ctx: FokosRouteContext<TPolicy>, doName: string) => DurableObjectStub,
		visit: (ctx: FokosRouteContext<TPolicy>, stub: DurableObjectStub) => Promise<void>,
	): Promise<void>;
}

type FokosPublicRoute = Omit<FokosRouteNode, "_hint">;
type FokosPublicRouting = {
	summary: FokosPublicRoute;
	routes: FokosPublicRoute[];
	forwardCount: number;
};
```

The router hashes the hash key to a root index with the function in `router.ts` and builds the root route
context. It caches nothing. The `walk` stub callback must use the same jurisdiction-aware helper as the runtime.
`FokosDB` builds the router from its table configuration. `traverseForDestroy` becomes a `walk` whose `visit`
calls `fokosDestroy`. The fence comes first, so a source cannot add a target after the traversal reads its last
page.

#### 4.2.16 Errors

`src/sharding/errors.ts` extends the base of `src/shared/errors.ts` with `defineCodes`, in the same way that
`src/shared/errors-operations.ts` does. It holds the codes that only partitions exchange:

- `partition_context_mismatch` is a `FokosInternalError`. The route context disagrees with stored identity.
- `partition_misrouted` is a `FokosRoutingError`. The key cannot belong to the partition or caller slice.
- `hash_partition_not_initialized` is a `FokosRoutingError`. A non-root hash partition has no identity. A caller
  that used a cache invalidates that hint and falls back.
- `range_partition_not_initialized` is a `FokosRoutingError`. A range partition has no identity. A speculative
  caller falls back.
- `partition_fanout_failed` is a `FokosInternalError`. An `attempt_all` group had a failed remote group.
- `repartition_not_cut_over` is a retryable `FokosUnavailableError`. The source still owns the slice.
- `repartition_unknown` is a `FokosInternalError`. The pull, acknowledgement, or start names no repartition.
- `repartition_target_unknown` is a `FokosInternalError`. The caller is not a repartition target.
- `repartition_slice_reclaimed` is a `FokosInternalError`. The source gave the promoted rows back.
- `sharding_local_must_be_sync` is a `FokosInternalError`. A synchronous `local` handler returned a thenable.
- `sharding_operation_invalid` is a `FokosInternalError`. A descriptor is inconsistent, or
  `fokosExecuteLocal` named an unknown or non-`readOnly` operation.

The existing codes move from `shared/errors.ts` with their values unchanged. `hash_partition_not_initialized`
is new. `partition_migrating` and `partition_over_size` stay in `shared/errors.ts`, because the client matches
them. `withFokosErrors` keeps its mapping of `repartition_not_cut_over` to `partition_migrating`. Host errors,
including admission rejections, pass through unchanged.

#### 4.2.17 Concurrency

A Durable Object runs one JavaScript thread, but `await` points interleave requests, alarms, and the fast-path
pass. The runtime keeps correctness with three rules:

1. Every durable transition is one `transactionSync` with no `await` inside. Hooks that run inside are
   synchronous by contract.
2. By default, the `local` handler of a `point`, `group`, `single_owner`, or `range` operation is synchronous.
   For every shape, `dispatch` has no `await` between owner resolution and the local call: routing, admission,
   `beforeForward`, and the local work run in one synchronous block, and the remote calls start after it
   (section 4.2.7 step 5). The runtime throws `sharding_local_must_be_sync` when a `localMode: "sync"` handler
   returns a thenable. A synchronous handler has no yield point. A cutover cannot interleave between the
   ownership decision and the write, on an owner or on a router that forwards the rest of a group. A read cannot
   return the source copy after a target accepted a write.
3. One scheduler pass at a time. Overlapping requests coalesce into one more pass.

**Asynchronous handlers.** An operation with `localMode: "async"` can await inside `local`, for example to read
an item and then send to a Queue, write to R2, or call an external service. Routing does not change: the runtime
resolves the owner synchronously, forwards to a remote owner, and otherwise calls `local` on this partition. The
lifecycle gate, admission, the envelope, learning, and `afterLocalSuccess` (after the promise settles) all apply.
Only the thenable check is skipped. A cutover can commit while the handler is suspended, so the host must follow
the write rule after any `await`:

- The handler writes partitioned data through `runtime.dispatch(writeOp, runtime.routeContext(), req)`.
  `dispatch` resolves the owner again, and a key that moved during the `await` is forwarded to its new owner.
- Or the handler calls `runtime.owns(key)` and performs the write in the same synchronous block, with no `await`
  between the check and the write. A `false` answer means the key moved; the handler then uses `dispatch` or
  returns a retryable error.

A read after an `await` can be stale under the same rule as the read-through. The host decides whether the
operation accepts that. A side effect outside the partition has no owner and needs no check. Local route evidence
names the partition that ran the handler, even when a write inside it was forwarded. When the partition becomes
a router during the `await`, the runtime drops an `evaluateSplit` signal because a router has nothing to split.
It routes `promotionCandidates` through `requestPromotion`, which forwards to the owner.

**Host writes outside `dispatch`.** The host owns its storage and writes it directly. The runtime gives it two
checks, and the host needs one of them only when the write can land on a partition that no longer owns the key:

- A write that a request names by route key, and that can therefore need forwarding, goes through `dispatch`.
  FokosDB stale-transaction recovery awaits the coordinator first and then calls `dispatch` for commit and
  cancel, because those items can have moved to a child.
- A write that is keyed by something else, or that touches rows the partition selects itself, runs directly.
  The host tests `runtime.lifecycle()` or `runtime.owns(key)` once, in the same synchronous block as the write,
  when the outcome depends on ownership. A `false` answer means the key moved; the host then uses `dispatch` or
  returns a retryable error.
- A write whose result does not depend on ownership needs no check. The FokosDB TTL sweep is one: it deletes
  expired rows with one bounded statement, its `canRun` is false on a router and on an importing target, and a
  stale copy on a promotion source is reclaimed by `cleanupSourceStep` in any case, so deleting it early changes
  nothing.

A `local` shape handler and a `runStep` follow these rules. Neither resolves an owner.

Remote fan-out inside a group operation runs with `Promise.all` for `fail_fast` and `Promise.allSettled` for
`attempt_all`. Migration pulls are sequential per target with one page in flight.

`ctx.blockConcurrencyWhile` is used in the constructor only. It does not wait for an in-flight request, so it
cannot close the cutover race.

#### 4.2.18 FokosDB host mapping

The FokosDB host maps current mechanisms as follows:

- `ensurePartitionContext`, `ensureMigration`, and `#rpc` become dispatch steps 1 and 2.
- `withSplitForwarding` becomes the `point` shape.
- `groupItemsByRouting` becomes the `group` shape. Prepare and read use `fail_fast`. Commit and cancel use
  `attempt_all`.
- The unconditional `cancelLocal(transactionId)` at the top of `txCancel` becomes its `beforeForward`, so a
  router releases its own pre-cutover lock rows before it forwards (section 4.2.6). Its `local` is empty.
- `wakeLockBlockedPromotion` in `txCommit` and `txCancel` becomes the `repartitionUnblocked` signal.
- The alarm after an accepted `prepareLocal` becomes a `stale_tx_recovery` job signal.
- `routeSingleDestination` becomes the `single_owner` shape.
- `walkRangeChildren` becomes the host `walk` callback of the `range` shape. It owns `QueryPageBudget` and the
  cursor.
- Read-through in `apiGetItem` and `apiQueryItems` uses `whileMigrating: "read_source"` and `readOnly: true`.
- The four `OperationIntent` values become the same four `admissionTag` values. `hooks.admit` keeps the 110% rule.
- Split checks and promotion candidates move to `afterLocalSuccess` signals.
- The flow's `lockCountForKey` becomes `beforeCutover` with `pendingLockCountForHashKey(hk) === 0`, consulted
  before the first target initialization and at cutover, as the flow consults it today.
- The flow's `cleanupStep` becomes `cleanupSourceStep` for `key_promotion`. Splits keep their rows.
- The flow's `onSplitCompleted` becomes `beforeComplete` for hash and range splits.
- `computeRangeSplitBoundaries` becomes `computeRangeBoundaries`.
- `FokosDbMigrationHost` becomes `hooks.migration` without a behavior change.
- `range_hierarchy` becomes runtime-owned `fokos_range_hierarchy`.
- Stale transaction recovery becomes a host job. Its `canRun` checks the role and import state.
- `TtlExpiry` stays host code and keeps its behavior: an in-memory timer that each FokosDB RPC method arms
  before it calls `dispatch`, one bounded `deleteExpiredItems` statement per chunk with no per-row ownership
  check (section 4.2.17), and `canSweep` read from `runtime.lifecycle()`. It registers no runtime job and arms
  no alarm, so an idle partition is not woken to expire rows, as today. The runtime forbids `setAlarm` only; a
  host timer is allowed. A later change can register it as a job with a `deadline()` when punctual expiry on
  idle partitions is wanted.
- `debugForcePromoteKey` and `fokosRequestPromotion` keep today's answer for a key whose promotion is `queued`
  or `planned`: `queued: false` with that state, not `partition_over_size`.
- Asynchronous local closures become synchronous `local` functions.
- Route hints move from `meta` and `partitionMetas` to `routing.summary` and `routing.routes`. Metrics stay in
  `value`. `db.ts` combines them.
- `fokosStaleTransactionMs`, `fokosGetColoInfo`, and `fokosTtlConfig` remain host methods.
- `fokosImportPagesPerPass` becomes `runtimeConfig().importPagesPerPass`.
- `debugForcePromoteKey` calls `runtime.requestPromotion`.
- `destroyPartition` and `traverseForDestroy` become `fokosDestroy` and `FokosRouter.walk`.

The coordinator stores the root `FokosRouteContext` per participant, as it stores the context today. It reads
`policy.nsTx` and `policy.ns` for its bindings. The FokosDB host validates both values before it selects a stub.
The coordinator pool uses the shard-group name `fokos.tc.<shardGroup>`.

#### 4.2.19 Deployment and rollback

There is no compatibility with partitions that the current code created. The runtime does not read
`__partition_context`, `__partition_depth`, `__topo_cache`, `__partial_range_topology`, or `range_hierarchy`.
There is no converter. A deployment of the new code starts with fresh Durable Object namespaces or fresh shard
groups.

The coordinator group changes from `fokos_tc.<tableName>` to `fokos.tc.<shardGroup>`. A retry with the same
`clientRequestToken` must not cross the old and new coordinator namespaces. Such a retry can run the transaction
a second time because the new coordinator has no old idempotency record.

The deployed Worker must use compatibility date `2026-04-21` or later. Alternatively, all RPC providers and
consumers must enable `enhanced_error_serialization`.

Rollback is a code revert together with a return to the old namespaces. Data written to the new namespaces is
not readable by the old code.

#### 4.2.20 Testing

- The example host lives in `packages/fokosdb/test/sharding/`. It imports nothing from FokosDB. The build guard
  of milestones M1 and M3 fails when the sharding entry reaches a FokosDB module.
- Unit tests for the codec, the caches, arbitration, and both state machines move with their modules in M1 and
  keep their cases.
- An ownership property test checks that `belongsToTarget(key)` equals `resolveOwner(key).target` for every target
  of a plan, for random keys, for hash and range plans.
- A frontier property test checks that visits do not overlap and that their union equals the requested interval.
  It covers empty, partial, full, stale, and evicted range caches in both directions.
- Range integration tests warm all leaves with one broad request and prove that the next request bypasses the
  range root. A partial cache test proves that cache gaps use the base target without a gap or duplicate. A
  learned partition that split again must plan and forward to its current leaves.
- Route-list tests cover one point, grouped points at different depths, and one range request across three leaves.
  They check `summary`, exact point and range scopes, leaf identities, internal hints, and total `forwardCount`.
  A grouped request with many keys on one leaf yields one entry with many scopes and one `_hint`. A request
  whose evidence crosses `ROUTE_EVIDENCE_MAX_BYTES` returns `routesTruncated: true`, a list under the cap, and
  an intact `summary`.
- A `group` test proves the synchronous order of section 4.2.7 step 5: a cutover that becomes durable while the
  remote groups are in flight finds the local lock already written on the node that owned the key at
  resolution time, and never on a node that resolved after the cutover.
- A `beforeForward` test proves that `txCancel` on a router between cutover and completion deletes the router's
  own pending rows and forwards to every child.
- A `repartitionUnblocked` test proves that a promotion held by a lock cuts over on the pass right after the
  cancel, not after the flat lock interval. A companion test proves that a locked key gets no range root until
  `beforeCutover` returns true.
- Scheduler tests prove that a `scheduleJob` call during an async `runStep` survives the pass, and that a router
  with pending rows and a stale-recovery `deadline()` in the past re-arms no alarm for that job.
- Repartition tests crash after queue and prove that the queue-time policy and data survive. They prove that
  cutover retains the plan, completion and cleanup receive the same snapshot, `nextKey` starts as null, and final
  cleanup deletes the plan chain. A maximum-shape plan-head test stays below the 2 MB KV limit and proves that
  target references and slices exist only in SQL.
- Scheduler tests prove that an idle pass does not write an alarm, a productive pass arms a fallback before its
  first transition, and an alarm-storage error escapes the handler.
- Stub tests prove that normal forwarding and `FokosRouter.walk` apply the jurisdiction and location hint.
- Integration tests in the Workers runtime also cover a hash split with a crash after `fokosInit`; a target crash
  after `imported`; an acknowledgement from a non-member; Bloom false positives on an uninitialized root and an
  `awaiting_data` root; a cached jump to an uninitialized hash child; a promotion queued while a split is
  `queued`; a range walk during import; and a `local` traversal that visits every node of a tree.
- A `point` operation whose `localMode: "sync"` handler returns a promise fails with `sharding_local_must_be_sync`.
- A `localMode: "async"` handler whose key moves to a child during its `await` writes through `dispatch` and the
  write lands on the new owner; the same handler with `owns(key)` sees `false` after the cutover.
- The FokosDB suites in `packages/fokosdb/test/` pass after M4. Assertions that go through the `FokosDB` client
  keep their codes. Assertions that call a `PartitionDO` stub directly unwrap the envelope; nothing else in them
  changes. `TestPartition`, `triggerHashSplit`, and `withMigrationHeld` are rewritten over the runtime state.

### 4.3 Future extensions

These hooks are not in this document's scope. Each is named with its purpose so that the core design leaves
room for it. None has a design yet.

- **Request observation.** A hook before and after every incoming `dispatch`, without the request or response
  payload, that cannot change the result. For latency and per-operation metrics.
- **Runtime events.** A stream from the runtime to the host for lifecycle events: bootstrapped, initialized as
  target, repartition queued, cutover, completed, cleaned, import completed, import acknowledged. The host
  decides what to record.
- **Destroy hook.** A call before `fokosDestroy` deletes storage, so the host can stop its own resources.
- **Log parameters.** A host callback that adds fields to every runtime log line.
- **Split-row cache.** An in-memory copy of this partition's split row and its targets, so owner resolution
  saves one SQL seek per request. The shipped flow and this document read SQL on purpose: the value that
  matters is absence, and a stale "no split row" makes a router serve rows its targets own. A cache needs a
  clear-on-write rule for every transaction that touches `fokos_repartitions` or `fokos_repartition_targets`
  (clear, never set, so a rolled-back transaction leaves no wrong value), and a test that proves eviction and
  staleness change nothing.
- **Scheduler write budget.** A pass writes `__fokos/jobs` once per job step and the alarm twice. A partition
  that runs passes often, for example during a long import, pays those writes on every pass. A later change can
  fold the job records into one row per job, skip the fallback write when the pass has no state change ahead,
  or batch the writes with a merge that keeps the rule of section 4.2.13 step 7.

## 5. Alternative options

**Base Durable Object class.** Rejected as the primary model because many hosts already extend another base
class, and JavaScript has single inheritance. A base class can wrap the runtime later.

**Class decorator or RPC dispatcher.** Rejected because the shapes differ per operation and the decorator would
need the same descriptor data; the explicit `dispatch` call is one line and carries the descriptor name.

**Topology-only extraction.** Move only the topology modules into a package. Rejected because the sharding code
depends on FokosDB rows for migration, boundaries, promotion, and cleanup. Without the hook boundary the package
cannot run a split.

**Build the runtime beside `PartitionDO` and swap at the end.** Rejected because the repartition flow, the
caches, and the codec exist; a second copy would be written and then one copy deleted. Moving in place writes
each line once.

**A lease around asynchronous local handlers.** Rejected. No FokosDB handler needs to await between owner
resolution and its write, and a shared lease with writer preference deadlocks on re-entrant `dispatch`. A
synchronous handler has no yield point and needs no guard.

**A host-computed range frontier.** Rejected because interval coverage, learned descendants, and speculative
fallback are runtime topology rules. A host-computed plan can create a gap or route every request through the
range root. The selected `range` shape makes the runtime compute the frontier and makes the host walk it.

**A separate npm package.** Deferred. A new entry of the `fokosdb` package shares `errors.ts`, the codec, and
the build. The directory boundary makes a later move a build-configuration change. The generic `src/shared/`
modules on the allow list of M1 can move to a shared top-level package at the same time.

## 6. Frequently asked questions

**Why does the host register operations instead of passing closures per call?**
The runtime must run the host's local handler on a source partition when a target reads through during import,
and the default `forward` must know the method name. It needs the handler by name. A registry also gives one
place for the shape, the admission tag, and the signals.

**Why is `local` synchronous by default?**
A synchronous handler has no yield point, so a cutover cannot interleave between the ownership decision and the
write. The runtime enforces the default, so an accidental `async` fails on the first call instead of leaving a
race. A host that must await opts in with `localMode: "async"` and closes the race itself with `dispatch` or
`owns`, as section 4.2.17 states.

**Why is `applyPage` synchronous?**
The runtime commits the page and the cursor in one `transactionSync`. SQLite and KV in Durable Objects are
synchronous, so a host import needs no `await`. Atomic commit removes the case where the page is applied and the
cursor is lost.

**Why does every request still carry topology and policy?**
A Durable Object has no other way to learn them. The constructor receives no parameters, and a library host
does not know at build time how many shard groups exist or which binding name the user chose. The cost is a few
integers and one small object per request.

**Can a stale policy overwrite a newer one?**
Yes, as today. The policy is last-writer-wins, and every caller writes it: a Worker, a forwarding partition,
and a coordinator that replays the context it stored when the transaction started. A coordinator alarm that
recovers an old transaction can therefore write an older policy over a newer one, and the next Worker request
writes the newer one back. Each flip costs one KV write and can queue a split at the older threshold. The
shipped `ensurePartitionContext` behaves the same way. A rule such as "only a Worker request updates the
policy" is not in this document.

**Why is the policy opaque instead of a typed set of thresholds?**
Different hosts need different policy. FokosDB needs two split condition sets and two binding keys. Another host
needs a row count or a tenant tier. The runtime only needs to store, compare, and forward the value.

**Why does the runtime plan a range frontier while the host walks it?**
The runtime owns interval coverage, topology caches, and speculative fallback. The host owns its request type,
budgets, cursor, visit limit, early exit, and result merge. This split lets a warm caller leap directly to range
leaves without putting host semantics in the runtime.

**How does a host with no range partitions use the runtime?**
It omits `computeRangeBoundaries`, omits `caches.promotionBloom`, and never returns `promotionCandidates`. Only
hash splits happen. The `range` shape is unused.

**Can a host write its own traversal?**
Yes. It registers a `local` shape operation whose handler reads `children()`, forwards the same operation with
`forward` to the children it selects, and merges the envelopes. It never creates a stub.

## 7. References

- `docs/ideas/fokos-sharding/2026-09-09-fokos-partition-runtime.md`
- `docs/agent-plans/2026-09-17-unified-repartition-flow.md`
- `packages/fokosdb/src/server/do-partition.ts`
- `packages/fokosdb/src/server/do-transaction-coordinator.ts`
- `packages/fokosdb/src/client/db.ts`
- `packages/fokosdb/src/shared/partition/repartition/repartition-flow.ts`
- `packages/fokosdb/src/shared/partition/repartition/repartition-types.ts`
- `packages/fokosdb/src/shared/partition/fokos-migration-host.ts`
- `packages/fokosdb/src/shared/partition-topology/partition-context.ts`
- `packages/fokosdb/src/shared/partition-topology/partition-id.ts`
- `packages/fokosdb/src/shared/partition-topology/split-policy.ts`
- `packages/fokosdb/src/shared/errors.ts`
- `packages/fokosdb/src/shared/errors-operations.ts`
- `packages/fokosdb/src/shared/do-stubs.ts`
- `packages/fokosdb/tsdown.config.ts`
- [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
- [Durable Objects Alarms API](https://developers.cloudflare.com/durable-objects/api/alarms/)
- [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Workers RPC](https://developers.cloudflare.com/workers/runtime-apis/rpc/)
- [Workers RPC error handling](https://developers.cloudflare.com/workers/runtime-apis/rpc/error-handling/)
