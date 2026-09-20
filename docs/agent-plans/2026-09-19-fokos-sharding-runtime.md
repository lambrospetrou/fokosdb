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
- The five operation shapes in use today are supported: `point`, `group`, `single_owner`, `scan`, and `local`.
- The hash-leaf ownership check, the exhaustive topology compare, the response envelope on every operation, and
  a row bound on the learned range hierarchy are added.
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
- Workers RPC preserves the `name`, the `message`, and the serializable own properties of an error and drops its
  prototype ([RPC error handling](https://developers.cloudflare.com/workers/runtime-apis/rpc/error-handling/)).
  Every runtime error must be a `FokosError` and must be matched with `FokosError.isCode`, never with
  `instanceof`.
- The host cannot receive constructor parameters. Topology and host policy travel with every request. A root
  hash partition bootstraps from its first request. Every other partition is created by `fokosInit` only.
- The runtime must not read a field of the host policy. It stores, compares, and forwards it as an opaque value.
- `rootTreesN` must be 1 to 65,000. `hashSplitN` and `rangeSplitN` must be 2 to 255. A shard group name must be
  non-empty and must not contain `.`.
- `rootTreesN` and `hashSplitN` must not change for a shard group that exists.
- Each source step must call at most `REPARTITION_RPC_CONCURRENCY` (6) targets.
- A migration page must hold at most 1,000 data rows and must scan at most 10,000 source rows.
- One target step must pull and commit at most one page. One pass runs up to `fokosImportPagesPerPass()` steps.

## 3. Milestones

The work is done in place. Each stage moves existing code and adapts it, so no code is written twice. Each stage
builds, type checks, and keeps the whole test suite green.

### M1 — Move the sharding code into `src/sharding/` and add the entry

Deliverables:

- The directory `packages/fokosdb/src/sharding/` with the entry `packages/fokosdb/src/sharding/index.ts`,
  exported as `fokosdb/sharding` in `package.json` and `tsdown.config.ts`.
- These modules move into it with their tests, with `git mv` and import path updates only:
  `shared/partition-topology/*`, `shared/partition/repartition/*`, `shared/partition/batch-scan.ts`,
  `shared/query/sk-interval.ts`, `shared/bloom-filter.ts`, `shared/hash-primitives.ts`, and the partition half
  of `shared/do-stubs.ts`. `txCoordinatorStub` and `txCoordinatorNamespace` stay in `shared/`. `sk-interval.ts`
  moves because `repartition-slice.ts` needs it, and `query/cursor.ts` imports it from its new home.
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
  FokosDB. It runs a hash split, a range split, a key promotion, and a `scan`.
- The property test of section 4.2.20.
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
`resolveOwner`, `children`, and `forward`. A host that needs a traversal that no shape offers writes it as a
recursive `local` operation over those calls.

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
			// The host owns the binding, the jurisdiction, and the location hint.
			stub: (routeCtx, doName) => env[routeCtx.policy.ns].getByName(doName),
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
	schema: 1;
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

| Part                    | Lifetime                   | Who sets it             | Who reads it                              |
| ----------------------- | -------------------------- | ----------------------- | ----------------------------------------- |
| `partitionId`, `doName` | Immutable                  | Router or source        | Runtime: identity check, child derivation |
| `topology`              | Immutable per shard group  | Worker at first request | Runtime: fan-out, names, ownership        |
| `rangeConfig`           | Mutable, last writer wins  | Worker on every request | Runtime: range split planning only        |
| `policy`                | Mutable, last writer wins  | Worker on every request | Host hooks only, through `runtime.policy()` |

Only `topology` is frozen. A field that the runtime reads only when it plans a repartition is mutable, so an
operator can change it without an outage. The plan that reads it snapshots it.

FokosDB maps its `PartitionContext` as follows. `tableName` becomes `topology.shardGroup`. `rootTreesN`,
`hashSplitN`, and `jurisdiction` become `topology`. `rangeSplitN` and `rangeAncestorsConfig` become
`rangeConfig`. `ns`, `nsTx`, `hashSplitConditions`, `rangeSplitConditions`, and `locationHint` become the
FokosDB `policy`. `primaryDoIdStr` is dropped: the name is deterministic and `idFromName` recreates the ID.

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
   the first valid route context. A range partition without a stored identity throws
   `range_partition_not_initialized`; only `fokosInit` creates it.

A target created by `fokosInit` receives its full route context from the source: the source derives the child
`partitionId` and `doName`, copies its own `topology`, and copies its stored `rangeConfig` and `policy`.

The runtime validates the topology bounds of section 2.3 at bootstrap and on every `fokosInit`, and the range
config bounds on every write of `__fokos/policy`. A violation throws `partition_context_options_invalid`.

**Runtime options**, a constructor argument:

```ts
type FokosRuntimeOptions<TPolicy> = {
	/** A stub for one partition of the host's own class. The host applies the binding, the jurisdiction, and the location hint. */
	stub(ctx: FokosRouteContext<TPolicy>, doName: string): DurableObjectStub;
	caches?: {
		hashArenaBytes?: number;
		rangeHierarchyMaxRows?: number;
		promotionBloom?: { expectedKeys: number; falsePositiveRate: number };
	};
	scheduler?: { fallbackAlarmMs?: number; fastPathDelayMs?: number };
};
```

When the runtime needs a stub outside a request, for example in a background job, it calls `stub` with its own
stored route context.

#### 4.2.3 Persisted state owned by the runtime

| State                | Location                            | Content                                                           |
| -------------------- | ----------------------------------- | ----------------------------------------------------------------- |
| Identity             | KV `__fokos/identity`               | `FokosPartitionIdentity`                                          |
| Policy               | KV `__fokos/policy`                 | `{ rangeConfig, policy }`                                         |
| Import record        | KV `__fokos/import`                 | `FokosImportRecord` (section 4.2.12)                              |
| Plan                 | KV `__fokos/repartition/<id>/plan`  | The immutable plan of a `planned` repartition; deleted at cutover |
| Destroy fence        | KV `__fokos/destroying`             | `true` after `fokosPrepareDestroy`                                |
| Job schedule         | KV `__fokos/jobs`                   | `{ [jobName]: { nextRunAt } }`                                    |
| Hash arena cache     | KV `__fokos/cache/hash_arena`       | `HashTopologySnapshot`, byte-bounded                              |
| Promotion Bloom cache| KV `__fokos/cache/promotion_bloom`  | `PartialRangeTopologySnapshot`, byte-bounded                      |
| Schema version       | KV `__fokos/schema_version`         | The last sharding migration that ran                              |
| Repartitions         | SQL `fokos_repartitions`            | One row per repartition, as shipped                               |
| Repartition targets  | SQL `fokos_repartition_targets`     | One row per target, as shipped                                    |
| Route overrides      | SQL `fokos_route_overrides`         | `hash_key` → repartition id, as shipped                           |
| Range hierarchy      | SQL `fokos_range_hierarchy`         | Learned descendant boundaries, row-bounded                        |

The three repartition tables keep the schema of `docs/agent-plans/2026-09-17-unified-repartition-flow.md`
section 4.2. `fokos_range_hierarchy` keeps the columns of `range_hierarchy` and adds `learned_at INTEGER NOT
NULL` for eviction order.

`FokosShardingStore` in `src/sharding/sharding-store.ts` owns every statement over these tables and keys. It
runs its migrations in the runtime constructor inside `blockConcurrencyWhile`, before the host runs its own
migrations, and records the last migration in `__fokos/schema_version`. The host's migration runner must not
name a `fokos_` table.

The runtime loads the identity, the policy, and the import record into memory in its constructor. It caches the
split row of this partition and its at most 255 targets after the first read. It does not load repartition rows
or route overrides and keeps no copy of them: a point override lookup is one indexed seek that joins
`fokos_route_overrides` to its repartition row. A stale negative answer on an in-memory copy would make a router
serve rows its targets own, so the lookup stays in SQL.

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
	/** The stored host policy. Throws `range_partition_not_initialized` before bootstrap. */
	policy(): TPolicy;
	/** The stored route context of this partition. */
	routeContext(): FokosRouteContext<TPolicy>;
	lifecycle(): FokosLifecycle;
	/**
	 * True when this partition owns the key now. Reads the topology and the route overrides only, never a
	 * cache, so a Bloom false positive cannot make a host sweep skip a key it owns.
	 */
	owns(key: RouteKey): boolean;
	/** The routing answer, caches included. A `speculative` remote owner is a hint, not a fact. */
	resolveOwner(key: RouteKey): FokosOwner;
	/** This router's targets in `target_index` order with their intervals. Empty on an owner. */
	children(): ScanChild[];
	/** Forward one registered operation to one target: derive the route context, call the stub, learn the caches, count the hop. */
	forward<Res>(target: FokosPartitionRef, op: string, req: unknown): Promise<FokosEnvelope<Res>>;

	// ─── signals and jobs ───
	requestSplitEvaluation(): void;
	/** Routes to the current owner of the key first (section 4.2.14), then queues there. */
	requestPromotion(hashKey: KeyBytes, data?: unknown): Promise<FokosRequestPromotionResult>;
	scheduleJob(name: string, runAt: number): void;
	/** One background pass. `alarm(info)` calls this and nothing else. */
	runDueWork(info?: AlarmInvocationInfo): Promise<void>;
}

type ScanChild = { ref: FokosPartitionRef; start: KeyBytes | null; end: KeyBytes | null };

type FokosLifecycle = {
	role: "owner" | "router";
	import: null | { state: "awaiting_data" | "importing" | "imported" | "active" };
	activeRepartition: null | { id: string; kind: RepartitionKind; state: "queued" | "planned" | "cutover" };
};
```

`forward` is the only way a host reaches another partition. The topology is a tree and every node runs the same
class, so any traversal is a recursive operation: the host registers an operation with `shape: "local"`, and its
handler does its local part, reads `children()`, forwards the same operation to the children it selects, and
merges the envelopes. Pre-order, post-order, parallel or sequential visits, early exit, and a different clip per
child are host code. The `scan` shape is that recursion with the ordered interval walk written once.

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
	 * Source side. Runs inside the cutover transactionSync. Return false to keep the plan `planned` and
	 * retry. FokosDB returns `pendingLockCountForHashKey(hk) === 0` for a promotion. Synchronous.
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

	/** Host background jobs. Section 4.2.13. */
	jobs?: FokosJob[];
}

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
	/** Host jobs that must run by a deadline because of this result. FokosDB arms stale-transaction recovery after `prepare`. */
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
			shape: "scan";
			whileMigrating: "read_source";
			readOnly: true;
			scan(req: Req): { hashKey: KeyBytes; start: KeyBytes | null; end: KeyBytes | null; descending: boolean };
			/** Pure. False drops a child that cannot contribute, for example one behind the resume cursor. Default: true. */
			include?(req: Req, child: ScanChild): boolean;
			/** The request for one child, built from the remaining request right before the forward. Section 4.2.10. */
			clip(remaining: Req, child: ScanChild): Req;
			fold(acc: Res | null, part: Res, remaining: Req, ctx: { child: ScanChild; hasLaterChild: boolean }):
				{ acc: Res; remaining: Req | null };
		})
	| {
			shape: "local";
			/** Can be async: a `local` shape has no owner resolution to race against. */
			local(req: unknown): unknown | Promise<unknown>;
		};
```

| Shape          | Owner resolution                             | Behavior                                                                         |
| -------------- | -------------------------------------------- | -------------------------------------------------------------------------------- |
| `point`        | One key                                      | Local, or forward to one partition. Learns caches from the envelope.             |
| `group`        | Every item                                   | Local group plus one group per remote partition. Host merges.                    |
| `single_owner` | Every item                                   | Exactly one destination. Otherwise return the host's `notApplicable` value.      |
| `scan`         | Hash key to a leaf, then ordered range visit | Visits the kept range children in order until `fold` stops.                      |
| `local`        | None                                         | Never forwarded, never gated. Used by `fokosExecuteLocal`, by admin, and by host traversals. |

When the items of a `single_owner` operation span more than one partition, the runtime returns the value the
host names as `notApplicable` instead of an error. Today that value is `{ outcome: "not_applicable" }` on
`SingleShotResponse` and `ReadSnapshotResponse`. It has no side effects and passes unchanged through every
forwarding hop, so the caller runs its multi-partition path. On a split table it is the ordinary answer for
such a set, so it is not an error.

A `group` operation on a router has an empty local group. The host `merge` receives zero or more parts. When
`items(req)` returns an empty list, the runtime runs `local(req)` on this partition, owner or router, with no
remote group: `txCancel` with no items means "release the lock of this transaction here", and a router in the
middle of a split holds locks too.

#### 4.2.7 The dispatch pipeline

`dispatch` runs these steps in this order.

1. **Identity.** Validate the identity and the topology, store a changed policy, or bootstrap a root
   (section 4.2.2).
2. **Lifecycle gate.** When `import.state` is `awaiting_data` or `importing`, the runtime first schedules
   `target_import` on the fast path and moves the fallback alarm to `now + fallbackAlarmMs` when that is earlier.
   Then, in both states:
   - `whileMigrating: "read_source"` → run owner resolution step 1 for the keys of the request and throw
     `partition_misrouted` on a miss. Then call `source.fokosExecuteLocal({ op, request, caller: selfRef,
     repartitionId })`, add one to `forwardCount`, and return (section 4.2.12). The source answers
     `repartition_not_cut_over` from its own row while it is `queued` or `planned`.
   - `whileMigrating: "retry"` → throw `partition_migrating`.
   The states `imported` and `active` pass the gate.
3. **Owner resolution** for every key (section 4.2.8). Group the items by destination.
4. **Admission.** When a local group exists, call `hooks.admit`. A rejection throws the host error unchanged.
5. **Execution.** Run the local group and the remote groups per the shape. Remote groups run in parallel. There
   is no `await` between step 3 and the local call.
6. **Learning.** For each successful remote envelope, learn the route (section 4.2.9) and add one to
   `forwardCount`.
7. **Signals.** When the local group succeeded and the descriptor has `afterLocalSuccess`, collect the signals and
   apply them: the repartition signals per section 4.2.11, and each `jobs` entry through `scheduleJob`, which
   persists the deadline and awaits `setAlarm`. An error here is logged and does not change the result.
8. **Envelope.** Return the result wrapped for a local result, or the merged remote envelope.

When `__fokos/destroying` is true, step 1 throws `partition_migrating` for every operation. `fokosStatus`,
`fokosPrepareDestroy`, and `fokosDestroy` stay available.

#### 4.2.8 Owner resolution

One function resolves a route key to an owner. Every shape uses it, and `runtime.resolveOwner` exposes it.

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
3. **Promotion Bloom cache.** When the filter reports a probable promotion by a descendant, the owner is the range
   root, `speculative: true`. Point and scan shapes use this step. Group and single-owner shapes skip it.
4. **Topology.** When this partition is a router, pick the child by the hash function at this depth, then apply
   the hash arena cache to jump deeper. Otherwise the owner is local.

Resolution order on a range partition:

1. **Ownership.** The hash key must equal `identity.range.hashKey`, and the sort key must be inside
   `[start, end)`. Otherwise `out_of_range`.
2. When this partition is a router, pick the child whose interval contains the sort key, then apply the range
   hierarchy cache to jump deeper. Otherwise the owner is local.

A speculative forward that fails with `range_partition_not_initialized` or `repartition_not_cut_over` resolves
again with the Bloom step disabled. The `repartition_not_cut_over` fallback applies to reads only; a write
throws it unchanged. Any other error propagates.

`out_of_range` throws `partition_misrouted`. It is a routing defect, not backpressure.

`runtime.owns(key)` runs hash steps 1, 2, and 4, or range steps 1 and 2, and skips the two caches. It answers
`true` only for `{ kind: "local" }`. A host job that sweeps its rows relies on it, and a Bloom false positive
must not make the sweep skip a key this partition owns.

The `belongsToTarget` predicate of `buildPage` is the same function as hash step 4 for hash children and range
step 2 for range children, with one addition for hash children: a key with a terminal route override returns
`false`. One implementation, two callers.

#### 4.2.9 Response envelope

The runtime wraps every result. Application response types do not carry routing fields.

```ts
type FokosEnvelope<T> = {
	value: T;
	route: {
		servedBy: FokosPartitionRef;
		servedByActorId: string;
		hashDepth: number;
		rangeDepth: number;
		forwardCount: number;
		/** Internal. Bounded ancestor boundaries of a range leaf. Consumers must drop it. */
		_hint?: { rangeAncestors: RangeAncestorInfo[] };
	};
};
```

A forwarding partition keeps `servedBy`, `hashDepth`, `rangeDepth`, and `_hint` from the child envelope and adds
one to `forwardCount`. A `forward` callback must return the envelope of the remote call unchanged.

An error follows the same rule. The partition that raises an error attaches its `route` as an own data property
of the error; own properties cross the RPC hop. A forwarding partition learns from `error.route`, adds one to
`forwardCount`, and rethrows the same error object. A partition without an identity attaches nothing.

Every operation returns an envelope, including the transaction operations. `TransactionCoordinatorDO` unwraps
the envelope of `txPrepare`, `txCommit`, and `txCancel` where it calls `partitionStubByName`. `db.ts` unwraps
the envelope of every partition call with `FokosRouter.unwrap` and maps `route` to its public `PartitionInfo`.
`partitionMetas` stays a FokosDB value inside `value`. This is what makes transaction fan-out learn routes.

#### 4.2.10 Route caches and the scan walk

Caches are hints. A miss, a full cache, a stale entry, or a disabled cache changes latency only. Ownership is
decided by the route override table and the topology, never by a cache.

| Cache            | Storage                             | Learns from                                                | Bound                              |
| ---------------- | ----------------------------------- | ---------------------------------------------------------- | ---------------------------------- |
| Hash arena       | KV `__fokos/cache/hash_arena`       | `route.hashDepth` of a forwarded envelope                  | `caches.hashArenaBytes`, depth cap |
| Range hierarchy  | SQL `fokos_range_hierarchy`         | `route._hint.rangeAncestors`                               | `caches.rangeHierarchyMaxRows`     |
| Promotion Bloom  | KV `__fokos/cache/promotion_bloom`  | A hash partition forwarded and `servedBy` is a range partition | filter size, no removal        |

The range hierarchy table holds learned rows only. A learn writes or refreshes `learned_at`. When full, the
runtime evicts the rows with the oldest `learned_at`, deepest first. A partition's own ancestors are in its
identity, so eviction cannot change `route._hint.rangeAncestors`. The default of `rangeHierarchyMaxRows` is
10,000. A row holds two boundary keys and a hash key, so with keys of about 1 KB each the table stays near
10 MB at the bound. The cache implementations move from `hash-topology.ts` and `partial-range-topology.ts` with
their tests and sit behind one internal contract: `lookup(key)`, `learn(key, envelope)`, `invalidate(key)`.

**The scan walk.** The runtime keeps the loop, the interval intersection, and the ordering. The host takes every
decision on its own types through `include`, `clip`, and `fold`:

1. On a hash partition, `scan` resolves the hash key as a `point` with a sentinel sort key: override, Bloom, and
   topology steps apply. A hash leaf runs `local`.
2. On a range owner, `scan` runs `local`.
3. On a range router, the runtime intersects its ordered children with the `scan` interval, orders them by
   `descending`, and calls `include(req, child)` for every candidate before the first forward. A `false` result
   drops the child. `include` is pure and reads the original request, so the runtime knows `hasLaterChild` exactly
   for every kept child.
4. The runtime walks the kept children in order. Right before each forward it calls `clip(remaining, child)`,
   where `remaining` is the original request for the first child and the `remaining` that the previous `fold`
   returned after that. The host carries its budgets and its cursor inside `remaining`, so each child receives
   what is left, not what the caller asked for.
5. After each forward the runtime calls `fold(acc, part, remaining, ctx)`. `remaining: null` stops the walk.
   `ctx.child` carries the interval, so the host can build a boundary cursor. `ctx.hasLaterChild` lets the host
   emit a cursor only when a later child can contribute.

Cursor skipping is host code in `include`; cursor placement and budget accounting are host code in `clip` and
`fold`. The FokosDB host writes `walkRangeChildren` with them, and the read-through clip of `fokosExecuteLocal`
with `clip` against the caller slice.

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
- Signals arrive from `afterLocalSuccess`, `requestSplitEvaluation`, and `requestPromotion`. The runtime handles a
  signal in the request that produced it, after the result is fixed: `evaluateSplit: true` calls
  `hooks.evaluateSplit` and queues the split when accepted; each
  `promotionCandidates` entry queues a `key_promotion` when arbitration accepts it; then the runtime schedules
  `source_repartition` on the fast path and sets the fallback alarm. The durable queue write and the `setAlarm`
  call are awaited. A failure in this step is logged and the request still returns its result.
- `debugForcePromoteKey` calls `runtime.requestPromotion`, which routes to the owner (section 4.2.14). It
  returns `queued` and `state` for `queued: true` and `already_promoted`, and throws `partition_over_size` for
  `split_in_progress`, as it does today.

#### 4.2.12 Migration protocol and read-through

The protocol is that of the shipped flow: one `fokosMigrationPull` RPC, an `overrides` phase that the runtime
owns and a `host` phase behind an opaque cursor, one page in flight, the page and its cursor committed in one
transaction, `imported` persisted before the acknowledgement, and the target's own fallback alarm. The source
owns the page budget of 20 MiB as one constant; the request carries none.

```ts
type FokosImportRecord = {
	schema: 1;
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
4. Extracts the keys with the descriptor (`key`, `items`, or `scan`) and tests each with `belongsToTarget` of the
   caller slice. A key outside the slice throws `partition_misrouted`. A `scan` request is clipped to the slice
   with `clip`; a cursor outside the clipped interval throws `partition_misrouted`.
5. For a hash key that a terminal override moved, resolves the owner (section 4.2.8 step 2), forwards with
   `forward`, and returns that envelope.
6. Otherwise runs `local` without owner resolution, without the lifecycle gate, without `admit`, and without
   `afterLocalSuccess`, and returns the result with the source's own `route`.

The target replaces `servedBy`, `hashDepth`, and `rangeDepth` with its own values, replaces `_hint` with its own
ancestors, and adds one to `forwardCount`. An answer that the source forwarded to an override owner is the one
exception: the target keeps `servedBy` and `_hint` and replaces the two depths only, so the caller learns the
promotion and the range boundaries.

#### 4.2.13 Background scheduler

The runtime owns the Durable Object alarm. The host delegates `alarm()` and must not call `setAlarm`.

```ts
type FokosJob = {
	name: string;
	/** False skips the job in this pass. Synchronous. */
	canRun(): boolean;
	/** One bounded, idempotent step. Can be async: it runs outside any transaction and calls `dispatch` for writes. */
	runStep(): { nextRunAt: number | null } | Promise<{ nextRunAt: number | null }>;
	/**
	 * The earliest time this job has durable work, read from the host's own storage, or null. Synchronous.
	 * The pass reads it at its end so the alarm covers work that no request signalled, for example a lock
	 * that a restart left behind, or the earliest TTL.
	 */
	deadline?(): number | null;
};
```

A host job reaches the alarm in two ways. A request that creates durable work returns a `jobs` signal from
`afterLocalSuccess` (section 4.2.6), and `dispatch` step 7 persists that deadline and arms the alarm before the
request returns; FokosDB does this after `prepare` so stale-transaction recovery runs even when the coordinator
never returns. A pass reads `deadline()` of every job at its end, so durable work that no request signalled
still gets an alarm.

A host job that mutates partitioned data must check `runtime.owns(key)` before it writes, because a promotion
source keeps some keys and gives others away.

Built-in jobs run first, in this order: `target_import`, `target_ack`, `source_repartition`, `source_cleanup`.
Host jobs follow in registration order. FokosDB registers stale-transaction recovery and TTL expiry here and
removes the timer of `TtlExpiry`.

One pass:

1. Check the destroy fence. A fenced pass runs nothing and arms nothing.
2. Arm the fallback alarm at `now + fallbackAlarmMs` before any state change or RPC.
3. For each job with `canRun()`, run `runStep()`. Catch its error, log it, and set its next run to
   `now + fallbackAlarmMs`. One failing job never stops another.
4. Persist `__fokos/jobs` with every `nextRunAt`.
5. Set the alarm to the earliest durable deadline: the earliest `nextRunAt`, the import deadline, the source
   deadline, and the `deadline()` of every host job. This write replaces the fallback and can move the alarm
   later.

The pass is `runtime.runDueWork(info?)`. It never throws. The fast path is an in-memory timer of
`fastPathDelayMs` (default 50 ms) that calls the same pass. The runtime keeps one in-flight pass promise. A
fast-path request or an alarm that arrives during a pass waits for it, then runs one more pass. Two passes never
interleave. A request path and the pre-pass fallback never replace an earlier deadline with a later one.

`runtime.scheduleJob(name, runAt)` moves `nextRunAt` of one job earlier only, persists, and re-arms the alarm when
the earliest deadline moved.

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
	fokosRequestPromotion(req: { hashKey: KeyBytes; data?: unknown }): Promise<FokosRequestPromotionResult>;
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

`fokosRequestPromotion` is new. The runtime resolves the owner of `{ hashKey, sortKey: empty }` as a point
operation with the Bloom step skipped. A route override or a range owner means the key is promoted: the result
is `{ queued: false, reason: "already_promoted", state, owner }`. A remote hash owner receives
`fokosRequestPromotion` and answers for itself. An importing owner throws `partition_migrating`. A local owner
runs arbitration and answers `queued: true`, or `reason: "split_in_progress"` when a split row refused it.

#### 4.2.15 Worker-side router

```ts
class FokosRouter<TPolicy> {
	/** Cheap to construct. A Worker can build one per request with a tenant-specific topology and policy. */
	constructor(topology: FokosTopology, rangeConfig: FokosRangeConfig, policy: TPolicy);
	rootContext(hashKey: KeyBytes): FokosRouteContext<TPolicy>;
	allRoots(): FokosRouteContext<TPolicy>[];
	unwrap<T>(envelope: FokosEnvelope<T>): { value: T; route: FokosPublicRoute };
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

/** The route of an envelope without `_hint`. */
type FokosPublicRoute = Omit<FokosEnvelope<unknown>["route"], "_hint">;
```

The router hashes the hash key to a root index with the function in `router.ts` and builds the root route
context. It caches nothing. `FokosDB` builds it from its table configuration, and `traverseForDestroy` becomes a
`walk` whose `visit` calls `fokosDestroy`. The fence comes first so that a source cannot add a target after the
traversal read its last page.

#### 4.2.16 Errors

`src/sharding/errors.ts` extends the base of `src/shared/errors.ts` with `defineCodes`, in the same way that
`src/shared/errors-operations.ts` does. It holds the codes that only partitions exchange:

| Code                                        | Class                    | Meaning                                                        |
| ------------------------------------------- | ------------------------ | -------------------------------------------------------------- |
| `partition_context_mismatch`                | `FokosInternalError`     | The route context disagrees with the stored identity.          |
| `partition_misrouted`                       | `FokosRoutingError`      | The key cannot belong to this partition, or is outside a slice.|
| `range_partition_not_initialized`           | `FokosRoutingError`      | A range partition has no identity. Speculative callers fall back. |
| `partition_fanout_failed`                   | `FokosInternalError`     | An `attempt_all` group had a failed remote group.              |
| `repartition_not_cut_over`                  | `FokosUnavailableError`  | The source still owns the slice. Retryable.                    |
| `repartition_unknown`                       | `FokosInternalError`     | Pull, ack, or start for an unknown repartition.                |
| `repartition_target_unknown`                | `FokosInternalError`     | The caller is not a member of the repartition.                 |
| `repartition_slice_reclaimed`               | `FokosInternalError`     | The source already gave the promoted rows back.                |
| `sharding_local_must_be_sync`               | `FokosInternalError`     | A `local` handler returned a thenable. Host defect.            |
| `sharding_operation_invalid`                | `FokosInternalError`     | A descriptor is inconsistent, or `fokosExecuteLocal` named an unknown or non-`readOnly` operation. |

The codes move from `shared/errors.ts` with their values unchanged. `partition_migrating` and
`partition_over_size` stay in `shared/errors.ts`, because the client matches them. `withFokosErrors` keeps its
mapping of `repartition_not_cut_over` to `partition_migrating`. Host errors, including admission rejections, pass
through unchanged.

#### 4.2.17 Concurrency

A Durable Object runs one JavaScript thread, but `await` points interleave requests, alarms, and the fast-path
pass. The runtime keeps correctness with three rules:

1. Every durable transition is one `transactionSync` with no `await` inside. Hooks that run inside are
   synchronous by contract.
2. By default, the `local` handler of a `point`, `group`, `single_owner`, or `scan` operation is synchronous,
   and `dispatch` has no `await` between owner resolution and the local call. The runtime throws
   `sharding_local_must_be_sync` when a `localMode: "sync"` handler returns a thenable. A synchronous handler has
   no yield point, so a cutover cannot interleave between the ownership decision and the write, and a read cannot
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

A read after an `await` can be stale under the same rule as the read-through: the host decides whether the
operation accepts that. A side effect outside the partition has no owner and needs no check. `servedBy` in the
envelope names the partition that ran the handler, even when a write inside it was forwarded. When the partition
became a router during the `await`, the runtime drops an `evaluateSplit` signal from `afterLocalSuccess`, because
a router has nothing to split, and routes `promotionCandidates` through `requestPromotion`, which forwards to the
owner.

A `local` shape handler and a `runStep` follow the same write rule. Neither resolves an owner, and each write
they make goes through `dispatch`. FokosDB stale-transaction recovery awaits the coordinator first and then calls
`dispatch` for commit and cancel.

Remote fan-out inside a group operation runs with `Promise.all` for `fail_fast` and `Promise.allSettled` for
`attempt_all`. Migration pulls are sequential per target with one page in flight.

`ctx.blockConcurrencyWhile` is used in the constructor only. It does not wait for an in-flight request, so it
cannot close the cutover race.

#### 4.2.18 FokosDB host mapping

| Today in `PartitionDO`                                          | With the runtime                                                                       |
| --------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `ensurePartitionContext`, `ensureMigration`, `#rpc`             | `dispatch` steps 1 and 2                                                               |
| `withSplitForwarding`                                           | `point` shape                                                                          |
| `groupItemsByRouting` for prepare, commit, cancel, read         | `group` shape; prepare and read use `fail_fast`, commit and cancel use `attempt_all`   |
| `await ensureAlarmSet(...)` after an accepted `prepareLocal`    | `afterLocalSuccess` returns `jobs: [{ name: "stale_tx_recovery", runAt }]`             |
| `routeSingleDestination` for snapshot read and single-shot      | `single_owner` shape                                                                   |
| `walkRangeChildren` in `apiQueryItems`                          | `scan` shape with `clip` and `fold` over `QueryPageBudget` and the cursor              |
| Read-through in `apiGetItem` and `apiQueryItems`                | `whileMigrating: "read_source"` with `readOnly: true`                                  |
| `OperationIntent` read, write, delete, ignore_size_reject       | `admissionTag` with the same four values; `hooks.admit` keeps the 110% rule            |
| `checkSplits`, `queuePromotionIfOverThreshold`, `drainPromotionCandidates` | `afterLocalSuccess` returns `{ evaluateSplit, promotionCandidates }`      |
| `lockCountForKey` dep of the flow                               | `beforeCutover` returns `pendingLockCountForHashKey(hk) === 0`                         |
| `cleanupStep` dep of the flow                                   | `cleanupSourceStep` for `key_promotion`; undefined for splits keeps their rows         |
| `onSplitCompleted` dep of the flow                              | `beforeComplete` for `hash_split` and `range_split`                                    |
| `computeRangeSplitBoundaries` in `PartitionStore`               | `computeRangeBoundaries`                                                               |
| `FokosDbMigrationHost` in `fokos-migration-host.ts`             | `hooks.migration`, unchanged                                                           |
| `range_hierarchy` table                                         | `fokos_range_hierarchy`, owned by the runtime                                          |
| `recoverStaleTransactions` in the alarm                         | Host job; `canRun` checks role and import state                                        |
| `TtlExpiry` with its own timer                                  | Host job; checks `runtime.owns(key)` per row                                           |
| `async` local closures                                          | Synchronous `local` functions                                                          |
| `meta` and `partitionMetas` with `_internal`                    | `FokosEnvelope.route`; `partitionMetas` stays inside `value`                           |
| `fokosStaleTransactionMs`, `fokosGetColoInfo`, `fokosTtlConfig`, `fokosImportPagesPerPass` | Unchanged host methods                                      |
| `debugForcePromoteKey` via `routeSingleDestination`             | `runtime.requestPromotion`                                                             |
| `destroyPartition`, `traverseForDestroy`                        | `fokosDestroy`, `FokosRouter.walk`                                                     |

The coordinator stores the root `FokosRouteContext` per participant, as it stores the context today. It reads
`policy.nsTx` and `policy.ns` for its bindings.

#### 4.2.19 Deployment and rollback

There is no compatibility with partitions that the current code created. The runtime does not read
`__partition_context`, `__partition_depth`, `__topo_cache`, `__partial_range_topology`, or `range_hierarchy`,
and there is no converter. A deployment of the new code starts with fresh Durable Object namespaces or fresh
shard groups.

Rollback is a revert of the code together with a return to the old namespaces. Data written to the new
namespaces is not readable by the old code.

#### 4.2.20 Testing

- The example host lives in `packages/fokosdb/test/sharding/`. It imports nothing from FokosDB. The build guard
  of milestones M1 and M3 fails when the sharding entry reaches a FokosDB module.
- Unit tests for the codec, the caches, arbitration, and both state machines move with their modules in M1 and
  keep their cases.
- A property test checks that `belongsToTarget(key)` equals `resolveOwner(key).target` for every target of a
  plan, for random keys, for hash and range plans.
- Integration tests in the Workers runtime drive with the example host: a hash split with a crash after
  `fokosInit`; a target crash after `imported`; an acknowledgement from a non-member; a Bloom false positive on an
  uninitialized root and on an `awaiting_data` root; a promotion queued while a split is `queued`; a `scan`
  across three range children during import; a `local` shape traversal that visits every node of a tree.
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

**A host-driven visit plan for `scan`.** The host receives the child list and returns the visits. Rejected
because the interval intersection and the ordering are runtime knowledge that every host would repeat; `clip`
returning `null` and `fold` receiving the child interval give the same freedom.

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

**Why is the policy opaque instead of a typed set of thresholds?**
Different hosts need different policy. FokosDB needs two split condition sets and two binding keys. Another host
needs a row count or a tenant tier. The runtime only needs to store, compare, and forward the value.

**How does a host with no range partitions use the runtime?**
It omits `computeRangeBoundaries`, omits `caches.promotionBloom`, and never returns `promotionCandidates`. Only
hash splits happen. The `scan` shape is unused.

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
