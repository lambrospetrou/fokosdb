# RFC — FokosPartitionRuntime: a composable sharding runtime for any Durable Object

**State:** Draft
**Date:** 2026-09-09
**Author:** Lambros Petrou

**Status:** Nothing is built. This document defines the abstraction. The existing behavior it must preserve is
recorded in `docs/ideas/fokos-sharding/gptsol-existing-behavior.md` and
`docs/ideas/fokos-sharding/gemini-existing-flows-spec.md`.

---

## 1. Table of Contents

- [1. Table of Contents](#1-table-of-contents)
- [2. Overview and Context](#2-overview-and-context)
- [3. Goals and Requirements](#3-goals-and-requirements)
- [4. Milestones](#4-milestones)
- [5. Proposed Solution](#5-proposed-solution)
  - [5.1 High-level overview](#51-high-level-overview)
  - [5.2 Technical details](#52-technical-details)
    - [5.2.1 Integration model](#521-integration-model)
    - [5.2.2 Identity, configuration, and the request reference](#522-identity-configuration-and-the-request-reference)
    - [5.2.3 Persisted state owned by the runtime](#523-persisted-state-owned-by-the-runtime)
    - [5.2.4 Runtime construction and hooks](#524-runtime-construction-and-hooks)
    - [5.2.5 Operation descriptors](#525-operation-descriptors)
    - [5.2.6 The dispatch pipeline](#526-the-dispatch-pipeline)
    - [5.2.7 Owner resolution](#527-owner-resolution)
    - [5.2.8 Response envelope](#528-response-envelope)
    - [5.2.9 Route caches](#529-route-caches)
    - [5.2.10 Repartition state machine](#5210-repartition-state-machine)
    - [5.2.11 Arbitration between repartitions](#5211-arbitration-between-repartitions)
    - [5.2.12 Migration protocol](#5212-migration-protocol)
    - [5.2.13 Read-through during import](#5213-read-through-during-import)
    - [5.2.14 Background scheduler](#5214-background-scheduler)
    - [5.2.15 Control-plane RPC surface](#5215-control-plane-rpc-surface)
    - [5.2.16 Worker-side router](#5216-worker-side-router)
    - [5.2.17 Errors](#5217-errors)
    - [5.2.18 Invariants and their mechanisms](#5218-invariants-and-their-mechanisms)
    - [5.2.19 Failure and recovery](#5219-failure-and-recovery)
    - [5.2.20 Concurrency](#5220-concurrency)
    - [5.2.21 Performance](#5221-performance)
    - [5.2.22 FokosDB adapter mapping](#5222-fokosdb-adapter-mapping)
    - [5.2.23 Deployment and rollback](#5223-deployment-and-rollback)
    - [5.2.24 Testing](#5224-testing)
  - [5.3 Open Questions](#53-open-questions)
- [6. Alternative Options](#6-alternative-options)
- [7. Frequently Asked Questions](#7-frequently-asked-questions)
- [8. References](#8-references)

---

## 2. Overview and Context

FokosDB stores items in `PartitionDO` (`packages/fokosdb/src/server/do-partition.ts`). One class does two jobs.
It is the FokosDB data partition: items, conditions, TTL, and the 2PC participant. It is also the sharding
runtime for that partition: identity, routing, topology caches, splits, key promotion, migration, and alarms.

The two jobs are mixed by hand in every RPC method. Each method repeats `ensurePartitionContext`,
`ensureMigration`, and `withSplitForwarding` or `groupItemsByRouting`. The sharding code reads FokosDB SQL rows:
items, pending transactions, deletion watermarks, key-size estimates, promoted keys, and the range hierarchy.
The audit in `gptsol-existing-behavior.md` counts 26 RPC methods that each carry this boilerplate, and lists 17
limitations that come from the mixed design (section 24 of that document).

The sharding part is valuable on its own. Any Durable Object that must grow past one object needs the same
things: deterministic identity, routing that survives splits, a durable cutover, resumable data migration,
and one alarm that drives many jobs. Today none of this is reusable. A refactor of `PartitionDO` alone does
not fix that, because the boundary between the two jobs does not exist in the code.

This document defines `FokosPartitionRuntime`. It is an object that any Durable Object class creates in its
constructor. The class gives the runtime a small set of callbacks and delegates a fixed set of `fokos`-prefixed
RPC methods to it. The runtime owns every topology transition and every forwarding decision. The class keeps
its storage, its RPC surface, and its data semantics.

### Glossary

| Term             | Meaning in this document                                                                           |
| ---------------- | -------------------------------------------------------------------------------------------------- |
| runtime          | One `FokosPartitionRuntime` instance inside one Durable Object.                                    |
| host             | The Durable Object class that creates the runtime and implements its hooks.                        |
| partition        | One Durable Object that takes part in a shard group. It is a hash partition or a range partition.  |
| shard group      | All partitions that share one root set and one `FokosTopology`. Former `tableName`.                |
| route key        | `{ hashKey, sortKey }` as `Uint8Array`. The runtime routes on these two values only.               |
| repartition      | One durable plan that moves ownership from a source partition to one or more target partitions.    |
| source, target   | The partition that gives ownership, and a partition that receives it, inside one repartition.      |
| cutover          | The durable write on the source after which new requests for the moved ownership go to targets.    |
| page             | One bounded, opaque unit of migration data that the host exports and imports.                      |

---

## 3. Goals and Requirements

### In scope

- A host class creates one runtime in its constructor and implements `FokosPartitionHooks`. No base class is
  needed. A host that extends another base class, for example the Agents SDK, can use the runtime.
- The runtime owns identity, owner resolution, forwarding order, caches, repartition state, migration
  scheduling, acknowledgements, and the Durable Object alarm. The host cannot bypass a transition.
- The host owns its storage schema, its RPC types, its local operation semantics, its admission policy, its
  split policy, its migration data, and its own background jobs.
- The runtime does not import or know any FokosDB type: no item row, no pending transaction, no watermark, no
  key-size estimate, no TTL, no coordinator ID.
- The five operation shapes in use today are supported: point, grouped fan-out, single-owner, ordered range scan,
  and local-only control.
- Every invariant in section 17 of `gptsol-existing-behavior.md` holds. Section 5.2.18 lists each one with its
  mechanism.
- The limitations in section 24 of the same audit that are control-plane defects are fixed, not preserved:
  acknowledgement crash gap (24.5), non-durable import start (24.6), promotion and split queue window (24.7),
  acknowledgement membership (24.15), overlapping background passes (24.12).
- All RPC methods the host must expose for the runtime are prefixed with `fokos`.
- FokosDB `PartitionDO` is rewritten as a host of the runtime and passes its existing test suites.

### Out of scope

- A base Durable Object class. It can come later as sugar over the runtime. Nothing in this design needs it.
- Changes to the FokosDB public client API or to the transaction coordinator protocol.
- A pluggable ownership strategy in the public API. The runtime ships the hash tree and the range tree as
  built-in strategies behind one internal contract. Making that contract public is a later decision.
- A Worker-side cache of the live split tree. The Worker still enters through a root partition.
- A live topology keeper Durable Object. The design stays decentralized.

### Requirements

- The runtime uses `ctx.storage.kv` and `ctx.storage.sql` of the host. It writes only keys under the prefix
  `__fokos/` and tables under the prefix `fokos_`. The host must not read or write those.
- Every state transition of the runtime is one `ctx.storage.transactionSync` call. In-memory mirrors update
  inside the same call.
- One serialized RPC message must stay under 32 MiB
  ([Workers RPC limits](https://developers.cloudflare.com/workers/runtime-apis/rpc/#limitations)). The default
  migration page budget is 20 MiB, the value in use today.
- One KV key and value together must stay under 2 MB
  ([Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)). Every KV
  snapshot the runtime writes has a byte budget below that.
- Each Durable Object has one alarm
  ([Alarms API](https://developers.cloudflare.com/durable-objects/api/alarms/)). The runtime owns it. The host
  must delegate `alarm()` and must not call `setAlarm` itself.
- Durable Object RPC carries only an error's message across the boundary, not its class or properties. Every
  runtime error is recognizable from its message.
- The host cannot receive constructor parameters through RPC. Shard group topology and host policy travel with
  every request, as today, so a Worker can choose them at run time per shard group and per tenant. Root hash
  partitions bootstrap from the first request. Range partitions and non-root hash partitions are created only
  by `fokosInit`.
- The runtime must not read any field of the host policy. It stores and forwards it as an opaque value.

---

## 4. Milestones

Each milestone is a separate change that builds and passes tests on its own.

1. **Foundation.** Package skeleton, `FokosRouteContext`, `FokosTopology`, partition ID codec (moved from
   `partition-id.ts`), errors, envelope, and the Worker-side `FokosRouter`. Unit tests only.
2. **Dispatch.** Runtime constructor, identity bootstrap, lifecycle gate, owner resolution, the point,
   group, single-owner, and local-execute shapes, and the built-in caches moved from `hash-topology.ts`,
   `partial-range-topology.ts`, and the `range_hierarchy` logic. Tested with a small example host that stores one
   SQL table and has no FokosDB dependency.
3. **Repartition.** The repartition state machine, migration loop, scheduler, and the control-plane RPCs.
   Hash split first, then range split, then key promotion. The example host gains export and import hooks.
4. **Scan.** The ordered range traversal shape.
5. **FokosDB host.** `PartitionDO` becomes a host. `PartitionStore` keeps the FokosDB tables and drops the
   sharding tables. The suites in section 25 of `gptsol-existing-behavior.md` pass. Old code is deleted.

---

## 5. Proposed Solution

### 5.1 High-level overview

The host class creates a runtime, tells it how to reach other partitions, and gives it callbacks. Every public
RPC method of the host becomes one call to `runtime.dispatch`. The runtime validates identity, applies the
lifecycle gate, resolves the owner of each key, runs the host's local handler or forwards to another partition,
learns caches, and returns an envelope around the host's result.

```
                Worker                                     Durable Object (host)
  ┌──────────────────────────────┐            ┌───────────────────────────────────────────────┐
  │ FokosRouter                  │            │ class MyDO extends DurableObject               │
  │  rootContext(hashKey) ───────┼── RPC ────►│   fokos = new FokosPartitionRuntime({...})     │
  │  allRoots()                  │  carries   │                                               │
  │  walk(...)                   │  topology  │   putItem(ctx, req)  = fokos.dispatch(...)    │
  └──────────────────────────────┘  + policy  │   getItem(ctx, req)  = fokos.dispatch(...)    │
                                              │   fokosInit(...)     = fokos.fokosInit(...)   │
                                              │   fokosMigrationPull = fokos.fokosMigrat...   │
                                              │   alarm(info)        = fokos.alarm(info)      │
                                              │                                               │
                                              │   hooks: evaluateSplit, exportPage,           │
                                              │          importPage, finalizeImport, ...      │
                                              └───────────────────────┬───────────────────────┘
                                                                      │ forwards, init, pull, ack
                                                                      ▼
                                                      other partitions of the same class
```

The runtime has one control-plane concept, the **repartition**. A hash split, a range split, and a key promotion
are three kinds of the same plan: select ownership on a source, create targets, cut routing over, migrate data,
collect acknowledgements, and clean the source. The host sees the plan through a few hooks. The runtime keeps
every durable stage.

Migration is one loop. The target asks the source for a page with an opaque cursor. The host exports the page on
the source and imports it on the target. The runtime checkpoints the cursor after each import and repeats until
the host returns no cursor. The host encodes its own phases in the cursor. The runtime never sees a row type.

One scheduler owns the Durable Object alarm. The runtime registers its own jobs. The host registers its jobs, for
example TTL expiry. Each job runs one bounded, idempotent step and reports when it wants to run next.

A reader who stops here knows the design: one runtime object, one dispatch call per RPC, one repartition state
machine, one migration loop, one scheduler, and `fokos`-prefixed RPC methods that the host delegates.

### 5.2 Technical details

#### 5.2.1 Integration model

The runtime is an owned object, not a base class and not a decorator.

- The host creates it in the constructor, before any other work, so its `blockConcurrencyWhile` runs first.
- The host implements the `FokosPartitionRpc` interface (section 5.2.15) with one-line delegations.
- The host calls `runtime.dispatch(operationName, ref, request)` from each public RPC method.
- The host reads `runtime.identity()` and `runtime.lifecycle()` when it needs partition facts.
- The host signals work with `runtime.requestSplitEvaluation()` and `runtime.requestPromotion(hashKey)`.

The host must not call a private method of another partition, must not resolve a stub for a control-plane
call, and must not call `setAlarm`. The runtime is the only code that creates stubs for `fokos*` methods. The
host creates stubs only inside its `forward` callbacks, and only for its own application RPC methods.

```ts
type MyPolicy = { ns: keyof Env; maxSizeMb: number };

export class MyPartitionDO extends DurableObject<Env> implements FokosPartitionRpc {
	readonly fokos: FokosPartitionRuntime<MyPolicy, MyCursor, MyPage>;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.fokos = new FokosPartitionRuntime({
			ctx,
			// The Worker names the binding inside the policy; the class does not need to know it.
			namespace: (routeCtx) => env[routeCtx.policy.ns] as DurableObjectNamespace,
			hooks: new MyHooks(ctx.storage, () => this.fokos.policy()),
			operations: myOperations(this),
		});
		void ctx.blockConcurrencyWhile(async () => this.runMyMigrations());
	}

	putItem(routeCtx: FokosRouteContext<MyPolicy>, req: PutReq): Promise<FokosEnvelope<PutRes>> {
		return this.fokos.dispatch("putItem", routeCtx, req);
	}

	fokosInit(req: FokosInitRequest) { return this.fokos.fokosInit(req); }
	fokosStartImport() { return this.fokos.fokosStartImport(); }
	fokosMigrationPull(req: FokosPullRequest) { return this.fokos.fokosMigrationPull(req); }
	fokosMigrationAck(req: FokosAckRequest) { return this.fokos.fokosMigrationAck(req); }
	fokosExecuteLocal(req: FokosExecuteLocalRequest) { return this.fokos.fokosExecuteLocal(req); }
	fokosStatus(routeCtx?: FokosRouteContext<MyPolicy>) { return this.fokos.fokosStatus(routeCtx); }
	fokosDestroy() { return this.fokos.fokosDestroy(); }
	alarm(info: AlarmInvocationInfo) { return this.fokos.alarm(info); }
}
```

#### 5.2.2 Identity, topology, policy, and the route context

A Durable Object cannot receive parameters at construction, and a library host such as FokosDB does not know at
build time which shard groups exist, how many roots each one has, or under which binding name the user attached
the class. The Worker decides all of that at request time. A Worker can give a paid tenant more root partitions
than a free tenant, with no code change in the Durable Object.

Every application request therefore carries a **route context**. It has three parts with three lifetimes.

```ts
type FokosRouteContext<TPolicy> = {
	schema: 1;
	/** Immutable identity of the target partition. */
	partitionId: string; // hex-encoded opaque bytes, same wire format as PartitionIdHelper today
	doName: string;      // `<shardGroup>.h.<root>[.<child>...]` or `<shardGroup>.r.<hk>.<start>.<end>`
	/** Immutable topology of the shard group. Persisted at creation. A later mismatch is an error. */
	topology: FokosTopology;
	/** Host policy. Opaque to the runtime. Persisted and replaced when a request carries a new value. */
	policy: TPolicy;
};

type FokosTopology = {
	shardGroup: string;
	rootTreesN: number;
	hashSplitN: number;
	/** Read when a range split is planned and stored in that plan. */
	rangeSplitN: number;
	/** Bounded ancestor set a new range child receives. */
	rangeAncestors: { fromRoot: number; fromLeaf: number };
};

type FokosPartitionRef = Pick<FokosRouteContext<unknown>, "partitionId" | "doName">;
```

| Part                       | Lifetime                       | Who sets it              | Who reads it                                   |
| -------------------------- | ------------------------------ | ------------------------ | ---------------------------------------------- |
| `partitionId`, `doName`    | Immutable                      | Router or source         | Runtime: identity check, child derivation      |
| `topology`                 | Immutable per shard group      | Worker at first request  | Runtime: child fan-out, names, ancestor hints  |
| `policy`                   | Mutable, last writer wins      | Worker on every request  | Host hooks only, through `runtime.policy()`    |

The runtime reads no field of `policy`. The host puts there whatever its hooks need: split thresholds, its own
namespace binding key, a coordinator binding key, tenant tier. FokosDB puts `ns`, `nsTx`, `hashSplitConditions`,
and `rangeSplitConditions` there.

**Persisted identity**, KV `__fokos/identity`, written once at bootstrap or `fokosInit`:

```ts
type FokosPartitionIdentity = {
	schema: 1;
	ref: FokosPartitionRef;
	kind: "hash" | "range";
	/** Hash: root index and child path. Range: hash key and [start, end). Decoded from ref.partitionId. */
	hash?: { rootIndex: number; path: number[] };
	range?: { hashKey: KeyBytes; start: KeyBytes | null; end: KeyBytes | null; depth: number };
	topology: FokosTopology;
};
```

**Persisted policy**, KV `__fokos/policy`, the last `policy` value a request carried. Background jobs read it
when no request is in flight, for example `evaluateSplit` with `trigger: "background"` and `cleanupSourceStep`.

Validation on each request:

1. `partitionId` and `doName` must equal the stored identity. A mismatch throws `fokos_identity_mismatch`.
2. Every field of `topology` must equal the stored topology. A mismatch throws `fokos_identity_mismatch`. The
   compare is exhaustive over the type, which closes audit 24.14.
3. `policy` is compared structurally with the stored value. When it differs, the runtime writes it in one
   `transactionSync` before it runs the operation. Equal values cost one compare and no write.

A root hash partition without a stored identity writes identity and policy from the first valid route context.
Any other partition without a stored identity throws `fokos_not_initialized`; only `fokosInit` creates it.

A target created by `fokosInit` receives its full route context from the source: the source derives the child
`partitionId` and `doName`, copies its own `topology`, and copies its current stored `policy`. Topology and
policy therefore flow from the Worker to the roots and from each source to its targets. No partition needs a
constructor parameter.

`rootTreesN` and `hashSplitN` must never change for an existing shard group. The runtime detects a change only
when a request reaches a partition that already exists; a root index that did not exist before bootstraps as
a fresh partition. The Worker must persist its topology choice per shard group. This is the same contract as
today.

The `primaryDoIdStr` field is dropped. The name is deterministic and `idFromName` recreates the ID. The
`PartitionIdHelper` codec moves into the runtime package unchanged.

**Runtime tuning**, a constructor option. These values do not change routing or ownership, so they can be code:

```ts
type FokosRuntimeOptions<TPolicy> = {
	/**
	 * The binding of the host's own class, resolved from a route context. The host stores its binding
	 * key inside `policy` when it does not know it at build time. Called for every stub the runtime creates.
	 */
	namespace(ctx: FokosRouteContext<TPolicy>): DurableObjectNamespace;
	caches?: {
		hashArenaBytes?: number;
		rangeHierarchyMaxRows?: number;
		promotionBloom?: { expectedKeys: number; falsePositiveRate: number } | false;
	};
	migration?: { pageBudgetBytes?: number; initRetries?: number };
	scheduler?: { fallbackAlarmMs?: number; fastPathDelayMs?: number };
};
```

When the runtime needs a stub outside a request, for example in a background job, it calls `namespace` with its
own stored route context, which the host policy inside it makes sufficient.

#### 5.2.3 Persisted state owned by the runtime

| State                         | Location                          | Content                                                        |
| ----------------------------- | --------------------------------- | -------------------------------------------------------------- |
| Identity                      | KV `__fokos/identity`             | `FokosPartitionIdentity`                                       |
| Policy                        | KV `__fokos/policy`               | Last host policy a request carried, opaque                     |
| Import record                 | KV `__fokos/import`               | Target-side repartition progress and opaque cursor             |
| Job schedule                  | KV `__fokos/jobs`                 | `{ [jobName]: { nextRunAt } }`                                 |
| Hash topology cache           | KV `__fokos/cache/hash_arena`     | `Uint32Array` arena snapshot, byte-bounded                     |
| Promotion Bloom cache         | KV `__fokos/cache/promotion_bloom`| Scalable Bloom filter snapshot, byte-bounded                   |
| Repartitions                  | SQL `fokos_repartitions`          | Source-side plans, one row per repartition                     |
| Repartition targets           | SQL `fokos_repartition_targets`   | One row per target: ref, slice, acknowledged                   |
| Route overrides               | SQL `fokos_route_overrides`       | `hash_key` → repartition id, for promoted keys                 |
| Range hierarchy cache         | SQL `fokos_range_hierarchy`       | Ancestor and learned descendant boundaries, row-bounded        |

```sql
CREATE TABLE fokos_repartitions (
	id            TEXT PRIMARY KEY,   -- `<partitionId>:<sequence>`
	kind          TEXT NOT NULL,      -- 'hash_split' | 'range_split' | 'key_promotion'
	state         TEXT NOT NULL,      -- 'queued' | 'cutover' | 'completed' | 'cleaned'
	plan          BLOB NOT NULL,      -- structured-clone of FokosRepartitionPlan
	cleanup_cursor BLOB,              -- opaque host cursor for source cleanup
	queued_at     INTEGER NOT NULL,
	cutover_at    INTEGER,
	completed_at  INTEGER,
	attempts      INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE fokos_repartition_targets (
	repartition_id TEXT NOT NULL REFERENCES fokos_repartitions(id),
	partition_id   TEXT NOT NULL,
	do_name        TEXT NOT NULL,
	slice          BLOB NOT NULL,
	acknowledged   INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (repartition_id, partition_id)
);
CREATE TABLE fokos_route_overrides (
	hash_key       BLOB PRIMARY KEY,
	repartition_id TEXT NOT NULL REFERENCES fokos_repartitions(id)
);
CREATE TABLE fokos_range_hierarchy (
	hash_key BLOB NOT NULL, depth INTEGER NOT NULL, start BLOB, end BLOB,
	PRIMARY KEY (hash_key, depth, start)
);
```

The runtime loads identity, policy, the import record, the repartition rows, the route overrides, and the cache
snapshots into memory inside `blockConcurrencyWhile` in its constructor. All later reads of this state are
in-memory. All writes go through `transactionSync` and update the memory mirror in the same call.

Repartition rows are permanent routing rules. A completed hash split makes the source a router forever. A
completed promotion sends one hash key to a range tree forever. Only progress fields are cleared.

#### 5.2.4 Runtime construction and hooks

```ts
class FokosPartitionRuntime<TPolicy, TCursor, TPage> implements FokosPartitionRpc {
	constructor(
		opts: FokosRuntimeOptions<TPolicy> & {
			ctx: DurableObjectState;
			hooks: FokosPartitionHooks<TPolicy, TCursor, TPage>;
			operations: Record<string, FokosOperation<any, any>>;
		},
	);

	dispatch<Req, Res>(op: string, routeCtx: FokosRouteContext<TPolicy>, req: Req): Promise<FokosEnvelope<Res>>;

	identity(): FokosPartitionIdentity;
	/** The stored host policy. Throws `fokos_not_initialized` before bootstrap. */
	policy(): TPolicy;
	/** The stored route context of this partition: identity, topology, and policy. */
	routeContext(): FokosRouteContext<TPolicy>;
	lifecycle(): FokosLifecycle;
	requestSplitEvaluation(): void;
	requestPromotion(hashKey: KeyBytes): FokosRequestPromotionResult;
	scheduleJob(name: string, runAt: number): void;
}
```

```ts
interface FokosPartitionHooks<TPolicy, TCursor, TPage> {
	/**
	 * Called after a local success that signals `evaluateSplit`, and by the background job while a
	 * split is queued. Returns true when the host wants this partition to split now. Synchronous.
	 * The host reads its own metrics and thresholds from `policy`. The runtime does not read SQL size.
	 */
	evaluateSplit(input: {
		identity: FokosPartitionIdentity;
		policy: TPolicy;
		trigger: "after_write" | "background";
	}): boolean;

	/**
	 * Range partitions only. Returns `childCount - 1` strictly increasing boundaries inside (start, end),
	 * or null when the host cannot produce valid boundaries yet. Synchronous.
	 */
	computeRangeBoundaries?(input: {
		hashKey: KeyBytes; start: KeyBytes | null; end: KeyBytes | null; childCount: number; policy: TPolicy;
	}): KeyBytes[] | null;

	/** Source side. One bounded page for one target. Can be async. */
	exportPage(
		req: FokosExportRequest<TCursor>,
	): FokosExportResult<TCursor, TPage> | Promise<FokosExportResult<TCursor, TPage>>;

	/** Target side. Runs inside the runtime's transactionSync together with the cursor checkpoint. */
	importPage(page: TPage, info: FokosImportInfo): void;

	/** Target side. Runs once after the last page and before the partition accepts local operations. */
	finalizeImport(info: FokosImportInfo): void | Promise<void>;

	/**
	 * Source side. Runs inside the cutover transactionSync. Return false to keep the plan queued.
	 * The host can write its own storage here; the write commits with the cutover or not at all.
	 */
	beforeCutover?(plan: FokosRepartitionPlan): boolean;

	/** Source side. Runs inside the completion transactionSync, after the last acknowledgement. */
	beforeComplete?(plan: FokosRepartitionPlan): void;

	/**
	 * Source side, optional. One bounded step of source cleanup after completion.
	 * Return `{ nextCursor: null }` when done. Undefined means the source keeps its data.
	 */
	cleanupSourceStep?(
		plan: FokosRepartitionPlan,
		cursor: unknown,
	): { nextCursor: unknown } | Promise<{ nextCursor: unknown }>;

	/** Called by the local admission step. Default: allow. Synchronous. */
	admit?(input: {
		op: string;
		admissionTag?: string;
		keys: RouteKey[];
		lifecycle: FokosLifecycle;
		policy: TPolicy;
	}): "allow" | { reject: Error };

	/** Observability and non-atomic follow-up. Runs after the transaction that caused it commits. */
	onLifecycleEvent?(event: FokosLifecycleEvent): void | Promise<void>;

	/** Host background jobs. Section 5.2.14. */
	jobs?: FokosJob[];

	logParams?(): Record<string, unknown>;
}
```

```ts
type FokosSlice =
	| { kind: "hash_child"; childIndex: number; depth: number }
	| { kind: "range"; hashKey: KeyBytes; start: KeyBytes | null; end: KeyBytes | null }
	| { kind: "promoted_key"; hashKey: KeyBytes };

type FokosExportRequest<TCursor> = {
	repartitionId: string;
	target: FokosPartitionRef;
	slice: FokosSlice;
	/** The same ownership function the runtime uses for routing. The host must filter rows with it. */
	belongsToTarget(key: RouteKey): boolean;
	cursor: TCursor | null;
	budgetBytes: number;
};
type FokosExportResult<TCursor, TPage> = { page: TPage; nextCursor: TCursor | null; bytes: number };
type FokosImportInfo = { repartitionId: string; source: FokosPartitionRef; slice: FokosSlice };

type FokosRepartitionPlan = {
	id: string;
	kind: "hash_split" | "range_split" | "key_promotion";
	source: FokosPartitionRef;
	targets: Array<{ ref: FokosPartitionRef; slice: FokosSlice }>;
	/** "router": the source owns nothing after cutover. "retains_others": it owns all non-selected keys. */
	sourceAfterCutover: "router" | "retains_others";
};

type FokosLifecycleEvent =
	| { type: "bootstrapped"; identity: FokosPartitionIdentity }
	| { type: "initialized_as_target"; identity: FokosPartitionIdentity; info: FokosImportInfo }
	| {
			type: "repartition_queued" | "repartition_cutover" | "repartition_completed" | "repartition_cleaned";
			plan: FokosRepartitionPlan;
		}
	| { type: "import_completed"; info: FokosImportInfo }
	| { type: "import_acknowledged"; info: FokosImportInfo };

type FokosLifecycle = {
	role: "owner" | "router";
	import: null | { state: "awaiting_data" | "importing" | "imported" | "active" };
	activeRepartition: null | { id: string; kind: FokosRepartitionPlan["kind"]; state: "queued" | "cutover" };
};
```

Rules for hooks:

- `evaluateSplit`, `computeRangeBoundaries`, `importPage`, `beforeCutover`, `beforeComplete`, and `admit` are
  synchronous. Four of them run inside a `transactionSync`. An `await` there is a defect.
- `exportPage`, `finalizeImport`, `cleanupSourceStep`, and `onLifecycleEvent` can be async.
- A hook that needs the host policy and does not receive it as input calls `runtime.policy()`. The value is the
  stored policy, which the last request updated.
- A hook must not throw to express a policy result. It returns the result. A thrown error is a defect and the
  runtime logs it, keeps durable state unchanged, and retries on the next background pass.
- `importPage` must be idempotent. The runtime commits the page and the cursor in one transaction, but the host
  cursor is opaque, so two pages can legitimately overlap at a phase boundary.

#### 5.2.5 Operation descriptors

The host registers each application operation once. The runtime uses the descriptor for dispatch, for
read-through during import, and for stale-transaction style recovery that the host runs through
`dispatch`. A registry replaces the current per-call closures so that `fokosExecuteLocal` (section 5.2.13)
can find the local handler by name.

```ts
type FokosOperationBase<Req, Res> = {
	/**
	 * "retry": while this partition imports, throw `fokos_importing` (retryable).
	 * "read_source": while this partition imports, run the same operation locally on the source partition.
	 */
	whileMigrating: "retry" | "read_source";
	/** Opaque to the runtime. Passed to hooks.admit. */
	admissionTag?: string;
	local(req: Req): Promise<Res> | Res;
	/**
	 * `target` is the full route context of the destination. The runtime derives it: the target identity,
	 * this partition's topology, and this partition's stored policy. The host passes it to its own RPC.
	 */
	forward(stub: DurableObjectStub, target: FokosRouteContext<unknown>, req: Req): Promise<FokosEnvelope<Res>>;
	/** Runs after a local success. Returns signals. Cannot change the result. Synchronous. */
	afterLocalSuccess?(req: Req, res: Res): FokosSignals | void;
};

type FokosSignals = { evaluateSplit?: boolean; promotionCandidates?: KeyBytes[] };

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
		})
	| (FokosOperationBase<Req, Res> & {
			shape: "scan";
			whileMigrating: "read_source";
			scan(req: Req): { hashKey: KeyBytes; start: KeyBytes | null; end: KeyBytes | null; descending: boolean };
			/** Restrict the request to one child interval. */
			clip(req: Req, interval: { start: KeyBytes | null; end: KeyBytes | null }): Req;
			/** Fold one child result into the accumulator. `remaining: null` stops the traversal. */
			fold(acc: Res | null, part: Res, req: Req): { acc: Res; remaining: Req | null };
		})
	| { shape: "local"; local(req: unknown): Promise<unknown> | unknown };
```

| Shape          | Owner resolution                              | Behavior                                                                  |
| -------------- | --------------------------------------------- | ------------------------------------------------------------------------- |
| `point`        | One key                                       | Local, or forward to one partition. Learns caches from the envelope.      |
| `group`        | Every item                                    | Local group plus one group per remote partition. Host merges.             |
| `single_owner` | Every item                                    | Exactly one destination. Otherwise throw `fokos_single_owner_fallback`.   |
| `scan`         | Hash key to a leaf, then ordered range visit  | Visits intersecting range children in order until `fold` stops.           |
| `local`        | None                                          | Never forwarded, never gated. Used by `fokosExecuteLocal` and by admin.   |

`single_owner` throws instead of returning a host value. The error has no side effects and passes unchanged
through every forwarding hop, so the caller runs its multi-partition path. This is the behavior of
`errSinglePartitionFastPathFallback` today.

A `group` operation on a partition that is a router has an empty local group. The host `merge` receives zero
or more parts and one part per remote target.

#### 5.2.6 The dispatch pipeline

`dispatch` runs these steps in this order. The order is the contract from section 21 of the audit.

1. **Identity.** Validate identity and topology, store a changed policy, or bootstrap a root (section 5.2.2).
2. **Lifecycle gate.** If `import.state` is `awaiting_data` or `importing`:
   - `whileMigrating: "read_source"` → call `source.fokosExecuteLocal({ op, request, caller: selfRef })`,
     add one to `forwardCount`, return.
   - `whileMigrating: "retry"` and state `importing` → throw `fokos_importing`.
   - `whileMigrating: "retry"` and state `awaiting_data` → throw `fokos_not_owner_yet`.
   The states `imported` and `active` pass the gate. The data is complete.
3. **Owner resolution** for every key (section 5.2.7). Group the items by destination.
4. **Admission.** If a local group exists, call `hooks.admit`. A rejection throws the host error unchanged.
5. **Execution.** Run the local group and the remote groups per the shape. Remote groups run in parallel.
6. **Learning.** For each successful remote envelope, learn the route (section 5.2.9), add one to
   `forwardCount`.
7. **Signals.** If the local group succeeded and the descriptor has `afterLocalSuccess`, collect signals and
   apply them (section 5.2.11). An error here is logged and does not change the result.
8. **Envelope.** Return the result wrapped for a local result, or the merged remote envelope.

The `awaiting_data` distinction in step 2 exists because a target is initialized before the source cuts over.
Until the target imports its first page it does not know that it owns anything. A speculative caller that
receives `fokos_not_owner_yet` treats it as a false positive (section 5.2.7). A source that has cut over
retries.

#### 5.2.7 Owner resolution

One function resolves a route key to an owner. Every shape uses it.

```ts
type FokosOwner =
	| { kind: "local" }
	| { kind: "remote"; target: FokosPartitionRef; speculative: boolean }
	| { kind: "out_of_range" };
```

Resolution order on a hash partition:

1. **Route override.** If `fokos_route_overrides` has the hash key and the repartition state is `cutover` or
   later, the owner is the promotion target: the range root, or a deeper range slice from the range hierarchy
   cache. This is authoritative.
2. **Promotion Bloom cache.** If the filter reports a probable promotion by a descendant, the owner is the
   range root, `speculative: true`. Point and scan shapes use this step. Group and single-owner shapes skip it.
3. **Topology.** If this partition is a router (`sourceAfterCutover: "router"` on a `cutover` or later
   repartition), pick the child by the hash function at this depth, then apply the hash arena cache to jump
   deeper. Otherwise the owner is local.

Resolution order on a range partition:

1. The hash key must equal `identity.range.hashKey`, and the sort key must be inside `[start, end)`. Otherwise
   `out_of_range`.
2. If this partition is a router, pick the child whose interval contains the sort key, then apply the range
   hierarchy cache to jump deeper. Otherwise the owner is local.

A speculative forward that fails with `fokos_not_initialized` or `fokos_not_owner_yet` resolves again with step 2
disabled. Any other error propagates.

`out_of_range` throws `fokos_out_of_range`. It is a routing defect, not backpressure. A hash leaf does not verify
its hash path; correct parent routing is the contract, as today.

The same `belongsToTarget` predicate that `exportPage` receives is derived from the same function as step 3 for
hash children and step 2 for range children. One implementation, two callers.

#### 5.2.8 Response envelope

The runtime wraps every result. Application response types do not carry routing fields.

```ts
type FokosEnvelope<T> = {
	value: T;
	route: {
		servedBy: FokosPartitionRef;
		hashDepth: number;
		rangeDepth: number;
		forwardCount: number;
		/** Internal. Bounded ancestor boundaries of a range leaf. Consumers must drop it. */
		_hint?: { rangeAncestors: Array<{ depth: number; start: KeyBytes; end: KeyBytes }> };
	};
};
```

A forwarding partition keeps `servedBy`, `hashDepth`, `rangeDepth`, and `_hint` from the child envelope and
adds one to `forwardCount`. The `forward` callback of an operation must return the envelope of the remote
call unchanged. The Worker-side `FokosRouter.unwrap(envelope)` returns `value` and a public route summary
without `_hint`.

Because every shape returns an envelope, transaction operations learn routes from their own fan-out. This
removes limitation 24.11 of the audit.

#### 5.2.9 Route caches

Caches are hints. A miss, a full cache, a stale entry, or a disabled cache changes latency only. Ownership is
decided by the route override table and the topology, never by a cache.

| Cache                | Storage                          | Learns from                                     | Bound                              |
| -------------------- | -------------------------------- | ----------------------------------------------- | ---------------------------------- |
| Hash arena           | KV `__fokos/cache/hash_arena`    | `route.hashDepth` of a forwarded envelope       | `caches.hashArenaBytes`, depth cap |
| Range hierarchy      | SQL `fokos_range_hierarchy`      | `route._hint.rangeAncestors`                    | `caches.rangeHierarchyMaxRows`     |
| Promotion Bloom      | KV `__fokos/cache/promotion_bloom` | A hash partition forwarded and `servedBy` is a range partition | filter size, no removal |

The range hierarchy table gains a row bound, which the current `range_hierarchy` table lacks (audit 24.10).
When full, the runtime evicts the deepest rows for the hash key with the oldest learn time. The cache
implementations move from `hash-topology.ts` and `partial-range-topology.ts` with their tests. They sit behind
one internal contract: `lookup(key)`, `learn(key, envelope)`, `invalidate(key, target)`. The contract is not
public in this version.

#### 5.2.10 Repartition state machine

One state machine serves hash splits, range splits, and key promotions.

**Source side** — `fokos_repartitions.state`:

```
 (none) ──queue──► queued ──start──► cutover ──last ack──► completed ──cleanup done──► cleaned
                     │  ▲
                     └──┘ retry: boundaries not ready, beforeCutover false, target init failed
```

| Transition            | Trigger                                              | Durable write (one `transactionSync`)                                  | Then                                              |
| --------------------- | ---------------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------- |
| `queue`               | Signal accepted by arbitration (5.2.11)              | Insert row `queued`; for promotion, insert route override              | Schedule `source_repartition`; set fallback alarm |
| `start` step 1: plan  | Job `source_repartition`, state `queued`             | none                                                                   | Build targets (table below)                       |
| `start` step 2: init  | Plan built                                           | none                                                                   | `target.fokosInit(...)` for all targets, parallel, `migration.initRetries` (default 5) each |
| `start` step 3: cut   | Every init succeeded                                 | `beforeCutover(plan)` must return true; set `cutover`, `cutover_at`; `role` becomes `router` for splits | `target.fokosStartImport()` all, best effort |
| `ack`                 | `fokosMigrationAck` from a member target             | Set `acknowledged = 1`; if all acknowledged: `beforeComplete(plan)`, set `completed` | `onLifecycleEvent(repartition_completed)`  |
| `cleanup`             | Job `source_cleanup`, state `completed`              | Each step: persist `cleanup_cursor`; on `null`: set `cleaned`          | Reschedule until done                             |

| Kind            | Selected ownership          | Targets                                                                   | `sourceAfterCutover` |
| --------------- | --------------------------- | ------------------------------------------------------------------------- | -------------------- |
| `hash_split`    | Every key of the source     | `hashSplitN` children, deterministic from the source path                 | `router`             |
| `range_split`   | The interval `[start, end)` | `rangeSplitN` children tiling the interval at `computeRangeBoundaries`    | `router`             |
| `key_promotion` | One hash key                | The range root `<shardGroup>.r.<hk>.~min.~max`                            | `retains_others`     |

A range split whose `computeRangeBoundaries` returns `null` stays `queued` and retries on the next pass with
exponential backoff from `fallbackAlarmMs` up to 5 minutes. A promotion whose `beforeCutover` returns `false`
does the same. Targets that a failed start created stay `awaiting_data`, and the next start reuses them;
`fokosInit` is idempotent for the same `(repartitionId, target)`.

**Target side** — KV `__fokos/import`:

```
 fokosInit ──► awaiting_data ──first page imported──► importing ──finalizeImport──► imported ──ack ok──► active
```

| Transition        | Trigger                                     | Durable write                                                    | Then                                  |
| ----------------- | ------------------------------------------- | ---------------------------------------------------------------- | ------------------------------------- |
| `fokosInit`       | Source `start` step 2                       | Identity, import record `awaiting_data`, range ancestors         | Set fallback alarm on the target      |
| `importing`       | First successful `fokosMigrationPull`       | With the first page: state `importing`, cursor                   | Loop                                  |
| `imported`        | `nextCursor === null` and `finalizeImport` returned | State `imported`, cursor deleted                           | Schedule `target_ack`                 |
| `active`          | `source.fokosMigrationAck` returned         | State `active`                                                   | `onLifecycleEvent(import_acknowledged)` |

The `imported` state is new. Today the child writes `migration_completed` and then acknowledges; a crash
between the two leaves the parent in `split_started` forever (audit 24.5). Here the `target_ack` job retries
until the source confirms. The target's own fallback alarm from `fokosInit` starts the import even when the
source's `fokosStartImport` trigger is lost (audit 24.6).

#### 5.2.11 Arbitration between repartitions

All arbitration runs inside one `transactionSync` that reads the repartition rows and writes the decision. This
closes the queue window of audit 24.7.

| Request                       | Accepted when                                                                                  |
| ----------------------------- | ---------------------------------------------------------------------------------------------- |
| queue `hash_split`            | Partition kind is `hash`, and no row of any kind is `queued` or `cutover`                      |
| queue `range_split`           | Partition kind is `range`, and no row is `queued` or `cutover`                                 |
| queue `key_promotion`         | Partition kind is `hash`, no `hash_split` row is `queued` or `cutover`, no override for the key |
| cut over `key_promotion`      | No `hash_split` row is `queued` or `cutover`, and `beforeCutover` returned true                |
| cut over a split              | `beforeCutover` returned true or is undefined                                                   |

A `hash_split` row in `completed` or `cleaned` does not need to block later promotions, because a router
receives no local writes and therefore no promotion signals. A `key_promotion` row in `completed` or `cleaned`
never blocks anything.

Signals arrive from `afterLocalSuccess`, `requestSplitEvaluation`, and `requestPromotion`. The runtime handles a
signal in the request that produced it, after the result is fixed:

1. `evaluateSplit: true` → call `hooks.evaluateSplit({ trigger: "after_write" })`; when true, queue the split for
   this partition kind.
2. `promotionCandidates` → for each key, queue a `key_promotion` when arbitration accepts it.
3. Schedule `source_repartition` on the fast path and set the fallback alarm.

The durable queue write and the `setAlarm` call are awaited. The fast-path timer is not. A failure in this step
is logged and the request still returns its result.

A hash child inherits promotions. Hash split targets receive, through the host's own page format, whatever the
host chooses to migrate. The runtime also copies the `fokos_route_overrides` rows whose hash key belongs to a
target, and the matching completed `key_promotion` rows, as part of `fokosInit`. This keeps the forwarding
pointer with the new owner without a host hook.

#### 5.2.12 Migration protocol

One RPC, one loop, one opaque cursor.

```ts
type FokosPullRequest<TCursor> = {
	repartitionId: string;
	target: FokosPartitionRef;
	cursor: TCursor | null;
	budgetBytes: number;
};
type FokosPullResult<TCursor, TPage> = { page: TPage; nextCursor: TCursor | null; bytes: number };
```

Source side, `fokosMigrationPull`:

1. The repartition row must exist, else `fokos_repartition_unknown`.
2. The target must be a member row, else `fokos_target_unknown`.
3. The state must be `cutover` or `completed`. State `queued` throws `fokos_not_owner_yet`: targets exist but the
   cutover is not durable, so the source still owns the data and must not export it.
4. Call `hooks.exportPage` with the slice, the `belongsToTarget` predicate, the cursor, and the budget.
5. Return the page unchanged.

Target side, job `target_import`:

1. Read the import record and cursor.
2. `page = source.fokosMigrationPull({ repartitionId, target: self, cursor, budgetBytes })`.
3. `transactionSync`: `hooks.importPage(page, info)`; write the returned cursor; on the first page set state
   `importing`.
4. Repeat while `nextCursor !== null`. The runtime can start the next pull before the current import completes;
   the cursor it uses is the `nextCursor` of the page it already holds.
5. `await hooks.finalizeImport(info)`.
6. `transactionSync`: state `imported`, cursor deleted.
7. Schedule `target_ack`.

Any error stops the loop, keeps the record as is, and reschedules with backoff. A resume starts strictly after
the last committed cursor. The host encodes phases inside the cursor and the page. For FokosDB that is
`items → pending transactions and watermark → promoted keys`. The runtime does not know these names.

The default `budgetBytes` is 20 MiB. The host must return a page whose serialized size is at or below the
budget. The runtime does not measure it. The RPC layer rejects a message above 32 MiB, and the runtime treats
that as a page error and retries with half the budget, down to 1 MiB.

#### 5.2.13 Read-through during import

An operation with `whileMigrating: "read_source"` runs on the source while the target imports. The target calls
`source.fokosExecuteLocal({ op, request, caller })`. The source:

1. Validates that `caller` is a target of one of its repartitions in state `cutover` or `completed`. Otherwise
   `fokos_target_unknown`. This authorization replaces the trust that `internalGetItemDirect` places in callers
   today (audit 24.13).
2. Finds the operation by name and runs its `local` handler without owner resolution and without the lifecycle
   gate.
3. Returns an envelope with `servedBy = source`.

A source that is a router for the requested key is never reached here: the target only reads from its own
source, and the source's local storage holds the complete data for the target's slice until cleanup, which runs
only after the target is `active`.

A `scan` shape must use `read_source`, because a range child that answers with partial data would return a wrong
ordered result rather than a retryable error.

#### 5.2.14 Background scheduler

The runtime owns the alarm and runs named jobs.

```ts
type FokosJob = {
	name: string;
	/** False skips the job in this pass. Synchronous. */
	canRun(): boolean;
	/** One bounded, idempotent step. */
	runStep(): Promise<{ nextRunAt: number | null }>;
};
```

Built-in jobs run first, in this order: `target_import`, `target_ack`, `source_repartition`, `source_cleanup`.
Host jobs follow in registration order. FokosDB registers stale-transaction recovery and TTL expiry here and
removes its separate TTL timer.

One pass:

1. For each job with `canRun()`, run `runStep()`. Catch its error, log it, and set its next run to
   `now + fallbackAlarmMs`. One failing job never stops another.
2. Persist `__fokos/jobs` with every `nextRunAt`.
3. `setAlarm(min(nextRunAt))` when any job wants to run. Otherwise `deleteAlarm()`.

The `alarm()` handler never throws. The runtime sets its own retry alarm instead, as the Alarms API guide
recommends. The fast path is an in-memory timer of `fastPathDelayMs` (default 50 ms) that calls the same pass.
The runtime keeps one in-flight pass promise. A fast-path request or an alarm that arrives during a pass waits
for it, then runs one more pass. Two passes never interleave (audit 24.12).

`runtime.scheduleJob(name, runAt)` lets the host request an earlier run for one of its jobs. It moves
`nextRunAt` earlier only, persists, and moves the alarm earlier if needed.

Every job, built-in or host, must be idempotent and resumable. A crash in the middle of a step is recovered by
the next step reading durable state.

#### 5.2.15 Control-plane RPC surface

The host implements this interface by delegation. The runtime implements it too, so each host method is one
line. These are the only RPC methods the runtime calls on a peer, and the runtime is the only caller of them.

```ts
interface FokosPartitionRpc {
	/** Create this partition as a target. Idempotent for the same (repartitionId, target). */
	fokosInit(req: FokosInitRequest): Promise<void>;
	/** Best-effort fast start of the import job. */
	fokosStartImport(): Promise<void>;
	/** Source side of the migration loop. */
	fokosMigrationPull(req: FokosPullRequest<unknown>): Promise<FokosPullResult<unknown, unknown>>;
	/** Target reports a complete import. Idempotent. Validates membership. */
	fokosMigrationAck(req: { repartitionId: string; target: FokosPartitionRef }): Promise<void>;
	/** Run one registered operation locally, without routing. Authorized targets only. */
	fokosExecuteLocal(req: { op: string; request: unknown; caller: FokosPartitionRef }): Promise<FokosEnvelope<unknown>>;
	/** Identity, lifecycle, outgoing links, cache statistics. Can bootstrap a root when `routeCtx` is given. */
	fokosStatus(routeCtx?: FokosRouteContext<unknown>): Promise<FokosStatus>;
	/** Delete the alarm and all storage, then abort the instance. The caller traverses links first. */
	fokosDestroy(): Promise<void>;
	alarm(info: AlarmInvocationInfo): Promise<void>;
}

type FokosInitRequest<TPolicy> = {
	repartitionId: string;
	source: FokosPartitionRef;
	/** Full route context of the new partition: identity, the source's topology, the source's stored policy. */
	target: FokosRouteContext<TPolicy>;
	slice: FokosSlice;
	rangeDepth?: number;
	rangeAncestors?: Array<{ depth: number; start: KeyBytes; end: KeyBytes }>;
	inheritedOverrides?: Array<{ hashKey: KeyBytes; plan: FokosRepartitionPlan }>;
};

type FokosStatus = {
	identity: FokosPartitionIdentity;
	lifecycle: FokosLifecycle;
	/** Targets of every repartition in state cutover or later: split children and promotion range roots. */
	links: FokosPartitionRef[];
	repartitions: Array<{ id: string; kind: string; state: string; acknowledged: number; targets: number }>;
};
```

`fokosInit` with a conflicting existing identity or a different `(repartitionId, source)` throws
`fokos_init_conflict`. A conflicting retry is a defect and must not be repaired silently.

`fokosStatus().links` gives one graph view for administration. Destruction walks `links` post-order and calls
`fokosDestroy` on each partition. The Worker does not need to know about split children and promoted keys
separately.

Debug operations of the host, for example a forced promotion, call `runtime.requestPromotion(hashKey)` from the
host's own RPC method. The runtime routes the request to the current owner of the key first, which fixes audit
24.8.

#### 5.2.16 Worker-side router

```ts
class FokosRouter<TPolicy> {
	/** Cheap to construct. A Worker can build one per request with a tenant-specific topology and policy. */
	constructor(topology: FokosTopology, policy: TPolicy);
	rootContext(hashKey: KeyBytes): FokosRouteContext<TPolicy>;
	allRoots(): FokosRouteContext<TPolicy>[];
	unwrap<T>(envelope: FokosEnvelope<T>): { value: T; route: FokosPublicRoute };
	/** Post-order traversal over `fokosStatus().links`, starting from every root. */
	walk(
		namespace: DurableObjectNamespace,
		visit: (ctx: FokosRouteContext<TPolicy>, stub: DurableObjectStub) => Promise<void>,
	): Promise<void>;
}
```

The router hashes the hash key to a root index with the function in `router.ts` today and builds the root route
context. It caches nothing. All deeper routing happens inside partitions.

The Worker chooses `topology` and `policy` at run time. FokosDB builds the router from its table configuration,
so a caller can give one table 4 roots and another table 64 roots, and can change split thresholds without a
deployment. The only rule is the one in section 5.2.2: `rootTreesN` and `hashSplitN` must stay fixed for a shard
group that exists.

#### 5.2.17 Errors

Durable Object RPC carries only `message` across the boundary. Every runtime error is a `FokosError` whose
message starts with `fokos[<code>]:`. `fokosErrorCode(e: unknown): FokosErrorCode | null` parses the code from
any `Error`, on either side of the boundary. Hosts and clients match on the code, never on the class.

| Code                          | Retryable | Meaning                                                                    |
| ----------------------------- | --------- | -------------------------------------------------------------------------- |
| `fokos_not_initialized`       | no        | Non-root partition has no identity. Speculative callers fall back.         |
| `fokos_identity_mismatch`     | no        | `ref` does not match the stored identity.                                  |
| `fokos_not_owner_yet`         | yes       | Target in `awaiting_data`, or source still `queued`. Speculative callers fall back. |
| `fokos_importing`             | yes       | Target imports and the operation is `whileMigrating: "retry"`.             |
| `fokos_out_of_range`          | no        | Key cannot belong to this partition. Routing defect.                       |
| `fokos_single_owner_fallback` | no        | `single_owner` items span more than one partition. No side effects.        |
| `fokos_init_conflict`         | no        | `fokosInit` disagrees with stored identity or import record.               |
| `fokos_repartition_unknown`   | no        | Pull or ack for an unknown repartition.                                    |
| `fokos_target_unknown`        | no        | Pull, ack, or execute-local from a partition that is not a member.         |
| `fokos_group_partial_failure` | yes       | `attempt_all` group had at least one failed remote group. Message lists them. |

Host errors, including admission rejections, pass through unchanged. The runtime never wraps them.

#### 5.2.18 Invariants and their mechanisms

| Invariant (audit section 17)                                             | Mechanism                                                                                   |
| ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------- |
| A root stays usable after any number of splits                           | Repartition rows are permanent; a router forwards forever                                   |
| A router never handles data locally after cutover                        | Owner resolution step 3 returns `remote` for every key when `role === "router"`             |
| Child selection and migration filtering use one ownership function       | `belongsToTarget` is derived from the same strategy function as owner resolution            |
| A range interval has one owner with `[start, end)`                       | Boundaries are in the immutable identity; `out_of_range` on violation                       |
| Cached routes are hints                                                  | Caches are consulted only after the override table; a bad hint lands on a router or errors  |
| Direct source reads bypass forwarding                                    | `fokosExecuteLocal` runs `local` without owner resolution                                   |
| The source owns data until all targets exist and cutover is durable      | `start` writes `cutover` only after every `fokosInit` returned; pulls in `queued` are refused |
| Init and ack are idempotent                                              | Both compare against stored rows and return on equality                                     |
| All targets acknowledge before `completed`                               | `ack` counts member rows; unknown targets are rejected                                      |
| Behavior is correct in `cutover` and `completed`                         | Owner resolution treats both states the same                                                |
| Normal writes never modify an incomplete target                          | Lifecycle gate, step 2 of dispatch                                                           |
| Reads stay available through the source when configured                  | `whileMigrating: "read_source"`                                                             |
| Migration pages are bounded and progress is durable                      | `budgetBytes`; cursor committed with the page                                               |
| Required host state migrates with the data                               | The host owns the page format and its phases                                                |
| Finalization runs before local operations are accepted                   | Gate passes only at `imported` or later, which follows `finalizeImport`                     |
| Acknowledgement and cleanup are retryable                                | `target_ack` and `source_cleanup` jobs with durable state                                   |
| Transaction operations route to current owners                           | Group shape resolves every item on every hop                                                |
| A commit decision is never blocked by size backpressure                  | Admission is a host hook; the FokosDB host allows its commit tag                            |
| Cancel tries all destinations and reports partial failure                | `failurePolicy: "attempt_all"` and `fokos_group_partial_failure`                            |
| Locks prepared before a split migrate to the new owner                   | The host's page phases include pending transactions                                         |
| Recovery uses the routed operation                                       | The host calls `dispatch` for commit and cancel from its recovery job                       |
| Promotion cutover never moves a key with local locks                     | `beforeCutover` runs inside the cutover transaction; FokosDB returns false on any lock      |
| Reads and deletes stay available on an over-size leaf                    | Admission tags are host policy; the runtime rejects nothing by size                         |
| Failed background work keeps durable state for retry                    | Every job step reads and writes durable state; errors do not roll back committed steps      |

#### 5.2.19 Failure and recovery

| Failure                                                          | Recovery                                                                                             |
| ---------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| Source crashes after some `fokosInit` calls, before cutover      | Row is `queued`; next pass re-inits (idempotent) and cuts over                                       |
| Source crashes after cutover, before `fokosStartImport`          | Targets start from their own fallback alarm                                                          |
| Target crashes mid-import                                        | Resume from the committed cursor; re-imported rows are idempotent                                    |
| Target crashes after `finalizeImport`, before writing `imported` | `finalizeImport` runs again; it must be idempotent                                                   |
| Target crashes after `imported`, before ack                      | `target_ack` retries until the source confirms                                                       |
| Source unreachable during ack                                    | `target_ack` retries with backoff; target already serves requests                                    |
| Ack from an unknown partition                                    | `fokos_target_unknown`; the split cannot complete on a bad count                                     |
| Bloom false positive on an uninitialized range root              | `fokos_not_initialized`; resolve again without the Bloom step                                        |
| Bloom hit on a range root that is `awaiting_data`                | `fokos_not_owner_yet`; same fallback; the source still owns the key                                  |
| RPC message above 32 MiB from `exportPage`                       | Retry with half the budget down to 1 MiB; then log and stop the job until the host fixes the export  |
| Host hook throws inside a `transactionSync`                      | Transaction rolls back; state unchanged; job retries on the next pass                                |
| Alarm handler fails six times                                    | Cannot happen by design: the handler catches all errors and sets its own retry alarm                |
| Request arrives while a pass runs                                | Single-threaded DO interleaves at `await`; all state writes are `transactionSync`, so no torn state  |

#### 5.2.20 Concurrency

A Durable Object runs one JavaScript thread, but `await` points interleave requests, alarms, and the fast-path
pass. The runtime keeps correctness with three rules:

1. Every durable transition is one `transactionSync` with no `await` inside. Hooks that run inside are
   synchronous by contract.
2. The in-memory mirror changes only inside the same `transactionSync`. A request that runs after the `await`
   sees either the old or the new state, never a mix.
3. One scheduler pass at a time. Overlapping requests coalesce into one more pass.

Remote fan-out inside a group operation runs with `Promise.all` for `fail_fast` and `Promise.allSettled` for
`attempt_all`. Migration pulls are sequential per target with one page of prefetch.

#### 5.2.21 Performance

Common path, a point operation served locally: identity and topology compare (in memory, seven scalar
fields), policy compare (in memory, structural, bounded by the policy size, which for FokosDB is about ten
fields), lifecycle check (in memory), override lookup (in-memory `Map`, one hash key), topology decision (in
memory), one envelope allocation. No storage read is added. A storage write happens only when the policy
changed since the last request. Compared with today, this removes the per-request `PromotionManager.statusFor`
SQL query on hash partitions. Rows in `fokos_route_overrides` are few per partition; the in-memory mirror costs
about 100 bytes per promoted key.

Forwarded point operation: one extra RPC hop per uncached level. With warm caches a root reaches the serving leaf
in one hop. Napkin math: a Durable Object RPC in the same colo takes about 1 to 5 ms; across regions about 50 to
150 ms. A cold three-level path therefore costs three hops before caches warm. `TODO: measure` after
milestone 2 with the example host.

Group operations add one `Map` per destination and parallel remote calls. Scans add one RPC per intersecting
range child, as today.

Background: one alarm per partition, one pass per alarm. A pass with no due job costs one KV read and one
`getAlarm`.

#### 5.2.22 FokosDB adapter mapping

`PartitionDO` becomes a host. The table maps today's mechanisms to the new boundary.

| Today in `PartitionDO`                                          | With the runtime                                                                       |
| --------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `ensurePartitionContext`, `ensureMigration`, `#rpc`             | `dispatch` steps 1 and 2                                                               |
| `withSplitForwarding`                                           | `point` shape                                                                          |
| `groupItemsByRouting` for prepare, commit, cancel, read         | `group` shape; cancel uses `attempt_all`                                               |
| `routeSingleDestination` for snapshot read and single-shot      | `single_owner` shape                                                                   |
| `walkRangeChildren` in `apiQueryItems`                          | `scan` shape with `clip` and `fold` over the query budgets and cursor                  |
| `internalGetItemDirect`, `internalQueryItemsDirect`             | `whileMigrating: "read_source"` on `getItem` and `queryItems`                          |
| `OperationIntent` read, write, delete, ignore_size_reject       | `admissionTag` with the same four values; `hooks.admit` keeps the 110% rule            |
| `checkSplits`, `checkSplitsNoKey`                               | `afterLocalSuccess` returns `{ evaluateSplit: true }`; `evaluateSplit` reads SQL size  |
| `maybeQueuePromotion` at 25% of `maxSizeMb`                     | `afterLocalSuccess` returns `promotionCandidates` from the key-size estimate           |
| `PromotionManager` cutover lock check                           | `beforeCutover` returns `pendingLockCountForHashKey(hk) === 0`                         |
| `PromotionManager.runGC`                                        | `cleanupSourceStep` for `key_promotion`; undefined result for splits keeps rows today  |
| `computeRangeSplitBoundaries` in `PartitionStore`               | `computeRangeBoundaries`                                                               |
| Three migration RPCs and cursors                                | One `exportPage` and `importPage` with a phased cursor `{ phase, inner }`              |
| `promoted_keys` table                                           | `fokos_route_overrides` and repartition rows, owned by the runtime                     |
| `range_hierarchy` table                                         | `fokos_range_hierarchy`, owned by the runtime                                          |
| Deletion of parent pending rows at `split_completed`            | `beforeComplete` for `hash_split` and `range_split`                                    |
| Stale-transaction recovery in `alarm`                           | Host job; `canRun` returns `lifecycle().role === "owner" && import is null or active`   |
| `TtlExpiry` with its own timer                                  | Host job                                                                               |
| `meta` and `partitionMetas` with `_internal`                    | `FokosEnvelope.route`; `partitionMetas` stays a FokosDB value inside `value`           |
| `fokosStaleTransactionMs`, `fokosGetColoInfo`, `fokosTtlConfig` | Unchanged host methods                                                                 |
| `PartitionContext` with thresholds and `ns`, `nsTx`             | `FokosRouteContext`; `ns`, `nsTx`, and split conditions move into the opaque `policy` |
| Immutable and mutable context comparison helpers                | Runtime compares topology exhaustively and policy structurally                          |

The transaction coordinator stores the root `FokosRouteContext` per participant, as it stores the context today.
It reads `policy.nsTx` and `policy.ns` for its bindings. Its participant list still does not change when the
tree changes.

#### 5.2.23 Deployment and rollback

The runtime ships as a separate entry of the `fokosdb` package or as its own package (open question 5.3.1).
FokosDB adopts it in milestone 5 as one change.

Existing partitions store `__partition_context`, `__split_status`, `__split_migration_status`,
`__split_migration_cursor`, `__topo_cache`, `__partial_range_topology`, and the tables `promoted_keys` and
`range_hierarchy`. The runtime does not read these. Open question 5.3.2 decides between a one-time converter
and a fresh start.

Rollback before milestone 5 is a revert of an unused package. Rollback after milestone 5 is a revert of the
FokosDB host change; it is safe only when 5.3.2 chose a converter that leaves the old keys in place.

#### 5.2.24 Testing

- The example host from milestone 2 lives in the package tests. It imports nothing from FokosDB. A build guard
  like `check-client-bundle` in `packages/fokosdb/tsdown.config.ts` fails when the runtime entry reaches a
  module below `src/server/` or `src/shared/partition/`.
- Unit tests for the codec, the caches, arbitration, and both state machines move from the files in section 25
  of the audit with their cases intact.
- A property test checks that `belongsToTarget(key)` equals `resolveOwner(key).target` for every target of a
  plan, for random keys, for hash and range plans.
- Integration tests in the Workers runtime drive: hash split with a crash after `fokosInit`; target crash after
  `imported`; ack from a non-member; Bloom false positive on an uninitialized root and on an `awaiting_data`
  root; a promotion queued while a split is `queued`; a `scan` across three range children during import.
- The FokosDB suites listed in section 25 of the audit pass without change to their assertions after
  milestone 5. Helpers such as `TestPartition`, `triggerHashSplit`, and `withMigrationHeld` are rewritten over
  the runtime state.

### 5.3 Open Questions

#### 5.3.1 Package boundary

Options: a new entry `fokosdb/partition-runtime` in the existing package, or a separate npm package. A separate
package gives a clean dependency check and its own version. A new entry is less publishing work and shares the
build. The answer changes the build configuration only.

#### 5.3.2 Existing data

Options: a one-time converter that the FokosDB host runs on first wake when `__fokos/identity` is absent and
`__partition_context` is present; or no converter, with existing tables recreated. The converter must map
`__split_status` and `promoted_keys` into repartition rows and the `migration_*` keys into an import record.
The answer changes rollback safety (section 5.2.23) and the size of milestone 5.

#### 5.3.3 Bloom step for group operations

Today transaction routing skips the promotion Bloom filter. This design keeps that. A group operation could use it
and treat `fokos_not_owner_yet` and `fokos_not_initialized` on a speculative group as a per-group retry without
the Bloom step. The answer changes hop count for transactions on promoted keys only.

#### 5.3.4 Source cleanup for splits

The runtime supports `cleanupSourceStep` for every kind. FokosDB today keeps split source rows forever. The
FokosDB host can return `undefined` for splits and keep that behavior, or reclaim the rows. The answer is a FokosDB
decision, not a runtime one.

#### 5.3.5 Public ownership strategy

The hash and range strategies sit behind one internal contract. Publishing that contract lets a host bring a
different ownership model. It also freezes an API before a second implementation exists. The answer changes
nothing for FokosDB.

---

## 6. Alternative Options

**Base Durable Object class.** A `FokosPartitionDO` base class with protected hooks. Rejected as the primary
model because many hosts already extend another base class, for example the Agents SDK, and JavaScript has single
inheritance. The runtime can be wrapped by such a base class later.

**Class decorator or RPC dispatcher.** A decorator that wraps every public method with the pipeline. Rejected
because the shapes differ per operation and the decorator would need the same descriptor data; the explicit
`dispatch` call is one line and carries the descriptor name.

**Generated wrappers.** Code generation from a schema of RPC methods. Rejected for now: it adds a build step
and a schema language for a gain that the registry already gives.

**Topology-only extraction.** Move only `shared/partition-topology/` into a package. Rejected because the audit
shows that the sharding code depends on FokosDB rows for migration, boundaries, promotion, and cleanup. Without
the hook boundary the package cannot run a split.

**One RPC method per migrated record kind.** Keep three migration RPCs. Rejected because it forces the runtime
to know host record kinds and to persist one cursor per kind. One opaque page and one cursor remove both.

**Separate orchestrators for split and promotion.** Keep `SplitStateMachine` and `PromotionManager`. Rejected
because both need the same durable stages, the same acknowledgement retry, and the same arbitration. One table
makes the mutual exclusion atomic.

---

## 7. Frequently Asked Questions

**Why does the host register operations instead of passing closures per call?**
The runtime must run the host's local handler on a source partition when a target reads through during import.
It needs the handler by name. A registry also gives one place for the shape, the intent, and the signals.

**Why is `importPage` synchronous?**
The runtime commits the page and the cursor in one `transactionSync`. SQLite and KV in Durable Objects are
synchronous, so a host import that writes storage needs no `await`. Atomic commit removes the case where the
page is applied and the cursor is lost.

**Why does the target keep a separate `imported` state?**
So that the acknowledgement can be retried after a crash. Today the child writes completion and then
acknowledges; a crash between the two leaves the parent split open forever.

**Why does the runtime reject pulls while the source is `queued`?**
Targets are created before cutover. A target's own fallback alarm can fire first. Exporting data before the
cutover is durable would let a target serve a key that the source still owns.

**Why does every request still carry topology and policy?**
A Durable Object has no other way to learn them. The constructor receives no parameters, and a library host
does not know at build time how many shard groups exist, how many roots each one has, or which binding name the
user chose. Carrying them lets the Worker decide per shard group and per tenant at run time, with no code in the
Durable Object. The cost is a few integers and one small object per request.

**How does a partition know the topology when a background job runs and no request is in flight?**
It stored the route context at bootstrap or `fokosInit`, and it replaces the policy part whenever a request
carries a different value. Background jobs read the stored value through `runtime.routeContext()`.

**Why is the policy opaque instead of a typed set of thresholds?**
Different hosts need different policy. FokosDB needs two split condition sets and two binding keys. Another host
needs a row count or a tenant tier. The runtime only needs to store, compare, and forward the value.

**Does the host still see routing metadata in its responses?**
Yes, through `FokosEnvelope.route`. The host's own response type has no routing fields. FokosDB maps
`route.servedBy`, `hashDepth`, `rangeDepth`, and `forwardCount` into its public `PartitionInfo`.

**How does a host with no range partitions use the runtime?**
It omits `computeRangeBoundaries`, sets `promotionBloom: false`, and never returns `promotionCandidates`. Only
hash splits happen. The `scan` shape is unused.

---

## 8. References

- `docs/ideas/fokos-sharding/gptsol-existing-behavior.md`
- `docs/ideas/fokos-sharding/gemini-existing-flows-spec.md`
- `packages/fokosdb/src/server/do-partition.ts`
- `packages/fokosdb/src/shared/partition/partition-peer.ts`
- `packages/fokosdb/src/shared/partition-errors.ts`
- `packages/fokosdb/src/shared/partition-topology/partition-context.ts`
- `packages/fokosdb/src/shared/partition-topology/partition-id.ts`
- [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
- [Durable Objects Alarms API](https://developers.cloudflare.com/durable-objects/api/alarms/)
- [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Workers RPC](https://developers.cloudflare.com/workers/runtime-apis/rpc/)
- [Distributed Transactions at Scale in Amazon DynamoDB (USENIX ATC 2023)](https://www.usenix.org/system/files/atc23-idziorek.pdf)
