# RFC — Composable sharding for Durable Objects

**State:** Draft  
**Date:** 2026-09-09  
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

FokosDB stores data in Durable Objects. A partition starts as one Durable Object and splits when it
reaches an application limit.

The current `PartitionDO` also owns routing, topology caches, split policy, promotion, migration, and
background recovery. This design couples the sharding system to FokosDB rows and transactions.

The sharding system must become a separate package. Other Durable Object applications must be able to
use it without adopting the FokosDB data model.

The extraction must happen before another application copies the current lifecycle code. Separate copies
would make routing and recovery fixes diverge.

The package must use composition. A custom Durable Object instantiates the runtime in its constructor.
The custom class keeps its existing base class, storage, RPC methods, and framework behavior.

The package does not provide a required base class. A future package can provide an optional base class.
This future class must use the same composition API.

### 1.1 Current constraints

Cloudflare exposes public methods on a Durable Object as RPC methods. RPC values must use supported
serializable types.

A nested runtime object does not expose methods through the host Durable Object stub. The host class must
therefore expose a small control-plane RPC surface.

A Durable Object has one native alarm. Another base class or SDK can already use that alarm. The sharding
runtime must not assume exclusive alarm ownership.

The custom Durable Object can access all Durable Object capabilities. This includes SQL, key-value
storage, WebSockets, outbound calls, and framework APIs.

### 1.2 Glossary

| Term | Meaning |
| --- | --- |
| Application | The custom Durable Object and its business logic. |
| Runtime | One `FokosSharding` instance inside a custom Durable Object. |
| Partition | One Durable Object that owns or routes part of an application keyspace. |
| Root | A partition that the Worker can select without live topology information. |
| Source | A partition that transfers ownership during a repartition. |
| Target | A partition that receives ownership during a repartition. |
| Repartition | A hash split, a range split, or one partition-key promotion. |
| Route override | A rule that sends one promoted partition key to a range root. |
| Application payload | Opaque application data in one migration page. |
| Control state | Runtime-owned identity, topology, migration, acknowledgement, and schedule state. |
| Host | The custom Durable Object class that owns the runtime. |

## 2. Goals and requirements

### 2.1 In scope

The first package version must:

- work through composition with any compatible SQLite-backed Durable Object class;
- let the host keep its current base class;
- let the host keep direct access to `DurableObjectState`, environment bindings, and storage;
- keep application request and response types outside the runtime;
- support root hash routing and recursive hash splits;
- support optional partition-key promotion into a range tree;
- support recursive range splits by sort key;
- support point operations, grouped operations, single-owner operations, and ordered range traversal;
- support a single opaque application migration-page protocol;
- support application policy before local work and after each routed request;
- support application policy for repartition selection and cutover;
- preserve FokosDB transaction routing and recovery behavior;
- use durable state before each outbound control-plane RPC;
- recover every non-terminal repartition after an object restart;
- keep routing caches optional for correctness;
- expose all required host control RPC through a `fokos`-prefixed method;
- support an alarm adapter for hosts that already use the native alarm;
- keep the Worker-side bundle free of the server runtime.

### 2.2 Out of scope

The first package version does not include:

- a required Durable Object base class;
- automatic interception of arbitrary host RPC methods;
- arbitrary topology algorithms other than the built-in hash and range strategies;
- concurrent writes to an incomplete target;
- dual writes between a source and a target;
- migration through a central coordinator;
- a global topology-keeper Durable Object;
- cross-namespace repartition targets;
- the legacy key-value Durable Object storage backend;
- application transaction semantics;
- application row encoding;
- application schema migration;
- automatic integration with every SDK alarm implementation.

A future package can add a base class, more topology strategies, or SDK-specific scheduler adapters.

### 2.3 Required host behavior

The host must route every ownership-sensitive application operation through `FokosSharding`.
This rule includes alarm tasks, WebSocket handlers, and framework callbacks that mutate partitioned data.

The host must use a compatibility date of `2024-04-03` or later, or it must enable the `rpc` flag.

The host must expose this RPC method:

```ts
fokosRpc(request: FokosRpcRequest<AppTypes>): Promise<FokosRpcResponse<AppTypes>>;
```

The method must delegate directly to the runtime. All control operations use this one method.

The host must arrange calls to `fokosRunDueWork`. The default integration calls it from `alarm()`.
A host with another alarm owner must use a scheduler adapter or an alarm multiplexer.

The host must make every migration import, finalizer, cleanup callback, and application job idempotent.

The host must use one canonical byte encoding for each partition key and sort key.

### 2.4 Compatibility requirements

The FokosDB public API must keep its current behavior.

The FokosDB Durable Object class names and deterministic partition names must stay stable during the
migration.

The client entry must not import a Durable Object class as a runtime value.

A transaction coordinator must be able to keep a root partition reference across later splits.

A source partition must remain a valid routing entry after it repartitions.

## 3. Milestones

### 3.1 Package shell and composition API

Create `fokos-sharding` with separate client and server entries. Add the type model, state store,
`FokosSharding`, `FokosRouter`, and the `fokosRpc` control union.

This milestone includes contract tests with a small custom Durable Object.

### 3.2 Hash routing and opaque migration

Add root routing, recursive hash splits, request leases, target initialization, the migration-page loop,
acknowledgement recovery, and source cleanup.

This milestone supports point operations and grouped operations without route caches.

### 3.3 Promotion, range routing, and caches

Add exact partition-key promotion, recursive range splits, ordered range traversal, route overrides, and
the current cache strategies.

This milestone also transfers runtime-owned route overrides during a later hash split.

### 3.4 Scheduling, administration, and failure recovery

Add the scheduler adapter, alarm work loop, status traversal, destruction, structured errors, and fault
injection tests.

### 3.5 FokosDB adoption

Instantiate `FokosSharding` inside `PartitionDO`. Move routing and migration behavior into the package.
Keep the item store and transaction participant in FokosDB.

Run all existing tests after each FokosDB operation moves to the runtime.

### 3.6 Legacy cleanup

Remove old sharding code and compatibility methods only after the new runtime reads all supported legacy
states. Keep the package boundary and bundle checks after cleanup.

## 4. Proposed solution

### 4.1 High-level overview

Each custom Durable Object creates one `FokosSharding` instance in its constructor.

The runtime owns the control plane:

- partition identity;
- routing topology;
- route caches;
- repartition state;
- migration progress;
- target acknowledgements;
- background-work deadlines.

The host owns the data plane:

- application RPC methods;
- local request behavior;
- request admission;
- application migration payloads;
- source cleanup;
- application background jobs.

The host passes local callbacks to the runtime. The callbacks can use the complete Durable Object API.
The runtime never serializes these callbacks or sends them over RPC.

The host exposes one `fokosRpc` method. Runtime instances use that method for all control-plane calls.
Application forwarding still calls the host's application RPC methods.

```text
Worker
  |
  | application RPC + FokosRequestContext
  v
Custom Durable Object
  |
  | owns
  +-- application storage and business methods
  |
  +-- FokosSharding
        |
        +-- route or handle locally
        +-- initialize targets through fokosRpc
        +-- pull migration pages through fokosRpc
        +-- acknowledge through fokosRpc
        +-- schedule durable recovery
```

A source writes the routing cutover before it sends new operations to targets. A target rejects mutations
until application migration and finalization complete.

The application migration protocol has one page operation. The application cursor can encode any number
of internal application phases.

### 4.2 Package entries

The package name in this RFC is `fokos-sharding`.

The package exports:

| Entry | Contents |
| --- | --- |
| `fokos-sharding/client` | `FokosRouter`, context creation, public result helpers, and client types. |
| `fokos-sharding/server` | `FokosSharding`, scheduler helpers, errors, and server types. |

The client entry must not import the server entry or a host Durable Object class as a runtime value.

The package does not export a base Durable Object class in the first version.

### 4.3 Composition model

The host owns the `FokosSharding` instance as a private field.

```ts
export class DocumentDO extends ExistingBaseClass<Env> implements FokosControlPlane<AppTypes> {
	readonly #fokos: FokosSharding<AppTypes, DocumentPeer>;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.#fokos = new FokosSharding({
			fokosState: ctx,
			fokosMigrationPageBytes: MIGRATION_PAGE_BYTES,
			fokosMaxRpcConcurrency: 4,
			fokosRetryPolicy: CONTROL_RETRY_POLICY,
			fokosGetPeer: (partition) => env.DOCUMENTS.getByName(partition.doName),
			fokosScheduler: createDocumentScheduler(ctx),
			fokosCallbacks: {
				fokosValidateApplicationConfig: (input) => this.#fokosValidateApplicationConfig(input),
				fokosCachePolicy: (input) => this.#fokosCachePolicy(input),
				fokosAdmitLocal: (input) => this.#fokosAdmitLocal(input),
				fokosPlanRepartition: (input) => this.#fokosPlanRepartition(input),
				fokosCanCutover: (input) => this.#fokosCanCutover(input),
				fokosReadMigrationPage: (input) => this.#fokosReadMigrationPage(input),
				fokosApplyMigrationPage: (input) => this.#fokosApplyMigrationPage(input),
				fokosFinalizeMigration: (input) => this.#fokosFinalizeMigration(input),
				fokosCleanupSourcePage: (input) => this.#fokosCleanupSourcePage(input),
				fokosInvokeSource: (input) => this.#fokosInvokeSource(input),
				fokosAfterRequest: (input) => this.#fokosAfterRequest(input),
			},
		});
	}

	fokosRpc(request: FokosRpcRequest<AppTypes>): Promise<FokosRpcResponse<AppTypes>> {
		return this.#fokos.fokosRpc(request);
	}
}
```

The host uses JavaScript private methods for callback implementations. Workers RPC therefore exposes only
the intentional application methods and `fokosRpc`.

The runtime calls `ctx.blockConcurrencyWhile` in its constructor to load only runtime-owned state.
The host can use another constructor gate for its own initialization.

The runtime does not call application callbacks during its constructor gate.

The host supplies `fokosMigrationPageBytes`. The package has no default until the RPC limit question is
resolved.

`fokosMaxRpcConcurrency` defaults to 4. The current Durable Object limit is 6 simultaneous outgoing
connections per request. The reserved capacity lets application callbacks use two other connections.
The host can lower the value and must not set it above the current platform limit.

`fokosRetryPolicy` returns the next absolute deadline for a failed control job. Its input contains the job
kind and consecutive failure count. The package exports an exponential policy builder, but the host must
supply its base delay and maximum delay. FokosDB uses its current migration and split retry values.

### 4.4 Application type map

One type map groups the application-specific wire types.

```ts
export interface FokosApplicationTypes {
	partitionConfig: unknown;
	policyTag: unknown;
	repartitionSignal: unknown;
	repartitionData: unknown;
	migrationCursor: unknown;
	migrationPayload: unknown;
	cleanupCursor: unknown;
	sourceRequest: unknown;
	sourceResponse: unknown;
	applicationJob: unknown;
}
```

Each concrete type that crosses RPC or enters runtime storage must be serializable by Workers RPC.
The types must not use custom prototypes unless Workers RPC supports them explicitly.

The runtime treats all application fields as opaque values.

### 4.5 Canonical routing key

The built-in topology uses this key shape:

```ts
export type FokosRoutingKey = {
	partitionKey: Uint8Array;
	sortKey?: Uint8Array;
};
```

The application encodes public keys before it calls the runtime.

The partition key selects a root and a hash child. The optional sort key selects a range child after a
partition-key promotion.

An application without a sort key omits `sortKey`. Such an application can disable promotion and range
splits.

The runtime compares encoded bytes only. It does not decode application keys.

### 4.6 Topology configuration and request context

The Worker creates one immutable topology configuration for a logical group.

```ts
export type FokosTopologyConfig = {
	schema: 1;
	groupName: string;
	rootPartitions: number;
	hashFanout: number;
};

export type FokosApplicationConfig<App extends FokosApplicationTypes> = {
	version: string;
	value: App["partitionConfig"];
};

export type FokosRequestContext<App extends FokosApplicationTypes> = {
	topology: FokosTopologyConfig;
	partition: FokosPartitionRef;
	applicationConfig: FokosApplicationConfig<App>;
};
```

`groupName` separates independent applications, tenants, or tables that use the same Durable Object
class.

The root count and hash fan-out must stay unchanged after the first root initializes.

A range split stores its target count and boundaries in its repartition plan. The application can select
a different valid target count for a later range split.

The runtime treats `applicationConfig.value` as opaque. The host changes `version` whenever the value
changes. Each partition stores the latest accepted version for background work. The value must not
contain secrets because it crosses RPC and enters partition storage.

The context can contain split thresholds, range fan-out, cache policy, transaction settings, or other host
configuration. The runtime does not inspect those fields.

When the version is unchanged, the runtime uses the stored value and ignores the incoming value. The host
must change the version for every value change.

When a version changes, the runtime calls `fokosValidateApplicationConfig` with the stored and incoming
values. It persists the incoming value only when the callback returns `accept`. A `reject` result returns
`FOKOS_CONTEXT_MISMATCH`.

A target initialization copies the source's latest accepted application configuration. A later normal
request can update that target independently through the same version check.

### 4.7 Partition identity

The first package version keeps the current opaque partition ID concepts:

- a hash partition contains a root index and a hash path;
- a range partition contains a partition key and immutable sort-key boundaries;
- a partition name is deterministic from `groupName` and the partition ID;
- a range partition owns `[start, end)`;
- a former owner keeps its Durable Object identity as a router.

The hash fan-out and each range target count must be between 2 and 255.
The root count must be between 1 and 65,000.

The runtime stores the resolved identity on the first valid request. Later contexts must match it.

A root hash partition can initialize lazily. Every non-root partition must initialize through
`fokosRpc({ kind: "initialize" })`.

A request to an uninitialized non-root partition returns `FOKOS_PHANTOM_PARTITION`.

The Worker uses `FokosRouter.fokosPickRoot` to select an initial root.

```ts
const fokosRouter = new FokosRouter<AppTypes, DocumentPeer>({
	fokosTopology,
	fokosApplicationConfig,
	fokosNamespace: env.DOCUMENTS,
});
const { fokosContext, fokosPeer } = fokosRouter.fokosPickRoot(key);
const result = await fokosPeer.put(fokosContext, request);
```

The router also provides `fokosRootContexts` and `fokosTraverse`. It does not need live split state for
normal requests.

The root hash function and partition ID codec must stay compatible with current FokosDB identities.
Hash child routing uses the absolute hash depth and the configured hash fan-out.

### 4.8 Constructor callbacks

The runtime constructor receives these callback groups.

```ts
export interface FokosCallbacks<App extends FokosApplicationTypes> {
	fokosValidateApplicationConfig(input: FokosConfigInput<App>): MaybePromise<"accept" | "reject">;
	fokosCachePolicy(input: FokosCachePolicyInput<App>): FokosCachePolicy;
	fokosAdmitLocal(input: FokosAdmissionInput<App>): MaybePromise<FokosAdmission>;
	fokosPlanRepartition(
		input: FokosPlanInput<App>,
	): MaybePromise<FokosRepartitionProposal<App["repartitionData"]> | null>;
	fokosCanCutover(input: FokosCutoverInput<App>): MaybePromise<"ready" | "defer">;
	fokosReadMigrationPage(
		input: FokosReadMigrationInput<App>,
	): MaybePromise<FokosApplicationMigrationPage<App>>;
	fokosApplyMigrationPage(input: FokosApplyMigrationInput<App>): MaybePromise<void>;
	fokosFinalizeMigration(input: FokosFinalizeMigrationInput<App>): MaybePromise<void>;
	fokosCleanupSourcePage?(
		input: FokosCleanupSourceInput<App>,
	): MaybePromise<FokosCleanupSourceResult<App>>;
	fokosInvokeSource(input: FokosSourceInvocation<App>): MaybePromise<App["sourceResponse"]>;
	fokosAfterRequest?(input: FokosAfterRequestInput<App>): MaybePromise<void>;
	fokosOnLifecycleEvent?(event: FokosLifecycleEvent<App>): MaybePromise<void>;
	fokosRunApplicationJob?(
		input: FokosApplicationJobInput<App>,
	): MaybePromise<FokosApplicationJobResult<App>>;
	fokosImportLegacyState?(input: FokosLegacyImportInput): MaybePromise<FokosLegacyImportResult<App> | null>;
	fokosMirrorControlTransition?(input: FokosControlTransitionInput<App>): MaybePromise<void>;
	fokosBeforeDestroy?(): MaybePromise<void>;
}
```

`MaybePromise<T>` means `T | Promise<T>`.

The runtime invokes critical callbacks through explicit state-machine steps. Critical callback errors keep
the step non-terminal and schedule a retry.

`fokosImportLegacyState` runs on the first runtime operation when runtime identity is absent. This happens
after constructor gates complete. The runtime validates its result before it writes imported state.

`fokosMirrorControlTransition` is a critical compatibility hook. The runtime calls it before the matching
runtime write and while it holds the applicable request lease. The callback must be idempotent and must
use local storage only. Its error blocks the new transition. FokosDB uses it only while rollback
compatibility writes are enabled.

`fokosAfterRequest` and `fokosOnLifecycleEvent` are observation hooks. They can update application
telemetry or schedule application work. They must not mutate partitioned application data. Their errors
do not change a completed result or transition. Correctness-critical work must use a critical callback or
an application job.

The server entry exports `fokosAcceptConfig`, `fokosDisableCaches`, `fokosAllowAll`,
`fokosNoRepartition`, `fokosNoMigration`, and `fokosNoSourceReads` adapters. A host can use them when a
capability has no application work.

### 4.9 Required control-plane RPC

The host implements one public method named `fokosRpc`.

```ts
export interface FokosControlPlane<App extends FokosApplicationTypes> {
	fokosRpc(request: FokosRpcRequest<App>): Promise<FokosRpcResponse<App>>;
}
```

`FokosRpcRequest` is a discriminated union.

```ts
export type FokosRpcRequest<App extends FokosApplicationTypes> =
	| { kind: "initialize"; request: FokosInitializeRequest<App> }
	| { kind: "migration_page"; request: FokosMigrationPageRequest<App> }
	| { kind: "migration_ack"; request: FokosMigrationAckRequest }
	| { kind: "source"; request: FokosSourceRequest<App> }
	| { kind: "status"; context?: FokosRequestContext<App> }
	| { kind: "run_due_work" }
	| { kind: "destroy"; context?: FokosRequestContext<App> };
```

The response is a matching discriminated union. The package peer helper checks that the response kind
matches the request kind.

| Kind | Main request fields | Response | Retry rule |
| --- | --- | --- | --- |
| `initialize` | Source, target, repartition ID, plan data | Stored target state | Runtime retries with a fresh peer. |
| `migration_page` | Plan, target, cursors, budget | Migration page | Target retries the same cursor. |
| `migration_ack` | Repartition ID and target identity | Recorded acknowledgement | Target retries until success. |
| `source` | Plan, target, read request | Read response | Application read policy owns retries. |
| `status` | Optional root bootstrap context | Partition status | Traversal retries transport failures. |
| `run_due_work` | None | Work summary and next deadline | Scheduler invokes it again when needed. |
| `destroy` | Optional root context | `FOKOS_DESTROYED` | Traversal treats the code as success. |

The runtime validates the source repartition ID and target identity for every migration and source call.
It rejects unknown targets before it invokes an application callback.

All required control-plane operations therefore use a `fokos`-prefixed RPC method. The unprefixed
`alarm()` method remains a Cloudflare system handler, not a package RPC.

### 4.10 Application point operation

A host point RPC calls `fokosRouteOne`.

```ts
async put(context: FokosRequestContext<AppTypes>, request: PutRequest): Promise<FokosRouted<PutResult>> {
	return this.#fokos.fokosRouteOne({
		fokosContext: context,
		fokosKey: request.key,
		fokosAccess: "mutate",
		fokosPolicyTag: "put",
		fokosDuringMigration: { mode: "retry" },
		fokosLocal: () => this.putLocal(request),
		fokosForward: (peer, target) => peer.put({ ...context, partition: target }, request),
		fokosRepartitionSignal: (result) => ({ kind: "put", result }),
	});
}
```

`fokosRouteOne` owns context validation, the migration gate, route overrides, split routing, cache hints,
the request lease, admission, and the post-request hook.

The runtime calls `fokosLocal` only when the current partition owns the key.

The runtime calls `fokosForward` with a fresh peer and the selected target context.
The callback must invoke the same application operation on the target.

The runtime does not retry `fokosForward`. It cannot assume that the application operation is idempotent.

### 4.11 Grouped and single-owner operations

A host multi-key RPC calls `fokosRouteMany`.

```ts
export type FokosRouteManyMode = "fanout" | "single_owner";
```

The operation supplies:

- the input items;
- one routing key for each item;
- one policy tag;
- local execution for a local group;
- remote execution for each remote group;
- a result combiner;
- a mode;
- an optional repartition signal for a successful local group.

In `fanout` mode, the runtime resolves every item and groups items by target. It dispatches every remote
group through the bounded RPC executor. It runs the local group under the request lease and waits for all
settled results. It gives those results to the application combiner.

A local group failure does not prevent calls to remote groups. The application combiner decides whether
to return a rejection, throw an error, or merge successful results.

In `single_owner` mode, the runtime verifies that one final partition owns all items. It returns
`FOKOS_SINGLE_OWNER_REQUIRED` before any application callback when the items have more than one owner.

Transaction prepare, commit, cancel, and read use `fanout` mode. Single-shot transactions and local
snapshot reads use `single_owner` mode.

### 4.12 Routed response envelope

Application routing methods return an internal envelope.

```ts
export type FokosRouted<T> = {
	value: T;
	fokos: {
		servedBy: FokosPartitionRef;
		forwardCount: number;
		routeHint?: FokosRouteHint;
	};
};
```

A local runtime creates the envelope. Each forwarder increments `forwardCount` and preserves the serving
partition.

The route hint is runtime-owned. The application must not use it for business logic.

A Worker client can call `fokosUnwrap` when it does not expose routing metadata. FokosDB maps selected
envelope fields into its current operation metadata.

### 4.13 Request routing order

A routed operation uses this order:

1. Validate or initialize the partition context.
2. Get a shared request lease.
3. Check destination migration state.
4. Apply an exact partition-key route override.
5. Apply a hash or range split route.
6. Validate local ownership.
7. Call `fokosAdmitLocal`.
8. Call the local application callback.
9. Create a local route hint.
10. Process an optional repartition signal.
11. Release the request lease.
12. Call `fokosAfterRequest`.

When a step selects a target, the runtime releases the lease before the outbound application RPC.
The target repeats the same routing order.

A cache can select a deeper target before step 4 only when its hint preserves all route authorities.
The target still validates its identity and ownership.

### 4.14 Request lease and async application methods

The runtime uses an in-memory read-write gate for ownership-sensitive work.

A local application operation holds a shared lease from route resolution through local completion.
The callback can await storage, network, SDK, or application operations while it holds this lease.

A routing cutover needs the exclusive lease. The cutover waits for all prior local operations.
New local operations wait while the cutover persists.

This gate prevents this race:

1. A request decides that the source owns a key.
2. The request awaits application I/O.
3. A background job cuts routing over to a target.
4. The request writes to the old source.

An object restart cancels all in-memory operations and rebuilds the gate. Durable source state still
shows whether cutover completed.

The gate protects only operations that use the runtime. A host method that bypasses the runtime can break
ownership.

### 4.15 Application admission

`fokosAdmitLocal` runs after core ownership checks and before local application work.

```ts
export type FokosAdmission =
	| { action: "allow" }
	| { action: "retry"; code: string; message: string; retryAfterMs?: number }
	| { action: "reject"; code: string; message: string };
```

The input contains:

- the partition identity;
- the operation policy tag;
- the routing keys;
- the partition lifecycle state;
- the current application configuration;
- the current repartition, when present.

FokosDB uses this callback for size backpressure. It allows reads, deletes, commits, and cancels while an
over-size leaf rejects growing writes.

The generic runtime does not define FokosDB operation intents or storage thresholds.

### 4.16 Post-request behavior

A routed operation can provide `fokosRepartitionSignal`. The runtime calls it only after successful local
work.

The runtime gives the signal to `fokosPlanRepartition`. Its input includes the partition identity, current
application configuration, current transitions, and trigger signal. The callback can return no plan or
one proposal. The runtime validates and persists a valid proposal.

A planning error does not replace a successful application result. The runtime logs the error and
schedules a background evaluation.

`fokosAfterRequest` runs once for each runtime invocation on a partition. Its input identifies:

- the operation tag;
- local, forwarded, source-read, rejected, or failed handling;
- the result class without application payload data;
- the elapsed time;
- the current partition identity.

The hook is observational. It cannot change routing state or the application result.

### 4.17 Built-in repartition proposals

The package exports builders for three plans.

```ts
fokosHashSplit({ data });
fokosRangeSplit({ boundaries, data });
fokosPromotePartitionKey({ partitionKey, data });
```

`data` is the opaque application repartition data.

The runtime snapshots the latest accepted application configuration in the plan. Migration and cleanup
callbacks receive this snapshot. A later configuration update does not change an in-flight plan.

A hash partition can start a hash split or a partition-key promotion.
A range partition can start a range split.

The runtime validates:

- fan-out bounds;
- target identity uniqueness;
- range boundary order;
- exact range coverage;
- plan compatibility with the source partition;
- encoded plan size against the control-record limit;
- no overlap with a non-terminal plan.

The runtime permits multiple non-terminal promotions when their partition keys differ. Each promotion has
its own source transition record.

The runtime permits one non-terminal full split. A queued full split blocks new promotions. Promotions
that already exist must reach `completed` before the runtime initializes full-split targets. Their source
cleanup can continue after target initialization because the migration helper excludes their keys.

A new promotion receives `FOKOS_REPARTITION_CONFLICT` while a full split is queued or active. An
overlapping promotion receives the same code. These conflicts do not change the application result.

The runtime schedules a policy evaluation after it archives the plan that caused a conflict. The planning
callback must be able to rediscover pending work from application state.

One durable arbitration record applies these rules. This removes the current mutual-exclusion window.

### 4.18 Source repartition state machine

The source stores one record for each non-terminal or routing-active repartition. The following state
machine applies to each record.

```text
none
  |
  | valid proposal
  v
queued
  |
  | all targets initialized
  | fokosCanCutover = ready
  | exclusive request lease
  v
active
  |
  | all targets acknowledged
  v
completed
  |
  | optional source cleanup completed
  v
archived
```

`queued` means that the source still owns the selected keyspace. Normal operations can use the source.

The runtime can initialize targets while the source is `queued`. A target cannot read migration pages
until the source is `active`.

The runtime calls `fokosCanCutover` while it holds the exclusive request lease. The callback can return
`defer`. It must not make external network calls.

The runtime writes `active` before it releases the exclusive lease. From that point, selected operations
route to targets.

`completed` means that every target imported data, finalized, and acknowledged.
Routing behavior is the same in `active` and `completed`.

For a promotion, the runtime writes the terminal route override as part of the `completed` transition.
A later full split can transfer that override while promotion cleanup continues.

`archived` means that optional source cleanup completed. The runtime keeps the terminal routing rule.

A full split makes the source a permanent router. A promotion keeps all non-selected partition keys local.

### 4.19 Target migration state machine

Each target stores one migration record.

```text
uninitialized
  |
  | valid and idempotent initialize RPC
  v
copying
  |
  | application page done
  | runtime metadata done
  | finalizer succeeded
  v
ack_pending
  |
  | parent accepted acknowledgement
  v
ready
```

`fokosRpc({ kind: "initialize" })` writes `copying` and schedules durable work before it returns.
The source does not need a second trigger RPC.

A target in `copying` rejects mutations. A read can use the optional source-read path.

The runtime calls `fokosFinalizeMigration` after the last page. The callback must be idempotent.
The runtime writes `ack_pending` only after the callback succeeds.

A target in `ack_pending` has complete application state. It can handle local operations.
It retries the parent acknowledgement until the parent returns success.

The runtime writes `ready` after a successful acknowledgement response. If the response is lost, the
runtime repeats the idempotent acknowledgement.

This state machine removes the current completion-before-acknowledgement crash gap.

### 4.20 Target initialization

The source calculates every target context before it sends initialization RPCs.

Target initialization contains:

- source context;
- source repartition ID;
- target context;
- repartition kind;
- application repartition data;
- target-specific ownership data.

Initialization is idempotent when every identity field matches stored state.
It returns `FOKOS_CONTEXT_MISMATCH` when a field conflicts.

The source tries all target initializations through the bounded RPC executor. A failed target stays
pending. A later pass creates a fresh stub and retries that target.

The source does not activate routing until every target reports successful initialization.

### 4.21 One application migration-page protocol

The target pulls pages from the source through `fokosRpc({ kind: "migration_page" })`.

After source cutover, selected mutations route to the target and receive a retry while it copies. The
request lease drains older source operations before cutover. The selected source data is therefore stable
for the page scan.

A host background task that mutates partitioned data must run through `fokosRunApplicationJob`. The
runtime holds a shared request lease and gives the callback the current lifecycle state and ownership
helper. The callback must check each affected key before it mutates application data.

```ts
export type FokosApplicationMigrationPage<App extends FokosApplicationTypes> =
	| {
			done: false;
			payload?: App["migrationPayload"];
			payloadBytes: number;
			nextCursor: App["migrationCursor"];
	  }
	| {
			done: true;
			payload?: App["migrationPayload"];
			payloadBytes: number;
	  };
```

The source callback receives:

- the source partition;
- the target partition;
- the persisted repartition plan;
- the previous application cursor;
- the maximum application payload bytes;
- a runtime ownership helper for the target.

The callback returns one bounded payload and its estimated wire size. The runtime rejects a page when
`payloadBytes` exceeds the application budget. The callback can encode any number of application phases
in its cursor.

The host must use the runtime ownership helper for every migrated application key. For a full hash split,
the helper excludes a partition key that already has a terminal route override. The target receives the
override record but does not receive a redundant application copy.

FokosDB can use these private cursor phases:

- item rows;
- transaction rows and deletion metadata;
- completion.

The package does not define these phases or separate RPC methods for them.

The target applies one payload through `fokosApplyMigrationPage`. This includes a payload in a `done`
page. The callback must accept a repeated payload without data loss or invalid overwrite.

The runtime persists `nextCursor` after the apply callback succeeds. A crash before the checkpoint causes
a repeated page.

The application sets `done: true` only when all required application state is present in the target.

### 4.22 Runtime metadata migration

The runtime can own terminal route overrides for promoted partition keys.

A later full hash split must move each override to the hash target that owns its partition key.
This lets a cached route enter through the target without bypassing the override.

The runtime includes its own bounded metadata in the migration envelope. It tracks a runtime cursor that
is separate from the opaque application cursor.

The runtime reserves envelope and metadata bytes before it gives a page budget to the application. The
combined estimated page size must not exceed `fokosMigrationPageBytes`.

The host sees one migration-page protocol. It does not read or write runtime metadata.

The target reaches `ack_pending` only after both runtime metadata and application data complete.

### 4.23 Source-read path during migration

A read operation can select this migration behavior:

```ts
fokosDuringMigration:
	| { mode: "retry" }
	| {
			mode: "source_read";
			request: App["sourceRequest"];
			fokosMapResponse: (response: App["sourceResponse"]) => Result;
	  };
```

The runtime permits `source_read` only for an operation marked `read`.

The target sends the opaque request through `fokosRpc({ kind: "source" })`.
The source validates the active repartition and target identity.

The source calls `fokosInvokeSource` without normal routing. The callback reads source-local state.
This rule prevents a forwarding loop.

A write operation uses `retry` while the target is `copying`. Online dual-write migration is out of scope.

### 4.24 Source cleanup

Source cleanup starts only after every target acknowledges complete migration.

The runtime calls `fokosCleanupSourcePage` with an opaque cleanup cursor and a work budget.
The callback returns one of these results:

```ts
type FokosCleanupSourceResult<App extends FokosApplicationTypes> =
	| { done: false; nextCursor: App["cleanupCursor"] }
	| { done: true };
```

The callback must be idempotent. The runtime checkpoints after each successful cleanup step.

A full split can keep source application data by omitting the callback or returning `done` immediately.
A promotion can delete migrated source data in bounded pages.

Routing never returns to the source when cleanup fails. The runtime keeps the terminal route and retries
cleanup.

### 4.25 Ordered range traversal

The package provides `fokosRouteRange` for an operation that must visit all range partitions that
intersect an interval.

The operation supplies:

- one partition key;
- lower and upper sort-key bounds;
- direction;
- application byte and item budgets;
- a partition-visit budget;
- an application cursor;
- local scan, forward, and merge callbacks.

The runtime orders children by immutable range boundary. It clips the interval before each child call.

A router does not execute the local scan callback. Only a range leaf executes it.

The runtime returns a topology cursor when more target partitions remain. The application cursor remains
opaque inside that topology cursor.

The route envelope counts every child RPC and identifies only leaves that perform application work.

### 4.26 Route caches

Caches are optional control-plane components.

```ts
export type FokosCachePolicy = {
	hash: { enabled: boolean; maxBytes: number };
	range: {
		enabled: boolean;
		ancestorsFromRoot: number;
		ancestorsFromLeaf: number;
		maxBytes: number;
	};
	promotion: { enabled: boolean; maxBytes: number; errorRate: number };
};
```

The runtime gets this policy from `fokosCachePolicy`. The callback receives the current application
configuration. The server entry exports `fokosDisableCaches`.

The first package version moves these existing strategies behind one internal cache interface:

- the hash path arena;
- learned range boundaries;
- the Bloom filter for descendant promotions.

A successful response carries a route hint. Each forwarding runtime can learn from that hint.

A stale hash hint reaches a former owner that remains a router.
A stale range hint reaches a partition with immutable boundaries.
A false promotion hint receives `FOKOS_PHANTOM_PARTITION` and falls back to hash routing.

A disabled, full, missing, or stale cache can add RPC hops. It must not change the selected owner.

The runtime persists caches under its reserved storage prefix. Cache state can be discarded during a
software migration or rollback.

### 4.27 Runtime storage

The runtime uses the standard Durable Object storage interface. It does not require application SQL
tables.

The runtime reserves keys below this prefix:

```text
__fokos_sharding/1/
```

Logical records include:

| Record | Purpose |
| --- | --- |
| identity | Immutable partition and topology identity. |
| arbitration | Full-split and promotion exclusion state. |
| source transition | One source repartition and its target acknowledgements. |
| target migration | Migration status and runtime and application cursors. |
| route override | One terminal promoted-key route. |
| work schedule | Runtime deadlines and application job tokens. |
| cache | Optional hash, range, and promotion hints. |

The runtime stores a schema number in every durable record.

Each key and value pair must fit the current 2 MB SQLite-backed Durable Object storage limit. Application
configuration and cursors must also fit this limit. The runtime stores route overrides as separate records
to prevent one unbounded value.

Each authority-changing transition must update one durable record. The runtime must not split one state
transition across independent writes. It must not store all route overrides in one value.

The runtime uses SHA-256 when a routing key must enter a storage-key name. It stores the full key in the
value and compares the bytes after lookup. A digest collision must not select the wrong route.

The runtime never reads or writes an application storage key, except during explicit destruction.

### 4.28 Background work and alarm integration

The runtime stores deadlines for unfinished control work. It processes bounded work in
`fokosRunDueWork`.

Core jobs include:

- target initialization;
- source cutover;
- target migration;
- acknowledgement retry;
- source cleanup.

The host can register application jobs through `fokosRunApplicationJob`.

The scheduler interface is:

```ts
export interface FokosScheduler {
	fokosSchedule(runAtMs: number): Promise<void>;
	fokosCancel(): Promise<void>;
}
```

`fokosCancel` removes only the runtime's schedule. A shared scheduler must preserve deadlines owned by
other components.

The runtime exposes `fokosScheduleApplicationJob(job, runAtMs)` for a host-owned job token. The runtime
stores the token and calls `fokosRunApplicationJob` when the deadline is due. It holds a shared request
lease during the callback.

The callback returns `{ done: true }` or `{ done: false, runAtMs }`. The runtime persists the next deadline
before it finishes the work pass.

The default `fokosNativeAlarmScheduler` is for a host where the runtime owns the native alarm. It reads
the current alarm and sets an earlier runtime deadline. A host with another alarm owner must provide a
shared scheduler adapter.

The host must call this from its system alarm handler:

```ts
async alarm(info: AlarmInvocationInfo): Promise<void> {
	await this.#fokos.fokosRunDueWork({ fokosAlarmInfo: info });
}
```

When another base class owns `alarm()`, the host must call both handlers and use one scheduler that merges
deadlines.

A host scheduler can call `fokosRpc({ kind: "run_due_work" })` instead of the default alarm bridge.

The runtime does not use `ctx.waitUntil` for durable progress. Durable Objects do not extend lifetime
through that method.

Each background pass catches job errors, stores or retains a future deadline, and returns. The system
alarm retry count is not the only recovery mechanism.

### 4.29 Control-plane retries

All control-plane operations are idempotent.

The runtime can retry:

- target initialization;
- migration-page reads;
- parent acknowledgements;
- status reads used by traversal;
- destruction after a prior successful delete.

Each retry gets a fresh Durable Object stub. Cloudflare marks a stub unusable after an exception.

The runtime does not retry application forwarding because it cannot infer application idempotency.
The application caller, transaction coordinator, or application policy owns those retries.

The runtime tries every independent target in a fan-out before it reports control-plane failures.

### 4.30 Error contract

The package exports `FokosShardingError` and predicates that inspect serializable fields.
Callers must not use `instanceof` across RPC.

```ts
export type FokosErrorCode =
	| "FOKOS_MIGRATING"
	| "FOKOS_REPARTITION_NOT_ACTIVE"
	| "FOKOS_CONTEXT_MISMATCH"
	| "FOKOS_INVALID_ROUTE"
	| "FOKOS_PHANTOM_PARTITION"
	| "FOKOS_SINGLE_OWNER_REQUIRED"
	| "FOKOS_REPARTITION_CONFLICT"
	| "FOKOS_PLAN_TOO_LARGE"
	| "FOKOS_CONTROL_RETRY"
	| "FOKOS_DESTROYED";
```

Each package error has these own properties:

- `code`;
- `retryable`;
- optional `retryAfterMs`;
- small serializable diagnostic fields.

The predicates also recognize a stable message marker for Workers that use legacy error serialization.

Application admission errors use application codes. The package does not reinterpret them.

### 4.31 Concurrency and ordering

The runtime writes each control transition before it sends an outbound RPC that depends on the transition.

The source initializes every target before it writes the routing cutover.

The source holds the exclusive request lease while it checks the cutover guard and writes the cutover.

The target rejects mutations during `copying`.

The target finalizer completes before the runtime writes `ack_pending`.

The source records each valid acknowledgement idempotently. It rejects an acknowledgement from an
unknown target.

The source enters `completed` only after every configured target acknowledges.

The runtime uses settled fan-out for independent control operations. One target failure does not prevent
calls to other targets.

The runtime does not depend on ordering between different Durable Object stubs.

### 4.32 Required invariants

The implementation must hold these invariants:

1. One routing key has one application owner.
2. A full-split parent handles no selected application data locally after cutover.
3. A promotion source handles only non-promoted partition keys locally after cutover.
4. Routing and migration use the same ownership helper.
5. The source owns selected data until cutover is durable.
6. A target accepts mutations only after finalization.
7. A repeated migration page is safe.
8. A completed target retries acknowledgement until the source records it.
9. A source cleanup error cannot reverse routing.
10. A cache hint cannot authorize local work.
11. Every non-terminal state has a stored future wake-up path.
12. Every required control RPC validates the partition and repartition identity.
13. A host application operation cannot bypass the request lease and remain conformant.

### 4.33 Administration and destruction

`FokosRouter` starts administration traversal from every root.

`fokosRpc({ kind: "status" })` returns:

- partition identity;
- source transition state;
- target migration state;
- direct split targets;
- route-override targets;
- pending work summary;
- cache statistics without cache payloads.

Traversal treats split targets and route overrides as one list of outgoing topology links. It deduplicates
shared targets by partition ID.

Destruction visits targets before their source. The host callback `fokosBeforeDestroy` stops application
resources.

The runtime then cancels its schedule, deletes all Durable Object storage, and aborts the object instance.

An application with external resources must delete them in `fokosBeforeDestroy`.
FokosDB must stop transaction coordinators before it destroys partitions.

### 4.34 Common-path performance

An unsplit point request adds:

- one in-memory context and state check;
- one shared request-lease acquisition;
- one application admission callback;
- one local application callback;
- no extra Durable Object RPC.

A cold split path adds one RPC for each router level. A learned route can skip known intermediate routers.

A migration page adds one source RPC and one destination apply step. The application selects the payload
budget.

The runtime loads durable control state during object initialization and keeps hot state in memory.
It writes cache state only when a hint adds information.

Before package release, benchmarks must measure:

- unsplit point-operation overhead;
- request-lease overhead with an async local callback;
- cold and warm routing depth;
- migration throughput for the FokosDB adapter;
- scheduler overhead with no pending work;
- client and server bundle sizes.

The performance-budget question in section 4.39 must be resolved before package approval.

### 4.35 FokosDB adapter

`PartitionDO` remains a Durable Object class. Its constructor creates `FokosSharding`.

The public FokosDB client API, request validation, application result types, `PartitionStore`,
`TransactionParticipant`, and `TtlExpiry` remain in FokosDB. Partition RPC implementations replace their
routing wrappers with `FokosSharding` calls.

Partition-to-partition and coordinator calls carry `FokosRouted`. The FokosDB Worker and transaction
coordinator unwrap the envelope before they process the application result.

The adapter maps current behavior as follows:

| Current FokosDB behavior | New integration |
| --- | --- |
| `apiPutItem`, `apiGetItem`, `apiDeleteItem` | `fokosRouteOne` |
| Transaction prepare, commit, cancel, and read | `fokosRouteMany` with `fanout` |
| Single-shot write and snapshot read | `fokosRouteMany` with `single_owner` |
| `apiQueryItems` | `fokosRouteRange` |
| Database-size rejection | `fokosAdmitLocal` |
| Hash and range split checks | `fokosPlanRepartition` |
| Heavy-key detection | `fokosPromotePartitionKey` proposal |
| Pending-lock promotion guard | `fokosCanCutover` |
| Hash, range, and promotion cache settings | `fokosCachePolicy` |
| Item and transaction migration | One application migration cursor and payload |
| Promotion source deletion | `fokosCleanupSourcePage` |
| TTL and stale transaction work | Application jobs or existing host scheduling |
| Routing metrics | `FokosRouted` mapped to FokosDB metadata |

FokosDB maps `rootTreesN` and `hashSplitN` into immutable topology configuration. It maps split
conditions, `rangeSplitN`, range ancestor policy, and transaction settings into opaque application
configuration. It derives the configuration version from a SHA-256 digest of their canonical encoding.

The FokosDB migration cursor owns its internal phases. The package does not know pending transactions or
deletion watermarks.

The runtime owns promotion route records. A hash split transfers these records to the correct hash target.
FokosDB no longer needs promotion rows for routing after legacy state migration.

The transaction coordinator keeps root partition contexts. A root remains a valid routing entry.
Commit and cancel continue to route through the current topology.

Stale transaction recovery continues to call public routed transaction methods.

### 4.36 Existing state migration

The FokosDB adapter implements `fokosImportLegacyState` as an idempotent legacy importer.

On the first runtime operation with no new identity, the runtime calls the importer. The importer reads
current FokosDB control state and maps:

- the partition context to runtime identity;
- a hash or range split to a source transition;
- child migration state to target migration state;
- completed promoted keys to terminal route overrides;
- every queued or active promotion to a source repartition;
- a completed child migration to `ack_pending` unless acknowledgement is known.

The importer does not rewrite item rows, pending transaction rows, or deletion metadata.

Hash and range route caches can be discarded and learned again. They are not authority.

The importer writes a migration marker only after it writes all required runtime records.
A restart can repeat the import.

### 4.37 Deployment and rollback

Deployment uses two compatibility stages.

Stage one adds `fokosRpc`, the legacy importer, and runtime shadow validation. Existing code remains the
routing authority. A code rollback is safe in this stage.

Stage two makes `FokosSharding` the routing authority. The old control RPC methods remain as adapters for
in-flight calls. The deployment must not remove old state yet.

A rollback from stage two needs current compatibility state. The FokosDB adapter uses
`fokosMirrorControlTransition` to mirror each new routing transition into the old readable format.
The runtime blocks the new transition when that critical callback fails.

Stage three stops compatibility writes after all supported partitions use the new state. A rollback to
old sharding code is not supported after this stage. Recovery uses a forward fix.

The implementation plan must define how deployment proves that no old runtime remains before stage three.

### 4.38 Testing

The package test suite must run in the Cloudflare Workers runtime.

Contract tests must use a small host Durable Object that extends a normal class and another host that
extends an SDK-style base class.

The tests must cover:

- lazy root initialization;
- idempotent target initialization;
- context conflicts;
- a repartition plan above the control-record limit;
- hash splits at more than one depth;
- range splits at more than one depth;
- partition-key promotion;
- concurrent promotions for different partition keys;
- full-split arbitration with queued and active promotions;
- a full split after completed promotions;
- route-override transfer;
- point, fan-out, and single-owner operations;
- ordered range traversal and pagination;
- reads and mutation rejection during migration;
- opaque migration cursors with multiple application phases;
- repeated migration-page import;
- a crash before and after each durable state transition;
- a lost acknowledgement response;
- one unavailable target during fan-out;
- RPC fan-out above the concurrency limit;
- source cleanup retries;
- async local callbacks during cutover;
- cache disablement, staleness, and capacity exhaustion;
- alarm restart and alarm-owner integration;
- status traversal and deduplication;
- destruction order;
- legacy state import and repeated import;
- compatibility RPC calls during stage two.

The FokosDB suite must pass without weaker assertions. New tests must cover each corrected recovery gap.

The build must keep the client bundle free of server code. Package checks must include typecheck,
formatting, package validation, and client bundle limits.

### 4.39 Open questions

#### 4.39.1 Package name

This RFC uses `fokos-sharding`. Approval must confirm the npm package name and export paths.

#### 4.39.2 Migration page budget

The FokosDB code currently targets about 20 MiB per migration response.

The generic package can require `fokosMigrationPageBytes` with no default. The application must select a
value below every current platform and payload limit.

TODO: Confirm the current Workers RPC message limit and choose whether the package can provide a default.

#### 4.39.3 SDK alarm adapters

The scheduler interface permits an Agents SDK adapter. The exact adapter depends on the SDK alarm and
schedule APIs.

TODO: Define and test the first Agents SDK integration before package approval.

#### 4.39.4 Legacy rollback duration

Stage two needs compatibility writes for rollback. Approval must define the observation period before
stage three removes them.

#### 4.39.5 Destruction authorization

A namespace binding is an internal capability, but any code with the binding can call `fokosRpc`.

Approval must decide whether `destroy` needs an application authorization callback in addition to the
binding boundary.

#### 4.39.6 Performance budgets

The required benchmarks are listed in section 4.34.

TODO: Define acceptance budgets for request overhead, migration throughput, scheduling, and bundle size.

## 5. Alternative options

### 5.1 Required base class

A base class can hide `fokosRpc`, constructor setup, and alarm delegation.

This option is not selected because a Durable Object can extend only one class. Agent frameworks and other
SDKs already provide base classes.

A future optional base class can wrap the composition API for applications without another base class.

### 5.2 Mixin or class decorator

A mixin or decorator can inject the required methods.

This option is not selected for the first version. It complicates Durable Object RPC types and generated
binding types. It can also hide the alarm integration requirement.

### 5.3 Generated host wrappers

Code generation can write `fokosRpc` and application routing wrappers.

This option is not required because one control RPC already has small boilerplate. A future generator can
use the same runtime API.

### 5.4 Separate sidecar Durable Object

A sidecar can own topology and scheduling outside the application Durable Object.

This option adds a control RPC to common request paths. It also prevents one local storage transaction
from coordinating application state and route policy.

### 5.5 Global topology keeper

A global topology keeper can answer every route from one authority.

This option adds a shared bottleneck and a new availability dependency. The selected design keeps each
source authoritative for its direct repartition.

### 5.6 Application-owned migration state

The runtime can give all migration state to the application.

This option reduces package code but duplicates cutover, acknowledgement, scheduling, and crash recovery
in every application. The selected design keeps control state in the runtime and application data opaque.

### 5.7 Writable target during migration

The package can permit writes while data copies from the source.

This option needs dual writes, change capture, or conflict reconciliation. The first version rejects
mutations until the target finalizes.

## 6. Frequently asked questions

### 6.1 Why does the host need `fokosRpc`?

Cloudflare exposes methods on the Durable Object class, not methods on a nested object. One delegated RPC
makes the composed runtime reachable by peer partitions.

### 6.2 Why use one control RPC instead of separate methods?

One method reduces host boilerplate and method-name conflicts. A discriminated union keeps control
operations typed.

### 6.3 Why must application methods call the runtime?

The package cannot intercept arbitrary methods on the host class. A direct application write can bypass
ownership and migration gates.

### 6.4 Can the host still use WebSockets or an Agent base class?

Yes. The runtime is a field, not a base class. The host keeps its existing APIs and class hierarchy.

The host must integrate the single native alarm when both systems need scheduled work.

### 6.5 Can the application use its own SQL schema?

Yes. The runtime uses reserved control keys. Migration callbacks can read and write any application
storage.

### 6.6 Why does the target pull data?

The target owns its cursor and checkpoints. A restart resumes work from target storage without a central
migration coordinator.

### 6.7 Why does migration have one application page operation?

The runtime needs only bounded data and a next cursor. The application cursor can sequence all of its
private data types and phases.

### 6.8 Why keep separate control states if migration is one operation?

Routing needs to know whether cutover happened and whether the target is complete. These facts must remain
durable even when the application cursor is opaque.

### 6.9 Can reads continue during migration?

Yes. A read operation can use `source_read`. The source callback bypasses normal routing and reads frozen
source state.

### 6.10 Can writes continue during migration?

No. The first version returns a retryable error until target finalization. This preserves a stable source
snapshot without dual writes.

### 6.11 Can split conditions differ by application?

Yes. `fokosPlanRepartition` can use any host state or policy. The runtime validates only topology safety.

### 6.12 Can request admission differ by operation?

Yes. Each operation passes an opaque policy tag to `fokosAdmitLocal`.

### 6.13 Can a partition use a different fan-out later?

A range partition can use a different target count in each plan. Hash fan-out is immutable for one
logical group in the first package version.

### 6.14 Why keep former owners as routers?

Workers and transaction coordinators can hold old partition references. A permanent router keeps those
references valid.

### 6.15 Why must acknowledgements retry after local finalization?

The acknowledgement response can be lost after the parent records it. An explicit `ack_pending` state
makes the repeat safe and guarantees parent completion.

### 6.16 Does the package retry application writes?

No. Only the application knows whether a write is idempotent. The package retries idempotent control work.

### 6.17 Can caches be disabled?

Yes. Disabling caches adds router RPCs but does not change ownership.

### 6.18 Why does destruction delete application storage?

The operation destroys one partition Durable Object. It is explicit and administrative.
The host can delete external resources in `fokosBeforeDestroy`.

## 7. References

- `docs/ideas/fokos-sharding/gptsol-existing-behavior.md`
- `packages/fokosdb/src/server/do-partition.ts`
- `packages/fokosdb/src/client/db.ts`
- `packages/fokosdb/src/shared/partition-topology/router.ts`
- `packages/fokosdb/src/shared/partition-topology/split-policy.ts`
- `packages/fokosdb/src/shared/partition/migration.ts`
- `packages/fokosdb/src/shared/partition/hash-key-promotion.ts`
- [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
- Invoke Durable Object methods:
  <https://developers.cloudflare.com/durable-objects/best-practices/create-durable-object-stubs-and-send-requests/>
- [Durable Object alarms](https://developers.cloudflare.com/durable-objects/api/alarms/)
- [Durable Object state](https://developers.cloudflare.com/durable-objects/api/state/)
- [Durable Object limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Workers RPC](https://developers.cloudflare.com/workers/runtime-apis/rpc/)
- [Workers RPC visibility](https://developers.cloudflare.com/workers/runtime-apis/rpc/visibility/)
- [Workers RPC error handling](https://developers.cloudflare.com/workers/runtime-apis/rpc/error-handling/)
