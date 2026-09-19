# RFC — Durable Object jurisdictions and location hints for a table

**State:** Draft
**Date:** 2026-09-19

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

Cloudflare gives each Durable Object namespace a set of jurisdictions. A jurisdiction keeps the object and its
storage inside a regulatory or geographic area. A caller selects one with `DurableObjectNamespace.jurisdiction()`,
which returns a subnamespace. Every ID and every stub that comes from the subnamespace stays in the area. The
values are `"eu"`, `"fedramp"`, `"fedramp-high"`, and `"us"`.

FokosDB cannot select one today. A user who must keep the data of a table inside the European Union cannot use the
library.

### 1.1 How the library reaches a namespace today

The library never passes a namespace object between components. A namespace is not serializable, so every
component looks the namespace up in `env` with a binding key. The key travels in `PartitionContext`: `ns` names
the partition binding and `nsTx` names the coordinator binding. Each RPC carries the context, because a Durable
Object takes no parameters at initialization time.

There are 24 places that read `env` with one of the two keys.

Six of them resolve an ID and hold no stub:

| Module | Function |
| --- | --- |
| `shared/partition-topology/router.ts` | `PartitionTopologyRouterImpl.pickPartition` |
| `shared/partition-topology/router.ts` | `PartitionTopologyRouterImpl.resolveRootPartitionContext` |
| `shared/partition-topology/partition-id.ts` | `resolveRangePartitionContext` |
| `shared/partition-topology/partition-id.ts` | `resolveDoId` |
| `shared/partition-topology/partition-id.ts` | `resolveHashChildPartitionContexts` |
| `shared/partition-topology/partition-id.ts` | `resolveDescendantHashPartitionContext` |

Eighteen of them acquire a stub:

| Module | Where |
| --- | --- |
| `client/db.ts` | `putItem`, `getItem`, `deleteItem`, and `queryItems`, through `partitionStub` |
| `client/db.ts` | the write fast path, the read fast path, both read-transaction phases, and both destroy callbacks, through `partitionStubByName` |
| `client/db.ts` | the coordinator pool, through `StaticShardedDO` |
| `server/do-partition.ts` | `repartitionDeps.getPeer` |
| `server/do-partition.ts` | the migration read-through of `apiGetItem` and of `apiQueryItems` |
| `server/do-partition.ts` | `forwardToRangeRootPartition` and the `forward` branch of `withSplitForwarding` |
| `server/do-partition.ts` | `getChildStub` |
| `server/do-partition.ts` | `recoverStaleTransactions`, which pokes the coordinator |
| `server/do-transaction-coordinator.ts` | prepare, commit, cancel, and the prepare retry |

`shared/do-stubs.ts`, `PartitionDO.get`, `PartitionDO.getByName`, `TransactionCoordinatorDO.get`, and
`TransactionCoordinatorDO.getByName` only pin the generic type of the stub. They add no routing.

### 1.2 Why a client option is not sufficient

A Durable Object resolves its own outbound namespaces from `env[ctx.ns]`. A jurisdiction that the client knows and
the context does not carry holds for the first hop only. The next hop leaves the jurisdiction: a forward to a
child partition, a forward to a range root, a migration read-through to the source, or a prepare from the
coordinator to a participant.

The jurisdiction must therefore be a field of `PartitionContext`, beside `ns` and `nsTx`.

### 1.3 The identity hazard

`env.X.jurisdiction("eu").idFromName(n)` and `env.X.idFromName(n)` name two different Durable Objects. The
jurisdiction is part of the identity of a table, in the same class as `hashSplitN`. A table that changes its
jurisdiction after the first write reaches a set of empty partitions, and the earlier data becomes unreachable.

The library cannot detect that change. The client derives the namespace and the context field from the same
value, so the old context and the new context never meet inside one Durable Object. Section 4.2.6 gives the
rules that limit the hazard, and section 4.2.5 gives the one check that does detect a plumbing fault.

### 1.4 The local runtime does not implement jurisdictions

`workerd` rejects the call. A probe on the test runtime of `packages/fokosdb`
(`@cloudflare/vitest-pool-workers@0.16.20`, `miniflare@4.20260625.0`, `workerd@1.20260625.1`) gives:

```
Error: Jurisdiction restrictions are not implemented in workerd.
```

The message is compiled into the `workerd` binary of every version in the lockfile (1.20260625.1, 1.20260828.1,
and 1.20260910.1), so a version upgrade does not remove it. Miniflare 4 also does not pass a binding-level
`jurisdiction` into the `workerd` configuration. The generated `worker-configuration.d.ts` declares
`jurisdiction()`, `DurableObjectJurisdiction`, and `DurableObjectId.jurisdiction`, so the code compiles and then
throws at run time.

The throw is synchronous and unconditional. A call on a path that every table uses stops the whole test suite.

`workerd` behaves differently for the other placement option of the same API: it accepts a `locationHint` and
ignores it. A hint therefore needs no guard, and a jurisdiction does.

### 1.5 Terms

| Term | Meaning |
| --- | --- |
| Jurisdiction | A regulatory or geographic area that holds a Durable Object and its storage. |
| Subnamespace | The `DurableObjectNamespace` that `jurisdiction()` returns. |
| Accessor | The one function that turns a context into the namespace to use. Section 4.2.2. |
| Plain binding | The namespace in `env`, with no jurisdiction applied. |

## 2. Goals and requirements

### 2.1 In scope

- A table selects one jurisdiction at creation time, and every Durable Object of the table is in it.
- The jurisdiction holds on every hop: a client request, a forward, a split, a migration, a promotion, a
  transaction, and a destroy.
- Every namespace resolution of a table that selects a jurisdiction applies it. No call site omits it, and no call
  site decides whether it is needed.
- A table that selects no jurisdiction behaves exactly as it does today, and calls `jurisdiction()` never.
- A Durable Object rejects a request whose context does not agree with the jurisdiction of its own ID.
- A table can select one location hint, and every stub call of the table carries it. Milestone 3.5 delivers this,
  and section 4.2.12 gives the details.

### 2.2 Requirements

- No RPC carries a namespace object. The context carries the jurisdiction as a string.
- The client bundle must not import a Durable Object class as a value. The accessor keeps the class imports
  type-only.
- The common path must not become slower. The accessor is one comparison and one property read when the table
  selects no jurisdiction.
- No storage migration. An existing table keeps its stored contexts.

### 2.3 Out of scope

- A table that spreads over more than one jurisdiction. The jurisdiction is a property of the table.
- A jurisdiction for the coordinators that differs from the jurisdiction of the partitions. One value covers both
  bindings. Section 4.2.3 gives the reason.
- A move of an existing table into a jurisdiction, or out of one. Section 4.2.6 gives the reason.
- A location hint for each root index. One value covers the table. A placement policy over the roots of one table
  is a separate change.
- An end-to-end test of a jurisdiction. Section 1.4 gives the reason, and section 4.2.10 gives what replaces it.

## 3. Milestones

### 3.1 The context field and the accessor

- `PartitionContext` gets `jurisdiction?: DurableObjectJurisdiction`.
- `PartitionContextCreator.create` accepts it.
- `areImmutableOptionsEqual` compares it.
- `shared/do-stubs.ts` gets `partitionNamespace` and `txCoordinatorNamespace`, and its stub helpers take a context
  instead of a namespace.
- The static helpers on `PartitionDO` and on `TransactionCoordinatorDO` that take a namespace are removed.
- All 24 call sites of section 1.1 use the accessor or a stub helper.

A table that selects no jurisdiction sees no change. This milestone ships on its own.

### 3.2 The coordinator namespace

- `FokosDBOptions` loses `transactionCoordinatorNs`.
- `FokosDB` builds `StaticShardedDO` over `txCoordinatorNamespace(env, partitionContext)`.
- `packages/fokosdb/README.md`, `examples/http-api/index.ts`, and the six test files that pass the option drop it.
- The coordinator distribution test of `test/transactions/tx-end-to-end.test.ts` moves its spy from an injected
  namespace to `env.TRANSACTION_COORDINATOR_DO.idFromName`.

This milestone is a breaking change of the public API.

### 3.3 The identity check

- `PartitionDO.ensurePartitionContext` compares `this.ctx.id.jurisdiction` with `pCtx.jurisdiction`.

### 3.4 Documentation

- `AGENTS.md` records the accessor rule in the topology section.
- `README.md` moves "Add jurisdictions support" out of the feature list.
- The package README documents the option and the identity hazard.

### 3.5 Location hints

- `PartitionContext` gets `locationHint?: DurableObjectLocationHint`, and `PartitionContextCreator.create` accepts
  it.
- The three stub helpers pass it to `get` and to `getByName`. Every stub call of the table carries it.
- `FokosDB` passes `shardLocationHintFn` to `StaticShardedDO`, so the coordinators take the same value.
- `areMutableOptionsEqual` compares it, because it is not part of the identity of an object.
- `README.md` moves the location-hint item out of the feature list.

The milestone ships after 3.1, because it needs the stub helpers of that milestone. It ships on its own and holds
no dependency on 3.2 or 3.3. Section 4.2.12 gives the details.

## 4. Proposed solution

### 4.1 High-level overview

The jurisdiction becomes one more field of the table configuration, beside the two binding keys it belongs with.
It travels in `PartitionContext`, so every component that resolves a namespace has it, wherever that component
runs.

One accessor replaces every direct `env[key]` read. The accessor is the only place in the library that calls
`jurisdiction()`, and it applies the jurisdiction at every resolution.

The stub helpers of the same module take a context as well, so one module owns both placement controls. A location
hint attaches to the stub call and not to the namespace, and milestone 3.5 adds it there as a second field of the
context. Section 4.2.12 gives that half.

```
PartitionContextCreator.create({ ns, nsTx, jurisdiction: "eu", ... })
                      │
                      ▼
            PartitionContext  ── carried by every RPC ──┐
                      │                                  │
        ┌─────────────┴──────────────┐                   │
        ▼                            ▼                   ▼
  FokosDB (Worker)            PartitionDO        TransactionCoordinatorDO
        │                            │                   │
        └──────────► partitionNamespace(env, ctx) ◄───────┘
                     txCoordinatorNamespace(env, ctx)
                              │
             ctx.jurisdiction undefined ?  env[ctx.ns]
                                        :  env[ctx.ns].jurisdiction(ctx.jurisdiction)
```

### 4.2 Technical details

#### 4.2.1 The context field

`PartitionContext` in `shared/partition-topology/partition-context.ts` gets:

```ts
/**
 * The Durable Object jurisdiction of every object of this table. It is part of the identity of the
 * table: a jurisdiction that changes names a different set of objects, and the earlier data becomes
 * unreachable.
 */
jurisdiction?: DurableObjectJurisdiction;
```

`DurableObjectJurisdiction` is an ambient type of the Workers runtime types, so the library exports nothing new.
`PartitionContextCreator.create` accepts the field and copies it into the context. The union type rejects an
invalid value at compile time, so `create` adds no run-time validation for it.

`areImmutableOptionsEqual` compares the field. Section 4.2.5 explains why that comparison is insurance and not
the protection.

#### 4.2.2 The accessor

`shared/do-stubs.ts` holds the accessor, because it is already the type-only boundary between a stub and a
Durable Object class:

```ts
export function partitionNamespace(env: Env, ctx: PartitionContext): DurableObjectNamespace<PartitionDO> {
	const ns = env[ctx.ns];
	return ctx.jurisdiction === undefined ? ns : ns.jurisdiction(ctx.jurisdiction);
}
```

`txCoordinatorNamespace` is the same function over `ctx.nsTx` and
`DurableObjectNamespace<TransactionCoordinatorDO>`.

The `env` parameter is explicit. `client/db.ts`, `router.ts`, and `partition-id.ts` pass the `env` they import
from `cloudflare:workers`. `do-partition.ts` and `do-transaction-coordinator.ts` pass `this.env`, which keeps the
style each module already uses.

Both functions import their class type-only, so the client bundle guard of `tsdown.config.ts` stays satisfied.

The `undefined` branch is load-bearing and not an optimization. Section 1.4 shows that `jurisdiction()` throws in
the local runtime, so a table that selects none must never reach the call.

The accessor applies the jurisdiction at every resolution, and no caller may skip it. A skip is not an
optimization: the jurisdiction is part of the identity of the object, so a resolution without it names a different
object. A caller therefore never reasons about whether the object exists already, or about which call created it.

After this change, a direct `env[ctx.ns]` or `env[ctx.nsTx]` read outside `do-stubs.ts` is a fault.

The three stub helpers of the same module take the context as well, and they are the only way to acquire a stub:

```ts
partitionStub(env, ctx, doId);
partitionStubByName(env, ctx, doName);
txCoordinatorStub(env, ctx, idOrName);
```

Each one resolves the namespace through the accessor and then calls `get` or `getByName`. A caller passes a
context and never a namespace. Section 4.2.12 gives the reason the stub call is a second seam and not the same
one.

The static helpers `PartitionDO.get`, `PartitionDO.getByName`, `TransactionCoordinatorDO.get`, and
`TransactionCoordinatorDO.getByName` are removed, because they take a namespace and therefore cannot apply the
jurisdiction. The nine sites inside the two Durable Object classes that call them, or that call
`this.env[ctx.ns].get` directly, use the helpers instead. `do-transaction-coordinator.test.ts` spies on
`PartitionDO.getByName` in ten places, and those spies move to the helper module.

#### 4.2.3 The coordinator namespace

`FokosDBOptions.transactionCoordinatorNs` is removed. `FokosDB` resolves the coordinator pool from the context:

```ts
this.#staticShardedTCs = new StaticShardedDO(txCoordinatorNamespace(env, partitionContext), {
	numShards: this.#options.numTxCoordinators,
	shardGroupName: `fokos_tc.${partitionContext.tableName}`,
});
```

The option and the context are two sources for one value, and they can disagree. `PartitionDO` resolves the
coordinator as `env[pCtx.nsTx]` when it recovers a stale transaction, so a caller that passes a jurisdictional
namespace in the option and no jurisdiction in the context makes the recovery path reach a coordinator that does
not exist. `nsTx` is already in the context, so the option adds nothing that the context does not hold.

One `jurisdiction` value covers both bindings. The coordinators of a table are always in the jurisdiction of its
partitions. A coordinator holds the operations of a transaction, which carry user data, until the transition to
`PREPARED` strips the payload, so a coordinator outside the area of the partitions would take that data out of the
area. A second field for the coordinators can be added later without a change to the accessor, because the
accessor already reads the binding key from the context.

`StaticShardedDO` accepts any `DurableObjectNamespace`, so a subnamespace needs no change in `durable-utils`.

The removal breaks every caller. The public surface loses one required field and gains none.

#### 4.2.4 The coordinator ID that comes back as a string

`TransactionParticipant` stores `coordinator_do_id`, which is `ctx.id.toString()` of the coordinator.
`PartitionDO.recoverStaleTransactions` restores it with `TransactionCoordinatorDO.get`, which calls
`idFromString`.

Cloudflare documents that the jurisdiction is encoded in the ID string, and that `idFromString` therefore works
on any binding of the namespace. The path needs no change for correctness. It uses the accessor anyway, so the
namespace of the call agrees with the rest of the table and the behaviour does not depend on that documented
property.

This is the only place in the library that receives an ID it did not build. Everywhere else the code resolves a
name into an ID itself, and `idFromName` must run on the subnamespace, so the jurisdiction must be known before
an ID exists. A jurisdiction read from an ID can therefore confirm a decision, and cannot make one.

#### 4.2.5 The identity check inside the partition

`DurableObjectId.jurisdiction` reports the jurisdiction of the object that serves the request, including inside an
alarm handler and for an object reached through `idFromString`. `PartitionDO.ensurePartitionContext` compares it
with the context of the request, and throws `FokosInternalError` with `partition_context_mismatch` when the two
differ.

The check catches a fault the context comparison cannot catch. The client derives the namespace and the context
field from one value, so a mismatch between them means a call site resolved the namespace without the accessor,
and the request reached an object of the wrong jurisdiction. The comparison in `areImmutableOptionsEqual` sees
two contexts that agree, because the fault is in the namespace and not in the context.

The check runs on a request that has a stored context and on a request that initializes one. It costs one string
comparison.

#### 4.2.6 Identity, immutability, and no migration

The jurisdiction is immutable for the life of a table, and the library cannot enforce that:

- A table that adds a jurisdiction reaches a set of empty Durable Objects and lazily initializes them. The
  earlier data stays in the objects of the plain binding, and nothing reads it.
- A table that removes its jurisdiction does the same in the other direction.
- Neither case produces an error, because the two sets of objects never exchange a context.

`packages/fokosdb/README.md` states the rule beside the `hashSplitN` rule.

No stored state changes:

- `primaryDoIdStr` holds a different string under a jurisdiction, because the ID differs. Nothing routes on it. It
  is one field of one log line in `do-partition.ts`, and neither `areImmutableOptionsEqual`,
  `areMutableOptionsEqual`, nor `ensurePartitionContext` reads it.
- `tc_participants` stores `partition_do_name`, which is a name and not an ID.
- The partition context in KV gains an optional field. An existing record has no `jurisdiction`, which reads as
  `undefined`, which is the plain binding. That is the behaviour the record already has.

#### 4.2.7 The transaction protocol

A transaction of one table stays inside one jurisdiction, because every participant of it belongs to the table.
FokosDB has no transaction over two tables, so two-phase commit never crosses a jurisdiction and the protocol
does not change.

The coordinator resolves each participant from the participant context it persisted, which carries the
jurisdiction. A coordinator that recovers after a restart therefore reaches the same partitions.

#### 4.2.8 Failure modes

| Fault | Result |
| --- | --- |
| A call site misses the accessor | The request reaches an object of the plain binding. Section 4.2.5 detects it on a partition and raises `partition_context_mismatch`. |
| A table changes its jurisdiction | The data becomes unreachable, with no error. Section 4.2.6. |
| A caller runs a jurisdiction on the local runtime | `jurisdiction()` throws the message of section 1.4 on the first call. |
| The account has no access to a jurisdiction | Cloudflare rejects the call. The library passes the error to the caller unchanged. |

#### 4.2.9 Performance

The accessor adds one comparison against `undefined` and one property read for each namespace resolution. A table
with a jurisdiction adds one `jurisdiction()` call for each resolution, which builds a subnamespace object and
performs no I/O.

A first request to a named object in a jurisdiction pays the same global lookup a named object pays today.

#### 4.2.10 Testing

Section 1.4 rules out an end-to-end test. The tests cover the accessor and the call sites instead:

- Unit tests of `partitionNamespace` and `txCoordinatorNamespace` over a fake `Env`. A namespace double records
  whether `jurisdiction()` was called and with which value, and returns a second double. The cases are: no
  jurisdiction calls `jurisdiction()` never and returns the plain binding; a jurisdiction calls it once with the
  value and returns the subnamespace; `nsTx` resolves the coordinator binding and not the partition binding.
- A test that a context with a jurisdiction, driven through `PartitionTopologyRouterImpl`, produces no call to
  `jurisdiction()` for a context without one. This holds the guard of section 4.2.2.
- A test of the check of section 4.2.5 on a partition with no jurisdiction: a context that names one is rejected
  with `partition_context_mismatch`.
- Unit tests of the three stub helpers over the same doubles: each helper calls `get` or `getByName` on the
  namespace the accessor returned, and on no other namespace.
- The existing suites prove the absence of a regression, because every one of them runs with no jurisdiction.

A deployed smoke test stays available to a maintainer with an account that has the jurisdiction, and this RFC does
not require one.

#### 4.2.11 Deployment and rollback

Milestone 3.1 is additive. A build that includes it behaves as the build before it for every existing table.

Milestone 3.2 breaks the constructor of `FokosDB`. It needs a minor version bump before the first stable release,
and a note in the package README.

A rollback is safe while no table selects a jurisdiction. After a table selects one, a rollback to a build without
the field makes the table unreachable, because the plain binding names different objects. The rollback plan for
such a table is to roll forward.

#### 4.2.12 Location hints

A location hint is the other placement control of the same API, and it is a different kind of value. The
difference decides where each one attaches:

| | Jurisdiction | Location hint |
| --- | --- | --- |
| Attaches to | the namespace, before `idFromName` | the stub call: `get(id, { locationHint })` or `getByName(name, { … })` |
| In the Durable Object ID | yes. One name gives two different IDs. | no |
| Strength | a constraint | best effort. The object spawns in a data center that minimizes latency from the hinted location. |
| When it applies | every call | the first `get()` for that object only. A later call ignores it. |
| A call site that omits it | reaches a different object, and the data is unreachable | places one object worse, and stays correct |

The stub helpers of section 4.2.2 are therefore the seam a hint needs, and the accessor is not. Milestone 3.1
builds the helpers, and milestone 3.5 adds one optional field and passes it in the helpers. No call site changes
twice.

`PartitionContext` gets `locationHint?: DurableObjectLocationHint`, one value for the whole table. The context
crosses RPC, so the field holds data and not a callback. It is not part of the identity of an object, so
`areMutableOptionsEqual` compares it and a table may change it. A change moves no object that exists, and applies
to each object the table creates after it.

Every stub call of the table carries the hint, as every namespace resolution carries the jurisdiction. The rule is
the same rule, for a weaker reason: a hint does not change which object a call reaches, so an omission costs
placement and not correctness. A helper that decides where a hint is worth passing would have to know which call
creates an object, and no call site can know that. A read of a partition that holds nothing creates it, because
`ensurePartitionContext` writes the context and the storage.

The hint changes the placement of a root partition and of a coordinator. It changes nothing else, and that is not
a reason to omit it. Cloudflare places an object close to the data center that made the first `get()` call for it.
A hash child, a split target, and a promoted range root are all created by a call from inside their parent, through
`getPeer` or `pickChildPartition`, so the default placement already keeps them close to their parent. A table-level
hint names the region the parent is in as well, so passing it on those calls agrees with the default instead of
fighting it.

The coordinator pool needs no new code: `StaticShardedDO` accepts
`shardLocationHintFn?: (shard) => DurableObjectLocationHint | undefined`. The same options bag of `get` also holds
`routingMode`, so a later read-replica change lands at the same seam.

A hint and a jurisdiction do not combine. When a request carries both, the jurisdiction wins and the hint is
ignored, so a hint serves a table that selects no jurisdiction. The library needs no validation of the pair,
because the platform resolves it. A table that sets both is not an error, and the hint has no effect.

`workerd` accepts a hint and ignores it (section 1.4), so a hint needs no guard for the test suite, and no local
test can prove a placement. The tests assert that each stub helper passes the value to `get` or to `getByName`.

## 5. Alternative options

**A second jurisdiction for the coordinators.** The context carries `jurisdiction` and `jurisdictionTx`, so a
table places its coordinators outside the area of its partitions. Rejected: section 4.2.3. A coordinator holds
the payload of a transaction until `PREPARED` strips it.

**A `FokosDB` option instead of a context field.** The client selects the jurisdiction and the context does not
carry it. Rejected: section 1.2. The jurisdiction holds for the first hop only.

**A namespace object in the options.** `FokosDB` takes the subnamespace the caller built. Rejected for the same
reason, and because a Durable Object cannot receive a namespace object over RPC.

**A jurisdiction read from the Durable Object ID at each call site.** Rejected: section 4.2.4. An ID reports a
jurisdiction, and a jurisdictional ID can only be built from a subnamespace, so the value must be known first.

**A binding for each jurisdiction in the Wrangler configuration.** A table names a binding key that is already
restricted. Rejected: Miniflare does not pass a binding-level jurisdiction into `workerd` either, the user must
add one binding for each jurisdiction, and `PartitionNamespaceKey` would admit keys that name the same class.

**`transactionCoordinatorNs` kept as an optional override.** Rejected: two sources for one value, which
section 4.2.3 shows can disagree in a way the recovery path cannot survive.

## 6. Frequently asked questions

**Does an existing table need a migration?** No. The field is optional, and an absent value is the behaviour of
today.

**Can a table move into a jurisdiction later?** No. Section 4.2.6. The objects of the two configurations are
different objects, and the library has no copy between them.

**Why is the accessor in `shared/do-stubs.ts` and not in `partition-context.ts`?** `do-stubs.ts` is the module
that already owns namespace-to-stub resolution under the type-only rule. `partition-context.ts` defines the
context, and a namespace read there would put an `env` access into the type module.

**Does the client bundle grow?** By one comparison. The accessor imports both classes type-only, exactly as
`do-stubs.ts` does today.

**Can the test suite exercise a jurisdiction?** No. Section 1.4. The tests of section 4.2.10 cover the decision
the library makes, not the behaviour of the platform.

**Can a table use a location hint as well?** Not from this RFC, and a later one gives it no value for a table that
selects a jurisdiction: the jurisdiction wins and the hint is ignored. Section 4.2.12.

**What happens to a transaction that spans two tables in two jurisdictions?** There is none. FokosDB has no
transaction over two tables.

## 7. References

- [Durable Object Namespace](https://developers.cloudflare.com/durable-objects/api/namespace/)
- [Durable Object ID](https://developers.cloudflare.com/durable-objects/api/id/)
- [Data location: restrict Durable Objects to a jurisdiction](https://developers.cloudflare.com/durable-objects/reference/data-location/)
