# RFC — Namespace fingerprints in the names of the admin, coordinator, and materialization Durable Objects

**State:** Completed
**Date:** 2026-10-31
**Author:** Lambros Petrou

**Status:** M1 and M2 are built. Section 6.6 lists the known limits.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

A Durable Object ID depends on two values: the namespace and the name. FokosDB uses `getByName` for all its objects.
The code paths in this document are relative to `packages/fokosdb/`.

The table options select four namespace bindings:

- `ns` holds the partitions and the GSI partitions.
- `nsTx` holds the transaction coordinators.
- `nsMat` holds the materialization partitions. It is optional.
- `nsAdmin` holds the topology admin objects. It is optional.

FokosDB names its shard groups `fokos.<type>.<name>`. Each Durable Object name of a group is `<shardGroup>~<suffix>`.
A table name cannot start with `fokos.`, and a shard group cannot contain `~`.

| Group | Namespace | Shard group today | Durable Object name today |
| --- | --- | --- | --- |
| Partitions | `ns` | `fokos.p.<table>` | `fokos.p.<table>~h.<root>[.<child>…]` or `~r.<hk>.<start>.<end>` |
| GSI partitions | `ns` | `fokos.gsiv1.<len>.<table>.<index>` | `<shardGroup>~…` |
| Coordinators | `nsTx` | `fokos.tc.<table>` | `fokos.tc.<table>~h.…` |
| Materialization | `nsMat` | `fokos.m.<table>` | `fokos.m.<table>~…` |
| Topology admin | `nsAdmin` | n/a | `fokos.topo.<shardGroup>`, for example `fokos.topo.fokos.p.users` |
| Table admin | `nsAdmin` | n/a | `fokos.admin.<table>`. Only a comment in `src/server/do-auto-admin.ts` defines it. |

A data Durable Object lives in the namespace of its own data. Thus the namespace already separates two tables with the
same name on two `ns` bindings. When an operator deletes a namespace and makes it again, all its objects are new and
empty.

Three object types live in one namespace and hold state about the data of a different namespace. That namespace can
serve more than one table. The name of these objects does not identify the data namespace, so two tables can share one
object.

### 1.1 Topology admin

`topologyAdminStub` in `src/shared/do-stubs.ts` selects the object by `nsAdmin`, the jurisdiction, and
`topologyAdminName(shardGroup)` only. The `ns` binding is not part of the name. The isolate cache scope in
`topologyCacheFor` in `src/shared/topology-hooks.ts` uses the same address, so it cannot separate the two tables.

Example: table `users` on `NS_A` and table `users` on `NS_B`, both with `nsAdmin: ADMIN`. Both tables send their split
reports to `fokos.topo.fokos.p.users`, and the object stores them as one shard group.

- When `rootTreesN` and `hashSplitN` are equal, the two topologies merge. A hint can point to a partition that exists
  only in the other namespace. That partition has no identity.
  - A `FokosDB` client and a forward inside a Durable Object remove the hint and fall back. The data stays correct.
    Each cache refresh brings the hint back, so some requests keep an extra round trip.
  - A coordinator resolves its participants with `speculative: false` and has no fallback, because it sends `txPrepare`
    from another client instance. A participant with no identity fails the prepare, and the coordinator cancels the
    transaction. The caller gets `transaction_cancelled`.
- When `rootTreesN` or `hashSplitN` is different, `registerRepartition` in `src/sharding/topology-keeper.ts` rejects
  each report of the second table. The second table never gets a topology.

The same effects occur when an operator deletes `ns` and makes it again with the same binding name, and does not call
`destroy()` first. The admin object keeps the split facts of the old namespace.

### 1.2 Coordinator

The coordinators of two tables with the same name and the same `nsTx` are the same objects. A transaction stores the
table configuration of each participant, so the commit and the cancel reach the correct namespace. The coordinator
finds a stored transaction by the idempotency token. The replay check uses `hashTransactionOperations`, which hashes the
items and the execution mode only. The table and `ns` are not part of the hash.

Example: a caller sends one token with the same items to `users` on `NS_A` and to `users` on `NS_B`. The second request
gets the stored result of the first request, and its writes never apply.

### 1.3 Materialization

The materialization objects of two tables with the same name and the same `nsMat` are the same objects. The steps:

1. `#ensureIdentity` in `src/sharding/runtime.ts` compares the partition ID, the name, and the topology of a request
   with the stored identity. The two tables have equal values, so the check passes.
2. When the policy is different and `policyVersion` is equal, `#ensureIdentity` stores the policy of the request. The
   policy of the two tables is different in `ns`, so each request of the other table replaces the stored `ns`.
3. `GsiWriter` in `src/server/materialization/gsi-writer.ts` gets the table configuration from the stored route context
   of the object. It keeps one client for each index and makes a new one only when `policyVersion` changes.

Thus the GSI writes of both tables go to one `ns`: the `ns` of the stored policy when `GsiWriter` made the client. The
GSI of one table gets the entries of the other table.

### 1.4 Gap in `destroy()`

`destroy()` in `src/client/db.ts` resets only the partition shard group and the coordinator shard group in the admin
object. The GSI partitions also report their splits, because their policy keeps `nsAdmin`. No code resets their shard
groups.

The materialization partitions use the sharding runtime and split, but `FokosDBMaterializationDO` gives no
`adminOutbox` hook and no `topologyCache` hook to its runtime. Thus the `m` shard group gets no reports today, and no
caller reads its topology.

The GSI partitions report their splits and read their own topology when they forward a request, because their policy
keeps `nsAdmin` and `topologyEntrypoint`. The callers outside the GSI partitions have no topology cache, so they enter
at the root: the export client in `FokosDBPartitionDO`, `GsiWriter`, and the GSI clients in `src/client/db.ts`.

### 1.5 Names that break the prefix rule

`topologyAdminName` in `src/sharding/topology-codec.ts` adds `fokos.topo.` to a shard group that already starts with
`fokos.`. The result has the prefix two times, for example `fokos.topo.fokos.p.users`.

## 2. Goals and requirements

### In scope

- Two tables with the same name on two different `ns` bindings never share an admin, coordinator, or materialization
  object. This is true also when they share `nsAdmin`, `nsTx`, and `nsMat`.
- After an operator deletes a namespace and makes it again, no object keeps state about the old namespace in a name that
  the new namespace uses.
- Each admin object name has the form `fokos.topo.<type>.<name>~<fp>`. No name has two `fokos.` prefixes.
- No two FokosDB Durable Object names in one namespace are equal unless they are the same object.
- `destroy()` resets the admin state of all four shard group types: `p`, `gsiv1`, `tc`, and `m`.

### Out of scope

- The names of partitions and GSI partitions do not change. Their namespace separates them already.
- Push targets do not change. The user selects their binding and their `objectName`.
- The generic sharding library does not learn the FokosDB prefixes. FokosDB builds the admin object name.
- The topology of the materialization partitions and of the GSI readers stays as it is (section 1.4). A later change can
  give `FokosDBMaterializationDO` the `adminOutbox` and `topologyCache` hooks, and give a topology cache to the export
  client, `GsiWriter`, and the GSI clients. The admin object names of this document already support that change.

### Requirements

- The fingerprint must be a synchronous value, because the code that makes a stub is synchronous.
- The fingerprint must be different for two different namespaces. A test with six bindings in
  `packages/fokosdb/wrangler.jsonc` confirmed this locally. The author confirmed that a namespace that is deleted and
  made again also gets new IDs.
- A Durable Object name can have any length. Cloudflare hashes the name for routing. `ctx.id.name` holds the first 1024
  bytes of the name. FokosDB reads `ctx.id.name` only for diagnostic fields: `actorName`, and the `doName` of a "not
  initialized" error.

## 3. Milestones

Two milestones. Each one ships on its own.

1. **M1 — `destroy()` resets all shard groups.** `destroy()` resets the admin state of the `p`, `tc`, `m`, and each
   `gsiv1` shard group (section 4.2.7). This fixes the gap in section 1.4. It does not depend on M2.
2. **M2 — Namespace fingerprints.** The fingerprint helper, the new `tc` and `m` shard groups, the new admin object
   names, the admin outbox, and the topology read path (sections 4.2.1 to 4.2.6), with the documentation and the tests.
   These parts ship together, because a writer and a reader of one admin object must compute the same name.
   `destroy()` uses the new admin object names.

## 4. Proposed solution

### 4.1 High-level overview

Each Durable Object name must include the fingerprint of each namespace whose state the object holds, except its own
namespace.

The fingerprint of a namespace is the ID of one fixed name in that namespace:

```ts
fp = base64url(bytes of env[binding].idFromName("fokos.fingerprint").toString()) // 43 characters, no padding
```

Example names for table `users` with GSI `byEmail`:

Partitions (`ns`). The shard group and the object names do not change.

- Shard group: `fokos.p.users`
- Object name: `fokos.p.users~h.3`
- Admin object name: `fokos.topo.p.users~<fp(ns)>`

GSI partitions (`ns`). The shard group and the object names do not change.

- Shard group: `fokos.gsiv1.5.users.byEmail`
- Admin object name: `fokos.topo.gsiv1.5.users.byEmail~<fp(ns)>`

Coordinators (`nsTx`):

- Shard group: `fokos.tc.users.<fp(ns)>`
- Object name: `fokos.tc.users.<fp(ns)>~h.3`
- Admin object name: `fokos.topo.tc.users.<fp(ns)>~<fp(nsTx)>`

Materialization partitions (`nsMat`):

- Shard group: `fokos.m.users.<fp(ns)>`
- Object name: `fokos.m.users.<fp(ns)>~h.3`
- Admin object name: `fokos.topo.m.users.<fp(ns)>~<fp(nsMat)>`

Table admin (not built): `fokos.admin.users~<fp(ns)>`.

The admin object name rule:

```
fokos.topo. + <shard group without the leading "fokos."> + "~" + <fp of the namespace that holds the group>
```

The two fingerprints in a coordinator admin name have different jobs:

- `fp(ns)` in the shard group separates two tables.
- `fp(nsTx)` after `~` separates a coordinator namespace that an operator deleted and made again.

Results:

1. When two tables have the same name on `NS_A` and `NS_B`, and share `nsTx`, `nsMat`, and `nsAdmin`, they have
   different `fp(ns)` values. They get separate coordinators, materialization objects, and admin objects.
2. When an operator deletes `ns` and makes it again, `fp(ns)` changes. The table gets new, empty coordinator,
   materialization, and admin objects.
3. When an operator deletes `nsTx` or `nsMat` and makes it again, the objects in that namespace are new. Their admin
   object names get the new fingerprint.
4. When an operator destroys a table with `destroy()` and makes it again on the same `ns`, `fp(ns)` stays the same.
   Thus `destroy()` must reset the admin state of all four groups.

### 4.2 Technical details

#### 4.2.1 The fingerprint

- A new helper `namespaceFingerprint(env, binding, jurisdiction)` in `src/shared/do-stubs.ts` returns the fingerprint.
- The helper uses the namespace with the jurisdiction of the table, the same namespace that the stubs use.
- The helper keeps the result for each isolate, keyed by the binding and the jurisdiction.
- The encoding is base64url of the 32 ID bytes, with no padding: 43 characters. The alphabet is `A–Z a–z 0–9 - _`.
  It never contains `.` or `~`.

#### 4.2.2 Why no two names collide

1. Each type has its own prefix: `p.`, `gsiv1.`, `tc.`, `m.`, `topo.`, `admin.`. No prefix is the start of another
   prefix. A table name cannot start with `fokos.`.
2. A shard group cannot contain `~` (`validateTopology` in `src/sharding/route-context.ts`). Thus the first `~` ends the
   group part of each name.
3. The `<fp>` at the end of a `tc` or `m` shard group has a fixed length of 43 characters. A table name can contain
   `.`, but the last 43 characters of the group are always the fingerprint.
4. The GSI shard group holds the length of the table name, so each pair of table name and index name gives one group.
5. A Durable Object ID depends on the namespace and the name. A namespace that is made again gets new objects.

The partitions, the coordinators, and the materialization objects use different classes, so `ns`, `nsTx`, and `nsMat`
are always different namespaces.

#### 4.2.3 Shard groups

- `coordinatorShardGroup` and `materializationShardGroup` in `src/shared/partition-context.ts` take `fp(ns)` and return
  `fokos.tc.<table>.<fp>` and `fokos.m.<table>.<fp>`.
- `baseTableName` removes the 44-character `.<fp>` suffix when it reads an `m` group. No code reads the table name of a
  `tc` group.
- The callers that build these groups have `env` and `policy.ns`, so they compute `fp(ns)` with the helper.
- The `FokosDB` constructor computes `fp(ns)`, and `fp(nsTx)` when the table has a topology cache. Thus the constructor
  checks that `ns` and `nsTx` each name a Durable Object namespace binding of the Worker. A wrong binding throws
  `fokosdb_options_invalid`.

#### 4.2.4 Admin object name

- FokosDB builds the admin object name in `src/shared/do-stubs.ts`. `topologyAdminName` moves out of
  `src/sharding/topology-codec.ts`, so the generic sharding library does not name admin objects. A host of the generic
  library names its own admin objects.
- The caller of each admin object knows the shard group and the namespace that holds it. Thus the caller computes the
  admin object name, and no code finds the namespace from the shard group prefix.

| Caller | Shard group | Namespace of the fingerprint |
| --- | --- | --- |
| `FokosDB`, the partitions client | `p` | `ns` |
| `FokosDB`, the coordinators client | `tc` | `nsTx` |
| `FokosDB.destroy()` | `p`, `tc`, `m`, `gsiv1` | `ns`, `nsTx`, `nsMat`, `ns` |
| `FokosDBTransactionCoordinatorDO`, the participant client | `p` | `ns` |
| `FokosDBPartitionDO`, its runtime hooks | `p` or `gsiv1` | `ns` |
| `FokosDBTransactionCoordinatorDO`, its runtime hooks | `tc` | `nsTx` |

- `TopologyAdminAddress` holds `nsAdmin`, the jurisdiction, and the admin object name. `topologyAdminStub` calls
  `getByName` with that name.
- The removal of the leading `fokos.` is safe. `FokosDBAutoAdminDO` receives only FokosDB shard groups, and each one
  starts with `fokos.`.
- The `admin(shardGroup)` callback of the sharding library does not change.

#### 4.2.5 Admin outbox

`adminOutboxFor` in `src/shared/topology-hooks.ts` takes the binding of the namespace that holds the shard group of the
sender, from the table in section 4.2.4. It computes the admin object name one time.

#### 4.2.6 Topology read path

- Each `FokosShardingClient` reads the topology of one shard group. Thus each topology cache options object holds one
  admin object name. `FokosDB` makes one options object for its partitions client and one for its coordinators client.
- `topologyCacheFor` takes the binding of the namespace that holds the shard group, and computes the address one time.
- The entrypoint props are the address: `nsAdmin`, the jurisdiction, and the admin object name.
  `FokosDBTopologyCache` in `src/server/topology-entrypoint.ts` calls the admin object with that name.
- The entrypoint answers 503 when the `sg` of the path does not belong to the admin object name of the props.
- Workers Cache keys an entry by the props and the path. The props hold the fingerprint, so a namespace that an operator
  makes again gets new entries.
- The isolate cache scope in `topologyCacheFor` holds `nsAdmin`, the jurisdiction, and the admin object name.
- The paths and the generic library do not change.

#### 4.2.7 `destroy()`

`destroy()` calls `resetShardGroup` for the `p`, `tc`, `m`, and each `gsiv1` shard group of the table. It runs the reset
last, as it does now, so a partition that the traversal has not reached cannot add a report after the reset.

#### 4.2.8 Cost

- The fingerprint costs one `idFromName` call for each binding and jurisdiction in each isolate.
- Each coordinator and materialization object name grows by 44 characters. These names travel in RPCs and stay in lock
  rows (`CoordinatorRef`). The number of rows does not change.
- A coordinator admin object name has about 110 characters.

#### 4.2.9 Migration

The project is not released. The change renames the coordinator, materialization, and admin objects in place. Existing
local data in those objects is lost.

#### 4.2.10 Documentation

Update the name descriptions in `src/server/do-auto-admin.ts`, `src/shared/partition-context.ts`, and `AGENTS.md`.

#### 4.2.11 Tests

1. Two tables named `users` on two `ns` bindings share `nsTx`, `nsMat`, and `nsAdmin`. Then check:
   - After a split of the first table, the admin object of the second table has no split.
   - A transaction with the same token and the same items applies its writes in both tables.
   - The GSI writes of each table stay in its own `ns`.
2. `destroy()` resets the admin state of the `p`, `tc`, `m`, and `gsiv1` shard groups.
3. A unit test of the name builders shows that table names and index names with `.` do not collide. Example: table
   `a.b` with index `c`, against table `a` with index `b.c`.
4. A unit test shows that `baseTableName` returns the table name of an `m` group with a fingerprint, and that the GSI
   shard group from the `m` group is equal to the GSI shard group from the `p` group.
5. The `FokosDB` constructor rejects an `ns` or an `nsTx` that names no binding, or a binding that is not a Durable
   Object namespace.

## 5. Alternative options

### 5.1 Fingerprint only in the admin object name

This fixes the admin collision only. The coordinator collision in section 1.2 and the materialization collision in
section 1.3 stay.

### 5.2 One admin object for each shard group, with a reset when the fingerprint changes

The admin object keeps the fingerprint of each shard group, and it resets the group when a report has a different
fingerprint. This fixes a namespace that is made again. Two live namespaces that share one admin object reset each other
again and again, so this option fails the first goal.

### 5.3 Reject the setup at configuration time

The table validation sees one table at a time. It cannot find a second table with the same name on another `ns`.

### 5.4 A documented rule only

The documentation tells operators to call `destroy()` before they delete or change a namespace. This needs no code, but
an operator can forget the rule, and it does not fix two live tables with the same name.

### 5.5 Fingerprint in the partition and GSI names too

The names of the partitions and the GSI partitions would grow by 44 characters, and the namespace separates them
already. No goal needs it.

### 5.6 Hex fingerprint

The hex form of the ID has 64 characters. Base64url holds the same 32 bytes in 43 characters.

### 5.7 Shorter fingerprint from a hash of the ID

A 12-byte prefix gives 16 characters. Cloudflare does not document which bytes of an ID depend on the namespace, so a
safe prefix needs a SHA-256 hash first. `crypto.subtle.digest` is asynchronous, and the code that makes a stub is
synchronous.

## 6. Frequently asked questions

### 6.1 Why do the partition names not need a fingerprint?

A partition lives in `ns`, the namespace of its own data. The namespace is part of the Durable Object ID, so two `ns`
bindings give two different objects for the same name.

### 6.2 Why is the fingerprint inside the `tc` and `m` shard groups and not after `~`?

The runtime builds a coordinator or a materialization name as `<shardGroup>~h.<root>…`. A shard group cannot contain
`~`, so the fingerprint must be a part of the shard group.

### 6.3 Why does a coordinator admin name have two fingerprints?

`fp(ns)` identifies the table. `fp(nsTx)` identifies the namespace that holds the coordinators. Each value changes in a
different case, so neither one can go.

### 6.4 Does `destroy()` still need the admin reset?

Yes. A table that an operator destroys and makes again on the same `ns` has the same `fp(ns)`, so it uses the same admin
objects.

### 6.5 Is a long name a problem?

No. Cloudflare hashes the name for routing. `ctx.id.name` holds the first 1024 bytes, and FokosDB reads it only for
diagnostic fields.

### 6.6 What are the known limits?

- After `destroy()`, a new table with the same name on the same `ns` uses the same admin objects and the same topology
  cache entries. For a short time, a cache can give hints for partitions that the destroy deleted. A read or a write
  falls back to the root. A transaction that a coordinator drives is cancelled. The doc comment of `destroy()` in
  `src/client/db.ts` gives this limit.
- A coordinator has no fallback when `txPrepare` misses at a participant address from a topology hint. A `FIXME` in
  `src/server/do-transaction-coordinator.ts`, at the `txPrepare` send, gives the fix: store the root context of the
  participant, and send the prepare again to the root.

## 7. References

- `docs/agent-plans/2026-10-23-encoded-topology-auto-admin.md`
- `packages/fokosdb/src/shared/do-stubs.ts`
- `packages/fokosdb/src/shared/partition-context.ts`
- `packages/fokosdb/src/shared/topology-hooks.ts`
- `packages/fokosdb/src/sharding/topology-codec.ts`
- `packages/fokosdb/src/sharding/topology-cache.ts`
- `packages/fokosdb/src/sharding/topology-keeper.ts`
- `packages/fokosdb/src/server/topology-entrypoint.ts`
- `packages/fokosdb/src/server/do-auto-admin.ts`
- `packages/fokosdb/src/server/do-transaction-coordinator.ts`
- `packages/fokosdb/src/server/materialization/do-materialization.ts`
- `packages/fokosdb/src/client/db.ts`
