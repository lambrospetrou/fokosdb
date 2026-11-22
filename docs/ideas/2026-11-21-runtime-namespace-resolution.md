# RFC — Durable Object namespace resolution from fingerprints and callbacks

**State:** Draft
**Date:** 2026-11-21
**Author:** Lambros Petrou

**Status:** Design only. Nothing is built.

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
- [8. Appendix: measured cost](#8-appendix-measured-cost)

## 1. Overview and context

The code paths in this document are relative to `packages/fokosdb/`.

A table selects four Durable Object namespaces by Worker `env` binding name:

- `ns` holds the partitions and the GSI partitions.
- `nsTx` holds the transaction coordinators.
- `nsMat` holds the materialization partitions. It is optional.
- `nsAdmin` holds the topology admin objects. It is optional.

`createTableConfig` in `src/shared/partition-context.ts` copies the four strings into `FokosDBPolicy`, and the route
context carries the policy on every RPC. Each initialized Durable Object stores the policy in `__fokos/policy`, and
repartition plan heads store a copy of it. `ParticipantCoordinatorAddress` stores `nsTx`, and `TopologyAdminAddress`
stores `nsAdmin` in the `ctx.props` of the `FokosDBTopologyCache` entrypoint. Every lookup reads `env[name]`, and all
such lookups are in `src/shared/do-stubs.ts`. `src/server/admin-outbox.ts` and `src/shared/topology-hooks.ts` read the
ambient `env` and `exports` of `cloudflare:workers`.

Two limits follow from the `env` name.

- A Worker that exposes its Durable Object classes only through `ctx.exports` has no `env` binding, so it cannot use
  FokosDB. The `PartitionNamespaceKey` type family filters `keyof Env`, so such a Worker cannot even construct the
  table options.
- The same namespace can have a different name in `env` and in `ctx.exports`. Workers commonly use uppercase binding
  names in `env` and camelCase export names. A lookup that tries `env[name]` and then `exports[name]` cannot find a
  namespace whose two names differ.

A local experiment in workerd measured the `ctx.exports` namespace API. The values are function-typed
`LoopbackServiceStub` objects with `idFromName`, `getByName`, `get`, and `jurisdiction`. Exports that are not Durable
Object namespaces expose the same method names, so a resolver must call `idFromName("fokos.fingerprint")` and require a
synchronous 64-hex result. The same class gives the same namespace fingerprint through `env` and through
`ctx.exports`, and two `env` bindings to one class give the same fingerprint. A namespace through `ctx.exports`
supports `jurisdiction()` and gives the same fingerprint as the same namespace through `env`. Section 8 lists the
measured costs.

## 2. Goals and requirements

In scope:

- FokosDB must work when a Durable Object namespace is bound in `env`, exported in `ctx.exports`, or both under
  different names.
- A table must keep its derived Durable Object names when the namespace moves between `env` and `ctx.exports`. The
  coordinator group `fokos.tc.<tableName>.<fp>`, the materialization group `fokos.m.<tableName>.<fp>`, and the admin
  names `fokos.topo.<shardGroup>~<fp>` must not change.
- The policy must stay serializable. It rides in every RPC and persists in `__fokos/policy`.
- Resolution must verify the namespace, not trust it. A wrong callback answer or a wrong stored selector must fail
  loudly and must never route data of a shard group to a namespace that does not hold it.
- Per-table namespace selection must keep working: `ns`, `nsTx`, `nsMat`, and `nsAdmin` can differ per table today.
- The jurisdiction of the table must keep working. `namespaceFingerprint` computes the ID on the namespace after
  `jurisdiction()`, and resolution must do the same.
- The change is breaking: new code does not read the old binding names. A stored record of the old shape is
  overwritten by the first request that carries the new shape, under the normal `policyVersion` rules.
- A partition whose storage is cloned into a different namespace, for backup and restore, must converge on the
  fingerprints of the new namespace through the normal policy-overwrite path. Its own namespace must need nothing
  stored at all.
- The client must not import a Durable Object class as a runtime value. The client bundle budget must hold.
- Resolution must not scan `env` or `ctx.exports` on every request. Each isolate resolves each fingerprint once.
- The push target binding of `FokosDBMaterializationDO` must resolve through `env` and `ctx.exports` too. It has no
  fingerprint and no identity rules, so it gets a different mechanism, in milestone M2.

Out of scope:

- Resolution of a namespace that is not reachable from the Worker at all. A resolver callback can only return a
  namespace the Worker can see.
- Per-partition namespaces. A table binds one namespace for each role, and every partition of the table must stay in
  it. This rule does not change.
- `topologyEntrypoint`. It is already an export name resolved through `exports`, and it stays a string.

## 3. Milestones

- **M1 — Namespace fingerprints in the policy.** `FokosDBPolicy` carries `nsFp`, `nsTxFp`, `nsMatFp`, and `nsAdminFp`
  in place of the binding names. `src/shared/do-stubs.ts` resolves a namespace from a fingerprint by a scan of `env`
  and `ctx.exports`, with a `resolveNamespace` callback as the escape hatch. `FokosDBOptions` gets `namespaces` and
  `resolveNamespace`, `FokosTableIdentity` drops the binding names, and `ParticipantCoordinatorAddress`,
  `TopologyAdminAddress`, `CoordinatorRef`, and the Durable Object classes change to match. The change is breaking:
  stored binding names are never read.
- **M2 — Push target binding override.** `FokosDBMaterializationDO` gets a protected `fokosPushTargetBinding(target)`
  method. The default reads `env[target.binding]` and then `exports[target.binding]`. `pushTargetStub` takes the
  resolved binding. Section 4.2.8 covers it.

M2 is independent of M1. Either order ships.

## 4. Proposed solution

### 4.1 High-level overview

A binding name is a lookup key, not an identity. FokosDB already computes the real identity: the namespace
fingerprint, the base64url ID of the fixed name `fokos.fingerprint`, 43 characters. The fingerprint is a property of
the namespace. It does not depend on the binding name, and it does not differ between `env` and `ctx.exports` for the
same namespace.

The change replaces each stored binding name with the stored fingerprint of that namespace. The client computes the
fingerprints from the namespace objects it is configured with. Each host — the client, a partition, a coordinator, a
materialization partition, the topology entrypoint — resolves "the namespace whose fingerprint matches" by a scan of
`env` and `ctx.exports`. A candidate matches when `idFromName("fokos.fingerprint")` on it, after `jurisdiction()`,
equals the stored fingerprint. A wrong answer cannot match, so a resolution failure is a missing namespace, never a
wrong namespace.

A `resolveNamespace` callback is the escape hatch. It gets the role, the table name, the shard group, the
jurisdiction, and the expected fingerprint. It returns a namespace, or `undefined` to fall back to the scan. The
runtime applies the jurisdiction to the answer and checks the fingerprint, so a callback that returns a wrong
namespace fails loudly.

Push targets keep `binding` as a serializable selector. They are user bindings, not FokosDB namespaces, so they have
no fingerprint to check. A protected method on `FokosDBMaterializationDO` resolves the selector, and the default reads
`env` and then `ctx.exports`.

### 4.2 Technical details

#### 4.2.1 What the policy stores

`FokosDBPolicy` replaces `ns`, `nsTx`, `nsMat`, and `nsAdmin` with `nsFp`, `nsTxFp`, `nsMatFp`, and `nsAdminFp`. The
optional fields keep their role: `nsMatFp` absent means the table has no materialization, and `nsAdminFp` absent means
no repartition reports.

The fingerprints go in the policy, the mutable part of the stored route context, and never in `__fokos/identity`. The
identity record rejects a changed value with `partition_context_mismatch`, so a fingerprint there would make a
partition cloned into a different namespace unreachable. The policy accepts a new value through `policyVersion`, so a
clone adopts the fingerprints of the first request it gets.

`FokosTableIdentity` drops `ns`, `nsTx`, and `nsMat`. The rule of the table keeps its force on the namespace objects
of `FokosDBOptions.namespaces`: their fingerprints never change after the table has data. A change to `nsFp` or
`nsTxFp` selects a different namespace and a different set of empty objects, exactly as a change to `ns` or `nsTx`
does today. `createTableConfig` computes the fingerprints and puts them in the policy.

`ParticipantCoordinatorAddress` carries `nsTxFp` and `jurisdiction`. `TopologyAdminAddress` carries `nsAdminFp`,
`jurisdiction`, and `name`, so `ctx.props` stays serializable.

`CoordinatorRef` gets a version 2 with `nsTxFp`: the fingerprint of the coordinator's own namespace. A coordinator
computes it from the namespace that resolves its own object. The recovery path resolves the coordinator namespace
through `nsTxFp` of the stored policy, and the `nsTxFp` of the reference marks the environment the transaction
belongs to.

#### 4.2.2 The resolver

```ts
interface FokosNamespaceRequest {
	role: "partitions" | "transactionCoordinators" | "materializations" | "admin";
	tableName: string;
	shardGroup: string;
	jurisdiction?: DurableObjectJurisdiction;
	fingerprint?: string;
}

type FokosNamespaceResolver = (req: FokosNamespaceRequest) => DurableObjectNamespace | undefined;
```

- `role` names the namespace to resolve.
- `tableName` and `shardGroup` identify the table. `shardGroup` carries the index name for a GSI partition group
  (`fokos.gsiv1.<n>.<tableName>.<indexName>`) and the partition fingerprint for a coordinator or materialization
  group. Every call site can decode `tableName` from the stored shard group, and the topology entrypoint can decode it
  from the admin name `fokos.topo.<group>~<fp>` in its `ctx.props`.
- `jurisdiction` is the jurisdiction of the table. The runtime applies it to the returned namespace. The callback
  returns the base namespace.
- `fingerprint` is the expected value, present when the caller knows it. It is absent on the client produce path,
  where the returned namespace generates the fingerprint.

The request must not carry the Durable Object name of the target, the route context, or `env`. A name would let a
callback give different partitions of one table different namespaces, which breaks the identity rule. `env` is
reachable through the callback's closure or `this` on a Durable Object subclass.

Every call site knows every field:

- The client has `table.name` and the configured jurisdiction.
- A Durable Object reads `shardGroup` from its stored identity and the fingerprints and jurisdiction from its stored
  policy.
- The `FokosDBTopologyCache` entrypoint reads them from `ctx.props`.

Resolution order for one fingerprint:

1. Call the configured `resolveNamespace`. When it returns a namespace, apply `jurisdiction` and require
   `idFromName("fokos.fingerprint")` to equal the expected fingerprint. A mismatch throws a configuration error.
2. When the callback is absent or returns `undefined`, scan `env` values and `ctx.exports` values. A candidate is a
   value on which `idFromName("fokos.fingerprint")` returns a 64-hex ID synchronously. The match applies
   `jurisdiction` to the candidate and compares the fingerprint. The scan must call `idFromName`: method presence is
   not a check, because exports that are not namespaces expose the same names.
3. When nothing matches, throw a configuration error that names the role, the shard group, and the fingerprint.

Each isolate caches the result by fingerprint, like the `fingerprints` map in `src/shared/do-stubs.ts` today.

#### 4.2.3 Client configuration

`FokosDBOptions` extends `FokosTableOptions`, and one `FokosDB` client serves one table, so the namespace options are
per-table by construction. `FokosDBOptions` gets two fields:

```ts
export type FokosDBOptions = FokosTableOptions & {
	/** The Durable Object namespaces of the table, one per role. */
	namespaces?: {
		partitions?: DurableObjectNamespace;
		transactionCoordinators?: DurableObjectNamespace;
		materializations?: DurableObjectNamespace;
		admin?: DurableObjectNamespace;
	};
	/** Resolves a namespace that `namespaces` does not give. Undefined falls back to the fingerprint scan. */
	resolveNamespace?: FokosNamespaceResolver;
	// ...
};
```

`table.ns`, `table.nsTx`, `table.nsMat`, and `nsAdmin` leave the options. The client resolves each configured role
once, computes the fingerprint, and puts it in the policy that every request carries. When a request later needs a
namespace — a stub, a fingerprint for an admin name, a `destroy` walk — the client resolves by fingerprint through
the same resolver.

```ts
new FokosDB(env, {
	namespaces: {
		partitions: ctx.exports.PartitionDO,
		transactionCoordinators: ctx.exports.TransactionCoordinatorDO,
		admin: env.FOKOS_ADMIN_DO,
	},
	resolveNamespace(req) {
		if (req.tableName === "eu_sessions") return env.EU_PARTITION_DO;
		return undefined; // fall back to the scan
	},
});
```

The `PartitionNamespaceKey` type family leaves the public API. `DataNamespaceKey` becomes a role or a fingerprint
selector.

#### 4.2.4 The Durable Object classes

Each class gets one optional protected method beside `fokosConfig()`:

```ts
class MyPartitionDO extends FokosDBPartitionDO {
	protected override fokosNamespace(req: FokosNamespaceRequest): DurableObjectNamespace | undefined {
		return this.ctx.exports.PartitionDO;
	}
}
```

The default implementation returns `undefined`, and the runtime falls back to the scan. A Worker that uses `env` or
`ctx.exports` in the common way configures nothing. `this` gives the override access to `this.env` and `this.ctx`.
Inside a Durable Object constructor, an RPC, and an alarm, the ambient `exports` and `ctx.exports` give the same
namespace values as in the Worker fetch context, so the scan works unchanged in every execution context. A namespace
through `ctx.exports` supports `jurisdiction()` and gives the same fingerprint as the same namespace through `env`.

The `FokosDBTopologyCache` entrypoint gets the same pattern for its `nsAdminFp` lookup. It is already a class the user
re-exports, so the override fits.

`src/server/admin-outbox.ts` and `src/shared/topology-hooks.ts` read the ambient `env` and `exports` today. The
resolver must be passed to them, because ambient `env` inside a Durable Object module is not necessarily the object's
`env`.

#### 4.2.5 Resolution for each call

| Call | Caller | Fingerprint source |
| --- | --- | --- |
| partition → peer partition | partition forwards, repartition | `nsFp` of the stored policy |
| partition → coordinator | stale transaction recovery | `nsTxFp` of the stored policy; the ref fp is a check |
| partition → materialization | change export | `nsMatFp` |
| partition, coordinator → admin | admin outbox, topology cache | `nsAdminFp` |
| coordinator → peer coordinator | route forwards | `nsTxFp` |
| coordinator → participant | prepare, commit, cancel | `nsFp`, also embedded in `fokos.tc.<table>.<fp>` |
| materialization → peer | route forwards | `nsMatFp` |
| materialization → GSI partition | `GsiWriter` | `nsFp`, also embedded in `fokos.m.<table>.<fp>` |
| topology entrypoint → admin | `FokosDBTopologyCache` | `nsAdminFp` of `ctx.props` |

A coordinator or a materialization partition can check `nsFp` twice: the stored policy says it, and the object's own
shard group embeds it. The two must agree at identity load.

The `nsFp` of a table is derivable at every consumer, so a stored `nsFp` is a cross-check and not the mechanism. A
partition finds its own namespace through the `ctx.id` check below, a coordinator reads `fp(ns)` from its own group
name `fokos.tc.<table>.<fp>`, and a materialization partition reads it from `fokos.m.<table>.<fp>`. A partition cloned
into a different namespace for restore thus serves peer traffic before the stored policy updates.

A Durable Object finds its own namespace without a stored value: a candidate where
`ns.idFromName(this.ctx.id.name)` equals `this.ctx.id` is the namespace the object lives in. The check is an assert on
the `nsFp` or `nsTxFp` result, not a lookup mechanism.

#### 4.2.6 Old records and deployment

The change is breaking. New code never reads `ns`, `nsTx`, `nsMat`, or `nsAdmin` strings: not in `__fokos/policy`,
repartition plan heads, `tc_participants.partition_context_json` rows, or `TopologyAdminAddress` props.

- A stored policy of the old shape has no fingerprints. `structurallyEqual` marks it different, and the first request
  of the new shape overwrites it under the normal `policyVersion` rules. The old fields are dead weight until then.
- A call that a Durable Object starts on its own, before any request of the new shape, cannot resolve a foreign
  namespace. The stale transaction recovery on an alarm is the example. It fails with a configuration error and
  retries, and the next request ends the wait. The object's own namespace stays resolvable through the `ctx.id`
  check.
- `CoordinatorRef` keeps two versions: version 2 rows carry `nsTxFp`, and version 1 rows resolve through the `nsTxFp`
  of the stored policy.
- `TopologyAdminAddress` props are caller-computed and cache-held, so new props carry `nsAdminFp` and old props
  expire.

A partition cloned into a different namespace keeps the old stored policy until the first request of the table
arrives with the new fingerprints and a higher `policyVersion`. Its own namespace needs nothing: the `ctx.id` check
resolves it, so peer traffic and the admin-name construction work before the stored policy updates. The restore flow
can also write the new policy into the clone directly, so the calls the object starts on its own work before the
first request. The shard group of a partition, `fokos.p.<table>`, embeds no fingerprint, so its identity and its
routingHash seed stay valid in the new namespace. The same holds for a GSI partition.

A coordinator or a materialization partition needs a name of the new shard group, `fokos.tc.<table>.<fp>` or
`fokos.m.<table>.<fp>` of the new partition namespace, because the name is its address and it embeds `fp(ns)`. The
stored `identity.topology.shardGroup` and `identity.ref.doName` of the clone still carry the old name, and the
identity check rejects the requests of the new name. The restore flow must rewrite both fields. The stored
routingHashes stay valid: the seed scope of `fokosDBSeedScopeOf` is the kind of the group (`fokos.tc`, `fokos.m`),
not the fingerprint, so the seed does not change with the namespace.

The full inventory of what each Durable Object stores against namespace identity:

| Stored item | Holds | Restore into a new namespace | Fixed by |
| --- | --- | --- | --- |
| `__fokos/identity` (partition, GSI) | `fokos.p.<table>`, `fokos.gsiv1.*` — no fingerprint | Nothing: identity, seed, and DO name stay valid | Nothing |
| `__fokos/identity` (coordinator, materialization) | `topology.shardGroup`, `ref.doName` with `fp(ns)` | The identity check rejects the new name | The restore flow writes the KV record with the new group name |
| `__fokos/identity.routingHashSeed` (all) | seed of the group kind (`fokos.tc`, `fokos.m`, `fokos.p`) | Nothing: the scope strips the fingerprint | Nothing |
| `__fokos/policy` (all) | `nsFp`, `nsTxFp`, `nsMatFp`, `nsAdminFp` | Old fingerprints | First request overwrites, or the restore flow writes the record |
| `pending_tx_info.coordinator_json` (partition) | `CoordinatorRef` v2 `nsTxFp`; v1 `doName` with `fp(ns)` | The reference marks the transaction foreign; recovery skips it | Nothing, or a row rewrite |
| `tc_participants.partition_context_json` (coordinator) | Route contexts with old fingerprints | Refreshed by transactions; `fp(ns)` derivable from the own group name | Nothing |
| Repartition plan heads (all) | Queue-time policy and target contexts | An in-flight repartition keeps the old fingerprints | Finish or abandon before the snapshot |
| `producer_fences`, `ancestor_fences` (materialization store) | Hex DO IDs of source-environment producers | Dead references | Cleared on first activation behind the restore marker; see below |
| `producer_fences`, `ancestor_fences` (partition store) | Hex partition DO IDs of source-environment producers | Dead references | Cleared on first activation behind the restore marker; see below |
| Admin name `fokos.topo.<group>~<fp>` | `fp` of the group's namespace in the DO name | New reports use new names | Do not clone admin objects |

A snapshot restore can only write KV items, in a budget of about 3 KB for each clone. `__fokos/identity` and
`__fokos/policy` are KV records, so the restore flow writes them directly: the policy with the new fingerprints, and
for a coordinator or materialization partition the identity with the new group name in `topology.shardGroup` and
`ref.doName`. Every other fixup is SQL and must wait for the first activation. The restore flow therefore also writes
one small marker, `__fokosdb/restored`, into the storage of each clone. On its first activation, behind the same gate
as the startup checks and before any request or background worker runs, a clone that finds the marker runs the SQL
fixup in one transaction and deletes the marker last. A crash rolls the transaction back, and the next activation
runs the fixup again.

The SQL fixup clears the fence tables: `producer_fences` and `ancestor_fences` of the partition store, and
`producer_fences` and `ancestor_fences` of the materialization store. Every ID in them belongs to a Durable Object
of the source environment, so the rows gate nobody. Clearing them is also safe by ownership, not only by dead IDs:
a split source is a router, the export worker never runs on a router (`canRun` is false), and its outbox rows moved
to its children during the migration, so no restored clone can send an `applyLatestImages` call for a range it does
not own. A clone that stayed a leaf owns its range legitimately. A restored child owns the inherited outbox rows and
sends them under its new ID. The fence rows rebuild from live calls, and the per-item revision check stays the
authoritative dedup until they do, so no item of the drain-before-snapshot precondition depends on the fences.

The marker must gate all work: a request, an alarm, or an export worker that runs before the fixup could write a
fence row that a later fixup would then delete. One marker item covers every SQL fixup of the restore flow on that
DO, so the same gate serves any further cleanup the restore needs.

#### 4.2.7 Failure modes

- **Wrong callback answer.** The fingerprint check fails. The request fails with a configuration error. No data moves.
- **No resolver and no match in `env` or `ctx.exports`.** The scan finds nothing and the request fails with a
  configuration error. Same effect as a missing binding today.
- **Wrong fingerprint in a new table config.** The client puts a fingerprint of the wrong namespace in the policy.
  Requests initialize empty objects in that namespace, the same failure as a wrong binding name today.
- **A scan that finds a namespace by fingerprint cannot be wrong.** `idFromName` output is scoped by the namespace and
  the jurisdiction, so a match is the stored namespace.

#### 4.2.8 Push targets (M2)

`PushSinkTargetDefinition.binding` stays a required serializable string in `__fokosdb/mat/targets`. It pins the target
for the drift check of `fokosTargets()` and carries no identity. `FokosDBMaterializationDO` gets:

```ts
protected fokosPushTargetBinding(target: PushSinkTargetDefinition): unknown {
	return (this.env as Record<string, unknown>)[target.binding]
		?? (exports as Record<string, unknown>)[target.binding];
}
```

`pushTargetStub` in `src/shared/do-stubs.ts` takes the resolved binding, applies `objectName` with `getByName`, and
checks `stub[target.method]`. An override selects by `targetId` or `target` and can return `ctx.exports` values, which
expose `getByName` and RPC methods by the same rules as a namespace candidate.

There is no fingerprint check: a wrong binding sends batches to a wrong sink, the same blast radius as a wrong `env`
name today. An unresolvable target fails `checkPushTarget`, and the target runner retries with backoff.

## 5. Alternative options

- **Keep the binding strings, add a `key → namespace` map.** One host callback maps the stored string to a namespace
  in `env` or `ctx.exports`, so storage changes nothing. Rejected: the string is still a user-typed name with the
  `keyof Env` typing problem, a `ctx.exports`-only Worker still cannot express the options, and the mapping must agree
  in the client and every Durable Object class with no check. The fingerprint check is what makes resolution safe.
- **Resolve from context only, store nothing.** The callback gets `tableName`, `shardGroup`, and `role`, and no
  fingerprint persists. Rejected: the partition → `nsTx`, partition → `nsMat`, and → `nsAdmin` edges have no embedded
  proof, so a wrong callback routes silently. A root hash partition auto-initializes from the request context, so a
  misroute writes data.
- **`env[binding] ?? exports[binding]` with the same name.** Rejected: it cannot find a namespace whose `env` and
  `ctx.exports` names differ, and it keeps `keyof Env` in the public types.
- **Scan-only resolution with no callback.** The measured scan cost is small, but a Worker can hold a namespace that a
  scan cannot see, for example a binding to another Worker's class under a name the resolver must not touch. The
  callback is cheap to keep.

## 6. Frequently asked questions

**Do the Durable Object names change when a namespace moves from `env` to `ctx.exports`?** No. The fingerprint is a
property of the namespace, so `fokos.tc.<table>.<fp>` and the admin names keep the same `<fp>`.

**Does per-table `nsTx` or `nsAdmin` keep working?** Yes. The policy holds one fingerprint per role for the table,
which is exactly what the strings expressed.

**What stops a wrong `resolveNamespace` from routing data to a different namespace?** The fingerprint check. The
answer must produce the stored fingerprint, so a wrong answer fails instead of routing.

**What does the scan cost?** Section 8 lists the measured numbers. Each isolate resolves a fingerprint once, and a
deployment with fewer than 10 namespaces resolves in tens of microseconds.

**What if two bindings point to the same namespace?** Both give the same fingerprint. Either one resolves.

**Can a table move to a new namespace?** No, same as today. The fingerprint is part of the identity; a different
fingerprint selects a different set of empty objects.

**What happens when a backup restores the storage of a partition into a different namespace?** The clone resolves its
own namespace through the `ctx.id` check, so peer traffic works on the first request. It keeps the old stored policy
until a request arrives with the new fingerprints and a higher `policyVersion`, then it stores the new policy. Calls
it starts on its own before that request fail and retry. A coordinator or a materialization partition needs its
stored identity rewritten too: `topology.shardGroup` and `ref.doName` embed `fp(ns)`. The routingHash seed needs no
rewrite, because the seed scope is the kind of the group and not the fingerprint.

**Can `resolveNamespace` redirect a restored clone to the new namespaces?** No. The runtime checks the answer of the
callback against the stored fingerprint, and the clone stores the old fingerprint, so the check rejects the correct
answer. Convergence comes from the policy overwrite, or from the restore flow that writes the new policy. Making the
callback authoritative instead would remove the fingerprint check, which is what makes a wrong answer loud.

## 7. References

- `src/shared/partition-context.ts` — `FokosDBPolicy`, `FokosTableIdentity`, `createTableConfig`, the `*NamespaceKey`
  types, `NAMESPACE_FINGERPRINT_LENGTH`.
- `src/shared/do-stubs.ts` — all `env[...]` namespace lookups, `namespaceFingerprint`, `pushTargetStub`,
  `ParticipantCoordinatorAddress`, `TopologyAdminAddress`, `DataNamespaceKey`.
- `src/shared/transaction-wire-types.ts` — `CoordinatorRef` and its version rule.
- `src/server/materialization/targets.ts` — `PushSinkTargetDefinition`.
- `src/server/materialization/do-materialization.ts` — `fokosTargets()` and the stored target list.
- `src/server/topology-entrypoint.ts` — the `TopologyAdminAddress` props flow.
- `docs/agent-plans/2026-10-31-namespace-fingerprints-in-do-names.md` — why the fingerprint is in the object names.
- [ctx.exports — Cloudflare Workers docs](https://developers.cloudflare.com/workers/runtime-apis/context/#exports)

## 8. Appendix: measured cost

Local workerd, `wrangler dev`, a Worker with 1000 `env` Durable Object bindings and 1000 exported Durable Object
classes. Each row is the minimum over 4 to 6 runs. These numbers bound the cost; they are not production limits.

### 8.1 Scan totals, full fingerprint pipeline per candidate, no match

| Scan | Time |
| --- | --- |
| 1000 `env` bindings | ~2–3 ms |
| 1000 `ctx.exports` namespaces | ~6–8 ms |
| `Object.values(env)` over 1000 entries | ~135 µs |

The exports scan is slower because `idFromName` on a `LoopbackServiceStub` goes through the loopback RPC dispatch,
while `idFromName` on an `env` namespace is a native call.

### 8.2 Cost by stage, per 1000-candidate pass

| Stage | `env` | `ctx.exports` |
| --- | --- | --- |
| enumerate + `o[k]` value access | ~0–200 µs | ~0–200 µs |
| + `.idFromName` property read | ~100–300 µs | ~100–200 µs |
| + `idFromName()` call + hex check | ~1300–1600 µs | ~5600–8900 µs |
| repeated `o.NsDO` access | ~0.5–2 ns | ~0–0.5 ns |
| repeated `o.NsDO.idFromName` read | ~0–0.5 ns | ~0–0.5 ns |

Enumeration, key access, and property reads are free on both. The call is the whole cost: ~1.4 µs on `env`, ~6–9 µs
on `ctx.exports`. Key and property access are memoized; `exports[k]` returns a cached stub.

### 8.3 Iteration style

Pure iteration of 1000 entries: `Object.values` ~1400 µs, `Object.keys` ~700 µs, `for...in` ~1000 µs,
`Object.entries` ~1200 µs on `env`; all four land at ~4.4–5.2 ms on `ctx.exports`, where the `.idFromName` property
read dominates. In the full scan all styles are equal, because the `idFromName` call dominates. `for...in` allocates
no array and is never slower.

### 8.4 Consequence

A scan of 2000 candidates costs about 9–11 ms once per unknown fingerprint per isolate, then the cache serves it. A
Worker with fewer than 10 namespaces resolves in tens of microseconds. The candidate count, not the loop style, is
the lever if the cost ever matters; the resolver callback is that lever.
