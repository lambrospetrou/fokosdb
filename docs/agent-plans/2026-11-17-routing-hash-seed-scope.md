# RFC — The seed of a routingHash comes from the seed scope of its shard group

**State:** Draft
**Date:** 2026-11-17
**Author:** Lambros Petrou

**Status:** Nothing is built. "Today" means the code on `main` at the date of this document.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 M1 — The seed scope](#42-m1--the-seed-scope)
  - [4.3 M2 — A base seed for a deployment](#43-m2--a-base-seed-for-a-deployment)
  - [4.4 Crafted keys](#44-crafted-keys)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

---

## 1. Overview and context

**Today.** Each shard group has one seed for its routingHashes: `routingHashSeedOf(shardGroup)`, the xxHash64 of the
whole shard group name with the base seed (`src/sharding/hash-primitives.ts`). The base seed is the public constant
`GOLDEN_RATIO_BIGINT`. The routing-hash RFC set this rule (`docs/agent-plans/2026-11-01-routing-hash.md`, Section
4.2.4). The shard group names hold the table name, and some hold the namespace fingerprint:

| Shard group | Holds |
| --- | --- |
| `fokos.p.<tableName>` | the table name |
| `fokos.tc.<tableName>.<fp(ns)>` | the table name and the namespace fingerprint |
| `fokos.m.<tableName>.<fp(ns)>` | the table name and the namespace fingerprint |
| `fokos.gsiv1.<n>.<tableName>.<indexName>` | the table name and the index name |

Each partition stores the seed in its identity and refuses all work when the stored seed differs from the derived
seed. Each `routing_hash` column of the partition, the coordinator and the materialization storage holds a value that
this seed gave.

**The problem.** The stored routingHashes of a table are valid only under the name of that table, and for the
coordinator and materialization groups only in one namespace. A restore of a table into a new table name, or into
another namespace, cannot use the stored rows: the client of the new table computes other routingHashes, and each
partition refuses its stored seed. `docs/agent-plans/2026-11-17-consistent-backup-and-restore.md` (M8) needs such a
restore.

**The reasons for a different seed for each group and each table**:

1. **The hash mapping of each group stays independent.** A different seed makes the routingHash of each group an
   independent function of the key. A GSI that keys by the same values as its base table then puts each key at a
   different tree position, so the trees are not mirrors. A routingHash also has no meaning outside its group: one
   passed to another group gives wrong owners.
2. **Crafted keys must not fit every table.** A caller that can choose keys must not build one set of keys that makes a
   lopsided tree in every table.

The evaluation of each reason:

- **Reason 1 holds as a benefit, not as a requirement.** A single seed for all groups would be correct too: the same
  key would land at the same tree position in each group, and a routingHash passed between groups would be a valid
  value. The four seeds buy independence instead. A GSI that keys by the same values as its base table gets an
  uncorrelated tree, so the splits and the migrations of the two groups do not coincide, and a collision pair of one
  group does not collide in another. A routingHash also keeps no meaning outside its group, so the materialization
  rule of the routing-hash RFC — a host must not pass the routingHash of the base partition through unchanged
  (Section 4.2.10) — stays checkable: a foreign routingHash gives wrong owners. The independence costs nothing,
  because the seed scopes already differ.
- **Reason 2 holds only while the table name is not known.** The base seed is public, so anyone who knows the table name
  computes the seed. xxHash64 is not a keyed hash, and with a known seed a caller can construct keys for a chosen
  output (`docs/agent-plans/2026-11-01-routing-hash.md`, Section 4.2.13). A seed for each table therefore makes a
  caller compute a key set for each table, but it does not stop the caller. [4.4](#44-crafted-keys) gives what this
  RFC changes for crafted keys, and M2 gives the mitigation.

A seed for each table gives no other property. Each table has its own Durable Objects, and Cloudflare places a
Durable Object by its ID, not by a routingHash. Two tables that put one hash key at the same tree position share no
resource, and each table splits by its own data.

## 2. Goals and requirements

**In scope.**

- G1. The seed of a shard group comes from its seed scope: its kind, and the index name for a GSI (M1).
- G2. Two tables get the same seed for each kind of group. Two namespaces get the same coordinator seed and the same
  materialization seed (M1).
- G3. The groups of one table keep four different seeds, and two GSIs of one table with different index names keep
  different seeds (M1).
- G4. The client, the router, the runtime and the identity check derive the same seed with no data in a request
  (M1, M2).
- G5. An operator can give a deployment its own base seed, `routingHashBaseSeed`. Without it, the base seed stays the
  public constant `GOLDEN_RATIO_BIGINT` (M2).

**Out of scope.**

- A migration of the stored data. M1 deploys to new namespaces ([4.2](#42-m1--the-seed-scope)).
- A change of `routingHashBaseSeed` in a deployment. Every stored routingHash depends on it
  ([4.3](#43-m2--a-base-seed-for-a-deployment)).

**Requirements.**

- R1. The topology and the requests must not carry the seed, the seed scope or `routingHashBaseSeed`.
- R2. `fokosSeedScope` must throw for a shard group of an unknown format.
- R3. The derivation must stay fixed after the release: a pinned test guards each seed scope, and each seed for a
  fixed `routingHashBaseSeed`.
- R4. Every client and every Durable Object class of a deployment must get the same `routingHashBaseSeed`, or none.

## 3. Milestones

| Milestone | Delivers | Changes stored data |
| --- | --- | --- |
| M1 — The seed scope | The seed comes from the seed scope. The option `routingSeedOf` of the sharding library. | Yes: it deploys to new namespaces. |
| M2 — A base seed for a deployment | The option `routingHashBaseSeed` of FokosDB. Opt-in. | Only for a deployment that sets it, which then starts on new namespaces. |

M1 is M1 of `docs/agent-plans/2026-11-17-consistent-backup-and-restore.md`, and it ships before the other
milestones of that RFC. M2 depends on M1 only. It can ship with M1 or later, because a deployment that does not set
`routingHashBaseSeed` keeps the seeds of M1.

## 4. Proposed solution

### 4.1 High-level overview

The seed of a shard group comes from a short scope string instead of the whole name:

| Shard group | Seed scope |
| --- | --- |
| `fokos.p.<tableName>` | `fokos.p` |
| `fokos.tc.<tableName>.<fp(ns)>` | `fokos.tc` |
| `fokos.m.<tableName>.<fp(ns)>` | `fokos.m` |
| `fokos.gsiv1.<n>.<tableName>.<indexName>` | `fokos.gsiv1.<n'>.<indexName>`, with `n'` the length of the index name |

M1: the seed is `routingHashSeedOf(fokosSeedScope(shardGroup))`, with the public base seed. M2: a deployment can set
`routingHashBaseSeed`, and the seed is then `routingHashSeedOf(fokosSeedScope(shardGroup), baseSeedOf(value))`. The
name `routingHashSeed` stays the derived seed of one shard group, as in the stored identity. The rest of the
routing-hash RFC does not change: the hash function, the width, the identity check and the storage layout stay.

### 4.2 M1 — The seed scope

**The option.** The sharding library does not know the formats of the FokosDB shard groups. `FokosShardingClient`,
`FokosRouter` and `FokosShardingRuntime` take the option `routingSeedOf: (shardGroup: string) => string`. It returns
the seed as 16 hex characters. The default is `routingHashSeedOf(shardGroup)`, so a user of the library alone sees no
change. The option is code, not data, so it does not travel in the topology or in a request (R1). M2 changes only the
function that FokosDB passes, not the library.

FokosDB passes `(shardGroup) => routingHashSeedOf(fokosSeedScope(shardGroup))` from `src/shared/partition-context.ts`
to all three. `fokosSeedScope` reads the format with the same parsers as `baseTableName` and `gsiShardGroup`, and it
throws for an unknown format (R2).

**The derivation.** Each caller of `routingHashSeedOf(shardGroup)` calls `routingSeedOf(shardGroup)`: the router, the
client, the runtime, `routingHashForShardGroup` in `src/sharding/hash-primitives.ts`, and `partitionIdentityFrom` in
`src/sharding/partition-id.ts`. The cache of seeds in `src/sharding/hash-primitives.ts` keys by the function and by the
shard group, so two functions in one isolate never share a cached seed.

**The identity check.** The runtime compares the stored seed with `routingSeedOf(shardGroup)` when it loads the
identity. A difference raises `partition_context_mismatch`, as today.

**What breaks.** The seed of every existing shard group changes. So the stored data of an existing table cannot be
read with the new code:

- An existing partition, coordinator or materialization DO refuses all work with `partition_init_failed`. No write
  goes to a wrong place, and no read gives a wrong item.
- The change deploys to new namespaces, as the routing-hash RFC did (Section 4.2.2). The package version is `0.0.0`,
  and the project has shipped breaking changes of the stored data before.

**What does not break.**

- Routing stays correct: the client, the router and the runtime use the same function (G4).
- The groups of one table keep different seeds, so the mappings stay independent and a routingHash keeps no meaning
  outside its group (G3, reason 1 of [1](#1-overview-and-context)).
- No property of a table depends on a seed of its own ([1](#1-overview-and-context)).

**Tests.**

- A pinned seed for each row of the seed scope table (R3).
- Two tables with different names get the same seed for each kind of group.
- Two namespaces give the same coordinator seed and the same materialization seed.
- The partition, coordinator, materialization and GSI groups of one table get four different seeds. Two GSIs of one
  table with different index names get different seeds.
- `fokosSeedScope` throws for a shard group of an unknown format.
- Two seed functions in one isolate do not share a cached seed.
- The existing tests of the routing-hash RFC pass with the new pinned values.

### 4.3 M2 — A base seed for a deployment

`routingHashBaseSeed` is an input of a hash function, not a credential. It changes where keys go in the trees. It
does not protect data, and it does not encrypt anything.

**The setting.** FokosDB gets the option `routingHashBaseSeed?: string`:

- in the options of `FokosDB` and `FokosDBAdmin`;
- in `fokosConfig()` of `FokosDBPartitionDO`, `FokosDBTransactionCoordinatorDO` and `FokosDBMaterializationDO`. A
  `fokosConfig()` can read `this.env` (`src/server/host-config.ts`), so the value can come from a variable of the
  Worker.

The base seed is `baseSeedOf(value) = hash64(value, GOLDEN_RATIO_BIGINT)`. FokosDB passes
`(shardGroup) => routingHashSeedOf(fokosSeedScope(shardGroup), baseSeedOf(value))` as `routingSeedOf`. An empty string
fails the validation of the options. The value does not go in the route context, the topology or a request (R1).

**Two places that must agree (R4).** The client computes the routingHash to choose a root, and each Durable Object
computes it to store and to route. A Durable Object always uses the `routingHashBaseSeed` of its own class, so a
difference never writes to a wrong place:

- A client with another value sends most keys to a root that does not hold them. The runtime refuses each such
  request with its misroute error ("key outside this partition" in `src/sharding/runtime.ts`). A key that reaches the
  right root by chance is served with the seed of the Durable Object, so its result is correct.
- A Durable Object class with another value than its stored identity refuses all work with `partition_init_failed`,
  as for any change of the seed.

**A change of the value.** Every stored routingHash depends on `routingHashBaseSeed`. A restore copies the stored
routingHashes, so a restore cannot change it either. A deployment that wants another value starts on new namespaces.

**Restores.** A restore into a new table of the same deployment keeps the value, so the copied routingHashes stay
valid (`docs/agent-plans/2026-11-17-consistent-backup-and-restore.md`, M8). A restore into another deployment needs
the same value there.

**Tests.**

- A pinned seed for each seed scope with a fixed `routingHashBaseSeed` (R3).
- With no `routingHashBaseSeed`, the seeds equal the seeds of M1.
- A client with another value than the Durable Objects gets the misroute error, and no item changes.
- A Durable Object class with another value than its stored identity answers `partition_init_failed`.
- An empty string fails the validation.

### 4.4 Crafted keys

**What M1 changes.** Today a caller must know the table name to compute the seeds of a table. After M1, all tables of
all deployments with no `routingHashBaseSeed` share the public seeds of each seed scope. One set of crafted keys then
fits the partition group of every such table, and a table name that callers do not know no longer hides the seed.

**What crafted keys can do.** The cost stays the cost that the routing-hash RFC accepts (Section 4.2.13): extra
splits, deeper routes, more storage and copy work, a promotion from full collisions, and at the hash depth limit, the
write refusal of the targeted interval. A caller must be able to choose the hash keys of the table, and the cost stays
in the table of the caller, because two tables share no Durable Object.

**What M2 gives back.** A deployment with its own `routingHashBaseSeed` has other seeds than every deployment with the
default, so a set of keys that someone built for the default seeds does not fit it. M2 is not a security control:
xxHash64 is not a keyed hash, and a caller who learns the value can compute the seeds again. M2 only removes the reuse
of one key set across deployments. An application that lets untrusted callers choose keys must still control that
cost.

## 5. Alternative options

**Keep a seed for each table.** It ties every stored routingHash to the table name, so a restore into another table
name cannot use the stored rows. Its only gain is that a caller must compute crafted keys for each table, which a
public table name already allows ([1](#1-overview-and-context)). M2 gives a stronger form of that gain. Rejected.

**A base seed for each table.** It ties every stored routingHash to the table again, and a restored table then needs
the base seed of its source. Rejected.

**A seed in the table config, copied to a restored table.** The client computes the routingHash before its first
request, so each client of the restored table must know the seed. Either the user configures it, or the client learns
it with one more round trip from each new isolate. Rejected.

**A compatibility flag for the old seeds.** Existing tables keep the old derivation, and new tables use the seed scope.
A client then needs a setting for each table to know which seed to use, and a restore can copy only a table with the
new seeds. Rejected: the package has no released data to keep.

**One seed for all groups.** It works for routing and stays correct: the same key lands at the same tree position in
each group, and a routingHash is a valid value in every group. But the four mappings collapse into one mirrored
mapping where key domains overlap, and a cross-group routingHash becomes undetectable rather than wrong. It loses
reason 1 of [1](#1-overview-and-context). Rejected: the independence costs nothing, because the seed scopes already
differ.

**A required `routingHashBaseSeed`.** Every deployment must set one. Rejected: most deployments do not let untrusted
callers choose keys, and a required value adds a setting that must never change and that every client must get.

## 6. Frequently asked questions

**Does a shared seed let one table affect another?** No. The tables share no Durable Object, and a Durable Object
lives where its ID puts it.

**Do crafted keys get easier after M1?** A crafted key set becomes reusable across tables and across deployments with
the default base seed ([4.4](#44-crafted-keys)). The cost of such keys does not change, and it stays in the table of
the caller. M2 removes the reuse for a deployment that sets its own `routingHashBaseSeed`.

**Why does the seed scope keep the index name of a GSI?** Two GSIs of one table then keep different seeds, as today.
A restore keeps the GSI definitions, so the index name does not tie the data to a table.

**Why is M2 not part of M1?** A deployment that does not set `routingHashBaseSeed` keeps the seeds of M1, so M2
changes nothing for it, and M2 can ship later with no break. M2 changes stored data only for a deployment that sets
it.

**Is `routingHashBaseSeed` a secret?** No. It is an input of a hash function, like the default base seed. Nothing in
FokosDB depends on callers not knowing it.

## 7. References

References:

- `docs/agent-plans/2026-11-01-routing-hash.md`
- `docs/agent-plans/2026-11-17-consistent-backup-and-restore.md`
