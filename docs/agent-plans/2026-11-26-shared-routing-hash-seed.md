# RFC — One routingHash seed for the built-in shard groups

**State:** Draft
**Date:** 2026-11-26
**Author:** Lambros Petrou

**Status:** Not built. This spec changes the decision of `docs/agent-plans/2026-11-17-routing-hash-seed-scope.md`.

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

---

## 1. Overview and context

**Today.** FokosDB derives a different effective seed for each kind of shard group. The partition, coordinator, and
materialization groups have different seed scopes. Each GSI also has a seed scope with its index name
(`src/shared/partition-context.ts`, `fokosDBSeedScopeOf`).

The seed scopes do not contain the table name or the namespace fingerprint. This property is necessary for restore.
`docs/agent-plans/2026-11-17-routing-hash-seed-scope.md` keeps different seeds for the group kinds. It says that the
independence costs nothing.

**The problem.** Independence is not free where two groups route the same encoded hash keys. The base partition group
and the materialization group always route the same encoded base hash keys. A base partition exports those keys to the
materialization group. With the default batch limit, different seeds can scatter one export batch over up to 100
materialization leaves.

A key-aligned GSI has the same problem. A key-aligned GSI uses the encoded base hash key as its encoded GSI hash key.
With the default target limit, different seeds can scatter one 100-entry fan-out wave over up to 100 GSI leaves.

The scatter has three costs:

1. One logical batch of 100 random entries can use 87 RPCs in the warm example of Section 4.2.2.
2. The batch waits for the slowest contacted partition.
3. Each receiver stores permanent `producer_fences` rows for more producers.

The scatter also has two benefits:

1. The write load of one upstream partition spreads over more downstream partitions.
2. Split positions, migration windows, collisions, and crafted keys decorrelate.

The correct design must choose between these costs and benefits. FokosDB chooses locality because partitioning is the
scaling mechanism of the system. A smaller partition must also have a smaller downstream dependency set.

**Why now.** The materialization and GSI architecture is new. The seed decides the shape of every export, every GSI
write, and every receiver fence table. A later change breaks every stored routingHash again.

### Glossary

| Term | Meaning |
| --- | --- |
| Deployment base seed | The value derived from `routingHashBaseSeed`, or `GOLDEN_RATIO_BIGINT` when it is absent. |
| Effective seed | The xxHash64 seed that a shard group uses for its keys. |
| Coordinate | The routingHash value of a key and the path that its digits select in a hash tree. |
| Key-aligned GSI | A GSI whose encoded hash key is the same byte sequence as the encoded base hash key. |
| Fan-out wave | One `gsiWriteEntries` call set that the target runner sends before it waits. |
| Dense producer graph | A shape that approaches every upstream partition writing to every downstream partition. |
| Sparse producer graph | A shape where each upstream partition writes inside one contiguous downstream interval. |
| Warm routing | Routing where an entry partition can forward directly to the final owner. |
| Cold routing | Routing where an entry partition forwards level by level. |

## 2. Goals and requirements

**In scope.**

- G1. By default, every built-in shard group of FokosDB uses the same effective seed.
- G2. The deployment can still set `routingHashBaseSeed`.
- G3. The derivation stays fixed and pinned by tests.
- G4. The sharding library keeps its existing `routingHashSeedResolver` hook.
- G5. The spec records the fan-out and availability trade-off.

**Out of scope.**

- A migration of existing stored routingHashes. Every stored routingHash depends on the old seeds.
- A high-level FokosDB API for a custom resolver. [4.2.9](#429-the-seed-api) gives the reason.
- Per-GSI topology overrides. [4.2.10](#4210-gsi-topology-overrides) gives the rule that such a future change must
  keep.
- Destination-selective target batches. The target cursor cannot represent skipped positions without more state.
- A change of `GOLDEN_RATIO_BIGINT`. No better xxHash64 seed value is known.

**Requirements.**

- R1. The default effective-seed derivation must not use a table name, a namespace fingerprint, or a GSI index name.
- R2. Every client and every Durable Object class of a deployment must use the same resolver, and therefore the same
  seed for each shard group.
- R3. After this RFC ships, the derivation must never change after a deployment writes data.
- R4. A custom resolver for FokosDB must be deterministic, pure, stable, and identical in every client and Durable
  Object class. It must not use time, randomness, request data, a table name, or a namespace fingerprint.
- R5. No document or implementation must treat the seed as a security boundary.
- R6. A change of `routingHashBaseSeed` must stay forbidden after a deployment writes data.
- R7. The public documentation of `routingHashBaseSeed` and of the FokosDB seed resolver must describe the shared
  effective seed. It must not say that each shard group gets a different seed.
- R8. Each host must still compute the routingHash of a key with the resolver and the shard group of its own group.
  It must not pass the routingHash of another group through unchanged
  (`docs/agent-plans/2026-11-01-routing-hash.md`, Section 4.2.10).
  With one effective seed the two values are equal, but a custom resolver of the sharding library can give different
  seeds again (G4).
- R9. The FokosDB seed resolver must still throw for a shard group of an unknown format, as
  `fokosDBSeedScopeOf` in `src/shared/partition-context.ts` does today.
- R10. High-level FokosDB must not expose a custom seed resolver. `routingHashBaseSeed` stays its only seed option.
- R11. A future per-GSI topology override must be refused for a GSI whose `hashKey` is exactly `{ ref: "hashKey" }`.

## 3. Milestones

| Milestone | Delivers | Changes stored data |
| --- | --- | --- |
| M1 — One effective seed | All built-in groups share one effective seed. Pinned tests cover the derivation. | Yes. |

## 4. Proposed solution

### 4.1 High-level overview

By default, FokosDB uses one effective seed for every built-in shard group:

```text
effectiveSeed(fokos.p) ==
effectiveSeed(fokos.m) ==
effectiveSeed(fokos.tc) ==
effectiveSeed(fokos.gsiv1...)
```

The FokosDB seed resolver uses the deployment base seed as the effective seed. With no `routingHashBaseSeed`, the
value is `GOLDEN_RATIO_BIGINT`. With a value, the deployment base seed is
`hash64(routingHashBaseSeed, GOLDEN_RATIO_BIGINT)`, as it is today.

The shard-group name is not an input to the FokosDB seed derivation any more. The resolver returns the same 64-bit
value for every shard group.

The partition, materialization, and GSI groups use the same `rootTreesN` and `hashSplitN` for one table. Therefore the
same routingHash selects the same coordinate in each of these groups. The coordinator group can use a different
`coordinatorRootsN`. The important consequence for the data groups is coordinate alignment:

```text
base key K              GSI key K
     |                       |
     +---- hash64(K, S) -----+
                 |
       same routingHash coordinate
                 |
      +----------+----------+
      |          |          |
  fokos.p    fokos.m    fokos.gsiv1
```

The groups do not share Durable Objects because they share a coordinate. Cloudflare places each Durable Object by its
ID. The groups do not synchronize splits. Each group owns its thresholds, runtime, and split state.

The same coordinate helps only when the encoded keys overlap:

- Base and materialization keys always overlap.
- Keys of a key-aligned GSI overlap by design.
- Coordinator tokens and item keys usually do not overlap.
- Keys of an unrelated GSI do not overlap.

When the encoded keys do not overlap, the same effective seed is neutral. xxHash64 already decorrelates different byte
sequences.

The cost is correlation. The partitions that own overlapping keys can become hot, split, and migrate at the same
coordinate in every data group. One crafted key set can also use one seed for every group. FokosDB accepts this cost
because automatic partitioning is its scaling mechanism. Locality gives fewer RPCs and sparser producer graphs where
keys overlap.

### 4.2 Technical details

#### 4.2.1 The derivation

The FokosDB seed resolver computes these values:

```text
deploymentSeed =
    GOLDEN_RATIO_BIGINT
    when routingHashBaseSeed is undefined

deploymentSeed =
    hash64(routingHashBaseSeed, GOLDEN_RATIO_BIGINT)
    when routingHashBaseSeed is present

effectiveSeedHex =
    deploymentSeed.toString(16).padStart(16, "0")
```

The FokosDB seed resolver returns `effectiveSeedHex` for every shard group of a known format (R9). With no
`routingHashBaseSeed`, the value is `000000009e3779b1`. `defaultRoutingHashSeedResolver` of the sharding library does
not change. It still derives a seed from the full shard-group name for direct users of the library.

The existing seed resolver interface returns hexadecimal text (`src/sharding/hash-primitives.ts`,
`RoutingHashSeedResolver`). The runtime parses it as the xxHash64 seed. A custom resolver of the sharding library
replaces this derivation. It can return different seeds for different groups, and then it owns the fan-out and
availability trade-off.

`GOLDEN_RATIO_BIGINT` is not a special xxHash64 seed. The golden ratio is useful as a multiplicative mixing constant.
Here it is only a fixed input value. No full-width golden-ratio constant or random constant has a known distribution
benefit. Stability is the important property.

#### 4.2.2 Base partition to materialization communication

The materialization group copies the topology parameters of the base group. It changes only the shard-group name
(`src/shared/partition-context.ts`, `materializationTableConfig`). Therefore both groups use the same `rootTreesN` and
`hashSplitN`.

The export client uses no topology cache. Therefore each entry partition is a materialization root
(`src/server/do-partition.ts`, `#sendExport`). The caller sends one RPC for each non-empty root at the same time, and
it waits for every answer.

By default, a base export batch contains up to 100 items. With different effective seeds, the materialization
positions of those items are independent of their base positions. For `E` items with independent uniform random
positions and `R` roots, the expected number of non-empty roots is:

```text
G(E, R) = R × (1 - (1 - 1/R)^E)
```

For `E = 100` and `R = 4`, the batch touches about 4 roots. For `R = 100`, it touches about 63 roots.

The roots forward again. Let `M` be the number of leaf hash partitions. The expected number of distinct leaves is:

```text
L(E, M) = M × (1 - (1 - 1/M)^E)
```

For example, use 100 entries with independent uniform random positions, `hashSplitN = 4`, and a full tree with 3
split levels. The table gives rounded expectations for the non-empty nodes from the roots to the leaves, and for the
physical RPC count:

| Roots | Non-empty nodes by level | Warm RPCs | Cold RPCs |
| ---: | --- | ---: | ---: |
| 1 | 1, 4, 16, 51 | 52 | 72 |
| 4 | 4, 16, 51, 83 | 87 | 154 |
| 100 | 63, 89, 97, 99 | 163 | 348 |

Warm routing counts the root level and the leaf level. Cold routing counts every level. Real routing is between these
cases. The table is a model, not a limit. A tree with another shape gives another count. Retries, migrations, and
range promotions can add more RPCs.

With the same effective seed, the exported keys of one base partition stay in one coordinate interval in the
materialization group. The batch enters through one materialization root. When both trees have the same shape, the
successful path can use 2 RPCs with warm routing:

```text
base partition -> materialization root -> materialization leaf
```

With cold routing in a tree of depth 3, the same path can use 4 RPCs:

```text
base partition -> root -> depth 1 -> depth 2 -> leaf
```

The trees can split differently. Therefore 1 final owner is not a guarantee. The guarantee is narrower: the overlap
stays inside the aligned subtree. It does not scatter over the whole materialization tree.

#### 4.2.3 Materialization to GSI communication

A GSI group also copies the topology parameters of the base group (`src/shared/partition-context.ts`,
`gsiTableConfig`). By default, the target runner of one materialization partition claims up to 100 base items for one
target step. At most 2 target workers run at the same time. Each target has at most 1 batch in flight
(`src/server/host-config.ts`).

The `GsiWriter` client uses no topology cache. Therefore each entry partition is a GSI root
(`src/server/materialization/gsi-writer.ts`, `GsiWriter`). It sends one RPC for each non-empty root at the same time,
and it waits for every answer.

One target step sends zero, one, or two fan-out waves:

1. The first wave writes the desired entries or deletes entries of absent items.
2. The second wave deletes old keys after the new entries apply.

One live item normally writes one entry. A key move writes the new key in the first wave and deletes old keys in the
second wave. Failed deletes can leave more old keys for later steps.

Each wave fans out by GSI owner (`src/server/materialization/target-runner.ts`, `TargetRunner`). When one call is
above the RPC size limit, the runner recursively splits it in half and sends the halves in sequence. The occupancy
model of Section 4.2.2 applies to each wave.

The seed changes the fan-out only for key overlap:

| GSI key shape | Same effective seed | Different effective seeds |
| --- | --- | --- |
| Unrelated high-cardinality keys | Scatter | Scatter |
| Key-aligned keys | Local overlap | Scatter |
| Few low-cardinality keys | Few hash positions | Few hash positions |

For unrelated GSI keys, the transformation from base key to GSI key causes the scatter. A seed cannot align keys that
are already different byte sequences.

For key-aligned keys, one materialization partition enters one GSI root with the same effective seed. The final owners
stay inside the aligned subtree. The same 2-RPC warm path is possible when the trees have the same shape.

For low-cardinality keys, the GSI can promote one hash key to a range tree. Inside that range tree, every entry has
the same routingHash. The seed then has no effect. The physical GSI sort key decides the range-tree fan-out. That key
orders by `gsiSK`, then `baseHK`, then `baseSK` (`src/shared/gsi/sort-key.ts`).

#### 4.2.4 Coordinator communication

The coordinator group routes idempotency tokens. Item operations route through the partition group. The coordinator
then drives the participant partitions:

```text
client -> coordinator(token)
client -> partition(item key)
coordinator -> partition(item key)
```

This is a control flow, not a copy flow. The coordinator does not copy a stream of item keys into another coordinator
group. Its position also cannot merge participant RPCs, because the coordinator and each participant are different
Durable Objects. Therefore the seed relation does not change the number of coordinator RPCs.

Automatically generated tokens are random. They do not overlap item keys in normal use. A user can still choose a
token with the same byte sequence as an item key. The groups then share the same routingHash value under one effective
seed. They do not necessarily share a coordinate, because `coordinatorRootsN` can differ from `rootTreesN`
(`src/client/db.ts`, `FokosDB`). Even when the positions correlate, the coordinator and the partition stay in different
Durable Objects.

A different coordinator seed gives no fan-out benefit. It gives only decorrelation when token bytes overlap item-key
bytes. The uniform seed is simpler.

#### 4.2.5 Producer fences

Each materialization receiver stores one permanent `producer_fences` row for each base producer. Each GSI receiver
stores one permanent row for each materialization producer. A receiver split copies the whole table to each child.
One row uses about 84 bytes: the `producer_id` is the Durable Object ID of the producer, 64 hexadecimal characters,
plus the epoch integer and the row overhead of SQLite. Therefore 10,000 producers use about 840 KB in each receiver.

With different effective seeds, the producer graph tends toward a dense shape:

```text
base partitions × materialization partitions
materialization partitions × GSI partitions
```

With the same effective seed, the first graph stays sparse because base keys and materialization keys overlap. The
second graph stays sparse only for key-aligned GSIs. It stays dense for unrelated GSI keys under either seed choice.

The seed cannot solve the producer-fence growth of unrelated GSIs. That growth is a consequence of maintaining a
secondary key order.

#### 4.2.6 Availability trade-off

The same effective seed has these benefits:

- fewer downstream RPCs for overlapping keys,
- lower tail latency for those batches,
- smaller concurrent fan-out,
- sparser producer graphs for overlapping keys,
- clearer partitionwise scaling.

The same effective seed has these costs:

- less downstream load spreading for a hot upstream partition,
- correlated split positions for overlapping keys,
- correlated migration windows,
- reuse of one crafted key set across every group,
- reuse of one collision pair across every group,
- less accidental detection of cross-group routingHash misuse.

Different effective seeds have the inverse trade-off. Their main benefit is shuffle load leveling. Their main cost is
fan-out and dense dependency state.

FokosDB chooses locality. A hot upstream partition can concentrate load in the aligned downstream subtree. This is an
availability risk. The design accepts it because automatic partitioning, not random shuffling, is the scaling model of
the system.

#### 4.2.7 Cursor and batching boundary

A target batch first takes due `target_retry` rows. This part normally uses at most half of the item and byte limits,
but it always takes at least 1 item. The batch then takes change positions after the cursor in sequence order
(`src/server/materialization/target-runner.ts`, `TargetRunner`).

The single cursor means prefix completion. A position becomes resolved when it has a terminal result or another owner
has the work:

- The write applies.
- The runner writes a durable `target_retry` row.
- The runner records a violation, or it removes the retry row of a rejected tombstone.
- A newer revision or an existing retry row owns the work.
- The item moved to another partition, and that partition owns the work.

The cursor does not move after an unknown answer. It also does not move when every item retries.

A different order inside the same contiguous batch does not reduce the RPC count. `resolveAll` already groups the
entries by entry partition before the write (`src/sharding/client.ts`, `FokosShardingClient`), and the runtime groups
them again by owner. A batch that skips positions because they map to other GSI partitions is different. The single
cursor cannot represent those skipped positions.
That design needs per-destination cursors, per-destination queues, or out-of-order completion state.

This RFC does not change target batching. Seed alignment already reduces the number of destination partitions for
key-aligned traffic. The sharding client already counts `clientRpcs` and `totalForwardCount`, but `GsiWriter` drops
them today. A later batching decision needs those measurements.

#### 4.2.8 Migration and rollback

This change alters every routingHash derivation. Existing rows and stored partition identities do not match the new
seed.

A deployment with existing data must not enable this change in place. It must use new namespaces, or it must first use
a migration outside this RFC. A rollback after new data exists has the same constraint.

The identity check is the safety mechanism. A partition whose stored seed differs from the computed seed refuses work
(`src/sharding/runtime.ts`).

#### 4.2.9 The seed API

High-level FokosDB exposes only `routingHashBaseSeed` (R10):

- On the client, it is `FokosDBOptions.routingHashBaseSeed` in `src/client/db.ts`.
- On each Durable Object class, it is the same field of the host config that `fokosConfig()` returns. The host
  config checks it in `src/server/host-config.ts`.

FokosDB builds its resolver from that value with `fokosDBRoutingHashSeedResolverOf`, and passes the resolver to its
sharding clients and runtimes. Only direct users of the sharding library pass a `routingHashSeedResolver` of their
own (G4).

With one effective seed for every group, a FokosDB user has no seed to tune for each group. A custom resolver would
only add a way to break R2: the clients and the Durable Object classes cannot prove that two functions are identical.

#### 4.2.10 GSI topology overrides

The alignment of a key-aligned GSI needs the same `rootTreesN` and `hashSplitN` in the GSI group and in the base
group. Today `gsiTableConfig` always copies the topology of the base table, so no GSI loses the alignment.

`docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md`, Section 4.3.3, lists per-GSI topology overrides as
a future follow-up. A GSI with another topology loses the alignment, and its fan-out goes back to the scatter of
Section 4.2.2. For example, the key `K` maps to root 2 of 4 in the materialization group, and to root 5 of 8 in the
GSI group.

That follow-up must keep R11. The check reads `GsiTargetDefinition` in `src/server/materialization/targets.ts` when it
validates the target list:

```ts
{ kind: "gsi", targetId: "byOwner", hashKey: { ref: "hashKey" }, sortKey: { ref: "data", path: "$.at" } }
```

A `hashKey` of exactly `{ ref: "hashKey" }`, with no `path`, makes the GSI hash key the same bytes as the base hash key.
The check refuses an override of `rootTreesN` or `hashSplitN` on such a GSI with a validation error.

The check cannot find a GSI whose key has the same value by chance, for example `{ ref: "data", path: "$.ownerId" }`
when `ownerId` holds the hash key. Such a GSI can take an override and lose the alignment. The documentation of the
override must say so.

#### 4.2.11 Testing

M1 must add or update these tests:

1. A pinned derivation test for the effective seed with no `routingHashBaseSeed`.
2. A pinned derivation test with a fixed `routingHashBaseSeed`.
3. A test that the partition, coordinator, materialization, and GSI groups return the same effective seed.
4. A test that a stored identity seed mismatch still refuses work.
5. An integration test that base export keys resolve inside the aligned materialization subtree.
6. An integration test that key-aligned GSI entries resolve inside the aligned GSI subtree.
7. A test that the materialization and GSI hosts compute the routingHash of a key with their own shard group (R8). The
   test uses a resolver that gives each group a different seed, because the default resolver now gives equal values.

M1 must also replace the tests that pin the opposite rule:

- `src/shared/partition-context.test.ts`, "gives the partition, coordinator, materialization, and GSI groups of one
  table four different seeds".
- `src/shared/partition-context.test.ts`, "gives two GSIs of one table with different index names different seeds".

M1 must update the doc comments of `fokosDBSeedScopeOf` and `fokosDBRoutingHashSeedResolver` in
`src/shared/partition-context.ts`. It must also mark G3 of `docs/agent-plans/2026-11-17-routing-hash-seed-scope.md` as
replaced by this RFC.

## 5. Alternative options

**Keep different effective seeds.** Rejected. It keeps shuffle load leveling. With the default batch limits, it permits
100-way leaf fan-out in one wave and dense producer graphs where keys overlap.

**Share the seed only between base partitions and materialization.** Rejected. It solves the guaranteed overlap, but it
keeps avoidable scatter for key-aligned GSIs. It also keeps a more complex seed model.

**Share the seed for data groups, but keep a different coordinator seed.** Rejected. The coordinator seed cannot reduce
RPC fan-out. It gives only decorrelation when token bytes overlap item-key bytes. The uniform rule is simpler.

**Use the 64-bit golden-ratio constant `0x9E3779B97F4A7C15`.** Rejected. It has no known benefit as an xxHash64 seed.
The existing constant is stable and sufficient.

**Use a random fixed constant.** Rejected. It has no known benefit over the existing constant.

**Hash one fixed scope before use as the effective seed.** Rejected. It adds one fixed hash step, but it has no known
distribution benefit. Direct use of the deployment base seed is simpler.

**Expose a custom seed resolver in high-level FokosDB.** Rejected. One effective seed leaves no seed to tune for each
group. A resolver function also cannot be proved identical in every client and Durable Object class. A static seed
map or named strategies are easier to audit, but they bring back different seeds without a use case.

**Ignore a topology override on a key-aligned GSI.** Rejected. The override then has no effect, and the user gets no
error. A validation error tells the user at once.

**Select target batch items by destination.** Rejected for this RFC. The single target cursor cannot represent skipped
positions. This optimization needs a separate watermark design.

## 6. Frequently asked questions

**Does one seed make hash quality worse?** No. xxHash64 uses the seed as input to its mixing function. One fixed seed
does not increase the collision probability inside one group.

**Does one seed couple splits?** No. Each group still owns its runtime, storage, thresholds, and split state. One seed
only makes split positions correlate when key domains and sizes correlate.

**Does alignment stop downstream scaling?** No. The downstream group can split the aligned subtree. Alignment removes
random load spreading; it does not remove partitioning.

**Does one seed put two groups in one Durable Object?** No. The Durable Object name and ID decide the object. The
routingHash decides only which object of one shard group owns a key.

**Do unrelated GSIs improve?** No. Their keys are already different byte sequences. Their fan-out stays the same under
either seed choice.

**Do low-cardinality GSIs improve?** Usually no. Their entries share few routingHashes. Promotion moves the entries into
range trees, where the physical sort key decides the fan-out.

**Are crafted keys easier?** Yes. One crafted key set can use the same routingHash value in every group. It can target
the same coordinate in the aligned data groups. This is a weak availability concern, not a security boundary. An
attacker can already craft keys for one public seed at a time.

**Is `routingHashBaseSeed` a secret?** No. It makes one precomputed crafted-key set less reusable across deployments,
but it is not a keyed cryptographic control.

**Why keep `GOLDEN_RATIO_BIGINT`?** It is fixed, public, and sufficient. No better seed value is known for xxHash64.
Changing it would break stored data without a measurable benefit.

## 7. References

- `docs/agent-plans/2026-11-17-routing-hash-seed-scope.md`
- `docs/agent-plans/2026-11-01-routing-hash.md`
- `docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md`
- `src/shared/partition-context.ts`
- `src/shared/gsi/sort-key.ts`
- `src/client/db.ts`
- `src/sharding/hash-primitives.ts`
- `src/sharding/client.ts`
- `src/sharding/runtime.ts`
- `src/server/do-partition.ts`
- `src/server/host-config.ts`
- `src/server/materialization/target-runner.ts`
- `src/server/materialization/gsi-writer.ts`
