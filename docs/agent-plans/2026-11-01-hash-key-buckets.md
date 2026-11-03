# RFC — hashPrefixBucket: route, promote, and store by a fixed-length hash of the hash key

**State:** Draft
**Date:** 2026-11-01
**Author:** Lambros Petrou

**Status:** Nothing is built. "Today" means the code on `main` at the date of this document.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
  - [1.1 Glossary](#11-glossary)
  - [1.2 The current system](#12-the-current-system)
  - [1.3 Problems](#13-problems)
  - [1.4 Why now](#14-why-now)
- [2. Goals and requirements](#2-goals-and-requirements)
  - [2.1 In scope](#21-in-scope)
  - [2.2 Out of scope](#22-out-of-scope)
  - [2.3 Requirements](#23-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
    - [4.2.1 Invariants](#421-invariants)
    - [4.2.2 Deployment](#422-deployment)
    - [4.2.3 Testing](#423-testing)
    - [4.2.4 M1 — The hashPrefixBucket and the seed](#424-m1--the-hashprefixbucket-and-the-seed)
    - [4.2.5 M2 — Hash routing by digits](#425-m2--hash-routing-by-digits)
    - [4.2.6 M3 — The storage layout](#426-m3--the-storage-layout)
    - [4.2.7 M4 — Promotions by hashPrefixBucket](#427-m4--promotions-by-hashprefixbucket)
    - [4.2.8 M5 — The promoted set in the hash topology encoding](#428-m5--the-promoted-set-in-the-hash-topology-encoding)
    - [4.2.9 M6 — The coordinator storage](#429-m6--the-coordinator-storage)
    - [4.2.10 M7 — The materialization storage](#4210-m7--the-materialization-storage)
    - [4.2.11 M8 — Topology admin objects for each promoted hashPrefixBucket](#4211-m8--topology-admin-objects-for-each-promoted-hashprefixbucket)
    - [4.2.12 Performance and cost](#4212-performance-and-cost)
    - [4.2.13 Crafted hash keys](#4213-crafted-hash-keys)
  - [4.3 Decisions](#43-decisions)
  - [4.4 Follow-ups](#44-follow-ups)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)
- [Appendix A. Measured facts](#appendix-a-measured-facts)
- [Appendix B. A worked example of the digits](#appendix-b-a-worked-example-of-the-digits)

## 1. Overview and context

FokosDB keeps the items of a table in partitions, one Durable Object for each partition. A partition that grows past
its cap splits by a hash of the hash key. It can also move one large hash key into a range tree of its own (a
promotion). Each split and each promotion copies rows into new partitions.

This RFC changes three things:

1. **Routing.** The hash tree routes by the digits of one fixed-length hash of the hash key, the hashPrefixBucket.
   Each hash partition then owns one interval of hashPrefixBuckets.
2. **Promotion.** A promotion moves one hashPrefixBucket, not one hash key. The topology then holds 8-byte values in
   place of variable-length hash keys.
3. **Storage.** `items` and the other key-ordered tables lead with the column `hash_prefix_bucket`, before `hk` and
   `sk`. The rows of a hash child, and the rows of a promoted hashPrefixBucket, are then one key range of the parent.
   A delete of that range frees whole pages.

The behavior of each operation does not change. Each read, write, query, and transaction gives the same result as
today.

The schemas belong to FokosDB. The sharding layer exposes the hashPrefixBucket and helpers for intervals and
ownership. Each host chooses its own schema. A prefix-leading layout is a guideline for indexed migration reads and
contiguous deletes. The sharding layer does not require it from every host table or application index.

The storage order is the precondition of a later split by storage snapshot (clone and prune). That split is not in
this RFC.

### 1.1 Glossary

| Term | Meaning in this document |
| --- | --- |
| hashPrefixBucket | The 64-bit xxHash64 of the encoded hash key, with the seed of the shard group. It is 8 bytes. |
| seed | The xxHash64 seed of the hashPrefixBuckets of one shard group: `xxh64(shardGroup, baseSeed)`. |
| base seed | The xxHash64 seed that derives the seed of each shard group. The default is `GOLDEN_RATIO_BIGINT`. |
| digit | One child index of a hashPrefixBucket: the root digit in base `rootTreesN`, then base `hashSplitN`. |
| node interval | The half-open hashPrefixBucket interval `[start, end)` that one hash partition owns. |
| storage prefix | The first primary-key column of an ordered table. In FokosDB it is the hashPrefixBucket. |
| composite key | A pair `(hk, sk)` of encoded keys, compared by `hk` first and then by `sk`. |
| promoted hashPrefixBucket | A hashPrefixBucket that a promotion moved into a range tree of its own. |
| contiguous delete | A `DELETE` whose rows are one interval of the key order of the B-tree that it deletes from. |
| high-water mark | The value in `item_id_counter`. Every `item_id` below it is reserved. |

The draft `docs/agent-plans/2026-10-31-item-size-facts-and-range-split.md` uses the word "bucket" for one row of
`item_stats`. `keyPairHash` in `sharding/key-codec.ts` uses "bucket" for a slot of a `KeyPairMap`. This RFC and its
code never use the word "bucket" alone. The term is always `hashPrefixBucket`.

### 1.2 The current system

**Routing.** `sharding/hash-primitives.ts` selects a root with `hashRootIndex(hashKey, rootTreesN)` and a child with
`hashChildIndex(hashKey, parentAbsDepth, K)`. Each one is `h32Raw(hashKey, seedForDepth(d)) % N`: an xxHash32 of the
hash key, with one seed for each depth. `FokosRouter.rootContext`, `FokosShardingRuntime#hashesHere`,
`#hashDescendant`, `hashPathOwns`, `HashTopology.findLeaf`, `findLeaf` of `topology-codec.ts`, and `hashKeyInSlice` of
`repartition-slice.ts` use them. `exports-client.ts` exports both functions.

**Names.** `partition-id.ts` names a hash partition `<shardGroup>~h.<root>[.<child>...]` and a range partition
`<shardGroup>~r.<hk>.<start>.<end>`, with `encodeRangeComponent` for each component. A range name holds the whole
hash key, so its length follows the hash key (up to `MAX_HASH_KEY_BYTES`, 1024 bytes).

**Promotions.** A promotion moves one hash key. These structures use the full hash key as their key:
`fokos_route_overrides`, `fokos_range_hierarchy`, `fokos_repartitions.hash_key`,
`fokos_repartition_targets.slice_hash_key`, `__fokos/cache/promotion_bloom`, the topology event, the Bloom filter of
the hash topology encoding, and the range topology encoding (`FKR1`, `fokos_topo_range_intervals`).

**Storage.** `PartitionStore` (`shared/partition/partition-store.ts`) stores items in a rowid table:

- `items.item_id INTEGER PRIMARY KEY` is the rowid. SQLite gives a new row `MAX(item_id) + 1`, so the table pages
  are in insert order, not in key order.
- `UNIQUE (hk, sk)` makes `sqlite_autoindex_items_1`. `idx_items_scan (hk, sk, est_row_bytes)` covers the size
  scans. `idx_items_ttl (ttl_epoch_utc_seconds, hk, sk)` is partial.
- `pending_transactions`, `materialization_outbox`, and `key_size_estimates` are in `hk` order.
- A hash child decides ownership by a hash of the hash key in JavaScript. Thus its migration walks the hash keys one
  at a time (`#walkByHashKey`).

`item_id` has two uses today, and no table links to it. The copy flow carries it (`MigratedItem.item_id`, the
duplicate check of `insertItemIfAbsent`). `deleteExpiredItems` deletes its victims with
`item_id IN (SELECT value FROM json_each(?1))`.

### 1.3 Problems

1. **A delete of a hash slice frees few pages.** A hash child that keeps 1/2 of the rows of its parent keeps 90% to
   94% of the `databaseSize` of the parent at 100 B to 200 B items, 73% at 500 B, and 62% to 72% at 1 KB to
   1.5 KB (Appendix A). A key-range split keeps 69% to 71%, because only the indexes are in key order. `VACUUM`
   fails, and `PRAGMA auto_vacuum` and `PRAGMA incremental_vacuum` fail with `SQLITE_AUTH`. Thus a split by clone
   and prune cannot work on the current layout.
2. **Each insert writes three B-trees.** An insert into `items` counts 3 rows written: the table and two indexes.
   Each row of an index update counts as one more row written
   ([SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)).
3. **Each item stores its key three times.** For a 41-byte hash key and a 32-byte sort key, the key bytes of one
   item are about 220: the row and two index entries.
4. **The topology grows with the hash keys.** Every promoted-key structure holds the full hash key. A range DO name,
   a range topology encoding, a route override, and a topology URL (`topology.GetForRange?...&hk=<b64>`) grow with
   it. The encoded topology RFC leaves the URL length of a long hash key as `TODO: measure`.
5. **No storage order makes a hash split contiguous.** The per-depth xxHash32 seeds make the child of a key
   independent at each level. Thus the rows of one hash child are scattered in any key order.

### 1.4 Why now

- The project is not released: `packages/fokosdb/package.json` has version `0.0.0`. After the public release, a
  change of the primary key of `items` rebuilds each table of each partition.
- The DO name is the address of the partition data. A change of the range names is free only before the release.
- The split by storage snapshot needs the contiguous order. It cannot start before this RFC ships.

## 2. Goals and requirements

### 2.1 In scope

1. Each shard group has one hashPrefixBucket function and one seed. The code derives the seed from the shard group
   name. The topology and the requests do not carry it. Each partition stores it in its identity, and refuses all work
   when the stored seed differs from the derived seed.
2. The hash tree maps a hashPrefixBucket to a partition by its digits. Each hash partition computes its node
   interval, depth, root index, and child path from its partition ID and the immutable topology.
3. `items` is a `WITHOUT ROWID` table with `PRIMARY KEY (hash_prefix_bucket, hk, sk)`. The other key-ordered tables
   of a partition also lead with the hashPrefixBucket.
4. `items.item_id` stays as the link id of future tables. A counter that reserves blocks assigns it.
5. A promotion moves one hashPrefixBucket. A range tree orders its rows by `(hk, sk)` and has composite boundaries.
   Range DO names, route overrides, promotion caches, topology events, and range topology encodings use the
   hashPrefixBucket as their key.
6. The hash topology encoding holds the promoted hashPrefixBuckets as an exact sorted list up to `N` values, and as a
   Bloom filter above `N`. A caller does the same work for a positive answer of either structure.
7. The coordinator uses the same hashPrefixBucket order and digits.
8. A promotion registers with the topology admin object of the table and with a new admin object for its
   hashPrefixBucket. Each later range split in that hashPrefixBucket registers only with the second object.
9. The runtime gives its host opaque storage prefixes, scope intervals, exact ownership checks, and the storage
   interval of each migration target. The host does not know the hash function or the width.
10. The cleanup of items and estimates uses contiguous deletes. Section 4.2.1 lists the exceptions.
11. `latest_items` is `WITHOUT ROWID` on `(hash_prefix_bucket, base_hk, base_sk)`. Its hashPrefixBucket uses the
    seed of the materialization shard group. Its `item_id` stays unique, so its links stay.

### 2.2 Out of scope

- **The split by storage snapshot (clone and prune).** A later RFC. This RFC makes its prune contiguous.
- **A change of the promotion flow.** A promotion keeps the copy flow, the overrides, and the lock rules of today.
  Only its key changes from a hash key to a hashPrefixBucket.
- **A delete of the item rows that a split source keeps.** `cleanupSourceStep` in `do-partition.ts` keeps them today
  (the `FIXME` there). The split by storage snapshot decides it.
- **A redesign of the materialization change log and its linked tables.** M7 clusters `latest_items` and keeps its
  links. `change_positions`, `target_retry`, and `target_index_keys` keep their layouts and behavior.
- **A hashPrefixBucket width of 16 bytes.** The width is the code constant `HASH_PREFIX_BUCKET_BYTES` (8). A width
  of 16 needs another hash function, and after the release it needs its own migration plan.
- **A configurable base seed in the options of `FokosDB`, `FokosShardingClient`, or the runtime.**
  `hashPrefixBucketSeedOf` accepts a base seed, so a platform can use its own. FokosDB uses the default. A follow-up
  spec must give one option that reaches the client, the router, the runtime, and the identity check together. The
  option must not travel in the topology.
- **Splits below the hash depth limit.** The limit is at most 64 (Section 4.2.5). A hash leaf at the limit can still
  promote a hashPrefixBucket, but it cannot hash-split. A follow-up spec must define range splits after the hash
  splits reach the limit.
- **Compatibility with the current layout.** This RFC is a breaking change (Section 4.2.2).

### 2.3 Requirements

This RFC changes the routing, the unit of a promotion, the storage layout, and the topology. Everything else stays:

- The public API, the canonical key encoding, the meaning of expressions, and item identity.
- Transaction guards, recovery rules, retry behavior, page budgets, and cleanup safety rules.
- Each key field, predicate, cursor field, and tie-breaker of today. The hashPrefixBucket is an extra leading column.
  It replaces none of them.
- The item-id uniqueness of both item stores, and the collision check of the migration.

The constraints on the solution:

1. **The hashPrefixBucket function, the base seed, and the seed of a table never change** (I1). A change makes data
   unreachable. A rename of a table makes a new shard group, so it is a new table.
2. **The hashPrefixBucket is never an item identity** (I9). Two hash keys can have one hashPrefixBucket. `KeyPairMap`
   stays the identity of an item in memory.
3. **A topology hint never changes a result.** The rules of
   `docs/agent-plans/2026-10-23-encoded-topology-auto-admin.md` (Section 2.3) stay.
4. **The client must not import a Durable Object class as a value**, and the client entry must not reach a runtime
   module (AGENTS.md). The hashPrefixBucket function and the digit walk go in `sharding/hash-primitives.ts`, which
   both entries use.
5. **DO SQL cannot bind a JavaScript `bigint`** (the comment of `operations_hash` in `do-transaction-coordinator.ts`).
   A stored hashPrefixBucket is a `BLOB`, and the code never converts it to a `number`.
6. **A local handler is synchronous.** The item-id counter must not keep state in memory ahead of storage after a
   rollback.
7. **A step of a background job stays inside the platform limits:** 30 s of CPU for each invocation by default
   ([Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)) and 128 MB of
   memory for each isolate ([Workers limits](https://developers.cloudflare.com/workers/platform/limits/)).
8. **A query has at most 100 bound parameters**
   ([Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)).
9. **No production hook for a test** (AGENTS.md).
10. **The draft item-stats RFC.** `docs/agent-plans/2026-10-31-item-size-facts-and-range-split.md` replaces
    `key_size_estimates` with `item_stats`, and replaces `idx_items_scan`. The RFC that ships second must apply the
    rules of the first. Each `item_stats` row leads with the hashPrefixBucket. The covering indexes of `items` and
    `latest_items` also lead with it. Promotion discovery keeps indexed totals and needs no full scan
    (Section 4.3.1).

## 3. Milestones

Each milestone ships alone and keeps `pnpm check` and `pnpm test` green (run `pnpm test` in a subagent). Before the
release, each milestone edits the schema migrations in place (Section 4.2.2), so the data of an earlier milestone does
not carry over.

1. **M1 — The hashPrefixBucket and the seed** (Section 4.2.4). The functions `hashPrefixBucket` and
   `hashPrefixBucketSeedOf`, the seed in the partition identity, and its check. Routing does not change yet.
   **DONE.**
2. **M2 — Hash routing by digits** (Section 4.2.5). Every hash route uses the digits of the hashPrefixBucket. Each
   hash partition knows its node interval. The depth limit is a split floor. Storage does not change yet. **DONE.**
3. **M3 — The storage layout** (Section 4.2.6). `items` is `WITHOUT ROWID` on `(hash_prefix_bucket, hk, sk)`, with
   the item-id counter and a TTL sweep by primary key. `pending_transactions`, `materialization_outbox`, and
   `key_size_estimates` lead with the hashPrefixBucket. A hash child reads its migration rows as one interval. **DONE.**
4. **M4 — Promotions by hashPrefixBucket** (Section 4.2.7). A promotion moves one hashPrefixBucket. Range names,
   overrides, slices, the hierarchy, caches, events, and range topology encodings use the hashPrefixBucket and composite
   boundaries. The size estimates become one indexed total for each hashPrefixBucket. **DONE.**
5. **M5 — The promoted set in the hash topology encoding** (Section 4.2.8). The hash topology encoding holds a sorted
   list of hashPrefixBuckets up to `N`, and a Bloom filter above `N`. Each positive answer fetches the range item in
   the background.
6. **M6 — The coordinator storage** (Section 4.2.9). The `tc_*` tables lead with the hashPrefixBucket of the token.
7. **M7 — The materialization storage** (Section 4.2.10). `latest_items` is `WITHOUT ROWID` on
   `(hash_prefix_bucket, base_hk, base_sk)`. A local counter assigns `item_id`, and `UNIQUE (item_id)` keeps the
   links and the collision check.
8. **M8 — Topology admin objects for each promoted hashPrefixBucket** (Section 4.2.11). A promotion registers with
   two admin objects. A range split registers only with the object of its hashPrefixBucket, and callers read the
   range topology encoding from that object.

## 4. Proposed solution

### 4.1 High-level overview

**The hashPrefixBucket.** Each hash key gets a 64-bit hashPrefixBucket: `xxh64(encodedHashKey, seed)`. The seed is
`xxh64(shardGroup, baseSeed)`, so each table, GSI, coordinator group, and materialization group has its own seed. The
code derives the seed, and each partition stores it in its identity to detect a change. The sort key
is not part of the hashPrefixBucket, so all items of one hash key have one hashPrefixBucket.

**The digits.** Read the hashPrefixBucket as a fraction `x` in `[0, 1)`. The root is the first digit of `x` in base
`rootTreesN`. Each level below is the next digit in base `hashSplitN`. Thus each hash partition owns one interval of
hashPrefixBuckets, and its children divide that interval into `hashSplitN` equal parts:

```
hashPrefixBucket space [0, 2^64)        rootTreesN R = 2, hashSplitN K = 2
├── root 0: [0, 2^63) ──── split ──► child 0.0: [0, 2^62)       child 0.1: [2^62, 2^63)
└── root 1: [2^63, 2^64)            (no split)
```

**The storage order.** Each table that a repartition cleans up keeps its rows in the order
`(hash_prefix_bucket, hk, sk)`. Thus the rows of a hash child are one key range of the parent, and so are the rows
of a promoted hashPrefixBucket:

```
items B-tree of a hash partition, in primary-key order
┌───────────────────────────────┬──────────────────────┬───────────────────────────────┐
│ hashPrefixBuckets of child 0  │ promoted b7          │ hashPrefixBuckets of child 1  │
│ (hash_prefix_bucket, hk, sk)  │ (b7, hkA, sk) ...    │ (hash_prefix_bucket, hk, sk)  │
└───────────────────────────────┴──────────────────────┴───────────────────────────────┘
  a prune of child 1 deletes      a promotion cleanup    a prune of child 0 deletes
  this range                      deletes this range     this range
  Each delete is one key range, so it frees whole pages.
```

**Promotions.** A promotion moves one hashPrefixBucket to a range tree. The range tree orders its rows by `(hk, sk)`,
so its boundaries are composite keys. Normally a hashPrefixBucket holds one hash key. Two hash keys that share all 64
bits go to the same range tree. This is correct, and it is rare.

**The topology.** The hash topology encoding holds the split bitmap, which does not change, and the promoted
hashPrefixBuckets: an exact sorted list of 8-byte values up to `N`, and a Bloom filter above `N`. A range topology
encoding uses the hashPrefixBucket as its key, and a range DO name starts with the 16 hex characters of the
hashPrefixBucket. Each promoted hashPrefixBucket gets its own topology admin object, so the reads of many range
topology encodings do not go to one object. The topology stores only fixed-length values, except the composite
boundaries inside a promoted hashPrefixBucket.

**The runtime API.** Each partition knows its scope interval in `(hash_prefix_bucket, hk, sk)` order, also a router
that owns no rows. The runtime gives the host opaque storage prefixes, interval helpers, exact ownership checks, and
the storage interval of each migration target. The host does not calculate hashes, digits, or prefix boundaries.

### 4.2 Technical details

#### 4.2.1 Invariants

Each invariant has its rule, then its mechanism, then the milestones that add it.

- **I1 — `hashSplitN`, `rootTreesN`, and the seed of a shard group never change after initialization.**
  `hashSplitN` and `rootTreesN` are in `FokosTopology`, and `topologiesEqual` compares them. The seed is
  `hashPrefixBucketSeedOf(shardGroup)`. `partitionIdentityFrom` stores it in `FokosPartitionIdentity`. When a
  partition loads its identity, it compares the stored seed with the derived seed. A difference fails the
  initialization: every operation answers `partition_init_failed`, and `fokosStatus` and the destroy RPCs stay
  available. A pinned test of the seed and of one hashPrefixBucket catches a change of the derivation before a
  deployment. (M1)
- **I2 — A table has one hashPrefixBucket width, and a partition never mixes widths.** `storagePrefix` returns
  exactly `HASH_PREFIX_BUCKET_BYTES` bytes in every partition. (M1, M3)
- **I3 — The digits are exact up to the depth limit, and no hash partition exists below it.** The limit is
  `R * K^d <= 2^64` and `d <= MAX_HASH_DEPTH`. Validation refuses a deeper partition ID. The split decision returns
  the floor at the limit. (M2)
- **I4 — The digit walk of a hashPrefixBucket and the node interval agree.** Both use the same integer arithmetic
  (Appendix B). A property test checks it. (M2)
- **I5 — Each partition knows its scope interval in `(hash_prefix_bucket, hk, sk)` order.** A hash partition has a
  hashPrefixBucket interval. A range partition has one hashPrefixBucket and an `(hk, sk)` interval.
  `storageInterval()` gives this immutable scope, also on a router. `ownedStorageInterval()` is null on a router.
  Its result bounds ownership, but it can include promoted hashPrefixBuckets. `owns` and `ownerCheck` check the
  actual ownership from durable facts. (M2, M3, M4)
- **I6 — The cleanup of items and estimates uses contiguous deletes in the prefix-leading primary B-tree.** Sections
  4.2.6 and 4.2.10 give the predicates. The exceptions are below. (M3, M6, M7)
- **I7 — The item-id counter never gives out an id that a committed row holds.** The high-water mark only increases.
  Memory moves to a new block only after the commit. An ingest raises the mark above each copied id. (M3, M7)
- **I8 — A range tree owns exactly one hashPrefixBucket, and all items of that hashPrefixBucket.** The promotion slice
  is `{ kind: "promoted_hash_prefix_bucket", hashPrefixBucket }`. Its copy reads every row of the hashPrefixBucket.
  (M4)
- **I9 — The hashPrefixBucket is never an item identity.** An application-key lookup keeps both key components after
  the hashPrefixBucket: `hk` and `sk`, or `base_hk` and `base_sk`. Internal materialization links keep their unique
  item-id lookups. A size estimate can use the hashPrefixBucket alone as its key, because it describes a group, not
  an item. (M3, M4, M7)
- **I10 — The encoding of a DO name of this RFC never changes after the release.** The same rule as for
  `encodeRangeComponent` today. (M4)

**The exceptions to I6.** I6 covers the primary B-trees of `items`, `latest_items`, `key_size_estimates`, and the
`tc_*` tables. These B-trees have no contiguous delete:

- `pending_transactions` and `materialization_outbox`, with their prefix-leading unique indexes. Both hold in-flight
  rows: open transaction locks, and changes that the export has not sent. Lock cleanup deletes whole transactions,
  and the keys of one transaction can be apart in the index. Outbox cleanup keeps its bounded rowid batches.
- The materialization tables `change_positions`, `target_retry`, and `target_index_keys`. Their log order and item-id
  links stay. M7 deletes the linked rows with their item in the same transaction.
- Each index that leads with another column: `idx_items_ttl`, `pending_transactions_transaction_id`,
  `pending_tx_info_due`, `key_size_estimates_by_bytes`, `idx_materialization_outbox_due`, the two time indexes of
  `tc_state`, the item-id indexes of both stores, and the tombstone-time index of the materialization store.

Section 4.2.12 gives the cost of the exceptions.

#### 4.2.2 Deployment

This RFC is a breaking change. It keeps no compatibility with the current layout. The project is not released
(Section 1.4), so no table holds data that must survive the change. The code keeps one path, and the topology has no
format version.

- Each milestone edits the schema migrations of `PartitionStore`, `KeySizeStats`, `FokosShardingStore`,
  `FokosTopologyKeeper`, `FokosDBTransactionCoordinatorDO`, and `MaterializationStore` in place. It adds no migration
  entry.
- The range ID schema `0x01`, the range DO names, and the layouts of the hash and range topology encodings change in
  place. The magics of the topology encodings (`FKT1`, `FKR1`), their schema 1, and the path segment `v1` stay.
- A deployment uses new namespaces, or calls `destroy()` on each table first. Existing Durable Objects are destroyed,
  not migrated. `examples/http-api` needs the same step.
- The first deployment of each milestone that changes a topology encoding runs with
  `cache.cross_version_cache: false`. Workers Cache then keys each entry by the Worker version, so the new code never
  reads a topology encoding that the old code cached
  ([cache keys](https://developers.cloudflare.com/workers/cache/cache-keys/)).
- Rollback: revert the code and deploy to new namespaces again.
- After the public release, the layout of this RFC is fixed. A later change of the hashPrefixBucket function, the
  width, or the storage order needs its own migration plan.

#### 4.2.3 Testing

The behavior of each operation does not change (Section 2.3). Thus the existing suites are the main proof of
correctness, and a milestone is done when they pass.

- When a test fails only because a key routes to another partition, a DO name or a partition ID has a new form, or a
  statement has a new column, update the test to the new value. Do not change what the test checks.
- When a test needs a key in a given partition, a test helper tries keys until the digits of the hashPrefixBucket
  match. This is cheap at a small depth.
- When a test needs two hash keys with one hashPrefixBucket, a test-only key generator constructs them. xxHash64 is
  not a cryptographic hash, so a generator can do this (Section 4.2.13). The test checks the real hash, and it adds
  no production hook.

Each milestone below lists only its new test cases: the behavior that no current test covers.

#### 4.2.4 M1 — The hashPrefixBucket and the seed

**The functions.** In `sharding/hash-primitives.ts`:

```ts
/**
 * The xxHash64 seed of the hashPrefixBuckets of a shard group, as 16 lowercase hex characters: xxh64
 * of its UTF-8 name with `baseSeed`. A platform can give its own `baseSeed`, and must then keep it for
 * the life of its data.
 */
export function hashPrefixBucketSeedOf(shardGroup: string, baseSeed: bigint = GOLDEN_RATIO_BIGINT): string;

/** The hashPrefixBucket of an encoded hash key: xxh64(hashKey, seed), as 8 bytes, most significant byte first. */
export function hashPrefixBucket(hashKey: KeyBytes, seed: bigint): Uint8Array;
```

- `hashPrefixBucketSeedOf` is `hash64(shardGroup, baseSeed)`. The default base seed `GOLDEN_RATIO_BIGINT` must never
  change.
- The seed is a hex string, so it stays JSON-safe in the stored identity. A caller that hashes parses it to a
  `bigint` once for each shard group (M2).
- The hashPrefixBucket is `hashers.h64Raw(hashKey, seed)`, written big-endian with
  `DataView.setBigUint64(0, h, false)`. Thus the unsigned byte order of the BLOB is the numeric order of the hash,
  with no sign-bit change.
- The input is the `KeyCodec` bytes of the hash key: the bytes that the partition stores in `hk`.
- The width is the code constant `HASH_PREFIX_BUCKET_BYTES` (8). No type, request, or stored record holds it.
- `xxhash-wasm` is already a dependency of `hash-primitives.ts`, so no package is added.

**The topology does not change.** `FokosTopology`, `validateTopology`, `topologiesEqual`, the topology event, the
keeper tables, the headers of the topology encodings, and the cache keys do not get a seed or a width. Each derived
shard group (the coordinator group, the materialization group, and each GSI) still makes its topology with
`{ ...table.topology, shardGroup }`. The seed comes from the shard group name, so the spread is correct.

**The seed in the partition identity.** In `sharding/route-context.ts`:

```ts
export type FokosPartitionIdentity = {
	// ... the fields of today ...
	/**
	 * `hashPrefixBucketSeedOf(topology.shardGroup)` when the identity was written. A partition whose
	 * stored seed differs from the seed that the code computes now refuses all work, because its
	 * stored rows and its routes use the stored seed.
	 */
	hashPrefixBucketSeed: string;
};
```

- `partitionIdentityFrom` sets the field for a hash partition and for a range partition. A partition writes its
  identity once, at bootstrap or at `fokosInit`.
- When the runtime loads the identity in its constructor, it puts the identity in memory, then compares the stored
  seed with `hashPrefixBucketSeedOf(identity.topology.shardGroup)`. A difference raises
  `partition_context_mismatch` into the initialization error of the runtime. Every operation then answers
  `partition_init_failed`, the alarm retries at `INIT_RETRY_ALARM_MS`, and `fokosStatus`, `fokosPrepareDestroy`, and
  `fokosDestroy` stay available. A deployment of the old derivation heals the object.
- The coordinator and the materialization partitions host the same runtime, so they get the same check.
- An identity without the field fails the check. Section 4.2.2 deploys to new namespaces, so no such identity
  exists.

**Code that changes.** `sharding/hash-primitives.ts`, `sharding/route-context.ts` (the identity type),
`sharding/partition-id.ts` (`partitionIdentityFrom`), `sharding/runtime.ts` (the check at load), and
`sharding/exports-client.ts` (it exports `hashPrefixBucket` and `hashPrefixBucketSeedOf`).

**Invariants.** I1, I2 (the width).

**New tests.**

- `hashPrefixBucket` gives 8 bytes, big-endian, equal to `h64Raw` of the key bytes. A fixed shard group and a fixed
  key give a pinned seed and a pinned hashPrefixBucket, so a change of the derivation or of the function fails the
  test.
- A custom base seed gives `hash64(shardGroup, baseSeed)`, and the default equals `GOLDEN_RATIO_BIGINT`.
- The partition, coordinator, materialization, and GSI groups of one table get four different seeds.
- `partitionIdentityFrom` stores the seed of the shard group, for a hash identity and for a range identity.
- A partition whose stored seed was changed answers `partition_init_failed`, still answers `fokosStatus`, and can
  still be destroyed.

#### 4.2.5 M2 — Hash routing by digits

**The digit walk.** In `sharding/hash-primitives.ts`, a hashPrefixBucket is two 32-bit limbs `x = hi * 2^32 + lo`.
One step with the base `M` (`rootTreesN` at the root, `hashSplitN` below) is:

```
a      = lo * M                      // < 2^48: exact in a JavaScript number
b      = hi * M + floor(a / 2^32)    // < 2^48 + 2^16: exact
digit  = floor(b / 2^32)
hi'    = b mod 2^32
lo'    = a mod 2^32
```

`x * M = b * 2^32 + lo'`, so `digit = floor(x * M / 2^64)` and `x' = (x * M) mod 2^64`. `rootTreesN` is at most
65,000 (`FOKOS_HASH_PARTITIONS_MAX`) and `hashSplitN` at most 255, so each product is below 2^53. The walk allocates
no `bigint`.

```ts
/** The digits of a hashPrefixBucket: the root digit, then one digit for each level, `depth` levels. */
export function hashPrefixBucketDigits(
	hashPrefixBucket: Uint8Array,
	rootTreesN: number,
	hashSplitN: number,
	depth: number,
): { root: number; path: number[] };

/** A cursor over the digits, for a walk that stops early (findLeaf, the hash arena). */
export class HashPrefixBucketDigitCursor {
	constructor(hashPrefixBucket: Uint8Array, rootTreesN: number);
	next(base: number): number;
}
```

**The node interval.** A node with the root `r` and the path `c1 .. cd` owns the real interval `[L, L + 1/D)`, with
`D = R * K^d` and `L = numer / D`, where `numer = (((r * K + c1) * K + c2) ... ) * K + cd`. Its hashPrefixBucket
interval is:

```
start = ceil(numer * 2^64 / D)
end   = ceil((numer + 1) * 2^64 / D)      // no upper bound when numer + 1 = D
```

- The code computes `start` and `end` with `bigint` once for each identity, and keeps them as 8-byte BLOB values.
- The last node of a level has `numer + 1 = D`. Its `end` is `2^64`, which has no 8-byte form. Thus its interval has
  no upper bound, and its statements use only `hash_prefix_bucket >= start`.
- A hashPrefixBucket `x` is in the interval if and only if its digit walk gives the path of the node (I4,
  Appendix B).
- No boundary is stored. The partition ID gives `r` and the path, as today. The hash split bitmap does not change.

**The depth limit.** The digits stay exact while `R * K^d <= 2^64`, because each interval then holds at least one
hashPrefixBucket. The limit of a topology is:

```
maxHashDepth(R, K) = min(MAX_HASH_DEPTH, the largest d with R * K^d <= 2^64)
MAX_HASH_DEPTH     = 64
```

Examples: `K = 2, R = 1` gives 64. `K = 2, R = 16` gives 60. `K = 4, R = 16` gives 30. `K = 255, R = 65,000`
gives 6. `R * K` is below 2^24, so every valid topology allows depth 1.

`MAX_HASH_DEPTH` bounds the depth of a lopsided branch (Section 4.2.13), the arena budget of `HashTopology`, and the
depth byte of the partition ID (at most 255). With `K = 2` and `R = 1`, 64 levels address 2^64 leaves under one root.
This is the address space, not an allocation of leaves. A node at this limit holds one hashPrefixBucket.

- `validateTopology` computes the limit. `partitionIdentityFrom` and `PartitionIdHelper.hashDescendantId` refuse a
  hash partition ID deeper than the limit, with `partition_context_options_invalid`.
- A hash leaf at the limit cannot split. `SplitPlanner#decide` returns the new floor `max_depth` for it, after the
  promotion step: a leaf at the limit still promotes its large hashPrefixBuckets. The split decision of the
  coordinator does the same.
- The hash arena (`HashTopology.create`) takes `maxDepth` relative to its owner: `maxHashDepth - ownerAbsDepth`.

**Code that changes.** Each user of `hashRootIndex` and `hashChildIndex` changes to the digits of the
hashPrefixBucket:

| Code | Change |
| --- | --- |
| `sharding/router.ts` `FokosRouter.rootContext` | the root digit of `hashPrefixBucket(hashKey, seed)` |
| `sharding/runtime.ts` `#hashesHere`, `#hashDescendant`, `hashPathOwns` | one hashPrefixBucket for each key |
| `sharding/hash-topology.ts` `findLeaf`, `updateFromHint`, `invalidate` | `HashPrefixBucketDigitCursor` |
| `sharding/topology-codec.ts` `findLeaf` | `HashPrefixBucketDigitCursor`; the root digit replaces `hashRootIndex` |
| `sharding/repartition-slice.ts` `hashKeyInSlice` | the digit at `slice.depth - 1` |
| `sharding/client.ts` (`FokosShardingClient.resolve`, `resolveRange`) | one hashPrefixBucket for each key |
| `sharding/partition-id.ts` | the re-exports go; `PartitionIdHelper` gets `hashInterval(partitionId, topology)` |
| `sharding/exports-client.ts` | adds `hashPrefixBucketDigits` and `hashInterval`; drops the two old functions |

`seedForDepth`, `SEED_DEPTH_PRIME`, and the per-depth seeds go. The router, the client, and the runtime get the seed
from `hashPrefixBucketSeedOf(topology.shardGroup)`, and keep its `bigint` form for each shard group. The topology does
not carry it (Section 4.2.4). Each resolve computes the hashPrefixBucket of a key once and passes it to each step. `RouteKey` can carry it for the length of one call. The runtime must not keep it
across calls, because the key bytes of a later call can differ.

**The runtime identity.** `FokosPartitionIdentity.hash` becomes `{ rootIndex, path, depth, start, end }`. `depth`
equals `path.length`, and `end` is null for the last node. `partitionIdentityFrom` computes these fields. A hash
partition keeps them after a split cutover. The root index and the path stay, although the interval also gives them.

**Invariants.** I3, I4, and the hash part of I5.

**New tests.**

- The limb step equals a `bigint` reference for random hashPrefixBuckets, every base from 2 to 255 and from 1 to
  65,000, and the values `0`, `2^64 - 1`, and each interval edge.
- A property test, for random `R`, `K`, depths up to the limit, and hashPrefixBuckets: the digit walk gives the path
  of a node if and only if the hashPrefixBucket is in its interval. The intervals of the `K` children tile the
  interval of the parent, with no gap and no overlap.
- `maxHashDepth` gives the examples above. A partition ID deeper than the limit fails.
- A hash leaf at the limit with two hashPrefixBuckets and no large one returns `max_depth`. With a large one, it
  promotes that one. A final interval with one hashPrefixBucket promotes it at any estimate. Use a topology whose
  final interval can hold two values.
- A router keeps its interval, root index, path, and depth after the cutover.

#### 4.2.6 M3 — The storage layout

##### The runtime API for storage intervals and ownership

The host treats a storage prefix as opaque bytes. It must not calculate the hash, its digits, or prefix boundaries.
`sharding/runtime-types.ts` and `sharding/runtime.ts` expose these helpers:

```ts
/** A position in the storage order (prefix, hk, sk). Absent `hk` and `sk` are the empty key, the minimum. */
export type FokosStorageKey = { prefix: Uint8Array; hk?: KeyBytes; sk?: KeyBytes };

/** A half-open storage interval. A null end has no upper bound. */
export type FokosStorageInterval = { start: FokosStorageKey; end: FokosStorageKey | null };

/** The storage prefix of a hash key in this partition: the first primary-key column of its rows. */
storagePrefix(hashKey: KeyBytes): Uint8Array;

/** This node's immutable scope interval. A router retains this interval but owns no rows. */
storageInterval(): FokosStorageInterval;

/**
 * Bounds the rows this node owns. Null on a router. Moved prefixes inside the interval remain in
 * these bounds, so a host must also use the exact ownership check before it changes rows.
 */
ownedStorageInterval(): FokosStorageInterval | null;

/** Exact current ownership from durable facts, without cache hints or outbound calls. */
owns(key: RouteKey): boolean;

/** An exact checker for one synchronous pass. It must not survive an await or a topology change. */
ownerCheck(): (key: RouteKey) => boolean;
```

- `storageInterval()` comes from the partition identity and the immutable topology. It stays available after the
  cutover. It describes the address space of the partition. It does not give permission to write the old copies.
- The scope of a hash partition is `[(start), (end))`, its node interval. After M4, the scope of a range partition
  uses its hashPrefixBucket and its composite boundaries. An unbounded start is `(p, x'', x'')`. An unbounded end is
  the start of the next prefix, which the runtime calculates. Only the last prefix has no upper bound. Before M4, the
  range helpers also limit the interval to the one hash key of the range tree.
- A FokosDB host turns an interval into one row-value predicate:
  `(hash_prefix_bucket, hk, sk) >= (?, ?, ?) AND (hash_prefix_bucket, hk, sk) < (?, ?, ?)`. A hash bound uses `x''`
  for `hk` and `sk`. `KeyCodec` never makes `x''` for a real hash key, so `(start, x'', x'')` is below every row of
  the hashPrefixBucket `start`. With a null end, the host omits the upper predicate. These scope bounds do not replace
  the full cursor of a stream ("The migration read" below).
- `owns` and `ownerCheck` exclude terminal promotions and answer false on a router. The batch checker reads the
  shared router facts once. It makes no topology-cache lookup and no peer RPC. Ownership does not bypass the
  migration guard: an importing target still refuses writes.
- The migration runtime gives a `FokosStorageInterval` with each `MigrationHost.buildPage` call. It computes the
  interval from the validated target reference, the slice, the source identity, and the immutable topology. The host
  does not compute it from `childIndex` and `depth`. After the cutover, `ownedStorageInterval()` of the source is
  null, but the target interval stays available.
- The SQL column name is `hash_prefix_bucket`, so a reader of the schema can find it. The host code treats the value
  as opaque bytes.

Until M4, a promotion still moves one hash key, and its migration and cleanup predicates keep that hash key. M3 must
not move or delete another hash key with the same hashPrefixBucket. M4 changes these predicates to the whole
hashPrefixBucket.

##### The `items` table

```sql
CREATE TABLE IF NOT EXISTS items (
    hash_prefix_bucket    BLOB    NOT NULL,
    hk                    BLOB    NOT NULL,
    sk                    BLOB    NOT NULL DEFAULT x'',
    item_id               INTEGER NOT NULL,
    data_kind             INTEGER NOT NULL DEFAULT 0,
    v                     INTEGER NOT NULL,
    last_read_ts          INTEGER NOT NULL DEFAULT 0,
    last_write_ts         INTEGER NOT NULL DEFAULT 0,
    ttl_epoch_utc_seconds INTEGER,
    est_row_bytes         INTEGER NOT NULL,
    data                  ANY     NOT NULL,

    PRIMARY KEY (hash_prefix_bucket, hk, sk)
) WITHOUT ROWID, STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_items_item_id ON items (item_id);

CREATE INDEX IF NOT EXISTS idx_items_scan ON items (hash_prefix_bucket, hk, sk, est_row_bytes);

CREATE INDEX IF NOT EXISTS idx_items_ttl ON items (ttl_epoch_utc_seconds)
    WHERE ttl_epoch_utc_seconds IS NOT NULL;

CREATE TABLE IF NOT EXISTS item_id_counter (
    id  INTEGER PRIMARY KEY CHECK (id = 1),
    hwm INTEGER NOT NULL
) STRICT;
INSERT OR IGNORE INTO item_id_counter (id, hwm) VALUES (1, 1);
```

- `WITHOUT ROWID` keeps the rows in the primary-key B-tree. Thus the table pages are in
  `(hash_prefix_bucket, hk, sk)` order, and a delete of a key range frees whole pages.
- The record stores the primary-key columns first and the other columns in declared order, so `data` stays last. A
  read of the metadata columns does not read the overflow pages of `data`.
- The schema comment of the migration replaces the rule "Do NOT change it to WITHOUT ROWID". The new comment gives
  the cost and the reason: a row above the inline payload limit of an index B-tree (about 1,002 bytes on a 4 KiB
  page) gets overflow pages that no other row can use. Appendix A gives the measured cost.
- `idx_items_ttl` declares only `ttl_epoch_utc_seconds`. A `WITHOUT ROWID` index appends the primary key, so each
  entry is `(ttl, hash_prefix_bucket, hk, sk)`. The implementation must check with `EXPLAIN QUERY PLAN` that the sweep
  seeks on its row-value cursor over the appended columns. If it does not, declare the index on
  `(ttl_epoch_utc_seconds, hash_prefix_bucket, hk, sk)`. The entries are the same, because a `WITHOUT ROWID` index
  does not store a primary-key column twice.
- `idx_items_scan` stays, with the hashPrefixBucket before `hk` (Section 4.3.1). It keeps the covering reads of the
  size checks, the count queries, and the range split planning.
- `idx_items_item_id` stays, to keep the collision check of the migration (Section 4.3.3). It also supports a lookup
  from a linked row to its item. In a `WITHOUT ROWID` table each index entry stores the full primary key, so the index
  costs one key copy for each item and one more row written for each insert. A link from an item to its linked rows
  needs no index.

**Every partition stores the full hashPrefixBucket.** Each row of each partition, hash or range, stores the full
hashPrefixBucket of its hash key. A range partition stores the hashPrefixBucket of its range tree, which is the value
that the rows had in the hash partition. Thus a later promotion by clone keeps the rows of the range root unchanged,
and `storagePrefix` has no special case for a partition kind.

##### The item-id counter

`items.item_id` stays as the link id of future tables tied to a stored item, for example a side table for large data,
or metadata for each item. A counter that reserves blocks assigns it:

1. `PartitionStore` keeps `next` and `limit` in memory. Both start at 0.
2. A write that creates a row takes `next` and adds 1. `upsertItem` knows that the row is absent when
   `#storedEstRowBytes` is 0, so an overwrite takes no id.
3. When `next == limit`, the store reserves a block:
   `UPDATE item_id_counter SET hwm = hwm + 1024 WHERE id = 1 RETURNING hwm`. The block is `[hwm - 1024, hwm)`.
4. Memory moves to the new block only after the transaction that wrote the reservation commits. The implementation
   picks one of two ways:
   - reserve in its own small `transactionSync` before the write, outside any other transaction; or
   - return the memory step to the caller, which applies it after its outermost `transactionSync` returns, as
     `FokosShardingRuntime#ensureIdentity` does for the identity.
5. When the transaction of the reservation rolls back, memory keeps the old exhausted block. The next write reserves
   again, from the rolled-back value. No committed row holds an id of the lost block, so I7 holds.
6. The migration ingest (`insertItemIfAbsent`) writes the copied `item_id`. Each page raises the mark in the same
   transaction: `UPDATE item_id_counter SET hwm = MAX(hwm, ?1) WHERE id = 1`, with the largest copied id plus 1.
   After the commit, memory drops its block (`next = limit = 0`), so the next write reserves above every copied id.

Rules:

- The ids are local to a partition. Gaps are allowed: a crash after a reservation loses the rest of the block.
- The mark only increases, so a deleted id never comes back. Today SQLite can give the highest deleted `item_id` to
  the next row (`docs/agent-plans/2026-09-13-items-link-item_id.md`, Section 4.2.5).
- A clone of the storage carries the `item_id_counter` row, so a clone needs no rule.
- The counter costs one more row written for each 1,024 created rows.
- The `ItemLinkId` brand and its rules stay (`partition-store.ts`).

**The duplicate check of `insertItemIfAbsent`.** `idx_items_item_id` keeps the check that the `INTEGER PRIMARY KEY`
gives today: a copied item whose id another item holds still fails. The lineage rule of
`docs/agent-plans/2026-09-13-items-link-item_id.md` (Section 4.2.3) still applies. A child copies from one parent, the
parent ids are unique, and the child raises its mark above every copied id. The constraint checks that rule at ingest.
Keep `ON CONFLICT (hash_prefix_bucket, hk, sk) DO NOTHING`, so a page retry skips the same item and does not hide an
item-id collision.

##### The other tables of a partition

```sql
CREATE TABLE IF NOT EXISTS pending_transactions (
    hash_prefix_bucket    BLOB    NOT NULL,
    hk                    BLOB    NOT NULL,
    sk                    BLOB    NOT NULL DEFAULT x'',
    transaction_id        TEXT    NOT NULL,
    operation             TEXT    NOT NULL,
    op_list               TEXT    NOT NULL,
    data_kind             INTEGER,
    ttl_epoch_utc_seconds INTEGER,
    data                  ANY,

    PRIMARY KEY (hash_prefix_bucket, hk, sk, transaction_id)
) STRICT;

CREATE INDEX IF NOT EXISTS pending_transactions_transaction_id
    ON pending_transactions (transaction_id, hash_prefix_bucket, hk, sk);

CREATE TABLE IF NOT EXISTS materialization_outbox (
    outbox_id          INTEGER PRIMARY KEY,
    hash_prefix_bucket BLOB    NOT NULL,
    hk                 BLOB    NOT NULL,
    sk                 BLOB    NOT NULL DEFAULT x'',
    revision           INTEGER NOT NULL,
    is_deleted         INTEGER NOT NULL,
    is_local_only      INTEGER NOT NULL DEFAULT 0,
    next_attempt_at    INTEGER NOT NULL DEFAULT 0,
    UNIQUE (hash_prefix_bucket, hk, sk)
) STRICT;

CREATE TABLE IF NOT EXISTS key_size_estimates (
    hash_prefix_bucket BLOB    NOT NULL,
    hk                 BLOB    NOT NULL,
    est_bytes          INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (hash_prefix_bucket, hk)
) WITHOUT ROWID, STRICT;

CREATE INDEX IF NOT EXISTS key_size_estimates_by_bytes ON key_size_estimates (est_bytes);
```

- `pending_transactions` stays a rowid table, because its rows hold data (the reason is in its migration comment).
  Its `PRIMARY KEY` index adds the hashPrefixBucket before `hk`. `pending_transactions_transaction_id` still leads with
  `transaction_id`, and whole-transaction lookups still use it. `pending_tx_info` does not change: it has no item key.
- A lock row of a put of a new item has no item row, and a delete in the outbox has no item row. Thus these tables
  keep the keys, and `item_id` cannot link them.
- `materialization_outbox` keeps `outbox_id` and its rule "use
  `INSERT ... ON CONFLICT (hash_prefix_bucket, hk, sk) DO UPDATE`".
- In M3, `key_size_estimates` keeps one row for each hash key, under the key `(hash_prefix_bucket, hk)`. Promotion
  discovery works as today, and does not scan or sum the keys of a hashPrefixBucket. M4 replaces these rows with one
  total for each hashPrefixBucket (Section 4.2.7). `KeySizeStats` (`shared/partition/key-size-stats.ts`) takes the
  opaque prefix as an argument of each method. `MaterializationStore` uses the same class, with the prefix from its
  own runtime.
- `latest_items` gets `hash_prefix_bucket BLOB NOT NULL`, and its unique item key becomes
  `(hash_prefix_bucket, base_hk, base_sk)`. Its point lookups and migration reads add the hashPrefixBucket. Its
  hashPrefixBucket uses the seed of the materialization shard group, not the seed of the base partition group. It
  stays a rowid table until M7. Its size expression includes the new column. M7 measures the final layout.
- No change: `deletion_metadata`, `materialization_export_state`, `ancestor_fences`, `producer_fences`.

An index that leads with a transaction id or a time keeps that leading column. The hashPrefixBucket goes into its
item-key part.

##### The statements that change

The general rule: each predicate, conflict target, cursor, and `ORDER BY` that names an item key adds
`hash_prefix_bucket` before `hk` and `sk`. Every other field, bound value, and tie-breaker stays. The main
statements:

- The point statements name the whole primary key: `WHERE hash_prefix_bucket = ? AND hk = ? AND sk = ?`. They are
  `getItem`, `getItemImage`, `#storedEstRowBytes`, `measureItemBytesWithStamp`, `bumpItemReadTs`, `readItemData`,
  `deleteItem`, and `updateItemSingleShot`. `#storedEstRowBytes` keeps `INDEXED BY idx_items_scan`.
- `upsertItem` inserts `hash_prefix_bucket` and `item_id`, with `ON CONFLICT (hash_prefix_bucket, hk, sk)`.
- `rangeScanConditions` and `queryScanStatement` start with `hash_prefix_bucket = ? AND hk = ?`, then the sort-key
  bounds. The count selection without an expression plan keeps its covering read of `idx_items_scan`.
- `itemsPageStatement` and `outboxPageStatement` use the cursor `(hash_prefix_bucket, hk, sk) > (?, ?, ?)`, and the
  same tuple in `ORDER BY`.
- `pendingTxPageStatement` uses the cursor `(hash_prefix_bucket, hk, sk, transaction_id) > (?, ?, ?, ?)`, and all
  four fields in `ORDER BY`. A page boundary between two locks of one item must keep both locks. A statement with a
  fixed hashPrefixBucket can drop it from the cursor, but it must keep every other cursor field.
- In M3, `probeHashKeys` still finds distinct hash keys through the prefix-leading key index. In M4 it becomes
  `probeHashPrefixBuckets`: the first hashPrefixBucket, one representative hash key, and whether another
  hashPrefixBucket follows (`hash_prefix_bucket > ?`). The host uses the representative to request a promotion
  through its hash-key API.
- `sortKeySizes` reads `(hk, sk, est_row_bytes)` of one hashPrefixBucket in `[start, end)` from `idx_items_scan`.
- `deleteItemsBatchForHashKey` becomes `deleteItemsBatchForHashPrefixBucket` in M4:

  ```sql
  DELETE FROM items WHERE (hash_prefix_bucket, hk, sk) IN
      (SELECT hash_prefix_bucket, hk, sk FROM items WHERE hash_prefix_bucket = ?1 ORDER BY hk, sk LIMIT ?2)
  ```

- `deletePendingTxCopiesBatch` and `deleteOutboxCopiesBatch` take a hashPrefixBucket in place of a hash key in M4.
- `pendingLockFor`, `insertPendingLock`, `deletePendingTxKeys`, `listPendingTxKeys`, and `listPendingTxItems` name the
  hashPrefixBucket where they select or address an item key. A lock key carries its hashPrefixBucket. A
  whole-transaction lookup still returns the locks of all hashPrefixBuckets of the transaction.
- `#upsertOutboxRow`, `markOutboxRowExported`, `deferOutboxRow`, `deleteOutboxRow`, `discardOutboxRow`, and
  `exportItemImage` name the hashPrefixBucket.
- `shared/expression/plan.ts` adds the hashPrefixBucket to its item lookup joins and predicates, with the bound values.
  Expression trees, literal encoding, key references, type checks, and evaluation order stay. Query filters still
  evaluate candidates. They do not change the candidate selection or the page budgets.

The hash-key walks and `sliceKeyRange` go when no caller is left ("The migration read" below). The promotion checks
and deletes by hash key stay until M4 replaces them.

The partition gets the hashPrefixBucket of a key from `this.fokos.storagePrefix(hk)`, and passes it to each store
method as a parameter. The store does not call the runtime. The cost is one xxHash64 of the hash key for each key and
each partition hop (Section 4.2.12).

##### The TTL sweep

`deleteExpiredItems` no longer deletes by `item_id`:

```sql
SELECT i.ttl_epoch_utc_seconds AS ttl, i.hash_prefix_bucket, i.hk, i.sk
  FROM items i INDEXED BY idx_items_ttl
 WHERE i.ttl_epoch_utc_seconds IS NOT NULL
   AND i.ttl_epoch_utc_seconds <= ?1
   AND (i.ttl_epoch_utc_seconds, i.hash_prefix_bucket, i.hk, i.sk) > (?3, ?4, ?5, ?6)   -- only with a resume position
   AND NOT EXISTS (SELECT 1 FROM pending_transactions p
                    WHERE p.hash_prefix_bucket = i.hash_prefix_bucket AND p.hk = i.hk AND p.sk = i.sk)
 ORDER BY i.ttl_epoch_utc_seconds, i.hash_prefix_bucket, i.hk, i.sk
 LIMIT ?2;

DELETE FROM items WHERE hash_prefix_bucket = ?1 AND hk = ?2 AND sk = ?3
 RETURNING est_row_bytes, ttl_epoch_utc_seconds, v, last_read_ts;
```

- The scan rule, the scan bound `TTL_SCAN_ROWS_PER_DELETED_ROW`, the ownership filter, and the deletion bookkeeping
  stay. `TtlScanPosition` gets the hashPrefixBucket.
- The sweep deletes each owned candidate with one primary-key statement, in the same `transactionSync`. A multi-row
  `VALUES` list holds at most 33 rows because of the 100 bound parameters, and `json_each` cannot carry a BLOB.
- The TTL sweep is not a repartition cleanup, so its deletes need not be contiguous.

##### The migration read

A hash child owns one hashPrefixBucket interval. Before the host builds a page, the runtime computes the interval of
the target. It passes the interval to `MigrationHost.buildPage` with the slice, the ownership predicate, and the page
budget. The interval is the scope of the target after the cutover, not the current ownership of the source.

- `FokosMigrationHost`, `MaterializationMigrationHost`, and the coordinator use the interval that the runtime gives.
  They must not compute bounds from a hash-child slice alone, because the slice has no root and no ancestor path. For
  example, child 0 at depth 1 has different intervals under root 0 and under root 1. `sliceKeyRange` goes.
- `KeyRange` (`partition-store.ts`) becomes an alias of `FokosStorageInterval`. `collectStream` reads that interval in
  storage order.
- `belongsToTarget` still excludes terminal promotions. In M3 a terminal promotion excludes only its hash key, and the
  read must not skip another hash key with the same hashPrefixBucket. From M4, a terminal promotion excludes the whole
  hashPrefixBucket, and the read skips it with one seek (`hash_prefix_bucket > ?`).
- The child writes its imported rows in storage order. The page builders of items, locks, and the outbox keep their
  budgets.
- Each stream adds the hashPrefixBucket to the front of its full row cursor. The SQL order, the resume predicate, the
  cursor comparison, and the page validation use that same full tuple. A resumed page starts at its validated cursor,
  not at the lower bound of the interval, and keeps the upper bound. Keep the row-value comparisons and the indexed
  seeks. Do not replace them with `OR` predicates or repeated scans.
- The stream order, the metadata transfer, the fences, the idempotent ingest, and the cursor commit rules stay.

##### The cleanups

| Cleanup | Statement | Contiguous |
| --- | --- | --- |
| Promotion: items | `deleteItemsBatchForHashPrefixBucket` | yes |
| Promotion: `key_size_estimates` | `DELETE FROM key_size_estimates WHERE hash_prefix_bucket = ?` | yes |
| Promotion and split: lock copies | `deletePendingTxCopiesBatch` | no: batches of whole transactions |
| Promotion and split: outbox copies | `deleteOutboxCopiesBatch` | no: bounded rowid batches |
| Coordinator split source | Section 4.2.9 | yes |

The table gives the final predicates, from M4. In M3 the promotion cleanups keep their predicates by hash key. The
lock and outbox exceptions keep the cleanup safety and batch rules of today.

**Invariants.** I2, I6, I7, I9, and the hash part of I5.

**New tests.**

- The item-id counter: an overwrite keeps `item_id`. A delete and a put of the same key give a new, higher id. No id
  repeats after the delete of the row with the highest id. A `transactionSync` that throws after a reservation,
  followed by a committed write, gives no id twice. An ingest page raises the mark above its copied ids.
- With a reserved block, an insert writes 3 rows without a TTL and 4 with a TTL. Measure the counter reservations
  apart from this.
- `EXPLAIN QUERY PLAN`: the TTL sweep seeks on its cursor. Point reads and cleanup deletes use the primary key. Size
  lookups, unfiltered count queries, and range split planning keep the covering reads of `idx_items_scan`.
- The runtime: `storageInterval()` stays after the cutover, and `ownedStorageInterval()` is null on a router. An
  unbounded range end stops at the next prefix, except for the last prefix.
- The migration interval: with `R = 2` and `K = 2`, child 0 at depth 1 gets a different interval under root 0 and
  under root 1, although the slice fields are the same. Both children import all their rows. Repeat below a hash
  parent that is not a root.
- Two hash keys with one hashPrefixBucket, and one of them promoted: a hash child still imports the other key, and the
  promotion cleanup keeps it. This test applies to M3 only. M4 replaces it.
- A storage bench, `test/storage-bench/prune-contiguity.workerd-bench.ts`, outside `pnpm test`: 8 MiB partitions,
  keys in random order, 100 B to 2 KB items. It deletes one half, then three quarters, of the hashPrefixBucket space
  with contiguous deletes. It reports the `databaseSize` that stays against the parent. The measurements of Appendix A
  gave 50% and 25%. The bench fails above a tolerance (`TODO: choose`). Run it with
  `pnpm bench:workerd test/storage-bench/prune-contiguity.workerd-bench.ts`.

#### 4.2.7 M4 — Promotions by hashPrefixBucket

##### A promotion moves one hashPrefixBucket

- `FokosSlice` (`repartition-slice.ts`) and `RepartitionSlice` (`sharding-store.ts`): `promoted_key { hashKey }`
  becomes `promoted_hash_prefix_bucket { hashPrefixBucket }`, and `range { hashKey, start, end }` becomes
  `range { hashPrefixBucket, start: FokosCompositeKey | null, end: FokosCompositeKey | null }`.
- The promotion copy reads every row of the hashPrefixBucket, all its hash keys, in `(hk, sk)` order. A hash key that
  shares all 64 bits with the promoted key moves with it (I8).
- The host keeps its hash-key API: `promotionCandidates` and `requestPromotion` take a hash key, and the runtime maps
  it to its hashPrefixBucket. Two candidates with one hashPrefixBucket make one promotion.
- `SplitPlanner#decide` probes hashPrefixBuckets:
  - no hashPrefixBucket: the floor `no_hash_key`;
  - one hashPrefixBucket: promote it, whatever its estimate;
  - two or more: promote the hashPrefixBuckets at or above the threshold first (up to `PROMOTION_CANDIDATES_MAX`,
    largest first), then split the leaf.

  A hash split cannot separate two hash keys of one hashPrefixBucket, so they count as one.
- `largestAtLeast` reads the indexed totals of the hashPrefixBuckets (below). It must not use the largest single hash
  key as the estimate of a hashPrefixBucket. Promotion discovery runs before the depth-limit check.
- `beforeComplete` and `cleanupSourceStep` in `do-partition.ts` remove the estimate row and the items of the
  hashPrefixBucket.

##### One indexed estimate for each hashPrefixBucket

M4 changes the M3 estimate table in place:

```sql
CREATE TABLE IF NOT EXISTS key_size_estimates (
    hash_prefix_bucket BLOB    NOT NULL PRIMARY KEY,
    hk                 BLOB    NOT NULL,
    est_bytes          INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID, STRICT;

CREATE INDEX IF NOT EXISTS key_size_estimates_by_bytes ON key_size_estimates (est_bytes);
```

- One row holds the total estimate of all items of that hashPrefixBucket in this partition. On a range leaf, the total
  covers only the composite interval of that leaf. The host gives `KeySizeStats` an opaque storage prefix. The class
  does not calculate hashes or read prefix bytes.
- `hk` is a representative hash key of the hashPrefixBucket, not part of the primary key. A new row stores the hash
  key of its first item, and later writes can keep it. The representative can outlive its own items, because its hash
  still selects the same hashPrefixBucket. It exists only for the hash-key promotion API of the host.
- Each item write applies its size delta to this one row, in the storage transaction of the item. This covers
  inserts, overwrites, updates, deletes, TTL expiry, and the temporary writes of a transaction evaluation. A rollback
  restores the row with the item changes. A row with no item bytes goes.
- A migration page groups the deltas of its inserted rows by prefix, and updates each row once. An item that is
  already present adds nothing. The page writes the item rows, the estimate rows, and the cursor in one transaction.
  A promotion removes the source row when the whole hashPrefixBucket moves.
- `bytesOf(prefix)` is one primary-key lookup. `largestAtLeast` seeks `key_size_estimates_by_bytes` and reads at most
  `PROMOTION_CANDIDATES_MAX` (5) rows in descending order, with at most 5 primary-key lookups. It returns their
  representative hash keys. The decision must not scan all hash keys, run a whole-table `GROUP BY`, or sum member rows
  on each call.
- The promotion signal of the write path uses the total after the delta, not the size of the changed hash key alone.
  It passes that total and the changed hash key to `promotionCandidates`. The runtime maps the hash key to its
  hashPrefixBucket and removes duplicate requests.
- `MaterializationStore` uses the same rules, with prefixes from its own runtime. The item-stats RFC can replace this
  table later, but it must keep an indexed promotion discovery by hashPrefixBucket.

With one hash key for each hashPrefixBucket, the total and the write frequency are the same as today. With colliding
keys, their combined total can start a promotion with no extra scan.

##### Composite keys

```ts
/** A position inside one hashPrefixBucket, in the order (hk, sk). */
export type FokosCompositeKey = { hk: KeyBytes; sk: KeyBytes };
```

`KeyCodec` gets `compareComposite(a, b)` (`compare(a.hk, b.hk)`, then `compare(a.sk, b.sk)`) and
`shortestCompositeSeparator(prev, cur)`:

- When `prev.hk` and `cur.hk` differ, the separator is `(s, x'')`, with `s` the shortest key such that
  `prev.hk < s <= cur.hk`. `x''` is the smallest sort key, so `(s, x'')` is above `prev` and at or below `cur`.
- When they are equal, the separator is `(hk, shortestSeparator(prev.sk, cur.sk))`.

A query of one hash key `H` with the sort-key interval `[s, e)` is the composite interval `[(H, s), (H, e))`. With no
upper sort-key bound, the upper bound is `(H || 0x00, x'')`. No hash key is between `H` and `H || 0x00`, so every row
of `H` is below this bound, and every row of a larger hash key is at or above it.

The code that compares composite keys:

- `sharding/sk-interval.ts`: `rangeIntersects`, `clipToChildRange`, `isChildFullyBeforeCursor`, `cursorFallsInChild`,
  and `makeBoundaryCursor` compare composite boundaries with the composite interval of one hash key.
- `sharding/range-frontier.ts`: `startCmp`, `endCmp`, `isStrictSubSlice`, and `planRangeFrontier`.
- `sharding/runtime.ts`: `#ownsByTopology` (the range branch), `#rangeChildFor`, `#rangeOwner`, `intervalInside`,
  `#assertCanOwnRange`, and `#planRange`.
- `sharding/repartition-flow.ts`: `#plan` (the `range_split` boundaries and the `key_promotion` target),
  `belongsToTarget`, and `#buildOverridesPage`.
- `shared/partition/split-planner.ts`: `planRangeBoundaries` takes `(hk, sk, bytes)` rows and makes composite
  separators.
- `shared/partition/fokos-migration-host.ts`: uses the target storage interval from the runtime.
- `server/do-partition.ts`: `clipQueryToVisit` and `walkRangeVisits`. A visit is a composite interval. The clipped
  request keeps the sort-key interval of its one hash key.

The sort-key range rules of today stay inside one hash key. `rangeSplitN` can still change.

##### Range partition IDs and DO names

- The range ID schema `SCHEMA_RANGE_V1` (`0x01`) changes its layout in place (Section 4.2.2). It holds the flags, the
  hashPrefixBucket (8 bytes), and the two composite boundaries, each as `hkLen u32 LE`, `skLen u32 LE`, and the bytes.
  The hashPrefixBucket comes first, so `PartitionIdHelper.rangeHashPrefixBucket(partitionId)` reads no boundary.
- The DO name of a range partition is `<shardGroup>~r.<hashPrefixBucket hex>.<start>.<end>`:
  - `<hashPrefixBucket hex>` is the 16 lowercase hex characters of the hashPrefixBucket. A range root is
    `<shardGroup>~r.<hashPrefixBucket hex>.~min.~max`, a fixed length for each shard group.
  - `<start>` is `~min` or `<hk component>.<sk component>`. `<end>` is `~max` or the same two components.
  - Each component uses `encodeRangeComponent`, which never contains `.`. An empty sort key gives the empty component.
    A component never starts with `~m`. Thus a reader knows the count of components of each boundary, and the name is
    injective.
  - A DO name has no length limit, because `getByName` hashes the name
    (`docs/agent-plans/2026-09-27-range-do-name-text-encoding.md`). A child name holds the hash key once for each
    bounded edge: two times at most.
- `resolveRangePartitionContext(base, hashPrefixBucket, start, end)` and `partitionIdentityFrom` change with it.
  `FokosPartitionIdentity.range` becomes `{ hashPrefixBucket, start, end, depth, ancestors }`.

**Follow-up, not in this RFC: prefix compression of the end boundary.** A range child with two bounded edges
holds two composite boundaries. Normally both boundaries have the same hash key, and they often share the start of
the sort key too, so the name holds the same bytes two times. A later change can encode the end boundary as the
length of its common prefix with the start boundary, followed by the end bytes after that prefix. The same rule can
apply to the range ID schema. The encoding must stay injective. I10 fixes the DO name encoding at the release, so
this change must ship before the release, or it needs its own migration plan.

The same compression can apply to two tables:

- `fokos_range_hierarchy`. The end boundary is not in a key (see "The sharding store" below), so it can be
  `end_shared_len INTEGER` and `end_suffix BLOB`. No seek reads the end: each reader gets whole rows, so JavaScript
  can rebuild the end. A row then holds the hash key about one time.
- `fokos_topo_range_intervals` in the keeper. Its primary key needs the end today, for the build order "start
  ascending, then end descending". When the row stores the depth, `(start, depth)` gives the same order, and the end
  can leave the key. TODO: check that the keeper has the depth when it writes the row.

##### The sharding store

```sql
CREATE TABLE IF NOT EXISTS fokos_repartitions (
    ...                                    -- the columns of today, except:
    hash_prefix_bucket BLOB                -- the promoted hashPrefixBucket of a key_promotion; NULL for a split
) STRICT;

CREATE TABLE IF NOT EXISTS fokos_repartition_targets (
    ...                                    -- the columns of today, except:
    slice_hash_prefix_bucket BLOB,         -- promoted_hash_prefix_bucket and range slices
    slice_start_hk BLOB, slice_start_sk BLOB,   -- NULL: the unbounded start
    slice_end_hk   BLOB, slice_end_sk   BLOB,   -- NULL: the unbounded end
    ...
) STRICT;

CREATE TABLE IF NOT EXISTS fokos_route_overrides (
    hash_prefix_bucket BLOB NOT NULL PRIMARY KEY,
    repartition_id     TEXT NOT NULL
) WITHOUT ROWID, STRICT;

CREATE TABLE IF NOT EXISTS fokos_range_hierarchy (
    hash_prefix_bucket BLOB    NOT NULL,
    depth              INTEGER NOT NULL,
    start_hk           BLOB    NOT NULL DEFAULT x'',
    start_sk           BLOB    NOT NULL DEFAULT x'',
    end_hk             BLOB    NOT NULL DEFAULT x'',
    end_sk             BLOB    NOT NULL DEFAULT x'',
    learned_at         INTEGER NOT NULL,
    PRIMARY KEY (hash_prefix_bucket, depth, start_hk, start_sk)
) WITHOUT ROWID, STRICT;
```

- A route override has an 8-byte key. `routeOverrideFor`, `insertRouteOverride`, `hasTerminalRouteOverride`, and
  `queryTerminalRouteOverridesPage` take a hashPrefixBucket. `PromotedKeyCursor` becomes `{ hashPrefixBucket }`.
- `fokos_range_hierarchy` keeps the rule of today: an unbounded edge is `x''`, which `KeyCodec` never makes for a real
  hash key. `findDeepestKnownRangeSlice(hashPrefixBucket, key: FokosCompositeKey)` decodes it to null.
- The primary key of `fokos_range_hierarchy` is `(hash_prefix_bucket, depth, start)`, and the end boundary is not in a
  key. The nodes of one depth do not overlap, so two nodes of one depth never have the same start. A range node never
  changes its boundaries. The table has no secondary index. Thus each row stores the hash key at most two times (the
  bounded start and the bounded end), as today. Without the change it stores the hash key four times: two columns,
  each in the table and in `idx_fokos_range_hierarchy_depth`.
- Each statement uses the primary key:
  - `findDeepestKnownRangeSlice` reads the deepest depth of the hashPrefixBucket. For each depth, it seeks the last row
    with `(start_hk, start_sk) <= (?, ?)`, and tests the end on that one row. An unbounded end (`x''`) passes the
    test.
  - `listLearnedRangeSlices` reads the rows of one hashPrefixBucket in `(depth, start)` order.
  - `learnRangeBoundary` uses `ON CONFLICT (hash_prefix_bucket, depth, start_hk, start_sk)`.
  - `deleteLearnedRangeSlice` takes the depth of the learned slice, and deletes by the primary key. Its callers in
    `runtime.ts` already have `learned.depth`.
  - The eviction deletes by the primary key, in the order `learned_at`, then `depth DESC`, as today.
- The start is the key, not the end. Both give one seek for each depth, because the nodes of one depth do not overlap.
  Only the start sorts correctly with the `x''` sentinel: as an end, `x''` sorts first but means "no upper bound", and
  needs an extra column such as `end_is_max`.
- `__fokos/cache/promotion_bloom` (`PartialRangeTopology`) holds the promoted hashPrefixBuckets that the partition
  learned. `#learn`, `#bloomForLearning`, and the speculation guard of `#groupByOwner` work by hashPrefixBucket. The
  rules of `docs/agent-plans/2026-09-27-learned-routes-for-every-dispatch-shape.md` stay, with "hash key H" read as
  "hashPrefixBucket B".
- Only the range boundaries, the range slices, and the range topology encodings add full hash-key bytes to the
  topology. They exist only for promoted hashPrefixBuckets.

##### The topology event and the keeper

- `FokosTopologyEvent.targets[].slice` carries the hashPrefixBucket and the composite boundaries. The `repartition_key`
  of a `range_split` is the hashPrefixBucket hex.
- `fokos_topo_range_dirty` and `fokos_topo_bodies` use the hashPrefixBucket in place of `hash_key`.
- `fokos_topo_range_intervals` becomes
  `(shard_group, hash_prefix_bucket, start_hk, start_sk, end_is_max, end_hk, end_sk, seq)`, with the primary key
  `(shard_group, hash_prefix_bucket, start_hk, start_sk, end_is_max DESC, end_hk DESC, end_sk DESC)`. `~min` is
  `(x'', x'')`. The build order stays: start ascending, then end descending, so a parent comes before its children.
- The range topology encoding (`FKR1`) changes in place. A boundary is two byte strings, so `boundaryOffsets` gets
  `2n + 1` values. Boundary `j` is the hash key `offsets[2j] .. offsets[2j + 1]` and the sort key
  `offsets[2j + 1] .. offsets[2j + 2]`. `countBoundaries` compares in place with `KeyCodec.compareSlice` on both
  parts. The levels, the depths, the interval rule, the size cap, and `truncatedAtRank` stay.
- `getRangeTopology(shardGroup, hashPrefixBucket)` and the path
  `/internal/v1/topology.GetForRange?sg=<b64>&bk=<16 hex>`. The URL has a fixed length for each shard group, so the
  `TODO: measure` of the URL length in the encoded topology RFC goes.
- `cachedRangeTopology` uses the key `r|<scope>|<shardGroup>|<hashPrefixBucket hex>` for a range entry.

##### The materialization store

`MaterializationStore` is a host of the same runtime, so its promotions also move hashPrefixBuckets. Its promotion copy
reads all hash keys of a hashPrefixBucket. It keeps the prefix-leading unique item key of M3, with the prefix from its
own shard group. The table stays a rowid table until M7 (Section 4.2.10).

**Invariants.** I5 (the range part), I8, I10.

**New tests.**

- `compareComposite` is a total order. `shortestCompositeSeparator` is strictly above `prev` and at or below `cur` for
  random pairs, also for `H` and `H || 0x00`.
- The range DO name is injective for random hashPrefixBuckets and composite boundaries, empty sort keys included.
- The range routing functions (`sk-interval.ts`, `range-frontier.ts`) and `planRangeBoundaries` with a
  hashPrefixBucket that holds two hash keys, and boundaries between the two keys.
- The estimate totals: colliding hash keys update one total. A rollback restores it. A retried migration page adds
  nothing twice. A deleted representative still routes a promotion. Discovery reads at most
  `PROMOTION_CANDIDATES_MAX` rows and scans no member keys.
- A leaf at its depth limit holds two hashPrefixBuckets. Each holds some hash keys below the threshold, but each total
  is above it. The decision promotes both and does not return `max_depth`. The write-path signal uses the same totals.
- The range topology encoding lookups agree with a reference walk for random composite trees.
- End to end: two hash keys with one hashPrefixBucket promote together. A range split between them puts each key in
  its own child. A query of each key returns only its rows. In the materialization store, a promotion moves every hash
  key of its hashPrefixBucket.

#### 4.2.8 M5 — The promoted set in the hash topology encoding

**The section.** The hash topology encoding (`FKT1`) holds one of two sections for the promoted hashPrefixBuckets:

| Count of promoted hashPrefixBuckets | Section | Answer |
| --- | --- | --- |
| at most `N` | `promoted`: `{ offset, count, width: 8 }`, a sorted array of 8-byte hashPrefixBuckets | exact |
| more than `N` | `bloom`: the Bloom filter of today, built from the hashPrefixBuckets | a positive can be false |

- The reader makes a byte view over the sorted array and searches it in place with a binary search of 8-byte compares
  (two big-endian `u32` compares). It allocates nothing.
- `N` is 4,096, a constant of the keeper. A list of 4,096 hashPrefixBuckets is 32 KiB, and a search takes 12 compares.
  A reader reads both sections, so a change of `N` changes only how the keeper encodes its next hash topology
  encoding. It needs no migration.
- A negative answer is final in both structures, because a Bloom filter has no false negatives. Only a stale hash
  topology encoding can give a wrong negative, and the hash leaf then forwards with its route override, as today.
- The build reads the range roots of `fokos_topo_range_intervals`. For the sorted section, a merge of the stored array
  with the new roots (`seq > built_version`) gives the new array in one pass. When the count passes `N`, the build
  reads all roots and makes the Bloom filter. `promotedKeysBloom` keeps its rules above `N`.

The size of each structure, for `n` promoted hashPrefixBuckets and a false positive rate `ε`:

| Structure | Bits for each hashPrefixBucket | At `n = 4,096` | Exact |
| --- | --- | --- | --- |
| Sorted 8-byte list | 64 | 32 KiB | yes |
| Bloom filter | `1.44 * log2(1/ε)`: 9.6 at 1%, 11.0 at 0.5% | 4.8 KiB at 1% | no |
| Elias-Fano or delta-coded sorted fingerprints | about `log2(1/ε) + 2`: 8.6 at 1% | 4.3 KiB at 1% | no |

Today the smallest promoted-key Bloom filter of a hash topology encoding is 25,540 bytes (16,384 capacity at 0.005).
Thus a sorted list of 4,096 hashPrefixBuckets costs 7 KiB more than the hash topology encoding of today.

**The same work for each positive.** On a positive answer from either structure, the caller starts the background
fetch of the range item of the hashPrefixBucket (`cachedRangeTopology`). A negative fetches nothing. Thus the latency
of a caller does not change when the hash topology encoding of its table changes structure at `N`. The resolution of
the encoded topology RFC (Section 4.2.9, "Resolution") stays:

1. A range entry exists and is not empty: the point lookup or the range lookup.
2. No range entry: the caller starts the refresh and sends to the range root `~r.<hashPrefixBucket hex>.~min.~max`. A
   Bloom false positive reaches an object that no promotion initialized. It answers
   `range_partition_not_initialized`, and the caller sends again with the hash path and no promoted-set step
   (`#missLadder`).
3. An empty range entry: the positive was false, and the caller uses the hash path.

The worst case of a caller is two cache reads: the hash item of the table (the bitmap and the promoted set), and,
only on a positive, the range item of that hashPrefixBucket.

**Code that changes.** `sharding/topology-codec.ts` (`parseHashBody`, `HashTopologyView`, and `maybePromoted`, which
becomes `isPromotedHashPrefixBucket`), `sharding/topology-encode.ts` (`encodeHashBody`, a sorted-section encoder and
merge), `sharding/topology-keeper.ts` (`#buildHashBody`), `sharding/topology-cache.ts`, `sharding/client.ts`, and
`sharding/runtime.ts` (where the runtime reads the hash topology encoding).

**New tests.**

- The sorted search is exact for random sets of 0 to `N` hashPrefixBuckets. At `N + 1` the hash topology encoding has
  a Bloom section. A reader refuses a `promoted` section whose `count * width` is outside the hash topology encoding,
  or whose values do not strictly increase.
- The keeper merge gives the same array as a full build, for reports in any order.
- A positive from the sorted section starts one range fetch, and a negative starts none.
- End to end: a promoted hashPrefixBucket with the count below `N` and above `N`.

Also run `test/topology-bench/hash-build.workerd-bench.ts` for the build cost with a sorted section.

#### 4.2.9 M6 — The coordinator storage

The route key of a coordinator is `tokenKey(token)`, the encoded idempotency token. Its hashPrefixBucket uses the seed
of the coordinator group `fokos.tc.<tableName>.<fp(ns)>` (M1), and its routing uses the digits from M2. M6 changes
its storage:

```sql
CREATE TABLE IF NOT EXISTS tc_state (
    hash_prefix_bucket BLOB NOT NULL,
    transaction_id     TEXT NOT NULL,
    idempotency_token  TEXT NOT NULL,
    ...                                   -- the other columns of today
    PRIMARY KEY (hash_prefix_bucket, transaction_id)
) WITHOUT ROWID, STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS tc_state_idempotency_token ON tc_state (hash_prefix_bucket, idempotency_token);
CREATE INDEX IF NOT EXISTS idx_tc_state_completed_at ON tc_state (completed_at) WHERE completed_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tc_state_recovery ON tc_state (next_recovery_at) WHERE completed_at IS NULL;

-- tc_participants: PRIMARY KEY (hash_prefix_bucket, transaction_id, partition_do_name)
-- tc_items:        PRIMARY KEY (hash_prefix_bucket, transaction_id, op_index)
-- tc_results:      PRIMARY KEY (hash_prefix_bucket, transaction_id, op_index)
```

- Each `tc_*` row leads with the hashPrefixBucket of its token. The hashPrefixBucket is a function of the token, so
  `UNIQUE (hash_prefix_bucket, idempotency_token)` gives the same rule as `UNIQUE (idempotency_token)`, in storage
  order.
- Each lookup, update, and delete by `transaction_id` adds the hashPrefixBucket to the transaction key. Participant
  names, operation indexes, ledger-state predicates, and the other fields stay. `initiateWrite` gets the token from
  the request. `recoverTransaction` gets it from the `CoordinatorRef`.
- The recovery and expiry scans keep their time order, and select the hashPrefixBucket with each transaction id. Each
  selected row uses its own hashPrefixBucket for later lookups and deletes. The hashPrefixBucket of one row must not
  limit the other due transactions.
- `buildMigrationPage` reads the interval of the target: `WHERE (hash_prefix_bucket, transaction_id) > (?, ?)` inside
  the interval, in place of a scan of the whole ledger with a filter. The cursor and `ORDER BY` keep `transaction_id`
  after the hashPrefixBucket. `validatePage` keeps its slice check. Each transaction still moves with its participant,
  item, and result rows, inside the page budgets of today.
- `deleteMigratedRowsStep` deletes in primary-key order, so each delete is contiguous.
- The split floor of a coordinator is "fewer than two hashPrefixBuckets, or the depth limit", in place of "fewer than
  two tokens".
- `idx_tc_state_completed_at` and `idx_tc_state_recovery` lead with a time, so they are exceptions to I6
  (Section 4.2.1).

**Invariants.** I5 and I6 for the coordinator.

**New tests.**

- The recovery and expiry scans select due rows from different hashPrefixBuckets, and use the full transaction key of
  each row.
- A coordinator split, with the control of `test/controlled-transaction-coordinator-do.ts`: the child gets exactly the
  transactions of its interval, and the page read reads no row outside it.

Also run `pnpm formal:p`. The coordinator protocol does not change, so every test case keeps its result.

#### 4.2.10 M7 — The materialization storage

M7 changes the storage of `MaterializationStore`. `latest_items` uses the same clustered layout as `items`, with its
base-key column names:

```sql
CREATE TABLE IF NOT EXISTS latest_items (
    hash_prefix_bucket BLOB    NOT NULL,
    base_hk            BLOB    NOT NULL,
    base_sk            BLOB    NOT NULL DEFAULT x'',
    item_id            INTEGER NOT NULL,
    revision           INTEGER NOT NULL,
    deleted_at_ms      INTEGER,
    newest_seq         INTEGER,
    data_kind          INTEGER,
    ttl_at             INTEGER,
    est_row_bytes      INTEGER NOT NULL DEFAULT 0,
    data               ANY,

    PRIMARY KEY (hash_prefix_bucket, base_hk, base_sk),
    UNIQUE (item_id)
) WITHOUT ROWID, STRICT;

CREATE INDEX IF NOT EXISTS idx_latest_items_deleted_gc
    ON latest_items (deleted_at_ms, item_id) WHERE deleted_at_ms IS NOT NULL;
```

##### The hashPrefixBucket and the item id

- The host gets the hashPrefixBucket from its own `fokos.storagePrefix(base_hk)`. It uses the seed of
  `fokos.m.<tableName>.<fp(ns)>`. The hashPrefixBucket of the base partition must not pass through unchanged. The
  store takes the value as a parameter and does not calculate a hash.
- `base_hk` and `base_sk` stay the canonical keys of the base item. The hashPrefixBucket is a separate value. It is not
  an item identity and not part of the canonical key encoding.
- The materialization store keeps its own `item_id_counter`, with the reservation and rollback rules of
  Section 4.2.6. A new item takes a local id, and an overwrite keeps it. This id is not the item id of the base
  partition. The migration copies the id unchanged and raises the local mark in the same page transaction. After the
  commit, memory drops any reserved block below the new mark.
- `UNIQUE (item_id)` is necessary, because operations find an item through its linked rows. It also keeps the
  collision check of `insertMigratedItem`. Its conflict target becomes `(hash_prefix_bucket, base_hk, base_sk)` in
  place of `INSERT OR IGNORE`. A retry of the same item skips it, and an id collision with another item fails the
  statement (Section 4.3.2).

##### The operations and linked rows

The links stay:

- `change_positions.item_id` joins to `latest_items.item_id` for each target read.
- `target_retry (target_id, item_id)` joins to `latest_items.item_id` for retries.
- `target_index_keys` keeps `idx_target_index_keys_item (item_id, target_id)`. Tombstone collection and the cleanup
  of migrated items keep their item-id lookups.

These operations seek through `UNIQUE (item_id)` in place of the rowid, with their guards and transactions of today.
The linked tables do not repeat the hashPrefixBucket or the base keys.

- Point lookups and upserts use the hashPrefixBucket and both base keys. The upsert keeps the rules of the live image,
  the tombstone, the revision, and the target position.
- The migration keeps `(hash_prefix_bucket, base_hk, base_sk)` order and uses the target interval from the runtime.
  The item, its positions, retries, and GSI keys move together, with no id remap. Page retries stay idempotent, and
  the page cursor and all imported rows commit in one transaction.
- The promotion cleanup selects a bounded batch in primary-key order for the promoted hashPrefixBucket. It deletes the
  positions, retries, GSI keys, and row of each item in one transaction. Ownership checks and fences stay.
- Tombstone collection keeps the cursor `(deleted_at_ms, item_id)`, its order, and its eligibility checks. The index
  declares `item_id`, so the seek stays after the primary-key change.
- `latestRowBytesExpr` and `LATEST_ROW_BYTES_K` must count the clustered row and the item-id index. Measure the new
  constants. Each stored estimate, estimate total, and row change stays in one transaction. If the item-stats RFC ships
  first, keep its fields and its covering index, with the hashPrefixBucket added.

`change_positions` stays a log in `seq` order. Target cursors, cursor snapshots, retries, and producer epochs keep
their rules. Only the primary B-tree of `latest_items` gets the contiguous cleanup. Its item-id index, its
tombstone-time index, and the linked tables are exceptions to I6. A prune does not free a proportional part of the
whole materialization database.

**Invariants.** I5, I6 for `latest_items`, I7, and I9.

**New tests.**

- The item-id counter of `latest_items`: an overwrite keeps its id. A new item after a delete gets a higher id. A
  rollback gives no id twice. An ingest raises the mark above the copied ids before local writes start again.
- `EXPLAIN QUERY PLAN`: point reads seek the primary key. Target reads, retries, and tombstone deletes seek through
  `UNIQUE (item_id)`, not a full-table scan. Tombstone pages seek from their `(deleted_at_ms, item_id)` cursor.
- A promotion cleanup removes only the rows of its hashPrefixBucket and their linked rows.

#### 4.2.11 M8 — Topology admin objects for each promoted hashPrefixBucket

**The problem.** Workers Cache coalesces the requests of one cache key, and each promoted hashPrefixBucket has its own
range cache key. Thus the range reads at one admin object add up over the hashPrefixBuckets: about
`hot hashPrefixBuckets * upper tiers / maxAgeS`. With 10,000 hot hashPrefixBuckets, 10 upper tiers (an assumption),
and `maxAgeS` 10, that is about 10,000 requests each second. The soft limit of one Durable Object is 1,000 requests
each second ([Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)). The
encoded topology RFC lists this as its follow-up 6.

**The design.**

- Each promoted hashPrefixBucket gets one topology admin object, with the name
  `hashPrefixBucketTopologyAdminName(shardGroup, fp, hashPrefixBucket)`:

  ```ts
  topologyAdminName(shardGroup, fp) + ".b." + hex(hashPrefixBucket)
  ```

  The fingerprint alphabet has no `.`, so the name is injective.
- A `key_promotion` registers with two admin objects. The object of the shard group adds the hashPrefixBucket to the
  promoted set of the hash topology encoding. The object of the hashPrefixBucket stores its range root.
- Each later `range_split` in the hashPrefixBucket registers only with the object of the hashPrefixBucket.
- The keeper code is the same for both objects. It serves the hash and range topologies from the repartitions that
  registered with it. The object of a hashPrefixBucket holds one range tree and no hash split.
- The runtime keeps one outbox row for each event. The host hook `adminOutboxFor` (`shared/topology-hooks.ts`) sends a
  `key_promotion` to both objects, and a `range_split` to the object of its hashPrefixBucket. Each keeper is idempotent
  by the event id, so a retry that sends to both again is safe.
- A caller reads a range topology encoding through the entrypoint, with the props of the object of the
  hashPrefixBucket. Workers Cache keys an entry by `ctx.props`, so each hashPrefixBucket keeps its own entry.
  `topologyCacheFor` sets `scope` to that address.
- `FokosTopologyCache` (`server/topology-entrypoint.ts`) accepts the admin name of a hashPrefixBucket, and checks it as
  it checks the admin name today.

**Destroy.** `destroy` (`client/db.ts`) resets each hashPrefixBucket admin during the traversal, before it deletes the
source that records the promotion. It resets the shard-group admin after the partition traversal completes. The
traversal must keep each durable record of a discovery until the work that it identifies succeeds.
`FokosShardingClient.destroy` keeps its fence-first, child-before-parent order, and exposes an awaited step before
each deletion. `FokosDB.destroy` uses that step for the admin resets. The generic traversal does not calculate FokosDB
admin addresses. The order:

1. Fence the source with `fokosPrepareDestroy`. Then read its split targets and all promotion pages. The fence stops
   new targets and new repartition reports from this source.
2. Fence and delete each target subtree, children before parents. This includes range targets that have not completed
   their import.
3. After a promoted range subtree is gone, await the reset of its hashPrefixBucket admin. The source must still hold
   the promotion reference. Each source that can report this hashPrefixBucket must already be fenced.
4. Call `fokosDestroy` on the source only after every child deletion and every admin reset succeeds. The traversal
   must not remove a promotion reference before its admin reset succeeds.
5. After the partition traversal of the shard group completes, reset the shard-group admin.

When a child deletion or an admin reset fails, the traversal stops before it deletes the source. A retry starts at the
roots, and finds the remaining work through the parent and promotion references that stay. The reset runs before the
deletion of the source, not in the `onDestroyed` notification of today, which is not awaited.

A reset can succeed and lose its response. A retry then resets the same admin again, and the epoch can advance again.
The keeper must still refuse each report from before the reset. The traversal must also handle more than one
reference to the same promoted range root.

**Cost.** One more Durable Object for each promoted hashPrefixBucket, with the storage of one range tree topology. One
more RPC for each promotion.

**New tests.**

- The admin name of a hashPrefixBucket is injective, and a short fingerprint is refused.
- A keeper that holds only range repartitions serves the range topology encoding and an empty hash topology encoding.
- A promotion reaches both objects. A range split reaches only the object of the hashPrefixBucket. A caller reads the
  range topology encoding from that object. `destroy` resets both.
- `destroy` keeps the order: fence the source, delete the range subtree, reset the hashPrefixBucket admin, delete the
  source, reset the shard-group admin.
- A reset that fails after the deletion of the range subtree leaves the source and its promotion reference. A new
  `destroy` finds the reference and completes the reset before it deletes the source.
- A new `destroy` completes after a lost reset response, after a partial child deletion, and with more than one
  reference to one range root. No source makes a new report after the reset of its hashPrefixBucket admin.

#### 4.2.12 Performance and cost

**Key bytes for each item**, for a 41-byte hash key and a 32-byte sort key (approximate):

| Layout | Key bytes for each item | Rows written for each insert |
| --- | ---: | ---: |
| Today: the row, `sqlite_autoindex_items_1`, `idx_items_scan` | 220 | 3 |
| Clustered table only (reference) | 81 | 1 |
| Clustered table with `UNIQUE (item_id)` only (reference) | 166 | 2 |
| Clustered table with `idx_items_scan` only (reference) | about 166 | 2 |
| Clustered table with `idx_items_scan` and `UNIQUE (item_id)` (this RFC) | 250 | 3 |

A row with a TTL adds one `idx_items_ttl` entry (about 90 bytes for these keys), today and after the change. The
counter adds one write for each block of 1,024 ids (Section 4.2.6).

**Disk for each logical byte** (Appendix A): 1.34 to 1.41 below about 900 B of data, against 1.52 to 2.23 today. At
1 KB to 2 KB of data it is 2.3 to 4.4, against 1.42 to 1.45 today. The cost above about 1 KB is the price of the
contiguous order: each row above the inline payload limit gets private overflow pages. Today `EST_ROW_BYTES_K`
(`item-size.ts`) counts the rowid and two index entries. Measure its value again for the new layout
(`TODO: measure`). Repeat the space measurements with `idx_items_scan` in the layout.

**Deletes.** In the clustered layout, deletes ran 2.5 to 6 times faster than today. A delete counts 1 row written in
both layouts. In the storage bench, a contiguous prune that kept `1/K` of the rows left `1/K` of the size. The
exceptions of Section 4.2.1 do not get this ratio.

**Reads.** Point reads stayed at about 9 µs to 15 µs. Each operation adds one xxHash64 of the hash key for each
partition hop, and one digit step of two multiplications for each level (`TODO: measure` the cost of `h64Raw` on a
41-byte key in workerd).

**Count queries and range split planning** keep the covering scan index with the hashPrefixBucket added. An
unfiltered count query reads `sk` and `est_row_bytes` without item data, as today. A filter that reads item data still
reads the item row. The latency of the prefix-leading index: `TODO: measure`.

**The exceptions of I6** (Section 4.2.1). The size of each exception index: `idx_items_ttl` has one entry for each
item with a TTL, and it can cover most items of a table where most items have a TTL. `idx_items_item_id` has one entry
for each item. `key_size_estimates_by_bytes` has one entry for each hashPrefixBucket after M4.
`pending_transactions_transaction_id` and `pending_tx_info_due` have one entry for each lock and each open transaction.
`idx_materialization_outbox_due` has one entry for each unsent change. A cleanup can leave the pages of these
B-trees part full. The RFC accepts this cost to keep the cleanup steps safe and bounded.

**The materialization store.** `UNIQUE (item_id)` adds one index entry for each item. The entry stores the full
primary key, and adds one index write for each insert. An item-id read uses that index and then the clustered table,
in place of a rowid lookup. Measure `LATEST_ROW_BYTES_K` and the latency of the linked reads for this layout
(`TODO: measure`).

**The hash topology encoding.** The promoted set costs 64 bits for each hashPrefixBucket up to `N`, and the Bloom
bits above `N`.

#### 4.2.13 Crafted hash keys

The default base seed is public, so the seed of each shard group is public and deterministic. xxHash64 is not a
cryptographic hash. For short binary keys, a caller can invert its operations and construct inputs for a chosen full
output. The design must not assume a brute-force cost of 2^64 trials, and must not treat the seed as protection
against chosen keys. A platform can give a secret base seed (Section 4.2.4). xxHash64 is not a keyed hash, so the
design does not treat a secret base seed as protection either.

The RFC accepts this availability risk:

- A caller can target the hashPrefixBucket or the node interval of an existing key. The constructed keys can force
  extra splits, deeper routes, and more storage and copy work. An honest key in the targeted interval can share the
  cost or the write refusal.
- Full collisions keep distinct item identities through `hk` and `sk`. Their combined estimate can start a promotion,
  and the range tree then splits by composite keys.
- The hash depth limit (Section 4.2.5) bounds the hash path. It does not make chosen-output keys harder to construct.
- At the depth limit, the partition still evaluates promotions first. A leaf with one hashPrefixBucket promotes it at
  any estimate. When a final interval holds two or more hashPrefixBuckets and none can promote, the leaf returns
  `max_depth`. It refuses writes above 1.1 times its cap. Reads, deletes, and transaction resolution keep their rules.
- Range splits after the hash depth limit are out of scope (Section 2.2). Until they ship, the split floor and the
  write refusal stay.

The package is not an authorization or abuse-control boundary for an application. An application that lets untrusted
callers choose keys must accept or control this cost.

### 4.3 Decisions

#### 4.3.1 Keep `idx_items_scan` until the item-stats RFC

Keep `idx_items_scan` as `(hash_prefix_bucket, hk, sk, est_row_bytes)`, with the covering reads of today. Compared
with the table alone, it costs one more key copy and one more index write for each insert (Section 4.2.12).
`docs/agent-plans/2026-10-31-item-size-facts-and-range-split.md` changes the covering columns and the item-size
model. That RFC owns the redesign of the index, and it must keep the prefix-leading order of this RFC.

#### 4.3.2 Cluster `latest_items`, and keep its `item_id` links

Cluster `latest_items` by `(hash_prefix_bucket, base_hk, base_sk)` in its own milestone, M7 (Section 4.2.10). The
hashPrefixBucket uses the seed of the materialization shard group. Keep `UNIQUE (item_id)` and the item-id links. The
linked tables keep their layouts. The contiguous cleanup covers only the primary B-tree of `latest_items`.

#### 4.3.3 Keep the item-id duplicate check of `insertItemIfAbsent`

Keep `idx_items_item_id` as `UNIQUE (item_id)`. The id stays unique across all hashPrefixBuckets of one partition, as
today. The constraint must not become `UNIQUE (hash_prefix_bucket, item_id)`. `insertItemIfAbsent` keeps its conflict
target on the full application key: a page retry skips the same item, and a different item with a used id fails. The
counter and the lineage rules prevent normal collisions, and the constraint keeps the failure behavior of today.

The index costs one key copy and one index write compared with a clustered table without it (Section 4.2.12). This
RFC accepts the cost. A later RFC can examine it again. A switch to a migration by clone does not remove it.

### 4.4 Follow-ups

These changes do less work on the route path. They do not change a result, and no milestone needs them.

**Measured cost.** Node, `xxhash-wasm`, keys of about 16 bytes:

| Step | Time |
| --- | ---: |
| One `h32Raw` and a modulo (one level before M2) | 34 ns |
| `h64Raw`, which returns a `bigint` | 81 ns |
| `hashPrefixBucket` (the hash and one 8-byte array) | 90 ns |
| `HashPrefixBucketDigitCursor`, the root and 4 levels | 13 ns to 16 ns |

The digit walk costs little. The hash is the cost, so each follow-up removes a repeated hash or an array:

1. **`FokosShardingRuntime#resolve`** hashes a key in `#ownsByTopology` and again in `#hashTopologyOwner` on a router.
   Compute the hashPrefixBucket once and give it to both.
2. **`FokosShardingRuntime#learn`** hashes each key again for each node of the envelope. Compute the hashPrefixBucket
   of each key once for the call.
3. **`FokosShardingRuntime#hashDescendant`** makes the digit array from the root, then removes the levels of the
   partition with `slice`. Step the cursor past the depth of the partition, and collect only the levels below it.
4. **`hashKeyInSlice`** (`repartition-slice.ts`) makes a digit array for each row that a migration reads. Step the
   cursor to the digit at `slice.depth - 1`, with no array.
5. **`FokosShardingClient#resolveKey`** hashes a key in `FokosRouter.rootContext` and again for `findLeaf`. Removing
   the second hash needs a router method that takes a hashPrefixBucket, which adds to the public API of the sharding
   client.

## 5. Alternative options

- **Drop `idx_items_scan` with the storage change.** This saves one index write and one key copy for each insert. The
  metadata scans then read the wide clustered rows in place of the narrow covering index. Rejected: the item-stats RFC
  owns the redesign of the index (Section 4.3.1).
- **Drop `idx_items_item_id` and trust the lineage rules alone.** This saves one key copy and one index write. It
  removes the item-id collision check at ingest. Rejected: the check keeps the failure behavior of today.
- **Keep the rowid `items` table and add the hashPrefixBucket to its unique index.** Only the index becomes
  contiguous. The table pages stay in insert order. A key-range split of a rowid table with keys in random order keeps
  69% to 71% of its size (Appendix A).
- **Keep `latest_items` in rowid order.** This keeps direct item-id seeks, but its rows stay in insert order. M7
  accepts the item-id index to cluster the rows.
- **Keep the per-depth xxHash32 seeds, and store by hashPrefixBucket.** A hash split is then scattered in
  hashPrefixBucket order. The storage order then helps only promotions and range splits.
- **A random 64-bit `item_id`.** Collision detection needs a unique index, and the collision probability is about
  `n^2 / 2^65`. A random id also puts rows at random positions of any B-tree that it orders
  (`docs/agent-plans/2026-09-13-items-link-item_id.md`, Section 5.2).
- **`item_id` from `MAX(item_id) + 1`.** No rowid gives it, a `MAX` read on each insert costs a seek, and a deleted id
  can come back.
- **The hashPrefixBucket as an `INTEGER` with the sign bit flipped.** DO SQL cannot bind a `bigint`, and a JavaScript
  `number` loses precision above 2^53. A BLOB of fixed width has the unsigned order with no trick.
- **Promotions of a hash key, with storage by hashPrefixBucket.** A range tree then owns one hash key inside one
  hashPrefixBucket, which is still one interval. But the names, the overrides, and the topology keep the
  variable-length hash key, which this RFC removes from the topology.
- **The seed and the width in `FokosTopology`, validated on each request.** Each RPC, the topology event, the keeper,
  and the hash topology encoding then carry them. `validateTopology` always passes for a topology that the client
  derives, and each derived shard group needs a helper in place of a spread. The stored seed in the partition
  identity gives the same guard at run time with none of these costs (Section 4.2.4).
- **A Bloom filter for every count of promoted hashPrefixBuckets** (today). It gives false positives also for a small
  set, where an exact list costs little.
- **Elias-Fano or delta-coded sorted fingerprints above `N`.** About `log2(1/ε) + 2` bits for each hashPrefixBucket,
  against `1.44 * log2(1/ε)` for a Bloom filter: 8.6 against 9.6 bits at 1%. The reader decodes a block for each
  lookup, and the code base already has a Bloom filter (`shared/bloom-filter.ts`). It can replace the Bloom section
  later, as a new section key.
- **One admin object for all range topology encodings** (today). The reads of many hashPrefixBuckets add up at one
  object (Section 4.2.11).
- **The hashPrefixBucket as a hex prefix of `transaction_id` in the coordinator.** The `tc_*` tables then need no new
  column, because `transaction_id` order is hashPrefixBucket order. But the host then knows the width and the encoding
  of the prefix, which the runtime API forbids, and the prefix is TEXT in place of a BLOB.
- **A format version in the topology, with one code path for each format.** No table holds data that must survive the
  change before the public release. Each extra path adds a switch to the hashPrefixBucket and digit functions, the
  partition IDs, every `PartitionStore` statement, the sharding store, and the keeper. Every test then runs once for
  each path.
- **The empty BLOB `x''` as the storage prefix of a range partition.** Every row of a range tree has the same
  hashPrefixBucket, so `x''` saves 8 bytes for each row and each index entry that leads with it: about 4% of a row with
  100 B of data. But a promotion by clone must then change the first primary-key column of each row of the range root,
  and in a `WITHOUT ROWID` table each change is one delete and one insert. It also adds a special case to
  `storagePrefix`. The saving does not pay for either cost.
- **A list from `ownedPrefixRanges()` in place of a storage interval.** A range partition also needs composite
  boundaries, and one storage-interval type covers both partition kinds. The exact ownership checks exclude moved
  prefixes without a list of every remaining interval.
- **`VACUUM` or `auto_vacuum` after a delete.** `VACUUM` fails ("cannot VACUUM from within a transaction"), and both
  pragmas fail with `SQLITE_AUTH`.

## 6. Frequently asked questions

**Why is the sort key not part of the hashPrefixBucket?**
All items of one hash key must stay in one partition and in one key range. Then a query of a hash key reads one
interval, and a promotion moves one interval.

**What happens when two hash keys have one hashPrefixBucket?**
They always go to the same hash partition, and a promotion moves both. Inside a range tree they are two hash-key
intervals. Each read and write names `hk` and `sk`, so their items stay apart (I9). With 2^64 values, the chance of a
collision among `n` hash keys is about `n^2 / 2^65`.

**Why does the topology not carry the seed?**
No routing step needs the seed in a request: the code derives it from the shard group name. A seed in the topology
adds bytes to each RPC, a field to the topology event, the keeper, and the topology encodings, and a check that always
passes, because the client derives the seed with the same function that checks it. It also makes a spread of a
derived shard group wrong.

**Then why does a partition store the seed?**
The stored rows and the routes of a partition use the seed of the day the partition was written. If a later code
change derives another seed, the partition refuses all work, and no write goes to the wrong place. The pinned test
catches the same change in CI. The stored seed is the guard at run time, and it costs nothing on a request.

**Why does the last interval of a level have no upper bound?**
Its upper bound is 2^64, which has no 8-byte form. Its statements use only `hash_prefix_bucket >= start`.

**Does `pending_transactions` become contiguous?**
Its primary-key index is in prefix order. Its table pages stay in rowid order, because its rows hold data. The
cleanup of a whole transaction can delete entries that are apart in the prefix index. The RFC accepts this exception
for temporary lock rows (Section 4.2.1).

**Why keep `item_id` if nothing links to it?**
A future table tied to a stored item needs a short link key. A key in a linked row repeats up to 1,536 key bytes. The
counter costs one write for each 1,024 created items.

**Can an `item_id` come back after a delete?**
No. The high-water mark only increases. Today SQLite can reuse the highest deleted id.

**Can `rangeSplitN` still change?**
Yes. It applies to the range splits that start after the change, as today.

**Does the hashPrefixBucket make a request slower?**
Each hop hashes the hash key once with xxHash64, and each level adds one digit step of two multiplications. The
measured point read stayed at 9 µs to 15 µs. The cost of the hash: `TODO: measure`.

**Why does a promotion still copy, when a clone is cheaper?**
A promotion moves one hashPrefixBucket, often a small part of its partition. A clone gives the range root a full image
and then deletes every other hashPrefixBucket. The split by storage snapshot decides it.

**Why `WITHOUT ROWID`, when it costs more disk for rows above 1 KB?**
In a rowid table the pages follow the insert order, and no delete of a key range frees whole pages. Below about 900 B
of data, the new layout uses less disk than today.

## 7. References

References:

- `docs/agent-plans/2026-10-23-encoded-topology-auto-admin.md`
- `docs/agent-plans/2026-09-13-items-link-item_id.md`
- `docs/agent-plans/2026-09-27-learned-routes-for-every-dispatch-shape.md`
- `docs/agent-plans/2026-09-27-range-do-name-text-encoding.md`
- `docs/agent-plans/2026-10-31-item-size-facts-and-range-split.md`
- `docs/agent-plans/2026-10-31-namespace-fingerprints-in-do-names.md`
- `docs/ideas/2026-10-19-exploration-sqlite-space-reclamation.md`
- `packages/fokosdb/src/sharding/hash-primitives.ts`, `route-context.ts`, `partition-id.ts`, `router.ts`,
  `runtime.ts`, `repartition-flow.ts`, `repartition-slice.ts`, `sharding-store.ts`, `hash-topology.ts`,
  `topology-codec.ts`, `topology-encode.ts`, `topology-keeper.ts`, `topology-cache.ts`, `key-codec.ts`
- `packages/fokosdb/src/shared/partition/partition-store.ts`, `key-size-stats.ts`, `fokos-migration-host.ts`,
  `split-planner.ts`, `item-size.ts`
- `packages/fokosdb/src/server/do-partition.ts`, `do-transaction-coordinator.ts`,
  `materialization/materialization-store.ts`
- `packages/fokosdb/src/shared/partition-context.ts`, `topology-hooks.ts`
- [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Workers limits](https://developers.cloudflare.com/workers/platform/limits/)
- [SQLite-backed Durable Object storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [SQLite: Clustered indexes and the WITHOUT ROWID optimization](https://www.sqlite.org/withoutrowid.html)
- [SQLite: Database file format](https://www.sqlite.org/fileformat2.html)
- [FokosDB encoded topology](https://www.lambrospetrou.com/articles/fokosdb/#fokosdb-encoded-topology)

## Appendix A. Measured facts

Local workerd, 8 MiB partitions, keys in random order.

**Space after a delete of a hash slice:**

| Layout and delete | 100 B to 200 B | 500 B | 1 KB to 1.5 KB | 2 KB and more |
| --- | ---: | ---: | ---: | ---: |
| Today, a hash child keeps 1/2 | 90% to 94% | 73% | 62% to 72% | 50% to 56% |
| Today, a hash child keeps 1/4 | 34% to 44% | | | |
| `(hash_prefix_bucket, hk, sk)`, contiguous delete, keeps 1/2 | 50% | 50% | 50% | 50% |
| `(hash_prefix_bucket, hk, sk)`, contiguous delete, keeps 1/4 | 25% | 25% | 25% | 25% |
| A fresh copy of 1/2 (the import of today) | about 50% | | | |
| Today, a key-range split | 69% to 71% | | | |

The ratio is the `databaseSize` of the child against the `databaseSize` of the parent.

**Disk for each logical byte:** `WITHOUT ROWID` uses 2.3 to 4.4 at 1 KB to 2 KB of data, against 1.42 to 1.45 today.
Below about 900 B it uses 1.34 to 1.41, against 1.52 to 2.23 today.

**Vacuum:** `VACUUM` fails ("cannot VACUUM from within a transaction"). `PRAGMA auto_vacuum` and
`PRAGMA incremental_vacuum` fail with `SQLITE_AUTH`.

**Rows written:** an insert into the `items` table of today counts 3 (the table and two indexes), and a delete
counts 1. An insert into a `WITHOUT ROWID` table with no secondary index counts 1. A delete is billed as a normal
write of the rows that it counts.

**Speed:** deletes in the clustered layout ran 2.5 to 6 times faster than today. Point reads stayed at about 9 µs to
15 µs.

## Appendix B. A worked example of the digits

`R = 3`, `K = 2`, and the hashPrefixBucket `x = 0xC000000000000000`, which is the fraction 0.75.

1. Root: `floor(0.75 * 3) = 2`. The rest is `0.75 * 3 - 2 = 0.25`.
2. Level 1: `floor(0.25 * 2) = 0`. The rest is `0.5`.
3. Level 2: `floor(0.5 * 2) = 1`. The rest is `0`.

The path is root 2, then `0`, then `1`. The node interval of that path:

- `numer = (2 * 2 + 0) * 2 + 1 = 9`, `D = 3 * 2^2 = 12`.
- `start = ceil(9 * 2^64 / 12)`, the fraction 0.75. `end = ceil(10 * 2^64 / 12)`, the fraction 0.8333.
- `x` is at the fraction 0.75, so it is in `[start, end)`, which agrees with the walk.

The sibling path `2, 0, 0` has `numer = 8`: the interval `[0.6667, 0.75)`. Its `end` is the `start` of `2, 0, 1`, so
the siblings tile their parent `2, 0`, which is `[0.6667, 0.8333)`.

With limbs, step 1 is: `hi = 0xC0000000`, `lo = 0`, `M = 3`. `a = 0`. `b = 0xC0000000 * 3 = 0x240000000`.
`digit = floor(b / 2^32) = 2`. `hi' = b mod 2^32 = 0x40000000`, `lo' = 0`. The rest is `0x4000000000000000`, the
fraction 0.25, as in step 1.
