# RFC — Asynchronous R2 replication of partitions with a copy-on-write B+tree

**State:** Draft
**Date:** 2026-11-27
**Author:** Lambros Petrou

**Status:** Nothing is built. "Today" means the code on `main` at the date of this document.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
  - [1.1 Glossary](#11-glossary)
  - [1.2 The current system](#12-the-current-system)
  - [1.3 Platform facts](#13-platform-facts)
  - [1.4 Problems](#14-problems)
- [2. Goals and requirements](#2-goals-and-requirements)
  - [2.1 In scope](#21-in-scope)
  - [2.2 Out of scope](#22-out-of-scope)
  - [2.3 Requirements](#23-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
    - [4.2.1 Units of data: item, block, pack, vlog](#421-units-of-data-item-block-pack-vlog)
    - [4.2.2 R2 objects](#422-r2-objects)
    - [4.2.3 DO-local state](#423-do-local-state)
    - [4.2.4 Write path](#424-write-path)
    - [4.2.5 Fast flush](#425-fast-flush)
    - [4.2.6 Slow flush](#426-slow-flush)
    - [4.2.7 Read path](#427-read-path)
    - [4.2.8 Caching](#428-caching)
    - [4.2.9 Splits, promotions and stale topology](#429-splits-promotions-and-stale-topology)
    - [4.2.10 Topology-free lookup](#4210-topology-free-lookup)
    - [4.2.11 Clones](#4211-clones)
    - [4.2.12 Snapshots and restored DOs](#4212-snapshots-and-restored-dos)
    - [4.2.13 Garbage collection](#4213-garbage-collection)
    - [4.2.14 Consistency](#4214-consistency)
    - [4.2.15 Invariants](#4215-invariants)
    - [4.2.16 Failure and recovery](#4216-failure-and-recovery)
    - [4.2.17 Performance and cost](#4217-performance-and-cost)
    - [4.2.18 Bottlenecks](#4218-bottlenecks)
    - [4.2.19 Knobs: freshness against cost](#4219-knobs-freshness-against-cost)
    - [4.2.20 Deployment and rollback](#4220-deployment-and-rollback)
    - [4.2.21 Testing](#4221-testing)
  - [4.3 Open questions](#43-open-questions)
  - [4.4 Future extensions](#44-future-extensions)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)
- [Appendix A. File formats](#appendix-a-file-formats)
- [Appendix B. Worked examples](#appendix-b-worked-examples)

---

## 1. Overview and context

A FokosDB table is many partition DOs. Each partition DO holds its items in local SQLite. Only that DO can serve a
read today. A reader far from the DO pays one cross-region round trip for each read. A hot key or a hot range puts all
of its read load on one DO.

This RFC adds an asynchronous R2 copy of each partition. A foreground write does no R2 operation. Each partition DO
uploads its changes in batches. Readers then read from R2 and from Workers Cache, without the partition DO.

The R2 copy is a copy-on-write (CoW) B+tree with key-value separation. Reads stay at a fixed number of hops. The design
needs no background compaction to keep reads efficient. The partition DO builds every R2 object from its local SQLite,
so it makes no R2 reads to write.

### 1.1 Glossary

- **DO** — a Durable Object.
- **Partition** — one `FokosDBPartitionDO` of a shard group. It is the only writer of its items.
- **Position** — the part of a DO name after `~`, for example `h.0.1` or `r.<routingHash hex>.<start>.<end>`.
- **Tree key** — the byte key that orders items in the tree: `routing_hash | hk | sk`, with an order-preserving
  encoding ([Appendix A.1](#a1-tree-key-encoding)). It has the same order as the primary key of `items`.
- **Item entry** — one item in a block: the tree key, the revision, and the value or a vlog pointer.
- **Leaf block** — a sorted run of item entries for one contiguous range of tree keys. It is the smallest unit that a
  reader reads and that the DO rewrites.
- **Router block** — a sorted run of separator keys and child pointers. A router block points to leaf blocks or to
  other router blocks.
- **Root** — the top router block. The manifest holds it inline.
- **Block pointer** — `(pack, offset, length)`. It names one block inside one pack.
- **Pack** — one immutable R2 object that holds many blocks. One slow flush writes one pack.
- **Vlog** — one immutable R2 object that holds item values larger than the inline limit.
- **Manifest** — one mutable R2 object for each partition. It holds the state, the root and the tail.
- **Tail** — a sorted run of the newest item entries, inside the manifest. The tail takes priority over the tree.
- **Fast flush** — an upload that rewrites the manifest with a new tail.
- **Slow flush** — an upload that moves the tail into the tree: one new pack and one new manifest.
- **Seq** — `r2_seq`, a counter of each partition that increases on each change of an item.
- **R2 reader** — a stateless Worker that reads R2 objects through Workers Cache.
- **Holder** — a partition, a clone partition or a snapshot that references a pack or a vlog.
- **Grace period** — the time between the moment a pack becomes unreferenced and its delete.

### 1.2 The current system

- `items` has the primary key `(routing_hash, hk, sk)` (`partition-store.ts`, migration 1). Each row has the revision
  `v`, `data_kind`, `ttl_epoch_utc_seconds`, `item_bytes` and `data`.
- `materialization_outbox` holds one row for each changed item that the export has not sent. A newer change of the
  item updates the same row. The export reads the data from `items`. This RFC copies that pattern.
- A partition splits into children. A promotion moves one routingHash into a range tree. Nothing merges partitions.
- `routingHashOf` (`src/sharding/hash-primitives.ts`) computes the routingHash from the hash key and the seed scope of
  the shard group ([the seed-scope plan](../agent-plans/2026-11-17-routing-hash-seed-scope.md)).
- A client caches the encoded topology through the `FokosDBTopologyCache` entrypoint. That entrypoint has Workers Cache
  enabled (`examples/http-api/wrangler.jsonc`).

### 1.3 Platform facts

| Fact | Value | Source |
| --- | --- | --- |
| Largest string, BLOB or row in DO SQLite | `MAX_ROW_LENGTH = 8 * 1024 * 1024 + 34` | workerd `src/workerd/util/sqlite.h` |
| Largest FokosDB item | `MAX_ITEM_BYTES = 1 MiB` | `src/shared/transaction-limits.ts` |
| Memory of one isolate | 128 MB | [Workers limits](https://developers.cloudflare.com/workers/platform/limits/) |
| R2 Class A (PUT, LIST) | $4.50 for each million | [R2 pricing](https://developers.cloudflare.com/r2/pricing/) |
| R2 Class B (GET, HEAD) | $0.36 for each million | [R2 pricing](https://developers.cloudflare.com/r2/pricing/) |
| R2 DELETE | free | [R2 pricing](https://developers.cloudflare.com/r2/pricing/) |
| R2 storage | $0.015 for each GB-month | [R2 pricing](https://developers.cloudflare.com/r2/pricing/) |
| R2 writes to one key | about 1 each second. TODO: confirm the current value. | [R2 limits](https://developers.cloudflare.com/r2/platform/limits/) |
| R2 conditional PUT | `onlyIf: { etagMatches }` | [R2 Workers API](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/) |
| Workers Cache tiers | A lower tier near the reader. An upper tier that each lower tier asks on a miss. | [Workers Cache](https://developers.cloudflare.com/workers/cache/) |
| Workers Cache collapsing | One Worker run for each cache key and data center when many misses arrive together. | [Workers Cache](https://developers.cloudflare.com/workers/cache/) |
| Workers Cache ranges | The cache stores the full `200` body and returns a `206` slice. It does not store a `206` from the Worker. | [Workers Cache](https://developers.cloudflare.com/workers/cache/) |
| Workers Cache cost | Each invocation is a billed Worker request, also on a hit. | [Workers Cache](https://developers.cloudflare.com/workers/cache/) |
| Workers Cache object size | The zone cache limit. At launch, the Free plan limit for all plans. | [Workers Cache limitations](https://developers.cloudflare.com/workers/cache/limitations/) |
| Workers Cache key | Includes the Worker version, unless `cross_version_cache` is set. | [Workers Cache keys](https://developers.cloudflare.com/workers/cache/cache-keys/) |
| Workers Cache purge | Free-tier rate limits on all plans. | [Workers Cache purge](https://developers.cloudflare.com/workers/cache/purge/) |
| Worker requests | $0.30 for each million | [Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/) |

### 1.4 Problems

1. A read far from the partition DO costs one cross-region round trip: about 80–200 ms. TODO: measure.
2. One DO serves all reads of a hot key or a hot range.
3. No reader can read a table without the partition DOs and the control plane.
4. A log-structured merge tree (LSM) on R2 needs background compaction to keep reads efficient. Compaction reads
   objects back from R2 and races the flush on the manifest.

## 2. Goals and requirements

### 2.1 In scope

1. A foreground write (`putItem`, `updateItem`, `deleteItem`, a transaction commit) does no R2 operation.
2. An R2 reader reads one item from R2 and Workers Cache with at most 1 cache call on the hot path.
3. A reader with a stale topology reads the last state of a split parent, or it follows the children.
4. A table clone shares all R2 data objects of the source. Both tables accept independent writes after the clone.
5. A reader finds the manifest of a key from `(table, hashKey)` and the routingHash function only.
6. A snapshot references existing R2 data objects. It copies no data objects.
7. The read cost does not increase with the write history of a partition.
8. Garbage collection does no tree walk and no full LIST in the common case.

### 2.2 Out of scope

- **Strong reads from R2.** R2 reads are eventually consistent. A strong read goes to the partition DO.
- **Atomic visibility of a cross-partition transaction in R2.** Only snapshots give it ([4.2.14](#4214-consistency)).
- **R2 copies of GSI partitions.** GSI partitions are `FokosDBPartitionDO` too. They can use the same mechanism
  later. This RFC does not specify it.
- **Queries and scans from R2.** The tree supports them. This RFC specifies `getItem` only. Scans are follow-up work.
- **Partition merges.** The design does not stop them ([4.2.9](#429-splits-promotions-and-stale-topology)).

### 2.3 Requirements

- A flush must not hold the 128 MB isolate memory. A flush must stream each object with a known length.
- A flush must not contain uncommitted transaction state.
- The manifest must change at most once each second, because of the R2 limit for each key.
- The DO must not read R2 to build a flush.
- A pack and a vlog must not change after the upload.
- A deleted pack must not be reachable from any manifest that a reader can still use.

## 3. Milestones

Each milestone ships on its own and is useful on its own.

1. **M1 — Tree and slow flush.** The `r2_outbox`, the local tree tables, the pack and the manifest without a tail. A
   reader library that does `getItem` from R2 directly. Delivers: an R2 copy and R2 reads with a slow freshness.
2. **M2 — Tail and fast flush.** The tail in the manifest, the vlog, the flush policy. Delivers: freshness of about
   1–3 s.
3. **M3 — Cache entrypoint.** `FokosDBR2Cache`, the isolate LRU, `minSeq`. Delivers: the hot-path latency and cost.
4. **M4 — Local garbage collection and compaction.** `_r2_packs`, `_r2_vlogs`, the grace period, the audit.
   Delivers: bounded storage.
5. **M5 — Splits, promotions, topology-free lookup.** Child manifests, `SPLIT`, `promoted`, the routing hint.
   Delivers: correct reads across topology changes.
6. **M6 — Clones and shared garbage collection.** The holder registry. Delivers: zero-copy clones.
7. **M7 — Snapshots.** Snapshot manifests and the link to
   [the backup and restore RFC](2026-11-17-consistent-backup-and-restore.md). Delivers: snapshots without data copies.

## 4. Proposed solution

### 4.1 High-level overview

Each partition DO keeps a CoW B+tree of its items in R2. The DO is the only writer. Its SQLite always holds the full,
current, sorted state. The DO therefore builds each new R2 object from SQLite with a range query. It never reads R2 to
write.

There are three units of data:

```
item     key + value, or key + vlog pointer               about 100 B – 1 KB in a block
  ⊂ block  sorted items of one contiguous key range         16–32 KB  (about 16–300 items)
     ⊂ pack  the dirty blocks of one slow flush               at most 16–32 MB  (about 500–2 000 blocks)
```

The tree decides which block holds a key. The pack decides only where the bytes of a block are. A pack holds the blocks
that were dirty in one slow flush. Those blocks can come from key ranges far apart.

```
            tables/<table>/p/<position>/manifest        (mutable, conditional PUT)
            ┌────────────────────────────────────┐
            │ meta: state, seq, children, …      │
            │ tail: newest item entries          │ ◄── fast flush: 1 PUT
            │ root: router block                 │ ◄── slow flush: 1 pack PUT + 1 manifest PUT
            └───────────────┬────────────────────┘
                            │ (pack, offset, length)
                            ▼
packs/<ulid>.pack  [router blocks][leaf blocks …]   (immutable)
                            │ (pack, offset, length)
                            ▼
packs/<ulid>.pack  [ … leaf block: item entries … ]   (immutable)
                            │ (vlog, offset, length)   only for values above about 1 KB
                            ▼
vlog/<ulid>.vlog   [ … value bytes … ]                (immutable)
```

The DO uploads changes in two ways:

1. **Fast flush.** The DO rewrites the manifest with a new tail: the item entries changed since the last slow flush.
   This costs 1 PUT. It runs at most once each second.
2. **Slow flush.** When the tail is full, the DO rebuilds each changed leaf block and its router path from SQLite. It
   writes the blocks into one new pack, then a new manifest with the new root and an empty tail. This costs 2 PUTs.

A reader gets the manifest, searches the tail, then follows the root to one leaf block. Packs and vlogs are immutable.
Workers Cache keeps them as whole objects for one year and returns byte ranges from them. An isolate LRU keeps the
manifests and the router blocks, so a hot read makes about 1 cache call.

Old blocks become dead when a slow flush replaces them. A pack with no live blocks is deleted after a grace period.
Compaction copies the live blocks of a pack with low use into a later pack. Compaction only frees space. Reads do not
get slower if compaction is late.

A split child, a clone and a snapshot reference existing packs. They copy no data. A registry of holders keeps a shared
pack until its last holder releases it.

### 4.2 Technical details

#### 4.2.1 Units of data: item, block, pack, vlog

| Unit | Contents | Size | Count for 1 GB, 1 KB items, 16 KB blocks |
| --- | --- | --- | --- |
| Item entry | Tree key, `r2_seq`, `v`, `data_kind`, TTL, and the value or a vlog pointer | 40 B – 1 KB | about 1,000,000 |
| Leaf block | Item entries of one key range `[lo, hi)` | 16–32 KB | about 62,000 |
| Router block | Separators, block pointers, `child_max_seq`, `child_items` | 16–32 KB | about 210 L1 blocks and 1 root |
| Pack | Blocks of one slow flush | at most 16–32 MB | Grows with slow flushes. Compaction bounds it. |
| Vlog | Values above the inline limit | at most 8–32 MB | One for each flush that has large values |

Rules:

- A key is in exactly one live leaf block. A leaf block is in exactly one pack.
- Each block has exactly one live copy: the copy in the pack of the last slow flush that changed it.
- A block has a variable size. A router entry holds its length.
- The tree has at most 2 router levels below the root. For 1 GB with 16 KB blocks, a router of about 300 entries gives
  `62 000 / 300 ≈ 210` L1 routers and one root of about 10 KB.
- An item value larger than the inline limit (start value 1 KB) goes into a vlog. The item entry holds the pointer.

**Block size.** The DO chooses the target size of each block at each slow flush:

| Write pattern | Target size | Reason |
| --- | --- | --- |
| Random updates (hash partitions) | 16–32 KB | One changed item rewrites one full block. |
| Sequential or append writes (range partitions with increasing sort keys, bulk loads, rebuilds) | 128–256 KB | All items in the block are new. A large block costs no extra rewrite. |
| Cold blocks that compaction copies | merged to 128–256 KB | Fewer router entries. |

The cost of a large block under random writes, for 100 writes each second and a slow flush each 5 s (about 500 dirty
blocks):

| Block size | Upload for each slow flush | Upload rate |
| --- | --- | --- |
| 16 KB | 8 MB | 1.6 MB/s |
| 32 KB | 16 MB | 3.2 MB/s |
| 128 KB | 64 MB | 13 MB/s |
| 512 KB | 256 MB | 51 MB/s |

#### 4.2.2 R2 objects

| Object | Mutable | Writer | Contents |
| --- | --- | --- | --- |
| `packs/<ulid>.pack` | no | The partition DO | Router blocks first, then leaf blocks, then an index and a footer |
| `vlog/<ulid>.vlog` | no | The partition DO | Value records, an index and a footer |
| `tables/<table>/p/<position>/manifest` | yes, `etagMatches` | The partition DO, or its parent at creation | Meta, root, tail |
| `tables/<table>/routing` | yes, `etagMatches` | Partitions on a split or a promotion | A hint: routingHash prefix → position |
| `snapshots/<snapshotId>/manifest` | no | The snapshot job | Copies of partition manifests at the cut |

- Packs and vlogs hold no table name. Many tables can share them.
- `<table>` is the stable identity of the table. See [Open question 1](#431-the-table-identity-in-r2-keys).
- [Appendix A](#appendix-a-file-formats) gives each format.

When each object is written:

| Event | Packs | Router blocks | Vlog | Manifest | Routing hint |
| --- | --- | --- | --- | --- | --- |
| Foreground write | – | – | – | – | – |
| Fast flush | – | – | 1, only if the tail has new large values | Overwrite: new tail | – |
| Slow flush | 1 | The path from each dirty leaf to the root | – | Overwrite: new root, empty tail | – |
| Compaction (part of a slow flush) | the same pack | The paths of the copied blocks | 1, if a vlog is below 50% use | the same manifest | – |
| First flush, bulk load, rebuild | N, at most the cap each | All levels | As needed | Create | – |
| Split | – | The root of each child | – | Create the children, then the parent → `SPLIT` | Update |
| Promotion | – | The root of the range partition | – | Create the range manifest, then add the key to `promoted` | Update |
| Clone | – | – | – | Create manifests under the new table with the same roots | Copy |
| Snapshot | – | – | – | – (one snapshot manifest) | – |

#### 4.2.3 DO-local state

New tables in the partition SQLite. Appendix A.7 gives the full schema.

- `r2_outbox` — one row for each item changed since the last slow flush. It holds the key, `r2_seq` and `is_deleted`,
  never the data. A newer change updates the same row. The pattern is the same as `materialization_outbox`.
- `r2_export_state` — one row: `next_seq`, `tail_seq` (the highest `r2_seq` in the uploaded manifest), `tree_seq` (the
  highest `r2_seq` in the uploaded tree), `manifest_etag`, `state`.
- `_r2_tree_nodes` — one row for each live block: level, `lo_key`, block pointer, `child_max_seq`, `item_count`,
  `dirty`.
- `_r2_packs`, `_r2_vlogs` — one row for each object the DO wrote or inherited: state, `total_bytes`, `live_bytes`,
  `shared`, `dead_since`.

Separator keys can be up to about 3 KB (a hash key of 2 KB and a sort key of 1 KB). `_r2_tree_nodes` stores the
shortest separator, so a row is about 40–100 B. For 62,000 blocks this is about 2.5–6 MB. TODO: measure.

#### 4.2.4 Write path

1. A foreground write changes `items` in the same SQLite transaction as today.
2. The same transaction increases `r2_export_state.next_seq` and upserts the `r2_outbox` row of the key with the new
   `r2_seq`. The upsert uses `INSERT ... ON CONFLICT (routing_hash, hk, sk) DO UPDATE`.
3. A transaction commit writes `items` and `r2_outbox` together. A prepare writes only `pending_transactions`, so a
   flush never sees uncommitted state.
4. A TTL expiry and a delete write `r2_outbox` with `is_deleted = 1`.
5. The write sets the flush alarm if no alarm is set.

The extra cost is one row write for each item write. See [Open question 2](#432-the-cost-of-the-r2_outbox-row).

#### 4.2.5 Fast flush

Trigger: `r2_outbox` has rows with `r2_seq > tail_seq`, and at least `F_fast` (start value 1 s) passed since the last
manifest PUT.

1. In one synchronous step, the DO reads all `r2_outbox` rows and their `items` data. It records `S = next_seq − 1`.
2. If a row has a value above the inline limit and the value is not in a vlog yet, the DO puts all such values into
   one new vlog. It records the vlog in `_r2_vlogs` as `uploading`, then uploads it.
3. The DO encodes the tail block: one item entry for each `r2_outbox` row, with tombstones for deletes.
4. If the encoded tail is larger than the tail cap (start value 256 KB), the DO runs a slow flush instead.
5. The DO PUTs the manifest with `etagMatches: manifest_etag`.
6. On success, the DO sets `tail_seq = S` and `manifest_etag`.

#### 4.2.6 Slow flush

Trigger: the tail is larger than the tail cap, or `F_slow` passed since the last slow flush (start value 30–60 s).

1. The DO marks the leaf blocks dirty that contain an `r2_outbox` key. It finds each leaf with a range lookup in
   `_r2_tree_nodes`.
2. For each dirty leaf, the DO reads the key range from `items` in order. It encodes one or more new leaf blocks at the
   target size. A leaf above 2× the target splits. A leaf below 0.25× the target merges with a neighbor.
3. The DO rebuilds each router on the path from a dirty leaf to the root.
4. Compaction adds work to the same flush: the live blocks of a pack below 30% use, while the pack is not shared.
5. The DO writes the blocks to scratch SQLite, adds up the length, and records the pack as `uploading`.
6. The DO streams the pack in one PUT with a known length (`FixedLengthStream`). It does not use multipart upload.
7. The DO PUTs the manifest with the new root, an empty tail and `etagMatches`.
8. In one SQLite transaction, the DO updates `_r2_tree_nodes`, the `live_bytes` of the old packs, sets
   `tree_seq = tail_seq = S`, and deletes the `r2_outbox` rows with `r2_seq <= S`.

A row changed after step 1 keeps a higher `r2_seq`, so step 8 keeps it.

**Large backlog.** When the dirty set is larger than one pack cap, the DO writes N packs, with a yield between them.
It PUTs one manifest at the end. The result is not one exact point in time ([4.2.14](#4214-consistency)).

**CPU.** The DO limits the work of each step and yields to foreground writes, the same as `ttl-expiry.ts` does with
`maxRowsBeforeSleep` and `maxBytesBeforeSleep`.

#### 4.2.7 Read path

`getItem(table, hk, sk, { minSeq? })` in an R2 reader:

1. Compute `routingHash` with `routingHashOf`. Find the position from the client topology or from the routing hint.
2. Get the manifest from the isolate LRU, or through `FokosDBR2Cache`.
3. If `state` is `SPLIT` or `MERGED`, follow `children` ([4.2.9](#429-splits-promotions-and-stale-topology)).
4. If `hk` is in `promoted`, go to the manifest of the range partition.
5. If `minSeq` is set and `meta.seq < minSeq`, send the read to the partition DO.
6. Binary-search the tail. If the key is there, return the entry. A tombstone means the item is absent.
7. Search the root. Get each router block from the isolate LRU, or through the cache.
8. Get the leaf block as a byte range of its pack through `FokosDBR2Cache`.
9. Binary-search the leaf. If the key is absent, the item is absent. A Bloom filter is not necessary.
10. If the entry has a vlog pointer, get the value as a byte range of the vlog.
11. If `ttl_epoch_utc_seconds` has passed, the item is absent.

Each block holds `crc32`. A reader that finds a bad checksum reads from the partition DO and logs the error.

#### 4.2.8 Caching

A new entrypoint `FokosDBR2Cache` has `"cache": { "enabled": true }` and `cross_version_cache: true`. Without
`cross_version_cache`, each deploy empties the cache of immutable objects.

| Path | R2 call | Response headers |
| --- | --- | --- |
| `GET /packs/<ulid>` | full GET | `Cache-Control: public, max-age=31536000, immutable` |
| `GET /vlog/<ulid>` | full GET | `Cache-Control: public, max-age=31536000, immutable` |
| `GET /m/<table>/<position>` | full GET | `Cache-Control: public, max-age=1, stale-while-revalidate=30` |

- A reader calls `ctx.exports.FokosDBR2Cache.fetch(request)` with a `Range` header. The cache returns the slice from
  the cached whole object.
- On a miss, the entrypoint returns the whole object as a `200`. The pack cap keeps this small.
- The isolate LRU holds manifests for at most 1 s and router blocks with no age limit. Router blocks are immutable.
- The entrypoint does not purge on each flush, because of the purge rate limits.
- A pack larger than the cache object limit uses per-block keys: `cf.cacheKey = "pack/<id>/<off>-<len>"`, and the
  entrypoint returns that block as a `200`. The pack cap makes this path rare.
- Local Vitest does not run Workers Cache. Tests check `Cf-Cache-Status` on a preview deploy.

#### 4.2.9 Splits, promotions and stale topology

**Split.** The tree key starts with the routingHash, so each hash child covers one contiguous range of tree keys.

1. The parent runs a slow flush, so the tail is empty.
2. The parent creates each child manifest if it does not exist. The child manifest has the clamped `bounds` and a root
   that references the subtrees of the parent in its range.
3. The parent PUTs its manifest with `state: SPLIT`, `children`, and `etagMatches`.
4. The parent does not flush again.
5. Each child copies the rows of `_r2_tree_nodes` and `_r2_packs` in its range from the parent, as part of the
   repartition copy. The child records the inherited packs as `shared` and registers as a holder
   ([4.2.13](#4213-garbage-collection)).

A leaf block on a split boundary has keys out of the range of one child. A reader filters each leaf by the `bounds` of
the manifest. The next slow flush of the child rewrites that block.

A reader that sees the parent as `ACTIVE` reads a stale state that is consistent. A reader that sees `SPLIT` always
finds the children, because step 2 is before step 3. A reader with a stale topology can read the final root and tail of
a `SPLIT` parent, or follow `children`. The parent keeps its final manifest readable for the split retention time
(start value 24 h).

**Promotion.**

1. The range partition creates its manifest with a root that references the subtree of the routingHash.
2. The hash partition adds the hash key to `promoted` in its next manifest PUT.
3. The next slow flush of the hash partition removes the blocks of that key from its tree.

A reader must check `promoted` before the tree. Without the check, a reader reads frozen data of the promoted key.

**Merge (future).** The merged partition writes one root over the two disjoint subtrees, and both sources go to
`MERGED`.

#### 4.2.10 Topology-free lookup

A reader that knows only `(table, hashKey, sortKey)`:

1. Compute `routingHash` with `routingHashOf` and the seed scope of the table.
2. Get `tables/<table>/routing`. Use the deepest position whose prefix matches the routingHash.
3. Get that manifest. If it is `SPLIT`, follow the child whose bounds hold the routingHash. Repeat.
4. If the hash key is in `promoted`, go to the range manifest. In a range partition, follow `children` by sort key.

The routing hint is only a hint. The `SPLIT` chain is the source of truth. Without the hint, the reader starts at the
root position of the shard group and follows the chain: one cached hop for each split level.

#### 4.2.11 Clones

1. The clone job creates one manifest under the new table for each partition of the source. Each new manifest has the
   same root, tail and seq as the source manifest.
2. Each new partition DO registers as a holder of every pack and vlog that it inherits.
3. After the clone, each table writes new packs. The two tables share only the packs from before the clone.

#### 4.2.12 Snapshots and restored DOs

- A snapshot is one immutable `snapshots/<snapshotId>/manifest`. It holds a copy of the meta, root and tail of each
  partition at the cut. The snapshot registers as a holder of each referenced pack and vlog.
- For an exact cut across partitions, the snapshot job restores DOs from bookmarks
  ([the backup and restore RFC](2026-11-17-consistent-backup-and-restore.md)), runs one slow flush in each, and copies
  their manifests.
- A DO restored from a bookmark has its full data in SQLite. On start, it checks the packs that `_r2_tree_nodes`
  references with `HEAD`. It rebuilds each missing block from SQLite. Thus garbage collection does not keep data for
  the 30-day bookmark window.

#### 4.2.13 Garbage collection

**Local garbage collection.** This is the common case. It needs no R2 read and no LIST.

1. A slow flush lowers `live_bytes` of each pack whose blocks it replaces.
2. When `live_bytes = 0` and `shared = 0`, the DO sets `dead_since`.
3. A DO alarm deletes each pack with `dead_since + grace < now`. It deletes the `_r2_packs` row after the delete.
4. Vlogs use the same steps.

**Shared garbage collection.** Only splits, clones and snapshots share packs.

- Registry DOs, sharded by `hash(pack_id)`, hold `pack_holders(pack_id, holder)`.
- A new holder registers its inherited packs in one batch for each registry shard.
- When a holder stops using a pack, it sends an idempotent `release(pack_id, holder)`.
- The owner of the pack is one holder too. A pack with no holder goes into the grace period, then the registry deletes
  it.
- A holder registers with the registry, not with its parent. Thus a clone of a clone needs no extra step.

**Grace period.** The grace period must be longer than the largest manifest age that a reader can use, plus the
longest request. That is `max-age + stale-while-revalidate` in both cache tiers, plus the isolate LRU age, plus the
split retention time for a `SPLIT` parent. Start value: 1 h, and 24 h after a split. A cached copy of a deleted pack is
harmless, because its content does not change.

**Compaction.**

- A slow flush copies the live blocks of a pack below 30% use, when the pack is not shared.
- A slow flush copies the live values of a vlog below 50% use into a new vlog.
- A slow flush also copies the live blocks of a pack whose dead bytes are older than `D_max` days, so deleted item data
  leaves R2 in a bounded time. See [Open question 3](#433-the-value-of-d_max).
- Compaction does not copy a shared pack. A copy frees nothing while another holder uses the pack.
- Compaction reads from SQLite. Reads do not get slower if compaction is late.

**Audit.** A monthly job LISTs `packs/` and `vlog/` and compares the result with `_r2_packs`, `_r2_vlogs` and the
registry. At 1,000 keys for each LIST call, 1,000,000 objects cost 1,000 Class A operations (less than $0.01). The
audit reports leaks. It deletes only objects older than the grace period that no holder knows.

#### 4.2.14 Consistency

- An R2 read is eventually consistent for each item.
- An R2 read can see one partition of a cross-partition transaction before the others.
- A normal flush encodes in one synchronous step, so the R2 state is the partition state at one `r2_seq`.
- A large backlog flush yields between packs. Each key is then at some version between the start and the end of the
  flush. An exact cut needs a snapshot ([4.2.12](#4212-snapshots-and-restored-dos)).
- A write returns its `r2_seq`. A read with `minSeq` gives read-your-writes. See
  [Open question 4](#434-r2_seq-in-the-write-response).
- Two cache tiers and data centers can serve different manifest ages. A client can see `seq` go down between two
  reads. `minSeq` also gives monotonic reads.
- A reader must treat an item with a passed `ttl_epoch_utc_seconds` as absent.

#### 4.2.15 Invariants

| Invariant | Mechanism |
| --- | --- |
| A pack or a vlog does not change after the upload. | New ULID names. No code path PUTs an existing name. |
| A manifest references only uploaded objects. | The DO PUTs the pack and the vlog before the manifest. |
| One writer for each manifest. | The single DO, plus `etagMatches`. After `SPLIT`, the parent does not flush. |
| A live manifest does not reference a deleted object. | `live_bytes`, the holder registry and the grace period. |
| The tail takes priority over the tree. | The reader searches the tail first. A tail entry has `r2_seq > tree_seq`. |
| The flush contains only committed state. | `r2_outbox` is written only with `items`. |
| A child reader sees no key outside its range. | `bounds` in the manifest. The reader filters each leaf. |

#### 4.2.16 Failure and recovery

| Failure | Recovery |
| --- | --- |
| The DO stops after a pack PUT and before the manifest PUT. | The pack is `uploading` in `_r2_packs`. On start, the DO deletes it and runs the flush again. |
| The manifest PUT fails with a precondition error. | Another writer changed it. This is a defect, or a restored DO. The DO GETs the manifest, logs it, and stops flushing until an operator acts. TODO: decide the action for a restored DO. |
| R2 is not available. | `r2_outbox` grows. Writes continue. The next flush is a large backlog flush. |
| A reader finds a missing pack (404). | The manifest is older than the grace period, or this is a defect. The reader reads from the partition DO. |
| A reader finds a bad checksum. | The reader reads from the partition DO and logs the error. |
| A registry `release` is lost. | `release` is idempotent. The holder sends it again until it gets a success. The audit finds the rest. |

#### 4.2.17 Performance and cost

**Latency of `getItem`.** Estimates. TODO: measure. Assumptions:

- An isolate LRU hit takes less than 0.1 ms. A Workers Cache hit takes 1–5 ms.
- An R2 range GET takes 20–50 ms near the bucket, and 80–250 ms far from it.
- A DO RPC takes 2–10 ms in one region, and 80–200 ms across continents.

 It is not known if a miss runs the Worker in the lower tier or in the upper tier.

| Path | Steps | Estimate |
| --- | --- | --- |
| Today: Worker → partition DO | 1 RPC | 5–15 ms near, 80–200 ms far |
| R2 reader, hot | LRU (manifest, routers) → lower-tier hit (leaf) | 2–8 ms |
| R2 reader, lower-tier miss, upper-tier hit | + 1 hop to the upper tier | 15–80 ms |
| R2 reader, both tiers miss | + R2 full GET of a pack of at most 16 MB, for each hop | 50–200 ms for each hop |
| Large value | + 1 cached range of the vlog | +2–80 ms |

**Read cost.** Each `FokosDBR2Cache` call is a billed Worker request (about $0.30 for each million), about the cost of
one Class B GET. The isolate LRU and the inline root keep a hot read at about 1 cache call.

**Write cost for each partition.** For `W` writes each second:

- Fast flush: 1 PUT each `F_fast` while writes arrive, plus 1 vlog PUT when the tail has large values.
- Slow flush: 2 PUTs each time the tail fills or `F_slow` passes.
- Bytes for each slow flush: about `min(dirty items, leaf count) × block size`.

Example estimates (TODO: measure), 1 KB items, random keys, `F_fast = 1 s`, tail cap 256 KB:

| Write rate | PUTs each second | Cost each month |
| --- | --- | --- |
| 1 write/s | about 1.0 | about $12 |
| 100 writes/s | about 1.4 | about $16 |
| 100 writes/s with `F_fast = 10 s` | about 0.13 | about $1.5 |

A partition with no writes makes no PUT.

**Storage.** Live data plus dead blocks in the grace period plus pack space below the compaction limit. At most about
`1 / 0.3 ≈ 3.3×` the live data before compaction, plus the grace period. TODO: measure.

#### 4.2.18 Bottlenecks

| Bottleneck | Effect |
| --- | --- |
| R2 allows about 1 write each second for one key. | The manifest changes at most once each second. Freshness cannot be below about 1 s. |
| Class A cost for each flush | The cost increases with the flush rate, not with the write count. |
| DO CPU | Encoding and compression share the isolate with foreground writes. A large slow flush adds write latency. |
| Block rewrite for random writes | Each changed item rewrites one full block. Upload bytes increase with the block size. |
| Manifest size | A large tail makes each manifest refresh larger, about once each second for each hot partition and upper-tier data center. |
| Cold reads | When both tiers miss, each hop needs one full GET of a pack. |
| Worker request billing | Each cache call costs about one Class B GET. |
| Shared packs | Splits and clones register many holders. Space returns only when the holders change their blocks. |
| Deleted data | Old packs keep deleted bytes until compaction or `D_max`. |

#### 4.2.19 Knobs: freshness against cost

Freshness is the time from a committed write to the moment an R2 reader can see it:

```
freshness ≈ the wait for the next fast flush   (0 … F_fast)
          + encode and PUT                      (about 50–200 ms, TODO: measure)
          + the manifest age in the cache       (max-age + revalidation)
          + the manifest age in the isolate LRU
```

| Knob | Lower value | Higher value | Start value |
| --- | --- | --- | --- |
| `F_fast` | Better freshness, more manifest PUTs | Less cost, worse freshness | 1 s (the R2 limit) |
| Tail cap | More slow flushes and packs | Larger manifest, fewer packs | 256 KB |
| `F_slow` | More packs | Fewer packs, more writes for each block | when the tail is full, or 30–60 s |
| Leaf block size | Fewer bytes for each rewrite, more router entries | More bytes for each rewrite, fewer hops | 16–32 KB |
| Inline value limit | More vlog PUTs and reads | Larger blocks | 1 KB |
| Pack cap | Faster cold range reads, more PUTs | Fewer PUTs, slower cold reads | 16–32 MB |
| Manifest `max-age` | Better freshness, more R2 GETs | Fewer GETs, worse freshness | 1 s |
| Manifest `stale-while-revalidate` | More reads wait for R2 | Fewer waits, older reads | 30 s |
| Manifest age in the isolate LRU | Better freshness, more billed cache calls | Fewer calls, worse freshness | 1 s |
| Grace period | Less storage | Safer for slow and stale readers | 1 h, 24 h after a split |
| Compaction limits | Less space, more rewrites | Fewer rewrites, more dead bytes | packs 30%, vlogs 50%, `D_max` |

Estimate with `F_fast = 1 s` and a tail cap of 256 KB: freshness of about 1.5–3 s at p99. TODO: measure.

#### 4.2.20 Deployment and rollback

- The R2 copy is a per-table option. It is off by default. TODO: decide the configuration surface with
  [the configuration surface RFC](2026-09-25-configuration-surface.md).
- The migration adds `r2_outbox`, `r2_export_state`, `_r2_tree_nodes`, `_r2_packs` and `_r2_vlogs`. When the option is
  off, a write does not write `r2_outbox`.
- When the option is turned on for an existing partition, the DO runs a first flush: a bulk build of the tree in N
  packs, then one manifest.
- Rollback: turn the option off. The DO stops flushing. The R2 objects stay until an operator deletes the prefix of
  the table. Readers fall back to the partition DO.

#### 4.2.21 Testing

- Unit tests for each format: encode, decode, the order of tree keys against the order of `items`.
- Property tests: after random writes, deletes, splits and flushes, each `getItem` from R2 equals the item in SQLite
  at `tail_seq`.
- Crash tests with the deterministic DO test machinery
  ([the deterministic DO test RFC](2026-09-22-deterministic-do-test-machinery.md)): a stop between each two steps of
  a fast flush and of a slow flush.
- Garbage collection tests: no live manifest, snapshot or holder references a deleted pack.
- A preview deploy checks `Cf-Cache-Status: HIT` for packs and manifests, and measures the latency table in 4.2.17.
- A cost test counts Class A operations for each write rate in 4.2.17.

### 4.3 Open questions

#### 4.3.1 The table identity in R2 keys

`<table>` must be stable across a rename and different between a source and its clone. The options are the table name
with the namespace fingerprint, or a new table UID. The answer changes the clone step and the topology-free lookup.

#### 4.3.2 The cost of the r2_outbox row

`r2_outbox` adds one row write to each item write. The other option is a new `r2_seq` column on `items` with an index,
plus a table of deleted keys. Both options need a benchmark of rows written and of write latency.

#### 4.3.3 The value of D_max

`D_max` bounds how long deleted item data stays in R2. A low value increases compaction rewrites. The value depends on
the data deletion promise of FokosDB.

#### 4.3.4 r2_seq in the write response

`minSeq` needs the `r2_seq` of a write in the write response. This changes the public API
([the public API audit](2026-11-24-public-api-and-wire-protocol-audit.md)).

#### 4.3.5 The repartition hook

The split steps in 4.2.9 need a hook in the repartition flow (`src/sharding/repartition-flow.ts`): the final slow
flush of the parent, and the copy of `_r2_tree_nodes` and `_r2_packs` to the children. The exact step is not decided.

#### 4.3.6 The block codec

Workers has `CompressionStream` with `gzip` and `deflate-raw`. A WebAssembly zstd gives a better ratio for small
blocks, at a CPU and bundle cost. TODO: measure both.

### 4.4 Future extensions

#### 4.4.1 Regional replica DOs

A replica DO is a regional DO that copies a set of hash keys or key ranges of one partition from R2 into its own
SQLite. It can run filters, projections and queries with the existing expression engine on local data.

A replica DO uses the same R2 objects and the same cache entrypoint as an R2 reader:

- It gets the manifest through `FokosDBR2Cache` and applies the tail entries with `r2_seq` above its local value.
- It walks the routers that cover its ranges and skips each child with `child_max_seq` at or below its local value.
- It gets the changed leaf blocks and upserts their items.

With this RFC alone, a replica DO gives little benefit over an R2 reader. For a `getItem`, a hot R2 reader already
takes about 2–8 ms (see [4.2.17](#4217-performance-and-cost)), and the replica is not fresher than the manifest. Before
a replica DO is worth building, a design must specify at least:

- Which reads go to a replica, and how a reader finds the replica of its region.
- How a replica chooses its keys and ranges, and when it evicts them.
- The poll interval and its cost, or a push of manifest changes to the replicas.
- The cost of a sync for each changed block, and the limit on SQLite size of a replica.
- The benefit for queries and scans, which this RFC does not specify from R2.

## 5. Alternative options

- **LSM on R2.** Sorted segments with Bloom filters and leveled manifests. Rejected: the read cost increases with the
  number of segments, and it needs a compactor that reads R2 back and races the flush on the manifest.
- **CoW tree with one R2 object for each leaf.** Leaves of 4–8 MB, a flat root. Rejected: one PUT for each dirty leaf.
  At 100 random writes each second, about 43 PUTs each second (about $500 each month for each partition).
- **One object for each leaf with a delta overlay.** Small delta objects, and leaf rewrites when deltas are large.
  Rejected: reads probe up to K deltas, and tombstones stay until the leaf rewrite. The tail in the manifest gives the
  same batch with no extra hop.
- **Mutable data objects with CAS.** Rejected: an overwrite is still one PUT for each object. Mutable objects need a
  short cache TTL or a purge. They break zero-copy clones, splits and snapshots, and they add the R2 limit for each key
  to each hot block.
- **Cache API (`caches.default`) with per-block keys.** Rejected: it is per data center, it does not collapse misses,
  and it is not tiered. Workers Cache gives all three.
- **Multipart upload for packs.** Rejected: each part is a Class A operation. A streamed PUT with a known length needs
  no buffer of the full pack.
- **A garbage collection side object in R2 for each partition.** Rejected: it adds 1 PUT to each slow flush. The owner
  DO already holds the same data in `_r2_packs`.

## 6. Frequently asked questions

**Is there one block for each item?** No. A leaf block holds about 16–300 items of one key range. A pack holds about
500–2,000 blocks of one slow flush. [Appendix B.1](#b1-one-update-from-write-to-garbage) shows one update.

**Why packs, and not one object for each block?** A slow flush with 500 dirty blocks costs 500 PUTs without packs and
1 PUT with packs. Packs batch PUTs. Blocks keep each rewrite small.

**Why blocks of 16–32 KB, and not 128 or 512 KB?** Random writes rewrite one full block for each changed item. See the
table in [4.2.1](#421-units-of-data-item-block-pack-vlog). Sequential writes use large blocks.

**Is this free of compaction?** Reads need no compaction. Compaction only frees space, it reads SQLite, and it can
run late.

**Why no Bloom filter?** Each key is in at most one leaf block, and the reader reads that block anyway.

**Can a reader see a half-applied transaction?** Yes, across partitions. Use the partition DO or a snapshot for an
atomic read.

**What happens when the reader cache is old?** The reader reads an older consistent state. The grace period keeps
every object of that state.

**Does a 1 MiB item fit?** Yes. The value goes into a vlog. The leaf holds a pointer of about 40 B.

## 7. References

- `docs/ideas/2026-11-17-consistent-backup-and-restore.md`
- `docs/agent-plans/2026-11-17-routing-hash-seed-scope.md`
- `docs/ideas/2026-09-22-deterministic-do-test-machinery.md`
- `docs/ideas/2026-09-25-configuration-surface.md`
- `docs/ideas/2026-11-24-public-api-and-wire-protocol-audit.md`
- `docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md`
- `packages/fokosdb/src/shared/partition/partition-store.ts`
- `packages/fokosdb/src/sharding/hash-primitives.ts`
- `packages/fokosdb/src/sharding/repartition-flow.ts`
- [Workers Cache](https://developers.cloudflare.com/workers/cache/)
- [R2 pricing](https://developers.cloudflare.com/r2/pricing/)
- [R2 limits](https://developers.cloudflare.com/r2/platform/limits/)
- [R2 Workers API](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/)
- [workerd `src/workerd/util/sqlite.h`](https://github.com/cloudflare/workerd/blob/main/src/workerd/util/sqlite.h)

---

## Appendix A. File formats

All integers are little-endian unless the text says otherwise. `varint` is an unsigned LEB128.

### A.1 Tree key encoding

```
routing_hash (8 B, big-endian, as stored in items) | escape(hk) 0x00 0x01 | escape(sk)
escape: each 0x00 byte → 0x00 0xFF
```

The byte order of tree keys is the order of `(routing_hash, hk, sk)`. TODO: check it against the BLOB order of the
`items` primary key and against [the key ordering note](key-ordering-utf8-convergence.md).

### A.2 Block

```
Header (16 B, not compressed)
  u8   type           1 = leaf, 2 = router, 3 = tail
  u8   level          0 = leaf, 1..2 = router
  u8   codec          0 = none, 1 = deflate-raw
  u8   flags
  u16  entry_count
  u32  raw_length     length after decompression
  u32  crc32          of the stored body
  u16  restart_count
Body (compressed with codec)
  entries[]           prefix-compressed against a restart point each 16 entries
  ref_table           leaf: u8 count, 16 B vlog ULID[]; router: u16 count, 16 B pack ULID[]
  u32 restart_offsets[restart_count]
```

Leaf or tail entry:

```
varint shared_len, varint suffix_len, suffix bytes     (tree key)
varint r2_seq
varint v
u8     kind           0 = inline, 1 = vlog, 2 = tombstone (tombstone only in a tail)
u8     data_kind
varint ttl_epoch_utc_seconds + 1   (0 = no TTL)
inline: varint value_len, value bytes
vlog:   u8 vlog_ref, u32 offset, u32 length
```

Router entry:

```
varint shared_len, varint suffix_len, suffix bytes     (shortest separator)
u16    pack_ref
u32    offset, u32 length
varint child_max_seq
varint child_items
```

### A.3 Pack: `packs/<ulid>.pack`

```
offset 0          router blocks of this flush (level 2, then level 1)
                  leaf blocks, in tree key order
index_offset      block index: [u8 type, u8 level, varint key_len, first_key, u32 offset, u32 length] …
end − 32          footer
                    "FKPK"   magic
                    u16      version
                    u16      flags
                    u32      block_count
                    u64      index_offset
                    u32      index_length
                    u32      crc32 of the index
                    u32      reserved
```

Readers do not read the index or the footer. A block pointer gives the offset and length. The audit and a rebuild use
the index. The pack does not name its writer, so a clone can share it. `_r2_packs` and the registry hold ownership.

### A.4 Vlog: `vlog/<ulid>.vlog`

```
records           value bytes, next to each other; deflate-raw when it saves at least 10%
index             [u64 routing_hash, varint r2_seq, u32 offset, u32 length, u8 codec] …
end − 32          footer: "FKVL", u16 version, u16 flags, u32 record_count, u64 index_offset, u32 index_length,
                  u32 crc32, u32 reserved
```

### A.5 Manifest: `tables/<table>/p/<position>/manifest`

```
"FKMF"  u32 magic
u16     version
u32     meta_length,  meta (UTF-8 JSON)
u32     root_length,  root block (type 2)
u32     tail_length,  tail block (type 3)
u32     crc32 of all bytes before it
```

Meta example:

```json
{
  "table": "T1",
  "position": "h.3",
  "state": "ACTIVE",
  "seq": 918244,
  "treeSeq": 917900,
  "bounds": { "lo": "6000000000000000", "hi": "8000000000000000" },
  "height": 2,
  "children": [],
  "promoted": ["dSNob3QtdGVuYW50"],
  "updatedAt": "2026-11-27T10:15:03Z"
}
```

- `seq` is the highest `r2_seq` in the tail. `treeSeq` is the highest `r2_seq` in the tree.
- `promoted` holds base64url hash keys. When the list is larger than 16 KB, it moves to a side object. TODO: decide the
  side object format.
- After a split: `"state": "SPLIT"`, and `"children": [{ "position": "h.3.0", "lo": "…", "hi": "…" }, …]`.

### A.6 Routing hint and snapshot manifest

`tables/<table>/routing`:

```json
{
  "table": "T1",
  "version": 412,
  "prefixes": { "6": "h.3", "60": "h.3.0", "64": "h.3.1" },
  "promoted": { "dSNob3QtdGVuYW50": "r.<routingHash hex>.~min.~max" }
}
```

`snapshots/<snapshotId>/manifest`:

```json
{
  "snapshotId": "S1",
  "createdAt": "2026-11-27T10:30:00Z",
  "partitions": [
    { "table": "T1", "position": "h.3", "seq": 918244, "copy": "snapshots/S1/p/T1/h.3" }
  ]
}
```

Each `copy` object has the manifest format of A.5.

### A.7 DO-local SQLite

```sql
CREATE TABLE IF NOT EXISTS r2_outbox (
    outbox_id    INTEGER PRIMARY KEY,
    routing_hash BLOB    NOT NULL,
    hk           BLOB    NOT NULL,
    sk           BLOB    NOT NULL DEFAULT x'',
    r2_seq       INTEGER NOT NULL,
    is_deleted   INTEGER NOT NULL,
    vlog_id      BLOB,              -- set when a fast flush put the value in a vlog
    vlog_offset  INTEGER,
    vlog_length  INTEGER,
    UNIQUE (routing_hash, hk, sk)
) STRICT;

CREATE TABLE IF NOT EXISTS r2_export_state (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    next_seq      INTEGER NOT NULL DEFAULT 1,
    tail_seq      INTEGER NOT NULL DEFAULT 0,
    tree_seq      INTEGER NOT NULL DEFAULT 0,
    manifest_etag TEXT,
    state         TEXT    NOT NULL DEFAULT 'ACTIVE'
) STRICT;

CREATE TABLE IF NOT EXISTS _r2_tree_nodes (
    node_id       INTEGER PRIMARY KEY,
    level         INTEGER NOT NULL,
    lo_key        BLOB    NOT NULL,
    parent_id     INTEGER,
    pack_id       BLOB    NOT NULL,
    offset        INTEGER NOT NULL,
    length        INTEGER NOT NULL,
    child_max_seq INTEGER NOT NULL,
    item_count    INTEGER NOT NULL,
    dirty         INTEGER NOT NULL DEFAULT 0
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS _r2_tree_nodes_key ON _r2_tree_nodes (level, lo_key);

CREATE TABLE IF NOT EXISTS _r2_packs (
    pack_id     BLOB    PRIMARY KEY,
    state       TEXT    NOT NULL,   -- uploading | live | dead
    total_bytes INTEGER NOT NULL,
    live_bytes  INTEGER NOT NULL,
    shared      INTEGER NOT NULL DEFAULT 0,
    created_at  INTEGER NOT NULL,
    dead_since  INTEGER
) STRICT;

-- _r2_vlogs has the same columns as _r2_packs, with vlog_id.

-- In each registry DO, sharded by hash(pack_id):
CREATE TABLE IF NOT EXISTS pack_holders (
    pack_id BLOB NOT NULL,
    holder  TEXT NOT NULL,          -- a partition DO id or a snapshot id
    PRIMARY KEY (pack_id, holder)
) STRICT;
```

### A.8 How the objects reference each other

```
tables/T1/routing                        (hint: routingHash prefix → position)
        │
        ▼
tables/T1/p/h.3/manifest  ─────── children / promoted ───► tables/T1/p/r.<hex>.~min.~max/manifest
   │  inline root (router block)
   │  inline tail (newest item entries)
   │
   │ (pack P7, offset 0, length 9 812)
   ▼
packs/P7.pack ── L1 router block
   │ (pack P2, offset 1 048 576, length 18 304)
   ▼
packs/P2.pack ── leaf block ── item entry (hk "u#42", sk "order#9")
                                   │ (vlog V5, offset 4 096, length 700 113)
                                   ▼
                              vlog/V5.vlog [4 096 … 704 208]

tables/T2/p/h.3/manifest   (a clone of T1) ── the same root ──► packs/P7, P2 … (shared)
snapshots/S1/manifest      ── copies of T1 manifests ─────────► packs/… (shared)
```

## Appendix B. Worked examples

### B.1 One update, from write to garbage

1. Item `u#42/order#9` is in leaf B2, at `P1 [40 960, 57 344)`. B2 holds about 16 items of one key range.
2. A `putItem` writes `items` and `r2_outbox` with `r2_seq = 918 001`. No R2 operation.
3. A fast flush writes the manifest with a tail that holds the item. Readers see the new value. B2 in P1 still holds
   the old value, but the tail takes priority.
4. A slow flush rebuilds B2 as B2' from `items`, with all of its items. It rebuilds the router A as A'. It writes
   `P2 = [A'][B2']`, then the manifest with a root that points to A' and an empty tail.
5. The old B2 and A in P1 are dead bytes. `_r2_packs.live_bytes` of P1 goes down.
6. When P1 has no live bytes, the DO sets `dead_since`, and it deletes P1 after the grace period. When P1 is below 30%
   use first, compaction copies its live blocks into a later pack.

### B.2 getItem(u#42, order#9)

1. Compute the routingHash. The topology gives `h.3`. Get the manifest from the isolate LRU.
2. `u#42` is not in `promoted`. The tail does not hold the key.
3. The root entry for the key is `(P7, 0, 9 812)`. Get the L1 router from the isolate LRU.
4. The L1 router entry is `(P2, 1 048 576, 18 304)`. Get that range of `packs/P2` through `FokosDBR2Cache`.
5. The leaf entry has `kind = vlog (V5, 4 096, 700 113)`. Get that range of `vlog/V5` and return the value.
