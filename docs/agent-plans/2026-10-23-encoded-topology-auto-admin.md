# RFC — Encoded topology: an admin Durable Object, Workers Cache, and a caller-side topology cache

**State:** Draft
**Date:** 2026-10-23
**Author:** Lambros Petrou

**Status:** Nothing is built. Section 3 lists the milestones.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
  - [4.3 Open questions](#43-open-questions)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

A FokosDB table starts with `rootTreesN` hash roots. Each partition splits when it grows past its cap, and the
tree gets deeper. The target scale is from about 1,000 partitions (500 GB) to about 1,000,000 partitions (500 TB).

Today a caller always enters at a root:

- `FokosShardingClient.resolve`, `resolveRange`, and `resolveAll` return `FokosRouter.rootContext`. Each one has
  the comment `// TODO Leapfrog to deep levels based on cached topology.`
- A root that has split forwards the request, and each router on the path forwards again. A request to a leaf at
  hash depth `d` costs `d` extra Durable Object round trips. At about 10 ms for each hop, a depth of 3 adds
  about 30 ms to each request.
- The roots and the shallow routers receive every request of the table. Their throughput is the limit of the
  table, not the throughput of the leaves.

Each partition already has a local route cache
(`docs/agent-plans/2026-09-27-learned-routes-for-every-dispatch-shape.md`): the hash arena (`HashTopology`), the
promotion Bloom filter (`PartialRangeTopology`), and the learned range slices
(`fokos_range_hierarchy`). A partition fills it from the routing of the responses that it forwards. A Worker has no
cache, and a partition knows only the part of the tree below it that its own traffic showed.

The article [FokosDB](https://www.lambrospetrou.com/articles/fokosdb/#fokosdb-encoded-topology) gives the
encoding. The hash tree is compact because `rootTreesN` and `hashSplitN` are fixed, partitions never merge, and
`hashChildIndex` is deterministic for each depth. So each hash partition needs one fact: "has it split?". The
range trees of promoted hash keys depend on the data, so they need their boundaries.

This RFC defines the source of truth for that topology, how a partition reports a repartition to it, how a Worker
or a Durable Object gets the encoded topology through Workers Cache, and how the caller uses it.

### Glossary

- **Admin DO** — the Durable Object of the host that holds the topology of one shard group. Its name is
  `fokos.topo.<shardGroup>` (`topologyAdminName`). For FokosDB it is an instance of `FokosDBAutoAdminDO`. The
  same class has one more instance for each table, `fokos.admin.<tableName>`, that holds the GSI catalog of
  `docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md` (Section 4.3.2) and no topology.
- **Topology keeper** — `FokosTopologyKeeper`, the class of the sharding library that the admin DO embeds. It
  stores the facts and encodes them.
- **Topology entrypoint** — a named `WorkerEntrypoint` of the host, with Workers Cache enabled, that serves the
  encoded topology over `fetch()`.
- **Hash split bitmap** — the encoding of the hash forest of one shard group: one bit for each hash partition.
- **Topology cache** — the module-scope cache in each isolate that holds the topology views over the fetched bytes.
- **Topology version** — the pair `(epoch, version)`. `version` increases when the keeper learns a new fact.
  `epoch` increases when the keeper resets a shard group.

## 2. Goals and requirements

### 2.1 In scope

1. A partition reports each completed repartition (hash split, key promotion, range split) to the admin DO. The
   report survives a restart of the partition and arrives at least once.
2. Each shard group has its own admin DO. It stores the topology of that shard group, and it is the source of
   truth for it.
3. A caller gets the encoded hash topology of one shard group through one shared cache entry.
4. `FokosShardingClient` sends a request to the deepest known hash partition of the key, in a Worker and in a
   Durable Object.
5. The sharding runtime uses the same topology when it forwards.
6. A caller gets the range topology of one promoted hash key through its own cache entry.
7. The host owns the admin DO, the entrypoint, and the transport. The sharding library gives the keeper, the hooks,
   and the helpers. The library does not import FokosDB code.
8. FokosDB uses all of this through `FokosDBAutoAdminDO`, and the feature is off when the consumer does not
   configure it.

### 2.2 Out of scope

- The local route cache of Section 4.3.1 of `docs/agent-plans/2026-09-26-fokos-sharding-client.md`. The client
  in this RFC does not learn from the routing of responses. The two caches can work together later.
- The GSI catalog in `fokos.admin.<tableName>`. `docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md`
  owns it.
- The follow-ups of Section 2.4.
- A purge or an invalidation of Workers Cache after a repartition. Section 5 gives the reason.

### 2.3 Requirements

1. **A topology hint never changes a result.** A missing, old, or wrong topology changes latency only. The rules
   of Section 4.3.1 of `docs/agent-plans/2026-09-26-fokos-sharding-client.md` apply without change: the fallback
   test, "remove one hint", the range entry rule, and "learn only from your own shard group".
2. **The keeper records a repartition only after the source writes `completed`.** At that point every target has
   acknowledged and has its data. A topology fact then never names a target that cannot serve.
3. **A topology request must not await a fetch.** `resolve` stays synchronous and does no I/O. A request with no
   cached topology enters at the root, as today.
4. **The topology cache must not keep a promise across requests.** The Workers runtime refuses I/O for one request
   from the context of another request. The caller passes each fetch to its own `waitUntil`, and the cache stores
   only the views over the fetched bytes, after the fetch resolves.
5. **The topology cache is bounded.** It is an `LRUCache` (`shared/cache-lru.ts`) bounded by the total bytes of
   its entries (Section 4.2.6).
6. **A caller never replaces a topology with an older one.** It accepts a response only when its topology version
   is equal to or higher than the cached one, compared as `(epoch, version)`.
7. **The admin DO receives few requests.** Registrations are one RPC for each repartition. Reads reach it only on a
   Workers Cache miss or a revalidation. Each shard group has its own admin DO, so the load of one shard group
   does not go to the admin DO of another.
8. **Every report carries the `actorId` of the source and of each target**, so the admin DO has the full record of
   what happened.
9. **The promoted-key Bloom filter starts small.** Its first size is about 25 KB, and the keeper rebuilds it larger
   when the number of promoted keys grows (Section 4.2.4).
10. **A caller uses the encoded topology in place.** The caller makes typed-array views over the fetched bytes and
    does not decode them into objects, so a refresh adds almost no GC pressure. The hash split bitmap does this in
    M2. The range topology does this with a sorted boundary list and two rank arrays in one `Uint8Array` (M5).

### 2.4 Follow-ups

Later changes, not in this RFC:

1. **A backfill** of the topology of a table that split before the feature was on.
2. **A lazy schema in `PartitionDO`.** Today the constructor writes the schema, so a request to a range root that
   no repartition created (a Bloom false positive, Section 4.2.9) leaves an empty object. A common interceptor can
   run the schema migrations only after the partition receives a valid request. A range root exists only after a
   repartition initializes it, so it always has a valid identity when it is real.
3. **Protection of the roots when the topology cache is empty.** Today a caller with no cached topology enters at
   the root (Requirement 2.3.3), so a cold start of many isolates sends their traffic to the roots. Possible
   changes, alone or together:
   - an option that makes a request wait for the first topology fetch, in place of the root;
   - native rate limiting with the Rate Limiting binding
     ([rate limit](https://developers.cloudflare.com/workers/runtime-apis/bindings/rate-limit/)), where a request
     above the limit waits for the topology.
4. **Visit planning in the caller.** In this RFC, a range request enters at the deepest known partition that holds
   the whole requested interval, and that partition plans the visits with `rangeVisits`. A caller can instead split
   the request into one visit for each segment run, and send each visit to the deepest known partition of its
   segments. Example: `["h", "m")` then goes to B1 with `[h, k)` and to B2 with `[k, m)`, and B gets no request.
   This removes one hop, and the ancestors get traffic only for segments with unknown leaves. The caller must then
   carry `QueryPageBudget` across its visits, and the cursor must name the place to resume. The range body of
   Section 4.2.9 already holds the data for this plan.
5. **A compact boundary index.** Front coding of the boundaries (blocks of 16, a full first boundary, then a shared
   prefix length and a suffix), or a Patricia trie in one `Uint8Array` as an index over the boundaries. Only when a
   measurement shows that the binary search of Section 4.2.9 costs too much.
6. **Hot range reads in one shard group.** `topology.GetForRange` has one cache key for each promoted hash key,
   so request collapsing does not merge the reads of two keys. The reads at the admin DO are about
   `hot promoted keys * upper tiers / maxAgeS`. Example: 10,000 hot keys, 10 upper tiers (an assumption), and
   `maxAgeS` 10 give about 10,000 requests each second. That is more than the soft limit of 1,000 requests each
   second of one Durable Object ([limits](https://developers.cloudflare.com/durable-objects/platform/limits/)).
   Options to examine:
   - The admin DO writes each range body to Workers KV, one KV key for each hash key, and callers read from KV.
     KV spreads the reads, but each read and each write costs more.
   - The admin DO writes the bodies to R2, and callers read from R2. The read throughput: `TODO: measure`.
   - The range root of each promoted key owns the range topology of that key, and each range split also reports
     to it. The range root then gets more load and more storage, and a cold start of it delays the routing, so
     this needs strict limits. A variant: the range root and the next level own it together, and the range root
     holds only the aggregated data from which it builds the body.
   - `M` range keepers for each shard group, chosen by `hash(hashKey) mod M`. `M` is fixed when the shard group
     is created, as `rootTreesN` is. The hash body stays in the admin DO of the shard group.

## 3. Milestones

Each milestone ships alone and keeps every test green.

### M1 — The keeper, the outbox hook, and the outbox job

`FokosTopologyKeeper`, the default class `FokosShardingAutoAdminDO`, the runtime option `adminOutbox`, the
`fokos_admin_outbox` table, and the `admin_outbox` job. The admin DO then holds the full topology. No caller reads
it yet.

### M2 — The encoded hash topology and the client topology cache

The hash split bitmap codec, the batched build of the keeper, the `topology.Get` read, the entrypoint helper, and
the topology cache in `FokosShardingClient`. A Worker, and a Durable Object that uses `FokosShardingClient`, then
enter at the deepest known hash partition.

### M3 — The runtime forwards with the topology

`#hashTopologyOwner` in `runtime.ts` also reads the topology cache. A root or a router then forwards to the
deepest known hash partition, also for keys that its own traffic never showed.

### M4 — FokosDB uses the feature

`FokosDBAutoAdminDO` embeds the keeper, with one instance for each shard group. `PartitionDO` and
`TransactionCoordinatorDO` configure the outbox hook. `FokosDB` and the coordinator configure the topology cache.
`destroy` resets the topology. The example `examples/http-api` enables it.

### M5 — The range topology

The `topology.GetForRange` read and the range part of the topology cache. A caller with a Bloom hit then enters at
the deepest known range partition of the promoted key, or at its range root when it has no range entry yet.

### M6 — The runtime forwards with the range topology (optional)

A partition reads the range part of the topology cache and the promoted-key Bloom filter of the hash body when it
forwards a promoted key. A hash partition and a range router then forward to the deepest known range partition of
the key, also for keys and intervals that their own forwards never showed, and the range root receives less
traffic. Section 4.2.9 gives the details. The range frontier (`rangeVisits`) is not in this milestone.

## 4. Proposed solution

### 4.1 High-level overview

```
 partition (source of a repartition)                       admin DO (one for each shard group)
 ──────────────────────────────────                       ─────────────────────────────
 completed ──► outbox row ──► admin_outbox job ──RPC──► registerRepartition(event)
                               (retry with backoff)    FokosTopologyKeeper: facts in SQLite,
                                                       (epoch, version), batched build of the bodies
                                                                      ▲
                                                                      │ RPC on a cache miss only
                                                       topology entrypoint (WorkerEntrypoint, cache on)
                                                       GET /internal/v1/topology.Get?sg=...
                                                       GET /internal/v1/topology.GetForRange?sg=...&hk=...
                                                                      ▲
                                                      Workers Cache: lower tier ─► upper tier, request
                                                      collapsing per cache key and data center
                                                                      ▲ fetch() through ctx.exports
                     ┌────────────────────────────────────────────────┴───────────────┐
              Worker isolate                                                 Durable Object isolate
              FokosShardingClient                                  FokosShardingRuntime / FokosShardingClient
              topology cache (LRU, module scope)                   topology cache (LRU, module scope)
              resolve(): bitmap walk ──► deepest known partition ──► send; fall back on a miss
```

The flow has three parts.

1. **Report.** When a repartition of a partition reaches `completed`, the runtime writes an outbox row in the same
   transaction. A runtime job sends the row to the host hook `adminOutbox.send`, and the host calls
   `registerRepartition` on the admin DO of its shard group over Durable Object RPC. The job retries until the
   hook succeeds. The keeper is idempotent by the repartition `id`, and it accepts reports in any order. Some
   seconds after a report, the keeper builds the encoded bodies again and stores them, so a read never encodes.
2. **Serve.** The host exports a topology entrypoint with Workers Cache enabled. Its `fetch()` calls the admin DO
   over RPC and answers the encoded topology with `Cache-Control: public, max-age=<M>, stale-while-revalidate=<S>`.
   Workers Cache never caches a Durable Object call directly, so this entrypoint is necessary. Workers Cache
   collapses concurrent misses of one cache key in one data center into one call, and the upper tier collapses
   them again. So the admin DO receives about one read for each upper tier in each `M` seconds.
3. **Use.** `FokosShardingClient` and the runtime read a module-scope topology cache in their isolate. `resolve`
   walks the hash split bitmap from the root with `hashChildIndex` and stops at the first partition that has not
   split. When the cache is empty or old, `resolve` starts a fetch through the `waitUntil` of the caller and
   returns the best answer that it has now. A miss at a deep entry falls back to a shallower entry, as the local
   caches do today.

The shared read `topology.Get` returns the same bytes for every key of a shard group, so all callers share one
cache entry. A read that depends on the hash key (`topology.GetForRange`) has its own cache entry for each key.

### 4.2 Technical details

#### 4.2.1 Placement

| Part | Module (under `src/`) | Entry |
| --- | --- | --- |
| Event type, codec, decoder | `sharding/topology-codec.ts` | `fokosdb/sharding/client` |
| Topology cache | `sharding/topology-cache.ts` | `fokosdb/sharding/client` |
| Keeper, `FokosShardingAutoAdminDO`, entrypoint helper | `sharding/topology-keeper.ts` | `fokosdb/sharding/server` |
| Admin outbox, `admin_outbox` job | `sharding/sharding-store.ts`, `sharding/runtime.ts` | `fokosdb/sharding/server` |
| `BloomFilter` and `layerHas` | `shared/bloom-filter.ts` | both sharding entries |
| `FokosDBAutoAdminDO` | `server/do-auto-admin.ts` | `fokosdb/server` |
| `FokosTopologyCache` entrypoint | `server/topology-entrypoint.ts` | `fokosdb/server` |

The client entry must not reach `topology-keeper.ts`. The build check of the client entry enforces it. The
bundler puts all of `shared/bloom-filter.ts` in a chunk that the server entries share with the client entries, so
the sharding client bundle also holds the `BloomFilter` class, of which it uses only `layerHas`. Its budget is
42 kB for this reason. The
topology cache module must be in the shared chunk with `router.ts`, so that each isolate has one instance (Section
4.3.1 of `docs/agent-plans/2026-09-26-fokos-sharding-client.md`).

#### 4.2.2 The report event

```ts
export type FokosTopologyEvent = {
	schema: 1;
	/** The repartition id. The keeper is idempotent by (shardGroup, id). */
	id: string;
	kind: RepartitionKind; // "hash_split" | "range_split" | "key_promotion"
	shardGroup: string;
	/** `source.doName` is the name of the source object. */
	source: FokosPartitionRef;
	/** `ctx.id.toString()` of the source. */
	actorId: string;
	/** `actorId` is `idFromName(ref.doName).toString()`, which the source computes with no RPC. */
	targets: Array<{ ref: FokosPartitionRef; slice: FokosSlice; actorId: string }>;
	/** `Date.now()` of the source when it wrote `completed`. */
	completedAt: number;
};
```

The runtime builds the event from the repartition row and its targets, which `FokosRepartitionPlan` already holds.
The event has no `actorName`, because `source.doName` already holds it. In JSON, each key byte array is a base64url
string.

#### 4.2.3 The report path (M1)

**The hook.** `FokosRuntimeOptions` gets one optional field:

```ts
/** Host code. The outbox, or undefined when it is off. The runtime calls it at each use. */
adminOutbox?(this: void): FokosAdminOutbox | undefined;

type FokosAdminOutbox = {
	/** Host code. Sends one entry to an admin DO, chosen from `eventType`. A throw means "retry later". */
	send(this: void, entry: { eventType: string; id: string; event: unknown }): Promise<void>;
};
```

The runtime calls the function only after the partition has its identity, so the answer can depend on the policy
of the partition. FokosDB uses this: the `nsAdmin` table option travels in the policy (Section 4.2.8). Absent, or
an answer of undefined: the runtime writes no outbox row, and the outbox job deletes the rows that remain.

**The outbox.** A new table for all the actions that a partition sends to the admin DO. This RFC adds one event
type. Later features, for example the GSIs, add their own types to the same table and the same job.

```sql
CREATE TABLE fokos_admin_outbox (
	event_type TEXT NOT NULL, -- "topology.repartition_completed"
	id TEXT NOT NULL,         -- unique for each event type: here the repartition id
	event BLOB NOT NULL,      -- jsonb() of the event
	attempt INTEGER NOT NULL DEFAULT 0,
	next_at INTEGER NOT NULL,
	PRIMARY KEY (event_type, id)
);
CREATE INDEX idx_fokos_admin_outbox_next_at ON fokos_admin_outbox (next_at);
```

In `RepartitionFlow.acceptAck`, the transaction that writes `completed` also inserts the row of type
`topology.repartition_completed`, when the host has an outbox hook. The outbox keeps its own copy of the event,
because `source_cleanup` deletes the plan chain before it writes `cleaned`. The job reads `event` with `json()`
and does not look inside it.

The existing hook `beforeComplete` cannot send the report. It is synchronous, it runs inside the transaction, and
it runs before the state is `completed`.

**The job.** A new built-in job `admin_outbox`, next to `source_repartition` and `source_cleanup` in
`BUILTIN_JOBS`:

1. The `deadline()` of the job is the lowest `next_at` of the outbox.
2. The step takes the due rows in `next_at` order, at most 16 in each step, and calls `send` for each one.
3. When `send` resolves, the step deletes the row.
4. When `send` throws, the step increments `attempt` and sets `next_at` to now plus an exponential backoff from
   1 s to 5 min.
5. When the partition is `destroying`, the job does nothing.
6. When the host removes the outbox hook, the next step deletes all outbox rows.

The job does not block cleanup, the next repartition, or any request. A report that fails makes the topology
older, and Requirement 2.3.1 makes that safe.

**The keeper.** `FokosTopologyKeeper` takes `SqlStorage` and owns these tables. The prefix `fokos_topo_` keeps
them apart from the tables of the host.

- `fokos_topo_groups`, key `shard_group`: `root_trees_n`, `hash_split_n`, `epoch`, `version`, `hash_version`,
  `reset_at`, `built_version`, `build_due_at`, `promoted_n`, `hash_nodes_n`.
- `fokos_topo_repartitions`, key `(shard_group, id)`: `kind`, `repartition_key`, `data` (`jsonb()` of the event),
  `actor_id`, `completed_at`, `received_at`, `seq`.
- `fokos_topo_hash_nodes`, key `(shard_group, node)`, `WITHOUT ROWID`: one row for each hash partition that split,
  and `seq`. The index `idx_fokos_topo_hash_nodes_seq` is on `(shard_group, seq)`.
- `fokos_topo_range_intervals`, key `(shard_group, hash_key, start, end_is_max DESC, end DESC)`: the known range
  intervals of each promoted hash key, in the order of the range build (Section 4.2.9), and `seq`. The partial index
  `idx_fokos_topo_range_roots` is on `(shard_group, seq)`, for the range roots only.
- `fokos_topo_range_dirty`, key `(shard_group, hash_key)`, `WITHOUT ROWID`: the promoted hash keys that need a new
  range body.
- `fokos_topo_bodies`, key `(shard_group, kind, hash_key, chunk)`: `bytes`. `kind` is `hash` or `range`.

`fokos_topo_repartitions` has one row for each report, of every kind. It makes the keeper idempotent by the report
`id`. `repartition_key` is the source partition ID for `hash_split` and `key_promotion`, and the hash key for
`range_split`. `seq` is the `version` of the shard group after the report. No build reads this table: registration
writes the facts into the other tables.

**Versions.** `version` is a sequence number for each shard group. It increases by 1 only when a report writes a
new fact: a new row in `fokos_topo_hash_nodes` or in `fokos_topo_range_intervals`. A report that writes no new fact
is stored, but does not change `version` and does not start a build. Examples are a `hash_split` whose node is
already known as the ancestor of another split, and a `key_promotion` whose range root a `range_split` already
inserted.

- `hash_version` is the `version` of the last new hash node or new range root. These are the facts of the hash
  body: the nodes give the bitmap, and the range roots give the promoted keys of the Bloom filter.
- `built_version` is the `hash_version` of the stored hash body. The hash body is built again only when
  `hash_version > built_version`. A `range_split` of a known promoted key does not change the hash body.
- The `seq` of a row in `fokos_topo_hash_nodes` or in `fokos_topo_range_intervals` is the `version` of the report
  that inserted the row. A row with `seq > built_version` is a fact that the stored hash body does not have.
- A hash key is in `fokos_topo_range_dirty` only when a report wrote a new interval of that key. Only those keys
  get a new range body. A `hash_split` makes no key dirty.

`registerRepartition(event)` runs in one `transactionSync`:

1. When `(shardGroup, id)` is in `fokos_topo_repartitions`, return. The report is a repeat.
2. When `event.completedAt < reset_at`, return. The report comes from a table that `destroy` removed. The check
   compares two clocks, so it is best effort (Section 6).
3. For a `hash_split`, insert the node of the source and the node of each ancestor with `INSERT OR IGNORE`. The
   walk goes from the source to the root and stops at the first node that is known, because a known node has all
   its ancestors.
4. For a `range_split` or a `key_promotion`, insert the range root, the source interval, and each target interval
   into `fokos_topo_range_intervals` with `INSERT OR IGNORE` (Section 4.2.9). When an interval is new, insert the
   hash key into `fokos_topo_range_dirty`.
5. Insert the report row. When step 3 or 4 wrote a new row, increment `version`, set `hash_version` to `version`
   when a hash node or a range root is new, and set `build_due_at` to `now + buildDelayMs` when it is NULL. A later
   report does not move `build_due_at`, so one build takes all the reports of the delay.
6. When `build_due_at` is set, call the host hook `scheduleBuild(build_due_at)`.

Registration derives the facts, so the order of arrival does not matter:

- `hash_split`: the source partition and each hash ancestor of it have split. A hash partition ID holds the root
  index and the whole child path (`PartitionIdHelper`), so each prefix of the path is an ancestor. A split is the
  only way that a child exists, so each ancestor has split. The `node` key is `[depth, rootHi, rootLo, ...path]`.
  The byte order of this key is the level order of the bitmap (Section 4.2.4), so the primary key gives the order
  of the build and SQLite does no sort.
- `key_promotion`: the hash key of each `promoted_key` target slice is promoted.
- `range_split`: the interval of the source and the interval of each target exist. The report carries no depth: a
  range `FokosSlice` holds only `hashKey`, `start`, and `end`. The build computes a rank for each interval from the
  nesting of the known intervals (Section 4.2.9), so a child that arrives before its parent is correct.

`promoted_n` and `hash_nodes_n` count the range roots and the hash nodes, so a build can size the Bloom filter and
the bitmap before it reads the rows.

The first event of a shard group also inserts its `fokos_topo_groups` row. The keeper reads `rootTreesN` and
`hashSplitN` from the decoded source ID and the target count. When an event has other values than the stored row,
the keeper refuses it with a validation error, because these values never change (AGENTS.md).

`resetShardGroup(shardGroup)` deletes the rows and the bodies of the shard group, increments `epoch`, sets
`version`, `hash_version`, `built_version`, `promoted_n`, and `hash_nodes_n` to 0, and sets `reset_at` to now.

The admin DO receives the report over Durable Object RPC, not over the cached entrypoint. Workers Cache caches only
`GET` and `HEAD`, so a `POST` through the entrypoint gains nothing and adds one hop.

**The default class.** `FokosShardingAutoAdminDO` extends `DurableObject`, embeds one keeper, and exposes the
keeper methods as RPC methods. Its constructor calls `keeper.runMigrations()` in `ctx.blockConcurrencyWhile()`. Its
`scheduleBuild` sets the alarm when the alarm is later or absent. Its `alarm()` calls `keeper.runDueBuilds(now)`
and sets the next alarm to `max(keeper.nextBuildAt(), now + 1)`, so a build that is not complete continues in the
next alarm. A host that has its own admin DO embeds `FokosTopologyKeeper` instead. It calls `runMigrations()` once
before any other method, and calls `runDueBuilds()` and `nextBuildAt()` from its own alarm.

**One admin DO for each shard group.** The library gives `topologyAdminName(shardGroup)`, which returns
`fokos.topo.<shardGroup>`. The report hook, the entrypoint, and `destroy` know the shard group, so they get the name
with no lookup. One admin DO then holds the rows and the bodies of one shard group only:

- The reads, the builds, and the storage of one shard group do not go to the admin DO of another. This is
  important for FokosDB: the base table and each GSI have their own promoted keys, so the range topology of each
  of these shard groups can grow without a limit.
- Each shard group gets the limits of one Durable Object for itself: a soft limit of 1,000 requests each second,
  and 10 GB of storage ([limits](https://developers.cloudflare.com/durable-objects/platform/limits/)).
- A build of one shard group does not delay the reads of another.

The keeper tables keep the `shard_group` column, so a host can put more than one shard group in one DO. This RFC
does not do that. The range reads of many hot promoted keys of one shard group all go to one admin DO. Section 2.4
(Follow-up 6) lists the options for that.

#### 4.2.4 The encoding (M2)

**The hash split bitmap.** One bit for each hash partition of the shard group, in level order:

- Positions `0 .. R-1` are the roots, with `R = rootTreesN`, in root index order.
- When bit `i` is 1, the partition at `i` has split. Its `K = hashSplitN` children are at positions
  `R + K * rank1(i) + c`, for child index `c` in `0 .. K-1`. `rank1(i)` is the number of 1 bits before `i`.
- The bitmap has `R + K * S` bits for `S` splits.

This is LOUDS for a tree whose nodes have 0 or `K` children. LOUDS stores the degree of each node, but the degree
here is always 0 or `K`, so one bit is enough: `N` bits for `N` partitions, against `2N + 1` bits for LOUDS.

The keeper writes a rank index into the body, in the section `rank`: one `u32` for each 512-bit block, with the
count of 1 bits before the block, and one more `u32` with the total count. `rank1(i)` is the block count plus a
popcount of at most 16 words. The rank index adds 1/16 to the bitmap bytes. The caller makes views over the
bitmap, the rank index, and the Bloom bits, and makes no allocation that grows with the topology.

The build encodes the bitmap from `fokos_topo_hash_nodes` in primary-key order. In level order, `rank1` of a split
partition is its index in that order, and the parents come in the order of their children. So the encoder reads two
cursors over the same rows at the same time: one for the nodes, and one that moves forward to the parent of each
node. The position of a node is `R + K * (index of its parent) + c`. The encoder holds no node after it moves past
it, and only the bitmap grows with the topology. Only the first build of an epoch uses this full encoder.

**The merge.** When a hash body is stored, the build does not read all the hash nodes. It reads only the rows with
`seq > built_version` (the pending splits), in `node` order, and merges them into the stored bitmap
(`mergeHashBitmap`):

- The merge walks the new bitmap in level order with a queue. An entry of the queue is a run of old positions, or
  one new node under a leaf of the old bitmap.
- The old nodes keep their order, and the children of the old splits come in the order of their parents. So the
  children of a run are the next run of old positions, and they are one entry. The merge needs no `rank1` of the
  old bitmap for them.
- The merge copies a run with a shifted copy of 32 bits at a time, and counts the 1 bits of the run in the same
  loop. It visits one at a time only a pending split and each node under it.
- A walk of the old bitmap gives the old position of the next pending split, or shows that the split is under an
  old leaf. The merge stops a run at that position, marks the split, and queues its `K` children as new nodes.
- The parent of each pending split is an old split or a pending split, because registration inserts the
  ancestors. A pending split is never an old split, because its row was a new fact. The merge refuses a pending
  split that it cannot place, and a count that does not agree with `hash_nodes_n`.

The cost is about `N / 32` word operations for `N` partitions, plus a walk and `K` queue entries for each pending
split. The memory is the new bitmap and the queue, which grows with the pending splits only. The result is the same
bitmap as the full encoder gives.

The Bloom filter of the promoted keys works in the same way. When the stored filter has the capacity of the new
count, the build copies its bits and adds only the range roots with `seq > built_version`. A Bloom filter is the OR
of the bits of its keys, so the bits are the same as a full build gives. When the count needs a larger capacity, the
build reads all the range roots again. The capacity doubles each time, so this occurs rarely.

```ts
// Returns the bitmap position and the absolute depth of the deepest known hash partition of hashKey.
function findLeaf(hashKey: KeyBytes, cut: ReadonlySet<number>): { pos: number; depth: number } {
	let pos = hashRootIndex(hashKey, R);
	let depth = 0;
	while (bit(pos)) {
		const next = R + K * rank1(pos) + hashChildIndex(hashKey, depth, K);
		if (cut.has(next)) break; // Section 4.2.6
		pos = next;
		depth += 1;
	}
	return { pos, depth };
}
```

The caller builds the context with `resolveDescendantHashPartitionContext` from the root and the child path of the
walk.

**The promoted-key Bloom filter.** The build makes a `BloomFilter` with one layer from the promoted keys:

- `errorRate` 0.005, `initialCapacityN` the smallest power of two of at least 16,384 and at least the number of
  promoted keys, and `maxSizeBytes` the bytes of that one layer.
- When a promotion makes the count larger than the capacity, the keeper builds the filter again at twice the
  capacity. It never adds a layer, because it has the exact set of keys.

Sizes from `bloomInitialLayerBytes(0.005, n)`:

| Capacity | Bytes |
| --- | --- |
| 16,384 | 25,540 |
| 32,768 | 51,079 |
| 131,072 | 204,316 |
| 1,048,576 | 1,634,527 |

**The body framing.** The hash body and the range body (Section 4.2.9) use the same framing. All numbers are
little endian.

```
magic (4 bytes) | u32 headerLen | header JSON (UTF-8, headerLen bytes) | padding to 8 | sections
```

- The header is a JSON object. It holds `schema`, `epoch`, `version` (a decimal string, because a `u64` does not
  fit in a JSON number), the fields of the body kind, and `sections`. Each section has an `offset` from the start
  of the body, and a `count` or a `length`.
- Each section starts at an offset that is a multiple of 8. The caller makes the typed-array views with no copy.
- The caller parses the header once for each fetch, and stores the views in the cache entry. A request reads only
  the views. A header of a few hundred bytes costs one `TextDecoder` call and one `JSON.parse` for each refresh,
  about one in each `refreshMs`. Cost: `TODO: measure`.
- The reader ignores a header key that it does not know. A new optional field or a new section is a new key, and
  it needs no new `schema`.
- The reader refuses a body with another magic, another `schema`, a section outside the body, or a section offset
  that is not a multiple of 8.
- Only a change that an old reader cannot read safely increments `schema`. Such a change also uses a new path
  segment (`v2`), because of `cross_version_cache` (Section 4.2.5).

**The hash body.** Magic `"FKT1"`. Example header:

```json
{ "schema": 1, "epoch": 3, "version": "1842", "rootTreesN": 16, "hashSplitN": 4,
  "sections": {
    "bitmap": { "offset": 256, "bitCount": 80 },
    "rank": { "offset": 272, "count": 2 },
    "bloom": { "offset": 280, "length": 25540, "errorRate": 0.005, "k": 8, "m": 204320,
               "capacity": 16384, "count": 0 } } }
```

The bitmap is padded to a multiple of 8 bytes. The caller makes a `Uint32Array` over the bitmap, a `Uint32Array`
over the rank index, and a view over the Bloom bits. The reader refuses a `rank` section whose `count` is not
`ceil(words / 16) + 1`, and a body whose `bitCount` is not `rootTreesN + hashSplitN * total`. The caller drops a body
whose `rootTreesN` or `hashSplitN` differs from its own `FokosTopology`, and logs one warning.

Size at 1,000,000 partitions and 1,000,000 promoted keys: 125,000 bytes of bitmap, about 7.8 KB of rank index,
1,634,527 bytes of Bloom filter, about 1.77 MB in total. At 1,000 partitions and no promotions the body
is about 25.6 KB, almost all of it the empty Bloom filter.

**The build.** A read never encodes. `runDueBuilds(now)` does one bounded step of each shard group whose
`build_due_at` is due, in `build_due_at` order, until it read `buildStepRows` rows (an option, default 100,000). One
step of a shard group:

1. When `hash_version > built_version`, it encodes the hash body. With a stored hash body, it merges the pending
   splits and the new range roots into it. Without one, it reads all of `fokos_topo_hash_nodes` and all the range
   roots. In one `transactionSync`, it replaces the stored hash body and sets `built_version` to `hash_version`.
   After a crash before this transaction, the next step merges the same rows again.
2. It builds the range body of each key in `fokos_topo_range_dirty` (Section 4.2.9). Each key has its own
   `transactionSync`: it reads the intervals, encodes the body, replaces the stored body, and deletes the dirty
   row. Before each key, when the step read `buildStepRows` rows, it sets `build_due_at` to `now` and stops.
3. When no key is dirty, it sets `build_due_at` to NULL.
4. It keeps the hash body of each shard group in memory, so a read answers with no SQL.

The two kinds of body do not wait for each other. The smallest unit of a step is one hash body or one range body,
so a step always does some work. The bound is in rows and not in time, because the clock of a Worker does not move
while the code runs with no I/O. A step does not yield, so no report arrives during a step. A report between two
steps writes its facts, and the next step builds what the report changed. A stop between two transactions leaves
the dirty rows or `build_due_at`, and the next alarm continues the build.

While the range bodies build, a new promoted key can be in the Bloom filter of the hash body before its range body
exists. The caller then gets the empty range answer (Section 4.2.9) and enters at the range root. The cost is extra
hops for some seconds.

A body is stored in chunks of `TOPOLOGY_BODY_CHUNK_BYTES` (1 MiB), because the documented limit of a `BLOB` or a
row is 2 MB ([Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)). Newer
workerd sets `MAX_ROW_LENGTH` to 8 MiB + 34 bytes (`src/workerd/util/sqlite.h`), and the page does not show it yet.
After the update of Wrangler, a larger constant gives fewer chunks, and one chunk for most bodies. The read and
the write of chunks stay the same. After an eviction, the first read loads the chunks of the hash body again and
does not encode. Before the first build, a read answers an empty topology of version 0.

`buildDelayMs` is an option of the keeper, default 2,000. A shorter delay gives fresher bodies; a longer delay
puts more reports in one build. A hash body build is one unit of a step: `buildStepRows` does not divide it. The
first build of an epoch reads each hash node twice and each promoted key once. A later build reads the pending rows
and the stored body.

Cost in workerd (`test/topology-bench/hash-build.workerd-bench.ts`): `R` 16, `K` 2, a balanced tree, one promoted key
for each 100 partitions, and the stored body in memory. The timers of workerd advance in whole milliseconds.
"Before" is the full build of every hash body, and "after" is the merge.

| Partitions | Hash nodes | Hash body | First build | +1 split, before | +1 split, after | +100 splits, before | +100 splits, after |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 42 | 25 KiB | 2–4 ms | 1 ms | 2 ms | 2 ms | 1 ms |
| 1,000 | 492 | 25 KiB | 2 ms | 2 ms | 1 ms | 2 ms | 2 ms |
| 10,000 | 4,992 | 27 KiB | 19–21 ms | 25 ms | 1 ms | 22 ms | 3 ms |
| 100,000 | 49,992 | 38 KiB | 126–135 ms | 130 ms | 1 ms | 144 ms | 2 ms |
| 1,000,000 | 499,992 | 155 KiB | 1.1 s | 1.1 s | 2 ms | 1.5 s | 2 ms |
| 10,000,000 | 4,999,992 | 1.5 MiB | 11.6 s | 10.6 s | 24 ms | 14.0 s | 16 ms |

The first build of an epoch stays one step: about 1.1 s at 1,000,000 partitions and 11.6 s at 10,000,000. A keeper
that registers a large topology after a reset therefore has one long step.

#### 4.2.5 The read path (M2)

**The admin DO** exposes `getTopology(shardGroup, ifNoneMatch?)`. It answers the last built body:
`{ status: 200, etag, body }`, or `{ status: 304, etag }` when `ifNoneMatch` equals the current ETag. The ETag is
`"<epoch>.<built_version>"`, and the body carries the same pair. A report that does not change the hash body does
not change this ETag, so the callers do not fetch the body again. A shard
group that the keeper does not know answers 200 with version 0 and no splits.

**The topology entrypoint.** The library gives `fokosTopologyEntrypointFetch(request, opts)`. The host calls it
from the `fetch()` of its own named `WorkerEntrypoint`:

```ts
export class FokosTopologyCache extends WorkerEntrypoint<Env, TopologyAdminAddress> {
	fetch(request: Request): Promise<Response> {
		return fokosTopologyEntrypointFetch(request, {
			// Host code: the stub of `topologyAdminName(shardGroup)`. The props of the caller select the
			// admin binding and the jurisdiction of the table.
			admin: (shardGroup) => topologyAdminStub(this.env, this.ctx.props, shardGroup),
			maxAgeS: 10,
			staleWhileRevalidateS: 600,
		});
	}
}
```

| Request | Admin call | Response |
| --- | --- | --- |
| `GET /internal/v1/topology.Get?sg=<b64>` | `getTopology` | 200 or 304, cache headers |
| `GET /internal/v1/topology.GetForRange?sg=<b64>&hk=<b64>` | `getRangeTopology` (M5) | 200 or 304, cache headers |
| any other method or path | none | 404 or 405 with `Cache-Control: no-store` |
| admin RPC throws | — | 503 with `Cache-Control: no-store` |

The cache headers are `Cache-Control: public, max-age=<maxAgeS>, stale-while-revalidate=<staleWhileRevalidateS>`
and the `ETag`. The rules for these headers:

- The response must set `Cache-Control`. Without it, Cloudflare applies heuristic freshness, and a 200 can stay
  fresh for 7200 s
  ([default cache behavior](https://developers.cloudflare.com/cache/concepts/default-cache-behavior/)).
- The response must not use `s-maxage`, `must-revalidate`, or `proxy-revalidate`. Each one turns off
  `stale-while-revalidate` and `stale-if-error`
  ([configuration](https://developers.cloudflare.com/workers/cache/configuration/)).
- Without `stale-if-error`, Cloudflare serves the stale entry while the Worker fails. A stale topology is safe, so
  that default is correct here.
- `maxAgeS: 0` is allowed. Then every request revalidates against the admin DO, and request collapsing no longer
  protects it. Use it only for a small table or a test.

**The cache key.** Workers Cache keys an entry by the entrypoint, the path and query, `ctx.props`, and the Worker
version ([cache keys](https://developers.cloudflare.com/workers/cache/cache-keys/)). The host is not part of the
key. So:

- The path carries the shard group, which is unique for each table and each kind (`fokos.p.<table>`,
  `fokos.tc.<table>`). The `v1` segment is the schema of the body. A change of the body format changes the path.
- Each caller of one table passes the same `ctx.props`. FokosDB passes the admin address of the table,
  `{ nsAdmin, jurisdiction }`, so all the callers of one table share one entry for each path.
- The consumer sets `cache.cross_version_cache: true`. The Worker version then leaves the key, and a deploy keeps
  the warm cache. The `v1` path segment still separates two formats. `cross_version_cache` needs Wrangler 4.107.0
  or later, and applies to every entrypoint whose cache is on.

**The consumer configuration.**

- `cache` on for the topology entrypoint only, through the `exports` map (Wrangler 4.107.0 or later). The default
  entrypoint keeps the cache off, so the other requests do not pay a cache lookup.
- The `enable_ctx_exports` compatibility flag, so a Worker and a Durable Object can call the entrypoint through
  `ctx.exports.FokosTopologyCache`.
- No route to the topology entrypoint. Only `ctx.exports` and a service binding then reach it, so the `/internal/`
  paths are not public.

**Cost.** Each call to the entrypoint is a billed Worker request, a hit too. A hit uses no CPU time
([Workers Cache](https://developers.cloudflare.com/workers/cache/)). Each isolate fetches about once in each
`refreshMs` (Section 4.2.6), so the cost is small against the request traffic.

#### 4.2.6 The topology cache (M2)

**The options.** `FokosShardingClientOptions` gets one optional field. `FokosRuntimeOptions` gets the same
options as a function, `topologyCache?(this: void): FokosTopologyCacheOptions | undefined`, which the runtime calls
at each forward of a hash key, after the partition has its identity. The host returns the same object while the
options do not change.

```ts
topologyCache?: {
	/** Host code. Fetches one encoded topology, for example through `ctx.exports.FokosTopologyCache.fetch`. */
	fetch(this: void, path: string, signal: AbortSignal): Promise<Response>;
	/** The `waitUntil` of the request (Worker) or of the `DurableObjectState` (Durable Object). */
	waitUntil(this: void, promise: Promise<unknown>): void;
	/** The age of an entry that starts a refresh. Default 10,000. */
	refreshMs?: number;
	/** The time after which a refresh that did not end can start again. Default 30,000. */
	refreshTimeoutMs?: number;
	/** The time after which a refresh stops waiting for its fetch. Default 5,000. */
	fetchTimeoutMs?: number;
};
```

Absent: the client and the runtime behave as today.

In a Worker the host gives the `waitUntil` of the request: the `ExecutionContext`, or the `waitUntil` that
`cloudflare:workers` exports (Section 4.2.8). In a Durable Object the host gives `this.ctx`. `DurableObjectState`
has `waitUntil`, and a promise there prevents eviction until it settles or for 15 minutes
([Durable Object state](https://developers.cloudflare.com/durable-objects/api/state/)).

**The cache.** One module-scope `LRUCache` for the isolate holds the hash entries and the range entries
(Section 4.2.9). The key of a hash entry is `h|<shardGroup>|<rootTreesN>|<hashSplitN>`. The client and the runtime
of one isolate share it.

```ts
type TopologyEntry = {
	epoch: number;
	version: bigint;
	bitmap: Uint32Array;
	/** A view over the `rank` section of the body. */
	rank: Uint32Array;
	bloom: BloomFilter;
	fetchedAt: number;
	/** Positions where a miss stopped the walk. Bounded to 1,024. Section "The fallback". */
	cut: Set<number>;
};
```

**The size limit.** The cache counts bytes, not entries. `TOPOLOGY_CACHE_BUDGET_BYTES` is 16 MiB for the isolate.
A Worker isolate has 128 MB ([Workers limits](https://developers.cloudflare.com/workers/platform/limits/)).

- The size of an entry is the `byteLength` of the fetched body, plus 8 bytes for each position in `cut`. The rank
  index is a view over the body, so it adds nothing.
- `LRUCache` gets an optional weight: `new LRUCache(capacity, { maxWeight, weightOf })`. `set` puts the new entry
  last, and then removes the least recently used entries until the total weight is at most `maxWeight`. It never
  removes the new entry. A `get` makes an entry the most recent.
- The cache does not store an entry larger than the budget, and logs one warning.
- A change of `cut` changes the weight of the entry. The cache sets the entry again, which runs the same eviction.

At 1,000,000 partitions a hash entry is about 1.77 MB, so the budget holds about 9 of them. At 1,000 partitions an
entry is about 26 KB.

**The refresh.** The cache keeps no promise. It keeps one plain value for each key: `refreshStartedAt`.

1. `resolve` reads the entry. It does no I/O.
2. When the entry is absent, or `now - fetchedAt > refreshMs`, and no refresh started in the last
   `refreshTimeoutMs`, `resolve` sets `refreshStartedAt` and calls `waitUntil(refresh(key))`.
3. `refresh` calls `fetch` with `AbortSignal.timeout(fetchTimeoutMs)`, reads the header of the body, and checks it
   (Section 4.2.4). When the signal fires, `refresh` stops waiting also when the host fetch does not read the
   signal. Thus the promise in `waitUntil` settles in at most `fetchTimeoutMs`, and a fetch that does not end does
   not keep a Durable Object in memory for the 15 minutes that `waitUntil` allows.
4. When the result is valid and its topology version is equal to or higher than the cached one, `refresh` stores a
   new entry. A new entry has an empty `cut` set.
5. In all cases, `refresh` clears `refreshStartedAt`. A failure logs one warning and keeps the old entry.

Defaults: `refreshMs` 10,000, `refreshTimeoutMs` 30,000. A test sets them low, and does not need a mock clock. A
request never awaits `refresh`. The first request of an
isolate enters at the root.

Workers Cache already collapses the fetches of many isolates. `refreshStartedAt` only stops one isolate from
sending many fetches for one key at the same time.

**Resolution.** With an entry, `resolve(key)` returns the context of `findLeaf(key.hashKey, entry.cut)`. Without
one, it returns the root. `resolveAll` groups by that result. The transaction fast path checks
`resolveAll(keys).length === 1`. With deeper entries, two keys under one split root resolve to two leaves, and
the transaction goes directly to a coordinator. Today the same transaction goes to the root first: the fast path
sees one group, the partitions below the root do not own every item, the answer is `not_applicable`, and
`#writeSingleShotFastPath` in `db.ts` then uses the coordinator. The topology removes that round trip, and the
behavior stays correct when the topology is old.

A key with a Bloom hit uses the hash path in M2. The hash leaf of the key holds its route override, because a
hash split moves the overrides to the child, so the leaf forwards to the range tree.

**The fallback.** A miss at an entry that came from the topology follows the fallback test of Section 4.3.1 of
`docs/agent-plans/2026-09-26-fokos-sharding-client.md`:

- The client adds the bitmap position of the entry to `entry.cut`. This removes one hint and keeps the rest of the
  topology.
- The client sends again to the parent of the entry. The parent is the result of `findLeaf` with the new cut.
- The `cut` set lives until the next topology version replaces the entry. A cut that a later version also has
  returns at most one miss for each isolate in each version.

A miss can only come from a topology of an old epoch or a fault, because the keeper records only completed
repartitions.

#### 4.2.7 The runtime (M3)

In `#hashTopologyOwner`, the runtime computes the depth from the arena, as today, and also from the topology
cache:

- It calls `findLeaf` for the key. When the result depth is larger than the depth of the partition, the walk went
  through this partition, because the key belongs to this partition (`#ownsByTopology`).
- The target is the deeper of the two hints. When the two hints give the same depth, the resolution keeps both.
- A `hash_partition_not_initialized` miss at a target deeper than the child goes to `#missLadder`. It adds the
  bitmap position of a topology hint to the cut of Section 4.2.6, and it calls `HashTopology.invalidate` for the
  arena hint, as today. The retry then resolves again without the hint that missed.

The host gives `topologyCache` in `FokosRuntimeOptions`, with the `waitUntil` of its `DurableObjectState`. A
partition then uses the topology also for keys that its own forwards never showed. This applies to the `point`,
`group`, and `single_owner` shapes. The range frontier (`#planRange`) keeps the arena hint only, because a visit
has no fallback after a hash miss.

#### 4.2.8 FokosDB (M4)

- `FokosDBAutoAdminDO` embeds one `FokosTopologyKeeper`. The class has two kinds of instances, on the same
  `nsAdmin` binding. The name selects the role:
  - `fokos.topo.<shardGroup>`: the topology of one shard group. A table has one for `fokos.p.<tableName>` and one
    for `fokos.tc.<tableName>` now, and later one for `fokos.m.<tableName>` and one for each GSI shard group. These
    shard groups get their route context from the base table
    (`docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md`), so they all have the same `nsAdmin`.
  - `fokos.admin.<tableName>`: the FokosDB administration of the table, that is the GSI catalog, `targets()`, and
    the activation status (Section 4.3.2 of the same document). It holds no topology. The GSI work adds its own
    state and event types to this instance.

  One class and one binding are enough, so the consumer configuration does not change for a new shard group.
- `PartitionDO` and `TransactionCoordinatorDO` set `adminOutbox.send`. The host chooses the admin DO from
  `eventType`: a `topology.repartition_completed` entry goes to the admin DO of the shard group of the partition
  (`topologyAdminName`). A later event type can go to `fokos.admin.<tableName>`. A coordinator splits by hash, so
  it reports `hash_split` events of its own shard group.
- `FokosTableOptions` gets two optional fields. They travel in `PartitionContext.policy`, as `ns` and `nsTx` do, so
  partitions and coordinators also have them:
  - `nsAdmin`: the binding key of the `FokosDBAutoAdminDO` namespace, for example `"FOKOS_ADMIN_DO"`.
    `shared/do-stubs.ts` resolves it with the jurisdiction of the table. Absent: no host sets `adminOutbox`, and
    no partition writes an outbox row.
  - `topologyEntrypoint`: the export name of the cached entrypoint, for example `"FokosTopologyCache"`. Absent: no
    host sets `topologyCache`.
- `FokosDBOptions` gets `topologyRefreshMs?` and `topologyRefreshTimeoutMs?`, which go to `refreshMs` and
  `refreshTimeoutMs` of its `topologyCache`. `PartitionDO.fokosConfig()` and the configuration of
  `TransactionCoordinatorDO` get the same two settings for the runtime (`topologyRefreshMs` and
  `topologyRefreshTimeoutMs`, defaults 10,000 and 30,000).
- `createTableConfig` refuses a `topologyEntrypoint` without `nsAdmin`.
- `FokosDBOptions` gets `useTopologyCache?: boolean`, default `true`. When it is `false`, the clients of that
  `FokosDB` get no `topologyCache`: they send no topology fetch, do not read the isolate cache, and enter at the
  root, as today. Tests use it, and so does a consumer that prefers no extra requests to fewer hops. It does not
  change the partitions and coordinators, which follow `topologyEntrypoint`.
- `FokosDB` builds `topologyCache` itself (`shared/topology-hooks.ts`), from `exports[topologyEntrypoint]` and
  `waitUntil`, both imported from `cloudflare:workers`. The fetch calls
  `exports[topologyEntrypoint]({ props: { nsAdmin, jurisdiction } }).fetch(...)`. The caller does not give an
  `ExecutionContext`. It sends the cache to both of its `FokosShardingClient` instances. `PartitionDO` and
  `TransactionCoordinatorDO` give it to their runtimes, with `this.ctx.waitUntil`.
- The partition client of the coordinator has no topology cache. The coordinator stores the address of each
  participant in `tc_participants`, and prepare, commit, cancel, and recovery send to it from other client
  instances, also after a restart. Only the client that resolved an address from the topology has its fallback, so
  a stored address from an old hint could cancel a valid transaction. A root address is always correct, and the
  root forwards with its own topology cache (M3). A later change can use the topology here when the fallback works
  across client instances and stored addresses, and when the groups that fall back to one owner join again, so
  that each owner gets its keys in one `txCommit`. In the vitest pool, the imported `exports` (with props) and the imported `waitUntil` work
  in a Worker and in a Durable Object. `ctx.exports` needs the `enable_ctx_exports` compatibility flag, which is the default from the compatibility date 2025-11-17.
- `destroy` calls `resetShardGroup` on the admin DO of each shard group of the table after the sharding destroy
  ends.
- `fokosdb/server` exports `FokosDBAutoAdminDO` and `FokosTopologyCache` (a `WorkerEntrypoint`). The consumer
  binds the first, and re-exports the second from its main module and enables its cache in the `exports` map.
  `FokosDBAutoAdminDO` is `FokosShardingAutoAdminDO` with no change. The `fokos.admin.<tableName>` role adds its
  own state later.
- The admin DOs use the jurisdiction of the table, through a helper in `shared/do-stubs.ts`, as the other classes
  do.

#### 4.2.9 The range topology (M5)

`getRangeTopology(shardGroup, hashKey)` answers the stored range body of the key. The ETag and the body carry the
`version` of the shard group when the step built the body. A key gets a new body only after a new interval, so the
version of its bodies always increases. A key with no stored body (a key that is not promoted, or a promoted key
whose first body is not built yet) answers an empty body of version 0, with no boundaries and no segments. Thus
the first stored body of a key always has another ETag, and a revalidation of the empty answer does not get a 304.
An empty answer is also cached: when it is old, the cost is extra hops.

**The model.** One object for each promoted hash key:

```ts
type RangeTopology = {
	/** The inner boundaries, distinct, in `KeyCodec.compare` order. `~min` and `~max` are implicit. */
	boundaries: KeyBytes[];
	/** n values. The rank of the deepest known interval that has boundary j strictly inside it. */
	boundaryLevel: Uint16Array | Uint32Array;
	/** n + 1 values. The rank of the deepest known interval that holds segment i. */
	segmentDepth: Uint16Array | Uint32Array;
};
```

- A position is `0` for `~min`, `1 .. n` for the inner boundaries, and `n + 1` for `~max`. The level of `~min` and
  of `~max` is -1.
- Segment `i` goes from position `i` to position `i + 1`. So `n` boundaries make `n + 1` segments.
- The rank of an interval is its depth among the known intervals: the range root has rank 0, and each known child
  has the rank of its nearest known ancestor plus 1. The build computes it. When a report is missing, the rank can
  be smaller than the real depth. The rank is never part of a DO name.
- The model has no record for each interval. The interval rule gives each known interval from the levels.

**The interval rule.** For a threshold `t` and the segments `i .. j`:

- the start is the nearest position at or before position `i` with a level less than `t`;
- the end is the nearest position at or after position `j + 1` with a level less than `t`.

The rule gives the deepest known interval that holds the segments, when `t` is the value of the lookup below.

**The example.** Hash key `u42`. The range root R `[~min, ~max)` split into A `[~min, g)`, B `[g, p)`, and
C `[p, ~max)`. Then B split into B1 `[g, k)` and B2 `[k, p)`.

```
position   0:~min    1:g    2:k    3:p    4:~max
level        -1       0      1      0      -1
segment          S0      S1     S2     S3
depth            1       2      2      1
```

`g` and `p` are strictly inside R only, so their level is 0. `k` is strictly inside B, so its level is 1. S0 is in
A, S1 in B1, S2 in B2, and S3 in C.

**A point lookup** for the sort key `s`:

1. `i` is the number of boundaries that are less than or equal to `s`. A binary search finds it.
2. `t` is `segmentDepth[i]`.
3. The interval rule for segment `i` with threshold `t` gives the target.

Example, `s = "h"`: `i` is 1 (S1), and `t` is 2. Before position 1, `g` has level 0, which is less than 2, so the
start is `g`. After position 2, `k` has level 1, which is less than 2, so the end is `k`. The caller sends to
`~r.u42.g.k`. For a leaf, both scans stop at the first position.

**A range lookup** for `[x, y)` follows the range entry rule of Section 4.3.1 of
`docs/agent-plans/2026-09-26-fokos-sharding-client.md`: the request enters at the deepest known partition that holds
the whole requested interval.

1. `i` is the number of boundaries that are less than or equal to `x`. `j` is the number of boundaries that are
   less than `y`. For an inclusive end, `j` is the number of boundaries that are less than or equal to `y`.
2. `D` is the smallest value of `segmentDepth[i .. j]` and of the levels of the positions `i + 1 .. j`.
3. The interval rule for the segments `i .. j` with threshold `D` gives the target.

Examples:

- `["h", "m")`: `i` is 1 and `j` is 2. `D` is the smallest of 2, 2, and the level of `k` (1), so `D` is 1. Before
  S1, `g` has level 0, so the start is `g`. After S2, `p` has level 0, so the end is `p`. The caller sends to
  `~r.u42.g.p`, and B plans the visits to B1 and B2 with `rangeVisits`.
- `["a", "h")`: `i` is 0 and `j` is 1. `D` is the smallest of 1, 2, and the level of `g` (0), so `D` is 0. The start
  is `~min`. After S1, `k` (1) and `p` (0) are not less than 0, and `~max` (-1) is. The caller sends to R.

The caller maps each sort-key condition to the interval that `rangeVisits` uses. `beginsWith(p)` is
`[p, prefixUpperBound(p))` of `KeyCodec`. A query with no sort-key condition goes to the range root. When `j` is less
than `i`, the interval is empty, and the caller uses the point lookup of `x`.

A lookup compares the bytes in place, through the boundary offsets, and makes no object for a boundary. The only new
object is the DO name string, which each send makes.

**A missing report.** When only the split of B is known, A and C are unknown. The levels stay `[0, 1, 0]`, and
`segmentDepth` is `[0, 2, 2, 0]`. A point lookup of `"a"` gives S0 and `t` 0. The scans find only `~min` and `~max`,
so the caller sends to R, and R forwards. A target is always a known interval.

**The fallback.** A miss at a target of rank `d` follows the hash rules of Section 4.2.6:

- The caller adds the start and end positions of the target to the `cut` set of the range entry.
- The caller applies the interval rule again with threshold `d - 1`. The rank has no gaps, so the result is the
  nearest known ancestor.
- The `cut` set lives until the next topology version replaces the entry.

**The body.** The framing of Section 4.2.4, with the magic `"FKR1"`. Example header for the example above:

```json
{ "schema": 1, "epoch": 3, "version": "1842", "rankBytes": 2, "maxRank": 2, "truncatedAtRank": null,
  "sections": {
    "boundaryOffsets": { "offset": 168, "count": 4 },
    "boundaryBytes": { "offset": 184, "length": 3 },
    "boundaryLevel": { "offset": 192, "count": 3 },
    "segmentDepth": { "offset": 200, "count": 4 } } }
```

- `boundaryOffsets` has `n + 1` `u32` values. Boundary `j` is the bytes `offsets[j] .. offsets[j + 1]` of
  `boundaryBytes`, in the `KeyCodec` encoding. The caller makes the DO name with `encodeRangeComponent` when it
  sends.
- `boundaryLevel` and `segmentDepth` hold unsigned integers of `rankBytes` bytes. The build uses 2 when `maxRank` is
  less than 65,535, and 4 in the other case. One byte is not enough: when the sort keys increase, for example
  timestamps, each split divides the last leaf, and the rank grows by 1 for each split.
- `truncatedAtRank` is the rank of the size cap (below), or `null`.

Size: 1,000 leaves with `rangeSplitN` 2 and 32-byte keys need about 32 KB of boundaries, 4 KB of offsets, and 4 KB
of ranks, so about 40 KB.

**The interval table.** `fokos_topo_range_intervals` holds the known intervals of each promoted hash key:

```sql
CREATE TABLE fokos_topo_range_intervals (
	shard_group TEXT NOT NULL,
	hash_key BLOB NOT NULL,
	start BLOB NOT NULL,          -- KeyCodec bytes; ~min is x''
	end_is_max INTEGER NOT NULL,  -- 1 for ~max
	end BLOB NOT NULL,            -- x'' when end_is_max is 1
	PRIMARY KEY (shard_group, hash_key, start, end_is_max DESC, end DESC)
) WITHOUT ROWID;
```

- `registerRepartition` writes the rows (Section 4.2.3). The primary key removes a repeat, for example B as a target
  of one report and as the source of the next.
- Each `range_split` and `key_promotion` also inserts the range root. A range split exists only after the promotion,
  so the range root is always a real partition, also when the `key_promotion` report is missing.
- `~min` is `encodeOptional(undefined)`, the empty key. It is the smallest `BLOB`. `~max` has no byte form, so
  `end_is_max` holds it.
- `KeyCodec.compare` is an unsigned byte compare, and a shorter key comes first when one key is a prefix of the
  other. This is the order of SQLite `BLOB` values, and `partition-store.ts` already scans keys in this order. So
  the primary key gives the order of the build: `start` ascending, then `end` descending. A parent comes before its
  children.

**The build** of one hash key reads the rows in that order once. Every report gives all the targets of its source,
and the targets cover the source. So every inner boundary is the start of a known interval, and:

- the boundaries are the distinct starts after `~min`, and they come in ascending order;
- `segmentDepth[i]` is the rank of the last row that starts at position `i`, which is the deepest one;
- the level of a boundary is the rank of the top of the stack before the first row that starts at it.

The steps:

1. Read the rows of `(shard_group, hash_key)` in primary-key order.
2. Keep a stack of the open ancestors. For each row, remove the top while it ends at or before the start of the row.
3. When the top ends after the start of the row and before the end of the row, the two intervals cross. Go to
   "Errors" below.
4. The top is the nearest known ancestor. The rank of the row is the rank of the top plus 1, or 0 when the stack is
   empty.
5. When the start of the row is new, append it as a boundary, with the rank of the top as its level, and start a new
   segment. Set the depth of the current segment to the rank of the row.
6. Push the row.

The dry run for the example:

| Row | Removed | Ancestor | Rank | Output |
| --- | --- | --- | --- | --- |
| R `[~min, ~max)` | none | none | 0 | S0 depth 0 |
| A `[~min, g)` | none | R | 1 | S0 depth 1 |
| B `[g, p)` | A | R | 1 | boundary `g`, level 0, S1 depth 1 |
| B1 `[g, k)` | none | B | 2 | S1 depth 2 |
| B2 `[k, p)` | B1 | B | 2 | boundary `k`, level 1, S2 depth 2 |
| C `[p, ~max)` | B2, B | R | 1 | boundary `p`, level 0, S3 depth 1 |

When only the report "B1 into `[g, h)` and `[h, k)`" is known, the rows are R, B1, `[g, h)`, `[h, k)`, with the
ranks 0, 1, 2, 2. The real depths are 0, 2, 3, 3. The stack finds the nearest known ancestor, so the ranks have no
gaps, and the fallback with `d - 1` always reaches a known partition.

**Errors.** In a split tree, two intervals are nested or disjoint. Two intervals that cross can come only from a
fault or from a report of an old epoch. Then the build of that hash key stops, stores a body with the range root
only, and logs one error. The build does not drop one row, because then a boundary can be a start of no known
interval.

**Memory and CPU.**

- The build keeps the stack and the body. The stack has one entry for each open ancestor: about 14 for 10,000
  leaves of a balanced tree, and up to one for each split in a tree of increasing keys. The body is at most
  `maxRangeBodyBytes`.
- The build makes no object for a row after the row leaves the stack.
- `SqlStorageCursor` is an iterator, and `rowsRead` increases as the cursor moves
  ([SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)). `batch-scan.ts`
  already reads rows one at a time in this way.
- A cursor has no snapshot across an `await`. So the build of one hash key runs in one `transactionSync` with no
  `await`: it reads the rows, encodes the body, and writes the chunks. A step stops between two hash keys after
  `buildStepRows` rows, and the next alarm continues (Section 4.2.4).
- For each row the build does one index step, a few byte compares, and some appends. The stack removes at most one
  entry for each row. Cost for 10,000 intervals: `TODO: measure`.
- The build reads only the hash keys in `fokos_topo_range_dirty`: the keys with a new interval (Section 4.2.3).

**The size cap.** `maxRangeBodyBytes` is an option of the keeper, default 1 MiB.

1. The first pass also counts the body bytes of the boundaries for each level.
2. When the body is larger than the cap, the build finds the largest rank `m` whose boundaries with a level less
   than `m` fit in the cap.
3. A second pass skips the rows with a rank larger than `m`. The boundaries with a level of `m` or more go, and each
   `segmentDepth` larger than `m` becomes `m`. The header has `truncatedAtRank: m`.

A caller then enters at most at rank `m`, and the partition forwards. The result is correct, with more hops. At
32-byte keys, 1 MiB holds about 26,000 leaves.

**The cache entry.** A range entry is in the same `LRUCache` as the hash entries, with the key
`r|<shardGroup>|<hashKey>`, the same byte budget, and the same refresh rules. It holds the parsed header, the views,
and the `cut` set. Its size is the `byteLength` of the body plus 16 bytes for each `cut` entry.

**Resolution** for a key with a Bloom hit:

1. When the range entry exists and is not empty, the caller uses the point lookup or the range lookup above.
2. When the range entry is absent, the caller starts a refresh and sends to the range root of the key
   (`~r.<hk>.~min.~max`). On a Bloom false positive, no repartition created that range root, and the runtime of the
   new object answers `range_partition_not_initialized`. The caller then sends again with the hash path and no
   Bloom step, as `#missLadder` in `runtime.ts` does. A range root in `awaiting_data` takes the same retry. The
   cost of a false positive is one extra hop and one empty object. Section 2.4 has the follow-up that removes the
   empty object.
3. When the range entry is empty, the Bloom hit was a false positive, and the caller uses the hash path.

The hash key goes in the query as base64url. The maximum length of the URL of a loopback call is
`TODO: measure`, and it limits the length of the hash key that this read supports. A longer key uses the hash
path.

**The runtime (M6, optional).** M3 gives the runtime the hash topology only. A promoted key still goes to the
deepest learned slice of `fokos_range_hierarchy`, or to the range root. The learned slices come only from the
responses to the forwards of the partition, and `rangeHierarchyMaxRows` bounds them. So the range root receives
the traffic of each caller with a cold cache, of each hash partition with no learned slice for the key, and of each
interval that no learned slice covers. M6 uses the cached range topology in the same way as M3 uses the hash
topology:

1. **The range owner.** `#rangeOwner` and the range router branch of `#resolve` also read
   `cachedRangeTopology(shardGroup, hashKey)`, and call `rangePointLookup` with the cut set of the entry. The
   target is the deeper of the learned slice and the topology target. On a range router, the topology target must
   be inside the interval of the router. A topology target outside it gives no hint.
2. **The miss.** A `range_partition_not_initialized` miss at a topology target goes to `#missLadder`. It adds
   `rangeCutKey` of the target to the cut set of the range entry, and the retry resolves again without that hint.
   The client does the same (Section 4.2.6).
3. **The Bloom step.** Where `#resolve` reads the Bloom filter of the partition, it also reads the promoted-key
   Bloom filter of the cached hash body (`maybePromoted`). A router then sends a promoted key directly into its
   range tree, and not first to the hash leaf that holds the route override. A false positive takes the existing
   retry with no Bloom step.
4. **The range frontier.** `#planRange` keeps the learned slices only. A planned visit has no fallback after a
   miss, so a topology hint there needs a fallback first. This is a follow-up.

For a partition, the Bloom filter and the range bodies are more useful than the hash bitmap. The hash arena learns
each split from the responses, stores it in SQLite, and keeps it, because a split never goes away. The Bloom filter
of a partition knows only the promotions that its own traffic showed, and its learned slices are bounded. The hash
body stays one body: at 1,000,000 partitions the bitmap is about 125 KB of 1.77 MB, so a separate Bloom body saves
little.

Tests for M6: a hash router forwards a promoted key to a range leaf that its own forwards never showed; a range
router forwards to a deeper range partition inside its interval and ignores a target outside it; a miss at a range
topology hint cuts the hint and the request completes; a Bloom hit of the topology on a router enters the range tree,
and a false positive completes through the hash path.

#### 4.2.10 Performance

- **Report.** One outbox insert in the `completed` transaction, one RPC for each repartition, and one row insert
  in the admin DO. A build starts at most once in each `buildDelayMs` for each shard group, and each alarm does one
step of at most `buildStepRows` rows. Only a new hash node or a new range root builds the hash body again, and only
a hash key with a new interval gets a new range body. A table with
  1,000,000 partitions and `hashSplitN` 4 has about 250,000 hash split reports over its life.
- **Read at the admin DO.** About one read for each upper tier in each `maxAgeS` for each cache key, and each data
  center whose lower tier misses at the same time goes to its upper tier. The number of upper tiers is
  `TODO: measure`. The shard groups of a table have different admin DOs, so their loads do not add up. The range
  reads of the hot promoted keys of one shard group do add up (Section 2.4, Follow-up 6).
- **Resolve.** One `hashChildIndex` (one xxhash of the hash key) and one `rank1` for each level of the walk. A walk
  to depth 4 is 4 hashes and 4 popcounts of at most 16 words. Cost: `TODO: measure`.
- **Revalidation.** After `maxAgeS`, Workers Cache serves the stale entry and calls the entrypoint in the
  background. When that call carries `If-None-Match`, the admin DO answers 304 with no body. When it does not, the
  admin DO sends the full body, about 1.76 MB at 1,000,000 partitions. The Workers Cache documentation confirms
  `If-None-Match` only for a revalidation after `invalidate()`
  ([purge](https://developers.cloudflare.com/workers/cache/purge/)). Behavior after `max-age`: `TODO: measure`.
  The entrypoint helper handles both cases.
- **Isolate refresh.** Each isolate fetches the body about once in each `refreshMs`. When a fetch with
  `If-None-Match` gets a 304 from the cache, the isolate does not get the body again. Behavior: `TODO: measure`.
- **Range resolve.** One binary search over the boundaries, then two scans of the levels. For a leaf each scan
  reads one value. A range lookup reads the levels across the range. Cost: `TODO: measure`.
- **Saved hops.** A request to a leaf at depth `d` saves up to `d` Durable Object round trips, about `10 * d` ms.

#### 4.2.11 Deployment and rollback

- The feature is off until the host configures `adminOutbox` and `topologyCache`. In FokosDB, that is
  `table.nsAdmin` and `table.topologyEntrypoint` (Section 4.2.8). Old partitions with no
  outbox hook write no outbox rows.
- A table that split before the outbox hook was on has an incomplete topology. Callers then enter at a shallower
  partition, which is correct. Section 2.4 lists a backfill as a follow-up.
- Rollback: remove the configuration. The outbox job deletes the outbox rows. The keeper tables stay and do no
  harm.
- A change of the body format uses a new path segment (`v2`), so old and new versions of the Worker do not read
  each other's entries when `cross_version_cache` is on.

#### 4.2.12 Testing

- **Codec.** A property test builds a random hash forest, encodes it, and checks that `findLeaf` matches a walk of
  the reference tree for random keys, with and without cuts.
- **Keeper.** Repeated events, events in any order (a child before its parent), a refused change of
  `hashSplitN`, a reset, an event older than `reset_at`, and a report of a known fact, which changes no version.
- **Build.** Many reports in one `buildDelayMs` give one build. A read before the build answers the older body. With
  `buildStepRows` 1, each step builds one body, and a report between two steps makes the next steps build its key.
  A `range_split` does not change the hash body, and a `hash_split` does not change a range body. A body above
  1 MiB is stored in chunks, and a new keeper over the same storage answers it with no build. `buildDelayMs` is an
  option, so the test needs no mock clock.
- **Codec.** The encoder refuses hash nodes with no parent and a node count that does not agree. The reader refuses
  a rank section that does not agree with the bitmap. The merge of any later forest into the bitmap of any earlier
  forest gives the body of a full build (property), also for a deep chain under one old leaf and splits over many
  words. The merge refuses a pending split that the old bitmap has, a pending split with no split parent, and a count
  that does not agree. An extended Bloom filter has the bits of a full build, and a count above the capacity builds
  the filter again from all the keys.
- **Merge in the keeper.** Reports in any order, with some reports missing and builds between them, give the same
  hash body as one full build of the same reports (property).
- **Outbox job.** A crash between `completed` and the RPC, a hook that throws and then succeeds, a host that
  removes the hook, and a partition that is `destroying`.
- **Topology cache.** No promise is stored; `waitUntil` receives each fetch; an older version is refused; a body
  larger than the budget is dropped; the LRU evicts by bytes, oldest first, and never the new entry; a miss adds one
  cut and the next request goes to the
  parent.
- **No cache.** With `useTopologyCache: false`, `FokosDB` sends no topology fetch and every request enters at
  the root.
- **Runtime range topology (M6, optional).** Section 4.2.9 lists the tests.
- **End to end.** In the vitest pool, a table splits to depth 2, the keeper receives the reports, and a new client
  enters at the leaf with `forwardCount` 0. The tests call the entrypoint helper and the keeper directly.
- **Range lookup (M5).** A property test makes random range trees with `rangeSplitN` 2 to 4, also trees of
  increasing keys. It drops a random part of the reports, builds the body, and checks each point lookup, range
  lookup, and fallback against a search of the known interval set. It runs with and without the size cap.
- **Range build (M5).** The example of Section 4.2.9 gives the values of its dry run. Repeated rows give one row.
  Two intervals that cross give a body with the range root only. A key with a missing `key_promotion` report
  still has the range root.
- **Body framing.** A header key that the reader does not know is ignored. Another `schema`, a section outside the
  body, and an offset that is not a multiple of 8 are refused.
- **Range false positive (M5).** A Bloom hit with no range entry for a key that is not promoted gets
  `range_partition_not_initialized` from the range root, and the request then completes through the hash path.
- **Workers Cache is not in local runs.** With Wrangler 4.140.0 (workerd 1.20260923.1), `wrangler dev` accepts the
  `cache` block and the `exports` map, but each `fetch()` to the entrypoint, from a Worker or from a Durable
  Object, runs the entrypoint again and has no `cf-cache-status` header. The vitest pool uses the same Miniflare.
  So the local tests check the headers that the entrypoint helper sets, and not collapsing or freshness. A check
  of collapsing, `stale-while-revalidate`, and `cross_version_cache` needs a deployed Worker.

### 4.3 Open questions

None.

## 5. Alternative options

- **Workers KV for the published topology.** KV allows 1 write each second for each key, has a minimum `cacheTtl`
  of 30 s, and needs one more binding ([KV limits](https://developers.cloudflare.com/kv/platform/limits/)). The
  admin DO stores the facts in a transaction, and Workers Cache gives request collapsing in front of it. Section
  2.4 (Follow-up 6) keeps KV as an option for hot range reads.
- **One admin DO for each table.** All the shard groups of the table (base partitions, coordinators, the
  materialization group, and each GSI) then share the request limit, the storage, and the alarm of one Durable
  Object. The range topology of the base table and of each GSI can grow without a limit.
- **A separate class for the topology DOs of FokosDB.** The consumer must then configure and deploy one more class
  and one more binding. The instance name already selects the role.
- **Durable Object RPC for every read.** Each isolate then calls the admin DO, and one DO for each shard group
  receives the read load of all isolates. Custom RPC methods bypass Workers Cache.
- **The hash key in `topology.Get`.** Each key then has its own cache entry, and the shared entry is lost.
- **LOUDS with `2N + 1` bits.** The fanout is always 0 or `hashSplitN`, so one bit for each partition holds the
  same tree.
- **The Cache API (`caches.default`).** It has no request collapsing and no tiered cache.
- **A purge or `invalidate()` after each report.** Purge uses the Free tier rate limits for every plan
  ([limitations](https://developers.cloudflare.com/workers/cache/limitations/)), and only the entrypoint that owns
  the entry can purge it. A stale topology costs hops only.
- **A report at cutover.** At cutover the targets still import, and every operation except a read answers
  `partition_migrating`.
- **A range interval list sorted by start.** Range intervals nest, so the last interval whose start is at or before a
  key can be a deep interval that ends before the key. The lookup then must go up to an ancestor.
- **Range intervals with parent links.** One 16-byte record for each known interval, with `start`, `end`,
  `parent`, and depth, plus a target for each segment. The ancestors add about `(L - 1) / (K - 1)` records for `L`
  leaves and `K = rangeSplitN`. The levels give the same lookups with two rank values for each boundary.
- **A Patricia trie now.** The DO name holds the full boundaries, so the body must keep them. A trie can only be an
  index over them, and adds about 16 bytes for each boundary. It replaces about `log2 n` byte compares of the binary
  search. Section 2.4 keeps it as a follow-up.
- **The real depth in the report.** A range `FokosSlice` has no depth. With a missing report the real depths have
  gaps, and a fallback to `d - 1` can give the same partition again. The rank of the build has no gaps.
- **A range build from the JSON of `fokos_topo_repartitions`.** The build must then hold all the intervals of a key
  in memory to sort them. `fokos_topo_range_intervals` gives them in order.
- **A promise in the topology cache.** A later request that awaits it does I/O for another request, which the
  Workers runtime refuses.

## 6. Frequently asked questions

**Does the admin DO need a `fetch()` handler?** No. The topology entrypoint is the HTTP surface. It calls the admin
DO over RPC. Workers Cache caches the entrypoint, and it never caches a Durable Object call.

**Why does the report not go through the entrypoint?** Workers Cache caches only `GET` and `HEAD`. A `POST`
through the entrypoint is one more hop with no gain, and a direct RPC needs no public path.

**Can a stale topology send a request to a partition that does not exist?** Only after a reset or a fault. The
keeper records only repartitions in `completed`, and partitions never merge. The fallback of Section 4.2.6 handles
the rest.

**Can a late report after `destroy` cause harm?** No. A report that a partition sent before `destroy` can pass the
`reset_at` check when the clocks of the two objects differ. The keeper then records a split that the new table does
not have. A caller sends once to a partition that does not exist, gets `hash_partition_not_initialized`, and cuts
that position (Section 4.2.6). The cost is one extra hop and one empty object, until the next topology version.

**Why one table for the repartitions of all kinds?** The table only makes the keeper idempotent, and no query
reads a field of one kind. Registration writes the facts into `fokos_topo_hash_nodes` and
`fokos_topo_range_intervals`, which hold them in the order of the build.

**Why is `FokosDBAutoAdminDO` not the generic class?** Its `fokos.admin.<tableName>` instance also owns the GSI
catalog and other FokosDB state. The
sharding library ships `FokosTopologyKeeper` and the small `FokosShardingAutoAdminDO` for a host that needs only
the topology.

**What does a deploy cost?** With `cross_version_cache: true`, nothing: the entries stay. Without it, each deploy
starts with an empty cache, and the first reads of each data center go to the admin DO.

## 7. References

- [FokosDB, the encoded topology](https://www.lambrospetrou.com/articles/fokosdb/#fokosdb-encoded-topology)
- `docs/agent-plans/2026-09-26-fokos-sharding-client.md`
- `docs/agent-plans/2026-09-27-learned-routes-for-every-dispatch-shape.md`
- `docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md`
- `docs/agent-plans/promoted-keys-bloom-filter-cache.md`
- `docs/ideas/topology-propagation-via-piggyback.md`
- [Workers Cache](https://developers.cloudflare.com/workers/cache/)
- [Workers Cache configuration](https://developers.cloudflare.com/workers/cache/configuration/)
- [Workers Cache keys](https://developers.cloudflare.com/workers/cache/cache-keys/)
- [Workers Cache purge](https://developers.cloudflare.com/workers/cache/purge/)
- [Workers Cache limitations](https://developers.cloudflare.com/workers/cache/limitations/)
- [Context (`ctx.exports`)](https://developers.cloudflare.com/workers/runtime-apis/context/)
- [Durable Object state](https://developers.cloudflare.com/durable-objects/api/state/)
- [Workers limits](https://developers.cloudflare.com/workers/platform/limits/)
- [KV limits](https://developers.cloudflare.com/kv/platform/limits/)
