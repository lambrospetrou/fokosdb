# RFC — One `meta` shape with a list of partitions for every FokosDB result and error

**State:** Draft
**Date:** 2026-10-16
**Author:** Lambros Petrou
**Status:** Not built.

## Table of Contents

1. [Overview and Context](#1-overview-and-context)
2. [Goals and Requirements](#2-goals-and-requirements)
3. [Milestones](#3-milestones)
4. [Proposed Solution](#4-proposed-solution)
5. [Alternative Options](#5-alternative-options)
6. [Frequently Asked Questions](#6-frequently-asked-questions)
7. [References](#7-references)

## 1. Overview and Context

### 1.1 The current system

Every partition RPC answers a `FokosEnvelope<T>`. `value` is the result. `routing` is the route evidence that the
sharding runtime adds. `routing.servedBy` lists the partitions that took part, each as a `FokosRouteNode` with
`ref.partitionId`, `ref.doName`, `actorId`, `hashDepth`, `rangeDepth` and a role. `RouteCollector.build` in
`src/sharding/envelope.ts` caps the list at `ROUTE_EVIDENCE_MAX_BYTES` (10 KB). It always keeps one node: the raiser
of an error, else the first node with the strongest role. It drops other nodes when the list is over the cap.

`src/client/partition-info.ts` builds the public `PartitionInfo` from `routing`:

```ts
type PartitionInfo = {
	servedByActorId: string;
	servedByActorName: string;
	servedByPartitionId: string;
	forwardCount: number;
	hashDepth: number;
	rangeDepth: number;
};
```

The methods return partition information in five different ways:

| Method or error | Partition information today |
| --- | --- |
| `putItem`, `getItem`, `deleteItem` | `meta: OperationMetrics & PartitionInfo` |
| `queryItems` | `meta: QueryItemsMeta` and `partitionMetas: QueryPartitionMeta[]` |
| `transactWriteItems`, `transactGetItems` | none |
| `FokosConditionCheckError` | `meta: OperationMetrics & PartitionInfo` |
| other errors that a partition raised | `meta?: PartitionInfo`, with no counts (`FokosDBError` in `src/client/db.ts`) |
| `FokosTransactionCancelledError` | none |

### 1.2 The problems

1. **The transaction methods return no partition information.** The fast paths call one partition, and the answer
   carries its routing. `#writeSingleShotFastPath` and `#readSnapshotFastPath` in `src/client/db.ts` read only
   `.value`, so the routing is lost.
2. **A cancelled transaction does not name its partitions.** When a partition cannot run its operations, each rejected
   reason has a code and an `error_id`, but no partition.
3. **`queryItems` can drop a leaf.** `leafPartitionInfo` finds each leaf by `partitionId` in `routing.servedBy`. When
   the 10 KB cap dropped the node, the client skips the leaf. Then `sum(partitionMetas[i].rowsRead)` can be less than
   `meta.rowsRead`, and `partitionMetas.length` can be less than `meta.partitionsVisited`.
4. **An unknown partition looks like a real one.** `routeInfo` returns `""` and `0` when it has no node. The type
   declares the fields as required, so a caller cannot tell "unknown" from a real value.
5. **The same field names have different shapes.** A routed error has `meta` with no counts. A condition error has
   `meta` with counts. `queryItems.meta` has no `rowsWritten` and no `databaseSize`, and it has `rowsReturned`.

### 1.3 Facts that constrain the design

- **A range partition name can be about 6 KB.** A range partition name is `<shardGroup>~r.<hk>.<start>.<end>`
  (`rangePartitionDoName` in `src/sharding/partition-id.ts`). It holds the hash key and the two sort-key bounds. With
  the default limits, the hash key is at most 1024 bytes and each bound at most 512 bytes (`MAX_HASH_KEY_BYTES` and
  `MAX_SORT_KEY_BYTES` in `src/shared/transaction-limits.ts`). `encodeRangeComponent` can write one byte as three
  characters (`%XX`), so the name can be up to about 6 KB. A range `partitionId` holds the same two bounds.
- **An actor ID is always 64 hex characters.** It is `DurableObjectId.toString()`.
- **A transaction has at most 100 operations** (`MAX_ITEMS_PER_TX` in `src/shared/transaction-limits.ts`). Each
  operation has one owner, so a transaction has at most 100 leaf partitions.
- **A `queryItems` page visits at most 100 partitions** (`MAX_PARTITION_VISITS_PER_PAGE` in
  `src/shared/query/page-budget.ts`).
- **A `group` operation can fan out.** `txPrepare`, `txCommit`, `txCancel` and `txReadForTransaction` are `group`
  operations (`operations()` in `src/server/do-partition.ts`). The partition that gets the request can forward parts
  of it to other owners. Its `merge` joins the answers and keeps no record of which owner gave which answer. Thus one
  coordinator participant, or one client group, can be more than one leaf partition.
- **A router visits the leaves of a range one after the other.** `walkRangeVisits` in `src/server/do-partition.ts`
  appends the items and the `partitionMetas` of each visit in visit order. Thus the items of one leaf are one
  contiguous block in the page.
- **A projected `queryItems` item is a flat record that the caller names** (`ProjectedItem` in
  `src/shared/expression/projection.ts`). A library field in that record can have the same name as a caller field.
- **A range partition does not know its own hash depth.** `#selfNode` in `src/sharding/runtime.ts` writes
  `hashDepth: 0` for a range partition. The hash partition that forwards into the range tree stamps the hash depth on
  the range nodes (`#rangeDepthStamp`).
- **Route learning reads only the routing envelope.** `#learn` in `src/sharding/runtime.ts` reads
  `routing.servedBy`. The client sharding layer reads only `routing.forwardCount` (`FokosShardingClient` in
  `src/sharding/client.ts`). No code reads the public `servedByPartitionId` or the `partitionId` of `LeafMetrics`
  for routing.
- **The coordinator stores the result of a transaction.** A replay with the same token and the `tx_recovery` job
  answer from `tc_state.results_json` (`loadFinalResponse` in `src/server/do-transaction-coordinator.ts`). A
  coordinator split copies `tc_state` and `tc_participants` rows to its children.

## 2. Goals and Requirements

### 2.1 In scope

1. Every result and every error of every `FokosDB` method has one field, `meta`, of one base type, `FokosMeta`.
2. `meta.partitions` lists each partition that ran the request, one entry for each partition.
3. A `transactGetItems` item and a rejected `transactWriteItems` operation point into `meta.partitions` by index.
   They do not repeat the partition information.
4. `meta.partitions` of `queryItems` has one entry for each leaf that the page visited. No entry is dropped.
5. `transactWriteItems`, `transactGetItems` and `FokosTransactionCancelledError` return `meta`, on the fast path and
   on the two-phase path.
6. The `meta` of the coordinator path of `transactWriteItems` names the transaction coordinator that answered, in
   `meta.txCoordinator`.

### 2.2 Requirements

- An entry must not hold a partition name or a partition ID that the partition itself sends in `value`. The name and
  the ID can be about 6 KB (section 1.3). The size of an entry must not depend on the keys.
- `servedByActorName` is optional. The client fills it when it knows the name, and leaves it absent when it does not.
- A count in an entry is optional in the base type. It is absent when the partition did not answer, or when its
  answer carries no counts. A success type can declare the counts as required where every entry has them.
- An absent value must be absent. A field must never hold `""` or `0` to mean "unknown".
- An error carries the `meta` of every answer that the client has when it raises the error or passes it on. This
  also applies to an error that the client raises itself after some partitions answered (section 4.2.3).
- `meta` is absent only when nothing answered: a validation error before any I/O, or a failure of the RPC from the
  Worker to the first partition.
- The routing envelope does not change. Route learning must not change.
- The coordinator stores no `meta`. It adds no column and no row. DO storage bills each row written.

### 2.3 Out of scope

- The item shape difference between `getItem` and `transactGetItems`, and the key source difference between the
  methods. They are not about `meta`.
- The two meanings of `idempotencyToken` when the caller sends no token. It is not about `meta`.
- A link from each passed operation of `transactWriteItems` to its partition. No caller needs it now. A rejected
  operation has this link (section 4.2.7).
- The same `meta` on a replay of a `transactWriteItems` token. `meta` lists the partitions that ran the request that
  answers, and a replay that only reads the stored outcome runs none (section 4.2.7).
- The counts of the transaction operations. A transaction entry has no counts now (section 4.2.6). A follow-up
  decides, for all methods together, which rows each operation counts. For example: does a prepare count the lock
  rows that it writes in `pending_transactions`, or only the rows of `items`?

## 3. Milestones

1. **All of it, in one change.** Every section of section 4 ships together, so that no released state has a method
   with a partial `meta`.

## 4. Proposed Solution

### 4.1 High-level overview

Each result and each error has `meta`. `meta` has two parts:

- `forwardCount`: the number of partition-to-partition forwards of the whole request.
- `partitions`: one entry for each partition that ran the request. An entry has the actor ID and the depths of the
  partition, its counts, and its name when the client knows it.

A method that uses one partition returns a list of one entry. A method that uses many partitions returns one entry
for each. A `transactGetItems` item and a rejected `transactWriteItems` operation carry `partitionIndex`, the index
of their partition in `meta.partitions`. For `queryItems`, each entry carries `itemCount` in place of an index on each item.

Each partition writes its own entry into the `value` of its answer. A router that merges the answers of other
partitions joins their lists. The 10 KB cap applies only to `routing`, so it cannot drop an entry. The client gets the
optional name from `routing`, when the node of that actor ID is still in the list.

```
 Worker (db.ts)                  router partition                 leaf partitions
 ──────────────                  ────────────────                 ───────────────
  request ─────────────────────▶  forward parts ───────────────▶  A: value.partitions = [A]
                                                 ───────────────▶  B: value.partitions = [B]
                                  merge: value.partitions = [A, B]
                                         routing.servedBy = [R, A]   (B dropped by the 10 KB cap)
  ◀───────────────────────────────
  meta.partitions = [ {A, name from routing}, {B, no name} ]
```

### 4.2 Technical details

#### 4.2.1 Public types

```ts
/** One partition that ran a request. */
type PartitionMeta = {
	/** The DurableObjectId of the partition, 64 hex characters. */
	servedByActorId: string;
	/** The name of the partition. Absent when the client does not know it. */
	servedByActorName?: string;
	/**
	 * FOR DEBUGGING ONLY. For a hash partition, its depth in the hash tree. A range partition has no hash depth, so
	 * for a range partition it is the depth of the hash partition that forwarded the request into the range tree.
	 * Absent for a range partition when no hash partition forwarded the request into the range tree.
	 */
	hashDepth?: number;
	/** FOR DEBUGGING ONLY. The depth in the range tree. Absent for a hash partition, which has no range depth. */
	rangeDepth?: number;
	/** Absent when the partition did not answer, or when its answer carries no counts. */
	rowsRead?: number;
	rowsWritten?: number;
	databaseSize?: number;
};

type FokosMeta<P extends PartitionMeta = PartitionMeta> = {
	/** The partition-to-partition forwards of the whole request. */
	forwardCount: number;
	partitions: P[];
	/** The transaction coordinator that answered. Present only on the coordinator path of `transactWriteItems`. */
	txCoordinator?: PartitionMeta;
};

/** An entry with every count, for a result where every partition answered. */
type AnsweredPartitionMeta = PartitionMeta & OperationMetrics;
```

`PartitionInfo`, `QueryItemsMeta` and `QueryPartitionMeta` are removed. Each method type below is a subtype of
`FokosMeta`, so every error declares one type:

```ts
type FokosDBError = FokosAnyError & { readonly meta?: FokosMeta };
```

#### 4.2.2 The result of each method

```ts
putItem    → { item, version, meta: FokosMeta<AnsweredPartitionMeta> & { partitions: [AnsweredPartitionMeta] } }
getItem    → { found, item,   meta: FokosMeta<AnsweredPartitionMeta> & { partitions: [AnsweredPartitionMeta] } }
deleteItem → { item, deleted, meta: FokosMeta<AnsweredPartitionMeta> & { partitions: [AnsweredPartitionMeta] } }

queryItems → {
	items, count, scannedCount, cursor?,
	meta: FokosMeta<AnsweredPartitionMeta & { itemCount: number }> & { rowsReturned: number },
}

transactWriteItems → { transactionId, idempotencyToken, meta: FokosMeta }

transactGetItems → {
	items: Array<MaybeReadItem & { partitionIndex: number }>,
	meta: FokosMeta,
}
```

The single-partition methods declare `partitions` as a tuple of one entry, so `meta.partitions[0]` needs no check for
`undefined`.

A caller reads `res.meta.partitions[0].rowsRead` in place of `res.meta.rowsRead`.

#### 4.2.3 The `meta` of each error

| Error | `meta` |
| --- | --- |
| A validation error before any I/O | absent |
| A failure of the RPC from the Worker to the first partition, with no routing | absent |
| `FokosConditionCheckError` | `FokosMeta<AnsweredPartitionMeta>`, one entry |
| An error that a partition raised on a single-partition method | `FokosMeta`, one entry with no counts |
| `FokosTransactionCancelledError` | `FokosMeta`, section 4.2.7 |
| An error that a partition raised on a transaction method | `FokosMeta`, the answers so far and the raiser entry |
| An error that a partition raised on a `queryItems` sub-query | `FokosMeta`, the answers so far and the raiser entry |
| `pending_write` or `read_conflict`, which the client raises in `transactGetItems` | `FokosMeta`, the answers so far |
| An error that the coordinator raises, for example `transaction_commit_pending` | `FokosMeta`, section 4.2.7 |

`withFokosErrors` in `src/client/db.ts` builds the entry of a routed error from the node that `build` always keeps:
the raiser. That node is always in `routing.servedBy`, so this entry always has `servedByActorName`.

"The answers so far" are the entries of every answer that the client has when it raises the error or passes it on:

- `queryItems`: the entries of the sub-queries that answered before the failed sub-query.
- `transactGetItems` on the fast path: the entries of the `txReadSnapshot` answer that says `aborted`.
- `transactGetItems` on the two-phase path: the entries of each group answer of the phases that ran. For
  `read_conflict`, this is both phases. When one group call fails, it is the other group answers of that phase and of
  the phase before it, and the raiser entry of the failed call.

The client joins these entries with the rule of the result: one entry for each `actorId`.

#### 4.2.4 The entry of one partition, on the wire

A partition writes its entry with the type below. It writes no name and no partition ID.

```ts
type WirePartitionMeta = {
	actorId: string;
	/** Absent in the entry that a range partition writes. Section 4.2.4 says who sets it. */
	hashDepth?: number;
	/** Absent in the entry that a hash partition writes. */
	rangeDepth?: number;
	rowsRead?: number;
	rowsWritten?: number;
	databaseSize?: number;
};
```

Each partition writes only the depth that it has. A depth that an entry has must be equal to the same depth of the
`FokosRouteNode` of the same partition, after `#rangeDepthStamp`:

- A hash partition has no range depth. It writes its own `hashDepth` and no `rangeDepth`. Its route node has
  `rangeDepth: 0` (`#selfNode`), and the entry does not copy that value.
- A range partition has no hash depth. It writes its own `rangeDepth` and no `hashDepth`.
- The hash partition that forwards a request into a range tree writes its own depth as the `hashDepth` of each range
  entry in the answer, as `#rangeDepthStamp` does on the range nodes. Thus the `hashDepth` of a range entry is the
  depth where its hash key left the hash tree.
- A request that goes straight to a range partition passes no hash partition, so its range entries have no
  `hashDepth`. Today each request enters at a hash root (`resolve` in `src/sharding/client.ts` returns
  `rootContext`), but a client cache of the topology can later send a request straight to a range partition.

`#forwardTo` and the read-through path in `src/sharding/runtime.ts` apply the stamp to the entries in `value` through
a hook of the operation descriptor:

```ts
// FokosOperationBase<Req, Res> in src/sharding/runtime-types.ts
/**
 * Writes `hashDepth` on the range entries of an answer that this hash partition got from a range partition. The
 * runtime calls it under the same condition as the route-node stamp.
 */
stampHashDepth?(res: Res, hashDepth: number): Res;
```

```ts
// #forwardTo in src/sharding/runtime.ts. `hashDepth` is the depth of this hash partition.
const stamp = isRangePartition(target) ? this.#rangeDepthStamp() : undefined;
const result = await call;
collector.mergeForwarded(result.routing, stamp);
return stamp && descriptor.stampHashDepth ? descriptor.stampHashDepth(result.value, hashDepth) : result.value;
```

- `#forwardTo` forwards for every shape, so the hook covers point, `group`, `single_owner` and `range` operations.
- The hook fills `hashDepth` only on an entry that has none. A range entry has none on the wire, so the first hash
  partition above the range tree fills it, and a partition above that one changes nothing. This is the rule of
  `#rangeDepthStamp` for route nodes.
- The runtime does not read `value`. The host owns the shape of `value`, and the runtime gives only the depth.
- The hook runs only on a result. An error carries no entries, and its raiser node in `routing` is already stamped.
- The query and transaction descriptors in `src/server/do-partition.ts` use one helper that maps `res.partitions`. An
  answer with no `partitions`, for example `not_applicable`, stays as it is.

#### 4.2.5 `queryItems`

On the wire, `LeafMetrics` in `src/server/do-partition.ts` changes from `OperationMetrics & { partitionId }` to
`WirePartitionMeta & OperationMetrics & { itemCount: number }`. `queryItemsLocal` writes the entry. `walkRangeVisits`
appends the entries in visit order, as it does now.

In `db.ts`:

1. The client appends the entries of each sub-query to `meta.partitions`, in order.
2. For each entry, the client finds the node with the same `actorId` in `routing.servedBy`. When it finds the node,
   it sets `servedByActorName` to `node.ref.doName`.
3. The items of entry `i` are the `itemCount` items after the items of entries `0` to `i - 1`.

`leafPartitionInfo` is removed. The page totals `rowsRead` and `partitionsVisited` are removed, because they are
equal to `sum(partitions[i].rowsRead)` and `partitions.length`. `rowsReturned` stays, because no entry reports it.

`QueryPageBudget` counts `res.partitionMetas.length`. It continues to count the entries of the list.

Size: an entry is about 150 bytes of JSON. A page has at most 100 entries, so the list adds at most about 15 KB.

#### 4.2.6 A transaction answer of a partition

Each answer of `txPrepare`, `txCommit`, `txCancel`, `txReadForTransaction`, `txReadSnapshot` and
`txExecuteSingleShot` carries one more field:

```ts
{ partitions: WirePartitionMeta[] }
```

- A local handler returns a list of one entry, itself.
- A `merge` appends the lists of its parts, in part order.

An index goes on the object that it names, not in a list that is parallel to the request. A merge does not keep the
order of the items, but each object keeps its own index, so a merge only adds the length of the lists before the part:

- A rejected result of a `txPrepare` answer has `partitionIndex`, the index of the partition that rejected it in the
  `partitions` of the answer. A local handler gives `0`. `mergePrepare` adds the offset of the part. A passed or a
  `not_evaluated` result has no index.
- An item of a `txReadForTransaction` answer has `partitionIndex`, the index of the partition that read it. A local
  handler gives `0`. The merge adds the offset of the part.
- `txReadSnapshot` and `txExecuteSingleShot` are `single_owner` operations, so their answers have one entry, and they
  carry no index.

A transaction entry has no counts. The participant methods in `TransactionParticipant`
(`src/shared/partition/transaction-participant.ts`) do not count rows today, and this change does not add a count. A
follow-up adds the counts of the transaction operations (section 2.3). The joins of section 4.2.7 and section 4.2.8
already sum the counts by `actorId`, so the follow-up changes only the local handlers.

#### 4.2.7 `transactWriteItems` and `FokosTransactionCancelledError`

The committed result:

```ts
{ transactionId, idempotencyToken, meta: FokosMeta }
```

The cancelled error:

```ts
FokosTransactionCancelledError {
	results: Array<
		| { outcome: "passed" }
		| { outcome: "not_evaluated" }
		| { outcome: "rejected"; reason: RejectionReason; partitionIndex?: number; itemOmitted?: "response_too_large" }
	>;
	meta: FokosMeta;
}
```

`RejectionReason` does not change. It has no partition fields. A rejected entry has no `partitionIndex` when the
request that answers did not get the answer that rejected it: a replay, a participant that did not answer, or a
rejection that another drive of the same transaction stored.

**The fast path.** `#writeSingleShotFastPath` reads `partitions` from the `txExecuteSingleShot` answer, and gives each
rejected operation `partitionIndex: 0`. When the partition raises an error, the error has no answer list. The client
then builds one entry from the raiser node of the error routing, with no counts, and gives each operation
`partitionIndex: 0`.

**The coordinator path.** The coordinator stores no `meta`. It joins the answers that the participant RPCs of the
current request get, in memory:

1. Each prepare, commit and cancel answer adds its entries to the join, with the name from the routing of the answer.
   The join sums the counts of one partition, by `actorId`. `forwardCount` is the sum of the forward counts of the
   answers.
2. A rejected result of a prepare answer gets the index of its entry in the join, by `opIndex`. Only the answer that
   the coordinator stores gives indexes, because the stored answers give the results of the cancel.
3. The answer of the request returns the join as `meta`, with the outcome that `loadFinalResponse` reads.

A participant that did not answer gets no entry. Its operations have the reason `prepare_unanswered` and no
`partitionIndex`.

A replay that finds a terminal transaction sends no participant RPC, so its `meta.partitions` is `[]` and its
`meta.forwardCount` is `0`. Its rejected results have no `partitionIndex`. A replay that drives a transaction further,
for example the commits that a `transaction_commit_pending` left, lists the partitions of the RPCs that it sends.

**The coordinator entry.** The transaction coordinator is a sharded Durable Object: its shard group
`fokos.tc.<tableName>` has `coordinatorRootsN` roots that split by hash. Thus its entry has the shape of a hash
partition entry, with no counts.

- The client builds `meta.txCoordinator` from the routing of its own RPC to the coordinator. `build` always keeps the
  node that ran the request or raised the error, so the entry always has `servedByActorName`.
- The fast path has no coordinator, so its `meta` has no `txCoordinator`.

| Case | `meta.txCoordinator` | `meta.partitions` |
| --- | --- | --- |
| Committed, on the coordinator path | present | the participant answers of this request |
| `FokosTransactionCancelledError`, on the coordinator path | present | the participant answers of this request |
| `transaction_commit_pending` | present | the participant answers of this request |
| Another coordinator error, for example `partition_migrating` | present | `[]` |
| The fast path, and every other method | absent | section 4.2.2 and section 4.2.3 |

A coordinator error carries the entries of the participant answers as its own `meta` over the wire, as
`FokosConditionCheckError` carries `reason` and `meta`. The client adds `meta.txCoordinator` from the routing of the
error. The forwards of the RPC from the client to the coordinator are not part of `forwardCount`.

#### 4.2.8 `transactGetItems`

```ts
{ items: Array<MaybeReadItem & { partitionIndex: number }>, meta: FokosMeta }
```

A found item uses the `ReadItem` envelope, not a flat record, so a `partitionIndex` field on each item is safe.

**The fast path.** `#readSnapshotFastPath` reads the list of one entry from the `txReadSnapshot` answer, and gives each
item `partitionIndex: 0`.

**The two-phase path.** `#readTransaction` sends one `txReadForTransaction` for each group in each phase. The client
joins the lists of all answers of both phases. It sums the counts of the two phases of one partition into one entry,
by `actorId`. Each phase 1 item changes its index from the list of its answer to the joined list. The returned items
are the phase 1 items, so the index names the partition that gave the phase 1 answer.

#### 4.2.9 Size

| Method | Most entries | Size of the list, without names |
| --- | --- | --- |
| single-partition method | 1 | about 150 bytes |
| `queryItems` page | 100 | about 15 KB |
| `transactWriteItems`, `transactGetItems` | 100 | about 15 KB |

A name adds its own length to an entry. A range name can be about 6 KB.

#### 4.2.10 The code that changes

Library:

- `packages/fokosdb/src/client/db.ts`, `src/client/index.ts`, `src/client/partition-info.ts`
- `packages/fokosdb/src/server/do-partition.ts`, `src/server/do-transaction-coordinator.ts`
- `packages/fokosdb/src/shared/types.ts`, `src/shared/transaction-api-types.ts`, `src/shared/transaction-wire-types.ts`
- `packages/fokosdb/src/shared/errors.ts`, `src/shared/errors-operations.ts`, `src/shared/query/page-budget.ts`
- `packages/fokosdb/src/shared/partition/transaction-participant.ts`
- `packages/fokosdb/src/sharding/runtime.ts`, `src/sharding/runtime-types.ts`

Tests and test helpers:

- `packages/fokosdb/src/client/db.test.ts`, `src/shared/errors-operations.test.ts`,
  `src/shared/query/page-budget.test.ts`
- `packages/fokosdb/test/destroy.test.ts`, `test/item-conditions-return-values.test.ts`, `test/item-errors.test.ts`,
  `test/sanity.test.ts`
- `packages/fokosdb/test/partition-do/`: `error-meta.test.ts`, `hash-split.test.ts`, `helpers.ts`, `item-crud.test.ts`,
  `partition-harness.ts`, `promotion.test.ts`, `query-items.test.ts`, `range-split.test.ts`, `read-through.test.ts`,
  `tx-participant.test.ts`
- `packages/fokosdb/test/property-based/`: `harness.ts`, `query-harness.ts`, `query-items-active-split.test.ts`,
  `query-items-split.test.ts`
- `packages/fokosdb/test/sharding-prototype/`: `fokosdb-client-sketch.ts`, `fokosdb-partition-host.ts`

Example:

- `examples/http-api/public/demo/index.html` reads `meta.servedByActorName`. `examples/http-api/src/rpc/routes.ts`
  passes `meta` on and reads no field of it. The `demo2` example reads the routing of `FokosShardingClient`, not
  `meta`, so it does not change.

#### 4.2.11 Testing

- A test for each method checks that `meta.partitions` names the partition that ran it, by `servedByActorId`.
- A `queryItems` test with more leaves than the 10 KB routing cap can hold checks that `partitions.length` is equal to
  the leaves visited. It checks that `sum(itemCount)` is equal to `items.length`, and that an entry with no name in
  the routing has no `servedByActorName`.
- A `transactWriteItems` test with a participant that fans out to two owners checks two entries and the index of each
  rejected operation.
- A replay test checks that a replay of a terminal transaction returns `meta.partitions` `[]` and no `partitionIndex`.
- A test with a promoted key checks that each range entry has the `hashDepth` of the hash partition that forwarded
  into the range tree, and that it is equal to the `hashDepth` of the route node of the same partition.
- A `transactGetItems` test that gets `pending_write` or `read_conflict` checks that the error has the entries of the
  partitions that answered.
- A `queryItems` test with a failed second sub-query checks that the error has the entries of the first sub-query
  and the raiser entry.
- A cancelled-transaction test checks the `partitionIndex` of each rejected entry on both paths.
- A coordinator-path test checks that `meta.txCoordinator` names the coordinator, on a commit, on a cancel and on
  `transaction_commit_pending`. A fast-path test checks that `meta.txCoordinator` is absent.
- A test checks that no field holds `""` or `0` to mean "unknown".

## 5. Alternative Options

### 5.1 A partition identity in each rejected reason

Add `servedByActorName` and `servedByActorId` to each reason of an execution failure. Rejected: every operation of a
partition repeats the same values. A range name can be about 6 KB, so 100 operations can add about 600 KB to the
error and to `results_json`. Each reason already has the `error_id` of its partition, and the partition log names the
partition for that `error_id`.

### 5.2 A name and a partition ID in each entry

Rejected: a range name and a range partition ID both hold the key bounds, and the partition would also send the same
node in `routing`. The size of an entry would then depend on the keys.

### 5.3 Keep `PartitionInfo` for the single-partition methods

Keep `meta: OperationMetrics & PartitionInfo` on `putItem`, `getItem` and `deleteItem`, and use `FokosMeta` only for
the methods that use many partitions. Rejected: an error then has two `meta` shapes, and `FokosDBError` must declare
a union. A caller must test the shape before it reads `meta`.

### 5.4 `partitionIndex` on each `queryItems` item

Rejected: a projected item is a flat record that the caller names, so a library field can have the same name as a
caller field. The items of one leaf are contiguous, so `itemCount` on each entry gives the same link.

### 5.5 The runtime builds the entries in the envelope

The runtime adds a second list to the envelope, with no cap and no names, and the host adds only the counts. It needs
no `stampHashDepth` hook. Rejected: it changes the routing envelope, and section 2.2 requires that the envelope stays
the same.

### 5.6 Store the `meta` of a transaction, so a replay returns the same `meta`

The coordinator stores the lists of each participant answer in new columns of `tc_participants`, and the joined
`meta` in `tc_state` with the terminal state. Rejected: the first answer of a phase can come from another drive, a
commit can finish in the `tx_recovery` job, and two drives can send the operations in a different order. Thus each
stored list needs the `opIndex` of each operation, a fixed join order, and a rule for which answer of a phase stays.
That is a large part of the coordinator for a value that is for debugging only.

### 5.7 A `partitionIndex` on each operation of `transactWriteItems`

Each answer carries `partitionIndexes`, a list that is parallel to the items of its request. Rejected: a merge does
not keep the order of the items, so each merge, the coordinator and the client must find each item by its key to
build the parent list. A passed operation has no object of its own in the answers of `txCommit` and `txCancel`, so
the list is the only place for its index.

### 5.8 Find the partition of each leaf in the routing

The current design. Rejected: the 10 KB cap can drop the node, and the client then cannot name the leaf.

## 6. Frequently Asked Questions

### 6.1 Does the removal of `partitionId` change routing or route learning?

No. Route learning reads `routing.servedBy`, and the routing envelope does not change. The two removed copies are
output only: the public `servedByPartitionId`, which `routeInfo` builds after routing is done, and the `partitionId`
of `LeafMetrics`, which only `leafPartitionInfo` reads.

### 6.2 Why is `servedByActorName` optional?

The name comes from `routing`, and the 10 KB cap can drop the node. The entry on the wire has no name, so that its
size does not depend on the keys. A single-partition result and its error always have the name, because `build`
always keeps the node that ran the request or raised the error.

### 6.3 Why are the counts optional in the base type?

One entry type serves every result and every error. A routed error, a participant that did not answer, and a
transaction entry have no counts. The success types of `putItem`, `getItem`, `deleteItem` and `queryItems` declare
the counts as required.

### 6.4 Why are there no page totals on `queryItems`?

No entry can be dropped, so the totals are equal to the sums over `meta.partitions`. Two copies of one value can
disagree.

## 7. References

- `docs/agent-plans/2026-09-27-learned-routes-for-every-dispatch-shape.md`
- `docs/agent-plans/2026-10-03-ordered-per-item-transact-write.md`
- `src/client/db.ts`, `src/client/partition-info.ts`
- `src/server/do-partition.ts`, `src/server/do-transaction-coordinator.ts`
- `src/sharding/envelope.ts`, `src/sharding/runtime.ts`, `src/sharding/partition-id.ts`
- `src/shared/types.ts`, `src/shared/transaction-api-types.ts`, `src/shared/transaction-wire-types.ts`
