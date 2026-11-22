# FokosDB Architecture

This page is a short technical introduction to FokosDB. Read it first if you learn the codebase. The second read is [internals/storage.md](./internals/storage.md), which tells what data each Durable Object stores, and why.

## Problem

FokosDB is a key-value database with strong consistency. It runs on Cloudflare Durable Objects (DOs), and its API and its transaction model follow DynamoDB. One DO has a storage limit and a throughput limit. Thus a table must use many DOs, and the database must move data between DOs when a table grows, while each request continues to see consistent data.

## Overview

A Worker runs the client, `FokosDB` (`client/db.ts`). The client sends each request to a DO. Four DO classes hold data. Three of them host the sharding runtime, `FokosShardingRuntime` (`sharding/runtime.ts`), which owns the routing, the splits, and the data migration between DOs.

| Durable Object                    | Holds                                                                    | Sharding runtime |
| --------------------------------- | ------------------------------------------------------------------------ | ---------------- |
| `FokosDBPartitionDO`              | the items of a table, or the entries of a GSI                            | yes              |
| `FokosDBTransactionCoordinatorDO` | the state of write transactions                                          | yes              |
| `FokosDBMaterializationDO`        | the latest image of each changed item, for the GSIs and the push targets | yes              |
| `FokosDBAutoAdminDO`              | the topology of each shard group                                         | no               |

A **shard group** is the set of DOs of one kind for one table. Each DO name starts with the name of its shard group:

- Partitions: `fokos.p.<tableName>`
- GSI partitions: `fokos.gsiv1.<n>.<tableName>.<indexName>`, where `<n>` is the length of the table name
- Coordinators: `fokos.tc.<tableName>.<fp(ns)>`
- Materialization: `fokos.m.<tableName>.<fp(ns)>`

`fp(ns)` is the fingerprint of the partition namespace (`namespaceFingerprint` in `shared/do-stubs.ts`). After the shard group, the sharding runtime adds the position of the DO in its tree: `~h.<root>[.<child>...]` for a hash partition, or `~r.<routingHash>.<start>.<end>` for a range partition.

The admin DO uses other names: `fokos.topo.<shardGroup without "fokos.">~<fp>` for one shard group, and the same name followed by `.rh.<16 hex>` for one promoted routingHash.

### How a request flows

```
Worker: FokosDB client
  |-- get, put, update, delete, batch, query ---> Partition [fokos.p]
  |-- gsi(index).queryItems --------------------> Partition [fokos.gsiv1]
  |-- transactWriteItems -----------------------> Coordinator [fokos.tc]
  |                                                 |-- prepare, commit, cancel --> Partition
  |-- topology read --> FokosDBTopologyCache --> Admin [fokos.topo]

Partition --(job: materialization_export)--> Materialization [fokos.m]
                                               |-- gsiWriteEntries --> Partition [fokos.gsiv1]
                                               |-- push batch -------> a push target of the user
Partition, Coordinator --(admin outbox: completed splits)--> Admin
```

1. **One item.** The client hashes the hash key and selects one of the `rootTreesN` root partitions. If the root has split, the sharding runtime of the root forwards the request to the partition that owns the key. With a topology cache, the client can send the request to the leaf directly.
2. **A query.** The root plans the range frontier and visits each partition that holds a part of the range. One page has a budget of items, bytes, and partition visits.
3. **A write transaction.** The client sends it to a coordinator. The coordinator runs a two-phase commit (2PC) with each partition that holds a key of the transaction.
4. **A read transaction.** The Worker runs it. It reads each partition two times and compares the results. No coordinator is necessary.
5. **Change capture.** A table with `captureChanges` records each change. A background job of the partition sends the changes to the materialization DOs, which write the GSI entries and call the push targets.
6. **Topology.** When a split is complete, the partition or the coordinator reports it to the admin DO. Callers read the topology back through the `FokosDBTopologyCache` Worker entrypoint, which uses the Workers Cache.

### How a shard group splits

All the DOs that host the sharding runtime split in the same way:

- **Hash split.** A partition that is larger than `hashSplitConditions.maxSizeMb` makes `hashSplitN` children. Then it becomes a router, and the children copy their share of the data in the background.
- **Promotion.** One routingHash that becomes too large moves into a range tree of its own.
- **Range split.** A range partition splits by `(hashKey, sortKey)` into `rangeSplitN` children.

The coordinator does only hash splits. See [The coordinator does only hash splits](#the-coordinator-does-only-hash-splits).

## Design decisions

### The DO name is the address of the data

A DO name selects the DO, and the DO holds the data. If the name of a DO changes, its data becomes unreachable. Thus the encoding of the names (`encodeRangeComponent`) must never change. For the same reason, `name`, `ns`, `nsTx`, `jurisdiction`, `rootTreesN`, `hashSplitN` and `coordinatorRootsN` of a table must never change after the table has data.

### The configuration travels in every request

Workers RPC cannot configure a DO when the DO starts. Thus each request carries the topology and the policy of its table (`PartitionContext`). The DO compares the topology with the topology that it stored. It replaces the stored policy only when the request has the same or a higher `policyVersion`.

### Each derived shard group copies the base table configuration

The client builds the configuration of the coordinators, the materialization DOs, and the GSIs from the base table configuration. Each one changes only a few values:

- Coordinators: the shard group, and `rootTreesN` becomes `coordinatorRootsN`.
- Materialization: the shard group only.
- GSI: the shard group and the key limits. The GSI also removes `captureChanges` and `nsMat`.

Thus one change to the base table, with a higher `policyVersion`, applies to all four shard groups.

### A DO name holds the fingerprint of the namespaces whose data it holds

The coordinator group and the materialization group of a table include `fp(ns)`. Thus two tables with the same name on two different partition namespaces never share a coordinator or a materialization DO.

### Each key starts with the routingHash

Each table that holds keyed data has a primary key that starts with `routing_hash`. Thus the rows of one hash child, or of one promoted routingHash, are one key range. A split reads that range and then deletes it, and the delete frees whole pages. [internals/storage.md](./internals/storage.md) has the details.

### The coordinator does only hash splits

The route key of a coordinator is the idempotency token. The routingHash is a hash of the token, so the tokens spread evenly, and each token holds the rows of one transaction only. Thus no routingHash can become too large. Thus the coordinator never needs a promotion or a range split, and `routing_hash` alone is enough to put the rows of a split target in one key range.

### The coordinator keys its rows by the transaction ID

`tc_state` has the primary key `(routing_hash, transaction_id)`, and a unique index on `(routing_hash, idempotency_token)`. The token only routes the request. All the other operations use the transaction ID, and a later version can remove the token.

### A read transaction does not use a coordinator

A read transaction writes nothing. Thus it needs no durable state, and the Worker can run it. It reads each partition two times and compares `found`, `version` and `max_deleted_v`. Any change aborts the transaction with `read_conflict`.

### `COMMITTING` is the point of no return

Each state change of the coordinator writes to SQLite before the coordinator sends an RPC. When the state is `COMMITTING`, the transaction must commit. More than one drive of the same transaction can run at the same time, but only the SQL write of `COMMITTING` or `CANCELLING` decides the outcome.

### A write to a locked item is refused

A non-transactional write to an item that a transaction has locked fails immediately. The partition does not delay it. Thus a write never waits for a lock.

### Materialization routes by the base key

The materialization DO routes each image by the base `(hashKey, sortKey)`, in its own shard group. Thus a split of the base table does not move rows in the materialization group, and the two groups split independently.

### A GSI is a table of partitions

The entries of a GSI are in `FokosDBPartitionDO` instances in the partition namespace of the table, with their own shard group. Thus a GSI query uses the same code as a table query, and a GSI with a hot hash key splits in the same way as a table.

### Requests carry expressions as trees, never as SQL

The client validates each condition, update, projection and filter, and sends it as an expression tree. Only the partition compiles a tree to SQL, and it refuses a request that carries a compiled plan. Thus only the partition checks the limits of the compiled SQL.

### The client never imports a DO class as a value

A value import pulls the whole server implementation into the client bundle. The client uses only the type-only helpers of `shared/do-stubs.ts`. `pnpm build` enforces this rule.
