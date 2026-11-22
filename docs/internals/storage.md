# Storage

This page tells what data each Durable Object (DO) stores in SQLite, and why each table has its key. Read [ARCHITECTURE.md](../ARCHITECTURE.md) first.

The comments above each `CREATE TABLE` in the code give the full rules. This page gives the overview and the reasons. The excerpts below show only the key columns.

## Rules for all the DOs

### The key starts with the routingHash

Each table that holds keyed data has a primary key that starts with `routing_hash`. The sharding runtime gives this value (`routingHashForKey`). It is the hash of the route key in the shard group of the DO. The store treats it as opaque bytes.

A split moves whole routingHashes. Thus the rows of one hash child are one key range, and the rows of one promoted routingHash are also one key range. A split reads that range, and then deletes it.

The routingHash is not an identity. Two hash keys can have the same routingHash, so `hk` and `sk` keep the items apart.

Each shard group has its own seed scope (`fokosDBSeedScopeOf`). Thus the partition, the GSI and the materialization DO compute different routingHashes for the same hash key.

### WITHOUT ROWID or rowid

A WITHOUT ROWID table stores its rows in primary-key order. A delete of a key range then frees whole pages. A rowid table keeps its pages in insert order, and a delete of a key range does not free them.

The cost: a WITHOUT ROWID table stores its rows in an index B-tree. On a 4 KiB page, the inline payload limit of an index B-tree is approximately 1002 bytes. A row above that limit gets its own overflow pages, and no other row can use their free space. Measurements on DO storage:

- Below approximately 900 bytes of data: 1.34 to 1.41 physical bytes for each logical byte with WITHOUT ROWID, and 1.52 to 2.23 with rowid.
- From 1 KB to 2 KB of data: 2.3 to 4.4 with WITHOUT ROWID, and 1.42 to 1.45 with rowid.

A rowid table with a primary key also writes a second B-tree entry for each insert, and DO billing counts each row written. Each table below states which cost it accepts.

### `data` is the last column

SQLite stores the primary-key columns first and the other columns in declared order. Thus a read of the metadata columns, for example `item_bytes`, stops before `data` and reads no overflow page.

### `item_id` links the other tables to an item

A large key is stored one time, in the item table. Other tables link to the item through the local integer `item_id`. A counter assigns it, and the counter only increases, so a deleted ID never comes back. A migration copies `item_id` without a change. Never send `item_id` through the API, in a cursor, or in a transaction row.

## Partition: `FokosDBPartitionDO`

The schema is in `shared/partition/partition-store.ts` and `shared/partition/item-stats.ts`. The same tables hold the items of a table and the entries of a GSI.

### `items`

```sql
PRIMARY KEY (routing_hash, hk, sk)   -- WITHOUT ROWID
UNIQUE INDEX (item_id)
INDEX (ttl_epoch_utc_seconds, routing_hash, hk, sk)
```

One row for each item, with its committed state only.

- WITHOUT ROWID, because this is the long-lived data of the partition, and a split must free the pages of the rows that it moves. The table accepts the overflow cost of large items.
- A query on one hash key seeks `(routing_hash, hk)` and reads `sk` in order.
- In a GSI partition, `hk` is the GSI hash key. `sk` is the physical GSI sort key: a tag byte, the escaped GSI sort key, the base hash key, the base sort key and a trailer. The base keys make each entry unique for each base item. The GSI sort key comes first, so the entries are in GSI sort-key order.
- No index repeats the order of the table. The size reads, the counts and the split scans read the primary key.

### `pending_transactions`

```sql
PRIMARY KEY (routing_hash, hk, sk, transaction_id)   -- rowid
INDEX (transaction_id, routing_hash, hk, sk)
```

One lock row for each key of a transaction that is in flight.

- Rowid, because the rows hold data and the rows are temporary. The order of the pages is not important for temporary rows. Measured with 2000 rows of 1500 bytes (3.00 MB of data): 9.41 MB with WITHOUT ROWID, and 4.20 MB with rowid.
- The second index exists because `transaction_id` is the last primary-key column, and a seek cannot start on it. Each commit lists the keys of its transaction through this index. The index includes the key columns, so the commit does not read the table.
- Never release the locks of a transaction by its transaction ID alone. That delete also removes the copies of a moved key.

### `pending_tx_info`

```sql
PRIMARY KEY (transaction_id)
INDEX (next_recovery_at) WHERE guarded_at IS NULL
```

One row for each transaction that has a lock here. It stores the facts that are the same for each key of the transaction. The row stays while a lock row of its transaction stays. The partial index gives the deadline and the batch of the stale-transaction job.

### `item_stats`

```sql
PRIMARY KEY (routing_hash, lower_hk, lower_sk)   -- WITHOUT ROWID
INDEX (promotion_est_bytes)
```

Buckets with the exact sums of the facts of their items. A bucket holds the items from its lower bound up to the bound of the next bucket. A floor seek finds the bucket of a key. The split planner and the promotion lookup read these rows, and do not scan `items`.

### `materialization_outbox`

```sql
outbox_id INTEGER PRIMARY KEY        -- rowid
UNIQUE (routing_hash, hk, sk)
INDEX (next_attempt_at, outbox_id)
```

One row for each changed item that the export has not sent yet. It holds the key and the revision, never the data.

- `outbox_id` gives the order in which the export sends the rows.
- The unique key merges many writes of one item into one row. A writer must keep `outbox_id`: use `INSERT ... ON CONFLICT DO UPDATE`, never `INSERT OR REPLACE`.
- Rowid, because the keys can be some kilobytes.

### Single rows and fences

`item_id_counter`, `deletion_metadata` and `materialization_export_state` have one row each. `ancestor_fences` and `producer_fences` are keyed by a DO ID. None of them holds an item key.

## Transaction coordinator: `FokosDBTransactionCoordinatorDO`

The schema is in `server/do-transaction-coordinator.ts`. The route key is the idempotency token, and `routing_hash` is the routingHash of the token. No table uses `(hk, sk)` as its key. The coordinator does only hash splits, so `routing_hash` alone puts the rows of a split target in one key range.

### `tc_state`

```sql
PRIMARY KEY (routing_hash, transaction_id)   -- WITHOUT ROWID
UNIQUE INDEX (routing_hash, idempotency_token)
INDEX (completed_at) WHERE completed_at IS NOT NULL
INDEX (next_recovery_at) WHERE completed_at IS NULL
```

One row for each transaction: its state, its timestamps and its results.

- The primary key uses the transaction ID, not the token. The token only routes the request, and all the other operations use the transaction ID. A later version can remove the token.
- The routingHash is a function of the token, so the unique index is the same rule as a unique token.
- `transaction_id` is random for each first use of a token. A token that comes back after the idempotency sweep thus starts a new transaction, and an old lock cannot match it.
- A terminal state must always set `completed_at`, because the recovery index depends on it.

### `tc_participants`

```sql
PRIMARY KEY (routing_hash, transaction_id, partition_do_name)   -- WITHOUT ROWID
```

One row for each partition of a transaction, with the outcome of each phase and the stored answer of the prepare.

### `tc_items`

```sql
PRIMARY KEY (routing_hash, transaction_id, op_index)   -- WITHOUT ROWID
```

One row for each operation of a transaction, in request order. More than one operation can have the same `(hk, sk)`, so `op_index` is in the key. Here `hk` and `sk` are the base keys of the item. They are data, not a route key.

- WITHOUT ROWID, with `data`. Each insert writes one row. The terminal transition deletes the rows, so they live only while the transaction is in flight. The keys are small.

### `tc_results`

```sql
PRIMARY KEY (routing_hash, transaction_id, op_index)   -- WITHOUT ROWID
```

The item images of the operations that failed their condition with `all_old`. They are separate rows, so `tc_state` does not grow past the 2 MB row limit. The rows stay until the idempotency sweep, and they are rare.

## Materialization: `FokosDBMaterializationDO`

The schema is in `server/materialization/materialization-store.ts`. The route key is the base `(hashKey, sortKey)`, in the materialization shard group. Thus a split of the base table does not move rows here.

### `latest_items`

```sql
PRIMARY KEY (routing_hash, base_hk, base_sk)   -- WITHOUT ROWID
UNIQUE (item_id)
INDEX (deleted_at_ms, item_id) WHERE deleted_at_ms IS NOT NULL
```

One row for each base item: its newest live image, or a tombstone. It has the same layout as `items`, for the same reasons.

### `item_stats`

The same table as in the partition, from the same `ItemStats` class.

### `change_positions`

```sql
seq INTEGER PRIMARY KEY   -- rowid
```

The positions that all the targets read in `seq` order. A row links to its item through `item_id`, and `prev_seq` links to the older position of the same item. It has no secondary index.

### `target_retry`

```sql
PRIMARY KEY (target_id, item_id)   -- rowid
```

The failed items of each target. A `retry` row sends the latest image after its backoff. A `violation` row records a permanent failure.

### `target_index_keys`

```sql
key_id INTEGER PRIMARY KEY   -- rowid
INDEX (item_id, target_id)
```

One row for each GSI key of an item that a GSI target can hold. The GSI keys have no index, because an index would store keys of some kilobytes two times. The code keeps each key unique.

### Targets and fences

`materialization_targets` is keyed by `target_id`, `producer_fences` by the producer DO ID, and `ancestor_fences` by `(target_id, ancestor_id)`. None of them holds an item key.

## Sharding runtime tables

The schema is in `sharding/sharding-store.ts`. The runtime adds these tables to each DO that hosts it. The host reads none of them.

- `fokos_repartitions` and `fokos_repartition_targets`: the split jobs of this DO and their targets. A target stores its slice as columns: a routingHash, or two range edges of `(hk, sk)`.
- `fokos_route_overrides`, keyed by `routing_hash`: the forward pointer of a routingHash whose data is now in a range tree.
- `fokos_range_hierarchy`, keyed by `(routing_hash, depth, start_hk, start_sk)`: the boundaries of range partitions that this DO learned. The table has a row limit, and the oldest rows go first.
- `fokos_admin_outbox`, keyed by `(event_type, id)`: the completed splits that this DO must report to the admin DO.

## Admin: `FokosDBAutoAdminDO`

The schema is in `sharding/topology-keeper.ts`. The admin DO holds no items. Each table is keyed by `shard_group` first, and the range tables then by `routing_hash`. It stores the reported splits, the hash nodes and the range intervals of each shard group, and the encoded topology that `FokosDBTopologyCache` serves.
