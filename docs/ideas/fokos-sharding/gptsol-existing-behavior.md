# FokosDB sharding: existing behavior and extraction boundary

Status: source audit, not a design specification  
Source revision: `769ec44279fad8dc05d0e628371ae4b667f4874a`  
Scope: the current interaction between `FokosDB`, `PartitionDO`, routing, topology caches, splits, key promotion, migration, transactions, and background work

## 1. Purpose

This document records the behavior that exists before the sharding system is extracted into a reusable package.

This document does not define the new package API. It identifies:

- the current responsibility boundaries;
- the request and lifecycle flows;
- the behavior that a refactor must preserve;
- the application capabilities that the sharding system currently assumes;
- candidate interfaces and hooks that the new package will need;
- current limitations and risks that must not become accidental requirements.

This audit was made from source code and tests. It does not use the existing sharding draft SPEC.

## 2. Main findings

1. The Worker-side router does not know the live split tree. It only selects a root hash partition.
2. Each split parent is the authority for its direct children. There is no active global topology keeper.
3. Requests discover deeper topology by forwarding through partition DOs.
4. Per-DO caches use successful responses to skip known intermediate routers. The caches are optimizations, not authorities.
5. Hash splits, key promotions, and range splits all use child-side pull migration.
6. `PartitionDO` combines sharding and FokosDB behavior manually in each RPC. There is no general request dispatcher.
7. The sharding code depends on FokosDB SQL state. It knows item rows, pending transaction rows, deletion watermarks, per-key size estimates, promoted-key rows, and range hierarchy rows.
8. A reusable package therefore needs more than the files under `shared/partition-topology/`. It needs a sharding runtime plus an application adapter.
9. The application adapter must describe operation routing, local execution, migration data, cutover guards, cleanup, and background work.
10. Some current behavior is incomplete or unsafe. These points are listed separately so the new package does not preserve them by accident.

## 3. Current component map

### 3.1 `FokosDB` in the Worker

`packages/fokosdb/src/client/db.ts` owns the public DynamoDB-like API.

It:

- validates and encodes public keys and values;
- asks `PartitionTopologyRouter` for an initial partition;
- calls a partition DO or transaction coordinator;
- drives two-phase read transactions;
- selects the single-partition transaction fast paths;
- decodes public results;
- removes internal routing hints from response metadata;
- traverses the topology for database destruction.

It does not know whether the selected root has split or whether a key was promoted.

### 3.2 Worker-side topology router

`PartitionTopologyRouterImpl` in `shared/partition-topology/router.ts`:

- hashes the hash key to one of `rootTreesN` root partitions;
- creates the deterministic root partition ID and DO name;
- resolves the Durable Object ID with `idFromName`;
- caches resolved root contexts in memory;
- returns all root contexts for full-tree traversal;
- performs dynamic post-order traversal for destruction.

`findPartition()` contains TODO sections for deeper hash and range routing. In current production behavior, it always returns a root hash partition.

### 3.3 `PartitionDO`

`packages/fokosdb/src/server/do-partition.ts` is both:

- the FokosDB data partition; and
- the sharding runtime for that partition.

It owns:

- partition context initialization and validation;
- topology policy construction;
- point-request forwarding;
- transaction-item grouping and fan-out;
- range-query tree traversal;
- topology cache learning;
- split orchestration;
- child initialization;
- migration serving and driving;
- promotion orchestration;
- shared alarm and timer scheduling;
- FokosDB item, condition, TTL, and transaction behavior.

This class is the main extraction boundary.

### 3.4 Split policy and split state

`shared/partition-topology/split-policy.ts` contains:

- `HashPartitionTopologyImpl`;
- `RangePartitionTopologyImpl`;
- operation-intent and routing decisions;
- child selection;
- split-plan preparation;
- cache learning.

`shared/partition-topology/split-state.ts` contains the persisted split state machine.

These classes do not make RPCs. `PartitionDO` owns stubs and RPC fan-out. However, the policy classes still depend on Durable Object storage and `PartitionStore`.

### 3.5 FokosDB storage adapter

`shared/partition/partition-store.ts` owns application tables and several sharding support tables.

The sharding system uses it for:

- database size;
- item migration scans and ingestion;
- range split boundary calculation;
- pending transaction migration;
- deletion-watermark migration;
- promoted-key state and garbage collection;
- per-hash-key size estimates;
- learned range topology.

This direct dependency prevents a topology-only extraction.

### 3.6 Partially separated lifecycle components

The code already has three useful seams:

- `SplitMigration` receives a `PartitionPeer` and a store.
- `PromotionManager` receives a range-root peer factory, a store, and a scheduler callback.
- `TransactionParticipant` receives a store and an item-upsert callback.

These seams show that remote stub acquisition can stay in the DO while reusable logic receives narrow capabilities.

## 4. Topology authority and identity

### 4.1 Decentralized authority

There is no active topology-keeper DO. Types for one exist in `partition-topology/types.ts`, but no implementation uses them.

The live topology is decentralized:

- the Worker knows all root partitions;
- each split parent persists its own direct child contexts;
- child identities are deterministic;
- deeper topology is discovered by forwarding;
- destruction reads each partition's status to discover children.

A split parent remains a stable routing entry after it splits.

### 4.2 Hash partition identity

A hash partition ID contains:

- schema byte;
- root index;
- absolute hash-tree depth;
- one child index for each depth.

The DO name is deterministic:

- root: `<table>.h.<root-index>`;
- descendant: `<table>.h.<root-index>.<child-index>...`.

The hash function uses the absolute tree depth as entropy. Child selection during normal routing and row filtering during migration use the same function and depth.

### 4.3 Range partition identity

A range partition ID contains:

- schema byte;
- promoted hash key;
- optional start sort-key boundary;
- optional end sort-key boundary.

A range partition owns `[start, end)`. A null start is negative infinity. A null end is positive infinity.

Its DO name is deterministic from the table name, hash key, and both boundaries. Boundaries are immutable for the life of the DO. A range node that splits keeps its identity and becomes a pure router.

### 4.4 Partition context

A resolved partition context is sent with normal application and transaction RPCs. It contains:

- table and namespace identity;
- root and split configuration;
- DO name and ID;
- opaque partition ID;
- optional range identity.

A hash DO can initialize lazily from the first normal request. A range DO cannot. A range DO must be created by `internalInitFromSplit`. A normal request to an uninitialized range DO returns a phantom-bounce error.

The DO persists the context and validates later requests against it.

Current immutable checks cover:

- context schema;
- table name;
- `rootTreesN`;
- `hashSplitN`;
- partition ID;
- DO name;
- range hash key and boundaries.

Current mutable checks cover:

- hash split conditions;
- `rangeSplitN`;
- range split conditions;
- range ancestor configuration.

The context also carries namespace fields because a DO cannot receive constructor configuration through Workers RPC.

Current creator defaults include:

- hash split fan-out 4 and 100 MiB when hash split configuration is absent;
- range split fan-out 4 and 500 MiB when range split conditions are absent;
- range ancestor selection `{ fromRoot: 0, fromLeaf: 3 }`.

`rootTreesN` and `hashSplitN` cannot change after initialization. `rangeSplitN` and the policy thresholds are treated as mutable configuration for later work.

## 5. Persisted and in-memory state

| State                                     | Location                        | Purpose                                                          |
| ----------------------------------------- | ------------------------------- | ---------------------------------------------------------------- |
| Partition context                         | KV `__partition_context`        | Partition identity and current table policy                      |
| Range depth                               | KV `__partition_depth`          | Range-tree depth                                                 |
| Parent context                            | KV `__parent_partition_context` | Migration source                                                 |
| Parent split type                         | KV `__parent_split_type`        | Child origin                                                     |
| Split state                               | KV `__split_status`             | `queued`, `started`, `completed`, children, and acknowledgements |
| Migration state                           | KV `__split_migration_status`   | `initialized`, `migrating`, or `completed`                       |
| Item migration cursor                     | KV `__split_migration_cursor`   | Crash resume for the item stream                                 |
| Hash topology cache                       | KV `__topo_cache`               | Known deeper hash paths                                          |
| Descendant-promotion Bloom filter         | KV `__partial_range_topology`   | Known keys promoted by hash descendants                          |
| Items                                     | SQL `items`                     | FokosDB committed data and migration source                      |
| Pending transaction state                 | SQL `pending_transactions`      | Locks and prepared payloads                                      |
| Deletion watermark                        | SQL `deletion_metadata`         | Transaction conflict ordering                                    |
| Per-key size                              | SQL `key_size_estimates`        | Promotion and range split calculations                           |
| Promotion state                           | SQL `promoted_keys`             | `queued`, `promoting`, `promoted`, and GC state                  |
| Range topology cache                      | SQL `range_hierarchy`           | Ancestor and learned descendant boundaries                       |
| Current context, parent, topology objects | Memory                          | Hot request state                                                |
| Range ancestors                           | Memory                          | Response routing hints                                           |
| Depth and colo data                       | Memory                          | Routing metadata and telemetry                                   |
| Background timer state                    | Memory                          | Best-effort fast scheduling                                      |

The application and sharding state share the same Durable Object storage and transaction API.

## 6. Initial request routing

For point operations, the path starts as follows:

1. `FokosDB` validates and encodes the key.
2. The Worker router hashes the hash key to a root index.
3. The Worker calls that root hash DO with a resolved context.
4. The root validates or stores the context.
5. The root checks migration state.
6. The root applies promotion routing.
7. The root asks its topology policy whether to handle, forward, or reject.
8. A split parent forwards to one child.
9. Each child repeats the same flow.
10. The serving leaf performs local application work.
11. Response metadata identifies the serving leaf and carries routing hints.
12. Each forwarding parent updates its local cache and increments `forwardCount`.
13. `FokosDB` removes internal hints before it returns the public result.

There is no redirect response to the Worker. Forwarding remains inside the partition tree.

## 7. Request eligibility and routing decisions

### 7.1 Partition lifecycle gate

| State                         | Point read          | Query read               | Growing write          | Delete  | Transaction prepare    | Commit or cancel  | Migration protocol                        |
| ----------------------------- | ------------------- | ------------------------ | ---------------------- | ------- | ---------------------- | ----------------- | ----------------------------------------- |
| Active leaf                   | Local               | Local                    | Local                  | Local   | Local                  | Local             | Only when authorized by another lifecycle |
| `split_queued` parent         | Local               | Local                    | Local unless over size | Local   | Local unless over size | Local             | Children can be initialized               |
| `split_started` parent        | Forward             | Forward or range fan-out | Forward                | Forward | Group and forward      | Group and forward | Serves child pulls and acks               |
| `split_completed` parent      | Forward             | Forward or range fan-out | Forward                | Forward | Group and forward      | Group and forward | Still serves late child pulls             |
| `migration_initialized` child | Parent read-through | Parent read-through      | Reject                 | Reject  | Reject                 | Reject            | Can start migration                       |
| `migration_migrating` child   | Parent read-through | Parent read-through      | Reject                 | Reject  | Reject                 | Reject            | Pulls from parent                         |
| `migration_completed` child   | Normal              | Normal                   | Normal                 | Normal  | Normal                 | Normal            | No more migration work                    |

`txReadSnapshot`, `txExecuteSingleShot`, debug resolution, and debug promotion also reject while the target is migrating.

### 7.2 Size backpressure

Only an operation with intent `write` can receive size backpressure.

The current rules are:

- queue a split above 100% of `maxSizeMb`;
- measure the full Durable Object SQLite database, not only application item payloads;
- reject a growing write above 110% of `maxSizeMb`;
- continue to serve reads;
- continue to serve deletes;
- continue to serve commit and cancel;
- continue to serve transaction reads.

Commit uses `ignore_size_reject` because prepare already stored the payload. A refused commit could stop a transaction after the commit decision.

`maxItems` is validated and stored, but it does not take part in split decisions.

### 7.3 Ownership checks

A range leaf verifies that a sort key is inside its immutable `[start, end)` range. An out-of-range item is a routing error, not backpressure.

A hash leaf does not verify that the hash key belongs to its encoded path. Correct callers and parent routing are the contract.

A range leaf also relies on callers to send the range structure's hash key. Its current `shouldAllow` check only validates the sort-key range.

## 8. Routing cache behavior

### 8.1 Worker root-context cache

The Worker router caches resolved root contexts by root index. It does not learn splits or promotions.

### 8.2 Hash topology cache

Each split hash parent has a persisted `HashTopology` arena.

Behavior:

1. A cold parent forwards to the immediate child.
2. The final response reports the serving hash depth.
3. The parent records the deeper path for that hash key.
4. A later request can resolve a deterministic descendant and skip intermediate hash routers.
5. If that descendant later splits, it forwards again.
6. The response reports the new depth, and the ancestor extends its cache.

The cache has a memory budget and depth cap. A stale entry is safe because a former leaf remains present as a router.

### 8.3 Range hierarchy cache

Range leaves return a bounded set of ancestor boundaries in internal response metadata. Forwarding hash and range nodes write these boundaries to `range_hierarchy`.

A later point request can resolve the deepest known range slice that contains its sort key and jump directly to that DO.

The set returned by a new child is selected from:

- the shallowest configured ancestors from the root side; and
- the deepest configured ancestors from the leaf side.

The default is `{ fromRoot: 0, fromLeaf: 3 }`.

A range boundary is immutable. A stale target is therefore still a valid node. It can forward if it has split further.

### 8.4 Partial range topology

A hash ancestor can learn that a descendant promoted a hash key. It stores the key in a persisted Bloom filter.

On a later point request:

1. the ancestor tests the Bloom filter;
2. a probable match tries the global range root directly;
3. a real promotion succeeds and skips the hash tree;
4. a false positive reaches an uninitialized range root;
5. the range root returns a phantom-bounce error;
6. the ancestor falls back to normal hash routing.

This cache can fill. When full, the request remains correct but no new promoted keys can be learned by that cache.

### 8.5 Routing hints in responses

Local item responses include:

- serving actor ID and name;
- serving partition ID;
- hash depth;
- range depth;
- forward count;
- internal range ancestor hints.

Forwarders preserve the serving leaf identity and increment only the forwarding count. `FokosDB.publicMeta()` removes `_internal` before the public response.

Transaction prepare, commit, cancel, and transaction-read responses do not carry these hints. Transaction paths can use an already-warm cache, but they do not learn a new path from their own responses. Transaction grouping also does not use the partial-range Bloom filter.

## 9. Hash split lifecycle

### 9.1 Queue

A successful local put, committed transaction with local writes, or successful single-shot write checks split conditions.

For a hash leaf:

- SQL database size must be above `hashSplitConditions.maxSizeMb`;
- no key can be in `queued` or `promoting` promotion state.

The split state moves from no record to `split_queued`. Repeated queue attempts are idempotent. The parent still owns and serves its data in this state.

The caller schedules both:

- a short in-memory background timer; and
- a fallback alarm.

### 9.2 Prepare children

The background job calculates `hashSplitN` deterministic children.

It calls `internalInitFromSplit` on all children in parallel. Each child call retries up to five times.

A child persists, in one synchronous storage transaction:

- its context;
- its parent context;
- parent split type `hash`;
- migration state `migration_initialized`.

Initialization is idempotent only when the child, parent, and split type agree. A conflicting retry fails.

### 9.3 Cut over parent routing

The parent changes to `split_started` only after every child initialization succeeds.

The persisted record contains the exact child contexts and an empty acknowledgement list. From this point, the parent is a pure router for application and transaction requests.

The parent then asks all children to start migration. These trigger calls are best effort and use `Promise.allSettled`.

### 9.4 Complete

Each child pulls and imports its data. It then acknowledges by DO name.

The parent adds each acknowledgement idempotently. When all children have acknowledged, it changes to `split_completed`.

In the same local storage transaction, the parent deletes all local pending transaction rows. The children now own authoritative lock copies.

The parent does not delete its local item rows after a hash split. It keeps them as a migration source and redundant data.

## 10. Range split lifecycle

A promoted hash key owns an independent range tree. Each range leaf has one hash key and one immutable sort-key interval.

### 10.1 Queue

A range leaf queues a range split when its SQL database size is above `rangeSplitConditions.maxSizeMb`.

### 10.2 Select boundaries

The parent calculates `rangeSplitN - 1` boundaries from application rows.

Current FokosDB behavior:

- use the maintained byte estimate for the hash key;
- scan rows in sort-key order;
- select byte-quantile crossing points;
- shorten each boundary to a separating prefix;
- require strictly increasing boundaries;
- require enough rows to give every child at least one row.

If valid boundaries cannot be made, the split remains queued and a later background cycle retries.

### 10.3 Create children

The children tile the parent's complete `[start, end)` interval. The parent does not retain the leftmost range. Every child is a new DO.

The child initialization also stores:

- range depth;
- a bounded range ancestor set.

### 10.4 Route and migrate

After all child initializations, the range parent changes to `split_started` and becomes a pure router.

Point operations select one child by sort key. A query can visit all intersecting children in sort order.

Each child pulls only the rows and pending transaction state in its interval. The parent changes to `split_completed` after all child acknowledgements.

As with a hash split, parent item rows are not deleted after completion.

## 11. Hash-key promotion lifecycle

Promotion moves all sort keys of one hash key from a hash leaf to an independent range tree.

### 11.1 Detection and queue

`PartitionStore` maintains an estimated total size for each hash key.

A successful put or committed transactional put or update checks the estimate. A hash key becomes a candidate at:

`hashSplitConditions.maxSizeMb * 0.25`

The promotion row changes from absent to `queued`. The queue operation schedules background work. A debug RPC can also queue a key directly.

A `queued` key still uses the hash leaf.

### 11.2 Cutover guard

The promotion background job:

1. stops if the hash partition has a split in `split_queued` or `split_started`;
2. resolves and initializes the global range root for the hash key;
3. checks the hash key for pending transaction locks in a synchronous storage transaction;
4. leaves the key `queued` if a lock exists;
5. changes `queued` to `promoting` if no lock exists;
6. asks the range root to start migration.

The lock-free transition is the cutover. New operations for the key now route to the range root.

### 11.3 Behavior during promotion migration

The range root starts in migration state.

- Reads sent to the range root read through to the hash parent with a direct local-read RPC.
- Writes and transaction operations sent to the range root reject until migration completes.
- The hash parent authorizes migration only while the key is `promoting`.
- No pending lock rows are expected for the key because cutover required zero locks.
- The deletion watermark is still copied.

### 11.4 Completion and source cleanup

The range root marks its migration complete and acknowledges the hash parent by hash key.

The parent changes `promoting` to `promoted` and schedules garbage collection.

Garbage collection:

- deletes source item rows in bounded batches;
- deletes pending rows for the promoted key;
- deletes the source key-size estimate when no source item remains;
- marks promotion GC complete.

The `promoted` row remains as the authoritative forwarding pointer.

### 11.5 Promotion and hash split interaction

The intended mutual exclusion is:

- `queued` or `promoting` promotion blocks a new hash split;
- `split_queued` or `split_started` blocks promotion cutover;
- a completed promotion can be inherited by the hash child that owns its hash key during a later hash split.

Hash child migration excludes all item rows for keys present in `promoted_keys`. It separately copies the forwarding-pointer rows to the correct hash child.

## 12. Migration protocol

### 12.1 General model

Migration is a child-side pull protocol.

The source remains available through direct local reads. The destination rejects normal mutations until it has a complete copy.

The protocol has three persisted child states:

- `migration_initialized`;
- `migration_migrating`;
- `migration_completed`.

`ensureMigration` changes `initialized` to `migrating` and sets a fallback alarm.

### 12.2 Item stream

The child asks the parent for batches with a keyset cursor.

Current limits are:

- about 20 MiB per response;
- 1,000-row source pages.

The child prefetches the next batch while it imports the current batch. It uses `INSERT OR IGNORE`, then checkpoints the item cursor after each batch.

A crash can resume strictly after the last persisted cursor. Re-import of a batch is idempotent.

### 12.3 Application metadata stream

After items, the child copies FokosDB transaction metadata:

- pending transaction rows that belong to the child;
- the partition deletion watermark.

A hash child also copies promoted-key forwarding rows that belong to it.

Only the item cursor is persisted. Metadata streams restart from the beginning after a crash. Their imports are idempotent or monotonic.

### 12.4 Source authorization and filtering

A parent does not accept an arbitrary migration read.

For hash split children, it requires:

- parent split state `split_started` or `split_completed`;
- a known child context;
- the same hash child selection function used for routing.

For range split children, it requires:

- parent split state `split_started` or `split_completed`;
- a known child context;
- filtering to the child's immutable interval.

For a promotion root, it requires:

- a hash parent; and
- promotion state `promoting` for that hash key.

### 12.5 Finalization

After all input streams, the child:

1. rebuilds per-key size estimates;
2. writes `migration_completed`;
3. deletes the item cursor;
4. acknowledges the parent;
5. arms TTL processing after the background driver observes completion.

Hash and range split children acknowledge by child DO name. A promotion root acknowledges by hash key.

## 13. Query routing

`queryItems` is not a point-operation fan-out.

The Worker selects a root hash partition by hash key and an empty sort-key sentinel.

### 13.1 Hash side

The hash tree routes the complete hash key to one hash leaf. If the key is promoted, routing enters the range root.

### 13.2 Range side

A non-split range leaf scans locally.

A split range node:

- selects children whose intervals intersect the query interval;
- visits them in ascending or descending order;
- clips the query interval to each child;
- passes the remaining byte, item, and partition-visit budgets;
- combines items and leaf-only metrics;
- creates a continuation cursor when more candidate children remain.

Range routers do not appear in `partitionMetas`. Their RPC fan-out is included in `forwardCount`.

### 13.3 Query during migration

A migrating child calls `internalQueryItemsDirect` on its parent. This RPC must read the parent's local rows only. It must not traverse the parent's children, because that would route back to the migrating child and make a loop.

## 14. Transaction interaction with sharding

### 14.1 Initial grouping

The client computes a root partition context for every transaction item. It stores that context in each transaction operation.

For write transactions, the transaction coordinator persists one participant per distinct root DO name. It keeps using that stable root context for prepare, commit, cancel, and recovery.

A root that has split forwards the transaction internally. The transaction coordinator does not need its participant list changed when the partition tree changes.

### 14.2 Multi-destination participant routing

`groupItemsByRouting` divides a request into:

- items owned locally; and
- item groups for child or range-root destinations.

It is used by:

- prepare;
- commit;
- cancel;
- transaction reads;
- stale transaction recovery.

A single root RPC can therefore fan out to local work and several children.

### 14.3 Prepare

Prepare:

- rejects on a migrating child;
- applies growing-write size backpressure;
- routes promoted keys to their range root;
- routes split keys to children;
- prepares all local and remote groups;
- merges per-operation rejection results;
- sets a stale-transaction alarm for accepted local locks.

A partial prepare can leave locks in groups that accepted. The coordinator sends cancel to all possible participants.

### 14.4 Commit

Commit:

- rejects on a migrating child so the coordinator retries later;
- ignores size backpressure;
- routes every key to its current owner;
- applies local pending payloads;
- fails if any child commit fails;
- checks split conditions after successful local writes.

This lets a lock prepared before a split migrate to a child and commit there after the split.

### 14.5 Cancel

Cancel:

- rejects on a migrating child;
- deletes the local transaction rows first;
- routes keys to current owners;
- attempts every child cancel with `Promise.allSettled`;
- throws after the fan-out if any child failed.

The coordinator keeps the transaction non-terminal and retries the failed destinations. Cancel routing remains active for both `split_started` and `split_completed` parents.

### 14.6 Transaction reads

The Worker drives a two-phase read for multi-partition requests. Each root `txReadForTransaction` can fan out again inside the current split tree.

The single-partition read fast path uses `txReadSnapshot`. The server accepts it only if one DO can handle every item. It forwards the complete request to one child if needed. If items require local plus remote work or more than one destination, it returns a fallback error without reading.

### 14.7 Single-shot writes

The single-partition write fast path uses `txExecuteSingleShot`.

The server uses the same one-destination rule as snapshot reads. A successful local operation applies all items in one synchronous storage transaction. It creates no transaction locks. It checks split conditions once after the transaction.

### 14.8 Stale transaction recovery

A leaf with old unguarded locks asks the stored transaction coordinator ID for the outcome.

- `COMMITTED`: call public `txCommit` so current routing and migration gates apply.
- `CANCELLED`: call public `txCancel`.
- `not_found` and all keys moved away: delete the redundant local rows.
- `not_found` inside the idempotency window: cancel.
- old `not_found` lock still owned here: quarantine it for operator resolution.

Split parents and migrating children do not run this sweep because their lock state is not authoritative.

## 15. Complete request interaction matrix

All RPC methods below, except `alarm`, pass through the private `#rpc` wrapper. The wrapper currently arms TTL work before it runs the method.

| RPC                                        | Context init or validation                   | Migration behavior                              | Split or promotion behavior                        | Local or post-request behavior                                        |
| ------------------------------------------ | -------------------------------------------- | ----------------------------------------------- | -------------------------------------------------- | --------------------------------------------------------------------- |
| `internalInitFromSplit`                    | Initializes only; checks idempotent retry    | Writes `migration_initialized`                  | No application routing                             | Stores parent and range metadata                                      |
| `status`                                   | Optional context can initialize a hash root  | No migration gate                               | Reads split and promotion state only               | Returns local debug state                                             |
| `internalTriggerMigration`                 | Requires stored context                      | Starts or confirms migration and schedules work | No application routing                             | Best-effort fast trigger                                              |
| `apiPutItem`                               | Validates context                            | Rejects while migrating                         | Promotion, Bloom, and split point routing          | Local lock and condition checks; upsert; promotion check; split check |
| `apiDeleteItem`                            | Validates context                            | Rejects while migrating                         | Promotion, Bloom, and split point routing          | Local lock and condition checks; delete; no split check               |
| `apiGetItem`                               | Validates context                            | Reads parent directly while migrating           | Promotion, Bloom, and split point routing          | Local item read                                                       |
| `internalGetItemDirect`                    | Requires stored context                      | Bypasses migration gate                         | Bypasses all routing                               | Reads source local storage                                            |
| `apiQueryItems`                            | Validates context                            | Reads parent directly while migrating           | Hash point routing or ordered range-tree traversal | Local bounded scan at leaves                                          |
| `internalQueryItemsDirect`                 | Requires stored context                      | Bypasses migration gate                         | Bypasses all routing                               | Scans source local storage only                                       |
| `migrationGetItemsBatch`                   | Requires stored context                      | Protocol method is not gated                    | Authorizes promotion or split child                | Returns filtered source rows                                          |
| `migrationGetPartitionTransactionMetadata` | Requires stored context                      | Protocol method is not gated                    | Authorizes promotion or split child                | Returns locks and watermark                                           |
| `migrationGetPromotedKeysBatch`            | Requires stored hash context                 | Protocol method is not gated                    | Filters rows for a hash child                      | Returns promotion pointers                                            |
| `migrationAcknowledgeChildComplete`        | Requires stored context                      | Protocol method is not gated                    | Advances split acknowledgements                    | Deletes all parent locks at full completion                           |
| `migrationAcknowledgePromotionComplete`    | Requires stored hash context                 | Protocol method is not gated                    | Advances promotion state                           | Schedules source GC                                                   |
| `txPrepare`                                | Validates context                            | Rejects while migrating                         | Groups local, split-child, and promoted-key work   | Creates local locks and alarm; merges results                         |
| `txCommit`                                 | Validates context                            | Rejects while migrating                         | Groups local, split-child, and promoted-key work   | Applies locks; checks local split after commit                        |
| `txCancel`                                 | Validates context                            | Rejects while migrating                         | Groups split-child and promoted-key work           | Clears local locks first; attempts all children                       |
| `txReadForTransaction`                     | Validates context                            | Rejects while migrating                         | Groups local, split-child, and promoted-key work   | Reads committed rows and lock flags                                   |
| `txReadSnapshot`                           | Validates context                            | Rejects while migrating                         | Requires one final destination                     | Reads one local consistent snapshot or returns fallback               |
| `txExecuteSingleShot`                      | Validates context                            | Rejects while migrating                         | Requires one final destination                     | Applies atomically without locks; checks split                        |
| `debugForceResolveTransaction`             | Validates context                            | Rejects while migrating                         | Calls public commit or cancel for current routing  | Clears quarantine guard                                               |
| `debugForcePromoteKey`                     | Validates context                            | Rejects while migrating                         | Does not route to a split child                    | Queues promotion on the called DO                                     |
| `destroyPartition`                         | No context requirement                       | No migration gate                               | Caller must traverse children first                | Stops timers, deletes alarm and storage, aborts instance              |
| `alarm`                                    | Requires stored context in background runner | Drives migration first                          | Drives queued split and promotion                  | Drives stale transactions and computes next alarm                     |

## 16. Shared background-work lifecycle

One partition alarm and one in-memory scheduling mechanism drive several independent jobs.

The current order is:

1. child migration;
2. parent split start;
3. stale transaction recovery;
4. promotion drive;
5. promotion source GC.

TTL uses a separate in-memory timer, not the partition alarm.

After a background pass, the DO calculates the earliest needed alarm for:

- incomplete migration;
- queued split;
- promotion or promotion GC;
- stale transaction recovery.

It also schedules a short in-memory run for fast progress.

Every background job is expected to be:

- idempotent;
- safe when runs overlap;
- resumable after a crash;
- isolated so one job failure does not permanently stop other work.

## 17. Behavior that the extraction must preserve

### 17.1 Routing invariants

- A stable root entry must remain usable after any number of splits.
- A split parent must never handle application data locally after cutover.
- Child selection and migration filtering must use the same ownership function.
- A range interval must have one owner and use `[start, end)` semantics.
- Cached routes must be hints. A stale hint must not permit work on the wrong owner.
- Internal direct-source reads must bypass forwarding.

### 17.2 Split invariants

- The parent owns the data until all children initialize and cutover is durable.
- The parent persists `split_started` before it treats children as owners.
- Child initialization and acknowledgements are idempotent.
- All children must acknowledge before `split_completed`.
- Request behavior must be correct in both `split_started` and `split_completed`.

### 17.3 Migration invariants

- Normal writes must not modify an incomplete destination.
- Reads must remain available through the complete source when configured to do so.
- Migration batches must be bounded.
- Cursor resume and repeated import must not lose or overwrite data incorrectly.
- Application metadata required for correctness must migrate with data.
- Destination finalization must run before normal local operations are accepted.
- Parent acknowledgement and source cleanup must be retryable.

### 17.4 Transaction invariants

- Prepare, commit, cancel, and reads must route to current owners.
- A commit decision must not be blocked by size backpressure.
- Cancel must try all destinations and report partial transport failure.
- Locks prepared before a split must migrate to the new owner.
- Recovery must call the normal routed operation instead of bypassing sharding.
- Promotion cutover must not move a key while it has local transaction locks.

### 17.5 Availability invariants

- Reads and deletes remain available on an over-size leaf.
- A migrating child can serve configured read-through operations.
- Best-effort caches can fail or fill without changing correctness.
- Failed background work must retain enough durable state for a later retry.

## 18. Required extraction boundary

The current code implies two main sides. This is a capability boundary, not a final API proposal.

### 18.1 Sharding runtime responsibilities

The reusable sharding package should own:

- partition identity and deterministic child resolution;
- root selection;
- split and migration lifecycle state;
- routing decisions and forwarding orchestration;
- topology caches and routing hints;
- child initialization protocol;
- migration scheduling and retry;
- child acknowledgements;
- request-state gates;
- generic background-work scheduling;
- topology traversal for administration;
- routing metrics such as forwarding count and serving partition identity.

### 18.2 Application partition responsibilities

The custom Durable Object should retain:

- its complete Durable Object state and storage access;
- its public RPC surface;
- application request and response types;
- key extraction from each request;
- local operation semantics;
- application transactions and locks;
- application-specific migration data;
- application-specific split metrics and policy inputs;
- application-specific cutover guards;
- source cleanup;
- application background jobs such as FokosDB TTL and stale transaction recovery.

The application must not be forced to use `PartitionStore` or the FokosDB SQL schema.

## 19. Operation shapes the runtime must support

A single `route(request)` function is not sufficient. Current FokosDB uses at least five operation shapes.

### 19.1 One-key, one-destination operation

Examples:

- put;
- get;
- delete.

The runtime can handle locally or forward to one destination. It can learn from response metadata.

### 19.2 Multi-key grouped fan-out

Examples:

- transaction prepare;
- commit;
- cancel;
- transaction read.

The runtime groups items by destination, can also keep a local group, runs groups, and lets the application merge responses.

### 19.3 Multi-key single-owner operation

Examples:

- single-shot write;
- single-partition snapshot read.

Every item must resolve to one final DO. Otherwise, the operation returns a no-side-effect fallback.

### 19.4 Ordered range traversal

Example:

- `queryItems` over a split range tree.

The runtime must support ordered child selection, interval clipping, response combination, budgets, and continuation cursors.

### 19.5 Local-only control operation

Examples:

- source reads used by migration;
- status;
- migration batch serving;
- acknowledgements;
- destroy.

These operations must bypass normal forwarding and use explicit authorization.

## 20. Required application capabilities

The new package will need an application adapter with capabilities equivalent to the following groups.

### 20.1 Request description

For each operation, the application must provide:

- operation name;
- operation shape;
- routing keys or item list;
- operation intent, such as read, growing write, shrinking write, or decided write;
- behavior while destination migration is incomplete;
- local handler;
- remote invocation function;
- response merge function when fan-out is allowed.

### 20.2 Request lifecycle hooks

The current flows require hooks at these points:

- before context initialization;
- after context initialization;
- before every request;
- after the migration gate;
- before local execution;
- after successful local commit;
- after a forwarded response;
- after the complete request;
- on request error.

The contract must state which hooks can mutate storage, which hooks are awaited, and which hooks must not change the application outcome.

`after successful local commit` is necessary for current FokosDB promotion checks and split checks. A general `after request` hook alone is too late and cannot tell whether local durable state changed.

### 20.3 Request eligibility policy

The application must be able to add a fast decision before local work:

- allow;
- forward by topology;
- reject with retryable backpressure;
- reject as invalid routing;
- use parent read-through while migrating.

Inputs can include:

- lifecycle state;
- operation intent;
- route key;
- application load and size signals;
- application health;
- custom maintenance state.

### 20.4 Split policy

The application must be able to define:

- whether a partition can queue a split;
- which split strategy to use;
- split fan-out;
- split-plan inputs;
- child boundaries or ownership descriptors;
- whether another lifecycle blocks the split;
- whether a queued split can start now;
- application work after split start and completion.

The package must not assume SQL database size or byte-quantile range boundaries.

### 20.5 Migration adapter

The generic package needs one bounded and resumable migration-page operation. The application must provide:

- an opaque cursor type;
- a source callback that returns one bounded payload and the next opaque cursor;
- a destination callback that imports the payload idempotently;
- a destination finalization callback;
- optional source cleanup after acknowledgement.

The sharding runtime repeats the same operation until the application reports completion. It persists the returned cursor only after the destination applies the payload. If apply succeeds but cursor persistence does not, the same page can arrive again, so import must be idempotent.

FokosDB can encode its current item, transaction-metadata, deletion-watermark, and promoted-key stages inside its cursor and payload. The generic package does not need to list or understand these stages. It must not know FokosDB row types.

### 20.6 Cutover and cleanup hooks

The application must be able to veto or defer a cutover. FokosDB uses this to require zero pending locks before key promotion.

The application also needs hooks for:

- before cutover;
- after durable cutover;
- after destination migration;
- after parent acknowledgement;
- bounded source cleanup;
- cleanup-complete detection.

### 20.7 Routing hint adapter

The runtime needs a generic way to:

- attach serving-partition information to a local response;
- extract a routing hint from a forwarded response;
- increment forwarding metrics;
- keep private hints out of the public application result.

This can be a response envelope. It should not require every application response type to contain FokosDB `meta` fields.

### 20.8 Background job registration

The application needs to register background jobs with:

- a stable job name;
- a `canRun` guard;
- an idempotent runner;
- a `needsWork` check;
- a next-run deadline;
- error isolation policy;
- optional ordering constraints against sharding jobs.

This is needed because FokosDB currently shares scheduling between migration, splits, promotions, stale transactions, and separate TTL work.

## 21. Ordering contracts for hooks

The new boundary must make these order rules explicit:

1. Validate or initialize partition identity before application routing.
2. Check incomplete migration before local application access.
3. Resolve authoritative promotion before normal split-child routing.
4. Persist child initialization before parent cutover.
5. Persist parent cutover before forwarding new mutations to children.
6. Import and finalize all required application state before a child accepts local mutations.
7. Run split or promotion detection only after a successful local mutation.
8. Do not let a post-mutation policy failure undo an already-decided transaction.
9. Apply parent completion cleanup atomically with, or safely after, the completion transition.
10. Learn caches only after a successful forwarded response.
11. Keep control and migration RPCs out of normal request forwarding.

## 22. Current overridable hooks

`PartitionDO` currently exposes only these subclass hooks:

- `fokosStaleTransactionMs()`;
- `fokosGetColoInfo()`;
- `fokosTtlConfig()`.

Split conditions are data in `PartitionContext`, not subclass hooks. The topology class construction, request gates, migration behavior, forwarding order, split planning, promotion policy, and background-job order are private or fixed.

The new package needs explicit composition points. Subclassing alone cannot provide the requested policy control without making many private methods protected.

## 23. Generalizations and simplifications for a reusable runtime

The current implementation has separate code paths because it implements concrete FokosDB behavior directly. The reusable package can use fewer primitives and let the custom Durable Object encode its detailed behavior behind callbacks.

This simplification has one limit: application-specific data stages can become opaque, but durable ownership and recovery stages must remain explicit in the sharding runtime.

### 23.1 Summary of possible simplifications

| Current FokosDB mechanism                                           | Smaller generic mechanism                                     |
| ------------------------------------------------------------------- | ------------------------------------------------------------- |
| Separate item, transaction-metadata, and promoted-key migrations    | One opaque, cursor-based migration-page loop                  |
| Separate direct point-read and query-read migration RPCs            | One operation-defined source fallback callback                |
| Separate hash split, range split, and promotion orchestrators       | One repartition plan with pluggable ownership strategies      |
| `withSplitForwarding`, `groupItemsByRouting`, and single-shot route | One owner resolver plus grouping helpers                      |
| Fixed read, write, delete, and commit intents                       | Application-defined admission tag or callback                 |
| FokosDB `meta` as the route-hint carrier                            | A generic transport envelope around any application response  |
| Hash arena, range hierarchy, and promotion Bloom logic in the DO    | Pluggable cache strategy behind one lookup and learn contract |
| SQL database-size split rules                                       | Application-defined split policy and split-plan builder       |
| Fixed migration, split, promotion, and transaction job order        | One resumable job scheduler with declared dependencies        |
| Special traversal of split children and promoted range roots        | One list of outgoing topology links                           |
| Full table policy in every request context                          | Stored immutable identity plus versioned mutable policy input |

### 23.2 A small generic control plane

The generic runtime only needs to coordinate these actions:

1. resolve the current owner for a routing key;
2. initialize one or more target partitions;
3. persist a routing cutover;
4. drive an application-defined migration loop;
5. receive and retry target acknowledgements;
6. run optional source cleanup;
7. schedule unfinished work.

It does not need to know why a split started, which tables contain data, how a transaction lock works, or which application fields must move.

### 23.3 One opaque migration-page loop

The three current FokosDB migration streams can become one application operation.

The runtime can use this loop:

1. load the destination's opaque migration cursor;
2. call the source adapter with the repartition plan, destination identity, cursor, and page budget;
3. receive an opaque payload and either a next cursor or a completion marker;
4. call the destination adapter to apply the payload;
5. persist the next cursor after the apply succeeds;
6. repeat until complete;
7. call the destination finalizer;
8. durably retry the parent acknowledgement until it succeeds.

The custom FokosDB adapter can use a cursor such as `{ phase: "items", cursor: ... }`, then `{ phase: "transactions", cursor: ... }`, and then `{ phase: "promotedKeys", cursor: ... }`. This structure is private to FokosDB. Another application can use one table, many tables, a storage API, or no stored records at all without changing the runtime.

The payload can also contain several application record kinds in one page. The package must not require one RPC method or one persisted cursor per kind.

The migration request can carry a runtime-owned repartition ID and destination ID. The runtime validates that the repartition is active and that the destination belongs to it before it calls the source adapter. The source adapter therefore does not need separate authorization branches for hash children, range children, and promoted keys.

The import callback must be idempotent. If the import succeeds and the cursor checkpoint fails, the runtime will request and apply the same page again.

### 23.4 Keep minimum durable handoff stages

The generic data loop can be one operation, but these control-plane facts cannot be only fields in an opaque application cursor:

- whether the destination is incomplete and must reject local mutations;
- whether routing has cut over from the source;
- whether destination finalization completed;
- whether the parent acknowledgement is still pending;
- whether source cleanup remains.

The runtime needs these facts to route requests and recover after a crash. It can use fewer state names than FokosDB, but it must make the ownership handoff and acknowledgement retry durable.

### 23.5 One resumable work primitive

Migration copy and source cleanup can use the same internal scheduling primitive:

- load an opaque cursor;
- run one bounded application step;
- persist the returned cursor;
- stop when the application reports done;
- schedule another run when work remains.

The callbacks differ, but the retry, cursor, budget, alarm, and error-isolation machinery can be shared. FokosDB promotion GC does not need a separate generic manager. It can be an optional source-cleanup step or an application background job.

### 23.6 One repartition plan for splits and promotions

Hash split, range split, and hash-key promotion share one control flow:

1. select some ownership from a source;
2. make target identities;
3. initialize the targets;
4. persist a routing rule that sends the selected ownership to them;
5. migrate application state;
6. acknowledge readiness;
7. clean redundant source state when required.

They differ in plan data:

| Operation     | Selected ownership              | Initial targets  | Source after cutover                  |
| ------------- | ------------------------------- | ---------------- | ------------------------------------- |
| Hash split    | All keys owned by the hash leaf | N hash children  | Permanent router                      |
| Range split   | The complete sort-key interval  | N range children | Permanent router                      |
| Key promotion | One hash key                    | One range root   | Still owns all non-selected hash keys |

A generic `RepartitionPlan` can describe:

- the selected ownership predicate or descriptor;
- target partition descriptors;
- the new routing rule;
- a cutover guard;
- application migration parameters;
- source retention or cleanup policy;
- whether the source becomes a pure router or keeps other ownership.

Hash and range policies can be built-in strategies. Key promotion can be an optional route-override strategy. The core state machine does not need three independent orchestrators.

### 23.7 One ownership strategy contract

The generic runtime does not need separate hard-coded hash and range branches. A topology strategy can provide:

- `owns(partition, key)`;
- `selectChild(partition, children, key)`;
- `makeTargets(partition, splitInput)`;
- optional ordered child selection for scans;
- deterministic target identity;
- key encoding, hashing, and comparison;
- cache hint creation and validation.

The current hash tree and range tree become two implementations of this contract. An application can provide a different ownership model without changing migration or request gates.

Ordered range traversal remains an optional strategy capability. Point routing must not contain range-query behavior.

### 23.8 One owner resolver for all item-routing modes

The current routing helpers can share one primitive that resolves each key to:

- local ownership;
- one remote partition;
- retry because the destination is incomplete;
- invalid routing;
- application rejection.

Other routing modes build on this result:

- a point operation resolves one key;
- grouped fan-out groups keys by resolved destination;
- a single-owner fast path accepts only one non-empty destination group;
- transaction prepare, commit, cancel, and read use the same grouping;
- recovery resolves stored keys through the same path.

This removes separate ownership logic from `withSplitForwarding`, `groupItemsByRouting`, and `routeSingleDestination`.

An ordered multi-partition scan still needs a topology traversal operation because it intentionally visits more than one owner.

### 23.9 Application-defined request admission

The generic runtime does not need FokosDB's `read`, `write`, `delete`, and `ignore_size_reject` intent values.

After topology and migration gates select a local candidate, the application can receive:

- its operation descriptor or an opaque policy tag;
- the routing key;
- partition lifecycle information;
- application state and load signals.

It returns allow, retryable rejection, permanent rejection, or another application-defined result. FokosDB can then keep its rules that reject growing writes but allow reads, deletes, and decided commits. Another application can use request count, memory, tenant state, storage size, or no admission policy.

The runtime only needs operation facts that affect sharding correctness, such as whether incomplete destination state can be used and whether the operation supports parent read-through.

The current `internalGetItemDirect` and `internalQueryItemsDirect` methods do not need generic equivalents. An operation that allows reads during migration can provide one source-fallback callback. That callback calls the application's unsharded local handler on the source. The runtime invokes it without normal routing, which prevents a forwarding loop. Operations that do not provide this callback return a retry result while migration is incomplete.

### 23.10 Fewer request lifecycle callbacks

The capabilities in section 20 do not require one public method for every listed phase. A smaller adapter can use:

- `admitLocal` for application request policy;
- `executeLocal` for local application work;
- `afterLocalSuccess` for split signals and application follow-up work;
- one discriminated `onLifecycleEvent` callback for initialization, cutover, migration completion, acknowledgement, and cleanup;
- an optional error-observation callback.

The runtime can own route-cache learning after a forwarded response. The application does not need a forwarding hook unless its protocol requires one.

`afterLocalSuccess` should return work signals instead of directly manipulating alarms. For example, it can request a split evaluation or schedule a named application job. This keeps alarm ownership in one place.

### 23.11 One generic response envelope

Routing metadata does not have to be part of every custom response type. The runtime can wrap an application result in an internal envelope that contains:

- application value;
- serving partition identity;
- forwarding count;
- optional opaque route hint;
- optional routing diagnostics.

The forwarding runtime can update this envelope without knowing the application value. The public client can remove it or map selected fields into public metrics.

This also lets transaction operations return route hints without adding FokosDB `meta` fields to prepare, commit, or cancel responses.

### 23.12 Pluggable and optional topology caches

The generic runtime needs only a cache contract:

- look up a route hint for a key;
- learn from a successful serving-partition result;
- handle a rejected or stale hint;
- persist or discard hints according to the strategy.

The hash arena, range hierarchy, and descendant-promotion Bloom filter can remain built-in strategy implementations. They do not have to appear in the core request dispatcher.

A cache miss, full cache, stale hint, or disabled cache must change latency only. It must not change ownership or correctness.

### 23.13 One scheduler for core and application work

The generic scheduler can run named resumable jobs. Each job provides:

- `canRun`;
- one bounded `runStep`;
- an opaque cursor when needed;
- `done` or `nextRunAt`;
- optional dependencies on other jobs.

The sharding runtime registers child migration, parent repartition, acknowledgement retry, and optional cleanup. FokosDB registers TTL and stale transaction recovery. Promotion detection becomes a split-policy signal; promotion cleanup uses the same cleanup job primitive.

The scheduler, not each callback, chooses the earliest alarm. In-memory timers can remain an optional fast path. Durable alarm state must be sufficient for progress.

### 23.14 Separate immutable identity from mutable policy

The current partition context mixes identity, namespace bindings, and mutable policy and sends them with normal requests.

A generic model can separate:

- immutable partition identity and ownership descriptor, stored at initialization;
- immutable topology parameters that affect deterministic routing;
- mutable application policy with a version or provider;
- small per-request routing context.

The first root request still needs a bootstrap path because Durable Object constructors cannot receive application parameters. Later requests do not need to resend every threshold and policy field when the stored version is current.

### 23.15 One peer gateway and one topology-link view

The package can define one structural control-plane peer for:

- target initialization;
- migration-page reads;
- acknowledgement;
- status needed for recovery or administration.

Application RPC forwarding remains an injected callback because the package does not know custom method signatures.

For traversal, each partition can expose one list of outgoing topology links. Split children and promoted range roots then use the same graph interface. Destruction and diagnostics do not need separate knowledge of `splitStatus` and `promotedKeys`.

### 23.16 FokosDB behavior that should remain outside the package

The generic package should not know:

- item SQL rows or JSON encoding;
- pending transaction row formats;
- deletion watermarks;
- transaction coordinator IDs;
- key-size estimate tables;
- the 25% promotion threshold;
- byte-quantile range boundary calculation;
- condition checks or transaction result merging;
- TTL deletion;
- stale transaction recovery;
- FokosDB row metrics or public response images.

FokosDB can implement these features through the request, split-policy, migration, cleanup, and background-job adapters.

### 23.17 Simplifications that are not safe

The refactor must not simplify away:

- durable routing cutover before new mutations use targets;
- mutation rejection on incomplete destinations;
- idempotent child initialization and migration import;
- bounded migration work and persisted progress;
- equality between request ownership and migration filtering;
- retryable child acknowledgement;
- transaction commit and cancel routing after a split;
- parent read-through or another explicit availability policy during migration;
- application cutover guards such as FokosDB pending-lock checks.

These are control-plane correctness rules. They belong in the generic runtime even when the application data protocol is opaque.

## 24. Current limitations and risks

These are observations, not behavior that a new package must preserve.

### 24.1 Worker router does not learn live topology

The Worker always enters through a root. All deeper routing savings happen inside partition DOs.

### 24.2 Split conditions are not general

Only `maxSizeMb` is implemented. `maxItems` has validation but no split logic.

### 24.3 Application ownership is partly implicit

Hash leaves do not verify their hash path. Range leaves validate the sort-key interval but not the range hash key. Correct parent routing is required.

### 24.4 Split source item rows are never reclaimed

Hash and range split parents retain all source item rows after completion. Only parent pending transaction rows are removed. Promotion has source GC, but ordinary splits do not.

### 24.5 Migration completion has an acknowledgement crash gap

The child persists `migration_completed` before it awaits the parent acknowledgement. If execution stops between those actions, a later migration run sees `completed` and does not retry the acknowledgement. The parent can remain in `split_started`.

A reusable package needs a separate acknowledgement-pending state or an idempotent post-completion retry.

### 24.6 Child migration start is not fully durable at initialization

`internalInitFromSplit` does not set its own fallback alarm. The parent triggers migration after cutover, but the trigger is best effort. If the trigger fails and no request reaches the child, migration may not start.

### 24.7 Promotion and split mutual exclusion has a queue window

A new hash split checks for in-flight promotions before it queues. Promotion drive checks for a queued or started split before cutover.

However, promotion queueing itself does not check split state, and split start does not re-check for a promotion queued after `split_queued`. This leaves a window where both records can exist. The new design needs one atomic lifecycle arbitration rule.

### 24.8 Forced promotion does not route

`debugForcePromoteKey` queues promotion on the called DO. It does not forward to a current hash child when called on a split parent.

### 24.9 Range ancestor reconstruction differs after a wake

During child initialization, an empty configured ancestor set leaves the in-memory response set empty. On a later DO wake, the constructor reconstructs non-root range state and appends the partition itself. This can change routing hints after eviction, including when ancestor caching is configured as fully off.

### 24.10 Learned range topology has no retention bound

`range_hierarchy` uses idempotent inserts but has no size limit, TTL, or cleanup policy.

### 24.11 Transaction routing does not learn from its own fan-out

Transaction responses have no route envelope. The transaction path can use existing caches but does not update them. It also does not use the descendant-promotion Bloom filter.

### 24.12 Background scheduling is best effort and can overlap

The fast scheduler resets its in-memory scheduled marker after one second even if work still runs. A later pass can overlap. Correctness therefore depends on every job being safe under overlap.

### 24.13 Direct migration read-through trusts correct routing

A migrating child reads directly from its parent. The direct point-read method does not filter by the requesting child's ownership. Production routing must send the request to the correct child.

### 24.14 Context comparison does not cover every field uniformly

The explicit immutable and mutable comparison helpers do not compare all context fields. Namespace values and `primaryDoIdStr` are not part of the normal context equality checks.

### 24.15 Child acknowledgement does not validate membership directly

The split state machine records an acknowledgement name before it proves that the name belongs to a configured child. Unknown names do not complete the split, but they can consume acknowledgement entries and later cause the count invariant to fail. The parent acknowledgement RPC must validate child identity in the new design.

### 24.16 Promotion scheduling is not awaited by write paths

The normal put path and the transaction participant call the asynchronous promotion check without awaiting its promise. The promotion row is inserted before the first suspension, but durable alarm scheduling and later failures are not part of the request result. A new lifecycle-hook contract must state whether post-write work is awaited or explicitly best effort.

### 24.17 Destruction behavior has no active integration test

The destruction suite is skipped because it hangs under the current Vitest integration. The traversal code is present, but the complete production flow is not checked by the normal test run.

## 25. Tests that define the current behavior

The main behavior tests are:

- `test/partition-do/hash-split.test.ts` — split state, forwarding, hash cache, and migration;
- `test/partition-do/range-split.test.ts` — range ownership, hierarchy, and promotion skip cache;
- `test/partition-do/promotion.test.ts` — promotion detection, lock deferral, routing, and inheritance;
- `test/partition-do/query-items.test.ts` — ordered range fan-out and migration direct reads;
- `test/partition-do/tx-participant.test.ts` — routed transaction operations and fast paths;
- `test/partition-do/tx-stale-recovery.test.ts` — recovery ownership gates;
- `test/transactions/tx-commit-fanout.test.ts` — transaction behavior during migration and outages;
- `shared/partition/migration.test.ts` — cursor resume and migration channel semantics;
- `shared/partition/hash-key-promotion.test.ts` — promotion state transitions and GC;
- `shared/partition-topology/hash-topology.test.ts` — cache learning and bounds;
- `shared/partition-topology/split-state.test.ts` — split transition semantics;
- `shared/partition-topology/split-policy.test.ts` — operation intent and backpressure.

## 26. Source files for the next design step

The main source boundaries are:

- `packages/fokosdb/src/client/db.ts`;
- `packages/fokosdb/src/server/do-partition.ts`;
- `packages/fokosdb/src/server/do-transaction-coordinator.ts`;
- `packages/fokosdb/src/shared/partition-topology/router.ts`;
- `packages/fokosdb/src/shared/partition-topology/partition-context.ts`;
- `packages/fokosdb/src/shared/partition-topology/partition-id.ts`;
- `packages/fokosdb/src/shared/partition-topology/split-policy.ts`;
- `packages/fokosdb/src/shared/partition-topology/split-state.ts`;
- `packages/fokosdb/src/shared/partition-topology/hash-topology.ts`;
- `packages/fokosdb/src/shared/partition-topology/partial-range-topology.ts`;
- `packages/fokosdb/src/shared/partition/migration.ts`;
- `packages/fokosdb/src/shared/partition/hash-key-promotion.ts`;
- `packages/fokosdb/src/shared/partition/partition-peer.ts`;
- `packages/fokosdb/src/shared/partition/partition-store.ts`;
- `packages/fokosdb/src/shared/partition/transaction-participant.ts`;
- `packages/fokosdb/src/shared/partition/ttl-expiry.ts`.

## 27. Recommended next design question

The next design step should choose the integration model before it defines concrete types:

- a base Durable Object class;
- a composed runtime owned by the custom DO;
- a class decorator or RPC dispatcher;
- generated wrappers around custom RPC methods.

The operation shapes and lifecycle hooks in this audit should be tested against each model. The selected model must let the custom DO keep full storage access while the sharding runtime keeps exclusive control of topology transitions and forwarding order.
