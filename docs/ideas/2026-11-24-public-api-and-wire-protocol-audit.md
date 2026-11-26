# Audit — Public API and Client-Server Wire Protocol

**State:** Open
**Date:** 2026-11-24

Paths below are relative to `packages/fokosdb/src/`.

## 1. Public API and Wire Inventory

### 1.1 Package Entry Points

- `fokosdb/client` (`client/index.ts`) — `FokosDB`, `FokosDBStd`, item, query, scan, batch, transaction, and expression types, `DATA_KINDS`, `DEFAULT_LIMITS`, `EXPRESSION_LIMITS`, `EXPRESSION_NATIVE_TYPES`, `ExpressionError`, and `FokosError` classes.
- `fokosdb/server` (`server/index.ts`) — `FokosDBPartitionDO`, `FokosDBTransactionCoordinatorDO`, `FokosDBMaterializationDO`, `FokosDBAutoAdminDO`, `FokosDBTopologyCache`, target types, and RPC types.
- `fokosdb/sharding/client` (`sharding/exports-client.ts`) — `FokosShardingClient`, `FokosRouter`, `KeyCodec`, hash primitives, partition ID helpers, topology readers, sort-key interval helpers, and sharding errors.
- `fokosdb/sharding/server` (`sharding/index-server.ts`) — `FokosShardingRuntime`, `FokosShardingStore`, `FokosScheduler`, `RepartitionSource`, `RepartitionTarget`, `FokosTopologyKeeper`, caches, and all `fokosdb/sharding/client` exports.

### 1.2 `FokosDB` and `FokosDBStd` Methods

- `FokosDB` (`client/db.ts`): `constructor(options)`, `options()`, `putItem`, `getItem`, `deleteItem`, `transactWriteItems`, `transactGetItems`, `queryItems`, `scanItems`, `batchGetItems`, `batchWriteItems`, `gsi(indexName)` (`queryItems`, `scanItems`), and `destroy()`.
- `FokosDBStd` (`client/fokos-std.ts`): `batchGetItemsWithRetry`, `batchWriteItemsWithRetry`, `sortKeySuccessor`, and `notBeginsWith`.

### 1.3 Wire Interactions

Every routed Durable Object method takes `(ctx: FokosDBRouteContext, req)` and returns `Promise<FokosEnvelope<Res>>`. A thrown `FokosError` carries `routing` as an own property.

- **Worker to `FokosDBPartitionDO`**: `apiPutItem`, `apiGetItem`, `apiDeleteItem`, `apiQueryItems`, `apiScanItems`, `apiBatchGetItems`, `apiBatchWriteItems`, `txExecuteSingleShot`, `txReadSnapshot`, `txReadForTransaction`, and the destroy control RPCs.
- **Worker to `FokosDBTransactionCoordinatorDO`**: `initiateWrite`, and the destroy control RPCs.
- **`FokosDBTransactionCoordinatorDO` to `FokosDBPartitionDO`**: `txPrepare`, `txCommit`, `txCancel`.
- **`FokosDBPartitionDO` to `FokosDBTransactionCoordinatorDO`**: `recoverTransactionForParticipant(req)` (unrouted entry; calls routed `recoverTransaction`).
- **`FokosDBPartitionDO` to `FokosDBMaterializationDO`**: `applyLatestImages`, and `fokosGsiTargetIds()` during `destroy()`.
- **`FokosDBMaterializationDO` to `FokosDBPartitionDO` and Push Sinks**: `gsiWriteEntries`, and `stub[target.method](batch)`.
- **Peer-to-peer Sharding Control (`FokosShardingRpc`)**: `[op](targetCtx, subReq)` forwards, plus `fokosInit`, `fokosStartImport`, `fokosMigrationPull`, `fokosMigrationAck`, `fokosExecuteLocal`, `fokosRequestPromotion`, `fokosStatus`, `fokosPromotions`, `fokosPrepareDestroy`, and `fokosDestroy`.
- **Topology Cache and Admin Plane**: `FokosDBTopologyCache.fetch` (`/internal/v1/topology.Get`, `/internal/v1/topology.GetForRange`), and `FokosDBAutoAdminDO` (`registerRepartition`, `resetShardGroup`, `getTopology`, `getRangeTopology`).

---

## 2. Findings

### 2.1 Correctness and Contract Defects

1. **Fast-path `transactWriteItems` returns a non-idempotent `idempotencyToken`** (`client/db.ts`).
   When `clientRequestToken` is absent, `#writeSingleShotFastPath` mints a UUID in the Worker and returns it as `idempotencyToken` on the result and on `FokosTransactionCancelledError`. No partition and no coordinator stores that token. A replay with `clientRequestToken: result.idempotencyToken` goes to a coordinator and runs the write transaction a second time. Also, a lost reply on `txExecuteSingleShot` raises `foreign_error` instead of `transaction_undecided`.

2. **Coordinator pending errors omit `idempotencyToken`** (`server/do-transaction-coordinator.ts`, `client/db.ts`).
   When the caller omits `clientRequestToken`, `db.ts` generates `idempotencyToken` internally. When `loadFinalResponse` raises `transaction_commit_pending` or `transaction_undecided`, `attributes` holds `{ transactionId, state }` and omits `idempotencyToken`. The caller never gets the generated token and cannot replay the transaction.

3. (DONE) **`FokosDBPartitionDO` and `FokosDBMaterializationDO` cache pre-init `fokosConfig()` forever** (`server/do-partition.ts`, `server/materialization/do-materialization.ts`).
   In `FokosDBPartitionDO.#api`, `this.#ttl.arm()` reads `this.config()` before `this.fokos.dispatch` writes the identity of a new root partition. `config()` caches the pre-init fallback result in `this.__cachedConfig` for the life of the isolate. A subclass override that checks `this.fokos?.initialized()` never runs with the initialized identity or policy.

4. (FUTURE-TODO) **Coordinator `txPrepare` has no fallback when a cached topology hint misses** (`server/do-transaction-coordinator.ts`).
   `initiateWriteLocal` resolves participant addresses from the topology cache and stores them in `tc_participants`. `drivePrepare` sends `txPrepare` through a new `FokosShardingClient` that holds no fallback map. After `destroy()` and table recreation within the 600-second cache window, `txPrepare` hits uninitialized child partitions and cancels valid transactions.

5. (WONTFIX) **Projected `queryItems` and `scanItems` drop item envelopes and GSI base keys, and expressions cannot read GSI base keys** (`shared/types.ts`, `client/db.ts`, `shared/expression/types.ts`).
   `getItem`, `batchGetItems`, and `transactGetItems` return projected rows inside `ReadItem` with keys and `version`. `queryItems` and `scanItems` with `projection` return bare `ProjectedItem[]` records with no keys, no `version`, and on a GSI no `baseHashKey` or `baseSortKey`. `ExpressionReference` also has no `baseHashKey` or `baseSortKey` reference, so a projected GSI query or filter cannot read the base item keys.
   A projected item is intentionally minimal: it holds only what the projection names, as in DynamoDB. A GSI target that projects `{ ref: "hashKey" }` and `{ ref: "sortKey" }` of the base item puts the base keys in the entry data, where a query projection or a filter can read them. The point reads move to the bare record of a projection in item 8.

### 2.2 Public `fokosdb/client` API Gaps and Inconsistencies

6. **No single-item `updateItem` method** (`client/db.ts`).
   `UpdateExpression` exists only in `transactWriteItems`. A single-item update must use a one-item transaction, returns no `version`, and raises `FokosTransactionCancelledError` instead of single-item errors.

7. **No `returnValues` option on successful writes** (`shared/types.ts`, `shared/transaction-api-types.ts`).
   Write operations support `returnValuesOnConditionCheckFailure?: "none" | "all_old"`, and have no option to return `all_old` or `all_new` when a write commits.

8. **Asymmetric result envelopes and outcome tags across read and batch methods** (`shared/types.ts`, `shared/transaction-api-types.ts`, `shared/batch-api-types.ts`).
   `getItem` nests keys and data under `res.item`, `transactGetItems` flattens them onto `res.items[i]`, and `batchGetItems` uses `outcome: "found" | "not_found"` instead of `found: boolean`. `batchGetItems` returns `{ items, unprocessedKeys }`, while `batchWriteItems` returns `{ results, unprocessedItems }`. Success outcomes use four different literals across public types: `"ok"` (while its doc comment says `applied`), `"passed"`, `"found"`, and `"applied"`.
   (DONE for the read shapes) `docs/agent-plans/2026-11-26-read-result-shape.md` gives every read the same item shape. The batch names and the write outcome literals are still open.

9. **Split pagination rules between `queryItems` and `scanItems`, and single-version cursors** (`shared/query/cursor.ts`, `shared/query/scan-cursor.ts`).
   `queryItems` takes only `cursor` and fingerprints `filter` and `projection`, so neither can change between pages. `scanItems` takes `exclusiveStartKey` or `cursor` (with no `lastEvaluatedKey`) and fingerprints only `shardGroup` and `segment`, so `filter` and `projection` can change between pages. Both decoders accept only version `1`, so a format bump breaks in-flight pagination during a gradual deploy.

10. **`FokosDBAPI` omits `scanItems` and `gsi`** (`shared/transaction-api-types.ts`).
    `FokosDBAPI` includes put, get, delete, query, transaction, and batch interfaces, and omits `scanItems` and `gsi(indexName)`.

11. **`FokosDBStd` ignores `db.options().retry` and backs off on `response_too_large`** (`client/fokos-std.ts`).
    `batchGetItemsWithRetry` and `batchWriteItemsWithRetry` take `db: ItemBatcher` and default to `DEFAULT_RETRY` instead of `db.options().retry`. `batchGetItemsWithRetry` also treats `code: "response_too_large"` as an error: it sleeps backoff delays between pages and stops after `maxAttempts` (default 5) even when zero errors occurred. `FokosDBStd` also has an implicit public constructor.

12. **`db.destroy()` has no confirmation guard and misses detached GSIs** (`client/db.ts`).
    `destroy()` sits on `FokosDB` with zero arguments. It discovers GSI shard groups only through `fokosGsiTargetIds()` on `FokosDBMaterializationDO` when `nsMat` is set, so removed GSI targets or unset `nsMat` leave orphaned GSI objects behind.

13. **Missing `db.gsi(indexName)` validation, `clientRequestToken` surrogate order, and shallow `db.options()`** (`client/db.ts`, `shared/partition-context.ts`, `shared/transaction-limits.ts`).
    `db.gsi("").queryItems(...)` fails in `gsiIndexNameStart` with `FokosInternalError` (`invariant_failed`, 500) instead of `FokosValidationError` (400). A `clientRequestToken` with a lone surrogate passes `validateClientRequestToken` and fails in `encodeHashKey` with `key_not_well_formed_utf16`. `db.options()` returns a shallow copy that shares nested `table` and `retry` objects and locks default formulas (`coordinatorRootsN`) into the public type.

### 2.3 Encapsulation Leaks in Public Types and Exports

14. **Physical partition names, neighbour item keys, depths, and database sizes in `FokosMeta`** (`shared/types.ts`, `shared/partition-meta.ts`).
    `PartitionMeta` exposes `servedByActorId`, `servedByActorName`, `hashDepth`, `rangeDepth`, and `databaseSize` on every result and error. A range `servedByActorName` embeds encoded `hashKey` and `sortKey` boundary values from items in the table, and becomes `undefined` whenever the 10 KiB routing cap drops nodes.

15. **Closed `FokosErrorCode` union exposes internal codes, `ExpressionError` is exported, and `retryable` is absent** (`shared/errors-operations.ts`, `shared/errors.ts`, `client/index.ts`).
    `FokosErrorCode` is a closed union that includes internal sharding and coordinator codes (`repartition_*`, `sharding_*`, `*_not_initialized`, `commit_keyset_mismatch`, `partition_fanout_failed`), so adding an internal code breaks exhaustive caller switches. `FokosError` has three category fields (`name`, `_tag`, `type`) and no `retryable` property or helper in `fokosdb/client`. `client/index.ts` also exports the internal `ExpressionError` class.

16. **SQLite limits in `EXPRESSION_LIMITS` and `{ ref: "v" }` naming mismatch** (`shared/expression/limits.ts`, `shared/expression/types.ts`).
    `EXPRESSION_LIMITS` exports SQLite implementation keys (`sqliteFunctionArguments`, `sqlitePatternBytes`, `compiledSqlBytes`, `completeStatementBindings`). `ExpressionReference` uses `{ ref: "v" }` (the SQLite column name) while `ReadItem` names the field `version`.

17. **Unmarked `fokosdb/sharding/*` exports and `ScanCursor` leak** (`sharding/exports-client.ts`, `sharding/index-server.ts`, `sharding/runtime-types.ts`).
    Both sharding entry points export internal stores, KV keys, schedulers, Bloom filters, and codecs without an `@experimental` marker. `FokosRangeInput.cursor` imports `ScanCursor` from `shared/partition/partition-store.ts` into the sharding type surface.

### 2.4 Client-Server Wire Protocol and Configuration Issues

18. **Caller `env` binding names on the wire and module-global `cloudflare:workers` imports** (`client/db.ts`, `shared/partition-context.ts`, `shared/do-stubs.ts`, `server/admin-outbox.ts`, `shared/topology-hooks.ts`).
    `new FokosDB(options)` reads module-global `env`, `exports`, and `waitUntil`. `FokosDBPolicy` carries caller `env` property names (`ns`, `nsTx`, `nsMat`, `nsAdmin`) on every RPC and persists them in storage. Server Durable Objects look up those exact names on their own `env`, which fails across separate Worker scripts with different binding names or `ctx.exports`-only Workers. `admin-outbox.ts` and `topology-hooks.ts` also read module-global `env` instead of `this.env`.

19. **Equal `policyVersion` overwrites stored policy on the request path** (`sharding/runtime.ts`).
    When `routeCtx.policyVersion === stored.policyVersion` and the policy differs, `#ensureIdentity` overwrites `__fokos/policy` synchronously. Two Workers at the same `policyVersion` (default `0`) with different options rewrite SQLite on alternating requests and can turn `captureChanges` off.

20. **Redundant wire contexts, `StoredItem` columns on the wire, four direction encodings, and two binary-in-JSON codecs** (`client/db.ts`, `server/do-partition.ts`, `server/do-transaction-coordinator.ts`, `shared/errors.ts`).
    Every RPC carries the full `FokosRouteContext`, `initiateWrite` sends the policy and range config twice (`ctx` and `req.table`), and `tc_participants` stores the full JSON context per participant. `apiQueryItems` and `apiScanItems` send `StoredItem` on the wire, including unused `last_read_ts` and `last_write_ts` columns. Query direction uses four representations (`scanIndexForward`, `"asc" | "desc"`, `descending`, `"fwd" | "rev"`). `RejectionReasonEncoded` decodes `KeyBytes` on the server, causing `do-transaction-coordinator.ts` (`stringifyTagged`) and `errors.ts` (`encodeBytes`) to maintain two different `Uint8Array`-in-JSON codecs.

21. **Client-supplied server budgets and client-only key limits** (`server/do-partition.ts`, `shared/transaction-limits.ts`, `server/host-config.ts`).
    `FokosDBPartitionDO` trusts client-computed query, scan, and batch budgets without clamping `remainingEvaluatedItems` on the server, and does not check `maxHashKeyBytes` or `maxSortKeyBytes` on writes. Coupled timeout pairs (`partitionMigratingRetryDeadlineMs` vs `fallbackAlarmMs`, and `staleTransactionMs` vs `fanoutRequestBudgetMs`) have no protocol check across nodes.

22. **Asymmetric client retry policies** (`client/db.ts`).
    `getItem`, `queryItems`, and `scanItems` do 0 retries on transient faults (aborting up to 99 completed visits in `scanItems`). `#readSnapshotFastPath` does not retry `partition_migrating`. `#readTransaction` uses `() => true` and retries deterministic errors (`sql_limit`, `partition_context_mismatch`) 5 times. `initiateWrite` does not retry transient runtime faults on the hop to the coordinator even though every call carries `idempotencyToken`.

23. **GSI reads, GSI writes, and Materialization DOs omit the topology cache** (`client/db.ts`, `server/materialization/gsi-writer.ts`, `server/materialization/do-materialization.ts`).
    GSI partitions send split and promotion events to `FokosDBAutoAdminDO`, which builds GSI topology bodies, yet `FokosDB.#gsiClient` and `GsiWriter.#client` configure no `topologyCache` and always enter at a GSI root. Conversely, `FokosDBMaterializationDO` configures no `adminOutbox` and no `topologyCache`, while `db.destroy()` still calls `resetShardGroup` on materialization admin objects.

24. **RPC and HTTP surfaces without `FokosRouteContext` for protocol versioning** (`server/do-transaction-coordinator.ts`, `sharding/repartition-types.ts`, `server/materialization/do-materialization.ts`, `sharding/topology-keeper.ts`).
    `recoverTransactionForParticipant`, eight `FokosShardingRpc` methods (`fokosStartImport`, `fokosMigrationPull`, `fokosMigrationAck`, `fokosExecuteLocal`, `fokosRequestPromotion`, `fokosPromotions`, `fokosDestroy`, and `fokosStatus`/`fokosPrepareDestroy` when `rootContext` is absent), `fokosGsiTargetIds`, `FokosDBAutoAdminDO` methods, and `FokosDBTopologyCache.fetch` do not carry `FokosRouteContext`.

25. **Exact workerd error string match in `classifyBatchFailure`** (`shared/batch-failure.ts`).
    `isRpcTooLarge` matches the English prefix `"Serialized RPC arguments or return values are limited to 32MiB"`. A wording change in `workerd` turns oversized batch writes into `"unknown"` instead of `"rejected"`.

### 2.5 Public `fokosdb/server` Surface and Lifecycle Issues

26. **Public `fokos` property, public override hooks, and test/debug RPCs on Durable Object classes** (`server/do-partition.ts`, `server/do-transaction-coordinator.ts`, `server/materialization/do-materialization.ts`).
    `readonly fokos` and `fokosNow()` are `public` on all three Durable Object classes, and `fokosGetColoInfo()` (which also reads undocumented `env.FOKOS_SHOULD_FETCH_COLO_INFO`) is `public` on `FokosDBPartitionDO`. Test methods `status` and `promotedKeyStatus` and `debugForcePromoteKey` are public RPCs on production `FokosDBPartitionDO`, while the production operator tool `debugForceResolveTransaction` has a `debug*` prefix.

27. **Missing override option types and leaked wire types in `fokosdb/server`** (`server/index.ts`).
    `fokosdb/server` exports internal wire types (`PartitionOps`, `PartitionRpc`, `MaterializationOps`, `ApplyLatestImagesRequest`, `ApplyLatestImagesResponse`, `LatestImage`), and omits the return types for subclass overrides (`PartitionDOConfigOverrides`, `TransactionCoordinatorDOConfigOverrides`, `MaterializationDOConfigOverrides`, `FokosRuntimeConfigOverrides`, `FokosTopologyKeeperOptions`, `ColoInfo`).

28. **`fokosTargets()` freezes the target list on the first request** (`server/materialization/do-materialization.ts`).
    `#targets()` stores `this.fokosTargets()` on the first request to a materialization partition and ignores all later code changes. An existing table cannot add, change, or remove a GSI or push target, and roots initialized before and after a deploy can store different target lists.
