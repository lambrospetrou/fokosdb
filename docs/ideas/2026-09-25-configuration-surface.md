# Configuration surface: an inventory of the constants and a proposal

Status: proposal. Nothing here is decided.

## 1. The problem

`PartitionDO`, `TransactionCoordinatorDO`, `FokosShardingRuntime` and `FokosDB` use many module-level constants: validation limits, page budgets, timeouts, retry intervals, and cache sizes. Only some of them can change today, and each one that can change uses a different mechanism:

| Mechanism today | Values |
| --- | --- |
| `PartitionContextCreator.create` options, carried in `FokosDBPolicy` on every RPC | `hashSplitConditions`, `rangeSplitConditions`, `rangeSplitN`, `hashSplitN`, `rootTreesN`, `rangeAncestorsConfig`, `jurisdiction`, `locationHint` |
| `FokosDBOptions` (constructor of the client) | `coordinatorRootsN`, `singlePartitionFastPath` |
| Per-call options | `limit`, `maxResponseBytes`, `clientRequestToken` |
| Overridable DO method | `fokosStaleTransactionMs()`, `fokosTtlConfig()`, `fokosFanoutRequestBudgetMs()`, `fokosNow()` |
| `FokosShardingRuntime` constructor option | `caches.{hashArenaBytes, rangeHierarchyMaxRows, promotionBloom}`, `scheduler.{fallbackAlarmMs, fastPathDelayMs}` |
| `hooks.runtimeConfig()` callback | `importPagesPerPass` |

A user cannot pass arguments to a Durable Object constructor, because the Workers runtime creates the object. A user can only subclass the class, read `env`, or send data in a request.

## 2. Scopes

Each value has one scope. The scope decides who must agree on the value, and so where the value can live.

- **Fixed** — part of an on-disk format, a wire format, or an identity scheme. A change breaks existing data or routing. It must stay a constant.
- **Platform** — a Workers or Durable Object limit. A configuration can make it smaller, never larger.
- **Table** — the client and one or more DO classes must use the same value, or they disagree about what is valid. Today only `FokosDBPolicy` reaches all three.
- **DO class** — operational tuning inside one DO class. No other party reads it.
- **Client** — tuning inside one `FokosDB` instance in the caller Worker.
- **Request** — one call can choose it.

Users: **C** = caller Worker (`FokosDB`), **P** = `PartitionDO`, **T** = `TransactionCoordinatorDO`, **R** = `FokosShardingRuntime` (inside P and T, and inside any other host).

## 3. Inventory

### 3.1 Item and key validation (`shared/transaction-limits.ts`)

| Constant | Value | Users | Today | Proposed scope | Notes |
| --- | --- | --- | --- | --- | --- |
| `MAX_HASH_KEY_BYTES` | 1024 | C (`encodeHashKey`), T (token key) | const | Table, capped | Raising it makes `doName` and range `partition_id` columns larger (`sharding-store.ts`). Lowering it is safe. |
| `MAX_SORT_KEY_BYTES` | 512 | C | const | Table, capped | Same. |
| `MAX_ITEM_BYTES` | 400 KiB | C (lower-bound check), P (`partition-store`, `transaction-participant`) | const | Table, capped | C and P must agree. The DO SQLite row and value size limit is the ceiling. |
| `MAX_ITEMS_PER_TX` | 100 | C only | const | Table, capped | T does not check it again: a client with other code can send more. |
| `MAX_PAYLOAD_BYTES_PER_TX` | 4 MiB | C only | const | Table, capped | Same as above. The Workers RPC message size is the ceiling. |
| `MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX` | 10 MiB | P, T (`applyImageCap`) | const | Table, capped | P and T must agree. |
| `MAX_CLIENT_REQUEST_TOKEN_BYTES` | 64 | C | const | Table | Must stay at or below `MAX_HASH_KEY_BYTES`, because the token is a coordinator hash key. |
| `TX_ORDER_TS_UNITS_PER_MS` | 1000 | C, P, T | const | Fixed | Stored in lock rows and in the ledger. |

### 3.2 Transaction timing

| Constant | Value | Users | Today | Proposed scope | Notes |
| --- | --- | --- | --- | --- | --- |
| `IDEMPOTENCY_WINDOW_MS` | 10 min | T (ledger sweep, `MAX_PREPARING_HOLD_MS`), P (lock-age quarantine) | const | Table | P quarantines a lock older than the window because T deletes its ledger row after the window. They must agree. |
| `STALE_TX_MS` (P) | 5 s | P | `fokosStaleTransactionMs()` | DO class | |
| `STALE_THRESHOLD_MS` (T) | 5 s | T | const | DO class | Separate from the P value, but both describe "a transaction nobody drives". |
| `MAX_PREPARING_HOLD_MS` | min(25 s, window) | T | const | Derived | Derive it from the two values above. Do not configure it separately. |
| `TX_FANOUT_REQUEST_BUDGET_MS` | 5 s | T | `fokosFanoutRequestBudgetMs()` | DO class | |
| `ALARM_RECOVERY_BUDGET_MS` | 30 s | T | const | DO class | Must stay below the DO alarm wall-clock limit. |
| `SWEEP_BATCH_ROWS` | 1000 | T | const | DO class | |
| `MAX_PARTICIPANT_ATTEMPTS_WITHOUT_DEADLINE` | 100 | T | const | DO class | |
| participant retry `{baseDelayMs: 100, maxDelayMs: 2000}` | | T (4 sites) | inline literal | DO class | |
| `MAX_CLOCK_SKEW_MS` | 5 s | P | static | DO class | Must be larger than the real clock skew between C, T and P. |
| stale scan batch `10`, TC recovery `LIMIT 100`, sweep `CHUNK_SIZE = 100` | | P, T | inline literal | DO class | |

### 3.3 Query page budgets (`shared/query/page-budget.ts`)

| Constant | Value | Users | Today | Proposed scope | Notes |
| --- | --- | --- | --- | --- | --- |
| `DEFAULT_EVALUATED_ITEMS_PER_PAGE` | 1000 | C | const | Client default, Request override | Already overridable per call with `limit`. |
| `MAX_EVALUATED_ITEMS_PER_PAGE` | 100 000 | C | const | Table, capped | P trusts the budget that C sends. |
| `MAX_EVALUATED_BYTES_PER_PAGE` | 100 MiB | C | const | Table | |
| `DEFAULT_RESPONSE_BYTES_PER_PAGE` | 3 MiB | C | const | Client default, Request override | Already per call with `maxResponseBytes`. |
| `MAX_RESPONSE_BYTES_PER_PAGE` | 16 MiB | C, P (margin comment) | const | Platform, capped | Keeps a 2x margin below the Workers RPC message size. |
| `MAX_PARTITION_VISITS_PER_PAGE` | 100 | C | const | Table | |
| `ITEM_ENVELOPE_BYTES` | 64 | P | const | Fixed | An estimator constant. A change moves page boundaries. |

### 3.4 Expression limits (`shared/expression/limits.ts`, exported as `EXPRESSION_LIMITS`)

| Keys | Users | Proposed scope | Notes |
| --- | --- | --- | --- |
| `operatorsAndFunctions`, `astDepth`, `updateActions`, `projectionEntries`, `jsonPathDereferences`, `inChoices`, `sqliteFunctionArguments`, `sqlitePatternBytes`, `projectionAliasBytes`, `jsonPathBytes`, `canonicalPayloadBytes` | C (compiler), P (plan checks in `expression/runtime.ts`, `partition-store`) | Table, capped | C compiles and P checks the plan again. They must agree. |
| `compiledSqlBytes` (100 000), `completeStatementBindings` (100) | C, P | Platform | These are the DO SQLite statement length and bound-parameter limits. They can only go down. |

### 3.5 Split and size policy

| Constant | Value | Users | Today | Proposed scope | Notes |
| --- | --- | --- | --- | --- | --- |
| `hashSplitConditions`, `rangeSplitConditions` | 100 MB, 500 MB | P, T | policy | Table (already) | `maxItems` is not read (FIXME in `partition-context.ts`). |
| `RANGE_PROMOTION_FRACTION` | 0.25 | P | const | Table | A split policy. It belongs next to `hashSplitConditions`. |
| admission margin `1.1` | | P, T | inline literal | Table | Same. |
| `MAX_TC_DATABASE_BYTES` | 5 GiB | T | const | DO class, capped | The 10 GB DO storage limit is the ceiling. |
| `EST_ROW_BYTES_K` | 108 | P | const | Fixed | Feeds stored `key_size_estimates`. |
| `FOKOS_HASH_PARTITIONS_MAX` | 65 000 | C, R | const | Fixed | The partition ID keeps the root index in two bytes. |
| `TX_COORDINATORS_PER_ROOT_TREE` | 2 | C | const | Client default | Already overridable with `coordinatorRootsN`. |

### 3.6 Sharding runtime and repartition flow

| Constant | Value | Users | Today | Proposed scope |
| --- | --- | --- | --- | --- |
| `DEFAULT_FALLBACK_ALARM_MS` | 5 s | R | ctor option | DO class |
| `DEFAULT_FAST_PATH_DELAY_MS` | 50 ms | R | ctor option | DO class |
| `DEFAULT_IMPORT_PAGES_PER_PASS` / `IMPORT_PAGES_PER_PASS` | 16 | R, P | `runtimeConfig()` | DO class |
| hash arena budget | 1 MiB | R | ctor option | DO class |
| `RANGE_HIERARCHY_MAX_ROWS` | 10 000 | R | ctor option | DO class |
| promotion Bloom `{300 000, 0.01}` | | R | ctor option | DO class |
| `RANGE_HIERARCHY_REFRESH_MS` | 60 s | R | const | DO class |
| `MAX_FORWARD_RETRIES` | 8 | R | const | DO class |
| `STATUS_PAGE_ENTRIES`, `STATUS_PAGE_BYTES` | 1000, 20 MiB | R | const | DO class, capped |
| `FOKOS_PAGE_BYTES`, `FOKOS_PAGE_ROWS`, `FOKOS_SCAN_ROWS` | 20 MiB, 1000, 10 000 | R, T | const | DO class, capped |
| `PAGE_BYTES`, `PAGE_ROWS`, `SCAN_ROWS` (P migration host) | same values, duplicated | P | const | DO class — reuse the runtime values |
| `SOURCE_RETRY_*`, `IMPORT_RETRY_*`, `LOCK_RETRY_MS`, `CLEANUP_RETRY_MS`, `NOT_CUT_OVER_RETRY_MS`, `NON_RETRYABLE_RETRY_MS` | 5 s to 5 min | R | const | DO class |
| `REPARTITION_RPC_CONCURRENCY` | 6 | R | const | Platform — six simultaneous outgoing connections |
| `ROUTE_EVIDENCE_MAX_BYTES` | 10 KiB | R | const | Fixed |
| bloom internals (`TIGHTENING_RATIO`, `LAYER_GROWTH_FACTOR`, …) | | R | const | Fixed |

### 3.7 PartitionDO host

| Value | Users | Today | Proposed scope |
| --- | --- | --- | --- |
| TTL sweep (`chunkSize`, `sleepMs`, `maxRowsBeforeSleep`, `maxBytesBeforeSleep`, `maxRowsPerCycle`, `initialDelayMs`) | P | `fokosTtlConfig()` | DO class |
| promoted-key cleanup batch `1000` | P | inline literal | DO class |
| `FOKOS_SHOULD_FETCH_COLO_INFO` | P | `env` | DO class |

### 3.8 Client (`client/db.ts`)

| Value | Users | Today | Proposed scope | Notes |
| --- | --- | --- | --- | --- |
| `TX_COORDINATOR_MIGRATING_RETRY_MS` | 15 s | C | const | Client | Must be larger than the runtime `fallbackAlarmMs` of T. |
| retry `{baseDelayMs: 100, maxDelayMs: 2000}` | C (3 sites) | inline literal | Client, Request override | |

### 3.9 Fixed constants that must never be configuration

Key codec tags, `DATA_KINDS`, plan versions and fixed binding counts, `CURSOR_VERSION`, `COORDINATOR_REF_VERSION`, `OPERATIONS_SEED`, hash seeds and `GOLDEN_RATIO`, `RANGE_MIN`/`RANGE_MAX`, `RESERVED_SHARD_GROUP_PREFIX`, the KV key names, job names, and the error code tables.

## 4. What the inventory shows

1. **Most tunables are DO-class operational values.** Only P or T or R reads them, so they need no agreement with the client.
2. **About 20 values are table contracts.** The client and one or more DO classes must use the same value. Key and item sizes, transaction limits, page ceilings, expression limits, the idempotency window, and the promotion fraction are in this group. Today they agree only because both sides import the same module at the same package version. A client and a DO on different package versions can already disagree.
3. **Several table limits are checked only on the client.** `MAX_ITEMS_PER_TX`, `MAX_PAYLOAD_BYTES_PER_TX` and the page ceilings are not checked again in T or P. For a public-facing limit that is acceptable only while the client is trusted code.
4. **Some values are coupled.** `MAX_PREPARING_HOLD_MS` derives from the stale threshold and the window. `TX_COORDINATOR_MIGRATING_RETRY_MS` must be larger than `fallbackAlarmMs`. `MAX_CLIENT_REQUEST_TOKEN_BYTES` must be at most `MAX_HASH_KEY_BYTES`. A configuration layer must validate these relations, not only each value.
5. **Several values have a platform ceiling.** A configuration can lower them but must refuse a value above the ceiling.
6. **There are duplicates.** The P migration host repeats the runtime page budgets, and the retry backoff literal appears seven times.

## 5. The options

### Option A: one isolate-global `FokosConfig` with setters

It does not solve the problem it targets. The caller Worker and the Durable Objects can run in different scripts (a DO binding with `script_name`), so a setter in the caller never reaches the DO isolate. Inside one isolate the value is shared by every table and every DO class in the script, so two tables cannot differ. The order of a setter and the first request is not guaranteed, and a change after the first request makes objects in the same isolate disagree. It also makes test isolation difficult. Reject it as the main mechanism.

### Option B: constructor arguments everywhere

This works for `FokosDB` and for `FokosShardingRuntime`, because user or host code constructs them. It cannot work for a Durable Object class, because the runtime constructs it.

### Option C: overridable methods on the DO classes

This is the only hook a DO class has, and the code already uses it. Today there are four separate methods with four shapes. One method per class, which returns one typed object, is easier to document and to validate.

### Option D: carry the table contract in the route context

`FokosDBPolicy` already travels in every RPC, and the DO already stores and compares it. It is the only channel that reaches C, P and T with the same value, and it already has one validating creator, `PartitionContextCreator.create`.

## 6. Recommendation

> Section 8 records the decisions after the audit. Where section 8 and this section disagree, section 8 applies.

Use three layers, one for each group of parties that must agree. Do not add a global mutable config.

### 6.1 Table limits → overrides only in `FokosDBPolicy.limits`

The route context travels in every RPC, so the table limits must add almost no bytes to it. Measured with `v8.serialize` (the structured-clone format of Workers RPC):

| What travels | Bytes |
| --- | --- |
| The whole route context today | 325 |
| All the limits as one nested object | 616 |
| All the limits as one positional array | 139 |
| Only the overrides, two values | 35 |
| Only the overrides, none (the field is absent) | 0 |

A full object almost triples the route context. A positional array is smaller, but it costs the same bytes on every request when nothing is overridden, and each index must stay fixed for life. Short key names make every reader worse. So the wire carries **only the values the user overrides**, and each side materializes the full limits from them.

**The wire.** `FokosDBPolicy.limits?: FokosDBLimitOverrides` is a flat, partial record with descriptive names, for example `{ maxItemBytes: 300_000, maxItemsPerTx: 50 }`. Flat, because a nested object adds a tag and a key for each level. `PartitionContextCreator.create` validates the overrides and omits `limits` when there are none, so a table with defaults sends zero extra bytes. It keeps an override that is equal to the default, because a user who pins a value must keep it when a later version changes the default.

**The helper.** One function in `shared/` turns overrides into the full, frozen limits:

```ts
export function resolveLimits(overrides: FokosDBLimitOverrides | undefined): FokosDBLimits;
```

It starts from the defaults of the package version that runs it, applies the overrides, checks each value against its platform ceiling and the coupled relations, and ignores keys it does not know (a newer client can send them to an older DO). With no overrides it returns the one frozen `DEFAULT_LIMITS` object and does no work.

**The cache.** No side materializes per request:

- `FokosDB` resolves once in its constructor, from the policy of its router.
- A DO reads `this.fokos.policy()`. The runtime already compares the incoming policy with `structurallyEqual` on every request, and it keeps the SAME stored object while nothing changed. So a `WeakMap<FokosDBPolicy, FokosDBLimits>` keyed on that stored object resolves once per policy change, not once per request. A key comparison of the incoming object would not work: every RPC delivers a new object.

**Different defaults across versions.** A client and a DO on different package versions can resolve different defaults. This is acceptable, because the DO is the truth for every stored-size check and the client check is only an early answer: a skew changes where a request is rejected, not what is stored. The limits that the DO does not check again today (`maxItemsPerTx`, `maxPayloadBytesPerTx`, the page ceilings) get a check in T and P, from the same resolved limits.

`RANGE_PROMOTION_FRACTION` and the admission margin do not go into this record (8.3, 8.4).

Why not a DO method for these: the client cannot see a DO method, so the client check and the DO check would drift, which is the problem this layer removes.

**Later, for the whole route context.** The same idea can shrink the rest of the route context: send a short hash of `rangeConfig` and `policy`, and send the full values only when the DO answers that it does not know the hash. That is a separate change, and the measured 325 bytes do not need it yet.

### 6.2 DO-class tuning → one `fokosConfig()` method per class

Replace `fokosStaleTransactionMs()`, `fokosTtlConfig()`, `fokosFanoutRequestBudgetMs()` and `hooks.runtimeConfig()` with one overridable method on each class. It returns a deep partial. A shared helper merges it with the defaults and validates it.

```ts
class PartitionDO {
	/** Override to change the operational tuning of this class. Read at each use. */
	protected fokosConfig(): PartitionDOConfigOverrides { return {}; }
}

type PartitionDOConfig = {
	runtime: FokosRuntimeConfig;        // scheduler, caches, import pages, retries, page budgets, forward retries
	staleTransactionMs: number;
	maxClockSkewMs: number;
	ttlSweep: TtlSweepConfig;
};

type TransactionCoordinatorDOConfig = {
	runtime: FokosRuntimeConfig;
	staleThresholdMs: number;
	fanoutRequestBudgetMs: number;
	participantRetry: { baseDelayMs: number; maxDelayMs: number; maxAttemptsWithoutDeadline: number };
	alarmRecoveryBudgetMs: number;
	sweepBatchRows: number;
	maxDatabaseBytes: number;
};
```

- The method can read `this.env`, so a deployment can tune values from `vars` without a subclass.
- Read the method at each use, and do not cache it in the constructor. The base constructor runs before the fields of a subclass exist, so a value read there ignores an override that uses a field. A read at each use also lets the test subclasses keep their per-instance control without a production hook for tests.
- Validation is a few number checks, so it can run at each read. Cache on the identity of the returned object if a profile shows a cost.
- `fokosNow()` stays a separate method. It is a clock, not a setting.

### 6.3 Runtime → one `FokosRuntimeConfig`

`FokosShardingRuntime` is also a library for other hosts, so it keeps its own type. Merge `caches`, `scheduler` and `FokosRuntimeConfigOverrides` into one `FokosRuntimeConfig`, and move the repartition retry intervals, the page budgets, `MAX_FORWARD_RETRIES`, `RANGE_HIERARCHY_REFRESH_MS` and the status page budgets into it. The constructor takes `config?: () => Partial<FokosRuntimeConfig>`. Values that size a structure at construction (the arena budget, the Bloom filter, the hierarchy row bound) are read once. All other values are read at each use. P and T pass `() => this.fokosConfig().runtime`. The P migration host uses the page budgets from the same object, which removes the duplicate constants.

### 6.4 Client → `FokosDBOptions`

Add `retry: { baseDelayMs, maxDelayMs }`, `coordinatorMigratingRetryMs`, and the default page values (`defaultQueryLimit`, `defaultMaxResponseBytes`) to `FokosDBOptions`. The table ceilings come from the policy (6.1), not from these options.

### 6.5 Request

Keep `limit`, `maxResponseBytes` and `clientRequestToken`. A later per-call deadline or `AbortSignal` can override the client retry budget. Nothing else needs a per-request value.

## 7. Order of work

Section 8 has the decisions. Each milestone is one reviewable change, and `pnpm test` passes after each one.

1. **Rename.** `FokosDb*` types become `FokosDB*` (8.1).
2. **Runtime configuration.** Add `FokosRuntimeConfig` and the `config` constructor callback. Move into it the `caches` and `scheduler` options, `importPagesPerPass`, the repartition retry intervals, the page and status budgets, `maxForwardRetries` and `rangeHierarchyRefreshMs`. Remove `hooks.runtimeConfig()` and `PartitionDO.IMPORT_PAGES_PER_PASS`. Merge the duplicate page and status constants. P and T implement `fokosRuntimeConfig()` (8.8).
3. **Host configuration.** Add `fokosConfig()` to P and T with the values of 8.3, and remove `fokosStaleTransactionMs()`, `fokosTtlConfig()` and `fokosFanoutRequestBudgetMs()`. Add the shared stale-transaction default, the shared admission margin constant, and the coordinator `participantRetry` policy. Remove `SplitConditions.maxItems`.
4. **Recovery budget defect** (8.7, item 1). A separate change, because it changes the retry flow of the `tx_recovery` job and its tests.
5. **Client configuration.** Add `FokosDBOptions.retry`. Add the key-size overrides in `FokosDBPolicy.limits` with `resolveLimits` and its cache (6.1).
6. **Documentation.** Every value: its default, its ceiling, what it affects, and whether a change is safe for a table that exists.
7. **Late reads** (8.8). Last, because each one changes a flow and its tests:
   - `FokosShardingStore` reads `rangeHierarchyMaxRows` through a getter at the eviction.
   - The TTL sweep: rename `TtlSweepConfig.initialDelayMs` to `ttlSweepDelayMs`, and make `TtlExpiry.arm()` default to it instead of the literal `500`. The constructor of `PartitionDO` no longer reads the configuration. Arming in the constructor is optional: if it stays, it uses a fixed `TTL_SWEEP_CONSTRUCTOR_DELAY_MS = 500` constant.

## 8. Decisions after the audit

### 8.1 Mechanisms

- **`fokosConfig()`** on each DO class (`PartitionDO`, `TransactionCoordinatorDO`) returns the overrides of the host settings. It replaces `fokosStaleTransactionMs()`, `fokosTtlConfig()` and `fokosFanoutRequestBudgetMs()`. It is read at each use (6.2).
- **`fokosRuntimeConfig()`** on each DO class returns the overrides of the `FokosShardingRuntime` settings. The runtime takes it as one constructor callback, `config: () => FokosRuntimeConfigOverrides`, which replaces the `caches` and `scheduler` options and `hooks.runtimeConfig()`. The runtime becomes a library of its own, so every value that the runtime reads is in `FokosRuntimeConfig`, even when FokosDB uses the default. A value that sizes a structure at construction is read once. All other values are read at each use.
- `config` is a top-level constructor option, next to `ctx`, `stub`, `hooks` and `operations`, and not a member of `hooks`. `operations` declares the API of the host and the behaviour of each operation. `hooks` holds the policy decisions and lifecycle steps the host implements: each one receives an input and returns a decision, and four of them run inside `transactionSync`. `config` returns data, not a decision. It covers the scheduler, the caches, the repartition flow and the forwarding, which are not partition lifecycle steps. A setting that applies to one operation stays in the descriptor of that operation, and can take its default from `config`.
- **`FokosDBOptions`** holds the client settings.
- **`FokosDBPolicy.limits`** holds only the table limits that the client and the DOs must share: the key sizes (8.3). Only the overrides travel (6.1).
- New FokosDB types spell `DB` in upper case (`FokosDBPolicy`, `FokosDBLimits`). The existing `FokosDb*` types (`FokosDbPolicy`, `FokosDbRouteContext`, `FokosDbStubContext`, `FokosDbTableConfig`, `FokosDbHostCursor`, `FokosDbHostPage`, `FokosDbMigrationHost`) get the same spelling in one rename. Three of them are exported from `fokosdb/client`, so the rename changes the public types.
- `fokosNow()` stays a separate method, because it is a clock and not a setting.

### 8.2 Remove

| Value | Reason |
| --- | --- |
| `SplitConditions.maxItems` | Validated, never read. |
| `PartitionDO.IMPORT_PAGES_PER_PASS` | Repeats the runtime default. `importPagesPerPass` stays in `FokosRuntimeConfig`. |

### 8.3 Configurable

| Value | Where | Notes |
| --- | --- | --- |
| `maxHashKeyBytes`, `maxSortKeyBytes` | `FokosDBPolicy.limits` | The doc comment must warn: **never decrease a key size limit after items with larger keys exist.** The keys are stored in range boundaries, `doName` and `partition_id` values, and route evidence. An increase is allowed. |
| `promotionFraction` | `PartitionDO.fokosConfig()` | See 8.5. `fokosConfig()` is read at each use, and it can read `this.fokos.identity()` and `this.fokos.policy()`, so a subclass can choose a value for each table or each partition at runtime. It adds nothing to the wire. |
| Retry policy of the client | `FokosDBOptions.retry` | One `{ baseDelayMs, maxDelayMs, maxAttempts }`, used by every client retry. See 8.6. |
| Retry policy of the coordinator | `TransactionCoordinatorDO.fokosConfig().participantRetry` | One policy, used by every participant fan-out. See 8.6. |
| Stale transaction threshold | `fokosConfig()` of each DO class | One shared default constant. The coordinator checks that its value is at least its fan-out budget. |
| Fan-out request budget | `TransactionCoordinatorDO.fokosConfig()` | |
| TTL sweep | `PartitionDO.fokosConfig().ttlSweep` | |
| `maxClockSkewMs` | `PartitionDO.fokosConfig()` | A guard against a transaction timestamp too far in the future. DO side only. |
| P batch sizes: stale-lock scan (10), promoted-key cleanup (1000) | `PartitionDO.fokosConfig()` | Inline literals today. |
| Runtime: `fallbackAlarmMs`, `fastPathDelayMs`, `importPagesPerPass`, hash arena budget, `rangeHierarchyMaxRows`, promotion Bloom sizing, `maxForwardRetries`, the repartition retry intervals, `rangeHierarchyRefreshMs` | `fokosRuntimeConfig()` | The Bloom sizing is capped by the KV value limit. |
| DO-side platform budgets: migration page bytes, rows and scan rows, status page budgets, coordinator `sweepBatchRows`, `alarmRecoveryBudgetMs`, `maxDatabaseBytes` | `fokosRuntimeConfig()` or `fokosConfig()` | They cost nothing on the wire, and tests can make them small. Each one is capped by its platform limit. |

### 8.4 Global constants (for now)

| Value | Reason |
| --- | --- |
| `IDEMPOTENCY_WINDOW_MS` | Correctness. P cancels an owned `not_found` lock only when the lock is younger than the window, because T keeps its ledger for the window. That is safe only while the P window ≤ the T window. The policy reaches each DO on its next request, so a configurable window can be larger on P than on T for a time, and P can then cancel a committed transaction. |
| Transaction limits: `MAX_ITEMS_PER_TX`, `MAX_PAYLOAD_BYTES_PER_TX`, `MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX`, `MAX_ITEM_BYTES`, `MAX_CLIENT_REQUEST_TOKEN_BYTES` | Platform-bound, and the client checks them, so a setting would travel on the wire. Add them later. |
| Query ceilings and `EXPRESSION_LIMITS` | Same reason. Per-request options already lower the page budgets. |
| `REPARTITION_RPC_CONCURRENCY` | The Workers limit of six simultaneous outgoing connections. |
| The admission margin (1.1) | One named constant, shared by P and T. |
| The fixed constants of 3.9 | Formats and identities. |
| Topology bounds (`hashSplitN` and `rangeSplitN` in 2..255, `rangeAncestors` in 0..10) | Encoded in the partition ID. |
| Size estimators (`EST_ROW_BYTES_K`, `ITEM_ENVELOPE_BYTES`, the fixed overheads in the page and route-evidence estimators) | They model a format, and `EST_ROW_BYTES_K` feeds stored estimates. |
| `TX_COORDINATORS_PER_ROOT_TREE` | Only the default of `coordinatorRootsN`, which is already an option. |

### 8.5 The promotion fraction

A hash partition keeps a size estimate for each hash key. When the estimate of one key reaches `hashSplitConditions.maxSizeMb × promotionFraction` (`promotionFraction` from `PartitionDO.fokosConfig()`), the partition promotes the key: the key moves into a range tree of its own, and that tree splits by sort key. A hash split cannot divide one hash key, so promotion is the only way to relieve a large key.

- **A lower fraction** promotes keys earlier. The hash partition stays small and its splits stay fast. The cost is more range trees: more Durable Objects, more route hops for the promoted keys, and more entries in the promotion Bloom filter of each hash partition.
- **A higher fraction** keeps large keys in the hash partition for longer. Fewer keys are promoted. The risk is that one key fills most of the partition: a hash split moves that key to one child as a whole, the child is soon over the cap again, and the partition refuses writes above 1.1 × `maxSizeMb`.
- **Valid values** are in (0, 1). The default is 0.25: one key can use a quarter of a partition before it moves.
- A change applies to the next size evaluation. It does not move back a key that is already promoted.

### 8.6 Retries

The client and the coordinator retry for different reasons, so each has its own policy, and each policy is set in one place and used by every retry on that side.

- **Client** (`FokosDBOptions.retry`): a caller waits on each retry, so the policy is short. It covers the read phases of `transactGetItems` (5 attempts today) and `partition_migrating` from a coordinator (a 15-second deadline today, which must stay longer than the runtime `fallbackAlarmMs` of the coordinator).
- **Coordinator** (`fokosConfig().participantRetry`): a request-driven fan-out retries until the fan-out budget ends. `prepare` also stops after 3 attempts today, because an over-size answer does not change inside one budget. A job-driven fan-out has no caller, so it retries more.

Today the backoff `{ baseDelayMs: 100, maxDelayMs: 2_000 }` is written seven times, and the attempt counts are inline literals.

### 8.7 Defects

1. **The recovery budget of the coordinator does not bound one transaction.** `recoverStaleTransactions` checks `ALARM_RECOVERY_BUDGET_MS` (30 s) only between two transactions. It calls `drivePrepare`, `runCommit` and `runCancel` with no request budget, so `retryable(deadlineMs)` receives `Infinity` and allows `MAX_PARTICIPANT_ATTEMPTS_WITHOUT_DEADLINE` (100) attempts per participant, each after a backoff of up to 2 s. One unreachable participant can therefore hold one job step for minutes. The proposed fix: the job gives each call the time that remains of its budget, `recoveryStartedAt + ALARM_RECOVERY_BUDGET_MS - now`, as its `requestBudgetMs`, the same way a request passes its fan-out budget. The participant retries then stop at the budget, the transaction stays non-terminal, and the next job run continues it after the stale threshold. `MAX_PARTICIPANT_ATTEMPTS_WITHOUT_DEADLINE` can then go. A request-driven fan-out has the same shape and already works this way.
2. **`PartitionContextCreator.create` replaced caller values.** Fixed: each option now defaults on its own, and the options object of the caller stays unchanged. `hashSplitN` has no default, because it is part of the topology.
3. **The retry policies are inline.** See 8.6.

### 8.8 The `fokosRuntimeConfig()` convention

The runtime cannot call a method of its host: it does not know the host class. Its contract is the constructor callback `config: () => FokosRuntimeConfigOverrides`. `fokosRuntimeConfig()` is a naming convention for the host side, and a host passes it through:

```ts
this.fokos = new FokosShardingRuntime({ ctx, stub, hooks, operations, config: () => this.fokosRuntimeConfig() });
```

- The callback is an arrow function. A bare `this.fokosRuntimeConfig` loses its receiver.
- `PartitionDO` and `TransactionCoordinatorDO` follow the convention, and a later helper base class for runtime hosts (as `ShardedDurableObject` in `examples/http-api/src/demo2/sharded-do.ts` does for the RPC delegation) wires the callback itself. A subclass then only overrides `fokosRuntimeConfig()`.
- **The runtime can call the callback in its constructor.** The contract is that the callback returns valid values at any time, the construction included. A host whose override reads its own fields must make sure that those fields have their values before the runtime is created, for example with a field initializer or a value from `env`. The runtime validates what it receives, and a value that is not valid fails the construction. The same contract applies to `fokosConfig()` and the host constructors.
- The runtime still reads each value as late as the code allows. Today two reads happen at construction and can move to the first use with a small change:
  - `FokosShardingStore` receives `rangeHierarchyMaxRows` as a number in its constructor. It can receive a getter and read it at the eviction, its only use (`sharding-store.ts`). The value then changes at runtime like the others.
  - `PartitionDO` arms the TTL sweep in its constructor with `fokosTtlConfig().initialDelayMs`. Every public RPC and every alarm arms it again. `TtlExpiry.arm()` falls back to the literal `500`, a duplicate default. Milestone 7 renames the field to `ttlSweepDelayMs`, makes `arm()` default to it, and either removes the constructor call or gives it the fixed `TTL_SWEEP_CONSTRUCTOR_DELAY_MS` (500 ms).

### 8.9 Coverage

A second pass over `src/` for numeric literals and module constants found no other value that controls behaviour. The remaining literals are estimator overheads, SQL parameter positions, format offsets in `partition-id.ts`, and error-message text. Every value in section 3 now has a place in 8.3 or 8.4, or is removed by 8.2.
