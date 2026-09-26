# RFC — One configuration surface for FokosDB and FokosShardingRuntime

**State:** Draft
**Date:** 2026-09-26
**Author:** Lambros Petrou

**Status:** Not started, except one fix: `PartitionContextCreator.create` now defaults each option on its own and
keeps the options object of the caller unchanged (section 4.2.12).

## Table of contents

- [1. Overview and context](#1-overview-and-context)
  - [1.1 Glossary](#11-glossary)
- [2. Goals and requirements](#2-goals-and-requirements)
  - [2.1 In scope](#21-in-scope)
  - [2.2 Out of scope](#22-out-of-scope)
  - [2.3 Requirements](#23-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
  - [4.3 Open questions](#43-open-questions)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)
- [Appendix A. Inventory of the values today](#appendix-a-inventory-of-the-values-today)

## 1. Overview and context

`PartitionDO`, `TransactionCoordinatorDO`, `FokosShardingRuntime` and `FokosDB` use many module-level constants.
The constants are validation limits, page budgets, timeouts, retry intervals, and cache sizes. Appendix A lists
each one, its value, and the code that reads it.

A user can change only some of these values today, and each changeable value uses a different mechanism:

| Mechanism today | Values |
| --- | --- |
| `PartitionContextCreator.create` options, carried in the policy on every RPC | `hashSplitConditions`, `rangeSplitConditions`, `rangeSplitN`, `hashSplitN`, `rootTreesN`, `rangeAncestorsConfig`, `jurisdiction`, `locationHint` |
| `FokosDBOptions` (the constructor of the client) | `coordinatorRootsN`, `singlePartitionFastPath` |
| Per-call options | `limit`, `maxResponseBytes`, `clientRequestToken` |
| Overridable DO methods | `fokosStaleTransactionMs()`, `fokosTtlConfig()`, `fokosFanoutRequestBudgetMs()`, `fokosNow()` |
| `FokosShardingRuntime` constructor options | `caches.{hashArenaBytes, rangeHierarchyMaxRows, promotionBloom}`, `scheduler.{fallbackAlarmMs, fastPathDelayMs}` |
| `hooks.runtimeConfig()` callback | `importPagesPerPass` |

The current state has four problems:

- **No consistent way to override a value.** Four DO methods have four different shapes. The runtime has
  constructor options and one hook for the same kind of setting.
- **Hard-coded values that users need to change.** `FokosShardingRuntime` will become a library of its own, used by
  applications other than FokosDB. Its hosts need their own values for the settings that FokosDB leaves at the
  default.
- **Duplicates.** The migration host of `PartitionDO` repeats the runtime page budgets. The status page budget is
  defined twice. The retry backoff `{ baseDelayMs: 100, maxDelayMs: 2_000 }` is written seven times.
- **Coupled values without a check.** `MAX_PREPARING_HOLD_MS` derives from the stale threshold and the idempotency
  window. The client retry deadline for `partition_migrating` (15 s) must be longer than the runtime fallback alarm
  of the coordinator (5 s). `MAX_CLIENT_REQUEST_TOKEN_BYTES` must be at most `MAX_HASH_KEY_BYTES`. No code checks
  these relations.

Two facts of the platform limit the design:

- A user cannot pass arguments to a Durable Object constructor, because the Workers runtime creates the object. A
  user can only subclass the class, read `env`, or send data in a request.
- The caller Worker and the Durable Objects can run in different scripts (a DO binding with `script_name`). A value
  in the caller Worker reaches a DO only inside an RPC.

### 1.1 Glossary

- **C** — the caller Worker, which runs `FokosDB`.
- **P** — `PartitionDO`.
- **T** — `TransactionCoordinatorDO`.
- **R** — `FokosShardingRuntime`. It runs inside P and T, and inside any other host.
- **Host** — a Durable Object class that creates a `FokosShardingRuntime`.
- **Override** — a value that a user sets. Every value without an override takes its default.
- **Platform ceiling** — a Workers or Durable Objects limit above which a value is not valid.

## 2. Goals and requirements

### 2.1 In scope

- Each host has two override methods: `fokosConfig()` for the settings of the host, and `fokosRuntimeConfig()` for
  the settings of the runtime.
- `FokosShardingRuntime` takes one top-level `config` constructor callback. Every value that the runtime reads is in
  `FokosRuntimeConfig`.
- `fokosConfig()` replaces `fokosStaleTransactionMs()`, `fokosTtlConfig()` and `fokosFanoutRequestBudgetMs()`.
  `config` replaces the `caches` and `scheduler` constructor options and `hooks.runtimeConfig()`.
- The key size limits are overrides in `FokosDBPolicy.limits`. Only the overrides travel on the wire.
- The client has one retry policy in `FokosDBOptions.retry`, and a separate retry deadline for the
  `partition_migrating` answer of a coordinator. The coordinator has one retry policy in its `fokosConfig()`. Every
  retry on each side uses the policy of that side.
- The runtime gives the active value of a runtime setting to each hook that needs it.
- The promotion fraction is a setting in `PartitionDO.fokosConfig()`.
- The duplicate constants are merged. Unused options are removed.
- The recovery budget of the coordinator bounds each transaction (section 4.2.11).
- Every setting has a documented default, ceiling, and effect, and says whether a change is safe for a table that
  exists.
- The `FokosDb*` types are renamed to `FokosDB*`.

### 2.2 Out of scope

- **Transaction limits, query ceilings and `EXPRESSION_LIMITS` as settings.** The client checks them, so a setting
  travels on the wire. They stay constants for now. They can become settings later.
- **Server checks of the transaction limits and the query ceilings.** Only the client checks `MAX_ITEMS_PER_TX`,
  `MAX_PAYLOAD_BYTES_PER_TX` and the query ceilings. T and P do not check them again in this change.
- **A smaller range partition identity.** The size of the `partitionId` and the `doName` of a range partition grows
  with the keys (section 4.2.3). This change keeps both formats. It keeps the default key size limits, and it only
  logs a warning above 2 KiB.
- **A change to the TTL arming in the constructor of `PartitionDO`.** M7 keeps the constructor arming as it is.
- **A configurable idempotency window.** Section 4.2.8 gives the correctness reason.
- **A smaller route context.** A hash of `rangeConfig` and `policy` in place of the full values is a separate
  change. The route context measures 325 bytes today, which does not need it.
- **A per-call deadline or `AbortSignal`.** It can override the client retry budget later.
- **A helper base class for runtime hosts.** Section 4.2.1 defines the convention that such a class will use.

### 2.3 Requirements

- A table with no overrides must send zero extra bytes in an RPC.
- A host setting and a runtime setting must add no bytes to an RPC.
- A host reads `fokosConfig()` at each use. A subclass can therefore return a value that depends on the partition,
  for example from `this.fokos.identity()` or `this.fokos.policy()`, or from `this.env`.
- The `config` callback and `fokosConfig()` must return valid values at any time, the construction of the host
  included. The runtime can call the callback in its constructor.
- The runtime and the hosts must validate each value against its range, its platform ceiling, and its coupled
  values. A value that is not valid fails the construction, or fails the operation that reads it.
- `FokosShardingRuntime` must not import FokosDB code. Its configuration type belongs to the runtime.
- A DO that runs an older package version must accept a policy from a newer client. It ignores the fields that it
  does not know.
- `pnpm test` must pass after each milestone.

## 3. Milestones

Each milestone is one change that a reviewer can approve on its own.

### M1 — Rename the `FokosDb*` types

Rename `FokosDbPolicy`, `FokosDbRouteContext`, `FokosDbStubContext`, `FokosDbTableConfig`, `FokosDbHostCursor`,
`FokosDbHostPage` and `FokosDbMigrationHost` to the `FokosDB*` spelling. Three of them are exported from
`fokosdb/client`, so the public types change. Each new type also uses the `FokosDB` spelling.

### M2 — Runtime configuration

Add `FokosRuntimeConfig` and the `config` constructor callback (section 4.2.1). Move the `caches` and `scheduler`
options, `importPagesPerPass`, the repartition retry intervals, the page and status budgets, `maxForwardRetries` and
`rangeHierarchyRefreshMs` into it. Remove `hooks.runtimeConfig()` and `PartitionDO.IMPORT_PAGES_PER_PASS`. Merge the
duplicate page and status constants. `MigrationHost.buildPage` gets the active page budgets as an argument. P and T
implement `fokosRuntimeConfig()`.

### M3 — Host configuration

Add `fokosConfig()` to P and T with the settings of section 4.2.2. Remove `fokosStaleTransactionMs()`,
`fokosTtlConfig()` and `fokosFanoutRequestBudgetMs()`. Add the shared stale-transaction default, the shared
admission margin constant, the coordinator retry policy, and the coordinator batch settings `sweepDeleteChunkRows`
and `recoveryScanRows`. Remove `SplitConditions.maxItems`.

### M4 — Recovery budget of the coordinator

Fix the defect in section 4.2.11. It is a separate milestone because it changes the retry flow of the
`tx_recovery` job and its tests.

### M5 — Client configuration

Add `FokosDBOptions.retry` and `FokosDBOptions.partitionMigratingRetryDeadlineMs` (section 4.2.5). Add the key size
overrides in `FokosDBPolicy.limits`, with `resolveLimits`, its cache, and the warning above 2 KiB (section 4.2.3).

### M6 — Documentation

Document every setting: its default, its ceiling, what it affects, and whether a change is safe for a table that
exists.

### M7 — Late reads

Move the two configuration reads that happen at construction to their first use (section 4.2.13). This milestone
is last because each change alters a flow and its tests.

## 4. Proposed solution

### 4.1 High-level overview

Each value has one owner, and the owner decides where the value lives. The owner is the set of parties that must
agree on the value.

| Layer | Where a user sets it | Who reads it | Wire cost |
| --- | --- | --- | --- |
| Table | `PartitionContextCreator.create` options → `FokosDBPolicy` | C, P, T | Only the overrides |
| Host | `fokosConfig()` on P or T | That host | None |
| Runtime | `fokosRuntimeConfig()` on the host → the `config` callback | R | None |
| Client | `FokosDBOptions` | C | None |
| Request | Per-call options | C, then the DO that serves the call | The request |
| Constant | Not settable | Everyone | None |

```
Caller Worker (C)                           Durable Object host (P or T)
+----------------------------+              +---------------------------------------------+
| FokosDBOptions             |              | fokosConfig()        -> host settings       |
|   retry                    |              | fokosRuntimeConfig() -> config callback     |
|   coordinatorRootsN, ...   |   RPC with   |                                             |
| FokosDBPolicy              | -----------> | FokosShardingRuntime({ ctx, stub, hooks,    |
|   limits (overrides only)  | route context|                        operations, config })|
| resolveLimits() (cached)   |              | resolveLimits(this.fokos.policy()) (cached) |
+----------------------------+              +---------------------------------------------+
```

The table layer holds only the key size limits, because only the key sizes are both settable and checked by more
than one party. A table without overrides sends no `limits` field. Each side builds the full limits from the
overrides with one shared function, and caches the result.

The host and runtime layers hold every other setting. They cost nothing on the wire, so they also hold the DO-side
platform budgets, which tests can make small.

Some values stay constants: values that encode a format, values that platform limits fix, and values that two DO
classes must share with no delay. The idempotency window is the main example.

### 4.2 Technical details

#### 4.2.1 Runtime configuration: `FokosRuntimeConfig` and the `config` callback

The runtime constructor takes five top-level options:

```ts
this.fokos = new FokosShardingRuntime({ ctx, stub, hooks, operations, config: () => this.fokosRuntimeConfig() });
```

`config` returns `FokosRuntimeConfigOverrides`, a partial of `FokosRuntimeConfig`. The runtime merges the overrides
with its defaults and validates the result.

`config` is a top-level option and not a member of `hooks`, for three reasons:

- `operations` declares the API of the host and the behaviour of each operation.
- `hooks` holds the policy decisions and the lifecycle steps that the host implements. Each hook receives an input
  and returns a decision, and four hooks run inside `transactionSync`.
- `config` returns data, not a decision. It covers the scheduler, the caches, the repartition flow and the
  forwarding, which are not lifecycle steps of a partition.

A setting that applies to one operation stays in the descriptor of that operation. It can take its default from
`config`.

**Active values in hooks.** When a hook needs a runtime setting, the runtime gives the hook the active value as an
argument. The flow is: `fokosRuntimeConfig()` → the `config` callback → the runtime resolves and validates the
values → the runtime calls the hook with the values it uses. The runtime stays the owner of the value, and the host
never resolves the runtime configuration itself.

The migration page budgets are the first case. `MigrationHost.buildPage` gets a fourth argument:

```ts
buildPage(
	cursor: unknown,
	slice: FokosSlice,
	belongsToTarget: (key: RouteKey) => boolean,
	budget: { pageBytes: number; pageRows: number; scanRows: number },
): { page: unknown; nextCursor: unknown };
```

The migration host of P (`fokos-migration-host.ts`) and the migration page of T (`buildMigrationPage` in
`do-transaction-coordinator.ts`) read their budgets from this argument. This removes `PAGE_BYTES`, `PAGE_ROWS` and
`SCAN_ROWS` from `fokos-migration-host.ts`, and the imports of `FOKOS_PAGE_BYTES` and `FOKOS_PAGE_ROWS` from T.

The runtime cannot call a method of its host, because it does not know the host class. The callback is its
contract. `fokosRuntimeConfig()` is the naming convention on the host side:

- The host passes an arrow function. A bare `this.fokosRuntimeConfig` loses its receiver.
- P and T follow the convention. A later helper base class for runtime hosts can wire the callback itself, as
  `ShardedDurableObject` in `examples/http-api/src/demo2/sharded-do.ts` wires the RPC delegation today. A subclass
  then only overrides `fokosRuntimeConfig()`.
- The runtime can call the callback in its constructor. When an override reads a field of the host, the host must
  give that field its value before it creates the runtime, for example with a field initializer or a value from
  `env`.

The runtime reads a value that sizes a structure once, when it first creates the structure. It reads every other
value at each use.

The settings of `FokosRuntimeConfig`:

| Setting | Default | Notes |
| --- | --- | --- |
| `fallbackAlarmMs` | 5,000 ms | How far ahead a pass arms its fallback alarm. The client retry deadline for `partition_migrating` depends on the value in T (section 4.2.5). |
| `fastPathDelayMs` | 50 ms | The delay of the in-memory fast path that runs a pass without an alarm. |
| `importPagesPerPass` | 16 | The import pages that one pass applies. Minimum: 1. |
| Hash arena budget | 1 MiB | Read when the arena is created. |
| `rangeHierarchyMaxRows` | 10,000 | The row bound of the learned range hierarchy. |
| Promotion Bloom sizing | 300,000 keys at a 1% false positive rate | Read when the filter is created. The serialized filter is one KV value and has a ceiling of 1.5 MiB, below the 2 MB KV value limit. |
| `maxForwardRetries` | 8 | The retries of one call after a speculative or cached forward misses. |
| `rangeHierarchyRefreshMs` | 60,000 ms | A learn refreshes `learned_at` only on a row older than this. |
| Repartition retry intervals | Source: 5 s backoff to 5 min. Import: 10 s backoff to 5 min. Lock: 5 s. Cleanup: 5 s. Not cut over: 10 s. Non-retryable: 5 min. | Today `SOURCE_RETRY_*`, `IMPORT_RETRY_*`, `LOCK_RETRY_MS`, `CLEANUP_RETRY_MS`, `NOT_CUT_OVER_RETRY_MS`, `NON_RETRYABLE_RETRY_MS`. |
| Migration page budgets | 20 MiB, 1,000 rows, 10,000 scanned rows | Today `FOKOS_PAGE_BYTES`, `FOKOS_PAGE_ROWS`, `FOKOS_SCAN_ROWS`. Capped by the Workers RPC message size. |
| Status page budgets | 1,000 entries, 20 MiB | Today `STATUS_PAGE_ENTRIES` and two definitions of `STATUS_PAGE_BYTES`. |

This spec gives a field name only where the decision gave one. M2 names the other fields.

#### 4.2.2 Host configuration: `fokosConfig()`

Each host has one overridable method. It returns a partial of the host settings. A shared helper merges it with the
defaults and validates it.

```ts
class PartitionDO {
	/** Override to change the settings of this class. Read at each use. */
	protected fokosConfig(): PartitionDOConfigOverrides {
		return {};
	}
}
```

- The host reads the method at each use, and does not cache the result. A subclass can therefore vary a value at
  runtime, and a test subclass keeps its control of one instance with no production hook for tests.
- The method can read `this.env`, so a deployment can set values from `vars` with no subclass.
- The validation is a few number checks, so it runs at each read. If a profile shows a cost, the host can cache on
  the identity of the returned object.
- `fokosNow()` stays a separate method, because it is a clock and not a setting.

The settings of `PartitionDO`:

| Setting | Default | Notes |
| --- | --- | --- |
| `staleTransactionMs` | 5,000 ms | How long a prepared lock waits before the stale sweep asks its coordinator to resolve it. Shared default with T (section 4.2.6). |
| `promotionFraction` | 0.25 | Section 4.2.7. |
| `maxClockSkewMs` | 5,000 ms | The farthest into the future that a transaction timestamp can be when P accepts a prepare. It must be larger than the real clock skew between C, T and P. |
| `ttlSweep` | See below | Today `fokosTtlConfig()`. |
| Stale-lock scan batch | 10 | Inline literal today. |
| Promoted-key cleanup batch | 1,000 | Inline literal today. |

`ttlSweep` has the fields of `TtlSweepConfig`: `chunkSize` (100), `sleepMs` (1,000), `maxRowsBeforeSleep` (10,000),
`maxBytesBeforeSleep` (50 MiB), `maxRowsPerCycle` (100,000), and `initialDelayMs` (500). M7 renames `initialDelayMs`.

The settings of `TransactionCoordinatorDO`:

| Setting | Default | Notes |
| --- | --- | --- |
| `staleTransactionMs` | 5,000 ms | Today `STALE_THRESHOLD_MS`. It must be at least `fanoutRequestBudgetMs`. |
| `fanoutRequestBudgetMs` | 5,000 ms | Today `fokosFanoutRequestBudgetMs()`. |
| `participantRetry` | Section 4.2.5 | |
| `alarmRecoveryBudgetMs` | 30,000 ms | Today `ALARM_RECOVERY_BUDGET_MS`. It must stay below the wall-clock limit of a DO alarm. |
| `sweepBatchRows` | 1,000 | Today `SWEEP_BATCH_ROWS`. The rows that one step of the idempotency sweep and one step of the source cleanup read. |
| `sweepDeleteChunkRows` | 100 | Today `CHUNK_SIZE` in `sweepExpiredTransactions`. The transaction IDs bound into one `DELETE … IN (…)` statement. Ceiling: 100, the bound-parameter limit of one SQLite statement in a DO. |
| `recoveryScanRows` | 100 | Today the `LIMIT 100` in `recoverStaleTransactions`. The non-terminal transactions that one step of the `tx_recovery` job reads. |
| `maxDatabaseBytes` | 5 GiB | Today `MAX_TC_DATABASE_BYTES`, half of the 10 GB storage limit of a DO. Capped by that limit. |

`MAX_PREPARING_HOLD_MS` stays derived: `min(5 × staleTransactionMs, IDEMPOTENCY_WINDOW_MS)`. It is not a setting.

#### 4.2.3 Table limits: `FokosDBPolicy.limits`

`FokosDBPolicy.limits?: FokosDBLimitOverrides` is a flat, partial record. It holds the key size limits:

| Setting | Default | Checked by |
| --- | --- | --- |
| `maxHashKeyBytes` | 1,024 | C (`encodeHashKey`), T (the coordinator key of the token) |
| `maxSortKeyBytes` | 512 | C |

The limits are measured on the encoded key bytes. An increase is allowed. A decrease is not safe after items with
larger keys exist, because the keys are stored in range boundaries, `doName` values, `partition_id` values, and
route evidence. The doc comment of each field must say: never decrease a key size limit after items with larger keys
exist.

The key size limits have no hard ceiling. When an override is above 2 KiB (2,048 bytes), `resolveLimits` logs a
warning, once for each resolved policy. A large key has these effects:

- **The size of a range partition identity.** A range partition has two names, and both contain three keys: the
  hash key, the start boundary, and the end boundary. A boundary is a sort key. `partition-id.ts` builds them:
  - The `partitionId` is the hex form of 10 header bytes plus the three raw keys. Hex doubles each byte.
  - The `doName` is `<shardGroup>.r.<hk>.<start>.<end>`. Each component keeps a safe ASCII byte as one character and
    percent-encodes every other byte as three characters. A text key costs 1 character for each byte, and a binary
    key costs 3.

  | Key sizes (hash key, sort key) | Raw bytes | `partitionId` | `doName`, text keys | `doName`, binary keys |
  | --- | --- | --- | --- | --- |
  | 1 KiB, 512 B (the defaults) | about 2 KiB | about 4 KiB | about 2 KiB | about 6 KiB |
  | 2 KiB, 2 KiB | about 6 KiB | about 12 KiB | about 6 KiB | about 18 KiB |

  A Durable Object name has no length limit. The runtime truncates `ctx.id.name` to its first 1,024 bytes, so
  FokosDB reads `ctx.id.name` only for logs and error attributes, and never for identity or routing.
- **The route context of each RPC.** The route context of a request to a range partition carries its `partitionId`
  and its `doName`, so each request grows with the keys.
- **Route evidence.** `routeNodeBytes` counts 2 bytes for each character of `doName` and `partitionId`, and the bytes
  of each ancestor boundary. Above `ROUTE_EVIDENCE_MAX_BYTES` (10 KiB) the envelope truncates the list of nodes. The
  request still succeeds, but the caller learns fewer boundaries, and later requests take more hops. With the default
  limits, one node of a range partition whose keys are at the maximum already counts about 12 KiB (text keys) to
  20 KiB (binary keys), which is above the cap.
- **The item size.** The stored size of an item counts both keys against `MAX_ITEM_BYTES` (400 KiB), so a larger key
  leaves less room for data.
- **Stored rows.** The learned range hierarchy holds up to `rangeHierarchyMaxRows` (10,000) rows, each with a hash
  key and two boundaries. Lock rows, `tc_items` rows, and query cursors also hold keys.
- **Index performance.** The SQLite indexes of the items hold the keys. A key larger than about 1 KiB does not fit in
  one index cell of a 4 KiB page, so each index lookup also reads overflow pages. TODO: measure.

**The wire.** The record is flat, because a nested object adds a tag and a key for each level.
`PartitionContextCreator.create` validates the overrides. When there are no overrides, it omits `limits`. When an
override is equal to the default, it keeps the override, so that a pinned value stays when a later version changes
the default.

Measured with `v8.serialize`, the structured-clone format of Workers RPC:

| What travels | Bytes |
| --- | --- |
| The whole route context today | 325 |
| All the limits as one nested object | 616 |
| All the limits as one positional array | 139 |
| Only the overrides, two values | 35 |
| Only the overrides, none (the field is absent) | 0 |

**The helper.** One function in `shared/` builds the full, frozen limits:

```ts
export function resolveLimits(overrides: FokosDBLimitOverrides | undefined): FokosDBLimits;
```

1. It starts from the defaults of the package version that runs it.
2. It applies the overrides and ignores the keys that it does not know. A newer client can send them to an older DO.
3. It checks each value against its ceiling and its coupled values. `maxHashKeyBytes` must be at least
   `MAX_CLIENT_REQUEST_TOKEN_BYTES` (64), because the token is a hash key of the coordinator.
4. With no overrides, it returns the one frozen `DEFAULT_LIMITS` object and does no other work.

The validation functions in `transaction-limits.ts` take the resolved limits as an argument. The constants stay as
the exported defaults.

**The cache.** No side resolves per request:

- `FokosDB` resolves once in its constructor, from the policy of its router.
- A DO resolves from `this.fokos.policy()`. The runtime compares the incoming policy with `structurallyEqual` on each
  request. While nothing changes, it keeps the same stored object. A `WeakMap<FokosDBPolicy, FokosDBLimits>` keyed on
  the stored object therefore resolves once per policy change. A key on the incoming object does not work, because
  each RPC delivers a new object.

**Different defaults across versions.** A client and a DO on different package versions can resolve different
defaults. This is acceptable. The client check is an early answer, and the DO check decides. A difference changes
where a request fails, not what the DO stores.

#### 4.2.4 Client configuration: `FokosDBOptions`

`FokosDBOptions` keeps `coordinatorRootsN` and `singlePartitionFastPath`, and adds `retry` and
`partitionMigratingRetryDeadlineMs`. Section 4.2.5 defines both. The per-call options `limit`, `maxResponseBytes`
and `clientRequestToken` stay as they are.

#### 4.2.5 Retry policies

The client and the coordinator retry for different reasons. Each side has one policy, set in one place, and every
retry on that side uses it.

- **Client.** A caller waits on each retry, so the policy is short.

  | Setting | Default | Used by |
  | --- | --- | --- |
  | `retry.baseDelayMs` | 100 ms | Every client retry |
  | `retry.maxDelayMs` | 2,000 ms | Every client retry |
  | `retry.maxAttempts` | 5 | The two read phases of `transactGetItems` |
  | `partitionMigratingRetryDeadlineMs` | 15,000 ms | `transactWriteItems`, while its coordinator answers `partition_migrating`. Today `TX_COORDINATOR_MIGRATING_RETRY_MS`. |

  `partitionMigratingRetryDeadlineMs` is a separate setting, because a coordinator split is a separate event: the
  client retries until a deadline, not for a number of attempts. The deadline must stay longer than
  `fallbackAlarmMs` of the coordinator, so that an import that a crash stopped can finish inside it. The client
  cannot read a DO setting, so the documentation of both settings must state the relation.

- **Coordinator** (`TransactionCoordinatorDO.fokosConfig().participantRetry`).

  | Setting | Default | Used by |
  | --- | --- | --- |
  | `participantRetry.baseDelayMs` | 100 ms | Every participant retry |
  | `participantRetry.maxDelayMs` | 2,000 ms | Every participant retry |
  | `participantRetry.prepareMaxAttempts` | 3 | The first prepare fan-out, in `drivePrepare` |
  | `participantRetry.prepareRecoveryMaxAttempts` | 5 | The prepare fan-out to the participants with no answer, in `runPrepareRecovery` |

  Both prepare paths also stop at once on `partition_over_size`, because an over-size answer does not change inside
  one budget. The commit and cancel fan-outs (`runCommit`, `runCancel`) have no attempt count. They retry until a
  deadline: `fanoutRequestBudgetMs` when a request drives them, and the remaining `alarmRecoveryBudgetMs` when the
  `tx_recovery` job drives them (section 4.2.11).

Today the backoff `{ baseDelayMs: 100, maxDelayMs: 2_000 }` is written three times in `db.ts` and four times in
`do-transaction-coordinator.ts`, and the attempt counts are inline literals.

#### 4.2.6 The stale-transaction threshold

`STALE_TX_MS` in P and `STALE_THRESHOLD_MS` in T are both 5 s, and both mean "a transaction that no request drives".
One constant in `shared/` holds the default, and both hosts expose `staleTransactionMs` in `fokosConfig()`.

T checks that its `staleTransactionMs` is at least its `fanoutRequestBudgetMs`. A smaller value makes the stale path
drive a transaction that a request still drives.

#### 4.2.7 The promotion fraction

A hash partition keeps a size estimate for each hash key. When the estimate of one key reaches
`hashSplitConditions.maxSizeMb × promotionFraction`, the partition promotes the key. The key moves into a range tree
of its own, and that tree splits by sort key. A hash split cannot divide one hash key, so promotion is the only way
to relieve a large key.

- **A lower fraction** promotes keys earlier. The hash partition stays small and its splits stay short. The cost is
  more range trees: more Durable Objects, more route hops for the promoted keys, and more entries in the promotion
  Bloom filter of each hash partition.
- **A higher fraction** keeps large keys in the hash partition for longer, and promotes fewer keys. The risk is one
  key that fills most of the partition. A hash split moves that key to one child as a whole, the child is soon over
  its cap again, and the partition refuses writes above 1.1 × `maxSizeMb`.
- **Valid values** are in (0, 1). The default is 0.25: one key can use a quarter of a partition before it moves.
- A change applies to the next size evaluation. It does not move back a key that is already promoted.

The setting is in `PartitionDO.fokosConfig()` and not in the policy. `fokosConfig()` runs at each use, so a subclass
can choose a value per table or per partition from `this.fokos.identity()` and `this.fokos.policy()`. It adds no
bytes to the wire.

#### 4.2.8 Constants

These values stay constants:

| Value | Reason |
| --- | --- |
| `IDEMPOTENCY_WINDOW_MS` (10 min) | Correctness. P cancels an owned lock that its coordinator reports as `not_found` only when the lock is younger than the window, because T keeps its ledger for the window. A lock is older than the completion of its transaction, so the cancel is safe only while the P window is at most the T window. The policy reaches each DO on the next request to it, so a configurable window can be larger on P than on T for a time. P can then cancel a committed transaction. The value is also DynamoDB parity. |
| `MAX_ITEMS_PER_TX` (100), `MAX_PAYLOAD_BYTES_PER_TX` (4 MiB), `MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX` (10 MiB), `MAX_ITEM_BYTES` (400 KiB), `MAX_CLIENT_REQUEST_TOKEN_BYTES` (64) | Platform ceilings apply, and the client checks them, so a setting travels on the wire. They can become settings later. |
| Query ceilings (`MAX_EVALUATED_ITEMS_PER_PAGE`, `MAX_EVALUATED_BYTES_PER_PAGE`, `MAX_RESPONSE_BYTES_PER_PAGE`, `MAX_PARTITION_VISITS_PER_PAGE`) and the query defaults | Same reason. `limit` and `maxResponseBytes` already lower the page budgets per request. |
| `EXPRESSION_LIMITS` | Same reason. `compiledSqlBytes` (100,000) and `completeStatementBindings` (100) are the SQLite statement length and bound-parameter limits of a DO. |
| `REPARTITION_RPC_CONCURRENCY` (6) | The Workers limit of six simultaneous outgoing connections for one request. |
| The admission margin (1.1) | One named constant, shared by P and T. It replaces four inline literals. |
| `TX_ORDER_TS_UNITS_PER_MS`, key codec tags, `DATA_KINDS`, plan versions and fixed binding counts, `CURSOR_VERSION`, `COORDINATOR_REF_VERSION`, `OPERATIONS_SEED`, the hash seeds and `GOLDEN_RATIO`, `RANGE_MIN` and `RANGE_MAX`, `RESERVED_SHARD_GROUP_PREFIX`, the KV key names, the job names, the error code tables | Formats and identities. A change breaks stored data or routing. |
| `FOKOS_HASH_PARTITIONS_MAX` (65,000) and the topology bounds (`hashSplitN` and `rangeSplitN` in 2..255, `rangeAncestors` in 0..10) | The partition ID encodes them. |
| `ROUTE_EVIDENCE_MAX_BYTES` (10 KiB) and the Bloom filter internals | Formats. |
| `EST_ROW_BYTES_K` (108), `ITEM_ENVELOPE_BYTES` (64), the fixed overheads of the page and route evidence estimators | They model a format. `EST_ROW_BYTES_K` feeds the stored `key_size_estimates`, and a change of `ITEM_ENVELOPE_BYTES` moves page boundaries. |
| `TX_COORDINATORS_PER_ROOT_TREE` (2) | Only the default of `coordinatorRootsN`, which is already an option. |

#### 4.2.9 Removals and merges

| Change | Reason |
| --- | --- |
| Remove `SplitConditions.maxItems` | `PartitionContextCreator` validates it, and no code reads it. |
| Remove `PartitionDO.IMPORT_PAGES_PER_PASS` and `hooks.runtimeConfig()` | P passes 16, which is the runtime default. `importPagesPerPass` stays in `FokosRuntimeConfig`. |
| Remove the `caches` and `scheduler` constructor options | `config` replaces them. |
| `PAGE_BYTES`, `PAGE_ROWS`, `SCAN_ROWS` in `fokos-migration-host.ts` use the runtime page budgets | The values are the same as `FOKOS_PAGE_*`, which T already imports. |
| One status page budget | `runtime.ts` and `repartition-flow.ts` both define `STATUS_PAGE_BYTES`. |
| One retry policy per side | Section 4.2.5. |
| One stale-transaction default | Section 4.2.6. |
| One admission margin constant | Section 4.2.8. |

#### 4.2.10 The route context stays as it is

The route context travels in every RPC. This spec adds one optional field, `FokosDBPolicy.limits`, which is absent
for a table with defaults. The DO stores the policy and compares it with `structurallyEqual`. A key whose value is
`undefined` counts as absent, so a policy without `limits` compares equal to a stored policy without `limits`.

#### 4.2.11 The recovery budget of the coordinator

`recoverStaleTransactions` in `do-transaction-coordinator.ts` checks `ALARM_RECOVERY_BUDGET_MS` (30 s) only between
two transactions. It drives each transaction with no request budget. The prepare paths stop after a fixed number of
attempts (section 4.2.5), but `runCommit` and `runCancel` pass an infinite deadline to `retryable`, which then allows
`MAX_PARTICIPANT_ATTEMPTS_WITHOUT_DEADLINE` (100) attempts for each participant. Each attempt waits a backoff of up
to 2 s. One participant that does not answer can hold one job step for minutes.

The fix:

1. The job computes the time that remains of its budget: `recoveryStartedAt + alarmRecoveryBudgetMs - now`.
2. The job passes that time as the `requestBudgetMs` argument of each call, and `runCommit` and `runCancel` use it
   as their deadline. A request passes its fan-out budget in
   the same argument today.
3. The participant retries stop at the end of the budget. The transaction stays non-terminal, and the next run of
   the job continues it after `staleTransactionMs`.
4. `MAX_PARTICIPANT_ATTEMPTS_WITHOUT_DEADLINE` is removed. Before the removal, M4 must check the other paths that
   call with no request budget.

#### 4.2.12 `PartitionContextCreator.create` defaults (done)

Before the fix, `PartitionContextCreator.create` set `rangeSplitN` to 4 when `rangeSplitConditions` was absent,
also when the caller gave `rangeSplitN`. It did the same to `hashSplitN`, and it changed the options object of the
caller.

Now each option takes its default on its own, and the options object of the caller stays unchanged. `hashSplitN` has
no default, because it is part of the topology and must never change. A call without `hashSplitN` fails with
`partition_context_options_invalid`. `src/shared/partition-context.test.ts` covers the three cases.

#### 4.2.13 Late reads (M7)

Two reads of configuration happen at construction today. The contract of section 4.2.1 allows them. M7 moves them
to their first use, so that the values follow the rule "read at each use":

- **`rangeHierarchyMaxRows`.** `FokosShardingStore` receives it as a number in its constructor. Its only use is the
  eviction after a write to the learned range hierarchy, in `sharding-store.ts`. The store receives a getter and
  reads the value at the eviction. The `>= 1` check moves with the read. The test in `sharding-store.test.ts` that
  passes 3 changes to match.
- **The TTL sweep delay.** The constructor of `PartitionDO` arms the sweep with `fokosTtlConfig().initialDelayMs`.
  Every public RPC and every alarm arms it again, and `TtlExpiry.arm()` falls back to the literal `500` there.
  - `TtlSweepConfig.initialDelayMs` is renamed to `ttlSweepDelayMs`.
  - `TtlExpiry.arm()` takes its default delay from `ttlSweepDelayMs`, in place of the literal `500`.
  - The arming in the constructor of `PartitionDO` stays as it is (section 2.2).

#### 4.2.14 Testing

- Each milestone passes `pnpm test`.
- The test subclasses `test/controlled-partition-do.ts` and `test/controlled-transaction-coordinator-do.ts` override
  `fokosStaleTransactionMs()` and `fokosFanoutRequestBudgetMs()` today. M3 moves them to `fokosConfig()`.
- The DO-side budgets are settings, so a test can make a page, a batch, or a budget small and reach a boundary with
  little data.
- Each validation rule gets a unit test: a value out of range, a value above its ceiling, and each coupled relation.
- `resolveLimits` gets unit tests for no overrides (the result is `DEFAULT_LIMITS`), unknown keys, the cache, and the
  warning for a key size limit above 2 KiB.
- A test gives `buildPage` a small budget through `fokosRuntimeConfig()` and checks that a migration takes more pages.
- M4 needs a test in which one participant does not answer, and the job step ends inside `alarmRecoveryBudgetMs`.

#### 4.2.15 Compatibility and rollout

- M1 changes public type names. It changes no runtime behaviour.
- M2 and M3 change the constructor options of the runtime and the override methods of the hosts. A subclass that
  overrides a removed method must move to `fokosConfig()`.
- M5 adds an optional wire field. An older DO ignores it. A table with no overrides sends the same bytes as today.
- A setting that changes during the life of a table applies at its next read. The documentation of M6 says, for
  each setting, whether a change is safe for a table that exists.

### 4.3 Open questions

No open questions. One TODO item stays in section 4.2.3: a measurement of the index cost of large keys.

## 5. Alternative options

### 5.1 One isolate-global `FokosConfig` with setters

Rejected. The caller Worker and the Durable Objects can run in different scripts, so a setter in the caller never
reaches the DO isolate. Inside one isolate, every table and every DO class of the script shares the value, so two
tables cannot differ. The order of a setter and the first request is not certain, and a change after the first
request makes the objects of one isolate disagree. It also makes test isolation difficult.

### 5.2 Constructor arguments everywhere

Rejected as the only mechanism. It works for `FokosDB` and `FokosShardingRuntime`, because user or host code creates
them. It cannot work for a Durable Object class, because the Workers runtime creates it.

### 5.3 All the limits in the policy

Rejected. The full nested object measures 616 bytes and almost triples the route context of 325 bytes. The client
cannot see a DO method, so the table layer holds only the values that the client and the DOs must share.

### 5.4 A positional array on the wire

Rejected. It measures 139 bytes on every request, also when nothing is overridden, and each index must stay fixed for
the life of the format.

### 5.5 Short key names on the wire

Rejected. It makes every reader of the code worse, and the overrides-only record already costs about 15 bytes for
each override.

### 5.6 `config` inside `hooks`

Rejected. Section 4.2.1 gives the reason.

### 5.7 The promotion fraction in the policy

Rejected. `PartitionDO.fokosConfig()` gives the same per-table control at runtime, with no bytes on the wire
(section 4.2.7).

### 5.8 A configurable idempotency window

Rejected. Section 4.2.8 gives the correctness reason.

### 5.9 One `fokosConfig()` that also holds the runtime settings

Rejected. The runtime becomes a library of its own and must not depend on the host configuration type. The runtime
settings therefore have their own method, `fokosRuntimeConfig()`, and their own callback.

## 6. Frequently asked questions

**Why can the runtime call the configuration callback in its constructor?**
The contract is simpler for a host when every read is valid. The host is responsible for values that are ready
before it creates the runtime.

**Why does a host read `fokosConfig()` at each use?**
A subclass can then change a value from the partition, from `env`, or from a test, with no restart. The read is a
few number checks.

**What happens when a client and a DO have different default limits?**
The client check is an early answer, and the DO check decides. The request fails in a different place. The stored
data does not change.

**Why are the key sizes settable when the transaction limits are not?**
Users can need longer keys in their applications. The transaction limits have platform ceilings, and there is no
request to change them yet.

**Why does the runtime give the page budgets to `buildPage`, and not the host read them?**
The runtime owns its settings. It resolves and validates them once, and every hook sees the same values that the
runtime uses.

**Why does the client not read `fallbackAlarmMs` to set its retry deadline?**
The client cannot read a DO setting. The documentation of both settings states the relation.

## 7. References

References:

- `docs/ideas/2026-09-25-configuration-surface.md` — the inventory and the discussion that this spec comes from.
- `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md` — the sharding runtime.
- `docs/agent-plans/2026-09-09-bounded-preparing-hold.md` — `MAX_PREPARING_HOLD_MS`.
- `docs/agent-plans/2026-08-30-item-ttl-expiration.md` — the TTL sweep.
- `examples/http-api/src/demo2/sharded-do.ts` — the RPC delegation of a runtime host.
- [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Workers limits](https://developers.cloudflare.com/workers/platform/limits/)

## Appendix A. Inventory of the values today

The users: C, P, T and R (section 1.1).

### A.1 Item, key and transaction values (`shared/transaction-limits.ts`)

| Constant | Value | Users |
| --- | --- | --- |
| `MAX_HASH_KEY_BYTES` | 1,024 | C (`encodeHashKey`), T (token key) |
| `MAX_SORT_KEY_BYTES` | 512 | C |
| `MAX_ITEM_BYTES` | 400 KiB | C (a lower-bound check), P (`partition-store.ts`, `transaction-participant.ts`) |
| `MAX_ITEMS_PER_TX` | 100 | C |
| `MAX_PAYLOAD_BYTES_PER_TX` | 4 MiB | C |
| `MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX` | 10 MiB | P, T (`applyImageCap`) |
| `MAX_CLIENT_REQUEST_TOKEN_BYTES` | 64 | C |
| `TX_ORDER_TS_UNITS_PER_MS` | 1,000 | C, P, T |
| `IDEMPOTENCY_WINDOW_MS` | 10 min | T (ledger sweep, `MAX_PREPARING_HOLD_MS`), P (lock-age quarantine) |
| `SWEEP_BATCH_ROWS` | 1,000 | T |
| `ALARM_RECOVERY_BUDGET_MS` | 30 s | T |

### A.2 Transaction timing in the hosts

| Constant | Value | Users | Today |
| --- | --- | --- | --- |
| `STALE_TX_MS` | 5 s | P | `fokosStaleTransactionMs()` |
| `STALE_THRESHOLD_MS` | 5 s | T | constant |
| `MAX_PREPARING_HOLD_MS` | min(25 s, window) | T | derived |
| `TX_FANOUT_REQUEST_BUDGET_MS` | 5 s | T | `fokosFanoutRequestBudgetMs()` |
| `MAX_PARTICIPANT_ATTEMPTS_WITHOUT_DEADLINE` | 100 | T | constant |
| Participant backoff `{ baseDelayMs: 100, maxDelayMs: 2_000 }` | | T (4 sites) | inline |
| `MAX_CLOCK_SKEW_MS` | 5 s | P | static field |
| Stale-lock scan batch, recovery `LIMIT`, sweep `CHUNK_SIZE` | 10, 100, 100 | P, T | inline |

### A.3 Query page budgets (`shared/query/page-budget.ts`)

| Constant | Value | Users |
| --- | --- | --- |
| `DEFAULT_EVALUATED_ITEMS_PER_PAGE` | 1,000 | C |
| `MAX_EVALUATED_ITEMS_PER_PAGE` | 100,000 | C |
| `MAX_EVALUATED_BYTES_PER_PAGE` | 100 MiB | C |
| `DEFAULT_RESPONSE_BYTES_PER_PAGE` | 3 MiB | C |
| `MAX_RESPONSE_BYTES_PER_PAGE` | 16 MiB | C, P |
| `MAX_PARTITION_VISITS_PER_PAGE` | 100 | C |

### A.4 Expression limits (`shared/expression/limits.ts`)

`EXPRESSION_LIMITS` has 13 keys: `operatorsAndFunctions`, `astDepth`, `updateActions`, `projectionEntries`,
`jsonPathDereferences`, `inChoices`, `sqliteFunctionArguments`, `sqlitePatternBytes`, `projectionAliasBytes`,
`jsonPathBytes`, `canonicalPayloadBytes`, `compiledSqlBytes` and `completeStatementBindings`. C uses them in the
compiler, and P checks a compiled plan again with them in `expression/runtime.ts` and `partition-store.ts`.

### A.5 Split and size values

| Value | Default | Users |
| --- | --- | --- |
| `hashSplitConditions.maxSizeMb`, `rangeSplitConditions.maxSizeMb` | 100 MB, 500 MB | P, T |
| `RANGE_PROMOTION_FRACTION` | 0.25 | P |
| Admission margin | 1.1 | P (1 site), T (3 sites) |
| `MAX_TC_DATABASE_BYTES` | 5 GiB | T |
| `TX_COORDINATORS_PER_ROOT_TREE` | 2 | C |

### A.6 Runtime and repartition flow

| Constant | Value | Today |
| --- | --- | --- |
| `DEFAULT_FALLBACK_ALARM_MS` | 5 s | `scheduler` option, passed by no host |
| `DEFAULT_FAST_PATH_DELAY_MS` | 50 ms | `scheduler` option, passed by no host |
| `DEFAULT_IMPORT_PAGES_PER_PASS` | 16 | `hooks.runtimeConfig()` |
| Hash arena budget | 1 MiB | `caches` option, passed by no host |
| `RANGE_HIERARCHY_MAX_ROWS` | 10,000 | `caches` option, passed by no host |
| Promotion Bloom sizing | 300,000 keys at 1% | `caches` option, passed by no host |
| `RANGE_HIERARCHY_REFRESH_MS` | 60 s | constant |
| `MAX_FORWARD_RETRIES` | 8 | constant |
| `STATUS_PAGE_ENTRIES`, `STATUS_PAGE_BYTES` | 1,000, 20 MiB | constants, `STATUS_PAGE_BYTES` twice |
| `FOKOS_PAGE_BYTES`, `FOKOS_PAGE_ROWS`, `FOKOS_SCAN_ROWS` | 20 MiB, 1,000, 10,000 | constants, repeated in `fokos-migration-host.ts` |
| Repartition retry intervals | 5 s to 5 min | constants |
| `REPARTITION_RPC_CONCURRENCY` | 6 | constant |

### A.7 `PartitionDO` host

| Value | Default | Today |
| --- | --- | --- |
| TTL sweep | section 4.2.2 | `fokosTtlConfig()` |
| Promoted-key cleanup batch | 1,000 | inline |
| `FOKOS_SHOULD_FETCH_COLO_INFO` | | `env`, read by `fokosGetColoInfo()` |

### A.8 Client (`client/db.ts`)

| Value | Default | Today |
| --- | --- | --- |
| `TX_COORDINATOR_MIGRATING_RETRY_MS` | 15 s | constant |
| Retry backoff `{ baseDelayMs: 100, maxDelayMs: 2_000 }` | | inline, 3 sites |
| Read-phase attempts of `transactGetItems` | 5 | inline |
