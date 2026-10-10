# RFC — Fix the API boundaries of FokosDB and FokosSharding before the first release

**State:** Draft
**Date:** 2026-10-10
**Author:** Lambros Petrou
**Status:** Not built. The open questions are decided, and the decisions are in the body. The expression compile
(4.2.1) and the stable sharding API (4.2.14, 4.2.15, 4.2.16, 4.2.18) are later work.

## Table of Contents

1. [Overview and Context](#1-overview-and-context)
2. [Goals and Requirements](#2-goals-and-requirements)
3. [Milestones](#3-milestones)
4. [Proposed Solution](#4-proposed-solution)
5. [Alternative Options](#5-alternative-options)
6. [Frequently Asked Questions](#6-frequently-asked-questions)
7. [References](#7-references)

## 1. Overview and Context

### 1.1 The problem

The first release makes four boundaries permanent. After the release, a change to one of them breaks a caller, a
deployed Worker, or stored data. Today each boundary shows more than it must.

| Boundary | What it shows today that it must not |
| --- | --- |
| `fokosdb/client` | DO names, tree depths, partition sizes, internal error codes, SQL compiler functions |
| `fokosdb/server` | RPC methods for tests, the sharding runtime as a public field, the wire types |
| Worker to DO wire protocol | SQL that names storage columns, caller binding names, no protocol version |
| `fokosdb/sharding/*` | About 100 names: the store, the scheduler, the Bloom filter, the hash functions |

A change is cheap now, because the package has no released version and breaking changes are permitted. The same
change after the release needs a compatibility window, or a major version.

### 1.2 The current boundaries

**`fokosdb/client`.** `FokosDB` has `putItem`, `getItem`, `deleteItem`, `queryItems`, `transactWriteItems`,
`transactGetItems`, `options()` and `destroy()`. `client/index.ts` lists every exported name.

- `FokosDBOptions` holds the table identity (`name`, `ns`, `nsTx`, `rootTreesN`, `hashSplitN`, `coordinatorRootsN`,
  `jurisdiction`) and the table policy (`locationHint`, `hashSplitConditions`, `rangeSplitN`, `rangeSplitConditions`,
  `rangeAncestorsConfig`, `limits`). The policy goes with every request, with a `policyVersion`.
- A single-item result has `meta: OperationMetrics & PartitionInfo`. A query result has `partitionMetas[]` and
  `cursor`. The two transaction results have no `meta`.
- An error is a `FokosError` or one of its subclasses, with `code`, `attributes`, and `error_id`.
- An expression is a public AST: `ConditionExpression`, `UpdateExpression`, `ProjectionExpression`.
- A cursor is a base64url string. `decodeCursor` in `shared/query/cursor.ts` accepts only `CURSOR_VERSION`.

**`fokosdb/server`.** The entry exports `PartitionDO`, `TransactionCoordinatorDO`, and the types `PartitionOps` and
`PartitionRpc`. A subclass can override `fokosConfig()` and `fokosRuntimeConfig()` (both `protected`), and `fokosNow()`
and `fokosGetColoInfo()` (both public).

**The wire protocol.** Each partition RPC is `method(ctx, req)` and answers `FokosEnvelope<T> = { value, routing }`.

- The route context (`FokosRouteContext` in `sharding/route-context.ts`) holds `schema: 2`, `partitionId`, `doName`,
  `topology`, `rangeConfig`, `policy`, and `policyVersion`.
- A request body has no version. An expression crosses as a compiled plan with `version: 1` and an SQL string.
- A query request carries the budgets that the client computed: `remainingEvaluatedItems`, `remainingEvaluatedBytes`,
  `remainingResponseBytes`, `remainingPartitionVisits`.
- The coordinator stores `partition_context_json` for each participant and `conditions_json` for each item.

**Formats that are stored or addressed.** These are part of the contract too: the DO name grammar (`fokos.p.`,
`fokos.tc.`, `~h.`, `~r.`), the partition ID encodings, `CoordinatorRef`, the cursor, the `__fokos/*` KV keys, the
order of `DATA_KINDS`, and the byte order of `KeyCodec`.

**`fokosdb/sharding/client`.** `FokosShardingClient` has `resolve`, `resolveRange`, `resolveAll`, `point`, `range`,
`send`, `walk` and `destroy`. `sharding/exports-client.ts` also exports `FokosRouter`, `PartitionIdHelper`, the hash
functions, the sk-interval helpers, `validateTopology`, and `structurallyEqual`.

**`fokosdb/sharding/server`.** `FokosShardingRuntime` is the host API. `sharding/index-server.ts` also exports
`FokosShardingStore`, `FOKOS_KV_KEYS`, `RepartitionSource`, `RepartitionTarget`, `FokosScheduler`, `BloomFilter`,
`HashTopology`, `PartialRangeTopology`, `RouteCollector`, `attachRouting`, `planRangeFrontier`, `selectRangeAncestors`,
and `collectBatch`. A host must write the 10 delegating control RPCs of `FokosShardingRpc` by hand.

### 1.3 What must stay as it is

- The entries list every name. No entry uses `export *` from a shared module.
- The client encodes keys one time, at its edge.
- `shared/transaction-api-types.ts` holds the public transaction types, and `shared/transaction-wire-types.ts` holds
  the wire types.
- An error crosses the wire as plain data (`FokosError.is`).
- The runtime treats migration pages and migration cursors as opaque.
- `FokosStoredRepartitionPlan` has a `schema` field. Partition IDs and `CoordinatorRef` have version tags.
- The client removes the routing from the envelope before it returns a result.

### 1.4 Evidence

Each row is a fact of the current code.

| Fact | Where |
| --- | --- |
| The compiled SQL names `i.data`, `i.data_kind`, `i.v`, `i.ttl_epoch_utc_seconds` | `shared/expression/compiler.ts` |
| Each plan version is `1`, and the DO accepts only that value | `shared/expression/plan.ts`, `validateConditionPlan` |
| The client entry exports `compileConditionExpression` and `compileUpdateExpression` | `client/index.ts` |
| The DO reads `env[ctx.policy.ns]` and `env[ctx.policy.nsTx]` from its own `env` | `shared/do-stubs.ts` |
| `meta` holds `servedByActorName`, `servedByActorId`, `hashDepth`, `rangeDepth`, `databaseSize` | `shared/types.ts` |
| A range DO name is `<shardGroup>~r.<hk>.<start>.<end>`, so `servedByActorName` holds a hash key | `AGENTS.md` |
| `fokosNow()`, `fokosGetColoInfo()` and `readonly fokos` are public on `PartitionDO` | `server/do-partition.ts` |
| A different policy with an equal `policyVersion` replaces the old one | `#ensureIdentity` in `sharding/runtime.ts` |
| `ExecutionFailureCode` is `Exclude<FokosErrorCode, PremiseRejectionCode>` | `shared/transaction-api-types.ts` |
| `EXPRESSION_LIMITS` has keys with `sqlite` and `compiledSql` in the name | `shared/expression/limits.ts` |

## 2. Goals and Requirements

### 2.1 In scope

- A request from a Worker on a different version must get a correct answer or an error, never a silent wrong result.
- A receiver must reject a request that it cannot serve correctly. It must not ignore a field that changes semantics.
- A public result must not show a DO name, a tree depth, a partition size, or a hash key of another item.
- A new internal error code must not be a breaking change for a caller.
- A subclass of a DO class must reach only the documented override methods.
- Each exported name of `fokosdb/client` and `fokosdb/server` must be a name that the project accepts to keep stable.
- A DO must find its peer namespaces when the caller script uses different binding names.

### 2.2 Out of scope

- The move of the expression compile into the DO. A separate RFC specifies it. Section 4.2.1 records the decision.
- The stable API of the sharding runtime. The runtime needs more work, and the first focus is FokosDB. Both sharding
  entries ship as `@experimental`. Sections 4.2.14, 4.2.15, 4.2.16 and 4.2.18 record the work for a later RFC.
- A change of the transaction model, the split algorithm, or the storage schema. This RFC changes boundaries only.
- A compatibility layer for the current wire format. No released version uses it.
- A deploy or migration plan for existing tables. Breaking changes are permitted before the release.

### 2.3 Requirements

- Correctness comes first. A change here must add as little code as the boundary needs.
- The client bundle must stay under its size budget. `pnpm build` checks it.
- The client must not import a Durable Object class as a value.
- A change must not add a storage row to the common path of a request.
- A change must not add a production hook for a test.

## 3. Milestones

Each milestone delivers a stable boundary on its own. The order puts the cheapest changes with the highest effect first.

| # | Milestone | Sections | Side that changes |
| --- | --- | --- | --- |
| 1 | Public results and errors have no physical detail | 4.2.6, 4.2.7, 4.2.8 | Client |
| 2 | The wire protocol has a version, and the table has a format version | 4.2.2, 4.2.5 | Client, DO, runtime |
| 3 | The DO classes show only the supported surface. Sharding is experimental | 4.2.11 to 4.2.13 | Server |
| 4 | Small contract fixes: the policy conflict and the cursor versions | 4.2.10, 4.2.17 | Client, runtime |
| 5 | The DO gets its binding names from server configuration | 4.2.3 | Client, DO |
| 6 | The other client API changes | 4.2.9 | Client |

Work that this RFC records and does not build:

- The expression compile in the DO (4.2.1): a separate RFC.
- The DO as the authority for limits (4.2.4): after the release.
- The stable API of the sharding runtime (4.2.14, 4.2.15, 4.2.16, 4.2.18, and the export trim of 4.2.13): a later RFC.

## 4. Proposed Solution

### 4.1 High-level overview

The change draws one line around each boundary and moves everything else behind it.

```
                 public, stable                     private, versioned                 private, free to change
  caller  ───►  fokosdb/client   ───────────────►  Worker to DO wire protocol  ───►  SQLite schema, DO names,
                results, errors,                   proto version, table format,       topology, caches, jobs
                expression AST                     expression plans
```

- **Public results.** A result holds the item, the row counts, and an optional `debug` object. The `debug` object is
  not stable, and a caller gets it only on request. It holds an opaque `servedBy` token, not a DO name.
- **Public errors.** The public code set is small and open. Each error has `retryable: boolean`. One table at the
  client edge maps every internal code to a public code and keeps the internal code in the attributes.
- **Wire protocol.** Each request carries a protocol version. A receiver that does not support the version answers
  `protocol_unsupported`, which the caller can retry. The table identity carries a format version for the hash
  function, the root selection, the DO name grammar, and the partition ID schema.
- **Expressions.** The compile moves into `PartitionDO`, so the storage column names stop being a contract. A separate
  RFC specifies that change (4.2.1).
- **Server classes.** A DO class has only the operation RPCs, the control RPCs, and the documented override methods.
  A subclass is a supported extension point, and it can reach only those override methods.
- **Sharding entries.** Both entries ship as `@experimental`. Their stable API is later work.

### 4.2 Technical details

#### 4.2.1 The expression boundary

**Decision.** The expression compile moves from the Worker into `PartitionDO`. A separate RFC specifies the change. It
is not written yet, and this RFC does not build it.

**Today.** The Worker compiles an expression into SQL that names the columns of the `items` table. The DO checks the
plan version, the size, and the number of bindings (`validateConditionPlan`), then runs the SQL. Three results follow:

- A change to the storage schema breaks every deployed Worker. Examples are a renamed column, a new JSONB layout, and
  a generated column.
- A plan version change needs the Worker and the DO to deploy at the same instant, because the check is `!==`.
- The coordinator stores plans in `tc_items.conditions_json`, and a lock row holds them too. A stored plan from older
  code can name a column that the new schema does not have.

**What the expression RFC must cover.**

- The validated AST crosses the wire. The client keeps validation, so a bad expression fails before the request
  leaves the Worker.
- The coordinator rows and the lock rows store the AST, not SQL.
- The DO must not run SQL that a caller wrote. This is the reason for the move: the DO must not trust its caller.
- The compile cost moves onto the DO, which serves one partition on one thread. A cache for each isolate, keyed by
  the canonical `identity` hash that the compiler already computes, removes the cost for a repeated expression only.
  `TODO: measure` the compile time of a typical condition.
- A comment in `client/db.ts` says that only the compiled plan crosses the RPC boundary. The move reverses it.
- `compileConditionExpression` and `compileUpdateExpression` leave `fokosdb/client`, or become
  `validate*Expression(): void`. Their return type `CompiledConditionPlan` has `sql: string`, so it is public today.

**What this RFC does.** The protocol version of 4.2.2 must ship first, so the expression RFC can change the request
body with a `proto` step.

#### 4.2.2 Protocol version on every RPC

**Today.** `schema: 2` in the route context describes only the shape of the context. A receiver ignores a field that
it does not know. A new optional field can thus change semantics with no error.

Two versions can run at the same time in three cases: a gradual deploy, a Worker and a DO in different scripts, and
two DOs of one class that started before and after a deploy.

**Change.**

1. Add `proto: <integer>` to the route context.
2. Each receiver has a constant range `[PROTO_MIN, PROTO_MAX]`. It answers `protocol_unsupported` for a value outside
   the range. The error is in the unavailable class, so the caller retries it during a deploy.
3. `FokosEnvelope` returns the `proto` of the receiver.
4. The rule covers Worker to DO, DO to DO forwarding, and coordinator to participant calls.

**Rule for a new field.** A field that changes semantics must come with a higher `proto`. A field that is only advice
can be ignored, and needs no new `proto`.

Section 5.1 has the reason this RFC does not use a list of required capabilities.

#### 4.2.3 Binding names on the wire

**Today.** `policy.ns` and `policy.nsTx` are binding names in the `env` of the caller. The DO reads the same names
from its own `env` (`partitionNamespace`, `txCoordinatorNamespace`, `txCoordinatorStubForParticipant`). The names are
equal when the Worker and the DO classes are in one script, which is the layout that `packages/fokosdb/README.md`
describes. The lookup fails when the caller script and the DO script use different binding names.

**Change.** The DO host gets its own namespace and its peer namespace from server configuration:
`fokosConfig().partitionBinding` and `fokosConfig().coordinatorBinding`. The defaults are the names in use today. The
client keeps `ns` and `nsTx` only to get its own stubs. `ns` then stops being part of the table identity on the wire.

**Decision.** A caller script can be different from the DO script, and can run a different version. A gradual
rollout makes a version difference common. This change must thus ship before the release.

#### 4.2.4 Authority for limits and budgets

**Today.** The DO trusts the client. The client computes the query budgets and sends them. `limits` is part of the
policy, which the client sends. `MAX_ITEMS_PER_TX` is a constant in `shared/transaction-limits.ts` that both sides
compile in.

**Decision.** The trust is acceptable for the first release, but the DO must become the authority. This is the same
direction as the move of the expression compile into the DO (4.2.1).

**Change.** The client sends its intent: `limit` and an optional maximum size. The DO clamps each value to its own
configured maximum. A server-side limit can then go down with a DO deploy only. The protocol version of 4.2.2 lets
this change ship after the release with no silent failure.

#### 4.2.5 Table format version

**Today.** The hash function, the root selection, the DO name grammar, and the partition ID schema have no common
version. A change to one of them sends a request to an empty DO, with no error.

**Change.** Add `table.format?: 1` to the table identity, with default `1`, and carry it in the route context. Format
`1` is the current set of four rules. A later format can change one of them on purpose. A DO rejects a request whose
format is different from its stored format, with the existing context mismatch error.

**Optional second part.** Store a fingerprint of the identity in root 0, and check it on the first request of a
client. A wrong `name`, `ns`, or `jurisdiction` then fails with `table_identity_mismatch`. This part adds one storage
read for each new client, and it is not part of milestone 2.

#### 4.2.6 Physical routing in `meta`

**Today.** `partitionInfoOf` in `client/partition-info.ts` copies `ref.doName` into `meta.servedByActorName`. For a
range partition, the DO name contains the hash key. A caller that logs `meta`, or returns it to its own user, shows
that key. `databaseSize` shows the size of a partition, and `hashDepth` and `rangeDepth` show the topology.

**Change.**

- The stable part is `meta = { rowsRead, rowsWritten, timings?: Record<string, number> }`. `timings` is best effort.
- Diagnostics move to `meta.debug?`, typed as unstable: `{ servedBy: string, forwardCount: number }`. `servedBy` is an
  opaque token. A caller gets `debug` only with a `debug: true` option, for one call or for the client.
- Remove `servedByActorName`, `servedByActorId`, `hashDepth`, `rangeDepth`, and `databaseSize` from the public type.
- Apply the same rule to `partitionMetas[]` of a query result.

#### 4.2.7 One result shape for every operation

`transactWriteItems` and `transactGetItems` return no `meta`. Add `meta` to both, with the type of 4.2.6. A wrapper
that a caller writes for one operation then works for all of them.

#### 4.2.8 Public error codes

**Today.** `FokosErrorCode` holds the sharding codes and the runtime codes: `partition_context_mismatch`,
`partition_fanout_failed`, `repartition_*`, `sharding_*`, `hash_partition_not_initialized`, `commit_keyset_mismatch`,
and others. `ExecutionFailureCode` puts almost all of them into the transaction results. A caller that switches over
every code breaks when a new internal code appears.

**Change.**

- **Public codes.** Validation, condition failed, transaction cancelled, conflict, pending, unavailable, throttled,
  `partition_migrating`, and internal.
- **One mapping table.** The client edge maps each other code to `internal_error` or `unavailable`, and puts the
  source code in `attributes.internalCode`. The table replaces the one special case that maps
  `repartition_not_cut_over` to `partition_migrating`.
- **Open set.** The public type is `KnownCode | (string & {})`. The documentation says that a new code is not a
  breaking change.
- **`retryable: boolean`.** Each error carries it. A caller reads it and does not classify by code.
- **`attributes`.** Each stable code has a typed public schema. Every other key goes under `attributes.debug`.
- **`ExecutionFailureCode`.** Narrow it to the public codes.

#### 4.2.9 Other client API changes

- **`TransactWriteItemsResult.transactionId`.** The id of one attempt ties the API to the current two-phase commit
  model, and `pending_conflict.conflictingTransactionId` shows the id of another transaction. Keep
  `idempotencyToken`. Remove `transactionId`, or move it under `debug`.
- **`destroy()`.** Move it to a `FokosDBAdmin` class or a `fokosdb/admin` entry. The minimum is
  `destroy({ confirm: tableName })`.
- **`options()`.** It returns resolved defaults, for example `coordinatorRootsN = 2 x rootTreesN`. A returned default
  becomes part of the contract. Return only what the caller gave, or document each default as stable.
- **Split tuning options.** `rangeSplitN`, `rangeSplitConditions`, `hashSplitConditions`, and `rangeAncestorsConfig`
  describe the current split algorithm. Move them under `table.advanced`, documented as unstable, or make them server
  configuration in `fokosConfig()`. Decide what an unknown key does: a throw makes the removal of a key a breaking
  change. `docs/ideas/2026-09-25-configuration-surface.md` has the inventory of these values.
- **`ExpressionReference { ref: "v" }`.** Rename the value to `"version"`.
- **`EXPRESSION_LIMITS`.** Rename the keys that say `sqlite` or `compiledSql`, or remove the export.
- **`ReadItem.version`.** Document the contract: the value goes up for each key and never repeats, also after a
  delete and a recreate (`max_deleted_v`).

#### 4.2.10 Cursor versions

`decodeCursor` must accept `CURSOR_VERSION` and the version before it. A gradual deploy then does not break a
pagination that is in progress. `CURSOR_VERSION` and the decoder stay out of the public entry.

#### 4.2.11 RPC surface of the DO classes

A public method of a Durable Object class is callable over RPC, and a subclass can depend on it.

| Member of `PartitionDO` | Change |
| --- | --- |
| `readonly fokos` | Make it `#fokos` |
| `fokosNow()`, `fokosGetColoInfo()` | Make them `protected` |
| `status`, `promotedKeyStatus`, `debugForcePromoteKey` | Move them to a subclass in a test-only entry |
| `debugForceResolveTransaction` | Keep it in production and give it an operator name. See below |
| `PartitionOps`, `PartitionRpc` | Remove the exports from `fokosdb/server` |

**`debugForceResolveTransaction` is an operator tool.** The stale-transaction job quarantines an owned lock that is
older than `IDEMPOTENCY_WINDOW_MS` when the coordinator answers `not_found`. The lock then waits for this RPC. It is
the only way to release a quarantined lock, so it must stay in the production class. Rename it so the name says what
it is, for example `adminForceResolveTransaction`.

**Constraint from the tests.** Test files read `stub.fokos` and replace `fokosNow` on one instance. The change must
keep both possible from a test, with no production hook. A test subclass that exposes them is one way.

**`walk` does not need `status`.** `FokosShardingClient.walk` reads the tree through the control RPC `fokosStatus`.
The move of `status` thus changes only the tests and the partition harness.

Section 5.2 has the reason this RFC does not use one multiplexed RPC method.

#### 4.2.12 The contract for a subclass

**Decision.** A subclass of `PartitionDO` or `TransactionCoordinatorDO` is a supported extension point.

- Supported overrides: `fokosConfig()`, `fokosRuntimeConfig()`, and `fokosNow()`. Each one is `protected`.
- Every other member of the class must be `#private`, or an RPC method of 4.2.11. A subclass can then depend only on
  the supported overrides, and TypeScript `private` is not enough, because it does not hide a member at run time.
- The documentation must list the supported overrides and say that a subclass must not override an RPC method.
- Two pairs of settings must agree between the client and the server: `staleTransactionMs` with
  `coordinatorFanoutBudgetMs`, and `partitionMigratingRetryDeadlineMs` with `fallbackAlarmMs`. The server must own
  each pair, or the server must report its value to the client.

#### 4.2.13 The sharding entries are experimental

**Decision.** The sharding runtime needs more work before it is stable. The first release makes only FokosDB stable.

**Change.**

- Mark `fokosdb/sharding/client` and `fokosdb/sharding/server` as `@experimental` in the entry files and in
  `packages/fokosdb/README.md`. A minor version can change an experimental name.
- `fokosdb/client` and `fokosdb/server` must not export a type from an experimental entry. A public FokosDB type that
  uses a sharding type must get its own definition, or the sharding type must become stable.

**Deferred: the export trim.** A later RFC cuts each sharding entry to this list.

`fokosdb/sharding/client` keeps:

- `FokosShardingClient` and `KeyCodec`.
- The call types, the result types, and the errors.
- The topology types and the configuration types that build a client.

`fokosdb/sharding/server` keeps:

- `FokosShardingRuntime` and its configuration.
- The operation types, the hook types, and the `MigrationHost` type.
- `FokosShardingRpc` and its request types, as opaque types.

Everything else moves to `fokosdb/sharding/internal`, documented as unstable, or loses its export. This covers the
store, the scheduler, the Bloom filter, `HashTopology`, `PartitionIdHelper`, the hash functions, the sk-interval
helpers, `FOKOS_KV_KEYS`, `RepartitionSource`, `RepartitionTarget`, `RouteCollector`, and the planner functions.

FokosDB imports these modules by relative path, so the trim does not touch FokosDB code.

#### 4.2.14 A base class for a sharding host

**Deferred.** The sharding entries ship as `@experimental`. Section 2.2 has the reason.

**Today.** A host writes the 10 control RPCs by hand (`examples/http-api/src/demo2/sharded-do.ts`). A new control RPC
then breaks every host.

**Change.** Ship `ShardedDurableObject`, or a mixin, in `fokosdb/sharding/server`. The other option is one control
method `fokosControl(req)` that carries every control request.

#### 4.2.15 Opaque routing types in the sharding API

**Deferred.** The sharding entries ship as `@experimental`. Section 2.2 has the reason.

- `FokosRouteContext`, `FokosPartitionRef`, and `FokosWalkNode.ctx` show `schema`, `doName`, and `partitionId`.
  `resolve*` and `walk` must return a branded opaque handle. Admin tools get an explicit `describe(handle)` method.
- `FokosRouteNode._rangeAncestors` and `servedByTruncated` move to an internal type.
- The hints in `FokosEnvelope.routing` get a version, or become opaque. Only `servedBy` (opaque) and `forwardCount`
  stay public.
- The routing that the runtime attaches to an error is an own property of the error object, next to the attributes of
  the host. Give it a namespace.
- `FokosRangeInput.cursor` in `sharding/runtime-types.ts` uses `ScanCursor` from `shared/partition`. This is a
  FokosDB type in the generic runtime. Define the type in `sharding/`, or make it generic or opaque.

#### 4.2.16 Migration streams and host data

**Deferred.** The sharding entries ship as `@experimental`. Section 2.2 has the reason.

- Add a stream identity and a version to `MigrationHost`: `buildPage(stream, ...)`. A host can then migrate more than
  one stream (items, locks, indexes) and change a page format.
- Brand the cursor type and the page type.
- Document that the runtime stores `FokosRepartitionPlan.data`, so the host must give it a version.

#### 4.2.17 Policy conflict

**Today.** In `#ensureIdentity`, a request with the stored `policyVersion` and a different policy replaces the stored
policy. The stored policy then depends on which request arrives last.

**Change.** Reject the request with `policy_conflict`. The comparison is the `structurallyEqual` check that the
function already makes, so the change adds no read and no row.

#### 4.2.18 Runtime configuration keys

**Deferred.** The sharding entries ship as `@experimental`. Section 2.2 has the reason.

`hashArenaBytes`, `promotionBloom*`, and `rangeHierarchy*` in `FokosRuntimeConfig` are tuning values. Mark them as
advanced and unstable. Decide whether the `RUNTIME_CONFIG_KEYS` validation rejects a key that a later version removed.
A rejection makes the removal of a key a breaking change for every configuration that still sets it.

#### 4.2.19 Testing

- Each milestone must keep `pnpm check` and `pnpm test` green.
- Milestone 1: a type test must fail when a public result type gains a field outside `meta` and `meta.debug`.
- Milestone 2: a test sends a `proto` outside the range to each DO class and expects `protocol_unsupported`. A second
  test sends a different `table.format` and expects the context mismatch error.
- Milestone 3: a test calls each removed member of `PartitionDO` over RPC and expects a failure. A type test fails
  when `fokosdb/client` or `fokosdb/server` exports a type from an experimental entry.
- Milestone 4: a test sends two different policies with one `policyVersion` and expects `policy_conflict`.


## 5. Alternative Options

### 5.1 A list of required capabilities on each request

The request carries `requires?: string[]`, and the receiver rejects a name that it does not know. This permits two
features to ship in any order. It was not chosen because one integer with an accepted range gives the same safety with
one comparison, and the package has one release line.

### 5.2 One multiplexed RPC method for each DO class

The class has one method `fokosCall(op, ctx, req)` in place of one method for each operation. An operation can then
appear or go with no change to the class surface, and the protocol check lives in one place. It was not chosen because
the runtime forwards a request by a call to the method with the operation name on the target stub. The change would
alter the forwarding path of the runtime, which is outside the scope of 2.2.

### 5.3 A test-only subclass for `debugForceResolveTransaction`

It was not chosen. Section 4.2.11 has the reason: the RPC is the release path for a quarantined lock in production.

### 5.4 Compile in the Worker, and let the DO accept two plan versions

The Worker compiles as it does today. The DO accepts plan version N and N-1, and the stored rows hold the AST. The
compile cost stays on the Worker, which scales out. It was not chosen because the DO then still runs SQL that the
caller wrote, and the storage schema stays a contract for one version. Section 4.2.1 has the decision.

## 6. Frequently Asked Questions

**Why do this before the release and not after?**
Before the release, each change is an edit. After the release, each change needs a compatibility window on the wire,
or a major version of the package.

**Does a single-script deploy need a protocol version?**
Yes. A gradual deploy runs two versions of one script at the same time, and a DO can stay on the old version while a
Worker runs the new one.

**Does the removal of `servedByActorName` remove a debugging tool?**
No. `meta.debug.servedBy` carries an opaque token for the same partition, and an admin tool can resolve it. The
default result stops showing a hash key.

**Can a user build on `fokosdb/sharding/*` in the first release?**
Yes, but the entries are `@experimental`. A minor version can change a name or a type of those entries.

**Does this RFC change stored data?**
No. The expression RFC of 4.2.1 changes what the coordinator rows and the lock rows store. The package has no
released version, so the SQL migrations change in place.

## 7. References

- `AGENTS.md`
- `packages/fokosdb/README.md`
- `docs/ideas/2026-09-25-configuration-surface.md`
- `docs/ideas/error-handling/2026-09-10-existing-error-flows.md`
- `docs/ideas/fokos-sharding/2026-09-09-fokos-partition-runtime.md`
- [Workers RPC](https://developers.cloudflare.com/workers/runtime-apis/rpc/)
- [Gradual deployments][gradual]

[gradual]: https://developers.cloudflare.com/workers/configuration/versions-and-deployments/gradual-deployments/
