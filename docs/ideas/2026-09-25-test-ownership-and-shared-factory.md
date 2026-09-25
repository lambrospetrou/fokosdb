# FokosDB Test Ownership and a Shared Test Factory

Status: **Draft**
Date: 2026-09-25

## Table of Contents

- [1. Overview and Context](#1-overview-and-context)
- [2. Goals and Requirements](#2-goals-and-requirements)
- [3. Timeline and Milestones](#3-timeline-and-milestones)
- [4. Proposed Solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Current test coverage](#42-current-test-coverage)
  - [4.3 Shared test factory](#43-shared-test-factory)
  - [4.4 Test ownership](#44-test-ownership)
  - [4.5 Open Questions](#45-open-questions)
- [5. Alternative Options](#5-alternative-options)
- [6. Frequently Asked Questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and Context

FokosDB has tests for sharding, server operations, and the client API. Many suites cross these layers.
The test file location does not always identify the code boundary under test.

This inventory counts `.test.ts` files in the current tree. It does not count individual test cases.
It excludes `packages/fokosdb/src/shared` tests and `examples/http-api` tests.

| Location | Test files | Main coverage |
| --- | ---: | --- |
| `packages/fokosdb/src/sharding` | 13 | Sharding algorithms, stored route state, IDs, and migration helpers |
| `packages/fokosdb/src/server` | 1 | Transaction coordinator state and storage |
| `packages/fokosdb/src/client` | 2 | FokosDB API and `FokosStd` |
| `packages/fokosdb/test` | 40 | Partition behavior, repartition, transactions, and public API properties |

The 40 files under `packages/fokosdb/test` include 15 partition tests, 8 transaction tests,
9 property-based tests, and 6 root-level tests. The `repartition` and `sharding` folders have one
file each. One partition test checks the test harness, not a FokosDB feature.

The project rules place partition behavior under `test/partition-do`, transactions under
`test/transactions`, and repartition flow tests under `test/repartition`.

## 2. Goals and Requirements

### In scope

- Record which suites cover each major feature.
- Separate sharding, FokosDB server, and public API test ownership.
- Describe what a shared FokosDB test factory can provide.
- Keep tests that check distinct boundaries, even when their feature areas overlap.

### Out of scope

- Implementing a test factory.
- Moving, removing, or rewriting test suites.
- Changing the public FokosDB API or the sharding API.

This draft makes no code changes. It proposes a test fixture contract for review.

## 3. Timeline and Milestones

This idea sets no implementation dates or milestones. Define them after the factory scope is agreed.

## 4. Proposed Solution

### 4.1 High-level overview

Keep tests at three boundaries:

1. Sharding tests cover algorithms, repartition flows, and a generic runtime host.
2. Server tests cover the FokosDB host, SQLite behavior, and transaction coordination.
3. Client tests cover public results, validation, routing metadata, and retries.

Sharding tests do not depend on the public FokosDB API. Some use FokosDB storage adapters where the
sharding contract needs them.

Use a shared test factory to build ordinary `FokosDB` fixtures from one place. Keep specialized
server and sharding harnesses separate. Keep a small set of end-to-end tests to verify that the
layers work together.

### 4.2 Current test coverage

#### Sharding algorithms and runtime

The 13 test files under `packages/fokosdb/src/sharding` cover these areas:

- Key encoding and ordering: `key-codec.test.ts`.
- Hash functions and route caches: `hash-primitives.test.ts` and `hash-topology.test.ts`.
- Partition IDs and contexts: `partition-id.test.ts`.
- Range math and planning: `sk-interval.test.ts`, `range-frontier.test.ts`, and
  `range-ancestors.test.ts`.
- Promoted-key lookup: `bloom-filter.test.ts` and `partial-range-topology.test.ts`.
- Stored route and repartition state: `sharding-store.test.ts`.
- Migration page budgets: `batch-scan.test.ts`.
- Route envelopes and router traversal: `envelope.test.ts` and `router.test.ts`.

Some of these tests use real Workers storage. `key-codec.test.ts` compares key ordering with SQLite.
`sharding-store.test.ts` uses real Durable Object storage.

`test/repartition/repartition-flow.test.ts` drives `RepartitionSource` and `RepartitionTarget` steps
directly. It covers arbitration, planning, initialization, cutover, migration pages, retries,
promotion, cleanup, and status pages. Its harness uses real SQLite and KV storage.

`test/sharding/counter-host.test.ts` runs `FokosShardingRuntime` through a small counter host. The host
imports the sharding entry and does not use FokosDB modules. The tests cover hash split, forwarding,
repeated splits, recovery after the source stops, and the hot-key split rule.

#### FokosDB server

`src/server` has one colocated test file: `do-transaction-coordinator.test.ts`. It checks coordinator
storage, state transitions, recovery, idempotency cleanup, migration pages, and destruction. It uses
internal state helpers to test transitions directly.

`PartitionDO` tests live under `test/partition-do`, not `src/server`. They cover item operations,
conditions, queries, splits, promotion, read-through, transaction participation, stale recovery,
errors, and destroy fences.

The 8 suites under `test/transactions` test two-phase commits through FokosDB. They cover fast paths,
atomicity, read consistency, operation results, error handling, retries, and coordinator splits.

#### FokosDB client and public API

`src/client/db.test.ts` calls FokosDB with real Durable Objects. Some isolated cases run against both
`PARTITION_DO` and `CUSTOM_PARTITION_DO`. The file imports server and sharding modules for test setup
and for selected checks.

The client test fixture sets hash and range split thresholds to 500 MB. Its query tests focus on
multi-query fan-out and page budgets, not range-tree walking.

`src/client/fokos-std.test.ts` covers `FokosStd` validation, typing, and negative-prefix queries.
Most property-based suites call FokosDB and compare its answers with models. They cover item sequences,
queries, transactions, updates, and operations during splits. `ownership.test.ts` checks routing and
migration ownership through the sharding runtime.

#### Feature overlap

The following groups have related checks at more than one boundary. This is overlap, not a count of
duplicate test files.

**Item operations, conditions, and TTL**

- Server: `test/partition-do/item-crud.test.ts` and `item-conditions.test.ts`.
- Public API: `src/client/db.test.ts`, `test/item-conditions-return-values.test.ts`,
  `test/item-errors.test.ts`, and `test/property-based/item-crud.test.ts`.
- Overlap: server and public API suites check related item results. The sharding layer does not own
  these item semantics.

**Queries and pagination**

- Sharding: range and scan helpers in `src/sharding/sk-interval.test.ts`, `range-frontier.test.ts`,
  `range-ancestors.test.ts`, `sharding-store.test.ts`, and `batch-scan.test.ts`.
- Server: `test/partition-do/query-items.test.ts`, `range-split.test.ts`, and `read-through.test.ts`.
- Public API: `src/client/db.test.ts` and the three query property suites under
  `test/property-based`.
- Overlap: several suites check ordering, cursors, and complete pages. The client fixture avoids
  range splits. The server and split property suites test range-tree behavior.

**Hash routing and splits**

- Sharding: hash helpers under `src/sharding`, `test/sharding/counter-host.test.ts`, and
  `test/repartition/repartition-flow.test.ts`.
- Server: `test/partition-do/hash-split.test.ts`, `tx-participant.test.ts`, and
  `migration-timestamps.test.ts`.
- Public API: `test/property-based/ownership.test.ts` and `transactions-split.test.ts`.
  `test/transactions/tx-coordinator-split.test.ts` covers hash splits of coordinators.
- Overlap: the generic host and `PartitionDO` tests both check split routing and retained data.
  The ownership property checks that routing and migration choose the same child.

**Range splits and key promotion**

- Sharding: range and promotion helpers under `src/sharding`, plus
  `test/repartition/repartition-flow.test.ts`.
- Server: `test/partition-do/range-split.test.ts`, `promotion.test.ts`, `query-items.test.ts`, and
  `read-through.test.ts`.
- Public API: `test/property-based/query-items-split.test.ts` and
  `query-items-active-split.test.ts` query a promoted range tree.
- Overlap: repartition and server tests check the move. Public query tests check the result after the
  move.

**Migration and import**

- Sharding: `batch-scan.test.ts`, `sharding-store.test.ts`, `test/repartition/repartition-flow.test.ts`,
  and `test/sharding/counter-host.test.ts`.
- Server: `test/partition-do/hash-split.test.ts`, `range-split.test.ts`, `read-through.test.ts`,
  `import-page-guards.test.ts`, `migration-timestamps.test.ts`, and transaction participant suites.
- Public API: `query-items-active-split.test.ts`, `transactions-split.test.ts`, and
  `test/transactions/tx-commit-fanout.test.ts`.
- Overlap: several suites check data retention and reads during movement. The sharding tests inspect
  protocol steps. The FokosDB tests check rows, locks, and public results.

**Transactions and recovery**

- Server: `src/server/do-transaction-coordinator.test.ts`,
  `test/partition-do/tx-participant.test.ts`, and `tx-stale-recovery.test.ts`.
- Public API: all 8 suites under `test/transactions`, plus the transaction property suites.
- Sharding: `test/repartition/repartition-flow.test.ts` checks pending transaction rows during import.
  The generic counter host does not test transaction operation shapes.
- Overlap: server and public suites both check transaction outcomes. The server tests inspect stored
  state. The public tests check atomicity, consistency, and replay behavior.

**Errors, routing metadata, and destroy**

- Sharding: `src/sharding/envelope.test.ts` and `router.test.ts`.
- Server: `test/partition-do/error-meta.test.ts`, `destroy-fence.test.ts`, and
  `test/partition-errors.test.ts`.
- Public API: `src/client/db.test.ts`, `test/item-errors.test.ts`, `test/partition-errors.test.ts`,
  and `test/destroy.test.ts`.
- Overlap: suites check related routing, error, and destroy outcomes at the layer where each result
  is built or used.

### 4.3 Shared test factory

The test suites build similar FokosDB fixtures in several places:

- `src/client/db.test.ts` defines `makeDBFor`.
- `test/property-based/harness.ts` defines `makeTestDB`.
- `test/transactions/tx-helpers.ts` defines `makeDB`.
- `test/destroy.test.ts` defines another `makeDB` with split settings.

These helpers set namespaces, table names, root counts, split thresholds, and coordinator options.
The options differ because each suite tests a different path. A shared factory can own the common
FokosDB construction and keep these differences as explicit options.

The proposed factory can provide:

- A unique table name by default, so fixtures do not share a shard group.
- A binding option for `PARTITION_DO` or `CUSTOM_PARTITION_DO`.
- Topology options for root counts, hash and range split sizes, and coordinator roots.
- Named fixture profiles for unsplit API tests, hash split tests, and range split tests.
- A separate controlled fixture for suites that need test-only DO controls.
- One `FokosDB` result for ordinary public API tests.

The factory sets topology. The test still drives the operation and the split. A profile does not hide
the behavior under test.

The ordinary client fixture returns `FokosDB`, not `FokosRouter`, `PartitionDO`, or
`FokosShardingRuntime`. A separate controlled fixture can return stubs or controls to server tests.
This keeps internal sharding types out of ordinary client tests.

The shared factory does not replace `partition-harness.ts`, `repartition-harness.ts`, or
`counter-table.ts`. Those helpers control migrations, alarms, storage, or a non-FokosDB host.

The first candidates for shared setup are `makeDBFor`, `makeTestDB`, and the ordinary path in
`test/transactions/tx-helpers.ts`. The destroy test can use the factory if its split settings fit the
same option model. This is a suggestion, not a migration plan.

### 4.4 Test ownership

| Test boundary | Owns | Keeps internal details |
| --- | --- | --- |
| Sharding | Routing, caches, repartition, migration, generic runtime behavior | Sharding state |
| FokosDB server | SQLite, item rules, admission, TTL, locks, migration pages | SQL, DO state, host hooks |
| FokosDB client | Public results, validation, cursors, metadata, retries | No sharding internals by default |
| End-to-end | A small set of split, migration, and transaction guarantees | Public results across layers |

The generic counter host tests one `point` operation. It does not test generic runtime behavior for
`range`, `group`, or `single_owner` operations. The repartition suite tests flow components directly,
not the full runtime dispatcher. FokosDB tests cover these paths through `PartitionDO`.

If the sharding runtime promises behavior for those operation shapes, add a small generic-host test
for each shape. Keep SQL and FokosDB item semantics in the FokosDB suites.

### 4.5 Open Questions

1. Does the shared public fixture support only `PARTITION_DO`, or also `CUSTOM_PARTITION_DO`?
2. Does the controlled fixture stay separate from the public fixture?
3. Does the factory return only `FokosDB`, or also safe fixture metadata such as the table name?
4. Which tests need a prepared split tree, and which only need split thresholds?
5. Which end-to-end cases remain after test ownership is clear?

## 5. Alternative Options

### Use only FokosDB tests

This keeps most tests above the sharding API. A sharding API change then affects fewer test files.
However, these tests do not test the sharding layer as an independent module. They also give less
detail about which repartition transition failed.

### Test every feature at every layer

This gives repeated checks at each boundary. It also increases test setup and maintenance.
Keep repeated checks only when each suite proves a different contract.

### Keep layered test ownership

This proposal keeps algorithm tests, server tests, public API tests, and a small number of end-to-end
tests. A sharding API change can update sharding-owned tests without changing public assertions.

## 6. Frequently Asked Questions

### Does the factory replace the split and migration harnesses?

No. Those harnesses control specific server or sharding behavior. The factory only builds FokosDB
fixtures.

### Does this proposal remove overlapping tests?

No. It records which boundary each test checks. A later review can identify tests that assert the
same contract without adding a new check.

## 7. References

- `AGENTS.md`
- `packages/fokosdb/src/sharding/batch-scan.test.ts`
- `packages/fokosdb/src/sharding/hash-topology.test.ts`
- `packages/fokosdb/src/sharding/key-codec.test.ts`
- `packages/fokosdb/src/sharding/range-frontier.test.ts`
- `packages/fokosdb/src/sharding/sharding-store.test.ts`
- `packages/fokosdb/src/server/do-transaction-coordinator.test.ts`
- `packages/fokosdb/src/client/db.test.ts`
- `packages/fokosdb/src/client/fokos-std.test.ts`
- `packages/fokosdb/test/partition-do/hash-split.test.ts`
- `packages/fokosdb/test/partition-do/item-crud.test.ts`
- `packages/fokosdb/test/partition-do/partition-harness.ts`
- `packages/fokosdb/test/partition-do/promotion.test.ts`
- `packages/fokosdb/test/partition-do/query-items.test.ts`
- `packages/fokosdb/test/repartition/repartition-flow.test.ts`
- `packages/fokosdb/test/repartition/repartition-harness.ts`
- `packages/fokosdb/test/sharding/counter-host.ts`
- `packages/fokosdb/test/sharding/counter-host.test.ts`
- `packages/fokosdb/test/property-based/harness.ts`
- `packages/fokosdb/test/property-based/ownership.test.ts`
- `packages/fokosdb/test/transactions/tx-helpers.ts`
- `packages/fokosdb/test/destroy.test.ts`
