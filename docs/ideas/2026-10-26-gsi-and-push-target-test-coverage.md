# RFC — Test coverage of GSIs and push targets: audit and plan

**State:** Draft
**Date:** 2026-10-26
**Author:** Lambros Petrou
**Status:** Nothing is built. Section 1 and the word "today" describe the code and the tests before this plan.

## Table of Contents

- [1. Overview and Context](#1-overview-and-context)
  - [1.1 The current system](#11-the-current-system)
  - [1.2 The problem](#12-the-problem)
  - [1.3 Current coverage](#13-current-coverage)
  - [1.4 Glossary](#14-glossary)
- [2. Goals and Requirements](#2-goals-and-requirements)
  - [2.1 In scope](#21-in-scope)
  - [2.2 Requirements](#22-requirements)
  - [2.3 Out of scope](#23-out-of-scope)
- [3. Milestones](#3-milestones)
- [4. Proposed Solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
    - [4.2.1 Milestone 1a: a missing push binding is an unknown answer](#421-milestone-1a-a-missing-push-binding-is-an-unknown-answer)
    - [4.2.2 Milestone 1b: a constant `bytes` GSI projection passes the validation](#422-milestone-1b-a-constant-bytes-gsi-projection-passes-the-validation)
    - [4.2.3 Milestone 2: the convergence property suite](#423-milestone-2-the-convergence-property-suite)
    - [4.2.4 Milestone 3: a late call after the tombstone GC](#424-milestone-3-a-late-call-after-the-tombstone-gc)
    - [4.2.5 Milestone 4: splits while work flows](#425-milestone-4-splits-while-work-flows)
    - [4.2.6 Milestone 5: the P model of the export and target hops](#426-milestone-5-the-p-model-of-the-export-and-target-hops)
    - [4.2.7 Milestone 6: pin tests and the cost test](#427-milestone-6-pin-tests-and-the-cost-test)
    - [4.2.8 Milestone 7: the transaction and TTL paths to a GSI](#428-milestone-7-the-transaction-and-ttl-paths-to-a-gsi)
    - [4.2.9 Milestone 8: the paths that no test reaches](#429-milestone-8-the-paths-that-no-test-reaches)
    - [4.2.10 Milestone 9: performance](#4210-milestone-9-performance)
    - [4.2.11 Fault injection without a production hook](#4211-fault-injection-without-a-production-hook)
  - [4.3 Open Questions](#43-open-questions)
- [5. Frequently Asked Questions](#5-frequently-asked-questions)
- [6. References](#6-references)

## 1. Overview and Context

### 1.1 The current system

`docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md` specifies the feature. Milestones 1 to 4 of that
spec are built. The parts are:

- **Capture and export.** A base `FokosDBPartitionDO` with `captureChanges = true` writes a `materialization_outbox`
  row in the same `transactionSync` as each item change. `MaterializationExporter`
  (`src/shared/partition/materialization-export.ts`) sends the latest images with `applyLatestImages`.
- **`FokosDBMaterializationDO`** (`src/server/materialization/do-materialization.ts`). It keeps `latest_items`, gives
  each accepted image a change position, and runs `TargetRunner` (`target-runner.ts`) for each target.
- **GSI targets.** The runner keeps the key set of each item in `target_index_keys`, and writes the GSI table with the
  internal operation `gsiWriteEntries` through `GsiWriter` (`gsi-writer.ts`). `db.gsi(indexName).queryItems` reads
  the GSI table.
- **Push targets.** The runner sends a `PushTargetBatch` to a `WorkerEntrypoint` or a `DurableObject` binding.
- **Epochs and fences.** Each producer sends its epoch and its ancestor fences with each call. Each receiver stores
  `producer_fences` and rejects a call below the stored epoch.

### 1.2 The problem

The GSIs and the push targets are a new part of FokosDB. Their reliability and correctness come first, and their
performance comes after. The tests do not show either one today:

- About 110 tests cover the feature. Most of them are unit tests of one component, with a controlled clock and
  controlled answers.
- Each end-to-end test uses 1 to 4 items, real Durable Objects, no injected fault, and no concurrency.
- No test checks, after random work, that each GSI and each push target agrees with the base table.
- The P model in `packages/fokosdb/formal/p/` has no model of the materialization. The epoch and fence protocol is the
  most subtle part of the feature, and only hand-written examples check it.
- The package has no line-coverage tool. This audit compares the tests with the rules of the spec and with the code
  paths.
- One benchmark, `test/gsi-cost-bench/gsi-cost.workerd-bench.ts`, measures row sizes and rows written. `pnpm test`
  does not run it, and no test stops a cost regression.

The audit also found two code paths that do not agree with the spec or that cost more than necessary. Section 4.2.1
and Section 4.2.2 describe them. Neither one is a confirmed defect yet.

### 1.3 Current coverage

| Layer | Files | What the tests check |
| --- | --- | --- |
| Outbox capture | `partition-store.test.ts` (outbox section), `materialization-outbox.test.ts` | The `is_local_only` rules. The delete revision `v + 1`. A recreate keeps `outbox_id`. The TTL sweep tombstone. A delete of an absent item writes no row. The copy and the source cleanup after a split and after a promotion. The export fences on the last page. |
| Exporter, unit | `src/shared/partition/materialization-export.test.ts` | Claim and acknowledgement. The same-epoch rule. The epoch raise after an unknown answer and after a fenced answer. A discarded answer of an old epoch. The epoch raise at a restart with a call open. The inherited fences. A partial `unprocessed` answer, and the backoff. A deferred unowned row. A dropped row that does not match its item. |
| Exporter, real DOs | `test/partition-do/materialization-export.test.ts` | A put, an update, and a delete reach `latest_items`. A failed condition and a cancel export nothing. A commit exports. |
| `applyLatestImages` | `apply-latest-images.test.ts` | The revision rules. Tombstones. Producer epochs. Ancestor fences. The over-size refusal, which still applies a delete. The answer of each item by its `opIndex`. |
| Change positions | `materialization-store.test.ts` | Ingest steps 1 to 4. The use of `read` and of `cursor`. The bound at the lowest cursor. |
| Target runner, unit | `target-runner.test.ts` | Batches and cursors. A slow target. A target that lags. Retry rows and their share of a batch. Violations. The circuit breaker and its probe. The epoch raise after an unknown answer and at a restart. The cleanup and the tombstone GC. Fences and promotion snapshots. The GSI key move. The halves after `batch_request_too_large`. A rejection of the whole call. GSI evaluation failures. |
| GSI evaluation | `gsi-target.test.ts` | `checkGsiPlans`: the SQL limit, and the statements that SQLite prepares on no row. The SQLite data error as a violation. |
| GSI receiver | `gsi-write-entries.test.ts` | Puts and deletes. The epoch fence. Ancestor fences. The migration of `producer_fences` with `MAX`. |
| `FokosDBMaterializationDO` splits | `split.test.ts`, `materialization-migration-host.test.ts` | A hash split and a promotion with a push target. The children send the fences and then drop them. Snapshot cursors. The copy of the key sets, in a unit test only. |
| GSI end-to-end and query | `gsi-targets.test.ts`, `gsi-query.test.ts` | An entry is written, moved, and deleted. A sparse key. A type violation, and its recovery on a newer write. Each sort-key condition. Cursors. Binary keys with a `0x00` byte. `{ ref: "sortKey" }`. A destroy, and a destroy that runs again. |
| Push target end-to-end | `push-targets.test.ts`, `stored-targets.test.ts` | One flow of a put, an update, and a delete. `validateTargets`. The check of the stored list at start. |

### 1.4 Glossary

| Term | Meaning |
| --- | --- |
| Quiescence | The state after the test stops its writes and each loop of each partition has no due work. |
| Convergence oracle | The expected state of each GSI and each push target, computed from the base table at quiescence. |
| Late call | A call of a producer that arrives at a receiver after a newer call of the same producer, or of its child. |

The other terms have the meaning of Section 1.4 of the materialization spec.

## 2. Goals and Requirements

### 2.1 In scope

- Fix or confirm the two code findings of Section 4.2.1 and Section 4.2.2, each one with its tests.
- A property-based suite that checks the convergence oracle after random work, splits, and injected faults.
- End-to-end tests for each P0 gap of Section 3: a late call after the tombstone GC, splits while work flows, and the
  transaction and TTL paths to a GSI.
- A P model of the export hop and the target hop, with monitors and seeded defects.
- Pin tests for the GSI shard group name and the derived GSI key limits, and a test of the rows written for each base
  write.
- Tests for each path of Section 4.2.9.
- Throughput and latency benchmarks for the export and the target runner.

### 2.2 Requirements

1. A test must not need a production hook (`AGENTS.md`). Section 4.2.11 gives the mechanism for faults.
2. A lifecycle test must use real timers and the scheduled-alarm test APIs. Global fake timers can run a background
   callback of a Durable Object in the wrong I/O context (`AGENTS.md`).
3. A property suite must use `fast-check` and must follow `test/property-based/README.md`: a small default run count,
   `FOKOS_PROPERTY_RUNS` for a deeper search, and the replay variables.
4. Each suite must make its own table with a `crypto.randomUUID()` prefix.
5. The convergence oracle must not depend on a clock or a timeout, as Requirement 9 of the materialization spec
   says for the delivery. A test waits for quiescence, and then compares.
6. The P model must follow the rules of `packages/fokosdb/formal/p/`: each `tcBug<Defect>` test case must fail with
   the monitor of its seeded defect, and each other test case must pass.

### 2.3 Out of scope

- A change of the materialization design. Section 4.2.1 changes one outcome of a failed push call, and Section 4.2.2
  makes the code agree with the spec. Nothing else changes.
- Production benchmarks. The benchmarks of this plan run in local workerd.
- A line-coverage tool. Section 4.3 has the question.

## 3. Milestones

The milestones run in this order. Each one ships on its own.

| Milestone | Priority | Delivers |
| --- | --- | --- |
| 1 | P0 | The two code findings (Sections 4.2.1 and 4.2.2), each with its tests. |
| 2 | P0 | The convergence property suite (Section 4.2.3). |
| 3 | P0 | An end-to-end test of a late call after the tombstone GC (Section 4.2.4). |
| 4 | P0 | End-to-end tests of splits while work flows (Section 4.2.5). |
| 5 | P0 | The P model of the export hop and the target hop (Section 4.2.6). |
| 6 | P1 | Pin tests, and a test of the rows written for each base write (Section 4.2.7). |
| 7 | P0 | End-to-end tests of the transaction and TTL paths to a GSI (Section 4.2.8). |
| 8 | P1 | Tests for the paths that no test reaches (Section 4.2.9). |
| 9 | P2 | Throughput and latency benchmarks (Section 4.2.10). |

Milestone 1 is first because each finding is small, isolated, and has a direct test. Milestone 7 is P0, and
Section 4.3.4 asks whether it moves before Milestone 6.

## 4. Proposed Solution

### 4.1 High-level overview

The plan has four layers of tests. Each layer finds a different kind of defect:

```text
1. Unit tests (exist)         one component, controlled clock and answers
2. End-to-end tests           real DOs: base -> FokosDBMaterializationDO -> GSI table or push target
                              + injected faults, + splits while work flows
3. Convergence property suite random operations, splits, and faults; then quiescence; then the oracle:
                                each GSI   == the projection of each live base item, with no extra entry
                                each push  == the base revision of each key as its latest record
4. P model                    the epoch and fence protocol, explored by the P checker and PEx
```

Layer 1 exists today. The plan first fixes two code paths that the audit found (Milestone 1). Then it adds layer 3
(Milestone 2), which checks the whole pipeline with one oracle. Then it adds the end-to-end tests of layer 2 for the
cases that a random search reaches only rarely: a late call after the GC (Milestone 3) and each kind of split
(Milestone 4). Then layer 4 checks the protocol with all interleavings that the checker explores (Milestone 5). Last,
the plan pins the stable encodings and the cost of a write, fills the remaining gaps, and measures the throughput.

### 4.2 Technical details

#### 4.2.1 Milestone 1a: a missing push binding is an unknown answer

**Current behavior.**

1. `validateTargets` (`src/server/materialization/targets.ts`) checks that `binding` and `method` are non-empty
   strings. Nothing checks that the Worker `env` has the binding.
2. `pushTargetStub` (`src/shared/do-stubs.ts`) throws an `invariant` error when `env` has no binding of that name.
   The `push` dependency of `TargetRunner` in `FokosDBMaterializationDO` calls it inside the call.
3. `TargetRunner.#send` catches each error and returns `null`, which means an unknown answer.
4. `TargetRunner.#complete` then calls `MaterializationStore.raiseTargetEpoch` (1 row written) and `#recordFailure`.
5. After `breakerFailures` (default 3) such failures, the circuit breaker opens. The runner then sends one probe every
   `probeIntervalMs` (default 5 s). Each probe fails in the same way, and raises the epoch again.

**Why it matters.** The call never left the partition, so no old call can apply. The epoch raise protects nothing,
and each raise costs one row written. Napkin math for one `FokosDBMaterializationDO` partition with this target:
`3600 s / 5 s = 720` raises each hour, which is 17,280 rows each day. With 100 partitions, that is about 1.7 million
rows each day, or about $1.73 each day at $1.00 for each million rows written (Section 1.3 of the materialization
spec). The target also gets no change, so the positions and the tombstones stay until the binding exists (Stage 1 of
Section 4.2.15 of the materialization spec).

A `method` that the binding does not have fails at the target, after the call left the partition. The runner cannot
tell that failure from a transport failure today. TODO: record the error that workerd raises for a method that the
target does not have.

**Proposed change.** A failure to get the stub is a definite failure of the whole call. The runner resolves the stub
before the call, outside the part that gives an unknown answer:

- The step writes no `retry` row, keeps the cursor, and does not raise the epoch.
- The circuit breaker counts one failure, as for a call whose items all fail with `retry`.
- The step logs one error with the `targetId` and the binding name.

Section 4.3 asks whether the partition also checks the bindings when it stores the target list and at start.

**Tests.**

- A `TargetRunner` unit test: a `push` dependency that throws before the call keeps the epoch and the cursor, writes
  no `retry` row, and opens the circuit breaker after `breakerFailures` steps.
- An end-to-end test: a materialization class with a push target whose binding is not in `env`. After some probes,
  `materialization_targets.epoch` is still 1, and `latest_items` holds each change.

#### 4.2.2 Milestone 1b: a constant `bytes` GSI projection passes the validation

**The rule.** Section 4.2.10 of the materialization spec says: "The validation of `GsiTargetDefinition` rejects a
compile-time constant `'bytes'` expression in `projection`." A GSI `projection` builds a JSONB document, which cannot
hold a `Uint8Array`.

**Current behavior.**

1. `validateTargets` calls `validateProjectionExpression` (`src/shared/expression/semantic.ts`) for a GSI
   `projection`. That function also serves query projections, where a `bytes` value is valid. It does not check the
   type.
2. `assertUpdateValueTypes` in `semantic.ts` has the rule for update values: it refuses a value whose only non-null
   type is `bytes`, for example a `{ b64: ... }` literal or `sqlite.unhex`. Nothing applies this rule to a GSI
   projection.
3. At run time, `desiredGsiEntry` (`gsi-target.ts`) returns the violation `update_value_is_bytes` for each live item.
   The step deletes each key in the key set, and parks each item as a violation.
4. A violation is a failure of one item, so the circuit breaker stays closed.

**Why it matters.** The GSI holds no entry, and each base write of each item costs a violation row write. The only
signal is one error log for each item.

**Proposed change.** `validateTargets` refuses a GSI `projection` entry whose only non-null type is `bytes`, with the
rule of `assertUpdateValueTypes`. A value that is `bytes` for some items only, for example `{ ref: "hashKey" }` on a
table with binary keys, stays valid, and stays a violation of each such item.

The check of the stored list at start (`#checkStoredTargets`) uses `validateTargets`. Thus a partition that already
stored such a target logs one error at each start, and continues with the stored list. Section 4.2.13 of the
materialization spec says that this check never refuses a stored list.

**Tests.**

- `validateTargets` unit tests: a `{ b64: ... }` literal and `sqlite.unhex` in a projection fail, and
  `{ ref: "hashKey" }` passes.
- A test of the stored-list check: a stored target with a constant `bytes` projection logs one error.
- An end-to-end test of the violation of one item: a projection of `{ ref: "hashKey" }` on a base item with a binary
  key parks that item with `update_value_is_bytes`, and a text-key item gets its entry.

#### 4.2.3 Milestone 2: the convergence property suite

**Location.** A new suite in `test/property-based/`, with its model next to `harness.ts`.

**Commands.** `fast-check` generates a sequence of these commands on a small key space, so that keys repeat:

- `putItem`, `updateItem`, and `deleteItem` on the base table, with and without a TTL;
- a `transactWriteItems` across more than one base partition, also with `executionMode: "ordered_per_item"`;
- a change of a GSI key attribute: set, change, remove, set to `null`, and set to a value of a type that is not
  valid;
- a split or a promotion of a base partition, of a `FokosDBMaterializationDO`, and of a GSI partition;
- an injected fault of Section 4.2.11: a held call, a lost answer, a refused call, and a restart of a partition.

**Oracle.** At quiescence, the suite reads the base table and computes the expected state:

1. Each GSI holds exactly one entry for each live base item with a valid GSI key. The entry has the projection of the
   latest image. No other entry exists.
2. For each live base item, the latest record of the key in each push target has the revision of the item. For a
   deleted key, the latest record is a tombstone at the delete revision. When the push target never got a revision of
   the key, it has no record of the key.
3. Each `FokosDBMaterializationDO` holds no tombstone that the GC can delete, and no `retry` row. It holds a
   `violation` row only for a live item whose latest image gives a GSI key that is not valid.
4. Each base partition holds no `materialization_outbox` row.

**Quiescence.** The suite drives `runDueWork` on each partition until no loop has due work, as `driveUntil` in
`test/materialization-do/split.test.ts` does. It uses no fixed wait.

#### 4.2.4 Milestone 3: a late call after the tombstone GC

Section 4.2.9 of the materialization spec gives the reason for the same-epoch rule: a late live image that arrives
after the GC deleted its tombstone creates the item again. Each side is tested on its own today. One end-to-end test
puts the two sides together:

1. Hold an export call of revision 2 of item X in flight.
2. Delete X. A newer call with a higher epoch exports revision 3 (the tombstone).
3. Let each target read the tombstone, and let the GC delete it.
4. Release the held call of revision 2.
5. Check that `latest_items` has no live row of X, that no GSI has an entry of X, and that no push target got
   revision 2 after revision 3.

A second test does the same on the target hop: a held `gsiWriteEntries` put of an old key arrives after the delete of
the key and after the GC.

#### 4.2.5 Milestone 4: splits while work flows

Each test writes while the split runs, waits for quiescence, and checks the convergence oracle of Section 4.2.3:

- **A base split and a base promotion.** The test today checks only the outbox rows, not `latest_items` and the
  targets.
- **A `FokosDBMaterializationDO` split and promotion with GSI targets.** Only a push target is tested today. This test
  checks that the key sets and the fences reach each `gsiWriteEntries` receiver.
- **A range split of a promoted key in `FokosDBMaterializationDO`.**
- **A split of a split** before the inherited work of the first split is done, on the `FokosDBMaterializationDO` side.
- **A promotion of a GSI table key with low cardinality**, for example `status = "OPEN"`, while writes flow, and then a
  `db.gsi(indexName).queryItems` across the range tree of the GSI table.

#### 4.2.6 Milestone 5: the P model of the export and target hops

The model adds producers, receivers, and splits to `packages/fokosdb/formal/p/`:

- **Machines.** A producer with its epoch and its in-flight calls. A receiver with `producer_fences`. A split that
  raises the epoch of the parent and gives fences to a child. A tombstone GC.
- **Monitors.** "A deleted item does not come back." "Each target converges to the latest revision." "No orphan GSI
  entry stays."
- **Seeded defects.** A GC that ignores the fences. A cutover with no epoch raise.

`docs/ideas/2026-10-09-p-model-of-fokosdb-transactions.md` describes the existing model of the transactions.

#### 4.2.7 Milestone 6: pin tests and the cost test

- **The GSI shard group name.** `gsiShardGroup` in `src/shared/partition-context.ts` gives
  `fokos.gsiv1.<n>.<tableName>.<indexName>`. The name is the address of the GSI data and must never change. A test
  pins it for names that contain `.`.
- **The derived GSI key limits.** A test pins `gsiLimits`: with the default limits, `maxSortKeyBytes` is 4,618 bytes.
  A property test checks that the physical sort key of each valid base item fits the limit, for text and binary keys
  at the maximum size, and for keys of only `0x00` bytes.
- **The rows written for each base write.** A test asserts the `rowsWritten` of each case of the table "Cost of one
  base write" in Section 4.2.15 of the materialization spec: a new item, a put whose GSI keys stay, a GSI key move, and
  a delete. It runs in `pnpm test`, so a cost regression fails CI.

#### 4.2.8 Milestone 7: the transaction and TTL paths to a GSI

- A `transactWriteItems` through the coordinator with `captureChanges = true` reaches each GSI.
- `executionMode: "ordered_per_item"` with more than one operation on one item exports the last revision.
- A committed transactional `check` writes no `materialization_outbox` row (Requirement 4 of the materialization
  spec).
- A TTL expiry of a base item deletes its GSI entries, and the GSI entry carries the `ttlAt` of the base item.

#### 4.2.9 Milestone 8: the paths that no test reaches

| Component | Paths with no test |
| --- | --- |
| `MaterializationExporter` | The `callTimeoutMs` timeout. A `rejected` item and its `rejectedRetryMs` defer. The `maxItems`, `maxBytes`, and `maxScannedRows` bounds. The backoff that doubles up to `failureBackoffMaxMs`. `canRun` false at the completion. A restart through the `materialization_export` job after an eviction. |
| `TargetRunner` | The `callTimeoutMs` timeout. The `maxScannedPositions` bound on a long run of skipped positions. `canRun` false while a batch is in flight. The round robin with more than 2 targets. A restart through the `target_push` job after an eviction. |
| `GsiWriter` | Some GSI partitions fail while others apply, in one call. A forward through a GSI router after a GSI split. The classification of a real transport error as a whole-call failure. The `invariant_failed` fallback. |
| `gsiWriteEntries` receiver | Above the size limit, the puts are refused and the deletes apply. The promotion signal of the GSI writes. |
| Violation codes | `update_value_is_bytes` from a projection (Section 4.2.2 adds it). `key_contains_nul`. `key_not_well_formed_utf16`. A derived GSI key above `maxHashKeyBytes` or `maxSortKeyBytes`. A failed delete during the violation cleanup, which must give a `retry` row. |
| Target configuration | More than 10 GSI targets. An invalid `fokosTargets()` at the first ingest: each item must be `unprocessed`, and the exporter must back off. The log when `fokosTargets()` is different from the stored list. A push binding that `env` does not have (Section 4.2.1 adds it). A `WorkerEntrypoint` push target with no `objectName`. |
| GSI query | `between` with a lower bound above the upper bound, and an empty `range`. `begins_with` on a prefix of only `0xFF` bytes, which has no successor. A query bound above `maxSortKeyBytes`. |
| Destroy | A late call that creates a root again after a destroy, and the second `destroy()` that deletes it. Section 4.2.13 of the materialization spec describes this best-effort case. |

#### 4.2.10 Milestone 9: performance

The benchmarks run with `pnpm bench:workerd`, as `gsi-cost.workerd-bench.ts` does:

- The drain rate of the export: images each second from one base partition. TODO: measure.
- The drain rate of the target runner after an outage, with 1, 2, and 10 GSI targets. Section 4.2.15 of the
  materialization spec estimates 1,000 to 2,000 items each second across 4 partitions. TODO: measure.
- The alarm writes of `TargetRunner.wake`. The method calls `scheduleRetry(now + fallbackAlarmMs)` each time the
  workers start from idle. This can cost one alarm write for each burst of ingests, unless `FokosScheduler` skips an
  alarm that is already set. TODO: measure.
- The epoch raises during an outage. Each probe with an unknown answer raises the epoch: one row at the producer, and
  one `producer_fences` row at each receiver that gets the call first. TODO: measure the rows for each hour of
  outage.

#### 4.2.11 Fault injection without a production hook

- **Base and GSI partitions.** A test table with `ns: "CONTROLLED_PARTITION_DO"` makes each base partition and each
  GSI partition a `ControlledPartitionDO` (`test/controlled-partition-do.ts`), because a GSI table uses the `ns` of the
  base table. The class gets new controls that hold, fail, or answer one `gsiWriteEntries` call.
- **`FokosDBMaterializationDO`.** A new test class, in the pattern of `ControlledPartitionDO`, holds, fails, or answers
  one `applyLatestImages` call. It overrides `fokosNow()` for a test clock, as the class comment of `fokosNow()`
  allows.
- **Push targets.** `PushSinkDO` (`test/materialization-do/push-target-host.ts`) gets controls that hold a call, lose
  its answer, and answer `retry` or `rejected` for chosen keys.

Each control is a field of one instance, so no other test can reach it.

### 4.3 Open Questions

#### 4.3.1 Does the partition check the push bindings when it stores the target list?

Section 4.2.1 makes a missing binding a definite failure at each step. The partition can also check `env` once:

- **At the first store.** A missing binding then stores nothing, and each item of the call is `unprocessed`, as for
  any target list that is not valid. This stops a table with a configuration error before it stores the list.
- **At each start**, in `#checkStoredTargets`. A missing binding logs one error, and the partition continues with the
  stored list.
- **Neither.** The step logs one error at each probe.

The answer changes when an operator sees the error, and whether a deploy that removes a binding stops new tables.

#### 4.3.2 Does the package add a line-coverage tool?

`@cloudflare/vitest-plugin` supports Istanbul coverage only, not V8 coverage. The tool shows the lines that no test
runs, but not the rules that no test checks. It is a project configuration change.

#### 4.3.3 Is the cost test of Section 4.2.7 exact or a range?

An exact count fails at each change of a statement. A range lets a small regression pass. The answer changes how
often the test needs an update.

#### 4.3.4 Does Milestone 7 move before Milestone 6?

Milestone 7 is P0 and Milestone 6 is P1. The order of Section 3 keeps the pin tests and the cost test first, because
they are small. The answer changes when the transaction and TTL paths to a GSI get an end-to-end test.

## 5. Frequently Asked Questions

### 5.1 Why is Milestone 1 first?

Each finding is small and isolated, and its fix comes with a direct test. Milestone 2 then runs on code that agrees
with the spec.

### 5.2 Why does the convergence suite come before the end-to-end tests of Milestones 3 and 4?

One oracle checks the export, the positions, the key sets, and the GC together, so it finds the most defects for its
cost. The end-to-end tests then cover the interleavings that a random search reaches only rarely.

### 5.3 Does this plan change the materialization design?

No. Section 4.2.1 changes the outcome of a push call that never left the partition, from unknown to a definite
failure. Section 4.2.2 makes `validateTargets` agree with Section 4.2.10 of the materialization spec.

## 6. References

References:

- `docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md`
- `docs/ideas/2026-10-09-p-model-of-fokosdb-transactions.md`
- `packages/fokosdb/test/property-based/README.md`
- `AGENTS.md`
- [Workers Vitest integration: known issues (coverage)](https://developers.cloudflare.com/workers/testing/vitest-integration/known-issues/)
