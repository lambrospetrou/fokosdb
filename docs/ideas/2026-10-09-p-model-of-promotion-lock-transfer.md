# RFC — A P model of a key promotion that moves its locks while transactions run

**State:** Draft
**Date:** 2026-10-09
**Author:** Lambros Petrou
**Status:** Not started. No model, script, or test exists yet.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
  - [1.1 The problem](#11-the-problem)
  - [1.2 Why now](#12-why-now)
  - [1.3 The flow in one page](#13-the-flow-in-one-page)
  - [1.4 Glossary](#14-glossary)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

### 1.1 The problem

A key promotion moves one hash key, with all its sort keys and its transaction locks, from a hash partition into a
new range root. `docs/agent-plans/2026-09-29-promotion-moves-its-locks.md` removed the rule that a promotion waits for
zero locks. Now a lock moves with its key, and the source keeps a copy of each moved lock until the target
acknowledges its import.

The flow has many actors and many messages in flight at the same time:

- the source partition, which still serves its other keys and their transactions;
- the range root, which the source creates, initializes, and then feeds with migration pages;
- the coordinator, which sends prepare, commit, and cancel to the source, and the source forwards the moved keys;
- the stale-transaction job on both partitions, which asks the coordinator about old locks;
- the operator repair `debugForceResolveTransaction`;
- restarts of any Durable Object between any two blocks.

The design found three defects before the code shipped (section 1.2 of the promotion RFC):

1. `recoverStaleTransactions` deleted a `not_found` transaction when none of its keys was owned. The target could
   still need those rows.
2. `commitLocal` compared the request with every pending row of the transaction, copies included. A commit of the
   retained keys then failed.
3. The routed `txCommit` and `txCancel` deleted by transaction id, so they also deleted the copies.

The tests in `packages/fokosdb/test/partition-do/tx-promotion-transfer.test.ts` and
`packages/fokosdb/test/property-based/transactions-split.test.ts` check chosen orders and sampled orders. They cannot
restart a Durable Object between two arbitrary blocks, and they do not explore the orders of in-flight messages
across a cutover. A defect of this kind loses a decided write or blocks a lock for good, and nothing reports it at
the time.

### 1.2 Why now

- The flow is new (implemented on 2026-09-29), and the next runtime changes build on it.
- `docs/agent-plans/2026-10-09-quint-model-of-the-transaction-coordinator.md` checks the coordinator rules. This
  model can use an abstract coordinator and spend its state space on routing, migration, and lock copies.

### 1.3 The flow in one page

The model follows these facts of the current code.

**Source states** (`packages/fokosdb/src/sharding/repartition-flow.ts`): `queued → planned → cutover → completed →
cleaned`. **Target states**: `awaiting_data → importing → imported → active`.

**Steps:**

1. The source queues the promotion of hash key `K` and plans one target, the range root `R`.
2. The source marks `R` `initializing`, and then calls `fokosInit`. `R` writes its identity and an import record in
   `awaiting_data`. On success the source marks `R` `initialized`.
3. When every target is `initialized`, the source writes `cutover` in one transaction. FokosDB does not implement
   `beforeCutover`, so a lock does not hold the cutover.
4. The source calls `fokosStartImport`. The call is an optimization: `R` also starts from its own alarm.
5. `R` pulls one page at a time with `fokosMigrationPull`: the overrides phase (empty for a promotion), then the host
   phase with the `items` stream and then the `pending_tx` stream. Each page commits with its cursor. A page whose
   cursor no longer matches the durable cursor is dropped. The last page commits with `imported`.
6. `R` retries `fokosMigrationAck` until the source accepts it. The source records the acknowledgement. When every
   target acknowledged, the same transaction writes `completed` and runs `beforeComplete`, which deletes the size
   estimate of `K`.
7. `cleanupSourceStep` deletes the pending copies of `K` in batches of whole transactions
   (`deletePendingTxCopiesBatch`), then the item copies of `K`. The last step writes `cleaned`.

The promotion RFC differs from the code in two places. Its section 4.2.8 deletes the pending copies in the completion
transaction, and its section 4.2.6 keeps `guarded_at` off a copy. The model follows the code and `AGENTS.md`.

**Ownership.** On the source, `owns(K)` is false when the route override of `K` is in `cutover`, `completed`, or
`cleaned`. `R` owns `K`. An importing `R` answers `partition_migrating` for every write and transaction operation,
and reads through the source for `apiGetItem` and `apiQueryItems`.

**Transactions** (`packages/fokosdb/src/server/do-partition.ts`,
`packages/fokosdb/src/shared/partition/transaction-participant.ts`):

- The coordinator groups the keys by root partition, so the source is the participant for both `K` and its retained
  keys `U`. After cutover, `dispatch` divides each `group` request by owner: the local part runs in the same block,
  and the moved part goes to `R`.
- `txPrepare` is `fail_fast`. `txCommit` and `txCancel` are `attempt_all`: they run every part and throw
  `partition_fanout_failed` after the local part applied.
- `commitLocal` builds the owned set: a row whose key is in the request, or a row that `owns()` accepts. With no
  owned row it answers the idempotent success. Otherwise the request must match the owned set, or it throws
  `commit_keyset_mismatch`. It deletes one row per key, never by transaction id alone.
- `cancelLocal` deletes the rows of the owned keys that `dispatch` hands it. An empty `items` list releases nothing.
- `recoverStaleTransactions` claims one transaction, skips it when every row is a copy, calls
  `recoverTransactionForParticipant`, then reads the rows and their owners again after the `await`. It applies the
  answer through `dispatch`, so each key reaches its current owner. A `not_found` answer for a lock older than
  `IDEMPOTENCY_WINDOW_MS` sets `guarded_at` on the `pending_tx_info` row, which covers the copies too.
- `debugForceResolveTransaction` sends every row of the transaction through `dispatch` and mutates owned rows only.

### 1.4 Glossary

The terms of the promotion RFC apply: **source**, **target**, **owned row**, **transfer copy**, **completion
transaction**, **importing target**, **imported target**. This document adds:

- **Block** — the synchronous code between two `await`s of one Durable Object.
- **Durable state** — the SQLite state of a machine. A restart keeps it.
- **Memory state** — every other field of a machine: in-flight continuations, timers, caches. A restart clears it.
- **Seeded defect** — a deliberate defect behind a flag of the model. The checker must find a violation for it.

## 2. Goals and requirements

### 2.1 In scope

- A P model of one source, one range root, one coordinator, one client, and an operator, for one promotion of one
  hash key while 2 or 3 transactions run.
- Each event handler of a Durable Object machine is one block of the code, and the model names the code function of
  each handler.
- Faults: a restart of any Durable Object between any two blocks, a lost request or answer, any delivery order of
  in-flight calls, and coarse time steps.
- The safety monitors of section 4.2.7 hold for every test case of section 4.2.9.
- The liveness monitors of section 4.2.7 hold when the faults stop.
- Each seeded defect of section 4.2.8 makes the checker report a violation of the monitor that the table names.
- Each real defect that the model finds gets a vitest regression test that runs the real code in workerd.

### 2.2 Out of scope

- **A hash split after a promotion, the Bloom filter, and learned routes.** They add more targets and a routing cache.
  They can extend this model after milestone 5.
- **Range splits and coordinator splits.** Same reason.
- **The coordinator protocol.** The Quint model checks it. This model uses an abstract coordinator that follows the
  same rules (section 4.2.6).
- **`apiQueryItems` and the range walk.** The model reads with `apiGetItem` only.
- **The TTL sweep, size admission, and the split decision.** The environment requests the promotion directly.
- **Serializability across keys.** The model checks only the effect of each transaction on its own keys.
- **PObserve.** A log-based check of production runs needs a log parser for the TypeScript code. It can come later.

### 2.3 Requirements

- The work must not change production code and must not add a production hook.
- The model must use the names of the code: states, operations, error codes, and function names.
- The tool version must be pinned.
- `pnpm check` must stay green.

## 3. Milestones

Each milestone ends with its test cases green and the seeded-defect table of section 4.2.8 updated.

1. **Happy path.** Machines, events, the repartition flow, `dispatch`, and the participant rules, with no faults.
   Monitors `Atomicity`, `Authority`, `CopiesUntouched`, `Retention`, and `SingleApply`. Test case `tcHappy`.
2. **Faults and recovery.** Restarts, lost calls, delivery order, time steps, the stale job, the quarantine, and
   `debugForceResolveTransaction`. Monitor `ReadAfterCommit`. Test cases `tcFaults`, `tcStale`, `tcRepair`.
3. **Seeded defects.** Each defect of section 4.2.8 behind a flag, and one test case for each that must fail.
4. **Liveness.** Hot states for `LocksResolve`, `PromotionCompletes`, and `ClientAnswered`.
5. **Link to the code.** A vitest regression test for each real defect that milestones 1 to 4 found, a `formal:p`
   script in `packages/fokosdb/package.json` and a root script that calls it, and the `AGENTS.md` entry.

Milestones 1 and 2 deliver a design check on their own.

## 4. Proposed solution

### 4.1 High-level overview

P describes a system as state machines that send events to each other. A machine handles one event at a time, to the
end, before it takes the next one. A Durable Object works the same way between two `await`s. Thus one P event handler
is one block of a Durable Object, and an `await` is a send followed by a later event.

```text
 Client --initiate/retry--> Coordinator (abstract)
                               |  txPrepare / txCommit / txCancel to the source
                               v
        +------------------------- Source S (hash leaf) ---------------------------+
        | owns U always, owns K until cutover                                      |
        | items, pending rows (owned or copy), pending_tx_info, repartition row    |
        +--------------------------------------------------------------------------+
           | fokosInit, fokosStartImport        ^ fokosMigrationPull, ack, read-through
           | forwarded parts of K               |
           v                                    |
        +------------------------- Range root R (created by fokosInit) ------------+
        | import record: awaiting_data -> importing -> imported -> active          |
        +--------------------------------------------------------------------------+
 Environment: restart S, R, or the coordinator; lose a call; move time; request the promotion
 Operator:    debugForceResolveTransaction on S or R
```

The P checker runs the test cases many times. In each run it chooses the order of the events, the fault points, and
each nondeterministic choice. Spec monitors receive events that the machines announce and check the rules of the
promotion RFC. A violation gives a trace that the checker can replay.

Each known defect is a flag of the model. The checker must find a violation when the flag is on. When it does not,
the model is too coarse at that point.

### 4.2 Technical details

#### 4.2.1 Layout and tools

```text
packages/fokosdb/formal/p/promotion-transfer/
  PromotionTransfer.pproj
  PSrc/      Partition.p, Coordinator.p, Client.p, Operator.p, RpcCall.p, Environment.p, Types.p
  PSpec/     Atomicity.p, Authority.p, Copies.p, Liveness.p
  PTst/      TestDriver.p, TestCases.p
```

- The checks run in the official Docker image `ghcr.io/p-org/p:<tag>`, which holds the .NET SDK 8.0, Java, and the
  `p` tool. Milestone 1 pins the tag. Only Docker is necessary on the machine.
- Commands, from `packages/fokosdb/formal/p/promotion-transfer/`:

  ```sh
  docker run --rm -v "$PWD":/workspace ghcr.io/p-org/p:<tag> p compile
  docker run --rm -v "$PWD":/workspace ghcr.io/p-org/p:<tag> p check -tc <test case> -i <runs>
  ```

- The run counts are `TODO: measure` in milestone 1.
- The P output directories `PGenerated/` and `PCheckerOutput/` go into `.gitignore` and `.prettierignore`.
- The package publishes only `dist`, so `formal/` does not go into the npm package.

#### 4.2.2 Semantics of the mapping

- **One handler, one block.** A handler must not contain more than one block of the code. A code path with an
  `await` becomes two handlers: the first sends the call and stores a continuation in memory state, and the second
  handles the answer.
- **Delivery order.** P delivers the events from one sender to one receiver in order. Workers RPC gives no such order,
  and calls can get lost. Each call therefore goes through a new `RpcCall` machine. It delivers the request, can lose
  the request or the answer within the fault budget, and returns the answer or an error to the caller. Independent
  `RpcCall` machines give every delivery order.
- **Durable and memory state.** Each Durable Object machine keeps its durable state in one record and its memory
  state in another. A restart event clears the memory state, drops the continuations, and keeps the durable record.
  An answer that arrives for a dropped continuation is ignored, as a lost promise in the code.
- **Time.** Ages are classes, as in section 4.2.5 of the Quint plan: `Fresh`, `Stale`, `OverWindow` for a lock. The
  environment moves one class forward at a time.

#### 4.2.3 Machines

| Machine | Durable state | Memory state | Code |
| --- | --- | --- | --- |
| `Partition` (S and R) | identity, `items`, pending rows, `pending_tx_info` (`created_at` class, `guarded_at`), repartition row and target row (source side), route override of `K`, import record and cursor (target side) | continuations of forwarded calls, the stale job in progress | `PartitionDO`, `FokosShardingRuntime`, `RepartitionSource`, `RepartitionTarget`, `FokosMigrationHost`, `TransactionParticipant` |
| `Coordinator` | for each transaction: state, stored prepare answers, commit and cancel outcomes | the drives in progress | abstract `TransactionCoordinatorDO` (section 4.2.6) |
| `Client` | — | the transactions it waits for | `FokosDB.transactWriteItems`, `FokosDB.getItem` |
| `Operator` | — | — | an operator who calls `debugForceResolveTransaction` |
| `RpcCall` | — | one request and its answer | Workers RPC, `FokosShardingClient` retries |
| `Environment` | — | the fault budget | eviction, crashes, the wall clock |

One `Partition` machine type serves both roles, as one `PartitionDO` class does in the code. The source creates `R`
with `new` on the first `fokosInit`. A later `fokosInit` to `R` reaches the same machine.

#### 4.2.4 Events

| Event | From → to | Handler models |
| --- | --- | --- |
| `eRequestPromotion(K)` | Environment → S | `requestPromotion`, `RepartitionSource.queue` |
| `eSourceStep` | S → S (alarm) | `sourceStep`: plan, initialize, cutover, notify, cleanup step |
| `eFokosInit` / `eFokosInitResp` | S ↔ R | `fokosInit`, `#initializeTargets` |
| `eStartImport` | S → R | `fokosStartImport` |
| `eImportStep` | R → R (alarm) | `importOnePage` |
| `eMigrationPull` / `eMigrationPage` | R ↔ S | `servePage`, `buildPage` of `FokosMigrationHost` |
| `eMigrationAck` / `eMigrationAckResp` | R ↔ S | `acceptAck`, `beforeComplete` |
| `eTxPrepare`, `eTxCommit`, `eTxCancel` and answers | Coordinator ↔ S, S ↔ R | `dispatch` of a `group` operation, `prepareLocal`, `commitLocal`, `cancelLocal` |
| `eGetItem` / `eGetItemResp` | Client ↔ S or R | `apiGetItem`, read through `fokosExecuteLocal` |
| `eStaleStep` | partition → itself (alarm) | `recoverStaleTransactions` |
| `eRecover` / `eRecoverResp` | partition ↔ Coordinator | `recoverTransactionForParticipant` |
| `eForceResolve` / `eForceResolveResp` | Operator ↔ partition | `debugForceResolveTransaction` |
| `eRestart` | Environment → any Durable Object | eviction or crash |
| `eAge` | Environment → partitions, Coordinator | the wall clock |

Page size in the model is one row. One row for each page gives the most interleavings between the `items` stream,
the `pending_tx` stream, and the transaction operations.

#### 4.2.5 Partition rules in the model

These rules repeat the code, so a reviewer can compare them one by one:

1. **Owner.** `owns(k)` on S is false for a key of `K` when the override is `cutover`, `completed`, or `cleaned`, and
   true for every other key. R owns every key of `K`.
2. **Import gate.** An importing R answers `partition_migrating` for `txPrepare`, `txCommit`, `txCancel`, and
   `debugForceResolveTransaction`, and reads through S for `apiGetItem`. An imported R serves all of them.
3. **Group dispatch.** S divides the keys by owner in the handler that receives the request. The local part runs in
   that handler. Each remote part goes to R in its own `RpcCall`. `txCommit` and `txCancel` answer
   `partition_fanout_failed` when a remote part failed, after the local part applied.
4. **Prepare.** A key locked by another transaction gives `pending_conflict`. Otherwise every key gets a lock row and
   the transaction gets a `pending_tx_info` row.
5. **Commit.** As `commitLocal` (section 1.3). One row per key is applied and deleted.
6. **Cancel.** As `cancelLocal`. Only the owned keys of the request.
7. **Stale job.** As `recoverStaleTransactions` (section 1.3). It runs only when `canSweepLocally` is true: not behind
   the destroy fence, not on a router, not on an importing target.
8. **Repair.** As `debugForceResolveTransaction`. Every row goes through rule 3.
9. **Migration pages.** The `items` stream copies the item rows of `K`. The `pending_tx` stream copies the lock rows of
   `K` with their `pending_tx_info` fields, `guarded_at` included. A page whose cursor does not match is dropped.
10. **Copy deletion.** The cleanup steps after `completed` delete the pending copies of `K`, one whole transaction
    at a time, and then the item copies.

#### 4.2.6 The abstract coordinator

The coordinator keeps one record for each transaction and follows these rules, which the Quint model checks:

- It writes each decision before it sends a commit or a cancel.
- It decides commit only when every stored prepare answer is `accepted`. A rejection, a fatal error, or a hold past
  the bound decides cancel.
- It sends commits only in `COMMITTING` and cancels only in `CANCELLING`, and retries until each participant confirms.
- `recoverTransactionForParticipant` answers `COMMITTED`, `CANCELLED`, `not_found` after the idempotency window, or
  `driving`, and a `driving` answer makes the recovery due now.
- It answers the client `committed` only in `COMMITTED`.

The model leaves out concurrent drives, because the Quint model covers their races. When the Quint model changes a
rule, this coordinator changes in the same change.

#### 4.2.7 Monitors

**Safety monitors:**

| Monitor | Rule | Events it observes | Source of the rule |
| --- | --- | --- | --- |
| `Atomicity` | No row of a transaction applies a commit when the decision is cancel. No owned row of a transaction is released by a cancel when the decision is commit. | `eDecision`, `eApplied`, `eReleased` | 2PC |
| `Authority` | A lock, an apply, a release, or a guard of an owned row happens only on the current owner of the key, and never on an importing target. | `eLocalTxMutation`, `eOwnerChanged`, `eImportState` | Promotion RFC 4.1, rule "Authority" |
| `CopiesUntouched` | No commit, cancel, quarantine, or repair deletes or changes a transfer copy. | `eCopyChanged` | Promotion RFC 4.2.5, 4.2.6 |
| `Retention` | S deletes a transfer copy only after it recorded the acknowledgement of R. | `eCopyDeleted`, `eAckRecorded` | Promotion RFC 4.1, rule "Retention" |
| `SingleApply` | Each key of a committed transaction applies at most once, over S and R together. | `eApplied` | Promotion RFC 4.2.9 |
| `ReadAfterCommit` | A read that starts after the client got `committed` returns the value of that transaction or of a later one. | `eClientAnswer`, `eReadStart`, `eReadResult` | Strong consistency of `getItem` |

`guarded_at` is one field of the `pending_tx_info` row, so a guard of owned rows also marks the copies of the same
transaction. `CopiesUntouched` counts a guard as a change of a copy only for a copy with no owned row of the same
transaction on that partition.

**Liveness monitors** (hot states). Each one holds when the environment stops the faults after its budget:

| Monitor | Rule |
| --- | --- |
| `LocksResolve` | Each lock row, on S or R, is eventually deleted or guarded. |
| `PromotionCompletes` | The promotion eventually reaches `cleaned`, and R reaches `active`. |
| `ClientAnswered` | Each transaction of the client eventually gets `committed` or `cancelled`. |

#### 4.2.8 Seeded defects

Each defect is a field of a `Bugs` record that the test driver passes to each machine at creation.

| Id | Change | Monitor that must fail | Source |
| --- | --- | --- | --- |
| D1 | The stale job deletes a `not_found` transaction when no row is owned. | `CopiesUntouched`, `SingleApply` or `Atomicity` | Promotion RFC 1.2, item 1 |
| D2 | `commitLocal` compares the request with all rows, copies included. | `ClientAnswered`, `LocksResolve` | Promotion RFC 1.2, item 2 |
| D3 | The routed `txCommit` and `txCancel` delete by transaction id. | `CopiesUntouched` | Promotion RFC 1.2, item 3 |
| D4 | S deletes the copies of `K` at cutover. | `Retention` | Promotion RFC 4.2.8 |
| D5 | An importing R serves transaction operations. | `Authority` | Promotion RFC 4.2.2, stage 3 |
| D6 | The stale job uses the owner result from before the coordinator call. | `Authority` | Promotion RFC 4.2.6 |
| D7 | A cancel with an empty `items` list releases by transaction id. | `CopiesUntouched` | Promotion RFC 4.2.5 |

For D1, D3, and D4, the run that loses a decided write must also exist: the copy goes before the `pending_tx` stream
reads it. The test case for each one asserts the first monitor, and the milestone records whether the checker also
finds the lost write.

#### 4.2.9 Configurations and test cases

Keys: `K` with sort keys `k1` and `k2`, and one retained hash key `U` with `u1`.

| Test case | Transactions | Faults | Expected |
| --- | --- | --- | --- |
| `tcHappy` | T1 on `k1`, `u1`; T2 on `k2` | none | pass |
| `tcFaults` | T1 on `k1`, `u1`; T2 on `k2`; T3 on `k1` (conflicts with T1) | 2 restarts, 2 lost calls | pass |
| `tcStale` | T1 on `k1`, `u1` | the coordinator stops answering until `OverWindow`, then answers `not_found` | pass |
| `tcRepair` | T1 on `k1`, `u1` | as `tcStale`, then the operator repairs on S and on R | pass |
| `tcBugD1` … `tcBugD7` | as `tcFaults` | as `tcFaults` | fail with the monitor of section 4.2.8 |

The environment requests the promotion at a nondeterministic point. A run therefore covers each order of the
promotion RFC table 4.2.9: a prepare before the cutover, during target initialization, after the cutover, and a
commit or a cancel during the import.

#### 4.2.10 Link to the code

P does not run the TypeScript code. The model links to the code in two ways:

1. **Regression tests.** Each real defect that the model finds becomes a vitest test in
   `packages/fokosdb/test/partition-do/tx-promotion-transfer.test.ts`. The test drives the same order with
   `TestPartition` (`triggerPromotion`, `runAlarm`, `drainUntil`) and the gates of
   `ControlledPartitionDO`: `testHoldPulls` with the `pending_tx` stream, `testRefuseAcks`, `testHoldInit`, and
   `testHoldPrepare`. A restart in the trace becomes a test control of `ControlledPartitionDO` or
   `ControlledTransactionCoordinatorDO` that calls `ctx.abort()`. The next request then makes a new instance that
   reads SQLite.
2. **Names.** Each handler names its code function in a comment, so a reviewer of a code change can find the handler
   that must change.

#### 4.2.11 Maintenance

- A plan that changes the repartition flow, the participant rules, or the ownership rules must update the model and
  its seeded-defect table in the same change.
- The spec-review skill asks for that update.
- `AGENTS.md` gets one line under "Where the detail lives" that names the model and the command.
- No CI runs the checks. The author of a change to one of these rules runs them.

#### 4.2.12 Cost

The model adds no production code and no runtime cost. The checks run outside `pnpm test`. Their run time is
`TODO: measure` in milestone 1.

#### 4.2.13 Testing

The work is correct when:

- Each test case of section 4.2.9 that expects a pass passes for the measured run count.
- Each seeded defect fails with its monitor.
- Each real defect has a vitest regression test that fails before its fix and passes after it.

## 5. Alternative options

- **Quint or TLA+ for this flow.** The flow creates a Durable Object at run time and has many calls in flight. A TLA
  model needs a fixed set of partition names and a hand-written network, and Apalache handles a growing structure
  slowly. P has machine creation and asynchronous events built in.
- **More property-based suites.** `packages/fokosdb/test/property-based/transactions-split.test.ts` runs the real
  code and finds real defects. It samples a few schedules, and it cannot restart a Durable Object between two
  arbitrary blocks or lose one answer of many.
- **Deterministic simulation of the real code** (`docs/ideas/2026-09-26-testing-approaches.md`, section 5.5). It
  checks the code itself, but it needs a simulation layer for workerd and SQLite. The model is a smaller first step.

## 6. Frequently asked questions

**Does a passing run prove the flow correct?** No. The P checker samples schedules within a bound. A pass shows that
it found no violation in the measured number of runs. A seeded defect that it finds shows that the bound reaches
that class of defect.

**Why an abstract coordinator?** The coordinator races are the subject of the Quint model. Here they only add states.
The abstract coordinator keeps the rules that the partitions depend on.

**Why one source and one target?** One promotion has one target. A hash split adds targets and the override rules of
`belongsToTarget`. That extension comes after milestone 5.

**Why a page size of one row?** A larger page hides the orders between two rows of one stream. The real page budget
changes only the number of pulls.

**What if the model finds a defect in the code?** The author writes the vitest regression test first, then fixes the
code in a separate change with its own plan when the fix changes a rule.

## 7. References

- `AGENTS.md`
- `packages/fokosdb/src/server/do-partition.ts`
- `packages/fokosdb/src/shared/partition/transaction-participant.ts`
- `packages/fokosdb/src/shared/partition/partition-store.ts`
- `packages/fokosdb/src/sharding/repartition-flow.ts`
- `packages/fokosdb/test/controlled-partition-do.ts`
- `packages/fokosdb/test/partition-do/partition-harness.ts`
- `packages/fokosdb/test/partition-do/tx-promotion-transfer.test.ts`
- `packages/fokosdb/test/property-based/transactions-split.test.ts`
- `docs/agent-plans/2026-09-17-unified-repartition-flow.md`
- `docs/agent-plans/2026-09-29-promotion-moves-its-locks.md`
- `docs/agent-plans/2026-10-09-quint-model-of-the-transaction-coordinator.md`
- `docs/ideas/2026-09-26-testing-approaches.md`
- [P documentation](https://p-org.github.io/P/)
- [Installing P](https://p-org.github.io/P/getstarted/install/)
- [P liveness specifications](https://p-org.github.io/P/advanced/importanceliveness/)
- [Systems Correctness Practices at AWS](https://queue.acm.org/detail.cfm?id=3712057)
