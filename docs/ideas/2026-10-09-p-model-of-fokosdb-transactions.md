# RFC — A P model of FokosDB transactions under faults, splits, and key promotions

**State:** Draft
**Date:** 2026-10-09
**Author:** Lambros Petrou
**Status:** M0 and M1 are complete. M2 is in review. M3 to M7 are not started.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
  - [1.1 The problem](#11-the-problem)
  - [1.2 Why now](#12-why-now)
  - [1.3 The system in one page](#13-the-system-in-one-page)
  - [1.4 Glossary](#14-glossary)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)
- [Appendix A. An order that only the `maxDeletedV` comparison catches](#appendix-a-an-order-that-only-the-maxdeletedv-comparison-catches)

## 1. Overview and context

### 1.1 The problem

`transactWriteItems` and `transactGetItems` give two public guarantees:

- A write transaction applies on all its items or on none.
- A read transaction returns a state that a serial order of the committed writes can produce.

Each guarantee depends on rules in more than one Durable Object: the client in the Worker, the coordinator, and each
partition. A hash split and a key promotion move items and locks between partitions while transactions run.

Each of these rules exists because of a defect:

- **Coordinator.** `markCommitting` requires a stored accepted answer from every participant. `runCommit` sends
  commits only in `COMMITTING`, and `runCancel` sends cancels only in `CANCELLING`. `drivePrepare` reads the state
  after its `CREATED → PREPARING` transition. `runPrepareRecovery` cancels a transaction that stays in `PREPARING`
  longer than `maxPreparingHoldMs` (`docs/agent-plans/2026-09-09-bounded-preparing-hold.md`).
- **Read transaction.** The item version once reset after a delete and a recreate, and an absent item returned zero
  in both phases. A create and a delete between the two phases then passed the comparison
  (`docs/agent-plans/2026-09-05-item-order-timestamps-and-read-revisions.md`, section 1.1). `max_deleted_v` now
  closes this gap (`docs/agent-plans/2026-10-03-max-deleted-version.md`).
- **Key promotion.** The design of `docs/agent-plans/2026-09-29-promotion-moves-its-locks.md` found three defects
  before the code shipped:
  1. `recoverStaleTransactions` deleted a `not_found` transaction when none of its keys was owned. The target could
     still need those rows.
  2. `commitLocal` compared the request with every pending row of the transaction, copies included. A commit of the
     retained keys then failed.
  3. The routed `txCommit` and `txCancel` deleted by transaction id, so they also deleted the copies.

The vitest suites check chosen orders. The property-based suites in `packages/fokosdb/test/property-based/` check
sampled orders of the real code. Neither can restart a Durable Object between two arbitrary blocks. Neither explores
the orders of in-flight messages across a cutover. A defect of this kind loses a decided write, returns a read that
no serial order gives, or keeps a lock for good. Nothing reports it at the time.

### 1.2 Why now

- The flows are new. The promotion moves its locks since 2026-09-29, and the read transaction compares
  `max_deleted_v` since 2026-10-03. The next runtime changes build on both.
- One model can hold the two public guarantees as monitors. Each later change to a flow can then check the same
  guarantees again.

### 1.3 The system in one page

The model follows these facts of the current code. Section 4.2 maps each fact to a part of the model.

#### 1.3.1 Write transactions

`FokosDB.transactWriteItems` (`packages/fokosdb/src/client/db.ts`) has two paths:

- **Single-partition path** (`#writeSingleShotFastPath`). When the client hint resolves every item to one partition
  and the caller gave no token, the client sends `txExecuteSingleShot`. The partition runs `executeSingleShot` in one
  storage transaction at its own clock, and takes no lock. A key with a lock gives `pending_conflict`. A partition
  that does not own every item answers `not_applicable`, and the client then uses the coordinator path. The client
  does not retry this call. A foreign error means that the outcome is unknown.
- **Coordinator path.** The client sends `initiateWrite` to the coordinator that owns the `clientRequestToken`. The
  client generates a token when the caller gave none. It retries `partition_migrating` with the same token until
  `partitionMigratingRetryDeadlineMs` (15 s by default).

The coordinator (`packages/fokosdb/src/server/do-transaction-coordinator.ts`):

- **States.** `CREATED → PREPARING → COMMITTING → COMMITTED`, or `CREATED → PREPARING → CANCELLING → CANCELLED`. Each
  transition writes to SQLite before the coordinator sends an RPC. `COMMITTING` is the point of no return.
- **Durable state.** `tc_state` holds the state, `transaction_ts`, `created_at`, `completed_at`, and
  `next_recovery_at`. `tc_participants` holds `prepare_outcome`, `commit_outcome`, `cancel_outcome`, and
  `error_json` for each participant. `tc_items` holds the operations and the participant of each one.
- **`initiateWriteLocal`.** A token with a stored row resumes that transaction (`resumeTransaction`). Otherwise the
  coordinator groups the operations by the root partition of each key, inserts the rows in `CREATED`, schedules
  `tx_recovery`, and calls `drivePrepare`.
- **`drivePrepare`.** It writes `PREPARING`, then reads the state again. When another drive already decided, it
  answers from the stored state. It sends `txPrepare` to each participant. The prepare retry (`prepareRetry`)
  retries every error except `partition_over_size` and a `FokosValidationError`. `storePrepareAnswer` keeps the
  first answer of each participant. `storePrepareError` keeps the first fatal error.
- **Decision.** When every answer in memory is accepted, the drive calls `markCommitting`. It writes `COMMITTING`
  only when the state is `PREPARING` and every stored answer is accepted. Otherwise `cancelTransactionInStore`
  writes `CANCELLING` when the state is `PREPARING`.
- **Fan-out.** `runCommit` sends `txCommit` (keys and `transactionTimestamp` only) when the stored state is
  `COMMITTING`. `runCancel` sends `txCancel` when the stored state is `CANCELLING`. A participant that does not
  confirm inside the budget keeps the transaction non-terminal for `tx_recovery`. `completeTransaction` writes the
  terminal state and `completed_at`.
- **Answer to the client** (`loadFinalResponse`): `committed` only in `COMMITTED`; the error
  `transaction_commit_pending` in `COMMITTING`; `cancelled` with results in `CANCELLING` and `CANCELLED`; the error
  `transaction_undecided` in `CREATED` and `PREPARING`.

The participant (`packages/fokosdb/src/shared/partition/transaction-participant.ts`):

- **`prepareLocal`.** A key with a lock of another transaction gives `pending_conflict`. A failed condition gives
  `condition_failed`. A timestamp that is not above the watermark gives `timestamp_conflict` (section 1.3.3). A
  timestamp more than `maxClockSkewMs` ahead of the partition clock gives `clock_skew`. Otherwise the lock block
  writes one `pending_tx_info` row and one lock row for each key. A repeated prepare of the same transaction finds
  its own lock and passes.
- **`commitLocal`.** It builds the owned set: a row whose key is in the request, or a row that the owner check
  accepts. With no owned row, it answers the idempotent success. Otherwise the request must match the owned set, or
  it throws `commit_keyset_mismatch`. It applies the `op_list` of each owned row in `opIndex` order, and deletes one
  row for each key, never by transaction id alone.
- **`cancelLocal`.** It deletes the lock rows of the owned keys that `dispatch` hands it. An empty key list releases
  nothing.

#### 1.3.2 Read transactions

`FokosDB.transactGetItems` has two paths:

- **Snapshot path** (`#readSnapshotFastPath`). When the client hint resolves every key to one partition, the client
  sends `txReadSnapshot`. The partition reads every key in one block. A key with a pending write gives `aborted`,
  and the client throws the pending-write error. `not_applicable` sends the client to the two-phase path. The client
  retries only a transient fault of the runtime.
- **Two-phase path** (`#readTransaction`). The client sends `txReadForTransaction` to each partition group, two
  times. Each phase retries every error up to `maxAttempts` (5 by default). The client aborts when an item has
  `hasPendingWrite` in either phase. It then compares the two phases by key (`KeyPairMap`): `found` must match, a
  found item compares `version`, and an absent item compares `maxDeletedV`. A difference gives `read_conflict`.

`readForTransactionLocal` writes nothing. A lock row of a `check` operation does not set `hasPendingWrite`
(`READ_ONLY_PENDING_OPERATIONS`).

#### 1.3.3 Versions and timestamps

- Every write increments the `v` of the item. A new row starts at `max_deleted_v + 1`, so the `v` of a key never
  repeats. `max_deleted_v` is the highest `v` of a row that a delete or a TTL expiry removed.
- Each item has `last_read_ts` and `last_write_ts`. A put and an update raise both with `MAX`. A committed `check`
  raises `last_read_ts` only.
- A prepared write must have a timestamp above `last_read_ts`. A prepared `check` must have a timestamp above
  `last_write_ts`. An operation on an absent key must have a timestamp above `max_delete_tx_order_ts`.
- A delete raises `max_delete_tx_order_ts` to at least the `last_read_ts` of the removed row. A new row starts its
  timestamps at least at `max_delete_tx_order_ts`.
- The coordinator stamps a transaction with its own clock (`txOrderTimestampNow`). The single-partition path and the
  single-item writes stamp with the partition clock, and `MAX` absorbs a stamp from a clock that lags.

#### 1.3.4 Single-item operations

- `apiPutItem` and `apiDeleteItem` refuse a key with any lock row with `item_locked_by_transaction`.
- `apiGetItem` reads the committed row. A lock does not block it.

#### 1.3.5 Recovery

- **Participant.** The `stale_tx_recovery` job (`recoverStaleTransactions` in
  `packages/fokosdb/src/server/do-partition.ts`) runs only when `canSweepLocally` is true: not behind the destroy
  fence, not on a router, and not on a target in `awaiting_data` or `importing`. It claims one transaction, skips it
  when every row is a copy, and calls `recoverTransactionForParticipant` on the coordinator. After the `await`, it
  reads the rows and their owners again, and applies the answer through `dispatch`. `COMMITTED` gives a commit,
  `CANCELLED` gives a cancel, and `driving` changes nothing. `not_found` gives a cancel when the lock is no older
  than `IDEMPOTENCY_WINDOW_MS`, and a quarantine (`guarded_at` on the `pending_tx_info` row) when it is older.
- **Coordinator.** `recoverTransactionLocal` answers from the ledger: `COMMITTED`, `CANCELLED`, `not_found` after
  the idempotency sweep, or `driving`. For `driving`, it makes the transaction and the `tx_recovery` job due now.
  The `tx_recovery` job claims due transactions and drives each one from its stored state. The `idempotency_sweep`
  job deletes a completed transaction `IDEMPOTENCY_WINDOW_MS` after `completed_at`.
- **Operator.** `debugForceResolveTransaction` sends every row of the transaction through `dispatch` and changes
  owned rows only.
- **Time values.** `staleTransactionMs` is 5000 ms by default. `maxPreparingHoldMs` is 5 × `staleTransactionMs`,
  capped at `IDEMPOTENCY_WINDOW_MS` (10 minutes). Thus `staleTransactionMs < maxPreparingHoldMs ≤
  IDEMPOTENCY_WINDOW_MS` for every valid configuration.

#### 1.3.6 Hash split

`docs/agent-plans/2026-09-17-unified-repartition-flow.md` (section 4.1.2) gives the flow:

1. The source queues the split. `#plan` writes one target row for each of the `hashSplitN` children.
2. `#initializeTargets` marks each target `initializing`, then calls `fokosInit`. The child writes its identity and
   an import record in `awaiting_data`.
3. When every target is `initialized`, `#cutover` writes `cutover`. From here the source is a router and owns no key.
4. `fokosStartImport` tells each child to start. Each child also starts from its own alarm.
5. Each child pulls one page at a time with `fokosMigrationPull`: the `overrides` phase, then the `items` stream,
   then the `pending_tx` stream. Every page of the `pending_tx` stream carries the deletion metadata, and the child
   merges it with `mergeDeletionMetadata` (`MAX`). The child inserts item rows with their `v` and their timestamps.
   Each page commits with its cursor. The last page writes `imported`.
6. Each child retries `fokosMigrationAck`. When every child acknowledged, the source writes `completed`.
7. `cleanupSourceStep` deletes the lock copies (`deletePendingTxCopiesBatch`) and writes `cleaned`. A router keeps
   its item rows for life and never serves them.

While a child is in `awaiting_data` or `importing`, `apiGetItem` reads through the source with `fokosExecuteLocal`,
and every other item and transaction operation answers `partition_migrating`.

#### 1.3.7 Key promotion

A key promotion moves one hash key, with all its sort keys and its locks, from a hash partition into a new range
root. The source keeps a copy of each moved lock until the target acknowledges its import.

**Source states** (`packages/fokosdb/src/sharding/repartition-flow.ts`): `queued → planned → cutover → completed →
cleaned`. **Target states**: `awaiting_data → importing → imported → active`.

**Steps:**

1. The source queues the promotion of hash key `K` and plans one target, the range root `R`.
2. The source marks `R` `initializing`, then calls `fokosInit`. `R` writes its identity and an import record in
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

**Transactions.** The coordinator groups the keys by root partition, so the source is the participant for both `K`
and its retained keys `U`. After cutover, `dispatch` divides each `group` request by owner: the local part runs in
the same block, and the moved part goes to `R`. `txPrepare` is `fail_fast`. `txCommit` and `txCancel` are
`attempt_all`: they run every part and throw `partition_fanout_failed` after the local part applied.

### 1.4 Glossary

- **Block** — the synchronous code between two `await`s of one Durable Object.
- **Durable state** — the SQLite state of a machine. A restart keeps it.
- **Memory state** — every other field of a machine: in-flight continuations, timers, caches. A restart clears it.
- **Drive** — one run of `drivePrepare`, `runPrepareRecovery`, `runCommit`, or `runCancel` for one transaction, from
  one caller.
- **Writer** — a committed write transaction, a committed single-partition transaction, or a single-item write.
- **Ghost state** — state that only a monitor keeps, for example the order in which writers applied on each key.
- **Seeded defect** — a deliberate defect behind a flag of the model. The checker must find a violation for it.
- **Environment** — the machine that injects faults and moves time.
- The terms of the promotion RFC also apply: **source**, **target**, **owned row**, **transfer copy**, **completion
  transaction**, **importing target**, **imported target**.

## 2. Goals and requirements

### 2.1 In scope

- One P project that models the client, the coordinator, and the partitions of one table.
- Each event handler of a Durable Object machine is one block of the code, and the model names the code function of
  each handler.
- The public guarantees of `transactWriteItems`, `transactGetItems`, `getItem`, `putItem`, and `deleteItem` are
  monitors. The monitors stay the same from one milestone to the next.
- Each milestone adds one environment: faults, timestamps, read transactions, a hash split, a key promotion. The
  monitors of the earlier milestones must hold in the new environment.
- Faults: a restart of any Durable Object between any two blocks, a lost request or answer, any delivery order of
  in-flight calls, and coarse time steps.
- Each seeded defect of section 4.2.12 makes the checker report a violation of the monitor that the table names.
- The liveness monitors hold when the faults stop.
- Each real defect that the model finds gets a vitest regression test that runs the real code in workerd.

### 2.2 Out of scope

- **Range splits, the Bloom filter, and learned routes.** They add more targets and a routing cache. They can extend
  the model after milestone 6.
- **The internals of the sharding runtime.** The model keeps the ownership rules and the repartition states that a
  FokosDB guarantee depends on (section 4.2.9). It does not model the route caches, the scheduler, or `walk`.
- **`queryItems` and the range walk.** The model reads with `getItem` and `transactGetItems` only.
- **The TTL sweep, size admission, and the split decision.** The environment requests a split or a promotion
  directly.
- **`executionMode: "ordered_per_item"`, update expressions, projections, and result payloads.** Each operation has
  at most one write for each item. A condition is `exists` or `not_exists` (section 4.2.3).
- **PObserve.** A log-based check of production runs needs a log parser for the TypeScript code. It can come later.

### 2.3 Requirements

- The work must not change production code and must not add a production hook.
- The model must use the names of the code: states, operations, error codes, and function names.
- The tool version must be pinned.
- `pnpm check` must stay green.
- The P output directories `PGenerated/` and `PCheckerOutput/` must go into `.gitignore` and `.prettierignore`.
- The npm package publishes only `dist`, so `formal/` must stay out of the package.

## 3. Milestones

Each milestone ends when its test cases pass, its seeded defects fail with their monitors, and the test cases of
every earlier milestone still pass. Each milestone delivers a design check on its own. Section 4.2 holds the rules
that each milestone builds.

| # | Scope | P concepts to learn | New monitors | Seeded defects | Status |
| --- | --- | --- | --- | --- | --- |
| M0 | Toolchain, one partition, single-item operations | machine, event, `send`, test case, trace replay | `VersionIncreases` | V1 | Done |
| M1 | `transactWriteItems` on a fixed topology, one drive, no faults | spec monitor, `announce` | `Atomicity`, `AnswerMatchesDecision`, `LockExclusion`, `ReadAfterCommit` | W1–W3 | Done |
| M2 | Faults, retries, recovery, concurrent drives | `$`, `choose`, failure injection, hot states | `SingleApply`, `LocksResolve`, `ClientAnswered`, `TransactionsComplete` | W4–W7 | In review |
| M3 | Clocks, timestamps, the single-partition path | ghost state | `WriteSerializable` | W8–W10 | Not started |
| M4 | `transactGetItems`: the snapshot path and the two-phase path | a monitor that searches serial orders | `ReadSerializable` | R1–R3 | Not started |
| M5 | Hash split under all of the above | `new` at run time | `Authority`, `CopiesUntouched`, `Retention`, `SplitCompletes` | S1–S5 | Not started |
| M6 | Key promotion with lock copies | — | `PromotionCompletes` | P1–P7 | Not started |
| M7 | Coordinator split (optional) | — | — | C1 | Not started |

### 3.1 M0 — Toolchain and one partition (done)

- **Scope.** The project layout and commands of section 4.2.1. One `Client` and one `Partition` with `apiPutItem`,
  `apiDeleteItem`, and `apiGetItem`. The `formal:p` scripts and the `AGENTS.md` entry of section 4.2.15.
- **Test cases.** `tcItems`, `tcBugNewRowVersionFromOne`.
- **Delivers.** A working toolchain, and a first replay of a failing trace.
- **Result.** Done. `tcItems` passes and `tcBugNewRowVersionFromOne` fails with `VersionIncreases`, in the random
  checker and in PEx. Section 4.2.1 gives the toolchain and the commands, and section 4.2.13 gives the measurements.

### 3.2 M1 — Write transactions, happy path (done)

- **Scope.** Two partitions, one coordinator with one drive, two clients. The coordinator path of section 4.2.7
  without recovery. `prepareLocal` with `pending_conflict` and `condition_failed`, `commitLocal`, `cancelLocal`, and
  the lock check of the single-item writes. Each call goes through an `RpcCall` machine with no fault (section
  4.2.2). No fault and no timestamp.
- **Test cases.** `tcWriteHappy`, `tcWriteConflict`, `tcBugCommitOnOneAccept`, `tcBugPutIgnoresLock`,
  `tcBugCommittedBeforeApply`.
- **Delivers.** A check of atomicity and of the client answer on a fixed topology.
- **Result.** Done. `tcWriteHappy` and `tcWriteConflict` pass in the random checker and in PEx. W1, W2, and W3 fail with
  their monitors. Section 4.2.13 gives the measurements. The model makes these decisions:
  - A storage call does not end a block. While a storage call runs, the input gate of the Durable Object holds every
    other event. Thus `initiateWriteLocal` and `drivePrepare` up to the prepare fan-out are one handler, although the
    code awaits `scheduleJob` between them. Only an RPC ends a block.
  - The `invariant` of `runCommit` (each participant has a stored accepted answer) is not in the model. With W1 the
    code throws there and sends no commit. `Atomicity` checks the same rule, so W1 shows that the monitor finds a
    commit decision with a rejected participant.
  - `tcBugCommittedBeforeApply` does not run `AnswerMatchesDecision`. W3 answers `committed` in `COMMITTING`, and
    that monitor fails at the answer, before the read. Without it, the test case shows that `ReadAfterCommit` finds
    the stale read.
  - The model has no `resumeTransaction`. A token that comes again is an assertion of the coordinator.

### 3.3 M2 — Faults and recovery

- **Scope.** The faults of the `RpcCall` machine, restarts, lost calls, and the clock (section 4.2.10). A
  retry with the same token. The `tx_recovery` job, `recoverTransactionLocal`, the `idempotency_sweep` job,
  `runPrepareRecovery` with `maxPreparingHoldMs`, the participant `stale_tx_recovery` job, the quarantine, and
  `debugForceResolveTransaction`. Concurrent drives of one transaction: the request, a retry, and `tx_recovery`.
  The prepare watermark of section 1.3.3 for puts.
- **Test cases.** `tcWriteFaults`, `tcWriteRetry`, `tcConcurrentDrives`, `tcStale`, `tcRepair`, `tcHold`,
  `tcBugTokenRowIgnored`, `tcBugStaleCancelsOnDriving`, `tcBugCancelInAnyState`, `tcBugNoPreparingHold`.
- **Delivers.** A check that a fault never breaks atomicity, and that every lock and every transaction ends when the
  faults stop.
- **Result.** Every test case gives its expected result. Section 4.2.13 gives the measurements. The model makes
  these decisions:
  - **Clock.** The `Environment` machine is also the wall clock (section 4.2.3). Each Durable Object stores
    `created_at`, `next_recovery_at`, and `completed_at` as ticks, and `nextRecoveryAt` follows the code. The values
    are `staleTransactionMs` = 1 tick, `maxPreparingHoldMs` = 2, `IDEMPOTENCY_WINDOW_MS` = 3, and
    `STALE_RECOVERY_MAX_DELAY_MS` = 2. Per-item age classes are not used, because they can make a lock look younger
    than its transaction. M3 adds an offset for each Durable Object within `maxClockSkewMs`.
  - **Faults.** The `Environment` owns one restart budget and one loss budget for the run. Each `RpcCall` asks it
    whether to lose the request or the answer. A lost answer gives the caller the error at once, and the request
    reaches the target in a later handler of the call, so later calls of the caller can reach the target first. A
    restart clears the memory state, keeps SQLite and the alarm, and gives each request that waits for the
    coordinator an error. The restarts can come only in the first 20 steps of the `Environment`.
  - **The watermark starts in M2.** Without it, a prepare retry that arrives after the commit locks the key again,
    and recovery applies the write a second time: a false `SingleApply` violation. In the code only the watermark
    stops this. The coordinator stamps each transaction with its id, which only goes up. A single-item write stamps
    0, which `MAX` absorbs. M3 adds the clocks, `clock_skew`, `check`, deletes, and W8 to W10.
  - **`SingleApply` holds inside the idempotency window.** A token that comes after the sweep starts a new
    transaction, as in the code. The monitor forgets a token when the sweep deletes it (`eTokenSwept`).
  - **A drive whose transaction the sweep deleted.** `loadFinalResponse` finds no row and throws, so the request
    gets an error.
  - **`tcHold` has 1 restart.** With only "`B` drops every call", `drivePrepare` cancels on the failed prepare, and
    W7 cannot fail. A coordinator restart during the prepare leaves the transaction in `PREPARING` for
    `runPrepareRecovery`.
  - **`tcStale` and `tcRepair` use the two clients of `tcWriteConflict`.** A lock stays after its transaction
    completed only when a prepare reaches a partition after the cancel. A late prepare after a commit gets
    `timestamp_conflict`. With T1 alone, T1 is never cancelled, so neither test case can reach the quarantine or the
    repair. Both test cases have the blackout and 2 lost calls. M3 adds `clock_skew`, which can cancel T1 alone. M6
    adds lock copies, which outlive their transaction, but the stale job skips a transaction whose rows are all
    copies. Neither makes the quarantine of an owned lock easier.
  - **`tcWriteRetry`** retries T1 up to 2 times after an error, `transaction_commit_pending`, or
    `transaction_undecided`, and then sends the token one more time after the final answer.
  - **`tcConcurrentDrives`** has 1 restart and 1 lost answer, and client 1 retries T1 up to 2 times.
  - **Search strategy.** `tcStale`, `tcRepair`, `tcBugPutIgnoresLock`, and `tcBugCancelInAnyState` run with
    `--sch-fairpct 10` (`TEST_STRATEGY` in `check.sh`). Their orders need many unlikely steps in a row: in 3000
    random schedules, the quarantine and the W6 order did not come. The unfair `--sch-pct` reports false liveness violations, because it
    can hold a machine back for longer than the clock runs. Use `--sch-fairpct` with the liveness monitors.
  - **Not in the model.** A fatal prepare error (`FokosValidationError`, `FokosExpressionError`),
    `partition_over_size`, the time budgets of a step, `recoveryConcurrentDrives`, and `staleLockScanRows`. The step
    of `tx_recovery` claims every due transaction, and a test case has at most two.

### 3.4 M3 — Timestamps and the single-partition path

- **Scope.** A clock for each Durable Object with bounded skew. The timestamp rules of section 1.3.3, `clock_skew`,
  `check` operations, deletes and recreates, and `txExecuteSingleShot`.
- **Test cases.** `tcClocks`, `tcSingleShot`, the test cases of W8 to W10.
- **Delivers.** A check that the timestamp order is a serial order of the committed two-phase transactions.

### 3.5 M4 — Read transactions

- **Scope.** `txReadSnapshot`, the two-phase path, `hasPendingWrite`, the `version` comparison, and the `maxDeletedV`
  comparison. The `getItem` part of `ReadAfterCommit` extends to `transactGetItems`.
- **Test cases.** `tcReadHappy`, `tcReadVsWrites`, `tcReadFaults`, `tcSnapshot`, the test cases of R1 to R3, and
  `tcReadVsWrites` with V1.
- **Delivers.** A check that a read transaction returns a state of some serial order of the committed writes.

### 3.6 M5 — Hash split

- **Scope.** A hash split of one root into two children while write and read transactions run, with the faults of
  M2. The router, the import gate, the read-through, the group dispatch, the migration streams, the merge of the
  deletion metadata, and the lock copies of the router (section 4.2.9).
- **Test cases.** `tcSplitWrites`, `tcSplitReads`, `tcSplitFaults`, the test cases of S1 to S5.
- **Delivers.** A check that every earlier guarantee holds across a split.

### 3.7 M6 — Key promotion

- **Scope.** One promotion of one hash key while 2 or 3 transactions run, with the faults of M2. The transfer copies,
  the retention until the acknowledgement, the cleanup, the stale job that skips copies, and the repair.
- **Test cases.** `tcPromotionHappy`, `tcPromotionFaults`, `tcPromotionStale`, `tcPromotionRepair`,
  `tcPromotionReads`, the test cases of P1 to P7.
- **Delivers.** A check that every earlier guarantee holds across a promotion, and that the copies stay intact.

### 3.8 M7 — Coordinator split (optional)

- **Scope.** A hash split of the coordinator shard group `fokos.tc.<tableName>`. Every durable transition runs
  `fokos.owns(token)` inside its `transactionSync`, and after the cutover it writes nothing and throws
  `partition_migrating`. The client retries with the same token until the child resumes the transaction.
- **Test cases.** `tcCoordinatorSplit`, the test case of C1.
- **Delivers.** A check that a token never drives two transactions across a coordinator split.
- `TODO: the author decides whether M7 is in scope, and supplies the coordinator migration facts for it.`

## 4. Proposed solution

### 4.1 High-level overview

P describes a system as state machines that send events to each other. A machine handles one event at a time, to the
end, before it takes the next one. A Durable Object works the same way between two `await`s. Thus one P event handler
is one block of a Durable Object, and an `await` is a send followed by a later event.

The model has two layers:

1. **Monitors are the specification.** Each public guarantee of FokosDB is one P spec monitor: atomicity, the
   client answer, lock exclusion, read-after-commit, serializable writes, serializable reads, and the end of every
   lock. The monitors observe events that the machines announce, and do not change when the topology changes.
2. **Environments are the tests.** Each milestone adds one harder environment: faults, clocks, read transactions, a
   hash split, a key promotion. The monitors of every earlier milestone must hold in it.

```text
 Client(s) --transactWriteItems--> Coordinator --txPrepare / txCommit / txCancel--+
    |      --transactGetItems / getItem / putItem / txExecuteSingleShot ------+    |
    |                                                                         v    v
    |                       +---------------- Root partition A ----------------------+
    |                       | items, locks, pending_tx_info, deletion metadata,      |
    |                       | repartition row (source side), import record (target)  |
    |                       +---------------------------------------------------------+
    |                          | M5: fokosInit, pages, ack   | M6: fokosInit, pages, ack
    |                          v                             v
    |                       Children A0, A1               Range root R
    |
    +--> Root partition B (same machine type)

 Environment: restart any DO, lose a call, move time, request a split or a promotion
 Operator:    debugForceResolveTransaction
 Monitors:    observe the announced events of every machine
```

The P checker runs each test case many times. In each run it chooses the order of the events, the fault points, and
each nondeterministic choice. A violation gives a trace that the checker can replay.

Each known defect is a flag of the model. The checker must find a violation when the flag is on. When it finds none,
the model is too coarse at that point, and the milestone is not complete.

### 4.2 Technical details

#### 4.2.1 Layout and tools

```text
packages/fokosdb/formal/p/
  FokosDB.pproj, Dockerfile, check.sh
  PSrc/   Types.p, Client.p, Coordinator.p, Partition.p, Participant.p, Topology.p, RpcCall.p,
          Environment.p, Operator.p
  PSpec/  Atomicity.p, Answers.p, Locks.p, Versions.p, WriteSerializable.p, ReadSerializable.p,
          Authority.p, Copies.p, Liveness.p
  PTst/   TestDriver.p, TestCases.p
```

- The checks run in Docker. The P project publishes no public image (`ghcr.io/p-org/p` refuses an anonymous pull),
  so `Dockerfile` builds the local image `fokosdb-p:3.1.0`: `mcr.microsoft.com/dotnet/sdk:8.0`, OpenJDK 17, Maven,
  and the NuGet tool `P` at version 3.1.0. `check.sh` pins the version and builds the image when it is missing. Only
  Docker is necessary on the machine.
- `pnpm formal:p` compiles the model and runs every test case. `pnpm formal:p <test case>` runs one. `SCHEDULES`
  sets the number of schedules of each test case (default 1000). The script fails when a `tcBug<Defect>` test case does
  not fail with the monitor of section 4.2.12, when another test case finds a bug, or when a name runs more than one
  test case: `p check -tc` runs every test case whose name starts with the given name.
- The random checker stops at the first bug. For each test case, the output gives the number of schedules and of
  timelines that the checker explored. A timeline count far below the schedule count shows that more schedules add
  little.
- `pnpm formal:p [--pex] [test case...] -- <p check option>...` gives each option after `--` to every `p check`
  call, for example `-- --sch-pct 3 --seed 42` for the PCT strategy with a fixed seed. The script refuses an option
  that it sets itself: `-tc`, `-o`, `--mode`, `--replay`, `-s`, and `-t`.
- The output of a test case goes to `PCheckerOutput/<test case>/`. `check.sh --replay <test case> <schedule file>`
  replays a bug.
- A small test case can also run in the exhaustive checker PEx: `pnpm formal:p --pex [test case...]`.
  `PEX_TIMEOUT` sets the time limit of each test case in seconds (default 60). A test case passes only when PEx
  explores every state (`correct for any depth`), and a `tcBug<Defect>` test case only when PEx finds the violation of
  its monitor. A run that stops at the time limit before it explores every state gives `INCOMPLETE`. The output goes to
  `PCheckerOutput/pex/<test case>/`.
- The PEx compile builds the model with Maven. The Maven repository (46 MB) stays in the ignored folder
  `packages/fokosdb/formal/p/.p-cache/`. The first PEx compile downloads it in about 40 s, and a later compile takes
  about 6 s.
- Each milestone records the run counts and the run times of its test cases in section 4.2.13.

#### 4.2.2 Semantics of the mapping

- **One handler, one block.** A handler must not contain more than one block of the code. A code path with an
  `await` becomes two handlers: the first sends the call and stores a continuation in memory state, and the second
  handles the answer.
- **Delivery order.** P delivers the events from one sender to one receiver in order. Workers RPC gives no such
  order, and a call can get lost. Each call therefore goes through a new `RpcCall` machine. It delivers the request
  and returns the answer to the caller. Independent `RpcCall` machines give every delivery order. From M2, it can also
  lose the request or the answer within the fault budget, and then returns an error to the caller. M1 needs the
  machine too: a P `send` puts the event in the inbox of the receiver at once, so with direct sends a commit that the
  coordinator sends before its answer always reaches the partition before a read that the client sends after the
  answer, and the prepares of two transactions reach each partition in the same order.
- **Durable and memory state.** Each Durable Object machine keeps its durable state in one record and its memory
  state in another. A restart event clears the memory state, drops the continuations, and keeps the durable record.
  An answer that arrives for a dropped continuation is ignored, as a lost promise in the code.
- **Retry.** A retry loop of `FokosShardingClient` is a bounded count in the continuation. Each try is a new
  `RpcCall`.

#### 4.2.3 Abstractions

- **Keys.** Each key is a pair of a hash key and a sort key, written `a1` for hash key `a` and sort key `1`. A
  static map gives the root partition of each hash key. The model names each partition with its role: `A`, `B`,
  `A0`, `A1`, `R`.
- **Values.** Each write writes a unique value id: the transaction id and the `opIndex`, or the id of the
  single-item write. A delete writes `absent`. Thus a monitor can tell which writer produced each value that a read
  returns.
- **Operations.** `put`, `delete`, and `check`. A condition is `exists`, `not_exists`, or none.
- **Versions.** `v` and `max_deleted_v` are integers, as in the code.
- **Time (M2).** One wall clock in integer ticks. The `Environment` sends each tick to every Durable Object with
  `eTick`. Each inbox is in send order, so a message that a Durable Object sends after it read tick `t` reaches each
  other Durable Object after tick `t`, and an age never looks smaller than in real time. The clock ticks only while
  some Durable Object has an alarm deadline (`eArmed`), and only while no call is open (`eRpcOpened`,
  `eRpcClosed`): a call takes milliseconds, and a tick is about `staleTransactionMs`. It stops 20 ticks after the end
  of the faults and the last change of an alarm, so a run ends.
- **Timestamps (M3).** Each Durable Object adds its own offset to the wall clock. The environment moves each offset,
  and keeps every pair of clocks within the skew bound. `maxClockSkewMs` is an integer of the same unit.
- **Page size.** One row for each migration page. This gives the most interleavings between the `items` stream, the
  `pending_tx` stream, and the transaction operations.

#### 4.2.4 Machines

| Machine | From | Durable state | Memory state | Code |
| --- | --- | --- | --- | --- |
| `Partition` | M0 | identity, `items` (`v`, value, `last_read_ts`, `last_write_ts`), lock rows, `pending_tx_info` (`created_at` class, `guarded_at`), deletion metadata, repartition row and target rows (source side), route override of a promoted key, import record and cursor (target side) | continuations of forwarded calls, the stale job in progress | `PartitionDO`, `TransactionParticipant`, `PartitionStore`, `FokosShardingRuntime`, `RepartitionSource`, `RepartitionTarget`, `FokosMigrationHost` |
| `Client` | M0 | — | the calls in progress and their retries | `FokosDB` |
| `Coordinator` | M1 | `tc_state`, `tc_participants`, `tc_items` (participant and operation only) | the drives in progress | `TransactionCoordinatorDO` |
| `RpcCall` | M1 | — | one request and its answer | Workers RPC, `FokosShardingClient` retries |
| `Environment` | M2 | — | the fault budget, the wall clock, the alarms that have a deadline, the open calls | eviction, crashes, the wall clock |
| `Operator` | M2 | — | the transactions it repaired | an operator who reads the lock-age guard error and the completion log, and calls `debugForceResolveTransaction` |

One `Partition` machine type serves every role, as one `PartitionDO` class does in the code. A source creates a
child or a range root with `new` on the first `fokosInit`. A later `fokosInit` to the same target reaches the same
machine.

#### 4.2.5 Events

| Event | From → to | From | Handler models |
| --- | --- | --- | --- |
| `ePutItem`, `eDeleteItem`, `eGetItem` and answers | Client ↔ Partition | M0 | `apiPutItem`, `apiDeleteItem`, `apiGetItem` |
| `eInitiateWrite` / `eInitiateWriteResp` | Client ↔ Coordinator | M1 | `initiateWriteLocal`, `resumeTransaction`, `loadFinalResponse` |
| `eTxPrepare`, `eTxCommit`, `eTxCancel` and answers | Coordinator ↔ Partition, Partition ↔ Partition | M1 | `drivePrepare`, `runCommit`, `runCancel`, `dispatch` of a `group` operation, `prepareLocal`, `commitLocal`, `cancelLocal` |
| `eAlarm` on the coordinator | Coordinator → itself (alarm) | M2 | `sweepExpiredTransactions`, then `recoverStaleTransactions` of the coordinator and `driveTransaction` |
| `eAlarm` on a partition | Partition → itself (alarm) | M2 | `recoverStaleTransactions` of the partition |
| `eGuardLogged`, `eLogLookup` / `eLogEntry` | Partition → Operator, Operator ↔ Coordinator | M2 | the lock-age guard error, and the completion log that an operator reads |
| `eRecover` / `eRecoverResp` | Partition ↔ Coordinator | M2 | `recoverTransactionForParticipant`, `recoverTransactionLocal` |
| `eForceResolve` / `eForceResolveResp` | Operator ↔ Partition | M2 | `debugForceResolveTransaction` |
| `eRestart` | Environment → any Durable Object | M2 | eviction or crash |
| `eTick` | Environment → every Durable Object | M2 | the wall clock; from M3 each Durable Object adds its offset |
| `eArmed`, `eAlarm` | Durable Object → Environment, Durable Object → itself | M2 | the alarm of a Durable Object and the `deadline()` of its jobs |
| `eMayLose` / `eLossDecision`, `eRpcOpened`, `eRpcClosed` | RpcCall ↔ Environment | M2 | a lost call, and the calls that are open |
| `eRpcFailed`, `eRpcBroken` | RpcCall → caller, target → RpcCall | M2 | a failed call, and a target that restarted or refused the call |
| `eExecuteSingleShot` / answer | Client ↔ Partition | M3 | `txExecuteSingleShot`, `executeSingleShot` |
| `eReadSnapshot` / answer | Client ↔ Partition | M4 | `txReadSnapshot` |
| `eReadForTransaction` / answer | Client ↔ Partition, Partition ↔ Partition | M4 | `txReadForTransaction`, `readForTransactionLocal` |
| `eRequestSplit`, `eRequestPromotion` | Environment → Partition | M5, M6 | `RepartitionSource.queue`, `requestPromotion` |
| `eSourceStep` | Partition → itself (alarm) | M5 | `sourceStep`: plan, initialize, cutover, notify, cleanup step |
| `eFokosInit` / `eFokosInitResp` | Partition ↔ Partition | M5 | `fokosInit`, `#initializeTargets` |
| `eStartImport` | Partition → Partition | M5 | `fokosStartImport` |
| `eImportStep` | Partition → itself (alarm) | M5 | `importOnePage` |
| `eMigrationPull` / `eMigrationPage` | Partition ↔ Partition | M5 | `servePage`, `buildPage` and `applyPage` of `FokosMigrationHost` |
| `eMigrationAck` / `eMigrationAckResp` | Partition ↔ Partition | M5 | `acceptAck`, `beforeComplete` |
| `eExecuteLocal` / answer | Partition ↔ Partition | M5 | the read-through with `fokosExecuteLocal` |

Each machine also announces the monitor events of section 4.2.11.

#### 4.2.6 Client rules

1. **`transactWriteItems`.** When the hint resolves every item to one partition and no token is set, the client
   sends `txExecuteSingleShot` (M3). It follows `not_applicable` with the coordinator path, and does not retry. On
   the coordinator path, it sends `initiateWrite` with a token and retries `partition_migrating` with the same token.
2. **The caller.** The test driver acts as the caller. After a lost answer, `transaction_commit_pending`, or
   `transaction_undecided`, it can retry with the same token. It always sets a token on such a retry, because a
   retry without one starts a new transaction by design.
3. **`transactGetItems`** (M4). The snapshot path and the two-phase path as in section 1.3.2, with the retry counts
   of the code.
4. **`getItem`, `putItem`, `deleteItem`.** One call to the root partition of the key.

#### 4.2.7 Coordinator rules

These rules repeat the code, so a reviewer can compare them one by one:

1. **Insert.** `initiateWriteLocal` as in section 1.3.1. A token with a stored row resumes it. A different operation
   set for a stored token gives `idempotent_parameter_mismatch`.
2. **Prepare.** `drivePrepare` writes `PREPARING`, reads the state again, and sends one `txPrepare` for each root
   partition. `storePrepareAnswer` keeps the first answer of each participant, and only in `PREPARING`.
   `storePrepareError` keeps the first fatal error, and a transient error can replace a transient error.
3. **Decide.** `markCommitting` writes `COMMITTING` only from `PREPARING` and only when every stored answer is
   accepted. `cancelTransactionInStore` writes `CANCELLING` only from `PREPARING`.
4. **Fan out.** `runCommit` sends only in `COMMITTING`, and only to the participants with no stored commit outcome.
   `runCancel` sends only in `CANCELLING`. `completeTransaction` writes the terminal state and `completed_at` only
   from `COMMITTING` or `CANCELLING`.
5. **Answer.** `loadFinalResponse` as in section 1.3.1.
6. **Recover** (M2). `runPrepareRecovery` re-prepares the participants with no stored answer. It cancels on a stored
   fatal error, on a rejection, or after `OverHold`. Otherwise it leaves the transaction in `PREPARING`.
   `tx_recovery` drives each due transaction from its stored state. `recoverTransactionLocal` answers as in section
   1.3.5. `idempotency_sweep` deletes a completed transaction after `OverWindow`.
7. **Concurrent drives** (M2). The request drive, a retry drive, and a `tx_recovery` drive can run for one
   transaction. Each drive is a continuation in memory state, so the drives interleave at their `await`s.

M1 has one drive and no recovery. Rules 6 and 7 start in M2. This model checks every rule of this section, and no
other model checks them (section 5).

#### 4.2.8 Partition rules

1. **Prepare.** As `prepareLocal` (section 1.3.1). M1 checks locks and conditions. M3 adds the timestamp rules and
   `clock_skew`.
2. **Commit.** As `commitLocal`. One row for each key applies and is deleted.
3. **Cancel.** As `cancelLocal`. Only the owned keys of the request.
4. **Single-item writes.** A key with any lock row gives `item_locked_by_transaction`. A new row starts at
   `max_deleted_v + 1`. A delete raises `max_deleted_v` and `max_delete_tx_order_ts`.
5. **Single-partition transaction** (M3). As `executeSingleShot`, at the partition clock.
6. **Reads** (M4). As `readForTransactionLocal`. `txReadSnapshot` answers `aborted` for a pending write.
7. **Stale job** (M2). As `recoverStaleTransactions` (section 1.3.5).
8. **Repair** (M2). As `debugForceResolveTransaction`. Every row goes through the group dispatch of section 4.2.9.

#### 4.2.9 Topology rules

These rules start in M5. Before M5, each root partition owns every key of its hash keys.

1. **Owner.** A split source in `cutover`, `completed`, or `cleaned` owns no key. A child owns the keys of its
   slice. On a promotion source, `owns(k)` is false for a key of `K` when the override is `cutover`, `completed`, or
   `cleaned`, and true for every other key. `R` owns every key of `K`.
2. **Import gate.** A target in `awaiting_data` or `importing` answers `partition_migrating` for `txPrepare`,
   `txCommit`, `txCancel`, `txReadForTransaction`, `txReadSnapshot`, `txExecuteSingleShot`, `apiPutItem`,
   `apiDeleteItem`, and `debugForceResolveTransaction`. It reads through the source for `apiGetItem`. An imported
   target serves all of them.
3. **Group dispatch.** A partition divides the keys of a `group` request by owner in the handler that receives the
   request. The local part runs in that handler. Each remote part goes to its owner in its own `RpcCall`. `txPrepare`
   and `txReadForTransaction` are `fail_fast`. `txCommit` and `txCancel` answer `partition_fanout_failed` when a
   remote part failed, after the local part applied.
4. **Single owner.** `txReadSnapshot` and `txExecuteSingleShot` answer `not_applicable` when one owner does not hold
   every key of the request.
5. **Migration pages.** The `items` stream copies the item rows of the slice with their `v` and timestamps. The
   `pending_tx` stream copies the lock rows of the slice with their `pending_tx_info` fields, `guarded_at` included.
   Every `pending_tx` page carries the deletion metadata, and the target merges it with `MAX`. A page whose cursor
   does not match is dropped.
6. **Copies.** A split router and a promotion source keep their lock rows of the moved keys as transfer copies. No
   local decision applies to a copy. The cleanup steps after `completed` delete the copies, one whole transaction at
   a time. A promotion source then deletes the item copies of `K`. A split router keeps its item rows and never
   serves them.
7. **Stale job.** It runs only when `canSweepLocally` is true, and it skips a transaction whose rows are all copies.

#### 4.2.10 Environment and faults

- **Restart.** A restart of any Durable Object between any two blocks, within the restart budget of the test case.
- **Lost call.** An `RpcCall` can drop the request or the answer, within the loss budget. The caller sees an error.
  After a lost answer, the request still reaches the target, before or after the later calls of the caller.
- **Time.** `eTick` moves the wall clock forward (section 4.2.3). M3 also moves the offset of one Durable Object.
- **End of faults.** After its budget, the environment stops all faults. The liveness monitors apply from that
  point.
- **Topology** (M5, M6). The environment requests the split or the promotion at a nondeterministic point. Thus a run
  covers a prepare before the cutover, during target initialization, and after the cutover, and a commit or a cancel
  during the import.

#### 4.2.11 Monitors

**Safety monitors:**

| Monitor | From | Rule | Events it observes |
| --- | --- | --- | --- |
| `VersionIncreases` | M0 | On each key, over all partitions, each new value has a `v` above every earlier `v` of the key, also after a delete and a recreate. | `eItemWritten` |
| `Atomicity` | M1 | No key of a transaction applies when the decision is cancel. No owned lock of a transaction is released by a cancel when the decision is commit. When a transaction reaches `COMMITTED`, every key of it has applied. | `eDecision`, `eApplied`, `eReleased`, `eTxCompleted` |
| `AnswerMatchesDecision` | M1 | The client gets `committed` only for a transaction in `COMMITTED`, and `cancelled` only for a transaction with the decision cancel. | `eDecision`, `eTxCompleted`, `eClientAnswer` |
| `LockExclusion` | M1 | While a transaction holds the lock of a key, no other writer changes the key. | `eLockWritten`, `eLockDeleted`, `eItemWritten` |
| `ReadAfterCommit` | M1 | A read that starts after the client got `committed` returns, for each key of that transaction, the value of that transaction or of a later writer. M4 extends it from `getItem` to `transactGetItems`. | `eClientAnswer`, `eReadStart`, `eReadResult` |
| `SingleApply` | M2 | For each `clientRequestToken`, each key of its operations applies at most once, over all partitions, inside the idempotency window of the token. | `eDecision`, `eApplied`, `eTokenSwept` |
| `WriteSerializable` | M3 | On each key, a committed write of a two-phase transaction has a timestamp above the stamp of every earlier committed read or write of the key. A committed `check` has a timestamp above the stamp of every earlier committed write. A single-item write and a single-partition transaction add their stamps, but the rule does not apply to them, because `MAX` absorbs a clock that lags. Thus the timestamp order is a serial order of the two-phase transactions. | `eApplied`, `eItemWritten` |
| `ReadSerializable` | M4 | The result of a read transaction equals the state after some prefix of some serial order of the writers. | `eItemWritten`, `eReadStart`, `eReadResult` |
| `Authority` | M5 | A lock, an apply, a release, or a guard of an owned row happens only on the current owner of the key, and never on an importing target. | `eLocalTxMutation`, `eOwnerChanged`, `eImportState` |
| `CopiesUntouched` | M5 | No commit, cancel, quarantine, or repair deletes or changes a transfer copy. | `eCopyChanged` |
| `Retention` | M5 | A source deletes a transfer copy only after it recorded the acknowledgement of the target. | `eCopyDeleted`, `eAckRecorded` |

`guarded_at` is one field of the `pending_tx_info` row, so a guard of owned rows also marks the copies of the same
transaction. `CopiesUntouched` counts a guard as a change of a copy only for a copy with no owned row of the same
transaction on that partition.

**`ReadSerializable` in detail.** The monitor keeps, for each key, the list of writers in apply order. When a read
ends, it lists every serial order of the writers that keeps the apply order of each key. For each order and each
prefix, it computes the value of each read key. The read passes when one prefix gives the read result, and that
prefix holds every writer whose client got `committed` before the read started. A test case has at most 4 writers,
so the monitor checks at most 4! = 24 orders and 5 prefixes for each order. Appendix A gives an order that this
monitor rejects.

**Liveness monitors** (hot states). Each one holds when the environment stops the faults after its budget:

| Monitor | From | Rule |
| --- | --- | --- |
| `LocksResolve` | M2 | Each lock row is eventually deleted or guarded. `tcHold` checks it only on the partitions that answer. |
| `ClientAnswered` | M2 | Each call of a client eventually gets an answer. |
| `TransactionsComplete` | M2 | Each coordinator transaction eventually reaches `COMMITTED` or `CANCELLED`. `tcHold` does not check it, because a cancel to a partition that never answers keeps `CANCELLING`. |
| `SplitCompletes` | M5 | The split eventually reaches `cleaned`, and every child reaches `active`. |
| `PromotionCompletes` | M6 | The promotion eventually reaches `cleaned`, and `R` reaches `active`. |

#### 4.2.12 Seeded defects

Each defect is a field of the `tBugs` record that the test driver passes to each machine at creation. The field and
its test case are named after what the defect does: the field `commitOnOneAccept` has the test case
`tcBugCommitOnOneAccept`. The Id links the defect to this table. A milestone names the field and the test case of
each defect that it adds. A test-case name must not be the start of another test-case name, because `p check -tc`
runs every test case whose name starts with the given name.

| Id | From | Test case | Change | Monitor that must fail | Source of the rule |
| --- | --- | --- | --- | --- | --- |
| V1 | M0 | `tcBugNewRowVersionFromOne` | A new row starts at `v = 1`, not at `max_deleted_v + 1`. | `VersionIncreases`; in `tcReadVsWrites` also `ReadSerializable` | `docs/agent-plans/2026-10-03-max-deleted-version.md` |
| W1 | M1 | `tcBugCommitOnOneAccept` | `drivePrepare` and `markCommitting` decide commit when one participant accepted. | `Atomicity` | 2PC |
| W2 | M1 | `tcBugPutIgnoresLock` | `apiPutItem` ignores the lock. | `LockExclusion` | `AGENTS.md`, "A non-transactional write to a locked item is REFUSED" |
| W3 | M1 | `tcBugCommittedBeforeApply` | `drivePrepare` answers `committed` after `markCommitting`, before `runCommit` ends. | `ReadAfterCommit` | `loadFinalResponse`: `committed` promises read-your-writes |
| W4 | M2 | `tcBugTokenRowIgnored` | `initiateWriteLocal` ignores the stored row of the token. | `SingleApply` | idempotency of `clientRequestToken` |
| W5 | M2 | `tcBugStaleCancelsOnDriving` | The participant stale job cancels on `driving`. | `Atomicity` | section 1.3.5 |
| W6 | M2 | `tcBugCancelInAnyState` | `runCancel` sends cancels without the `CANCELLING` check. | `Atomicity` | Quint RFC, section 1.1 |
| W7 | M2 | `tcBugNoPreparingHold` | `runPrepareRecovery` has no `maxPreparingHoldMs` bound. | `LocksResolve` in `tcHold` | `docs/agent-plans/2026-09-09-bounded-preparing-hold.md` |
| W8 | M3 | — | `prepareLocal` skips the timestamp watermark check. | `WriteSerializable` | section 1.3.3 |
| W9 | M3 | — | `executeSingleShot` ignores the lock. | `LockExclusion` | `executeSingleShot` |
| W10 | M3 | — | A new row starts its timestamps at its own stamp, not at least `max_delete_tx_order_ts`. | `WriteSerializable` | section 1.3.3 |
| R1 | M4 | — | The two-phase path skips phase 2. | `ReadSerializable` | section 1.3.2 |
| R2 | M4 | — | The two-phase path ignores `hasPendingWrite`. | `ReadSerializable` | section 1.3.2 |
| R3 | M4 | — | The two-phase path ignores `maxDeletedV` for an absent item. | `ReadSerializable` | Appendix A |
| S1 | M5 | — | A child does not merge the deletion metadata of the source. | `VersionIncreases`, `ReadSerializable` | section 1.3.6 |
| S2 | M5 | — | An importing child serves `txPrepare`. | `Authority` | section 4.2.9, rule 2 |
| S3 | M5 | — | A router serves `apiGetItem` from its own item rows. | `ReadAfterCommit` | section 1.3.6 |
| S4 | M5 | — | The import skips the `pending_tx` stream. | `Atomicity` | section 1.3.6 |
| S5 | M5 | — | A split router deletes its lock copies at cutover. | `Retention` | section 4.2.9, rule 6 |
| P1 | M6 | — | The stale job deletes a `not_found` transaction when no row is owned. | `CopiesUntouched`, `SingleApply` or `Atomicity` | Promotion RFC 1.2, item 1 |
| P2 | M6 | — | `commitLocal` compares the request with all rows, copies included. | `ClientAnswered`, `LocksResolve` | Promotion RFC 1.2, item 2 |
| P3 | M6 | — | The routed `txCommit` and `txCancel` delete by transaction id. | `CopiesUntouched` | Promotion RFC 1.2, item 3 |
| P4 | M6 | — | The source deletes the copies of `K` at cutover. | `Retention` | Promotion RFC 4.2.8 |
| P5 | M6 | — | An importing `R` serves transaction operations. | `Authority` | Promotion RFC 4.2.2, stage 3 |
| P6 | M6 | — | The stale job uses the owner result from before the coordinator call. | `Authority` | Promotion RFC 4.2.6 |
| P7 | M6 | — | A cancel with an empty `items` list releases by transaction id. | `CopiesUntouched` | Promotion RFC 4.2.5 |
| C1 | M7 | — | A coordinator transition writes after the cutover. | `SingleApply` | `AGENTS.md`, `fokos.owns(token)` |

For P1, P3, and P4, the run that loses a decided write must also exist: the copy goes before the `pending_tx` stream
reads it. The test case for each one asserts the first monitor, and the milestone records whether the checker also
finds the lost write. The same applies to S4 and S5.

#### 4.2.13 Configurations and test cases

Keys: `a1` on root `A`; `b1` and `b2` on root `B`. M5 adds `c1`: hash keys `a` and `c` are on `A`, and the split
moves `a` to child `A0` and `c` to child `A1` (`hashSplitN = 2`). M6 adds `a2`: the promotion moves hash key `a`
(`a1`, `a2`) to `R`, and `c1` stays on `A`.

| Test case | Writers and readers | Faults | Expected |
| --- | --- | --- | --- |
| `tcItems` | put, delete, put, get of `a1` | none | pass |
| `tcWriteHappy` | T1 puts `a1`, `b1`; T2 puts `b2` | none | pass |
| `tcWriteConflict` | client 1: T1 puts `a1`, `b1`, then a get of `b1`; client 2: T2 puts `a1` with `not_exists`, `b2`, then a put of `a1` | none | pass |
| `tcWriteFaults` | as `tcWriteConflict` | 2 restarts, 2 lost calls | pass |
| `tcWriteRetry` | T1 as `tcWriteHappy`, and the caller retries T1 with the same token up to 2 times, then sends it once more after the final answer | 2 lost answers | pass |
| `tcConcurrentDrives` | T1 puts `a1`, `b1` with a request drive, a retry drive, and a `tx_recovery` drive; T2 puts `a1` | 1 restart, 1 lost answer | pass |
| `tcStale` | as `tcWriteConflict` | the coordinator drops `recoverTransactionForParticipant` until the sweep deleted the transaction, then answers `not_found`; 2 lost calls | pass |
| `tcRepair` | as `tcStale` | as `tcStale`, and the operator repairs on `A` and on `B` after a lock-age guard error | pass |
| `tcHold` | T1 puts `a1`, `b1` | `B` drops every call; 1 restart | pass |
| `tcClocks` | T1 checks `a1` and puts `b1`; T2 puts `a1`; T3 deletes `b1`; a put of `b1`; T4 puts `b1` | clock skew | pass |
| `tcSingleShot` | a single-partition transaction on `b1`, `b2`; T1 puts `a1`, `b1` | clock skew | pass |
| `tcReadHappy` | R1 reads `a1`, `b1` after T1 | none | pass |
| `tcReadVsWrites` | W1 creates `a1`, puts `b1`; W2 deletes `a1`, puts `b2`; R1 reads `a1`, `b1`, `b2` | none | pass |
| `tcReadFaults` | as `tcReadVsWrites` | 2 restarts, 2 lost calls | pass |
| `tcSnapshot` | R1 reads `b1`, `b2` on the snapshot path; T1 puts `b1`, `b2`; T2 puts `a1`, `b1` | none | pass |
| `tcSplitWrites` | T1 puts `a1`, `b1`; T2 deletes `a1` and puts `c1`; T3 puts `a1` | the split of `A` | pass |
| `tcSplitReads` | as `tcReadVsWrites` with `a1` and `c1` on `A` | the split of `A` | pass |
| `tcSplitFaults` | as `tcSplitWrites` | the split of `A`, 2 restarts, 2 lost calls | pass |
| `tcPromotionHappy` | T1 on `a1`, `c1`; T2 on `a2` | the promotion of `a` | pass |
| `tcPromotionFaults` | T1 on `a1`, `c1`; T2 on `a2`; T3 on `a1` (conflicts with T1) | the promotion of `a`, 2 restarts, 2 lost calls | pass |
| `tcPromotionStale` | T1 on `a1`, `c1` | the promotion of `a`; the coordinator does not answer until `OverWindow`, then answers `not_found` | pass |
| `tcPromotionRepair` | T1 on `a1`, `c1` | as `tcPromotionStale`, then the operator repairs on `A` and on `R` | pass |
| `tcPromotionReads` | as `tcReadVsWrites` with `a1`, `a2`, `c1` on `A` | the promotion of `a` | pass |
| `tcCoordinatorSplit` | T1 as `tcWriteHappy`, with retries | the split of the coordinator | pass |
| `tcBug<Defect>` | the test case of the milestone that the defect needs | as that test case | fail with the monitor of section 4.2.12 |

Each milestone measures the run count of each of its test cases. A test case that passes in PEx within its timeout
records that fact.

The values are from the model of M2, which adds the clock, the recovery jobs, and the `Environment` to every test
case. The PEx column of M1 also gives the result with the model of M1.

| Test case | Milestone | Schedules | Run time | Result | PEx |
| --- | --- | --- | --- | --- | --- |
| `tcItems` | M0 | 1000 | 12 s | no bug; 3 timelines | `correct for any depth` |
| `tcBugNewRowVersionFromOne` | M0 | 1000 | 1 s | `VersionIncreases` fails in schedule 1 | counterexample |
| `tcWriteHappy` | M1 | 1000 | 26 s | no bug; 122 timelines | not complete in 120 s (M1 model: `correct for any depth`, 2,935 states, 2 s) |
| `tcWriteConflict` | M1 | 1000 | 34 s | no bug; 797 timelines | not complete in 120 s (M1 model: `correct for any depth`, 44,675 states, 9 s) |
| `tcBugCommitOnOneAccept` | M1 | 1000 | 1 s | `Atomicity` fails in schedule 1 | counterexample |
| `tcBugPutIgnoresLock` | M1 | 1000, fair PCT | 1 s | `LockExclusion` fails in schedule 7; the random strategy needs about 400 to 900 | counterexample in 94 s |
| `tcBugCommittedBeforeApply` | M1 | 1000 | 1 s | `ReadAfterCommit` fails in schedule 4 | counterexample |
| `tcWriteFaults` | M2 | 1000 | 37 s | no bug; 948 timelines | — |
| `tcWriteRetry` | M2 | 1000 | 38 s | no bug; 552 timelines | — |
| `tcConcurrentDrives` | M2 | 1000 | 33 s | no bug; 764 timelines | — |
| `tcStale` | M2 | 1000, fair PCT | 28 s | no bug; 956 timelines | — |
| `tcRepair` | M2 | 1000, fair PCT | 32 s | no bug; 958 timelines | — |
| `tcHold` | M2 | 1000 | 82 s | no bug; 44 timelines; each run lasts until the clock stops | — |
| `tcBugTokenRowIgnored` | M2 | 1000 | 1 s | `SingleApply` fails in schedule 1 | counterexample in 2 s |
| `tcBugStaleCancelsOnDriving` | M2 | 1000 | 1 s | `Atomicity` fails in schedule 5 | counterexample in 4 s |
| `tcBugCancelInAnyState` | M2 | 1000, fair PCT | 21 s | `Atomicity` fails in schedule 592; between 10 and 600 in other runs; none in 5000 random schedules | not complete in 120 s |
| `tcBugNoPreparingHold` | M2 | 1000 | 1 s | `LocksResolve` fails in schedule 2 | counterexample in 22 s |

The run time is the wall time of `check.sh` for one test case, Docker start included. The schedule of a defect is
from one run, and changes with the seed. "Fair PCT" is `--sch-fairpct 10`.

In M2, probes confirmed that `tcStale` and `tcRepair` reach a lock that a late prepare wrote after the cancel, the
quarantine, and the repair after the quarantine (with fair PCT: 245, 397, and 474 schedules), and that the
`tcWriteRetry` replay answers `committed` from the ledger. In `tcConcurrentDrives`, no answer was
`transaction_commit_pending` or `transaction_undecided` in 3000 schedules: with 1 lost answer, no fan-out can use up
its attempts.

In M1, probes (a temporary monitor that fails when a run reaches a state) confirmed that `tcWriteConflict` reaches
each of these orders: T1 cancelled, T2 committed, T1 and T2 both committed, the put of `a1` applied, and the get of
`b1` found and absent. A run where T1 and T2 both cancel is not possible: the two transactions share only `a1`, and
the one that gets its lock first has no other conflict.

#### 4.2.14 Link to the code

P does not run the TypeScript code. The model links to the code in two ways:

1. **Regression tests.** Each real defect that the model finds becomes a vitest test before its fix. A defect of the
   coordinator goes into `packages/fokosdb/test/transactions/`. A defect of a split goes into
   `packages/fokosdb/test/partition-do/hash-split.test.ts`, and a defect of a promotion into
   `packages/fokosdb/test/partition-do/tx-promotion-transfer.test.ts`. The test drives the same order with
   `TestPartition` (`triggerHashSplit`, `triggerPromotion`, `runAlarm`), `drainUntil`, and the gates of
   `ControlledPartitionDO`: `testHoldPulls` with the `pending_tx` stream, `testRefuseAcks`, `testHoldInit`,
   `testHoldPrepare`, and `testHoldReadPhase`. A restart in the trace becomes a new test control of
   `ControlledPartitionDO` or `ControlledTransactionCoordinatorDO` that calls `ctx.abort()`. The next request then
   makes a new instance that reads SQLite.
2. **Names.** Each handler names its code function in a comment, so a reviewer of a code change can find the handler
   that must change.

#### 4.2.15 Maintenance

- A plan that changes a rule of section 4.2.6 to 4.2.9 must update the model and its seeded-defect table in the same
  change.
- The spec-review skill asks for that update.
- M0 adds a `formal:p` script to `packages/fokosdb/package.json` and a root script that calls it.
- M0 adds one line to `AGENTS.md` under "Where the detail lives" that names the model and the command.
- No CI runs the checks. The author of a change to one of these rules runs them.

#### 4.2.16 Cost

The model adds no production code and no runtime cost. The checks run outside `pnpm test`. In M2, `pnpm formal:p`
takes about 6 minutes for the 17 test cases. `pnpm formal:p --pex` stops each test case that it cannot complete at
`PEX_TIMEOUT`, so run it on chosen test cases. The first run also builds the Docker image, which takes about 1
minute. Section 4.2.13 records the run time of each test case.

#### 4.2.17 Testing

The work is correct when:

- Each test case of section 4.2.13 that expects a pass passes for the measured run count.
- Each seeded defect fails with its monitor.
- Each real defect has a vitest regression test that fails before its fix and passes after it.

## 5. Alternative options

- **The Quint model of the coordinator** (`docs/ideas/2026-10-09-quint-model-of-the-transaction-coordinator.md`) for
  the coordinator rules of section 4.2.7, concurrent drives included. It adds the ITF trace replay in vitest and
  liveness under fairness in TLC. It is not chosen for now: this model checks the same rules in M1 and M2, and one
  tool keeps one set of monitors. When the Quint model is built later, M2 drops `tcConcurrentDrives` and the defect
  W6, and the coordinator of this model follows the rules that the Quint model checks.
- **Quint with Choreo for the whole plan.** Choreo gives a fixed process set (`choreo(processes = NODES)`) and a
  message soup, in which a message stays in the set of its receiver and a transition reads the whole set. This gives
  every delivery order and every duplicate with no extra work, and Quint states a property over the whole state with
  sets and quantifiers. TLC checks liveness under fairness, and the ITF traces replay in vitest. But the normal
  `step` reacts to the whole message set, so one transition is not one block of a Durable Object. `step_micro`
  consumes one message at a time, and the Choreo docs say that it gives less coverage in the simulator and less
  chance to finish `quint verify`. A split child and a range root must also exist before the run. Milestones 5 and 6
  need one handler for each block, machines created at run time, and a deep search of schedules, so P fits them
  better. Two tools would also need two copies of the monitors.
- **Quint or TLA+ for the promotion flow only.** The flow creates a Durable Object at run time and has many calls in
  flight. A TLA model needs a fixed set of partition names and a hand-written network, and Apalache handles a
  growing structure slowly. P has machine creation and asynchronous events built in.
- **One P project for each milestone.** Each project is smaller. But the monitors then exist in more than one copy,
  and a later environment does not check the earlier guarantees. One project keeps one copy.
- **More property-based suites.** `packages/fokosdb/test/property-based/transactions-split.test.ts` runs the real
  code and finds real defects. It samples a few schedules, and it cannot restart a Durable Object between two
  arbitrary blocks or lose one answer of many.
- **Deterministic simulation of the real code** (`docs/ideas/2026-09-26-testing-approaches.md`, section 5.5). It
  checks the code itself, but it needs a simulation layer for workerd and SQLite. The model is a smaller first step.

## 6. Frequently asked questions

**Does a passing run prove the design correct?** No. The default P checker samples schedules within a bound. A pass
shows that it found no violation in the measured number of runs. A seeded defect that it finds shows that the bound
reaches that class of defect. PEx explores every state of a small test case, but only of that test case.

**Why do the monitors stay the same in every milestone?** They state the public guarantees of FokosDB, and a split or
a promotion must not change those guarantees. Thus a monitor written in M1 checks the split in M5 with no change.

**Why does M1 have an `RpcCall` machine when it has no fault?** A P `send` puts the event in the inbox of the
receiver at once. With direct sends, a commit that the coordinator sends before its answer always reaches the partition
before a read that the client sends after the answer, so W3 cannot fail. The prepares of two transactions would also
reach each partition in the same order. Workers RPC gives no such order. M2 adds the faults to the same machine.

**Why model only one write for each item?** `ordered_per_item` changes how one partition evaluates a request, in one
block. It does not change the messages between the Durable Objects, which are the subject of this model.

**Why one source and one target for a promotion?** One promotion has one target. A hash split after a promotion adds
the override rules of `belongsToTarget`. That extension comes after M6.

**Why a page size of one row?** A larger page hides the orders between two rows of one stream. The real page budget
changes only the number of pulls.

**Why at most 4 writers in a test case?** `ReadSerializable` lists every serial order of the writers. 4 writers give
at most 24 orders, which the monitor checks at the end of each read.

**What if the model finds a defect in the code?** The author writes the vitest regression test first, then fixes the
code in a separate change with its own plan when the fix changes a rule.

## 7. References

- `AGENTS.md`
- `packages/fokosdb/src/client/db.ts`
- `packages/fokosdb/src/server/do-transaction-coordinator.ts`
- `packages/fokosdb/src/server/do-partition.ts`
- `packages/fokosdb/src/server/host-config.ts`
- `packages/fokosdb/src/shared/partition/transaction-participant.ts`
- `packages/fokosdb/src/shared/partition/partition-store.ts`
- `packages/fokosdb/src/sharding/repartition-flow.ts`
- `packages/fokosdb/test/controlled-partition-do.ts`
- `packages/fokosdb/test/controlled-transaction-coordinator-do.ts`
- `packages/fokosdb/test/partition-do/partition-harness.ts`
- `packages/fokosdb/test/partition-do/hash-split.test.ts`
- `packages/fokosdb/test/partition-do/tx-promotion-transfer.test.ts`
- `packages/fokosdb/test/property-based/transactions-split.test.ts`
- `docs/agent-plans/2026-09-05-item-order-timestamps-and-read-revisions.md`
- `docs/agent-plans/2026-09-09-bounded-preparing-hold.md`
- `docs/agent-plans/2026-09-17-unified-repartition-flow.md`
- `docs/agent-plans/2026-09-29-promotion-moves-its-locks.md`
- `docs/agent-plans/2026-10-03-max-deleted-version.md`
- `docs/ideas/2026-10-09-quint-model-of-the-transaction-coordinator.md`
- `docs/ideas/2026-09-26-testing-approaches.md`
- [P documentation](https://p-org.github.io/P/)
- [Installing P](https://p-org.github.io/P/getstarted/install/)
- [Using the P compiler and checker](https://p-org.github.io/P/getstarted/usingP/)
- [P tutorial: Two Phase Commit](https://p-org.github.io/P/tutorial/twophasecommit/)
- [P tutorial: Timer, Failure, and Shared Memory](https://p-org.github.io/P/tutorial/common/)
- [P liveness specifications](https://p-org.github.io/P/advanced/importanceliveness/)
- [PEx exhaustive checking](https://p-org.github.io/P/advanced/pex/)
- [Choreo](https://quint.sh/docs/choreo)
- [Choreo tutorial](https://quint.sh/docs/choreo/tutorial)
- [Choreo micro steps](https://quint.sh/docs/choreo/step-micro)
- [Systems Correctness Practices at AWS](https://queue.acm.org/detail.cfm?id=3712057)
- [ATC 2023, Idziorek et al.](https://www.usenix.org/system/files/atc23-idziorek.pdf)

## Appendix A. An order that only the `maxDeletedV` comparison catches

Configuration: `a1` on `A`; `b1` and `b2` on `B`. Writer W1 creates `a1` and puts `b1`. Writer W2 deletes `a1` and
puts `b2`. Reader R1 reads `a1`, `b1`, and `b2` on the two-phase path.

1. Phase 1 on `A` reads `a1` absent, before W1 prepares on `A`.
2. W1 prepares on `A` and `B`, and commits on `A` and `B`.
3. Phase 1 on `B` reads `b1` = W1 and `b2` = old.
4. Phase 2 on `B` reads `b1` = W1 and `b2` = old, before W2 prepares on `B`.
5. W2 prepares on `A` and `B`, and commits on `A`. `a1` is absent again.
6. Phase 2 on `A` reads `a1` absent.

No item has a pending write in either phase. `found` and `version` match for `b1` and `b2`, and `a1` is absent in
both phases. The result is not serializable:

- `b1` = W1 puts W1 before R1.
- `a1` absent then needs W2 before R1, because W1 created `a1`.
- `b2` = old needs R1 before W2.

In the code, the delete of W2 raises the `max_deleted_v` of `A` between the two phases, so R1 aborts with
`read_conflict`. With defect R3, R1 returns the result, and `ReadSerializable` reports the violation.
