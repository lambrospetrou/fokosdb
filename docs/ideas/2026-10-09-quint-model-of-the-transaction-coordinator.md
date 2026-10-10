# RFC — A Quint model of the transaction coordinator and its recovery

**State:** Draft
**Date:** 2026-10-09
**Author:** Lambros Petrou
**Status:** Not started. No model, script, or test exists yet.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
  - [1.1 The problem](#11-the-problem)
  - [1.2 Why now](#12-why-now)
  - [1.3 The coordinator in one page](#13-the-coordinator-in-one-page)
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

`TransactionCoordinatorDO` drives a write transaction through two-phase commit. More than one drive of one
transaction can run at the same time: the request, a retry with the same `clientRequestToken`, and the `tx_recovery`
job. Each drive stops at every `await`, and another drive can run its synchronous block in that gap. A participant
can also ask the coordinator about a stale lock at any moment.

The rules that keep this safe are subtle, and each one exists because of a defect:

- `markCommitting` requires a stored accepted answer from every participant. A participant keeps only its first
  answer, so it can reject one drive and accept another. The drive that got the accept sees only accepted answers in
  memory, but the transaction must cancel.
- `runCommit` sends commits only when the stored state is `COMMITTING`, and `runCancel` sends cancels only when it is
  `CANCELLING`. A fan-out that follows a decision that lost breaks atomicity.
- `drivePrepare` reads the state after its `CREATED → PREPARING` transition. A drive that continues after another
  drive decided can lock keys after the cancel released them.
- `runPrepareRecovery` cancels a transaction that stays in `PREPARING` longer than `maxPreparingHoldMs`.
  `docs/agent-plans/2026-09-09-bounded-preparing-hold.md` describes the defect: without the bound, a transaction
  whose participant never answers stays in `PREPARING` for the life of the coordinator and holds its locks.

Tests found these defects after the code existed. The vitest suites check chosen interleavings, and the
property-based suites in `packages/fokosdb/test/property-based/` sample interleavings of the real code. Neither
explores every order of the synchronous blocks, and neither checks a liveness property. A liveness defect, such as
the unbounded `PREPARING` hold, shows only as a test that never ends or as a lock in production that never goes away.

### 1.2 Why now

- The coordinator rules are stable and written down in `AGENTS.md` (section "Transactions (2PC)"). A model can follow
  them one to one.
- `docs/ideas/2026-09-26-testing-approaches.md` (sections 5.8 and 5.9) recommends a formal model of 2PC as the next
  design-level check, and Quint for its trace export.
- `docs/agent-plans/2026-10-09-p-model-of-promotion-lock-transfer.md` uses an abstract coordinator. This model checks
  the rules of that abstract coordinator, so the P model does not need to check them again.

### 1.3 The coordinator in one page

The model follows these facts of the current code. Section 4.2.3 maps each one to a model action.

**Durable state** (`packages/fokosdb/src/server/do-transaction-coordinator.ts`):

| Table | What the model keeps |
| --- | --- |
| `tc_state` | `state`, `created_at`, `completed_at`, `next_recovery_at`. |
| `tc_participants` | `prepare_outcome`, `commit_outcome`, `cancel_outcome`, and `error_json` as none, transient, or fatal. |
| `tc_items` | Only the participant of each operation and the kind of the operation (write or check). |

**States.** `CREATED → PREPARING → COMMITTING → COMMITTED`, or `CREATED → PREPARING → CANCELLING → CANCELLED`. The
coordinator writes each transition to SQLite before it sends an RPC. `COMMITTING` is the point of no return. Only the
SQL writes of `COMMITTING` (`markCommitting`) and of `CANCELLING` (`cancelTransactionInStore`) decide, and both
require `PREPARING`. The coordinator does not write `PREPARED`, and the model leaves out that state.

**Drives.** `initiateWriteLocal` inserts the transaction, schedules `tx_recovery`, and calls `drivePrepare`. A retry
with the same token calls `resumeTransaction`, which drives from the stored state. The `tx_recovery` job claims due
transactions with `claimDueTransactions` and drives each one with `driveTransaction`. At most
`recoveryConcurrentDrives` (6) drives of the job run at the same time.

**Participant side** (`packages/fokosdb/src/shared/partition/transaction-participant.ts` and
`packages/fokosdb/src/server/do-partition.ts`):

- `prepareLocal` refuses a key that another transaction locks (`pending_conflict`), refuses a timestamp at or below
  the item watermark (`timestamp_conflict`), and otherwise writes one lock row for each key.
- `commitLocal` applies the lock rows and deletes them. With no lock row it answers the idempotent success.
- `cancelLocal` deletes the lock rows.
- The `stale_tx_recovery` job claims a stale transaction and calls `recoverTransactionForParticipant`. The answer
  `COMMITTED` gives a commit and `CANCELLED` gives a cancel. The answer `not_found` gives a cancel when the lock is no
  older than `IDEMPOTENCY_WINDOW_MS`, and a quarantine (`guarded_at`) when it is older. The answer `driving` changes
  nothing.

**Coordinator recovery answers.** `recoverTransactionLocal` answers from the ledger. For a transaction that is not
complete, it sets `next_recovery_at` to now, makes the `tx_recovery` job due now, and answers `driving`.

**Idempotency window.** The `idempotency_sweep` job deletes a completed transaction `IDEMPOTENCY_WINDOW_MS`
(10 minutes) after `completed_at`. After the sweep, `recoverTransactionLocal` answers `not_found`.

**Time values.** `staleTransactionMs` is 5000 ms by default. `maxPreparingHoldMs` is 5 × `staleTransactionMs`, capped
at `IDEMPOTENCY_WINDOW_MS` (`packages/fokosdb/src/server/host-config.ts`). Thus
`staleTransactionMs < maxPreparingHoldMs ≤ IDEMPOTENCY_WINDOW_MS` for every valid configuration.

### 1.4 Glossary

- **Drive** — one run of `drivePrepare`, `runPrepareRecovery`, `runCommit`, or `runCancel` for one transaction, from
  one caller.
- **Block** — the synchronous code between two `await`s of one DO. No other request of that DO runs inside a block.
- **Decision** — the SQL write of `COMMITTING` or of `CANCELLING`.
- **Late prepare** — a `txPrepare` that reaches a participant after the coordinator decided.
- **Age class** — the abstract age of a transaction, a lock, or a completed transaction (section 4.2.5).
- **Mutation** — a deliberate defect in a copy of the model. The checker must find a violation for each mutation.

## 2. Goals and requirements

### 2.1 In scope

- A Quint model of one coordinator, its concurrent drives, its `tx_recovery` job, its idempotency sweep, and 2 or 3
  abstract participants with their stale-transaction job.
- Each action of the model is one block of the code, and the model names the code function of each action.
- The safety properties of section 4.2.6 hold in the Quint simulator and in `quint verify` for the configurations of
  section 4.2.8.
- The liveness properties of section 4.2.6 hold in `quint verify --backend=tlc` under the fairness of section 4.2.6.
- Each mutation of section 4.2.7 makes the checker report a violation of the property that the table names.
- A corpus of ITF traces from the model replays in vitest in workerd against `ControlledTransactionCoordinatorDO` and
  `ControlledPartitionDO`, and each replayed step gives the state that the trace expects.
- One command runs each check from the repository root.

### 2.2 Out of scope

- **Partition splits and promotions.** The P model covers them
  (`docs/agent-plans/2026-10-09-p-model-of-promotion-lock-transfer.md`).
- **Coordinator split.** A transition after a cutover throws `partition_migrating` and the client retries with the
  same token. This adds a second coordinator and a migration stream. It can extend this model after milestone 5.
- **Coordinator state loss.** `fokosDestroy` deletes the ledger, but it also fences every partition, so the model has
  no state where a live participant asks a coordinator with no ledger before the window ends.
- **Item-level serializability across transactions.** The property-based suites and the read-transaction rules
  cover it. The model has one item for each participant and checks only the effect of one transaction on it.
- **Result payloads.** `results_json`, `tc_results`, the image cap, and `operations_hash`. They change the answer,
  not the decision.
- **Read transactions, the single-partition path, and `executionMode: "ordered_per_item"`.**
- **Admission.** `coordinator_over_size` refuses a new transaction before any state exists.

### 2.3 Requirements

- The work must not change production code and must not add a production hook. A control that the trace replay needs
  goes into `ControlledTransactionCoordinatorDO` or `ControlledPartitionDO`, which are test classes.
- The model must use the names of the code: state names, table and column names, and function names.
- The tool versions must be pinned. A new dependency must follow the release-age rule of `pnpm-workspace.yaml`.
- `pnpm check` must stay green. A committed trace corpus must not fail `prettier --check .`.

## 3. Milestones

Each milestone ends with its checks green and the mutation table of section 4.2.7 updated.

1. **Decision core.** The coordinator state, the drives of `drivePrepare`, `runPrepareRecovery`, `runCommit`,
   `runCancel`, and `completeTransaction`, the network, and participants that answer prepare, commit, and cancel.
   Invariants `Atomicity`, `NoReturn`, `CommitNeedsStoredAccepts`, `FanoutFollowsDecision`,
   `TerminalHasCompletedAt`, `FatalErrorSticks`, `AnswerMatchesState`. Mutations M2, M3, and M4 must fail.
2. **Recovery and time.** Age classes, `tx_recovery`, `recoverTransactionLocal`, the participant stale job, the
   quarantine, the idempotency sweep, late prepares, and coordinator restart. Invariants `NoSecondApply` and
   `CancelOnlyUndecidedOrCancelled`. Mutation M5 must fail.
3. **Liveness.** Fairness conditions and the temporal properties `EventualDecision`, `EventualCompletion`, and
   `EventualLockResolution` under `--backend=tlc`. Mutation M1 must fail.
4. **Trace replay.** Trace generation with `quint run --mbt --out-itf`, a committed corpus, the vitest replay suite,
   and the test controls that it needs.
5. **Wiring.** A `formal:quint` script in `packages/fokosdb/package.json` and a root script that calls it, the
   `AGENTS.md` entry, and the spec-review check that a plan which changes a coordinator rule also updates the model.

Milestones 1 to 3 deliver a design check on their own. Milestone 4 links the model to the code.

## 4. Proposed solution

### 4.1 High-level overview

The model is a state machine of one coordinator and 2 or 3 participants. Each transition of the model is one block
of the code: the code cannot interleave inside a block, so the model does not either. Between two blocks, any other
enabled block can run. The checker explores these orders.

```text
                 Start / Retry                    tx_recovery claim
                      |                                  |
                      v                                  v
  +------------------------------------------------------------------+
  | Coordinator: tc_state, tc_participants                           |
  |   drives d1..dN, each one a program counter at an await          |
  +------------------------------------------------------------------+
        | txPrepare / txCommit / txCancel        ^ answers, errors
        v  (in-flight set: delay, loss, repeat)  |
  +------------------------------------------------------------------+
  | Participants p1..p3: lock row, item watermark, applied, guard    |
  |   stale job -> recoverTransactionForParticipant -> answer        |
  +------------------------------------------------------------------+
        environment: restart the coordinator, age the transaction,
                     sweep a completed transaction
```

The environment can delay, lose, or repeat any message, restart the coordinator (drives stop, SQLite stays), and move
time forward in coarse steps.

The model checks three kinds of properties:

- **Safety invariants**, for example "no participant applies a commit while another participant releases a lock by a
  cancel". The Quint simulator samples long runs. `quint verify` checks them for every run up to a bound.
- **Liveness properties**, for example "every transaction leaves `PREPARING`, even when one participant never
  answers". TLC checks them through `quint verify --backend=tlc`.
- **Mutations.** Each known defect becomes a constant of the model. The checker must report a violation when the
  constant is on. A mutation that the checker does not catch shows that the model is too coarse.

The model links to the code in one direction: the simulator writes ITF traces, and a vitest suite replays them on the
real Durable Objects in workerd and compares the state after each step.

### 4.2 Technical details

#### 4.2.1 Layout and tools

```text
packages/fokosdb/formal/quint/tx-coordinator/
  txCoordinator.qnt        the model: types, state, actions, properties
  txCoordinatorMutations.qnt  one instance of the model for each mutation
  configs.qnt              the instances of section 4.2.8
  traces/                  the committed ITF corpus (milestone 4)
packages/fokosdb/test/formal/
  itf.ts                   a small ITF reader
  tx-coordinator-itf.test.ts  the replay suite
```

- Quint comes from the npm package `@informalsystems/quint`, as a dev dependency of `packages/fokosdb` with a
  pinned version.
- `quint verify` downloads Apalache on first use. `--backend=tlc` needs Java 17 or later.
- `packages/fokosdb/formal/quint/tx-coordinator/traces/` goes into `.prettierignore`, because `prettier --check .`
  checks every JSON file.
- The package publishes only `dist`, so `formal/` does not go into the npm package.

#### 4.2.2 Granularity

One model action is one block of the code. The code holds this property itself: a DO runs no other request inside a
block, and each durable transition runs in one `transactionSync`.

A drive is a record with a program counter. An `await` in the code is a program counter value in the model. The
action that leaves that value is the code after the `await`.

A participant answer is a message. The coordinator block that handles the answer is a separate action, so other
blocks can run between the participant block and the coordinator block.

#### 4.2.3 State and actions

State sketch. Names follow the code. The final model can change the encoding, but not the names.

```quint
module txCoordinator {
  const PARTICIPANTS: Set[str]
  const MAX_DRIVES: int
  const OP_KIND: str -> str            // participant -> "write" | "check"

  type TcState = Created | Preparing | Committing | Committed | Cancelling | Cancelled | Swept
  type Outcome = Unknown | Accepted | Rejected
  type PrepareError = NoError | Transient | Fatal
  type Age = Fresh | Stale | OverHold | OverWindow

  // tc_state and tc_participants
  var state: TcState
  var txAge: Age
  var prepareOutcome: str -> Outcome
  var errorJson: str -> PrepareError
  var commitOutcome: str -> bool
  var cancelOutcome: str -> bool
  var completedAt: bool                 // completed_at IS NOT NULL
  var completedAge: Age                 // the idempotency window after completion

  // drives: program counters, lost on a restart
  var drives: int -> DrivePc

  // network: in-flight requests and answers
  var inFlight: Set[Message]

  // participants: one item each
  var lock: str -> bool                 // a lock row of this transaction
  var lockAge: str -> Age
  var applied: str -> int               // commits applied to the item
  var watermarkAtTx: str -> bool        // the item watermark is at or above the transaction timestamp
  var guarded: str -> bool              // pending_tx_info.guarded_at IS NOT NULL

  // history, read only by the properties
  var everCommitting: bool
  var everCancelling: bool
  var cancelReleased: str -> bool       // a cancel deleted a lock row that a prepare wrote before the decision
}
```

Actions and the code they model:

| Action | Code | Block |
| --- | --- | --- |
| `Start` | `initiateWriteLocal` | Insert `CREATED`, `tc_items`, `tc_participants`. Then call `drivePrepare`. |
| `Retry` | `initiateWrite`, `resumeTransaction` | Start a drive from the stored state. |
| `BeginPrepare(d)` | `drivePrepare` | `CREATED → PREPARING`, read the state. Stop when it is not `PREPARING`. |
| `SendPrepare(d, p)` | `drivePrepare`, `runPrepareRecovery` | Put `txPrepare` in flight. |
| `OnPrepareAnswer(d, p)` | `storePrepareAnswer` | Store the first answer, only in `PREPARING`, never over a fatal error. |
| `OnPrepareError(d, p, e)` | `storePrepareError` | Store a transient or fatal error. A fatal error stays. |
| `DecideAfterPrepare(d)` | `drivePrepare` after `Promise.allSettled` | All in memory accepted: `markCommitting`. Else cancel. |
| `RecoverPrepare(d)` | `runPrepareRecovery` | A stored fatal error cancels. Else send prepares to `Unknown` participants. |
| `DecideAfterRecovery(d)` | `runPrepareRecovery` | All stored accepted: commit. Rejected, fatal, or `OverHold`: cancel. Else stay. |
| `BeginCommit(d)` | `runCommit` | Read the state. Only `COMMITTING` sends to participants with no `commit_outcome`. |
| `OnCommitDone(d)` | `runCommit` | Store confirmed outcomes. Complete when none is missing. |
| `BeginCancel(d)` | `runCancel` | Read the state. Only `CANCELLING` sends to participants with no `cancel_outcome`. |
| `OnCancelDone(d, p)` | `runCancel` | Store `cancel_outcome`. Complete when none is missing. |
| `Complete(d, s)` | `completeTransaction` | `COMMITTING → COMMITTED` or `CANCELLING → CANCELLED`, set `completed_at`. |
| `RecoveryClaim` | `claimDueTransactions`, `driveTransaction` | When due, start a drive from the stored state. |
| `Poke` | `recoverTransactionLocal` | Answer from the ledger. Not complete: make the job due, answer `driving`. |
| `Sweep` | `sweepExpiredTransactions` | A completed transaction in `OverWindow`: delete it (`Swept`). |
| `ApplyPrepare(p)` | `prepareLocal` | Watermark at or above the timestamp, for a write: reject. Else lock. |
| `ApplyCommit(p)` | `commitLocal` | Apply and delete the lock row. No lock row: idempotent success. |
| `ApplyCancel(p)` | `cancelLocal` | Delete the lock row. |
| `StaleAsk(p)` | `recoverStaleTransactions` | A lock in `Stale` or older, not guarded: ask the coordinator. |
| `StaleAnswer(p, r)` | `recoverStaleTransactions` | Apply the answer as section 1.3 describes. |
| `Deliver`, `Lose`, `Repeat` | Workers RPC and `FokosShardingClient` retries | Network faults. A lost answer is a transient error. |
| `RestartCoordinator` | DO eviction or crash | Clear `drives`. Keep every durable variable. |
| `AgeTx`, `AgeLock`, `AgeCompleted` | wall clock | Move one age class forward (section 4.2.5). |

A check operation commits by raising the read watermark only (`bumpItemReadTs`). Its late prepare compares against
`last_write_ts`, which a commit of a check does not raise. Thus a late prepare of a committed check can take a lock
again. The model keeps this case, because `docs/agent-plans/2026-10-03-max-deleted-version.md` covers only a late
prepare of a put, an update, or a delete.

#### 4.2.4 Messages

| Message | From | To | Effect |
| --- | --- | --- | --- |
| `Prepare(d, p)` | drive `d` | `p` | `ApplyPrepare(p)`, then an answer or an error to `d`. |
| `PrepareAnswer(d, p, o)` | `p` | drive `d` | `OnPrepareAnswer`. Dropped when `d` stopped. |
| `Commit(d, p)` | drive `d` | `p` | `ApplyCommit(p)`, then a confirmation to `d`. |
| `Cancel(d, p)` | drive `d` | `p` | `ApplyCancel(p)`, then a confirmation to `d`. |
| `Recover(p)` | `p` | coordinator | `Poke`, then an answer to `p`. |
| `RecoverAnswer(p, r)` | coordinator | `p` | `StaleAnswer(p, r)`. |

The fault budget is a constant of each configuration: the number of lost messages, repeated messages, and restarts.

#### 4.2.5 Time

The model uses age classes, not a clock:

| Class | Meaning for a transaction | Meaning for a lock | Meaning for a completed transaction |
| --- | --- | --- | --- |
| `Fresh` | Younger than `staleTransactionMs`. | Younger than `staleTransactionMs`. | Inside the window. |
| `Stale` | `tx_recovery` can claim it. | The stale job can ask about it. | Inside the window. |
| `OverHold` | Older than `maxPreparingHoldMs`. | — | Inside the window. |
| `OverWindow` | Older than `IDEMPOTENCY_WINDOW_MS`. | Older than the window: `not_found` quarantines. | The sweep can delete it. |

An age action moves one class forward. The order `Fresh < Stale < OverHold < OverWindow` follows from the
configuration rule of section 1.3.

Reasons:

- A clock makes the state space infinite or adds a bound that hides a liveness defect. Four classes keep the state
  space finite.
- Every guard of the code compares an age with one of these three values. Two ages in the same class give the same
  behavior in every guard.
- A lock is younger than its transaction, because prepare runs after the insert. The model holds
  `lockAge ≤ txAge`.

#### 4.2.6 Properties

**Safety invariants:**

| Name | Statement | Mechanism in the code |
| --- | --- | --- |
| `Atomicity` | No participant has `applied > 0` while another participant has `cancelReleased`. | `markCommitting`, the state guards of `runCommit` and `runCancel`. |
| `NoReturn` | `everCommitting` implies `state ∉ {Cancelling, Cancelled}`, and `everCancelling` implies `state ∉ {Committing, Committed}`. | Both decisions require `PREPARING`. |
| `CommitNeedsStoredAccepts` | `state ∈ {Committing, Committed}` implies `prepareOutcome[p] = Accepted` for every `p`. | The `NOT EXISTS` clause of `markCommitting`. |
| `FanoutFollowsDecision` | A `Commit` message exists only when `everCommitting`. A `Cancel` message exists only when `everCancelling`. | The state reads of `runCommit` and `runCancel`. |
| `TerminalHasCompletedAt` | `state ∈ {Committed, Cancelled}` if and only if `completedAt`. | `completeTransaction`, required by `idx_tc_state_recovery`. |
| `FatalErrorSticks` | A stored fatal error stays until the decision, and the decision is cancel. | `storePrepareError`, `storePrepareAnswer`. |
| `AnswerMatchesState` | A client answer `committed` exists only in `Committed`. An answer `cancelled` exists only in `Cancelling` or `Cancelled`. | `loadFinalResponse`. |
| `NoSecondApply` | `applied[p] ≤ 1` for a participant whose operation is a write. | The watermark check of `prepareLocal`, the idempotent `commitLocal`. |
| `CancelOnlyUndecidedOrCancelled` | A participant whose operation is a write cancels a lock only when `everCommitting` is false. | The `not_found` and window rules of the stale job, the watermark check of `prepareLocal`. |

**Liveness properties** (temporal, checked with TLC):

| Name | Statement |
| --- | --- |
| `EventualDecision` | `state` always leaves `Created` and `Preparing`, also when one participant never answers. |
| `EventualCompletion` | When every participant eventually answers, `state` reaches `Committed` or `Cancelled`. |
| `EventualLockResolution` | Every lock row is eventually deleted or guarded. |

**Fairness.** The liveness properties assume weak fairness of `RecoveryClaim`, of every enabled drive action, of
`Deliver` for a message to a participant that the configuration marks as reachable, of `StaleAsk`, and of each age
action. `EventualDecision` does not assume `Deliver` to an unreachable participant.

#### 4.2.7 Mutations

Each mutation is a constant of `txCoordinator.qnt`, off by default. `txCoordinatorMutations.qnt` instantiates the
model once for each mutation with the constant on.

| Id | Change | Property that must fail | Source of the rule |
| --- | --- | --- | --- |
| M1 | `DecideAfterRecovery` ignores `OverHold`. | `EventualDecision` | `2026-09-09-bounded-preparing-hold.md` |
| M2 | `markCommitting` uses the answers in memory, not the stored answers. | `Atomicity`, `CommitNeedsStoredAccepts` | The doc comment of `markCommitting` |
| M3 | `runCancel` sends cancels in any state. | `Atomicity` | The doc comment of `runCancel` |
| M4 | `storePrepareError` lets a transient error replace a fatal error. | `FatalErrorSticks` | The doc comment of `storePrepareError` |
| M5 | `prepareLocal` skips the timestamp check. | `NoSecondApply` | `2026-10-03-max-deleted-version.md` |

A mutation that the checker does not catch is a finding: either the model leaves out the behavior, or the rule is
not necessary. The milestone that adds the mutation must resolve it.

#### 4.2.8 Configurations and commands

| Name | Participants | Drives | Operations | Faults |
| --- | --- | --- | --- | --- |
| `small` | 2 | 2 | write, write | 1 loss, 1 repeat, 1 restart |
| `mixed` | 2 | 3 | write, check | 2 losses, 1 repeat, 1 restart |
| `wide` | 3 | 3 | write, write, check | 2 losses, 1 repeat, 2 restarts |

Commands, from `packages/fokosdb/formal/quint/tx-coordinator/`:

```sh
quint run configs.qnt --main=small --invariant=safety --max-samples=100000 --max-steps=60
quint verify configs.qnt --main=small --invariant=safety --max-steps=30
quint verify configs.qnt --main=small --backend=tlc --temporal=liveness
quint run txCoordinatorMutations.qnt --main=m2 --invariant=safety   # must report a violation
```

The run time of each command is `TODO: measure` in milestone 1. Milestone 5 sets the sample counts from that
measurement.

#### 4.2.9 Trace replay in workerd

The tools of Quint for model-based testing run the driver in Node, and FokosDB runs in workerd. The replay therefore
reads a committed corpus:

1. `quint run configs.qnt --main=small --mbt --n-traces=<n> --out-itf=traces/small.itf.json` writes the traces. With
   `--mbt`, each state carries `mbt::actionTaken` and `mbt::nondetPicks`.
2. The replay suite imports the corpus with `import.meta.glob(..., { eager: true })`, so no file system access is
   necessary in workerd.
3. For each step, the driver calls the code that the action models, and then reads `tc_state`, `tc_participants`,
   and `pending_transactions` with `runInDurableObject`. It compares a projection of that state with the state of the
   trace.

Driver mapping:

| Action | Driver call |
| --- | --- |
| `Start`, `Retry` | `initiateWrite` on the coordinator, without `await`. |
| `RecoveryClaim` | `runDurableObjectAlarm` on the coordinator. |
| `StaleAsk` | `runDurableObjectAlarm` on the partition. |
| `Deliver(Prepare)` | Release a held `txPrepare` on `ControlledPartitionDO`. |
| `Lose(PrepareAnswer)` | Apply the prepare, then fail the RPC. |
| `AgeTx`, `AgeLock` | Move the clock of the instance (`fokosNow`) through a test control. |
| `Repeat` | Send the same request to the partition again. |
| `RestartCoordinator` | A test control that calls `ctx.abort()` on the coordinator. |

`ctx.abort()` resets the Durable Object: the in-memory drives stop, and the next request makes a new instance that
reads SQLite. The call that runs `ctx.abort()` fails, so the driver expects that failure.

New test controls, only in the `Controlled*` classes:

- Hold a `txPrepare` before it applies, and fail it before or after it applies. Today `testHoldPrepare` holds only
  after the apply.
- Hold and fail `txCommit` and `txCancel` before or after they apply. Today `testTxResponse` replaces the call.
- Set a clock offset for `fokosNow` on one instance of each class.
- Restart the coordinator with `ctx.abort()`.

#### 4.2.10 Maintenance

- A plan that changes a coordinator rule of `AGENTS.md` must update the model and its mutation table in the same
  change.
- The spec-review skill asks for that update.
- `AGENTS.md` gets one line under "Where the detail lives" that names the model and the command.
- No CI runs the checks. The author of a change to a coordinator rule runs them.

#### 4.2.11 Cost

The model adds no production code and no runtime cost. The replay suite runs inside `pnpm test`. Its run time is
`TODO: measure` in milestone 4. The simulator and TLC runs stay outside `pnpm test`.

#### 4.2.12 Testing

The work is correct when:

- Every safety invariant holds for each configuration of section 4.2.8.
- Every liveness property holds under TLC for `small` and `mixed`.
- Every mutation of section 4.2.7 gives a violation.
- The replay suite passes on the committed corpus.

## 5. Alternative options

- **TLA+ with TLC.** Same semantics and the same checker. The syntax is further from TypeScript, and the model-based
  testing support is less direct. Quint compiles to the same checkers, so the choice keeps TLC.
- **P.** P fits the message-heavy runtime flow better (section 4.2 of the P plan). For one coordinator with a fixed
  set of participants, exhaustive checks and TLC liveness matter more.
- **FizzBee.** Fast to learn, but younger, and its model-based testing runner also runs in Node.
- **More `fast-check` suites.** They run the real code and find real defects, but they sample a few schedules, they
  cannot stop a drive between two arbitrary blocks, and they check no liveness property.

## 6. Frequently asked questions

**Does a passing model prove that the code is correct?** No. It shows that the rules are correct for the model and
the bounds. The trace replay shows that the code follows the model on the traces of the corpus.

**Why one transaction?** Every rule of the coordinator applies to one transaction at a time. Two transactions add
lock conflicts, which the participant suites and the property-based suites already cover. A configuration with two
transactions can come later.

**Why abstract participants?** The model checks the coordinator protocol. A participant needs only its lock row, its
item watermark, and its stale job to answer every message the coordinator sends.

**What happens when the model and the code disagree?** The replay suite fails. The author then decides which one is
wrong, and fixes it in the same change.

**Why does the model include the check operation?** A late prepare of a committed check can take a lock again, and the
stale job then applies the commit a second time. After the sweep, the stale job gets `not_found` and cancels that
lock. Both results raise only the read watermark or change nothing. The model makes this case explicit, so
`NoSecondApply` and `CancelOnlyUndecidedOrCancelled` apply to writes only.

## 7. References

- `AGENTS.md`
- `packages/fokosdb/src/server/do-transaction-coordinator.ts`
- `packages/fokosdb/src/server/do-partition.ts`
- `packages/fokosdb/src/server/host-config.ts`
- `packages/fokosdb/src/shared/partition/transaction-participant.ts`
- `packages/fokosdb/src/shared/transaction-limits.ts`
- `packages/fokosdb/test/controlled-partition-do.ts`
- `packages/fokosdb/test/controlled-transaction-coordinator-do.ts`
- `docs/agent-plans/2026-09-09-bounded-preparing-hold.md`
- `docs/agent-plans/2026-10-03-max-deleted-version.md`
- `docs/agent-plans/2026-10-09-p-model-of-promotion-lock-transfer.md`
- `docs/ideas/2026-09-26-testing-approaches.md`
- [Quint documentation](https://quint-lang.org/docs/getting-started)
- [Quint model checkers](https://quint-lang.org/docs/model-checkers)
- [Quint model-based testing](https://quint-lang.org/docs/model-based-testing)
- [ATC 2023, Idziorek et al.](https://www.usenix.org/system/files/atc23-idziorek.pdf)
