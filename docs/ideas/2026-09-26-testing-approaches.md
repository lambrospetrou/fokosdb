# Testing Approaches for FokosDB and the Fokos Sharding Runtime

Status: **draft**
Date: 2026-09-26

## Table of Contents

- [1. Overview and context](#1-overview-and-context)
- [2. What the suites have today](#2-what-the-suites-have-today)
- [3. The seams that decide the cost](#3-the-seams-that-decide-the-cost)
- [4. How to read the ratings](#4-how-to-read-the-ratings)
- [5. The approaches](#5-the-approaches)
  - [5.1 Invariant density in the production code](#51-invariant-density-in-the-production-code)
  - [5.2 Global invariants at quiescence](#52-global-invariants-at-quiescence)
  - [5.3 Fault injection at the RPC boundary (a nemesis)](#53-fault-injection-at-the-rpc-boundary-a-nemesis)
  - [5.4 History recording and a consistency checker](#54-history-recording-and-a-consistency-checker)
  - [5.5 Deterministic simulation at the flow tier and the runtime tier](#55-deterministic-simulation-at-the-flow-tier-and-the-runtime-tier)
  - [5.6 Full deterministic simulation of FokosDB](#56-full-deterministic-simulation-of-fokosdb)
  - [5.7 Antithesis](#57-antithesis)
  - [5.8 Formal specification and model checking](#58-formal-specification-and-model-checking)
  - [5.9 A link from the model to the code (MBT and trace validation)](#59-a-link-from-the-model-to-the-code-mbt-and-trace-validation)
  - [5.10 Metamorphic properties](#510-metamorphic-properties)
  - [5.11 Differential testing](#511-differential-testing)
  - [5.12 Mutation testing](#512-mutation-testing)
  - [5.13 Fuzzing of untrusted input](#513-fuzzing-of-untrusted-input)
  - [5.14 Soak and chaos on the real platform](#514-soak-and-chaos-on-the-real-platform)
  - [5.15 Upgrade and compatibility testing](#515-upgrade-and-compatibility-testing)
  - [5.16 Guard scripts (fitness functions)](#516-guard-scripts-fitness-functions)
  - [5.17 Deductive verification](#517-deductive-verification)
- [6. Summary table](#6-summary-table)
- [7. Suggested order of priority](#7-suggested-order-of-priority)
- [8. Open questions](#8-open-questions)
- [9. References](#9-references)

## 1. Overview and context

FokosDB tests today with unit tests, Durable Object integration tests, and a property-based suite
that grows. This document lists other techniques that can find defects of correctness and
reliability in FokosDB and in the Fokos Sharding Runtime. For each technique it gives:

- the tools or libraries to use,
- the complexity to implement it on the code of today,
- the return on effort,
- the maintenance cost when the code changes fast. The question is: does the technique continue to
  give value, or does it need its own changes all the time?

Section 7 gives an order of priority and the reasons for it.

`docs/ideas/testing.md` keeps the reading list. `docs/ideas/2026-09-22-deterministic-do-test-machinery.md`
makes the existing suites reliable. This document is about the techniques to add.

## 2. What the suites have today

| Tier | Where | What it proves |
| --- | --- | --- |
| Units | `src/**/*.test.ts` | Pure algorithms: key codec, intervals, frontier, bloom filter, expressions, stores |
| Flow tier | `test/repartition/repartition-flow.test.ts` | `RepartitionSource` and `RepartitionTarget` steps over real stores, with an explicit `now` and a peer adapter, no RPC |
| Generic runtime host | `test/sharding/counter-host.test.ts` | `FokosShardingRuntime` without FokosDB, including a source that aborts |
| Durable Object tier | `test/partition-do`, `test/transactions` | Real RPC, real SQLite, real alarms, with `ControlledPartitionDO` and `ControlledTransactionCoordinatorDO` |
| Property-based | `test/property-based/` | Stateless properties, model-based command runs, disjoint and contending transaction batches, splits during writes and queries |

The property-based suite already does **model-based testing** in the fast-check sense: `fc.commands`
drives the real database and an in-memory map at the same time. The contending batch uses a
**serializability oracle** on the final state: some order of the committed transactions must give
the final state of the pool.

Three limits of the current suite matter for this document:

1. **The network never fails.** Every RPC either answers or throws an error that the callee raised.
   No test loses a response after the callee applied the request, and no test delivers a request
   twice. For 2PC and for the migration pull, that ambiguous case is the dangerous one.
2. **The oracle sees only the final state.** The contending batch checks the state after the batch
   drains. It does not check the reads inside the batch, and it does not check real-time order.
   "Strongly consistent" is a promise of strict serializability, which is a claim about real time.
3. **The interleavings come from the machine, not from the test.** A concurrent run explores only the
   orders that workerd happens to choose. The same seed does not give the same interleaving, and a
   rare order does not come back.

## 3. The seams that decide the cost

The cost of each technique depends on the seams that the code has now. The facts below come from the
current tree.

**Seams that exist:**

- `FokosShardingRuntime` takes `stub(ctx, doName)` as a dependency (`src/sharding/runtime-types.ts`).
  The runtime never makes a stub itself. The counter host passes its own
  (`test/sharding/counter-host.ts`), thus a test host controls every hop of the runtime.
- `FokosRuntimeOptions.scheduler` has `fastPathDelayMs` and `fallbackAlarmMs`. `PartitionDO` passes
  neither, but a test host such as the counter host can pass them. A test host is not a production
  hook.
- `FokosScheduler.runDueWork()` is the one entry of all background work, and `alarm()` calls only it.
- `RepartitionSource` and `RepartitionTarget` take their dependencies (`identity`, `getPeer`, `hooks`,
  `scheduleWork`) in a constructor. The flow-tier harness already builds them over real storage.
- `TransactionParticipant` takes `store` and `now`. `TtlExpiry` takes `nowSec`.
- Both DO classes have `fokosNow()`, which a subclass can override.
- The Controlled DO subclasses can gate, count, answer, or fail one operation on one instance.
- The counter host has `debugAbort()`, which calls `ctx.abort()` and restarts the instance.
- Update expressions accept the array append selector `$[#]`. This makes a list-append workload for
  a history checker possible (section 5.4).

**Seams that do not exist:**

- `Date.now()` is still read directly in `src/sharding/scheduler.ts` (4 places),
  `src/sharding/sharding-store.ts` (`learnRangeBoundary`), `src/shared/transaction-limits.ts`
  (`txOrderTimestampNow`), and one deadline helper in `src/server/do-transaction-coordinator.ts`.
- `crypto.randomUUID()` makes the transaction id in the coordinator and the `error_id` of each error.
- `setTimeout` is used in the scheduler fast path, in `TtlExpiry`, and in `src/server/do-partition.ts`.
- `PartitionStore`, `FokosShardingStore` and `FokosScheduler` take a `DurableObjectStorage`. The
  coordinator uses `this.ctx.storage` directly in about 48 places.
- The 2PC state machine of the coordinator is in private methods of `TransactionCoordinatorDO`. No
  class like `RepartitionSource` holds it with injected dependencies.
- `ctx.abort()` on a coordinator hangs the workers pool
  (`src/server/do-transaction-coordinator.test.ts`). It works on the counter host.

## 4. How to read the ratings

- **Complexity**: S (days), M (one to two weeks), L (three to six weeks), XL (more than six weeks).
- **Return on effort**: the defects it can find that the current suites cannot find, divided by the
  cost. Low, Medium, High, Very high.
- **Sync cost**: how often the test machinery itself must change when the product code changes.
  Low means that the technique tests a contract that changes rarely (the public API, the RPC
  operation names, a protocol invariant). High means that it copies an internal detail.

## 5. The approaches

### 5.1 Invariant density in the production code

**What it is.** Assertions of the internal rules inside `src/`, in the TigerStyle sense: each state
transition asserts its precondition and its postcondition. `src/shared/invariant.ts` exists and has
about 88 call sites.

Candidate invariants:

- The coordinator never writes `CANCELLING` when the stored state is `PREPARED` or later.
- A commit or a cancel on a participant never finds a lock row with a different transaction id for
  the same key.
- A migration cursor only moves forward, and a page never holds a row that `belongsToTarget` refuses.
- A router never serves a `local` write for a key that it does not own.
- `QueryPageBudget` never goes below zero, and a cursor always points after the last item of its page.
- A version of an item only increases.

**Tools.** The existing `invariant()`. No library.

**Complexity.** S. It grows one assertion at a time.

**Return on effort.** Very high. Each assertion runs in every unit test, every property run, every
fault injection run and every simulation run. The techniques of this document find a bad state only
when a check looks at it. An assertion looks at it at the moment it occurs, and the stack trace
names the step. A property failure without assertions names only the final symptom.

**Sync cost.** Low. The assertion is part of the code. A change of the code changes the assertion in
the same diff. An assertion that has become false fails loudly, and does not rot silently.

**Tradeoff.** An assertion on a hot path costs CPU. Keep them on state transitions, not on per-row
loops, or compute the check only when a cheap condition is true.

### 5.2 Global invariants at quiescence

**What it is.** After a property run drains (no split in flight, no transaction in flight), a checker
reads every Durable Object of the table and asserts the rules that no single DO can check:

- Every key of the model is in exactly one leaf, and in the leaf that `resolveOwner` gives.
- No `pending_transactions` row remains, and no coordinator has a non-terminal row.
- The union of the items of all leaves is the model, with no extra row (an extra row is a row that a
  split did not clean, or a lost delete).
- Each router has no items that routing can reach.
- No job in `__fokos/jobs` is overdue by more than one pass.

The concurrent transaction suite already probes for a stuck lock with a write. This makes the check
direct and complete.

**Tools.** `runInDurableObject`, `listDurableObjectIds` of `cloudflare:test`, the `status` operation,
and the stores. One helper in `test/property-based/`.

**Complexity.** S to M.

**Return on effort.** High. It finds leaks that no read shows: an orphan lock that expires later, a
row that stays in a source after cleanup, a coordinator row that never reaches a terminal state.
These defects become cost and latency in production, and a public-API test does not see them.

**Sync cost.** Medium. The checker reads internal tables. Keep it on the `status` page and on the
store methods, not on raw SQL, so that a schema change touches the store and not the checker.

### 5.3 Fault injection at the RPC boundary (a nemesis)

**What it is.** A Jepsen-style nemesis inside the existing model-based runs. The command list of
`fc.commands` gets fault commands next to the operations:

| Fault | How | What it tests |
| --- | --- | --- |
| Fail before execution | The receiver throws before `super.op()` | Retries and error mapping |
| Fail after execution | The receiver calls `super.op()`, then throws a network-style error | Idempotency of `prepare`, `commit`, `cancel`, `fokosMigrationPull`, `fokosInit` |
| Duplicate delivery | The receiver calls `super.op()` twice | Idempotency without a retry of the caller |
| Delay | The receiver holds the call behind a gate | Reordering between two callers |
| Restart | `ctx.abort()` through a debug RPC, as `debugAbort()` does in the counter host | In-memory state loss: caches, the fast-path timer, a pass in flight |
| Alarm failure | The alarm throws once | The platform retry of an alarm |
| Clock jump | `fokosNow()` of a Controlled DO returns a later time | Stale recovery, the idempotency window, TTL |

The important point: **a fault must not change the answer.** The model stays the same in-memory
map. The run accepts only the errors that the harness accepts today (`FokosUnavailableError`,
`FokosTransactionPendingError`, and the ordering cancels). Thus the oracle needs no change, and every
existing model-based property becomes a fault property.

**Tools.** `ControlledPartitionDO` and `ControlledTransactionCoordinatorDO` (the "answer or error for
N calls" test control exists already; "fail after execution" and "duplicate" are two more modes),
fast-check `fc.commands`, and `runDurableObjectAlarm`.

**Complexity.** M. The fault modes are about 100 lines in the Controlled classes. The larger work is
the choice of the target DO from inside a command: a command must name "the partition that owns key
K now" or "the coordinator of token T".

**Return on effort.** Very high. The lost response after an applied `prepare` or `commit` is the most
common defect class of 2PC and of migration protocols. The idempotency rules in `AGENTS.md` ("prepare,
commit and cancel are idempotent") are claims that no test attacks today. The restart fault tests the
rule "every transition writes to SQLite before it sends an RPC".

**Sync cost.** Low. The faults target operation names, which `PartitionOps` declares once and which
change rarely. The oracle does not change. A new operation gets faults without a new test.

**Limits.** A restart of a coordinator hangs the workers pool today. Resolve that first, or restart
only partitions until it is resolved. The faults run at the receiver, thus a fault "on the way back"
is modeled as "fail after execution", which is the same for the caller.

### 5.4 History recording and a consistency checker

**What it is.** Record every operation of a concurrent run as a history: the invoke time, the
completion time, the arguments, and the result (or "unknown" for a timeout or a network error). Then
give the history to a checker that decides if some legal order explains it.

- For single keys: **linearizability** of a register per key (put, get, delete, conditional write).
- For transactions: **strict serializability** of `transactWriteItems` and `transactGetItems`. Use an
  Elle **list-append** workload: each item is a JSON array, each write appends a unique value with
  `$[#]`, and each read returns the whole list. The list gives Elle the version order for free, and
  Elle then finds cycles (G0, G1c, G2, and real-time violations) in time linear in the history.

This removes the need to write a model for the concurrent case. The checker is the model. The
current contending-batch oracle checks only the final state; a history checker also checks every read
inside the batch and the real-time order between batches.

**Tools.**

- [Elle](https://github.com/jepsen-io/elle) through [elle-cli](https://github.com/ligurio/elle-cli)
  (JVM, reads JSON histories). The strongest option for transactions.
- [Porcupine](https://github.com/anishathalye/porcupine) (Go) for per-key linearizability. A small Go
  program reads the JSON history.
- A TypeScript port of the Wing-Gong-Lowe search for per-key registers is about 200 lines, and it runs
  inside vitest. A search over one key with a few dozen operations is fast. Use it for the fast
  default runs, and use Elle in a deeper job.

The recorder runs inside workerd and writes JSON. The checker runs outside, in Node or the JVM, after
the vitest run, or reads the file that the test prints. A per-key TypeScript checker needs no outside
process.

**Complexity.** M. The recorder is a wrapper over `FokosDB` in the harness. The work is the list-append
workload and the export format.

**Return on effort.** High to very high. It is the only technique here that tests the headline claim,
"globally strongly consistent", in real time and with concurrent reads. Combined with section 5.3 it
tests the claim under faults, which is what Jepsen does.

**Sync cost.** Very low. It is black-box on the public API. The API changes rarely, and the checker
never changes.

**Limits.** Real time inside one isolate is the clock of the Worker. That clock is coarse in the
Workers runtime (it advances on I/O). An operation pair that overlaps in real time looks concurrent to
the checker, which is safe: the checker then accepts more orders, and it can miss a violation but it
never reports a false one.

### 5.5 Deterministic simulation at the flow tier and the runtime tier

**What it is.** Deterministic simulation testing (DST) runs the system under a scheduler that the test
owns. A seed decides every choice: which step runs next, which message arrives, which message is lost,
which node restarts, and what time it is. The same seed replays the same run, and fast-check shrinks a
failing run to a short one.

A full DST of workerd is not possible from inside the tests (section 5.6). Two tiers of the code are
already close to it, and they need no production change:

**The flow tier.** `test/repartition/repartition-harness.ts` already:

- builds real `RepartitionSource` and `RepartitionTarget` over real storage,
- passes an explicit `now` (`T0`),
- sends every peer call through a test adapter (`peer`), with no RPC,
- rebuilds both halves on every entry, which is a restart of all in-memory state.

The change is to make the adapter and the step order come from a seed:

1. A fast-check arbitrary draws a list of actions: "run one pass on node N", "deliver the next pending
   peer call of node N", "fail it before", "fail it after", "duplicate it", "advance `now` by D",
   "write key K on the source" (for the read-through and the refusal paths).
2. Pending peer calls go into a queue that the test owns. With `fc.scheduler()` (fast-check), every
   `await` of an adapter call resolves only when the scheduler lets it. fast-check then explores the
   order of the concurrent pulls and acknowledgements of the children, and it prints the order of a
   failure.
3. After each action the harness checks the flow invariants: no row is lost, no row is in two owners,
   routing and `belongsToTarget` agree, each import state only moves forward.
4. At the end the harness drives every node to quiescence and checks section 5.2.

**The runtime tier.** The counter host is a test host. It can:

- pass `scheduler: { fastPathDelayMs: <large>, fallbackAlarmMs: <large> }`, so no background work runs
  unless the test calls `runDueWork()`,
- pass a `stub()` that wraps the real stub with a fault layer that the seed controls,
- restart a node with `debugAbort()`.

Thus the test drives every pass and every hop of the generic runtime, including hash splits during
writes and a source that restarts in each phase. This tests `FokosShardingRuntime` as a product of its
own, which the sharding client spec (`docs/agent-plans/2026-09-26-fokos-sharding-client.md`) makes more
important.

**Tools.** fast-check (`fc.commands`, `fc.scheduler()`, `fc.scheduledModelRun`), the flow-tier
harness, the counter host. Nothing new to install.

**Complexity.** M for the flow tier. M for the runtime tier. Each is about one to two weeks.

**Return on effort.** Very high for the Sharding Runtime. The repartition flow (about 1300 lines) and
the runtime (about 1850 lines) are the most complex code of the repository, and their defects are
orders of steps. The Durable Object tier can reach only the orders that workerd chooses. This tier
reaches the rare orders on purpose, runs thousands of them in seconds, and replays each failure from a
seed. It removes the "a test that measures the clock" class of section 1 of the test machinery idea,
because time is an input.

**Sync cost.** Medium. The flow tier depends on the step API of `RepartitionSource` and
`RepartitionTarget`. Two rules keep the cost low:

- Drive through `runDueWork()` and the job list, not through named steps. A new job then runs in the
  simulation without a change of the simulator.
- Draw faults per peer method from the `FokosRepartitionPeer` type. A new peer method gets faults
  automatically when the adapter wraps every method of the object.

**Limits.** Each step still runs on real storage in workerd, thus the storage is not faulted. That is
acceptable: SQLite in a Durable Object is transactional, and the protocol risk is between steps, not
inside one. The coordinator gets no flow tier until its state machine moves into a class with injected
dependencies, like `RepartitionSource`. That refactor is a production change, but not a test hook: it
gives the coordinator the same shape as the repartition flow.

### 5.6 Full deterministic simulation of FokosDB

**What it is.** The TigerBeetle or FoundationDB approach: run many PartitionDOs, coordinators and
clients in one process, on simulated storage, a simulated network, a simulated clock, and a seeded
random source. Every run is exactly repeatable, and the simulator can run millions of operations a
minute on one core.

**What it needs from the code** (from section 3):

- One clock for all of `src/`: the four `Date.now()` sites of the scheduler, `learnRangeBoundary`,
  `txOrderTimestampNow`, and the coordinator deadline helper.
- One random source for the transaction id and the `error_id`.
- One timer source for the scheduler fast path, `TtlExpiry` and `do-partition.ts`.
- A shim of `DurableObjectStorage` (`sql.exec` over `node:sqlite` or `sql.js`, `kv`, `transactionSync`,
  `getAlarm` and `setAlarm`), and of `DurableObject` from `cloudflare:workers`.
- A simulated RPC that copies the Workers semantics: structured clone of arguments, the loss of the
  error class (the tagged error RPC exists for this), and the input and output gates of a Durable
  Object.

**Tools.** None off the shelf for Durable Objects. The design references are the TigerBeetle VOPR and
the FoundationDB simulator. fast-check can own the seed and the shrink.

**Complexity.** XL. The clock, random and timer seams are small (days), and follow the direction of
the `fokosNow()` change. The platform shim is the large part.

**Return on effort.** High in the long run, but Medium now. The fidelity risk is large: the code
depends on the input and output gates ("every local write commits before the first `await`"), on
`blockConcurrencyWhile`, and on alarm retry semantics. A shim that models these wrongly gives false
confidence, and a defect of the shim looks like a defect of the product. Sections 5.3 and 5.5 give most
of the value on the real runtime.

**Sync cost.** Low for the product code once the seams exist (they are one-time changes). Medium for
the shim, which follows the Durable Objects API, and that API changes more slowly than FokosDB.

**Recommendation.** Do the clock, random and timer seams now. They are cheap, they help every other
tier (a Controlled DO can then override one clock), and they keep this option open. Build the shim
only if sections 5.3 and 5.5 still leave defects that need a whole-system order to reproduce.

### 5.7 Antithesis

**What it is.** A commercial platform that runs containers inside a deterministic hypervisor. It
explores faults and orders, and it replays any run exactly. It needs no change of the code under test.

**How it fits.** Run `workerd` with the `examples/http-api` worker in one container, and a workload
client in a second container. The workload is the history recorder of section 5.4, and the checker is
Elle. Antithesis adds network faults, pauses and clock changes between the two, and restarts the
workerd process.

**Tools.** [Antithesis](https://antithesis.com/), its JavaScript SDK for assertions in the workload
client (the SDK targets Node.js, not workerd), Docker Compose.

**Complexity.** M for the setup. The cost is money, not code.

**Return on effort.** High if the budget exists. It is the only way to get a deterministic
whole-system run of the real workerd binary. It does not see inside one workerd process as a set of
Durable Objects, thus faults between two DOs of one process are not injected; only process-level and
client-level faults are.

**Sync cost.** Very low. It is black-box.

### 5.8 Formal specification and model checking

**What it is.** A model of the protocols, not the code, and a model checker that explores every order
of the model up to a bound. The protocols of FokosDB that deserve a model:

1. **2PC with splits.** Coordinator states, participant locks, stale recovery, the idempotency window,
   the `guarded_at` quarantine, `txCancel` in `beforeForward`, a lock that moves to a child during a
   migration, and a coordinator that splits by hash.
2. **Repartition.** Queue, plan, init, cutover, import pages, read-through, acknowledgement, cleanup,
   and promotion before a split.
3. **Read transactions.** The two reads and the `deleteRevision` comparison.

Invariants to check: atomicity, "`PREPARED` is the point of no return", no lost lock, exactly one
owner per key, no lost row across a split, a read transaction never returns a mixed snapshot, and
liveness (every prepared transaction commits eventually, every split completes eventually under
fair scheduling).

Several agent plans fix defects that a model finds before the code exists:
`2026-09-20-over-size-partition-split-deadlock.md` (a deadlock), `2026-09-09-bounded-preparing-hold.md`
(a liveness bound), and `2026-09-20-query-entry-point-into-a-range-tree.md` (a routing defect). A
model checker finds deadlocks and liveness defects directly, and tests find them late.

**Tools.**

| Tool | Language | Strengths | Weaknesses for this project |
| --- | --- | --- | --- |
| [FizzBee](https://fizzbee.io/) | Python-like (Starlark) | Fastest to learn. Explores interleavings. Has a model-based testing mode with TypeScript scaffolding, and a skill for coding agents. | Younger tool, smaller community. The MBT runner calls the system from Node, but FokosDB runs in workerd (section 5.9). |
| [Quint](https://quint.sh/) | TypeScript-like, TLA semantics | Syntax the team reads. Random simulator, and `quint verify` through Apalache. ITF traces, and [quint-connect-ts](https://github.com/dearlordylord/quint-connect-ts). | Symbolic checks are slow on large state. quint-connect-ts needs Node, not workerd. |
| TLA+ / PlusCal with TLC or Apalache | TLA+ | The most mature. Many published models of 2PC and Paxos to start from. Trace validation exists. | Steepest learning curve. Syntax far from TypeScript. |
| [P](https://p-org.github.io/P/) | C#-like state machines | Used at AWS for S3 and DynamoDB. PObserve checks production logs against the same spec. | .NET toolchain. Less common outside AWS. |

**Complexity.** M for the first model (2PC with one split), including the time to learn the tool. Each
later protocol is S to M.

**Return on effort.** High for the design, especially before a protocol change. The README lists
protocol changes that are coming: writes during migration, a topology keeper, CASPaxos partitions for
multi-region availability, transactional reads during a migration. Each of these is a place where a
model gives the most value before the code.

**Sync cost.** Low for code changes, Medium for protocol changes. A model describes the protocol, so a
refactor, a rename or a new error code does not touch it. A protocol change must update the model,
and that is the point: make the model part of the agent plan of each protocol change, and let the
spec-review skill ask for it. A model that no plan updates rots quietly, because nothing fails when it
drifts. Section 5.9 closes that gap.

**Recommendation.** Start with FizzBee or Quint for the 2PC-with-splits model. FizzBee gives the
fastest first result and is already in `docs/ideas/testing.md`. Quint gives trace export that the
workerd tests can replay (section 5.9). Pick one and do not model everything: model the protocol that
changes next.

### 5.9 A link from the model to the code (MBT and trace validation)

**What it is.** Two ways to keep a formal model and the code in agreement, so that the model cannot
drift silently.

- **Model to code (model-based testing).** The model checker generates traces (sequences of actions and
  expected states). A driver replays each trace on the real system and compares the state after each
  step. FizzBee MBT and quint-connect do this. The tools run the driver in Node, and FokosDB runs in
  workerd. The practical path: generate the traces offline (`quint run --mbt --out-itf` gives JSON),
  commit a corpus, and replay the corpus inside vitest in workerd with a small ITF reader. The Controlled
  DOs and the flow-tier harness give the driver the control points it needs.
- **Code to model (trace validation).** The code logs each state transition as a structured event (the
  coordinator already writes each transition to SQLite before its RPC). A checker reads the events of
  every test run and asks if the model allows that sequence. This is what PObserve does for P, and what
  TLA+ trace validation does. It uses every existing test as input, and it needs no new driver.

**Tools.** Quint ITF traces and an ITF reader, FizzBee MBT, TLA+ trace validation, P and PObserve.

**Complexity.** M to L. The driver or the event mapping is the refinement map from the code to the
model. That map is the hard part.

**Return on effort.** Medium now, High once a model exists and the protocols stabilize.

**Sync cost.** Medium to High. The refinement map touches the names of internal states and events.
Trace validation is cheaper than MBT here: a log event per transition is small and changes with the
transition.

**Recommendation.** Wait until a model exists and the protocol it models stops changing every week.
Then prefer trace validation of the coordinator transitions.

### 5.10 Metamorphic properties

**What it is.** A property that compares two runs of the system with each other, and needs no model.
Examples for FokosDB:

- A query drained with `limit: n` gives the same sequence as the same query drained with `limit: m`.
- A query in reverse order gives the reverse sequence.
- A projection gives a subset of the full item, field by field.
- A `count` equals the length of the drained projection.
- The same operations on a table with `rootTreesN: 1` and on a table with `rootTreesN: 5` (or with and
  without splits) give the same answers. The active-split suite already uses this idea.
- A filter `A AND B` gives the intersection of the results of `A` and `B`.

**Tools.** fast-check. Nothing new.

**Complexity.** S per property.

**Return on effort.** Medium to High for the query and expression engine.

**Sync cost.** Very low. A metamorphic property states a relation of the public API, and no model has
to follow the code. This is the answer to "a model must change all the time": when a feature is too new
to model, a metamorphic property still holds.

### 5.11 Differential testing

**What it is.** Run the same input on two implementations and compare.

- **The expression engine against a JavaScript reference evaluator.** SQLite evaluates filters,
  conditions, projections and updates. A small reference evaluator in test code gives a second answer
  for random expressions over random documents. Every disagreement is a defect of the compiler, of the
  reference, or an undocumented semantic.
- **FokosDB against DynamoDB Local.** The API follows DynamoDB. A differential run finds the places
  where the semantics differ. Many differences are intentional (binary key order, limits, error codes),
  thus a normalization layer is necessary.

**Tools.** fast-check, the expression types and `test-fixtures.ts`, DynamoDB Local (Docker, Java).

**Complexity.** M for the expression reference. M to L for DynamoDB Local, because of the
normalization and because DynamoDB Local runs outside workerd.

**Return on effort.** High for the expression engine, which is large (the compiler is about 1100
lines) and has many corner cases of types and paths. Low to Medium for DynamoDB Local, because the
normalization hides much of the signal.

**Sync cost.** Medium for the reference evaluator: each new expression function needs its reference.
The cost is small per function, and the reference is also a clear specification of the function. High
for DynamoDB Local.

### 5.12 Mutation testing

**What it is.** A tool changes the production code in small ways (a `<` becomes `<=`, a condition
becomes `true`, a statement goes away) and runs the tests. A change that no test detects is a gap.

This matters more than usual here: coding agents write many of the tests. A test that runs the code but
asserts too little looks like coverage and is not. Mutation testing measures the assertions, not the
lines.

**Tools.** [StrykerJS](https://stryker-mutator.io/) with the vitest runner. The vitest runner and the
Cloudflare pool can conflict; if they do, use the Stryker `command` runner, which runs any command
and reads its exit code. It is slower, so scope it.

**Complexity.** S to M. The setup is small. The run time is the cost: the suite takes about a minute,
thus scope each run to one file and to the tests that cover it.

**Return on effort.** Medium to High as a periodic audit. The first targets: `do-transaction-coordinator.ts`,
`repartition-flow.ts`, `transaction-participant.ts`, `range-frontier.ts`, `sk-interval.ts`,
`page-budget.ts`, `key-codec.ts`.

**Sync cost.** None. It needs no test code. Run it on demand or weekly, not on every change.

### 5.13 Fuzzing of untrusted input

**What it is.** Random or coverage-guided input to the functions that decode data from a client: the
query cursor, key bytes, expressions and their bindings, the partition context, and the envelope. A
cursor is opaque to the client but it comes back from the client, thus it is untrusted input. A decoder
must refuse bad input with a validation error, never with an internal error or a wrong answer.

**Tools.** fast-check with large run counts (already present; enough for most of these), or
[Jazzer.js](https://github.com/CodeIntelligenceTesting/jazzer.js) for coverage-guided fuzzing in Node.
Jazzer.js runs in Node, thus only the pure modules qualify.

**Complexity.** S with fast-check. M with Jazzer.js.

**Return on effort.** Medium. The surface is small and the defects are mostly robustness defects, but
a tampered cursor that reads outside its range is a security defect.

**Sync cost.** Low. Decoders change rarely.

### 5.14 Soak and chaos on the real platform

**What it is.** A long run against a deployed worker on Cloudflare, with the history recorder of
section 5.4 in the load client and a checker at the end. The platform supplies faults that miniflare
does not: DO eviction, a DO reset at each deploy, `overloaded` errors, colo placement, real network
latency, and real alarm retry. A deploy during the run is a free restart of every DO.

**Tools.** k6 (`tools/k6_basic.js` exists) or a Node client, the `examples/http-api` worker, Elle for
the history, Workers Analytics Engine or logs for the latency and error rates.

**Complexity.** M.

**Return on effort.** Medium to High. It is the only test of the gap between miniflare and the real
platform. It is slow and it costs money, so it runs nightly or before a release, not on every change.

**Sync cost.** Low. Black-box.

### 5.15 Upgrade and compatibility testing

**What it is.** Build the previous release, create a table and write to it (with splits in flight and
transactions in flight), then stop, start the new build on the same persisted state, and continue the
model-based run. A second variant runs an old client against a new DO, because a gradual deployment of
Workers runs two versions at the same time.

It tests the SQLite schema migrations of each DO, the `__fokos/` records, the wire types, and the
stored `PartitionContext`.

**Tools.** A git worktree of the previous tag, `pnpm build` of each, miniflare persisted state
(`.wrangler/state`), the property harness.

**Complexity.** M.

**Return on effort.** Low now, because the README says breaking changes continue and no data must
survive. Very high from the first stable release: a split or a transaction in flight during a deploy
is the normal case in production.

**Sync cost.** Low. The test compares release N-1 with release N. It never names the change.

### 5.16 Guard scripts (fitness functions)

**What it is.** Small scripts that fail the build when a structural rule breaks. The repository has two:
`tools/check-key-invariants.sh` and `tools/check-test-machinery.js`. More rules that support the
techniques above:

- No `Date.now()`, `crypto.randomUUID()` or `setTimeout` in `src/` outside one clock, random and timer
  module (after section 5.6 adds them). This keeps the simulation seams from rotting.
- No `this.ctx.storage.sql.exec` on a `fokos_` table outside `src/sharding/`.
- Every public method of `PartitionDO` is one `this.fokos.dispatch(...)` call.
- Every operation name in `PartitionOps` has at least one fault test (section 5.3).

**Tools.** Node scripts with no dependencies, or custom oxlint rules.

**Complexity.** S.

**Return on effort.** High for the cost. Each rule stops a class of defect forever.

**Sync cost.** Low. A rule changes only when the architecture changes.

### 5.17 Deductive verification

**What it is.** A proof that the code meets a specification for every input, in a tool such as Dafny,
Lean, Why3 or Verus.

**Fit.** No production-ready deductive verifier for TypeScript exists. A proof needs a port of the code
into the verifier language, and the port drifts from the TypeScript on the next change. The only
candidates are small pure kernels that change rarely: the order preservation of `KeyCodec`, and the
interval algebra of `sk-interval.ts`. The property tests already cover both well, and `key-codec.test.ts`
compares the order with SQLite.

**Complexity.** L. **Return on effort.** Low. **Sync cost.** High.

**Recommendation.** Do not do this. Model checking (section 5.8) gives the value of formal methods at
the protocol level, where the risk is.

## 6. Summary table

| # | Approach | Tools | Complexity | Return on effort | Sync cost |
| --- | --- | --- | --- | --- | --- |
| 5.1 | Invariant density | `invariant()` | S | Very high | Low |
| 5.2 | Global invariants at quiescence | `cloudflare:test`, stores, `status` | S–M | High | Medium |
| 5.3 | RPC fault injection (nemesis) | Controlled DOs, `fc.commands` | M | Very high | Low |
| 5.4 | History + consistency checker | Elle / elle-cli, Porcupine, TS WGL checker | M | High–Very high | Very low |
| 5.5 | DST at the flow and runtime tiers | fast-check `fc.scheduler`, flow harness, counter host | M + M | Very high | Medium |
| 5.6 | Full DST of FokosDB | custom simulator | XL | Medium now, High later | Low (code), Medium (shim) |
| 5.7 | Antithesis | Antithesis, Docker Compose | M + money | High | Very low |
| 5.8 | Model checking | FizzBee / Quint / TLA+ / P | M | High | Low (code), Medium (protocol) |
| 5.9 | Model-to-code link | Quint ITF, FizzBee MBT, trace validation | M–L | Medium, High later | Medium–High |
| 5.10 | Metamorphic properties | fast-check | S | Medium–High | Very low |
| 5.11 | Differential (expression reference) | fast-check, a JS evaluator | M | High | Medium |
| 5.11 | Differential (DynamoDB Local) | DynamoDB Local | M–L | Low–Medium | High |
| 5.12 | Mutation testing | StrykerJS | S–M | Medium–High | None |
| 5.13 | Fuzzing of untrusted input | fast-check, Jazzer.js | S–M | Medium | Low |
| 5.14 | Soak and chaos on Cloudflare | k6, Elle, http-api | M | Medium–High | Low |
| 5.15 | Upgrade and compatibility | git worktree, persisted miniflare state | M | Low now, Very high at release | Low |
| 5.16 | Guard scripts | Node scripts, oxlint | S | High | Low |
| 5.17 | Deductive verification | Dafny, Lean | L | Low | High |

## 7. Suggested order of priority

The order follows three rules:

1. **Multiply what exists before adding a new system.** The property-based suite, the Controlled DOs
   and the flow-tier harness are strong assets. A technique that turns each existing test into a
   stronger test beats a technique that needs a new test tree.
2. **Prefer an oracle that does not follow the code.** With changes every day, an oracle that is the
   public API (a history checker, a metamorphic relation, "a fault must not change the answer") keeps
   its value. An oracle that copies internal states must change with them.
3. **Attack the claims that no test attacks today.** Idempotency under a lost response, strict
   serializability in real time, and rare orders of the repartition steps.

**Phase 1: cheap multipliers (about one week)**

1. **Invariant density (5.1)** and **guard scripts (5.16)**. Each assertion makes every later technique
   report the first bad step instead of the last symptom. Start on the coordinator transitions, the
   participant lock rows, and the migration cursor.
2. **The clock, random and timer seams** of section 5.6, without the simulator. They are small, they
   continue the `fokosNow()` direction, and a Controlled DO can then move time for one instance. Add
   the guard rule that holds them.

**Phase 2: faults and a real oracle (two to four weeks)**

3. **RPC fault injection (5.3)**. The highest value for the cost: two more modes in the Controlled DOs
   and fault commands in the existing model-based runs. The oracle does not change. It attacks
   the idempotency of 2PC and of the migration pull, which no test attacks today.
4. **Global invariants at quiescence (5.2)**. It runs at the end of every fault run and finds the
   leaks that a fault leaves behind: orphan locks, rows left in a source, coordinators that never
   finish.
5. **History recording and a consistency checker (5.4)**. First a per-key linearizability checker in
   TypeScript inside vitest, then an Elle list-append workload for transactions in a deeper job. It
   tests the headline claim, it is black-box, and it never needs a change.

**Phase 3: deterministic orders for the Sharding Runtime (two to four weeks)**

6. **DST at the flow tier and the runtime tier (5.5)**. The Sharding Runtime is the most complex code
   and now a product of its own. The flow harness and the counter host already have the seams; fast-check
   already has the scheduler. Drive through `runDueWork()` and wrap every peer method, so that a new job
   or a new peer method needs no change of the simulator.

**Phase 4: design-level checks, on the next protocol change**

7. **A formal model of 2PC with splits (5.8)**, in FizzBee or Quint. Write it as part of the next agent
   plan that changes a protocol (writes during migration, the topology keeper, or transactional reads
   during a migration), not as a separate project. It is the only technique that finds a deadlock or a
   liveness defect before the code exists.

**Phase 5: periodic audits, from now on**

8. **Mutation testing (5.12)**, scoped to the files of section 5.12, weekly or on demand. It audits
   the tests that agents write, at no maintenance cost.
9. **Metamorphic properties (5.10)** and **the expression reference evaluator (5.11)**, each time a
   change touches the query or expression code.
10. **Fuzzing of the cursor and the other decoders (5.13)** with fast-check, when the cursor format
    changes next.

**Phase 6: before the first stable release**

11. **Soak and chaos on Cloudflare (5.14)** with the history checker of step 5 in the load client.
12. **Upgrade and compatibility testing (5.15)**. From the first release that promises durable data,
    this becomes a gate for each release.

**Later, only if the need shows**

13. **A model-to-code link (5.9)**, once a model exists and its protocol has stopped changing weekly.
    Prefer trace validation of the coordinator transitions.
14. **Full DST of FokosDB (5.6)** or **Antithesis (5.7)**, if phases 2 and 3 still leave defects that
    only a whole-system order reproduces, or if flakes of the Durable Object tier remain the main cost.
    Antithesis if a budget exists, because it needs no platform shim and has no fidelity risk.

**Not recommended:** deductive verification (5.17) and differential testing against DynamoDB Local
(5.11). The first drifts from the code on each change. The second spends most of its effort on
intentional differences.

## 8. Open questions

1. Can a coordinator restart without a hang of the workers pool? Fault injection of restarts on the
   coordinator (5.3) depends on it. The counter host restarts without a hang, thus the difference is
   worth a measurement.
2. Does the StrykerJS vitest runner work with `@cloudflare/vitest-plugin`, or is the `command` runner
   necessary?
3. What is the resolution of the clock inside the vitest workerd isolate? The history checker (5.4)
   treats a coarse clock safely, but a finer clock finds more violations.
4. FizzBee or Quint? Decide after one day with each on the same small model of the coordinator states.

## 9. References

- `docs/ideas/testing.md`
- `docs/ideas/2026-09-22-deterministic-do-test-machinery.md`
- `docs/ideas/2026-09-25-test-ownership-and-shared-factory.md`
- `test/property-based/harness.ts`, `test/repartition/repartition-harness.ts`, `test/sharding/counter-host.ts`
- `test/controlled-partition-do.ts`, `test/controlled-transaction-coordinator-do.ts`
- `src/sharding/scheduler.ts`, `src/sharding/runtime-types.ts`
- [fast-check](https://github.com/dubzzz/fast-check): `fc.commands`, `fc.scheduler()` and `fc.scheduledModelRun` (see its race condition guide)
- [Elle](https://github.com/jepsen-io/elle) and [elle-cli](https://github.com/ligurio/elle-cli)
- [Porcupine](https://github.com/anishathalye/porcupine)
- [FizzBee model-based testing](https://fizzbee.io/testing/)
- [Quint model-based testing](https://quint-lang.org/docs/model-based-testing) and [quint-connect-ts](https://github.com/dearlordylord/quint-connect-ts)
- [P](https://p-org.github.io/P/) and [Systems Correctness Practices at AWS](https://queue.acm.org/detail.cfm?id=3712057)
- [Antithesis JavaScript SDK](https://antithesis.com/docs/using_antithesis/sdk/javascript_sdk/)
- [StrykerJS](https://stryker-mutator.io/)
- [Jazzer.js](https://github.com/CodeIntelligenceTesting/jazzer.js)
- [TigerStyle](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md)
