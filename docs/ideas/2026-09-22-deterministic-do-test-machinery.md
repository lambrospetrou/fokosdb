# Deterministic Test Machinery for the Durable Object Suites

Status: **draft**. Written on 2026-09-22 after two flakes of the same week: a migration hold that a
sibling test removed, and a property suite that ran out of its retry budget under load. Both tests
were correct. The machinery under them measures the speed of the machine, and it keeps its test
controls on state that the whole isolate shares.

## 1. The problem

Two failures of the test machinery, and not of the code under test, occurred in one week.

**A test control that the whole isolate shares.** `withMigrationHeld` in
`test/partition-do/partition-harness.ts` held a migration open with `vi.spyOn` on
`PartitionDO.prototype.fokosMigrationPull`. The RPC dispatcher looks for an operation on the class,
thus the hold could not go on the DO instance. Three facts then removed it:

- `vitest.config.ts` sets `restoreMocks`, thus vitest calls `vi.restoreAllMocks()` around each test.
- `test/partition-do/query-items.test.ts` was `describe.concurrent`, and `concurrent` goes to the
  nested suites.
- The test that holds the migration is sequential, but a test of a *sibling* suite operates at the
  same time.

A sibling test started, vitest restored all spies, and the hold disappeared. The pulls went to the
real method, the hold recorded no child, and the deadline reported
`migration RPC not received from <both children>`. The failure looked like a defect of the split for
two weeks, and it is not one.

**A deadline that measures the clock.** The full suite puts 330 s of test time into 57 s of wall
clock. Thus the same work needs 2 to 6 times more time than it needs alone. Each deadline in the
machinery is a guess about that speed:

| Deadline | Value | Where |
| --- | --- | --- |
| split completion | 15 s | `drainUntil` in `test/partition-do/partition-harness.ts` |
| migration hold | 30 s | `withMigrationHeld` in the same file |
| churn retry | 40 x 25 ms = 1 s | `CHURN_RETRY_LIMIT` in `query-items-active-split.test.ts` |
| test timeout | 45 s | `vitest.config.ts` |

The churn budget failed twice in three full-suite runs on 2026-09-22. Each failure is a
`fokos/partition_migrating` error from `#deleteItem` in `src/client/db.ts`: the migration window was
longer than one second under load. The file passed alone each time, in 32 s.

A failure of this kind carries no information. The code is correct, and the clock ran out.

## 2. The current machinery

| Tier | Tests | Location | Time | Flakes |
| --- | --- | --- | --- | --- |
| units beside the code | 762 | `src/**/*.test.ts` | ms | none recorded |
| Durable Object integration | ~295 | `test/partition-do`, `test/transactions`, `test/repartition` | s | most of them |
| property-based | 16 | `test/property-based/` | ~30 s each | load flakes |

The machinery itself is small: 9 `vi.waitFor` deadlines, 12 alarm runs, 10 sleeps and 34 `vi.spyOn`
calls in the whole `test/` tree. Four of the nine deadlines are in `partition-harness.ts`, and they
carry nearly every Durable Object test. One change in that file reaches the whole tier.

Three facts of the product make a deterministic drive possible. They are true today, and no change to
`src/` is necessary to use them.

- **One scheduler drives all background work.** `FokosScheduler` in `src/sharding/scheduler.ts` runs
  the import, acknowledgement, repartition and cleanup jobs. Two passes never interleave. Each step is
  bounded and idempotent, and it reports its next run.
- **The pass is callable.** `FokosShardingRuntime.runDueWork` is public, `PartitionDO.fokos` is
  public, and `alarm()` calls `runDueWork()` and nothing else. Thus
  `runInDurableObject(stub, (p) => p.fokos.runDueWork())` runs exactly what an alarm runs.
- **The work that remains is durable.** Each job writes its next run to `__fokos/jobs`. A test can
  read that record and know if a partition has work that is due.

The runtime also wakes itself with a 50 ms fast path, and it arms a fallback alarm 5 s ahead
(`DEFAULT_FAST_PATH_DELAY_MS` and `DEFAULT_FALLBACK_ALARM_MS` in `src/sharding/runtime.ts`).
`PartitionDO` passes no scheduler options, thus these two values apply to every test.

## 3. Goals and scope

### 3.1 In scope

- A test fails when the system stops, and not when the machine is slow.
- No test control is on state that a different test can reach. A test control belongs to one
  Durable Object instance.
- The suites stay safe when a person makes a file concurrent again.
- A rule that the machinery holds is enforced by a check, and not only by a comment.
- The machinery stays small enough that one person can read all of it.

### 3.2 Out of scope

- A production hook for a test. `AGENTS.md` forbids it, and no part of this document needs one.
- Global fake timers. They run a Durable Object callback in the wrong I/O context, which `AGENTS.md`
  records.
- Deterministic simulation testing of the whole database. `docs/ideas/testing.md` keeps that subject.
  This document makes the existing suites reliable.
- A change of what the suites test. Each test keeps its assertions.

## 4. Milestones

Each milestone delivers a result on its own, and the next one does not wait for it.

1. **Progress-based waiting** (section 5.2). The harness drives the scheduler and fails on a stall.
   It reaches every Durable Object test and touches no test file.
2. **Test controls on one instance** (section 5.3). A test-only subclass replaces the prototype
   mocks. It removes the class of failure of section 1.
3. **The guard script** (section 5.6). It holds milestones 1 and 2.
4. **Tier migration** (section 5.4), as each area is touched. No fixed date.
5. **Budgets as deadlines** (section 5.5), at the next failure of the property suite.

Progress: milestones 1, 2 and 3 are done.

- The harness drives the scheduler and fails after 20 idle rounds.
- The partition test controls are on `ControlledPartitionDO` in `test/controlled-partition-do.ts`.
  The coordinator test controls are on `ControlledTransactionCoordinatorDO` in
  `test/controlled-transaction-coordinator-do.ts`.
- `tools/check-test-machinery.js` holds the rules. Its `PROTOTYPE_SPY_EXCEPTIONS` list has one
  entry, `test/partition-do/promotion.test.ts`. Section 8.2 gives the reason.
- Milestone 4 started. The resume of a truncated migration page is a test of the flow tier, in
  `test/repartition/repartition-flow.test.ts`. It replaces the Durable Object test that capped each
  page at one row, and `withMigrationBatchCap` is removed.
- Milestone 5 is done. The churn of `query-items-active-split.test.ts` refuses a write that meets
  `partition_migrating`, and its retry for other unavailable errors has a deadline of 10 s.

## 5. Proposed solution

### 5.1 High-level overview

The machinery changes on two axes. On the time axis, a test drives the background work instead of a
wait for it. On the control axis, a test changes one object instead of one class.

```
today                                   proposed

test ──poll every 100 ms──> status      test ──run one pass──> each node
     ──run alarm if armed──> tree             ──read status + __fokos/jobs
     fails when 15 s pass                     fails when a full round changes nothing

test ──vi.spyOn(Class.prototype)        test ──rpc──> ControlledPartitionDO instance
     visible to every test in the             a field of one object
     isolate; restoreAllMocks removes it      nothing else can reach it
```

A round of work, and not a period of time, becomes the measure of progress. The machine speed changes
the time a split needs. It does not change the number of passes the split needs.

### 5.2 Wait for progress, not for the clock

`drainUntil` and each `await*` helper of `TestPartition` change to one loop:

1. Test the condition. Return when it holds.
2. Take a fingerprint of the tree: the status and the `__fokos/jobs` record of each node.
3. Run one scheduler pass on each node with `p.fokos.runDueWork()`.
4. Compare the fingerprint. A change, or a job that is due, resets the idle count. If neither exists,
   increase the idle count.
5. Fail when the idle count reaches its limit, and report the state of each node, as the deadline
   reports it today.

```ts
// A round of work, and not a period of time, is the measure. The machine speed changes the time a
// split needs. It does not change the number of passes.
async function driveUntil(tree: TestPartition[], check: () => Promise<boolean>, label: string): Promise<void> {
	for (let idle = 0; idle < IDLE_ROUNDS_BEFORE_FAILURE; ) {
		if (await check()) return;
		const before = await treeFingerprint(tree);
		for (const node of tree) await node.runDueWork();
		idle = (await treeFingerprint(tree)) === before && !(await anyJobDue(tree)) ? idle + 1 : 0;
	}
	throw new Error(`${label} made no progress: ${await report(tree)}`);
}
```

The wall-clock limit stays as a backstop, at the test timeout of `vitest.config.ts`. Only a true stall
reaches it, thus its value stops a run that hangs and it does not decide a correct run.

Tradeoff: the loop drives the same DO that the runtime also drives with its alarm and its 50 ms fast
path. The scheduler makes that safe, because it runs one pass at a time and it queues the second one.
The test keeps no assumption that it is the only driver.

Cost: about 40 lines in `test/partition-do/partition-harness.ts`. No test file changes.

### 5.3 Put the test controls on a test-only subclass

`test/worker-entry.ts` already exports `CustomPartitionDO extends PartitionDO` with its own binding,
and `src/client/db.test.ts` runs its whole suite over it. The test controls use the same pattern:

```ts
// test/worker-entry.ts
// A test drives the test controls of this class through its own RPCs. The class adds no behavior of its own:
// `PartitionDO` does all the work, and each override only gates or counts.
export class ControlledPartitionDO extends PartitionDO {
	#pullGate: PullGate | null = null;
	#pullCalls = 0;

	override async fokosMigrationPull(req: FokosMigrationPullRequest): Promise<FokosMigrationPage> {
		this.#pullCalls++;
		if (this.#pullGate?.matches(req)) await this.#pullGate.held;
		return await super.fokosMigrationPull(req);
	}

	async testHoldPulls(spec: PullGateSpec): Promise<void> { /* … */ }
	async testReleasePulls(): Promise<void> { /* … */ }
	async testPullStats(): Promise<{ calls: number }> { /* … */ }
}
```

The gate is a field of one instance. No other test can reach it, `vi.restoreAllMocks()` does not know
it, and the dispatcher finds the method because it is on the prototype of the subclass.

The `PartitionContext` carries the namespace in `ctx.ns`, and each child of a split inherits it. Thus
a tree that starts in `CONTROLLED_PARTITION_DO` stays in that namespace, and the source that a child
pulls from is the same class.

`withMigrationHeld` keeps its signature. It loses
`replaceMigrationPull`, `vi.spyOn`, and the `installed()` invariant that reports a lost hold.

Tradeoff: those tests then measure a subclass, and not `PartitionDO` itself. The repository accepts
this for `CustomPartitionDO` already. The subclass overrides only the operations it must gate, and
each override calls `super`.

Cost: one class in `test/worker-entry.ts`, one binding and one `new_sqlite_classes` entry in
`packages/fokosdb/wrangler.jsonc`, and a namespace switch in the harness. About 100 lines.

The transaction suites use the same pattern. `makeDB({ controlled: true })` in
`test/transactions/tx-helpers.ts` puts a table on the two controlled classes, with a pool of one
coordinator, thus a test can reach that coordinator by its name. The test controls are these:

| Class | Test control | Use |
| --- | --- | --- |
| `ControlledPartitionDO` | a log of the requests of each `tx*` operation | counts the RPCs of a path |
| `ControlledPartitionDO` | an answer or an error for an operation, for N calls or until cleared | a partition that cannot execute a set, or that is unreachable |
| `ControlledPartitionDO` | a hold after the phase-one read of `txReadForTransaction` | a real mutation between the two phases |
| `ControlledPartitionDO` | the stale-transaction time | a short stale threshold |
| `ControlledTransactionCoordinatorDO` | a count of `initiateWrite` calls, and the fan-out budget | the coordinator path and a short budget |

A call with no rule returns the promise of `PartitionDO` itself, thus its timing does not change.

### 5.4 Move the observation down a tier

Some tests of the Durable Object tier ask a question that holds no Durable Object. The first one
asked if a truncated migration page resumes after its last row. It capped each page at one row with
`withMigrationBatchCap`, and it ran a full split.

That question is now a test of the flow tier: "resumes each stream after its last row when the slice
needs more than one page", in `test/repartition/repartition-flow.test.ts`. The harness of that file
drives the real `RepartitionSource`, `RepartitionTarget` and `FokosMigrationHost` over real stores. It
sends each call directly to the other side, with no alarm, no RPC and no wait.

- The source holds more rows and more locks than one page, and some sort keys are empty.
- A hash split runs. Each child reads the stream of its next pull from its own import record.
- The test asserts that the items stream and the lock stream each took more than one pull, and that
  each child holds exactly the rows and locks that it owns.
- The test operates in less than one second. It fails when the host drops the rest of a stream
  after its first page.

Rule: a test belongs to the Durable Object tier only when it needs two partitions and a real RPC hop.
Each test that moves down is a test that can never flake.

### 5.5 Budgets as deadlines, and not as attempt counts

`CHURN_RETRY_LIMIT` is 40 attempts with a 25 ms sleep, which is a budget of one second. The migration
window under full-suite load is longer. Two answers, and the suite can take both:

- Give the budget in milliseconds, in one place, so that its value states what it protects.
- Treat `fokos/partition_migrating` the way the churn already treats `fokos/partition_over_size`: a
  refusal that the model understands, and not an error that ends the run.

Done, with both answers. A child that imports refuses the write before the write applies, thus the
churn does not record the key in the model. `CHURN_RETRY_BUDGET_MS` is 10 s. It stops a table that
stopped, and it is much more than a retry needs under the load of the full suite.

### 5.6 The guard script

`tools/check-key-invariants.sh` is the precedent: a backstop for a rule that a comment cannot hold.
`tools/check-test-machinery.js` keeps the rules of this document. It is a Node script with no
dependencies. Each rule is one function in its `CHECKS` list. The function gets each test file and
returns its errors. To add a rule, add a function to the list.

- `noPrototypeSpy`: no `vi.spyOn(` on a `prototype` under `test/`. A file in
  `PROTOTYPE_SPY_EXCEPTIONS` is permitted, with its reason. The check fails when an entry has no
  spy or its file does not exist, thus the list can only become shorter.
- `noConcurrentHoldUser`: no `describe.concurrent` in a file that uses `withMigrationHeld`.
- `noBareTimerInPartitionTests`: no bare `setTimeout` or `sleep` in `test/partition-do/`. A timer
  that is not a wait is permitted when its line, or the line above it, has `guard: allow-timer` and
  a reason.

It runs as `check:test-machinery` in the `test` script of `packages/fokosdb/package.json`, after the
key check.

### 5.7 The concurrency policy

The speed comes from the file level, and not from the test level. Measured on 2026-09-22:

| Scope | With `describe.concurrent` | Sequential |
| --- | --- | --- |
| `test/partition-do/query-items.test.ts` | 7.9 s of test time | 8.1–9.4 s |
| `test/partition-do/hash-split.test.ts` | 8.8 s | 10.7 s |
| `test/partition-do/read-through.test.ts` | 1.9 s | 0.6 s |
| full suite, wall clock | 56.5 s | 56.8 s and 57.0 s |

The three files together gain about 3.5 s of test time alone, and the full suite shows no difference,
because 82 files already operate in parallel. Thus the suites stay sequential. Speed comes from a
split of a long file into two files, and from a fixture that `beforeAll` builds once.

Sections 5.2 and 5.3 make a concurrent file safe again. That is insurance, and not a reason to make
one concurrent.

A shared table has a limit: the timestamp conflict. The coordinator stamps a transaction with its
own clock, and a partition refuses a stamp that is not above the stamp of the item, or above the last
delete of the partition. That stamp can come from another test on a shared table, or from a write of
the same test in the same millisecond. The clock of miniflare can also go back. A wait of 1 ms before
the write does not prevent this, because the clock can go back more than 1 ms.

A test that writes through the coordinator and expects a result that is not a timestamp conflict
uses `writeOutcomeWithClockRetry` in `test/transactions/tx-helpers.ts`. The helper sends the
transaction again, after a wait of 2 ms, only after a cancel with `timestamp_conflict`, and at most
five times. A cancelled transaction applies nothing, thus a new attempt is safe. The helper refuses a
request with a `clientRequestToken`, because a replay of the token returns the same cancel. A test
that asserts the conflict itself keeps `writeOutcome`. The property suites accept the conflict as a
cancel of the model (`ORDERING_CANCEL_CODES` in `test/property-based/harness.ts`).

## 6. Alternative options

**A scheduler option from `PartitionDO`.** `src/sharding/runtime-types.ts` already declares
`scheduler.fastPathDelayMs` and `scheduler.fallbackAlarmMs`, and `PartitionDO` passes neither. A test
that could set them would stop all background work and get a complete freeze. `AGENTS.md` forbids a
production hook for a test, and sections 5.2 and 5.3 need no freeze. The option stays available if a
deployment ever needs to tune the scheduler, which is configuration and not a test hook.

**Global fake timers.** They control time exactly, and they run a Durable Object background callback
in the wrong I/O context. `AGENTS.md` records the cross-object TTL errors that follow.

**Longer deadlines.** The migration hold went from 5 s to 30 s earlier, and the failure rate went from
4 of 8 runs to 2 of 8. A longer deadline hides the fault and pays for it in the time of a real stall.

**A mock library with per-instance scope.** The dispatcher looks for an operation on the class, thus
no mock of the DO instance is reachable. A subclass is the only test control on one instance that the RPC
model allows.

## 7. Frequently asked questions

**Does a subclass weaken the coverage of `PartitionDO`?** The subclass inherits every operation and
each override calls `super`. The tests that need no test control keep the `PARTITION_DO` binding, thus the
production class stays covered by most of the tier.

**What happens when a test forgets to release a gate?** The gate is a field of one instance, and the
namespace of each test carries a `crypto.randomUUID()` prefix. A gate that stays closed blocks only
the partitions of that test, and the test timeout of `vitest.config.ts` reports it.

**Why not delete the deadlines?** A hang needs an end. The deadline becomes the backstop of a run that
hangs, and the progress rule decides a run that is only slow.

**Does the drive loop hide a defect that the alarm would find?** The loop calls `runDueWork`, which is
what `alarm()` calls. A defect in the alarm plumbing itself stays covered by the tests that call
`runDurableObjectAlarm`.

## 8. Open questions

**8.1 The size of the idle limit.** The loop fails after N rounds with no change and no due job. N
must be more than the longest chain of steps that produce no observable state change. TODO: measure
the longest such chain in a hash split and in a range split.

**8.2 The reach of the subclass.** Decided: every file moves to a test control. `destroy-fence.test.ts`
and the four transaction suites use the test controls now. One spy remains:
`PartialRangeTopology.prototype.maybePromoted` in `test/partition-do/promotion.test.ts`. That class
is the private bloom filter of the runtime, thus no subclass can reach it without a production
hook. The spy is in a sequential `describe` at the top level of the file. Vitest runs the sibling
tasks of a suite in groups by their `concurrent` flag, and one group ends before the next starts.
Thus no other test of the file operates while the spy is installed, and the spy is safe. A comment
at the spy says that the `describe` must stay sequential. The file stays in
`PROTOTYPE_SPY_EXCEPTIONS`.

**8.3 Other shared spies.** The guard finds only a spy on a prototype. `test/partition-do/tx-stale-recovery.test.ts`
spies on `Date.now` and on the `doStubs` module. That state is also shared by the whole isolate,
and `vi.restoreAllMocks()` of another test can remove it. The file is sequential today. TODO: decide
if the guard also refuses a spy on a global or on a module.

References:

- `docs/ideas/testing.md`
- `docs/ideas/2026-09-20-query-entry-point-into-a-range-tree.md`
- [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
