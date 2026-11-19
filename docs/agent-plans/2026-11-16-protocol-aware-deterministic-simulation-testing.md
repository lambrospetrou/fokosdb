# RFC — Protocol-aware deterministic simulation testing of the Durable Objects

**State:** Draft
**Date:** 2026-11-16
**Author:** Lambros Petrou

**Status:** Nothing is built. Scratch tests outside the repository verified the workerd facts of section 4.2.2.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
  - [4.3 Open questions](#43-open-questions)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

The goal of this work: a change to the implementation of FokosDB, for example a performance change, must not need a
review of correctness by hand. A test must find each defect that the change adds. The code paths in this document are
relative to `packages/fokosdb/`.

The test suites today:

- The Durable Object tier (`test/partition-do`, `test/transactions`) uses real RPC, real SQLite and real alarms, with
  `ControlledPartitionDO` and `ControlledTransactionCoordinatorDO`.
- The property-based suites (`test/property-based/`) use `fc.commands` to drive the database and an in-memory model at
  the same time. `transactions-serial-faulty.test.ts` adds faults. The contending batch uses a serializability oracle
  on the final state.
- The P model (`formal/p/`) has monitors for the 2PC and repartition protocols: `Atomicity`, `SingleApply`,
  `Authority`, `CopiesUntouched`, `Retention`, `LocksResolve`, `LockExclusion`, `CommittedNeverQuarantined`,
  `VersionIncreases`, `WriteSerializable`, `ReadSerializable`, `AnswerMatchesDecision`, `ReadAfterCommit`,
  `ClientAnswered`, `TransactionsComplete`, and `SplitCompletes`. The monitors check the model, not the code.

The limits of these suites:

1. **The seed does not set the timing.** `test/property-based/README.md` says so: a replay can pass for a real
   failure, because a split or a recovery can start at a different time.
2. **The checks are at the API and at the end of the run.** No check reads the internal state of a Durable Object
   after each step. A double apply or a bad merge is found only if it changes the final state.
3. **The serializability oracle has no real time.** It checks that some serial order gives the final state. It does
   not check that the order agrees with the order of the answers.
4. **No check for progress of each Durable Object.** A run can pass while one Durable Object stays blocked.

TigerBeetle describes the method that removes these limits ([Protocol-aware deterministic simulation
testing](https://tigerbeetle.com/blog/2026-08-20-protocol-aware-dst/)). The simulator controls every source of
non-determinism, it reads the protocol state of every node, and it checks invariants after each step.

### 1.1 Glossary

| Term | Meaning in this document |
| --- | --- |
| Simulator | The test code that selects each step, injects each fault, and runs the checkers. |
| Step | One event that the simulator selects. Section 4.2.3 lists the step kinds. |
| Decision log | The ordered list of the steps of one run. A replay of the log gives the same run. |
| Chaos phase | The part of a run with client operations and faults. |
| Heal phase | The part of a run after the chaos phase, with no fault and no new client operation. |
| Quiescent | The state of a Durable Object that has no work left. Section 4.2.8 defines it for each class. |
| Wrapper | The test-only subclass that `withDst` makes from a final Durable Object class. |
| Platform | A `FokosPlatform` object: the clock, the timers, and the random IDs of one Durable Object. |
| Outcome unknown | A client answer that does not say whether a write applied. Section 4.2.6 lists them. |

## 2. Goals and requirements

### 2.1 In scope

1. A run is a function of its seed. The same seed and the same code give the same steps and the same answers.
2. A failure shrinks with fast-check, and it replays with `FOKOS_PROPERTY_SEED`, `FOKOS_PROPERTY_PATH` and
   `FOKOS_PROPERTY_REPLAY_PATH`.
3. The simulator controls the order of every RPC between Durable Objects, every timer, every alarm, and every clock.
4. The simulator injects the faults of section 4.2.4.
5. The checkers test the public guarantees of section 4.2.6, with real time.
6. The checkers test the protocol invariants of section 4.2.7 after each step, through `fokosInspect`.
7. After the heal phase, every Durable Object of the run is quiescent. When it is not quiescent within the step
   budget, the run fails.
8. Each checker has at least one seeded defect that it finds (section 4.2.9).
9. The tests do not read SQLite tables and do not contain SQL. A schema change changes no test.

### 2.2 Out of scope

| Item | Reason |
| --- | --- |
| Storage faults (a failed `put`, a reset during a storage call) | They need more patches of `ctx.storage`. A later version can add them. |
| A simulator in Node.js with a copy of the Durable Object platform | A lot of custom work. Build it only if the workerd simulator is too slow. |
| Mutation testing of `src/` | The seeded defects of section 4.2.9 come first. |
| Cost goldens (RPCs by method, `rowsWritten`, steps to heal) | Add them when the checkers are stable. |
| Splits, migration and promotion before milestone M6 | The Fork API of workerd replaces the migration flow. Tests of the current flow have a short life. |
| A run against a deployed Worker | The wrapper exists only in `test/`. |
| Interception inside `FokosDB` | The client uses the global `env`. The simulator calls the client, and the wrapper observes the call where it arrives. |

### 2.3 Requirements

- The wrapper, the simulator and the checkers are in `test/` only. Production code gets no test hook and no flag.
- Two changes in `src/` are production surface: `FokosPlatform` (section 4.2.1) and `fokosInspect` (section 4.2.5).
  Both have a production use: the default platform is the production clock, and `fokosInspect` is a read-only
  operations API.
- The suite runs in the Workers runtime with `@cloudflare/vitest-plugin`, as every other suite does.
- Budget: about 2 minutes of simulation in each `pnpm test`, and about 30 minutes in a nightly run.

## 3. Milestones

| Milestone | Delivers | Where |
| --- | --- | --- |
| M1 | `FokosPlatform`, with the production default, wired into every call site of section 4.2.1. No behavior change. | `src/` |
| M2 | `fokosInspect` on the partition and the coordinator, with `selfCheck`. | `src/` |
| M3 | `withDst`, the simulator, the faults, the checkers and the seeded defects for 2PC with no split: partitions and the coordinator. | `test/` |
| M4 | The topology admin: `fokosInspect` on the admin Durable Object, its checkers and its quiescence. | `src/`, `test/` |
| M5 | Materialization, GSIs and push targets: `fokosInspect` on the materialization Durable Object, the checkers of guarantee 1f. | `src/`, `test/` |
| M6 | Splits, migration and promotion, after the Fork API replaces the migration flow. | `test/` |
| Fix (DONE) | A runtime fault on the hop to the coordinator, a reset included, answers the retryable `transaction_undecided` (section 4.2.11). | `src/` |

Each milestone is useful alone. M1 and M2 change no behavior. M3 finds 2PC defects before M4 starts. The fix is
independent of the other milestones.

## 4. Proposed solution

### 4.1 High-level overview

The test Worker exports a wrapped class for each Durable Object class. The wrapper sits around the real class, so the
code under test is the production code. The wrapper does four things:

1. It holds each outgoing RPC of its Durable Object until the simulator delivers it, and it holds each reply until
   the simulator delivers it.
2. It gives the Durable Object a simulated platform: a virtual clock, virtual timers, and seeded random IDs.
3. It replaces the alarm with a virtual alarm that only the simulator fires.
4. It adds control methods, for example "release message 7" or "fire timer 3", that the simulator calls over RPC.

The simulator is a fast-check property. fast-check generates the client operations and the decision at each step: the
message to deliver, the timer to fire, the fault to inject. After each step, the checkers read the protocol state of
each Durable Object with `fokosInspect` and check the invariants. After the chaos phase, the heal phase runs with no
faults until every Durable Object is quiescent.

```text
  fast-check: operations + decisions
              │
              ▼
  ┌────────────────────── simulator (test context) ──────────────────────┐
  │ pending messages, timers, alarms, clocks  ──►  checkers               │
  └──────┬──────────────────────────┬──────────────────────────┬─────────┘
         │ client call               │ __dst* control RPC        │ fokosInspect
         ▼                           ▼                           ▼
  ┌──────────────┐   held RPC   ┌──────────────┐   held RPC   ┌──────────────┐
  │ wrapper(P1)  │ ───────────► │ wrapper(TC)  │ ───────────► │ wrapper(P2)  │
  │ real P1 code │ ◄─────────── │ real TC code │ ◄─────────── │ real P2 code │
  └──────────────┘  held reply  └──────────────┘  held reply  └──────────────┘
```

### 4.2 Technical details

#### 4.2.1 `FokosPlatform`

```ts
interface FokosPlatform {
  now(): number;
  setTimeout(fn: () => void, ms: number): FokosTimer;
  clearTimeout(t: FokosTimer): void;
  wait(ms: number): Promise<void>;
  randomId(): string; // replaces crypto.randomUUID().replaceAll("-", "")
  random(): number; // replaces Math.random in the retry backoff
}
export const systemPlatform: FokosPlatform;
```

- `systemPlatform` uses `Date.now`, `setTimeout`, `clearTimeout`, `scheduler.wait`, `crypto.randomUUID` and
  `Math.random`.
- Each Durable Object class gets an overridable method `fokosPlatform(ctx)` that returns `systemPlatform`. It replaces
  `fokosNow()`. The class keeps the result in a private field: `#platform = this.fokosPlatform(ctx)`.
- The method reads only `ctx`, because the base constructor calls it before the fields of a subclass exist.
- The class gives the platform to `FokosShardingRuntime`. The runtime gives it to `FokosScheduler`, `RepartitionFlow`
  (in place of the `now = Date.now()` defaults), `sharding-store` and the topology cache.
- The class gives the platform to `TransactionParticipant`, `TtlExpiry`, the materialization exporter,
  `TargetRunner`, and each `setAlarm(…)` call.
- `FokosDB` gets an optional `platform` option. It uses the platform for the transaction ID, the deadlines and the
  retry backoff.
- `keyPairSeed` in `src/sharding/key-codec.ts` stays as it is, outside the platform. Section 4.2.1.1 tells why.
- The `error_id` of an error (`newErrorId` in `src/shared/errors.ts`) stays on `crypto.randomUUID`. It is only a
  name, no decision reads it, and the checkers ignore it.
- `jitterBackoff` and `tryWhile` of `durable-utils/retries` (version 0.3.8 or later) take an optional platform with
  `random` and `wait`. Each call site gives the `FokosPlatform` of its caller.
- The platform is one object for each Durable Object, not a module global. Each Durable Object then has its own clock,
  which can be skewed, and each timer belongs to one Durable Object.
- No wrapper replaces the globals of the runtime (`Date.now`, `setTimeout`, `Math.random`) while a callback runs.
  In vitest all Durable Objects and the test share one `globalThis`. A callback that awaits lets the requests of other
  Durable Objects run while the replacement is in place, and they then read the wrong clock, timers and random values.
- `FokosTimer` is a number, because the Workers runtime returns a number from `setTimeout`. `clearTimeout` accepts
  `undefined`, as the global does.
- The topology cache gets its clock and its fetch timeout from the platform of its caller. The timeout is an
  `AbortController` with a platform timer, in place of `AbortSignal.timeout`.
- The loop in `fokosDestroy` that clears all timers stays on the globals. It clears the real timers of the isolate,
  and `ctx.abort` follows it.
- A test that sets the clock of one instance overrides `fokosPlatform` in a test subclass
  (`ControlledPartitionDO`, `ControlledTransactionCoordinatorDO`). The returned platform reads `testNow()` of the
  instance at each call, so `vi.spyOn(instance, "testNow")` changes the clock after the constructor ran.
- M1 adds no new test of the platform. The tests that set the clock through `testNow` show the wiring, and the full
  suite shows that the behavior did not change.

The call sites today:

| API | Call sites |
| --- | --- |
| `Date.now()` | `src/sharding/scheduler.ts`, `src/sharding/sharding-store.ts` (`learnRangeBoundary`), `src/shared/transaction-limits.ts` (`txOrderTimestampNow`), a deadline helper in `src/server/do-transaction-coordinator.ts`, the `setAlarm` calls, `src/sharding/runtime.ts`, `src/sharding/repartition-flow.ts`, `src/sharding/topology-cache.ts`, `src/sharding/topology-keeper.ts`, the deadlines in `src/client/db.ts` |
| `setTimeout` | the scheduler fast path, `TtlExpiry`, `src/server/do-partition.ts`, `throttleTrailing` in `src/shared/tsutils.ts`, the admin outbox in the runtime, the call timeouts of the exporter and of `TargetRunner`, the batch retry in `src/client/fokos-std.ts` |
| `AbortSignal.timeout` | the topology fetch in `src/sharding/topology-cache.ts` |
| `scheduler.wait` | `TtlExpiry`, the materialization exporter, `TargetRunner`, the early start of the alarm in the runtime |
| `crypto.randomUUID()` | the transaction ID, the `error_id` of each error (which stays as it is) |
| `Math.random` | `jitterBackoff` and `tryWhile` of `durable-utils/retries`: `FokosShardingClient`, the admin outbox, `RepartitionFlow`, the batch retry |
| `crypto.getRandomValues` | `keyPairSeed` in `src/sharding/key-codec.ts`, which stays as it is (section 4.2.1.1) |

M1 must find every call site again with a search, because this table can be incomplete.

##### 4.2.1.1 `keyPairSeed` stays as it is

`keyPairHash` in `src/sharding/key-codec.ts` hashes a `(hashKey, sortKey)` pair with a seed from
`crypto.getRandomValues`. The first call makes the seed, and the isolate keeps it. It is the only call of
`crypto.getRandomValues` or `Math.random` in `src/`, so `FokosPlatform` has no `random()`. The simulator does not
control the seed, and this does not break determinism:

- A `KeyPairMap` uses the hash only when it has more than 8 entries, and then only to select a bucket. A byte
  comparison decides that two keys are equal.
- `entries()` and `values()` give the insertion order, not the bucket order.
- Thus `get`, `has` and the iteration order give the same result for each seed. Only the CPU time of a hash
  collision changes. No checker and no cost golden measures CPU time.
- The random seed prevents a caller from choosing keys that go into one bucket. A fixed seed would remove this
  protection.

A change that makes a result depend on the hash, for example an iteration in bucket order, must also give the seed
through the platform. The options then: the platform of the first caller seeds `keyPairSeed`, or each `KeyPairMap`
gets its seed from the platform of its owner.

#### 4.2.2 The test wrapper `withDst`

`withDst(Final)` returns a subclass of the final class. `test/worker-entry.ts` exports it under the class name of a
binding, as it exports `ControlledPartitionDO` today.

```ts
function withDst<C extends DurableObjectClass>(Final: C): C {
  return class extends Final {
    constructor(ctx: DurableObjectState, env: Env) {
      patchAlarms(ctx);                 // virtual setAlarm/getAlarm/deleteAlarm on the real ctx.storage
      super(ctx, wrapEnv(ctx, env));    // every outgoing stub comes from env
      return new Proxy(this, observeEntryPoints(ctx)); // `this` of each method is the real object
    }
    protected override fokosPlatform(ctx: DurableObjectState) { return sim.platformFor(ctx); }
    async __dstRelease(messageId: string, mode: Delivery) { /* … */ }
    async __dstFireTimer(timerId: string) { /* … */ }
    async __dstAlarm() { return await this.alarm(/* … */); }
    async __dstRestart() { this.ctx.abort("dst restart"); }
  };
}
```

These workerd facts set the shape of the wrapper. Scratch tests verified each one on workerd 1.20260923.1, under
`wrangler dev` and under `@cloudflare/vitest-plugin` 1.2.7:

| Fact | Consequence |
| --- | --- |
| A constructor can return a `Proxy`. RPC methods, getters, `fetch`, thrown errors with own properties, and calls between Durable Objects go through its `get` trap. | The wrapper can observe every entry point. |
| When a base class returns the `Proxy`, the `#private` fields of the subclass go on the `Proxy` and the fields of the base class go on the target. Internal `this.x()` calls also go through the trap. | The `Proxy` must wrap the final class, after the whole object exists, and each method must run with `this` set to the real object. |
| RPC refuses a method that is an own property of the instance. The vitest plugin refuses a method that exists only in the trap. | Each control method must be a method on the prototype of the wrapper. |
| A promise that a Durable Object made cannot be resolved from the test context. The call does not throw, but the request never continues, and vitest reports "Cannot perform I/O on behalf of a different Durable Object". A later release by RPC gets "Promise will never complete". | The simulator releases a held call only with an RPC into the Durable Object that holds it. This error stops the run with a timeout, and not with a failure. |
| `super()` refuses a `Proxy` of `ctx`: "constructor parameter 1 is not of type 'DurableObjectState'". | The wrapper patches `setAlarm`, `getAlarm` and `deleteAlarm` on the real `ctx.storage` with `Object.defineProperty`. This also stops the real alarm. |
| `stub.alarm()` throws "'alarm' is a reserved method". | `__dstAlarm()` calls `alarm()` inside the Durable Object. |
| `ctx.abort()` from an RPC throws in that call. A new stub then gets a new instance, and the SQLite rows stay. On a coordinator, the full suite passes, with no hang of the workers pool. | `__dstRestart()` is a restart fault for every class. |

The wrapper holds a call at the caller, not at the receiver:

1. A Durable Object calls a method on a stub from the wrapped `env`.
2. The wrapper records a pending message and waits on a gate in the caller.
3. The simulator calls `__dstRelease(id, mode)` on the caller. The wrapper sends the real RPC.
4. When the reply arrives, the wrapper records a pending reply and waits again.
5. The simulator releases the reply. The caller code continues.

A client call starts in the test context, so the simulator holds it before it starts.

#### 4.2.3 The simulator

**Step kinds.** At each step, the simulator selects one of:

- deliver a pending request,
- deliver a pending reply,
- fire a timer,
- fire an alarm,
- advance the clock of one Durable Object,
- restart one Durable Object,
- start a client operation (chaos phase only).

**Interleaving.** Two requests can run inside one Durable Object at the same time, for example two drives of one
transaction. The simulator selects their order. A Durable Object can switch between requests only at an `await` that
opens its input gate: an RPC, a timer, or an alarm. The simulator controls each of these. An `await` on storage keeps
the input gate closed, so it is not a point of a switch (section 6, question 2).

**End of a step.** A step ends when each request in each Durable Object waits at a gate of the simulator or has ended.
The wrapper counts the requests that run and the requests that wait.

**Message identity.** The ID of a message does not depend on time: `(target Durable Object name, method, origin,
call number of the origin)`. The origin is the name of the calling Durable Object, or `client:<n>`.

**Generation.** fast-check generates the client operations and the decisions. A decision is an index into the sorted
list of the possible steps, so a shrink of the decisions gives a valid run. Each run also writes the decision log, with
the message IDs, so that a reader can follow the run.

**Clocks.** The simulator keeps one global time. The clock of a Durable Object is the global time plus its skew. A
clock fault changes the skew. The clock of one Durable Object never goes back.

**Isolation.** Each run uses new table names. The simulator knows each Durable Object of the run because each wrapper
registers its name when it starts.

**The registry.** The wrappers write the pending messages, the gates and the counters of the requests into module
state, and the simulator reads them there. A step ends when the counters show that no request runs. The wrapper can
also resolve a promise of the simulator, so the simulator can wait for a signal in place of a poll.

**The vitest plugin.** These facts set the shape of the simulator. Scratch tests verified each one on workerd
1.20260923.1, `@cloudflare/vitest-plugin` 1.2.7 and vitest 4.1.11:

| Fact | Consequence |
| --- | --- |
| The test context, each Durable Object, and each call between Durable Objects use one module instance and one `globalThis`. A test that imports the `main` module gets the module of the bindings. | The registry is module state. No RPC reads it. |
| A Durable Object can resolve a promise that the test context made. | The wrapper can signal the end of a step to the simulator. |
| A wrapped `env` in `super()` works. A held request and its held reply continue after a release RPC. A virtual timer that a release RPC fires can send an RPC after the release RPC returns. | The wrapper of section 4.2.2 works under the plugin. |
| After `ctx.abort()`, the gates of the old instance stay in module state, and the new instance uses the same module state. A release of an old gate from the new instance gets "Cannot perform I/O on behalf of a different Durable Object". | The registry keys each gate by the instance. A restart removes the gates of the old instance, and nothing resolves them. |
| A rejected promise with no handler is an unhandled error for vitest. vitest then exits with code 1, also when each test passes. | The simulator attaches a handler to each promise when it makes it, for example the promise of a client call that a restart rejects. |
| Each `ctx.abort()` writes `uncaught exception` and `outputGateBroken` from workerd to the log. The run does not fail. | The log capture ignores these lines. They are not the cause of a recovery. |
| `vi.spyOn(console, "log")` in the test context gets the log lines of each Durable Object. | The log capture of section 4.2.7 needs no change in `src/`. |
| A request that arrives while a Durable Object waits in a loop of 200 `storage.put` calls runs after the loop. | An `await` on storage is not a point of a switch (section 6, question 2). |
| Each test file has its own module state, its own Durable Object instances and its own storage, also with `--max-workers=1`. | A DST file shares no state with other files. It needs no vitest project of its own. Other files change only the wall time. |

#### 4.2.4 Faults

| Fault | What the wrapper does | Error that the caller gets |
| --- | --- | --- |
| Drop the request | It does not send the RPC. | `Error("Network connection lost.")` with `retryable: true` |
| Drop the reply | It sends the RPC, then it discards the reply. | The same error. The caller cannot know that the call applied. |
| Duplicate | It sends the RPC two times. The caller gets the second reply. | none |
| Delay, reorder | The simulator delivers the message at a later step. | none |
| Overload | It does not send the RPC. | an error with `overloaded: true` |
| Restart | `__dstRestart()`: `ctx.abort()`. Held calls and timers of the instance are lost. The virtual alarm stays. | `{ durableObjectReset: true }` for each open call |
| Alarm failure | `__dstAlarm()` throws one time. The virtual alarm stays armed. | none |
| Clock skew, clock jump | The simulator changes the skew of one Durable Object. | none |

The injected errors have the shape of the workerd errors, so the retry rules of the client and of the runtime run.
M3 must check each shape against a real workerd error.

#### 4.2.5 `fokosInspect`

Each Durable Object class gets a read-only RPC that returns its protocol state. The class that owns the schema writes
it, so a schema change and its `fokosInspect` change are in one file, and the typecheck finds a mismatch. The test code
reads only these shapes.

```ts
fokosInspect(req: {
  what: ("items" | "locks" | "transactions" | "repartition" | "jobs" | "selfCheck")[];
  hashKey?: KeyBytes;
  range?: { start?: FokosCompositeKey; end?: FokosCompositeKey };
  transactionId?: string;
  cursor?: string;
  limit?: number;
}): Promise<{
  items?: { hashKey: KeyBytes; sortKey: KeyBytes; version: number; data: Uint8Array | string; expiresAt?: number }[];
  locks?: { transactionId: string; hashKey: KeyBytes; sortKey: KeyBytes; op: string }[];
  transactions?: { transactionId: string; state: string; participants: string[] }[];
  repartition?: { role: "leaf" | "router" | "importing"; phase?: string; children?: string[] };
  jobs?: { kind: string; dueAt: number }[];
  selfCheck?: { violations: string[] };
  cursor?: string;
}>;
```

- `items` holds committed state only.
- `transactions` is for the coordinator. `locks` is for the partition.
- `selfCheck` runs the invariants that need the schema inside the Durable Object, and returns only the violations.
  The first checks: the facts of each `item_stats` bucket agree with its items, no routingHash has more than 16 items
  and no bucket, each lock row has its
  `pending_tx_info` row, and an alarm is armed at or before the earliest due job.
- `fokosInspect` answers on an object with no identity, and it does not create one, as `fokosStatus` does.
- A filter can be added later. A field can be added later. A field is not removed while a test reads it.
- Each page must stay below the RPC limit of 32 MiB.
- `items`, `locks`, `transactions` and `selfCheck` are paged. `repartition` and `jobs` are small, so they are not paged.
  A request with `cursor` or `limit` names exactly one paged kind. A request with more than one paged kind and no
  cursor gets the first page of each, and it fails when a page is not the last one. The default `limit` is 100 and
  the maximum is 1,000. A page of items also stops before 16 MiB, counted as two bytes for each `item_bytes` byte
  plus the keys, because a string can go on the wire as two bytes for each character. A page always holds one item.
  A page of items reads the data of its own items only. A bad request fails with `inspect_request_invalid`, before
  any read.
- `items` and `locks` hold only the keys that the partition owns now (`ownerCheck`). A router and a promotion source
  can keep copies of keys that another partition owns, and these copies are not its state. A router owns no key, so
  it reads no row.
- A page of items or locks reads at most 10,000 rows, so a filter that matches few rows (for example a `range` with no
  `hashKey`) has a bound on its cost. A page can hold no entry and still have a cursor, as in `queryItems`.
- A filter that the class does not have (`hashKey` and `range` on the coordinator) and a kind that the class does not
  have fail with `inspect_request_invalid`. The `transactionId` filter of the coordinator reads `tc_state` in key
  order, because no index has `transaction_id` first.
- `selfCheck` pages the buckets of `item_stats`, because a check of all buckets reads every item and can pass the CPU
  limit of a request. A page also stops before a bucket that takes the stored item count of the page past 50,000, but
  it always checks one bucket. After the last bucket, `selfCheck` finds each routingHash with more than 16 items and no
  bucket. It seeks from one routingHash to the next, counts at most 17 items only for a routingHash with no bucket, and
  checks at most 1,000 routingHashes on a page. Its cursor position is the hex of the last routingHash. The checks that are not paged (the alarm, and the lock rows) go on the first page only, with at
  most 100 violations each.
- `repartition.role` is `importing` while the import of the partition is not complete (`awaiting_data` or
  `importing`), `router` after the split cutover, and `leaf` in all other cases. `phase` is the state of the active
  repartition (`queued`, `planned` or `cutover`). `children` holds the Durable Object names of the split targets.
- `jobs` holds each runtime job and host job that can run, with the deadline that the next pass of the scheduler reads:
  the earlier of its scheduled run and its `deadline()`, and only the scheduled run while the job is marked failed.
  `FokosScheduler.inspect()` gives them, and it writes nothing. The alarm check of `selfCheck` compares the earliest one
  with `getAlarm()`. It does not run while a pass runs, because a pass arms a fallback alarm and replaces it at its end.
  It does not run while `scheduleJob` arms the alarm, while an `alarm` handler runs (`getAlarm()` can then give null),
  or after the destroy fence. It reads the jobs before and after `getAlarm()`, and it reports a
  violation only when both reads show a job before the alarm. This check found that the last `fokosMigrationAck` made
  `source_cleanup` due and armed no alarm, so a lost fast-path pass left the cleanup without an alarm. The ack now arms
  the alarm at the cleanup deadline. The check is correct only when the object is quiet: a
  request that writes a job and arms the alarm in two steps can be between them. A simulator calls it when no request
  runs. The runtime gives this state with `FokosShardingRuntime.inspect()`.
- A cursor holds the paged kind and the position after the page: a list of the hex of each key part, and the text of
  a transaction ID. A cursor of another kind, a cursor that does not parse, a position with the wrong parts, and a
  `limit` that is not an integer from 1 to 1,000 fail with `inspect_request_invalid`.
- `locks.op` is the `operation` column. The ordered `op_list` is not in the shape until a checker needs it.
- The `selfCheck` of the coordinator has the alarm check, and a check that each transaction that is not complete has a
  `next_recovery_at`. `participants` is empty after a transaction completes, because completion deletes the
  `tc_participants` rows.

#### 4.2.6 Public guarantees

| # | Operation | Guarantee |
| --- | --- | --- |
| 1a | `getItem`, `putItem`, `updateItem`, `deleteItem` | Linearizable, also during a split or a promotion. A retryable `partition_migrating` error means that the write did not apply. |
| 1b | `transactWriteItems` | Strictly serializable with all the operations of this table: a transaction that got its answer before another operation started comes first in the serial order. |
| 1c | `transactGetItems` | Strictly serializable when it succeeds. A `read_conflict` is allowed at any time. In a heal phase with no writer it must succeed. |
| 1d | `batchGetItems`, `batchWriteItems` | Each item is linearizable alone. The batch is not atomic. |
| 1e | `queryItems` | No snapshot across pages. Each item it returns had that value at some time during the query. A key that is present and not changed during the whole query is returned exactly one time, also when a split occurs between pages. |
| 1f | GSIs, push targets | Eventual. At quiescence, each one is equal to a function of the base table. The versions of each key arrive in increasing order. An uncommitted or cancelled write never arrives. |
| 1g | TTL | An expired item can be read until its delete runs. After the delete it does not come back. |

Why 1b holds today: prepare locks each key of the transaction, `check` keys included, until commit. A write that is not
transactional fails on a locked key, and `transactGetItems` fails on a lock. The `committed` answer waits for the commit
of each participant. So the real-time commit order is a valid serial order
(`docs/agent-plans/rejected/2026-09-05-item-timestamp-ordering-columns.md`). The timestamps only reject more
transactions. They do not set the order.

**Outcome unknown.** These answers do not say whether a write applied: `transaction_commit_pending`,
`transaction_undecided`, `foreign_error`, and each error with `retryable: true` on a write. The checker accepts either
result for the write. A later read or a replay of the token must agree with one of the two results. Real-time order
applies only to a definite answer.

#### 4.2.7 Checkers

The checkers have four layers. A failure stops the run at the step where it occurs.

1. **The public API.** A history checker tests section 4.2.6. Each operation has its start step and its answer step.
2. **2PC.** Port the P monitors to TypeScript, over the `fokosInspect` state: `Atomicity`, `SingleApply`,
   `LockExclusion`, `LocksResolve`, `CommittedNeverQuarantined`, `VersionIncreases`, `AnswerMatchesDecision`,
   `ReadAfterCommit`, `TransactionsComplete`, and `ClientAnswered`.
3. **Sharding** (M6). `Authority`, `CopiesUntouched`, `Retention`, `SplitCompletes`.
4. **The local derived state.** `selfCheck` of each Durable Object.

**Same identity, same content.** A registry maps `(table, hashKey, sortKey, version)` to a digest of the data. Each
observation adds to it: an item from `fokosInspect`, a client read, a GSI image, a push delivery, and in M6 a migration
page. Two observations with one identity and two digests fail the run.

**A recovery needs a cause.** The simulator captures `console` output in the test isolate. It keys each line by the
Durable Object name in `logParams`. Each recovery log line needs an injected fault in the decision log that explains
it. Examples: `tx_recovery` finishes a transaction only after a restart or a lost message, and a retry runs out only
after a persistent fault. M3 lists the log lines and their causes.

#### 4.2.8 Quiescence and the heal phase

After the chaos phase, the simulator stops all faults and all client operations. It then delivers messages and fires
timers and alarms until each Durable Object is quiescent.

| Class | Quiescent when |
| --- | --- |
| Partition | No lock rows, no due job, no split or import, an empty outbox, no virtual alarm before the next due job. |
| Coordinator | Each transaction is `COMMITTED` or `CANCELLED`, no due job, no virtual alarm before the next due job. |
| Topology admin | TODO: M4 defines it. |
| Materialization | TODO: M5 defines it. |

The heal phase has a step budget. When a Durable Object is not quiescent at the end of the budget, the run fails with
the `fokosInspect` state of that Durable Object. The budget is TODO: M3 measures it.

#### 4.2.9 Proving the checkers

Each checker gets at least one seeded defect in a test-only subclass, as the `tcBug*` cases do in the P model. The
test asserts that the checker finds the defect within a fixed run count. A private method cannot be overridden, so
some defects need mutation testing of `src/`, which is out of scope.

#### 4.2.10 Run budget

- `pnpm test` runs about 2 minutes of simulation. `FOKOS_PROPERTY_RUNS` sets the run count.
- A nightly run uses a higher `FOKOS_PROPERTY_RUNS` for about 30 minutes. Today `.github/workflows/` has no test
  workflow (section 4.3).

#### 4.2.11 The answer to a coordinator reset

When the coordinator resets during `transactWriteItems`, the caller gets `{ durableObjectReset: true }`, with no
`retryable` marker. `FokosDB` reports it as `FokosInternalError: foreign_error`, which is not retryable. The
transaction can then commit through recovery. A scratch test showed it: the client got `foreign_error`, the alarm
recovered the transaction, both keys were committed, and a replay of the token answered `committed`.

The fix: `FokosDB` reports each runtime fault on the hop to the coordinator as the retryable `transaction_undecided`.
A runtime fault is an error with `retryable`, `overloaded` or `durableObjectReset`. The error holds the token in
`attributes.idempotencyToken`, also when `FokosDB` made the token, and keeps the `foreign_error` as `cause`. An error
with no runtime marker stays `foreign_error`.

### 4.3 Open questions

#### 4.3.1 The names of the control methods

The names `__dstRelease`, `__dstFireTimer`, `__dstAlarm` and `__dstRestart` are placeholders. A name must not clash
with a method of a Durable Object class.

#### 4.3.2 The nightly run

No workflow runs the tests today. Options: add a scheduled GitHub workflow, or run the long run by hand before a
release.

## 5. Alternative options

| Option | Why not |
| --- | --- |
| A common `DurableObject` base class that returns the `Proxy` | The `#private` fields break, and internal calls go through the trap (section 4.2.2). |
| The wrapper in `src/` behind `env.FOKOS_DST_PROXY_DO_ENABLED` | It adds a production hook for a test. It helps only a run against a deployed Worker, which is out of scope. A `.dev.vars` value is a string, so `"false"` is true. |
| Own a random generator in place of fast-check | Longer runs, but no shrink. |
| Structured event codes in `src/` for recovery | Clearer than the log lines, but more production surface. The log capture needs no `src/` change. |
| Tests that read the SQLite tables through `ctx.storage.sql` | A schema change breaks them only at run time, and the author of the change must know that they exist. |
| A test adapter that calls the store readers | The stores are `#private` on the Durable Object, so the adapter must build a second store over the same `sql` handle. No reader lists all items. |
| A `Proxy` of `ctx` | workerd refuses it in `super()`. |
| A simulator in Node.js | Out of scope (section 2.2). It is about 100 times faster and it can crash at any `await`. |

## 6. Frequently asked questions

**1. Why does the wrapper hold a call at the caller, and not at the receiver?** The simulator can release a gate only
with an RPC into the Durable Object that made it. At the caller, one wrapper holds both the request and the reply.

**2. Why does the simulator not control `await` on storage?** workerd closes the input gate of a Durable Object while
it waits for storage. No other request or reply enters the Durable Object at that time, so the order is the same in
each run. The SQL API and the `storage.kv` API are synchronous. Outside the alarm, only two calls use the
asynchronous API, and neither is a point of a switch:

- The `put` of the promotion Bloom filter in `src/sharding/sharding-store.ts`. `#flushBloom` in
  `src/sharding/runtime.ts` starts it in a timer task and does not wait for it.
- The `deleteAll()` in `fokosDestroy` in `src/sharding/runtime.ts`. It runs in `blockConcurrencyWhile`, and
  `ctx.abort` follows it.

The gap is storage faults, which are out of scope.

**3. Why is the platform an object for each Durable Object?** The clocks of two Durable Objects can be skewed, and
each timer must fire inside its own Durable Object.

**4. Why is `fokosInspect` not a test hook?** It is read-only, it changes no behavior, and an operator can use it to
debug a partition or a coordinator, next to `status`, `fokosStatus` and the `debug*` RPCs.

**5. Why are splits last?** The Fork API of workerd will replace the migration flow. The split stays the same, but its
tests must change. The 2PC, admin and materialization checkers do not depend on the migration flow.

## 7. References

- `docs/ideas/2026-09-22-deterministic-do-test-machinery.md`
- `docs/ideas/2026-09-26-testing-approaches.md`
- `docs/ideas/2026-10-09-p-model-of-fokosdb-transactions.md`
- `docs/ideas/2026-10-09-quint-model-of-the-transaction-coordinator.md`
- `docs/agent-plans/rejected/2026-09-05-item-timestamp-ordering-columns.md`
- `packages/fokosdb/test/property-based/README.md`
- `packages/fokosdb/formal/p/PSpec/`
- [Protocol-aware deterministic simulation testing](https://tigerbeetle.com/blog/2026-08-20-protocol-aware-dst/)
- [Durable Objects: Easy, Fast, Correct — Choose three](https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/)
- [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
