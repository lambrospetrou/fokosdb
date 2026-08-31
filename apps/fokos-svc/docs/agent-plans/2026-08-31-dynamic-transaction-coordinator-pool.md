# RFC — Dynamic transaction coordinator pool

**State:** Draft
**Date:** 2026-08-31
**Author:** Lambros

---

## Table of Contents

1. [Overview and Context](#1-overview-and-context)
2. [Goals and Requirements](#2-goals-and-requirements)
3. [Milestones](#3-milestones)
4. [Proposed Solution](#4-proposed-solution)
5. [Alternative Options](#5-alternative-options)
6. [Frequently Asked Questions](#6-frequently-asked-questions)
7. [References](#7-references)

---

## 1. Overview and Context

### 1.1 The problem

`FokosDB` sizes its coordinator pool at
`numTxCoordinators ?? TX_COORDINATORS_PER_ROOT_TREE * rootTreesN` and fixes that size for the life of
the table (`src/lib/db.ts`). Two problems follow.

1. **The size is a guess made before any traffic exists.** `rootTreesN` counts the root partitions of
   a table, and a partition count is not a transaction count. Most operations on a table can be
   non-transactional, and a single-partition transaction never reaches a coordinator at all
   (`#writeSingleShotFastPath`). A table therefore gets a pool sized from a number that has no
   relation to the load the pool carries.
2. **The size cannot follow the traffic.** A table splits its partitions and grows its write rate over
   its life, and the pool stays where it started. The only lever is a redeploy with a new
   `numTxCoordinators`, which needs a human to decide the number.

The two extremes are both wrong. One Durable Object per transaction bounds storage perfectly and pays
a cold start of hundreds of ms on every transaction and every recovery. One fixed pool serves a small
table badly, because it spreads rare transactions over shards that are always cold.

### 1.2 What makes a dynamic size possible

A wrong pool size is a performance fault and not a correctness fault, with one exception.

- `initiateWrite` stamps every lock with `coordinator_do_id`, and the participant alarm resolves it
  with `idFromString`. The pool size routes only the initial claim of a new transaction, so a size
  change never re-routes an in-flight transaction.
- A `clientRequestToken` replay is the exception. It must reach the shard that holds its `tc_state`
  row, so its route must not move.

This plan therefore splits the pool in two. A transaction with a `clientRequestToken` keeps today's
fixed pool and today's routing. A transaction without one gets a second, dedicated pool whose width
changes freely, because a tokenless call has no replay contract: the library generates its
idempotency token, and a tokenless call can therefore use any pool size at any time.

### 1.3 What the reader must know

- The dynamic pool carries **multi-partition tokenless writes only**. `#writeSingleShotFastPath`
  answers a single-partition tokenless transaction in one round trip with no coordinator, and read
  transactions run in the Worker. For a table whose transactions are mostly single-partition, the
  dynamic pool sees almost nothing.
- A Worker builds a `FokosDB` for each request, so any state that must live across requests belongs to
  a module-level object and not to an instance field.
- Every signal a Worker can read is local. The in-flight counter is per isolate. The rate limiting
  binding is per colo. Neither can measure the load of a table across the fleet.
- A coordinator shard is the one component that sees the traffic of every isolate and every colo for
  its own shard. It is already on the request path, so it can answer with what it sees at no extra
  round trip.
- A Durable Object sustains about 1,000 requests per second (the Durable Objects soft limit), and a
  cold start costs hundreds of ms.

---

## 2. Goals and Requirements

In scope. Each statement must be true when the work is finished.

1. A table with rare transactions uses one warm coordinator shard, and pays no cold start after the
   first transaction.
2. A table whose transaction rate grows widens its pool without a redeploy and without an operator.
3. The width follows measured coordinator load. It never reads the partition count and never reads
   `rootTreesN`.
4. An operator can set a floor and a ceiling per table, so a table with a known baseline starts wide
   enough and no table can run away.
5. The routing logic is a pure class with no knowledge of Durable Objects. It takes a clock and its
   constants in the constructor, returns shard indices, and holds every counter, cache and eviction
   rule it needs. A unit test drives it with no mock and no Durable Object.
6. The width never changes the outcome of a transaction. A wrong width costs latency and nothing else.
7. Every cross-shard retry is safe: the library retries on another shard only when the platform proves
   that the first shard ran no code.
8. The isolate holds a bounded amount of routing state, whatever the number of tables it serves.

Out of scope, with the reason.

- **The tokened pool.** It keeps the fixed size and the routing it has today. Its replay
  contract needs a stable route, and the signals of this plan are all local, so none of them can
  drive a fleet-consistent size change. A separate plan can revisit it.
- **Removal of `clientRequestToken`.** A later plan can consider it. If the token goes, the dynamic
  pool becomes the only pool and 4.2.1 collapses to one shard group. Nothing here depends on the
  answer, which is why the shard group name avoids the word "tokenless".
- **Placement.** The pool is placed where it is first touched, as today.
- **Changes to the two-phase commit protocol, the coordinator state machine, or garbage collection.**

---

## 3. Milestones

Each step builds on the one before it, and the work can stop after any step.

1. **Split the pool.** Give tokenless transactions a dedicated shard group and route them with
   `hash(transactionId) % W`, with `W` fixed at `minTxCoordinators` (4.2.1). No dynamics. This step
   alone replaces "spread rare transactions over 100 cold shards" with "use one warm shard", and most
   tables need nothing more.
2. **Add the local control loop.** Ship `CoordinatorPoolRouter` with its per-table state, the
   in-flight counter, the local rate window, and the grow and shrink rules that use them (4.2.2,
   4.2.3, 4.2.7). The width now follows the traffic of one isolate.
3. **Add the coordinator observation.** The request carries the width the caller used, and the
   response carries what the shard sees (4.2.4). The width now converges across isolates and colos.
4. **Add the colo rate binding.** Check it before dispatch (4.2.5). This covers the one moment no
   other signal exists: the first request of a fresh isolate.
5. **Add the overload signal.** Handle a 429 with the escalation ladder and the fail-closed predicate
   (4.2.6).
6. **Add warm-up.** Ping a newly added shard off the critical path (4.2.9).

---

## 4. Proposed Solution

### 4.1 High-level overview

Two pools, two routing rules:

```
fokos_tc.<tableName>.s-<i>    i ∈ [0, Nstable)   with a clientRequestToken
                                                 Nstable = numTxCoordinators
                                                           ?? TX_COORDINATORS_PER_ROOT_TREE * rootTreesN
                                                 hash(clientRequestToken) % Nstable, never changes

fokos_tc.<tableName>.d-<i>    i ∈ [0, W)         without a clientRequestToken
                                                 W between minTxCoordinators and maxTxCoordinators
                                                 hash(transactionId) % W, changes with the load
```

The pools are separate objects and not two routing rules over one range. A shared range would mix
tokened traffic into every measurement of the control loop below, and the loop would then widen the
dynamic pool for load that the dynamic pool cannot carry.

`W` is per isolate. Four signals move it, and each one covers a blind spot that the others cannot see.

| Signal | Read | Blind spot it covers | Direction |
| --- | --- | --- | --- |
| Colo rate binding | before dispatch | the first request of a fresh isolate, when no other signal exists | widen |
| Overload error (429) | on failure | ground truth, not an estimate | widen, first |
| Coordinator observation | on the response | the load of every other isolate and colo | widen only |
| Local in-flight and rate | continuous | fast local reaction, and the only input that shrinks | both |

The width grows fast and shrinks slowly. A late widening costs queueing at a shard. An early shrink
costs a cold start, which is worth hundreds of ms.

Section 4.2.8 walks two requests through the whole calculation with numbers.

All the logic lives in one pure class, `CoordinatorPoolRouter`. It takes a clock and its constants,
receives observations, and answers with a shard index. It never builds a Durable Object stub, never
calls the rate binding, and never sends a ping. The caller does all of that and reports the results
back.

### 4.2 Technical details

#### 4.2.1 The two pools

- `FokosDBOptions` gains `minTxCoordinators` (default 1) and `maxTxCoordinators` (default 1,024). Both
  are integers of 1 or more, and `minTxCoordinators` must not exceed `maxTxCoordinators`.
- The default ceiling of 1,024 is a runaway guard, and not a platform limit. At `targetRpsPerShard` of
  500 it carries about 512,000 transactions per second for one table, which is headroom no table is
  expected to need. A table that needs more sets a larger value.
- Nothing in the platform bounds the ceiling. A shard stays warm while it takes traffic more often than
  the eviction interval of a Durable Object, which is `W ≤ R * T_evict` for a table at `R` transactions
  per second, and the control law already holds `W` proportional to the load. The ceiling therefore
  bounds a control loop that misbehaves, and never sizes a healthy pool.
- A shard that falls outside the current `[0, W)` keeps its own alarm, drives its own non-terminal
  transactions, and sweeps its own `tc_state` rows one idempotency window after each one completes. A
  shrink therefore strands nothing, and no external step reclaims a shard the width left behind.

#### 4.2.2 `CoordinatorPoolRouter`: the pure class

Two levels. `TablePool` holds the control loop for one table and nothing else. `CoordinatorPoolRouter`
owns the map of tables, the eviction policy and the clock. The split keeps the memory bound in one
place, because "evict the table with the lowest rate" is a decision across tables that one table
cannot make.

```ts
export type PoolObservation = {
    /** Requests in flight at the shard when it answered. Valid immediately. */
    inflightNow: number;
    /** Requests the shard counted in its current window. */
    requests: number;
    /** How long that window has run. The real elapsed time, however short. */
    observedMs: number;
    /**
     * The largest `widthUsed` any caller reported to this shard in the current window. A new table
     * entry seeds its width from this, and nothing else reads it. See 4.2.4.
     */
    peerWidth: number;
};

export type TablePoolLimits = { minShards: number; maxShards: number };

export type CoordinatorPoolRouterOptions = {
    now?: () => number;
    /**
     * Asks the colo rate limiting binding about one key (4.2.5). `true` means the key is inside its
     * limit, which matches the `success` field of the binding, so no caller can invert it. The class
     * builds the key and decides when to ask. Omit it and the class skips the check, which is how
     * milestones 1 to 3 run. A rejected promise counts as "inside the limit".
     */
    isWithinRateLimit?: (key: string) => Promise<boolean>;
    /** Tables held before the lowest-rate entry is evicted. Default 256. */
    maxTables?: number;
    targetInflightPerShard?: number; // default 100
    targetRpsPerShard?: number;      // default 500
    growStepMax?: number;            // default 64
    growCooldownMs?: number;         // default 2_000, with jitter
    shrinkIntervalMs?: number;       // default 60_000
    rateWindowMs?: number;           // default 10_000
    minObservationMs?: number;       // default 2_000, see 4.2.4
    maxRateChecks?: number;          // default 4, see 4.2.5
};

export class CoordinatorPoolRouter {
    constructor(options?: CoordinatorPoolRouterOptions);

    /**
     * Shard index for this attempt. `attempt` is 0, 1 or 2. See 4.2.6. Asynchronous because it asks
     * `isWithinRateLimit` before it answers (4.2.5).
     */
    getShard(
        tableName: string,
        limits: TablePoolLimits,
        transactionId: string,
        attempt: number,
    ): Promise<{ shard: number; width: number }>;

    /**
     * Runs one coordinator call with the in-flight count kept balanced, including on the error path:
     *
     *     onDispatch(tableName);
     *     try { return await fn(); } finally { onSettle(tableName); }
     *
     * This is the documented way to call a shard. `onDispatch` and `onSettle` stay exported for
     * tests, and a caller that pairs them by hand risks a leak that never heals (4.2.7).
     */
    withRequest<T>(tableName: string, fn: () => Promise<T>): Promise<T>;
    onDispatch(tableName: string): void;
    onSettle(tableName: string): void;

    /** Records one shard that answered with an overload (4.2.6). */
    observeOverload(tableName: string, shard: number): void;
    /**
     * `widthUsed` is the width `getShard` answered with, and not the width now. The two differ when
     * another request widens the pool while this one is in flight, and the estimator of 4.2.4 needs
     * the divisor that actually routed this request.
     */
    mergeObservation(tableName: string, widthUsed: number, observation: PoolObservation): void;

    /**
     * Half-open range of the shards the last growth added, `[start, end)`. Answers it once and then
     * forgets it. A growth only extends the upper end, so the range is always contiguous and two
     * growths before a read merge into one. `start === end` means nothing is pending, which a
     * `for (let i = start; i < end; i++)` loop handles with no check.
     */
    takeWarmupRange(tableName: string): { start: number; end: number };

    /** Current width. For tests and logs; `getShard` answers the width a request must report. */
    widthFor(tableName: string): number;
}
```

Three rules keep the class pure and testable.

- **No timers.** A Workers isolate cannot rely on one. Every window roll, every decay and every
  cooldown is computed lazily from `now()` on the next call. A test advances a fake clock and calls a
  method.
- **No randomness.** `getShard` derives the index from `hash(transactionId)`, which the caller already
  generates. The spread is the same as `Math.random`, and a test is deterministic.
- **No I/O.** The class returns indices and ranges. The caller maps an index to a Durable Object name
  and sends the pings. `isWithinRateLimit` is the one exception, and it hides its platform behind a
  string and a boolean, so the class still knows nothing about Cloudflare while it owns the parts
  worth testing: which key it asks about, and when.
- **Policy inside, control flow outside.** The class decides which shard an attempt uses and when the
  width moves. The caller decides whether to make another attempt and which errors it rethrows. The
  overload predicate therefore stays at the call site (4.2.6): the class would only use it to filter
  a signal, and the caller needs it anyway to write its loop.
- **No state across an await.** `getShard` awaits `isWithinRateLimit`, and another request can grow
  the width during that await. The method reads the width again after the hook answers, and never
  decides from a value it read before it.

One module-level instance is shared by every `FokosDB` in the isolate:

```ts
const poolRouter = new CoordinatorPoolRouter();
```

#### 4.2.3 Per-table state and the control law

```
TablePool = {
    width,                          // current W
    seeded,                         // false until the first observation seeds the width, see 4.2.4
    limits,                         // last min/max seen for this table
    inflight, inflightHwm,          // concurrency, and its high-water mark
    windowCount, windowStartedAt,   // local rate over rateWindowMs
    lastRate,                       // rate of the last complete local window
    overloadedShards,               // at most 2 entries with timestamps, for 4.2.6
    pendingWarmupStart,             // half-open range the last growth added
    pendingWarmupEnd,
    grownAt, shrunkAt,
}
```

The width moves under two rules.

**Grow.** Take the largest lower bound any signal gives, and clamp:

```
width = min(max(width, localBound, observationBound, breachBound, overloadBound), maxShards)
```

- No growth adds more than `growStepMax` (64) shards. A doubling is right at width 8. At width 1,000
  it is a thousand cold starts at once.
- No growth happens within `growCooldownMs` (2,000 ms, jittered) of the last one, except a growth that
  an overload error drives (4.2.6). An overload is ground truth, and the others are estimates.

**Shrink.** Only the local signals shrink the width, at most one halving per `shrinkIntervalMs`
(60,000 ms), and never below `minShards`.

The local signals under-count, because one isolate sees one share of the traffic. That makes them a
correct *lower bound* for growth, and it makes them look wrong for shrinking. They are still the right
input, because **the width is isolate-local**. It governs only the requests this isolate sends. An
isolate whose own traffic stopped sends almost nothing, so its narrow width costs almost nothing, and
its first request afterwards gets an observation (4.2.4) that widens it again. The rate binding
(4.2.5) covers that first request in the meantime.

#### 4.2.4 Signal: the coordinator observation

Both fields travel under `_internal`, which `FokosDB` strips before it answers the customer, the same
convention `PartitionInfoInternal` already uses for `rangeAncestors`.

```
InitiateWriteRequest._internal.widthUsed:       number
InitiateWriteResponse._internal.poolObservation: PoolObservation
```

**The shard reports facts and computes nothing.** It counts what reaches it, keeps the largest
`widthUsed` any caller reported in the current window, and answers with those numbers. Every derived
value belongs to the caller. A shard that computed a width would hold a policy that belongs to the
client library, and would have to be redeployed to change a constant.

**`widthUsed` exists for `peerWidth` and for nothing else.** The estimator below multiplies by the
width the *caller* routed with, which the caller already holds, so the shard needs no width for that.
It needs one to answer the separate question of what the fleet is doing, which no caller can see.

The shard keeps an in-flight count and a request counter over a tumbling window of `rateWindowMs`. It
reports the count of the current window and the real time that window has run, so `observedMs` is the
age of the window and never a placeholder. A shard 150 ms old reports 150.

**The caller decides what a short window is worth.** It ignores the rate term while
`observedMs < minObservationMs` (2,000 ms) and keeps the concurrency term, which needs no
normalization: 20 requests in flight at a shard 150 ms old genuinely is 20 concurrent.

The gate matters in one direction. A short window that under-counts is already harmless, because an
observation only widens. A short window that over-counts is not: a shard 100 ms old that catches a
burst of 100 first-requests normalizes to 1,000 requests per second, which widens the pool, and each
widening creates more new shards that produce more short samples. The gate cuts that loop at its
source.

The gate costs little. A shard hot enough to justify a widening takes traffic, so it stays warm and
its window fills. The gate silences a shard that is quiet or new, and a quiet shard has nothing to say
about widening. It also silences every shard for the first 2,000 ms of each 10,000 ms window, which is
20% of the time, and the concurrency term covers that gap. If measurement shows the gap matters, the
shard can report the previous window as a second pair of raw numbers and let the caller choose; that
stays inside the same boundary, because it adds facts and no decision.

**A new table entry seeds its width from `peerWidth`, once.** An isolate that has just started, or
one whose entry the LRU evicted, holds `minShards` while the fleet can run far wider. Routing is a
prefix, so shard 0 takes traffic from every isolate at every width, and a caller at `minShards` always
lands there. The isolate that knows least therefore asks the shard that sees most. On the first
observation for an entry, the caller sets `width = clamp(max(width, peerWidth), minShards, maxShards)`
and marks the entry seeded. Every later observation ignores `peerWidth`.

**A seed adds nothing to the warm-up range.** It catches up to a width other isolates already run, so
the shards it takes in are in use and warm by definition. Only a growth this isolate reaches on its
own can create an object, and only those indices need a ping.

Seeding is limited to the first observation on purpose. A standing floor of `peerWidth` would ratchet:
one isolate holding a stale wide value would hold the whole fleet there, and no local decay could pull
it down. Applied once, a seed that is too wide is a starting point that the normal shrink of 4.2.7
takes away.

Seeding also keeps the ladder of 4.2.5 conservative. Without it the ladder would carry the whole
burden of convergence, and reaching a fleet width of 50 from 1 would need several requests spread over
`growCooldownMs` — with each of those requests sent to a shard the binding had already flagged. The
two mechanisms answer different questions: seeding answers "what is everyone else doing", and the
ladder answers "is this one shard hot right now".

The caller turns the observation into a lower bound on the width:

```
routing is uniform over widthUsed, so   inflightNow ≈ total / widthUsed

widthFromInflight = widthUsed * inflightNow / targetInflightPerShard
widthFromRate     = observedMs > 0
                    ? widthUsed * (requests / (observedMs / 1000)) / targetRpsPerShard
                    : 0
observationBound  = ceil(max(widthFromInflight, widthFromRate))
```

**The observation only ever widens.** The estimator assumes every caller uses the same width, and the
fleet breaks that assumption exactly when it matters. With a fresh isolate at width 1 and a warm one
at width 10, sharing 100 concurrent transactions, shard 0 sees about 55 and shard 5 sees about 5. The
warm isolate asks both. Shard 0 answers `10 * 55/100 → 6` and shard 5 answers `10 * 5/100 → 1`. Both
tell the caller that behaves correctly to narrow, and the second tells it to collapse onto the shard
that is already hot. `width = max(width, bound)` removes the failure, and it costs nothing, because
shrinking is slow and local by design.

**Convergence takes a few round trips, and that is enough.** A fresh caller at width 1 that reaches a
shard holding 300 concurrent transactions widens to 3, then to 9, then to 27, because each answer
multiplies its own width by the same ratio. It reaches a correct width in three or four round trips,
and the rate binding of 4.2.5 covers the requests in between. A shard that also reported the widest
value it has seen would converge in one, and is rejected: one caller reporting a stale high width
would then widen the whole fleet, and an observation only widens, so nothing would pull it back.

**The two targets are not one signal.** Little's Law ties them, `inflight = rps * latency`, so they
cross at one latency. With `targetInflightPerShard` at 100 and `targetRpsPerShard` at 500 they cross at
200 ms. Below that, which is the normal case for participants in one location, the rate bound binds
first. Above it, when participants are slow or remote, the concurrency bound binds first and stops one
shard from holding unbounded pending state. When concurrency is high and the rate is low, the
bottleneck is downstream and a wider pool cannot move it, so the concurrency bound grows the width
conservatively.

#### 4.2.5 Signal: the colo rate binding

`getShard` asks `isWithinRateLimit` about `<tableName>.d-<shard>` before it answers, for the shard it
is about to name. The caller passes the hook once, in the constructor, and calls one method.

**A breach is not a rejection.** The shard would still serve the request. The answer means this colo
has pushed a lot at that shard recently, so the ladder treats it as a reason to look elsewhere and
never as a reason to fail.

The ladder inside one `getShard` call asks up to `maxRateChecks` (4) times and widens at most twice:

1. Pick a shard and ask about it. Inside the limit: answer it.
2. On a breach, pick a shard that has not been asked about yet, **at the same width**, and ask again.
   A single hot shard is not evidence that the pool is small, and a re-pick inside the current width
   costs one cheap question and no cold start.
3. Widen only when the current width has nothing left to try: either every shard in it has been asked
   about, which is what happens at width 1 and 2, or two distinct shards in it have breached. Then
   pick from the wider range and ask again.
4. After the fourth question, or after the second widening, answer the last shard picked and ask no
   more. The request leaves either way.

**The check count and the widening count are separated on purpose.** A question to the binding is
colo-local and costs single-digit milliseconds, so spending three or four of them to avoid a hot shard
is cheap. A widening is not cheap: it creates cold shards, and an observation only ever widens, so a
width that grows for a bad reason waits for `shrinkIntervalMs` to come back. Capping the widenings at
two therefore bounds how far one request can move the width, whatever the threshold turns out to be
(4.3.2), while the extra questions cost only latency, and only on a request that already met a hot
shard. A healthy request asks once.

Both widenings obey `growStepMax` and `maxShards`, and they ignore `growCooldownMs` inside the call,
which exists to space growths across requests and not to stop an escalation inside one. `grownAt`
moves once, at the end, so the cooldown governs what follows.

The class owns the key and the timing on purpose. Both are easy to get wrong from outside and both
fail silently: a key of `<tableName>` measures the whole table instead of one shard, a check that runs
after dispatch protects nothing, and a caller that records a breach without asking for a new index
leaves the request on the hot shard. Inside the class each of them is one assertion in a test.

- **Key the binding on the shard, not on the table.** The budget of a shard is a constant, because it
  is the limit of one Durable Object, and the width is what scales with the table. One threshold
  therefore covers every table size, and no tier ladder is needed.
- **Set the threshold at the full per-shard budget in per-colo terms.** The binding cannot estimate
  global load and must not try. It answers one question: is this colo alone about to overload this
  shard. The observation of 4.2.4 covers every other colo, one round trip later. A lower threshold
  fires early and buys a cold start of hundreds of ms on the small-table path that this plan exists to
  improve, while a late answer costs about one round trip of queueing.
- `minTxCoordinators` is the lever for a table that starts cold and busy, not a lower threshold.
- The hook is optional. Without it the class skips the check, which is how milestones 1 to 3 run. A
  hook that rejects counts as "inside the limit", so a binding that fails never fails a transaction.

#### 4.2.6 Signal: the overload error

An overload error from a shard is the only signal that is a fact and not an estimate, so it grows the
width first and ignores the cooldown.

**A cross-shard retry is safe only when the platform refused the request before the object ran any
code.** A tokenless retry on another shard is a different transaction, with a different
`transaction_id` and a different generated token. If the first shard had already written `tc_state`
and dispatched its prepares, the second transaction meets the locks of the first, answers
`pending_conflict`, and cancels, while the first commits through its alarm. The client then reads
`cancelled` for a transaction that committed. That is worse than a lost response, because the caller
gets a confident wrong answer instead of an error it owns.

The predicate therefore **fails closed**. It matches only the platform's overload rejection, in the
shape of `isSinglePartitionFastPathFallbackError`, which is the existing example of one error that
means "nothing happened". Every other failure, a lost connection and a reset object included, reaches
the caller.

The predicate lives at the call site, next to the loop it governs. The class holds the part that is
policy — which shard an attempt uses, and when two distinct overloads widen the pool — and `getShard`
carries it in its `attempt` argument:

```ts
for (let attempt = 0; attempt <= 2; attempt++) {
    const { shard, width } = await router.getShard(tableName, limits, transactionId, attempt);
    try {
        const res = await router.withRequest(tableName, () => callShard(shard));
        router.mergeObservation(tableName, width, res._internal.poolObservation);
        return res;
    } catch (err) {
        if (!isDurableObjectOverloadedError(err)) throw err;
        router.observeOverload(tableName, shard);
        lastError = err;
    }
}
throw lastError;
```

The escalation ladder needs no probe, because a failed request needs a destination anyway:

1. Attempt 0 fails with an overload. The caller reports the shard with `observeOverload`.
2. Attempt 1 goes to a different index inside `[0, width)`. When the width is 1, the router grows to
   at least 2 first, so an index exists.
3. Attempt 1 also fails with an overload. Two distinct shards prove the overload is not local to one
   shard, so the router grows the width now.
4. Attempt 2 goes to one of the **newly added** indices. A shard that has never existed cannot be
   overloaded, which trades a cold start for a failure.

There is no attempt 3. When the pool is saturated, more attempts only add latency to a request that
fails anyway.

Separate requests reach the same conclusion without a retry: `overloadedShards` holds the shards that
answered with an overload inside the last `rateWindowMs`, and two distinct entries grow the width.

This loop cannot run away. An overload grows the width, a growth creates cold shards, and a cold shard
cannot answer with an overload.

#### 4.2.7 Local signals and shrink

- `withRequest` wraps one coordinator call. It runs `onDispatch`, runs the call, and runs `onSettle`
  in a `finally`, so a thrown error cannot leak the count. A leaked increment never heals: `inflight`
  stays high, `inflightHwm` stays with it, and the width is pinned wide for the life of the isolate
  with nothing to report the fault.
- `onDispatch` increments `inflight`, raises `inflightHwm`, and counts one request in the window.
- `onSettle` decrements `inflight`.
- Any call rolls the window when `now - windowStartedAt >= rateWindowMs`, stores `lastRate`, and halves
  `inflightHwm` so a single burst does not hold the width up for ever.

```
localBound = ceil(max(inflightHwm / targetInflightPerShard, lastRate / targetRpsPerShard))
```

A shrink runs on a window roll, at most once per `shrinkIntervalMs`, and only when `localBound` and the
last observation bound both sit under half the current width. It halves the width, with `minShards` as
the floor.

#### 4.2.8 Worked example

One table, `orders`, with `minShards` 1 and `maxShards` 1,024, and the default constants:
`targetInflightPerShard` 100, `targetRpsPerShard` 500, `minObservationMs` 2,000, `growStepMax` 64.
The fleet is already busy. This isolate is not: it has just started and holds no entry for the table.

**Request 1.**

| Step | Value |
| --- | --- |
| LRU lookup | miss, so the entry starts at `width = minShards = 1` |
| `getShard(orders, limits, tx-a1, 0)` | `hash("tx-a1") % 1 = 0` |
| `isWithinRateLimit("orders.d-0")` | `true`, so the shard stands. Answers `{ shard: 0, width: 1 }` |
| `withRequest` | `inflight` 0 → 1, `inflightHwm` 1, `windowCount` 1 |
| shard 0 answers | `{ inflightNow: 300, requests: 4200, observedMs: 6000, peerWidth: 3 }` |
| `withRequest` finally | `inflight` 1 → 0 |

`mergeObservation(orders, widthUsed = 1, observation)`:

```
the entry is unseeded, so it seeds first: max(1, peerWidth 3) = 3
widthFromInflight = 1 * 300 / 100                    = 3
observedMs 6000 ≥ minObservationMs 2000, so the rate term counts:
  rate            = 4200 / (6000/1000)               = 700 requests per second
  widthFromRate   = 1 * 700 / 500                    = 1.4
observationBound  = ceil(max(3, 1.4))                = 3
width             = min(max(3, 3), 1024)             = 3
```

The growth adds 2 shards, which is under `growStepMax`, so it stands. The entry records `grownAt`, and
the warm-up range becomes `[1, 3)`. The caller reads `takeWarmupRange`, gets `{ start: 1, end: 3 }`,
and sends a `ping()` to each index through `ctx.waitUntil`. The local signals said nothing here:
`inflightHwm` of 1 gives `ceil(1/100) = 1`, which loses to the observation.

**Request 2, 300 ms later, same isolate.**

| Step | Value |
| --- | --- |
| `getShard(orders, limits, tx-a2, 0)` | `hash("tx-a2") % 3 = 2` |
| `isWithinRateLimit("orders.d-2")` | `true`. Answers `{ shard: 2, width: 3 }` |
| shard 2 answers | `{ inflightNow: 20, requests: 100, observedMs: 100, peerWidth: 3 }` |

Shard 2 is the object the ping created 100 ms ago. Several isolates found it at once, so its window
holds 100 requests over 100 ms.

```
the entry is seeded already, so peerWidth is ignored
widthFromInflight = 3 * 20 / 100                      = 0.6
observedMs 100 < minObservationMs 2000, so the rate term is dropped
observationBound  = ceil(0.6)                         = 1
width             = min(max(3, 1), 1024)              = 3      unchanged
```

The gate earns its place here. Without it the rate term would read `3 * (100/0.1) / 500 = 6` and
double the pool off a 100 ms sample of a burst that is about to disperse — and each new shard it
created would produce another short sample. The observation is widen-only, so the low bound of 1 does
no harm on its own.

**What follows.** Shard 2 keeps taking traffic, and its window passes 2,000 ms. Its rate term then
counts, and if the fleet load is real the width climbs again: an answer of 300 concurrent at width 3
gives `3 * 300 / 100 = 9`. Each answer multiplies the width by the same ratio, so the isolate reaches a
correct width in three or four round trips. When the traffic stops, no observation arrives to widen
anything, `inflightHwm` halves on each window roll, and after `shrinkIntervalMs` the width halves,
down to `minShards`.

**A fresh isolate in a busy colo.** The same table, and a colo that has served it for a while: the
other isolates there run at width 16, and the rate limit threshold is 5,000 requests per 10,000 ms per
shard, which is the `targetRpsPerShard` of 500 written in the units of the binding. A new isolate
starts and holds no entry.

| Step | Value |
| --- | --- |
| LRU lookup | miss, so `width = minShards = 1` |
| pick | `hash("tx-c1") % 1 = 0` |
| `isWithinRateLimit("orders.d-0")` | `false`. Shard 0 takes about 520 requests per second from this colo |
| widen | width 1 has nothing else to try, so `width` 1 → 2, warm-up range `[1, 2)` |
| pick again, not shard 0 | shard 1 |
| `isWithinRateLimit("orders.d-1")` | `false` |
| widen | width 2 has nothing else to try, so `width` 2 → 4, warm-up range merges to `[1, 4)` |
| pick again, not shard 0 or 1 | `hash("tx-c1") % 4 = 3` |
| `isWithinRateLimit("orders.d-3")` | `true`, so shard 3 stands. The second widening is also the last |
| answer | `{ shard: 3, width: 4 }`, and `grownAt` is now set |
| `withRequest` | `inflight` 0 → 1, `inflightHwm` 1, `windowCount` 1 |
| shard 3 answers | `{ inflightNow: 40, requests: 3600, observedMs: 9000, peerWidth: 16 }` |

`mergeObservation(orders, widthUsed = 4, observation)`:

```
the entry is unseeded, so it seeds first: max(4, peerWidth 16) = 16
widthFromInflight = 4 * 40 / 100                      = 1.6
rate              = 3600 / 9                          = 400 requests per second
widthFromRate     = 4 * 400 / 500                     = 3.2
observationBound  = ceil(max(1.6, 3.2))               = 4
width             = min(max(16, 4), 1024)             = 16
```

Six things in that trace are worth naming.

- **The binding did work no other signal could.** It steered the first request of the isolate away from
  two shards that this colo already pushes to their budget, and onto one it confirmed, before any RPC
  left. The isolate had no history, and an observation only arrives with a response.
- **The third question is what found the healthy shard.** A ladder that stopped after two would have
  sent this request to shard 1, which the binding had just flagged. Three colo-local questions cost
  single-digit milliseconds each and no cold start, and they run only on a request that already met a
  hot shard.
- **The seed, not the ladder, reaches the fleet width.** The ladder took the isolate from 1 to 4 and
  found a healthy shard, which is all it is for. `peerWidth` then moved it to the 16 the colo uses, in
  the same round trip. Without the seed the isolate would sit at 4, cover only shards 0 to 3, and need
  several requests spread over `growCooldownMs` to climb, with every one of them sent to a shard the
  binding had already flagged.
- **The estimator alone would not have found 16.** It answers 4, and it is right to: shard 3 sits
  inside its budget, so nothing about that shard asks for a wider pool. The estimator measures one
  shard, and the seed measures the fleet.
- **The warm-up range stayed at `[1, 4)`.** The seed to 16 added nothing to it, because the shards it
  took in are ones the colo already runs. The ladder's own widening did add `[1, 4)`, and the class
  cannot tell that those are warm too, so three pings land on warm objects and return at once.
- **The stats after the request** are `width` 16, `seeded` true, `inflight` 0, `inflightHwm` 1,
  `windowCount` 1, `grownAt` set, `overloadedShards` empty, and the warm-up range emptied by
  `takeWarmupRange`. The cooldown now blocks a further breach-driven growth for 2,000 ms. An overload
  error still bypasses it (4.2.6).

#### 4.2.9 Warm-up

A growth answers a burst, so the newly added shards are cold exactly when latency matters. After a
growth the caller reads `takeWarmupRange` and sends a `ping()` to each index in it through
`ctx.waitUntil`, then routes real traffic there from the next request. `ping()` is an RPC that returns
at once. The cost that matters is the construction of the object and its migrations, and the ping pays
it off the critical path.

**The pending range is two integers, not a list.** A growth extends the width upward, so the shards it
adds are `[width_before, width_after)` and nothing else. A second growth before the caller reads moves
only the end, and a shrink clamps the end to the new width, which empties the range when the shrink
passes the start. Two integers therefore describe every case, and no growth allocates.

The pings need no per-shard bookkeeping either. `growCooldownMs` limits a table to one growth every
2,000 ms, and `growStepMax` limits a growth to 64 shards, so the pings are bounded by construction. A
map keyed by `tableName` and shard would hold `tables * width` entries, which competes with the memory
budget of 4.2.10 for no extra protection.

#### 4.2.10 Memory and eviction

The router holds at most `maxTables` (256) entries. When it admits a new table at capacity, it evicts
the entry with the lowest `lastRate`, and breaks a tie with the oldest access. A linear scan over 256
entries is cheap enough that a heap is not worth its code.

Eviction by rate and not by recency is deliberate. A table with a high rate is the one whose width was
expensive to learn and whose next request arrives soonest. A table evicted by mistake starts again at
`minShards` and relearns its width from its first observation.

Each entry holds a fixed number of counters, the two integers of the warm-up range, and
`overloadedShards`. That set never needs more than two entries: the rule of 4.2.6 fires on the second
distinct shard, and the entry grows the width and clears the set at that moment. An entry is therefore
a fixed size, and the total is bounded by `maxTables`, whatever the number of tables the Worker serves.

#### 4.2.11 Correctness invariants

1. **The width never changes an outcome.** `initiateWrite` stamps `coordinator_do_id` on every lock,
   and recovery resolves it with `idFromString`, so the width routes only the initial claim.
2. **A tokenless transaction has no replay contract**, so any width is legal at any moment, and two
   isolates can disagree.
3. Together: a wrong width costs latency. It cannot cost atomicity, idempotency or durability.
4. **The one exception is the cross-shard retry** of 4.2.6, which is safe only for a rejection that
   proves no code ran.

#### 4.2.12 What this plan changes outside the router

Everything else lives inside `CoordinatorPoolRouter`. These are the points where this plan touches the
existing code.

- **`FokosDBOptions`** gains `minTxCoordinators` (default 1) and `maxTxCoordinators` (default 1,024).
  `numTxCoordinators` keeps its name, its default and its meaning, and now sizes the pool for
  transactions that carry a `clientRequestToken` alone (4.2.1).
- **A second shard group per table**, `fokos_tc.<tableName>.d-<i>`, for transactions without a token
  (4.2.1).
- **`TransactWriteItemsOptions` stays as it is.** A per-call override of the pool size buys nothing
  once the pool grows on its own, so the option list keeps `items` and `clientRequestToken`.
- **`TransactionCoordinatorDO`** gains an in-flight count, a request counter over a tumbling window,
  the largest `_internal.widthUsed` reported in that window, `_internal.poolObservation` on
  `InitiateWriteResponse` (4.2.4), and a `ping()` RPC that only constructs the object (4.2.9).
- **`FokosDB`** holds one module-level `CoordinatorPoolRouter`, runs the attempt loop of 4.2.6 for a
  tokenless transaction, and strips `_internal` before it answers the caller.
- **FIXME: `destroy()` needs a rework for the dynamic pool.** It walks a fixed shard range today, and
  the dynamic pool has no fixed range. Nothing in this plan depends on it: `destroy()` is a debug and
  admin tool, and every shard already reclaims its own storage through its sweep (4.2.1).

  The likely fix removes the problem instead of scaling the traversal. Each coordinator shard arms an
  alarm and deletes itself with `deleteAll()` and `abort()` about 10 days after its last request. A
  shard idle that long holds nothing, because the sweep deleted its `tc_state` rows one idempotency
  window after each transaction completed, which is 10 minutes. `destroy()` then reaches no
  coordinator at all.

  One condition guards the self-delete: **a shard must not delete itself while it holds a non-terminal
  transaction.** An unreachable participant keeps a transaction in `COMMITTING` for as long as it stays
  away, and the shard is the only holder of that decision. The alarm therefore checks for a
  non-terminal row, and re-arms instead of deleting when it finds one.

#### 4.2.13 Testing

`CoordinatorPoolRouter` takes a clock and its constants, and returns indices, so its tests need no
Durable Object and no mock.

- A fresh table answers `minShards` and routes inside `[0, minShards)`.
- `hash(transactionId) % width` spreads a large set of ids evenly over the width.
- An observation with a high `inflightNow` widens the pool. An observation from a shard with
  `observedMs` of 0 never widens it through the rate term.
- `mergeObservation` uses the `widthUsed` it is given and not the current width: a growth between the
  dispatch and the response does not change the bound the observation produces.
- The first observation for a new entry seeds the width from `peerWidth`, and a later observation with
  a larger `peerWidth` leaves the width alone.
- A `peerWidth` below the current width never narrows it, and a `peerWidth` above `maxShards` clamps.
- An entry the LRU evicted and admitted again seeds from `peerWidth` a second time.
- An observation that suggests a smaller width leaves the width unchanged, including the two-isolate
  case of 4.2.4 where a cold shard suggests 1 to a caller at width 10.
- A growth adds at most `growStepMax` shards, and a second growth inside `growCooldownMs` is refused,
  unless an overload drives it.
- Two overloads on distinct shards grow the width. Attempt 2 answers an index that the growth added.
- One overload on one shard, with the second attempt succeeding, does not grow the width on its own.
- A width of 1 with an overload grows to 2 before it answers attempt 1.
- The width halves after `shrinkIntervalMs` of low local load, and stops at `minShards`.
- The width never exceeds `maxShards`, whatever the observation.
- Admitting table 257 evicts the table with the lowest rate, and not the least recently used one.
- `takeWarmupRange` answers `[width_before, width_after)` once, and then answers an empty range.
- Two growths before a read merge into one range that covers both, and a shrink clamps the end, which
  empties the range when the shrink passes the start.
- `getShard` asks `isWithinRateLimit` for the key `<tableName>.d-<shard>` of the shard it is about to
  name, and asks nothing else.
- A hook that answers `false` makes `getShard` widen and answer an index from the wider range, and the
  index it answers is not the one that breached.
- At a width above 2, one breach makes `getShard` re-pick at the same width without widening, and two
  distinct breaches at that width make it widen.
- A hook that answers `false` for every shard makes `getShard` ask `maxRateChecks` times, widen no more
  than twice, and answer a shard.
- The ladder inside one `getShard` call widens while `growCooldownMs` has not passed, and the growth it
  leaves behind then blocks the next request from widening on a breach.
- A router with no hook never widens from this signal and answers the same index as one whose hook
  always answers `true`.
- A hook that rejects leaves the width unchanged, and `getShard` still answers an index.
- A growth that lands while `isWithinRateLimit` is pending is not lost: `getShard` answers from the
  width that exists when the hook resolves, and not from the width it read before the await.
- `withRequest` decrements `inflight` when the wrapped call answers and when it throws, and it
  rethrows the original error unchanged.

In the Workers runtime (`@cloudflare/vitest-pool-workers`):

- A tokenless multi-partition transaction commits through the dynamic pool, and a token-bearing one
  through the fixed pool, and the two pools hold different Durable Objects.
- A tokenless transaction in flight recovers after the width changes, because its lock carries
  `coordinator_do_id`.
- `_internal` never reaches the response of `FokosDB.transactWriteItems`.

### 4.3 Open Questions

#### 4.3.1 The two targets

`targetInflightPerShard` is 100 and `targetRpsPerShard` is 500, which is half the Durable Objects soft
limit. The second number sets where the two bounds cross, which is 200 ms of transaction latency, and
therefore which bound governs the normal case. TODO: measure the throughput and the latency of a
coordinator shard under transaction load.

#### 4.3.2 The rate binding threshold

The binding is per colo, and the number of colos that serve one table is unknown and changes. 4.2.5
argues for the full per-shard budget, because an early answer costs a cold start and a late answer
costs about one round trip.

A threshold set far below the real per-shard budget makes every question a breach. Each request then
asks four times and widens twice, and `growCooldownMs` holds the next widening for 2,000 ms, so the
width reaches `maxShards` in about 8 seconds and waits for `shrinkIntervalMs` to come back. The damage
stops at `maxShards`, which is why that rail is per table. TODO: measure the per-shard budget before
fixing the threshold.

---

## 5. Alternative Options

**One namespace, two routing rules.** Route tokened traffic over `[0, Nstable)` and tokenless traffic
over `[0, W)` of the same shard group, so the two share warm objects. Rejected: every input of the
control loop then measures both populations. The observation of 4.2.4 attributes tokened load to the
tokenless population, the rate binding of 4.2.5 counts mixed traffic under one key, and an overload
error becomes ambiguous. `minTxCoordinators` gives a small table its warm shard without the
contamination.

**A tiered pool for tokened traffic, with a probe on replay.** Restrict the tokened size to a ladder of
powers of two, and probe the previous tier when the current one holds no record for a token. The
finite idempotency window bounds the probe to one window after a change. Rejected for now: the probe
needs a marker of when the tier changed that every isolate agrees on, and every signal in this plan is
local. The tokened path is also the minority path for a client that writes idempotent transactions
with condition expressions.

**A width derived from the partition count.** Rejected: a partition count is not a transaction count,
most operations on a table can be non-transactional, and a single-partition transaction never reaches
the pool.

**Pick the best of two shards.** Sample two indices and send the request to the one with the better
recent observation. It smooths an uneven spread better than one uniform pick. Deferred: it needs
per-shard state in the isolate, which is `tables * width` entries, and uniform routing over a width the
control loop already keeps correct leaves little for it to fix.

**Sticky routing per isolate.** Each isolate holds one shard until an overload or a width change. It
keeps fewer objects warm per isolate. Rejected: it concentrates the load of a busy isolate on one
shard, and the width is then a poor description of the spread, which breaks the estimator of 4.2.4.

---

## 6. Frequently Asked Questions

**Why not one Durable Object per transaction?**
Every transaction and every recovery then pays a cold start of hundreds of ms.

**Why can the ceiling be 1,024 and not 256?**
Nothing in the platform stops thousands. At `targetRpsPerShard` of 500 a ceiling of 1,024 carries about
512,000 transactions per second for one table. A shard stays warm while
`W ≤ R * T_evict`, and the control law already keeps the width proportional to the load, so the ceiling
guards a runaway loop and does not size the pool.

**Two isolates can disagree about the width. Is that a problem?**
No. The width routes only the initial claim of a new transaction, and a tokenless transaction has no
replay contract, so any width is legal (4.2.11). Disagreement affects the estimator of 4.2.4, which is
why an observation only ever widens.

**Why does the local signal shrink the width when it cannot see the fleet?**
Because the width is isolate-local. It governs only the requests this isolate sends, and an isolate
whose traffic stopped sends almost nothing. Its first request afterwards gets an observation that
widens it again, and the rate binding of 4.2.5 covers that request in the meantime.

**Why is a 429 retried on another shard, when other failures are not?**
An overload rejection proves the object ran no code, so the retry is a first attempt. Any other
failure can leave a transaction in flight, and a retry is then a second transaction that meets the
locks of the first, cancels, and reports `cancelled` for a transaction that commits (4.2.6).

**What does a table gain if it never widens past `minTxCoordinators`?**
Milestone 1 on its own: a dedicated shard group, and one warm shard instead of a claim spread over the
fixed pool. For a table whose transactions are mostly single-partition, that is the whole benefit, and
the control loop never engages.

**Where does the state live between requests?**
In one module-level `CoordinatorPoolRouter`. A Worker builds a `FokosDB` for each request, so an
instance field would reset on every request and the width would never leave `minShards`.

**Why not a `WeakMap` keyed by table name?**
A `WeakMap` takes object keys, and the only identity that is stable across requests is the table name,
which is a string. A `Map` with the eviction rule of 4.2.10 is the only shape that bounds the memory.

---

## 7. References

- `docs/agent-plans/2026-08-30-bounded-stateful-transaction-coordination.md`
- `docs/agent-plans/2026-08-29-stateless-transaction-coordination.md` (rejected predecessor)
- `docs/agent-plans/2026-08-23-single-partition-transaction-fast-path.md`
- [Cloudflare Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Cloudflare Workers limits](https://developers.cloudflare.com/workers/platform/limits/)
- [Cloudflare Workers rate limiting binding](https://developers.cloudflare.com/workers/runtime-apis/bindings/rate-limit/)
