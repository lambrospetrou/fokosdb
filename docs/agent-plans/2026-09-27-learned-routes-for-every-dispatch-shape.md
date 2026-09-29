# RFC — Every dispatch shape uses the learned routes inside the partitions

**State:** Implemented
**Date:** 2026-09-27
**Author:** Lambros Petrou

**Status:** M1, M2, M3, and M4 are built. Sections 1 and 4 describe the code before this spec as "today". The cost
of one `findDeepestKnownRangeSlice` query in section 4.2.10 is not measured yet.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)
- [8. Appendix](#8-appendix)
  - [8.1 The call tree of a group dispatch](#81-the-call-tree-of-a-group-dispatch)

## 1. Overview and context

### 1.1 How a partition resolves an owner today

`FokosShardingRuntime.#resolve` in `src/sharding/runtime.ts` finds the owner of one route key. It reads durable
facts and three caches. The caches are the learned topology of the partition:

| Source | Kind | What it gives |
| --- | --- | --- |
| Topology and `fokos_route_overrides` | durable fact | the direct child, or the range root after a cutover |
| Hash arena (`HashTopology`) | cache | a jump to a deeper hash descendant |
| Promotion Bloom filter | cache | a guess that a descendant promoted the hash key |
| Learned range slices (`fokos_range_hierarchy`) | cache | a jump to a deeper range partition |

`ResolveOptions` turns off two of the caches: `bloom` and `learnedRange`. The hash arena has no switch. Each
dispatch shape uses a different set today:

| Shape | Operations | Arena | Bloom | Learned slices | Fallback after a cache miss |
| --- | --- | --- | --- | --- | --- |
| `point` | item RPCs | yes | yes | yes | yes, `#forwardPoint` |
| `range` | `apiQueryItems` | yes | yes | yes | yes, `#forwardRangeVisit` |
| `group` | `txPrepare`, `txCommit`, `txCancel`, `txReadForTransaction` | yes | no | no | no |
| `single_owner` | `txReadSnapshot`, `txExecuteSingleShot` | yes | no | no | no |

`#dispatchGroup` and `#dispatchSingleOwner` call `#resolve(key, EXACT)`, where `EXACT` is
`{ bloom: false, learnedRange: false }`. `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md` gives the rule:
"Group and single-owner shapes skip it" (the Bloom step).

Each transaction operation, and each stale-transaction recovery, uses a `group` or a `single_owner` shape. So no
transaction uses the Bloom filter or the learned range slices. The partitions still learn from transaction
traffic, because `#forwardTo` calls `#learn` on each result and on each routed error.

### 1.2 The cost

Example: the hash root splits once. The depth-1 hash leaf `D` promotes the hash key `H`, so `D` holds the route
override of `H`. The range tree of `H` has two levels: the range root `R`, the range router `R1`, and the range
leaf `L`. The coordinator sends
`txPrepare` for `(H, sk)` to the hash root, as `FokosShardingClient.resolveAll` does today.

| Path | Forwards |
| --- | --- |
| `apiGetItem` for `(H, sk)` today: root → `L` (Bloom hit, learned slice) | 1 |
| `txPrepare` for `(H, sk)` today: root → `D` (topology) → `R` (override) → `R1` → `L` | 4 |

A write transaction sends `txPrepare` and `txCommit`, so it pays these extra forwards two times. A read
transaction sends `txReadForTransaction` two times. The extra forwards grow with the depth of the range tree.
No data gives the share of transaction traffic that goes to promoted keys. The share can be high. A promotion
moves a hash key that grew past `hashSplitConditions.maxSizeMb * RANGE_PROMOTION_FRACTION`. Such a hash key holds
more data than the other hash keys, so it can also receive more transactions than they do.

### 1.3 Why the transaction shapes cannot use the caches as they are

Three gaps block a one-line change of `EXACT`:

1. **No fallback after a cache miss.** `#dispatchGroup` and `#dispatchSingleOwner` forward with `#forwardTo`
   directly. A cache hint can name a partition that does not exist or that has no data yet. `#forwardPoint` then
   forgets the hint and resolves again. The group and single-owner paths propagate the error. This is already a
   gap for the hash arena: a cached hash jump that answers `hash_partition_not_initialized` fails the operation.
2. **`txCommit` needs all the keys of one owner in one call.** `TransactionParticipant.commitLocal` in
   `src/shared/partition/transaction-participant.ts` compares the keys of the request with every lock row of the
   transaction on this partition. When the two sets differ, it throws `commit_keyset_mismatch`. This check finds
   a commit that misses a key. It also means that one owner must receive its keys in one sub-request.
3. **A Bloom false positive splits keys of one owner.** The Bloom filter answers for one hash key. A false
   positive sends one hash key to a range root, and sends the other hash keys of the same owner along the
   topology.

Example of gap 3 with gap 2. The hash leaf `D` owns the hash keys `H1` and `H2`. A transaction has locks on
`(H1, a)` and `(H2, b)` at `D`. The hash root receives `txCommit` for both keys:

1. The Bloom filter of the root gives a false positive for `H1`. The root sends `{a}` to the range root of `H1`,
   and `{b}` to `D`.
2. `D` compares `{b}` with its lock rows `{a, b}`, and throws `commit_keyset_mismatch`.
3. The range root of `H1` does not exist, and answers `range_partition_not_initialized`. A fallback sends `{a}` to
   `D`, and `D` throws `commit_keyset_mismatch` again.
4. The coordinator retries. The Bloom filter never forgets a key, so each retry fails in the same way. The
   transaction stays in `COMMITTING`, and its locks stay.

A single-owner operation has a smaller form of gap 3. A Bloom false positive makes keys of one owner look like
keys of two owners. The partition then answers `not_applicable`, and `FokosDB` takes the two-phase path. The
result is correct, but the two-phase path adds at least one more round of RPCs.

## 2. Goals and requirements

### 2.1 In scope

- The `group` and `single_owner` shapes resolve an owner with the Bloom filter and with the learned range slices,
  as the `point` shape does.
- The `group` and `single_owner` shapes handle the same cache-miss errors as the `point` shape, including a
  cached hash jump. Their retries turn Bloom off and keep learned range slices after milestone 2.
- In one dispatch of a `group` operation, each partition that runs the local handler receives the keys it owns in
  one sub-request.
- A Bloom false positive does not make a `single_owner` operation answer `not_applicable` for keys of one owner.
- The recovery paths get the change without a change of their own. The stale-transaction job and
  `debugForceResolveTransaction` call `dispatch`.
- The example of section 1.2 takes 1 forward after the root learns `L`.

### 2.2 Out of scope

- A route cache in `FokosShardingClient`. The caller side keeps its entry at the root. A later spec replaces it.
  `docs/agent-plans/2026-09-26-fokos-sharding-client.md` owns that work. Section 6 gives the rule from this spec
  that the caller cache must also keep.
- `owns()`, `belongsToTarget`, and the forward inside `fokosRequestPromotion`. They must stay exact. Section
  4.2.9 gives the reasons.
- A change to `commitLocal` or to any other host handler.
- A change to how a partition learns. `docs/agent-plans/2026-09-27-range-self-hint-and-route-evidence-floor.md`
  owns the learning path.
- The `range` shape. It already uses each cache.

### 2.3 Requirements

- A cache hint must never change a result. A missing, old, or false hint changes the latency only.
- The one-call-per-owner rule of section 4.2.2 must hold for each `group` operation, with and without a miss.
- A `group` operation must be safe under partial fan-out and repeated sub-requests for each key, even without
  Bloom. Section 4.2.7 gives the proof for the current operations.
- A fallback must start only after an error that proves no handler applied, or after an error on an operation
  that is idempotent for each key. Section 4.2.7 gives the proof for each operation.
- `MAX_FORWARD_RETRIES` (8) must bound the fallbacks of one fallback chain. Section 4.2.4 defines the chain.
- The change must not change a stored schema.
- The change must not add a production hook for a test.

## 3. Milestones

Each milestone ships alone and leaves the system correct.

1. **The miss ladder for a set of keys.** Move the decision of `#fallbackAfterMiss` into a form that takes one
   resolution, a list of keys, and the dispatch shape. Add the fallback to `#dispatchGroup` and
   `#dispatchSingleOwner`. Both shapes resolve with `EXACT` on the first attempt and on each retry. This
   milestone closes gap 1 for the hash arena.
2. **Learned range slices.** The two shapes resolve with `{ bloom: false, learnedRange: true }` on the first
   attempt and on retries. The learned slices keep the one-call-per-owner rule without a guard (section 4.2.2).
   This milestone removes the forwards below a partition that holds the route override.
3. **The Bloom step.** The two shapes first resolve with `{ bloom: true, learnedRange: true }`, with the speculation
   guard of section 4.2.3 and the single-owner rule of section 4.2.5. A retry keeps learned slices but turns Bloom
   off. This milestone removes the forwards above the partition that holds the route override.
4. **Documents.** Update the resolution order in `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`, the
   `ResolveOptions` comments, the `txCancel` comment in `src/server/do-partition.ts` (section 4.2.8), and
   `AGENTS.md` where it describes the shapes.

## 4. Proposed solution

### 4.1 High-level overview

Each dispatch shape resolves with all the caches. Two rules keep the transaction shapes correct:

1. **One call per owner.** In one `group` dispatch, each owner receives its keys in one sub-request. So two keys
   of one owner must resolve to the same target. The hash arena and the learned range slices keep this rule by
   their structure. A Bloom hit can break it. So a `group` operation uses a Bloom hit for a hash key only when no
   key of another hash key has the same exact target.
2. **A fallback after a miss.** When a forward fails because of a cache hint, the partition forgets the hint,
   resolves the keys of that forward again, and sends them to the new targets. A group or single-owner retry does
   not use Bloom. It can still use learned range slices. Point and range requests keep their current fallbacks.

```text
hash root, txPrepare for (H, sk)                       today       after this spec

  resolve (H, sk)                                       EXACT       HINTED
    override for H here?            no
    Bloom says H is promoted?       (skipped today)                 yes -> range tree of H
    learned slice for (H, sk)?      (skipped today)                 L
    speculation guard               -                               no other hash key goes to D -> keep the hit

  forward                                               -> D        -> L
  on range_partition_not_initialized / awaiting_data                forget the hint, resolve again, forward
```

A single-owner operation forwards one request along one chain, so the one-call-per-owner rule cannot break. It
needs only the fallback. When a Bloom hit makes its keys look like keys of two owners, the partition resolves
the keys again without the Bloom step before it answers `not_applicable`.

### 4.2 Technical details

#### 4.2.1 Resolution modes

`src/sharding/runtime.ts` keeps three constants:

- `EXACT`, which is `{ bloom: false, learnedRange: false }`. Its users are `owns()` and `fokosRequestPromotion`.
- `HINTED` (new), which is `{ bloom: true, learnedRange: true }`. Its users are `#dispatchPoint`,
  `#dispatchGroup`, `#dispatchSingleOwner`, and `resolveOwner`.
- `HINTED_NO_BLOOM` (new), which is `{ bloom: false, learnedRange: true }`. Its users are each `group` and
  `single_owner` retry, and the single-owner rule of section 4.2.5.

`EXACT` still uses the hash arena. Section 4.2.2 shows why the arena keeps the one-call-per-owner rule. The miss
ladder of section 4.2.6 turns Bloom off for each `group` and `single_owner` retry. It keeps learned slices on from
milestone 2 onward. The `point` shape keeps its current retry options.

#### 4.2.2 The one-call-per-owner rule

**Rule.** When two keys of one `group` request have the same owner, the entry partition sends them in the same
sub-request, or runs them in the same local call.

The runtime keeps the rule when each source of the resolution gives the same target for two keys of one owner.

**Topology and route overrides keep the rule.** They are durable facts. The owner of a key is one partition.

**The hash arena keeps the rule.** `HashTopology.findLeaf` walks the child indexes of the hash key. Two keys with
the same owner have the same indexes down to the owner. So the walk gives the same depth for both. The arena
learns only from partitions that answered, and hash partitions never merge. So the arena has no node below an
owner.

**The learned range slices keep the rule.** A range tree holds one hash key. Each slice that contains a sort key
is the owner of that key, or an ancestor of the owner. The owner is a leaf. So each slice that contains one key
of the owner also contains each other key of the owner. `findDeepestKnownRangeSlice` then gives the same slice
for both keys. Range partitions never merge, so each learned slice still exists.

**The promotion Bloom filter can break the rule.** It answers for one hash key. A false positive, or a true
positive before the cutover, sends one hash key away from the partition that owns it. The other hash keys of
that owner go along the topology.

Only `txCommit` needs the rule, because of `commit_keyset_mismatch`. The rule applies to each `group` operation,
so the runtime has one path.

The argument for the learned slices assumes that each stored slice is a partition that exists. `#learn` stores
only the slices of nodes that answered. After `destroy`, no partition of the shard group exists, and no request
reaches one.

#### 4.2.3 The speculation guard of a group

`#dispatchGroup` resolves each item with `HINTED` in one loop. It groups each item without a Bloom hit by
target, and it keeps the items with `via: "bloom"` aside, by hash key. When at least one item has a Bloom hit, it
runs the guard:

1. For each hash key `H` with a Bloom hit, find the exact target of `H`: `local`, or the hash descendant that the
   hash arena names.
2. When another hash key has an item at the exact target of `H`, send each item of `H` to that exact target.
   Otherwise, keep the Bloom resolution of each item of `H`.

The guard needs no exact resolution of the other items, for three reasons:

- The resolution of an item without a Bloom hit is exact, or it names a range partition of its own hash key. A
  range partition of another hash key is never the exact target of `H`.
- The Bloom filter answers for the hash key, so each item of `H` has a Bloom hit.
- `H` has no cut-over route override here, so its exact target is its topology owner. This is also the answer of
  `{ bloom: false, learnedRange: true }` for `H`.

The guard runs only when a Bloom hit exists. It reads `routerRole()` once, and it walks the arena in memory once
for each hash key with a Bloom hit. It reads no route override row.

**Why the guard is enough.** A Bloom group of `H` can fall back only to the exact target of `H`. The guard makes
sure that no other hash key has that target. So the fallback sends the keys of `H` to a partition that receives
no other key of the request from this partition.

More than one group of `H` can fall back to the same target only when the learned slices split the keys of `H`
into more than one group. Learned slices of `H` exist only when `H` cut over at some hash partition. Then the exact
target of `H` is a hash router, or the partition with the override, and neither runs the local handler for a key
of `H`. The owners in the range tree receive their keys once from that target, because of section 4.2.2.

The guard is conservative. Example: the exact target of `H` is the hash router `C`, and another hash key also
goes to `C`. The two keys can have different owners below `C`. The entry partition cannot know this, so it drops
the Bloom hit. The keys of `H` then go through `C`, and `C` applies its own caches.

#### 4.2.4 The group dispatch

`#dispatchGroup` changes as follows. The steps that do not change keep their current order.

1. Resolve each item with `HINTED`. An `out_of_range` item throws `partition_misrouted`, as now.
2. Run the speculation guard of section 4.2.3.
3. Admit the local items, run `beforeForward`, run the local handler, and start each remote group. These steps
   stay in one synchronous block with steps 1 and 2.
4. Each remote group runs its forward through the group ladder:
   1. Forward the sub-request with `#forwardTo`.
   2. On an error, ask the miss ladder of section 4.2.6 for the next options. The items of one group share one
      target. So they share one resolution: the same `via`, the same learned slice, and the same `relDepth`. The
      ladder forgets the hint one time for the group.
   3. When the ladder gives no next options, or the group reached `MAX_FORWARD_RETRIES`, throw the error.
   4. Otherwise, call `collector.forget` for the target. Then resolve each item of the group again with Bloom off
      and with the learned-slice setting of this milestone. Group the items by target. Do not turn Bloom back on
      later in this fallback chain.
   5. When some items resolve to `local`, admit them and run the local handler for them. This runs in one
      synchronous block with step 4.4, as the local fallback of `#forwardPoint` does.
   6. Send each new remote group through this ladder, with the retry count plus 1.
   7. The group ladder returns a list of parts: one for each forward that answered, and one for a local run.
5. `fail_fast` and `attempt_all` apply to the settled group ladders, as they apply to the remote calls now. A
   failed local run in step 4.5 counts as a failure of its group.
6. `descriptor.merge` receives each part of each group ladder, and the part of the local call of step 3.

Step 4.5 can run the local handler only when step 3 ran no local handler for this request. The guard makes sure
of this: a Bloom group falls back to `local` only when `local` is its exact target, and then no other item is
local.

**The retry count.** A fallback chain is one remote group of step 3 and each group that its fallbacks make. The
count starts at 0 for each remote group of step 3. A new group of step 4.6 takes the count of the group that
failed, plus 1. So `MAX_FORWARD_RETRIES` bounds each chain, not the whole dispatch. The reasons:

1. The other shapes use the same rule. `#forwardPoint` counts the retries of one chain, and `#forwardRangeVisit`
   counts the retries of one visit.
2. A group with a bad hint cannot use the retries of a healthy group. With one count for the whole dispatch, one
   group could use all the retries. Then a healthy group of an `attempt_all` operation, such as `txCommit`, fails
   at its first miss. The coordinator retries, so the result stays correct, but the commit takes longer.
3. The real number of retries is small. Each retry of the miss ladder removes the hint that caused it: it deletes
   a learned slice, invalidates an arena path, or turns the Bloom step off. So a chain soon has no hint left to
   miss. The theoretical bound of one dispatch is the number of items times `MAX_FORWARD_RETRIES`.

The limit does not depend on the retry policy of the coordinator. The coordinator retries the whole operation
after any failure.

#### 4.2.5 The single-owner dispatch

`#dispatchSingleOwner` changes as follows:

1. Resolve each key with `HINTED`.
2. When the keys do not resolve to one remote target, and at least one key has `via: "bloom"`, resolve each key
   again with `{ bloom: false, learnedRange: true }`. Section 4.2.2 shows that this answer puts the keys of one
   owner on one target.
3. When each key is local, run the local handler, as now.
4. When the keys are local and remote, or on two remote targets, answer `notApplicable`, as now.
5. When each key resolves to one remote target, forward the request. On an error, ask the miss ladder for the next
   options. Forget the hint, resolve each key again with Bloom off and with the learned-slice setting of this
   milestone, and go back to step 3. Keep Bloom off for the rest of this fallback chain. The local run of step 3
   then runs in one synchronous block with the new resolution.

A single-owner operation never fans out. Each partition on the chain forwards the whole request to one target, or
answers `notApplicable`, or runs the handler. Each error that starts a fallback comes from a check that runs
before the handler: the identity check, or the lifecycle gate. So a fallback never repeats a handler, and
`txExecuteSingleShot` can use it.

#### 4.2.6 The miss ladder

The ladder is the decision of `#fallbackAfterMiss`. It moves into a function that takes one resolution, the keys
of the forward, the error, `readOnly`, and the dispatch shape. It returns the next options, or null. For `group`
and `single_owner`, every retry uses `bloom: false`. It uses `learnedRange: false` in milestone 1 and
`learnedRange: true` from milestone 2 onward. A later miss in the same chain never turns Bloom back on. The
`point` shape keeps its current options.

The ladder checks these cases in order:

1. **A learned slice** and `range_partition_not_initialized`. Delete the slice with `deleteLearnedRangeSlice`.
   A point retry keeps the Bloom step as before. A group or single-owner retry turns it off. Learned slices stay
   on, so the next resolution can jump to another known slice instead of the range root.
2. **`via: "bloom"`** and `range_partition_not_initialized`, or `partition_migrating` with
   `importState: "awaiting_data"`. Forget nothing, because the filter cannot forget a key. The next options turn
   the Bloom step off, with the learned slices on.
3. **`via: "bloom"` on a read-only operation** and `repartition_not_cut_over`. Forget nothing. The next options
   turn the Bloom step off, with the learned slices on.
4. **`via: "hash"` with `relDepth > 1`** and `hash_partition_not_initialized`. Call `arena.invalidate` on the path
   of the hash key. A point retry uses `HINTED`. A group or single-owner retry keeps Bloom off and uses the
   learned-slice setting of its milestone.
5. **Any other case.** Forget nothing, and return null. The error goes to the caller.

A `group` or `single_owner` operation has `whileMigrating: "throw"`, so it never reads through its source. It
cannot receive `repartition_not_cut_over`, and case 3 never applies to it. On a hash partition without a cut-over
override, Bloom is the only way to enter the range tree directly. A retry with Bloom off follows the hash tree
until it reaches the override. That partition can still use its learned range slices to skip the range root.

For a group, case 4 forgets the path of one key of the group. Each key of the group has the same path down
to the target, so one call is enough.

A Bloom forward to a target in `awaiting_data` receives `partition_migrating` directly and retries without Bloom.
The error can also come from a range child below the target. On a `fail_fast` group, the router propagates the
child error, so the Bloom sender uses case 2. On an `attempt_all` group (`txCommit` or `txCancel`), the router wraps
the child error as `partition_fanout_failed`. The sender does not use the miss ladder for that error. It reports
the error to its caller. A coordinator keeps the transaction nonterminal and retries it. Section 4.2.7 covers
any part that already applied.

#### 4.2.7 Why a repeated sub-request is safe

A point operation or a single-owner operation starts a fallback only after an error that proves no handler ran.
A group operation is different. A router can send one group to several children, even without Bloom. Some owners
can apply their part before another owner returns an error. A miss fallback or a caller retry can send those keys
again. Each current group operation is idempotent for each key:

| Operation | The second call at an owner that applied |
| --- | --- |
| `txPrepare` | The lock row has the same transaction ID, so the item answers `passed`. |
| `txCommit` | The owner has no lock row for the transaction, so it answers `committed`. |
| `txCancel` | A second release finds no pending row and changes nothing. |
| `txReadForTransaction` | A read changes nothing. |

A repeated `txCommit` reaches each owner with the same keys as the first call, because of section 4.2.2. So an
owner that did not apply still receives its whole key set in one sub-request.

A new `group` operation must be safe to repeat for each key after partial fan-out. Turning Bloom off does not
remove this requirement: a split router can apply one part while another part fails. The runtime cannot check
whether a host handler meets it.

#### 4.2.8 The partitions that a jump skips

A jump skips hash routers, the partition with the route override, and range routers. The hash arena already skips
hash routers today. The only operation that runs work on a router is `txCancel`: its `beforeForward` calls
`cancelLocal(transactionId)` on each partition of the path.

A skipped partition holds no lock row that only its own release can delete:

- A hash router or a range router between cutover and completion keeps its pre-cutover lock rows. The child
  imports them before it serves. `beforeComplete` calls `deleteAllPendingTx` when the split completes. A router
  runs no stale-transaction job, because `canSweepLocally()` is false.
- The partition that promoted a key has no lock on that key at cutover. `beforeCutover` refuses the cutover
  while a lock exists.

The comment on `txCancel.beforeForward` in `src/server/do-partition.ts` says "Every hop releases by transaction
id". Milestone 4 changes it: each partition that the cancel reaches releases by transaction ID, and a skipped
router keeps its rows until its split completes.

`docs/ideas/2026-09-26-promotion-moves-its-locks.md` proposes that a promotion moves its locks, and that
`txCancel` releases in its `local` handler. The source then keeps transfer copies until the target acknowledges.
The source cleanup deletes the copies. A jump that skips the source does not change that design.
UPDATE: see `docs/agent-plans/2026-09-29-promotion-moves-its-locks.md` sections 4.2.5 and 4.2.8 for changes.

#### 4.2.9 What stays exact

- **`owns()`**. The host sweeps (the stale-transaction job and the TTL sweep) must not skip a key that this
  partition owns. A Bloom false positive must not change the answer.
- **`belongsToTarget`**. It decides which rows move to a child. It is a durable fact, not a route.
- **`fokosRequestPromotion`**. It must reach the hash partition that owns the key, to queue the promotion there.
  A Bloom hit or a learned slice names a range partition, which is the wrong target. It keeps the hash arena.
- **The coordinator shard group** (`fokos.tc.<tableName>`). Its operations have the `point` shape, so they
  already use each cache. It has no promotion.

#### 4.2.10 Performance

- **Common path, no Bloom hit.** The resolution of each key reads the route override, as now. A key of a
  promoted hash key adds one `findDeepestKnownRangeSlice` query. The query uses the primary key of
  `fokos_range_hierarchy`, `(hk, sk_start_boundary, sk_end_boundary)`. `TODO: measure` the cost of one query. A
  transaction has at most `MAX_ITEMS_PER_TX` (100) items.
- **Bloom hit.** The guard adds one `routerRole()` read, and one arena walk in memory for each hash key with a
  Bloom hit.
- **Saved forwards.** Each skipped forward is one RPC between Durable Objects, which costs one network round
  trip. The example of section 1.2 goes from 4 forwards to 1, for each of `txPrepare` and `txCommit`.
- **Miss.** A Bloom false positive costs one extra forward, as on the point shape. The filter keeps the false
  positive, so each request for that hash key pays it. The default false positive rate is 1%: the
  `FokosShardingRuntime` constructor sets `falsePositiveRate: 0.01` when `caches.promotionBloom` is absent.

#### 4.2.11 Deployment and rollback

No stored data changes. During a rollout, an old partition resolves with `EXACT` and a new partition resolves with
`HINTED`. Each partition decides only its own next hop, so a mix of the two routes each key correctly. A rollback
returns to the current behavior. The learned rows stay valid hints.

#### 4.2.12 Testing

Tests run in the Workers runtime through `@cloudflare/vitest-plugin`.

- **Milestone 1, arena fallback.** In `test/partition-do/promotion.test.ts`, use a test-only cache setup to make a hash router name a descendant that does not exist. Prepare two keys of one owner,
  then commit through the router. Make Bloom answer `true` for only one key. The miss retry must keep Bloom off and
  send both keys to their owner in one call. Do not add a production hook. If no test-only setup can make this miss,
  report the arena fallback and Bloom-off retry as untested before milestone 1 ships. Run this test again in
  milestone 3. The test needs a spy on a prototype, and `tools/check-test-machinery.js` permits such a spy only in
  `promotion.test.ts`.
- **Milestone 2, learned slices.** In `test/partition-do/promotion.test.ts` or `range-split.test.ts`: build the
  shape of section 1.2 with `TestPartition` (`splitHash`, `makeRangeRoot`, `triggerRangeSplit`). Warm the caches
  with one read. Then `txPrepare` and `txCommit` for `(H, sk)` through the partition with the override have a
  `forwardCount` of 1 below that partition.
- **Milestone 3, Bloom hit.** The same shape through the hash root: `txPrepare`, `txCommit`, and `txReadSnapshot`
  have a `forwardCount` of 1.
- **Milestone 3, the guard.** Spy on `PartialRangeTopology.prototype.maybePromoted` so that it returns `true` for
  `H1` only, as the test "serves a read and a write at the source when a Bloom false positive names a range root
  before its cutover" does. Keep the `describe` sequential and at the top level, for the reason in that test.
  Then prepare and commit a transaction on `(H1, a)` and `(H2, b)`, where one hash leaf owns both keys. The commit
  succeeds, and the hash leaf applies both items. Without the guard, this test fails with `commit_keyset_mismatch`.
  Do it again with a spy that returns `true` for `H1` and `H2`, so that two Bloom hash keys have one exact target.
- **Milestone 3, before the cutover.** Hold `fokosInit` of the range root of `H` with `testHoldInit`, as the
  existing test does. Make the Bloom filter answer `true` for `H`. A transaction on `H` prepares and commits at the
  hash leaf.
- **Milestone 3, single owner.** With the same spy, `txExecuteSingleShot` on `(H1, a)` and `(H2, b)` of one leaf
  commits, and does not answer `not_applicable`.
- **Property suites.** `test/property-based/transactions-split.test.ts` and `transactions-concurrent.test.ts`
  must pass without a change. They run transactions while partitions split.

## 5. Alternative options

- **Let `commitLocal` apply a subset of the lock rows.** Then any split of the keys is safe. Rejected: the key-set
  check finds a commit that misses a key. Without it, a commit that misses a key reports `committed`, and the lock
  row waits for the stale-transaction job. That job can find `not_found` after `IDEMPOTENCY_WINDOW_MS`, and the
  write is lost. This spec also does not change host handlers.
- **Send the Bloom groups first, then the other groups.** The fallback of a Bloom group then joins the other keys
  of its owner. Rejected: it adds one full round trip to each transaction with a Bloom hit, which is the case this
  spec wants to make faster.
- **Collect all failed groups, then fall back together.** This keeps the rule without the guard. Rejected: the
  fallback of one group waits for the slowest group, and the local handler can still have run for other keys in
  step 3. The guard is simpler and has no wait.
- **One retry count for the whole dispatch.** Rejected: one group with a bad hint can use the retries of the other
  groups. Section 4.2.4 gives the reasons for one count for each fallback chain.
- **The guard for `txCommit` only.** Only `txCommit` needs the one-call-per-owner rule, so a descriptor flag can
  turn the guard on for `txCommit` only. The other group operations then keep each Bloom hit. Rejected: one path
  for each group operation is easier to prove and to keep consistent. The flag saves at most one forward, for a
  transaction that has a Bloom hit and another hash key with the same exact target. Add the flag only when a
  measurement shows that this saving is large.
- **Leave the transaction shapes exact, and let the caller cache do the work.** Rejected: the stale-transaction
  job and `debugForceResolveTransaction` run inside a partition and never use the caller cache. The caller also
  cannot see the Bloom filter and the learned slices of each partition.

## 6. Frequently asked questions

**Is a Bloom hit on a group operation safe when the promotion has not cut over?**
Yes. The range root answers `partition_migrating` with `importState: "awaiting_data"` before any handler runs.
The ladder resolves the keys again without the Bloom step, and they go to the hash partition that still owns them.
The guard makes sure that this partition receives no other key of the request.

**Why do the learned range slices need no guard?**
Section 4.2.2 shows that each learned slice that contains one key of an owner contains each key of that owner. So
two keys of one owner always resolve to the same slice.

**Does a jump skip a lock that only a router holds?**
No. Section 4.2.8 lists each skipped partition and the step that deletes its rows.

**Must the caller cache keep the same rule?**
Yes. A caller that groups keys by a cached entry must send the keys of one owner in one request, or `txCommit`
fails with `commit_keyset_mismatch`. A caller cache that learns only from partitions that answered keeps the rule
for the same reasons as section 4.2.2. A caller cache with a Bloom filter needs a guard like section 4.2.3.

**Does the speculation guard cost anything when no key is promoted?**
No. It runs only when at least one key has a Bloom hit.

## 7. References

- `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`
- `docs/agent-plans/2026-09-26-fokos-sharding-client.md`
- `docs/agent-plans/2026-09-27-range-self-hint-and-route-evidence-floor.md`
- `docs/agent-plans/promoted-keys-bloom-filter-cache.md`
- `docs/ideas/2026-09-26-promotion-moves-its-locks.md` — UPDATE: see
  `docs/agent-plans/2026-09-29-promotion-moves-its-locks.md` for changes.
- `packages/fokosdb/src/sharding/runtime.ts`
- `packages/fokosdb/src/sharding/hash-topology.ts`
- `packages/fokosdb/src/sharding/sharding-store.ts`
- `packages/fokosdb/src/server/do-partition.ts`
- `packages/fokosdb/src/shared/partition/transaction-participant.ts`
- `packages/fokosdb/test/partition-do/promotion.test.ts`

## 8. Appendix

### 8.1 The call tree of a group dispatch

The tree shows one `txCommit` request that enters a hash router. `txCommit` is a `group`
operation with `failurePolicy: "attempt_all"`. Each function is in `src/sharding/runtime.ts`, except the two
functions that name `do-partition.ts`.

```text
PartitionDO.txCommit                          (do-partition.ts)
└─ #api                                       (do-partition.ts: arms the TTL sweep)
   └─ fokos.dispatch
      └─ #guard                               (turns an error into a routed error)
         ├─ #ensureIdentity                   (checks the route context, stores a new policy)
         ├─ new RouteCollector
         └─ #dispatch
            ├─ [importing] #whileImporting    → throws partition_migrating (whileMigrating: "throw")
            └─ #dispatchGroup
               ├─ descriptor.items(req)       (the host gives one entry for each key)
               ├─ [no items] #admit → #beforeForward → #runLocal → #applySignals → return
               │
               ├─ #groupByOwner(HINTED)       ── the synchronous block starts here ──
               │  ├─ #resolve (for each key)
               │  │  ├─ #ownsByTopology       → out_of_range → partition_misrouted
               │  │  ├─ route override        → #rangeOwner (via "override", learned slice or range root)
               │  │  ├─ [bloom on] Bloom hit  → #rangeOwner (via "bloom", learned slice or range root)
               │  │  ├─ not a router          → local
               │  │  └─ router                → #arena().findLeaf → #hashDescendant (via "hash")
               │  │  (one loop: an entry without a Bloom hit → addToGroups; a Bloom hit → set aside by hash key)
               │  └─ [a Bloom hit] #guardBloomHits
               │     ├─ #hashTopologyOwner (for each Bloom hash key H) → the exact target of H
               │     └─ the exact target of H also gets another hash key → the entries of H go there
               │
               ├─ #admit (the local keys)
               ├─ #beforeForward              (txCancel: cancelLocal on each partition that the cancel reaches)
               │
               └─ #runGroups(retries = 0)
                  ├─ the local part:
                  │  ├─ #localCall
                  │  ├─ descriptor.subRequest
                  │  ├─ collector.add(#selfNode("executed"))
                  │  └─ #runLocal             → the host handler (synchronous)
                  ├─ for each remote group (started, not awaited):
                  │  └─ #forwardGroup         ── the synchronous block stops at the first await ──
                  │     ├─ descriptor.subRequest
                  │     ├─ #forwardTo
                  │     │  ├─ collector.countForward
                  │     │  ├─ stub[op](targetCtx, req)   → the next partition runs this tree again
                  │     │  ├─ success: #learn → collector.mergeForwarded
                  │     │  └─ error:   #learn → mergeForwarded → addRaiser → throw again
                  │     └─ on an error, the group ladder:
                  │        ├─ #missLadder(keys, resolution, e, readOnly, "group")
                  │        │  ├─ learned slice + range_partition_not_initialized → deleteLearnedRangeSlice
                  │        │  ├─ via "bloom" + not initialized or awaiting_data  → forget nothing
                  │        │  ├─ via "hash", relDepth > 1 + hash_partition_not_initialized
                  │        │  │     → #arena().invalidate → putHashArena
                  │        │  └─ any other case → null → throw again
                  │        ├─ [null, or retries ≥ maxForwardRetries] → throw again
                  │        ├─ collector.forget(target)
                  │        ├─ #groupByOwner(HINTED_NO_BLOOM)   (the entries of this group only, no guard)
                  │        ├─ #admit (the keys that are local now)
                  │        └─ #runGroups(retries + 1, [])   ← the same tree again
                  │           └─ throws its first failure (the local one first), or returns its parts
                  ├─ await the local value → keep its signals
                  ├─ #applySignals(beforeForward + local)
                  ├─ collector.add(#selfNode("merged"))   (only when a remote group exists)
                  └─ settle the remote groups
                     ├─ attempt_all: Promise.allSettled → parts and remoteFailures
                     └─ fail_fast:   Promise.all → the first rejection throws
               │
               ├─ a local failure   → throw the local error
               ├─ a remote failure  → throw partition_fanout_failed (cause: the first failure)
               └─ envelope(descriptor.merge(parts), collector.build())
```

How to read the tree:

- **The synchronous block.** The block starts at the first `#resolve` and stops at the first `await` in
  `#forwardGroup`. It contains the resolution, `#admit`, `#beforeForward`, the local handler, and the start of each
  remote RPC. No step yields inside the block, so a cutover cannot come between the owner decision and the write.
- **The fallback chain.** A fallback in `#forwardGroup` calls `#groupByOwner`, `#admit`, and `#runGroups` again,
  with `retries + 1`. One fallback chain is one path down this recursion. `maxForwardRetries` bounds each chain
  (section 4.2.4).
- **The failure path.** Inside a fallback, `#forwardGroup` throws the first failure without a change. Only
  `#dispatchGroup` wraps a remote failure as `partition_fanout_failed`. So each partition hop wraps an
  `attempt_all` failure one time.
- **The next partition.** Each `stub[op]` call in `#forwardTo` starts the tree again on the target, with a new
  `RouteCollector`. The routing of the target comes back through `mergeForwarded`.

The single-owner dispatch has the same first steps. `#singleOwner(HINTED)` resolves the keys. When a Bloom hit puts
them on more than one partition, it resolves them again with `HINTED_NO_BLOOM` (section 4.2.5). `#forwardSingleOwner`
then forwards the whole request, and its fallback asks `#missLadder` with the shape `"single_owner"`.
