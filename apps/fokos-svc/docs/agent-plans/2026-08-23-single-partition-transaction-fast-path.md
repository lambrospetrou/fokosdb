# Single-Partition Transaction Fast Path — Implementation Plan

A transaction whose items are all owned by one partition DO does not need two-phase commit. That DO
can validate and apply the whole set inside one `storage.transactionSync`, so it can answer in a
single round trip. This plan specifies the eligibility rule, the participant primitive, the failure
model, and an ordered set of milestones that can be reviewed and shipped independently.

## Goal

Collapse `transactWriteItems` and `transactGetItems` to one round trip when every item in the
transaction is owned by a single partition DO, and make that path behave like the non-transactional
write path (`apiPutItem`) rather than like a distributed protocol.

## Why the current flow is expensive

A transaction whose items share one hash key **already has exactly one participant**:
`tc_participants` is keyed on `partitionContext.doName`, and `PartitionTopologyRouterImpl.pickPartition`
resolves every hash key to its root partition. The fan-out is already one. We pay for 2PC anyway.

| Path | Serialized DO hops | Payload persisted |
| --- | --- | --- |
| Write, today | 3 + forwarding depth | `tc_items`, `pending_transactions`, `items` |
| Write, fast path | 1 + forwarding depth | `items` |
| Read, today | 3 + forwarding depth | — |
| Read, fast path | 1 + forwarding depth | — |

On top of the hops, the current write path sets a coordinator alarm, sets a participant stale-tx
alarm, writes and deletes a lock row per item, and leaves a `tc_state` row that is never collected.
The read path runs two full sequential phases to detect interleaving that cannot occur inside a
single DO.

## Scope

**In scope:** the eligibility predicate, a single-shot execute primitive on `TransactionParticipant`,
the two new `PartitionDO` RPCs, the client-side gate in `db.ts`, the opt-out option, and test
coverage of both paths.

**Deliberately out of scope:** any change to `do-transaction-coordinator.ts`. Milestones 1 and 2 do
not touch that file at all, so the 2PC path stays byte-for-byte identical and the fast path can be
turned off with a single option if it misbehaves.

---

## The eligibility rule

The safety condition is **exactly one DO executes every item**. It is not "one hash key" and not
"one partition name". Forwarding hops on the way to that DO cost latency but not correctness,
because the nodes in between own nothing and do nothing.

### Layer 1 — client hint, in `db.ts`

All items resolve to the same `partitionContext.doName`, and — for writes — the caller supplied no
`clientRequestToken`. Cheap, no I/O, necessary but not sufficient.

### Layer 2 — server authority, in `PartitionDO`

Run the existing `groupItemsByRouting` and read the shape of the result:

| Routing result | Action |
| --- | --- |
| `forwarded.size === 0` | Execute here, atomically |
| `forwarded.size === 1 && local.length === 0` | Forward the whole request to that one child |
| anything else | Throw the fallback error, touch nothing |

### Layer 3 — fallback

The fallback is a **thrown error**, not a response variant. The response unions stay honest —
`committed | rejected` for a write, `committed | aborted` for a read — and a caller that forgets to
handle the fallback fails loudly instead of silently recording "nothing happened" as an outcome.

It must be recognised by a **message substring sentinel**, with an `is…Error()` predicate beside
`isPartitionExceededDatabaseSizeError` in `do-partition.ts`. Durable Object RPC carries only an
error's message across the boundary — not its class, not any custom property — so a dedicated error
subclass would not survive the hop. This is the same constraint that produced `OVER_SIZE_SENTINEL`.

The error carries **zero side effects**, which is what makes it safe to raise from any depth of a
forwarding chain: it propagates up through the intermediate routers on its own, and the client then
runs the normal coordinator path and pays one wasted hop. A transport failure raises a different
error with no sentinel, so it is never mistaken for a fallback and is never retried as one.

### How the two shapes behave

- **Same hash key.** A hash key never spans two hash partitions: hash splits are by hash key, and
  `HashPartitionTopologyImpl.shouldAllow` is partition-wide and key-independent. The chain always
  converges on one DO. The only fan-out case is a promoted key whose range structure has split by
  sort key, with the transaction's sort keys straddling a boundary.
- **Same partition, different hash keys.** Eligible while the partition is whole. Once a split
  starts, everything forwards and different hash keys go to different children, so it falls back.
  Supported because the predicate is the same code either way, but it is not the reliable case.

---

## The participant primitive

One new method on `TransactionParticipant`: the validation loop from `prepareLocal` — pending-lock
check, condition evaluation, deletion watermark — followed directly by the effects from
`#applyCommitItems`, applied from the **request** items instead of from persisted lock rows.

```
validate every item  →  apply every item
        ── one store.transactionSync ──
```

### Rules the call site must hold

1. **No `await` between the routing decision and the apply.** `await ensureMigration()` runs first;
   then routing and the apply must sit in one synchronous block. Both `PromotionManager.statusFor`
   and `shouldAllow` are synchronous, and a split can only advance at an input-gate point, so this
   closes the split and promotion races completely.
2. **Still reject on a foreign lock** in `pending_transactions`, with `pending_conflict`, exactly as
   `prepareLocal` does. A concurrent 2PC transaction must still win or lose cleanly.
3. **Take no locks.** The path holds nothing, so it needs no cancel, no stale-tx alarm, and no
   recovery. It also can never be the cause of another transaction's `pending_conflict` — the net
   effect on the system is fewer conflicts, not more.
4. **The partition stamps the timestamp** with its own clock, the same way `apiPutItem` does. The
   coordinator is not involved, so there is no second clock to skew against.

`check` operations still bump `last_transaction_ts`; `transactGetItems` uses it as its second
conflict signal alongside the item version.

### Timestamps

Stamp plain `Date.now()`, and drop the `timestamp_conflict` check, the `clock_skew` check, and the
`maxDeletedTs` watermark check on this path. That is exactly what `apiPutItem` does today: it stamps
`Date.now()` and performs none of the three.

Those checks order a transaction against writes that could interleave between its prepare and its
commit. This path has no such window — one DO validates and applies the whole set serially inside one
storage transaction, so serializability comes from the execution order, not from comparing stamps.
Keeping the checks would only preserve two rejection classes that cannot fire for a real reason, and
keeping `clock_skew` is meaningless once there is no second clock in the protocol.

Do **not** stamp `max(now, highest touched last_transaction_ts + 1, …)` to force monotonicity. It
advances by one per *transaction* while wall clock advances by one per *millisecond*, so above
roughly a thousand transactions per second against the same item it ratchets away from real time
instead of healing — and an item at that write rate is exactly the one that gets promoted to a range
structure. Below that rate it self-corrects, but the failure lands on the hottest keys, which is the
worst place for it. `PartitionStore.upsertItem` already carries this warning against the increment
form, for the same reason; monotonicity is its job, not the caller's.

A lower stamp cannot corrupt the column. `PartitionStore` already enforces per-item monotonicity in
SQL — `upsertItem`, `bumpItemLastTransactionTs`, and `bumpMaxDeletedTs` all write
`MAX(existing, incoming)` — so a stamp from a partition whose clock lags a coordinator's is absorbed
rather than applied. The invariant comment on `upsertItem` is also where the increment form above is
already ruled out, for the same runaway reason.

The one observable effect: on such a write the item's `last_transaction_ts` does not advance, even
though the item changed. Nothing depends on it doing so. `transactGetItems` uses the item's `version`
as its primary conflict signal, and `v = v + 1` runs on every upsert, so the write is always
detected; `lastCommittedTs` is only the secondary signal that catches a delete and recreate landing
back on the same version. A later coordinator-path transaction compares its own stamp against the
same stored value it would have seen anyway, so no new rejection appears there either.

Dropping the prepare-time check does not let stale data overwrite newer data on this path. That check
closes the window between a transaction deciding and committing, during which another writer can
land. This path has no such window: it reads current state and applies inside one storage
transaction, so its write is the newest by construction. The `MAX` in the store is an independent
backstop on the column, not the thing doing that work.

### Split checks

After the synchronous apply commits, call `checkSplits` once, exactly where `apiPutItem` calls it.

**Once per transaction, not once per item.** Both `shouldSplit` implementations ignore their key
arguments; the decision is `databaseSize` plus `hasInFlightPromotions`, which is partition-wide.
A per-item loop would perform up to 100 database-size reads and 100 promoted-key queries for a
decision that cannot differ between them. Carry a comment at the call site stating that assumption,
so that the heavy-hitter statistics idea noted in `shouldSplit` trips over it rather than silently
under-counting a transaction that touched many keys.

**Call it bare, with no `try`/`catch`, exactly as `apiPutItem` does.** Everything it touches is local
to this DO: the topology cache, a database-size read, a promoted-keys query, a KV write, and an
alarm. The apply that just ran is proof that this DO's storage is working, so a throw out of
`checkSplits` means a defect, not a transient condition. Absorbing it would hide that defect and
leave the partition silently unable to split.

Promotion needs no change: `onItemUpserted` already fires `maybeQueuePromotion` per committed put.

A delete-only transaction will now call `checkSplits` where `apiDeleteItem` does not. Harmless — a
shrinking partition cannot cross the threshold — and not worth a special case.

---

## Reads

`transactGetItems` takes no idempotency token and persists nothing, so its fast path is unconditional
and carries no durability question at all.

When one DO owns every requested key, `readForTransactionLocal` runs with no `await` inside a
single-threaded DO — that already **is** a consistent snapshot. The second phase exists only to
detect interleaving across partitions, and there is none to detect. `read_conflict` therefore becomes
unreachable for single-partition reads, which is a robustness win under contention as well as a
latency one.

The `pending_write` abort stays, for parity with the multi-partition path.

---

## Writes and the idempotency token

A write transaction that carries a `clientRequestToken` **does not take the fast path**. An
idempotent replay is answered from the coordinator's ledger, and the partition keeps no such record.
Gating on the token preserves every idempotency guarantee we have today, including the operation-set
fingerprint mismatch check, for zero new storage and zero new failure modes.

Comment to carry at the gate:

```ts
// A transaction that carries a client request token does not use this path: an idempotent replay
// is answered from the coordinator's ledger, and a partition keeps no record of finished
// transactions.
//
// TODO: give the partition its own completed-transaction-token storage. Once a partition can
// recognise a token it has already executed and return that outcome, this restriction lifts and
// token-bearing single-partition transactions can take the same single round trip.
```

### Over-size backpressure

`groupItemsByRouting(items, "write", …)` throws when the partition is over its cap. Today that throw
is swallowed by the coordinator and reported as `{ outcome: "cancelled", reason: transient_error }`.
The fast path must produce the same answer, so the `db.ts` call site catches
`isPartitionExceededDatabaseSizeError` and maps it. This is the one place where preserving current
behaviour needs explicit code rather than the absence of code.

Typed size errors are a separate change, tracked with the per-operation cancellation reasons already
noted in `transaction-types.ts`.

### Retries

None. The write path in `db.ts` has no retry today and the fast path does not add one.

Consequence to accept knowingly: a partition that is mid-migration throws from `ensureMigration`, and
the coordinator's prepare currently retries that up to three times. The fast path replaces that hop,
so those transactions now surface the error to the caller. The throw happens before any write, so
nothing partially applies — the transaction fails where it used to retry.

---

## Opt-out

`FokosDBOptions.singlePartitionFastPath`, default `true`.

It belongs on the constructor, not on `TransactWriteItemsOptions` / `TransactGetItemsOptions`: the
fast path is an execution strategy that is semantically equivalent for the caller, so the caller has
no basis on which to choose it, and a public per-call option is very hard to remove later. The
option is immutable per `FokosDB` instance, and every test already builds its own instance.

---

## Milestones

Each milestone is independently reviewable, independently shippable, and leaves the system correct.

### Milestone 1 — Eligibility predicate, opt-out, read fast path

The smallest end-to-end slice. It persists nothing, so a mistake cannot corrupt data.

**Files**

- `transaction-limits.ts` — add `singlePartitionTarget(items): PartitionContextResolved | null`,
  returning the shared context when every item resolves to one `doName`. (This file will want a
  rename once it holds routing as well as validation; not part of this milestone.)
- `transaction-types.ts` — `ReadSnapshotRequest` / `ReadSnapshotResponse`, the latter being
  `committed` / `aborted(pending_write)`.
- `do-partition.ts` — the fallback sentinel and its `is…Error()` predicate, beside the existing
  over-size pair; `txReadSnapshot`, implementing layer 2 and the single-destination forward.
- `db.ts` — `FokosDBOptions.singlePartitionFastPath`; gate in `transactGetItems`; catch the fallback
  and fall through to the coordinator.

**Done when** a single-partition `transactGetItems` performs one RPC to the partition namespace, and
a multi-partition one is unchanged.

**Tests**

- Single-partition read returns the same items as the coordinator path, positionally matched.
- A pending lock still aborts with `pending_write`.
- The fallback error is raised, is caught by `db.ts`, and the coordinator path returns the right
  answer. A transport failure is **not** treated as a fallback.
- With the option off, the coordinator path runs and results are identical.

### Milestone 2 — Write fast path

**Files**

- `partition/transaction-participant.ts` — `executeSingleShot`.
- `transaction-types.ts` — `SingleShotRequest` / `SingleShotResponse`
  (`committed` / `rejected(reason)`). The request carries no timestamp; the partition stamps it.
- `do-partition.ts` — `txExecuteSingleShot`, reusing the milestone 1 routing shape; `checkSplits`
  after the apply, called bare as `apiPutItem` does.
- `db.ts` — gate in `transactWriteItems` (option on, no `clientRequestToken`, predicate matches);
  over-size mapping; the token TODO comment.

The response still carries `transactionId` and `idempotencyToken` so the public shape is unchanged;
`db.ts` generates both as it does today. Nothing stores them on this path.

**Done when** an untokened single-partition `transactWriteItems` performs one RPC, writes no lock
rows, sets no alarms, and leaves no coordinator state.

**Tests**

- Commit, condition failure, and `pending_conflict` against a concurrent 2PC transaction.
- Atomicity: a failed condition on one item leaves no partial writes **and** no lock rows.
- A transaction carrying a `clientRequestToken` still takes the coordinator path.
- A write that crosses the size cap still reports `{ cancelled, transient_error }`.
- `checkSplits` runs: a fast-path transaction that pushes the partition over its threshold queues a
  split, which transactional writes do not do today.

### Milestone 3 — Split-check parity on the transactional commit path

Fixes an existing gap rather than adding to the fast path, and is separable from both milestones
above.

`checkSplits` has exactly one caller, `apiPutItem`. `txCommit` does not call it, and the background
job only *runs* a split that is already queued — it never queues one. A workload that writes only
through `transactWriteItems` therefore grows without ever splitting. Milestone 2 fixes this for the
fast path; this milestone fixes it for the coordinator path, so the two transactional write paths do
not diverge on capacity behaviour.

**Done when** a partition driven over its threshold purely by multi-partition transactions queues a
split.

### Milestone 4 — Token-bearing transactions (deferred)

Not built here. Two routes are open and neither needs deciding yet:

- Give the partition completed-transaction-token storage, so token-bearing single-partition
  transactions reach one round trip.
- Have the coordinator keep its ledger and call `txExecuteSingleShot` when it has exactly one
  participant, which reaches two round trips with no new storage anywhere.

---

## Test coverage of both paths

`test/transactions.test.ts` runs twice, parameterized over `singlePartitionFastPath`, so every
assertion is checked against both paths.

Routing is deterministic: `hashRootIndex` depends only on the hash-key bytes and `rootTreesN`, and
the table name is not involved. Which path a given test takes is therefore fixed, never flaky.
Measured against the current suite at `rootTreesN: 100`:

| Test | Keys | Root indices | Path |
| --- | --- | --- | --- |
| isolation, non-tx put vs transaction | `iso-shared`, `iso-tx-only` | 94, 76 | coordinator |
| conflict, overlapping keys | `c-shared`, `c-only-a`, `c-only-b` | 79, 32, 35 | coordinator |
| serializability, loser retries | `ser-key` | 20 | **fast path** |
| idempotency, same token replays | `idemp-1`, `idemp-2` | 44, 75 | coordinator (token) |
| idempotency, token mismatch rejected | `mismatch-1`, `mismatch-2` | 31, 42 | coordinator (token) |
| all 9 transaction cases in `db.test.ts` | `rootTreesN: 1` | — | **fast path** |

Both idempotency tests pass a token, so the token gate keeps them on the coordinator path with no
change needed. Every transaction case in `db.test.ts` exercises the fast path for free, because that
suite runs at `rootTreesN: 1`.

### The one test that must change

The serializability test writes a single hash key with no token, so it becomes a fast-path
transaction. Its premise — two transactions contend on a pending lock and the loser retries — stops
existing, because the fast path takes no lock: both transactions serialize inside the DO and both
commit. It needs the option turned off to keep testing what it means to test, plus a fast-path
counterpart asserting that both transactions commit cleanly with no retry.

### New tests

- **Path pinning.** Assert which path a given key set takes, so a routing change cannot silently
  move coverage from one path to the other.
- **The fallback.** A promoted hash key whose range structure has split, with sort keys straddling
  the boundary, so the DO raises the fallback and the coordinator path finishes the work.
  This is the only case that exercises the fallback and it needs real setup — expect it to be the
  most expensive test to write.
