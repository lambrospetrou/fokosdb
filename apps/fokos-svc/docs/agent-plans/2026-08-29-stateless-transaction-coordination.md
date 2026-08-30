# Stateless transaction coordination

Date: 2026-08-29
Status: **rejected — the performance regression is a blocker.** The driver runs in a
Worker, which has no durable storage, so the decision write costs one network round
trip. A write with a `clientRequestToken` grows from three round trips to four.
Superseded by `docs/agent-plans/2026-08-30-bounded-stateful-transaction-coordination.md`,
which keeps the correctness analysis of this document and reuses its independent wins.

References:

- `docs/agent-plans/dynamodb-distributed-transactions-plan.md`
- `docs/agent-plans/2026-08-23-single-partition-transaction-fast-path.md`
- [Distributed Transactions at Scale in Amazon DynamoDB, USENIX ATC 2023](https://www.usenix.org/system/files/atc23-idziorek.pdf)
- [Workers `waitUntil` import, 2025-08-08](https://developers.cloudflare.com/changelog/post/2025-08-08-add-waituntil-cloudflare-workers/)

---

## 1. Goal and scope

Remove all transaction state from the coordinator Durable Objects. The transaction
outcome moves to a **ledger**: a second FokosDB instance that holds one small row per
transaction. The 2PC driver moves into the caller's Worker.

This gives:

1. No coordinator can fill up, because no coordinator stores anything.
2. Any coordinator can resolve any transaction. No coordinator is a single point of
   failure.
3. The coordinator pool becomes freely resizable.
4. Consistency is unchanged. A client that receives `committed` can read what it wrote.

The cost: a write with a `clientRequestToken` needs one more round trip than today.
Section 11 lists all benefits and drawbacks.

Not in scope:

- Changes to the 2PC semantics, the conflict rules, or the timestamp rules.
- Changes to the single-partition fast paths.
- Cross-table transactions.
- Location hints for the ledger or the coordinators. See 13.

---

## 2. The problem today

`FokosDB` creates a `StaticShardedDO` pool of `DEFAULT_NUM_TRANSACTION_COORDINATORS`
(100) coordinator shards (`db.ts`). Each shard stores many transactions in `tc_state`,
`tc_participants` and `tc_items`. Four problems follow:

1. **Storage grows without bound.** There is no garbage collection. `tc_items` holds the
   full payload, up to `MAX_PAYLOAD_BYTES_PER_TX` (4 MB) per transaction. A Durable
   Object holds at most 10 GB. That is 2,500 maximum-size transactions per shard,
   permanently.
2. **A down shard blocks its transactions.** `pending_transactions.coordinator_do_id`
   names one DO. The recovery alarm can reach that one object and no other. While it is
   unavailable, its locks stay held, and a held lock makes every non-transactional write
   to that key throw.
3. **The pool cannot be resized.** The shard that owns an idempotency token also stores
   the transaction state. Change the pool size, and a replay cannot find its state. The
   caveat in `db.ts` (`numTransactionCoordinators`) records this.
4. **Placement is arbitrary.** Each shard lives where it was first created. That location
   is unrelated to the caller and unrelated to the partitions.

---

## 3. The design in short

Three components:

- **The driver** runs in the caller's Worker. It prepares, decides, commits, and
  releases. It stores nothing. If it dies, recovery takes over.
- **The ledger** is a second FokosDB instance. It stores one row per transaction: the
  identity, the decision, and a recovery hint. It never stores payload. Every write to
  it is a compare-and-set.
- **The participants** (`PartitionDO`) are unchanged in role. Each one stores the payload
  for its own items in `pending_transactions`, and its stale-lock alarm is the recovery
  trigger, as today.

The `TransactionCoordinatorDO` pool remains, but only as a stateless recovery worker:
a poked shard reads the ledger, forces a cancel if the transaction is undecided, and
pushes the outcome to the other participants.

---

## 4. The two invariants

Everything below follows from these two rules.

> **I1. Single decision.** A transaction has exactly one decision record. It is written
> by compare-and-set. A driver may send `txCommit`, `txCommitDecided` or `txCancel` only
> after it has durably won that compare-and-set.

Prepare outcomes are not deterministic. `pending_conflict`, `timestamp_conflict` and
`clock_skew` depend on timing, so two drivers can reach opposite conclusions. The
compare-and-set prevents a split brain. Every relaxation in this document — many drivers,
shard fallback, a resizable pool — is safe only because of I1.

> **I2. Keys are routing, not payload.** A key set sent with a commit or a cancel names
> the descendants that can also hold locks. It never carries item data. An empty key set
> means "act locally, do not fan out". A lock holder that no fan-out reaches resolves
> itself through its own stale-transaction alarm.

I2 is not new. `CancelRequest.items` (`transaction-types.ts`) documents it today. This
proposal applies the same rule to commit.

---

## 5. Where the data lives

| Data | Home | Written at | Survives a driver crash |
| --- | --- | --- | --- |
| Item payload (`data`, `kind`, `conditions`) | the accepting participant's `pending_transactions` rows | prepare | yes |
| Item payload, second copy | the caller's own request | — | only if the caller replays |
| `txId`, `transactionTs` | ledger row, and every lock row | claim / prepare | yes |
| Decision and reason | ledger row only | the decision point | yes |
| Participant list | ledger row | claim, or the decision | yes |
| `operationsHash` | ledger row, token transactions only | claim | yes |

A participant stores **only its own items**. `prepareLocal` inserts one lock row per item
in the request it received, and a driver sends a partition only the items routed to it.
No participant knows the full key set, the participant list, or the size of the
transaction. The only global facts a participant holds are `transaction_id`,
`transaction_ts` and the ledger key, and every one of its lock rows repeats them.

Three consequences define the whole protocol:

1. **A prepare needs the payload, so only a caller can send one.** The original caller,
   or a caller that replays the same `clientRequestToken` with the same operations. A
   recovery driver can never start or extend a prepare phase.
2. **Finishing a decided transaction needs no payload.** The commit path applies
   `pendingRow.data` from the participant's own store and ignores the data in the
   request. So any driver can finish a decided transaction.
3. **An undecided transaction with no caller present always resolves to `CANCELLED`.**
   That is the only legal outcome before the decision, and it is why the ledger row
   stays small.

---

## 6. The ledger

### 6.1 The instance

A second FokosDB instance, used through the normal public API. It uses no transactions,
so there is no recursion: every access is a `getItem`, a conditional `putItem`, or a
`deleteItem`, and each is one hop to one partition.

- **Table name**: `fokos_tx.` + the main table name. The prefix is reserved:
  `PartitionContextImpl.create` rejects any user `tableName` that starts with
  `fokos_tx.`, so a user table can never collide with a ledger.
- **Namespace binding**: the same `PARTITION_DO` binding as the main instance. A DO name
  is `${tableName}.h.${root}…` (`PartitionIdHelper.doName`), so the prefix alone keeps
  the two instances apart. No new wrangler binding is needed.
- **`nsTx`**: required by the type, never used. The ledger runs no transactions.
- **`rootTreesN`**: three times the main instance's value, capped at 65,000. See 12.

Each `FokosDB` instance builds its own ledger instance from its own `PartitionContext`.
The table name is derived, so two databases can never share a ledger.

The ledger instance must never run a transaction. `FokosDB` marks it internally, and
`transactWriteItems` and `transactGetItems` throw on it. Without this guard, a future
change could make the ledger depend on the thing it exists to serve.

### 6.2 The row

One row per transaction:

```
hk = ledgerKey            // = clientRequestToken, else txId
{
  txId,                   // authoritative transaction id for this ledger key
  transactionTs,          // the timestamp every participant stamped at prepare
  state,                  // PREPARING | COMMITTED | CANCELLED
  reason?,                // RejectionReason, when CANCELLED
  participants: [doName], // recovery hint, see 9.2; empty on a T5 row
  operationsHash?,        // token transactions only
}
```

One row serves both the idempotency check and the decision. This is deliberate: a
participant knows only `txId`, so it could not find a separate row keyed by a token. The
lock row carries the ledger key instead.

`participants` is at most `MAX_ITEMS_PER_TX` (100) partition names of 64 hex characters:
about 6.4 KB worst case, a few hundred bytes in practice, far below `MAX_ITEM_BYTES`
(400 KB). Keys are not stored. 100 keys at maximum size is 153.6 KB, and it would buy
nothing: the only partition that needs a key already holds it.

### 6.3 The writes

Every ledger write is a compare-and-set. This table is the complete set — there is no
other way to move the row.

| # | Transition | Who | Condition | Row after |
| --- | --- | --- | --- | --- |
| T1 | absent → PREPARING | driver, token only | `item_not_exists` | `{txId, ts, opsHash, participants, PREPARING}`, `v=1` |
| T2 | absent → terminal | driver, tokenless only | `item_not_exists` | `{txId, ts, participants, COMMITTED\|CANCELLED, reason?}`, `v=1` |
| T3 | PREPARING → terminal | driver: the original, or a replay | `v = 1 AND txId = <mine> AND state = PREPARING` | `{…, COMMITTED\|CANCELLED, reason?}`, `v=2` |
| T4 | PREPARING → CANCELLED | recovery | `v = 1 AND txId = <the poked txId> AND state = PREPARING` | `{…, CANCELLED}`, `v=2` |
| T5 | absent → CANCELLED | recovery, tokenless | `item_not_exists` | `{txId, ts from the poke, CANCELLED}`, `v=1` |
| T6 | terminal → absent | driver with the full key set, after every participant confirmed, tokenless rows only | `v = <what it wrote> AND txId = <mine>` | absent |

Five rules govern them:

> **R-A. A terminal state is final.** No transition leaves `COMMITTED` or `CANCELLED`
> except T6. This is I1 in practice.

The `state = PREPARING` clause in T3 and T4 is not redundant with the version. The
version encodes terminality today only because exactly one write follows the claim. That
arithmetic breaks when anything else touches the row, and it never held for T2 and T5,
where a terminal row also sits at `v=1`.

> **R-B. Every write names its transaction, not only its version.** A version says "the
> row is as I last saw it". It does not say "the row is mine".

This matters only on the token path, where the ledger key is not the transaction id. The
failure it prevents: D1 claims `tok` with `txId=A` and stalls. TTL reclaims the row. The
client replays, and D2 claims `tok` with `txId=B`, at `v=1` again. D1 wakes and decides on
`v=1` alone — it wins, and stamps `{txId: A, COMMITTED}` over B's claim. D2 reads back
`COMMITTED` and answers the client "committed" for a transaction that applied nothing.
The `txId` clause on D1's write stops this at the source.

> **R-C. A failed compare-and-set is a read, not an error.** The loser adopts the row it
> gets back (6.4) and branches on three cases, in this order:
>
> 1. `row.txId ≠ mine` → the ledger key was reused. Abandon. Never apply this decision,
>    never write. Cancel own locks.
> 2. `row.txId = mine` and the state is the one this driver intended → a lost
>    acknowledgement. The driver's own write landed. Proceed as the winner.
> 3. `row.txId = mine` and the state is the other one → the driver lost the race. Follow
>    the row.
>
> Case 2 is the ordinary shape of every tokenless decision retry: T2's condition is
> `item_not_exists`, so a retry of a write that landed always fails the condition. A
> driver that treats it as a loss reports a cancelled transaction that committed.

> **R-D. Only a driver releases (T6),** only after every participant confirmed, and only
> a tokenless row. A token row is the idempotency window itself. Releasing it on
> completion would shrink that window to zero: a client whose answer was lost on the
> network would replay the token, find nothing, and execute the transaction a second
> time. Only TTL may end a token row. This is R1 of section 10.

> **R-E. A decision may be sent to a participant only after it is durable.** Never
> before, and never by a driver that lost. Rule 1 in 9.3 gives the split-brain this
> prevents.

### 6.4 Two prerequisites in the conditional-write path

Both are general FokosDB features. Both must ship before the ledger.

**Conditions over an arbitrary attribute.** `ItemCondition[]` is already evaluated as a
conjunction (`evaluateConditionsOnItem`, `partition-store.ts`), but `attribute_equals`
accepts only `attribute: "v"` (`types.ts`). T3, T4 and T6 condition on `txId` and
`state`, so `attribute_equals` must accept any attribute.

**`putItem` returns the item that failed the condition.** Matches DynamoDB's
`ReturnValuesOnConditionCheckFailure`. Today a failed condition only throws. Every lost
compare-and-set in this design is followed by a read of the same row; this option
collapses the two into one round trip. The partition already holds the item at the point
of failure, so nothing extra is read. Carry the item on the structured error for now; if
the README todo unifies condition failures on returned values, move it there. The ledger
module reads it from whichever shape wins.

---

## 7. Protocol changes

### 7.1 `PrepareRequest.coordinatorDoId` becomes `ledgerKey`

The field stored in `pending_transactions.coordinator_do_id` is the whole thread from a
lock back to its outcome. It stops being a DO id and becomes the ledger key. The column
is renamed to `ledger_key` through a `SQLSchemaMigrations` step, and `listStalePendingTx`
returns it.

The `invariant` in `prepareLocal` stays: a lock that cannot name its outcome is
unreleasable, and that is still the worst possible outcome.

The lock row does **not** store a coordinator name. A partition computes the recovery
shard itself. See 9.2.

### 7.2 `PrepareRequest` carries `numTransactionCoordinators`

A partition needs the pool size to route a recovery poke, and the poke is sent from the
stale-TX alarm, which runs with no incoming request to read a value from. So the prepare
carries the pool size, and the participant stores it in every lock row, next to
`ledger_key`. `listStalePendingTx` returns both.

A stored value can go stale when the pool is resized. That is safe: any shard can resolve
any transaction (9.2), so a poke routed with an old pool size still lands on a
coordinator that works.

The value stays out of `PartitionContext`. There it would either enter
`areImmutableOptionsEqual` (`partition-context.ts`) and make every partition reject
requests the moment the pool is resized, or sit outside the equality check as a stored
copy that no code path refreshes. Per-lock storage has neither problem.

### 7.3 New RPC: `txCommitDecided`

```ts
txCommitDecided(pCtx, { transactionId, transactionTimestamp }): Promise<{ appliedCount: number }>
```

The keyless commit, for recovery only:

- **Guarded exactly like a write.** It calls `ensureMigration` and throws the retryable
  503 while the partition is migrating, and it throws the same way while a split is in
  progress (`split_started`). A splitting parent accepts no writes, and this is a write:
  a local apply inside either window would race the children's data copy and lose the
  committed item. The caller treats the throw as best effort; the locks resolve through
  the children's own alarms once migration hands them over.
- In the normal state: builds the item set from this partition's own
  `pending_transactions` rows for `transactionId`, exactly as the stale-TX alarm does
  today, and applies it through the public local commit path.
- Asserts the rows' `transaction_ts` equals `transactionTimestamp`.
- Does **not** fan out. Per I2, a descendant that holds forwarded locks resolves itself
  through its own alarm.
- No pending rows means a no-op `committed`, so the call is idempotent. That is also the
  answer after `split_completed`: the rows have migrated to the children.

`appliedCount` is diagnostic only. The driver logs a surprising value and never acts on
it: after the decision the commit must proceed regardless.

**Why it cannot forward to children the way `txCommit` does.** The two RPCs get their
keys from different places. `txCommit` routes with keys from the caller's request, so it
can reach the children even when this partition holds nothing. `txCommitDecided` can only
derive keys from this partition's own `pending_transactions` rows, and in every split
state that derivation buys nothing:

- **Normal**: there are no children. The local rows are the complete set.
- **`split_started`**: the rows are still here, but the children reject every write with
  a 503 while they migrate. Forwarding fails exactly like the local apply is forbidden.
  `txCommit` fails the same way in this window; the two RPCs do not differ here.
- **`split_completed`**: the rows are gone. `pending_transactions` is deleted atomically
  with the transition (`acknowledgeChildMigrationComplete`), after every child fetched
  its copy. There is nothing left to derive routing from. The children hold their own
  migrated lock rows — including `ledger_key` — and resolve through their own alarms.

A keyless broadcast to all `splitN` children at `split_completed` would work, but it is
recursive down the split tree, and it only shaves `STALE_TX_MS` off a path that needs a
driver crash plus a split between prepare and recovery. Not worth it.

Cancel needs no new RPC. `txCancel` with an empty key set already has exactly this
behaviour, and it is already documented.

### 7.4 `txCommit` keeps its keys and loses its payload

The driver path keeps `txCommit` unchanged in shape. It has the keys, so
`groupItemsByRouting` forwards precisely to the children that hold locks. There is no
broadcast during a split and no route table to maintain.

One fix, worth shipping first on its own: `runCommit` currently loads the full payload
with `loadItems` and ships it on every commit RPC, but `#applyCommitItems` reads
`pendingRow.data` and never touches `item.data`. A 4 MB transaction re-sends 4 MB for
nothing. `runCancel` already uses `loadItemKeys` and says why in its comment. Commit must
do the same.

| Caller | Has keys | RPC | Fan-out |
| --- | --- | --- | --- |
| Driver (Worker, or a replay) | yes | `txCommit` with keys | exact, by key |
| Recovery | no | `txCommitDecided` | none |

The key-set cross-check in `commitLocal` stays on the driver path, where it compares what
the driver believes against what the partition holds. The recovery path derives the set
from the partition's own state, so it needs no check.

### 7.5 The commit is awaited; `waitUntil` carries only cleanup

The driver decides, then awaits the commit fan-out, and only then answers — the same
order as today, where `drivePrepare` awaits `runCommit`. Answering at the decision point
would save one round trip, but it would break read-your-writes. See 17.

Work that belongs after the answer uses an execution extension:

```ts
import { waitUntil } from "cloudflare:workers";
```

It carries the tokenless ledger release (T6), the lock cleanup after a failed decision
(9.3b), and the continued commit retries when a participant stayed unreachable past the
in-request budget (9.3c). The imported `waitUntil` behaves exactly like `ctx.waitUntil`
and needs no `ExecutionContext`, so the library uses it directly with no change to the
public API. The project's compatibility date (2026-05-23) is well past its release.
Execution extends up to 30 seconds after the response — far more than cleanup needs. The
participants' own alarms are the backstop if it is cut short.

### 7.6 `destroy()` changes order and target

`destroy()` is an admin and debug flow. It assumes the database has no live traffic, and
this proposal keeps that assumption.

Today `destroy()` sweeps the coordinators first, mainly so that a replayed
`clientRequestToken` cannot be answered from a dead transaction's `tc_state`. That job
moves to the ledger:

1. **The ledger is destroyed first.** It holds the idempotency window now.
2. **The coordinator sweep is dropped.** A stateless coordinator has nothing to wipe, so
   `destroyCoordinator` and `DESTROY_ABORT_SENTINEL` go away.
3. **Partitions last**, unchanged.

What the order cannot promise: the driver is a Worker, and `destroy()` cannot stop a
Worker. Wiping the ledger does not stop a live driver from deciding either — T2 is
conditioned on `item_not_exists` and passes trivially against an emptied ledger. A driver
racing a destroy can re-create ledger rows and commit into partitions the traversal
already wiped. The no-live-traffic assumption is what makes destroy correct; the ordering
only keeps the common case tidy.

---

## 8. Lifecycle

Three state machines run at once. Only two are durable, and neither is the driver.

**The ledger row** (6.3) is the only place a decision exists. A tokenless transaction
never enters `PREPARING`: it goes from absent straight to a terminal state. A token
transaction always claims first, because the claim is the idempotency record.

**The participant lock** is the only place the payload exists:

```
   absent ──prepare accepted──> HELD ──commit applied──> absent
                                     ──cancel──────────> absent
```

`HELD` is the only state. A participant never records a decision, so it can never answer
"what happened to this transaction" on its own. That is why the ledger exists.

**The driver** is durable nowhere. A driver crash is written down nowhere. It is
inferred, always from the same pair of facts: a lock is `HELD` and its ledger row is not
terminal.

### 8.1 What is true at every instant

For one transaction, across all participants:

- **L1.** A lock is `HELD` ⟹ that participant accepted prepare at `transaction_ts`.
  There is no other way to create one.
- **L2.** The row is `COMMITTED` ⟹ every participant accepted. Only a driver that saw
  every accept may write `COMMITTED`, and per R-E it writes before it tells anyone.
- **L3.** A lock is `HELD` and the row is absent or `PREPARING` ⟹ undecided, and cancel
  is the correct outcome — reached only through T4 or T5, never by releasing the lock
  directly. Absence is not a decision: a delayed T2 can still win it. The one exception
  is an over-age lock (10.2), which means the row was reclaimed and must raise, not
  cancel.
- **L4.** No lock is `HELD` ⟹ nothing about the row can harm anyone. That is why T6 is
  gated on exactly that.

L3 is the load-bearing one. Absent and `PREPARING` are the same state to every reader.

### 8.2 Tokenless, step by step

```
 Worker                     P1            P2          Ledger
   |--prepare(txId, ts, items->P1, ledgerKey=txId)-->|            |   RTT 1
   |--prepare(txId, ts, items->P2, ledgerKey=txId)------->|       |
   |          ^^^^ the only arrow that carries payload            |
   |<--accepted-------------|             |                       |
   |<--accepted---------------------------|                       |
   |                                                              |
   |--T2: putItem(hk=txId, {COMMITTED, ts, [P1,P2]},                |
   |              cond: item_not_exists)------------------------->|   RTT 2
   |<--v=1--------------------------------------------------------|
   |                                                              |
   |--txCommit(txId, ts, KEYS)-->|        |                           RTT 3
   |--txCommit(txId, ts, KEYS)----------->|   (each applies from
   |<--ok-----------------------|<--ok----|    its own rows)
   |
   ===> answer the client "committed"     3 round trips, same as today
   |
   |  (in waitUntil, after the response)
   |--T6: deleteItem(hk=txId, cond: v=1 AND txId)---------------->|
```

| Step | Durable after it | The driver dies here → |
| --- | --- | --- |
| S0 | nothing | nothing existed, nothing was locked |
| S1 prepare, partly accepted | locks on the accepting participants | each lock alarms at `STALE_TX_MS`, pokes, reads absent, T5, cancels. Correct by L3 |
| S2 prepare, all accepted | locks everywhere | identical to S1: still absent, still cancelled |
| S3 T2 wins | **the decision** | see below |
| S4 commit fan-out | locks released as each returns | remaining locks alarm, poke, read `COMMITTED`, apply. Idempotent |
| S5 client answered | — | the rest is cleanup |
| S6 T6, in `waitUntil` | nothing | the row leaks until TTL. Harmless (10.1) |

Two facts make the table readable:

**The client is answered at S5 and nowhere else,** after the commit applied everywhere. A
client that reads a key right after a `committed` answer sees the value the transaction
wrote. A tokenless client that receives no answer knows nothing and can only retry, which
is a brand-new transaction. So cancelling at S1/S2 is the correct outcome:
cancel-then-retry applies the transaction once, commit-then-retry applies it twice.

**S3 is the point of no return, and it is one write.** The gap between "every participant
accepted" and "the decision is durable" is a single ledger round trip, bounded further by
the decision-retry budget of 9.3(a).

**The decision race: a slow T2 against a recovery force-cancel.** The tokenless ledger
row is written *after* prepare, so a lock can exist while the row is absent. Suppose the
driver's T2 is delayed past `STALE_TX_MS`. P1's alarm pokes recovery, recovery reads
absent — and must **not** cancel on that reading. Absence is not a decision (L3): the
delayed T2 can still land. Recovery must first win T5, the durable `CANCELLED` tombstone.
T2 and T5 both condition on `item_not_exists` for the same key, so exactly one wins:

- **T5 wins.** The delayed T2 fails its condition. The driver reads the row back (R-C
  case 3): `CANCELLED`, its own `txId` — it lost. It never sends `txCommit` and answers
  the client `cancelled`. Ledger, locks and answer all agree: cancelled.
- **T2 wins** (the write landed early; only the response was slow). Recovery's T5 fails
  its condition, reads back `COMMITTED` with the poked `txId` (R-C), and **commits**
  instead of cancelling. The driver's slow acknowledgement arrives as a win, it runs the
  commit fan-out (idempotent against recovery's applies), and answers `committed`.

There is no interleaving where the ledger says one thing and the participants did the
other, because nobody acts on absence and nobody acts before winning the compare-and-set.

This is a real semantics change from today, and porting the old code as-is would
reintroduce the bug. Today the coordinator writes its state row **before** any prepare,
so "no row" proves the transaction can never commit, and the alarm path safely treats
`not_found` as cancelled (`do-partition.ts`). In this design that proof is gone. The
`not_found → cancel locally` shortcut must be **removed**: a participant releases a lock
only on a terminal outcome that recovery returns after reading — or writing — the ledger
row. See 9.2.

Recovery from a death at S3 or S4:

```
 t+STALE_TX_MS   P1's stale-TX alarm fires
   P1 --recoverTransaction(txId, ledgerKey, ts)--> TransactionCoordinatorDO[shard]
                    TC --getItem(hk=ledgerKey)--> Ledger
                    TC <--{txId, COMMITTED, ts, [P1,P2]}--
   P1 <--{state: COMMITTED, transactionTs: ts}-- TC
   P1 applies its own rows locally, as it does today
                    TC --txCommitDecided(txId, ts)--> P2
                       (P1 is skipped; it is the caller)
```

Each arrow into a participant carries two scalars. Nothing carries keys or data.

The poke carries `transaction_ts`, which the participant has on every lock row. A
recovery driver forcing T5 has no other source for it, because no claim was written, and
the row's `transactionTs` must be set for the `txCommitDecided` assertion to work later.

A T5 row's `participants` list is **empty**: recovery knows only the participant that
poked it. So a tokenless force-cancel reaches no one else, and each other lock holder
resolves on its own alarm, within the same `STALE_TX_MS`. See 17 for why this is not
worth a pre-prepare claim.

If P2 has split since prepare, the name in the list points at the parent. While the split
is still migrating, `txCommitDecided` is rejected there like any other write (7.3). After
`split_completed` the parent's rows have migrated and the call is a no-op. Either way the
children resolve themselves through their own alarms, per I2, bounded by `STALE_TX_MS`.

The recovery driver never runs T6. See 10.

### 8.3 With `clientRequestToken`, step by step

The claim is written before prepare, and `ledgerKey` is the token.

```
   |--T1: putItem(hk=tok, {PREPARING, txId, ts, opsHash, [P1,P2]},
   |              cond: item_not_exists)---------------------->|   RTT 1
   |<--v=1-----------------------------------------------------|
   |--prepare(txId, ts, items, ledgerKey=tok)--> P1, P2             RTT 2
   |--T3: putItem(hk=tok, {COMMITTED, …},
   |              cond: v=1 AND txId=<mine> AND state=PREPARING)|   RTT 3
   |--txCommit(txId, ts, KEYS)-----------------> P1, P2             RTT 4
   ===> answer the client "committed"
```

Four round trips, one more than today. The claim is the extra one: the price of a
transaction that any driver can resolve without the caller's payload. There is no T6. A
token row is the idempotency window, and only TTL ends it (10).

| Step | Durable after it | The driver dies here → |
| --- | --- | --- |
| C0 | nothing | (a) |
| C1 T1 wins | the claim: identity, `ts`, `opsHash`, participants. **No locks yet** | a lockless `PREPARING` row. Nothing pokes it, because no lock can go stale. It holds nothing and blocks nothing. A replay resolves it; otherwise TTL reclaims it |
| C2 prepare, partly or fully accepted | claim + locks | (b) |
| C3 T3 wins | **the decision** | as 8.2 S3, but recovery runs T4 instead of T5, and the participant list is present, so one pass reaches everyone |
| C4 commit fan-out | locks released as each returns | as 8.2 S4 |
| C5 client answered | — | (c) |

The claim must exist before any lock does, or a replay could re-prepare against a
transaction it cannot name.

**(a) Crash before the claim lands.** Nothing exists and no locks were taken. The replay
claims and runs normally.

**(b) Crash during prepare — P1 locked, P2 not.**

```
 Ledger[tok] = {txId, ts, opsHash, PREPARING, [P1,P2], v=1}
 P1: lock(txId, ledgerKey=tok)          P2: nothing
```

Two resolvers exist. Both are safe, and I1 picks the winner.

*The client replays with the same token and the same operations.* T1 fails, so the driver
reads the row back (6.4). The state is `PREPARING` and `operationsHash` matches, so it
adopts `txId` and `transactionTs` and continues the **same** transaction. It re-prepares
P1, which is idempotent (`prepareLocal` skips an item whose lock already carries the same
`transaction_id`), and prepares P2 from the client's own payload. Then it runs T3 with
the adopted `txId`. The ledger supplies identity and the guard; the replaying client
supplies the data.

A stale claim is not re-driven. The adopted `transactionTs` is the original one, and
`prepareLocal` rejects with `timestamp_conflict` when any item moved on since, so an old
claim would fan out only to be cancelled. The threshold is `STALE_TX_MS`: past it, a
recovery driver may already be forcing `CANCELLED`, so the replay would race for an
outcome it is likely to lose. A claim older than that is force-cancelled and answered
`cancelled`, with no prepare sent. This is a deliberate downgrade from today, where the
coordinator alarm can re-drive such a transaction to commit from `tc_items`. A ledger
row holds no payload, so it cannot.

*No replay ever comes.* P1's alarm pokes a coordinator. `PREPARING` is undecided, so the
coordinator runs T4, then cancels P1, and P2 from the participant list (a no-op there). A
later replay reads `CANCELLED` and gets a truthful answer.

*Both at once.* One compare-and-set at `v=1` wins. If recovery wins, the replay reads
`CANCELLED`, answers cancelled, and its own fresh locks are released by it or by their
alarms. If the replay wins, recovery reads `COMMITTED` and commits instead.

**(c) Crash after the decision, before the client is answered.** This is the case the
token exists for.

```
 Client replays with tok
   T1 fails --> the row comes back --> {COMMITTED, opsHash matches, txId matches}
   ===> answer "committed". No prepare. The payload is never used.
```

One round trip with the option of 6.4, two without. Today it costs one, because the
coordinator reads its own storage.

**(d) Replay with the same token and different operations.** `operationsHash` does not
match. Throw, exactly as `initiateWrite` does today.

**(e) Replay after TTL reclaimed the row.** T1 succeeds and a new `txId` is created. The
idempotency window has expired. See 10, and note R-B: this case is why every token-path
write names its `txId`.

---

## 9. Recovery

### 9.1 Who resolves what

| Situation | Resolver |
| --- | --- |
| Happy path | the caller's Worker |
| Undecided, caller present | the replaying caller, per 8.3(b) |
| Undecided, no caller | a coordinator, forces `CANCELLED` |
| Decided | a coordinator, or each lock holder's own alarm |

A transaction is unfinished if and only if some participant holds a lock. Participants
already alarm on their own locks. So there is no global sweeper and no scan of the
ledger. The timer system DynamoDB needs is already distributed across the partitions.

One case falls outside the rule: a claim whose driver died before any prepare landed
(8.3, C1). It is a `PREPARING` row with zero locks. Nothing pokes it, it blocks nothing,
and a replay or TTL resolves it.

### 9.2 The coordinator pool

`TransactionCoordinatorDO` and its `StaticShardedDO` pool stay. They lose their storage,
not their job. A pool is kept instead of one object per recovered transaction because a
per-transaction object would pay a cold start on every recovery, and recovery is exactly
the path that is already late.

- **Shard key**: the ledger key. Any shard can resolve any transaction, so the choice is
  free. The ledger key gives affinity: repeated pokes for one transaction land on one
  warm shard and deduplicate the work.
- **Fallback**: on failure, retry with the shard key `ledgerKey + ":1"`, then
  `ledgerKey + ":2"`. This reuses `StaticShardedDO.one`. Fallback is safe only because
  of I1.
- **Resizing**: `numTransactionCoordinators` becomes freely adjustable. No shard owns
  state, the idempotency record lives in the ledger, and a poke that lands on a
  different shard after a resize resolves identically. Delete the caveat in `db.ts`.

`recoverTransaction(txId, ledgerKey, transactionTs)` keeps today's shape, plus the
timestamp (8.2 explains why). It:

1. Reads the ledger row.
2. **Verifies `row.txId === txId`.** A mismatch means the row belongs to a later
   transaction that reused a reclaimed token. Do not apply it, and do not attempt T4.
   See R-B.
3. Undecided or absent → force `CANCELLED` (T4 or T5), and only on a **won**
   compare-and-set treat the transaction as cancelled. A lost one is a read (R-C): adopt
   the row that comes back.
4. Returns the decision to the poking partition, which applies its own rows locally, as
   today.
5. Pushes `txCommitDecided` or `txCancel` (empty key set) to the other participants in
   the list, best effort.

`recoverTransaction` never returns `not_found`, and the alarm's `not_found → cancel
locally` handling is removed with it. Today "no row at the coordinator" proves the
transaction can never commit, because the coordinator writes its state before any
prepare. Here a tokenless row is written after prepare, so absence proves nothing — it is
resolved into a durable `CANCELLED` through T5 before anything is released. See the
decision race in 8.2. The only return values are `COMMITTED` and `CANCELLED`.

The read-transaction path (`initiateRead`) moves out of the DO into the Worker. It
persists nothing, so it needs no ledger, no recovery, and no state.

### 9.3 Fast recovery inside the driver

`PartitionDO.STALE_TX_MS` is 5,000 ms. A held lock makes a non-transactional write to
that key **throw**, not wait. So between a driver losing a dependency and the partition
alarm firing, there is a five-second window of user-visible errors on exactly those keys.

In most failure modes the driver still runs — only the ledger or one participant failed.
So the driver closes the window itself instead of waiting for the alarm. This is best
effort and adds no new state. Three phases, three budgets:

**(a) The decision, before the answer.** Retry the compare-and-set in the request path.
Keep this budget **below `STALE_TX_MS`**. Past that, a recovery driver may force
`CANCELLED` and win — safe, but it turns a transaction that would have committed into a
cancelled one.

**(b) The decision could not be written.** Answer the client with a retryable error, then
keep working in `waitUntil`: force `CANCELLED`, then release the locks. First retry at 50
to 100 ms, then exponential backoff with jitter, capped near 1 s, giving up around 15 s
to stay inside the 30 s `waitUntil` budget. This case matters most: without it, locks
stay held for the full `STALE_TX_MS`.

**(c) The decision is durable, a participant commit failed.** The commit is awaited
(7.5), so first retry `txCommit` inside the request, within a short budget. If a
participant stays unreachable past it, answer `committed` anyway — the decision cannot
change — and keep retrying in `waitUntil`. This is the one case where the answer can
precede full application, exactly as today when `runCommit` exhausts its retries. The
participants' alarms remain the backstop.

`tryWhile` already provides `baseDelayMs` and `maxDelayMs`, so this is a retry budget,
not new machinery.

Three rules:

1. **Never release a lock before the decision is durable.** The failure: D1 decides to
   cancel and releases P1's lock without writing the decision. A replay D2 finds
   `PREPARING`, re-prepares P1, and wins `COMMITTED`. D1 then releases P2's lock, still
   believing it cancelled. P1 commits and P2 does not. I1 prevents this: D1's cancel must
   win the compare-and-set first, and it would learn it lost.
2. **Back off. Do not poll.** An unhealthy Durable Object polled every 50 ms by every
   failed transaction is a thundering herd on the thing that already struggles.
3. **This is a latency optimization, never a correctness mechanism.** `waitUntil` does
   not survive isolate death. The partition alarm stays the backstop.

---

## 10. Retention and reclamation

Reclaiming a decision row is the one genuinely dangerous operation in this design.

**The hazard.** If a `COMMITTED` row disappears while a lock for it still exists, the
holder's recovery reads "absent", wins T5 against a row that no longer guards its key,
and rolls back a transaction that the other participants committed. Atomicity is gone.
T5 is correct against a row that never existed; it is fatal against a row that was
reclaimed.

Only reclamation creates the hazard. One rule keeps it away entirely:

> **R1. Only a driver that holds the full key set may delete a row, only after every
> participant has confirmed, and only a tokenless row (keyed by `txId`).** A token row is
> never deleted by a driver: it is the idempotency window, and only TTL may end it (see
> R-D). A recovery driver never deletes anything, because it cannot know whether a
> participant it reached had forwarded locks to children.

Under R1, a row is deleted only when no lock for it can exist (L4). So "absent" always
means "never decided", and cancelling is always correct. **The design is safe with no
reclamation at all**, which is why it can ship before TTL exists.

### 10.1 Shipping without TTL

The cost of omitting TTL is growth, not incorrectness:

- **Every token transaction leaves one row, permanently.** R1 forbids deleting token
  rows, so the ledger grows with token-transaction volume. Rows are a few hundred bytes
  each, and `tc_state` today is never garbage collected either, so this is parity with
  the current design, not a regression.
- Tokenless rows whose driver died before T6 are never reclaimed. Rare.
- Lockless `PREPARING` rows (9.1) are never reclaimed. Rare.

Mark this with a `FIXME` in the ledger module. The whole point of the ledger is that it
can outgrow a Durable Object, so it must not stay unbounded forever: TTL is a requirement
for any deployment that uses tokens heavily.

### 10.2 What TTL will require

TTL deletes rows on a timer, not on confirmation, so it breaks R1 and creates the hazard
above. Two rules make it safe, and both must land with TTL:

1. **"Absent" means cancelled only for a young lock.** The alarm compares the lock's
   `created_at` against the retention window. An over-age lock raises an operational
   error instead of cancelling. Add this check now regardless: it converts a silent
   atomicity violation into a loud one, and it costs one comparison.
2. **A decision applies to a lock only when `row.txId` matches the lock's
   `transaction_id`.** This protects against a reclaimed row whose ledger key was
   reused.

**Sizing: many minutes, not seconds.** A healthy transaction lives well under a second,
and `STALE_TX_MS` is 5,000 ms. Retention must sit orders of magnitude above that — tens
of minutes — for two reasons:

1. It is the deadline the over-age check compares against. With that much headroom, an
   over-age lock is a genuine pathology, not a slow transaction losing a race.
2. It is also the idempotency window (8.3e). DynamoDB documents ten minutes for the same
   reason.

Both pull the same way, so one configured number serves both. The over-age check must
read its threshold from that same value. Two constants would drift: drift one way fires
false alarms, drift the other way restores the silent violation.

Only `COMMITTED` needs the full window on the tokenless side: losing a `CANCELLED` row is
harmless, because a lock holder that reads "absent" cancels, which is the recorded
outcome. Token rows keep the full window in every state, because the window is the
token's contract.

---

## 11. Benefits and drawbacks

Client-visible round trips for a multi-partition write:

| Path | Today | Proposed |
| --- | --- | --- |
| No token | 3 | 3 |
| With `clientRequestToken` | 3 | 4 |
| Replay of a decided transaction | 1 | 1 (2 without the option of 6.4) |

Benefits:

- **Coordinator storage** goes from "up to 4 MB per transaction, kept forever, 10 GB cap
  per shard" to zero. The ledger holds a few hundred bytes per transaction, transient on
  the tokenless path.
- **Blast radius** goes from "every transaction whose token hashes to a down shard" to
  "one transaction, if its own ledger row is unreachable".
- **The pool is resizable**, and any coordinator can recover any transaction.
- **Bytes on the wire drop sharply**: commit stops carrying the payload (7.4).
- **Lock release after a driver dependency fails** drops from `STALE_TX_MS` (5,000 ms)
  to about 100 ms, best effort (9.3). A held lock makes non-transactional writes throw,
  so this window is user-visible errors, not just latency.
- **Read transactions** leave the pool entirely (9.2).

Drawbacks:

- **A token write costs one more round trip** (the claim). The single-partition fast
  path is unaffected and stays at one.
- **A stale token claim is cancelled, not re-driven** (8.3b). Today the coordinator can
  re-drive it to commit from its stored payload.
- **The ledger grows until TTL ships** (10.1), at a few hundred bytes per token
  transaction.
- **Two prerequisite features** must land first: arbitrary-attribute conditions and
  return-item-on-condition-failure (6.4).

Unchanged:

- Consistency. The answer still comes after the commit applies (7.5).
- Per-transaction Durable Object requests, in order of magnitude: one ledger request
  replaces one coordinator request, and the `2N` participant requests stay.
- The 2PC semantics, conflict rules, and timestamp rules.

---

## 12. Sizing the ledger instance

Splits are triggered by size only (`hashSplitConditions.maxSizeMb`, default 100 MB).
Tokenless rows are deleted quickly. Token rows grow until TTL ships (10.1), so a heavy
token workload can eventually split a ledger partition — tolerable, because writes to a
splitting partition 503 briefly and the drivers retry. The harder limit is throughput: it
stays pinned at `rootTreesN` partitions times the per-object soft limit of about 1,000
requests per second, for the life of the table, because `rootTreesN` cannot change after
initialization.

The rule for now: `ledger.rootTreesN = main.rootTreesN * 3`, capped at 65,000. The
multiplier is a placeholder. It reflects two facts: a transaction touches several main
partitions but exactly one ledger row, and ledger rows are tiny, so the instance is bound
by request rate, not bytes.

**TODO**: replace the multiplier with a real heuristic, or add an opt-in RPS-based split
policy so rate-bound tables split on the dimension that constrains them. Until then,
over-provision `rootTreesN`: an idle hash partition costs almost nothing, and the value
cannot be raised later.

---

## 13. Deferred

Neither item blocks this proposal.

**Timestamp tie-breaking.** `transactionTs` moves from the coordinator's clock to the
caller's. Both are already spread across many locations, so this is not a regression,
and the ordering rules do not change. The tie breaker can no longer be the coordinator
identity, because one driver can stamp a transaction and another can resume it; the
transaction id is stable across both. Tracked in the README todo list.

**Placement.** The ledger and the coordinators get no location hints for now. Both are
placed where they are first touched, which is already no worse than today. Revisit once
the design runs.

---

## 14. Changes by file

| File | Change |
| --- | --- |
| `lib/transaction-ledger.ts` | new. `claim`, `decide`, `read`, `release` over a FokosDB instance |
| `lib/transaction-driver.ts` | new. The driver state machine, extracted from the DO, with no storage of its own |
| `lib/do-transaction-coordinator.ts` | shrinks to `recoverTransaction`; SQLite tables, migrations, alarm and the read path removed |
| `lib/db.ts` | `transactWriteItems` and `transactGetItems` run the driver directly; ledger instance construction (6.1); `destroy()` order (7.6); `numTransactionCoordinators` caveat removed |
| `lib/do-partition.ts` | new `txCommitDecided`, guarded like a write (7.3); stale-TX alarm pokes the pool by shard with fallback; the `not_found → cancel` shortcut removed (9.2); lock-age guard (10.2) |
| `lib/partition/partition-store.ts` | `coordinator_do_id` → `ledger_key`, plus a `num_transaction_coordinators` column on the lock row, with a schema migration; both returned by `listStalePendingTx` |
| `lib/partition/transaction-participant.ts` | `commitDecidedLocal`; `PrepareRequest` field rename |
| `lib/partition-topology/partition-context.ts` | reject `tableName` values starting with `fokos_tx.` (6.1) |
| `lib/transaction-types.ts` | `ledgerKey` and `numTransactionCoordinators` on `PrepareRequest`; `CommitDecidedRequest`; ledger row types; `RecoverTransactionResult` shrinks to the two terminal states (9.2) |
| `lib/transaction-limits.ts` | doc comment: validation is the client's boundary only; the coordinator no longer validates |
| `lib/partition/partition-store.ts`, `lib/do-partition.ts`, `lib/types.ts` | the two prerequisites of 6.4: return the failing item, and `attribute_equals` over an arbitrary attribute |

`stringifyReason` and `parseReason` (`do-transaction-coordinator.ts`) move to the ledger
module with the rest of the row encoding. They exist because plain JSON mangles a
`Uint8Array` key. Miss them, and a cancelled transaction over binary keys reports the
wrong key after recovery.

---

## 15. Implementation order

Each step is shippable on its own.

1. `runCommit` sends keys, not the payload (7.4). Pure win, no design change.
2. Add the lock-age guard of 10.2. It fixes a live correctness hazard.
3. Move `initiateRead` into the Worker. Isolated, and it removes all read-transaction
   load from the pool.
4. Add the two conditional-write prerequisites (6.4). Independent and useful on their
   own. The `txId` clauses of 6.3 are not expressible without the second one.
5. Extract the driver out of `TransactionCoordinatorDO`, behind a ledger interface, and
   run it with the **current** coordinator storage as the ledger. No behaviour change,
   fully testable.
6. Swap in the FokosDB-backed ledger. Rename `coordinator_do_id`. Carry
   `numTransactionCoordinators` on the prepare and store it per lock row (7.2). Add the
   `fokos_tx.` table-name validation (6.1).
7. Move the happy path into `db.ts`. The pool keeps `recoverTransaction` only.
8. Update `destroy()` to wipe the ledger first (7.6).

Steps 5 to 7 carry the breaking changes. Step 5 is what makes 6 and 7 safe.

TTL is **not** in this list. Per 10.1 the design ships correct without it, and the rules
it later requires are in 10.2.

---

## 16. Testing

Tests run in the real Workers runtime, so the races below are reachable. Each needs a
deterministic hook, not a sleep.

**Compare-and-set races** — I1 needs direct coverage, not coverage through the happy
path:

- Two drivers decide opposite outcomes for one transaction. Exactly one record survives,
  and the loser adopts it.
- A replay and a recovery driver race on one `PREPARING` claim, in both orders (8.3b).
- A driver that lost the race must not send commit or cancel (9.3 rule 1). This failure
  is silent if it regresses.
- A driver retries a decision whose acknowledgement was lost. It must read its own row
  back and report the outcome it wrote, not a loss (R-C case 2).
- A ledger key reclaimed and reused: the stalled first driver's decide must fail on the
  `txId` clause, not win on the version (R-B).
- The tokenless decision race (8.2): a T2 delayed past `STALE_TX_MS` races recovery's T5,
  in both orders. If T5 wins, the driver answers `cancelled` and no participant applies.
  If T2 wins, recovery commits instead of cancelling. In neither order does a participant
  release a lock on reading "absent".

**Consistency**:

- Read-your-writes: a `committed` answer returns only after every reachable participant
  applied (7.5). A `getItem` and a non-transactional `putItem` on a written key, issued
  immediately after the answer, both see the new state.

**Recovery**:

- `txCommitDecided` is idempotent, and a no-op when the partition holds no rows.
- `txCommitDecided` throws the retryable 503 while the partition is migrating or
  splitting, exactly as a write does (7.3), and never applies rows locally in either
  window.
- A participant that split between prepare and recovery: the parent rejects while
  migrating, no-ops after `split_completed`, and the children resolve through their own
  alarms.
- A poke that lands on a fallback shard resolves identically to the first choice.
- The pool is resized between prepare and recovery, and the transaction still resolves
  from the pool size stored on the lock row (7.2).

**Retention** (R1): a tokenless row is deleted only after every participant confirmed; a
token row is never deleted by a driver and still answers a replay after the transaction
completed; a recovery driver never deletes. Two more tests belong with TTL (10.2), not
before:

- A decision row removed while a lock still exists raises the over-age error, never a
  silent cancel.
- A `ledgerKey` reused after reclamation never has its decision applied to an old lock —
  the `row.txId` check.

**Idempotency**: replay before the decision, after it, with different operations, and
after the retention window.

**Migration parity**: step 5 of the implementation order runs the extracted driver
against the current coordinator storage. The existing `test/transactions.test.ts` suite
must pass unchanged at that step. That checkpoint is what makes steps 6 and 7 safe.

---

## 17. Rejected alternatives

**Keep the coordinator pool and move only its storage.** It pays the ledger hop and gets
nothing back: five round trips instead of four on the token path. It also loses the cheap
local index that `alarm()` uses to find unfinished work, so recovery becomes
participant-driven anyway — which is most of what the pool was for. Every invariant in
section 4 is still required.

**One coordinator Durable Object per transaction.** Perfect isolation, no shard key. But
every recovery pays a cold start, and recovery is exactly the path that is already late.
A warm shared pool is better, and I1 makes sharing safe.

**Keyless commit everywhere.** Attractive on the wire, but keys *are* the routing
information: `groupItemsByRouting` derives the owning child from the key itself. Removing
them forces either a broadcast to every child during a split, or a per-transaction route
table maintained through migration. Carrying the keys is cheaper than both. Hence the
split in 7.4.

**A participant holds the decision.** Designate the partition that owns the first item as
the decision holder, and fold the decision write into a partition the driver already
talks to. It removes the second FokosDB instance entirely. Rejected: it puts
per-transaction storage and decision-serving load back on data partitions, which is the
thing this proposal removes; outcome availability becomes coupled to a data partition;
and that partition can split, so decision rows would travel through the migration flow,
during which writes are rejected.

**Claim before prepare on the tokenless path too.** A `PREPARING` row written in parallel
with the prepare fan-out would let one recovery pass reach every participant. Rejected:
absent and `PREPARING` are the same state to a recovery driver (L3), so the row changes
no outcome. It would cost a second write per transaction on the one instance that is
rate-bound (12); it would serialize the two ledger writes, because T3
cannot name a version it has not seen, turning three round trips into four; and it would
make the lockless `PREPARING` row reachable for every tokenless transaction. What it buys
— one recovery pass instead of per-lock alarms — is bounded by the same `STALE_TX_MS`
either way.

**Answer before the commit lands.** The decision is the point of no return, so the driver
could answer `committed` as soon as the ledger write is durable and run the commit
fan-out in `waitUntil`, saving one round trip on every path. Rejected because it changes
the consistency a client observes: until the fan-out applies, a `getItem` on a key the
transaction wrote returns the old value, a non-transactional write to it throws
`pending_conflict`, and a follow-up transaction on it is rejected, not delayed — usually
for milliseconds, but for seconds when a participant is mid-migration. A client that is
told "committed" must be able to read what it wrote. Read-your-writes is worth one round
trip.

**Cooperative termination, with no ledger at all.** A participant with no record cannot
distinguish "never prepared" from "committed and cleaned up". That is the classic
blocking case in 2PC. Fixing it requires a per-transaction outcome record at each
participant, which is the storage cost this proposal removes.
