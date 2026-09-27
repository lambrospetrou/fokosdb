# RFC — A promotion cuts over with the locks of its key

**State:** Draft
**Date:** 2026-09-26
**Implementation:** Not implemented. Cancellation compatibility, recovery queries, and deployment choices remain open.

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

### 1.1 The current wait

A promotion moves one hash key, including all its sort keys, into a range root. A split moves all locally owned
keys into children. Both use the same repartition flow and migration streams.

A promotion currently waits until its hash key has no transaction locks. `PartitionDO.hooks().beforeCutover`
sets this rule:

```ts
beforeCutover: (plan) =>
  plan.kind !== "key_promotion" || this.#store.pendingLockCountForHashKey(promotedKeyOf(plan)) === 0,
```

The runtime checks the hook before target initialization and inside the cutover transaction. The second check
covers a prepare that arrives during the initialization RPC. A refused cutover leaves the plan before cutover.
The source polls every five seconds. A commit or cancel can wake the promotion earlier.

This rule blocks promotion, not new transactions. The source continues to serve the key before cutover.
Transactions on different sort keys can keep its lock count above zero. A quarantined lock waits for an operator
and can hold promotion indefinitely.

An unfinished promotion also blocks the source's hash split. If the source exceeds its size cap, other keys can
then receive `partition_over_size` too.

A range root does not always exist during this wait. The first hook check can prevent initialization entirely.
An empty range root can exist when a lock arrives during or after initialization, before cutover.

A Bloom false positive can name that empty root. The sharding client RFC records this case as problem 7.
The current runtime resolves that path again without the Bloom filter. That fallback fixes the false route, not
the promotion wait.

### 1.2 Why the current guard protects transaction state

A pending row contains both the lock and the prepared operation payload. It is not only a mutual-exclusion flag.
The coordinator removes its payload at `PREPARED`. Its later commit request carries keys, not the write payload.
A lost pending row can therefore lose a decided write.

The earlier promotion design avoided this case by requiring zero locks at cutover. The runtime RFC retains that
invariant. The current migration host can copy pending rows for every repartition kind, including a promotion.
Its `belongsToTarget` predicate selects the promoted hash key.

The copy mechanism alone is insufficient. A promotion source still serves other keys, unlike a split source.
Its transaction code must distinguish owned pending rows from rows retained for transfer.

Three host paths must distinguish owned pending rows from transfer copies:

1. `recoverStaleTransactions` can delete a `not_found` transaction when all its keys route away. The target need
   not have copied those rows yet. An over-age unresolved payload can disappear instead of entering quarantine.
2. `TransactionParticipant.commitLocal` compares the local request with every stored pending row for the
   transaction. A request for retained keys then conflicts with the copies of promoted keys.
3. `debugForceResolveTransaction` currently discovers every locally stored pending row for the transaction.
   It must exclude transfer copies. The operator addresses each current owner for emergency repair.

After source cleanup, the old source has no moved keys to discover for emergency repair. Transparent repair
through that old source is not a requirement of this change.

## 2. Goals and requirements

### 2.1 In scope

- A promotion must move a key with pending or quarantined transaction locks.
- Local transaction validation and mutation must use locally owned pending rows.
- Source recovery must preserve pending rows required by an unfinished transfer.
- Promotion and split cancellation must preserve source transfer copies until acknowledgement permits cleanup.
- Emergency repair must remain available on each complete current owner, with the existing request type.
- The existing migration streams, import gate, routed operations, and coordinator recovery must remain in use.
- Tests must cover transactions that span promoted keys and keys retained by the source.

### 2.2 Out of scope

- Removal of the import pause for writes or transaction operations.
- A pre-copy protocol, a change log, or dual writes during migration.
- Changes to the coordinator state machine or its point of no return.
- Reclamation of the item copies retained by a completed split source.
- Removal of the mutual exclusion between a hash split and an unfinished promotion.
- Removal of the Bloom fallback for an uninitialized or `awaiting_data` range root.
- Automatic transaction-wide discovery of emergency-repair keys or current lock owners.
- Emergency repair through the original source after its keys move.
- New administrative APIs or records that prove a historical transaction outcome.

### 2.3 Constraints

- `PREPARED` remains the point of no return. A prepared transaction must commit.
- `items` must contain committed state only. `pending_transactions` must contain unresolved operations.
- A local ownership check and its mutation must run without an intervening `await`.
- A transaction operation must not resolve an incomplete target's pending rows.
- A source must retain transfer copies until the required target acknowledgement is durable.
- Acknowledgement must prove complete import, not transaction completion.
- Coordinator-driven retries must preserve the transaction ID, key scope, and decided outcome.
- A commit or cancel must attempt every destination and report a failed destination.
- Commit and cancel must remain exempt from size rejection.
- Hosts must use runtime ownership methods. They must not query the sharding tables directly.
- Tests must not require a new production hook.

## 3. Milestones

The delivery milestones and deployment sequence remain open in section 4.3.3.

The implementation has a required dependency order. Ownership-scoped participant operations and recovery must
precede removal of the promotion lock guard. Emergency repair must respect ownership and work on complete targets.
Section 4.2.12 defines the tests required before the guard is removed.

## 4. Proposed solution

### 4.1 High-level overview

A cutover transfers authority over a pending operation, together with authority over its key. The source keeps
a copy until migration has delivered the operation. The target refuses transaction operations until its import
is complete.

The design separates three responsibilities:

| Responsibility | Meaning |
| --- | --- |
| Authority | The current owner can resolve the pending operation, once its import is complete. |
| Retention | The source keeps the pending payload until acknowledgement permits cleanup. |
| Repair scope | The addressed owner obtains emergency-repair keys from its owned pending rows. |

After promotion, the source continues to resolve transactions for its retained keys. It excludes transfer copies
from local commit validation and stale recovery. After a split, the source owns no application keys and resolves
no transactions locally.

```text
Source owns key and lock
        |
        | durable cutover
        v
Source retains transfer copy ---- import ----> Target holds complete key and lock
        |                                          |
        | no local transaction resolution          | normal operations and recovery
        |                                          | acknowledgement
        +--------------- cleanup <-----------------+
```

The target can resolve a transaction before the source receives its acknowledgement. A pending row left on the
source is then only a transfer copy. Its presence must not block another locally owned part of that transaction.

Normal recovery continues through the coordinator. Quarantine remains a safeguard when decision evidence is
missing. Emergency repair keeps the existing per-partition API. The operator calls each current lock owner,
which reads its owned pending rows. This change adds no transaction-wide discovery protocol.

### 4.2 Technical details

#### 4.2.1 Terms and state boundaries

An **owned pending row** belongs to a key for which `fokos.owns(key)` is true. An importing target owns its slice,
but the import gate still prevents transaction resolution.

A **transfer copy** is a source pending row whose key moved at cutover. Migration still needs that row until the
required acknowledgement. The row can remain after the target resolves its own copy.

Normal operations below include reads, writes, prepare, commit, cancel, and transactional reads. Normal lock
conflicts and admission checks still apply.

The source states remain:

```text
queued -> planned -> cutover -> completed -> cleaned
```

The target states remain:

```text
awaiting_data -> importing -> imported -> active
```

`imported` permits application operations. `active` records a successful acknowledgement response. A lost
response can leave the target `imported` after the source has already recorded completion.

The rules apply to one transfer. If a target later repartitions, routed operations follow the current owners again.
Emergency repair uses the current-owner contract in section 4.2.7.

#### 4.2.2 Promotion stages

Let `S` be the hash source, `K` the promoted hash key, and `R` its range root. Let `U` be the other keys that `S`
still owns. `K` includes every sort key under that hash key.

| Stage | Source for `K` | Source for `U` | Target `R` |
| --- | --- | --- | --- |
| 1. Queued/planned | Own and serve. | Own and serve. | Not initialized. |
| 2. Before cutover | Own and serve. | Own and serve. | Await data. |
| 3. Import incomplete | Forward; retain copies. | Own and serve. | Import; read through. |
| 4. Imported, no ack | Forward; retain copies. | Own and serve. | Serve; recover; ack. |
| 5. Ack recorded | Forward; queue cleanup. | Own and serve. | Serve and recover. |
| 6. Cleaned | Forward; copies gone. | Own and serve. | Serve and recover. |

**Stage 1 — Queued or planned, before initialization.** `S` owns both `K` and `U`. It takes and resolves locks
normally. `R` is not yet an initialized target.

**Stage 2 — Target initialized, before cutover.** `S` still serves both sets of keys. Existing or new locks do not
prevent the proposed cutover. `R` remains `awaiting_data` and serves no application operation. The source refuses
migration pulls before cutover. A direct ordinary read through `R` cannot bypass that restriction.

**Stage 3 — Cutover recorded, import incomplete.** `S` routes normal operations for `K` to `R`. It retains item
and pending-row copies for migration. It supplies migration pages and authorized ordinary read-through calls.
It must not commit, cancel, quarantine, or recover `K` locally.

`S` continues normal operations and stale recovery for `U`. Its local transaction checks exclude the transfer
copies of `K`.

`R` owns `K`, but holds incomplete state. It imports items, locks, and deletion metadata. It sends ordinary reads
to `S`. It rejects writes, transactional reads, prepare, commit, cancel, and forced resolution with
`partition_migrating`. It runs neither stale recovery nor the TTL sweep during import.

**Stage 4 — Import complete, acknowledgement not recorded at the source.** `R` serves normal operations from
its complete state. It can commit, cancel, or quarantine imported locks. It retries acknowledgement independently
of transaction completion. `S` retains its transfer copies and excludes them from local transaction decisions.

**Stage 5 — Acknowledgement recorded, promotion completed.** `S` schedules cleanup. It stops serving migration
pages and read-through calls for `K`. Requests for `K` still route to `R`. A lost acknowledgement response does
not prevent source cleanup or target service.

**Stage 6 — Source cleanup complete.** The bounded batches of section 4.2.8 have removed both sets of copies for
`K`. `S` removes the size estimate and keeps the route to the range tree. Cleanup leaves `U` and its locks
untouched. An unresolved lock can remain at `R` after its source copy is gone.

#### 4.2.3 A transaction across promoted and retained keys

Suppose transaction `T` locks one item under `K` and one under `U`.

```text
Before cutover:
    S owns: K, U

After cutover:
    S owns: U
    S retains for transfer: K
    R owns: K
```

Dispatch must divide commit or cancel by current ownership:

| Part | Required action |
| --- | --- |
| `U` | `S` validates and resolves its owned pending rows. |
| `K`, import incomplete | `S` forwards. `R` refuses temporarily. The driver retries. |
| `K`, import complete | `S` forwards. `R` resolves its owned pending rows. |
| Source copy of `K` | Transaction resolution preserves it. Cleanup removes it after acknowledgement. |

After local resolution of `U`, a retry can find only the transfer copy of `K` on `S`. This is an idempotent local
success, not `commit_keyset_mismatch`.

#### 4.2.4 Split stages

A hash split or range split moves every locally owned key into children. The source retains no locally owned
application keys after cutover. It becomes a router.

Already-promoted keys remain in their range trees. A hash child inherits the relevant route override, not the
promoted key's item or pending-row copies.

| Stage | Source `S` | Each child |
| --- | --- | --- |
| 1. Queued/planned | Own and serve its keys. | Not initialized. |
| 2. Before cutover | Serve; wait for all initializations. | Await data. |
| 3. Import incomplete | Forward; retain and export copies. | Import; read through. |
| 4. Imports differ | Forward; retain all pending copies. | Serve only after its import. |
| 5. Last ack recorded | Delete pending copies; remain router. | Serve and recover. |
| 6. Cleaned | Remain router; keep item copies. | Serve and recover. |

**Stage 1 — Queued or planned, before initialization.** `S` serves its keys and takes or resolves locks normally.

**Stage 2 — Children initialized, before cutover.** `S` continues normal operations. Cutover waits for every
child to initialize, but not for locks to clear. Each child remains `awaiting_data` and serves no application
operation. A child cannot import before source cutover.

**Stage 3 — Cutover recorded, import incomplete.** `S` forwards application operations. It applies no local
application writes or transaction resolutions. It retains source rows for migration and ordinary read-through.
It runs no stale recovery or TTL sweep. Each incomplete child follows the import restrictions of section 4.2.2.

**Stage 4 — Some children have completed import.** An imported child can serve normal operations and recover
its locks. An incomplete sibling remains gated. `S` routes each key to its current child. It keeps all pending-row
copies until every child acknowledges. The design does not reclaim one child's pending copies early.

**Stage 5 — Last acknowledgement recorded, split completed.** `S` deletes all source pending-row copies inside
the completion transaction. Every child already holds complete state. This acknowledgement condition does not
require the children to resolve their transactions.

**Stage 6 — Source cleanup complete.** `S` remains a router. The existing split policy retains its old item rows.
Those rows are not authoritative application state. Each child retains any unresolved locks it owns.

#### 4.2.5 Pending-row operations after cutover

In this table, an imported target includes both `imported` and `active`. An importing target includes
`awaiting_data` after source cutover and `importing`.

| Action | Promotion source | Split source | Importing target | Imported target |
| --- | --- | --- | --- | --- |
| Take a lock | Owned keys only. | Forward. | Reject. | Normal prepare. |
| Apply commit | Owned rows only. | Forward. | Reject. | Resolve owned rows. |
| Apply cancel | Owned rows only. | Forward. | Reject. | Resolve owned rows. |
| Stale recovery | Owned rows only. | Do not run. | Do not run. | Run normally. |
| Old `not_found` | Guard owned rows. | No local decision. | No decision. | Guard owned rows. |
| Export locks | Promoted slice. | Each child slice. | Not applicable. | No further import. |
| Delete copies | Cleanup after ack. | After every ack. | Not permitted. | Rows are owned. |

A promotion source forwards the moved part of commit or cancel. Both source kinds preserve transfer copies
until the cleanup condition in sections 4.2.2 and 4.2.4 holds.

`TransactionParticipant.commitLocal` must use the owned pending set for all three decisions:

1. Determine whether no unresolved local operation remains.
2. Validate the local request's key set.
3. Select the pending rows to apply and delete.

The local request must match the owned pending set when that set is nonempty. The method must preserve the
existing mismatch error for a malformed request. If the owned pending set is empty, the method returns the
existing idempotent success without treating transfer copies as unresolved local work.

Validation, application, and deletion must remain atomic. Deletion must identify the transaction and its resolved
local keys. Removing the mismatch check while keeping `deletePendingTx(transactionId)` would lose transfer copies.

`txCancel.beforeForward` must stop deleting every stored pending row by transaction ID. A source must forward
moved keys without deleting their copies. Cancellation resolves only the owned rows within its local scope.
A pure split router performs no local cancellation.

An empty cancel must not erase transfer copies. Its current local-only compatibility contract needs a decision
in section 4.3.1. Coordinator-driven cancellation continues to carry the transaction's routing keys.

#### 4.2.6 Stale recovery and quarantine

`canSweepLocally()` remains false on a split router, an incomplete target, or a partition behind the destroy
fence. It remains true on a promotion source because that source retains other keys.

A promotion source must exclude transfer copies from stale-recovery selection and decisions. The filter applies
to individual pending rows, not to whole transactions. One transaction can have both owned and transferred keys.

After the coordinator RPC returns, recovery must read the remaining rows and check current ownership again.
It must not use ownership captured before the `await` to authorize a local mutation.

For locally owned rows:

- A `COMMITTED` result applies through routed `txCommit`.
- A `CANCELLED` result applies through routed `txCancel`.
- A `not_found` result within the idempotency window follows the existing routed cancellation rule.
- An over-age `not_found` result quarantines owned rows and logs that transition once.
- A failed coordinator call preserves the pending rows for retry.

For transfer copies, these results authorize no local mutation. In particular, `not_found` must not delete a row
because its key routes away. The migration cleanup path removes that copy after acknowledgement.

Guard updates and guard removal must use the same owned-row scope. A source must not change quarantine metadata
on a transfer copy after cutover. An existing `guarded_at` value must survive migration. If an unguarded row moves,
the complete target evaluates its age and coordinator result itself.

The stale scan and its deadline must exclude transfer copies and guarded rows. Transfer copies must not consume
the recovery selection indefinitely or repeatedly arm an already-due alarm. They must not starve younger owned
locks. The bounded query and deadline implementation remains open in section 4.3.2.

#### 4.2.7 Emergency repair on the current owner

Forced recovery remains an emergency repair path, not a normal transaction timeout. The coordinator stores its
record before prepare and retains nonterminal transactions. It records completion only after every participant
confirms resolution. Normal promotion, migration, or RPC failure must not require an operator to choose an outcome.

An owned lock older than `IDEMPOTENCY_WINDOW_MS`, together with `not_found`, triggers the existing quarantine rule.
The window is 10 minutes. Lock age alone does not trigger quarantine. A coordinator RPC failure retains the lock
for retry. An unreadable coordinator reference also preserves the lock and can require operator repair.

Quarantine protects against missing decision evidence. The operator diagnoses the problem and supplies the repair
outcome. The repair RPC does not diagnose the transaction or reconstruct its decision.

**Request and scope.** Keep `DebugForceResolveTransactionRequest` unchanged:

```ts
type DebugForceResolveTransactionRequest = {
  transactionId: TransactionId;
  outcome: "commit" | "cancel";
};
```

The method still takes a partition context and uses `shape: "local"` with `whileMigrating: "throw"`.
The operator must address each current owner that holds an unresolved part of the transaction.

The handler must select all locally owned pending rows for that transaction. It must exclude transfer copies,
even while those copies remain on a promotion source. The caller supplies no item keys and cannot select an
arbitrary subset of the owner's pending rows.

For a nonempty owned set, the handler derives the keys and the original transaction timestamp from those rows.
It applies the supplied outcome through routed `txCommit` or `txCancel`. Selection and entry into that routed
operation must have no intervening `await`. Guard removal must obey the owned-row rule of section 4.2.6.
The ordinary transaction paths continue to enforce ownership and the import gate.

**Empty sets and retries.** If no owned pending rows remain, the call returns the existing successful no-op result.
It applies no write and needs no original timestamp. A timestamp used to construct an empty internal request
must not affect stored item state. The response does not prove that the transaction existed or committed globally.

A repeated repair call keeps the transaction ID and chosen outcome. It reads the remaining owned rows again.
A lost response after local resolution therefore permits a successful no-op retry without applying the write twice.
If ownership moves before another call executes, the operator must address the current owners again.
Success on an old source does not prove that a moved lock was resolved.

| Stage | Emergency-repair behavior |
| --- | --- |
| Before cutover | Call the source. It reads and resolves its owned pending rows. |
| After cutover, import incomplete | The target refuses repair. The source can repair only retained keys. |
| After import | Call the target for moved keys. It reads its owned pending rows. |
| After source cleanup | Call the current owner. The old source does not discover moved keys. |

A split source owns no keys after cutover. An emergency call there must not resolve or delete its transfer copies.
The operator calls the complete children that hold the unresolved rows. Current-owner identification remains an
operator task; this RFC adds no automated inventory or transaction-wide recovery driver.

**Coordinator and diagnostic records.** The coordinator is not a permanent key directory:

| Coordinator point | Keys and result available internally |
| --- | --- |
| Before completion | `tc_items` retains keys. At `PREPARED`, it drops the write payload. |
| At completion | `completeTransaction` deletes `tc_items` and `tc_participants`. |
| After result expiry | The idempotency sweep deletes the terminal state and result records. |

`recoverTransactionForParticipant` reports or drives an outcome. It does not return a recovery key list.
The emergency tool gets keys from the addressed owner's pending rows, not from this coordinator call.
A quarantine log contains the keys observed by one participant. It remains diagnostic evidence, not proof of a
complete transaction scope or historical outcome.

#### 4.2.8 Migration, acknowledgement, and cleanup

The flow keeps its existing phase order: route overrides first, then the host phase. The host streams remain
`items` followed by `pending_tx`. The target must finish both streams before it accepts transaction operations.

The pending stream must preserve the prepared operation, payload, transaction timestamp, coordinator reference,
creation time, and quarantine marker. Deletion metadata continues to accompany that stream, including an empty
page when the slice has no pending rows.

Each page and its cursor must commit together. The last page and the `imported` state must commit together.
A stale page must not reinsert a pending row after import or transaction resolution.

Source transfer copies remain unchanged by transaction operations after cutover. This gives the pending stream
a stable set for that slice. Application requests for other keys on a promotion source can continue.

A target acknowledges only after its complete import is durable. The source records the required acknowledgements
before it deletes transfer copies. Acknowledgement failure changes cleanup progress, not the target's ability to
serve its complete state.

Promotion cleanup must bound both item deletion and pending-row deletion. Each step can delete one batch from
each table for the promoted hash key. Each batch must use the existing item-cleanup limit of 1,000 rows.
`deletePendingTxForHashKey` currently deletes every matching pending row. The promotion cleanup path must replace
that unrestricted deletion with a bounded batch.

If either table still contains source copies for that key, `cleanupSourceStep` must report incomplete work.
The repartition stays `completed` and retries through the existing cleanup schedule. An empty item set must not
end cleanup while pending-row copies remain. Only after both sets are empty can cleanup remove the size estimate
and allow the runtime to delete the plan and mark the repartition `cleaned`.

Each step must remain idempotent and resume after a restart from the remaining stored rows. Batches must leave
other hash keys untouched. They delete source copies only after acknowledgement, not the target's unresolved locks.

Split completion remains unchanged: it deletes all source pending rows after every child acknowledges.
The batching change above applies to promotion cleanup.

The retention rule concerns pending rows. The existing TTL sweep can reclaim logically expired, unlocked item
copies on a promotion source. It must not delete pending payloads or bypass a lock. An incomplete target and a
split router do not run that sweep.

No new durable ledger or migration stream is selected. The existing pending-row fields and repartition records
carry the transfer. Section 4.3.2 leaves the ownership-aware query implementation open.

#### 4.2.9 Transaction interleavings and retry behavior

| Order | Required result |
| --- | --- |
| Prepare before cutover | Its lock moves with the key. It does not hold promotion. |
| Prepare during target init | Cutover still proceeds. The resulting pending row joins the transfer. |
| Commit before cutover | The source commits the item and removes its pending row before migration can read them. |
| Cutover before commit | Commit routes to the target. It retries until import permits resolution. |
| Prepare after cutover | It reaches the target. An incomplete target refuses it without taking a lock there. |
| Cancel during import | Source copies remain. The target refuses until import finishes, then removes its owned locks. |
| Import before ack | The target can resolve transactions. Source copies do not block retained source keys. |

Owner resolution and local mutation run in the same synchronous block. A commit cannot resolve locally before
cutover and apply locally after cutover. Thus, separate item and pending streams do not duplicate a committed
operation. Source maintenance must also obey the retention rule.

A new prepare can accept on one destination and fail on another. The initial coordinator driver cancels after
its prepare attempts fail. Cancellation must reach every destination that could hold a lock, including one whose
prepare response was lost. The existing recovery path can continue an undecided prepare before its hold deadline.

One commit retry budget need not cover the whole import. If a fan-out cannot finish, the coordinator remains
`COMMITTING`. The caller receives `transaction_commit_pending`; durable recovery tries again. The coordinator
reaches `COMMITTED` only after every participant confirms.

Cancellation also remains nonterminal until every required destination confirms. A failed fan-out must not lose
the keys needed by its next attempt. Emergency repair remains a separate per-owner operation under section 4.2.7.

#### 4.2.10 Availability and cost

The change removes lock lifetime from promotion's cutover prerequisites. It does not give cutover a wall-time
bound. Target initialization, scheduler progress, and failed RPCs can still delay it.

Writes and transaction operations remain unavailable on an incomplete target. Ordinary reads use the source.
An imported child need not wait for a slower sibling. A promotion source continues to serve retained keys.

A quarantined lock no longer holds promotion or the source hash split behind it. It still blocks conflicting
operations at the target until forced resolution.

The wire protocol keeps its current migration calls and page budgets. Promotion now includes pending payloads
in the existing stream. Transfer copies remain on the source until acknowledgement, even when cancellation has
already reached another owner. This trades temporary storage retention for one consistent cleanup rule.

Local commit must distinguish owned rows from transfer copies. Stale scans and deadlines need the same distinction.
The implementation must preserve bounded background work and avoid a full pending-table scan on every request.
Section 4.3.2 records the query strategy decision and the required cost check.

#### 4.2.11 Compatibility and deployment

The old promotion guard must remain until participant operations, stale recovery, and repair obey the ownership rules.
An implementation must not enable lock transfer while a source can still delete or misinterpret transfer copies.

The change affects current internal contracts:

- `txCancel` no longer clears transfer copies at each forwarding hop.
- Emergency repair selects owned rows only. Its request type stays unchanged; the operator calls current owners.
- Promotion tests must no longer expect a lock to defer cutover.
- Tests for split-router cancellation must expect retention until all children acknowledge.

The coordinator states, public transaction outcomes, and normal commit routing remain unchanged. Generic
`beforeCutover` hooks remain available to other sharding hosts. FokosDB stops using its lock count as a promotion
cutover condition.

Section 4.3.3 leaves deployment compatibility and rollback open. A rollback cannot assume that a promotion
source has no transferred locks once the new behavior has cut over a locked key. Restoring the old guard alone
does not repair an already-started transfer.

#### 4.2.12 Verification

Use the existing Workers test infrastructure. No production test hook is required.

**Participant and routing tests**

- Commit a transaction across a promoted key and a retained key.
- Hold target acknowledgement after import. Both owned parts must resolve without source cleanup.
- Retry the source-local commit while only transfer copies remain. It must succeed without applying twice.
- Preserve `commit_keyset_mismatch` for an incorrect owned key set.
- Cancel through a promotion source and a split router. Transfer copies must remain until their cleanup condition.

**Recovery tests**

- Cut over an over-age, unguarded `not_found` lock before its pending page is copied. Preserve its payload.
- Copy a quarantined lock and retain `guarded_at`.
- Recover a transaction with both owned rows and transfer copies. Change only the owned rows.
- Recheck ownership after an awaited coordinator response.
- Exclude transfer copies and guarded rows from recovery deadlines. They must not starve owned stale locks.
- Keep rows when the coordinator RPC fails or its reference cannot be read.

**Migration and coordinator tests**

- Prepare before cutover and during target initialization. Verify that the lock reaches the target.
- Commit on either side of cutover. Verify exactly one application and the correct final item version.
- Cancel before, during, and after the pending stream. Verify no surviving owned lock after successful cancellation.
- Hold one split child in import while another serves requests. Keep source pending copies until every child acks.
- Lose initialization, page, or acknowledgement responses. Restart and resume from durable state.
- Replay a stale page after resolution. It must not recreate the lock.
- Exceed one commit retry budget during import. Preserve the commit decision and finish through recovery.
- Fail prepare after another group accepted. Cancel all groups that could hold locks.
- Preserve the Bloom fallback before cutover and the migration refusal after cutover.

**Promotion cleanup tests**

- Reclaim more than one batch of pending-row copies. Each step must respect the limit for both tables.
- Leave pending-row copies after the item set becomes empty. Keep the plan and `completed` state until both drain.
- Start cleanup with pending-row copies but no item copies. It must still drain every pending batch.
- Restart between batches and repeat cleanup. Preserve other keys and the target's unresolved locks.
- Remove the size estimate and mark `cleaned` only after both source copy sets are empty.

**Emergency-repair tests**

- Call the current range owner after promotion cleanup and each current child after split completion.
- Force commit and cancel for transferred quarantined rows with the existing request type.
- On a promotion source, resolve owned rows but preserve transfer copies of the same transaction.
- Use the stored transaction timestamp when applying a forced commit.
- Repeat repair after a lost response. An empty owned set must return a no-op without changing item state.
- Call an old source with transfer copies only. It must neither resolve the target nor mutate those copies.
- Refuse emergency repair while the addressed target imports.

The existing test "defers cutover to 'promoting' while the key has a pending transaction lock" must change.
It must prove transfer instead of deferral. Repartition tests that use locks to hold promotion also need new
control through the existing migration harness.

### 4.3 Open questions

#### 4.3.1 Empty cancellation compatibility

The current `CancelRequest` permits an empty key list as a local-only release by transaction ID. The new rule
must preserve transfer copies in that case.

TODO: Decide whether to retain owned-only local release for an empty list or reject that internal request form.
The decision must cover existing callers and tests. It must not reintroduce deletion of transfer copies.

#### 4.3.2 Ownership-aware scans and deadlines

The store currently selects stale transactions and their earliest deadline without an ownership filter.
A filter after the existing limit can repeatedly select transfer copies and miss eligible owned rows.

TODO: Select a bounded ownership-aware scan and deadline strategy. Define and measure its query cost before approval.
Define its restart behavior.
The host must keep the runtime's ownership boundary and must not query `fokos_` tables directly.

#### 4.3.3 Delivery, compatibility, and rollback

TODO: Define the delivery milestones, deployment sequence, and rollback procedure before approval.
Cover old source recovery, the changed cancellation contract, and forced-recovery callers during deployment.
A rollback must account for transfers that already moved locked keys.

## 5. Alternative options

### 5.1 Stop new prepares and drain existing locks

A durable drain state can refuse new lock acquisition while allowing existing transactions to finish. This
prevents fresh transactions from repeatedly delaying promotion. It starts the transaction-write outage before
cutover and still waits indefinitely for a quarantined lock. It does not meet the lock-transfer goal.

### 5.2 Delete the promotion guard without host changes

The pending stream can copy locks, but source recovery can delete them before the copy. Local commit also
misinterprets transfer copies. Removing the guard alone does not establish the required ownership and retention rules.

### 5.3 Allow writes at the source during import

Forwarding writes back to the source is insufficient. A later page can miss or overwrite their effects.
Pre-copy with a durable change log, or a separate transaction handoff protocol, needs a different design.

### 5.4 Delete source copies early on cancel

A durable cancellation driver can justify early deletion if it retains every destination and retries to completion.
This design instead preserves copies until acknowledgement. Commit and cancel then share one retention rule.

### 5.5 Add transaction-wide emergency recovery

An explicit key list and automatic owner discovery could permit emergency repair through an old source after cleanup.
That needs separate scope, completeness, and retry contracts. It is not required for correct lock transfer.
This RFC retains current-owner repair and leaves that operator-tool expansion outside its scope.

## 6. Frequently asked questions

**Why can a lock move before its transaction decides?**

The pending row carries the prepared operation. The new owner inherits that row and the obligation to resolve it.
The import gate prevents use of incomplete state.

**Does a target acknowledgement mean its transactions finished?**

No. It means the target durably imported its slice. An unresolved lock can remain there after source cleanup.

**Can a source delete only rows it owns?**

Transaction resolution changes owned rows. Migration cleanup deletes non-owned transfer copies after acknowledgement.
These are separate operations with separate authorization conditions.

**Why do promotion and split sources differ?**

A promotion source still owns its other keys. A split source owns none and acts only as a router for application
operations. Both retain transfer copies under the same principle.

**Must a commit retry budget cover the whole import?**

No. The coordinator keeps its durable decision and retries through recovery. It must not report `COMMITTED`
until every participant confirms.

**Does forced recovery get its keys from the coordinator?**

No. The operator calls the current lock owner. The tool derives its keys from that owner's pending rows.
The coordinator deletes key rows at transaction completion and is not a permanent directory.

**Does a slow transaction require forced recovery?**

No. Nonterminal coordinator records remain available for automatic recovery. Quarantine requires an over-age owned
lock and `not_found`, not age alone. Emergency repair remains a safeguard for missing evidence or an unusable reference.

**Can an old source repair moved locks after cleanup?**

No. It has no owned pending rows for those keys. A successful no-op there is not a transaction-wide outcome.
The operator must address the current owners.

**Does this remove every promotion delay?**

No. It removes the wait for transaction locks to clear. Initialization failures and import work can still delay
progress. The Bloom fallback and import gate remain necessary.

## 7. References

- `docs/agent-plans/2026-09-26-fokos-sharding-client.md` — problem 7 and section 4.2.10.
- `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md` — hooks, dispatch, ownership, and recovery.
- `docs/agent-plans/2026-09-17-unified-repartition-flow.md` — migration and acknowledgement state machines.
- `docs/agent-plans/2026-08-30-bounded-stateful-transaction-coordination.md` — quarantine and per-partition repair.
- `docs/ideas/fokos-sharding/2026-09-09-fokos-partition-runtime.md` — the earlier promotion lock invariant.
- `packages/fokosdb/src/server/do-partition.ts` — `operations`, `hooks`, and `recoverStaleTransactions`.
- `packages/fokosdb/src/server/do-transaction-coordinator.ts` — commit, recovery, and transaction record lifetime.
- `packages/fokosdb/src/shared/partition/transaction-participant.ts` — local commit and cancellation.
- `packages/fokosdb/src/shared/partition/partition-store.ts` — pending-row queries, guards, and deletion.
- `packages/fokosdb/src/shared/partition/fokos-migration-host.ts` — item and pending streams.
- `packages/fokosdb/src/shared/transaction-wire-types.ts` — cancellation and forced-recovery request types.
- `packages/fokosdb/src/sharding/runtime.ts` — operation dispatch and the import gate.
- `packages/fokosdb/src/sharding/repartition-flow.ts` — cutover, import, acknowledgement, and cleanup.
- `packages/fokosdb/test/partition-do/promotion.test.ts` — promotion deferral and the Bloom fallback.
- `packages/fokosdb/test/partition-do/tx-stale-recovery.test.ts` — quarantine and stale recovery.
- `packages/fokosdb/test/repartition/repartition-flow.test.ts` — migration, lock transfer, and acknowledgements.
