# RFC — A promotion cuts over with the locks of its key

**State:** Draft
**Date:** 2026-09-26
**Implementation:** Not implemented.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
  - [4.3 Decisions](#43-decisions)
  - [4.4 Open questions](#44-open-questions)
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
3. `debugForceResolveTransaction` currently discovers every locally stored pending row for the transaction
   and hands their keys to `dispatch`. The routing is safe, because `dispatch` resolves the owner of each key
   again. The local mutation is not: the routed `txCommit` and `txCancel` must change owned rows only. The
   handler must also report how many rows it resolved, because a call that finds no owned row answers today
   with the same success as a real repair.

After source cleanup, the old source has no moved keys to discover for emergency repair. Repair through that
old source after its cleanup is not a requirement of this change.

## 2. Goals and requirements

### 2.1 In scope

- A promotion must move a key with pending or quarantined transaction locks.
- A source must find its transfer keys in the repartition records the runtime already stores, through a SQL
  fragment the runtime exports. No new table, hook, or per-row mark.
- Local transaction validation and mutation must use locally owned pending rows.
- Source recovery must preserve pending rows required by an unfinished transfer.
- Promotion and split cancellation must preserve source transfer copies until acknowledgement permits cleanup.
- Emergency repair must remain available on each complete current owner, with the existing request type.
  Its response must say how many rows the partition resolved locally, how many keys it forwarded, and which
  partitions answered for the forwarded keys. Each call must write one log line with that result.
- A target that imports a quarantined lock must log the lock with its own `doName`.
- A `txCancel` request with an empty `items` list must stay legal and release no pending row.
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
- Emergency repair through the original source after its cleanup removed the transfer copies.
- New administrative APIs or records that prove a historical transaction outcome.
- A rollback of the code on partitions that already cut over a locked key. The change ships to a new
  deployment with no existing partitions.

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
- Hosts must use runtime ownership methods. They must not name a sharding table in their own SQL; a host
  statement reaches a sharding table only through a SQL fragment the runtime exports.
- Tests must not require a new production hook.

## 3. Milestones

Each milestone ends with `pnpm check` and `pnpm test` green. The promotion lock guard stays in place until
milestone 4. Until then no production source holds a transfer copy, and the tests of the earlier milestones
build that state by hand through the migration harness.

1. **Transfer keys and owned-row scope.** Add the `movedHashKeys` SQL fragment to the runtime (section 4.2.1).
   Scope `commitLocal`, the cancel release, quarantine, guard removal, the stale selection, and the recovery
   deadline to owned rows, as sections 4.2.5 and 4.2.6 define them. Keep every owned-row statement within the
   parameter limit of section 4.2.5. An empty cancel releases nothing. Tests:
   "Participant and routing tests" and "Recovery tests" of section 4.2.12.
2. **Migration and cleanup.** Bound the pending-row deletion of promotion cleanup and let the pending stream
   carry the rows of the promoted key. Log each quarantined lock that a target imports (section 4.2.6). Tests:
   "Migration and coordinator tests" and "Promotion cleanup tests" of section 4.2.12.
3. **Emergency repair.** Route every row of the transaction, mutate owned rows only, report the counts and the
   answering partitions of section 4.2.7, and log each call. Tests: "Emergency-repair tests" of section 4.2.12.
4. **Guard removal.** Remove the lock count from `beforeCutover`, remove the two `repartitionUnblocked`
   signals, change the tests that hold a promotion with a lock, and change the code comments that section
   4.2.11 lists. Run the transfer tests of milestones 1 and 2 again through a real cutover: a prepare lands
   during target initialization, the source cuts over, the target imports the lock, and the target commits it.

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
| Repair scope | The addressed partition routes every row it holds to its current owner and mutates only its owned rows. |

The runtime already records every promotion with its hash key and its state. A hash key whose promotion is in
`cutover` or `completed` is a transfer key, and the pending rows under it are transfer copies. A local path that
holds the rows of one transaction asks `owns()` for the rows it cannot place. A scan that holds no transaction
excludes the transfer keys with a SQL fragment the runtime exports. Nothing new is written at cutover.

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
missing. Emergency repair keeps the existing per-partition request. The operator calls a partition that holds
rows of the transaction. That partition routes every row to its current owner, mutates only the rows it owns,
and reports how many it resolved and how many keys it forwarded. This change adds no transaction-wide discovery
protocol.

### 4.2 Technical details

#### 4.2.1 Terms and state boundaries

An **owned pending row** belongs to a key for which `fokos.owns(key)` is true. An importing target owns its slice,
but the import gate still prevents transaction resolution.

A **transfer key** is a hash key whose promotion on this source is in `cutover` or `completed`. The runtime
already stores that fact: `fokos_repartitions` carries `kind`, `state`, and `hash_key` for every promotion, and
`idx_fokos_repartitions_due (state, next_attempt_at, seq)` finds the two states with one seek each. The key
stops being a transfer key when the runtime writes `cleaned`, which happens in the transaction of the last
cleanup step, after the host reported both copy sets empty. A split source has no transfer key: it owns no key
after cutover and resolves no transaction locally. The number of transfer keys is not bounded by one: a
`completed` promotion of a large key stays in that state for as many cleanup steps as its rows need, and forced
promotions can put many keys there at once.

A **transfer copy** is a source pending row whose hash key is a transfer key. Migration still needs that row
until the required acknowledgement. The row can remain after the target resolves its own copy. The two
definitions agree: a pending row for a transfer key can exist on the source only from before the cutover,
because every later operation on that key resolves to the range root.

Two tools read that fact, one for each kind of caller:

- **`fokos.owns(key)`**, for a path that holds the rows of one transaction. It reads the route override and
  the split row, never the Bloom filter. A row whose key is in the request the runtime handed to the local
  handler is owned by construction; only a row outside the request needs the call. A transaction has at most
  `MAX_ITEMS_PER_TX` rows, and a row outside the request exists only when a transfer copy or a malformed
  request exists, so the normal path makes no call.
- **`fokos.sql.movedHashKeys()`**, for a scan that holds no transaction. It returns the text of a `SELECT`
  with one column, `hash_key`, over the runtime's own tables:

  ```sql
  SELECT hash_key FROM fokos_repartitions WHERE kind = 'key_promotion' AND state IN ('cutover', 'completed')
  ```

  The host splices it into its own statement as `hk NOT IN (<fragment>)`. The fragment binds no parameter,
  so the host statement keeps one constant text and the statement cache holds it. SQLite runs the uncorrelated
  subquery once per statement into an ephemeral index and probes it once per scanned row, so the cost does
  not depend on the number of pending rows and grows only with the number of transfer keys, once per statement.
  The fragment is the contract: the runtime can change its tables and keeps the text in step, and the host
  never names a `fokos_` table. `PartitionStore` receives a callback that returns the fragment, either at
  construction or as an argument of each method that needs it. The store calls the callback only when it runs
  the statement. The constructor builds the store before the runtime, and the runtime needs the store for its
  hooks, so the store cannot read the fragment at construction. The callback returns the same text on every
  call, so the statement text stays constant.

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

**Stage 3 — Cutover recorded, import incomplete.** The promotion row is in `cutover`, so `K` is a transfer key.
`S` routes normal operations for `K` to `R`. It retains item and pending-row copies for migration. It supplies
migration pages and authorized ordinary read-through calls. It must not commit, cancel, quarantine, or recover
`K` locally.

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
`K`. `S` removes the size estimate, the runtime writes `cleaned` in the same transaction, and `K` stops being a
transfer key. `S` keeps the route to the range tree. Cleanup leaves `U` and its locks untouched. An unresolved
lock can remain at `R` after its source copy is gone.

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

The owned pending set is built from the rows the method already lists for the transaction. A row whose key is
in the local request is owned by construction, because the runtime resolved that key to this partition in the
same synchronous block. A row whose key is outside the request is owned only when `fokos.owns(key)` says so;
otherwise it is a transfer copy and leaves the set. The normal path has no row outside the request and makes no
`owns()` call. The local request must match the owned pending set when that set is nonempty. The method must
preserve the existing mismatch error for a malformed request, which is the one case where an owned row lies
outside the request. If the owned pending set is empty, the method returns the existing idempotent success
without treating transfer copies as unresolved local work.

Validation, application, and deletion must remain atomic. Deletion must identify the transaction and its resolved
local keys. Removing the mismatch check while keeping `deletePendingTx(transactionId)` would lose transfer copies.

The release of `txCancel` moves from `beforeForward` into its `local` handler. The runtime hands that handler
the owned part of the request, and the handler deletes the rows of the transaction whose keys are in that part.
No ownership check is needed: the keys are owned by construction. A source therefore forwards moved keys
without deleting their copies, and a pure split router, which has no owned part, performs no local
cancellation. `txCancel` then has no `beforeForward`, and the note in `do-partition.ts` about a release that
owner resolution could skip goes with it.

An empty `items` list stays legal. The runtime runs the `local` handler with the whole request, and its owned
part is empty, so the handler releases no row and answers `cancelled`. The handler must not release by
transaction id. On a promotion source, that release deletes transfer copies. On a split router, it deletes the
copies that the children still import. The emergency repair of section 4.2.7 sends an empty cancel when it finds
no row, and the empty cancel then changes nothing. The `CancelRequest` doc comment changes from "release
locally" to "release nothing".

**Statement shape.** One SQLite query on a Durable Object binds at most 100 parameters. A transaction can hold
`MAX_ITEMS_PER_TX` (100) keys on one partition, and each key binds two parameters, so one statement cannot bind
every owned key. The commit deletion, the cancel release, `guardPendingTx`, and `clearPendingTxGuard` must each
use one of two shapes:

1. One statement per key, by the primary key `(hk, sk, transaction_id)`, all inside one `transactionSync`.
2. One statement with `transaction_id = ?` and `hk NOT IN (<movedHashKeys fragment>)`.

Shape 2 selects exactly the owned rows on a partition that owns keys, because each of its pending rows is an
owned row or a transfer copy. A split router owns no key, and each of its rows is a copy for a child that the
fragment does not name. Each of the four statements therefore runs only when its path holds at least one owned
key. With no owned key, the path changes no row.

#### 4.2.6 Stale recovery and quarantine

`canSweepLocally()` remains false on a split router, an incomplete target, or a partition behind the destroy
fence. It remains true on a promotion source because that source retains other keys.

A promotion source must exclude transfer copies from stale-recovery selection and decisions. The selection uses
the `movedHashKeys` fragment of section 4.2.1, and the decisions use `owns()` on the rows of the selected
transaction, per distinct hash key. The filter applies to individual pending rows, not to whole transactions.
One transaction can have both owned and transferred keys.

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

Guard updates and guard removal must use the same owned-row scope: `guardPendingTx` and `clearPendingTxGuard`
take the owned rows of the transaction in a statement shape of section 4.2.5, not the transaction id alone. A
source must not change quarantine metadata on a transfer copy after cutover. An existing `guarded_at` value must
survive migration. If an unguarded row moves, the complete target evaluates its age and coordinator result
itself.

**Log of an imported quarantine.** The stale scan skips a guarded row, and the guard logs only when a lock
enters quarantine. A target therefore stays silent about a guarded row it imports, unless it logs the import.
When the migration host applies a pending page, it writes one line for each transaction whose rows in that page
carry `guarded_at`:

- the message `"fokos/partition: imported a quarantined lock"`;
- `transactionId`, `coordinatorDoName`, `idempotencyToken`, and the encoded keys of the rows of that transaction
  in the page;
- `lockCreatedAt`, `guardedAt`, and the `doName` and `partitionId` of the target.

The operator calls `debugForceResolveTransaction` on the logged `doName`. The rule applies to every import. A
split child therefore logs the quarantined locks it receives too, and the latest line names the current owner.

The host writes the line inside the page transaction. A page that rolls back and applies again writes the line
again. A stale page that the target drops writes nothing. The line must not fail the page, because a throw rolls
the page back on every retry and stops the import. The host therefore reads the coordinator reference without
validation, and it logs an unreadable reference as its raw text. `FokosMigrationHost` receives a callback that
returns the log fields and the `doName` and `partitionId` of the partition, as `TtlExpiry` receives `logParams`.

The stale scan and its deadline must exclude transfer copies and guarded rows. `listStalePendingTx` and
`earliestUnguardedPendingTxCreatedAt` add `AND hk NOT IN (<movedHashKeys fragment>)` to their `WHERE` clause,
always, with no flag: with no promotion in flight the subquery is one empty seek. Without the clause the
deadline of the oldest transfer copy is in the past for the whole import, the scheduler sets the alarm in the
past and wakes the fast path, and the pass repeats every 50 milliseconds with up to ten coordinator calls and
ten forwards each. With it the deadline lands on the oldest owned row or on nothing, the alarm moves into the
future, and the passes stop.

Both queries walk `pending_transactions_created_at` from its start and skip the rows the clause excludes. With
`n` transfer copies older than the oldest owned row, one query steps past `n` index entries and probes the
ephemeral index of the subquery once per entry. `n` is the number of locks that were in flight under the
promoted keys at their cutovers, not the number of transfer keys: most transfer keys have no pending row. The
cost does not depend on the payload bytes of the rows.

The scheduler reads the `deadline()` of every runnable job up to three times in one pass: to find the due jobs,
to find the earliest deadline, and again after the steps. It reads them on every pass of any job, not only on
a pass of the stale job. While the copies exist, the deadline query therefore pays its cost up to three times
per pass, and the stale selection pays it once per pass of the stale job. Section 4.2.12 requires a measurement
at ten thousand pending rows under the promoted key. Section 4.4.1 holds the open question of how to remove the
repeated reads.

#### 4.2.7 Emergency repair on the current owner

Forced recovery remains an emergency repair path, not a normal transaction timeout. The coordinator stores its
record before prepare and retains nonterminal transactions. It records completion only after every participant
confirms resolution. Normal promotion, migration, or RPC failure must not require an operator to choose an outcome.

An owned lock older than `IDEMPOTENCY_WINDOW_MS`, together with `not_found`, triggers the existing quarantine rule.
The window is 10 minutes. Lock age alone does not trigger quarantine. A coordinator RPC failure retains the lock
for retry. An unreadable coordinator reference also preserves the lock and can require operator repair.

Quarantine protects against missing decision evidence. The operator diagnoses the problem and supplies the repair
outcome. The repair RPC does not diagnose the transaction or reconstruct its decision.

**Request, response, and scope.** Keep `DebugForceResolveTransactionRequest` unchanged. The response gives two
counts and the partitions that answered for the forwarded keys:

```ts
type DebugForceResolveTransactionRequest = {
  transactionId: TransactionId;
  outcome: "commit" | "cancel";
};

type DebugForceResolveTransactionResponse = {
  outcome: "committed" | "cancelled";
  /** Pending rows of the transaction that this partition owned and resolved in this call. */
  resolvedLocally: number;
  /** Keys of the transaction that this partition sent to their current owners. Not the rows those owners resolved. */
  forwarded: number;
  /** The partitions that executed the forwarded keys. Call each one with the same request to read its own `resolvedLocally`. */
  forwardedTo: { partitionId: string; doName: string }[];
  /** True when the byte cap of the route evidence dropped a partition from `forwardedTo`. */
  forwardedToTruncated: boolean;
};
```

The method still takes a partition context and uses `shape: "local"` with `whileMigrating: "throw"`.

The handler selects every locally stored pending row of the transaction, owned rows and transfer copies alike,
and hands all their keys to the routed `txCommit` or `txCancel`, as the handler does today. `dispatch` resolves
the owner of each key again: the owned keys apply on this partition, and the keys of transfer copies reach
their current owner. The routed operations mutate owned rows only, as section 4.2.5 requires, so a transfer
copy is a routing key here and never a local write. The caller supplies no item keys and cannot select an
arbitrary subset of the rows.

The handler derives the original transaction timestamp from an owned row when one exists, else from any row of
the transaction. Selection and entry into the routed operation must have no intervening `await`. Guard removal
runs after a routed operation that returned, and obeys the owned-row rule of section 4.2.6. The ordinary
transaction paths continue to enforce ownership and the import gate: a forwarded key whose owner still imports
fails the call with `partition_fanout_failed` after the local part applied, and the operator repeats the call
later, which then finds no owned row and forwards again.

`resolvedLocally` is the number of rows the local part deleted. `forwarded` is the number of keys in the remote
groups. `forwardedTo` lists the partitions that executed those keys. The handler takes the list from the routing
of the routed call: every node with the role `executed`, except this partition. The route evidence has a byte
cap, and `forwardedToTruncated` is true when the cap dropped a node.

`forwarded` counts the keys sent, not the rows resolved. A current owner that holds no row of the transaction
answers with success and changes nothing. The operator calls each partition in `forwardedTo` with the same
request, and each one answers its own `resolvedLocally`. A call that finds no row answers zero counts and an
empty `forwardedTo`.

**Log.** Each call writes one line, so the operator has a record of each forced outcome:

- After the routed call returns, the message is `"fokos/partition: forced resolution applied"`. The line carries
  `transactionId`, `outcome`, `resolvedLocally`, `forwarded`, the `doName` of each partition in `forwardedTo`,
  `forwardedToTruncated`, and the `doName` and `partitionId` of this partition.
- When the routed call throws, the message is `"fokos/partition: forced resolution failed"`. The line carries
  `transactionId`, `outcome`, the error code, the `causeCode` of a `partition_fanout_failed`, and the `doName`
  and `partitionId` of this partition. The handler then throws the error again.

The local part can have applied before a failure. When `causeCode` is `partition_migrating`, an owner still
imports, and the operator repeats the call later.

**Empty sets and retries.** If the partition holds no row of the transaction, the call applies no write and
needs no original timestamp. A timestamp used to construct an empty internal request must not affect stored
item state. With the outcome `cancel`, the internal request has an empty `items` list, which section 4.2.5 keeps
legal and which releases no row. The response does not prove that the transaction existed or committed globally.

A repeated repair call keeps the transaction ID and chosen outcome. It reads the remaining rows again. A lost
response after local resolution therefore permits a retry that resolves nothing locally, forwards what remains,
and applies no write twice. Two zero counts tell the operator that this partition holds nothing of the
transaction and that the current owners must be addressed directly. The import log of section 4.2.6 names the
partition that received each quarantined lock.

| Stage | Emergency-repair behavior |
| --- | --- |
| Before cutover | Call the source. It resolves its owned rows and reports them in `resolvedLocally`. |
| After cutover, import incomplete | The source resolves its owned rows and forwards the moved keys; the target refuses them and the call fails. Repeat later. |
| After import | Call the source or the target. The source forwards the moved keys, counts them in `forwarded`, and names the target in `forwardedTo`. The target resolves its owned rows. |
| After source cleanup | Call the current owner that the import log names. The old source holds no row and answers two zero counts. |

A split source owns no key after cutover. An emergency call there forwards every row to the children, mutates
nothing locally, and reports `resolvedLocally: 0`. A child that still imports refuses, and the call fails until
the child finishes. After the split completes, the router holds no row and answers two zero counts; the operator
then calls the children that the import log names. This RFC adds no automated inventory or transaction-wide
recovery driver.

**Coordinator and diagnostic records.** The coordinator is not a permanent key directory:

| Coordinator point | Keys and result available internally |
| --- | --- |
| Before completion | `tc_items` retains keys. At `PREPARED`, it drops the write payload. |
| At completion | `completeTransaction` deletes `tc_items` and `tc_participants`. |
| After result expiry | The idempotency sweep deletes the terminal state and result records. |

`recoverTransactionForParticipant` reports or drives an outcome. It does not return a recovery key list.
The emergency tool gets keys from the pending rows of the addressed partition, not from this coordinator call.
A quarantine log contains the keys observed by one participant. It remains diagnostic evidence, not proof of a
complete transaction scope or historical outcome.

#### 4.2.8 Migration, acknowledgement, and cleanup

The cutover transaction does not change. The state it already writes is what makes the key a transfer key, so
a promotion of a key with ten thousand locked sort keys cuts over as fast as one with none.

The flow keeps its existing phase order: route overrides first, then the host phase. The host streams remain
`items` followed by `pending_tx`. The target must finish both streams before it accepts transaction operations.

The pending stream must preserve the prepared operation, payload, transaction timestamp, coordinator reference,
creation time, and quarantine marker. It carries no ownership mark: a target owns every row it receives, and its
own repartition records decide ownership from then on. Deletion metadata continues to accompany that stream,
including an empty page when the slice has no pending rows.

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
and allow the runtime to delete the plan and mark the repartition `cleaned`. The runtime writes `cleaned` in
the transaction of that last step, so the key is a transfer key for as long as any copy remains and not one
statement longer.

Each step must remain idempotent and resume after a restart from the remaining stored rows. Batches must leave
other hash keys untouched. They delete source copies only after acknowledgement, not the target's unresolved locks.

Split completion remains unchanged: it deletes all source pending rows after every child acknowledges.
The batching change above applies to promotion cleanup.

The retention rule concerns pending rows. The existing TTL sweep can reclaim logically expired, unlocked item
copies on a promotion source. It must not delete pending payloads or bypass a lock. An incomplete target and a
split router do not run that sweep.

No new durable record and no new migration stream. The existing pending-row fields carry the transfer, and
the existing repartition records say which rows are copies.

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

Local commit and cancel make no `owns()` call on the normal path: the keys of the request are owned by
construction, and `owns()` runs only for a row outside the request. Shape 1 of section 4.2.5 replaces one
deletion by transaction id with one primary-key deletion per owned key. Shape 2 keeps one statement. Quarantine
and repair call `owns()` once per distinct hash key of one transaction. The stale scan and the deadline build
the transfer-key set once per statement from the runtime's repartition index and probe it once per scanned row;
on a partition with no promotion in flight that is one empty seek. While the copies exist, the deadline pays one
extra index step per transfer copy older than the oldest owned row, up to three times per pass. The scan pays
the same step once per pass of the stale job (section 4.2.6).

After the target reaches `imported`, every lock it received is already older than the stale threshold, so its
recovery job drains them ten transactions per pass with one coordinator call each. A key with thousands of
pending rows at cutover therefore produces a burst of coordinator calls from the target. This is the same
drain a partition performs today after a restart with many stale locks, and it is the intended behaviour.

#### 4.2.11 Compatibility and deployment

The old promotion guard must remain until participant operations, stale recovery, and repair obey the ownership rules.
An implementation must not enable lock transfer while a source can still delete or misinterpret transfer copies.

The change affects current internal contracts:

- `txCancel` no longer clears pending rows at each forwarding hop. Its release runs in its `local` handler on
  owned rows only. An empty `items` list stays legal and releases no row.
- Emergency repair routes every row it holds and mutates owned rows only. Its request type stays unchanged;
  its response gains `resolvedLocally`, `forwarded`, `forwardedTo`, and `forwardedToTruncated`. Each call
  writes one log line.
- A target logs each quarantined lock that it imports (section 4.2.6).
- `txCommit` and `txCancel` no longer signal `repartitionUnblocked`. No promotion waits on a lock, so the
  signal has nothing to wake. The signal type stays in the runtime for other hosts.
- The runtime exports the `fokos.sql.movedHashKeys()` fragment of section 4.2.1, the first of its SQL
  fragments. `beforeCutover` stays for other hosts, and FokosDB no longer implements it.
- Promotion tests must no longer expect a lock to defer cutover.
- Tests for split-router cancellation must expect retention until all children acknowledge.

The coordinator states, public transaction outcomes, and normal commit routing remain unchanged.

The two earlier RFCs carry a note "Superseded by `docs/ideas/2026-09-26-promotion-moves-its-locks.md`" with the
new rule at each statement of the old rule: `docs/agent-plans/2026-09-17-unified-repartition-flow.md` in
sections 4.3, 4.4, 4.7.3, 4.10, and 4.11, and `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md` in
sections 4.2.5, 4.2.6, 4.2.11, 4.2.18, and 4.2.20.

The following code comments describe the old rule. The implementation changes each one to the new rule:

- `packages/fokosdb/src/shared/partition/fokos-migration-host.ts`: the comment "A promoted key never has a
  lock" in the pending-transaction page builder.
- `packages/fokosdb/src/shared/transaction-wire-types.ts`: the `CancelRequest.items` doc comment, which says
  the release is by transaction id and that an empty list releases locally.
- `packages/fokosdb/src/server/do-partition.ts`: the `beforeCutover` comment "A promotion cannot move a locked
  key", the `txCommit` comment "A commit can release a lock a promotion waits for", and the `txCancel` doc
  comment and its `beforeForward` note.
- `packages/fokosdb/src/shared/partition/partition-store.ts`: the `deletePendingTxForHashKey` comment "a
  fully-promoted key can have no live locks here anymore".
- `packages/fokosdb/src/sharding/sharding-store.ts`: the `markPromotionsDueNow` comment "A promotion that cannot
  move a locked key parks itself 5 seconds out". The method stays for other hosts, and the comment names the
  host condition in general terms.

The change ships to a new deployment with no existing partitions, so no partition ever holds a transfer copy
under the old code. A rollback of the code on a partition that holds one is out of scope (section 2.2): the old
`commitLocal` would refuse every commit that spans a moved key and a retained key with `commit_keyset_mismatch`,
and the old `not_found` path would delete a transfer copy before the target pulled it.

#### 4.2.12 Verification

Use the existing Workers test infrastructure. No production test hook is required.

**Participant and routing tests**

- Commit a transaction across a promoted key and a retained key.
- Hold target acknowledgement after import. Both owned parts must resolve without source cleanup.
- Retry the source-local commit while only transfer copies remain. It must succeed without applying twice.
- Preserve `commit_keyset_mismatch` for an incorrect owned key set.
- Cancel through a promotion source and a split router. Transfer copies must remain until their cleanup condition.
- Send a `txCancel` with an empty `items` list to a promotion source with transfer copies and to a split router
  before completion. Each answers `cancelled` and leaves every pending row in place.
- Commit and cancel a transaction with `MAX_ITEMS_PER_TX` keys on one partition, and quarantine one on a
  promotion source. Every owned-row statement must stay within the parameter limit of section 4.2.5.
- Cut over a promotion with a lock under the key. The `movedHashKeys` fragment must name the key as soon as the
  row is `cutover`, and a prepare that arrived during target initialization must count as a transfer copy.

**Recovery tests**

- Cut over an over-age, unguarded `not_found` lock before its pending page is copied. Preserve its payload.
- Copy a quarantined lock through a promotion and through a split. The target retains `guarded_at` and logs one
  line per transaction with its own `doName`. A guarded row with an unreadable coordinator reference still
  imports.
- Recover a transaction with both owned rows and transfer copies. Change only the owned rows.
- Recheck ownership after an awaited coordinator response.
- Exclude transfer copies and guarded rows from recovery deadlines. They must not starve owned stale locks.
- Hold a promotion source in `cutover` with transfer copies only. The recovery job must report no deadline, the
  pass must set no alarm in the past, and no coordinator call must leave the source.
- Measure the stale selection and the deadline query with ten thousand pending rows under the promoted key
  and one owned stale lock behind them. Record the rows read per query and per pass; the numbers go into the
  code comment of the query, as the existing comment on `listStalePendingTx` does.
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
- Remove the size estimate and mark `cleaned` only after both source copy sets are empty. The fragment must
  stop naming the key in the same transaction.

**Emergency-repair tests**

- Call the current range owner after promotion cleanup and each current child after split completion. Each
  answers its own rows in `resolvedLocally` and `forwarded: 0`.
- Force commit and cancel for transferred quarantined rows with the existing request type.
- On a promotion source, resolve owned rows, forward the moved keys, and preserve the transfer copies of the
  same transaction. The response counts both parts.
- Use the stored transaction timestamp when applying a forced commit.
- Repeat repair after a lost response. A partition with no row must answer two zero counts and change no item.
- Call an old source with transfer copies only, after the target imported. The target must resolve its rows,
  and the copies on the source must stay. The response must carry `resolvedLocally: 0` and `forwarded: n`, and
  `forwardedTo` must name the target. The source writes one `forced resolution applied` line.
- Call an old source with transfer copies only, while the target imports. The call must fail, the copies must
  stay, and a later call must succeed. The failed call writes one `forced resolution failed` line with
  `causeCode` `partition_migrating`.
- Call a split router before completion. Every row must reach its child, and the router must mutate nothing.
- Refuse emergency repair while the addressed target imports.

The existing test "defers cutover to 'promoting' while the key has a pending transaction lock" must change.
It must prove transfer instead of deferral. Repartition tests that use locks to hold promotion also need new
control through the existing migration harness.

### 4.3 Decisions

#### 4.3.1 Empty cancellation

The current `CancelRequest` permits an empty key list as a local-only release by transaction ID. The coordinator
sends every key of a participant, so no transaction path sends that form. Emergency repair sends it when the
partition holds no row of the transaction. The form stays legal, and its meaning changes: an empty cancel
releases no row. A release by transaction id deletes transfer copies on a promotion source and the copies for
the children on a split router, and the owned part of an empty request is empty (section 4.2.5).

#### 4.3.2 Ownership-aware scans and deadlines

Four ways to keep transfer copies out of the two scans were considered:

- A filter in JavaScript after the existing `LIMIT`. Rejected: the ten selected transactions can all be
  transfer copies, and the deadline would call `owns()` on row after row at every pass.
- A per-row mark written at cutover. Rejected: SQLite rewrites the whole record on an `UPDATE`, payload
  included, so marking ten thousand pending rows of one key rewrites their payloads inside the cutover
  transaction.
- A host table with one row per transfer key, written by a new cutover hook. Rejected: it duplicates
  `fokos_repartitions`, which already holds the key and the state, and it adds a table, a migration, a hook,
  and an ordering rule for cleanup.
- The list of transfer keys as bound parameters. Rejected for now: the list is not small over time, because a
  `completed` promotion stays until its bounded cleanup drains, and bound parameters have a limit, so the host
  would need several statements per scan and a statement text that changes with the count.

The decision is the `fokos.sql.movedHashKeys()` fragment of section 4.2.1: one `SELECT` over the runtime's own
tables, spliced into the two scans as an uncorrelated subquery. It works at any number of transfer keys, binds
no parameter, keeps the statement text constant, and reads the same index the runtime reads. Section 4.2.6
gives the cost, and section 4.2.12 requires its measurement.

A later change can expose the transfer keys to hosts as a JavaScript iterable as well, for a host that needs
the keys themselves and not a predicate. Such a host runs its statement once per chunk of keys that fits under
the bound-parameter limit. That is a separate runtime API and not part of this change.

#### 4.3.3 Delivery and rollback

The milestones are in section 3. The change ships to a new deployment with no existing partitions, and a code
rollback on a partition that has cut over a locked key is out of scope (sections 2.2 and 4.2.11).

### 4.4 Open questions

#### 4.4.1 One deadline read per pass

The scheduler reads the `deadline()` of every runnable job up to three times in one pass (section 4.2.6).
While transfer copies exist, each read of the stale-job deadline steps past every copy older than the oldest
owned row. This change does not remove the repeated reads. The options are:

1. The scheduler reads each deadline once at the start of a pass and once after the steps. A step can change a
   deadline, so the read after the steps stays. This changes the scheduler for every host.
2. The stale job keeps its deadline in memory. A prepare, a commit, a cancel, a guard change, and a cleanup step
   clear the value. This changes the FokosDB job only, and adds one more place that must track every write path.
3. Keep the reads and accept the cost that the measurement of section 4.2.12 gives.

The answer changes the scheduler or the stale job, not the transfer rules.

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

An explicit key list and automatic owner discovery could permit emergency repair through an old source after
cleanup. That needs separate scope, completeness, and retry contracts. It is not required for correct lock
transfer. This RFC keeps the per-partition repair, which routes through the rows the partition still holds, and
leaves that operator-tool expansion outside its scope.

### 5.6 Repair reads owned rows only

The handler could select owned rows only and leave transfer copies out of the routed operation. A call on the
old source would then resolve nothing for the moved key and could not tell the operator so. Routing the copies
costs nothing extra, mutates nothing on the source, and reaches the target for as long as the copies exist.
This design routes them and reports the counts.

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

No. The operator calls a partition that holds rows of the transaction. The tool derives its keys from those
rows and routes each key to its current owner. The coordinator deletes key rows at transaction completion and
is not a permanent directory.

**Does a slow transaction require forced recovery?**

No. Nonterminal coordinator records remain available for automatic recovery. Quarantine requires an over-age owned
lock and `not_found`, not age alone. Emergency repair remains a safeguard for missing evidence or an unusable reference.

**Can an old source repair moved locks after cleanup?**

No. After cleanup it holds no row for those keys and answers two zero counts. Before cleanup it can: its
transfer copies route the repair to the target, `forwarded` reports how many keys went there, and `forwardedTo`
names the target. Two zero counts tell the operator to address the current owners. The target logged each
quarantined lock when it imported the lock, so that line names the partition to call.

**Does `forwarded` prove that the owners resolved the forwarded keys?**

No. It counts the keys sent. An owner that holds no row of the transaction answers with success and changes
nothing. The operator calls each partition in `forwardedTo` with the same request to read its own
`resolvedLocally`.

**Why does the source write nothing at cutover to mark the copies?**

The runtime already writes the fact: the promotion row moves to `cutover`, and it carries the hash key. A mark
on each pending row would rewrite every prepared payload under the key inside the cutover transaction, because
SQLite rewrites the whole record on an `UPDATE`. A separate table would repeat what `fokos_repartitions`
holds. The host reads the fact through `owns()` when it has the rows in hand, and through the
`movedHashKeys` SQL fragment when it scans.

**Why a SQL fragment and not a view or a list?**

A view compiles to the same plan but needs a migration each time the runtime changes its tables, and its name
is checked only when a statement runs. A list of keys needs bound parameters, which have a limit, and a
statement text that changes with the count. The fragment is checked by the compiler, changes with the runtime
code, binds nothing, and works at any count.

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
- `packages/fokosdb/src/sharding/sharding-store.ts` — `fokos_repartitions` and its indexes.
- `packages/fokosdb/src/sharding/scheduler.ts` — the pass, the job deadlines, and the alarm re-arm.
- `packages/fokosdb/test/partition-do/promotion.test.ts` — promotion deferral and the Bloom fallback.
- `packages/fokosdb/test/partition-do/tx-stale-recovery.test.ts` — quarantine and stale recovery.
- `packages/fokosdb/test/repartition/repartition-flow.test.ts` — migration, lock transfer, and acknowledgements.
