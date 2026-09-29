# RFC — A promotion cuts over with the locks of its key

**State:** Draft
**Date:** 2026-09-29
**Status:** Not implemented.

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

A promotion moves one hash key, with all its sort keys, into a range root. A split moves all locally owned keys
into children. Both use the same repartition flow and the same migration streams.

A promotion waits until its hash key has no transaction locks. `PartitionDO.hooks().beforeCutover` sets the rule:

```ts
beforeCutover: (plan) =>
  plan.kind !== "key_promotion" || this.#store.pendingLockCountForHashKey(promotedKeyOf(plan)) === 0,
```

The runtime checks the hook before target initialization and again inside the cutover transaction. The second
check covers a prepare that arrives during the initialization RPC. A refused cutover leaves the plan before
cutover. The source asks again every 5 seconds (`lockRetryMs`), and a commit or a cancel can wake it earlier.

The rule blocks the promotion, not new transactions. These are the results:

- The source continues to serve the key, and transactions on other sort keys can keep the lock count above zero.
- A quarantined lock waits for an operator, so it can hold the promotion indefinitely.
- An unfinished promotion blocks the hash split of the source. When the source is over its size cap, other keys
  then also get `partition_over_size`.
- The first hook check can prevent target initialization, so no range root exists. A lock that arrives during or
  after initialization leaves an empty range root before cutover.
- A Bloom false positive can name that empty root. This is problem 7 of the sharding client RFC. The runtime
  resolves that path again without the Bloom filter. That fixes the false route, not the wait.

### 1.2 Why a lock cannot move today

A pending row holds the lock and the prepared operation payload. The coordinator drops its copy of the payload at
`PREPARED`, and its commit request carries keys only. So a lost pending row loses a decided write. The earlier
promotion design prevented this with the zero-lock rule, and the runtime RFC kept it.

The migration host already copies pending rows for every repartition kind. Its `belongsToTarget` predicate
selects the promoted hash key. The copy alone is not enough: a promotion source still serves its other keys, so
it holds its own locks and the copies of the moved key in one table. Three host paths misread the copies:

1. `recoverStaleTransactions` deletes a `not_found` transaction when none of its keys is owned. The target can
   still need those rows, so an over-age payload disappears and never enters quarantine.
2. `TransactionParticipant.commitLocal` compares the request with every pending row of the transaction. A commit
   of the retained keys then fails against the copies of the moved keys.
3. `debugForceResolveTransaction` hands every local row of the transaction to `dispatch`. The routing is correct,
   because `dispatch` resolves each owner again. The local mutation is wrong: the routed `txCommit` and `txCancel`
   delete by transaction id, so they also delete the copies. Also, a call that finds no owned row answers with
   the same success as a real repair.

### 1.3 Glossary

- **Source** — the partition that moves keys. **Target** — a partition that receives them: the range root of a
  promotion, or a child of a split.
- **Owned row** — a pending row whose key `fokos.owns(key)` resolves to this partition.
- **Transfer key** — on a source, a hash key whose promotion is in `cutover`.
- **Transfer copy** — a source pending row under a transfer key. Only a promotion source has them. A split source
  has pending rows for its children, and this document calls them split copies.
- **Completion transaction** — the transaction of `RepartitionSource.acceptAck` that records the last
  acknowledgement, writes `completed`, and runs the `beforeComplete` hook.
- **Importing target** — a target in `awaiting_data` after source cutover, or in `importing`. **Imported
  target** — a target in `imported` or `active`.

## 2. Goals and requirements

### 2.1 In scope

- A promotion must move a key that has pending or quarantined locks.
- The source must find its transfer keys in the repartition records that the runtime already stores, through a
  SQL fragment that the runtime exports. The change adds no table, no hook, and no per-row mark.
- Local transaction validation and mutation must use owned rows only.
- Source recovery must keep every pending row that an unfinished transfer needs.
- A promotion source and a split source must keep their copies until the last acknowledgement, also when a
  cancel arrives.
- Emergency repair must stay available on each imported current owner, with the current request type. Its
  response must give the number of rows it resolved locally and the number of keys it forwarded. Each call must
  write one log line with that result, also when the call fails after the local part applied.
- A target that imports a quarantined lock must log the lock with its own `doName`.
- A `txCancel` with an empty `items` list must stay legal and release no pending row.
- The change must keep the current migration streams, import gate, routed operations, and coordinator recovery.
- Tests must cover transactions that span a moved key and a retained key.

### 2.2 Out of scope

- Removal of the import pause for writes and transaction operations.
- A pre-copy protocol, a change log, or dual writes during migration.
- Changes to the coordinator state machine or its point of no return.
- Reclamation of the item rows that a completed split source keeps.
- Removal of the mutual exclusion between a hash split and an unfinished promotion.
- Removal of the Bloom fallback for an uninitialized or `awaiting_data` range root.
- Automatic discovery of the keys or the current owners of a transaction for emergency repair.
- Emergency repair through the old source after its completion transaction deleted the copies.
- New administrative APIs, or records that prove a historical transaction outcome.
- A code rollback on a partition that has cut over a locked key (section 4.2.12).

### 2.3 Constraints

- `PREPARED` stays the point of no return. A prepared transaction must commit.
- `items` must hold committed state only. `pending_transactions` must hold unresolved operations.
- A local ownership check and the mutation it allows must run with no `await` between them.
- A transaction operation must not resolve the pending rows of an importing target.
- The source must keep its copies until the required acknowledgement is durable. An acknowledgement proves a
  complete import, not complete transactions.
- A coordinator retry must keep the transaction id, the key scope, and the decided outcome.
- A commit or a cancel must try every destination and report each failed destination.
- Commit and cancel must stay exempt from size rejection.
- A host must use the ownership methods of the runtime. A host statement must not name a sharding table; it
  reaches one only through a SQL fragment that the runtime exports.
- Tests must not need a new production hook.

## 3. Milestones

Each milestone ends with `pnpm check` and `pnpm test` green. The lock guard of `beforeCutover` stays until
milestone 4, so no production source holds a transfer copy before then. The tests of milestones 1 to 3 build that
state by hand through the migration harness.

1. **Transfer keys and owned-row scope.** Add the `movedHashKeys` fragment to the runtime (section 4.2.1). Scope
   `commitLocal`, the cancel release, quarantine, the stale selection, and the recovery deadline to owned rows
   (sections 4.2.5 and 4.2.6). An empty cancel releases nothing. Tests: "Participant and routing" and "Recovery"
   in section 4.2.13.
2. **Migration and cleanup.** Delete the pending copies of the promoted key in the completion transaction, and
   remove the pending delete from the cleanup step (section 4.2.8). Log each quarantined lock that a target
   imports (section 4.2.6). Tests: "Migration and coordinator" and "Promotion cleanup" in section 4.2.13.
3. **Emergency repair.** Route every row of the transaction, mutate owned rows only, return the two counts, log
   each call, and remove `clearPendingTxGuard` (section 4.2.7). Tests: "Emergency repair" in section 4.2.13.
4. **Guard removal.** Remove the lock count from `beforeCutover`, and make the runtime changes of section 4.2.11.
   Change the tests that hold a promotion with a lock. Run the transfer tests of milestones 1 and 2 again through
   a real cutover: a prepare lands during target initialization, the source cuts over, the target imports the
   lock, and the target commits it.

## 4. Proposed solution

### 4.1 High-level overview

A cutover moves the authority over each pending operation together with the authority over its key. There are
three rules:

| Rule | Meaning |
| --- | --- |
| Authority | The current owner resolves the pending operation, after its import is complete. |
| Retention | The source keeps the pending payload until the target acknowledges its import. |
| Repair scope | A repair routes every row it holds to the current owner and mutates only its owned rows. |

The source writes nothing new at cutover. The runtime already records each promotion with its hash key and its
state. A hash key whose promotion is in `cutover` is a transfer key, and each pending row under it is a transfer
copy. The completion transaction deletes those copies, so a copy exists only while its promotion is in
`cutover`.

A path that holds the rows of one transaction asks `owns()` for each row it cannot place. A scan that holds no
transaction excludes the transfer keys with the `movedHashKeys` SQL fragment of the runtime.

After cutover, a promotion source continues to resolve transactions on its retained keys. It keeps the copies of
the moved key out of every local transaction decision. A split source owns no key after cutover and resolves no
transaction locally.

```text
Source owns key and lock
        |
        | durable cutover
        v
Source keeps transfer copy ----- import ----> Target holds complete key and lock
        |                                          |
        | no local transaction resolution          | normal operations and recovery
        |                                          | acknowledgement
        +------ completion transaction <-----------+
                deletes the copies
```

The target can resolve a transaction before the source receives the acknowledgement. The row left on the source
is then only a copy, and it must not block the owned part of that transaction on the source.

Normal recovery continues through the coordinator. Quarantine stays the safeguard when decision evidence is
missing. Emergency repair keeps its per-partition request. The operator calls a partition that holds rows of the
transaction. That partition routes every row to its current owner, mutates only its owned rows, and returns how
many rows it resolved and how many keys it forwarded.

### 4.2 Technical details

#### 4.2.1 Transfer keys and the `movedHashKeys` fragment

**Transfer key.** `fokos_repartitions` holds `kind`, `state`, and `hash_key` for every promotion. The index
`idx_fokos_repartitions_due (state, next_attempt_at, seq)` finds the `cutover` rows with one seek. A key stops
being a transfer key when the runtime writes `completed`. The same transaction deletes every pending copy under
the key (section 4.2.8). Forced promotions can put many keys in `cutover` at once, so the number of transfer keys
has no bound of one.

A transfer copy exists only from before the cutover. After cutover, every operation on the key resolves to the
range root. A copy can remain after the target resolved its own row.

A split source has no transfer key:

- A hash split cannot queue while a promotion is in `queued`, `planned`, or `cutover`.
- A promotion cannot queue on a partition that has a split row.
- A promotion that completed before the split queued has no pending copy left.

**Two tools.** Each kind of caller uses one:

- **`fokos.owns(key)`** — for a path that holds the rows of one transaction. It reads the route override and the
  split row, never the Bloom filter. A key in the request that the runtime handed to a local handler is owned by
  construction. Only a row outside the request needs the call. Such a row exists only when a transfer copy or a
  malformed request exists, so the normal path makes no call.
- **`fokos.sql.movedHashKeys()`** — for a scan that holds no transaction. It returns the text of a `SELECT` with
  one column, `hash_key`:

  ```sql
  SELECT hash_key FROM fokos_repartitions
   WHERE kind = 'key_promotion' AND state = 'cutover' AND hash_key IS NOT NULL
  ```

The host splices the fragment into its statement as `hk NOT IN (<fragment>)`. Only `listStalePendingTx` and
`earliestUnguardedPendingTxCreatedAt` use it (section 4.2.6). The fragment rules are:

- **NULL.** When a `NOT IN` subquery returns one NULL, the predicate is NULL for every row, and the statement
  selects nothing. Stale recovery on the partition then stops with no error. `queue()` never writes a promotion
  with a NULL `hash_key`, but the fragment excludes NULL itself, so the host does not depend on that invariant.
- **Constant text.** The fragment binds no parameter, so the host statement keeps one text and the statement cache
  holds it.
- **Cost.** SQLite runs the uncorrelated subquery once per statement into an ephemeral index, and probes it once
  per scanned row. The build cost grows with the number of transfer keys, not with the number of pending rows.
- **Contract.** The runtime keeps the text in step with its tables. The host never names a `fokos_` table.
- **Wiring.** `PartitionStore` receives a callback at construction that returns the fragment. The store calls it
  only when it runs a statement, because the constructor builds the store before the runtime exists. The callback
  returns the same text on every call.

**State machines.** They do not change:

```text
Source:  queued -> planned -> cutover -> completed -> cleaned
Target:  awaiting_data -> importing -> imported -> active
```

`imported` permits application operations. `active` records a successful acknowledgement response. A lost
response can leave the target `imported` after the source recorded completion. The rules apply to one transfer.
When a target later repartitions, routed operations follow the new owners.

#### 4.2.2 Promotion stages

`S` is the hash source, `K` the promoted hash key with all its sort keys, `R` its range root, and `U` the keys that
`S` keeps.

| Stage | Source for `K` | Source for `U` | Target `R` |
| --- | --- | --- | --- |
| 1. Queued or planned | Own and serve. | Own and serve. | Not initialized. |
| 2. Before cutover | Own and serve. | Own and serve. | `awaiting_data`. |
| 3. Import incomplete | Forward; keep copies. | Own and serve. | Import; read through. |
| 4. Imported, no ack | Forward; keep copies. | Own and serve. | Serve; recover; ack. |
| 5. Completed | Forward; pending copies gone; clean items. | Own and serve. | Serve and recover. |
| 6. Cleaned | Forward; item copies gone. | Own and serve. | Serve and recover. |

1. `S` owns `K` and `U` and takes and resolves locks normally.
2. `S` serves both key sets. A lock does not hold the cutover. `R` serves no application operation. `S` refuses
   migration pulls before cutover, and a direct read through `R` cannot get around that refusal.
3. `K` is a transfer key. `S` routes each operation on `K` to `R`. It keeps the item and pending copies, and serves
   migration pages and authorized read-through calls. It must not commit, cancel, quarantine, or recover `K`
   locally. It continues all operations and stale recovery on `U`. `R` imports items, locks, and deletion
   metadata, and sends ordinary reads to `S`. `R` refuses writes, transactional reads, prepare, commit, cancel, and
   forced resolution with `partition_migrating`. It runs no stale recovery and no TTL sweep.
4. `R` serves from its complete state. It can commit, cancel, and quarantine the imported locks. It retries its
   acknowledgement independently of transaction completion. `S` keeps its copies out of local decisions.
5. The completion transaction deletes every pending copy of `K`, so `K` stops being a transfer key. `S` schedules
   the item cleanup and stops serving migration pages and read-through calls for `K`. A lost acknowledgement
   response does not stop the cleanup or the target.
6. The item batches of section 4.2.8 have deleted the item copies. `S` deletes the size estimate, and the runtime
   writes `cleaned` in the same transaction. `S` keeps the route to the range tree. `U` and its locks do not
   change. An unresolved lock can remain on `R`.

#### 4.2.3 A transaction across a moved key and a retained key

Transaction `T` locks one item under `K` and one under `U`. After cutover, `S` owns `U` and keeps a copy of `K`,
and `R` owns `K`. `dispatch` divides a commit or a cancel by the current owner:

| Part | Required action |
| --- | --- |
| `U` | `S` validates and resolves its owned rows. |
| `K`, import incomplete | `S` forwards. `R` refuses with `partition_migrating`. The coordinator retries. |
| `K`, import complete | `S` forwards. `R` resolves its owned rows. |
| Copy of `K` on `S` | Transaction resolution keeps it. The completion transaction deletes it. |

After `S` resolved `U`, a retry finds only the copy of `K` on `S`. That is an idempotent success, not
`commit_keyset_mismatch`.

#### 4.2.4 Split stages

A hash split or a range split moves every owned key into children, and the source becomes a router. A promoted
key stays in its range tree. A hash child inherits its route override. `belongsToTarget` excludes the key from the
item and pending streams of a hash child.

| Stage | Source `S` | Each child |
| --- | --- | --- |
| 1. Queued or planned | Own and serve. | Not initialized. |
| 2. Before cutover | Serve; wait for every child to initialize. | `awaiting_data`. |
| 3. Import incomplete | Forward; keep and export split copies. | Import; read through. |
| 4. Imports differ | Forward; keep all split copies. | Serve after its own import. |
| 5. Completed | Split copies gone; stay a router. | Serve and recover. |
| 6. Cleaned | Stay a router; keep item rows. | Serve and recover. |

- The cutover waits for every child to initialize, not for locks. A child cannot import before the cutover.
- After cutover, `S` applies no local write and no local transaction resolution. It runs no stale recovery and no
  TTL sweep. An importing child follows the rules of an importing target (section 4.2.2, stage 3).
- An imported child serves and recovers its locks while a sibling still imports. `S` keeps every split copy until
  every child acknowledges. It does not delete the copies of one child early.
- The completion transaction deletes every split copy (`deleteAllPendingTx`). The acknowledgements do not need
  the children to resolve their transactions.
- The old item rows of `S` are not authoritative state. Each child keeps its unresolved locks.

#### 4.2.5 Local commit and cancel

| Action | Promotion source | Split source | Importing target | Imported target |
| --- | --- | --- | --- | --- |
| Take a lock | Owned keys only. | Forward. | Refuse. | Normal prepare. |
| Apply commit | Owned rows only. | Forward. | Refuse. | Owned rows. |
| Apply cancel | Owned rows only. | Forward. | Refuse. | Owned rows. |
| Stale recovery | Owned rows only. | Does not run. | Does not run. | Normal. |
| Old `not_found` | Guard owned rows. | No local decision. | No decision. | Guard owned rows. |
| Export locks | Promoted slice. | Each child slice. | Not applicable. | No further import. |
| Delete copies | Completion transaction. | Completion transaction. | Not permitted. | Rows are owned. |

**Commit.** `TransactionParticipant.commitLocal` must use the owned set for three decisions:

1. Is there unresolved local work?
2. Does the request key set match?
3. Which rows does it apply and delete?

The method builds the owned set from the rows it already lists for the transaction. A row whose key is in the
request is owned: the runtime resolved that key to this partition in the same synchronous block. A row outside the
request is owned only when `owns()` says so; otherwise it is a transfer copy and leaves the set. When the owned set
is empty, the method returns the current idempotent success. When it is not empty, the request must match it. An
owned row outside the request means a malformed request, and the method keeps the current
`commit_keyset_mismatch`.

Validation, application, and deletion stay in one storage transaction. The delete must name the transaction and
each resolved key. Do not remove the mismatch check and keep `deletePendingTx(transactionId)`, because that delete
also removes the transfer copies.

**Cancel.** The release of `txCancel` moves from `beforeForward` into its `local` handler:

- The runtime hands the handler the owned part of the request. The handler deletes the rows of the transaction
  under those keys, with no ownership check.
- A source forwards the moved keys and keeps their copies. A split router has no owned part and releases nothing.
- `txCancel` has no `beforeForward` any more. The FIXME in `do-partition.ts` about a release that owner
  resolution can skip goes with it.

An empty `items` list stays legal. The runtime runs the `local` handler with the whole request. The owned part is
empty, so the handler releases no row and answers `cancelled`. The handler must not release by transaction id:
on a promotion source that deletes transfer copies, and on a split router it deletes the split copies that the
children still import. The coordinator sends every key of a participant, so only emergency repair sends an empty
cancel (section 4.2.7). The `CancelRequest.items` doc comment changes from "release locally" to "release
nothing".

**Statement shape.** One SQLite query on a Durable Object binds at most 100 parameters ([limits][do-limits]). One
partition can hold `MAX_ITEMS_PER_TX` (100) keys of a transaction, and each key binds two parameters. So the
commit delete, the cancel release, and `guardPendingTx` run one statement per owned key, by the primary key
`(hk, sk, transaction_id)`, inside one `transactionSync`. Each path already holds its owned keys. A path with no
owned key changes no row. These statements do not use the fragment, so a local mutation depends only on the
ownership decision of `dispatch` or `owns()`.

#### 4.2.6 Stale recovery and quarantine

`canSweepLocally()` does not change. It is false on a split router, on an importing target, and behind the
destroy fence. It stays true on a promotion source, because that source keeps other keys.

**Selection.** The stale selection excludes transfer copies with the fragment. Recovery then uses `owns()` on the
rows of the selected transaction, once per distinct hash key. The filter applies per row, because one transaction
can have owned rows and transfer copies.

**After the coordinator call.** Recovery must read the remaining rows again and check ownership again. It must not
use an ownership result from before the `await` to allow a local mutation. For owned rows:

- `COMMITTED` applies through the routed `txCommit`.
- `CANCELLED` applies through the routed `txCancel`.
- `not_found` within `IDEMPOTENCY_WINDOW_MS` (10 minutes) applies the routed cancel, as today.
- `not_found` for a lock older than the window quarantines the owned rows and logs the transition once.
- A failed coordinator call keeps the rows for the next pass. An unreadable coordinator reference also keeps
  them, and can need operator repair.

Lock age alone does not quarantine a lock. For a transfer copy, no result allows a local mutation. The branch
that deletes a `not_found` transaction when none of its keys is owned goes.

**Guard.** `guardPendingTx` takes the owned rows of the transaction, with the statement shape of section 4.2.5. A
source must not change `guarded_at` on a transfer copy. The migration keeps `guarded_at`. When an unguarded row
moves, the imported target evaluates its age and its coordinator result itself.

**Scan and deadline.** `listStalePendingTx` and `earliestUnguardedPendingTxCreatedAt` add
`AND hk NOT IN (<movedHashKeys fragment>)` to their `WHERE` clause, always, with no flag. With no promotion in
`cutover`, the subquery is one empty seek. Without the clause, the deadline of the oldest copy is in the past for
the whole import. The scheduler then sets the alarm in the past and wakes the fast path, so the pass repeats every
50 ms (`fastPathDelayMs`), with up to ten coordinator calls and ten forwards each time. With the clause, the
deadline is the oldest owned row or null.

Both queries walk `pending_transactions_created_at` from its start and skip the excluded rows. With `n` transfer
copies older than the oldest owned row, one query reads `n` index entries and probes the subquery index `n` times.
`n` is the number of locks under keys in `cutover`, not the number of transfer keys, and payload bytes do not
change it. The scheduler reads the `deadline()` of every runnable job up to three times per pass, on a pass of
any job. So while copies exist, the deadline query pays this cost up to three times per pass. The stale selection
pays it once per pass of the stale job. The cost ends when the completion transaction deletes the copies. Section
4.3.1 holds the open question about the repeated reads.

**Log of an imported quarantine.** The stale scan skips a guarded row, and the guard logs only when a lock enters
quarantine. So a target writes nothing about a guarded row it imports, unless the import logs it. When
`FokosMigrationHost` applies a pending page, it writes one line for each transaction that has guarded rows in that
page. The stream pages in `(hk, sk, transaction_id)` order, so a transaction can span pages and get one line per
page. The line carries:

- the message `"fokos/partition: imported a quarantined lock"`;
- `transactionId`, `coordinatorDoName`, `idempotencyToken`, and the encoded keys of the rows of the transaction
  in that page;
- `lockCreatedAt`, `guardedAt`, and the `doName` and `partitionId` of the target.

The rule applies to every import, so a split child logs the quarantined locks it receives too. The latest line
names the current owner, and the operator calls `debugForceResolveTransaction` there. The host writes the line
inside the page transaction:

- A page that rolls back and applies again writes the line again. A stale page that the target drops writes
  nothing.
- The line must not fail the page, because a throw rolls the page back on every retry and stops the import. So
  the host reads the coordinator reference without validation, and logs an unreadable reference as raw text.
- `FokosMigrationHost` receives a callback that returns the log fields and the `doName` and `partitionId` of the
  partition, as `TtlExpiry` receives `logParams`.

#### 4.2.7 Emergency repair

Forced resolution stays an emergency repair, not a transaction timeout. The coordinator stores its record before
prepare, keeps each nonterminal transaction, and records completion only after every participant confirms. A
promotion, a migration, or an RPC failure must not need an operator to choose an outcome. The operator diagnoses
the problem and supplies the outcome. The RPC does not diagnose or reconstruct a decision.

**Contract.** The request does not change. The response gains two counts:

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
};
```

The method still takes a partition context, and uses `shape: "local"` with `whileMigrating: "throw"`.

**Handler.** In one synchronous block, the handler does these steps:

1. It selects every local pending row of the transaction, owned rows and transfer copies alike.
2. It computes `resolvedLocally`, the number of rows for which `owns()` is true, and `forwarded`, the number of the
   other rows.
3. It takes the transaction timestamp from any row. Prepare writes one timestamp to every row of a transaction.
4. It calls the routed `txCommit` or `txCancel` with the keys of all rows.

`dispatch` resolves each key again: the owned keys apply here, and the other keys reach their current owner. The
routed operations mutate owned rows only (section 4.2.5), so a transfer copy is a routing key and never a local
write. The local part deletes exactly the rows that `resolvedLocally` counts. The caller supplies no keys and
cannot choose a subset of the rows.

The handler does not clear `guarded_at` after the routed call, and `clearPendingTxGuard` goes. A routed call that
returned has deleted every owned row of the transaction on this partition. The rows that remain are transfer
copies, which the source must not change.

A successful call has applied the outcome on every owner. `forwarded` counts the keys sent, not the rows
resolved: an owner that already resolved its rows answers with success and changes nothing.

**Errors.** When a forwarded key has an importing owner, the call fails with `partition_fanout_failed` and
`causeCode` `partition_migrating`, after the local part applied. The operator repeats the call later. The repeat
finds no owned row and forwards again.

**Log.** Each call writes one line:

- After the routed call returns: `"fokos/partition: forced resolution applied"`, with `transactionId`, `outcome`,
  `resolvedLocally`, `forwarded`, and the `doName` and `partitionId` of this partition.
- When the routed call throws: `"fokos/partition: forced resolution failed"`, with the same fields, the error
  code, and the `causeCode` of a `partition_fanout_failed`. The handler then throws the error again.

The runtime throws `partition_fanout_failed` only after the local part committed, because a failed local part
outranks a failed remote group. So on that code the failure line keeps `resolvedLocally`. On every other code the
local part applied no row, and the line carries `resolvedLocally: 0`.

**Empty sets and retries.** When the partition holds no row of the transaction, the call applies no write and
answers two zero counts. With `cancel`, the internal request has an empty `items` list, which releases no row.
The response does not prove that the transaction existed or committed. A repeated call keeps the transaction id
and the outcome, and reads the remaining rows again. After a lost response, the retry resolves nothing locally,
forwards what remains, and applies no write twice.

**Where to call.** The repair takes its keys from the pending rows of the called partition. The coordinator does
not keep them: `completeTransaction` deletes `tc_items` and `tc_participants`. This change adds no key inventory
and no transaction-wide recovery driver.

The result of a call depends on the stage:

- **Before cutover.** Call the source. It resolves its owned rows.
- **Promotion, import incomplete.** The source resolves its owned rows and forwards the moved keys. The target
  refuses, and the call fails. Repeat later.
- **Promotion, imported, before completion.** Call the source or the target. The source forwards every transfer
  copy of the transaction and counts them in `forwarded`. The target resolves its owned rows.
- **Promotion, after completion.** Call the owner that the import log names. The old source holds no copy and
  answers `forwarded: 0`. With no owned row, it answers two zero counts.
- **Split, before completion.** The router forwards every row to the children, mutates nothing, and answers
  `resolvedLocally: 0`. The call fails while a child imports.
- **Split, after completion.** The router holds no row and answers two zero counts. Call the children that the
  import log names.

The completion transaction deletes all copies of the promoted key at once. So the source forwards every row of a
transaction, or none, and never part of one.

#### 4.2.8 Migration, acknowledgement, and cleanup

The cutover transaction does not change. The state it already writes makes the key a transfer key. So a key with
ten thousand locked sort keys cuts over as fast as a key with none.

**Migration.** The flow keeps its phase order: route overrides first, then the host phase. The host streams stay
`items`, then `pending_tx`. The target must finish both streams before it accepts transaction operations.

- The pending stream must keep the prepared operation, the payload, the transaction timestamp, the coordinator
  reference, the creation time, and `guarded_at`.
- It carries no ownership mark. A target owns every row it receives, and its own repartition records decide
  ownership after that.
- The deletion metadata goes with the stream, in an empty page when the slice has no pending rows.
- Each page commits together with its cursor. The last page commits together with the `imported` state.
- A stale page must not insert a pending row again after the import or after a transaction resolution.
- Transaction operations do not change the source copies after cutover, so the pending stream reads a stable set.

**Acknowledgement.** A target acknowledges only after its complete import is durable. The source records every
required acknowledgement before it deletes a copy. A failed acknowledgement delays the cleanup, not the target.

**Cleanup.** The source deletes the two copy sets of a promoted key at different times:

1. **Pending copies, in the completion transaction.** `beforeComplete` calls `deletePendingTxForHashKey(K)` for a
   promotion, as it calls `deleteAllPendingTx()` for a split. `cleanupSourceStep` stops deleting pending rows.
   This delete has no batch bound, because it deletes a subset of what a split completion deletes in the same
   call. Section 5.8 gives the reason for one delete.
2. **Item copies, in cleanup steps.** Each step deletes at most `promotedKeyCleanupRows` (1,000) item copies, in
   `sk` order, as today. While item copies remain, `cleanupSourceStep` reports incomplete work, and the
   repartition stays `completed` and retries on its current schedule. When the item copies are empty, the step
   deletes the size estimate, and the runtime deletes the plan and writes `cleaned`.

Each step is idempotent and resumes after a restart from the remaining rows. A step changes no other hash key, and
never deletes a lock of the target. Split completion does not change.

**Invariant: the source holds every transfer copy of a transaction, or none.** Before the completion transaction,
no path deletes a copy. Transaction operations do not change the copies, because the keys route to the target. The
TTL sweep, stale recovery, and emergency repair do not change them either. The completion transaction deletes all
of them and ends the transfer key.

The target imported exactly the copies under the key. Its rows of one transaction change only as a whole: a commit
deletes the full owned set, and a cancel receives every key. So a key set that the source forwards holds every row
of the transaction at each owner, and the key set check of section 4.2.5 passes.

**TTL sweep.** The TTL sweep of a promotion source can delete expired item copies that have no lock. It must not
delete a pending row or bypass a lock. An importing target and a split router do not run it.

The change adds no durable record and no migration stream.

#### 4.2.9 Transaction interleavings

| Order | Required result |
| --- | --- |
| Prepare before cutover | The lock moves with the key and does not hold the promotion. |
| Prepare during target initialization | The cutover proceeds. The pending row joins the transfer. |
| Commit before cutover | The source commits the item and deletes its pending row before migration reads either. |
| Cutover before commit | The commit routes to the target and retries until the import completes. |
| Prepare after cutover | It reaches the target. An importing target refuses it and takes no lock. |
| Cancel during import | The source copies stay. The target refuses until its import ends, then deletes its locks. |
| Import before acknowledgement | The target resolves transactions. The source copies do not block the retained keys. |

Owner resolution and the local mutation run in one synchronous block. So a commit cannot resolve locally before
the cutover and apply locally after it, and the separate item and pending streams cannot apply a committed
operation twice.

A prepare can succeed on one destination and fail on another. The coordinator then cancels. The cancel must reach
every destination that can hold a lock, also one whose prepare response was lost. The current recovery path can
continue an undecided prepare before its hold deadline.

One commit retry budget does not need to cover the whole import. When a fan-out cannot finish, the coordinator
stays `COMMITTING`, the caller gets `transaction_commit_pending`, and durable recovery tries again. The coordinator
reaches `COMMITTED` only after every participant confirms. A cancel also stays nonterminal until every required
destination confirms. A failed fan-out must keep the keys that its next try needs.

#### 4.2.10 Availability and cost

- **Cutover.** The change removes lock lifetime from the cutover conditions. Target initialization, scheduler
  progress, and failed RPCs can still delay the cutover, so it still has no wall-time bound.
- **Import window.** Writes and transaction operations stay unavailable on an importing target. Ordinary reads go
  to the source. An imported child does not wait for a slower sibling. A promotion source continues to serve its
  retained keys.
- **Quarantined locks.** A quarantined lock no longer holds the promotion or the hash split behind it. It still
  blocks conflicting operations on the target until forced resolution.
- **Storage.** The migration keeps its calls and page budgets, and a promotion now sends pending payloads in the
  current stream. The copies stay on the source until the completion transaction, also when a cancel has already
  reached the owner.
- **Local commit and cancel.** The normal path makes no `owns()` call. One delete by transaction id becomes one
  primary-key delete per owned key.
- **Quarantine and repair.** They call `owns()` once per distinct hash key of one transaction.
- **Scan and deadline.** Section 4.2.6 gives the cost.
- **Target drain.** After the import, many imported locks can already be older than `staleTransactionMs`. A lock
  prepared during target initialization can still be younger. The target drains the stale locks ten transactions
  per pass (`staleLockScanRows`), with one coordinator call each. A key with thousands of pending rows at cutover
  therefore sends a burst of coordinator calls from the target. A partition does the same drain today after a
  restart with many stale locks.

#### 4.2.11 Code changes

**Contracts.**

- `txCancel` releases in its `local` handler, on owned rows only. It no longer releases at each forwarding hop.
- `debugForceResolveTransaction` routes every row, mutates owned rows only, returns `resolvedLocally` and
  `forwarded`, and writes one log line per call. It no longer clears `guarded_at`, and
  `PartitionStore.clearPendingTxGuard` goes.
- A promotion deletes its pending copies in `beforeComplete`, not in `cleanupSourceStep`.
- A target logs each quarantined lock that it imports.
- The runtime exports `fokos.sql.movedHashKeys()`, its first SQL fragment.

**Runtime.** The `repartitionUnblocked` signal goes. No promotion waits on a lock, so the signal has nothing to
wake, and no other host sends it. These go with it:

- `FokosSignals.repartitionUnblocked` and its branch in `#applySignals`;
- `RepartitionSource.onRepartitionUnblocked`;
- `FokosShardingStore.markPromotionsDueNow`.

`beforeCutover` stays in the runtime, and FokosDB no longer implements it. A plan that the hook holds waits and
asks again. No signal brings the retry forward.

The runtime setting `lockRetryMs` becomes `cutoverHoldRetryMs`. It is the retry interval of any `beforeCutover`
refusal, not of a lock. The default stays 5,000 ms, the test value stays 1, and the validation stays "an integer of
at least 1". The rename changes the type, the doc comment, both defaults, and the validation in
`runtime-config.ts`, the two reads in `repartition-flow.ts`, and the case in `runtime-config.test.ts`. The new doc
comment is: "When the `beforeCutover` hook of the host holds a repartition, the source asks the hook again after
this time."

**Code comments.** Each comment below states the old rule, and the implementation changes it:

- `fokos-migration-host.ts`: "A promoted key never has a lock" in the pending page builder.
- `transaction-wire-types.ts`: the `CancelRequest.items` doc comment, which says that the release is by
  transaction id and that an empty list releases locally.
- `do-partition.ts`: "A promotion cannot move a locked key" on `beforeCutover`; "A commit can release a lock a
  promotion waits for" on `txCommit`; the `txCancel` doc comment and its `beforeForward` note; and "A promotion
  moved one key of many and must not touch the rest" on `beforeComplete`, which now also names the pending copies.
- `partition-store.ts`: "a fully-promoted key can have no live locks here anymore" on `deletePendingTxForHashKey`.
  The method now runs at promotion completion.
- `repartition-flow.ts`: "the plan waits at the flat interval, until a signal wakes it" in `#initializeTargets`.
  No signal wakes the plan any more, and the comment names `cutoverHoldRetryMs`.
- `runtime-types.ts`: "for example a lock release by transaction id" on `beforeForward`. No host operation has a
  `beforeForward` after this change, so the comment names the host work in general terms.

**Prototype.** `test/sharding-prototype/fokosdb-partition-host.ts` states the old rule in code: the
`repartitionUnblocked` signal of `txCommit`, the `beforeForward` release of `txCancel`, the lock count in
`beforeCutover`, and the `debugForceResolveTransaction` handler with its `clearPendingTxGuard` call. It changes
with the host. `pnpm check` type-checks it, so its repair handler must return the new response type.

**Earlier RFCs.** Each gets the note "Superseded by `docs/agent-plans/2026-09-29-promotion-moves-its-locks.md`",
with the new rule at each statement of the old rule:

- `docs/agent-plans/2026-09-17-unified-repartition-flow.md`: sections 4.3, 4.4, 4.7.3, 4.10, and 4.11.
- `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md`: sections 4.2.5, 4.2.6, 4.2.11, 4.2.18, and 4.2.20.
- `docs/agent-plans/2026-09-26-fokos-sharding-client.md`: section 1.2 (problem 7) and section 6 ("Why can a
  promotion wait for a long time before its cutover?").

The coordinator states, the public transaction outcomes, and the normal commit routing do not change.

#### 4.2.12 Deployment and rollback

The lock guard of `beforeCutover` must stay until participant operations, stale recovery, and repair follow the
ownership rules (milestones 1 to 3). A source must not receive a transfer copy while it can still delete or
misread one.

The change ships to a new deployment with no existing partitions, so no partition holds a transfer copy under the
old code. A code rollback on a partition that holds one is out of scope. The old code would fail in two ways:

- The old `commitLocal` refuses every commit that spans a moved key and a retained key, with
  `commit_keyset_mismatch`.
- The old `not_found` path deletes a transfer copy before the target pulls it.

#### 4.2.13 Verification

Use the current Workers test infrastructure. No production test hook.

**Participant and routing**

- Commit a transaction across a moved key and a retained key.
- Hold the target acknowledgement after the import. Both owned parts resolve before the completion.
- Retry the source commit when only transfer copies remain. It succeeds and applies nothing twice.
- Keep `commit_keyset_mismatch` for a wrong owned key set.
- Cancel through a promotion source and through a split router. The copies stay until the last acknowledgement.
- Send a `txCancel` with an empty `items` list to a promotion source with transfer copies, and to a split router
  before completion. Each answers `cancelled` and keeps every pending row.
- Commit and cancel a transaction with `MAX_ITEMS_PER_TX` keys on one partition, and quarantine one on a promotion
  source. Every owned-row statement stays within 100 parameters.
- Cut over a promotion with a lock under the key. The fragment names the key from the moment the row is
  `cutover`. A prepare that arrived during target initialization counts as a transfer copy. The fragment stops
  naming the key in the completion transaction.
- Add a `key_promotion` row in `cutover` with a NULL `hash_key`. The two stale queries select the same rows as
  without that row.

**Recovery**

- Cut over an over-age, unguarded `not_found` lock before its pending page is copied. Its payload stays.
- Copy a quarantined lock through a promotion and through a split. The target keeps `guarded_at`, and writes one
  line per page for each transaction with guarded rows in that page, with its own `doName`. Include a transaction
  whose rows span two pages. A guarded row with an unreadable coordinator reference still imports.
- Recover a transaction with owned rows and transfer copies. Only the owned rows change.
- Check ownership again after the awaited coordinator response.
- Keep transfer copies and guarded rows out of the recovery deadline. They do not starve an owned stale lock.
- Hold a promotion source in `cutover` with transfer copies only. The job reports no deadline, the pass sets no
  alarm in the past, and no coordinator call leaves the source.
- Measure the stale selection and the deadline query with ten thousand pending rows under the promoted key and one
  owned stale lock behind them. Record the rows read per query and per pass in the code comment of each query, as
  the comment on `listStalePendingTx` does today.
- Keep the rows when the coordinator call fails or its reference is unreadable.

**Migration and coordinator**

- Prepare before cutover and during target initialization. The lock reaches the target.
- Commit on each side of the cutover. The item applies once and has the correct final version.
- Cancel before, during, and after the pending stream. No owned lock survives a successful cancel.
- Hold one split child in import while another serves. The source keeps its split copies until every child
  acknowledges.
- Lose an initialization, page, or acknowledgement response. Restart and resume from durable state.
- Replay a stale page after a resolution. It does not create the lock again.
- Exceed one commit retry budget during the import. The decision stays, and recovery finishes the commit.
- Fail a prepare after another group accepted. Every group that can hold a lock gets the cancel.
- Keep the Bloom fallback before cutover and the migration refusal after cutover.

**Promotion cleanup**

- Complete a promotion with the pending copies of transactions of 1 to `MAX_ITEMS_PER_TX` keys under the promoted
  key, and with pending rows of retained keys. The completion transaction deletes every copy under the promoted
  key and no other row.
- Repeat the last acknowledgement after completion. It deletes nothing more and changes no state.
- Delete the item copies in more than one step. Each step deletes at most `promotedKeyCleanupRows` item copies and
  no pending row.
- Restart between steps and repeat the cleanup. Other keys and the locks of the target stay.
- Delete the size estimate and write `cleaned` only after the item copies are empty.

**Emergency repair**

- Call the range root after promotion completion, and each child after split completion. Each answers its own rows
  in `resolvedLocally`, and `forwarded: 0`.
- Force a commit and a cancel for quarantined rows that moved, with the current request type.
- On a promotion source, resolve the owned rows, forward the moved keys, and keep the transfer copies of the same
  transaction. The response counts both parts.
- On a promotion source with owned rows and transfer copies of one transaction, force a commit while the target
  imports. The call fails. Its `forced resolution failed` line carries `resolvedLocally` equal to the owned rows it
  committed. The repeat after the import logs `resolvedLocally: 0`.
- Use the stored transaction timestamp for a forced commit.
- Repeat the repair after a lost response. A partition with no row answers two zero counts and changes no item.
- Call an old source with transfer copies only, after the target imported. The target resolves its rows. The copies
  on the source stay and keep their `guarded_at`. The response carries `resolvedLocally: 0` and `forwarded: n`, and
  the source writes one `forced resolution applied` line. After completion, the same call answers `forwarded: 0`.
- Call an old source with transfer copies only, while the target imports. The call fails, the copies stay, and a
  later call succeeds. The failed call writes one `forced resolution failed` line with `causeCode`
  `partition_migrating` and `resolvedLocally: 0`.
- Call a split router before completion. Every row reaches its child, and the router mutates nothing.
- Refuse the repair while the called target imports.

**Tests that change**

- "defers cutover to 'promoting' while the key has a pending transaction lock" (`test/partition-do/promotion.test.ts`)
  must prove the transfer instead of the wait. The repartition tests that hold a promotion with a lock need another
  control through the migration harness.
- "deletes a not_found lock directly when all its keys route away" (`test/partition-do/tx-stale-recovery.test.ts`)
  tests the branch that section 4.2.6 removes. In milestone 1 it must prove that `not_found` deletes no transfer
  copy.

### 4.3 Open questions

#### 4.3.1 One deadline read per pass

The scheduler reads the `deadline()` of every runnable job up to three times per pass (section 4.2.6). While
transfer copies exist, each read of the stale-job deadline steps past every copy older than the oldest owned row.
This change keeps the repeated reads. The options are:

1. The scheduler reads each deadline once at the start of a pass and once after the steps. A step can change a
   deadline, so the read after the steps stays. This changes the scheduler for every host.
2. The stale job keeps its deadline in memory. A prepare, a commit, a cancel, a guard change, and a completion clear
   it. This changes only the FokosDB job, and adds one more place that must track every write path.
3. Keep the reads and accept the cost that the measurement of section 4.2.13 gives.

The answer changes the scheduler or the stale job, not the transfer rules.

## 5. Alternative options

### 5.1 Stop new prepares and drain the locks

A durable drain state refuses new locks and lets the current transactions finish. Fresh transactions then cannot
delay the promotion again and again. Rejected: it starts the write outage before the cutover, and it still waits
indefinitely for a quarantined lock.

### 5.2 Remove the guard with no host change

The pending stream already copies locks. Rejected: source recovery can delete a copy before the target pulls it,
and the local commit misreads the copies (section 1.2).

### 5.3 Writes on the source during the import

Rejected: a write that the source applies during the import can be missed or overwritten by a later page. Pre-copy
with a durable change log, or a separate handoff protocol for transactions, needs a different design.

### 5.4 Delete the source copies early on cancel

A durable cancel driver that keeps every destination and retries to the end can allow an early delete. Rejected:
one retention rule for commit and cancel is simpler.

### 5.5 Transaction-wide emergency recovery

An explicit key list and automatic owner discovery can allow a repair through the old source after completion.
Rejected: it needs its own scope, completeness, and retry contracts, and the lock transfer does not need it.

### 5.6 Repair reads owned rows only

The handler selects owned rows only and leaves the transfer copies out. Rejected: a call on the old source then
resolves nothing for the moved key and cannot tell the operator. Routing the copies costs nothing, mutates nothing
on the source, and reaches the target while the copies exist.

### 5.7 Other ways to keep transfer copies out of the scans

- **A JavaScript filter after the current `LIMIT`.** Rejected: the ten selected transactions can all be copies,
  and the deadline calls `owns()` on row after row on each pass.
- **A per-row mark at cutover.** Rejected: SQLite rewrites the whole record on an `UPDATE`, so marking ten thousand
  pending rows rewrites their payloads inside the cutover transaction.
- **A host table with one row per transfer key, written by a new cutover hook.** Rejected: it repeats
  `fokos_repartitions`, and adds a table, a migration, a hook, and an order rule for cleanup.
- **A SQL view.** Rejected: it compiles to the same plan, but it needs a migration each time the runtime changes
  its tables, and SQLite checks its name only when a statement runs. The fragment is TypeScript, so the compiler
  checks it, and it changes with the runtime code.
- **The transfer keys as bound parameters.** Rejected for now: the list has no bound of one, and a statement binds
  at most 100 parameters. The host then needs more than one statement per scan, and a text that changes with the
  count. A later runtime API can expose the keys as an iterable for a host that needs the keys themselves. Such a
  host runs its statement once per chunk that fits the parameter limit.

### 5.8 Delete the pending copies in bounded cleanup batches

Each cleanup step deletes a bounded batch of pending copies, as it does for items. Rejected: a batch can delete
part of the copies of one transaction. A repair through the source before the next step then forwards part of the
rows of that transaction, and the target refuses the forced commit with `commit_keyset_mismatch`. The split
completion already deletes all pending rows of the partition in one call, so one delete of the rows under one key
adds no new cost class.

## 6. Frequently asked questions

**Why can a lock move before its transaction decides?**

The pending row carries the prepared operation. The new owner gets the row and the duty to resolve it. The import
gate prevents any use of incomplete state.

**Does an acknowledgement mean that the transactions of the target finished?**

No. It means that the target imported its slice durably. An unresolved lock can remain on the target after the
source cleanup.

**Does a slow transaction need forced resolution?**

No. The coordinator keeps each nonterminal record for automatic recovery. Quarantine needs an over-age owned lock
and `not_found`, not age alone.

**Does this remove every promotion delay?**

No. It removes the wait for locks. Initialization failures and import work can still delay a promotion. The Bloom
fallback and the import gate stay necessary.

## 7. References

- `docs/agent-plans/2026-09-26-fokos-sharding-client.md` — problem 7 and section 4.2.10.
- `docs/agent-plans/2026-09-19-fokos-sharding-runtime.md` — hooks, dispatch, ownership, and recovery.
- `docs/agent-plans/2026-09-17-unified-repartition-flow.md` — the migration and acknowledgement state machines.
- `docs/agent-plans/2026-08-30-bounded-stateful-transaction-coordination.md` — quarantine and per-partition repair.
- `docs/ideas/fokos-sharding/2026-09-09-fokos-partition-runtime.md` — the earlier zero-lock rule for promotion.
- `packages/fokosdb/src/server/do-partition.ts` — `operations`, `hooks`, and `recoverStaleTransactions`.
- `packages/fokosdb/src/server/do-transaction-coordinator.ts` — commit, recovery, and record lifetime.
- `packages/fokosdb/src/shared/partition/transaction-participant.ts` — local commit and cancel.
- `packages/fokosdb/src/shared/partition/partition-store.ts` — pending-row queries, guards, and deletes.
- `packages/fokosdb/src/shared/partition/fokos-migration-host.ts` — the item and pending streams.
- `packages/fokosdb/src/shared/partition/ttl-expiry.ts` — the `logParams` callback pattern.
- `packages/fokosdb/src/shared/transaction-wire-types.ts` — the cancel and forced-resolution types.
- `packages/fokosdb/src/sharding/runtime.ts` — dispatch, `owns`, and the import gate.
- `packages/fokosdb/src/sharding/repartition-flow.ts` — cutover, import, acknowledgement, and cleanup.
- `packages/fokosdb/src/sharding/sharding-store.ts` — `fokos_repartitions` and its indexes.
- `packages/fokosdb/src/sharding/scheduler.ts` — the pass, the job deadlines, and the alarm.
- `packages/fokosdb/src/sharding/runtime-config.ts` — `lockRetryMs` and `fastPathDelayMs`.
- `packages/fokosdb/test/partition-do/promotion.test.ts` — the promotion wait and the Bloom fallback.
- `packages/fokosdb/test/partition-do/tx-stale-recovery.test.ts` — quarantine and stale recovery.
- `packages/fokosdb/test/repartition/repartition-flow.test.ts` — migration, lock transfer, and acknowledgements.
- [Durable Objects limits][do-limits] — at most 100 bound parameters per SQL query.

[do-limits]: https://developers.cloudflare.com/durable-objects/platform/limits/
