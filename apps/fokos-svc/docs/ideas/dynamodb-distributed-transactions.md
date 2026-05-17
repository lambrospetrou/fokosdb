# Prompt: Implement DynamoDB-Style Distributed Transactions over Durable Objects

## Context and Goal

I have an existing key-value database built on Cloudflare Durable Objects (DOs) that follows the DynamoDB hash-key + sort-key model. Each data partition is a `PartitionDO` — a Durable Object that owns a contiguous key range and stores items in an internal SQLite table. I now want to introduce **distributed ACID transactions across different `PartitionDO` instances**, supporting `TransactWriteItems` and `TransactGetItems` (with conditional checks), following the design described in the [_"Distributed Transactions at Scale in Amazon DynamoDB"_ USENIX ATC 2023 paper (Idziorek et al.)](https://www.usenix.org/system/files/atc23-idziorek.pdf) and the [_Amazon DynamoDB: A Scalable, Predictably Performant, and Fully Managed NoSQL Database Service_ USENIX ATC 2022 paper (Elhemali et al.)](https://www.usenix.org/system/files/atc22-elhemali.pdf).

---

## Existing System Summary

- **`PartitionDO`**: owns a key-range shard. Has an SQLite `items` table with at least `(hashKey, sortKey, data, …)` columns. All single-item reads and writes go directly to the responsible `PartitionDO` — the transaction layer must **never intercept or slow down non-transactional operations**.
- **Request entrypoint**: a standard Cloudflare Worker that receives client API calls and fans out to the appropriate DOs.
- **Naming / routing**: there is already a mechanism to map a `(hashKey, sortKey)` to its owning `PartitionDO` stub.

---

## Target Architecture

### Components

#### 1. Client Worker

The normal Worker that accepts `TransactWriteItems` and `TransactGetItems` requests from external clients. It is responsible for:

- Validating the request (item count ≤ 100, no duplicate keys, etc.).
- Selecting or deriving the Transaction Coordinator to use (routing described below).
- Handing the full transaction payload to the TC and waiting for the final outcome.
- Returning success or a structured failure reason to the client.

The Worker is **stateless with respect to the transaction** — all durable state lives inside the TC.

#### 2. Transaction Coordinator (TC) — a Durable Object

The TC is the single durable authority for one transaction. It:

- Persists the complete transaction payload (all items, conditions, operations, assigned timestamp) before any protocol messages are sent, so a crash at any point is recoverable.
- Drives the two-phase protocol against the relevant `PartitionDO`s.
- Records every state transition durably before acting on it.
- Exposes an RPC interface that the Client Worker calls to initiate and that a Recovery Manager calls to resume.

**TC identity and routing.** For now, the TC is a standalone DO per transaction. The DO name/ID is derived deterministically from a `ClientRequestToken` when one is provided, enabling idempotent retries. Without a token the Worker generates a random text (or simply `crypto.randomUUID().replaceAll("-", "")`). A future optimization can shard TCs into a fixed-size cluster, but the interface must not preclude that.

**TC state machine.** The TC transitions through exactly these states, each persisted in its SQLite storage before the actions associated with that transition are taken:

```
CREATED → PREPARING → PREPARED → COMMITTING → COMMITTED
                    ↘                        ↗
                     → CANCELLING → CANCELLED
```

- `CREATED`: payload and timestamp written; no protocol messages sent yet.
- `PREPARING`: `Prepare` messages sent (or in-flight) to all participant `PartitionDO`s.
- `PREPARED`: all participants returned `accepted`; the TC has decided to commit. **This is the point of no return.** Once this state is persisted, the transaction MUST eventually commit.
- `COMMITTING`: `Commit` messages sent (or in-flight) to all participants.
- `COMMITTED`: all participants confirmed the commit; the TC records the final outcome and can serve idempotent re-reads of the result.
- `CANCELLING` / `CANCELLED`: any participant rejected, or a precondition failed, or a timeout expired before `PREPARED` was reached. `Cancel` messages are sent to any participant that previously returned `accepted`.

State must be persisted **before** the first outbound message for that state (write-ahead semantics). The TC does not need to wait for all `Commit` messages to complete before returning success to the client — the `PREPARED` state alone is the durability guarantee. However, it must continue driving the commit in the background and handle retries.

#### 3. PartitionDO (existing, extended)

`PartitionDO` does not know what a "transaction" is at the application level — it interacts with the TC through a **typed TypeScript interface** (a contract, not an import). The interface consists of four operations, described in the Message Contracts section below.

The `PartitionDO` must be extended with:

- A **`pending_transactions` table** to record which items are currently "locked" by an accepted-but-not-yet-committed transaction.
- A **`last_transaction_ts` column** on the `items` table: the wall-clock timestamp (milliseconds since epoch, 64-bit integer) of the most recent committed write to that item. This is the primary conflict-detection datum. This timestamp is also updated for items that only had a Condition Check evaluated even if no write happened to this specific item during the committed transaction. This prevents a future lower-timestamp write from invalidating a check that was already accepted.
- A **`max_delete_timestamp`** stored in the `ctx.storage.kv` KV storage, or create a new table `deletion_metadata` with a single column `max_deleted_ts` which is equal to `max(max_deleted_ts, last_transaction_ts)` where `last_transaction_ts` is the timestamp of the transaction deleting any item of the partition. Updated every time an item is deleted.
- A **`transaction_idempotency` table** with columns `transaction_id, transaction_ts, outcome` in order to be able to handle idempotent transaction requests. This table should be cleaned up periodically to delete entries far in the past, `Date.now() - transaction_ts > 10 minutes`. The cleanup is done after each transaction's final state transition (committed or aborted) and deleting the oldest 100 rows from `transaction_idempotency` exceeding our age threshold.

**Design choice — pending transaction storage.** Recommend a separate `pending_transactions` table keyed by `(hashKey, sortKey, transactionId)` holding `{operation, data, conditions, timestamp}`. This keeps the `items` table clean (committed state only) and makes the acceptance check a simple join. Document why this is preferred over a "shadow write" column on `items`.

---

### Message Contracts (TypeScript interface shapes — no implementation)

The following describes the _logical_ interface `PartitionDO` must expose to the TC, and which the TC must call. These will become concrete RPC endpoints. Name them precisely so the implementation step has no ambiguity.

#### `prepare(request: PrepareRequest): PrepareResponse`

Called by TC in Phase 1. The `PartitionDO` must, **atomically within a single SQLite transaction**:

1. If the `transaction.timestamp` is more than N seconds in the future (default N=5) then there is some clock skew issue and we reject this request.
2. Check, for each item the transaction would **check-only** (not write), that `transaction.timestamp > item.last_transaction_ts` and no pending write transaction is on that item.
3. Evaluate all `ConditionExpression`s for items in this partition referenced by the transaction.
4. Check, for each item the transaction would **write**, that `transaction.timestamp > item.last_transaction_ts` (no committed write is newer than this transaction's timestamp).
5. Check, for each item the transaction would **write**, that there is no other already-accepted (pending) transaction on that item.

If all checks pass: insert rows into `pending_transactions` for every item this TC touches (write or check) and return `{ outcome: "accepted" }`.

If any check fails: return `{ outcome: "rejected", reason: RejectionReason }` where `RejectionReason` is a discriminated union of `{ type: "condition_failed", itemKey }`, `{ type: "timestamp_conflict", itemKey }`, and `{ type: "pending_conflict", itemKey, conflictingTransactionId }`. **Do not modify any item or pending-transaction state on a rejection.**

Idempotency: if a `Prepare` for the same `transactionId` has already been accepted, return `{ outcome: "accepted" }` without re-running checks.

High level pseudocode for the prepare step:

```
def processPrepare(PrepareInput input):
   item = readItem(input)
   if item != NONE:
      if evaluateConditionsOnItem(item, input.conditions)
         AND evaluateSystemRestrictions(item, input)
         AND item.timestamp < input.timestamp
         AND item.ongoingTransactions == NONE:
            item.ongoingTransaction = input.transactionId
            return SUCCESS

      return FAILED

   else: #item does not exist
      item = new Item(input.item)
      if evaluateConditionsOnItem(input.conditions)
         AND evaluateSystemRestrictions(input)
         AND partition.maxDeleteTimestamp < input.timestamp:
            item.ongoingTransaction = input.transactionId
            return SUCCESS

   return FAILED
```

#### `commit(request: CommitRequest): CommitResponse`

Called by TC in Phase 2, only after the TC has reached `PREPARED` state. The `PartitionDO` must, **atomically**:

1. Apply all writes (Put / Update / Delete) to the `items` table for items in this partition.
2. Set `last_transaction_ts = transaction.timestamp` on every written item and on every check-only item touched by this transaction (so future lower-timestamp transactions cannot retroactively invalidate the check).
3. Remove all rows from `pending_transactions` for this `transactionId`.

Return `{ outcome: "committed" }`. Idempotent: if this `transactionId` is no longer in `pending_transactions` and the item's `last_transaction_ts` matches, the write was already applied — return `{ outcome: "committed" }` without error.

#### `cancel(request: CancelRequest): CancelResponse`

Called by TC when the transaction is being aborted. The `PartitionDO` must remove all `pending_transactions` rows for this `transactionId` and return `{ outcome: "cancelled" }`. Idempotent: if no rows exist, return `{ outcome: "cancelled" }`. Must **never** touch the `items` table (writes were never applied).

#### `readForTransaction(request: ReadRequest): ReadResponse`

Called by TC during Phase 1 of a `TransactGetItems`. Returns, for each requested item, `{ value, lastCommittedTs, hasPendingWrite: boolean }`. If `hasPendingWrite` is true, the TC must abort this read transaction (the item's current committed state cannot be trusted as consistent with the in-flight write). The `PartitionDO` must not write anything in response to this call — it is truly read-only.

---

### Timestamp Assignment

The TC assigns **one timestamp per transaction** at the moment the transaction enters `CREATED` state. The timestamp is `Date.now()` (milliseconds) on the TC's machine, concatenated with a suffix of the TC's DO shard ID to break ties. This produces a total order over all transactions from a given TC instance.

**Correctness does not depend on clock accuracy.** If the timestamp is "too old" relative to a recently committed item, the `prepare` check will reject it and the transaction is cancelled with `TIMESTAMP_CONFLICT`. The application retries and the new attempt gets a fresh, larger timestamp. Tight clock synchronization (which Cloudflare's infrastructure provides) reduces the false-abort rate but is not required for safety.

**No commit-wait.** Unlike Spanner, there is no need to wait out a clock-uncertainty window at commit time, because DynamoDB's protocol (and this design) provides serializability but not strict (real-time) serializability.

---

### TransactWriteItems — Full Protocol Flow

1. **Client Worker** validates request, derives TC DO name from `ClientRequestToken` (or generates a UUID), and fetches the TC DO stub.
2. **Client Worker → TC**: `initiateWrite(payload)`. Payload includes: list of operations (`PutItem | DeleteItem | ConditionCheck` per item, each with its `hashKey + sortKey`, optional `ConditionExpression`, and data), and the `ClientRequestToken`, and also the partition Durable Object IDs to communicate with.
3. **TC** (in `CREATED` state): assigns timestamp, persists full payload + timestamp + `CREATED` state. Transitions to `PREPARING`.
4. **TC** (in `PREPARING`): fans out `prepare(...)` calls **in parallel** to all distinct `PartitionDO`s that own items referenced in the transaction.
5. **TC** collects responses:
   - All `accepted` → persist `PREPARED` state → transition to `COMMITTING`.
   - Any `rejected` → persist `CANCELLING` state → fan out `cancel(...)` to all `PartitionDO`s that returned `accepted` → persist `CANCELLED` → return structured failure to client.
6. **TC** (in `COMMITTING`): fans out `commit(...)` calls **in parallel** to all participant `PartitionDO`s. At this point the TC may return success to the Client Worker **immediately** (the `PREPARED` state is the durability guarantee; the commit will complete regardless of TC crash). Continue driving commits in background, retrying any that failed, until all are confirmed.
7. Persist `COMMITTED`.

---

### TransactGetItems — Full Protocol Flow (Two-Phase Writeless)

1. **Client Worker → TC**: `initiateRead(payload)`. Payload is a list of `{ hashKey, sortKey }` items to read, and also the partition Durable Object IDs to communicate with.
2. **TC** assigns timestamp, persists `CREATED` state.
3. **TC Phase 1**: fans out `readForTransaction(...)` in parallel to all relevant `PartitionDO`s, retrieving each item's `{ value, lastCommittedTs, hasPendingWrite }`.
4. **TC** evaluates: if any item has `hasPendingWrite: true`, abort the read transaction with `READ_CONFLICT` — the caller should retry. Otherwise proceed to Phase 2.
5. **TC Phase 2**: fans out a second round of `readForTransaction(...)` to the same `PartitionDO`s.
6. **TC** compares LSNs / `lastCommittedTs` values between Phase 1 and Phase 2. If any item's value changed between the two reads, abort with `READ_CONFLICT` — retry. Otherwise, return the Phase 1 values as the consistent snapshot.
7. **No writes are ever issued by a `TransactGetItems`** — `PartitionDO` state is never mutated.

The two-phase approach ensures that the returned set is a consistent snapshot: no write transaction committed between the two read phases could have "snuck in" without being detected by the changed `lastCommittedTs`.

---

### Recovery Manager

An alarm handler (ensuring it's set every time a transaction is created) for each TC Durable Object periodically scans the TC ledger (the KV store holding transaction metadata) for transactions stuck in `PREPARING` or `COMMITTING` states beyond a configurable staleness threshold (e.g., 10 seconds).

For each stalled transaction:

- If stuck in `PREPARING`: re-send `prepare` to all participants (idempotent). If all accept, transition to `PREPARED`/`COMMITTING` and drive to completion. If any reject, transition to `CANCELLING` and cancel.
- If stuck in `COMMITTING`: re-send `commit` to all participants (idempotent). All will either return `committed` or be idempotently re-applied.

Because `prepare`, `commit`, and `cancel` are all idempotent keyed by `transactionId`, **two concurrent recovery attempts for the same transaction are safe** — they converge to the same outcome.

`PartitionDO`s can also trigger self-recovery: if a `prepare` or write request arrives and conflicts with a `pending_transactions` row whose `createdAt` is older than the staleness threshold, the `PartitionDO` should return `{ outcome: "rejected", reason: { type: "pending_conflict", conflictingTransactionId } }`, which causes the incoming TC to surface the stalled ID to the recovery manager.

Also, each `PartitionDO` should extend their alarm handlers to be scheduled (if not already scheduled) every time there is an incoming `TransactWriteItens` operation, and the alarm handler should do a scan in the `pending_transactions` table (indexed by created_at timestamp) and for transactions that are pending after N seconds (default N=60) calls the TC for that transaction in order to progress it forward and either commit or abort it.

---

### Non-Transactional Operations — Invariant

Single-item `GetItem`, `PutItem`, and `DeleteItem` operations must continue to flow directly from the Client Worker to the `PartitionDO` **without passing through any TC**. The `PartitionDO`'s SQLite logic for these operations must:

- **Reads**: return the current committed item value. If a `pending_transactions` row exists for that item, still return the committed value (the pending write is invisible until committed). Optionally surface a `hasPendingWrite` hint if the caller opts in.
- **Writes**: proceed normally against the `items` table. Set `last_transaction_ts` to `Date.now()`. These writes assign a fresh timestamp locally; they do not go through TC timestamp assignment. This is safe because non-transactional writes are serialized by the `PartitionDO`'s single-writer DO model. **If a `pending_transactions` row exists for the target item**, the non-transactional write should succeed but must set `last_transaction_ts` to a value greater than any pending transaction's timestamp, ensuring that when that pending transaction's `prepare` check runs, it will see a conflict and be rejected. This prevents a non-transactional write from silently being "overwritten" by an already-prepared transaction.

---

### Isolation Guarantees

**`TransactWriteItems`**: serializable isolation with respect to all other `TransactWriteItems`, all `TransactGetItems`, and all single-item operations. Serial order = TC-assigned timestamp order.

**`TransactGetItems`**: serializable isolation with respect to all `TransactWriteItems` and all single-item writes. May be aborted under contention (unlike MVCC-based systems) and should be retried by the caller.

**Not guaranteed**: strict (real-time) serializability. A transaction may appear to be ordered in the past if clocks are skewed, but the data will always be consistent. No arbitrary historical snapshots (no `readAt` timestamp support in this design).

**`BatchWriteItem` / `BatchGetItem` (non-transactional)**: each individual item operation is serializable; the batch as a whole is not atomically consistent.

---

### Capacity and Limits (to be enforced by Client Worker + TC)

- Max items per `TransactWriteItems`: 100.
- Max aggregate payload: 4 MB.
- No duplicate `(hashKey, sortKey)` pairs within a single transaction.
- `ClientRequestToken` idempotency window: 10 minutes (TC DO keeps data available for this period after `COMMITTED` or `CANCELLED`).

---

### What Success Looks Like

A correct implementation satisfies these invariants, which should drive the test suite:

- Two `TransactWriteItems` calls that touch overlapping items and run concurrently always produce an outcome equivalent to one running entirely before the other.
- A `TransactGetItems` that runs concurrently with a `TransactWriteItems` either sees the full pre-transaction state or the full post-transaction state — never a mix.
- A `TransactWriteItems` that crashes the TC after `PREPARED` is written eventually commits — retrying the client call (with the same `ClientRequestToken`) or waiting for the Recovery Manager must produce a `COMMITTED` outcome, not a `CANCELLED` one.
- Non-transactional single-item reads and writes are never blocked, queued, or slowed by an in-flight transaction on a _different_ item in the same `PartitionDO`.
- Retrying a `TransactWriteItems` with the same `ClientRequestToken` within 10 minutes always returns the same outcome as the first attempt.
