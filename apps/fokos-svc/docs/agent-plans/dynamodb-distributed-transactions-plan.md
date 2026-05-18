# Implementation Plan: DynamoDB-Style Distributed Transactions

**Source spec:** `docs/ideas/dynamodb-distributed-transactions.md`
**Status tracking:** Each milestone has a `[ ]` checkbox. Mark `[x]` when the milestone is reviewed and merged.

---

## Architecture Summary

Two new components layered on top of the existing `PartitionDO`:

1. **`TransactionCoordinatorDO`** — one DO per transaction (for now; schema supports future pooling). Drives 2-phase commit, owns all durable transaction state.
2. **PartitionDO extensions** — four new RPC methods (`prepare`, `commit`, `cancel`, `readForTransaction`) plus schema additions.

The Client Worker (already stateless) gains two new methods: `transactWriteItems` and `transactGetItems`.

Non-transactional single-item ops (`putItem`, `getItem`, `deleteItem`) are **never intercepted or slowed** by the transaction layer.

### Idempotency token semantics

Every transaction has two identifiers:

- **`transactionId`** — always a fresh UUID (`crypto.randomUUID().replaceAll("-", "")`), internal to one attempt.
- **`idempotencyToken`** — the external idempotency key. Equals `clientRequestToken` when the caller provides one; otherwise falls back to `transactionId`. The TC DO is **named** by `idempotencyToken`.

This separation allows the same TC DO instance to be looked up by external clients for idempotent retries while keeping internal transaction attempts uniquely identified.

---

## File Map

| File                                    | Change Type      | Purpose                                                                                           |
| --------------------------------------- | ---------------- | ------------------------------------------------------------------------------------------------- |
| `src/lib/transaction-types.ts`          | **New**          | All internal transaction protocol types                                                           |
| `src/lib/types.ts`                      | Extend           | Public-facing transaction request/result types                                                    |
| `src/lib/do-partition.ts`               | Extend           | New SQL tables + 4 RPC methods + non-tx write updates + split migration                           |
| `src/lib/do-transaction-coordinator.ts` | **Replace stub** | Full TC state machine                                                                             |
| `src/lib/db.ts`                         | Extend           | `transactWriteItems` + `transactGetItems` public API                                              |
| `src/index.ts`                          | Extend           | Wire new endpoints if HTTP layer exists                                                           |
| `wrangler.jsonc`                        | Verify           | `TransactionCoordinatorDO` already registered — confirm binding name `TRANSACTION_COORDINATOR_DO` |

---

## Milestone 1 — Transaction Types

**Goal:** Define all TypeScript interfaces so later milestones compile against a stable contract.
No runtime logic in this milestone — types only.

### 1a. New file `src/lib/transaction-types.ts`

```typescript
// ─── Shared primitives ────────────────────────────────────────────────────────

/** Internal per-attempt identifier. Always a UUID, never reused across retries. */
export type TransactionId = string;

/** External idempotency key. = clientRequestToken when provided, else transactionId. */
export type IdempotencyToken = string;

export type TransactionTimestamp = number; // Date.now() ms

// ─── PartitionDO — Prepare ────────────────────────────────────────────────────

export type TransactionOperationType = "put" | "delete" | "check";

export type TransactionItem = {
	hashKey: string;
	sortKey?: string;
	operation: TransactionOperationType;
	/** Required for "put". */
	data?: Uint8Array | string;
	/** Optional for all operation types. */
	conditions?: import("./types.js").ItemCondition[];
};

export type PrepareRequest = {
	transactionId: TransactionId;
	/** DO name of the TC. Stored in pending_transactions so the recovery alarm can call it. */
	coordinatorDoName: string;
	transactionTimestamp: TransactionTimestamp;
	/** All items in this partition that the transaction touches. */
	items: TransactionItem[];
};

export type RejectionReason =
	| { type: "condition_failed"; hashKey: string; sortKey?: string }
	| { type: "timestamp_conflict"; hashKey: string; sortKey?: string }
	| { type: "pending_conflict"; hashKey: string; sortKey?: string; conflictingTransactionId: TransactionId }
	| { type: "clock_skew"; serverTimestampMs: number; transactionTimestampMs: number };

export type PrepareResponse = { outcome: "accepted" } | { outcome: "rejected"; reason: RejectionReason };

// ─── PartitionDO — Commit ─────────────────────────────────────────────────────

export type CommitRequest = {
	transactionId: TransactionId;
	transactionTimestamp: TransactionTimestamp;
	/** Items to apply. Same items as those accepted in prepare. */
	items: TransactionItem[];
};

export type CommitResponse = { outcome: "committed" };

// ─── PartitionDO — Cancel ─────────────────────────────────────────────────────

export type CancelRequest = {
	transactionId: TransactionId;
};

export type CancelResponse = { outcome: "cancelled" };

// ─── PartitionDO — ReadForTransaction ─────────────────────────────────────────

export type ReadForTransactionRequest = {
	transactionId: TransactionId;
	items: Array<{ hashKey: string; sortKey?: string }>;
};

export type ReadForTransactionItemResult =
	| {
			found: true;
			hashKey: string;
			sortKey?: string;
			data: Uint8Array | string;
			lastCommittedTs: TransactionTimestamp;
			hasPendingWrite: boolean;
	  }
	| { found: false; hashKey: string; sortKey?: string; lastCommittedTs: TransactionTimestamp; hasPendingWrite: boolean };

export type ReadForTransactionResponse = {
	items: ReadForTransactionItemResult[];
};

// ─── TC State Machine ─────────────────────────────────────────────────────────

export type TCState = "CREATED" | "PREPARING" | "PREPARED" | "COMMITTING" | "COMMITTED" | "CANCELLING" | "CANCELLED";

// ─── TC RPC (called by Client Worker / FokosDB) ───────────────────────────────

export type TCWriteOperation = {
	hashKey: string;
	sortKey?: string;
	operation: TransactionOperationType;
	data?: Uint8Array | string;
	conditions?: import("./types.js").ItemCondition[];
	/** DO name of the PartitionDO that owns this key. Resolved by the caller. */
	partitionDoName: string;
};

export type InitiateWriteRequest = {
	/** When provided, used as idempotencyToken and TC DO name for deduplication. */
	clientRequestToken?: string;
	operations: TCWriteOperation[];
};

export type InitiateWriteResponse =
	| { outcome: "committed"; transactionId: TransactionId; idempotencyToken: IdempotencyToken }
	| { outcome: "cancelled"; transactionId: TransactionId; idempotencyToken: IdempotencyToken; reason: RejectionReason };

export type TCReadItem = {
	hashKey: string;
	sortKey?: string;
	/** DO name of the PartitionDO that owns this key. Resolved by the caller. */
	partitionDoName: string;
};

export type InitiateReadRequest = {
	items: TCReadItem[];
};

export type InitiateReadResponse =
	| { outcome: "committed"; items: ReadForTransactionItemResult[] }
	| { outcome: "aborted"; reason: "read_conflict" | "pending_write" };
```

### 1b. Extend `src/lib/types.ts`

Add public-facing aliases re-exported from `transaction-types.ts`:

```typescript
// Add to types.ts (re-export the client-facing subset):
export type {
	InitiateWriteRequest,
	InitiateWriteResponse,
	InitiateReadRequest,
	InitiateReadResponse,
	TCWriteOperation,
	TCReadItem,
} from "./transaction-types.js";
```

**TODO checklist:**

- [x] Create `src/lib/transaction-types.ts` with all types above
- [x] Add re-exports to `src/lib/types.ts`
- [x] Ensure it compiles (`npx tsc --noEmit`)

---

## Milestone 2 — Wrangler Config and Env Types

**Goal:** Verify (and fix if needed) that `wrangler.jsonc` and `worker-configuration.d.ts` expose the TC DO binding before any implementation code references it.

### 2a. `wrangler.jsonc` verification

`TransactionCoordinatorDO` is already registered. Confirm:

- Binding name is `TRANSACTION_COORDINATOR_DO` — this exact string is used throughout the implementation (in `PartitionDO` recovery alarm and in `FokosDB`).
- The migration entry includes `TransactionCoordinatorDO` in `new_sqlite_classes` (already in the `v1` migration tag per the codebase exploration).

If the binding name in `wrangler.jsonc` differs, update it to `TRANSACTION_COORDINATOR_DO` and update all references.

### 2b. `worker-configuration.d.ts`

Ensure `Env` includes:

```typescript
TRANSACTION_COORDINATOR_DO: DurableObjectNamespace<TransactionCoordinatorDO>;
```

**TODO checklist:**

- [x] Confirm `wrangler.jsonc` binding name is `TRANSACTION_COORDINATOR_DO`
- [x] Confirm `worker-configuration.d.ts` `Env` has `TRANSACTION_COORDINATOR_DO`
- [x] Run `wrangler deploy --dry-run` (or `wrangler dev`) to confirm config is valid

---

## Milestone 3 — PartitionDO Schema Migrations

**Goal:** Add the new tables and the `last_transaction_ts` column to every `PartitionDO`'s SQLite database via the existing `SQLSchemaMigrations` mechanism. Update non-transactional writes to gate on pending transactions before proceeding.

### New SQL migrations to append to `sqlMigrations` array in `do-partition.ts`

```sql
-- Migration 2: Add last_transaction_ts column to items table
ALTER TABLE items ADD COLUMN last_transaction_ts INTEGER NOT NULL DEFAULT 0;

-- Migration 3: pending_transactions table
-- Stores items "locked" by an accepted-but-not-yet-committed transaction.
-- Keyed by (hk, sk, transaction_id); one pending entry per item is enforced
-- at the application layer (prepare rejects a second conflicting transaction).
-- coordinator_do_name: the TC DO name stored here so the recovery alarm can call it.
-- created_at: wall-clock ms, used by recovery alarm to detect stale entries.
CREATE TABLE IF NOT EXISTS pending_transactions (
    hk                    TEXT    NOT NULL,
    sk                    TEXT    NOT NULL DEFAULT '',
    transaction_id        TEXT    NOT NULL,
    transaction_ts        INTEGER NOT NULL,
    operation             TEXT    NOT NULL,  -- 'put' | 'delete' | 'check'
    data                  ANY,               -- NULL for delete/check
    conditions_json       TEXT,              -- JSON-encoded ItemCondition[] | NULL
    coordinator_do_name   TEXT    NOT NULL DEFAULT '',
    created_at            INTEGER NOT NULL,
    PRIMARY KEY (hk, sk, transaction_id)
) WITHOUT ROWID, STRICT;

-- Index to let the recovery alarm scan efficiently by age.
CREATE INDEX IF NOT EXISTS pending_transactions_created_at
    ON pending_transactions (created_at);

-- Migration 4: deletion_metadata table (singleton, tracks max committed delete ts per partition)
-- Note: INTEGER PRIMARY KEY makes this a rowid table; do not use WITHOUT ROWID here.
CREATE TABLE IF NOT EXISTS deletion_metadata (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    max_deleted_ts  INTEGER NOT NULL DEFAULT 0
) STRICT;
INSERT OR IGNORE INTO deletion_metadata (id, max_deleted_ts) VALUES (1, 0);
```

> **Design note — why `pending_transactions` is a separate table:**
> Keeping pending (uncommitted) state in a separate table means the `items` table always holds only committed state. This makes reads simple (`SELECT` from `items` = committed truth), avoids NULL-column pollution on every row, and makes the acceptance check a clean lookup rather than a multi-branch conditional update.

### Non-transactional write behavior (`putItem` and `deleteItem`)

Before executing a non-transactional write, check whether a pending transaction is holding a lock on the target item. If one exists, **reject the write** — the caller must retry later (after the transaction commits or is cancelled):

```typescript
// At the top of putItem / deleteItem local handler, before the write SQL:
const sk = opts.sortKey ?? "";
const pendingRow = this.ctx.storage.sql
	.exec<{ transaction_id: string }>(`SELECT transaction_id FROM pending_transactions WHERE hk = ? AND sk = ? LIMIT 1`, opts.hashKey, sk)
	.toArray()[0];
if (pendingRow) {
	throw new Error(
		`fokos/partition: item is locked by an in-progress transaction ` + `(transactionId=${pendingRow.transaction_id}), retry later.`,
	);
}
// FIXME: The ATC 2023 paper §4 "Adapting timestamp ordering for key-value operations"
// describes optimizations where a non-transactional write can proceed even with a pending
// transaction by using a higher timestamp to force the pending transaction to abort on
// commit, rather than blocking the write outright. Implement those optimizations to reduce
// false rejections under contention.
```

If no pending transaction exists, proceed with the write as normal and set `last_transaction_ts = Date.now()` — inlined into the existing UPSERT:

```sql
-- putItem — add last_transaction_ts to the existing UPSERT:
INSERT INTO items (hk, sk, data, ttl_epoch_utc_seconds, v, last_transaction_ts)
VALUES (?, ?, ?, ?, 1, ?)                   -- last ? = Date.now()
ON CONFLICT(hk, sk) DO UPDATE SET
  data = excluded.data,
  ttl_epoch_utc_seconds = excluded.ttl_epoch_utc_seconds,
  v = v + 1,
  last_transaction_ts = excluded.last_transaction_ts;

-- deleteItem — add last_transaction_ts to the DELETE (items table only; deletion_metadata
-- is only updated by transactional commits, not non-transactional deletes).
-- After the DELETE, update last_transaction_ts on any surviving row is moot; the row is gone.
-- No extra UPDATE needed.
```

**TODO checklist:**

- [x] Add migrations 2–4 to `sqlMigrations` in `do-partition.ts`
- [x] Add pending-transaction conflict check at the top of `putItem` and `deleteItem` local handlers
- [x] Update `putItem` SQL to include `last_transaction_ts = Date.now()` in the upsert
- [x] Update `deleteItem` to set `last_transaction_ts = Date.now()` on the deleted item's row before deleting (so any read that races sees the tombstone timestamp — actually since the row is deleted this is moot; just ensure the pending-transaction check runs first)
- [x] Run existing test suite to confirm no regressions (`npm test`)

---

## Milestone 4 — PartitionDO Transaction RPC Methods

**Goal:** Implement the four transaction protocol methods on `PartitionDO`. No TC code yet — these can be unit-tested in isolation using direct RPC calls.

### 4a. `prepare(request: PrepareRequest): Promise<PrepareResponse>`

All checks and the final insert must run inside a **single `storage.sql` transaction** (use `ctx.storage.transactionSync` or wrap with `BEGIN IMMEDIATE` / `COMMIT` via `sql.exec`):

```
1. Clock skew guard:
   IF request.transactionTimestamp > Date.now() + 5_000:
     return { outcome: "rejected", reason: { type: "clock_skew", serverTimestampMs: Date.now(), transactionTimestampMs: request.transactionTimestamp } }

2. For each item in request.items:

   a. Read current committed state:
      itemRow = SELECT last_transaction_ts, data, v FROM items WHERE hk = ? AND sk = ?

   b. Read pending state:
      pendingRow = SELECT transaction_id, transaction_ts
                   FROM pending_transactions WHERE hk = ? AND sk = ? LIMIT 1

   c. If pendingRow exists:
      IF pendingRow.transaction_id == request.transactionId:
        continue  -- this item was already locked by the same transaction (idempotent re-prepare)
      return { outcome: "rejected", reason: { type: "pending_conflict",
               hashKey, sortKey, conflictingTransactionId: pendingRow.transaction_id } }

   d. Evaluate conditions on current committed state:
      itemSnapshot = itemRow ? { found: true, hk, sk, v: itemRow.v } : { found: false, hk, sk }
      evaluateConditionsOnItem(itemSnapshot, item.conditions, "prepare")
        -- throws on failure → catch and return { outcome: "rejected", reason: { type: "condition_failed", hashKey, sortKey } }

   e. Timestamp conflict check (applies to ALL operation types — write and check-only):
      IF itemRow EXISTS:
        IF request.transactionTimestamp <= itemRow.last_transaction_ts:
          return { outcome: "rejected", reason: { type: "timestamp_conflict", hashKey, sortKey } }

      IF itemRow DOES NOT EXIST AND item.operation is "put" or "delete":
        max_deleted_ts = SELECT max_deleted_ts FROM deletion_metadata WHERE id = 1
        IF request.transactionTimestamp <= max_deleted_ts:
          return { outcome: "rejected", reason: { type: "timestamp_conflict", hashKey, sortKey } }

3. All checks passed — lock every item:
   For each item in request.items:
     INSERT OR IGNORE INTO pending_transactions
       (hk, sk, transaction_id, transaction_ts, operation, data, conditions_json,
        coordinator_do_name, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, request.coordinatorDoName, Date.now())
   -- INSERT OR IGNORE: safe for idempotent re-prepare of individual items

4. return { outcome: "accepted" }
```

### 4b. `commit(request: CommitRequest): Promise<CommitResponse>`

All in a single SQL transaction:

```
1. Idempotency check:
   IF no pending_transactions rows exist for this transactionId:
     return { outcome: "committed" }   -- already committed or never locked (safe no-op)

2. For each item in request.items:
   pendingRow = SELECT * FROM pending_transactions
                WHERE hk = ? AND sk = ? AND transaction_id = request.transactionId

   IF pendingRow NOT FOUND: continue  -- already committed for this item

   SWITCH pendingRow.operation:
     CASE "put":
       INSERT INTO items (hk, sk, data, ttl_epoch_utc_seconds, v, last_transaction_ts)
       VALUES (?, ?, ?, NULL, 1, request.transactionTimestamp)
       ON CONFLICT(hk, sk) DO UPDATE SET
         data = excluded.data,
         ttl_epoch_utc_seconds = excluded.ttl_epoch_utc_seconds,
         v = v + 1,
         last_transaction_ts = excluded.last_transaction_ts

     CASE "delete":
       DELETE FROM items WHERE hk = ? AND sk = ?
       UPDATE deletion_metadata
         SET max_deleted_ts = MAX(max_deleted_ts, request.transactionTimestamp)
         WHERE id = 1

     CASE "check":
       -- No data write; update last_transaction_ts so a lower-ts transaction
       -- cannot retroactively invalidate this accepted check.
       UPDATE items
         SET last_transaction_ts = MAX(last_transaction_ts, request.transactionTimestamp)
         WHERE hk = ? AND sk = ?

3. DELETE FROM pending_transactions WHERE transaction_id = request.transactionId

4. return { outcome: "committed" }
```

### 4c. `cancel(request: CancelRequest): Promise<CancelResponse>`

```
DELETE FROM pending_transactions WHERE transaction_id = request.transactionId
-- Never touches items table — pending writes are discarded.

return { outcome: "cancelled" }
```

Idempotent: if no rows exist for `transactionId`, the DELETE is a no-op and `"cancelled"` is still returned.

### 4d. `readForTransaction(request: ReadForTransactionRequest): Promise<ReadForTransactionResponse>`

Purely read-only — no writes of any kind:

```
results = []
For each item in request.items:
  itemRow = SELECT data, last_transaction_ts, v FROM items WHERE hk = ? AND sk = ?
  pendingRow = SELECT 1 FROM pending_transactions WHERE hk = ? AND sk = ? LIMIT 1

  hasPendingWrite = pendingRow != null
  lastCommittedTs = itemRow ? itemRow.last_transaction_ts : 0

  IF itemRow:
    results.push({ found: true, hashKey, sortKey, data: itemRow.data, lastCommittedTs, hasPendingWrite })
  ELSE:
    results.push({ found: false, hashKey, sortKey, lastCommittedTs, hasPendingWrite })

return { items: results }
```

**TODO checklist:**

- [x] Implement `prepare` with all 4 steps (single SQL transaction, idempotency via `pending_transactions`)
- [x] Implement `commit` (set `last_transaction_ts = request.transactionTimestamp` for all operation types)
- [x] Implement `cancel`
- [x] Implement `readForTransaction`
- [ ] Write unit tests in `do-partition.test.ts`:
  - `prepare`: accepted path, condition_failed, timestamp_conflict, pending_conflict, clock_skew, idempotent re-prepare (pending row with same transactionId → accepted without re-running checks)
  - `commit`: put written with correct `last_transaction_ts`, delete updates `deletion_metadata`, check-only updates `last_transaction_ts`, idempotent re-commit
  - `cancel`: clears rows, idempotent cancel on already-cleared transaction
  - `readForTransaction`: found/not-found, `hasPendingWrite` flag set correctly
  - Non-transactional `putItem` with no pending tx: proceeds, stamps `last_transaction_ts = Date.now()`
  - Non-transactional `putItem` with a pending tx on the same item: rejected with conflict error

---

## Milestone 5 — Split Migration: Extend for New Tables

**Goal:** When a `PartitionDO` splits and a child migrates data from the parent, the child must also receive the transaction-related state for its key range. Without this, a newly split partition would lose pending transaction locks and the deletion metadata high-water mark, breaking correctness.

### What needs to be migrated

| Table                                | Migration needed                                                                                                                          |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `items` (with `last_transaction_ts`) | Already migrated by existing `getItemsBatch` — `last_transaction_ts` is a column on `items` so it comes along automatically              |
| `pending_transactions`               | Rows for items in the child's key range must be migrated so in-flight transaction locks are preserved                                     |
| `deletion_metadata.max_deleted_ts`   | Child must inherit the parent's value as a safe upper bound for its key range                                                             |

### 5a. New parent RPC: `getPartitionTransactionMetadata`

Add a new method to `PartitionDO` (called once by the child at the very end of migration, after all `items` batches are complete):

```typescript
async getPartitionTransactionMetadata(opts: {
  childPartitionContext: PartitionContextResolved;
}): Promise<{
  maxDeletedTs: number;
  pendingTransactions: Array<{
    hk: string; sk: string; transaction_id: string; transaction_ts: number;
    operation: string; data: string | Uint8Array | null;
    conditions_json: string | null; coordinator_do_name: string; created_at: number;
  }>;
}> {
  // Verify caller is a known child (same guard as getItemsBatch).
  const splitStatus = this.ensureTopology(this.pCtx()).splitStatus();
  invariant(splitStatus?.status === "split_started", ...);
  const isKnownChild = splitStatus.childPartitionContexts.some(c => c.doName === opts.childPartitionContext.doName);
  invariant(isKnownChild, ...);

  const isCorrectHashChildPartition = topology.makeIsCorrectChildHashPartition(pCtx, opts.childPartitionContext);

  // Fetch pending_transactions rows for items in the child's key range.
  const pendingRows = this.ctx.storage.sql.exec<...>(
    `SELECT hk, sk, transaction_id, transaction_ts, operation, data,
            conditions_json, coordinator_do_name, created_at
     FROM pending_transactions`
  ).toArray().filter(row => isCorrectHashChildPartition(row.hk, row.sk === "" ? undefined : row.sk));

  const maxDeletedTs = this.ctx.storage.sql.exec<{ max_deleted_ts: number }>(
    `SELECT max_deleted_ts FROM deletion_metadata WHERE id = 1`
  ).toArray()[0]?.max_deleted_ts ?? 0;

  return { maxDeletedTs, pendingTransactions: pendingRows };
}
```

### 5b. Child `runMigration` extension

After the cursor loop completes (`cursor === null`), call `getPartitionTransactionMetadata` and apply the result atomically:

```typescript
// After the items migration loop in runMigration():
const { maxDeletedTs, pendingTransactions } = await parentStub.getPartitionTransactionMetadata({
	childPartitionContext: pCtx,
});

// Apply atomically:
if (pendingTransactions.length > 0) {
	for (const row of pendingTransactions) {
		this.ctx.storage.sql.exec(
			`INSERT OR IGNORE INTO pending_transactions
         (hk, sk, transaction_id, transaction_ts, operation, data,
          conditions_json, coordinator_do_name, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			row.hk,
			row.sk,
			row.transaction_id,
			row.transaction_ts,
			row.operation,
			row.data,
			row.conditions_json,
			row.coordinator_do_name,
			row.created_at,
		);
	}
}
// Set deletion_metadata to at least the parent's value (safe upper bound for this key range).
this.ctx.storage.sql.exec(`UPDATE deletion_metadata SET max_deleted_ts = MAX(max_deleted_ts, ?) WHERE id = 1`, maxDeletedTs);
```

### 5c. `MigratedItem` type extension

The existing `MigratedItem` type and `getItemsBatch` response are unchanged — migration of `items` (including `last_transaction_ts`) happens automatically since the column is part of `items`. Only the `getPartitionTransactionMetadata` call is added at the end.

**TODO checklist:**

- [x] Add `getPartitionTransactionMetadata` to `PartitionDO` (paginated, 20 MB per batch)
- [x] Extend `runMigration` in `PartitionDO` to call `getPartitionTransactionMetadata` after the items loop and apply the result
- [x] Verify that `getItemsBatch` already propagates `last_transaction_ts` (it should, since it reads all columns)
- [ ] Write migration tests: after a split, child partition has correct `pending_transactions` and `deletion_metadata` values

---

## Milestone 6 — TransactionCoordinatorDO

**Goal:** Full TC implementation. This is the most complex milestone.

### 6a. TC SQLite Schema

The schema is designed so that one TC DO instance can manage **multiple transactions** (future pooling). All tables are keyed by `transaction_id`; `tc_state` additionally carries the `idempotency_token` as its primary key for deduplication lookups.

The `rejection_reason_json` column on `tc_state` stores the serialized `RejectionReason` for cancelled transactions — no separate KV store needed, everything stays in one table.

```sql
-- Stored inside TransactionCoordinatorDO's own SQLite storage.

-- One row per idempotency_token (= one logical client operation).
-- transaction_id is the internal per-attempt UUID.
-- rejection_reason_json: populated on CANCELLING, so the reason can be re-served idempotently.
CREATE TABLE IF NOT EXISTS tc_state (
    idempotency_token       TEXT    NOT NULL PRIMARY KEY,
    transaction_id          TEXT    NOT NULL,
    state                   TEXT    NOT NULL,  -- TCState enum
    transaction_ts          INTEGER NOT NULL,
    created_at              INTEGER NOT NULL,
    rejection_reason_json   TEXT               -- JSON-encoded RejectionReason | NULL
) WITHOUT ROWID, STRICT;

-- One row per (transaction_id, partition). Tracks per-participant protocol outcomes.
CREATE TABLE IF NOT EXISTS tc_participants (
    transaction_id      TEXT    NOT NULL,
    partition_do_name   TEXT    NOT NULL,
    prepare_outcome     TEXT,   -- NULL | 'accepted' | 'rejected'
    commit_outcome      TEXT,   -- NULL | 'committed'
    cancel_outcome      TEXT,   -- NULL | 'cancelled'
    PRIMARY KEY (transaction_id, partition_do_name)
) WITHOUT ROWID, STRICT;

-- Full transaction payload per (transaction_id, item).
-- Persisted atomically with CREATED state before any protocol messages are sent.
CREATE TABLE IF NOT EXISTS tc_items (
    transaction_id      TEXT    NOT NULL,
    hk                  TEXT    NOT NULL,
    sk                  TEXT    NOT NULL DEFAULT '',
    operation           TEXT    NOT NULL,  -- 'put' | 'delete' | 'check'
    data                ANY,
    conditions_json     TEXT,
    partition_do_name   TEXT    NOT NULL,
    PRIMARY KEY (transaction_id, hk, sk)
) WITHOUT ROWID, STRICT;
```

### 6b. TC State Machine

```
CREATED → PREPARING → PREPARED → COMMITTING → COMMITTED
                    ↘                        ↗
                     → CANCELLING → CANCELLED
```

Every state is written to `tc_state.state` **before** any protocol messages for that state are sent (write-ahead semantics).

### 6c. `initiateWrite(request: InitiateWriteRequest): Promise<InitiateWriteResponse>`

```
1. Derive identifiers:
   transactionId    = crypto.randomUUID().replaceAll("-", "")
   idempotencyToken = request.clientRequestToken ?? transactionId
   coordinatorDoName = ctx.id.name  -- the TC DO's own name

2. Idempotency check (lookup by idempotencyToken):
   row = SELECT transaction_id, state, rejection_reason_json FROM tc_state
         WHERE idempotency_token = idempotencyToken
   IF row exists:
     switch row.state:
       "COMMITTED":  return { outcome: "committed", transactionId: row.transaction_id, idempotencyToken }
       "CANCELLED":  return { outcome: "cancelled", transactionId: row.transaction_id, idempotencyToken,
                              reason: JSON.parse(row.rejection_reason_json) }
       "PREPARING" | "PREPARED" | "COMMITTING":
                     await runCommit(row.transaction_id, idempotencyToken); return committed
       "CANCELLING": await runCancel(row.transaction_id, idempotencyToken); return cancelled
       "CREATED":    fall through — re-drive from CREATED

3. Validate request:
   - operations.length <= 100
   - no duplicate (hashKey, sortKey) pairs
   - total payload <= 4 MB

4. Assign timestamp:
   transactionTs = Date.now()
   // TODO: append DO shard suffix for tie-breaking when TC pooling is introduced

5. Persist CREATED state atomically (single SQL transaction):
   BEGIN;
   INSERT INTO tc_state
     (idempotency_token, transaction_id, state, transaction_ts, created_at)
   VALUES (idempotencyToken, transactionId, 'CREATED', transactionTs, Date.now());
   INSERT INTO tc_items (transaction_id, hk, sk, operation, data, conditions_json, partition_do_name)
     VALUES ... (one row per operation);
   INSERT INTO tc_participants (transaction_id, partition_do_name)
     VALUES ... (one row per distinct partitionDoName);
   COMMIT;

   Set recovery alarm: await ctx.storage.setAlarm(Date.now() + 5_000)

6. Transition to PREPARING:
   UPDATE tc_state SET state = 'PREPARING' WHERE idempotency_token = idempotencyToken

7. Fan out prepare() in parallel to all participants:
   results = await Promise.allSettled(
     participants.map(partitionDoName =>
       partitionDoStub.prepare({
         transactionId,
         coordinatorDoName,
         transactionTimestamp: transactionTs,
         items: itemsForPartition(transactionId, partitionDoName),
       })
     )
   )
   -- Record each outcome in tc_participants (accepted or rejected)

8. Evaluate outcomes:
   IF all accepted:
     UPDATE tc_state SET state = 'PREPARED' WHERE idempotency_token = idempotencyToken
     -- PREPARED is the point of no return. Return success to caller immediately.
     ctx.waitUntil(runCommit(transactionId, idempotencyToken))
     return { outcome: "committed", transactionId, idempotencyToken }

   IF any rejected:
     firstRejectionReason = first rejected participant's reason
     UPDATE tc_state
       SET state = 'CANCELLING', rejection_reason_json = JSON.stringify(firstRejectionReason)
       WHERE idempotency_token = idempotencyToken
     Fan out cancel() to participants where prepare_outcome == 'accepted' (in parallel)
     UPDATE tc_state SET state = 'CANCELLED' WHERE idempotency_token = idempotencyToken
     return { outcome: "cancelled", transactionId, idempotencyToken, reason: firstRejectionReason }

9. runCommit(transactionId, idempotencyToken):
   UPDATE tc_state SET state = 'COMMITTING' WHERE idempotency_token = idempotencyToken
   Fan out commit() to all participants, retrying failures with exponential backoff.
   All must return "committed" (idempotent — safe to retry indefinitely).
   UPDATE tc_participants SET commit_outcome = 'committed' as each one confirms.
   Once all confirmed:
     UPDATE tc_state SET state = 'COMMITTED' WHERE idempotency_token = idempotencyToken
```

### 6d. `initiateRead(request: InitiateReadRequest): Promise<InitiateReadResponse>`

Read-only transactions use an ephemeral TC instance (random UUID). No `clientRequestToken` needed.

> **Intentional deviation from spec:** The spec says the TC should persist `CREATED` state for `TransactGetItems`. We skip all SQLite writes for reads. Since reads never mutate `PartitionDO` state, a TC crash mid-read is harmless — the client gets no response and retries. There is no durability requirement for read-only transactions, so state persistence would only add latency with no correctness benefit.

```
1. transactionId = crypto.randomUUID().replaceAll("-", "")

2. Phase 1: fan out readForTransaction() to all relevant partitions in parallel.
   Collect per item: { value, lastCommittedTs, hasPendingWrite }

3. If any item has hasPendingWrite == true:
   return { outcome: "aborted", reason: "pending_write" }

4. Phase 2: fan out readForTransaction() again to the same partitions in parallel.

5. Compare Phase 1 vs Phase 2 per item:
   If any item's lastCommittedTs differs:
     return { outcome: "aborted", reason: "read_conflict" }

6. return { outcome: "committed", items: phase1Results }
   (no writes to any PartitionDO)
```

### 6e. TC Alarm (Recovery Manager)

```typescript
async alarm(): Promise<void> {
  const rows = sql.exec<{
    idempotency_token: string; transaction_id: string; state: TCState; created_at: number;
  }>(
    `SELECT idempotency_token, transaction_id, state, created_at FROM tc_state
     WHERE state NOT IN ('COMMITTED', 'CANCELLED')`
  ).toArray();

  const STALE_THRESHOLD_MS = 5_000;
  for (const row of rows) {
    if (Date.now() - row.created_at < STALE_THRESHOLD_MS) continue;
    switch (row.state) {
      case "PREPARING":
        await runPrepareRecovery(row.transaction_id, row.idempotency_token);
        break;
      case "COMMITTING":
        await runCommitRecovery(row.transaction_id, row.idempotency_token);
        break;
      case "CANCELLING":
        await runCancelRecovery(row.transaction_id, row.idempotency_token);
        break;
    }
  }

  // Re-arm if any non-terminal transactions remain.
  const remaining = sql.exec<{ n: number }>(
    `SELECT COUNT(*) as n FROM tc_state WHERE state NOT IN ('COMMITTED', 'CANCELLED')`
  ).toArray()[0]?.n ?? 0;
  if (remaining > 0) {
    await ctx.storage.setAlarm(Date.now() + STALE_THRESHOLD_MS);
  }
}
```

**`runPrepareRecovery(transactionId, idempotencyToken)` algorithm:**

```
1. Load all participants for this transactionId from tc_participants.
2. For participants where prepare_outcome IS NULL (no recorded response yet):
   Re-fan out prepare() calls in parallel (idempotent — PartitionDO returns "accepted" if
   already locked, checks pass again if not locked yet).
   Record each outcome in tc_participants.
3. Evaluate outcomes across ALL participants (including previously recorded ones):
   IF all participants have prepare_outcome == 'accepted':
     UPDATE tc_state SET state = 'PREPARED' WHERE idempotency_token = idempotencyToken
     await runCommit(transactionId, idempotencyToken)
   IF any participant has prepare_outcome == 'rejected':
     firstRejectionReason = first rejection reason from tc_participants or prepare response
     UPDATE tc_state SET state = 'CANCELLING',
                         rejection_reason_json = JSON.stringify(firstRejectionReason)
       WHERE idempotency_token = idempotencyToken
     await runCancelRecovery(transactionId, idempotencyToken)
```

**`runCommitRecovery(transactionId, idempotencyToken)` algorithm:**

1. Load participants where commit_outcome IS NULL.
2. Re-fan out commit() to those participants in parallel (idempotent).
3. Record commit_outcome = 'committed' for each that responds.
4. Once all commit_outcome == 'committed':
   UPDATE tc_state SET state = 'COMMITTED' WHERE idempotency_token = idempotencyToken

**`runCancelRecovery(transactionId, idempotencyToken)` algorithm:**

1. Load participants where prepare_outcome == 'accepted' AND cancel_outcome IS NULL.
2. Re-fan out cancel() to those participants in parallel (idempotent).
3. Record cancel_outcome = 'cancelled' for each that responds.
4. Once all eligible participants have cancel_outcome == 'cancelled':
   UPDATE tc_state SET state = 'CANCELLED' WHERE idempotency_token = idempotencyToken

### 6f. `recoverTransaction()` RPC

Called by PartitionDO recovery alarm to poke a stale TC into scheduling its alarm:

```typescript
async recoverTransaction(): Promise<void> {
  if (!(await this.ctx.storage.getAlarm())) {
    await this.ctx.storage.setAlarm(Date.now());
  }
}
```

### 6g. TC DO naming and routing

- DO name = `clientRequestToken` if provided (validate: printable ASCII, max 128 chars)
- DO name = `transactionId` (UUID) otherwise
- Caller derives the DO ID via `env.TRANSACTION_COORDINATOR_DO.idFromName(idempotencyToken)`

**TODO checklist:**

- [x] Create TC SQLite schema (tc_state with `rejection_reason_json`, tc_participants, tc_items) with migrations
- [x] Implement `initiateWrite` with full 9-step algorithm (stores rejection reason in `tc_state.rejection_reason_json`)
- [x] Implement `runCommit` background driver with exponential backoff retry
- [x] Implement `initiateRead` two-phase algorithm
- [x] Implement alarm recovery handler covering PREPARING, COMMITTING, CANCELLING
- [x] Implement `recoverTransaction()` RPC

---

## Milestone 7 — Recovery: PartitionDO Stale Pending Transactions

**Goal:** PartitionDO alarms detect stale pending transaction locks and poke the owning TC to recover them.

### 7a. PartitionDO alarm extension

The `coordinator_do_name` column was added in Milestone 3 and populated by `prepare()` in Milestone 4. This milestone wires the alarm that reads it.

Extend `alarm()` in `do-partition.ts` to run after the existing split/migration handling:

```typescript
const STALE_THRESHOLD_MS = 5_000;
const staleTxRows = this.ctx.storage.sql
	.exec<{ transaction_id: string; coordinator_do_name: string }>(
		`SELECT DISTINCT transaction_id, coordinator_do_name
     FROM pending_transactions WHERE created_at < ? LIMIT 10`,
		Date.now() - STALE_THRESHOLD_MS,
	)
	.toArray();

for (const row of staleTxRows) {
	if (!row.coordinator_do_name) continue;
	try {
		const tcId = this.env.TRANSACTION_COORDINATOR_DO.idFromName(row.coordinator_do_name);
		await this.env.TRANSACTION_COORDINATOR_DO.get(tcId).recoverTransaction();
	} catch (e) {
		console.error({
			message: "fokos/partition: failed to poke stale TC",
			transactionId: row.transaction_id,
			error: String(e),
		});
	}
}

// Re-arm alarm if any pending transactions still exist.
const remaining =
	this.ctx.storage.sql.exec<{ count: number }>(`SELECT COUNT(*) as count FROM pending_transactions`).toArray()[0]?.count ?? 0;
if (remaining > 0 && !(await this.ctx.storage.getAlarm())) {
	await this.ctx.storage.setAlarm(Date.now() + STALE_THRESHOLD_MS);
}
```

### 7b. Schedule alarm on incoming `prepare`

At the end of a successful `prepare()` (after inserting pending rows), schedule an alarm if none is set:

```typescript
if (!(await this.ctx.storage.getAlarm())) {
	await this.ctx.storage.setAlarm(Date.now() + STALE_THRESHOLD_MS);
}
```

**TODO checklist:**

- [x] Add alarm extension to `PartitionDO` (calls `tcStub.recoverTransaction()` for stale pending rows)
- [x] Schedule alarm in `prepare()` after successful accept
- [ ] Write test: pending transaction older than 60 s triggers `recoverTransaction()` call on the TC

---

## Milestone 8 — FokosDB Client API

**Goal:** Expose `transactWriteItems` and `transactGetItems` on `FokosDB` as the public entry point.

### 8a. Extend `FokosDB` in `src/lib/db.ts`

```typescript
export type FokosDBOptions = {
	ns: DurableObjectNamespace<PartitionDO>;
	topology: PartitionTopologyRouter;
	tcNs: DurableObjectNamespace<TransactionCoordinatorDO>; // NEW
};
```

```typescript
async transactWriteItems(opts: {
  operations: Array<{
    hashKey: string;
    sortKey?: string;
    operation: "put" | "delete" | "check";
    data?: Uint8Array | string;
    conditions?: ItemCondition[];
  }>;
  clientRequestToken?: string;
}): Promise<InitiateWriteResponse> {
  // 1. Resolve partitionDoName for each operation
  const operations: TCWriteOperation[] = opts.operations.map(op => {
    const { partitionContext } = this.options.topology.pickPartition(op.hashKey, op.sortKey);
    return { ...op, partitionDoName: partitionContext.doName };
  });

  // 2. Validate: <= 100 items, no duplicate keys, total payload <= 4 MB
  validateTransactWriteItems(operations);

  // 3. Derive idempotencyToken and route to TC DO
  const idempotencyToken = opts.clientRequestToken ?? crypto.randomUUID().replaceAll("-", "");
  const tcStub = this.options.tcNs.get(this.options.tcNs.idFromName(idempotencyToken));
  return await tcStub.initiateWrite({ clientRequestToken: opts.clientRequestToken, operations });
}

async transactGetItems(opts: {
  items: Array<{ hashKey: string; sortKey?: string }>;
}): Promise<InitiateReadResponse> {
  // 1. Resolve partitionDoName for each item
  const items: TCReadItem[] = opts.items.map(item => {
    const { partitionContext } = this.options.topology.pickPartition(item.hashKey, item.sortKey);
    return { ...item, partitionDoName: partitionContext.doName };
  });

  // 2. Read-only TCs are ephemeral — random UUID DO name, no idempotency
  const tcStub = this.options.tcNs.get(
    this.options.tcNs.idFromName(crypto.randomUUID().replaceAll("-", ""))
  );
  return await tcStub.initiateRead({ items });
}
```

### 8b. Validation helper

```typescript
function validateTransactWriteItems(ops: TCWriteOperation[]): void {
	if (ops.length > 100) throw new Error("TransactWriteItems: max 100 items");
	let totalBytes = 0;
	const seen = new Set<string>();
	for (const op of ops) {
		const key = `${op.hashKey}\0${op.sortKey ?? ""}`;
		if (seen.has(key)) throw new Error(`TransactWriteItems: duplicate key (${op.hashKey}, ${op.sortKey ?? ""})`);
		seen.add(key);
		if (op.data) totalBytes += typeof op.data === "string" ? op.data.length * 2 : op.data.byteLength;
	}
	if (totalBytes > 4 * 1024 * 1024) throw new Error("TransactWriteItems: total payload exceeds 4 MB");
}
```

**TODO checklist:**

- [x] Add `transactionCoordinatorNs` to `FokosDBOptions`
- [x] Implement `transactWriteItems` with validation and TC call
- [x] Implement `transactGetItems` with ephemeral TC DO
- [ ] Update `src/index.ts` worker entrypoint to pass `transactionCoordinatorNs` when constructing `FokosDB`

---

## Milestone 9 — Tests

**Goal:** A comprehensive test suite that validates all correctness invariants stated in the spec.

Start with end-to-end integration tests first to get the happy path working, then add fine-grained unit tests for each component.

### 9a. Integration tests — end-to-end (new `transactions.test.ts`) ← **Start here**

These tests exercise the full stack: `FokosDB` → `TransactionCoordinatorDO` → `PartitionDO`.

- **Happy path write**: `transactWriteItems` across two partitions commits; both items are readable after
- **Happy path read**: `transactGetItems` across two partitions returns consistent snapshot
- **Serializability**: two concurrent `TransactWriteItems` on overlapping items — one wins, one gets `timestamp_conflict`; combined effect is as if they ran serially
- **Read consistency**: `TransactGetItems` concurrent with `TransactWriteItems` — sees either full pre-tx state or full post-tx state, never a mix
- **Durability after TC crash**: TC crashes (simulated) after `PREPARED` is written; retry with same `clientRequestToken` produces `COMMITTED`
- **Non-transactional isolation**: single-item `putItem` on an item with no pending tx proceeds; single-item `putItem` on a locked item is rejected
- **Idempotency window**: retrying `transactWriteItems` with same `clientRequestToken` within 10 minutes returns same outcome
- **Condition check**: `TransactWriteItems` with a `check` operation on a non-existent item is cancelled with `condition_failed`

### 9b. PartitionDO unit tests (extend `do-partition.test.ts`)

- `prepare` — happy path: returns accepted, inserts pending row with `coordinator_do_name`
- `prepare` — condition_failed: `item_exists` on non-existent item
- `prepare` — timestamp_conflict: item's `last_transaction_ts` >= transaction ts
- `prepare` — pending_conflict: another transaction holds the lock; returns `conflictingTransactionId`
- `prepare` — clock_skew: transaction ts more than 5 s in the future
- `prepare` — idempotent re-prepare: same `idempotencyToken` returns accepted without re-running checks
- `commit` — put: item written, `last_transaction_ts = transactionTimestamp`, pending row deleted
- `commit` — delete: item deleted, `deletion_metadata.max_deleted_ts` updated, pending row deleted
- `commit` — check-only: `last_transaction_ts` updated, no data change, pending row deleted
- `commit` — idempotent re-commit: calling twice is safe
- `cancel` — clears pending rows, items untouched
- `cancel` — idempotent: safe on already-cancelled transaction
- `readForTransaction` — item with pending write: `hasPendingWrite=true`
- `readForTransaction` — clean item: `hasPendingWrite=false`, correct `lastCommittedTs`
- Non-transactional `putItem` with no pending tx: proceeds, stamps `last_transaction_ts`
- Non-transactional `putItem` with pending tx: rejected with conflict error

### 9c. TC unit tests (new `do-transaction-coordinator.test.ts`)

- State transitions: CREATED → PREPARING → PREPARED → COMMITTING → COMMITTED
- State transitions: PREPARING → CANCELLING → CANCELLED (one partition rejects)
- Idempotent: `initiateWrite` twice with same `clientRequestToken` returns same result; state only traversed once
- Crash after PREPARED: alarm recovery re-drives commit to completion; same token returns COMMITTED
- Crash in COMMITTING: alarm recovery retries commit for unconfirmed participants
- `initiateRead`: consistent two reads returns items; pending write aborts; changed ts between reads aborts
- `rejection_reason_json` stored in `tc_state` and returned on idempotent re-read of a CANCELLED transaction

**TODO checklist:**

- [ ] Write integration tests (`transactions.test.ts`) — 9a
- [ ] Write PartitionDO unit tests — 9b
- [ ] Write TC unit tests — 9c
- [ ] All pre-existing tests must still pass

---

## Implementation Order

```
M1 (Types)
  → M2 (Wrangler Config — verify bindings early)
    → M3 (PartitionDO Schema + non-tx write gate) + regression tests
      → M4 (PartitionDO RPC Methods) + unit tests
        → M5 (Split Migration Extension)
          → M6 (TransactionCoordinatorDO)
            → M7 (Recovery Alarm)
              → M8 (FokosDB Client API)
                → M9 (Tests — integration first, then unit)
```

M1, M2, M5 are quick (~30–60 min each). M4 and M6 are the meaty milestones.

---

## Key Invariants to Validate in Every PR

1. `items` table always holds committed state only — no shadow/pending columns.
2. Non-transactional `getItem` never reads from `pending_transactions`.
3. Non-transactional `putItem`/`deleteItem` is **rejected** (not silently skipped) when a pending transaction holds the item's lock.
4. Every TC state transition writes the new state to SQLite **before** any outbound RPC for that state.
5. `prepare`, `commit`, and `cancel` are idempotent (safe for concurrent recovery runs).
6. TC in `PREPARED` state MUST eventually commit — it must never transition to CANCELLING from PREPARED.
7. Rejection reason is stored in `tc_state.rejection_reason_json` — never in a separate KV entry.

---

## Open Questions / Decisions Left to Agent

- **Timestamp tie-breaking:** For now use `Date.now()` alone. Leave a `// TODO: append shard suffix for multi-TC pooling` comment in `initiateWrite`.
- **`TransactGetItems` TC lifecycle:** Read-only TCs are ephemeral (random UUID name, no idempotency). They can expire without cleanup.
- **`ctx.waitUntil` for background commit:** Use `this.ctx.waitUntil(runCommit(...))` in `initiateWrite` so the commit continues after the RPC returns. Ensure the DO stays alive for the background task.
- **Non-transactional write rejection UX:** The thrown error should carry the `transactionId` of the conflicting transaction so the caller can log it. Revisit error format when implementing the ATC §4 optimization (FIXME noted in M3).

---

## Appendix A — Post-Implementation Fixes

Fixes identified during post-implementation review and applied to the codebase.

- [x] **Fix 1 — Commit invariant check.** `PartitionDO.commitLocal` now asserts a 1:1 mapping between request items and `pending_transactions` rows (count equality + every request key exists in the pending set) before applying writes.
- [x] **Fix 2 — Client-side put-data validation.** `validateTransactWriteItems` in `db.ts` rejects `put` operations with missing `data` before the request reaches the TC.
- [x] **Fix 3 — Bounded commit retries.** `runCommit` retry predicate changed from `() => true` to `(_err, nextAttempt) => nextAttempt < 10`.
- [x] **Fix 4 — Missing tc_state index.** Added `CREATE INDEX IF NOT EXISTS tc_state_transaction_id ON tc_state (transaction_id)` to the TC migration, since `recoverTransaction` queries by `transaction_id` but the PK is `idempotency_token`.
- [x] **Fix 5 — State guards on UPDATE statements.** All `UPDATE tc_state SET state = ...` statements now include `AND state = '...'` (or `AND state IN (...)`) guards to prevent stale/duplicate transitions.
- [x] **Fix 6 — Transaction split-forwarding.** `prepare`, `commit`, `cancel`, and `readForTransaction` in PartitionDO now forward to child partitions during splits, matching the existing behavior of `putItem`/`deleteItem`. When splitting, all items must route to children — local and forwarded paths are mutually exclusive (enforced by invariant).

---

## Appendix B — Deviations from Plan (Improvements)

Intentional deviations from the original plan that improve the design.

1. **Inline idempotency in `tc_state`.** The plan called for a separate `tc_idempotency` table. The implementation stores the idempotency token as the PK of `tc_state` directly, eliminating a join and a table.
2. **`ctx.waitUntil` for background commit.** The plan did not specify the mechanism for continuing commit after the RPC returns. The implementation uses `ctx.waitUntil(runCommit(...))` to keep the DO alive during background commit work.
3. **`recoverTransaction` inlined into TC.** The plan suggested a separate recovery path. The implementation reuses the same `runPrepare`/`runCommit` methods with state-guard UPDATEs, making recovery and the happy path share code.
4. **Alarm-based TC recovery.** The plan mentioned recovery but not the trigger mechanism. The implementation sets a DO alarm as a fallback that fires `recoverTransaction` if the TC stalls.
