# Correctness Bugs: Split/Transaction Boundary Fixes

## Background

Six correctness issues were found across the write/read flows, the 2PC transaction protocol,
and the partition split + migration process. Bugs 1–5 are ordered by severity; Bug 6 is a
later-discovered lock leak (same severity class as Bug 2) reached through a migration race.

---

## Bug 1 — DATA LOSS: commit forwarded to children that have no pending record (CRITICAL)

### Root cause

When a transaction is **prepared** on a parent partition while it is in `split_queued`
(`prepareLocal` runs, items are locked in the parent's `pending_transactions`), a background
job may start the split and transition the parent to `split_started` before the **commit**
arrives. When commit arrives, `groupItemsByRouting` sees `split_started` / `split_completed`
and routes all items to children. Children were just created and are in `migration_migrating` —
they have no `pending_transactions` row for this transaction. `commitLocal` returns
`{outcome:"committed"}` as a silent no-op. The parent's forwarded-commit path then deletes its
own rows. TC sees "committed" from all participants and marks the transaction COMMITTED.
**The items are never written anywhere.**

The forwarded-commit DELETE is also the mechanism that prevents recovery: once the parent's
rows are deleted, `getPartitionTransactionMetadata` called by migrating children can no longer
find them, so migration will not transfer the pending records to the children either.

### Fix — Option C: reject commit during migration, depend on TC recovery

Add `await this.ensureMigration("commit")` at the top of `commit()` in `PartitionDO`, mirroring
the existing guards on `putItem` and `deleteItem`. This causes the child to throw a 503-style
error when it is still in `migration_migrating`.

Recovery chain:
1. Child throws → parent's `await this.getChildStub(...).commit(...)` propagates the error
   **before** the `DELETE FROM pending_transactions` line, so the parent keeps its rows.
2. TC's `tryWhile` retries up to 10 times, then `Promise.allSettled` catches the failure.
   `commit_outcome` stays `NULL`. TC remains in `COMMITTING`.
3. Meanwhile, the child's `runMigration` calls `getPartitionTransactionMetadata` on the parent.
   The parent still has the rows → child migrates the pending records.
4. Child transitions to `migration_completed`.
5. TC alarm fires (5 s) → `runCommit` → sends commit to parent → parent forwards to child →
   child's `ensureMigration` returns false (migration complete) → `commitLocal` finds
   `pendingCount > 0` (migrated in step 3) → applies write → deletes child's pending row.
6. Parent deletes its own pending rows. TC marks COMMITTED. ✓

The parent's stale-TX alarm also acts as a secondary trigger: it calls `recoverTransaction` on
the TC, which re-drives `runCommit` if still in COMMITTING.

### Client-visible behavior change

This is not purely internal. Before the fix, a commit forwarded to a migrating child silently
"succeeds" (the no-op) and the client gets `committed` immediately — with data loss. After the
fix, the in-line `runCommit` invoked from `drivePrepare` fails against the migrating child,
`loadFinalResponse` sees state `COMMITTING` (not `COMMITTED`) and throws
`"transaction … still in progress, retry later"`. The client must retry with the same
idempotency token; `resumeTransaction` re-drives `runCommit`, and once migration has completed
the retry commits. This is the correct trade-off (a transient error instead of a misleading
success), but any test or client expecting an immediate `committed` must be updated to retry.

### File / lines

`do-partition.ts` — `commit()` (~line 637):

```typescript
async commit(pCtx: PartitionContextResolved, request: CommitRequest): Promise<CommitResponse> {
    this.ensurePartitionContext(pCtx);
    await this.ensureMigration("commit");   // ← ADD THIS LINE

    const { local, forwarded } = this.groupItemsByRouting(request.items);
    ...
}
```

No other changes needed. `ensureMigration` already handles all three migration states
correctly and is safe to call on a non-migrating (parent) partition — it returns immediately
when status is absent or `migration_completed`.

---

## Bug 2 — LOCK LEAK: cancel not forwarded when parent is `split_completed` (IMPORTANT)

### Root cause

`cancel()` in `PartitionDO` forwards the cancel to children only when
`splitStatus.status === "split_started"`. Once the last child acknowledges migration and the
parent transitions to `split_completed`, subsequent cancel calls skip the forwarding block.

Any transaction that was **prepared on children** (because the parent was in `split_started`
during prepare and forwarded the prepare to children) and is then cancelled **after
`split_completed`** will have its cancel swallowed by the parent. Children retain their
`pending_transactions` rows permanently, locking those items for all future non-transactional
writes.

The TC marks the transaction as CANCELLED (because the parent returned `{outcome:"cancelled"}`
after its own — empty — local delete). Children's stale-TX recovery calls `recoverTransaction`;
the TC is already CANCELLED and returns a no-op. Children are never cleaned up.

### Fix

Extend the forwarding condition to include `split_completed`:

```typescript
// do-partition.ts — cancel() (~line 742)
if (
    splitStatus?.status === "split_started" ||
    splitStatus?.status === "split_completed"   // ← ADD THIS
) {
    for (const childPCtx of splitStatus.childPartitionContexts) {
        try {
            await this.getChildStub(childPCtx).cancel(childPCtx, request);
        } catch (e) {
            console.error({ ... });
        }
    }
}
```

`PartitionDO.cancel` already deletes its own rows first (before the forwarding block), so this
change only adds the forwarding step — no atomicity risk. The child's `cancel` is a plain
DELETE which is idempotent: if the child never received the prepare (e.g., it was prepared
before the split), the DELETE is a no-op, which is safe.

> **Note:** Bug 6 modifies this same forwarding block (adds `ensureMigration` and replaces the
> error-swallowing `catch` with error propagation). Apply Bug 2 and Bug 6 together; the final
> combined form of `cancel()` is shown in the Bug 6 section.

---

## Bug 3 — Parent pending_transactions never proactively cleaned up at `split_completed` (IMPORTANT)

### Root cause

At `split_completed`, all children have completed `runMigration`, which includes pulling
`pending_transactions` from the parent via `getPartitionTransactionMetadata`. The parent's
copies are now redundant: children own all pending records. However, the parent never
proactively deletes them.

The intended cleanup path (parent's forwarded-commit DELETE after all children commit) is
reactive and depends on the TC actively retrying. There is a scenario where the TC reaches a
terminal state (COMMITTED) without the parent having executed that DELETE — for example, a
parent crash between returning `{outcome:"committed"}` and committing its own SQL DELETE.
On restart, the parent has orphaned rows. The stale-TX alarm calls `recoverTransaction`, but
the TC is already COMMITTED and returns a no-op. The rows are never deleted.

### Fix

In `PartitionDO.acknowledgeChildMigrationComplete`, after the topology transition, check
whether the status became `split_completed` and delete all `pending_transactions` from the
parent:

```typescript
// do-partition.ts — acknowledgeChildMigrationComplete (~line 494)
async acknowledgeChildMigrationComplete(childDoName: string): Promise<void> {
    const topology = this.ensureTopology(this.pCtx());
    // Wrap the topology transition (a KV write) and the cleanup DELETE in one atomic
    // write so a crash can never leave the parent in split_completed with orphaned rows.
    this.ctx.storage.transactionSync(() => {
        topology.acknowledgeChildMigration(childDoName);

        // Once ALL children have migrated, their copies of pending_transactions are
        // authoritative. Delete the parent's (now redundant) copies so they cannot
        // confuse stale-TX recovery or linger after a crash.
        if (topology.splitStatus()?.status === "split_completed") {
            this.ctx.storage.sql.exec(`DELETE FROM pending_transactions`);
        }
    });
}
```

`acknowledgeChildMigration` writes the split status via `this.#storage.kv.put`. There is no
`await` between it and the `DELETE`, so DO write-coalescing very likely already commits them
atomically; the explicit `transactionSync` makes that guarantee unconditional and matches how
the rest of this file groups multi-write invariants (e.g. `prepareLocal`, `commitLocal`,
`runMigration`).

**Why this is safe:**
- Post-split prepares are always forwarded to children; the parent never calls `prepareLocal`
  again. So every row in the parent's table at `split_completed` is a pre-split row that
  children already hold.
- Subsequent commit forwarding still works: children have the rows, apply the write, parent's
  transaction-scoped DELETE in the forwarded path is a no-op. ✓
- Subsequent cancel forwarding (with Bug 2 fix) still works: children delete their rows,
  parent's DELETE is a no-op. ✓
- This call is idempotent. If called again after `split_completed` (e.g., duplicate ack from a
  child), the delete runs on an already-empty table. ✓
- The stale-TX alarm on the parent will find no rows and skip TC pings. ✓

---

## Bug 4 — Non-transactional `deleteItem` does not update `deletion_metadata` (MODERATE)

### Root cause

`applyCommitItems` (transactional delete) updates `deletion_metadata.max_deleted_ts` so that
future transactions with earlier timestamps are rejected (`prepareLocal` checks this watermark
when an item is absent). Non-transactional `deleteItem` omits this update, so a transaction
with a timestamp assigned between the non-transactional delete and the previous
`max_deleted_ts` can successfully prepare and commit a `put` for the deleted key, silently
ignoring the intervening delete.

### Fix

`do-partition.ts` — `deleteItem` local path (~line 339), after the `DELETE FROM items` exec:

```typescript
const writeRes = this.ctx.storage.sql.exec(
    `DELETE FROM items WHERE hk = ? AND sk = ?`,
    opts.hashKey,
    sk,
);
// ← ADD: keep deletion watermark consistent with transactional deletes
if (writeRes.rowsWritten > 0) {
    this.ctx.storage.sql.exec(
        `UPDATE deletion_metadata SET max_deleted_ts = MAX(max_deleted_ts, ?) WHERE id = 1`,
        Date.now(),
    );
}
```

Only update when a row was actually deleted (`rowsWritten > 0`) to avoid bumping the watermark
on a no-op delete.

`Date.now()` is the correct watermark basis: the non-transactional `putItem` path already uses
`Date.now()` for `last_transaction_ts`, and TC transaction timestamps are `Date.now()`-derived,
so the units are consistent. Like the transactional delete in `applyCommitItems`, this is
conservative (a partition-wide watermark may over-reject older transactions), which is safe.

**Minor:** the extra `deletion_metadata` UPDATE is not reflected in the returned
`meta.rowsWritten`. This is cosmetic (metrics under-count by one write) and not worth extra
plumbing.

---

## Bug 5 — `check` operation on non-existent items skips `deletion_metadata` check (MINOR)

### Root cause

In `prepareLocal`, the `max_deleted_ts` watermark is only checked for `put` and `delete`
operations on non-existent items:

```typescript
} else if (item.operation === "put" || item.operation === "delete") {
    if (request.transactionTimestamp <= (metaRow?.max_deleted_ts ?? 0)) { ... }
}
```

A `check` operation on a non-existent item (i.e., one previously deleted by a transaction with
a higher timestamp) passes unconditionally. On commit, the corresponding
`UPDATE items SET last_transaction_ts = ...` is a no-op. The transaction commits believing it
validly observed the item as non-existent, even though a newer transaction already changed the
item's state.

### Fix

`do-partition.ts` — `prepareLocal` (~line 595): extend the condition to cover `check`:

```typescript
} else if (
    item.operation === "put" ||
    item.operation === "delete" ||
    item.operation === "check"   // ← ADD
) {
    const metaRow = this.ctx.storage.sql
        .exec<{ max_deleted_ts: number }>(
            `SELECT max_deleted_ts FROM deletion_metadata WHERE id = 1`,
        )
        .toArray()[0];
    if (request.transactionTimestamp <= (metaRow?.max_deleted_ts ?? 0)) {
        return {
            outcome: "rejected",
            reason: { type: "timestamp_conflict", hashKey: item.hashKey, sortKey: item.sortKey },
        };
    }
}
```

Note: `max_deleted_ts` is a partition-wide watermark, not per-item. This check is therefore
conservative (may over-reject). For `put`/`delete` this was already accepted; `check` gets the
same treatment.

---

## Bug 6 — LOCK LEAK: cancel races with migration's pending-tx copy (IMPORTANT)

### Root cause

`runMigration` copies the parent's `pending_transactions` in two steps separated by an `await`:
it reads a batch from the parent via `getPartitionTransactionMetadata` (a remote RPC), then
inserts the rows locally. Workers RPCs that await on remote I/O are **interleaving points** — an
incoming `cancel` RPC can run on the child in between, while the migration is suspended on the
read.

Sequence that leaks a lock:
1. The child's migration reads a batch containing tx `T`'s pending row from the parent
   (suspended on the `getPartitionTransactionMetadata` RPC).
2. A `cancel(T)` arrives at the parent. `cancel` deletes the parent's own rows for `T` first,
   then forwards the cancel to the child.
3. The child's `cancel` runs `DELETE WHERE transaction_id = T` — but the row has not been
   inserted yet, so it is a no-op. The child returns `{outcome:"cancelled"}`.
4. The child's migration resumes and inserts `T`'s row from the batch read in step 1. The row
   is now **orphaned** on the child (its `created_at` is copied from the parent, so it is
   already old).
5. The parent returns `{outcome:"cancelled"}`; the TC marks `T` CANCELLED (terminal).
6. The child's stale-TX recovery pokes the TC, but `T` is already CANCELLED → `recoverTransaction`
   is a no-op. The orphaned row locks that item against all future writes, permanently.

This is the same class of permanent lock leak as Bug 2, reached by a different path.

### Fix — reject cancel during migration + propagate forwarding failures

Mirror Bug 1's reject-and-recover approach. Two coordinated changes to `cancel()`, **both
required**:

1. Add `await this.ensureMigration("cancel")` at the top, so a child that is still
   `migration_migrating` *rejects* the forwarded cancel instead of running a premature no-op
   DELETE.
2. Stop swallowing child-cancel errors in the forwarding loop. Collect failures, attempt every
   child, then rethrow if any failed — so the failure reaches the TC.

```typescript
// do-partition.ts — cancel() (~line 736). Shown with the Bug 2 split_completed change folded in.
async cancel(pCtx: PartitionContextResolved, request: CancelRequest): Promise<CancelResponse> {
    this.ensurePartitionContext(pCtx);
    await this.ensureMigration("cancel");   // ← Bug 6: reject while THIS partition is migrating
    this.ctx.storage.sql.exec(`DELETE FROM pending_transactions WHERE transaction_id = ?`, request.transactionId);

    const topology = this.ensureTopology(pCtx);
    const splitStatus = topology.splitStatus();
    if (
        splitStatus?.status === "split_started" ||
        splitStatus?.status === "split_completed"   // ← Bug 2
    ) {
        const errors: unknown[] = [];
        for (const childPCtx of splitStatus.childPartitionContexts) {
            try {
                await this.getChildStub(childPCtx).cancel(childPCtx, request);
            } catch (e) {
                // ← Bug 6: do NOT swallow. A migrating child throws here; we must surface it so
                // the TC keeps retrying (stays CANCELLING) until migration completes and the
                // forwarded cancel can delete the row the child copied during the race.
                console.error({
                    ...this.logParams(),
                    message: "fokos/partition.cancel: failed to forward cancel to child",
                    childDoName: childPCtx.doName,
                    transactionId: request.transactionId,
                    error: String(e),
                });
                errors.push(e);
            }
        }
        if (errors.length > 0) {
            throw new Error(`fokos/partition.cancel: ${errors.length} child cancel(s) failed for transaction ${request.transactionId}`);
        }
    }

    return { outcome: "cancelled" };
}
```

Collecting-then-throwing (rather than throwing on the first failure, as `commit` forwarding
does) lets a single pass release locks on every reachable child while still signalling the TC to
retry the ones that are still migrating.

### Recovery chain

1. The child rejects the forwarded cancel while `migration_migrating` → the parent rethrows.
2. TC's `runCancel` records no `cancel_outcome`, leaves the transaction in `CANCELLING`, and the
   TC alarm reschedules (CANCELLING is non-terminal).
3. The child finishes migration — which may insert the orphaned row from the race — and
   transitions to `migration_completed`.
4. The TC retries the cancel. Two independent triggers: the TC alarm, and the child's own
   stale-TX poke (the orphaned row's `created_at` is already stale, so it pokes immediately).
5. The parent re-forwards the cancel; the child is now `migration_completed`, `ensureMigration`
   returns false, and the `DELETE` removes the orphaned row. ✓
6. All children confirm → TC marks CANCELLED. ✓

### Why both changes are required

- **`ensureMigration` alone is not enough:** if the parent keeps swallowing the child's error it
  still returns `{outcome:"cancelled"}`, the TC marks `T` CANCELLED, recovery stops, and the
  orphan persists. The error must propagate.
- **Propagation alone is not enough:** without `ensureMigration` the child's no-op DELETE
  "succeeds" before the row is inserted, so there is no error to propagate in the first place.

### Dependency

Depends on Bug 2: the recovery retry in step 5 only reaches the child if cancel is forwarded at
`split_completed` (by the time the retry lands, the parent has usually transitioned).

---

## Implementation order

| Step | Bug | Why this order |
|------|-----|----------------|
| 1 | Bug 2 (cancel forwarding at split_completed) | Self-contained; prerequisite for both Bug 1 and Bug 6 recovery reaching children |
| 2 | Bug 1 (ensureMigration in commit) | Core data-loss fix; relies on Bug 2 so commits/cancels during migration reach children |
| 3 | Bug 6 (reject cancel during migration + propagate) | Same reject-and-recover pattern as Bug 1; relies on Bug 2's split_completed forwarding for cleanup. Modifies the same `cancel()` block as Bug 2 — apply together |
| 4 | Bug 3 (proactive cleanup at split_completed) | Builds on Bugs 1 & 6: with commit/cancel well-defined during migration, parent cleanup timing is well-defined |
| 5 | Bug 4 (deleteItem + deletion_metadata) | Independent of split logic |
| 6 | Bug 5 (check + max_deleted_ts) | Independent, lowest risk |

## Testing notes

- **Bug 1**: The existing `__testing__beforeMigrationComplete` hook can be used to inject a
  commit call between migration data copy and `migration_completed` status write, verifying
  that the commit is rejected and the data is correctly applied after migration finishes.
  Also assert the client-visible change: the in-line commit attempt surfaces a transient
  "retry later" (not a silent `committed`), and a retry with the same idempotency token resolves
  to `committed` once migration completes.
- **Bug 2**: Test a cancel that arrives after the last child acks migration (transitioning
  parent to `split_completed`). Verify the child's `pending_transactions` are empty afterward.
- **Bug 3**: After all children ack, verify parent's `pending_transactions` table is empty
  immediately, without waiting for a TC retry.
- **Bug 4 & 5**: Unit-test `prepareLocal` directly: delete an item non-transactionally, then
  try to prepare a transaction with an earlier timestamp targeting the same key.
- **Bug 6**: Use `__testing__beforeMigrationComplete` to deliver a `cancel` for an in-flight
  transaction between the pending-tx copy and the `migration_completed` write (i.e. while the
  child is still `migration_migrating`). Verify: (a) the child rejects the forwarded cancel and
  the parent propagates the failure; (b) the transaction stays `CANCELLING`; (c) after migration
  completes and the TC retries, the child's `pending_transactions` are empty and the item is no
  longer locked (a subsequent non-transactional write succeeds).
