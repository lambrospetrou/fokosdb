# Alarm Optimization: setTimeout Fast Paths + ensureAlarmSet

## Goal

Reduce latency for background jobs (migration, split) by firing them immediately via `setTimeout`
rather than waiting for the Alarm API (which has ~1–2 s scheduling overhead). The alarm becomes a
durable fallback that fires if the DO is evicted before the timeout runs.

Two structural fixes travel alongside: a helper that sets the alarm only when it would fire
_earlier_ than the current scheduled time, and a deduplication guard so we never hold more than one
logically-pending background-work timeout at a time.

---

## Files

- `src/lib/do-partition.ts` — primary changes
- `src/lib/partition-topology/partition-topology.ts` — remove alarm from `queueSplit`, fix
  `startSplit` idempotency

---

## Background: current alarm call sites

| Site                                | Call                                       | Purpose                                                    |
| ----------------------------------- | ------------------------------------------ | ---------------------------------------------------------- |
| `ensureMigration` (line 933)        | `setAlarm(Date.now())`                     | Kick off migration after any write during migration status |
| `prepareLocal` (line 601)           | `setAlarm(Date.now() + 5_000)` if no alarm | Arm stale-TX recovery loop                                 |
| `alarm()` re-arm (line 863)         | `setAlarm(Date.now() + 5_000)` if no alarm | Keep alarm alive while pending TXs exist                   |
| `queueSplit` in topology (line 567) | `setAlarm(Date.now())` if no alarm         | Start split process                                        |

**Common bug across all four**: they only check _whether_ an alarm exists, not _when_ it fires.
If an alarm is already set for T+30 s but we need one at T+0, the earlier request is silently
dropped. The `ensureAlarmSet` helper (Step 1) fixes this.

---

## Step 1 — Add `ensureAlarmSet(targetMs: number)`

Private method on `PartitionDO`. Replaces all four inline `getAlarm()` + `setAlarm()` patterns.

```typescript
private async ensureAlarmSet(targetMs: number): Promise<void> {
    const existing = await this.ctx.storage.getAlarm();
    if (existing === null || targetMs < existing) {
        await this.ctx.storage.setAlarm(targetMs);
    }
}
```

No behavioral change yet — just establishes the primitive every subsequent step uses.

**Sites updated in this step**:

- `ensureMigration`: `setAlarm(Date.now())` → `ensureAlarmSet(Date.now())`
- `prepareLocal`: remove the `if (!(await getAlarm()))` guard, call `ensureAlarmSet(Date.now() + STALE_TX_MS)` unconditionally on accepted prepare
- `alarm()` re-arm: keep wiring for now; will be rewritten in Step 3
- `queueSplit` in topology: **remove its `setAlarm` call entirely** (see Step 8) — the DO owns alarm scheduling

---

## Step 2 — Add `scheduleBackgroundWork(delayMs)` with deduplication

New private field + private method on `PartitionDO`.

**Field**:

```typescript
#_backgroundWorkScheduledAt: number | null = null;
```

**Method**:

```typescript
private scheduleBackgroundWork(delayMs = 10): void {
    const targetTime = Date.now() + delayMs;
    // Only create a new setTimeout if no pending one exists, or this one fires earlier.
    if (this.#_backgroundWorkScheduledAt !== null && this.#_backgroundWorkScheduledAt <= targetTime) {
        return;
    }
    this.#_backgroundWorkScheduledAt = targetTime;
    setTimeout(() => {
        this.#_backgroundWorkScheduledAt = null;
        void this.runBackgroundWork();
    }, delayMs);
}
```

**Invariant**: at most one logically-pending background-work timeout at any time. A new one is
scheduled only if it would fire strictly earlier than the currently tracked one. This prevents
accumulating hundreds of timers under write bursts.

DO hibernation note: Cloudflare DOs hibernate after ~10 s of inactivity, but with a setTimeout set they can live up to 1-2 minutes.
`setTimeout` callbacks only run while the DO is alive. We therefore never pass `delayMs` values large enough to cross the
hibernation boundary — that range belongs entirely to the alarm. Typical values here: 0–50 ms for
migration/split fast paths.

---

## Step 3 — Extract `runBackgroundWork()` shared handler

New private async method. Both `alarm()` and `setTimeout` callbacks delegate here. It always reads
KV as the authoritative source of truth, so duplicate invocations are safe.

Each job declares its next-needed alarm time via a local `wantAlarm(ms)` accumulator. After all
jobs, `ensureAlarmSet` is called with the minimum collected value (if any).

Two structural rules for each job:

1. **Own try/catch** — a failure in one job must not prevent the others from running. Jobs are
   independent: stale-TX recovery should always run even if migration or split is broken.
2. **Post-condition `wantAlarm`** — after running (success _or_ caught failure), each job re-reads
   its status from KV. If work is still incomplete, it calls `wantAlarm` to schedule a retry alarm.
   This guarantees that a DO with no incoming traffic still retries failed jobs without relying on
   the next write request to re-arm the alarm.

```typescript
private static readonly STALE_TX_MS = 5_000;
private static readonly MIGRATION_FALLBACK_ALARM_MS = 10_000;
private static readonly SPLIT_FALLBACK_ALARM_MS = 5_000;

private async runBackgroundWork(): Promise<void> {
    let nextAlarmMs: number | null = null;
    const wantAlarm = (ms: number) => {
        if (nextAlarmMs === null || ms < nextAlarmMs) nextAlarmMs = ms;
    };

    // ── Job 1: Partition migration ────────────────────────────────────────────
    try {
        const migrationStatus = this.ctx.storage.kv.get<PartitionSplitMigrationStatus>(
            PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS,
        );
        if (migrationStatus === "migration_initialized" || migrationStatus === "migration_migrating") {
            if (migrationStatus === "migration_initialized") {
                this.ctx.storage.kv.put<PartitionSplitMigrationStatus>(
                    PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_migrating",
                );
            }
            await tryWhile(
                async () => { await this.runMigration(); },
                (_error, nextAttempt) => nextAttempt <= 5,
            );
        }
    } catch (error) {
        console.error({ ...this.logParams(), message: "fokos/partition: Migration job failed.", error: String(error), errorProps: error });
    } finally {
        // Post-condition: if still migrating (job failed), schedule a retry alarm.
        const postStatus = this.ctx.storage.kv.get<PartitionSplitMigrationStatus>(PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS);
        if (postStatus === "migration_migrating") {
            wantAlarm(Date.now() + PartitionDO.MIGRATION_FALLBACK_ALARM_MS);
        }
    }

    // ── Job 2: Partition split ────────────────────────────────────────────────
    const topology = this.ensureTopology(this.pCtx());
    try {
        const splitStatus = topology.splitStatus();
        if (splitStatus?.status === "split_queued") {
            console.log({ ...this.logParams(), message: "fokos/partition: Running split process.", splitStatus });
            await tryWhile(
                async () => { await topology.startSplit(); },
                (_error, nextAttempt) => nextAttempt <= 5,
            );
        }
    } catch (error) {
        console.error({ ...this.logParams(), message: "fokos/partition: Split job failed.", error: String(error), errorProps: error });
    } finally {
        // Post-condition: if still queued (job failed), schedule a retry alarm.
        if (topology.splitStatus()?.status === "split_queued") {
            wantAlarm(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
        }
    }

    // ── Job 3: Stale transaction recovery ────────────────────────────────────
    try {
        const staleTxRows = this.ctx.storage.sql
            .exec<{ transaction_id: string; coordinator_do_id: string }>(
                `SELECT DISTINCT transaction_id, coordinator_do_id
                 FROM pending_transactions WHERE created_at < ? LIMIT 10`,
                Date.now() - PartitionDO.STALE_TX_MS,
            )
            .toArray();
        for (const row of staleTxRows) {
            if (!row.coordinator_do_id) continue;
            try {
                const tcId = this.env.TRANSACTION_COORDINATOR_DO.idFromString(row.coordinator_do_id);
                await this.env.TRANSACTION_COORDINATOR_DO.get(tcId).recoverTransaction(row.transaction_id);
            } catch (e) {
                console.error({
                    ...this.logParams(),
                    message: "fokos/partition: failed to poke stale TC",
                    transactionId: row.transaction_id,
                    error: String(e),
                });
            }
        }
    } catch (error) {
        console.error({ ...this.logParams(), message: "fokos/partition: Stale TX recovery job failed.", error: String(error), errorProps: error });
    } finally {
        // Re-arm if pending TX locks remain (need future poke cycles).
        const pendingCount =
            this.ctx.storage.sql
                .exec<{ n: number }>(`SELECT COUNT(*) as n FROM pending_transactions`)
                .toArray()[0]?.n ?? 0;
        if (pendingCount > 0) {
            wantAlarm(Date.now() + PartitionDO.STALE_TX_MS);
        }
    }

    // ── Arm alarm to earliest requested time ─────────────────────────────────
    if (nextAlarmMs !== null) {
        await this.ensureAlarmSet(nextAlarmMs);
    }
}
```

---

## Step 4 — Slim down `alarm()`

The alarm handler becomes a thin wrapper. All logic moves to `runBackgroundWork`.

```typescript
async alarm(alarmInfo: AlarmInvocationInfo): Promise<void> {
    console.log({ ...this.logParams(), message: "fokos/partition: Alarm triggered.", alarmInfo });
    this.__testing__alarm_running = true;
    try {
        await this.runBackgroundWork();
    } finally {
        this.__testing__alarm_running = false;
    }
}
```

The old "Alarm fired but no split is queued, nothing to do" log moves implicitly to `runBackgroundWork`
(no jobs trigger, `nextAlarmMs` stays null, no alarm set). Add a log at the end of `runBackgroundWork`
if nothing fired: `"fokos/partition: Background work ran, nothing to do."`.

---

## Step 5 — Fast path in `initFromSplit`

After the three `kv.put` calls that persist migration state, schedule immediate background work and
arm a fallback alarm:

```typescript
// Fallback: alarm fires if the DO is evicted before setTimeout runs.
await this.ensureAlarmSet(Date.now() + PartitionDO.MIGRATION_FALLBACK_ALARM_MS);
// Fast path: begin migration in this request's event loop turn.
this.scheduleBackgroundWork(0);
```

This replaces the current flow where migration only starts when the next write request hits
`ensureMigration` and that request's alarm fires ~1–2 s later.

---

## Step 6 — Update `triggerMigration` and `ensureMigration`

`triggerMigration` currently calls `ensureMigration("triggerMigration", false)` which sets an
immediate alarm. With the new design:

```typescript
async triggerMigration(): Promise<void> {
    const isMigrating = await this.ensureMigration("triggerMigration", false);
    if (isMigrating) {
        this.scheduleBackgroundWork(0);
    }
}
```

`ensureMigration` replaces `setAlarm(Date.now())` with
`ensureAlarmSet(Date.now() + MIGRATION_FALLBACK_ALARM_MS)` — no longer needs to be immediate
because `scheduleBackgroundWork(0)` handles the fast path.

```typescript
private async ensureMigration(op: string, throwIfMigrating = true): Promise<boolean> {
    const migrationStatus = this.ctx.storage.kv.get<PartitionSplitMigrationStatus>(
        PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS,
    );
    if (!migrationStatus || migrationStatus === "migration_completed") return false;
    if (migrationStatus === "migration_initialized") {
        this.ctx.storage.kv.put<PartitionSplitMigrationStatus>(
            PartitionDO.KV_KEYS.SPLIT_MIGRATION_STATUS, "migration_migrating",
        );
    }
    await this.ensureAlarmSet(Date.now() + PartitionDO.MIGRATION_FALLBACK_ALARM_MS);
    if (throwIfMigrating) {
        throw new Error(`fokos/partition:${op}: Partition split in progress, please retry later.`);
    }
    return true;
}
```

---

## Step 7 — Fast path in `checkSplits`

After `maybeQueueSplit` returns a non-null result, schedule the split immediately:

```typescript
private async checkSplits(
    pCtx: PartitionContextResolved, hashKey: string, sortKey?: string,
): Promise<SplitStatusKVItem | undefined> {
    const topologyRouter = this.ensureTopology(pCtx);
    const splitStatus = await topologyRouter.maybeQueueSplit(hashKey, sortKey);
    if (splitStatus) {
        console.log({ ...this.logParams(), message: "fokos/partition: Split conditions met.", splitStatus });
        await this.ensureAlarmSet(Date.now() + PartitionDO.SPLIT_FALLBACK_ALARM_MS);
        this.scheduleBackgroundWork(10);
    }
    return splitStatus;
}
```

---

## Step 8 — Remove alarm from `queueSplit` in `partition-topology.ts`

`queueSplit` currently calls `setAlarm(Date.now())` directly on `this.#storage`. This violates the
principle that the DO owns alarm scheduling — the topology should only manage its own KV state.

Remove the `getAlarm`/`setAlarm` block from `queueSplit` entirely. The comment can note that the
caller (`PartitionDO.checkSplits`) is responsible for scheduling.

```typescript
async queueSplit(splitType: SplitType): Promise<SplitStatusKVItem> {
    const nowStatus = this.splitStatus();
    if (!nowStatus) {
        this.#storage.kv.put<SplitStatusKVItem>(PartitionTopologyImpl.KV_KEYS.SPLIT_STATUS, {
            status: "split_queued",
            splitType,
            createdAt: Date.now(),
            partitionContext: this.partitionContext,
        });
    }
    // Alarm scheduling is the caller's responsibility (PartitionDO.checkSplits).
    const written = this.splitStatus();
    invariant(written != null, "fokos/topology.queueSplit: KV write succeeded but splitStatus() returned null");
    return written;
}
```

---

## Step 9 — Fix `startSplit` idempotency in `partition-topology.ts`

The current opening `invariant` throws if status is not `split_queued`. Both the `setTimeout` fast
path and the alarm fallback can call `startSplit` — whichever fires second will see status
`split_started` and should be a no-op, not an error.

```typescript
async startSplit() {
    const splitStatus = this.splitStatus();
    if (!splitStatus || splitStatus.status !== "split_queued") {
        // Already started or completed — idempotent no-op.
        return;
    }
    // ... rest of existing logic unchanged ...
}
```

`acknowledgeChildMigration` is already idempotent (returns early for `split_completed`) — no change
needed.

---

## Idempotency & race-safety matrix

| Job           | Both paths fire                         | Why safe                                                                         |
| ------------- | --------------------------------------- | -------------------------------------------------------------------------------- |
| Migration     | `runBackgroundWork` called twice        | Job 1 checks `migrationStatus`; second call sees `migration_completed` and skips |
| Split start   | `startSplit` called twice               | Returns early if status ≠ `split_queued` (Step 9)                                |
| Stale TX poke | Poke loop runs twice                    | Time-based SQL filter; poking a TC twice is idempotent                           |
| Alarm re-arm  | `ensureAlarmSet` called from both paths | `getAlarm()` read + conditional write; worst case sets alarm to the earlier time |

Single-threaded guarantee: Cloudflare DOs process one I/O continuation at a time. Two invocations
of `runBackgroundWork` cannot truly overlap — one will complete before the other reads KV state,
so "second call sees completed status" holds reliably.

---

## Constants to introduce (as private static readonly on `PartitionDO`)

```typescript
private static readonly STALE_TX_MS = 5_000;
private static readonly MIGRATION_FALLBACK_ALARM_MS = 10_000;
private static readonly SPLIT_FALLBACK_ALARM_MS = 5_000;
```

---

## Testing considerations

1. `__testing__alarm_running` must remain — tests use it to detect when the alarm is executing.
2. Tests that fire `triggerMigration()` and then manually trigger the alarm still work: the
   `setTimeout(0)` will have fired (or been cleared) before the test manually invokes `alarm()`.
   In Miniflare/test environments, `setTimeout(0)` typically resolves before the test's next
   `await`, so the migration may already be complete before the alarm is manually triggered.
3. Tests verifying the alarm-as-fallback path should call `ctx.storage.setAlarm` manually after
   ensuring no `setTimeout` is pending (`#_backgroundWorkScheduledAt === null`). Since this field
   is private, expose it as `__testing__backgroundWorkScheduledAt` alongside the other test hooks.

---

## Implementation order

1. Step 1 (`ensureAlarmSet`) — pure refactor, no behavior change
2. Step 8 + Step 9 (topology fixes) — removes alarm from topology, fixes `startSplit`
3. Step 2 (`scheduleBackgroundWork` field + method) — new plumbing, not yet wired
4. Step 3 (`runBackgroundWork`) — extract from `alarm()`
5. Step 4 (slim `alarm()`) — delegates to `runBackgroundWork`
6. Steps 5–7 (fast paths in `initFromSplit`, `triggerMigration`/`ensureMigration`, `checkSplits`) — wire fast paths
7. Run full test suite; confirm no regressions in split and migration tests
