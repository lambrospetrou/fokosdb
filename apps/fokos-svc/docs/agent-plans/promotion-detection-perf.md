# Promotion Detection Performance Fix

## Problem

`runBackgroundWork()` in `do-partition.ts` runs frequently. The promotion detection block (lines ~1800–1821) executes a full `GROUP BY hk` scan over the entire `items` table every time the DB size exceeds the threshold and 30 s have elapsed since the last run:

```sql
SELECT hk, SUM(LENGTH(CAST(data AS BLOB)) + LENGTH(sk) + 80) AS est_bytes
FROM items GROUP BY hk HAVING est_bytes >= ? ORDER BY est_bytes DESC LIMIT 5
```

This is O(total items), reads every blob, and gets worse as the partition grows — exactly when it matters most.

## Solution

Maintain an incremental per-hash-key size estimate updated on every write. Move detection to the write path so the background job never scans for heavy keys again.

Two new schema objects:

- `est_row_bytes INTEGER` column on `items` — per-row size stored at write time, enabling cheap delta computation without re-reading `data`
- `key_size_estimates(hk PK, est_bytes INTEGER)` — per-hash-key running total; detection is a point lookup after each write

---

## Migration (migration 4)

```sql
ALTER TABLE items ADD COLUMN est_row_bytes INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS key_size_estimates (
  hk        TEXT    NOT NULL PRIMARY KEY,
  est_bytes INTEGER NOT NULL DEFAULT 0
);
```

Existing rows get `est_row_bytes = 0`. `key_size_estimates` starts empty and is built up incrementally as writes happen after the migration.

No eager backfill needed — this is not yet deployed and we don't need backwards compatibility and phase 5 below will delete the existing mechanism inside runBackgroundJob.

---

## Helper function

Add next to the existing `estimateItemBytes` helper at the bottom of the file:

```typescript
function estimateRowBytes(data: string | Uint8Array, hk: string, sk: string): number {
	const dataBytes = typeof data === "string" ? data.length : data.byteLength;
	// hk and sk are variable-length and physically stored in every row (WITHOUT ROWID PK),
	// so a long hk with many sort keys contributes significant storage that must be counted per-row.
	// 40 = fixed overhead: integer columns (v, ttl_epoch_utc_seconds, last_transaction_ts, est_row_bytes ≈ 4×8 = 32 bytes)
	//      + SQLite B-tree record metadata (header varints, null bitmap ≈ 8 bytes).
	return dataBytes + hk.length + sk.length + 40;
}
```

The original SQL estimate used `+ LENGTH(sk) + 80` and omitted `hk` because the `GROUP BY hk` made it a constant per group. For our per-row accumulation both string columns must be counted explicitly, so the fixed constant shrinks accordingly.

---

## Delta pattern

### PUT (upsert)

1. Pre-read: `SELECT est_row_bytes FROM items WHERE hk = ? AND sk = ? LIMIT 1` → `oldEst` (treat `null` as `0` — new row)
2. Compute `newEst = estimateRowBytes(data, sk)` in JS
3. Add `est_row_bytes = ?` bound to `newEst` in the INSERT column list and the `ON CONFLICT DO UPDATE SET` clause
4. Update summary (single upsert handles both new-hk and existing-hk cases):

   ```sql
   INSERT INTO key_size_estimates (hk, est_bytes) VALUES (?, ?)
   ON CONFLICT(hk) DO UPDATE SET est_bytes = MAX(0, est_bytes + excluded.est_bytes - ?)
   ```

   Bound as `(hk, newEst, oldEst)`. When `oldEst = 0` (insert) the delta equals `newEst`; when `oldEst > 0` (update) the delta is `newEst - oldEst`. Both cases are correct.

5. **Detection (write-path):** after updating `key_size_estimates`, compute:

   ```typescript
   const newKeyEst = (existingKeyEst ?? 0) - oldEst + newEst; // or read from RETURNING
   const threshold = (pCtx.hashSplitConditions.maxSizeMb ?? 0) * RANGE_PROMOTION_FRACTION * 1024 * 1024;
   if (isHashPartition(pCtx) && threshold > 0 && newKeyEst >= threshold && !this.#_promotedKeys.has(hk)) {
   	// queue promotion — same INSERT OR IGNORE + #_promotedKeys.set logic as today
   }
   ```

   `newKeyEst` can be derived in JS without an extra SELECT: it equals `previousKeyEst + delta`, where `previousKeyEst` can be read back from a `RETURNING est_bytes` on the `key_size_estimates` upsert, or just computed from `oldKeyEst + (newEst - oldEst)` if you first read it.

   Simpler: just use `RETURNING est_bytes` on the `key_size_estimates` upsert to get the final value in one shot.

### DELETE

1. Pre-read: `SELECT est_row_bytes FROM items WHERE hk = ? AND sk = ? LIMIT 1` → `oldEst` (must be before the delete)
2. Do the delete
3. If `rowsWritten > 0`:
   ```sql
   UPDATE key_size_estimates SET est_bytes = MAX(0, est_bytes - ?) WHERE hk = ?
   ```
   Bound as `(oldEst, hk)`.

No detection needed on delete (size is shrinking).

---

## Write locations to update

### 1. `putItem` (~L304)

Direct non-transactional write. Apply the full PUT delta pattern above.

### 2. `deleteItem` (~L376)

Direct non-transactional delete. Apply the DELETE delta pattern above.

### 3. `applyCommitItems` — `put` branch (~L935)

Transactional put (called inside `commitLocal`). Apply the full PUT delta pattern. Runs inside `transactionSync`, so the pre-read and summary update both happen atomically.

### 4. `applyCommitItems` — `delete` branch (~L950)

Transactional delete. Apply the DELETE delta pattern. Same transactionSync context.

Note: the `check` operation only updates `last_transaction_ts`, not `data` — skip it.

### 5. Migration inserts — `runHashChildMigration` and `runRangeChildMigration`

The INSERT OR IGNORE loops receive `MigratedItem` rows that already have `data` and `sk`. For each item:

- Compute `est_row_bytes` in JS and add it to the INSERT column list
- Do **not** maintain `key_size_estimates` row-by-row during migration (adds complexity, partition is blocked on user writes anyway)
- After all item batches complete and before calling `acknowledgeChildMigrationComplete` / `acknowledgePromotionComplete`, run a one-time rebuild:
  ```sql
  INSERT INTO key_size_estimates (hk, est_bytes)
  SELECT hk, SUM(est_row_bytes) FROM items GROUP BY hk
  ON CONFLICT(hk) DO UPDATE SET est_bytes = excluded.est_bytes
  ```
  This is a one-time scan on a freshly-populated partition — acceptable cost.

---

## Background job changes

### Phase 5: Remove the detection block from `runBackgroundWork`

The entire block from `// Detection: find heavy keys...` (~L1791) through the closing `catch` (~L1824) can be deleted once the write-path detection is in place.

The 30 s rate-limit field `#_lastPromotionDetectionAt` and its initialiser can be removed too.

The `databaseSize > threshold` guard in `checkSplits` (~L1094) that schedules background work early can also be removed — detection now fires immediately on the write that crosses the threshold.

### GC cleanup

After the promotion GC batch delete (~L1842), when residual hits 0:

```typescript
if (residual === 0) {
	this.ctx.storage.sql.exec(`DELETE FROM key_size_estimates WHERE hk = ?`, hashKey);
}
```

The residual check already exists at ~L1890 in the alarm-scheduling block — reuse that query result or move the GC check there.

---

## What does NOT change

- `promoted_keys` table and `#_promotedKeys` in-memory map — unchanged
- `startPromotion` — unchanged
- The Drive and GC loops in `runBackgroundWork` — unchanged
- Detection threshold formula — same constant, same `RANGE_PROMOTION_FRACTION`
- The `checkSplits` scheduling for hash-split conditions — unchanged (only the promotion-threshold guard inside it is removed)

---

## Risk / edge cases

- **Counter drift on crash:** if the process crashes between the `items` upsert and the `key_size_estimates` update (they are not in the same `transactionSync`), the counter will be slightly off. Wrap the pair in `transactionSync` in the direct write paths (`putItem`, `deleteItem`).
- **`applyCommitItems` pre-read overhead:** this runs inside `commitLocal` which is already inside a `transactionSync`. Adding a SELECT per item is additional SQL round-trips. Given that commit batches are typically small and the SELECT is a PK point lookup, this is acceptable.
