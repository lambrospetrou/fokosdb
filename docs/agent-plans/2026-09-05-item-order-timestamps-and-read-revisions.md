# RFC — Item order timestamps and transactional-read revisions

**State:** Draft
**Date:** 2026-09-05
**Author:** Lambros

---

## Table of Contents

1. [Overview and Context](#1-overview-and-context)
2. [Goals and Requirements](#2-goals-and-requirements)
3. [Timeline and Milestones](#3-timeline-and-milestones)
4. [Proposed Solution](#4-proposed-solution)
5. [Alternative Options](#5-alternative-options)
6. [Frequently Asked Questions](#6-frequently-asked-questions)
7. [References](#7-references)

---

## 1. Overview and Context

### 1.1 Current behavior

Each live item has one `last_transaction_ts` value. A put, an update, and a `check` advance this value.
A delete removes the item row and can advance the partition-wide `max_deleted_ts` value.

`TransactionParticipant.prepareLocal` compares every operation with the same item timestamp. A transaction fails
with `timestamp_conflict` when its timestamp is not greater than that item timestamp.

This rule treats reads and writes as the same operation. It rejects a `check` when a newer `check` committed after
the last item write. The two checks read the same committed value and can use either order.

`FokosDB.transactGetItems` uses a two-phase writeless protocol. Each phase reads the item version, the item
timestamp, and the pending-lock state. The client aborts when these values differ between the two phases.

A committed `check` changes `last_transaction_ts` without changing the item contents. A `check` between the read
phases can therefore cause a false `read_conflict`.

The item version detects writes while the item row continues to exist. The version resets after a delete and a
recreate. The timestamp is a second signal, but two writes can receive the same millisecond value. An absent item
returns zero in both phases, so an absent-create-delete sequence can also pass the comparison.

### 1.2 Proposed behavior

The item stores separate read and write timestamps:

- `last_read_ts` is the item order watermark for a committed `check` or content mutation.
- `last_write_ts` is the item order watermark and write revision for a content mutation.

A `check` compares only with `last_write_ts`. A content mutation compares with both timestamps. This preserves the
current conflict rule for every content mutation and removes read-after-read conflicts.

The timestamps use microsecond-shaped integer values. A physical millisecond occupies 1,000 logical values. A
write to an existing item can advance `last_write_ts` inside one millisecond without a high-resolution timer.

The deletion metadata stores two values with different purposes:

- `max_delete_order_ts` preserves the current absent-item prepare rule.
- `delete_revision` changes when an operation removes at least one item row.

Each transactional-read result carries the owner partition's `delete_revision`. The client compares it across the
two phases. This detects a delete and recreate even when the item version and write timestamp repeat.

The design keeps the two-phase read protocol writeless. Normal reads and transactional reads do not update
`last_read_ts`.

### 1.3 Glossary

**Base timestamp:** An operation timestamp in microsecond-shaped units. The value is `Date.now() * 1_000` for a
partition-local operation. A two-phase transaction receives the value from its coordinator.

**Content mutation:** A put, an update, or a delete that removes an item row.

**Item order timestamp:** A timestamp that `prepareLocal` uses to reject an out-of-order operation on a live item.

**Write revision:** A `last_write_ts` value that changes after each content mutation while the item row exists.

**Delete revision:** The partition-wide `delete_revision` counter. It changes after an operation removes at least
one item row.

---

## 2. Goals and Requirements

### 2.1 In scope

- The `items` table must replace `last_transaction_ts` with `last_read_ts` and `last_write_ts`.
- The timestamps must use microsecond-shaped safe integers.
- A write to an existing item must advance `last_write_ts`, including writes in one physical millisecond.
- Every put and update must advance both item timestamps.
- A successful `check` on a live item must advance only `last_read_ts`.
- A `check` must compare its transaction timestamp only with `last_write_ts`.
- A put, update, or delete must compare its transaction timestamp with both item timestamps.
- Content mutations must keep their current prepare outcomes at millisecond granularity.
- The current absent-item prepare rule must remain unchanged apart from the timestamp unit and column name.
- The deletion metadata must rename `max_deleted_ts` to `max_delete_order_ts`.
- The deletion metadata must add `delete_revision`.
- An actual row removal must advance `delete_revision`.
- A delete that finds no row must not advance `delete_revision`.
- A transactional read must ignore committed and pending `check` operations as content mutations.
- A transactional read must detect a delete and recreate between its two phases.
- Hash splits, range splits, and hash-key promotion must preserve all timestamps and revisions.
- The internal RPC types must carry the new revision fields without a public API change.

### 2.2 Out of scope

- The change must not add the Thomas Write Rule or discard an old write at commit.
- The change must not permit multiple prepared write transactions on one item.
- The change must not add `max_absent_read_ts` or another watermark for a `check` on an absent item.
- The change must not propagate a removed row's read timestamp into `max_delete_order_ts`.
- The change must not add timestamp tests to the single-partition write fast path.
- The change must not make `getItem`, `queryItems`, or `transactGetItems` write a read timestamp.
- The change must not replace the two-phase transactional-read protocol with a one-phase protocol.
- The change must not add per-key tombstones.
- The change must not change the coordinator state machine, recovery protocol, or idempotency behavior.
- The change must not change TTL visibility or the logical timestamp of TTL deletion.

### 2.3 Requirements that constrain the solution

- Every stored timestamp, revision, and item version must be a JavaScript safe integer.
- A statement that contains a `+1` increment on a stored timestamp, revision, or version returns the stored
  value, and the store asserts `Number.isSafeInteger` in JavaScript inside the same storage transaction. A
  failed assertion throws and rolls the operation back. The code must not use a SQLite `CHECK` constraint for
  this bound.
- The code must use `1_000` logical units per millisecond.
- The code must not use nanosecond epoch values in a JavaScript `number`.
- `data` must remain the last column of `items`.
- Neither item timestamp must join `idx_items_scan`.
- Every item timestamp update must be monotonic.
- Every deletion metadata update must be atomic with its delete operation.
- A transaction that rolls back must also roll back its timestamp and revision changes.
- Migration ingest must copy timestamps and must not create new logical mutations.
- Promotion cleanup must not advance deletion metadata.
- The client must strip all timestamp and revision fields from the public response.
- A pending `check` must continue to lock writers until its transaction commits or cancels.
- The timestamp unit change must not change TTL, alarm, staleness, or idempotency time units.

---

## 3. Timeline and Milestones

### M0 — Microsecond timestamp units

Scale every order timestamp to microsecond-shaped units. Keep the single `last_transaction_ts` column and every
current conflict rule.

- Add the shared `TIMESTAMP_UNITS_PER_MS` constant.
- The coordinator writes `tc_state.transaction_ts` as `Date.now() * TIMESTAMP_UNITS_PER_MS`.
- The single-shot path and every partition-local stamp use the same expression. This includes every indirect
  producer of an order timestamp, such as the `Date.now()` fallback that `debugForceResolveTransaction` passes to
  `txCommit`. The `TransactionTimestamp` type comment moves to the new unit.
- The clock-skew test compares physical milliseconds.
- The `clock_skew` rejection renames `serverTimestampMs` and `transactionTimestampMs` to
  `serverTimestampMicros` and `transactionTimestampMicros`, both carrying values in the order unit.
- The TTL sweep watermark uses `ttl_epoch_utc_seconds * 1_000_000`.
- `created_at`, `completed_at`, alarm deadlines, staleness ages, and TTL storage keep their current units.
- `TransactionParticipant` separates its wall-clock source from its order-timestamp source. One injected clock
  currently supplies both the staleness age and the single-shot transaction timestamp.

M0 must not add the logical write increment. A write keeps `MAX(last_transaction_ts, base_timestamp)`. The item
version already detects each write to a live row, so the increment gives no new signal before `last_write_ts`
exists.

M0 changes no schema, no prepare outcome, and no read outcome. It also removes no conflict: a check still
conflicts with a newer check, and a delete and recreate inside one millisecond still passes the transactional-read
comparison. M0 isolates the unit change from the conflict-rule change and reduces M1 to a column change.

The project is before its first release, so M0 needs no data migration. A development environment with stored
millisecond values must be discarded.

### M1 — Schema and timestamp helpers

Replace the item timestamp, rename the delete watermark, and add `delete_revision`. Add the write-stamp helper on
top of the M0 units. Update every `PartitionStore` item reader and writer.

M1 delivers a complete storage model. No transaction rule can use the new fields before all writers maintain them.

### M2 — Prepare and commit behavior

Update condition evaluation, update probing, `prepareLocal`, `commitLocal`, and `executeSingleShot`. Apply the
operation matrix in section 4.2.3.

M2 removes read-after-read conflicts for `check`. It preserves the current content-mutation guards.

### M3 — Transactional-read revisions

Update the read RPC result, pending-lock classification, partition read methods, and the two-phase comparison.
Return `last_write_ts` and `delete_revision` as internal revision values.

M3 removes false conflicts from `check` operations. It also detects delete-and-recreate ABA sequences.

### M4 — Split, promotion, and verification

Copy both item timestamps and both deletion metadata values through every migration path. Add unit and integration
tests for the complete design. Run all project verification commands. Update `AGENTS.md` and any other project
documentation that names the removed fields (`last_transaction_ts`, `max_deleted_ts`).

---

## 4. Proposed Solution

### 4.1 High-level overview

The item has two independent order watermarks:

```text
items row
  hk, sk
  data, data_kind
  v
  last_read_ts
  last_write_ts
  ttl_epoch_utc_seconds
  est_row_bytes
```

A content mutation advances both timestamps. A `check` advances only the read timestamp.

```text
                                  committed operation
                                           |
                    +----------------------+----------------------+
                    |                                             |
                 check                                     content mutation
                    |                                             |
       advance last_read_ts                         advance last_read_ts
                                                    advance last_write_ts
```

Prepare applies these tests to a live item:

```text
check:
  transaction_ts > last_write_ts

put, update, or delete:
  transaction_ts > last_read_ts
  transaction_ts > last_write_ts
```

The second rule is equivalent to the current comparison against the combined timestamp. The first rule accepts an
older read after a newer read when no newer write exists.

The transactional-read protocol compares three mutation signals:

```text
live-row write:        item version and last_write_ts
row lifecycle change:  delete_revision
pending mutation:      pending put, update, or delete
```

A pending or committed `check` changes none of these signals.

### 4.2 Technical details

#### 4.2.1 Timestamp representation

Add one shared constant:

```ts
const TIMESTAMP_UNITS_PER_MS = 1_000;
```

A coordinator creates a transaction timestamp with:

```ts
const transactionTs = Date.now() * TIMESTAMP_UNITS_PER_MS;
```

A partition-local operation creates its base timestamp with the same expression. The code must assert that every
created timestamp is a safe integer.

The same bound applies on the increment side. `last_write_ts + 1`, `delete_revision + 1`, and `v + 1` run inside
SQL, and SQLite computes them as 64-bit integers, so a statement cannot detect the JavaScript bound on its own.
Only those increments need a guard: `MAX` combines a stored value that a previous assertion bounded with a
created timestamp that the code asserts, so a `MAX` result can never cross the bound first.

A statement that contains an increment returns the stored value with `RETURNING`, and the store asserts
`Number.isSafeInteger` in JavaScript inside the same storage transaction. A failed assertion throws, so
`transactionSync` rolls the whole operation back with a clear error. The check runs after the write on purpose:
reading the current value before the statement would cost the same billed read, and on `upsertItem` the existing
index-only estimate read cannot carry `v` without losing its covering index. The upsert and update statements
already return `v`, so their assertions cost nothing extra; the deletion-metadata update gains a `RETURNING`
clause for the assertion. The existing `v` invariant on the upsert and update paths tightens to the same bound.
The code must not use a SQLite `CHECK` constraint: the bound is a JavaScript limit, and the application-side
assertion keeps the failure next to the other write guards.

A write to an existing item calculates its write stamp as follows:

```text
write_stamp = MAX(base_timestamp, last_write_ts + 1)
```

An insert has no previous write timestamp:

```text
write_stamp = base_timestamp
```

The writer applies:

```text
last_write_ts = write_stamp
last_read_ts  = MAX(last_read_ts, write_stamp)
```

This rule gives one item a logical write sequence inside one physical millisecond. It does not use a partition-wide
clock, so a fast coordinator on one key cannot move an unrelated key's timestamp.

The logical part must not wrap at 999. The value must continue to increase if one item receives more than 1,000
writes before `Date.now()` advances. This case can move the item timestamp into the next physical millisecond.
Transactions can receive conservative `timestamp_conflict` results until wall time catches up.

A `check` applies:

```text
last_read_ts = MAX(last_read_ts, transaction_timestamp)
```

A `check` must not increment `last_read_ts` above a newer read timestamp. This rule permits reads to arrive out of
timestamp order.

The clock-skew test must compare physical milliseconds:

```text
FLOOR(transaction_timestamp / 1_000) <= Date.now() + MAX_CLOCK_SKEW_MS
```

The `clock_skew` rejection renames `serverTimestampMs` and `transactionTimestampMs` to `serverTimestampMicros`
and `transactionTimestampMicros`. Both carry values in the order unit: `serverTimestampMicros` is the partition
wall clock times `TIMESTAMP_UNITS_PER_MS`, and `transactionTimestampMicros` is the transaction timestamp. This
changes the public shape of that rejection reason. The rejection reason is persisted in
`tc_state.rejection_reason_json` and replayed verbatim, so rows written before the change keep the old field
names.

The following values remain in their current units:

- `created_at` and `completed_at` values remain in epoch milliseconds.
- Alarm deadlines and recovery ages remain in milliseconds.
- `ttl_epoch_utc_seconds` remains in epoch seconds.
- The idempotency window remains in milliseconds.

A TTL expiry timestamp converts to the new order unit with:

```text
ttl_expiry_order_ts = ttl_epoch_utc_seconds * 1_000_000
```

#### 4.2.2 Schema

The `items` table becomes:

```sql
CREATE TABLE IF NOT EXISTS items (
    hk                    BLOB    NOT NULL,
    sk                    BLOB    NOT NULL DEFAULT x'',
    data_kind             INTEGER NOT NULL DEFAULT 0,
    v                     INTEGER NOT NULL,
    last_read_ts          INTEGER NOT NULL DEFAULT 0,
    last_write_ts         INTEGER NOT NULL DEFAULT 0,
    ttl_epoch_utc_seconds INTEGER,
    est_row_bytes         INTEGER NOT NULL,
    data                  ANY     NOT NULL,

    PRIMARY KEY (hk, sk)
) STRICT;
```

The index definitions do not change.

The deletion metadata becomes:

```sql
CREATE TABLE IF NOT EXISTS deletion_metadata (
    id                    INTEGER PRIMARY KEY CHECK (id = 1),
    max_delete_order_ts   INTEGER NOT NULL DEFAULT 0,
    delete_revision       INTEGER NOT NULL DEFAULT 0
) STRICT;
```

`max_delete_order_ts` and `delete_revision` use different update rules. Section 4.2.5 defines those rules.

#### 4.2.3 Operation matrix

| Operation | Prepare guard on a live row | Timestamp update |
| --- | --- | --- |
| Two-phase put | Above both timestamps | Advance both at commit |
| Two-phase update | Above both timestamps | Advance both at commit |
| Two-phase delete | Above both timestamps | Remove the row at commit |
| Two-phase `check` | Above `last_write_ts` | Advance `last_read_ts` at commit |
| Single-shot put | No timestamp guard | Advance both |
| Single-shot update | No timestamp guard | Advance both |
| Single-shot delete | No timestamp guard | Remove the row |
| Single-shot `check` | No timestamp guard | Advance `last_read_ts` |
| Non-transactional put | No timestamp guard | Advance both |
| Non-transactional delete | No timestamp guard | Remove the row |
| TTL sweep | No timestamp guard | Remove the rows |
| Migration ingest | Not applicable | Copy both source values |
| Promotion cleanup | Not applicable | No timestamp or revision update |

A condition on a put, update, or delete does not change this matrix. Every successful content mutation advances both
item timestamps. This conservative rule preserves the combined timestamp behavior.

A failed condition writes no timestamp. The failed operation does not enter the committed transaction history.

#### 4.2.4 Prepare behavior

`TransactionParticipant.prepareLocal` must get both item timestamps from its existing read source:

- `PartitionStore.getItemStamp` must return both timestamps.
- `ConditionEvaluationResult` must return both timestamps.
- `UpdateProbeResult` must return both timestamps.

The condition and update SQL statements must select both columns. The participant must not add a second item read.

For `operation === "check"`, prepare rejects when:

```text
transaction_timestamp <= last_write_ts
```

For every other operation, prepare rejects when:

```text
transaction_timestamp <= last_read_ts
OR transaction_timestamp <= last_write_ts
```

When the item is absent, prepare must retain the current rule:

```text
transaction_timestamp <= max_delete_order_ts
```

A `check` on an absent item must not add another watermark. Locks continue to provide serializability for this
case.

The pending-lock rule does not change for write transactions. A pending `check` remains an exclusive lock against
another write transaction and against a non-transactional writer.

#### 4.2.5 Delete ordering and delete revision

`max_delete_order_ts` replaces `max_deleted_ts` without changing its meaning.

The store updates `max_delete_order_ts` with:

```text
max_delete_order_ts = MAX(max_delete_order_ts, candidate_order_timestamp)
```

The candidate remains specific to each path:

| Delete path | Candidate order timestamp | Update when the row is absent? |
| --- | --- | --- |
| Two-phase transaction | Coordinator transaction timestamp | Yes |
| Single-shot transaction | Partition base timestamp | Yes |
| Non-transactional delete | Partition base timestamp | No |
| TTL sweep | Maximum expiry timestamp in the deleted batch | Not applicable |

`delete_revision` is not an order timestamp. It is a partition-local monotonic counter.

The store increments `delete_revision` after each delete statement that removes one or more item rows:

```text
delete_revision = delete_revision + 1
```

The following rules apply:

- Each successful single-item delete statement increments the revision once.
- A transaction with multiple successful delete statements can increment the revision more than once.
- A TTL sweep increments the revision once when its batch removes at least one row.
- A transactional delete of an absent item does not increment the revision.
- A non-transactional delete of an absent item does not increment the revision.
- Promotion cleanup does not increment the revision.
- A rolled-back delete does not increment the revision.

The store must update both metadata fields in one statement when a delete changes both. This keeps the existing
metadata row-write count for that path.

The counter can start at zero. The client compares revisions for equality and does not compare them with item
timestamps.

#### 4.2.6 Transactional-read protocol

Replace the internal `lastCommittedTs` field with explicit revision fields:

```ts
type ReadForTransactionItemResultEncoded = {
    // Existing item fields and keys.
    lastWriteTs: number;
    deleteRevision: number;
    hasPendingWrite: boolean;
};
```

Until M3 lands, `readForTransactionLocal` keeps the `lastCommittedTs` field and maps it to `last_read_ts`. That
column is the continuation of `last_transaction_ts`: every committed operation that touches the item advances it,
so the two-phase comparison keeps its current outcomes. M3 changes the field to `last_write_ts` and adds
`deleteRevision`.

A live item returns its `last_write_ts`. An absent item returns zero for `lastWriteTs`. Every item result returns the
current `delete_revision` of the local owner partition.

The participant must read `delete_revision` once per local RPC. It must reuse that value for every result from the
same local partition. Split fan-out can return different values for items owned by different children.

The client compares two phase results with:

```text
same key
AND same found state
AND same deleteRevision
AND, when found:
    same version
    AND same lastWriteTs
```

A different `deleteRevision` causes `read_conflict`, including an unrelated deletion in the same partition. This
is a conservative false conflict, and it is a new abort source: today an unrelated deletion never aborts a read,
because it does not touch the read item's version or timestamp. The abort rate grows with the number of owner
partitions a read touches and with the latency between the two phases — one delete in any touched partition
between the phases aborts the whole read. A per-key tombstone is necessary to remove it.

The client returns the phase-one values after every comparison succeeds. It strips `lastWriteTs`,
`deleteRevision`, and `hasPendingWrite` at the public boundary.

The single-partition read fast path already reads one atomic snapshot. It does not need a two-phase revision
comparison. It can omit the metadata read or ignore the returned revision.

#### 4.2.7 Pending checks during transactional reads

`PartitionStore.pendingLockFor` must expose the pending operation or a dedicated method must test for a pending
content mutation.

`hasPendingWrite` is false only for a pending `check`. Every other pending operation value sets it to true:

```text
hasPendingWrite = pendingRow != null && pendingRow.operation !== "check"
```

The classification must be an allowlist of the read-only operations, not a list of the write operations. An
operation value that the code does not know must fail closed and count as a pending write.

The pending `check` cannot change the item contents. Its lock still prevents another writer from invalidating its
condition before commit.

When a transaction checks item A and writes item B, a transactional read of B still sees a pending write. A
transactional read that reads only A can serialize on either side of the check transaction.

The same classification must apply to the two-phase read and the single-partition read fast path.

#### 4.2.8 Deletes, TTL, and ABA detection

The revision comparison detects these row-lifecycle sequences:

```text
found(v=1, write=T) -> delete -> recreate(v=1, write=T) -> found
absent               -> create -> delete                   -> absent
```

Each sequence contains an actual row removal, so `delete_revision` changes. The comparison fails even when the
item version and write timestamp return to their first values.

The TTL sweep uses its expiry timestamp for `max_delete_order_ts`, as it does today. The sweep increments
`delete_revision` at the time of the actual row removal. The two values can therefore describe different orders.

A delete of an absent item can advance `max_delete_order_ts` without advancing `delete_revision`. The delete
orders a write transaction but does not change readable item state.

#### 4.2.9 Split and promotion migration

`MigratedItem` must replace `last_transaction_ts` with both item timestamps. `queryItemsPage` and
`queryRangeItemsPage` must select both values. `insertItemIfAbsent` must write both values without modification.

`GetPartitionTransactionMetadataResult` must replace `maxDeletedTs` with:

```ts
{
    maxDeleteOrderTs: number;
    deleteRevision: number;
    pendingTransactions: PendingTransactionRow[];
    nextCursor: PendingTransactionCursor | null;
}
```

Each child must merge the metadata with `MAX`:

```text
child.max_delete_order_ts = MAX(child value, parent value)
child.delete_revision     = MAX(child value, parent value)
```

The parent can return the same metadata values on each pending-lock page. The merge is idempotent.

Migration ingest must not advance `delete_revision`. Promotion cleanup removes obsolete parent copies and must not
advance it. These operations move ownership and do not change logical item state.

A child in `migration_migrating` continues to reject write and transaction RPCs. This guard prevents a local
mutation before the child receives the source timestamps and revisions.

#### 4.2.10 Atomicity and failure behavior

Every existing storage transaction must include its timestamp and revision updates. A successful response must not
leave a content mutation without its revisions.

The two-phase participant preserves its current write-ahead protocol. Prepare stores the lock and operation.
Commit applies the operation, advances the revisions, and removes the lock in one local storage transaction.

Commit retry behavior does not change. A retry that finds no pending rows returns `committed` and must not advance a
revision again.

A delete retry before commit completion runs inside the same local transaction. A rollback restores the deleted
row and the previous `delete_revision`.

The recovery path continues to call the public commit and cancel methods. These methods retain migration guards
and split routing.

#### 4.2.11 Performance and storage

Each item gains one integer column because one current column becomes two. The column does not join an index.
`estRowBytesExpr`'s fixed overhead (`EST_ROW_BYTES_K`) and `estimateItemBytes` count the integer columns of the
row. Both must grow by one column so that `key_size_estimates`, the split and promotion thresholds, and the
migration batch byte budget do not drift low.

A put, update, or `check` continues to update one item row. The second timestamp does not add a billed row write.
It increases the stored row width.

An actual delete already updates the deletion metadata. Updating `max_delete_order_ts` and `delete_revision` in one
statement keeps that metadata update at one row write.

The safe-integer assertions read the stored values back. `RETURNING` on a statement reports the returned rows as
additional billed reads. The upsert and update statements already return rows, so their assertions add no read.
The deletion-metadata update gains one billed read per call from its new `RETURNING` clause. The check-bump
needs no assertion: its `MAX` combines already-bounded values.

Each two-phase transactional-read RPC adds one deletion metadata row read per local owner partition. A read over N
owner partitions adds 2N metadata row reads across both phases. The implementation must not read the metadata once
per item.

Each internal item result gains one number for `deleteRevision`. `lastWriteTs` replaces `lastCommittedTs`, so it
does not add another response field.

Cloudflare bills SQLite storage by rows read and rows written. Updating one physical SQLite page does not determine
the billed count. The change must use the SQL cursor metrics to verify the expected counts.

TODO: benchmark the transactional-read latency and abort rate under unrelated deletion load.

#### 4.2.12 Deployment and rollback

The project is before its first release. A clean development state can use an edited initial schema.

Any environment with persisted item rows needs a migration. That migration must initialize both timestamps from
the old value:

```text
last_read_ts  = last_transaction_ts * 1_000
last_write_ts = last_transaction_ts * 1_000
```

This mapping is conservative. An old `check` value can temporarily look like a write timestamp, but it cannot admit
an operation that the old schema rejected.

The migration must preserve the old delete watermark with:

```text
max_delete_order_ts = max_deleted_ts * 1_000
delete_revision = 0
```

A zero initial delete revision cannot identify row lifecycles that finished before deployment. It correctly detects
all row removals after deployment.

The unit migration must also scale stored transaction order timestamps:

```text
pending_transactions.transaction_ts = pending_transactions.transaction_ts * 1_000
tc_state.transaction_ts              = tc_state.transaction_ts * 1_000
```

The migration must not scale `created_at`, `completed_at`, TTL values, alarm times, or recovery ages. An in-flight
transaction must use one unit on the coordinator and every participant. The release must ship the coordinator,
partition, RPC type, and schema changes in one Worker version.

`data` must remain the last item column. If SQLite cannot rename and add the columns in the required order, the
migration must build a replacement table and copy every row.

The item and metadata schema changes are not compatible with old code that names the removed columns. A rollback
after persistent state uses the inverse data migration or a forward fix. A code-only rollback is not safe.

#### 4.2.13 Tests and verification

The store tests must prove:

- Every put and update path advances both item timestamps.
- Two writes in one millisecond advance `last_write_ts` on one live item.
- A write timestamp never decreases when the wall clock or coordinator clock is behind.
- A `check` advances only `last_read_ts`.
- An out-of-order `check` does not decrease `last_read_ts`.
- A real delete increments `delete_revision` once.
- A TTL batch increments `delete_revision` once when it removes rows.
- An absent delete does not increment `delete_revision`.
- A transactional absent delete still advances `max_delete_order_ts`.
- Promotion cleanup changes neither deletion metadata value.
- Every stored timestamp, revision, and item version is a safe integer.
- A seeded boundary value makes the next incrementing write fail and roll back, for the write timestamp, the
  delete revision, and the item version alike.
- The partition migration scales `pending_transactions.transaction_ts` and no wall-time field.
- The coordinator migration scales `tc_state.transaction_ts` and no wall-time field.

The participant tests must prove:

- A `check` above `last_write_ts` succeeds when it is below `last_read_ts`.
- A `check` at or below `last_write_ts` fails with `timestamp_conflict`.
- A write at or below either item timestamp fails with `timestamp_conflict`.
- A pending `check` does not set `hasPendingWrite`.
- A pending put, update, or delete sets `hasPendingWrite`.
- The first commit advances the expected timestamps and revisions.
- A commit retry does not advance a timestamp or revision.

The transactional-read tests must prove:

- A committed `check` between phases does not cause `read_conflict`.
- A pending `check` during a phase does not cause `pending_write`.
- A pending content mutation still causes `pending_write`.
- A normal write between phases causes `read_conflict`.
- A delete and recreate with the same version and write timestamp causes `read_conflict`.
- An absent-create-delete sequence causes `read_conflict`.
- An unrelated deletion in the same partition causes the documented conservative `read_conflict`.
- An unrelated deletion in a different partition does not change another partition's revision.
- The single-partition read fast path applies the same pending-lock classification: a pending `check` does not
  abort it and a pending put, update, or delete does.

The migration tests must prove:

- Hash migration copies both item timestamps and both deletion metadata values.
- Range promotion copies both item timestamps and both deletion metadata values.
- Range split migration copies both item timestamps and both deletion metadata values.
- Migration retries do not advance a timestamp or revision.
- A child keeps the inherited values after it starts serving traffic.

The project verification must run:

```text
pnpm test
pnpm check
pnpm build
```

---

## 5. Alternative Options

### 5.1 Keep one item timestamp

Keep `last_transaction_ts` for every operation.

This option stores one less integer per item and changes less SQL. It cannot distinguish a committed read from a
committed write. An older `check` therefore conflicts with a newer `check`, although both read the same value.

The transactional-read protocol also sees a committed `check` as a content mutation. A `check` between phases can
cause `read_conflict` without a content change.

The one timestamp is therefore worse for check contention and transactional-read availability.

### 5.2 Use one delete timestamp for ordering and revision

Make `max_delete_order_ts` advance after every delete and return it as the absent-item revision.

This option removes `delete_revision`. It also raises the prepare watermark when a delete candidate is below the
current maximum. A transaction against any absent item can then fail inside the new logical interval.

A transactional delete of an absent item must advance the order watermark. The same update would also look like a
content change to a transactional read, although the item remained absent.

Separate fields preserve the current prepare behavior and advance the read revision only after a row removal.

### 5.3 Use a global partition HLC for every item timestamp

Allocate every item timestamp from one durable partition clock.

This option gives the partition one total operation order. A coordinator timestamp can be up to
`MAX_CLOCK_SKEW_MS` ahead of the partition clock. Observing that timestamp can move the global clock ahead.
A later operation on an unrelated key then inherits that value and can cause cross-key timestamp conflicts.

Per-item logical write timestamps avoid this propagation. `delete_revision` covers the lifecycle boundary where
the item row disappears.

### 5.4 Add a per-key tombstone

Keep a revision row for each deleted key.

This option detects an absent-item lifecycle without conflicts from unrelated deletes. It adds storage for deleted
keys and needs a safe garbage-collection rule. A tombstone cannot expire while an older transactional read or
transaction timestamp can still require it.

The partition-wide `delete_revision` uses constant storage and accepts conservative read conflicts.

### 5.5 Use nanosecond epoch values

Store `Date.now() * 1_000_000` in each timestamp.

The current epoch value exceeds `Number.MAX_SAFE_INTEGER`. The Workers SQL API returns numeric values as JavaScript
numbers, so the low-order revision bits lose precision. Microsecond-shaped values remain safe.

### 5.6 Write a read timestamp from every read

Advance `last_read_ts` for `getItem`, query operations, and `transactGetItems`.

This option can support a one-phase timestamp-ordering read protocol. It turns persistent, replicated reads into
writes. The current two-phase protocol avoids that write cost and remains in scope.

---

## 6. Frequently Asked Questions

### Why does every put and update advance `last_read_ts`?

The rule preserves the current combined watermark. A future write must remain above every earlier content mutation
and every earlier `check`. The design does not use the Thomas Write Rule.

### Why does a `check` compare only with `last_write_ts`?

A check reads the current item and does not change it. A newer read does not invalidate that value. A newer write
means that the check would read a value from after its timestamp, so prepare rejects it.

### Why does a pending `check` still block writers?

Prepare evaluated the check condition against the current item. A writer could invalidate that condition before
the transaction commits. The lock keeps the checked value stable.

### Why can a transactional read ignore a pending `check`?

The pending check cannot change the item. A read of that item can serialize before or after the check. A pending
content mutation on another requested item still aborts the transactional read.

### Why does every transactional-read item carry `deleteRevision`?

A live item can be deleted and recreated between phases. The recreated row can repeat the old version and write
timestamp. The partition revision proves that a row removal occurred during the read.

### Why is `delete_revision` separate from `max_delete_order_ts`?

The order timestamp can stay unchanged when an older deletion occurs. The revision must change after every actual
row removal. An absent transactional delete advances the order timestamp but does not change readable state.

### Why does an unrelated delete abort a transactional read?

Every item result carries one partition-wide delete revision. The read cannot identify which key changed that
revision. It aborts rather than miss a delete-and-recreate sequence on the requested key.

### Why use microsecond-shaped values when Workers exposes a millisecond clock?

The low three decimal digits are logical slots. A write can use the previous item timestamp plus one when
`Date.now()` does not advance. Workers intentionally keeps `Date.now()` fixed between I/O events.

### Does the change expose timestamps or revisions in the public API?

No. These values remain internal RPC bookkeeping. `FokosDB.transactGetItems` strips them before it returns.

---

## 7. References

- `packages/fokosdb/src/shared/partition/partition-store.ts`
- `packages/fokosdb/src/shared/partition/transaction-participant.ts`
- `packages/fokosdb/src/shared/partition/migration.ts`
- `packages/fokosdb/src/shared/partition/partition-peer.ts`
- `packages/fokosdb/src/shared/transaction-types.ts`
- `packages/fokosdb/src/client/db.ts`
- `packages/fokosdb/src/server/do-partition.ts`
- `packages/fokosdb/src/server/do-transaction-coordinator.ts`
- `docs/agent-plans/2026-08-23-single-partition-transaction-fast-path.md`
- `docs/agent-plans/2026-08-30-item-ttl-expiration.md`
- [Distributed Transactions at Scale in Amazon DynamoDB](https://www.usenix.org/system/files/atc23-idziorek.pdf)
- <https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/>
- <https://developers.cloudflare.com/durable-objects/platform/pricing/>
- <https://developers.cloudflare.com/workers/runtime-apis/web-standards/>
