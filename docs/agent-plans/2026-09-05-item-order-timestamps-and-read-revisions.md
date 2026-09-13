# RFC — Item order timestamps and transactional-read revisions

**State:** Completed on 2026-09-13.
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
phases can therefore cause a false `read_conflict`. A pending `check` lock causes a `pending_write` abort although
the check cannot change the item.

The item version detects writes while the item row continues to exist. The version resets after a delete and a
recreate. The timestamp is a second signal, but two writes can receive the same millisecond value. An absent item
returns zero in both phases, so an absent-create-delete sequence can also pass the comparison.

### 1.2 Proposed behavior

The item stores separate read and write timestamps:

- `last_read_ts` is the item order watermark for a committed `check` or content mutation.
- `last_write_ts` is the item order watermark for a content mutation.

A `check` compares only with `last_write_ts`. A content mutation compares with `last_read_ts`. This preserves the
current conflict rule for every content mutation and removes read-after-read conflicts.

The timestamps use microsecond-shaped integer values: `Date.now() * 1_000`. The low three decimal digits are
zero for every timestamp this change creates. They are reserved for a later coordinator tie-breaking scheme.

The deletion metadata stores two values with different purposes:

- `max_delete_tx_order_ts` preserves the current absent-item prepare rule.
- `delete_revision` changes when a user delete removes at least one item row.

Each transactional-read result carries the owner partition's `delete_revision`. The client compares it across the
two phases. This detects a delete and recreate even when the item version repeats.

A transactional read ignores a pending `check` lock. Only a pending put, update, or delete aborts it.

The design keeps the two-phase read protocol writeless. Normal reads and transactional reads do not update
`last_read_ts`.

### 1.3 Glossary

**Order timestamp:** An integer in microsecond-shaped units. The value is `Date.now() * TX_ORDER_TS_UNITS_PER_MS`
for a partition-local operation. A two-phase transaction receives the value from its coordinator.

**Base timestamp:** The order timestamp that a writer stamps on an item. The coordinator transaction timestamp for
a two-phase commit, the partition clock for every other writer.

**Content mutation:** A put, an update, or a delete that removes an item row.

**Item order timestamp:** A timestamp that `prepareLocal` uses to reject an out-of-order operation on a live item.

**Delete revision:** The partition-wide `delete_revision` counter. It changes after a user delete removes at least
one item row.

---

## 2. Goals and Requirements

### 2.1 In scope

- The `items` table must replace `last_transaction_ts` with `last_read_ts` and `last_write_ts`.
- Every order timestamp must use microsecond-shaped safe integers.
- Every put and update must advance both item timestamps with `MAX`.
- A successful `check` on a live item must advance only `last_read_ts`.
- A `check` must compare its transaction timestamp only with `last_write_ts`.
- A put, update, or delete must compare its transaction timestamp with `last_read_ts`.
- Content mutations must keep their current prepare outcomes at millisecond granularity.
- The current absent-item prepare rule must remain unchanged apart from the timestamp unit and column name.
- The deletion metadata must rename `max_deleted_ts` to `max_delete_tx_order_ts`.
- The deletion metadata must add `delete_revision`.
- A user delete that removes a row must advance `delete_revision`.
- A delete that finds no row must not advance `delete_revision`.
- The TTL sweep must not advance `delete_revision`.
- A transactional read must ignore committed and pending `check` operations.
- A transactional read must detect a user delete and recreate between its two phases.
- Hash splits, range splits, and hash-key promotion must preserve all timestamps and revisions.
- The internal RPC types must carry `deleteRevision` and `hasPendingWrite`. The client must strip both.
- The `clock_skew` rejection must report its two timestamps in the order unit under new field names.

### 2.2 Out of scope

- The change must not add the Thomas Write Rule or discard an old write at commit.
- The change must not permit multiple prepared write transactions on one item.
- The change must not add `max_absent_read_ts` or another watermark for a `check` on an absent item.
- The change must not propagate a removed row's read timestamp into `max_delete_tx_order_ts`.
- The change must not add timestamp tests to the single-partition write fast path.
- The change must not make `getItem`, `queryItems`, or `transactGetItems` write a read timestamp.
- The change must not replace the two-phase transactional-read protocol with a one-phase protocol.
- The change must not add per-key tombstones.
- The change must not add a logical increment to an item timestamp. Section 5.7 gives the reason.
- The change must not allocate the sub-millisecond digits of a coordinator timestamp. That is a later RFC.
- The change must not change the coordinator state machine, recovery protocol, or idempotency behavior.
- The change must not change TTL visibility or the logical timestamp of TTL deletion.
- The change must not add a data migration. Section 4.2.11 gives the deployment rule.

### 2.3 Requirements that constrain the solution

- Every stored order timestamp must be a JavaScript safe integer. One shared helper creates every order
  timestamp and asserts `Number.isSafeInteger` on it (section 4.2.1).
- The code must use `TX_ORDER_TS_UNITS_PER_MS = 1_000`.
- The code must not use nanosecond epoch values in a JavaScript `number`.
- `data` must remain the last column of `items`.
- Neither item timestamp must join `idx_items_scan`.
- Every item timestamp update must be monotonic. Every writer uses `MAX(stored, incoming)`.
- Every deletion metadata update must be atomic with its delete operation.
- A transaction that rolls back must also roll back its timestamp and revision changes.
- Migration ingest must copy timestamps and must not create new logical mutations.
- Promotion cleanup must not advance deletion metadata.
- A pending `check` must continue to lock writers until its transaction commits or cancels.
- The timestamp unit change must not change TTL, alarm, staleness, or idempotency time units.

---

## 3. Timeline and Milestones

### M1 — Storage, transactions, reads, and migration paths

One change to the whole tree. It delivers a working system at every commit boundary that the review pauses on.

- Add the shared `TX_ORDER_TS_UNITS_PER_MS` constant and the `txOrderTimestampNow()` helper. Both live in `shared/transaction-limits.ts`. Move every
  order-timestamp producer to the helper: the coordinator's `tc_state.transaction_ts`, the single-shot path,
  `apiPutItem`, `apiDeleteItem`, and the `Date.now()` fallback in `debugForceResolveTransaction`. Move the TTL
  sweep watermark to the new unit. Update the `TransactionTimestamp` type comment.
- Separate the participant's wall-clock source from its order-timestamp source. One injected clock currently
  supplies both the staleness age and the single-shot transaction timestamp.
- Change the clock-skew test and the `clock_skew` rejection fields (section 4.2.1).
- Replace the item timestamp column, rename the delete watermark, add `delete_revision` (section 4.2.2).
- Update every reader and writer of the removed columns: `PartitionStore`, `composeConditionStatement` in
  `shared/expression/plan.ts`, `composeUpdateProbeStatement` and both probe result types in
  `shared/expression/runtime.ts`, `TransactionParticipant`, `MigratedItem`, `insertItemIfAbsent`, and
  `GetPartitionTransactionMetadataResult`.
- Apply the prepare and commit rules of sections 4.2.3 and 4.2.4.
- Apply the transactional-read protocol of sections 4.2.6 and 4.2.7 on the two-phase path and the
  single-partition fast path.
- Copy both item timestamps and both deletion metadata values through every migration path (section 4.2.9).

### M2 — Verification and documentation

- Add the unit and integration tests of section 4.2.12.
- Run `pnpm test`, `pnpm check`, and `pnpm build`.
- Update `AGENTS.md` and every other project document that names the removed fields (`last_transaction_ts`,
  `max_deleted_ts`, `lastCommittedTs`, `serverTimestampMs`, `transactionTimestampMs`).

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
```

`last_read_ts >= last_write_ts` holds on every row (section 4.2.4), so the second rule is the current comparison
against the combined timestamp. The first rule accepts an older read after a newer read when no newer write
exists.

The transactional-read protocol compares three mutation signals:

```text
live-row write:        item version
row lifecycle change:  delete_revision
pending mutation:      pending put, update, or delete
```

A pending or committed `check` changes none of these signals.

### 4.2 Technical details

#### 4.2.1 Timestamp representation

Add one shared constant and one shared helper in `shared/`:

```ts
const TX_ORDER_TS_UNITS_PER_MS = 1_000;

function txOrderTimestampNow(): TransactionTimestamp {
    const ts = Date.now() * TX_ORDER_TS_UNITS_PER_MS;
    invariant(Number.isSafeInteger(ts), "order timestamp is not a safe integer");
    return ts;
}
```

The helper is the only producer of an order timestamp. The coordinator calls it for `tc_state.transaction_ts`;
every partition-local writer calls it for its base timestamp. `Date.now() * 1_000` stays below
`Number.MAX_SAFE_INTEGER` until the year 2255, so the assertion guards a broken clock, not normal operation. A
stamp site that calls `Date.now()` directly for an order timestamp is a defect.

No stored value needs an assertion after the write. Every item timestamp is `MAX(stored, created)`, and both
operands are already bounded. `v` keeps its existing `version >= 1` invariant. `delete_revision` grows by one per
user delete and cannot approach the bound.

The low three decimal digits of every timestamp this change creates are zero. A later RFC can allocate them to
coordinator tie-breaking, for example a shard suffix or a per-coordinator counter, without a schema change. Item
stamps do not use them.

A writer applies:

```text
last_write_ts = MAX(last_write_ts, base_timestamp)
last_read_ts  = MAX(last_read_ts,  base_timestamp)
```

An insert sets both to `base_timestamp`.

A `check` applies:

```text
last_read_ts = MAX(last_read_ts, transaction_timestamp)
```

A `check` whose timestamp is below the stored `last_read_ts` leaves it unchanged. The `MAX` lets an older `check`
commit after a newer one without lowering the watermark and without adding to it.

The clock-skew test must compare physical milliseconds:

```text
FLOOR(transaction_timestamp / TX_ORDER_TS_UNITS_PER_MS) <= Date.now() + MAX_CLOCK_SKEW_MS
```

The `clock_skew` rejection renames `serverTimestampMs` and `transactionTimestampMs` to `serverTimestampMicros`
and `transactionTimestampMicros`. Both carry values in the order unit: `serverTimestampMicros` is the partition
wall clock times `TX_ORDER_TS_UNITS_PER_MS`, and `transactionTimestampMicros` is the transaction timestamp. This
is a breaking change to the public rejection shape and is accepted.

The following values remain in their current units:

- `created_at` and `completed_at` values remain in epoch milliseconds.
- Alarm deadlines and recovery ages remain in milliseconds.
- `ttl_epoch_utc_seconds` remains in epoch seconds.
- The idempotency window remains in milliseconds.

A TTL expiry timestamp converts to the order unit with:

```text
ttl_expiry_order_ts = ttl_epoch_utc_seconds * 1_000 * TX_ORDER_TS_UNITS_PER_MS
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
    max_delete_tx_order_ts   INTEGER NOT NULL DEFAULT 0,
    delete_revision       INTEGER NOT NULL DEFAULT 0
) STRICT;
```

`max_delete_tx_order_ts` and `delete_revision` use different update rules. Section 4.2.5 defines those rules.

The change edits the existing initial migrations in place. Section 4.2.11 gives the deployment rule.

#### 4.2.3 Operation matrix

| Operation | Prepare guard on a live row | Timestamp update |
| --- | --- | --- |
| Two-phase put | Above `last_read_ts` | Advance both at commit |
| Two-phase update | Above `last_read_ts` | Advance both at commit |
| Two-phase delete | Above `last_read_ts` | Remove the row at commit |
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

A condition on a put, update, or delete does not change this matrix. Every successful content mutation advances
both item timestamps. This conservative rule preserves the combined timestamp behavior.

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
```

`last_read_ts >= last_write_ts` holds on every row. An insert sets both to the same value. A write applies the
same `MAX(stored, base)` to both, which keeps the order. A `check` raises only `last_read_ts`. Migration copies
both. The write guard therefore does not need a second comparison against `last_write_ts`.

When the item is absent, prepare must retain the current rule:

```text
transaction_timestamp <= max_delete_tx_order_ts
```

A `check` on an absent item must not add another watermark. Locks continue to provide serializability for this
case.

The pending-lock rule does not change for write transactions. A pending `check` remains an exclusive lock against
another write transaction and against a non-transactional writer.

#### 4.2.5 Delete ordering and delete revision

`max_delete_tx_order_ts` replaces `max_deleted_ts` without changing its meaning.

The store updates `max_delete_tx_order_ts` with:

```text
max_delete_tx_order_ts = MAX(max_delete_tx_order_ts, candidate_order_timestamp)
```

The candidate remains specific to each path:

| Delete path | Candidate order timestamp | Update when the row is absent? |
| --- | --- | --- |
| Two-phase transaction | Coordinator transaction timestamp | Yes |
| Single-shot transaction | Partition base timestamp | Yes |
| Non-transactional delete | Partition base timestamp | No |
| TTL sweep | Maximum expiry timestamp in the deleted batch | Not applicable |

`delete_revision` is not an order timestamp. It is a partition-local monotonic counter.

The store increments `delete_revision` after each user delete statement that removes one or more item rows:

```text
delete_revision = delete_revision + 1
```

The following rules apply:

- Each successful single-item delete statement increments the revision once.
- A transaction with multiple successful delete statements can increment the revision more than once.
- A transactional delete of an absent item does not increment the revision.
- A non-transactional delete of an absent item does not increment the revision.
- The TTL sweep does not increment the revision. Section 4.2.8 gives the reason.
- Promotion cleanup does not increment the revision.
- A rolled-back delete does not increment the revision.

The store must update both metadata fields in one statement when a delete changes both. This keeps the existing
metadata row-write count for that path.

The counter can start at zero. The client compares revisions for equality and does not compare them with item
timestamps.

#### 4.2.6 Transactional-read protocol

Replace the internal `lastCommittedTs` field with the revision field:

```ts
type ReadForTransactionItemResultEncoded = {
    // Existing item fields and keys.
    deleteRevision: number;
    hasPendingWrite: boolean;
};
```

Every item result returns the current `delete_revision` of the local owner partition. The participant reads it
through one store method, `deleteRevisionFor(hk)`. In this design the method ignores its parameter and returns
the partition value, so the participant reads it once per local RPC and reuses it for every item. The parameter
is the seam for the bucketed variant of section 5.9, where the participant reads once per distinct bucket. Split
fan-out can return different values for items owned by different children.

The client compares two phase results with:

```text
same key
AND same found state
AND same deleteRevision
AND, when found:
    same version
```

`version` detects every write to a live row, because `v = v + 1` runs on every upsert. `deleteRevision` detects
every user removal of a row, including a delete and recreate that returns `v` to its first value. The item
timestamps are not part of the comparison. Two writes inside one millisecond share an order timestamp and
differ in `v`, so a timestamp adds no signal. The item result does not carry `last_read_ts` or `last_write_ts`.
A later change can add them to the RPC `meta` for debugging; they must not join the comparison.

A different `deleteRevision` causes `read_conflict`, including an unrelated user deletion in the same partition.
This is a conservative false conflict, and it is a new abort source: today an unrelated deletion never aborts a
read, because it does not touch the read item's version or timestamp. Section 4.2.10 gives the expected rate.

The client returns the phase-one values after every comparison succeeds. It strips `deleteRevision` and
`hasPendingWrite` at the public boundary.

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

The revision comparison detects these row-lifecycle sequences when a user delete removes the row:

```text
found(v=1) -> delete -> recreate(v=1) -> found
absent     -> create -> delete        -> absent
```

Each sequence contains a user row removal, so `delete_revision` changes. The comparison fails even when the item
version returns to its first value.

The TTL sweep is physical reclamation, not a logical mutation. The logical deletion of an expired item happens at
its expiry instant, which is a function of the row's own data. The TTL contract already permits a reader to see
an expired item until the sweep removes it. The sweep therefore uses its expiry timestamp for
`max_delete_tx_order_ts`, as it does today, and does not touch `delete_revision`.

The two sequences above with a TTL removal in place of the user delete:

- `absent -> create -> sweep -> absent`. If the created item was expired when written, it was logically absent
  the whole time and the phase-one snapshot is valid. If it expired inside the read window, the state
  `{item expired, other items as read}` exists from the later of the expiry and the other writes until phase two,
  so the snapshot is valid.
- `found(v=1, ts=T) -> sweep -> recreate(v=1, ts=T)`. The recreate must land in the same millisecond `T` as the
  original create, with a sweep chunk between them. The sweep timer starts at least 500 ms after the RPC that arms
  it, so this needs a create, a sweep, and a recreate inside one millisecond. This is a documented limit.

A delete of an absent item can advance `max_delete_tx_order_ts` without advancing `delete_revision`. The delete
orders a write transaction but does not change readable item state.

#### 4.2.9 Split and promotion migration

`MigratedItem` must replace `last_transaction_ts` with both item timestamps. `queryItemsPage` and
`queryRangeItemsPage` must select both values. `insertItemIfAbsent` must write both values without modification.

`GetPartitionTransactionMetadataResult` must replace `maxDeletedTs` with:

```ts
{
    maxDeleteTxOrderTs: number;
    deleteRevision: number;
    pendingTransactions: PendingTransactionRow[];
    nextCursor: PendingTransactionCursor | null;
}
```

Each child must merge the metadata with `MAX`:

```text
child.max_delete_tx_order_ts = MAX(child value, parent value)
child.delete_revision     = MAX(child value, parent value)
```

The parent can return the same metadata values on each pending-lock page. The merge is idempotent.

Ownership of a key moves only from a parent to a child. The parent stops mutating its rows at `split_started`:
writes forward to children, the TTL sweep and the stale-transaction sweep stop, and the child pulls the metadata
after that point. The child therefore starts with a revision that is not below any value the parent returned to a
phase-one read, and every later user delete on the child raises it. A read whose phases straddle the handoff
either passes correctly or aborts conservatively; it cannot miss a removal.

Migration ingest must not advance `delete_revision`. Promotion cleanup removes obsolete parent copies and must not
advance it. These operations move ownership and do not change logical item state.

A child in `migration_migrating` continues to reject write and transaction RPCs. This guard prevents a local
mutation before the child receives the source timestamps and revisions.

#### 4.2.10 Atomicity, failure behavior, and performance

Every existing storage transaction must include its timestamp and revision updates. A successful response must not
leave a content mutation without its revisions.

The two-phase participant preserves its current write-ahead protocol. Prepare stores the lock and operation.
Commit applies the operation, advances the revisions, and removes the lock in one local storage transaction.
Commit contains no new assertion and no new rejection: every value it writes is a `MAX` of bounded operands or a
counter increment.

Commit retry behavior does not change. A retry that finds no pending rows returns `committed` and must not advance a
revision again.

A rollback restores a deleted row and the previous `delete_revision`.

The recovery path continues to call the public commit and cancel methods. These methods retain migration guards
and split routing.

Storage. Each item gains one integer column because one current column becomes two. The column does not join an
index. `estRowBytesExpr`'s fixed overhead (`EST_ROW_BYTES_K`) and `estimateItemBytes` count the integer columns
of the row. Both must grow by one column so that `key_size_estimates`, the split and promotion thresholds, and
the migration batch byte budget do not drift low.

Row writes. A put, update, or `check` continues to update one item row. An actual delete already updates the
deletion metadata row; updating `max_delete_tx_order_ts` and `delete_revision` in one statement keeps that at one
row write.

Row reads. Each two-phase transactional-read RPC adds one deletion metadata row read per local owner partition. A
read over N owner partitions adds 2N metadata row reads across both phases. The implementation must not read the
metadata once per item. No path adds a `RETURNING` read.

False aborts. With D user deletes per second on a partition and a gap of G seconds between the two phases, a
two-phase read that touches that partition aborts with probability about `1 - e^(-D*G)`. At 10 deletes/s and a
20 ms gap that is about 18%; at 1 delete/s it is about 2%. The TTL sweep adds nothing to D. Section 5.9 gives
the follow-up that divides D by a constant.

Cloudflare bills SQLite storage by rows read and rows written. Updating one physical SQLite page does not determine
the billed count. The change must use the SQL cursor metrics to verify the expected counts.

TODO: benchmark the transactional-read latency and abort rate under user deletion load.

#### 4.2.11 Deployment and rollback

The project is before its first release. The change edits the initial schema migrations in place: the `items`
table definition, the `deletion_metadata` table definition, and its seed row.

There is no data migration. Every existing environment with persisted partition or coordinator state is
discarded and redeployed. Stored millisecond timestamps, `last_transaction_ts` columns, and `max_deleted_ts`
columns are not converted.

An in-flight transaction must use one unit on the coordinator and every participant. The release must ship the
coordinator, partition, RPC type, and schema changes in one Worker version.

A code-only rollback is not safe after the new schema has written rows. Rollback is a redeploy from a clean state.

#### 4.2.12 Tests and verification

The store tests must prove:

- Every put and update path advances both item timestamps with `MAX`, and never lowers either.
- A `check` advances only `last_read_ts`.
- An out-of-order `check` does not decrease `last_read_ts`.
- `last_read_ts >= last_write_ts` after every writer, including a `check` on a row a lagging clock wrote.
- A real user delete increments `delete_revision` once and updates both metadata fields in one statement.
- A TTL batch does not change `delete_revision` and advances `max_delete_tx_order_ts` to the largest expiry in
  the order unit.
- An absent delete does not increment `delete_revision`.
- A transactional absent delete still advances `max_delete_tx_order_ts`.
- Promotion cleanup changes neither deletion metadata value.
- Every created order timestamp is a safe integer and a multiple of `TX_ORDER_TS_UNITS_PER_MS`.

The participant tests must prove:

- A `check` above `last_write_ts` succeeds when it is below `last_read_ts`.
- A `check` at or below `last_write_ts` fails with `timestamp_conflict`.
- A write at or below `last_read_ts` fails with `timestamp_conflict`.
- The clock-skew test compares physical milliseconds and reports both fields in the order unit.
- A pending `check` does not set `hasPendingWrite`.
- A pending put, update, or delete sets `hasPendingWrite`.
- An unknown pending operation value sets `hasPendingWrite`.
- The first commit advances the expected timestamps and revisions.
- A commit retry does not advance a timestamp or revision.

The transactional-read tests must prove:

- A committed `check` between phases does not cause `read_conflict`.
- A pending `check` during a phase does not cause `pending_write`.
- A pending content mutation still causes `pending_write`.
- A normal write between phases causes `read_conflict`.
- A delete and recreate with the same version causes `read_conflict`.
- An absent-create-delete sequence causes `read_conflict`.
- An unrelated user deletion in the same partition causes the documented conservative `read_conflict`.
- A TTL sweep between phases does not cause `read_conflict`.
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

### 5.2 Use one delete timestamp for ordering and revision

Make `max_delete_tx_order_ts` advance after every delete and return it as the absent-item revision.

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

### 5.4 Add a per-key tombstone

Keep a revision row for each deleted key.

This option detects an absent-item lifecycle without conflicts from unrelated deletes. It adds storage for deleted
keys and needs a safe garbage-collection rule. A tombstone cannot expire while an older transactional read or
transaction timestamp can still require it.

The partition-wide `delete_revision` uses constant storage and accepts conservative read conflicts.

### 5.5 Use nanosecond epoch values

Store `Date.now() * 1_000_000` in each timestamp.

The current epoch value exceeds `Number.MAX_SAFE_INTEGER`. The Workers SQL API returns numeric values as JavaScript
numbers, so the low-order digits lose precision. Microsecond-shaped values remain safe until the year 2255.

### 5.6 Write a read timestamp from every read

Advance `last_read_ts` for `getItem`, query operations, and `transactGetItems`.

This option can support a one-phase timestamp-ordering read protocol. It turns persistent, replicated reads into
writes. The current two-phase protocol avoids that write cost and remains in scope.

### 5.7 Add a logical increment to the item write timestamp

Stamp a write to an existing item with `MAX(base_timestamp, last_write_ts + 1)` so that two writes in one
millisecond receive different `last_write_ts` values.

The increment changes no decision. A coordinator timestamp is a multiple of `TX_ORDER_TS_UNITS_PER_MS`, so
`T * 1000 <= S * 1000 + k` with `0 <= k <= 999` is true exactly when `T <= S`; the logical part never changes a
prepare outcome. The two-phase read already detects every same-millisecond write through `v`, and every delete
and recreate through `delete_revision`.

The increment also has a cost. Above 1,000 writes per second on one item it moves the stamp ahead of wall time
and keeps it there while the rate holds, so every coordinator transaction on that item fails with
`timestamp_conflict`. Bounding the increment inside the current millisecond removes the runaway but leaves an
increment that changes nothing. Each incrementing statement would also need a `RETURNING` read and a safe-integer
assertion, and an assertion that fails at commit leaves a PREPARED transaction unfinishable.

### 5.8 Keep a delete revision per hash key on `key_size_estimates`

Add a `delete_revision` column to the existing per-hash-key row and read one row per distinct hash key.

This scopes a false abort to deletes under the read's own hash keys. It depends on the invariant that a
`key_size_estimates` row is removed only when the partition stops serving that key. A future space-reclaim job
that removes rows of empty hash keys on a serving partition would reset the revision to zero and reopen the
delete-and-recreate miss. The migration would also need to carry every `(hk, delete_revision)` pair to the child.

### 5.9 Bucketed delete revisions (follow-up)

Keep N revision rows (for example N = 256) in `deletion_metadata`, keyed by `hash(hk) mod N`. A user delete
increments its bucket. A read reads one row per distinct bucket it touches. Migration merges N values with `MAX`.

This divides the false-abort rate of section 4.2.10 by N with constant storage and no garbage-collection
constraint. It costs one hash per deleted item and one small fixed table. The TTL sweep is excluded from the
revision in both designs, so buckets add nothing to the sweep.

The partition-wide design of this RFC is built so that this follow-up touches three places: the store method that
reads the revision for a hash key (section 4.2.6), the `WHERE id = ?` of the metadata update statement, and the
metadata migration page, which returns N values instead of one. The wire shape of the item result and the client
comparison do not change.

### 5.10 Advance `delete_revision` from the TTL sweep

Increment the revision once per sweep chunk that removes rows.

`PartitionDO` arms the sweep on every RPC with a 500 ms delay. A sweep cycle removes up to 10,000 rows in chunks
of 100 with a zero-delay yield between chunks, and each chunk is one storage transaction. On a partition with a
steady flow of expiring rows, every two-phase read whose phases straddle a chunk boundary aborts with
`read_conflict`, for present and absent items alike, and the client cannot control the source. Section 4.2.8
gives the reason the sweep is not a logical mutation and the one unreachable case that excluding it gives up.

---

## 6. Frequently Asked Questions

### Why does every put and update advance `last_read_ts`?

The rule preserves the current combined watermark. A future write must remain above every earlier content mutation
and every earlier `check`. The design does not use the Thomas Write Rule.

### Why does a `check` compare only with `last_write_ts`?

A check reads the current item and does not change it. A newer read does not invalidate that value. A newer write
means that the check would read a value from after its timestamp, so prepare rejects it.

### Why does a write compare only with `last_read_ts`?

`last_read_ts >= last_write_ts` holds on every row (section 4.2.4). A comparison against `last_write_ts` can
never reject a write that the comparison against `last_read_ts` accepted.

### Why does a pending `check` still block writers?

Prepare evaluated the check condition against the current item. A writer could invalidate that condition before
the transaction commits. The lock keeps the checked value stable.

### Why can a transactional read ignore a pending `check`?

The pending check cannot change the item. A read of that item can serialize before or after the check. A pending
content mutation on another requested item still aborts the transactional read.

### Why does every transactional-read item carry `deleteRevision`?

A live item can be deleted and recreated between phases. The recreated row can repeat the old version. The
partition revision proves that a user row removal occurred during the read.

### Why is the item timestamp not part of the read comparison?

`v` changes on every write to a live row, including two writes in one millisecond, which share a timestamp.
`delete_revision` covers the one case where `v` repeats. The timestamp adds no signal.

### Why is `delete_revision` separate from `max_delete_tx_order_ts`?

The order timestamp can stay unchanged when an older deletion occurs. The revision must change after every user
row removal. An absent transactional delete advances the order timestamp but does not change readable state.

### Why does the TTL sweep not advance `delete_revision`?

The sweep reclaims rows whose logical deletion already happened at their expiry instant. Section 4.2.8 shows that
the snapshot stays valid without the revision change, and section 5.10 shows the abort storm the change would
cause.

### Why does an unrelated delete abort a transactional read?

Every item result carries one partition-wide delete revision. The read cannot identify which key changed that
revision. It aborts rather than miss a delete-and-recreate sequence on the requested key. Section 5.9 is the
follow-up that narrows the scope.

### Why use microsecond-shaped values when Workers exposes a millisecond clock?

The low three decimal digits are headroom. A later coordinator tie-breaking scheme can put a shard suffix or a
per-coordinator counter there without a schema change, which is expensive after the first release. Item stamps
do not use the digits, and no decision in this RFC depends on them.

### Does the change expose timestamps or revisions in the public API?

No. These values remain internal RPC bookkeeping. `FokosDB.transactGetItems` strips them before it returns. The
one public change is the `clock_skew` rejection field rename in section 4.2.1.

---

## 7. References

- `packages/fokosdb/src/shared/partition/partition-store.ts`
- `packages/fokosdb/src/shared/partition/transaction-participant.ts`
- `packages/fokosdb/src/shared/partition/migration.ts`
- `packages/fokosdb/src/shared/partition/partition-peer.ts`
- `packages/fokosdb/src/shared/partition/ttl-expiry.ts`
- `packages/fokosdb/src/shared/expression/plan.ts`
- `packages/fokosdb/src/shared/expression/runtime.ts`
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
