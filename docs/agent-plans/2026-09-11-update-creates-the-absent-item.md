# RFC — An update in transactWriteItems creates the absent item

**State:** Draft
**Date:** 2026-09-11
**Author:** Lambros

---

## Table of Contents

1. [Overview and Context](#1-overview-and-context)
2. [Goals and Requirements](#2-goals-and-requirements)
3. [Milestones](#3-milestones)
4. [Proposed Solution](#4-proposed-solution)
5. [Test Plan](#5-test-plan)
6. [Alternative Options](#6-alternative-options)
7. [Frequently Asked Questions](#7-frequently-asked-questions)
8. [References](#8-references)

---

## 1. Overview and Context

### 1.1 The problem

The `update` operation of `FokosDB.transactWriteItems` applies only to an item that already exists.
When the item is absent, the partition rejects the operation with `update_not_applicable`, and the
whole transaction cancels.

DynamoDB `UpdateItem` behaves differently. It creates the item when the item is absent, unless a
condition expression refuses. The counter idiom depends on this: `SET n = if_not_exists(n, 0) + 1`
must work on the first call, when no item exists yet.

The current behaviour makes the caller do a `put` first, or retry with a `put` after the cancel.
Both cost a round trip, and neither is atomic with the rest of the transaction.

`README.md` records this as an open task.

### 1.2 Where the current behaviour is decided

Three modules hold the rule:

1. **The compiler.** `compileUpdateExpression` in
   `packages/fokosdb/src/shared/expression/compiler.ts:113` starts the document expression at the
   stored column, `let accumulator = "i.data"`, and puts `(i.hk IS NOT NULL)` and
   `(i.data_kind = <json>)` into `applicableSql` as its first two terms. Every value renderer
   (`referencePresent`, `referenceType`, `referenceValue`) reads the same column and reports
   `missing` when the row is absent.
2. **The probe.** `composeUpdateProbeStatement` in
   `packages/fokosdb/src/shared/expression/runtime.ts:122` runs `applicableSql` over a `LEFT JOIN`,
   so an absent row gives `applicable = 0`.
3. **The two write statements.** `PartitionStore.insertPendingUpdateLock` selects `FROM items`, so
   an absent row writes no pending lock. `PartitionStore.updateItemSingleShot` reads the stored size
   first and throws `item_not_found_for_update` when the row is absent.

`TransactionParticipant.#precheckWrite` turns `applicable = 0` into the `update_not_applicable`
rejection, on both the two-phase path and the single-shot path.

### 1.3 What already supports creation

The commit half of the two-phase path needs no change. `TransactionParticipant.#applyCommitItems`
applies an `update` with `PartitionStore.upsertItem`, the same call a `put` uses, and that statement
is an upsert. Once prepare writes a pending row for an absent item, commit creates the item.

---

## 2. Goals and Requirements

### 2.1 In scope

1. An `update` operation applies to an absent item. It creates the item.
2. The pre-image of an absent item is the empty JSON document, `{}`. The created item has
   `data_kind = json`, `v = 1`, and the TTL of the operation, or no TTL when the operation sets none.
3. A condition on the operation decides, exactly as it does today. A condition that needs the item
   (`{ op: "exists", args: [{ ref: "hashKey" }] }`) fails on an absent item and cancels the
   transaction. No code reads the condition to decide whether creation is allowed.
4. Both write paths behave the same: the two-phase path (`prepare` then `commit`) and the
   single-shot path (`executeSingleShot`).
5. An item that exists with `data_kind` of `text` or `bytes` still rejects with
   `update_not_applicable`. Only an absent item gains the new behaviour.
6. Every other applicability rule stays: a `set` on a missing parent rejects, a missing operand
   rejects, a value that is bytes rejects with `update_value_is_bytes`, and the result must stay an
   array or an object.
7. The size limit, the timestamp ordering, the deletion watermark, the split routing, the migration
   guard, the recovery paths and the idempotency window keep their behaviour.

### 2.2 Out of scope

| Item | Reason |
| --- | --- |
| A non-transactional `updateItem` | The operation does not exist yet. |
| A base document other than `{}` | An item created by `put` can hold any kind. An update writes paths, so it needs an object. |
| Return values (`ALL_NEW`, `UPDATED_NEW`) | `TransactWriteItems` returns none, as today. |
| An opt-out flag on the operation | A condition is the opt-out. Section 6.1 gives the reason. |

### 2.3 Constraints

- A partition must not load the document into JavaScript. Every test and every write stays in SQL.
- Prepare must not accept a write that commit cannot apply. Every rejection stays in the check pass.
- The compiled plan keeps its shape and its JSON form. `CompiledUpdatePlan` gains no field.
- The canonical identity of an update expression does not change. It is computed over the AST, in
  `canonicalUpdateIdentity`, so idempotency of a retried `clientRequestToken` is unaffected.
- The condition compiler shares `referencePresent`, `referenceType` and `referenceValue` with the
  update compiler. A condition must keep its current answers for an absent item.

---

## 3. Milestones

| Milestone | Content |
| --- | --- |
| M1 | The compiler renders an update against a pre-image that exists for an absent row. |
| M2 | `PartitionStore` writes both update statements as upserts. |
| M3 | The participant, the error table and the comments follow. Tests and documents follow. |

Each milestone is a complete change. M1 alone changes no behaviour, because the store still refuses
an absent row.

---

## 4. Proposed Solution

### 4.1 The pre-image of an absent item

One definition holds the change:

| Name | SQL | Meaning |
| --- | --- | --- |
| Pre-image document | `COALESCE(i.data, jsonb('{}'))` | The document the update reads and writes. |
| Pre-image is JSON | `(i.hk IS NULL OR i.data_kind = <json>)` | The update may apply to this row. |

`items.data` is `NOT NULL`, so a NULL value in either expression means one thing only: the row is
absent. Every statement that runs an update plan must therefore make the row absent rather than make
no row at all, which section 4.3 covers.

These two expressions replace `i.data` and the pair `(i.hk IS NOT NULL) AND (i.data_kind = <json>)`
in the update compiler. They must NOT replace them in the condition compiler.

### 4.2 The compiler

`CompileContext` gains the two expressions above, with the current values as the default. The update
compiler sets the new values; the condition compiler keeps the defaults. Every renderer then reads
the context instead of naming the column.

The sites to change in `packages/fokosdb/src/shared/expression/compiler.ts`:

| Site | Today | With creation |
| --- | --- | --- |
| `compileUpdateExpression`, the accumulator | `i.data` | The pre-image document |
| `compileUpdateExpression`, the first two applicable terms | `(i.hk IS NOT NULL)`, `(i.data_kind = <json>)` | One term: the pre-image is JSON |
| `targetGuardSql` | `json_type(i.data, ...)` | `json_type(<pre-image>, ...)` |
| `referencePresent` for `data` at a path | `i.hk IS NOT NULL AND i.data_kind = <json>` guard | The pre-image is JSON guard, over the pre-image |
| `referencePresent` for the whole `data` | `(i.hk IS NOT NULL)` | `1` — the pre-image always exists |
| `referenceType` for `data`, both forms | Reads the column, `missing` when absent | Reads the pre-image |
| `referenceValue` for `data`, both forms | Reads the column | Reads the pre-image |

`referencePresent` handles `hashKey`, `v` and `data` in one fall-through branch. The `data` case
must leave that branch, because only `data` changes.

`v`, `ttlAt`, `hashKey` and `sortKey` keep their current answer, which is `missing` for an absent
row. Section 6.2 holds the option to resolve the two key references from the bound keys.

The consequence for a caller is exact and testable:

| Update on an absent item | Result |
| --- | --- |
| `set $.a = 1` | Creates `{"a":1}`. The parent of `$.a` is `$`, and `{}` is an object. |
| `set $.a = if_not_exists($.a, 0) + 1` | Creates `{"a":1}`. The counter idiom works on the first call. |
| `set $.a.b = 1` | Rejects. The parent `$.a` does not exist. DynamoDB rejects the same shape. |
| `set $[#] = 1` | Rejects. The base document is an object, not an array. |
| `remove $.a` | Creates `{}`. `jsonb_remove` on a missing path makes no change. |
| A value that reads `$.x` without `if_not_exists` | Rejects. The operand is missing. |
| A value that reads `v` or `ttlAt` | Rejects. There is no version and no TTL yet. |

### 4.3 The three statements that run a plan

**The probe**, `composeUpdateProbeStatement`. It already joins with `LEFT JOIN`, so `applicableSql`
and the size expression answer for an absent row without a change. One guard needs the new form:
`value_type_ok` is wrapped in `CASE WHEN i.hk IS NOT NULL AND i.data_kind = <json> THEN ... ELSE 1
END`, which must become the pre-image-is-JSON test. `item_present` keeps its meaning and the
participant keeps reading it.

**The pending lock**, `PartitionStore.insertPendingUpdateLock`. Change the source from
`FROM items AS i WHERE i.hk = ?1 AND i.sk = ?2` to a one-row source with a `LEFT JOIN`:

```sql
FROM (VALUES (1)) LEFT JOIN items AS i ON i.hk = ?1 AND i.sk = ?2
```

The statement then always writes one pending row. The row already carries `operation = 'update'` and
the json kind code, and it holds the materialized JSONB document, so commit stays a plain upsert.
The TTL branch is unchanged: `i.ttl_epoch_utc_seconds` is NULL for an absent row, which is the right
value for a created item.

This change is required, not optional. `commitLocal` compares the pending key set with the commit
request key set and raises `commit_keyset_mismatch` when they differ. A prepare that accepts an item
and writes no lock for it would fail the whole transaction at commit.

**The single-shot write**, `PartitionStore.updateItemSingleShot`. Two changes:

1. Delete the `item_not_found_for_update` throw. The stored size of an absent row is 0, and 0 is
   already the correct old value for the `key_size_estimates` delta.
2. Replace the `UPDATE items` statement with an upsert over the same `LEFT JOIN` source, in the
   shape `PartitionStore.upsertItem` already uses:

```sql
INSERT INTO items (hk, sk, data_kind, ttl_epoch_utc_seconds, v, last_transaction_ts, est_row_bytes, data)
SELECT ?1, ?2, <json code>, <ttl expression>, 1, <ts>, <est_row_bytes>, <document>
FROM (VALUES (1)) LEFT JOIN items AS i ON i.hk = ?1 AND i.sk = ?2
WHERE <est_row_bytes> <= <limit>
ON CONFLICT(hk, sk) DO UPDATE SET
  data = excluded.data,
  data_kind = excluded.data_kind,
  ttl_epoch_utc_seconds = excluded.ttl_epoch_utc_seconds,
  est_row_bytes = excluded.est_row_bytes,
  v = v + 1,
  last_transaction_ts = MAX(last_transaction_ts, excluded.last_transaction_ts)
RETURNING v, est_row_bytes
```

The `WHERE` clause keeps the size guard: when it removes the source row, the statement writes
nothing and returns no row, and the existing `throwItemTooLarge` answers. The TTL expression keeps
its two branches — the bound value when the operation sets one, `i.ttl_epoch_utc_seconds` when it
does not — and the `LEFT JOIN` makes the second branch NULL for a created item.

The statement adds one parameter to the tail (the json kind code stays interpolated, the timestamp
and the TTL already bind), so check the tail against `UPDATE_MAX_TRAILING_BINDING_COUNT` in
`plan.ts`. `StatementTail` raises when a statement binds a wider tail than the compiler charged.

### 4.4 The participant

`TransactionParticipant` needs no logic change, but two comments become wrong:

- `#precheckWrite` lists "a missing item" as a cause of `update_not_applicable`. Remove it.
- `prepareLocal` says "An update never reaches it today, because an update applies only to an item
  that exists" over the deletion-watermark branch. An update of an absent item now reaches that
  branch, and the branch is already correct: with no live item, `max_deleted_ts` is the only
  ordering signal, exactly as it is for a `put` of an absent item.

The rest follows without a change:

- The probe returns `itemPresent = false` for a created item, so `prepareLocal` takes the
  watermark branch and rejects with `timestamp_conflict` when a newer delete wins.
- `#imageForFailedCondition` returns no image when the item is absent, which is correct.
- `onItemUpserted` receives the new key size estimate from both paths, so promotion accounting and
  split accounting stay correct for a created item.

### 4.5 Errors

`INTERNAL_CODES.item_not_found_for_update` loses its only raise site. Remove the code from
`packages/fokosdb/src/shared/errors.ts` and from the table in
`docs/agent-plans/2026-09-10-structured-error-handling.md`. Do not reuse its `error_id` segment.

`update_not_applicable` keeps its meaning, with one cause fewer.

### 4.6 The SQLite behaviour this rests on

Measured in the Workers SQLite runtime, with a scratch test over real Durable Object storage:

| Question | Result |
| --- | --- |
| Does `AND` short-circuit, so `json_type(COALESCE(i.data, jsonb('{}')), '$')` never runs for a `text` row? | Yes. The guarded expression returns 0 and raises nothing. |
| Does `jsonb_set(jsonb('{}'), '$.a', 5)` work, and does `json_type` read the result? | Yes. `{"a":5}`, of type `object`. |
| Does `jsonb_remove(jsonb('{}'), '$.a')` work? | Yes. `{}`, unchanged. |
| Is `json_type(jsonb('{}'), '$.a')` NULL, so a nested target rejects? | Yes. |
| Does an upsert whose source is `(VALUES (1)) LEFT JOIN items` insert, and then update on the second call? | Yes. `v = 1`, then `v = 2`, with the document stored. |
| Does the pending insert over the same source write a row for an absent item, with a NULL TTL? | Yes. |

The upsert reads the table it writes. SQLite allows this for a single source row, and the measured
result above is the proof for this statement shape.

---

## 5. Test Plan

### 5.1 Tests that change

| Test | Change |
| --- | --- |
| `partition/transaction-participant.test.ts:194` "rejects ... when item is missing or not json" | Split. The missing item now creates. The text item still rejects. |
| `partition/transaction-participant.test.ts:622` single-shot missing item | The item is created. |
| `partition/partition-store.test.ts:461` `item_not_found_for_update` | The statement creates the row. |
| `test/transactions/tx-update-expressions.test.ts:106` missing, text, bytes | Split the missing case out. |
| `expression/compiler.test.ts:253` `applicableSql` contains `i.hk IS NOT NULL` | Assert the new guard. |

### 5.2 Tests to add

1. An update of an absent item creates it, with `v = 1` and the expected document, on the two-phase
   path and on the single-shot path.
2. `set $.n = if_not_exists($.n, 0) + 1` on an absent item creates `{"n":1}`, and a second
   transaction makes it 2.
3. A condition `{ op: "exists", args: [{ ref: "hashKey" }] }` cancels the transaction with
   `condition_failed` on an absent item, and the item stays absent.
4. A condition `not_exists` on the hash key passes, and the item is created.
5. `set $.a.b` on an absent item rejects with `update_not_applicable`.
6. A `remove`-only update of an absent item creates `{}`.
7. `ttlAt` on the operation reaches the created item; without it the created item has no TTL.
8. An update of an absent item whose value reads `v` rejects.
9. An update that creates an item larger than `MAX_ITEM_BYTES` rejects with `item_too_large` and
   writes nothing.
10. An update of a key that was deleted by a later transaction rejects with `timestamp_conflict`
    through the deletion watermark.
11. A condition expression still reports an absent item as missing. One existing condition fixture
    over an absent item is enough to hold the boundary.
12. A multi-partition transaction that creates one item and updates one existing item commits both.

---

## 6. Alternative Options

### 6.1 An explicit flag on the operation

An `upsert: true` field on the operation would keep the current default. It is not chosen: DynamoDB
has no such flag, the condition already expresses both intents, and a second way to say "the item
must exist" makes two sources of truth for one rule.

### 6.2 Key references in a value of a created item

A value may read `hashKey` or `sortKey`. For an absent row both render from `i.hk` and `i.sk`, so
both are `missing`, and an update that needs one cannot create the item.

Every statement that runs an update plan binds the keys at `?1` and `?2`, which is what
`UPDATE_FIXED_BINDING_COUNT` fixes, so the update compiler could render the two references from the
parameters instead of the columns. The value is then the same for a created item and for an existing
one.

This is a separate change with its own risk, and the counter idiom does not need it. Keep it out of
the first milestone, and add it when a caller asks.

### 6.3 A base document chosen by the caller

The empty object is the only base this RFC defines. A caller who needs an array-rooted item, or any
non-JSON kind, uses a `put` with a `not_exists` condition. DynamoDB items are always maps, so `{}`
is the shape that matches.

---

## 7. Frequently Asked Questions

**Does the implementation read the condition to decide whether to create?**
No. The condition is evaluated on the pre-image, as it is today. An absent item fails a condition
that needs the item, and the transaction cancels before the update applies. Nothing analyses the
condition expression.

**Does commit change?**
No. Commit already applies an `update` with `upsertItem`, which inserts when the row is absent. Only
prepare and the single-shot write need a change.

**What happens to a transaction that was prepared before the deploy?**
Its pending row already holds the materialized document, so commit applies it unchanged. A
coordinator that re-sends a stored plan after the deploy prepares it with the old compiled SQL,
which stays valid SQL and keeps the old answer. The product is not released, so no plan version bump
is needed.

**Why not measure the new size in JavaScript?**
The size prepare measures must be the size commit writes. Both come from the same SQL expression
over the same pre-image. A JavaScript measure would drift from the stored JSONB.

---

## 8. References

- `docs/agent-plans/2026-09-02-update-expressions.md` — the update expression engine. Section 2.1
  item 5, the applicability table of section 4.2.7, and the first FAQ entry state the rule this RFC
  changes. Update all three when this ships.
- `docs/agent-plans/2026-09-10-structured-error-handling.md` — the error code tables.
- `README.md` — the open task that this RFC answers.
- [Amazon DynamoDB `UpdateItem`](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html)
