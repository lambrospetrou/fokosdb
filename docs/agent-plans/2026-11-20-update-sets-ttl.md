# RFC — An update action sets or removes the TTL of an item

**State:** Draft
**Date:** 2026-11-20
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

In DynamoDB, the TTL is an ordinary number attribute that holds epoch seconds. The table settings give its name.
An update can change it with an expression, for example `SET expiresAt = expiresAt + :d`.

FokosDB keeps the TTL in its own column, `ttl_epoch_utc_seconds`, and not in the document. An `update` operation
can set the TTL only with the `ttlAt` field of the operation, which is a literal number. To extend a TTL, the
client must read the item first, compute the new value, and then write it with a condition on `v`. This costs one
more round trip, and the write fails when another writer changes the item between the read and the write.

`docs/agent-plans/2026-09-02-update-expressions.md` §2.2 keeps an action on `ttlAt` out of scope ("`ttlAt` stays
an operation field"). This RFC replaces that line.

### 1.2 What exists today

- `{ ref: "ttlAt" }` is already a value that a condition and an update value can read (`ttlValue` in
  `shared/expression/operation-registry.ts`). It is `missing` when the item has no TTL. On an absent item, an
  update value that reads `ttlAt` rejects (`docs/agent-plans/2026-09-11-update-creates-the-absent-item.md` §4.2).
- `UpdateTarget` in `shared/expression/types.ts` is `{ ref: "data"; path: string }` only. `analyzeUpdate` in
  `shared/expression/semantic.ts` rejects every other target.
- Both write statements of an update have a slot for the TTL as one SQL fragment:
  - `PartitionStore.updateItemSingleShot` passes `ttlSql` to `composeUpdateWriteStatement`
    (`shared/expression/plan.ts`).
  - `PartitionStore.insertPendingUpdateLock` puts `ttlExpr` into the lock row.

  The fragment is a bound literal when the operation has `ttlAt`, and `UPDATE_STORED_TTL_SQL`
  (`i.ttl_epoch_utc_seconds`) when it has none.
- The write statement returns `ttl_epoch_utc_seconds`, and `updateItemSingleShot` gives that value to the item
  stats facts. A TTL that SQL computes therefore keeps the facts correct.
- `item_bytes` does not include the TTL. A computed TTL does not change the size check of the probe.
- `TransactionParticipant.precheck` runs `probeUpdate`. When the update does not apply, it answers
  `update_value_is_bytes` if `valueTypeSql` failed, and `update_not_applicable` for every other cause.
- The coordinator stores the update tree in `tc_items.update_json`, and the fingerprint covers the tree.

## 2. Goals and Requirements

### 2.1 In scope

1. An update action can target the TTL:

   ```ts
   { action: "set",    target: { ref: "ttlAt" }, value: <ExpressionValue> }
   { action: "remove", target: { ref: "ttlAt" } }
   ```

2. `set ttlAt` evaluates its value on the pre-image, the same as every other `set` value. Thus
   `set ttlAt = ttlAt + 3600` reads the stored TTL.
3. `remove ttlAt` writes `NULL` to `ttl_epoch_utc_seconds`. On an item with no TTL, it makes no change to the TTL.
4. The value of `set ttlAt` must be an integer greater than zero. This is the rule of `validateTtlAt` for the
   literal field.
5. When the value is `missing`, the update rejects with `update_not_applicable`. This is the current rule for a
   `set` value with a missing operand. The caller can write `if_not_exists(ttlAt, :now) + :d` instead.
6. When the value is present and is not an integer greater than zero, the update rejects with the new code
   `update_ttl_invalid`. The update never removes the TTL silently, and never keeps the old TTL silently.
7. Both write paths support the action: the single-shot path and the two-phase path.

### 2.2 Out of scope

| Item | Reason |
| --- | --- |
| A table setting that copies an attribute into the TTL | Section 6.1. |
| The DynamoDB rule that ignores an invalid TTL value | Section 6.2. |
| A cast from text to number | Section 6.3. |
| An update of the TTL of a bytes or text item | An update needs a JSON document. Use a `put`. |

### 2.3 Requirements that constrain the solution

- `db.ts` and the partition must reject an operation that has the `ttlAt` field and also a `ttlAt` action.
- An update must hold at most one action that targets `ttlAt`.
- The compiled SQL and the bindings stay inside the current limits (`sql_limit`). The TTL fragment counts in the
  SQL size and in the binding count of the widest statement.
- `db.ts` validates the tree and does not compile it. Only a partition compiles.

## 3. Milestones

1. **The TTL target.** The type, the validation, the compiler, both write statements, the precheck, the client
   validation, and the tests of section 5. This milestone delivers the whole feature.
2. **The documents.** Update `README.md` and the public type documentation of `UpdateAction`.

## 4. Proposed Solution

### 4.1 High-level overview

An update action can now set or remove the TTL of the item. The partition compiles the `set` value into one SQL
fragment and puts it into the slot that holds the literal `ttlAt` today. The value reads the stored item before
the update, as every other `set` value does.

The fragment checks its own value. When the value is an integer greater than zero, the write stores it. When the
value is missing or is not valid, the update does not apply, and the probe tells the two causes apart. A `remove`
action puts `NULL` into the slot.

```
update actions ──► compileUpdateExpression ──► documentSql   (data actions)
                                           └─► ttlSql        (ttlAt action, or none)
                                           └─► applicableSql (+ the TTL terms)

ttlSql:  no ttlAt action, no ttlAt field  → i.ttl_epoch_utc_seconds
         ttlAt field                       → bound literal (unchanged)
         set ttlAt = <expr>                → <compiled expr>
         remove ttlAt                      → NULL
```

**Example 1: extend the TTL by one hour.** The item has `ttlAt: 1700000000`. This transaction writes
`ttlAt: 1700003600` and increments a counter in the same write. The client does not read the item first.

```ts
await db.transactWriteItems({
	items: [
		{
			operation: "update",
			hashKey: "session#42",
			update: [
				{
					action: "set",
					target: { ref: "data", path: "$.renewals" },
					value: { fn: "+", args: [{ ref: "data", path: "$.renewals" }, { val: 1 }] },
				},
				{
					action: "set",
					target: { ref: "ttlAt" },
					value: { fn: "+", args: [{ ref: "ttlAt" }, { val: 3600 }] },
				},
			],
		},
	],
});
```

When the item has no TTL, this update rejects with `update_not_applicable`, because the operand `ttlAt` is missing.
To set a TTL also on an item with no TTL, the caller writes
`{ fn: "if_not_exists", args: [{ ref: "ttlAt" }, { val: <now> }] }` in place of `{ ref: "ttlAt" }`.

**Example 2: the `ttlAt` field and a `ttlAt` action in one operation.** This operation is invalid. `db.ts` throws
`FokosValidationError` with the code `transact_operation_fields_invalid` before any I/O, because two sources give
the TTL.

```ts
await db.transactWriteItems({
	items: [
		{
			operation: "update",
			hashKey: "session#42",
			ttlAt: 1700000000,
			update: [{ action: "set", target: { ref: "ttlAt" }, value: { val: 1700003600 } }],
		},
	],
});
```

### 4.2 Types and validation

- `UpdateTarget` becomes `{ ref: "data"; path: string } | { ref: "ttlAt" }`.
- `analyzeUpdate` accepts `{ ref: "ttlAt" }` with no other field. It rejects a second action on `ttlAt` with
  `invalid_ast`. A `ttlAt` target takes no part in the overlap check of the data paths.
- For `set ttlAt`, the static types of the value must include `number`. A value whose types hold no `number`
  rejects at validation with `invalid_type`, before any I/O.
- An update that holds only `ttlAt` actions is valid. Its document is the pre-image, and `v` increments.
- `db.ts` rejects an `update` operation that has the `ttlAt` field and also a `ttlAt` action, with
  `FokosValidationError` and the code `transact_operation_fields_invalid`. The partition checks the same rule,
  because a partition never trusts the client.

### 4.3 Compiler and plan

`compileUpdateExpression` adds two outputs to `CompiledUpdatePlan`:

| Field | Value |
| --- | --- |
| `ttlSql` | The fragment for the TTL slot, or `undefined` when the update has no `ttlAt` action. |
| `ttlValidSql` | `1`, or the test of section 4.3.1. It is also a term of `applicableSql`. |

`ttlSql` for `set ttlAt = <expr>` is the value of the expression in `logical` mode. For `remove ttlAt` it is
`NULL`.

#### 4.3.1 The value test

For `set ttlAt`, two terms join `applicableSql`:

1. The present test of the value (`renderPresent`), as for every `set` value. It fails for a missing value.
2. `ttlValidSql`: `CASE WHEN <present> THEN typeof(<value>) = 'integer' AND <value> > 0 ELSE 1 END`.
   It fails only for a present value. A missing value passes it, so the present test alone rejects a missing value.
   A JSON `null` is present, and its `typeof` is `'null'`, so `ttlValidSql` rejects it.

The `typeof` test also catches these cases:

- A JSON number with a fraction or an exponent, for example `1.7e9`. SQLite reads it as `real`.
- An integer overflow in `+` or `*`. SQLite gives a `real` result.
- `text`, `boolean`, `null`, `array`, and `object` values.

The probe reads `ttlValidSql` separately, as it reads `valueTypeSql`. When the update does not apply,
`TransactionParticipant.precheck` answers with the first failed cause in this order:

1. `update_value_is_bytes`, when `valueTypeSql` failed.
2. `update_ttl_invalid`, when `ttlValidSql` failed.
3. `update_not_applicable`, for every other cause.

`update_ttl_invalid` is a new rejection code. It joins `TransactionRejectionReason` and `PremiseRejectionCode` in
`shared/transaction-api-types.ts`, and the codes in `shared/errors.ts`. It is a separate code for the same reason
that `update_value_is_bytes` is: the caller can fix the cause. A missing value stays `update_not_applicable`,
because `ttlValidSql` passes for a missing value.

#### 4.3.2 Size and binding limits

The compiler includes `ttlSql` in the widest write statement when it checks `sql_limit`. Its bindings are part of
`context.bindings`, so the binding count already covers them.

### 4.4 The write statements

| Statement | Change |
| --- | --- |
| `updateItemSingleShot` | `ttlSql` comes from the plan when the plan has one, else from the current branch. |
| `insertPendingUpdateLock` | `ttlExpr` comes from the plan when the plan has one, else from the current branch. |
| `composeUpdateWriteStatement` | No change. It already takes `ttlSql` as a fragment. |

The lock row stores the computed TTL. `commitLocal` applies the lock row as a put, so commit writes the TTL that
prepare computed.

The single-shot statement returns `ttl_epoch_utc_seconds`, and the item stats facts read it from there. No extra
read is needed.

### 4.5 Transactions with more than one operation on one item

With `executionMode: "ordered_per_item"`, an update can read `ttlAt` after a put or an update of the same item in
the same transaction. `validateVersionReferences` refuses only a read of `v`, because `v` can differ between
prepare and commit. Thus a read of `ttlAt` after a write of the same item is allowed, and
`validateVersionReferences` does not change. Section 5, case 9, tests this rule.

## 5. Test Plan

Each case runs on the single-shot path and on the two-phase path.

1. `set ttlAt = :literal` stores the literal.
2. `set ttlAt = ttlAt + 3600` adds 3600 to the stored TTL.
3. `set ttlAt = ttlAt + 3600` on an item with no TTL rejects with `update_not_applicable`.
4. `set ttlAt = if_not_exists(ttlAt, :now) + 3600` works on an item with no TTL and on an item with a TTL.
5. `set ttlAt = data.expiresAt` copies the attribute. A value of `1.7e9`, `"1700000000"`, `0`, `-1`, or `null`
   rejects with `update_ttl_invalid`. A value that is bytes rejects with `update_value_is_bytes`.
6. `remove ttlAt` clears the TTL. On an item with no TTL, it makes no change to the TTL, and `v` increments.
7. An update on an absent item: `set ttlAt = :literal` creates the item with that TTL, and a value that reads
   `ttlAt` rejects.
8. The `ttlAt` field and a `ttlAt` action in one operation reject in `db.ts`, and in the partition when the
   client check is skipped. Two `ttlAt` actions in one update reject.
9. `ordered_per_item`: a put with `ttlAt: T`, then an update `set ttlAt = ttlAt + 10` of the same item, commits
   with the TTL `T + 10`.
10. The item stats facts match the items after each case. The TTL sweep deletes an item after its computed TTL.
11. A `set ttlAt` value that reaches the SQL size limit rejects with `sql_limit`.

## 6. Alternative Options

### 6.1 Copy a document attribute into the TTL on every write

A table setting names a TTL attribute, and the backend adds `set ttlAt = <attribute path>` to every update. This
copies the DynamoDB model. It is not chosen:

- FokosDB has no per-table TTL attribute setting.
- Item data can be bytes or text, which have no attribute to read.
- Puts and batch writes need the same rule, so the change is larger than updates.
- Each update gets an action that the caller did not write.

A caller who ports DynamoDB code writes `set ttlAt = data.expiresAt`, or sets the attribute and the TTL to the same
value.

### 6.2 Ignore an invalid TTL value, as DynamoDB does

DynamoDB ignores an invalid TTL attribute because that attribute is ordinary data. Here the caller names the TTL as
the target. Example: the item has no TTL, and the caller sends `set ttlAt = ttlAt + 3600`. If the update ignores
the invalid value, the write succeeds, the item never expires, and the caller gets no signal. Not chosen.

### 6.3 A cast from text to number

A cast lets `set ttlAt = data.expiresAt` accept a number written as text, for example `"1700000000"`. `ttlAt` in
FokosDB is always a number, and the literal field never accepts text. A cast function is a separate feature with
its own semantics. Not chosen.

### 6.4 Evaluate the value on the document after the update

DynamoDB reads the TTL from the stored item after the update, because the TTL is an attribute of the item. A `set`
value in DynamoDB and in FokosDB reads the item before the update. A `ttlAt` action that reads the item after the
update is the only action with that rule. Not chosen.

## 7. Frequently Asked Questions

**Does the literal `ttlAt` field of the operation stay?**
Yes. `set ttlAt = :literal` gives the same result, but existing callers keep working.

**Does a computed TTL change the size of the item?**
No. `item_bytes` does not include the TTL.

**Does the coordinator need a schema change?**
No. The tree is in `tc_items.update_json`, and `tc_items.ttl_epoch_utc_seconds` stays `NULL` for an update with a
`ttlAt` action.

## 8. References

- `docs/agent-plans/2026-09-02-update-expressions.md`
- `docs/agent-plans/2026-09-11-update-creates-the-absent-item.md`
- `docs/agent-plans/2026-08-30-item-ttl-expiration.md`
- [DynamoDB Time to Live](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html)
