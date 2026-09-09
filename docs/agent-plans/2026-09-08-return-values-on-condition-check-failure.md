# RFC — Return the item when a condition check fails

**State:** Completed
**Date:** 2026-09-08
**Author:** Lambros

---

## Table of Contents

1. [Overview and Context](#1-overview-and-context)
2. [Goals and Requirements](#2-goals-and-requirements)
3. [Milestones](#3-milestones)
4. [Proposed Solution](#4-proposed-solution)
5. [Future Work](#5-future-work)
6. [Alternative Options](#6-alternative-options)
7. [Frequently Asked Questions](#7-frequently-asked-questions)
8. [References](#8-references)

---

## 1. Overview and Context

### 1.1 The problem

A caller writes an item under a condition. The condition fails. The caller learns that the condition
failed, and nothing else.

`PartitionDO.apiPutItem` and `PartitionDO.apiDeleteItem` raise `Error("fokos/putItem: condition
failed")`. The message carries no key and no value. `FokosDB.transactWriteItems` answers with one
`RejectionReason` for the whole transaction. The reason names the key, but a transaction can hold 100
operations, and the caller sees one of them.

Three costs follow:

1. **An extra read on every optimistic retry.** A caller that writes under `attribute_equals(v, 7)`
   must read the item again to learn the current `v`. That costs a second round trip. Another writer
   can change the item between the failure and the read, so the value the caller reads is not always
   the value the condition compared.
2. **No per-operation detail in a transaction.** A transaction with 100 operations reports one
   reason. The caller cannot tell which of the other 99 operations were acceptable.
3. **A lost diagnosis.** The condition compared against a stored value that no later read reproduces
   with certainty. Only the partition that evaluated the condition holds that value.

DynamoDB solves the first cost with `ReturnValuesOnConditionCheckFailure` and the second with
`CancellationReasons`. This RFC specifies both for FokosDB.

### 1.2 What the reader must know about the current system

- `FokosDB` in `packages/fokosdb/src/client/db.ts` compiles a condition expression into a
  `CompiledConditionPlan`. It sends the plan to the partition that owns the key.
- `PartitionStore.evaluateCondition` runs the plan. `composeConditionStatement` joins the `items`
  table and returns three columns: `item_present`, `condition_ok`, and `last_transaction_ts`.
- A write transaction runs on one of two paths. The single-shot path gives the whole operation set to
  one partition, which validates and applies it inside one storage transaction. The two-phase path
  locks each item at prepare and applies the stored payload at commit.
- `TransactionParticipant.prepareLocal` checks every operation first, then locks every item. The
  check pass returns at the first rejection.
- The transaction coordinator fans prepare out to every participant, keeps the first rejection
  reason, and writes it to `tc_state.rejection_reason_json`. It replays that reason for
  `IDEMPOTENCY_WINDOW_MS`, which is 10 minutes.
- `tc_state` is keyed by `idempotency_token`, with a secondary index on `transaction_id`. The other
  coordinator tables, `tc_items` and `tc_participants`, are keyed by `transaction_id`. Every state
  transition holds both ids, and the alarm sweep deletes `tc_state` rows by token.
- Durable Object RPC carries only an error's message across the boundary. Both sentinels in
  `packages/fokosdb/src/shared/partition-errors.ts` match on a message substring for that reason. The
  `enhanced_error_serialization` compatibility flag, on by default from 2026-04-21, relaxes this: an
  error keeps its own properties through V8 serialization. It does not keep the stack, and the
  documentation promises own properties, not the prototype. Section 6.2 gives the reason this library
  does not depend on the flag.
- `validateTransactWriteOperations` rejects a duplicate `(hashKey, sortKey)` pair. A key names one
  operation.
- `hashTransactionOperations` fingerprints an operation set with a fold that is commutative, so the
  same items in a different order are one request. No positional value is returned, so nothing
  depends on that order.
- `PartitionDO.txPrepare` is recursive. A partition that has split routes each item to the child that
  owns it and merges the answers. So one participant, as the coordinator sees it, can be a tree of
  Durable Objects.

### 1.3 Glossary

| Term | Meaning in this document |
| --- | --- |
| Image | The stored item as it was when the partition evaluated the condition. |
| Operation | One entry of a `transactWriteItems` request. |
| Check pass | The loop in which a participant evaluates every operation before it locks any item. |
| Lock pass | The loop in which a participant locks every item, after the check pass accepts. |
| Forwarding node | A partition that routes an item to a child instead of evaluating it locally. |

---

## 2. Goals and Requirements

### 2.1 In scope

1. `putItem`, `deleteItem`, and each operation of `transactWriteItems` must accept
   `returnValuesOnConditionCheckFailure`, with the values `"none"` and `"all_old"`. The default is
   `"none"`.
2. When the value is `"all_old"` and a condition fails on an item that exists, the caller must
   receive that item: its data, its data kind, its version, and its TTL.
3. A cancelled `transactWriteItems` must carry one result for each operation of the request, in
   request order.
4. A result must name one of three outcomes: the operation passed its checks, a participant did not
   evaluate it, or a participant rejected it. A rejected result must carry the rejection reason.
5. The image must add no cost to a conditional write whose condition passes. Only a failed condition
   may read the row again, and only when the caller asked for the image.
6. The image must cost nothing when the caller does not ask for it. The partition must not read the
   data column.
7. The image bytes the caller receives for one transaction must stay at or below
   `MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX`.
8. The shapes this RFC adds must extend to `FokosDB.updateItem` and to success images without a
   change of their meaning.

### 2.2 Out of scope

| Item | Reason |
| --- | --- |
| `FokosDB.updateItem` | Its own RFC. This RFC changes no update behaviour. |
| `ReturnValues` on a successful write (`all_old`, `all_new`) | Its own RFC. It needs a per-operation result on the committed path, which this RFC does not add. |
| An image on a rejection other than `condition_failed` | `update_not_applicable` and `item_too_large` can carry one later. The field is additive. |
| An image on `pending_conflict` for `putItem` and `deleteItem` | A held lock asks the caller to retry. It is not a failed precondition. The path keeps its current `throw`. |
| A durable fix for coordinator storage growth | Section 4.2.7 adds a size guard that refuses new transactions before the object fills. Retention, eviction, and a per-coordinator image budget need their own RFC. |

### 2.3 Requirements that constrain the solution

- The condition statement must not change. Its text, its plan, and its cost are the same in every
  mode, so a passing condition costs what it costs today.
- The check pass must stay free of side effects. Every statement it runs is a `SELECT`, and a
  returned rejection commits the storage transaction it runs inside.
- Prepare must not fail after it accepts. This RFC must not move a test out of the check pass.
- The coordinator must not JSON-encode image bytes. Its `$u8` tag costs about 4 characters for each
  byte, which turns a 10 MiB payload into about 40 MiB of text.
- A caller that raises the cap must see more images and the same outcome codes.
- The outcome codes a caller sees must not depend on which partition owns each operation. The set of
  images may depend on it, because every node caps what it sends on its own.
- A participant's answer must survive a coordinator that stops before it decides the transaction.

---

## 3. Milestones

Every milestone is shipped.

**M0 — `tc_state` keyed by `transaction_id`.** The primary key of `tc_state` moves from
`idempotency_token` to `transaction_id`, and the secondary index moves the other way: a `UNIQUE` index
on `idempotency_token` replaces the index on `transaction_id`. Every other coordinator table is keyed
by `transaction_id`, and so is the `tc_results` table of M2, so after M0 every state transition and
the sweep address one transaction by one key. The token is read in one place, the replay lookup at the
top of `initiateWrite`. M0 changes no behaviour and ships first. M2 depends on it.

**M1 — The image on `putItem` and `deleteItem`.** `PartitionStore` gains an image read that runs
after a failed condition. The two item RPCs answer with a result union. `db.ts` raises a typed error
that carries the image. M1 ships on its own: a caller of `putItem` reads the item that failed the
condition.

**M2 — The per-operation results of `transactWriteItems`.** Both write paths evaluate every operation
and return one result for each. `opIndex` and `returnValuesOnConditionCheckFailure` travel from
`tc_items` through the prepare fan-out, and every result carries its `opIndex` back, so every node
merges by index and not by arrival order. The coordinator persists each participant's whole answer
with that participant's prepare outcome, stops storing images once an execution failure is known,
merges the answers at `CANCELLING`, applies the cap, and deletes the images the cap drops. Five schema
edits carry it, and the operation fingerprint stops being commutative. M2 reuses the image type and
the image read of M1.

**M3 — The coordinator size guard.** `initiateWrite` refuses a new transaction above
`MAX_TC_DATABASE_BYTES`. M3 depends on no other milestone and can ship at any point. It must ship
before M2 reaches a production table, because M2 starts to persist images.

---

## 4. Proposed Solution

### 4.1 High-level overview

A caller asks for the image with one field on the operation it already sends:

```
putItem({ hashKey, data, condition, returnValuesOnConditionCheckFailure: "all_old" })
```

The condition statement does not change. When it reports a failed condition on a row that exists, and
the caller asked for the image, the partition runs one more statement inside the same storage
transaction: a primary-key read of that row that returns the decoded value, its kind, its version, its
TTL, and the byte count of the value. The two statements run with no `await` between them inside a
single-threaded Durable Object, so the image is the row the condition compared.

```
condition passes, any mode          condition fails, "none"          condition fails, "all_old"
──────────────────────────          ───────────────────────          ──────────────────────────
 condition statement                 condition statement              condition statement
                                                                      image statement
        1 row read                          1 row read                       2 row reads
```

An error cannot carry the image across the RPC boundary, because Durable Object RPC keeps only the
message. So `apiPutItem` and `apiDeleteItem` answer with a result union instead of a `throw`. The
`db.ts` boundary runs inside the Worker, with no RPC hop after it, so it turns the rejection back into
an exception. The public behaviour does not change: a failed condition still raises. The exception now
carries the reason and the image.

A transaction answers with more. A participant evaluates every operation it owns and returns one
result for each:

```
request       op0        op1        op2        op3        op4
              ─────      ─────      ─────      ─────      ─────
partition A   op0                   op2                   op4
partition B              op1                   op3

A rejects op2, B accepts

results       passed     passed     rejected   passed     passed
                                    + image
reason        condition_failed on op2
```

Every operation carries the index it had in the request, so each node merges the answers by index and
never by arrival order. Every node caps the image bytes it sends, and the coordinator caps the whole
array once more before it returns it beside the reason it returns today. The reason names why the
transaction cancelled. The array says what happened to each operation.

### 4.2 Technical details

#### 4.2.1 The image type

The image follows the pattern of `ReadForTransactionItemValueOf<D>`: one type, parameterized by the
representation of `data`, with an encoded variant for the wire and a public variant for the caller.

```ts
export type ConditionCheckImageOf<D> = {
	hashKey: string | Uint8Array;
	sortKey?: string | Uint8Array;
	data: D;
	kind: DataKind;
	version: number;
	/** Epoch UTC seconds. Absent when the item has no expiry instant. */
	ttlAt?: number;
};

/** Wire variant. json data is JSON text, which db.ts parses once at the public boundary. */
export type ConditionCheckImageEncoded = ConditionCheckImageOf<string | Uint8Array>;

/** Public variant, surfaced by db.ts. */
export type ConditionCheckImage = ConditionCheckImageOf<string | Uint8Array | JsonValue>;
```

The image carries its own keys. It is nested inside a `condition_failed` reason, which names the same
keys, so the pair is repeated; the image is a self-contained item, and a caller that pulls one out of
a results array reads it without carrying the reason it came from. `version` is the value a caller
feeds back into an `attribute_equals` condition on the retry, so it is the field that makes the image
useful.

**An image needs an item.** A condition that fails on an absent item returns no image. A caller that
writes under `attribute_not_exists` and fails learns that the item exists, and receives it. A caller
that writes under `attribute_exists` and fails learns that the item does not exist, and receives
nothing.

#### 4.2.2 The image read

`composeConditionStatement`, `evaluateConditionPlan`, and the compiled SQL budget do not change. The
image comes from a second statement, `PartitionStore.getItemImage(hk, sk)`, which the caller runs only
when the condition statement reported `conditionOk = false` and `itemPresent = true`, and only when
the operation asked for `"all_old"`:

```sql
SELECT CASE WHEN data_kind = <JSON_KIND_CODE> THEN json(data) ELSE data END AS data,
       data_kind,
       v,
       ttl_epoch_utc_seconds,
       octet_length(CASE WHEN data_kind = <JSON_KIND_CODE> THEN json(data) ELSE data END) AS image_bytes
FROM items
WHERE hk = ? AND sk = ?
```

It is `PartitionStore.getItem` plus the `image_bytes` column and minus `last_transaction_ts`, and it
reuses the `DATA_SELECT_DECODED` constant. Both statements run inside the same `transactionSync`, with
no `await` between them, so the row it returns is the row the condition compared.

**Why not one statement.** The first draft of this RFC widened the condition statement with the image
columns, each gated on the condition result through a CTE. Measured on SQLite 3.45 with a 200 KB json
row, the widened statement cost 500 µs when the condition passed and 950 µs when it failed, against
11 µs for the statement of today. SQLite flattens a single-use CTE into the outer query and
substitutes the `condition_ok` expression into every `CASE` that reads it, so the predicate ran six
times per row, and the JSONB was decoded whether or not the condition passed. `AS MATERIALIZED`
removes the flattening and pays for a temporary table instead, which measured the same as a second
decode. The two-statement shape costs the success path nothing, and it costs the failure path one
primary-key lookup on a page the condition statement has just read, about 390 µs of which is the one
JSONB decode any image return has to pay.

Three notes on the shape:

- `image_bytes` is the byte count of the value the statement returns, which is the value the response
  carries. For a `json` row the statement returns JSON text, not the stored JSONB, so this number is
  the transfer size and not the on-disk size. SQLite computes it, so the cap of section 4.2.6 needs no
  pass over the data in JavaScript. A JavaScript measurement of a UTF-8 string is either a lower bound
  or a second encode of the whole value.
- The statement decodes a json row twice, once for `data` and once for `image_bytes`. A CTE that
  decodes once and reads it twice measured no faster, because SQLite materializes it. The double
  decode stays until a measurement shows a cheaper shape.
- The statement is one fixed text with no plan embedded, so Workers SQLite keeps one prepared
  statement for it, and the compiler budget for conditions is untouched. A condition at the budget
  evaluates the same in both modes, because the mode does not reach the condition statement.

`evaluateConditionPlan` keeps its signature. `getItemImage` returns the shape `getItem` returns. It
carries no key, because the caller already holds the keys it passed in:

```ts
getItemImage(hk: KeyBytes, sk: KeyBytes): {
	row?: { data: string | Uint8Array; kind: DataKind; version: number; ttlAt?: number; imageBytes: number };
	rowsRead: number;
	rowsWritten: number;
};
```

`TransactionParticipant` and the item RPCs call it after a failed condition when the operation asks
for it. One function, `conditionFailedReason`, builds the reason and its `item` from these fields and
the decoded keys, so every path that reports a failed condition builds one the same way. `imageBytes`
stays beside the result for the cap.

#### 4.2.3 The item RPCs

**Where the field lives.** One type carries the option everywhere:

```ts
export type ReturnValuesOnConditionCheckFailure = "none" | "all_old";
```

It is an optional field, default `"none"`, on the public `PutItemOptions`, `DeleteItemOptions`, and
every member of `TransactWriteItem`; on the wire types `PutItemRpcRequest`, `DeleteItemRpcRequest`,
`TCWriteOperation`, and `TransactionItem`; and on the `tc_items` row of section 4.2.7.
`validateTransactWriteOperations` rejects any other value, because the HTTP surface reaches it
without TypeScript, and `putItem` and `deleteItem` run the same check at their entry.

`PutItemRpcResponse` and `DeleteItemRpcResponse` become unions:

```ts
export type PutItemRpcResponse =
	| { outcome: "ok"; version: number; meta: OperationMetrics & PartitionInfoInternal }
	| {
			outcome: "rejected";
			reason: RejectionReasonEncoded;
			meta: OperationMetrics & PartitionInfoInternal;
	  };
```

**The image sits inside the reason.** `RejectionReason` becomes generic over the representation of
the image, exactly as the read result is generic over the representation of `data`:

```ts
export type RejectionReasonOf<I = ConditionCheckImage> =
	| { type: "condition_failed"; hashKey: string | Uint8Array; sortKey?: string | Uint8Array; item?: I }
	// every other member is unchanged
	| { type: "transient_error" };

export type RejectionReasonEncoded = RejectionReasonOf<ConditionCheckImageEncoded>;
export type RejectionReason = RejectionReasonOf<ConditionCheckImage>;
```

The image belongs to one failed condition, and `condition_failed` is the reason that names it, so the
two travel together and no caller has to pair a reason with a sibling field. The alternative, a
sibling `item` on every response and result that can carry one, is recorded in section 6.

A reason travels to two places that must not carry a `MAX_ITEM_BYTES` value: the transaction-level
`reason` of `InitiateWriteResponse`, and `tc_state.rejection_reason_json`. So the image is stripped
at each: `pickWinningReason` removes it when a node picks the reason for its whole answer, and the
coordinator removes it again from every reason it stores. One copy of an image reaches the caller,
on the per-operation result that owns it, and one copy reaches storage, in `tc_results`.

`#apiPutItem` and `#apiDeleteItem` replace one `throw` each. The pending-lock `throw` above it does
not change:

```ts
const conditionRes = req.condition ? this.#store.evaluateCondition(req.condition, hashKey, sortKey) : null;
if (conditionRes && !conditionRes.conditionOk) {
	const image = wantsImage && conditionRes.itemPresent ? this.#store.getItemImage(hashKey, sortKey) : undefined;
	return { outcome: "rejected", reason: conditionFailedReason(keys, image?.row), meta };
}
```

`getItemImage` reports its own `rowsRead`, and the rejected response's `meta` sums it with the
condition statement's metrics, as the `ok` path sums the condition and the write today.

`RejectionReason` needs no new member. It already holds `condition_failed`, and it already holds
`update_not_applicable` and `update_value_is_bytes` from the transactional update path. So
`FokosDB.updateItem` reuses the same union without a change.

#### 4.2.4 The public boundary

`db.ts` maps the rejection back to an exception. The public signature of `putItem` and `deleteItem`
does not change, and a caller that ignores the image needs no edit.

```ts
export class ConditionCheckFailedError extends Error {
	readonly reason: RejectionReason;
	readonly meta: OperationMetrics & PartitionInfo;

	/** The image the reason carries, for a caller that wants it without the reason around it. */
	get item(): ConditionCheckImage | undefined;
}
```

It lives in `packages/fokosdb/src/shared/partition-errors.ts`, beside the predicates the client
already matches on. It imports the image type with `import type`, so it pulls no Durable Object class
into the client bundle. Its message keeps the substring `condition failed`, which the messages of the
two `throw` sites it replaces carry today, so a caller or a test that matches on that text keeps
working.

The error also carries `meta`. Today a failed condition loses the metrics and the partition
information, because a `throw` in the local handler passes neither.

`db.ts` parses the image data once, at the same boundary that parses a `getItem` result. For
`kind: "json"` the wire holds JSON text, and the public value is a `JsonValue`.

#### 4.2.5 The per-operation results of a transaction

```ts
/** Wire variant. `imageBytes` is coordinator bookkeeping, which db.ts strips. */
export type TransactWriteOperationResultEncoded =
	| { outcome: "passed" }
	| { outcome: "not_evaluated" }
	| {
			outcome: "rejected";
			reason: RejectionReasonEncoded;
			imageBytes?: number;
			itemOmitted?: "response_too_large";
	  };

/** What a participant answers: the same result, labelled with the index the request gave it. */
export type ParticipantOperationResultEncoded = TransactWriteOperationResultEncoded & { opIndex: number };

/** Public variant, surfaced by db.ts. */
export type TransactWriteOperationResult =
	| { outcome: "passed" }
	| { outcome: "not_evaluated" }
	| {
			outcome: "rejected";
			reason: RejectionReason;
			itemOmitted?: "response_too_large";
	  };
```

The image is on `reason.item`, per section 4.2.3. `itemOmitted` stays on the result and not on the
reason, because it is not part of why the operation was rejected: it says why the caller cannot see
the image the reason would otherwise carry.

`imageBytes` is the `image_bytes` column of section 4.2.2. Every level of the cap in section 4.2.6
uses this one number, so no two levels measure the same image differently. It is internal, and
`db.ts` drops it at the public boundary together with `opIndex`, as it drops `lastCommittedTs` and
`hasPendingWrite` from a read result.

A result carries no key, on the wire or in public. The wire entry is identified by its `opIndex`, and
the public entry by its position, which section 2.1 requires to match the request. So a caller reads
`results[i]` against the operation it sent at index `i`, exactly as it reads a `transactGetItems`
answer.

**Every operation carries its request index and its image flag.** `TransactionItem` gains `opIndex`
and `returnValuesOnConditionCheckFailure`. Both reach the evaluating node on the same path the
operation takes. On the single-shot path `db.ts` assigns `opIndex` from the request array and passes
the flag the caller set. On the two-phase path the coordinator rebuilds every `TransactionItem` from
`tc_items` before each prepare, in `drivePrepare` and again in `runPrepareRecovery`, so both fields
must be columns of `tc_items`: `op_index` and `return_values_on_condition_check_failure`. A flag that
lived only in the `InitiateWriteRequest` would be lost before the first prepare RPC, and no participant
would ever return an image on this path. The index travels with the operation to whichever node
evaluates it, and every result carries it back. Nothing depends on the order in which a node sends
items or receives answers, so the coordinator keeps its current send order and no path sorts first.

The index is what makes the merge work at every level. A participant sorts its own results by
`opIndex` before it runs the cap of section 4.2.6, so it walks the same order the coordinator walks.
No node has to know how the operations spread over partitions.

**A prepare RPC is a routing tree, not one store.** `PartitionDO.txPrepare` calls
`groupItemsByRouting` and forwards a subset of its items to each child. Today it collapses the answers
with `results.find((r) => r.outcome === "rejected")`, which takes an arbitrary child's rejection and
not the request-order-first one. Each forwarding node instead does what the coordinator does, one
level down:

1. It concatenates the result arrays of its children and of its own local pass.
2. A child that answered `accepted` sends no array, so the node fills `passed` for the operations it
   forwarded to that child.
3. It answers `accepted` only when every child and its own pass accepted. Otherwise it answers
   `rejected` with the merged array, and sets its own hop-level `reason` with `pickWinningReason`
   over that array. The choice does not decide the transaction — the node that builds the caller's
   answer runs the same rule over the array it merges — so no answer depends on which child replied
   first.
4. It applies the cap of section 4.2.6 to the merged array before it answers, over the images its
   children kept.

An execution failure is the one answer that carries no array, so a node propagates it as it received
it. The rule below ranks it above every per-operation rejection.

Every entry carries its own `opIndex`, so this is one merge repeated at every level.

The order in which a participant takes its locks does not change, and nothing depends on it. Prepare
takes every lock or none, inside one storage transaction, and a conflicting lock rejects instead of
waiting. There is no lock-ordering deadlock to preserve.

**The check pass evaluates every operation.** `prepareLocal` and `executeSingleShot` stop returning at
the first rejection. Each records one result for each operation it owns, then decides:

1. When no operation rejected, the participant runs its lock pass and answers `accepted`. It sends no
   result array, because every operation it owns passed.
2. When one or more operations rejected, the participant locks nothing and answers `rejected` with
   the result array, and with the hop-level `reason` that `pickWinningReason` takes from it.

An item that `prepareLocal` skips as an idempotent re-prepare, because this same transaction already
holds its lock, records `passed`. The lock is proof that an earlier check pass accepted it.

The lock pass and the accept path do not change. A rejection still returns before the first lock.

**`not_evaluated` covers the participant that answered neither.** A prepare RPC that throws after its
retries leaves its operations with no result. The coordinator treats the throw as an execution failure:
it cancels the transaction with `transient_error`, and the rule below marks every operation
`not_evaluated`.

**An execution failure outranks every condition.** `clock_skew` and `transient_error` say that the
transaction could not run. They do not say that a caller's premise was wrong, and they belong to no
operation. So the coordinator reports one alone: when any participant returns one, it becomes the
transaction's `reason`, every operation is `not_evaluated`, and no image is returned. This holds even
for an operation that another participant evaluated and rejected. The caller has one failure to act
on, and the answer to it is a retry.

**An execution failure stops the image writes.** The fan-out itself does not change: `drivePrepare`
still waits for every participant's prepare to settle, then decides once, and `runCancel` still sends
each cancel after the fan-out, so a cancel never overtakes a prepare. The shortcut is only in what the
coordinator stores while it waits. Once one participant has reported an execution failure, the outcome
is known: every operation will be `not_evaluated` and no image will be returned. So from that moment
the coordinator records each later answer's `prepare_outcome` and `answer_json` as before, and writes
no `tc_results` row for it. The images that participants reported before the failure are already on
disk, and the `CANCELLING` write deletes them, as section 4.2.7 states. A crash between the failure
and `CANCELLING` therefore leaves at most the images written before the failure, which recovery
deletes on the same path.

`runPrepareRecovery` follows the same rule: when a stored `tc_participants` row already carries an
execution failure, a re-prepared participant's answer is stored without its images.

**One rule picks the transaction reason, and every node runs it over the array it answers with.**
`pickWinningReason` takes the reason of the rejected result with the lowest `opIndex`, and strips its
image. A participant runs it over its own results, a forwarding node over the array it merged, and
the coordinator over the whole merged array; on the single-shot path the one partition that owns
every operation runs it, and its answer is already the transaction's. When any participant returned
an execution failure, that failure is the reason instead, and only the coordinator can see that.
Today the reason is the first rejection in the iteration order of `Promise.allSettled` over the
participants, which no send order can pin down.

`reason` stays on every rejected answer, and `results` joins it when the check pass produced one:

```ts
export type PrepareResponse =
	| { outcome: "accepted" }
	| {
			outcome: "rejected";
			/** The error for this answer as a whole. Its image is stripped. */
			reason: RejectionReasonEncoded;
			/** Absent when the node rejected before the check pass, as an execution failure does. */
			results?: ParticipantOperationResultEncoded[];
	  };
```

A node sets `reason` from one of its own rejected results. That value is a diagnostic for its hop and
not the transaction's answer, because the coordinator derives the transaction reason from the merged
array whenever the answer carries one. The coordinator reads `reason` only when the answer carries no
array, which is the execution failure above.

`SingleShotResponse` takes the same rejected shape. The coordinator's answer splits into a wire
variant and a public variant, as the read path splits `InitiateReadResponseEncoded` from
`InitiateReadResponse`. `InitiateWriteResponseEncoded` is what `initiateWrite` returns. Its cancelled
variant gains the positional array beside the `reason` it already carries, with json image data as
JSON text and the internal fields still present. It stays free of the recursive `JsonValue`, for the
same reason the read wire type does:

```ts
| {
		outcome: "cancelled";
		transactionId: TransactionId;
		idempotencyToken: IdempotencyToken;
		reason: RejectionReasonEncoded;
		results: TransactWriteOperationResultEncoded[];
  }
```

`InitiateWriteResponse` is the public variant `FokosDB.transactWriteItems` returns. `db.ts` builds it
from the encoded one at the same boundary that decodes a read result: it parses json image data into
a `JsonValue` and drops `opIndex` and `imageBytes`. It does not sort. Every producer of an array has
already sorted it — a node runs the cap of section 4.2.6, which sorts by `opIndex`, before it answers,
and the coordinator builds `results_json` by position — so a sort here would only hide a producer that
stopped doing so. The committed variant is the same in both.

`reason` keeps its meaning: why the transaction cancelled. `results` says what happened to each
operation. A cause that belongs to no operation has a `reason` and no rejected result, so both fields
stay.

A committed transaction carries no array. Every operation passed, so the array holds no information.

#### 4.2.6 The cap

```ts
export const MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX = 10 * 1024 * 1024; // 10 MiB
```

`MAX_ITEMS_PER_TX` is 100 and `MAX_ITEM_BYTES` is 400 KiB, so one transaction holds at most 40 MiB of
images. A cap of 40 MiB is therefore the same as no cap, and it is above the 32 MiB that one Workers
RPC message can carry.

The constant is its own, and is not derived from `MAX_PAYLOAD_BYTES_PER_TX`. That one caps what a
caller sends; this one caps what one answer on the rejection path returns. Different paths move the
two values under different pressure, so one constant for both would make a change to either one a
change to both. 10 MiB is a safe first value, and this constant is the only knob a change to it needs.

**One rule, applied at every level.** The rule sorts a list of results by `opIndex` and walks it:

1. It adds the `imageBytes` of each rejected result that carries an image.
2. When one image would take the running total above the cap, it drops that image and sets
   `itemOmitted: "response_too_large"` on that result.
3. It sets the same field on every later result that carries an image.
4. It leaves a result that already carries `itemOmitted` as it is. That image was dropped one level
   down, and it is not counted, because it is not sent.

A participant applies the rule to its own operations before it answers. A forwarding node applies it
to the array it merged from its children. The coordinator applies it to the whole merged array. Every
level uses `MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX`, which is a shared constant, so no level sends it
on the wire.

**Each level bounds its own answer, and only that.** The rule exists so that no RPC response and no
stored array carries more than the cap. It does not make the image set independent of the tree below
the coordinator. A lower node caps over the operations it owns, and a later operation on another node
can still fit at the coordinator after an earlier one was dropped below it. So the caller can see an
operation with `itemOmitted` followed by one with an image, and the same operation set can return a
different image set when the operations spread differently over partitions. The outcome codes do not
change with the spread, and the total image bytes stay at or below the cap on every path. The public
API documentation states this on `TransactWriteOperationResult`: `itemOmitted` says that this image
did not fit in some answer on the way back, not that every later image is absent.

The coordinator pass stays because lower nodes cap independently. Two participants can each answer
under the cap and together exceed it.

On the single-shot path one partition owns every operation, so its pass produces the final answer. No
second pass runs.

The outcome code of a result never changes. A caller that raises the cap sees more images and the same
codes, which is what section 2.3 asks for. A result that carries no image and no `itemOmitted` field
either passed, or its caller asked for no image, or its item does not exist.

The cap does not bind on `putItem` and `deleteItem`. One image holds at most `MAX_ITEM_BYTES`, which
is below 10 MiB, so those paths do not consult it.

#### 4.2.7 The coordinator

**The codes and the images are stored apart.** An outcome code is small, holds no item data, and
survives JSON encoding without loss. An image holds up to `MAX_ITEM_BYTES`, and a `bytes` row and a
`json` row are both BLOBs on disk. JSON text cannot hold a BLOB without an encoding that grows it, so
the images need a column that stores the value as the partition returned it.

**`tc_state` is keyed by `transaction_id` (M0).** Today the primary key is `idempotency_token` and
`transaction_id` has a secondary index. Every other coordinator table is keyed by `transaction_id`,
`tc_results` below joins them, and every state transition already holds both ids. M0 swaps the two:

```sql
CREATE TABLE IF NOT EXISTS tc_state (
    transaction_id          TEXT    NOT NULL PRIMARY KEY,
    idempotency_token       TEXT    NOT NULL,
    -- the remaining columns do not change
) WITHOUT ROWID, STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS tc_state_idempotency_token ON tc_state (idempotency_token);
CREATE INDEX IF NOT EXISTS idx_tc_state_completed_at ON tc_state (completed_at) WHERE completed_at IS NOT NULL;
```

The index is `UNIQUE`, so the schema keeps the guarantee the old primary key gave: one token names one
transaction. After M0 the token is read in one place, the replay lookup at the top of `initiateWrite`.
Every `UPDATE tc_state` and every `loadStateRow` after it addresses the row by `transaction_id`, which
every caller already carries beside the token. `recoverTransaction` and `loadFinalResponse` read the
row through the primary key instead of the secondary index, and the sweep below deletes two tables by
one list of ids.

Five schema changes for M2, all edited in place:

| Table | Column | Purpose |
| --- | --- | --- |
| `tc_items` | `op_index INTEGER NOT NULL` | The position of the operation in the request. It travels to the evaluating node on `TransactionItem.opIndex` and comes back on every result. |
| `tc_items` | `return_values_on_condition_check_failure INTEGER NOT NULL DEFAULT 0` | The caller's flag for this operation, `1` for `"all_old"`. `toTransactionItems` reads it into `TransactionItem.returnValuesOnConditionCheckFailure`, so every prepare, first or recovered, asks the participant for the same image the caller asked for. `stripPayload` leaves it in place; it is one integer. |
| `tc_participants` | `answer_json TEXT` | One participant's rejected `PrepareResponse` with every `item` removed: its `reason`, and its `results` when the check pass produced them, each rejected entry keeping its `imageBytes`. It holds no image. NULL for an accepted participant. |
| `tc_state` | `results_json TEXT` | The merged, capped array of outcome codes and reasons. It holds no image. |
| `tc_results` | new table | One row for each image a participant reported. |

```sql
CREATE TABLE IF NOT EXISTS tc_results (
    transaction_id  TEXT    NOT NULL,
    op_index        INTEGER NOT NULL,
    image_kind      INTEGER NOT NULL,
    image_version   INTEGER NOT NULL,
    image_ttl_epoch_utc_seconds INTEGER,
    image_data      ANY     NOT NULL,
    PRIMARY KEY (transaction_id, op_index)
) WITHOUT ROWID, STRICT;
```

`image_data` is `ANY`, which is what `tc_items.data` already uses to hold `string | Uint8Array`
without a serialization step. Text and JSON text bind as TEXT, and bytes bind as a BLOB, exactly as
the partition returned them. No JSON encoding of binary happens anywhere, so `stringifyReason` keeps
its `$u8` tag unchanged: it carries only keys.

`results_json` and `answer_json` hold up to `MAX_ITEMS_PER_TX` reasons instead of one, so the worst
case is worth naming. A key pair is at most `MAX_HASH_KEY_BYTES` plus `MAX_SORT_KEY_BYTES`, which is
1.5 KiB, and the `$u8` tag costs about 4 characters for each byte of a binary key. So 100 rejected
operations over binary keys make a row of about 600 KiB. A Durable Object caps a row at 2 MB, so this
fits with room to spare. It is also the reason the images are not in that row. Both columns are
written with `stringifyReason` and read with `parseReason`, so a binary key round-trips as it does in
`rejection_reason_json` today.

**A participant's whole answer persists with its prepare outcome.** The coordinator writes
`tc_participants.answer_json` and that participant's `tc_results` rows in the same storage transaction
as the `UPDATE tc_participants SET prepare_outcome = ?` that records the answer they belong to. No part
of a participant's answer lives only in memory. That includes the hop-level `reason` of an answer that
carries no array, so a `clock_skew` learned before a crash is still `clock_skew` after it, where today
the recovery path can only write `transient_error`.

This is what lets the recovery path answer. `drivePrepare` learns each participant's answer at a
different time, and it decides to cancel on a condition only once every answer is in. So the outcome
write and the `CANCELLING` write are seconds and several remote RPCs apart, and a coordinator evicted
in that window loses everything it held in memory. `runPrepareRecovery` then re-prepares only
participants whose `prepare_outcome IS NULL`, so it never asks a participant that already rejected. It
must not ask, because a second evaluation reads data that has moved. The answer persisted with the
outcome is what leaves recovery a complete array to merge, with every outcome, reason, and image the
participant reported.

The request bounds the write, not the participant count. One operation carries at most one image, one
image holds at most `MAX_ITEM_BYTES`, and one operation belongs to exactly one participant. So the
images of a whole transaction total at most `MAX_ITEMS_PER_TX` times `MAX_ITEM_BYTES`, or 40 MiB,
however they spread. The per-participant cap of section 4.2.6 bounds one RPC; this bounds the storage.

**The merge at `CANCELLING` has one source for each participant.** In one storage transaction with
the `UPDATE` that writes the state, the coordinator reads every `tc_participants` row of the
transaction and builds the array from three cases:

| Participant row | What the merge does |
| --- | --- |
| `prepare_outcome = 'accepted'` | Every operation of that participant is `passed`. The participant sent no array, and the lock it holds is the proof that its check pass accepted. |
| `prepare_outcome = 'rejected'` and `answer_json` carries `results` | The participant's entries, each at its own `opIndex`. |
| `prepare_outcome = 'rejected'` and `answer_json` carries no `results`, or `prepare_outcome IS NULL` | An execution failure. The merge stops: every operation of the transaction is `not_evaluated`, the transaction `reason` is the stored `reason` of that participant or `transient_error` for a NULL row, and the coordinator deletes every `tc_results` row of the transaction. |

A NULL row at this merge is a prepare that threw after its retries. In `drivePrepare` every prepare
has settled before the write, so a NULL row can only be a throw. In `runPrepareRecovery` the NULL
participants are re-prepared first, and the transition runs only when another participant rejected,
so a row that is still NULL threw again. Both cases are the `transient_error` of today.

The coordinator then applies the cap of section 4.2.6 to the merged array, writes
`tc_state.results_json`, and deletes the `tc_results` rows the cap dropped. Both writers of that
transition run this one function over storage, so `drivePrepare` and `runPrepareRecovery` cannot
merge differently.

`loadFinalResponse` reads `tc_state.results_json`, then reads the images with one query ordered by
`op_index`, and joins them by index onto the `condition_failed` reason at that position. `tc_results`
stores no key, because the reason it is joined to names the item, so the rebuilt image takes its keys
from there. An idempotent replay therefore answers with the same array as the first call, because the
cap already applied before the write. A NULL `results_json` answers with an empty array, and the
images it cannot name stay on disk until the sweep removes them with the transaction. That is the same torn-row degradation that makes `rejection_reason_json` fall back to
`transient_error`, and it says "no per-operation detail", not "nothing was evaluated".

A cancellation that carries no image writes one small row and no `tc_results` row, which is the common
case.

**The sweep selects one batch and deletes two tables by it.** After M0, `tc_state` and `tc_results`
are both keyed by `transaction_id`. The sweep selects the batch of `transaction_id` values whose
`completed_at` is past the cutoff, at most `SWEEP_BATCH_ROWS` of them, then deletes the rows of both
tables for those ids inside one `transactionSync`. One list drives both deletes, so a `tc_results` row
cannot lose the `tc_state` row that names it. Two `DELETE ... WHERE transaction_id IN (SELECT ...)`
statements that repeat the subquery would not do: the second subquery runs after the first delete and
selects a different batch.

**A size guard refuses new transactions.**

```ts
export const MAX_TC_DATABASE_BYTES = 5 * 1024 * 1024 * 1024; // 5 GB
```

`initiateWrite` throws when `this.ctx.storage.sql.databaseSize` is above it, before it writes the
`CREATED` row. It refuses only new work. A replay of a known token, a recovery, and every alarm path
still run, so a full object strands no transaction that is already in flight.

The refusal is a plain `Error` whose message tells the caller to retry, not a sentinel with a
matching predicate. Nothing in this package branches on it: the two sentinels of
`partition-errors.ts` exist because the coordinator and `db.ts` have to recognise a partition's
answer and act on it, and no code acts on this one — it reaches the caller of
`FokosDB.transactWriteItems` and stops there. A caller that wants to retry automatically needs a
sentinel, and that is the point at which to add one.

The value is half of the 10 GB a Durable Object holds. The other half is the headroom the refusal
needs to be useful: a coordinator that stopped taking work still has to drive every transaction it
already accepted to a terminal state, and each of those writes state before it sends its outbound
RPCs. A guard near the ceiling would refuse new work and then wedge on the old.

The guard bounds the damage. Section 5 holds the real work.

#### 4.2.8 Invariants

| Invariant | Mechanism |
| --- | --- |
| A passing condition costs what it costs today. | The condition statement does not change. The image read is a second statement that runs only after a failed condition on an existing row, and only when the operation asked for it. |
| The image is the row the condition compared. | Both statements run inside one `transactionSync` with no `await` between them, in a single-threaded Durable Object. |
| The check pass writes nothing. | Every statement it runs is a `SELECT`. A returned rejection commits the storage transaction it runs inside, so a write in the check pass would survive a rejection. |
| Prepare does not fail after it accepts. | This RFC moves no test out of the check pass. It only removes the early return. |
| Every prepare asks for the image the caller asked for. | The flag is a column of `tc_items`, and `toTransactionItems` reads it on the first prepare and on every recovered one. |
| `results` is positional to the request. | Every operation carries its `opIndex` to the node that evaluates it and back again, and the coordinator fills every index from one of the three sources of section 4.2.7. No merge depends on an arrival order. |
| A participant's answer survives a crash between that answer and the cancellation. | Its reason, its results, and its images are written in the same storage transaction as the prepare outcome they belong to, so recovery reads back exactly what the participant reported. |
| No image is stored after an execution failure is known. | The coordinator stores each later answer without its images, and the `CANCELLING` write deletes the images stored before the failure. |
| One token names one transaction. | M0 replaces the primary key on `idempotency_token` with a `UNIQUE` index on it, so the schema keeps the guarantee while `transaction_id` becomes the key. |
| A `tc_results` row never outlives the transaction that wrote it. | Both tables are keyed by `transaction_id`. The sweep selects one batch of ids and deletes from both tables by that list in one storage transaction. |
| A stored array is a capped array, and so is every answer on the way to it. | Every node applies the cap before it answers, and the coordinator applies it before it writes `tc_state.results_json`. |
| The outcome codes a caller sees do not depend on which partition owns each operation. | Every node evaluates every operation it owns, and the cap changes images, never codes. |
| The coordinator never JSON-encodes image bytes. | `tc_results.image_data` is an `ANY` column. `answer_json` and `results_json` hold codes and reasons, and `reasonWithoutImage` removes the image from every reason they hold, so a stored reason carries only keys. |
| A reason that leaves one node for another carries no image. | `pickWinningReason` strips it from the hop-level `reason` of every rejected answer, so the transaction-level `reason` and `tc_state.rejection_reason_json` hold keys and no item data. |
| No internal field reaches the public result. | `db.ts` builds `InitiateWriteResponse` from `InitiateWriteResponseEncoded`, and the public type holds neither `imageBytes` nor `opIndex`. |

#### 4.2.9 Performance

**The image costs the failure path one lookup and one decode.** A passing condition runs the
statement it runs today and nothing else. A failed condition under `"all_old"` on an existing row runs
one more primary-key lookup on a page the condition has just read, and decodes the value once for the
response. Section 4.2.2 gives the measurement: about 390 µs for a 200 KB json row, almost all of it
the JSONB decode that any image return has to pay.

**Rejection costs what acceptance already costs, plus the images asked for.** The accept path evaluates
every operation today, because it cannot accept without doing so. The change makes the reject path do
the same. A 100-operation transaction that rejects on its first operation runs 100 condition
evaluations instead of 1, and one image read for each failed condition that asked for one. Each
evaluation is one lookup on the `WITHOUT ROWID` primary key plus the predicate. The worst case adds
fewer than 200 row reads to a transaction that is already allowed 100 on its accept path, and every
one of them is a primary-key lookup, so it adds no new kind of work for the partition.

**An execution failure saves the image writes that follow it.** The fan-out takes the time it takes
today. Once one participant has reported `clock_skew` or a prepare has thrown, the coordinator stores
the later answers without their images, so a transaction that will return no image writes no more of
them.

**The image bytes of one transaction have a fixed ceiling.** One operation carries at most one image,
one image holds at most `MAX_ITEM_BYTES`, and one operation belongs to exactly one participant. So the
images of a transaction total at most 40 MiB, however the operations spread over partitions.

**The per-participant cap bounds one RPC, not the transaction.** A partition that owns 100 rejected
operations of 400 KiB each sends 10 MiB instead of 40 MiB. When 100 partitions own one operation each,
every participant answers far under its own cap, and 40 MiB still reaches the coordinator across the
100 RPCs. Section 6 records the option that would lower that number.

**The coordinator writes up to 40 MiB and keeps 10 MiB.** It writes each participant's images when
that participant answers, and it deletes the images the cap drops at `CANCELLING`. The ceiling above
bounds one transaction, and `MAX_TC_DATABASE_BYTES` guards the object against the sum over concurrent
transactions.

#### 4.2.10 Testing

1. A conditional `putItem` that fails returns the stored item, its kind, its version, and its TTL, for
   each of the three data kinds.
2. A conditional `putItem` that fails on an absent item returns no image.
3. A `putItem` with `returnValuesOnConditionCheckFailure: "none"` that fails returns no image, and its
   `meta.rowsRead` equals the value it reports today. The same call with `"all_old"` reports one more
   row read.
4. A conditional `putItem` that succeeds reads the same number of rows whether or not the caller asks
   for an image, and `getItemImage` is not called.
5. `deleteItem` repeats tests 1 to 3.
6. `getItemImage` returns `imageBytes` equal to the byte length of the `data` it returns, for each of
   the three data kinds, including a text value with characters above U+007F.
7. A cancelled transaction returns one result for each operation, in request order, with `passed` on
   the operations that were acceptable.
8. A transaction whose operations span two partitions returns results in request order, not in
   partition order, and each participant receives the flag the caller set on its operations. The
   two-phase path is the one under test, so it runs with a `clientRequestToken`.
9. A transaction in which two operations fail their conditions returns two images.
10. A transaction whose images exceed the cap fills images in request order, sets `itemOmitted` on the
    rest, and changes no outcome code.
11. A participant whose own images exceed the cap answers with the images the cap kept and sets
    `itemOmitted` on the rest.
12. One operation set returns the same outcome codes and the same images whether one partition owns
    every operation or several partitions own them.
13. A committed transaction returns no array.
14. The public results array carries no `imageBytes` and no `opIndex`.
15. A prepare RPC that throws leaves its operations `not_evaluated`, and the transaction cancels with
    `transient_error`.
16. A transaction in which one participant returns `clock_skew` and another rejects an operation on
    its condition reports `clock_skew`, marks every operation `not_evaluated`, and returns no image.
    The `tc_results` rows of the rejecting participant are gone once `CANCELLING` is written.
17. A transaction in which one participant returns an execution failure and a later participant rejects
    an operation with an image records the later participant's outcome and reasons, writes no
    `tc_results` row for it, and still waits for every prepare before it writes `CANCELLING`.
18. A `clock_skew` answer persisted before a crash is reported as `clock_skew` by the recovery path,
    not as `transient_error`.
19. An idempotent replay of a cancelled transaction returns the same array as the first call,
    including the images and the `itemOmitted` fields.
20. A replay that reuses a `clientRequestToken` with the same operations in a different order is
    rejected as a different request, and does not answer with the first call's array.
21. Two requests that differ only in `returnValuesOnConditionCheckFailure` fingerprint differently, so
    the second under one token is rejected instead of replaying the first.
22. A transaction over binary keys and binary data round-trips its images through `tc_results`, and
    the stored `image_data` is a BLOB.
23. The sweep deletes the `tc_results` rows of every `tc_state` row it deletes, and leaves no orphan
    row behind.
24. A coordinator above `MAX_TC_DATABASE_BYTES` refuses a new transaction, and still answers a replay,
    drives a recovery, and runs its alarm.
25. The existing `condition failed` assertions in `test/partition-do/item-conditions.test.ts` and
    `test/transactions/tx-end-to-end.test.ts` still hold: the stub tests read the `rejected` outcome,
    and the `db.ts` tests still match `ConditionCheckFailedError` by that substring.
26. After M0, the existing coordinator suite passes unchanged: a replay by token, a
    `recoverTransaction` by transaction id, and the sweep all find the same rows they find today, and
    a second `INSERT` with a used token fails on the `UNIQUE` index.

**Two cases this RFC does not test.** A transaction reaching a partition that has since split
exercises the forwarding-node merge of section 4.2.5, and the suite has no harness that splits a
partition under a prepare. The merge itself is the same `pickWinningReason` and cap that test 12
covers one level up, so the gap is the routing, not the rule. A crash between a rejected prepare
answer and the `CANCELLING` write needs a coordinator that stops mid-fan-out; tests 17 and 18 reach
the same recovery path from stored state, which is what that crash leaves behind.

#### 4.2.11 Deployment and rollback

The field is optional and defaults to `"none"`, so a caller that does not set it sees the behaviour it
sees today. The one change for that caller is that `putItem` and `deleteItem` raise
`ConditionCheckFailedError` instead of `Error`. `ConditionCheckFailedError` extends `Error` and its
message keeps the `condition failed` substring, so a caller that catches `Error` or matches on the
message still works. A caller of the stub RPCs directly, which only the test suites do, reads the
`rejected` outcome instead of catching a throw.

**`itemOmitted` is documented as a per-answer fact.** The TSDoc on `TransactWriteOperationResult`,
which is the public documentation of `results`, says that an `itemOmitted` entry can be followed by
an entry that carries an image, and that the image set of one operation set can change with the
partition layout while the outcome codes do not.

**The operation fingerprint changes in two ways.** `hashTransactionOperations` chains
`returnValuesOnConditionCheckFailure` into each operation. So a retry that adds the flag is a
different request wearing the same token, and the coordinator rejects it instead of replaying an
outcome that carries no image. The fold across operations also stops being commutative. `results` is
positional to the request, so the same items in a different order are no longer the same request: a
caller that builds its item list from an unordered structure and retries must build it in the same
order. Without this change, a replay answers with the first call's array against the retry's
positions, and the caller reads one operation's image under another operation's key.

**`db.ts` fills the array on the path that never reaches a coordinator.** The single-shot fast path
answers `cancelled` from the partition's own result array. One branch of it builds a response in the
Worker: the partition past its size cap, which answers `transient_error`. That branch fills
`not_evaluated` for every operation, because no participant evaluated any of them. The other branch
returns `null` and runs the coordinator path, which answers for itself.

The schema changes of M0 and M2 are edits to the existing migration. The package is at version `0.0.0`,
so a breaking schema change costs no migration path. A local `.wrangler/` state that already ran the
migration must be wiped before the edited migration can take effect.

---

## 5. Future Work

1. **Retention and eviction on the transaction coordinator.** Section 4.2.7 adds a `databaseSize`
   guard that refuses new transactions, which stops a full object from wedging the state machine. The
   coordinator writes each state transition before it sends its outbound RPCs, so a write that fails
   with `SQLITE_FULL` leaves the transaction undriven while its reads keep working. The guard is not a
   fix. Retention is still time-based, because the alarm sweeps rows older than
   `IDEMPOTENCY_WINDOW_MS`. A coordinator that fills inside that window refuses work until the window
   passes, and persisted images bring the ceiling closer. A per-coordinator image budget, eviction
   under pressure, and a way to spread a table's transactions over more coordinators belong to that
   RFC. `numTxCoordinators` is not that way: it names the coordinator a replay must reach, so a change
   to it while a token is in flight sends the retry to an object with no record of the transaction.
2. **No image retention for a transaction that asked for no idempotency.**
   `FokosDB.transactWriteItems` generates a token when the caller supplies none, and it sends that
   token to the coordinator as `clientRequestToken`. So the coordinator cannot tell a generated token
   from a caller's own, and it keeps the `tc_results` rows of every cancelled transaction until the
   sweep, one `IDEMPOTENCY_WINDOW_MS` later. No caller can replay a token it never chose, so that
   storage buys nothing. `InitiateWriteRequest.clientRequestToken` is optional and `initiateWrite`
   falls back to the `transactionId`, so `db.ts` can leave the field out and the coordinator reads the
   absence as the signal. It still writes the images when a participant answers, because section 4.2.7
   needs them for recovery, and it then deletes them at the terminal transition instead of at the
   sweep. Two details belong to that work. The client still needs a locally generated token to pick
   the coordinator shard, and that token stops matching the stored one, so a retry starts a new
   transaction. A result whose image was dropped by this rule needs a marker that separates it from a
   result whose item does not exist.
3. **`FokosDB.updateItem`**, with the same result union and the same image type.
4. **`ReturnValues` on a successful write.** `all_new` for an update is the high-value mode. The update
   probe already builds the complete new document to measure it, so the value is in hand.
5. **An image on `update_not_applicable`.** That reason is a documented catch-all, so the caller
   cannot diagnose it without the item. The field is already on the result.

---

## 6. Alternative Options

**Attach the image to the thrown error at the partition.** Section 6.2 gives this its own comparison,
because the `enhanced_error_serialization` compatibility flag makes it possible.

**Return a union from the public `putItem`.** A failed condition would stop being an exception. Every
call site would need an edit, and the happy path would gain a discriminant that almost no put needs.
DynamoDB raises for the same reason.

**Carry the image as a sibling of the reason.** Every response and result that can hold an image
would gain an `item` field beside its `reason`: `PutItemRpcResponse`, `DeleteItemRpcResponse`,
`TransactWriteOperationResultEncoded`, and its public variant. Nothing then has to strip an image out
of a reason on the way to the transaction-level `reason` or to `tc_state.rejection_reason_json`,
because no reason ever holds one.

It was rejected because the image belongs to exactly one reason. `condition_failed` is the only
member that produces one, and a sibling field puts it where every other member appears to permit it,
so a caller reads `item` against a reason that can never carry one. The image inside the reason makes
the type say what the data is: `RejectionReasonOf<I>` names the image only on the member that has
one. The price is the stripping, and it is one function, `pickWinningReason`, plus the coordinator's
`reasonWithoutImage` on the two columns it writes — each of them a place that already had to be
careful about size.

**Key each result by its `(hashKey, sortKey)` instead of by `opIndex`.** The pair is unique inside one
transaction, because `validateTransactWriteOperations` rejects a duplicate. It works, and it forces
every node to re-derive identity through `KeyCodec.pairKey`. The index is already in hand.

**Stop the check pass at the first rejection and mark the rest `not_evaluated`.** This costs no extra
evaluation, and a participant that returns early already knows that the operations before the failing
one passed. It was rejected because a caller with 100 operations wants to know about all of them, and
not about the prefix that ran before the first failure. The cost falls on the rejection path only, and
section 4.2.9 shows it adds no new worst case.

**Drop every image when the transaction passes the cap.** An all-or-nothing rule is simpler, and it
loses the images of the operations that would have fit. A fill in request order is deterministic and
returns more.

**Apply the cap only at the coordinator.** Each participant would send every image it collected, so
one partition that owns 100 rejected operations would send 40 MiB, above the 32 MiB one RPC message
can carry, for the coordinator to reduce to 10 MiB. The lower pass costs one running total and keeps
every answer on the way back under the cap. Its price is the one section 4.2.6 documents: the image
set can depend on how the operations spread over partitions.

**Make the coordinator drop every image after the first one a lower node dropped.** This would give
the caller a strict fill in request order whatever the tree below. It was rejected because it throws
away images that fit, to keep a property no caller has asked for. The outcome codes are the same
either way, and the documentation states what `itemOmitted` means.

**Give each participant a share of the cap.** This would lower the 40 MiB that reaches the coordinator
when the operations spread over many partitions. The coordinator cannot compute a share before it sees
the image sizes, so it would need a second round of RPCs. That trade is not worth one round trip on
the rejection path.

**Write `CANCELLING` as soon as the first execution failure arrives.** The decision cannot change once
one participant reports `clock_skew` or a prepare throws, so the coordinator could answer the caller
before the slower prepares settle. It was rejected because `runCancel` would then race the prepares
still in flight: a cancel that reaches a partition before its prepare releases nothing, and the prepare
that lands after it locks the items until the stale-transaction alarm releases them. The fan-out keeps
its current shape, and section 4.2.5 takes the one saving that costs no race: it stops storing images
once the failure is known.

**Hold each participant's images in memory until `CANCELLING`, and persist only its codes early.**
This writes at most one cap of images for the whole transaction instead of up to 40 MiB, and it
answers every normal cancellation identically. It was rejected because a coordinator evicted during
the prepare fan-out then loses the images of every participant that had already answered, and it can
report those operations only with the outcome and no item. The saving is bounded by 40 MiB, which is a
quarter of one transaction's own payload ceiling and small against `MAX_TC_DATABASE_BYTES`. It does
not buy a degraded answer on a recovery path.

**Hold the images as JSON in `tc_state`.** A base64 tag would cost about 1.33 characters for each
byte, against about 4 for the current `$u8` tag. Both encode bytes that need no encoding. An `ANY`
column stores the value the partition returned, and it needs no reviver and no compatibility with rows
written before the deploy.

**Hold the images in the Durable Object KV API**, keyed by `<transaction_id>/<op_index>`. This is the
closest option to the one chosen, and section 6.1 compares the two.

### 6.1 The image store: a table or the KV API

The KV API stores into a hidden `__cf_kv` table in the same SQLite database, and each method is atomic
with the other storage operations. So both options write inside the same `transactionSync`, and both
are synchronous. Neither serializes: an `ANY` column binds a string as TEXT and a `Uint8Array` as a
BLOB, and a KV value is structured-cloneable.

| | `tc_results` table | KV, keyed by transaction and index |
| --- | --- | --- |
| Schema change | One new table | None |
| Write one image | One `INSERT`, four binds | One `kv.put`, the whole object |
| Read the images of one transaction | One `SELECT`, ordered by `op_index` | One `kv.list` with a prefix |
| Delete the images of one transaction | One `DELETE`, which reads no value | `kv.list` then `delete`, which reads every value it deletes |
| Coordinator state | All of it in SQL | Split across SQL and KV |

The last two rows decide it. The sweep runs on an alarm and deletes each transaction past
`IDEMPOTENCY_WINDOW_MS`. A `DELETE ... WHERE transaction_id = ?` touches no image bytes. A KV prefix
delete first lists, and a list returns the values, so the sweep would read up to
`MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX` for each transaction it removes.

A KV sweep can avoid that read. `tc_state.results_json` records which operations carry an image, so
the sweep can build the exact keys and delete them without a list. That costs one read of the array
for each swept transaction, and it makes the two stores depend on each other.

The KV option is otherwise the smaller change, and it keeps the image whole on both sides instead of
one image split over four columns. An image is an opaque payload that nothing queries by content,
which is the shape a key-value store fits. The table wins on the sweep, which runs on every
cancellation, and on keeping the whole coordinator state in SQL.

### 6.2 The item RPCs: a result union or a thrown error

The `enhanced_error_serialization` compatibility flag, on by default from 2026-04-21, keeps an error's
own properties through V8 serialization. So `apiPutItem` can throw an error that carries the image,
and the property reaches `db.ts`. The two wrangler projects in this repository set
`compatibility_date` to `2026-05-23`, so the flag is on for both.

The public behaviour is the same either way. `FokosDB.putItem` raises `ConditionCheckFailedError` for
a failed condition, and a caller writes the same `catch` block. The choice is only about the hop
between the partition and `db.ts`.

The union stays, for three reasons.

1. **This package is a library, and it does not own the compatibility date.** A consumer sets
   `compatibility_date` in its own wrangler configuration, and it can set
   `legacy_error_serialization`. A consumer on a date before 2026-04-21 would receive an error with no
   image, and with nothing to say why. A feature that carries data must not depend on the
   configuration of the Worker that imports it.
2. **The prototype does not survive.** The documentation promises own properties, not the class. An
   error that crosses the hop arrives without its prototype, so
   `err instanceof ConditionCheckFailedError` is false. `db.ts` builds the error instead, on the same
   side as the caller, so the class is always right.
3. **The transaction path needs the return-value mechanism anyway.** `PrepareResponse`,
   `SingleShotResponse`, and `InitiateWriteResponse` carry the results array and its images as return
   values, whatever the flag does. A thrown image on the item RPCs would add a second mechanism for
   one payload.

The flag does make one improvement possible outside this RFC. The two sentinels in
`partition-errors.ts` match on a message substring, and a property is a firmer test. That change must
keep the substring as a fallback, for the same reason as point 1.

---

## 7. Frequently Asked Questions

**Why does the option keep DynamoDB's name when it covers only `condition_failed`?**
The name matches the scope. Only a failed condition returns an image in this RFC. When
`update_not_applicable` gains one, the name will describe less than the field does, and that is the
point at which to revisit it.

**Why does `putItem` still raise for a held lock?**
A held lock asks the caller to retry, and a failed condition tells the caller its premise was wrong.
They are different answers, so they keep different mechanisms until an RFC unifies them.

**A per-operation evaluation can throw. Why does the result array not contain the error?**
`evaluateConditionPlan` and `probeUpdatePlan` raise `ExpressionError` when a compiled plan is invalid
or when SQLite cannot run it. That means a caller bug or a corrupt plan, not a data condition. The
check pass lets it throw, as it does today, and the coordinator reports `transient_error`. A report of
it as a per-operation outcome would invite a caller to treat a deterministic bug as a retryable
condition.

Full evaluation does make the throw reachable in a case where it is not reachable today: an earlier
operation fails its condition, and a later operation holds an invalid plan. Today the participant
returns before it reaches the later operation. With full evaluation it evaluates the later operation
and raises. The invalid plan is a real fault, so the raise is the better answer.

**Why is `itemOmitted` a separate field instead of an outcome code?**
The outcome says what happened to the operation. The marker says why the caller cannot see the image.
One code for both would lose the first fact, and a change to the cap would then change outcome codes
and not only images.

**Can a result with `itemOmitted` be followed by a result with an image?**
Yes. Every node on the way back caps the bytes it sends over the operations it owns, and the
coordinator caps the merged array over the images that reached it. An image dropped by one partition
does not make the coordinator drop the images other partitions kept. The total stays under the cap on
every hop, the outcome codes do not depend on the layout, and the image set can. The TSDoc on
`TransactWriteOperationResult` states this.

**Does the results array appear on a committed transaction?**
No. Every operation passed, so the array holds no information. `ReturnValues` on a successful write
needs a per-operation result on the committed path, and that is a separate RFC.

**What does a caller see when it asks for an image and the item does not exist?**
A rejected result with no `item` and no `itemOmitted`. The condition failed because the item is
absent, and there is nothing to return.

**Why does a rejected participant keep its images in storage until `CANCELLING`?**
The coordinator cannot decide the transaction until every participant answers, and it can stop
between the two moments. Storage is the only place an answer survives that. The coordinator deletes
the images the cap drops when it writes `CANCELLING`.

---

## 8. References

- `docs/agent-plans/2026-09-02-update-expressions.md`
- `docs/agent-plans/2026-08-29-typed-expression-engine-spec.md`
- `docs/agent-plans/2026-09-05-item-order-timestamps-and-read-revisions.md`
- [DynamoDB UpdateItem API reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html)
- [DynamoDB TransactWriteItems API reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html)
- [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
