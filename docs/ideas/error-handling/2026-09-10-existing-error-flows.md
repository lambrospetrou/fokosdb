# Findings — the error flows of the public API today

**State:** Findings. This document records the current behaviour. It proposes no solution.
**Date:** 2026-09-10
**Author:** Lambros
**Scope:** every operation of `packages/fokosdb/src/client/db.ts`, and each layer that supplies an
error to it.

References:

- `docs/agent-plans/2026-09-08-return-values-on-condition-check-failure.md`
- `README.md`, the "Features" list
- [Cloudflare Workers RPC](https://developers.cloudflare.com/workers/runtime-apis/rpc/)
- [Durable Objects error handling](https://developers.cloudflare.com/durable-objects/best-practices/error-handling/)

Line numbers are correct at commit `b87356d`. Symbol names are the durable reference.

---

## Table of Contents

1. [Summary](#1-summary)
2. [The failure models in use](#2-the-failure-models-in-use)
3. [The error flow of each operation](#3-the-error-flow-of-each-operation)
4. [Findings](#4-findings)
5. [What the condition-image work already changed](#5-what-the-condition-image-work-already-changed)
6. [Constraint — an error is data across a hop, not a class](#6-constraint--an-error-is-data-across-a-hop-not-a-class)
7. [Open questions for the design](#7-open-questions-for-the-design)

---

## 1. Summary

The library has no error model. It has five mechanisms that grew one at a time. A caller cannot do
these four things:

1. Tell a caller fault from a server fault.
2. Tell a retryable failure from a permanent one.
3. Read a machine-readable code. Only `ExpressionError` supplies one.
4. Write one handler for one condition. A failed condition, a full partition and an invalid key each
   arrive in a different shape.

The result is visible in the example worker. `examples/http-api/index.ts:290` handles
`HTTPException` and `ExpressionError`. Every other error becomes HTTP 500 "Internal Server Error".
An empty hash key, a failed condition, and "please retry later" all return 500.

`README.md` already records the goal: "Proper structured errors thrown to differentiate user vs
server errors. Pick ONE failure model."

---

## 2. The failure models in use

| # | Mechanism | Where it is used | Machine-readable | Reaches the caller |
| --- | --- | --- | --- | --- |
| A | `throw new Error(...)` with a text prefix | validation, locks, migration, size, routing | no | yes |
| B | `throw new ExpressionError(code, ...)` | `shared/expression/*` | yes, `code` | yes. It is one of two classes the client barrel exports |
| C | `throw new ConditionCheckFailedError(...)` | `putItem`, `deleteItem` | yes, through `reason` | yes |
| D | A returned union, `{ outcome: "cancelled" \| "aborted", reason }` | `transactWriteItems`, `transactGetItems` | yes | yes |
| E | A message substring plus an `is*Error()` predicate | `shared/partition-errors.ts`, `shared/cf-utils.ts`, `server/do-transaction-coordinator.ts`, `server/do-partition.ts:2376` | no | no. No barrel exports the predicates |

Mechanism F is `invariant()` in `shared/invariant.ts`. It raises `Error("invariant_failed: ...")`.
The message reaches the caller and names internal state.

### 2.1 The text prefixes do not agree

Seven prefixes are in use, and some sites have none:

| Prefix | Example |
| --- | --- |
| `fokos:` | `shared/transaction-limits.ts:140` |
| `fokosdb:` | `client/db.ts:185`, `shared/transaction-limits.ts:61` |
| `fokos/<operation>:` | `client/db.ts:540`, `server/do-partition.ts:518` |
| `fokos/partition:` | `shared/partition-errors.ts:33` |
| `fokos/tc:` | `server/do-transaction-coordinator.ts:478` |
| `fokos/KeyCodec.encode:` | `shared/partition-topology/key-codec.ts:53` |
| `invariant_failed:` | `shared/invariant.ts:29` |
| none | `shared/cache-lru.ts:13`, `shared/tsutils.ts:2` |

`client/db.ts` uses `fokos:` and `fokosdb:` in the same file.

---

## 3. The error flow of each operation

### 3.1 `putItem`

| Failure | Mechanism | Site |
| --- | --- | --- |
| `ttlAt` is not an integer, or is not more than zero | A | `client/db.ts:135` |
| `returnValuesOnConditionCheckFailure` is not `none` or `all_old` | A | `shared/transaction-limits.ts:288` |
| The hash key or sort key is empty, holds NUL, or is not well-formed UTF-16 | A | `shared/transaction-limits.ts:126` |
| The encoded key is above its byte cap | A | `shared/transaction-limits.ts:155` |
| `data` is a primitive, is circular, or holds a BigInt | A | `client/db.ts:76` |
| `data` is above the item cap | A | `shared/transaction-limits.ts:91` |
| The condition expression is not valid | B | `shared/expression/*` |
| **The condition failed** | **C** | `client/db.ts:221` |
| An in-flight transaction holds the item | A | `server/do-partition.ts:518` |
| The partition is above its size cap | A + E | `shared/partition-errors.ts:33` |
| The partition migrates after a split | A | `server/do-partition.ts:1781` |
| The range DO is not initialised (phantom bounce) | A + E | `server/do-partition.ts:1723` |
| The partition context does not match | F | `ensurePartitionContext` |
| The stored row is above the cap | A | `shared/partition/partition-store.ts` |
| The Durable Object is overloaded, or the transport failed | raw Cloudflare error | — |

`putItem` never retries.

### 3.2 `getItem`

Key validation raises A. A migration does **not** fail the read: `ensureMigration("getItem", false)`
falls back to the parent partition. A corrupt stored JSON document raises A at `client/db.ts:106`,
after a `console.error`. `getItem` never retries.

### 3.3 `deleteItem`

The same set as `putItem`, without the data checks. A failed condition raises C at
`client/db.ts:261`. Note that "the item was absent" is the returned field `deleted: false`, and a
failed condition is a thrown error.

### 3.4 `queryItems`

| Failure | Mechanism | Site |
| --- | --- | --- |
| `queries` is empty | A | `client/db.ts:540` |
| `limit` or `maxPageBytes` is not a positive integer | A | `client/db.ts:543` |
| Six separate cursor faults | A | `client/db.ts:572`, `shared/query/cursor.ts:46` |
| A sort-key bound is an empty string | A | `shared/partition-topology/key-codec.ts:53` |

A migration falls back to the parent, as `getItem` does. `queryItems` never retries.

Two notes:

- The empty sort-key bound raises `fokos/KeyCodec.encode: empty string key is not allowed`. The
  message names an internal class. `begins_with: ""` is short-circuited before the encode and is
  safe, but `{ op: "eq", value: "" }` reaches the encoder.
- When the partition-visit budget runs out, `queryItems` writes a `console.warn` and paginates
  early. The caller receives a `cursor` and cannot tell an exhausted budget from a normal next page.

### 3.5 `transactWriteItems`

Client validation raises A and B. The call then takes one of two paths, and the two paths do not
fail in the same way.

**The single-shot fast path** (`#writeSingleShotFastPath`):

- The fallback sentinel makes the client run the coordinator path. The caller sees nothing.
- An over-size partition becomes `{ outcome: "cancelled", reason: { type: "transient_error" } }`,
  with every operation marked `not_evaluated`.
- **Every other error is thrown.** A migration error and an `ExpressionError` reach the caller as a
  throw.

**The two-phase path**:

- Every prepare that threw after its retries becomes `{ type: "transient_error" }`
  (`server/do-transaction-coordinator.ts:572`). A transport failure, a migration error, an
  `ExpressionError`, a mis-route and an over-size partition are one value.
- Two retryable states are thrown, not returned: `COMMIT_PENDING_SENTINEL`
  (`do-transaction-coordinator.ts:439`) and `UNDECIDED_SENTINEL` (`:478`).
- A reused `clientRequestToken` with a different operation set is thrown.

### 3.6 `transactGetItems`

**The fast path** retries only when `isErrorRetryable(err)` is true, three times, then throws.

**The two-phase path** retries **every** error five times
(`client/db.ts:458`, `:483`), then returns `{ outcome: "aborted", reason: "transient_error" }`.

The read path has its own reason vocabulary: the flat strings `read_conflict`, `pending_write` and
`transient_error`. It shares no shape with `RejectionReason`.

### 3.7 `destroy`

`destroy` swallows only the abort sentinel. Any other error is thrown in the middle of the
traversal, and the table is left in a partial state.

---

## 4. Findings

### F1 — One fact has two shapes

A failed condition is a thrown `ConditionCheckFailedError` in `putItem` and `deleteItem`. It is a
returned `cancelled` value in `transactWriteItems`. A caller cannot use one handler.

### F2 — The same call fails differently because of data placement

`transactWriteItems` over one partition **throws** when the partition migrates. The same call over
two partitions **returns** `cancelled` with `transient_error`. `transactGetItems` behaves the same
way: the single-partition path throws, and the multi-partition path returns `aborted`.

Placement is invisible to the caller, and a split changes it. The failure model of an operation must
not depend on where the data is.

### F3 — `transient_error` loses the cause

Every prepare failure becomes `transient_error`. A deterministic fault (an `ExpressionError` from a
bad plan, a permanent mis-route) and a true transient fault (a network blip) are one value. A caller
that retries `transient_error` loops on the deterministic ones.

The condition-image RFC records the same gap for one case: it wants "a size-rejected prepare [to]
say so instead of reporting `transient_error`".

### F4 — The caller cannot classify an error

Four predicates exist and none is exported:

- `isPartitionExceededDatabaseSizeError`
- `isSinglePartitionFastPathFallbackError`
- `isTransactionUndecidedError`
- `isTransactionCommitPendingError`

`client/index.ts` exports two classes: `ExpressionError` and `ConditionCheckFailedError`. A caller
that must know why a write failed has to match on message text.

The example worker shows the cost. It was not updated for `ConditionCheckFailedError`, so a failed
condition is still an HTTP 500 there.

### F5 — Nothing says whether a retry can help

These four are the same type today:

| Message | Retry helps |
| --- | --- |
| `Partition split in progress, please retry later.` | yes |
| `item is locked by an in-progress transaction ... retry later.` | yes |
| `fokos: hashKey must not be empty` | no. Caller fault |
| `condition failed` | no. The premise was wrong |

### F6 — The retry policy is arbitrary

| Path | Policy |
| --- | --- |
| `putItem`, `getItem`, `deleteItem`, `queryItems` | none |
| `#readSnapshotFastPath` | `isErrorRetryable`, 3 attempts |
| `#readTransaction` phase 1 and phase 2 | **any error**, 5 attempts |
| Coordinator prepare | any error except over-size, 3 attempts |
| Coordinator commit | any error except over-size, 5 attempts |

`#readTransaction` retries a deterministic fault five times with backoff before it answers with a
`transient_error` that carries no cause.

### F7 — No error carries structured data

Outside `ExpressionError` and `ConditionCheckFailedError`, no thrown error carries a code, a
category, a retryable marker, a key, a transaction id or an identifier. Two sites in `client/db.ts`
set `cause`. No other site does.

Control flow across the RPC hop matches on `message.includes(...)`. An edit to a message breaks the
control flow, and no test catches it at the type level. Two sentinels must also not contain each
other, which is a rule that only a comment states.

### F8 — A server fault and a caller fault have one type

These reach the caller as a plain `Error`, and nothing separates them from `hashKey must not be
empty`:

- `invariant_failed: fokos/partition.ensurePartitionContext: partition context mismatch`
- `fokos/partition.commit: pending_transactions has N items but request has M ...`
- `fokos: failed to parse json item data returned by the store`
- `fokos/KeyCodec.encode: empty string key is not allowed`

The messages also leak internal identifiers: DO names, partition ids, transaction ids and row
counts.

### F9 — Two reason vocabularies

`RejectionReason` has eight variants and carries keys. The read path has three flat strings. Neither
relates to `ExpressionErrorCode`, and neither relates to the set of thrown errors on the
non-transactional path.

### F10 — Silent degradation

- `queryItems` warns to the console and paginates early when the visit budget runs out. The caller
  sees only a cursor.
- `loadFinalResponse` turns a torn coordinator row into `transient_error` rather than reporting
  corruption (`do-transaction-coordinator.ts:443`).

Both hide a real event behind a value that means something else.

### F11 — A getter on an error does not cross a hop

`ConditionCheckFailedError` in `shared/partition-errors.ts` exposes the old item image through an
accessor:

```ts
get item(): ConditionCheckImage | undefined {
	return this.reason.type === "condition_failed" ? this.reason.item : undefined;
}
```

An accessor lives on the prototype, so it is not an own property. RPC serializes own properties
only. The class therefore has three own properties — `name`, `reason` and `meta` — and `item` is not
one of them. An instance that crosses a hop arrives with `item` absent.

Nothing is broken today, because `db.ts` builds the error in the caller's isolate and the value also
lives in the own property `reason.item`. The finding is about the rule it shows: **a computed field
on an error must be a data property, or its value must be reachable through one.** Section 6 states
the rule.

---

## 5. What the condition-image work already changed

`docs/agent-plans/2026-09-08-return-values-on-condition-check-failure.md` has landed since the first
pass of this analysis. It closed two findings that this document does not repeat:

1. **A per-operation result array.** A cancelled `transactWriteItems` now carries `results`, one
   entry for each operation, in request order, with the outcomes `passed`, `not_evaluated` and
   `rejected`. The `FIXME` in `shared/transaction-types.ts` is resolved.
2. **A typed error for a failed condition.** `putItem` and `deleteItem` raise
   `ConditionCheckFailedError`, which carries `reason`, `meta` and the old item image.

That work also set a precedent worth keeping: **the wire carries a returned union, and `db.ts`
builds the thrown class at the public boundary.** Section 6.2 of that RFC gives the reason, and
section 6 of this document states it as a rule: a class does not cross an RPC hop, so it must be
built on the side that raises it to the caller.

---

## 6. Constraint — an error is data across a hop, not a class

This is settled, and it shapes every choice in section 7.

### 6.1 The rule

A custom error class never survives a Workers RPC hop. The Cloudflare documentation lists what
crosses and what does not:

| Crosses the hop | Does not cross the hop |
| --- | --- |
| The effective `name` and `message` | The prototype, the class and the constructor |
| Serializable own properties, non-enumerable ones included | `instanceof` for a custom class |
| `cause` | Prototype methods and accessors |
| — | The original stack trace |

The guidance is direct: treat an error as a data contract of documented fields such as `name` and
`code`, and not as a class instance.

The rule is therefore **not** "the prototype must survive". It is the opposite:

> No classification, in this library or in the code that uses it, may depend on `instanceof`.

### 6.2 Where an error is caught, and what each place can use

| Boundary | Crosses a hop | What it can use |
| --- | --- | --- |
| A caller catches an error from `db.ts` | no. `db.ts` runs in the caller's isolate | `instanceof` works. This is where almost every `catch` block is |
| `db.ts` catches an error from a `PartitionDO` or the coordinator | yes | `name` and `code`. `db.ts` then builds the class locally |
| The coordinator catches an error from a participant | yes | `name` and `code`. Message substrings do this job today |
| A `PartitionDO` catches an error from a child in a forwarding chain | yes | `name` and `code` |
| A caller's own middle-layer Worker rethrows to its caller | yes | `name` and `code`. This library cannot change it |

The last row is the reason the rule binds the public API and not only the internals. A consumer that
puts a service Worker in front of FokosDB and lets an error propagate gives its own caller a
reconstructed object with no prototype. If the documented way to recognise a failed condition is
`instanceof`, that consumer's outer Worker cannot recognise one.

### 6.3 What follows for the error classes

1. `name`, `type`, `code`, `error_id`, the caller-or-internal marker and the retryable marker must
   be own data properties, assigned in the constructor. Those are the fields that cross a hop.
2. No accessors. See F11. A computed field must be a data property, or its value must be reachable
   through one.
3. A duck-typed predicate is the documented way to classify — a check on `name`, or on `code`, or a
   shared `isFokosError(e)` helper. `instanceof` stays a convenience that works in-process only.
4. Every custom attribute of an error code must be serializable. A `Uint8Array` key, a nested reason
   object and a `cause` all qualify. A function, a class instance and a getter do not.
5. `error_id` gains its value from this rule. It is a plain string, so it crosses every hop
   unchanged: a middle layer can log it, and the outermost caller can report the same identifier.

---

## 7. Open questions for the design

1. Does every failure become a thrown error, including the transaction outcomes that are returned
   values today? A `cancelled` transaction is an outcome, not a fault, so the answer is not obvious
   for `transactWriteItems` and `transactGetItems`.
2. Is `RejectionReason` the one vocabulary for both the transactional and non-transactional paths,
   or does a new code set replace it?
3. Section 6 settles that the hop classifies on `name` and `code`, and not on a message substring.
   What is open is the fallback: `enhanced_error_serialization` is on by default only from
   compatibility date `2026-04-21`, and this library does not own the `compatibility_date` of the
   worker that imports it. A consumer on an earlier date, or one that sets
   `legacy_error_serialization`, receives no custom own properties. Three options: keep the
   substring as a second test, carry the code in a returned union instead of on the error, or
   declare a minimum compatibility date.
4. Where does the retry policy live? Today it is spread over six sites with four different rules.
