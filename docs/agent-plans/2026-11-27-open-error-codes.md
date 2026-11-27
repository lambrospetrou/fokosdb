# RFC — Closed error categories, open error codes

**State:** Under-review
**Date:** 2026-11-27
**Author:** Lambros

## Table of contents

1. [Overview and context](#1-overview-and-context)
2. [Goals and requirements](#2-goals-and-requirements)
3. [Milestones](#3-milestones)
4. [Proposed solution](#4-proposed-solution)
5. [Alternative options](#5-alternative-options)
6. [Frequently asked questions](#6-frequently-asked-questions)
7. [References](#7-references)

## 1. Overview and context

Every error of the library extends `FokosError` (`shared/errors.ts`).
An error has a category (`name` and `_tag`, for example `FokosConflictError`) and a code (`code`, for example
`read_conflict`).
`defineCodes` makes the code tables, and each code definition has its category, its origin, and its HTTP status hint.

The library has 85 codes in 9 categories now.
`FokosAnyError` (`shared/errors-operations.ts`) is the closed union of all of them, and
`FokosErrorCode = FokosAnyError["code"]`.
The package entries export these closed unions:

- `fokosdb/client` exports `FokosAnyError`, `FokosErrorCode`, `isFokosAnyError`, and `FokosDBError`, which is
  `FokosAnyError & { meta? }`.
- `fokosdb/sharding/client` exports `FokosShardingError`, the closed union of the sharding codes
  (`sharding/errors.ts`).

One code type does two jobs:

1. **Internal control flow.** The server code must know each code exactly, so that a switch is exhaustive.
   A closed union is correct for this job.
2. **The caller contract.** The caller needs a set that can grow with no breaking change.
   A closed union is wrong for this job.

This causes four defects.

**A new code breaks the build of a caller.**
A caller can write an exhaustive switch on `e.code` with `assertNever` in the `default` branch.
When a release adds a code, for example a new validation code, that switch no longer compiles.
New codes are the most frequent change to the error surface.

**The public type exposes internal codes.**
16 of the 85 codes have the origin `internal`, for example `repartition_slice_reclaimed`,
`sharding_local_must_be_sync`, `partition_fanout_failed`, and `commit_keyset_mismatch`.
Some of them never reach a caller in a normal run, for example `hash_partition_not_initialized`, because the client
falls back to the root.
A caller sees them in the editor and can write a case for them.
After that, a rename or a removal of an internal code is a breaking change.
`ExecutionFailureCode = Exclude<FokosErrorCode, PremiseRejectionCode>` (`shared/transaction-api-types.ts`) puts the
same codes into the results of a cancelled transaction.

**The guard is false for a code it does not know.**
`isFokosDBError` uses `defineErrorGuard`, which is true only for a code of its tables.
During a gradual deploy, this sequence occurs:

1. A new server raises a new code `x`.
2. An old Worker gets the error and calls `isFokosDBError(e)`. The result is `false`.
3. The Worker handles a FokosDB error as an unknown error, and it loses the category and the `error_id`.

The type is also false at runtime.
`FokosError.wrap` gives back any `FokosError` with its code unchanged.
`client/db.ts` casts `err.code as FokosErrorCode` and `as ExecutionFailureCode` at 4 sites.

**A caller cannot tell if a retry can succeed.**
`FokosError` has no `retryable` field.
`origin` does not answer the question.
For example, `item_locked_by_transaction` has the origin `caller` and the hint 409, but a retry later can succeed.
`isRuntimeRetryableError` reads only the marker of the Workers runtime, and `fokosdb/client` does not export it.
A rule by category is not sufficient, because one code can be retryable in one case and not in another.
Only the code that raises the error knows the case.

A change of the code type only (`Known | (string & {})` in each category) does not remove these defects.
Section 5 gives the reasons.

The sharding layer is a library on its own, and a host can use it without FokosDB.
A host also defines its own codes with `defineCodes`.
So the solution must be in `shared/errors.ts`, which imports nothing, and not in the FokosDB modules.

The package is not released yet.
A breaking change of the public types costs the least now.

## 2. Goals and requirements

In scope:

- The set of categories is the error contract, and it is closed. An exhaustive switch on `_tag` compiles across
  releases that add codes.
- No type that a package entry exports has a union of codes. `code` is `string` in every public type.
- A new code is not a breaking change. A removed code is a breaking change.
- A caller checks a known code with an exported code definition, and a misspelled code does not compile.
- A typed payload is selected by a closed field (`outcome` or the category), never by an open code.
- The results of a cancelled transaction put "the partition could not run the operation" in a new `failed` outcome.
- Each error has an own field `retryable: boolean`. The code that raises the error sets it.
- One guard factory in `shared/errors.ts` makes a guard by category. FokosDB, the sharding layer, and a host
  package each make their guard with it.
- The closed unions `FokosAnyError` and `FokosShardingAnyError` stay for the server code. No package entry exports
  them.
- The category set covers each action that a caller can take now or in planned work. `FokosRoutingError` merges into
  `FokosInternalError`, and `FokosExpressionError` merges into `FokosValidationError`. A new `FokosThrottledError`
  reports an overloaded partition. A new `FokosUnprocessableError` reports a valid request that fails in the current
  state of the item.

Out of scope:

- A mapping of internal codes to new public codes at the client edge. Section 5 gives the reason.
  The current mapping of `repartition_not_cut_over` to `partition_migrating` in `client/db.ts`
  (`mapInternalErrorToPublic`, `publicBatchCode`) stays.
- Typed `attributes` for each code. `attributes` stays `Record<string, unknown>`, with documented keys.
  A stable payload goes into a typed field of a category, as `reason`, `results`, and `meta` do now.
- A change of `origin` or of `httpStatusHint` for a code that keeps its category. Section 6 says why
  `partition_context_mismatch` keeps the origin `internal`.
- A protocol version on the wire (item 24 of the audit).

Requirements:

- An error must keep every field across a Workers RPC hop and through `FokosError.toWire` and `FokosError.fromWire`.
  The classes hold own data properties only, as now.
- The helpers `isRuntimeRetryableError` and `isRuntimeFault` must keep their behavior. The new field must not change
  what they return.
- The server code must keep exhaustive checks over the codes it raises.

## 3. Milestones

1. **The `failed` outcome.** The transaction results and their wire type get the `failed` outcome (section 4.2.4).
   `RejectionReason` keeps only the premise codes.
   This milestone works on its own: the types stay closed, and every reader of the results compiles with exact
   narrowing.
2. **The category set.** `FokosRoutingError` and `FokosExpressionError` merge into other categories, and
   `FokosThrottledError` and `FokosUnprocessableError` are new (sections 4.2.1 and 4.2.2).
3. **Open public types.** The category guard factory, the public error types with `string` codes for FokosDB and for
   the sharding layer, and the new exports of both entries (sections 4.2.3, 4.2.5, and 4.2.6).
4. **The `retryable` field** (section 4.2.7).

## 4. Proposed solution

### 4.1 High-level overview

The design takes the model of gRPC status codes and of Google AIP-193.
A small and fixed set of statuses tells the caller what action to take.
An open reason string, which a known domain owns, tells the exact cause.

The rules:

1. **The category is the contract.** `_tag` tells the action: fix the request, retry later, read the state, or report
   a defect. The set is closed and small. A package adds a category only when it needs typed fields.
2. **The code is open data.** `code` is `string` in every public type. The code tables are the known codes, and the
   entries export them as values. `FokosError.isCode(e, DEF)` narrows an error to one known code.
3. **A payload is selected by a closed field.** When the shape of the data depends on what happened, `outcome` or the
   category selects the shape. An open code never selects it.
4. **`retryable` is a fact of each error.** The code that raises the error sets it, and it moves with the error.
5. **One guard machinery for every package.** `shared/errors.ts` makes guards by category. FokosDB and the sharding
   layer do not depend on each other for it.
6. **The closed unions are internal.** The server code keeps exact, exhaustive types. The caller never sees them.

```
                 public, stable                              internal, exact
  caller ──►  category (_tag): closed set          server ──►  FokosAnyError: closed union of every code
              code: string + exported code tables              exhaustive switch over codes
              retryable: boolean                               code tables (defineCodes)
              typed fields of a category
```

The effect of each future change:

| Future change | Today | With this design |
| --- | --- | --- |
| A new validation code | Breaking: an exhaustive switch fails | Not breaking: a new constant in a table |
| A new internal code | Breaking, and it is public | Not breaking, and it is not in a public type |
| A new code of a host package | The same defects as FokosDB | Not breaking: the same rules |
| A new premise kind with its own fields | Breaking | Breaking. The shape of the data changes, so this is correct |
| A new category | Breaking | Breaking. This is a change of the action set, and it is rare |

### 4.2 Technical details

#### 4.2.1 The categories

Each category has one action for the caller.
The gRPC status code of each category is the reason for it: gRPC and Google AIP-193 use one fixed set of statuses,
and each status tells an action.

| Category | Action of the caller | gRPC code | Typed fields |
| --- | --- | --- | --- |
| `FokosValidationError` | Fix the request. It fails in each state. | `INVALID_ARGUMENT`, `OUT_OF_RANGE` | |
| `FokosUnprocessableError` | This item does not fit the request. Change one of them. | `FAILED_PRECONDITION` | |
| `FokosConditionCheckError` | The condition failed. Read `reason`. | `FAILED_PRECONDITION` | `reason`, `meta` |
| `FokosConflictError` | Another operation conflicts. Retry later. | `ABORTED` | |
| `FokosTransactionCancelledError` | Read `results`. | `ABORTED` | `results`, `meta` |
| `FokosTransactionPendingError` | The outcome is not known. Replay, or read. | `DEADLINE_EXCEEDED` | |
| `FokosUnavailableError` | Retry later. | `UNAVAILABLE` | |
| `FokosThrottledError` | Wait, then retry with backoff. | `RESOURCE_EXHAUSTED` | |
| `FokosInternalError` | Report a defect, or retry when `retryable` is true. | `INTERNAL`, `UNKNOWN`, `DATA_LOSS` | |

`FokosTransactionPendingError` is more exact than `DEADLINE_EXCEEDED`.
It says that the outcome is not known, and it gives `attributes.idempotencyToken` for a replay.

The gRPC codes `NOT_FOUND`, `ALREADY_EXISTS`, `CANCELLED`, `UNIMPLEMENTED`, `PERMISSION_DENIED`, and
`UNAUTHENTICATED` have no category.
Section 6 gives the reason for each one.

A new category is a breaking change.
A package adds a category only when its errors need typed fields that no category has, or when a caller must take an
action that no category tells.

#### 4.2.2 Changes to the category set

**`FokosRoutingError` merges into `FokosInternalError`.**
`SHARDING_ROUTING_CODES` has 3 codes, and all of them have the origin `internal`.
For a caller, the action is the action of `FokosInternalError`, and gRPC also gives both one code.
The 3 codes move to the category `FokosInternalError` with no change of the code or of the segment.
The sharding code that checks them uses `FokosError.isCode` with their definitions, so it changes with the tables.
`FokosRoutingError` is removed from `shared/errors.ts` and from the exports.

Rule 4 of `classifyBatchFailure` (`shared/batch-failure.ts`) checks the category, so it must change.
The rule answers a `FokosError` with routing as `unprocessed`, except a `FokosInternalError` on a write, which is
`unknown`, because runtime code can raise one after a handler wrote.
The 3 routing codes come from a partition before any handler runs, and now they are `unprocessed`.
After the merge, the exception of rule 4 does not apply to a code of `SHARDING_ROUTING_CODES`, so a batch write keeps
the answer `unprocessed` for them.

**`FokosThrottledError` is new.**
A Durable Object can be overloaded, and then the Workers runtime puts `overloaded: true` on the error.
The Cloudflare documentation says that a caller must not send the call again at once, because a retry makes the
overload worse.
Now `FokosError.wrap` makes that error `foreign_error`, with the origin `internal` and the hint 500, because `wrap`
gives the origin `service` only when the runtime marker `retryable` is true.
So a caller sees a defect, but the cause is the load on one partition.

The category is in `shared/errors.ts`, because `wrap` is there and the sharding layer gets the same runtime errors.

- `FokosError.wrap` makes an error with the runtime marker `overloaded: true` a `FokosThrottledError`. It keeps the
  runtime markers in `attributes`, as it does now, so `isRuntimeRetryableError` and `isRuntimeFault` give the same
  results.
- Its origin is `service`, and its `retryable` field is `true`. The retries inside the library do not change:
  `isRuntimeRetryableError` stays false for an overloaded error, so the library does not send the call again at once.
- The Workers runtime raises `overloaded` before the request runs on the overloaded Durable Object. So the hop where
  the error occurs applied nothing.
- No object passes an overload error to its caller after its own work applied:
  - The coordinator stores the error of a participant as a prepare error, and the transaction cancels. The error is
    then a `failed` entry of the cancel.
  - A `group` dispatch puts the error of a remote part in `partition_fanout_failed`.
  - A router forwards a point operation, and a router owns no keys.

  So a `FokosThrottledError` that reaches `client/db.ts` means that nothing applied, and `retryable: true` is correct.
  A new path that applies work and then forwards an overload error must raise a different error with
  `retryable: false`.
- `undecidedOnRuntimeFault` and `fastPathUndecided` (`client/db.ts`) keep their check of `foreign_error`. A
  `FokosThrottledError` does not become `transaction_undecided`, because its outcome is known: nothing applied.
  The test of `tx-coordinator-fault.test.ts` for `{ overloaded: true }` injects the fault after the work, which the
  runtime does not do. It changes to inject the fault before the work, and it expects `FokosThrottledError` and no
  transaction.
- `classifyBatchFailure` answers a `FokosThrottledError` as `unprocessed`, because nothing applied. Now rule 5 answers
  it as `unknown` on a write.
- The code is `overloaded`, the same word as the runtime marker. Any Durable Object can be overloaded: a partition, a
  coordinator, or an admin object.
- The hint is 429. HTTP clients and proxies read it as "wait, then retry with backoff", and it is different from the
  503 of `FokosUnavailableError`, so a caller can tell load from a migration.
- A later `retryAfterMs` field, as `RetryInfo` of AIP-193, is an optional field, so it is not a breaking change.

**`FokosExpressionError` merges into `FokosValidationError`.**
`FokosExpressionError` has 1 code, `expression_invalid`, and the action of its caller is the action of
`FokosValidationError`.
The server code already handles the two categories as one:
`isFatalPrepare` (`server/do-transaction-coordinator.ts`) and `classifyBatchFailure` (`shared/batch-failure.ts`)
check `FokosValidationError.is(err) || FokosExpressionError.is(err)`.
gRPC also gives both `INVALID_ARGUMENT`.

- `EXPRESSION_CODES.expression_invalid` moves to the category `FokosValidationError`, with the same code and segment.
- The caller checks it with `FokosError.isCode(e, FOKOS_DB_CODES.expression_invalid)` in place of a check by `_tag`.
- `attributes.expressionCode` stays, and `ExpressionErrorCode` stays exported. Its type is documented, and it is not a
  compile-time type: rule 3 of section 4.1 does not allow an open code to select a typed payload.
- `FokosExpressionError` is removed from `shared/errors.ts` and from the exports.

**`FokosUnprocessableError` is new.**
gRPC separates two kinds of caller errors.
`INVALID_ARGUMENT` is for a request that fails in each state of the system.
`FAILED_PRECONDITION` is for a valid request that fails in the current state.
The library has errors of the second kind, and they are in `VALIDATION_CODES` now:

| Code | The cause, for this item |
| --- | --- |
| `update_not_applicable` | The update cannot apply to the stored item. |
| `update_value_is_bytes` | A `set` value evaluates to bytes, and a JSON document cannot hold bytes. |
| `update_ttl_invalid` | A `set` value of `ttlAt` is not a whole number greater than zero. |
| `item_too_large` | The stored row of the item is above the size limit, after a put or an update. |
| the evaluation failure of an expression | SQLite cannot evaluate the expression on the data of the item. |

The action of the caller is different from a validation error.
A validation error tells a defect in the code of the caller.
`FokosUnprocessableError` tells that this item does not fit the request, and the same request can succeed on other
items.
In a batch, the caller keeps the results of the other items.

Now the update codes reach a caller only as premise reasons of a cancelled transaction.
A single-item `updateItem` (audit item 6) raises them as errors, so the category must exist before the first release.

- The four codes move to a table of the category `FokosUnprocessableError`, with the same codes and segments.
  The premise codes of `RejectionReason` do not change.
- The origin is `caller`, the hint is 422, and `retryable` is `false`.
- `withExpressionErrors` raises a `FokosUnprocessableError` for an evaluation failure, and a `FokosValidationError`
  with `expression_invalid` for each other `ExpressionErrorCode`. The evaluation failure gets the new code
  `expression_evaluation_failed`, and the original `ExpressionErrorCode` stays in `attributes.expressionCode`.
- `runtime_capability` is the evaluation failure. `asEvaluationFailure` (`shared/expression/runtime.ts`) gives it for
  `SQLITE_ERROR` and `SQLITE_TOOBIG`. The compiler rejects the static defects first (types, paths, `sql_limit`), so
  an error at evaluation comes mostly from the data of the item. When the expression itself is the cause, each item
  fails with the same code, and the message tells it.
- `item_too_large` has one code for each write: a put and an update in a transaction (the precheck of
  `shared/partition/transaction-participant.ts`), and `putItem` (`throwItemTooLarge` in
  `shared/partition/partition-store.ts`). For a put, the size comes from the request and the keys. The client check
  `item_data_too_large` catches most puts that are too large first, so the partition check is a last guard.
- `isFatalPrepare` and `classifyBatchFailure` check `FokosValidationError` and `FokosUnprocessableError`.
  The same request fails again while the item stays the same, so a retry of the prepare cannot clear it, and a batch
  answers the item as `rejected`.
- The name must not use "execution". `ExecutionFailureCode` and the `failed` outcome mean that the partition could
  not run the operation, which is a fault of the service.

#### 4.2.3 Public types with open codes

Each package entry exports a union of its categories with `string` codes.
For FokosDB, in `shared/errors-operations.ts`:

```ts
export type FokosDBError = (
	| FokosValidationError<string>
	| FokosUnprocessableError<string>
	| FokosConditionCheckError<string>
	| FokosConflictError<string>
	| FokosTransactionCancelledError<string>
	| FokosTransactionPendingError<string>
	| FokosUnavailableError<string>
	| FokosThrottledError<string>
	| FokosInternalError<string>
) & { readonly meta?: FokosMeta };
```

`FokosDBError` moves from `client/db.ts` to `shared/errors-operations.ts`, with `isFokosDBError`.
`client/db.ts` imports them from there.

The sharding layer exports the same kind of union for its categories: validation, unavailable, throttled, and internal.
The public name stays `FokosShardingError`.
The closed union of the sharding codes stays in `sharding/errors.ts` for the server code, with the new name
`FokosShardingAnyError`. The suffix `AnyError` marks each closed internal union.

A caller narrows a known code with a code definition:

```ts
if (FokosError.isCode(e, FOKOS_DB_CODES.hash_key_empty)) {
	// e.code is "hash_key_empty"
}
```

A misspelled code does not compile, and the check also compares the category.
The editor suggests the codes through the exported tables.
An exhaustive switch on `e.code` is not possible. An exhaustive switch on `e._tag` is.

The public result types use `string` for each code:

- `BatchGetItemResult` and `BatchWriteItemResult` (`shared/batch-api-types.ts`). `outcome` already selects the shape,
  and `reason` of a batch write is optional, so the change has no effect on narrowing.
- `BatchUnprocessedCode` becomes `string`. Its doc comment keeps the known value `response_too_large`.
- The `failed` outcome of a cancelled transaction (section 4.2.4).

The casts of a code go away, because `string` takes any code: 4 casts to `FokosErrorCode` and to
`ExecutionFailureCode` in `client/db.ts`, 1 cast to `ExecutionFailureCode` in `server/do-transaction-coordinator.ts`,
and 1 cast to `BatchUnprocessedCode` in `classifyBatchFailure` (`shared/batch-failure.ts`).

#### 4.2.4 The `failed` outcome of a cancelled transaction

`RejectionReason` is a union, and `code` selects its member.
8 members are premise codes (`PremiseRejectionCode`): `condition_failed`, `timestamp_conflict`, `pending_conflict`,
`clock_skew`, `update_not_applicable`, `update_value_is_bytes`, `update_ttl_invalid`, and `item_too_large`.
Some of them have their own fields, for example `item` of `condition_failed` and `conflictingTransactionId` of
`pending_conflict`.
The last member is `{ code: ExecutionFailureCode; hashKey; sortKey?; error_id }`: the partition could not run the
operation.

An open code in the last member breaks the narrowing of the union.
TypeScript cannot express "any string except these 8 codes", so `reason.code === "condition_failed"` keeps the last
member, and `reason.item` does not compile.
A trial of an open `ExecutionFailureCode` failed the type check at 12 sites in `client/db.ts`,
`server/do-transaction-coordinator.ts`, and one test.
Each caller that reads `reason.item` or `reason.conflictingTransactionId` fails in the same way.

So the failure moves to its own outcome:

```ts
export type TransactWriteOperationResult =
	| { outcome: "passed" }
	| { outcome: "not_evaluated" }
	| { outcome: "rejected"; reason: RejectionReason; partitionIndex?: number; itemOmitted?: "response_too_large" }
	| { outcome: "failed"; code: string; error_id: string; partitionIndex?: number };
```

- `rejected`: a premise of the operation did not hold. `RejectionReason` has only the 8 premise members, and their
  codes stay a closed union. A new premise kind changes the shape of the data, so it is a breaking change.
- `failed`: the partition that owns the operation could not run it. Every operation of that partition has the same
  `code` and the same `error_id`, as now.
- `failed` holds no keys, as the batch results. `results[i]` answers `items[i]` of the request.
  No code reads the keys of an execution failure now. The coordinator reads `reason.hashKey` only for
  `condition_failed`, to build the old item image again.

`batchWriteItems` already uses this pattern: `rejected` and `unprocessed` are different outcomes.

The change also applies to:

- The wire type `RejectionReasonEncoded` (`shared/transaction-wire-types.ts`) and the wire form of the operation
  results.
- Each producer of an execution failure: the participant (`shared/partition/transaction-participant.ts`), the
  coordinator (`server/do-transaction-coordinator.ts`), and the fast path in `client/db.ts`.
- The constructor of `FokosTransactionCancelledError`. It reads the code definition of each `rejected` entry and of
  each `failed` entry to choose `origin` and `httpStatusHint`.
- `examples/http-api`, which serializes the results.

The coordinator stores the results.
The package is not released, so the stored format changes with no conversion.

#### 4.2.5 The category guard

`shared/errors.ts` gets a factory:

```ts
export function defineCategoryGuard<U extends FokosError>(
	...categories: ReadonlyArray<{ readonly tag: string }>
): (e: unknown) => e is U;
```

The guard is true when `FokosError.is(e)` is true and `e._tag` is the tag of one of the categories.
It does not read the code, so it is true for a code that this version does not know.

- FokosDB: `isFokosDBError = defineCategoryGuard<FokosDBError>(...)` with its 9 categories.
- Sharding: `isFokosShardingError = defineCategoryGuard<FokosShardingError>(...)` with its 4 categories.
- A host package makes its own guard in the same way.

`defineErrorGuard`, the guard by code, stays for the closed internal unions, for example `isFokosAnyError`.

#### 4.2.6 The exports of the entries

`fokosdb/client`:

- Exports `FokosDBError`, `isFokosDBError`, and `FokosError` with the category classes, as now.
- Exports one table, `FOKOS_DB_CODES`, with only the codes that a caller can act on. Each entry is the same code
  definition object as in its source table, so `FokosError.isCode` gives the same result with both. The table also
  has the sharding codes that a FokosDB caller acts on: `key_not_well_formed_utf16`, `key_encode_empty`,
  `partition_context_options_invalid`, and `partition_migrating`. It does not have `topology_event_invalid`,
  `repartition_not_cut_over`, or any internal code.
- Does not export the separate code tables.
- Does not export `FokosAnyError`, `FokosErrorCode`, or `isFokosAnyError`.

`fokosdb/sharding/client`:

- Exports the open `FokosShardingError`, `isFokosShardingError`, and the code tables, as now.
- Does not export `FokosShardingAnyError`.

The closed unions stay in their modules for the server code and for the tests.
An exhaustive switch in the server code still compiles, and it fails when a code has no case.

#### 4.2.7 The `retryable` field

`FokosError` gets an own field:

```ts
/** True when the same call, sent again with no change, is safe and can succeed. */
readonly retryable: boolean;
```

The rule of the field is strict, because a caller can send a call again with a generic wrapper.
The Cloudflare documentation tells callers to retry when `e.retryable` is true.
When the same call with no change can apply a write two times, the field must be `false`.

The value comes from these sources, in this order:

1. The call site. `FokosErrorOptions` gets `retryable?: boolean`, as it has `origin` and `httpStatusHint`.
2. The code definition. `defineCodes` gets a `retryable` default for each table.

The defaults:

| Codes | `retryable` |
| --- | --- |
| Every `FokosUnavailableError` code | `true` |
| Every `FokosConflictError` code, except `producer_epoch_fenced` | `true` |
| `foreign_error` | `false`. A read sets `true` for a runtime fault, as the next list says |
| Every `FokosThrottledError` code | `true` |
| Every `FokosTransactionCancelledError` and `FokosTransactionPendingError` code | `false`. The next list says why |
| `producer_epoch_fenced` | `false`. The receiver fences the same call again, because its stored epoch stays the same |
| Validation, unprocessable, condition check, and the other internal codes | `false` |

Two cases set the value:

- **A runtime fault (`foreign_error`)**: `FokosError.wrap` gives `false`. A runtime fault can come after the write, for
  example a lost reply, so the outcome of a write is not known. `classifyBatchFailure` answers it as `unknown`, and
  `fastPathUndecided` answers `transaction_undecided`. An operation that applies nothing sets `true` when the runtime
  marker is `retryable` and not `overloaded`: `getItem`, `queryItems`, `scanItems`, `batchGetItems`,
  `transactGetItems`, and the reads of `db.gsi(indexName)`. `client/db.ts` sets it where it raises the error of each
  read.

- **A transaction error**: `FokosTransactionCancelledError` and `FokosTransactionPendingError` always have
  `retryable: false`. A transaction is retried only when the library is sure that it is idempotent, and the library
  cannot be sure in either case:
  - A cancel applied nothing, but its premise did not hold. Example: a caller reads an item, computes a new value,
    and writes it with no condition. The transaction cancels with `timestamp_conflict`, because another client wrote
    the item. A retry of the same call commits the stale value, and the write of the other client is lost. Only the
    caller can decide if the request is still correct. It reads `results` for that.
  - A replay of a pending transaction with the same token is idempotent only inside the idempotency window. The
    coordinator deletes the record of a token `IDEMPOTENCY_WINDOW_MS` (10 minutes) after the transaction completes.
    A replay after that runs the transaction again. Also, when `client/db.ts` made the token, the same call makes a
    new token and starts a second transaction. The caller replays with `attributes.idempotencyToken` when it decides
    to.

  DynamoDB does the same: the AWS SDKs do not retry `TransactionCanceledException` on their own.

The field and the wire:

- `retryable` goes into `BASE_FIELD_NAMES`. The compile-time check in `shared/errors.ts` fails when it is absent.
- `FokosErrorWire` gets `retryable`. `fromWire` of a record with no `retryable` gives `false`.
- The Workers runtime keeps an own `retryable` property of an application error across an RPC hop.
  `test/tagged-error-rpc.test.ts` shows this for the runtime markers.

The field and the runtime marker:

- The runtime marker `retryable` is on a raw runtime error, and `FokosError.wrap` moves it to `attributes.retryable`.
- `isRuntimeRetryableError` and `isRuntimeFault` check `FokosError.is(e)` first, and then read only `e.attributes`.
  So the new own field does not change what they return.
- Each helper that reads the runtime marker must check `FokosError.is(e)` first. A comment on the two helpers states
  this rule.

### 4.3 Testing

- A type test: a `FokosDBError` with an unknown code compiles, and an exhaustive switch on `_tag` compiles.
- A type test: `reason.code === "condition_failed"` narrows `RejectionReason`, and `reason.item` compiles.
- `isFokosDBError` and `isFokosShardingError` are true for an unknown code of a known category, and false for an
  unknown category.
- A cancelled transaction with a partition that could not run its operations has `failed` entries with `code` and
  `error_id`. The existing tests of the cancel results change to the new outcome.
- `retryable` survives a Workers RPC hop and `toWire` with `fromWire`, for each category.
- `isRuntimeFault` and `isRuntimeRetryableError` are false for a `FokosConflictError` with `retryable: true`.
- An update that does not apply to the stored item, and an expression that fails on the data of one item, give
  `FokosUnprocessableError`. A batch answers that item as `rejected` and answers the other items.
- An error with the runtime marker `overloaded: true` becomes `FokosThrottledError` with `retryable: true`.
- `FokosTransactionCancelledError` and `FokosTransactionPendingError` have `retryable: false` in each case: with a
  token of the caller, with a token that `client/db.ts` made, and on the fast path.
- A put and an update whose stored row is over the limit give `item_too_large` in `FokosUnprocessableError`.
- A runtime fault with the marker `retryable` gives `retryable: true` on a read, and `retryable: false` on a write.
- A batch write answers a routed `partition_misrouted` as `unprocessed`, and a `FokosThrottledError` as `unprocessed`.

## 5. Alternative options

**A list of known codes in each category (`Known | (string & {})`).**
Each category keeps a union of its known codes, open with `(string & {})`.
Not chosen:

- Each new code needs a decision about its list, and a wrong decision is a contract change.
- Any union where an open code selects the member breaks narrowing. `RejectionReason` showed this at 12 sites, and
  each new type of this kind breaks again.
- It is in the FokosDB modules only. The sharding layer and a host package keep the same defects.

**A mapping table at the client edge.**
`docs/ideas/2026-10-10-api-boundary-freeze.md` (section 4.2.8) maps each internal code to `internal_error` or
`unavailable`, with the source code in `attributes.internalCode`.
Not chosen:

- It changes the code of an error between the server log and the caller, and the table must be kept current.
- `string` codes give the same protection to the caller with no change at runtime.
- It does not help the sharding layer.

**A helper that derives `retryable` from the category.**
An exported `isRetryableError(e)` reads the category, the origin, and the entries of a cancel.
Not chosen: one code can be retryable in one case and not in another, and only the code that raises the error knows
the case.

**Narrowing of `RejectionReason` by `error_id`.**
One `rejected` outcome with an open code. Only the failure member has `error_id`, so each reader checks
`"error_id" in reason` before it checks `code`.
Not chosen: each reader, callers too, must remember the order of the checks, and the compiler does not enforce it.

**A closed `ExecutionFailureCode`.**
Keep the closed list of every code in the transaction results.
Not chosen: the internal codes stay public in the transaction results.

## 6. Frequently asked questions

**Why is an exhaustive switch on codes not supported?**
Each new code breaks it, and new codes are the most frequent change.
An exhaustive switch on `_tag` is supported, and each category tells the action.

**How does a user of the sharding layer without FokosDB handle errors?**
The user calls `isFokosShardingError(e)`, switches on `e._tag`, and checks codes with `FokosError.isCode` and the
sharding code tables.
Nothing of it imports a FokosDB module.

**Why is `origin` not the retry signal?**
`origin` tells who must act: the caller, the service, or the developers.
`retryable` tells if the same call can succeed.
A conflict has the origin `caller`, and a retry later can succeed.

**Why do `NOT_FOUND`, `ALREADY_EXISTS`, `CANCELLED`, `UNIMPLEMENTED`, and the access control codes have no category?**
No case exists now, and no planned work makes one.

- `NOT_FOUND`: a read of an absent item gives an empty result, as in DynamoDB. A caller creates no tables or indexes.
  `db.gsi("nope")` gives an empty result, because a check needs one RPC for each new index name (audit item 13).
- `ALREADY_EXISTS`: a put that must not replace an item uses a condition, so it is a `FokosConditionCheckError`.
- `CANCELLED`: the methods take no `AbortSignal`.
- `UNIMPLEMENTED`: the planned protocol version (audit item 24) needs an error for an old object during a deploy. That
  error clears when the deploy completes, so it is a `FokosUnavailableError` with `retryable: true`.
- `PERMISSION_DENIED` and `UNAUTHENTICATED`: the host Worker does the access control, not the library.

A category with no codes cannot be tested, and its meaning can be wrong when the first real case comes.

**Why does `partition_context_mismatch` keep the origin `internal`, when its cause can be configuration?**
One cause is the deployment. For example, two deployments resolve different routing hash seeds for one shard group,
and `FokosShardingRuntime` (`sharding/runtime.ts`) raises the code when it checks the seed.
The operator must fix the deployment, and the code of the caller cannot fix it at runtime.
Internal errors already go to the operator, with the hint 500 and a log line.
The attributes name the expected value and the stored value.

**Does the server code lose exhaustive checks?**
No. `FokosAnyError` and `FokosShardingAnyError` stay, with every code as a literal.

## 7. References

- `docs/ideas/2026-11-24-public-api-and-wire-protocol-audit.md` (item 15)
- `docs/ideas/2026-10-10-api-boundary-freeze.md` (section 4.2.8)
- `docs/agent-plans/2026-09-10-structured-error-handling.md`
- [gRPC status codes](https://grpc.github.io/grpc/core/md_doc_statuscodes.html)
- [AIP-193: Errors](https://google.aip.dev/193)
- [Durable Objects error handling](https://developers.cloudflare.com/durable-objects/best-practices/error-handling/)
