# RFC — Structured errors across the FokosDB library

**State:** Completed
**Date:** 2026-09-10
**Author:** Lambros

---

## Table of Contents

1. [Overview and Context](#1-overview-and-context)
2. [Goals and Requirements](#2-goals-and-requirements)
3. [Milestones](#3-milestones)
4. [Proposed Solution](#4-proposed-solution)
5. [Alternative Options](#5-alternative-options)
6. [Frequently Asked Questions](#6-frequently-asked-questions)
7. [Appendix](#7-appendix)

---

## 1. Overview and Context

### 1.1 The problem

The library has no error model. It has five mechanisms that grew one at a time. `README.md` records
the goal: "Proper structured errors thrown to differentiate user vs server errors. Pick ONE failure
model."

A caller cannot do four things:

1. Tell a caller fault from an internal fault.
2. Tell a transient condition from a permanent one.
3. Read a machine-readable code. Only `ExpressionError` supplies one.
4. Write one handler for one condition. A failed condition, a full partition, and an invalid key each
   arrive in a different shape.

The example worker shows the cost. `api.onError` in `examples/http-api/index.ts` handles
`HTTPException` and `ExpressionError`. Every other error becomes HTTP 500 "Internal Server Error". An
empty hash key, a failed condition, and "please retry later" all return 500.

`docs/ideas/error-handling/2026-09-10-existing-error-flows.md` holds the full survey of the current
flows and the 11 findings this RFC closes.

### 1.2 What the reader must know about the current system

- `FokosDB` in `packages/fokosdb/src/client/db.ts` is the public boundary. It validates the input,
  encodes the keys and the data, and calls the Durable Object stubs.
- Two Durable Object classes do the work. `PartitionDO` in `packages/fokosdb/src/server/do-partition.ts`
  stores the items. `TransactionCoordinatorDO` in
  `packages/fokosdb/src/server/do-transaction-coordinator.ts` drives two-phase commit.
- A `PartitionDO` forwards an operation to a child partition when it has split. `withSplitForwarding`
  and `forwardToRangeRootPartition` own that forwarding.
- Workers RPC carries the `name` and the `message` of an error, plus its serializable own properties,
  including non-enumerable ones such as `cause`. It does not carry the prototype, the class, the
  property descriptors, or the stack. `instanceof` for a custom class fails after the hop.
- The `enhanced_error_serialization` compatibility flag controls the own-property behaviour. The flag
  is on by default from compatibility date `2026-04-21`. Before that date, RPC uses legacy error
  reconstruction and drops custom own properties. The flag must be on for both the caller and the
  callee of a hop.
- `ctx.storage.transactionSync(callback)` rolls back when the callback throws. It commits when the
  callback returns. The source has 20 such blocks.
- `tryWhile` from `durable-utils` retries a function that throws. The source has 11 call sites.
- Five errors cross an RPC boundary and a predicate matches each one by a substring of the message.
  `isPartitionExceededDatabaseSizeError` and `isSinglePartitionFastPathFallbackError` live in
  `packages/fokosdb/src/shared/partition-errors.ts`. `isTransactionUndecidedError` and
  `isTransactionCommitPendingError` live in `do-transaction-coordinator.ts`. `isPhantomBounceError`
  in `do-partition.ts` matches `"phantom-bounce"`, which a fresh range DO throws across the hop to
  `maybeForwardToRangeRootPartition`.
- Every Durable Object response carries `meta`. `meta._internal.rangeAncestors` holds the boundaries
  of the serving leaf. `recordForwardResult` caches them, so a later request skips the router chain.
  The two call sites run after a successful forward. A throw skips both.
- `docs/agent-plans/2026-09-08-return-values-on-condition-check-failure.md` has landed. A cancelled
  `transactWriteItems` now carries a `results` array, one entry for each operation, in request order.
  `putItem` and `deleteItem` raise `ConditionCheckFailedError`. The partition returns
  `{ outcome: "rejected", reason }` over RPC and `db.ts` builds the class.

### 1.3 Glossary

| Term | Meaning in this document |
| --- | --- |
| Category | The coarse error class. It is the value of `name` and of `_tag`. |
| Code | The fine-grained identifier of one failure, in snake case. |
| Origin | The field that says where the fault sits: `caller`, `service`, or `internal`. |
| Happy-path answer | A successful result, including a negative one such as `found: false`. |
| Hop | One Workers RPC call between two Durable Objects, or between a Worker and a Durable Object. |
| Stamp | The act of attaching routing meta to an error before the error leaves a partition. |

---

## 2. Goals and Requirements

### 2.1 In scope

1. Every error that `FokosDB` raises must be a `FokosError`. A fault from outside the library must be
   wrapped in one.
2. Every error must carry a category, a code, an `error_id`, an origin, and an `httpStatusHint`.
3. The category and the code must be contractual. The message must not be contractual.
4. Classification must never depend on `instanceof`.
5. The five message-substring predicates must be removed. A code check replaces each one; the
   phantom-bounce catch in `maybeForwardToRangeRootPartition` reads
   `FokosError.isCode(e, ROUTING_CODES.range_partition_not_initialized)`.
6. A cancelled `transactWriteItems` must report each operation as plain data, not as nested error
   objects. A rejected entry keeps its `RejectionReason` record, whose `code` discriminant holds the
   code of section 7.1.
7. A failed prepare must keep the cause of the failure. It must not report `transient_error` alone.
8. A partition must stamp its routing meta on an error. Each level that learns from the meta of a
   successful response must learn from the stamped meta in the same way, with the same meta changes
   at each hop. This is the last milestone.
9. A failed condition must carry the code `condition_failed`, whichever operation the caller used.
   `putItem` and `deleteItem` raise `FokosConditionCheckError` with that code. A transaction raises
   `FokosTransactionCancelledError`, the error for the whole transaction, and each rejected operation
   carries `reason.code === "condition_failed"` in its `results` entry.
10. `destroy()` must stop at the first failure.
11. A transaction must raise the same exception, category, and code for one failure on the
    single-partition fast path and on the two-phase path. Section 4.2.5 holds the tables.

### 2.2 Out of scope

| Item | Reason |
| --- | --- |
| `Result<T, E>` helpers and `tryXyz()` methods | A later RFC. This RFC throws everywhere and adds no `Result` API. |
| Early pagination of `queryItems` on an exhausted budget | Metadata, not a failure. It belongs in `meta`. |
| `FokosDB.updateItem` | It does not exist yet. The shapes here extend to it without a change. |
| A change to the expression sub-library | A wrapper carries `ExpressionError`. See section 4.2.7. |
| A `transient` field and a shared retry policy | A later RFC. The caller decides when to retry. The internal retry predicates keep the behaviour they have today. See section 4.2.9. |

### 2.3 Constraints

1. Every Worker that runs `FokosDB` and every Worker that exports the Durable Object classes must set
   a `compatibility_date` of `2026-04-21` or later. On an older date an error crosses a hop without
   its own properties, so the `code` and the `_tag` do not arrive.
2. The error classes must live under `packages/fokosdb/src/shared/`. The client matches on them, and
   the `check-client-bundle` plugin fails the build when a client chunk reaches `src/server/`.
3. The client bundle has a size budget. The number of classes must stay between 6 and 10. The code
   carries the fine detail.
4. Every field that must cross a hop must be an own data property. A getter, a prototype method, and
   a computed accessor must not hold data that a caller needs.
5. A message must stay as static as practical. Cloudflare Observability groups on the message, and a
   message that changes on every call defeats that grouping. The dynamic detail belongs in the
   attributes.
6. A caller-visible message must not carry an internal identifier. A Durable Object name, a partition
   id, and a transaction id belong in the attributes.

---

## 3. Milestones

The candidates below follow the structure of section 4, and each one delivers on its own.

1. The base class, the category classes, and the code tables.
2. The item operations: `putItem`, `getItem`, `deleteItem`, `queryItems`, and the validation helpers.
3. The partition internals: the wrap in `#rpc`, and the removal of the three partition-side message
   predicates.
4. The transaction paths: the same errors on the fast path and on the two-phase path (section
   4.2.5), the stored cause of a failed prepare, the thrown abort for `transactGetItems`, and the
   removal of the two coordinator predicates.
5. The test migration from message assertions to code assertions.
6. The example worker: the `httpStatusHint` mapping in `api.onError`, the `transactWriteItems` route,
   and the demo page.
7. The routing meta on errors: the stamp in `#rpc` and the learning in the two forwarding paths, with
   the same behaviour as the success path (section 4.2.8). This milestone starts after milestones 1
   to 6, when every error flows correctly.

---

## 4. Proposed Solution

### 4.1 High-level overview

Every failure becomes a thrown error. Each error extends one base class, `FokosError`, which the
library owns. The library uses the classes as ordinary errors. It throws them and catches them, and it
adds no `Result` type. Section 5.4 gives the reason the library writes the base class instead of using
a tagged-error library.

Three facts decide that the library throws rather than returns a failure.

1. `ctx.storage.transactionSync` rolls back when its callback throws, and commits when the callback
   returns. A returned failure inside one of the 20 blocks commits a partial write.
2. `tryWhile` retries a function that throws. A returned failure never retries, so all 11 call sites
   would need a second mechanism.
3. SQLite, the RPC transport, an overloaded Durable Object, and `ctx.abort` all throw. A returned
   failure gives the library two error systems instead of one.

An error carries a category, a code, an `error_id`, an origin, and an `httpStatusHint` that a caller
can act on, and any number of attributes for its own code.

```
FokosConflictError
  name            "FokosConflictError"        <- the category, and the value of _tag
  type            "conflict_error"            <- the category in snake case
  code            "item_locked_by_transaction" <- the fine-grained identifier
  error_id        "e_vnfeg6_<32 hex>"         <- unique to this one event
  origin          "caller"                    <- "caller", "service" or "internal"
  httpStatusHint  409
  attributes      { transactionId, hashKey, sortKey }
```

Every field in the sketch is an own data property, so each one crosses an RPC hop. The classes
declare no instance methods, and every helper is static (section 4.2.1). The prototype does not cross
a hop, so the library never classifies with `instanceof`. It compares `name` or `code`, and it gives
callers a guard that does the same.

One rule has one exception. A participant answers about N operations at once, and a throw carries one
answer. So the prepare and single-shot RPCs keep a returned union, and `db.ts` converts it to a
thrown `FokosTransactionCancelledError`. Section 4.2.5 states the exception, and section 5.2 gives
the reason the RFC does not remove it.

Milestone 7 also closes a gap that exists today. A partition learns the topology of its descendants
from `meta` on a successful response. An error carries no `meta`, so a throw loses that knowledge.
Each error then carries the same routing meta, and each forwarding level reads it before it rethrows.

```
 caller Worker                PartitionDO (router)         PartitionDO (leaf)
      |                               |                            |
      |-- putItem ------------------->|                            |
      |                               |-- forward ---------------->|
      |                               |                            | raises
      |                               |                      FokosUnavailableError
      |                               |                      + stamp(meta)   <- #rpc stamps
      |                               |<---------------------------|
      |                               | learnFromErrorMeta(meta)   <- best effort
      |                               | rethrow unchanged          <- error_id preserved
      |<------------------------------|                            |
      | db.ts materializes the class  |                            |
```

### 4.2 Technical details

#### 4.2.1 The base class and the category classes

Two modules hold the errors:

- `packages/fokosdb/src/shared/errors.ts` holds the machinery, the seven generic category classes,
  and one code table for each of them (section 4.2.2). It imports nothing, so another package can reuse
  it.
- `packages/fokosdb/src/shared/errors-operations.ts` holds what depends on the types of the library:
  the two categories whose fields hold types of the data model, `FokosConditionCheckError` (`reason`,
  `meta`) and `FokosTransactionCancelledError` (`reason`, `results`), with their code tables;
  `withExpressionErrors` (section 4.2.7); the union `FokosAnyError` of every error the library raises;
  and its guard `isFokosAnyError` (section 4.2.4). It extends `errors.ts` in the same way that another
  package does.

`FokosError` extends `Error` and declares the contractual fields. Each category extends `FokosError`
and declares its tag once, as a static. The constructor takes a code definition (section 4.2.2) and
assigns every field as an own property, so every field crosses a hop.

```ts
/** `T` is the category and `C` is the union of the codes the error can carry. */
export abstract class FokosError<T extends string = string, C extends string = string> extends Error {
    readonly _tag: T;
    readonly type: string;
    readonly code: C;
    readonly error_id: string;
    readonly origin: "caller" | "service" | "internal";
    readonly httpStatusHint: number;
    readonly attributes: Record<string, unknown>;

    constructor(code: FokosCodeDef<T, C>, options: { message: string; attributes?; cause?; origin?; httpStatusHint?; error_id? });

    /**
     * True after any number of hops for an error of any category, including one that another package
     * defines. On a category class it holds for that category only. It reads own properties only.
     * See section 4.2.4.
     */
    static is<K>(this: K, e: unknown): e is InstanceType<K>;

    /**
     * Returns `e` unchanged when it is a FokosError, with or without its prototype. Wraps any other
     * value as `foreign_error` and keeps it as `cause`. See section 4.2.10.
     */
    static wrap(e: unknown): FokosError;

    /**
     * The plain record for storage. It accepts a class instance, an error that crossed a hop, or a
     * foreign value, which it wraps first. See section 4.2.6.
     */
    static toWire(e: unknown): FokosErrorWire;

    /**
     * Builds the class in the calling isolate from a wire record or from an error that crossed a hop.
     * A category that the module does not define keeps its tag and its fields.
     */
    static fromWire(w: FokosErrorWire | FokosError): FokosError;
}

/** Each category class. `C` is open, so another package can raise its own codes in the category. */
export class FokosConflictError<C extends string = string> extends FokosError<"FokosConflictError", C> {
    static readonly tag = "FokosConflictError";
}

type FokosErrorWire = {
    name: string;
    message: string;
    code: string;
    error_id: string;
    origin: "caller" | "service" | "internal";
    httpStatusHint: number;
    attributes: Record<string, unknown>;
    cause?: { error: string; errorProps: Record<string, unknown> };
};
```

The classes hold data only. They declare no instance method and no accessor. Every helper is a static
function that takes an error or a plain record and reads its own properties. So a helper gives the
same result for a class instance, for an error that crossed a hop, and for a wire record. An error
that crossed a hop has no prototype, and a helper that is a prototype member would be absent on it.

`FokosError.toWire` stores the cause as `{ error: String(cause), errorProps: { ...cause } }`. Both
parts are plain data, so the record survives JSON storage and any RPC hop. An `Error` object as a
cause would store as `{}`.

The static `tag` of a category is what lets the one static `is` serve every category:
`FokosConflictError.is(e)` compares `_tag` with `FokosConflictError.tag`.

A category can declare own data fields beside the base ones, with a constructor that assigns them.
`FokosConditionCheckError` and `FokosTransactionCancelledError` do so. `FOKOS_ERROR_CATEGORIES` in
`errors.ts` does not list them, so `FokosError.fromWire` builds an error of either one as the generic
class that keeps its tag and all its fields.

| Category (`name` and `_tag`) | `type` | Typical origin | Typical `httpStatusHint` |
| --- | --- | --- | --- |
| `FokosValidationError` | `validation_error` | caller | 400 |
| `FokosExpressionError` | `expression_error` | caller | 400 |
| `FokosConditionCheckError` | `condition_check_error` | caller | 409 |
| `FokosConflictError` | `conflict_error` | caller | 409 |
| `FokosTransactionCancelledError` | `transaction_cancelled_error` | caller | 409 |
| `FokosUnavailableError` | `unavailable_error` | service | 503 |
| `FokosTransactionPendingError` | `transaction_pending_error` | service | 503 |
| `FokosRoutingError` | `routing_error` | internal | 500 |
| `FokosInternalError` | `internal_error` | internal | 500 |

The code definition declares the default origin and `httpStatusHint` of each code. The instance
fields are the source of truth: a constructor takes the defaults of the definition unless the call
site passes others. Two call sites pass others. `wrap` does so for a foreign error that carries the
runtime `retryable` marker (section 4.2.10). `FokosTransactionCancelledError` takes the fields of its
reason (section 4.2.6). A consumer must read the fields on the error, not the code tables. The table
above gives the common value for each category.

`origin` says where the fault sits, and it has three values. Each one names a party, so the three stay
on one dimension:

| `origin` | Meaning | Who acts | Typical hint |
| --- | --- | --- | --- |
| `caller` | The request is wrong, or it conflicts. | Change the request, or retry. | 400, 409 |
| `service` | FokosDB cannot do the work now. The condition clears on its own. | Retry. | 503 |
| `internal` | FokosDB holds a defect, or its state is corrupt. It does not clear. | Report it. | 500 |

A partition over its size cap, a partition that migrates after a split, and an undecided transaction
all carry `service`. None of them is a defect, and a split or the coordinator alarm clears each one.
They must not carry `internal`, because an `internal` rate must stay at zero and must raise an alarm
when it does not.

The code says how the library detected the fault, so `origin` must not repeat it. The code
`invariant_failed` already names a broken assertion, and `origin` stays `internal` for it.

#### 4.2.2 The code definitions and `error_id`

A code is a value. A code definition carries the code, its category, its default origin, its default
`httpStatusHint`, and its 6-character segment. `defineCodes(tag, origin, httpStatusHint, segments)`
makes the definitions of one category, and a call site passes one of them to the constructor as its
first argument:

```ts
export const VALIDATION_CODES = defineCodes("FokosValidationError", "caller", 400, { hash_key_empty: "2fzzq9", ... });
throw new FokosValidationError(VALIDATION_CODES.hash_key_empty, { message: "hashKey must not be empty" });
```

The constructor refuses a definition of another category at compile time, and it takes the code as a
literal type. No central table limits which codes exist, so another package defines its own codes
in the same way (section 4.2.4). Section 7.1 holds the starting codes.

The `error_id` has the form `e_<segment>_<suffix>`.

- `<segment>` is 6 characters, fixed for the life of the code. An author assigns it by hand. The only
  rule is that it must be unique across every code. A test asserts that every segment and every code
  of the code tables is unique.
- `<suffix>` is `crypto.randomUUID().replaceAll("-", "")`, which gives 32 hexadecimal characters. The
  source already uses this form for `transactionId`.

The alphabet of a segment is `a-hjkmnp-z2-9`. It drops `i`, `l`, `o`, `0`, and `1`, so a reader
cannot confuse two characters in a log or a support ticket. This command emits a segment that the
repository does not use:

```bash
used=$(grep -rhoE 'e_[a-hjkmnp-z2-9]{6}_' packages/ 2>/dev/null | sed -E 's/e_(.{6})_/\1/' | sort -u)
c=$(LC_ALL=C tr -dc 'a-hjkmnp-z2-9' < /dev/urandom | head -c6)
grep -qxF "$c" <<<"$used" || echo "$c"
```

**The minting rule.** The node that first detects the failure mints the `error_id`. Every later hop
and every wrapper keeps it. So one `error_id` identifies one event, not one layer. A wrapper that
needs its own identity sets `cause` and keeps the inner `error_id` reachable.

#### 4.2.3 The message and the contract

The category and the code are contractual. A consumer can depend on both. The message is not
contractual, and it can change in any release.

The message starts with the code, then a fixed phrase. It holds no dynamic detail. The attributes
hold the dynamic detail:

```
fokos/<code>: <fixed phrase>
```

`name` holds the category and nothing else, so a check on `name` stays exact. The code appears in the
message for a human who reads a log line.

#### 4.2.4 Classification

A hop drops the prototype, so a guard must read own properties only.
`packages/fokosdb/test/tagged-error-rpc.test.ts` measures this over a real Durable Object RPC call:
`name`, `_tag`, `cause` and every payload field arrive, and every prototype member is gone.

One union type carries the classification of the library. It lists each category with the codes that
the library raises in it:

```ts
/** Every error the library raises. `_tag` and `code` are literals, so a switch narrows and stays exhaustive. */
export type FokosAnyError =
    | FokosValidationError<FokosCodesOf<typeof VALIDATION_CODES>>
    | FokosConditionCheckError<FokosCodesOf<typeof CONDITION_CHECK_CODES>>
    | /* ... */ FokosInternalError<FokosCodesOf<typeof INTERNAL_CODES>>;
export const isFokosAnyError = defineErrorGuard<FokosAnyError>(...FOKOS_LIBRARY_CODE_TABLES);
```

`FokosError` and `FokosAnyError` are not the same thing, and both are needed. `FokosError` is the base
class that a category extends. `FokosAnyError` is the union that a caller narrows, because `_tag` and
`code` on the base are `string` and on the union they are literals.

The library gives four ways to classify an error, and none of them uses `instanceof`:

```ts
// 1. Any FokosError, of any package, after any number of hops. `_tag` and `code` stay `string`.
FokosError.is(e)

// 2. One category. The static tag makes the one static `is` enough for all nine.
FokosConflictError.is(e)

// 3. Every error of this library. A switch on `_tag` then narrows `code` to the codes of the category.
isFokosAnyError(e)

// 4. One code. It narrows `code` to the literal.
FokosError.isCode(e, CONFLICT_CODES.item_locked_by_transaction)
FokosError.isCode(e, "item_locked_by_transaction")
```

`FokosError.isCode` takes a code definition or a plain string. With a definition from a code table, a
misspelt code does not compile and the check also compares the category. A plain string serves a code
that arrives as data: the compiler cannot check its spelling, and the check compares the code only,
which is enough because codes are unique across every package. The library uses the definition form.

| Guard | The question it answers | Typical use |
| --- | --- | --- |
| `FokosError.is(e)` | Is it any FokosError, of any package? | Generic handling: `httpStatusHint` to an HTTP status, `error_id` to a log, `origin` to an alarm |
| `FokosConflictError.is(e)` | Is it this category? | A coarse decision, for example to retry any conflict |
| `isFokosAnyError(e)` and a switch on `_tag` | Which error of this library is it? | Exhaustive handling, with `code` narrowed for each category |
| `FokosError.isCode(e, CODES.x)` | Is it exactly this one failure? | Control flow on one condition: the fast-path fallback, the over-size retry skip, the phantom bounce |

`defineErrorGuard` makes the guard of a union from its code tables. The guard holds only for a code of
those tables with the category of its definition. So the union type it narrows to is true even when an
error of another package carries a category of this library.

A guard narrows to the class type. The classes hold data only, so the class type describes an error
that crossed a hop exactly: the compiler cannot offer a member that the hop removed.

A caller that runs in the same isolate as `db.ts` can also use `instanceof`, because `db.ts` builds
the class in that isolate. The library must not document `instanceof` as a way to classify. A consumer
that puts its own Worker in front of FokosDB and rethrows an error gives its caller an object with no
prototype, and `instanceof` fails there.

Often no guard is needed. `_tag` is a literal discriminant, so a switch narrows on its own:

```ts
if (isFokosAnyError(e)) {
    switch (e._tag) {
        case "FokosConflictError": return retry();
        case "FokosValidationError": return badRequest(e.code);
    }
}
```

**Extension by another package.** A package that reuses `errors.ts`, for example a future sharding
package, extends the system in three ways, and no module registers anything at runtime:

1. It defines its own codes with `defineCodes`, in a category of `errors.ts` or in its own category.
2. It defines its own category as a subclass of `FokosError` with a static `tag`.
3. It declares the union of every error it raises, the unions of the packages it depends on included,
   and makes its guard with `defineErrorGuard`. Each entry point exports its union and its guard.

```ts
export const SHARD_CODES = defineCodes("FokosUnavailableError", "service", 503, { shard_migrating: "k3m9xz" });
export type ShardAnyError = FokosAnyError | FokosUnavailableError<"shard_migrating"> | FokosShardError<"shard_moved">;
export const isShardAnyError = defineErrorGuard<ShardAnyError>(...FOKOS_CODE_TABLES, SHARD_CODES, SHARD_OWN_CODES);
```

`isFokosAnyError` does not hold for an error of the sharding package, so a caller that knows only this
library does not misread it. That caller still reads `origin` and `httpStatusHint` through
`FokosError.is`. Category names and codes must be unique across packages. One test in the repository
asserts it over every code table.

#### 4.2.5 The transaction exception to the rule

A participant answers about N operations at once. It reports `passed`, `not_evaluated`, or `rejected`
for each one. A throw carries one answer, so it cannot report the other N-1 operations.

So `PrepareResponse` and `SingleShotResponse` keep their returned union. `db.ts` converts a
`cancelled` outcome to a thrown `FokosTransactionCancelledError`, which carries the `results` array as
an own data property. This is the only deviation from "everything except a happy-path answer throws".

The model follows DynamoDB's `TransactionCanceledException`: one exception, and one reason for each
operation, in request order. There is no reason for the whole transaction. A failure that stops a
partition from running its operations is reported on each operation of that partition, as DynamoDB
reports `ThrottlingError` on each affected item.

A returned union also removes a dependency. The results array crosses a hop as a return value, which
structured clone has always carried. It does not depend on `enhanced_error_serialization`.

A `results` entry is a verdict, not an exception: the operation did not fail in isolation, the
transaction cancelled, and the entry records why. It keeps the plain shape it has today — a rejected
entry carries its `RejectionReason` record:

```ts
type TransactWriteOperationResult =
    | { outcome: "passed" }        // DynamoDB "None": the premise of this operation held
    | { outcome: "not_evaluated" } // no participant judged it
    | {
          outcome: "rejected";
          reason: RejectionReason;
          itemOmitted?: "response_too_large";
      };

type RejectionReason =
    // A premise of the operation did not hold.
    | { code: "condition_failed"; hashKey; sortKey?; item? }
    | { code: "clock_skew"; hashKey; sortKey?; serverTimestampMs; transactionTimestampMs }
    | { code: "timestamp_conflict" | "pending_conflict" | "update_not_applicable" | /* ... */; hashKey; sortKey? }
    // The partition that owns the operation could not run it. Every operation of that partition
    // carries the same code and the same error_id, which names the error in the logs.
    | { code: ExecutionFailureCode; hashKey; sortKey?; error_id: string };
```

Every `code` is a literal, so a check on `reason.code` narrows the record. `ExecutionFailureCode` is
every code of the library except the seven premise codes.

This is the shape DynamoDB uses for `TransactionCanceledException.CancellationReasons`: plain
`{ Code, Message, Item }` structs, not nested exceptions. `RejectionReason` keeps its shape, with its
discriminant renamed from `type` to `code`: the `reason.code` values are the codes of section 7.1 —
`condition_failed`, `item_too_large`, and the rest keep one spelling on an item RPC and inside a
cancelled transaction. Plain data crosses any number of hops unchanged — a consumer's Worker can
rethrow the cancelled error and `results` arrives intact. A nested `Error` instance would lose its
prototype at the first hop and could not serialize again on the next, which is why an entry must
never hold an `Error` object.

`FokosError.toWire` and `FokosError.fromWire` do not touch `results`. They serve the single-error
path: `toWire` serializes an error the coordinator must store (section 4.2.6), and `fromWire`
materializes a thrown error that crossed a hop, because a reconstructed error carries the same
fields. `db.ts` builds the thrown `FokosConditionCheckError` for `putItem`/`deleteItem` from a
`reason` record — one vocabulary, materialized only where a throw actually happens. A router
partition merges the arrays of its children and converts nothing, and the coordinator stores the
array in `tc_state.results_json` and replays it without conversion.

**The errors of a transaction.** A transaction raises the same exception, category, and code for one
failure on the single-partition fast path and on the two-phase path. Placement is invisible to the
caller, and a split changes it, so the error must not depend on it. The two tables below list every
failure. A row marked "no" for one path is a failure that path cannot have.

`transactWriteItems`:

| Failure | Fast path | Two-phase path | Exception | `code` |
| --- | --- | --- | --- | --- |
| The input is not valid: keys, data, `ttlAt`, token, item count, duplicate keys, payload size, operation fields | yes | yes | `FokosValidationError` | The code from section 7.1, for example `transact_duplicate_key` |
| An expression does not compile | yes | yes | `FokosExpressionError` | `expression_invalid` |
| The premise of an operation does not hold | yes | yes | `FokosTransactionCancelledError` | `transaction_cancelled`. Each rejected `results` entry holds its `RejectionReason`: `condition_failed`, `timestamp_conflict`, `pending_conflict`, `update_not_applicable`, `update_value_is_bytes`, or `item_too_large` |
| The transaction timestamp is too far ahead of the partition clock | no. The partition takes its own timestamp | yes | `FokosTransactionCancelledError` | `transaction_cancelled`. Each operation of that partition is rejected with `reason.code` `clock_skew` |
| A partition refuses the work before it applies anything: migration, over-size, mis-route, a range DO that is not initialized, a context mismatch, an expression fault at evaluation, a failed invariant | yes | yes | `FokosTransactionCancelledError` | `transaction_cancelled`. Each operation of that partition is rejected with the code of the raised error, for example `partition_migrating`, and its `error_id`. The operations of the other partitions keep their own answers |
| The outcome is unknown: a transport or runtime fault on a hop where the reply can be lost after the apply | any hop | the hop from `db.ts` to the coordinator | `FokosInternalError` | `foreign_error` |
| The decision is not final yet | no | yes | `FokosTransactionPendingError` | `transaction_undecided` or `transaction_commit_pending` |
| The token was used for another set of operations | no. The fast path takes no token | yes | `FokosValidationError` | `idempotent_parameter_mismatch` |
| The coordinator is over its storage cap | no | yes | `FokosUnavailableError` | `coordinator_over_size` |
| The items span more than one partition | yes | no | Not raised. `db.ts` runs the two-phase path | `single_partition_fast_path_not_applicable`, internal only |

On the fast path, `db.ts` converts a thrown error into `FokosTransactionCancelledError` when
`FokosError.is(err)` holds and its code is neither `foreign_error` nor
`single_partition_fast_path_not_applicable`. One partition owns every operation of this path, so
every operation is rejected with the code and the `error_id` of that error. Two rules make this safe:

1. `txExecuteSingleShot` does not throw after its apply commits. Today it calls `checkSplitsNoKey`
   after the apply, bare. It now logs a failed split check and returns `committed`, as `txCommit`
   does. So every error that partition code raises on this path means that nothing applied.
2. A lost reply always arrives as `foreign_error`. It is a raw error on the hop from `db.ts`, and a
   router's `#rpc` wraps it as `foreign_error` on a deeper hop.

A foreign error that SQLite raises inside the apply also arrives as `foreign_error`. The apply rolled
back, but `db.ts` cannot tell that error from a lost reply, so it stays an unknown outcome. On the
two-phase path the coordinator cancels the transaction, so the same fault is a cancellation there.
This is the one difference between the paths. It follows from what the library can know, not from
where the data is.

`transactGetItems`:

| Failure | Fast path | Two-phase path | Exception | `code` |
| --- | --- | --- | --- | --- |
| The input is not valid | yes | yes | `FokosValidationError` | The code from section 7.1 |
| An item has a pending write | yes | yes | `FokosConflictError` | `pending_write` |
| A write changed an item between the two phases | no. One partition reads a snapshot | yes | `FokosConflictError` | `read_conflict` |
| A partition read fails after the retries of the path | yes | yes | The error as the partition raised it | For example `partition_migrating`, `partition_misrouted`, or `foreign_error` |
| A participant drops a requested key | no | yes | `FokosInternalError` | `invariant_failed` |
| The items span more than one partition | yes | no | Not raised. `db.ts` runs the two-phase path | `single_partition_fast_path_not_applicable`, internal only |

A read applies nothing, so a failed read never has an unknown outcome. `db.ts` rethrows the error of a
failed phase call in place of the `transient_error` collapse in `#readTransaction`. The
`transient_error` reason value goes away on the read path and on the write path.

#### 4.2.6 A failed prepare keeps its cause

`cancelTransactionInStore` in `do-transaction-coordinator.ts` reports `{ type: "transient_error" }`
for every participant whose prepare threw after its retries. A transport failure, a migrating
partition, an `ExpressionError`, a mis-routed item, and an over-size partition become one value.

The coordinator now stores the cause in storage, not in memory, because both writers of `CANCELLING`
must merge the same stored answers:

1. When a prepare throws after its last attempt, the coordinator writes `FokosError.toWire(err)` to
   a new column, `tc_participants.error_json`. `prepare_outcome` stays NULL, so recovery can still
   re-prepare the participant.
2. The merge in `cancelTransactionInStore` reads `error_json` only for a participant whose
   `prepare_outcome` is NULL. A later answer therefore replaces the stored error.
3. A NULL participant with no `error_json` exists only when the coordinator stopped between the throw
   and the write. It reports `prepare_unanswered`.
4. Each operation of a participant that did not answer is rejected with the code and the `error_id`
   of that error: a transport fault reports `foreign_error`, an over-size partition reports
   `partition_over_size`. A rejected participant whose stored answer cannot be read back reports
   `unexpected_transaction_state` on its operations. Every other operation keeps the answer of its own
   participant, the images of a failed condition included.

The error record stays inside the coordinator. The cancelled answer holds only the `results` array,
and a `CANCELLING` or `CANCELLED` row without stored results raises `unexpected_transaction_state`.

`db.ts` throws `FokosTransactionCancelledError` for every cancelled outcome. Its `origin` and
`httpStatusHint` come from the codes of the rejected entries. The first origin in the order `caller`,
`internal`, `service` wins, with the hint of the first entry of that origin in request order: a
premise that must change comes before a defect, and only a cancel whose every failure clears on its
own is a service condition. The definition of `transaction_cancelled` is the fallback when no entry
is rejected.

#### 4.2.7 `ExpressionError`

`ExpressionError` stays as it is, in `packages/fokosdb/src/shared/expression/errors.ts`. The
expression sub-library does not change.

Each caller of `compileConditionExpression`, `compileUpdateExpression`, `evaluateConditionPlan`, and
`probeUpdatePlan` wraps an `ExpressionError` in a `FokosExpressionError`. The wrapper sets `cause` to
the original error and copies its `ExpressionErrorCode` into an attribute.

`client/index.ts` keeps its `ExpressionError` export for typing the `cause`, but a consumer that
catches `ExpressionError` today does not catch the wrapper: `e instanceof ExpressionError` is false
on it. The match moves to `e.code === "expression_invalid"` or `FokosExpressionError.is`. This is a stated
break — the same one `transactWriteItems` takes when `cancelled` becomes a throw, and the one
`ConditionCheckFailedError` takes when `FokosConditionCheckError` replaces it.

#### 4.2.8 Topology learning on the error path

This is milestone 7. It starts after milestones 1 to 6, when every error flows correctly.

A partition learns the boundaries of its descendants from `meta` on a response. `recordForwardResult`
runs after a successful forward in `withSplitForwarding` and in `forwardToRangeRootPartition`. An
error carries no `meta` today, so a throw loses that knowledge. On a range hop the stamped meta
carries the serving leaf's `rangeAncestors` — the boundaries the ancestor needs.

Learning runs only where the success path learns today: `withSplitForwarding` and
`forwardToRangeRootPartition`. The transaction RPCs and the single-shot fallback route through
`groupItemsByRouting` and `routeSingleDestination`. Those paths carry no meta on success, so they
learn nothing on the error path either. `db.ts` does not learn.

Four rules fix it.

1. **The stamp point.** `#rpc` in `do-partition.ts` wraps every RPC method, so it is the one place
   that stamps. It catches, attaches the routing meta of the partition as an own data property named
   `meta`, of type `PartitionInfoInternal`, and rethrows. It stamps only when `#_partitionContext` is
   set, so an error raised before the partition resolves its context does not carry a meaningless
   context. The stamp is best effort: it must never throw in place of the error it annotates, and it
   stamps the wrapped error — `wrap` first, stamp second — so the meta lands on the `FokosError` a
   caller reads, not on the `cause`. Only `PartitionDO` stamps; the coordinator needs no routing meta.
2. **The same meta changes as the success path.** `#rpc` attaches a meta only when the error carries
   none. Each forwarding level then changes the stamped meta exactly as it changes a success meta:
   `forwardCount + 1` at the same two places, and the `hashDepth` rewrite that
   `forwardToRangeRootPartition` applies at a hash-to-range hop. One helper applies the change to a
   result meta and to an error meta, so the two paths cannot drift. A test asserts that the error meta
   equals the success meta for the same route.
3. **Each level reads, then rethrows unchanged.** A forwarding level calls `learnFromErrorMeta`
   before it rethrows. The error keeps its `error_id` and its attributes.
4. **Learning is best effort.** `recordForwardResult` holds its own invariants. It asserts that
   `toCtx` is a descendant of `fromCtx`, and that `responseHashDepth` is at least `toAbsDepth`. An
   error with an absent or partial meta trips one of those inside a catch block, and the invariant
   then replaces the original error. So `learnFromErrorMeta` checks that the meta is present and
   well-formed, and it swallows anything that it raises. A lost cache update costs one extra hop
   later. A masked error costs a debugging session.

```ts
try {
    const result = await forward(stub, partitionContext);
    topology.recordForwardResult(hashKey, ctx, partitionContext, result.meta);
    return result;
} catch (e) {
    learnFromErrorMeta(topology, hashKey, ctx, partitionContext, e);
    throw e;
}
```

`db.ts` strips `_internal` from a stamped meta before the error leaves, the way `publicMeta` strips it
from a response — the boundaries feed the routers, and the actor names stay inside.
`FokosError.toWire` does not copy the routing meta, so a stored `reason` never carries it.

#### 4.2.9 Retry is the caller's decision

An error carries no field that says whether to retry. The caller decides from the category and the
code. Retry safety belongs to the operation, not to the error: a `transactWriteItems` without a
`clientRequestToken` can have applied its first attempt. The token decides what a retry does:

| Code | What a retry with the same `clientRequestToken` does |
| --- | --- |
| `transaction_undecided`, `transaction_commit_pending` | It returns the final outcome once the coordinator reaches it. Retry with the same token. |
| `transaction_cancelled` | It returns the stored cancellation for `IDEMPOTENCY_WINDOW_MS` (10 minutes). A cancelled transaction applied nothing, so a new attempt with a new token is safe. |
| `foreign_error` from `transactWriteItems` | The outcome is unknown. With a token, the same token returns the stored outcome. Without a token, a retry can apply the writes twice. |

The library keeps the internal retry predicates it has today, with two changes. A code check replaces
the substring predicate, so prepare still does not retry `partition_over_size`. And
`#readSnapshotFastPath` uses `isRuntimeRetryableError` in place of `isErrorRetryable` from
`durable-utils`, because `wrap` moves the runtime markers into `attributes` (section 4.2.10).

#### 4.2.10 Foreign errors and the string match

`FokosError.wrap(e: unknown)` — the static member in the sketch of section 4.2.1 — returns `e`
unchanged when `FokosError.is(e)` holds, and wraps anything else. It sets `cause` to `e` and it gives
the error the code `foreign_error` with the origin `internal`. When `e` is an object, it also copies
every own data property of `e` — except `name`, `message`, `stack`, and `cause` — into `attributes`,
so foreign fields stay reachable: the runtime's `retryable` and `overloaded` markers arrive as
`attributes.retryable` and `attributes.overloaded`. Each RPC entry point and each public method wraps
its body, so every error that leaves `db.ts` is a `FokosError`.

The runtime marks a fault it considers transient with a `retryable` own property, and `wrap` honours
it: when `e` carries `retryable: true`, the wrapped error gets `origin: "service"` and
`httpStatusHint: 503` in place of the defaults of `foreign_error`. A platform fault that
clears on its own is a service condition, not a defect, and the `internal` alarm stays silent for it.
A foreign error without the marker keeps `foreign_error`/`internal`. The `retryable`, `overloaded`,
and `remote` markers get no top-level fields — they stay inside `attributes`. The helper
`isRuntimeRetryableError(e)` returns `retryable && !overloaded`, as `isErrorRetryable` does. It reads
the markers on a raw error and in `attributes` on a wrapped one.

A fault that the library did not cause stays outside the contract. A syntax error in consumer code is
an example. `wrap` reports such a fault as `foreign_error`, and `FokosInternalError` must not
hide a defect in this library behind that code.

One string match stays. `isDestroyAbortError` in `packages/fokosdb/src/shared/cf-utils.ts` matches
`DESTROY_ABORT_SENTINEL`. `ctx.abort(message)` makes the runtime construct the error, and the library
only supplies the string, so the error can never carry a property. The match inspects the error and
its `cause` chain, so a wrapped abort error still matches when the top level does not. It is the one
remaining string match in the library.

#### 4.2.11 The compatibility-date requirement

The library needs own properties to cross a hop. `enhanced_error_serialization` is on by default from
`2026-04-21`, and Cloudflare needs it on both the caller and the callee of a hop. Both Durable Object
classes run in the script of the consumer, so one `compatibility_date` governs every hop between
them. The hop from `db.ts` can start in a different Worker, which binds the classes from another
script. So the requirement covers every Worker that runs `FokosDB` and every Worker that exports the
Durable Object classes.

On an older date an error arrives without its own properties — no `code`, no `_tag` — so
`FokosError.is` does not hold and `wrap` reports it as `foreign_error`. The requirement is documented
for the consumer, and `tagged-error-rpc.test.ts` pins the behaviour the library needs. There is no
runtime probe: a production path must not spend a request on a self-test.

#### 4.2.12 The `withStacktrace` helper

RPC drops the stack of an error. `withStacktrace(err)` copies the stack into an own property, so it
crosses the hop. A call site opts in, because a stack can carry internal detail that an end user must
not see.

#### 4.2.13 Happy-path negatives stay values

These answers are not failures, and they must stay values:

- `getItem` returns `found: false`.
- `deleteItem` returns `deleted: false`.
- `queryItems` returns an empty page.
- `transactGetItems` and `transactWriteItems` return a committed outcome.

#### 4.2.14 `destroy()`

`destroy()` stops at the first failure. It keeps the behaviour it has today, and it raises a
`FokosError` in place of the raw error. A partial destroy stays partial, and the caller can call
`destroy()` again.

#### 4.2.15 Testing

1. A test asserts that every 6-character segment and every code in the code tables is unique.
2. A test asserts that every category, code, origin, and `httpStatusHint` crosses an RPC hop as an
   own property.
   A test asserts that `FokosError.toWire` and `FokosError.fromWire` round-trip every category without
   loss, from a class instance and from an error that crossed a hop.
   A test asserts that no category class declares a prototype member.
3. `packages/fokosdb/test/tagged-error-rpc.test.ts` pins what an error keeps across a hop, and which
   guards work there. It must fail if a runtime change stops an own property from crossing.
4. A test runs each row of the two transaction tables in section 4.2.5 on the fast path and on the
   two-phase path, and asserts the same exception, category, and code.
5. In milestone 7, a test asserts that the error meta equals the success meta for the same route at
   each forwarding level, and that a malformed meta does not replace the original error.
6. The 185 `toThrow` assertions in the suite move from a message match to a code match. The message
   is not contractual, so a test must not pin it.

#### 4.2.16 The example worker

`api.onError` in `examples/http-api/index.ts` reads `httpStatusHint`, and it returns the category, the
code, and the `error_id` in the body. A failed condition stops returning 500. The handler keeps its
`HTTPException` branch.

The `transactWriteItems` route changes with the throw: it no longer returns an `outcome:
"cancelled"` body. The error reaches `api.onError`, which answers 409 with the code and the
`error_id`, and can add the `results` entries to the body — serialized through the same `encodeData`
step a committed response takes, so a binary payload needs no new handling. `public/demo/index.html`
reads the error body in place of `body.outcome` and `body.reason`.

### 4.3 Open Questions

None. The retry policy is out of scope (section 2.2).

---

## 5. Alternative Options

### 5.1 A `Result<T, E>` type returned everywhere inside the library

Rejected. Four reasons:

1. `ctx.storage.transactionSync` rolls back when its callback throws. A store method that returns a
   failure inside one of the 20 blocks returns normally, so the transaction commits a partial write.
   Correctness then depends on every call site checking the result and rethrowing, and a missed check
   turns a loud fault into silent corruption.
2. `tryWhile` retries a function that throws. A function that returns a failure never retries, so all
   11 call sites need a second retry mechanism.
3. SQLite, the RPC transport, an overloaded Durable Object, and `ctx.abort` throw. A `Result` type
   inside the library gives two error systems that must interconvert at each layer.
4. TypeScript has no `?` operator. The recursive forwarding chains rely on an error that travels up
   through each router untouched, and a `Result` needs a propagation branch at every level.

### 5.2 A thrown error that carries the per-operation transaction results

Rejected for now. A participant throws one error that carries the `results` array as a payload field.
A router partition then merges the plain arrays of its children and throws one new error, so the cost
is one allocation for each hop, which matches the cost of a returned union.

The RFC keeps the returned union because the results array then crosses a hop as a return value.
Structured clone has always carried a return value, so the transaction results do not depend on
`enhanced_error_serialization`. The compatibility-date requirement in section 2.3 stays, because the
single-answer errors need own properties for their code.

### 5.3 A returned variant for the single-partition fast-path fallback

Rejected. The fallback is a signal that the items of a transaction straddle more than one partition.
A returned variant removes the substring match, and a code on a thrown error removes it as well. A
thrown error also keeps the fallback visible.

### 5.4 `TaggedError` from `better-result` as the base class

Rejected. The library writes its own base class.

`TaggedError` gives four things this design uses: `_tag` and `name` as own properties, the payload
assigned as own properties, the prototype fix-up for a subclass, and the `cause` handling. It gives
four more that this design must not use:

| Feature | Why it is unusable here |
| --- | --- |
| `static is()` | It is `e instanceof this`, and a hop drops the prototype. |
| `isTaggedError()` | It needs a callable `toJSON`, which is a prototype member. |
| `match()` | A prototype member. It is gone after a hop. |
| `[Symbol.iterator]` | It serves `Result.gen()`, and section 2.2 puts `Result` out of scope. |

`TaggedError` also holds no static tag, so a generic guard has nothing on the class to compare
against. The library would add a factory to stamp one.

The cost decides it. `match()` and the iterator keep `Panic`, `Err` and the generator machinery
reachable, so a bundler cannot drop them:

| Base | Minified | Minified and gzipped |
| --- | --- | --- |
| `TaggedError`, two categories | 4359 B | 1396 B |
| A written base class, two categories | 563 B | 315 B |

The client bundle is 64.9 kB against the 72 kB budget that `check-client-bundle` holds, so 3.8 kB
takes more than half of the headroom that is left. A written base class costs about 20 lines, and it
owns `is`, `wrap`, `toWire`, and `fromWire`, which this design needs anyway.

A later `Result` RFC does not change the answer. `Result.err(value)` accepts any error type, so
interoperability needs no tagged-error library. Only `yield*` composition inside `Result.gen()` needs
the iterator, and the base class can declare one.

### 5.5 One error class for each code

Rejected. The client bundle has a size budget, and `check-client-bundle` fails the build when the
bundle grows past it. Between 6 and 10 category classes give the `name` that crosses a hop, and the
code carries the fine detail as a string.

---

## 6. Frequently Asked Questions

**Why does the library throw when a returned union crosses an RPC hop with more fidelity?**
`ctx.storage.transactionSync` rolls back on a throw and commits on a return. The rollback of 20
storage transactions depends on the throw. Section 5.1 gives the other three reasons.

**Why does a failed condition throw, when it is an answer and not a fault?**
A caller asked whether its premise holds, and the premise did not hold. DynamoDB raises
`TransactionCanceledException` for the same case, and `putItem` and `deleteItem` already raise
`ConditionCheckFailedError`. One mechanism for one fact is the goal of this RFC.

**What does a caller see when an error crosses its own middle-layer Worker?**
The `name`, the `message`, and every own property, which covers the category, the code, the
`error_id`, the origin, the `httpStatusHint`, and the attributes. The prototype does not cross, so
`instanceof` fails there. `FokosError.is` and a check on `code` both work.

**Why does the library keep one string match?**
`ctx.abort` makes the runtime build the error, so the library cannot attach a property to it.

**Why does the stamp happen in `#rpc` and not at each throw site?**
`#rpc` wraps every RPC method of `PartitionDO`, so one change covers every error. A stamp at each
throw site needs an edit at more than 30 places, and a new throw site can forget it.

**Does an error replace the `meta` that a successful response carries?**
No. The success path keeps `meta` on the response. An error carries the same routing meta as an own
property, so both paths feed `recordForwardResult`.

---

## 7. Appendix

### 7.1 The starting codes

The origin is `c` for caller, `s` for service, and `i` for internal. The origin and the hint are the
defaults that section 4.2.1 describes.

| Code | Category | Segment | Origin | Hint |
| --- | --- | --- | --- | --- |
| `hash_key_empty` | Validation | `2fzzq9` | c | 400 |
| `sort_key_empty` | Validation | `2gjvju` | c | 400 |
| `key_contains_nul` | Validation | `42r8z7` | c | 400 |
| `key_not_well_formed_utf16` | Validation | `4767pp` | c | 400 |
| `hash_key_too_large` | Validation | `4v2p4p` | c | 400 |
| `sort_key_too_large` | Validation | `58daxm` | c | 400 |
| `key_encode_empty` | Validation | `58sjts` | c | 400 |
| `item_data_too_large` | Validation | `6z7eb3` | c | 400 |
| `item_data_wrong_type` | Validation | `7vxpb8` | c | 400 |
| `item_data_not_json_serializable` | Validation | `bfvvtt` | c | 400 |
| `ttl_at_invalid` | Validation | `brcy77` | c | 400 |
| `return_values_option_invalid` | Validation | `ed9wyr` | c | 400 |
| `client_request_token_invalid` | Validation | `f9azze` | c | 400 |
| `idempotent_parameter_mismatch` | Validation | `fn733z` | c | 400 |
| `transact_items_empty` | Validation | `gmjfgw` | c | 400 |
| `transact_items_too_many` | Validation | `h58dgv` | c | 400 |
| `transact_duplicate_key` | Validation | `hgxg2r` | c | 400 |
| `transact_payload_too_large` | Validation | `hsvepa` | c | 400 |
| `transact_operation_fields_invalid` | Validation | `jr49a5` | c | 400 |
| `query_queries_empty` | Validation | `k44ag9` | c | 400 |
| `query_limit_invalid` | Validation | `k4g8z5` | c | 400 |
| `query_max_page_bytes_invalid` | Validation | `k7zmpj` | c | 400 |
| `cursor_malformed` | Validation | `pndxkq` | c | 400 |
| `cursor_version_unknown` | Validation | `s62ybe` | c | 400 |
| `cursor_query_index_out_of_range` | Validation | `sevnxx` | c | 400 |
| `cursor_direction_mismatch` | Validation | `sfcvks` | c | 400 |
| `cursor_fingerprint_mismatch` | Validation | `t3kbec` | c | 400 |
| `num_tx_coordinators_invalid` | Validation | `uc9fkn` | c | 400 |
| `partition_context_options_invalid` | Validation | `nr8nsg` | c | 400 |
| `expression_invalid` | Expression | `ucjjtz` | c | 400 |
| `condition_failed` | ConditionCheck | `usbs9w` | c | 409 |
| `item_locked_by_transaction` | Conflict | `vnfeg6` | c | 409 |
| `timestamp_conflict` | Conflict | `vw99ky` | c | 409 |
| `pending_conflict` | Conflict | `w65ens` | c | 409 |
| `read_conflict` | Conflict | `wx4mnz` | c | 409 |
| `pending_write` | Conflict | `xam35s` | c | 409 |
| `clock_skew` | Conflict | `xy3rrw` | s | 503 |
| `item_too_large` | Validation | `ynzx4p` | c | 400 |
| `update_not_applicable` | Validation | `yysds3` | c | 400 |
| `update_value_is_bytes` | Validation | `z9ar7e` | c | 400 |
| `transaction_cancelled` | TransactionCancelled | `zd7rzd` | c | 409 |
| `transaction_undecided` | TransactionPending | `28ahbe` | s | 503 |
| `transaction_commit_pending` | TransactionPending | `3wbgez` | s | 503 |
| `partition_over_size` | Unavailable | `49j6ez` | s | 503 |
| `partition_migrating` | Unavailable | `4rpgyu` | s | 503 |
| `coordinator_over_size` | Unavailable | `tg8r62` | s | 503 |
| `prepare_unanswered` | Unavailable | `mpncbz` | s | 503 |
| `partition_misrouted` | Routing | `6ddzyj` | i | 500 |
| `range_partition_not_initialized` | Routing | `6ue24c` | i | 500 |
| `single_partition_fast_path_not_applicable` | Routing | `7647dt` | i | 500 |
| `invariant_failed` | Internal | `85quf8` | i | 500 |
| `partition_context_mismatch` | Internal | `8hv63q` | i | 500 |
| `item_data_parse_failed` | Internal | `dx9mht` | i | 500 |
| `commit_keyset_mismatch` | Internal | `e3kh5s` | i | 500 |
| `unexpected_transaction_state` | Internal | `j6uhd6` | i | 500 |
| `partition_fanout_failed` | Internal | `f3aqhc` | i | 500 |
| `foreign_error` | Internal | `jvufz5` | i | 500 |

A throw that has no code of its own maps to the nearest existing one: the `initFromSplit` conflict
uses `partition_context_mismatch`, and a stored row that cannot be read back uses
`unexpected_transaction_state`. `txCancel`'s fan-out failure reports `partition_fanout_failed` with
the first child error as `cause`.

`invariant()` raises `invariant_failed` with the fixed message `an internal invariant failed`. The text
of the call site can hold dynamic detail and internal names, so it goes to `attributes.detail`.

The store raises `item_too_large` when the stored row of a `putItem` is over the cap, because the
client check counts the data only and the store also counts both keys and the row overhead. It is the
same code a transaction reports for the same fact. On the transactional paths the check pass rejects
the item first, so the store never raises it there.

`PartitionContextCreator.create` raises `partition_context_options_invalid` for a topology option of
the consumer that is not valid, with the option name and the value in `attributes`.

### 7.2 The findings this RFC closes

`docs/ideas/error-handling/2026-09-10-existing-error-flows.md` numbers the findings F1 to F11.

| Finding | Where this RFC closes it |
| --- | --- |
| F1, one fact with two shapes | 4.2.5 |
| F2, the failure model changes with data placement | 4.2.5, the transaction tables |
| F3, `transient_error` loses the cause | 4.2.6 |
| F4, the caller cannot classify an error | 4.2.4, 4.2.16 |
| F5, nothing says whether a retry helps | Out of scope. See section 2.2. Section 4.2.9 gives the token rules. |
| F6, the retry policy is arbitrary | Out of scope. See section 2.2. |
| F7, no error carries structured data | 4.2.1, 4.2.2 |
| F8, a server fault and a caller fault share one type | 4.2.1, 2.3 rule 6 |
| F9, two reason vocabularies | 4.2.5 |
| F10, silent degradation | 4.2.6. The `queryItems` budget is out of scope. |
| F11, a getter does not cross a hop | 2.3 rule 4 |

References:

- `docs/ideas/error-handling/2026-09-10-existing-error-flows.md`
- `docs/agent-plans/2026-09-08-return-values-on-condition-check-failure.md`
- `README.md`, the "Features" list
- [better-result tagged errors](https://better-result.dev/errors/tagged-errors)
- [Workers RPC error handling](https://developers.cloudflare.com/workers/runtime-apis/rpc/error-handling/)
- [Durable Objects Storage API](https://developers.cloudflare.com/durable-objects/api/storage-api/)
