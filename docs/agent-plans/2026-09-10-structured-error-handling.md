# RFC — Structured errors across the FokosDB library

**State:** Draft
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
  reconstruction and drops custom own properties.
- `ctx.storage.transactionSync(callback)` rolls back when the callback throws. It commits when the
  callback returns. The source has 20 such blocks.
- `tryWhile` from `durable-utils` retries a function that throws. The source has 11 call sites.
- Four errors cross an RPC boundary and a predicate matches each one by a substring of the message.
  `isPartitionExceededDatabaseSizeError` and `isSinglePartitionFastPathFallbackError` live in
  `packages/fokosdb/src/shared/partition-errors.ts`. `isTransactionUndecidedError` and
  `isTransactionCommitPendingError` live in `do-transaction-coordinator.ts`.
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
2. Every error must carry a category, a code, an `error_id`, an origin, a retryable field, and an
   `httpStatusHint`.
3. The category and the code must be contractual. The message must not be contractual.
4. Classification must never depend on `instanceof`.
5. The four message-substring predicates must be removed. A code check replaces each one.
6. A cancelled `transactWriteItems` must report each operation with the same error shape that a
   thrown error uses. `RejectionReason` must be removed.
7. A failed prepare must keep the cause of the failure. It must not report `transient_error` alone.
8. A partition must stamp its routing meta on an error, and each forwarding level must learn from it.
9. The library must raise one error type for a condition, whichever operation the caller used.
10. `destroy()` must stop at the first failure.

### 2.2 Out of scope

| Item | Reason |
| --- | --- |
| `Result<T, E>` helpers and `tryXyz()` methods | A later RFC. This RFC throws everywhere and adds no `Result` API. |
| Early pagination of `queryItems` on an exhausted budget | Metadata, not a failure. It belongs in `meta`. |
| `FokosDB.updateItem` | It does not exist yet. The shapes here extend to it without a change. |
| A change to the expression sub-library | A wrapper carries `ExpressionError`. See section 4.2.7. |

### 2.3 Constraints

1. The consumer must set a `compatibility_date` of `2026-04-21` or later. The library must fail at
   startup with a clear message when own properties do not cross a hop.
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

1. The base class, the category classes, the code registry, and the startup probe.
2. The item operations: `putItem`, `getItem`, `deleteItem`, `queryItems`, and the validation helpers.
3. The partition internals: the stamp in `#rpc`, the learning in the two forwarding paths, and the
   removal of the two `partition-errors.ts` predicates.
4. The transaction paths: the per-operation results, the cause of a failed prepare, and the removal
   of the two coordinator predicates.
5. The test migration from message assertions to code assertions.
6. The example worker: the `httpStatusHint` mapping in `api.onError`.

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

An error carries six fields that a caller can act on, and any number of attributes for its own code.

```
FokosConflictError
  name            "FokosConflictError"        <- the category, and the value of _tag
  type            "conflict_error"            <- the category in snake case
  code            "item_locked_by_transaction" <- the fine-grained identifier
  error_id        "e_sevnxx_<32 hex>"         <- unique to this one event
  origin          "caller"                    <- "caller", "service" or "internal"
  retryable       true                        <- the condition is transient
  httpStatusHint  409
  attributes      { transactionId, hashKey, sortKey }
```

`name`, `type`, `code`, and the other five are own data properties, so each one crosses an RPC hop.
The prototype does not cross a hop, so the library never classifies with `instanceof`. It compares
`name` or `code`, and it gives callers a guard that does the same.

One rule has one exception. A participant answers about N operations at once, and a throw carries one
answer. So the prepare and single-shot RPCs keep a returned union, and `db.ts` converts it to a
thrown `FokosTransactionCancelledError`. Section 4.2.5 states the exception, and section 5.1 gives
the reason the RFC does not remove it.

The change also closes a gap that exists today. A partition learns the topology of its descendants
from `meta` on a successful response. An error carries no `meta`, so a throw loses that knowledge.
Each error now carries the same routing meta, and each forwarding level reads it before it rethrows.

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

`packages/fokosdb/src/shared/errors.ts` holds every class. One file, so the registry of codes and the
classes cannot drift apart.

`FokosError` extends `Error` and declares the contractual fields. Each category extends `FokosError`
and declares its tag once, as a static. The constructor assigns every field as an own property, so
every field crosses a hop.

```ts
export abstract class FokosError extends Error {
    readonly _tag: string;
    readonly type: string;
    readonly code: string;
    readonly error_id: string;
    readonly origin: "caller" | "service" | "internal";
    readonly retryable: boolean;
    readonly httpStatusHint: number;

    /** True after any number of hops. It reads own properties only. See section 4.2.4. */
    static is(e: unknown): e is FokosErrorData<FokosAnyError>;

    /** Wraps a foreign fault as `foreign_error` and keeps it as `cause`. See section 4.2.10. */
    static wrap(e: unknown): FokosError;

    /** The plain shape that crosses a hop inside a value. See section 4.2.5. */
    toWire(): FokosErrorWire;
    static fromWire(w: FokosErrorWire): FokosError;
}
```

A static `tag` on each category is what lets one generic guard replace one guard per category. The
base class assigns `_tag` from it, so the string appears once.

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

The origin, the retryable field, and the `httpStatusHint` belong to the code, not to the category.
The registry declares all three for each code. The table above gives the common value for each
category. A category must not override the value the registry declares.

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

#### 4.2.2 The code registry and `error_id`

The registry maps each code to its category, its origin, its retryable field, its `httpStatusHint`,
and its 6-character segment. Section 7.1 holds the starting registry.

The `error_id` has the form `e_<segment>_<suffix>`.

- `<segment>` is 6 characters, fixed for the life of the code. An author assigns it by hand. The only
  rule is that it must be unique in the registry. A test asserts that every segment in the registry
  is unique.
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

The message starts with the code, then a fixed phrase, then the dynamic detail, if the code has any:

```
fokos/<code>: <fixed phrase> (<attribute summary>)
```

`name` holds the category and nothing else, so a check on `name` stays exact. The code appears in the
message for a human who reads a log line.

#### 4.2.4 Classification

A hop drops the prototype, so a guard must read own properties only.
`packages/fokosdb/test/tagged-error-rpc.test.ts` measures this over a real Durable Object RPC call:
`name`, `_tag`, `cause` and every payload field arrive, and every prototype member is gone.

Two types carry the classification.

```ts
/** The union of the nine categories. `_tag` is a literal here, so a switch narrows and stays exhaustive. */
export type FokosAnyError = FokosValidationError | FokosConflictError | /* ... */ FokosInternalError;

/** An error as it arrives after a hop: the data, without the prototype members. */
export type FokosErrorData<E> = Omit<E, "toWire">;
```

`FokosError` and `FokosAnyError` are not the same thing, and both are needed. `FokosError` is the base
class that a category extends. `FokosAnyError` is the union that a caller narrows, because `_tag` on
the base is `string` and `_tag` on the union is a literal.

The library gives three ways to classify an error, and none of them uses `instanceof`:

```ts
// 1. Any error this library raises, after any number of hops.
FokosError.is(e)

// 2. One category. The static tag makes one generic guard enough for all nine.
isFokosErrorOf(e, FokosConflictError)

// 3. One code.
e.code === "item_locked_by_transaction"
```

```ts
export function isFokosErrorOf<C extends { tag: string; prototype: FokosAnyError }>(
    e: unknown,
    cls: C,
): e is FokosErrorData<C["prototype"]> {
    return FokosError.is(e) && e._tag === cls.tag;
}
```

A guard must narrow to `FokosErrorData`, not to the class. After a hop the object holds no prototype
member, so a narrowing to the class lets the compiler offer a method that is not there.

A caller that runs in the same isolate as `db.ts` can also use `instanceof`, because `db.ts` builds
the class in that isolate. The library must not document `instanceof` as a way to classify. A consumer
that puts its own Worker in front of FokosDB and rethrows an error gives its caller an object with no
prototype, and `instanceof` fails there.

Often no guard is needed. `_tag` is a literal discriminant, so a switch narrows on its own:

```ts
if (FokosError.is(e)) {
    switch (e._tag) {
        case "FokosConflictError": return retry();
        case "FokosValidationError": return badRequest(e.code);
    }
}
```

#### 4.2.5 The transaction exception to the rule

A participant answers about N operations at once. It reports `passed`, `not_evaluated`, or `rejected`
for each one. A throw carries one answer, so it cannot report the other N-1 operations.

So `PrepareResponse` and `SingleShotResponse` keep their returned union. `db.ts` converts a
`cancelled` outcome to a thrown `FokosTransactionCancelledError`, which carries the `results` array.
This is the only deviation from "everything except a happy-path answer throws".

A returned union also removes a dependency. The results array crosses a hop as a return value, which
structured clone has always carried. It does not depend on `enhanced_error_serialization`.

Each entry of `results` holds the serialized shape of an error: `name`, `type`, `code`, `error_id`,
`origin`, `retryable`, `httpStatusHint`, and the attributes. It must not hold an `Error` instance,
because a nested instance loses its prototype at the first hop and the next hop cannot serialize it
again.

`FokosError.toWire` and `FokosError.fromWire` are the two ends of that array. A participant calls
`toWire` when it puts a rejected operation into `results`. A router partition merges the arrays of its
children and converts nothing, because the entries are already plain. The coordinator stores them in
`tc_results` and replays them without conversion. `db.ts` calls `fromWire` on each entry at the public
boundary, so a caller receives real classes inside `FokosTransactionCancelledError.results`.

`fromWire` also materializes a single thrown error that crossed a hop, because a reconstructed error
carries the same fields. So one converter serves both paths.

`RejectionReason` is removed. So `item_too_large` from `putItem` and `item_too_large` inside a
cancelled transaction are one code with one shape.

#### 4.2.6 A failed prepare keeps its cause

`cancelTransactionInStore` in `do-transaction-coordinator.ts` reports `{ type: "transient_error" }`
for every participant whose prepare threw after its retries. A transport failure, a migrating
partition, an `ExpressionError`, a mis-routed item, and an over-size partition become one value.

The coordinator now stores the serialized error of the failed prepare and reports it. A caller learns
whether the failure is transient, and a caller that retries a deterministic fault stops looping.

#### 4.2.7 `ExpressionError`

`ExpressionError` stays as it is, in `packages/fokosdb/src/shared/expression/errors.ts`. The
expression sub-library does not change.

Each caller of `compileConditionExpression`, `compileUpdateExpression`, `evaluateConditionPlan`, and
`probeUpdatePlan` wraps an `ExpressionError` in a `FokosExpressionError`. The wrapper sets `cause` to
the original error and copies its `ExpressionErrorCode` into an attribute.

`client/index.ts` keeps its `ExpressionError` export, so a consumer that catches it today still
catches it.

#### 4.2.8 Topology learning on the error path

A partition learns the boundaries of its descendants from `meta` on a response. `recordForwardResult`
runs after a successful forward in `withSplitForwarding` and in `forwardToRangeRootPartition`. An
error carries no `meta` today, so a throw loses that knowledge. The fast-path fallback loses the most:
the node that raises it knows that its items straddle its children, which is the knowledge the
ancestor wants.

Four rules fix it.

1. **The stamp point.** `#rpc` in `do-partition.ts` wraps every RPC method, so it is the one place
   that stamps. It catches, attaches the routing meta of the partition, and rethrows. It stamps only
   when `#_partitionContext` is set, so an error raised before the partition resolves its context
   does not carry a meaningless context.
2. **The first stamp wins.** `#rpc` attaches a meta only when the error carries none. On the success
   path, the `rangeAncestors` of the leaf travel upward untouched while each hop increments
   `forwardCount`. An error follows the same rule, so the deepest knowledge reaches the top.
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

#### 4.2.9 The retryable field

`retryable` says that the condition is transient. It does not say that the caller can safely repeat
the call. Retry safety belongs to the operation, not to the error. A `transactWriteItems` that
carries no `clientRequestToken` must not repeat after a failure, because the first attempt can have
applied. The coordinator never retries a write for that reason.

Each `tryWhile` predicate reads `err.retryable` in place of its own test. The attempt counts stay per
call site, because the budgets differ. Section 4.3.1 holds the open question about where the policy
lives.

#### 4.2.10 Foreign errors and the two string matches

`toFokosError(e: unknown): FokosError` wraps anything that is not a `FokosError`. It sets `cause`, and
it gives the error the code `foreign_error` with the origin `internal`. Each RPC entry point and each
public method wraps its body, so every error that leaves `db.ts` is a `FokosError`.

A fault that the library did not cause stays outside the contract. A syntax error in consumer code is
an example. `toFokosError` reports such a fault as `foreign_error`, and `FokosInternalError` must not
hide a defect in this library behind that code.

Two string matches stay.

1. `isDestroyAbortError` in `packages/fokosdb/src/shared/cf-utils.ts` matches
   `DESTROY_ABORT_SENTINEL`. `ctx.abort(message)` makes the runtime construct the error, and the
   library only supplies the string. So the error can never carry a property. This is a documented
   special case, and it is the second deviation from the rules of this RFC.
2. The startup probe in section 4.2.11 reads a probe property and falls back to the message when the
   property is absent. That is the test that detects a wrong compatibility date.

#### 4.2.11 The compatibility-date requirement

The library needs own properties to cross a hop. Both Durable Object classes run in the script of the
consumer, so one `compatibility_date` governs every internal hop.

The library probes once, on the first request that reaches a `PartitionDO`. It throws an error with a
probe own property across one hop and checks whether the property arrives. When the property is
absent, the library raises a `FokosInternalError` with the code `error_serialization_unsupported` and
a message that names the needed compatibility date. A consumer that sets
`legacy_error_serialization` gets a clear failure instead of errors with no code.

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

1. A test asserts that every 6-character segment in the registry is unique.
2. A test asserts that every category, code, origin, retryable field, and `httpStatusHint` crosses an
   RPC hop as an own property.
   A test asserts that `toWire` and `fromWire` round-trip every category without loss.
3. `packages/fokosdb/test/tagged-error-rpc.test.ts` pins what an error keeps across a hop, and which
   guards work there. It must fail if a runtime change stops an own property from crossing.
4. A test asserts that a stamped meta reaches each forwarding level, and that a malformed meta does
   not replace the original error.
5. The 185 `toThrow` assertions in the suite move from a message match to a code match. The message
   is not contractual, so a test must not pin it.

#### 4.2.16 The example worker

`api.onError` in `examples/http-api/index.ts` reads `httpStatusHint`, and it returns the category, the
code, and the `error_id` in the body. A failed condition stops returning 500. The handler keeps its
`HTTPException` branch.

### 4.3 Open Questions

#### 4.3.1 Where the retry policy lives

Each `tryWhile` predicate can read `err.retryable` and keep its own attempt count, or one helper can
own a default budget for every call site. The first option keeps the six budgets that the source
already tunes per path. The second option gives one place to change. The answer changes how many call
sites the work touches, and it does not change the shape of an error.

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
thrown error also keeps the fallback visible, and section 4.2.8 makes the node that raises it teach
its ancestor the boundaries it found.

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

**Why is `item_too_large` not a returned outcome?**
It is the same fault as the 400 KB check that `validateItemDataSize` runs on the client. The partition
detects it later because it measures the stored row. A caller treats both the same way, so both get
one code and one shape.

**Why does `retryable` not mean "safe to retry"?**
`retryable` describes the condition. Retry safety describes the call. A `transactWriteItems` with no
`clientRequestToken` can have applied its first attempt, so a repeat can apply it twice. A field that
reads as "safe to retry" invites that defect.

**What does a caller see when an error crosses its own middle-layer Worker?**
The `name`, the `message`, and every own property, which covers the category, the code, the
`error_id`, the origin, the retryable field, and the attributes. The prototype does not cross, so
`instanceof` fails there. `isFokosError` and a check on `code` both work.

**Why does the library keep two string matches?**
`ctx.abort` makes the runtime build the error, so the library cannot attach a property to it. The
startup probe must work when own properties are absent, because that is the condition it detects.

**Why does the stamp happen in `#rpc` and not at each throw site?**
`#rpc` wraps every RPC method of `PartitionDO`, so one change covers every error. A stamp at each
throw site needs an edit at more than 30 places, and a new throw site can forget it.

**Does an error replace the `meta` that a successful response carries?**
No. The success path keeps `meta` on the response. An error carries the same routing meta as an own
property, so both paths feed `recordForwardResult`.

---

## 7. Appendix

### 7.1 The starting code registry

The origin is `c` for caller, `s` for service, and `i` for internal. The `R` column is the retryable
field.

| Code | Category | Segment | Origin | R | Hint |
| --- | --- | --- | --- | --- | --- |
| `hash_key_empty` | Validation | `2fzzq9` | c | no | 400 |
| `sort_key_empty` | Validation | `2gjvju` | c | no | 400 |
| `key_contains_nul` | Validation | `42r8z7` | c | no | 400 |
| `key_not_well_formed_utf16` | Validation | `4767pp` | c | no | 400 |
| `hash_key_too_large` | Validation | `4v2p4p` | c | no | 400 |
| `sort_key_too_large` | Validation | `58daxm` | c | no | 400 |
| `key_encode_empty` | Validation | `58sjts` | c | no | 400 |
| `item_data_too_large` | Validation | `6z7eb3` | c | no | 400 |
| `item_data_wrong_type` | Validation | `7vxpb8` | c | no | 400 |
| `item_data_not_json_serializable` | Validation | `bfvvtt` | c | no | 400 |
| `ttl_at_invalid` | Validation | `brcy77` | c | no | 400 |
| `return_values_option_invalid` | Validation | `ed9wyr` | c | no | 400 |
| `client_request_token_invalid` | Validation | `f9azze` | c | no | 400 |
| `idempotent_parameter_mismatch` | Validation | `fn733z` | c | no | 400 |
| `transact_items_empty` | Validation | `gmjfgw` | c | no | 400 |
| `transact_items_too_many` | Validation | `h58dgv` | c | no | 400 |
| `transact_duplicate_key` | Validation | `hgxg2r` | c | no | 400 |
| `transact_payload_too_large` | Validation | `hsvepa` | c | no | 400 |
| `transact_operation_fields_invalid` | Validation | `jr49a5` | c | no | 400 |
| `query_queries_empty` | Validation | `k44ag9` | c | no | 400 |
| `query_limit_invalid` | Validation | `k4g8z5` | c | no | 400 |
| `query_max_page_bytes_invalid` | Validation | `k7zmpj` | c | no | 400 |
| `cursor_malformed` | Validation | `pndxkq` | c | no | 400 |
| `cursor_version_unknown` | Validation | `s62ybe` | c | no | 400 |
| `cursor_query_index_out_of_range` | Validation | `sevnxx` | c | no | 400 |
| `cursor_direction_mismatch` | Validation | `sfcvks` | c | no | 400 |
| `cursor_fingerprint_mismatch` | Validation | `t3kbec` | c | no | 400 |
| `num_tx_coordinators_invalid` | Validation | `uc9fkn` | c | no | 400 |
| `expression_invalid` | Expression | `ucjjtz` | c | no | 400 |
| `condition_failed` | ConditionCheck | `usbs9w` | c | no | 409 |
| `item_locked_by_transaction` | Conflict | `vnfeg6` | c | yes | 409 |
| `timestamp_conflict` | Conflict | `vw99ky` | c | yes | 409 |
| `pending_conflict` | Conflict | `w65ens` | c | yes | 409 |
| `read_conflict` | Conflict | `wx4mnz` | c | yes | 409 |
| `pending_write` | Conflict | `xam35s` | c | yes | 409 |
| `clock_skew` | Conflict | `xy3rrw` | s | yes | 503 |
| `item_too_large` | Validation | `ynzx4p` | c | no | 400 |
| `update_not_applicable` | Validation | `yysds3` | c | no | 400 |
| `update_value_is_bytes` | Validation | `z9ar7e` | c | no | 400 |
| `transaction_cancelled` | TransactionCancelled | `zd7rzd` | c | no | 409 |
| `transaction_undecided` | TransactionPending | `28ahbe` | s | yes | 503 |
| `transaction_commit_pending` | TransactionPending | `3wbgez` | s | yes | 503 |
| `partition_over_size` | Unavailable | `49j6ez` | s | yes | 503 |
| `partition_migrating` | Unavailable | `4rpgyu` | s | yes | 503 |
| `partition_misrouted` | Routing | `6ddzyj` | i | no | 500 |
| `range_partition_not_initialized` | Routing | `6ue24c` | i | no | 500 |
| `single_partition_fast_path_not_applicable` | Routing | `7647dt` | i | no | 500 |
| `invariant_failed` | Internal | `85quf8` | i | no | 500 |
| `partition_context_mismatch` | Internal | `8hv63q` | i | no | 500 |
| `stored_item_too_large` | Internal | `cd8y95` | i | no | 500 |
| `item_data_parse_failed` | Internal | `dx9mht` | i | no | 500 |
| `commit_keyset_mismatch` | Internal | `e3kh5s` | i | no | 500 |
| `item_not_found_for_update` | Internal | `h5vq43` | i | no | 500 |
| `unexpected_transaction_state` | Internal | `j6uhd6` | i | no | 500 |
| `error_serialization_unsupported` | Internal | `j7fhqe` | i | no | 500 |
| `foreign_error` | Internal | `jvufz5` | i | no | 500 |

### 7.2 The findings this RFC closes

`docs/ideas/error-handling/2026-09-10-existing-error-flows.md` numbers the findings F1 to F11.

| Finding | Where this RFC closes it |
| --- | --- |
| F1, one fact with two shapes | 4.2.5 |
| F2, the failure model changes with data placement | 4.2.5 |
| F3, `transient_error` loses the cause | 4.2.6 |
| F4, the caller cannot classify an error | 4.2.4, 4.2.16 |
| F5, nothing says whether a retry helps | 4.2.9 |
| F6, the retry policy is arbitrary | 4.2.9, 4.3.1 |
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
