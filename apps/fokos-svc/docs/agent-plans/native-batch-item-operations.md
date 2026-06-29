# Native Batch Item Operations

Status: **proposal / not implemented**. This document captures the GitHub issue draft and the
implementation contract for native, non-transactional `BatchGet` / `BatchWrite` style operations in
FokosDB.

The scope is intentionally narrower than DynamoDB parity and broader than a client-side helper loop.
The core operations should be native FokosDB RPCs that preserve the existing partition routing,
migration, split, lock, promotion, and retry behavior.

## Source hierarchy and quality bar

Implementation decisions should be resolved in this order:

1. Existing FokosDB behavior and style. Match the public API shape in `FokosDB`, the HTTP RPC shape in
   `src/index.ts`, the validation style in `transaction-limits.ts`, the split/migration rules in
   `PartitionDO`, and the level of detail in the existing `queryItems` / `KeyCodec` plans.
2. Official DynamoDB BatchGetItem / BatchWriteItem documentation, used as the semantic baseline for
   limits, duplicate-key rejection, non-atomic writes, and `UnprocessedKeys` / `UnprocessedItems`:
   - <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html>
   - <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html>
3. Current Cloudflare Workers / Durable Objects limits, used only to choose safe FokosDB server-side
   bounds:
   - <https://developers.cloudflare.com/workers/platform/limits/>
   - <https://developers.cloudflare.com/durable-objects/platform/limits/>

This work should not be a thin wrapper over existing single-item calls. It should be a native,
well-factored implementation with tests first around operation semantics, limits, split/migration
guards, and partial-failure behavior.

## GitHub issue draft

### Title

Native non-transactional BatchGet/BatchWrite operations

### Body

FokosDB's README lists "Batch item operations (non-transactions)" as a TODO. This issue proposes a
native FokosDB implementation for non-transactional batch reads and writes:

- `batchGetItems({ items })`
- `batchWriteItems({ operations })`

The goal is a DynamoDB-shaped, FokosDB-native contract rather than full DynamoDB API parity.

#### Scope

- Add public `FokosDB` methods for `batchGetItems` and `batchWriteItems`.
- Add corresponding HTTP RPC actions.
- Add a focused operation-semantics note under `docs/agent-plans/`.
- Add tests for public API behavior, HTTP API behavior, validation, partial failures,
  split/migration routing, unprocessed retry paths, and request limits.

#### Locked semantics

- Empty batches reject during preflight.
- Each batch has a hard server-side max item count.
- Each batch has a hard server-side total payload byte ceiling.
- Each batch write operation has a hard forwarded sub-batch byte ceiling, so deterministic oversize
  writes reject during preflight instead of returning retryable `UnprocessedItems`.
- Server-side split/range fan-out is bounded so one request cannot hit unbounded Durable Object
  subrequests.
- `batchWriteItems` supports only `put` and `delete`.
- `batchWriteItems` has no conditions. Conditional writes remain the job of `transactWriteItems`.
- Duplicate keys reject the whole batch during preflight.
- Runtime write failures return retryable `UnprocessedItems`.
- Runtime read failures return retryable `UnprocessedKeys`.
- `batchGetItems` is strongly consistent per item, but is not a cross-partition snapshot.
- Retrying unprocessed puts/deletes is safe but not version-idempotent: last write wins and versions
  may bump again.

#### Retryable unprocessed cases

Return retryable unprocessed entries for:

- pending transaction locks;
- migration or split-in-progress guards;
- partition over-limit / retry-later errors;
- transient child RPC failures.

#### Implementation constraints

- Do not route batch writes through `TransactionCoordinatorDO`.
- Do not add SQL migrations.
- Do not call `validateTransactWriteOperations` directly. Extract shared validation helpers or add
  batch-specific validation so batch errors are named as batch errors.
- `batchWriteItems` must call `ensureMigration("batchWriteItems")` before routing, matching the
  write/transaction migration-guard rule.
- Forwarded child partitions must re-check migration at their own RPC boundary.
- Reuse transaction-style multi-item routing via `groupItemsByRouting`, but thread an `inputIndex`
  through routed work so results can be reconstructed in request order.
- Reuse the existing single-item local write behavior for pending-lock checks, condition-free
  upsert/delete, promotion queueing, and split checks.

#### Acceptance criteria

- Empty batches reject.
- Oversized item count, total payload bytes, or single-operation forwarded bytes are rejected by
  explicit validation; server-side split/range fan-out is bounded by item-count limits plus DO-side
  split/sub-batch guards, as defined by the implementation note.
- Duplicate keys reject.
- Mixed put/delete batch writes work.
- Partial write failures return retryable unprocessed items without hiding successful writes.
- Batch reads preserve input order and report unprocessed keys.
- Migration tests prove writes do not bypass `ensureMigration`.
- Existing tests pass.

#### Non-goals

- Full DynamoDB API parity.
- Conditional batch writes.
- Atomic batch writes.
- GSI or index behavior.
- Automatic retry helper, unless added separately as `FokosStd`.

## Implementation contract

### Public API

Use the same public key shape as the existing single-item and transaction APIs:

```ts
type BatchGetItemsOptions = {
  items: Array<{ hashKey: string | Uint8Array; sortKey?: string | Uint8Array }>;
};

type BatchWriteItemsOptions = {
  operations: Array<
    | { operation: "put"; hashKey: string | Uint8Array; sortKey?: string | Uint8Array; data: string | Uint8Array; ttlSeconds?: number; ttlEpochUTCSeconds?: number }
    | { operation: "delete"; hashKey: string | Uint8Array; sortKey?: string | Uint8Array }
  >;
};
```

The final result types should be decided in the implementation PR, but they must make these
properties explicit:

- results can be correlated back to the original input position;
- successful items are not hidden by unrelated failures;
- retryable failures carry enough information for a caller to retry just those entries;
- hard validation failures throw before any item is applied.

For `batchWriteItems`, the primary correlation field is `inputIndex`, not key matching. Results may
also echo keys for ergonomics/debugging, but callers should be able to reconstruct the outcome of the
request from `inputIndex` alone.

Do not return per-applied item versions from the core `batchWriteItems` result. A single `putItem`
returns a version, but batch write is a partial-success/retry API: it should report processed versus
unprocessed entries, not become a read-after-write result API. Callers that need versions can read the
items they care about after the batch completes.

### Batch write execution

`batchWriteItems` is non-atomic across the whole request and non-atomic within one partition. This is
intentional. Callers that need atomicity must use `transactWriteItems`.

The write path should compose existing behavior rather than bypass it:

1. Validate and encode public keys at the `FokosDB` boundary.
2. Reject empty batches, duplicate keys, unsupported operations, invalid keys, and oversized payloads
   before any write is attempted.
3. Pick initial partitions from the client-side topology.
4. At the `PartitionDO` RPC boundary, call `ensureMigration("batchWriteItems")` before routing.
5. Use `groupItemsByRouting` to split items into local and forwarded groups.
6. Keep `inputIndex` on every routed item so destination grouping does not destroy request order.
7. Forward child groups with `Promise.allSettled`.
8. Apply local items independently using the same semantics as single-item `putItem` and `deleteItem`:
   pending-lock check, upsert/delete, promotion queueing, and split checks.
9. Return successes and retryable unprocessed entries without converting sibling successes into
   failures.

The local write implementation should factor the existing single-item local bodies instead of copying
large blocks. If factoring is too invasive, copying the smallest necessary block is acceptable for the
first PR, but the behavior must stay aligned with single-item writes.

### Batch get execution

`batchGetItems` is strongly consistent per key. It is not a global point-in-time snapshot across
partitions; consistent multi-key snapshots remain the job of `transactGetItems`.

Reads should preserve input order and return `found: false` for missing items. Runtime read failures
that are safe to retry should be returned as `UnprocessedKeys`.

For migration, reads should keep the same behavior as `getItem`: a migrating child may read directly
from its parent rather than throwing.

### Limits

The implementation must define explicit server-side limits:

- max items per batch;
- max encoded payload bytes per batch;
- max server-side split/range fan-out or forwarded sub-batches per batch;
- max forwarded sub-batch payload bytes.

Do not rely on Cloudflare platform limits as the first line of defense. Pick conservative defaults
below the current Workers / Durable Objects limits, document the rationale in code, and keep the
constants easy to tune.

At implementation time, re-check the current official Cloudflare Workers limits and Durable Objects
limits before choosing byte and DO-side sub-batch constants:

- <https://developers.cloudflare.com/workers/platform/limits/>
- <https://developers.cloudflare.com/durable-objects/platform/limits/>

### Validation

Batch validation should share lower-level helpers with transaction validation where useful:

- public key validation;
- encoded key-size validation;
- duplicate key detection;
- payload-byte estimation;
- max item count checks.

Do not directly reuse `validateTransactWriteOperations` as the batch validator because its public
errors and constants are transaction-specific. Batch validation should produce batch-specific
diagnostics.

### Failure taxonomy

Use a discriminated union for per-item retryable failures. At minimum:

- `pending_lock`;
- `migration_in_progress`;
- `partition_over_limit`;
- `transient_error`.

When practical, `pending_lock` should carry the conflicting transaction id, matching the diagnostic
surface of the existing single-item write path.

Hard input errors should throw before work starts, not appear as unprocessed entries.

### Test checklist

- public `FokosDB.batchGetItems` preserves input order;
- public `FokosDB.batchGetItems` returns missing items as `found: false`;
- public `FokosDB.batchGetItems` reports retryable `UnprocessedKeys` for transient child failures;
- public `FokosDB.batchGetItems` rejects oversized item count and payload bytes;
- public `FokosDB.batchWriteItems` rejects empty operations;
- public `FokosDB.batchWriteItems` rejects duplicate keys, treating absent sort key as the empty key;
- public `FokosDB.batchWriteItems` rejects unsupported operations and conditions;
- public `FokosDB.batchWriteItems` rejects oversized item count and payload bytes;
- public `FokosDB.batchWriteItems` preserves input correlation across routed groups;
- mixed put/delete writes succeed;
- pending transaction locks become retryable unprocessed write entries;
- migration guard is called before routing and in forwarded children;
- partition over-limit / retry-later errors become retryable unprocessed write entries;
- successful sibling writes remain visible when another item is unprocessed;
- HTTP RPC schemas accept the valid request shapes and reject invalid shapes;
- existing transaction, query, migration, promotion, and split tests still pass.

### Preparation before implementation

Before writing production code:

1. Review the existing `queryItems`, transaction, validation, and HTTP RPC shapes and use them as the
   primary local pattern.
2. Check the official DynamoDB BatchGetItem / BatchWriteItem docs and record the specific semantics
   being adopted or intentionally changed.
3. Confirm the final public result shapes for `BatchGetItemsResult` and `BatchWriteItemsResult`.
4. Confirm concrete server defaults for max items, max payload bytes, and forwarded sub-batch bytes
   after checking current Cloudflare limits.
5. Decide and implement DO-side split/range subrequest bounding. Do not enforce fan-out by counting
   `FokosDB`'s initial `pickPartition` roots: `rootTreesN` is user-configurable, and split/range
   expansion happens behind the initial root. M2/M3 should bound real subrequest growth with item-count
   limits plus DO-side split/sub-batch guards.
6. Factor the existing single-item local write bodies properly enough that batch writes cannot drift
   from `putItem` / `deleteItem` behavior.
7. Extract or design shared validation helpers without changing transaction error strings.
8. Add tests for validation and result-shape behavior first, then fill in the implementation.
9. Run the full FokosDB test suite before asking for review.
