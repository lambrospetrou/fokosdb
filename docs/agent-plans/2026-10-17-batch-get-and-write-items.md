# RFC — Batch item operations: `batchGetItems` and `batchWriteItems`

**State:** Draft
**Date:** 2026-10-17
**Author:** Lambros

## Table of Contents

- [1. Overview and Context](#1-overview-and-context)
  - [1.1 The current system](#11-the-current-system)
  - [1.2 The problem](#12-the-problem)
  - [1.3 Facts that constrain the design](#13-facts-that-constrain-the-design)
  - [1.4 Glossary](#14-glossary)
- [2. Goals and Requirements](#2-goals-and-requirements)
  - [2.1 In scope](#21-in-scope)
  - [2.2 Requirements](#22-requirements)
  - [2.3 Out of scope](#23-out-of-scope)
- [3. Milestones](#3-milestones)
- [4. Proposed Solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
    - [4.2.1 Public API](#421-public-api)
    - [4.2.2 Validation and limits](#422-validation-and-limits)
    - [4.2.3 Wire types](#423-wire-types)
    - [4.2.4 Outcomes and their classification](#424-outcomes-and-their-classification)
    - [4.2.5 Runtime: the `collect` failure policy](#425-runtime-the-collect-failure-policy)
    - [4.2.6 Runtime: read-through for a `group` operation](#426-runtime-read-through-for-a-group-operation)
    - [4.2.7 `apiBatchGetItems`](#427-apibatchgetitems)
    - [4.2.8 The response limit of a batch read](#428-the-response-limit-of-a-batch-read)
    - [4.2.9 Projections](#429-projections)
    - [4.2.10 `apiBatchWriteItems`](#4210-apibatchwriteitems)
    - [4.2.11 The client](#4211-the-client)
    - [4.2.12 `FokosStd` retry helpers](#4212-fokosstd-retry-helpers)
    - [4.2.13 Invariants](#4213-invariants)
    - [4.2.14 Transactions, splits, promotions, and TTL](#4214-transactions-splits-promotions-and-ttl)
    - [4.2.15 Performance](#4215-performance)
    - [4.2.16 Deployment](#4216-deployment)
    - [4.2.17 Testing](#4217-testing)
- [5. Alternative Options](#5-alternative-options)
- [6. Frequently Asked Questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and Context

### 1.1 The current system

FokosDB has three kinds of item access. `db.ts` sends every partition RPC and every coordinator RPC through a
`FokosShardingClient` (`src/sharding/client.ts`). Each partition RPC is one `this.fokos.dispatch(op, ctx, req)` call on
`PartitionDO` (`src/server/do-partition.ts`).

- **Single-item operations.** `putItem`, `getItem` and `deleteItem` are `point` operations. `db.ts` sends one RPC to
  the root partition of the key, and the root forwards the request to the owner.
- **Transactions.** `transactWriteItems` goes through a `TransactionCoordinatorDO` with two-phase commit, or through
  the single-partition fast path. `transactGetItems` reads every key two times and compares the versions. Both
  methods lock or abort on contention.
- **Queries.** `queryItems` is a `range` operation over the sort keys of one hash key.

The `group` shape sends many keys in one request. `#readTransaction` in `src/client/db.ts` groups the keys by root
partition with `FokosShardingClient.resolveAll` and sends one RPC per root with `send`. Each root resolves the owner
of each key, runs the keys it owns in its local handler, and forwards one sub-request to each other owner. `merge`
joins the answers of the parts. `txPrepare`, `txCommit`, `txCancel` and `txReadForTransaction` are the `group`
operations today.

A `group` operation has one of two failure policies (`FokosOperation` in `src/sharding/runtime-types.ts`):

- `fail_fast` throws at the first failed part.
- `attempt_all` runs every part, then throws `partition_fanout_failed` when one part failed.

### 1.2 The problem

A caller has no way to read or write many independent items in one call:

- A transaction locks its items, aborts on contention, and costs a coordinator round trip on the multi-partition path.
- One single-item call per item costs one Worker-to-partition RPC per item.

DynamoDB has `BatchGetItem` and `BatchWriteItem` for this case, and the FokosDB API follows DynamoDB. The GSI plan
also needs a batch write: its Milestone 3 writes each GSI table with `batchWriteItems`
(`docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md`).

### 1.3 Facts that constrain the design

- **A `group` operation cannot report a partial success.** `fail_fast` drops the parts that answered.
  `attempt_all` throws after every part settles, and the results of the parts that applied are lost. The admission
  of the local entries runs in `#dispatchGroup` before `#runGroups`, so a refusal throws for the whole request before
  any remote part starts (`src/sharding/runtime.ts`).
- **Read-through supports only the `point` and `range` shapes.** `#validateOperations` refuses `read_source` for any
  other shape, and `#executeLocal` serves only those two. A `group` read on an importing partition gets
  `partition_migrating`, while `getItem` reads through to the source.
- **A Workers RPC message can be at most 32 MiB** (the Workers RPC documentation, section "Limitations"). The
  constant `WORKERS_RPC_MAX_BYTES` in `src/sharding/rpc-limits.ts` holds this value. Each limit on one RPC message
  is a fraction of it.
- **An item can be 400 KB** (`MAX_ITEM_BYTES` in `src/shared/transaction-limits.ts`). `estimateItemBytes` in
  `src/shared/partition/partition-store.ts` counts text as 2 bytes per character. Thus 100 items can need more than
  32 MiB in one answer.
- **A partition refuses a non-transactional write to a locked item at once.** It does not wait for the lock
  (`itemLockedError` in `src/server/do-partition.ts`).
- **A partition refuses a write above 1.1 times its cap.** The `admit` hook gates on the admission tag `write` only,
  because a delete is how a client brings an over-size partition back under its cap.
- **A local handler writes in synchronous `transactionSync` blocks.** A block that throws rolls back its own writes.
  A block that completed keeps its writes, also when the handler throws later.
- **`projectionPlanOf` compiles on each call** (`src/shared/expression/request-plans.ts`). `updatePlanOf` keeps the
  plan of a tree in a `WeakMap`, so one request compiles each update tree one time.
- **The single-shot fast path classifies an error.** An error with routing that is not `foreign_error` means that
  nothing applied. A foreign error can be an answer lost after the apply, so its outcome is unknown
  (`#writeSingleShotFastPath` in `src/client/db.ts`).
- **The client bundle has a budget of 100 KiB minified** (`CLIENT_MAX_BYTES` in `tsdown.config.ts`). The client can
  import `durable-utils/*`, which has `jitterBackoff` in `durable-utils/retries`.

### 1.4 Glossary

- **Batch** — the items of one `batchGetItems` or `batchWriteItems` call.
- **Part** — the keys of a `group` request that one partition runs: the local part, or one remote sub-request.
- **Failure part** — a part that failed: its admission or its local handler threw, or its forward still failed after
  the miss ladder. It carries the error in place of a result.
- **`opIndex`** — the position of an item in the batch. Every wire answer names its items by `opIndex`.
- **Share** — the response bytes that one part of a batch read can return.

## 2. Goals and Requirements

### 2.1 In scope

1. `FokosDB.batchGetItems` reads 1 to 100 items by key and returns one result for each item, in request order.
2. `FokosDB.batchWriteItems` puts or deletes 1 to 100 items and returns one result for each item, in request order.
3. Each result says whether the item applied or the read answered, and whether the caller can send the item again.
4. The result of each method carries `unprocessedKeys` or `unprocessedItems`: the entries to send again, in the
   request format.
5. A `group` operation can return a partial success, with the failure policy `collect`.
6. A `group` operation can read through to its source while its partition imports.
7. `FokosStd` has one retry helper for each batch method. Each helper sends the unprocessed entries again, with
   exponential backoff and jitter, up to a maximum number of attempts.

### 2.2 Requirements

- Each item must apply or read atomically. The batch is not atomic and gives no isolation across items.
- A batch read must return the committed state of each item, as `getItem` does.
- A batch write must apply each item with the same functions as `putItem` and `deleteItem`: the lock check, the
  upsert, and the delete.
- After I/O starts, neither method throws. A validation error throws before any I/O.
- Neither method retries. The `FokosStd` helpers do the retries.
- A partition answer must not carry a key. The client builds the public results from its own request, by `opIndex`.
- One batch read answer must stay under the 32 MiB RPC limit.
- The client does not cap the payload of a batch write. A root RPC above the RPC limit fails at the RPC, and each item
  of that root comes back `rejected` (section 4.2.4).
- A batch write handler must not throw after its first write. The code that runs after a committed write block must
  not throw.
- `merge` of a `collect` operation must not throw.
- The current `group` operations must keep their failure policies and their behavior.

### 2.3 Out of scope

- Conditions, update expressions, and `returnValuesOnConditionCheckFailure` in a batch write. `BatchWriteItem` has
  none of them, and `putItem`, `deleteItem` and `transactWriteItems` cover them.
- Eventually consistent reads. Every FokosDB read is strongly consistent.
- More than one table per call. One `FokosDB` client is one table.
- Retries inside `batchGetItems` and `batchWriteItems`.
- Chunking and parallel calls in the `FokosStd` helpers. A helper takes the limits of its method. A later change can
  add both.
- A change to `transactGetItems`, `transactWriteItems`, or the coordinator.

## 3. Milestones

Each milestone ships on its own. The read comes first: it has no unknown outcome and no write rules, so the `collect`
policy gets its first user without a risk to stored data.

1. **The `collect` failure policy** (section 4.2.5). A `group` operation can return a partial success. The tests use
   the counter host of `test/sharding/`. AGENTS.md gets the exception to the rule "Never swallow a group error", and
   hazard 3 of `.claude/skills/spec-review/references/review-checklist.md` gets the same exception.
2. **`batchGetItems`** (sections 4.2.1 to 4.2.4, 4.2.7 to 4.2.9, 4.2.11). The public types, the validation, the wire
   types, `apiBatchGetItems` with `whileMigrating: "throw"`, the response budget, the projection memo, the `db.ts`
   method, and the exports. A key on an importing partition comes back `unprocessed` with `partition_migrating`.
3. **Read-through for `group`** (section 4.2.6). `apiBatchGetItems` changes to `whileMigrating: "read_source"`, and
   a batch read reads through as `getItem` does. AGENTS.md lists `apiBatchGetItems` with the operations that read
   through. Hazard 1 of `.claude/skills/spec-review/references/review-checklist.md` adds a read-only `group`
   operation to the operations that can use `"read_source"`.
4. **`batchWriteItems`** (section 4.2.10). The public types, the validation, the wire types, `apiBatchWriteItems`, the
   shared write functions, the per-item admission, the `db.ts` method, and the exports.
5. **The `FokosStd` retry helpers** (section 4.2.12).
6. **Surface.** The routes of the HTTP example (`examples/http-api/src/rpc/`), the batch commands of the property-based
   item model, and the shape list of AGENTS.md.

## 4. Proposed Solution

### 4.1 High-level overview

The two methods use one pattern. `db.ts` validates the batch and gives each item its `opIndex`. It groups the items by
root partition and sends one RPC to each root, all at the same time. Each root runs a `group` operation with the new
failure policy `collect`. The root runs its own keys, forwards one sub-request to each other owner, and joins the
answers. A part that fails does not fail the request: its items get an outcome from the error.

A partition answers with lists by `opIndex` and carries no key. `db.ts` joins the answers of the roots and builds one
result for each request item, in request order. It also builds the list of entries to send again.

```
 Worker (db.ts)                       root partition R1                        owners
 ──────────────                       ─────────────────                        ──────
 validate, encode, opIndex 0..n-1
 resolveAll: items by root
 R1: ops 0, 2, 5 ──────────────────▶  group, failurePolicy "collect"
                                      local part: op 0 ──▶ handler
                                      remote part: ops 2, 5 ─────────────────▶ child C: handler
                                      merge: lists by opIndex  ◀──────────────  (or a failure part)
 R2: ops 1, 3, 4 ──────▶ ...
 join by opIndex  ◀────────────────
 results[i], unprocessed entries, meta
```

Each item has one of four outcomes:

| Outcome | Meaning | Send it again? |
| --- | --- | --- |
| `applied` (write), `found` or `not_found` (read) | The item applied, or the read answered. | — |
| `unprocessed` | Nothing of the item applied. | Yes |
| `rejected` | The same request fails the same way again, for example `item_too_large`. | No |
| `unknown` (write only) | A transport error or an internal error. The item can have applied. | Yes (section 6.3) |

`FokosStd.batchGetItemsWithRetry` and `FokosStd.batchWriteItemsWithRetry` call the method again with the unprocessed
entries until none stays or the attempts run out.

### 4.2 Technical details

#### 4.2.1 Public API

The public batch types live in a new module, `src/shared/batch-api-types.ts`. `client/index.ts` exports each name.
`FokosDBAPI` in `src/shared/transaction-api-types.ts` adds `ItemBatcher`.

```ts
/** One key of `batchGetItems`. Its projection replaces the projection of the request. */
export type BatchGetItemKey = ItemKey & { projection?: readonly ProjectionExpression[] };

/** `Ts` names the type of each item by position, as `TransactGetItemsOptions` does. */
export type BatchGetItemsOptions<Ts extends readonly unknown[] = never[]> = {
	/** 0 to 100 keys. An empty list sends no RPC. Two keys of one item fail the request with `batch_duplicate_key`. */
	items: { [K in keyof Ts]: BatchGetItemKey };
	/** The projection of each key that has no projection of its own. */
	projection?: readonly ProjectionExpression[];
	/** The most bytes of found items in the result (section 4.2.8). Defaults to 24 MiB, and is clamped to it. */
	maxResponseBytes?: number;
};

/** `code` is a FokosDB error code, or `response_too_large` when the response budget ran out. */
export type BatchUnprocessedCode = FokosErrorCode | "response_too_large";

export type BatchGetItemResult<T = never> =
	| ({ outcome: "found"; partitionIndex: number } & ReadItem<T>)
	| ({ outcome: "not_found"; partitionIndex: number } & ItemKey)
	| ({ outcome: "rejected"; code: FokosErrorCode; error_id: string; partitionIndex?: number } & ItemKey)
	| ({ outcome: "unprocessed"; code: BatchUnprocessedCode; error_id?: string; partitionIndex?: number } & ItemKey);

export type BatchGetItemsResult<Ts extends readonly unknown[] = never[]> = {
	/** `items[i]` answers `items[i]` of the request, with the type `Ts[i]`. */
	items: { [K in keyof Ts]: BatchGetItemResult<Ts[K]> };
	/** The request entries of the `unprocessed` results, in request order. */
	unprocessedKeys: BatchGetItemKey[];
	meta: FokosMeta;
};

export type BatchWriteItem =
	| { operation: "put"; hashKey: HashKey; sortKey?: SortKey; data: string | Uint8Array | JsonComposite; ttlAt?: number }
	| { operation: "delete"; hashKey: HashKey; sortKey?: SortKey };

export type BatchWriteItemsOptions = {
	/** 0 to 100 operations. An empty list sends no RPC. Two operations on one item fail the request with `batch_duplicate_key`. */
	items: BatchWriteItem[];
};

export type BatchWriteItemResult =
	| { outcome: "applied"; operation: "put"; version: number; partitionIndex: number }
	| { outcome: "applied"; operation: "delete"; deleted: boolean; partitionIndex: number }
	| { outcome: "rejected"; code: FokosErrorCode; error_id: string; partitionIndex?: number }
	| { outcome: "unprocessed"; code: FokosErrorCode; error_id: string; partitionIndex?: number }
	| { outcome: "unknown"; code: FokosErrorCode; error_id: string; partitionIndex?: number };

export type BatchWriteItemsResult = {
	/** `results[i]` answers `items[i]` of the request. */
	results: BatchWriteItemResult[];
	/** The request entries of the `unprocessed` and `unknown` results, in request order. */
	unprocessedItems: BatchWriteItem[];
	meta: FokosMeta;
};

export interface ItemBatcher {
	batchGetItems<Ts extends readonly unknown[] = never[]>(
		opts: NoInfer<BatchGetItemsOptions<Ts>>,
	): Promise<BatchGetItemsResult<Ts>>;
	batchWriteItems(opts: BatchWriteItemsOptions): Promise<BatchWriteItemsResult>;
}
```

- `partitionIndex` is the index of the partition of the item in `meta.partitions`. It is absent when no partition
  answered for the item: a transport failure, or a failure part with no routing.
- A read result carries the caller's own keys. A write result carries no key, as `TransactWriteOperationResult`.
- `meta` is `FokosMeta` with no `txCoordinator`.
- `Ts` of `batchGetItems` types each item by position, as `transactGetItems` does. A call that names no `Ts` keeps the
  widest types, and `NoInfer` keeps `Ts` off the inference path. `db.ts` erases the positional types inside the
  method, as `#transactGetItems` does: every item takes the same path, and only the caller knows the type of each
  position.
- `unprocessedKeys` has no positional type, because its length and its positions differ from the request.
- An entry of `unprocessedKeys` keeps its own projection. A caller that sends the entries again must also send the
  `projection` of the request.
- An `unprocessed` read result has no `error_id` only for `response_too_large`. Every other outcome with a code has an
  `error_id`.

#### 4.2.2 Validation and limits

`src/shared/transaction-limits.ts` gets `MAX_ITEMS_PER_BATCH = 100`, `validateBatchGetItems` and
`validateBatchWriteItems`. Each function encodes each key one time and returns the key bytes in request order, as
`validateTransactWriteOperations` does. A batch with no item is valid: the client method sends no RPC and answers an
empty result.

| Rule | Code |
| --- | --- |
| The batch has more than 100 items. | `batch_items_too_many` |
| Two items have the same key bytes (`KeyPairMap`). | `batch_duplicate_key` |
| A write is not `put` or `delete`. | `batch_operation_fields_invalid` |
| A put has no data. A delete has data or `ttlAt`. | `batch_operation_fields_invalid` |
| An item has `condition`, `update`, or `returnValuesOnConditionCheckFailure`. | `batch_operation_fields_invalid` |
| A key is not valid. | the codes of `validateItemKeys`, `encodeHashKey`, `encodeSortKey` |
| The data of a put is not valid or above 400 KB. | the codes of `encodeItemData` and `validateItemDataSize` |
| `ttlAt` is not valid. | `ttl_at_invalid` |
| `maxResponseBytes` is not a positive integer. | `batch_max_response_bytes_invalid` |
| A projection is not valid. | the codes of `validateProjectionExpression` |

The client does not cap the payload of a batch write. A root RPC above the RPC limit gives `rejected` with the new
code `batch_request_too_large` for each item of that root (section 4.2.4).

Each new code (`batch_items_too_many`, `batch_duplicate_key`, `batch_operation_fields_invalid`,
`batch_request_too_large`, `batch_max_response_bytes_invalid`) gets a new id in `VALIDATION_CODES` (`src/shared/errors.ts`).

#### 4.2.3 Wire types

The wire types live with the item RPC types in `src/server/do-partition.ts`. Keys are `KeyBytes`, and `sortKey` is
always present. No answer carries a key.

```ts
type BatchGetItemsRpcRequest = {
	items: Array<{ opIndex: number; hashKey: KeyBytes; sortKey: KeyBytes; projection?: readonly ProjectionExpression[] }>;
	projection?: readonly ProjectionExpression[];
	/** The `maxResponseBytes` of the batch (section 4.2.8). Every part gets the same value. */
	maxResponseBytes: number;
};

type BatchGetItemsRpcResponse = {
	found: Array<{ opIndex: number; item: GetItemRpcItem; bytes: number; partitionIndex: number }>;
	notFound: Array<{ opIndex: number; partitionIndex: number }>;
	failed: BatchFailure[];
	partitions: WirePartitionMeta[];
};

type BatchWriteRpcItem = { opIndex: number; hashKey: KeyBytes; sortKey: KeyBytes } & (
	| { operation: "put"; data: string | Uint8Array; kind: DataKind; ttlAt?: number }
	| { operation: "delete" }
);

type BatchWriteItemsRpcRequest = { items: BatchWriteRpcItem[] };

type BatchWriteItemsRpcResponse = {
	appliedPuts: Array<{ opIndex: number; version: number; partitionIndex: number }>;
	appliedDeletes: Array<{ opIndex: number; deleted: boolean; partitionIndex: number }>;
	failed: BatchFailure[];
	partitions: WirePartitionMeta[];
};

/** One error that stopped one or more items. */
type BatchFailure = {
	outcome: "rejected" | "unprocessed" | "unknown";
	code: BatchUnprocessedCode;
	error_id?: string;
	/** Absent when no partition answered. */
	partitionIndex?: number;
	opIndexes: number[];
};
```

- `GetItemRpcItem` is the `item` of the two found variants of `GetItemRpcResponse`. Both answers use the one alias.
- One failure part gives one `BatchFailure` for all its items. One error of one item gives one `BatchFailure` with one
  `opIndex`.
- An answering partition writes its entry with its counts. A remote failure part adds the entry of the partition that
  raised the error, from `routing.servedBy[0]`, with no counts. A remote failure part with no routing adds no entry. A
  local failure part adds the entry of this partition, with no counts.
- `merge` appends the lists of the parts in part order. It moves each `partitionIndex` by the number of entries of
  the parts before it (`withOffset`).

#### 4.2.4 Outcomes and their classification

One function in `src/shared/`, `classifyBatchFailure(error, { write, local })`, gives the outcome, the code, and the
`error_id` of an error. The partition and `db.ts` both use it. It wraps an error that is not a `FokosError` with
`FokosError.wrap`, which gives it the code `foreign_error` and an `error_id`. The rules apply in this order:

1. An error whose message, or the message of its cause, starts with
   `Serialized RPC arguments or return values are limited to 32MiB` is `rejected` with `batch_request_too_large`.
   workerd raises it for an RPC value above 32 MiB. The full message continues with the size of the value.
2. A `FokosValidationError` or a `FokosExpressionError` is `rejected`.
3. An error of the local part is `unprocessed`. A write handler throws only before its first write, a write block
   that throws rolls back its own writes, and the code after a committed block does not throw (section 4.2.10). A
   read applies nothing.
4. A `FokosError` that carries routing is `unprocessed`, except a `FokosInternalError` of a write. Partition code
   raised it before any handler ran on that part (section 4.2.13, invariant 4).
5. Any other error is `unknown` for a write and `unprocessed` for a read.

For rule 1, the function makes a `FokosValidationError` with `batch_request_too_large`, with the error of workerd as
its cause. The result carries its `error_id`. In a batch, the error comes from the arguments of a request: a batch
write answer is small, and the response limit holds a batch read answer under the RPC limit (section 4.2.8). The caller
serializes the arguments, so the request does not leave the caller and nothing of it applies. The RPC limit test of
section 4.2.17 checks this. Sending the same items to the same root fails the same way, so the outcome is `rejected`.
The caller can send the items again in smaller batches.

Rule 4 is stricter for a write than the rule of the single-shot fast path (section 1.3). That rule treats each error
with routing that is not `foreign_error` as "nothing applied". A `FokosInternalError` such as `invariant_failed` can
come from runtime code after a handler wrote. For a write, such an error is `unknown`, and `foreign_error` is a
`FokosInternalError` too. The caller can still send an `unknown` item again.

The response limit of a batch read gives `unprocessed` with `response_too_large` and no `error_id` (section 4.2.8).

#### 4.2.5 Runtime: the `collect` failure policy

`FokosOperation` in `src/sharding/runtime-types.ts` gets a third `group` variant. The `merge` of `collect` receives
failure parts, so the current operations keep their types.

```ts
export type FokosGroupFailure<Req> = { target: FokosPartitionRef | "local"; request: Req; error: unknown };

// shape: "group", failurePolicy: "fail_fast" | "attempt_all"
merge(parts: Array<FokosGroupPart<Req, Res>>): Res;
// shape: "group", failurePolicy: "collect"
merge(parts: Array<FokosGroupPart<Req, Res> | FokosGroupFailure<Req>>): Res;
```

Under `collect`, `#dispatchGroup`, `#runGroups` and `#forwardGroup` in `src/sharding/runtime.ts` change as follows:

1. The admission of the local entries runs inside the local part. A refusal makes the local part a failure part.
   `#admit` still requests the split evaluation before it throws.
2. A throw from the local handler makes the local part a failure part.
3. `#forwardGroup` runs the miss ladder as today. When the forward still fails, the group becomes one failure part,
   with the sub-request that it sent.
4. `#forwardTo` merges the routing of a failed forward into the collector as evidence. It does not call `addRaiser`,
   because the envelope is a success, and it does not replace the routing of the error. Thus `merge` reads the raiser
   from `servedBy[0]` of the error.
5. `dispatch` returns `merge(parts)` in the envelope. It never throws `partition_fanout_failed`.

A part that fails does not stop the other parts. The empty request (no item) runs the local handler, as today.

#### 4.2.6 Runtime: read-through for a `group` operation

A `group` operation with `readOnly: true` can declare `whileMigrating: "read_source"`. `#validateOperations` accepts
it.

On the importing target, `#whileImporting` does not change: it checks each key with `#ownsByTopology`, lists itself
as `read_through`, and sends the whole request to the source with `fokosExecuteLocal`.

On the source, `#executeLocal` gets a `group` branch:

1. Each key must be inside the caller slice (`sliceIncludesItem`), or the source throws `partition_misrouted`.
2. When the slice is `hash_child`, each key with a terminal route override goes to its range owner (`#rangeOwner`
   with `"override"`). The source groups these keys by target and forwards one sub-request (`subRequest`) to each
   with `#forwardGroup`.
3. When at least one other key remains, the other keys run the local handler in one sub-request.
4. `merge` joins the parts. The failure policy of the operation applies to the forwards, so under `collect` a failed
   forward becomes a failure part.

This is the `point` branch of `#executeLocal`, applied to each key of the request.

#### 4.2.7 `apiBatchGetItems`

`PartitionDO` gets the RPC method `apiBatchGetItems(ctx, req)`, which is one `#api` call, and the entry in
`PartitionOps`.

| Field | Value |
| --- | --- |
| `shape` | `group` |
| `whileMigrating` | `read_source` (`throw` in Milestone 2) |
| `readOnly` | `true` |
| `admissionTag` | `read` |
| `failurePolicy` | `collect` |
| `items` | each item, with its key |
| `subRequest` | the items of the part |
| `merge` | joins the parts, then applies the response limit (section 4.2.8) |

The local handler is synchronous, so it reads its keys as one snapshot of the partition. For each item, in request
order:

1. It reads the item with the body of `readItemLocally`, with the projection of section 4.2.9.
2. An absent item goes into `notFound`. It costs no response bytes.
3. A present item goes into `found`, with its estimated `bytes`. The estimate uses the rules of `estimateItemBytes` or
   `estimateProjectedRowBytes`, with the keys of the request. The handler does not apply the response limit: `merge`
   does (section 4.2.8).
4. An error of the item goes into `failed`, with the outcome of section 4.2.4.

The handler writes one entry for its partition, with the summed `rowsRead` and `rowsWritten`.

#### 4.2.8 The response limit of a batch read

One batch read answer holds at most `maxResponseBytes` of found items, in the estimate of each item.
`batchGetItems` takes `maxResponseBytes` as an option, as `queryItems` does. It must be a positive integer, or the call
throws `batch_max_response_bytes_invalid`. It defaults to `MAX_BATCH_GET_RESPONSE_BYTES`, and `fitBatchResponse` clamps
it to that constant on each node. The constant is in `src/shared/transaction-limits.ts`, and it is 75% of
`WORKERS_RPC_MAX_BYTES` (24 MiB). The client sends the same value to every call, with no split.

`fitBatchResponse` in `src/shared/batch-response.ts` applies the limit. It keeps the found items in `opIndex` order
while the sum of their bytes stays at most the limit. An item that does not fit does not stop a smaller item after
it. The item of `opIndex` 0 always stays. Each item that it drops becomes `unprocessed` with `response_too_large`,
with the `partitionIndex` of the partition that read it.

Two places apply the limit:

- The `merge` of `apiBatchGetItems`, on each node, after it joins its parts. `merge` runs on every node under
  `collect`, so no answer that crosses an RPC goes above the limit.
- `db.ts`, after it joins the answers of its calls. Each answer fits in the limit, but the answers of several calls
  together can go above it.

Thus:

- One answer is at most 24 MiB plus one item. One item is at most about 1 MB in the estimate: a stored row of 400 KB
  that the estimate counts at 2 bytes per character. That stays below the 32 MiB RPC limit.
- The limit never stops the key of `opIndex` 0. A caller that sends the `unprocessedKeys` again therefore makes
  progress, because the first unprocessed key becomes `opIndex` 0 of the next call. A failure of the part of
  `opIndex` 0 is not a stop of the limit, and it gives the outcome of section 4.2.4.
- A node can read items that it then drops. That happens only when a batch holds more than 24 MiB of found items. The
  100 keys of a batch hold at most about 40 MB, so a node never holds more than that in memory.

#### 4.2.9 Projections

- The projection of an item replaces the projection of the request. An item with neither reads the complete item.
- `db.ts` validates each projection one time and keeps its output names. The names of item `i` come from its own
  projection, or from the projection of the request.
- The request carries the request projection one time, and an item carries its own projection only when it has one.
- `projectionPlanOf` keeps the plan of a tree in a `WeakMap`, as `updatePlanOf` does. Thus a partition compiles the
  request projection one time per request, and each item projection one time. The map holds the tree weakly, so the
  plan lives as long as the request.

#### 4.2.10 `apiBatchWriteItems`

`PartitionDO` gets the RPC method `apiBatchWriteItems(ctx, req)`, which is one `#api` call, and the entry in
`PartitionOps`.

| Field | Value |
| --- | --- |
| `shape` | `group` |
| `whileMigrating` | `throw` |
| `admissionTag` | `batch_write`, which the `admit` hook allows |
| `failurePolicy` | `collect` |
| `items` | each item, with its key |
| `subRequest` | the items of the part |

`putItemLocal`, `deleteItemLocal` and the batch handler share one function for each step: the lock check, the upsert
with `txOrderTimestampNow()`, and the delete. The `admit` hook and the batch handler share one function for the size
check.

The local handler:

1. When the part has at least one put, runs the size check one time, before any write. When the partition is above
   1.1 times its cap, all puts go into one `failed` entry as `unprocessed` with `partition_over_size`, and the handler
   signals `evaluateSplit`. Each delete still applies.
2. Runs each other item, in request order, in its own `transactionSync` block:
   - A locked item throws `item_locked_by_transaction`, and the block writes nothing.
   - A put calls the upsert, with `ttlAt`. The store can throw `item_too_large`.
   - A delete calls the delete.
3. Catches the error of each block and adds a `failed` entry with the outcome of section 4.2.4. The next item runs.
   The catch covers the block only. After a block commits, the handler only appends the result of the item to its
   answer lists, and that code does not throw.
4. When at least one put applied, calls `signalGrowth` one time with the promotion candidates of the applied puts, as
   `txCommit` and `txExecuteSingleShot` do.

Thus the handler never throws after its first write, and an error that it catches belongs to a block that rolled
back. All blocks run in one synchronous call, so no other request runs between two items.

#### 4.2.11 The client

`batchGetItems` and `batchWriteItems` in `src/client/db.ts`:

1. Validate the batch (section 4.2.2). Check the operation fields before `encodeItemData`, so a put with no `data`
   throws `batch_operation_fields_invalid`. Encode each put with `encodeItemData`. Give each item `opIndex = i`.
2. Group the items by root with `resolveAll`.
3. Send one RPC to each root with `send`, all at the same time, with no retry. A batch read then applies the response
   limit to the joined found items (section 4.2.8).
4. Join the answers with `PartitionMetaJoin`, and move each `partitionIndex` to the joined list, as
   `#readTransaction` does. Add the `forwardCount` of each answer.
5. Classify a failed root RPC with section 4.2.4. When the error carries routing, add `routing.forwardCount` to
   `forwardCount`, and add `routeNodeMeta(raiser)` to `PartitionMetaJoin` to get the `partitionIndex` of the items of
   that root. A root RPC above the RPC limit gives each of its items `rejected` with `batch_request_too_large`, and
   the other roots still apply their items.
6. Build `results[i]` from the item with `opIndex` `i`, and the unprocessed list from the request entries.

A partition can report an internal code that a client does not know. `mapInternalErrorToPublic` in `db.ts` maps such a
code on an error today: `repartition_not_cut_over` becomes `partition_migrating`. `db.ts` applies the same mapping to
the code of each result.

Each `opIndex` must get one outcome. A missing outcome is a defect: `db.ts` logs it and gives the item `unknown`
(write) or `unprocessed` (read) with `invariant_failed`. An error that `db.ts` raises while it decodes one found item
becomes the `unprocessed` outcome of that item. No error leaves the method after I/O starts.

#### 4.2.12 `FokosStd` retry helpers

```ts
class FokosStd {
	static batchGetItemsWithRetry<Ts extends readonly unknown[] = never[]>(
		db: ItemBatcher,
		opts: NoInfer<BatchGetItemsOptions<Ts>>,
		retry?: FokosDBRetryOptions,
	): Promise<BatchGetItemsResult<Ts>>;
	static batchWriteItemsWithRetry(
		db: ItemBatcher,
		opts: BatchWriteItemsOptions,
		retry?: FokosDBRetryOptions,
	): Promise<BatchWriteItemsResult>;
}
```

Each helper:

1. Validates `retry` with the rules of `validateRetryOptions` (`fokosdb_options_invalid`) before any I/O.
2. Calls the method with every item.
3. While the unprocessed list is not empty and the calls are fewer than `maxAttempts`: waits
   `jitterBackoff(retryIndex, baseDelayMs, maxDelayMs)` milliseconds, where `retryIndex` starts at `0` for the first
   retry, then calls the method with the unprocessed list. A batch read sends the request projection again.
4. Writes each answer into the positions of the first request. In `batchWriteItemsWithRetry`, an item that got
   `unknown` on an earlier call keeps `unknown` when a later call does not return `applied`. It joins the `meta` of
   each call with `PartitionMetaJoin`, moves each `partitionIndex`, and adds the `forwardCount` of each call.
5. Returns the result in the shape of the method. The entries that stay unprocessed after the last call stay in the
   unprocessed list.

The defaults are the defaults of `FokosDBRetryOptions`: 100 ms, 2,000 ms, and 5 attempts. A helper never sends a
`rejected` item again. The validation error of the first call throws before any I/O. The class comment of `FokosStd`
changes: the retry helpers send requests only through the `ItemBatcher` they get.

A helper takes the limits of its method: 1 to 100 items, and the method validates them. A helper makes one call at a
time and does not split the items into more than one call. Each later call holds at most the items of the call
before it. The result keeps the positions of the first request, so `Ts` of the first request types it.

#### 4.2.13 Invariants

1. Each `opIndex` of a request gets exactly one outcome.
2. `applied` means the item committed. `unprocessed` and `rejected` mean nothing of the item applied. `unknown` means
   the item can have applied.
3. A batch write handler never throws after its first write (section 4.2.10).
4. Under `collect`, an error from a target means that no handler ran on the target or below it. The local part and
   every remote part become parts, and `merge` must not throw, so a target that ran a handler answers with a value.
   Thus the miss ladder never sends a key again to an owner that applied it. A defect can break this invariant, so
   the classification treats a `FokosInternalError` of a write as `unknown` (section 4.2.4).
5. A batch write changes `v`, `max_deleted_v`, `max_delete_tx_order_ts` and the item timestamps with the functions of
   `putItem` and `deleteItem`.
6. One batch read answer is at most 24 MiB plus one item (section 4.2.8).
7. A partition refuses a batch write to a locked item at once. It does not wait for the lock.
8. No partition answer carries a key.

#### 4.2.14 Transactions, splits, promotions, and TTL

- **Transactions.** A batch write refuses a locked item (`unprocessed`, `item_locked_by_transaction`), as `putItem`
  does. Each applied item changes `v`, so a read transaction that overlaps it gets `read_conflict`. A batch read
  returns the committed state and does not show a pending write, as `getItem` does.
- **Splits.** The resolution of the owners and the local work run in one synchronous block, so a cutover cannot fall
  between them. A batch write on an importing partition gets `unprocessed` with `partition_migrating`, as `putItem`
  gets the error. A batch read reads through (section 4.2.6).
- **Promotions.** The `group` resolution sends each key of a promoted hash key to its range tree, as it does for
  `txPrepare`.
- **One call per owner.** No batch operation compares a keyset, so the Bloom speculation guard is harmless here.
- **TTL.** A batch read can return an expired item until the sweep deletes it, as `getItem` can.

#### 4.2.15 Performance

- **RPCs.** The Worker sends at most `min(rootTreesN, n)` RPCs, all at the same time, where 100 single-item calls send
  100. Each root sends one forward to each remote owner of its keys, all at the same time. The latency is one Worker
  round trip plus the forwards of the slowest root. A root reaches an owner in one forward when its route cache knows
  the owner, as for every `group` operation. TODO: measure the latency.
- **Storage.** A read runs one statement per key. A write runs one upsert or one delete per key. The rows read and
  written are the rows of the single-item operations.
- **Compile.** A partition compiles each distinct projection tree one time per request (section 4.2.9).
- **Memory.** A node holds the found items of its parts before `merge` trims them, at most about 40 MB for 100 keys
  of 400 KB. The answer that it sends holds at most 24 MiB plus one item.

#### 4.2.16 Deployment

- There is no schema change and no data migration.
- The two operations are new RPC methods. Deploy the Worker that defines `PartitionDO` before a client that calls
  them.
- During a gradual deployment, a part can reach a partition of the old version. That part fails, and its items come
  back `unprocessed` or `unknown`.
- To roll back, remove the client calls first, then the operations.

#### 4.2.17 Testing

- **`collect`.** The counter host of `test/sharding/` gets a `group` operation with `collect`. The tests cover a local
  admission refusal, a local throw, a remote failure after the miss ladder, and a request with no failure. They check
  that `dispatch` returns a value and that the head of the routing is not the raiser.
- **Read-through.** `test/partition-do/read-through.test.ts` covers a batch read on an importing child, and a promoted
  key that the source forwards to its range root. It uses `TestPartition`.
- **Batch read.** A new `test/partition-do/batch-get-items.test.ts` covers found and absent items, the precedence of
  the item projection, `response_too_large` above `maxResponseBytes`, the trim at a router after a hash split, a promotion, and
  `partitionIndex`. `src/shared/batch-response.test.ts` covers the rules of `fitBatchResponse`. Client tests cover the
  validation codes, the positional results, `unprocessedKeys`, the limit on the joined answers of several calls, and
  a failed root RPC (`test/controlled-partition-do.ts`).
- **Batch write.** A new `test/partition-do/batch-write-items.test.ts` covers puts and deletes, `version` and
  `deleted`, a locked item, an over-size partition (puts refused, deletes applied, split evaluation), `item_too_large`,
  an importing child, a transport fault (`unknown`), a `FokosInternalError` from a remote part (`unknown`), and that
  the method never throws after I/O.
- **Transactions.** A read transaction that overlaps a batch write gets `read_conflict`. A batch write to a locked key
  gets `item_locked_by_transaction`.
- **Helpers.** The `FokosStd` tests cover the loop until no entry stays, `maxAttempts`, `fokosdb_options_invalid`
  before any I/O, no resend of a `rejected` item, an `unknown` write that a later retry does not apply, and the
  joined `meta`.
- **Property-based.** The item model of `test/property-based/harness.ts` gets batch commands. The model applies each
  `applied` result and leaves each other item unchanged.
- **RPC limit.** A test sends a batch write above 32 MiB to one root: 100 puts of 400,000 ASCII characters under one
  hash key. Each item of that root must come back `rejected` with `batch_request_too_large`, and a read of each key
  must find nothing of the batch. A second test checks that `FokosStd.batchWriteItemsWithRetry` makes one call only.
- **Types.** `src/client/db.test.ts` has type tests for `transactGetItems<[...]>`. The same tests for `batchGetItems`
  check that `batchGetItems<[A, B]>` types `items[0]` with `A` and `items[1]` with `B`, that a request with the wrong
  number of items does not compile, and that a call with no `Ts` keeps the widest types.

## 5. Alternative Options

### 5.1 `attempt_all` and a client fallback to single-item calls

The root throws `partition_fanout_failed`, and the client sends each item of that root again as a single-item call.
Rejected: `attempt_all` drops the results of the parts that applied, so the client sends applied writes again.

### 5.2 One `transactionSync` per leaf

The GSI plan applies all items of one leaf in one `transactionSync`. Rejected: a locked item or an `item_too_large`
item rolls back every other item of the leaf. One block per item gives each item its own outcome, and all blocks
still run in one synchronous call.

### 5.3 One single-item RPC per key from the Worker

Rejected: 100 Worker-to-partition RPCs for 100 items, where the `group` shape sends at most one RPC per root.

### 5.4 Wire answers in request order, with the keys

Rejected: each key can be 1,024 bytes of hash key and 512 bytes of sort key by default. The client already has the
keys, so the answer names each item by `opIndex`.

### 5.5 Runtime admission of the whole local part

The operation keeps the admission tag `write`, and the runtime refuses the whole local part above the cap. Rejected:
a partition over its cap then refuses the deletes of a batch, while `deleteItem` passes.

### 5.6 No read-through, and a client fallback to `apiGetItem`

The client reads each `partition_migrating` key again with a point read, which reads through. Rejected: it costs one
Worker RPC for each key of an importing partition, and it adds a second read path to `db.ts`.

### 5.7 Other response limits for a batch read

**A share for each part, by key count.** Each node splits its budget over its parts by key count, and each part reads only up to its share. A node then never
reads an item that it drops. Rejected: items can have very different sizes. A part with 3 items of 400 KB next to a
part with 50 small items gets 3/53 of the budget, so it stops 2 of its items while the whole answer is far below the
limit.

**No limit for a batch of one call.** A batch with one call has no other answer to join. Rejected: 100 items of 400 KB are about 40 MB, which is above the
32 MiB RPC limit. The call then fails for all of its keys, and the same keys fail the same way again, so the caller
never makes progress. One call can also go through a router to several partitions.

### 5.8 A payload cap on a batch write

A cap such as 16 MiB refuses a large batch before any I/O. Rejected: the RPC limit already stops a request above
32 MiB, and the client gives each item of that root `rejected` with `batch_request_too_large` (section 4.2.4).

### 5.9 The DynamoDB result shape

DynamoDB returns the found items in no order and leaves out the absent items. Rejected: the caller then matches each
item to its key. `transactGetItems` already answers in request order.

### 5.10 One type `T` for every item of a batch read

One `T` fits a batch of items of one kind, as `queryItems` types its items. Rejected: a batch reads unrelated items,
and `transactGetItems` already types each item by position with `Ts`.

### 5.11 Chunking and parallel calls in the `FokosStd` helpers

A helper can accept more than 100 items, split them into calls of 100, and send the calls at the same time. Not now:
the helpers take the limits of their methods. A later change can add both.

## 6. Frequently Asked Questions

### 6.1 Why does `batchWriteItems` never throw?

The items of one batch apply on different partitions. An error for the whole call cannot say which items applied.
Each result says it for its own item.

### 6.2 Is a batch read a snapshot?

No. A handler reads the items of its partition in one synchronous block, so they are one snapshot of that partition.
Two partitions read their items at two different times. `transactGetItems` gives a snapshot across partitions.

### 6.3 Can one item apply two times?

Not inside one call: neither `db.ts` nor the runtime sends a write again, except the miss ladder, which section
4.2.13 (invariant 4) covers. A caller that sends an `unknown` item again can apply it a second time. Then `v`
increments again, and the second apply overwrites a write that another client made between the two.

### 6.4 Why does a delete apply while the partition is over its cap?

A delete is how a client brings an over-size partition back under its cap. `deleteItem` passes for the same reason.

### 6.5 Why does the projection of an item replace the projection of the request?

One batch can read items of different shapes. The request projection covers the common shape, and an item projection
covers an exception.

### 6.6 What changes in the GSI plan?

Its Milestone 3 uses `batchWriteItems` from this spec. It names `FokosShardingClient.group`, which does not exist:
`db.ts` uses `resolveAll` and `send`. It applies a leaf in one `transactionSync`, and this spec uses one block per
item (section 5.2).

## 7. References

- `docs/agent-plans/2026-10-17-coalesced-materialization-and-gsis.md`
- `docs/agent-plans/2026-10-16-unified-operation-meta.md`
- `docs/agent-plans/2026-09-27-learned-routes-for-every-dispatch-shape.md`
- `docs/agent-plans/2026-09-14-read-projections-and-query-filters.md`
- [Amazon DynamoDB API Reference: BatchGetItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html)
- [Amazon DynamoDB API Reference: BatchWriteItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html)
- [Cloudflare Workers: Remote-procedure call (RPC)](https://developers.cloudflare.com/workers/runtime-apis/rpc/)
- [Cloudflare Workers: Limits](https://developers.cloudflare.com/workers/platform/limits/)
