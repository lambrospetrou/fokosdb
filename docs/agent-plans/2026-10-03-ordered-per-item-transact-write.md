# RFC — Ordered per-item execution for transactWriteItems

**State:** Draft
**Date:** 2026-10-03
**Author:** Lambros
**Status:** Nothing is built. The design decisions in section 4 are agreed.

## Table of contents

- [1. Overview and context](#1-overview-and-context)
  - [1.1 The current write transaction](#11-the-current-write-transaction)
  - [1.2 The problem](#12-the-problem)
  - [1.3 Glossary](#13-glossary)
- [2. Goals and requirements](#2-goals-and-requirements)
- [3. Milestones](#3-milestones)
- [4. Proposed solution](#4-proposed-solution)
  - [4.1 High-level overview](#41-high-level-overview)
  - [4.2 Technical details](#42-technical-details)
- [5. Alternative options](#5-alternative-options)
- [6. Frequently asked questions](#6-frequently-asked-questions)
- [7. References](#7-references)

## 1. Overview and context

### 1.1 The current write transaction

`FokosDB.transactWriteItems` applies up to `MAX_ITEMS_PER_TX` (100) operations atomically. Each operation is a
`put`, a `delete`, a `check`, or an `update`. Each operation has an optional condition.

The current behavior has these properties:

- `validateTransactWriteOperations` rejects two operations on the same `(hashKey, sortKey)` pair with
  `transact_duplicate_key`.
- `db.ts` gives each operation its request position as `opIndex`. Every result carries `opIndex` back, so each node
  merges results by request order.
- A cancelled transaction raises `FokosTransactionCancelledError`. Its `results[i]` answers request operation `i`,
  with the outcome `passed`, `rejected`, or `not_evaluated`.
- Two execution paths exist. The single-partition path sends the whole set to one partition through
  `txExecuteSingleShot`. The two-phase path sends the set to a `TransactionCoordinatorDO`, which drives 2PC.
- The coordinator stores one `tc_items` row for each operation, with `PRIMARY KEY (transaction_id, hk, sk)`.
- A participant stores one `pending_transactions` row for each key. The row holds the effect that commit applies:
  the operation, the data, the kind, and the TTL. Prepare materializes an update into its new document.
- SQLite evaluates each condition and each update against the committed `items` row.

### 1.2 The problem

A caller cannot send two operations for the same item in one transaction. For example, a caller cannot check an item,
then update it, then check the result, all in one atomic request.

### 1.3 Glossary

- **Item** — one `(hashKey, sortKey)` pair. A missing `sortKey` is the empty sort key. Two keys are the same item
  only when their encoded bytes are equal (section 4.2.4.2).
- **Operation** — one entry of the request `items` array. Its `opIndex` is its position in that array.
- **Sequence** — the operations of one item, in `opIndex` order.
- **Committed state** — the `items` row of an item before the transaction starts.
- **Private state** — the state of an item after the earlier operations of its sequence. Only the transaction that
  makes it can see it.
- **Final image** — the effect of the whole sequence of an item, as one lock row holds it: the final data or "absent",
  and the number of writes (section 4.2.6).
- **Single-op key** — an item with one operation in the request. **Multi-op key** — an item with two or more.
- **Evaluation block** and **lock block** — the two `transactionSync` calls of a prepare (section 4.2.5).

## 2. Goals and requirements

### 2.1 In scope

1. `transactWriteItems` accepts `executionMode: "standard" | "ordered_per_item"`. The default is `"standard"`.
2. Standard mode keeps its current behavior. It continues to reject repeated items with `transact_duplicate_key`.
3. In `"ordered_per_item"` mode, a request can contain more than one operation for the same item.
4. The operations of each item run in `opIndex` order. Each operation sees the effects of the earlier operations of
   its item.
5. For each operation, the partition evaluates the condition and its own checks first, and then applies the
   operation.
6. Each operation acts as a standalone write. Each write increments `v`. A delete of the committed row increments
   `delete_revision`. A put after a delete creates a new row with `v = 1`. `delete_revision` and
   `max_delete_tx_order_ts` change by the net effect of the sequence, on both paths (section 4.2.6.1).
7. All operations of one transaction use the same transaction timestamp.
8. The first failure of an item is the only failure that the item reports. The later operations of that item get
   `not_evaluated`. The other items continue, so that the result can report their outcomes.
9. One failure cancels the whole transaction. Continued evaluation collects diagnostics. It does not permit a partial
   commit.
10. The result type stays the same: one result for each `opIndex`, as section 4.2.3 defines.
11. A condition failure image shows the state immediately before the failed operation, private changes included.
12. Both execution paths support ordered mode: the single-partition path and the two-phase path.
13. The coordinator uses `PRIMARY KEY (transaction_id, op_index)` for `tc_items` in both modes. A schema migration
    changes existing databases atomically, and it keeps every `op_index`.

### 2.2 Requirements

- The private state of a transaction must be invisible to every other request.
- Prepare must persist the accepted final image of each item. Commit must not evaluate a condition again.
- The sequence must keep the rules for versions, TTL, timestamps, deletion metadata, and item size.
- A participant must hold one lock for each item.
- Every node must agree which operations belong to one item. Each node compares the key bytes, and no hash value
  decides that two keys are the same item.
- Commit and cancel must send each item key one time.
- The original `opIndex` of each operation must stay the same through routing, retries, recovery, and migration.
- The idempotency fingerprint must include the execution mode.
- The current request validation and limits must stay. `MAX_ITEMS_PER_TX` (100) and `MAX_PAYLOAD_BYTES_PER_TX` (4 MB)
  count operations, not unique items.
- The ordering guarantee applies to each item. Different partitions can run in parallel.

### 2.3 Out of scope

- **A global order across items or partitions.** The guarantee applies to each item only.
- **The `clientRequestToken` rule of the single-partition path.** A request with a token skips that path in both
  modes. A later change will look at that rule again.
- **A mixed-version deployment.** The schema and the API can change in a way that breaks old code. Section 4.2.15
  gives the deployment rules.
- **A separate public method.** Section 5 records the reason.

## 3. Milestones

Each milestone ships alone. Milestones 1 to 3 keep the behavior of standard mode, with one exception: after
milestone 2, two different keys with the same hash are two items, and the client no longer rejects them as a
duplicate. Milestone 4 makes ordered mode available.

1. **Coordinator schema.** Change `tc_items` to `PRIMARY KEY (transaction_id, op_index)` with a schema migration.
   Send each key one time in commit and cancel. Section 4.2.9.
2. **Final image in the lock row.** Add `version_delta` to `pending_transactions`, the `replace` operation, and the
   `PendingLockOperation` type. Make commit apply them with an exhaustive `switch`, add the store method that deletes
   the row of a `replace` and changes only `delete_revision`, and make `commitLocal` refuse a duplicate key. Make the
   migration stream carry the new column. Add `KeyPairMap` and use it at each identity site in place of
   `KeyCodec.pairKey`. Sections 4.2.4.2 and 4.2.6 to 4.2.8.
3. **The sequence engine.** Make `prepareLocal` and `executeSingleShot` use one function that groups operations by
   item and runs each sequence. Read `deletion_metadata` before the loop. Apply the timestamp rule, the result rules,
   the final images, and the delete effect on the single-partition path. Sections 4.2.3 to 4.2.6.1.
4. **Public API.** Add `executionMode`, validate its value, skip the duplicate check in ordered mode, add the mode to
   the fingerprint, expose the mode in the HTTP example, and update the public documentation. Sections 4.2.1, 4.2.2,
   and 4.2.9.
5. **Cross-path tests.** Add the tests of section 4.2.16 that cover recovery, migration, and idempotent retries in
   ordered mode.

## 4. Proposed solution

### 4.1 High-level overview

The client accepts repeated items when the caller selects `"ordered_per_item"`. Nothing below the client reads the
mode. The coordinator and the partition always run each item as a sequence. A standard request is a set of sequences
that each have one operation, so standard mode keeps its current behavior.

The partition groups the operations of a request by item and sorts each group by `opIndex`. For each operation, it
checks the lock, evaluates the condition, runs the update and size checks, and checks the timestamp. When another
operation of the same item follows, the partition applies the operation to `items` as a private write. The last
operation of an item makes no private write.

A prepare uses two `transactionSync` calls with no `await` between them:

1. The evaluation block runs every sequence and builds the final image of each multi-op key. Then it throws, and
   SQLite rolls back every private write.
2. When every operation passed, the lock block writes one lock row for each item, with its final image.

No other request can run between the two blocks, so no request sees the private state. Commit writes the final image
of each lock row and evaluates nothing.

The single-partition path uses the same sequence engine in one `transactionSync`. When every operation passed, it
applies the last operation of each item and keeps all writes. When one operation failed, it throws to roll back.

Only a multi-op key makes private writes, and an item with *n* operations makes *n − 1* of them. A transaction with no
repeated item, in either mode, costs the same as a standard transaction costs now.

The coordinator stores one `tc_items` row for each operation, keyed by `opIndex`. It sends all operations of an item to
the participant in one prepare. It sends each key one time in commit and cancel.

```text
db.ts ── validate (repeated items only in ordered mode), fingerprint includes the mode
  │
  ├── single-partition path ──► partition: one transactionSync
  │                               sequence engine: check → apply, per operation
  │                               all passed → keep writes │ one failed → throw, roll back
  │
  └── two-phase path ──► coordinator: tc_items (transaction_id, op_index)
                           │ prepare: all operations of each item, sorted by opIndex
                           ▼
                         partition prepare
                           evaluation block: sequence engine, read final images, throw → roll back
                           lock block: one lock row per item, with its final image
                           │
                           ▼ commit / cancel: unique keys
                         partition commit: write the final image, evaluate nothing
```

### 4.2 Technical details

#### 4.2.1 Public API

`TransactWriteItemsOptions` in `shared/transaction-api-types.ts` gets one field:

```ts
export type TransactWriteItemsOptions = {
	items: TransactWriteItem[];
	clientRequestToken?: string;
	/** Defaults to "standard". "ordered_per_item" accepts more than one operation for the same item. */
	executionMode?: "standard" | "ordered_per_item";
};
```

`TransactWriteItemsResult`, `TransactWriteOperationResult`, and `FokosTransactionCancelledError` do not change.

The HTTP example exposes the mode. `TransactWriteItemsBodySchema` in `examples/http-api/src/rpc/schemas.ts` gets
`executionMode` as an optional field with the two values. The `transactWriteItems` route in
`examples/http-api/src/rpc/routes.ts` already passes the parsed body to `FokosDB.transactWriteItems`.

These documentation comments change:

- `TransactWriteOperationResult` — `not_evaluated` also means that an earlier operation of the same item failed.
- `returnValuesOnConditionCheckFailure` — in ordered mode, the image can show a private state. The transaction
  cancelled, so that state was never committed.

#### 4.2.2 Validation and limits

`db.ts` validates `executionMode` before any other check of the request. A value other than `"standard"` or
`"ordered_per_item"` fails with a `FokosValidationError` with the new code `transact_execution_mode_invalid` in
`VALIDATION_CODES`. An absent value is `"standard"`. The check runs on both paths, before the request leaves the
client, so no node below the client sees a mode that is not valid.

`validateTransactWriteOperations` in `shared/transaction-limits.ts` gets the execution mode. In ordered mode, it skips
the `transact_duplicate_key` check. In standard mode, the check finds a duplicate with `KeyPairMap`, so it rejects two
operations only when their key bytes are equal (section 4.2.4.2). Every other check stays the same for both modes:

- The key validation and the canonical key bytes.
- `MAX_ITEMS_PER_TX` (100), counted over operations.
- `MAX_PAYLOAD_BYTES_PER_TX` (4 MB), summed over the data of all operations.
- `MAX_ITEM_BYTES` (400 KB) for the data of each put.
- The rules for each operation type: data, condition, and update plan.

`db.ts` keeps `opIndex = i` for request position `i`. `InitiateWriteRequest` gets `executionMode`, which the
coordinator uses only for the fingerprint. `SingleShotRequest` and `PrepareRequest` do not carry the mode.

#### 4.2.3 Result rules

Every operation gets one result, at the position of its `opIndex`.

- **Every operation of the item passed.** Each operation gets `passed`.
- **Operation *k* failed a check of its own.** The operations before *k* get `passed`. Operation *k* gets `rejected`
  with its reason. The operations after *k* get `not_evaluated`.
- **Another transaction holds the lock of the item.** The first operation gets `rejected` with `pending_conflict`.
  The later operations get `not_evaluated`.
- **The partition refused the whole request**, for example with `clock_skew`. Every operation of that partition gets
  the code, as now.
- **The partition failed with an execution error.** Every operation of that partition gets the code and the
  `error_id`, as now.
- **This transaction already holds the lock of the item** (a repeated prepare). Each operation gets `passed`.

The checks of one operation run in the current order: the lock, the condition, `#precheckWrite`, then the timestamp.
The first check that fails gives the reason.

`applyImageCap` already sorts by `opIndex` before it caps the image bytes. The grouping by item does not change which
images it drops.

#### 4.2.4 The sequence engine

One function in `TransactionParticipant` serves `prepareLocal` and `executeSingleShot`. The function does these steps:

1. Group the operations by item with `KeyPairMap` (section 4.2.4.2). Sort each group by `opIndex`. The order of the
   request array does not matter.
2. Read the committed `deletion_metadata` row one time, before the first private write. The two-phase path uses
   `max_delete_tx_order_ts` for the timestamp check (section 4.2.4.1). The single-partition path reads the row only
   when the request has a multi-op key, and uses both values for its last write (section 4.2.5).
3. For each item, check the lock one time, against the committed state.
4. For each operation of the item, in order:
   1. Evaluate the condition against `items`. On a failure, read the image with `#imageForFailedCondition`.
   2. Run `#precheckWrite` against `items`.
   3. On the two-phase path, check the timestamp against the committed stamps (section 4.2.4.1).
   4. When a check fails, record the failure, mark the later operations of the item `not_evaluated`, and continue
      with the next item.
   5. When the operation passed and another operation of the same item follows, apply the operation to `items` as a
      private write.
5. Return one result for each operation. When every operation passed, also return the lock operation of each item
   (section 4.2.6). On the two-phase path, then also build the final image of each multi-op key. The private rows are
   still in `items` at this point, because the block has not rolled back yet.

Step 4.5 uses the current store writes: `upsertItem`, `updateItemSingleShot`, `deleteItem` with
`bumpTxOrderTsAlways`, and `bumpItemReadTs`. Each write uses the transaction timestamp. Both callers make the same
private writes. The last operation of an item makes no private write, and so the only operation of a single-op key
makes none. A transaction with no repeated item therefore makes no private write, and a rejected one pays only the
reads of its checks, as now.

At step 4.1 and step 4.2 of operation *k*, `items` holds the state that operations 1 to *k − 1* left. For a single-op
key, that is the committed state, so the result is the same as now.

Every switch over an operation type is exhaustive. The engine, the final image, and the commit apply each handle every
member of the type, and the compiler rejects a missing member. A value that the code reads from storage is not typed
by the compiler, so the code checks it with `invariant()` before it uses it.

The store writes collect promotion candidates. The engine discards the candidates of a write that rolls back.

##### 4.2.4.1 The timestamp rule

The timestamp check uses only the committed state from before the transaction. Conditions, update checks, and size
checks use the private state.

The reason: all operations of a transaction use one timestamp `T`. A check against the private state rejects valid
sequences:

- `put → check`: the put sets `last_read_ts = last_write_ts = T`. The check then finds `T <= T`.
- `delete → put`: the delete sets `max_delete_tx_order_ts = T`. The put of the absent item then finds `T <= T`.

The committed values come from two places, and the engine reads each of them before a private write can change it:

- **`max_delete_tx_order_ts`** — `deletion_metadata` holds one value for the whole partition (`id = 1`). A private
  delete of any item changes it. The engine therefore reads it one time, at the start of the evaluation block and
  before it processes the first item, and uses that value for every item.
- **The stamps of an item** — `last_read_ts` and `last_write_ts` belong to the row of the item. Only the operations of
  that item write the row. The first operation of the item reads them, as now, and the engine keeps them for the later
  operations of the item.

Without the first rule, a private delete of one item makes a later absent item fail. An example: item A, which exists,
has `delete → put`, and item B, which is absent, has `put`. The private delete of A sets the watermark to `T`, and the
put of B then finds `T <= T`.

Each operation applies its own watermark rule, as now: a `check` uses `last_write_ts`, and a mutation uses
`last_read_ts`. The failure goes to the first operation that does not pass.

This changes one detail of `prepareLocal`. Now it takes the stamps from the condition read or the update probe
(`conditionResult ?? probe`). For operation 2 and later, that read sees the private row. The engine must use the kept
stamps for those operations.

`clock_skew` and `pending_conflict` also use the committed state. The single-partition path has no timestamp check,
as now.

##### 4.2.4.2 Item identity

**The problem.** Now each identity site in the code uses `KeyCodec.pairKey`. It is a 128-bit value made of two xxhash64
values with a fixed public seed. It is a hash, so two different keys can have the same value. A caller can find two
such sort keys with a birthday search of about 2^32 hashes.

Now the client rejects such a pair as a false `transact_duplicate_key`, so the pair never reaches a partition. Ordered
mode skips that check. The coordinator and SQLite compare the key bytes, but the participant compares `pairKey`. The
two sides then disagree about the number of items, with these results:

- If the engine groups by `pairKey`, two items become one sequence. One item gets no lock, but its operations report
  `passed`. Commit writes only the other item, so the transaction is not atomic.
- If the engine groups by bytes, commit collapses the two keys into one entry. Then one of two failures occurs:
  - The duplicate check throws on each attempt, and a `PREPARED` transaction stays in `COMMITTING` and keeps both
    locks.
  - Commit writes the lock row of one item into the other item.

**The rule.** Two keys are the same item only when the bytes of their hash keys and of their sort keys are equal. A
hash value can select where to look for a key, but only a byte comparison decides that two keys are equal.

**The primitive.** `KeyPairMap<V>` in `sharding/key-codec.ts` is a map from a `(hashKey, sortKey)` pair to a value:

- Up to 8 entries, it keeps the entries in an array and finds a key with a byte comparison of each entry.
- Above 8 entries, it also keeps a `Map` from `hash32(sortKey, hash32(hashKey))` to the entries with that hash. It
  finds a key with one hash and a byte comparison of the entries in that bucket.
- The byte comparison checks the two lengths first, then compares the bytes from the last byte to the first. The
  sort key goes first, because two keys of one transaction usually have the same hash key and differ in the sort key.
- It allocates no string and no `BigInt` for a key.

A map with a hash key only, such as the owner cache of `ownsByHashKey`, uses the same class with the empty sort key.

**The sites.** Each of these uses `KeyPairMap`:

- The duplicate check of `validateTransactWriteOperations` (section 4.2.2).
- The grouping of the sequence engine (section 4.2.4, step 1).
- `commitLocal`: the key set of the request, the map of owned lock rows, the duplicate-key check (section 4.2.8), and
  the owner cache of `ownsByHashKey`.
- `validateTransactGetItemKeys`, and the pairing of the two phases of a read transaction in `db.ts`. These sites are
  safe now, because the client rejects a colliding pair. They change so that the codebase has one identity rule.

After the change, `KeyCodec.pairKey` has no caller, and the change removes it. The coordinator needs no change: SQLite
compares `BLOB` values by bytes, so `SELECT DISTINCT` in `loadItemKeys` and the primary keys of `tc_items` and
`pending_transactions` already use the rule.

**The measured cost.** A workerd test inside `@cloudflare/vitest-plugin` measured the work to find the distinct items
of one request. Each value is the median of 7 batches of at least 200 ms. The 2 KiB keys are the worst case: a long
shared prefix makes a comparison from the first byte slow, and a long shared suffix makes a comparison from the last
byte slow.

| Option | 2 ops, 1 item | 10 ops, one hash key | 100 ops, one hash key | 100 ops, 100 hash keys | 100 ops, 2 KiB keys, shared prefix | 8 ops, 2 KiB keys, shared suffix | 100 ops, 2 KiB keys, shared suffix |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `pairKey` `Set` (now, not exact) | 0.28 µs | 1.6 µs | 16 µs | 47 µs | 51 µs | 3.9 µs | 75 µs |
| `pairKey` `Map` and a byte check | 0.35 µs | 1.9 µs | 19 µs | 80 µs | 58 µs | 4.2 µs | 110 µs |
| Byte comparison of each pair | 0.03 µs | 0.16 µs | 18 µs | 49 µs | 19 µs | 55 µs | 10 ms |
| Sort, then compare neighbors | 0.13 µs | 1.3 µs | 6.8 µs | 4.3 µs | 250 µs | 18 µs | 246 µs |
| base64 text in a `Set` | 0.55 µs | 2.9 µs | 29 µs | 30 µs | 173 µs | 13 µs | 178 µs |
| hex text in a `Set` | 0.33 µs | 1.7 µs | 17 µs | 15 µs | 590 µs | 44 µs | 568 µs |
| `hash32` `Map` and a byte check | 0.24 µs | 1.2 µs | 12 µs | 12 µs | 69 µs | 5.2 µs | 68 µs |
| **`KeyPairMap`** (array up to 8, then `hash32`) | **0.03 µs** | **1.3 µs** | **13 µs** | **13 µs** | **71 µs** | **57 µs** | **71 µs** |

The results:

- For a small request with short keys, `KeyPairMap` is the fastest option: 0.03 µs for 2 operations, against
  0.28 µs now. Above 8 entries, it costs the same as the `hash32` map.
- Its worst case is about 71 µs, at 100 operations with 2 KiB keys. The hash makes the worst case grow with the
  bytes of the keys, and not with the square of the number of operations. The byte comparison of each pair takes
  10 ms in its worst case, and the sort takes 250 µs.
- The array part has its own worst case, 57 µs at 8 operations with 2 KiB keys. That is less than the worst case of
  the hash part, so the limit of 8 does not raise the worst case of the map.
- With short keys, it costs less than the `pairKey` `Set` that the code uses now. With 2 KiB keys, it costs up to
  20 µs more, because the exact answer needs a byte comparison.
- `hash32` returns a number, so the map needs no `BigInt`. A 32-bit hash has more collisions than `pairKey`. A
  collision costs one more byte comparison and does not change the answer. With 100 keys, a collision occurs in
  about one request of a million.
- Each value is small compared with one RPC of the transaction. The choice keeps the exact answer and a bounded worst
  case at no cost in the usual path.

#### 4.2.5 Private state: the two blocks of a prepare

`prepareLocal` runs two `transactionSync` calls:

1. **The evaluation block.** It runs the sequence engine. It copies every condition failure image into JavaScript.
   When every operation passed, it builds the final image of each multi-op key after the loop. Then it always throws a
   sentinel that carries the results. SQLite rolls back every write of the block.
2. **The lock block.** It runs only when every operation passed. It writes the `pending_tx_info` row and one
   `pending_transactions` row for each item. It writes no lock row for an item that this transaction already locks.
   The engine did not evaluate such an item (section 4.2.10), so the item has no final image, and its lock row
   already holds the final image of the first prepare.

The two blocks are safe for these reasons:

- No `await` runs between them, and the Durable Object runs one JavaScript event at a time. No other request can read
  `items` between the blocks.
- The evaluation block commits nothing. When the object stops between the two blocks, no lock exists, and the
  coordinator sends the prepare again.
- The `dispatch` of the sharding runtime does not wrap a local handler in a `transactionSync`. The blocks are top-level
  transactions, and nested transactions are not necessary.
- `PartitionStore` keeps no in-memory state. Its fields are `#storage` and `#migrations`, and it does not use
  `ctx.storage.kv`. A rollback therefore leaves no stale value in memory.

`executeSingleShot` runs the sequence engine in one `transactionSync`:

1. When the request has a multi-op key, the engine reads the committed `deletion_metadata` row before the loop
   (section 4.2.4, step 2).
2. The engine runs every sequence. Its private deletes change `deletion_metadata`, as the store writes do now.
3. When one operation failed, the block throws a sentinel that carries the results. SQLite rolls back every write.
4. When every operation passed, the block applies the last operation of each item, as its apply loop does now. For a
   single-op key, the last operation is its only operation. For a multi-op key, the last operation applies on top of
   the private writes of the earlier operations.
5. When the request has a multi-op key, the block writes `deletion_metadata` one time: the value from step 1 plus the
   delete effect of each item (section 4.2.6.1). This write replaces the changes of the private deletes. Then the
   block returns, and all writes commit.

Without a multi-op key, the block skips steps 1 and 5. Each item then has one operation, so the replay already gives
the delete effect of each item, and standard mode costs the same as now.

This path builds no final image. Step 5 gives the single-partition path the same end state as the two-phase path. The
item rows, `key_size_estimates`, and `item_id` already agree without it:

- Each write of the replay keeps `key_size_estimates` equal to the size of the current row, so after the sequence it
  holds the size of the final row.
- A delete followed by an insert gives a new `item_id` on both paths. A `replace` row also deletes and inserts.
- The final row has the same data, `v`, TTL, and timestamps on both paths (section 4.2.6).

Only `deletion_metadata` differs, because the replay changes it for each delete and the lock row changes it at most one
time for each item. Section 4.2.6.1 gives an example and its effect on other transactions.

A rollback must not change any in-memory state. This applies to each value that a block changes: the Bloom filter,
the size estimates, the TTL timer, and every cache. Promotion candidates and job signals go to `call.signal(...)` only
after the block committed.

#### 4.2.6 The final image of an item

The lock row of an item holds its final image. Commit applies that image to the committed row and evaluates nothing.

Now a lock row holds one operation, and commit takes the rest from the committed row: `upsertItem` sets `v = v + 1`,
and `deleteItem` increments `delete_revision` when a row exists. A sequence collapses into one lock row, so commit
needs two more facts:

1. **The number of writes.** `put → update` must give `v + 2`.
2. **Whether the sequence deleted the committed row and wrote it again.** `delete → put` must increment
   `delete_revision` and start `v` again at 1. A read transaction compares `version` and `deleteRevision`. Without the
   increment, it can miss the delete, because `v` can return to its old value.

The lock row stores both facts as relative values. Commit applies them to the committed row at commit time, as it
applies `v + 1` now.

| Sequence | Lock `operation` | Data | `version_delta` |
| --- | --- | --- | --- |
| Every operation is a `check` | `check` | none | not used |
| The final state is absent | `delete` | none | not used |
| The final state is present, and the rule for `replace` is false | `put` or `update` | the final data | see below |
| The final state is present, and the rule for `replace` is true | `replace` | the final data | see below |

The rule for `replace` is true when the committed row exists and the sequence contains a `delete`. A write before the
delete does not change the rule. The delete removes the row of the item, also when an earlier write of the sequence
changed that row.

`version_delta` is the number of writes after the last delete of the sequence. When the sequence has no delete, it is
the number of writes. A write is a `put` or an `update`.

The lock block writes the row of a single-op key as now. Its `version_delta` is 1, which is the default value.

For a multi-op key, the engine computes the `operation` and `version_delta` from the whole sequence. The final data,
kind, and TTL come from the last write of the sequence: the last `put` or `update` after the last `delete`. A `check`
adds nothing to the final state, and the last operation makes no private write.

| Last write of the sequence | Final data, kind, and TTL | Read? |
| --- | --- | --- |
| `put`, as the last operation or with only `check`s after it | the data, kind, and `ttlAt` of that `put` | no |
| `update`, as the last operation | a document `SELECT` of the update on the private row | yes |
| `update`, with only `check`s after it | the private row as the update left it | yes |
| no write after the last `delete` | absent | no |
| no write and no `delete` (only `check`s) | none: a `check` row | no |

The details of the two reads:

- **The document `SELECT`.** It runs `plan.documentSql` on the private row, with the `LEFT JOIN` that
  `insertPendingUpdateLock` uses. The kind is `json`. The TTL is the `ttlAt` of the update, else the TTL of the private
  row. This is a new store method: the `SELECT` part of `insertPendingUpdateLock`, which returns the document and does
  not insert it.
- **The private row read.** It returns the stored bytes, the kind, and the TTL of the row that the update wrote.

Both reads must return the stored bytes. `getItemImage` is not correct for them, because it decodes JSONB to JSON
text, and a JSONB to text to JSONB round trip is not size-stable. Section 4.2.7 gives the reason in detail.

The last write passed `#precheckWrite`, so the final data fits in `MAX_ITEM_BYTES`.

The data of each lock row comes from one of these places. Only the marked case copies data out of SQLite into
JavaScript:

| Case | Where the lock row data comes from | Copied into JavaScript? |
| --- | --- | --- |
| Single-op `put` | the request data | no, it is already in memory |
| Single-op `update` | `INSERT … SELECT` on the committed row, inside SQLite, as now | no |
| `delete`, or `check` with no write | no data | no |
| Multi-op key whose last write is a `put` | the request data of that `put` | no |
| Multi-op key whose last write is an `update` | the private row, through one of the two reads above | **yes** |
| Single-partition path, any key | no lock row: the writes commit directly | no |

The private row exists only inside the evaluation block, and the lock block runs after the rollback. That is why the
marked case must copy the data. A single-op `update` reads the committed row, which still exists in the lock block, so
its `INSERT … SELECT` stays in SQLite.

These two kinds of image are different, and they have opposite reasons:

- **A condition failure image** (`all_old`) exists only for an operation whose condition failed, that asked for
  `all_old`, and whose item exists (section 4.2.11).
- **A final image** exists only when every operation passed, because it is the data that commit writes.

A `check` after a write does not change the lock row. The write already moves `last_read_ts` to `T`.

Examples:

| Sequence on an item with `v = 4` | Lock row | Result at commit |
| --- | --- | --- |
| `put → update` | `update`, `version_delta = 2` | `v = 6` |
| `check → put` | `put`, `version_delta = 1` | `v = 5` |
| `delete → put` | `replace`, `version_delta = 1` | `v = 1`, `delete_revision + 1` |
| `put → delete → put` | `replace`, `version_delta = 1` | `v = 1`, `delete_revision + 1` |
| `update → delete → put → update` | `replace`, `version_delta = 2` | `v = 2`, `delete_revision + 1` |
| `put → delete → put → delete` | `delete` | the row is deleted, `delete_revision + 1` |
| `check → check` | `check` | `last_read_ts` moves to `T` |

| Sequence on an absent item | Lock row | Result at commit |
| --- | --- | --- |
| `put → update` | `update`, `version_delta = 2` | `v = 2` |
| `put → delete → put` | `put`, `version_delta = 1` | `v = 1` |
| `put → delete` | `delete` | `max_delete_tx_order_ts` moves to `T`, as now |

##### 4.2.6.1 The delete effect of an item

`deletion_metadata` holds one `delete_revision` and one `max_delete_tx_order_ts` for the whole partition. Both paths
change it with the delete effect of each item, which depends only on the lock operation of the item and on the
committed row:

| Lock operation | Committed row | `delete_revision` | `max_delete_tx_order_ts` |
| --- | --- | --- | --- |
| `replace` | exists | + 1 | no change |
| `delete` | exists | + 1 | `MAX(current, T)` |
| `delete` | absent | no change | `MAX(current, T)` |
| `put`, `update`, `check` | either | no change | no change |

`replace` does not change `max_delete_tx_order_ts`, for these reasons:

- The watermark orders a transaction on an absent item. An absent item has no row, so no row holds its stamps.
- After a `replace`, the item is present. Its new row holds `last_read_ts = last_write_ts = T`.
- On the two-phase path, prepare accepted `T` only above the `last_read_ts` of the committed row. The new stamps are
  therefore not lower than the old stamps, and a later transaction on the item orders against `T` through the row,
  as after a `put`.
- On the single-partition path, `T` is the clock of the partition, as now. The watermark takes `MAX(current, T)` and
  not the old stamps of the row, so a change of the watermark gives no ordering that the row does not give.
- `replace` leaves no item absent, so the watermark has nothing to order.

`replace` still increments `delete_revision`, because `v` starts again at 1 and a read transaction must see the
change (section 4.2.6). This is the one delete that removes a row and changes only `delete_revision`. `deleteItem`
cannot apply this effect, because it changes both values in one statement when it deletes a row.

On the two-phase path, commit applies the effect through the store writes of section 4.2.8. On the single-partition
path, step 5 of `executeSingleShot` applies it (section 4.2.5). One function maps a lock operation and the existence
of the committed row to the delete effect, and the single-partition path uses only that function. A test runs every
sequence on both paths and compares `deletion_metadata`, so the function and the store writes of commit cannot drift
apart.

The effect is not a replay of each delete. It is the effect of the net operation of the item, which is the operation
that a standard transaction sends for the same end state:

- `put → delete → put` on an absent item leaves the same state as a standard `put`.
- `put → delete → put → delete` on an existing item leaves the same state as a standard `delete`.

A replay of each delete changes `deletion_metadata` for deletes that have no net effect. Because the values are shared
by the whole partition, that makes other transactions abort with no benefit. An example with
`put → delete → put` on an absent item B, at timestamp `T`, in a partition with `delete_revision = 10` and
`max_delete_tx_order_ts = W`, where `W < T`:

| | Replay of each delete | Delete effect of the item |
| --- | --- | --- |
| Item B | present, `v = 1` | present, `v = 1` |
| `delete_revision` | 11 | 10 |
| `max_delete_tx_order_ts` | `T` | `W` |

With the replay, these two failures can occur on other items of the partition:

1. A multi-partition `transactGetItems` reads item C, which did not change, before and after the commit. Its two reads
   of `deleteRevision` differ, and it aborts with `read_conflict`.
2. A later two-phase prepare on an absent item D has a coordinator timestamp `T' <= T`. This happens when the clock of
   the coordinator is behind the clock of the partition, within `maxClockSkewMs`. The prepare fails with
   `timestamp_conflict`.

Both aborts are safe, because the caller retries. With the delete effect, neither abort occurs. An ordered
transaction makes other transactions abort no more often than the standard transaction with the same net effect. A
`replace` has no standard equivalent. Its only effect on other transactions is the `delete_revision` increment, which
can abort a read transaction as in failure 1.

In standard mode each item has one operation, so its delete effect is the effect of that operation on both paths, as
now.

#### 4.2.7 Lock row schema and its migration

A new partition schema migration adds one column to `pending_transactions`:

```sql
ALTER TABLE pending_transactions ADD COLUMN version_delta INTEGER NOT NULL DEFAULT 1;
```

`operation` also gets the value `replace`. The column is `TEXT` with no constraint, so the value needs no schema change.

The lock row gets its own operation type, `PendingLockOperation = "put" | "update" | "replace" | "delete" | "check"`.
It is separate from `TransactionOperationType`, so that a wire request cannot carry `replace`. `PendingTxItem.operation`
and the result of `pendingLockFor` use the new type in place of `string`. Each store method that reads
`pending_transactions.operation` checks the value with `invariant()` and returns the typed value.

The lock rows that exist need no data change. Each of them holds one operation, and `DEFAULT 1` gives the current
result at commit.

The data of a `put`, `update`, or `replace` row has the form that `upsertItem` binds verbatim. When the last write of
the item is a `put`, it is the request data, as now. When the last write is an `update`, the row holds the stored
bytes, which are JSONB for kind `json`. `itemDataExpr` binds a JSONB `Uint8Array` verbatim. The comment of
`insertPendingUpdateLock` gives the reason for the stored bytes: a JSONB to text to JSONB round trip changes the size,
so the bytes that the checks measured must be the bytes that commit writes.

The migration stream must carry the new column. `pendingTxPageStatement` selects it, and `insertPendingLock` writes it
on the target.

#### 4.2.8 Commit and the single-partition path

`commitLocal` raises a `FokosInternalError` when the request contains one key two times. The proposed code is
`commit_duplicate_key` in `INTERNAL_CODES`, next to `commit_keyset_mismatch`. The check, the key set of the request,
and the map of owned lock rows use `KeyPairMap`, so two keys are one key only when their bytes are equal (section
4.2.4.2).

- **Why the check is necessary.** Now `commitLocal` compares the size of the request key set with the number of owned
  lock rows. A duplicate key passes that comparison, and `#applyCommitItems` then applies the same lock row two times:
  a `put` row with `version_delta = 2` gives `v + 4`.
- **Why it is an internal error.** The coordinator sends unique keys (section 4.2.9), so a duplicate key is a defect in
  the code, not an input of a caller. The commit fails, and the coordinator stays non-terminal and retries, as for
  `commit_keyset_mismatch`.
- **Cancel.** A cancel with a duplicate key releases the same lock two times, which changes nothing, so cancel keeps
  its current behavior.

`#applyCommitItems` applies each lock row with the transaction timestamp `T`. It is an exhaustive `switch` over
`PendingLockOperation`:

- **`put` and `update`** — upsert the row with the stored data, kind, and TTL. On an existing row, set
  `v = v + version_delta`. On a new row, set `v = version_delta`. Move `last_read_ts` and `last_write_ts` to
  `MAX(current, T)`, as `upsertItem` does now.
- **`replace`** — delete the committed row and subtract its size from `key_size_estimates`, as `deleteItem` does.
  When the statement deleted a row, increment `delete_revision`. Do not change `max_delete_tx_order_ts` (section
  4.2.6.1). This is a new store method, because `deleteItem` changes both values. Then insert the row with the
  stored data, kind, and TTL, and `v = version_delta`.
- **`delete`** — call `deleteItem` with `bumpTxOrderTsAlways`, as now.
- **`check`** — call `bumpItemReadTs`, as now.

`upsertItem` gets `version_delta` as an input. The current callers pass 1.

The store updates `key_size_estimates` from the final row, as it does now. Commit reports promotion candidates for
`put`, `update`, and `replace` rows. The rules of `commitLocal` for the key set, the copies, and the release of each key
stay the same.

Commit applies relative values, so it gives the correct result on the committed row that it finds. It does not depend
on an unchanged committed row between prepare and commit.

`executeSingleShot` does not write lock rows. It applies each operation directly (section 4.2.5).

#### 4.2.9 Coordinator

**Schema migration.** A new coordinator migration rebuilds `tc_items` with the new primary key:

```sql
CREATE TABLE tc_items_new (
    transaction_id      TEXT    NOT NULL,
    hk                  BLOB    NOT NULL,
    sk                  BLOB    NOT NULL DEFAULT x'',
    op_index            INTEGER NOT NULL,
    operation           TEXT    NOT NULL,
    data                ANY,
    data_kind           INTEGER,
    ttl_epoch_utc_seconds INTEGER,
    conditions_json     TEXT,
    update_json         TEXT,
    partition_do_name   TEXT    NOT NULL,
    return_values_on_condition_check_failure INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (transaction_id, op_index)
) WITHOUT ROWID, STRICT;
INSERT INTO tc_items_new SELECT transaction_id, hk, sk, op_index, operation, data, data_kind, ttl_epoch_utc_seconds,
    conditions_json, update_json, partition_do_name, return_values_on_condition_check_failure FROM tc_items;
DROP TABLE tc_items;
ALTER TABLE tc_items_new RENAME TO tc_items;
```

The migration copies `op_index` unchanged. It cannot fail on the new key, because `db.ts` gives each operation of a
transaction a different `op_index`. `runAllSync` runs it in one `transactionSync`, so an active transaction keeps all
of its rows or the migration does not apply.

**Keys and payload.**

- `initiateWrite` writes one `tc_items` row for each operation, as now.
- `loadItems` reads the rows `ORDER BY op_index`, as now. A prepare and a prepare from recovery therefore send the
  operations of an item in request order. The participant sorts them again (section 4.2.4).
- `loadItemKeys` must return each `(hk, sk, partition_do_name)` one time, for example with `SELECT DISTINCT`.
  `runCommit` and `runCancel` use it, so commit and cancel send each key one time. All operations of an item have the
  same `partition_do_name`, because the root partition depends on the hash key only.
- `cancelTransactionInStore` builds the positional results from `loadItems`, as now.
- `applyMigrationPage` uses `INSERT OR REPLACE INTO tc_items`. The new primary key keeps the insert idempotent.

**Fingerprint.** `hashTransactionOperations` gets the execution mode. In standard mode, the hash must stay the same as
now, so that a stored `operations_hash` keeps its meaning. In ordered mode, the function chains the mode into the hash.
`db.ts` sends `"standard"` when the caller gives no mode, so a missing mode and `"standard"` give the same hash.
`tc_state` gets no column for the mode.

#### 4.2.10 Routing, retries, recovery, and migration

- **Routing.** `txPrepare` is a `group` operation. All operations of an item must go to the same owner in one
  sub-request. The runtime must resolve equal keys of one dispatch to the same owner, also when the Bloom filter
  takes part. A test must prove this rule.
- **Order.** The participant sorts each item by `opIndex`. A change of the array order by routing, forwarding, or
  migration has no effect.
- **Repeated prepare.** When this transaction already holds the lock of an item, every operation of the item gets
  `passed`, and the engine does not evaluate the item again. The lock block writes no lock row for that item (section
  4.2.5). Its lock row and the `pending_tx_info` row of the transaction already exist and do not change. When every
  item of the request is locked by this transaction, the lock block writes nothing, and the prepare answers
  `accepted`. `insertPendingLock` keeps `INSERT OR IGNORE`, because the migration stream can send one lock row two
  times.
- **Commit and cancel.** The coordinator sends unique keys (section 4.2.9). The lock row has one row for each key, so
  `commitLocal` compares the same key sets as now.
- **Recovery.** The stale-transaction job and `debugForceResolveTransaction` read the keys from the lock rows. These
  keys are unique.
- **Partition migration.** A lock row moves with its final image (section 4.2.7).
- **Coordinator migration.** A migration page carries the `tc_items` rows with their `op_index`.

#### 4.2.11 Condition failure images

`#imageForFailedCondition` reads the row from `items` in the same block as the condition. For operation *k*, `items`
holds the private state that operations 1 to *k − 1* left. The image therefore shows the state immediately before
operation *k*, with its private data, `v`, and TTL.

- When an earlier operation deleted the item, the condition sees no item, and the result has no image.
- When the first operation of an item fails, the image shows the committed state, as now.
- The evaluation block copies the image into JavaScript before it rolls back.
- `MAX_CONDITION_CHECK_IMAGE_BYTES_PER_TX` (10 MiB) and `applyImageCap` apply as now.

#### 4.2.12 Invariants

- **No request sees a private state.** The evaluation block always rolls back. No `await` runs between the two
  blocks.
- **A transaction commits all items or none.** Prepare writes locks only when every operation passed. The
  single-partition path rolls back on one failure.
- **Commit evaluates nothing.** The lock row holds the final image.
- **Commit gives each write of the sequence its version increment.** The lock row stores `version_delta` and
  `replace`, and commit applies them to the committed row (section 4.2.8).
- **One lock for each item.** `pending_transactions` keeps `PRIMARY KEY (hk, sk, transaction_id)`. The lock block
  writes one row for each item.
- **Each `opIndex` keeps its value.** `db.ts` sets it. `tc_items` uses it as its key. Every wire type carries it.
- **Each item runs in request order.** The participant sorts each group by `opIndex`.
- **A rollback leaves memory unchanged.** `PartitionStore` has no in-memory state. Signals go out only after a
  commit.

#### 4.2.13 Concurrency and failure

- **Requests on the same partition.** The Durable Object runs one event at a time. Each block is synchronous, so no
  other request runs inside a block or between the two blocks of a prepare.
- **More than one drive of a transaction.** A request, a retry with the same token, and `tx_recovery` can send the same
  prepare at the same time. The lock check of a repeated prepare answers `passed` for each operation (section 4.2.10).
  The coordinator rules for `PREPARED` and `CANCELLING` do not change.
- **Stop between the blocks.** The evaluation block committed nothing, so no lock exists. The coordinator sends the
  prepare again.
- **Stop after the lock block.** The lock rows hold the final images. The coordinator commits or cancels as now.
- **Migration during prepare.** A partition that imports answers `partition_migrating`, as now.

#### 4.2.14 Performance

- **Single-op keys, in both modes.** The engine makes no private write for a single-op key. The evaluation block makes
  the same reads as the current check pass, and its rollback has no write to undo. The lock block makes the same
  writes as now. A rejected transaction pays only the reads.
- **Multi-op keys.** A multi-op key with *n* operations costs *n* condition evaluations and *n − 1* private writes in
  the evaluation block. A key whose last write is an `update` adds one read for the final image. It pays the private
  writes also when the transaction is rejected, because a later operation needs the state that an earlier one wrote.
  Section 4.2.14.1 gives the memory, and section 4.2.14.2 gives the measured cost.
- **Commit.** Commit makes one write for each item, not one for each operation.
- **Wire.** Commit and cancel send unique keys. A prepare carries each operation, as now.
- **Single-partition path.** A request with a multi-op key reads and writes the `deletion_metadata` row one more time
  (section 4.2.5). A request with no multi-op key costs the same as now.

##### 4.2.14.1 Memory of the final images

A prepare holds the final images of its multi-op keys in JavaScript between the evaluation block and the lock block.
The data must leave SQLite, because the rollback of the evaluation block discards every row that the block wrote.

Only a multi-op key whose last write is an `update` copies data out of SQLite (section 4.2.6). A last write that is a
`put` uses the request data, which is already in memory, and an absent final state has no data. The evaluation block
builds the final images only when every operation passed, so it never holds final images and condition failure images
at the same time.

The worst case is about 20 MB. A request has at most `MAX_ITEMS_PER_TX` (100) operations, so it has at most 50 multi-op
keys, and each final image is at most `MAX_ITEM_BYTES` (400 KB). An example:

1. A request has 50 items, each with `update → update`. Each committed item holds 400 KB. The request payload is small,
   because an update carries no data, so `MAX_PAYLOAD_BYTES_PER_TX` does not limit it.
2. The evaluation block applies the first update of each item as a private write: 50 writes.
3. Every operation passed. After the loop, the block runs the document `SELECT` of the second update of each item, and
   copies 50 JSONB documents of 400 KB into JavaScript: 20 MB.
4. The block throws, and SQLite rolls back the 50 private writes. The 50 documents stay in JavaScript.
5. The lock block writes 50 lock rows from the documents. The documents are then garbage.

The memory of a Durable Object is 128 MB. Standard mode already holds a larger worst case: `prepareLocal` collects
every condition failure image before `applyImageCap` drops the images above 10 MiB. A rejected request with 100
operations on items of 400 KB therefore holds about 40 MB of images.

##### 4.2.14.2 Measured cost of the evaluation block

A scratch test measured the evaluation block on Durable Object storage in the local Workers runtime of
`@cloudflare/vitest-plugin`. The block ran the reads of the prechecks (`getItemStamp`, `measureItemBytes`,
`probeUpdate`), the private writes (`upsertItem`, `updateItemSingleShot`, `deleteItem`), and the read of the final row.
Each key had a committed JSON item before the test. The operations were `put` and `update` in turn. Every item had two
or more operations, so the rollback column shows ordered mode when every item is a multi-op key.

The test compared three modes:

- **reads only** — the prechecks with no private write. This is the cost of standard mode.
- **rollback** — the evaluation block of ordered mode, which throws at the end.
- **commit** — the same block, which commits at the end.

Each value is the median of 15 runs, less the median of an empty `runInDurableObject` call (2 ms). The timer has a
resolution of 1 ms. `rowsWritten` is the sum that the store writes report.

| Transaction | reads only | rollback | commit | `rowsWritten` |
| --- | --- | --- | --- | --- |
| 2 items × 2 operations, 100 B | 0 ms | 4 ms | 5 ms | 8 |
| 10 items × 10 operations, 1 KB, 1 delete for each item | 2 ms | 8 ms | 8 ms | 200 |
| 1 item × 100 operations, 40 KB | 3 ms | 18 ms | 18 ms | 200 |
| 10 items × 2 operations, 380 KB | 5 ms | 49 ms | 57 ms | 40 |
| 50 items × 2 operations, 1 KB | 3 ms | 10 ms | 8 ms | 200 |

The results:

- A rollback costs the same as a commit of the same writes. The cost comes from the writes, not from the rollback.
- The cost grows with the bytes that the block writes. The largest case writes 10 items of 380 KB, two times each,
  in about 50 ms.
- The pricing page of Durable Objects does not say if a rolled-back row counts as a row written. In the worst case,
  each private write counts: 200 rows cost $0.0002 at $1.00 for each million rows.
- The lock block is not in these numbers. It writes one lock row for each item, as a standard prepare does now.
- The measured block wrote every operation. The design writes *n − 1* operations for an item with *n*, so the private
  writes of an item with 2 operations are half of the measured writes.

**The worst case.** `MAX_ITEMS_PER_TX` (100) limits a prepare to 99 private writes. `MAX_PAYLOAD_BYTES_PER_TX`
does not limit their bytes, because an `update` carries no data (section 4.2.14.1). Each private write can rewrite
an item of `MAX_ITEM_BYTES`, so one prepare can write about 40 MB that the rollback discards.

A second scratch test measured this case with the design as written: *n − 1* private writes for each item, and the
document `SELECT` of section 4.2.6 for each final image. Each committed item was a JSON document of about 390 KB.
Each operation was an `update` that sets `$.n` to `$.n + 1`. Each value is the median of 15 runs, less the median of
an empty `runInDurableObject` call (3 to 4 ms). The test ran two times, and the table gives both values.

| Transaction | reads only | evaluation block | prepare (both blocks) | single-partition path |
| --- | --- | --- | --- | --- |
| 1 item × 100 operations | 9 / 10 ms | 28 / 30 ms | 33 / 39 ms | 27 / 29 ms |
| 50 items × 2 operations | 12 / 13 ms | 40 / 51 ms | 121 / 218 ms | 40 / 43 ms |
| 100 items × 1 operation (standard mode) | 15 / 16 ms | 16 / 16 ms | 240 / 418 ms | 51 / 53 ms |

- **reads only** — the lock check and the update probe of each operation.
- **evaluation block** — the reads, the private writes, the final images, and the rollback.
- **prepare** — the evaluation block and then the lock block.
- **single-partition path** — the reads and every write of every operation, with a commit.

The results:

- The worst case of the evaluation block, 1 item with 100 operations, takes about 30 ms. Its 99 private writes of
  about 390 KB all change one row.
- The lock block costs more than the evaluation block when the final images are large. A standard prepare of 100
  updates of 390 KB items takes 240 to 418 ms now, and almost all of it is the lock block. Ordered mode does not add
  to the lock block: it writes one lock row for each item, as standard mode does.
- On the single-partition path, ordered mode adds no write. That path writes every operation in both modes.
- A repeated prepare and a drive of the `tx_recovery` job run the evaluation block again. A rejected prepare also
  pays for its private writes, up to the operation that failed.
- The evaluation block of the worst case costs less than the lock block of a standard prepare of 100 large updates.
  Ordered mode needs no smaller limit.

The local runtime is not the production runtime. These numbers compare the modes with each other. They do not
predict the latency in production.

#### 4.2.15 Deployment and rollback

- The two schema migrations run in the constructor, inside `blockConcurrencyWhile`, before the first request.
- Old and new code must not run at the same time. An old participant ignores the mode and sees repeated items as
  separate operations.
- The standard-mode fingerprint does not change. A retry of a transaction from before the deployment keeps its
  `operations_hash`.
- A rollback to old code is not supported after a migration ran. Old code cannot read the new `tc_items` key, the
  `version_delta` column, or a `replace` lock row.

#### 4.2.16 Testing

The tests use the current suites in `test/partition-do/`, `test/transactions/`, `test/repartition/`, and
`test/property-based/`.

- **Repeated-item sequences.** `put → update`, `put → check`, `delete → put`, `put → delete`, `check → check`, and
  long sequences. Each test checks the data, `v`, the TTL, `delete_revision`, `max_delete_tx_order_ts`, and the
  timestamps after commit.
- **First failure.** A failure at each position of a sequence. Check the `passed`, `rejected`, and `not_evaluated`
  results, and the results of the other items.
- **Full rollback.** A failure on one item leaves every item unchanged, on both paths.
- **Both paths.** Each sequence test runs on the single-partition path and on the two-phase path, and expects the
  same item rows, `key_size_estimates`, and `deletion_metadata` on both.
- **Delete effect.** `put → delete → put` on an absent item changes neither `delete_revision` nor
  `max_delete_tx_order_ts`, on both paths. A concurrent multi-partition `transactGetItems` of another item in the
  partition commits. `delete → put` on an existing item increments `delete_revision` and does not change
  `max_delete_tx_order_ts`, on both paths. After it, a prepare on another absent item with a timestamp at or below
  `T` and above the earlier watermark is accepted, and a prepare on the replaced item at or below `T` fails with
  `timestamp_conflict`.
- **Timestamp rule.** `put → check` and `delete → put` pass. A sequence fails with `timestamp_conflict` against a newer
  committed stamp, at the first operation whose rule fails. A transaction with an existing item A (`delete → put`) and
  an absent item B (`put`) in one partition commits, in both orders of A and B in the request.
- **Duplicate commit key.** A `txCommit` request with one key two times fails with `commit_duplicate_key` and changes
  no row.
- **Images.** An `all_old` image of operation *k* shows the private state before *k*. A private delete gives no image.
- **Item size.** An intermediate state above `MAX_ITEM_BYTES` fails the operation that makes it.
- **Final image.** For each row of the last-write table in section 4.2.6, the committed row is the same as the row
  that a private write of the last operation gives. The last operation of an item makes no private write. A
  `put → check` sequence makes no read for its final image.
- **Idempotent retries.** A retry with the same token and the same mode gets the stored outcome. A retry with a
  different mode gets the token mismatch error. A standard-mode fingerprint keeps its current value.
- **Mode validation.** An `executionMode` other than the two values fails with `transact_execution_mode_invalid`, with
  and without a `clientRequestToken`, and sends no RPC. An absent mode gives the same result and the same fingerprint
  as `"standard"`.
- **Recovery.** A repeated prepare, the coordinator `tx_recovery` job, and the partition stale-transaction job resolve
  an ordered transaction.
- **Repeated prepare of a multi-op key.** A second prepare of an accepted ordered transaction answers `accepted`. It
  changes no lock row and no `pending_tx_info` row, and the commit after it gives the same item rows as a commit after
  one prepare.
- **Item identity.** The test finds two different sort keys with the same `hash32(sortKey, hash32(hashKey))`. On
  average, a search finds such a pair after about 80 000 random keys. Each site of section 4.2.4.2 treats the two
  keys as two items: standard mode accepts them, the engine makes two sequences, prepare writes two lock rows, and
  commit applies each lock row to its own item. The request holds more than 8 keys, so the hash part of `KeyPairMap`
  answers. A unit test of `KeyPairMap` covers equal keys, a length difference, a difference in the first byte, a
  difference in the last byte, and the change from the array to the hash at the ninth entry.
- **Partition migration.** A hash split and a promotion move a lock row with its final image. Commit on the target
  applies the image.
- **Coordinator migration.** A coordinator split moves `tc_items` rows with their `op_index`.
- **Schema migrations.** A database with active transactions keeps every `tc_items` row and every `op_index`. A lock
  row from before the migration gets `version_delta = 1` and commits with the same result as before.
- **Routing.** Equal keys of one `txPrepare` dispatch go to one owner in one sub-request, with the Bloom filter on.
- **Memory.** A rolled-back block leaves no change in memory.
- **Property-based.** A `fast-check` suite compares random ordered transactions with a reference model of standalone
  operations. For `deletion_metadata`, the model applies the delete effect of section 4.2.6.1 and not a replay of
  each delete. The suite runs each transaction on both paths and expects the same state.

## 5. Alternative options

- **Evaluate the private state in JavaScript.** SQLite evaluates every condition and every update now. A second
  evaluator in JavaScript can give a different answer than SQLite.
- **A scratch table for the private state.** The compiled condition and update plans read `items`. A scratch table
  needs a second form of each plan and more code.
- **Nested `transactionSync` (savepoints).** The Durable Object documentation does not describe nested transactions.
  Two top-level blocks need no nesting.
- **Store the operation list in the lock row and replay it at commit.** A lock row can then hold up to 100 times
  `MAX_ITEM_BYTES`, and commit does the work again. The final image is one row.
- **Store the absolute final version and two delete counters in the lock row.** This gives a strict replay of each
  operation, also for `delete_revision` and `max_delete_tx_order_ts`. It needs three columns and a data migration of
  the lock rows that exist. The final `v` is correct only when the committed row does not change between prepare and
  commit, so commit must also check that rule. The strict replay of `deletion_metadata` also makes other transactions
  abort for deletes that have no net effect (section 4.2.6.1).
- **On the single-partition path, roll back and then apply the final images with the commit apply.** This gives one
  definition of the delete effect. It adds a rollback, a second block, and a read of the final image for each
  multi-op key. One write of `deletion_metadata` at the end of the block gives the same end state (section 4.2.5).
- **Undo the private writes by hand, so that the lock block can read the final images in SQLite.** The block copies
  the committed rows aside, writes the lock rows with `INSERT … SELECT` from the private rows, and then restores
  `items`, `key_size_estimates`, and `deletion_metadata`. One missed restore corrupts the committed state.
- **A second pass for each multi-op key, to hold one final image at a time.** After the evaluation block, the prepare
  runs the private writes of each multi-op key again, reads its final image, rolls back, and writes its lock row. A stop
  in the middle leaves the locks of some items, and the repeated prepare of the coordinator completes the rest. The
  peak memory goes from about 20 MB to 400 KB, but the private writes of every multi-op key run two times, and the
  prepare runs two blocks for each multi-op key. The 20 MB worst case is less than the 40 MB of condition failure
  images that standard mode can hold now (section 4.2.14.1).
- **Private writes for every item in prepare.** This makes one path for all items, but it adds writes and a rollback
  to every standard-mode prepare.
- **A private write for the last operation of a multi-op key.** The final image is then one read of the final row,
  with one rule for all operation types. It costs one more write for each multi-op key, which is double the private
  writes of an item with 2 operations.
- **A separate method `transactWriteOrderedItems()`.** It is a wrapper of one line, but it doubles the public types,
  the documentation, and the HTTP surface. The `executionMode` option gives the same function.
- **Send the mode to the partition.** The engine gives the same result for both modes. The client is the validation
  boundary, so the partition needs no mode.
- **Check the timestamp against the private state.** All operations use one timestamp, so valid sequences fail
  (section 4.2.4.1).
- **Keep `pairKey`, and reject a hash collision at the client in ordered mode.** This keeps the false
  `transact_duplicate_key` of standard mode, and every site still depends on one client check. A caller that sends
  the RPC without `db.ts` reaches the participant with the collision.
- **Other forms of exact identity.** A byte comparison of each pair takes 10 ms in its worst case. A sort takes
  250 µs. A base64 or hex text key allocates a string for each key and costs up to 590 µs. Section 4.2.4.2 gives the
  measurements.

## 6. Frequently asked questions

**Does ordered mode change a request with no repeated item?**
No. Each item is a sequence of one operation, and the result is the same as in standard mode.

**Can an ordered transaction commit in part?**
No. One failure cancels the transaction. The other items continue only to report their results.

**Is there an order across items?**
No. The order applies to the operations of each item. Different partitions run in parallel.

**Which image does `all_old` return?**
The state immediately before the failed operation, with the private changes of the earlier operations of its item.

**Why is the two-block prepare safe without nested transactions?**
No `await` runs between the blocks, and the first block commits nothing. Section 4.2.5 gives the full reasons.

**Do the two paths leave the same state?**
Yes. The item rows and `key_size_estimates` agree without extra work. The single-partition path writes
`deletion_metadata` one time at the end, with the same delete effect that commit applies (sections 4.2.5 and 4.2.6.1).

**Can a prepare keep the final images in SQLite and out of JavaScript?**
Not with a rollback: the rollback discards every row that the evaluation block wrote, so the final data must leave
SQLite to reach the lock block. Section 5 lists the two options that avoid the copy or reduce it, and their cost. The
worst case is about 20 MB (section 4.2.14.1).

**What happens to a lock that was prepared before the deployment?**
It gets `version_delta = 1`, the default value of the new column. A lock row from before the deployment holds one
operation, so commit gives the same result as before (section 4.2.7).

**Why does the coordinator not store the mode in a column?**
Nothing below the client reads the mode. The fingerprint is its only use, and `operations_hash` holds it.

**Does the single-partition path change its rule for `clientRequestToken`?**
No. A request with a token uses the two-phase path in both modes.

## 7. References

References:

- `AGENTS.md`
- `docs/agent-plans/2026-08-23-single-partition-transaction-fast-path.md`
- `docs/agent-plans/2026-09-02-update-expressions.md`
- `docs/agent-plans/2026-09-05-item-order-timestamps-and-read-revisions.md`
- `docs/agent-plans/2026-09-08-return-values-on-condition-check-failure.md`
- `docs/agent-plans/2026-09-27-learned-routes-for-every-dispatch-shape.md`
- `docs/agent-plans/2026-09-29-promotion-moves-its-locks.md`
- [SQLite-backed Durable Object Storage](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)
- [ATC 2023, Idziorek et al.](https://www.usenix.org/system/files/atc23-idziorek.pdf)
