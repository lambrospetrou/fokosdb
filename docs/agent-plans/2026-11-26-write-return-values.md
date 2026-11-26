# RFC — The old item on a successful `putItem` and `deleteItem`

**State:** Implemented
**Date:** 2026-11-26
**Author:** Lambros

## 1. Overview and context

`putItem` and `deleteItem` return the old item only when their condition fails.
The option `returnValuesOnConditionCheckFailure: "all_old"` puts it in the `FokosConditionCheckError`.
A write that succeeds cannot return the old item.
A caller that needs it must read the item first, in a second request, and the item can change between the two.

The results also echo the keys of the request:

```ts
putItem    → { item: { hashKey, sortKey }, version, meta }
deleteItem → { item: { hashKey, sortKey }, deleted, meta }
```

The caller already has those keys.
In every read result, `item` is a whole item, so the same field name has two meanings.

DynamoDB `PutItem` and `DeleteItem` take `ReturnValues: "NONE" | "ALL_OLD"`.
The response has `Attributes`, the old item, only when `ALL_OLD` is set and an old item existed.
The response never echoes the keys.

## 2. Goals and requirements

In scope:

- `putItem` and `deleteItem` take `returnValues?: "none" | "all_old"`. The default is `"none"`.
- With `"all_old"`, the result has `item`, the old item, when an old item existed.
- No write result echoes the keys of the request.
- With `"none"`, the write costs the same as before: no extra read, and no extra bytes.

Out of scope:

- `returnValues` on `batchWriteItems` and `transactWriteItems`.
  DynamoDB `BatchWriteItem` and `TransactWriteItems` do not have it.
  A large old image for each operation would also make the response budgets of these methods harder.
- `"all_new"`, `"updated_old"`, and `"updated_new"`.
  After a put, the caller has the new data, and the result has `version`.
  These values belong to a future `updateItem` method: item 6 of
  `docs/ideas/2026-11-24-public-api-and-wire-protocol-audit.md`.

The change is a breaking change to the public types. The package is not released, so no compatibility layer is
needed.

## 3. Milestones

1. The public types, the wire, the partition, the client, the HTTP example, and the tests, in one change.

## 4. Proposed solution

### 4.1 High-level overview

1. **`item` of a write result is the old item.**
   It has the shape of a complete `ReadItem`, the same as `getItem` returns, with the keys of the caller.
   It is present only with `returnValues: "all_old"`, and only when an old item existed.
2. **A write result has no keys.**
3. **`deleted` stays on `deleteItem`**, because it tells the outcome also with `"none"`.

### 4.2 Technical details

#### 4.2.1 Before and after

Example item: `u1`, `data: { n: 1 }`, `version: 7`.

```ts
// before
putItem({ hashKey: "u1", data: { n: 2 } })   → { item: { hashKey: "u1" }, version: 8, meta }
deleteItem({ hashKey: "u1" })                → { item: { hashKey: "u1" }, deleted: true, meta }

// after
putItem({ hashKey: "u1", data: { n: 2 } })   → { version: 8, meta }
putItem({ hashKey: "u1", data: { n: 2 }, returnValues: "all_old" })
  → { item: { hashKey: "u1", kind: "json", data: { n: 1 }, version: 7 }, version: 8, meta }
putItem({ hashKey: "new", data: "x", returnValues: "all_old" })   → { version: 1, meta }
deleteItem({ hashKey: "u1", returnValues: "all_old" })
  → { item: { hashKey: "u1", kind: "json", data: { n: 2 }, version: 8 }, deleted: true, meta }
deleteItem({ hashKey: "gone", returnValues: "all_old" })           → { deleted: false, meta }
```

#### 4.2.2 Public types

The types are in `shared/types.ts`.

```ts
type ReturnValues = "none" | "all_old";

type PutItemOptions = { ...; returnValues?: ReturnValues };
type DeleteItemOptions = { ...; returnValues?: ReturnValues };

type PutItemResult<T = never> = { item?: ReadItem<T>; version: number; meta: SinglePartitionMeta };
type DeleteItemResult<T = never> = { item?: ReadItem<T>; deleted: boolean; meta: SinglePartitionMeta };
```

`putItem<T>` and `deleteItem<T>` take the same `T` as `getItem<T>`: the caller's own type of the old json value.

#### 4.2.3 Wire and partition

- `PutItemRpcRequest` and `DeleteItemRpcRequest` in `server/do-partition.ts` get `returnValues`.
- The `ok` variants of `PutItemRpcResponse` and `DeleteItemRpcResponse` get `old?: WriteOldItem`.
  `WriteOldItem` is the row that `PartitionStore.getItemImage` returns: `{ data; kind; ttlAt?; version; imageBytes }`.
  The partition sends that row with no copy, and the client builds the public item from its fields and drops
  `imageBytes`.
- `conditionalWrite` reads the old row with `PartitionStore.getItemImage` before the write, when the write asks for
  it.
  The read runs in the same `transactionSync` as the condition and the write, so the image is the row that the
  write replaced.
  A condition that fails still uses `returnValuesOnConditionCheckFailure`, as before.
- The cost with `"all_old"` is one more row read and the bytes of the image.
  The image is at most `MAX_ITEM_BYTES`, as the image of a failed condition.
- The image is always the current row.
  `apiPutItem` and `apiDeleteItem` are refused while the partition migrates, so a write runs only on the partition
  that owns the row.

#### 4.2.4 Client and examples

- `validateReturnValues` checks the option, as `validateReturnValuesOnConditionCheckFailure` does.
- `client/db.ts` builds the results of section 4.2.1.
- `examples/http-api` accepts `returnValues` and encodes the old item with `encodeData`.

#### 4.2.5 Tests

- `putItem` and `deleteItem` with `"all_old"`, with and without an old item.
- `"none"`, and no option: no `item`.
- A condition with `"all_old"` that passes.
- A json, a text, and a bytes old item, with a TTL.
- A value of `returnValues` that is not valid.
- The type of `item`.

## 5. Alternative options

- **Keep the keys in `item` and add the old image as a new field, for example `old`.**
  It is not a breaking change. It was not chosen, because `item` then has a different meaning in a write result and
  in a read result, and the keys are bytes that the caller already has.
- **Read the old image in the same statement as the old facts of `upsertItem`.**
  It saves one statement. It was not chosen, because each put then reads the data of the old row, also with
  `"none"`.

## 6. References

- `docs/ideas/2026-11-24-public-api-and-wire-protocol-audit.md`, item 7
- `docs/agent-plans/2026-11-26-read-result-shape.md`
- [DynamoDB PutItem][ddb-put]
- [DynamoDB DeleteItem][ddb-delete]

[ddb-put]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html
[ddb-delete]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html
