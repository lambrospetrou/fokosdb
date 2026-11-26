# RFC — One shape for the item of each read result

**State:** Implemented
**Date:** 2026-11-26
**Author:** Lambros

## 1. Overview and context

The read methods of `FokosDB` give one item in different shapes.

- A projected `getItem`, `batchGetItems`, or `transactGetItems` returns a `ReadItem` with `kind: "projected"`.
  The item has the keys, `version`, and the projected record in `data`.
- A projected `queryItems` or `scanItems` returns only the bare record.
  Code that holds such a record cannot tell it from a complete item, because a record can have its own `kind` and
  `data` fields.
- Three methods say "found" in three ways: `found: true`, `outcome: "found"`, and an item in `items`.
- A "not found" result echoes the keys of the request.
  The results are positional, so the caller already has those keys.

The `ReadItem` doc comment in `shared/types.ts` says that "one shape serves every read".
Query and scan break that rule.

DynamoDB uses one rule for every read.
An item is only its attributes, a projection returns only the attributes that it names, and an absent `Item` is
"not found".
In DynamoDB a projected item and a complete item have the same form: a map of attributes.
In FokosDB they have different forms, so each item must say which form it has.

## 2. Goals and requirements

In scope:

- An item of a read result has one of two shapes, and each shape has `kind` and `data`:
  - Without a projection, it is the full `ReadItem`, or `GsiReadItem` on a GSI.
  - With a projection, it is `{ kind: "projected", data: record }`, with no keys, no `version`, and no `ttlAt`.
- `item.kind` tells the shape of each item, also in code that does not know the request.
- An absent `item` means "not found" in `getItem`, `transactGetItems`, and `batchGetItems`.
- No read result echoes the keys of the request.
- `if (r.item)` is the one check for a found item in each read method. After it, `r.item.data` is valid.
- The type of each single-request read is exact when the request shows whether it has a projection.
  When the request does not show it, the type is the union of the two shapes.

Out of scope:

- The outcome names across the batch and write methods: `"ok"`, `"passed"`, `"applied"`.
  Item 8 of `docs/ideas/2026-11-24-public-api-and-wire-protocol-audit.md` covers them.
  The name `"ok"` of this RFC can change there.
- Expression references for the base keys of a GSI entry.
  A GSI target that projects `{ ref: "hashKey" }` and `{ ref: "sortKey" }` of the base item puts the base keys in
  the entry data.
- The wire of `transactGetItems`. Section 4.2.3 gives the reason.

The change is a breaking change to the public types. The package is not released, so no compatibility layer is
needed.

## 3. Milestones

1. The public types, the client, the `getItem` and `batchGetItems` wire, the examples, and the tests, in one change.

## 4. Proposed solution

### 4.1 High-level overview

Three rules apply to each read method:

1. **An item has one of two shapes, and both have `kind` and `data`.**
   Without a projection, it is the full `ReadItem`, or `GsiReadItem` on a GSI.
   With a projection, it is `{ kind: "projected", data: record }`.
   The record holds only what the projection names, and the caller gets a key or `version` only when the
   projection names it.
   `kind` is `"json"`, `"text"`, `"bytes"`, or `"projected"`, so a `switch (item.kind)` handles every item.
2. **An absent `item` means "not found".**
   No result has `found`, no batch entry has `"found"` or `"not_found"`, and no entry echoes the keys of the request.
3. **`batchGetItems` keeps `outcome` only to tell whether the read answered.**
   The values are `"ok"`, `"rejected"`, and `"unprocessed"`.
   The failure variants declare `item?: never`, so `r.item` is a valid read on each entry.

Each field has one meaning: `outcome` tells whether the read answered, `item` tells whether the item exists, and
`item.kind` tells the shape of the item.

### 4.2 Technical details

#### 4.2.1 Before and after

Example item: `user#1` / `profile`, `data: { name: "Ana", age: 30 }`, `version: 7`.
`P` is the projection `[{ expr: { ref: "data", path: "$.name" }, as: "name" }]`.

`getItem`:

```ts
// before: found
{ found: true, item: { hashKey: "user#1", sortKey: "profile", kind: "json", data: { name: "Ana", age: 30 }, version: 7 }, meta }
// before: found, with P
{ found: true, item: { hashKey: "user#1", sortKey: "profile", kind: "projected", data: { name: "Ana" }, version: 7 }, meta }
// before: not found
{ found: false, item: { hashKey: "user#2", sortKey: "profile" }, meta }

// after: found
{ item: { hashKey: "user#1", sortKey: "profile", kind: "json", data: { name: "Ana", age: 30 }, version: 7 }, meta }
// after: found, with P
{ item: { kind: "projected", data: { name: "Ana" } }, meta }
// after: not found
{ meta }
```

`transactGetItems`:

```ts
// before
{ items: [
    { found: true, partitionIndex: 0, hashKey: "user#1", sortKey: "profile", kind: "projected", data: { name: "Ana" }, version: 7 },
    { found: false, partitionIndex: 0, hashKey: "user#2", sortKey: "profile" },
  ], meta }

// after
{ items: [
    { partitionIndex: 0, item: { kind: "projected", data: { name: "Ana" } } },
    { partitionIndex: 0 },
  ], meta }
```

`batchGetItems`. The four keys are found, not found, rejected, and unprocessed:

```ts
// before
{ items: [
    { outcome: "found", partitionIndex: 0, hashKey: "user#1", sortKey: "profile", kind: "projected", data: { name: "Ana" }, version: 7 },
    { outcome: "not_found", partitionIndex: 0, hashKey: "user#2", sortKey: "profile" },
    { outcome: "rejected", code: "sql_limit", error_id: "e-91", partitionIndex: 1, hashKey: "user#3", sortKey: "profile" },
    { outcome: "unprocessed", code: "response_too_large", hashKey: "user#4", sortKey: "profile" },
  ], unprocessedKeys: [ { hashKey: "user#4", sortKey: "profile", projection: P } ], meta }

// after
{ items: [
    { outcome: "ok", partitionIndex: 0, item: { kind: "projected", data: { name: "Ana" } } },
    { outcome: "ok", partitionIndex: 0 },
    { outcome: "rejected", code: "sql_limit", error_id: "e-91", partitionIndex: 1 },
    { outcome: "unprocessed", code: "response_too_large" },
  ], unprocessedKeys: [ { hashKey: "user#4", sortKey: "profile", projection: P } ], meta }
```

`queryItems` and `scanItems`, on the table and on a GSI, with P:

```ts
// before
{ items: [ { name: "Ana" } ], count: 1, scannedCount: 1, cursor, meta }

// after
{ items: [ { kind: "projected", data: { name: "Ana" } } ], count: 1, scannedCount: 1, cursor, meta }
```

A page without a projection does not change.

Caller code:

```ts
// before
if (res.found) use(res.item);
for (const r of tx.items) if (r.found) use(r);
for (const r of batch.items) if (r.outcome === "found") use(r);

// after
if (res.item) use(res.item);
for (const r of tx.items) if (r.item) use(r.item);
for (const r of batch.items) if (r.item) use(r.item);

// code that does not know the request
function toJson(item: ReadResultItem) {
  switch (item.kind) {
    case "projected": return item.data;                // the record
    case "bytes": return base64(item.data);            // Uint8Array
    case "text": case "json": return item.data;
  }
}

// a failed batch read; the key comes from the request, by position
batch.items.forEach((r, i) => {
  if (r.outcome !== "ok") log(r.code, opts.items[i].hashKey);
});
```

#### 4.2.2 Public types

The types are in `shared/types.ts`, `shared/transaction-api-types.ts`, and `shared/batch-api-types.ts`.

```ts
type ProjectedReadItem<T> = { kind: "projected"; data: CallerType<T, ProjectedItem> };

type ReadResultItem<T> = ReadItem<T> | ProjectedReadItem<T>;

type GetItemResult<T> = { item?: ReadItem<T>; meta: SinglePartitionMeta };
type GetItemProjectedResult<T> = { item?: ProjectedReadItem<T>; meta: SinglePartitionMeta };

type TransactGetItemResult<T> = { item?: ReadResultItem<T>; partitionIndex: number };

type BatchGetItemResult<T> =
  | { outcome: "ok"; partitionIndex: number; item?: ReadResultItem<T> }
  | { outcome: "rejected"; code: FokosErrorCode; error_id: string; partitionIndex?: number; item?: never }
  | { outcome: "unprocessed"; code: BatchUnprocessedCode; error_id?: string; partitionIndex?: number; item?: never };
```

- `ReadItemValue` loses its `{ kind: "projected" }` member.
- The `ReadItem` doc comment states the two shapes of section 4.1.
- `QueryItemsProjectedResult` holds `ProjectedReadItem<T>` items, on the table and on a GSI.
- `getItem`, `queryItems`, `scanItems`, and the GSI `queryItems` and `scanItems` each have three overloads.
  TypeScript uses the first overload that matches the request:

  ```ts
  getItem<T = never>(opts: GetItemOptions & { projection: readonly ProjectionExpression[] }): Promise<GetItemProjectedResult<T>>;
  getItem<T = never>(opts: GetItemOptions & { projection?: undefined }): Promise<GetItemResult<T>>;
  getItem<T = never>(opts: GetItemOptions): Promise<GetItemResult<T> | GetItemProjectedResult<T>>;
  ```

  1. A request with a projection array gets the projected type.
  2. A request with no projection gets the complete type.
  3. A request whose projection can be `undefined`, for example an optional variable, gets the union.
     `item.kind` then narrows it.
  The query and scan methods follow the same pattern with their page types.
- An entry of `transactGetItems` and `batchGetItems` can have its own projection.
  Thus its `item` has the type `ReadResultItem<T>`, and `item.kind` narrows it.

#### 4.2.3 Wire

`getItem` and `batchGetItems` read an item through `readItem` in `server/do-partition.ts`, and send
`GetItemRpcItem`.
A projected read sends `{ projected: row }`, without `kind`, `version`, or `ttlAt`.
`estimateProjectedRowBytes` already counts only the cells of the row, so the response budget does not change.

`transactGetItems` keeps its wire type, `ReadForTransactionItemResultEncoded` in
`shared/transaction-wire-types.ts`.
A read transaction reads twice, and `sameCommittedState` in `client/db.ts` compares the `version` of each item to
find a `read_conflict`.
Thus `version` must stay on that wire. The client drops the fields when it builds the public result.

#### 4.2.4 Client

- `client/db.ts` builds the shapes of section 4.2.1 for `getItem`, `transactGetItems`, `batchGetItems`,
  `queryItems`, and `scanItems`.
  It does not copy the keys of the request into a result.
- The wire does not carry `kind: "projected"`. The client adds it when it builds the public item.
- `FokosDBStd.batchGetItemsWithRetry` merges the results by position and retries `"unprocessed"`.
  Its logic does not change. Its types and doc comments change.

#### 4.2.5 Examples and tests

- The examples `examples/http-api` and `examples/canary` change to the new shapes.
- The existing tests change to the new shapes.
- New tests:
  - A `getItem` of an absent item returns no `item`.
  - A projected `getItem`, `transactGetItems`, `batchGetItems`, `queryItems`, and `scanItems` return
    `{ kind: "projected", data: record }`.
  - `batchGetItems` returns one entry for each outcome, and the failure entries have no keys.
  - Type tests with `expectTypeOf` show, for each read method:
    - `if (r.item)` narrows the type, and `r.item.data` is then valid.
    - A request with a literal projection, with no projection, and with an optional projection gets the projected
      type, the complete type, and the union.
    - `item.kind` narrows the union.

## 5. Alternative options

- **`outcome: "found" | "not_found"` in `batchGetItems`.**
  A `switch` covers the four states with one check each.
  It was not chosen, because batch code then reads `r.outcome === "found"`, while `getItem` and
  `transactGetItems` read `res.item`.
  It also puts two facts in one field: whether the read answered, and whether the item exists.
- **Keep the keys on the `rejected` and `unprocessed` entries.**
  A caller can then log a failed key without its index.
  It was not chosen, because the entries then have different shapes, and the keys of an `unprocessed` entry are
  already in `unprocessedKeys`.
  The keys also cost bytes when an application serializes the result. 1,000 keys of up to 2 KiB each give up to
  about 2 MiB.
- **A bare record as the projected item.**
  It is the smallest item. It was not chosen, because code that holds an item cannot tell a record from a complete
  item: a record can have its own `kind` and `data` fields.
  The `kind` tag costs about 20 bytes for each item when an application serializes the result, and nothing on the
  wire.
- **A `projected: true` flag on the result in place of the item.**
  The record stays exactly what the projection names.
  It was not chosen, because code that gets only an item, and not its result, still cannot tell the two shapes
  apart.
- **Keep the wire of `getItem` and `batchGetItems`, and drop the fields in the client only.**
  It is a smaller change. It was not chosen, because the wire then sends fields that no caller reads.

## 6. Frequently asked questions

**Why does a projected item keep `kind`, but not the keys or `version`?**
`kind` is the only field that tells the shape of the item. The keys, `version`, and `ttlAt` are data that the
caller did not ask for.
A record field named `kind` causes no confusion, because the record is inside `data`.

**Why not one generic signature, `getItem<O extends GetItemOptions>(opts: O)`, in place of the overloads?**
TypeScript cannot take `T` from the caller and also infer `O`, so `getItem<Player>(...)` would break.

**How does the caller get the keys or the version of a projected item?**
The projection names them: `{ expr: { ref: "hashKey" }, as: "hk" }` or `{ expr: { ref: "v" }, as: "version" }`.

## 7. References

- `docs/agent-plans/2026-09-14-read-projections-and-query-filters.md`
- `docs/ideas/2026-11-24-public-api-and-wire-protocol-audit.md`
- [DynamoDB projection expressions][ddb-projection]
- [DynamoDB global secondary indexes][ddb-gsi]

[ddb-projection]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ProjectionExpressions.html
[ddb-gsi]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GSI.html
