# Typed expression engine

Date: 2026-08-29  
Status: **accepted design; M0-M3 implemented**

References:

- [DynamoDB condition and filter expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html)
- `docs/agent-plans/item-data-kinds-jsonb.md`
- `docs/adr/001-query-items-operation.md`
- `docs/agent-plans/query-items-design.md`
- `docs/agent-plans/key-codec-bytes-implementation.md`

---

## 1. Goal and scope

Build one reusable expression library that:

1. Accepts a typed AST from `FokosDB`.
2. Validates the AST in the front Worker.
3. Compiles it once into a versioned SQLite plan.
4. Sends the compiled plan to the owning partition or partitions.
5. Supports write conditions, flat projections, and query filters.
6. Supports safe row-local SQLite scalar functions.
7. Cannot widen a point lookup or key-range query.
8. Can support builders, text syntax, and updates later.

Implementation order:

1. Make the existing query batch fetch memory-safe. This milestone is independent of the expression engine.
2. Build the standalone AST, validation, and compiler library with scalar literal bindings.
3. Add write conditions for put, delete, and transactions.
4. Add canonical JSON bindings for array and object literals as an additive milestone.
5. Add projections for point, transaction, and query reads.
6. Add query filters with correct paging and metering.

The first version does not include:

- Raw SQL from callers.
- TypeScript builders or a text parser.
- Update expressions or `updateItem`.
- A public table scan.
- Nested projection reconstruction.
- Binary expression literals or tagged nested binary values.
- Set values.
- Direct array or object equality.
- Expression indexes.
- DynamoDB wire syntax, type tags, or 38-digit numbers.

---

## 2. Current data and system rules

The `items` table has these expression sources:

| Public reference | SQLite source | Notes |
| --- | --- | --- |
| `hashKey` | `hk` | Canonical key BLOB |
| `sortKey` | `sk` | Empty BLOB means no sort key |
| `v` | `v` | Item version |
| `ttl` | `ttl_epoch_utc_seconds` | Missing when SQL value is null |
| `data` | `data` and `data_kind` | Text, bytes, or JSONB |
| `data` path | JSONB `data` | Valid only when `data_kind` is JSON |

Do not expose `last_transaction_ts`, raw `data_kind`, or `est_row_bytes` as references.
`est_row_bytes` contains an internal split/promotion estimate. It is not a stable public item size.
Use an approved function such as `sqlite.octet_length(data)` when the encoded data size is needed.

The current public client rejects TTL writes. Implement the `ttl` reference now, but document that it
normally returns missing until TTL support ships. Internal rows with TTL values must work.

Conditions must preserve these existing rules:

- Evaluate on the partition that owns the item.
- Run after migration and split routing.
- Reject a conflicting pending lock before a non-transactional write.
- Evaluate only committed item state; do not expose or modify another transaction's pending state.
- Evaluate transaction conditions during prepare.
- Do not re-evaluate a condition after prepare succeeds.
- Check all single-partition transaction conditions before any write.
- Keep transaction idempotency sensitive to the condition.

Filters run after the existing hash-key and sort-key selection. They never change routing or the
normalized sort-key interval.

---

## 3. Public AST

The first version accepts plain typed objects. It has no builder or text parser.

### 3.1 References and values

```ts
type ExpressionReference =
  | { ref: "hashKey" }
  | { ref: "sortKey" }
  | { ref: "v" }
  | { ref: "ttl" }
  | { ref: "data"; path?: string };

type ExpressionValue =
  | { val: JsonValue }
  | ExpressionReference
  | { fn: string; args: readonly ExpressionValue[] };
```

`{ ref: "data" }` means the complete value. A path selects a JSON attribute:

```ts
{ ref: "data", path: "$.profile.email" }
```

Expression literals are JSON values: string, number, Boolean, null, array, or object. Reject direct
`Uint8Array` literals. Binary expression literals need an explicit tagged format and are deferred.

Scalar literals use SQLite-compatible scalar bindings. Array and object literals use canonical JSON
text bindings. The compiler serializes each composite literal once while it traverses the caller AST.
It uses the same canonical object-key order as the expression identity. The compiler then emits a
fixed `jsonb(?)`, `json(?)`, or `json_each(?)` form for the operation that consumes the value.

The serialized binding retains its logical `array` or `object` type. The compiler must not treat the
JSON text as a native `text` expression value. The serializer must enforce the expression depth and
payload limits. It must reject cycles and values outside `JsonValue`. It must not mutate the caller
value or allocate a normalized object tree.

Composite literal support is additive. Before its milestone ships, semantic validation must reject
array and object literals. This lets the scalar condition milestone ship first. The later milestone
can enable composite literals without a public AST change or a change to scalar compiled plans.

### 3.2 Conditions

```ts
type ConditionExpression =
  | {
      op: "eq" | "ne" | "lt" | "lte" | "gt" | "gte";
      args: readonly [left: ExpressionValue, right: ExpressionValue];
    }
  | {
      op: "between";
      args: readonly [value: ExpressionValue, lower: ExpressionValue, upper: ExpressionValue];
    }
  | {
      op: "in";
      args: readonly [value: ExpressionValue, choice: ExpressionValue, ...choices: ExpressionValue[]];
    }
  | {
      op: "and" | "or";
      args: readonly [ConditionExpression, ConditionExpression, ...ConditionExpression[]];
    }
  | { op: "not"; args: readonly [condition: ConditionExpression] }
  | { op: "exists" | "not_exists"; args: readonly [reference: ExpressionReference] }
  | {
      op: "begins_with";
      args: readonly [value: ExpressionValue, prefix: ExpressionValue];
    }
  | {
      op: "contains";
      args: readonly [value: ExpressionValue, search: ExpressionValue];
    };
```

Rules:

- Every condition node has only `op` and `args`.
- `and` and `or` need at least two conditions.
- `not`, `exists`, and `not_exists` need one argument.
- `between` uses `[value, lower, upper]` and includes both bounds.
- `in` uses `[value, ...choices]` and allows 1-100 choices.
- A condition root must be a condition. Do not use scalar truthiness.
- Test Boolean values with `eq` or `ne`.
- Reject unknown fields.

`size` and `attribute_type` are value functions:

```ts
{
  op: "gte",
  args: [{ fn: "size", args: [{ ref: "data", path: "$.tags" }] }, { val: 2 }],
}
```

```ts
{
  op: "eq",
  args: [{ fn: "attribute_type", args: [{ ref: "data", path: "$.profile" }] }, { val: "object" }],
}
```

### 3.3 Projections and API fields

```ts
type ProjectionExpression = {
  expr: ExpressionValue;
  as?: string;
};
```

Public options use:

```ts
condition?: ConditionExpression;
filter?: ConditionExpression;
projection?: readonly ProjectionExpression[];
```

Replace the old plural `conditions` field with singular `condition`.

### 3.4 Allocation rule

`args` needs one array per operation or function. Accept this cost. The AST is small, short-lived, and
compiled before the RPC. Do not clone, freeze, or rebuild the complete AST. Generate the canonical
identity and compiled plan while traversing the caller AST.

---

## 4. JSON paths and value semantics

### 4.1 SQLite JSON paths

Use one SQLite JSON path string. Do not use segment arrays.

Supported read paths include:

```text
$
$.profile.email
$.items[0]
$.items[#-1]
```

Support SQLite quoting for special object labels. Reject `$[#]` in conditions, filters, and
projections. Reserve it for future array-append updates.

The path validator must check:

- One `$` root.
- Valid read selectors.
- Maximum encoded length and dereference depth.
- No trailing expression text.

Do not retain parsed segments. SQLite is the final syntax authority.

Always bind paths:

```sql
json_extract(data, ?)
json_type(data, ?)
```

Never insert a caller path into SQL text.

### 4.2 Native types

| Type | Meaning |
| --- | --- |
| `missing` | Missing item/reference/path, absent sort key, absent TTL, or path on non-JSON data |
| `null` | Present JSON null or a present computed SQL-null result |
| `boolean` | JSON true or false |
| `number` | Version, TTL, JSON integer/real, or numeric result |
| `text` | String key, text data, JSON string, or text result |
| `bytes` | Binary key, byte data, or BLOB result |
| `array` | JSON array |
| `object` | JSON object |

Map SQLite integer and real JSON types to `number`. Map SQLite true and false types to `boolean`.

Missing and null are different:

- `exists(path)` is true for JSON null.
- `not_exists(path)` is false for JSON null.
- `eq(path, null)` matches JSON null, not missing.
- A direct projection omits missing and includes JSON null.

Use `json_type()` to distinguish missing from JSON null. `json_extract()` returns SQL null for both.

### 4.3 Non-JSON data

A JSON path on text or byte data returns missing. Do not call a JSON function on that data.
Therefore:

- `exists` is false.
- `not_exists` is true.
- Ordinary comparisons do not match.
- Filters do not match.
- Direct projections omit the field.

The complete `data` reference works for text, bytes, and JSON.

A nested `Uint8Array` in JSON input keeps normal `JSON.stringify` behavior and becomes an ordinary
JSON object. It has no binary semantics.

### 4.4 Keys

Expression references use logical public keys:

- String keys behave as text.
- Binary keys behave as bytes without the internal `0xFF` tag.
- An absent sort key is missing.

Use canonical key bytes for ordering. Use logical key values for `attribute_type`, `size`, projection,
and functions that need public text or bytes.

A string key literal stays the caller's public string in the compiled plan (section 6.3). The
partition encodes it with `KeyCodec.encode` when it materializes the bindings. Semantic validation
rejects an empty string literal against a key reference: `KeyCodec` rejects empty keys, and the
absent sort key is missing, not the empty string.

A key comparison must match only rows whose logical key type is compatible with the literal. The
canonical byte order does not enforce this for every operation. Binary keys sort above all text keys,
so `gt` and `gte` with a text literal would match every binary-keyed row, `ne` would match every
binary-keyed row, and a byte-level `contains` search could match inside a binary key's payload. The
compiler must add a logical-key-type guard (first canonical byte is not `0xFF`) to `gt`, `gte`, `ne`,
and `contains` on key references. `eq`, `lt`, `lte`, `between`, `in`, and `begins_with` need no
guard: UTF-8 never contains the byte `0xFF`, so a text literal cannot equal, prefix-match, or sort
above a binary key. Apply the same rule mirrored when byte key literals ship later.

Binary key literals are deferred. Direct binary-key comparison, prefix, or containment against a
literal is not in version one. A safe function such as `sqlite.hex` can return text for comparison.

---

## 5. Condition and function semantics

### 5.1 Comparisons and logic

Supported comparison operations are `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `between`, and `in`.

Rules:

- Compare values only when their logical types are compatible.
- Boolean true does not equal number `1`.
- Text `"1"` does not equal number `1`.
- Missing does not equal null.
- Compare integer and real JSON values as numbers.
- Compare text in UTF-8 binary order.
- Compare bytes in unsigned byte order.
- Reject direct array/object `eq` and `ne`.
- Reject relational comparison on array, object, or null.
- Reject a statically invalid type combination.
- A dynamically incompatible row does not match.

`and`, `or`, and `not` can nest within the AST depth limit. Preserve missing/null semantics. Do not
rely on general SQLite truthiness.

### 5.2 Existence

`exists` and `not_exists` are exact opposites. A missing item makes every reference missing.

Create-only condition:

```ts
condition: { op: "not_exists", args: [{ ref: "hashKey" }] }
```

### 5.3 Prefix and containment

`begins_with` is case-sensitive. Do not compile it to `LIKE`. Use a fixed prefix operation such as
`substr` or `instr`. It supports two compatible text values or two byte-valued expressions. A text
prefix literal against a key reference binds as canonical key bytes (section 4.4). Direct
byte-prefix literals are deferred.

`contains` supports:

- Text substring search.
- Byte subsequence search when both expressions produce bytes.
- JSON array membership for scalar string, number, Boolean, or null values.

Reject array/object search values. Direct byte search literals are deferred.
Use compiler-owned `json_each` only for scalar array containment and object size.

Byte-mode `begins_with` and `contains` on the whole `data` reference must include the `data_kind`
guard. A JSONB row is physically a BLOB, but its logical type is array or object, so it must never
match a byte prefix or a byte subsequence search. The storage class alone cannot make this
distinction.

### 5.4 Size and attribute type

`size(value)` returns:

| Value | Result |
| --- | --- |
| Text | UTF-8 byte count |
| Bytes | Byte count |
| Array | Element count |
| Object | Member count |
| Missing or unsupported runtime type | Missing |

Reject `size` on a statically known null, Boolean, or number.

`attribute_type(value)` returns one of the type names in section 4.2, including `missing`.

### 5.5 Fokos functions and SQLite allowlist

Compile Fokos functions with explicit compiler cases:

- `size`
- `attribute_type`

SQLite functions use the `sqlite.` namespace and one exact allowlist:

```ts
{ fn: "sqlite.lower", args: [{ ref: "data", path: "$.email" }] }
```

A SQLite function is eligible when it is:

1. A scalar function documented by SQLite.
2. Available in the deployed Workers SQLite runtime.
3. Deterministic for every accepted call.
4. Side-effect-free and row-local.
5. Independent of connection, schema, build, clock, and query-planner state.
6. Not an aggregate, window, table-valued, FTS, or extension-loading function.
7. Not an output constructor that can request a large allocation from a small numeric or format
   argument.

The compiler requires an exact allowlist match, removes the `sqlite.` namespace, compiles the
arguments, and emits the fixed SQLite name. It enforces the global 32-argument limit. SQLite checks
exact arity, input types, coercion, and null behavior. Direct SQLite calls use SQLite semantics, not
Fokos semantic-function rules.

The pattern and escape arguments of `glob` and `like` must be literals. Semantic validation rejects
a non-literal pattern and a pattern above the 50-byte Workers pattern limit. A row-derived pattern
could exceed the limit at runtime and abort the whole statement, and with query filters the same row
would abort every retry of that page.

The version-one allowlist contains deterministic functions only in every context. A later extension
can add argument-aware or read-only-context rules for date/time and nondeterministic functions.
Callers cannot invoke `json_each` or other table-valued functions; the compiler can use fixed
`json_each` forms internally.

Allowed condition; emits `lower(<email>) = ?`:

```ts
{
  op: "eq",
  args: [
    { fn: "sqlite.lower", args: [{ ref: "data", path: "$.email" }] },
    { val: "user@example.com" },
  ],
}
```

Allowed projection; SQLite `coalesce` semantics apply:

```ts
{
  expr: {
    fn: "sqlite.coalesce",
    args: [{ ref: "data", path: "$.name" }, { val: "Anonymous" }],
  },
  as: "name",
}
```

Rejected before routing because `load_extension` is not allowlisted:

```ts
{ fn: "sqlite.load_extension", args: [{ val: "extension" }] }
```

Appendix A defines the initial core, math, and date/time classifications. Test every allowlisted name
in the actual Workers SQLite runtime before release.

---

## 6. Validation, compilation, and security

### 6.1 Front-Worker compilation

`FokosDB` performs all expensive work before routing:

1. Validate AST shape, paths, types, function policy, and limits.
2. Generate the canonical expression identity.
3. Compile a versioned SQLite plan.
4. Count all operation bindings.

Partition DOs receive compiled plans. They only check plan version, context, SQL/binding size, and
required metadata. After statement composition and immediately before execution, the partition
materializes the plan's binding descriptors into SQL parameter values (section 6.3). This trust
model requires controlled callers of the partition namespace.

Keep the validator and compiler independent of Workers, routing, and Durable Objects. If direct
untrusted partition callers are allowed later, run the same library in the partition or authenticate
compiled plans.

### 6.2 Limits

| Limit | Version-one value |
| --- | --- |
| Operators and function calls | 300 |
| AST depth | 32 |
| JSON path dereferences | 32 |
| `in` choices | 100 before SQL binding validation |
| SQLite function arguments | At most 32 |
| `glob`/`like` pattern literal | At most 50 encoded bytes |
| One path | 4 KiB encoded |
| Canonical expression payload | At least 512 KiB; below the 2 MiB SQLite value limit |
| Compiled SQL | Below 100 KiB |
| Complete statement bindings | At most 100 |

Export limits from one module. Test each limit and one value above it. Retrieve current Cloudflare
limits again before implementation.

### 6.3 Binding descriptors

A compiled plan carries an ordered list of binding descriptors instead of materialized SQL values.
Every descriptor is one JSON-safe scalar, so a plan survives JSON persistence and RPC unchanged:

- `val` — a scalar literal: string, number, Boolean, or null. Binds as-is.
- `json` — canonical JSON text for one array or object literal (after the composite-literal
  milestone). Binds as text into the fixed `jsonb(?)`, `json(?)`, or `json_each(?)` forms.
- `keyText` — a string key literal, stored as the caller's public string. The partition binds
  `KeyCodec.encode(value)` as canonical key bytes.
- `path` — one SQLite JSON path string.

The kind names follow the native type vocabulary (section 4.2), so future binary literals extend the
set unambiguously: `keyBytes` for a binary key literal and `bytes` for a binary data literal, each
carried as base64 text. The kind tags are persisted inside plans, so a rename is a plan-version
change — do not shorten them.

The partition materializes descriptors in order, after statement composition and immediately before
execution. `KeyCodec.encode` is pure and deterministic, so a retry, a recovery replay, and a
migrated lock all materialize identical bindings.

Key references compile to SQL columns and need no binding. The target item's own key is a fixed
operation binding that the partition supplies from the request.

Version one does not pack or deduplicate bindings. Count expression bindings, fixed operation
bindings, range/cursor bindings, and combined filter/projection bindings. One descriptor counts as
one binding each time the SQL uses it. Reject a request before routing if the complete statement can
exceed 100 bindings. The partition performs the same final count after statement composition.

An AST can contain 100 `in` choices but still fail the SQL binding limit.

### 6.4 Canonical identity and compiled plan

Generate canonical identity bytes/text while traversing the original AST. Do not allocate a second
normalized tree.

Canonical encoding uses fixed operator/function spelling, fixed field order, ordered arguments, and
canonical object-literal key order. The composite-literal serializer emits the canonical JSON text
once. The compiler reuses that text as the SQLite binding and as input to the identity encoder.

Use the identity for transaction idempotency and query cursor fingerprints. Do not hash generated SQL
formatting.

A compiled plan contains:

- Plan version and context.
- Compiler-generated SQL fragments.
- Ordered binding descriptors (section 6.3).
- Required columns and complete-data/JSON-path dependencies.
- Result type/presence metadata.
- Projection output mapping when needed.
- Canonical expression identity.

Plans contain only JSON-serializable values. They do not contain the recursive AST or any byte
value. Invariant: a compiled plan must round-trip through `JSON.stringify` and `JSON.parse`
unchanged. Test this on every plan fixture; it keeps the `conditions_json` persistence lossless.

Persist transaction condition plans in the existing `tc_items.conditions_json` and
`pending_transactions.conditions_json` columns. Do not rename these SQL columns. Include serialized
plans in transaction payload limits and `estimatePendingTxBytes`. Migration copies them as opaque
text.

Keep old compiled-plan decoders while an in-flight transaction can still contain that version.

### 6.5 SQL safety and required columns

Only emit compiler-owned columns: `hk`, `sk`, `v`, `ttl_epoch_utc_seconds`, `data_kind`, and `data`.
Use fixed internal aliases. Never use projection aliases as SQL identifiers.

All values and JSON paths are bindings. The public API cannot supply SQL text, identifiers,
subqueries, aggregates, comments, statements, or collations. Compiler-generated SQL crosses only the
trusted internal RPC boundary.

The compiler reports required columns:

- Key/version/TTL conditions do not read `data`.
- A data-kind-only check can read `data_kind` without `data`.
- Whole-data and JSON-path value operations read `data` and `data_kind`.

An existing-item write condition should cost one primary-key item-row read plus the write. A missing
item reads no stored item row. Verify `rowsRead` in Workers SQLite.

### 6.6 Missing-item SQL

A condition must return a result when the item does not exist. Use a one-row source and a primary-key
`LEFT JOIN` to `items`, or an equivalent fixed statement:

```sql
SELECT items.hk IS NOT NULL AS item_present, (<predicate>) AS condition_ok
FROM (SELECT ? AS requested_hk, ? AS requested_sk) AS requested
LEFT JOIN items
  ON items.hk = requested.requested_hk
 AND items.sk = requested.requested_sk
```

Expression key references read joined item columns, not requested keys. Therefore, all references are
missing when the join misses. Verify with `EXPLAIN QUERY PLAN` that this is a primary-key lookup. If
Workers SQLite cannot keep that plan, use a small parity-tested missing-item evaluator only for the
missing case.

### 6.7 Errors

Distinguish invalid AST/path/function/arity/type, complexity limit, SQL/binding limit, condition
failure, and runtime capability errors. Do not include complete item data or sensitive literal values
in errors by default.

---

## 7. Write conditions

Conditions apply to `putItem`, `deleteItem`, and transaction put/delete/check operations.

Examples:

```ts
await db.putItem({
  hashKey: "user#123",
  data: { status: "active" },
  condition: { op: "not_exists", args: [{ ref: "hashKey" }] },
});
```

```ts
await db.putItem({
  hashKey: "user#123",
  data: { status: "active" },
  condition: { op: "eq", args: [{ ref: "v" }, { val: 7 }] },
});
```

```ts
await db.deleteItem({
  hashKey: "order#123",
  condition: {
    op: "and",
    args: [
      { op: "eq", args: [{ ref: "data", path: "$.status" }, { val: "cancelled" }] },
      { op: "lt", args: [{ ref: "data", path: "$.retryCount" }, { val: 3 }] },
    ],
  },
});
```

A condition reads the committed image before the operation. A put condition does not inspect the
replacement data.

Non-transactional order:

1. Complete migration/split routing.
2. Reject a pending lock.
3. Evaluate the condition.
4. Apply the write in the same synchronous storage transaction.

Transaction prepare order:

1. Check the existing lock.
2. Evaluate the condition on committed state.
3. Apply timestamp conflict rules.
4. Lock items only after all local operations pass.

A prepared condition is not evaluated again. The lock protects the committed image until commit or
cancel. The single-partition fast path evaluates all conditions before any write. A check operation
requires one condition.

Keep current failure behavior: non-transactional failure throws; transaction failure returns
`condition_failed`. Rich failure results are deferred.

Integration requirements:

- Replace `conditions?: ItemCondition[]` with `condition?: ConditionExpression` in public and RPC
  types.
- Keep existing `conditions_json` SQL column names.
- Compile once in `FokosDB`; send the plan through the transaction coordinator.
- Hash canonical condition identity in transaction idempotency.
- Update check-operation validation in `transaction-limits.ts`.
- Replace the old JS evaluator and `getItemStamp` condition path.
- Retain transaction timestamps for conflict checks.
- Account for serialized plans in TC/pending row size and migration.
- Preserve migration guards, split forwarding, cancel, commit, and recovery behavior.

---

## 8. Projections

Projections apply to `getItem`, each `transactGetItems` item, and `queryItems`. Without a projection,
return the current complete item.

Version-one output is a flat record.

Rules:

- `as` is optional for a direct reference.
- `as` is required for a computed expression.
- Default names are `hashKey`, `sortKey`, `v`, `ttl`, `data`, or the exact JSON path.
- Resolved names must be non-empty and unique.
- Missing direct values are omitted.
- JSON null is included as `null`.
- A direct SQLite function SQL-null result is included as `null`; Fokos functions can return missing.
- Reverse-index paths are valid because output is flat.
- Projected keys/byte data use the existing binary public read boundary.
- Aliases are output data only, never SQL identifiers.

Example:

```ts
projection: [
  { expr: { ref: "hashKey" }, as: "id" },
  { expr: { ref: "data", path: "$.profile.email" } },
  {
    expr: { fn: "sqlite.lower", args: [{ ref: "data", path: "$.profile.email" }] },
    as: "normalizedEmail",
  },
]
```

The result keys are `id`, `$.profile.email`, and `normalizedEmail`.

Use explicit projected RPC/result variants. Do not represent a flat projected record as a complete
item. Transaction reads keep version and transaction timestamp fields for conflict detection even
when the projection omits them.

---

## 9. Query filters

Filters use `ConditionExpression` and apply only to `queryItems` candidates.

Example:

```ts
await db.queryItems({
  queries: [{ hashKey: "account#123" }],
  filter: {
    op: "and",
    args: [
      {
        op: "in",
        args: [
          { ref: "data", path: "$.status" },
          { val: "pending" },
          { val: "processing" },
        ],
      },
      { op: "gte", args: [{ ref: "data", path: "$.total" }, { val: 100 }] },
    ],
  },
});
```

### 9.1 Candidate access

The hash key, sort interval, and cursor select candidates. A filter cannot change these values or
select another table. Compile the filter once and send the same plan to all visited leaves.

Do not put the filter in the candidate `WHERE`. Emit it as a result column:

```sql
SELECT hk, sk, <source columns>, (<filter>) AS expression_match
FROM items
WHERE hk = ? AND <sort/cursor bounds>
ORDER BY sk <direction>
LIMIT ?
```

`LIMIT` uses an internal candidate-batch size, not the public returned-row limit. Every admitted
candidate advances the cursor. Return only matching candidates.

### 9.2 Paging and accounting

Track returned and evaluated counts separately.

- Public `limit` caps returned rows.
- Byte budget charges the full source item before filter and projection.
- Partition-visit budget is unchanged.
- A page can return no rows and still return a cursor.
- The cursor points after the last evaluated candidate, not the last returned row.
- A first oversized candidate is evaluated and charged to guarantee progress, even when it does not
  match.
- Otherwise, stop before admitting a candidate that exceeds the remaining byte budget. Do not mark it
  evaluated or advance past it; the next page must evaluate it.

Change the batch collector so it charges bytes for every candidate and applies inclusion separately.
Propagate evaluated bytes/counts through range parents.

Apply operations in this order:

1. Select by key interval.
2. Charge full source bytes.
3. Evaluate filter.
4. Apply projection to matches.
5. Return matches.

### 9.3 Cursor identity and metrics

Cursor identity includes:

- Ordered query list.
- Encoded hash keys.
- Normalized sort intervals and inclusivity.
- Directions.
- Filter identity or a no-filter marker.
- Projection identity or a no-projection marker.

It excludes byte budget, returned limit, and the cursor itself.

Report:

- SQL rows read.
- Candidates evaluated.
- Rows returned.
- Full source bytes charged.
- Partitions visited.
- Forward count.

Parent range nodes combine child results without evaluating expressions. A non-matching row still
advances child and global cursors.

---

## 10. Standalone library and milestones

Put reusable code under `src/lib/expression/`. Separate AST types, path validation, semantic
validation, the SQLite allowlist, canonical identity, SQLite compilation, and Workers SQLite fixtures.
The library must not import the router, partition, or transaction coordinator.

Each milestone must keep tests and type checks green.

### M0 — DONE — Memory-safe query batch fetches

This milestone is independent of the expression engine. It must not change the public `queryItems`
contract or import expression code.

Deliver:

- Replace the current 1,000-row internal query fetch with a fixed batch size from 10 to 20 rows.
- Run more synchronous SQLite queries until the collector fills the public page or reaches a budget.
- Apply the byte, item, and partition-visit budgets with the existing cursor rules.
- Keep at most one small fetched batch in memory at a time.

Test queries with 400 KiB items, small items, both directions, byte and item limits, and range-tree
fan-out. Verify that each store fetch requests at most 20 rows. Verify that all pages contain no gaps
or duplicate items.

### M1 — DONE — AST and semantic fixtures

Deliver:

- Public AST and projection types.
- Native type vocabulary and limits.
- Table-driven semantic fixtures.
- Representative API examples.

Test valid/invalid shapes, argument arity, missing/null truth tables, and limit fixtures.

### M2 — DONE — Canonical identity and paths

Deliver:

- Canonical identity generation without a second AST tree.
- JSON-compatible scalar literal validation.
- Allocation-light SQLite path validation.
- Non-negative and `[#-N]` indexes.
- Read-context rejection of `[#]`.

Test stable identity, field/argument order, valid/special paths, malformed paths, path limits, and
hostile bound path text.

### M3 — DONE — Function allowlist and semantic validation

Deliver explicit `size`/`attribute_type` compilation, the deterministic SQLite scalar allowlist,
complete scalar AST validation, explicit composite-literal rejection, empty-key-literal rejection,
the literal-only `glob`/`like` pattern rule, and required-column analysis.

Test every operator shape, arity, type rule, unknown field/function, deterministic write policy,
rejection of aggregate/window/table/connection/extension functions, empty key literals, and
non-literal or oversized `glob`/`like` patterns.

### M4 — SQLite compiler

Deliver:

- All references and data-kind guards.
- Missing/null handling.
- Strict scalar comparison and logical operations.
- `between`, direct-placeholder `in`, existence, prefix, containment, size, and attribute type.
- Logical-key-type guards for `gt`, `gte`, `ne`, and `contains` on key references.
- Safe SQLite scalar calls.
- Binding descriptors, partition-side materialization, and binding-count metadata.
- Missing-item evaluation.
- Result presence/type metadata.

Test in Workers SQLite:

- All scalar semantic fixtures.
- Missing versus null and JSON guards.
- Scalar comparisons, rejected composite comparisons, and rejected composite literals.
- Text and scalar-array containment.
- UTF-8 size and reverse paths.
- Every allowlisted SQLite function.
- Direct-placeholder `in`, including a large valid AST rejected when all operation bindings exceed
  the SQL limit.
- Binding limit at 100 and 101.
- Generated SQL and bound-value separation.
- Plan JSON round-trip equality on every compiled fixture.
- Mixed string and binary sort keys: `gt`, `gte`, `ne`, and `contains` with a text literal do not
  match binary-keyed rows; `eq`, `lt`, `between`, and `begins_with` fixtures confirm no guard is
  needed.
- Primary-key query plan for missing-item evaluation.
- Existing/missing row-read metrics.
- Required-column behavior.

### M5 — Write conditions

Integrate all four write paths. Replace the old condition union/evaluator. Add public, RPC, HTTP, TC,
and persistence changes listed in section 7.

Test all operators on put/delete, JSON conditions, no-write on failure, lock/check order,
two-phase/single-shot behavior, idempotency, plan persistence/size, migration, and split routing.

### M6 — Composite JSON literal bindings

This milestone is additive. M1-M5 must reject array and object literals during semantic validation.
Scalar condition expressions and their compiled plans must remain compatible when this milestone
ships.

Deliver:

- A canonical JSON serializer for array and object literals.
- One traversal that validates, enforces limits, writes canonical JSON text, and feeds identity bytes.
- Canonical JSON text bindings with explicit logical `array` or `object` metadata.
- Compiler-owned `jsonb(?)`, `json(?)`, and `json_each(?)` forms where each operation needs them.
- Persistence and JSON round-trip support for the serialized bindings in compiled plans.

Test nested and empty composites, array order, canonical object-key order, escaped strings, all JSON
scalar children, cycles, unsupported runtime values, depth, and payload limits. Test different object
insertion orders with equal identities and equal bindings. Test `size`, `attribute_type`, `contains`,
and condition expressions in Workers SQLite.

### M7 — Projections

Deliver in this order:

1. `getItem` projection and explicit projected response.
2. Per-item `transactGetItems` projection with conflict fields preserved.
3. `queryItems` projection with cursor identity and full-source byte accounting.

Test default names, aliases, duplicates, missing/null, computed values, composite literals, reverse
paths, all source kinds, absent projection compatibility, transaction ordering, and split reads.

### M8 — Query filters

Deliver in this order:

1. Leaf candidate matching with the filter as a result column.
2. Separate evaluated/returned counts and bytes; last-evaluated cursors.
3. Range-tree and multi-query propagation; public metrics and HTTP output.

Test all condition operations as filters, mixed data kinds, zero/high selectivity, empty pages with
cursors, ascending/descending scans, no duplicate/omitted rows, promoted/range-split keys, budget
rules, cursor identity changes, and fixed candidate routing.

### M9 — Hardening and documentation

Run the full cross-surface matrix. Verify every allowlisted function in Workers SQLite. Measure large
expressions and 400 KiB JSON items. Add public examples and an ADR for shipped filter/projection
behavior.

---

## 11. Cross-cutting acceptance matrix

| Dimension | Required cases |
| --- | --- |
| Presence | Missing and existing item |
| Data kind | JSON object/array, text, bytes |
| JSON value | Missing, null, Boolean, integer, real, text, empty/non-empty array/object |
| Path | Root, normal/special field, array index, reverse index, missing parent, wrong container |
| Keys | String hash/sort, absent sort, binary key where no literal is needed |
| Operations | All conditions and semantic functions |
| Writes | Put, delete, transaction put/delete/check, single-shot |
| Reads | Get, transactional get, hash-leaf query, promoted/range query |
| Paging | Both directions, item/byte/visit stop, zero matches, rejected boundary rows |
| Topology | Unsplit, split started/completed, migrating child |
| Safety | Invalid/large AST, hostile path, unknown function, duplicate alias, nondeterministic write |

The feature is complete when all milestones pass and:

- One AST and validator serve all surfaces.
- Caller values and paths remain bound data.
- Conditions work on every write path.
- Flat projections work on all read paths.
- Filters preserve routing, paging, and accounting.
- Missing/null and data-kind rules are consistent.
- A query expression cannot scan outside its key range.

---

## 12. Future extensions

These items are not part of M0-M9:

- **Binding compaction:** If needed, evaluate a bound JSON/`json_each` argument pool, numbered binding
  reuse, or one shared filter/projection pool. Measure it before replacing one-descriptor-per-use
  bindings.
- **Untrusted partition callers:** Re-run validation/compilation in partitions or authenticate plans.
- **Composite equality:** Define object order, array order, number normalization, missing/null rules,
  traversal limits, and cost. Do not use JSONB byte equality.
- **Binary values:** Add an explicit base64/tagged literal and nested-value format.
- **Sets:** Add stored types, uniqueness, equality, containment, size, and update rules.
- **Nested projections:** Define overlap, arrays, reverse indexes, aliases, missing values, and
  collisions.
- **Projection output limits:** TODO: Add limits for the projection count, SQL result columns,
  projected bytes per item, and total response bytes. Account for internal result columns. Add a
  policy for SQLite functions that can increase output size. Keep this response protection separate
  from query source-byte metering.
- **Filter limits/indexing:** Add an evaluated-item limit or indexes only with preserved paging.
  Expression indexes need one audited literal-path renderer because bound paths do not match index
  expression text.
- **Builders/text syntax:** Produce the same AST and use the same validation.
- **Condition failure detail:** Consider old-item returns, expression-node paths, and per-operation
  transaction reasons. Define `or`/`not` failure meaning first.
- **Additional operations:** Consider arithmetic, quantified arrays, and infix `LIKE`/`GLOB`.
- **Read-only functions:** Consider argument-aware date/time functions and nondeterministic functions
  that are restricted to filters or projections.
- **Compiled-plan cache:** Add a front-Worker cache only after measurements justify it.
- **Public scan:** Design explicit bounded scan semantics.

### Future update expressions

`updateItem` must reuse references, paths, value expressions, validation, projection, and compilation.
It is not part of this plan.

Required future behavior:

- Set/replace/remove JSON paths.
- Numeric add/subtract.
- List append/prepend and `if_not_exists`.
- All actions read the original item image, not prior actions in the same request.
- `$[#]` is valid only for update append.
- Reject duplicate targets, parent/child conflicts, key updates, and invalid parents.
- Use SQLite JSONB value functions; do not load JSON into JavaScript.
- Increment `v` once and preserve transaction timestamp, size estimates, TTL, locks, split, migration,
  and promotion rules.
- For transaction updates, either materialize the result during prepare or require deterministic
  expressions and evaluate against the locked image.
- Reuse projections for old/new return values.

---

## Appendix A — SQLite scalar function allowlist

Sources:

- [SQLite core scalar functions](https://sqlite.org/lang_corefunc.html)
- [SQLite math functions](https://sqlite.org/lang_mathfunc.html)
- [SQLite date/time functions](https://sqlite.org/lang_datefunc.html)
- [Cloudflare Durable Object SQLite API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Cloudflare Durable Object limits](https://developers.cloudflare.com/durable-objects/platform/limits/)

Only add a name to the code allowlist after it passes an actual Workers SQLite test. If a candidate is
not available in the deployed runtime, keep it excluded even when upstream SQLite documents it.

### A.1 Core functions to allow

```ts
const SQLITE_CORE_FUNCTIONS = new Set([
  "abs",
  "char",
  "coalesce",
  "concat",
  "concat_ws",
  "glob",
  "hex",
  "ifnull",
  "iif",
  "instr",
  "length",
  "like",
  "lower",
  "ltrim",
  "nullif",
  "octet_length",
  "quote",
  "replace",
  "round",
  "rtrim",
  "sign",
  "substr",
  "substring",
  "trim",
  "typeof",
  "unhex",
  "unicode",
  "upper",
]);
```

Notes:

- `glob(pattern, value)` and `like(pattern, value[, escape])` use SQLite argument order and the
  Workers 50-byte pattern limit. The pattern and escape arguments must be literals, validated before
  routing (section 5.5).
- `length(text)` counts Unicode code points. `octet_length(text)` counts encoded bytes. Fokos `size`
  has its own semantics.
- `coalesce`, `ifnull`, and `iif` use SQLite null and truth rules.
- `typeof` returns a SQLite storage class; use Fokos `attribute_type` for native expression types.
- `hex` can reach the SQLite output limit and fail.
- `substring` is an alias of `substr`; keep both because Workers supports both public names.

### A.2 Core functions to exclude or defer

| Function | Reason |
| --- | --- |
| `changes`, `total_changes`, `last_insert_rowid` | Read connection or transaction state |
| `random`, `randomblob` | Nondeterministic |
| `format`, `printf`, `zeroblob` | Can request large output from a small format or numeric argument |
| `load_extension` | Loads external code and has side effects |
| `sqlite_compileoption_get`, `sqlite_compileoption_used`, `sqlite_offset`, `sqlite_source_id`, `sqlite_version` | Read build, schema, or connection details |
| `likelihood`, `likely`, `unlikely` | Query-planner hints; the compiler owns query planning |
| `max`, `min` | One-argument forms are aggregates; a simple name allowlist cannot safely select only scalar forms |
| `if` | Alias added after the current Workers allowlist; use `iif` |
| `unistr`, `unistr_quote` | Not in the current Workers allowlist |

Do not expose Workers-internal names such as `max_scalar` or `min_scalar`.

### A.3 Math functions to allow

```ts
const SQLITE_MATH_FUNCTIONS = new Set([
  "acos",
  "acosh",
  "asin",
  "asinh",
  "atan",
  "atan2",
  "atanh",
  "ceil",
  "cos",
  "cosh",
  "degrees",
  "exp",
  "floor",
  "ln",
  "log",
  "log2",
  "mod",
  "pi",
  "pow",
  "radians",
  "sin",
  "sinh",
  "sqrt",
  "tan",
  "tanh",
  "trunc",
]);
```

These functions are deterministic and row-local. SQLite returns null for invalid numeric input or a
domain error where documented. Results use SQLite floating-point semantics.

`log(x)` is base 10. `log(base, x)` uses the first argument as the base.

Exclude these aliases unless the Workers runtime adds and tests them:

| Function | Use instead |
| --- | --- |
| `ceiling` | `ceil` |
| `log10` | `log` |
| `power` | `pow` |

### A.4 Date/time functions to defer

Do not allow these functions in version one:

- `date`
- `time`
- `datetime`
- `julianday`
- `unixepoch`
- `strftime`
- `timediff`
- `current_date`
- `current_time`
- `current_timestamp`

They can use `now`, omitted time values, or local-time modifiers. A simple name allowlist cannot
ensure deterministic behavior for every call.

Before enabling them, decide:

- Whether they are allowed only in filters/projections or also in write conditions.
- Whether omitted time values, `now`, `localtime`, and other clock-dependent modifiers are allowed.
- Whether row-derived arguments can contain special values such as `now`.
- Whether the front Worker replaces current time with one bound request timestamp.
- How all transaction participants and recovered prepares use the same time value.
- Whether time-dependent filters may change results between cursor pages.

Then use a separate read-only allowlist or add argument-aware validation. Do not add date/time
functions to the simple deterministic allowlist.
