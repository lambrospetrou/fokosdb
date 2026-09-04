# RFC — Update expressions for transactWriteItems

**State:** Completed
**Date:** 2026-09-02
**Author:** Lambros

---

## Table of Contents

1. [Overview and Context](#1-overview-and-context)
2. [Goals and Requirements](#2-goals-and-requirements)
3. [Milestones](#3-milestones)
4. [Proposed Solution](#4-proposed-solution)
5. [Future Work](#5-future-work)
6. [Alternative Options](#6-alternative-options)
7. [Frequently Asked Questions](#7-frequently-asked-questions)
8. [References](#8-references)

---

## 1. Overview and Context

### 1.1 The problem

A caller can only replace a whole item. `FokosDB.putItem` and the `put` operation of
`FokosDB.transactWriteItems` write the complete `data` value. To change one field, the caller must read the
item, change the field in the client, and write the item back under a version condition. That costs two round
trips, and it fails under contention.

Three costs follow:

1. **Lost updates under contention.** Two clients that increment the same counter must serialize through a
   condition failure and a retry. A counter update needs no read at all.
2. **Wasted payload.** A change of one field in a 400 KiB item sends 400 KiB over the wire twice.
3. **No atomic field change inside a transaction.** A transaction can put or delete a whole item. It cannot
   change one field of an item and one field of another item in the same atomic set.

The typed expression engine already solves the read half of this problem. Milestones M0 to M6 of
`docs/agent-plans/2026-08-29-typed-expression-engine-spec.md` ship the AST, the JSON path validator, the
semantic validator, the canonical identity, the SQLite compiler, the binding descriptors, and write conditions.
That spec lists update expressions as future work and states the required behaviour. This RFC specifies it.

### 1.2 What the reader must know about the current system

- An item stores `data` as one of three kinds: `text`, `bytes`, or `json`. The `data_kind` column is the
  discriminant. A `json` item stores SQLite JSONB in a BLOB column.
- An expression value is a typed object: a literal, a byte literal, a reference to `hashKey`, `sortKey`, `v`,
  `ttlAt` or `data`, or a call. A call names either a Fokos operation (`size`, `attribute_type`) or an
  allowlisted SQLite scalar function under the `sqlite.` namespace.
- The engine keeps `missing` and `null` apart. `json_extract` returns SQL NULL for both, so the compiler uses
  `json_type` to tell them apart.
- `FokosDB` validates and compiles an expression before it routes the request. A partition receives a compiled
  plan, checks its version and binding count, materializes the binding descriptors, and runs the SQL.
- A write transaction runs on one of two paths. The two-phase path locks each item at prepare and applies the
  stored payload at commit. The single-shot path runs the whole set in one partition, inside one storage
  transaction, with no lock.
- `TransactionParticipant.prepareLocal` checks every item first, then locks every item. A rejection returns
  before the first lock.
- A prepared decision is never evaluated again. The lock holds the committed image until commit or cancel.

### 1.3 Glossary

| Term | Meaning in this document |
| --- | --- |
| Action | One modification of one target path: `set` or `remove`. |
| Target | The `data` JSON path that an action modifies. |
| Pre-image | The committed item as it was before any action of the same update applied. |
| Applicability | The runtime test that decides if an update can apply to the current item. |
| Materialization | The step that computes the complete new item document and stores it. |

---

## 2. Goals and Requirements

### 2.1 In scope

1. `FokosDB.transactWriteItems` accepts an `update` operation with an ordered action list.
2. The actions are `set` and `remove`. There is no `ADD` action and no `DELETE` action.
3. Every value of every action reads the pre-image. An action never reads the result of an earlier action in
   the same update.
4. An update never invents a value. A missing operand rejects the operation.
5. An update applies only to an item that exists and has `data_kind = json`. Any other state rejects the
   operation.
6. A `set` applies only when its target resolves against the pre-image. Any other target rejects the operation.
   A `remove` of a missing path is a no-op.
7. The result of an update must stay an `array` or an `object`.
8. Every user write path carries the item size limit into its SQL statement. A write that makes an item larger
   than the limit writes nothing.
9. An update increments `v` by one, keeps the TTL of the pre-image unless the operation sets `ttlAt`, and keeps
   the monotonic rule of `last_transaction_ts`.
10. A new value operation costs one registry entry in one file. A new action kind costs one registry entry plus
    its contribution to the document expression.
11. The compiled update plan is JSON-serializable, and it survives `JSON.stringify` and `JSON.parse` unchanged.
12. Split, migration, promotion, recovery, and idempotency keep their current behaviour.

### 2.2 Out of scope

| Item | Reason |
| --- | --- |
| `FokosDB.updateItem`, the non-transactional path | The transactional path proves the semantics first. |
| Return values (`ALL_OLD`, `ALL_NEW`, `UPDATED_NEW`) | DynamoDB `TransactWriteItems` returns none either. |
| Sets, and the `ADD` and `DELETE` keywords | The engine has no set type. Section 4.2.4 gives the replacements. |
| `merge`, `list_append`, and array and object values | Each needs composite literals. Section 5 gives the reason. |
| Element removal by value (`array_difference`) | It needs a predicate over elements. Section 6.4 gives the reason. |
| A computed target path | It removes the overlap and required-column analysis. Section 6.7 gives the reason. |
| An action on `hashKey`, `sortKey`, `v`, or `ttlAt` | Keys are immutable. `ttlAt` stays an operation field. |
| A change of `data_kind` | An update needs a JSON document. A kind change is a `put`. |

### 2.3 Requirements that constrain the solution

- A partition must not load a JSON document into JavaScript. The document expression runs in SQLite.
- Commit must not fail after prepare accepted. Every test that can reject a write runs at prepare — for a `put`
  as well as for an update, because both now have a size that only the store can measure.
- The complete statement binding limit stays at 100, and the compiled SQL limit stays below 100 KiB. A compiled
  plan is a fragment of a statement, so the limit belongs to the statement, not to the plan. Section 4.2.14
  gives the reserve this costs.
- One item can hold at most `MAX_ITEM_BYTES` (400 KiB). `PartitionStore` must hold this limit, not only the
  client. Section 4.3 gives the mechanism.
- A transaction can hold at most `MAX_PAYLOAD_BYTES_PER_TX` (4 MiB). This limit controls the bytes on the
  network, which is the largest bottleneck. It does not control the bytes a transaction writes, and an update
  is expected to write more bytes than it carries.
- The existing condition fixtures must keep their results. The registry refactor changes no compiled SQL for a
  condition.

---

## 3. Milestones

Each milestone keeps the tests and the type checks green. M1 to M3 change no public API, so they can ship
alone. M4 is the first milestone a caller can use. Every milestone is shipped.

### M1 — Operation registry and action registry

**Status:** Completed

Collapse the per-operation branches into one declarative table. Today a new Fokos operation needs one branch in
`analyzeFunction` in `src/shared/expression/semantic.ts`, and three branches in `src/shared/expression/compiler.ts`:
`renderPresent`, `renderType`, and `renderValue`. A new SQLite function needs one arity entry in
`src/shared/expression/sqlite-functions.ts` and one name in a result class set in `semantic.ts`.

Deliver:

- An `OperationDefinition` record: name, `contexts`, arity, a type rule over the argument facts, and a render
  function. Presence and result type have defaults, because most operations are always present and have a fixed
  result type. The AST key stays `fn`, because the condition AST already ships with it.
- Generated registry entries for the `sqlite.` allowlist, from the existing arity tables and result classes.
- An `ExpressionContext` value (`condition`, `update-value`) threaded through validation and compilation. A
  later milestone of `docs/agent-plans/2026-08-29-typed-expression-engine-spec.md` adds a context for a filter
  and for a projection.
- An action registry that holds the target rules of each action kind.

`contexts` is a bitflag, not a set. Most operations belong to every context: 54 allowlisted SQLite functions,
`size`, and `attribute_type` are all valid in a condition and as an update value, and only `if_not_exists`, `+`,
`-`, and `*` narrow to `update-value` in this version. A set for each of 60 entries allocates 60 objects to hold
one or two members each, and a membership test is then a hash lookup. One integer for each entry costs nothing,
the default is the constant that names every context, and a narrowed entry states its own flags. The type sets
in `src/shared/expression/semantic.ts` stay as they are: those are shared instances that every entry reads, not
one instance for each entry.

The path validator does not use `ExpressionContext`. A target is `{ ref: "data", path }` and never holds a `fn`
node, so no operation is gated by a target. The read mode and the write mode of section 4.2.2 are modes of the
path validator.

The operators stay in the compiler: `contains` with its `json_each` form, the constant folding, the choice
between a logical value and canonical key bytes, and the type guard emission. These read more than one node.

This milestone changes no behaviour. The proof is that every existing compiled plan fixture is unchanged.

### M2 — Update AST, validation, and identity

**Status:** Completed

Deliver the public AST of section 4.2.1, the write path validator, the target rules, the overlap detection, the
canonical update identity, and the `updateActions` limit. The library validates an update and produces its
identity. It does not compile one.

### M3 — Scalar update compiler

**Status:** Completed

Deliver the document expression of section 4.2.5 for `set` and `remove`, with scalar values only. Deliver the
value operations `if_not_exists`, `+`, `-`, and `*`. Deliver the applicability guards, the result type guard,
the size guard, the binding descriptors, and the `CompiledUpdatePlan`. Test every rule in Workers SQLite.

### M4 — transactWriteItems integration

**Status:** Completed

Wire the plan into both write paths, with materialization at prepare. Deliver the public and RPC types, the
`tc_items` column, the `PartitionStore` methods, the participant changes, the two rejection reasons, the
validation rules, the idempotency change, and the HTTP example schema. After this milestone a caller can change
one field of an item inside a transaction.

The item size limit of section 4.3 ships here as well, and it changes a `put`. The limit is the stored row, not
the client's data, so both write paths must measure a `put` in their check pass with the same SQL the write
uses. The precheck of section 4.2.10 is one definition for both operations and both paths.

### M5 — Hardening and documentation

**Status:** Completed

Run the acceptance matrix of section 4.2.12. Measure an update on a 400 KiB item. Add happy path examples in the `expressions.test.ts` in a describe block dedicated for updates.

The 400 KiB measurement is in the table at the end of section 4.2.13. The showcase examples are the three
`expression showcase: update ...` blocks of `packages/fokosdb/src/shared/expression/expressions.test.ts`.

---

## 4. Proposed Solution

### 4.1 High-level overview

An update is an ordered list of actions on JSON paths of one item.

```ts
await db.transactWriteItems({
  items: [
    {
      operation: "update",
      hashKey: "user#123",
      update: [
        { action: "set", target: { ref: "data", path: "$.status" }, value: { val: "active" } },
        { action: "set", target: { ref: "data", path: "$.loginCount" },
          value: { fn: "+", args: [{ fn: "if_not_exists", args: [{ ref: "data", path: "$.loginCount" },
                                                                { val: 0 }] }, { val: 1 }] } },
        { action: "remove", target: { ref: "data", path: "$.tempToken" } },
      ],
      condition: { op: "eq", args: [{ ref: "v" }, { val: 7 }] },
    },
  ],
});
```

`FokosDB` validates the action list and compiles it into one SQLite document expression. The targets nest. Every
value reads the stored `data` column:

```text
jsonb_remove(                                  <- action 3, target
  jsonb_set(                                   <- action 2, target
    jsonb_set(i.data, ?p1, ?v1),               <- action 1, target
    ?p2, <if_not_exists(i.data at $.loginCount, 0) + 1>),
  ?p3)
                      ^^^^^^
                      every value reads i.data, never the accumulated document
```

SQL evaluates each value argument against the current row before the enclosing function runs. The pre-image rule
of DynamoDB is therefore a property of the compiled shape, not of an interpreter. `REMOVE a SET b = a, c = b`
gives the same answer as DynamoDB gives.

The partition runs this expression twice on the two-phase path:

```text
prepare                                        commit
-------                                        ------
1. check the lock                              1. read the pending row
2. evaluate the condition on the pre-image     2. upsert its stored payload
3. probe: applicable? what size?               3. delete the pending row
4. apply the timestamp rules
5. lock the item, and store the complete
   new document in pending_transactions
```

Step 5 is the design decision that carries this RFC. The lock holds the pre-image, so prepare can compute the
final document and store it. Commit then applies a payload that is already a plain `put`. Commit, migration, and
recovery need no knowledge of update expressions, and commit cannot fail after prepare accepted.

The single-shot path keeps its two passes. It probes every item first, then applies one `UPDATE` statement per
item inside the same storage transaction.

An update is strict. It never invents a value:

- The item must exist, and its `data_kind` must be `json`.
- The target of a `set` must resolve against the pre-image. A `remove` of a missing path is a no-op.
- An operand that evaluates to `missing` rejects the operation, except inside `if_not_exists`.
- The result must stay an `array` or an `object`, and must fit in `MAX_ITEM_BYTES`.

A rejection cancels the whole transaction, as a failed condition does.

### 4.2 Technical details

#### 4.2.1 Public AST

```ts
type UpdateTarget = { ref: "data"; path: string };

type UpdateAction =
  | { action: "set"; target: UpdateTarget; value: ExpressionValue }
  | { action: "remove"; target: UpdateTarget };

type UpdateExpression = readonly UpdateAction[];
```

Rules:

- An update must hold 1 to `EXPRESSION_LIMITS.updateActions` actions.
- `action` names the action kind. It is one key of the action registry.
- Each action node must hold only the fields of its kind. The compiler rejects an unknown field.
- A target must be `{ ref: "data", path }`. The path must not be the root `$`.
- The action order is the order of the array, and the order is significant. Section 4.2.2 gives the rule.
- `value` is an `ExpressionValue`. It reuses the references, the literals, the paths, and the operations of the
  expression engine.

The public operation:

```ts
{
  operation: "update";
  hashKey: string | Uint8Array;
  sortKey?: string | Uint8Array;
  update: UpdateExpression;
  ttlAt?: number;
  condition?: ConditionExpression;
}
```

#### 4.2.2 Target rules

The compiler must reject an update when any of these is true:

1. Two actions name the same path.
2. One target path is a parent of another target path. The test compares path segments, so `$.ab` is not a child
   of `$.a`.
3. A target path is the root `$`.
4. A `remove` action names a plain array index `[N]`, and another `remove` action names a reverse index `[#-N]`
   under the same parent. Section 4.2.6 gives the reason.

`$[#]` is the append form. It is valid only as the target of a `set` action. The read path validator keeps its
current rejection of `$[#]` in conditions, filters, and projections.

**The action order is significant.** Every value reads the pre-image, so the order never changes a value. The
order changes the document, because each action wraps the result of the actions before it. Two targets can pass
the overlap rules and still depend on their order:

```ts
[ { action: "set", target: { ref: "data", path: "$.list[#]" }, value: { val: "x" } },
  { action: "remove", target: { ref: "data", path: "$.list[0]" } } ]
```

Neither path is a parent of the other, so both rules pass. An append before a removal and a removal before an
append give different arrays. The declared order is therefore authoritative, and the compiler keeps it, except
for the removals it must reorder under section 4.2.6.

The canonical identity holds the actions in the declared order. Two updates that differ only in the order of
two independent actions therefore have two identities, and transaction idempotency treats them as two requests.
The engine does not sort the actions, because the example above shows that the order can carry meaning.

The path validator gains a write mode. The write mode accepts `$[#]` and emits the path segments to the caller,
so the overlap test in rule 2 can run. The read mode keeps its current behaviour and retains no segments. The
segments of an update target are bounded by the action limit of 32 and by the depth limit of 32, so one update
allocates at most 1024 segments.

#### 4.2.3 Value semantics

An update value follows the native type vocabulary and the missing and null rules of the expression engine. Two
rules are specific to the write context:

1. **A missing operand rejects the operation.** DynamoDB raises a validation error for `SET b = a` when `a` does
   not exist. The engine does the same. The one exception is the second argument of `if_not_exists`.
2. **The engine never writes a value it did not compute.** A `set` whose value is missing must not store JSON
   null. Rule 1 removes this case.

These two rules close most of the corruption surface. A caller that wants the tolerant behaviour writes it, and
the stored plan then shows the intent.

#### 4.2.4 The operation set

The registry holds three categories. The category decides whose semantics apply.

- **Fokos semantic operations.** Missing-aware, and this engine owns their rules: `size`, `attribute_type`,
  `if_not_exists`, `+`, `-`, and `*`. A name is an identifier or an operator symbol. A symbol can never collide
  with an identifier, so both live under one `fn` key and one registry.
- **SQLite functions.** SQLite rules apply, under the `sqlite.` namespace. This is the existing allowlist.
- **Document functions.** The compiler owns them, and a caller cannot call one. These are the `jsonb_*` writers
  of section 4.2.5.

The rule for a new operation:

> Add a Fokos operation only when one of three things is true. SQLite expresses it as an operator and not as a
> scalar function. SQLite NULL semantics contradict the missing and null model. The operation must be
> compiler-owned for safety.

Each new operation meets the rule:

- `if_not_exists(path, value)` returns `value` when the path is missing, and the path value when it is present.
  `coalesce` cannot replace it: `json_extract` returns SQL NULL for a missing path and for a stored JSON null,
  so `coalesce` would also replace a stored null. DynamoDB treats a stored null as present.
- `+`, `-`, and `*` render the SQLite arithmetic operators, and each keeps the symbol of the operator it
  renders. SQLite has no scalar function for them. SQLite computes the arithmetic. The engine owns the missing
  rule, the type rule, and the result validation. Both operands must be `number`. A result that is not finite
  rejects the operation, because JSON holds no NaN and no Infinity.
- Each arithmetic operation takes exactly two arguments. `-` is never unary, so an arity of one is an error, and
  a caller negates a value with `{ fn: "*", args: [{ val: -1 }, x] }`. A symbol that changes meaning with its
  argument count would need two type rules and two render rules for one registry entry.

There is no `divide` operator and no `%` operator in this set. SQLite integer division truncates: `5 / 2` gives
`2`, while `5 / 2.0` gives `2.5`. The SQLite `%` operator casts its operands to integers: `5.5 % 2` gives `1`.
Both results can surprise a caller who stores integers. Two allowlisted functions cover the cases that remain:
`sqlite.mod` gives the remainder over real numbers, and `sqlite.pow` gives real division, because
`{ fn: "*", args: [a, { fn: "sqlite.pow", args: [b, { val: -1 }] }] }` computes `a / b`. A caller who needs
another form computes it in the client.

The replacement for each removed DynamoDB action:

| DynamoDB | Fokos form |
| --- | --- |
| `ADD n :v` on a number | `set` with `+` over `if_not_exists(path, 0)` and `v` |
| `ADD s :v` on a set | Section 5.3 gives the `list_append` replacement. |
| `DELETE s :v` on a set | Not supported. Section 6.4 gives the reason. |
| `SET p = if_not_exists(p, :v)` | `set` with `if_not_exists(path, v)` |
| `SET p = p + :v` | `set` with `+` over `path` and `v` |

The engine does not apply an implicit default when an attribute is missing. The builder SDK emits
`+` over `if_not_exists(path, 0)` and `n` for its `add` helper. Section 6.5 gives the reason.

**Output size in the write context.** An allowlisted function means something different when the result is
stored. In a condition, a function whose output is larger than its input can only stop the statement. In an
update, the partition stores the output. `hex`, `replace`, `concat`, and `concat_ws` can each grow their input.
The size guard of section 4.2.7 is therefore mandatory in the update context, and section 4.3 holds the same
limit at the store.

The same asymmetry gives the `contexts` flag of M1 its long-term purpose. Appendix A.4 of
`docs/agent-plans/2026-08-29-typed-expression-engine-spec.md` defers the date and time functions, and it records
that a later version can restrict them to a filter and a projection. A clock-dependent value must never reach an
item, because the stored value would not reproduce. Such an operation is legal in a read context and illegal in
the update context, so `contexts` must be able to narrow in that direction as well.

#### 4.2.5 The document expression

The compiler produces one SQL expression that computes the complete new document from the stored `data` column.

| Action | Contribution |
| --- | --- |
| `set` | `jsonb_set(<accumulator>, <path>, <value>)` |
| `remove` | `jsonb_remove(<accumulator>, <path>)` |

Two rules hold the pre-image invariant:

1. The accumulator carries only the targets. Each contribution wraps the previous one.
2. Every value expression renders against the base column `i.data`. A value must never render against the
   accumulator.

The compiler uses the JSONB variants, because `data` stores JSONB. `jsonb_set`, `jsonb_remove`, `jsonb_insert`,
and `jsonb_patch` all exist in the Workers SQLite runtime, and each returns a BLOB. `json_set` also accepts a
JSONB argument, and it returns text.

Both write paths store the expression unchanged. The single-shot path writes it into `items.data`, and the
two-phase path writes it into the `data` column of the pending row. Neither converts it to text.

**A pending row of an update holds JSONB, not JSON text.** A `put` stores its JSON text there, because that is
what the client sent, and `PartitionStore.upsertItem` encodes it at commit. An update must not do the same,
because a JSONB-to-text-to-JSONB round trip is not size-stable. `jsonb_set` keeps a string element unescaped,
and re-parsing the rendered text bakes the escapes into the blob, one byte for each character that JSON
escaping expands. Measured in the Workers SQLite runtime, over the octet length of the whole document after
`jsonb_set` wrote one string member:

| Characters in the string that JSON escaping expands | Direct | After `jsonb(json(...))` |
| --- | --- | --- |
| None | 36 | 36 |
| One newline | 27 | 28 |
| Two backslashes | 30 | 32 |
| Two backslashes and two quotation marks | 41 | 45 |

Text above U+07FF does not grow, because JSON carries it verbatim. Only the escaped characters do.

Prepare measures the document to answer the size test of section 4.2.7. If commit then stored a larger value,
prepare could accept an update that commit cannot write, which breaks the invariant that commit never fails.
Storing the blob makes the bytes prepare measured the exact bytes commit writes. `upsertItem` therefore reads
the pair of the kind and the JavaScript type at its write site: for a `json` row a string is JSON text and needs
`jsonb(?)`, and a `Uint8Array` is raw JSONB and binds verbatim. `MigratedItem` and `insertItemIfAbsent` already
hold that rule for the migration path, so it adds no new convention. Section 5.4 records the same change for the
`put` path, which is the only place a conversion remains.

The store measures the size of the new value with the `estRowBytesExpr` formula over the same expression, as
`PartitionStore.upsertItem` does for a `put`. `octet_length` accepts a JSONB value.

#### 4.2.6 The order of a removal

`jsonb_remove` applies its paths from left to right, and a removal from an array shifts the elements after it.
`jsonb_remove(jsonb_remove(d, '$.r[1]'), '$.r[2]')` on `["c","h","n","s","x"]` gives `["c","n","x"]`, so it
removes the original elements 1 and 3. DynamoDB removes the original elements 1 and 2, which gives
`["c","s","x"]`.

The compiler must sort the removals of plain array indexes under one parent in descending index order. A
descending order keeps each index valid when its removal runs.

A reverse index `[#-N]` has no fixed position at compile time, so the compiler cannot sort it against a plain
index. Rule 4 of section 4.2.2 rejects that combination.

#### 4.2.7 Applicability, and the guards

An update applies only when every test passes:

| Test | SQL form |
| --- | --- |
| The item exists | `i.hk IS NOT NULL` |
| The data is JSON | `i.data_kind = <json code>` |
| The target of each `set` is valid | The rules below this table |
| No operand is missing | The presence expression of each value, from `renderPresent` |
| No value is bytes | `<the type expression of each value> <> 'bytes'`, the `valueTypeSql` of the plan |
| The result stays a document | `json_type(<document expression>) IN ('array', 'object')` |

The six tests are the `applicableSql` of the compiled plan. The size test is separate, because it is not
specific to an update: section 4.3 holds it for every write, and the probe of section 4.2.10 returns the measured
size beside the applicability answer.

**A JSON document cannot hold bytes, and the type is not always known when the plan compiles.** SQLite reads a
BLOB argument of `jsonb_set` as JSONB, and it neither converts it nor refuses it: a blob that happens to be
valid JSONB becomes a silently wrong member, and one that is not leaves a document that no later read can
decode. Two layers hold the rule:

- `validateUpdateExpression` refuses a value whose only non-null type is bytes — a byte literal, or `unhex`.
  That answer is the same for every item, so it belongs at compile time, in the client. A byte literal is still
  a valid ARGUMENT: `size` over one is a number.
- The compiler carries a per-item test for a value that is bytes for SOME items only: a key reference, which is
  text for a text key and bytes for a binary one, and any SQLite function, which is typed by what it returns
  for the row. The test is the value's own type expression, so it also covers a function that wraps a key.

**Only this cause is reported on its own.** The probe evaluates `valueTypeSql` beside `applicableSql`, and the
participant answers `update_value_is_bytes` instead of `update_not_applicable` when it fails. It is the one
cause a caller can act on: the same expression is valid for the next item. Every other cause — a missing item,
a non-json item, a missing target parent, an index past the end — stays one answer, as DynamoDB reports its own.

A guard is needed because the SQLite behaviour is silent in both directions. The measured behaviour in the
Workers SQLite runtime is:

| Case | SQLite result |
| --- | --- |
| `jsonb_set` on a missing parent, `$.p.q` on `{"a":1}` | Creates the parent: `{"a":1,"p":{"q":5}}` |
| `jsonb_set` on three missing levels, `$.p.q.r.s` | Creates all three levels |
| `jsonb_set` on a missing parent with an index, `$.p[0]` or `$.p[#]` | Creates an array parent: `{"a":1,"p":[5]}` |
| `jsonb_set` where the parent is a scalar, `$.a.q` on `{"a":1}` | No change |
| `jsonb_set` on an array index past the end, `$.a[7]` on a 2-element array | No change |
| `jsonb_set` on a reverse index past the start, `$.a[#-9]` | No change |
| `jsonb_remove` on a missing path or an index past the end | No change |

The text writers `json_set` and `json_remove` give the same result in each case. `json_type` reads a `jsonb_set`
result directly, which the result type test needs.

Both behaviours are wrong for a `set`. A silent creation turns a mistyped path into a new subtree that the
caller never asked for. A silent no-op inside an all-or-nothing transaction reports success for work that did
not happen. The guards make each `set` case a rejection. The last row is the `remove` case, and it keeps the
SQLite behaviour.

The rule for a `set` target depends on its last selector:

| Last selector | Rule | SQL guard |
| --- | --- | --- |
| A member name, `$.p.q` | The parent must be an object. | `json_type(i.data,'$.p') = 'object'` |
| An index, `$.p[3]` | The element must exist. | `json_type(i.data,'$.p[3]') IS NOT NULL` |
| The append form, `$.p[#]` | The parent must be an array. | `json_type(i.data, '$.p') = 'array'` |

DynamoDB appends when a `SET` names an index past the end of a list. This engine rejects instead, and a caller
appends with the explicit `$[#]` form. One spelling for one intent keeps the canonical identity stable.

A `remove` action needs no target guard. DynamoDB documents `REMOVE` on an absent attribute as a no-op, and
`json_remove` on a missing path makes no change, so the two agree. A `remove` of a missing path leaves the
document unchanged, and the update still increments `v`.

#### 4.2.8 The compiled plan

```ts
type CompiledUpdatePlan = {
  version: typeof UPDATE_PLAN_VERSION;
  kind: "update";
  documentSql: string;
  applicableSql: string;
  bindings: readonly ExpressionBindingDescriptor[];
  bindingCount: number;
  completeBindingCount: number;
  requiredColumns: readonly ExpressionRequiredColumn[];
  dataDependencies: { completeData: boolean; paths: readonly string[] };
  identity: string;
};
```

`completeBindingCount` is `UPDATE_FIXED_BINDING_COUNT` plus `bindingCount`: the two key parameters that every
statement binds first, plus the plan's own. Together they occupy `?1` to `completeBindingCount`, and a statement
appends its own parameters from the next number. Section 4.2.14 gives the budget this creates.

The plan reuses the binding descriptor kinds of the condition plan, and the same partition-side materialization.
A plan holds only JSON-serializable values. Every plan fixture must survive `JSON.stringify` and `JSON.parse`
unchanged, because the coordinator persists the plan as text.

`UPDATE_PLAN_VERSION` starts at 1. A partition must keep the decoder of an older version while an in-flight
transaction can still hold it.

#### 4.2.9 Storage and protocol changes

| Place | Change |
| --- | --- |
| `TransactionOperationType` | Add `"update"`. |
| `TransactWriteItem` | Add the `update` variant of section 4.2.1. |
| `TransactionItem`, `TCWriteOperation` | Add `update?: CompiledUpdatePlan`. |
| `tc_items` | Add `update_json TEXT`. The coordinator rebuilds a prepare from this table. |
| `pending_transactions` | No new column. An update stores a payload with the shape of a `put`, as JSONB. |
| `RejectionReason` | Add `update_not_applicable`, `update_value_is_bytes` and `item_too_large`, each with the item key. |
| `validateTransactWriteOperations` | An `update` carries `update` and no `data`. Count the plan in the payload. |
| `PartitionStore.upsertItem` | Take the item size limit as a binding, and throw when the guard writes no row. |
| `PartitionStore.measureItemBytes` | New. The exact stored size of a `put`, for the check pass of section 4.2.10. |
| `TransactionParticipant` | One private precheck holds every rejectable test. Both write paths call it. |
| `hashOperation` | Chain the update plan identity and its presence flag. |
| `FokosDB.transactWriteItems` | Compile the update once, beside the condition. |
| `examples/http-api/index.ts` | Add the fourth `strictObject` variant and an update schema. |

The schema change edits the existing migration in place. The product is not released, so a breaking schema
change is allowed.

The pending row of an update holds:

- `operation` = `'update'`, which keeps the operation visible for accounting.
- `data` = the complete new document, as JSONB, computed in SQL. Section 4.2.5 gives the reason for the binary
  form.
- `data_kind` = the `json` code.
- `ttl_epoch_utc_seconds` = the `ttlAt` of the operation when it is present, and the value of the pre-image when
  it is not.

`TransactionParticipant.commitLocal` handles `"update"` with the same `PartitionStore.upsertItem` call it uses
for `"put"`. `upsertItem` overwrites the TTL column from its payload, so the pre-image TTL must reach the pending
row. Without this rule an update would clear the TTL of the item.

#### 4.2.10 The two write paths

**One precheck holds every rejectable test.** `TransactionParticipant` owns one private precheck, and both write
paths run it in their check pass, before either path writes anything. It answers for a `put` and for an
`update`, and it is the only place that can reject a write:

| Operation | What the precheck runs | Rejections |
| --- | --- | --- |
| `put` | `PartitionStore.measureItemBytes`, one statement with no `FROM` clause | `item_too_large` |
| `update` | The probe statement of this section | `update_not_applicable`, `update_value_is_bytes`, `item_too_large` |
| `delete`, `check` | Nothing. Neither writes data, so neither has a size. | None |

Both measures use the same SQL the write stores, so the answer is exact. Nothing after the check pass may
reject: on the two-phase path prepare has already answered "accepted" and the coordinator is entitled to commit;
on the single-shot path `transactionSync` rolls back on a throw and NOT on a returned rejection, so a rejection
from the apply pass would keep the writes the pass had already made. A size guard that trips after the check
pass is therefore an invariant failure, whose throw rolls the whole set back.

**Two-phase prepare.** `prepareLocal` keeps its structure: it checks every item, then it locks every item.

Check pass, for each item:

1. Check the pending lock.
2. Evaluate the condition on the committed state, with the existing condition plan.
3. Run the precheck. For an update the probe returns `item_present`, `applicable`, `new_size`, and
   `last_transaction_ts`.
4. Reject with the reason the precheck returned.
5. Apply the timestamp rules. An update reuses the `last_transaction_ts` of its probe, instead of reading the
   row again.

Lock pass, for each update item: run one `INSERT INTO pending_transactions ... SELECT ... FROM items` that
computes the document expression again and stores it.

The document expression therefore runs twice at prepare. This is the cost of the check-then-lock structure,
which gives the all-or-nothing property across the items of one partition. The probe discards its document and
returns only the size, so the document never enters JavaScript.

**Single-shot.** `executeSingleShot` keeps its two passes. The check pass runs the same probe. The apply pass
runs one statement per update item:

```sql
UPDATE items
   SET data = <document expression>,
       est_row_bytes = <the estRowBytesExpr formula over the same expression>,
       v = v + 1,
       last_transaction_ts = MAX(last_transaction_ts, ?)
       [, ttl_epoch_utc_seconds = ?]
 WHERE hk = ? AND sk = ?
   AND <the estRowBytesExpr formula over the same expression> <= ?
RETURNING v, est_row_bytes
```

The TTL column keeps the value of the pre-image unless the operation sets `ttlAt`, which is the rule of section
2.1. Which of the two applies is known when the statement is built, so the assignment in brackets is present
only for an operation that sets `ttlAt`, and the statement carries no run-time test. The pending lock insert of
the two-phase path branches the same way, between the bound value and `i.ttl_epoch_utc_seconds`. `PartitionStore` owns this statement, and it keeps the `key_size_estimates` delta as `upsertItem` does. The
caller passes the new size estimate to `onItemUpserted`, so promotion and split accounting stay correct.

The `WHERE` guard repeats the size test the check pass already answered. It is the invariant of section 4.3,
which no code path may pass, not a second decision: `PartitionStore` raises when it writes no row.

**Two operations on one item.** `validateTransactWriteOperations` already rejects a duplicate key, so no item
has two actions from two operations. Per-item pre-image is therefore the complete rule. DynamoDB applies the
same restriction.

#### 4.2.11 Invariants

| Invariant | The mechanism that holds it |
| --- | --- |
| Every value reads the pre-image | The compiled shape of section 4.2.5. Values render against `i.data`. |
| The pre-image holds from prepare to commit | The pending lock. A non-transactional write to a locked key fails. |
| Commit cannot fail after prepare accepted | Prepare materializes the document and measures its exact stored size. Commit applies a stored payload. |
| An update applies at most once | Commit deletes the pending row in the apply transaction. A re-prepare skips it. |
| `v` increases by one for each update | `upsertItem` on the two-phase path, and `v = v + 1` on the single-shot path. |
| `last_transaction_ts` never moves backwards | The `MAX` rule of both statements. |
| An item never exceeds `MAX_ITEM_BYTES` | The precheck of section 4.2.10 decides it. The statement guard of section 4.3 is the invariant behind it. |
| A rejection writes nothing | Every rejectable test runs before the first write of either path. |
| The size prepare measured is the size commit writes | The pending row of an update holds JSONB, so no conversion changes it. Section 4.2.5. |
| A plan survives persistence | The JSON round-trip test on every plan fixture. |

#### 4.2.12 Testing

The acceptance matrix of section 11 of `docs/agent-plans/2026-08-29-typed-expression-engine-spec.md` applies.
Add these cases:

1. **Pre-image.** `REMOVE a SET b = a, c = b` gives `{"b": 1, "c": 2}` from `{"a": 1, "b": 2, "c": 3}`.
2. **Removal order.** Two removals of plain array indexes under one parent remove the original elements.
3. **Rejections.** A missing item, a `text` item, a `bytes` item, a missing operand, a non-finite arithmetic
   result, and a result above `MAX_ITEM_BYTES`.
4. **Targets.** A duplicate target, a parent and child pair, `$.a` with `$.ab`, the root target, and a mixed
   plain and reverse index removal.
5. **Silent SQLite behaviour.** A `set` on a missing parent does not create the parent. A `set` on an index past
   the end of an array does not change the item. A `set` where the parent is a scalar does not change the item.
   Each case is a rejection. A `remove` of a missing path is a no-op that still increments `v`.
6. **Item state.** The TTL of the pre-image survives; `ttlAt` on the operation replaces it; `v` increases by
   one; `est_row_bytes` and `key_size_estimates` match the stored document; `onItemUpserted` runs.
7. **Protocol.** Prepare, commit, cancel, recovery replay, re-prepare, a lock conflict, a timestamp conflict,
   the single-shot path, split routing, and a migrating child.
8. **Idempotency.** Two different updates under one `clientRequestToken` are rejected.
9. **Size guard.** A `put` and an update that each exceed the limit write nothing, leave `v` and the stored row
   unchanged, and leave `key_size_estimates` unchanged. A migration ingest of an item above the current limit
   still succeeds.
10. **The size limit rejects before any write.** A `put` between the client ceiling and the store ceiling — over
    `MAX_ITEM_BYTES` as a stored row, under it as data — is cancelled with `item_too_large` on BOTH write paths,
    and no item of the transaction is stored. On the two-phase path the rejection must come from prepare: a
    transaction that reaches commit and cannot apply is stuck in `COMMITTING` forever.
11. **The measured size is the stored size.** An update whose value needs escaping, sized to fit as JSONB but
    not as re-parsed text, commits on both write paths.
12. **Plans.** JSON round-trip equality, an update at the action limit, and the binding budget: a plan at
    `completeStatementBindings` minus the reserve of section 4.2.14 compiles and commits on both write paths,
    and one binding more is refused by the compiler.
13. **Registry.** Every existing condition fixture compiles to the same SQL after M1.

#### 4.2.13 Performance

Measured in the Workers SQLite runtime, over one JSON item of 300 KB, with 300 runs of each statement after a
warm-up, and one action that copies a 300 KB member:

| Statement | Total for 300 runs | For one run |
| --- | --- | --- |
| `UPDATE` with no size guard | 110 ms to 115 ms | about 0.37 ms |
| `UPDATE` with the inline size guard | 110 ms to 114 ms | about 0.37 ms |
| The probe, which computes the document and returns only its size | 43 ms | about 0.14 ms |

The write dominates the cost, not the expression. The guard repeats the document expression, and the difference
between the guarded statement and the unguarded statement stays inside the run-to-run spread. JSONB is a binary
format, so `jsonb_set` splices the value instead of a parse of the whole document.

The rest of the cost model:

- A scalar update on the single-shot path costs one probe statement and one `UPDATE` statement, against one
  primary key lookup each. A `put` of the same item costs one read of the size estimate and one upsert.
- A `put` also costs one `measureItemBytes` statement in the check pass of each transactional path. It has no
  `FROM` clause, so it reads no row: the cost is the encode the write would pay anyway. That is the price of
  the rule that commit cannot fail.
- **Bind a value that changes between calls; interpolate one that does not.** Workers SQLite keeps a prepared
  statement for each distinct SQL string, so a literal that varies per call compiles a new statement every
  time. Measured over 2000 runs of one 826-byte `UPDATE` in the Workers runtime: 56 ms with a bound parameter,
  52 ms with a constant literal, and 169 ms with a literal that changed each run. The TTL and the timestamps
  therefore bind, and the `json` kind code is interpolated. A compiled document expression varies per update
  expression and not per call, so it keeps its statement across the calls that repeat it.
- The two-phase path evaluates the document expression twice at prepare, and copies the document once from
  `pending_transactions` to `items` at commit. For a 400 KiB item that is about 800 KiB of writes for one
  transaction. The second evaluation is the probe, which costs about 0.14 ms for a 300 KB item. Section 6.1
  gives the reason. Neither copy converts between JSONB and text, because the pending row holds JSONB.
- The payload of an update request is the size of the plan, not the size of the item. A change of one field in
  a 400 KiB item sends about 200 bytes instead of 400 KiB.
- The document expression grows with the action count. The limits of section 4.2.14 bound it.

**One 400 KiB item.** Measured in the Workers SQLite runtime over one JSON item whose stored row is 399,525
bytes, with 300 runs of each statement after a warm-up, and one action that sets a scalar member. The loops are
timed from outside the Durable Object, because Workers freezes the clock between two I/O operations. An empty
loop of 300 calls costs 1 ms to 2 ms, which is the floor of every row below.

| Statement | Total for 300 runs | For one run |
| --- | --- | --- |
| The probe | 19 ms to 20 ms | about 0.07 ms |
| `UPDATE` with the inline size guard | 34 ms to 36 ms | about 0.12 ms |
| The pending lock insert of the two-phase path | 84 ms to 87 ms | about 0.29 ms |
| `measureItemBytes` for a `put` of the same item | 95 ms to 96 ms | about 0.32 ms |
| `upsertItem` for a `put` of the same item | 205 ms to 207 ms | about 0.69 ms |

An update of one member of a 400 KiB item costs about a sixth of a `put` of the same item, and the request
carries about 200 bytes instead of 400 KiB. The two statements a `put` needs — the measure in the check pass and
the upsert — together cost about 1.0 ms, against about 0.19 ms for the probe and the `UPDATE` of an update.
`jsonb_set` splices the value instead of parsing and re-encoding the whole document, which is where the
difference comes from.

#### 4.2.14 Limits

| Limit | Value |
| --- | --- |
| `updateActions` | 32 |
| Operators and function calls | 300, the existing limit |
| AST depth | 32, the existing limit |
| One path | 4 KiB encoded, the existing limit |
| Complete statement bindings | 100, the existing limit |
| `UPDATE_FIXED_BINDING_COUNT`, the keys every statement binds first | 2 |
| `UPDATE_MAX_TRAILING_BINDING_COUNT`, the widest statement-local tail | 6 |
| Bindings available to one update plan | 92 |
| Compiled SQL | Below 100 KiB, the existing limit |

The condition statement and the update statement are separate statements, so each counts its bindings on its
own.

**The budget belongs to the widest statement, not to the plan.** A compiled plan is a fragment, and three
statements embed it: the probe, the single-shot `UPDATE`, and the pending lock insert. Each binds the keys
before the plan and its own parameters after it, and the plan's numbering is fixed when it compiles. The
compiler therefore charges both the reserve and the widest tail, exactly as `compileConditionExpression`
charges `CONDITION_FIXED_BINDING_COUNT`. A plan that compiles runs on every path.

Charging the widest tail to every plan is what makes the limit one number instead of three. The alternative is
a plan that the single-shot path accepts and the two-phase path refuses, which is invisible to a caller,
because the router picks the path. Raising a statement's tail therefore lowers the budget for every update
expression, which is why the number lives beside the plan and not at the call site.

### 4.3 The item size limit at the store

`MAX_PAYLOAD_BYTES_PER_TX` controls the bytes on the network. It does not control the bytes a write stores. A
`put` carries the bytes it stores, so the two match today. An update breaks the match: a plan of about 200 bytes
can produce an item of 400 KiB. This is the intended behaviour, because the network is the larger cost.

The size limit that matters is therefore the per-item limit, and it must hold at the store. `PartitionStore`
already computes the exact stored size in SQL with `estRowBytesExpr`, so the limit becomes one more bound
parameter of the same statement.

Every user write statement takes the limit as a binding, and the guard uses the size expression the statement
already computes:

```sql
INSERT INTO items (hk, sk, data_kind, ttl_epoch_utc_seconds, v, last_transaction_ts, est_row_bytes, data)
 SELECT ?1, ?2, ?3, ?4, 1, ?5, <estRowBytesExpr>, <data expression> WHERE <estRowBytesExpr> <= ?7
 ON CONFLICT(hk, sk) DO UPDATE SET ...
RETURNING v, est_row_bytes
```

Rules:

- The statement returns zero rows when the guard fails. SQLite raises nothing, so the guard costs no rollback of
  its own.
- **The guard is the invariant, not the decision.** `PartitionStore` turns zero rows into a thrown error rather
  than reporting it to every caller, because only the non-transactional `putItem` can legitimately reach it —
  that path has no earlier pass, and the error is its answer. Both transactional paths measured the same size in
  their check pass, so a throw there is an unreachable state, and it rolls back the storage transaction that the
  write runs inside. Section 4.2.10 gives the reason a later rejection is not allowed. No caller tests a return
  value for the guard.
- One guard covers both branches of an upsert. A source row that the `WHERE` removes never reaches the conflict,
  so the `DO UPDATE` branch never runs.
- The update statement of section 4.2.10 carries the same guard in its own `WHERE`, and it also returns zero
  rows when the guard fails.
- A write that stores no row also leaves `key_size_estimates` unchanged, because the caller applies the size
  delta only when the write returns a row.
- The guard repeats the size expression, so one guarded statement evaluates the document expression twice. The
  measurement in section 4.2.13 shows that this costs nothing that a test can separate from noise. Section 6.6
  records the rejected form that computes the document once.

Measured in the Workers SQLite runtime: the guarded upsert rejects a new key and an existing key, leaves the
stored row unchanged for a rejected update, keeps `v` unchanged, and returns an empty row set in each case. The
`INSERT ... SELECT ... WHERE ... ON CONFLICT` form needs the `WHERE` clause, which the guard supplies.

Two consequences:

1. **The client check becomes a lower bound, and the store check becomes the truth.** `itemDataBytes` measures a
   string with `String.length`, which counts UTF-16 code units, so it never over-counts and it can accept text
   that is three times larger in UTF-8. The store measures the stored bytes with `octet_length`, and
   `est_row_bytes` also counts the keys and the fixed per-row overhead, which is the accounting DynamoDB uses.
   The two ceilings therefore differ for EVERY write, not only for text above U+07FF: a `put` of exactly
   `MAX_ITEM_BYTES` of data is over the limit as a stored row. This closes the open item recorded in
   `itemDataBytes` in `src/shared/transaction-limits.ts`.

   A write between the two ceilings must be rejected by the check pass, not by the write statement.
   `measureItemBytes` in the precheck of section 4.2.10 is what makes that true for a `put`. Without it a
   two-phase put in that band is accepted by the client, accepted by prepare, and refused by the guard at
   commit — and a transaction that has answered "accepted" cannot be cancelled, so it stays in `COMMITTING`
   and holds its locks forever.
2. **Internal data movement must not carry the guard.** `PartitionStore.insertItemIfAbsent` ingests rows during
   a migration, and those rows were accepted under the limit of their time. A guard there would drop an item
   when the limit falls, and `INSERT OR IGNORE` would drop it silently. The migration path therefore keeps no
   size guard.

Both write paths keep the precheck of section 4.2.10. It reports `item_too_large` before the transaction locks
or writes anything, which is a clean rejection. The statement guard is the invariant that no code path can
pass.

## 5. Future Work

Sections 5.1 to 5.3 need M8 of `docs/agent-plans/2026-08-29-typed-expression-engine-spec.md`. That milestone
delivers the canonical JSON serializer for an array literal and an object literal, the `json` binding
descriptor, and the compiler-owned `jsonb(?)`, `json(?)`, and `json_each(?)` forms. Until it ships, an update
value can only be a scalar, so none of those three items can work. Section 5.4 needs no other milestone.

### 5.1 Array and object values

An action such as `{ action: "set", target: { ref: "data", path: "$.tags" }, value: { val: ["a", "b"] } }` needs
a canonical JSON text binding for the literal. Semantic validation rejects an array literal and an object
literal today.

### 5.2 The `merge` action

`{ action: "merge", target, value }` applies RFC 7396 JSON Merge Patch with `jsonb_patch(<accumulator>,
<value>)`. It merges a whole subtree in one action, and it needs no parent to exist. DynamoDB has no equivalent
action, so this is a superset feature. The `value` is an object, so the action needs section 5.1 first.

Two rules need an answer before it ships. RFC 7396 says that a null member of a patch deletes the key, which
contradicts the missing and null model of this engine, where a stored null is a present value. The measured
behaviour confirms it: `json_patch(jsonb('{"a":1,"b":{"c":2}}'), '{"a":null}')` gives `{"b":{"c":2}}`. And a
scalar patch replaces the whole document, so `merge` needs the result type test of section 4.2.7. The measured
behaviour confirms it: `json_patch(jsonb('{"a":1,"b":{"c":2}}'), '5')` gives the JSON type `integer`.

### 5.3 `list_append`

`list_append(list1, list2)` appends the elements of `list2` to `list1`. It replaces the `ADD` action on a set.
It needs a compiler-owned aggregate over `json_each`, so a caller cannot write it, and `json_each` returns a
composite element as text. The aggregate must restore the element type before it groups the elements, or a
nested array and a nested object become strings. The second operand is usually a literal array, so the action
needs section 5.1 first.

### 5.4 JSONB in the pending row of a `put`

An update already stores JSONB in its pending row. Section 4.2.5 gives the reason, which is correctness and not
cost: the size prepare measures must be the size commit writes. `upsertItem` therefore already reads the pair
of `kind` and the JavaScript type at its write site, so the convention exists.

A `put` still stores JSON text, and `upsertItem` encodes it to JSONB at commit. That is the one place left
where a document changes its representation between the two write paths.

Move the encode to prepare. `insertPendingLock` then binds `jsonb(?)` for a `json` row, and commit binds the
blob verbatim. For a 400 KiB item this removes one text-to-JSONB parse from commit, which is the pass that holds
the lock and must not fail, and adds it to prepare, which is the pass that is allowed to reject.
`estimatePendingTxBytes` then measures the stored bytes of a `json` row instead of a text length.

The change needs no schema change, because `pending_transactions.data` is already `ANY`.

The cost is the same rule at one more write site: `insertPendingLock`, where migration ingest passes a blob and
prepare of a `put` passes text. `MigratedItem` and `insertItemIfAbsent` already hold it for the migration path.
Section 2.8 of `docs/agent-plans/item-data-kinds-jsonb.md` records the current rule for a pending row, and it
must be rewritten with the change.

---

## 6. Alternative Options

### 6.1 Evaluate the update at commit, not at prepare

Prepare stores only the plan, after it checks applicability and measures the exact result size. Commit evaluates
the document expression again and applies it. This is sound: the lock makes the pre-image immutable, and
determinism is already an allowlist invariant, so the value that commit computes is the value prepare measured.
It removes the copy of the document, and it holds the plan instead of the item in a migrating pending row.

Not chosen for two reasons that are not about cost. Commit stays a plain upsert, so commit, migration, and
recovery need no knowledge of updates. And it keeps the protocol invariant "nothing is evaluated after prepare",
instead of weakening it to "only provably deterministic work is evaluated after prepare". Materialization at
prepare also makes an `ALL_NEW` return value cheap to add later.

The cost is recorded in section 4.2.13.

### 6.2 One whole-document expression instead of an action list

An update is one expression whose result is the new `data`. `set` and `remove` become functions inside it. This
is the most flexible form, and the pre-image rule is automatic.

Not chosen. The engine loses the ability to analyze what the update touches. Duplicate and overlap detection,
required-column analysis, per-path metering, expression indexes, and a future per-path return value all need the
targets to be visible in the plan. The action list keeps the targets analyzable, while the value side stays an
open expression language. That asymmetry is the design.

### 6.3 Keep the `ADD` and `DELETE` actions

DynamoDB documents both as legacy, and recommends `SET`. `ADD` on a number is `+` with `if_not_exists`, and
both set forms need a set type this engine does not have. Two keywords with narrow rules are not worth the
surface.

### 6.4 Ship `array_union` and `array_difference` now

They emulate set semantics for a system with no set type, and an invented semantic cannot be withdrawn later.
Element removal by value needs either a bespoke function or a predicate over elements. The predicate is the
general form: one element comprehension over `json_each` gives filter, difference, and later map, and it is the
machinery that quantified arrays need. It also needs a cap on the evaluated element count, because it is the
first construct with unbounded per-element work. Removal by value therefore waits for the general form.

### 6.5 An implicit default in the arithmetic functions

`+` treats a missing operand as 0, which matches the DynamoDB `ADD` action.

Not chosen, for four reasons:

1. The idempotency fingerprint hashes the canonical plan. Two spellings of one intent would give two identities.
2. A strict rule can be relaxed later. An implicit default cannot be tightened later.
3. There is no single empty value: 0 for `+`, `[]` for `list_append`, `""` for a text function. The rule
   would be per function, and hidden.
4. DynamoDB is not consistent here. `ADD` tolerates a missing attribute, and `SET a = a + :v` on a missing `a`
   is a validation error. There is no single behaviour to match.

The builder SDK emits `+` over `if_not_exists(path, 0)` and `n` for its `add` helper, so a caller gets the
tolerant form at the layer where it belongs.

### 6.6 One materialized CTE for the document expression

A `WITH computed AS MATERIALIZED (SELECT <document expression> AS doc FROM items WHERE ...)` clause computes the
new document once, and the `SET` clause, the size column, and the guard each read `(SELECT doc FROM computed)`.
The form is correct: it reads the pre-image, it holds the guard, and a rejected write leaves the row and `v`
unchanged.

Not chosen, because it is slower. The measurement of section 4.2.13, repeated for this form, gives 143 ms and
151 ms for 300 runs, against 110 ms to 115 ms for the inline form. The materialization writes the document into
a temporary table, and each of the three references then reads a 300 KB value back. That costs more than a
second splice of a JSONB value. The inline repeat stays.

### 6.7 A computed target path

A target that is computed from item data removes duplicate detection, overlap detection, required-column
analysis, per-path metering, and any future expression index. Targets stay static.

---

### 6.8 Other shapes for an update action

The AST of section 4.2.1 follows two rules that the expression engine already follows. The key names the
category, and the value names the member: `fn` is a category and `"size"` is a member, so `action` is a category
and `"set"` is a member. Operands of the same role are positional, and operands of different roles are named:
`{ op, args }` holds values only, while `{ ref, path }` and `{ expr, as }` name their parts. An action holds a
write target and a read value, which are different roles, so the parts are named.

The rejected shapes:

- `{ op: "set", args: [target, value] }` — `args[0]` is a write target and `args[1]` is a read value. The shape
  hides the difference, and target validation becomes positional.
- `{ set: target, value }` — `set` is a member of an open set, so each new action adds a top-level key. A node
  that holds two action keys needs its own rejection rule.
- `{ op: "set", target, value }` — it gives `op` two operand shapes. One word must have one meaning.
- `{ update: { set: [...], remove: [...] } }` — it groups the actions by kind and loses the total order, which
  the append and removal example of section 4.2.2 needs. It also saves little, because an action with two
  operands needs an object for each element.
- `{ "$.a": { set: value } }`, a map keyed by target — it makes a duplicate target impossible, but a parent and
  child pair still needs the segment test. It also depends on the key order of an object, and it puts a caller
  path in key position.

The AST does not travel. `FokosDB` compiles it to SQL and to binding descriptors before it routes the request,
so the length of a node costs the caller some source text and costs the system nothing. Clarity is therefore
worth more than brevity here.

---

## 7. Frequently Asked Questions

**Does an update create the item when it is absent?**
No. DynamoDB `UpdateItem` upserts. This engine rejects, because `data` can hold `text` or `bytes`, so a path
write on an absent item has no defined meaning. A caller that wants create-or-update uses `put` with a condition.

**Why does an update need a JSON item?**
A path addresses a JSON document. A `text` item and a `bytes` item have no paths. A change of kind is a `put`.

**Can one transaction hold an update and a put for one item?**
No. `validateTransactWriteOperations` rejects a duplicate key. DynamoDB applies the same restriction.

**What does a caller see when the update cannot apply?**
The transaction is cancelled with `update_not_applicable`, `update_value_is_bytes` or `item_too_large`, and the reason carries the item
key. This is the same shape a failed condition uses.

**Does an update change the routing of a request?**
No. An update addresses one item by its primary key. Split forwarding, migration guards, and promotion behave as
they do for a `put`.

**How much does it cost to add a new operation after M1?**
One registry entry in one file, plus its tests. A new action kind costs one registry entry plus its contribution
to the document expression.

**Why is arithmetic a Fokos operation and not a `sqlite.` function?**
SQLite expresses arithmetic as an operator, and the allowlist holds scalar functions. There is no `sqlite.+` to
allow. SQLite still computes the arithmetic; the engine owns the missing rule and the result validation. The
name keeps the operator symbol, because `{ fn: "+" }` reads as the arithmetic it performs.

**Why does prepare compute the document twice?**
`prepareLocal` checks every item before it locks any item, which makes prepare all-or-nothing across the items
of one partition. A rejection returns a value, and `transactionSync` commits what the callback already wrote, so
a write in the check pass would leave an orphan lock behind a later rejection. The check pass therefore probes
and discards, and the lock pass computes the document again. The probe costs about 0.14 ms for a 300 KB item.
A single pass is possible: it materializes each item and throws a typed rejection to force the rollback, and the
caller converts the exception into the rejection response outside `transactionSync`. That saves one probe per
item, and it uses an exception for an expected outcome. The measured cost does not pay for the change.

This is not the repeated expression of section 4.3. That one is inside a single statement, and section 4.2.13
measures it as free.

**A `put` of exactly 400 KB used to be accepted. Why is it rejected now?**
The limit is the stored row, and it always was: `est_row_bytes` counts the encoded data plus both keys plus the
fixed per-row overhead, which is the accounting DynamoDB uses. Only the client check counted the data alone, and
it stays a lower bound because it cannot know the encoded size. Section 4.3 gives the two consequences. The
rejection is clean — the transaction is cancelled with `item_too_large` and writes nothing — because both write
paths measure the stored size before they write.

**Can a caller see a different answer on the two write paths?**
No, and every rule here is written to keep it that way, because the router picks the path and the caller cannot.
The size test measures the same bytes on both paths, which is why the pending row of an update holds JSONB. The
binding budget charges the widest statement to every plan, which is why a plan that compiles runs everywhere.

**Does the 4 MiB transaction payload cap bound the bytes a transaction writes?**
No, and that is intended. The payload cap controls the network, which is the larger cost. A plan of about 200
bytes can grow an item to 400 KiB. The per-item limit is the one that bounds storage, and section 4.3 holds it
at the store.

**Why is there no `merge` action in this version?**
`merge` needs an object operand, which needs the composite literal milestone of the expression engine. Section
5.2 holds its design.

**Can a condition use the new arithmetic operations?**
Not in this version. The context gate of M1 restricts them to `update-value`, which keeps the condition test
matrix unchanged. A later version can widen the context of one registry entry.

---

## 8. References

- `docs/agent-plans/2026-08-29-typed-expression-engine-spec.md`
- `docs/agent-plans/item-data-kinds-jsonb.md`
- `docs/agent-plans/key-codec-bytes-implementation.md`
- `docs/agent-plans/2026-08-23-single-partition-transaction-fast-path.md`
- `docs/agent-plans/2026-08-30-bounded-stateful-transaction-coordination.md`
- [DynamoDB update expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html)
- [SQLite JSON functions](https://sqlite.org/json1.html)
- [RFC 7396 — JSON Merge Patch](https://www.rfc-editor.org/rfc/rfc7396)
